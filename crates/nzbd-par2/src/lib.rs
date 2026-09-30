//! par2 packet walking, with no opinion about what the packets are for.
//!
//! A par2 file carries, among other things, a **FileDesc packet per source
//! file, holding that file's real name**. That fact is useful to two very
//! different parts of nzbd, and they cannot share code through
//! `nzbd-post`:
//!
//! - **Post-processing** verifies and repairs with it (`nzbd-post::par2`),
//!   and needs slice sizes, CRCs and recovery-block counts too.
//! - **The download engine** wants only the names, and wants them *the
//!   moment the main par2 file lands* — which for an obfuscated post is
//!   the difference between a job called
//!   `cc310b9901757996b0bdfd880c666e3812e6531d` for its whole life and one
//!   that names itself a minute in. `nzbd-post` depends on `nzbd-engine`,
//!   so the engine cannot reach into it; this leaf crate is where the one
//!   shared parser lives instead of a second copy.
//!
//! Deliberately dependency-free and synchronous. Callers decide about
//! blocking, I/O and error types.

/// Every par2 packet starts with this.
pub const MAGIC: &[u8] = b"PAR2\0PKT";

/// One source file, as the recovery set describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileDesc {
    pub md5_full: [u8; 16],
    pub id: [u8; 16],
    /// The file's REAL name — the thing an obfuscated post hides.
    pub name: String,
    pub length: u64,
    pub md5_16k: [u8; 16],
}

/// What one par2 file had to say.
#[derive(Debug, Clone, Default)]
pub struct Scan {
    pub set_id: Option<[u8; 16]>,
    pub invalid: bool,
    /// From the Main packet; `0` when this file had none (a `.volNNN+MM`
    /// recovery volume on its own, for instance).
    pub slice_size: u64,
    pub descs: Vec<FileDesc>,
    /// Per-file slice CRCs from IFSC packets, keyed by file id.
    pub crcs: Vec<([u8; 16], Vec<u32>)>,
    /// Recovery-slice exponents seen, for counting recovery blocks.
    pub exponents: Vec<u32>,
}

impl Scan {
    pub fn has_descs(&self) -> bool {
        !self.descs.is_empty()
    }
}

/// Does this look like a par2 file? Checks the magic only, so it costs one
/// 8-byte read.
///
/// Extension is not evidence. An obfuscated post names its par2 files the
/// same random way it names everything else — job #182's recovery index
/// arrived as `LKKp171CWZ3IrtvUyiLuNWIqWtos` — so anything that keys off
/// `.par2` finds nothing exactly when it matters most.
pub fn is_par2(head: &[u8]) -> bool {
    head.len() >= MAGIC.len() && &head[..MAGIC.len()] == MAGIC
}

/// Walk one par2 file's packets.
///
/// Tolerant by construction: a torn or still-downloading file stops the
/// walk at the bad length rather than failing, and whatever was read
/// before that point is returned. Callers get partial truth or no truth,
/// never a wrong answer.
pub fn scan(bytes: &[u8]) -> Scan {
    use md5::{Digest, Md5};
    let mut out = Scan::default();
    let mut seen_ids: Vec<[u8; 16]> = Vec::new();
    let mut pos = 0usize;
    while pos + 64 <= bytes.len() {
        if &bytes[pos..pos + 8] != MAGIC {
            // Packets are 4-byte aligned; step, don't give up. A par2 file
            // can carry leading junk when an uploader concatenates.
            pos += 4;
            continue;
        }
        let len = u64::from_le_bytes(bytes[pos + 8..pos + 16].try_into().unwrap()) as usize;
        if len < 64 || !len.is_multiple_of(4) || len > bytes.len().saturating_sub(pos) {
            break; // torn / partial file
        }
        let digest: [u8; 16] = Md5::digest(&bytes[pos + 32..pos + len]).into();
        if digest.as_slice() != &bytes[pos + 16..pos + 32] {
            out.invalid = true;
            break;
        }
        let set_id: [u8; 16] = bytes[pos + 32..pos + 48].try_into().unwrap();
        if out.set_id.is_some_and(|id| id != set_id) {
            out.invalid = true;
            break;
        }
        out.set_id = Some(set_id);
        let ptype = &bytes[pos + 48..pos + 64];
        let body = &bytes[pos + 64..pos + len];
        match ptype {
            b"PAR 2.0\0Main\0\0\0\0" if body.len() >= 12 => {
                out.slice_size = u64::from_le_bytes(body[0..8].try_into().unwrap());
            }
            b"PAR 2.0\0FileDesc" if body.len() >= 56 => {
                let mut id = [0u8; 16];
                id.copy_from_slice(&body[0..16]);
                if !seen_ids.contains(&id) {
                    seen_ids.push(id);
                    let mut md5_16k = [0u8; 16];
                    md5_16k.copy_from_slice(&body[32..48]);
                    out.descs.push(FileDesc {
                        md5_full: body[16..32].try_into().unwrap(),
                        id,
                        name: String::from_utf8_lossy(&body[56..])
                            .trim_end_matches('\0')
                            .to_string(),
                        length: u64::from_le_bytes(body[48..56].try_into().unwrap()),
                        md5_16k,
                    });
                }
            }
            b"PAR 2.0\0IFSC\0\0\0\0" if body.len() >= 16 => {
                let mut id = [0u8; 16];
                id.copy_from_slice(&body[0..16]);
                if !out.crcs.iter().any(|(k, _)| *k == id) {
                    let mut v = Vec::new();
                    for chunk in body[16..].chunks_exact(20) {
                        v.push(u32::from_le_bytes(chunk[16..20].try_into().unwrap()));
                    }
                    out.crcs.push((id, v));
                }
            }
            b"PAR 2.0\0RecvSlic" if body.len() >= 4 => {
                let e = u32::from_le_bytes(body[0..4].try_into().unwrap());
                if !out.exponents.contains(&e) {
                    out.exponents.push(e);
                }
            }
            _ => {}
        }
        pos += len;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(ptype: &[u8; 16], body: &[u8]) -> Vec<u8> {
        use md5::{Digest, Md5};
        let len = (64 + body.len()).next_multiple_of(4);
        let mut p = vec![0; len];
        p[..8].copy_from_slice(MAGIC);
        p[8..16].copy_from_slice(&(len as u64).to_le_bytes());
        p[48..64].copy_from_slice(ptype);
        p[64..64 + body.len()].copy_from_slice(body);
        let digest = Md5::digest(&p[32..]);
        p[16..32].copy_from_slice(&digest);
        p
    }

    fn filedesc(id: u8, name: &str, length: u64) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&[id; 16]); // file id
        body.extend_from_slice(&[0u8; 16]); // full md5
        body.extend_from_slice(&[id; 16]); // md5 of first 16k
        body.extend_from_slice(&length.to_le_bytes());
        body.extend_from_slice(name.as_bytes());
        while body.len() % 4 != 0 {
            body.push(0); // packets are 4-byte aligned, names are padded
        }
        packet(b"PAR 2.0\0FileDesc", &body)
    }

    #[test]
    fn the_real_names_come_out_of_the_filedesc_packets() {
        let mut f = packet(b"PAR 2.0\0Main\0\0\0\0", &{
            let mut b = 384_000u64.to_le_bytes().to_vec();
            b.extend_from_slice(&2u32.to_le_bytes());
            b.extend_from_slice(&[0u8; 8]);
            b
        });
        f.extend(filedesc(
            1,
            "Some.Movie.2024.1080p.WEB-DL-GRP.part01.rar",
            100,
        ));
        f.extend(filedesc(
            2,
            "Some.Movie.2024.1080p.WEB-DL-GRP.part02.rar",
            100,
        ));

        assert!(is_par2(&f));
        let s = scan(&f);
        assert_eq!(s.slice_size, 384_000);
        assert_eq!(s.descs.len(), 2);
        assert_eq!(
            s.descs[0].name,
            "Some.Movie.2024.1080p.WEB-DL-GRP.part01.rar"
        );
        assert_eq!(s.descs[0].length, 100);
        assert!(s.has_descs());
    }

    /// The engine reads this file the instant the writer finalizes it, and
    /// a recovery volume may still be arriving. A truncated tail must cost
    /// the packets after the tear and nothing before it.
    #[test]
    fn a_torn_tail_keeps_what_was_already_readable() {
        let mut f = filedesc(1, "First.File.mkv", 10);
        let second = filedesc(2, "Second.File.mkv", 20);
        f.extend_from_slice(&second[..second.len() - 8]); // cut mid-packet

        let s = scan(&f);
        assert_eq!(s.descs.len(), 1, "the intact packet still parsed");
        assert_eq!(s.descs[0].name, "First.File.mkv");
    }

    /// Extension is not evidence — that is the whole reason this is
    /// content-sniffed. Anything without the magic is not a par2 file.
    #[test]
    fn only_the_magic_says_par2() {
        assert!(!is_par2(b"Rar!\x1a\x07\x00\x00"));
        assert!(!is_par2(b"PAR2"), "a short read is not a match");
        assert!(!is_par2(&[]));
        assert!(scan(b"not a par2 file at all, no magic anywhere in here")
            .descs
            .is_empty());
    }

    /// A file id repeated across packets (par2 sets repeat FileDesc in
    /// every volume) must not multiply the file list.
    #[test]
    fn a_repeated_file_id_is_recorded_once() {
        let mut f = filedesc(7, "Movie.mkv", 10);
        f.extend(filedesc(7, "Movie.mkv", 10));
        assert_eq!(scan(&f).descs.len(), 1);
    }
}
