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
    /// Validated recovery payload lengths, excluding the exponent.
    pub recovery_sizes: Vec<(u32, u64)>,
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
                    out.recovery_sizes.push((e, body.len() as u64 - 4));
                }
            }
            _ => {}
        }
        pos += len;
    }
    out
}

/// Strict streaming discovery for complete, quiescent recovery inputs.
/// Recovery bodies use 64 KiB of storage; metadata is capped at 64 MiB per
/// file. The callback runs between reads and can cancel blocking discovery.
pub fn scan_reader(
    input: &mut impl std::io::Read,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> std::io::Result<Scan> {
    use md5::{Digest, Md5};
    let mut out = Scan::default();
    let mut recovery_seen = std::collections::HashMap::new();
    let mut metadata_bytes = 0u64;
    loop {
        checkpoint()?;
        let mut header = [0; 64];
        let n = input.read(&mut header[..1])?;
        if n == 0 {
            break;
        }
        input.read_exact(&mut header[1..])?;
        let len = u64::from_le_bytes(header[8..16].try_into().unwrap());
        if &header[..8] != MAGIC || len < 64 || !len.is_multiple_of(4) {
            out.invalid = true;
            break;
        }
        let set: [u8; 16] = header[32..48].try_into().unwrap();
        if out.set_id.is_some_and(|id| id != set) {
            out.invalid = true;
            break;
        }
        out.set_id = Some(set);
        let recovery = &header[48..64] == b"PAR 2.0\0RecvSlic";
        if !recovery {
            metadata_bytes = metadata_bytes.saturating_add(len);
            if metadata_bytes > 64 * 1024 * 1024 {
                return Err(std::io::Error::other("PAR metadata size limit"));
            }
        }
        let mut hash = Md5::new();
        hash.update(&header[32..]);
        let mut packet = if recovery {
            Vec::new()
        } else {
            header.to_vec()
        };
        let mut exponent = [0; 4];
        let mut offset = 0u64;
        let mut remaining = len - 64;
        let mut buffer = [0; 65536];
        while remaining > 0 {
            checkpoint()?;
            let n = remaining.min(buffer.len() as u64) as usize;
            input.read_exact(&mut buffer[..n])?;
            hash.update(&buffer[..n]);
            if recovery && offset == 0 && n >= 4 {
                exponent.copy_from_slice(&buffer[..4]);
            }
            if !recovery {
                packet.extend_from_slice(&buffer[..n]);
            }
            remaining -= n as u64;
            offset += n as u64;
        }
        if hash.finalize().as_slice() != &header[16..32] || (recovery && len < 68) {
            out.invalid = true;
            break;
        }
        if recovery {
            let e = u32::from_le_bytes(exponent);
            if recovery_seen.get(&e).is_some_and(|size| *size != len - 68) {
                out.invalid = true;
                break;
            }
            if let std::collections::hash_map::Entry::Vacant(entry) = recovery_seen.entry(e) {
                metadata_bytes = metadata_bytes.saturating_add(64);
                if metadata_bytes > 64 * 1024 * 1024 {
                    return Err(std::io::Error::other("PAR recovery metadata size limit"));
                }
                entry.insert(len - 68);
                out.exponents.push(e);
                out.recovery_sizes.push((e, len - 68));
            }
        } else {
            let parsed = scan(&packet);
            if parsed.slice_size > 0 {
                if out.slice_size != 0 && out.slice_size != parsed.slice_size {
                    out.invalid = true;
                    break;
                }
                out.slice_size = parsed.slice_size;
            }
            for desc in parsed.descs {
                if out.descs.iter().any(|d| d.id == desc.id && d != &desc) {
                    out.invalid = true;
                    break;
                }
                if !out.descs.iter().any(|d| d.id == desc.id) {
                    out.descs.push(desc);
                }
            }
            for (id, crcs) in parsed.crcs {
                if out
                    .crcs
                    .iter()
                    .any(|(key, previous)| *key == id && previous != &crcs)
                {
                    out.invalid = true;
                    break;
                }
                if !out.crcs.iter().any(|(key, _)| *key == id) {
                    out.crcs.push((id, crcs));
                }
            }
        }
    }
    Ok(out)
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
    #[test]
    fn streaming_matches_packet_evidence_and_rejects_corruption() {
        let mut bytes = filedesc(1, "-file with spaces.bin", 120);
        bytes.extend(packet(b"PAR 2.0\0RecvSlic", &[2, 0, 0, 0, 9, 8, 7, 6]));
        let memory = scan(&bytes);
        let stream = scan_reader(&mut &bytes[..], &|| Ok(())).unwrap();
        assert_eq!(stream.descs, memory.descs);
        assert_eq!(stream.recovery_sizes, memory.recovery_sizes);
        assert_eq!(stream.exponents, memory.exponents);
        let mut corrupted = bytes.clone();
        *corrupted.last_mut().unwrap() ^= 1;
        assert!(
            scan_reader(&mut &corrupted[..], &|| Ok(()))
                .unwrap()
                .invalid
        );
        assert!(scan_reader(&mut &bytes[..bytes.len() - 1], &|| Ok(())).is_err());
        let mut mixed = packet(b"PAR 2.0\0RecvSlic", &[3, 0, 0, 0]);
        mixed[32] = 1;
        use md5::{Digest, Md5};
        let digest = Md5::digest(&mixed[32..]);
        mixed[16..32].copy_from_slice(&digest);
        bytes.extend(mixed);
        assert!(scan_reader(&mut &bytes[..], &|| Ok(())).unwrap().invalid);
    }
    #[test]
    fn streaming_large_recovery_uses_bounded_reads_and_cooperative_cancellation() {
        use md5::{Digest, Md5};
        use std::io::Read;
        let length = 65 * 1024 * 1024u64;
        let mut header = [0; 64];
        header[..8].copy_from_slice(MAGIC);
        header[8..16].copy_from_slice(&(68 + length).to_le_bytes());
        header[48..64].copy_from_slice(b"PAR 2.0\0RecvSlic");
        let mut hash = Md5::new();
        hash.update(&header[32..]);
        hash.update([0; 4]);
        let zeros = [0; 65536];
        for _ in 0..length / 65536 {
            hash.update(zeros);
        }
        header[16..32].copy_from_slice(&hash.finalize());
        struct Bounded<R>(R);
        impl<R: Read> Read for Bounded<R> {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                assert!(buffer.len() <= 65536);
                self.0.read(buffer)
            }
        }
        let reader = std::io::Cursor::new(header).chain(std::io::repeat(0).take(length + 4));
        let scan = scan_reader(&mut Bounded(reader), &|| Ok(())).unwrap();
        assert!(!scan.invalid);
        assert_eq!(scan.recovery_sizes, vec![(0, length)]);
        let mut reader = std::io::Cursor::new(header).chain(std::io::repeat(0).take(length + 4));
        let checks = std::cell::Cell::new(0);
        let error = scan_reader(&mut reader, &|| {
            checks.set(checks.get() + 1);
            if checks.get() > 3 {
                Err(std::io::ErrorKind::Interrupted.into())
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
    }
}
