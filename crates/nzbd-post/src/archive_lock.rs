//! Judge a finished archive volume during download: is it an encrypted
//! archive that no known password opens? (`nzbd_engine::archive_probe`.)
//!
//! Two steps, the cheap one first:
//!
//! 1. [`sniff`] reads the volume's own headers — content, never the name,
//!    because obfuscated posts hide extensions. Only the first volume of a
//!    RAR set (or a header-encrypted RAR volume, whose volume number is
//!    itself encrypted) and a 7z whose header lies inside the file can be
//!    judged; everything else is [`Sniff::Plain`] and costs one small read.
//! 2. For an encrypted one, each known password is tested with the
//!    configured extractor (`unrar t` / `7z t`), read exactly as unpack reads
//!    it. Only when every candidate is refused is the volume locked —
//!    the same evidence that ends unpack `failed/unpack`.
//!
//! What a first volume cannot settle stays with unpack: a RAR4 set whose
//! file data alone is encrypted has no password check value, so a wrong
//! password shows only as a checksum error at the end of the file, in a
//! later volume; a split 7z keeps its header in its last part.
use std::io::Read;
use std::path::{Path, PathBuf};

use nzbd_engine::archive_probe::{ArchiveProbe, ArchiveProbeJob, ArchiveVerdict};
use tokio_util::sync::CancellationToken;

use crate::manager::PostConfig;
use crate::tools::Extractors;
use crate::ArchiveKind;

/// Header bytes read from a volume. RAR and 7z start headers are tiny; a
/// first file header with a long name and extra records stays well inside.
const SNIFF_BYTES: u64 = 256 * 1024;

const RAR5_SIGNATURE: &[u8] = b"Rar!\x1a\x07\x01\x00";
const RAR4_SIGNATURE: &[u8] = b"Rar!\x1a\x07\x00";
const SEVENZIP_SIGNATURE: &[u8] = b"7z\xbc\xaf\x27\x1c";
/// 7z AES-256 + SHA-256 coder id.
const SEVENZIP_AES: &[u8] = &[0x06, 0xf1, 0x07, 0x01];

/// What a volume's headers say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sniff {
    /// Not an encrypted archive that can be judged from this volume.
    Plain,
    /// Encrypted (headers or file entries); judge it with the extractor.
    Encrypted(ArchiveKind),
}

/// Read `path`'s headers. Any read failure is `Plain`: this check only
/// ever saves bandwidth, and unpack still judges whatever it skips.
pub fn sniff(path: &Path) -> Sniff {
    let Ok(file) = std::fs::File::open(path) else {
        return Sniff::Plain;
    };
    let len = file.metadata().map(|m| m.len()).unwrap_or(0);
    let mut head = Vec::new();
    if file.take(SNIFF_BYTES).read_to_end(&mut head).is_err() {
        return Sniff::Plain;
    }
    if let Some(rest) = head.strip_prefix(RAR5_SIGNATURE) {
        return rar5(rest);
    }
    if let Some(rest) = head.strip_prefix(RAR4_SIGNATURE) {
        return rar4(rest);
    }
    if head.starts_with(SEVENZIP_SIGNATURE) {
        return sevenzip(path, &head, len);
    }
    Sniff::Plain
}

/// RAR5 variable-length integer: 7 bits per byte, high bit = more.
fn vint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *buf.get(*pos)?;
        *pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

fn rar5(buf: &[u8]) -> Sniff {
    rar5_headers(buf).unwrap_or(Sniff::Plain)
}

fn rar5_headers(buf: &[u8]) -> Option<Sniff> {
    const ENCRYPTION_HEADER: u64 = 4;
    const MAIN_HEADER: u64 = 1;
    const FILE_HEADER: u64 = 2;
    const HAS_EXTRA: u64 = 0x0001;
    const HAS_DATA: u64 = 0x0002;
    const MAIN_VOLUME_NUMBER: u64 = 0x0002;
    const EXTRA_ENCRYPTION: u64 = 0x01;
    let mut pos = 0usize;
    // A handful of headers is enough to reach the first file header.
    for _ in 0..16 {
        pos = pos.checked_add(4)?; // header CRC32
        let size = usize::try_from(vint(buf, &mut pos)?).ok()?;
        let start = pos;
        let end = start.checked_add(size)?;
        let header = buf.get(start..end)?;
        let mut at = 0usize;
        let kind = vint(header, &mut at)?;
        let flags = vint(header, &mut at)?;
        let extra = if flags & HAS_EXTRA != 0 {
            usize::try_from(vint(header, &mut at)?).ok()?
        } else {
            0
        };
        let data = if flags & HAS_DATA != 0 {
            vint(header, &mut at)?
        } else {
            0
        };
        match kind {
            // Everything after the signature is encrypted, the volume
            // number included: any volume of the set can be tested.
            ENCRYPTION_HEADER => return Some(Sniff::Encrypted(ArchiveKind::Rar)),
            MAIN_HEADER => {
                let archive_flags = vint(header, &mut at)?;
                if archive_flags & MAIN_VOLUME_NUMBER != 0 {
                    return Some(Sniff::Plain); // a later volume
                }
            }
            FILE_HEADER => {
                let records = header.get(size.checked_sub(extra)?..)?;
                let mut r = 0usize;
                while r < records.len() {
                    let record = usize::try_from(vint(records, &mut r)?).ok()?;
                    let next = r.checked_add(record)?;
                    let mut t = r;
                    if vint(records, &mut t)? == EXTRA_ENCRYPTION {
                        return Some(Sniff::Encrypted(ArchiveKind::Rar));
                    }
                    r = next;
                }
                return Some(Sniff::Plain);
            }
            _ => {}
        }
        pos = end.checked_add(usize::try_from(data).ok()?)?;
    }
    None
}

fn rar4(buf: &[u8]) -> Sniff {
    rar4_headers(buf).unwrap_or(Sniff::Plain)
}

fn rar4_headers(buf: &[u8]) -> Option<Sniff> {
    const MAIN_HEAD: u8 = 0x73;
    const FILE_HEAD: u8 = 0x74;
    const MHD_VOLUME: u16 = 0x0001;
    const MHD_PASSWORD: u16 = 0x0080;
    const MHD_FIRSTVOLUME: u16 = 0x0100;
    const LHD_PASSWORD: u16 = 0x0004;
    const LONG_BLOCK: u16 = 0x8000;
    let u16_at = |at: usize| -> Option<u16> {
        Some(u16::from_le_bytes(buf.get(at..at + 2)?.try_into().ok()?))
    };
    let mut pos = 0usize;
    for _ in 0..16 {
        let kind = *buf.get(pos + 2)?;
        let flags = u16_at(pos + 3)?;
        let size = usize::from(u16_at(pos + 5)?);
        if size < 7 {
            return None;
        }
        match kind {
            MAIN_HEAD => {
                // RAR 3+ marks the first volume. (RAR 2 volumes carry no
                // such flag and are left to unpack.)
                if flags & MHD_VOLUME != 0 && flags & MHD_FIRSTVOLUME == 0 {
                    return Some(Sniff::Plain);
                }
                if flags & MHD_PASSWORD != 0 {
                    return Some(Sniff::Encrypted(ArchiveKind::Rar));
                }
            }
            FILE_HEAD => {
                return Some(if flags & LHD_PASSWORD != 0 {
                    Sniff::Encrypted(ArchiveKind::Rar)
                } else {
                    Sniff::Plain
                });
            }
            _ => {}
        }
        let add = if flags & LONG_BLOCK != 0 {
            u32::from_le_bytes(buf.get(pos + 7..pos + 11)?.try_into().ok()?) as usize
        } else {
            0
        };
        pos = pos.checked_add(size)?.checked_add(add)?;
    }
    None
}

/// A 7z archive keeps its header at the end, located by the start header.
/// It is judged only when that header lies inside this file (a whole,
/// unsplit archive) and names the AES coder — for an encrypted header
/// (`kEncodedHeader`) or for encrypted file streams alike, the coder id is
/// stored in the clear.
fn sevenzip(path: &Path, head: &[u8], len: u64) -> Sniff {
    let field = |at: usize| -> Option<u64> {
        Some(u64::from_le_bytes(head.get(at..at + 8)?.try_into().ok()?))
    };
    let (Some(offset), Some(size)) = (field(12), field(20)) else {
        return Sniff::Plain;
    };
    let Some(start) = offset.checked_add(32) else {
        return Sniff::Plain;
    };
    if size == 0 || size > SNIFF_BYTES || start.checked_add(size).is_none_or(|end| end > len) {
        return Sniff::Plain;
    }
    let mut header = vec![0u8; size as usize];
    let read = std::fs::File::open(path).and_then(|mut file| {
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut header)
    });
    if read.is_err() {
        return Sniff::Plain;
    }
    if header
        .windows(SEVENZIP_AES.len())
        .any(|w| w == SEVENZIP_AES)
    {
        Sniff::Encrypted(ArchiveKind::SevenZip)
    } else {
        Sniff::Plain
    }
}

/// The post-processing side of [`ArchiveProbe`]: this node's `[post]`
/// extractors, unpack rules and password file.
pub struct EncryptedArchiveProbe {
    cfg: PostConfig,
}

impl EncryptedArchiveProbe {
    pub fn new(cfg: PostConfig) -> EncryptedArchiveProbe {
        EncryptedArchiveProbe { cfg }
    }

    fn unpacks(&self, category: Option<&str>) -> bool {
        self.cfg
            .rule_for(category)
            .and_then(|rule| rule.unpack)
            .unwrap_or(self.cfg.unpack)
    }

    /// Whether every known password is refused, in unpack's order: the
    /// release's own first (none: one attempt without a password), then the
    /// password file, read only once those are refused. `None` when the
    /// extractor or the password file could not answer.
    async fn all_refused(
        &self,
        path: &Path,
        kind: ArchiveKind,
        release: &[String],
        cancel: CancellationToken,
    ) -> Option<bool> {
        let ex = Extractors {
            unrar_cmd: self.cfg.unrar_cmd.clone(),
            sevenzip_cmd: self.cfg.sevenzip_cmd.clone(),
            timeout: self.cfg.tool_timeout,
        };
        let mut tried: Vec<Option<String>> = if release.is_empty() {
            vec![None]
        } else {
            release.iter().cloned().map(Some).collect()
        };
        for candidate in &tried {
            if !ex
                .refuses_password(path, kind, candidate.as_deref(), cancel.clone())
                .await
                .ok()?
            {
                return Some(false);
            }
        }
        for candidate in crate::manager::password_file_candidates(&self.cfg).ok()? {
            if tried
                .iter()
                .any(|seen| seen.as_deref() == Some(candidate.as_str()))
            {
                continue;
            }
            if cancel.is_cancelled() {
                return None;
            }
            if !ex
                .refuses_password(path, kind, Some(&candidate), cancel.clone())
                .await
                .ok()?
            {
                return Some(false);
            }
            tried.push(Some(candidate));
        }
        Some(true)
    }
}

impl ArchiveProbe for EncryptedArchiveProbe {
    fn probe(
        &self,
        file: &Path,
        job: &ArchiveProbeJob,
        cancel: &CancellationToken,
    ) -> ArchiveVerdict {
        if !self.unpacks(job.category.as_deref()) {
            return ArchiveVerdict::Opens; // nothing will unpack it
        }
        let Sniff::Encrypted(kind) = sniff(file) else {
            return ArchiveVerdict::Plain;
        };
        // This runs on the engine's blocking pool; the extractor is a child
        // process with its own small runtime so the probe needs nothing from
        // the caller's.
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return ArchiveVerdict::Opens;
        };
        let path: PathBuf = file.to_path_buf();
        match runtime.block_on(self.all_refused(&path, kind, &job.passwords, cancel.clone())) {
            Some(true) => {
                tracing::warn!(file = %path.display(), "encrypted archive refused every known password");
                ArchiveVerdict::Locked(format!(
                    "{} — found in its first volume, so the download was stopped",
                    crate::manager::ENCRYPTED_ARCHIVE_DIAGNOSTIC
                ))
            }
            _ => ArchiveVerdict::Opens,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rar5_header(kind: u8, flags: u8, body: &[u8], extra: &[u8]) -> Vec<u8> {
        let mut inner = vec![kind, flags];
        if flags & 0x01 != 0 {
            inner.push(extra.len() as u8);
        }
        inner.extend_from_slice(body);
        inner.extend_from_slice(extra);
        let mut out = vec![0, 0, 0, 0, inner.len() as u8];
        out.extend(inner);
        out
    }

    fn rar5(headers: &[Vec<u8>]) -> Vec<u8> {
        let mut out = RAR5_SIGNATURE.to_vec();
        for h in headers {
            out.extend_from_slice(h);
        }
        out
    }

    fn write(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }

    #[test]
    fn rar5_header_encryption_is_encrypted() {
        let tmp = tempfile::tempdir().unwrap();
        let bytes = rar5(&[rar5_header(4, 0, &[0, 0, 15], &[])]);
        let path = write(tmp.path(), "x", &bytes);
        assert_eq!(sniff(&path), Sniff::Encrypted(ArchiveKind::Rar));
    }

    #[test]
    fn rar5_encrypted_file_entry_is_encrypted_only_on_the_first_volume() {
        let tmp = tempfile::tempdir().unwrap();
        // File header: flags HAS_EXTRA; extra = one encryption record.
        let file = rar5_header(2, 0x01, &[0, 0, 0], &[2, 0x01, 0]);
        let first = rar5(&[rar5_header(1, 0, &[0x01], &[]), file.clone()]);
        let later = rar5(&[rar5_header(1, 0, &[0x03, 2], &[]), file]);
        let plain = rar5(&[
            rar5_header(1, 0, &[0x01], &[]),
            rar5_header(2, 0x01, &[0, 0, 0], &[2, 0x02, 0]),
        ]);
        assert_eq!(
            sniff(&write(tmp.path(), "a", &first)),
            Sniff::Encrypted(ArchiveKind::Rar)
        );
        assert_eq!(sniff(&write(tmp.path(), "b", &later)), Sniff::Plain);
        assert_eq!(sniff(&write(tmp.path(), "c", &plain)), Sniff::Plain);
    }

    fn rar4_block(kind: u8, flags: u16, body: &[u8]) -> Vec<u8> {
        let size = (7 + body.len()) as u16;
        let mut out = vec![0, 0, kind];
        out.extend_from_slice(&flags.to_le_bytes());
        out.extend_from_slice(&size.to_le_bytes());
        out.extend_from_slice(body);
        out
    }

    #[test]
    fn rar4_flags_decide_encryption_and_first_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let main = |flags| rar4_block(0x73, flags, &[0; 6]);
        // File header with LONG_BLOCK: PACK_SIZE 0 so no data follows.
        let file = |flags: u16| rar4_block(0x74, 0x8000 | flags, &[0; 25]);
        let mk = |blocks: Vec<Vec<u8>>| {
            let mut out = RAR4_SIGNATURE.to_vec();
            for b in blocks {
                out.extend(b);
            }
            out
        };
        let cases = [
            (mk(vec![main(0x0080)]), Sniff::Encrypted(ArchiveKind::Rar)),
            (
                mk(vec![main(0x0101), file(0x0004)]),
                Sniff::Encrypted(ArchiveKind::Rar),
            ),
            (mk(vec![main(0x0001), file(0x0004)]), Sniff::Plain),
            (mk(vec![main(0x0000), file(0x0000)]), Sniff::Plain),
        ];
        for (i, (bytes, want)) in cases.into_iter().enumerate() {
            assert_eq!(
                sniff(&write(tmp.path(), &format!("r{i}"), &bytes)),
                want,
                "case {i}"
            );
        }
    }

    #[test]
    fn sevenzip_aes_coder_is_encrypted_when_the_header_is_inside() {
        let tmp = tempfile::tempdir().unwrap();
        let mk = |header: &[u8], declared: u64| {
            let mut out = SEVENZIP_SIGNATURE.to_vec();
            out.extend_from_slice(&[0, 4, 0, 0, 0, 0]); // version + start CRC
            out.extend_from_slice(&0u64.to_le_bytes()); // next header offset
            out.extend_from_slice(&declared.to_le_bytes());
            out.extend_from_slice(&[0; 4]);
            out.extend_from_slice(header);
            out
        };
        let aes = [0x17, 0x06, 0x24, 0x06, 0xf1, 0x07, 0x01, 0x00];
        assert_eq!(
            sniff(&write(tmp.path(), "a", &mk(&aes, aes.len() as u64))),
            Sniff::Encrypted(ArchiveKind::SevenZip)
        );
        // Header past the end of this part (a split archive): not judged.
        assert_eq!(
            sniff(&write(tmp.path(), "b", &mk(&aes, 4096))),
            Sniff::Plain
        );
        assert_eq!(
            sniff(&write(tmp.path(), "c", &mk(&[0x01, 0x04, 0x00], 3))),
            Sniff::Plain
        );
    }

    #[test]
    fn non_archives_and_truncated_headers_are_plain() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(sniff(&write(tmp.path(), "a", b"PAR2\0PKT")), Sniff::Plain);
        assert_eq!(sniff(&write(tmp.path(), "b", RAR5_SIGNATURE)), Sniff::Plain);
        assert_eq!(sniff(&tmp.path().join("missing")), Sniff::Plain);
    }
}
