//! Judge a finished archive volume during download: is it an encrypted
//! archive that no known password opens? (`nzbd_engine::archive_probe`.)
//!
//! Two steps, the cheap one first:
//!
//! 1. [`sniff`] reads the volume's own headers — content, never the name,
//!    because obfuscated posts hide extensions — and answers only for an
//!    archive that carries a **password verifier**: encrypted headers
//!    (RAR5's archive encryption header, RAR4's `MHD_PASSWORD`, 7z's
//!    AES-encoded header), which no wrong password decrypts, or a RAR5
//!    first volume whose file entry stores a password check value.
//!    Everything else is [`Sniff::Plain`] and costs one small read.
//! 2. Each known password is tried against that verifier with the
//!    configured extractor — a listing (`l`) for encrypted headers, a test
//!    (`t`) for a RAR5 check value — and only an **unambiguous** refusal
//!    counts ([`crate::tools::Extractors::refuses_password`]). Only when
//!    every candidate is refused is the volume locked.
//!
//! Wording such as "Corrupt file or wrong password" is never a refusal
//! here: damage in this volume or in a later one already on disk reads the
//! same, and that is par2's to repair. So what a verifier cannot settle
//! stays with unpack, which judges as before: a RAR4 set whose file data
//! alone is encrypted (a wrong password shows only as a checksum error at
//! the end of the file), 7z with encrypted streams but a plain header, a
//! split 7z (its header is in the last part).
use std::collections::HashSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use nzbd_engine::archive_probe::{ArchiveProbe, ArchiveProbeJob, ArchiveVerdict};
use nzbd_types::JobId;
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
    /// Encrypted with a password verifier; judge it with the extractor.
    /// `headers`: the headers themselves are encrypted, so a listing
    /// proves the password; otherwise a RAR5 file check value, which the
    /// test command checks before reading any data.
    Verifiable { kind: ArchiveKind, headers: bool },
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
    const PASSWORD_CHECK: u64 = 0x0001;
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
            ENCRYPTION_HEADER => {
                return Some(Sniff::Verifiable {
                    kind: ArchiveKind::Rar,
                    headers: true,
                })
            }
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
                        let _version = vint(records, &mut t)?;
                        let encryption_flags = vint(records, &mut t)?;
                        // Without a check value a wrong password shows only
                        // as a checksum error: not a verdict this can give.
                        return Some(if encryption_flags & PASSWORD_CHECK != 0 {
                            Sniff::Verifiable {
                                kind: ArchiveKind::Rar,
                                headers: false,
                            }
                        } else {
                            Sniff::Plain
                        });
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
                    return Some(Sniff::Verifiable {
                        kind: ArchiveKind::Rar,
                        headers: true,
                    });
                }
            }
            // Plain headers: encrypted file data (`LHD_PASSWORD`) has no
            // password check value in RAR4, so it is unpack's to judge.
            FILE_HEAD => return Some(Sniff::Plain),
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
/// unsplit archive) and is itself AES-encrypted: a `kEncodedHeader` whose
/// coder (stored in the clear) is AES. Encrypted streams under a plain
/// header have no verifier — a wrong password reads as a data error.
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
    const ENCODED_HEADER: u8 = 0x17;
    if header.first() == Some(&ENCODED_HEADER)
        && header
            .windows(SEVENZIP_AES.len())
            .any(|w| w == SEVENZIP_AES)
    {
        Sniff::Verifiable {
            kind: ArchiveKind::SevenZip,
            headers: true,
        }
    } else {
        Sniff::Plain
    }
}

/// How long one candidate may take. A refusal is immediate (a listing of
/// encrypted headers, or the RAR5 check value read before any data); only a
/// password that opens the archive goes on to read data, and stopping that
/// early changes nothing — it was not a refusal. So the bound costs no
/// verdict, and it keeps a probe from holding the job's terminal-use guard
/// for the configured extraction timeout.
const CANDIDATE_TIMEOUT: Duration = Duration::from_secs(60);

/// The post-processing side of [`ArchiveProbe`]: this node's `[post]`
/// extractors, unpack rules and password file.
pub struct EncryptedArchiveProbe {
    cfg: PostConfig,
    /// Jobs with an extractor run in flight. Every volume of a
    /// header-encrypted RAR5 set is verifiable and the password is the same
    /// for all, so one judgement per job at a time is enough.
    judging: Mutex<HashSet<JobId>>,
}

/// Releases a job's [`EncryptedArchiveProbe::judging`] claim.
struct Claim<'a> {
    judging: &'a Mutex<HashSet<JobId>>,
    job: JobId,
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        self.judging
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.job);
    }
}

impl EncryptedArchiveProbe {
    pub fn new(cfg: PostConfig) -> EncryptedArchiveProbe {
        EncryptedArchiveProbe {
            cfg,
            judging: Mutex::new(HashSet::new()),
        }
    }

    fn unpacks(&self, category: Option<&str>) -> bool {
        self.cfg
            .rule_for(category)
            .and_then(|rule| rule.unpack)
            .unwrap_or(self.cfg.unpack)
    }

    fn claim(&self, job: JobId) -> Option<Claim<'_>> {
        self.judging
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(job)
            .then_some(Claim {
                judging: &self.judging,
                job,
            })
    }

    /// Whether every known password is refused, in unpack's order: the
    /// release's own first (none: one attempt without a password), then the
    /// password file, read only once those are refused. `None` when the
    /// extractor or the password file could not answer.
    async fn all_refused(
        &self,
        path: &Path,
        sniffed: (ArchiveKind, bool),
        release: &[String],
        cancel: CancellationToken,
    ) -> Option<bool> {
        let (kind, headers) = sniffed;
        let ex = Extractors {
            unrar_cmd: self.cfg.unrar_cmd.clone(),
            sevenzip_cmd: self.cfg.sevenzip_cmd.clone(),
            timeout: self.cfg.tool_timeout.min(CANDIDATE_TIMEOUT),
        };
        let mut tried: Vec<Option<String>> = if release.is_empty() {
            vec![None]
        } else {
            release.iter().cloned().map(Some).collect()
        };
        let refused = |candidate: Option<String>| {
            let ex = &ex;
            let cancel = cancel.clone();
            async move {
                if cancel.is_cancelled() {
                    return None;
                }
                ex.refuses_password(path, kind, headers, candidate.as_deref(), cancel)
                    .await
                    .ok()
            }
        };
        for candidate in &tried {
            if !refused(candidate.clone()).await? {
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
            if !refused(Some(candidate.clone())).await? {
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
        let Sniff::Verifiable { kind, headers } = sniff(file) else {
            return ArchiveVerdict::Plain;
        };
        // Another volume of this job is being judged; its verdict stands
        // for the job.
        let Some(_claim) = self.claim(job.job) else {
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
        match runtime.block_on(self.all_refused(
            &path,
            (kind, headers),
            &job.passwords,
            cancel.clone(),
        )) {
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
        assert_eq!(sniff(&path), HEADERS_RAR);
    }

    const HEADERS_RAR: Sniff = Sniff::Verifiable {
        kind: ArchiveKind::Rar,
        headers: true,
    };

    #[test]
    fn rar5_file_check_value_is_verifiable_only_on_the_first_volume() {
        let tmp = tempfile::tempdir().unwrap();
        // File header: flags HAS_EXTRA; extra = one encryption record
        // (size 3: type 1, version 0, flags 1 = password check present).
        let file = rar5_header(2, 0x01, &[0, 0, 0], &[3, 0x01, 0, 0x01]);
        // The same record without a check value cannot be judged.
        let unchecked = rar5(&[
            rar5_header(1, 0, &[0x01], &[]),
            rar5_header(2, 0x01, &[0, 0, 0], &[3, 0x01, 0, 0x00]),
        ]);
        let first = rar5(&[rar5_header(1, 0, &[0x01], &[]), file.clone()]);
        let later = rar5(&[rar5_header(1, 0, &[0x03, 2], &[]), file]);
        let plain = rar5(&[
            rar5_header(1, 0, &[0x01], &[]),
            rar5_header(2, 0x01, &[0, 0, 0], &[2, 0x02, 0]),
        ]);
        assert_eq!(
            sniff(&write(tmp.path(), "a", &first)),
            Sniff::Verifiable {
                kind: ArchiveKind::Rar,
                headers: false
            }
        );
        assert_eq!(sniff(&write(tmp.path(), "b", &later)), Sniff::Plain);
        assert_eq!(sniff(&write(tmp.path(), "d", &unchecked)), Sniff::Plain);
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
    fn rar4_only_encrypted_headers_are_verifiable() {
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
            (mk(vec![main(0x0080)]), HEADERS_RAR),
            (mk(vec![main(0x0181)]), HEADERS_RAR),
            // A later volume, header-encrypted or not.
            (mk(vec![main(0x0081)]), Sniff::Plain),
            // Encrypted file data has no check value in RAR4.
            (mk(vec![main(0x0101), file(0x0004)]), Sniff::Plain),
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
    fn sevenzip_is_verifiable_when_its_inside_header_is_aes_encoded() {
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
            Sniff::Verifiable {
                kind: ArchiveKind::SevenZip,
                headers: true
            }
        );
        // AES streams under a plain header: no verifier.
        let plain_header = [0x01, 0x04, 0x06, 0x24, 0x06, 0xf1, 0x07, 0x01];
        assert_eq!(
            sniff(&write(tmp.path(), "d", &mk(&plain_header, 8))),
            Sniff::Plain
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

    /// A fake extractor answering every run with `reply` (shell code) and
    /// logging `<command> <password>`; returns the probe and the log path.
    fn probe_with(root: &Path, reply: &str, password_file: Option<&str>) -> EncryptedArchiveProbe {
        use std::os::unix::fs::PermissionsExt;
        let tool = root.join("fake-unrar");
        std::fs::write(
            &tool,
            format!(
                "#!/bin/sh\nmode=$1; pw=\nfor a; do case \"$a\" in -p*) pw=\"${{a#-p}}\";; esac; done\n\
                 echo \"$mode $pw\" >> '{}'\n{reply}\n",
                root.join("log").display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o755)).unwrap();
        let password_file = password_file.map(|lines| {
            let path = root.join("passwords.txt");
            std::fs::write(&path, lines).unwrap();
            path
        });
        EncryptedArchiveProbe::new(PostConfig {
            unrar_cmd: tool.to_string_lossy().into_owned(),
            sevenzip_cmd: tool.to_string_lossy().into_owned(),
            password_file,
            ..PostConfig::default()
        })
    }

    fn verdict(root: &Path, probe: &EncryptedArchiveProbe, passwords: &[&str]) -> ArchiveVerdict {
        // A header-encrypted RAR5 volume under a name that contains
        // "password", the way a missing next volume is named on stderr.
        let file = write(
            root,
            "the.password.game.part1.rar",
            &rar5(&[rar5_header(4, 0, &[0, 0, 15], &[])]),
        );
        probe.probe(
            &file,
            &ArchiveProbeJob {
                job: JobId(9),
                category: None,
                passwords: passwords.iter().map(|p| p.to_string()).collect(),
            },
            &CancellationToken::new(),
        )
    }

    #[test]
    fn only_an_unambiguous_refusal_locks() {
        let refused = "echo 'The specified password is incorrect.' >&2; exit 11";
        let tmp = tempfile::tempdir().unwrap();
        let probe = probe_with(tmp.path(), refused, None);
        assert!(matches!(
            verdict(tmp.path(), &probe, &["wrong"]),
            ArchiveVerdict::Locked(_)
        ));
        // Damage reads like a wrong password but is par2's to repair; a
        // missing next volume names its (password-containing) path.
        for reply in [
            "echo 'Checksum error in the encrypted file x. Corrupt file or wrong password.' >&2; exit 3",
            "echo \"Cannot find volume $PWD/the.password.game.part2.rar\" >&2; exit 10",
            "exit 0",
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let probe = probe_with(tmp.path(), reply, None);
            assert_eq!(
                verdict(tmp.path(), &probe, &["right"]),
                ArchiveVerdict::Opens,
                "{reply}"
            );
        }
    }

    #[test]
    fn password_file_candidates_follow_the_release_password() {
        let tmp = tempfile::tempdir().unwrap();
        let reply = "[ \"$pw\" = second ] && exit 0; echo 'The specified password is incorrect.' >&2; exit 11";
        let probe = probe_with(tmp.path(), reply, Some("first\nwrong\nsecond\nthird\n"));
        assert_eq!(
            verdict(tmp.path(), &probe, &["wrong"]),
            ArchiveVerdict::Opens
        );
        let log = std::fs::read_to_string(tmp.path().join("log")).unwrap();
        assert_eq!(log, "l wrong\nl first\nl second\n");
        // No usable candidate at all: locked without the release's own.
        let tmp = tempfile::tempdir().unwrap();
        let refused = "echo 'The specified password is incorrect.' >&2; exit 11";
        let probe = probe_with(tmp.path(), refused, None);
        assert!(matches!(
            verdict(tmp.path(), &probe, &[]),
            ArchiveVerdict::Locked(_)
        ));
        let log = std::fs::read_to_string(tmp.path().join("log")).unwrap();
        assert_eq!(log, "l -\n");
    }

    #[test]
    fn unpack_disabled_category_is_never_judged() {
        let tmp = tempfile::tempdir().unwrap();
        let mut probe = probe_with(tmp.path(), "exit 11", None);
        probe.cfg.unpack = false;
        assert_eq!(
            verdict(tmp.path(), &probe, &["wrong"]),
            ArchiveVerdict::Opens
        );
        assert!(!tmp.path().join("log").exists());
    }

    #[test]
    fn non_archives_and_truncated_headers_are_plain() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(sniff(&write(tmp.path(), "a", b"PAR2\0PKT")), Sniff::Plain);
        assert_eq!(sniff(&write(tmp.path(), "b", RAR5_SIGNATURE)), Sniff::Plain);
        assert_eq!(sniff(&tmp.path().join("missing")), Sniff::Plain);
    }
}
