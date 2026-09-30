//! Hardened subprocess runner + the par2/unrar/7z tool wrappers
//! (ARCHITECTURE.md §9): argv-only, scrubbed env, timeout, output caps,
//! kill-on-drop.

use crate::{ArchiveKind, ExtractOutcome, PostError, RepairResult, VerifyResult};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncReadExt;

pub const OUTPUT_CAP: usize = 256 * 1024;

#[derive(Debug)]
pub struct ToolOutput {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
}

/// Run a tool with hardening. `PostError::ToolMissing` on spawn failure.
pub async fn run_tool(
    cmd: &str,
    args: &[&str],
    cwd: &Path,
    timeout: Duration,
) -> Result<ToolOutput, PostError> {
    let mut child = tokio::process::Command::new(cmd)
        .args(args)
        .current_dir(cwd)
        .env_clear()
        .env(
            "PATH",
            std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".into()),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| PostError::ToolMissing(format!("{cmd}: {e}")))?;

    let out = child.stdout.take().unwrap();
    let mut err = child.stderr.take().unwrap();
    let read_capped = |mut r: tokio::process::ChildStdout| async move {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            match r.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if buf.len() < OUTPUT_CAP {
                        buf.extend_from_slice(&chunk[..n.min(OUTPUT_CAP - buf.len())]);
                    }
                }
            }
        }
        buf
    };
    let read_err = async {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            match err.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if buf.len() < OUTPUT_CAP {
                        buf.extend_from_slice(&chunk[..n.min(OUTPUT_CAP - buf.len())]);
                    }
                }
            }
        }
        buf
    };

    let result = tokio::time::timeout(timeout, async {
        let (o, e, status) = tokio::join!(read_capped(out), read_err, child.wait());
        (o, e, status)
    })
    .await;

    match result {
        Ok((o, e, Ok(status))) => Ok(ToolOutput {
            code: status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&o).into_owned(),
            stderr: String::from_utf8_lossy(&e).into_owned(),
        }),
        Ok((_, _, Err(e))) => Err(PostError::Subprocess(format!("{cmd}: {e}"))),
        Err(_) => {
            let _ = child.kill().await;
            Err(PostError::Subprocess(format!("{cmd}: timed out")))
        }
    }
}

// ---------------------------------------------------------------------------
// par2 (subprocess side; native quick-verify lives in par2.rs)
// ---------------------------------------------------------------------------

pub struct Par2Tool {
    pub cmd: String,
    pub timeout: Duration,
}

impl Par2Tool {
    /// Full verification via the subprocess (the fallback when quick
    /// verification reports damage). Parses par2cmdline's plain output.
    pub async fn verify_full(&self, main_par2: &Path) -> Result<VerifyResult, PostError> {
        let dir = main_par2.parent().unwrap_or(Path::new("."));
        let name = main_par2.file_name().unwrap().to_string_lossy();
        let out = run_tool(&self.cmd, &["verify", "-q", &name], dir, self.timeout).await?;
        let text = format!("{}\n{}", out.stdout, out.stderr);
        if nzbd_engine::is_out_of_space(&text) {
            return Err(PostError::Subprocess(text.chars().take(4096).collect()));
        }
        Ok(parse_verify_output(&text, out.code))
    }

    pub async fn repair(&self, main_par2: &Path) -> Result<RepairResult, PostError> {
        let dir = main_par2.parent().unwrap_or(Path::new("."));
        let name = main_par2.file_name().unwrap().to_string_lossy();
        let out = run_tool(&self.cmd, &["repair", "-q", &name], dir, self.timeout).await?;
        let text = format!("{}\n{}", out.stdout, out.stderr);
        if nzbd_engine::is_out_of_space(&text) {
            return Err(PostError::Subprocess(text.chars().take(4096).collect()));
        }
        if out.code == 0
            && (text.contains("Repair complete")
                || text.contains("repair is not required")
                || text.contains("All files are correct"))
        {
            Ok(RepairResult::Repaired)
        } else {
            tracing::warn!(code = out.code, "par2 repair failed: {}", text.trim());
            Ok(RepairResult::Failed)
        }
    }
}

pub fn parse_verify_output(text: &str, code: i32) -> VerifyResult {
    let grab = |needle: &str| -> Option<u32> {
        for line in text.lines() {
            if let Some(rest) = line.trim().strip_prefix("You ") {
                if rest.contains(needle) {
                    return rest.split_whitespace().find_map(|w| w.parse().ok());
                }
            }
        }
        None
    };
    if code == 0
        || text.contains("All files are correct")
        || text.contains("repair is not required")
    {
        return VerifyResult::Intact;
    }
    let available = grab("recovery blocks available").unwrap_or(0);
    if text.contains("Repair is possible") {
        return VerifyResult::Repairable {
            blocks_available: available,
            blocks_needed: 0,
        };
    }
    if let Some(needed) = grab("more recovery blocks") {
        return VerifyResult::NeedMoreBlocks {
            blocks_needed: needed,
        };
    }
    if text.contains("Repair is not possible") {
        // "not possible" without a block count = data beyond any repair
        return VerifyResult::Unrepairable;
    }
    VerifyResult::Unrepairable
}

// ---------------------------------------------------------------------------
// unrar / 7z extraction
// ---------------------------------------------------------------------------

pub struct Extractors {
    pub unrar_cmd: String,
    pub sevenzip_cmd: String,
    pub timeout: Duration,
}

/// The volume of a multi-volume set this file is, if it is one.
///
/// `name.part07.rar` → 7, `name.r05` → 7 (old-style counts the bare `.rar` as
/// volume 1 and `.r00` as volume 2), `name.rar` → 1. Anything else → None.
fn volume_number(name: &str) -> Option<u32> {
    if let Some(idx) = name.rfind(".part") {
        if name.ends_with(".rar") {
            let digits: String = name[idx + 5..]
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            return digits.parse().ok();
        }
    }
    if name.ends_with(".rar") {
        return Some(1);
    }
    let ext = name.rsplit_once('.')?.1;
    if ext.len() == 3 && ext.starts_with('r') && ext[1..].bytes().all(|b| b.is_ascii_digit()) {
        return ext[1..].parse::<u32>().ok().map(|n| n + 2);
    }
    None
}

/// The volumes missing from a set, by number.
///
/// Extraction has no completeness check of its own: unrar is handed a first
/// volume and trusted to walk the chain, and when the chain stops it writes
/// what it had. Checking before we start turns "a truncated film reported as
/// finished" into a named failure — and names the volume, because "volume 43
/// is missing" is actionable where "unpack failed" is not.
pub fn missing_volumes(dir: &Path, first: &Path) -> Vec<u32> {
    let stem = first
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_lowercase();
    // The set's base name: strip .partNN.rar, .rNN or .rar.
    let base = match stem.rfind(".part") {
        Some(i) if stem.ends_with(".rar") => stem[..i].to_owned(),
        _ => match stem.rsplit_once('.') {
            Some((b, _)) => b.to_owned(),
            None => stem.clone(),
        },
    };
    let mut seen: Vec<u32> = Vec::new();
    for p in files_of(dir) {
        if p.parent() != first.parent() {
            continue;
        }
        let name = p
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_lowercase();
        let matches_set = match name.rfind(".part") {
            Some(i) if name.ends_with(".rar") => name[..i] == base,
            _ => name.rsplit_once('.').is_some_and(|(b, _)| b == base),
        };
        if !matches_set {
            continue;
        }
        if let Some(n) = volume_number(&name) {
            seen.push(n);
        }
    }
    // One volume is not a set, and a set of one cannot have a gap.
    if seen.len() < 2 {
        return Vec::new();
    }
    seen.sort_unstable();
    seen.dedup();
    let highest = *seen.last().unwrap_or(&1);
    (1..=highest).filter(|n| !seen.contains(n)).collect()
}

/// Files (not directories) directly in `dir`.
fn files_of(dir: &Path) -> Vec<PathBuf> {
    crate::namespace::files(dir).unwrap_or_default()
}

/// Every file belonging to the same volume set as `first`, and their total
/// size on disk.
fn volume_set(dir: &Path, first: &Path) -> (usize, u64) {
    let stem = first
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_lowercase();
    let base = match stem.rfind(".part") {
        Some(i) if stem.ends_with(".rar") => stem[..i].to_owned(),
        _ => match stem.rsplit_once('.') {
            Some((b, _)) => b.to_owned(),
            None => stem.clone(),
        },
    };
    let mut count = 0usize;
    let mut bytes = 0u64;
    for p in files_of(dir) {
        if p.parent() != first.parent() {
            continue;
        }
        let name = p
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_lowercase();
        let in_set = match name.rfind(".part") {
            Some(i) if name.ends_with(".rar") => name[..i] == base,
            _ => name.rsplit_once('.').is_some_and(|(b, _)| b == base),
        };
        if in_set && volume_number(&name).is_some() {
            count += 1;
            bytes += std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
        }
    }
    (count, bytes)
}

/// Total size of everything under `dir`, recursively.
fn tree_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| {
            let p = e.path();
            if p.is_dir() {
                tree_bytes(&p)
            } else {
                std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0)
            }
        })
        .sum()
}

/// Did the extraction actually consume the whole volume set?
///
/// Returns the shortfall message when it plainly did not.
///
/// Extractors lie. `unrar-free` — the GPL clone Debian installs when you ask
/// for `unrar`, and what this image shipped until it was caught — cannot do
/// multi-volume archives and does not say so: given volume 1 of a five-volume
/// set with every volume present, it extracts volume 1, prints "All OK" and
/// exits 0. Nothing downstream could tell that from a real success, so a
/// 48 GiB film arrived as its first 500 MiB, marked complete.
///
/// The check is deliberately arithmetic rather than a parse of the archive's
/// declared contents: it needs no listing pass, no per-format output format,
/// and it holds for any extractor. Decompressed output is never meaningfully
/// smaller than the compressed input it came from, so output far below the
/// bytes fed in means volumes went unread. Stored media archives — what
/// nearly every release is — land within a hair of 1.0.
///
/// The 90% floor is slack for archive headers and recovery records, not a
/// tolerance for missing data: the failure this catches is off by whole
/// volumes, so anything near the boundary is already something else.
pub fn unpack_shortfall(dir: &Path, first: &Path, dest: &Path) -> Option<String> {
    let (volumes, packed) = volume_set(dir, first);
    if volumes < 2 || packed == 0 {
        return None; // not a set; nothing to be short of
    }
    let extracted = tree_bytes(dest);
    let floor = packed / 10 * 9;
    if extracted >= floor {
        return None;
    }
    Some(format!(
        "extracted {extracted} bytes from a {volumes}-volume set totalling {packed} \
         bytes — the extractor stopped early (an extractor without multi-volume \
         support reports success after the first volume; check `post.unrar_cmd`)"
    ))
}

/// First-volume archives in a directory, extraction-ordered.
pub fn detect_archives(dir: &Path) -> Vec<(PathBuf, ArchiveKind)> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    let mut names: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
    names.sort();
    for p in names {
        let name = p
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_lowercase();
        if name.ends_with(".rar") {
            // Multi-volume: only the first (partN with N==1, or bare .rar).
            if let Some(idx) = name.rfind(".part") {
                let num: String = name[idx + 5..]
                    .chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect();
                if num.parse::<u32>().map(|n| n != 1).unwrap_or(false) {
                    continue;
                }
            }
            out.push((p, ArchiveKind::Rar));
        } else if name.ends_with(".7z") {
            out.push((p, ArchiveKind::SevenZip));
        } else if name.ends_with(".zip") {
            out.push((p, ArchiveKind::Zip));
        } else if name.ends_with(".001") {
            out.push((p, ArchiveKind::Split));
        }
    }
    out
}

impl Extractors {
    pub async fn extract(
        &self,
        archive: &Path,
        kind: ArchiveKind,
        dest: &Path,
        password: Option<&str>,
    ) -> Result<ExtractOutcome, PostError> {
        std::fs::create_dir_all(dest)?;
        let dir = archive.parent().unwrap_or(Path::new("."));
        // RAR, then 7-Zip if RAR did not deliver.
        //
        // The fallback is not belt-and-braces, it is the working path on any
        // image that has `unrar-free` instead of the real thing: that clone
        // extracts volume 1 of a multi-volume set, prints "All OK" and exits
        // 0. 7-Zip reads the same archive correctly. Rather than demand a
        // particular binary, try the configured one and check the result — a
        // tool that under-delivers gets overruled by one that does not.
        if matches!(kind, ArchiveKind::Rar) {
            let first = self.extract_once(archive, kind, dest, password).await;
            let usable = match &first {
                Ok(o) => o.success && unpack_shortfall(dir, archive, dest).is_none(),
                Err(_) => false,
            };
            if usable {
                return first;
            }
            // Password failures are about the credential, not the tool, and
            // retrying with another extractor cannot help.
            if let Ok(o) = &first {
                if o.password_error || o.disk_space_error {
                    return first;
                }
            }
            tracing::warn!(
                archive = %archive.display(),
                "unrar did not deliver the whole archive; retrying with 7-Zip"
            );
            // Preserve prior outputs using exclusive names. Production callers
            // additionally allocate each retry in a journal-owned generation.
            let mut retained = false;
            for generation in 0..64 {
                let failed = dest.with_extension(if generation == 0 {
                    "first-attempt".to_string()
                } else {
                    format!("first-attempt-{generation}")
                });
                if std::fs::symlink_metadata(&failed).is_ok() {
                    continue;
                }
                nzbd_state::fileops::rename_exclusive(dest, &failed)
                    .map_err(|e| PostError::Subprocess(e.to_string()))?;
                retained = true;
                break;
            }
            if !retained {
                return Err(PostError::Subprocess(
                    "extractor attempt limit requires review".into(),
                ));
            }
            std::fs::create_dir(dest)?;
            let second = self
                .extract_once(archive, ArchiveKind::SevenZip, dest, password)
                .await;
            // The fallback gets the same delivery check the first attempt
            // got. "Everything is Ok" over a short extraction is the exact
            // lie this path exists to catch, and exempting the second tool
            // from the test would leave `extract` returning success for an
            // archive nothing read — the `unrar-free` failure again, one
            // tool along.
            let delivered = match &second {
                Ok(o) => o.success && unpack_shortfall(dir, archive, dest).is_none(),
                Err(_) => false,
            };
            if delivered {
                let mut outcome = second?;
                if let Ok(first) = first {
                    outcome.attempts.splice(0..0, first.attempts);
                }
                return Ok(outcome);
            }
            // Keep both attempts; the fallback is the actual last failing tool.
            return match second {
                Ok(mut o) => {
                    o.success = false;
                    match first {
                        Ok(first) => {
                            o.attempts.splice(0..0, first.attempts);
                        }
                        Err(e) => {
                            o.attempts.insert(
                                0,
                                crate::ExtractAttempt {
                                    executable: self.unrar_cmd.clone(),
                                    exit_code: -1,
                                    diagnostic: e.to_string().chars().take(4096).collect(),
                                },
                            );
                        }
                    }
                    Ok(o)
                }
                Err(e) => Err(e),
            };
        }
        self.extract_once(archive, kind, dest, password).await
    }

    async fn extract_once(
        &self,
        archive: &Path,
        kind: ArchiveKind,
        dest: &Path,
        password: Option<&str>,
    ) -> Result<ExtractOutcome, PostError> {
        std::fs::create_dir_all(dest)?;
        let dir = archive.parent().unwrap_or(Path::new("."));
        let arch = archive.to_string_lossy().into_owned();
        let dest_s = dest.to_string_lossy().into_owned();
        match kind {
            ArchiveKind::Rar => {
                let pw = match password {
                    Some(p) => format!("-p{p}"),
                    None => "-p-".into(),
                };
                let dest_slash = format!("{dest_s}/");
                // `-idc` silences the copyright banner but KEEPS the result
                // line. `-idq` suppressed everything, so the diagnostics below
                // could never fire: the exit code was the only evidence, and
                // unrar can stop at a missing volume having written a whole
                // one — a success by that measure and a truncated film by any
                // other.
                let args = [
                    "x",
                    "-y",
                    "-o+",
                    "-idc",
                    pw.as_str(),
                    arch.as_str(),
                    dest_slash.as_str(),
                ];
                let out = run_tool(&self.unrar_cmd, &args, dir, self.timeout).await?;
                let all = format!("{}\n{}", out.stdout, out.stderr);
                // NZBGet's unrar exit-code map: 11 = wrong password,
                // 5 = write/disk error, 0 = success.
                //
                // Exit 0 is necessary and not sufficient. unrar must also say
                // "All OK", exactly as 7z must say "Everything is Ok" in the
                // arm below — a rule this code enforced for one extractor and
                // not the other, which is the whole difference between
                // noticing a broken volume chain and shipping one volume.
                let broke_the_chain = ["Cannot find volume", "You need to start extraction"]
                    .iter()
                    .any(|m| all.contains(m));
                let lower = all.to_lowercase();
                let quota_error = lower.contains("quota exceeded") || lower.contains("disk quota");
                let capacity = nzbd_engine::is_out_of_space(&all)
                    || lower.contains("there is not enough space")
                    || lower.contains("disk full");
                let diagnostic = match password {
                    Some(secret) if !secret.is_empty() => all.replace(secret, "[redacted]"),
                    _ => all.clone(),
                }
                .chars()
                .take(4096)
                .collect();
                Ok(ExtractOutcome {
                    attempts: vec![crate::ExtractAttempt {
                        executable: if matches!(kind, ArchiveKind::Rar) {
                            self.unrar_cmd.clone()
                        } else {
                            self.sevenzip_cmd.clone()
                        },
                        exit_code: out.code,
                        diagnostic,
                    }],
                    quota_error,
                    success: out.code == 0 && all.contains("All OK") && !broke_the_chain,
                    password_error: out.code == 11
                        || out.stderr.contains("password")
                        || out.stdout.contains("password is incorrect"),
                    disk_space_error: capacity || quota_error || out.code == 5,
                })
            }
            ArchiveKind::SevenZip | ArchiveKind::Zip | ArchiveKind::Split => {
                let pw = format!("-p{}", password.unwrap_or(""));
                let dest_flag = format!("-o{dest_s}");
                let args = ["x", "-y", pw.as_str(), dest_flag.as_str(), arch.as_str()];
                let out = run_tool(&self.sevenzip_cmd, &args, dir, self.timeout).await?;
                let all = format!("{}\n{}", out.stdout, out.stderr);
                // 7z requires the literal success line (nzbget does the same).
                let lower = all.to_lowercase();
                let quota_error = lower.contains("quota exceeded") || lower.contains("disk quota");
                let capacity = nzbd_engine::is_out_of_space(&all)
                    || lower.contains("there is not enough space")
                    || lower.contains("disk full");
                let diagnostic = match password {
                    Some(secret) if !secret.is_empty() => all.replace(secret, "[redacted]"),
                    _ => all.clone(),
                }
                .chars()
                .take(4096)
                .collect();
                Ok(ExtractOutcome {
                    attempts: vec![crate::ExtractAttempt {
                        executable: if matches!(kind, ArchiveKind::Rar) {
                            self.unrar_cmd.clone()
                        } else {
                            self.sevenzip_cmd.clone()
                        },
                        exit_code: out.code,
                        diagnostic,
                    }],
                    quota_error,
                    success: out.code == 0 && all.contains("Everything is Ok"),
                    password_error: all.contains("Wrong password"),
                    disk_space_error: capacity || quota_error,
                })
            }
        }
    }
}

/// Test-only probe for an external tool. Returns `false` — and the calling
/// test self-skips with a notice — when the binary is missing, so a laptop
/// without `par2`/`7z` still passes the suite. Setting `NZBD_REQUIRE_TOOLS`
/// (CI does) turns a missing tool into a loud failure instead, so CI can
/// never silently lose coverage.
#[cfg(test)]
pub(crate) fn require_tool(tool: &str) -> bool {
    let found = std::process::Command::new(tool)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok();
    if found {
        return true;
    }
    if std::env::var_os("NZBD_REQUIRE_TOOLS").is_some() {
        panic!("`{tool}` is required because NZBD_REQUIRE_TOOLS is set — install it in this environment");
    }
    eprintln!(
        "SKIPPED: `{tool}` not installed — `brew install par2 p7zip` / `apt-get install par2 p7zip-full` for full local coverage"
    );
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    // These subprocess cases assert parsing, fallback, and filesystem
    // behavior rather than scheduler latency. Keep a generous hard ceiling so
    // a loaded runner cannot turn a successful one-line fixture into the
    // timeout outcome the assertion is deliberately not testing.
    const SUBPROCESS_TEST_TIMEOUT: Duration = Duration::from_secs(10);

    fn write_tool(path: &Path, body: &str) {
        std::fs::write(path, body).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    fn verify_output_parsing() {
        assert_eq!(
            parse_verify_output("All files are correct, repair is not needed.", 0),
            VerifyResult::Intact
        );
        let dmg = "Repair is required.\nYou have 8 recovery blocks available.\nRepair is possible.";
        assert_eq!(
            parse_verify_output(dmg, 1),
            VerifyResult::Repairable {
                blocks_available: 8,
                blocks_needed: 0
            }
        );
        let need = "Repair is required.\nYou have 0 recovery blocks available.\nYou need 3 more recovery blocks to be able to repair these files.";
        assert_eq!(
            parse_verify_output(need, 1),
            VerifyResult::NeedMoreBlocks { blocks_needed: 3 }
        );
    }

    #[test]
    fn archive_detection_first_volumes_only() {
        let tmp = tempfile::tempdir().unwrap();
        for n in [
            "a.part1.rar",
            "a.part2.rar",
            "b.rar",
            "c.zip",
            "d.7z",
            "e.001",
            "e.002",
            "x.txt",
        ] {
            std::fs::write(tmp.path().join(n), b"x").unwrap();
        }
        let found = detect_archives(tmp.path());
        let names: Vec<String> = found
            .iter()
            .map(|(p, _)| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            names,
            vec!["a.part1.rar", "b.rar", "c.zip", "d.7z", "e.001"]
        );
    }

    #[tokio::test]
    async fn sevenzip_zip_roundtrip_and_wrong_password() {
        if !require_tool("7z") {
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("in");
        std::fs::create_dir(&src).unwrap();
        std::fs::write(src.join("hello.txt"), b"hello world").unwrap();

        // plain zip (archive root = the file itself, like release archives)
        let st = std::process::Command::new("7z")
            .args(["a", "-tzip", "-y", "../plain.zip", "hello.txt"])
            .current_dir(&src)
            .output()
            .expect("7z required for tests (apt-get install p7zip-full)");
        assert!(st.status.success());

        // passworded zip
        let st = std::process::Command::new("7z")
            .args(["a", "-tzip", "-y", "-psecret", "../locked.zip", "hello.txt"])
            .current_dir(&src)
            .output()
            .unwrap();
        assert!(st.status.success());

        let ex = Extractors {
            unrar_cmd: "unrar".into(),
            sevenzip_cmd: "7z".into(),
            timeout: Duration::from_secs(30),
        };
        let out_dir = tmp.path().join("out");
        let r = ex
            .extract(
                &tmp.path().join("plain.zip"),
                ArchiveKind::Zip,
                &out_dir,
                None,
            )
            .await
            .unwrap();
        assert!(r.success);
        assert_eq!(
            std::fs::read(out_dir.join("hello.txt")).unwrap(),
            b"hello world"
        );

        let r = ex
            .extract(
                &tmp.path().join("locked.zip"),
                ArchiveKind::Zip,
                &tmp.path().join("out2"),
                Some("wrong"),
            )
            .await
            .unwrap();
        assert!(!r.success);
        assert!(r.password_error, "wrong password must be detected");

        let r = ex
            .extract(
                &tmp.path().join("locked.zip"),
                ArchiveKind::Zip,
                &tmp.path().join("out3"),
                Some("secret"),
            )
            .await
            .unwrap();
        assert!(r.success);
    }

    #[tokio::test]
    async fn missing_tool_is_tool_missing() {
        let err = run_tool(
            "definitely-not-a-tool-xyz",
            &[],
            Path::new("/tmp"),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, PostError::ToolMissing(_)));
    }

    #[tokio::test]
    async fn tool_runner_caps_output_and_kills_a_timed_out_child() {
        let tmp = tempfile::tempdir().unwrap();
        let out = run_tool(
            "/bin/sh",
            &["-c", "dd if=/dev/zero bs=300000 count=1 2>/dev/null"],
            tmp.path(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(out.code, 0);
        assert_eq!(out.stdout.len(), OUTPUT_CAP);

        let err = run_tool(
            "/bin/sh",
            &["-c", "while :; do :; done"],
            tmp.path(),
            Duration::from_millis(20),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, PostError::Subprocess(message) if message.contains("timed out")));
    }

    #[tokio::test]
    async fn par2_tool_reports_verify_and_repair_failures() {
        let tmp = tempfile::tempdir().unwrap();
        let tool = tmp.path().join("fake-par2");
        write_tool(
            &tool,
            "#!/bin/sh\n\
             if [ \"$1\" = verify ]; then\n\
               echo 'You need 4 more recovery blocks to repair these files.'\n\
               exit 1\n\
             fi\n\
             if [ \"$3\" = good.par2 ]; then\n\
               echo 'All files are correct'\n\
               exit 0\n\
             fi\n\
             echo 'repair failed' >&2\n\
             exit 2\n",
        );
        let par2 = Par2Tool {
            cmd: tool.to_string_lossy().into_owned(),
            timeout: SUBPROCESS_TEST_TIMEOUT,
        };

        assert_eq!(
            par2.verify_full(&tmp.path().join("set.par2"))
                .await
                .unwrap(),
            VerifyResult::NeedMoreBlocks { blocks_needed: 4 }
        );
        assert_eq!(
            par2.repair(&tmp.path().join("good.par2")).await.unwrap(),
            RepairResult::Repaired
        );
        assert_eq!(
            par2.repair(&tmp.path().join("bad.par2")).await.unwrap(),
            RepairResult::Failed
        );
        assert_eq!(
            parse_verify_output("Repair is not possible", 1),
            VerifyResult::Unrepairable
        );
        assert_eq!(
            parse_verify_output("unrecognised tool output", 1),
            VerifyResult::Unrepairable
        );
    }

    #[test]
    fn volume_sets_name_gaps_and_reject_short_extraction() {
        let missing = tempfile::tempdir().unwrap().path().join("missing");
        assert!(missing_volumes(&missing, &missing.join("set.part01.rar")).is_empty());
        assert!(detect_archives(&missing).is_empty());
        assert_eq!(tree_bytes(&missing), 0);

        let tmp = tempfile::tempdir().unwrap();
        let first = tmp.path().join("set.part01.rar");
        std::fs::write(&first, vec![0u8; 100]).unwrap();
        std::fs::write(tmp.path().join("set.part03.rar"), vec![0u8; 100]).unwrap();
        std::fs::write(tmp.path().join("other.part02.rar"), vec![0u8; 100]).unwrap();
        std::fs::create_dir(tmp.path().join("set.part02.rar")).unwrap();
        assert_eq!(missing_volumes(tmp.path(), &first), vec![2]);
        assert_eq!(volume_number("set.part07.rar"), Some(7));
        assert_eq!(volume_number("set.rar"), Some(1));
        assert_eq!(volume_number("set.r05"), Some(7));
        assert_eq!(volume_number("set.part.rar"), None);
        assert_eq!(volume_number("set.bin"), None);

        let dest = tmp.path().join("out");
        std::fs::create_dir_all(dest.join("nested")).unwrap();
        std::fs::write(dest.join("nested/partial.bin"), vec![0u8; 20]).unwrap();
        let shortfall = unpack_shortfall(tmp.path(), &first, &dest).unwrap();
        assert!(shortfall.contains("2-volume set totalling 200 bytes"));
        std::fs::write(dest.join("complete.bin"), vec![0u8; 180]).unwrap();
        assert_eq!(unpack_shortfall(tmp.path(), &first, &dest), None);

        let old_first = tmp.path().join("old.rar");
        std::fs::write(&old_first, vec![0u8; 80]).unwrap();
        std::fs::write(tmp.path().join("old.r00"), vec![0u8; 80]).unwrap();
        std::fs::write(tmp.path().join("old.r02"), vec![0u8; 80]).unwrap();
        assert_eq!(missing_volumes(tmp.path(), &old_first), vec![3]);
        assert!(unpack_shortfall(tmp.path(), &old_first, &dest).is_some());
        std::fs::write(dest.join("last-bytes.bin"), vec![0u8; 20]).unwrap();
        assert_eq!(unpack_shortfall(tmp.path(), &old_first, &dest), None);

        let single = tmp.path().join("single.rar");
        std::fs::write(&single, b"one").unwrap();
        assert_eq!(unpack_shortfall(tmp.path(), &single, &dest), None);
    }

    #[tokio::test]
    async fn rar_extraction_falls_back_but_not_for_a_bad_password() {
        let tmp = tempfile::tempdir().unwrap();
        let archive = tmp.path().join("single.rar");
        std::fs::write(&archive, b"rar data").unwrap();
        let unrar_ok = tmp.path().join("unrar-ok");
        write_tool(
            &unrar_ok,
            "#!/bin/sh\nfor dest; do :; done\nmkdir -p \"$dest\"\nprintf full > \"${dest}/payload.bin\"\necho 'All OK'\n",
        );
        let unused = tmp.path().join("unused-7z");
        write_tool(&unused, "#!/bin/sh\nexit 99\n");
        let extractors = Extractors {
            unrar_cmd: unrar_ok.to_string_lossy().into_owned(),
            sevenzip_cmd: unused.to_string_lossy().into_owned(),
            timeout: SUBPROCESS_TEST_TIMEOUT,
        };
        let dest = tmp.path().join("single-out");
        let outcome = extractors
            .extract(&archive, ArchiveKind::Rar, &dest, None)
            .await
            .unwrap();
        assert!(outcome.success);
        assert_eq!(std::fs::read(dest.join("payload.bin")).unwrap(), b"full");

        let bad_password = tmp.path().join("unrar-password");
        write_tool(
            &bad_password,
            "#!/bin/sh\necho 'password is incorrect' >&2\nexit 11\n",
        );
        let extractors = Extractors {
            unrar_cmd: bad_password.to_string_lossy().into_owned(),
            sevenzip_cmd: unused.to_string_lossy().into_owned(),
            timeout: SUBPROCESS_TEST_TIMEOUT,
        };
        let outcome = extractors
            .extract(
                &archive,
                ArchiveKind::Rar,
                &tmp.path().join("password-out"),
                Some("wrong"),
            )
            .await
            .unwrap();
        assert!(!outcome.success);
        assert!(outcome.password_error);
    }

    #[tokio::test]
    async fn truncated_rar_output_retries_with_sevenzip() {
        let tmp = tempfile::tempdir().unwrap();
        let first = tmp.path().join("set.part01.rar");
        std::fs::write(&first, vec![0u8; 100]).unwrap();
        std::fs::write(tmp.path().join("set.part02.rar"), vec![0u8; 100]).unwrap();
        let short_unrar = tmp.path().join("short-unrar");
        write_tool(
            &short_unrar,
            "#!/bin/sh\nfor dest; do :; done\nmkdir -p \"$dest\"\nprintf x > \"${dest}/partial.bin\"\necho 'All OK'\n",
        );
        // The fallback must actually deliver the set, not merely print the
        // success line: 200 packed bytes put the shortfall floor at 180.
        let good_7z = tmp.path().join("good-7z");
        write_tool(
            &good_7z,
            // 7-Zip's destination is the `-oDEST` flag, argument 4.
            "#!/bin/sh\ndest=${4#-o}\nmkdir -p \"$dest\"\nhead -c 200 /dev/zero > \"${dest}/whole.bin\"\necho 'Everything is Ok'\n",
        );
        let extractors = Extractors {
            unrar_cmd: short_unrar.to_string_lossy().into_owned(),
            sevenzip_cmd: good_7z.to_string_lossy().into_owned(),
            timeout: SUBPROCESS_TEST_TIMEOUT,
        };
        let dest = tmp.path().join("fallback-out");
        let outcome = extractors
            .extract(&first, ArchiveKind::Rar, &dest, None)
            .await
            .unwrap();
        assert!(
            outcome.success,
            "7-Zip fallback must replace a short extraction"
        );
        assert_eq!(
            std::fs::metadata(dest.join("whole.bin")).unwrap().len(),
            200,
            "the reported success must be the fallback's full-size output"
        );
        assert!(
            !dest.join("partial.bin").exists(),
            "the short first attempt's output was discarded before the retry"
        );
    }

    /// A fallback that also under-delivers is not a success.
    ///
    /// `Extractors::extract` used to shortfall-check only the first attempt,
    /// so a 7-Zip run that printed "Everything is Ok" over an empty
    /// destination was returned as a complete extraction — the whole failure
    /// the fallback exists to catch, arriving through the fallback itself.
    #[tokio::test]
    async fn sevenzip_fallback_that_is_also_short_is_not_a_success() {
        let tmp = tempfile::tempdir().unwrap();
        let first = tmp.path().join("set.part01.rar");
        std::fs::write(&first, vec![0u8; 100]).unwrap();
        std::fs::write(tmp.path().join("set.part02.rar"), vec![0u8; 100]).unwrap();
        let short_unrar = tmp.path().join("short-unrar");
        write_tool(
            &short_unrar,
            "#!/bin/sh\nfor dest; do :; done\nmkdir -p \"$dest\"\nprintf x > \"${dest}/partial.bin\"\necho 'All OK'\n",
        );
        // Prints the success line, writes nothing.
        let lying_7z = tmp.path().join("lying-7z");
        write_tool(&lying_7z, "#!/bin/sh\necho 'Everything is Ok'\n");
        let extractors = Extractors {
            unrar_cmd: short_unrar.to_string_lossy().into_owned(),
            sevenzip_cmd: lying_7z.to_string_lossy().into_owned(),
            timeout: SUBPROCESS_TEST_TIMEOUT,
        };
        let dest = tmp.path().join("both-short-out");
        let outcome = extractors
            .extract(&first, ArchiveKind::Rar, &dest, None)
            .await
            .unwrap();
        assert!(
            !outcome.success,
            "a fallback that read none of the volume set must not be reported as a complete extraction"
        );
        assert!(
            unpack_shortfall(tmp.path(), &first, &dest).is_some(),
            "the destination really is short, so the outcome above is the tool's claim being overruled"
        );
    }

    #[tokio::test]
    async fn extractors_preserve_the_first_failure_and_name_disk_exhaustion() {
        let tmp = tempfile::tempdir().unwrap();
        let archive = tmp.path().join("broken.rar");
        std::fs::write(&archive, b"rar data").unwrap();
        let failed = tmp.path().join("failed-tool");
        write_tool(&failed, "#!/bin/sh\necho failed >&2\nexit 2\n");

        // When both RAR attempts fail the merge keeps the FIRST outcome,
        // because that is the attempt whose password/disk flags were set by
        // the tool that actually understands the archive. Give the two tools
        // different failures so the choice is observable: unrar exits 5
        // (unrar's write/disk-error code), 7-Zip fails with no disk marker
        // at all. Merging the other way round would silently lose the
        // "the volume is full" diagnosis operators act on.
        let disk_full_unrar = tmp.path().join("disk-full-unrar");
        write_tool(
            &disk_full_unrar,
            "#!/bin/sh\necho 'cannot write' >&2\nexit 5\n",
        );
        let extractors = Extractors {
            unrar_cmd: disk_full_unrar.to_string_lossy().into_owned(),
            sevenzip_cmd: failed.to_string_lossy().into_owned(),
            timeout: SUBPROCESS_TEST_TIMEOUT,
        };
        let outcome = extractors
            .extract(
                &archive,
                ArchiveKind::Rar,
                &tmp.path().join("failed-out"),
                None,
            )
            .await
            .unwrap();
        assert!(!outcome.success);
        assert!(
            outcome.disk_space_error,
            "the merged outcome must carry the first attempt's disk flag; the 7-Zip fallback never set one"
        );

        let good_7z = tmp.path().join("fallback-7z");
        write_tool(&good_7z, "#!/bin/sh\necho 'Everything is Ok'\n");
        let missing_unrar = Extractors {
            unrar_cmd: tmp
                .path()
                .join("missing-unrar")
                .to_string_lossy()
                .into_owned(),
            sevenzip_cmd: good_7z.to_string_lossy().into_owned(),
            timeout: SUBPROCESS_TEST_TIMEOUT,
        };
        let outcome = missing_unrar
            .extract(
                &archive,
                ArchiveKind::Rar,
                &tmp.path().join("missing-unrar-out"),
                None,
            )
            .await
            .unwrap();
        assert!(outcome.success, "a missing unrar must fall back to 7-Zip");

        let disk_full = tmp.path().join("disk-full-7z");
        write_tool(
            &disk_full,
            "#!/bin/sh\necho 'There is not enough space' >&2\nexit 2\n",
        );
        let extractors = Extractors {
            unrar_cmd: failed.to_string_lossy().into_owned(),
            sevenzip_cmd: disk_full.to_string_lossy().into_owned(),
            timeout: SUBPROCESS_TEST_TIMEOUT,
        };
        let outcome = extractors
            .extract(
                &tmp.path().join("archive.zip"),
                ArchiveKind::Zip,
                &tmp.path().join("disk-out"),
                None,
            )
            .await
            .unwrap();
        assert!(!outcome.success);
        assert!(outcome.disk_space_error);
    }

    #[test]
    fn missing_required_tools_follow_the_ci_contract() {
        let missing = "definitely-not-a-required-tool-xyz";
        if std::env::var_os("NZBD_REQUIRE_TOOLS").is_some() {
            assert!(std::panic::catch_unwind(|| require_tool(missing)).is_err());
        } else {
            assert!(!require_tool(missing));
        }
    }
}
