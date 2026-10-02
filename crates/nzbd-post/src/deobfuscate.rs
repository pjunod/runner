//! Final-filename deobfuscation after evidence-based name recovery.
//!
//! A single dominant file may inherit the job name. Multiple similarly sized
//! files retain their names: lexical order of obfuscated names establishes
//! neither archive order nor episode identity.

use std::path::{Path, PathBuf};

/// Extensions never renamed (disc structures, recovery data, split
/// volumes) — mirrors SABnzbd's exclusion list.
const SKIP_EXTS: &[&str] = &[
    "vob", "rar", "par2", "mts", "m2ts", "cpi", "clpi", "mpl", "mpls", "bdm", "bdmv", "nzb", "sfv",
    "srr",
];

fn ext_of(p: &Path) -> String {
    p.extension()
        .map(|e| e.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default()
}

fn stem_of(p: &Path) -> String {
    p.file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn skip_ext(p: &Path) -> bool {
    let e = ext_of(p);
    if SKIP_EXTS.contains(&e.as_str()) {
        return true;
    }
    // Split volumes — `.r00`, `.z01`, `.001`. Shared with the signature
    // renamer on purpose: the two passes have to agree about what a volume
    // is, and they did not. A set survived this one and was mangled by the
    // other.
    crate::rename::split_volume_ext(&e)
}

/// `S01E02` / `1x02`-style tokens mean the name maps to an episode — it
/// is never treated as obfuscated, no matter what else it looks like.
fn episode_pattern(stem: &str) -> bool {
    let b = stem.as_bytes();
    for i in 0..b.len() {
        // SxxEyy (case-insensitive, 1-2 digit season and episode)
        if b[i] == b's' || b[i] == b'S' {
            let d = b[i + 1..].iter().take_while(|c| c.is_ascii_digit()).count();
            if (1..=2).contains(&d) && i + 1 + d < b.len() {
                let j = i + 1 + d;
                if (b[j] == b'e' || b[j] == b'E')
                    && b.get(j + 1).is_some_and(|c| c.is_ascii_digit())
                {
                    return true;
                }
            }
        }
        // NxNN ("1x02", "10x02")
        if b[i] == b'x'
            && i > 0
            && b[i - 1].is_ascii_digit()
            && b.get(i + 1).is_some_and(|c| c.is_ascii_digit())
            && b.get(i + 2).is_some_and(|c| c.is_ascii_digit())
        {
            return true;
        }
    }
    false
}

fn is_hex(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Strict tier: names that can only be machine noise. This is the gate
/// for multi-file (season pack) renames, where a false positive would
/// mangle many files at once.
pub fn is_definitely_obfuscated(stem: &str) -> bool {
    if episode_pattern(stem) {
        return false;
    }
    // 16+ hex digits and nothing else (covers the classic exactly-32 case)
    if stem.len() >= 16 && is_hex(stem) {
        return true;
    }
    // UUID: 8-4-4-4-12
    let parts: Vec<&str> = stem.split('-').collect();
    if parts.len() == 5
        && [8, 4, 4, 4, 12]
            .iter()
            .zip(&parts)
            .all(|(n, p)| p.len() == *n && is_hex(p))
    {
        return true;
    }
    // 40+ chars of lowercase hex and dots
    if stem.len() >= 40
        && stem
            .bytes()
            .all(|b| b == b'.' || b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return true;
    }
    // 30+ hex digits alongside 2+ bracketed sections
    let hex_digits = stem.bytes().filter(|b| b.is_ascii_hexdigit()).count();
    let opens = stem.bytes().filter(|b| *b == b'[').count();
    let closes = stem.bytes().filter(|b| *b == b']').count();
    if hex_digits >= 30 && opens >= 2 && closes >= 2 {
        return true;
    }
    stem.starts_with("abc.xyz")
}

/// SABnzbd's `is_probably_obfuscated`, ported: a handful of "definitely
/// noise" patterns, a handful of "clearly a meaningful name" patterns,
/// and an *obfuscated by default* fallthrough for everything else.
pub fn is_probably_obfuscated(stem: &str) -> bool {
    if episode_pattern(stem) {
        return false;
    }
    if is_definitely_obfuscated(stem) {
        return true;
    }
    let upper = stem.chars().filter(|c| c.is_ascii_uppercase()).count();
    let lower = stem.chars().filter(|c| c.is_ascii_lowercase()).count();
    let letters = upper + lower;
    let digits = stem.chars().filter(|c| c.is_ascii_digit()).count();
    let spacish = stem
        .chars()
        .filter(|c| matches!(c, ' ' | '.' | '_'))
        .count();
    // Meaningful-name patterns (SABnzbd's negatives):
    if upper >= 2 && lower >= 2 && spacish >= 1 {
        return false;
    }
    if spacish >= 3 {
        return false;
    }
    if letters >= 4 && digits >= 4 && spacish >= 1 {
        return false;
    }
    if stem.chars().next().is_some_and(|c| c.is_ascii_uppercase())
        && lower >= 2
        && upper as f64 / lower as f64 <= 0.25
    {
        return false;
    }
    true
}

fn unique_target(parent: &Path, stem: &str, suffix: &str) -> PathBuf {
    let first = parent.join(format!("{stem}{suffix}"));
    if !first.exists() {
        return first;
    }
    for n in 2.. {
        let cand = parent.join(format!("{stem} ({n}){suffix}"));
        if !cand.exists() {
            return cand;
        }
    }
    unreachable!()
}

/// Rename `path` from `old_stem` to `new_stem`, dragging along every
/// same-directory file that shares the stem prefix (`x.mkv` brings
/// `x.dut.srt`, `x-sample.mkv`, …). Suffixes are preserved verbatim.
fn rename_with_companions(
    old_stem: &str,
    new_stem: &str,
    parent: &Path,
    all_files: &[PathBuf],
    out: &mut Vec<(PathBuf, PathBuf)>,
    custody: crate::rename::Custody<'_>,
) -> Result<(), crate::PostError> {
    for f in all_files {
        if f.parent() != Some(parent) {
            continue;
        }
        let name = f.file_name().map(|n| n.to_string_lossy().into_owned());
        let Some(name) = name else { continue };
        let Some(suffix) = name.strip_prefix(old_stem) else {
            continue;
        };
        let target = unique_target(parent, new_stem, suffix);
        crate::rename::rename_owned(f, target.clone(), custody)?;
        out.push((f.clone(), target));
    }
    Ok(())
}

/// Container evidence restores a missing extension only. It does not identify
/// episodes or validate the whole stream; exact PAR names remain authoritative.
pub fn restore_media_extensions(
    dir: &Path,
    protected: &std::collections::HashSet<String>,
) -> Result<Vec<(PathBuf, PathBuf)>, crate::PostError> {
    restore_media_extensions_owned(dir, protected, None)
}
pub fn restore_media_extensions_owned(
    dir: &Path,
    protected: &std::collections::HashSet<String>,
    custody: crate::rename::Custody<'_>,
) -> Result<Vec<(PathBuf, PathBuf)>, crate::PostError> {
    use std::io::Read;
    fn vint(bytes: &[u8], at: &mut usize) -> Option<usize> {
        let first = *bytes.get(*at)?;
        let n = first.leading_zeros() as usize + 1;
        if n > 8 {
            return None;
        }
        let mut value = usize::from(first & (0xff >> n));
        *at += 1;
        for _ in 1..n {
            value = value
                .checked_mul(256)?
                .checked_add(usize::from(*bytes.get(*at)?))?;
            *at += 1;
        }
        Some(value)
    }
    fn kind(bytes: &[u8]) -> Option<&'static str> {
        if bytes.starts_with(&[0x1a, 0x45, 0xdf, 0xa3]) {
            let mut at = 4;
            let size = vint(bytes, &mut at)?;
            let end = at.checked_add(size)?;
            if end > bytes.len() {
                return None;
            }
            let bytes = &bytes[..end];
            while at < end {
                let start = at;
                let first = *bytes.get(at)?;
                let id_len = first.leading_zeros() as usize + 1;
                if id_len > 4 {
                    return None;
                }
                at = at.checked_add(id_len)?;
                let id = bytes.get(start..at)?;
                let len = vint(bytes, &mut at)?;
                let data = bytes.get(at..at.checked_add(len)?)?;
                if id == [0x42, 0x82] {
                    return match data {
                        b"matroska" => Some("mkv"),
                        b"webm" => Some("webm"),
                        _ => None,
                    };
                }
                at += len;
            }
        }
        if bytes.get(..4) == Some(b"RIFF") && bytes.get(8..12) == Some(b"AVI ") {
            return Some("avi");
        }
        if bytes.get(4..8) == Some(b"ftyp") {
            return match bytes.get(8..12)? {
                b"M4A " => Some("m4a"),
                b"isom" | b"iso2" | b"mp41" | b"mp42" | b"avc1" | b"M4V " => Some("mp4"),
                _ => None,
            };
        }
        None
    }
    let mut plan = Vec::new();
    for path in crate::namespace::files(dir)? {
        if path.extension().is_some()
            || protected.contains(
                &path
                    .strip_prefix(dir)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
            )
        {
            continue;
        }
        let mut bytes = Vec::new();
        nzbd_state::fileops::open(&path)
            .map_err(|e| crate::PostError::Subprocess(e.to_string()))?
            .take(4096)
            .read_to_end(&mut bytes)?;
        if let Some(ext) = kind(&bytes) {
            plan.push((path.clone(), path.with_extension(ext)));
        }
    }
    for (_, target) in &plan {
        if target.symlink_metadata().is_ok() {
            return Err(crate::PostError::Subprocess(format!(
                "media extension target exists: {}",
                target.display()
            )));
        }
    }
    for (source, target) in &plan {
        crate::rename::rename_owned(source, target.clone(), custody)?;
    }
    Ok(plan)
}

/// The final deobfuscation pass. Returns the applied `(from, to)` pairs.
///
/// `protected` holds filenames whose correctness is *proven* — the names
/// recorded inside the job's par2 set. Evidence always outranks the
/// heuristic: a protected file is never renamed, no matter how odd its
/// name looks (release groups do ship legitimately weird names).
pub fn deobfuscate_dir(
    dir: &Path,
    job_name: &str,
    protected: &std::collections::HashSet<String>,
) -> Result<Vec<(PathBuf, PathBuf)>, crate::PostError> {
    deobfuscate_dir_owned(dir, job_name, protected, None)
}
pub fn deobfuscate_dir_owned(
    dir: &Path,
    job_name: &str,
    protected: &std::collections::HashSet<String>,
    custody: crate::rename::Custody<'_>,
) -> Result<Vec<(PathBuf, PathBuf)>, crate::PostError> {
    let job_stem = job_name.trim().trim_end_matches(".nzb").trim();
    // A job whose *own* name is noise gives us nothing to rename toward.
    if job_stem.is_empty() || is_definitely_obfuscated(job_stem) {
        return Ok(Vec::new());
    }

    let Ok(files) = crate::namespace::files(dir) else {
        return Ok(Vec::new());
    };
    // Discovery shares the validated namespace; heuristic renaming remains
    // deliberately shallow and excludes every hidden path component.
    let files: Vec<_> = files
        .into_iter()
        .filter(|p| {
            p.strip_prefix(dir).is_ok_and(|relative| {
                relative.components().count() <= 6
                    && !relative
                        .components()
                        .any(|c| c.as_os_str().to_string_lossy().starts_with('.'))
            })
        })
        .collect();
    let is_protected = |p: &PathBuf| {
        p.strip_prefix(dir)
            .is_ok_and(|n| protected.contains(&n.to_string_lossy().into_owned()))
    };
    // Protected files stay in the candidate list — they anchor the
    // dominance math (a junk sidecar must not inherit the job name just
    // because the real main file is evidence-protected) — but are never
    // themselves renamed.
    let mut cands: Vec<(PathBuf, u64)> = files
        .iter()
        .filter(|p| !skip_ext(p))
        .map(|p| {
            let size = std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
            (p.clone(), size)
        })
        .collect();
    if cands.is_empty() {
        return Ok(Vec::new());
    }
    cands.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let mut renames = Vec::new();
    let dominant = cands.len() == 1 || cands[0].1 >= cands[1].1.saturating_mul(3);
    if dominant {
        let (path, _) = &cands[0];
        let stem = stem_of(path);
        if !is_protected(path)
            && is_probably_obfuscated(&stem)
            && !stem.eq_ignore_ascii_case(job_stem)
        {
            if let Some(parent) = path.parent() {
                rename_with_companions(&stem, job_stem, parent, &files, &mut renames, custody)?;
            }
        }
        return Ok(renames);
    }

    // A pack needs per-file evidence; never fabricate an episode sequence.
    Ok(renames)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_container_extensions_do_not_invent_pack_names_or_override_evidence() {
        let tmp = tempfile::tempdir().unwrap();
        let mkv = b"\x1a\x45\xdf\xa3\x8b\x42\x82\x88matroska";
        for name in ["random1", "random2", "exact"] {
            std::fs::write(tmp.path().join(name), mkv).unwrap();
        }
        std::fs::write(tmp.path().join("unknown"), b"not media").unwrap();
        let protected = ["exact".to_string()].into_iter().collect();
        assert_eq!(
            restore_media_extensions(tmp.path(), &protected)
                .unwrap()
                .len(),
            2
        );
        assert!(tmp.path().join("random1.mkv").is_file());
        assert!(tmp.path().join("random2.mkv").is_file());
        assert!(tmp.path().join("exact").is_file());
        assert!(tmp.path().join("unknown").is_file());
        std::fs::write(tmp.path().join("random1"), mkv).unwrap();
        assert!(restore_media_extensions(tmp.path(), &protected).is_err());
    }

    #[test]
    fn heuristics_definite_tier() {
        assert!(is_definitely_obfuscated("b082fa0beaa644d3aa01045d5b8d0b36"));
        assert!(is_definitely_obfuscated("deadbeefdeadbeef"));
        assert!(is_definitely_obfuscated(
            "123e4567-e89b-12d3-a456-426614174000"
        ));
        assert!(is_definitely_obfuscated("abc.xyz-release-4021"));
        // Short hex is not definite (could be a real word like "decade")
        assert!(!is_definitely_obfuscated("deadbeef00"));
        assert!(!is_definitely_obfuscated("Great.Movie.2026"));
        assert!(!is_definitely_obfuscated("s01e02"));
    }

    #[test]
    fn heuristics_probable_tier() {
        // Meaningful names survive
        assert!(!is_probably_obfuscated("Great.Movie.2026.1080p.WEB"));
        assert!(!is_probably_obfuscated("The.Show.S01E02.720p"));
        assert!(!is_probably_obfuscated("My Home Video"));
        assert!(!is_probably_obfuscated("show.1x02.name"));
        // Noise defaults to obfuscated (SABnzbd's aggressive fallthrough)
        assert!(is_probably_obfuscated("kqwjfhkwqjhf"));
        assert!(is_probably_obfuscated("deadbeefdeadbeef"));
    }

    #[test]
    fn dominant_file_renamed_with_companions() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("a1b2c3d4e5f6a7b8.mkv"), vec![0u8; 9000]).unwrap();
        std::fs::write(tmp.path().join("a1b2c3d4e5f6a7b8.dut.srt"), b"subs").unwrap();
        std::fs::write(tmp.path().join("readme.nfo"), b"nfo").unwrap();

        let renames =
            deobfuscate_dir(tmp.path(), "Great.Show.S02.1080p.WEB", &Default::default()).unwrap();
        assert_eq!(renames.len(), 2);
        assert!(tmp.path().join("Great.Show.S02.1080p.WEB.mkv").exists());
        assert!(tmp.path().join("Great.Show.S02.1080p.WEB.dut.srt").exists());
        assert!(tmp.path().join("readme.nfo").exists(), "nfo untouched");
    }

    #[test]
    fn real_names_left_alone() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("Actual.Release.Name.2026.mkv"),
            vec![0u8; 9000],
        )
        .unwrap();
        assert!(deobfuscate_dir(tmp.path(), "Job.Name", &Default::default())
            .unwrap()
            .is_empty());
        assert!(tmp.path().join("Actual.Release.Name.2026.mkv").exists());
    }

    #[test]
    fn season_pack_never_invents_episode_order() {
        let tmp = tempfile::tempdir().unwrap();
        for stem in ["9f8e7d6c5b4a3f2e", "1a2b3c4d5e6f7a8b", "deadbeefcafef00d"] {
            std::fs::write(tmp.path().join(format!("{stem}.mkv")), vec![0u8; 5000]).unwrap();
        }
        let renames = deobfuscate_dir(tmp.path(), "Show.S03.1080p", &Default::default()).unwrap();
        assert!(renames.is_empty());
        for stem in ["9f8e7d6c5b4a3f2e", "1a2b3c4d5e6f7a8b", "deadbeefcafef00d"] {
            assert!(tmp.path().join(format!("{stem}.mkv")).exists());
        }
    }

    #[test]
    fn mixed_pack_untouched() {
        // One real episode name proves the poster wasn't hiding names —
        // numbering the rest would destroy information.
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("Show.S03E01.1080p.mkv"), vec![0u8; 5000]).unwrap();
        std::fs::write(tmp.path().join("9f8e7d6c5b4a3f2e.mkv"), vec![0u8; 5000]).unwrap();
        assert!(
            deobfuscate_dir(tmp.path(), "Show.S03.1080p", &Default::default())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn protected_names_survive_the_pass() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("a1b2c3d4e5f6a7b8.mkv"), vec![0u8; 9000]).unwrap();
        let protected: std::collections::HashSet<String> =
            ["a1b2c3d4e5f6a7b8.mkv".to_string()].into_iter().collect();
        assert!(deobfuscate_dir(tmp.path(), "Job.Name", &protected)
            .unwrap()
            .is_empty());
        assert!(tmp.path().join("a1b2c3d4e5f6a7b8.mkv").exists());
    }

    #[test]
    fn obfuscated_job_name_disables_the_pass() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("a1b2c3d4e5f6a7b8.mkv"), vec![0u8; 9000]).unwrap();
        assert!(
            deobfuscate_dir(tmp.path(), "cafebabecafebabecafebabe", &Default::default())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn heuristic_boundaries_preserve_human_names() {
        assert!(is_definitely_obfuscated(
            "deadbeef.deadbeef.deadbeef.deadbeef.deadbeef"
        ));
        assert!(is_definitely_obfuscated(
            "[deadbeefdeadbeef][cafebabecafebabe]"
        ));
        assert!(!is_probably_obfuscated("one.two_three four"));
        assert!(!is_probably_obfuscated("abcd.1234"));
        assert!(!is_probably_obfuscated("Titlecaseword"));
        assert!(!is_definitely_obfuscated("Show.S1E2"));
    }

    #[test]
    fn nested_dominant_file_avoids_hidden_files_and_name_collisions() {
        let tmp = tempfile::tempdir().unwrap();
        let nested = tmp.path().join("nested");
        std::fs::create_dir(&nested).unwrap();
        std::fs::write(nested.join("a1b2c3d4e5f6a7b8.mkv"), vec![0u8; 9000]).unwrap();
        std::fs::write(nested.join("a1b2c3d4e5f6a7b8.srt"), b"subs").unwrap();
        std::fs::write(nested.join("Job.Name.mkv"), b"occupied").unwrap();
        std::fs::write(tmp.path().join("root-note.txt"), b"different parent").unwrap();
        std::fs::write(tmp.path().join(".hidden.mkv"), vec![0u8; 20_000]).unwrap();

        let renames = deobfuscate_dir(tmp.path(), "Job.Name.nzb", &Default::default()).unwrap();
        assert_eq!(renames.len(), 2);
        assert!(nested.join("Job.Name (2).mkv").exists());
        assert!(nested.join("Job.Name.srt").exists());
        assert!(nested.join("Job.Name.mkv").exists());
        assert!(tmp.path().join(".hidden.mkv").exists());
        assert!(tmp.path().join("root-note.txt").exists());
    }

    #[test]
    fn empty_or_unreadable_directories_are_noops() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(deobfuscate_dir(tmp.path(), "", &Default::default())
            .unwrap()
            .is_empty());
        assert!(deobfuscate_dir(
            &tmp.path().join("missing"),
            "Useful.Job.Name",
            &Default::default()
        )
        .unwrap()
        .is_empty());
        std::fs::write(tmp.path().join("piece.001"), b"split volume").unwrap();
        assert!(
            deobfuscate_dir(tmp.path(), "Useful.Job.Name", &Default::default())
                .unwrap()
                .is_empty()
        );

        let mut deep = tmp.path().to_path_buf();
        for part in ["a", "b", "c", "d", "e", "f", "g"] {
            deep.push(part);
            std::fs::create_dir(&deep).unwrap();
        }
        std::fs::write(deep.join("deadbeefdeadbeef.mkv"), b"too deep").unwrap();
        assert!(
            deobfuscate_dir(tmp.path(), "Useful.Job.Name", &Default::default())
                .unwrap()
                .is_empty()
        );
        assert!(deep.join("deadbeefdeadbeef.mkv").exists());
    }
}
