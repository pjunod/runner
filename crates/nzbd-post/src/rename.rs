//! par-rename + rar-rename (ARCHITECTURE.md §9): recover real filenames of
//! obfuscated posts before verification/unpack.
//!
//! - **par-rename**: par2 FileDesc packets carry each source file's name
//!   and the MD5 of its first 16 KiB. Any disk file whose 16k-hash matches
//!   a description is renamed to its true name. Obfuscated `.par2` files
//!   themselves are found by content (`PAR2\0PKT` magic), not extension.
//! - **rar-rename**: files whose *content* is a RAR/7z/zip volume but
//!   whose name hides it get an extension back. Multi-volume RAR sets are
//!   numbered in stem order (uploaders obfuscate consistently); RAR5
//!   internal volume numbers are honored when present.

use md5::{Digest, Md5};
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};

const PAR2_MAGIC: &[u8] = b"PAR2\0PKT";
const RAR_MAGIC: &[u8] = b"Rar!\x1a\x07"; // v4: +\x00, v5: +\x01\x00
const SEVENZIP_MAGIC: &[u8] = b"7z\xbc\xaf\x27\x1c";
const ZIP_MAGIC: &[u8] = b"PK\x03\x04";

fn head(path: &Path, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    let Ok(mut f) = std::fs::File::open(path) else {
        return Vec::new();
    };
    let mut read = 0;
    while read < n {
        match f.read(&mut buf[read..]) {
            Ok(0) => break,
            Ok(k) => read += k,
            Err(_) => break,
        }
    }
    buf.truncate(read);
    buf
}

fn md5_16k(path: &Path) -> Option<[u8; 16]> {
    let data = head(path, 16384);
    if data.is_empty() {
        return None;
    }
    let mut h = Md5::new();
    h.update(&data);
    Some(h.finalize().into())
}

fn files_of(dir: &Path) -> Vec<PathBuf> {
    crate::namespace::files(dir).unwrap_or_default()
}

pub(crate) fn full_md5(path: &Path) -> Option<[u8; 16]> {
    let mut file = nzbd_state::fileops::open(path).ok()?;
    let mut digest = Md5::new();
    let mut buf = [0; 65536];
    loop {
        let n = file.read(&mut buf).ok()?;
        if n == 0 {
            break;
        }
        digest.update(&buf[..n]);
    }
    Some(digest.finalize().into())
}

fn create_parents(root: &Path, target: &Path) -> std::io::Result<()> {
    let relative = target.strip_prefix(root).map_err(std::io::Error::other)?;
    nzbd_state::fileops::parents(root, relative).map_err(std::io::Error::other)
}

fn ext_is(p: &Path, ext: &str) -> bool {
    p.extension()
        .map(|e| e.eq_ignore_ascii_case(ext))
        .unwrap_or(false)
}

/// Rename a file, refusing to clobber. Returns the final path on success.
fn safe_rename(from: &Path, to: PathBuf) -> Option<(PathBuf, PathBuf)> {
    if from == to.as_path() || to.exists() {
        return None;
    }
    match nzbd_state::fileops::rename_exclusive(from, &to) {
        Ok(()) => {
            tracing::info!(from = %from.display(), to = %to.display(), "renamed");
            Some((from.to_path_buf(), to))
        }
        Err(e) => {
            tracing::warn!(from = %from.display(), error = %e, "rename failed");
            None
        }
    }
}

/// par-rename. Returns `(old, new)` pairs so the caller can remap download
/// evidence (whole-file CRCs are content-addressed; only paths change).
pub fn par_rename(dir: &Path) -> Vec<(PathBuf, PathBuf)> {
    par_rename_owned(dir, None)
}

pub fn par_rename_owned(
    dir: &Path,
    custody: Option<(&nzbd_state::artifacts::Inventory, u32)>,
) -> Vec<(PathBuf, PathBuf)> {
    let mut renames = Vec::new();

    // 1. Give obfuscated par2 files their extension back (by magic).
    for p in files_of(dir) {
        if !ext_is(&p, "par2") && head(&p, 8) == PAR2_MAGIC {
            let to = dir.join(format!(
                "{}.par2",
                p.file_stem().unwrap_or_default().to_string_lossy()
            ));
            if let Some(pair) = safe_rename(&p, to) {
                renames.push(pair);
            }
        }
    }

    // Prefixes narrow candidates; size and full digest establish intact identity.
    let Ok(sets) = crate::par2::load_sets(dir) else {
        return renames;
    };
    for set in sets {
        let mut wanted: HashMap<[u8; 16], Vec<&crate::par2::Par2File>> = HashMap::new();
        for f in &set.files {
            wanted.entry(f.md5_16k).or_default().push(f);
        }
        for p in files_of(dir) {
            if ext_is(&p, "par2") || p.extension().is_some_and(|e| e == "part") {
                continue;
            }
            let Some(hash) = md5_16k(&p) else {
                continue;
            };
            let Some(catalog) = wanted.get(&hash) else {
                continue;
            };
            let matches: Vec<_> = catalog
                .iter()
                .filter(|f| {
                    std::fs::metadata(&p).is_ok_and(|m| m.len() == f.length)
                        && full_md5(&p) == Some(f.md5_full)
                })
                .collect();
            if matches.len() != 1 {
                continue;
            }
            let f = matches[0];
            let Ok(relative) = crate::namespace::relative(&f.name) else {
                continue;
            };
            let target = dir.join(relative);
            if p == target {
                continue;
            }
            if create_parents(dir, target.parent().unwrap()).is_err() {
                continue;
            }
            if let Some((inventory, job)) = custody {
                if inventory.restore_file(job, &p, &target).is_ok() {
                    renames.push((p, target));
                }
            } else if let Some(pair) = safe_rename(&p, target) {
                renames.push(pair);
            }
        }
    }
    renames
}

/// RAR5 archives carry their volume number in the main archive header;
/// parse just enough (magic + one vint field walk) to extract it.
fn rar5_volume_number(data: &[u8]) -> Option<u64> {
    // RAR5 signature is 8 bytes: Rar!\x1a\x07\x01\x00
    if data.len() < 8 || &data[..7] != b"Rar!\x1a\x07\x01" {
        return None;
    }
    let mut pos = 8usize;
    let vint = |data: &[u8], pos: &mut usize| -> Option<u64> {
        let mut v = 0u64;
        for i in 0..10 {
            let b = *data.get(*pos)?;
            *pos += 1;
            v |= ((b & 0x7f) as u64) << (7 * i);
            if b & 0x80 == 0 {
                return Some(v);
            }
        }
        None
    };
    // Header: crc32(4) + size(vint) + type(vint) + flags(vint) …
    pos += 4;
    let _size = vint(data, &mut pos)?;
    let htype = vint(data, &mut pos)?;
    if htype != 1 {
        return None; // expected the main archive header
    }
    let hflags = vint(data, &mut pos)?;
    if hflags & 0x0001 != 0 {
        let _extra = vint(data, &mut pos)?;
    }
    let arcflags = vint(data, &mut pos)?;
    // 0x0001 = volume, 0x0002 = volume number field present
    if arcflags & 0x0002 != 0 {
        return vint(data, &mut pos); // 0-based volume number
    }
    if arcflags & 0x0001 != 0 {
        return Some(0); // first volume of a set
    }
    None
}

/// An extension that names a continuation volume of a set someone else has
/// already numbered.
///
/// Two shapes, and the rule is deliberately wider than the one case that bit
/// us:
///
///   - all digits — `.001`–`.999` (7z/split), and
///   - one letter then two digits — `.r00` (RAR), `.z01` (split ZIP), and
///     every other archiver that followed the same convention.
///
/// **These must never be renamed.** Every volume of a set — not just the
/// first — begins with the format's magic bytes, so a signature-based renamer
/// sees a whole old-style set as a pile of "hidden" archives and renumbers
/// them into `.partNN.rar`. That severs the chain from the real first volume
/// `name.rar`, which keeps its own extension because `rar` is a known one.
/// unrar then extracts volume 1, goes looking for `name.r00`, and finds it
/// renamed away.
///
/// The result is a file exactly one volume long — 500 MiB minus a header, for
/// a typical set — reported as a completed download, after which `cleanup_dir`
/// deletes the renamed husks and takes the recoverable data with them. It cost
/// two 40-60 GB remuxes before anyone noticed, because the extraction
/// "succeeded" in about a second and nothing compared the result to the job
/// size.
///
/// The rule errs wide on purpose. Declining to rename a genuinely obfuscated
/// file whose random extension happens to look like `a01` costs one unpack
/// that fails loudly; renaming a real volume corrupts the set silently. Those
/// are not comparable prices.
pub(crate) fn split_volume_ext(ext: &str) -> bool {
    if ext.len() != 3 {
        return false;
    }
    let b = ext.as_bytes();
    b.iter().all(|c| c.is_ascii_digit())
        || (b[0].is_ascii_alphabetic() && b[1].is_ascii_digit() && b[2].is_ascii_digit())
}

/// rar-rename (plus 7z/zip signatures). Returns `(old, new)` pairs.
pub fn rar_rename(dir: &Path) -> Vec<(PathBuf, PathBuf)> {
    let mut renames = Vec::new();
    let known = ["rar", "7z", "zip", "par2", "nzb", "sfv", "nfo", "srr"];
    let mut hidden_rars: Vec<(PathBuf, Option<u64>)> = Vec::new();

    for p in files_of(dir) {
        let ext = p
            .extension()
            .map(|e| e.to_string_lossy().to_lowercase())
            .unwrap_or_default();
        if known.contains(&ext.as_str()) || split_volume_ext(&ext) {
            continue;
        }
        let h = head(&p, 32);
        if h.starts_with(RAR_MAGIC) {
            hidden_rars.push((p, rar5_volume_number(&h)));
        } else if h.starts_with(SEVENZIP_MAGIC) {
            let to = dir.join(format!(
                "{}.7z",
                p.file_stem().unwrap_or_default().to_string_lossy()
            ));
            if let Some(pair) = safe_rename(&p, to) {
                renames.push(pair);
            }
        } else if h.starts_with(ZIP_MAGIC) {
            let to = dir.join(format!(
                "{}.zip",
                p.file_stem().unwrap_or_default().to_string_lossy()
            ));
            if let Some(pair) = safe_rename(&p, to) {
                renames.push(pair);
            }
        }
    }

    match hidden_rars.len() {
        0 => {}
        1 => {
            let (p, _) = &hidden_rars[0];
            let to = dir.join(format!(
                "{}.rar",
                p.file_stem().unwrap_or_default().to_string_lossy()
            ));
            if let Some(pair) = safe_rename(p, to) {
                renames.push(pair);
            }
        }
        _ => {
            // Multi-volume: RAR5 volume numbers when available, else stem
            // order (obfuscation is applied uniformly, preserving order).
            let base = hidden_rars[0]
                .0
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned();
            let numbered = hidden_rars.iter().all(|(_, n)| n.is_some());
            let mut ordered: Vec<PathBuf> = if numbered {
                let mut v = hidden_rars.clone();
                v.sort_by_key(|(_, n)| n.unwrap());
                v.into_iter().map(|(p, _)| p).collect()
            } else {
                hidden_rars.iter().map(|(p, _)| p.clone()).collect()
            };
            ordered.sort_by_key(|p| {
                hidden_rars
                    .iter()
                    .position(|(q, _)| q == p)
                    .unwrap_or(usize::MAX)
            });
            if numbered {
                let mut v = hidden_rars.clone();
                v.sort_by_key(|(_, n)| n.unwrap());
                ordered = v.into_iter().map(|(p, _)| p).collect();
            }
            for (i, p) in ordered.iter().enumerate() {
                let to = dir.join(format!("{base}.part{:02}.rar", i + 1));
                if let Some(pair) = safe_rename(p, to) {
                    renames.push(pair);
                }
            }
        }
    }
    renames
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn par_rename_recovers_obfuscated_names() {
        if !crate::tools::require_tool("par2") {
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let data: Vec<u8> = (0..50_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(tmp.path().join("Great.Movie.2026.mkv"), &data).unwrap();
        let ok = Command::new("par2")
            .args([
                "create",
                "-q",
                "-q",
                "-s8192",
                "-c4",
                "set.par2",
                "Great.Movie.2026.mkv",
            ])
            .current_dir(tmp.path())
            .status()
            .expect("par2 required")
            .success();
        assert!(ok);

        // Obfuscate: data file AND the par2 index lose their names.
        std::fs::rename(
            tmp.path().join("Great.Movie.2026.mkv"),
            tmp.path().join("a9f3c2e1"),
        )
        .unwrap();
        std::fs::rename(tmp.path().join("set.par2"), tmp.path().join("b7d1")).unwrap();

        let renames = par_rename(tmp.path());
        assert!(tmp.path().join("Great.Movie.2026.mkv").exists());
        assert!(tmp.path().join("b7d1.par2").exists(), "par2 magic detected");
        assert!(renames
            .iter()
            .any(|(o, n)| o.ends_with("a9f3c2e1") && n.ends_with("Great.Movie.2026.mkv")));

        // Idempotent: nothing left to rename.
        assert!(par_rename(tmp.path()).is_empty());
    }

    #[test]
    fn rar_rename_by_signature() {
        let tmp = tempfile::tempdir().unwrap();
        // A real single-volume rar is not required — the signature is.
        let mut rar4 = b"Rar!\x1a\x07\x00".to_vec();
        rar4.extend_from_slice(&[0u8; 64]);
        std::fs::write(tmp.path().join("obfuscated01"), &rar4).unwrap();
        std::fs::write(tmp.path().join("readme.txt"), b"hello").unwrap();
        let mut z = SEVENZIP_MAGIC.to_vec();
        z.extend_from_slice(&[0u8; 32]);
        std::fs::write(tmp.path().join("mystery"), &z).unwrap();

        let renames = rar_rename(tmp.path());
        assert!(tmp.path().join("obfuscated01.rar").exists());
        assert!(tmp.path().join("mystery.7z").exists());
        assert!(
            tmp.path().join("readme.txt").exists(),
            "plain files untouched"
        );
        assert_eq!(renames.len(), 2);
    }

    /// A volume set someone else numbered must survive this pass untouched.
    ///
    /// THE regression test. Every RAR volume begins with the same `Rar!`
    /// magic, not just the first, so a signature renamer sees an old-style set
    /// as a pile of hidden archives and renumbers them into `.partNN.rar` —
    /// severing the chain from `set.rar`, which keeps its name because `rar`
    /// is a known extension. unrar then wrote volume 1, went looking for
    /// `set.r00`, and stopped. The observed cost: a 48 GiB remux delivered as
    /// a 500 MiB file, reported as a completed download, with the renamed
    /// volumes deleted afterwards by cleanup.
    #[test]
    fn a_numbered_volume_set_is_never_renamed() {
        let tmp = tempfile::tempdir().unwrap();
        let mut rar = b"Rar!\x1a\x07\x00".to_vec();
        rar.extend_from_slice(&[0u8; 64]);

        // Old-style RAR: the chain is set.rar → set.r00 → set.r01 …
        for name in ["set.rar", "set.r00", "set.r01", "set.r02"] {
            std::fs::write(tmp.path().join(name), &rar).unwrap();
        }
        // Split ZIP and 7z/split numbering are the same shape and the same
        // trap — a continuation volume carrying the format's magic bytes.
        let mut zip = ZIP_MAGIC.to_vec();
        zip.extend_from_slice(&[0u8; 32]);
        std::fs::write(tmp.path().join("pack.zip"), &zip).unwrap();
        std::fs::write(tmp.path().join("pack.z01"), &zip).unwrap();
        std::fs::write(tmp.path().join("pack.z02"), &zip).unwrap();
        let mut sz = SEVENZIP_MAGIC.to_vec();
        sz.extend_from_slice(&[0u8; 32]);
        std::fs::write(tmp.path().join("blob.7z.001"), &sz).unwrap();
        std::fs::write(tmp.path().join("blob.7z.002"), &sz).unwrap();

        let before: Vec<String> = files_of(tmp.path())
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        let renames = rar_rename(tmp.path());
        let after: Vec<String> = files_of(tmp.path())
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();

        assert!(
            renames.is_empty(),
            "a numbered volume set must not be renamed; got {renames:?}"
        );
        assert_eq!(
            before, after,
            "renaming a continuation volume severs the chain from its first \
             volume, and the extractor then writes exactly one volume"
        );
        for name in ["set.rar", "set.r00", "set.r01", "set.r02"] {
            assert!(tmp.path().join(name).exists(), "{name} was renamed away");
        }
    }

    /// The wide rule, stated as a table. It errs toward leaving files alone:
    /// declining to rename a genuinely obfuscated file costs one loud unpack
    /// failure, where renaming a real volume corrupts a set in silence.
    #[test]
    fn split_volume_extensions_are_recognised() {
        for yes in ["r00", "r99", "z01", "c00", "a01", "001", "999", "000"] {
            assert!(split_volume_ext(yes), "{yes} is a volume extension");
        }
        for no in ["rar", "mkv", "nfo", "", "r0", "r000", "0a0", "abc", "1ab"] {
            assert!(!split_volume_ext(no), "{no} is not a volume extension");
        }
    }

    /// The renamer still does its actual job: a genuinely obfuscated single
    /// archive, with no volume numbering to respect, is still named.
    #[test]
    fn an_obfuscated_single_archive_is_still_renamed() {
        let tmp = tempfile::tempdir().unwrap();
        let mut rar = b"Rar!\x1a\x07\x00".to_vec();
        rar.extend_from_slice(&[0u8; 64]);
        std::fs::write(tmp.path().join("a1b2c3d4e5"), &rar).unwrap();

        let renames = rar_rename(tmp.path());
        assert_eq!(renames.len(), 1, "{renames:?}");
        assert!(tmp.path().join("a1b2c3d4e5.rar").exists());
    }

    #[test]
    fn rar5_volume_number_parses() {
        // Synthesized minimal RAR5 main header: sig + crc + size +
        // type=1 + hflags=0 + arcflags=volume|number + number=3.
        let mut d = b"Rar!\x1a\x07\x01\x00".to_vec();
        d.extend_from_slice(&[0, 0, 0, 0]); // header crc (unchecked)
        d.push(5); // header size vint
        d.push(1); // type = main
        d.push(0); // header flags
        d.push(0x03); // archive flags: volume + number present
        d.push(3); // volume number
        assert_eq!(rar5_volume_number(&d), Some(3));

        let mut first = b"Rar!\x1a\x07\x01\x00".to_vec();
        first.extend_from_slice(&[0, 0, 0, 0]);
        first.push(4);
        first.push(1);
        first.push(0);
        first.push(0x01); // volume, no explicit number => first
        assert_eq!(rar5_volume_number(&first), Some(0));

        assert_eq!(rar5_volume_number(b"Rar!\x1a\x07\x00garbage"), None); // RAR4
    }

    #[test]
    fn filesystem_helpers_fail_closed_and_never_clobber() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("missing");
        assert!(head(&missing, 8).is_empty());
        assert!(md5_16k(&missing).is_none());
        assert!(files_of(&missing).is_empty());
        assert!(
            head(tmp.path(), 8).is_empty(),
            "directories are not readable payloads"
        );

        let empty = tmp.path().join("empty");
        std::fs::write(&empty, b"").unwrap();
        assert!(md5_16k(&empty).is_none());
        assert!(safe_rename(&empty, empty.clone()).is_none());

        let occupied = tmp.path().join("occupied");
        std::fs::write(&occupied, b"keep").unwrap();
        assert!(safe_rename(&empty, occupied.clone()).is_none());
        assert_eq!(std::fs::read(&occupied).unwrap(), b"keep");

        let impossible = tmp.path().join("no-parent/target");
        assert!(safe_rename(&empty, impossible).is_none());
        assert!(empty.exists());
    }

    #[test]
    fn rar5_parser_rejects_malformed_and_non_volume_headers() {
        assert_eq!(rar5_volume_number(b"short"), None);

        let mut overflow = b"Rar!\x1a\x07\x01\x00".to_vec();
        overflow.extend_from_slice(&[0, 0, 0, 0]);
        overflow.extend_from_slice(&[0x80; 10]);
        assert_eq!(rar5_volume_number(&overflow), None);

        let mut wrong_type = b"Rar!\x1a\x07\x01\x00".to_vec();
        wrong_type.extend_from_slice(&[0, 0, 0, 0]);
        wrong_type.extend_from_slice(&[4, 2, 0, 0]);
        assert_eq!(rar5_volume_number(&wrong_type), None);

        let mut extra_non_volume = b"Rar!\x1a\x07\x01\x00".to_vec();
        extra_non_volume.extend_from_slice(&[0, 0, 0, 0]);
        extra_non_volume.extend_from_slice(&[5, 1, 1, 0, 0]);
        assert_eq!(rar5_volume_number(&extra_non_volume), None);
    }

    #[test]
    fn signature_rename_handles_zip_collisions_and_rar4_sets() {
        let tmp = tempfile::tempdir().unwrap();
        let mut zip = ZIP_MAGIC.to_vec();
        zip.extend_from_slice(&[0u8; 32]);
        std::fs::write(tmp.path().join("zipblob"), &zip).unwrap();
        std::fs::write(tmp.path().join("held"), &zip).unwrap();
        std::fs::write(tmp.path().join("held.zip"), b"keep").unwrap();

        let mut rar4 = b"Rar!\x1a\x07\x00".to_vec();
        rar4.extend_from_slice(&[0u8; 64]);
        std::fs::write(tmp.path().join("a-hidden"), &rar4).unwrap();
        std::fs::write(tmp.path().join("b-hidden"), &rar4).unwrap();

        let renames = rar_rename(tmp.path());
        assert!(tmp.path().join("zipblob.zip").exists());
        assert_eq!(std::fs::read(tmp.path().join("held.zip")).unwrap(), b"keep");
        assert!(
            tmp.path().join("held").exists(),
            "collision must not clobber"
        );
        assert!(tmp.path().join("a-hidden.part01.rar").exists());
        assert!(tmp.path().join("a-hidden.part02.rar").exists());
        assert_eq!(renames.len(), 3);
    }

    #[test]
    fn rar5_volume_numbers_control_multi_volume_order() {
        fn numbered(volume: u8) -> Vec<u8> {
            let mut data = b"Rar!\x1a\x07\x01\x00".to_vec();
            data.extend_from_slice(&[0, 0, 0, 0]);
            data.extend_from_slice(&[5, 1, 0, 3, volume]);
            data
        }

        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("a-second"), numbered(1)).unwrap();
        std::fs::write(tmp.path().join("z-first"), numbered(0)).unwrap();
        let renames = rar_rename(tmp.path());

        assert_eq!(renames.len(), 2);
        assert_eq!(
            std::fs::read(tmp.path().join("a-second.part01.rar")).unwrap(),
            numbered(0)
        );
        assert_eq!(
            std::fs::read(tmp.path().join("a-second.part02.rar")).unwrap(),
            numbered(1)
        );
    }
}
