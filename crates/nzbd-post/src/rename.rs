//! par-rename + rar-rename (ARCHITECTURE.md §9): recover real filenames of
//! obfuscated posts before verification/unpack.
//!
//! - **par-rename**: par2 FileDesc packets carry each source file's name
//!   and the MD5 of its first 16 KiB. Any disk file whose 16k-hash matches
//!   a description is renamed to its true name. Obfuscated `.par2` files
//!   themselves are found by content (`PAR2\0PKT` magic), not extension.
//! - **rar-rename**: files whose *content* is a RAR/7z/zip volume but
//!   whose name hides it get an extension back. Multi-volume RAR sets are
//!   ordered by checked headers; split-file continuity establishes membership.
//!   Ambiguous sets retain their names and fail explicitly.

use crate::PostError;
use md5::{Digest, Md5};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
type Renames = Result<Vec<(PathBuf, PathBuf)>, PostError>;
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
fn safe_rename(from: &Path, to: PathBuf) -> Result<Option<(PathBuf, PathBuf)>, PostError> {
    if from == to.as_path() {
        return Ok(None);
    }
    match nzbd_state::fileops::rename_exclusive(from, &to) {
        Ok(()) => {
            tracing::info!(from = %from.display(), to = %to.display(), "renamed");
            Ok(Some((from.to_path_buf(), to)))
        }
        Err(e) => Err(PostError::Subprocess(format!(
            "rename {} to {}: {e}",
            from.display(),
            to.display()
        ))),
    }
}

pub(crate) type Custody<'a> = Option<(&'a nzbd_state::artifacts::Inventory, u32)>;
pub(crate) fn rename_owned(
    from: &Path,
    to: PathBuf,
    custody: Custody<'_>,
) -> Result<Option<(PathBuf, PathBuf)>, PostError> {
    if from == to {
        return Ok(None);
    }
    if let Some((inventory, job)) = custody {
        inventory
            .restore_file(job, from, &to)
            .map_err(|e| PostError::Subprocess(e.to_string()))?;
        Ok(Some((from.to_path_buf(), to)))
    } else {
        safe_rename(from, to)
    }
}

/// par-rename. Returns `(old, new)` pairs so the caller can remap download
/// evidence (whole-file CRCs are content-addressed; only paths change).
pub fn par_rename(dir: &Path) -> Renames {
    par_rename_owned(dir, None)
}

pub fn par_rename_owned(
    dir: &Path,
    custody: Option<(&nzbd_state::artifacts::Inventory, u32)>,
) -> Renames {
    let mut renames = Vec::new();

    // 1. Give obfuscated par2 files their extension back (by magic).
    for p in files_of(dir) {
        if !ext_is(&p, "par2") && head(&p, 8) == PAR2_MAGIC {
            let to = dir.join(format!(
                "{}.par2",
                p.file_stem().unwrap_or_default().to_string_lossy()
            ));
            if let Some(pair) = rename_owned(&p, to, custody)? {
                renames.push(pair);
            }
        }
    }

    // Prefixes narrow candidates; size and full digest establish intact identity.
    let sets = crate::par2::load_sets(dir)?;
    for set in sets {
        let mut wanted: HashMap<[u8; 16], Vec<&crate::par2::Par2File>> = HashMap::new();
        for f in &set.files {
            wanted.entry(f.md5_16k).or_default().push(f);
        }
        for p in files_of(&set.root) {
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
            let target = set.root.join(relative);
            if p == target {
                continue;
            }
            create_parents(&set.root, target.parent().unwrap())?;
            if let Some((inventory, job)) = custody {
                inventory.restore_file(job, &p, &target).map_err(|e| {
                    PostError::Subprocess(format!(
                        "par rename {} to {}: {e}",
                        p.display(),
                        target.display()
                    ))
                })?;
                renames.push((p, target));
            } else if let Some(pair) = safe_rename(&p, target)? {
                renames.push(pair);
            }
        }
    }
    Ok(renames)
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
    let size = usize::try_from(vint(data, &mut pos)?).ok()?;
    let end = pos.checked_add(size)?;
    if end > data.len()
        || crc32fast::hash(data.get(12..end)?)
            != u32::from_le_bytes(data.get(8..12)?.try_into().ok()?)
    {
        return None;
    }
    let data = &data[..end];
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
    // Without a volume-number field, this is either the first volume or
    // a standalone archive; both occupy position zero.
    Some(0)
}

/// RAR4 stores the volume number in ENDARC, after an optional data CRC.
/// Walk validated headers and seek over packed data; filenames and lexical
/// order are not evidence. See UnRAR arcread.cpp HEAD_ENDARC / EARC_VOLNUMBER.
#[derive(Clone, PartialEq, Eq)]
struct RarMember {
    name: Vec<u8>,
    size: u64,
    method: u8,
    version: u8,
}
struct RarVolume {
    new_numbering: bool,
    number: u64,
    first: Option<(RarMember, bool)>,
    last: Option<(RarMember, bool)>,
}
fn rar4_volume_number(path: &Path) -> Option<u64> {
    rar4_volume(path).map(|v| v.number)
}
fn rar4_volume(path: &Path) -> Option<RarVolume> {
    let mut file = nzbd_state::fileops::open(path).ok()?;
    let length = file.metadata().ok()?.len();
    let mut magic = [0u8; 7];
    file.read_exact(&mut magic).ok()?;
    if &magic != b"Rar!\x1a\x07\x00" {
        return None;
    }
    let mut offset = 7u64;
    let mut first = None;
    let mut new_numbering = false;
    let mut first_member = None;
    let mut last_member = None;
    for _ in 0..100_000 {
        file.seek(SeekFrom::Start(offset)).ok()?;
        let mut short = [0u8; 7];
        file.read_exact(&mut short).ok()?;
        let flags = u16::from_le_bytes([short[3], short[4]]);
        let size = u16::from_le_bytes([short[5], short[6]]) as usize;
        if size < 7 || offset.checked_add(size as u64)? > length {
            return None;
        }
        let mut header = vec![0u8; size];
        header[..7].copy_from_slice(&short);
        file.read_exact(&mut header[7..]).ok()?;
        if crc32fast::hash(&header[2..]) as u16 != u16::from_le_bytes([short[0], short[1]]) {
            return None;
        }
        let word = |pos: usize| -> Option<u32> {
            Some(u32::from_le_bytes(
                header.get(pos..pos + 4)?.try_into().ok()?,
            ))
        };
        if offset == 7 {
            if short[2] != 0x73 || size < 13 || flags & 0x80 != 0 {
                return None;
            }
            if flags & 1 == 0 {
                return Some(RarVolume {
                    new_numbering: flags & 0x10 != 0,
                    number: 0,
                    first: None,
                    last: None,
                });
            } // single-volume archive
            first = Some(flags & 0x100 != 0);
            new_numbering = flags & 0x10 != 0;
        }
        if short[2] == 0x7b {
            if flags & 8 == 0 {
                return None;
            }
            let pos = 7 + if flags & 2 != 0 { 4 } else { 0 };
            let number = u16::from_le_bytes(header.get(pos..pos + 2)?.try_into().ok()?);
            if first == Some(true) && number != 0 {
                return None;
            }
            return Some(RarVolume {
                new_numbering,
                number: u64::from(number),
                first: first_member,
                last: last_member,
            });
        }
        if short[2] == 0x74 {
            if size < 32 || flags & 4 != 0 {
                return None;
            }
            let name_len = u16::from_le_bytes(header[26..28].try_into().ok()?) as usize;
            let start = if flags & 0x100 != 0 { 40 } else { 32 };
            let mut unpacked = u64::from(word(11)?);
            if flags & 0x100 != 0 {
                unpacked |= u64::from(word(36)?) << 32;
            }
            let member = RarMember {
                name: header.get(start..start + name_len)?.to_vec(),
                size: unpacked,
                method: header[25],
                version: header[24],
            };
            if first_member.is_none() {
                first_member = Some((member.clone(), flags & 1 != 0));
            }
            last_member = Some((member, flags & 2 != 0));
        }
        let mut packed = if flags & 0x8000 != 0 {
            u64::from(word(7)?)
        } else {
            0
        };
        if short[2] == 0x74 && flags & 0x100 != 0 {
            packed |= u64::from(word(32)?) << 32;
        }
        offset = offset.checked_add(size as u64)?.checked_add(packed)?;
        if offset >= length {
            return None;
        }
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

/// SFV provides the volume names that older RAR4 headers omit. Match the
/// complete file checksum, reject ambiguous mappings, then journal each rename.
fn sfv_restore_archives(dir: &Path, custody: Custody<'_>) -> Renames {
    let files = crate::namespace::files(dir)?;
    let archives: Vec<_> = files
        .iter()
        .filter(|path| head(path, 7).starts_with(RAR_MAGIC))
        .collect();
    let unresolved: std::collections::HashSet<_> = archives
        .iter()
        .filter(|path| {
            let extension = path
                .extension()
                .unwrap_or_default()
                .to_string_lossy()
                .to_ascii_lowercase();
            (extension != "rar" && !split_volume_ext(&extension))
                || (extension.len() == 3
                    && extension.starts_with('r')
                    && extension.as_bytes()[1..].iter().all(u8::is_ascii_digit)
                    && !archives.contains(&&path.with_extension("rar")))
        })
        .filter_map(|path| path.parent())
        .collect();
    if unresolved.is_empty() {
        return Ok(Vec::new());
    }
    let mut catalogs = std::collections::BTreeMap::<PathBuf, u32>::new();
    for sfv in files.iter().filter(|path| {
        ext_is(path, "sfv")
            && path
                .parent()
                .is_some_and(|parent| unresolved.contains(parent))
    }) {
        if std::fs::metadata(sfv)?.len() > 2 * 1024 * 1024 {
            continue;
        }
        let bytes = std::fs::read(sfv)?;
        for raw in bytes.split(|byte| *byte == b'\n') {
            let raw = raw.trim_ascii();
            if raw.is_empty() || raw.starts_with(b";") {
                continue;
            }
            // SFV comments commonly use a legacy encoding. Unrepresentable
            // filenames cannot be restored, but unrelated entries still can.
            let Ok(line) = std::str::from_utf8(raw) else {
                continue;
            };
            let Some((name, checksum)) = line.rsplit_once(char::is_whitespace) else {
                continue;
            };
            if checksum.len() != 8 {
                continue;
            }
            let Ok(crc) = u32::from_str_radix(checksum, 16) else {
                continue;
            };
            let relative = crate::namespace::relative(name.trim())?;
            let extension = relative
                .extension()
                .unwrap_or_default()
                .to_string_lossy()
                .to_ascii_lowercase();
            if extension != "rar"
                && !(extension.len() == 3
                    && extension.as_bytes()[0] >= b'r'
                    && extension.as_bytes()[0] <= b'z'
                    && extension.as_bytes()[1..].iter().all(u8::is_ascii_digit))
            {
                continue;
            }
            let target = sfv.parent().unwrap().join(relative);
            if catalogs
                .insert(target, crc)
                .is_some_and(|prior| prior != crc)
            {
                return Err(PostError::Subprocess(
                    "conflicting SFV archive names".into(),
                ));
            }
        }
    }
    if catalogs.is_empty() {
        return Ok(Vec::new());
    }
    let mut candidates = Vec::new();
    for path in archives {
        if !path
            .parent()
            .is_some_and(|parent| unresolved.contains(parent))
        {
            continue;
        }
        let mut input = nzbd_state::fileops::open(path).map_err(std::io::Error::other)?;
        let mut digest = crc32fast::Hasher::new();
        let mut buffer = [0; 65536];
        loop {
            let count = input.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            digest.update(&buffer[..count]);
        }
        candidates.push((path, digest.finalize()));
    }
    let mut sources = std::collections::HashSet::new();
    let mut plan = Vec::new();
    for (target, checksum) in catalogs {
        let matches: Vec<_> = candidates
            .iter()
            .filter(|(path, crc)| path.parent() == target.parent() && *crc == checksum)
            .collect();
        if matches.is_empty() {
            continue;
        } // extractor diagnoses absent/damaged volumes
        if matches.len() != 1 || !sources.insert(matches[0].0.clone()) {
            return Err(PostError::Subprocess(
                "ambiguous SFV archive checksum; filenames preserved".into(),
            ));
        }
        let source = matches[0].0;
        if source == &target {
            continue;
        }
        if target.symlink_metadata().is_ok() {
            return Err(PostError::Subprocess(
                "SFV archive target already exists; filenames preserved".into(),
            ));
        }
        plan.push((source.clone(), target));
    }
    let mut renamed = Vec::new();
    for (source, target) in plan {
        if let Some(pair) = rename_owned(&source, target, custody)? {
            renamed.push(pair);
        }
    }
    Ok(renamed)
}

/// Restore only sets whose order AND membership are established. Correctly
/// named sets remain the extractor's responsibility. No lexical ordering.
pub fn rar_rename(dir: &Path) -> Renames {
    rar_rename_owned(dir, None)
}
pub fn rar_rename_owned(dir: &Path, custody: Custody<'_>) -> Renames {
    let mut renamed = sfv_restore_archives(dir, custody)?;
    let known = ["rar", "7z", "zip", "par2", "nzb", "sfv", "nfo", "srr"];
    let mut groups: std::collections::BTreeMap<PathBuf, Vec<PathBuf>> = Default::default();
    let mut plan = Vec::new();
    for p in files_of(dir) {
        let ext = p
            .extension()
            .map(|e| e.to_string_lossy().to_lowercase())
            .unwrap_or_default();
        let h = head(&p, 128);
        if h.starts_with(RAR_MAGIC) {
            groups
                .entry(p.parent().unwrap().into())
                .or_default()
                .push(p);
        } else if !known.contains(&ext.as_str()) && !split_volume_ext(&ext) {
            let extension = if h.starts_with(SEVENZIP_MAGIC) {
                Some("7z")
            } else if h.starts_with(ZIP_MAGIC) {
                Some("zip")
            } else {
                None
            };
            if let Some(e) = extension {
                plan.push((p.clone(), p.with_extension(e)));
            }
        }
    }
    for (parent, files) in groups {
        let hidden = files.iter().any(|p| {
            let e = p
                .extension()
                .map(|e| e.to_string_lossy().to_lowercase())
                .unwrap_or_default();
            e != "rar" && !split_volume_ext(&e)
        });
        // A known old-style continuation must have a matching first volume.
        let mismatched = files.iter().any(|p| {
            p.extension().is_some_and(|e| {
                let e = e.to_string_lossy().to_ascii_lowercase();
                e.len() == 3
                    && e.starts_with('r')
                    && e[1..].bytes().all(|b| b.is_ascii_digit())
                    && !files.contains(&p.with_extension("rar"))
            })
        });
        if !hidden && !mismatched {
            continue;
        }
        if files.len() == 1 {
            let p = &files[0];
            let h = head(p, 128);
            let n = if h.starts_with(b"Rar!\x1a\x07\x00") {
                rar4_volume_number(p)
            } else {
                rar5_volume_number(&h)
            };
            if n != Some(0) {
                return Err(PostError::Subprocess(
                    "RAR head is missing or invalid; filenames preserved".into(),
                ));
            }
            plan.push((p.clone(), p.with_extension("rar")));
            continue;
        }
        let mut volumes = Vec::new();
        for p in files {
            let info = rar4_volume(&p).ok_or_else(|| {
                PostError::Subprocess(
                    "RAR set lacks checked order/membership evidence; filenames preserved".into(),
                )
            })?;
            volumes.push((p, info));
        }
        volumes.sort_by_key(|(_, v)| v.number);
        for (i, (_, v)) in volumes.iter().enumerate() {
            let valid = v.new_numbering == volumes[0].1.new_numbering
                && v.number == i as u64
                && if i == 0 {
                    v.first.as_ref().is_some_and(|(_, split)| !split)
                } else {
                    match (&volumes[i - 1].1.last, &v.first) {
                        (Some((previous, true)), Some((current, true))) => previous == current,
                        _ => false,
                    }
                };
            if !valid {
                return Err(PostError::Subprocess(
                    "RAR volume continuity is ambiguous or incomplete; filenames preserved".into(),
                ));
            }
        }
        if !volumes
            .last()
            .unwrap()
            .1
            .last
            .as_ref()
            .is_some_and(|(_, split)| !split)
        {
            return Err(PostError::Subprocess(
                "RAR final volume missing; filenames preserved".into(),
            ));
        }
        let head = &volumes[0].0;
        let stem = head.file_stem().unwrap().to_string_lossy();
        let base = stem.strip_suffix(".part01").unwrap_or(&stem).to_owned();
        let modern = volumes[0].1.new_numbering;
        for (i, (path, _)) in volumes.into_iter().enumerate() {
            let name = if modern {
                format!("{base}.part{:02}.rar", i + 1)
            } else if i == 0 {
                format!("{base}.rar")
            } else if i <= 900 {
                format!(
                    "{base}.{}{:02}",
                    (b'r' + ((i - 1) / 100) as u8) as char,
                    (i - 1) % 100
                )
            } else {
                return Err(PostError::Subprocess(
                    "RAR volume count exceeds old-style naming range".into(),
                ));
            };
            plan.push((path, parent.join(name)));
        }
    }
    let mut targets = std::collections::HashSet::new();
    for (source, target) in &plan {
        if !targets.insert(target.clone())
            || (source != target && target.symlink_metadata().is_ok())
        {
            return Err(PostError::Subprocess(
                "archive rename target already exists; filenames preserved".into(),
            ));
        }
    }
    for (source, target) in plan {
        if let Some(pair) = rename_owned(&source, target, custody)? {
            renamed.push(pair);
        }
    }
    Ok(renamed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn correctly_named_archives_ignore_optional_sfv_encoding() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("release.rar"), rar4_fixture(None)).unwrap();
        std::fs::write(
            tmp.path().join("post.sfv"),
            b"; legacy comment \xff\nrelease.rar 00000000\n",
        )
        .unwrap();
        assert!(rar_rename(tmp.path()).unwrap().is_empty());
    }

    #[test]
    fn sfv_restores_obfuscated_old_rar_without_volume_numbers() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = String::new();
        for (i, name) in ["release.rar", "release.r00", "release.r01"]
            .iter()
            .enumerate()
        {
            let mut bytes = rar4_fixture(Some(i as u16));
            // Old RAR4 ENDARC may omit EARC_VOLNUMBER. Preserve its CRC.
            let end = bytes.len() - 9;
            bytes.truncate(end);
            let header = [0x7b, 0, 0, 7, 0];
            bytes.extend((crc32fast::hash(&header) as u16).to_le_bytes());
            bytes.extend(header);
            let source = format!("different-hash-{i}.{}", if i == 0 { "rar" } else { "r00" });
            std::fs::write(tmp.path().join(source), &bytes).unwrap();
            catalog.push_str(&format!("{name} {:08x}\n", crc32fast::hash(&bytes)));
        }
        let mut sfv = b"; non-UTF8 comment \xff\n".to_vec();
        sfv.extend(catalog.as_bytes());
        std::fs::write(tmp.path().join("post.sfv"), sfv).unwrap();
        let renames = rar_rename(tmp.path()).unwrap();
        assert_eq!(renames.len(), 3);
        for name in ["release.rar", "release.r00", "release.r01"] {
            assert!(tmp.path().join(name).exists());
        }
    }

    #[test]
    fn sfv_rejects_ambiguous_checksums_and_unsafe_paths_without_renaming() {
        let tmp = tempfile::tempdir().unwrap();
        let bytes = rar4_fixture(None);
        for name in ["one", "two"] {
            std::fs::write(tmp.path().join(name), &bytes).unwrap();
        }
        std::fs::write(
            tmp.path().join("post.sfv"),
            format!("release.rar {:08x}\n", crc32fast::hash(&bytes)),
        )
        .unwrap();
        assert!(rar_rename(tmp.path()).is_err());
        assert!(tmp.path().join("one").exists());
        assert!(!tmp.path().join("release.rar").exists());
        std::fs::write(tmp.path().join("post.sfv"), "../outside.rar 00000000\n").unwrap();
        assert!(rar_rename(tmp.path()).is_err());
    }

    fn rar4_fixture(volume: Option<u16>) -> Vec<u8> {
        fn block(kind: u8, flags: u16, body: &[u8]) -> Vec<u8> {
            let mut h = vec![0, 0, kind];
            h.extend_from_slice(&flags.to_le_bytes());
            h.extend_from_slice(&((7 + body.len()) as u16).to_le_bytes());
            h.extend_from_slice(body);
            let crc = crc32fast::hash(&h[2..]) as u16;
            h[..2].copy_from_slice(&crc.to_le_bytes());
            h
        }
        let mut bytes = b"Rar!\x1a\x07\x00".to_vec();
        let flags = volume.map_or(0, |n| 0x11 | if n == 0 { 0x100 } else { 0 });
        bytes.extend(block(0x73, flags, &[0; 6]));
        if let Some(n) = volume {
            let data = [b'a' + n as u8];
            let mut member = Vec::new();
            member.extend(1u32.to_le_bytes());
            member.extend(3u32.to_le_bytes());
            member.push(3); // Unix host
            member.extend(crc32fast::hash(if n == 2 { b"abc" } else { &data }).to_le_bytes());
            member.extend(0u32.to_le_bytes());
            member.extend([20, 0x30]); // RAR2, stored
            member.extend(9u16.to_le_bytes());
            member.extend(0x20u32.to_le_bytes());
            member.extend(b"video.mkv");
            bytes.extend(block(
                0x74,
                0x8000 | if n > 0 { 1 } else { 0 } | if n < 2 { 2 } else { 0 },
                &member,
            ));
            bytes.extend(data);
        }
        if let Some(n) = volume {
            // EARC_NEXT_VOLUME tells extractors to open the next volume;
            // FILE_SPLIT_AFTER alone does not establish that signal.
            bytes.extend(block(0x7b, 8 | if n < 2 { 1 } else { 0 }, &n.to_le_bytes()));
        }
        bytes
    }

    #[test]
    fn rar4_order_comes_from_end_headers_not_lexical_names() {
        let tmp = tempfile::tempdir().unwrap();
        for (name, number) in [("a-last", 2), ("m-middle", 1), ("z-head", 0)] {
            std::fs::write(tmp.path().join(name), rar4_fixture(Some(number))).unwrap();
        }
        assert_eq!(rar_rename(tmp.path()).unwrap().len(), 3);
        for number in 0..3 {
            assert_eq!(
                rar4_volume_number(&tmp.path().join(format!("z-head.part{:02}.rar", number + 1))),
                Some(number)
            );
        }
    }

    #[test]
    fn unknown_or_duplicate_rar_order_is_an_error_without_renaming() {
        for duplicate in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let head = rar4_fixture(Some(0));
            let other = if duplicate {
                head.clone()
            } else {
                b"Rar!\x1a\x07\x00invalid".to_vec()
            };
            std::fs::write(tmp.path().join("first"), &head).unwrap();
            std::fs::write(tmp.path().join("other"), &other).unwrap();
            assert!(rar_rename(tmp.path()).is_err());
            assert_eq!(std::fs::read(tmp.path().join("first")).unwrap(), head);
            assert_eq!(std::fs::read(tmp.path().join("other")).unwrap(), other);
        }
    }

    #[test]
    fn six_raw_media_files_recover_exact_par_names_from_hidden_index() {
        if !crate::tools::require_tool("par2") {
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let names: Vec<_> = (1..=6).map(|n| format!("Show.S01E{n:02}.mkv")).collect();
        for (i, name) in names.iter().enumerate() {
            let bytes: Vec<_> = (0..32768).map(|j| ((j + i * 13) % 251) as u8).collect();
            std::fs::write(tmp.path().join(name), bytes).unwrap();
        }
        let output = Command::new("par2")
            .args(["create", "-q", "-q", "-s8192", "-c1", "set.par2"])
            .args(&names)
            .current_dir(tmp.path())
            .output()
            .unwrap();
        assert!(output.status.success());
        for (i, name) in names.iter().enumerate() {
            std::fs::rename(
                tmp.path().join(name),
                tmp.path().join(format!("obfuscated-{}", 6 - i)),
            )
            .unwrap();
        }
        for p in files_of(tmp.path()) {
            if ext_is(&p, "par2") {
                std::fs::rename(&p, p.with_extension("hidden")).unwrap();
            }
        }
        par_rename(tmp.path()).unwrap();
        for (i, name) in names.iter().enumerate() {
            let bytes: Vec<_> = (0..32768).map(|j| ((j + i * 13) % 251) as u8).collect();
            assert_eq!(std::fs::read(tmp.path().join(name)).unwrap(), bytes);
        }
    }

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

        let renames = par_rename(tmp.path()).unwrap();
        assert!(tmp.path().join("Great.Movie.2026.mkv").exists());
        assert!(tmp.path().join("b7d1.par2").exists(), "par2 magic detected");
        assert!(renames
            .iter()
            .any(|(o, n)| o.ends_with("a9f3c2e1") && n.ends_with("Great.Movie.2026.mkv")));

        // Idempotent: nothing left to rename.
        assert!(par_rename(tmp.path()).unwrap().is_empty());
    }

    #[test]
    fn rar_rename_by_signature() {
        let tmp = tempfile::tempdir().unwrap();
        // A real single-volume rar is not required — the signature is.
        let rar4 = rar4_fixture(None);
        std::fs::write(tmp.path().join("obfuscated01"), &rar4).unwrap();
        std::fs::write(tmp.path().join("readme.txt"), b"hello").unwrap();
        let mut z = SEVENZIP_MAGIC.to_vec();
        z.extend_from_slice(&[0u8; 32]);
        std::fs::write(tmp.path().join("mystery"), &z).unwrap();

        let renames = rar_rename(tmp.path()).unwrap();
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
        let rar = rar4_fixture(None);

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
        let renames = rar_rename(tmp.path()).unwrap();
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
        let rar = rar4_fixture(None);
        std::fs::write(tmp.path().join("a1b2c3d4e5"), &rar).unwrap();

        let renames = rar_rename(tmp.path()).unwrap();
        assert_eq!(renames.len(), 1, "{renames:?}");
        assert!(tmp.path().join("a1b2c3d4e5.rar").exists());
    }

    fn rar5_header(fields: &[u8]) -> Vec<u8> {
        let mut data = b"Rar!\x1a\x07\x01\x00".to_vec();
        let mut header = vec![fields.len() as u8];
        header.extend(fields);
        data.extend(crc32fast::hash(&header).to_le_bytes());
        data.extend(header);
        data
    }

    #[test]
    fn rar5_volume_number_parses() {
        // Synthesized minimal RAR5 main header: sig + crc + size +
        // type=1 + hflags=0 + arcflags=volume|number + number=3.
        assert_eq!(rar5_volume_number(&rar5_header(&[1, 0, 3, 3])), Some(3));
        assert_eq!(rar5_volume_number(&rar5_header(&[1, 0, 1])), Some(0));
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
        assert!(safe_rename(&empty, empty.clone()).unwrap().is_none());

        let occupied = tmp.path().join("occupied");
        std::fs::write(&occupied, b"keep").unwrap();
        assert!(safe_rename(&empty, occupied.clone()).is_err());
        assert_eq!(std::fs::read(&occupied).unwrap(), b"keep");

        let impossible = tmp.path().join("no-parent/target");
        assert!(safe_rename(&empty, impossible).is_err());
        assert!(empty.exists());
    }

    #[test]
    fn rar5_parser_rejects_malformed_and_accepts_standalone_headers() {
        assert_eq!(rar5_volume_number(b"short"), None);

        let mut overflow = b"Rar!\x1a\x07\x01\x00".to_vec();
        overflow.extend_from_slice(&[0, 0, 0, 0]);
        overflow.extend_from_slice(&[0x80; 10]);
        assert_eq!(rar5_volume_number(&overflow), None);

        let mut wrong_type = b"Rar!\x1a\x07\x01\x00".to_vec();
        wrong_type.extend_from_slice(&[0, 0, 0, 0]);
        wrong_type.extend_from_slice(&[4, 2, 0, 0]);
        assert_eq!(rar5_volume_number(&wrong_type), None);

        assert_eq!(rar5_volume_number(&rar5_header(&[1, 1, 0, 0])), Some(0));
        let mut corrupt = rar5_header(&[1, 0, 0]);
        corrupt[8] ^= 1;
        assert_eq!(rar5_volume_number(&corrupt), None);
    }

    #[test]
    fn signature_rename_handles_zip_collisions_and_rar4_sets() {
        let tmp = tempfile::tempdir().unwrap();
        let mut zip = ZIP_MAGIC.to_vec();
        zip.extend_from_slice(&[0u8; 32]);
        std::fs::write(tmp.path().join("zipblob"), &zip).unwrap();
        std::fs::write(tmp.path().join("held"), &zip).unwrap();
        std::fs::write(tmp.path().join("held.zip"), b"keep").unwrap();

        let rar4 = rar4_fixture(None);
        std::fs::write(tmp.path().join("a-hidden"), &rar4).unwrap();
        std::fs::write(tmp.path().join("b-hidden"), &rar4).unwrap();

        assert!(rar_rename(tmp.path()).is_err());
        assert_eq!(std::fs::read(tmp.path().join("held.zip")).unwrap(), b"keep");
        assert!(tmp.path().join("held").exists());
        assert!(tmp.path().join("a-hidden").exists());
        assert!(tmp.path().join("b-hidden").exists());
    }

    #[test]
    fn rar5_volume_numbers_alone_do_not_prove_membership() {
        let tmp = tempfile::tempdir().unwrap();
        for (name, n) in [("first", 0), ("second", 1)] {
            std::fs::write(tmp.path().join(name), rar5_header(&[1, 0, 3, n])).unwrap();
        }
        assert!(rar_rename(tmp.path()).is_err());
        assert!(tmp.path().join("first").exists());
        assert!(tmp.path().join("second").exists());
    }

    #[test]
    fn mismatched_stems_extract_the_complete_stored_rar_set() {
        if !crate::tools::require_tool("7z") {
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        for (name, number) in [("a-last.r01", 2), ("m-middle.r00", 1), ("z-head.rar", 0)] {
            let mut bytes = rar4_fixture(Some(number));
            // Old-style numbering in the actual archive header.
            bytes[10] &= !0x10;
            let checksum = crc32fast::hash(&bytes[9..20]) as u16;
            bytes[7..9].copy_from_slice(&checksum.to_le_bytes());
            std::fs::write(tmp.path().join(name), bytes).unwrap();
        }
        rar_rename(tmp.path()).unwrap();
        assert!(tmp.path().join("z-head.r00").is_file());
        let out = tmp.path().join("out");
        let result = Command::new("7z")
            .arg("x")
            .arg("-y")
            .arg(format!("-o{}", out.display()))
            .arg(tmp.path().join("z-head.rar"))
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{} {}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(std::fs::read(out.join("video.mkv")).unwrap(), b"abc");
    }

    #[test]
    fn mixed_rar_membership_and_missing_tail_preserve_inputs() {
        for mixed in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            for (name, number) in [("head.rar", 0), ("middle.r00", 1)] {
                let mut bytes = rar4_fixture(Some(number));
                if mixed && number == 1 {
                    // Name in the FILE_HEAD differs, with valid header CRC.
                    let offset = 20;
                    bytes[offset + 32] = b'X';
                    let size = u16::from_le_bytes(bytes[offset + 5..offset + 7].try_into().unwrap())
                        as usize;
                    let crc = crc32fast::hash(&bytes[offset + 2..offset + size]) as u16;
                    bytes[offset..offset + 2].copy_from_slice(&crc.to_le_bytes());
                }
                std::fs::write(tmp.path().join(name), bytes).unwrap();
            }
            assert!(rar_rename(tmp.path()).is_err());
            assert!(tmp.path().join("head.rar").exists());
            assert!(tmp.path().join("middle.r00").exists());
        }
    }
}
