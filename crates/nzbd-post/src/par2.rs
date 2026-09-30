//! par2 packet parsing + native quick verification (ARCHITECTURE.md §9).
//!
//! We parse par2 packets ourselves (simple); GF(2^16) repair math stays in
//! the `par2` subprocess. Quick verification never re-reads file data: par2
//! stores per-slice CRC32s with the last slice zero-padded to the block
//! size, so `combine(slice crcs)` must equal
//! `combine(whole-file CRC from download, crc(zero padding))` — and the
//! whole-file CRC is exactly what the engine computed from segment CRCs at
//! finalize time.

use crate::{DownloadEvidence, PostError, VerifyResult};
use nzbd_yenc::crc32_combine;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Par2File {
    pub md5_full: [u8; 16],
    pub id: [u8; 16],
    pub name: String,
    pub length: u64,
    pub md5_16k: [u8; 16],
    pub slice_crcs: Vec<u32>,
}

#[derive(Debug, Clone, Default)]
pub struct Par2Set {
    pub par_paths: Vec<PathBuf>,
    pub set_id: [u8; 16],
    pub root: PathBuf,
    pub slice_size: u64,
    pub files: Vec<Par2File>,
    /// Distinct recovery blocks present across the parsed .par2 files.
    pub recovery_blocks: u32,
    /// The "main" par2 file (smallest, index packets) for subprocess calls.
    pub main_path: Option<PathBuf>,
}

/// Parse every readable `*.par2` in a directory into one set.
///
/// The packet walking itself lives in `nzbd-par2`, a leaf crate, because
/// the download engine needs the same FileDesc names *during* a download
/// and cannot depend on this crate (`nzbd-post` depends on `nzbd-engine`).
/// One parser, two callers — the alternative was a second copy that would
/// drift.
pub fn load_dir(dir: &Path) -> Result<Option<Par2Set>, PostError> {
    let mut sets = load_sets(dir)?;
    if sets.len() > 1 {
        return Err(PostError::Subprocess(
            "multiple PAR sets require set-aware processing".into(),
        ));
    }
    Ok(sets.pop())
}

pub fn load_sets(dir: &Path) -> Result<Vec<Par2Set>, PostError> {
    let mut groups: std::collections::BTreeMap<[u8; 16], Vec<PathBuf>> = Default::default();
    for path in crate::namespace::files(dir)? {
        if !path
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("par2"))
        {
            continue;
        }
        if std::fs::metadata(&path)?.len() > 64 * 1024 * 1024 {
            return Err(PostError::Subprocess("PAR metadata size limit".into()));
        }
        let scan = nzbd_par2::scan(&std::fs::read(&path)?);
        if scan.invalid {
            return Err(PostError::Subprocess(
                "invalid PAR packet digest or mixed set".into(),
            ));
        }
        if let Some(set_id) = scan.set_id {
            groups.entry(set_id).or_default().push(path);
        }
    }
    let mut sets = Vec::new();
    for (set_id, paths) in groups {
        let root = paths
            .first()
            .and_then(|p| p.parent())
            .unwrap_or(dir)
            .to_path_buf();
        if paths.iter().any(|p| p.parent() != Some(root.as_path())) {
            return Err(PostError::Subprocess(
                "PAR set spans multiple catalog roots; review required".into(),
            ));
        }
        if let Some(mut set) = load_one(&root, paths)? {
            set.set_id = set_id;
            sets.push(set);
        }
    }
    Ok(sets)
}

fn load_one(dir: &Path, mut par_files: Vec<PathBuf>) -> Result<Option<Par2Set>, PostError> {
    par_files.sort();
    let mut slice_size = 0u64;
    type Description = (String, u64, [u8; 16], [u8; 16]);
    let mut descs: HashMap<[u8; 16], Description> = HashMap::new();
    let mut order: Vec<[u8; 16]> = Vec::new();
    let mut crcs: HashMap<[u8; 16], Vec<u32>> = HashMap::new();
    let mut exponents: BTreeSet<u32> = BTreeSet::new();
    let mut main_path: Option<PathBuf> = None;
    let mut main_size = u64::MAX;

    for path in &par_files {
        let Ok(bytes) = std::fs::read(path) else {
            continue; // unreadable / still paused-not-downloaded
        };
        let scan = nzbd_par2::scan(&bytes);
        if scan.slice_size > 0 {
            slice_size = scan.slice_size;
        }
        let has_descs = scan.has_descs();
        for d in &scan.descs {
            if descs
                .insert(d.id, (d.name.clone(), d.length, d.md5_16k, d.md5_full))
                .is_none()
            {
                order.push(d.id);
            }
        }
        for (id, v) in scan.crcs {
            crcs.entry(id).or_insert(v);
        }
        exponents.extend(scan.exponents);
        // The main file is conventionally the smallest one with FileDesc packets.
        if has_descs && bytes.len() as u64 <= main_size {
            main_size = bytes.len() as u64;
            main_path = Some(path.clone());
        }
    }

    if descs.is_empty() || slice_size == 0 {
        return Ok(None);
    }
    let files = order
        .into_iter()
        .map(|id| {
            let (name, length, md5_16k, md5_full) = descs.remove(&id).expect("id came from descs");
            Par2File {
                md5_full,
                id,
                name,
                length,
                md5_16k,
                slice_crcs: crcs.get(&id).cloned().unwrap_or_default(),
            }
        })
        .collect();
    Ok(Some(Par2Set {
        par_paths: par_files,
        set_id: [0; 16],
        root: dir.into(),
        slice_size,
        files,
        recovery_blocks: exponents.len() as u32,
        main_path,
    }))
}

/// CRC32 of `len` zero bytes (for the last-slice padding).
pub fn zero_crc(len: u64) -> u32 {
    let mut h = crc32fast::Hasher::new();
    let buf = [0u8; 8192];
    let mut left = len;
    while left > 0 {
        let n = left.min(8192) as usize;
        h.update(&buf[..n]);
        left -= n as u64;
    }
    h.finalize()
}

/// One file's quick check: does the padded whole-file CRC derived from
/// download evidence equal the fold of the par2 slice CRCs?
pub fn quick_check_file(f: &Par2File, slice_size: u64, disk_len: u64, whole_crc: u32) -> bool {
    if disk_len != f.length || f.slice_crcs.is_empty() || slice_size == 0 {
        return false;
    }
    let n_slices = f.length.div_ceil(slice_size);
    if f.slice_crcs.len() as u64 != n_slices {
        return false;
    }
    let mut expected: Option<u32> = None;
    for crc in &f.slice_crcs {
        expected = Some(match expected {
            None => *crc,
            Some(prev) => crc32_combine(prev, *crc, slice_size),
        });
    }
    let pad = n_slices * slice_size - f.length;
    let actual = if pad > 0 {
        crc32_combine(whole_crc, zero_crc(pad), pad)
    } else {
        whole_crc
    };
    expected == Some(actual)
}

/// Quick verification of a whole set against download evidence
/// (whole-file CRCs the engine combined from segments — zero re-reads).
pub fn quick_verify(set: &Par2Set, evidence: &[DownloadEvidence]) -> VerifyResult {
    let mut damaged = 0u32;
    for f in &set.files {
        let ev = evidence.iter().find(|e| e.path == set.root.join(&f.name));
        let ok = match ev {
            Some(e) => match e.crc32 {
                Some(crc) => {
                    let disk_len = std::fs::metadata(&e.path).map(|m| m.len()).unwrap_or(0);
                    quick_check_file(f, set.slice_size, disk_len, crc)
                }
                None => false, // holes: whole-file CRC unknown
            },
            None => false, // file missing entirely
        };
        if !ok {
            damaged += 1;
        }
    }
    if damaged == 0 {
        VerifyResult::Intact
    } else {
        VerifyResult::Repairable {
            blocks_available: set.recovery_blocks,
            blocks_needed: 0, // unknown until a full verify counts them
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use std::process::Command;

    fn crc(data: &[u8]) -> u32 {
        let mut h = crc32fast::Hasher::new();
        h.update(data);
        h.finalize()
    }

    #[test]
    fn zero_crc_matches_direct() {
        for n in [0u64, 1, 100, 8192, 20000] {
            assert_eq!(zero_crc(n), crc(&vec![0u8; n as usize]), "n={n}");
        }
    }

    #[test]
    fn parse_and_quick_verify_real_par2() {
        if !crate::tools::require_tool("par2") {
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        let data: Vec<u8> = (0..50_000u32).map(|i| (i * 31 % 251) as u8).collect();
        let file = tmp.path().join("payload.bin");
        std::fs::write(&file, &data).unwrap();
        let ok = Command::new("par2")
            .args([
                "create",
                "-q",
                "-q",
                "-s8192",
                "-c8",
                "set.par2",
                "payload.bin",
            ])
            .current_dir(tmp.path())
            .status()
            .unwrap()
            .success();
        assert!(ok, "par2 create failed");

        let set = load_dir(tmp.path()).unwrap().expect("set parsed");
        assert_eq!(set.slice_size, 8192);
        assert_eq!(set.files.len(), 1);
        assert_eq!(set.files[0].name, "payload.bin");
        assert_eq!(set.files[0].length, 50_000);
        assert_eq!(set.files[0].slice_crcs.len(), 7); // ceil(50000/8192)
        assert_eq!(set.recovery_blocks, 8);
        assert!(set.main_path.as_ref().unwrap().ends_with("set.par2"));

        // Quick verify from "download evidence" — the whole-file CRC only.
        let ev = vec![DownloadEvidence {
            path: file.clone(),
            crc32: Some(crc(&data)),
            segment_crcs: vec![],
        }];
        assert_eq!(quick_verify(&set, &ev), VerifyResult::Intact);

        // A single flipped byte must fail the quick check.
        let mut bad = data.clone();
        bad[25_000] ^= 0xFF;
        let ev_bad = vec![DownloadEvidence {
            path: file.clone(),
            crc32: Some(crc(&bad)),
            segment_crcs: vec![],
        }];
        match quick_verify(&set, &ev_bad) {
            VerifyResult::Repairable {
                blocks_available, ..
            } => assert_eq!(blocks_available, 8),
            other => panic!("expected damage, got {other:?}"),
        }

        // Unknown whole-file CRC (holes) is treated as damage.
        let ev_none = vec![DownloadEvidence {
            path: file,
            crc32: None,
            segment_crcs: vec![],
        }];
        assert!(matches!(
            quick_verify(&set, &ev_none),
            VerifyResult::Repairable { .. }
        ));
    }

    #[test]
    fn malformed_sets_and_incomplete_evidence_fail_closed() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("junk.par2"), b"not a par2 set").unwrap();
        symlink(
            tmp.path().join("missing-target"),
            tmp.path().join("unreadable.par2"),
        )
        .unwrap();
        assert!(load_dir(tmp.path()).is_err());

        let data = b"abc";
        let file = Par2File {
            md5_full: [0; 16],
            id: [1; 16],
            name: "payload.bin".into(),
            length: data.len() as u64,
            md5_16k: [2; 16],
            slice_crcs: vec![crc(data)],
        };
        assert!(quick_check_file(&file, 3, 3, crc(data)));
        assert!(!quick_check_file(&file, 3, 2, crc(data)));
        assert!(!quick_check_file(&file, 0, 3, crc(data)));
        let mut wrong_slices = file.clone();
        wrong_slices.slice_crcs.push(crc(data));
        assert!(!quick_check_file(&wrong_slices, 3, 3, crc(data)));

        let set = Par2Set {
            par_paths: vec![],
            set_id: [0; 16],
            root: tmp.path().into(),
            slice_size: 3,
            files: vec![file],
            recovery_blocks: 7,
            main_path: None,
        };
        assert_eq!(
            quick_verify(&set, &[]),
            VerifyResult::Repairable {
                blocks_available: 7,
                blocks_needed: 0,
            }
        );
    }
}
