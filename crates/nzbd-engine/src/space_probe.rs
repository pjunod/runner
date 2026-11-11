//! W5a: the evidence that clears a write-observed out-of-space latch.
//!
//! statvfs on a Gluster distribute mount sums its bricks, so it reported
//! 132 GB free while every write to the full nuc3 brick failed, and a latch
//! cleared by "twice the floor free" flapped every 30 s. The failing layer is
//! the only honest witness: the latch clears once real writes, each fsynced,
//! succeed in the directories that reported ENOSPC. Several distinct names
//! are written because a distribute volume places each file by its name.

use std::io::Write;
use std::path::{Path, PathBuf};

/// Files per directory; each lands on the brick its name hashes to.
const PROBE_FILES: usize = 8;
/// Bytes per file: enough that allocation, not just a dirent, is exercised.
const PROBE_BYTES: usize = 1024 * 1024;

/// Write, fsync and remove `PROBE_FILES` probe files in every directory.
/// Any failure (ENOSPC or otherwise) is the answer.
pub(crate) fn probe(dirs: &[PathBuf]) -> Result<(), String> {
    if dirs.is_empty() {
        return Err("no directory to probe".into());
    }
    let block = vec![0x5au8; PROBE_BYTES];
    for dir in dirs {
        for index in 0..PROBE_FILES {
            probe_one(dir, index, &block)
                .map_err(|error| format!("probe write in {}: {error}", dir.display()))?;
        }
    }
    Ok(())
}

fn probe_one(dir: &Path, index: usize, block: &[u8]) -> std::io::Result<()> {
    let path = dir.join(format!(".nzbd-space-probe-{}-{index}", std::process::id()));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.write_all(block)?;
        file.sync_all()
    })();
    let removed = std::fs::remove_file(&path);
    result?;
    match removed {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

/// The directory a write failure names, when the report carries a path.
pub(crate) fn dir_of(path: &Path) -> PathBuf {
    if path.is_dir() {
        path.to_path_buf()
    } else {
        path.parent().map(Path::to_path_buf).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_writes_syncs_and_removes() {
        let dir = tempfile::tempdir().unwrap();
        probe(&[dir.path().to_path_buf()]).unwrap();
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn probe_fails_where_writes_fail() {
        let dir = tempfile::tempdir().unwrap();
        assert!(probe(&[dir.path().join("missing")]).is_err());
        assert!(probe(&[]).is_err());
    }
}
