//! W5a: the evidence that clears a write-observed out-of-space latch.
//!
//! statvfs on a Gluster distribute mount sums its bricks, so it reported
//! 132 GB free while every write to the full nuc3 brick failed. A probe that
//! writes *new* files proves nothing either: DHT places each new name by its
//! hash, so the probe can land on the other brick, succeed, clear the latch,
//! and the real write fails again — the latch flaps. The only honest witness
//! is the operation that failed, so each failure is recorded with what
//! failed ([`SpaceOp`]) and clears only on evidence from that same operation:
//!
//! * a job file's write — an allocation on **that same file object** (the
//!   partial the writer was extending) succeeds, or the writer's own next
//!   write to it lands;
//! * a job file's finalize — the job's own finalize retry succeeds (it is
//!   allowed to run while the latch holds: it fetches nothing);
//! * the terminal store — its own replay lands;
//! * a post-processing write — that job's own retry (its ladder) finishes;
//! * an unattributed report (the torrent backend, a bare report) — probe
//!   files in the directory, the only evidence such a report allows. Those
//!   files carry [`PROBE_PREFIX`], which the payload manifest ignores and the
//!   startup sweep removes.
//!
//! A failure whose operation no longer exists (the file or job is gone, the
//! directory was removed) is dropped: there is nothing left to retry, and a
//! new write that fails records a new failure.

use std::io::Write;
use std::path::{Path, PathBuf};

use nzbd_types::{FileId, JobId};

/// Name prefix of the directory probe files (unattributed reports only).
pub const PROBE_PREFIX: &str = nzbd_state::artifacts::SPACE_PROBE_PREFIX;
/// Files per directory probe; each lands where its name hashes to.
const PROBE_FILES: usize = 8;
/// Bytes allocated per probe: enough that allocation, not a dirent, is tested.
const PROBE_BYTES: usize = 1024 * 1024;

/// What failed with ENOSPC/EDQUOT — and so what has to succeed again.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SpaceOp {
    /// A segment write or sync to one job file's partial.
    Write { job: JobId, file: FileId },
    /// Publishing one job file (link, directory sync).
    Finalize { job: JobId, file: FileId },
    /// The terminal store / history database.
    Terminal,
    /// A post-processing write for one job.
    Post { job: JobId },
    /// A report with no operation attached; the path is the directory (empty
    /// for "every guarded root").
    Directory(PathBuf),
}

/// One recorded failure.
#[derive(Debug, Clone)]
pub(crate) struct SpaceFailure {
    /// The file whose write failed (Write) or the directory (Directory).
    pub path: Option<PathBuf>,
    /// The engine's ENOSPC count when this failure was last reported: a
    /// probe started before a newer report of the same failure proves nothing.
    pub observed: u64,
}

/// What one probe found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProbeResult {
    /// The same operation's resource accepted an allocation.
    Cleared,
    /// It still fails.
    Holds(String),
    /// The failing object no longer exists; nothing is left to retry.
    Gone,
}

/// The filesystem side of a probe (a fake in tests).
pub(crate) trait ProbeFs: Send + Sync {
    /// Allocate on the existing file itself, then give the space back.
    fn probe_file(&self, path: &Path) -> ProbeResult;
    /// Write, fsync and remove probe files in a directory.
    fn probe_dir(&self, dir: &Path) -> ProbeResult;
}

/// One planned probe.
#[derive(Debug, Clone)]
pub(crate) struct Planned {
    pub op: SpaceOp,
    pub observed: u64,
    pub target: Target,
}

#[derive(Debug, Clone)]
pub(crate) enum Target {
    File(PathBuf),
    Dirs(Vec<PathBuf>),
}

/// Run the planned probes; each answers for its own failure only.
pub(crate) fn run(plan: &[Planned], fs: &dyn ProbeFs) -> Vec<(SpaceOp, u64, ProbeResult)> {
    plan.iter()
        .map(|planned| {
            let result = match &planned.target {
                Target::File(path) => fs.probe_file(path),
                Target::Dirs(dirs) => {
                    let mut result = ProbeResult::Gone;
                    for dir in dirs {
                        match existing_ancestor(dir) {
                            Some(dir) => match fs.probe_dir(&dir) {
                                ProbeResult::Cleared => result = ProbeResult::Cleared,
                                other => {
                                    result = other;
                                    break;
                                }
                            },
                            None => continue,
                        }
                    }
                    result
                }
            };
            (planned.op.clone(), planned.observed, result)
        })
        .collect()
}

/// Finding 2: a failing directory removed later is probed through its
/// nearest existing ancestor (still on the volume that failed), never
/// reported as NotFound forever.
pub(crate) fn existing_ancestor(dir: &Path) -> Option<PathBuf> {
    let mut current = Some(dir);
    while let Some(path) = current {
        if !path.as_os_str().is_empty() && path.is_dir() {
            return Some(path.to_path_buf());
        }
        current = path.parent();
    }
    None
}

/// The real filesystem.
pub(crate) struct RealFs;

impl ProbeFs for RealFs {
    fn probe_file(&self, path: &Path) -> ProbeResult {
        match allocate_on(path) {
            Ok(()) => ProbeResult::Cleared,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => ProbeResult::Gone,
            Err(error) => {
                ProbeResult::Holds(format!("probe allocation on {}: {error}", path.display()))
            }
        }
    }

    fn probe_dir(&self, dir: &Path) -> ProbeResult {
        let block = vec![0x5au8; PROBE_BYTES];
        for index in 0..PROBE_FILES {
            if let Err(error) = probe_one(dir, index, &block) {
                return ProbeResult::Holds(format!("probe write in {}: {error}", dir.display()));
            }
        }
        ProbeResult::Cleared
    }
}

/// Allocate `PROBE_BYTES` past the end of the existing file (the same object,
/// so the same brick), fsync, then give the allocation back. The file's
/// visible size and contents are unchanged.
fn allocate_on(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::{FileExt, OpenOptionsExt};
    let file = std::fs::OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(std::io::ErrorKind::NotFound.into());
    }
    let len = metadata.len();
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let offset = libc::off_t::try_from(len).map_err(std::io::Error::other)?;
        // SAFETY: plain syscall on an owned descriptor.
        let rc = unsafe {
            libc::fallocate(
                file.as_raw_fd(),
                libc::FALLOC_FL_KEEP_SIZE,
                offset,
                PROBE_BYTES as libc::off_t,
            )
        };
        if rc == 0 {
            let synced = file.sync_data();
            // Truncating to the unchanged size frees the post-EOF blocks.
            let released = file.set_len(len);
            synced?;
            if let Err(error) = released {
                tracing::warn!(path = %path.display(), %error, "space probe could not release its preallocation");
            }
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if !matches!(
            error.raw_os_error(),
            Some(libc::EOPNOTSUPP | libc::ENOSYS | libc::EINVAL)
        ) {
            return Err(error);
        }
    }
    // No fallocate: write past the end, fsync, and truncate back.
    let block = vec![0u8; PROBE_BYTES];
    let written = file
        .write_all_at(&block, len)
        .and_then(|()| file.sync_data());
    let restored = file.set_len(len).and_then(|()| file.sync_data());
    written?;
    restored
}

fn probe_one(dir: &Path, index: usize, block: &[u8]) -> std::io::Result<()> {
    let path = dir.join(format!("{PROBE_PREFIX}{}-{index}", std::process::id()));
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

/// Remove directory probe files a killed run left in `dir` (startup).
pub(crate) fn sweep_probe_files(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let is_probe = entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(PROBE_PREFIX));
        if is_probe
            && entry.file_type().is_ok_and(|kind| kind.is_file())
            && std::fs::remove_file(entry.path()).is_ok()
        {
            removed += 1;
        }
    }
    removed
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
    fn file_probe_allocates_on_the_same_file_and_leaves_it_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join(".runner-file-1.part");
        std::fs::write(&part, b"abc").unwrap();
        assert_eq!(RealFs.probe_file(&part), ProbeResult::Cleared);
        assert_eq!(std::fs::read(&part).unwrap(), b"abc");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
        assert_eq!(
            RealFs.probe_file(&dir.path().join("missing.part")),
            ProbeResult::Gone
        );
    }

    #[test]
    fn dir_probe_writes_syncs_and_removes() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(RealFs.probe_dir(dir.path()), ProbeResult::Cleared);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    /// Finding 2: a deleted failing directory is probed via its nearest
    /// existing ancestor instead of failing NotFound forever.
    #[test]
    fn a_removed_directory_is_probed_through_its_ancestor() {
        let dir = tempfile::tempdir().unwrap();
        let gone = dir.path().join("job/sub");
        assert_eq!(existing_ancestor(&gone).unwrap(), dir.path());
        let plan = [Planned {
            op: SpaceOp::Directory(gone.clone()),
            observed: 1,
            target: Target::Dirs(vec![gone]),
        }];
        let results = run(&plan, &RealFs);
        assert_eq!(results[0].2, ProbeResult::Cleared);
        let results = run(
            &[Planned {
                op: SpaceOp::Directory(PathBuf::new()),
                observed: 1,
                target: Target::Dirs(vec![]),
            }],
            &RealFs,
        );
        assert_eq!(results[0].2, ProbeResult::Gone);
    }

    #[test]
    fn startup_sweep_removes_leftover_probe_files_only() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(format!("{PROBE_PREFIX}9-0")), b"x").unwrap();
        std::fs::write(dir.path().join("keep.mkv"), b"x").unwrap();
        assert_eq!(sweep_probe_files(dir.path()), 1);
        assert!(dir.path().join("keep.mkv").exists());
    }
}
