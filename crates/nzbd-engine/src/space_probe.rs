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

/// One guarded root as it was when a failure was reported.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct GuardedVolume {
    /// A configured write root (one member of a grouped filesystem, never
    /// the group's common path: two bind mounts of one filesystem share a
    /// device but their common path is another filesystem).
    pub root: PathBuf,
    /// The device the root was on — the volume a write had just reached.
    pub device: Option<u64>,
    /// The device of the filesystem holding the root's parent: what the
    /// root shows when its volume is not mounted.
    pub parent_device: Option<u64>,
}

/// One recorded failure.
#[derive(Debug, Clone)]
pub(crate) struct SpaceFailure {
    /// The file whose write failed (Write) or the directory (Directory).
    pub path: Option<PathBuf>,
    /// The engine's ENOSPC count when this failure was last reported: a
    /// probe started before a newer report of the same failure proves nothing.
    pub observed: u64,
    /// The guarded volumes as they were when the failure was reported — the
    /// mount was there, a write had just reached it. A probe never walks
    /// above the root containing its target, and an unmounted volume (its
    /// root now on the parent filesystem, or missing) is never mistaken for
    /// one with room. A remount (a new device that is not the parent's)
    /// rebinds the recorded device.
    pub volumes: Vec<GuardedVolume>,
}

impl SpaceFailure {
    /// The bound for a probe of `path`: the deepest recorded volume root
    /// containing it, with its recorded devices.
    pub fn bound(&self, path: &Path) -> Bound {
        self.volumes
            .iter()
            .filter(|volume| !volume.root.as_os_str().is_empty() && path.starts_with(&volume.root))
            .max_by_key(|volume| volume.root.components().count())
            .map(|volume| Bound {
                root: Some(volume.root.clone()),
                device: volume.device,
                parent_device: volume.parent_device,
            })
            .unwrap_or_default()
    }

    /// Every guarded volume, once: one member root per known device (they
    /// are one filesystem), every root whose device is unknown.
    pub fn probe_roots(&self) -> Vec<(PathBuf, Bound)> {
        let mut seen = std::collections::HashSet::new();
        self.volumes
            .iter()
            .filter(|volume| volume.device.is_none_or(|device| seen.insert(device)))
            .map(|volume| (volume.root.clone(), self.bound(&volume.root)))
            .collect()
    }

    /// Apply a probe's rebinds: the volume at `root` is mounted again on
    /// `device`.
    pub fn rebind(&mut self, rebound: &[(PathBuf, u64)]) {
        for (root, device) in rebound {
            for volume in self
                .volumes
                .iter_mut()
                .filter(|volume| &volume.root == root)
            {
                volume.device = Some(*device);
            }
        }
    }
}

/// Where a probe may look: never above `root`, and only on `device`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Bound {
    /// The guarded root containing the target; None when no guarded root
    /// does (the target itself is then the only place to look).
    pub root: Option<PathBuf>,
    /// The device the failing write reached, when known.
    pub device: Option<u64>,
    /// The device under the root's mount point, when known.
    pub parent_device: Option<u64>,
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
    /// The device of `path` when it is an existing directory.
    fn dir_device(&self, path: &Path) -> Option<u64> {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(path)
            .ok()
            .filter(std::fs::Metadata::is_dir)
            .map(|metadata| metadata.dev())
    }
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
    File(PathBuf, Bound),
    Dirs(Vec<(PathBuf, Bound)>),
}

/// One probe's answer: the failure, the count it was planned under, what
/// it found, and the roots found mounted again on a new device.
pub(crate) type Probed = (SpaceOp, u64, ProbeResult, Vec<(PathBuf, u64)>);

/// Run the planned probes; each answers for its own failure only.
pub(crate) fn run(plan: &[Planned], fs: &dyn ProbeFs) -> Vec<Probed> {
    plan.iter()
        .map(|planned| {
            let mut rebound = Vec::new();
            let mut mounted = |bound: &Bound| -> Result<Bound, String> {
                let current = mounted_bound(bound, fs)?;
                if let (Some(root), Some(device)) = (&current.root, current.device) {
                    if current.device != bound.device {
                        rebound.push((root.clone(), device));
                    }
                }
                Ok(current)
            };
            let result = match &planned.target {
                Target::File(path, bound) => {
                    let parent = path.parent().unwrap_or(path);
                    match mounted(bound).map(|bound| locate(parent, &bound, fs)) {
                        Err(reason) | Ok(Located::Unavailable(reason)) => {
                            ProbeResult::Holds(reason)
                        }
                        // The volume is there and the file's directory is
                        // gone: so is the file.
                        Ok(Located::Gone) => ProbeResult::Gone,
                        Ok(Located::Dir(_)) => fs.probe_file(path),
                    }
                }
                Target::Dirs(dirs) => {
                    let mut result = ProbeResult::Gone;
                    for (dir, bound) in dirs {
                        match mounted(bound).map(|bound| locate(dir, &bound, fs)) {
                            Ok(Located::Dir(dir)) => match fs.probe_dir(&dir) {
                                ProbeResult::Cleared => result = ProbeResult::Cleared,
                                other => {
                                    result = other;
                                    break;
                                }
                            },
                            Err(reason) | Ok(Located::Unavailable(reason)) => {
                                result = ProbeResult::Holds(reason);
                                break;
                            }
                            Ok(Located::Gone) => continue,
                        }
                    }
                    result
                }
            };
            rebound.sort();
            rebound.dedup();
            (planned.op.clone(), planned.observed, result, rebound)
        })
        .collect()
}

/// Is the guarded root's volume mounted now, and on which device? The root
/// missing, or on its parent's device, is an unmounted volume: not evidence.
/// Any other device than the recorded one is the volume mounted again (an
/// NFS/autofs remount gets a new device): the bound is rebound to it.
pub(crate) fn mounted_bound(bound: &Bound, fs: &dyn ProbeFs) -> Result<Bound, String> {
    let (Some(root), Some(recorded)) = (&bound.root, bound.device) else {
        return Ok(bound.clone());
    };
    match fs.dir_device(root) {
        None => Err(format!(
            "guarded root {} is missing (not mounted?)",
            root.display()
        )),
        Some(now) if now == recorded => Ok(bound.clone()),
        Some(now) if bound.parent_device == Some(now) => Err(format!(
            "guarded root {} is on its parent filesystem (not mounted?)",
            root.display()
        )),
        Some(now) => Ok(Bound {
            device: Some(now),
            ..bound.clone()
        }),
    }
}

/// Where [`locate`] found the place to probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Located {
    /// This existing directory, on the recorded device.
    Dir(PathBuf),
    /// Nothing exists and no guarded root bounds it: the object is gone.
    Gone,
    /// The guarded root is missing or is not the recorded device (an
    /// unmounted volume): not evidence of anything, and not gone either.
    Unavailable(String),
}

/// Finding 2: a failing directory removed later is probed through its
/// nearest existing ancestor — but never above the guarded root containing
/// it, and only on the device the failure was recorded on. An unmounted
/// volume leaves its mount point on the parent filesystem; probing that
/// would report room the failed volume does not have.
pub(crate) fn locate(dir: &Path, bound: &Bound, fs: &dyn ProbeFs) -> Located {
    let mut current = dir;
    loop {
        if !current.as_os_str().is_empty() {
            if let Some(device) = fs.dir_device(current) {
                if bound.device.is_some_and(|recorded| recorded != device) {
                    return Located::Unavailable(format!(
                        "{} is not on the volume that failed (not mounted?)",
                        current.display()
                    ));
                }
                return Located::Dir(current.to_path_buf());
            }
        }
        let Some(root) = &bound.root else {
            return Located::Gone;
        };
        match current.parent() {
            Some(parent) if current != root.as_path() && parent.starts_with(root) => {
                current = parent;
            }
            _ => {
                return Located::Unavailable(format!(
                    "guarded root {} is missing (not mounted?)",
                    root.display()
                ))
            }
        }
    }
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
            // Truncating to the current size frees the post-EOF blocks. Read
            // it again: never cut bytes a writer appended meanwhile, nor
            // re-extend a file truncated meanwhile (the owner skips files
            // with a live writer; this closes the rest).
            let current = file.metadata().map_or(len, |metadata| metadata.len());
            let released = file.set_len(current);
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

    fn bound(root: &Path, device: Option<u64>) -> Bound {
        Bound {
            root: Some(root.to_path_buf()),
            device,
            parent_device: None,
        }
    }

    fn device_of(path: &Path) -> u64 {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(path).unwrap().dev()
    }

    /// Finding 2: a deleted failing directory is probed via its nearest
    /// existing ancestor instead of failing NotFound forever — within its
    /// guarded root.
    #[test]
    fn a_removed_directory_is_probed_through_its_ancestor() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        std::fs::create_dir(&root).unwrap();
        let gone = root.join("job/sub");
        let bound = bound(&root, Some(device_of(&root)));
        assert_eq!(locate(&gone, &bound, &RealFs), Located::Dir(root.clone()));
        let plan = [Planned {
            op: SpaceOp::Directory(gone.clone()),
            observed: 1,
            target: Target::Dirs(vec![(gone, bound)]),
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

    /// Injected device identities: paths under `mount` report `volume`,
    /// everything else `parent`; existence is the real filesystem's.
    struct Devices {
        mount: PathBuf,
        volume: std::sync::atomic::AtomicU64,
    }
    const PARENT: u64 = 1;
    const VOLUME: u64 = 10;
    impl Devices {
        fn set(&self, device: u64) {
            self.volume
                .store(device, std::sync::atomic::Ordering::SeqCst);
        }
    }
    impl ProbeFs for Devices {
        fn probe_file(&self, path: &Path) -> ProbeResult {
            RealFs.probe_file(path)
        }
        fn probe_dir(&self, dir: &Path) -> ProbeResult {
            RealFs.probe_dir(dir)
        }
        fn dir_device(&self, path: &Path) -> Option<u64> {
            RealFs.dir_device(path)?;
            Some(if path.starts_with(&self.mount) {
                self.volume.load(std::sync::atomic::Ordering::SeqCst)
            } else {
                PARENT
            })
        }
    }

    /// Finding 5: a probe never walks above the guarded root. An unmounted
    /// volume (the root on its parent's device), a missing root, and a
    /// missing .part on a dropped mount all hold — none reports Cleared or
    /// Gone. A remount (any other new device) is the volume again: it is
    /// probed normally and the recorded device is rebound.
    #[test]
    fn an_unmounted_or_missing_root_is_not_evidence_and_a_remount_rebinds() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("mnt");
        std::fs::create_dir_all(root.join("job")).unwrap();
        let part = root.join("job/.runner-file-1.part");
        std::fs::write(&part, b"abc").unwrap();
        let recorded = Bound {
            root: Some(root.clone()),
            device: Some(VOLUME),
            parent_device: Some(PARENT),
        };
        let fs = Devices {
            mount: root.clone(),
            volume: VOLUME.into(),
        };
        let probe = |target: Target, fs: &dyn ProbeFs| {
            run(
                &[Planned {
                    op: SpaceOp::Directory(root.clone()),
                    observed: 1,
                    target,
                }],
                fs,
            )
            .remove(0)
        };
        let targets = || {
            [
                Target::Dirs(vec![(root.join("job/gone"), recorded.clone())]),
                Target::File(part.clone(), recorded.clone()),
            ]
        };
        // Mounted: the targets answer, nothing rebinds.
        for target in targets() {
            let (_, _, result, rebound) = probe(target, &fs);
            assert_eq!(result, ProbeResult::Cleared);
            assert!(rebound.is_empty());
        }
        // Unmounted: the root is on its parent's device.
        fs.set(PARENT);
        for target in targets().into_iter().chain([Target::File(
            root.join("job/.runner-file-2.part"),
            recorded.clone(),
        )]) {
            assert!(
                matches!(probe(target, &fs).2, ProbeResult::Holds(_)),
                "unmounted is neither cleared nor gone"
            );
        }
        // Remounted (NFS/autofs: a new device): probed, and rebound.
        fs.set(VOLUME + 1);
        for target in targets() {
            let (_, _, result, rebound) = probe(target, &fs);
            assert_eq!(result, ProbeResult::Cleared, "a remount is not a wedge");
            assert_eq!(rebound, vec![(root.clone(), VOLUME + 1)]);
        }
        let mut failure = SpaceFailure {
            path: None,
            observed: 1,
            volumes: vec![GuardedVolume {
                root: root.clone(),
                device: Some(VOLUME),
                parent_device: Some(PARENT),
            }],
        };
        failure.rebind(&[(root.clone(), VOLUME + 1)]);
        assert_eq!(failure.bound(&part).device, Some(VOLUME + 1));
        // The root itself missing: never probes its parent.
        std::fs::remove_dir_all(&root).unwrap();
        for target in [
            Target::Dirs(vec![(root.join("job"), recorded.clone())]),
            Target::File(part.clone(), recorded.clone()),
        ] {
            assert!(matches!(probe(target, &fs).2, ProbeResult::Holds(_)));
        }
    }

    /// A failure with no guarded root around it: only the directory itself
    /// is looked at, and when it is gone so is the failure.
    #[test]
    fn an_unbounded_directory_is_gone_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        let gone = dir.path().join("gone");
        assert_eq!(locate(&gone, &Bound::default(), &RealFs), Located::Gone);
    }

    #[test]
    fn the_failure_records_its_deepest_volume() {
        let failure = SpaceFailure {
            path: None,
            observed: 1,
            volumes: vec![
                volume("/mnt", Some(1)),
                volume("/mnt/proc", Some(2)),
                volume("", None),
            ],
        };
        assert_eq!(
            failure.bound(Path::new("/mnt/proc/job/x.part")),
            bound(Path::new("/mnt/proc"), Some(2))
        );
        assert_eq!(failure.bound(Path::new("/srv/x")), Bound::default());
    }

    fn volume(root: &str, device: Option<u64>) -> GuardedVolume {
        GuardedVolume {
            root: PathBuf::from(root),
            device,
            parent_device: None,
        }
    }

    /// Review finding 2: two bind mounts of one filesystem (/downloads,
    /// /media) are recorded as their own member roots with the group's
    /// device — never the group's common path (`/`, another filesystem) —
    /// and an every-root probe probes one member per device.
    #[test]
    fn grouped_member_roots_are_bounded_and_probed_once() {
        let failure = SpaceFailure {
            path: None,
            observed: 1,
            volumes: vec![
                volume("/downloads", Some(7)),
                volume("/media", Some(7)),
                volume("/srv/done", None),
            ],
        };
        assert_eq!(
            failure.bound(Path::new("/media/tv/x.mkv")),
            bound(Path::new("/media"), Some(7))
        );
        assert_eq!(failure.bound(Path::new("/etc")), Bound::default());
        let roots: Vec<PathBuf> = failure
            .probe_roots()
            .into_iter()
            .map(|(root, bound)| {
                assert_eq!(bound.root.as_ref(), Some(&root));
                root
            })
            .collect();
        assert_eq!(
            roots,
            vec![PathBuf::from("/downloads"), PathBuf::from("/srv/done")]
        );
        // A bind mount: the probe of a member root on the group's device.
        let dir = tempfile::tempdir().unwrap();
        let downloads = dir.path().join("downloads");
        let media = dir.path().join("media");
        std::fs::create_dir_all(&downloads).unwrap();
        std::fs::create_dir_all(&media).unwrap();
        let device = device_of(&downloads);
        let failure = SpaceFailure {
            path: None,
            observed: 1,
            volumes: vec![
                GuardedVolume {
                    root: downloads,
                    device: Some(device),
                    parent_device: None,
                },
                GuardedVolume {
                    root: media,
                    device: Some(device),
                    parent_device: None,
                },
            ],
        };
        let results = run(
            &[Planned {
                op: SpaceOp::Directory(PathBuf::new()),
                observed: 1,
                target: Target::Dirs(failure.probe_roots()),
            }],
            &RealFs,
        );
        assert_eq!(results[0].2, ProbeResult::Cleared);
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
