//! Durable, no-replace directory publication. File visibility may be incremental
//! on filesystems without flagged rename; callers publish completion only after
//! this operation has verified and synced the complete destination.
use super::*;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PublishedDirectory {
    pub identity: Identity,
    pub files: Vec<FileEntry>,
    pub source_retained: bool,
}
#[derive(Clone, Serialize, Deserialize)]
struct Publication {
    source: PathBuf,
    source_identity: Identity,
    target: PathBuf,
    target_parent: Identity,
    files: Vec<FileEntry>,
    linked: bool,
    directories: BTreeMap<String, Identity>,
    published: Option<PublishedDirectory>,
    #[serde(default)]
    retirement_done: bool,
}

fn parent(root: &File, relative: &str) -> Result<(File, PathBuf)> {
    let path = Path::new(relative);
    let name = path
        .file_name()
        .ok_or_else(|| Error::Conflict("empty publication name".into()))?;
    let dir = match path.parent().filter(|p| !p.as_os_str().is_empty()) {
        Some(p) => fs::open_relative(root, &p.to_string_lossy())?,
        None => root.try_clone()?,
    };
    Ok((dir, PathBuf::from(name)))
}

#[cfg(unix)]
fn mkdir_at(parent: &File, name: &Path) -> Result<File> {
    use std::os::{fd::AsRawFd, unix::ffi::OsStrExt};
    let name_c = std::ffi::CString::new(name.as_os_str().as_bytes())
        .map_err(|_| Error::Conflict("invalid directory name".into()))?;
    if unsafe { libc::mkdirat(parent.as_raw_fd(), name_c.as_ptr(), 0o755) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    fs::sync_directory(parent)?;
    fs::open_at(parent, name, true)
}
#[cfg(not(unix))]
fn mkdir_at(_: &File, _: &Path) -> Result<File> {
    Err(Error::Conflict(
        "descriptor directory publication unsupported".into(),
    ))
}

#[cfg(unix)]
fn link_file(source: &File, target: &File, entry: &FileEntry) -> Result<()> {
    use std::os::{fd::AsRawFd, unix::ffi::OsStrExt};
    let (src, name) = parent(source, &entry.path)?;
    let (dst, _) = parent(target, &entry.path)?;
    let input = fs::open_at(&src, &name, false)?;
    if fs::identity(&input.metadata()?) != entry.identity {
        return Err(Error::Conflict("publication source file changed".into()));
    }
    input.sync_all()?;
    let name_c = std::ffi::CString::new(name.as_os_str().as_bytes())
        .map_err(|_| Error::Conflict("invalid publication file name".into()))?;
    if unsafe {
        libc::linkat(
            src.as_raw_fd(),
            name_c.as_ptr(),
            dst.as_raw_fd(),
            name_c.as_ptr(),
            0,
        )
    } != 0
    {
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(err.into());
        }
    }
    let output = fs::open_at(&dst, &name, false)?;
    if fs::identity(&output.metadata()?) != entry.identity {
        return Err(Error::Conflict(
            "publication target file conflicts with source".into(),
        ));
    }
    fs::sync_directory(&dst)?;
    Ok(())
}
#[cfg(not(unix))]
fn link_file(_: &File, _: &File, _: &FileEntry) -> Result<()> {
    Err(Error::Conflict(
        "descriptor link publication unsupported".into(),
    ))
}

fn verified_target(p: &Publication) -> Result<PublishedDirectory> {
    let root = fs::open_dir(&p.target)?;
    let identity = fs::identity(&root.metadata()?);
    let expected_root = if p.linked {
        p.directories.get("")
    } else {
        Some(&p.source_identity)
    };
    if !expected_root.is_some_and(|e| e.same_object(&identity)) {
        return Err(Error::Conflict(
            "publication target directory changed".into(),
        ));
    }
    let files = fs::manifest(&root, 100_000)?;
    if files.len() != p.files.len() {
        return Err(Error::Conflict(
            "publication manifest incomplete or extended".into(),
        ));
    }
    for (actual, expected) in files.iter().zip(&p.files) {
        if actual.path != expected.path || actual.identity.directory != expected.identity.directory
        {
            return Err(Error::Conflict("publication manifest changed".into()));
        }
        let valid = if expected.identity.directory {
            let identity = if p.linked {
                p.directories.get(&expected.path)
            } else {
                Some(&expected.identity)
            };
            identity.is_some_and(|i| i.same_object(&actual.identity))
        } else {
            actual.identity == expected.identity
        };
        if !valid {
            return Err(Error::Conflict("publication entry identity changed".into()));
        }
    }
    Ok(PublishedDirectory {
        identity,
        files,
        source_retained: p.linked,
    })
}

#[cfg(test)]
thread_local! { pub(super) static RETIRE_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

impl Inventory {
    /// Caller holds the inventory mutation guard. No marker inside the payload
    /// grants ownership; every directory identity is checkpointed in SQLite.
    pub(super) fn publish_directory_unlocked(
        &self,
        source: &Path,
        target: &Path,
        artifact: &str,
        key: &str,
    ) -> Result<PublishedDirectory> {
        let key = format!("publish-{key}");
        let (mut op, mut p) = match self.operation(&key) {
            Ok(op) => {
                let p: Publication = serde_json::from_str(&op.request)?;
                if op.artifact != artifact
                    || p.source != source
                    || p.target != target
                    || op.kind != "directory_publish"
                {
                    return Err(Error::Conflict(
                        "publication request identity changed".into(),
                    ));
                }
                (op, p)
            }
            Err(Error::NotFound) => {
                fs::absolute(source)?;
                fs::absolute(target)?;
                let dir = fs::open_dir(source)?;
                let root = fs::open_dir(
                    target
                        .parent()
                        .ok_or_else(|| Error::Conflict("missing target parent".into()))?,
                )?;
                let p = Publication {
                    source: source.into(),
                    source_identity: fs::identity(&dir.metadata()?),
                    target: target.into(),
                    target_parent: fs::identity(&root.metadata()?),
                    files: fs::manifest(&dir, 100_000)?,
                    linked: false,
                    directories: BTreeMap::new(),
                    published: None,
                    retirement_done: false,
                };
                let op = Operation {
                    id: key,
                    artifact: artifact.into(),
                    kind: "directory_publish".into(),
                    state: "running".into(),
                    request: serde_json::to_string(&p)?,
                    created_at: now(),
                    not_before: 0,
                    attempts: 1,
                    next_retry: 0,
                    error: None,
                };
                save_operation(&self.db.lock().unwrap(), &op)?;
                (op, p)
            }
            Err(e) => return Err(e),
        };
        if op.state == "cancelled" {
            return Err(Error::Conflict("publication was cancelled".into()));
        }
        let target_parent = fs::open_dir(target.parent().unwrap())?;
        if !p
            .target_parent
            .same_object(&fs::identity(&target_parent.metadata()?))
        {
            return Err(Error::Conflict("publication parent changed".into()));
        }
        if p.published.is_some() {
            return verified_target(&p);
        }
        let persist = |op: &mut Operation, p: &Publication| -> Result<()> {
            op.request = serde_json::to_string(p)?;
            save_operation(&self.db.lock().unwrap(), op)
        };
        if !p.linked {
            // A crash after atomic rename but before acknowledgement is proven
            // by the original directory identity and full captured manifest.
            if source
                .symlink_metadata()
                .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound)
            {
                p.published = Some(verified_target(&p)?);
            } else {
                let dir = fs::open_dir(source)?;
                if !p
                    .source_identity
                    .same_object(&fs::identity(&dir.metadata()?))
                    || fs::manifest(&dir, 100_000)? != p.files
                {
                    return Err(Error::Conflict("publication source changed".into()));
                }
                match fs::rename_exclusive(source, target) {
                    Ok(()) => p.published = Some(verified_target(&p)?),
                    Err(Error::Io(e))
                        if matches!(
                            e.raw_os_error(),
                            Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP)
                        ) =>
                    {
                        p.linked = true;
                        persist(&mut op, &p)?;
                    }
                    Err(e) => {
                        if matches!(&e, Error::Io(io) if io.kind() == std::io::ErrorKind::CrossesDevices)
                        {
                            op.state = "cancelled".into();
                            op.error = Some(
                                "cross-filesystem move requires destination-local scratch".into(),
                            );
                            persist(&mut op, &p)?;
                        }
                        return Err(e);
                    }
                }
            }
        }
        if p.linked {
            let src = fs::open_dir(source)?;
            if !p
                .source_identity
                .same_object(&fs::identity(&src.metadata()?))
                || fs::manifest(&src, 100_000)? != p.files
            {
                return Err(Error::Conflict("publication source changed".into()));
            }
            let root = if let Some(expected) = p.directories.get("") {
                let dir = fs::open_dir(target)?;
                if !expected.same_object(&fs::identity(&dir.metadata()?)) {
                    return Err(Error::Conflict("publication root replaced".into()));
                }
                dir
            } else {
                // If creation survived but its checkpoint did not, EEXIST
                // requires review. Never infer ownership of that directory.
                let dir = mkdir_at(&target_parent, Path::new(target.file_name().unwrap()))?;
                p.directories
                    .insert(String::new(), fs::identity(&dir.metadata()?));
                persist(&mut op, &p)?;
                dir
            };
            for entry in p.files.clone().iter().filter(|e| e.identity.directory) {
                if let Some(expected) = p.directories.get(&entry.path) {
                    let dir = fs::open_relative(&root, &entry.path)?;
                    if !expected.same_object(&fs::identity(&dir.metadata()?)) {
                        return Err(Error::Conflict("publication subdirectory replaced".into()));
                    }
                } else {
                    let (dir, name) = parent(&root, &entry.path)?;
                    let child = mkdir_at(&dir, &name)?;
                    p.directories
                        .insert(entry.path.clone(), fs::identity(&child.metadata()?));
                    persist(&mut op, &p)?;
                }
            }
            for entry in p.files.iter().filter(|e| !e.identity.directory) {
                link_file(&src, &root, entry)?;
            }
            for entry in p.files.iter().rev().filter(|e| e.identity.directory) {
                fs::sync_directory(&fs::open_relative(&root, &entry.path)?)?;
            }
            fs::sync_directory(&root)?;
            fs::sync_directory(&target_parent)?;
            p.published = Some(verified_target(&p)?);
        }
        op.state = "succeeded".into();
        persist(&mut op, &p)?;
        Ok(p.published.unwrap())
    }

    /// Only after the owning operation commits. A partial cleanup can resume;
    /// unexpected files or identities are retained for explicit review.
    pub(super) fn retire_publication_source_unlocked(&self, key: &str) -> Result<()> {
        let mut op = self.operation(&format!("publish-{key}"))?;
        let mut p: Publication = serde_json::from_str(&op.request)?;
        if p.retirement_done {
            return Ok(());
        }
        let result = self.retire_publication_entries_unlocked(key);
        p.retirement_done = result.is_ok();
        op.error = result
            .as_ref()
            .err()
            .map(|e| format!("publication ready; staging cleanup pending: {e}"));
        op.request = serde_json::to_string(&p)?;
        save_operation(&self.db.lock().unwrap(), &op)?;
        result
    }

    fn retire_publication_entries_unlocked(&self, key: &str) -> Result<()> {
        #[cfg(test)]
        if RETIRE_FAILURE.with(|f| f.replace(false)) {
            return Err(std::io::Error::from_raw_os_error(libc::EIO).into());
        }
        let op = self.operation(&format!("publish-{key}"))?;
        let p: Publication = serde_json::from_str(&op.request)?;
        if op.state != "succeeded" || !p.published.as_ref().is_some_and(|r| r.source_retained) {
            return Ok(());
        }
        let dir = match fs::open_dir(&p.source) {
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            other => other?,
        };
        if !p
            .source_identity
            .same_object(&fs::identity(&dir.metadata()?))
        {
            return Err(Error::Conflict(
                "publication cleanup source replaced".into(),
            ));
        }
        let entries = fs::manifest(&dir, 100_000)?;
        for entry in &entries {
            if !p.files.iter().any(|expected| {
                expected.path == entry.path
                    && expected.identity.same_object(&entry.identity)
                    && (entry.identity.directory || entry.identity == expected.identity)
            }) {
                return Err(Error::Conflict(
                    "publication cleanup source contents changed".into(),
                ));
            }
        }
        for entry in entries.iter().rev() {
            fs::remove_entry(&dir, entry)?;
        }
        let parent = fs::open_dir(p.source.parent().unwrap())?;
        let current = fs::open_at(&parent, Path::new(p.source.file_name().unwrap()), true)?;
        if !p
            .source_identity
            .same_object(&fs::identity(&current.metadata()?))
        {
            return Err(Error::Conflict("publication cleanup path replaced".into()));
        }
        fs::unlink(&parent, Path::new(p.source.file_name().unwrap()), true)?;
        fs::sync_directory(&parent)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (tempfile::TempDir, Inventory, PathBuf, PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        let source = temp.path().join("source");
        std::fs::create_dir(&source).unwrap();
        std::fs::create_dir(source.join("nested")).unwrap();
        std::fs::write(source.join("nested/media.mkv"), b"original").unwrap();
        let target = temp.path().join("target");
        (temp, inventory, source, target)
    }
    #[test]
    fn linked_publication_is_replayable_and_retirement_is_separate() {
        let (temp, inventory, source, target) = fixture();
        fs::RENAME_FAILURE.with(|f| f.set(Some(libc::EINVAL)));
        let result = inventory
            .publish_directory_unlocked(&source, &target, "test", "one")
            .unwrap();
        assert!(result.source_retained);
        assert!(source.join("nested/media.mkv").exists());
        assert_eq!(
            fs::identity(&std::fs::metadata(source.join("nested/media.mkv")).unwrap()),
            fs::identity(&std::fs::metadata(target.join("nested/media.mkv")).unwrap())
        );
        drop(inventory);
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        inventory
            .publish_directory_unlocked(&source, &target, "test", "one")
            .unwrap();
        inventory.retire_publication_source_unlocked("one").unwrap();
        inventory.retire_publication_source_unlocked("one").unwrap();
        assert!(!source.exists());
        assert_eq!(
            std::fs::read(target.join("nested/media.mkv")).unwrap(),
            b"original"
        );
    }
    #[test]
    fn interrupted_links_resume_but_uncheckpointed_directories_do_not() {
        let (_temp, inventory, source, target) = fixture();
        fs::RENAME_FAILURE.with(|f| f.set(Some(libc::EINVAL)));
        inventory
            .publish_directory_unlocked(&source, &target, "test", "one")
            .unwrap();
        let mut op = inventory.operation("publish-one").unwrap();
        let mut p: Publication = serde_json::from_str(&op.request).unwrap();
        p.published = None;
        op.state = "running".into();
        std::fs::remove_file(target.join("nested/media.mkv")).unwrap();
        op.request = serde_json::to_string(&p).unwrap();
        save_operation(&inventory.db.lock().unwrap(), &op).unwrap();
        inventory
            .publish_directory_unlocked(&source, &target, "test", "one")
            .unwrap();
        p.directories.clear();
        op.request = serde_json::to_string(&p).unwrap();
        save_operation(&inventory.db.lock().unwrap(), &op).unwrap();
        assert!(inventory
            .publish_directory_unlocked(&source, &target, "test", "one")
            .is_err());
        assert_eq!(
            std::fs::read(target.join("nested/media.mkv")).unwrap(),
            b"original"
        );
    }
    #[test]
    fn empty_foreign_directory_and_changed_file_are_never_replaced() {
        let (_temp, inventory, source, target) = fixture();
        std::fs::create_dir(&target).unwrap();
        let before = fs::identity(&std::fs::metadata(&target).unwrap());
        fs::RENAME_FAILURE.with(|f| f.set(Some(libc::EINVAL)));
        assert!(inventory
            .publish_directory_unlocked(&source, &target, "test", "one")
            .is_err());
        assert!(before.same_object(&fs::identity(&std::fs::metadata(&target).unwrap())));
        assert!(std::fs::read_dir(&target).unwrap().next().is_none());
    }
}
