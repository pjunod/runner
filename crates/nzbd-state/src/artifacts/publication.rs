//! Durable, no-replace directory publication. File visibility may be incremental
//! on filesystems without flagged rename; callers publish completion only after
//! this operation has verified and synced the complete destination.
use super::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Read;

fn digest_file(root: &File, path: &str, expected: &Identity) -> Result<String> {
    #[cfg(test)]
    if DIGEST_FAILURE.with(|failure| failure.replace(false)) {
        return Err(std::io::Error::from_raw_os_error(libc::EIO).into());
    }
    #[cfg(test)]
    let gate = {
        let root_identity = fs::identity_for_file(root)?;
        let mut configured = DIGEST_GATE.lock().unwrap();
        if configured
            .as_ref()
            .is_some_and(|(expected_root, expected_file, _)| {
                expected_root == &root_identity && expected_file == expected
            })
        {
            configured.take().map(|(_, _, gate)| gate)
        } else {
            None
        }
    };
    #[cfg(test)]
    if let Some(gate) = gate {
        gate.0.wait();
        gate.1.wait();
    }
    let mut file = fs::open_relative(root, path)?;
    if fs::identity_for_file(&file)? != *expected {
        return Err(Error::Conflict(format!(
            "publication input changed: {path}"
        )));
    }
    let mut digest = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        digest.update(&buffer[..n]);
    }
    if fs::identity_for_file(&file)? != *expected {
        return Err(Error::Conflict(format!(
            "publication input changed while hashing: {path}"
        )));
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn controlled_link_matches(expected: &Identity, observed: &Identity) -> bool {
    expected.same_object(observed)
        && expected.bytes == observed.bytes
        && expected.modified == observed.modified
}

type MutationGuard<'a> = std::sync::MutexGuard<'a, ()>;

struct PublicationLease<'a> {
    inventory: &'a Inventory,
    id: String,
}

impl Drop for PublicationLease<'_> {
    fn drop(&mut self) {
        self.inventory
            .publication_active
            .lock()
            .unwrap()
            .remove(&self.id);
    }
}

fn manifest_identity_matches(root: &File, captured: &[FileEntry]) -> Result<bool> {
    let observed = fs::manifest(root, 100_000)?;
    if observed.len() != captured.len() {
        return Ok(false);
    }
    Ok(observed.iter().zip(captured).all(|(actual, expected)| {
        actual.path == expected.path && actual.identity == expected.identity
    }))
}

fn manifest_matches_capture<'a>(
    inventory: &'a Inventory,
    guard: &mut Option<MutationGuard<'a>>,
    root: &File,
    observed: &[FileEntry],
    captured: &[FileEntry],
    linked: bool,
) -> Result<bool> {
    if observed.len() != captured.len() {
        return Ok(false);
    }
    for (actual, expected) in observed.iter().zip(captured) {
        if actual.path != expected.path || actual.identity.directory != expected.identity.directory
        {
            return Ok(false);
        }
        if expected.identity.directory {
            if !expected.identity.same_object(&actual.identity) {
                return Ok(false);
            }
        } else {
            let identity_matches = if linked {
                controlled_link_matches(&expected.identity, &actual.identity)
            } else {
                expected.identity == actual.identity
            };
            if !identity_matches {
                return Ok(false);
            }
            if let Some(expected_digest) = expected.digest.as_deref() {
                if inventory.digest_file_yielding(guard, root, &actual.path, &actual.identity)?
                    != expected_digest
                {
                    return Ok(false);
                }
            } else if linked {
                // Older hardlink journals cannot prove the ctime transition.
                return Ok(false);
            }
        }
    }
    Ok(true)
}

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
    #[serde(default)]
    owner_generation: Option<String>,
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
fn link_file<'a>(
    inventory: &'a Inventory,
    guard: &mut Option<MutationGuard<'a>>,
    operation_id: &str,
    source: &File,
    target: &File,
    entry: &FileEntry,
) -> Result<()> {
    use std::os::{fd::AsRawFd, unix::ffi::OsStrExt};
    let (src, name) = parent(source, &entry.path)?;
    let (dst, _) = parent(target, &entry.path)?;
    let input = fs::open_at(&src, &name, false)?;
    let input_identity = fs::identity_for_file(&input)?;
    if !controlled_link_matches(&entry.identity, &input_identity)
        || entry.digest.as_deref()
            != Some(
                inventory
                    .digest_file_yielding(guard, &src, &name.to_string_lossy(), &input_identity)?
                    .as_str(),
            )
    {
        return Err(Error::Conflict("publication source file changed".into()));
    }
    inventory.ensure_publication_active(operation_id)?;
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
    let observed = fs::identity_for_file(&output)?;
    if !controlled_link_matches(&entry.identity, &observed)
        || entry.digest.as_deref()
            != Some(
                inventory
                    .digest_file_yielding(guard, &dst, &name.to_string_lossy(), &observed)?
                    .as_str(),
            )
    {
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

fn verified_target<'a>(
    inventory: &'a Inventory,
    guard: &mut Option<MutationGuard<'a>>,
    p: &Publication,
) -> Result<PublishedDirectory> {
    let root = fs::open_dir(&p.target)?;
    let identity = fs::identity_for_file(&root)?;
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
            if !controlled_link_matches(&expected.identity, &actual.identity) {
                false
            } else if let Some(expected_digest) = expected.digest.as_deref() {
                inventory.digest_file_yielding(guard, &root, &actual.path, &actual.identity)?
                    == expected_digest
            } else {
                false
            }
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
#[cfg(test)]
thread_local! { static DIGEST_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
#[cfg(test)]
type DigestGate = Option<(
    Identity,
    Identity,
    std::sync::Arc<(std::sync::Barrier, std::sync::Barrier)>,
)>;
#[cfg(test)]
static DIGEST_GATE: std::sync::Mutex<DigestGate> = std::sync::Mutex::new(None);

impl Inventory {
    pub(super) fn selected_publication_file_digests(
        &self,
        key: &str,
    ) -> Result<Vec<crate::artifacts::DeliveryFile>> {
        let op = self.operation(&format!("publish-{key}"))?;
        if op.state != "succeeded" {
            return Err(Error::Conflict(
                "selected publication journal is not committed".into(),
            ));
        }
        let publication: Publication = serde_json::from_str(&op.request)?;
        if publication.published.is_none() {
            return Err(Error::Conflict(
                "selected publication has no durable published snapshot".into(),
            ));
        }
        let mut files = publication
            .files
            .into_iter()
            .filter(|entry| !entry.identity.directory)
            .map(|entry| {
                let sha256 = entry.digest.ok_or_else(|| {
                    Error::Conflict(format!(
                        "selected publication lacks a durable digest: {}",
                        entry.path
                    ))
                })?;
                Ok(crate::artifacts::DeliveryFile {
                    path: entry.path,
                    size_bytes: entry.identity.bytes,
                    sha256,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        files.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
        if files.is_empty() {
            return Err(Error::Conflict(
                "selected publication journal has no payload files".into(),
            ));
        }
        Ok(files)
    }

    fn digest_file_yielding<'a>(
        &'a self,
        guard: &mut Option<MutationGuard<'a>>,
        root: &File,
        path: &str,
        expected: &Identity,
    ) -> Result<String> {
        drop(guard.take());
        let digest = digest_file(root, path, expected);
        *guard = Some(self.mutation_guard()?);
        let digest = digest?;
        let current = fs::open_relative(root, path)?;
        if fs::identity_for_file(&current)? != *expected {
            return Err(Error::Conflict(format!(
                "publication input changed after hashing: {path}"
            )));
        }
        Ok(digest)
    }

    fn ensure_publication_active(&self, id: &str) -> Result<()> {
        let operation = self.operation(id)?;
        if operation.state == "cancelled" {
            return Err(Error::Conflict("publication was cancelled".into()));
        }
        Ok(())
    }

    /// Caller holds the inventory mutation guard. No marker inside the payload
    /// grants ownership; every directory identity is checkpointed in SQLite.
    pub(super) fn publish_directory_unlocked<'a>(
        &'a self,
        guard: &mut Option<MutationGuard<'a>>,
        source: &Path,
        target: &Path,
        artifact: &str,
        key: &str,
    ) -> Result<PublishedDirectory> {
        let key = format!("publish-{key}");
        if !self.publication_active.lock().unwrap().insert(key.clone()) {
            return Err(std::io::Error::from(std::io::ErrorKind::WouldBlock).into());
        }
        let _lease = PublicationLease {
            inventory: self,
            id: key.clone(),
        };
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
                let files = fs::manifest(&dir, 100_000)?;
                let owner_generation = match self.get(artifact) {
                    Ok(owner) => Some(owner.generation),
                    Err(Error::NotFound) => None,
                    Err(error) => return Err(error),
                };
                let p = Publication {
                    source: source.into(),
                    source_identity: fs::identity_for_file(&dir)?,
                    owner_generation,
                    target: target.into(),
                    target_parent: fs::identity_for_file(&root)?,
                    files,
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
        let baseline_request = op.request.clone();
        let mut hashes_added = false;
        if p.files
            .iter()
            .any(|entry| !entry.identity.directory && entry.digest.is_none())
        {
            let hash_root = fs::open_dir(source)?;
            for entry in p.files.iter_mut().filter(|entry| !entry.identity.directory) {
                if entry.digest.is_some() {
                    continue;
                }
                entry.digest = Some(self.digest_file_yielding(
                    guard,
                    &hash_root,
                    &entry.path,
                    &entry.identity,
                )?);
                hashes_added = true;
            }
        }
        if hashes_added {
            self.ensure_publication_active(&op.id)?;
            let current_source = fs::open_dir(source)?;
            let current_parent = fs::open_dir(target.parent().unwrap())?;
            if !p
                .source_identity
                .same_object(&fs::identity_for_file(&current_source)?)
                || !manifest_identity_matches(&current_source, &p.files)?
                || !p
                    .target_parent
                    .same_object(&fs::identity_for_file(&current_parent)?)
            {
                return Err(Error::Conflict(
                    "publication inputs changed while capturing manifest".into(),
                ));
            }
            if let Some(generation) = p.owner_generation.as_ref() {
                if self.get(artifact)?.generation != *generation {
                    return Err(Error::Conflict(
                        "publication owner generation changed during manifest capture".into(),
                    ));
                }
            }
            let current_operation = self.operation(&op.id)?;
            if current_operation.state == "cancelled"
                || current_operation.artifact != op.artifact
                || current_operation.kind != op.kind
                || current_operation.created_at != op.created_at
                || current_operation.request != baseline_request
            {
                return Err(Error::Conflict(
                    "publication journal advanced during manifest capture".into(),
                ));
            }
            op.request = serde_json::to_string(&p)?;
            save_operation(&self.db.lock().unwrap(), &op)?;
        }
        let target_parent = fs::open_dir(target.parent().unwrap())?;
        if !p
            .target_parent
            .same_object(&fs::identity_for_file(&target_parent)?)
        {
            return Err(Error::Conflict("publication parent changed".into()));
        }
        if p.published.is_some() {
            return verified_target(self, guard, &p);
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
                p.published = Some(verified_target(self, guard, &p)?);
            } else {
                let dir = fs::open_dir(source)?;
                if !p.source_identity.same_object(&fs::identity_for_file(&dir)?)
                    || !manifest_matches_capture(
                        self,
                        guard,
                        &dir,
                        &fs::manifest(&dir, 100_000)?,
                        &p.files,
                        false,
                    )?
                {
                    return Err(Error::Conflict("publication source changed".into()));
                }
                self.ensure_publication_active(&op.id)?;
                match fs::rename_exclusive(source, target) {
                    Ok(()) => p.published = Some(verified_target(self, guard, &p)?),
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
            if !p.source_identity.same_object(&fs::identity_for_file(&src)?)
                || !manifest_matches_capture(
                    self,
                    guard,
                    &src,
                    &fs::manifest(&src, 100_000)?,
                    &p.files,
                    true,
                )?
            {
                return Err(Error::Conflict("publication source changed".into()));
            }
            self.ensure_publication_active(&op.id)?;
            let root = if let Some(expected) = p.directories.get("") {
                let dir = fs::open_dir(target)?;
                if !expected.same_object(&fs::identity_for_file(&dir)?) {
                    return Err(Error::Conflict("publication root replaced".into()));
                }
                dir
            } else {
                // If creation survived but its checkpoint did not, EEXIST
                // requires review. Never infer ownership of that directory.
                self.ensure_publication_active(&op.id)?;
                let dir = mkdir_at(&target_parent, Path::new(target.file_name().unwrap()))?;
                p.directories
                    .insert(String::new(), fs::identity_for_file(&dir)?);
                persist(&mut op, &p)?;
                dir
            };
            for entry in p.files.clone().iter().filter(|e| e.identity.directory) {
                if let Some(expected) = p.directories.get(&entry.path) {
                    let dir = fs::open_relative(&root, &entry.path)?;
                    if !expected.same_object(&fs::identity_for_file(&dir)?) {
                        return Err(Error::Conflict("publication subdirectory replaced".into()));
                    }
                } else {
                    let (dir, name) = parent(&root, &entry.path)?;
                    let child = mkdir_at(&dir, &name)?;
                    p.directories
                        .insert(entry.path.clone(), fs::identity_for_file(&child)?);
                    persist(&mut op, &p)?;
                }
            }
            for entry in p.files.iter().filter(|e| !e.identity.directory) {
                self.ensure_publication_active(&op.id)?;
                link_file(self, guard, &op.id, &src, &root, entry)?;
            }
            for entry in p.files.iter().rev().filter(|e| e.identity.directory) {
                fs::sync_directory(&fs::open_relative(&root, &entry.path)?)?;
            }
            fs::sync_directory(&root)?;
            fs::sync_directory(&target_parent)?;
            p.published = Some(verified_target(self, guard, &p)?);
        }
        self.ensure_publication_active(&op.id)?;
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
        if !p.source_identity.same_object(&fs::identity_for_file(&dir)?) {
            return Err(Error::Conflict(
                "publication cleanup source replaced".into(),
            ));
        }
        let entries = fs::manifest(&dir, 100_000)?;
        for entry in &entries {
            let Some(expected) = p.files.iter().find(|expected| expected.path == entry.path) else {
                return Err(Error::Conflict(
                    "publication cleanup source contents changed".into(),
                ));
            };
            let identity_matches = if entry.identity.directory {
                expected.identity.same_object(&entry.identity)
            } else {
                if !controlled_link_matches(&expected.identity, &entry.identity) {
                    false
                } else if let Some(expected_digest) = expected.digest.as_deref() {
                    digest_file(&dir, &entry.path, &entry.identity)? == expected_digest
                } else {
                    false
                }
            };
            if !identity_matches {
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
            .same_object(&fs::identity_for_file(&current)?)
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
    fn arm_digest_gate(
        root: &Path,
        relative: &str,
        gate: std::sync::Arc<(std::sync::Barrier, std::sync::Barrier)>,
    ) {
        let root_file = fs::open_dir(root).unwrap();
        let file = fs::open_relative(&root_file, relative).unwrap();
        *DIGEST_GATE.lock().unwrap() = Some((
            fs::identity_for_file(&root_file).unwrap(),
            fs::identity_for_file(&file).unwrap(),
            gate,
        ));
    }
    fn publish(inventory: &Inventory, source: &Path, target: &Path) -> Result<PublishedDirectory> {
        let mut guard = Some(inventory.mutation_guard()?);
        inventory.publish_directory_unlocked(&mut guard, source, target, "test", "one")
    }
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
        let result = publish(&inventory, &source, &target).unwrap();
        assert!(result.source_retained);
        assert!(source.join("nested/media.mkv").exists());
        assert_eq!(
            fs::identity(&std::fs::metadata(source.join("nested/media.mkv")).unwrap()),
            fs::identity(&std::fs::metadata(target.join("nested/media.mkv")).unwrap())
        );
        drop(inventory);
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        publish(&inventory, &source, &target).unwrap();
        inventory.retire_publication_source_unlocked("one").unwrap();
        inventory.retire_publication_source_unlocked("one").unwrap();
        assert!(!source.exists());
        assert_eq!(
            std::fs::read(target.join("nested/media.mkv")).unwrap(),
            b"original"
        );
    }
    #[test]
    fn nested_hardlinks_verify_and_target_eio_remains_retryable() {
        let (_temp, inventory, source, target) = fixture();
        fs::RENAME_FAILURE.with(|f| f.set(Some(libc::EINVAL)));
        let result = publish(&inventory, &source, &target).unwrap();
        assert!(result.source_retained);
        assert_eq!(
            std::fs::read(target.join("nested/media.mkv")).unwrap(),
            b"original"
        );

        let operation = inventory.operation("publish-one").unwrap();
        let publication: Publication = serde_json::from_str(&operation.request).unwrap();
        DIGEST_FAILURE.with(|failure| failure.set(true));
        let mut guard = Some(inventory.mutation_guard().unwrap());
        let error = verified_target(&inventory, &mut guard, &publication).unwrap_err();
        assert!(matches!(error, Error::Io(ref io) if io.raw_os_error() == Some(libc::EIO)));
    }

    #[test]
    fn atomic_rename_replay_and_published_replay_do_not_require_source_path() {
        let (_temp, inventory, source, target) = fixture();
        let published = publish(&inventory, &source, &target).unwrap();
        assert!(!published.source_retained);
        assert!(!source.exists());

        // Simulate a crash after atomic rename and target verification but
        // before the publication operation acknowledgement was saved.
        let mut operation = inventory.operation("publish-one").unwrap();
        let mut publication: Publication = serde_json::from_str(&operation.request).unwrap();
        publication.published = None;
        operation.request = serde_json::to_string(&publication).unwrap();
        operation.state = "running".into();
        save_operation(&inventory.db.lock().unwrap(), &operation).unwrap();

        let replayed = publish(&inventory, &source, &target).unwrap();
        assert!(!replayed.source_retained);
        assert_eq!(
            std::fs::read(target.join("nested/media.mkv")).unwrap(),
            b"original"
        );
        assert!(publish(&inventory, &source, &target).is_ok());
    }
    #[test]
    fn concurrent_publish_and_cancel_during_hash_preserve_the_durable_journal() {
        use std::sync::{Arc, Barrier};

        let (temp, inventory, source, target) = fixture();
        let inventory = Arc::new(inventory);
        let gate = Arc::new((Barrier::new(2), Barrier::new(2)));
        arm_digest_gate(&source, "nested/media.mkv", gate.clone());
        let worker_inventory = inventory.clone();
        let worker_source = source.clone();
        let worker_target = target.clone();
        let worker =
            std::thread::spawn(move || publish(&worker_inventory, &worker_source, &worker_target));

        // The first publisher has persisted its digest-free request and
        // released the global mutation guard while hashing. A second caller
        // cannot advance or overwrite that operation.
        gate.0.wait();
        assert!(matches!(
            publish(&inventory, &source, &target),
            Err(Error::Io(ref error)) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
        gate.1.wait();
        worker.join().unwrap().unwrap();
        *DIGEST_GATE.lock().unwrap() = None;
        assert!(target.join("nested/media.mkv").exists());

        let second = temp.path().join("second-source");
        std::fs::create_dir(&second).unwrap();
        std::fs::write(second.join("file.mkv"), b"content").unwrap();
        let second_target = temp.path().join("second-target");
        let gate = Arc::new((Barrier::new(2), Barrier::new(2)));
        arm_digest_gate(&second, "file.mkv", gate.clone());
        let worker_inventory = inventory.clone();
        let worker_source = second.clone();
        let worker_target = second_target.clone();
        let worker = std::thread::spawn(move || {
            let mut guard = Some(worker_inventory.mutation_guard().unwrap());
            worker_inventory.publish_directory_unlocked(
                &mut guard,
                &worker_source,
                &worker_target,
                "test",
                "cancel-during-hash",
            )
        });
        gate.0.wait();
        {
            let _guard = inventory.mutation_guard().unwrap();
            let mut op = inventory.operation("publish-cancel-during-hash").unwrap();
            op.state = "cancelled".into();
            op.error = Some("cancelled by test".into());
            save_operation(&inventory.db.lock().unwrap(), &op).unwrap();
        }
        gate.1.wait();
        assert!(worker.join().unwrap().is_err());
        *DIGEST_GATE.lock().unwrap() = None;
        let op = inventory.operation("publish-cancel-during-hash").unwrap();
        assert_eq!(op.state, "cancelled");
        let publication: Publication = serde_json::from_str(&op.request).unwrap();
        assert!(publication.files.iter().all(|file| file.digest.is_none()));
        assert!(!second_target.exists());
    }
    #[test]
    fn interrupted_links_resume_but_uncheckpointed_directories_do_not() {
        let (_temp, inventory, source, target) = fixture();
        fs::RENAME_FAILURE.with(|f| f.set(Some(libc::EINVAL)));
        publish(&inventory, &source, &target).unwrap();
        let mut op = inventory.operation("publish-one").unwrap();
        let mut p: Publication = serde_json::from_str(&op.request).unwrap();
        p.published = None;
        op.state = "running".into();
        std::fs::remove_file(target.join("nested/media.mkv")).unwrap();
        op.request = serde_json::to_string(&p).unwrap();
        save_operation(&inventory.db.lock().unwrap(), &op).unwrap();
        publish(&inventory, &source, &target).unwrap();
        p.directories.clear();
        op.request = serde_json::to_string(&p).unwrap();
        save_operation(&inventory.db.lock().unwrap(), &op).unwrap();
        assert!(publish(&inventory, &source, &target).is_err());
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
        assert!(publish(&inventory, &source, &target).is_err());
        assert!(before.same_object(&fs::identity(&std::fs::metadata(&target).unwrap())));
        assert!(std::fs::read_dir(&target).unwrap().next().is_none());
    }
}
