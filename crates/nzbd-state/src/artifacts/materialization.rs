//! Accepted cluster generations enter Inventory through one durable namespace transition.
use super::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Read;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalGenerationFile {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalGeneration {
    pub result_id: String,
    pub fence: u64,
    pub source: PathBuf,
    pub files: Vec<ExternalGenerationFile>,
}
/// Canonical lifecycle seal identity shared with the cluster publisher. Each
/// version binds exactly its protocol fields; a newer seal never falls back
/// to the legacy tuple if its additional evidence fails validation.
#[allow(clippy::too_many_arguments)]
pub fn external_generation_identity<F: Serialize>(
    version: u8,
    job: u32,
    incarnation: &str,
    owner: &str,
    fence: u64,
    job_sha256: &str,
    files: &[F],
    artifact: &Option<ClusterArtifactCopy>,
    directory: &Option<(u64, u64)>,
    outcomes: &[DeliveryFileOutcome],
    disposition: &Option<String>,
) -> Result<String> {
    let identity = if version >= 3 {
        serde_json::to_vec(&(
            job,
            incarnation,
            owner,
            fence,
            job_sha256,
            files,
            artifact,
            directory,
            outcomes,
            disposition,
        ))?
    } else if version >= 2 {
        serde_json::to_vec(&(
            job,
            incarnation,
            owner,
            fence,
            job_sha256,
            files,
            artifact,
            directory,
            outcomes,
        ))?
    } else {
        serde_json::to_vec(&(job, incarnation, fence, job_sha256, files))?
    };
    Ok(format!("sha256:{:x}", Sha256::digest(identity)))
}
#[derive(Deserialize)]
struct Seal {
    #[serde(default)]
    identity_version: u8,
    #[serde(default)]
    owner_node_id: String,
    #[serde(default)]
    artifact_copy: Option<ClusterArtifactCopy>,
    #[serde(default)]
    directory_identity: Option<(u64, u64)>,
    #[serde(default)]
    file_outcomes: Vec<DeliveryFileOutcome>,
    #[serde(default)]
    source_disposition: Option<String>,
    job_id: u32,
    job_incarnation: String,
    fence: u64,
    result_id: String,
    job_sha256: String,
    files: Vec<ExternalGenerationFile>,
    total_bytes: u64,
}
#[derive(Serialize, Deserialize)]
struct Materialization {
    job: u32,
    root: PathBuf,
    target: PathBuf,
    accepted: ExternalGeneration,
    root_identity: Identity,
    source_identity: Identity,
    source_files: Vec<FileEntry>,
    previous: Option<Artifact>,
    previous_registered: bool,
    building: PathBuf,
    backup: PathBuf,
    building_identity: Option<Identity>,
    copied: bool,
    selected: Artifact,
}

struct NamespaceUse<'a> {
    inventory: &'a Inventory,
    key: Option<(String, String)>,
}
impl Drop for NamespaceUse<'_> {
    fn drop(&mut self) {
        if let Some(key) = &self.key {
            let _coordinator = self.inventory.mutation.lock().unwrap();
            self.inventory
                .reconciliation_uses
                .lock()
                .unwrap()
                .remove(key);
        }
    }
}

fn conflict(message: &str) -> Error {
    Error::Conflict(message.into())
}
fn bounded_read(file: File, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(conflict("sealed generation metadata exceeds its limit"));
    }
    Ok(bytes)
}
fn authenticated(job: u32, generation: &ExternalGeneration) -> Result<(ExternalGeneration, File)> {
    fs::absolute(&generation.source)?;
    if generation.source.file_name() != Some(std::ffi::OsStr::new("files")) {
        return Err(conflict(
            "accepted generation source is not its sealed files directory",
        ));
    }
    let parent = fs::open_dir(
        generation
            .source
            .parent()
            .ok_or_else(|| conflict("missing sealed generation parent"))?,
    )?;
    let seal: Seal = serde_json::from_slice(&bounded_read(
        fs::open_at(&parent, Path::new("manifest.json"), false)?,
        16 * 1024 * 1024,
    )?)?;
    let job_bytes = bounded_read(
        fs::open_at(&parent, Path::new("job.json"), false)?,
        256 * 1024 * 1024,
    )?;
    let identity = external_generation_identity(
        seal.identity_version,
        seal.job_id,
        &seal.job_incarnation,
        &seal.owner_node_id,
        seal.fence,
        &seal.job_sha256,
        &seal.files,
        &seal.artifact_copy,
        &seal.directory_identity,
        &seal.file_outcomes,
        &seal.source_disposition,
    )?;
    if seal.job_id != job
        || seal.fence != generation.fence
        || seal.result_id != generation.result_id
        || identity != generation.result_id
        || format!("{:x}", Sha256::digest(&job_bytes)) != seal.job_sha256
        || (!generation.files.is_empty() && generation.files != seal.files)
    {
        return Err(conflict(
            "accepted generation does not match its authenticated seal",
        ));
    }
    let mut paths = std::collections::HashSet::new();
    let mut bytes = 0u64;
    for file in &seal.files {
        let path = Path::new(&file.path);
        if file.path.is_empty()
            || path.is_absolute()
            || path
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
            || !paths.insert(file.path.clone())
            || file.sha256.len() != 64
            || !file.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(conflict(
                "accepted generation contains an invalid file proof",
            ));
        }
        bytes = bytes
            .checked_add(file.bytes)
            .ok_or_else(|| conflict("accepted generation byte count overflow"))?;
    }
    if bytes != seal.total_bytes {
        return Err(conflict(
            "accepted generation total does not match its seal",
        ));
    }
    let source = fs::open_at(&parent, Path::new("files"), true)?;
    let mut accepted = generation.clone();
    accepted.files = seal.files;
    Ok((accepted, source))
}
fn verify_files(
    dir: &File,
    accepted: &ExternalGeneration,
    legacy_marker: bool,
) -> Result<Vec<FileEntry>> {
    let entries = fs::manifest(dir, 100_000)?;
    let expected = accepted
        .files
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect::<BTreeMap<_, _>>();
    let mut seen = 0usize;
    for entry in entries.iter().filter(|entry| !entry.identity.directory) {
        if legacy_marker
            && entry.path == ".nzbd-generation"
            && !expected.contains_key(entry.path.as_str())
        {
            let marker = bounded_read(fs::open_relative(dir, &entry.path)?, 4096)?;
            if marker != accepted.result_id.as_bytes() {
                return Err(conflict(
                    "legacy generation marker differs from accepted result",
                ));
            }
            continue;
        }
        let proof = expected
            .get(entry.path.as_str())
            .ok_or_else(|| conflict("generation contains an unaccepted file"))?;
        let mut file = fs::open_relative(dir, &entry.path)?;
        if fs::identity_for_file(&file)? != entry.identity || entry.identity.bytes != proof.bytes {
            return Err(conflict("generation file identity or length changed"));
        }
        let mut digest = Sha256::new();
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            digest.update(&buffer[..count]);
        }
        if fs::identity_for_file(&file)? != entry.identity
            || format!("{:x}", digest.finalize()) != proof.sha256
        {
            return Err(conflict(
                "generation file changed or failed its accepted digest",
            ));
        }
        seen += 1;
    }
    if seen != expected.len() {
        return Err(conflict("generation is missing an accepted file"));
    }
    Ok(entries)
}
fn directory_at(root: &File, path: &Path) -> Result<Option<File>> {
    match fs::open_at(
        root,
        Path::new(
            path.file_name()
                .ok_or_else(|| conflict("missing publication name"))?,
        ),
        true,
    ) {
        Ok(file) => Ok(Some(file)),
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}
fn same_directory(file: &File, expected: &Identity) -> Result<()> {
    if !expected.same_object(&fs::identity_for_file(file)?) {
        return Err(conflict("materialization directory identity changed"));
    }
    Ok(())
}
#[cfg(unix)]
fn mkdir_at(parent: &File, name: &std::ffi::OsStr) -> Result<File> {
    use std::os::{fd::AsRawFd, unix::ffi::OsStrExt};
    let name_c =
        std::ffi::CString::new(name.as_bytes()).map_err(|_| conflict("invalid directory name"))?;
    if unsafe { libc::mkdirat(parent.as_raw_fd(), name_c.as_ptr(), 0o700) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    fs::sync_directory(parent)?;
    fs::open_at(parent, Path::new(name), true)
}
#[cfg(not(unix))]
fn mkdir_at(_: &File, _: &std::ffi::OsStr) -> Result<File> {
    Err(conflict(
        "checked materialization requires descriptor-relative filesystem support",
    ))
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn rename_at(root: &File, from: &Path, to: &Path) -> Result<()> {
    use std::os::{fd::AsRawFd, unix::ffi::OsStrExt};
    let from = std::ffi::CString::new(from.file_name().unwrap().as_bytes())
        .map_err(|_| conflict("invalid source name"))?;
    let to = std::ffi::CString::new(to.file_name().unwrap().as_bytes())
        .map_err(|_| conflict("invalid target name"))?;
    #[cfg(target_os = "linux")]
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            root.as_raw_fd(),
            from.as_ptr(),
            root.as_raw_fd(),
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    #[cfg(target_os = "macos")]
    let result = unsafe {
        libc::renameatx_np(
            root.as_raw_fd(),
            from.as_ptr(),
            root.as_raw_fd(),
            to.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    fs::sync_directory(root)
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn rename_at(_: &File, _: &Path, _: &Path) -> Result<()> {
    Err(conflict("atomic checked materialization is unavailable"))
}

impl Inventory {
    /// Verify the exact durable materialization receipt and its still-current
    /// owned namespace. New materializations have no payload marker file.
    pub fn materialized_generation_matches(
        &self,
        job: u32,
        root: &Path,
        target: &Path,
        generation: &ExternalGeneration,
    ) -> Result<bool> {
        let _coordinator = self.mutation_guard()?;
        let key = format!(
            "materialize-{:x}",
            Sha256::digest(serde_json::to_vec(&(
                job,
                root,
                target,
                &generation.result_id,
                generation.fence
            ))?)
        );
        let operation = match self.operation(&key) {
            Ok(operation) => operation,
            Err(Error::NotFound) => return Ok(false),
            Err(error) => return Err(error),
        };
        if operation.kind != "materialize_generation" || operation.state != "succeeded" {
            return Ok(false);
        }
        let record: Materialization = serde_json::from_str(&operation.request)?;
        if record.job != job
            || record.root != root
            || record.target != target
            || record.accepted != *generation
        {
            return Ok(false);
        }
        let Some(current) = self.for_job(job)? else {
            return Ok(false);
        };
        if !current.owned
            || current.id != record.selected.id
            || current.generation != record.selected.generation
            || current.path != record.target
            || current.root != record.root
            || current.root_identity != record.selected.root_identity
            || current.identity != record.selected.identity
            || current.files != record.selected.files
        {
            return Ok(false);
        }
        // The receipt proves the completed digest/copy/fsync transaction;
        // current descriptor identity proves that its selected namespace was
        // not replaced before READY. Do not invent a legacy marker file.
        let _selected = self.verify(&current)?;
        Ok(true)
    }

    /// Bind a detached predecessor to this exact succeeded materialization.
    /// Only its generated retention may be released; later policy changes
    /// remain protected by the existing deletion guards and journal.
    pub(super) fn cluster_materialized_predecessor(
        &self,
        copy: &ClusterArtifactCopy,
        result: &str,
    ) -> Result<bool> {
        let _guard = self.mutation_guard()?;
        let Some(selected) = self.for_job(copy.job_id)? else {
            return Ok(false);
        };
        let rows = {
            let db = self.db.lock().unwrap();
            let mut query=db.prepare("SELECT data FROM operations WHERE artifact=?1 AND state='succeeded' AND json_extract(data,'$.kind')='materialize_generation' LIMIT 2")?;
            let rows = query.query_map([&selected.id], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        if rows.len() != 1 {
            return Ok(false);
        }
        let operation: Operation = serde_json::from_str(&rows[0])?;
        let record: Materialization = serde_json::from_str(&operation.request)?;
        let Some(previous) = record.previous else {
            return Ok(false);
        };
        let mut current = self.get(&copy.publication.artifact)?;
        if copy.storage_instance_id != self.installation
            || copy.selected
            || copy.source != copy.publication
            || record.job != copy.job_id
            || record.accepted.result_id != result
            || record.selected.id != selected.id
            || record.selected.generation != selected.generation
            || previous.job != Some(copy.job_id)
            || previous.id != current.id
            || previous.generation != current.generation
            || current.generation != copy.publication.generation
            || !previous.owned
            || !current.owned
            || current.job.is_some()
            || current.root != previous.root
            || current.path != previous.path
            || current.root_identity != previous.root_identity
            || current.identity != previous.identity
            || current.files != previous.files
        {
            return Ok(false);
        }
        if current.state == "retained"
            && current.keep
            && current.hold.as_deref() == Some("review: superseded by accepted generation")
            && !previous.keep
            && previous.hold.is_none()
            && current.revision == previous.revision.saturating_add(1)
        {
            // This is the exact pin created by materialization, not a user
            // Keep or review action. Keep job detached from the new result.
            current.keep = false;
            current.hold = None;
            current.revision += 1;
            current.updated_at = now();
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            save_artifact(&tx, &current)?;
            event(
                &tx,
                &current.id,
                "materialized_predecessor_retirement",
                &operation.id,
            )?;
            tx.commit()?;
        }
        Ok(true)
    }

    pub fn materialize_generation(
        &self,
        job: u32,
        root: &Path,
        target: &Path,
        generation: &ExternalGeneration,
        policy: (bool, bool, bool),
    ) -> Result<Artifact> {
        self.materialize_generation_checked(job, root, target, generation, policy, &|| Ok(()))
    }

    pub fn materialize_generation_checked(
        &self,
        job: u32,
        root: &Path,
        target: &Path,
        generation: &ExternalGeneration,
        policy: (bool, bool, bool),
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Artifact> {
        checkpoint()?;
        fs::absolute(root)?;
        fs::absolute(target)?;
        if target.parent() != Some(root)
            || generation.source.starts_with(target)
            || target.starts_with(&generation.source)
        {
            return Err(conflict(
                "materialization target must be a separate immediate child of its root",
            ));
        }
        let _allocation = self.reserve_allocation(job)?;
        let mut namespace = NamespaceUse {
            inventory: self,
            key: None,
        };
        let root_fd = fs::open_dir(root)?;
        let root_identity = fs::identity_for_file(&root_fd)?;
        let key = format!(
            "materialize-{:x}",
            Sha256::digest(serde_json::to_vec(&(
                job,
                root,
                target,
                &generation.result_id,
                generation.fence
            ))?)
        );
        let (mut operation, mut record) = match self.operation(&key) {
            Ok(operation) => {
                let record: Materialization = serde_json::from_str(&operation.request)?;
                if operation.kind != "materialize_generation"
                    || record.job != job
                    || record.root != root
                    || record.target != target
                    || record.accepted.result_id != generation.result_id
                    || record.accepted.fence != generation.fence
                    || record.accepted.source != generation.source
                    || (!generation.files.is_empty() && record.accepted.files != generation.files)
                {
                    return Err(conflict(
                        "materialization request differs from its durable accepted proof",
                    ));
                }
                (operation, record)
            }
            Err(Error::NotFound) => {
                let (accepted, source) = authenticated(job, generation)?;
                let source_files = verify_files(&source, &accepted, false)?;
                let mut previous = self.for_job(job)?;
                let mut previous_registered = previous.is_some();
                if let Some(old) = &mut previous {
                    let old_fd = self.verify(old)?;
                    old.files = fs::manifest(&old_fd, 100_000)?;
                    if old.path != target && directory_at(&root_fd, target)?.is_some() {
                        return Err(conflict("target belongs to another namespace"));
                    }
                } else if let Some(existing) = directory_at(&root_fd, target)? {
                    // Cold accepted replay may find output from another leader.
                    // Prove all bytes, preserve this unowned tree, and create a
                    // fresh owned allocation rather than rebinding its inode.
                    let files = verify_files(&existing, &accepted, true)?;
                    if let Some(known) = self.for_path(target)? {
                        if known.job.is_some() || self.artifact_in_use(&known)? {
                            return Err(conflict("materialization target is already in use"));
                        }
                        same_directory(
                            &existing,
                            known
                                .identity
                                .as_ref()
                                .ok_or_else(|| conflict("target has no captured identity"))?,
                        )?;
                        previous = Some(known);
                        previous_registered = true;
                    } else {
                        let existing_identity = fs::identity_for_file(&existing)?;
                        let db = self.db.lock().unwrap();
                        previous = Some(Artifact {
                            id: id(&db)?,
                            generation: id(&db)?,
                            revision: 1,
                            job: None,
                            path: target.into(),
                            origin_path: target.into(),
                            root: root.into(),
                            root_identity: root_identity.clone(),
                            identity: Some(existing_identity),
                            state: "unknown".into(),
                            owned: false,
                            automatic_cleanup_eligible: false,
                            delivery_lifecycle: false,
                            keep: true,
                            keep_on_failure: true,
                            failure_assessment: None,
                            hold: Some("review: preexisting accepted output".into()),
                            created_at: now(),
                            updated_at: now(),
                            retention_seconds: 0,
                            deadline: None,
                            eligible_seconds: 0,
                            files,
                            error: None,
                            inspected_at: Some(now()),
                        });
                    }
                }
                let cohort = previous
                    .as_ref()
                    .filter(|old| old.owned && old.job == Some(job))
                    .map(|old| {
                        (
                            old.automatic_cleanup_eligible,
                            old.delivery_lifecycle,
                            old.keep_on_failure,
                        )
                    })
                    .unwrap_or(policy);
                let db = self.db.lock().unwrap();
                let selected = Artifact {
                    id: id(&db)?,
                    generation: id(&db)?,
                    revision: 1,
                    job: Some(job),
                    path: target.into(),
                    origin_path: target.into(),
                    root: root.into(),
                    root_identity: root_identity.clone(),
                    identity: None,
                    state: "active".into(),
                    owned: cfg!(unix),
                    automatic_cleanup_eligible: cohort.0,
                    delivery_lifecycle: cohort.1,
                    keep: !cfg!(unix),
                    keep_on_failure: cohort.2,
                    failure_assessment: None,
                    hold: None,
                    created_at: now(),
                    updated_at: now(),
                    retention_seconds: 0,
                    deadline: None,
                    eligible_seconds: 0,
                    files: Vec::new(),
                    error: None,
                    inspected_at: Some(now()),
                };
                drop(db);
                let record = Materialization {
                    job,
                    root: root.into(),
                    target: target.into(),
                    accepted,
                    root_identity: root_identity.clone(),
                    source_identity: fs::identity_for_file(&source)?,
                    source_files,
                    previous,
                    previous_registered,
                    building: root.join(format!(".{key}.building")),
                    backup: root.join(format!(".{key}.backup")),
                    building_identity: None,
                    copied: false,
                    selected,
                };
                let operation = Operation {
                    id: key.clone(),
                    artifact: record.selected.id.clone(),
                    kind: "materialize_generation".into(),
                    state: "running".into(),
                    request: serde_json::to_string(&record)?,
                    created_at: now(),
                    not_before: 0,
                    attempts: 1,
                    next_retry: 0,
                    error: None,
                };
                checkpoint()?;
                let _coordinator = self.mutation_guard()?;
                if self.operation(&key).is_ok() {
                    return Err(conflict("materialization was concurrently admitted"));
                }
                let mut db = self.db.lock().unwrap();
                let tx = db.transaction()?;
                if !record.previous_registered {
                    if let Some(old) = &record.previous {
                        save_artifact(&tx, old)?;
                    }
                }
                save_operation(&tx, &operation)?;
                tx.commit()?;
                (operation, record)
            }
            Err(error) => return Err(error),
        };
        same_directory(&root_fd, &record.root_identity)?;
        if operation.state == "succeeded" {
            let current = self
                .for_job(job)?
                .ok_or_else(|| conflict("selected generation binding is missing"))?;
            if current.id != record.selected.id
                || current.generation != record.selected.generation
                || current.path != target
            {
                return Err(conflict("selected generation was superseded"));
            }
            let selected = self.verify(&current)?;
            verify_files(&selected, &record.accepted, false)?;
            checkpoint()?;
            return Ok(current);
        }
        if operation.state != "running" {
            return Err(conflict("materialization requires review"));
        }
        {
            let _coordinator = self.mutation_guard()?;
            self.check_materialization_binding(&record)?;
            if let Some(old) = &record.previous {
                let current = self.get(&old.id)?;
                if current.generation != old.generation
                    || current.path != old.path
                    || current.root != old.root
                    || self.reconciliation_in_use(&current)
                {
                    return Err(conflict("prior generation custody changed"));
                }
                self.reconciliation_uses
                    .lock()
                    .unwrap()
                    .insert((old.id.clone(), old.generation.clone()));
                namespace.key = Some((old.id.clone(), old.generation.clone()));
            }
        }
        if !record.copied {
            let (accepted, source) = authenticated(job, &record.accepted)?;
            if accepted != record.accepted {
                return Err(conflict("sealed accepted proof changed"));
            }
            same_directory(&source, &record.source_identity)?;
            if fs::manifest(&source, 100_000)? != record.source_files {
                return Err(conflict("sealed source identity changed"));
            }
            let building = match directory_at(&root_fd, &record.building)? {
                Some(building) => {
                    same_directory(
                        &building,
                        record.building_identity.as_ref().ok_or_else(|| {
                            conflict("uncheckpointed building directory requires review")
                        })?,
                    )?;
                    building
                }
                None => {
                    if record.building_identity.is_some() {
                        return Err(conflict("journaled building directory disappeared"));
                    }
                    checkpoint()?;
                    let building = mkdir_at(&root_fd, record.building.file_name().unwrap())?;
                    record.building_identity = Some(fs::identity_for_file(&building)?);
                    self.materialization_checkpoint(&mut operation, &record)?;
                    building
                }
            };
            for proof in &record.accepted.files {
                checkpoint()?;
                let expected = record
                    .source_files
                    .iter()
                    .find(|file| file.path == proof.path && !file.identity.directory)
                    .ok_or_else(|| conflict("accepted source file disappeared"))?;
                let mut input = fs::open_relative(&source, &proof.path)?;
                if fs::identity_for_file(&input)? != expected.identity {
                    return Err(conflict("sealed source file identity changed"));
                }
                let relative = Path::new(&proof.path);
                let mut parent = building.try_clone()?;
                if let Some(ancestors) = relative.parent() {
                    for component in ancestors.components() {
                        let name = component.as_os_str();
                        parent = match fs::open_at(&parent, Path::new(name), true) {
                            Ok(dir) => dir,
                            Err(Error::Io(error))
                                if error.kind() == std::io::ErrorKind::NotFound =>
                            {
                                mkdir_at(&parent, name)?
                            }
                            Err(error) => return Err(error),
                        };
                    }
                }
                // Complete checkpointed copies are idempotent. An unfinished
                // uncheckpointed file is preserved instead of overwritten.
                match fs::open_at(&parent, Path::new(relative.file_name().unwrap()), false) {
                    Ok(_) => {
                        verify_files(&building, &record.accepted, false)?;
                        break;
                    }
                    Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
                let mut output = fs::create_file_exclusive_at(
                    &parent,
                    Path::new(relative.file_name().unwrap()),
                )?;
                let mut hash = Sha256::new();
                let mut bytes = 0u64;
                let mut buffer = [0u8; 64 * 1024];
                loop {
                    checkpoint()?;
                    let count = input.read(&mut buffer)?;
                    if count == 0 {
                        break;
                    }
                    output.write_all(&buffer[..count])?;
                    hash.update(&buffer[..count]);
                    bytes += count as u64;
                }
                output.sync_all()?;
                fs::sync_directory(&parent)?;
                if bytes != proof.bytes
                    || format!("{:x}", hash.finalize()) != proof.sha256
                    || fs::identity_for_file(&input)? != expected.identity
                {
                    return Err(conflict("copied generation failed accepted content proof"));
                }
            }
            record.selected.files = verify_files(&building, &record.accepted, false)?;
            fs::sync_directory(&building)?;
            fs::sync_directory(&root_fd)?;
            record.copied = true;
            self.materialization_checkpoint(&mut operation, &record)?;
            cut("copied")?;
        }
        let building_identity = record
            .building_identity
            .as_ref()
            .ok_or_else(|| conflict("copied generation has no building identity"))?;
        if let Some(old) = &record.previous {
            if old.path == record.target {
                match directory_at(&root_fd, &record.backup)? {
                    Some(backup) => {
                        same_directory(
                            &backup,
                            old.identity
                                .as_ref()
                                .ok_or_else(|| conflict("old allocation has no identity"))?,
                        )?;
                        if fs::manifest(&backup, 100_000)? != old.files {
                            return Err(conflict("preserved old target changed after backup"));
                        }
                    }
                    None => {
                        let previous = directory_at(&root_fd, target)?
                            .ok_or_else(|| conflict("old target disappeared before backup"))?;
                        same_directory(
                            &previous,
                            old.identity
                                .as_ref()
                                .ok_or_else(|| conflict("old allocation has no identity"))?,
                        )?;
                        if fs::manifest(&previous, 100_000)? != old.files {
                            return Err(conflict("old target changed before backup"));
                        }
                        checkpoint()?;
                        rename_at(&root_fd, target, &record.backup)?;
                    }
                }
                cut("backup")?;
            }
        }
        match directory_at(&root_fd, target)? {
            Some(selected) => same_directory(&selected, building_identity)?,
            None => {
                let building = directory_at(&root_fd, &record.building)?
                    .ok_or_else(|| conflict("copied building directory is missing"))?;
                same_directory(&building, building_identity)?;
                verify_files(&building, &record.accepted, false)?;
                checkpoint()?;
                rename_at(&root_fd, &record.building, target)?;
            }
        }
        cut("selected")?;
        let selected = directory_at(&root_fd, target)?
            .ok_or_else(|| conflict("selected target is missing"))?;
        same_directory(&selected, building_identity)?;
        record.selected.identity = Some(fs::identity_for_file(&selected)?);
        record.selected.files = verify_files(&selected, &record.accepted, false)?;
        same_directory(&fs::open_dir(root)?, &record.root_identity)?;
        let mut retained = record.previous.clone();
        if let Some(old) = &mut retained {
            let current = self.get(&old.id)?;
            if current.generation != old.generation
                || current.path != old.path
                || current.root != old.root
            {
                return Err(conflict("prior metadata binding changed before commit"));
            }
            old.keep_on_failure = current.keep_on_failure;
            if old.owned && old.job == Some(job) {
                record.selected.keep_on_failure = current.keep_on_failure;
            }
            old.job = None;
            old.keep = true;
            old.state = "retained".into();
            old.revision = current.revision + 1;
            old.updated_at = now();
            if old.path == record.target {
                old.path = record.backup.clone();
            }
            old.hold = current
                .hold
                .or_else(|| Some("review: superseded by accepted generation".into()));
            self.sidecar(old)?;
        }
        self.sidecar(&record.selected)?;
        checkpoint()?;
        let _coordinator = self.mutation_guard()?;
        checkpoint()?;
        self.check_materialization_binding(&record)?;
        // Sidecar writes carry only namespace identity. Merge mutable policy
        // and revisions at the actual SQLite commit boundary.
        if let (Some(original), Some(old)) = (&record.previous, &mut retained) {
            let current = self.get(&original.id)?;
            if current.generation != original.generation
                || current.path != original.path
                || current.root != original.root
            {
                return Err(conflict(
                    "prior generation changed while writing materialization receipts",
                ));
            }
            if original.owned && original.job == Some(job) {
                record.selected.keep_on_failure = current.keep_on_failure;
            }
            let backup_path = old.path.clone();
            let files = old.files.clone();
            *old = current;
            old.path = backup_path;
            old.files = files;
            old.job = None;
            old.keep = true;
            old.state = "retained".into();
            old.revision += 1;
            old.updated_at = now();
            old.hold
                .get_or_insert_with(|| "review: superseded by accepted generation".into());
        }
        let saved = self.operation(&operation.id)?;
        if saved.request != operation.request || saved.state != operation.state {
            return Err(conflict("materialization journal advanced before commit"));
        }
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        if let Some(old) = &retained {
            save_artifact(&tx, old)?;
        }
        save_artifact(&tx, &record.selected)?;
        operation.state = "succeeded".into();
        operation.request = serde_json::to_string(&record)?;
        save_operation(&tx, &operation)?;
        event(
            &tx,
            &record.selected.id,
            "generation_materialized",
            &record.accepted.result_id,
        )?;
        tx.commit()?;
        Ok(record.selected)
    }

    fn check_materialization_binding(&self, record: &Materialization) -> Result<()> {
        let expected = record
            .previous
            .as_ref()
            .filter(|old| old.job == Some(record.job));
        match (expected, self.for_job(record.job)?) {
            (None, None) => Ok(()),
            (Some(old), Some(current))
                if current.id == old.id
                    && current.generation == old.generation
                    && current.path == old.path
                    && current.root == old.root =>
            {
                Ok(())
            }
            _ => Err(conflict(
                "job allocation changed while materializing accepted generation",
            )),
        }
    }

    fn materialization_checkpoint(
        &self,
        operation: &mut Operation,
        record: &Materialization,
    ) -> Result<()> {
        let _coordinator = self.mutation_guard()?;
        let current = self.operation(&operation.id)?;
        if current.state != operation.state || current.request != operation.request {
            return Err(conflict("materialization journal changed"));
        }
        operation.request = serde_json::to_string(record)?;
        save_operation(&self.db.lock().unwrap(), operation)
    }
}
#[cfg(test)]
thread_local! { static CUT: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) }; }
fn cut(_name: &str) -> Result<()> {
    #[cfg(test)]
    if CUT.with(|cut| {
        if cut.get() == Some(_name) {
            cut.set(None);
            true
        } else {
            false
        }
    }) {
        return Err(std::io::Error::from_raw_os_error(libc::EIO).into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (tempfile::TempDir, Inventory, PathBuf, ExternalGeneration) {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("target");
        std::fs::create_dir(&root).unwrap();
        let sealed = temp.path().join("sealed");
        std::fs::create_dir_all(sealed.join("files")).unwrap();
        std::fs::write(sealed.join("files/media"), b"accepted").unwrap();
        let job = b"{\"id\":7}";
        std::fs::write(sealed.join("job.json"), job).unwrap();
        let files = vec![ExternalGenerationFile {
            path: "media".into(),
            bytes: 8,
            sha256: format!("{:x}", Sha256::digest(b"accepted")),
        }];
        let job_sha256 = format!("{:x}", Sha256::digest(job));
        let result_id = format!(
            "sha256:{:x}",
            Sha256::digest(
                serde_json::to_vec(&(7u32, "incarnation", 9u64, &job_sha256, &files)).unwrap()
            )
        );
        std::fs::write(sealed.join("manifest.json"), serde_json::to_vec(&serde_json::json!({"job_id":7,"job_incarnation":"incarnation","fence":9,"result_id":result_id,"job_sha256":job_sha256,"files":files,"total_bytes":8})).unwrap()).unwrap();
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        let generation = ExternalGeneration {
            result_id,
            fence: 9,
            source: sealed.join("files"),
            files,
        };
        (temp, inventory, root, generation)
    }

    #[test]
    fn lifecycle_v3_materialization_requires_full_seal_identity() {
        let (_temp, inventory, root, mut accepted) = fixture();
        let manifest = accepted.source.parent().unwrap().join("manifest.json");
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
        value["identity_version"] = 3.into();
        value["owner_node_id"] = "worker-owner".into();
        value["directory_identity"] = serde_json::json!([77, 88]);
        value["file_outcomes"] = serde_json::json!([{"sourcePath":"media","publicationPath":"media","classification":"video","outcome":"published","repairDependency":false,"sourceCleanupStatus":"pending"}]);
        value["source_disposition"] = "recoverable".into();
        let seal: Seal = serde_json::from_value(value.clone()).unwrap();
        accepted.result_id = external_generation_identity(
            seal.identity_version,
            seal.job_id,
            &seal.job_incarnation,
            &seal.owner_node_id,
            seal.fence,
            &seal.job_sha256,
            &seal.files,
            &seal.artifact_copy,
            &seal.directory_identity,
            &seal.file_outcomes,
            &seal.source_disposition,
        )
        .unwrap();
        value["result_id"] = accepted.result_id.clone().into();
        std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
        value["source_disposition"] = "unusable".into();
        std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(
            authenticated(7, &accepted).is_err(),
            "v3 evidence change fell back to legacy identity"
        );
        value["source_disposition"] = "recoverable".into();
        std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
        let target = root.join("v3-job");
        inventory
            .materialize_generation(7, &root, &target, &accepted, (false, false, false))
            .unwrap();
        assert_eq!(std::fs::read(target.join("media")).unwrap(), b"accepted");
    }

    #[test]
    fn accepted_materialization_preserves_old_generation_and_replays_namespace_cuts() {
        for boundary in ["copied", "backup", "selected"] {
            let (_temp, inventory, root, accepted) = fixture();
            let target = root.join("job");
            let old = inventory
                .allocate_for_delivery(7, &root, &target, true, true)
                .unwrap();
            std::fs::write(target.join("old"), b"retain old").unwrap();
            CUT.with(|cut| cut.set(Some(boundary)));
            assert!(inventory
                .materialize_generation(7, &root, &target, &accepted, (false, false, false))
                .is_err());
            let selected = inventory
                .materialize_generation(7, &root, &target, &accepted, (false, false, false))
                .unwrap();
            assert_ne!(old.generation, selected.generation);
            assert!(selected.automatic_cleanup_eligible && selected.delivery_lifecycle);
            assert_eq!(std::fs::read(target.join("media")).unwrap(), b"accepted");
            let retained = inventory.get(&old.id).unwrap();
            assert!(retained.keep && retained.job.is_none());
            assert_eq!(
                std::fs::read(retained.path.join("old")).unwrap(),
                b"retain old"
            );
            assert_eq!(
                inventory
                    .materialize_generation(7, &root, &target, &accepted, (false, false, false))
                    .unwrap()
                    .generation,
                selected.generation
            );
        }
    }

    #[test]
    fn accepted_receipt_cannot_rebind_an_unrelated_target_inode() {
        let (_temp, inventory, root, accepted) = fixture();
        let target = root.join("job");
        let selected = inventory
            .materialize_generation(7, &root, &target, &accepted, (true, false, false))
            .unwrap();
        std::fs::rename(&target, root.join("selected-preserved")).unwrap();
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("media"), b"accepted").unwrap();
        assert!(inventory
            .materialize_generation(7, &root, &target, &accepted, (true, false, false))
            .is_err());
        assert_eq!(
            inventory.for_job(7).unwrap().unwrap().generation,
            selected.generation
        );
    }
}
