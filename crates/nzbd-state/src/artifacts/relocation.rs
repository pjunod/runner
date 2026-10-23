use super::*;
use sha2::{Digest, Sha256};
use std::io::{Read, Seek};

#[derive(Serialize, Deserialize)]
pub(super) struct Relocation {
    source: Artifact,
    destination: PathBuf,
    #[serde(default)]
    requested_destination: Option<PathBuf>,
    destination_root: Identity,
    scratch: Option<Artifact>,
    #[serde(default)]
    registry: bool, // legacy journals only
    #[serde(default)]
    publication: Option<super::publication::PublishedDirectory>,
    #[serde(default)]
    publication_key: Option<String>,
    #[serde(default)]
    cleanup_done: bool,
    #[serde(default)]
    defer_source_retirement: bool,
    /// When present, publish this verified subset from a private scratch tree
    /// while retaining the complete source snapshot for Runner cleanup.
    #[serde(default)]
    publication_files: Option<Vec<FileEntry>>,
    #[serde(default)]
    file_outcomes: Vec<DeliveryFileOutcome>,
    #[serde(default)]
    source_cleanup_removed: Vec<String>,
    #[serde(default)]
    source_cleanup_current: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelocationResult {
    pub operation_id: String,
    pub artifact_id: String,
    pub generation: String,
    pub published_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedPublication {
    pub path: PathBuf,
    pub generation: String,
    pub files: Vec<super::DeliveryFile>,
}

#[cfg(test)]
thread_local! { pub(super) static FAULTS: std::cell::RefCell<std::collections::HashMap<&'static str, i32>> = Default::default(); }
#[cfg(test)]
thread_local! { pub(super) static FAIL_SOURCE_UNLINK_REFRESH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
#[cfg(test)]
type CopyGate = Option<(
    Identity,
    std::sync::Arc<(std::sync::Barrier, std::sync::Barrier)>,
)>;
#[cfg(test)]
pub(super) static COPY_GATE: std::sync::Mutex<CopyGate> = std::sync::Mutex::new(None);
fn boundary(stage: &'static str, scope: Option<&Identity>) -> Result<()> {
    #[cfg(test)]
    if let Some(code) = FAULTS.with(|f| f.borrow_mut().remove(stage)) {
        return Err(std::io::Error::from_raw_os_error(code).into());
    }
    #[cfg(test)]
    if stage == "copy_chunk" {
        let gate = {
            let mut configured = COPY_GATE.lock().unwrap();
            if configured
                .as_ref()
                .is_some_and(|(expected, _)| scope.is_some_and(|scope| scope == expected))
            {
                configured.take().map(|(_, gate)| gate)
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.0.wait();
            gate.1.wait();
        }
    }
    let _ = stage;
    let _ = scope;
    Ok(())
}
impl Inventory {
    /// Return the durable target of a successful selected-file publication
    /// for this job, if one has already committed. Used when finalization
    /// restarts after the publication journal completed but before History.
    pub fn selected_publication_for_job(&self, job: u32) -> Result<Option<PathBuf>> {
        let Some(artifact) = self.for_job(job)? else {
            return Ok(None);
        };
        let db = self.db.lock().unwrap();
        let mut statement = db.prepare(
            "SELECT data FROM operations WHERE artifact=?1 AND state='succeeded' AND json_extract(data,'$.kind')='relocate' ORDER BY json_extract(data,'$.created_at') DESC",
        )?;
        let rows = statement.query_map([&artifact.id], |row| row.get::<_, String>(0))?;
        for raw in rows {
            let raw = raw?;
            let operation: Operation = serde_json::from_str(&raw)?;
            let movement: Relocation = serde_json::from_str(&operation.request)?;
            if movement.publication_files.is_some() && movement.destination == artifact.path {
                return Ok(Some(artifact.path));
            }
        }
        Ok(None)
    }

    /// Return the immutable selected-file snapshot only when it belongs to
    /// this exact private processing source generation. Cluster Post workers
    /// use this to seal payloads after the source directory has been moved.
    pub fn selected_publication_for_source(
        &self,
        job: u32,
        source_path: &Path,
    ) -> Result<Option<SelectedPublication>> {
        let Some(artifact) = self.for_job(job)? else {
            return Ok(None);
        };
        let raw_operations = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare(
                "SELECT data FROM operations WHERE artifact=?1 AND state='succeeded' AND json_extract(data,'$.kind')='relocate' ORDER BY json_extract(data,'$.created_at') DESC",
            )?;
            let rows = statement.query_map([&artifact.id], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in raw_operations {
            let operation: Operation = serde_json::from_str(&raw)?;
            let movement: Relocation = serde_json::from_str(&operation.request)?;
            if movement.publication_files.is_none()
                || movement.destination != artifact.path
                || movement.source.path != source_path
                || movement.source.generation != artifact.generation
            {
                continue;
            }
            let publication_key = movement.publication_key.as_deref().ok_or_else(|| {
                Error::Conflict("selected publication has no operation key".into())
            })?;
            let files = self.selected_publication_file_digests(publication_key)?;
            return Ok(Some(SelectedPublication {
                path: artifact.path,
                generation: artifact.generation,
                files,
            }));
        }
        Ok(None)
    }

    pub(super) fn deferred_retirement_ready(&self, relocation_id: &str) -> Result<Option<bool>> {
        let Ok(op) = self.operation(relocation_id) else {
            return Ok(None);
        };
        if op.kind != "relocate" {
            return Ok(None);
        }
        let movement: Relocation = serde_json::from_str(&op.request)?;
        if !movement.defer_source_retirement {
            return Ok(None);
        }
        let ready = self.db.lock().unwrap().query_row(
            "SELECT EXISTS(SELECT 1 FROM deliveries WHERE artifact=?1 AND generation=?2)",
            params![movement.source.id, movement.source.generation],
            |row| row.get(0),
        )?;
        Ok(Some(ready))
    }

    /// Called inside the transaction transferring authority to deletion.
    /// Scratch has its own inventory identity and remains available for review.
    pub(super) fn cancel_relocations(
        db: &Connection,
        artifact: &str,
        generation: &str,
        reason: &str,
    ) -> Result<()> {
        let raws = {
            let mut stmt = db.prepare("SELECT data FROM operations WHERE artifact=?1 AND state NOT IN ('succeeded','cancelled') AND json_extract(data,'$.kind')='relocate' AND json_extract(json_extract(data,'$.request'),'$.source.generation')=?2")?;
            let rows = stmt.query_map([artifact, generation], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in raws {
            let mut op: Operation = serde_json::from_str(&raw)?;
            op.state = "cancelled".into();
            op.error = Some(reason.into());
            save_operation(db, &op)?;
            event(db, artifact, "relocation_cancelled", &op.id)?;
        }
        Ok(())
    }

    /// A successful delete is terminal for its generation. Older reconcilers
    /// could overwrite its tombstone with a stale move's review state. Restore
    /// the journal's result before interpreting any unfinished operations;
    /// never inspect or delete the bytes now occupying the old pathname.
    pub(super) fn reconcile_deleted_artifacts(&self) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let raws = {
            // For legacy journals require a still-uncommitted move that names
            // this generation and predates even the deletion REQUEST. This is
            // stricter than comparing with success time (old operations do not
            // store that timestamp). Equal-second ordering is left for review.
            let mut stmt = tx.prepare("SELECT DISTINCT a.data FROM artifacts a
                JOIN operations d ON d.artifact=a.id
                WHERE d.state='succeeded' AND json_extract(d.data,'$.kind')='delete'
                AND (
                    json_extract(json_extract(d.data,'$.request'),'$[4]')=json_extract(a.data,'$.generation')
                    OR (json_array_length(json_extract(d.data,'$.request'))=4 AND EXISTS (
                        SELECT 1 FROM operations proof WHERE proof.artifact=a.id
                        AND proof.state IN ('running','review')
                        AND json_extract(proof.data,'$.kind')='relocate'
                        AND json_extract(proof.data,'$.created_at') < json_extract(d.data,'$.created_at')
                        AND json_extract(json_extract(proof.data,'$.request'),'$.source.generation')=json_extract(a.data,'$.generation')
                    ))
                )
                AND (a.state!='deleted' OR EXISTS (
                    SELECT 1 FROM operations m WHERE m.artifact=a.id
                    AND m.state NOT IN ('succeeded','cancelled')
                    AND json_extract(m.data,'$.kind')='relocate'
                ))")?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in raws {
            let mut a: Artifact = serde_json::from_str(&raw)?;
            Self::cancel_relocations(
                &tx,
                &a.id,
                &a.generation,
                "source deletion already committed",
            )?;
            if a.state != "deleted" {
                a.state = "deleted".into();
                a.hold = None;
                a.error = None;
                a.updated_at = now();
                a.revision += 1;
                save_artifact(&tx, &a)?;
                event(
                    &tx,
                    &a.id,
                    "deletion_reconciled",
                    "restored committed deletion",
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Revisions may advance during PP/review, but an operation only owns its
    /// original generation and location while that generation remains live.
    /// Both commit and error handling must obey the same authority check.
    fn relocation_source(&self, op: &Operation, movement: &Relocation) -> Result<Option<Artifact>> {
        let persisted = self.operation(&op.id)?;
        let current = self.get(&op.artifact)?;
        let source = &movement.source;
        let same_identity = match (&current.identity, &source.identity) {
            (Some(a), Some(b)) => a.same_object(b),
            _ => false,
        };
        Ok(
            (matches!(persisted.state.as_str(), "running" | "review" | "retry")
                && matches!(
                    current.state.as_str(),
                    "active" | "transitioning" | "retained"
                )
                && current.id == source.id
                && current.generation == source.generation
                && current.path == source.path
                && current.root == source.root
                && current.owned == source.owned
                && same_identity)
                .then_some(current),
        )
    }

    fn cancel_obsolete_relocation(&self, op: &mut Operation) -> Result<()> {
        // A concurrent deletion may have already cancelled this operation.
        // Keep its recorded reason instead of overwriting that transition.
        let saved = self.operation(&op.id)?;
        if matches!(saved.state.as_str(), "succeeded" | "cancelled") {
            *op = saved;
            return Ok(());
        }
        op.state = "cancelled".into();
        op.error = Some("relocation no longer owns the source generation".into());
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_operation(&tx, op)?;
        event(&tx, &op.artifact, "relocation_cancelled", &op.id)?;
        tx.commit()?;
        Ok(())
    }

    fn save_relocation_progress(&self, op: &Operation, movement: &Relocation) -> Result<()> {
        let _guard = self.mutation_guard()?;
        self.relocation_source(op, movement)?
            .ok_or_else(|| Error::Conflict("relocation authority revoked".into()))?;
        save_operation(&self.db.lock().unwrap(), op)
    }

    /// Journal before moving. A cross-volume move publishes a verified copy,
    /// then retires only the exact source entries captured by this operation.
    pub fn relocate(&self, job: u32, destination: &Path) -> Result<RelocationResult> {
        self.relocate_checked(job, destination, &|| Ok(()))
    }
    pub fn relocate_checked(
        &self,
        job: u32,
        destination: &Path,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<RelocationResult> {
        self.relocate_checked_mode(job, destination, checkpoint, false)
    }

    /// Publish a completed generation while retaining Runner's source until a
    /// durable Delivery manifest has been recorded by the caller.
    pub fn relocate_for_delivery_checked(
        &self,
        job: u32,
        destination: &Path,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<RelocationResult> {
        self.relocate_checked_mode(job, destination, checkpoint, true)
    }

    /// Publish only a verified selected manifest. The source tree is retained
    /// until the durable Delivery record authorizes its independent cleanup.
    pub fn publish_selected_for_delivery_checked(
        &self,
        job: u32,
        destination: &Path,
        selected_files: Vec<FileEntry>,
        file_outcomes: Vec<DeliveryFileOutcome>,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<RelocationResult> {
        self.relocate_selected_checked_mode(
            job,
            destination,
            checkpoint,
            selected_files,
            file_outcomes,
            true,
        )
    }

    /// Publish selected verified payload for ordinary new Runner work. The
    /// relocation journal owns source retirement, so no Curator receipt is
    /// required to finish this publication safely.
    pub fn publish_selected_checked(
        &self,
        job: u32,
        destination: &Path,
        selected_files: Vec<FileEntry>,
        file_outcomes: Vec<DeliveryFileOutcome>,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<RelocationResult> {
        self.relocate_selected_checked_mode(
            job,
            destination,
            checkpoint,
            selected_files,
            file_outcomes,
            false,
        )
    }

    fn relocate_checked_mode(
        &self,
        job: u32,
        destination: &Path,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
        defer_source_retirement: bool,
    ) -> Result<RelocationResult> {
        self.relocate_checked_mode_inner(
            job,
            destination,
            checkpoint,
            defer_source_retirement,
            None,
            Vec::new(),
        )
    }

    fn relocate_selected_checked_mode(
        &self,
        job: u32,
        destination: &Path,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
        selected_files: Vec<FileEntry>,
        file_outcomes: Vec<DeliveryFileOutcome>,
        defer_source_retirement: bool,
    ) -> Result<RelocationResult> {
        if selected_files.is_empty() || selected_files.iter().any(|entry| entry.identity.directory)
        {
            return Err(Error::Conflict(
                "selected publication has no payload files".into(),
            ));
        }
        self.relocate_checked_mode_inner(
            job,
            destination,
            checkpoint,
            defer_source_retirement,
            Some(selected_files),
            file_outcomes,
        )
    }

    /// Populate the operation-owned private tree. Existing files are accepted
    /// only after matching the immutable source generation byte-for-byte, so an
    /// interrupted copy can resume without treating a partial tree as complete.
    fn populate_relocation_scratch(
        &self,
        source: &Artifact,
        scratch: &mut Artifact,
        copy_files: &[FileEntry],
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<()> {
        let source_root = self.verify(source)?;
        self.verify_root(scratch)?;
        let scratch_root = fs::open_dir(&scratch.path)?;
        let observed_root = fs::identity_for_file(&scratch_root)?;
        if !scratch
            .identity
            .as_ref()
            .is_some_and(|identity| identity.same_object(&observed_root))
        {
            return Err(Error::Conflict("relocation scratch root changed".into()));
        }
        let mut directories = std::collections::BTreeSet::new();
        for entry in copy_files {
            if !source.files.iter().any(|known| known == entry) {
                return Err(Error::Conflict("relocation source manifest changed".into()));
            }
            let mut parent = Path::new(&entry.path).parent();
            while let Some(path) = parent {
                if path.as_os_str().is_empty() {
                    break;
                }
                directories.insert(path.to_path_buf());
                parent = path.parent();
            }
        }
        for directory in &directories {
            crate::fileops::parents(&scratch.path, directory)?;
            fs::open_relative(
                &scratch_root,
                directory.to_str().ok_or_else(|| {
                    Error::Conflict("relocation scratch directory is not UTF-8".into())
                })?,
            )?;
        }
        for entry in copy_files.iter().filter(|entry| !entry.identity.directory) {
            checkpoint()?;
            let source_file = fs::open_relative(&source_root, &entry.path)?;
            if fs::identity_for_file(&source_file)? != entry.identity {
                return Err(Error::Conflict("source changed during relocation".into()));
            }
            let target_file = match fs::open_relative(&scratch_root, &entry.path) {
                Ok(file) => Some(file),
                Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error),
            };
            if let Some(target_file) = target_file {
                let target_identity = fs::identity_for_file(&target_file)?;
                if target_identity.directory {
                    return Err(Error::Conflict(
                        "relocation scratch file became a directory".into(),
                    ));
                }
                let digest = |file: &File| -> Result<Vec<u8>> {
                    let mut file = file.try_clone()?;
                    file.rewind()?;
                    let mut hasher = Sha256::new();
                    let mut buffer = [0u8; 64 * 1024];
                    loop {
                        checkpoint()?;
                        let count = file.read(&mut buffer)?;
                        if count == 0 {
                            break;
                        }
                        hasher.update(&buffer[..count]);
                    }
                    Ok(hasher.finalize().to_vec())
                };
                let equal = target_identity.bytes == entry.identity.bytes
                    && digest(&source_file)? == digest(&target_file)?
                    && fs::identity_for_file(&source_file)? == entry.identity
                    && fs::identity_for_file(&target_file)? == target_identity;
                if equal {
                    target_file.sync_all()?;
                    continue;
                }
                // The final pathname is never replaced based on a path-only
                // observation. It may be unrelated or externally modified;
                // leave it in place and require review.
                return Err(Error::Conflict(
                    "completed relocation scratch file differs from source".into(),
                ));
            }
            let mut input = fs::open_relative(&source_root, &entry.path)?;
            let relative = Path::new(&entry.path);
            let parent = relative
                .parent()
                .filter(|path| !path.as_os_str().is_empty());
            let target_parent = match parent {
                Some(parent) => fs::open_relative(
                    &scratch_root,
                    parent.to_str().ok_or_else(|| {
                        Error::Conflict("relocation scratch parent is not UTF-8".into())
                    })?,
                )?,
                None => scratch_root.try_clone()?,
            };
            let temp_suffix = Sha256::digest(format!("{}\n{}", scratch.id, entry.path).as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let temp_name = format!(".runner-copy-{temp_suffix}");
            let temp_path = Path::new(&temp_name);
            if let Ok(temp_file) = fs::open_relative(&scratch_root, &temp_name) {
                let temp_identity = fs::identity_for_file(&temp_file)?;
                if temp_identity.directory {
                    return Err(Error::Conflict(
                        "relocation temporary copy became a directory".into(),
                    ));
                }
                let temp_entry = fs::manifest(&scratch_root, 100_000)?
                    .into_iter()
                    .find(|observed| observed.path == temp_name)
                    .ok_or_else(|| Error::Conflict("temporary copy disappeared".into()))?;
                fs::remove_entry(&scratch_root, &temp_entry)?;
            }
            let mut output = fs::create_file_exclusive_at(&scratch_root, temp_path)?;
            let created_identity = fs::identity_for_file(&output)?;
            let mut bytes = 0u64;
            let mut buffer = [0u8; 65_536];
            loop {
                checkpoint()?;
                let count = input.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                checkpoint()?;
                output.write_all(&buffer[..count])?;
                boundary("copy_chunk", source.identity.as_ref())?;
                bytes += count as u64;
            }
            output.sync_all()?;
            let digest = |file: &File| -> Result<Vec<u8>> {
                let mut file = file.try_clone()?;
                file.rewind()?;
                let mut hasher = Sha256::new();
                let mut buffer = [0u8; 65_536];
                loop {
                    checkpoint()?;
                    let count = file.read(&mut buffer)?;
                    if count == 0 {
                        break;
                    }
                    hasher.update(&buffer[..count]);
                }
                Ok(hasher.finalize().to_vec())
            };
            drop(output);
            // create_file_exclusive_at intentionally returns a write-only
            // descriptor. Reopen the private temporary file read-only before
            // verifying its bytes, and bind that read to the object just
            // written so an intervening replacement cannot be adopted.
            let output = fs::open_relative(&scratch_root, &temp_name)?;
            let reopened_identity = fs::identity_for_file(&output)?;
            if !created_identity.same_object(&reopened_identity)
                || bytes != entry.identity.bytes
                || digest(&input)? != digest(&output)?
                || fs::identity_for_file(&input)? != entry.identity
                || fs::identity_for_file(&output)? != reopened_identity
            {
                return Err(Error::Conflict(
                    "relocation copy verification failed".into(),
                ));
            }
            drop(output);
            fs::rename_exclusive(&scratch.path.join(&temp_name), &scratch.path.join(relative))?;
            fs::sync_directory(&target_parent)?;
        }
        for directory in directories.iter().rev() {
            let relative = directory.to_str().ok_or_else(|| {
                Error::Conflict("selected publication directory is not UTF-8".into())
            })?;
            fs::sync_directory(&fs::open_relative(&scratch_root, relative)?)?;
        }
        fs::sync_directory(&scratch_root)?;
        scratch.files = fs::manifest(&scratch_root, 100_000)?;
        let mut expected_paths: std::collections::BTreeSet<String> = copy_files
            .iter()
            .filter(|entry| !entry.identity.directory)
            .map(|entry| entry.path.clone())
            .collect();
        for entry in copy_files.iter().filter(|entry| !entry.identity.directory) {
            let mut parent = Path::new(&entry.path).parent();
            while let Some(path) = parent {
                if path.as_os_str().is_empty() {
                    break;
                }
                expected_paths.insert(path.to_string_lossy().into_owned());
                parent = path.parent();
            }
        }
        let actual_paths: std::collections::BTreeSet<_> = scratch
            .files
            .iter()
            .map(|entry| entry.path.clone())
            .collect();
        if actual_paths != expected_paths {
            return Err(Error::Conflict(
                "selected publication tree differs from durable selection".into(),
            ));
        }
        Ok(())
    }

    fn relocate_checked_mode_inner(
        &self,
        job: u32,
        destination: &Path,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
        defer_source_retirement: bool,
        publication_files: Option<Vec<FileEntry>>,
        file_outcomes: Vec<DeliveryFileOutcome>,
    ) -> Result<RelocationResult> {
        let guard = self.mutation_guard()?;
        checkpoint()?;
        let mut source = self.for_job(job)?.ok_or(Error::NotFound)?;
        // A committed generation is stable even when its physical path differs
        // from the caller's ordinary destination. Revalidate before reuse.
        let completed = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare("SELECT data FROM operations WHERE artifact=?1 AND state='succeeded' AND json_extract(data,'$.kind')='relocate'")?;
            let rows = statement.query_map([&source.id], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in completed {
            let op: Operation = serde_json::from_str(&raw)?;
            let movement: Relocation = serde_json::from_str(&op.request)?;
            if movement
                .requested_destination
                .as_deref()
                .unwrap_or(&movement.destination)
                == destination
                && movement.destination == source.path
                && movement.defer_source_retirement == defer_source_retirement
                && movement.publication_files == publication_files
                && movement.file_outcomes == file_outcomes
            {
                let dir = self.verify(&source)?;
                if fs::manifest(&dir, 100_000)? != source.files {
                    return Err(Error::Conflict("committed generation changed".into()));
                }
                return Ok(RelocationResult {
                    operation_id: op.id,
                    artifact_id: source.id,
                    generation: source.generation,
                    published_path: source.path,
                });
            }
        }
        if source.path == destination {
            if publication_files.is_some() {
                return Err(Error::Conflict(
                    "selected publication requires a unique target".into(),
                ));
            }
            self.verify(&source)?;
            return Ok(RelocationResult {
                operation_id: format!("move-{}-{}", source.id, source.generation),
                artifact_id: source.id,
                generation: source.generation,
                published_path: source.path,
            });
        }
        // The mutation guard serializes claims. Revisions are not generations:
        // a review outcome cannot authorize a fresh scratch copy.
        let unresolved: i64 = self.db.lock().unwrap().query_row(
            "SELECT COUNT(*) FROM operations WHERE artifact=?1 AND state NOT IN ('succeeded','cancelled') AND json_extract(data,'$.kind')='relocate'",
            [&source.id], |row| row.get(0))?;
        if unresolved > 0 {
            return Err(Error::Conflict(format!(
                "{unresolved} unresolved relocation attempts require reconciliation"
            )));
        }
        if source.state != "active" || source.hold.as_deref().is_some_and(|h| h != "review") {
            return Err(Error::Conflict(
                "payload is not quiescent for relocation".into(),
            ));
        }
        let dir = self.verify(&source)?;
        source.files = fs::manifest(&dir, 100_000)?;
        if let Some(selected) = &publication_files {
            if selected
                .iter()
                .any(|entry| !source.files.iter().any(|observed| observed == entry))
            {
                return Err(Error::Conflict(
                    "selected publication source identity changed".into(),
                ));
            }
        }
        fs::absolute(destination)?;
        let root = destination
            .parent()
            .ok_or_else(|| Error::Conflict("missing destination root".into()))?;
        std::fs::create_dir_all(root)?;
        let root_dir = fs::open_dir(root)?;
        if destination.try_exists()? {
            return Err(Error::Conflict("move destination already exists".into()));
        }
        let key = format!(
            "move-{}-{}-{}-{:x}",
            source.id,
            source.generation,
            source.revision,
            Sha256::digest(destination.as_os_str().as_encoded_bytes())
        );
        let mut relocation = Relocation {
            source: source.clone(),
            destination: destination.into(),
            requested_destination: Some(destination.into()),
            destination_root: fs::identity_for_file(&root_dir)?,
            scratch: None,
            registry: false,
            publication: None,
            publication_key: None,
            cleanup_done: false,
            defer_source_retirement,
            publication_files: publication_files.clone(),
            file_outcomes,
            source_cleanup_removed: Vec::new(),
            source_cleanup_current: None,
        };
        let mut op = Operation {
            id: key.clone(),
            artifact: source.id.clone(),
            kind: "relocate".into(),
            state: "running".into(),
            request: serde_json::to_string(&relocation)?,
            created_at: now(),
            not_before: 0,
            attempts: 1,
            next_retry: 0,
            error: None,
        };
        source.state = "transitioning".into();
        source.revision += 1;
        {
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            save_artifact(&tx, &source)?;
            save_operation(&tx, &op)?;
            tx.commit()?;
        }
        drop(guard);
        let result = (|| {
            let root = relocation.destination.parent().unwrap();
            let root_dir = fs::open_dir(root)?;
            let destination = relocation.destination.clone();
            boundary("before_move", source.identity.as_ref())?;
            checkpoint()?;
            let direct_key = format!("{key}-direct");
            let initial = {
                let mut guard = Some(self.mutation_guard()?);
                self.relocation_source(&op, &relocation)?
                    .ok_or_else(|| Error::Conflict("relocation authority revoked".into()))?;
                self.verify(&relocation.source)?;
                checkpoint()?;
                if defer_source_retirement || publication_files.is_some() {
                    // Delivery handoff and selected publication must leave
                    // the complete processing source intact. A selected tree
                    // is always staged privately, even on the same volume,
                    // so unresolved siblings never enter the completed path.
                    Err(Error::Io(std::io::Error::from(
                        std::io::ErrorKind::CrossesDevices,
                    )))
                } else {
                    self.publish_directory_unlocked(
                        &mut guard,
                        &source.path,
                        &destination,
                        &source.id,
                        &direct_key,
                    )
                }
            };
            match initial {
                Ok(published) => {
                    relocation.publication = Some(published);
                    relocation.publication_key = Some(direct_key);
                    op.request = serde_json::to_string(&relocation)?;
                    self.save_relocation_progress(&op, &relocation)?;
                }
                Err(Error::Io(e))
                    if e.kind() == std::io::ErrorKind::CrossesDevices
                        || e.raw_os_error() == Some(18) =>
                {
                    let _capacity = crate::capacity::reserve(root, source.summary().1)?;
                    let scratch_path = root.join(format!(".runner-{key}"));
                    // An existing name is never authority, even after a crash.
                    std::fs::create_dir(&scratch_path)?;
                    fs::sync_directory(&root_dir)?;
                    let scratch_dir = fs::open_dir(&scratch_path)?;
                    let mut scratch = source.clone();
                    scratch.id = format!("scratch-{key}");
                    scratch.job = None;
                    scratch.generation = id(&self.db.lock().unwrap())?;
                    scratch.path = scratch_path.clone();
                    scratch.root = root.into();
                    scratch.root_identity = relocation.destination_root.clone();
                    scratch.identity = Some(fs::identity_for_file(&scratch_dir)?);
                    scratch.files.clear();
                    scratch.state = "active".into();
                    scratch.owned = true;
                    scratch.keep = true;
                    scratch.hold = Some("review: relocation staging".into());
                    self.sidecar(&scratch)?;
                    save_artifact(&self.db.lock().unwrap(), &scratch)?;
                    relocation.scratch = Some(scratch.clone());
                    op.request = serde_json::to_string(&relocation)?;
                    self.save_relocation_progress(&op, &relocation)?;
                    let copy_files = publication_files.as_ref().unwrap_or(&source.files);
                    self.populate_relocation_scratch(
                        &source,
                        &mut scratch,
                        copy_files,
                        checkpoint,
                    )?;
                    relocation.scratch = Some(scratch.clone());
                    op.request = serde_json::to_string(&relocation)?;
                    save_artifact(&self.db.lock().unwrap(), &scratch)?;
                    self.save_relocation_progress(&op, &relocation)?;
                    let copy_key = format!("{key}-copy");
                    let published = {
                        let mut guard = Some(self.mutation_guard()?);
                        self.relocation_source(&op, &relocation)?.ok_or_else(|| {
                            Error::Conflict("relocation authority revoked".into())
                        })?;
                        self.verify(&scratch)?;
                        checkpoint()?;
                        self.publish_directory_unlocked(
                            &mut guard,
                            &scratch_path,
                            &destination,
                            &source.id,
                            &copy_key,
                        )?
                    };
                    relocation.publication = Some(published);
                    relocation.publication_key = Some(copy_key);
                    op.request = serde_json::to_string(&relocation)?;
                    self.save_relocation_progress(&op, &relocation)?;
                }
                Err(e) => return Err(e),
            }
            let _guard = self.mutation_guard()?;
            self.commit_relocation(&mut op, &mut relocation)
        })();
        let guard = self.mutation_guard()?;
        if let Err(e) = result {
            if self.relocation_source(&op, &relocation)?.is_none() {
                self.cancel_obsolete_relocation(&mut op)?;
                return Err(e);
            }
            if matches!(e, Error::Io(_)) {
                let delay = [30, 60, 120, 300][op.attempts.saturating_sub(1).min(3) as usize];
                op.state = "retry".into();
                op.next_retry = now().saturating_add(delay);
            } else {
                op.state = "review".into();
            }
            op.error = Some(e.to_string());
            save_operation(&self.db.lock().unwrap(), &op)?;
            if let Some(scratch) = &relocation.scratch {
                let mut row = self.get(&scratch.id)?;
                row.state = "retained".into();
                save_artifact(&self.db.lock().unwrap(), &row)?;
            }
            // Keep the source identity at its known location when publication
            // did not happen, allowing PP to report retained files accurately.
            if self.verify(&relocation.source).is_ok() {
                let mut retained = self.get(&relocation.source.id)?;
                retained.state = "active".into();
                retained.error = Some(e.to_string());
                save_artifact(&self.db.lock().unwrap(), &retained)?;
            }
            return Err(e);
        }
        drop(guard);
        if !defer_source_retirement {
            if let Err(e) = self.retire_relocation_source(&op.id) {
                tracing::warn!(operation=%op.id, error=%e, "relocation committed; source retirement pending");
            }
        }
        let published = self.get(&source.id)?;
        Ok(RelocationResult {
            operation_id: op.id,
            artifact_id: published.id,
            generation: published.generation,
            published_path: published.path,
        })
    }
    fn commit_relocation(&self, op: &mut Operation, movement: &mut Relocation) -> Result<()> {
        let mut current = self.relocation_source(op, movement)?.ok_or_else(|| {
            Error::Conflict("relocation no longer owns the source generation".into())
        })?;
        let root = movement.destination.parent().unwrap();
        let root_dir = fs::open_dir(root)?;
        if !movement
            .destination_root
            .same_object(&fs::identity_for_file(&root_dir)?)
        {
            return Err(Error::Conflict("move destination root changed".into()));
        }
        boundary("registry_commit", None)?;
        let target = fs::open_dir(&movement.destination)?;
        let expected = movement.scratch.as_ref().unwrap_or(&movement.source);
        let observed = fs::identity_for_file(&target)?;
        let expected_identity = movement
            .publication
            .as_ref()
            .map(|p| &p.identity)
            .or(expected.identity.as_ref());
        if !expected_identity.is_some_and(|i| i.same_object(&observed)) {
            return Err(Error::Conflict("move publication identity changed".into()));
        }
        let files = fs::manifest(&target, 100_000)?;
        let expected_files = movement
            .publication
            .as_ref()
            .map(|p| &p.files)
            .unwrap_or(&expected.files);
        if &files != expected_files {
            return Err(Error::Conflict("move publication contents changed".into()));
        }
        // A verified hard-link publication changes ctime on the shared source
        // inode. Carry forward only that controlled transition: the original,
        // source, and published entries must remain the same object with the
        // same size/mtime, and the target must exactly match its durable
        // post-link publication snapshot. This lets the later source-delete
        // journal authorize the duplicate without weakening external-change
        // checks.
        let rebound_source_files = if movement
            .publication
            .as_ref()
            .is_some_and(|publication| publication.source_retained)
            && movement.scratch.is_none()
        {
            let source_root = fs::open_dir(&movement.source.path)?;
            let source_files = fs::manifest(&source_root, 100_000)?;
            if source_files.len() != movement.source.files.len()
                || source_files.len() != files.len()
            {
                return Err(Error::Conflict(
                    "hard-link source manifest changed during publication".into(),
                ));
            }
            for ((original, observed), published) in
                movement.source.files.iter().zip(&source_files).zip(&files)
            {
                if original.path != observed.path
                    || observed.path != published.path
                    || original.identity.directory != observed.identity.directory
                    || observed.identity.directory != published.identity.directory
                {
                    return Err(Error::Conflict(
                        "hard-link source manifest paths changed during publication".into(),
                    ));
                }
                if original.identity.directory {
                    // Directory entries in the link fallback are created
                    // privately under the target; they are intentionally not
                    // the same directory object as their source parents.
                    if !original.identity.same_object(&observed.identity) {
                        return Err(Error::Conflict(
                            "hard-link source directory changed during publication".into(),
                        ));
                    }
                } else if !original.identity.same_object(&observed.identity)
                    || original.identity.bytes != observed.identity.bytes
                    || original.identity.modified != observed.identity.modified
                    || observed.identity != published.identity
                {
                    return Err(Error::Conflict(format!(
                        "hard-link source file changed during publication: {}",
                        original.path
                    )));
                }
            }
            Some(source_files)
        } else {
            None
        };
        if let Some(files) = &rebound_source_files {
            movement.source.files = files.clone();
        }
        current.path = movement.destination.clone();
        current.root = root.into();
        current.root_identity = movement.destination_root.clone();
        current.identity = Some(fs::identity_for_file(&target)?);
        current.files = files;
        current.state = "active".into();
        current.revision += 2;
        self.sidecar(&current)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        if let Some(scratch) = &movement.scratch {
            let mut obsolete = scratch.clone();
            obsolete.state = "source_gone".into();
            obsolete.updated_at = now();
            save_artifact(&tx, &obsolete)?;
        }
        if movement.scratch.is_some()
            || movement
                .publication
                .as_ref()
                .is_some_and(|p| p.source_retained)
        {
            let mut old = movement.source.clone();
            old.id = format!("source-{}", op.id);
            old.job = None;
            old.state = "retained".into();
            // Verified, synced destination and custody commit authorize
            // retiring this duplicate source through the existing delete journal.
            old.hold = None;
            old.keep = false;
            old.retention_seconds = 0;
            old.deadline = None;
            self.sidecar(&old)?;
            save_artifact(&tx, &old)?;
            if old.owned
                && old.automatic_cleanup_eligible
                && !old.keep
                && old.hold.is_none()
                && !movement.defer_source_retirement
                && movement.publication_files.is_none()
            {
                // Publication and retirement admission share one transaction.
                save_operation(
                    &tx,
                    &Operation {
                        id: format!("retire-{}", op.id),
                        artifact: old.id.clone(),
                        kind: "delete".into(),
                        state: "queued".into(),
                        request: serde_json::to_string(&(
                            &old.id,
                            old.revision,
                            0u64,
                            false,
                            &old.generation,
                        ))?,
                        created_at: now(),
                        not_before: now(),
                        attempts: 0,
                        next_retry: 0,
                        error: None,
                    },
                )?;
            }
        }
        save_artifact(&tx, &current)?;
        op.state = "succeeded".into();
        op.error = None;
        op.request = serde_json::to_string(movement)?;
        save_operation(&tx, op)?;
        event(
            &tx,
            &current.id,
            "relocated",
            &current.path.to_string_lossy(),
        )?;
        tx.commit()?;
        Ok(())
    }
    fn retire_relocation_source(&self, key: &str) -> Result<()> {
        let mut op = self.operation(key)?;
        let mut movement: Relocation = serde_json::from_str(&op.request)?;
        if op.state != "succeeded" || movement.cleanup_done {
            return Ok(());
        }
        if !movement.source.automatic_cleanup_eligible {
            // Keep the old source physically in place without recording
            // cleanup_done or a successful Delivery cleanup outcome.
            return Ok(());
        }
        if movement.defer_source_retirement {
            let ready: bool = self.db.lock().unwrap().query_row(
                "SELECT EXISTS(SELECT 1 FROM deliveries WHERE artifact=?1 AND generation=?2)",
                params![movement.source.id, movement.source.generation],
                |row| row.get(0),
            )?;
            if !ready || !movement.source.delivery_lifecycle {
                return Err(Error::Conflict(
                    "deferred relocation source cannot retire before durable delivery".into(),
                ));
            }
        }
        if movement.scratch.is_some() {
            if let Some(publication_key) = &movement.publication_key {
                let _guard = self.mutation_guard()?;
                self.retire_publication_source_unlocked(publication_key)?;
            }
        }
        let expected_source_record = movement.scratch.is_some()
            || movement
                .publication
                .as_ref()
                .is_some_and(|published| published.source_retained);
        match self.get(&format!("source-{key}")) {
            Ok(old) => {
                if !old.terminal() {
                    if !old.owned || old.keep || old.hold.is_some() {
                        return Err(Error::Conflict(format!(
                            "source retirement is blocked by ownership/hold state ({})",
                            old.state
                        )));
                    }
                    if movement.publication_files.is_some() {
                        if !self.retire_selected_source_entries(&mut op, &mut movement, old)? {
                            let retained = movement
                                .file_outcomes
                                .iter()
                                .filter(|outcome| outcome.source_cleanup_status == "retained")
                                .map(|outcome| outcome.source_path.as_str())
                                .collect::<Vec<_>>()
                                .join(", ");
                            return Err(Error::Conflict(format!(
                                "blocked: retained source files [{retained}]; extract or classify them, or explicitly remove them from Runner storage"
                            )));
                        }
                    } else {
                        let delete_key = format!("retire-{key}");
                        let deletion = match self.operation(&delete_key) {
                            Ok(op) => op,
                            Err(Error::NotFound) => self.request_delete(
                                &old.id,
                                old.revision,
                                &format!("retire-{:x}", Sha256::digest(key.as_bytes())),
                                0,
                            )?,
                            Err(e) => return Err(e),
                        };
                        let result = self.execute_delete(&deletion.id)?;
                        if result.state != "succeeded" {
                            return Err(Error::Conflict(format!(
                                "source retirement {}: {}",
                                result.state,
                                result.error.unwrap_or_default()
                            )));
                        }
                    }
                }
            }
            Err(Error::NotFound) if expected_source_record => {
                return Err(Error::Conflict(
                    "expected source artifact record is missing; retirement outcome is unresolved"
                        .into(),
                ));
            }
            Err(Error::NotFound) => {}
            Err(error) => return Err(error),
        }
        // Source deletion and this manifest refresh are separate durable
        // writes. Re-run the digest-bound refresh even when the source row is
        // already terminal, covering a crash immediately after unlink.
        if movement.scratch.is_none()
            && movement
                .publication
                .as_ref()
                .is_some_and(|publication| publication.source_retained)
        {
            self.refresh_hardlink_publication_after_source_unlink(&movement)?;
        }
        movement.cleanup_done = true;
        op.request = serde_json::to_string(&movement)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_operation(&tx, &op)?;
        tx.execute(
            "UPDATE deliveries SET cleanup_status='succeeded',cleanup_error=NULL,cleanup_next_retry_at=NULL,cleanup_blocked_since=NULL WHERE artifact=?1 AND generation=?2",
            params![movement.source.id, movement.source.generation],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Unlinking the verified source hardlink changes ctime on the still-live
    /// published inode. Refresh that exact published generation after the
    /// authorized source-delete journal succeeds, accepting only the same
    /// inode/path/size/mtime set; all later operations use the resulting exact
    /// identity snapshot.
    fn refresh_hardlink_publication_after_source_unlink(
        &self,
        movement: &Relocation,
    ) -> Result<()> {
        #[cfg(test)]
        if FAIL_SOURCE_UNLINK_REFRESH.with(|fail| fail.replace(false)) {
            return Err(std::io::Error::from_raw_os_error(libc::EIO).into());
        }
        let published = self.get(&movement.source.id)?;
        if published.generation != movement.source.generation
            || published.path != movement.destination
            || published.state != "active"
        {
            return Err(Error::Conflict(
                "published generation changed during source unlink".into(),
            ));
        }
        let root = self.verify(&published)?;
        let observed = fs::manifest(&root, 100_000)?;
        if observed.len() != published.files.len() {
            return Err(Error::Conflict(
                "published manifest changed during source unlink".into(),
            ));
        }
        let publication_key = movement
            .publication_key
            .as_deref()
            .ok_or_else(|| Error::Conflict("hard-link publication has no durable key".into()))?;
        let publication = self.operation(&format!("publish-{publication_key}"))?;
        if publication.state != "succeeded" {
            return Err(Error::Conflict(
                "hard-link publication digest proof is not durable".into(),
            ));
        }
        let request: serde_json::Value = serde_json::from_str(&publication.request)?;
        let mut digests = std::collections::HashMap::new();
        for entry in request["files"].as_array().ok_or_else(|| {
            Error::Conflict("hard-link publication manifest is unavailable".into())
        })? {
            let path = entry["path"]
                .as_str()
                .ok_or_else(|| Error::Conflict("hard-link publication path is invalid".into()))?;
            if entry["identity"]["directory"] == true {
                continue;
            }
            let digest = entry["digest"].as_str().ok_or_else(|| {
                Error::Conflict(format!("hard-link publication digest is missing: {path}"))
            })?;
            digests.insert(path.to_owned(), digest.to_owned());
        }
        for (expected, actual) in published.files.iter().zip(&observed) {
            if expected.path != actual.path
                || expected.identity.directory != actual.identity.directory
                || !expected.identity.same_object(&actual.identity)
                || (!actual.identity.directory
                    && (expected.identity.bytes != actual.identity.bytes
                        || expected.identity.modified != actual.identity.modified))
            {
                return Err(Error::Conflict(format!(
                    "published file changed during source unlink: {}",
                    expected.path
                )));
            }
            if !actual.identity.directory {
                let expected_digest = digests.get(&actual.path).ok_or_else(|| {
                    Error::Conflict(format!(
                        "hard-link publication digest is missing: {}",
                        actual.path
                    ))
                })?;
                let mut file = fs::open_relative(&root, &actual.path)?;
                let before = fs::identity_for_file(&file)?;
                if before != actual.identity {
                    return Err(Error::Conflict(format!(
                        "published file changed before source-unlink verification: {}",
                        actual.path
                    )));
                }
                let mut hasher = Sha256::new();
                let mut buffer = [0u8; 64 * 1024];
                loop {
                    let read = file.read(&mut buffer)?;
                    if read == 0 {
                        break;
                    }
                    hasher.update(&buffer[..read]);
                }
                if fs::identity_for_file(&file)? != before
                    || format!("{:x}", hasher.finalize()) != *expected_digest
                {
                    return Err(Error::Conflict(format!(
                        "published file digest changed during source unlink: {}",
                        actual.path
                    )));
                }
            }
        }
        let _guard = self.mutation_guard()?;
        let mut current = self.get(&movement.source.id)?;
        if current.generation != published.generation
            || current.path != published.path
            || current.state != "active"
        {
            return Err(Error::Conflict(
                "published generation changed while refreshing source-unlink identity".into(),
            ));
        }
        if current.files == observed {
            return Ok(());
        }
        if current.revision != published.revision || current.files != published.files {
            return Err(Error::Conflict(
                "published artifact changed while refreshing source-unlink identity".into(),
            ));
        }
        current.files = observed;
        current.revision += 1;
        save_artifact(&self.db.lock().unwrap(), &current)
    }

    fn retire_selected_source_entries(
        &self,
        op: &mut Operation,
        movement: &mut Relocation,
        mut source: Artifact,
    ) -> Result<bool> {
        let _guard = self.mutation_guard()?;
        let root_is_missing = matches!(
            std::fs::symlink_metadata(&source.path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound
        );
        if movement.source_cleanup_current.as_deref() == Some("@root") && root_is_missing {
            source.state = "deleted".into();
            source.files.clear();
            source.updated_at = now();
            source.revision += 1;
            movement.source_cleanup_current = None;
            op.request = serde_json::to_string(movement)?;
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            save_artifact(&tx, &source)?;
            save_operation(&tx, op)?;
            tx.commit()?;
            return Ok(true);
        }
        let root = self.verify(&source)?;
        let observed = fs::manifest(&root, 100_000)?;
        for entry in &observed {
            if !movement.source.files.iter().any(|expected| {
                expected.path == entry.path
                    && expected.identity.same_object(&entry.identity)
                    && (entry.identity.directory || expected.identity == entry.identity)
            }) {
                return Err(Error::Conflict(format!(
                    "source changed outside selected publication cleanup: {}",
                    entry.path
                )));
            }
        }
        for expected in movement
            .source
            .files
            .iter()
            .filter(|entry| !entry.identity.directory)
        {
            let Some(outcome_index) = movement
                .file_outcomes
                .iter()
                .position(|outcome| outcome.source_path == expected.path)
            else {
                return Err(Error::Conflict(format!(
                    "selected publication has no source outcome for {}",
                    expected.path
                )));
            };
            let outcome = movement.file_outcomes[outcome_index].outcome.as_str();
            let resolved = matches!(outcome, "published" | "consumed");
            let recoverable = matches!(
                outcome,
                "retained_processing"
                    | "unverified_review"
                    | "archive_review"
                    | "unsupported_review"
                    | "assessment_retry"
            );
            if recoverable || (source.keep_on_failure && !resolved) {
                movement.file_outcomes[outcome_index].source_cleanup_status = if recoverable {
                    "retained".into()
                } else {
                    "retained_keep".into()
                };
                op.request = serde_json::to_string(movement)?;
                save_operation(&self.db.lock().unwrap(), op)?;
                continue;
            }
            if movement.file_outcomes[outcome_index].source_cleanup_status == "removed" {
                continue;
            }
            let present = observed.iter().find(|entry| entry.path == expected.path);
            if let Some(actual) = present {
                if actual != expected {
                    return Err(Error::Conflict(format!(
                        "source file identity changed: {}",
                        expected.path
                    )));
                }
                movement.source_cleanup_current = Some(expected.path.clone());
                op.request = serde_json::to_string(movement)?;
                save_operation(&self.db.lock().unwrap(), op)?;
                fs::remove_entry(&root, expected)?;
                let parent = Path::new(&expected.path).parent().unwrap_or(Path::new(""));
                let parent_dir = if parent.as_os_str().is_empty() {
                    root.try_clone()?
                } else {
                    fs::open_relative(
                        &root,
                        parent
                            .to_str()
                            .ok_or_else(|| Error::Conflict("non-UTF-8 source path".into()))?,
                    )?
                };
                fs::sync_directory(&parent_dir)?;
            } else if movement.source_cleanup_current.as_deref() != Some(expected.path.as_str()) {
                return Err(Error::Conflict(format!(
                    "source file disappeared before cleanup authority: {}",
                    expected.path
                )));
            }
            movement.file_outcomes[outcome_index].source_cleanup_status = "removed".into();
            if !movement.source_cleanup_removed.contains(&expected.path) {
                movement.source_cleanup_removed.push(expected.path.clone());
            }
            movement.source_cleanup_current = None;
            source.files = fs::manifest(&root, 100_000)?;
            movement.source_cleanup_current = None;
            op.request = serde_json::to_string(movement)?;
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            save_operation(&tx, op)?;
            save_artifact(&tx, &source)?;
            if let Some(raw) = tx
                .query_row(
                    "SELECT data FROM deliveries WHERE artifact=?1 AND generation=?2",
                    params![movement.source.id, movement.source.generation],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
            {
                let mut delivery: crate::artifacts::Delivery = serde_json::from_str(&raw)?;
                if let Some(row) = delivery
                    .file_outcomes
                    .iter_mut()
                    .find(|row| row.source_path == expected.path)
                {
                    row.source_cleanup_status = "removed".into();
                }
                tx.execute(
                    "UPDATE deliveries SET data=?3 WHERE artifact=?1 AND generation=?2",
                    params![
                        movement.source.id,
                        movement.source.generation,
                        serde_json::to_string(&delivery)?
                    ],
                )?;
            }
            tx.commit()?;
        }
        let mut remaining = fs::manifest(&root, 100_000)?;
        remaining.sort_by_key(|entry| std::cmp::Reverse(entry.path.matches('/').count()));
        for directory in remaining.iter().filter(|entry| entry.identity.directory) {
            if movement.file_outcomes.iter().any(|outcome| {
                (matches!(
                    outcome.outcome.as_str(),
                    "retained_processing"
                        | "unverified_review"
                        | "archive_review"
                        | "unsupported_review"
                        | "assessment_retry"
                ) || (source.keep_on_failure
                    && !matches!(outcome.outcome.as_str(), "published" | "consumed")))
                    && (outcome.source_path == directory.path
                        || outcome
                            .source_path
                            .starts_with(&format!("{}/", directory.path)))
            }) {
                continue;
            }
            let child = fs::open_relative(&root, &directory.path)?;
            if fs::names(&child)?.is_empty() {
                fs::remove_entry(&root, directory)?;
                let parent = Path::new(&directory.path).parent().unwrap_or(Path::new(""));
                let parent_dir = if parent.as_os_str().is_empty() {
                    root.try_clone()?
                } else {
                    fs::open_relative(
                        &root,
                        parent
                            .to_str()
                            .ok_or_else(|| Error::Conflict("non-UTF-8 source path".into()))?,
                    )?
                };
                fs::sync_directory(&parent_dir)?;
            }
        }
        source.files = fs::manifest(&root, 100_000)?;
        let retained_paths: Vec<_> = movement
            .file_outcomes
            .iter()
            .filter(|outcome| {
                matches!(
                    outcome.source_cleanup_status.as_str(),
                    "retained" | "retained_keep"
                )
            })
            .map(|outcome| outcome.source_path.clone())
            .collect();
        if !retained_paths.is_empty() {
            source.state = "retained".into();
            source.updated_at = now();
            source.revision += 1;
            self.sidecar(&source)?;
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            save_artifact(&tx, &source)?;
            op.request = serde_json::to_string(movement)?;
            save_operation(&tx, op)?;
            tx.commit()?;
            return Ok(false);
        }
        if !fs::names(&root)?.is_empty() {
            return Err(Error::Conflict(
                "unresolved entries remain after per-file cleanup".into(),
            ));
        }
        let parent = fs::open_dir(
            source
                .path
                .parent()
                .ok_or_else(|| Error::Conflict("source has no parent".into()))?,
        )?;
        let current = fs::open_at(&parent, Path::new(source.path.file_name().unwrap()), true)?;
        let current_identity = fs::identity_for_file(&current)?;
        if !source
            .identity
            .as_ref()
            .is_some_and(|identity| identity.same_object(&current_identity))
        {
            return Err(Error::Conflict(
                "source directory identity changed before cleanup".into(),
            ));
        }
        movement.source_cleanup_current = Some("@root".into());
        op.request = serde_json::to_string(movement)?;
        save_operation(&self.db.lock().unwrap(), op)?;
        fs::unlink(&parent, Path::new(source.path.file_name().unwrap()), true)?;
        fs::sync_directory(&parent)?;
        source.state = "deleted".into();
        source.files.clear();
        source.updated_at = now();
        source.revision += 1;
        self.sidecar(&source)?;
        movement.source_cleanup_current = None;
        op.request = serde_json::to_string(movement)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &source)?;
        save_operation(&tx, op)?;
        tx.commit()?;
        Ok(true)
    }

    /// Retire only a deferred relocation source whose exact publication
    /// generation already has a durable Delivery obligation.
    pub(super) fn retire_source_for_delivery(
        &self,
        artifact_id: &str,
        generation: &str,
        publication_path: &Path,
    ) -> Result<bool> {
        let published = self.get(artifact_id)?;
        if published.generation != generation || !published.delivery_lifecycle {
            return Err(Error::Conflict(
                "source retirement is not authorized for this delivery cohort".into(),
            ));
        }
        let ids = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare(
                "SELECT id FROM operations WHERE artifact=?1 AND json_extract(data,'$.kind')='relocate' ORDER BY rowid DESC",
            )?;
            let rows = statement.query_map([artifact_id], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let mut source_relocation_seen = false;
        for id in ids {
            let op = self.operation(&id)?;
            let movement: Relocation = serde_json::from_str(&op.request)?;
            if movement.source.generation != generation {
                continue;
            }
            source_relocation_seen = true;
            if op.state == "succeeded"
                && movement.defer_source_retirement
                && movement.destination == publication_path
            {
                self.retire_relocation_source(&id)?;
                return Ok(true);
            }
        }
        if !source_relocation_seen && published.origin_path == publication_path {
            // The exact artifact generation was published in place and has
            // no relocation journal, so there is no duplicate source copy.
            return Ok(true);
        }
        Err(Error::Conflict(
            "durable delivery has no matching source-retirement operation".into(),
        ))
    }

    pub fn delivery_file_outcomes(
        &self,
        artifact_id: &str,
        generation: &str,
    ) -> Result<Vec<DeliveryFileOutcome>> {
        let raw_rows = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare(
                "SELECT data FROM operations WHERE artifact=?1 AND state='succeeded' AND json_extract(data,'$.kind')='relocate' ORDER BY rowid DESC",
            )?;
            let rows = statement.query_map([artifact_id], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in raw_rows {
            let operation: Operation = serde_json::from_str(&raw)?;
            let movement: Relocation = serde_json::from_str(&operation.request)?;
            if movement.source.generation == generation
                && movement.defer_source_retirement
                && movement.publication_files.is_some()
            {
                return Ok(movement.file_outcomes);
            }
        }
        Ok(Vec::new())
    }

    pub(super) fn delivery_source_ref(
        &self,
        artifact_id: &str,
        generation: &str,
        publication_path: &str,
    ) -> Result<(String, String)> {
        let raws = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare(
                "SELECT data FROM operations WHERE artifact=?1 AND json_extract(data,'$.kind')='relocate' ORDER BY rowid DESC",
            )?;
            let rows = statement.query_map([artifact_id], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in raws {
            let operation: Operation = serde_json::from_str(&raw)?;
            let movement: Relocation = serde_json::from_str(&operation.request)?;
            if movement.source.generation == generation
                && movement.destination.to_string_lossy() == publication_path
                && movement.defer_source_retirement
            {
                return Ok((format!("source-{}", operation.id), generation.to_string()));
            }
        }
        // In-place publication has no duplicate processing artifact.
        Ok((artifact_id.to_string(), generation.to_string()))
    }

    pub fn retire_delivery_source(&self, delivery_id: &str) -> Result<crate::artifacts::Delivery> {
        let delivery = self.delivery(delivery_id)?;
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let claimed = tx.execute(
            "UPDATE deliveries SET cleanup_attempts=cleanup_attempts+1,cleanup_status='running',cleanup_error=NULL,cleanup_next_retry_at=NULL,cleanup_started_at=COALESCE(cleanup_started_at,?2),cleanup_last_attempt_at=?2,cleanup_completed_at=NULL WHERE id=?1 AND cleanup_status IN ('pending','retry') AND COALESCE(cleanup_next_retry_at,0)<=?2",
            params![delivery_id, now()],
        )?;
        let attempt: Option<u32> = if claimed > 0 {
            let attempt: u32 = tx.query_row(
                "SELECT cleanup_attempts FROM deliveries WHERE id=?1",
                [delivery_id],
                |row| row.get(0),
            )?;
            let started_at_ms = now_millis();
            let previous_end: Option<i64> = tx.query_row(
                "SELECT MAX(ended_at) FROM delivery_cleanup_attempts WHERE delivery_id=?1",
                [delivery_id],
                |row| row.get(0),
            )?;
            let wait_ms = previous_end.map_or(0, |end| started_at_ms.saturating_sub(end).max(0));
            tx.execute("INSERT INTO delivery_cleanup_attempts(delivery_id,attempt,started_at,wait_ms,outcome) VALUES(?1,?2,?3,?4,'running')", params![delivery_id, attempt, started_at_ms, wait_ms])?;
            Some(attempt)
        } else {
            None
        };
        tx.commit()?;
        drop(db);
        drop(_guard);
        let Some(attempt) = attempt else {
            return self.delivery(delivery_id);
        };
        let result = self.retire_source_for_delivery(
            &delivery.artifact_id,
            &delivery.publication_generation,
            Path::new(&delivery.publication_path),
        );
        match result {
            Ok(true) => {
                self.finish_cleanup_attempt(delivery_id, attempt, "succeeded", None)?;
                self.db.lock().unwrap().execute(
                    "UPDATE deliveries SET cleanup_status='succeeded',cleanup_error=NULL,cleanup_next_retry_at=NULL,cleanup_blocked_since=NULL,cleanup_completed_at=?2 WHERE id=?1",
                    params![delivery_id, now()],
                )?;
                self.delivery(delivery_id)
            }
            Ok(false) => {
                let attempts: u32 = self.db.lock().unwrap().query_row(
                    "SELECT cleanup_attempts FROM deliveries WHERE id=?1",
                    [delivery_id],
                    |row| row.get(0),
                )?;
                let delay = match attempts {
                    0 | 1 => 30,
                    2 => 60,
                    3 => 120,
                    _ => 300,
                };
                let message = "source retirement could not prove disposal or preserved ownership";
                self.finish_cleanup_attempt(delivery_id, attempt, "retry", Some(message))?;
                self.db.lock().unwrap().execute(
                    "UPDATE deliveries SET cleanup_status='retry',cleanup_error=?2,cleanup_next_retry_at=?3,cleanup_blocked_since=COALESCE(cleanup_blocked_since,?4) WHERE id=?1",
                    params![delivery_id, message, now().saturating_add(delay), now()],
                )?;
                self.delivery(delivery_id)
            }
            Err(error) => {
                let message = error.to_string();
                if message.starts_with("blocked:") {
                    self.finish_cleanup_attempt(delivery_id, attempt, "blocked", Some(&message))?;
                    self.db.lock().unwrap().execute(
                        "UPDATE deliveries SET cleanup_status='blocked',cleanup_error=?2,cleanup_next_retry_at=NULL,cleanup_blocked_since=COALESCE(cleanup_blocked_since,?3) WHERE id=?1",
                        params![delivery_id, message, now()],
                    )?;
                    return self.delivery(delivery_id);
                }
                let attempts: u32 = self.db.lock().unwrap().query_row(
                    "SELECT cleanup_attempts FROM deliveries WHERE id=?1",
                    [delivery_id],
                    |row| row.get(0),
                )?;
                let delay = match attempts {
                    0 | 1 => 30,
                    2 => 60,
                    3 => 120,
                    _ => 300,
                };
                let next_retry = now().saturating_add(delay);
                self.finish_cleanup_attempt(delivery_id, attempt, "retry", Some(&message))?;
                self.db.lock().unwrap().execute(
                    "UPDATE deliveries SET cleanup_status='retry',cleanup_error=?2,cleanup_next_retry_at=?3,cleanup_blocked_since=COALESCE(cleanup_blocked_since,?4) WHERE id=?1",
                    params![delivery_id, message, next_retry, now()],
                )?;
                self.delivery(delivery_id)
            }
        }
    }

    fn finish_cleanup_attempt(
        &self,
        delivery_id: &str,
        attempt: u32,
        outcome: &str,
        error: Option<&str>,
    ) -> Result<()> {
        self.db.lock().unwrap().execute(
            "UPDATE delivery_cleanup_attempts SET ended_at=?3,outcome=?4,error=?5 WHERE delivery_id=?1 AND attempt=?2 AND ended_at IS NULL",
            params![delivery_id, attempt, now_millis(), outcome, error],
        )?;
        Ok(())
    }

    /// User-triggered reconciliation after the source became available or an
    /// operator resolved a blocking condition. This retries only Runner's
    /// separately identified source; the transferred publication is untouched.
    pub fn retry_delivery_source_cleanup(
        &self,
        delivery_id: &str,
    ) -> Result<crate::artifacts::Delivery> {
        let changed = self.db.lock().unwrap().execute(
            "UPDATE deliveries SET cleanup_status='retry',cleanup_error=NULL,cleanup_next_retry_at=0 WHERE id=?1 AND cleanup_status IN ('blocked','retry')",
            [delivery_id],
        )?;
        if changed == 0 {
            return self.delivery(delivery_id);
        }
        self.retire_delivery_source(delivery_id)
    }
    pub fn reconcile_relocations(&self) -> Result<()> {
        let mut guard = Some(self.mutation_guard()?);
        let raws = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare("SELECT data FROM operations WHERE (state IN ('running','review') OR (state='retry' AND COALESCE(json_extract(data,'$.next_retry'),0)<=?1)) AND json_extract(data,'$.kind')='relocate' LIMIT 25")?;
            let rows = stmt.query_map([now()], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let mut retired = Vec::new();
        for raw in raws {
            let mut op: Operation = serde_json::from_str(&raw)?;
            let mut movement: Relocation = serde_json::from_str(&op.request)?;
            if self.relocation_source(&op, &movement)?.is_none() {
                self.cancel_obsolete_relocation(&mut op)?;
                continue;
            }
            // Preserve an admitted deletion's revision/hold through its undo
            // window. Once it starts, execute_delete transfers authority;
            // cancelling it leaves ordinary relocation recovery available.
            let deletion_pending: bool = self.db.lock().unwrap().query_row(
                "SELECT EXISTS(SELECT 1 FROM operations WHERE artifact=?1 AND state='queued' AND json_extract(data,'$.kind')='delete')",
                [&op.artifact], |r| r.get(0),
            )?;
            if deletion_pending {
                continue;
            }
            let recovered = (|| {
                if movement.publication.is_none() && !movement.registry {
                    let (source_path, suffix) = match &movement.scratch {
                        Some(scratch) => (scratch.path.clone(), "copy"),
                        None => (movement.source.path.clone(), "direct"),
                    };
                    let publication_key = format!("{}-{suffix}", op.id);
                    // Only a journaled publication can resume; legacy moves
                    // still use their captured identities in commit_relocation.
                    if self
                        .operation(&format!("publish-{publication_key}"))
                        .is_ok()
                    {
                        movement.publication = Some(self.publish_directory_unlocked(
                            &mut guard,
                            &source_path,
                            &movement.destination,
                            &op.artifact,
                            &publication_key,
                        )?);
                        let current_op = self.operation(&op.id)?;
                        if current_op.request != op.request
                            || !matches!(current_op.state.as_str(), "running" | "review" | "retry")
                            || self.relocation_source(&current_op, &movement)?.is_none()
                        {
                            return Err(Error::Conflict(
                                "relocation authority changed during publication replay".into(),
                            ));
                        }
                        movement.publication_key = Some(publication_key);
                        op.request = serde_json::to_string(&movement)?;
                        save_operation(&self.db.lock().unwrap(), &op)?;
                    } else if let Some(scratch_snapshot) = movement.scratch.as_ref() {
                        // A crash or retryable I/O failure may leave a partial
                        // private copy before publication was journaled. Resume
                        // only after each existing file is matched against the
                        // exact source generation; never expose this tree yet.
                        let mut scratch = self.get(&scratch_snapshot.id)?;
                        let copy_files = movement
                            .publication_files
                            .as_ref()
                            .unwrap_or(&movement.source.files);
                        drop(guard.take());
                        let checkpoints = std::sync::atomic::AtomicUsize::new(0);
                        let copy_checkpoint = || {
                            use std::sync::atomic::Ordering;
                            if checkpoints
                                .fetch_add(1, Ordering::Relaxed)
                                .is_multiple_of(16)
                            {
                                let current_op = self.operation(&op.id).map_err(|error| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::Interrupted,
                                        error.to_string(),
                                    )
                                })?;
                                let current_source = self.get(&op.artifact).map_err(|error| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::Interrupted,
                                        error.to_string(),
                                    )
                                })?;
                                if !matches!(
                                    current_op.state.as_str(),
                                    "running" | "review" | "retry"
                                ) || current_source.generation != movement.source.generation
                                    || current_source.path != movement.source.path
                                {
                                    return Err(std::io::Error::new(
                                        std::io::ErrorKind::Interrupted,
                                        "relocation authority changed during resumed copy",
                                    ));
                                }
                            }
                            Ok(())
                        };
                        self.populate_relocation_scratch(
                            &movement.source,
                            &mut scratch,
                            copy_files,
                            &copy_checkpoint,
                        )?;
                        guard = Some(self.mutation_guard()?);
                        let current_op = self.operation(&op.id)?;
                        if current_op.request != op.request
                            || !matches!(current_op.state.as_str(), "running" | "review" | "retry")
                        {
                            return Err(Error::Conflict(
                                "relocation journal changed during resumed copy".into(),
                            ));
                        }
                        self.relocation_source(&current_op, &movement)?
                            .ok_or_else(|| {
                                Error::Conflict(
                                    "relocation authority changed during resumed copy".into(),
                                )
                            })?;
                        let current_scratch = self.get(&scratch.id)?;
                        if current_scratch.generation != scratch.generation
                            || current_scratch.path != scratch.path
                        {
                            return Err(Error::Conflict(
                                "relocation scratch generation changed during resumed copy".into(),
                            ));
                        }
                        self.verify(&scratch)?;
                        scratch.state = "active".into();
                        scratch.hold = Some("review: relocation staging".into());
                        movement.scratch = Some(scratch.clone());
                        op.request = serde_json::to_string(&movement)?;
                        let mut db = self.db.lock().unwrap();
                        let tx = db.transaction()?;
                        save_artifact(&tx, &scratch)?;
                        save_operation(&tx, &op)?;
                        tx.commit()?;
                        drop(db);
                        let published = self.publish_directory_unlocked(
                            &mut guard,
                            &scratch.path,
                            &movement.destination,
                            &op.artifact,
                            &publication_key,
                        )?;
                        let current_op = self.operation(&op.id)?;
                        if current_op.request != op.request
                            || !matches!(current_op.state.as_str(), "running" | "review" | "retry")
                            || self.relocation_source(&current_op, &movement)?.is_none()
                        {
                            return Err(Error::Conflict(
                                "relocation authority changed during publication".into(),
                            ));
                        }
                        movement.publication = Some(published);
                        movement.publication_key = Some(publication_key);
                        op.request = serde_json::to_string(&movement)?;
                        save_operation(&self.db.lock().unwrap(), &op)?;
                    } else if let Some(copy_files) = movement.publication_files.as_ref() {
                        // Capacity or directory creation may fail before the
                        // first scratch row is durable. Recreate that private
                        // staging phase from the immutable relocation record.
                        let root = movement.destination.parent().ok_or_else(|| {
                            Error::Conflict("move destination has no parent".into())
                        })?;
                        let root_dir = fs::open_dir(root)?;
                        if !movement
                            .destination_root
                            .same_object(&fs::identity_for_file(&root_dir)?)
                        {
                            return Err(Error::Conflict(
                                "move destination root changed before staging retry".into(),
                            ));
                        }
                        let bytes = copy_files
                            .iter()
                            .filter(|entry| !entry.identity.directory)
                            .map(|entry| entry.identity.bytes)
                            .sum();
                        drop(guard.take());
                        let _capacity = crate::capacity::reserve(root, bytes)?;
                        guard = Some(self.mutation_guard()?);
                        let current_op = self.operation(&op.id)?;
                        if !matches!(current_op.state.as_str(), "running" | "review" | "retry")
                            || current_op.request != op.request
                            || self.relocation_source(&current_op, &movement)?.is_none()
                        {
                            return Err(Error::Conflict(
                                "relocation authority changed before staging retry".into(),
                            ));
                        }
                        let scratch_path = root.join(format!(".runner-{}", op.id));
                        match fs::open_dir(&scratch_path) {
                            Ok(existing) if fs::names(&existing)?.is_empty() => {}
                            Ok(_) => {
                                return Err(Error::Conflict(
                                    "unregistered relocation staging path is not empty".into(),
                                ));
                            }
                            Err(Error::Io(error))
                                if error.kind() == std::io::ErrorKind::NotFound =>
                            {
                                std::fs::create_dir(&scratch_path)?;
                                fs::sync_directory(&root_dir)?;
                            }
                            Err(error) => return Err(error),
                        }
                        let scratch_dir = fs::open_dir(&scratch_path)?;
                        let mut scratch = movement.source.clone();
                        scratch.id = format!("scratch-{}", op.id);
                        scratch.job = None;
                        scratch.generation = id(&self.db.lock().unwrap())?;
                        scratch.path = scratch_path.clone();
                        scratch.root = root.into();
                        scratch.root_identity = movement.destination_root.clone();
                        scratch.identity = Some(fs::identity_for_file(&scratch_dir)?);
                        scratch.files.clear();
                        scratch.state = "active".into();
                        scratch.owned = true;
                        scratch.keep = true;
                        scratch.hold = Some("review: relocation staging".into());
                        self.sidecar(&scratch)?;
                        movement.scratch = Some(scratch.clone());
                        op.request = serde_json::to_string(&movement)?;
                        let mut db = self.db.lock().unwrap();
                        let tx = db.transaction()?;
                        save_artifact(&tx, &scratch)?;
                        save_operation(&tx, &op)?;
                        tx.commit()?;
                        drop(db);
                        drop(guard.take());

                        let checkpoints = std::sync::atomic::AtomicUsize::new(0);
                        let copy_checkpoint = || {
                            use std::sync::atomic::Ordering;
                            if checkpoints
                                .fetch_add(1, Ordering::Relaxed)
                                .is_multiple_of(16)
                            {
                                let current_op = self.operation(&op.id).map_err(|error| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::Interrupted,
                                        error.to_string(),
                                    )
                                })?;
                                let current_source = self.get(&op.artifact).map_err(|error| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::Interrupted,
                                        error.to_string(),
                                    )
                                })?;
                                if !matches!(
                                    current_op.state.as_str(),
                                    "running" | "review" | "retry"
                                ) || current_source.generation != movement.source.generation
                                    || current_source.path != movement.source.path
                                {
                                    return Err(std::io::Error::new(
                                        std::io::ErrorKind::Interrupted,
                                        "relocation authority changed during resumed copy",
                                    ));
                                }
                            }
                            Ok(())
                        };
                        self.populate_relocation_scratch(
                            &movement.source,
                            &mut scratch,
                            copy_files,
                            &copy_checkpoint,
                        )?;
                        guard = Some(self.mutation_guard()?);
                        let current_op = self.operation(&op.id)?;
                        if current_op.request != op.request
                            || !matches!(current_op.state.as_str(), "running" | "review" | "retry")
                        {
                            return Err(Error::Conflict(
                                "relocation journal changed during resumed copy".into(),
                            ));
                        }
                        self.relocation_source(&current_op, &movement)?
                            .ok_or_else(|| {
                                Error::Conflict(
                                    "relocation authority changed during resumed copy".into(),
                                )
                            })?;
                        scratch.state = "active".into();
                        scratch.hold = Some("review: relocation staging".into());
                        movement.scratch = Some(scratch.clone());
                        op.request = serde_json::to_string(&movement)?;
                        let mut db = self.db.lock().unwrap();
                        let tx = db.transaction()?;
                        save_artifact(&tx, &scratch)?;
                        save_operation(&tx, &op)?;
                        tx.commit()?;
                        drop(db);
                        let publication_key = format!("{}-copy", op.id);
                        movement.publication = Some(self.publish_directory_unlocked(
                            &mut guard,
                            &scratch.path,
                            &movement.destination,
                            &op.artifact,
                            &publication_key,
                        )?);
                        let current_op = self.operation(&op.id)?;
                        if current_op.request != op.request
                            || !matches!(current_op.state.as_str(), "running" | "review" | "retry")
                            || self.relocation_source(&current_op, &movement)?.is_none()
                        {
                            return Err(Error::Conflict(
                                "relocation authority changed during publication".into(),
                            ));
                        }
                        movement.publication_key = Some(publication_key);
                        op.request = serde_json::to_string(&movement)?;
                        save_operation(&self.db.lock().unwrap(), &op)?;
                    }
                }
                self.commit_relocation(&mut op, &mut movement)
            })();
            if let Err(e) = recovered {
                if guard.is_none() {
                    guard = Some(self.mutation_guard()?);
                }
                let current_op = self.operation(&op.id)?;
                let still_owns_source =
                    matches!(self.relocation_source(&current_op, &movement), Ok(Some(_)));
                if !matches!(current_op.state.as_str(), "running" | "review" | "retry")
                    || current_op.artifact != op.artifact
                    || current_op.kind != op.kind
                    || current_op.request != op.request
                    || !still_owns_source
                {
                    // A concurrent delete, newer generation, or successful
                    // reconciler already owns the durable state. Never revive
                    // it with this stale worker's retry/review snapshot.
                    continue;
                }
                if matches!(e, Error::Io(_)) {
                    op.attempts = op.attempts.saturating_add(1);
                    let delay = [30, 60, 120, 300][op.attempts.saturating_sub(1).min(3) as usize];
                    op.state = "retry".into();
                    op.next_retry = now().saturating_add(delay);
                } else {
                    op.state = "review".into();
                }
                op.error = Some(format!("interrupted move: {e}"));
                if let Some(scratch) = &movement.scratch {
                    if let Ok(mut row) = self.get(&scratch.id) {
                        row.state = "retained".into();
                        save_artifact(&self.db.lock().unwrap(), &row)?;
                    }
                }
                save_operation(&self.db.lock().unwrap(), &op)?;
                let mut a = self.get(&op.artifact)?;
                a.hold = Some("review: interrupted move".into());
                a.state = "retained".into();
                a.revision += 1;
                save_artifact(&self.db.lock().unwrap(), &a)?;
            } else if movement.source.automatic_cleanup_eligible
                && (!movement.defer_source_retirement || movement.publication_files.is_some())
                || self.db.lock().unwrap().query_row(
                    "SELECT EXISTS(SELECT 1 FROM deliveries WHERE artifact=?1 AND generation=?2)",
                    params![op.artifact, movement.source.generation],
                    |row| row.get::<_, bool>(0),
                )?
            {
                retired.push(op.id);
            }
        }
        {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare("SELECT id FROM operations o WHERE state='succeeded' AND json_extract(data,'$.kind')='relocate' AND COALESCE(json_extract(json_extract(data,'$.request'),'$.cleanup_done'),0)=0 AND json_extract(json_extract(data,'$.request'),'$.source.automatic_cleanup_eligible')=1 AND (COALESCE(json_extract(json_extract(data,'$.request'),'$.defer_source_retirement'),0)=0 OR json_extract(json_extract(data,'$.request'),'$.publication_files') IS NOT NULL OR EXISTS (SELECT 1 FROM deliveries d WHERE d.artifact=o.artifact AND d.generation=json_extract(json_extract(o.data,'$.request'),'$.source.generation'))) LIMIT 25")?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            retired.extend(rows.collect::<std::result::Result<Vec<_>, _>>()?);
        }
        drop(guard.take());
        retired.sort();
        retired.dedup();
        for key in retired {
            if let Err(e) = self.retire_relocation_source(&key) {
                tracing::warn!(operation=%key, error=%e, "publication committed; source retirement remains pending");
            }
        }
        Ok(())
    }
}

impl Inventory {
    pub fn pending_relocations(&self, artifact: &str) -> Result<Vec<Operation>> {
        let db = self.db.lock().unwrap();
        let mut stmt = db.prepare("SELECT data FROM operations WHERE artifact=?1 AND state IN ('running','review','retry') AND json_extract(data,'$.kind')='relocate'")?;
        let rows = stmt.query_map([artifact], |r| r.get::<_, String>(0))?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }

    /// Explicitly relinquish one failed move. The original directory must still
    /// match its snapshot. Published/changed identities require reconciliation,
    /// never force-release. Residual scratch and partial targets remain visible
    /// in inventory for an explicit disposition.
    pub fn abandon_relocation(
        &self,
        key: &str,
        revision: u64,
        generation: &str,
    ) -> Result<Artifact> {
        let _guard = self.mutation_guard()?;
        let mut op = self.operation(key)?;
        if op.kind != "relocate" || !matches!(op.state.as_str(), "running" | "review" | "retry") {
            return Err(Error::Conflict("relocation is not pending".into()));
        }
        let movement: Relocation = serde_json::from_str(&op.request)?;
        let mut source = self.get(&op.artifact)?;
        if source.revision != revision
            || source.generation != generation
            || source.generation != movement.source.generation
            || source.path != movement.source.path
            || source.terminal()
        {
            return Err(Error::Conflict(
                "relocation source generation or revision changed".into(),
            ));
        }
        let root = self.verify(&source)?;
        let actual_files = fs::manifest(&root, 100_000)?;
        if actual_files.len() != movement.source.files.len() {
            return Err(Error::Conflict("relocation source contents changed".into()));
        }
        for suffix in ["direct", "copy"] {
            if self
                .operation(&format!("publish-{}-{suffix}", op.id))
                .is_ok_and(|p| p.state == "succeeded")
            {
                return Err(Error::Conflict(
                    "publication committed; reconcile the move".into(),
                ));
            }
        }
        for expected in &movement.source.files {
            let current = fs::open_relative(&root, &expected.path)?;
            let actual = fs::identity_for_file(&current)?;
            if !expected.identity.same_object(&actual)
                || (!actual.directory && expected.identity != actual)
            {
                return Err(Error::Conflict("relocation source contents changed".into()));
            }
        }
        if movement.publication.is_some() {
            return Err(Error::Conflict(
                "relocation has a verified publication; reconcile it instead of abandoning".into(),
            ));
        }
        if source
            .hold
            .as_deref()
            .is_some_and(|h| !h.starts_with("review: interrupted move") && h != "review")
        {
            return Err(Error::Conflict("source has an independent hold".into()));
        }
        if movement.destination.symlink_metadata().is_ok() {
            self.discover_unlocked(
                movement.destination.parent().unwrap(),
                &movement.destination,
            )?;
        }
        op.state = "cancelled".into();
        op.error = Some(
            "operator abandoned relocation; original retained, scratch requires review".into(),
        );
        source.state = movement.source.state.clone();
        source.hold = None;
        source.error = None;
        source.revision += 1;
        source.files = fs::manifest(&root, 100_000)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        for suffix in ["direct", "copy"] {
            if let Ok(mut publication) =
                read::<Operation>(&tx, "operations", &format!("publish-{}-{suffix}", op.id))
            {
                if publication.state == "succeeded" {
                    return Err(Error::Conflict(
                        "publication committed before acknowledgement; reconcile the move".into(),
                    ));
                }
                publication.state = "cancelled".into();
                publication.error = op.error.clone();
                save_operation(&tx, &publication)?;
            }
        }
        if let Some(scratch) = movement.scratch {
            let mut retained = scratch;
            retained.state = "retained".into();
            retained.hold = Some("review: abandoned relocation staging".into());
            retained.revision += 1;
            save_artifact(&tx, &retained)?;
        }
        save_artifact(&tx, &source)?;
        save_operation(&tx, &op)?;
        event(&tx, &source.id, "relocation_abandoned", &op.id)?;
        tx.commit()?;
        Ok(source)
    }
}
