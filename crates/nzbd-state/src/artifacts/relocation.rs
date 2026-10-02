use super::*;
use sha2::{Digest, Sha256};
use std::io::{Read, Seek};

#[derive(Serialize, Deserialize)]
struct Relocation {
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelocationResult {
    pub operation_id: String,
    pub artifact_id: String,
    pub generation: String,
    pub published_path: PathBuf,
}

#[cfg(test)]
thread_local! { pub(super) static FAULTS: std::cell::RefCell<std::collections::HashMap<&'static str, i32>> = Default::default(); }
fn boundary(stage: &'static str) -> Result<()> {
    #[cfg(test)]
    if let Some(code) = FAULTS.with(|f| f.borrow_mut().remove(stage)) {
        return Err(std::io::Error::from_raw_os_error(code).into());
    }
    let _ = stage;
    Ok(())
}
impl Inventory {
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
        Ok((matches!(persisted.state.as_str(), "running" | "review")
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
            .then_some(current))
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
        let guard = self.mutation_guard()?;
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
            destination_root: fs::identity(&root_dir.metadata()?),
            scratch: None,
            registry: false,
            publication: None,
            publication_key: None,
            cleanup_done: false,
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
            boundary("before_move")?;
            let direct_key = format!("{key}-direct");
            let initial = {
                let _guard = self.mutation_guard()?;
                self.relocation_source(&op, &relocation)?
                    .ok_or_else(|| Error::Conflict("relocation authority revoked".into()))?;
                self.verify(&relocation.source)?;
                self.publish_directory_unlocked(&source.path, &destination, &source.id, &direct_key)
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
                    scratch.identity = Some(fs::identity(&scratch_dir.metadata()?));
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
                    for entry in &source.files {
                        let target = scratch_path.join(&entry.path);
                        if entry.identity.directory {
                            std::fs::create_dir(&target)?;
                            continue;
                        }
                        let mut input = fs::open_relative(&dir, &entry.path)?;
                        if fs::identity(&input.metadata()?) != entry.identity {
                            return Err(Error::Conflict("source changed during relocation".into()));
                        }
                        let mut output = std::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(&target)?;
                        // Network mounts may reject copy_file_range; use the
                        // portable read/write path for the verified copy.
                        let mut bytes = 0;
                        let mut buffer = vec![0u8; 1024 * 1024];
                        loop {
                            let n = input.read(&mut buffer)?;
                            if n == 0 {
                                break;
                            }
                            output.write_all(&buffer[..n])?;
                            bytes += n as u64;
                        }
                        output.sync_all()?;
                        input.rewind()?;
                        let hash = |f: &mut File| -> Result<Vec<u8>> {
                            let mut h = Sha256::new();
                            let mut b = [0u8; 1024 * 1024];
                            loop {
                                let n = f.read(&mut b)?;
                                if n == 0 {
                                    break;
                                }
                                h.update(&b[..n]);
                            }
                            Ok(h.finalize().to_vec())
                        };
                        if bytes != entry.identity.bytes
                            || hash(&mut input)? != hash(&mut File::open(&target)?)?
                            || fs::identity(&input.metadata()?) != entry.identity
                        {
                            return Err(Error::Conflict(
                                "relocation copy verification failed".into(),
                            ));
                        }
                    }
                    // Every nested directory entry must be durable before publication.
                    for entry in source.files.iter().rev().filter(|e| e.identity.directory) {
                        fs::sync_directory(&fs::open_relative(&scratch_dir, &entry.path)?)?;
                    }
                    fs::sync_directory(&scratch_dir)?;
                    scratch.files = fs::manifest(&scratch_dir, 100_000)?;
                    relocation.scratch = Some(scratch.clone());
                    op.request = serde_json::to_string(&relocation)?;
                    save_artifact(&self.db.lock().unwrap(), &scratch)?;
                    self.save_relocation_progress(&op, &relocation)?;
                    let copy_key = format!("{key}-copy");
                    let published = {
                        let _guard = self.mutation_guard()?;
                        self.relocation_source(&op, &relocation)?.ok_or_else(|| {
                            Error::Conflict("relocation authority revoked".into())
                        })?;
                        self.verify(&scratch)?;
                        self.publish_directory_unlocked(
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
            self.commit_relocation(&mut op, &relocation)
        })();
        let guard = self.mutation_guard()?;
        if let Err(e) = result {
            if self.relocation_source(&op, &relocation)?.is_none() {
                self.cancel_obsolete_relocation(&mut op)?;
                return Err(e);
            }
            op.state = "review".into();
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
        if let Err(e) = self.retire_relocation_source(&op.id) {
            tracing::warn!(operation=%op.id, error=%e, "relocation committed; source retirement pending");
        }
        let published = self.get(&source.id)?;
        Ok(RelocationResult {
            operation_id: op.id,
            artifact_id: published.id,
            generation: published.generation,
            published_path: published.path,
        })
    }
    fn commit_relocation(&self, op: &mut Operation, movement: &Relocation) -> Result<()> {
        let mut current = self.relocation_source(op, movement)?.ok_or_else(|| {
            Error::Conflict("relocation no longer owns the source generation".into())
        })?;
        let root = movement.destination.parent().unwrap();
        let root_dir = fs::open_dir(root)?;
        if !movement
            .destination_root
            .same_object(&fs::identity(&root_dir.metadata()?))
        {
            return Err(Error::Conflict("move destination root changed".into()));
        }
        boundary("registry_commit")?;
        let target = fs::open_dir(&movement.destination)?;
        let expected = movement.scratch.as_ref().unwrap_or(&movement.source);
        let observed = fs::identity(&target.metadata()?);
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
        current.path = movement.destination.clone();
        current.root = root.into();
        current.root_identity = movement.destination_root.clone();
        current.identity = Some(fs::identity(&target.metadata()?));
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
            if old.owned && !old.keep && old.hold.is_none() {
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
        if movement.scratch.is_some() {
            if let Some(publication_key) = &movement.publication_key {
                let _guard = self.mutation_guard()?;
                self.retire_publication_source_unlocked(publication_key)?;
            }
        }
        if let Ok(old) = self.get(&format!("source-{key}")) {
            if old.owned && !old.keep && old.hold.is_none() && !old.terminal() {
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
        movement.cleanup_done = true;
        op.request = serde_json::to_string(&movement)?;
        save_operation(&self.db.lock().unwrap(), &op)?;
        Ok(())
    }
    pub fn reconcile_relocations(&self) -> Result<()> {
        let guard = self.mutation_guard()?;
        let raws = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare("SELECT data FROM operations WHERE state IN ('running','review') AND json_extract(data,'$.kind')='relocate' LIMIT 25")?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
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
                            &source_path,
                            &movement.destination,
                            &op.artifact,
                            &publication_key,
                        )?);
                        movement.publication_key = Some(publication_key);
                        op.request = serde_json::to_string(&movement)?;
                        save_operation(&self.db.lock().unwrap(), &op)?;
                    }
                }
                self.commit_relocation(&mut op, &movement)
            })();
            if let Err(e) = recovered {
                op.state = "review".into();
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
            } else {
                retired.push(op.id);
            }
        }
        {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare("SELECT id FROM operations WHERE state='succeeded' AND json_extract(data,'$.kind')='relocate' AND COALESCE(json_extract(json_extract(data,'$.request'),'$.cleanup_done'),0)=0 LIMIT 25")?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            retired.extend(rows.collect::<std::result::Result<Vec<_>, _>>()?);
        }
        drop(guard);
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
        let mut stmt = db.prepare("SELECT data FROM operations WHERE artifact=?1 AND state IN ('running','review') AND json_extract(data,'$.kind')='relocate'")?;
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
        if op.kind != "relocate" || !matches!(op.state.as_str(), "running" | "review") {
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
            let actual = fs::identity(&current.metadata()?);
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
