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
    registry: bool,
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
fn probe_publication(root: &Path, token: &str) -> Result<()> {
    boundary("capability")?;
    let a = root.join(format!(".runner-probe-{token}-a"));
    let b = root.join(format!(".runner-probe-{token}-b"));
    std::fs::create_dir(&a)?;
    let result = fs::rename_exclusive(&a, &b);
    // Remove only entries allocated by this probe. No recursive cleanup.
    let _ = std::fs::remove_dir(&a);
    if result.is_ok() {
        std::fs::remove_dir(&b)?;
    }
    result
}

/// Registry fallback is unreachable from ordinary production relocation until
/// all consumers have a separate managed mapping and native durability evidence.
/// The normal API always passes None. This policy is used by synthetic gates.
#[derive(Clone, Debug)]
pub struct RegistryPolicy {
    pub managed_root: PathBuf,
    pub consumers_isolated: bool,
}

impl Inventory {
    /// Journal before moving. A cross-volume move publishes a verified copy,
    /// then retires only the exact source entries captured by this operation.
    pub fn relocate(&self, job: u32, destination: &Path) -> Result<RelocationResult> {
        self.relocate_with_registry(job, destination, None)
    }

    pub fn relocate_with_registry(
        &self,
        job: u32,
        destination: &Path,
        policy: Option<&RegistryPolicy>,
    ) -> Result<RelocationResult> {
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
            "move-{}-{}-{:x}",
            source.id,
            source.generation,
            Sha256::digest(destination.as_os_str().as_encoded_bytes())
        );
        let mut relocation = Relocation {
            source: source.clone(),
            destination: destination.into(),
            requested_destination: Some(destination.into()),
            destination_root: fs::identity(&root_dir.metadata()?),
            scratch: None,
            registry: false,
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
            // Probe the actual destination before moving or allocating payload
            // scratch. Repeat each time so a remount cannot reuse stale evidence.
            let publication = probe_publication(root, &key);
            if let Err(error) = publication {
                let unsupported = matches!(&error, Error::Io(e) if matches!(e.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP)));
                let Some(policy) = policy.filter(|p| p.consumers_isolated && unsupported) else {
                    return Err(error);
                };
                fs::absolute(&policy.managed_root)?;
                if policy.managed_root.starts_with(&source.path)
                    || source.path.starts_with(&policy.managed_root)
                    || policy.managed_root.starts_with(root)
                    || root.starts_with(&policy.managed_root)
                {
                    return Err(Error::Conflict(
                        "registry root overlaps an ordinary scan root".into(),
                    ));
                }
                let managed = fs::open_dir(&policy.managed_root)?;
                relocation.registry = true;
                relocation.destination_root = fs::identity(&managed.metadata()?);
                relocation.destination = policy.managed_root.join(format!("generation-{key}"));
                op.request = serde_json::to_string(&relocation)?;
                save_operation(&self.db.lock().unwrap(), &op)?;
            }
            let root = relocation.destination.parent().unwrap();
            let root_dir = fs::open_dir(root)?;
            let destination = relocation.destination.as_path();
            boundary("before_move")?;
            let initial = if relocation.registry {
                Err(Error::Io(std::io::Error::from_raw_os_error(libc::EXDEV)))
            } else {
                fs::rename_exclusive(&source.path, destination)
            };
            match initial {
                Ok(()) => (),
                Err(Error::Io(e))
                    if e.kind() == std::io::ErrorKind::CrossesDevices
                        || e.raw_os_error() == Some(18) =>
                {
                    let _capacity = crate::capacity::reserve(root, source.summary().1)?;
                    let scratch_path = if relocation.registry {
                        destination.to_path_buf()
                    } else {
                        root.join(format!(".runner-{key}"))
                    };
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
                    save_operation(&self.db.lock().unwrap(), &op)?;
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
                        let bytes = std::io::copy(&mut input, &mut output)?;
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
                    save_operation(&self.db.lock().unwrap(), &op)?;
                    if !relocation.registry {
                        fs::rename_exclusive(&scratch_path, destination)?;
                    }
                }
                Err(e) => return Err(e),
            }
            let _guard = self.mutation_guard()?;
            self.commit_relocation(&mut op, &relocation)
        })();
        let guard = self.mutation_guard()?;
        if let Err(e) = &result {
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
        }
        drop(guard);
        result?;
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
        if !expected
            .identity
            .as_ref()
            .is_some_and(|i| i.same_object(&observed))
        {
            return Err(Error::Conflict("move publication identity changed".into()));
        }
        let files = fs::manifest(&target, 100_000)?;
        if files != expected.files {
            return Err(Error::Conflict("move publication contents changed".into()));
        }
        let mut current = self.get(&movement.source.id)?;
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
            let mut old = movement.source.clone();
            old.id = format!("source-{}", op.id);
            old.job = None;
            old.state = "retained".into();
            // Local fsync/registry commit is not the server's power-loss
            // contract. Source retirement needs separate evidence and receipts.
            old.hold = Some("review: publication durability and handoff".into());
            old.keep = current.keep;
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
                        request: serde_json::to_string(&(&old.id, old.revision, 0u64, false))?,
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
        if let Ok(old) = self.get(&format!("source-{key}")) {
            if old.owned && !old.keep && old.hold.is_none() && !old.terminal() {
                let op = self.request_delete(&old.id, old.revision, &format!("retire-{key}"), 0)?;
                self.execute_delete(&op.id)?;
            }
        }
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
            let movement: Relocation = serde_json::from_str(&op.request)?;
            if let Err(e) = self.commit_relocation(&mut op, &movement) {
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
        drop(guard);
        for key in retired {
            self.retire_relocation_source(&key)?;
        }
        Ok(())
    }
}
