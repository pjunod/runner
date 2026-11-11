//! Root-scoped startup recovery. Payload syscalls never own the coordinator.
use super::*;

/// Immutable metadata boundary captured before the owner is exposed to
/// runtime callers. No payload manifests or filesystem handles are retained.
#[derive(Clone)]
pub struct StartupReconciliation {
    artifacts: std::collections::HashMap<String, StartupArtifact>,
    operations: std::collections::HashMap<String, [u8; 32]>,
    roots: Vec<PathBuf>,
}
#[derive(Clone)]
struct StartupArtifact {
    generation: String,
    revision: u64,
    root: PathBuf,
    path: PathBuf,
    state: String,
    job: Option<u32>,
}
impl StartupReconciliation {
    pub fn roots(&self) -> &[PathBuf] {
        &self.roots
    }

    pub(super) fn contains_artifact(&self, artifact: &Artifact) -> bool {
        self.artifacts.get(&artifact.id).is_some_and(|captured| {
            captured.generation == artifact.generation
                && captured.revision == artifact.revision
                && captured.root == artifact.root
                && captured.path == artifact.path
                && captured.state == artifact.state
                && captured.job == artifact.job
        })
    }

    pub(super) fn contains_operation(&self, operation: &Operation) -> Result<bool> {
        let Some(fingerprint) = self.operations.get(&operation.id) else {
            return Ok(false);
        };
        Ok(*fingerprint == operation_fingerprint(operation)?)
    }

    /// A new runtime journal can claim an existing allocation without changing
    /// its revision (for example, workspace admission). Such an allocation is
    /// also outside startup's authority, even though its generation is old.
    pub(super) fn allows_artifact(
        &self,
        inventory: &Inventory,
        artifact: &Artifact,
    ) -> Result<bool> {
        let db = inventory.db.lock().unwrap();
        self.allows_artifact_db(&db, artifact)
    }

    pub(super) fn allows_artifact_db(&self, db: &Connection, artifact: &Artifact) -> Result<bool> {
        if !self.contains_artifact(artifact) {
            return Ok(false);
        }
        let mut statement = db.prepare("SELECT data FROM operations WHERE artifact=?1")?;
        let mut rows = statement.query([&artifact.id])?;
        while let Some(row) = rows.next()? {
            let operation: Operation = serde_json::from_str(&row.get::<_, String>(0)?)?;
            if !self.contains_operation(&operation)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn operation_fingerprint(operation: &Operation) -> Result<[u8; 32]> {
    use sha2::{Digest, Sha256};
    Ok(Sha256::digest(serde_json::to_vec(operation)?).into())
}

#[derive(Serialize, Deserialize)]
struct OrphanReceipt {
    generation: String,
    previous_keep: bool,
    previous_hold: Option<String>,
    classified_revision: u64,
}

#[cfg(test)]
type RootOpenRelease = std::sync::Arc<(Mutex<bool>, std::sync::Condvar)>;
#[cfg(test)]
type RootOpenGate = Option<(PathBuf, std::sync::mpsc::Sender<()>, RootOpenRelease)>;

#[cfg(test)]
pub(super) static ROOT_OPEN_GATE: Mutex<RootOpenGate> = Mutex::new(None);

#[cfg(test)]
pub(super) fn before_root_open(root: &Path) {
    let gate = {
        let mut gate = ROOT_OPEN_GATE.lock().unwrap();
        if gate.as_ref().is_some_and(|(path, _, _)| path == root) {
            gate.take().map(|(_, arrival, release)| (arrival, release))
        } else {
            None
        }
    };
    if let Some((arrival, release)) = gate {
        let _ = arrival.send(());
        let mut released = release.0.lock().unwrap();
        while !*released {
            released = release.1.wait(released).unwrap();
        }
    }
}

/// Custody lasts until the actual blocking worker returns, including errors.
/// Every participant is reserved together before any payload is inspected.
pub(super) struct ReconciliationLease<'a> {
    inventory: &'a Inventory,
    keys: Vec<(String, String)>,
}
impl ReconciliationLease<'_> {
    /// Extend custody in the same short transaction that records freshly
    /// allocated scratch, before another worker can observe an unleased row.
    pub(super) fn extend_locked(
        &mut self,
        artifact: &Artifact,
        _coordinator: &std::sync::MutexGuard<'_, ()>,
    ) -> Result<()> {
        let current = self.inventory.get(&artifact.id)?;
        if current.generation != artifact.generation
            || current.path != artifact.path
            || current.root != artifact.root
            || current.revision != artifact.revision
        {
            return Err(Error::Conflict(
                "scratch custody changed before reservation".into(),
            ));
        }
        let key = (artifact.id.clone(), artifact.generation.clone());
        let mut uses = self.inventory.reconciliation_uses.lock().unwrap();
        if !self.keys.contains(&key) {
            if !uses.insert(key.clone()) {
                return Err(Error::Conflict("scratch custody is already active".into()));
            }
            self.keys.push(key);
        }
        Ok(())
    }
}
impl Drop for ReconciliationLease<'_> {
    fn drop(&mut self) {
        let _coordinator = self.inventory.mutation.lock().unwrap();
        let mut uses = self.inventory.reconciliation_uses.lock().unwrap();
        for key in &self.keys {
            uses.remove(key);
        }
        self.inventory.reconciliation_released.notify_all();
    }
}

impl Inventory {
    pub(super) fn reconciliation_in_use(&self, artifact: &Artifact) -> bool {
        self.reconciliation_uses
            .lock()
            .unwrap()
            .contains(&(artifact.id.clone(), artifact.generation.clone()))
    }

    pub(super) fn reserve_reconciliation(
        &self,
        artifacts: &[Artifact],
    ) -> Result<Option<ReconciliationLease<'_>>> {
        let coordinator = self.mutation_guard()?;
        self.reserve_reconciliation_locked(artifacts, None, &coordinator)
    }

    /// The caller may already own one processing lease. It remains live while
    /// this extra reservation fences runtime namespace mutation during I/O.
    pub(super) fn reserve_reconciliation_locked(
        &self,
        artifacts: &[Artifact],
        authority: Option<&AttemptUse>,
        _coordinator: &std::sync::MutexGuard<'_, ()>,
    ) -> Result<Option<ReconciliationLease<'_>>> {
        self.reserve_reconciliation_inner(artifacts, authority, false)
    }

    /// Runtime transform/relocation APIs already operate within the single
    /// admitted processing attempt. Its AttemptUse remains live throughout;
    /// this additional exclusive namespace lease serializes its stages with
    /// reconciliation/deletion without holding the global coordinator.
    pub(super) fn reserve_processing_mutation(
        &self,
        artifacts: &[Artifact],
    ) -> Result<Option<ReconciliationLease<'_>>> {
        let _coordinator = self.mutation_guard()?;
        self.reserve_reconciliation_inner(artifacts, None, true)
    }

    fn reserve_reconciliation_inner(
        &self,
        artifacts: &[Artifact],
        authority: Option<&AttemptUse>,
        processing: bool,
    ) -> Result<Option<ReconciliationLease<'_>>> {
        let mut keys = Vec::new();
        for artifact in artifacts {
            let current = self.get(&artifact.id)?;
            if current.generation != artifact.generation
                || current.path != artifact.path
                || current.root != artifact.root
                || current.revision != artifact.revision
                || self.reconciliation_in_use(&current)
                || current
                    .job
                    .is_some_and(|job| self.allocation_uses.lock().unwrap().contains(&job))
                || (!processing
                    && self.artifact_in_use(&current)?
                    && !self.authorized_processing_use(&current, authority))
            {
                return Ok(None);
            }
            let key = (artifact.id.clone(), artifact.generation.clone());
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        self.reconciliation_uses
            .lock()
            .unwrap()
            .extend(keys.iter().cloned());
        Ok(Some(ReconciliationLease {
            inventory: self,
            keys,
        }))
    }

    pub(super) fn save_reconciliation_operation(&self, operation: &Operation) -> Result<()> {
        let _coordinator = self.mutation_guard()?;
        let saved = self.operation(&operation.id)?;
        if matches!(saved.state.as_str(), "succeeded" | "cancelled") {
            return Err(Error::Conflict(
                "reconciliation journal no longer active".into(),
            ));
        }
        save_operation(&self.db.lock().unwrap(), operation)
    }

    pub(super) fn check_reconciliation_source(
        &self,
        operation: &Operation,
        source: &Artifact,
    ) -> Result<()> {
        let current = self.get(&source.id)?;
        let saved = self.operation(&operation.id)?;
        if current.generation != source.generation
            || current.path != source.path
            || current.root != source.root
            || current.revision != source.revision
            || saved.request != operation.request
            || saved.state != operation.state
        {
            return Err(Error::Conflict(
                "reconciliation custody changed while inspecting payload".into(),
            ));
        }
        Ok(())
    }

    /// Capture the startup inventory while only this owner can admit work.
    /// The coordinator and one SQLite read transaction give artifacts and
    /// journals the same cutoff; no root is opened or probed here.
    pub fn capture_startup_reconciliation(&self) -> Result<StartupReconciliation> {
        let _coordinator = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let mut artifacts = std::collections::HashMap::new();
        let mut roots = Vec::new();
        {
            let mut statement = tx.prepare("SELECT id,json_extract(data,'$.generation'),json_extract(data,'$.revision'),json_extract(data,'$.root'),path,state,json_extract(data,'$.job') FROM artifacts")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let root = PathBuf::from(row.get::<_, String>(3)?);
                roots.push(root.clone());
                artifacts.insert(
                    row.get::<_, String>(0)?,
                    StartupArtifact {
                        generation: row.get(1)?,
                        revision: u64::try_from(row.get::<_, i64>(2)?).map_err(|_| {
                            Error::Conflict("invalid startup artifact revision".into())
                        })?,
                        root,
                        path: PathBuf::from(row.get::<_, String>(4)?),
                        state: row.get(5)?,
                        job: row.get(6)?,
                    },
                );
            }
        }
        let mut operations = std::collections::HashMap::new();
        {
            let mut statement = tx.prepare("SELECT data FROM operations")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let operation: Operation = serde_json::from_str(&row.get::<_, String>(0)?)?;
                operations.insert(operation.id.clone(), operation_fingerprint(&operation)?);
            }
        }
        tx.commit()?;
        roots.sort();
        roots.dedup();
        Ok(StartupReconciliation {
            artifacts,
            operations,
            roots,
        })
    }

    /// Metadata-only enumeration: an unavailable mount cannot delay startup.
    pub fn startup_reconciliation_roots(&self) -> Result<Vec<PathBuf>> {
        let db = self.db.lock().unwrap();
        let mut statement = db.prepare("SELECT DISTINCT json_extract(data,'$.root') FROM artifacts ORDER BY json_extract(data,'$.root')")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|root| Ok(PathBuf::from(root?))).collect()
    }

    /// Run in the bounded worker for this root before admitting its recovered
    /// jobs. Other roots and metadata commits never wait on its payload I/O.
    pub fn reconcile_startup_root(&self, root: &Path, live_jobs: &[u32]) -> Result<()> {
        let snapshot = self.capture_startup_reconciliation()?;
        self.reconcile_startup_root_snapshot(root, live_jobs, &snapshot)
    }

    /// Replay only the inventory that existed at the owner's synchronous
    /// startup boundary, even if this root's worker runs much later.
    pub fn reconcile_startup_root_snapshot(
        &self,
        root: &Path,
        live_jobs: &[u32],
        snapshot: &StartupReconciliation,
    ) -> Result<()> {
        self.reconcile_deleted_artifacts_snapshot(Some(snapshot))?;
        self.park_legacy_startup_orphans(root)?;
        self.reconcile_relocations_root_snapshot(root, Some(snapshot))?;
        self.reconcile_transforms_root_snapshot(root, Some(snapshot))?;
        let rows = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare("SELECT data FROM artifacts WHERE state IN ('allocating','retiring','active') AND json_extract(data,'$.root')=?1")?;
            let rows = statement.query_map([root.to_string_lossy().as_ref()], |row| {
                row.get::<_, String>(0)
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in rows {
            let original: Artifact = serde_json::from_str(&raw)?;
            if !snapshot.allows_artifact(self, &original)? {
                continue;
            }
            if self.is_transform_scratch(&original)? {
                continue;
            }
            let live = original.job.is_some_and(|job| live_jobs.contains(&job));
            if original.state == "active" && live {
                continue;
            }
            let Some(_lease) = self.reserve_reconciliation(std::slice::from_ref(&original))? else {
                continue;
            };
            // Recheck after reserving: a runtime workspace may have been
            // admitted between the metadata query and this custody lease.
            if !snapshot.allows_artifact(self, &original)? {
                continue;
            }
            let mut artifact = original.clone();
            if self.verify_root(&artifact).is_err() {
                continue;
            }
            if artifact.state == "allocating" {
                if !artifact.path.try_exists()? && live {
                    std::fs::create_dir(&artifact.path)?;
                    fs::sync_directory(&fs::open_dir(&artifact.root)?)?;
                }
                if !artifact.path.try_exists()? {
                    artifact.state = "source_gone".into();
                } else {
                    // mkdir without a committed sidecar cannot prove ownership.
                    artifact.identity =
                        Some(fs::identity_for_file(&fs::open_dir(&artifact.path)?)?);
                    artifact.owned = false;
                    artifact.keep = true;
                    artifact.hold = Some("review".into());
                    artifact.state = if live { "active" } else { "retained" }.into();
                }
            } else if let Ok(dir) = self.verify(&artifact) {
                artifact.files = fs::manifest(&dir, 100_000)?;
                artifact.inspected_at = Some(now());
                artifact.state = "retained".into();
                // Interrupted journal holds survive orphan classification.
                if artifact.hold.is_none()
                    || artifact.hold.as_deref() == Some("waiting for writer stop")
                {
                    artifact.hold = if artifact.owned {
                        None
                    } else {
                        Some("review".into())
                    };
                }
            } else {
                artifact.hold = Some("review: interrupted writer retirement".into());
            }
            let orphan_receipt = (!live).then(|| OrphanReceipt {
                generation: artifact.generation.clone(),
                previous_keep: artifact.keep,
                previous_hold: artifact.hold.clone(),
                classified_revision: original.revision + 1,
            });
            if !live && artifact.state == "retained" && artifact.owned {
                // W9: an owned allocation whose job is gone is a failed
                // source with the failed-payload TTL, not a review state.
                // The durable binding stays, so an import/adoption that
                // arrives later reclaims it (`reclaim_recovered_orphan`).
                // No-wedge: closed scope — an owned allocation is Runner's
                // own download whenever it was made, so pre-eligibility rows
                // park too; a person's Keep still retains it, and a hold it
                // already had keeps its own registered exit.
                let hold = artifact.hold.take();
                Self::park_orphan(&mut artifact, self.settings()?.failed_retention_days);
                artifact.automatic_cleanup_eligible = true;
                artifact.hold = hold;
            } else if !live && artifact.owned {
                // Not retained (an interrupted writer retirement): its own
                // hold's registered exit reclassifies it.
            } else if !live {
                // Imports/adoptions may arrive while this payload worker is
                // running. Retain the durable binding so a later owner can
                // still locate a nondefault root without guessing its path.
                artifact.keep = true;
                artifact.hold = Some("review: allocation has no recovered job".into());
            }
            let _coordinator = self.mutation_guard()?;
            let current = self.get(&original.id)?;
            if current.generation != original.generation
                || current.path != original.path
                || current.root != original.root
                || current.revision != original.revision
            {
                continue;
            }
            artifact.revision += 1;
            artifact.updated_at = now();
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            if let Some(receipt) = orphan_receipt {
                save_operation(
                    &tx,
                    &Operation {
                        id: format!("startup-orphan-{}-{}", artifact.id, artifact.generation),
                        artifact: artifact.id.clone(),
                        kind: "startup_orphan".into(),
                        state: "retained".into(),
                        request: serde_json::to_string(&receipt)?,
                        created_at: now(),
                        not_before: 0,
                        attempts: 0,
                        next_retry: 0,
                        error: None,
                    },
                )?;
            }
            save_artifact(&tx, &artifact)?;
            tx.commit()?;
        }
        Ok(())
    }

    /// A startup orphan's failed-payload retention (W9).
    fn park_orphan(artifact: &mut Artifact, retention_days: u32) {
        artifact.state = "parked_failed".into();
        artifact.hold = None;
        artifact.retention_seconds = u64::from(retention_days) * 86400;
        artifact.deadline = Some(now().saturating_add(artifact.retention_seconds as i64));
    }

    /// W9 upgrade: an owned orphan an older build classified as a review
    /// hold (`retained`, Keep, "allocation has no recovered job") with no
    /// operator change since becomes a parked failure with the TTL.
    /// Idempotent: a converted row no longer carries the hold.
    pub(super) fn park_legacy_startup_orphans(&self, root: &Path) -> Result<usize> {
        let retention_days = self.settings()?.failed_retention_days;
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let rows = {
            let mut statement = tx.prepare("SELECT a.data,o.data FROM artifacts a JOIN operations o ON o.id='startup-orphan-'||a.id||'-'||json_extract(a.data,'$.generation') WHERE a.state='retained' AND json_extract(a.data,'$.hold')='review: allocation has no recovered job' AND json_extract(a.data,'$.root')=?1 AND o.state='retained'")?;
            let rows = statement.query_map([root.to_string_lossy().as_ref()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let mut parked = 0;
        for (raw, op) in rows {
            let mut artifact: Artifact = serde_json::from_str(&raw)?;
            let operation: Operation = serde_json::from_str(&op)?;
            let receipt: OrphanReceipt = serde_json::from_str(&operation.request)?;
            if !artifact.owned || receipt.generation != artifact.generation {
                continue;
            }
            // No-wedge: closed scope. Keep is what it was before the
            // classification pinned it (a later person's change wins), and a
            // hold it had before keeps its own registered exit.
            if artifact.revision == receipt.classified_revision {
                artifact.keep = receipt.previous_keep;
            }
            Self::park_orphan(&mut artifact, retention_days);
            artifact.automatic_cleanup_eligible = true;
            artifact.hold = receipt.previous_hold.clone();
            artifact.revision += 1;
            artifact.updated_at = now();
            save_artifact(&tx, &artifact)?;
            event(&tx, &artifact.id, "retention", "startup orphan parked")?;
            parked += 1;
        }
        tx.commit()?;
        Ok(parked)
    }

    /// Reclaim only the automatic startup classification when a job is later
    /// imported/adopted. Never infer ownership or clear an operator's choice.
    pub fn reclaim_recovered_orphan(&self, job: u32) -> Result<Option<Artifact>> {
        let Some(original) = self.for_job(job)? else {
            return Ok(None);
        };
        let reviewed = original.state == "retained"
            && original.hold.as_deref() == Some("review: allocation has no recovered job");
        let parked = original.state == "parked_failed" && original.hold.is_none();
        if !reviewed && !parked {
            return Ok(Some(original));
        }
        let key = format!("startup-orphan-{}-{}", original.id, original.generation);
        let Ok(mut operation) = self.operation(&key) else {
            return Ok(Some(original));
        };
        let receipt: OrphanReceipt = serde_json::from_str(&operation.request)?;
        if operation.kind != "startup_orphan"
            || operation.state != "retained"
            || receipt.generation != original.generation
        {
            return Ok(Some(original));
        }
        let _lease = self
            .reserve_reconciliation(std::slice::from_ref(&original))?
            .ok_or_else(|| {
                Error::Conflict("orphan reconciliation custody is already active".into())
            })?;
        self.verify(&original)?;
        let _coordinator = self.mutation_guard()?;
        self.check_reconciliation_source(&operation, &original)?;
        let mut artifact = self.get(&original.id)?;
        artifact.state = "active".into();
        artifact.hold = receipt.previous_hold;
        artifact.retention_seconds = 0;
        artifact.deadline = None;
        // Retention controls made after classification remain authoritative.
        if artifact.revision == receipt.classified_revision {
            artifact.keep = receipt.previous_keep;
        }
        artifact.revision += 1;
        artifact.updated_at = now();
        operation.state = "succeeded".into();
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &artifact)?;
        save_operation(&tx, &operation)?;
        tx.commit()?;
        Ok(Some(artifact))
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    fn inventory() -> (tempfile::TempDir, Inventory, PathBuf) {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("payload");
        std::fs::create_dir(&root).unwrap();
        let inventory = Inventory::open(&temporary.path().join("state")).unwrap();
        (temporary, inventory, root)
    }

    /// W9: an owned allocation with no recovered job is a parked failure
    /// with the failed-payload TTL, not a review hold; a later import
    /// reclaims it. An older build's review classification converts once.
    #[test]
    fn startup_orphan_is_parked_with_a_ttl_and_legacy_review_converts() {
        let (_temporary, inventory, root) = inventory();
        let path = root.join("orphan");
        inventory
            .allocate_for_delivery(91, &root, &path, true, true)
            .unwrap();
        std::fs::write(path.join("media.mkv"), b"bytes").unwrap();
        inventory.finish(91, &path, &root, "active").unwrap();
        let snapshot = inventory.capture_startup_reconciliation().unwrap();
        inventory
            .reconcile_startup_root_snapshot(&root, &[], &snapshot)
            .unwrap();
        let parked = inventory.for_job(91).unwrap().unwrap();
        assert_eq!(parked.state, "parked_failed");
        assert!(!parked.keep && parked.hold.is_none());
        assert!(parked.deadline.is_some_and(|deadline| deadline > now()));
        assert!(parked.earliest_expiry().is_some(), "expires on its own");
        let reclaimed = inventory.reclaim_recovered_orphan(91).unwrap().unwrap();
        assert_eq!(reclaimed.state, "active");
        assert!(reclaimed.deadline.is_none());

        // Legacy shape: what 0.8 wrote for the same orphan.
        let key = format!("startup-orphan-{}-{}", parked.id, parked.generation);
        let mut legacy = inventory.get(&parked.id).unwrap();
        legacy.state = "retained".into();
        legacy.keep = true;
        legacy.hold = Some("review: allocation has no recovered job".into());
        let mut operation = inventory.operation(&key).unwrap();
        operation.state = "retained".into();
        let mut receipt: OrphanReceipt = serde_json::from_str(&operation.request).unwrap();
        receipt.classified_revision = legacy.revision;
        operation.request = serde_json::to_string(&receipt).unwrap();
        {
            let db = inventory.db.lock().unwrap();
            save_artifact(&db, &legacy).unwrap();
            save_operation(&db, &operation).unwrap();
        }
        assert_eq!(inventory.park_legacy_startup_orphans(&root).unwrap(), 1);
        assert_eq!(inventory.park_legacy_startup_orphans(&root).unwrap(), 0);
        let converted = inventory.get(&parked.id).unwrap();
        assert_eq!(converted.state, "parked_failed");
        assert!(!converted.keep && converted.hold.is_none());
    }

    #[test]
    fn startup_snapshot_does_not_claim_new_runtime_allocation_or_finished_workspace() {
        let (_temporary, inventory, root) = inventory();
        let snapshot = inventory.capture_startup_reconciliation().unwrap();
        let source = inventory
            .allocate(77, &root, &root.join("runtime"))
            .unwrap();
        std::fs::write(source.path.join("payload"), b"owned").unwrap();
        let workspace = inventory.workspace(77, "par_repair", "abc").unwrap();
        std::fs::write(workspace.scratch.path.join("temporary"), b"scratch").unwrap();
        inventory.finish_workspace(&workspace).unwrap();
        let source_before = inventory.get(&source.id).unwrap();
        let scratch_before = inventory.get(&workspace.scratch.id).unwrap();
        let operation_before = inventory.operation(&workspace.operation_id).unwrap();

        inventory
            .reconcile_startup_root_snapshot(&root, &[], &snapshot)
            .unwrap();

        assert_eq!(
            serde_json::to_value(inventory.get(&source.id).unwrap()).unwrap(),
            serde_json::to_value(source_before).unwrap()
        );
        assert_eq!(
            serde_json::to_value(inventory.get(&workspace.scratch.id).unwrap()).unwrap(),
            serde_json::to_value(scratch_before).unwrap()
        );
        assert_eq!(
            serde_json::to_value(inventory.operation(&workspace.operation_id).unwrap()).unwrap(),
            serde_json::to_value(operation_before).unwrap()
        );
        assert!(!inventory.for_job(77).unwrap().unwrap().keep);
        assert!(inventory.for_job(77).unwrap().unwrap().hold.is_none());
    }

    #[test]
    fn startup_snapshot_excludes_new_runtime_journal_on_existing_generation() {
        let (_temporary, inventory, root) = inventory();
        let source = inventory
            .allocate(78, &root, &root.join("existing"))
            .unwrap();
        std::fs::write(source.path.join("payload"), b"owned").unwrap();
        let snapshot = inventory.capture_startup_reconciliation().unwrap();
        let workspace = inventory.workspace(78, "par_repair", "def").unwrap();
        let source_before = inventory.get(&source.id).unwrap();
        let operation_before = inventory.operation(&workspace.operation_id).unwrap();

        inventory
            .reconcile_startup_root_snapshot(&root, &[], &snapshot)
            .unwrap();

        assert_eq!(
            serde_json::to_value(inventory.get(&source.id).unwrap()).unwrap(),
            serde_json::to_value(source_before).unwrap()
        );
        assert_eq!(
            serde_json::to_value(inventory.operation(&workspace.operation_id).unwrap()).unwrap(),
            serde_json::to_value(operation_before).unwrap()
        );
        assert!(workspace.scratch.path.exists());
    }
}
