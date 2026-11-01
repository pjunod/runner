//! Root-scoped startup recovery. Payload syscalls never own the coordinator.
use super::*;

#[derive(Serialize, Deserialize)]
struct OrphanReceipt {
    generation: String,
    previous_keep: bool,
    previous_hold: Option<String>,
    classified_revision: u64,
}

#[cfg(test)]
pub(super) static ROOT_OPEN_GATE: Mutex<
    Option<(
        PathBuf,
        std::sync::mpsc::Sender<()>,
        std::sync::Arc<(Mutex<bool>, std::sync::Condvar)>,
    )>,
> = Mutex::new(None);

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
        self.reconcile_deleted_artifacts()?;
        self.reconcile_relocations_root(root)?;
        self.reconcile_transforms_root(root)?;
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
            if !live {
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

    /// Reclaim only the automatic startup classification when a job is later
    /// imported/adopted. Never infer ownership or clear an operator's choice.
    pub fn reclaim_recovered_orphan(&self, job: u32) -> Result<Option<Artifact>> {
        let Some(original) = self.for_job(job)? else {
            return Ok(None);
        };
        if original.state != "retained"
            || original.hold.as_deref() != Some("review: allocation has no recovered job")
        {
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
