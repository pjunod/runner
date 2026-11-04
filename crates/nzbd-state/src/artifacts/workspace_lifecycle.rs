//! Attempt custody and internal scratch retirement; independent of payload expiry.
use super::*;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceGeneration {
    pub artifact: String,
    pub generation: String,
}

/// Metadata authority to revisit one already-requested failed deletion.
/// This is not evidence that any payload bytes remain valid or readable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailedCleanupProof {
    job: u32,
    source: SourceGeneration,
    root: PathBuf,
    path: PathBuf,
    operation: String,
    request: String,
}
impl FailedCleanupProof {
    pub fn job(&self) -> u32 {
        self.job
    }
    pub fn source(&self) -> &SourceGeneration {
        &self.source
    }
    pub fn operation_id(&self) -> &str {
        &self.operation
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Clones share one registered use, which remains live until the last actual
/// worker exits. Dropping an async join handle does not release worker custody.
#[derive(Clone)]
pub struct AttemptUse(Arc<UseLease>);
struct UseLease {
    inventory: Arc<Inventory>,
    source: SourceGeneration,
}
impl AttemptUse {
    pub fn source(&self) -> &SourceGeneration {
        &self.0.source
    }
}
impl Drop for UseLease {
    fn drop(&mut self) {
        let _coordinator = self.inventory.mutation.lock().unwrap();
        self.inventory
            .workspace_uses
            .lock()
            .unwrap()
            .remove(&(self.source.artifact.clone(), self.source.generation.clone()));
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FinalizationProof {
    Local {
        history_seq: u64,
    },
    Cluster {
        receipt: String,
        accepted_at_ms: i64,
    },
}
impl FinalizationProof {
    fn valid(&self) -> bool {
        match self {
            Self::Local { history_seq } => *history_seq > 0,
            Self::Cluster {
                receipt,
                accepted_at_ms,
            } => !receipt.is_empty() && *accepted_at_ms > 0,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct RetirementReceipt {
    source: SourceGeneration,
    transform: String,
    scratch: SourceGeneration,
    scratch_revision: u64,
    outcome: String,
    proof: FinalizationProof,
}
#[derive(Debug, Serialize)]
pub struct WorkspaceAssessment {
    pub operation: String,
    pub source: SourceGeneration,
    pub scratch: SourceGeneration,
    pub source_state: String,
    pub transform_state: String,
    pub hold: Option<String>,
    pub keep: bool,
    pub identity_valid: bool,
    pub bytes: u64,
    pub reasons: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct StagedCompletion {
    pub source: SourceGeneration,
    pub outcome: String,
    pub proof: FinalizationProof,
}

impl Inventory {
    /// Inspect only durable cleanup metadata. In particular, a partially
    /// deleted generation must never pass through ordinary payload recovery.
    pub fn failed_cleanup_proof(&self, job: u32) -> Result<Option<FailedCleanupProof>> {
        let _coordinator = self.mutation_guard()?;
        let Some(artifact) = self.for_job(job)? else {
            return Ok(None);
        };
        let operation = match self.operation(&format!("failed-delete-{}", artifact.id)) {
            Ok(operation) => operation,
            Err(Error::NotFound) => return Ok(None),
            Err(error) => return Err(error),
        };
        let proof = FailedCleanupProof {
            job,
            source: SourceGeneration {
                artifact: artifact.id,
                generation: artifact.generation,
            },
            root: artifact.root,
            path: artifact.path,
            operation: operation.id,
            request: operation.request,
        };
        self.failed_cleanup_binding(&proof)?;
        Ok(Some(proof))
    }

    /// Revalidate the immutable operation binding without granting payload
    /// export, processing custody, or permission to create a deletion request.
    pub fn validate_failed_cleanup_proof(&self, proof: &FailedCleanupProof) -> Result<()> {
        let _coordinator = self.mutation_guard()?;
        self.failed_cleanup_binding(proof).map(|_| ())
    }

    // Caller holds the mutation coordinator. Retry state/deadlines may evolve;
    // the selected job, generation, namespace and deletion request may not.
    fn failed_cleanup_binding(&self, proof: &FailedCleanupProof) -> Result<(Artifact, Operation)> {
        let artifact = self.for_job(proof.job)?.ok_or(Error::NotFound)?;
        let operation = self.operation(&proof.operation)?;
        let request: DeleteRequest = serde_json::from_str(&operation.request)?;
        let exact_request = matches!(request,
            DeleteRequest::Scoped((ref id, _, 0, false, ref generation))
                if id == &artifact.id && generation == &artifact.generation);
        if artifact.id != proof.source.artifact
            || artifact.generation != proof.source.generation
            || artifact.job != Some(proof.job)
            || artifact.root != proof.root
            || artifact.path != proof.path
            || !artifact.owned
            || !matches!(
                artifact.state.as_str(),
                "retained" | "deleting" | "delete_failed" | "deleted" | "source_gone"
            )
            || operation.id != format!("failed-delete-{}", artifact.id)
            || operation.artifact != artifact.id
            || operation.kind != "delete"
            || operation.request != proof.request
            || !matches!(
                operation.state.as_str(),
                "queued" | "running" | "retry" | "succeeded" | "cancelled" | "review"
            )
            || !exact_request
        {
            return Err(Error::Conflict(
                "failed cleanup source or deletion binding changed".into(),
            ));
        }
        Ok((artifact, operation))
    }

    /// Resume only the proven journal. The private reservation prevents other
    /// workers from acquiring this generation until the actual executor exits.
    /// All deletion policy, deadline and filesystem checks stay in that executor.
    pub fn replay_failed_cleanup(
        self: &Arc<Self>,
        proof: &FailedCleanupProof,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Operation> {
        checkpoint()?;
        let custody = {
            let _coordinator = self.mutation_guard()?;
            checkpoint()?;
            let (artifact, operation) = self.failed_cleanup_binding(proof)?;
            if matches!(
                operation.state.as_str(),
                "succeeded" | "cancelled" | "review"
            ) || operation.not_before > now()
                || operation.next_retry > now()
            {
                return Ok(operation);
            }
            if self.artifact_in_use(&artifact)? {
                return Err(std::io::Error::from(std::io::ErrorKind::WouldBlock).into());
            }
            self.workspace_uses
                .lock()
                .unwrap()
                .insert((artifact.id.clone(), artifact.generation.clone()), 1);
            AttemptUse(Arc::new(UseLease {
                inventory: self.clone(),
                source: proof.source.clone(),
            }))
        };
        let check = || {
            checkpoint()?;
            // execute_delete_with_use first calls this under its coordinator:
            // a policy edit between reservation and execution cannot substitute
            // another request. Later checks can only refuse the same operation.
            self.failed_cleanup_binding(proof)
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            Ok(())
        };
        self.execute_delete_with_use(&proof.operation, Some((&custody, &check)))
    }

    pub fn acquire_attempt_use(self: &Arc<Self>, job: u32) -> Result<Option<AttemptUse>> {
        self.acquire_attempt_use_checked(job, &|| Ok(()))
    }
    pub fn acquire_attempt_use_checked(
        self: &Arc<Self>,
        job: u32,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Option<AttemptUse>> {
        self.acquire_use(job, checkpoint, true)
    }
    pub fn acquire_terminal_use(
        self: &Arc<Self>,
        job: u32,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Option<AttemptUse>> {
        self.acquire_use(job, checkpoint, false)
    }
    fn acquire_use(
        self: &Arc<Self>,
        job: u32,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
        resume: bool,
    ) -> Result<Option<AttemptUse>> {
        checkpoint()?;
        // Reserve the exact generation under the short coordinator. The
        // reservation fences namespace mutation while root verification runs
        // without holding a global lock across any payload syscall.
        let artifact = {
            let _coordinator = self.mutation_guard()?;
            let Some(artifact) = self.for_job(job)? else {
                return Ok(None);
            };
            let key = (artifact.id.clone(), artifact.generation.clone());
            if self.workspace_uses.lock().unwrap().contains_key(&key)
                || self.reconciliation_in_use(&artifact)
                || self.allocation_uses.lock().unwrap().contains(&job)
            {
                return Err(Error::Conflict(
                    "previous processing or allocation workers have not quiesced".into(),
                ));
            }
            if artifact.hold.is_some()
                || !matches!(
                    artifact.state.as_str(),
                    "active" | "transitioning" | "retained" | "completed"
                )
            {
                return Err(Error::Conflict(
                    "processing source is held or retiring".into(),
                ));
            }
            self.workspace_uses.lock().unwrap().insert(key, 1);
            artifact
        };
        let lease = AttemptUse(Arc::new(UseLease {
            inventory: self.clone(),
            source: SourceGeneration {
                artifact: artifact.id.clone(),
                generation: artifact.generation.clone(),
            },
        }));
        self.verify(&artifact)?;
        checkpoint()?;
        {
            let _coordinator = self.mutation_guard()?;
            let mut current = self.get(&artifact.id)?;
            if current.generation != artifact.generation
                || current.path != artifact.path
                || current.root != artifact.root
                || current.state != artifact.state
                || current.hold.is_some()
            {
                return Err(Error::Conflict(
                    "processing source changed during custody verification".into(),
                ));
            }
            if resume {
                if matches!(current.state.as_str(), "retained" | "completed") {
                    current.state = "active".into();
                    current.revision += 1;
                    save_artifact(&self.db.lock().unwrap(), &current)?;
                }
                self.db.lock().unwrap().execute(
                    "DELETE FROM workspace_finalizations WHERE source=?1 AND generation=?2",
                    params![current.id, current.generation],
                )?;
            }
        }
        Ok(Some(lease))
    }
    /// A managed seed's exact terminal journal can outlive its payload path.
    /// This only reserves the existing use registry: it adopts no inode and
    /// resets no stage/finalization state. The caller validates fresh ownership
    /// or an existing deletion receipt and supplies current backend/ref proof.
    pub(super) fn acquire_seed_retirement_use(
        self: &Arc<Self>,
        job: u32,
        source: &SourceGeneration,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<AttemptUse> {
        let _coordinator = self.mutation_guard()?;
        checkpoint()?;
        let artifact = self.get(&source.artifact)?;
        if artifact.generation != source.generation
            || artifact.job != Some(job)
            || !artifact.owned
            || !artifact.automatic_cleanup_eligible
            || artifact.hold.is_some()
            || !matches!(
                artifact.state.as_str(),
                "active" | "retained" | "completed" | "deleting" | "delete_failed"
            )
        {
            return Err(Error::Conflict(
                "managed seed retirement source generation/cohort is held or changed".into(),
            ));
        }
        if self.artifact_in_use(&artifact)? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "managed seed source still has active readers or writers",
            )
            .into());
        }
        checkpoint()?;
        self.workspace_uses
            .lock()
            .unwrap()
            .insert((source.artifact.clone(), source.generation.clone()), 1);
        Ok(AttemptUse(Arc::new(UseLease {
            inventory: self.clone(),
            source: source.clone(),
        })))
    }
    pub(super) fn authorized_processing_use(
        &self,
        artifact: &Artifact,
        guard: Option<&AttemptUse>,
    ) -> bool {
        guard.is_some_and(|guard| {
            std::ptr::eq(self, guard.0.inventory.as_ref())
                && guard.source().artifact == artifact.id
                && guard.source().generation == artifact.generation
                && self.source_in_use(guard.source())
        })
    }
    /// Terminal Delete is performed by the current owner after all other
    /// workers have joined. Its guard continues to fence outside actors until
    /// the deletion worker actually exits; Keep and holds remain mandatory.
    pub fn delete_failed_payload(
        &self,
        guard: &AttemptUse,
        job: u32,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Operation> {
        checkpoint()?;
        let artifact = {
            let _mutation = self.mutation_guard()?;
            checkpoint()?;
            let artifact = self.for_job(job)?.ok_or(Error::NotFound)?;
            if !self.authorized_processing_use(&artifact, Some(guard)) {
                return Err(Error::Conflict(
                    "terminal delete lacks its live generation owner".into(),
                ));
            }
            if artifact.keep_on_failure {
                return Err(Error::Conflict(
                    "per-download Keep on failure is enabled".into(),
                ));
            }
            let path = artifact.path.clone();
            let root = artifact.root.clone();
            drop(_mutation);
            self.finish_artifact_unlocked(artifact, &path, &root, "retained")?
        };
        if !self.authorized_processing_use(&artifact, Some(guard)) {
            return Err(Error::Conflict(
                "terminal delete lacks its live generation owner".into(),
            ));
        }
        let key = format!("failed-delete-{}", artifact.id);
        let op = match self.operation(&key) {
            Ok(op) => op,
            Err(Error::NotFound) => self.request_delete_with_use(
                &artifact.id,
                artifact.revision,
                &key,
                0,
                false,
                Some((guard, checkpoint)),
            )?,
            Err(error) => return Err(error),
        };
        self.execute_delete_with_use(&op.id, Some((guard, checkpoint)))
    }
    pub(super) fn source_in_use(&self, source: &SourceGeneration) -> bool {
        if self
            .reconciliation_uses
            .lock()
            .unwrap()
            .contains(&(source.artifact.clone(), source.generation.clone()))
        {
            return true;
        }
        self.workspace_uses
            .lock()
            .unwrap()
            .contains_key(&(source.artifact.clone(), source.generation.clone()))
    }
    pub(super) fn artifact_in_use(&self, artifact: &Artifact) -> Result<bool> {
        if artifact
            .job
            .is_some_and(|job| self.allocation_uses.lock().unwrap().contains(&job))
        {
            return Ok(true);
        }
        if self.source_in_use(&SourceGeneration {
            artifact: artifact.id.clone(),
            generation: artifact.generation.clone(),
        }) {
            return Ok(true);
        }
        Ok(self
            .scratch_operation(&artifact.id)?
            .is_some_and(|(_, workspace)| {
                self.source_in_use(&SourceGeneration {
                    artifact: workspace.source.id,
                    generation: workspace.source.generation,
                })
            }))
    }
    /// Preserve custody if an owned process group cannot be proven stopped.
    pub fn hold_processing_uncertainty(
        &self,
        source: &SourceGeneration,
        reason: &str,
    ) -> Result<()> {
        let _coordinator = self.mutation_guard()?;
        let mut artifact = self.get(&source.artifact)?;
        if artifact.generation != source.generation {
            return Err(Error::Conflict("processing hold generation changed".into()));
        }
        if artifact.hold.is_none() {
            artifact.hold = Some("review: subprocess quiescence unconfirmed".into());
            artifact.error = Some(reason.into());
            artifact.revision += 1;
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            save_artifact(&tx, &artifact)?;
            event(
                &tx,
                &artifact.id,
                "processing_quiescence_unconfirmed",
                reason,
            )?;
            tx.commit()?;
        }
        Ok(())
    }
    pub fn processing_in_use(&self, job: u32) -> Result<bool> {
        let _coordinator = self.mutation_guard()?;
        Ok(self.for_job(job)?.is_some_and(|a| {
            self.source_in_use(&SourceGeneration {
                artifact: a.id,
                generation: a.generation,
            })
        }))
    }

    /// Persist the history association before stamping PP_DONE. Pending
    /// evidence cannot admit retirement until the matching stamp is confirmed.
    pub fn stage_workspace_finalization(
        &self,
        source: &SourceGeneration,
        outcome: &str,
        proof: &FinalizationProof,
    ) -> Result<()> {
        let _coordinator = self.mutation_guard()?;
        let parent = self.get(&source.artifact)?;
        if parent.generation != source.generation || !proof.valid() {
            return Err(Error::Conflict(
                "pending completion generation or history evidence changed".into(),
            ));
        }
        self.db.lock().unwrap().execute("INSERT INTO workspace_finalizations(source,generation,outcome,proof,job,activated) VALUES(?1,?2,?3,?4,?5,0) ON CONFLICT(source,generation) DO UPDATE SET outcome=excluded.outcome,proof=excluded.proof,job=excluded.job,activated=CASE WHEN workspace_finalizations.proof=excluded.proof AND workspace_finalizations.outcome=excluded.outcome THEN workspace_finalizations.activated ELSE 0 END",
            params![source.artifact,source.generation,outcome,serde_json::to_string(proof)?,parent.job])?;
        Ok(())
    }
    pub fn confirm_stamped_finalization(&self, completion: &StagedCompletion) -> Result<()> {
        let _coordinator = self.mutation_guard()?;
        if self.get(&completion.source.artifact)?.generation != completion.source.generation
            || !completion.proof.valid()
        {
            return Err(Error::Conflict("terminal stamp generation changed".into()));
        }
        let updated = self.db.lock().unwrap().execute("UPDATE workspace_finalizations SET activated=1 WHERE source=?1 AND generation=?2 AND outcome=?3 AND proof=?4",
            params![completion.source.artifact, completion.source.generation, completion.outcome,serde_json::to_string(&completion.proof)?])?;
        if updated != 1 {
            return Err(Error::Conflict(
                "terminal stamp has no matching pending completion".into(),
            ));
        }
        Ok(())
    }
    pub fn staged_completions(&self, job: u32) -> Result<Vec<StagedCompletion>> {
        let db = self.db.lock().unwrap();
        let mut statement = db.prepare("SELECT source,generation,outcome,proof FROM workspace_finalizations WHERE job=?1 AND activated=0 ORDER BY source LIMIT 25")?;
        let rows = statement.query_map([job], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        rows.map(|row| {
            let (artifact, generation, outcome, proof) = row?;
            Ok(StagedCompletion {
                source: SourceGeneration {
                    artifact,
                    generation,
                },
                outcome,
                proof: serde_json::from_str(&proof)?,
            })
        })
        .collect()
    }
    /// Authority acceptance is itself the cluster terminal evidence. Persist
    /// it before terminalization so maintenance can replay a crash at either step.
    pub fn accept_cluster_completion(
        &self,
        source: &SourceGeneration,
        outcome: &str,
        proof: FinalizationProof,
    ) -> Result<()> {
        {
            let _coordinator = self.mutation_guard()?;
            let parent = self.get(&source.artifact)?;
            if parent.generation != source.generation
                || self.source_in_use(source)
                || !matches!(proof, FinalizationProof::Cluster { .. })
                || !proof.valid()
            {
                return Err(Error::Conflict(
                    "cluster acceptance lacks generation or quiescence evidence".into(),
                ));
            }
            self.db.lock().unwrap().execute("INSERT INTO workspace_finalizations(source,generation,outcome,proof,job,activated) VALUES(?1,?2,?3,?4,?5,1) ON CONFLICT(source,generation) DO UPDATE SET outcome=excluded.outcome,proof=excluded.proof,job=excluded.job,activated=1",
                params![source.artifact,source.generation,outcome,serde_json::to_string(&proof)?,parent.job])?;
        }
        self.reconcile_workspace_retirements()?;
        self.run_workspace_retirements()
    }
    /// Called only after durable whole-job completion/authority acceptance and
    /// joined workers. This receipt is not inferred from a stage return.
    pub fn finalize_workspaces(
        &self,
        source: &SourceGeneration,
        outcome: &str,
        proof: FinalizationProof,
    ) -> Result<()> {
        {
            let _coordinator = self.mutation_guard()?;
            let parent = self.get(&source.artifact)?;
            if parent.generation != source.generation
                || self.source_in_use(source)
                || !proof.valid()
            {
                return Err(Error::Conflict(
                    "workspace finalization lacks generation or quiescence evidence".into(),
                ));
            }
            if !self.workspace_parent_transferred(&parent)?
                && !matches!(
                    parent.state.as_str(),
                    "completed" | "parked_failed" | "retained" | "deleted" | "source_gone"
                )
            {
                return Err(Error::Conflict("workspace parent has not finalized".into()));
            }
            self.db.lock().unwrap().execute(
                "INSERT INTO workspace_finalizations(source,generation,outcome,proof,job,activated) VALUES(?1,?2,?3,?4,?5,1) ON CONFLICT(source,generation) DO UPDATE SET outcome=excluded.outcome,proof=excluded.proof,job=excluded.job,activated=1",
                params![source.artifact, source.generation, outcome, serde_json::to_string(&proof)?,parent.job],
            )?;
        }
        self.reconcile_workspace_retirements()?;
        self.run_workspace_retirements()
    }

    fn workspace_finalization(
        &self,
        source: &SourceGeneration,
    ) -> Result<Option<(String, FinalizationProof)>> {
        let row: Option<(String, String)> = self.db.lock().unwrap().query_row(
            "SELECT outcome,proof FROM workspace_finalizations WHERE source=?1 AND generation=?2 AND activated=1",
            params![source.artifact, source.generation], |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?;
        row.map(|(outcome, proof)| Ok((outcome, serde_json::from_str(&proof)?)))
            .transpose()
    }
    fn scratch_operation(&self, scratch: &str) -> Result<Option<(Operation, Workspace)>> {
        let raw: Option<String> = self.db.lock().unwrap().query_row(
            "SELECT data FROM operations WHERE json_extract(data,'$.kind') IN ('extract','par_repair') AND json_extract(json_extract(data,'$.request'),'$.scratch.id')=?1 LIMIT 1",
            [scratch], |row| row.get(0),
        ).optional()?;
        raw.map(|raw| {
            let op: Operation = serde_json::from_str(&raw)?;
            let workspace = serde_json::from_str(&op.request)?;
            Ok((op, workspace))
        })
        .transpose()
    }

    /// Publication relinquishes the parent to its consumer, not the private
    /// scratch. Only the exact durable handoff permits this ownership exception;
    /// consumer moves/removal must not require reading its payload again.
    fn workspace_parent_transferred(&self, parent: &Artifact) -> Result<bool> {
        if parent.state != "transferred" || parent.owned || !parent.delivery_lifecycle {
            return Ok(false);
        }
        let delivery = match self.delivery(&format!("{}:{}", parent.id, parent.generation)) {
            Ok(delivery) => delivery,
            Err(Error::NotFound) => return Ok(false),
            Err(error) => return Err(error),
        };
        Ok(delivery.artifact_id == parent.id
            && delivery.publication_generation == parent.generation
            && delivery.runner_instance_id == self.installation
            && delivery.publication_path == parent.path.to_string_lossy()
            && !delivery.files.is_empty())
    }

    /// The API has durably recorded a confirmed queue deletion. Retire only
    /// internal scratch; keeping the source for Undo/recovery is independent.
    pub fn finalize_deleted_workspaces(&self, entry: &crate::HistoryEntry) -> Result<()> {
        let Some(id) = entry
            .params
            .iter()
            .find_map(|(key, value)| (key == "Artifact:Id").then_some(value))
        else {
            return Ok(());
        };
        let parent = self.get(id)?;
        {
            let Some(_lease) = self.reserve_reconciliation(std::slice::from_ref(&parent))? else {
                return Ok(());
            };
            self.record_deleted_workspace_finalization(&parent, entry)?;
        }
        self.reconcile_workspace_retirements()?;
        self.run_workspace_retirements()
    }

    fn record_deleted_workspace_finalization(
        &self,
        parent: &Artifact,
        entry: &crate::HistoryEntry,
    ) -> Result<()> {
        let param = |key: &str| {
            entry
                .params
                .iter()
                .find_map(|(k, v)| (k == key).then_some(v.as_str()))
        };
        // Old DELETED entries lack Artifact:Generation. Their random allocation
        // id, job, retained path and timestamp jointly bind the existing source.
        // Allocation ids are never recycled when a new generation is allocated.
        let exact_generation = param("Artifact:Generation");
        if entry.status != "DELETED"
            || entry.seq <= 0
            || parent.job != Some(entry.job.0)
            || param("Artifact:Id") != Some(parent.id.as_str())
            || entry.completed_at_unix < parent.created_at
            || exact_generation.is_some_and(|generation| generation != parent.generation)
            || (exact_generation.is_none() && entry.final_dir.as_deref() != parent.path.to_str())
            || !parent.owned
            || !matches!(
                parent.state.as_str(),
                "retained" | "parked_failed" | "deleted" | "source_gone"
            )
        {
            return Ok(());
        }
        let _coordinator = self.mutation_guard()?;
        self.check_payload_capture(parent)?;
        // A confirmed deletion supersedes an unfinished PP stamp barrier after
        // workers quiesce. Already activated terminal evidence remains intact.
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let inserted = tx.execute(
            "INSERT INTO workspace_finalizations(source,generation,outcome,proof,job,activated) VALUES(?1,?2,'DELETED',?3,?4,1) ON CONFLICT(source,generation) DO UPDATE SET outcome=excluded.outcome,proof=excluded.proof,job=excluded.job,activated=1 WHERE workspace_finalizations.activated=0",
            params![parent.id, parent.generation, serde_json::to_string(&FinalizationProof::Local { history_seq: entry.seq as u64 })?, entry.job.0],
        )?;
        if inserted != 0 {
            event(
                &tx,
                &parent.id,
                "workspace_deleted_finalization_recovered",
                &entry.seq.to_string(),
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn workspace_assessment(&self, scratch: &str) -> Result<Option<WorkspaceAssessment>> {
        let Some((op, workspace)) = self.scratch_operation(scratch)? else {
            return Ok(None);
        };
        self.assess_workspace(&op, &workspace).map(Some)
    }
    fn assess_workspace(
        &self,
        op: &Operation,
        workspace: &Workspace,
    ) -> Result<WorkspaceAssessment> {
        self.assess_workspace_reserved(op, workspace, false)
    }
    fn processing_workers_active(&self, source: &SourceGeneration) -> bool {
        self.workspace_uses
            .lock()
            .unwrap()
            .contains_key(&(source.artifact.clone(), source.generation.clone()))
    }
    fn assess_workspace_reserved(
        &self,
        op: &Operation,
        workspace: &Workspace,
        reserved: bool,
    ) -> Result<WorkspaceAssessment> {
        let scratch = self.get(&workspace.scratch.id)?;
        let parent = self.get(&workspace.source.id)?;
        let transferred = self.workspace_parent_transferred(&parent)?;
        let source = SourceGeneration {
            artifact: parent.id.clone(),
            generation: workspace.source.generation.clone(),
        };
        let identity_valid =
            scratch.generation == workspace.scratch.generation && self.verify(&scratch).is_ok();
        let mut reasons = Vec::new();
        if !workspace.keep_provenance {
            reasons.push("operator_keep_provenance_unknown".into());
        }
        if scratch.keep {
            reasons.push("operator_keep".into());
        }
        if parent.keep {
            reasons.push("source_operator_keep".into());
        }
        if scratch
            .hold
            .as_deref()
            .is_some_and(|h| h != "transform workspace")
        {
            reasons.push("review_or_recovery_hold".into());
        }
        if !scratch.owned || (!parent.owned && !transferred) {
            reasons.push("ownership_unproven".into());
        }
        if !identity_valid {
            reasons.push("scratch_identity_changed".into());
        }
        if parent.generation != workspace.source.generation {
            reasons.push("source_generation_changed".into());
        }
        if if reserved {
            self.processing_workers_active(&source)
        } else {
            self.source_in_use(&source)
        } {
            reasons.push("processing_workers_active".into());
        }
        if !matches!(op.state.as_str(), "succeeded" | "failed") {
            reasons.push("transform_not_terminal".into());
        }
        if !transferred
            && !matches!(
                parent.state.as_str(),
                "completed" | "parked_failed" | "retained" | "deleted" | "source_gone"
            )
        {
            reasons.push("source_not_finalized".into());
        }
        if parent.hold.is_some() {
            reasons.push("source_review_or_recovery_hold".into());
        }
        let finalization = self.workspace_finalization(&source)?;
        if !finalization
            .as_ref()
            .is_some_and(|(_, proof)| proof.valid())
        {
            reasons.push("terminal_job_evidence_missing".into());
        }
        if parent.state == "source_gone" && finalization.is_none() {
            reasons.push("source_disappearance_is_not_completion".into());
        }
        let pending_recovery: bool = self.db.lock().unwrap().query_row(
            "SELECT EXISTS(SELECT 1 FROM recoveries WHERE artifact IN (?1,?2) AND state NOT IN ('imported','cancelled','partial'))",
            params![parent.id, scratch.id], |row| row.get(0),
        )?;
        if pending_recovery {
            reasons.push("recovery_active".into());
        }
        if parent.state == "deleted" {
            let authorized: bool = self.db.lock().unwrap().query_row(
                "SELECT EXISTS(SELECT 1 FROM operations WHERE artifact=?1 AND state='succeeded' AND json_extract(data,'$.kind')='delete' AND json_extract(json_extract(data,'$.request'),'$[4]')=?2)",
                params![parent.id, parent.generation], |row| row.get(0),
            )?;
            if !authorized {
                reasons.push("source_deletion_evidence_missing".into());
            }
        } else if !transferred && !parent.terminal() && self.verify(&parent).is_err() {
            reasons.push("source_identity_changed".into());
        }
        let bytes = scratch.summary().1;
        Ok(WorkspaceAssessment {
            operation: op.id.clone(),
            source,
            scratch: SourceGeneration {
                artifact: scratch.id,
                generation: scratch.generation,
            },
            source_state: parent.state,
            transform_state: op.state.clone(),
            hold: scratch.hold,
            keep: scratch.keep,
            identity_valid,
            bytes,
            reasons,
        })
    }
    /// One bounded cursor advances past protected records rather than letting
    /// one permanently held workspace starve the remainder of the inventory.
    pub fn reconcile_workspace_retirements(&self) -> Result<()> {
        self.reconcile_workspace_retirements_with_history(None)
    }

    pub(super) fn reconcile_workspace_retirements_with_history(
        &self,
        history: Option<&crate::history::HistoryDb>,
    ) -> Result<()> {
        let raws = {
            let db = self.db.lock().unwrap();
            let cursor = self.workspace_cursor.lock().unwrap().clone();
            let mut query = db.prepare("SELECT data FROM operations WHERE id>?1 AND json_extract(data,'$.kind') IN ('extract','par_repair') ORDER BY id LIMIT 25")?;
            let rows = query.query_map([&cursor], |row| row.get::<_, String>(0))?;
            let mut raws = rows.collect::<std::result::Result<Vec<_>, _>>()?;
            if raws.is_empty() && !cursor.is_empty() {
                // A new terminal receipt may qualify an already visited row.
                // Wrap immediately while retaining the 25-row work bound.
                self.workspace_cursor.lock().unwrap().clear();
                let rows = query.query_map([""], |row| row.get::<_, String>(0))?;
                raws = rows.collect::<std::result::Result<Vec<_>, _>>()?;
            }
            raws
        };
        for raw in raws {
            let mut op: Operation = serde_json::from_str(&raw)?;
            *self.workspace_cursor.lock().unwrap() = op.id.clone();
            let workspace: Workspace = serde_json::from_str(&op.request)?;
            let Ok(mut scratch) = self.get(&workspace.scratch.id) else {
                continue;
            };
            if scratch.terminal() || matches!(scratch.state.as_str(), "deleting" | "delete_failed")
            {
                continue;
            }
            let source = SourceGeneration {
                artifact: workspace.source.id.clone(),
                generation: workspace.source.generation.clone(),
            };
            let parent = self.get(&source.artifact)?;
            let Some(_lease) = self.reserve_reconciliation(&[parent.clone(), scratch.clone()])?
            else {
                continue;
            };

            // Revisit pre-upgrade transforms in the same bounded cursor sweep.
            // A history entry cannot grant ownership or waive any scratch check.
            if workspace.keep_provenance
                && parent.generation == source.generation
                && self.workspace_finalization(&source)?.is_none()
            {
                if let (Some(history), Some(job)) = (history, parent.job) {
                    if let Some(entry) = history
                        .deleted_artifact_completion(crate::JobId(job), &parent.id)
                        .map_err(|error| {
                            Error::Conflict(format!("deleted workspace history: {error}"))
                        })?
                    {
                        self.record_deleted_workspace_finalization(&parent, &entry)?;
                    }
                }
            }

            if let Some((outcome, FinalizationProof::Cluster { .. })) =
                self.workspace_finalization(&source)?
            {
                let parent = self.get(&source.artifact)?;
                if parent.generation == source.generation
                    && !self.processing_workers_active(&source)
                    && parent.hold.is_none()
                    && matches!(parent.state.as_str(), "active" | "transitioning")
                {
                    let path = parent.path.clone();
                    let root = parent.root.clone();
                    self.finish_artifact_unlocked(
                        parent,
                        &path,
                        &root,
                        if outcome == "SUCCESS" {
                            "completed"
                        } else {
                            "retained"
                        },
                    )?;
                }
            }
            // Durable whole-job failure + joined workers can terminalize an
            // aborted stage. A successful job never supplies this inference.
            if op.state == "running"
                && workspace.keep_provenance
                && !self.processing_workers_active(&SourceGeneration {
                    artifact: workspace.source.id.clone(),
                    generation: workspace.source.generation.clone(),
                })
                && self
                    .workspace_finalization(&SourceGeneration {
                        artifact: workspace.source.id.clone(),
                        generation: workspace.source.generation.clone(),
                    })?
                    .is_some_and(|(outcome, proof)| outcome != "SUCCESS" && proof.valid())
            {
                let dir = self.verify(&scratch)?;
                scratch.files = fs::manifest(&dir, 100_000)?;
                scratch.state = "retained".into();
                op.state = "failed".into();
                let _coordinator = self.mutation_guard()?;
                let original = self.get(&scratch.id)?;
                if original.generation != scratch.generation
                    || original.revision != scratch.revision
                {
                    continue;
                }
                let mut db = self.db.lock().unwrap();
                let tx = db.transaction()?;
                save_artifact(&tx, &scratch)?;
                save_operation(&tx, &op)?;
                tx.commit()?;
            }
            let assessment = self.assess_workspace_reserved(&op, &workspace, true)?;
            let retirement_id = format!("retire-{}", scratch.generation);
            if self.operation(&retirement_id).is_ok_and(|op| {
                matches!(
                    op.state.as_str(),
                    "queued" | "running" | "retry" | "review" | "succeeded"
                )
            }) {
                continue;
            }
            if !assessment.reasons.is_empty() {
                let error = Some(format!(
                    "workspace cleanup: {}",
                    assessment.reasons.join(", ")
                ));
                if scratch.error != error {
                    let _coordinator = self.mutation_guard()?;
                    self.check_payload_capture(&scratch)?;
                    scratch.error = error;
                    save_artifact(&self.db.lock().unwrap(), &scratch)?;
                }
                continue;
            }
            let Some((outcome, proof)) = self.workspace_finalization(&assessment.source)? else {
                continue;
            };
            let _coordinator = self.mutation_guard()?;
            self.check_payload_capture(&scratch)?;
            let current_parent = self.get(&source.artifact)?;
            if current_parent.generation != source.generation
                || current_parent.hold.is_some()
                || current_parent.keep
            {
                continue;
            }
            scratch.hold = None; // only the known automatic transform hold
            scratch.error = None;
            scratch.revision += 1;
            let receipt = RetirementReceipt {
                source: assessment.source,
                transform: op.id,
                scratch: assessment.scratch,
                scratch_revision: scratch.revision,
                outcome,
                proof,
            };
            let retirement = Operation {
                id: retirement_id,
                artifact: scratch.id.clone(),
                kind: "retire_workspace".into(),
                state: "queued".into(),
                request: serde_json::to_string(&receipt)?,
                created_at: now(),
                not_before: 0,
                attempts: 0,
                next_retry: 0,
                error: None,
            };
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            save_artifact(&tx, &scratch)?;
            save_operation(&tx, &retirement)?;
            event(
                &tx,
                &scratch.id,
                "workspace_retirement_admitted",
                &retirement.request,
            )?;
            tx.commit()?;
        }
        Ok(())
    }
    pub(super) fn run_workspace_retirements(&self) -> Result<()> {
        let keys = {
            let db = self.db.lock().unwrap();
            let mut query = db.prepare("SELECT id FROM operations WHERE state IN ('queued','running','retry') AND json_extract(data,'$.kind')='retire_workspace' AND json_extract(data,'$.next_retry')<=unixepoch() ORDER BY json_extract(data,'$.created_at'),id LIMIT 25")?;
            let rows = query.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for key in keys {
            if let Err(error) = self.execute_workspace_retirement(&key) {
                tracing::warn!(operation=%key, %error, "workspace retirement remains pending");
            }
        }
        Ok(())
    }
    pub fn execute_workspace_retirement(&self, key: &str) -> Result<Operation> {
        let _lease;
        let coordinator = self.mutation_guard()?;
        let mut op = self.operation(key)?;
        if op.kind != "retire_workspace" {
            return Err(Error::Conflict("not a workspace retirement".into()));
        }
        if !matches!(op.state.as_str(), "queued" | "running" | "retry") || op.next_retry > now() {
            return Ok(op);
        }
        let mut receipt: RetirementReceipt = serde_json::from_str(&op.request)?;
        let mut scratch = self.get(&op.artifact)?;
        if scratch.generation != receipt.scratch.generation || !receipt.proof.valid() {
            op.state = "review".into();
            op.error = Some("workspace retirement generation/evidence changed".into());
            save_operation(&self.db.lock().unwrap(), &op)?;
            return Ok(op);
        }
        if scratch.keep || scratch.hold.is_some() || !scratch.owned {
            op.state = "cancelled".into();
            op.error = Some("workspace acquired Keep or a hold".into());
            save_operation(&self.db.lock().unwrap(), &op)?;
            return Ok(op);
        }
        let transform = self.operation(&receipt.transform)?;
        let workspace: Workspace = serde_json::from_str(&transform.request)?;
        if transform.id != workspace.operation_id
            || receipt.scratch.artifact != scratch.id
            || receipt.source.artifact != workspace.source.id
            || receipt.source.generation != workspace.source.generation
            || receipt.scratch.artifact != workspace.scratch.id
            || receipt.scratch.generation != workspace.scratch.generation
            || self.get(&receipt.source.artifact)?.generation != receipt.source.generation
        {
            op.state = "review".into();
            op.error = Some(
                "workspace receipt no longer matches its source, transform or finalization".into(),
            );
            save_operation(&self.db.lock().unwrap(), &op)?;
            return Ok(op);
        }
        let Some((outcome, proof)) = self.workspace_finalization(&receipt.source)? else {
            return Err(Error::Conflict(
                "workspace retirement awaits current whole-job finalization".into(),
            ));
        };
        if !proof.valid() {
            return Err(Error::Conflict(
                "workspace finalization evidence is invalid".into(),
            ));
        }
        let parent = self.get(&receipt.source.artifact)?;
        let Some(lease) = self.reserve_reconciliation_locked(
            &[parent.clone(), scratch.clone()],
            None,
            &coordinator,
        )?
        else {
            return Err(Error::Conflict(
                "workspace workers have not quiesced".into(),
            ));
        };
        _lease = lease;
        drop(coordinator);
        let assessment = self.assess_workspace_reserved(&transform, &workspace, true)?;
        let replay_missing = op.attempts > 0
            && std::fs::symlink_metadata(&scratch.path)
                .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound);
        let reasons: Vec<_> = assessment
            .reasons
            .iter()
            .filter(|r| r.as_str() != "scratch_identity_changed" || !replay_missing)
            .collect();
        if !reasons.is_empty() {
            return Err(Error::Conflict(format!(
                "workspace retirement deferred: {reasons:?}"
            )));
        }
        if op.attempts == 0 && scratch.revision != receipt.scratch_revision {
            op.state = "cancelled".into();
            op.error = Some("workspace policy changed after admission".into());
            save_operation(&self.db.lock().unwrap(), &op)?;
            return Ok(op);
        }
        // A restart may supersede the source's terminal receipt while this
        // old scratch generation remains pending. Requalify against the new
        // durable finalization, preserving deletion replay attempts/manifests.
        let coordinator = self.mutation_guard()?;
        self.check_payload_capture(&scratch)?;
        self.check_payload_capture(&parent)?;
        let saved = self.operation(key)?;
        if saved.request != op.request || saved.state != op.state || saved.attempts != op.attempts {
            return Err(Error::Conflict(
                "workspace retirement journal changed".into(),
            ));
        }
        receipt.outcome = outcome;
        receipt.proof = proof;
        op.request = serde_json::to_string(&receipt)?;
        op.state = "running".into();
        op.attempts += 1;
        scratch.state = "deleting".into();
        {
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            save_operation(&tx, &op)?;
            save_artifact(&tx, &scratch)?;
            tx.commit()?;
        }
        drop(coordinator);
        let admitted = scratch.clone();
        match self.delete_manifest_checked(&scratch, op.attempts) {
            Ok(()) => {
                scratch.state = "deleted".into();
                scratch.error = None;
                op.state = "succeeded".into();
                op.error = None;
            }
            Err(error) => {
                let conflict = matches!(error, Error::Conflict(_));
                op.state = if conflict { "review" } else { "retry" }.into();
                op.error = Some(error.to_string());
                scratch.state = "delete_failed".into();
                scratch.error = op.error.clone();
                if conflict {
                    scratch.hold = Some("review: workspace cleanup".into());
                }
                op.next_retry =
                    now() + [60, 300, 1800, 21600][op.attempts.saturating_sub(1).min(3) as usize];
            }
        }
        let _coordinator = self.mutation_guard()?;
        self.check_payload_capture(&admitted)?;
        scratch.revision += 1;
        scratch.updated_at = now();
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &scratch)?;
        save_operation(&tx, &op)?;
        event(
            &tx,
            &scratch.id,
            &op.state,
            op.error
                .as_deref()
                .unwrap_or("workspace retirement confirmed"),
        )?;
        tx.commit()?;
        Ok(op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (tempfile::TempDir, Arc<Inventory>, Workspace) {
        fixture_with_delivery(false)
    }
    fn fixture_with_delivery(delivery: bool) -> (tempfile::TempDir, Arc<Inventory>, Workspace) {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("processing");
        std::fs::create_dir(&root).unwrap();
        let inventory = Arc::new(Inventory::open(&temp.path().join("state")).unwrap());
        let path = root.join("job");
        inventory
            .allocate_for_delivery(12, &root, &path, delivery, delivery)
            .unwrap();
        std::fs::write(path.join("original"), b"original").unwrap();
        let workspace = inventory.workspace(12, "par_repair", "abc").unwrap();
        std::fs::write(workspace.scratch.path.join("scratch"), b"reconstructed").unwrap();
        inventory.finish_workspace(&workspace).unwrap();
        (temp, inventory, workspace)
    }
    fn source(workspace: &Workspace) -> SourceGeneration {
        SourceGeneration {
            artifact: workspace.source.id.clone(),
            generation: workspace.source.generation.clone(),
        }
    }
    fn terminal(inventory: &Inventory, workspace: &Workspace, state: &str) {
        inventory
            .finish(12, &workspace.source.path, &workspace.source.root, state)
            .unwrap();
    }
    fn record(inventory: &Inventory, workspace: &Workspace) {
        inventory.db.lock().unwrap().execute("INSERT INTO workspace_finalizations(source,generation,outcome,proof,activated) VALUES(?1,?2,'SUCCESS',?3,1)", params![workspace.source.id, workspace.source.generation, serde_json::to_string(&FinalizationProof::Local {history_seq:1}).unwrap()]).unwrap();
    }
    fn delivered_fixture() -> (tempfile::TempDir, Arc<Inventory>, Workspace, Delivery) {
        let (temp, inventory, workspace) = fixture_with_delivery(true);
        terminal(&inventory, &workspace, "completed");
        let delivery = inventory.create_delivery(12, true, None).unwrap();
        (temp, inventory, workspace, delivery)
    }
    fn deleted_entry(workspace: &Workspace) -> crate::HistoryEntry {
        serde_json::from_value(serde_json::json!({
            "job": workspace.source.job.unwrap(), "name": "deleted job",
            "status": "DELETED", "size": 8, "completed_at_unix": now(),
            "final_dir": workspace.source.path,
            "params": [["Artifact:Id", workspace.source.id]]
        }))
        .unwrap()
    }

    fn partial_failed_cleanup() -> (
        tempfile::TempDir,
        Arc<Inventory>,
        PathBuf,
        FailedCleanupProof,
    ) {
        let (temp, inventory, workspace) = fixture();
        let path = workspace.source.path.clone();
        std::fs::write(path.join("survivor"), b"still owned").unwrap();
        inventory
            .finish(12, &path, &workspace.source.root, "active")
            .unwrap();
        assert!(inventory.failed_cleanup_proof(12).unwrap().is_none());
        let custody = inventory.acquire_attempt_use(12).unwrap().unwrap();
        DELETE_FAIL_AFTER.with(|counter| counter.set(Some(1)));
        let operation = inventory
            .delete_failed_payload(&custody, 12, &|| Ok(()))
            .unwrap();
        assert_eq!(operation.state, "retry");
        assert_eq!(operation.attempts, 1);
        assert_eq!(std::fs::read_dir(&path).unwrap().count(), 1);
        drop(custody);
        let proof = inventory.failed_cleanup_proof(12).unwrap().unwrap();
        (temp, inventory, path, proof)
    }

    #[test]
    fn failed_cleanup_replays_partial_deletion_after_restart_without_processing_authority() {
        let (temp, inventory, path, proof) = partial_failed_cleanup();
        assert!(inventory.acquire_attempt_use(12).is_err());
        assert!(inventory.acquire_terminal_use(12, &|| Ok(())).is_err());
        let before = inventory.operation(proof.operation_id()).unwrap();
        let deferred = inventory.replay_failed_cleanup(&proof, &|| Ok(())).unwrap();
        assert_eq!(
            serde_json::to_value(&deferred).unwrap(),
            serde_json::to_value(&before).unwrap()
        );
        assert!(path.exists());
        inventory.close().unwrap();
        drop(inventory);

        let inventory = Arc::new(Inventory::open(&temp.path().join("state")).unwrap());
        assert_eq!(inventory.failed_cleanup_proof(12).unwrap().unwrap(), proof);
        inventory.validate_failed_cleanup_proof(&proof).unwrap();
        // Model expiry of the real retry deadline, preserving its authorization.
        let mut due = inventory.operation(proof.operation_id()).unwrap();
        due.next_retry = 0;
        save_operation(&inventory.db.lock().unwrap(), &due).unwrap();
        let completed = inventory.replay_failed_cleanup(&proof, &|| Ok(())).unwrap();
        assert_eq!(completed.id, before.id);
        assert_eq!(completed.request, before.request);
        assert_eq!(completed.attempts, 2);
        assert_eq!(completed.state, "succeeded");
        assert!(!path.exists());
        assert_eq!(
            inventory.get(&proof.source.artifact).unwrap().state,
            "deleted"
        );
        inventory.validate_failed_cleanup_proof(&proof).unwrap();
        assert_eq!(
            inventory
                .replay_failed_cleanup(&proof, &|| Ok(()))
                .unwrap()
                .attempts,
            2
        );
    }

    #[test]
    fn failed_cleanup_replay_preserves_keep_hold_deadline_and_live_custody_checks() {
        let (_temp, inventory, path, proof) = partial_failed_cleanup();
        let mut due = inventory.operation(proof.operation_id()).unwrap();
        due.next_retry = 0;
        save_operation(&inventory.db.lock().unwrap(), &due).unwrap();
        let artifact = inventory.get(&proof.source.artifact).unwrap();
        let kept = inventory
            .retention(&artifact.id, artifact.revision, true, None)
            .unwrap();
        inventory.validate_failed_cleanup_proof(&proof).unwrap();
        assert!(inventory.replay_failed_cleanup(&proof, &|| Ok(())).is_err());
        let mut artifact = inventory
            .retention(&kept.id, kept.revision, false, None)
            .unwrap();
        artifact.hold = Some("review: test".into());
        save_artifact(&inventory.db.lock().unwrap(), &artifact).unwrap();
        assert!(inventory.replay_failed_cleanup(&proof, &|| Ok(())).is_err());
        artifact.hold = None;
        save_artifact(&inventory.db.lock().unwrap(), &artifact).unwrap();
        let reader = inventory
            .reserve_reconciliation(std::slice::from_ref(&artifact))
            .unwrap()
            .unwrap();
        assert!(
            matches!(inventory.replay_failed_cleanup(&proof, &|| Ok(())),
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
        drop(reader);
        assert!(inventory
            .replay_failed_cleanup(&proof, &|| Err(std::io::ErrorKind::Interrupted.into()))
            .is_err());
        assert_eq!(
            inventory.operation(proof.operation_id()).unwrap().attempts,
            1
        );
        assert_eq!(std::fs::read_dir(path).unwrap().count(), 1);
        assert!(!inventory.processing_in_use(12).unwrap());
    }

    #[test]
    fn failed_cleanup_proof_rejects_changed_or_legacy_authority_and_never_creates_intent() {
        let (_temp, inventory, _path, proof) = partial_failed_cleanup();
        let original = inventory.operation(proof.operation_id()).unwrap();
        for alteration in ["generation", "legacy", "automatic", "artifact", "kind"] {
            let mut operation = original.clone();
            match alteration {
                "generation" => {
                    operation.request = serde_json::to_string(&(
                        &proof.source.artifact,
                        1u64,
                        0u64,
                        false,
                        "other-generation",
                    ))
                    .unwrap()
                }
                "legacy" => {
                    operation.request =
                        serde_json::to_string(&(&proof.source.artifact, 1u64, 0u64, false)).unwrap()
                }
                "automatic" => {
                    operation.request = serde_json::to_string(&(
                        &proof.source.artifact,
                        1u64,
                        0u64,
                        true,
                        &proof.source.generation,
                    ))
                    .unwrap()
                }
                "artifact" => operation.artifact = "another-source".into(),
                "kind" => operation.kind = "prune".into(),
                _ => unreachable!(),
            }
            save_operation(&inventory.db.lock().unwrap(), &operation).unwrap();
            assert!(
                inventory.validate_failed_cleanup_proof(&proof).is_err(),
                "{alteration}"
            );
            assert!(inventory.failed_cleanup_proof(12).is_err(), "{alteration}");
        }
        save_operation(&inventory.db.lock().unwrap(), &original).unwrap();
        let mut changed = inventory.get(&proof.source.artifact).unwrap();
        changed.job = Some(99);
        save_artifact(&inventory.db.lock().unwrap(), &changed).unwrap();
        assert!(inventory.validate_failed_cleanup_proof(&proof).is_err());
        assert!(inventory.failed_cleanup_proof(12).unwrap().is_none());
        assert_eq!(
            serde_json::to_value(inventory.operation(proof.operation_id()).unwrap()).unwrap(),
            serde_json::to_value(original).unwrap()
        );
    }

    #[test]
    fn failed_cleanup_replay_rejects_replaced_survivors_without_adopting_them() {
        let (_temp, inventory, path, proof) = partial_failed_cleanup();
        let survivor = std::fs::read_dir(&path)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::remove_file(&survivor).unwrap();
        std::fs::write(&survivor, b"replacement must remain").unwrap();
        let mut due = inventory.operation(proof.operation_id()).unwrap();
        due.next_retry = 0;
        save_operation(&inventory.db.lock().unwrap(), &due).unwrap();
        let operation = inventory.replay_failed_cleanup(&proof, &|| Ok(())).unwrap();
        assert_eq!(operation.state, "review");
        assert_eq!(std::fs::read(survivor).unwrap(), b"replacement must remain");
        assert!(inventory.acquire_attempt_use(12).is_err());
    }

    #[test]
    fn new_delivery_finalization_retires_scratch_without_reclaiming_publication() {
        let (_temp, inventory, workspace, delivery) = delivered_fixture();
        inventory
            .finalize_workspaces(
                &source(&workspace),
                "SUCCESS",
                FinalizationProof::Local { history_seq: 1 },
            )
            .unwrap();
        assert!(!workspace.scratch.path.exists());
        assert_eq!(
            inventory.get(&workspace.source.id).unwrap().state,
            "transferred"
        );
        assert_eq!(inventory.delivery(&delivery.delivery_id).unwrap(), delivery);
        assert_eq!(
            std::fs::read(workspace.source.path.join("original")).unwrap(),
            b"original"
        );
    }

    #[test]
    fn existing_delivered_scratch_retires_after_restart_and_consumer_move() {
        let (temp, inventory, workspace, delivery) = delivered_fixture();
        record(&inventory, &workspace); // receipt already present in the stuck backlog
        let library = temp.path().join("consumer-library");
        std::fs::rename(&workspace.source.path, &library).unwrap();
        std::fs::write(library.join("original"), b"consumer-owned replacement").unwrap();
        let consumer_identity = fs::identity(&std::fs::metadata(library.join("original")).unwrap());
        drop(inventory);
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        inventory.tick().unwrap();
        assert!(!workspace.scratch.path.exists());
        assert_eq!(
            inventory.get(&workspace.scratch.id).unwrap().state,
            "deleted"
        );
        let persisted = inventory.delivery(&delivery.delivery_id).unwrap();
        assert_eq!(persisted.files, delivery.files);
        assert_eq!(persisted.manifest_digest, delivery.manifest_digest);
        assert_eq!(persisted.publication_path, delivery.publication_path);
        assert_eq!(
            persisted.publication_generation,
            delivery.publication_generation
        );
        assert_eq!(
            std::fs::read(library.join("original")).unwrap(),
            b"consumer-owned replacement"
        );
        assert_eq!(
            fs::identity(&std::fs::metadata(library.join("original")).unwrap()),
            consumer_identity
        );
    }

    #[test]
    fn transferred_workspace_requires_exact_delivery_and_preserves_keep_and_holds() {
        for blocker in [
            "missing_delivery",
            "wrong_generation",
            "source_keep",
            "scratch_keep",
            "scratch_hold",
            "pending_completion",
        ] {
            let (_temp, inventory, workspace, delivery) = delivered_fixture();
            record(&inventory, &workspace);
            match blocker {
                "missing_delivery" => {
                    inventory
                        .db
                        .lock()
                        .unwrap()
                        .execute("DELETE FROM deliveries", [])
                        .unwrap();
                }
                "wrong_generation" => {
                    let mut wrong = delivery;
                    wrong.publication_generation = "another-generation".into();
                    inventory
                        .db
                        .lock()
                        .unwrap()
                        .execute(
                            "UPDATE deliveries SET data=?1",
                            [serde_json::to_string(&wrong).unwrap()],
                        )
                        .unwrap();
                }
                "pending_completion" => {
                    inventory
                        .db
                        .lock()
                        .unwrap()
                        .execute("UPDATE workspace_finalizations SET activated=0", [])
                        .unwrap();
                }
                _ => {
                    let mut artifact = inventory
                        .get(if blocker == "source_keep" {
                            &workspace.source.id
                        } else {
                            &workspace.scratch.id
                        })
                        .unwrap();
                    if blocker == "scratch_hold" {
                        artifact.hold = Some("review: operator".into());
                    } else {
                        artifact.keep = true;
                    }
                    save_artifact(&inventory.db.lock().unwrap(), &artifact).unwrap();
                }
            }
            inventory.tick().unwrap();
            assert!(workspace.scratch.path.exists(), "{blocker}");
            assert!(workspace.source.path.join("original").exists(), "{blocker}");
        }
    }

    #[test]
    fn legacy_deleted_workspace_backlog_replays_in_bounded_batches_after_restart() {
        let (temp, inventory, first) = fixture();
        let history_path = temp.path().join("history.sqlite");
        let history = crate::history::HistoryDb::open(&history_path, None).unwrap();
        let mut workspaces = vec![first];
        for job in 13..43 {
            let root = workspaces[0].source.root.clone();
            let path = root.join(format!("job-{job}"));
            inventory.allocate(job, &root, &path).unwrap();
            std::fs::write(path.join("original"), b"original").unwrap();
            let workspace = inventory.workspace(job, "par_repair", "abc").unwrap();
            std::fs::write(workspace.scratch.path.join("scratch"), b"scratch").unwrap();
            inventory.finish_workspace(&workspace).unwrap();
            workspaces.push(workspace);
        }
        for workspace in &workspaces {
            inventory
                .finish(
                    workspace.source.job.unwrap(),
                    &workspace.source.path,
                    &workspace.source.root,
                    "retained",
                )
                .unwrap();
            // Reproduce the old API: ordinary DELETED history, no generation
            // param, no workspace_finalizations row and no future delete call.
            history.record(&deleted_entry(workspace)).unwrap();
        }
        drop(inventory);
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        inventory.tick_with_history(Some(&history)).unwrap();
        let remaining = workspaces
            .iter()
            .filter(|ws| ws.scratch.path.exists())
            .count();
        assert_eq!(remaining, workspaces.len() - 25);
        inventory.tick_with_history(Some(&history)).unwrap();
        for workspace in &workspaces {
            assert!(!workspace.scratch.path.exists());
            assert_eq!(
                inventory.get(&workspace.scratch.id).unwrap().state,
                "deleted"
            );
            assert_eq!(
                std::fs::read(workspace.source.path.join("original")).unwrap(),
                b"original"
            );
        }
        // Receipts survive loss of visible history; replay is idempotent.
        drop(history);
        inventory.tick().unwrap();
    }

    #[test]
    fn deleted_history_cannot_authorize_unrelated_or_live_workspace_cleanup() {
        for blocker in [
            "wrong_id",
            "wrong_generation",
            "wrong_path",
            "predates_allocation",
            "not_deleted",
            "live_source",
            "scratch_keep",
            "review_hold",
            "workers_active",
        ] {
            let (temp, inventory, workspace) = fixture();
            terminal(&inventory, &workspace, "retained");
            let mut entry = deleted_entry(&workspace);
            let mut guard = None;
            match blocker {
                "wrong_id" => entry.params[0].1 = "unrelated-allocation".into(),
                "wrong_generation" => entry
                    .params
                    .push(("Artifact:Generation".into(), "unrelated-generation".into())),
                "wrong_path" => entry.final_dir = Some("/unrelated/path".into()),
                "predates_allocation" => entry.completed_at_unix = workspace.source.created_at - 1,
                "not_deleted" => entry.status = "SUCCESS".into(),
                "live_source" => {
                    terminal(&inventory, &workspace, "active");
                }
                "workers_active" => {
                    guard = inventory.acquire_attempt_use(12).unwrap();
                }
                _ => {
                    let mut scratch = inventory.get(&workspace.scratch.id).unwrap();
                    if blocker == "scratch_keep" {
                        scratch.keep = true;
                    } else {
                        scratch.hold = Some("review: operator".into());
                    }
                    save_artifact(&inventory.db.lock().unwrap(), &scratch).unwrap();
                }
            }
            let history =
                crate::history::HistoryDb::open(&temp.path().join("history.sqlite"), None).unwrap();
            history.record(&entry).unwrap();
            inventory.tick_with_history(Some(&history)).unwrap();
            assert!(workspace.scratch.path.exists(), "{blocker}");
            assert!(workspace.source.path.join("original").exists(), "{blocker}");
            drop(guard);
        }
    }

    #[test]
    fn confirmed_deletion_replaces_pending_post_stamp_after_restart() {
        let (temp, inventory, workspace) = fixture();
        terminal(&inventory, &workspace, "retained");
        inventory
            .stage_workspace_finalization(
                &source(&workspace),
                "SUCCESS",
                &FinalizationProof::Local { history_seq: 17 },
            )
            .unwrap();
        let history =
            crate::history::HistoryDb::open(&temp.path().join("history.sqlite"), None).unwrap();
        history.record(&deleted_entry(&workspace)).unwrap();
        drop(inventory);
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        inventory.tick_with_history(Some(&history)).unwrap();
        assert!(!workspace.scratch.path.exists());
        assert_eq!(
            inventory
                .workspace_finalization(&source(&workspace))
                .unwrap()
                .unwrap()
                .0,
            "DELETED"
        );
        assert!(workspace.source.path.join("original").exists());
    }
    #[test]
    fn processing_successor_requires_fresh_path_and_preserves_prior_custody() {
        let (temp, inventory, workspace) = fixture();
        let root = temp.path().join("private");
        std::fs::create_dir(&root).unwrap();
        let target = root.join("fence-2");
        std::fs::create_dir(&target).unwrap();
        assert!(inventory
            .allocate_processing_successor(12, &root, &target)
            .is_err());
        assert_eq!(
            inventory.for_job(12).unwrap().unwrap().id,
            workspace.source.id
        );
        std::fs::remove_dir(&target).unwrap();
        let guard = inventory.acquire_attempt_use(12).unwrap().unwrap();
        assert!(inventory
            .allocate_processing_successor(12, &root, &target)
            .is_err());
        assert!(!target.exists());
        drop(guard);
        let mut kept = inventory.get(&workspace.source.id).unwrap();
        kept.keep = true;
        kept.hold = Some("operator review".into());
        save_artifact(&inventory.db.lock().unwrap(), &kept).unwrap();
        let next = inventory
            .allocate_processing_successor(12, &root, &target)
            .unwrap();
        assert!(next.owned);
        assert!(!next.keep);
        assert!(next.hold.is_none());
        assert_ne!(next.generation, workspace.source.generation);
        let old = inventory.get(&workspace.source.id).unwrap();
        assert_eq!(old.job, None);
        assert!(old.keep);
        assert_eq!(old.hold.as_deref(), Some("operator review"));
        assert!(old.path.join("original").exists());
        let guard = inventory.acquire_attempt_use(12).unwrap().unwrap();
        assert_eq!(guard.source().generation, next.generation);
        assert!(inventory.workspace(12, "par_repair", "abc2").is_ok());
    }
    #[test]
    fn terminal_receipts_retire_only_scratch_and_restart_gets_fresh_generation() {
        for state in ["completed", "parked_failed", "retained"] {
            let (_temp, inventory, workspace) = fixture();
            terminal(&inventory, &workspace, state);
            assert!(workspace.scratch.path.exists());
            inventory
                .finalize_workspaces(
                    &source(&workspace),
                    "SUCCESS",
                    FinalizationProof::Local { history_seq: 1 },
                )
                .unwrap();
            assert_eq!(
                inventory.get(&workspace.scratch.id).unwrap().state,
                "deleted"
            );
            assert!(!workspace.scratch.path.exists());
            assert_eq!(
                std::fs::read(workspace.source.path.join("original")).unwrap(),
                b"original"
            );
            if state == "parked_failed" {
                continue;
            }
            let guard = inventory.acquire_attempt_use(12).unwrap().unwrap();
            let fresh = inventory.workspace(12, "par_repair", "abc").unwrap();
            assert_ne!(fresh.scratch.generation, workspace.scratch.generation);
            assert_ne!(fresh.scratch.path, workspace.scratch.path);
            assert!(fresh.scratch.path.exists());
            drop(guard);
        }
    }
    #[test]
    fn cleanup_assessment_preserves_each_protection_and_legacy_ambiguity() {
        for reason in [
            "operator_keep",
            "review_or_recovery_hold",
            "operator_keep_provenance_unknown",
            "source_not_finalized",
            "transform_not_terminal",
            "terminal_job_evidence_missing",
            "scratch_identity_changed",
        ] {
            let (_temp, inventory, workspace) = fixture();
            terminal(&inventory, &workspace, "completed");
            if reason != "terminal_job_evidence_missing" {
                record(&inventory, &workspace);
            }
            let mut scratch = inventory.get(&workspace.scratch.id).unwrap();
            match reason {
                "operator_keep" => scratch.keep = true,
                "review_or_recovery_hold" => scratch.hold = Some("review: operator".into()),
                "operator_keep_provenance_unknown" => {
                    let mut op = inventory.operation(&workspace.operation_id).unwrap();
                    let mut legacy = workspace.clone();
                    legacy.keep_provenance = false;
                    op.request = serde_json::to_string(&legacy).unwrap();
                    save_operation(&inventory.db.lock().unwrap(), &op).unwrap();
                }
                "source_not_finalized" => {
                    let mut parent = inventory.get(&workspace.source.id).unwrap();
                    parent.state = "active".into();
                    save_artifact(&inventory.db.lock().unwrap(), &parent).unwrap();
                }
                "transform_not_terminal" => {
                    let mut op = inventory.operation(&workspace.operation_id).unwrap();
                    op.state = "running".into();
                    save_operation(&inventory.db.lock().unwrap(), &op).unwrap();
                }
                "scratch_identity_changed" => {
                    std::fs::rename(&scratch.path, scratch.path.with_extension("preserved"))
                        .unwrap();
                    std::fs::create_dir(&scratch.path).unwrap();
                }
                _ => {}
            }
            save_artifact(&inventory.db.lock().unwrap(), &scratch).unwrap();
            let assessment = inventory
                .workspace_assessment(&scratch.id)
                .unwrap()
                .unwrap();
            assert!(
                assessment.reasons.iter().any(|r| r == reason),
                "{reason}: {:?}",
                assessment.reasons
            );
            inventory.reconcile_workspace_retirements().unwrap();
            inventory.run_workspace_retirements().unwrap();
            assert!(scratch.path.exists());
            assert_eq!(inventory.get(&scratch.id).unwrap().keep, scratch.keep);
        }
    }
    #[test]
    fn actual_worker_custody_fences_retry_and_delete_until_last_clone_exits() {
        let (_temp, inventory, workspace) = fixture();
        let guard = inventory.acquire_attempt_use(12).unwrap().unwrap();
        let worker = guard.clone();
        drop(guard);
        terminal(&inventory, &workspace, "retained");
        assert!(inventory.validate_post_retry(12).is_err());
        assert!(inventory.acquire_attempt_use(12).is_err());
        assert!(inventory
            .finalize_workspaces(
                &source(&workspace),
                "FAILURE",
                FinalizationProof::Local { history_seq: 1 }
            )
            .is_err());
        let parent = inventory.for_job(12).unwrap().unwrap();
        assert!(inventory
            .request_delete(&parent.id, parent.revision, "live-worker", 0)
            .is_err());
        drop(worker);
        assert!(inventory.validate_post_retry(12).is_ok());
        inventory
            .finalize_workspaces(
                &source(&workspace),
                "FAILURE",
                FinalizationProof::Cluster {
                    receipt: "accepted-lease".into(),
                    accepted_at_ms: 1,
                },
            )
            .unwrap();
        assert!(!workspace.scratch.path.exists());
    }
    #[test]
    fn receipt_revalidation_preserves_keep_generation_and_restart_races() {
        for change in ["keep", "generation", "restart"] {
            let (_temp, inventory, workspace) = fixture();
            terminal(&inventory, &workspace, "completed");
            record(&inventory, &workspace);
            inventory.reconcile_workspace_retirements().unwrap();
            let key = format!("retire-{}", workspace.scratch.generation);
            let mut scratch = inventory.get(&workspace.scratch.id).unwrap();
            let mut guard = None;
            match change {
                "keep" => {
                    scratch.keep = true;
                    scratch.revision += 1;
                    save_artifact(&inventory.db.lock().unwrap(), &scratch).unwrap();
                }
                "generation" => {
                    scratch.generation = "replacement".into();
                    save_artifact(&inventory.db.lock().unwrap(), &scratch).unwrap();
                }
                "restart" => {
                    guard = inventory.acquire_attempt_use(12).unwrap();
                }
                _ => unreachable!(),
            }
            let _ = inventory.execute_workspace_retirement(&key);
            assert!(workspace.scratch.path.exists(), "{change}");
            drop(guard);
        }
    }
    #[test]
    fn interrupted_retirement_replays_only_its_captured_manifest() {
        let (temp, inventory, workspace) = fixture();
        terminal(&inventory, &workspace, "completed");
        record(&inventory, &workspace);
        inventory.reconcile_workspace_retirements().unwrap();
        let key = format!("retire-{}", workspace.scratch.generation);
        let mut op = inventory.operation(&key).unwrap();
        op.state = "running".into();
        op.attempts = 1;
        save_operation(&inventory.db.lock().unwrap(), &op).unwrap();
        std::fs::remove_file(workspace.scratch.path.join("scratch")).unwrap();
        drop(inventory);
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        assert_eq!(
            inventory.execute_workspace_retirement(&key).unwrap().state,
            "succeeded"
        );
        assert_eq!(
            inventory.execute_workspace_retirement(&key).unwrap().state,
            "succeeded"
        );
        assert!(workspace.source.path.join("original").exists());
    }
    #[test]
    fn bounded_cursor_progresses_past_held_workspaces() {
        let (_temp, inventory, workspace) = fixture();
        for i in 0..30 {
            let held = inventory
                .workspace(12, "par_repair", &format!("{i:04x}"))
                .unwrap();
            inventory.finish_workspace(&held).unwrap();
            let mut scratch = inventory.get(&held.scratch.id).unwrap();
            scratch.keep = true;
            save_artifact(&inventory.db.lock().unwrap(), &scratch).unwrap();
        }
        terminal(&inventory, &workspace, "completed");
        record(&inventory, &workspace);
        // First page is bounded; the later transform is not starved by that page.
        inventory.reconcile_workspace_retirements().unwrap();
        assert!(!inventory.workspace_cursor.lock().unwrap().is_empty());
        inventory.reconcile_workspace_retirements().unwrap();
        inventory.run_workspace_retirements().unwrap();
        assert!(!workspace.scratch.path.exists());
    }
    #[test]
    fn pending_history_evidence_cannot_retire_until_matching_stamp_is_confirmed() {
        let (temp, inventory, workspace) = fixture();
        terminal(&inventory, &workspace, "completed");
        let source = source(&workspace);
        let proof = FinalizationProof::Local { history_seq: 7 };
        inventory
            .stage_workspace_finalization(&source, "SUCCESS", &proof)
            .unwrap();
        inventory.reconcile_workspace_retirements().unwrap();
        inventory.run_workspace_retirements().unwrap();
        assert!(workspace.scratch.path.exists());
        drop(inventory);
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        let pending = inventory.staged_completions(12).unwrap();
        assert_eq!(pending.len(), 1);
        let wrong = StagedCompletion {
            source: source.clone(),
            outcome: "SUCCESS".into(),
            proof: FinalizationProof::Local { history_seq: 8 },
        };
        assert!(inventory.confirm_stamped_finalization(&wrong).is_err());
        inventory.confirm_stamped_finalization(&pending[0]).unwrap();
        inventory.reconcile_workspace_retirements().unwrap();
        inventory.run_workspace_retirements().unwrap();
        assert!(!workspace.scratch.path.exists());
    }
    #[cfg(unix)]
    #[test]
    fn cancelled_cross_volume_copy_and_publication_preserve_owned_source() {
        for cancellation_checkpoint in [3, 7] {
            let (temp, inventory, workspace) = fixture();
            std::fs::write(workspace.source.path.join("large"), vec![1; 200000]).unwrap();
            let target = temp.path().join("completed/job");
            let guard = inventory.acquire_attempt_use(12).unwrap().unwrap();
            fs::RENAME_FAILURE.with(|fault| fault.set(Some(libc::EXDEV)));
            let checks = std::cell::Cell::new(0);
            let result = inventory.relocate_checked(12, &target, &|| {
                checks.set(checks.get() + 1);
                if checks.get() == cancellation_checkpoint {
                    Err(std::io::ErrorKind::Interrupted.into())
                } else {
                    Ok(())
                }
            });
            assert!(result.is_err());
            assert!(!target.exists());
            assert_eq!(
                std::fs::read(workspace.source.path.join("large"))
                    .unwrap()
                    .len(),
                200000
            );
            assert!(inventory.processing_in_use(12).unwrap());
            assert!(inventory.acquire_attempt_use(12).is_err());
            drop(guard);
            fs::RENAME_FAILURE.with(|fault| fault.set(None));
        }
    }
    #[test]
    fn terminal_delete_cancelled_while_waiting_for_mutation_preserves_payload() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let (_temp, inventory, workspace) = fixture();
        let guard = inventory.acquire_attempt_use(12).unwrap().unwrap();
        let mutation = inventory.mutation_guard().unwrap();
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancelled = cancelled.clone();
        let worker_inventory = inventory.clone();
        let (started, waiting) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let first = std::cell::Cell::new(true);
            worker_inventory.delete_failed_payload(&guard, 12, &|| {
                if first.replace(false) {
                    started.send(()).unwrap();
                    return Ok(());
                }
                if worker_cancelled.load(Ordering::SeqCst) {
                    Err(std::io::ErrorKind::Interrupted.into())
                } else {
                    Ok(())
                }
            })
        });
        waiting
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        cancelled.store(true, Ordering::SeqCst);
        drop(mutation);
        assert!(worker.join().unwrap().is_err());
        assert!(workspace.source.path.join("original").exists());
        assert_eq!(inventory.for_job(12).unwrap().unwrap().state, "active");
        assert!(matches!(
            inventory.operation(&format!("failed-delete-{}", workspace.source.id)),
            Err(Error::NotFound)
        ));
    }
    #[test]
    fn terminal_delete_owner_fences_outside_deletion_until_actual_exit() {
        let (_temp, inventory, workspace) = fixture();
        let guard = inventory.acquire_attempt_use(12).unwrap().unwrap();
        terminal(&inventory, &workspace, "retained");
        let artifact = inventory.for_job(12).unwrap().unwrap();
        assert!(inventory
            .request_delete(&artifact.id, artifact.revision, "outsider", 0)
            .is_err());
        assert_eq!(
            inventory
                .delete_failed_payload(&guard, 12, &|| Ok(()))
                .unwrap()
                .state,
            "succeeded"
        );
        assert!(inventory.processing_in_use(12).unwrap());
        assert!(workspace.scratch.path.exists());
        drop(guard);
        inventory
            .finalize_workspaces(
                &source(&workspace),
                "PAR_FAILURE",
                FinalizationProof::Local { history_seq: 1 },
            )
            .unwrap();
        assert!(!workspace.scratch.path.exists());
    }
}
