//! Node-local durable controls for a leased worker's exact source generation.
use super::*;

/// Stable authority tuple. Lease renewal expiry is deliberately absent: a
/// renewal retains this work, while new leader intent or reassignment does not.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkerControlContext {
    pub lease_id: String,
    pub job_incarnation: String,
    pub job_revision: u64,
    pub control_revision: u64,
    pub epoch: u64,
}

pub const WORKER_CONTROL_CONTEXT_PARAM: &str = "*Cluster:ControlContext";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerControlCheckpoint {
    pub context: WorkerControlContext,
    pub source: SourceGeneration,
    pub control: nzbd_types::JobControl,
    pub probe_params: Vec<(String, String)>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerCompletionProof {
    pub accepted: bool,
    pub job: nzbd_types::JobId,
    pub context: WorkerControlContext,
    pub source: SourceGeneration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerRetryAdoption {
    pub source: Artifact,
    pub control: nzbd_types::JobControl,
    pub probe_params: Vec<(String, String)>,
    pub completed_job: Option<nzbd_types::Job>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WorkerCompletionCheckpoint {
    context: WorkerControlContext,
    source: SourceGeneration,
    job: nzbd_types::Job,
}

impl WorkerControlContext {
    pub fn for_job(job: &nzbd_types::Job) -> Option<Self> {
        let values = job
            .params
            .iter()
            .filter(|(key, _)| key == WORKER_CONTROL_CONTEXT_PARAM)
            .collect::<Vec<_>>();
        if values.len() != 1 || values[0].1.len() > 4096 {
            return None;
        }
        let context: Self = serde_json::from_str(&values[0].1).ok()?;
        if context.lease_id.is_empty()
            || context.lease_id.len() > 256
            || context.job_incarnation.is_empty()
            || context.job_incarnation.len() > 256
        {
            return None;
        }
        Some(context)
    }
}

impl Inventory {
    /// Persist selected-publication/terminal evidence before a volatile worker
    /// queue stamp. Call on a blocking preparation thread; descriptors are bulk.
    pub fn checkpoint_worker_completion(&self, job: &nzbd_types::Job) -> Result<()> {
        self.prepare_worker_completion(job).map(|_| ())
    }
    pub fn prepare_worker_completion(
        &self,
        job: &nzbd_types::Job,
    ) -> Result<WorkerCompletionProof> {
        let encoded_job = serde_json::to_string(job)?;
        if encoded_job.len() > 16 * 1024 * 1024 {
            return Err(Error::Conflict("worker completion exceeds supported 16 MiB descriptor limit; source retained for review".into()));
        }
        let context = WorkerControlContext::for_job(job)
            .ok_or_else(|| Error::Conflict("worker completion has no lease authority".into()))?;
        if !job.params.iter().any(|(key, _)| {
            key == nzbd_types::PP_DONE_PARAM || key == "*PP:selected-publication-outcome"
        }) {
            return Err(Error::Conflict(
                "worker completion has no durable publication outcome".into(),
            ));
        }
        let source = self.for_job(job.id.0)?.ok_or(Error::NotFound)?;
        if !source.owned {
            return Err(Error::Conflict(
                "worker completion cannot claim transferred bytes".into(),
            ));
        }
        if !source.terminal() {
            self.verify(&source)?;
        }
        let encoded = format!(
            "{{\"context\":{},\"source\":{},\"job\":{}}}",
            serde_json::to_string(&context)?,
            serde_json::to_string(&SourceGeneration {
                artifact: source.id.clone(),
                generation: source.generation.clone()
            })?,
            encoded_job
        );
        let _guard = self.mutation_guard()?;
        let current = self.for_job(job.id.0)?.ok_or(Error::NotFound)?;
        if current.id != source.id
            || current.generation != source.generation
            || current.path != source.path
            || !current.owned
        {
            return Err(Error::Conflict(
                "worker publication generation changed before checkpoint".into(),
            ));
        }
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let authority: String = tx.query_row(
            "SELECT data FROM worker_authority WHERE job=?1",
            [job.id.0],
            |row| row.get(0),
        )?;
        if serde_json::from_str::<WorkerControlContext>(&authority)? != context {
            return Err(Error::Conflict(
                "worker completion lease authority changed".into(),
            ));
        }
        let receipt_id = format!("{}:{}", source.id, source.generation);
        let accepted: Option<String> = tx
            .query_row(
                "SELECT data FROM worker_completion_receipts WHERE id=?1",
                [&receipt_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(accepted) = accepted {
            let (accepted_context, accepted_source, _): (
                WorkerControlContext,
                SourceGeneration,
                FinalizationProof,
            ) = serde_json::from_str(&accepted)?;
            if accepted_context != context
                || accepted_source.artifact != source.id
                || accepted_source.generation != source.generation
            {
                return Err(Error::Conflict(
                    "accepted worker completion identity changed".into(),
                ));
            }
            return Ok(WorkerCompletionProof {
                accepted: true,
                job: job.id,
                context,
                source: accepted_source,
            });
        }
        let previous: Option<String> = tx
            .query_row(
                "SELECT data FROM worker_completions WHERE job=?1",
                [job.id.0],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(previous) = previous {
            let previous: WorkerCompletionCheckpoint = serde_json::from_str(&previous)?;
            if previous.context == context
                && previous
                    .job
                    .params
                    .iter()
                    .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
                && !job
                    .params
                    .iter()
                    .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
            {
                return Err(Error::Conflict(
                    "terminal worker completion cannot regress".into(),
                ));
            }
        }
        tx.execute("INSERT INTO worker_completions(job,data) VALUES(?1,?2) ON CONFLICT(job) DO UPDATE SET data=excluded.data", params![job.id.0, encoded])?;
        tx.commit()?;
        Ok(WorkerCompletionProof {
            accepted: false,
            job: job.id,
            context,
            source: SourceGeneration {
                artifact: source.id,
                generation: source.generation,
            },
        })
    }

    pub fn worker_completion_accepted(
        &self,
        context: &WorkerControlContext,
        source: &SourceGeneration,
    ) -> Result<bool> {
        let id = format!("{}:{}", source.artifact, source.generation);
        let receipt: Option<String> = self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT data FROM worker_completion_receipts WHERE id=?1",
                [id],
                |row| row.get(0),
            )
            .optional()?;
        let Some(receipt) = receipt else {
            return Ok(false);
        };
        let (accepted_context, accepted_source, _): (
            WorkerControlContext,
            SourceGeneration,
            FinalizationProof,
        ) = serde_json::from_str(&receipt)?;
        if accepted_context != *context || accepted_source != *source {
            return Err(Error::Conflict(
                "accepted worker completion identity changed".into(),
            ));
        }
        Ok(true)
    }

    /// Compact the duplicate descriptor only after a durable cluster receipt
    /// and cleanup obligation exist. Keep the exact small acceptance proof.
    pub fn acknowledge_worker_completion(
        &self,
        job: u32,
        context: &WorkerControlContext,
        source: &SourceGeneration,
        proof: &FinalizationProof,
    ) -> Result<bool> {
        let FinalizationProof::Cluster {
            receipt,
            accepted_at_ms,
            ..
        } = proof
        else {
            return Err(Error::Conflict(
                "worker completion requires cluster acceptance".into(),
            ));
        };
        if receipt.is_empty() || *accepted_at_ms <= 0 {
            return Err(Error::Conflict(
                "worker completion acceptance proof is incomplete".into(),
            ));
        }
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let matches: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM worker_completions WHERE job=?1 AND json_extract(data,'$.context')=json(?2) AND json_extract(data,'$.source')=json(?3))", params![job, serde_json::to_string(context)?, serde_json::to_string(source)?], |row| row.get(0))?;
        let receipt_id = format!("{}:{}", source.artifact, source.generation);
        let encoded = serde_json::to_string(&(context, source, proof))?;
        let previous: Option<String> = tx
            .query_row(
                "SELECT data FROM worker_completion_receipts WHERE id=?1",
                [&receipt_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(previous) = previous {
            return if previous == encoded {
                Ok(true)
            } else {
                Err(Error::Conflict(
                    "worker completion acceptance identity changed".into(),
                ))
            };
        }
        if !matches {
            return Ok(false);
        }
        tx.execute(
            "INSERT INTO worker_completion_receipts(id,job,data) VALUES(?1,?2,?3)",
            params![receipt_id, job, encoded],
        )?;
        tx.execute("DELETE FROM worker_completions WHERE job=?1", [job])?;
        tx.commit()?;
        Ok(true)
    }

    /// Explicit adoption after the cluster validates a new lease. Only an
    /// unchanged authoritative job/control revision may continue retained work.
    pub fn adopt_worker_retry(
        &self,
        job: u32,
        context: &WorkerControlContext,
    ) -> Result<Option<WorkerRetryAdoption>> {
        let _guard = self.mutation_guard()?;
        let completion: Option<String> = self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT data FROM worker_completions WHERE job=?1",
                [job],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(raw) = completion {
            let mut checkpoint: WorkerCompletionCheckpoint = serde_json::from_str(&raw)?;
            let old = &checkpoint.context;
            if old.job_incarnation == context.job_incarnation
                && old.job_revision == context.job_revision
                && old.control_revision == context.control_revision
                && context.epoch >= old.epoch
            {
                let source = self.get(&checkpoint.source.artifact)?;
                if source.generation == checkpoint.source.generation
                    && source.owned
                    && source.job == Some(job)
                    && !self.artifact_in_use(&source)?
                {
                    if !source.terminal() {
                        self.verify(&source)?;
                    }
                    let mut db = self.db.lock().unwrap();
                    let tx = db.transaction()?;
                    let authority: Option<String> = tx
                        .query_row(
                            "SELECT data FROM worker_authority WHERE job=?1",
                            [job],
                            |row| row.get(0),
                        )
                        .optional()?;
                    let allowed = authority
                        .as_deref()
                        .map(serde_json::from_str::<WorkerControlContext>)
                        .transpose()?
                        .is_none_or(|current| {
                            current.job_incarnation == context.job_incarnation
                                && current.job_revision == context.job_revision
                                && current.control_revision == context.control_revision
                                && current.epoch <= context.epoch
                        });
                    if allowed {
                        let local: Option<String> = tx
                            .query_row(
                                "SELECT data FROM worker_controls WHERE job=?1",
                                [job],
                                |row| row.get(0),
                            )
                            .optional()?;
                        let mut probe_params = Vec::new();
                        if let Some(raw) = local {
                            let mut local: WorkerControlCheckpoint = serde_json::from_str(&raw)?;
                            if local.context == checkpoint.context
                                && local.source == checkpoint.source
                            {
                                let completed_revision = checkpoint
                                    .job
                                    .control()
                                    .and_then(|control| control.revision.parse::<u64>().ok())
                                    .unwrap_or(0);
                                let local_revision =
                                    local.control.revision.parse::<u64>().unwrap_or(0);
                                if local_revision >= completed_revision {
                                    checkpoint.job.set_control(&local.control);
                                    checkpoint.job.status = if local.control.lifecycle == "held"
                                        || local.control.manual_pause
                                    {
                                        nzbd_types::JobStatus::Paused
                                    } else {
                                        checkpoint.job.status
                                    };
                                    probe_params = local.probe_params.clone();
                                    local.context = context.clone();
                                    tx.execute(
                                        "UPDATE worker_controls SET data=?2 WHERE job=?1",
                                        params![job, serde_json::to_string(&local)?],
                                    )?;
                                }
                            }
                        }
                        checkpoint.context = context.clone();
                        checkpoint
                            .job
                            .params
                            .retain(|(key, _)| key != WORKER_CONTROL_CONTEXT_PARAM);
                        checkpoint.job.params.push((
                            WORKER_CONTROL_CONTEXT_PARAM.into(),
                            serde_json::to_string(context)?,
                        ));
                        tx.execute("INSERT INTO worker_authority(job,data) VALUES(?1,?2) ON CONFLICT(job) DO UPDATE SET data=excluded.data", params![job, serde_json::to_string(context)?])?;
                        tx.execute(
                            "UPDATE worker_completions SET data=?2 WHERE job=?1",
                            params![job, serde_json::to_string(&checkpoint)?],
                        )?;
                        tx.commit()?;
                        let control = checkpoint.job.control().unwrap_or(nzbd_types::JobControl {
                            version: 1,
                            revision: "0".into(),
                            lifecycle: "running".into(),
                            cause: String::new(),
                            stage: "move".into(),
                            retry_policy: "complete".into(),
                            message: "durable worker publication ready".into(),
                            instance: format!("completion-{job}"),
                            previous_status: Some(nzbd_types::JobStatus::PostQueued),
                            manual_pause: false,
                            retry_attempt: 0,
                            next_retry_at_unix: None,
                            blocked_since_unix: None,
                        });
                        return Ok(Some(WorkerRetryAdoption {
                            source,
                            control,
                            probe_params,
                            completed_job: Some(checkpoint.job),
                        }));
                    }
                }
            }
        }
        let raw: Option<String> = self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT data FROM worker_controls WHERE job=?1",
                [job],
                |row| row.get(0),
            )
            .optional()?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let mut checkpoint: WorkerControlCheckpoint = serde_json::from_str(&raw)?;
        let old = &checkpoint.context;
        if old.job_incarnation != context.job_incarnation
            || old.job_revision != context.job_revision
            || old.control_revision != context.control_revision
            || context.epoch < old.epoch
        {
            return Ok(None);
        }
        let source = self.get(&checkpoint.source.artifact)?;
        if source.generation != checkpoint.source.generation
            || !source.owned
            || source.terminal()
            || source.job != Some(job)
            || self.artifact_in_use(&source)?
        {
            return Ok(None);
        }
        self.verify(&source)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let current: Option<String> = tx
            .query_row(
                "SELECT data FROM worker_authority WHERE job=?1",
                [job],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(raw) = current {
            let current: WorkerControlContext = serde_json::from_str(&raw)?;
            if current.job_incarnation != context.job_incarnation
                || current.job_revision != context.job_revision
                || current.control_revision != context.control_revision
                || current.epoch > context.epoch
            {
                return Ok(None);
            }
        }
        checkpoint.context = context.clone();
        tx.execute("INSERT INTO worker_authority(job,data) VALUES(?1,?2) ON CONFLICT(job) DO UPDATE SET data=excluded.data", params![job, serde_json::to_string(context)?])?;
        tx.execute(
            "UPDATE worker_controls SET data=?2 WHERE job=?1",
            params![job, serde_json::to_string(&checkpoint)?],
        )?;
        tx.commit()?;
        Ok(Some(WorkerRetryAdoption {
            source,
            control: checkpoint.control,
            probe_params: checkpoint.probe_params,
            completed_job: None,
        }))
    }

    /// Bind an externally validated grant before publishing a volatile worker
    /// queue row. This prevents a later restart replaying an older checkpoint.
    pub fn bind_worker_context(&self, job: u32, context: &WorkerControlContext) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let db = self.db.lock().unwrap();
        let raw: Option<String> = db
            .query_row(
                "SELECT data FROM worker_authority WHERE job=?1",
                [job],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(raw) = raw {
            let previous: WorkerControlContext = serde_json::from_str(&raw)?;
            if previous.job_incarnation == context.job_incarnation
                && (context.epoch < previous.epoch
                    || context.job_revision < previous.job_revision
                    || context.control_revision < previous.control_revision)
            {
                return Err(Error::Conflict("stale worker grant authority".into()));
            }
        }
        db.execute("INSERT INTO worker_authority(job,data) VALUES(?1,?2) ON CONFLICT(job) DO UPDATE SET data=excluded.data", params![job, serde_json::to_string(context)?])?;
        Ok(())
    }

    pub fn checkpoint_worker_control(
        &self,
        job: &nzbd_types::Job,
        expected_revision: &str,
    ) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let context = WorkerControlContext::for_job(job)
            .ok_or_else(|| Error::Conflict("worker control has no lease authority".into()))?;
        let source = self.for_job(job.id.0)?.ok_or(Error::NotFound)?;
        if !source.owned || source.terminal() {
            return Err(Error::Conflict(
                "worker control requires an owned source generation".into(),
            ));
        }
        self.verify(&source)?;
        let control = job
            .control()
            .ok_or_else(|| Error::Conflict("worker control missing".into()))?;
        let source_generation = SourceGeneration {
            artifact: source.id,
            generation: source.generation,
        };
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let authority: String = tx.query_row(
            "SELECT data FROM worker_authority WHERE job=?1",
            [job.id.0],
            |row| row.get(0),
        )?;
        if serde_json::from_str::<WorkerControlContext>(&authority)? != context {
            return Err(Error::Conflict("worker grant authority changed".into()));
        }
        let raw: Option<String> = tx
            .query_row(
                "SELECT data FROM worker_controls WHERE job=?1",
                [job.id.0],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(raw) = raw {
            let previous: WorkerControlCheckpoint = serde_json::from_str(&raw)?;
            if previous.context == context
                && previous.source == source_generation
                && (previous.control.instance != control.instance
                    || previous.control.revision != expected_revision)
            {
                return Err(Error::Conflict("worker control revision changed".into()));
            }
        }
        let checkpoint = WorkerControlCheckpoint {
            context,
            source: source_generation,
            control,
            probe_params: job
                .params
                .iter()
                .filter(|(key, _)| {
                    matches!(key.as_str(), "*Control:attempts" | "*Control:probe_after")
                })
                .cloned()
                .collect(),
        };
        tx.execute("INSERT INTO worker_controls(job,data) VALUES(?1,?2) ON CONFLICT(job) DO UPDATE SET data=excluded.data", params![job.id.0, serde_json::to_string(&checkpoint)?])?;
        tx.commit()?;
        Ok(())
    }

    pub fn worker_control(
        &self,
        job: u32,
        context: &WorkerControlContext,
    ) -> Result<Option<WorkerControlCheckpoint>> {
        let raw: Option<String> = self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT data FROM worker_controls WHERE job=?1",
                [job],
                |row| row.get(0),
            )
            .optional()?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let checkpoint: WorkerControlCheckpoint = serde_json::from_str(&raw)?;
        if checkpoint.context != *context {
            return Ok(None);
        }
        let authority: Option<String> = self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT data FROM worker_authority WHERE job=?1",
                [job],
                |row| row.get(0),
            )
            .optional()?;
        if authority
            .as_deref()
            .map(serde_json::from_str::<WorkerControlContext>)
            .transpose()?
            .as_ref()
            != Some(context)
        {
            return Ok(None);
        }
        let Some(source) = self.for_job(job)? else {
            return Ok(None);
        };
        if source.id != checkpoint.source.artifact
            || source.generation != checkpoint.source.generation
            || !source.owned
            || source.terminal()
        {
            return Ok(None);
        }
        self.verify(&source)?;
        Ok(Some(checkpoint))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selected_publication_adoption_keeps_newer_retry_and_compacts_on_acceptance() {
        let temporary = tempfile::tempdir().unwrap();
        let state = temporary.path().join("state");
        let root = temporary.path().join("private");
        std::fs::create_dir(&root).unwrap();
        let inventory = Inventory::open(&state).unwrap();
        let path = root.join("source");
        inventory
            .allocate_for_delivery(42, &root, &path, true, true)
            .unwrap();
        std::fs::write(path.join("episode.mkv"), b"verified bytes").unwrap();
        inventory.finish(42, &path, &root, "active").unwrap();
        let context = WorkerControlContext {
            lease_id: "old-lease".into(),
            job_incarnation: "job-incarnation".into(),
            job_revision: 7,
            control_revision: 3,
            epoch: 1,
        };
        let mut job = nzbd_types::Job {
            id: nzbd_types::JobId(42),
            kind: nzbd_types::JobKind::Nzb,
            name: "publication".into(),
            dir_name: "source".into(),
            name_provisional: false,
            delivery_lifecycle: true,
            queued_at_unix: 1,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: Default::default(),
            params: vec![
                (
                    WORKER_CONTROL_CONTEXT_PARAM.into(),
                    serde_json::to_string(&context).unwrap(),
                ),
                ("*PP:selected-publication-outcome".into(), "SUCCESS".into()),
            ],
            files: Vec::new(),
            totals: Default::default(),
            status: nzbd_types::JobStatus::PostQueued,
            torrent: None,
            stages: Vec::new(),
        };
        inventory.bind_worker_context(42, &context).unwrap();
        inventory.checkpoint_worker_completion(&job).unwrap();
        let control = nzbd_types::JobControl {
            version: 1,
            revision: "2".into(),
            lifecycle: "held".into(),
            cause: "capacity".into(),
            stage: "move".into(),
            retry_policy: "scheduled_retry".into(),
            message: "retry in 300 seconds".into(),
            instance: "attempt-42".into(),
            previous_status: Some(nzbd_types::JobStatus::PostQueued),
            manual_pause: false,
            retry_attempt: 9,
            next_retry_at_unix: Some(999999),
            blocked_since_unix: Some(123),
        };
        job.set_control(&control);
        inventory.checkpoint_worker_control(&job, "1").unwrap();
        drop(inventory);
        let inventory = Inventory::open(&state).unwrap();
        let mut next = context;
        next.epoch = 2;
        next.lease_id = "new-lease".into();
        let adopted = inventory.adopt_worker_retry(42, &next).unwrap().unwrap();
        assert_eq!(adopted.control.next_retry_at_unix, Some(999999));
        assert_eq!(
            adopted
                .completed_job
                .as_ref()
                .unwrap()
                .control()
                .unwrap()
                .retry_attempt,
            9
        );
        assert_eq!(
            adopted.completed_job.as_ref().unwrap().status,
            nzbd_types::JobStatus::Paused
        );
        let late = adopted.completed_job.as_ref().unwrap().clone();
        let source = SourceGeneration {
            artifact: adopted.source.id,
            generation: adopted.source.generation,
        };
        let proof = FinalizationProof::Cluster {
            receipt: "accepted-copy-receipt".into(),
            accepted_at_ms: 1000,
        };
        assert!(inventory
            .acknowledge_worker_completion(42, &next, &source, &proof)
            .unwrap());
        assert!(inventory
            .acknowledge_worker_completion(42, &next, &source, &proof)
            .unwrap());
        inventory.prepare_worker_completion(&late).unwrap();
        let count: i64 = inventory
            .db
            .lock()
            .unwrap()
            .query_row("SELECT count(*) FROM worker_completions", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "accepted full descriptors must not accumulate");
        let count: i64 = inventory
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT count(*) FROM worker_completion_receipts",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "small exact acceptance proof survives compaction");
    }
}
