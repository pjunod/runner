//! The consumer's delete receipt is separate from the download's terminal.
use super::*;

/// The exact consumer admission owning a newly allocated source.
#[derive(Clone, Debug)]
pub struct JobAdmissionBinding {
    pub job_id: u32,
    pub transfer: String,
    pub instance: String,
    pub generation: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobDispositionRequest {
    pub request_id: String,
    pub action: JobDispositionAction,
    pub transfer: String,
    pub runner_instance_id: String,
    pub generation: String,
    #[serde(default)]
    pub source_artifact_id: Option<String>,
    #[serde(default)]
    pub source_generation: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobDispositionAction {
    Delete,
}

impl JobDispositionRequest {
    pub fn valid(&self) -> bool {
        fn valid(value: &str, max: usize) -> bool {
            !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
        }
        valid(&self.request_id, 128)
            && valid(&self.transfer, 1024)
            && valid(&self.runner_instance_id, 128)
            && valid(&self.generation, 256)
            && match (&self.source_artifact_id, &self.source_generation) {
                (Some(id), Some(generation)) => valid(id, 256) && valid(generation, 256),
                (None, None) => true,
                _ => false,
            }
    }

    pub fn source(&self) -> Option<SourceGeneration> {
        Some(SourceGeneration {
            artifact: self.source_artifact_id.clone()?,
            generation: self.source_generation.clone()?,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobDispositionState {
    Pending,
    Blocked,
    Verified,
}

#[derive(Clone, Debug)]
pub struct JobDisposition {
    pub request: JobDispositionRequest,
    pub job_id: u32,
    pub state: JobDispositionState,
    pub reason: Option<String>,
    pub operation_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JobDispositionRecord {
    pub request: JobDispositionRequest,
    pub job_id: u32,
    pub source: Option<SourceGeneration>,
    pub stopped: bool,
    pub state: JobDispositionState,
    pub reason: Option<String>,
    pub operation_id: Option<String>,
}

impl JobDispositionRecord {
    pub fn status(&self) -> JobDisposition {
        JobDisposition {
            request: self.request.clone(),
            job_id: self.job_id,
            state: self.state,
            reason: self.reason.clone(),
            operation_id: self.operation_id.clone(),
        }
    }
}

impl Inventory {
    pub fn bind_job_disposition_source(
        &self,
        binding: &JobAdmissionBinding,
        artifact: &Artifact,
    ) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let current = self.get(&artifact.id)?;
        if current.job != Some(binding.job_id)
            || current.generation != artifact.generation
            || !current.owned
            || current.terminal()
        {
            return Err(Error::Conflict(
                "source differs from its owning admission".into(),
            ));
        }
        let db = self.db.lock().unwrap();
        let existing: Option<(u32, String, String, String)> = db.query_row(
            "SELECT job,transfer,instance,admission_generation FROM job_disposition_sources WHERE artifact=?1 AND source_generation=?2",
            params![artifact.id, artifact.generation],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        ).optional()?;
        if let Some((job, transfer, instance, generation)) = existing {
            if job == binding.job_id
                && transfer == binding.transfer
                && instance == binding.instance
                && generation == binding.generation
            {
                return Ok(());
            }
            return Err(Error::Conflict(
                "source was allocated to another admission".into(),
            ));
        }
        db.execute(
            "INSERT INTO job_disposition_sources(artifact,source_generation,job,transfer,instance,admission_generation)
             VALUES(?1,?2,?3,?4,?5,?6)",
            params![artifact.id, artifact.generation, binding.job_id, binding.transfer, binding.instance, binding.generation],
        )?;
        Ok(())
    }

    pub fn job_disposition_source(
        &self,
        binding: &JobAdmissionBinding,
    ) -> Result<Option<SourceGeneration>> {
        let db = self.db.lock().unwrap();
        let mut statement = db.prepare(
            "SELECT artifact,source_generation FROM job_disposition_sources
             WHERE job=?1 AND transfer=?2 AND instance=?3 AND admission_generation=?4 LIMIT 2",
        )?;
        let rows = statement
            .query_map(
                params![
                    binding.job_id,
                    binding.transfer,
                    binding.instance,
                    binding.generation
                ],
                |row| {
                    Ok(SourceGeneration {
                        artifact: row.get(0)?,
                        generation: row.get(1)?,
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        match rows.as_slice() {
            [] => Ok(None),
            [source] => Ok(Some(source.clone())),
            _ => Err(Error::Conflict(
                "this admission has more than one original source".into(),
            )),
        }
    }

    pub fn finish_job_disposition_source(
        &self,
        source: &SourceGeneration,
        job: &nzbd_types::Job,
        writer_files: &[(String, Identity)],
    ) -> Result<()> {
        let mut artifact = self.get(&source.artifact)?;
        if artifact.generation != source.generation || artifact.job.is_some_and(|id| id != job.id.0)
        {
            return Err(Error::Conflict(
                "source generation changed before writer retirement".into(),
            ));
        }
        if !matches!(artifact.state.as_str(), "active" | "retiring") {
            return Ok(());
        }
        if artifact.job != Some(job.id.0) {
            return Err(Error::Conflict(
                "active source owner changed before writer retirement".into(),
            ));
        }
        let path = artifact.path.clone();
        let root = artifact.root.clone();
        let torrent = job.kind == nzbd_types::JobKind::Torrent;
        let mut owned: std::collections::HashMap<_, _> = writer_files
            .iter()
            .map(|(path, identity)| (path.clone(), identity.clone()))
            .collect();
        owned.extend(
            artifact
                .files
                .iter()
                .map(|file| (file.path.clone(), file.identity.clone())),
        );
        if torrent {
            owned.extend(self.torrent_disposition_copy_entries(job.id.0, source)?);
        }
        let captured =
            self.finish_artifact_checked(artifact.clone(), &path, &root, "retained", |files| {
                if files.iter().any(|file| {
                    !owned.get(&file.path).is_some_and(|identity| {
                        if file.identity.directory {
                            identity.same_object(&file.identity)
                        } else {
                            identity == &file.identity
                        }
                    })
                }) {
                    return Err(Error::Conflict(
                        "source contains entries without recorded writer identity".into(),
                    ));
                }
                Ok(())
            });
        match captured {
            Ok(_) => Ok(()),
            Err(Error::Conflict(reason)) => {
                let _guard = self.mutation_guard()?;
                self.check_payload_capture(&artifact)?;
                artifact.state = "retained".into();
                artifact.hold = Some("review".into());
                artifact.error = Some(reason.clone());
                artifact.revision += 1;
                artifact.updated_at = now();
                let mut db = self.db.lock().unwrap();
                let tx = db.transaction()?;
                save_artifact(&tx, &artifact)?;
                event(&tx, &artifact.id, "disposition_review", &reason)?;
                tx.commit()?;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    pub fn job_disposition(&self, request_id: &str) -> Result<Option<JobDispositionRecord>> {
        let db = self.db.lock().unwrap();
        let data: Option<String> = db
            .query_row(
                "SELECT data FROM job_dispositions WHERE id=?1",
                [request_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(data.map(|raw| serde_json::from_str(&raw)).transpose()?)
    }

    pub fn job_disposition_fences(&self) -> Result<Vec<(u32, String, String)>> {
        let db = self.db.lock().unwrap();
        let mut statement =
            db.prepare("SELECT job,transfer,generation FROM job_dispositions WHERE stopped=0")?;
        let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn admit_job_disposition(
        &self,
        job: u32,
        request: JobDispositionRequest,
        source: Option<SourceGeneration>,
    ) -> Result<JobDispositionRecord> {
        let _guard = self.mutation_guard()?;
        if let Some(existing) = self.job_disposition(&request.request_id)? {
            if existing.job_id != job || existing.request != request {
                return Err(Error::Conflict(
                    "disposition request ID has a different binding".into(),
                ));
            }
            return Ok(existing);
        }
        let receipt = JobDispositionRecord {
            request,
            job_id: job,
            source,
            stopped: false,
            state: JobDispositionState::Pending,
            reason: None,
            operation_id: None,
        };
        let rows = self.db.lock().unwrap().execute(
            "INSERT INTO job_dispositions(id,job,transfer,generation,stopped,data)
             VALUES(?1,?2,?3,?4,0,?5) ON CONFLICT(job,transfer,generation) DO NOTHING",
            params![
                receipt.request.request_id,
                job,
                receipt.request.transfer,
                receipt.request.generation,
                serde_json::to_string(&receipt)?
            ],
        )?;
        if rows != 1 {
            return Err(Error::Conflict(
                "this admission already has another disposition request".into(),
            ));
        }
        Ok(receipt)
    }

    pub fn save_job_disposition(&self, receipt: &JobDispositionRecord) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let Some(current) = self.job_disposition(&receipt.request.request_id)? else {
            return Err(Error::NotFound);
        };
        if current.job_id != receipt.job_id
            || current.request != receipt.request
            || (current.source != receipt.source
                && !self.disposition_source_moved(&current, receipt)?)
            || (current.stopped && !receipt.stopped)
            || (current.operation_id.is_some() && current.operation_id != receipt.operation_id)
        {
            return Err(Error::Conflict("disposition binding changed".into()));
        }
        if current.state != JobDispositionState::Verified {
            self.db.lock().unwrap().execute(
                "UPDATE job_dispositions SET stopped=?2,data=?3 WHERE id=?1",
                params![
                    receipt.request.request_id,
                    receipt.stopped,
                    serde_json::to_string(receipt)?
                ],
            )?;
        }
        Ok(())
    }

    fn disposition_source_moved(
        &self,
        previous: &JobDispositionRecord,
        next: &JobDispositionRecord,
    ) -> Result<bool> {
        let Some(source) = next.source.as_ref() else {
            return Ok(false);
        };
        if previous.operation_id.is_some() {
            return Ok(false);
        }
        if previous.source.is_none() && previous.request.source().is_none() && !previous.stopped {
            return Ok(true);
        }
        // A cut-over may turn the original source into the consumer's
        // publication and record a new exact Runner-owned copy. Absent
        // independent evidence for both sides, keep the original binding.
        let Some(original) = previous.source.as_ref() else {
            return Ok(false);
        };
        if self
            .disposition_delivery_source(
                next.job_id,
                &next.request.transfer,
                &next.request.runner_instance_id,
            )?
            .as_ref()
            != Some(source)
        {
            return Ok(false);
        }
        let original = self.get(&original.artifact)?;
        if original.terminal() && original.gone_reason.is_none() {
            return Ok(true);
        }
        let id = format!("{}:{}", original.id, original.generation);
        match self.delivery(&id) {
            Ok(delivery) => Ok(delivery.job_id == next.job_id
                && delivery.request_token.as_deref() == Some(&next.request.transfer)
                && delivery.runner_instance_id == next.request.runner_instance_id),
            Err(Error::NotFound) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Exact publication rows remain with the consumer. Other artifact rows
    /// that cannot be joined to the request's source are unresolved.
    fn disposition_unbound_copy(&self, receipt: &JobDispositionRecord) -> Result<bool> {
        let db = self.db.lock().unwrap();
        Ok(db.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM artifacts a WHERE
                    (a.job=?1 OR a.id IN (
                        SELECT 'source-'||o.id FROM operations o JOIN artifacts p ON p.id=o.artifact
                        WHERE p.job=?1 AND json_extract(o.data,'$.kind')='relocate'
                    ))
                    AND a.id<>?2 AND a.state<>'deleted'
                    AND NOT EXISTS (
                        SELECT 1 FROM deliveries d WHERE d.artifact=a.id
                        AND d.generation=json_extract(a.data,'$.generation')
                        AND json_extract(d.data,'$.requestToken')=?3
                        AND json_extract(d.data,'$.runnerInstanceId')=?4
                    )
            )",
            params![
                receipt.job_id,
                receipt.source.as_ref().map_or("", |s| &s.artifact),
                receipt.request.transfer,
                receipt.request.runner_instance_id
            ],
            |row| row.get(0),
        )?)
    }

    /// A source referred to by a delivery may be the publication itself on
    /// a same-filesystem handoff. Its exact receipt is the custody proof.
    pub fn disposition_delivery_source(
        &self,
        job: u32,
        transfer: &str,
        instance: &str,
    ) -> Result<Option<SourceGeneration>> {
        let db = self.db.lock().unwrap();
        let mut statement = db.prepare(
            "SELECT DISTINCT json_extract(data,'$.sourceArtifactId'),json_extract(data,'$.sourceGeneration')
             FROM deliveries WHERE json_extract(data,'$.jobId')=?1
             AND json_extract(data,'$.requestToken')=?2
             AND json_extract(data,'$.runnerInstanceId')=?3 LIMIT 2",
        )?;
        let rows = statement
            .query_map(params![job, transfer, instance], |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        match rows.as_slice() {
            [] => Ok(None),
            [(Some(artifact), Some(generation))]
                if !artifact.is_empty() && !generation.is_empty() =>
            {
                Ok(Some(SourceGeneration {
                    artifact: artifact.clone(),
                    generation: generation.clone(),
                }))
            }
            _ => Err(Error::Conflict(
                "publication sources require exact ownership review".into(),
            )),
        }
    }

    pub(super) fn disposition_keep_override(&self, id: &str, artifact: &Artifact) -> Result<bool> {
        let Some(receipt) = self.job_disposition(id)? else {
            return Ok(false);
        };
        let exact = SourceGeneration {
            artifact: artifact.id.clone(),
            generation: artifact.generation.clone(),
        };
        if !receipt.stopped
            || receipt.source.as_ref() != Some(&exact)
            || receipt.request.source().as_ref() != Some(&exact)
            || artifact.state == "transferred"
            || !artifact.owned
        {
            return Ok(false);
        }
        let db = self.db.lock().unwrap();
        Ok(!db.query_row(
            "SELECT EXISTS(SELECT 1 FROM deliveries WHERE artifact=?1 AND generation=?2)",
            params![artifact.id, artifact.generation],
            |row| row.get::<_, bool>(0),
        )?)
    }

    pub fn settle_job_disposition(
        &self,
        mut receipt: JobDispositionRecord,
    ) -> Result<JobDisposition> {
        if !receipt.stopped || receipt.state == JobDispositionState::Verified {
            return Ok(receipt.status());
        }
        if let Some(source) = self.disposition_delivery_source(
            receipt.job_id,
            &receipt.request.transfer,
            &receipt.request.runner_instance_id,
        )? {
            if receipt.source.as_ref() != Some(&source) && receipt.operation_id.is_none() {
                let mut updated = receipt.clone();
                updated.source = Some(source);
                if self.disposition_source_moved(&receipt, &updated)? {
                    self.save_job_disposition(&updated)?;
                    receipt = updated;
                }
            }
        }
        let (state, reason) = match self.advance_disposition_source(&mut receipt) {
            Ok(state) => state,
            Err(Error::Conflict(reason) | Error::CleanupBlocked(reason)) => {
                (JobDispositionState::Blocked, Some(reason))
            }
            Err(error) => return Err(error),
        };
        receipt.state = state;
        receipt.reason = reason;
        self.save_job_disposition(&receipt)?;
        Ok(receipt.status())
    }

    fn advance_disposition_source(
        &self,
        receipt: &mut JobDispositionRecord,
    ) -> Result<(JobDispositionState, Option<String>)> {
        use JobDispositionState::*;
        if self.open_intent_for_job(receipt.job_id)? {
            return Ok((
                Pending,
                Some("publication movement is still reconciling".into()),
            ));
        }
        let terminal = self.terminal(receipt.job_id)?;
        if terminal
            .as_ref()
            .is_some_and(|terminal| terminal.transfer.as_deref() != Some(&receipt.request.transfer))
        {
            return Err(Error::Conflict(
                "terminal belongs to another transfer".into(),
            ));
        }
        if terminal.as_ref().is_some_and(|terminal| {
            !terminal.finished() || terminal.salvage == SalvageState::Pending
        }) {
            return Ok((
                Pending,
                Some("failed-source salvage is still reconciling".into()),
            ));
        }
        if self.disposition_unbound_copy(receipt)? {
            return Err(Error::Conflict(
                "an additional source copy requires ownership review".into(),
            ));
        }
        let Some(binding) = &receipt.source else {
            if terminal.as_ref().is_some_and(|terminal| {
                terminal.publication.is_none()
                    && (terminal.outcome == nzbd_types::TerminalOutcome::Cancelled
                        || (terminal.fetched.success_articles == 0
                            && matches!(
                                terminal.stage.as_deref(),
                                Some("fetch" | "torrent_metadata")
                            )
                            && terminal.fetch_cause.is_some()))
            }) {
                return Ok((Verified, None));
            }
            return Err(Error::Conflict(
                "retired admission has no exact source or absence proof".into(),
            ));
        };
        let artifact = match self.get(&binding.artifact) {
            Ok(artifact) if artifact.generation == binding.generation => artifact,
            Ok(_) | Err(Error::NotFound) => {
                return Err(Error::Conflict(
                    "the recorded source generation is unavailable".into(),
                ))
            }
            Err(error) => return Err(error),
        };
        if let Some(terminal) = terminal
            .as_ref()
            .filter(|terminal| terminal.outcome == nzbd_types::TerminalOutcome::Cancelled)
        {
            let mut entry = terminal.entry.clone();
            entry.seq = terminal.history_seq.unwrap_or(0);
            if entry
                .params
                .iter()
                .any(|(key, value)| key == "Artifact:Id" && value == &binding.artifact)
                && entry.params.iter().any(|(key, value)| {
                    key == "Artifact:Generation" && value == &binding.generation
                })
            {
                self.finalize_deleted_workspaces(&entry)?;
            }
        }
        if self.disposition_workspace_pending(binding)? {
            return Ok((
                Pending,
                Some("the source's post-processing workspace is still retiring".into()),
            ));
        }
        let publication_id = format!("{}:{}", binding.artifact, binding.generation);
        match self.delivery(&publication_id) {
            Ok(delivery)
                if delivery.artifact_id == binding.artifact
                    && delivery.publication_generation == binding.generation
                    && delivery.job_id == receipt.job_id
                    && delivery.request_token.as_deref() == Some(&receipt.request.transfer)
                    && delivery.runner_instance_id == receipt.request.runner_instance_id =>
            {
                return Ok((Verified, None));
            }
            Ok(_) => {
                return Err(Error::Conflict(
                    "another publication owns the recorded source".into(),
                ))
            }
            Err(Error::NotFound) => {}
            Err(error) => return Err(error),
        }
        match artifact.state.as_str() {
            "deleted" => {
                if let Some(stages) = self.torrent_disposition_stages(receipt.job_id, binding)? {
                    return match stages.state.as_str() {
                        "succeeded" => Ok((Verified, None)),
                        "review" => Ok((Blocked, stages.error)),
                        _ => Ok((
                            Pending,
                            stages
                                .error
                                .or(Some("torrent staging cleanup is pending".into())),
                        )),
                    };
                }
                return Ok((Verified, None));
            }
            "source_gone"
                if artifact.gone_reason.is_none()
                    && artifact.path.parent() == Some(artifact.root.as_path()) =>
            {
                let root = self.verify_root(&artifact)?;
                if let Some(name) = artifact.path.file_name() {
                    if matches!(fs::open_at(&root, Path::new(name), true),
                        Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound)
                    {
                        return Ok((Verified, None));
                    }
                }
                return Err(Error::Conflict(
                    "the recorded source absence could not be verified".into(),
                ));
            }
            "source_gone" => {
                return Err(Error::Conflict(
                    "the former source mount needs review".into(),
                ))
            }
            _ => {}
        }
        if let Some(hold) = &artifact.hold {
            return Ok((
                Blocked,
                Some(
                    artifact
                        .error
                        .clone()
                        .unwrap_or_else(|| format!("source ownership hold: {hold}")),
                ),
            ));
        }
        if artifact.keep && receipt.request.source().as_ref() != Some(binding) {
            return Ok((Blocked, Some("Runner Keep covers this source. Release it in Runner Files after reviewing the generation.".into())));
        }
        if let Some(id) = &receipt.operation_id {
            let operation = self.operation(id)?;
            if !disposition_operation_matches(&operation, binding)? {
                return Err(Error::Conflict(
                    "the recorded file operation has a different binding".into(),
                ));
            }
            return match operation.state.as_str() {
                "queued" | "running" | "retry" => Ok((Pending, operation.error)),
                _ => Ok((
                    Blocked,
                    operation
                        .error
                        .or(Some("source deletion requires review".into())),
                )),
            };
        }
        // Derive the operation key before writing it. If a crash separates
        // the operation insert from the receipt update, the next poll finds it.
        use sha2::{Digest, Sha256};
        let key = format!(
            "consumer-delete-{:x}",
            Sha256::digest(receipt.request.request_id.as_bytes())
        );
        let operation = match self.operation(&key) {
            Ok(operation) => operation,
            Err(Error::NotFound) => {
                self.request_delete(&artifact.id, artifact.revision, &key, 0)?
            }
            Err(error) => return Err(error),
        };
        if !disposition_operation_matches(&operation, binding)? {
            return Err(Error::Conflict(
                "the file operation has a different binding".into(),
            ));
        }
        receipt.operation_id = Some(operation.id);
        Ok((Pending, None))
    }

    fn disposition_workspace_pending(&self, source: &SourceGeneration) -> Result<bool> {
        let db = self.db.lock().unwrap();
        Ok(db.query_row(
            "SELECT EXISTS(SELECT 1 FROM operations o JOIN artifacts a
             WHERE json_extract(o.data,'$.kind') IN ('extract','par_repair')
             AND a.id=json_extract(json_extract(o.data,'$.request'),'$.scratch.id')
             AND json_extract(json_extract(o.data,'$.request'),'$.source.id')=?1
             AND json_extract(json_extract(o.data,'$.request'),'$.source.generation')=?2
             AND a.state NOT IN ('deleted','source_gone','transferred'))",
            params![source.artifact, source.generation],
            |row| row.get(0),
        )?)
    }
}

fn disposition_operation_matches(operation: &Operation, source: &SourceGeneration) -> Result<bool> {
    if operation.kind != "delete" || operation.artifact != source.artifact {
        return Ok(false);
    }
    let request: DeleteRequest = serde_json::from_str(&operation.request)?;
    let (artifact, _, _, _, generation) = request.fields();
    Ok(artifact == source.artifact && generation == Some(source.generation.as_str()))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn request(inventory: &Inventory, key: &str) -> JobDispositionRequest {
        JobDispositionRequest {
            request_id: key.into(),
            action: JobDispositionAction::Delete,
            transfer: "transfer-a".into(),
            runner_instance_id: inventory.installation.clone(),
            generation: "admission-a".into(),
            source_artifact_id: None,
            source_generation: None,
        }
    }

    #[test]
    fn disposition_waits_for_stop_and_resumes_the_exact_artifact_operation() {
        let folder = tempfile::tempdir().unwrap();
        let state = folder.path().join("state");
        let root = folder.path().join("incomplete");
        std::fs::create_dir(&root).unwrap();
        let path = root.join("episode");
        let inventory = Inventory::open(&state).unwrap();
        let artifact = inventory.allocate(41, &root, &path).unwrap();
        std::fs::write(path.join("episode.mkv"), b"owned contents").unwrap();
        let artifact = inventory.finish(41, &path, &root, "retained").unwrap();
        let source = SourceGeneration {
            artifact: artifact.id,
            generation: artifact.generation,
        };
        let request = request(&inventory, "delete-a");
        let mut receipt = inventory
            .admit_job_disposition(41, request.clone(), Some(source))
            .unwrap();
        assert_eq!(
            inventory.job_disposition_fences().unwrap(),
            [(41, "transfer-a".into(), "admission-a".into())]
        );
        let awaiting_stop = inventory.settle_job_disposition(receipt.clone()).unwrap();
        assert_eq!(awaiting_stop.state, JobDispositionState::Pending);
        assert!(awaiting_stop.operation_id.is_none());
        assert!(path.join("episode.mkv").exists());
        receipt.stopped = true;
        inventory.save_job_disposition(&receipt).unwrap();
        let started = inventory.settle_job_disposition(receipt).unwrap();
        let operation_id = started.operation_id.unwrap();
        assert_eq!(inventory.operation(&operation_id).unwrap().state, "queued");
        drop(inventory);

        let inventory = Inventory::open(&state).unwrap();
        let receipt = inventory.job_disposition("delete-a").unwrap().unwrap();
        assert_eq!(receipt.request, request);
        assert!(inventory.job_disposition_fences().unwrap().is_empty());
        assert_eq!(
            inventory.execute_delete(&operation_id).unwrap().state,
            "succeeded"
        );
        let settled = inventory.settle_job_disposition(receipt).unwrap();
        assert_eq!(settled.state, JobDispositionState::Verified);
        assert_eq!(settled.operation_id.as_deref(), Some(operation_id.as_str()));
        assert!(!path.exists());
    }

    #[test]
    fn disposition_rejects_reused_request_and_source_bindings() {
        let folder = tempfile::tempdir().unwrap();
        let inventory = Inventory::open(&folder.path().join("state")).unwrap();
        let request = request(&inventory, "delete-b");
        inventory
            .admit_job_disposition(42, request.clone(), None)
            .unwrap();
        let mut changed = request.clone();
        changed.transfer = "successor".into();
        assert!(matches!(
            inventory.admit_job_disposition(42, changed, None),
            Err(Error::Conflict(_))
        ));
        let mut changed = request;
        changed.request_id = "another-request".into();
        assert!(matches!(
            inventory.admit_job_disposition(42, changed, None),
            Err(Error::Conflict(_))
        ));
    }

    #[test]
    fn disposition_only_overrides_keep_for_a_confirmed_source_generation() {
        let folder = tempfile::tempdir().unwrap();
        let root = folder.path().join("incomplete");
        std::fs::create_dir(&root).unwrap();
        let inventory = Inventory::open(&folder.path().join("state")).unwrap();
        for (job, exact) in [(44, false), (45, true)] {
            let path = root.join(job.to_string());
            inventory.allocate(job, &root, &path).unwrap();
            std::fs::write(path.join("owned.mkv"), b"owned").unwrap();
            let artifact = inventory.finish(job, &path, &root, "retained").unwrap();
            let kept = inventory
                .retention(&artifact.id, artifact.revision, true, None)
                .unwrap();
            let source = SourceGeneration {
                artifact: kept.id,
                generation: kept.generation,
            };
            let mut request = request(&inventory, &format!("delete-kept-{job}"));
            if exact {
                request.source_artifact_id = Some(source.artifact.clone());
                request.source_generation = Some(source.generation.clone());
            }
            let mut receipt = inventory
                .admit_job_disposition(job, request, Some(source))
                .unwrap();
            receipt.stopped = true;
            inventory.save_job_disposition(&receipt).unwrap();
            let status = inventory.settle_job_disposition(receipt).unwrap();
            if exact {
                assert_eq!(
                    inventory
                        .execute_delete(&status.operation_id.unwrap())
                        .unwrap()
                        .state,
                    "succeeded"
                );
                assert!(!path.exists());
            } else {
                assert_eq!(status.state, JobDispositionState::Blocked);
                assert!(path.join("owned.mkv").exists());
            }
        }
    }

    #[test]
    fn disposition_preserves_files_when_a_bound_manifest_conflicts() {
        let folder = tempfile::tempdir().unwrap();
        let root = folder.path().join("incomplete");
        std::fs::create_dir(&root).unwrap();
        let path = root.join("title");
        let inventory = Inventory::open(&folder.path().join("state")).unwrap();
        inventory.allocate(43, &root, &path).unwrap();
        std::fs::write(path.join("original.mkv"), b"owned").unwrap();
        let artifact = inventory.finish(43, &path, &root, "retained").unwrap();
        let source = SourceGeneration {
            artifact: artifact.id,
            generation: artifact.generation,
        };
        let request = request(&inventory, "delete-c");
        let mut receipt = inventory
            .admit_job_disposition(43, request, Some(source))
            .unwrap();
        receipt.stopped = true;
        inventory.save_job_disposition(&receipt).unwrap();
        let started = inventory.settle_job_disposition(receipt).unwrap();
        std::fs::write(path.join("unrelated.txt"), b"foreign").unwrap();

        let operation = inventory
            .execute_delete(&started.operation_id.unwrap())
            .unwrap();
        assert_eq!(operation.state, "review");
        let receipt = inventory.job_disposition("delete-c").unwrap().unwrap();
        assert_eq!(
            inventory.settle_job_disposition(receipt).unwrap().state,
            JobDispositionState::Blocked
        );
        assert_eq!(std::fs::read(path.join("original.mkv")).unwrap(), b"owned");
        assert_eq!(
            std::fs::read(path.join("unrelated.txt")).unwrap(),
            b"foreign"
        );
    }
}
