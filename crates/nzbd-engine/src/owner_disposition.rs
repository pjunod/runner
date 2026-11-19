//! Admit destructive consumer requests in the same lane that owns job identity.
use super::*;
use crate::EngineError;
use nzbd_state::artifacts::{
    Error, Inventory, JobAdmissionBinding, JobDisposition, JobDispositionRecord,
    JobDispositionRequest, JobDispositionState, SalvageState, SourceGeneration,
};

type Reply = oneshot::Sender<Result<JobDisposition, EngineError>>;

fn engine_error(error: Error) -> EngineError {
    match error {
        Error::Conflict(reason) => EngineError::RequestConflict(reason),
        error => EngineError::Lifecycle(error.to_string()),
    }
}

fn job_transfer(job: &Job) -> Option<&str> {
    job.params
        .iter()
        .find_map(|(key, value)| (key == "monarr-transfer").then_some(value.as_str()))
}

fn source_for_request(
    inventory: &Inventory,
    request: &JobDispositionRequest,
    admission: &UrlAdmissionReceipt,
    live_proof: Option<SourceGeneration>,
) -> Result<Option<SourceGeneration>, Error> {
    let delivery = inventory.disposition_delivery_source(
        admission.job_id,
        &request.transfer,
        &request.runner_instance_id,
    )?;
    // A delivery is authoritative for the exact post-move source. Older
    // snapshots without a pinned receipt can use a current queue allocation.
    let pinned = match (&admission.source_artifact_id, &admission.source_generation) {
        (Some(artifact), Some(generation)) => Some(SourceGeneration {
            artifact: artifact.clone(),
            generation: generation.clone(),
        }),
        (None, None) => None,
        _ => {
            return Err(Error::Conflict(
                "admission source binding is incomplete".into(),
            ))
        }
    };
    let allocated = inventory.job_disposition_source(&JobAdmissionBinding {
        job_id: admission.job_id,
        transfer: admission.request_token.clone(),
        instance: admission.runner_instance_id.clone(),
        generation: admission.generation.clone(),
    })?;
    // A numeric inventory lookup is deliberately insufficient. Old records
    // can only supply a pin when a matching terminal independently exists.
    let terminal = inventory.terminal(admission.job_id)?;
    let history_source = pinned.filter(|source| {
        terminal.as_ref().is_some_and(|terminal| {
            terminal.transfer.as_deref() == Some(&request.transfer)
                && terminal
                    .entry
                    .params
                    .iter()
                    .any(|(k, v)| k == "Artifact:Id" && v == &source.artifact)
                && terminal
                    .entry
                    .params
                    .iter()
                    .any(|(k, v)| k == "Artifact:Generation" && v == &source.generation)
        })
    });
    let source = delivery.or(allocated).or(history_source).or(live_proof);
    if request
        .source()
        .is_some_and(|expected| source.as_ref() != Some(&expected))
    {
        return Err(Error::Conflict(
            "the requested source generation does not match the admission".into(),
        ));
    }
    if let Some(source) = &source {
        let artifact = inventory
            .get(&source.artifact)
            .map_err(|error| match error {
                Error::NotFound => {
                    Error::Conflict("the requested source generation is unavailable".into())
                }
                error => error,
            })?;
        if artifact.generation != source.generation
            || artifact.job.is_some_and(|job| job != admission.job_id)
        {
            return Err(Error::Conflict(
                "the source artifact belongs to a different generation".into(),
            ));
        }
    }
    Ok(source)
}

pub(super) fn disposition_writer_files(
    job: &Job,
    source: &SourceGeneration,
    installation: &str,
) -> Vec<(String, nzbd_state::artifacts::Identity)> {
    job.files
        .iter()
        .flat_map(|file| {
            let current = format!("*File:proof:{}", file.id.0);
            let publication = format!("*File:publication-proof:{}", file.id.0);
            job.params
                .iter()
                .filter(move |(key, _)| key == &current || key == &publication)
                .filter_map(|(_, encoded)| {
                    let proof = serde_json::from_str::<DurableFileProof>(encoded).ok()?;
                    (proof.installation == installation
                        && proof.allocation_generation == source.generation)
                        .then_some(proof)
                })
                .flat_map(move |proof| {
                    let part = format!(".runner-file-{}.part", file.id.0);
                    let names = [
                        Some(part),
                        proof.publication_name,
                        Some(file.filename.clone()),
                    ];
                    let identity = proof.identity;
                    names
                        .into_iter()
                        .flatten()
                        .map(move |name| (name, identity.clone()))
                })
        })
        .collect()
}

impl Owner {
    pub(super) fn job_admission_binding(&self, job: &Job) -> Option<JobAdmissionBinding> {
        let token = job_transfer(job)?;
        self.state
            .url_admission_receipts
            .iter()
            .find(|receipt| receipt.job_id == job.id.0 && receipt.request_token == token)
            .map(|receipt| JobAdmissionBinding {
                job_id: job.id.0,
                transfer: token.into(),
                instance: receipt.runner_instance_id.clone(),
                generation: receipt.generation.clone(),
            })
    }

    pub(super) fn disposition_allows_failed_followup(job: &Job) -> bool {
        job.status == JobStatus::Failed
            && job
                .params
                .iter()
                .any(|(key, value)| key == nzbd_types::PP_DONE_PARAM && value != "DELETED")
    }

    fn disposition_live_source(&self, job: JobId) -> Option<SourceGeneration> {
        let record = self.state.job(job)?;
        let allocation = self.allocation_cache.get(&job)?;
        let source = SourceGeneration {
            artifact: allocation.id.clone(),
            generation: allocation.generation.clone(),
        };
        (allocation.owned
            && !disposition_writer_files(record, &source, &self.artifacts.installation).is_empty())
        .then_some(source)
    }

    pub(super) fn job_disposing(&self, job: JobId) -> bool {
        let Some((transfer, generation)) = self.disposition_fences.get(&job) else {
            return false;
        };
        self.state.job(job).is_some_and(|record| {
            job_transfer(record) == Some(transfer)
                && !record.params.iter().any(|(key, value)| {
                    matches!(
                        key.as_str(),
                        "*URL:AdmissionGeneration"
                            | crate::torrent_acquisitions::ADMISSION_GENERATION_PARAM
                    ) && value != generation
                })
        })
    }

    fn disposition_binding(
        &self,
        job: JobId,
        request: &JobDispositionRequest,
    ) -> Option<UrlAdmissionReceipt> {
        if self.consumer_instance_id != request.runner_instance_id
            || self
                .state
                .job(job)
                .and_then(job_transfer)
                .is_some_and(|transfer| transfer != request.transfer)
            || self
                .state
                .job(job)
                .is_some_and(|row| job_transfer(row).is_none())
            || (self.persist_guard.is_none()
                && self.state.job(job).is_some_and(|row| {
                    row.params.iter().any(|(key, value)| {
                        matches!(
                            key.as_str(),
                            "*URL:AdmissionGeneration"
                                | crate::torrent_acquisitions::ADMISSION_GENERATION_PARAM
                        ) && value != &request.generation
                    })
                }))
        {
            return None;
        }
        self.state
            .url_admission_receipts
            .iter()
            .find(|row| {
                row.job_id == job.0
                    && row.request_token == request.transfer
                    && row.runner_instance_id == request.runner_instance_id
                    && row.generation == request.generation
            })
            .cloned()
    }

    pub(super) fn advance_job_disposition(
        &mut self,
        job: JobId,
        request: JobDispositionRequest,
        native_joined: bool,
        url_joined: bool,
        reply: Reply,
    ) {
        let Some(admission) = self.disposition_binding(job, &request) else {
            let _ = reply.send(Err(EngineError::RequestConflict(
                "Runner instance, job, transfer or admission generation differs".into(),
            )));
            return;
        };
        if self.persistence_fenced || !self.persist {
            let _ = reply.send(Err(EngineError::OwnershipLost));
            return;
        }
        // This store is local. Cluster authority must prove all owner copies
        // and commit removal through its replicated control ledger.
        if self.persist_guard.is_some() {
            let _ = reply.send(Ok(JobDisposition {
                admitted: false,
                request,
                job_id: job.0,
                state: JobDispositionState::Blocked,
                reason: Some(
                    "cluster disposition requires exact replicated source-owner evidence".into(),
                ),
                operation_id: None,
            }));
            return;
        }
        if self.pending_control_commits.contains(&job)
            || self.pending_control_revisions.contains_key(&job)
            || self.allocating_jobs.contains(&job)
            || !self.disposition_in_flight.insert(job)
        {
            let _ = reply.send(Ok(JobDisposition {
                admitted: false,
                request,
                job_id: job.0,
                state: JobDispositionState::Pending,
                reason: Some(
                    "Runner is settling another job transition; retry the same request".into(),
                ),
                operation_id: None,
            }));
            return;
        }
        let epoch = self.job_epochs.get(&job).copied();
        let inventory = self.artifacts.clone();
        let live_proof = self.disposition_live_source(job);
        let work_request = request.clone();
        self.pending_control_commits.insert(job);
        self.io_then(
            move || {
                Ok((|| {
                    if let Some(existing) = inventory.job_disposition(&work_request.request_id)? {
                        if existing.job_id != job.0 || existing.request != work_request {
                            return Err(Error::Conflict(
                                "disposition request ID has a different binding".into(),
                            ));
                        }
                        return Ok(existing);
                    }
                    let source =
                        source_for_request(&inventory, &work_request, &admission, live_proof)?;
                    inventory.admit_job_disposition(job.0, work_request, source)
                })())
            },
            move |owner, result| {
                owner.pending_control_commits.remove(&job);
                let receipt = match result {
                    Ok(Ok(receipt)) => receipt,
                    Ok(Err(error)) => {
                        owner.disposition_in_flight.remove(&job);
                        let _ = reply.send(Err(engine_error(error)));
                        return;
                    }
                    Err(error) => {
                        owner.disposition_in_flight.remove(&job);
                        let _ = reply.send(Err(EngineError::Lifecycle(error.to_string())));
                        return;
                    }
                };
                if owner.job_epochs.get(&job).copied() != epoch
                    || owner.disposition_binding(job, &request).is_none()
                {
                    owner.disposition_in_flight.remove(&job);
                    let _ = reply.send(Err(EngineError::RequestConflict(
                        "job changed during disposition admission".into(),
                    )));
                    return;
                }
                if receipt.state == JobDispositionState::Verified {
                    owner.disposition_in_flight.remove(&job);
                    let _ = reply.send(Ok(receipt.status()));
                    return;
                }
                if receipt.stopped {
                    owner.settle_stopped_disposition(receipt, reply);
                    return;
                }
                let prior_fence = owner.job_disposing(job);
                owner.disposition_fences.insert(
                    job,
                    (
                        receipt.request.transfer.clone(),
                        receipt.request.generation.clone(),
                    ),
                );
                owner.stop_disposition_job(
                    job,
                    receipt,
                    native_joined && prior_fence,
                    url_joined && prior_fence,
                    reply,
                );
            },
        );
    }

    fn stop_disposition_job(
        &mut self,
        job: JobId,
        receipt: JobDispositionRecord,
        native_joined: bool,
        url_joined: bool,
        reply: Reply,
    ) {
        let Some(record) = self.state.job(job).cloned() else {
            if receipt.source.is_some() || receipt.unallocated_retirement() {
                self.finish_disposition_stop(receipt, reply);
            } else {
                self.reply_disposition_wait(
                    receipt,
                    JobDispositionState::Blocked,
                    "retired admission has no exact source or durable payload-absence proof",
                    reply,
                );
            }
            return;
        };
        if record.torrent.is_some() || self.delegated.contains_key(&job) {
            self.reply_disposition_wait(
                receipt,
                JobDispositionState::Blocked,
                "the backend must acknowledge this exact writer and source before disposal",
                reply,
            );
            return;
        }
        // Failed work retains the existing salvage path. A previously
        // recorded failure may still own a verified publication attempt.
        if !Self::disposition_allows_failed_followup(&record) {
            if let Some(cancel) = self.post_cancellations.get(&job) {
                cancel.cancel();
            }
        }
        if let Some(verification) = self.verification.get(&job) {
            verification.cancel.cancel();
        }
        self.retire_job_writers(job);
        self.attempts.retain(|reference, _| reference.job != job);
        self.pending_finalize
            .retain(|(candidate, _)| *candidate != job);
        self.publish_now();
        self.bump_epoch();
        if !url_joined && record.params.iter().any(|(key, _)| key == "*URL") {
            self.reply_disposition_wait(
                receipt,
                JobDispositionState::Pending,
                "Runner is joining the URL resolution writer",
                reply,
            );
            return;
        }
        if crate::torrent_acquisitions::is_acquisition(&record) && !native_joined {
            self.reply_disposition_wait(
                receipt,
                JobDispositionState::Pending,
                "Runner is joining the native torrent acquisition writer",
                reply,
            );
            return;
        }

        if self.post_admissions.contains_key(&job)
            || self.record_transitions.contains(&job)
            || self.pending_name_inspections.contains_key(&job)
            || self.terminals_in_flight.contains(&job)
            || self.deferred_terminals.contains_key(&job)
            || self
                .verification
                .get(&job)
                .is_some_and(|state| state.running)
            || self
                .retiring_writers
                .get(&job)
                .is_some_and(|writers| writers.iter().any(|writer| !*writer.stopped.borrow()))
        {
            self.reply_disposition_wait(
                receipt,
                JobDispositionState::Pending,
                "Runner is joining this job's writers and settling its terminal",
                reply,
            );
            return;
        }
        let inventory = self.artifacts.clone();
        let candidate = record.clone();
        let existing_source = receipt.source.clone();
        let live_proof = self.disposition_live_source(job);
        let request = receipt.request.clone();
        let admission = self
            .disposition_binding(job, &receipt.request)
            .expect("validated by owner");
        let epoch = self.job_epochs.get(&job).copied();
        self.pending_control_commits.insert(job);
        self.io_then(
            move || {
                Ok((|| {
                    // A native copy can publish its allocation immediately
                    // before observing the new fence. Its exact admission pin
                    // can be adopted only after the backend actually joins.
                    let source = match existing_source {
                        Some(existing) => inventory
                            .disposition_delivery_source(
                                job.0,
                                &request.transfer,
                                &request.runner_instance_id,
                            )?
                            .or(Some(existing)),
                        None => source_for_request(&inventory, &request, &admission, live_proof)?,
                    };
                    if inventory.open_intent_for_job(job.0)? {
                        return Ok(StopEvidence::Pending(
                            "publication movement is still reconciling",
                        ));
                    }
                    if let Some(terminal) = inventory.terminal(job.0)? {
                        if terminal.transfer.as_deref() != job_transfer(&candidate) {
                            return Err(Error::Conflict(
                                "stored terminal belongs to a different transfer".into(),
                            ));
                        }
                        if !terminal.finished() || terminal.salvage == SalvageState::Pending {
                            return Ok(StopEvidence::Pending(
                                "the actual terminal or salvage is still reconciling",
                            ));
                        }
                        return Ok(StopEvidence::Terminal(source));
                    }
                    if candidate
                        .params
                        .iter()
                        .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
                        || candidate.status == JobStatus::Failed
                    {
                        return Ok(StopEvidence::Pending(
                            "the finished transfer's terminal is still reconciling",
                        ));
                    }
                    Ok(StopEvidence::Cancelled(source))
                })())
            },
            move |owner, result| {
                owner.pending_control_commits.remove(&job);
                if owner.job_epochs.get(&job).copied() != epoch
                    || owner.disposition_binding(job, &receipt.request).is_none()
                {
                    owner.disposition_in_flight.remove(&job);
                    let _ = reply.send(Err(EngineError::RequestConflict(
                        "job changed while Runner joined its writers".into(),
                    )));
                    return;
                }
                match result {
                    Ok(Ok(StopEvidence::Pending(reason))) => owner.reply_disposition_wait(
                        receipt,
                        JobDispositionState::Pending,
                        reason,
                        reply,
                    ),
                    Ok(Ok(StopEvidence::Terminal(source))) => {
                        owner.commit_disposition_source(job, receipt, source, None, reply);
                    }
                    Ok(Ok(StopEvidence::Cancelled(source))) => {
                        let mut terminal = crate::terminal::terminal_from_job(
                            &record,
                            nzbd_types::TerminalOutcome::Cancelled,
                            "DELETED",
                            None,
                            vec![(nzbd_types::PP_DONE_PARAM.into(), "DELETED".into())],
                            crate::terminal::TerminalFacts {
                                reason: None,
                                fetch_cause: None,
                                stage: None,
                                message: Some("deleted by the operator".into()),
                                blames_release: false,
                                salvage: SalvageState::None,
                                keep_on_failure: false,
                                publication: None,
                            },
                            unix_now(),
                        );
                        if let Some(source) = &source {
                            terminal.entry.params.retain(|(key, _)| {
                                !matches!(key.as_str(), "Artifact:Id" | "Artifact:Generation")
                            });
                            terminal
                                .entry
                                .params
                                .push(("Artifact:Id".into(), source.artifact.clone()));
                            terminal
                                .entry
                                .params
                                .push(("Artifact:Generation".into(), source.generation.clone()));
                        }
                        owner.commit_disposition_source(
                            job,
                            receipt,
                            source,
                            Some(terminal),
                            reply,
                        );
                    }
                    Ok(Err(error)) => {
                        owner.disposition_in_flight.remove(&job);
                        let _ = reply.send(Err(engine_error(error)));
                    }
                    Err(error) => {
                        owner.disposition_in_flight.remove(&job);
                        let _ = reply.send(Err(EngineError::Lifecycle(error.to_string())));
                    }
                }
            },
        );
    }

    fn commit_disposition_source(
        &mut self,
        job: JobId,
        mut receipt: JobDispositionRecord,
        source: Option<SourceGeneration>,
        terminal: Option<nzbd_state::artifacts::JobTerminal>,
        reply: Reply,
    ) {
        let inventory = self.artifacts.clone();
        let epoch = self.job_epochs.get(&job).copied();
        self.pending_control_commits.insert(job);
        self.io_then(
            move || {
                receipt.source = source;
                Ok(inventory.save_job_disposition(&receipt).map(|()| receipt))
            },
            move |owner, persisted| {
                owner.pending_control_commits.remove(&job);
                let receipt = match persisted {
                    Ok(Ok(receipt)) => receipt,
                    Ok(Err(error)) => {
                        owner.disposition_in_flight.remove(&job);
                        let _ = reply.send(Err(engine_error(error)));
                        return;
                    }
                    Err(error) => {
                        owner.disposition_in_flight.remove(&job);
                        let _ = reply.send(Err(EngineError::Lifecycle(error.to_string())));
                        return;
                    }
                };
                if owner.job_epochs.get(&job).copied() != epoch
                    || owner.disposition_binding(job, &receipt.request).is_none()
                {
                    owner.disposition_in_flight.remove(&job);
                    let _ = reply.send(Err(EngineError::RequestConflict(
                        "job changed before terminal commit".into(),
                    )));
                    return;
                }
                if let Some(terminal) = terminal {
                    owner.record_terminal_then(
                        terminal,
                        Box::new(move |owner, terminal| {
                            if terminal.is_some_and(|terminal| {
                                terminal.transfer.as_deref() == Some(&receipt.request.transfer)
                                    && terminal.finished()
                                    && terminal.salvage != SalvageState::Pending
                            }) && owner.disposition_binding(job, &receipt.request).is_some()
                            {
                                owner.retire_disposition_job(job, receipt, reply);
                            } else {
                                owner.reply_disposition_wait(
                                    receipt,
                                    JobDispositionState::Pending,
                                    "terminal history is still being committed",
                                    reply,
                                );
                            }
                        }),
                    );
                } else {
                    owner.retire_disposition_job(job, receipt, reply);
                }
            },
        );
    }

    fn retire_disposition_job(&mut self, job: JobId, receipt: JobDispositionRecord, reply: Reply) {
        self.delete_job_with_source_then(
            job,
            false,
            Some(receipt.clone()),
            move |owner, stopped| {
                if stopped {
                    owner.finish_disposition_stop(receipt, reply);
                } else {
                    owner.reply_disposition_wait(
                        receipt,
                        JobDispositionState::Pending,
                        "Runner is still retiring the queue and source writers",
                        reply,
                    );
                }
            },
        );
    }

    fn finish_disposition_stop(&mut self, mut receipt: JobDispositionRecord, reply: Reply) {
        receipt.stopped = true;
        receipt.state = JobDispositionState::Pending;
        receipt.reason = None;
        let inventory = self.artifacts.clone();
        let job = JobId(receipt.job_id);
        self.io_then(
            move || {
                Ok((|| {
                    inventory.save_job_disposition(&receipt)?;
                    inventory
                        .job_disposition(&receipt.request.request_id)?
                        .ok_or(Error::NotFound)
                })())
            },
            move |owner, result| match result {
                Ok(Ok(receipt)) => {
                    owner.disposition_fences.remove(&job);
                    owner.settle_stopped_disposition(receipt, reply);
                }
                Ok(Err(error)) => {
                    owner.disposition_in_flight.remove(&job);
                    let _ = reply.send(Err(engine_error(error)));
                }
                Err(error) => {
                    owner.disposition_in_flight.remove(&job);
                    let _ = reply.send(Err(EngineError::Lifecycle(error.to_string())));
                }
            },
        );
    }

    fn reply_disposition_wait(
        &mut self,
        mut receipt: JobDispositionRecord,
        state: JobDispositionState,
        reason: &'static str,
        reply: Reply,
    ) {
        receipt.state = state;
        receipt.reason = Some(reason.into());
        let inventory = self.artifacts.clone();
        let job = JobId(receipt.job_id);
        self.io_then(
            move || {
                Ok(inventory
                    .save_job_disposition(&receipt)
                    .map(|()| receipt.status()))
            },
            move |owner, result| owner.reply_disposition_result(job, result, reply),
        );
    }

    fn settle_stopped_disposition(&mut self, receipt: JobDispositionRecord, reply: Reply) {
        let job = JobId(receipt.job_id);
        let inventory = self.artifacts.clone();
        let lookup = inventory.clone();
        let fallback = self.dest_dir.clone();
        let source = receipt.source.clone();
        self.io_then(
            move || {
                Ok(match source {
                    Some(source) => lookup.get(&source.artifact).map(|artifact| artifact.root),
                    None => Ok(fallback.clone()),
                }
                .or_else(|error| match error {
                    Error::NotFound => Ok(fallback),
                    error => Err(error),
                }))
            },
            move |owner, result| match result {
                Ok(Ok(root)) => owner.payload_then(
                    root,
                    move || Ok(inventory.settle_job_disposition(receipt)),
                    move |owner, result| owner.reply_disposition_result(job, result, reply),
                ),
                Ok(Err(error)) => owner.reply_disposition_result(job, Ok(Err(error)), reply),
                Err(error) => owner.reply_disposition_result(job, Err(error), reply),
            },
        );
    }

    fn reply_disposition_result(
        &mut self,
        job: JobId,
        result: Result<Result<JobDisposition, Error>, nzbd_state::StateError>,
        reply: Reply,
    ) {
        self.disposition_in_flight.remove(&job);
        let _ = reply.send(
            result
                .map_err(|error| EngineError::Lifecycle(error.to_string()))
                .and_then(|result| result.map_err(engine_error)),
        );
    }
}

enum StopEvidence {
    Pending(&'static str),
    Terminal(Option<SourceGeneration>),
    Cancelled(Option<SourceGeneration>),
}
