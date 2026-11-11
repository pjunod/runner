//! Runtime recovery workers. Actor state never performs payload I/O.
use super::*;
use crate::recovery::{self, DurableFileProof, ProvenRange, ValidationOutcome, WRITER_PROTOCOL};
use nzbd_state::artifacts::{Artifact, Identity, Inventory};
use serde::{Deserialize, Serialize};

const MAX_VERIFICATION_WORKERS: usize = 2;
const BATCH_BYTES: u64 = 32 * 1024 * 1024;
const BATCH_RANGES: usize = 64;
const PENDING: &str = "*Recovery:pending";
const REASON: &str = "*Recovery:reason";

#[derive(Debug)]
pub(super) struct VerificationState {
    pub generation: u64,
    pub phase: String,
    pub reason: Option<String>,
    pub checked_bytes: u64,
    pub total_bytes: u64,
    pub retry_at: Instant,
    pub attempts: u32,
    pub cancel: CancellationToken,
    pub running: bool,
    pub review_required: bool,
    partial: Option<VerifiedJob>,
    turn: u64,
}
impl VerificationState {
    pub(super) fn progress(&self) -> crate::snapshot::VerificationProgress {
        crate::snapshot::VerificationProgress {
            state: self.phase.clone(),
            reason: self.reason.clone(),
            checked_bytes: self.checked_bytes,
            total_bytes: self.total_bytes,
        }
    }
}

#[derive(Debug)]
pub(crate) struct VerificationResult {
    // The reservation belongs to the actual worker, even after job removal or
    // ticket invalidation. Only a completed thread may release this root slot.
    root: PathBuf,
    outcome: Result<VerifiedJob, VerificationFailure>,
}
#[derive(Debug)]
struct VerifiedJob {
    complete: bool,
    completed_files: HashSet<FileId>,
    observations: HashMap<FileId, (String, Option<Identity>)>,
    partial_range: Option<PartialRange>,
    allocation: Artifact,
    proofs: Vec<(FileId, DurableFileProof)>,
    invalid: Vec<(FileId, u32)>,
    published: HashSet<FileId>,
    reopen_finalize: HashSet<FileId>,
    transformed: bool,
    post_retry_validated: bool,
    checked_bytes: u64,
    publication_repair: bool,
    repair_files: Vec<PublicationRepairFile>,
}
struct PartialRange {
    file: FileId,
    range: ProvenRange,
    read: u64,
    digest: crc32fast::Hasher,
}
impl std::fmt::Debug for PartialRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartialRange")
            .field("file", &self.file)
            .field("range", &self.range)
            .field("read", &self.read)
            .finish()
    }
}
fn read_piece(
    file: &mut std::fs::File,
    identity: &Identity,
    cursor: &mut PartialRange,
    budget: u64,
) -> (ValidationOutcome, u64, bool) {
    use std::io::{Read, Seek, SeekFrom};
    if !cursor.range.end().is_some_and(|end| end <= identity.bytes) {
        return (ValidationOutcome::Mismatch, 0, true);
    }
    match recovery::capture_identity(file) {
        Ok(observed) if observed == *identity => {}
        Ok(_) => return (ValidationOutcome::IdentityConflict, 0, true),
        Err(error) => return (ValidationOutcome::Unavailable(error), 0, true),
    }
    if let Err(error) = file.seek(SeekFrom::Start(cursor.range.offset + cursor.read)) {
        return (ValidationOutcome::Unavailable(error.to_string()), 0, true);
    }
    let mut bytes = 0;
    let mut buffer = [0u8; 65536];
    while cursor.read < u64::from(cursor.range.len) && bytes < budget {
        let amount = (u64::from(cursor.range.len) - cursor.read)
            .min(budget - bytes)
            .min(buffer.len() as u64) as usize;
        if let Err(error) = file.read_exact(&mut buffer[..amount]) {
            return (
                ValidationOutcome::Unavailable(error.to_string()),
                bytes,
                true,
            );
        }
        cursor.digest.update(&buffer[..amount]);
        cursor.read += amount as u64;
        bytes += amount as u64;
    }
    match recovery::capture_identity(file) {
        Ok(observed) if observed == *identity => {
            let complete = cursor.read == u64::from(cursor.range.len);
            let outcome = if complete && cursor.digest.clone().finalize() != cursor.range.crc {
                ValidationOutcome::Mismatch
            } else {
                ValidationOutcome::Valid(observed)
            };
            (outcome, bytes, complete)
        }
        Ok(_) => (ValidationOutcome::IdentityConflict, bytes, true),
        Err(error) => (ValidationOutcome::Unavailable(error), bytes, true),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PublicationRepairFile {
    file: FileId,
    publication: String,
    identity: Identity,
    generation: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct PublicationRepairIntent {
    protocol: u32,
    installation: String,
    allocation_generation: String,
    files: Vec<PublicationRepairFile>,
}

#[derive(Debug)]
struct VerificationFailure {
    reason: String,
    checked_bytes: u64,
    review_required: bool,
    /// The owner verified against an allocation snapshot that a committed
    /// inventory transition (e.g. a publication move reconciled at startup)
    /// has since replaced: reload the allocation and verify again.
    stale_allocation: bool,
}
impl VerificationFailure {
    fn stale(reason: impl ToString, checked_bytes: u64) -> Self {
        Self {
            reason: reason.to_string(),
            checked_bytes,
            review_required: false,
            stale_allocation: true,
        }
    }
    fn unavailable(reason: impl ToString, checked_bytes: u64) -> Self {
        Self {
            reason: reason.to_string(),
            checked_bytes,
            review_required: false,
            stale_allocation: false,
        }
    }
    fn conflict(reason: impl ToString, checked_bytes: u64) -> Self {
        Self {
            reason: reason.to_string(),
            checked_bytes,
            review_required: true,
            stale_allocation: false,
        }
    }
}

impl Owner {
    pub(super) fn retry_publication_repair_then(
        &mut self,
        job: JobId,
        revision: String,
        by: RetryBy,
        callback: impl FnOnce(&mut Owner, Result<(), String>) + Send + 'static,
    ) {
        if by != RetryBy::Operator {
            callback(
                self,
                Err("legacy publication repair requires an explicit operator Retry".into()),
            );
            return;
        }
        let Some(before) = self.state.job(job).cloned() else {
            callback(self, Err("job not found".into()));
            return;
        };
        let Some(control) = before
            .control()
            .filter(|control| control.lifecycle == "held" && control.revision == revision)
        else {
            callback(self, Err("the hold changed; reload and retry".into()));
            return;
        };
        let raw = parameter(&before, "*Recovery:publication-repair")
            .unwrap_or("")
            .to_owned();
        let intent = match serde_json::from_str::<PublicationRepairIntent>(&raw) {
            Ok(intent)
                if intent.protocol == 1
                    && intent.installation == self.artifacts.installation
                    && !intent.files.is_empty() =>
            {
                intent
            }
            _ => {
                callback(
                    self,
                    Err(
                        "publication repair lacks exact file identity evidence; review required"
                            .into(),
                    ),
                );
                return;
            }
        };
        let files = intent.files;
        let consumer_owned = before.params.iter().any(|(key, value)| {
            (key == "Delivery:Custody" && value != "runner_owned")
                || (key == "Artifact:Custody"
                    && !matches!(value.as_str(), "runner_owned" | "no_payload"))
        });
        if before.torrent.is_some()
            || consumer_owned
            || before
                .params
                .iter()
                .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
            || self.post_admissions.contains_key(&job)
            || self.retiring_writers.contains_key(&job)
            || self
                .verification
                .get(&job)
                .is_some_and(|state| state.running)
            || self
                .pending_name_inspections
                .get(&job)
                .is_some_and(|count| *count > 0)
            || before.files.iter().any(|file| {
                self.writers
                    .get(&file.id)
                    .is_some_and(|writer| !*writer.stopped.borrow())
            })
        {
            callback(
                self,
                Err("publication custody/readers/writers must quiesce before repair".into()),
            );
            return;
        }
        let Some(allocation) = self
            .allocation_cache
            .get(&job)
            .cloned()
            .filter(|allocation| allocation.state == "active" && allocation.job == Some(job.0))
        else {
            callback(
                self,
                Err(
                    "only an active download-owned allocation may repair a legacy publication"
                        .into(),
                ),
            );
            return;
        };
        if allocation.generation != intent.allocation_generation {
            callback(
                self,
                Err("publication repair belongs to a retired allocation".into()),
            );
            return;
        }
        let resuming_intent =
            parameter(&before, "*Recovery:publication-repair-intent") == Some(raw.as_str());
        if files.iter().any(|file| {
            let generation =
                parameter(&before, &format!("*File:generation:{}", file.file.0)).unwrap_or("");
            generation != file.generation && !(resuming_intent && generation.starts_with("repair-"))
        }) {
            callback(
                self,
                Err("publication repair file generation changed; review required".into()),
            );
            return;
        }
        // A durable new incarnation is established before any namespace change.
        // Previous proof stays as an immutable receipt, never as write authority.
        let epoch = fresh_epoch();
        self.job_epochs.insert(job, epoch);
        let record = self.state.job_mut(job).unwrap();
        for file in &files {
            let key = format!("*File:proof:{}", file.file.0);
            if let Some(proof) = record
                .params
                .iter()
                .find(|(candidate, _)| candidate == &key)
                .map(|(_, proof)| proof.clone())
            {
                set_owner_param(
                    &mut record.params,
                    &format!("*File:retired-proof:{}", file.file.0),
                    Some(proof),
                );
            }
            set_owner_param(&mut record.params, &key, None);
            set_owner_param(
                &mut record.params,
                &format!("*File:generation:{}", file.file.0),
                Some(format!("repair-{epoch}-{}", file.file.0)),
            );
            self.writer_epochs.remove(&file.file);
            self.pending_writer_birth.remove(&file.file);
        }
        set_owner_param(
            &mut record.params,
            "*Recovery:publication-repair-intent",
            Some(raw.clone()),
        );
        set_owner_param(&mut record.params, PENDING, Some("1".into()));
        if let Some(state) = self.verification.get_mut(&job) {
            state.cancel.cancel();
            state.generation = epoch;
            state.phase = "saving".into();
            state.running = false;
        }
        self.pending_control_commits.insert(job);
        self.dirty = true;
        self.persist_then(move|owner,committed| {
            if owner.cancel.is_cancelled() {
                owner.pending_control_commits.remove(&job);
                owner.block_verification(job,VerificationFailure::unavailable("publication repair stopped before namespace mutation during shutdown",0));
                callback(owner,Err("publication repair stopped during shutdown; retained intent is retryable".into()));
                return;
            }
            if owner.job_epochs.get(&job)!=Some(&epoch){owner.pending_control_commits.remove(&job);callback(owner,Err("job generation changed before repair".into()));return;}
            if !committed {owner.pending_control_commits.remove(&job);owner.block_verification(job,VerificationFailure::conflict("file generation retirement is not durable; publication retained",0));callback(owner,Err("file generation retirement is not durable; publication retained".into()));return;}
            if let Some(state)=owner.verification.get_mut(&job){state.phase="repairing".into();state.running=true;}
            let inventory=owner.artifacts.clone();let files_for_worker=files.clone();
            owner.payload_then(allocation.root.clone(),move||stage_publication_repair(job,&allocation,&inventory,&files_for_worker)
                .map_err(nzbd_state::StateError::Corrupt),move|owner,result| {
                // payload_then returns only after its Inventory guard and descriptors
                // have been dropped, including when a rename or sync failed.
                if let Some(state)=owner.verification.get_mut(&job){state.running=false;}
                owner.pending_control_commits.remove(&job);
                if owner.job_epochs.get(&job)!=Some(&epoch){owner.drain_replacements();callback(owner,Err("job changed while repair was stopping".into()));return;}
                if let Err(error)=result {
                    owner.block_verification(job,VerificationFailure::conflict(error.to_string(),0));
                    owner.publish_now();owner.bump_epoch();callback(owner,Err(error.to_string()));return;
                }
                let record=owner.state.job_mut(job).unwrap();
                set_owner_param(&mut record.params,"*Recovery:publication-repair",None);
                set_owner_param(&mut record.params,"*Recovery:publication-repair-intent",None);
                let current_control=record.control();
                let unchanged=current_control.as_ref().is_some_and(|current|current.revision==revision);
                if unchanged {
                    let mut released=control.clone();
                    released.revision=match released.revision.parse::<u64>().ok().and_then(|revision|revision.checked_add(1)){Some(revision)=>revision.to_string(),None=>{callback(owner,Err("hold revision exhausted".into()));return;}};
                    released.lifecycle="running".into();released.manual_pause=false;released.previous_status=Some(JobStatus::Queued);
                    released.next_retry_at_unix=None;released.released_at_unix=Some(unix_now());
                    released.message="Operator Retry restored the legacy publication to download-owned partial; retained ranges require verification".into();
                    record.set_control(&released);record.status=JobStatus::Queued;
                }
                for file in &files {if let Some(file)=record.files.iter_mut().find(|entry|entry.id==file.file){file.finalized=false;file.crc32=None;}}
                if let Some(state)=owner.verification.get_mut(&job){state.phase="saving".into();state.running=false;}
                owner.dirty=true;
                owner.persist_then(move|owner,committed| {
                    if owner.job_epochs.get(&job)!=Some(&epoch){callback(owner,Err("job changed before repair checkpoint".into()));return;}
                    if !committed {
                        if let Some(record)=owner.state.job_mut(job) {
                            set_owner_param(&mut record.params,"*Recovery:publication-repair",Some(raw.clone()));
                            set_owner_param(&mut record.params,"*Recovery:publication-repair-intent",Some(raw));
                            // Restore only this helper's own release revision.
                            if record.control().is_some_and(|current|current.lifecycle=="running" && current.revision.parse::<u64>().ok()==control.revision.parse::<u64>().ok().and_then(|revision|revision.checked_add(1))) {
                                record.set_control(&control);record.status=JobStatus::Held;
                            }
                        }
                        owner.block_verification(job,VerificationFailure::conflict("repair namespace is safe but its completion checkpoint is not durable; retry is idempotent",0));
                        callback(owner,Err("repair completion checkpoint is not durable".into()));
                    }else {
                        owner.queue_verification(job);
                        callback(owner,if unchanged{Ok(())}else{Err("publication restored, but a newer user hold/pause remains effective".into())});
                    }
                    owner.publish_now();owner.bump_epoch();
                });
            });
        });
    }

    pub(super) fn queue_verification(&mut self, job: JobId) {
        self.artifacts.invalidate_writer_naming(job.0);
        let Some(current) = self.state.job(job) else {
            return;
        };
        if current.torrent.is_some() || super::transitions::failed_cleanup_only(current) {
            return;
        }
        let total_bytes = done_bytes(current);
        let generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
        let missing_allocation = !self.allocation_cache.contains_key(&job);
        let journal_loading = self
            .pending_journal_loads
            .get(&job)
            .is_some_and(|loading| loading.0 == generation);
        let loading = !self.startup_roots_ready
            || missing_allocation
            || self.allocating_jobs.contains(&job)
            || journal_loading;
        let drive_allocation = missing_allocation
            && self.startup_roots_ready
            && !self.allocating_jobs.contains(&job)
            && !self.pending_control_commits.contains(&job);
        if let Some(existing) = self.verification.get_mut(&job) {
            // A resume changes priority, not the number of live storage readers.
            if existing.running {
                return;
            }
            existing.cancel.cancel();
        }
        self.verification.insert(
            job,
            VerificationState {
                generation,
                phase: if loading { "loading" } else { "pending" }.into(),
                reason: None,
                checked_bytes: 0,
                total_bytes,
                retry_at: Instant::now(),
                attempts: 0,
                cancel: self.cancel.child_token(),
                running: false,
                review_required: false,
                partial: None,
                turn: 0,
            },
        );
        if let Some(current) = self.state.job_mut(job) {
            set_owner_param(&mut current.params, PENDING, Some("1".into()));
            set_owner_param(&mut current.params, REASON, None);
        }
        self.dirty = true;
        self.publish_now();
        self.bump_epoch();
        if drive_allocation {
            // Missing metadata is a prerequisite, not evidence of unavailable
            // storage. The allocation marker bounds this nested ticket update
            // and its actual completion schedules verification when ready.
            self.queue_payload_recovery(job);
        }
    }

    /// A boot verification waits for its payload root; if the job's
    /// `completed` terminal is stamped meanwhile (the terminal replay of a
    /// crash after the history write), its payload is the publication's and
    /// nothing is left to prove — yet nothing would ever run or clear that
    /// verification, and a job under verification can be neither exported
    /// nor admitted. Drop exactly those waiting verifications.
    ///
    /// Only a successful, stamped completion qualifies: a failed job's
    /// retained or partially deleted source (failed cleanup included) keeps
    /// its verification, which fences export and admission until its `Done`
    /// bits are proven or dropped.
    pub(super) fn drop_unneeded_verifications(&mut self) {
        let stale = self
            .state
            .jobs
            .iter()
            .filter(|job| {
                job.status == JobStatus::Completed
                    && job.torrent.is_none()
                    && !super::transitions::failed_cleanup_only(job)
                    && job
                        .params
                        .iter()
                        .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
            })
            .filter(|job| {
                self.verification
                    .get(&job.id)
                    .is_some_and(|state| !state.running)
            })
            .map(|job| job.id)
            .collect::<Vec<_>>();
        if stale.is_empty() {
            return;
        }
        for job in stale {
            self.verification.remove(&job);
            if let Some(current) = self.state.job_mut(job) {
                set_owner_param(&mut current.params, PENDING, None);
                set_owner_param(&mut current.params, REASON, None);
            }
        }
        // The fence lifted: callers the owner deferred (export, admission)
        // wait for this queue change.
        self.dirty = true;
        self.publish_now();
        self.bump_epoch();
    }

    /// Re-read the job's allocation and queue its verification again. Not a
    /// retry ladder: it runs once per observed inventory transition.
    fn reload_stale_allocation(&mut self, job: JobId) {
        if let Some(state) = self.verification.get_mut(&job) {
            state.running = false;
            state.phase = "loading".into();
            state.partial = None;
        }
        self.queue_payload_recovery(job);
        self.dirty = true;
        self.publish_now();
        self.bump_epoch();
    }

    pub(super) fn start_verifications(&mut self) {
        self.drop_unneeded_verifications();
        if self.cancel.is_cancelled() || self.verification_roots.len() >= MAX_VERIFICATION_WORKERS {
            return;
        }
        let now = Instant::now();
        let mut candidates = self
            .state
            .jobs
            .iter()
            .enumerate()
            .filter_map(|(position, job)| {
                let state = self.verification.get(&job.id)?;
                (!state.running
                    && !super::transitions::failed_cleanup_only(job)
                    && state.phase != "saving"
                    && state.phase != "loading"
                    && !state.review_required
                    && state.retry_at <= now
                    && !self.delegated.contains_key(&job.id)
                    && !self.pending_control_commits.contains(&job.id)
                    && !self.pending_control_revisions.contains_key(&job.id)
                    && !self.post_admissions.contains_key(&job.id)
                    && !self.retiring_writers.contains_key(&job.id)
                    && !self.allocating_jobs.contains(&job.id)
                    && !self
                        .pending_journal_loads
                        .get(&job.id)
                        .is_some_and(|loading| self.job_epochs.get(&job.id) == Some(&loading.0))
                    && !job
                        .files
                        .iter()
                        .any(|file| self.writers.contains_key(&file.id)))
                .then_some((
                    matches!(job.status, JobStatus::Paused | JobStatus::Held) || job.held(),
                    std::cmp::Reverse(job.priority),
                    state.turn,
                    position,
                    job.id,
                ))
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(paused, priority, turn, position, _)| {
            (*paused, *priority, *turn, *position)
        });
        let mut changed = false;
        for (_, _, _, _, job) in candidates {
            if self.verification_roots.len() >= MAX_VERIFICATION_WORKERS {
                break;
            }
            let Some(allocation) = self.allocation_cache.get(&job).cloned() else {
                self.queue_payload_recovery(job);
                changed = true;
                continue;
            };
            if self.verification_roots.contains(&allocation.root) {
                continue;
            }
            let Some(snapshot) = self.state.job(job).cloned() else {
                continue;
            };
            let Some(state) = self.verification.get_mut(&job) else {
                continue;
            };
            let generation = state.generation;
            if self.job_epochs.get(&job) != Some(&generation) {
                continue;
            }
            state.running = true;
            state.turn = fresh_epoch();
            let partial = state.partial.take();
            state.phase = "checking".into();
            state.reason = None;
            let cancellation = state.cancel.clone();
            let inventory = self.artifacts.clone();
            let root = allocation.root.clone();
            self.verification_roots.insert(root.clone());
            let (result_tx, result_rx) = oneshot::channel();
            let worker = std::thread::Builder::new()
                .name(format!("verify-{}", job.0))
                .spawn(move || {
                    // The custody guard and every descriptor live inside this call
                    // and are dropped before the completion signal is sent.
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        verify_job_batch(
                            &snapshot,
                            allocation,
                            &inventory,
                            &cancellation,
                            generation,
                            partial,
                            true,
                        )
                    }))
                    .unwrap_or_else(|_| {
                        Err(VerificationFailure::unavailable(
                            "verification worker panicked",
                            0,
                        ))
                    });
                    let _ = result_tx.send(result);
                });
            match worker {
                Ok(worker) => {
                    let engine = self.engine_tx.clone();
                    self.tracker.spawn(async move {
                        let outcome = result_rx.await.unwrap_or_else(|_| {
                            Err(VerificationFailure::unavailable(
                                "verification worker stopped without a result",
                                0,
                            ))
                        });
                        // Receipt alone is not quiescence. Join after receiving
                        // the terminal signal; the worker has no remaining I/O.
                        let outcome = if worker.join().is_err() {
                            Err(VerificationFailure::unavailable(
                                "verification worker failed while stopping",
                                0,
                            ))
                        } else {
                            outcome
                        };
                        let _ = engine
                            .send(EngineMsg::VerificationFinished {
                                job,
                                generation,
                                result: Box::new(VerificationResult { root, outcome }),
                            })
                            .await;
                    });
                }
                Err(error) => {
                    self.verification_roots.remove(&root);
                    self.block_verification(
                        job,
                        VerificationFailure::unavailable(
                            format!("start verification worker: {error}"),
                            0,
                        ),
                    );
                }
            }
            changed = true;
        }
        if changed {
            self.dirty = true;
            self.publish_now();
            self.bump_epoch();
        }
    }

    fn block_verification(&mut self, job: JobId, failure: VerificationFailure) {
        if let Some(state) = self.verification.get_mut(&job) {
            state.running = false;
            state.phase = "blocked".into();
            state.checked_bytes = failure.checked_bytes;
            state.reason = Some(failure.reason.clone());
            state.review_required = failure.review_required;
            state.attempts = state.attempts.saturating_add(1);
            let backoff = 5_u64
                .saturating_mul(1_u64 << state.attempts.min(6))
                .min(300);
            state.retry_at = Instant::now() + Duration::from_secs(backoff);
        }
        if let Some(current) = self.state.job_mut(job) {
            // This is verification state, never a user-pause or evidence reset.
            set_owner_param(&mut current.params, PENDING, Some("1".into()));
            set_owner_param(&mut current.params, REASON, Some(failure.reason));
        }
    }

    pub(super) fn on_verification_result(
        &mut self,
        job: JobId,
        generation: u64,
        result: VerificationResult,
    ) {
        self.verification_roots.remove(&result.root);
        let current_ticket = self
            .verification
            .get(&job)
            .is_some_and(|state| state.generation == generation && state.running);
        if !current_ticket {
            return;
        }
        if self.job_epochs.get(&job) != Some(&generation) || self.state.job(job).is_none() {
            // Replacement may have been queued while the old reader was still
            // in a syscall. Only now may its ticket be replaced and retried.
            self.verification.remove(&job);
            if self.state.job(job).is_some() && !self.cancel.is_cancelled() {
                self.queue_verification(job);
            }
            return;
        }
        if self
            .verification
            .get(&job)
            .is_some_and(|state| state.cancel.is_cancelled())
        {
            self.block_verification(
                job,
                VerificationFailure::unavailable(
                    "verification was cancelled; evidence retained",
                    0,
                ),
            );
            self.dirty = true;
            self.publish_now();
            self.bump_epoch();
            return;
        }
        if self.post_admissions.contains_key(&job)
            || self.retiring_writers.contains_key(&job)
            || self.state.job(job).is_some_and(|job| {
                job.files
                    .iter()
                    .any(|file| self.writers.contains_key(&file.id))
            })
        {
            self.block_verification(
                job,
                VerificationFailure::unavailable(
                    "verification scope changed while a reader or writer retained custody",
                    0,
                ),
            );
            self.dirty = true;
            self.publish_now();
            self.bump_epoch();
            return;
        }
        let verified = match result.outcome {
            Ok(verified) => verified,
            Err(failure) if failure.stale_allocation => {
                self.reload_stale_allocation(job);
                return;
            }
            Err(failure) => {
                self.block_verification(job, failure);
                self.dirty = true;
                self.publish_now();
                self.bump_epoch();
                return;
            }
        };
        if !self.allocation_cache.get(&job).is_some_and(|cached| {
            cached.id == verified.allocation.id
                && cached.generation == verified.allocation.generation
                && cached.path == verified.allocation.path
                && cached.root == verified.allocation.root
        }) {
            // The worker read the current allocation; the owner's cached
            // snapshot is the stale one.
            self.reload_stale_allocation(job);
            return;
        }
        if !verified.complete {
            if let Some(state) = self.verification.get_mut(&job) {
                state.running = false;
                state.phase = "pending".into();
                state.checked_bytes = verified.checked_bytes;
                state.partial = Some(verified);
                state.retry_at = Instant::now();
            }
            self.publish_now();
            self.bump_epoch();
            // The reader has joined and released its root. Refill immediately;
            // a bounded batch is a fairness boundary, not a one-second throttle.
            self.start_verifications();
            return;
        }
        let original = self.state.job(job).unwrap().clone();
        let proof_params = verified
            .proofs
            .iter()
            .map(|(file, proof)| {
                serde_json::to_string(proof).map(|json| (*file, proof.generation.clone(), json))
            })
            .collect::<Result<Vec<_>, _>>();
        let proof_params = match proof_params {
            Ok(params) => params,
            Err(error) => {
                self.block_verification(
                    job,
                    VerificationFailure::unavailable(error, verified.checked_bytes),
                );
                self.dirty = true;
                self.publish_now();
                self.bump_epoch();
                return;
            }
        };
        let current = self.state.job_mut(job).unwrap();
        for (file, number) in &verified.invalid {
            if let Some(file) = current
                .files
                .iter_mut()
                .find(|candidate| candidate.id == *file)
            {
                if let Some(segment) = file
                    .segments
                    .iter_mut()
                    .find(|segment| segment.number == *number)
                {
                    // The ticket fences disk writers; still require the exact
                    // captured Done range so unrelated state can never be erased.
                    let captured = original
                        .files
                        .iter()
                        .find(|candidate| candidate.id == file.id)
                        .and_then(|file| {
                            file.segments
                                .iter()
                                .find(|segment| segment.number == *number)
                        });
                    if captured.is_some_and(|captured| captured.state == segment.state) {
                        segment.state = SegmentState::Pending;
                        file.finalized = false;
                        file.crc32 = None;
                    }
                }
            }
        }
        for file in &mut current.files {
            if verified.published.contains(&file.id) {
                file.finalized = true;
            }
            if verified.reopen_finalize.contains(&file.id) {
                file.finalized = false;
            }
        }
        for (file, file_generation, proof) in proof_params {
            set_owner_param(
                &mut current.params,
                &format!("*File:generation:{}", file.0),
                Some(file_generation),
            );
            set_owner_param(
                &mut current.params,
                &format!("*File:proof:{}", file.0),
                Some(proof),
            );
        }
        // Drop old proof on invalid ranges only when no replacement proof was
        // established for that file. Never let one new write bless legacy bits.
        for (file, _) in &verified.invalid {
            if !verified.proofs.iter().any(|(proven, _)| proven == file) {
                set_owner_param(
                    &mut current.params,
                    &format!("*File:proof:{}", file.0),
                    None,
                );
            }
        }
        let needs_download = !verified.transformed
            && (!verified.invalid.is_empty() || !verified.reopen_finalize.is_empty());
        accepted_hold_rewrite(current, verified.post_retry_validated, needs_download);
        // A job whose terminal is recorded never re-enters the download
        // queue (contract §2.1: the terminal is emitted once, never revised).
        let terminal = current
            .params
            .iter()
            .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM);
        if needs_download && !terminal && !current.held() && current.status != JobStatus::Paused {
            current.status = JobStatus::Queued;
        }
        recompute_job_totals(current);
        set_owner_param(&mut current.params, PENDING, None);
        set_owner_param(&mut current.params, REASON, None);
        self.dirty = true;
        // The fence stays through the actual durable acknowledgement. A
        // concurrently requested pause/control remains effective on failure.
        if verified.publication_repair {
            for file in &verified.repair_files {
                set_owner_param(
                    &mut self.state.job_mut(job).unwrap().params,
                    &format!("*File:generation:{}", file.file.0),
                    Some(file.generation.clone()),
                );
            }
            set_owner_param(
                &mut self.state.job_mut(job).unwrap().params,
                "*Recovery:publication-repair",
                serde_json::to_string(&PublicationRepairIntent {
                    protocol: 1,
                    installation: self.artifacts.installation.clone(),
                    allocation_generation: verified.allocation.generation.clone(),
                    files: verified.repair_files.clone(),
                })
                .ok(),
            );
        }
        if let Some(state) = self.verification.get_mut(&job) {
            state.phase = "saving".into();
            state.running = false;
        }
        self.persist_then(move |owner,committed| {
            if owner.job_epochs.get(&job)!=Some(&generation)
                || !owner.verification.get(&job).is_some_and(|state|state.generation==generation && state.phase=="saving") {return;}
            if !committed {
                if let Some(current)=owner.state.job_mut(job) {
                    let was_paused=current.status==JobStatus::Paused;
                    // Restore only recovery-owned evidence. User settings and
                    // control revisions may have changed while storage waited.
                    current.files=original.files;
                    current.totals=original.totals;
                    if !was_paused && !current.held(){current.status=original.status;}
                    current.params.retain(|(key,_)|!key.starts_with("*File:proof:") && !key.starts_with("*File:generation:") && key!="*Recovery:publication-repair");
                    current.params.extend(original.params.into_iter().filter(|(key,_)|key.starts_with("*File:proof:") || key.starts_with("*File:generation:") || key=="*Recovery:publication-repair"));
                }
                owner.block_verification(job,VerificationFailure::unavailable("verified recovery transition could not be persisted",verified.checked_bytes));
                owner.dirty=true;owner.publish_now();owner.bump_epoch();return;
            }
            owner.allocation_cache.insert(job,verified.allocation);
            if verified.publication_repair {
                // The retained partial is repairable by runner itself: the
                // retry restores the download-owned partial for repair. It is
                // retried on the ladder and bounded by the hold deadline
                // (contract §2.3); it is not a terminal.
                owner.hold_job(job,nzbd_types::HoldCause::StorageUnavailable,"finalize","legacy publication has proven invalid ranges; retry restores download-owned partial for repair");
                owner.block_verification(job,VerificationFailure::conflict("corrupt download publication requires an explicit generation-safe repair transition",verified.checked_bytes));
                owner.dirty=true;owner.publish_now();owner.bump_epoch();return;
            }
            owner.verification.remove(&job);
            owner.publish_now();owner.bump_epoch();owner.drain_replacements();
            if owner.state.job(job).is_some_and(|job|!job.held() && matches!(job.status,JobStatus::Queued|JobStatus::Downloading)) {
                let finalize=owner.state.job(job).unwrap().files.iter().filter(|file|file.is_terminal() && file.has_any_done() && !file.finalized).map(|file|file.id).collect::<Vec<_>>();
                for file in finalize{owner.send_finalize(job,file);}
                owner.check_job_complete(job);
            }
            // The reader has joined and its evidence is durably acknowledged.
            // Fill its freed root slot now; waiting for the one-second owner
            // tick adds a second per queued job after authority adoption.
            owner.start_verifications();
        });
    }
}

fn stage_publication_repair(
    job: JobId,
    allocation: &Artifact,
    inventory: &Arc<Inventory>,
    files: &[PublicationRepairFile],
) -> Result<(), String> {
    if inventory
        .has_transform_custody(job.0)
        .map_err(|error| error.to_string())?
    {
        return Err(
            "post-processing transform custody must be reconciled; download repair refused".into(),
        );
    }
    if inventory
        .delivery_for_job(job.0)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("delivery custody already exists; download repair refused".into());
    }
    let current = inventory
        .for_job(job.0)
        .map_err(|error| error.to_string())?
        .ok_or("allocation disappeared")?;
    if current.id != allocation.id
        || current.generation != allocation.generation
        || current.path != allocation.path
        || current.root != allocation.root
        || current.state != "active"
    {
        return Err("allocation changed; repair refused".into());
    }
    let _custody = inventory
        .acquire_terminal_use(job.0, &|| Ok(()))
        .map_err(|error| error.to_string())?
        .ok_or("allocation custody unavailable")?;
    for file in files {
        if !recovery::safe_name(&file.publication) {
            return Err("unsafe publication name".into());
        }
        let partial = allocation
            .path
            .join(format!(".runner-file-{}.part", file.file.0));
        let publication = allocation.path.join(&file.publication);
        let inspect = |path: &Path| -> Result<Option<Identity>, String> {
            match std::fs::symlink_metadata(path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(error.to_string()),
                Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
                    Err("repair path is not a regular file".into())
                }
                Ok(_) => {
                    let descriptor =
                        nzbd_state::fileops::open(path).map_err(|error| error.to_string())?;
                    recovery::capture_identity(&descriptor).map(Some)
                }
            }
        };
        let matching = |identity: &Identity| {
            file.identity.same_object(identity)
                && file.identity.bytes == identity.bytes
                && file.identity.modified == identity.modified
        };
        let final_identity = inspect(&publication)?;
        let partial_identity = inspect(&partial)?;
        if final_identity
            .as_ref()
            .is_some_and(|identity| !matching(identity))
            || partial_identity
                .as_ref()
                .is_some_and(|identity| !matching(identity))
        {
            return Err(
                "publication/partial identity changed since the accepted legacy mismatch".into(),
            );
        }
        match (final_identity, partial_identity) {
            (Some(_), None) => nzbd_state::fileops::rename_exclusive(&publication, &partial)
                .map_err(|error| error.to_string())?,
            (Some(_), Some(_)) => {
                // Both aliases name the same retained object. Remove only the
                // publication alias; the stable partial preserves every byte.
                std::fs::remove_file(&publication).map_err(|error| error.to_string())?;
            }
            (None, Some(_)) => {} // interrupted prior Retry: already staged
            (None, None) => {
                return Err("publication and partial are unavailable; evidence retained".into());
            }
        }
        std::fs::File::open(&allocation.path)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn done_bytes(job: &Job) -> u64 {
    job.files
        .iter()
        .flat_map(|file| &file.segments)
        .filter_map(|segment| match segment.state {
            SegmentState::Done { len, .. } => Some(u64::from(len)),
            _ => None,
        })
        .fold(0, u64::saturating_add)
}
fn parameter<'a>(job: &'a Job, name: &str) -> Option<&'a str> {
    job.params
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}
fn ranges(file: &nzbd_types::FileEntry) -> Vec<ProvenRange> {
    file.segments
        .iter()
        .filter_map(|segment| match segment.state {
            SegmentState::Done { offset, len, crc } => Some(ProvenRange {
                seg_number: segment.number,
                offset,
                len,
                crc,
            }),
            _ => None,
        })
        .collect()
}
fn checkpoint(cancel: &CancellationToken) -> std::io::Result<()> {
    if cancel.is_cancelled() {
        Err(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "verification cancelled",
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
fn verify_job(
    job: &Job,
    allocation: Artifact,
    inventory: &Arc<Inventory>,
    cancel: &CancellationToken,
    ticket: u64,
) -> Result<VerifiedJob, VerificationFailure> {
    verify_job_batch(job, allocation, inventory, cancel, ticket, None, false)
}

fn verify_job_batch(
    job: &Job,
    allocation: Artifact,
    inventory: &Arc<Inventory>,
    cancel: &CancellationToken,
    ticket: u64,
    partial: Option<VerifiedJob>,
    bounded: bool,
) -> Result<VerifiedJob, VerificationFailure> {
    let mut checked = partial.as_ref().map_or(0, |partial| partial.checked_bytes);
    let batch_started = std::time::Instant::now();
    let mut batch_bytes = 0u64;
    let mut batch_ranges = 0usize;
    if job
        .params
        .iter()
        .any(|(key, _)| key == "*Recovery:publication-repair")
    {
        return Err(VerificationFailure::conflict(
            "corrupt download publication requires an explicit generation-safe repair transition",
            0,
        ));
    }
    let check = || checkpoint(cancel);
    check().map_err(|error| VerificationFailure::unavailable(error, checked))?;
    let current = inventory
        .for_job(job.id.0)
        .map_err(|error| VerificationFailure::unavailable(error, checked))?
        .ok_or_else(|| VerificationFailure::conflict("allocation is no longer present", checked))?;
    if current.id != allocation.id
        || current.generation != allocation.generation
        || current.path != allocation.path
        || current.root != allocation.root
    {
        return Err(VerificationFailure::stale(
            "allocation authority changed before verification",
            checked,
        ));
    }
    let transformed = inventory
        .has_transform_custody(job.id.0)
        .map_err(|error| VerificationFailure::unavailable(error, checked))?;
    let old_rar_retry = job.control().is_some_and(|control| control.version==1 && control.lifecycle=="held"
        && !control.manual_pause && matches!(control.cause.as_str(), "unknown" | "post_failure")
        && control.stage=="rar_rename" && control.message=="subprocess failed: RAR set lacks checked order/membership evidence; filenames preserved");
    // validate_post_retry intentionally refuses an already-live AttemptUse.
    // Earn this permission first; acquire_terminal_use verifies the root and
    // current generation again and retains it throughout subsequent reads.
    let post_retry_validated = transformed || old_rar_retry;
    if post_retry_validated {
        inventory
            .validate_post_retry(job.id.0)
            .map_err(|error| VerificationFailure::conflict(error, checked))?;
    }
    // Review #283-1: a restart's reconciliation pass on this source (an open
    // publication move being finished) holds it momentarily; verification
    // serializes behind it instead of failing against it and backing off.
    let custody = inventory
        .acquire_terminal_use_after_reconciliation(job.id.0, &check)
        .map_err(|error| VerificationFailure::unavailable(error, checked))?
        .ok_or_else(|| {
            VerificationFailure::conflict("allocation custody is unavailable", checked)
        })?;
    if custody.source().artifact != allocation.id
        || custody.source().generation != allocation.generation
    {
        return Err(VerificationFailure::conflict(
            "allocation custody generation differs",
            checked,
        ));
    }
    if partial.as_ref().is_some_and(|prior| {
        prior.transformed != transformed || prior.post_retry_validated != post_retry_validated
    }) {
        return Err(VerificationFailure::conflict(
            "payload custody changed between verification batches",
            checked,
        ));
    }
    let mut verified = partial.unwrap_or_else(|| VerifiedJob {
        complete: false,
        completed_files: HashSet::new(),
        observations: HashMap::new(),
        partial_range: None,
        allocation: current,
        proofs: vec![],
        invalid: vec![],
        published: HashSet::new(),
        reopen_finalize: HashSet::new(),
        transformed,
        post_retry_validated,
        checked_bytes: 0,
        publication_repair: false,
        repair_files: vec![],
    });
    if transformed {
        // Existing PP operation journals own transformed inputs. Never compare
        // those paths to obsolete pre-transform download CRCs or reset Done.
        inventory
            .verified_custody_evidence(job.id.0, check)
            .map_err(|error| VerificationFailure::conflict(error, checked))?;
        verified.checked_bytes = done_bytes(job);
        verified.complete = true;
        return Ok(verified);
    }
    for (file, (publication, identity)) in &verified.observations {
        let current = recovery::resolve_payload(&verified.allocation.path, *file, publication)
            .map_err(|error| {
                VerificationFailure::conflict(
                    format!("batch identity reconciliation: {error:?}"),
                    checked,
                )
            })?;
        if current.as_ref().map(|resolved| &resolved.identity) != identity.as_ref() {
            return Err(VerificationFailure::conflict(
                "file identity changed between verification batches",
                checked,
            ));
        }
    }
    for file in &job.files {
        if verified.completed_files.contains(&file.id) {
            continue;
        }
        check().map_err(|error| VerificationFailure::unavailable(error, checked))?;
        let done = ranges(file);
        if done.is_empty() {
            verified.completed_files.insert(file.id);
            continue;
        }
        let file_generation = parameter(job, &format!("*File:generation:{}", file.id.0))
            .map(str::to_owned)
            .unwrap_or_else(|| format!("verification-{ticket}-{}", file.id.0));
        let proof = parameter(job, &format!("*File:proof:{}", file.id.0))
            .and_then(|raw| serde_json::from_str::<DurableFileProof>(raw).ok());
        let publication = parameter(job, &format!("*File:publication:{}", file.id.0))
            .or_else(|| {
                proof
                    .as_ref()
                    .and_then(|proof| proof.publication_name.as_deref())
            })
            .unwrap_or(&file.filename);
        let expected_size = parameter(job, &format!("*File:size:{}", file.id.0))
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|size| *size > 0);
        let resolved = recovery::resolve_payload(&verified.allocation.path, file.id, publication)
            .map_err(|error| match error {
            recovery::ResolveError::IdentityConflict => VerificationFailure::conflict(
                format!("file {} has conflicting payload identities", file.id.0),
                checked,
            ),
            recovery::ResolveError::Unavailable { message, .. } => {
                VerificationFailure::unavailable(message, checked)
            }
        })?;
        let Some(resolved) = resolved else {
            // acquire_terminal_use already established root/allocation identity.
            // A matching-generation proof contradicting absence is an integrity
            // hold; a legacy absent range is invalid only in download custody.
            if proof.as_ref().is_some_and(|proof| {
                proof.protocol == WRITER_PROTOCOL
                    && proof.generation == file_generation
                    && proof.allocation_generation == verified.allocation.generation
                    && proof.installation == inventory.installation
            }) {
                return Err(VerificationFailure::conflict(
                    format!(
                        "durably proven file {} is absent on its enrolled root",
                        file.id.0
                    ),
                    checked,
                ));
            }
            verified
                .invalid
                .extend(done.iter().map(|range| (file.id, range.seg_number)));
            verified
                .observations
                .insert(file.id, (publication.to_owned(), None));
            verified.completed_files.insert(file.id);
            continue;
        };
        let bound_proof = proof.as_ref().filter(|proof| {
            proof.protocol == WRITER_PROTOCOL
                && proof.generation == file_generation
                && proof.allocation_generation == verified.allocation.generation
                && proof.installation == inventory.installation
        });
        if bound_proof.is_some_and(|proof| !proof.identity.same_object(&resolved.identity)) {
            return Err(VerificationFailure::conflict(
                format!("durable file {} identity changed", file.id.0),
                checked,
            ));
        }
        // Every range in the queue must be coherent even when no proof exists.
        // An overlap is metadata ambiguity, not permission to redownload over it.
        if !recovery::coherent_ranges(&done, u64::MAX) {
            return Err(VerificationFailure::conflict(
                format!(
                    "file {} has overlapping or malformed completed ranges",
                    file.id.0
                ),
                checked,
            ));
        }
        let claimed: HashSet<ProvenRange> = bound_proof
            .map(|proof| proof.ranges.iter().cloned().collect())
            .unwrap_or_default();
        // Validate the complete proof once per file; per-range work is then
        // a set lookup rather than repeatedly checking all pairwise extents.
        let trust_metadata = recovery::metadata_trustworthy(&resolved.identity)
            && bound_proof.is_some_and(|proof| {
                proof.covers(
                    &file_generation,
                    &verified.allocation.generation,
                    &inventory.installation,
                    &resolved.identity,
                    &[],
                )
            });
        let mut valid = verified
            .proofs
            .iter()
            .find(|(id, _)| *id == file.id)
            .map(|(_, proof)| proof.ranges.clone())
            .unwrap_or_default();
        let mut accepted: HashSet<u32> = valid
            .iter()
            .map(|range| range.seg_number)
            .chain(
                verified
                    .invalid
                    .iter()
                    .filter(|(id, _)| *id == file.id)
                    .map(|(_, number)| *number),
            )
            .collect();
        // A range the writer's durable proof covers is not re-read, and a
        // file whose every range it covers is not even opened.
        let mut descriptor: Option<std::fs::File> = None;
        for range in &done {
            if accepted.contains(&range.seg_number) {
                continue;
            }
            check().map_err(|error| VerificationFailure::unavailable(error, checked))?;
            if bounded
                && (batch_ranges >= BATCH_RANGES
                    || batch_bytes >= BATCH_BYTES
                    || ((batch_ranges > 0 || batch_bytes > 0)
                        && batch_started.elapsed() >= std::time::Duration::from_millis(250)))
            {
                break;
            }
            if trust_metadata && claimed.contains(range) {
                valid.push(range.clone());
                accepted.insert(range.seg_number);
                batch_ranges += 1;
                continue;
            }
            if descriptor.is_none() {
                descriptor = Some(
                    nzbd_state::fileops::open(&resolved.path)
                        .map_err(|error| VerificationFailure::unavailable(error, checked))?,
                );
            }
            let cursor = verified.partial_range.get_or_insert_with(|| PartialRange {
                file: file.id,
                range: range.clone(),
                read: 0,
                digest: crc32fast::Hasher::new(),
            });
            if cursor.file != file.id || cursor.range != *range {
                return Err(VerificationFailure::conflict(
                    "range changed between verification batches",
                    checked,
                ));
            }
            let budget = if bounded {
                BATCH_BYTES.saturating_sub(batch_bytes)
            } else {
                u64::MAX
            };
            let (outcome, bytes, complete) = read_piece(
                descriptor.as_mut().expect("opened above"),
                &resolved.identity,
                cursor,
                budget,
            );
            checked = checked.saturating_add(bytes);
            batch_bytes = batch_bytes.saturating_add(bytes);
            if !complete {
                break;
            }
            verified.partial_range = None;
            batch_ranges += 1;
            match outcome {
                ValidationOutcome::Valid(_) => valid.push(range.clone()),
                ValidationOutcome::Mismatch | ValidationOutcome::Missing
                    if claimed.contains(range) =>
                {
                    return Err(VerificationFailure::conflict(
                        format!("file {} contradicts current durable range proof", file.id.0),
                        checked,
                    ));
                }
                ValidationOutcome::Mismatch | ValidationOutcome::Missing => {
                    verified.invalid.push((file.id, range.seg_number))
                }
                ValidationOutcome::Unavailable(reason) => {
                    return Err(VerificationFailure::unavailable(reason, checked));
                }
                ValidationOutcome::IdentityConflict => {
                    return Err(VerificationFailure::conflict(
                        "file changed during verification batch",
                        checked,
                    ));
                }
            }
            accepted.insert(range.seg_number);
        }
        let file_complete = done
            .iter()
            .all(|range| accepted.contains(&range.seg_number));
        // Stamp legacy verification only after a sync of the same opened object;
        // validate current identity both before and after that sync. A file
        // proven entirely by its durable writer proof was never opened: its
        // resolved identity is the one the proof matched.
        let identity = if let Some(descriptor) = &descriptor {
            let before = recovery::capture_identity(descriptor)
                .map_err(|error| VerificationFailure::unavailable(error, checked))?;
            if before != resolved.identity {
                return Err(VerificationFailure::conflict(
                    "file changed before verification checkpoint",
                    checked,
                ));
            }
            descriptor
                .sync_data()
                .map_err(|error| VerificationFailure::unavailable(error, checked))?;
            let identity = recovery::capture_identity(descriptor)
                .map_err(|error| VerificationFailure::unavailable(error, checked))?;
            if identity != before {
                return Err(VerificationFailure::conflict(
                    "file changed during verification checkpoint",
                    checked,
                ));
            }
            identity
        } else {
            resolved.identity.clone()
        };
        let final_path = verified.allocation.path.join(publication);
        let published = match std::fs::symlink_metadata(&final_path) {
            Ok(meta) if meta.is_file() && !meta.file_type().is_symlink() => {
                let target = nzbd_state::fileops::open(&final_path)
                    .map_err(|error| VerificationFailure::unavailable(error, checked))?;
                let target_identity = recovery::capture_identity(&target)
                    .map_err(|error| VerificationFailure::unavailable(error, checked))?;
                if !identity.same_object(&target_identity) {
                    return Err(VerificationFailure::conflict(
                        "publication identity differs",
                        checked,
                    ));
                }
                true
            }
            Ok(_) => {
                return Err(VerificationFailure::conflict(
                    "publication is not a regular file",
                    checked,
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(VerificationFailure::unavailable(error, checked)),
        };
        if file_complete && published && valid.len() != done.len() {
            verified.publication_repair = true;
            verified.repair_files.push(PublicationRepairFile {
                file: file.id,
                publication: publication.to_owned(),
                identity: identity.clone(),
                generation: file_generation.clone(),
            });
        }
        if file_complete && published && valid.len() == done.len() {
            let size = expected_size.ok_or_else(|| {
                VerificationFailure::unavailable(
                    "expected publication size is not recorded",
                    checked,
                )
            })?;
            if size != identity.bytes {
                return Err(VerificationFailure::conflict(
                    "publication length differs from expected size",
                    checked,
                ));
            }
            if file
                .segments
                .iter()
                .all(|segment| matches!(segment.state, SegmentState::Done { .. }))
            {
                verified.published.insert(file.id);
            }
        } else if file_complete && file.finalized {
            verified.reopen_finalize.insert(file.id);
        }
        verified
            .observations
            .insert(file.id, (publication.to_owned(), Some(identity.clone())));
        verified.proofs.retain(|(id, _)| *id != file.id);
        if !valid.is_empty() {
            verified.proofs.push((
                file.id,
                DurableFileProof {
                    protocol: WRITER_PROTOCOL,
                    generation: file_generation,
                    allocation_generation: verified.allocation.generation.clone(),
                    installation: inventory.installation.clone(),
                    identity,
                    ranges: valid,
                    publication_name: Some(publication.to_owned()),
                },
            ));
        }
        if file_complete {
            verified.completed_files.insert(file.id);
        } else {
            break;
        }
        if bounded
            && (batch_ranges >= BATCH_RANGES
                || batch_bytes >= BATCH_BYTES
                || ((batch_ranges > 0 || batch_bytes > 0)
                    && batch_started.elapsed() >= std::time::Duration::from_millis(250)))
        {
            break;
        }
    }
    verified.complete = job
        .files
        .iter()
        .all(|file| verified.completed_files.contains(&file.id));
    check().map_err(|error| VerificationFailure::unavailable(error, checked))?;
    let final_allocation = inventory
        .for_job(job.id.0)
        .map_err(|error| VerificationFailure::unavailable(error, checked))?
        .ok_or_else(|| {
            VerificationFailure::conflict("allocation disappeared while verifying", checked)
        })?;
    if final_allocation.id != verified.allocation.id
        || final_allocation.generation != verified.allocation.generation
        || final_allocation.path != verified.allocation.path
        || final_allocation.root != verified.allocation.root
    {
        return Err(VerificationFailure::conflict(
            "allocation changed while verifying",
            checked,
        ));
    }
    verified.allocation = final_allocation;
    verified.checked_bytes = checked;
    Ok(verified)
}

/// A pre-v2 hold whose bytes startup verification re-checks and releases
/// when they prove out (not a terminal: the boot conversion leaves it).
fn legacy_coverage_hold(control: &nzbd_types::JobControl) -> bool {
    // `identity_conflict` is how a pre-v2 build wrote it.
    matches!(
        control.cause.as_str(),
        "identity_conflict" | "storage_unavailable"
    ) && control.stage == "finalize"
        && control.message == super::COVERAGE_UNVERIFIED
}

fn legacy_rar_order_hold(control: &nzbd_types::JobControl) -> bool {
    matches!(control.cause.as_str(), "unknown" | "post_failure")
        && control.stage == "rar_rename"
        && control.message
            == "subprocess failed: RAR set lacks checked order/membership evidence; filenames preserved"
}

fn accepted_hold_rewrite(job: &mut Job, post_retry_validated: bool, needs_download: bool) {
    let Some(mut control) = job.control().filter(|control| {
        control.version == 1 && control.lifecycle == "held" && !control.manual_pause
    }) else {
        return;
    };
    let finalize = legacy_coverage_hold(&control);
    let rar = post_retry_validated && legacy_rar_order_hold(&control);
    if !finalize && !rar {
        return;
    }
    let Some(revision) = control
        .revision
        .parse::<u64>()
        .ok()
        .and_then(|revision| revision.checked_add(1))
    else {
        return;
    };
    control.revision = revision.to_string();
    control.lifecycle = "running".into();
    control.message = if finalize {
        "rechecking retained download for repair".into()
    } else {
        "retrying archive restoration with SFV evidence".into()
    };
    job.set_control(&control);
    job.status = if rar && !needs_download {
        JobStatus::PostQueued
    } else {
        JobStatus::Queued
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_io_never_becomes_an_invalid_range() {
        let error =
            VerificationFailure::unavailable(std::io::Error::from_raw_os_error(libc::EIO), 123);
        assert_eq!(error.checked_bytes, 123);
        assert!(!error.review_required);
        assert!(!error.reason.is_empty());
        let contradiction = VerificationFailure::conflict("current proof contradicts storage", 456);
        assert!(contradiction.review_required);
    }
    #[test]
    fn cancellation_checkpoint_is_distinct_from_corruption() {
        let cancellation = CancellationToken::new();
        assert!(checkpoint(&cancellation).is_ok());
        cancellation.cancel();
        assert_eq!(
            checkpoint(&cancellation).unwrap_err().kind(),
            std::io::ErrorKind::Interrupted
        );
    }
    fn fixture_job() -> Job {
        Job {
            id: JobId(7),
            kind: JobKind::Nzb,
            name: "fixture".into(),
            dir_name: "fixture".into(),
            name_provisional: false,
            delivery_lifecycle: false,
            queued_at_unix: 0,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: Default::default(),
            params: vec![("*File:size:1".into(), "6".into())],
            files: vec![nzbd_types::FileEntry {
                id: FileId(1),
                subject: "old-name".into(),
                filename: "old-name".into(),
                filename_confirmed: true,
                is_par2: false,
                paused: false,
                groups: vec![],
                date: None,
                crc32: None,
                finalized: false,
                segments: vec![
                    nzbd_types::Segment {
                        message_id: "a".into(),
                        number: 1,
                        size: 3,
                        state: SegmentState::Done {
                            offset: 0,
                            len: 3,
                            crc: crc32fast::hash(b"abc"),
                        },
                    },
                    nzbd_types::Segment {
                        message_id: "b".into(),
                        number: 2,
                        size: 3,
                        state: SegmentState::Done {
                            offset: 3,
                            len: 3,
                            crc: crc32fast::hash(b"def"),
                        },
                    },
                ],
            }],
            totals: Default::default(),
            status: JobStatus::Paused,
            torrent: None,
            stages: vec![],
        }
    }

    fn fixture_inventory(temp: &tempfile::TempDir) -> (Arc<Inventory>, Artifact) {
        let root = temp.path().join("payloads");
        std::fs::create_dir_all(&root).unwrap();
        let inventory = Arc::new(Inventory::open(&temp.path().join("state")).unwrap());
        let allocation = inventory.allocate(7, &root, &root.join("fixture")).unwrap();
        (inventory, allocation)
    }

    #[test]
    fn bounded_batches_keep_progress_and_same_object_cursor() {
        let temp = tempfile::tempdir().unwrap();
        let (inventory, allocation) = fixture_inventory(&temp);
        let mut job = fixture_job();
        let bytes: Vec<u8> = (0..128).map(|n| n as u8).collect();
        std::fs::write(allocation.path.join(".runner-file-1.part"), &bytes).unwrap();
        job.files[0].segments = bytes
            .iter()
            .enumerate()
            .map(|(index, byte)| nzbd_types::Segment {
                message_id: index.to_string().into(),
                number: index as u32 + 1,
                size: 1,
                state: SegmentState::Done {
                    offset: index as u64,
                    len: 1,
                    crc: crc32fast::hash(&[*byte]),
                },
            })
            .collect();
        let cancel = CancellationToken::new();
        let first = verify_job_batch(
            &job,
            allocation.clone(),
            &inventory,
            &cancel,
            11,
            None,
            true,
        )
        .unwrap();
        assert!(!first.complete);
        assert!(first.checked_bytes > 0 && first.checked_bytes <= 64);
        assert_eq!(first.proofs[0].1.ranges.len() as u64, first.checked_bytes);
        let mut result = first;
        for _ in 0..128 {
            if result.complete {
                break;
            }
            result = verify_job_batch(
                &job,
                allocation.clone(),
                &inventory,
                &cancel,
                11,
                Some(result),
                true,
            )
            .unwrap();
        }
        assert!(result.complete);
        assert_eq!(
            result.checked_bytes, 128,
            "previous batch must not reread payload"
        );
        assert_eq!(result.proofs[0].1.ranges.len(), 128);
    }

    #[test]
    fn one_large_range_yields_and_resumes_crc_without_rereading() {
        let temp = tempfile::tempdir().unwrap();
        let (inventory, allocation) = fixture_inventory(&temp);
        let bytes = vec![0x5au8; BATCH_BYTES as usize + 4096];
        std::fs::write(allocation.path.join(".runner-file-1.part"), &bytes).unwrap();
        let mut job = fixture_job();
        job.files[0].segments.truncate(1);
        job.files[0].segments[0].size = bytes.len() as u32;
        job.files[0].segments[0].state = SegmentState::Done {
            offset: 0,
            len: bytes.len() as u32,
            crc: crc32fast::hash(&bytes),
        };
        let cancel = CancellationToken::new();
        let first = verify_job_batch(
            &job,
            allocation.clone(),
            &inventory,
            &cancel,
            11,
            None,
            true,
        )
        .unwrap();
        assert!(!first.complete);
        assert_eq!(first.checked_bytes, BATCH_BYTES);
        assert_eq!(first.partial_range.as_ref().unwrap().read, BATCH_BYTES);
        let second =
            verify_job_batch(&job, allocation, &inventory, &cancel, 11, Some(first), true).unwrap();
        assert!(second.complete);
        assert_eq!(second.checked_bytes, bytes.len() as u64);
        assert!(second.invalid.is_empty());
    }

    #[test]
    fn identity_change_between_batches_never_reuses_prior_crc() {
        let temp = tempfile::tempdir().unwrap();
        let (inventory, allocation) = fixture_inventory(&temp);
        let bytes = vec![0x5au8; BATCH_BYTES as usize + 4096];
        let path = allocation.path.join(".runner-file-1.part");
        std::fs::write(&path, &bytes).unwrap();
        let mut job = fixture_job();
        job.files[0].segments.truncate(1);
        job.files[0].segments[0].state = SegmentState::Done {
            offset: 0,
            len: bytes.len() as u32,
            crc: crc32fast::hash(&bytes),
        };
        let cancel = CancellationToken::new();
        let first = verify_job_batch(
            &job,
            allocation.clone(),
            &inventory,
            &cancel,
            11,
            None,
            true,
        )
        .unwrap();
        std::fs::rename(&path, path.with_extension("old")).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        let failure =
            verify_job_batch(&job, allocation, &inventory, &cancel, 11, Some(first), true)
                .unwrap_err();
        assert!(failure.review_required);
        assert!(failure.reason.contains("identity"));
    }

    #[test]
    fn legacy_mismatch_invalidates_only_bad_range_and_stamps_only_checked_scope() {
        let temp = tempfile::tempdir().unwrap();
        let (inventory, allocation) = fixture_inventory(&temp);
        std::fs::write(allocation.path.join(".runner-file-1.part"), b"abcBAD").unwrap();
        let job = fixture_job();
        let verified =
            verify_job(&job, allocation, &inventory, &CancellationToken::new(), 11).unwrap();
        assert_eq!(verified.invalid, vec![(FileId(1), 2)]);
        assert_eq!(verified.proofs.len(), 1);
        assert_eq!(verified.proofs[0].1.ranges.len(), 1);
        assert_eq!(verified.proofs[0].1.ranges[0].seg_number, 1);
        assert_eq!(verified.checked_bytes, 6);
        // Workers propose evidence; they never reset saved ranges or user intent.
        assert_eq!(job.status, JobStatus::Paused);
        assert!(job.files[0]
            .segments
            .iter()
            .all(|segment| matches!(segment.state, SegmentState::Done { .. })));
    }

    #[test]
    fn missing_allocation_is_uncertain_but_missing_legacy_file_on_verified_root_is_invalid() {
        let temp = tempfile::tempdir().unwrap();
        let (inventory, allocation) = fixture_inventory(&temp);
        let job = fixture_job();
        let verified = verify_job(
            &job,
            allocation.clone(),
            &inventory,
            &CancellationToken::new(),
            11,
        )
        .unwrap();
        assert_eq!(verified.invalid, vec![(FileId(1), 1), (FileId(1), 2)]);
        std::fs::remove_dir_all(&allocation.path).unwrap();
        assert!(verify_job(&job, allocation, &inventory, &CancellationToken::new(), 12).is_err());
        assert!(job.files[0]
            .segments
            .iter()
            .all(|segment| matches!(segment.state, SegmentState::Done { .. })));
    }

    #[test]
    fn current_proof_fast_path_and_truncation_contradiction_are_distinct() {
        let temp = tempfile::tempdir().unwrap();
        let (inventory, allocation) = fixture_inventory(&temp);
        let path = allocation.path.join(".runner-file-1.part");
        std::fs::write(&path, b"abcdef").unwrap();
        let mut job = fixture_job();
        let first = verify_job(
            &job,
            allocation.clone(),
            &inventory,
            &CancellationToken::new(),
            11,
        )
        .unwrap();
        let proof = &first.proofs[0].1;
        set_owner_param(
            &mut job.params,
            "*File:generation:1",
            Some(proof.generation.clone()),
        );
        set_owner_param(
            &mut job.params,
            "*File:proof:1",
            Some(serde_json::to_string(proof).unwrap()),
        );
        let fast = verify_job(
            &job,
            allocation.clone(),
            &inventory,
            &CancellationToken::new(),
            12,
        )
        .unwrap();
        assert_eq!(fast.checked_bytes, 0);
        assert!(fast.invalid.is_empty());
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(2)
            .unwrap();
        let contradiction =
            verify_job(&job, allocation, &inventory, &CancellationToken::new(), 13).unwrap_err();
        assert!(contradiction.review_required);
        assert!(job.files[0]
            .segments
            .iter()
            .all(|segment| matches!(segment.state, SegmentState::Done { .. })));
    }

    #[test]
    fn final_only_recovery_uses_recorded_publication_and_requires_expected_size() {
        let temp = tempfile::tempdir().unwrap();
        let (inventory, allocation) = fixture_inventory(&temp);
        let final_path = allocation.path.join("published-name");
        std::fs::write(&final_path, b"abcdef").unwrap();
        let mut job = fixture_job();
        set_owner_param(
            &mut job.params,
            "*File:publication:1",
            Some("published-name".into()),
        );
        let verified = verify_job(
            &job,
            allocation.clone(),
            &inventory,
            &CancellationToken::new(),
            11,
        )
        .unwrap();
        assert!(verified.published.contains(&FileId(1)));
        assert!(verified.invalid.is_empty());
        assert!(!allocation.path.join(".runner-file-1.part").exists());
        set_owner_param(&mut job.params, "*File:size:1", None);
        let unresolved =
            verify_job(&job, allocation, &inventory, &CancellationToken::new(), 12).unwrap_err();
        assert!(!unresolved.review_required);
        assert!(unresolved.reason.contains("size"));
    }
    #[test]
    fn explicit_legacy_repair_staging_preserves_bytes_and_retries_idempotently() {
        let temp = tempfile::tempdir().unwrap();
        let (inventory, allocation) = fixture_inventory(&temp);
        let publication = allocation.path.join("published-name");
        std::fs::write(&publication, b"abcBAD").unwrap();
        let mut job = fixture_job();
        set_owner_param(
            &mut job.params,
            "*File:publication:1",
            Some("published-name".into()),
        );
        let checked = verify_job(
            &job,
            allocation.clone(),
            &inventory,
            &CancellationToken::new(),
            11,
        )
        .unwrap();
        assert_eq!(checked.invalid, vec![(FileId(1), 2)]);
        assert!(checked.publication_repair);
        assert_eq!(checked.repair_files.len(), 1);
        stage_publication_repair(job.id, &allocation, &inventory, &checked.repair_files).unwrap();
        let partial = allocation.path.join(".runner-file-1.part");
        assert!(!publication.exists());
        assert_eq!(std::fs::read(&partial).unwrap(), b"abcBAD");
        stage_publication_repair(job.id, &allocation, &inventory, &checked.repair_files).unwrap();
        assert_eq!(std::fs::read(&partial).unwrap(), b"abcBAD");
        // The owner accepted only the bad legacy range as Pending. The
        // surviving extent is checked again before a replacement writer opens.
        job.files[0].segments[1].state = SegmentState::Pending;
        set_owner_param(
            &mut job.params,
            "*File:generation:1",
            Some("repair-new-generation".into()),
        );
        let rechecked =
            verify_job(&job, allocation, &inventory, &CancellationToken::new(), 12).unwrap();
        assert!(rechecked.invalid.is_empty());
        assert_eq!(rechecked.proofs[0].1.ranges.len(), 1);
        assert_eq!(rechecked.proofs[0].1.ranges[0].seg_number, 1);
    }

    #[test]
    fn explicit_repair_refuses_a_replaced_publication_identity() {
        let temp = tempfile::tempdir().unwrap();
        let (inventory, allocation) = fixture_inventory(&temp);
        let publication = allocation.path.join("published-name");
        std::fs::write(&publication, b"abcBAD").unwrap();
        let mut job = fixture_job();
        set_owner_param(
            &mut job.params,
            "*File:publication:1",
            Some("published-name".into()),
        );
        let checked = verify_job(
            &job,
            allocation.clone(),
            &inventory,
            &CancellationToken::new(),
            11,
        )
        .unwrap();
        // Keep the old inode alive so the replacement cannot reuse it.
        let held = nzbd_state::fileops::open(&publication).unwrap();
        std::fs::remove_file(&publication).unwrap();
        std::fs::write(&publication, b"abcdef").unwrap();
        assert!(
            stage_publication_repair(job.id, &allocation, &inventory, &checked.repair_files)
                .is_err()
        );
        assert_eq!(std::fs::read(&publication).unwrap(), b"abcdef");
        assert!(!allocation.path.join(".runner-file-1.part").exists());
        drop(held);
    }
}
