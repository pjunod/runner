//! The owner's side of the terminal record (contract §2.1; `crate::terminal`).
//!
//! Every producer reaches [`Owner::record_terminal_then`]: the owner's own
//! download-end and fetch failures, hold expiry and legacy-hold conversion
//! directly, post-processing and the operator delete through
//! `QueueCommand::RecordTerminal`. The chain is row + history (persistence
//! lane) → markers on the queue row (snapshot commit) → emit, each marked on
//! the row; [`Owner::replay_terminals`] finishes any chain a crash cut short,
//! at boot and on the tick.
use super::*;
use crate::terminal::{self, TerminalError};
use nzbd_state::artifacts::{JobTerminal, TerminalStep};
use nzbd_types::{FailureReason, FetchCause};

pub(super) type TerminalCallback = Box<dyn FnOnce(&mut Owner, Option<JobTerminal>) + Send>;

/// How often the tick replays unfinished terminal rows.
pub(super) const TERMINAL_REPLAY_EVERY: Duration = Duration::from_secs(30);

impl Owner {
    /// End `job` as `failed/<reason>`. Refused (false) when the job is gone,
    /// already terminal, or a terminal for it is already being recorded.
    pub(super) fn fail_job_terminal(
        &mut self,
        job: JobId,
        reason: FailureReason,
        fetch_cause: Option<FetchCause>,
        stage: &str,
        message: &str,
    ) -> bool {
        self.fail_job_terminal_then(
            job,
            reason,
            fetch_cause,
            stage,
            message,
            Box::new(|_, _| {}),
        )
    }

    pub(super) fn fail_job_terminal_then(
        &mut self,
        job: JobId,
        reason: FailureReason,
        fetch_cause: Option<FetchCause>,
        stage: &str,
        message: &str,
        callback: TerminalCallback,
    ) -> bool {
        // Gone, already terminal, or another terminal for it already in
        // flight or waiting: the first one stands.
        let settled = self.terminals_in_flight.contains(&job)
            || self.deferred_terminals.contains_key(&job)
            || self.state.job(job).is_none_or(|row| {
                row.params
                    .iter()
                    .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
            });
        if settled {
            callback(self, None);
            return false;
        }
        let row = self.state.job(job).expect("checked above");
        // The same fences a hold respects (review #280-5): an automatic
        // control transition or any other control commit still in flight
        // owns the job's next state, and a fenced store cannot take a row.
        // The terminal is not dropped: it waits and is retried on the tick.
        let fenced = self.persistence_fenced
            || self.automatic_control_pending(job)
            || self.pending_control_commits.contains(&job);
        if fenced {
            let terminal = terminal::failed_terminal(row, reason, fetch_cause, stage, message);
            tracing::warn!(
                job = job.0,
                reason = reason.as_str(),
                "terminal waits for an in-flight control commit"
            );
            self.deferred_terminals.insert(job, (terminal, None));
            callback(self, None);
            return false;
        }
        tracing::warn!(
            job = job.0,
            reason = reason.as_str(),
            stage,
            %message,
            "job failed; recording its terminal before anything touches the bytes"
        );
        let terminal = terminal::failed_terminal(row, reason, fetch_cause, stage, message);
        self.record_terminal_then(terminal, callback);
        true
    }

    /// Run the terminal chain for `terminal` (a fresh producer's row or a
    /// stored row being replayed).
    pub(super) fn record_terminal_then(
        &mut self,
        terminal: JobTerminal,
        callback: TerminalCallback,
    ) {
        self.record_terminal_with_then(terminal, None, callback)
    }

    pub(super) fn record_terminal_with_then(
        &mut self,
        terminal: JobTerminal,
        history: Option<Arc<nzbd_state::history::HistoryDb>>,
        callback: TerminalCallback,
    ) {
        let job = JobId(terminal.job);
        if self.persistence_fenced {
            self.deferred_terminals
                .entry(job)
                .or_insert((terminal, history));
            callback(self, None);
            return;
        }
        if !self.terminals_in_flight.insert(job) {
            callback(self, None);
            return;
        }
        self.pending_control_commits.insert(job);
        // Contract §2.1: a job's terminal row, once stored, is authoritative —
        // a later attempt never replaces it; `record_terminal` returns the
        // stored row and the chain resumes from its flags.
        let inventory = self.artifacts.clone();
        let terminal_retry = terminal.clone();
        let history_retry = history.clone();
        let history = history.or_else(|| self.history.clone());
        self.io_then(
            move || {
                let Some(history) = history else {
                    return Ok(Err(TerminalError::Persistence(
                        terminal::TerminalFault::plain("no history store for the terminal"),
                    )));
                };
                Ok(terminal::persist_terminal(&inventory, &history, &terminal))
            },
            move |owner, result| match result {
                Ok(Ok(row)) => owner.stamp_terminal_then(row, callback),
                Ok(Err(TerminalError::InjectedCrash)) => {
                    owner.release_terminal(job);
                    callback(owner, None);
                }
                Ok(Err(TerminalError::NotRecorded(error))) => {
                    // Review #280-4: no row to replay — keep the terminal
                    // itself and retry it, so the job cannot re-complete.
                    owner
                        .deferred_terminals
                        .entry(job)
                        .or_insert((terminal_retry, history_retry));
                    owner.terminal_not_durable(job, error, callback)
                }
                Ok(Err(TerminalError::Persistence(error))) => {
                    owner.terminal_not_durable(job, error, callback)
                }
                Err(error) => {
                    owner
                        .deferred_terminals
                        .entry(job)
                        .or_insert((terminal_retry, history_retry));
                    owner.terminal_not_durable(job, terminal::TerminalFault::of(&error), callback)
                }
            },
        );
    }

    /// Retry the terminals kept by [`Self::record_terminal_with_then`] and
    /// [`Self::fail_job_terminal_then`] whose fence has lifted.
    fn retry_deferred_terminals(&mut self) {
        let ready = self
            .deferred_terminals
            .keys()
            .copied()
            .filter(|job| {
                !self.terminals_in_flight.contains(job)
                    && !self.pending_control_commits.contains(job)
                    && !self.automatic_control_pending(*job)
            })
            .collect::<Vec<_>>();
        for job in ready {
            let Some((terminal, history)) = self.deferred_terminals.remove(&job) else {
                continue;
            };
            tracing::info!(
                job = job.0,
                "retrying a terminal that had not been recorded"
            );
            self.record_terminal_with_then(terminal, history, Box::new(|_, _| {}));
        }
    }

    fn release_terminal(&mut self, job: JobId) {
        self.terminals_in_flight.remove(&job);
        self.pending_control_commits.remove(&job);
        // The chain's fence lifted (its `emitted` flag is durable, or it
        // stopped): a caller deferred on it waits for this queue change
        // (review #283-1 — the event goes out before the flag is written).
        self.publish_now();
        self.bump_epoch();
    }

    /// A terminal-store write or read that failed after the terminal itself
    /// is settled (the replay finishes it later). A full state volume still
    /// latches out-of-space, so downloads stop filling it.
    pub(super) fn terminal_store_fault(&mut self, whence: &str, fault: &StoreFault) {
        if crate::out_of_space(fault.cause.as_ref()) {
            self.observe_space_failure(whence, crate::space_probe::SpaceOp::Terminal, None);
        }
    }

    /// Contract §2.1: a terminal that could not be written holds the job
    /// `storage_unavailable` on stage `persistence`; the row (if it landed)
    /// is replayed and the event goes out when the write lands.
    fn terminal_not_durable(
        &mut self,
        job: JobId,
        error: terminal::TerminalFault,
        callback: TerminalCallback,
    ) {
        self.release_terminal(job);
        tracing::error!(job = job.0, %error, "terminal record is not durable; holding the job until storage answers");
        if crate::out_of_space(error.cause.as_ref()) {
            self.observe_space_failure(
                "terminal record",
                crate::space_probe::SpaceOp::Terminal,
                None,
            );
        }
        if self.state.job(job).is_some() {
            self.hold_job(
                job,
                nzbd_types::HoldCause::StorageUnavailable,
                "persistence",
                &format!("the terminal record could not be written: {error}"),
            );
        }
        callback(self, None);
    }

    fn stamp_terminal_then(&mut self, row: JobTerminal, callback: TerminalCallback) {
        let job = JobId(row.job);
        let present = self.state.job(job).is_some();
        let stamped = self
            .state
            .job(job)
            .is_some_and(|current| terminal::queue_row_stamped(current, &row));
        if row.markers_stamped || !present || stamped {
            // Nothing (left) to stamp: the queue row is gone (an operator
            // delete, a retired row) or already carries this stamp.
            if row.markers_stamped {
                self.emit_terminal_then(row, callback);
                return;
            }
            let inventory = self.artifacts.clone();
            self.io_then(
                move || {
                    Ok(inventory
                        .mark_terminal_step(row.job, &TerminalStep::MarkersStamped)
                        .map(|()| {
                            let mut row = row;
                            row.markers_stamped = true;
                            row
                        })
                        .map_err(|error| terminal::TerminalFault::of(&error)))
                },
                move |owner, result| match result {
                    Ok(Ok(row)) => owner.emit_terminal_then(row, callback),
                    Ok(Err(error)) => owner.terminal_not_durable(job, error, callback),
                    Err(error) => owner.terminal_not_durable(
                        job,
                        terminal::TerminalFault::of(&error),
                        callback,
                    ),
                },
            );
            return;
        }
        let before = self.state.job(job).cloned();
        {
            let current = self.state.job_mut(job).expect("present");
            terminal::stamp_queue_row(current, &row);
        }
        for file in self
            .state
            .job(job)
            .map(|row| row.files.iter().map(|f| f.id).collect::<Vec<_>>())
            .unwrap_or_default()
        {
            self.finalize_sent.remove(&file);
        }
        self.pending_finalize.retain(|(id, _)| *id != job);
        self.dirty = true;
        self.bump_epoch();
        let inventory = self.artifacts.clone();
        self.persist_then(move |owner, ok| {
            if !ok {
                if let (Some(before), Some(current)) = (before, owner.state.job_mut(job)) {
                    *current = before;
                }
                owner.terminal_not_durable(
                    job,
                    terminal::TerminalFault::plain("the queue snapshot did not commit"),
                    callback,
                );
                return;
            }
            owner.io_then(
                move || {
                    Ok(inventory
                        .mark_terminal_step(row.job, &TerminalStep::MarkersStamped)
                        .map(|()| {
                            let mut row = row;
                            row.markers_stamped = true;
                            row
                        })
                        .map_err(|error| terminal::TerminalFault::of(&error)))
                },
                move |owner, result| match result {
                    Ok(Ok(row)) => owner.emit_terminal_then(row, callback),
                    Ok(Err(error)) => owner.terminal_not_durable(job, error, callback),
                    Err(error) => owner.terminal_not_durable(
                        job,
                        terminal::TerminalFault::of(&error),
                        callback,
                    ),
                },
            );
        });
    }

    fn emit_terminal_then(&mut self, row: JobTerminal, callback: TerminalCallback) {
        let job = JobId(row.job);
        if self
            .artifacts
            .take_injected_terminal_crash(row.job, &TerminalStep::MarkersStamped)
        {
            self.release_terminal(job);
            callback(self, None);
            return;
        }
        if row.emitted {
            self.release_terminal(job);
            self.publish_now();
            callback(self, Some(row));
            return;
        }
        self.publish_now();
        let Some(event) = terminal::event_for(&row) else {
            // Unreachable: the chain stamps only after the history step.
            self.release_terminal(job);
            callback(self, None);
            return;
        };
        self.emit(event);
        let inventory = self.artifacts.clone();
        self.io_then(
            move || {
                // The store error is classified here, before it is rendered:
                // a full state volume is the out-of-space latch, not
                // corruption (review #296-4).
                Ok(inventory
                    .mark_terminal_step(row.job, &TerminalStep::Emitted)
                    .map(|()| {
                        let mut row = row;
                        row.emitted = true;
                        row
                    })
                    .map_err(|error| StoreFault::of(&error)))
            },
            move |owner, result| {
                owner.release_terminal(job);
                match result {
                    Ok(Ok(row)) => {
                        // W5a: the terminal store took a write, and this
                        // job's post-processing reached its end.
                        owner.space_op_succeeded(&crate::space_probe::SpaceOp::Terminal);
                        owner.space_op_succeeded(&crate::space_probe::SpaceOp::Post { job });
                        callback(owner, Some(row))
                    }
                    Ok(Err(fault)) => {
                        owner.terminal_store_fault("terminal emitted flag", &fault);
                        tracing::warn!(job = job.0, error = %fault.message, "terminal emitted; its emitted flag will be replayed");
                        callback(owner, None);
                    }
                    Err(error) => {
                        tracing::warn!(job = job.0, %error, "terminal emitted; its emitted flag will be replayed");
                        callback(owner, None);
                    }
                }
            },
        );
    }

    /// Finish every terminal chain a crash or a storage fault cut short.
    /// Runs at boot before hold recovery and the post manager, then on the
    /// tick.
    pub(super) fn replay_terminals(&mut self) {
        if self.persistence_fenced || !self.persist {
            return;
        }
        self.retry_deferred_terminals();
        let inventory = self.artifacts.clone();
        self.io_then(
            move || {
                Ok(inventory
                    .unfinished_terminals()
                    .map_err(|error| StoreFault::of(&error)))
            },
            |owner, result| match result {
                Ok(Err(fault)) => {
                    owner.terminal_store_fault("terminal replay", &fault);
                    tracing::warn!(error = %fault.message, "terminal replay could not read job_terminals")
                }
                Ok(Ok(rows)) => {
                    // W5a: nothing left to replay means a recorded terminal
                    // store failure has no operation left to retry.
                    if rows.is_empty()
                        && owner.terminals_in_flight.is_empty()
                        && owner.deferred_terminals.is_empty()
                    {
                        owner.space_op_succeeded(&crate::space_probe::SpaceOp::Terminal);
                    }
                    for row in rows {
                        let job = JobId(row.job);
                        if owner.terminals_in_flight.contains(&job) {
                            continue;
                        }
                        tracing::info!(job = row.job, "replaying an unfinished terminal record");
                        owner.record_terminal_then(row, Box::new(|_, _| {}));
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "terminal replay could not read job_terminals")
                }
            },
        );
    }

    /// Contract §2.3: a persisted control whose cause is one of the pre-v2
    /// terminal causes (`io`, `identity_conflict`, `post_failure`) becomes
    /// the terminal disposition the contract names for it — with the event —
    /// before any hold recovery runs.
    pub(super) fn convert_legacy_holds(&mut self) {
        // Startup reconciliation queues payload verification first; until
        // it has, a job cannot be told apart from one verification owns.
        if !self.startup_roots_ready {
            return;
        }
        let legacy = self
            .state
            .jobs
            .iter()
            // Startup verification owns a job until it settles (it may
            // release a legacy hold whose bytes prove out); the tick
            // converts whatever is still held afterwards.
            .filter(|job| {
                !self.verification.contains_key(&job.id)
                    && !self.pending_control_commits.contains(&job.id)
                    && !self.terminals_in_flight.contains(&job.id)
            })
            .filter_map(|job| {
                let control = job.control().filter(|c| c.lifecycle == "held")?;
                let reason = nzbd_types::HoldCause::legacy_terminal(&control.cause)?;
                Some((job.id, reason, control.stage, control.message))
            })
            .collect::<Vec<_>>();
        for (job, reason, stage, message) in legacy {
            tracing::warn!(
                job = job.0,
                reason = reason.as_str(),
                %stage,
                "converting a legacy terminal hold into its terminal failure"
            );
            self.fail_job_terminal(job, reason, None, &stage, &message);
        }
    }

    /// Contract §2.3: every held control carries a deadline. A hold stored
    /// by an earlier build has none; it gets one from now (an upgrade must
    /// not expire a week-old hold at boot without the operator seeing it).
    pub(super) fn arm_hold_deadlines(&mut self) {
        let now = unix_now();
        let mut changed = false;
        for job in &mut self.state.jobs {
            let Some(mut control) = job.control().filter(|c| {
                c.lifecycle == "held"
                    && c.deadline_unix.is_none()
                    && !terminal::seed_retirement_stage(&c.stage)
            }) else {
                continue;
            };
            let deadline =
                now.saturating_add(self.tuning.hold_deadline_secs(&control.hold_cause()));
            control.deadline_unix = Some(deadline);
            control.deadline_in_s = Some(deadline.saturating_sub(now));
            job.set_control(&control);
            changed = true;
        }
        if changed {
            self.dirty = true;
        }
    }

    /// Contract §2.3: a hold past its deadline ends `failed/hold_expired`
    /// with the hold's stage and message; its bytes are parked.
    pub(super) fn expire_hold(&mut self, job: JobId, revision: &str) -> bool {
        let Some(control) = self
            .state
            .job(job)
            .and_then(Job::control)
            .filter(|c| c.lifecycle == "held" && c.revision == revision)
        else {
            return false;
        };
        if !control
            .deadline_unix
            .is_some_and(|deadline| deadline <= unix_now())
        {
            return false;
        }
        let message = format!(
            "{} hold expired after its deadline: {}",
            control.cause, control.message
        );
        self.fail_job_terminal(
            job,
            FailureReason::HoldExpired,
            None,
            &control.stage,
            &message,
        )
    }

    /// Emit `job_status_changed` for every job whose projected status or
    /// stage changed since the last published snapshot (contract §2.4).
    /// `projected` is the published snapshot's `(job, status)`: the
    /// owner's status with the snapshot overlay (a job under verification
    /// or with a control commit in flight reads `queued`).
    pub(super) fn emit_status_changes(&mut self, projected: &[(JobId, JobStatus)]) {
        let mut seen = HashSet::new();
        let mut changes = Vec::new();
        for (id, published) in projected {
            let Some(job) = self.state.job(*id) else {
                continue;
            };
            seen.insert(job.id);
            // A held job's status is `Held` (M6b), so the published status
            // is the projection.
            let status = published.as_str();
            let stage = match published {
                JobStatus::Post { stage } => Some(stage.as_str()),
                _ => None,
            };
            if self.last_status.get(&job.id) != Some(&(status, stage)) {
                self.last_status.insert(job.id, (status, stage));
                changes.push(Event::JobStatusChanged {
                    job: job.id,
                    transfer: nzbd_types::transfer_of(&job.params),
                    status: status.to_string(),
                    stage: stage.map(str::to_string),
                    control: job.control(),
                });
            }
        }
        self.last_status.retain(|job, _| seen.contains(job));
        for change in changes {
            self.emit(change);
        }
    }
}

/// A terminal-store failure, classified while the typed error is in hand.
#[derive(Debug)]
pub(super) struct StoreFault {
    pub(super) cause: Option<nzbd_types::HoldCause>,
    pub(super) message: String,
}
impl StoreFault {
    pub(super) fn of(error: &nzbd_state::artifacts::Error) -> Self {
        StoreFault {
            cause: crate::error_hold_cause(error),
            message: error.to_string(),
        }
    }
}
