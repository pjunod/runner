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
        let refused = self.persistence_fenced
            || self.terminals_in_flight.contains(&job)
            || self.state.job(job).is_none_or(|row| {
                row.params
                    .iter()
                    .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
            });
        if refused {
            callback(self, None);
            return false;
        }
        let row = self.state.job(job).expect("checked above");
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
        let job = JobId(terminal.job);
        if self.persistence_fenced || !self.terminals_in_flight.insert(job) {
            callback(self, None);
            return;
        }
        self.pending_control_commits.insert(job);
        // A queue row without `*PP:done` is a fresh attempt: a stamped row
        // left by an earlier attempt of the same job id (a requeue or a
        // post-processing restart) is history, not this attempt's terminal.
        let fresh_attempt = self.state.job(job).is_some_and(|row| {
            !row.params
                .iter()
                .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
        });
        let inventory = self.artifacts.clone();
        let history = self.history.clone();
        self.io_then(
            move || {
                if fresh_attempt {
                    if let Some(existing) = inventory
                        .terminal(terminal.job)
                        .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?
                    {
                        if existing.markers_stamped {
                            inventory.forget_terminal(terminal.job).map_err(|error| {
                                nzbd_state::StateError::Corrupt(error.to_string())
                            })?;
                        }
                    }
                }
                Ok(terminal::persist_terminal(
                    &inventory,
                    history.as_deref(),
                    &terminal,
                ))
            },
            move |owner, result| match result {
                Ok(Ok(row)) => owner.stamp_terminal_then(row, callback),
                Ok(Err(TerminalError::InjectedCrash)) => {
                    owner.release_terminal(job);
                    callback(owner, None);
                }
                Ok(Err(TerminalError::Persistence(error))) => {
                    owner.terminal_not_durable(job, &error, callback)
                }
                Err(error) => owner.terminal_not_durable(job, &error.to_string(), callback),
            },
        );
    }

    fn release_terminal(&mut self, job: JobId) {
        self.terminals_in_flight.remove(&job);
        self.pending_control_commits.remove(&job);
    }

    /// Contract §2.1: a terminal that could not be written holds the job
    /// `storage_unavailable` on stage `persistence`; the row (if it landed)
    /// is replayed and the event goes out when the write lands.
    fn terminal_not_durable(&mut self, job: JobId, error: &str, callback: TerminalCallback) {
        self.release_terminal(job);
        tracing::error!(job = job.0, %error, "terminal record is not durable; holding the job until storage answers");
        if crate::is_out_of_space(error) {
            self.observe_out_of_space("terminal record");
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
                    inventory
                        .mark_terminal_step(row.job, &TerminalStep::MarkersStamped)
                        .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?;
                    let mut row = row;
                    row.markers_stamped = true;
                    Ok(row)
                },
                move |owner, result| match result {
                    Ok(row) => owner.emit_terminal_then(row, callback),
                    Err(error) => owner.terminal_not_durable(job, &error.to_string(), callback),
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
                owner.terminal_not_durable(job, "the queue snapshot did not commit", callback);
                return;
            }
            owner.io_then(
                move || {
                    inventory
                        .mark_terminal_step(row.job, &TerminalStep::MarkersStamped)
                        .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?;
                    let mut row = row;
                    row.markers_stamped = true;
                    Ok(row)
                },
                move |owner, result| match result {
                    Ok(row) => owner.emit_terminal_then(row, callback),
                    Err(error) => owner.terminal_not_durable(job, &error.to_string(), callback),
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
        self.emit(terminal::event_for(&row));
        let inventory = self.artifacts.clone();
        self.io_then(
            move || {
                inventory
                    .mark_terminal_step(row.job, &TerminalStep::Emitted)
                    .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?;
                let mut row = row;
                row.emitted = true;
                Ok(row)
            },
            move |owner, result| {
                owner.release_terminal(job);
                match result {
                    Ok(row) => callback(owner, Some(row)),
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
        let inventory = self.artifacts.clone();
        self.io_then(
            move || {
                inventory
                    .unfinished_terminals()
                    .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))
            },
            |owner, result| match result {
                Ok(rows) => {
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
        let legacy = self
            .state
            .jobs
            .iter()
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
    pub(super) fn emit_status_changes(&mut self) {
        let mut seen = HashSet::new();
        let mut changes = Vec::new();
        for job in &self.state.jobs {
            seen.insert(job.id);
            let status = nzbd_types::projected_status(job);
            let stage = match job.status {
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
