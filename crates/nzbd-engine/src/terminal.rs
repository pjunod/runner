//! One terminal fact per job (pipeline contract v2 §2.1).
//!
//! Every producer — the owner at download end (`health`, `io`), the URL
//! fetcher (`fetch`), post-processing (`par`, `unpack`, `script`, `publish`,
//! `io`, `identity`, and `completed`), hold expiry (`hold_expired`), the
//! boot conversion of legacy holds, and the operator delete (`cancelled`) —
//! builds a [`JobTerminal`] and hands it to the owner. The owner performs
//! the steps below from that one row, each idempotent and each marked done
//! on it, so a crash between any two replays the rest:
//!
//! 1. the `job_terminals` row and the artifact's salvage obligation, in one
//!    SQLite transaction ([`persist_terminal`], on the persistence lane);
//! 2. the history append ([`persist_terminal`]);
//! 3. the queue-row markers (`*PP:done`, `*PP:failure-at`, `Failure:*`,
//!    `*PP:history-seq`) — [`stamp_queue_row`], committed with the snapshot;
//! 4. the `job_pp_finished` emit — [`event_for`], the one place the event
//!    is built.
//!
//! Salvage and disposal run only after step 4, from the row (§2.2, §2.6).

use crate::Event;
use nzbd_state::artifacts::{Inventory, JobTerminal, SalvageState, TerminalStep};
use nzbd_state::history::HistoryDb;
use nzbd_types::{
    FailureReason, FetchCause, Fetched, Job, JobControl, JobKind, JobStatus, Publication,
    TerminalOutcome, CONTROL_PARAM, PP_DONE_PARAM,
};

/// The param that keeps a failed source (the job's own keep-on-failure flag).
pub const KEEP_ON_FAILURE_PARAM: &str = "*Runner:KeepOnFailure";
/// Stamped with the terminal: when the failure was recorded.
pub const FAILURE_AT_PARAM: &str = "*PP:failure-at";
pub const HISTORY_SEQ_PARAM: &str = "*PP:history-seq";
/// The history incarnation when no history database is configured (an
/// embedded engine in tests). The native daemon refuses to start without one.
pub const NO_HISTORY_INCARNATION: &str = "none";

/// Seed retirement is a lifecycle after the terminal; its holds never
/// expire into a terminal.
pub fn seed_retirement_stage(stage: &str) -> bool {
    matches!(stage, "torrent_seed_retire" | "torrent_seed_forget")
}

pub fn keeps_failed_source(job: &Job) -> bool {
    job.params
        .iter()
        .any(|(key, value)| key == KEEP_ON_FAILURE_PARAM && value == "1")
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn bounded(message: &str) -> String {
    message.chars().take(2048).collect()
}

/// History params: the job's non-internal params plus `*Control:v1`.
pub fn history_params(job: &Job) -> Vec<(String, String)> {
    job.params
        .iter()
        .filter(|(key, _)| !key.starts_with('*') || key == CONTROL_PARAM)
        .cloned()
        .collect()
}

fn set(params: &mut Vec<(String, String)>, key: &str, value: String) {
    params.retain(|(existing, _)| existing != key);
    params.push((key.to_string(), value));
}

/// The control a terminal leaves on the row: a held control is resolved
/// (`lifecycle: terminal`, revision + 1) so `held()` is false and the last
/// authoritative revision travels with history and the event.
fn terminal_control(job: &Job, message: &str) -> Option<JobControl> {
    let mut control = job.control()?;
    if control.lifecycle != "held" {
        return Some(control);
    }
    let next = control
        .revision
        .parse::<u64>()
        .unwrap_or(0)
        .saturating_add(1);
    control.revision = next.to_string();
    control.lifecycle = "terminal".into();
    control.next_retry_at_unix = None;
    control.deadline_in_s = None;
    control.message = bounded(&format!("ended: {message}"));
    Some(control)
}

/// Whether runner owes a salvage assessment for this failure (§2.2): the
/// reason allows it, the job is in the lifecycle cohort whose sources runner
/// can publish from, and something was actually fetched.
pub fn salvage_owed(job: &Job, reason: FailureReason) -> bool {
    reason.salvage_possible()
        && job.lifecycle_eligible()
        && (job.kind == JobKind::Torrent || Fetched::of(job).success_articles > 0)
}

/// Build the failed terminal for `job` from its queue row.
pub fn failed_terminal(
    job: &Job,
    reason: FailureReason,
    fetch_cause: Option<FetchCause>,
    stage: &str,
    message: &str,
) -> JobTerminal {
    let message = bounded(message);
    let salvage = if salvage_owed(job, reason) {
        SalvageState::Pending
    } else {
        SalvageState::None
    };
    let blames = reason.blames_release(fetch_cause);
    let keep = keeps_failed_source(job);
    let status = reason.history_status();
    let at = unix_now();
    let mut markers = vec![
        (PP_DONE_PARAM.to_string(), status.to_string()),
        (FAILURE_AT_PARAM.to_string(), at.to_string()),
        ("Failure:Stage".to_string(), stage.to_string()),
        ("Failure:Reason".to_string(), reason.as_str().to_string()),
        ("Failure:Message".to_string(), message.clone()),
        ("Failure:BlamesRelease".to_string(), blames.to_string()),
        (
            "Failure:Salvage".to_string(),
            salvage.wire().as_str().to_string(),
        ),
    ];
    if let Some(cause) = fetch_cause {
        markers.push(("Failure:FetchCause".into(), cause.as_str().into()));
    }
    if keep {
        markers.push(("Failure:KeepOnFailure".into(), "true".into()));
    }
    if let Some(control) = terminal_control(job, &message) {
        markers.push((
            CONTROL_PARAM.into(),
            serde_json::to_string(&control).expect("control serializes"),
        ));
    }
    terminal_from_job(
        job,
        TerminalOutcome::Failed,
        status,
        None,
        markers,
        TerminalFacts {
            reason: Some(reason),
            fetch_cause,
            stage: Some(stage.to_string()),
            message: Some(message),
            blames_release: blames,
            salvage,
            keep_on_failure: keep,
            publication: None,
        },
        at,
    )
}

/// The facts a terminal carries beyond the job row.
pub struct TerminalFacts {
    pub reason: Option<FailureReason>,
    pub fetch_cause: Option<FetchCause>,
    pub stage: Option<String>,
    pub message: Option<String>,
    pub blames_release: bool,
    pub salvage: SalvageState,
    pub keep_on_failure: bool,
    pub publication: Option<Publication>,
}

/// Build a terminal whose history entry is the job row with `markers`
/// applied. `final_dir` is the entry's `final_dir`.
pub fn terminal_from_job(
    job: &Job,
    outcome: TerminalOutcome,
    status: &str,
    final_dir: Option<String>,
    markers: Vec<(String, String)>,
    facts: TerminalFacts,
    completed_at_unix: i64,
) -> JobTerminal {
    let mut stamped = job.clone();
    for (key, value) in &markers {
        set(&mut stamped.params, key, value.clone());
    }
    let entry = nzbd_state::HistoryEntry {
        job: job.id,
        name: job.name.clone(),
        category: job.category.clone(),
        final_dir,
        status: status.to_string(),
        size: job.totals.size,
        health: nzbd_types::Health::calc(&job.totals).0,
        params: history_params(&stamped),
        dupe_key: job.dupe.key.clone(),
        dupe_score: job.dupe.score,
        completed_at_unix,
        hidden: false,
        first_seen_at_unix: None,
        last_seen_at_unix: None,
        seen_count: 0,
        removed_at_unix: None,
        picked_up_by: None,
        record: Some(nzbd_state::JobRecord::from_job(&stamped)),
        stages: job.stages.clone(),
        seq: 0,
    };
    JobTerminal {
        job: job.id.0,
        transfer: nzbd_types::transfer_of(&job.params),
        outcome,
        reason: facts.reason,
        fetch_cause: facts.fetch_cause,
        stage: facts.stage,
        message: facts.message,
        blames_release: facts.blames_release,
        salvage: facts.salvage,
        keep_on_failure: facts.keep_on_failure,
        fetched: Fetched::of(job),
        publication: facts.publication,
        markers,
        entry,
        history_seq: None,
        history_incarnation: None,
        markers_stamped: false,
        emitted: false,
        disposal: None,
        salvage_error: None,
        created_at: 0,
        updated_at: 0,
    }
}

/// Why a terminal step did not land.
#[derive(Debug)]
pub enum TerminalError {
    /// The row or the history append failed: the job holds
    /// `storage_unavailable` on stage `persistence` and is replayed.
    Persistence(String),
    /// Test support: an injected crash stopped the replay after a step.
    InjectedCrash,
}

impl std::fmt::Display for TerminalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TerminalError::Persistence(error) => write!(f, "terminal not durable: {error}"),
            TerminalError::InjectedCrash => f.write_str("injected crash"),
        }
    }
}

/// Steps 1 and 2 (sync; run on the persistence lane). Idempotent: resumes
/// from the stored row's flags.
pub fn persist_terminal(
    inventory: &Inventory,
    history: Option<&HistoryDb>,
    terminal: &JobTerminal,
) -> Result<JobTerminal, TerminalError> {
    let mut row = inventory
        .record_terminal(terminal)
        .map_err(|error| TerminalError::Persistence(error.to_string()))?;
    if inventory.take_injected_terminal_crash(row.job, &TerminalStep::Recorded) {
        return Err(TerminalError::InjectedCrash);
    }
    if row.history_seq.is_none() {
        let (seq, incarnation) = match history {
            Some(history) => {
                let (seq, _) = history
                    .record_seq_durable(&row.entry)
                    .map_err(|error| TerminalError::Persistence(error.to_string()))?;
                let incarnation = history
                    .incarnation()
                    .map_err(|error| TerminalError::Persistence(error.to_string()))?;
                (seq, incarnation)
            }
            None => (0, NO_HISTORY_INCARNATION.to_string()),
        };
        let step = TerminalStep::HistoryWritten {
            seq,
            incarnation: incarnation.clone(),
        };
        inventory
            .mark_terminal_step(row.job, &step)
            .map_err(|error| TerminalError::Persistence(error.to_string()))?;
        row.history_seq = Some(seq);
        row.history_incarnation = Some(incarnation);
        if inventory.take_injected_terminal_crash(row.job, &step) {
            return Err(TerminalError::InjectedCrash);
        }
    }
    Ok(row)
}

/// Step 3: apply the row's markers to the queue row.
pub fn stamp_queue_row(job: &mut Job, row: &JobTerminal) {
    for (key, value) in &row.markers {
        set(&mut job.params, key, value.clone());
    }
    if let Some(seq) = row.history_seq {
        set(&mut job.params, HISTORY_SEQ_PARAM, seq.to_string());
    }
    job.status = match row.outcome {
        TerminalOutcome::Completed => JobStatus::Completed,
        TerminalOutcome::Failed | TerminalOutcome::Cancelled => JobStatus::Failed,
    };
}

/// Whether the queue row already carries this row's stamp.
pub fn queue_row_stamped(job: &Job, row: &JobTerminal) -> bool {
    let done = row
        .markers
        .iter()
        .find(|(key, _)| key == PP_DONE_PARAM)
        .map(|(_, value)| value.as_str());
    done.is_some()
        && job
            .params
            .iter()
            .any(|(key, value)| key == PP_DONE_PARAM && Some(value.as_str()) == done)
        && row.history_seq.is_none_or(|seq| {
            job.params
                .iter()
                .any(|(key, value)| key == HISTORY_SEQ_PARAM && value == &seq.to_string())
        })
}

/// Step 4: the one place `job_pp_finished` is built.
pub fn event_for(row: &JobTerminal) -> Event {
    let entry = &row.entry;
    Event::JobPpFinished {
        job: entry.job,
        name: entry.name.clone(),
        category: entry.category.clone(),
        transfer: row.transfer.clone(),
        outcome: row.outcome,
        reason: row.reason,
        fetch_cause: row.fetch_cause,
        stage: row.stage.clone(),
        message: row.message.clone(),
        blames_release: row.blames_release,
        salvage: row.salvage.wire(),
        keep_on_failure: row.keep_on_failure,
        fetched: row.fetched,
        publication: row.publication.clone(),
        pp_status: entry.status.clone(),
        final_dir: row
            .publication
            .as_ref()
            .map(|p| p.path.clone())
            .or_else(|| entry.final_dir.clone()),
        size_bytes: entry.size,
        health: entry.health,
        params: entry.params.clone(),
        history_seq: row.history_seq.unwrap_or(0),
        history_incarnation: row
            .history_incarnation
            .clone()
            .unwrap_or_else(|| NO_HISTORY_INCARNATION.into()),
    }
}

/// A pre-M1 history status string's reason, for rows that crashed before
/// they were stamped (`FAILURE/HEALTH` → health, …).
pub fn reason_for_status(status: &str) -> Option<FailureReason> {
    FailureReason::ALL
        .into_iter()
        .find(|reason| reason.history_status() == status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_reason_has_a_distinct_prefixed_history_status() {
        let mut seen = std::collections::HashSet::new();
        for reason in FailureReason::ALL {
            let status = reason.history_status();
            assert!(seen.insert(status));
            assert!(
                status.starts_with("FAILURE") || status.ends_with("_FAILURE"),
                "{status}"
            );
            assert_eq!(reason_for_status(status), Some(reason));
        }
    }
}
