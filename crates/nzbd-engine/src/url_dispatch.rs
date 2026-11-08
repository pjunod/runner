//! URL execution is derived from the committed queue, not an API future.
//! Epochs and worker exits are coalesced hints; the snapshot retains the work.
use crate::EngineHandle;
use nzbd_types::{JobId, JobStatus};
use std::collections::{HashMap, HashSet};
use tokio::time::{Duration, Instant};

pub(crate) fn retry_delay(attempt: u32) -> Duration {
    Duration::from_secs([30, 60, 120, 300][attempt.min(3) as usize])
}

#[derive(Default)]
struct Attempt {
    failures: u32,
    running: bool,
    retry_at: Option<Instant>,
}

pub(crate) async fn run(handle: EngineHandle, startup_duplicates: Vec<(JobId, JobId)>) {
    run_with_retry(handle, startup_duplicates, retry_delay).await;
}

pub(crate) async fn run_with_retry(
    handle: EngineHandle,
    startup_duplicates: Vec<(JobId, JobId)>,
    backoff: fn(u32) -> Duration,
) {
    let mut changes = handle.queue_changes.clone();
    let mut attempts = HashMap::<JobId, Attempt>::new();
    // Legacy duplicate disposal is a recovery decision made once. Never
    // re-run duplicate policy for ordinary, already accepted admissions.
    let mut duplicates: HashMap<_, _> = startup_duplicates.into_iter().collect();
    loop {
        if handle.cancel.is_cancelled() {
            return;
        }
        let snapshot = handle.snapshot();
        let pending: HashSet<_> = snapshot
            .jobs
            .iter()
            .filter(|job| job.status == JobStatus::Fetching)
            .map(|job| job.id)
            .collect();
        attempts.retain(|job, _| pending.contains(job));
        duplicates.retain(|job, _| pending.contains(job));
        let active = handle
            .url_fetches
            .lock()
            .unwrap_or_else(|poisoned| {
                tracing::error!("URL executor set poisoned during reconciliation");
                poisoned.into_inner()
            })
            .clone();
        let authority = !handle.url_outbox_external
            || (snapshot.queue_authority
                && handle
                    .owner_persist_guard
                    .as_ref()
                    .is_none_or(|guard| guard()));
        let now = Instant::now();
        let mut next_retry = None;
        for job in snapshot
            .jobs
            .iter()
            .filter(|job| job.status == JobStatus::Fetching)
        {
            let attempt = attempts.entry(job.id).or_default();
            if active.contains(&job.id) {
                continue;
            }
            if attempt.running {
                attempt.running = false;
                let delay = backoff(attempt.failures);
                attempt.failures = attempt.failures.saturating_add(1);
                attempt.retry_at = Some(now + delay);
                tracing::warn!(
                    job = job.id.0,
                    attempt = attempt.failures,
                    delay_secs = delay.as_secs(),
                    "URL admission still pending after worker exit; dispatch will retry"
                );
            }
            // Paused URL submissions still acquire their NZB, exactly as the
            // former caller-owned path did. A lifecycle hold is different.
            let held = job
                .control
                .as_ref()
                .is_some_and(|control| control.lifecycle == "held" || control.version != 1);
            if !authority || held {
                continue;
            }
            if let Some(deadline) = attempt.retry_at.filter(|deadline| *deadline > now) {
                next_retry = Some(next_retry.map_or(deadline, |old: Instant| old.min(deadline)));
                continue;
            }
            attempt.running = match duplicates.get(&job.id) {
                Some(original) => handle.spawn_url_acquisition(job.id, Some(*original)),
                None => handle.spawn_url_fetch(job.id),
            };
            attempt.retry_at = None;
        }
        // Register the watch observation before sleeping. A change during a
        // scan remains unseen and wakes the next pass; Notify retains an exit
        // permit even if the worker finished before this select was entered.
        tokio::select! {
            _ = handle.cancel.cancelled() => return,
            result = changes.changed() => if result.is_err() { return; },
            _ = handle.url_fetch_wakeup.notified() => {},
            _ = async {
                match next_retry {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_dispatch_retry_backoff_is_bounded_and_saturates() {
        assert_eq!(
            (0..5)
                .map(|attempt| retry_delay(attempt).as_secs())
                .collect::<Vec<_>>(),
            [30, 60, 120, 300, 300]
        );
        assert_eq!(retry_delay(u32::MAX), Duration::from_secs(300));
    }
}
