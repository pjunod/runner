//! Automatic hold recovery — the one place a hold ends without a person.
//!
//! Each pass reads the published snapshot and, for every held job whose
//! cause earns an automatic policy (`HoldCause::policy`), runs the same
//! release an operator `resume` would (`EngineHandle::release_hold`). A
//! refused timed release is pushed out on the hold's backoff with the
//! refusal recorded on the job, so the row always says why it is still
//! held and when it tries next.
//!
//! It runs off the owner loop: the custody check and the capacity probe
//! touch the payload mount and run in `spawn_blocking` inside
//! `release_hold`. It acts only while this engine is the queue authority,
//! and never on a hold an operator paused.

use crate::{EngineHandle, RetryBy};
use nzbd_types::{HoldPolicy, JobKind};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Pass cadence. Timed holds fire on their own `next_retry_at_unix`; this is
/// only how promptly a due one is noticed.
const PASS: Duration = Duration::from_secs(5);
/// `reobserve_mount` holds have no time: their volume identity is re-checked
/// every this-many passes.
const REOBSERVE_EVERY: u64 = 6;

pub(crate) async fn run(handle: EngineHandle, cancel: CancellationToken) {
    let mut tick = tokio::time::interval(PASS);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut passes = 0u64;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tick.tick() => {}
        }
        passes = passes.wrapping_add(1);
        if pass(&handle, passes.is_multiple_of(REOBSERVE_EVERY))
            .await
            .is_err()
        {
            break; // engine closed
        }
    }
}

/// One recovery pass; returns how many holds it released.
pub(crate) async fn pass(
    handle: &EngineHandle,
    reobserve: bool,
) -> Result<usize, crate::EngineError> {
    let snapshot = handle.snapshot();
    if !snapshot.queue_authority {
        return Ok(0);
    }
    let now = crate::owner::unix_now();
    let mut released = 0;
    for job in snapshot.jobs.iter() {
        if job.kind == JobKind::Torrent {
            continue;
        }
        let Some(control) = job
            .control
            .as_ref()
            .filter(|c| c.lifecycle == "held" && !c.manual_pause)
        else {
            continue;
        };
        let policy = control.hold_cause().policy();
        let due = match policy {
            HoldPolicy::Review => false,
            HoldPolicy::ReobserveMount => reobserve,
            HoldPolicy::ScheduledRetry | HoldPolicy::ResumeSameJob => {
                control.next_retry_at_unix.is_some_and(|at| at <= now)
            }
        };
        if !due {
            continue;
        }
        match handle
            .release_hold(job.id, control.revision.clone(), RetryBy::Recovery)
            .await?
        {
            Ok(()) => released += 1,
            Err(reason) if policy.timed() => {
                tracing::info!(job = job.id.0, cause = %control.cause, %reason, "automatic retry refused; rescheduled");
                handle
                    .reschedule_hold(job.id, control.revision.clone(), reason)
                    .await?;
            }
            Err(reason) => {
                tracing::debug!(job = job.id.0, cause = %control.cause, %reason, "volume still unavailable");
            }
        }
    }
    Ok(released)
}
