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
    // The first pass waits one interval: boot has just re-armed every stored
    // hold, and the engine should be serving before it starts retrying them.
    let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + PASS, PASS);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut passes = 0u64;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tick.tick() => {}
        }
        passes = passes.wrapping_add(1);
        match pass(&handle, passes.is_multiple_of(REOBSERVE_EVERY)).await {
            Err(crate::EngineError::Closed) => break,
            Err(error) => {
                tracing::warn!(%error, "hold recovery pass unavailable; retrying on the next pass");
            }
            Ok(_) => {}
        }
    }
}

/// One recovery pass; returns how many holds it released. Due holds are
/// released concurrently: each release may wait on its own volume (bounded
/// by the probe deadline), and one wedged job must not hold up the rest.
pub(crate) async fn pass(
    handle: &EngineHandle,
    reobserve: bool,
) -> Result<usize, crate::EngineError> {
    handle.reconcile_native_seed_retirements().await?;
    let snapshot = handle.snapshot();
    if !snapshot.queue_authority {
        return Ok(0);
    }
    let now = crate::owner::unix_now();
    let mut releases = tokio::task::JoinSet::new();
    let eligible = snapshot
        .jobs
        .iter()
        .filter(|job| !snapshot.pending_automatic_controls.contains(&job.id))
        .filter(|job| job.assigned_node.is_none())
        .filter(|job| {
            job.kind != JobKind::Torrent
                || job.torrent_role == Some("acquisition")
                || snapshot
                    .native_seed_retirements
                    .get(&job.id)
                    .is_some_and(|marker| marker.phase == "detached")
        })
        .filter_map(|job| {
            let control = job
                .control
                .as_ref()
                .filter(|c| c.lifecycle == "held" && !c.manual_pause)?;
            let due = match control.hold_cause().policy() {
                HoldPolicy::Review => false,
                HoldPolicy::ReobserveMount => reobserve,
                HoldPolicy::ScheduledRetry | HoldPolicy::ResumeSameJob => {
                    control.next_retry_at_unix.is_some_and(|at| at <= now)
                }
            };
            due.then_some((job, control))
        })
        .collect::<Vec<_>>();
    if eligible.is_empty() {
        return Ok(0);
    }
    let start = handle
        .recovery_cursor
        .fetch_add(8, std::sync::atomic::Ordering::Relaxed)
        % eligible.len();
    for offset in 0..eligible.len().min(8) {
        let (job, control) = eligible[(start + offset) % eligible.len()];
        let handle = handle.clone();
        let (id, revision, cause) = (job.id, control.revision.clone(), control.cause.clone());
        releases.spawn(async move {
            match handle
                .release_hold(id, revision.clone(), RetryBy::Recovery)
                .await?
            {
                Ok(()) => Ok(true),
                Err(reason) => {
                    // Every refusal lands on the job: a timed hold moves its
                    // next attempt out, a re-observed one records why.
                    tracing::info!(job = id.0, %cause, %reason, "automatic hold release refused");
                    handle.reschedule_hold(id, revision, reason).await?;
                    Ok(false)
                }
            }
        });
    }
    let mut released = 0;
    while let Some(joined) = releases.join_next().await {
        match joined {
            Ok(Ok(true)) => released += 1,
            Ok(Ok(false)) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => tracing::warn!(error = %e, "hold release task failed"),
        }
    }
    Ok(released)
}
