//! Restart phase timings, measured from process start (no-wedge item 7).
//!
//! A restart is three questions an operator asks: when could I reach the
//! API, when did the first article lease go out, and how long did each
//! payload root's lifecycle reconciliation take (which now runs in the
//! background and gates only jobs with an open lifecycle intent).
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

static BOOT: OnceLock<Instant> = OnceLock::new();
static LISTENER_MS: AtomicU64 = AtomicU64::new(u64::MAX);

/// The process's boot instant: the first call wins. `nzbd run` calls this
/// before anything else; an engine built without it starts the clock at
/// its own spawn.
pub fn boot_instant() -> Instant {
    *BOOT.get_or_init(Instant::now)
}

/// Milliseconds since [`boot_instant`].
pub fn since_boot_ms() -> u64 {
    u64::try_from(boot_instant().elapsed().as_millis()).unwrap_or(u64::MAX - 1)
}

/// The public API listener is bound. Only the first bind is a boot phase;
/// a later rebind (a config reload) is not.
pub fn mark_listener_bound() {
    let _ = LISTENER_MS.compare_exchange(
        u64::MAX,
        since_boot_ms(),
        Ordering::AcqRel,
        Ordering::Acquire,
    );
}

pub fn listener_ms() -> Option<u64> {
    match LISTENER_MS.load(Ordering::Acquire) {
        u64::MAX => None,
        ms => Some(ms),
    }
}

/// Every field is milliseconds since process start, `None` until reached.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StartupPhases {
    /// Public API listener bound.
    pub listener_ms: Option<u64>,
    /// Queue owner constructed (snapshot loaded, journals replayed).
    pub owner_ready_ms: Option<u64>,
    /// Every recovered job classified: proven ones released to the
    /// scheduler, the rest given a verification ticket.
    pub classified_ms: Option<u64>,
    /// First article lease handed to a connection.
    pub first_lease_ms: Option<u64>,
    /// Jobs that got a startup verification ticket (unproven ranges,
    /// identity conflicts, open lifecycle intents).
    pub startup_tickets: u32,
    /// Jobs released without one.
    pub startup_untouched: u32,
    /// Background lifecycle reconciliation, per payload root.
    pub roots: Vec<RootReconciliation>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RootReconciliation {
    pub root: String,
    /// When it started and how long it ran.
    pub started_ms: u64,
    pub duration_ms: Option<u64>,
    /// `None` while it runs.
    pub ok: Option<bool>,
}
