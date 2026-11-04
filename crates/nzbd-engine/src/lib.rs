//! The download engine (ARCHITECTURE.md §8).
//!
//! Architecture: **single-owner queue task + message passing**. One task
//! owns all queue state (jobs/files/segments/leases) and is the only place
//! it mutates; connection tasks (one per NNTP connection, pull model) ask it
//! for leased segments, decode article bodies incrementally and hand the
//! bytes to per-file writer tasks; readers get lock-free `arc-swap`
//! snapshots. Crash safety: an append-only segment journal + debounced
//! atomic queue snapshots (`nzbd-state`) — kill -9 loses at most the
//! last un-fsynced second.
//!
//! Public surface: [`Engine::spawn`] → [`EngineHandle`].

mod admissions;
pub mod automatic_control;
pub mod backend;
mod cluster_stop;
mod download_capacity;
pub mod events;
pub mod failover;
pub mod fetch;
mod hold_recovery;
mod hold_release;
pub mod queue;
pub mod rate;
pub mod snapshot;
pub mod torrent_acquisitions;
pub mod torrent_runtime;
pub mod volumes;

mod imports;
pub mod lifecycle;
pub use lifecycle::frozen_lifecycle_timing;
mod owner;
mod pool;
mod recovery;
mod writer;

pub use events::Event;
pub use owner::{MirrorStats, MoveOp, RetryBy};
pub use queue::clean_job_name;
pub use snapshot::{
    new_shared_snapshot, JobSummary, QueueSnapshot, RepairPhase, RepairProgress, ServerVolume,
    SharedSnapshot,
};

use nzbd_nntp::transport::{tls_client_config, TlsClientConfig};
use nzbd_types::{Job, JobId, ServerDef, TlsMode};
use owner::{EngineMsg, Owner, QueueCommand};
use rate::{RateLimiter, SpeedMeter};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use volumes::DiskGuardRoot;

/// Does this error message name an out-of-space condition?
///
/// Matched on the message because that is what survives every layer this
/// has to cross — `std::io::Error` stringified by a writer task, the fsx
/// wrappers' `op path: source`, a subprocess's stderr. ENOSPC and the
/// quota variant both count: a gluster or NFS mount answers `EDQUOT` for
/// exactly the same condition, and the whole point of this signal is that
/// it does not depend on which lie statvfs is telling.
pub fn is_out_of_space(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("no space left on device")
        || lower.contains("os error 28")
        || lower.contains("disk quota exceeded")
        || lower.contains("os error 122")
        || lower.contains("no storage space")
}

#[cfg(test)]
type UrlPreparationGate = Option<(
    String,
    std::sync::Arc<(std::sync::Barrier, std::sync::Barrier)>,
)>;
#[cfg(test)]
pub(crate) static URL_PREPARATION_GATE: std::sync::Mutex<UrlPreparationGate> =
    std::sync::Mutex::new(None);
fn url_preparation_checkpoint(identity: &str) {
    #[cfg(test)]
    {
        let gate = {
            let mut configured = URL_PREPARATION_GATE.lock().unwrap();
            if configured
                .as_ref()
                .is_some_and(|(expected, _)| expected == identity)
            {
                configured.take().map(|(_, gate)| gate)
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.0.wait();
            gate.1.wait();
        }
    }
    let _ = identity;
}

fn url_retry_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn url_storage_error(error: nzbd_state::artifacts::Error) -> EngineError {
    match error {
        nzbd_state::artifacts::Error::Io(source) => {
            EngineError::State(nzbd_state::StateError::Io {
                op: "persist URL source",
                path: PathBuf::from("url-resolutions"),
                source,
            })
        }
        nzbd_state::artifacts::Error::NotFound => EngineError::OwnershipLost,
        other => EngineError::Lifecycle(other.to_string()),
    }
}

/// The storage under a path stopped answering: a network or FUSE mount lost
/// its transport, a stale handle after a server-side remount, a device that
/// went away, a remote that timed out or reset, or the generic EIO such
/// mounts report meanwhile. Nothing about the job is wrong; the same
/// operation succeeds once the volume is back, so these holds retry on
/// their own ([`nzbd_types::HoldCause::StorageUnavailable`]).
///
/// Matched only on the std `(os error N)` suffix every `io::Error` Display
/// ends with — the error has usually crossed a `to_string()` by the time a
/// hold is classified, and free text (payload names, extractor output) must
/// not steer the cause.
pub fn is_storage_unavailable(msg: &str) -> bool {
    // EIO ENXIO EAGAIN ENODEV ENOLINK ECONNABORTED ECONNRESET ENOTCONN
    // ETIMEDOUT ECONNREFUSED EHOSTDOWN EHOSTUNREACH ESTALE EREMOTEIO
    const ERRNOS: [u16; 14] = [
        5, 6, 11, 19, 67, 103, 104, 107, 110, 111, 112, 113, 116, 121,
    ];
    ERRNOS
        .iter()
        .any(|n| msg.contains(&format!("(os error {n})")))
}

/// How long a hold release waits on a filesystem probe (custody check,
/// capacity probe) against the volume that failed. A hung NFS/FUSE mount
/// blocks the probe's thread in the kernel; the release gives up on it and
/// treats that as a refusal, so one wedged job cannot stall the recovery of
/// every other job or the daemon's shutdown.
const HOLD_PROBE_TIMEOUT: Duration = Duration::from_secs(20);

/// Run a blocking filesystem probe off the runtime with a deadline. The
/// error says why there is no answer.
struct HoldProbeLease {
    probes: Arc<Mutex<HashSet<JobId>>>,
    job: JobId,
}
impl Drop for HoldProbeLease {
    fn drop(&mut self) {
        self.probes.lock().unwrap().remove(&self.job);
    }
}
async fn bounded_probe<T: Send + 'static>(
    probes: Arc<Mutex<HashSet<JobId>>>,
    job: JobId,
    probe: impl FnOnce() -> T + Send + 'static,
) -> Result<T, String> {
    bounded_probe_deadline(probes, job, HOLD_PROBE_TIMEOUT, probe).await
}
async fn bounded_probe_deadline<T: Send + 'static>(
    probes: Arc<Mutex<HashSet<JobId>>>,
    job: JobId,
    deadline: Duration,
    probe: impl FnOnce() -> T + Send + 'static,
) -> Result<T, String> {
    if !probes.lock().unwrap().insert(job) {
        return Err("a previous probe for this job is still waiting on its volume".into());
    }
    let lease = HoldProbeLease { probes, job };
    match tokio::time::timeout(
        deadline,
        tokio::task::spawn_blocking(move || {
            let _lease = lease;
            probe()
        }),
    )
    .await
    {
        Ok(Ok(answer)) => Ok(answer),
        Ok(Err(error)) => Err(format!("did not finish: {error}")),
        Err(_) => Err(format!(
            "got no answer from the volume within {} s",
            HOLD_PROBE_TIMEOUT.as_secs()
        )),
    }
}

/// The hold cause for a failed write or filesystem step, by errno.
pub fn io_hold_cause(msg: &str) -> nzbd_types::HoldCause {
    if is_out_of_space(msg) {
        if msg.to_lowercase().contains("quota") {
            nzbd_types::HoldCause::Quota
        } else {
            nzbd_types::HoldCause::Capacity
        }
    } else if is_storage_unavailable(msg) {
        nzbd_types::HoldCause::StorageUnavailable
    } else {
        nzbd_types::HoldCause::Io
    }
}

fn fetch_retry_state(job: &Job) -> crate::fetch::FetchRetryState {
    let value = |key: &str| {
        job.params
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    };
    crate::fetch::FetchRetryState {
        attempts: value(crate::fetch::RETRY_ATTEMPT_PARAM)
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
        next_retry_at: value(crate::fetch::NEXT_RETRY_PARAM).and_then(|value| value.parse().ok()),
        blocked_since: value(crate::fetch::BLOCKED_SINCE_PARAM)
            .and_then(|value| value.parse().ok()),
        last_error: value(crate::fetch::LAST_ERROR_PARAM).map(str::to_string),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("invalid NZB: {0}")]
    Nzb(#[from] nzbd_nzb::NzbError),
    #[error("state: {0}")]
    State(#[from] nzbd_state::StateError),
    #[error("tls: {0}")]
    Tls(String),
    #[error("file lifecycle: {0}")]
    Lifecycle(String),
    #[error("request token conflict: {0}")]
    RequestConflict(String),
    #[error("request-token admission could not be durably saved")]
    AdmissionNotDurable,
    #[error("job intent or source changed during journal preparation; import was not applied")]
    StaleImport,
    #[error("queue authority was lost; durable URL work is retained for its owner")]
    OwnershipLost,
    #[error("engine is shutting down")]
    Closed,
    /// The owner may still apply an issued command after this deadline.
    /// Callers must reconcile mutations rather than assume they were cancelled.
    #[error("engine request timed out while {0}")]
    Timeout(&'static str),
}

/// Behavioral knobs, defaulting to NZBGet's values (ARCHITECTURE.md §3.3).
#[derive(Debug, Clone)]
pub struct Tuning {
    pub article_retries: u8,
    pub retry_interval: Duration,
    pub article_timeout: Duration,
    pub connect_timeout: Duration,
    /// How long an idle connection is kept before being closed.
    pub idle_hold: Duration,
    pub propagation_delay: Duration,
    /// Queue `*.volNNN+MM.par2` files paused (delayed-par download).
    pub pause_extra_pars: bool,
    /// Pause all downloading when free space on any configured write volume
    /// drops below this (0 = disabled).
    pub min_free_disk_bytes: u64,
    /// Daily / monthly download quotas in bytes (0 = unlimited); force-
    /// priority jobs bypass quota (never the disk guard).
    pub daily_quota_bytes: u64,
    pub monthly_quota_bytes: u64,
    /// Day of month the monthly quota window starts (1–28).
    pub quota_start_day: u32,
    /// Abort a download the moment its health falls below critical health
    /// (unrepairable even with all par2): remaining segments are failed
    /// instead of downloaded, and the job finishes as Failed — the PP
    /// health gate then parks/deletes per `[post] health_action`. Wired
    /// from `health_action != "none"`.
    pub health_abort: bool,
}

impl Default for Tuning {
    fn default() -> Self {
        Tuning {
            article_retries: 3,
            retry_interval: Duration::from_secs(10),
            article_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(30),
            idle_hold: Duration::from_secs(5),
            propagation_delay: Duration::ZERO,
            pause_extra_pars: true,
            min_free_disk_bytes: 0,
            daily_quota_bytes: 0,
            monthly_quota_bytes: 0,
            quota_start_day: 1,
            health_abort: false,
        }
    }
}

#[derive(Clone)]
pub struct EngineConfig {
    pub servers: Vec<ServerDef>,
    /// Whether ordinary queued jobs may use provider connections. Cluster
    /// post-processing-only nodes set this false: their connection pool is
    /// available solely to the explicit delayed-PAR recovery lane.
    pub download_enabled: bool,
    /// Journal + snapshots live here (the shared volume in cluster mode).
    pub state_dir: PathBuf,
    /// Node-local lifecycle database, separate from cluster journals.
    pub artifact_dir: Option<PathBuf>,
    /// Independently enrolled inventory identity; never create a new store on mismatch.
    pub expected_installation_id: Option<String>,
    /// Completed jobs are written to `<dest_dir>/<job name>/`.
    pub dest_dir: PathBuf,
    /// Ordered configured torrent payload roots used only for removal safety.
    pub torrent_payload_roots: Vec<PathBuf>,
    /// Durable terminal history. Torrent removal is not retired from the
    /// queue until this store confirms its append+fsync protocol.
    pub history: Option<Arc<nzbd_state::history::HistoryDb>>,
    /// Every configured filesystem root the daemon may write. The enforcing
    /// guard probes all of them and gates intake on the lowest reading.
    pub disk_guard_roots: Vec<DiskGuardRoot>,
    pub tuning: Tuning,
    pub speed_limit_bps: Option<u64>,
    /// How many jobs may download at once, from the config file. `None`
    /// leaves whatever the persisted snapshot carries (or 1 on a fresh
    /// install). Like the speed limit, this is runtime-adjustable, so the
    /// config value is a starting position rather than a fixed ceiling.
    pub max_active_downloads: Option<u32>,
    /// Queue-authority persistence (snapshot save + journal compaction +
    /// recovery-at-boot). `false` for cluster worker engines: they start
    /// empty and receive jobs as leases (journals stay on regardless).
    pub persist_queue: bool,
    /// Fencing suffix for this engine's per-job journal files — the
    /// cluster work-lease id, or "local".
    pub journal_suffix: String,
    /// Checked immediately before every snapshot commit (CLUSTERING.md
    /// §6.4): returns false once this node is no longer the authority.
    pub persist_guard: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
}

impl EngineConfig {
    pub fn single_node(
        servers: Vec<ServerDef>,
        state_dir: PathBuf,
        dest_dir: PathBuf,
        tuning: Tuning,
        speed_limit_bps: Option<u64>,
    ) -> EngineConfig {
        EngineConfig {
            servers,
            download_enabled: true,
            state_dir,
            artifact_dir: None,
            expected_installation_id: None,
            disk_guard_roots: vec![DiskGuardRoot {
                label: "downloads".into(),
                path: dest_dir.clone(),
            }],
            dest_dir,
            torrent_payload_roots: Vec::new(),
            history: None,
            tuning,
            max_active_downloads: None,
            speed_limit_bps,
            persist_queue: true,
            journal_suffix: "local".into(),
            persist_guard: None,
        }
    }
}

pub struct Engine;

impl Engine {
    /// Recover state, spawn the owner task and one connection task per
    /// configured connection, and return the handle.
    pub async fn spawn(cfg: EngineConfig) -> Result<EngineHandle, EngineError> {
        let shared = new_shared_snapshot();
        let (events, _) = broadcast::channel(512);
        let (epoch_tx, epoch_rx) = watch::channel(0u64);
        let initial_budgets = pool::BudgetEnvelope {
            generation: 0,
            allowances: cfg
                .servers
                .iter()
                .map(|server| {
                    (
                        server.id,
                        if cfg.download_enabled && server.active {
                            server.max_connections.max(1)
                        } else {
                            0
                        },
                    )
                })
                .collect(),
        };
        let (budget_tx, budget_rx) = watch::channel(initial_budgets);
        let (engine_tx, engine_rx) = mpsc::channel::<EngineMsg>(1024);
        // The owner gets the command-producing half. Keep the adapter half
        // with the engine handle until the runtime executor takes it; this
        // preserves the single owner-to-backend seam without starting a peer
        // session in the dormant engine configuration.
        let (backend_owner, backend_adapter) = backend::backend_channel(64, 64);
        let meter = Arc::new(SpeedMeter::new());
        let limiter = Arc::new(RateLimiter::new(cfg.speed_limit_bps));
        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();
        let servers = Arc::new(cfg.servers.clone());
        let budget_tracker = Arc::new(pool::BudgetTracker::new(
            servers
                .iter()
                .filter(|server| server.active)
                .map(|server| usize::from(server.max_connections.max(1)))
                .sum(),
        ));

        // TLS configs once per server.
        let mut tls_by_server: Vec<Option<TlsClientConfig>> = Vec::new();
        for s in servers.iter() {
            tls_by_server.push(match s.tls {
                TlsMode::Tls => Some(
                    tls_client_config(s.cert_verification)
                        .map_err(|e| EngineError::Tls(e.to_string()))?,
                ),
                TlsMode::None => None,
            });
        }

        let owner_heartbeat = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let owner = Owner::recover(
            &cfg.state_dir,
            cfg.artifact_dir.as_deref(),
            cfg.expected_installation_id.as_deref(),
            cfg.dest_dir.clone(),
            cfg.torrent_payload_roots.clone(),
            cfg.history.clone(),
            servers.clone(),
            cfg.tuning.clone(),
            cfg.download_enabled,
            cfg.persist_queue,
            &cfg.journal_suffix,
            cfg.persist_guard.clone(),
            budget_tx,
            shared.clone(),
            events.clone(),
            epoch_tx,
            meter.clone(),
            limiter.clone(),
            cfg.speed_limit_bps,
            cfg.max_active_downloads,
            backend_owner,
            engine_tx.clone(),
            tracker.clone(),
            cancel.clone(),
            owner_heartbeat.clone(),
        )?;
        // Seed the shared snapshot with recovered state synchronously, so
        // the API never serves an empty queue in the window before the
        // async run loop's first publish (UI "queue is empty" flash).
        let mut owner = owner;
        owner.seed_snapshot();
        let artifacts = owner.artifacts.clone();
        let url_resolutions = owner.url_resolutions.clone();
        {
            let store = url_resolutions.clone();
            let stop = cancel.clone();
            tracker.spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(30));
                loop {
                    tokio::select! { _ = stop.cancelled() => break, _ = interval.tick() => {} }
                    let store = store.clone();
                    match tokio::task::spawn_blocking(move || store.maintenance(25)).await {
                        Ok(Ok(_)) => {}
                        outcome => {
                            tracing::warn!(?outcome, "URL cache maintenance requires review")
                        }
                    }
                }
            });
        }
        let shutdown_complete = owner.shutdown_complete.clone();
        {
            let inventory = artifacts.clone();
            let history = cfg.history.clone();
            let stop = cancel.clone();
            tracker.spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(30));
                loop {
                    tokio::select! { _ = stop.cancelled() => break, _ = interval.tick() => {} }
                    let inventory = inventory.clone();
                    let history = history.clone();
                    if let Ok(Err(e)) = tokio::task::spawn_blocking(move || {
                        inventory.tick_with_history(history.as_deref())
                    })
                    .await
                    {
                        tracing::error!(error = %e, "file lifecycle maintenance failed");
                    }
                }
            });
        }
        // Jobs recovered mid-fetch: their fetch tasks died with the old
        // process — re-spawn one per unique URL below (once the handle
        // exists) and fail same-URL pile-ups as duplicates.
        let (refetch, refetch_dupes) =
            crate::owner::plan_url_refetches_scoped(owner.pending_url_fetches());

        // Multi-root free-space prober — OFF the owner loop and startup
        // critical path. Each root has its own blocking task and deadline,
        // so a wedged FUSE/network mount cannot prevent the daemon serving
        // its API or suppress fresh readings from other volumes.
        {
            let disk_guard = owner.disk_guard_handle();
            let roots = cfg.disk_guard_roots.clone();
            let probe_cancel = cancel.clone();
            tracker.spawn(async move {
                let mut probe = crate::volumes::DiskGuardProbe::default();
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        _ = probe_cancel.cancelled() => break,
                        _ = tick.tick() => {
                            let started = std::time::Instant::now();
                            let Some(reading) = probe
                                .probe_until_cancelled(
                                    &roots,
                                    std::time::Duration::from_secs(2),
                                    &probe_cancel,
                                )
                                .await
                            else {
                                break;
                            };
                            let ms = started.elapsed().as_millis() as u64;
                            if ms > 2000 {
                                tracing::warn!(
                                    ms,
                                    roots = roots.len(),
                                    complete = reading.all_roots_known,
                                    "multi-root free-space probe was delayed or hit a per-root \
                                     deadline; retaining last-known data for any unresponsive volume"
                                );
                            }
                            disk_guard.store(Arc::new(reading));
                        }
                    }
                }
            });
        }

        tracker.spawn(owner.run(engine_rx));

        // Connection tasks: `max_connections` per active server. Tasks are
        // cheap when parked; sockets only exist while there is work.
        for (i, server) in servers.iter().enumerate() {
            if !server.active {
                continue;
            }
            for conn_index in 0..server.max_connections.max(1) {
                tracker.spawn(pool::connection_task(pool::ConnCtx {
                    server: server.clone(),
                    conn_index,
                    tls: tls_by_server[i].clone(),
                    engine_tx: engine_tx.clone(),
                    epoch: epoch_rx.clone(),
                    budgets: budget_rx.clone(),
                    budget_tracker: budget_tracker.clone(),
                    limiter: limiter.clone(),
                    meter: meter.clone(),
                    cancel: cancel.clone(),
                    connect_timeout: cfg.tuning.connect_timeout,
                    read_timeout: cfg.tuning.article_timeout,
                    idle_hold: cfg.tuning.idle_hold,
                }));
            }
        }
        tracker.close();

        let handle = EngineHandle {
            artifacts,
            url_resolutions,
            url_dest_dir: cfg.dest_dir.clone(),
            import_state_dir: cfg.state_dir.clone(),
            url_history: cfg.history.clone(),
            url_outbox_external: cfg.persist_guard.is_some(),
            owner_persist_guard: cfg.persist_guard.clone(),
            native_seed_recovery_cursor: Default::default(),
            cmd_tx: engine_tx,
            shared,
            events,
            cancel,
            tracker,
            backend_adapter: Arc::new(std::sync::Mutex::new(Some(backend_adapter))),
            budget_tracker,
            url_fetches: Arc::new(Mutex::new(HashSet::new())),
            hold_probes: Arc::new(Mutex::new(HashSet::new())),
            recovery_cursor: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            post_scope: None,
            queue_changes: epoch_rx,
            owner_heartbeat,
            shutdown_complete,
        };

        // Automatic hold recovery: one task, off the owner loop, for every
        // policy that releases without a person (docs/HELD_JOBS_PLAN.md §4.5).
        handle
            .tracker
            .spawn(hold_recovery::run(handle.clone(), handle.cancel.clone()));

        for (job, url) in refetch {
            tracing::info!(job = job.0, %url, "re-spawning NZB fetch for job recovered mid-fetch");
            // Startup verification may still own this recovered row. The
            // fetch task reads its durable retry state once that row is ready.
            handle.spawn_url_fetch(job, url, crate::fetch::FetchRetryState::default());
        }
        for (job, original) in refetch_dupes {
            let h = handle.clone();
            handle.tracker.spawn(async move {
                let _ = h
                    .fail_url_fetch(
                        job,
                        format!("duplicate of job #{} (same URL was re-added)", original.0),
                    )
                    .await;
            });
        }

        Ok(handle)
    }
}

/// Options for [`EngineHandle::add_nzb_opts`].
#[derive(Debug, Clone, Default)]
pub struct AddOpts {
    pub category: Option<String>,
    pub priority: i32,
    /// Torrent-only cumulative upload ratio limit. `None` or zero means
    /// unlimited; non-torrent admission ignores it.
    pub seed_ratio_limit: Option<f64>,
    /// Torrent-only cumulative seeding-time limit in seconds. `None` or zero
    /// means unlimited; non-torrent admission ignores it.
    pub seed_time_limit_secs: Option<u64>,
    pub stop_seeding_on_complete: Option<bool>,
    /// Duplicate-detection metadata (key/score/mode) carried on the job.
    pub dupe: Option<nzbd_types::DupeInfo>,
    /// Add in Paused state (NZBGet `AddPaused`).
    pub paused: bool,
    /// Job parameters set at admit time (a consumer's own tracking id —
    /// Sonarr's `drone`, monarr's `monarr-transfer`). Applied in the same
    /// owner-loop turn as the add, so the param is on the job from its
    /// first appearance in the queue rather than a round-trip later; that
    /// is what makes it visible in the UI, history and compat
    /// `Parameters` with no second write path. `*`-prefixed keys are
    /// reserved for internals and are rejected at the API edge.
    pub params: Vec<(String, String)>,
    /// Tags supplied through the qBittorrent compatibility API. They are
    /// persisted only after duplicate-hash detection succeeds, so a repeated
    /// add cannot relabel an existing torrent with a new transfer token.
    pub torrent_tags: Vec<String>,
    /// Who asked for this job (`monarr`, `Sonarr`, `web-ui`), from the
    /// request's client header. Stored as the reserved `*Client` param and
    /// used to name a job whose own documents name it nothing —
    /// `queue::requestor_name`. `None` when the caller is anonymous.
    pub client: Option<String>,
}

/// Carry the requesting client alongside the caller's params, under the
/// reserved `*Client` key. Reserved so a client cannot claim to be someone
/// else through the public params surface, and a param so it rides the
/// snapshot and history with everything else the job knows about itself.
fn with_client(mut params: Vec<(String, String)>, client: Option<String>) -> Vec<(String, String)> {
    if let Some(c) = client
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty())
    {
        params.push((crate::queue::CLIENT_PARAM.to_string(), c));
    }
    params
}

fn validate_external_params(params: &[(String, String)]) -> Result<(), EngineError> {
    if params.iter().any(|(key, _)| key.starts_with('*')) {
        return Err(EngineError::RequestConflict(
            "job parameters cannot set Runner internal keys".into(),
        ));
    }
    Ok(())
}

/// Owner-issued permission for one generation of post-processing work.
#[derive(Clone, Debug)]
pub struct PostAdmission {
    pub job: JobId,
    pub generation: u64,
    pub cancellation: CancellationToken,
}

/// Permission to resume an existing failed-source deletion. This grants no
/// verified payload reads, download progress, repair or publication authority.
#[derive(Clone, Debug)]
pub struct FailedCleanupAdmission {
    pub admission: PostAdmission,
    pub job: nzbd_types::Job,
    pub cleanup: nzbd_state::artifacts::FailedCleanupProof,
}

/// Cloneable handle to a running engine.
#[derive(Clone)]
pub struct EngineHandle {
    artifacts: Arc<nzbd_state::artifacts::Inventory>,
    url_resolutions: Arc<nzbd_state::artifacts::UrlResolutionStore>,
    url_dest_dir: PathBuf,
    import_state_dir: PathBuf,
    url_history: Option<Arc<nzbd_state::history::HistoryDb>>,
    url_outbox_external: bool,
    owner_persist_guard: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    native_seed_recovery_cursor: Arc<std::sync::Mutex<u32>>,
    cmd_tx: mpsc::Sender<EngineMsg>,
    shared: SharedSnapshot,
    events: broadcast::Sender<Event>,
    cancel: CancellationToken,
    tracker: TaskTracker,
    backend_adapter: Arc<std::sync::Mutex<Option<backend::BackendAdapterPort>>>,
    budget_tracker: Arc<pool::BudgetTracker>,
    url_fetches: Arc<Mutex<HashSet<JobId>>>,
    hold_probes: Arc<Mutex<HashSet<JobId>>>,
    recovery_cursor: Arc<std::sync::atomic::AtomicUsize>,
    post_scope: Option<PostAdmission>,
    queue_changes: watch::Receiver<u64>,
    owner_heartbeat: Arc<std::sync::atomic::AtomicU64>,
    shutdown_complete: Arc<std::sync::atomic::AtomicBool>,
}

struct UrlFetchGuard {
    jobs: Arc<Mutex<HashSet<JobId>>>,
    job: JobId,
}

impl Drop for UrlFetchGuard {
    fn drop(&mut self) {
        if let Ok(mut jobs) = self.jobs.lock() {
            jobs.remove(&self.job);
        }
    }
}

#[derive(Clone, Debug)]
pub struct BudgetApplyReceipt {
    pub generation: u64,
    pub allowances: std::collections::HashMap<nzbd_types::ServerId, u16>,
    /// Every spawned connection task observed the generation between NNTP
    /// batches. False means the caller must conservatively reserve the old
    /// capacity until a later acknowledgement.
    pub drained: bool,
}

impl EngineHandle {
    pub fn owner_last_progress_unix(&self) -> u64 {
        self.owner_heartbeat
            .load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn installation_id(&self) -> String {
        self.artifacts.installation.clone()
    }

    pub async fn begin_post_admission(
        &self,
        job: JobId,
    ) -> Result<Option<PostAdmission>, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send(QueueCommand::BeginPostAdmission { job, reply })
            .await?;
        owner_reply(rx).await
    }

    /// Acquire metadata-only replay permission as the current queue authority.
    /// Worker cleanup requires a separate current-grant capability.
    pub async fn begin_failed_cleanup_admission(
        &self,
        job: JobId,
    ) -> Result<Option<FailedCleanupAdmission>, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send(QueueCommand::BeginFailedCleanupAdmission { job, reply })
            .await?;
        owner_reply(rx).await
    }

    pub async fn failed_cleanup_metadata(&self) -> Result<Option<nzbd_types::Job>, EngineError> {
        let admission = self
            .post_scope
            .clone()
            .ok_or_else(|| EngineError::Lifecycle("failed cleanup scope is absent".into()))?;
        let (reply, rx) = oneshot::channel();
        self.send(QueueCommand::FailedCleanupMetadata { admission, reply })
            .await?;
        owner_reply(rx).await
    }
    /// Wait for the current lifetime's verified PP custody. Queue changes
    /// wake retries; cancellation never drops an issued admission request.
    pub async fn wait_post_admission(
        &self,
        job: JobId,
        cancellation: &CancellationToken,
    ) -> Result<Option<PostAdmission>, EngineError> {
        let mut changes = self.queue_changes.clone();
        loop {
            if cancellation.is_cancelled() || self.cancel.is_cancelled() {
                return Ok(None);
            }
            let _ = changes.borrow_and_update();
            // Receive every issued result even if cancellation wins during
            // persistence. An issued scope must be explicitly released.
            if let Some(admission) = self.begin_post_admission(job).await? {
                if cancellation.is_cancelled() || self.cancel.is_cancelled() {
                    self.end_post_admission(admission).await?;
                    return Ok(None);
                }
                return Ok(Some(admission));
            }
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(None),
                _ = self.cancel.cancelled() => return Ok(None),
                changed = changes.changed() => { changed.map_err(|_| EngineError::Closed)?; }
            }
        }
    }

    pub fn for_post_admission(&self, admission: &PostAdmission) -> Self {
        let mut scoped = self.clone();
        scoped.post_scope = Some(admission.clone());
        scoped
    }
    pub async fn end_post_admission(&self, admission: PostAdmission) -> Result<(), EngineError> {
        self.roundtrip_unit(|reply| QueueCommand::EndPostAdmission { admission, reply })
            .await
    }
    pub async fn recovery_document(&self) -> Result<nzbd_state::QueueSnapshotDoc, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send(QueueCommand::RecoveryDocument { reply }).await?;
        owner_reply(rx).await
    }
    pub async fn remove_orphan_torrent_source(&self, job: JobId) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::RemoveOrphanTorrentSource { job, reply })
            .await
    }
    fn command_message(&self, command: QueueCommand) -> EngineMsg {
        match &self.post_scope {
            Some(admission) => EngineMsg::ScopedCommand {
                admission: admission.clone(),
                command: Box::new(command),
            },
            None => EngineMsg::Command(command),
        }
    }
    pub async fn register_repair_attempt(
        &self,
        job: JobId,
        attempt: String,
    ) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::RegisterRepairAttempt {
            job,
            attempt,
            reply,
        })
        .await
    }
    pub async fn update_repair_progress(&self, job: JobId, progress: RepairProgress) {
        let _ = self
            .send(QueueCommand::RepairProgress { job, progress })
            .await;
    }
    pub fn close_repair_attempt(&self, job: JobId, attempt: String) {
        let command = self.command_message(QueueCommand::CloseRepairAttempt { job, attempt });
        if let Err(tokio::sync::mpsc::error::TrySendError::Full(command)) =
            self.cmd_tx.try_send(command)
        {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                let sender = self.cmd_tx.clone();
                runtime.spawn(async move {
                    let _ = sender.send(command).await;
                });
            }
        }
    }

    pub fn artifacts(&self) -> Arc<nzbd_state::artifacts::Inventory> {
        self.artifacts.clone()
    }

    /// Transfer the sole adapter endpoint to the backend runtime. There can
    /// be only one consumer because backend commands are ordered FIFO.
    pub fn take_backend_adapter(&self) -> Option<backend::BackendAdapterPort> {
        self.backend_adapter.lock().ok()?.take()
    }

    pub async fn reserve_torrent_admission(
        &self,
        source: nzbd_types::TorrentSource,
        secret: Vec<u8>,
        opts: AddOpts,
    ) -> Result<JobId, EngineError> {
        validate_external_params(&opts.params)?;
        validate_torrent_tags(&opts.torrent_tags)?;
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::ReserveTorrentAdmission {
            source,
            secret,
            opts,
            reply: tx,
        })
        .await?;
        owner_reply(rx).await?.map_err(EngineError::State)
    }

    pub async fn commit_torrent_admission(
        &self,
        job: JobId,
        name: String,
        opts: AddOpts,
        record: nzbd_types::TorrentRecord,
    ) -> Result<Option<Result<JobId, JobId>>, EngineError> {
        validate_external_params(&opts.params)?;
        validate_torrent_tags(&opts.torrent_tags)?;
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::CommitTorrentAdmission {
            job,
            commit: Box::new(crate::queue::TorrentAdmissionCommit {
                name,
                category: opts.category,
                priority: opts.priority,
                paused: opts.paused,
                params: with_client(opts.params, opts.client),
                record,
            }),
            reply: tx,
        })
        .await?;
        rx.await.map_err(|_| EngineError::Closed)?
    }

    pub async fn commit_managed_torrent_admission(
        &self,
        job: JobId,
        name: String,
        opts: AddOpts,
        record: nzbd_types::TorrentRecord,
        submission_sha256: String,
        descriptor: torrent_acquisitions::NativeDescriptorRef,
    ) -> Result<torrent_acquisitions::ManagedTorrentAdmission, EngineError> {
        validate_external_params(&opts.params)?;
        torrent_acquisitions::managed_token(&opts.params)?;
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::CommitManagedTorrentAdmission {
            job,
            commit: Box::new(crate::queue::TorrentAdmissionCommit {
                name,
                category: opts.category,
                priority: opts.priority,
                paused: opts.paused,
                params: with_client(opts.params, opts.client),
                record,
            }),
            submission_sha256,
            descriptor,
            reply: tx,
        })
        .await?;
        rx.await.map_err(|_| EngineError::Closed)?
    }

    pub async fn start_torrent_acquisition(&self, job: JobId) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::StartTorrentAcquisition { job, reply })
            .await
    }
    pub async fn complete_torrent_acquisition(
        &self,
        job: JobId,
        donor: JobId,
        source: nzbd_state::artifacts::Artifact,
    ) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::CompleteTorrentAcquisition {
            job,
            donor,
            source: Box::new(source),
            reply,
        })
        .await
    }
    pub async fn hold_torrent_acquisition(
        &self,
        job: JobId,
        cause: nzbd_types::HoldCause,
        message: String,
    ) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::HoldTorrentAcquisition {
            job,
            cause,
            message,
            reply,
        })
        .await
    }
    pub fn processing_root(&self) -> &std::path::Path {
        &self.url_dest_dir
    }

    pub async fn cancel_torrent_admission(&self, job: JobId) -> Result<bool, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::CancelTorrentAdmission { job, reply: tx })
            .await?;
        owner_reply(rx).await?.map_err(EngineError::State)
    }
    /// Parse and enqueue an NZB. Parsing happens on the caller's task so a
    /// large or hostile NZB never stalls the queue owner.
    pub async fn add_nzb(
        &self,
        name: &str,
        content: &[u8],
        category: Option<String>,
        priority: i32,
    ) -> Result<JobId, EngineError> {
        self.add_nzb_opts(
            name,
            content,
            AddOpts {
                category,
                priority,
                ..AddOpts::default()
            },
        )
        .await
    }

    /// Add a URL job: registered immediately (status `Fetching`), the NZB
    /// is fetched in the background, then the job queues like any other.
    /// A fetch/parse failure fails the job (history: `FAILURE/FETCH`).
    ///
    /// A transfer-token URL admission is durably idempotent across terminal
    /// jobs, history clearing, and restart. Reusing its token with changed
    /// submission fields returns a conflict.
    pub async fn add_url(
        &self,
        name: &str,
        url: &str,
        opts: AddOpts,
    ) -> Result<JobId, EngineError> {
        validate_external_params(&opts.params)?;
        // Name the placeholder through the same junk-stripper the fetched
        // NZB's metadata pass uses, not a raw `rsplit('/')`: indexer URLs
        // glue query params after the filename ("…af51ab….nzb&i=…&r=<key>")
        // and that used to become the visible job title (field report
        // 2026-07-25) — with the user's API key in it.
        let name = {
            let provided = name.trim();
            let base = if provided.is_empty() { url } else { provided };
            let cleaned = crate::queue::strip_name_junk(base);
            if cleaned.is_empty() {
                "download".to_string()
            } else {
                cleaned
            }
        };
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::AddUrl {
            name,
            url: url.to_string(),
            category: opts.category,
            priority: opts.priority,
            dupe: opts.dupe,
            paused: opts.paused,
            params: with_client(opts.params, opts.client),
            reply: tx,
        })
        .await?;
        let (id, created) = rx.await.map_err(|_| EngineError::Closed)??;
        if created {
            self.spawn_url_fetch(
                id,
                url.to_string(),
                crate::fetch::FetchRetryState::default(),
            );
        }
        Ok(id)
    }

    /// Look up the durable outcome for one transfer token, including after
    /// its visible queue/history row has been removed.
    pub async fn url_admission(
        &self,
        request_token: &str,
    ) -> Result<Option<nzbd_state::UrlAdmissionReceipt>, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::GetUrlAdmission {
            request_token: request_token.to_string(),
            reply: tx,
        })
        .await?;
        owner_reply(rx).await
    }

    /// Background NZB download for a URL job. Shutdown-aware: an in-flight
    /// fetch aborts on cancel instead of holding graceful shutdown open for
    /// up to the 60 s hop timeout.
    fn spawn_url_fetch(&self, id: JobId, url: String, _retry_state: crate::fetch::FetchRetryState) {
        if !self
            .url_fetches
            .lock()
            .is_ok_and(|mut jobs| jobs.insert(id))
        {
            return;
        }
        let handle = self.clone();
        self.tracker.spawn(async move {
            let _fetch_guard = UrlFetchGuard { jobs: handle.url_fetches.clone(), job: id };
            let mut changes = handle.queue_changes.clone();
            let origin = loop {
                match handle.export_job(id).await {
                    Ok(Some(origin)) => break origin,
                    Err(EngineError::Lifecycle(message)) if message == "job verification or writer retirement is still in progress; retry later" => {
                        tokio::select! {
                            _ = handle.cancel.cancelled() => return,
                            result = changes.changed() => if result.is_err() { return; },
                        }
                    }
                    _ => return,
                }
            };
            let retry_state = fetch_retry_state(&origin);
            let identity = match nzbd_state::artifacts::url_source_identity(&origin) { Ok(identity) => identity, Err(_) => return };
            let mut storage_retry = nzbd_state::artifacts::UrlStorageRetry::default();
            let retry_store = handle.url_resolutions.clone(); let retry_id = identity.clone();
            if let Ok(Ok(Some(retry))) = tokio::task::spawn_blocking(move || retry_store.retry(&retry_id)).await {
                let seconds = retry.next_retry_at.saturating_sub(url_retry_now()).max(0) as u64;
                storage_retry = retry;
                tokio::select! { _ = handle.cancel.cancelled() => return, _ = tokio::time::sleep(Duration::from_secs(seconds)) => {} }
            }
            let mut failure = None;
            let mut received = loop {
                let store = handle.url_resolutions.clone(); let cached_origin = origin.clone();
                match tokio::task::spawn_blocking(move || store.source(&cached_origin)).await {
                    Ok(Ok(bytes)) => break bytes,
                    Ok(Err(nzbd_state::artifacts::Error::NotFound)) => return,
                    Ok(Err(nzbd_state::artifacts::Error::Io(error))) => {
                        if !handle.wait_url_storage_retry(&origin, &identity, &format!("cached NZB source read: {error}"), &mut storage_retry).await { return; }
                    }
                    error => { failure = Some(format!("durable URL source requires review: {error:?}")); break None; }
                }
            };
            if received.is_none() && failure.is_none() {
                let mut events = handle.subscribe();
                let fetched = tokio::select! {
                    _ = handle.cancel.cancelled() => return,
                    _ = async {
                        loop { match events.recv().await {
                            Ok(Event::JobDeleted { job }) if job == id => break,
                            Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => break,
                        }}
                    } => return,
                    result = crate::fetch::http_get_observed(&url, retry_state, {
                        let progress_handle = handle.clone();
                        let progress_origin = origin.clone();
                        let progress_identity = identity.clone();
                        move |progress| {
                            let handle = progress_handle.clone(); let origin = progress_origin.clone(); let identity = progress_identity.clone();
                            async move {
                                let mut retry = nzbd_state::artifacts::UrlStorageRetry::default();
                                loop {
                                    if handle.cancel.is_cancelled() { return false; }
                                    match handle.record_url_fetch_progress(id, progress.clone()).await {
                                        Ok(active) => return active,
                                        Err(EngineError::OwnershipLost | EngineError::Closed) => return false,
                                        Err(error) => if !handle.wait_url_storage_retry_after(&origin, &identity, &error.to_string(), &mut retry, progress.next_retry_at).await { return false; },
                                    }
                                }
                            }
                        }
                    }) => result,
                };
                match fetched {
                    Ok(bytes) => received = Some(bytes),
                    Err(crate::fetch::FetchError::Cancelled) => return,
                    Err(error) => failure = Some(error.to_string()),
                }
            }
            if let Some(mut bytes) = received {
                loop {
                    let store = handle.url_resolutions.clone(); let source_origin = origin.clone();
                    // Move the one received buffer through storage retries;
                    // no per-second cloning or parsing on the queue actor.
                    let candidate = bytes;
                    match tokio::task::spawn_blocking(move || { let result = store.cache_source(&source_origin, &candidate); (result, candidate) }).await {
                        Ok((Ok(_), _)) => break,
                        Ok((Err(nzbd_state::artifacts::Error::NotFound), _)) => return,
                        Ok((Err(nzbd_state::artifacts::Error::Conflict(message)), _)) => { failure = Some(message); break; }
                        Ok((Err(error), retained)) => {
                            bytes = retained;
                            if !handle.wait_url_storage_retry(&origin, &identity, &error.to_string(), &mut storage_retry).await { return; }
                        }
                        Err(_) => return,
                    }
                }
            }
            loop {
                if handle.cancel.is_cancelled() { return; }
                let prepared = match handle.prepare_url_resolution(&origin, failure.clone()).await {
                    Ok(prepared) => prepared,
                    Err(EngineError::OwnershipLost | EngineError::Closed) => return,
                    Err(EngineError::Lifecycle(message)) => {
                        // Size/identity/parse constraints are permanent for this
                        // input. Persist an actionable failed disposition once.
                        if failure.as_deref() == Some(&message) {
                            let _ = handle.hold_job(id, nzbd_types::HoldCause::IdentityConflict, "url_fetch", &message).await;
                            tracing::error!(job=id.0, %message, "URL source retained for review"); return;
                        }
                        failure = Some(message); continue;
                    }
                    Err(error) => {
                        if !handle.wait_url_storage_retry(&origin, &identity, &error.to_string(), &mut storage_retry).await { return; }
                        continue;
                    }
                };
                let (tx, rx) = oneshot::channel();
                if handle.send(QueueCommand::CompleteUrlFetch { job: id, source_identity: identity.clone(), prepared: Box::new(prepared), reply: tx }).await.is_err() { return; }
                match rx.await {
                    Ok(Ok(true)) => {
                        if !handle.url_outbox_external {
                            loop {
                                let store = handle.url_resolutions.clone(); let acknowledgement = identity.clone();
                                match tokio::task::spawn_blocking(move || store.acknowledge(&acknowledgement)).await {
                                    Ok(Ok(_)) => break,
                                    error => if !handle.wait_url_storage_retry(&origin, &identity, &format!("URL queue commit is durable; outbox compaction pending: {error:?}"), &mut storage_retry).await { return; },
                                }
                            }
                        }
                        return;
                    }
                    Ok(Ok(false)) | Ok(Err(EngineError::OwnershipLost)) | Err(_) => return,
                    Ok(Err(error)) => if !handle.wait_url_storage_retry(&origin, &identity, &error.to_string(), &mut storage_retry).await { return; },
                }
            }
        });
    }

    async fn wait_url_storage_retry(
        &self,
        origin: &Job,
        identity: &str,
        message: &str,
        fallback: &mut nzbd_state::artifacts::UrlStorageRetry,
    ) -> bool {
        self.wait_url_storage_retry_after(origin, identity, message, fallback, None)
            .await
    }
    async fn wait_url_storage_retry_after(
        &self,
        origin: &Job,
        identity: &str,
        message: &str,
        fallback: &mut nzbd_state::artifacts::UrlStorageRetry,
        not_before: Option<i64>,
    ) -> bool {
        let store = self.url_resolutions.clone();
        let id = identity.to_owned();
        let error = message.to_owned();
        let retry = match tokio::task::spawn_blocking(move || {
            store.schedule_storage_retry_not_before(&id, &error, not_before)
        })
        .await
        {
            Ok(Ok(retry)) => retry,
            _ => {
                let now = url_retry_now();
                nzbd_state::artifacts::UrlStorageRetry {
                    attempts: fallback.attempts.saturating_add(1),
                    next_retry_at: nzbd_state::artifacts::url_storage_retry_deadline(
                        identity,
                        fallback.attempts,
                        now,
                        fallback.next_retry_at,
                        not_before,
                    ),
                    blocked_since: if fallback.blocked_since == 0 {
                        now
                    } else {
                        fallback.blocked_since
                    },
                    error: message.chars().take(4096).collect(),
                }
            }
        };
        *fallback = retry.clone();
        let warning = url_retry_now().saturating_sub(retry.blocked_since) >= 1800;
        tracing::warn!(job=origin.id.0, attempts=retry.attempts, next_retry=retry.next_retry_at, warning, error=%retry.error, "URL storage commit pending; durable source retained");
        let progress = crate::fetch::FetchRetryState {
            attempts: retry.attempts,
            next_retry_at: Some(retry.next_retry_at),
            blocked_since: Some(retry.blocked_since),
            last_error: Some(format!(
                "storage: {}{}",
                retry.error,
                if warning {
                    "; warning: blocked for at least 30 minutes"
                } else {
                    ""
                }
            )),
        };
        if matches!(
            self.record_url_fetch_progress(origin.id, progress).await,
            Err(EngineError::OwnershipLost | EngineError::Closed)
        ) {
            return false;
        }
        let seconds = retry.next_retry_at.saturating_sub(url_retry_now()).max(0) as u64;
        tokio::select! { _ = self.cancel.cancelled() => false, _ = tokio::time::sleep(Duration::from_secs(seconds)) => true }
    }

    async fn prepare_url_resolution(
        &self,
        origin: &Job,
        failure: Option<String>,
    ) -> Result<nzbd_state::artifacts::UrlResolutionRecord, EngineError> {
        let store = self.url_resolutions.clone();
        let source_origin = origin.clone();
        let identity = nzbd_state::artifacts::url_source_identity(origin)
            .map_err(|error| EngineError::Lifecycle(error.to_string()))?;
        let record_id = identity.clone();
        let history = self.url_history.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            if store.is_cancelled(&source_origin)? {
                return Err(nzbd_state::artifacts::Error::NotFound);
            }
            if let Some(record) = store.record(&record_id)? {
                return Ok(Err(record));
            }
            let parsed = match failure {
                Some(error) => Err(error),
                None => {
                    let bytes = store.source(&source_origin)?.ok_or_else(|| {
                        nzbd_state::artifacts::Error::Conflict(
                            "durable NZB source unavailable".into(),
                        )
                    })?;
                    if let Some(history) = history {
                        history
                            .spool_nzb(source_origin.id, &bytes)
                            .map_err(|error| {
                                nzbd_state::artifacts::Error::Io(std::io::Error::other(
                                    error.to_string(),
                                ))
                            })?;
                    }
                    url_preparation_checkpoint(&record_id);
                    nzbd_nzb::parse(&bytes).map_err(|error| format!("nzb parse: {error}"))
                }
            };
            Ok(Ok(parsed))
        })
        .await
        .map_err(|_| EngineError::Closed)?
        .map_err(url_storage_error)?;
        let parsed = match outcome {
            Err(record) => return Ok(record),
            Ok(parsed) => parsed,
        };
        let count = parsed
            .as_ref()
            .map(|parsed| parsed.files.len())
            .unwrap_or(0);
        let count = u32::try_from(count)
            .map_err(|_| EngineError::Lifecycle("NZB contains too many files".into()))?;
        let first = if count == 0 {
            0
        } else {
            let (tx, rx) = oneshot::channel();
            self.send(QueueCommand::ReserveUrlFiles {
                job: origin.id,
                source_identity: identity.clone(),
                count,
                reply: tx,
            })
            .await?;
            rx.await.map_err(|_| EngineError::Closed)??
        };
        let store = self.url_resolutions.clone();
        let origin = origin.clone();
        let root = self.url_dest_dir.clone();
        tokio::task::spawn_blocking(move || {
            let mut staging = queue::QueueState::default(); staging.jobs.push(origin.clone());
            match parsed {
                Ok(parsed) => {
                    let reservation = store.reserve_files(&origin, first, count)?;
                    staging.next_file_id = reservation.first_file_id.saturating_sub(1);
                    staging.complete_url_fetch(origin.id, &parsed, true);
                    let base = queue::job_dir_name(&staging.jobs[0]);
                    // A stable admission suffix prevents concurrently prepared
                    // URLs from claiming the same uncreated directory.
                    staging.jobs[0].dir_name = format!("{base}.job-{}-url-{}", origin.id.0, &identity[..12]);
                    if root.join(&staging.jobs[0].dir_name).symlink_metadata().is_ok() {
                        // Existing bytes require the same durable preparation;
                        // only stage() replay may authorize reuse of that path.
                        return Err(nzbd_state::artifacts::Error::Conflict("URL publication directory already exists; retained bytes require review".into()));
                    }
                }
                Err(error) => {
                    staging.jobs[0].status = nzbd_types::JobStatus::Failed;
                    staging.jobs[0].params.extend([("Failure:Stage".into(), "url_fetch".into()), ("Failure:Reason".into(), error.chars().take(2048).collect())]);
                }
            }
            store.stage(&origin, &staging.jobs[0])
        }).await.map_err(|_| EngineError::Closed)?.map_err(url_storage_error)
    }

    /// Read durable, unacknowledged URL resolutions. Events are only hints;
    /// cluster consumers drain this at startup and periodically after failures.
    pub async fn url_resolution_outbox(
        &self,
        limit: u32,
    ) -> Result<Vec<nzbd_state::artifacts::UrlResolutionRecord>, EngineError> {
        let store = self.url_resolutions.clone();
        tokio::task::spawn_blocking(move || store.pending(limit))
            .await
            .map_err(|_| EngineError::Closed)?
            .map_err(|error| EngineError::Lifecycle(error.to_string()))
    }

    pub async fn url_resolution_faults(
        &self,
        limit: u32,
    ) -> Result<Vec<nzbd_state::artifacts::UrlResolutionFault>, EngineError> {
        let store = self.url_resolutions.clone();
        tokio::task::spawn_blocking(move || store.faults(limit))
            .await
            .map_err(|_| EngineError::Closed)?
            .map_err(url_storage_error)
    }

    pub async fn ack_url_resolution(&self, resolution_id: &str) -> Result<bool, EngineError> {
        let store = self.url_resolutions.clone();
        let identity = resolution_id.to_owned();
        tokio::task::spawn_blocking(move || store.acknowledge(&identity))
            .await
            .map_err(|_| EngineError::Closed)?
            .map_err(|error| EngineError::Lifecycle(error.to_string()))
    }

    /// Restart durable URL placeholders after cluster authority takeover.
    /// Duplicate execution is harmless: completion applies only while the
    /// job remains `Fetching`, and the control adapter revision-checks the
    /// single winning resolution.
    pub async fn resume_url_fetches(&self) -> Result<(), EngineError> {
        for summary in &self.snapshot().jobs {
            if summary.status != nzbd_types::JobStatus::Fetching {
                continue;
            }
            if let Some(job) = self.export_job(summary.id).await? {
                if let Some((_, url)) = job.params.iter().find(|(key, _)| key == "*URL") {
                    self.spawn_url_fetch(job.id, url.clone(), fetch_retry_state(&job));
                }
            }
        }
        Ok(())
    }

    async fn fail_url_fetch(&self, job: JobId, error: String) -> Result<(), EngineError> {
        let Some(origin) = self.export_job(job).await? else {
            return Ok(());
        };
        let source_identity = nzbd_state::artifacts::url_source_identity(&origin)
            .map_err(|error| EngineError::Lifecycle(error.to_string()))?;
        let prepared = self.prepare_url_resolution(&origin, Some(error)).await?;
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::CompleteUrlFetch {
            job,
            source_identity,
            prepared: Box::new(prepared),
            reply: tx,
        })
        .await?;
        rx.await.map_err(|_| EngineError::Closed)??;
        Ok(())
    }

    async fn record_url_fetch_progress(
        &self,
        job_id: JobId,
        progress: crate::fetch::FetchRetryState,
    ) -> Result<bool, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::UpdateUrlFetchProgress {
            job: job_id,
            progress,
            reply: tx,
        })
        .await?;
        rx.await.map_err(|_| EngineError::Closed)?
    }

    /// [`EngineHandle::add_nzb`] with the full option set (dupe metadata,
    /// add-paused).
    pub async fn add_nzb_opts(
        &self,
        name: &str,
        content: &[u8],
        opts: AddOpts,
    ) -> Result<JobId, EngineError> {
        validate_external_params(&opts.params)?;
        let parsed = nzbd_nzb::parse(content)?;
        let name = crate::queue::clean_job_name(name, &parsed);
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::AddParsed {
            name,
            parsed: Box::new(parsed),
            category: opts.category,
            priority: opts.priority,
            dupe: opts.dupe,
            paused: opts.paused,
            params: with_client(opts.params, opts.client),
            reply: tx,
        })
        .await?;
        owner_reply(rx).await?.map_err(EngineError::State)
    }

    pub async fn pause_job(&self, job: JobId) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::Pause { job, reply })
            .await
    }

    pub async fn resume_job(&self, job: JobId) -> Result<bool, EngineError> {
        self.resume_resource_job(job, true).await
    }

    async fn resume_resource_job(&self, job: JobId, force: bool) -> Result<bool, EngineError> {
        if let Some(record) = self.export_job(job).await? {
            if let Some(control) = record.control().filter(|c| c.lifecycle == "held") {
                // Every hold answers an operator resume: the same release the
                // recovery task would run, without its due-time or pause
                // checks. A refusal carries its reason to the caller.
                return match self
                    .release_hold(
                        job,
                        control.revision,
                        if force {
                            RetryBy::Operator
                        } else {
                            RetryBy::Recovery
                        },
                    )
                    .await?
                {
                    Ok(()) => Ok(true),
                    Err(reason) => Err(EngineError::Lifecycle(reason)),
                };
            }
        }
        self.roundtrip_bool(|reply| QueueCommand::Resume { job, reply })
            .await
    }

    /// End the hold at `revision`, by whichever release its cause takes.
    /// Everything that touches the payload mount (the custody check, the
    /// capacity probe) runs in `spawn_blocking` here, never on the owner
    /// loop, so a wedged FUSE/NFS mount cannot stall the queue. The inner
    /// error is the refusal reason.
    pub async fn release_hold(
        &self,
        job: JobId,
        revision: String,
        by: RetryBy,
    ) -> Result<Result<(), String>, EngineError> {
        self.release_hold_prepared(job, revision, by, self.owner_persist_guard.clone(), false)
            .await
    }

    /// A leased execution lane must prove that the exact grant remains live
    /// before touching its source and again at the owner commit.
    pub async fn release_hold_checked(
        &self,
        job: JobId,
        revision: String,
        by: RetryBy,
        authority: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<Result<(), String>, EngineError> {
        self.release_hold_prepared(job, revision, by, Some(authority), true)
            .await
    }

    async fn release_hold_prepared(
        &self,
        job: JobId,
        revision: String,
        by: RetryBy,
        authority: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
        worker_checked: bool,
    ) -> Result<Result<(), String>, EngineError> {
        if authority.as_ref().is_some_and(|check| !check()) {
            return Ok(Err("execution authority is no longer live".into()));
        }
        let Some(record) = self
            .recovery_document()
            .await?
            .jobs
            .into_iter()
            .find(|record| record.id == job)
        else {
            return Ok(Err("job not found".into()));
        };
        let Some(control) = record
            .control()
            .filter(|c| c.lifecycle == "held" && c.revision == revision)
        else {
            return Ok(Err(
                "the hold changed since it was read; reload and retry".into()
            ));
        };
        let source = self
            .artifacts()
            .for_job(job.0)
            .map_err(|error| EngineError::Lifecycle(error.to_string()))?;
        let mut guard = if worker_checked {
            hold_release::HoldReleaseGuard::prepare(&record, source.as_ref(), authority)
        } else {
            hold_release::HoldReleaseGuard::prepare_queue_owner(&record, source.as_ref(), authority)
        };
        if !self.snapshot().queue_authority && !guard.matches(&self.artifacts(), &record, false) {
            return Ok(Err("a live execution grant is required on this node".into()));
        }
        if torrent_acquisitions::preparation_hold(&record, &control) {
            let (reply, result) = oneshot::channel();
            self.send(QueueCommand::RetryHold {
                job,
                revision,
                by,
                custody_checked: false,
                guard,
                reply,
            })
            .await?;
            return result.await.map_err(|_| EngineError::Closed);
        }
        let cause = control.hold_cause();
        if cause == nzbd_types::HoldCause::Allocation {
            // Allocation may have no owned payload yet: no capacity probe,
            // which requires one. The next lease re-allocates.
            return Ok(self
                .roundtrip_bool(|reply| QueueCommand::ReleaseResourceHold {
                    job,
                    revision,
                    by,
                    guard,
                    reply,
                })
                .await?
                .then_some(())
                .ok_or_else(|| "the allocation hold could not be released".to_string()));
        }
        if cause.is_storage() && !control.post_side() {
            if by == RetryBy::Recovery && self.snapshot().disk_low {
                return Ok(Err("waiting for free space".into()));
            }
            return self
                .release_capacity_hold(&record, revision, by, guard)
                .await;
        }
        let custody_checked = if control.post_side() {
            let inventory = self.artifacts();
            let probe_guard = guard.clone();
            match bounded_probe(self.hold_probes.clone(), job, move || {
                if !probe_guard.live() {
                    return Err(nzbd_state::artifacts::Error::Conflict(
                        "worker grant expired before custody probe".into(),
                    ));
                }
                if !probe_guard.checked() {
                    return inventory.prepare_post_retry(job.0);
                }
                inventory.prepare_post_retry_checked(job.0, &|| {
                    if probe_guard.live() {
                        Ok(())
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "worker grant expired during relocation preparation",
                        ))
                    }
                })
            })
            .await
            {
                Ok(Ok(source)) => {
                    guard = guard.rebind_source(&record, Some(&source));
                    true
                }
                Ok(Err(e)) => return Ok(Err(format!("post-processing retry refused: {e}"))),
                Err(why) => return Ok(Err(format!("custody check {why}"))),
            }
        } else {
            false
        };
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::RetryHold {
            job,
            revision,
            by,
            custody_checked,
            guard,
            reply: tx,
        })
        .await?;
        owner_reply(rx).await
    }

    /// Capacity/quota release: the owner's probe backoff, then an admission
    /// and write/flush probe on the payload path, then the owner release.
    async fn release_capacity_hold(
        &self,
        record: &Job,
        revision: String,
        by: RetryBy,
        guard: hold_release::HoldReleaseGuard,
    ) -> Result<Result<(), String>, EngineError> {
        let job = record.id;
        if !self
            .roundtrip_bool(|reply| QueueCommand::BeginResourceProbe {
                job,
                revision: revision.clone(),
                by,
                guard: guard.clone(),
                reply,
            })
            .await?
        {
            return Ok(Err("the storage probe is backing off; retry shortly".into()));
        }
        let path = match self.artifacts().for_job(job.0) {
            Ok(Some(artifact)) => artifact.path,
            Ok(None) => return Ok(Err("held payload identity unavailable".into())),
            Err(e) => return Ok(Err(e.to_string())),
        };
        let record = record.clone();
        let token = format!("{}-{}", job.0, revision);
        // Admission and a write/flush probe must pass before writes resume.
        let inventory = self.artifacts();
        let probe_guard = guard.clone();
        let health = bounded_probe(self.hold_probes.clone(), job, move || {
            if !probe_guard.live() {
                return Err(std::io::Error::other(
                    "worker grant expired before storage probe",
                ));
            }
            inventory
                .validate_download_retry(job.0)
                .map_err(std::io::Error::other)?;
            let bytes = download_capacity::remaining(&record, &path);
            nzbd_state::capacity::health(&path, bytes, &token)
        })
        .await;
        match health {
            Ok(Ok(_reservation)) => {}
            Ok(Err(e)) => return Ok(Err(format!("storage is still not admitting writes: {e}"))),
            Err(why) => return Ok(Err(format!("storage probe {why}"))),
        }
        Ok(self
            .roundtrip_bool(|reply| QueueCommand::ReleaseResourceHold {
                job,
                revision,
                by,
                guard,
                reply,
            })
            .await?
            .then_some(())
            .ok_or_else(|| "the storage hold could not be released".to_string()))
    }

    /// Run one automatic hold-recovery pass now (the background task runs
    /// one every few seconds). Returns how many holds it released.
    pub async fn run_hold_recovery_pass(&self) -> Result<usize, EngineError> {
        hold_recovery::pass(self, true).await
    }

    /// Move an automatic hold's next attempt out, recording why.
    pub async fn reschedule_hold(
        &self,
        job: JobId,
        revision: String,
        reason: String,
    ) -> Result<bool, EngineError> {
        self.reschedule_hold_prepared(
            job,
            revision,
            reason,
            self.owner_persist_guard.clone(),
            false,
        )
        .await
    }

    pub async fn reschedule_hold_checked(
        &self,
        job: JobId,
        revision: String,
        reason: String,
        authority: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<bool, EngineError> {
        self.reschedule_hold_prepared(job, revision, reason, Some(authority), true)
            .await
    }

    async fn reschedule_hold_prepared(
        &self,
        job: JobId,
        revision: String,
        reason: String,
        authority: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
        worker_checked: bool,
    ) -> Result<bool, EngineError> {
        if authority.as_ref().is_some_and(|check| !check()) {
            return Ok(false);
        }
        let Some(record) = self.export_job(job).await? else {
            return Ok(false);
        };
        let source = self
            .artifacts()
            .for_job(job.0)
            .map_err(|error| EngineError::Lifecycle(error.to_string()))?;
        let guard = if worker_checked {
            hold_release::HoldReleaseGuard::prepare(&record, source.as_ref(), authority)
        } else {
            hold_release::HoldReleaseGuard::prepare_queue_owner(&record, source.as_ref(), authority)
        };
        self.roundtrip_bool(|reply| QueueCommand::RescheduleHold {
            job,
            revision,
            reason,
            guard,
            reply,
        })
        .await
    }

    pub async fn delete_job(&self, job: JobId, delete_files: bool) -> Result<bool, EngineError> {
        let torrent = self
            .snapshot()
            .jobs
            .iter()
            .find(|j| j.id == job)
            .is_some_and(|j| j.torrent_role == Some("seed"));
        let native = self
            .export_job(job)
            .await?
            .filter(torrent_acquisitions::is_acquisition);
        if native.is_some()
            && self
                .artifacts
                .for_job(job.0)
                .map_err(|e| EngineError::Lifecycle(e.to_string()))?
                .is_none()
        {
            return self
                .roundtrip_bool(|reply| QueueCommand::Delete {
                    job,
                    delete_files: false,
                    reply,
                })
                .await;
        }
        if delete_files && !torrent {
            let inventory = self.artifacts.clone();
            let artifact = inventory
                .for_job(job.0)
                .map_err(|e| EngineError::Lifecycle(e.to_string()))?;
            let Some(mut artifact) = artifact else {
                return if self.snapshot().jobs.iter().any(|j| j.id == job) {
                    Err(EngineError::Lifecycle(
                        "payload ownership needs review".into(),
                    ))
                } else {
                    Ok(false)
                };
            };
            if artifact.state != "deleted" {
                if !artifact.owned
                    || artifact.keep
                    || artifact
                        .hold
                        .as_deref()
                        .is_some_and(|h| h != "waiting for writer stop")
                {
                    return Err(EngineError::Lifecycle(
                        "payload has Keep, recovery, or ownership review hold".into(),
                    ));
                }
                if matches!(artifact.state.as_str(), "active" | "retiring") {
                    let (tx, rx) = oneshot::channel();
                    self.send(QueueCommand::QuiescePayload { job, reply: tx })
                        .await?;
                    let writers = rx
                        .await
                        .map_err(|_| EngineError::Closed)?
                        .map_err(EngineError::Lifecycle)?;
                    for mut writer in writers {
                        if !*writer.stopped.borrow() {
                            tokio::time::timeout(
                                Duration::from_secs(10),
                                writer.stopped.wait_for(|done| *done),
                            )
                            .await
                            .map_err(|_| {
                                EngineError::Lifecycle("writer stopping; retry deletion".into())
                            })?
                            .map_err(|_| {
                                EngineError::Lifecycle("writer acknowledgement lost".into())
                            })?;
                        }
                    }
                    artifact = inventory
                        .finish(job.0, &artifact.path, &artifact.root, "retained")
                        .map_err(|e| EngineError::Lifecycle(e.to_string()))?;
                }
                let op_id = format!("job-delete-{}", artifact.id);
                let op = match inventory.operation(&op_id) {
                    Ok(op) => op,
                    Err(nzbd_state::artifacts::Error::NotFound) => inventory
                        .request_delete(&artifact.id, artifact.revision, &op_id, 0)
                        .map_err(|e| EngineError::Lifecycle(e.to_string()))?,
                    Err(e) => return Err(EngineError::Lifecycle(e.to_string())),
                };
                let operation = op.id.clone();
                let result = tokio::time::timeout(
                    Duration::from_secs(10),
                    tokio::task::spawn_blocking(move || inventory.execute_delete(&op.id)),
                )
                .await
                .map_err(|_| {
                    EngineError::Lifecycle(format!("operation {operation} pending; retry"))
                })?
                .map_err(|e| EngineError::Lifecycle(e.to_string()))?
                .map_err(|e| EngineError::Lifecycle(e.to_string()))?;
                if result.state != "succeeded" {
                    return Err(EngineError::Lifecycle(format!(
                        "operation {} {}: {}",
                        result.id,
                        result.state,
                        result.error.unwrap_or_default()
                    )));
                }
            }
            // Only retire the queue after the durable terminal receipt exists.
            let _ = self
                .roundtrip_bool(|reply| QueueCommand::Delete {
                    job,
                    delete_files: false,
                    reply,
                })
                .await?;
            return Ok(true);
        }
        self.roundtrip_bool(|reply| QueueCommand::Delete {
            job,
            delete_files,
            reply,
        })
        .await
    }

    pub async fn set_priority(&self, job: JobId, priority: i32) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::SetPriority {
            job,
            priority,
            reply,
        })
        .await
    }

    /// Persist the operator's per-download choice to retain the processing
    /// source if this job fails. This flag is independent from Runner's
    /// custody state and is never set implicitly by handoff.
    pub async fn set_keep_on_failure(&self, job: JobId, keep: bool) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::SetKeepOnFailure { job, keep, reply })
            .await
    }

    pub async fn set_category(
        &self,
        job: JobId,
        category: Option<String>,
    ) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::SetCategory {
            job,
            category,
            reply,
        })
        .await
    }

    pub async fn set_torrent_seed_policy(
        &self,
        job: JobId,
        policy: nzbd_types::SeedPolicy,
    ) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::SetTorrentSeedPolicy { job, policy, reply })
            .await
    }

    /// Reorder a job in the queue (position is the scheduling tiebreaker
    /// within a priority band).
    pub async fn move_job(&self, job: JobId, op: MoveOp) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::Move { job, op, reply })
            .await
    }

    /// Pause all downloading. `source` names the requesting client (UA /
    /// "web-ui" / "cli") — it is logged and carried on the
    /// `queue_pause_changed` event, because a queue that keeps flipping
    /// pause state is ALWAYS some client sending these commands and the
    /// operator needs to see which one.
    pub async fn pause_all(&self, source: &str) -> Result<(), EngineError> {
        let source = source.to_string();
        self.roundtrip_unit(|reply| QueueCommand::PauseAll { source, reply })
            .await
    }

    pub async fn resume_all(&self, source: &str) -> Result<(), EngineError> {
        let source = source.to_string();
        self.roundtrip_unit(|reply| QueueCommand::ResumeAll { source, reply })
            .await
    }

    pub async fn set_speed_limit(&self, bytes_per_sec: Option<u64>) -> Result<(), EngineError> {
        self.roundtrip_unit(|reply| QueueCommand::SetSpeedLimit {
            bytes_per_sec,
            reply,
        })
        .await
    }

    pub async fn set_torrent_upload_limit(
        &self,
        bytes_per_sec: Option<u64>,
    ) -> Result<(), EngineError> {
        self.roundtrip_unit(|reply| QueueCommand::SetTorrentUploadLimit {
            bytes_per_sec,
            reply,
        })
        .await
    }

    /// Set how many jobs may download at once. Returns the value applied
    /// after clamping.
    pub async fn set_max_active_downloads(&self, n: u32) -> Result<u32, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::SetMaxActiveDownloads { n, reply: tx })
            .await?;
        owner_reply(rx).await
    }

    // -- cluster operations (used by nzbd-cluster; harmless elsewhere) ------

    /// Select a durably accepted immutable result through the owner's payload
    /// lane, recording its allocation generation before importing queue facts.
    pub async fn materialize_generation(
        &self,
        job: nzbd_types::Job,
        root: PathBuf,
        target: PathBuf,
        generation: nzbd_state::artifacts::ExternalGeneration,
        history: Option<nzbd_state::HistoryEntry>,
        emit_finished: bool,
    ) -> Result<(), EngineError> {
        let (reply, response) = oneshot::channel();
        self.send(QueueCommand::MaterializeGeneration {
            request: Box::new(owner::GenerationMaterialization {
                job,
                root,
                target,
                generation,
                history,
                emit_finished,
            }),
            reply,
        })
        .await?;
        owner_reply(response).await?.map_err(EngineError::State)
    }

    /// Insert or replace a job with ids preserved; optionally fold its
    /// shared per-job journals (cross-node resume).
    pub async fn import_job(
        &self,
        job: nzbd_types::Job,
        fold_journals: bool,
        emit_finished: bool,
    ) -> Result<(), EngineError> {
        if self.post_scope.is_some() {
            return self
                .apply_post_record(job, true)
                .await?
                .then_some(())
                .ok_or_else(|| EngineError::Lifecycle("post-processing scope changed".into()));
        }
        self.import_job_checked(job, fold_journals, emit_finished, Arc::new(|| true))
            .await
    }

    /// Import with a memory-only authority checkpoint rechecked during CRCs and at apply.
    pub async fn import_job_checked(
        &self,
        job: nzbd_types::Job,
        fold_journals: bool,
        emit_finished: bool,
        checkpoint: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<(), EngineError> {
        let authority = imports::ImportAuthority(checkpoint);
        let (job, preparation) = if fold_journals {
            let (tx, rx) = oneshot::channel();
            self.send(QueueCommand::ImportFence {
                job: job.id,
                reply: tx,
            })
            .await?;
            let expected = rx.await.map_err(|_| EngineError::Closed)??;
            let inventory = self.artifacts.clone();
            let state = self.import_state_dir.clone();
            let destination = self.url_dest_dir.clone();
            let id = job.id;
            let preparation_authority = authority.clone();
            match tokio::task::spawn_blocking(move || {
                imports::prepare(
                    job,
                    expected,
                    &state,
                    &destination,
                    inventory,
                    preparation_authority,
                )
            })
            .await
            {
                Ok(Ok((job, proof))) => (job, Some(proof)),
                failed => {
                    let _ = self
                        .roundtrip_unit(|reply| QueueCommand::EndImportPreparation {
                            job: id,
                            reply,
                        })
                        .await;
                    return match failed {
                        Ok(Err(error)) => Err(error),
                        _ => Err(EngineError::Closed),
                    };
                }
            }
        } else {
            (job, None)
        };
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::ImportJob {
            job: Box::new(job),
            preparation,
            authority,
            emit_finished,
            reply: tx,
        })
        .await?;
        rx.await.map_err(|_| EngineError::Closed)?
    }
    async fn apply_post_record(
        &self,
        job: nzbd_types::Job,
        require_snapshot: bool,
    ) -> Result<bool, EngineError> {
        let admission = self
            .post_scope
            .clone()
            .ok_or_else(|| EngineError::Lifecycle("post-processing scope is absent".into()))?;
        let (reply, rx) = oneshot::channel();
        self.send(QueueCommand::ApplyPostRecord {
            admission,
            job: Box::new(job),
            require_snapshot,
            reply,
        })
        .await?;
        owner_reply(rx).await
    }
    pub async fn import_job_if_present(&self, job: nzbd_types::Job) -> Result<bool, EngineError> {
        if self.post_scope.is_some() {
            return self.apply_post_record(job, true).await;
        }
        let (reply, rx) = oneshot::channel();
        self.send(QueueCommand::ImportJobIfPresent {
            job: Box::new(job),
            require_snapshot: true,
            completion_proof: None,
            reply,
        })
        .await?;
        owner_reply(rx).await
    }
    pub async fn import_worker_job_if_present(
        &self,
        job: nzbd_types::Job,
    ) -> Result<bool, EngineError> {
        let (job, completion_proof) = if job.params.iter().any(|(key, _)| {
            key == nzbd_types::PP_DONE_PARAM || key == "*PP:selected-publication-outcome"
        }) {
            let inventory = self.artifacts.clone();
            tokio::task::spawn_blocking(move || {
                inventory
                    .prepare_worker_completion(&job)
                    .map(|proof| (job, Some(proof)))
            })
            .await
            .map_err(|_| EngineError::Closed)?
            .map_err(|error| EngineError::Lifecycle(error.to_string()))?
        } else {
            (job, None)
        };
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::ImportJobIfPresent {
            job: Box::new(job),
            require_snapshot: false,
            completion_proof,
            reply: tx,
        })
        .await?;
        owner_reply(rx).await
    }

    pub async fn export_job(&self, job: JobId) -> Result<Option<nzbd_types::Job>, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::ExportJob { job, reply: tx })
            .await?;
        let result = owner_reply(rx).await?.map(|job| *job);
        if result.is_none() {
            // Cleanup scopes deliberately cannot read the full recovery
            // document. Their explicit metadata capability still distinguishes
            // a fenced row from an absent job without granting payload export.
            let cleanup_exists = if self.post_scope.is_some() {
                self.failed_cleanup_metadata().await?.is_some()
            } else {
                false
            };
            let exists = cleanup_exists
                || self
                    .recovery_document()
                    .await?
                    .jobs
                    .iter()
                    .any(|record| record.id == job);
            if !exists {
                return Ok(None);
            }
            return Err(EngineError::Lifecycle(
                "job verification or writer retirement is still in progress; retry later".into(),
            ));
        }
        Ok(result)
    }

    /// Enter a post-processing stage. Sets the status *and* appends the
    /// stage to the job's timeline in one command, closing the span being
    /// left with `prev_ms` — the caller's monotonic measurement of it.
    ///
    /// `set_job_status` deliberately does NOT do this: it is the terminal
    /// and PostQueued path, and a stage entered through it would leave the
    /// timeline silently one entry short.
    pub async fn enter_post_stage(
        &self,
        job: JobId,
        stage: nzbd_types::PostStage,
        at_unix: i64,
        prev_ms: Option<u64>,
    ) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::EnterPostStage {
            job,
            stage,
            at_unix,
            prev_ms,
            reply,
        })
        .await
    }

    /// Close the running stage span — the pipeline ended, by success,
    /// failure or an early return. No reply: the job is already on its way
    /// to history and nothing downstream waits on the timing.
    pub async fn close_post_stage(&self, job: JobId, at_unix: i64, ms: Option<u64>) {
        let _ = self
            .send(QueueCommand::ClosePostStage { job, at_unix, ms })
            .await;
    }

    /// Queue a stage close without spawning another task.
    ///
    /// This is the cancellation/drop path used by post-processing. Keeping
    /// the send synchronous preserves mailbox order: a replacement attempt
    /// may enter its first stage immediately after the old future is dropped,
    /// and a detached async close from the old attempt could otherwise arrive
    /// late and close the replacement's brand-new span.
    pub fn close_post_stage_now(&self, job: JobId, at_unix: i64, ms: Option<u64>) {
        let _ = self
            .cmd_tx
            .try_send(self.command_message(QueueCommand::ClosePostStage { job, at_unix, ms }));
    }

    pub async fn remove_job_silent(&self, job: JobId) -> Result<bool, EngineError> {
        if let Some(admission) = &self.post_scope {
            if admission.job != job {
                return Ok(false);
            }
            return self
                .roundtrip_bool(|reply| QueueCommand::FinishPostAdmission {
                    admission: admission.clone(),
                    reply,
                })
                .await;
        }
        self.roundtrip_bool(|reply| QueueCommand::RemoveJobSilent {
            job,
            retirement: None,
            reply,
        })
        .await
    }

    /// Retire executor state after the authority accepted completion and the
    /// caller joined the exact grant's writers/execution. A later grant is
    /// protected by the actor's context check.
    pub async fn remove_accepted_worker_job(
        &self,
        job: JobId,
        context: nzbd_state::artifacts::WorkerControlContext,
    ) -> Result<bool, EngineError> {
        if self.post_scope.is_some() {
            return Ok(false);
        }
        self.roundtrip_bool(|reply| QueueCommand::RemoveJobSilent {
            job,
            retirement: Some(owner::WorkerRetirement::Accepted(context)),
            reply,
        })
        .await
    }

    /// Revoke only the exact imported worker grant after its execution has
    /// stopped. Shared resume journals survive expiry/cancellation; same-ID
    /// successors remain protected through the durable retirement barrier.
    pub async fn retire_worker_job(
        &self,
        job: JobId,
        context: nzbd_state::artifacts::WorkerControlContext,
    ) -> Result<bool, EngineError> {
        if self.post_scope.is_some() {
            return Ok(false);
        }
        self.roundtrip_bool(|reply| QueueCommand::RemoveJobSilent {
            job,
            retirement: Some(owner::WorkerRetirement::Revoked(context)),
            reply,
        })
        .await
    }

    pub async fn set_delegated(
        &self,
        job: JobId,
        node: Option<String>,
    ) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::SetDelegated { job, node, reply })
            .await
    }

    /// Fire-and-forget remote progress overlay for a delegated job.
    pub fn mirror_progress(&self, job: JobId, stats: MirrorStats) {
        let _ = self
            .cmd_tx
            .try_send(self.command_message(QueueCommand::MirrorProgress {
                job,
                node: None,
                stats,
            }));
    }

    pub fn mirror_progress_from(&self, job: JobId, node: String, stats: MirrorStats) {
        let _ = self
            .cmd_tx
            .try_send(self.command_message(QueueCommand::MirrorProgress {
                job,
                node: Some(node),
                stats,
            }));
    }

    pub async fn fold_job_journals(&self, job: JobId) -> Result<(), EngineError> {
        self.roundtrip_unit(|reply| QueueCommand::FoldJobJournals { job, reply })
            .await
    }

    pub async fn set_server_budgets(
        &self,
        budgets: std::collections::HashMap<nzbd_types::ServerId, u16>,
    ) -> Result<BudgetApplyReceipt, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::SetServerBudgets { budgets, reply: tx })
            .await?;
        let (generation, allowances) = rx.await.map_err(|_| EngineError::Closed)?;
        let drained = self
            .budget_tracker
            .wait_for(generation, std::time::Duration::from_secs(10))
            .await;
        Ok(BudgetApplyReceipt {
            generation,
            allowances,
            drained,
        })
    }

    pub async fn set_download_enabled(&self, enabled: bool) -> Result<(), EngineError> {
        self.roundtrip_unit(|reply| QueueCommand::SetDownloadEnabled { enabled, reply })
            .await
    }

    /// Set the operator's per-server connection counts without a
    /// restart. Returns what was actually applied — each value clamped to
    /// the number of connection tasks that server spawned at boot, since
    /// raising the count beyond that needs new sockets and therefore a
    /// bounce.
    pub async fn set_server_connection_caps(
        &self,
        caps: std::collections::HashMap<nzbd_types::ServerId, u16>,
    ) -> Result<std::collections::HashMap<nzbd_types::ServerId, u16>, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::SetServerConnectionCaps { caps, reply: tx })
            .await?;
        owner_reply(rx).await
    }

    /// Become the queue authority (cluster leader took office).
    pub async fn adopt_authority(&self) -> Result<(), EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::AdoptAuthority { reply: tx })
            .await?;
        owner_reply(rx).await?.map_err(EngineError::State)
    }

    pub async fn adopt_replicated_authority(
        &self,
        jobs: Vec<nzbd_types::Job>,
    ) -> Result<(), EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send(QueueCommand::AdoptReplicatedAuthority { jobs, reply })
            .await?;
        owner_reply(rx).await?.map_err(EngineError::State)
    }

    /// Crash-only demotion: keep only `keep` (leases still executing),
    /// stop authority persistence.
    pub async fn retain_jobs(&self, keep: Vec<JobId>) -> Result<(), EngineError> {
        self.roundtrip_unit(|reply| QueueCommand::RetainJobs { keep, reply })
            .await
    }

    /// Explicit PP retry; the owner verifies the observed hold and payload custody.
    pub async fn abandon_relocation(
        &self,
        operation: String,
        revision: u64,
        generation: String,
    ) -> Result<Result<nzbd_state::artifacts::Artifact, String>, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::AbandonRelocation {
            operation,
            revision,
            generation,
            reply: tx,
        })
        .await?;
        owner_reply(rx).await
    }

    /// Persist an automatic retry schedule only while the observed hold still owns this job.
    #[allow(clippy::too_many_arguments)]
    pub async fn schedule_post_retry(
        &self,
        job: JobId,
        revision: String,
        retry_attempt: u8,
        next_retry: i64,
        blocked_since: i64,
        message: String,
    ) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::SchedulePostRetry {
            job,
            revision,
            retry_attempt,
            next_retry,
            blocked_since,
            message,
            reply,
        })
        .await
    }

    /// Operator retry from post-processing's own surfaces (restart, Retry):
    /// the same release as `resume` on a held job.
    pub async fn retry_post_hold(&self, job: JobId, revision: String) -> Result<bool, EngineError> {
        Ok(self
            .release_hold(job, revision, RetryBy::Operator)
            .await?
            .is_ok())
    }

    /// Persist a nonterminal fence before exposing it to consumers.
    pub async fn hold_job(
        &self,
        job: JobId,
        cause: nzbd_types::HoldCause,
        stage: &str,
        message: &str,
    ) -> Result<bool, EngineError> {
        self.hold_job_until(job, cause, stage, message, None).await
    }
    pub async fn hold_job_until(
        &self,
        job: JobId,
        cause: nzbd_types::HoldCause,
        stage: &str,
        message: &str,
        not_before: Option<i64>,
    ) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::HoldJob {
            job,
            cause,
            stage: stage.into(),
            message: message.into(),
            not_before,
            reply,
        })
        .await
    }

    /// Post-processing status transition (PostQueued / Post{stage} / final).
    pub async fn set_job_status(
        &self,
        job: JobId,
        status: nzbd_types::JobStatus,
    ) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::SetJobStatus { job, status, reply })
            .await
    }

    /// Pause or resume one file inside a job (NZBGet FilePause/FileResume).
    pub async fn set_file_paused(
        &self,
        job: JobId,
        file: nzbd_types::FileId,
        paused: bool,
    ) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::SetFilePaused {
            job,
            file,
            paused,
            reply,
        })
        .await
    }

    /// Remove one file from a job (NZBGet FileDelete).
    pub async fn delete_file(
        &self,
        job: JobId,
        file: nzbd_types::FileId,
    ) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::DeleteFile { job, file, reply })
            .await
    }

    /// Delayed-par download: returns recovery blocks now fetching.
    ///
    /// `block_size` is the recovery-set slice size from the job's par2
    /// index; pass it whenever it is known, or hash-named volumes (every
    /// obfuscated post) cannot be priced and repair starves.
    pub async fn unpause_par_blocks(
        &self,
        job: JobId,
        blocks: u32,
        block_size: Option<u64>,
    ) -> Result<u32, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::UnpauseParBlocks {
            job,
            blocks,
            block_size,
            reply: tx,
        })
        .await?;
        owner_reply(rx).await
    }

    /// Lock-free snapshot of the queue (never blocks the engine).
    pub fn snapshot(&self) -> Arc<QueueSnapshot> {
        self.shared.load_full()
    }

    pub fn shared_snapshot(&self) -> SharedSnapshot {
        self.shared.clone()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    /// Publish an event on the engine's broadcast from outside the owner
    /// loop. Post-processing runs in its own crate but its events belong on
    /// the same stream as everything else — a second bus would mean two
    /// orderings, two subscribe calls, and an SSE stream that is only
    /// half the story. Fire-and-forget: no subscribers is not an error.
    /// Report an out-of-space failure observed on a real write (the post
    /// paths use this; the writer reports its own inline). Never blocks:
    /// a full disk is exactly when the engine's mailbox is busiest, and a
    /// dropped duplicate report costs nothing — the latch is already set.
    pub fn report_out_of_space(&self, whence: impl Into<String>) {
        let _ = self.cmd_tx.try_send(EngineMsg::OutOfSpace {
            whence: whence.into(),
        });
    }

    pub fn emit(&self, event: Event) {
        if let Some(admission) = &self.post_scope {
            let _ = self.cmd_tx.try_send(EngineMsg::ScopedEvent {
                admission: admission.clone(),
                event,
            });
        } else {
            let _ = self.events.send(event);
        }
    }

    /// Graceful shutdown: stop leasing, flush journal + snapshot, clear the
    /// unclean marker, wait for every task.
    pub async fn shutdown(&self) -> bool {
        self.cancel.cancel();
        self.tracker.wait().await;
        let inventory = self.artifacts.clone();
        let closed = match tokio::task::spawn_blocking(move || inventory.close()).await {
            Ok(Ok(())) => true,
            Ok(Err(error)) => {
                tracing::error!(%error, "inventory shutdown failed");
                false
            }
            Err(error) => {
                tracing::error!(%error, "inventory shutdown worker failed");
                false
            }
        };
        closed
            && self
                .shutdown_complete
                .load(std::sync::atomic::Ordering::Acquire)
    }

    async fn send(&self, cmd: QueueCommand) -> Result<(), EngineError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.cmd_tx.send(self.command_message(cmd)),
        )
        .await
        .map_err(|_| EngineError::Timeout("waiting to enqueue a command"))?
        .map_err(|_| EngineError::Closed)
    }

    async fn roundtrip_bool(
        &self,
        make: impl FnOnce(oneshot::Sender<bool>) -> QueueCommand,
    ) -> Result<bool, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(make(tx)).await?;
        owner_reply(rx).await
    }

    async fn roundtrip_unit(
        &self,
        make: impl FnOnce(oneshot::Sender<()>) -> QueueCommand,
    ) -> Result<(), EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(make(tx)).await?;
        owner_reply(rx).await
    }
}

/// Bound control-plane waits independently of a storage worker's lifetime.
/// A timeout does not cancel issued storage I/O or permit a replacement worker.
async fn owner_reply<T>(reply: oneshot::Receiver<T>) -> Result<T, EngineError> {
    tokio::time::timeout(Duration::from_secs(10), reply)
        .await
        .map_err(|_| EngineError::Timeout("waiting for the owner reply"))?
        .map_err(|_| EngineError::Closed)
}

/// Reserved durable queue key for qBittorrent tag passthrough. Admission
/// callers cannot set it through `params`; only the typed torrent option is
/// copied here by the queue owner after duplicate detection.
pub const QBIT_TAGS_PARAM: &str = "*Qbit:Tags";

fn validate_torrent_tags(tags: &[String]) -> Result<(), EngineError> {
    let mut seen = HashSet::new();
    let mut transfer_seen = false;
    let invalid = tags.len() > 32
        || tags.iter().any(|tag| {
            let duplicate = !seen.insert(tag);
            let invalid_transfer = if let Some(token) = tag.strip_prefix("monarr-transfer-") {
                let invalid = transfer_seen
                    || token.is_empty()
                    || token.len() > 128
                    || !token
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
                transfer_seen = true;
                invalid
            } else {
                false
            };
            duplicate
                || invalid_transfer
                || tag.is_empty()
                || tag.len() > 256
                || tag.contains(',')
                || tag.chars().any(char::is_control)
        })
        || tags.iter().map(String::len).sum::<usize>() + tags.len().saturating_sub(1) > 1024;
    if invalid {
        return Err(EngineError::RequestConflict(
            "qBittorrent tags are malformed or exceed supported count or size limits".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod owner_deadline_tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn owner_reply_distinguishes_timeout_from_closed_channel() {
        let (reply, rx) = oneshot::channel::<bool>();
        let error = owner_reply(rx).await.unwrap_err();
        assert!(matches!(error, EngineError::Timeout(_)));
        assert!(error.to_string().contains("timed out"));
        assert!(!error.to_string().contains("shutting down"));
        assert!(
            reply.send(true).is_err(),
            "a late reply is not an acknowledgement"
        );

        let (reply, rx) = oneshot::channel::<bool>();
        drop(reply);
        assert!(matches!(owner_reply(rx).await, Err(EngineError::Closed)));

        let (reply, rx) = oneshot::channel();
        reply.send(true).unwrap();
        assert!(owner_reply(rx).await.unwrap());
    }

    #[tokio::test]
    async fn public_requests_preserve_enqueue_and_reply_deadlines() {
        let temp = tempfile::tempdir().unwrap();
        let mut handle = Engine::spawn(EngineConfig::single_node(
            vec![],
            temp.path().join("state"),
            temp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        assert!(handle.shutdown().await);
        tokio::time::pause();
        for full in [false, true] {
            let (sender, receiver) = mpsc::channel(1);
            handle.cmd_tx = sender;
            if full {
                let (reply, _rx) = oneshot::channel();
                assert!(handle
                    .cmd_tx
                    .send(handle.command_message(QueueCommand::ExportJob {
                        job: JobId(1),
                        reply,
                    }))
                    .await
                    .is_ok());
            }
            let error = handle.adopt_authority().await.unwrap_err();
            assert!(matches!(error, EngineError::Timeout(_)), "{error}");
            assert!(error
                .to_string()
                .contains(if full { "enqueue" } else { "owner reply" }));
            drop(receiver);
            assert!(matches!(
                handle.adopt_authority().await,
                Err(EngineError::Closed)
            ));
        }
    }
}

#[cfg(test)]
mod hold_probe_tests {
    use super::*;
    #[tokio::test]
    async fn timed_out_probe_keeps_singleflight_until_blocked_thread_finishes() {
        let probes = Arc::new(Mutex::new(HashSet::new()));
        let gate = Arc::new(std::sync::Barrier::new(2));
        let blocked = gate.clone();
        let owned = probes.clone();
        let (entered, started) = oneshot::channel();
        let first = tokio::spawn(async move {
            bounded_probe_deadline(owned, JobId(901), Duration::from_millis(25), move || {
                let _ = entered.send(());
                blocked.wait();
                true
            })
            .await
        });
        started.await.unwrap();
        let timeout = first.await.unwrap();
        let second = bounded_probe_deadline(
            probes.clone(),
            JobId(901),
            Duration::from_millis(25),
            || true,
        )
        .await;
        tokio::task::spawn_blocking(move || gate.wait())
            .await
            .unwrap();
        for _ in 0..100 {
            if probes.lock().unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(timeout.is_err());
        assert!(second.unwrap_err().contains("previous probe"));
        assert!(probes.lock().unwrap().is_empty());
        assert!(
            bounded_probe_deadline(probes, JobId(901), Duration::from_secs(2), || true)
                .await
                .unwrap()
        );
    }
}

#[cfg(test)]
mod out_of_space_tests {
    use super::*;

    #[test]
    fn storage_unavailable_matches_dropped_volumes_by_errno_only() {
        for n in [
            5, 6, 11, 19, 67, 103, 104, 107, 110, 111, 112, 113, 116, 121,
        ] {
            let msg = format!("write /processing/x.part: whatever (os error {n})");
            assert!(is_storage_unavailable(&msg), "{msg}");
            assert_eq!(
                io_hold_cause(&msg),
                nzbd_types::HoldCause::StorageUnavailable
            );
        }
        for no in [
            "Permission denied (os error 13)",
            "os error 57",
            "(os error 50)",
            "(os error 1107)",
            "a release named Transport endpoint is not connected",
            "finalize: complete checkpoint digest differs",
        ] {
            assert!(!is_storage_unavailable(no), "{no}");
            assert_eq!(io_hold_cause(no), nzbd_types::HoldCause::Io, "{no}");
        }
        assert_eq!(
            io_hold_cause("No space left on device (os error 28)"),
            nzbd_types::HoldCause::Capacity
        );
        assert_eq!(
            io_hold_cause("Disk quota exceeded (os error 122)"),
            nzbd_types::HoldCause::Quota
        );
    }
    use super::is_out_of_space;

    /// Every shape this string arrives in — the writer's stringified
    /// `io::Error`, the fsx wrapper's `op path: source`, the quota variant
    /// a gluster/NFS mount answers instead of ENOSPC.
    #[test]
    fn recognises_the_shapes_a_full_volume_arrives_in() {
        assert!(is_out_of_space("No space left on device (os error 28)"));
        assert!(is_out_of_space(
            "write /working/monarr/completed/x.part05.rar.part: no storage space"
        ));
        assert!(is_out_of_space("io: No space left on device"));
        assert!(is_out_of_space("Disk quota exceeded (os error 122)"));
        assert!(!is_out_of_space("Permission denied (os error 13)"));
        assert!(!is_out_of_space("connection reset by peer"));
    }
}

#[cfg(test)]
mod torrent_admission_persistence_tests {
    use super::{validate_external_params, AddOpts, Engine, EngineConfig, EngineError, Tuning};
    use nzbd_state::torrent_sources::PendingSourceStore;
    use nzbd_types::TorrentSource;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn external_params_cannot_set_qbit_tags() {
        let error =
            validate_external_params(&[("*Qbit:Tags".into(), "monarr-transfer-forged".into())])
                .expect_err("qBittorrent tags are an internal persisted field");
        assert!(error.to_string().contains("Runner internal keys"));
    }

    #[tokio::test]
    async fn cancellation_reports_snapshot_failure_and_keeps_recovery_evidence() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        let authority = Arc::new(AtomicBool::new(true));
        let mut config = EngineConfig::single_node(
            vec![],
            state_dir.clone(),
            temp.path().join("dest"),
            Tuning::default(),
            None,
        );
        config.persist_guard = Some({
            let authority = authority.clone();
            Arc::new(move || authority.load(Ordering::SeqCst))
        });
        let engine = Engine::spawn(config).await.unwrap();
        let job = engine
            .reserve_torrent_admission(
                TorrentSource::Magnet,
                b"magnet:?xt=urn:btih:0000000000000000000000000000000000000000".to_vec(),
                AddOpts::default(),
            )
            .await
            .unwrap();

        authority.store(false, Ordering::SeqCst);
        assert!(matches!(
            engine.cancel_torrent_admission(job).await,
            Err(EngineError::State(_))
        ));

        let persisted = nzbd_state::SnapshotStore::open(&state_dir)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert_eq!(persisted.pending_admissions.len(), 1);
        assert_eq!(persisted.pending_admissions[0].job_id, job);
        assert_eq!(
            PendingSourceStore::open(&state_dir)
                .unwrap()
                .inventory()
                .unwrap(),
            vec![job]
        );

        engine.shutdown().await;
    }
}
