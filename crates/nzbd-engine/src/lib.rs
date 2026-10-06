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

pub mod backend;
pub mod events;
pub mod failover;
pub mod fetch;
pub mod queue;
pub mod rate;
pub mod snapshot;
pub mod torrent_runtime;
pub mod volumes;

mod owner;
mod pool;
mod writer;

pub use events::Event;
pub use owner::{MirrorStats, MoveOp};
pub use queue::clean_job_name;
pub use snapshot::{
    new_shared_snapshot, JobSummary, QueueSnapshot, RepairPhase, RepairProgress, ServerVolume,
    SharedSnapshot,
};

use nzbd_nntp::transport::{tls_client_config, TlsClientConfig};
use nzbd_types::{JobId, ServerDef, TlsMode};
use owner::{EngineMsg, Owner, QueueCommand};
use rate::{RateLimiter, SpeedMeter};
use std::path::PathBuf;
use std::sync::Arc;
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
    #[error("engine is shutting down")]
    Closed,
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

        let owner = Owner::recover(
            &cfg.state_dir,
            cfg.artifact_dir.as_deref(),
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
        )?;
        // Seed the shared snapshot with recovered state synchronously, so
        // the API never serves an empty queue in the window before the
        // async run loop's first publish (UI "queue is empty" flash).
        let mut owner = owner;
        owner.seed_snapshot();
        let artifacts = owner.artifacts.clone();
        {
            let inventory = artifacts.clone();
            let stop = cancel.clone();
            tracker.spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(30));
                loop {
                    tokio::select! { _ = stop.cancelled() => break, _ = interval.tick() => {} }
                    let inventory = inventory.clone();
                    if let Ok(Err(e)) = tokio::task::spawn_blocking(move || inventory.tick()).await
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
            crate::owner::plan_url_refetches(owner.pending_url_fetches());

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
            cmd_tx: engine_tx,
            shared,
            events,
            cancel,
            tracker,
            backend_adapter: Arc::new(std::sync::Mutex::new(Some(backend_adapter))),
            budget_tracker,
        };

        for (job, url) in refetch {
            tracing::info!(job = job.0, %url, "re-spawning NZB fetch for job recovered mid-fetch");
            handle.spawn_url_fetch(job, url);
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

/// Cloneable handle to a running engine.
#[derive(Clone)]
pub struct EngineHandle {
    artifacts: Arc<nzbd_state::artifacts::Inventory>,
    cmd_tx: mpsc::Sender<EngineMsg>,
    shared: SharedSnapshot,
    events: broadcast::Sender<Event>,
    cancel: CancellationToken,
    tracker: TaskTracker,
    backend_adapter: Arc<std::sync::Mutex<Option<backend::BackendAdapterPort>>>,
    budget_tracker: Arc<pool::BudgetTracker>,
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
        let command = EngineMsg::Command(QueueCommand::CloseRepairAttempt { job, attempt });
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
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::ReserveTorrentAdmission {
            source,
            secret,
            opts,
            reply: tx,
        })
        .await?;
        rx.await
            .map_err(|_| EngineError::Closed)?
            .map_err(EngineError::State)
    }

    pub async fn commit_torrent_admission(
        &self,
        job: JobId,
        name: String,
        opts: AddOpts,
        record: nzbd_types::TorrentRecord,
    ) -> Result<Option<Result<JobId, JobId>>, EngineError> {
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
        rx.await.map_err(|_| EngineError::Closed)
    }

    pub async fn cancel_torrent_admission(&self, job: JobId) -> Result<bool, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::CancelTorrentAdmission { job, reply: tx })
            .await?;
        rx.await
            .map_err(|_| EngineError::Closed)?
            .map_err(EngineError::State)
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
    /// Adding a URL that is already `Fetching` returns the existing job's
    /// id instead of creating a duplicate (clients re-add when a fetch
    /// looks stalled; each retry must not queue another copy).
    pub async fn add_url(
        &self,
        name: &str,
        url: &str,
        opts: AddOpts,
    ) -> Result<JobId, EngineError> {
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
        let (id, created) = rx.await.map_err(|_| EngineError::Closed)?;
        if created {
            self.spawn_url_fetch(id, url.to_string());
        }
        Ok(id)
    }

    /// Background NZB download for a URL job. Shutdown-aware: an in-flight
    /// fetch aborts on cancel instead of holding graceful shutdown open for
    /// up to the 60 s hop timeout.
    fn spawn_url_fetch(&self, id: JobId, url: String) {
        let handle = self.clone();
        self.tracker.spawn(async move {
            let fetched = tokio::select! {
                _ = handle.cancel.cancelled() => return, // shutting down
                r = crate::fetch::http_get(&url) => r,
            };
            let outcome = match fetched {
                Ok(bytes) => match nzbd_nzb::parse(&bytes) {
                    Ok(parsed) => Ok(parsed),
                    Err(e) => Err(format!("nzb parse: {e}")),
                },
                Err(e) => Err(e.to_string()),
            };
            match outcome {
                Ok(parsed) => {
                    let (tx, rx) = oneshot::channel();
                    let _ = handle
                        .send(QueueCommand::CompleteUrlFetch {
                            job: id,
                            parsed: Box::new(parsed),
                            reply: tx,
                        })
                        .await;
                    let _ = rx.await;
                }
                Err(error) => {
                    let _ = handle.fail_url_fetch(id, error).await;
                }
            }
        });
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
                    self.spawn_url_fetch(job.id, url.clone());
                }
            }
        }
        Ok(())
    }

    async fn fail_url_fetch(&self, job: JobId, error: String) -> Result<(), EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::FailUrlFetch {
            job,
            error,
            reply: tx,
        })
        .await?;
        rx.await.map_err(|_| EngineError::Closed)
    }

    /// [`EngineHandle::add_nzb`] with the full option set (dupe metadata,
    /// add-paused).
    pub async fn add_nzb_opts(
        &self,
        name: &str,
        content: &[u8],
        opts: AddOpts,
    ) -> Result<JobId, EngineError> {
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
        rx.await
            .map_err(|_| EngineError::Closed)?
            .map_err(EngineError::State)
    }

    pub async fn pause_job(&self, job: JobId) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::Pause { job, reply })
            .await
    }

    pub async fn resume_job(&self, job: JobId) -> Result<bool, EngineError> {
        if let Some(record) = self.export_job(job).await? {
            if let Some(control) = record.control().filter(|c| c.lifecycle == "held") {
                // Allocation may have no owned payload yet: do not run the
                // capacity probe, which requires one. The queue owner persists
                // release and wakes normal allocation admission for this job.
                if control.cause == "allocation" {
                    return self
                        .roundtrip_bool(|reply| QueueCommand::ReleaseResourceHold {
                            job,
                            revision: control.revision.clone(),
                            reply,
                        })
                        .await;
                }
                if !matches!(control.cause.as_str(), "capacity" | "quota") {
                    return Ok(false);
                }
                let revision = control.revision.clone();
                if !self
                    .roundtrip_bool(|reply| QueueCommand::BeginResourceProbe {
                        job,
                        revision: revision.clone(),
                        reply,
                    })
                    .await?
                {
                    return Ok(false);
                }
                let inventory = self.artifacts();
                let path = inventory
                    .for_job(job.0)
                    .map_err(|e| EngineError::Lifecycle(e.to_string()))?
                    .ok_or_else(|| {
                        EngineError::Lifecycle("held payload identity unavailable".into())
                    })?
                    .path;
                let bytes = record
                    .files
                    .iter()
                    .map(|f| {
                        let key = format!("*File:size:{}", f.id.0);
                        record
                            .params
                            .iter()
                            .find(|(k, _)| k == &key)
                            .and_then(|(_, v)| v.parse::<u64>().ok())
                            .unwrap_or_else(|| f.segments.iter().map(|s| s.size as u64).sum())
                    })
                    .sum();
                let token = format!("{}-{}", job.0, revision);
                // Explicit resume is the operator release for unavailable quota
                // telemetry. Admission and a write/flush probe still must pass.
                let health = tokio::task::spawn_blocking(move || {
                    nzbd_state::capacity::health(&path, bytes, &token)
                })
                .await;
                let Ok(Ok(_reservation)) = health else {
                    return Ok(false);
                };
                return self
                    .roundtrip_bool(|reply| QueueCommand::ReleaseResourceHold {
                        job,
                        revision,
                        reply,
                    })
                    .await;
            }
        }
        self.roundtrip_bool(|reply| QueueCommand::Resume { job, reply })
            .await
    }

    pub async fn delete_job(&self, job: JobId, delete_files: bool) -> Result<bool, EngineError> {
        let torrent = self
            .snapshot()
            .jobs
            .iter()
            .find(|j| j.id == job)
            .is_some_and(|j| j.kind == nzbd_types::JobKind::Torrent);
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
        rx.await.map_err(|_| EngineError::Closed)
    }

    // -- cluster operations (used by nzbd-cluster; harmless elsewhere) ------

    /// Insert or replace a job with ids preserved; optionally fold its
    /// shared per-job journals (cross-node resume).
    pub async fn import_job(
        &self,
        job: nzbd_types::Job,
        fold_journals: bool,
        emit_finished: bool,
    ) -> Result<(), EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::ImportJob {
            job: Box::new(job),
            fold_journals,
            emit_finished,
            reply: tx,
        })
        .await?;
        rx.await.map_err(|_| EngineError::Closed)
    }

    /// Atomically replace an existing authority copy without ever inserting a
    /// missing job, returning true only after its queue snapshot commits.
    /// Used before and after awaited terminal side effects so demotion or an
    /// ordinary snapshot I/O failure cannot leave an in-memory-only finalizer
    /// key or resurrect a job the node just lost.
    pub async fn import_job_if_present(&self, job: nzbd_types::Job) -> Result<bool, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::ImportJobIfPresent {
            job: Box::new(job),
            reply: tx,
        })
        .await?;
        rx.await.map_err(|_| EngineError::Closed)
    }

    pub async fn export_job(&self, job: JobId) -> Result<Option<nzbd_types::Job>, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::ExportJob { job, reply: tx })
            .await?;
        rx.await
            .map(|o| o.map(|b| *b))
            .map_err(|_| EngineError::Closed)
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
            .try_send(EngineMsg::Command(QueueCommand::ClosePostStage {
                job,
                at_unix,
                ms,
            }));
    }

    pub async fn remove_job_silent(&self, job: JobId) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::RemoveJobSilent { job, reply })
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
            .try_send(EngineMsg::Command(QueueCommand::MirrorProgress {
                job,
                node: None,
                stats,
            }));
    }

    pub fn mirror_progress_from(&self, job: JobId, node: String, stats: MirrorStats) {
        let _ = self
            .cmd_tx
            .try_send(EngineMsg::Command(QueueCommand::MirrorProgress {
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
        rx.await.map_err(|_| EngineError::Closed)
    }

    /// Become the queue authority (cluster leader took office).
    pub async fn adopt_authority(&self) -> Result<(), EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(QueueCommand::AdoptAuthority { reply: tx })
            .await?;
        rx.await
            .map_err(|_| EngineError::Closed)?
            .map_err(EngineError::State)
    }

    pub async fn adopt_replicated_authority(
        &self,
        jobs: Vec<nzbd_types::Job>,
    ) -> Result<(), EngineError> {
        self.roundtrip_unit(|reply| QueueCommand::AdoptReplicatedAuthority { jobs, reply })
            .await
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
        rx.await.map_err(|_| EngineError::Closed)
    }

    pub async fn retry_post_hold(&self, job: JobId, revision: String) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::RetryPostHold {
            job,
            revision,
            reply,
        })
        .await
    }

    /// Persist a nonterminal fence before exposing it to consumers.
    pub async fn hold_job(
        &self,
        job: JobId,
        cause: &str,
        stage: &str,
        message: &str,
    ) -> Result<bool, EngineError> {
        self.roundtrip_bool(|reply| QueueCommand::HoldJob {
            job,
            cause: cause.into(),
            stage: stage.into(),
            message: message.into(),
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
        rx.await.map_err(|_| EngineError::Closed)
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
        let _ = self.events.send(event);
    }

    /// Graceful shutdown: stop leasing, flush journal + snapshot, clear the
    /// unclean marker, wait for every task.
    pub async fn shutdown(&self) {
        self.cancel.cancel();
        self.tracker.wait().await;
        if let Err(error) = self.artifacts.close() {
            tracing::error!(%error,"inventory shutdown failed");
        }
    }

    async fn send(&self, cmd: QueueCommand) -> Result<(), EngineError> {
        self.cmd_tx
            .send(EngineMsg::Command(cmd))
            .await
            .map_err(|_| EngineError::Closed)
    }

    async fn roundtrip_bool(
        &self,
        make: impl FnOnce(oneshot::Sender<bool>) -> QueueCommand,
    ) -> Result<bool, EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(make(tx)).await?;
        rx.await.map_err(|_| EngineError::Closed)
    }

    async fn roundtrip_unit(
        &self,
        make: impl FnOnce(oneshot::Sender<()>) -> QueueCommand,
    ) -> Result<(), EngineError> {
        let (tx, rx) = oneshot::channel();
        self.send(make(tx)).await?;
        rx.await.map_err(|_| EngineError::Closed)
    }
}

#[cfg(test)]
mod out_of_space_tests {
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
    use super::{AddOpts, Engine, EngineConfig, EngineError, Tuning};
    use nzbd_state::torrent_sources::PendingSourceStore;
    use nzbd_types::TorrentSource;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

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
