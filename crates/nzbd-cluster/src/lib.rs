//! Cluster runtime (CLUSTERING.md): elected leader + workers over a shared
//! work volume. Every node runs the same subsystems — an engine in
//! worker mode, the election observer, the node registry, the worker loop
//! and the (dormant until elected) leader scheduler — plus an API layer
//! that answers locally when leader and proxies to the leader otherwise.
//!
//! Phase C1 distributes whole-job downloads; PP leases (C2) reuse the same
//! protocol when phase 2 lands.

pub mod control;
pub mod election;
pub mod http;
pub mod layout;
pub mod leader;
pub mod proto;
pub mod proxy;
pub mod range;
pub mod registry;
pub mod worker;

use axum::routing::get;
use axum::{middleware, response::IntoResponse, Json, Router};
use control::ControlStore;
use election::{persist_guard, spawn_election, spawn_replicated_election, ElectionCfg, LeaderView};
use http::ClusterClient;
use layout::SharedLayout;
use leader::{spawn_leader_task, LeaderDurability, LeaderShared};
use nzbd_engine::{Engine, EngineConfig, EngineHandle, Tuning};
use nzbd_types::ServerDef;
use proxy::{proxy_to_leader, ProxyState};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use worker::{spawn_worker, ActiveLeases};

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("engine: {0}")]
    Engine(#[from] nzbd_engine::EngineError),
    #[error("replicated control: {0}")]
    Control(String),
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub cluster_id: String,
    pub node_name: String,
    pub shared_dir: PathBuf,
    /// How peers reach this node's API (scheme + host + port).
    pub advertise_url: String,
    pub secret: String,
    pub coordinator: bool,
    pub priority: u32,
    pub download: bool,
    pub max_download_jobs: u32,
    pub post_process: bool,
    /// Concurrent PP pipelines this node may run (C2).
    pub pp_slots: u32,
    pub lease_interval: Duration,
    pub takeover_after: Duration,
    pub worker_ttl: Duration,
    pub control_dir: PathBuf,
    pub control_node_id: u64,
    pub control_raft_bind: String,
    pub control_api_bind: String,
    pub control_peers: Vec<ControlPeer>,
    pub download_weight: u32,
    pub pp_weight: u32,
    /// Every configured write root on this node, used by the engine's
    /// enforcing low-disk guard.
    pub disk_guard_roots: Vec<nzbd_engine::volumes::DiskGuardRoot>,
    pub torrent_payload_roots: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPeer {
    pub id: u64,
    pub raft_addr: String,
    pub api_addr: String,
}

/// Post-processing wiring for a cluster node (C2): the PP pipeline config
/// and the history store (SQLite local, JSONL on the shared volume).
#[derive(Clone)]
pub struct PpSetup {
    pub post: nzbd_post::manager::PostConfig,
    pub history: std::sync::Arc<nzbd_state::history::HistoryDb>,
}

pub struct ClusterRuntime {
    pub engine: EngineHandle,
    cfg: ClusterConfig,
    view: watch::Receiver<LeaderView>,
    leader_shared: Arc<LeaderShared>,
    client: ClusterClient,
    pp: Option<PpSetup>,
    history_worker: Option<nzbd_state::history::HistoryWorker>,
    pp_manager: Option<nzbd_post::manager::PostManagerHandle>,
    control: Option<ControlStore>,
    diagnostics: watch::Receiver<serde_json::Value>,
    cancel: CancellationToken,
    tracker: TaskTracker,
}

impl ClusterRuntime {
    /// Boot the node: engine in worker mode (empty queue; per-node fenced
    /// journals on the shared volume), election, registry, worker loop,
    /// leader task. Queue authority is adopted if/when this node wins.
    pub async fn start(
        cfg: ClusterConfig,
        servers: Vec<ServerDef>,
        tuning: Tuning,
        dest_dir: PathBuf,
        speed_limit_bps: Option<u64>,
        max_active_downloads: Option<u32>,
        mut pp: Option<PpSetup>,
    ) -> Result<ClusterRuntime, ClusterError> {
        let layout = SharedLayout::new(&cfg.shared_dir, &cfg.node_name)?;
        if let Some(setup) = pp.as_mut() {
            if setup.post.failure_action == nzbd_post::manager::FailureAction::Park {
                // A leader can lose authority after the move but before its
                // terminal stamp. Park on the shared cluster volume so the
                // successor can observe and finish that idempotent action.
                setup.post.failed_dir = Some(cfg.shared_dir.join(".nzbd-cluster/failed"));
            }
        }
        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();

        let owner_incarnation = format!(
            "{}-{}-{}",
            cfg.node_name,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let control = if cfg.coordinator {
            let store = ControlStore::start(&cfg)
                .await
                .map_err(ClusterError::Control)?;
            migrate_legacy_control(&store, &layout, &cfg)
                .await
                .map_err(ClusterError::Control)?;
            Some(store)
        } else {
            None
        };

        let election_cfg = ElectionCfg {
            cluster_id: cfg.cluster_id.clone(),
            node: cfg.node_name.clone(),
            api_url: cfg.advertise_url.clone(),
            eligible: cfg.coordinator,
            priority: cfg.priority,
            lease_interval: cfg.lease_interval,
            takeover_after: cfg.takeover_after,
        };
        let view = if let Some(store) = control.clone() {
            spawn_replicated_election(
                layout.clone(),
                election_cfg,
                store,
                owner_incarnation.clone(),
                cancel.clone(),
                &tracker,
            )
        } else {
            spawn_election(layout.clone(), election_cfg, cancel.clone(), &tracker)
        };

        let guard = persist_guard(layout.clone(), view.clone(), cfg.node_name.clone());
        let engine = Engine::spawn(EngineConfig {
            servers: servers.clone(),
            // Cluster startup is fail-closed. Role transitions enable
            // ordinary downloads only while this process is a worker.
            download_enabled: false,
            state_dir: layout.state_dir(),
            artifact_dir: Some(cfg.control_dir.join("file-lifecycle")),
            dest_dir: dest_dir.clone(),
            torrent_payload_roots: cfg.torrent_payload_roots.clone(),
            history: pp.as_ref().map(|setup| setup.history.clone()),
            disk_guard_roots: cfg.disk_guard_roots.clone(),
            tuning,
            speed_limit_bps,
            max_active_downloads,
            persist_queue: false, // adopted on taking office
            journal_suffix: cfg.node_name.clone(),
            persist_guard: Some(guard.clone()),
        })
        .await?;

        // Cluster engines start fail-closed. A grant/heartbeat from a leader
        // that understands provider partitioning must explicitly enable
        // connections. This also makes a new PP worker safe under an older
        // leader, whose PP grants carried no budget capability at all.
        let zero: std::collections::HashMap<_, _> = servers.iter().map(|s| (s.id, 0u16)).collect();
        let _ = engine.set_server_budgets(zero).await;

        let client = ClusterClient::new(cfg.secret.clone());
        let leader_shared = LeaderShared::new(
            engine.clone(),
            layout.clone(),
            dest_dir.clone(),
            cfg.clone(),
            servers.clone(),
            view.clone(),
            LeaderDurability::new(
                control.clone(),
                pp.as_ref().map(|setup| setup.history.clone()),
                owner_incarnation.clone(),
            )
            .with_post(pp.as_ref().map(|setup| setup.post.clone())),
        );
        spawn_leader_task(leader_shared.clone(), cancel.clone(), &tracker);
        registry::spawn_registry(
            layout.clone(),
            cfg.clone(),
            engine.clone(),
            cancel.clone(),
            &tracker,
        );
        // URL fetch is the one queue transition that completes after its API
        // request. Its placeholder is already durable; resolve only that
        // exact row, under the same revision and serialization rules.
        {
            let mut events = engine.subscribe();
            let event_engine = engine.clone();
            let event_shared = leader_shared.clone();
            let event_cancel = cancel.clone();
            tracker.spawn(async move {
                loop {
                    let job = tokio::select! {
                        _ = event_cancel.cancelled() => break,
                        event = events.recv() => match event {
                            Ok(nzbd_engine::Event::UrlFetchResolved { job }) => job,
                            Ok(_) => continue,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    };
                    if !event_shared.view.borrow().is_me {
                        continue;
                    }
                    let _serial = event_shared.mutation_serial.lock().await;
                    let desired = event_engine.export_job(job).await.ok().flatten();
                    let result = match (&event_shared.control, desired) {
                        (Some(control), Some(job)) => control.commit_url_resolution(&job).await,
                        _ => Err("resolved URL job disappeared".into()),
                    };
                    if let Err(error) = result {
                        tracing::error!(job = job.0, %error, "URL resolution rolled back");
                        let _ = event_shared.restore_control_projection().await;
                    }
                }
            });
        }
        // Filesystem and control-plane inspection stays off request paths.
        // The endpoint below serves this bounded background snapshot.
        let initial_diagnostics = serde_json::json!({"status": "starting"});
        let (diagnostics_tx, diagnostics) = watch::channel(initial_diagnostics);
        {
            let diagnostic_layout = layout.clone();
            let diagnostic_view = view.clone();
            let diagnostic_leader = leader_shared.clone();
            let diagnostic_node = cfg.node_name.clone();
            let diagnostic_control = control.clone();
            let diagnostic_cancel = cancel.clone();
            tracker.spawn(async move {
                loop {
                    let v = diagnostic_view.borrow().clone();
                    let leader = diagnostic_leader.diagnostic_snapshot();
                    let control_healthy = match &diagnostic_control {
                        Some(control) => Some(control.is_healthy().await),
                        None => None,
                    };
                    let _ = diagnostics_tx.send(serde_json::json!({
                        "self": diagnostic_node,
                        "role": if v.is_me { "leader" } else { "worker" },
                        "is_leader": v.is_me,
                        "epoch": v.epoch(),
                        "leader": v.record.as_ref().map(|record| serde_json::json!({
                            "node": record.node,
                            "api_url": record.api_url,
                        })),
                        "control": {
                            "mode": "hiqlite-fixed-voters",
                            "authority_available": v.record.is_some(),
                            "quorum_commit_healthy": control_healthy,
                        },
                        "nodes": registry::read_nodes(&diagnostic_layout),
                        "work": leader,
                        "sampled_at_unix_ms": std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default().as_millis(),
                    }));
                    tokio::select! {
                        _ = diagnostic_cancel.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                    }
                }
            });
        }

        let active: ActiveLeases = Default::default();
        spawn_worker(
            cfg.clone(),
            servers.clone(),
            engine.clone(),
            view.clone(),
            client.clone(),
            active.clone(),
            pp.clone(),
            dest_dir.clone(),
            owner_incarnation.clone(),
            cancel.clone(),
            &tracker,
        );
        let history = pp.as_ref().map(|s| s.history.clone());
        let _ = &history; // (kept alongside pp in the runtime below)

        // The elected authority is a scheduler/projection only. It does not
        // execute unfenced local PP; all attempts run under remote leases and
        // publish private immutable generations.
        let pp_manager = None;

        // Crash-only demotion: on losing leadership, keep only the jobs we
        // still execute as a worker; drop authority state.
        {
            let engine = engine.clone();
            let mut view_rx = view.clone();
            let active = active.clone();
            let demote_cancel = cancel.clone();
            let worker_downloads = cfg.download;
            tracker.spawn(async move {
                let mut was_me = view_rx.borrow().is_me;
                let _ = engine
                    .set_download_enabled(!was_me && worker_downloads)
                    .await;
                loop {
                    tokio::select! {
                        _ = demote_cancel.cancelled() => break,
                        changed = view_rx.changed() => {
                            if changed.is_err() { break }
                        }
                    }
                    let is_me = view_rx.borrow().is_me;
                    let _ = engine
                        .set_download_enabled(!is_me && worker_downloads)
                        .await;
                    if was_me && !is_me {
                        let keep: Vec<_> =
                            active.lock().unwrap().values().map(|st| st.job).collect();
                        tracing::warn!(kept = keep.len(), "demoted: dropping authority state");
                        let _ = engine.retain_jobs(keep).await;
                    }
                    was_me = is_me;
                }
            });
        }
        let history_worker = pp
            .as_ref()
            .map(|s| s.history.start_worker())
            .transpose()
            .map_err(|e| ClusterError::Control(format!("history worker: {e}")))?;
        tracker.close();

        Ok(ClusterRuntime {
            engine,
            cfg,
            view,
            leader_shared,
            client,
            pp,
            history_worker,
            pp_manager,
            control,
            diagnostics,
            cancel,
            tracker,
        })
    }

    pub fn is_leader(&self) -> bool {
        self.view.borrow().is_me
    }

    pub fn leader_view(&self) -> LeaderView {
        self.view.borrow().clone()
    }

    /// A cheap "am I the leader right now?" probe for gating work (e.g. the
    /// post-processing manager runs only on the queue authority until C2).
    pub fn leader_gate(&self) -> impl Fn() -> bool + Send + Sync + 'static {
        let view = self.view.clone();
        move || view.borrow().is_me
    }

    /// Where the authoritative history JSONL lives on the shared volume.
    pub fn history_dir(&self) -> PathBuf {
        SharedLayout::new(&self.cfg.shared_dir, &self.cfg.node_name)
            .expect("layout exists")
            .history_dir()
    }

    /// The full node router: cluster endpoints (answered locally) + the
    /// native API and compat shim (proxied to the leader from non-leaders).
    pub fn router(&self, compat_version: &str, options: Vec<(String, String)>) -> Router {
        self.router_with_auth(compat_version, options, Default::default())
    }

    /// [`ClusterRuntime::router`] with HTTP auth on the API + compat
    /// surface. Cluster peer endpoints keep their own shared-secret auth
    /// and are never behind user credentials.
    pub fn router_with_auth(
        &self,
        compat_version: &str,
        options: Vec<(String, String)>,
        auth: nzbd_api::AuthConfig,
    ) -> Router {
        self.router_full(compat_version, options, auth, None, None, None)
    }

    /// Full router: auth + daemon log ring + watch-dir scan notify.
    #[allow(clippy::too_many_arguments)]
    pub fn router_full(
        &self,
        compat_version: &str,
        options: Vec<(String, String)>,
        auth: nzbd_api::AuthConfig,
        log: Option<Arc<nzbd_api::LogBuffer>>,
        scan_notify: Option<Arc<tokio::sync::Notify>>,
        feeds: Option<nzbd_feed::FeedsHandle>,
    ) -> Router {
        let history = self.pp.as_ref().map(|s| s.history.clone());
        let shared_clients = std::sync::Arc::new(nzbd_api::ClientRegistry::default());
        let compat_state = nzbd_compat::CompatState {
            config: Arc::new(nzbd_compat::CompatConfig {
                version: compat_version.to_string(),
            }),
            engine: self.engine.clone(),
            history: history.clone(),
            options: Arc::new(options),
            log: log.clone(),
            scan_notify,
            feeds,
            clients: Some(shared_clients.clone()),
        };
        let proxied = nzbd_api::require_auth(
            nzbd_api::router_with(nzbd_api::ApiState {
                engine: self.engine.clone(),
                torrent: None,
                history,
                log,
                setup: None, // cluster mode always has a config file
                clients: Some(shared_clients.clone()),
                shutdown: None,
                pp_stats: None,
                pp_manager: self.pp_manager.clone(),
                events: None, // router_with starts the hub
            })
            .merge(nzbd_compat::router(compat_state)),
            auth.clone(),
        )
        .layer(middleware::from_fn_with_state(
            self.leader_shared.clone(),
            commit_control_mutation,
        ))
        .layer(middleware::from_fn_with_state(
            ProxyState {
                node: self.cfg.node_name.clone(),
                view: self.view.clone(),
                client: self.client.clone(),
            },
            proxy_to_leader,
        ));

        let info = ClusterInfoState {
            snapshot: self.diagnostics.clone(),
        };
        let diagnostics = nzbd_api::require_auth(
            Router::new().route("/api/v1/cluster", get(cluster_info).with_state(info)),
            auth,
        );
        Router::new()
            .merge(leader::router(self.leader_shared.clone()))
            .merge(diagnostics)
            .merge(proxied)
    }

    /// Stop cluster tasks, then flush the engine.
    pub async fn shutdown(&self) {
        if let Some(worker) = &self.history_worker {
            worker.stop();
        }
        self.cancel.cancel();
        self.tracker.wait().await;
        self.engine.shutdown().await;
        if let Some(control) = &self.control {
            if let Err(error) = control.shutdown().await {
                tracing::warn!(%error, "replicated control shutdown did not complete cleanly");
            }
        }
    }
}

async fn commit_control_mutation(
    axum::extract::State(shared): axum::extract::State<Arc<LeaderShared>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let safe = matches!(
        *request.method(),
        axum::http::Method::GET | axum::http::Method::HEAD | axum::http::Method::OPTIONS
    );
    if safe || !shared.view.borrow().is_me {
        return next.run(request).await;
    }
    let _serial = shared.mutation_serial.lock().await;
    if !shared.authority_ready() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "queue authority is still being adopted; retry"
            })),
        )
            .into_response();
    }
    let healthy = match &shared.control {
        Some(control) => control.is_healthy().await,
        None => false,
    };
    if !healthy {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "replicated control quorum is unavailable; mutation not accepted"
            })),
        )
            .into_response();
    }
    let before = match shared.engine_projection().await {
        Ok(jobs) => jobs,
        Err(error) => {
            tracing::error!(%error, "could not capture queue projection before mutation");
            return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    };
    let response = next.run(request).await;
    if !response.status().is_success() {
        let _ = shared.restore_control_projection().await;
        return response;
    }
    if let Err(error) = shared.commit_mutation_delta(&before).await {
        tracing::error!(%error, "queue mutation was rolled back after replicated commit failed");
        let _ = shared.restore_control_projection().await;
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "replicated control did not commit the mutation; local projection rolled back"
            })),
        )
            .into_response();
    }
    response
}

async fn migrate_legacy_control(
    control: &ControlStore,
    layout: &SharedLayout,
    cfg: &ClusterConfig,
) -> Result<(), String> {
    use sha2::{Digest, Sha256};

    if let Some((cluster_id, fingerprint)) = control.migration_identity().await? {
        if cluster_id == cfg.cluster_id {
            tracing::debug!(%fingerprint, "legacy control migration already complete");
            return Ok(());
        }
        return Err(format!(
            "control store belongs to cluster {cluster_id}; refusing cluster {}",
            cfg.cluster_id
        ));
    }

    let queue_path = layout.state_dir().join("queue.json");
    let bytes = match std::fs::read(&queue_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(format!("read legacy queue for migration: {error}")),
    };
    let mut migration_hash = Sha256::new();
    migration_hash.update(b"queue\0");
    migration_hash.update(&bytes);
    migration_hash.update(b"history\0");
    hash_tree_if_present(
        &layout.history_dir(),
        &layout.history_dir(),
        &mut migration_hash,
    )?;
    let fingerprint = format!("sha256:{:x}", migration_hash.finalize());
    let snapshot = if bytes.is_empty() {
        nzbd_state::QueueSnapshotDoc::default()
    } else {
        serde_json::from_slice(&bytes)
            .map_err(|error| format!("decode legacy queue for migration: {error}"))?
    };

    let backup = cfg
        .control_dir
        .join("migration-backup")
        .join(fingerprint.trim_start_matches("sha256:"));
    std::fs::create_dir_all(&backup)
        .map_err(|error| format!("create migration backup: {error}"))?;
    if !bytes.is_empty() {
        let target = backup.join("queue.json");
        if !target.exists() {
            std::fs::copy(&queue_path, &target)
                .map_err(|error| format!("backup legacy queue: {error}"))?;
        }
    }
    copy_tree_if_present(&layout.history_dir(), &backup.join("history"))?;

    if control
        .migrate_legacy_snapshot(&cfg.cluster_id, &fingerprint, &snapshot)
        .await?
    {
        tracing::info!(%fingerprint, backup = %backup.display(), "legacy cluster control migrated");
    }
    Ok(())
}

fn copy_tree_if_present(source: &std::path::Path, target: &std::path::Path) -> Result<(), String> {
    if !source.exists() {
        return Ok(());
    }
    std::fs::create_dir_all(target)
        .map_err(|error| format!("create history backup {}: {error}", target.display()))?;
    for entry in std::fs::read_dir(source)
        .map_err(|error| format!("read history backup source {}: {error}", source.display()))?
    {
        let entry = entry.map_err(|error| format!("read history backup entry: {error}"))?;
        let from = entry.path();
        let to = target.join(entry.file_name());
        if entry
            .file_type()
            .map_err(|error| format!("read history backup file type: {error}"))?
            .is_dir()
        {
            copy_tree_if_present(&from, &to)?;
        } else if !to.exists() {
            std::fs::copy(&from, &to)
                .map_err(|error| format!("copy history backup {}: {error}", from.display()))?;
        }
    }
    Ok(())
}

fn hash_tree_if_present(
    root: &std::path::Path,
    path: &std::path::Path,
    hash: &mut sha2::Sha256,
) -> Result<(), String> {
    use sha2::Digest;
    if !path.exists() {
        return Ok(());
    }
    let mut entries = std::fs::read_dir(path)
        .map_err(|error| format!("read legacy history {}: {error}", path.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("read legacy history entry: {error}"))?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let entry_path = entry.path();
        let metadata = std::fs::symlink_metadata(&entry_path)
            .map_err(|error| format!("stat legacy history {}: {error}", entry_path.display()))?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "legacy history contains unsupported symlink {}",
                entry_path.display()
            ));
        }
        hash.update(
            entry_path
                .strip_prefix(root)
                .unwrap_or(&entry_path)
                .to_string_lossy()
                .as_bytes(),
        );
        hash.update([0]);
        if metadata.is_dir() {
            hash_tree_if_present(root, &entry_path, hash)?;
        } else {
            hash.update(std::fs::read(&entry_path).map_err(|error| {
                format!("read legacy history {}: {error}", entry_path.display())
            })?);
        }
    }
    Ok(())
}

#[derive(Clone)]
struct ClusterInfoState {
    snapshot: watch::Receiver<serde_json::Value>,
}

/// Local (unproxied) cluster diagnostics: this node's view of leadership
/// and membership.
async fn cluster_info(
    axum::extract::State(s): axum::extract::State<ClusterInfoState>,
) -> Json<serde_json::Value> {
    Json(s.snapshot.borrow().clone())
}
