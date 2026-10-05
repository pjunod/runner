//! Native REST API (`/api/v1`): status, job CRUD, queue controls,
//! history, SSE events, Prometheus `/metrics` and HTTP auth
//! (ARCHITECTURE.md §10.1). OpenAPI + roles are the remaining items.

mod artifacts;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use base64::Engine as _;

pub mod eventhub;
pub mod logbuf;
pub mod torrent_admission;
pub mod version;
pub use eventhub::{EventHub, Replay};
pub use logbuf::{LogBuffer, LogBufferLayer};
use nzbd_engine::{EngineHandle, JobSummary, QueueSnapshot};
use nzbd_state::history::HistoryDb;
use nzbd_types::metrics::PpStageStats;
use nzbd_types::{JobId, JobStatus};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tokio_stream::StreamExt as _;

/// Router state: the engine plus optional stores wired by the daemon.
#[derive(Clone)]
pub struct ApiState {
    pub engine: EngineHandle,
    /// Present only when the daemon started the single-node torrent session.
    pub torrent: Option<torrent_admission::TorrentAdmissionService>,
    pub history: Option<Arc<HistoryDb>>,
    pub log: Option<Arc<LogBuffer>>,
    /// First-run setup mode: present when the daemon booted with a
    /// `--config` path that doesn't exist yet. The UI offers a setup form;
    /// `POST /api/v1/setup` writes the file and asks the daemon to reload.
    pub setup: Option<Arc<SetupHandle>>,
    /// Observed compat-API consumers (shared with the compat shim).
    pub clients: Option<Arc<ClientRegistry>>,
    /// Flips to `true` when the daemon is shutting down / reloading.
    /// Long-lived SSE streams watch it and end promptly so axum's
    /// graceful shutdown can drain and the process can re-serve — without
    /// it, an open `/api/v1/events` stream blocks a restart forever.
    pub shutdown: Option<tokio::sync::watch::Receiver<bool>>,
    /// Post-processing stage timings, shared with the PP manager. `None`
    /// when the daemon runs without post-processing.
    pub pp_stats: Option<Arc<PpStageStats>>,
    /// Reliable recovery controls for live post-processing attempts. `None`
    /// when post-processing is disabled (or this node does not run it).
    pub pp_manager: Option<nzbd_post::manager::PostManagerHandle>,
    /// Seq-stamped event stream + replay ring. Filled in by `router_with`
    /// when the caller leaves it `None`, so there is exactly one hub per
    /// router and no code path where events are unnumbered.
    pub events: Option<Arc<EventHub>>,
}

/// Shared handle between the setup endpoint and the daemon's run loop.
pub struct SetupHandle {
    /// Where the config file lives / will be written. None = the daemon
    /// runs on pure defaults with no `--config` (settings become
    /// read-only in the UI).
    pub config_path: Option<std::path::PathBuf>,
    /// True when the daemon booted with a missing config file: the UI
    /// shows the first-run wizard instead of the app.
    pub setup_mode: bool,
    /// The effective listen address (recorded into the written config so
    /// a later bare `nzbd run --config …` binds the same way).
    pub bind: String,
    /// Signals the run loop to tear down and re-run with the new config.
    pub reload: tokio::sync::Notify,
    /// True once a config has been written (the run loop turns this into
    /// a reload instead of an exit).
    pub applied: std::sync::atomic::AtomicBool,
    /// Probed at boot: can the daemon actually create the config file?
    /// False in containers with read-only config mounts (ConfigMaps,
    /// `:ro` binds) — the UI then steers to copy-the-TOML-yourself.
    pub writable: bool,
    /// The currently-running configuration — what the settings editor
    /// shows (masked) and merges masked secrets from on save.
    pub current: std::sync::Mutex<nzbd_config::Config>,
    /// Sections saved to disk but not yet applied — the UI's "restart
    /// required" banner. Cleared by a restart (fresh handle).
    pub pending_restart: std::sync::Mutex<std::collections::BTreeSet<&'static str>>,
    /// Set when this boot found no config file and recovered one from the
    /// mirror on the data volume: the path it came from. The UI turns this
    /// into "your config directory isn't keeping anything — fix the mount".
    pub recovered_from: Option<std::path::PathBuf>,
}

impl SetupHandle {
    /// First-run setup mode: the config file doesn't exist yet.
    pub fn new(config_path: std::path::PathBuf, bind: String) -> Self {
        let writable = probe_writable(&config_path);
        SetupHandle {
            config_path: Some(config_path),
            setup_mode: true,
            bind,
            reload: tokio::sync::Notify::new(),
            applied: std::sync::atomic::AtomicBool::new(false),
            writable,
            current: std::sync::Mutex::new(nzbd_config::Config::default()),
            pending_restart: std::sync::Mutex::new(Default::default()),
            recovered_from: None,
        }
    }

    /// Normal running mode: powers the settings editor + hot reload.
    pub fn for_running(
        config_path: Option<std::path::PathBuf>,
        bind: String,
        current: nzbd_config::Config,
    ) -> Self {
        let writable = config_path.as_deref().map(probe_writable).unwrap_or(false);
        SetupHandle {
            config_path,
            setup_mode: false,
            bind,
            reload: tokio::sync::Notify::new(),
            applied: std::sync::atomic::AtomicBool::new(false),
            writable,
            current: std::sync::Mutex::new(current),
            pending_restart: std::sync::Mutex::new(Default::default()),
            recovered_from: None,
        }
    }

    /// Record that this boot ran on a config recovered from the mirror.
    pub fn recovered_from(mut self, from: Option<std::path::PathBuf>) -> Self {
        self.recovered_from = from;
        self
    }

    /// Keep the durable copy of the configuration in step with the file.
    ///
    /// Called after every successful write of the real config — first-run
    /// setup and the settings editor both — so the copy on the data volume
    /// is never a stale config from three edits ago. Best-effort: the
    /// operator's save already succeeded, and failing it now because the
    /// spare copy didn't land would be a worse bargain than the risk it
    /// insures against. Returns the mirror path when it was written.
    pub fn mirror_config(cfg: &nzbd_config::Config, toml_text: &str) -> Option<std::path::PathBuf> {
        let state_dir = cfg.state_dir();
        match nzbd_config::durable::save_mirror(&state_dir, toml_text) {
            Ok(p) => {
                tracing::info!(path = %p.display(), "saved a durable copy of the configuration");
                Some(p)
            }
            Err(e) => {
                tracing::warn!(
                    dir = %state_dir.display(),
                    error = %e,
                    "could not save the durable copy of the configuration — if the \
                     config directory is not a mounted volume, this config will not \
                     survive the container being recreated"
                );
                None
            }
        }
    }
}

#[cfg(test)]
type StorageTarget = nzbd_config::StorageRoot;

/// One filesystem used by one or more configured paths. `label` joins the
/// roles that depend on it, while `path` is their closest common ancestor.
/// Values are optional when no existing ancestor can be measured.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StoragePathDto {
    pub label: String,
    pub path: String,
    pub available_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    /// False means the current cycle could not measure this row. Non-None
    /// capacity is last-known evidence; None means it has never been known.
    pub current: bool,
}

#[cfg(test)]
fn measure_storage(targets: &[StorageTarget]) -> Vec<StoragePathDto> {
    let roots = targets
        .iter()
        .map(|target| nzbd_engine::volumes::DiskGuardRoot {
            label: target.label.clone(),
            path: target.path.clone(),
        })
        .collect::<Vec<_>>();
    nzbd_engine::volumes::storage_volume_readings(&roots)
        .into_iter()
        .map(storage_path_dto)
        .collect()
}

#[cfg(test)]
fn storage_path_dto(reading: nzbd_engine::volumes::StorageVolumeReading) -> StoragePathDto {
    StoragePathDto {
        label: reading.label,
        path: reading.path.to_string_lossy().into_owned(),
        available_bytes: reading.available_bytes,
        total_bytes: reading.total_bytes,
        current: reading.current,
    }
}

/// Try to create (and remove) a sibling probe file where the config will
/// go. Advisory only — the real write still reports its own error.
fn probe_writable(config_path: &std::path::Path) -> bool {
    let Some(parent) = config_path.parent() else {
        return false;
    };
    if std::fs::create_dir_all(parent).is_err() {
        return false;
    }
    let probe = parent.join(".nzbd-setup-probe");
    if std::fs::write(&probe, b"").is_err() {
        return false;
    }
    let _ = std::fs::remove_file(&probe);
    true
}

/// Observed API consumers. Answers "is Sonarr even talking to this
/// daemon?", and now "is monarr subscribed to events right now?", without
/// reading logs.
///
/// Shared between the compat shim and the native API: a consumer that
/// speaks `/api/v1` used to be completely invisible here, so a working
/// native client looked exactly like a broken one.
#[derive(Default)]
pub struct ClientRegistry {
    inner: std::sync::Mutex<std::collections::HashMap<String, ClientInfo>>,
    /// UA of the request currently being dispatched (advisory — used to
    /// stamp history observations without threading it everywhere).
    current: std::sync::Mutex<Option<String>>,
}

#[derive(Clone, Serialize)]
pub struct ClientInfo {
    pub user_agent: String,
    pub first_seen_unix: i64,
    pub last_seen_unix: i64,
    pub calls: u64,
    pub last_method: String,
    /// Which API this client last spoke: `compat` or `native`.
    pub api: &'static str,
    /// `/api/v1/events` streams this client holds open right now. Nonzero
    /// means push is genuinely attached — the fact an operator otherwise
    /// has to infer from the absence of complaints.
    pub event_subscriptions: u32,
}

impl ClientRegistry {
    pub fn note(&self, user_agent: Option<&str>, method: &str, now_unix: i64) {
        self.note_api(user_agent, method, now_unix, "compat");
    }

    /// A call on the native `/api/v1` surface. Kept distinct from `note`
    /// in one respect: it does not claim the `current()` slot, which the
    /// compat shim uses to attribute history observations to the request
    /// it is inside. Native handlers pass their client name explicitly.
    pub fn note_native(&self, client: Option<&str>, method: &str, now_unix: i64) {
        let mut m = self.inner.lock().unwrap();
        Self::touch(&mut m, client, method, now_unix, "native");
    }

    fn note_api(&self, user_agent: Option<&str>, method: &str, now_unix: i64, api: &'static str) {
        *self.current.lock().unwrap() = user_agent.map(String::from);
        let mut m = self.inner.lock().unwrap();
        Self::touch(&mut m, user_agent, method, now_unix, api);
    }

    fn touch<'a>(
        m: &'a mut std::collections::HashMap<String, ClientInfo>,
        name: Option<&str>,
        method: &str,
        now_unix: i64,
        api: &'static str,
    ) -> &'a mut ClientInfo {
        let ua = name.unwrap_or("unknown").to_string();
        let e = m.entry(ua.clone()).or_insert_with(|| ClientInfo {
            user_agent: ua,
            first_seen_unix: now_unix,
            last_seen_unix: now_unix,
            calls: 0,
            last_method: String::new(),
            api,
            event_subscriptions: 0,
        });
        e.last_seen_unix = now_unix;
        e.calls += 1;
        e.last_method = method.to_string();
        e.api = api;
        e
    }

    /// UA of the in-flight compat request (best effort).
    pub fn current(&self) -> Option<String> {
        self.current.lock().unwrap().clone()
    }

    /// Register one open event stream, released when the returned guard
    /// drops. A guard, not a pair of calls: the stream can end at any
    /// await point, and a count that only decrements on the tidy path
    /// would show phantom subscribers forever.
    pub fn subscribe(self: &Arc<Self>, client: Option<String>) -> Subscription {
        let now = unix_now();
        {
            let mut m = self.inner.lock().unwrap();
            let e = Self::touch(&mut m, client.as_deref(), "events", now, "native");
            e.event_subscriptions += 1;
        }
        Subscription {
            registry: self.clone(),
            key: client.unwrap_or_else(|| "unknown".into()),
        }
    }

    /// Live clients, most-recent first. Anything not heard from within
    /// `CLIENT_TTL_SECS` is dropped from the registry (a client that has
    /// gone quiet for 5 minutes isn't "connected" — and this also keeps
    /// the map from growing unbounded as clients cycle User-Agents).
    ///
    /// A client with an open event stream is exempt from the TTL: a push
    /// consumer legitimately makes no requests for hours, and pruning it
    /// would report "nothing connected" about the one connection that
    /// matters most.
    pub fn snapshot(&self, now_unix: i64) -> Vec<ClientInfo> {
        let mut m = self.inner.lock().unwrap();
        m.retain(|_, c| c.event_subscriptions > 0 || now_unix - c.last_seen_unix < CLIENT_TTL_SECS);
        let mut v: Vec<ClientInfo> = m.values().cloned().collect();
        drop(m);
        v.sort_by_key(|c| std::cmp::Reverse(c.last_seen_unix));
        v
    }
}

/// One open event stream's registration.
pub struct Subscription {
    registry: Arc<ClientRegistry>,
    key: String,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let mut m = self.registry.inner.lock().unwrap();
        if let Some(c) = m.get_mut(&self.key) {
            c.event_subscriptions = c.event_subscriptions.saturating_sub(1);
        }
    }
}

/// A compat client not seen in this many seconds is no longer considered
/// connected and is pruned from the registry.
const CLIENT_TTL_SECS: i64 = 300;

/// HTTP auth requirements (NZBGet `ControlUsername`/`ControlPassword`
/// parity plus a bearer token). Enforced only when a password or token is
/// configured; `/healthz` is always open.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    pub username: String,
    pub password: Option<String>,
    pub token: Option<String>,
}

impl AuthConfig {
    pub fn required(&self) -> bool {
        self.password.is_some() || self.token.is_some()
    }
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().min(b.len()).max(1) {
        let (x, y) = (
            a.get(i).copied().unwrap_or(0),
            b.get(i).copied().unwrap_or(0),
        );
        diff |= (x ^ y) as usize;
    }
    diff == 0
}

fn authorized(auth: &AuthConfig, header: Option<&str>) -> bool {
    if !auth.required() {
        return true;
    }
    let Some(header) = header else { return false };
    if let Some(token) = header.strip_prefix("Bearer ") {
        if let Some(want) = &auth.token {
            return constant_time_eq(token.trim(), want);
        }
        return false;
    }
    if let Some(b64) = header.strip_prefix("Basic ") {
        let Some(want_pw) = &auth.password else {
            return false;
        };
        let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(b64.trim()) else {
            return false;
        };
        let decoded = String::from_utf8_lossy(&decoded);
        let Some((user, pass)) = decoded.split_once(':') else {
            return false;
        };
        return constant_time_eq(user, &auth.username) & constant_time_eq(pass, want_pw);
    }
    false
}

/// Wrap a router with auth enforcement. `/healthz` stays open; everything
/// else answers 401 (with a Basic challenge, which NZBGet clients expect)
/// until credentials match.
pub fn require_auth(router: Router, auth: AuthConfig) -> Router {
    if !auth.required() {
        return router;
    }
    let auth = Arc::new(auth);
    router.layer(axum::middleware::from_fn(
        move |req: axum::extract::Request, next: axum::middleware::Next| {
            let auth = auth.clone();
            async move {
                if auth_exempt(req.uri().path()) {
                    return next.run(req).await;
                }
                let header = req
                    .headers()
                    .get(axum::http::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                if authorized(&auth, header.as_deref()) {
                    next.run(req).await
                } else {
                    (
                        StatusCode::UNAUTHORIZED,
                        [(
                            axum::http::header::WWW_AUTHENTICATE,
                            "Basic realm=\"nzbd\", charset=\"UTF-8\"",
                        )],
                        "unauthorized",
                    )
                        .into_response()
                }
            }
        },
    ))
}

#[derive(Debug, Serialize)]
pub struct StatusDto {
    /// The build identity the UI footer shows: the crate version, then
    /// what `git describe` said about the checkout — so it moves on every
    /// commit — or `+unknown` when the build had no way to know. See
    /// [`crate::version`]; a bare, never-changing "0.1.0" shipped a
    /// hundred commits under one number (field report 2026-07-27).
    pub version: &'static str,
    /// UTC compile stamp of the running binary — pins *when*, where the
    /// version pins *what*.
    pub built: &'static str,
    pub up_since_unix: i64,
    pub download_rate_bps: u64,
    pub remaining_bytes: u64,
    pub session_downloaded_bytes: u64,
    pub download_paused: bool,
    /// Queue-hold reasons (why nothing is downloading right now).
    pub disk_low: bool,
    /// Lowest measured free bytes across all configured write roots.
    pub disk_guard_free_bytes: Option<u64>,
    /// Operator-facing role of the root that currently limits intake.
    pub disk_guard_label: Option<String>,
    /// Configured path whose containing filesystem currently limits intake.
    pub disk_guard_path: Option<String>,
    /// True while an observed failed write, rather than only the forecast,
    /// is actively holding intake.
    pub disk_guard_write_latched: bool,
    /// True only when the enforcing cycle measured every configured root.
    pub disk_guard_all_roots_known: bool,
    /// Cumulative out-of-space errors observed on real writes since start.
    /// `disk_guard_write_latched` identifies the cause of the current hold;
    /// this historical count must not be used for that attribution.
    pub enospc_observed: u64,
    /// Operation and path of the last one.
    pub enospc_where: Option<String>,
    pub quota_reached: bool,
    pub blocked_servers: Vec<u32>,
    /// Critical-health abort armed (`[post] health_action` park/delete)?
    pub health_abort: bool,
    pub speed_limit_bps: Option<u64>,
    /// How many jobs may download at once (1..=100). The queue page's
    /// control reads back from here.
    pub max_active_downloads: u32,
    /// Jobs waiting for a turn: `Queued` and `Paused`.
    pub jobs_queued: u32,
    /// Jobs the daemon is actively working on — downloading, fetching a
    /// URL's NZB, waiting for a post slot, or in a post-processing stage.
    ///
    /// This deliberately counts more than `Downloading`. It used to count
    /// only that, so `PostQueued`, `Post { .. }` and `Fetching` fell into
    /// neither bucket and a job that spent twenty minutes repairing
    /// reported as `0 / 0` — the daemon claiming to be idle while grinding
    /// through a par set. A job in the queue is in exactly one of these
    /// three numbers now, which is the property that makes them worth
    /// showing at all.
    pub jobs_downloading: u32,
    /// Of those active jobs, how many are past the download and in the
    /// post-processing pipeline — including the ones queued for a post
    /// slot, matching what the compat layer already reports as
    /// `PostJobCount`. A subset of `jobs_downloading`, not a fourth
    /// bucket, so the three top-level counts still partition the queue.
    pub jobs_post: u32,
    pub jobs_finished: u32,
    /// Per-news-server wire rates and volumes. Same counters as
    /// `download_rate_bps`, so the chips the UI draws from these sum to the
    /// header tile — two numbers that claim to be the same thing must be
    /// the same measurement.
    pub servers: Vec<nzbd_engine::ServerVolume>,
    /// Cached capacity readings for the paths this process relies on.
    pub storage: Vec<StoragePathDto>,
}

pub fn status_dto(snap: &QueueSnapshot) -> StatusDto {
    status_dto_with_storage(snap, storage_from_snapshot(snap))
}

fn storage_from_snapshot(snap: &QueueSnapshot) -> Vec<StoragePathDto> {
    snap.storage_volumes
        .iter()
        .map(|volume| StoragePathDto {
            label: volume.label.clone(),
            path: volume.path.clone(),
            available_bytes: volume.available_bytes,
            total_bytes: volume.total_bytes,
            current: volume.current,
        })
        .collect()
}

fn status_dto_with_storage(snap: &QueueSnapshot, storage: Vec<StoragePathDto>) -> StatusDto {
    let count =
        |pred: &dyn Fn(&JobSummary) -> bool| snap.jobs.iter().filter(|j| pred(j)).count() as u32;
    StatusDto {
        version: version::full(),
        built: version::BUILT,
        up_since_unix: snap.up_since_unix,
        download_rate_bps: snap.download_rate_bps,
        remaining_bytes: snap.remaining_bytes,
        session_downloaded_bytes: snap.session_downloaded_bytes,
        download_paused: snap.download_paused,
        disk_low: snap.disk_low,
        disk_guard_free_bytes: snap.disk_guard_free_bytes,
        disk_guard_label: snap.disk_guard_label.clone(),
        disk_guard_path: snap.disk_guard_path.clone(),
        disk_guard_write_latched: snap.disk_guard_write_latched,
        disk_guard_all_roots_known: snap.disk_guard_all_roots_known,
        enospc_observed: snap.enospc_observed,
        enospc_where: snap.enospc_where.clone(),
        quota_reached: snap.quota_reached,
        blocked_servers: snap.blocked_servers.clone(),
        health_abort: snap.health_abort,
        speed_limit_bps: snap.speed_limit_bps,
        max_active_downloads: snap.max_active_downloads,
        jobs_queued: count(&|j| matches!(j.status, JobStatus::Queued | JobStatus::Paused)),
        jobs_downloading: count(&|j| {
            matches!(
                j.status,
                JobStatus::Downloading
                    | JobStatus::Fetching
                    | JobStatus::PostQueued
                    | JobStatus::Post { .. }
            )
        }),
        jobs_post: count(&|j| matches!(j.status, JobStatus::PostQueued | JobStatus::Post { .. })),
        jobs_finished: count(&|j| {
            matches!(
                j.status,
                JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
            )
        }),
        servers: snap.server_volumes.clone(),
        storage,
    }
}

async fn get_status(State(st): State<ApiState>) -> Json<StatusDto> {
    Json(status_dto(&st.engine.snapshot()))
}

async fn healthz() -> &'static str {
    "ok"
}

/// The embedded web UI (phase 4): one self-contained page, no build step.
async fn ui_index() -> Response {
    (
        [
            (axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8"),
            // Revalidate (not no-store): UI updates land on next load, and
            // the service worker still keeps an offline copy.
            (axum::http::header::CACHE_CONTROL, "no-cache"),
        ],
        include_str!("../ui/index.html"),
    )
        .into_response()
}

/// Default `Cache-Control: no-store` on every response that didn't set its
/// own policy — i.e. all the live JSON endpoints. Browsers (Safari most
/// aggressively) heuristically cache same-URL `fetch()` GETs that carry no
/// cache header, which froze the dashboard's 5 s poll on whatever the
/// first response said until a full page reload. Live data must never be
/// served from a browser cache; the PWA assets keep their explicit
/// long-lived headers.
async fn no_store_by_default(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let mut resp = next.run(req).await;
    if !resp
        .headers()
        .contains_key(axum::http::header::CACHE_CONTROL)
    {
        resp.headers_mut().insert(
            axum::http::header::CACHE_CONTROL,
            axum::http::HeaderValue::from_static("no-store"),
        );
    }
    resp
}

// PWA assets, all compiled into the binary. Cache header keeps phones
// from re-fetching icons on every open; the shell itself (/ and sw.js)
// stays revalidated so UI updates land immediately.
fn asset(ctype: &'static str, cache: bool, bytes: &'static [u8]) -> Response {
    let cache_control = if cache {
        "public, max-age=86400"
    } else {
        "no-cache"
    };
    (
        [
            (axum::http::header::CONTENT_TYPE, ctype),
            (axum::http::header::CACHE_CONTROL, cache_control),
        ],
        bytes,
    )
        .into_response()
}

async fn pwa_manifest() -> Response {
    asset(
        "application/manifest+json",
        false,
        include_bytes!("../ui/manifest.webmanifest"),
    )
}
async fn pwa_sw() -> Response {
    asset("text/javascript", false, include_bytes!("../ui/sw.js"))
}
async fn icon_192() -> Response {
    asset(
        "image/png",
        true,
        include_bytes!("../ui/icons/icon-192.png"),
    )
}
async fn icon_512() -> Response {
    asset(
        "image/png",
        true,
        include_bytes!("../ui/icons/icon-512.png"),
    )
}
async fn icon_maskable() -> Response {
    asset(
        "image/png",
        true,
        include_bytes!("../ui/icons/icon-maskable-512.png"),
    )
}
async fn apple_touch_icon() -> Response {
    asset(
        "image/png",
        true,
        include_bytes!("../ui/icons/apple-touch-icon.png"),
    )
}

/// Paths that must work without credentials: health for probes, and the
/// PWA identity assets — browsers fetch the manifest, icons and service
/// worker updates without sending Authorization, and a 401 there breaks
/// install/updates. They carry no user data.
fn auth_exempt(path: &str) -> bool {
    matches!(
        path,
        "/healthz" | "/manifest.webmanifest" | "/sw.js" | "/apple-touch-icon.png"
    ) || path.starts_with("/icons/")
}

async fn list_jobs(State(st): State<ApiState>) -> Response {
    let snap = st.engine.snapshot();
    Json(json!({ "jobs": snap.jobs })).into_response()
}

async fn get_job(State(st): State<ApiState>, Path(id): Path<u32>) -> Response {
    let snap = st.engine.snapshot();
    match snap.jobs.iter().find(|j| j.id == JobId(id)) {
        Some(job) if job.kind == nzbd_types::JobKind::Torrent => {
            match st.engine.export_job(JobId(id)).await {
                Ok(Some(record)) => {
                    let mut value = serde_json::to_value(job).unwrap_or_else(|_| json!({}));
                    if let Some(object) = value.as_object_mut() {
                        object.insert("torrent".into(), json!(record.torrent));
                    }
                    Json(value).into_response()
                }
                Ok(None) => not_found(),
                Err(err) => error(StatusCode::SERVICE_UNAVAILABLE, &err.to_string()),
            }
        }
        Some(job) => Json(job.clone()).into_response(),
        None => not_found(),
    }
}

/// `GET /api/v1/jobs/{id}/files` — per-file detail for the job panel:
/// name, size, segment progress, pause/par2 flags, assembled state.
async fn get_job_files(State(st): State<ApiState>, Path(id): Path<u32>) -> Response {
    use nzbd_types::SegmentState;
    match st.engine.export_job(JobId(id)).await {
        Ok(Some(job)) => {
            if let Some(torrent) = &job.torrent {
                let files = torrent
                    .files
                    .iter()
                    .enumerate()
                    .map(|(index, file)| {
                        json!({
                            "id": index,
                            "filename": file.path,
                            "size_bytes": file.length,
                            "downloaded_bytes": file.downloaded_bytes,
                            "selected": file.selected,
                            "verified": torrent.ready_at_unix.is_some() && file.selected,
                        })
                    })
                    .collect::<Vec<_>>();
                return Json(json!({ "job": job.id.0, "name": job.name, "files": files }))
                    .into_response();
            }
            let files: Vec<serde_json::Value> = job
                .files
                .iter()
                .map(|f| {
                    let mut done = 0u32;
                    let mut failed = 0u32;
                    let mut done_bytes = 0u64;
                    let size: u64 = f.segments.iter().map(|s| s.size as u64).sum();
                    for s in &f.segments {
                        match s.state {
                            SegmentState::Done { len, .. } => {
                                done += 1;
                                done_bytes += len as u64;
                            }
                            SegmentState::Failed => failed += 1,
                            _ => {}
                        }
                    }
                    json!({
                        "id": f.id.0,
                        "filename": f.filename,
                        "size_bytes": size,
                        "downloaded_bytes": done_bytes,
                        "total_segments": f.segments.len() as u32,
                        "done_segments": done,
                        "failed_segments": failed,
                        "paused": f.paused,
                        "is_par2": f.is_par2,
                        "assembled": f.finalized,
                    })
                })
                .collect();
            Json(json!({ "job": job.id.0, "name": job.name, "files": files })).into_response()
        }
        Ok(None) => not_found(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
}

async fn get_job_torrent(State(st): State<ApiState>, Path(id): Path<u32>) -> Response {
    match st.engine.export_job(JobId(id)).await {
        Ok(Some(job)) => match job.torrent {
            Some(torrent) => Json(json!({
                "job": id,
                "info_hash_v1": torrent.info_hash_v1,
                "phase": torrent.phase,
                "content_path": torrent.content_path,
                "selected_bytes": torrent.selected_bytes,
                "downloaded_bytes": torrent.downloaded_bytes,
                "uploaded_bytes": torrent.uploaded_bytes,
                "ratio": if torrent.selected_bytes == 0 { 0.0 } else { torrent.uploaded_bytes as f64 / torrent.selected_bytes as f64 },
                "seeding_seconds": torrent.seeding_seconds,
                "seed_ratio_limit": torrent.seed_policy.ratio_limit,
                "seed_time_limit_secs": torrent.seed_policy.time_limit_secs,
                "seed_policy": torrent.seed_policy,
                "stop_reason": torrent.stop_reason,
                "ready_at_unix": torrent.ready_at_unix,
                "last_activity_unix": torrent.last_activity_unix,
                "last_error": torrent.last_error,
            }))
            .into_response(),
            None => not_found(),
        },
        Ok(None) => not_found(),
        Err(_) => error(StatusCode::SERVICE_UNAVAILABLE, "queue owner unavailable"),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetSeedPolicyBody {
    #[serde(default)]
    use_defaults: bool,
    #[serde(default)]
    stop_on_complete: bool,
    ratio_limit: Option<f64>,
    time_limit_secs: Option<u64>,
}

async fn set_seed_policy(
    State(st): State<ApiState>,
    Path(id): Path<u32>,
    Json(body): Json<SetSeedPolicyBody>,
) -> Response {
    if body.ratio_limit.is_some_and(|v| !v.is_finite() || v <= 0.0)
        || body.time_limit_secs == Some(0)
    {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Limits must be positive; use null for unlimited",
        );
    }
    let job = match st.engine.export_job(JobId(id)).await {
        Ok(Some(job)) if job.torrent.is_some() => job,
        Ok(_) => return not_found(),
        Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "queue owner unavailable"),
    };
    let policy = if body.use_defaults {
        let Some(service) = &st.torrent else {
            return error(StatusCode::SERVICE_UNAVAILABLE, "BitTorrent is disabled");
        };
        service.seed_defaults(job.category.as_deref())
    } else {
        nzbd_types::SeedPolicy {
            stop_on_complete: body.stop_on_complete,
            ratio_limit: body.ratio_limit,
            time_limit_secs: body.time_limit_secs,
        }
    };
    match st.engine.set_torrent_seed_policy(JobId(id), policy).await {
        Ok(true) => Json(json!({"ok": true, "seed_policy": policy})).into_response(),
        Ok(false) => error(StatusCode::CONFLICT, "Could not save seeding policy"),
        Err(_) => error(StatusCode::SERVICE_UNAVAILABLE, "queue owner unavailable"),
    }
}

async fn get_job_torrent_file(State(st): State<ApiState>, Path(id): Path<u32>) -> Response {
    let Some(service) = &st.torrent else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "BitTorrent is disabled");
    };
    match service.export_metainfo(JobId(id)).await {
        Ok(Some(bytes)) => (
            [
                (axum::http::header::CONTENT_TYPE, "application/x-bittorrent"),
                (axum::http::header::CACHE_CONTROL, "no-store"),
            ],
            bytes,
        )
            .into_response(),
        Ok(None) => not_found(),
        Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "torrent export failed"),
    }
}

/// `GET /api/v1/jobs/{id}/nzb` — the job's NZB, regenerated from queue
/// state (subjects, groups, message-ids, sizes — everything the daemon
/// needed to download is everything an NZB contains). Downloadable from
/// the job panel; the original upload is not retained, so this IS the
/// canonical export.
async fn get_job_nzb(State(st): State<ApiState>, Path(id): Path<u32>) -> Response {
    match st.engine.export_job(JobId(id)).await {
        Ok(Some(job)) => {
            let xml = nzbd_engine::queue::job_to_nzb(&job);
            let fname = nzbd_engine::queue::sanitize_name(&job.name);
            (
                [
                    (
                        axum::http::header::CONTENT_TYPE,
                        "application/x-nzb; charset=utf-8".to_string(),
                    ),
                    (
                        axum::http::header::CONTENT_DISPOSITION,
                        format!("attachment; filename=\"{}.nzb\"", fname.replace('"', "_")),
                    ),
                ],
                xml,
            )
                .into_response()
        }
        Ok(None) => not_found(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct AddJobQuery {
    name: Option<String>,
    category: Option<String>,
    priority: Option<i32>,
    /// Fetch the NZB from this URL instead of the request body.
    url: Option<String>,
    paused: Option<bool>,
    seed_ratio_limit: Option<f64>,
    seed_time_limit_secs: Option<u64>,
    stop_seeding_on_complete: Option<bool>,
    dupe_key: Option<String>,
    dupe_score: Option<i32>,
    /// URL-encoded JSON object of string→string, set on the job at admit
    /// time: a consumer's own tracking id (Sonarr's `drone`, monarr's
    /// `monarr-transfer`). Params flow from the job into history rows and
    /// the compat `Parameters` array through the existing plumbing, so
    /// this is one write, not a second path.
    params: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SetJobPriorityBody {
    priority: i32,
}

/// `PUT /api/v1/jobs/{id}/priority` — change the scheduler priority for a
/// queued or active job. Higher numeric values are selected first; priority
/// 900 is the existing force band that can run through soft holds.
async fn set_job_priority(
    State(st): State<ApiState>,
    Path(id): Path<u32>,
    Json(body): Json<SetJobPriorityBody>,
) -> Response {
    match st.engine.set_priority(JobId(id), body.priority).await {
        Ok(true) => Json(json!({ "ok": true, "priority": body.priority })).into_response(),
        Ok(false) => not_found(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
}

/// Parse the `params` query field. `*`-prefixed keys are rejected rather
/// than accepted-and-ignored: that prefix is nzbd's internal namespace
/// (`*PP:done`, `*URL`, `*Unpack:Password`), and letting a client write
/// there would let it forge post-processing state. The error names the
/// offending key, because "invalid params" tells the operator nothing.
fn parse_add_params(raw: &str) -> Result<Vec<(String, String)>, String> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| format!("params must be a JSON object of strings: {e}"))?;
    let Some(obj) = value.as_object() else {
        return Err("params must be a JSON object of strings".into());
    };
    let mut out = Vec::with_capacity(obj.len());
    for (k, v) in obj {
        if k.starts_with('*') {
            return Err(format!(
                "param key {k:?} is reserved: keys starting with '*' are internal to nzbd"
            ));
        }
        let Some(v) = v.as_str() else {
            return Err(format!("param {k:?} must be a string"));
        };
        out.push((k.clone(), v.to_string()));
    }
    Ok(out)
}

/// `POST /api/v1/jobs` with the raw NZB document as the request body.
/// (Multipart and `{url}` forms arrive in phase 3.)
async fn add_job(
    State(st): State<ApiState>,
    Query(q): Query<AddJobQuery>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .unwrap_or("");
    if matches!(
        content_type,
        "application/x-bittorrent" | "application/json"
    ) {
        let Some(torrent) = &st.torrent else {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "BitTorrent is disabled in this daemon configuration",
            );
        };
        return torrent
            .handle_http_post_with_options(
                headers,
                body,
                nzbd_engine::AddOpts {
                    category: q.category,
                    priority: q.priority.unwrap_or(0),
                    paused: q.paused.unwrap_or(false),
                    seed_ratio_limit: q.seed_ratio_limit,
                    seed_time_limit_secs: q.seed_time_limit_secs,
                    stop_seeding_on_complete: q.stop_seeding_on_complete,
                    ..Default::default()
                },
            )
            .await;
    }
    let name = q.name.unwrap_or_default();
    let params = match q.params.as_deref().map(parse_add_params).transpose() {
        Ok(p) => p.unwrap_or_default(),
        Err(msg) => return error(StatusCode::UNPROCESSABLE_ENTITY, &msg),
    };
    let opts = nzbd_engine::AddOpts {
        category: q.category,
        priority: q.priority.unwrap_or(0),
        paused: q.paused.unwrap_or(false),
        dupe: q.dupe_key.map(|key| nzbd_types::DupeInfo {
            key,
            score: q.dupe_score.unwrap_or(0),
            mode: None,
        }),
        params,
        // Who asked. Only used when the job's own documents name it
        // nothing — see `queue::requestor_name`.
        client: consumer_name(&headers).or_else(|| client_name(&headers)),
        ..Default::default()
    };
    if let Some(url) = &q.url {
        return match st.engine.add_url(&name, url, opts).await {
            Ok(id) => (StatusCode::CREATED, Json(json!({ "id": id }))).into_response(),
            Err(e) => error(StatusCode::UNPROCESSABLE_ENTITY, &e.to_string()),
        };
    }
    if body.is_empty() {
        return error(
            StatusCode::BAD_REQUEST,
            "empty body; POST the NZB document (or pass ?url=)",
        );
    }
    match st.engine.add_nzb_opts(&name, &body, opts).await {
        Ok(id) => (StatusCode::CREATED, Json(json!({ "id": id }))).into_response(),
        Err(e) => error(StatusCode::UNPROCESSABLE_ENTITY, &e.to_string()),
    }
}

/// Everything needed to put a deleted job back, captured before the engine
/// forgets it. Either the regenerated NZB (a queued/downloading job) or the
/// source URL (a job still fetching its NZB, which has no articles yet).
struct Parked {
    entry: nzbd_state::HistoryEntry,
    nzb: Option<Vec<u8>>,
}

/// Snapshot a job for parking. Runs BEFORE the delete, while the queue
/// still has it; the caller only writes the record if the delete succeeded.
async fn park_snapshot(st: &ApiState, job: JobId) -> Option<Parked> {
    st.history.as_ref()?;
    let j = st.engine.export_job(job).await.ok().flatten()?;
    let url = j
        .params
        .iter()
        .find(|(k, _)| k == "*URL")
        .map(|(_, v)| v.clone());
    // A `Fetching` job has no articles to export — park its URL instead.
    let nzb = if j.files.is_empty() {
        None
    } else {
        Some(nzbd_engine::queue::job_to_nzb(&j).into_bytes())
    };
    if nzb.is_none() && url.is_none() {
        return None; // nothing to put back: don't promise an undo
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let mut params = j.params.clone();
    if nzb.is_some() {
        // The URL is only the requeue source when there is no NZB; keeping
        // both would make the choice ambiguous on the way back.
        params.retain(|(k, _)| k != "*URL");
    }
    Some(Parked {
        entry: nzbd_state::HistoryEntry {
            job,
            name: j.name.clone(),
            category: j.category.clone(),
            final_dir: None,
            status: "DELETED".into(),
            size: j.totals.size,
            health: nzbd_types::Health::calc(&j.totals).0,
            params,
            dupe_key: j.dupe.key.clone(),
            dupe_score: j.dupe.score,
            completed_at_unix: now,
            hidden: false,
            first_seen_at_unix: None,
            last_seen_at_unix: None,
            seen_count: 0,
            removed_at_unix: None,
            picked_up_by: None,
            record: Some(nzbd_state::JobRecord::from_job(&j)),
            stages: j.stages.clone(),
            seq: 0,
        },
        nzb,
    })
}

/// Write the parked record + spool. Called only after the engine confirmed
/// the delete, so history can never claim a job that is still queued.
async fn park_write(st: &ApiState, mut parked: Parked) -> bool {
    if let Ok(Some(a)) = st.engine.artifacts().for_job(parked.entry.job.0) {
        parked.entry.params.retain(|(k, _)| k != "Artifact:Id");
        parked
            .entry
            .params
            .push(("Artifact:Id".into(), a.id.clone()));
        if !a.terminal() {
            parked.entry.final_dir = Some(a.path.to_string_lossy().into_owned());
        }
    }
    let Some(db) = st.history.clone() else {
        return false;
    };
    let job = parked.entry.job;
    tokio::task::spawn_blocking(move || {
        if let Some(bytes) = &parked.nzb {
            if let Err(e) = db.spool_nzb(job, bytes) {
                tracing::warn!(job = job.0, error = %e, "could not park the deleted job's NZB");
                return false;
            }
        }
        match db.record(&parked.entry) {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!(job = job.0, error = %e, "could not write the DELETED history entry");
                db.drop_spool(job);
                false
            }
        }
    })
    .await
    .unwrap_or(false)
}

async fn job_action(
    State(st): State<ApiState>,
    Path((id, action)): Path<(u32, String)>,
) -> Response {
    let engine = &st.engine;
    let job = JobId(id);
    if let Some(raw) = action.strip_prefix("post-restart-") {
        use nzbd_post::manager::{RestartPoint as P, RestartPostError as E};
        let from = match raw {
            "all" => P::Beginning,
            "verify" => P::Verify,
            "unpack" => P::Unpack,
            "cleanup" => P::Cleanup,
            "move" => P::Move,
            "scripts" => P::Scripts,
            _ => return error(
                StatusCode::BAD_REQUEST,
                "unknown post-processing restart point (all|verify|unpack|cleanup|move|scripts)",
            ),
        };
        let Some(manager) = st.pp_manager.as_ref() else {
            return error(
                StatusCode::NOT_IMPLEMENTED,
                "post-processing recovery is not available on this node",
            );
        };
        return match manager.restart(job, from).await {
            Ok(()) => Json(json!({
                "ok": true,
                "restarting": from.as_str(),
            }))
            .into_response(),
            Err(E::NotFound) => not_found(),
            Err(e @ (E::AlreadyFinished | E::NotReady | E::UnsafePoint | E::NotOwned)) => {
                error(StatusCode::CONFLICT, &e.to_string())
            }
            Err(E::Closed) => error(StatusCode::SERVICE_UNAVAILABLE, &E::Closed.to_string()),
        };
    }
    // Delete parks. The job's regenerated NZB (or its source URL) is
    // captured first, the engine deletes exactly as it always did, and only
    // a CONFIRMED delete writes the `DELETED` history entry the UI offers
    // Undo on. Export-then-delete is a benign race: if the job finishes in
    // between, the delete answers Ok(false) -> 404 and the next tick
    // reconciles the UI. No locking — `delete_job`'s single-writer
    // semantics are not renegotiated by a UI feature.
    if action == "delete" || action == "delete-files" {
        let files = action == "delete-files";
        let snapshot = park_snapshot(&st, job).await;
        return match engine.delete_job(job, files).await {
            Ok(true) => {
                let mut parked = false;
                if let Some(s) = snapshot {
                    parked = park_write(&st, s).await;
                }
                Json(json!({ "ok": true, "parked": parked })).into_response()
            }
            Ok(false) => not_found(),
            Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
        };
    }
    let result = match action.as_str() {
        "pause" => engine.pause_job(job).await,
        "resume" => engine.resume_job(job).await,
        "move-top" => engine.move_job(job, nzbd_engine::MoveOp::Top).await,
        "move-up" => engine.move_job(job, nzbd_engine::MoveOp::Up).await,
        "move-down" => engine.move_job(job, nzbd_engine::MoveOp::Down).await,
        "move-bottom" => engine.move_job(job, nzbd_engine::MoveOp::Bottom).await,
        _ => {
            return error(
                StatusCode::BAD_REQUEST,
                "unknown action (pause|resume|delete|delete-files|move-top|move-up|move-down|move-bottom)",
            )
        }
        // delete / delete-files are handled above (they park first).
    };
    match result {
        Ok(true) => Json(json!({ "ok": true })).into_response(),
        Ok(false) => match engine.export_job(job).await {
            Ok(Some(record)) => {
                let message = record
                    .control()
                    .filter(|c| c.lifecycle == "held")
                    .map(|c| format!("job is held: {} ({})", c.message, c.cause))
                    .unwrap_or_else(|| {
                        format!("{action} is not available in the job's current state")
                    });
                error(StatusCode::CONFLICT, &message)
            }
            Ok(None) => not_found(),
            Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
        },
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
}

async fn queue_action(
    State(st): State<ApiState>,
    Path(action): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response {
    let engine = &st.engine;
    let source = client_source(&headers);
    let result = match action.as_str() {
        "pause" => engine.pause_all(&source).await,
        "resume" => engine.resume_all(&source).await,
        _ => return error(StatusCode::BAD_REQUEST, "unknown action (pause|resume)"),
    };
    match result {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
}

/// Attribution for state-changing calls: the UI names itself via
/// `X-Nzbd-Client`; anything else is identified by User-Agent (browsers
/// get compacted to their first token — "Mozilla/5.0" tells us enough).
fn client_source(headers: &axum::http::HeaderMap) -> String {
    if let Some(v) = headers.get("x-nzbd-client").and_then(|v| v.to_str().ok()) {
        if !v.trim().is_empty() {
            return v.trim().chars().take(60).collect();
        }
    }
    headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .map(|ua| {
            ua.split_whitespace()
                .next()
                .unwrap_or("unknown")
                .to_string()
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".into())
}

#[derive(Debug, Deserialize)]
struct SpeedLimitBody {
    bytes_per_sec: Option<u64>,
}

async fn set_speed_limit(State(st): State<ApiState>, Json(body): Json<SpeedLimitBody>) -> Response {
    match st.engine.set_speed_limit(body.bytes_per_sec).await {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct MaxActiveBody {
    n: u32,
}

/// `PUT /api/v1/queue/max-active-downloads` — how many jobs may download
/// at once, changed while the daemon runs.
///
/// Answers with the value actually applied rather than echoing the
/// request: it is clamped to 1..=100, and a caller that asked for 0 needs
/// to be told it got 1, not left to assume the queue is now stopped.
async fn set_max_active_downloads(
    State(st): State<ApiState>,
    Json(body): Json<MaxActiveBody>,
) -> Response {
    match st.engine.set_max_active_downloads(body.n).await {
        Ok(n) => Json(json!({ "ok": true, "max_active_downloads": n })).into_response(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
}

/// `POST /api/v1/servers/test` — live connectivity probe for a news
/// server, exactly as the form describes it (saved or not): connect,
/// greeting, optional AUTHINFO, through the production NNTP transport.
/// Backs the "test connection" buttons in Settings and the first-run
/// wizard. Always answers 200 with `{ok, message}` — an unreachable host
/// is a probe *result*, not an API error.
#[derive(Debug, Deserialize)]
struct TestServerBody {
    host: String,
    /// Defaults to the NNTP convention for the chosen transport:
    /// 563 with TLS, 119 without.
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    tls: bool,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    /// "strict" (default) | "minimal" | "none".
    #[serde(default)]
    cert_verification: Option<String>,
    /// Index into the saved `[[server]]` list. The settings form only ever
    /// holds the mask for a stored password — this lets the daemon swap in
    /// the real secret so "test" works without retyping it.
    #[serde(default)]
    server_index: Option<usize>,
}

async fn test_server(State(st): State<ApiState>, Json(body): Json<TestServerBody>) -> Response {
    let port = body.port.unwrap_or(if body.tls { 563 } else { 119 });
    let cert = match body.cert_verification.as_deref() {
        None | Some("") | Some("strict") => nzbd_types::CertLevel::Strict,
        Some("minimal") => nzbd_types::CertLevel::Minimal,
        Some("none") => nzbd_types::CertLevel::None,
        Some(other) => {
            return error(
                StatusCode::BAD_REQUEST,
                &format!("unknown cert_verification '{other}' (strict|minimal|none)"),
            )
        }
    };
    let mut password = body.password;
    if password.as_deref() == Some(nzbd_config::SECRET_MASK) {
        let stored = st.setup.as_ref().and_then(|s| {
            let cfg = s.current.lock().unwrap();
            body.server_index
                .and_then(|i| cfg.servers.get(i))
                .and_then(|srv| srv.password.clone())
        });
        match stored {
            Some(p) => password = Some(p),
            None => {
                return Json(json!({
                    "ok": false,
                    "message": "stored password not available here — retype it to test"
                }))
                .into_response()
            }
        }
    }
    let outcome = nzbd_nntp::transport::probe_server(
        &body.host,
        port,
        body.tls,
        cert,
        body.username.as_deref().filter(|u| !u.trim().is_empty()),
        password.as_deref(),
        std::time::Duration::from_secs(10),
    )
    .await;
    let (ok, message) = match outcome {
        Ok(m) => (true, m),
        Err(m) => (false, m),
    };
    Json(json!({ "ok": ok, "message": message })).into_response()
}

/// `GET /api/v1/events` — engine events as SSE (`event:` = variant name,
/// `data:` = JSON payload). Lagged consumers observe a `lagged` event and
/// should resync from `/api/v1/status`.
///
/// On top of the discrete engine events, a `tick` event carries the full
/// `{status, jobs}` read model at 1 Hz (skipped while nothing changes).
/// Progress bars, rates and ETAs have no discrete event — without the
/// tick, a dashboard only moves as fast as its poll fallback, which is
/// exactly the "page frozen until I hit refresh" field report. Header
/// stats and per-job rows come from the SAME snapshot, so they can never
/// disagree the way two separately-cached endpoints did.
async fn sse_events(State(st): State<ApiState>, headers: axum::http::HeaderMap) -> Response {
    use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
    use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};

    let hub = match st.events.clone() {
        Some(h) => h,
        None => return error(StatusCode::NOT_IMPLEMENTED, "event hub not configured"),
    };
    // Subscribe BEFORE resolving the replay window. The reverse order has
    // a hole exactly one event wide: anything published between reading
    // the ring and attaching to the fan-out is in neither. Overlapping the
    // two costs a duplicate, which the seq filter below drops.
    let live = hub.subscribe();
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(eventhub::EventId::parse);
    let replay = hub.replay(last_event_id);
    let boot = hub.boot();

    // Note the subscriber so `GET /api/v1/clients` can show that a push
    // consumer is actually attached — "is monarr subscribed right now" is
    // otherwise only answerable by reading logs.
    let subscriber = st
        .clients
        .as_ref()
        .map(|c| c.subscribe(client_name(&headers)));
    let guard = hub.stream_guard();

    let engine = st.engine.clone();
    let log = st.log.clone();
    let mut shutdown = st.shutdown.clone();
    // Forward engine events into the SSE body, but stop the instant the
    // daemon starts shutting down — an open `/api/v1/events` connection
    // must never hold graceful shutdown (and thus a restart) open.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<SseEvent, std::convert::Infallible>>(64);
    tokio::spawn(async move {
        // Both live for the lifetime of the stream: dropping them is what
        // decrements the gauge and the per-client subscription count.
        let _guard = guard;
        let _subscriber = subscriber;
        let events = BroadcastStream::new(live);
        tokio::pin!(events);
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_tick_payload: Option<String> = None;
        // Staleness clock for the `hb` event, and the log cursor. A new
        // stream tails the log from "now": replaying the whole ring on
        // every connect would flood a page that has not opened the Logs
        // tab, and the REST endpoint is the backfill for when it does.
        let mut last_frame = std::time::Instant::now();
        let mut last_log_id = log.as_ref().map(|l| l.newest_id()).unwrap_or(0);
        // Highest seq already written to this stream — the de-dupe against
        // the deliberate subscribe/replay overlap above.
        let mut sent_seq = 0u64;

        // ---- resume ------------------------------------------------------
        match replay {
            eventhub::Replay::Live => {}
            eventhub::Replay::Reset => {
                // The honest answer to "I missed some": say so, and name
                // why, so the client reconciles instead of assuming the
                // stream it is now reading is complete.
                let sse = SseEvent::default()
                    .event("reset")
                    .data(json!({"reason": "gap"}).to_string());
                if tx.send(Ok(sse)).await.is_err() {
                    return;
                }
                last_frame = std::time::Instant::now();
            }
            eventhub::Replay::Frames(frames) => {
                for f in frames {
                    let sse = SseEvent::default()
                        .id(eventhub::EventId { boot, seq: f.seq }.to_string())
                        .event(f.name)
                        .data(&*f.data);
                    if tx.send(Ok(sse)).await.is_err() {
                        return;
                    }
                    sent_seq = sent_seq.max(f.seq);
                }
                last_frame = std::time::Instant::now();
            }
        }

        loop {
            tokio::select! {
                _ = wait_shutdown(&mut shutdown) => break,
                _ = tick.tick() => {
                    // Log lines ride the same 1 Hz loop: a "tail -f" that
                    // updates once a second is the right cost/fidelity
                    // trade, and batching keeps per-connection state to a
                    // single cursor.
                    if let Some(buf) = &log {
                        let (entries, dropped) = buf.since_capped(last_log_id, LOG_BATCH_MAX);
                        if !entries.is_empty() {
                            last_log_id = entries.last().map(|r| r.id).unwrap_or(last_log_id);
                            let payload =
                                json!({ "entries": entries, "dropped": dropped }).to_string();
                            let sse = SseEvent::default().event("log").data(payload);
                            if tx.send(Ok(sse)).await.is_err() {
                                break;
                            }
                            last_frame = std::time::Instant::now();
                        }
                    }
                    let payload = tick_payload(&engine.snapshot());
                    if last_tick_payload.as_deref() != Some(payload.as_str()) {
                        let sse = SseEvent::default().event("tick").data(&payload);
                        if tx.send(Ok(sse)).await.is_err() {
                            break; // client hung up
                        }
                        last_tick_payload = Some(payload);
                        last_frame = std::time::Instant::now();
                    } else if last_frame.elapsed() >= HEARTBEAT_AFTER {
                        // The tick was suppressed as a duplicate — correct,
                        // an idle queue should cost nothing (battery and
                        // radio on phone PWAs). But that leaves a client
                        // unable to tell "nothing is happening" from "this
                        // stream died", which is exactly the state the old
                        // gray "polling" dot could not distinguish. `hb` is
                        // that distinction, and only that: it carries no
                        // read model. Axum's KeepAlive comment lines stay —
                        // proxies need them, but EventSource cannot see
                        // them by design.
                        let sse = SseEvent::default()
                            .event("hb")
                            .data(json!({ "now_unix": unix_now() }).to_string());
                        if tx.send(Ok(sse)).await.is_err() {
                            break;
                        }
                        last_frame = std::time::Instant::now();
                    }
                }
                next = events.next() => match next {
                    Some(Ok(frame)) => {
                        if frame.seq <= sent_seq {
                            continue; // already replayed
                        }
                        // `tick`/`hb`/`log` above carry no `id:` on purpose:
                        // they are stream-local views, not engine facts, and
                        // resuming from one would mean nothing.
                        let sse = SseEvent::default()
                            .id(eventhub::EventId { boot, seq: frame.seq }.to_string())
                            .event(frame.name)
                            .data(&*frame.data);
                        if tx.send(Ok(sse)).await.is_err() {
                            break; // client hung up
                        }
                        sent_seq = frame.seq;
                        last_frame = std::time::Instant::now();
                    }
                    Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                        // This stream fell behind the hub. The client's own
                        // `Last-Event-ID` will cover the hole on reconnect;
                        // until then it at least knows there is one.
                        let sse = SseEvent::default()
                            .event("lagged")
                            .data(json!({ "skipped": n }).to_string());
                        let _ = tx.send(Ok(sse)).await;
                        last_frame = std::time::Instant::now();
                    }
                    None => break, // hub closed (engine gone)
                },
            }
        }
    });
    Sse::new(ReceiverStream::new(rx))
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// How a caller identifies itself: `X-Nzbd-Client` when it sets one (the
/// header the UI already uses for pause attribution, and what the
/// integration contract asks consumers to send), else its User-Agent.
fn client_name(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get("x-nzbd-client")
        .or_else(|| headers.get(axum::http::header::USER_AGENT))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// The caller, but only when it is another *application* — not this daemon's
/// own web UI.
///
/// Used for the handoff signal, where that distinction is the whole point. A
/// history read means "a consumer has seen these entries", and an operator
/// opening the History tab must not satisfy it: if the browser counted, every
/// row would flip to `seen by Chrome` the moment you looked at the page, and
/// the column that tells you whether your *arr collected the files would be
/// answering a different question than the one it asks.
///
/// An explicit `X-Nzbd-Client` is always a consumer — a browser has no reason
/// to send it. Otherwise a User-Agent counts only if it is a product token
/// rather than a browser's `Mozilla/…` masquerade, which is the same rule the
/// UI uses to decide whether to prettify a chip label.
fn consumer_name(headers: &axum::http::HeaderMap) -> Option<String> {
    if headers
        .get("x-nzbd-role")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("operator"))
    {
        return None;
    }
    if let Some(explicit) = headers
        .get("x-nzbd-client")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        return Some(explicit);
    }
    headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && !s.starts_with("Mozilla/"))
}

/// How long a silent stream may stay silent before it emits `hb`. Shorter
/// than the client's 6.5 s staleness threshold, so an idle daemon never
/// looks disconnected.
const HEARTBEAT_AFTER: std::time::Duration = std::time::Duration::from_secs(5);
/// Log lines per `log` frame. A burst above this is reported as `dropped`
/// rather than paced out over minutes.
const LOG_BATCH_MAX: usize = 200;

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// The 1 Hz SSE `tick` body: header status + job rows from one snapshot.
fn tick_payload(snap: &QueueSnapshot) -> String {
    json!({ "status": status_dto(snap), "jobs": snap.jobs }).to_string()
}

/// Resolve when the daemon is shutting down; pend forever if no shutdown
/// signal was wired (e.g. bare test router, cluster proxy).
async fn wait_shutdown(rx: &mut Option<tokio::sync::watch::Receiver<bool>>) {
    match rx {
        Some(rx) => {
            let _ = rx.wait_for(|down| *down).await;
        }
        None => std::future::pending::<()>().await,
    }
}

/// One engine event as `(SSE event name, JSON body)`.
///
/// Names are spelled out per variant rather than derived from a catch-all,
/// because consumers key on them: the UI nudges its refresh on specific
/// names, and monarr's subscriber switches on `job_pp_finished`. A rename
/// here is a breaking change for both.
///
/// Returns a `Value`, not a string, so the event hub can stamp `"seq"`
/// into the body without re-parsing what we just serialized.
fn event_json(ev: &nzbd_engine::Event) -> (&'static str, serde_json::Value) {
    use nzbd_engine::Event as E;
    match ev {
        E::JobControlChanged { job, control } => (
            "job_control_changed",
            json!({"job": job.0, "control": control}),
        ),
        E::JobAdded { job, name } => ("job_added", json!({"job": job.0, "name": name})),
        E::JobFinished {
            job,
            name,
            status,
            health,
        } => (
            "job_finished",
            json!({"job": job.0, "name": name, "status": status, "health": health}),
        ),
        E::JobDeleted { job } => ("job_deleted", json!({"job": job.0})),
        E::UrlFetchResolved { job } => ("url_fetch_resolved", json!({"job": job.0})),
        E::FileFinished {
            job,
            file,
            filename,
            ok,
        } => (
            "file_finished",
            json!({"job": job.0, "file": file.0, "filename": filename, "ok": ok}),
        ),
        E::SegmentExhausted { job, file, segment } => (
            "segment_exhausted",
            json!({"job": job.0, "file": file.0, "segment": segment}),
        ),
        E::ServerBlocked { server, seconds } => (
            "server_blocked",
            json!({"server": server.0, "seconds": seconds}),
        ),
        E::QueuePauseChanged { paused, source } => (
            "queue_pause_changed",
            json!({"paused": paused, "source": source}),
        ),
        E::SpeedLimitChanged { bytes_per_sec } => (
            "speed_limit_changed",
            json!({"bytes_per_sec": bytes_per_sec}),
        ),
        E::MaxActiveDownloadsChanged { n } => ("max_active_downloads_changed", json!({"n": n})),
        E::JobAssigned { job, node } => ("job_assigned", json!({"job": job.0, "node": node})),
        E::JobPpStage { job, name, stage } => (
            "job_pp_stage",
            json!({"job": job.0, "name": name, "stage": stage.as_str()}),
        ),
        E::JobPpFinished {
            job,
            name,
            category,
            pp_status,
            final_dir,
            size_bytes,
            health,
            params,
            history_seq,
        } => (
            "job_pp_finished",
            json!({
                "job": job.0,
                "name": name,
                "category": category,
                "pp_status": pp_status,
                "final_dir": final_dir,
                "size_bytes": size_bytes,
                "health": health,
                "params": params,
                "history_seq": history_seq,
            }),
        ),
    }
}

/// `GET /metrics` — Prometheus text exposition from the queue snapshot.
async fn metrics(State(st): State<ApiState>) -> Response {
    let snap = st.engine.snapshot();
    let mut by_status: std::collections::BTreeMap<&'static str, u32> = Default::default();
    for j in snap.jobs.iter() {
        let k = match j.status {
            JobStatus::Queued => "queued",
            JobStatus::Downloading => "downloading",
            JobStatus::Paused => "paused",
            JobStatus::Fetching => "fetching",
            JobStatus::PostQueued | JobStatus::Post { .. } => "post_processing",
            JobStatus::Completed => "completed",
            JobStatus::Failed => "failed",
            JobStatus::Deleted => "deleted",
        };
        *by_status.entry(k).or_insert(0) += 1;
    }
    use std::fmt::Write;
    let mut out = String::with_capacity(1024);
    let inventory = st.engine.artifacts();
    if let Ok(Ok(lifecycle)) = tokio::task::spawn_blocking(move || inventory.metrics()).await {
        out.push_str(&lifecycle);
    }
    let m = &mut out;
    let _ = writeln!(m, "# TYPE nzbd_download_rate_bytes_per_second gauge");
    let _ = writeln!(
        m,
        "nzbd_download_rate_bytes_per_second {}",
        snap.download_rate_bps
    );
    let _ = writeln!(m, "# TYPE nzbd_remaining_bytes gauge");
    let _ = writeln!(m, "nzbd_remaining_bytes {}", snap.remaining_bytes);
    let _ = writeln!(m, "# TYPE nzbd_session_downloaded_bytes counter");
    let _ = writeln!(
        m,
        "nzbd_session_downloaded_bytes {}",
        snap.session_downloaded_bytes
    );
    let _ = writeln!(m, "# TYPE nzbd_download_paused gauge");
    let _ = writeln!(m, "nzbd_download_paused {}", snap.download_paused as u8);
    let _ = writeln!(m, "# TYPE nzbd_speed_limit_bytes_per_second gauge");
    let _ = writeln!(
        m,
        "nzbd_speed_limit_bytes_per_second {}",
        snap.speed_limit_bps.unwrap_or(0)
    );
    let _ = writeln!(m, "# TYPE nzbd_max_active_downloads gauge");
    let _ = writeln!(m, "nzbd_max_active_downloads {}", snap.max_active_downloads);
    let _ = writeln!(m, "# TYPE nzbd_jobs gauge");
    for (k, v) in by_status {
        let _ = writeln!(m, "nzbd_jobs{{status=\"{k}\"}} {v}");
    }
    let torrent_jobs = snap
        .jobs
        .iter()
        .filter(|job| job.kind == nzbd_types::JobKind::Torrent)
        .collect::<Vec<_>>();
    let _ = writeln!(m, "# TYPE nzbd_torrent_jobs gauge");
    let _ = writeln!(m, "nzbd_torrent_jobs {}", torrent_jobs.len());
    let _ = writeln!(m, "# TYPE nzbd_torrent_upload_rate_bytes_per_second gauge");
    let _ = writeln!(
        m,
        "nzbd_torrent_upload_rate_bytes_per_second {}",
        torrent_jobs
            .iter()
            .map(|job| job.upload_rate_bps)
            .sum::<u64>()
    );
    // This is the sum across jobs currently retained by the queue and can
    // decrease when history is removed, so it is intentionally a gauge.
    let _ = writeln!(m, "# TYPE nzbd_torrent_uploaded_bytes gauge");
    let _ = writeln!(
        m,
        "nzbd_torrent_uploaded_bytes {}",
        torrent_jobs
            .iter()
            .map(|job| job.uploaded_bytes)
            .sum::<u64>()
    );
    let _ = writeln!(m, "# TYPE nzbd_torrent_useful_peers gauge");
    let _ = writeln!(
        m,
        "nzbd_torrent_useful_peers {}",
        torrent_jobs
            .iter()
            .map(|job| u64::from(job.useful_peers))
            .sum::<u64>()
    );
    let _ = writeln!(m, "# TYPE nzbd_up_since_seconds gauge");
    let _ = writeln!(m, "nzbd_up_since_seconds {}", snap.up_since_unix);
    // Integration observability: is the event stream actually producing,
    // and is anything actually listening. A pipeline that silently stopped
    // pushing looks identical to an idle one until these two disagree.
    if let Some(hub) = &st.events {
        let _ = writeln!(m, "# TYPE nzbd_events_emitted_total counter");
        for (event, n) in hub.counts() {
            let _ = writeln!(m, "nzbd_events_emitted_total{{event=\"{event}\"}} {n}");
        }
        let _ = writeln!(m, "# TYPE nzbd_sse_clients gauge");
        let _ = writeln!(m, "nzbd_sse_clients {}", hub.sse_clients());
    }
    if let Some(stats) = &st.pp_stats {
        let rows = stats.snapshot();
        if !rows.is_empty() {
            let _ = writeln!(m, "# TYPE nzbd_pp_stage_seconds summary");
            for (stage, count, secs) in rows {
                let label = stage.as_str();
                let _ = writeln!(
                    m,
                    "nzbd_pp_stage_seconds_count{{stage=\"{label}\"}} {count}"
                );
                let _ = writeln!(m, "nzbd_pp_stage_seconds_sum{{stage=\"{label}\"}} {secs}");
            }
        }
    }
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        out,
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct LogsQuery {
    after: Option<u64>,
    limit: Option<usize>,
    /// Comma-separated scopes to include: `system`, `job`, `file`.
    /// Default: everything. The UI defaults to `system,job` — per-file
    /// lines are two orders of magnitude noisier and drown the rest.
    scope: Option<String>,
    /// Only lines about this job (its job- and file-scoped records) —
    /// powers the per-job activity tail in the job detail panel.
    job: Option<u32>,
}

/// `GET /api/v1/logs` — recent daemon log entries from the in-memory ring.
async fn get_logs(State(st): State<ApiState>, Query(q): Query<LogsQuery>) -> Response {
    let Some(buf) = &st.log else {
        return error(StatusCode::NOT_IMPLEMENTED, "log buffer not configured");
    };
    let limit = q.limit.unwrap_or(200).min(2000);
    let scopes: Option<Vec<logbuf::LogScope>> = q.scope.as_deref().map(|s| {
        s.split(',')
            .filter_map(|part| match part.trim() {
                "system" => Some(logbuf::LogScope::System),
                "job" => Some(logbuf::LogScope::Job),
                "file" => Some(logbuf::LogScope::File),
                _ => None,
            })
            .collect()
    });
    let entries = match q.after {
        // `after` paging is the compat loadlog path — unfiltered.
        Some(after) => buf.since(after, limit),
        None => buf.tail_filtered(limit, scopes.as_deref(), q.job),
    };
    Json(json!({ "entries": entries })).into_response()
}

#[derive(Debug, Deserialize)]
struct HistoryQuery {
    limit: Option<usize>,
    /// Page form: skip this many of the newest-first list. Ignored on the
    /// cursor path, which already has a position of its own.
    offset: Option<usize>,
    /// Cursor form: entries with `seq > since_seq`, oldest first. This is
    /// the catch-up path after a consumer was away, and the poll path's
    /// way to fetch only what is new. It exists because SSE is lossy by
    /// design — the cursor is what makes that loss harmless.
    since_seq: Option<i64>,
}

async fn get_history_sync(State(st): State<ApiState>) -> Response {
    match st.history {
        Some(db) => Json(json!(db.sync_status())).into_response(),
        None => error(StatusCode::NOT_IMPLEMENTED, "history store not configured"),
    }
}

async fn history_sync_action(State(st): State<ApiState>, Path(action): Path<String>) -> Response {
    let Some(db) = st.history else {
        return error(StatusCode::NOT_IMPLEMENTED, "history store not configured");
    };
    let paused = match action.as_str() {
        "pause" => true,
        "resume" => false,
        _ => return error(StatusCode::BAD_REQUEST, "expected pause or resume"),
    };
    match tokio::task::spawn_blocking(move || {
        db.set_sync_paused(paused)?;
        Ok::<_, nzbd_state::StateError>(db.sync_status())
    })
    .await
    {
        Ok(Ok(status)) => Json(json!(status)).into_response(),
        Ok(Err(e)) => error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// `GET /api/v1/history` — completed/failed jobs (NZBGet parity: finished
/// jobs leave the queue and live here).
///
/// Two shapes, kept separate rather than merged into one clever query:
/// `?limit=`/`?offset=` is newest-first for the UI, and `?since_seq=` is
/// oldest-first for a consumer walking forward (so the last row's `seq` is
/// its next cursor). Mixing the two orders in one response is how cursors
/// quietly start skipping rows. `offset` belongs only to the first shape —
/// a cursor already carries a position, and combining the two would give a
/// consumer two ways to say where it is and no rule for which wins.
///
/// The response carries `total` (matching rows, not rows in this page) so
/// the pager can say "1–20 of 179" without a second round trip. Every
/// shape still answers under `entries`, so nothing that reads this today
/// has to change.
///
/// The read is also the handoff signal. A consumer listing history is the
/// only evidence nzbd has that anyone came for the files, and until this was
/// wired the compat `history` RPC was the sole writer of `last_seen` — so a
/// *native* consumer polling every 30 s left every finished job reading
/// `⏳ awaiting pickup` forever, which inverts the meaning of the one column
/// that answers "did my *arr take these?". The browser is excluded; see
/// [`consumer_name`].
async fn get_history(
    State(st): State<ApiState>,
    headers: axum::http::HeaderMap,
    Query(q): Query<HistoryQuery>,
) -> Response {
    let Some(db) = &st.history else {
        return error(StatusCode::NOT_IMPLEMENTED, "history store not configured");
    };
    let db = db.clone();
    let limit = q.limit.unwrap_or(200).min(10_000);
    let offset = q.offset.unwrap_or(0);
    let since = q.since_seq;
    let consumer = consumer_name(&headers);
    let now = unix_now();
    let entries = tokio::task::spawn_blocking(move || {
        db.read_page(limit, offset, since).map(|(entries, total)| {
            // Record the pull before decorating: this poll SAW these
            // entries. Both shapes count — a `since_seq` catch-up walk is
            // still the consumer reading them. A failure here is not worth
            // failing the read over; the observation is diagnostic, the
            // response is the job.
            if consumer.is_some() {
                let jobs: Vec<crate::JobId> = entries.iter().map(|e| e.job).collect();
                if !jobs.is_empty() {
                    let _ = db.mark_seen(&jobs, consumer.as_deref(), now);
                }
            }
            // `can_requeue` is derived, not stored: it answers "is the
            // requeue source still on this node?", which only a look at the
            // spool (or the parked `*URL`) can honestly say.
            //
            // ONE directory listing for the page, not one `stat` per row:
            // the spool lives beside the state dir, which on a network
            // mount made this loop the bulk of a history read (250 ms for
            // 179 rows, nuc3 2026-07-29).
            let spooled = db.spooled_ids();
            let entries = entries
                .into_iter()
                .map(|e| {
                    let can =
                        spooled.contains(&e.job.0) || e.params.iter().any(|(k, _)| k == "*URL");
                    let mut v = serde_json::to_value(&e).unwrap_or_else(|_| json!({}));
                    if let Some(o) = v.as_object_mut() {
                        o.insert("can_requeue".into(), json!(can));
                        o.insert(
                            "kind".into(),
                            json!(e
                                .record
                                .as_ref()
                                .map(|record| record.kind)
                                .unwrap_or(nzbd_types::JobKind::Nzb)),
                        );
                    }
                    v
                })
                .collect::<Vec<_>>();
            (entries, total, db.sync_status())
        })
    })
    .await;
    match entries {
        Ok(Ok((entries, total, sync))) => Json(json!({
            "entries": entries,
            "total": total,
            "offset": offset,
            "limit": limit,
            "sync": sync,
        }))
        .into_response(),
        Ok(Err(e)) => error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// `GET /api/v1/openapi.json` — a compact machine-readable surface
/// summary (full schema docs are generated in a later pass).
async fn openapi() -> Response {
    Json(json!({
        "openapi": "3.0.3",
        "info": { "title": "nzbd native API", "version": env!("CARGO_PKG_VERSION") },
        "paths": {
            "/api/v1/status": { "get": { "summary": "Queue + rate + guard status" } },
            "/api/v1/jobs": {
                "get": { "summary": "List queue jobs" },
                "post": { "summary": "Add a job (NZB body, or ?url=)",
                          "parameters": [
                              {"name": "name", "in": "query"},
                              {"name": "category", "in": "query"},
                              {"name": "priority", "in": "query"},
                              {"name": "url", "in": "query"},
                              {"name": "paused", "in": "query"},
                              {"name": "dupe_key", "in": "query"},
                              {"name": "dupe_score", "in": "query"},
                              {"name": "params", "in": "query",
                               "description": "URL-encoded JSON object of string→string job params; '*' keys rejected"}
                          ] }
            },
            "/api/v1/jobs/{id}": { "get": { "summary": "Job detail" } },
            "/api/v1/jobs/{id}/priority": { "put": { "summary": "Set scheduler priority; higher values run first and 900 is force" } },
            "/api/v1/jobs/{id}/torrent/seed-policy": { "put": { "summary": "Set stop_on_complete, positive ratio_limit/time_limit_secs (null = unlimited), or use_defaults; never starts a stopped torrent" } },
            "/api/v1/jobs/{id}/actions/{action}": { "post": { "summary": "pause|resume|delete|delete-files|move-*|post-restart-*; delete answers {ok, parked}" } },
            "/api/v1/queue/actions/{action}": { "post": { "summary": "pause|resume" } },
            "/api/v1/queue/speed-limit": { "put": { "summary": "Set speed limit (bytes_per_sec)" } },
            "/api/v1/servers/test": { "post": { "summary": "Live news-server connectivity probe (connect + greeting + AUTHINFO)" } },
            "/api/v1/jobs/{id}/files": { "get": { "summary": "Per-file detail (segments done/failed, sizes, paused, par2)" } },
            "/api/v1/jobs/{id}/nzb": { "get": { "summary": "Download the job's NZB (regenerated from queue state)" } },
            "/api/v1/history": { "get": { "summary": "Finished and deleted jobs (entries carry can_requeue and seq; response carries total/offset/limit)",
                                          "parameters": [
                                              {"name": "limit", "in": "query"},
                                              {"name": "offset", "in": "query",
                                               "description": "Page: skip N of the newest-first list; ignored with since_seq"},
                                              {"name": "since_seq", "in": "query",
                                               "description": "Cursor: entries with seq > N, ascending"}
                                          ] } },
            "/api/v1/history-sync": { "get": { "summary": "History worker status, freshness, I/O and index placement" } },
            "/api/v1/history-sync/{action}": { "post": { "summary": "Pause or resume background reconciliation; local writes continue" } },
            "/api/v1/history/{id}/actions/{action}": { "post": { "summary": "restore|hide|delete|delete-files|requeue" } },
            "/api/v1/logs": { "get": { "summary": "Recent daemon log entries" } },
            "/api/v1/events": { "get": { "summary": "Engine events (SSE); frames carry id: <seq>, Last-Event-ID resumes, 'reset' means poll-reconcile" } },
            "/metrics": { "get": { "summary": "Prometheus exposition" } },
            "/healthz": { "get": { "summary": "Liveness" } }
        }
    }))
    .into_response()
}

fn not_found() -> Response {
    error(StatusCode::NOT_FOUND, "no such job")
}

fn error(code: StatusCode, msg: &str) -> Response {
    (code, Json(json!({ "error": msg }))).into_response()
}

pub fn router(engine: EngineHandle) -> Router {
    router_with(ApiState {
        engine,
        torrent: None,
        history: None,
        log: None,
        setup: None,
        clients: None,
        shutdown: None,
        pp_stats: None,
        pp_manager: None,
        events: None,
    })
}

// ---------------------------------------------------------------------------
// First-run setup
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct SetupServerReq {
    #[serde(default)]
    name: Option<String>,
    host: String,
    #[serde(default = "default_nntp_port")]
    port: u16,
    #[serde(default = "default_true")]
    tls: bool,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    connections: Option<u16>,
}

fn default_nntp_port() -> u16 {
    563
}
fn default_true() -> bool {
    true
}

#[derive(serde::Deserialize)]
struct SetupReq {
    main_dir: String,
    dest_dir: String,
    server: SetupServerReq,
    #[serde(default)]
    api_password: Option<String>,
    /// Validate and render the TOML without writing or reloading — the UI's
    /// "show config" button, and the manual fallback for deployments where
    /// the config path isn't writable (read-only mounts, ConfigMaps).
    #[serde(default)]
    preview: bool,
}

async fn get_setup(State(st): State<ApiState>) -> Response {
    match &st.setup {
        Some(s) => Json(json!({
            "setup_mode": s.setup_mode && !s.applied.load(std::sync::atomic::Ordering::Relaxed),
            "config_path": s.config_path.as_ref().map(|p| p.display().to_string()),
            "writable": s.writable,
            // "Writable" was never the whole question. A config directory
            // on the container's own layer is writable and still throws
            // every save away the next time the container is recreated —
            // the failure that kept sending working installs back to this
            // wizard. false = ephemeral, true = persists, null = unknown.
            "durable": match s.config_path.as_deref().map(nzbd_config::durable::durability) {
                Some(nzbd_config::durable::Durability::Ephemeral) => Some(false),
                Some(nzbd_config::durable::Durability::Persistent) => Some(true),
                _ => None,
            },
            // Set when this boot recovered the config from the data volume
            // because the config file was gone.
            "recovered_from": s.recovered_from.as_ref().map(|p| p.display().to_string()),
            // Where the durable copy lives, so the UI can name it — but
            // ONLY once there is a real config. In setup mode `current` is
            // still Config::default(), so this would name a state dir
            // derived from a main_dir the operator has not chosen yet: a
            // confident, wrong path. Null means "not until you save".
            "mirror_path": (!s.setup_mode).then(|| {
                nzbd_config::durable::mirror_path(&s.current.lock().unwrap().state_dir())
                    .display()
                    .to_string()
            }),
            // Lets the UI explain that config_path is a *container* path
            // and how to find its host side (docker inspect / docker cp).
            "container": in_container(),
            // Docker sets the container's hostname to its own ID, so the
            // UI can print `docker container inspect <id>` commands that
            // work verbatim on the host — no guessing the container name.
            "container_id": hostname(),
        }))
        .into_response(),
        None => Json(json!({ "setup_mode": false })).into_response(),
    }
}

/// Best-effort "are we in a container?" — advisory, for setup-UI wording
/// only. One implementation, shared with the durability probe that has to
/// agree with it: two copies of this answer drifting apart is exactly how
/// you get a UI that says "persistent" while the daemon logs "ephemeral".
use nzbd_config::durable::in_container;

fn hostname() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .or_else(|_| std::fs::read_to_string("/etc/hostname"))
        .map(|s| s.trim().to_string())
        .ok()
        .filter(|s| !s.is_empty())
}

async fn post_setup(State(st): State<ApiState>, Json(req): Json<SetupReq>) -> Response {
    let Some(setup) = &st.setup else {
        return error(StatusCode::NOT_FOUND, "not in setup mode");
    };
    if !setup.setup_mode {
        return error(StatusCode::NOT_FOUND, "not in setup mode");
    }
    if setup.applied.load(std::sync::atomic::Ordering::Relaxed) {
        return error(StatusCode::CONFLICT, "setup already applied; reloading");
    }
    let Some(cfg_path) = setup.config_path.clone() else {
        return error(
            StatusCode::CONFLICT,
            "no config path (started without --config)",
        );
    };
    if req.main_dir.trim().is_empty()
        || req.dest_dir.trim().is_empty()
        || req.server.host.is_empty()
    {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "main_dir, dest_dir and server.host are required",
        );
    }

    let mut cfg = nzbd_config::Config::default();
    cfg.paths.main_dir = req.main_dir.trim().into();
    cfg.paths.dest_dir = req.dest_dir.trim().into();
    cfg.api.bind = setup.bind.clone();
    cfg.api.password = req
        .api_password
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(String::from);
    cfg.servers.push(nzbd_config::ServerConfig {
        name: req
            .server
            .name
            .clone()
            .filter(|n| !n.trim().is_empty())
            .unwrap_or_else(|| "primary".into()),
        host: req.server.host.clone(),
        port: req.server.port,
        tls: req.server.tls,
        username: req.server.username.clone().filter(|s| !s.is_empty()),
        password: req.server.password.clone().filter(|s| !s.is_empty()),
        connections: req.server.connections.unwrap_or(8).max(1),
        ..nzbd_config::ServerConfig::default()
    });

    let toml_text = match nzbd_config::to_toml(&cfg) {
        Ok(t) => t,
        Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    // Round-trip through the strict parser so we never write a config the
    // next boot would refuse.
    if let Err(e) = nzbd_config::Config::from_toml(&toml_text) {
        return error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
    }
    let path = cfg_path.display().to_string();
    if req.preview {
        return Json(json!({ "preview": true, "path": path, "toml": toml_text })).into_response();
    }
    // A failed write is common in containers (read-only mount, ConfigMap,
    // unmounted volume path owned by root). Hand the rendered TOML back so
    // the UI can offer copy-it-yourself instead of eating the form.
    let write_failed = |op: String| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": op,
                "path": path,
                "toml": toml_text,
                "hint": "config location not writable — copy this TOML into the file yourself (place it on the volume mounted at the path above), then restart nzbd",
            })),
        )
            .into_response()
    };
    if let Some(parent) = cfg_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            return write_failed(format!("create {}: {e}", parent.display()));
        }
    }
    if let Err(e) = std::fs::write(&cfg_path, &toml_text) {
        return write_failed(format!("write {}: {e}", cfg_path.display()));
    }
    // Before anything else: the copy that makes this survive a container
    // recreate even if /etc/nzbd turns out not to be mounted.
    let mirrored = SetupHandle::mirror_config(&cfg, &toml_text);
    tracing::info!(path = %cfg_path.display(), "setup: configuration written; reloading");
    setup
        .applied
        .store(true, std::sync::atomic::Ordering::Relaxed);
    setup.reload.notify_one();
    Json(json!({
        "written": path,
        "reloading": true,
        "toml": toml_text,
        "mirrored": mirrored.map(|p| p.display().to_string()),
    }))
    .into_response()
}

// ---------------------------------------------------------------------------
// Settings editor (Settings tab): the live config as TOML, secrets masked
// ---------------------------------------------------------------------------

async fn get_config(State(st): State<ApiState>) -> Response {
    let Some(h) = &st.setup else {
        return error(StatusCode::NOT_FOUND, "no config handle");
    };
    let masked = {
        let cur = h.current.lock().unwrap();
        nzbd_config::mask_secrets(&cur)
    };
    // Config omits a default torrent section to keep existing TOML files
    // unchanged. The form needs the complete section (including defaults)
    // to render valid values and collect edits when first enabling it.
    let mut config = json!(masked);
    config["torrent"] = json!(masked.torrent);
    let pending: Vec<&str> = h.pending_restart.lock().unwrap().iter().copied().collect();
    match nzbd_config::to_toml(&masked) {
        Ok(toml) => Json(json!({
            "path": h.config_path.as_ref().map(|p| p.display().to_string()),
            "writable": h.writable,
            "toml": toml,
            "config": config,
            "mask": nzbd_config::SECRET_MASK,
            "pending_restart": pending,
        }))
        .into_response(),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// PUT the config (JSON from the settings form, or raw TOML from the
/// advanced editor): validate strictly, restore masked secrets, write
/// the file, live-apply what a running daemon can absorb (speed limit),
/// and report which sections need a restart. Does NOT restart by itself
/// — that's `POST /api/v1/restart`.
async fn put_config(
    State(st): State<ApiState>,
    headers: axum::http::HeaderMap,
    body: String,
) -> Response {
    let Some(h) = &st.setup else {
        return error(StatusCode::NOT_FOUND, "no config handle");
    };
    let Some(cfg_path) = h.config_path.clone() else {
        return error(
            StatusCode::CONFLICT,
            "daemon is running without --config; there is no file to edit",
        );
    };
    let is_json = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.contains("json"))
        || body.trim_start().starts_with('{');
    let mut new_cfg = if is_json {
        match serde_json::from_str::<nzbd_config::Config>(&body) {
            Ok(c) => c,
            Err(e) => return error(StatusCode::UNPROCESSABLE_ENTITY, &e.to_string()),
        }
    } else {
        // Unvalidated on purpose: what the editor sends still has
        // SECRET_MASK where the secrets go, and validation rejects the
        // mask. Merge first, validate immediately after — see below.
        match nzbd_config::Config::parse_toml_unvalidated(&body) {
            Ok(c) => c,
            Err(e) => return error(StatusCode::UNPROCESSABLE_ENTITY, &e.to_string()),
        }
    };
    let old_cfg = { h.current.lock().unwrap().clone() };
    // A mask we cannot resolve is a password we would be DELETING. It used
    // to become None and the save reported success (field report
    // 2026-07-26). Refuse, and name the field the operator has to retype.
    let unresolved = nzbd_config::merge_masked_secrets(&mut new_cfg, &old_cfg);
    if !unresolved.is_empty() {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!(
                "{} still reads {} and there is no saved value to restore it from \
                 (a renamed or newly added server has no previous secret). Type the \
                 real value in before saving — saving as-is would delete it.",
                unresolved.join(", "),
                nzbd_config::SECRET_MASK,
            ),
        );
    }
    // Now that the secrets are real, hold the whole thing to the strict
    // validator — the TOML path skipped it above.
    if let Err(e) = new_cfg.validate() {
        return error(StatusCode::UNPROCESSABLE_ENTITY, &e.to_string());
    }
    let (live, restart) = nzbd_config::diff_sections(&old_cfg, &new_cfg);
    let toml_text = match nzbd_config::to_toml(&new_cfg) {
        Ok(t) => t,
        Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    if let Some(parent) = cfg_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("create {}: {e}", parent.display()),
            );
        }
    }
    if let Err(e) = std::fs::write(&cfg_path, &toml_text) {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!(
                "write {}: {e} — config location not writable; copy your edit into the file by hand",
                cfg_path.display()
            ),
        );
    }
    // The file is on disk; keep the durable copy in step with it.
    let mirrored = SetupHandle::mirror_config(&new_cfg, &toml_text);
    // Live-apply what the engine can absorb without a restart.
    if live.contains(&"speed limit") {
        let bps = new_cfg.queue.speed_limit_kib.map(|k| k * 1024);
        let _ = st.engine.set_speed_limit(bps).await;
    }
    if live.contains(&"max active downloads") {
        let _ = st
            .engine
            .set_max_active_downloads(new_cfg.queue.max_active_downloads)
            .await;
    }
    if live.contains(&"torrent upload limit") {
        let bps = (new_cfg.torrent.upload_limit_kib > 0)
            .then_some(new_cfg.torrent.upload_limit_kib * 1024);
        let _ = st.engine.set_torrent_upload_limit(bps).await;
    }
    // Connection counts: applied live, but only down to the number of
    // sockets that exist. Asking for more than a server spawned at boot
    // writes to the file and takes effect on the next start — and says
    // so, rather than leaving the operator with a saved setting and no
    // change in behavior.
    let mut conn_capped: Vec<String> = Vec::new();
    if live.contains(&"connections") {
        let caps: std::collections::HashMap<nzbd_types::ServerId, u16> = new_cfg
            .servers
            .iter()
            .enumerate()
            .map(|(i, s)| (nzbd_types::ServerId(i as u32), s.connections))
            .collect();
        if let Ok(applied) = st.engine.set_server_connection_caps(caps).await {
            for (i, s) in new_cfg.servers.iter().enumerate() {
                let got = applied.get(&nzbd_types::ServerId(i as u32)).copied();
                if got.is_some_and(|g| g < s.connections) {
                    conn_capped.push(format!(
                        "{}: using {} of {} until restart",
                        if s.name.is_empty() { &s.host } else { &s.name },
                        got.unwrap_or(0),
                        s.connections
                    ));
                }
            }
        }
    }
    *h.current.lock().unwrap() = new_cfg;
    let pending: Vec<&str> = {
        let mut p = h.pending_restart.lock().unwrap();
        p.extend(restart.iter().copied());
        p.iter().copied().collect()
    };
    tracing::info!(
        path = %cfg_path.display(),
        live = live.join(","),
        pending = pending.join(","),
        "settings saved"
    );
    Json(json!({
        "ok": true,
        "applied_live": live,
        "restart_required": pending,
        "connection_notes": conn_capped,
        "mirrored": mirrored.map(|p| p.display().to_string()),
    }))
    .into_response()
}

/// Restart the daemon: tear down and re-run with the config on disk.
/// The listener bounces for a moment; the UI polls /healthz.
async fn post_restart(State(st): State<ApiState>) -> Response {
    let Some(h) = &st.setup else {
        return error(StatusCode::NOT_FOUND, "no config handle");
    };
    tracing::info!("restart requested from the settings UI");
    h.applied.store(true, std::sync::atomic::Ordering::Relaxed);
    h.reload.notify_one();
    Json(json!({ "ok": true, "restarting": true })).into_response()
}

/// Observed compat clients (Sonarr/Radarr polling us) — the UI's
/// "connected clients" strip.
async fn get_clients(State(st): State<ApiState>) -> Response {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let list = st
        .clients
        .as_ref()
        .map(|c| c.snapshot(now))
        .unwrap_or_default();
    Json(json!({ "clients": list })).into_response()
}

/// Put a parked (`DELETED`) entry back in the queue — the server half of
/// the delete-with-Undo the UI offers. The requeue source is the spooled
/// NZB, or the `*URL` param when the job was still fetching when it was
/// deleted. On success the history entry and its spool are removed: the
/// job is queued again, so a `DELETED` record for it would be a lie.
async fn history_requeue(st: &ApiState, db: Arc<HistoryDb>, job: JobId) -> Response {
    let lookup = db.clone();
    let found = tokio::task::spawn_blocking(move || {
        let entry = lookup
            .list_filtered(10_000, true)
            .ok()?
            .into_iter()
            .find(|e| e.job == job)?;
        let nzb = lookup.read_spool(job);
        Some((entry, nzb))
    })
    .await;
    let Ok(Some((entry, nzb))) = found else {
        return not_found();
    };
    let url = entry
        .params
        .iter()
        .find(|(k, _)| k == "*URL")
        .map(|(_, v)| v.clone());
    let opts = nzbd_engine::AddOpts {
        category: entry.category.clone(),
        priority: 0,
        client: None, // a requeue re-uses the name the entry already has
        dupe: (!entry.dupe_key.is_empty()).then(|| nzbd_types::DupeInfo {
            key: entry.dupe_key.clone(),
            score: entry.dupe_score,
            mode: None,
        }),
        paused: false,
        // A requeue is the same download again, so it carries the same
        // consumer params — the tracking id that names this transfer must
        // survive an undo, or the trace it belongs to ends mid-story.
        params: entry
            .params
            .iter()
            .filter(|(k, _)| !k.starts_with('*'))
            .cloned()
            .collect(),
        ..Default::default()
    };
    let added = match (&nzb, &url) {
        (Some(bytes), _) => st.engine.add_nzb_opts(&entry.name, bytes, opts).await,
        (None, Some(u)) => st.engine.add_url(&entry.name, u, opts).await,
        (None, None) => {
            return error(
                StatusCode::NOT_FOUND,
                "no parked NZB for this entry — nothing to requeue",
            )
        }
    };
    match added {
        Ok(new_id) => {
            // `delete` drops the spool with the record.
            let _ = tokio::task::spawn_blocking(move || db.delete(job)).await;
            tracing::info!(from = job.0, to = new_id.0, "history entry requeued");
            Json(json!({ "id": new_id.0 })).into_response()
        }
        Err(e) => error(StatusCode::UNPROCESSABLE_ENTITY, &e.to_string()),
    }
}

/// Manual handoff controls. `restore` un-hides an entry so a connected
/// *arr sees it again on its next poll and re-imports; `hide` does the
/// reverse; `delete` removes the record; `delete-files` also removes the
/// job's final directory from disk; `requeue` puts a parked delete back.
async fn history_action(
    State(st): State<ApiState>,
    Path((id, action)): Path<(u32, String)>,
    headers: axum::http::HeaderMap,
) -> Response {
    let Some(db) = st.history.clone() else {
        return error(StatusCode::NOT_IMPLEMENTED, "history store not configured");
    };
    let job = JobId(id);
    if action == "requeue" {
        return history_requeue(&st, db, job).await;
    }
    if action == "delete-files" {
        let lookup = db.clone();
        let entry = match tokio::task::spawn_blocking(move || lookup.get(job)).await {
            Ok(Ok(Some(entry))) => entry,
            Ok(Ok(None)) => {
                let inventory = st.engine.artifacts();
                if let Ok(Some(a)) = inventory.for_job(id) {
                    if let Ok(op) = inventory.operation(&format!("history-delete-{}", a.id)) {
                        if op.state == "succeeded" {
                            return Json(json!({"ok":true,"files_removed":true})).into_response();
                        }
                    }
                }
                return not_found();
            }
            _ => return error(StatusCode::INTERNAL_SERVER_ERROR, "history lookup failed"),
        };
        if let Some(torrent) = entry
            .record
            .as_ref()
            .and_then(|record| record.torrent.as_ref())
        {
            if torrent.payload == nzbd_types::TorrentPayloadDisposition::Retained {
                return error(
                    StatusCode::CONFLICT,
                    "retained torrent payloads must be removed from the live torrent workflow, where the owned file inventory can be verified",
                );
            }
            // The backend already confirmed deletion before history was
            // written. Remove only the record; never recurse through a
            // persisted torrent path after the live ownership proof is gone.
            let deleted = tokio::task::spawn_blocking(move || db.delete(job)).await;
            return match deleted {
                Ok(Ok(true)) => Json(json!({ "ok": true, "files_removed": false })).into_response(),
                Ok(Ok(false)) => not_found(),
                _ => error(StatusCode::INTERNAL_SERVER_ERROR, "history store error"),
            };
        }
        let inventory = st.engine.artifacts();
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), tokio::task::spawn_blocking(move || {
            let artifact_id = entry.params.iter().find(|(k,_)| k == "Artifact:Id")
                .map(|(_,v)|v).ok_or_else(||nzbd_state::artifacts::Error::Conflict("legacy History has no allocation identity; inspect and delete it through Files".into()))?;
            let artifact = inventory.get(artifact_id)?;
            if artifact.job!=Some(job.0) {return Err(nzbd_state::artifacts::Error::Conflict("History allocation belongs to another job".into()));}
            if artifact.state=="deleted" { db.delete(job).map_err(|e|nzbd_state::artifacts::Error::Conflict(e.to_string()))?; return Ok(true); }
            if entry.final_dir.as_deref().map(std::path::Path::new) != Some(artifact.path.as_path()) {
                return Err(nzbd_state::artifacts::Error::Conflict("History path does not match the owned generation".into()));
            }
            let key = format!("history-delete-{}", artifact.id);
            let op = match inventory.operation(&key) {
                Ok(op) => op,
                Err(nzbd_state::artifacts::Error::NotFound) => inventory.request_delete(&artifact.id, artifact.revision, &key, 0)?,
                Err(e) => return Err(e),
            };
            let op = inventory.execute_delete(&op.id)?;
            if op.state != "succeeded" { return Err(nzbd_state::artifacts::Error::Conflict(format!("operation {} {}: {}", op.id, op.state, op.error.unwrap_or_default()))); }
            db.delete(job).map_err(|e| nzbd_state::artifacts::Error::Conflict(e.to_string()))?;
            Ok(true)
        })).await;
        return match result {
            Ok(Ok(Ok(_))) => Json(json!({"ok":true,"files_removed":true})).into_response(),
            Ok(Ok(Err(e))) => error(StatusCode::CONFLICT, &e.to_string()),
            _ => error(
                StatusCode::SERVICE_UNAVAILABLE,
                "file deletion pending; History retained; retry the same request",
            ),
        };
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let act = action.clone();
    // Hiding an entry right after import IS the "imported" signal, so it
    // must name who did it — a native consumer hiding via `/api/v1` gets
    // the same `picked_up_by` attribution the compat `HistoryDelete` path
    // has always written. Without this, a native monarr's imports look
    // like a human clicked them away.
    let by = client_name(&headers).unwrap_or_else(|| "user".into());
    let result = tokio::task::spawn_blocking(move || match act.as_str() {
        "restore" => db.restore(job).map(|ok| (ok, None::<bool>)),
        "hide" => db.hide(job, Some(&by), now).map(|ok| (ok, None)),
        "delete" => db.delete(job).map(|ok| (ok, None)),
        _ => Ok((false, None)),
    })
    .await;
    match result {
        Ok(Ok((true, files))) => {
            Json(json!({ "ok": true, "files_removed": files })).into_response()
        }
        Ok(Ok((false, _))) => match action.as_str() {
            "restore" | "hide" | "delete" | "delete-files" => not_found(),
            _ => error(
                StatusCode::BAD_REQUEST,
                "unknown action (restore|hide|delete|delete-files|requeue)",
            ),
        },
        Ok(Err(e)) => error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        Err(_) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "history operation interrupted; retry required",
        ),
    }
}

/// Build the native API router.
///
/// Must be called from within a Tokio runtime: the event hub's pump task
/// starts here so that events are numbered from the daemon's first moment,
/// not from whenever the first SSE client happens to connect. Every caller
/// already builds its router inside one.
pub fn router_with(state: ApiState) -> Router {
    if let Some(setup) = &state.setup {
        let roots = setup
            .current
            .lock()
            .unwrap()
            .storage_roots()
            .into_iter()
            .map(|r| r.path)
            .collect::<Vec<_>>();
        if let Err(error) = state.engine.artifacts().protect_roots(&roots) {
            tracing::error!(%error,"could not persist protected directory roles");
        }
        let cfg = setup.current.lock().unwrap().clone();
        let inventory = state.engine.artifacts();
        if let Err(error) =
            inventory.configure_scan(artifacts::scan_request(&cfg, &inventory, Vec::new()))
        {
            tracing::error!(%error,"could not persist discovery scopes");
        }
    }
    let state = ApiState {
        events: state
            .events
            .clone()
            .or_else(|| Some(EventHub::spawn(&state.engine))),
        ..state
    };
    let clients = state.clients.clone();
    let upload_body_limit = state.torrent.as_ref().map_or(2 * 1024 * 1024, |torrent| {
        torrent.max_request_body_bytes().saturating_add(64 * 1024)
    });
    Router::new()
        .merge(artifacts::router())
        .route("/api/v1/status", get(get_status))
        .route(
            "/api/v1/jobs",
            get(list_jobs)
                .post(add_job)
                .layer(axum::extract::DefaultBodyLimit::max(upload_body_limit)),
        )
        .route("/api/v1/jobs/{id}", get(get_job))
        .route("/api/v1/jobs/{id}/priority", put(set_job_priority))
        .route("/api/v1/jobs/{id}/files", get(get_job_files))
        .route("/api/v1/jobs/{id}/torrent", get(get_job_torrent))
        .route(
            "/api/v1/jobs/{id}/torrent/seed-policy",
            put(set_seed_policy),
        )
        .route("/api/v1/jobs/{id}/torrent-file", get(get_job_torrent_file))
        .route("/api/v1/jobs/{id}/nzb", get(get_job_nzb))
        .route("/api/v1/jobs/{id}/actions/{action}", post(job_action))
        .route("/api/v1/queue/actions/{action}", post(queue_action))
        .route("/api/v1/queue/speed-limit", put(set_speed_limit))
        .route(
            "/api/v1/queue/max-active-downloads",
            put(set_max_active_downloads),
        )
        .route("/api/v1/servers/test", post(test_server))
        .route("/api/v1/history", get(get_history))
        .route("/api/v1/history-sync", get(get_history_sync))
        .route("/api/v1/history-sync/{action}", post(history_sync_action))
        .route(
            "/api/v1/history/{id}/actions/{action}",
            post(history_action),
        )
        .route("/api/v1/clients", get(get_clients))
        .route("/api/v1/events", get(sse_events))
        .route("/api/v1/logs", get(get_logs))
        .route("/metrics", get(metrics))
        .route("/api/v1/openapi.json", get(openapi))
        .route("/api/v1/setup", get(get_setup).post(post_setup))
        .route("/api/v1/config", get(get_config).put(put_config))
        .route("/api/v1/restart", post(post_restart))
        .route("/healthz", get(healthz))
        .route("/", get(ui_index))
        .route("/manifest.webmanifest", get(pwa_manifest))
        .route("/sw.js", get(pwa_sw))
        .route("/icons/icon-192.png", get(icon_192))
        .route("/icons/icon-512.png", get(icon_512))
        .route("/icons/icon-maskable-512.png", get(icon_maskable))
        .route("/apple-touch-icon.png", get(apple_touch_icon))
        .layer(axum::middleware::from_fn(no_store_by_default))
        // Attribution for the native surface. The compat shim has noted
        // its callers for a long time; a consumer speaking `/api/v1` was
        // invisible, which made a perfectly healthy native client look
        // exactly like nothing being connected at all.
        .layer(axum::middleware::from_fn(
            move |req: axum::extract::Request, next: axum::middleware::Next| {
                let clients = clients.clone();
                async move {
                    if let Some(reg) = &clients {
                        let path = req.uri().path();
                        if path.starts_with("/api/v1") && !auth_exempt(path) {
                            reg.note_native(
                                client_name(req.headers()).as_deref(),
                                path,
                                unix_now(),
                            );
                        }
                    }
                    next.run(req).await
                }
            },
        ))
        .with_state(state)
}

/// Re-exported so the daemon can hand the same snapshot to the compat shim.
pub fn snapshot(engine: &EngineHandle) -> Arc<QueueSnapshot> {
    engine.snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use nzbd_engine::{Engine, EngineConfig, Tuning};
    use tower::util::ServiceExt;

    async fn test_engine(tmp: &tempfile::TempDir) -> EngineHandle {
        Engine::spawn(EngineConfig::single_node(
            vec![], // no connections; queue logic only
            tmp.path().join("state"),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap()
    }

    const NZB: &str = r#"<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
<file poster="p" date="1720000000" subject="&quot;f.bin&quot; yEnc (1/1)">
<groups><group>a.b</group></groups>
<segments><segment bytes="1000" number="1">m1@x</segment></segments>
</file></nzb>"#;

    #[tokio::test]
    async fn disabled_router_rejects_torrent_admission_without_side_effects() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        let response = router(engine.clone())
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/jobs")
                    .header("content-type", "application/x-bittorrent")
                    .body(axum::body::Body::from("d4:infode"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(engine.snapshot().jobs.is_empty());
        let store =
            nzbd_state::torrent_sources::PendingSourceStore::open(&tmp.path().join("state"))
                .unwrap();
        assert!(store.inventory().unwrap().is_empty());
        engine.shutdown().await;
    }

    fn summary(id: u32, status: JobStatus) -> JobSummary {
        JobSummary {
            control: None,
            id: nzbd_types::JobId(id),
            kind: nzbd_types::JobKind::Nzb,
            name: format!("job {id}"),
            status,
            category: None,
            priority: 0,
            size_bytes: 0,
            downloaded_bytes: 0,
            failed_bytes: 0,
            remaining_bytes: 0,
            total_articles: 0,
            done_articles: 0,
            failed_articles: 0,
            files_total: 0,
            files_done: 0,
            health: 1000,
            critical_health: 850,
            critical_health_estimated: true,
            rate_bps: 0,
            retried_articles: 0,
            assigned_node: None,
            pp_done: false,
            ready: false,
            ready_at_unix: None,
            uploaded_bytes: 0,
            upload_rate_bps: 0,
            ratio: 0.0,
            seeding_seconds: 0,
            useful_peers: 0,
            torrent_phase: None,
            torrent_control_intent: None,
            seed_policy: None,
            seed_stop_reason: None,
            torrent_error: None,
            dupe_key: String::new(),
            dupe_score: 0,
            params: vec![],
            stages: vec![],
        }
    }

    #[test]
    fn neutral_transfer_fields_are_additive_and_keep_status_vocabulary() {
        let value = serde_json::to_value(summary(7, JobStatus::Downloading)).unwrap();
        assert_eq!(value["kind"], "nzb");
        assert_eq!(value["ready"], false);
        assert!(value["ready_at_unix"].is_null());
        assert_eq!(value["status"], "downloading");
        assert_eq!(value["id"], 7);
        assert_eq!(value["name"], "job 7");
    }

    /// Every job in the queue lands in exactly one of the three counts.
    ///
    /// It did not used to: `jobs_downloading` counted only `Downloading`,
    /// so a job in any post-processing stage — or fetching a URL's NZB —
    /// fell into neither bucket, and the header tile read `0 / 0` while
    /// the daemon ground through a par repair. The partition IS the
    /// feature; a status tile that can silently omit a job is worse than
    /// no tile at all.
    #[test]
    fn status_counts_partition_the_queue() {
        let snap = QueueSnapshot {
            jobs: vec![
                summary(1, JobStatus::Queued),
                summary(2, JobStatus::Paused),
                summary(3, JobStatus::Downloading),
                summary(4, JobStatus::Fetching),
                summary(5, JobStatus::PostQueued),
                summary(
                    6,
                    JobStatus::Post {
                        stage: nzbd_types::PostStage::ParRepair,
                    },
                ),
                summary(
                    7,
                    JobStatus::Post {
                        stage: nzbd_types::PostStage::Unpack,
                    },
                ),
                summary(8, JobStatus::Completed),
                summary(9, JobStatus::Failed),
                summary(10, JobStatus::Deleted),
            ],
            ..Default::default()
        };
        let dto = status_dto(&snap);
        assert_eq!(dto.jobs_queued, 2, "queued + paused");
        assert_eq!(
            dto.jobs_downloading, 5,
            "downloading + fetching + post_queued + 2 post stages"
        );
        assert_eq!(
            dto.jobs_post, 3,
            "post_queued counts as post-processing, as it does in compat"
        );
        assert_eq!(dto.jobs_finished, 3);
        assert_eq!(
            dto.jobs_queued + dto.jobs_downloading + dto.jobs_finished,
            snap.jobs.len() as u32,
            "no job may fall between the buckets"
        );
    }

    /// A job repairing on its own is visible work, not an idle daemon.
    #[test]
    fn a_lone_repairing_job_is_not_reported_as_idle() {
        let snap = QueueSnapshot {
            jobs: vec![summary(
                1,
                JobStatus::Post {
                    stage: nzbd_types::PostStage::ParRepair,
                },
            )],
            ..Default::default()
        };
        let dto = status_dto(&snap);
        assert_eq!((dto.jobs_downloading, dto.jobs_queued), (1, 0));
    }

    #[test]
    fn status_publishes_the_limiting_disk_guard_root() {
        let snap = QueueSnapshot {
            disk_low: true,
            disk_guard_free_bytes: Some(42),
            disk_guard_label: Some("category: tv".into()),
            disk_guard_path: Some("/library/tv".into()),
            disk_guard_write_latched: true,
            ..Default::default()
        };
        let dto = status_dto(&snap);
        assert!(dto.disk_low);
        assert_eq!(dto.disk_guard_free_bytes, Some(42));
        assert_eq!(dto.disk_guard_label.as_deref(), Some("category: tv"));
        assert_eq!(dto.disk_guard_path.as_deref(), Some("/library/tv"));
        assert!(dto.disk_guard_write_latched);
    }

    #[test]
    fn storage_probe_covers_configured_paths_and_missing_destinations() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = nzbd_config::Config::default();
        cfg.paths.main_dir = tmp.path().join("working");
        cfg.paths.queue_dir = Some(tmp.path().join("state"));
        cfg.paths.dest_dir = tmp.path().join("downloads");
        cfg.paths.temp_dir = Some(tmp.path().join("scratch"));
        cfg.post.failed_dir = Some(tmp.path().join("failed"));
        cfg.categories.push(nzbd_config::CategoryConfig {
            name: "tv".into(),
            dest_dir: Some(tmp.path().join("library/tv")),
            ..Default::default()
        });
        std::fs::create_dir_all(tmp.path().join("state")).unwrap();

        let targets = cfg.storage_roots();
        let labels: Vec<_> = targets.iter().map(|t| t.label.as_str()).collect();
        assert_eq!(
            labels,
            [
                "state",
                "downloads",
                "working",
                "failed",
                "temporary",
                "category: tv"
            ]
        );
        let measured = measure_storage(&targets);
        #[cfg(unix)]
        {
            assert_eq!(measured.len(), 1, "paths on one volume share one monitor");
            assert_eq!(
                measured[0].label,
                "state · downloads · working · failed · temporary · category: tv"
            );
            assert_eq!(measured[0].path, tmp.path().to_string_lossy());
        }
        #[cfg(windows)]
        assert_eq!(
            measured.len(),
            targets.len(),
            "Windows conservatively keeps distinct configured path rows"
        );
        assert!(measured.iter().all(|p| p.total_bytes.unwrap_or(0) > 0));
        assert!(measured
            .iter()
            .all(|p| { p.available_bytes.unwrap_or(u64::MAX) <= p.total_bytes.unwrap_or(0) }));
    }

    /// The endpoint answers with the value it applied, not the one it was
    /// handed. Someone who asks for 0 has to learn they got 1 — otherwise
    /// they walk away believing they just stopped the queue.
    #[tokio::test]
    async fn the_concurrency_endpoint_reports_what_it_applied() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        let app = router(engine.clone());

        for (asked, want) in [(0u32, 1u32), (1, 1), (5, 5), (100, 100), (100_000, 100)] {
            let resp = app
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .method("PUT")
                        .uri("/api/v1/queue/max-active-downloads")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(format!("{{\"n\":{asked}}}")))
                        .unwrap(),
                )
                .await
                .unwrap();
            let v = body_json(resp).await;
            assert_eq!(
                v["max_active_downloads"], want,
                "asked for {asked}, should have been told {want}"
            );
        }

        // And the value is readable back from status, which is what the
        // queue page's box reads on every tick.
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/status")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(body_json(resp).await["max_active_downloads"], 100);
        engine.shutdown().await;
    }

    async fn body_json(resp: axum::response::Response) -> serde_json::Value {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn resume_reports_a_hold_as_conflict_and_missing_job_as_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        let id = engine
            .add_nzb("held", NZB.as_bytes(), None, 0)
            .await
            .unwrap();
        engine
            .hold_job(
                id,
                "identity_conflict",
                "download_write",
                "conflicting yEnc declared size",
            )
            .await
            .unwrap();
        let app = router(engine.clone());
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::post(format!("/api/v1/jobs/{}/actions/resume", id.0))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = body_json(response).await;
        assert!(body["error"]
            .as_str()
            .unwrap()
            .contains("conflicting yEnc declared size"));
        assert!(engine.export_job(id).await.unwrap().unwrap().held());
        let response = app
            .oneshot(
                axum::http::Request::post("/api/v1/jobs/999999/actions/resume")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        engine.shutdown().await;
    }

    /// With no history store there is nowhere to park, so delete says so
    /// rather than promising an Undo that cannot work. (`nzbd run` always
    /// configures history; a bare router — the cluster proxy — does not.)
    #[tokio::test]
    async fn delete_without_a_history_store_reports_parked_false() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        let app = router(engine.clone());
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/jobs?name=nohist")
                    .body(axum::body::Body::from(NZB))
                    .unwrap(),
            )
            .await
            .unwrap();
        let id = body_json(resp).await["id"].as_u64().unwrap();

        let resp = app
            .oneshot(
                axum::http::Request::post(format!("/api/v1/jobs/{id}/actions/delete"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let v = body_json(resp).await;
        assert_eq!(v["ok"], true, "the delete itself still works");
        assert_eq!(v["parked"], false, "…but there is no Undo to offer");
    }

    #[tokio::test]
    async fn failed_history_file_deletion_preserves_the_retry_record() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        let db = Arc::new(HistoryDb::open(&tmp.path().join("history.sqlite"), None).unwrap());
        let blocked = tmp.path().join("payload");
        std::fs::write(&blocked, b"not a directory").unwrap();
        let entry: nzbd_state::HistoryEntry = serde_json::from_value(json!({
            "job": 71, "name": "preserve", "category": null,
            "final_dir": blocked.to_str(), "status": "FAILURE", "size": 15,
            "completed_at_unix": 1
        }))
        .unwrap();
        db.record(&entry).unwrap();
        let app = router_with(ApiState {
            engine: engine.clone(),
            history: Some(db.clone()),
            torrent: None,
            log: None,
            setup: None,
            clients: None,
            shutdown: None,
            pp_stats: None,
            pp_manager: None,
            events: None,
        });
        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::post("/api/v1/history/71/actions/delete-files")
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(!response.status().is_success());
            assert!(db.get(JobId(71)).unwrap().is_some());
            assert_eq!(std::fs::read(&blocked).unwrap(), b"not a directory");
        }
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn requeue_durably_removes_the_deleted_history_record() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        let shared = tmp.path().join("history-log");
        let db =
            Arc::new(HistoryDb::open(&tmp.path().join("history.sqlite"), Some(&shared)).unwrap());
        let app = router_with(ApiState {
            engine: engine.clone(),
            torrent: None,
            history: Some(db.clone()),
            log: None,
            setup: None,
            clients: None,
            shutdown: None,
            pp_stats: None,
            pp_manager: None,
            events: None,
        });

        let added = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/jobs?name=undo-me")
                    .body(axum::body::Body::from(NZB))
                    .unwrap(),
            )
            .await
            .unwrap();
        let old_id = body_json(added).await["id"].as_u64().unwrap();
        let deleted = app
            .clone()
            .oneshot(
                axum::http::Request::post(format!("/api/v1/jobs/{old_id}/actions/delete"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::OK);
        assert_eq!(body_json(deleted).await["parked"], true);
        assert_eq!(db.list_filtered(10, true).unwrap()[0].status, "DELETED");

        let requeued = app
            .clone()
            .oneshot(
                axum::http::Request::post(format!("/api/v1/history/{old_id}/actions/requeue"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(requeued.status(), StatusCode::OK);
        let new_id = body_json(requeued).await["id"].as_u64().unwrap();
        assert_ne!(new_id, old_id, "the transfer is a new live queue job");
        assert!(db.list_filtered(10, true).unwrap().is_empty());
        assert!(!db.has_spool(JobId(old_id as u32)));

        db.refresh().unwrap();
        assert!(db.list_since(0, 10).unwrap().is_empty());
        let peer = HistoryDb::open(&tmp.path().join("peer.sqlite"), Some(&shared)).unwrap();
        assert!(
            peer.list_filtered(10, true).unwrap().is_empty(),
            "a fresh node must not rebuild the obsolete DELETED record"
        );
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn add_list_status_and_actions() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        let app = router(engine.clone());

        // Add.
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/jobs?name=myjob&priority=50")
                    .body(axum::body::Body::from(NZB))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let v = body_json(resp).await;
        let id = v["id"].as_u64().unwrap();

        // List.
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/jobs")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let v = body_json(resp).await;
        assert_eq!(v["jobs"][0]["name"], "myjob");
        assert_eq!(v["jobs"][0]["priority"], 50);

        // Reprioritize an existing job through the native API.
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::put(format!("/api/v1/jobs/{id}/priority"))
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(r#"{"priority":100}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let v = body_json(resp).await;
        assert_eq!(v["ok"], true);
        assert_eq!(v["priority"], 100);
        assert_eq!(
            engine
                .export_job(JobId(id as u32))
                .await
                .unwrap()
                .unwrap()
                .priority,
            100
        );

        // Status.
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/status")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let v = body_json(resp).await;
        assert_eq!(v["jobs_queued"], 1);
        assert_eq!(v["remaining_bytes"], 1000);

        // Pause + resume + delete.
        for (action, expect) in [("pause", true), ("resume", true), ("delete", true)] {
            let resp = app
                .clone()
                .oneshot(
                    axum::http::Request::post(format!("/api/v1/jobs/{id}/actions/{action}"))
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "{action}");
            assert_eq!(body_json(resp).await["ok"], expect, "{action}");
        }

        // Gone now.
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::get(format!("/api/v1/jobs/{id}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        engine.shutdown().await;
    }

    #[tokio::test]
    async fn rejects_bad_nzb_and_bad_actions() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        let app = router(engine.clone());

        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/jobs")
                    .body(axum::body::Body::from("<html>nope</html>"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/jobs/1/actions/explode")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        engine.shutdown().await;
    }

    #[test]
    fn client_registry_prunes_after_ttl() {
        let reg = ClientRegistry::default();
        // Two clients seen at t=0; a third arrives later.
        reg.note(Some("Sonarr/4"), "history", 0);
        reg.note(Some("Radarr/5"), "history", 0);
        assert_eq!(reg.snapshot(10).len(), 2);

        // At t = TTL-1 both are still live; Radarr checks in again.
        reg.note(Some("Radarr/5"), "listgroups", CLIENT_TTL_SECS - 1);
        let live = reg.snapshot(CLIENT_TTL_SECS - 1);
        assert_eq!(live.len(), 2);
        assert_eq!(live[0].user_agent, "Radarr/5", "most-recent first");

        // Jump past Sonarr's TTL but within Radarr's: Sonarr is pruned.
        let live = reg.snapshot(CLIENT_TTL_SECS + 1);
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].user_agent, "Radarr/5");
        assert_eq!(live[0].calls, 2);

        // Long silence: everyone is gone and the map is empty.
        assert!(reg.snapshot(CLIENT_TTL_SECS * 3).is_empty());
    }

    #[test]
    fn auth_matrix() {
        use base64::Engine as _;
        let b64 = |s: &str| base64::engine::general_purpose::STANDARD.encode(s);

        // No credentials configured: everything passes.
        let open = AuthConfig::default();
        assert!(authorized(&open, None));

        let auth = AuthConfig {
            username: "paul".into(),
            password: Some("s3cret".into()),
            token: Some("tok123".into()),
        };
        assert!(!authorized(&auth, None));
        assert!(!authorized(&auth, Some("Basic definitely-not-b64!")));
        assert!(authorized(
            &auth,
            Some(&format!("Basic {}", b64("paul:s3cret")))
        ));
        assert!(!authorized(
            &auth,
            Some(&format!("Basic {}", b64("paul:wrong")))
        ));
        assert!(!authorized(
            &auth,
            Some(&format!("Basic {}", b64("eve:s3cret")))
        ));
        assert!(authorized(&auth, Some("Bearer tok123")));
        assert!(!authorized(&auth, Some("Bearer nope")));

        // Password-only config rejects bearer attempts.
        let basic_only = AuthConfig {
            username: "paul".into(),
            password: Some("pw".into()),
            token: None,
        };
        assert!(!authorized(&basic_only, Some("Bearer pw")));
        assert!(authorized(
            &basic_only,
            Some(&format!("Basic {}", b64("paul:pw")))
        ));

        let unicode = AuthConfig {
            username: "møbiłe".into(),
            password: Some("päss—🔒".into()),
            token: None,
        };
        assert!(authorized(
            &unicode,
            Some(&format!("Basic {}", b64("møbiłe:päss—🔒")))
        ));
    }

    #[tokio::test]
    async fn auth_layer_guards_routes_but_not_healthz() {
        use base64::Engine as _;
        use tower::util::ServiceExt;
        let tmp = tempfile::tempdir().unwrap();
        let engine = nzbd_engine::Engine::spawn(nzbd_engine::EngineConfig::single_node(
            vec![],
            tmp.path().join("state"),
            tmp.path().join("dest"),
            nzbd_engine::Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let app = require_auth(
            router(engine.clone()),
            AuthConfig {
                username: "u".into(),
                password: Some("p".into()),
                token: None,
            },
        );

        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/status")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            resp.headers()
                .get(axum::http::header::WWW_AUTHENTICATE)
                .unwrap(),
            "Basic realm=\"nzbd\", charset=\"UTF-8\""
        );

        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::get("/healthz")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "healthz stays open");

        let creds = base64::engine::general_purpose::STANDARD.encode("u:p");
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/status")
                    .header("authorization", format!("Basic {creds}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn metrics_exposition_shape() {
        use tower::util::ServiceExt;
        let tmp = tempfile::tempdir().unwrap();
        let engine = nzbd_engine::Engine::spawn(nzbd_engine::EngineConfig::single_node(
            vec![],
            tmp.path().join("state"),
            tmp.path().join("dest"),
            nzbd_engine::Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let app = router(engine.clone());
        let resp = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("nzbd_download_rate_bytes_per_second"));
        assert!(text.contains("# TYPE nzbd_jobs gauge"));
        assert!(text.contains("nzbd_remaining_bytes 0"));
        engine.shutdown().await;
    }

    /// Field report 2026-07-25: the dashboard froze until a browser
    /// reload — Safari served the 5 s poll from its HTTP cache because the
    /// JSON endpoints sent no Cache-Control at all. Live endpoints must say
    /// `no-store`; the PWA assets keep their long-lived cache; the shell
    /// revalidates.
    #[tokio::test]
    async fn cache_headers_no_store_on_api_cacheable_assets() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        let app = router(engine.clone());

        let get = |path: &'static str| {
            let app = app.clone();
            async move {
                app.oneshot(
                    axum::http::Request::get(path)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
            }
        };

        for path in ["/api/v1/status", "/api/v1/jobs", "/api/v1/setup"] {
            let resp = get(path).await;
            assert_eq!(
                resp.headers()
                    .get(axum::http::header::CACHE_CONTROL)
                    .and_then(|v| v.to_str().ok()),
                Some("no-store"),
                "{path} must never be served from a browser cache"
            );
        }
        let resp = get("/icons/icon-192.png").await;
        assert_eq!(
            resp.headers()
                .get(axum::http::header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("public, max-age=86400"),
            "icons keep their long-lived cache header"
        );
        let resp = get("/").await;
        assert_eq!(
            resp.headers()
                .get(axum::http::header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("no-cache"),
            "the shell revalidates so UI updates land on next load"
        );
        engine.shutdown().await;
    }

    /// The SSE stream must carry data by itself — progress/rate have no
    /// discrete engine event, so without the 1 Hz `tick` a dashboard on a
    /// quiet-event queue never moves (the "frozen until refresh" report).
    /// The first frame arrives immediately (interval tick zero).
    #[tokio::test]
    async fn sse_stream_sends_tick_with_full_read_model() {
        use tokio_stream::StreamExt as _;
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;

        // A job in the queue so the tick payload carries a row.
        let app = router(engine.clone());
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/jobs?name=tickjob")
                    .body(axum::body::Body::from(NZB))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        let resp = app
            .oneshot(
                axum::http::Request::get("/api/v1/events")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            resp.headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.starts_with("text/event-stream")),
            "SSE content type"
        );

        let mut body = resp.into_body().into_data_stream();
        let mut text = String::new();
        // Collect frames until the tick shows up (bounded by the timeout).
        let deadline = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while let Some(chunk) = body.next().await {
                text.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
                if text.contains("event: tick") && text.contains("\n\n") {
                    break;
                }
            }
        })
        .await;
        assert!(deadline.is_ok(), "no tick frame within 5 s: {text:?}");
        assert!(text.contains("event: tick"), "got: {text:?}");
        assert!(
            text.contains("download_rate_bps") && text.contains("tickjob"),
            "tick carries status + job rows: {text:?}"
        );
        engine.shutdown().await;
    }

    /// An idle queue deduplicates its ticks on purpose — the quiet-stream
    /// property is deliberate (battery and radio on phone PWAs). That
    /// leaves the client unable to tell "nothing is happening" from "this
    /// stream is dead", which is precisely the hole `hb` closes.
    #[tokio::test]
    async fn idle_stream_emits_a_heartbeat_instead_of_going_silent() {
        use tokio_stream::StreamExt as _;
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        // No jobs: after the first tick every later one is a duplicate.
        let resp = router(engine.clone())
            .oneshot(
                axum::http::Request::get("/api/v1/events")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let mut body = resp.into_body().into_data_stream();
        let mut text = String::new();
        let got = tokio::time::timeout(std::time::Duration::from_secs(9), async {
            while let Some(chunk) = body.next().await {
                text.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
                if text.contains("event: hb") {
                    return true;
                }
            }
            false
        })
        .await;
        assert_eq!(
            got,
            Ok(true),
            "no hb within 9 s of an idle stream: {text:?}"
        );
        assert!(
            text.contains("now_unix"),
            "hb carries the daemon's clock so the client can measure staleness: {text:?}"
        );
        // The heartbeat is a liveness signal, not a second read model.
        let hb = text.split("event: hb").nth(1).unwrap_or_default();
        assert!(
            !hb.contains("download_rate_bps"),
            "hb must stay tiny — it is not a tick: {hb:?}"
        );
        engine.shutdown().await;
    }

    /// Daemon log lines reach the page over the same stream, batched onto
    /// History paging is server-side because the cost it bounds is
    /// server-side (see `HistoryDb::list_page`). The wire contract the
    /// pager needs: a page holds only its own rows, `total` counts ALL
    /// matching rows rather than the page, and consecutive pages tile the
    /// list without overlap or gap.
    #[tokio::test]
    async fn history_pages_carry_their_own_rows_and_the_whole_total() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        let db = Arc::new(
            HistoryDb::open(
                &tmp.path().join("history.sqlite"),
                Some(&tmp.path().join("hjsonl")),
            )
            .unwrap(),
        );
        let now = 1_800_000_000;
        for i in 0..25u32 {
            db.record(&nzbd_state::HistoryEntry {
                job: JobId(i),
                name: format!("job-{i}"),
                category: None,
                final_dir: None,
                status: "SUCCESS".into(),
                size: 10,
                health: 1000,
                params: vec![],
                dupe_key: String::new(),
                dupe_score: 0,
                completed_at_unix: now - (25 - i as i64) * 60,
                hidden: false,
                first_seen_at_unix: None,
                last_seen_at_unix: None,
                seen_count: 0,
                removed_at_unix: None,
                picked_up_by: None,
                record: None,
                stages: vec![],
                seq: 0,
            })
            .unwrap();
        }
        let app = router_with(ApiState {
            engine: engine.clone(),
            torrent: None,
            history: Some(db.clone()),
            log: None,
            setup: None,
            clients: None,
            shutdown: None,
            pp_stats: None,
            pp_manager: None,
            events: None,
        });
        let page = |offset: usize, limit: usize| {
            let app = app.clone();
            async move {
                let resp = app
                    .oneshot(
                        axum::http::Request::get(format!(
                            "/api/v1/history?limit={limit}&offset={offset}"
                        ))
                        .body(axum::body::Body::empty())
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(resp.status(), StatusCode::OK);
                body_json(resp).await
            }
        };

        let paused = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/history-sync/pause")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(paused.status(), StatusCode::OK);
        assert_eq!(body_json(paused).await["paused"], true);
        let invalid = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/history-sync/forget")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        let first = page(0, 10).await;
        assert_eq!(
            first["sync"]["paused"], true,
            "paused reconciliation does not prevent indexed reads"
        );
        assert_eq!(first["entries"].as_array().unwrap().len(), 10);
        assert_eq!(first["total"], 25, "total counts the list, not the page");
        assert_eq!(first["offset"], 0);
        assert_eq!(first["limit"], 10);
        assert_eq!(first["entries"][0]["name"], "job-24", "newest first");

        let ids = |v: &serde_json::Value| -> Vec<String> {
            v["entries"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| e["name"].as_str().unwrap().to_string())
                .collect()
        };
        let mut tiled = ids(&first);
        tiled.extend(ids(&page(10, 10).await));
        let last = page(20, 10).await;
        assert_eq!(last["entries"].as_array().unwrap().len(), 5, "short tail");
        assert_eq!(last["total"], 25, "total does not shrink on the last page");
        tiled.extend(ids(&last));
        let expected: Vec<String> = (0..25u32).rev().map(|i| format!("job-{i}")).collect();
        assert_eq!(tiled, expected, "the pages reassemble the list exactly");

        // Off the end is an empty page, not an error and not a wrap-around.
        let past = page(100, 10).await;
        assert!(past["entries"].as_array().unwrap().is_empty());
        assert_eq!(past["total"], 25);

        // The cursor shape is untouched by offset: it has a position of
        // its own, and honouring both would give a consumer two.
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/history?since_seq=0&limit=3&offset=99")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let v = body_json(resp).await;
        let names = ids(&v);
        assert_eq!(names.len(), 3);
        assert_eq!(names[0], "job-0", "cursor walk is oldest-first from seq 0");
    }

    #[test]
    fn operator_clients_do_not_count_as_history_consumers() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-nzbd-client", "nzbd-mobile/1.0".parse().unwrap());
        assert_eq!(consumer_name(&headers).as_deref(), Some("nzbd-mobile/1.0"));

        headers.insert("x-nzbd-role", "operator".parse().unwrap());
        assert_eq!(consumer_name(&headers), None);
        assert_eq!(client_name(&headers).as_deref(), Some("nzbd-mobile/1.0"));
    }

    /// the 1 Hz loop. A `tail -f` that updates once a second is the right
    /// cost/fidelity trade; the alternative is one frame per line.
    #[tokio::test]
    async fn sse_stream_carries_log_lines() {
        use tokio_stream::StreamExt as _;
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        let log = LogBuffer::new(500);
        let app = router_with(ApiState {
            engine: engine.clone(),
            torrent: None,
            history: None,
            log: Some(log.clone()),
            setup: None,
            clients: None,
            shutdown: None,
            pp_stats: None,
            pp_manager: None,
            events: None,
        });
        // Pre-existing lines must NOT be replayed: a fresh connection tails
        // from now, and the REST endpoint is the backfill.
        log.push("INFO", "ancient history".into());
        let resp = app
            .oneshot(
                axum::http::Request::get("/api/v1/events")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let mut body = resp.into_body().into_data_stream();
        let mut text = String::new();
        // Wait for the first frame before logging anything: that is what
        // proves the stream task has started and taken its log cursor.
        // Pushing before then would race the cursor, not the feature.
        let started = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while let Some(chunk) = body.next().await {
                text.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
                if text.contains("event: tick") {
                    return true;
                }
            }
            false
        })
        .await;
        assert_eq!(started, Ok(true), "stream never started: {text:?}");
        log.push("INFO", "after the connect".into());
        let got = tokio::time::timeout(std::time::Duration::from_secs(6), async {
            while let Some(chunk) = body.next().await {
                text.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
                if text.contains("event: log") {
                    return true;
                }
            }
            false
        })
        .await;
        assert_eq!(got, Ok(true), "no log frame within 6 s: {text:?}");
        assert!(text.contains("after the connect"), "got: {text:?}");
        assert!(
            !text.contains("ancient history"),
            "a new stream must not replay the whole ring: {text:?}"
        );
        assert!(text.contains("\"dropped\""), "batches report what they cut");
        engine.shutdown().await;
    }

    /// A burst bigger than one frame shows the NEWEST lines and says how
    /// many it skipped. Showing the oldest instead would wedge the tail in
    /// the past for as long as the burst lasted.
    #[test]
    fn log_batches_cap_and_report_what_they_skipped() {
        let log = LogBuffer::new(1000);
        for i in 0..500 {
            log.push("INFO", format!("line {i}"));
        }
        let (batch, dropped) = log.since_capped(0, LOG_BATCH_MAX);
        assert_eq!(batch.len(), LOG_BATCH_MAX, "the cap holds");
        assert_eq!(dropped, 300, "…and the remainder is reported, not hidden");
        assert_eq!(batch[0].text, "line 300", "the newest 200, not the oldest");
        assert_eq!(batch[199].text, "line 499");
        // Draining to the end leaves nothing behind and nothing skipped.
        let (rest, dropped) = log.since_capped(batch[199].id, LOG_BATCH_MAX);
        assert!(rest.is_empty());
        assert_eq!(dropped, 0);
        assert_eq!(
            log.newest_id(),
            batch[199].id,
            "cursor lines up with the ring"
        );
    }

    /// Per-server rates are the header rate sliced by provider — the same
    /// bytes counted twice, never two independently-derived figures. This
    /// is the same-measurement invariant the per-job rates already keep
    /// (field report 2026-07-25: a header claiming 93 MiB/s over rows that
    /// claimed 56 — and 2026-07-26: a header window disagreeing with the
    /// chips by 2.5× — which is why the header is now literally the sum of
    /// the per-server EMAs, folded from one time-stamped drain).
    /// The build identity the footer shows, as the *running build* sees
    /// it — [`crate::version`] tests the composition, this tests that this
    /// binary actually got one.
    ///
    /// A build from a checkout has no excuse: if `.git` is right there and
    /// the version still says `+unknown`, the identity chain is broken and
    /// every deploy from this tree would ship anonymous. A build from an
    /// unpacked tarball genuinely cannot know, and says so; that is the
    /// designed behaviour, not a failure.
    #[test]
    fn build_identity_is_populated() {
        let v = version::full();
        assert!(
            v.starts_with(env!("CARGO_PKG_VERSION")),
            "full version {v:?} extends the Cargo version"
        );
        let checkout = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.git")
            .exists();
        if checkout {
            assert!(
                !v.ends_with("+unknown"),
                "built from a checkout but the version is {v:?} — the build \
                 script saw neither git nor NZBD_GIT_DESCRIBE"
            );
        } else {
            eprintln!("note: no checkout visible, build identity is {v:?}");
        }
        let b = version::BUILT;
        assert!(
            b.len() == 20 && b.ends_with(" UTC") && &b[4..5] == "-" && &b[13..14] == ":",
            "stamp shaped 'YYYY-MM-DD HH:MM UTC': {b:?}"
        );
    }

    #[test]
    fn per_server_rates_are_the_same_bytes_as_the_header_rate() {
        use nzbd_engine::rate::{fold_wire_ema, SpeedMeter};
        let m = SpeedMeter::new();
        m.add_for(1, 0, 600);
        m.add_for(2, 0, 400);
        m.add_for(1, 1, 1000);
        let d = m.drain();
        let job_sum: u64 = d.per_job.values().sum();
        let server_sum: u64 = d.per_server.values().sum();
        assert_eq!(job_sum, 2000);
        assert_eq!(server_sum, job_sum, "same bytes, two attributions");
        assert_eq!(d.per_server[&0], 1000);
        assert_eq!(d.per_server[&1], 1000);
        assert!(
            m.drain().per_server.is_empty(),
            "draining resets, so the next window starts clean"
        );
        // The header the snapshot publishes is Σ per-server EMAs — fold the
        // same drain into both maps and the sums cannot disagree.
        let mut job_ema = std::collections::HashMap::new();
        let mut server_ema = std::collections::HashMap::new();
        fold_wire_ema(&mut job_ema, &d.per_job, 1.0);
        fold_wire_ema(&mut server_ema, &d.per_server, 1.0);
        let header: u64 = server_ema.values().map(|e| e.max(0.0) as u64).sum();
        let rows: u64 = job_ema.values().map(|e| e.max(0.0) as u64).sum();
        assert_eq!(header, rows, "tile and rows are one measurement");
    }

    /// The status DTO carries the per-server rows the UI draws its chips
    /// from, one per configured server even before any bytes move.
    #[tokio::test]
    async fn status_carries_named_per_server_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        let resp = router(engine.clone())
            .oneshot(
                axum::http::Request::get("/api/v1/status")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let v = body_json(resp).await;
        assert!(
            v["servers"].is_array(),
            "status carries per-server rows: {v}"
        );
        for row in v["servers"].as_array().unwrap() {
            assert!(row["name"].is_string(), "each row is named: {row}");
            assert!(row["rate_bps"].is_number(), "…and carries a rate: {row}");
        }
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn settings_can_enable_default_torrent_config() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        let path = tmp.path().join("nzbd.toml");
        let mut cfg = nzbd_config::Config::default();
        cfg.paths.main_dir = tmp.path().join("data");
        let initial = nzbd_config::to_toml(&cfg).unwrap();
        assert!(!initial.contains("[torrent]"));
        std::fs::write(&path, initial).unwrap();
        let app = router_with(ApiState {
            engine: engine.clone(),
            torrent: None,
            history: None,
            log: None,
            setup: Some(Arc::new(SetupHandle::for_running(
                Some(path.clone()),
                "127.0.0.1:0".into(),
                cfg.clone(),
            ))),
            clients: None,
            shutdown: None,
            pp_stats: None,
            pp_manager: None,
            events: None,
        });
        let get = || {
            axum::http::Request::get("/api/v1/config")
                .body(axum::body::Body::empty())
                .unwrap()
        };
        let response = app.clone().oneshot(get()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut settings = body_json(response).await;
        assert_eq!(settings["config"]["torrent"], json!(cfg.torrent));
        assert!(!settings["toml"].as_str().unwrap().contains("[torrent]"));

        settings["config"]["torrent"]["enabled"] = json!(true);
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::put("/api/v1/config")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(settings["config"].to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let saved = body_json(response).await;
        assert!(saved["restart_required"]
            .as_array()
            .unwrap()
            .contains(&json!("torrent")));
        let persisted =
            nzbd_config::Config::from_toml(&std::fs::read_to_string(path).unwrap()).unwrap();
        cfg.torrent.enabled = true;
        assert_eq!(persisted, cfg);
        let reloaded = body_json(app.oneshot(get()).await.unwrap()).await;
        assert_eq!(reloaded["config"]["torrent"], json!(cfg.torrent));
        engine.shutdown().await;
    }

    /// The "test connection" endpoint: live probe against a real (mock)
    /// NNTP server through the production transport — greeting + auth,
    /// wrong-password and dead-port shapes, and stored-password resolution
    /// via the config mask (the browser never holds real secrets).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_test_endpoint_probes_and_resolves_masked_password() {
        let ns = nzbd_nserv::NservBuilder::new()
            .credentials("alice", "s3cret")
            .start()
            .await
            .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;

        // Router WITH a running-mode setup handle: server 0 has a stored
        // password the form only ever sees as the mask.
        let mut cfg = nzbd_config::Config::default();
        cfg.servers.push(nzbd_config::ServerConfig {
            host: "127.0.0.1".into(),
            port: ns.port(),
            tls: false,
            username: Some("alice".into()),
            password: Some("s3cret".into()),
            ..Default::default()
        });
        let app = router_with(ApiState {
            engine: engine.clone(),
            torrent: None,
            history: None,
            log: None,
            setup: Some(Arc::new(SetupHandle::for_running(
                None,
                "127.0.0.1:0".into(),
                cfg,
            ))),
            clients: None,
            shutdown: None,
            pp_stats: None,
            pp_manager: None,
            events: None,
        });

        let probe = |app: Router, body: serde_json::Value| async move {
            let resp = app
                .oneshot(
                    axum::http::Request::post("/api/v1/servers/test")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            body_json(resp).await
        };

        // Real credentials: greeting + AUTHINFO succeed.
        let v = probe(
            app.clone(),
            json!({ "host": "127.0.0.1", "port": ns.port(), "tls": false,
                    "username": "alice", "password": "s3cret" }),
        )
        .await;
        assert_eq!(v["ok"], true, "{v}");
        assert!(v["message"].as_str().unwrap().contains("established"));

        // Wrong password: connected but login fails.
        let v = probe(
            app.clone(),
            json!({ "host": "127.0.0.1", "port": ns.port(), "tls": false,
                    "username": "alice", "password": "wrong" }),
        )
        .await;
        assert_eq!(v["ok"], false);
        assert!(v["message"].as_str().unwrap().contains("login failed"));

        // Masked password + server_index: the daemon swaps in the stored
        // secret — testing a saved server needs no retyping.
        let v = probe(
            app.clone(),
            json!({ "host": "127.0.0.1", "port": ns.port(), "tls": false,
                    "username": "alice", "password": nzbd_config::SECRET_MASK,
                    "server_index": 0 }),
        )
        .await;
        assert_eq!(v["ok"], true, "mask resolves against config: {v}");

        // Dead port: a probe result, not an API error.
        let v = probe(
            app.clone(),
            json!({ "host": "127.0.0.1", "port": 1, "tls": false }),
        )
        .await;
        assert_eq!(v["ok"], false);
        assert!(v["message"].as_str().unwrap().contains("Connection failed"));

        // Mask with nothing to resolve against (bare router, no setup
        // handle): explicit "retype it" instead of probing with the
        // literal mask string as the password.
        let bare = router(engine.clone());
        let resp = bare
            .oneshot(
                axum::http::Request::post("/api/v1/servers/test")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        json!({ "host": "127.0.0.1", "port": ns.port(), "tls": false,
                                "username": "alice", "password": nzbd_config::SECRET_MASK })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let v = body_json(resp).await;
        assert_eq!(v["ok"], false);
        assert!(v["message"].as_str().unwrap().contains("retype"));

        engine.shutdown().await;
    }

    /// Job detail surfaces: per-file listing and the regenerated NZB.
    /// The NZB must round-trip through the real parser — a download of
    /// the export re-added anywhere must describe the same articles.
    #[tokio::test]
    async fn job_files_and_nzb_export() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(&tmp).await;
        let app = router(engine.clone());

        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::post("/api/v1/jobs?name=exportme")
                    .body(axum::body::Body::from(NZB))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let id = body_json(resp).await["id"].as_u64().unwrap();

        // Per-file detail.
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::get(format!("/api/v1/jobs/{id}/files"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let v = body_json(resp).await;
        assert_eq!(v["files"].as_array().unwrap().len(), 1);
        assert_eq!(v["files"][0]["filename"], "f.bin");
        assert_eq!(v["files"][0]["total_segments"], 1);
        assert_eq!(v["files"][0]["size_bytes"], 1000);

        // NZB export: correct headers, parses, same segment ids.
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::get(format!("/api/v1/jobs/{id}/nzb"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp
            .headers()
            .get(axum::http::header::CONTENT_DISPOSITION)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.contains("exportme.nzb")));
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed = nzbd_nzb::parse(&bytes).expect("regenerated NZB parses");
        assert_eq!(parsed.files.len(), 1);
        assert_eq!(parsed.files[0].segments.len(), 1);
        assert_eq!(parsed.files[0].segments[0].message_id, "m1@x");
        assert_eq!(parsed.meta.title.as_deref(), Some("exportme"));

        // Unknown job: 404s, not a panic.
        let resp = app
            .oneshot(
                axum::http::Request::get("/api/v1/jobs/9999/nzb")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        engine.shutdown().await;
    }

    /// Every engine event crosses the wire under its own name — the UI
    /// keys refresh nudges on these (a pause emitted as an opaque "event"
    /// was invisible to it; part of the stale-pause report).
    #[test]
    fn event_wire_names_cover_pause_and_limit() {
        use nzbd_engine::Event as E;
        let wire = |ev: &E| {
            let (name, body) = event_json(ev);
            (name, body.to_string())
        };
        let (name, data) = wire(&E::QueuePauseChanged {
            paused: true,
            source: "monarr/1.0".into(),
        });
        assert_eq!(name, "queue_pause_changed");
        assert!(data.contains("true"));
        assert!(
            data.contains("monarr/1.0"),
            "pause events carry their source: {data}"
        );
        let (name, data) = wire(&E::SpeedLimitChanged {
            bytes_per_sec: Some(1024),
        });
        assert_eq!(name, "speed_limit_changed");
        assert!(data.contains("1024"));
        let (name, _) = wire(&E::JobAssigned {
            job: JobId(7),
            node: Some("n2".into()),
        });
        assert_eq!(name, "job_assigned");

        // The post-processing pair is what the integration contract is
        // built on; their names and the stage spelling are load-bearing
        // for every consumer, so they are asserted, not assumed.
        let (name, data) = wire(&E::JobPpStage {
            job: JobId(7),
            name: "Show.S01E01".into(),
            stage: nzbd_types::PostStage::ParVerify,
        });
        assert_eq!(name, "job_pp_stage");
        assert!(data.contains("\"stage\":\"par_verify\""), "{data}");
        let (name, data) = wire(&E::JobPpFinished {
            job: JobId(7),
            name: "Show.S01E01".into(),
            category: Some("tv".into()),
            pp_status: "SUCCESS".into(),
            final_dir: Some("/dest/Show.S01E01".into()),
            size_bytes: 42,
            health: 1000,
            params: vec![("monarr-transfer".into(), "t-42-a3f9c1".into())],
            history_seq: 913,
        });
        assert_eq!(name, "job_pp_finished");
        assert!(
            data.contains("\"final_dir\":\"/dest/Show.S01E01\""),
            "{data}"
        );
        assert!(data.contains("\"history_seq\":913"), "{data}");
        assert!(
            data.contains("monarr-transfer"),
            "the transfer id must survive to the wire: {data}"
        );
    }
}
