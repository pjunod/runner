//! Narrow qBittorrent Web API projection for Sonarr and Radarr.
//!
//! This is deliberately not a general qBittorrent emulator. Routes are
//! limited to the download-client workflow and every mutation is delegated to
//! nzbd's queue owner or torrent admission service.

use axum::body::Bytes;
use axum::extract::{ConnectInfo, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use nzbd_api::torrent_admission::TorrentAdmissionService;
use nzbd_engine::{AddOpts, EngineHandle, MoveOp};
use nzbd_types::{JobId, JobKind, SeedPolicy, TorrentPhase, TorrentSource};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;

const SESSION_TTL_SECS: i64 = 3600;
const MAX_SESSIONS: usize = 256;
const MAX_FAILED_LOGINS: usize = 64;
const MAX_QBIT_TAGS: usize = 32;
const MAX_QBIT_TAG_BYTES: usize = 256;
const MAX_QBIT_TAGS_BYTES: usize = 1024;
const TRANSFER_TAG_PREFIX: &str = "monarr-transfer-";

#[derive(Clone)]
pub struct QbitState {
    pub engine: EngineHandle,
    pub torrent: TorrentAdmissionService,
    pub auth: QbitAuth,
    pub save_path: PathBuf,
    pub dht: bool,
    pub queueing: bool,
    pub global_seed_ratio: f64,
    pub global_seed_minutes: u64,
    pub clients: Option<Arc<nzbd_api::ClientRegistry>>,
    categories: Arc<Mutex<CategoryStore>>,
    sessions: Arc<Mutex<HashMap<String, i64>>>,
    failed_logins: Arc<Mutex<HashMap<IpAddr, VecDeque<i64>>>>,
}

#[derive(Clone, Default)]
pub struct QbitAuth {
    pub username: String,
    pub password: Option<String>,
    pub token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OverlayCategory {
    save_path: PathBuf,
}

#[derive(Debug, Clone, Default)]
struct CategoryStore {
    path: PathBuf,
    configured: BTreeMap<String, PathBuf>,
    overlay: BTreeMap<String, OverlayCategory>,
}

impl QbitState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        engine: EngineHandle,
        torrent: TorrentAdmissionService,
        auth: QbitAuth,
        state_dir: PathBuf,
        save_path: PathBuf,
        configured_categories: BTreeMap<String, PathBuf>,
        dht: bool,
        queueing: bool,
        global_seed_ratio: f64,
        global_seed_minutes: u64,
        clients: Option<Arc<nzbd_api::ClientRegistry>>,
    ) -> Self {
        let path = state_dir.join("torrent-categories.json");
        let overlay = load_category_overlay(&path, &save_path);
        for (name, category) in &overlay {
            torrent.register_category_payload_root(name.clone(), category.save_path.clone());
        }
        Self {
            engine,
            torrent,
            auth,
            save_path,
            dht,
            queueing,
            global_seed_ratio,
            global_seed_minutes,
            clients,
            categories: Arc::new(Mutex::new(CategoryStore {
                path,
                configured: configured_categories,
                overlay,
            })),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            failed_logins: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

/// Load only canonical qBittorrent-created category roots contained by the
/// configured global torrent root. The daemon calls this before recovery so
/// overlay-category jobs have the same authorized root inventory on restart
/// as they do after the compatibility router is mounted.
pub fn load_overlay_category_roots(
    state_dir: &std::path::Path,
    save_path: &std::path::Path,
) -> BTreeMap<String, PathBuf> {
    load_category_overlay(&state_dir.join("torrent-categories.json"), save_path)
        .into_iter()
        .map(|(name, category)| (name, category.save_path))
        .collect()
}

fn load_category_overlay(
    path: &std::path::Path,
    save_path: &std::path::Path,
) -> BTreeMap<String, OverlayCategory> {
    let Ok(save_path) = std::fs::canonicalize(save_path) else {
        return BTreeMap::new();
    };
    let mut overlay: BTreeMap<String, OverlayCategory> = std::fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default();
    overlay.retain(|_, category| {
        let Ok(root) = std::fs::canonicalize(&category.save_path) else {
            return false;
        };
        if !root.starts_with(&save_path) {
            return false;
        }
        category.save_path = root;
        true
    });
    overlay
}

pub fn router(state: QbitState) -> Router {
    let upload_body_limit = state
        .torrent
        .max_request_body_bytes()
        .saturating_add(64 * 1024);
    Router::new()
        .route("/api/v2/auth/login", post(login))
        .route("/api/v2/app/webapiVersion", get(webapi_version))
        .route("/api/v2/app/version", get(version))
        .route("/api/v2/app/preferences", get(preferences))
        .route("/api/v2/torrents/info", get(torrent_info))
        .route("/api/v2/torrents/properties", get(torrent_properties))
        .route("/api/v2/torrents/files", get(torrent_files))
        .route(
            "/api/v2/torrents/add",
            post(torrent_add).layer(axum::extract::DefaultBodyLimit::max(upload_body_limit)),
        )
        .route("/api/v2/torrents/delete", post(torrent_delete))
        .route("/api/v2/torrents/setCategory", post(set_category))
        .route("/api/v2/torrents/categories", get(categories))
        .route("/api/v2/torrents/createCategory", post(create_category))
        .route("/api/v2/torrents/setShareLimits", post(set_share_limits))
        .route("/api/v2/torrents/topPrio", post(top_priority))
        .route("/api/v2/torrents/setForceStart", post(set_force_start))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            authenticate,
        ))
        .with_state(state)
}

async fn authenticate(State(state): State<QbitState>, request: Request, next: Next) -> Response {
    if request.uri().path() == "/api/v2/auth/login" || authorized(&state, request.headers()) {
        if request.uri().path() != "/api/v2/auth/login" {
            note_client(
                &state,
                request.headers(),
                request.method().as_str(),
                request.uri().path(),
            );
        }
        return next.run(request).await;
    }
    (StatusCode::FORBIDDEN, "Forbidden").into_response()
}

fn authorized(state: &QbitState, headers: &HeaderMap) -> bool {
    if let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(token) = value.strip_prefix("Bearer ") {
            if state
                .auth
                .token
                .as_deref()
                .is_some_and(|expected| secret_eq(expected, token))
            {
                return true;
            }
        }
        if let Some(encoded) = value.strip_prefix("Basic ") {
            if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded) {
                if let Ok(pair) = String::from_utf8(bytes) {
                    if let Some((username, password)) = pair.split_once(':') {
                        if valid_credentials(state, username, password) {
                            return true;
                        }
                    }
                }
            }
        }
    }
    let now = unix_now();
    let sid = headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|cookies| {
            cookies.split(';').find_map(|cookie| {
                let (name, value) = cookie.trim().split_once('=')?;
                (name == "SID").then_some(value)
            })
        });
    let Some(sid) = sid else { return false };
    let mut sessions = state.sessions.lock().unwrap();
    sessions.retain(|_, expiry| *expiry > now);
    sessions.get(sid).is_some_and(|expiry| *expiry > now)
}

async fn login(
    State(state): State<QbitState>,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if authorized(&state, &headers) {
        return "Ok.".into_response();
    }
    let now = unix_now();
    let peer = address.ip();
    {
        let mut all_failures = state.failed_logins.lock().unwrap();
        all_failures.retain(|_, failures| {
            failures.retain(|at| now - *at <= 60);
            !failures.is_empty()
        });
        let failures = all_failures.entry(peer).or_default();
        while failures.front().is_some_and(|at| now - at > 60) {
            failures.pop_front();
        }
        if failures.len() >= MAX_FAILED_LOGINS {
            return (StatusCode::TOO_MANY_REQUESTS, "Fails.").into_response();
        }
    }
    let form = form_fields(&body);
    let valid = valid_credentials(
        &state,
        form.get("username").map_or("", String::as_str),
        form.get("password").map_or("", String::as_str),
    );
    if !valid {
        state
            .failed_logins
            .lock()
            .unwrap()
            .entry(peer)
            .or_default()
            .push_back(now);
        return (StatusCode::FORBIDDEN, "Fails.").into_response();
    }
    let mut bytes = [0_u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    let sid = hex::encode(bytes);
    let mut sessions = state.sessions.lock().unwrap();
    sessions.retain(|_, expiry| *expiry > now);
    if sessions.len() >= MAX_SESSIONS {
        if let Some(oldest) = sessions
            .iter()
            .min_by_key(|(_, expiry)| **expiry)
            .map(|(id, _)| id.clone())
        {
            sessions.remove(&oldest);
        }
    }
    sessions.insert(sid.clone(), now + SESSION_TTL_SECS);
    note_client(&state, &headers, "POST", "/api/v2/auth/login");
    (
        [(
            header::SET_COOKIE,
            format!("SID={sid}; Path=/; HttpOnly; SameSite=Lax; Max-Age={SESSION_TTL_SECS}"),
        )],
        "Ok.",
    )
        .into_response()
}

fn valid_credentials(state: &QbitState, username: &str, password: &str) -> bool {
    state.auth.password.as_deref().is_some_and(|expected| {
        secret_eq(&state.auth.username, username) && secret_eq(expected, password)
    })
}

fn secret_eq(expected: &str, supplied: &str) -> bool {
    expected.len() == supplied.len() && expected.as_bytes().ct_eq(supplied.as_bytes()).into()
}

async fn webapi_version() -> &'static str {
    "2.8.1"
}
async fn version() -> &'static str {
    "v4.6.0-nzbd"
}

async fn preferences(State(state): State<QbitState>) -> Json<Value> {
    Json(json!({
        "save_path": state.save_path,
        "dht": state.dht,
        "queueing_enabled": state.queueing,
        "max_ratio_enabled": state.global_seed_ratio > 0.0,
        "max_ratio": state.global_seed_ratio,
        "max_seeding_time_enabled": state.global_seed_minutes > 0,
        "max_seeding_time": state.global_seed_minutes,
    }))
}

#[derive(Deserialize, Default)]
struct InfoQuery {
    category: Option<String>,
    hashes: Option<String>,
}

async fn torrent_info(
    State(state): State<QbitState>,
    Query(query): Query<InfoQuery>,
) -> Json<Vec<Value>> {
    let hash_filter = query.hashes.as_deref().map(split_hashes);
    let mut rows = Vec::new();
    for summary in state
        .engine
        .snapshot()
        .jobs
        .iter()
        .filter(|job| job.kind == JobKind::Torrent)
    {
        if query.category.as_deref().is_some_and(|category| {
            category != "all" && summary.category.as_deref() != Some(category)
        }) {
            continue;
        }
        let Ok(Some(job)) = state.engine.export_job(summary.id).await else {
            continue;
        };
        let Some(torrent) = job.torrent else { continue };
        if hash_filter.as_ref().is_some_and(|hashes| {
            !hashes
                .iter()
                .any(|hash| hash.eq_ignore_ascii_case(&torrent.info_hash_v1))
        }) {
            continue;
        }
        let progress = if torrent.selected_bytes == 0 {
            0.0
        } else {
            torrent.downloaded_bytes as f64 / torrent.selected_bytes as f64
        };
        let save_path = if torrent.payload_root.as_os_str().is_empty() {
            &state.save_path
        } else {
            &torrent.payload_root
        };
        let tags = qbit_tags_from_params(&job.params);
        rows.push(json!({
            "hash": torrent.info_hash_v1,
            "name": job.name,
            "size": torrent.selected_bytes,
            "progress": progress.clamp(0.0, 1.0),
            "eta": summary.remaining_bytes.checked_div(summary.rate_bps).unwrap_or(8640000),
            "state": qbit_state(torrent.phase, summary.useful_peers, summary.upload_rate_bps),
            "category": job.category.unwrap_or_default(),
            "tags": tags,
            "save_path": save_path,
            "content_path": torrent.content_path.unwrap_or_else(|| save_path.join(&job.name)),
            "ratio": summary.ratio,
            "ratio_limit": torrent.seed_policy.ratio_limit.unwrap_or(-1.0),
            "seeding_time_limit": torrent.seed_policy.time_limit_secs.map(|secs| secs / 60).map_or(-1_i64, |minutes| minutes as i64),
            "last_activity": torrent.last_activity_unix.unwrap_or(0),
        }));
    }
    Json(rows)
}

#[derive(Deserialize)]
struct HashQuery {
    hash: String,
}

async fn torrent_properties(
    State(state): State<QbitState>,
    Query(query): Query<HashQuery>,
) -> Response {
    let Some(job) = find_by_hash(&state.engine, &query.hash).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let torrent = job.torrent.unwrap();
    let save_path = if torrent.payload_root.as_os_str().is_empty() {
        state.save_path
    } else {
        torrent.payload_root
    };
    Json(json!({
        "save_path": save_path,
        "seeding_time": torrent.seeding_seconds,
        "total_uploaded": torrent.uploaded_bytes,
        "addition_date": 0,
        "completion_date": torrent.ready_at_unix.unwrap_or(-1),
    }))
    .into_response()
}

async fn torrent_files(State(state): State<QbitState>, Query(query): Query<HashQuery>) -> Response {
    let Some(job) = find_by_hash(&state.engine, &query.hash).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let torrent = job.torrent.unwrap();
    let ready = torrent.ready_at_unix.is_some();
    Json(
        torrent
            .files
            .into_iter()
            .enumerate()
            .map(|(index, file)| {
                json!({
                    "index": index,
                    "name": file.path,
                    "size": file.length,
                    "progress": if file.length == 0 { 0.0 } else { file.downloaded_bytes as f64 / file.length as f64 },
                    "priority": if file.selected { 1 } else { 0 },
                    "is_seed": ready,
                })
            })
            .collect::<Vec<_>>(),
    )
    .into_response()
}

async fn torrent_add(State(state): State<QbitState>, headers: HeaderMap, body: Bytes) -> Response {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let parts = if content_type.starts_with("multipart/form-data") {
        let Some(boundary) = content_type
            .split("boundary=")
            .nth(1)
            .map(|value| value.trim_matches('"'))
        else {
            return (StatusCode::BAD_REQUEST, "Fails.").into_response();
        };
        multipart_fields(&body, boundary)
    } else {
        form_fields(&body)
            .into_iter()
            .map(|(name, value)| (name, value.into_bytes()))
            .collect()
    };
    let text = |name: &str| {
        parts
            .get(name)
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .unwrap_or("")
            .trim()
    };
    let tag_value = match parts.get("tags") {
        None => "",
        Some(bytes) => match std::str::from_utf8(bytes) {
            Ok(value) => value,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    "Invalid tags: tag data must be UTF-8.",
                )
                    .into_response();
            }
        },
    };
    let torrent_tags = match parse_qbit_tags(tag_value) {
        Ok(tags) => tags,
        Err(message) => {
            return (StatusCode::BAD_REQUEST, message).into_response();
        }
    };
    let opts = AddOpts {
        category: nonempty(text("category")),
        paused: matches!(text("paused"), "true" | "1"),
        seed_ratio_limit: add_ratio_limit(text("ratioLimit")),
        seed_time_limit_secs: add_time_limit(text("seedingTimeLimit")),
        client: headers
            .get(header::USER_AGENT)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string),
        torrent_tags,
        ..Default::default()
    };
    let result = if let Some(bytes) = parts.get("torrents") {
        state.torrent.admit_raw(bytes.clone(), opts).await
    } else if let Some(uri) = nonempty(text("urls")) {
        let source = if uri.starts_with("magnet:") {
            TorrentSource::Magnet
        } else {
            TorrentSource::Url
        };
        state.torrent.admit_source(source, uri, opts).await
    } else {
        return (StatusCode::BAD_REQUEST, "Fails.").into_response();
    };
    match result {
        Ok(_) => "Ok.".into_response(),
        Err(error) => {
            tracing::debug!(error = %error, "qBittorrent-compatible add rejected");
            (StatusCode::BAD_REQUEST, "Fails.").into_response()
        }
    }
}

fn parse_qbit_tags(value: &str) -> Result<Vec<String>, &'static str> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(Vec::new());
    }
    if value.len() > MAX_QBIT_TAGS_BYTES {
        return Err("Invalid tags: total tag data exceeds 1024 bytes.");
    }
    let mut tags = Vec::new();
    let mut transfer_tag_seen = false;
    for part in value.split(',') {
        let tag = part.trim();
        if tag.is_empty() {
            return Err("Invalid tags: empty entries are not supported.");
        }
        if tag.len() > MAX_QBIT_TAG_BYTES {
            return Err("Invalid tags: each tag must be at most 256 bytes.");
        }
        if tag.chars().any(char::is_control) {
            return Err("Invalid tags: control characters are not supported.");
        }
        if tags.iter().any(|existing| existing == tag) {
            return Err("Invalid tags: duplicate entries are not supported.");
        }
        if let Some(token) = tag.strip_prefix(TRANSFER_TAG_PREFIX) {
            if transfer_tag_seen || !valid_transfer_token(token) {
                return Err("Invalid tags: transfer tag must contain one valid token.");
            }
            transfer_tag_seen = true;
        }
        tags.push(tag.to_string());
        if tags.len() > MAX_QBIT_TAGS {
            return Err("Invalid tags: at most 32 tags are supported.");
        }
    }
    Ok(tags)
}

fn valid_transfer_token(token: &str) -> bool {
    !token.is_empty()
        && token.len() <= 128
        && token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn qbit_tags_from_params(params: &[(String, String)]) -> String {
    params
        .iter()
        .rev()
        .find(|(key, _)| key == nzbd_engine::QBIT_TAGS_PARAM)
        .map(|(_, value)| value.clone())
        .unwrap_or_default()
}

async fn torrent_delete(State(state): State<QbitState>, body: Bytes) -> Response {
    let form = form_fields(&body);
    let delete_files = form
        .get("deleteFiles")
        .is_some_and(|value| matches!(value.as_str(), "true" | "1"));
    mutate_hashes(&state, form.get("hashes"), |engine, id| async move {
        engine.delete_job(id, delete_files).await
    })
    .await
}

async fn set_category(State(state): State<QbitState>, body: Bytes) -> Response {
    let form = form_fields(&body);
    let category = form.get("category").and_then(|value| nonempty(value));
    mutate_hashes(&state, form.get("hashes"), move |engine, id| {
        let category = category.clone();
        async move { engine.set_category(id, category).await }
    })
    .await
}

async fn categories(State(state): State<QbitState>) -> Json<Value> {
    let store = state.categories.lock().unwrap();
    let mut rows = serde_json::Map::new();
    for (name, path) in &store.configured {
        rows.insert(name.clone(), json!({"name":name,"savePath":path}));
    }
    for (name, category) in &store.overlay {
        rows.entry(name.clone())
            .or_insert_with(|| json!({"name":name,"savePath":category.save_path}));
    }
    Json(Value::Object(rows))
}

async fn create_category(State(state): State<QbitState>, body: Bytes) -> Response {
    let form = form_fields(&body);
    let Some(name) = form.get("category").and_then(|value| valid_category(value)) else {
        return (StatusCode::CONFLICT, "Fails.").into_response();
    };
    let mut store = state.categories.lock().unwrap();
    if store
        .configured
        .keys()
        .chain(store.overlay.keys())
        .any(|existing| existing.eq_ignore_ascii_case(&name))
    {
        return (StatusCode::CONFLICT, "Fails.").into_response();
    }
    let save_path = state.save_path.join(&name);
    if let Err(error) = std::fs::create_dir_all(&save_path) {
        tracing::warn!(error = %error, "could not create qBittorrent category directory");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let save_path = match std::fs::canonicalize(save_path) {
        Ok(path) => path,
        Err(error) => {
            tracing::warn!(error = %error, "could not canonicalize qBittorrent category directory");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let mut candidate = store.clone();
    candidate.overlay.insert(
        name.clone(),
        OverlayCategory {
            save_path: save_path.clone(),
        },
    );
    match persist_categories(&candidate) {
        Ok(()) => {
            store.overlay = candidate.overlay;
            state
                .torrent
                .register_category_payload_root(name, save_path);
            "Ok.".into_response()
        }
        Err(error) => {
            tracing::warn!(error = %error, "could not persist qBittorrent category overlay");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn set_share_limits(State(state): State<QbitState>, body: Bytes) -> Response {
    let form = form_fields(&body);
    let ratio = share_ratio_limit(form.get("ratioLimit"), state.global_seed_ratio);
    let time = share_time_limit(form.get("seedingTimeLimit"), state.global_seed_minutes);
    mutate_hashes(&state, form.get("hashes"), move |engine, id| async move {
        engine
            .set_torrent_seed_policy(
                id,
                SeedPolicy {
                    stop_on_complete: false,
                    ratio_limit: ratio,
                    time_limit_secs: time,
                },
            )
            .await
    })
    .await
}

async fn top_priority(State(state): State<QbitState>, body: Bytes) -> Response {
    let form = form_fields(&body);
    mutate_hashes(&state, form.get("hashes"), |engine, id| async move {
        engine.move_job(id, MoveOp::Top).await
    })
    .await
}

async fn set_force_start(State(state): State<QbitState>, body: Bytes) -> Response {
    let form = form_fields(&body);
    let enabled = form
        .get("value")
        .is_some_and(|value| matches!(value.as_str(), "true" | "1"));
    mutate_hashes(&state, form.get("hashes"), move |engine, id| async move {
        engine.set_priority(id, if enabled { 900 } else { 0 }).await
    })
    .await
}

async fn mutate_hashes<F, Fut>(state: &QbitState, hashes: Option<&String>, op: F) -> Response
where
    F: Fn(EngineHandle, JobId) -> Fut,
    Fut: std::future::Future<Output = Result<bool, nzbd_engine::EngineError>>,
{
    let Some(hashes) = hashes else {
        return (StatusCode::BAD_REQUEST, "Fails.").into_response();
    };
    let ids = ids_for_hashes(&state.engine, &split_hashes(hashes)).await;
    if ids.is_empty() {
        return (StatusCode::NOT_FOUND, "Fails.").into_response();
    }
    for id in ids {
        match op(state.engine.clone(), id).await {
            Ok(true) => {}
            _ => return (StatusCode::CONFLICT, "Fails.").into_response(),
        }
    }
    "Ok.".into_response()
}

async fn find_by_hash(engine: &EngineHandle, hash: &str) -> Option<nzbd_types::Job> {
    let ids = ids_for_hashes(engine, &[hash.to_string()]).await;
    engine.export_job(*ids.first()?).await.ok().flatten()
}

async fn ids_for_hashes(engine: &EngineHandle, hashes: &[String]) -> Vec<JobId> {
    let all = hashes.iter().any(|hash| hash == "all");
    let mut ids = Vec::new();
    for summary in engine
        .snapshot()
        .jobs
        .iter()
        .filter(|job| job.kind == JobKind::Torrent)
    {
        if let Ok(Some(job)) = engine.export_job(summary.id).await {
            if let Some(torrent) = job.torrent {
                if all
                    || hashes
                        .iter()
                        .any(|hash| hash.eq_ignore_ascii_case(&torrent.info_hash_v1))
                {
                    ids.push(job.id);
                }
            }
        }
    }
    ids
}

fn qbit_state(phase: TorrentPhase, peers: u32, upload_bps: u64) -> &'static str {
    match phase {
        TorrentPhase::FetchingSource | TorrentPhase::FetchingMetadata => "metaDL",
        TorrentPhase::Queued => "queuedDL",
        TorrentPhase::Checking => "checkingDL",
        TorrentPhase::Downloading if peers > 0 => "downloading",
        TorrentPhase::Downloading => "stalledDL",
        TorrentPhase::PausedDownload => "pausedDL",
        TorrentPhase::Seeding if upload_bps > 0 => "uploading",
        TorrentPhase::Seeding => "stalledUP",
        TorrentPhase::PausedSeed => "pausedUP",
        TorrentPhase::MissingFiles => "missingFiles",
        TorrentPhase::Failed => "error",
    }
}

fn form_fields(body: &[u8]) -> HashMap<String, String> {
    url::form_urlencoded::parse(body).into_owned().collect()
}

fn multipart_fields(body: &[u8], boundary: &str) -> HashMap<String, Vec<u8>> {
    let marker = format!("--{boundary}").into_bytes();
    let mut result = HashMap::new();
    let mut cursor = 0;
    while let Some(start) =
        find_bytes(&body[cursor..], &marker).map(|at| cursor + at + marker.len())
    {
        let mut part_start = start;
        if body.get(part_start..part_start + 2) == Some(b"--") {
            break;
        }
        if body.get(part_start..part_start + 2) == Some(b"\r\n") {
            part_start += 2;
        }
        let Some(header_end) =
            find_bytes(&body[part_start..], b"\r\n\r\n").map(|at| part_start + at)
        else {
            break;
        };
        let headers = String::from_utf8_lossy(&body[part_start..header_end]);
        let name = headers.split(';').find_map(|piece| {
            piece
                .trim()
                .strip_prefix("name=\"")
                .and_then(|value| value.strip_suffix('"'))
        });
        let data_start = header_end + 4;
        let Some(next) = find_bytes(&body[data_start..], &marker).map(|at| data_start + at) else {
            break;
        };
        let data_end = next.saturating_sub(2);
        if let Some(name) = name {
            result.insert(name.to_string(), body[data_start..data_end].to_vec());
        }
        cursor = next;
    }
    result
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    (!needle.is_empty())
        .then(|| {
            haystack
                .windows(needle.len())
                .position(|window| window == needle)
        })
        .flatten()
}

fn split_hashes(value: &str) -> Vec<String> {
    value
        .split('|')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn nonempty(value: &str) -> Option<String> {
    (!value.trim().is_empty()).then(|| value.trim().to_string())
}

/// qBittorrent uses -2 for "inherit the global limit" and -1 for
/// "unlimited". At admission, absence represents inheritance and an explicit
/// zero is nzbd's unlimited sentinel.
fn add_ratio_limit(value: &str) -> Option<f64> {
    match value.trim().parse::<f64>().ok()? {
        -2.0 => None,
        -1.0 => Some(0.0),
        value if value >= 0.0 && value.is_finite() => Some(value),
        _ => None,
    }
}

fn add_time_limit(value: &str) -> Option<u64> {
    match value.trim().parse::<i64>().ok()? {
        -2 => None,
        -1 => Some(0),
        minutes if minutes >= 0 => Some((minutes as u64).saturating_mul(60)),
        _ => None,
    }
}

/// Unlike admission, setShareLimits must resolve -2 immediately because it
/// replaces the job's already-materialized policy.
fn share_ratio_limit(value: Option<&String>, global: f64) -> Option<f64> {
    match value.and_then(|value| value.trim().parse::<f64>().ok()) {
        Some(-2.0) => (global > 0.0 && global.is_finite()).then_some(global),
        Some(value) if value > 0.0 && value.is_finite() => Some(value),
        _ => None,
    }
}

fn share_time_limit(value: Option<&String>, global_minutes: u64) -> Option<u64> {
    match value.and_then(|value| value.trim().parse::<i64>().ok()) {
        Some(-2) => (global_minutes > 0).then(|| global_minutes.saturating_mul(60)),
        Some(minutes) if minutes > 0 => Some((minutes as u64).saturating_mul(60)),
        _ => None,
    }
}

fn valid_category(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()
        && value.len() <= 128
        && !value.contains(['/', '\\'])
        && value != "."
        && value != ".."
        && !value.chars().any(char::is_control))
    .then(|| value.to_string())
}

fn note_client(state: &QbitState, headers: &HeaderMap, method: &str, path: &str) {
    let Some(clients) = &state.clients else {
        return;
    };
    let agent = headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok());
    clients.note(agent, &format!("{method} {path}"), unix_now());
}

fn persist_categories(store: &CategoryStore) -> std::io::Result<()> {
    if let Some(parent) = store.path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temp = store.path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(&store.overlay).map_err(std::io::Error::other)?;
    let mut file = std::fs::File::create(&temp)?;
    use std::io::Write;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(&temp, &store.path)?;
    if let Some(parent) = store.path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use nzbd_engine::{Engine, EngineConfig, EngineHandle, Tuning};
    use nzbd_torrent::{TorrentSession, TorrentSessionConfig};
    use tower::ServiceExt;

    fn multipart_add_request(bytes: &[u8], tags: &str) -> axum::http::Request<Body> {
        multipart_add_request_with_tag_bytes(bytes, tags.as_bytes())
    }

    fn multipart_add_request_with_tag_bytes(
        bytes: &[u8],
        tag_bytes: &[u8],
    ) -> axum::http::Request<Body> {
        let boundary = "qbit-test-boundary";
        let mut body =
            format!("--{boundary}\r\nContent-Disposition: form-data; name=\"tags\"\r\n\r\n")
                .into_bytes();
        body.extend_from_slice(tag_bytes);
        body.extend_from_slice(
            format!("\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"torrents\"; filename=\"input.torrent\"\r\nContent-Type: application/x-bittorrent\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(bytes);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        axum::http::Request::builder()
            .method("POST")
            .uri("/api/v2/torrents/add")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .header(header::AUTHORIZATION, "Bearer qbit-test-token")
            .body(Body::from(body))
            .unwrap()
    }

    fn info_request() -> axum::http::Request<Body> {
        axum::http::Request::builder()
            .uri("/api/v2/torrents/info?category=all")
            .header(header::AUTHORIZATION, "Bearer qbit-test-token")
            .body(Body::empty())
            .unwrap()
    }

    async fn qbit_test_state(root: &std::path::Path, engine: EngineHandle) -> QbitState {
        let state_dir = root.join("state");
        let save_path = root.join("payload");
        std::fs::create_dir_all(&save_path).unwrap();
        let session = TorrentSession::start(save_path.clone(), TorrentSessionConfig::default())
            .await
            .unwrap();
        let torrent =
            TorrentAdmissionService::new(engine.clone(), session, state_dir, false, false);
        QbitState::new(
            engine,
            torrent,
            QbitAuth {
                username: String::new(),
                password: None,
                token: Some("qbit-test-token".into()),
            },
            root.join("qbit-state"),
            save_path,
            BTreeMap::new(),
            false,
            true,
            0.0,
            0,
            None,
        )
    }

    fn engine_config(root: &std::path::Path) -> EngineConfig {
        EngineConfig::single_node(
            vec![],
            root.join("state"),
            root.join("completed"),
            Tuning::default(),
            None,
        )
    }

    #[tokio::test]
    async fn qbit_transfer_tag_survives_duplicate_admission_and_snapshot_restart() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::spawn(engine_config(root.path())).await.unwrap();
        let state = qbit_test_state(root.path(), engine.clone()).await;
        let app = router(state);
        let torrent = include_bytes!("../../../fuzz/seeds/metainfo_preflight/valid-v1.torrent");

        let invalid = app
            .clone()
            .oneshot(multipart_add_request(
                torrent,
                "monarr-transfer-one,monarr-transfer-two",
            ))
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        let invalid_utf8 = app
            .clone()
            .oneshot(multipart_add_request_with_tag_bytes(torrent, b"\xff"))
            .await
            .unwrap();
        assert_eq!(invalid_utf8.status(), StatusCode::BAD_REQUEST);
        let empty = app.clone().oneshot(info_request()).await.unwrap();
        let empty: Value =
            serde_json::from_slice(&empty.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert!(empty.as_array().unwrap().is_empty());

        let first = app
            .clone()
            .oneshot(multipart_add_request(
                torrent,
                "tv, monarr-transfer-first-token",
            ))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let duplicate = app
            .clone()
            .oneshot(multipart_add_request(
                torrent,
                "tv,monarr-transfer-second-token",
            ))
            .await
            .unwrap();
        assert_eq!(duplicate.status(), StatusCode::OK);

        let before_restart = app.clone().oneshot(info_request()).await.unwrap();
        let before_restart: Value = serde_json::from_slice(
            &before_restart
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes(),
        )
        .unwrap();
        assert_eq!(before_restart[0]["tags"], "tv,monarr-transfer-first-token");

        engine.shutdown().await;
        drop(app);

        let restarted = Engine::spawn(engine_config(root.path())).await.unwrap();
        let state = qbit_test_state(root.path(), restarted.clone()).await;
        let after_restart = router(state).oneshot(info_request()).await.unwrap();
        let after_restart: Value = serde_json::from_slice(
            &after_restart
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes(),
        )
        .unwrap();
        assert_eq!(after_restart[0]["tags"], "tv,monarr-transfer-first-token");
        restarted.shutdown().await;
    }

    #[test]
    fn phase_projection_matches_arr_contract() {
        assert_eq!(qbit_state(TorrentPhase::FetchingSource, 0, 0), "metaDL");
        assert_eq!(qbit_state(TorrentPhase::Downloading, 0, 0), "stalledDL");
        assert_eq!(qbit_state(TorrentPhase::Downloading, 1, 0), "downloading");
        assert_eq!(qbit_state(TorrentPhase::Seeding, 0, 0), "stalledUP");
        assert_eq!(qbit_state(TorrentPhase::Seeding, 0, 1), "uploading");
        assert_eq!(qbit_state(TorrentPhase::PausedSeed, 0, 0), "pausedUP");
    }

    #[test]
    fn multipart_keeps_binary_torrent_bytes() {
        let boundary = "nzbd-boundary";
        let binary = b"d4:infod4:name4:testee\0\xff";
        let mut body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"category\"\r\n\r\ntv\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"torrents\"; filename=\"x.torrent\"\r\nContent-Type: application/x-bittorrent\r\n\r\n"
        )
        .into_bytes();
        body.extend_from_slice(binary);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        let fields = multipart_fields(&body, boundary);
        assert_eq!(fields.get("category").unwrap(), b"tv");
        assert_eq!(fields.get("torrents").unwrap(), binary);
    }

    #[test]
    fn qbit_transfer_tags_are_trimmed_bounded_and_projected_back() {
        let tags = parse_qbit_tags(" movies , monarr-transfer-a_1-B2 ").unwrap();
        assert_eq!(tags, ["movies", "monarr-transfer-a_1-B2"]);
        assert_eq!(
            qbit_tags_from_params(&[(nzbd_engine::QBIT_TAGS_PARAM.to_string(), tags.join(","),)]),
            "movies,monarr-transfer-a_1-B2"
        );
        assert!(parse_qbit_tags("").unwrap().is_empty());
        assert!(parse_qbit_tags(" , ").is_err());
        assert!(parse_qbit_tags("monarr-transfer-one,monarr-transfer-two").is_err());
        assert!(parse_qbit_tags(&format!("monarr-transfer-{}", "t".repeat(129))).is_err());
        assert!(parse_qbit_tags(&"x".repeat(MAX_QBIT_TAGS_BYTES + 1)).is_err());
        assert!(parse_qbit_tags("ok\ninvalid").is_err());
    }

    #[test]
    fn category_names_reject_paths_and_controls() {
        assert_eq!(valid_category("tv"), Some("tv".into()));
        assert_eq!(valid_category("../tv"), None);
        assert_eq!(valid_category("tv/movies"), None);
        assert_eq!(valid_category("tv\n"), Some("tv".into()));
        assert_eq!(valid_category("tv\u{0000}"), None);
    }

    #[test]
    fn qbit_share_limit_sentinels_preserve_inherit_and_unlimited() {
        assert_eq!(add_ratio_limit("-2"), None);
        assert_eq!(add_ratio_limit("-1"), Some(0.0));
        assert_eq!(add_ratio_limit("1.5"), Some(1.5));
        assert_eq!(add_time_limit("-2"), None);
        assert_eq!(add_time_limit("-1"), Some(0));
        assert_eq!(add_time_limit("5"), Some(300));

        let inherit = "-2".to_string();
        let unlimited = "-1".to_string();
        assert_eq!(share_ratio_limit(Some(&inherit), 1.25), Some(1.25));
        assert_eq!(share_ratio_limit(Some(&unlimited), 1.25), None);
        assert_eq!(share_time_limit(Some(&inherit), 45), Some(2_700));
        assert_eq!(share_time_limit(Some(&unlimited), 45), None);
    }
}
