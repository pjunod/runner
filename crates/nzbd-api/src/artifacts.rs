//! File lifecycle API. Reads use the cached inventory; scans and copies are jobs.
use crate::{error, ApiState};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use nzbd_state::artifacts::{Error, Inventory, ListFilter, ListQuery, ListSort, Receipt, Settings};
use serde::Deserialize;
use serde_json::json;
use std::{path::PathBuf, sync::Arc};

pub fn router() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/artifacts", get(list))
        .route(
            "/api/v1/artifacts/settings",
            get(settings).put(put_settings),
        )
        .route("/api/v1/artifacts/scan", post(scan))
        .route(
            "/api/v1/artifacts/retention-preview",
            post(preview_retention),
        )
        .route(
            "/api/v1/artifacts/retention-apply/{id}",
            post(apply_retention),
        )
        .route("/api/v1/artifacts/{id}", get(detail))
        .route("/api/v1/artifacts/{id}/files", get(files))
        .route("/api/v1/artifacts/{id}/events", get(events))
        .route("/api/v1/artifacts/{id}/inspect", post(inspect))
        .route("/api/v1/artifacts/{id}/adopt", post(adopt))
        .route(
            "/api/v1/artifacts/{id}/release-review",
            post(release_review),
        )
        .route("/api/v1/artifacts/{id}/retention", post(retention))
        .route("/api/v1/artifacts/{id}/delete", post(delete))
        .route("/api/v1/artifact-operations/{id}", get(operation))
        .route("/api/v1/artifact-operations/{id}/cancel", post(cancel))
        .route("/api/v1/artifacts/{id}/recoveries", post(stage))
        .route("/api/v1/recoveries", get(recoveries))
        .route("/api/v1/recoveries/{id}", get(recovery))
        .route("/api/v1/recoveries/{id}/claim", post(claim))
        .route("/api/v1/recoveries/{id}/receipt", post(receipt))
        .route("/api/v1/recoveries/{id}/cancel", post(cancel_recovery))
        .route(
            "/api/v1/recoveries/{id}/delete-receipted-source",
            post(prune_source),
        )
        .route("/api/v1/recoveries/{id}/cancel-ack", post(cancel_ack))
}
fn failure(e: Error) -> Response {
    let status = match e {
        Error::NotFound => StatusCode::NOT_FOUND,
        Error::Conflict(_) => StatusCode::CONFLICT,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    error(status, &e.to_string())
}
async fn work<T: serde::Serialize + Send + 'static>(
    f: impl FnOnce() -> nzbd_state::artifacts::Result<T> + Send + 'static,
) -> Response {
    match tokio::task::spawn_blocking(f).await {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(e)) => failure(e),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
}
#[derive(Default, Deserialize)]
struct Page {
    #[serde(default)]
    include_terminal: bool,
    #[serde(default)]
    offset: usize,
    #[serde(default)]
    after: i64,
    /// Page size, 1..=200 (default 50).
    limit: Option<usize>,
    /// `live` (default) · `attention` · `owned` · `cleared` · `all`.
    filter: Option<ListFilter>,
    /// `updated` (default) · `size` · `files` · `name`.
    sort: Option<ListSort>,
    /// Case-insensitive substring of the folder path.
    #[serde(default)]
    q: String,
}
/// Run whatever is queued right now, off the request thread. The 30 s
/// maintenance tick remains the safety net; a click should not wait for it.
fn kick(db: Arc<Inventory>) {
    tokio::task::spawn_blocking(move || {
        if let Err(e) = db.run_tasks() {
            tracing::warn!(error = %e, "file inventory task run failed");
        }
    });
}
/// A delete or prune admitted with an undo window is runnable only once the
/// window closes; run it then, not at the next tick up to 30 s later.
fn kick_after(db: Arc<Inventory>, seconds: u64) {
    tokio::spawn(async move {
        tokio::time::sleep(
            std::time::Duration::from_secs(seconds) + std::time::Duration::from_millis(250),
        )
        .await;
        let _ = tokio::task::spawn_blocking(move || {
            if let Err(e) = db.run_due_deletes() {
                tracing::warn!(error = %e, "file inventory delete run failed");
            }
            if let Err(e) = db.run_tasks() {
                tracing::warn!(error = %e, "file inventory task run failed");
            }
        })
        .await;
    });
}
async fn list(State(st): State<ApiState>, Query(p): Query<Page>) -> Response {
    let db = st.engine.artifacts();
    let query = ListQuery {
        offset: p.offset,
        limit: p.limit.unwrap_or(50).clamp(1, 200),
        // `include_terminal` is the pre-filter spelling; an explicit filter wins.
        filter: p.filter.unwrap_or(if p.include_terminal {
            ListFilter::All
        } else {
            ListFilter::Live
        }),
        sort: p.sort.unwrap_or_default(),
        q: p.q,
    };
    work(move || {
        let page = db.list_page(&query)?;
        let now = nzbd_state::artifacts::now();
        let entries: Vec<_> = page
            .rows
            .into_iter()
            .map(|row| {
                json!({
                    "files": row.files,
                    "bytes": row.bytes,
                    "measured": row.artifact.measured(),
                    "earliest_expiry": row.artifact.earliest_expiry(now),
                    "artifact": row.artifact,
                })
            })
            .collect();
        Ok(json!({
            "entries": entries,
            "offset": query.offset,
            "limit": query.limit,
            "total": page.total,
            "counts": page.counts,
            "filter": query.filter,
            "sort": query.sort,
            "discovery": db.discovery_status()?,
        }))
    })
    .await
}
async fn detail(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    work(move || {
        let mut a = db.get(&id)?;
        a.files.clear();
        Ok(a)
    })
    .await
}
async fn files(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    Query(p): Query<Page>,
) -> Response {
    let db = st.engine.artifacts();
    work(move || {
        let a = db.get(&id)?;
        let (files, bytes) = a.summary();
        Ok(json!({
            "revision": a.revision,
            // `total` counts manifest entries (directories included) and pages
            // the list; `files`/`bytes` are the payload the row reports.
            "total": a.files.len(),
            "file_count": files,
            "bytes": bytes,
            "measured": a.measured(),
            "files": a.files.into_iter().skip(p.offset).take(200).collect::<Vec<_>>(),
        }))
    })
    .await
}
async fn events(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    Query(p): Query<Page>,
) -> Response {
    let db = st.engine.artifacts();
    work(move || db.events(&id, p.after)).await
}
async fn operation(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    work(move || db.operation(&id)).await
}
async fn cancel(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    work(move || db.cancel_delete(&id)).await
}
async fn inspect(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    match db.submit_task("inspect", &id, "", json!({})) {
        Ok(op) => {
            kick(db);
            (StatusCode::ACCEPTED, Json(op)).into_response()
        }
        Err(e) => failure(e),
    }
}
#[derive(Deserialize)]
struct Revision {
    revision: u64,
}
async fn adopt(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<Revision>,
) -> Response {
    if let Err(response) = validate_artifact_role(&st, &id) {
        return response;
    }
    let db = st.engine.artifacts();
    work(move || db.adopt(&id, body.revision)).await
}
async fn release_review(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<Revision>,
) -> Response {
    let db = st.engine.artifacts();
    work(move || db.release_review(&id, body.revision)).await
}
#[derive(Deserialize)]
struct Retention {
    revision: u64,
    keep: bool,
    seconds: Option<u64>,
}
async fn retention(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<Retention>,
) -> Response {
    let db = st.engine.artifacts();
    work(move || db.retention(&id, body.revision, body.keep, body.seconds)).await
}
#[derive(Deserialize)]
struct Delete {
    revision: u64,
    idempotency_key: String,
    #[serde(default)]
    undo_seconds: u64,
}
async fn delete(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<Delete>,
) -> Response {
    if let Err(response) = validate_artifact_role(&st, &id) {
        return response;
    }
    let db = st.engine.artifacts();
    let runner = db.clone();
    let undo = body.undo_seconds;
    match tokio::task::spawn_blocking(move || {
        db.request_delete(&id, body.revision, &body.idempotency_key, body.undo_seconds)
    })
    .await
    {
        Ok(Ok(op)) => {
            kick_after(runner, undo);
            (StatusCode::ACCEPTED, Json(op)).into_response()
        }
        Ok(Err(e)) => failure(e),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
}

#[allow(clippy::result_large_err)] // Axum error responses are returned directly.
fn validate_artifact_role(st: &ApiState, id: &str) -> Result<(), Response> {
    let artifact = st.engine.artifacts().get(id).map_err(failure)?;
    if let Some(cfg) = config(st) {
        if cfg
            .storage_roots()
            .iter()
            .any(|r| r.path == artifact.path || r.path.starts_with(&artifact.path))
        {
            return Err(error(
                StatusCode::CONFLICT,
                "artifact overlaps a configured directory role",
            ));
        }
    }
    Ok(())
}
fn config(st: &ApiState) -> Option<nzbd_config::Config> {
    st.setup.as_ref().map(|s| s.current.lock().unwrap().clone())
}
async fn settings(State(st): State<ApiState>) -> Response {
    let db = st.engine.artifacts();
    let cfg = config(&st);
    work(move||{let mut s=db.settings()?;let credential=!s.consumer_token.is_empty();s.consumer_token.clear();let mut advisory=Vec::new();advisory.push(json!({"requirement":"Consumer credential for claims and receipts","met":credential}));advisory.push(json!({"requirement":"Dedicated published recovery directory mounted read-only in Curator","met":null,"detail":"Configure /recovery separately from completed downloads; verify on the Curator host."}));if let Some(cfg)=cfg{advisory.push(json!({"requirement":"Single authoritative lifecycle writer","met":!cfg.cluster.enabled}));if s.recovery_root.as_os_str().is_empty(){s.recovery_root=nzbd_config::expand_home(&cfg.paths.main_dir).join("recovery");}}Ok(json!({"settings":s,"advisory":advisory,"installation":db.installation,"discovery":db.discovery_status()?}))}).await
}
#[derive(Deserialize)]
struct SettingsUpdate {
    enabled: bool,
    failed_retention_days: u32,
    recovery_root: PathBuf,
    consumer_token: Option<String>,
}
async fn put_settings(State(st): State<ApiState>, Json(body): Json<SettingsUpdate>) -> Response {
    let db = st.engine.artifacts();
    work(move || {
        let current = db.settings()?;
        let next = Settings {
            enabled: body.enabled,
            failed_retention_days: body.failed_retention_days,
            recovery_root: body.recovery_root,
            consumer_token: body
                .consumer_token
                .filter(|s| !s.is_empty())
                .unwrap_or(current.consumer_token),
        };
        db.set_settings(&next)?;
        Ok(json!({"ok":true,"enabled":next.enabled}))
    })
    .await
}
pub(crate) fn scan_request(
    cfg: &nzbd_config::Config,
    db: &Inventory,
    active: Vec<PathBuf>,
) -> serde_json::Value {
    let mut roots = vec![
        nzbd_config::expand_home(&cfg.paths.main_dir),
        cfg.dest_dir(),
        cfg.post
            .failed_dir
            .as_ref()
            .map(|p| nzbd_config::expand_home(p))
            .unwrap_or_else(|| nzbd_config::expand_home(&cfg.paths.main_dir).join("failed")),
    ];
    if let Some(p) = &cfg.paths.inter_dir {
        roots.push(nzbd_config::expand_home(p));
    }
    for category in &cfg.categories {
        if let Some(p) = &category.dest_dir {
            roots.push(nzbd_config::expand_home(p));
        }
    }
    roots.sort();
    roots.dedup();
    let mut excluded: Vec<_> = cfg.storage_roots().into_iter().map(|r| r.path).collect();
    excluded.push(nzbd_config::expand_home(&cfg.paths.main_dir).join("recovery"));
    if let Ok(settings) = db.settings() {
        if !settings.recovery_root.as_os_str().is_empty() {
            excluded.push(settings.recovery_root);
        }
    }
    json!({"roots":roots,"excluded":excluded,"active":active})
}
async fn scan(State(st): State<ApiState>) -> Response {
    let Some(cfg) = config(&st) else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "configuration unavailable");
    };
    let db = st.engine.artifacts();
    let mut active = Vec::new();
    for summary in &st.engine.snapshot().jobs {
        if let Ok(Some(job)) = st.engine.export_job(summary.id).await {
            active.push(cfg.dest_dir().join(nzbd_engine::queue::job_dir_name(&job)));
        }
    }
    let request = scan_request(&cfg, &db, active);
    match db.submit_task("scan", "installation", "", request) {
        Ok(op) => {
            kick(db);
            (StatusCode::ACCEPTED, Json(op)).into_response()
        }
        Err(e) => failure(e),
    }
}
#[derive(Deserialize)]
struct Stage {
    #[serde(default)]
    preview: bool,
    revision: u64,
    idempotency_key: String,
    files: Vec<String>,
}
async fn stage(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<Stage>,
) -> Response {
    let db = st.engine.artifacts();
    let Some(cfg) = config(&st) else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "configuration unavailable");
    };
    let settings = match db.settings() {
        Ok(s) => s,
        Err(e) => return failure(e),
    };
    let root = if settings.recovery_root.as_os_str().is_empty() {
        nzbd_config::expand_home(&cfg.paths.main_dir).join("recovery")
    } else {
        settings.recovery_root
    };
    if cfg
        .storage_roots()
        .iter()
        .any(|r| r.label != "working" && (root.starts_with(&r.path) || r.path.starts_with(&root)))
    {
        return error(
            StatusCode::CONFLICT,
            "recovery root overlaps a download, state, watch or category role",
        );
    }
    if body.preview {
        return work(move || db.preview_recovery(&id, body.revision, &body.files, &root)).await;
    }
    match db.submit_task(
        "stage",
        &id,
        &body.idempotency_key,
        json!({"revision":body.revision,"files":body.files,"root":root}),
    ) {
        Ok(op) => (StatusCode::ACCEPTED, Json(op)).into_response(),
        Err(e) => failure(e),
    }
}
async fn recoveries(State(st): State<ApiState>, Query(p): Query<Page>) -> Response {
    let db = st.engine.artifacts();
    work(move || db.recoveries_visible(p.offset, p.include_terminal)).await
}
async fn recovery(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    work(move || db.recovery(&id)).await
}
#[allow(clippy::result_large_err)] // Axum error responses are returned directly.
fn consumer(db: &Arc<Inventory>, headers: &HeaderMap) -> Result<String, Response> {
    let s = db.settings().map_err(failure)?;
    let supplied = headers
        .get("x-recovery-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if s.consumer_token.is_empty() || supplied.as_bytes() != s.consumer_token.as_bytes() {
        return Err(error(
            StatusCode::UNAUTHORIZED,
            "recovery consumer credential required",
        ));
    }
    Ok("curator".into())
}
#[derive(Deserialize)]
struct Claim {
    import_id: String,
    manifest_digest: String,
}
async fn claim(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Claim>,
) -> Response {
    let db = st.engine.artifacts();
    let who = match consumer(&db, &headers) {
        Ok(c) => c,
        Err(r) => return r,
    };
    work(move || db.claim_recovery(&id, &who, &body.import_id, &body.manifest_digest)).await
}
async fn receipt(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Receipt>,
) -> Response {
    let db = st.engine.artifacts();
    let who = match consumer(&db, &headers) {
        Ok(c) => c,
        Err(r) => return r,
    };
    work(move || db.recovery_receipt(&id, &who, body)).await
}
async fn cancel_recovery(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    work(move || db.cancel_recovery(&id, None, false)).await
}
async fn cancel_ack(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let db = st.engine.artifacts();
    let who = match consumer(&db, &headers) {
        Ok(c) => c,
        Err(r) => return r,
    };
    work(move || db.cancel_recovery(&id, Some(&who), true)).await
}

#[derive(Deserialize)]
struct PolicyDays {
    days: u32,
}
async fn preview_retention(State(st): State<ApiState>, Json(body): Json<PolicyDays>) -> Response {
    let db = st.engine.artifacts();
    work(move || db.preview_retention(body.days)).await
}
async fn apply_retention(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    work(move || db.apply_retention(&id)).await
}

async fn prune_source(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    let r = match db.recovery(&id) {
        Ok(r) => r,
        Err(e) => return failure(e),
    };
    if let Err(response) = validate_artifact_role(&st, &r.artifact) {
        return response;
    }
    match db.submit_task("prune", &r.artifact, "", json!({"recovery":id})) {
        Ok(op) => {
            // Prune carries a fixed 8 s undo window (tasks.rs).
            kick_after(
                db,
                op.not_before.saturating_sub(op.created_at).max(0) as u64,
            );
            (StatusCode::ACCEPTED, Json(op)).into_response()
        }
        Err(e) => failure(e),
    }
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt;
    use nzbd_engine::{Engine, EngineConfig, Tuning};
    use tower::util::ServiceExt;

    async fn get(engine: &nzbd_engine::EngineHandle, uri: &str) -> (u16, serde_json::Value) {
        let response = crate::router(engine.clone())
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status().as_u16();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&body).unwrap())
    }

    // Field report 2026-09-28: after a scan the Files tab showed every folder
    // as "0 entries · 0 B", and Inspect queued work for a 30 s tick. The list
    // now says whether a row was measured, carries totals and per-view
    // counts, and an inspect runs as soon as it is admitted.
    #[tokio::test]
    async fn list_reports_measurement_counts_and_inspect_runs_without_the_tick() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = Engine::spawn(EngineConfig::single_node(
            vec![],
            tmp.path().join("state"),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let root = tmp.path().join("dest");
        std::fs::create_dir_all(&root).unwrap();
        let folder = root.join("Some.Show.S01E01");
        std::fs::create_dir(&folder).unwrap();
        std::fs::write(folder.join("ep.mkv"), vec![b'x'; 4096]).unwrap();
        let a = engine.artifacts().discover(&root, &folder, false).unwrap();

        let (status, page) = get(&engine, "/api/v1/artifacts?filter=attention&limit=10").await;
        assert_eq!(status, 200);
        assert_eq!(page["total"], 1);
        assert_eq!(page["limit"], 10);
        assert_eq!(page["counts"]["attention"], 1);
        assert_eq!(page["counts"]["owned"], 0);
        assert_eq!(
            page["entries"][0]["measured"], false,
            "discovered, not walked"
        );
        assert_eq!(page["entries"][0]["files"], 0);
        assert_eq!(
            page["entries"][0]["artifact"]["files"]
                .as_array()
                .map(Vec::len),
            Some(0),
            "manifest stripped in SQL"
        );

        let response = crate::router(engine.clone())
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/artifacts/{}/inspect", a.id))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 202);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let op: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let op_id = op["id"].as_str().unwrap().to_owned();
        // The kick runs off-thread; well under a second, never 30 of them.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let (_, op) = get(&engine, &format!("/api/v1/artifact-operations/{op_id}")).await;
            if op["state"] == "succeeded" {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "inspect never ran: {op}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let (_, page) = get(&engine, "/api/v1/artifacts?sort=size&q=some.show").await;
        assert_eq!(page["total"], 1);
        assert_eq!(page["entries"][0]["measured"], true);
        assert_eq!(page["entries"][0]["files"], 1);
        assert_eq!(page["entries"][0]["bytes"], 4096);
        let (_, none) = get(&engine, "/api/v1/artifacts?q=nomatch").await;
        assert_eq!(none["total"], 0);
        assert_eq!(
            none["counts"]["live"], 1,
            "counts describe the inventory, not the search"
        );
        engine.shutdown().await;
    }
}
