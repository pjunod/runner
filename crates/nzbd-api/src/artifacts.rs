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
        .route("/api/v1/deliveries", get(deliveries))
        .route("/api/v1/deliveries/{id}", get(delivery))
        .route("/api/v1/deliveries/{id}/accept", post(accept_delivery))
        .route(
            "/api/v1/deliveries/{id}/source-cleanup/retry",
            post(retry_delivery_cleanup),
        )
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
        .route("/api/v1/artifacts/{id}/relocations", get(relocations))
        .route(
            "/api/v1/artifact-operations/{id}/abandon-relocation",
            post(abandon_relocation),
        )
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

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeliveryPage {
    #[serde(default)]
    after: i64,
    limit: Option<u32>,
    #[serde(default)]
    include_accepted: bool,
}

#[cfg(test)]
mod delivery_query_tests {
    use super::DeliveryPage;

    #[test]
    fn delivery_query_uses_the_documented_camel_case_flag() {
        let page: DeliveryPage =
            serde_urlencoded::from_str("after=41&limit=100&includeAccepted=true").unwrap();
        assert_eq!(page.after, 41);
        assert_eq!(page.limit, Some(100));
        assert!(page.include_accepted);
    }
}

async fn deliveries(State(st): State<ApiState>, Query(page): Query<DeliveryPage>) -> Response {
    let db = st.engine.artifacts();
    let limit = page.limit.unwrap_or(50);
    let include_accepted = page.include_accepted;
    work(move || {
        let (deliveries, next_cursor) =
            db.deliveries_page(page.after, limit, include_accepted)?;
        Ok(json!({
            "protocolVersion": 1,
            "deliveries": deliveries,
            "nextCursor": next_cursor,
            "cursorRule": "Rescan with after=0 each sweep, or durably persist every returned delivery before advancing the cursor."
        }))
    })
    .await
}

async fn delivery(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    work(move || db.delivery(&id)).await
}

async fn retry_delivery_cleanup(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    work(move || db.retry_delivery_source_cleanup(&id)).await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeliveryAcceptance {
    protocol_version: u8,
    publication_generation: String,
    manifest_digest: String,
    consumer_receipt_id: String,
}

async fn accept_delivery(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<DeliveryAcceptance>,
) -> Response {
    let db = st.engine.artifacts();
    if body.protocol_version != 1 {
        return error(
            StatusCode::BAD_REQUEST,
            "unsupported delivery protocol version",
        );
    }
    work(move || {
        db.accept_delivery(
            &id,
            &body.publication_generation,
            &body.manifest_digest,
            &body.consumer_receipt_id,
        )
    })
    .await
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
                    "measured": row.measured,
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
        let mut value = serde_json::to_value(&a)?;
        if let Some(assessment) = db.workspace_assessment(&id)? {
            value["workspace_cleanup"] = serde_json::to_value(assessment)?;
        }
        Ok(value)
    })
    .await
}
async fn relocations(State(st): State<ApiState>, Path(id): Path<String>) -> Response {
    let db = st.engine.artifacts();
    work(move || db.pending_relocations(&id)).await
}
#[derive(Deserialize)]
struct AbandonRelocation {
    revision: u64,
    generation: String,
}
async fn abandon_relocation(
    State(st): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<AbandonRelocation>,
) -> Response {
    let db = st.engine.artifacts();
    let op = match db.operation(&id) {
        Ok(op) => op,
        Err(e) => return failure(e),
    };
    if let Err(response) = validate_artifact_role(&st, &op.artifact) {
        return response;
    }
    match st
        .engine
        .abandon_relocation(id, body.revision, body.generation)
        .await
    {
        Ok(Ok(artifact)) => Json(artifact).into_response(),
        Ok(Err(e)) => error(StatusCode::CONFLICT, &e),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
    }
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
    if let Some(p) = cfg
        .paths
        .inter_dir
        .as_ref()
        .filter(|p| !p.as_os_str().is_empty())
    {
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
            active.push(
                db.for_job(job.id.0)
                    .ok()
                    .flatten()
                    .map(|a| a.path)
                    .unwrap_or_else(|| {
                        cfg.download_dir()
                            .join(nzbd_engine::queue::job_dir_name(&job))
                    }),
            );
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
    use axum::http::StatusCode;
    use http_body_util::BodyExt;
    use nzbd_engine::{Engine, EngineConfig, Tuning};
    use nzbd_state::artifacts::{Receipt, ReceiptFile, Settings};
    use std::sync::Arc;
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

    async fn response_json(response: axum::response::Response) -> serde_json::Value {
        let body = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&body).unwrap()
    }

    #[test]
    fn processing_root_discovery_excludes_internal_state_and_configured_roots() {
        use super::{scan_request, Inventory, PathBuf};
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = nzbd_config::Config::default();
        cfg.paths.main_dir = tmp.path().join("processing");
        cfg.paths.dest_dir = cfg.paths.main_dir.join("complete");
        cfg.paths.inter_dir = Some(PathBuf::new());
        let db = Inventory::open(&cfg.state_dir()).unwrap();
        let scratch = cfg.state_dir().join("transform-workspaces/private");
        std::fs::create_dir_all(&scratch).unwrap();
        std::fs::write(scratch.join("data"), b"private repair bytes").unwrap();
        for directory in [
            cfg.dest_dir(),
            cfg.paths.main_dir.join("failed"),
            cfg.paths.main_dir.join("recovery"),
        ] {
            std::fs::create_dir_all(directory).unwrap();
        }
        let orphan = cfg.download_dir().join("orphan");
        std::fs::create_dir(&orphan).unwrap();
        std::fs::write(orphan.join("payload"), b"payload").unwrap();
        let request = scan_request(&cfg, &db, Vec::new());
        let operation = db
            .submit_task("scan", "installation", "processing-default-scan", request)
            .unwrap();
        assert_eq!(db.execute_task(&operation.id).unwrap().state, "succeeded");
        let rows = db.list(0, 100).unwrap();
        assert_eq!(
            rows.len(),
            1,
            "internal state roots must not become orphan payloads"
        );
        assert_eq!(rows[0].path, orphan);
        assert!(rows[0].measured());
        assert!(scratch.join("data").exists());
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

    #[tokio::test]
    async fn recovery_http_routes_enforce_consumer_scope_and_persist_receipts() {
        let tmp = tempfile::tempdir().unwrap();
        let working = tmp.path().join("working");
        let downloads = tmp.path().join("downloads");
        let source_root = tmp.path().join("source");
        let recovery_root = tmp.path().join("recovery");
        for root in [&working, &downloads, &source_root, &recovery_root] {
            std::fs::create_dir_all(root).unwrap();
        }
        let state_dir = tmp.path().join("state");
        let engine = Engine::spawn(EngineConfig::single_node(
            vec![],
            state_dir.clone(),
            downloads.clone(),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let artifacts = engine.artifacts();
        let publication = source_root.join("release-generation");
        artifacts
            .allocate_for_delivery(801, &source_root, &publication, true, true)
            .unwrap();
        std::fs::create_dir_all(&publication).unwrap();
        std::fs::write(publication.join("episode.mkv"), b"recovery payload").unwrap();
        let artifact = artifacts
            .finish(801, &publication, &source_root, "completed")
            .unwrap();
        artifacts
            .set_settings(&Settings {
                enabled: true,
                failed_retention_days: 30,
                recovery_root: recovery_root.clone(),
                consumer_token: "recovery-secret".into(),
            })
            .unwrap();

        let mut cfg = nzbd_config::Config::default();
        cfg.paths.main_dir = working;
        cfg.paths.dest_dir = downloads;
        cfg.paths.queue_dir = Some(state_dir);
        let setup = Arc::new(crate::SetupHandle::for_running(
            None,
            "127.0.0.1:0".into(),
            cfg,
        ));
        let app = crate::router_with(crate::ApiState {
            engine: engine.clone(),
            torrent: None,
            history: None,
            log: None,
            setup: Some(setup),
            clients: None,
            shutdown: None,
            pp_stats: None,
            pp_manager: None,
            events: None,
        });

        let preview = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/artifacts/{}/recoveries", artifact.id))
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "preview": true,
                            "revision": artifact.revision,
                            "idempotency_key": "preview-recovery",
                            "files": ["episode.mkv"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(preview.status(), StatusCode::OK);
        let preview = response_json(preview).await;
        assert_eq!(preview["selected_files"], 1);
        assert_eq!(
            artifacts.recoveries(0).unwrap().len(),
            0,
            "preview is read-only"
        );

        let stage_uri = format!("/api/v1/artifacts/{}/recoveries", artifact.id);
        let stage_body = serde_json::json!({
            "preview": false,
            "revision": artifact.revision,
            "idempotency_key": "stage-recovery-task",
            "files": ["episode.mkv"]
        });
        let queued = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(&stage_uri)
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(stage_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(queued.status(), StatusCode::ACCEPTED);
        let queued = response_json(queued).await;
        assert_eq!(queued["kind"], "stage");
        let replay = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(&stage_uri)
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(stage_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::ACCEPTED);
        assert_eq!(response_json(replay).await["id"], queued["id"]);

        // Seed a durable published recovery via the same inventory API used by
        // the background stage task, then exercise the public claim/receipt
        // handlers against its exact persisted identity.
        let staged = artifacts
            .stage_recovery(
                &artifact.id,
                artifact.revision,
                "api-recovery-request",
                &["episode.mkv".into()],
                &recovery_root,
            )
            .unwrap();
        assert_eq!(staged.state, "published");
        let detail = app
            .clone()
            .oneshot(
                axum::http::Request::get(format!("/api/v1/recoveries/{}", staged.id))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(detail.status(), StatusCode::OK);
        assert_eq!(
            response_json(detail).await["manifest_digest"],
            staged.manifest_digest
        );

        let claim_uri = format!("/api/v1/recoveries/{}/claim", staged.id);
        let claim_body = serde_json::json!({
            "import_id": "curator-import-801",
            "manifest_digest": staged.manifest_digest,
        });
        for token in [None, Some("wrong-token")] {
            let mut request = axum::http::Request::builder()
                .method("POST")
                .uri(&claim_uri)
                .header("content-type", "application/json");
            if let Some(token) = token {
                request = request.header("x-recovery-token", token);
            }
            let response = app
                .clone()
                .oneshot(
                    request
                        .body(axum::body::Body::from(claim_body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
        assert_eq!(artifacts.recovery(&staged.id).unwrap().state, "published");

        let claimed = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(&claim_uri)
                    .header("content-type", "application/json")
                    .header("x-recovery-token", "recovery-secret")
                    .body(axum::body::Body::from(claim_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(claimed.status(), StatusCode::OK);
        assert_eq!(response_json(claimed).await["state"], "claimed");

        let wrong_receipt = serde_json::json!({
            "import_id": "other-import",
            "manifest_digest": staged.manifest_digest,
            "files": []
        });
        let wrong = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/recoveries/{}/receipt", staged.id))
                    .header("content-type", "application/json")
                    .header("x-recovery-token", "recovery-secret")
                    .body(axum::body::Body::from(wrong_receipt.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::CONFLICT);
        assert_eq!(artifacts.recovery(&staged.id).unwrap().state, "claimed");

        let receipt = Receipt {
            import_id: "curator-import-801".into(),
            manifest_digest: staged.manifest_digest.clone(),
            files: staged
                .files
                .iter()
                .map(|file| ReceiptFile {
                    id: file.id.clone(),
                    bytes: file.bytes,
                    sha256: file.sha256.clone(),
                    result: "imported".into(),
                })
                .collect(),
        };
        let accepted = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/recoveries/{}/receipt", staged.id))
                    .header("content-type", "application/json")
                    .header("x-recovery-token", "recovery-secret")
                    .body(axum::body::Body::from(
                        serde_json::to_vec(&receipt).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        assert_eq!(response_json(accepted).await["state"], "imported");

        let cancel = app
            .clone()
            .oneshot(
                axum::http::Request::post(format!("/api/v1/recoveries/{}/cancel", staged.id))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cancel.status(), StatusCode::CONFLICT);
        let cancel_ack = app
            .clone()
            .oneshot(
                axum::http::Request::post(format!("/api/v1/recoveries/{}/cancel-ack", staged.id))
                    .header("x-recovery-token", "recovery-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cancel_ack.status(), StatusCode::CONFLICT);

        let visible = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/recoveries?include_terminal=true")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(visible.status(), StatusCode::OK);
        assert_eq!(response_json(visible).await[0]["state"], "imported");
        let hidden = app
            .oneshot(
                axum::http::Request::get("/api/v1/recoveries")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response_json(hidden).await.as_array().unwrap().len(), 0);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn artifact_http_routes_report_inventory_and_refuse_configured_directory_roles() {
        let tmp = tempfile::tempdir().unwrap();
        let working = tmp.path().join("working");
        let protected = tmp.path().join("protected");
        let downloads = protected.join("downloads");
        for root in [&working, &downloads] {
            std::fs::create_dir_all(root).unwrap();
        }
        let engine = Engine::spawn(EngineConfig::single_node(
            vec![],
            tmp.path().join("state"),
            downloads.clone(),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let nested_payload = downloads.join("release").join("episode.mkv");
        std::fs::create_dir_all(nested_payload.parent().unwrap()).unwrap();
        std::fs::write(&nested_payload, b"protected payload").unwrap();
        let artifact = engine
            .artifacts()
            .discover(tmp.path(), &protected, false)
            .unwrap();
        engine.artifacts().inspect(&artifact.id).unwrap();

        let mut cfg = nzbd_config::Config::default();
        cfg.paths.main_dir = working;
        cfg.paths.dest_dir = downloads;
        cfg.paths.queue_dir = Some(tmp.path().join("state"));
        let setup = Arc::new(crate::SetupHandle::for_running(
            None,
            "127.0.0.1:0".into(),
            cfg,
        ));
        let app = crate::router_with(crate::ApiState {
            engine: engine.clone(),
            torrent: None,
            history: None,
            log: None,
            setup: Some(setup),
            clients: None,
            shutdown: None,
            pp_stats: None,
            pp_manager: None,
            events: None,
        });

        let detail = app
            .clone()
            .oneshot(
                axum::http::Request::get(format!("/api/v1/artifacts/{}", artifact.id))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(detail.status(), StatusCode::OK);
        assert_eq!(response_json(detail).await["id"], artifact.id);

        let files = app
            .clone()
            .oneshot(
                axum::http::Request::get(format!(
                    "/api/v1/artifacts/{}/files?offset=0",
                    artifact.id
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(files.status(), StatusCode::OK);
        let files = response_json(files).await;
        assert_eq!(
            files["total"], 3,
            "manifest includes nested directories and file"
        );
        assert_eq!(files["file_count"], 1);
        assert_eq!(files["bytes"], b"protected payload".len());

        let events = app
            .clone()
            .oneshot(
                axum::http::Request::get(format!(
                    "/api/v1/artifacts/{}/events?after=0",
                    artifact.id
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(events.status(), StatusCode::OK);
        assert!(response_json(events).await.as_array().is_some());

        let delete = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/artifacts/{}/delete", artifact.id))
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "revision": artifact.revision,
                            "idempotency_key": "must-not-delete-configured-root",
                            "undo_seconds": 0
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete.status(), StatusCode::CONFLICT);
        assert!(response_json(delete).await["error"]
            .as_str()
            .unwrap()
            .contains("configured directory role"));
        assert_eq!(
            std::fs::read(&nested_payload).unwrap(),
            b"protected payload"
        );

        let missing = app
            .oneshot(
                axum::http::Request::get("/api/v1/artifacts/not-an-artifact")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn artifact_settings_http_masks_credentials_and_preserves_on_empty_update() {
        let tmp = tempfile::tempdir().unwrap();
        let working = tmp.path().join("working");
        let downloads = tmp.path().join("downloads");
        std::fs::create_dir_all(&working).unwrap();
        std::fs::create_dir_all(&downloads).unwrap();
        let state_dir = tmp.path().join("state");
        let recovery_root = tmp.path().join("recovery");
        let engine = Engine::spawn(EngineConfig::single_node(
            vec![],
            state_dir.clone(),
            downloads.clone(),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        engine
            .artifacts()
            .set_settings(&Settings {
                enabled: true,
                failed_retention_days: 20,
                recovery_root: recovery_root.clone(),
                consumer_token: "do-not-erase".into(),
            })
            .unwrap();
        let mut cfg = nzbd_config::Config::default();
        cfg.paths.main_dir = working;
        cfg.paths.dest_dir = downloads;
        cfg.paths.queue_dir = Some(state_dir);
        let setup = Arc::new(crate::SetupHandle::for_running(
            None,
            "127.0.0.1:0".into(),
            cfg,
        ));
        let app = crate::router_with(crate::ApiState {
            engine: engine.clone(),
            torrent: None,
            history: None,
            log: None,
            setup: Some(setup),
            clients: None,
            shutdown: None,
            pp_stats: None,
            pp_manager: None,
            events: None,
        });

        let get_settings = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/artifacts/settings")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_settings.status(), StatusCode::OK);
        let visible = response_json(get_settings).await;
        assert_eq!(visible["settings"]["consumer_token"], "");
        assert_eq!(visible["advisory"][0]["met"], true);

        let updated = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("PUT")
                    .uri("/api/v1/artifacts/settings")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "enabled": false,
                            "failed_retention_days": 45,
                            "recovery_root": recovery_root,
                            "consumer_token": ""
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(updated.status(), StatusCode::OK);
        assert_eq!(response_json(updated).await["enabled"], false);
        let persisted = engine.artifacts().settings().unwrap();
        assert_eq!(persisted.failed_retention_days, 45);
        assert_eq!(persisted.consumer_token, "do-not-erase");
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn delivery_http_errors_distinguish_bad_protocol_from_missing_receipt() {
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
        let app = crate::router(engine.clone());
        let get_missing = app
            .clone()
            .oneshot(
                axum::http::Request::get("/api/v1/deliveries/missing-delivery")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_missing.status(), StatusCode::NOT_FOUND);

        let accept_uri = "/api/v1/deliveries/missing-delivery/accept";
        let unsupported = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(accept_uri)
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "protocolVersion": 2,
                            "publicationGeneration": "generation",
                            "manifestDigest": "digest",
                            "consumerReceiptId": "receipt"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unsupported.status(), StatusCode::BAD_REQUEST);
        assert!(response_json(unsupported).await["error"]
            .as_str()
            .unwrap()
            .contains("unsupported delivery protocol"));

        let missing = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(accept_uri)
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "protocolVersion": 1,
                            "publicationGeneration": "generation",
                            "manifestDigest": "digest",
                            "consumerReceiptId": "receipt"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);

        let retry_missing = app
            .oneshot(
                axum::http::Request::post(
                    "/api/v1/deliveries/missing-delivery/source-cleanup/retry",
                )
                .body(axum::body::Body::from("{}"))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retry_missing.status(), StatusCode::NOT_FOUND);
        engine.shutdown().await;
    }
}
