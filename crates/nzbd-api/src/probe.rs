//! `POST /api/v1/probe` — the decoded head of a release's main file, for
//! Curator's pre-flight measurement (Curator ADR 0026 §6) — and
//! `GET /api/v1/probes`, the recent probes for the UI.
//!
//! Bytes, not opinions: the handler fetches the NZB the caller names (the
//! same fetch an add uses), reads only the articles covering the head, and
//! returns them base64-encoded, or the reason it could not. Nothing is
//! queued and nothing touches disk.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{error, unix_now, ApiState};

/// The capability `/api/v1/status` advertises for this endpoint.
pub const CAPABILITY: &str = "release-probe-v1";

/// A probe that has not answered in this long is abandoned: the caller is
/// ranking search results and would rather fall back to the name.
const PROBE_TIMEOUT: Duration = Duration::from_secs(90);

/// Default head size: enough for container headers and the first frame.
const DEFAULT_BYTES: usize = 4 << 20;

/// How many recent probes the log keeps for the UI.
const LOG_LEN: usize = 200;

/// One probe, as the UI's Probes view shows it. No URL is kept: indexer
/// URLs carry API keys.
#[derive(Debug, Clone, Serialize)]
pub struct ProbeRecord {
    pub at: i64,
    /// The NZB's own title, else the inner file name.
    pub release: String,
    pub file: String,
    pub file_size: u64,
    pub path: Vec<&'static str>,
    pub articles: usize,
    pub articles_missing: usize,
    pub bytes: usize,
    /// "ok", or the refusal reason.
    pub result: String,
    pub detail: String,
    pub millis: u64,
}

/// Recent probes, newest first, plus running totals since start.
#[derive(Default)]
pub struct ProbeLog {
    inner: Mutex<LogInner>,
}

#[derive(Default)]
struct LogInner {
    recent: VecDeque<ProbeRecord>,
    served: u64,
    unavailable: u64,
    bytes: u64,
}

impl ProbeLog {
    fn record(&self, r: ProbeRecord) {
        let mut g = self.inner.lock().unwrap();
        if r.result == "ok" {
            g.served += 1;
        } else {
            g.unavailable += 1;
        }
        g.bytes += r.bytes as u64;
        g.recent.push_front(r);
        g.recent.truncate(LOG_LEN);
    }
}

#[derive(Deserialize)]
pub(crate) struct ProbeQuery {
    url: Option<String>,
    bytes: Option<usize>,
}

/// `POST /api/v1/probe?url=…&bytes=…`, or the NZB itself as the body.
pub(crate) async fn probe_release(
    State(state): State<ApiState>,
    Extension(log): Extension<Arc<ProbeLog>>,
    Query(q): Query<ProbeQuery>,
    body: Bytes,
) -> Response {
    let started = std::time::Instant::now();
    let nzb = match (&q.url, body.is_empty()) {
        (Some(url), _) => match nzbd_engine::fetch::http_get(url).await {
            Ok(b) => b,
            Err(e) => {
                return error(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    &format!("could not fetch the NZB: {e}"),
                )
            }
        },
        (None, false) => body.to_vec(),
        (None, true) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "give ?url= or the NZB as the body",
            )
        }
    };
    let servers = state
        .setup
        .as_ref()
        .map(|s| s.current.lock().unwrap().server_defs())
        .unwrap_or_default();
    if !servers.iter().any(|s| s.active) {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "no news servers are configured",
        );
    }
    let want = q.bytes.unwrap_or(DEFAULT_BYTES);
    let title = nzbd_nzb::parse(&nzb).ok().and_then(|p| p.meta.title);
    let outcome =
        match tokio::time::timeout(PROBE_TIMEOUT, nzbd_probe::probe(&nzb, servers, want)).await {
            Ok(o) => o,
            Err(_) => nzbd_probe::Outcome::Unavailable {
                reason: nzbd_probe::Reason::Unreadable,
                detail: "the news servers did not answer in time".into(),
            },
        };
    let millis = started.elapsed().as_millis() as u64;
    match outcome {
        nzbd_probe::Outcome::Head(h) => {
            log.record(ProbeRecord {
                at: unix_now(),
                release: title.unwrap_or_else(|| h.file.clone()),
                file: h.file.clone(),
                file_size: h.file_size,
                path: h.path.clone(),
                articles: h.articles,
                articles_missing: h.articles_missing,
                bytes: h.bytes.len(),
                result: "ok".into(),
                detail: String::new(),
                millis,
            });
            Json(json!({
                "status": "ok",
                "file": h.file,
                "file_size": h.file_size,
                "path": h.path,
                "articles": h.articles,
                "articles_missing": h.articles_missing,
                "bytes": h.bytes.len(),
                "head": base64::engine::general_purpose::STANDARD.encode(&h.bytes),
            }))
            .into_response()
        }
        nzbd_probe::Outcome::Unavailable { reason, detail } => {
            let reason = serde_json::to_value(reason)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default();
            log.record(ProbeRecord {
                at: unix_now(),
                release: title.unwrap_or_default(),
                file: String::new(),
                file_size: 0,
                path: Vec::new(),
                articles: 0,
                articles_missing: 0,
                bytes: 0,
                result: reason.clone(),
                detail: detail.clone(),
                millis,
            });
            // A refusal is an answer about the release, not a failed request.
            Json(json!({ "status": "unavailable", "reason": reason, "detail": detail }))
                .into_response()
        }
    }
}

/// `GET /api/v1/probes`: totals since start and the recent probes.
pub(crate) async fn list_probes(Extension(log): Extension<Arc<ProbeLog>>) -> Response {
    let g = log.inner.lock().unwrap();
    Json(json!({
        "served": g.served,
        "unavailable": g.unavailable,
        "bytes": g.bytes,
        "recent": g.recent.iter().collect::<Vec<_>>(),
    }))
    .into_response()
}
