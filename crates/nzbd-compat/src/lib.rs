//! NZBGet compatibility shim (ARCHITECTURE.md §10.2).
//!
//! Speaks NZBGet's JSON-RPC 1.1 dialect on `/jsonrpc`: no `"jsonrpc":"2.0"`
//! member, positional params, `{"version":"1.1","id":…,"result":…}` envelope.
//! Phase 3 C1 — the Sonarr/Radarr certification surface — is served:
//! `version`, `status`, `listgroups`, `append` (v13+ and legacy positional
//! forms), `history`, `editqueue` (3- and 4-arg forms), `config`/
//! `loadconfig`, `rate`, `pausedownload`/`resumedownload`.
//! XML-RPC (`/xmlrpc`), JSON-P, GET-form safe methods and the auth tiers
//! are the remaining phase-3 items.
//!
//! Field-shape rules (do not "fix" them — clients parse by name):
//! - 64-bit sizes are split into `…Lo` / `…Hi` / `…MB` triplets.
//! - Deprecated aliases (`FirstID`, `LastID`, …) are preserved.
//! - History `Status` is `"TOTAL/DETAIL"` (`SUCCESS/ALL`, `FAILURE/PAR`, …).

pub mod xmlrpc;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use base64::Engine as _;
use nzbd_engine::{EngineHandle, JobSummary};
use nzbd_state::history::HistoryDb;
use nzbd_state::HistoryEntry;
use nzbd_types::{JobId, JobStatus};
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct CompatConfig {
    /// Version string reported to clients (Sonarr gates on >=12 / >=16).
    pub version: String,
}

impl Default for CompatConfig {
    fn default() -> Self {
        CompatConfig {
            version: "26.2".into(),
        }
    }
}

#[derive(Clone)]
pub struct CompatState {
    pub config: Arc<CompatConfig>,
    pub engine: EngineHandle,
    /// History store (None until the daemon wires it — e.g. bare tests).
    pub history: Option<Arc<HistoryDb>>,
    /// `config` method projection: NZBGet-style option (Name, Value) pairs.
    pub options: Arc<Vec<(String, String)>>,
    /// Daemon log ring (serves `log`, receives `writelog`).
    pub log: Option<Arc<nzbd_api::LogBuffer>>,
    /// Wakes the watch-dir scanner (`scan` method).
    pub scan_notify: Option<Arc<tokio::sync::Notify>>,
    /// RSS feed engine (`fetchfeeds`/`viewfeed`).
    pub feeds: Option<nzbd_feed::FeedsHandle>,
    /// Shared consumer registry (who is polling us; stamps history
    /// observations with the client's User-Agent).
    pub clients: Option<Arc<nzbd_api::ClientRegistry>>,
}

impl CompatState {
    pub fn new(config: CompatConfig, engine: EngineHandle) -> CompatState {
        CompatState {
            config: Arc::new(config),
            engine,
            history: None,
            options: Arc::new(Vec::new()),
            log: None,
            scan_notify: None,
            feeds: None,
            clients: None,
        }
    }
}

/// Split a u64 into NZBGet's `(Lo, Hi, MB)` wire triplet.
pub fn split64(v: u64) -> (u32, u32, u64) {
    (v as u32, (v >> 32) as u32, v / 1024 / 1024)
}

/// Build the JSON-RPC 1.1-flavored response envelope NZBGet clients expect.
pub fn envelope(id: Value, result: Result<Value, (i64, &str)>) -> Value {
    match result {
        Ok(result) => json!({ "version": "1.1", "id": id, "result": result }),
        Err((code, msg)) => json!({
            "version": "1.1",
            "id": id,
            "error": { "code": code, "message": msg }
        }),
    }
}

/// NZBGet queue status string for a job.
fn group_status(j: &JobSummary) -> &'static str {
    match j.status {
        JobStatus::Queued => "QUEUED",
        JobStatus::Downloading => "DOWNLOADING",
        JobStatus::Paused => "PAUSED",
        JobStatus::Fetching => "FETCHING",
        JobStatus::PostQueued => "PP_QUEUED",
        JobStatus::Post { stage } => match stage {
            nzbd_types::PostStage::ParRename | nzbd_types::PostStage::RarRename => "RENAMING",
            nzbd_types::PostStage::ParVerify => "VERIFYING_SOURCES",
            nzbd_types::PostStage::ParRepair => "REPAIRING",
            nzbd_types::PostStage::Unpack => "UNPACKING",
            nzbd_types::PostStage::Cleanup | nzbd_types::PostStage::Move => "MOVING",
            nzbd_types::PostStage::PostUnpackRename => "RENAMING",
            nzbd_types::PostStage::Script => "EXECUTING_SCRIPT",
        },
        JobStatus::Completed => "SUCCESS",
        JobStatus::Failed => "FAILURE",
        JobStatus::Deleted => "DELETED",
    }
}

/// Native history status → NZBGet `"TOTAL/DETAIL"` wire form.
fn history_status(native: &str) -> &str {
    match native {
        "SUCCESS" => "SUCCESS/ALL",
        "PAR_FAILURE" => "FAILURE/PAR",
        "UNPACK_FAILURE" => "FAILURE/UNPACK",
        "SCRIPT_FAILURE" => "WARNING/SCRIPT",
        other => other, // already TOTAL/DETAIL shaped ("FAILURE/HEALTH", "DELETED/MANUAL")
    }
}

fn history_json(e: &HistoryEntry) -> Value {
    let (flo, fhi, fmb) = split64(e.size);
    let status = history_status(&e.status);
    let total = status.split('/').next().unwrap_or("SUCCESS");
    let params: Vec<Value> = e
        .params
        .iter()
        .map(|(k, v)| json!({ "Name": k, "Value": v }))
        .collect();
    json!({
        "NZBID": e.job.0,
        "ID": e.job.0, // deprecated alias
        "Kind": "NZB",
        "Name": e.name,
        "NZBName": e.name,
        "NZBNicename": e.name, // deprecated alias
        "RemoteName": format!("{}.nzb", e.name),
        "Status": status,
        "TotalStatus": total,
        "Category": e.category.clone().unwrap_or_default(),
        "FileSizeLo": flo,
        "FileSizeHi": fhi,
        "FileSizeMB": fmb,
        "DestDir": e.final_dir.clone().unwrap_or_default(),
        "FinalDir": e.final_dir.clone().unwrap_or_default(),
        "HistoryTime": e.completed_at_unix,
        "Health": e.health,
        "CriticalHealth": 1000,
        "ParStatus": match status {
            "FAILURE/PAR" => "FAILURE",
            "SUCCESS/ALL" => "SUCCESS",
            _ => "NONE",
        },
        "UnpackStatus": match status {
            "FAILURE/UNPACK" => "FAILURE",
            "SUCCESS/ALL" => "SUCCESS",
            _ => "NONE",
        },
        "MoveStatus": "SUCCESS",
        "ScriptStatus": if status == "WARNING/SCRIPT" { "FAILURE" } else { "NONE" },
        "DeleteStatus": match status {
            "DELETED/DUPE" => "DUPE",
            s if s.starts_with("DELETED") => "MANUAL",
            _ => "NONE",
        },
        "MarkStatus": "NONE",
        "UrlStatus": "NONE",
        "Parameters": params,
        "ScriptStatuses": [],
        "ServerStats": [],
        "Deleted": total == "DELETED", // deprecated alias
        "DownloadedSizeLo": flo,
        "DownloadedSizeHi": fhi,
        "DownloadedSizeMB": fmb,
        "DownloadTimeSec": 0,
        "PostTotalTimeSec": 0,
        "ParTimeSec": 0,
        "RepairTimeSec": 0,
        "UnpackTimeSec": 0,
        "DupeKey": e.dupe_key,
        "DupeScore": e.dupe_score,
        "DupeMode": "SCORE",
        "RetryData": false,
    })
}

// ---------------------------------------------------------------------------
// param helpers (positional JSON-RPC arrays, NZBGet-tolerant coercions)
// ---------------------------------------------------------------------------

fn p_str(params: &Value, i: usize) -> String {
    params
        .get(i)
        .map(|v| match v {
            Value::String(s) => s.clone(),
            other => other.to_string().trim_matches('"').to_string(),
        })
        .unwrap_or_default()
}

fn p_i64(params: &Value, i: usize) -> i64 {
    match params.get(i) {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        Some(Value::Bool(b)) => *b as i64,
        _ => 0,
    }
}

fn p_bool(params: &Value, i: usize) -> bool {
    match params.get(i) {
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0) != 0,
        Some(Value::String(s)) => s.eq_ignore_ascii_case("true") || s == "1",
        _ => false,
    }
}

fn p_ids(params: &Value, i: usize) -> Vec<JobId> {
    match params.get(i) {
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(|v| v.as_i64().map(|n| JobId(n as u32)))
            .collect(),
        Some(Value::Number(n)) => n.as_i64().map(|n| JobId(n as u32)).into_iter().collect(),
        _ => Vec::new(),
    }
}

/// Decode append content: base64-encoded NZB (the documented form) or raw
/// XML (tolerated). `None` if it is neither.
fn decode_nzb(content: &str) -> Option<Vec<u8>> {
    let trimmed = content.trim();
    if trimmed.contains("<nzb") || trimmed.starts_with("<?xml") {
        return Some(trimmed.as_bytes().to_vec());
    }
    let cleaned: String = trimmed.chars().filter(|c| !c.is_whitespace()).collect();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&cleaned)
        .ok()?;
    let head = String::from_utf8_lossy(&decoded[..decoded.len().min(4096)]).to_lowercase();
    head.contains("<nzb").then_some(decoded)
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub async fn dispatch(
    state: &CompatState,
    method: &str,
    params: &Value,
) -> Result<Value, (i64, &'static str)> {
    dispatch_cluster(state, method, params, None).await
}
async fn dispatch_cluster(
    state: &CompatState,
    method: &str,
    params: &Value,
    context: Option<&nzbd_api::cluster_removal::ClusterRemovalContext>,
) -> Result<Value, (i64, &'static str)> {
    match method {
        "version" => Ok(Value::String(state.config.version.clone())),
        "status" => Ok(status_json(state)),
        "listgroups" => Ok(listgroups_json(state)),
        "append" => append(state, params).await,
        "history" => history(state, params).await,
        "editqueue" => editqueue(state, params, context).await,
        "config" | "loadconfig" => {
            let opts: Vec<Value> = state
                .options
                .iter()
                .map(|(k, v)| json!({ "Name": k, "Value": v }))
                .collect();
            Ok(Value::Array(opts))
        }
        "listfiles" => listfiles(state, params).await,
        "servervolumes" => {
            // Simplified from NZBGet's slot arrays: totals + current day and
            // month windows per server, from the live volume counters.
            let snap = state.engine.snapshot();
            Ok(Value::Array(
                snap.server_volumes
                    .iter()
                    .map(|v| {
                        let (tlo, thi, tmb) = split64(v.total_bytes);
                        let (dlo, dhi, dmb) = split64(v.day_bytes);
                        let (mlo, mhi, mmb) = split64(v.month_bytes);
                        json!({
                            "ServerID": v.server,
                            "TotalSizeLo": tlo, "TotalSizeHi": thi, "TotalSizeMB": tmb,
                            "DaySizeLo": dlo, "DaySizeHi": dhi, "DaySizeMB": dmb,
                            "MonthSizeLo": mlo, "MonthSizeHi": mhi, "MonthSizeMB": mmb,
                        })
                    })
                    .collect(),
            ))
        }
        "sysinfo" => {
            let tool = |cmd: &str| {
                let path = std::env::var_os("PATH")
                    .and_then(|paths| {
                        std::env::split_paths(&paths)
                            .map(|d| d.join(cmd))
                            .find(|p| p.is_file())
                    })
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default();
                json!({ "Name": cmd, "Path": path, "Version": "" })
            };
            Ok(json!({
                "OS": { "Name": std::env::consts::OS, "Version": std::env::consts::ARCH },
                "CPU": { "Model": "", "Arch": std::env::consts::ARCH },
                "Network": { "PublicIP": "", "PrivateIP": "" },
                "Tools": [tool("par2"), tool("unrar"), tool("7z")],
                "Libraries": [
                    { "Name": "rustls", "Version": "" },
                    { "Name": "tokio", "Version": "" }
                ],
            }))
        }
        "testserver" => testserver(params).await,
        "log" => {
            // log(IDFrom, NumberOfEntries) — IDFrom 0 = the newest N.
            let id_from = p_i64(params, 0).max(0) as u64;
            let n = p_i64(params, 1).clamp(1, 2000) as usize;
            let entries = match &state.log {
                Some(buf) if id_from > 0 => buf.since(id_from - 1, n),
                Some(buf) => buf.tail(n),
                None => Vec::new(),
            };
            Ok(Value::Array(
                entries
                    .iter()
                    .map(|r| {
                        json!({
                            "ID": r.id,
                            "Kind": r.kind,
                            "Time": r.time_unix,
                            "Text": r.text,
                        })
                    })
                    .collect(),
            ))
        }
        "fetchfeeds" => {
            if let Some(f) = &state.feeds {
                f.fetch_now();
            }
            Ok(Value::Bool(true))
        }
        "viewfeed" => {
            // viewfeed(ID) — the last poll's items with filter verdicts.
            let Some(feeds) = &state.feeds else {
                return Ok(Value::Array(vec![]));
            };
            let id = p_i64(params, 0).max(0) as u32;
            let items: Vec<Value> = feeds
                .preview(id)
                .iter()
                .map(|p| {
                    let (slo, shi, smb) = split64(p.item.size);
                    json!({
                        "Title": p.item.title,
                        "Filename": format!("{}.nzb", p.item.title),
                        "URL": p.item.url,
                        "SizeLo": slo,
                        "SizeHi": shi,
                        "SizeMB": smb,
                        "Category": p.item.category,
                        "Time": 0,
                        "Match": if p.accepted { "ACCEPTED" } else { "REJECTED" },
                        "Status": if p.new { "NEW" } else { "BACKLOG" },
                    })
                })
                .collect();
            Ok(Value::Array(items))
        }
        "scan" => {
            if let Some(n) = &state.scan_notify {
                n.notify_one();
            }
            Ok(Value::Bool(true))
        }
        "rate" => {
            let kib = p_i64(params, 0).max(0) as u64;
            let limit = if kib == 0 { None } else { Some(kib * 1024) };
            match state.engine.set_speed_limit(limit).await {
                Ok(()) => Ok(Value::Bool(true)),
                Err(_) => Ok(Value::Bool(false)),
            }
        }
        "pausedownload" | "pausedownload2" => {
            let source = current_client(state);
            Ok(Value::Bool(state.engine.pause_all(&source).await.is_ok()))
        }
        "resumedownload" | "resumedownload2" => {
            let source = current_client(state);
            Ok(Value::Bool(state.engine.resume_all(&source).await.is_ok()))
        }
        "writelog" => {
            // writelog(Kind, Text) — scripts and clients inject log lines.
            if let Some(buf) = &state.log {
                let kind = match p_str(params, 0).to_uppercase().as_str() {
                    "ERROR" => "ERROR",
                    "WARNING" => "WARNING",
                    "DETAIL" => "DETAIL",
                    "DEBUG" => "DETAIL",
                    _ => "INFO",
                };
                buf.push(kind, p_str(params, 1));
            }
            Ok(Value::Bool(true))
        }
        _ => Err((1, "Invalid procedure")),
    }
}

fn status_json(state: &CompatState) -> Value {
    let snap = state.engine.snapshot();
    let (rlo, rhi, rmb) = split64(snap.remaining_bytes);
    let (dlo, dhi, dmb) = split64(snap.session_downloaded_bytes);
    let uptime = (nowish() - snap.up_since_unix).max(0);
    let post_jobs = snap
        .jobs
        .iter()
        .filter(|j| matches!(j.status, JobStatus::PostQueued | JobStatus::Post { .. }))
        .count() as u32;
    json!({
        "RemainingSizeLo": rlo,
        "RemainingSizeHi": rhi,
        "RemainingSizeMB": rmb,
        "DownloadedSizeLo": dlo,
        "DownloadedSizeHi": dhi,
        "DownloadedSizeMB": dmb,
        "DownloadRate": snap.download_rate_bps.min(u32::MAX as u64) as u32,
        "AverageDownloadRate": 0,
        "DownloadLimit": snap.speed_limit_bps.unwrap_or(0).min(u32::MAX as u64) as u32,
        "DownloadPaused": snap.download_paused,
        "Download2Paused": snap.download_paused, // deprecated alias
        "PostPaused": false,
        "ScanPaused": false,
        "ServerStandBy": snap.download_rate_bps == 0,
        "UpTimeSec": uptime,
        "DownloadTimeSec": uptime,
        "ThreadCount": 1,
        "PostJobCount": post_jobs,
        "UrlCount": 0,
        "QuotaReached": snap.quota_reached,
        "NewsServers": [],
    })
}

fn listgroups_json(state: &CompatState) -> Value {
    let snap = state.engine.snapshot();
    let groups: Vec<Value> = snap
        .jobs
        .iter()
        .filter(|j| {
            !matches!(
                j.status,
                JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
            )
        })
        .map(|j| {
            let (flo, fhi, fmb) = split64(j.size_bytes);
            let remaining = j.remaining_bytes;
            let (rlo, rhi, rmb) = split64(remaining);
            let (dlo, dhi, dmb) = split64(j.downloaded_bytes);
            json!({
                "NZBID": j.id,
                "FirstID": j.id, // deprecated alias
                "LastID": j.id,  // deprecated alias
                "NZBName": j.name,
                "NZBNicename": j.name, // deprecated alias
                "Kind": "NZB",
                "Status": group_status(j),
                "Category": j.category.clone().unwrap_or_default(),
                "Priority": j.priority,
                "FileSizeLo": flo,
                "FileSizeHi": fhi,
                "FileSizeMB": fmb,
                "RemainingSizeLo": rlo,
                "RemainingSizeHi": rhi,
                "RemainingSizeMB": rmb,
                "DownloadedSizeLo": dlo,
                "DownloadedSizeHi": dhi,
                "DownloadedSizeMB": dmb,
                "PausedSizeLo": 0, "PausedSizeHi": 0, "PausedSizeMB": 0,
                "FileCount": j.files_total,
                "RemainingFileCount": j.files_total - j.files_done,
                "RemainingParCount": 0,
                "Health": j.health,
                "CriticalHealth": j.critical_health,
                "DupeKey": j.dupe_key,
                "DupeScore": j.dupe_score,
                "DupeMode": "SCORE",
                // NZBGet reports a queued job's PP parameters here, and
                // this used to be a hardcoded empty array — so a client
                // that set a tracking id at add time could not read it
                // back until the job reached history. Same values the
                // history projection has always used, same shape.
                "Parameters": j
                    .params
                    .iter()
                    .map(|(k, v)| json!({ "Name": k, "Value": v }))
                    .collect::<Vec<_>>(),
                "ScriptStatuses": [],
                "ServerStats": [],
                "PostInfoText": "NONE",
                "PostStageProgress": 0,
                "PostTotalTimeSec": 0,
                "PostStageTimeSec": 0,
            })
        })
        .collect();
    Value::Array(groups)
}

/// `append` — v13+ form `(NZBFilename, Content, Category, Priority,
/// AddToTop, AddPaused, DupeKey, DupeScore, DupeMode, [PPParameters])` and
/// the legacy pre-13 form `(NZBFilename, Category, Priority, AddToTop,
/// Content)`. Returns the NZBID, or 0 on failure (NZBGet's convention).
async fn append(state: &CompatState, params: &Value) -> Result<Value, (i64, &'static str)> {
    let filename = p_str(params, 0);
    // Which positional form? v13+ carries content at [1]; legacy at [4].
    let (content, category, priority, add_paused, dupe) = match classify_content(&p_str(params, 1))
    {
        Some(c) => (
            Some(c),
            p_str(params, 2),
            p_i64(params, 3) as i32,
            p_bool(params, 5),
            parse_dupe(params, 6),
        ),
        None => (
            classify_content(&p_str(params, 4)),
            p_str(params, 1),
            p_i64(params, 2) as i32,
            false,
            None,
        ),
    };
    let Some(content) = content else {
        tracing::warn!(%filename, "append: content is neither base64 NZB, raw XML, nor a URL");
        return Ok(json!(0));
    };
    let category = (!category.is_empty()).then_some(category);

    // Duplicate check (NZBGet DupeCheck): a same-key success in history or
    // a same-key job in the queue blocks the add (mode-dependent).
    if let Some(d) = &dupe {
        if is_duplicate(state, d).await {
            tracing::info!(%filename, key = %d.key, "append rejected: duplicate");
            record_dupe_reject(state, &filename, d).await;
            return Ok(json!(0));
        }
    }

    match content {
        AppendContent::Nzb(bytes) => {
            let opts = nzbd_engine::AddOpts {
                category,
                priority,
                dupe: dupe.clone(),
                paused: add_paused,
                // The compat surface is frozen: NZBGet's `append` has no
                // params argument, and clients that want them use the
                // separate `editqueue`/`GroupSetParameter` call as before.
                params: Vec::new(),
                client: Some(current_client(state)),
                ..Default::default()
            };
            match state.engine.add_nzb_opts(&filename, &bytes, opts).await {
                Ok(id) => Ok(json!(id.0)),
                Err(e) => {
                    tracing::warn!(%filename, error = %e, "append failed");
                    Ok(json!(0))
                }
            }
        }
        AppendContent::Url(url) => {
            let opts = nzbd_engine::AddOpts {
                category,
                priority,
                dupe: dupe.clone(),
                paused: add_paused,
                // The compat surface is frozen: NZBGet's `append` has no
                // params argument, and clients that want them use the
                // separate `editqueue`/`GroupSetParameter` call as before.
                params: Vec::new(),
                client: Some(current_client(state)),
                ..Default::default()
            };
            match state.engine.add_url(&filename, &url, opts).await {
                Ok(id) => Ok(json!(id.0)),
                Err(e) => {
                    tracing::warn!(%filename, error = %e, "append(url) failed");
                    Ok(json!(0))
                }
            }
        }
    }
}

enum AppendContent {
    Nzb(Vec<u8>),
    Url(String),
}

/// NZBGet accepts base64 NZB content, raw XML, or an HTTP(S) URL in the
/// content slot.
fn classify_content(content: &str) -> Option<AppendContent> {
    let t = content.trim();
    if t.starts_with("http://") || t.starts_with("https://") {
        return Some(AppendContent::Url(t.to_string()));
    }
    decode_nzb(content).map(AppendContent::Nzb)
}

fn parse_dupe(params: &Value, first: usize) -> Option<nzbd_types::DupeInfo> {
    let key = p_str(params, first);
    if key.is_empty() {
        return None;
    }
    let score = p_i64(params, first + 1) as i32;
    let mode = match p_str(params, first + 2).to_uppercase().as_str() {
        "ALL" => nzbd_types::DupeMode::All,
        "FORCE" => nzbd_types::DupeMode::Force,
        _ => nzbd_types::DupeMode::Score,
    };
    Some(nzbd_types::DupeInfo {
        key,
        score,
        mode: Some(mode),
    })
}

/// True when the add must be rejected as a duplicate.
async fn is_duplicate(state: &CompatState, d: &nzbd_types::DupeInfo) -> bool {
    if d.mode == Some(nzbd_types::DupeMode::Force) {
        return false;
    }
    // Queue: a live same-key job blocks (Score: only an equal-or-better one).
    let snap = state.engine.snapshot();
    let queue_hit = snap.jobs.iter().any(|j| {
        j.dupe_key == d.key
            && !matches!(j.status, JobStatus::Failed | JobStatus::Deleted)
            && (d.mode == Some(nzbd_types::DupeMode::All) || j.dupe_score >= d.score)
    });
    if queue_hit {
        return true;
    }
    // History: a same-key SUCCESS blocks (Score: equal-or-better only).
    let Some(db) = &state.history else {
        return false;
    };
    let db = db.clone();
    let entries = tokio::task::spawn_blocking(move || {
        let _ = db.refresh();
        db.list(1000)
    })
    .await;
    match entries {
        Ok(Ok(entries)) => entries.iter().any(|e| {
            e.dupe_key == d.key
                && e.status.starts_with("SUCCESS")
                && (d.mode == Some(nzbd_types::DupeMode::All) || e.dupe_score >= d.score)
        }),
        _ => false,
    }
}

/// The rejected duplicate shows up in history as `DELETED/DUPE` (so *arr
/// clients see a terminal state instead of a silent swallow).
async fn record_dupe_reject(state: &CompatState, filename: &str, d: &nzbd_types::DupeInfo) {
    let Some(db) = &state.history else { return };
    let entry = HistoryEntry {
        job: JobId(0),
        name: filename.trim_end_matches(".nzb").to_string(),
        category: None,
        final_dir: None,
        status: "DELETED/DUPE".into(),
        size: 0,
        health: 1000,
        params: vec![],
        dupe_key: d.key.clone(),
        dupe_score: d.score,
        completed_at_unix: nowish(),
        hidden: false,
        first_seen_at_unix: None,
        last_seen_at_unix: None,
        seen_count: 0,
        removed_at_unix: None,
        picked_up_by: None,
        record: None,
        stages: Vec::new(),
        seq: 0,
    };
    let db = db.clone();
    let _ = tokio::task::spawn_blocking(move || db.record(&entry)).await;
}

async fn history(state: &CompatState, params: &Value) -> Result<Value, (i64, &'static str)> {
    let Some(db) = &state.history else {
        return Ok(Value::Array(vec![]));
    };
    // NZBGet `history(hidden)`: true includes hidden (client-removed) rows.
    let include_hidden = matches!(params.get(0), Some(Value::Bool(true)));
    let ua = state.clients.as_ref().and_then(|c| c.current());
    let db = db.clone();
    let entries = tokio::task::spawn_blocking(move || {
        let entries = db.list_filtered(1000, include_hidden)?;
        // The pull we exist to observe: this poll SAW these entries.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let jobs: Vec<nzbd_types::JobId> = entries.iter().map(|e| e.job).collect();
        let _ = db.mark_seen(&jobs, ua.as_deref(), now);
        Ok::<_, nzbd_state::StateError>(entries)
    })
    .await;
    match entries {
        Ok(Ok(entries)) => Ok(Value::Array(entries.iter().map(history_json).collect())),
        _ => Err((2, "history store error")),
    }
}

/// `editqueue` — v16+ `(Command, Param, IDs)` and v13 `(Command, Offset,
/// Param, IDs)`; both accepted by arity. Returns bool like NZBGet.
async fn editqueue(
    state: &CompatState,
    params: &Value,
    context: Option<&nzbd_api::cluster_removal::ClusterRemovalContext>,
) -> Result<Value, (i64, &'static str)> {
    let command = p_str(params, 0);
    // 4-arg legacy form has a numeric Offset at [1] and IDs at [3].
    let legacy = params.get(3).is_some() && params.get(1).map(|v| v.is_number()).unwrap_or(false);
    let (text, ids) = if legacy {
        (p_str(params, 2), p_ids(params, 3))
    } else {
        (p_str(params, 1), p_ids(params, 2))
    };
    if ids.is_empty() {
        return Ok(Value::Bool(false));
    }

    let mut ok = true;
    for id in ids {
        let done = match command.as_str() {
            "GroupPause" => state.engine.pause_job(id).await.unwrap_or(false),
            "GroupResume" => state.engine.resume_job(id).await.unwrap_or(false),
            "GroupDelete" | "GroupParkDelete" | "GroupTrashDelete" => {
                delete_to_history(state, id, false).await
            }
            "GroupFinalDelete" => {
                if context.is_some() {
                    delete_to_history(state, id, false).await
                } else {
                    state.engine.delete_job(id, true).await.unwrap_or(false)
                }
            }
            "GroupSetPriority" => {
                let prio: i32 = text.parse().unwrap_or(0);
                state.engine.set_priority(id, prio).await.unwrap_or(false)
            }
            "GroupMoveTop" => state
                .engine
                .move_job(id, nzbd_engine::MoveOp::Top)
                .await
                .unwrap_or(false),
            "GroupMoveBottom" => state
                .engine
                .move_job(id, nzbd_engine::MoveOp::Bottom)
                .await
                .unwrap_or(false),
            "GroupMoveUp" => state
                .engine
                .move_job(id, nzbd_engine::MoveOp::Up)
                .await
                .unwrap_or(false),
            "GroupMoveDown" => state
                .engine
                .move_job(id, nzbd_engine::MoveOp::Down)
                .await
                .unwrap_or(false),
            "GroupMoveOffset" => {
                // Approximate a signed offset with repeated single steps.
                let offset: i64 = text.parse().unwrap_or(0);
                let op = if offset < 0 {
                    nzbd_engine::MoveOp::Up
                } else {
                    nzbd_engine::MoveOp::Down
                };
                let mut done = true;
                for _ in 0..offset.unsigned_abs().min(1000) {
                    done &= state.engine.move_job(id, op).await.unwrap_or(false);
                }
                done
            }
            "GroupSetCategory" => state
                .engine
                .set_category(id, (!text.is_empty()).then(|| text.clone()))
                .await
                .unwrap_or(false),
            "GroupSetParameter" => match text.split_once('=') {
                Some((k, _)) if k.starts_with('*') => false,
                Some((k, v)) => {
                    let (k, v) = (k.to_string(), v.to_string());
                    edit_job(state, id, move |j| {
                        if let Some(p) = j.params.iter_mut().find(|(pk, _)| *pk == k) {
                            p.1 = v.clone();
                        } else {
                            j.params.push((k.clone(), v.clone()));
                        }
                    })
                    .await
                }
                None => false,
            },
            "FilePause" | "FileResume" | "FileDelete" => {
                // IDs here are FILE ids; find the owning job in the queue.
                let snap = state.engine.snapshot();
                let mut done = false;
                'jobs: for j in snap.jobs.iter() {
                    let Ok(Some(job)) = state.engine.export_job(j.id).await else {
                        continue;
                    };
                    if job.files.iter().any(|f| f.id.0 == id.0) {
                        let fid = nzbd_types::FileId(id.0);
                        done = match command.as_str() {
                            "FilePause" => state
                                .engine
                                .set_file_paused(job.id, fid, true)
                                .await
                                .unwrap_or(false),
                            "FileResume" => state
                                .engine
                                .set_file_paused(job.id, fid, false)
                                .await
                                .unwrap_or(false),
                            _ => state.engine.delete_file(job.id, fid).await.unwrap_or(false),
                        };
                        break 'jobs;
                    }
                }
                done
            }
            // NZBGet semantics: HistoryDelete HIDES the entry (this is how
            // Sonarr/Radarr clean up after a successful import — we record
            // it as the imported-by signal); FinalDelete removes it.
            "HistoryDelete" => match &state.history {
                Some(db) => {
                    let db = db.clone();
                    let ua = state.clients.as_ref().and_then(|c| c.current());
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    tokio::task::spawn_blocking(move || {
                        db.hide(id, ua.as_deref(), now).unwrap_or(false)
                    })
                    .await
                    .unwrap_or(false)
                }
                None => false,
            },
            "HistoryFinalDelete" => match &state.history {
                Some(db) => {
                    let db = db.clone();
                    tokio::task::spawn_blocking(move || db.delete(id).unwrap_or(false))
                        .await
                        .unwrap_or(false)
                }
                None => false,
            },
            other => {
                tracing::debug!(command = other, "editqueue: unsupported command");
                false
            }
        };
        if done {
            if let Some(context) = context {
                match command.as_str() {
                    "GroupFinalDelete" => context.record(id.0, true),
                    "GroupDelete" | "GroupParkDelete" | "GroupTrashDelete" => {
                        context.record(id.0, false)
                    }
                    _ => {}
                }
            }
        }
        ok &= done;
    }
    Ok(Value::Bool(ok))
}

/// `testserver(Host, Port, Username, Password, Encryption, Cipher,
/// Timeout)` — live connect + greeting + optional AUTHINFO against a news
/// server; returns the greeting text or the error string (NZBGet shape).
/// Same probe as the native `/api/v1/servers/test` endpoint.
async fn testserver(params: &Value) -> Result<Value, (i64, &'static str)> {
    let host = p_str(params, 0);
    let port = p_i64(params, 1).clamp(1, 65535) as u16;
    let user = p_str(params, 2);
    let pass = p_str(params, 3);
    let tls = p_bool(params, 4);
    let timeout = std::time::Duration::from_secs(p_i64(params, 6).clamp(2, 60) as u64);

    let outcome = nzbd_nntp::transport::probe_server(
        &host,
        port,
        tls,
        nzbd_types::CertLevel::Strict,
        (!user.is_empty()).then_some(user.as_str()),
        Some(pass.as_str()),
        timeout,
    )
    .await;
    // NZBGet's shape: always a string, success and failure alike.
    Ok(Value::String(match outcome {
        Ok(msg) => msg,
        Err(msg) => msg,
    }))
}

/// `listfiles(IDFrom, IDTo, NZBID)` — the files of one queued group
/// (modern clients pass 0,0,NZBID).
async fn listfiles(state: &CompatState, params: &Value) -> Result<Value, (i64, &'static str)> {
    let nzbid = if p_i64(params, 2) > 0 {
        p_i64(params, 2)
    } else {
        p_i64(params, 0)
    };
    if nzbid <= 0 {
        return Ok(Value::Array(vec![]));
    }
    let Ok(Some(job)) = state.engine.export_job(JobId(nzbid as u32)).await else {
        return Ok(Value::Array(vec![]));
    };
    let files: Vec<Value> = job
        .files
        .iter()
        .map(|f| {
            let total: u64 = f.segments.iter().map(|s| s.size as u64).sum();
            let done: u64 = f
                .segments
                .iter()
                .filter(|s| matches!(s.state, nzbd_types::SegmentState::Done { .. }))
                .map(|s| s.size as u64)
                .sum();
            let remaining = total.saturating_sub(done);
            let (flo, fhi, _) = split64(total);
            let (rlo, rhi, _) = split64(remaining);
            json!({
                "ID": f.id.0,
                "NZBID": job.id.0,
                "Filename": f.filename,
                "Subject": f.subject,
                "FileSizeLo": flo,
                "FileSizeHi": fhi,
                "RemainingSizeLo": rlo,
                "RemainingSizeHi": rhi,
                "Paused": f.paused,
                "PostTime": f.date.unwrap_or(0),
                "FilenameConfirmed": f.filename_confirmed,
                "ActiveDownloads": 0,
                "CompletedArticles": f.done_segments(),
                "TotalArticles": f.segments.len(),
            })
        })
        .collect();
    Ok(Value::Array(files))
}

/// Delete a queue job the NZBGet way: it becomes a history entry.
async fn delete_to_history(state: &CompatState, id: JobId, with_files: bool) -> bool {
    let exported = state.engine.export_job(id).await.ok().flatten();
    let deleted = state
        .engine
        .delete_job(id, with_files)
        .await
        .unwrap_or(false);
    if deleted {
        if let (Some(db), Some(job)) = (&state.history, exported) {
            let entry = HistoryEntry {
                job: id,
                name: job.name.clone(),
                category: job.category.clone(),
                final_dir: None,
                status: "DELETED/MANUAL".into(),
                size: job.totals.size,
                health: nzbd_types::Health::calc(&job.totals).0,
                params: job
                    .params
                    .iter()
                    .filter(|(k, _)| !k.starts_with('*'))
                    .cloned()
                    .collect(),
                dupe_key: job.dupe.key.clone(),
                dupe_score: job.dupe.score,
                completed_at_unix: nowish(),
                hidden: false,
                first_seen_at_unix: None,
                last_seen_at_unix: None,
                seen_count: 0,
                removed_at_unix: None,
                picked_up_by: None,
                // A job deleted mid-pipeline keeps the stages it did
                // reach — that is often exactly why it was deleted.
                record: None,
                stages: job.stages.clone(),
                seq: 0,
            };
            let db = db.clone();
            let _ = tokio::task::spawn_blocking(move || db.record(&entry)).await;
        }
    }
    deleted
}

/// Export → mutate → import (atomic job replacement).
async fn edit_job<F: FnOnce(&mut nzbd_types::Job)>(state: &CompatState, id: JobId, f: F) -> bool {
    match state.engine.export_job(id).await {
        Ok(Some(mut job)) => {
            f(&mut job);
            state.engine.import_job(job, false, false).await.is_ok()
        }
        _ => false,
    }
}

fn nowish() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// GET form: `/jsonrpc/method?param=…` is NZBGet's URL style; we accept
/// `?method=X&params=[…]` (params = JSON array, URL-encoded).
async fn jsonrpc_get(
    State(state): State<CompatState>,
    headers: axum::http::HeaderMap,
    cluster_removal: Option<axum::Extension<nzbd_api::cluster_removal::ClusterRemovalContext>>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let method = q.get("method").cloned().unwrap_or_default();
    note_client(&state, &headers, &method);
    let params: Value = q
        .get("params")
        .and_then(|p| serde_json::from_str(p).ok())
        .unwrap_or(Value::Array(vec![]));
    let result = dispatch_cluster(
        &state,
        &method,
        &params,
        cluster_removal.as_ref().map(|context| &context.0),
    )
    .await;
    let env = envelope(Value::Null, result);
    match q.get("callback") {
        // JSON-P: cb({...});
        Some(cb) if !cb.is_empty() => (
            [(axum::http::header::CONTENT_TYPE, "application/javascript")],
            format!("{cb}({env});"),
        )
            .into_response(),
        _ => Json(env).into_response(),
    }
}

async fn jsonrpc(
    State(state): State<CompatState>,
    headers: axum::http::HeaderMap,
    cluster_removal: Option<axum::Extension<nzbd_api::cluster_removal::ClusterRemovalContext>>,
    body: String,
) -> Json<Value> {
    let req: Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(_) => return Json(envelope(Value::Null, Err((4, "Parse error")))),
    };
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let method = req
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let params = req.get("params").cloned().unwrap_or(Value::Array(vec![]));
    note_client(&state, &headers, method);
    Json(envelope(
        id,
        dispatch_cluster(
            &state,
            method,
            &params,
            cluster_removal.as_ref().map(|context| &context.0),
        )
        .await,
    ))
}

/// Record who is talking to the compat API (User-Agent), so the UI can
/// show "Radarr polling, last seen 12s ago" and history observations can
/// be stamped with the client that made them.
pub fn note_client(state: &CompatState, headers: &axum::http::HeaderMap, method: &str) {
    if let Some(reg) = &state.clients {
        let ua = headers
            .get(axum::http::header::USER_AGENT)
            .and_then(|v| v.to_str().ok());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        reg.note(ua, method, now);
    }
    // State-changing compat calls are logged with their caller. The
    // polling reads (status/listgroups/history/…) fire every second and
    // stay quiet; anything that CHANGES daemon state must be attributable
    // from the log alone — "the queue keeps unpausing" is always some
    // client issuing these (field report 2026-07-25).
    if matches!(
        method,
        "pausedownload"
            | "pausedownload2"
            | "resumedownload"
            | "resumedownload2"
            | "pausepost"
            | "resumepost"
            | "pausescan"
            | "resumescan"
            | "rate"
            | "shutdown"
            | "reload"
            | "scheduleresume"
    ) {
        let ua = headers
            .get(axum::http::header::USER_AGENT)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown");
        tracing::info!(%method, client = %ua, "compat state-changing call");
    }
}

/// The UA of the client behind the current compat request, for command
/// attribution (best effort — the registry stamps it per request).
fn current_client(state: &CompatState) -> String {
    state
        .clients
        .as_ref()
        .and_then(|c| c.current())
        .unwrap_or_else(|| "compat client".into())
}

pub fn router(state: CompatState) -> Router {
    Router::new()
        .route("/jsonrpc", post(jsonrpc).get(jsonrpc_get))
        .route("/jsonprpc", post(jsonrpc).get(jsonrpc_get))
        .route("/xmlrpc", post(xmlrpc::handle))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nzbd_engine::{Engine, EngineConfig, Tuning};

    async fn test_state(tmp: &tempfile::TempDir) -> CompatState {
        std::fs::create_dir_all(tmp.path().join("dest")).unwrap();
        let engine = Engine::spawn(EngineConfig::single_node(
            vec![],
            tmp.path().join("state"),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let history = HistoryDb::open(&tmp.path().join("history.sqlite"), None).unwrap();
        CompatState {
            config: Arc::new(CompatConfig::default()),
            clients: Some(Arc::new(nzbd_api::ClientRegistry::default())),
            engine,
            history: Some(Arc::new(history)),
            options: Arc::new(vec![
                ("ControlPort".into(), "6789".into()),
                ("Category1.Name".into(), "tv".into()),
            ]),
            log: Some(nzbd_api::LogBuffer::new(100)),
            scan_notify: None,
            feeds: None,
        }
    }

    async fn export_fixture_job(engine: &EngineHandle, id: JobId) -> nzbd_types::Job {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match engine.export_job(id).await {
                    Err(nzbd_engine::EngineError::Lifecycle(reason))
                        if reason == "job verification or writer retirement is still in progress; retry later" =>
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await,
                    result => return result.unwrap().unwrap(),
                }
            }
        }).await.expect("fixture payload verification must complete")
    }

    const NZB: &str = r#"<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
<file poster="p" date="1720000000" subject="&quot;f.bin&quot; yEnc (1/1)">
<groups><group>a.b</group></groups>
<segments><segment bytes="4194304" number="1">m1@x</segment></segments>
</file></nzb>"#;

    #[test]
    fn split64_matches_nzbget_wire_format() {
        let v = (7u64 << 32) | 123;
        let (lo, hi, mb) = split64(v);
        assert_eq!(lo, 123);
        assert_eq!(hi, 7);
        assert_eq!(mb, v / 1024 / 1024);
        assert_eq!(((hi as u64) << 32) | lo as u64, v);
    }

    #[tokio::test]
    async fn version_envelope_and_status_shape() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;

        let result = dispatch(&state, "version", &Value::Null).await.unwrap();
        assert_eq!(result, Value::String("26.2".into()));
        let env = envelope(json!(7), Ok(result));
        assert_eq!(env["version"], "1.1"); // JSON-RPC 1.1 dialect, not 2.0
        assert_eq!(env["id"], 7);
        assert_eq!(env["result"], "26.2");
        assert!(env.get("jsonrpc").is_none());

        let status = dispatch(&state, "status", &Value::Null).await.unwrap();
        for key in [
            "RemainingSizeLo",
            "RemainingSizeHi",
            "RemainingSizeMB",
            "DownloadRate",
            "DownloadPaused",
            "Download2Paused",
            "ServerStandBy",
            "UpTimeSec",
            "PostJobCount",
        ] {
            assert!(status.get(key).is_some(), "missing {key}");
        }
        state.engine.shutdown().await;
    }

    #[tokio::test]
    async fn listgroups_reflects_live_queue_with_triplets() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let id = state
            .engine
            .add_nzb("My Show S01E01", NZB.as_bytes(), Some("tv".into()), 0)
            .await
            .unwrap();

        let groups = dispatch(&state, "listgroups", &Value::Null).await.unwrap();
        let g = &groups[0];
        assert_eq!(g["NZBID"], id.0);
        assert_eq!(g["FirstID"], id.0, "deprecated alias preserved");
        assert_eq!(g["NZBName"], "My Show S01E01");
        assert_eq!(g["Status"], "QUEUED");
        assert_eq!(g["Category"], "tv");
        assert_eq!(g["FileSizeMB"], 4);
        assert_eq!(g["FileSizeLo"], 4_194_304u32);
        assert_eq!(g["Health"], 1000);

        // status remaining reflects the queued article bytes
        let status = dispatch(&state, "status", &Value::Null).await.unwrap();
        assert_eq!(status["RemainingSizeLo"], 4_194_304u32);
        state.engine.shutdown().await;
    }

    #[tokio::test]
    async fn append_v13_form_like_sonarr() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let b64 = base64::engine::general_purpose::STANDARD.encode(NZB);
        // Sonarr: [filename, content, category, priority, addToTop,
        //          addPaused, dupeKey, dupeScore, dupeMode]
        let params = json!([
            "My.Show.S01E01.nzb",
            b64,
            "tv",
            0,
            false,
            true,
            "",
            0,
            "SCORE"
        ]);
        let id = dispatch(&state, "append", &params).await.unwrap();
        assert!(id.as_i64().unwrap() > 0, "append must return the NZBID");

        let groups = dispatch(&state, "listgroups", &Value::Null).await.unwrap();
        assert_eq!(groups[0]["NZBName"], "My.Show.S01E01"); // .nzb stripped
        assert_eq!(groups[0]["Category"], "tv");
        assert_eq!(groups[0]["Status"], "PAUSED", "AddPaused honored");

        // Legacy pre-13 positional form: content at [4].
        let params = json!(["Old.Client.nzb", "movies", 0, false, NZB]);
        let id2 = dispatch(&state, "append", &params).await.unwrap();
        assert!(id2.as_i64().unwrap() > id.as_i64().unwrap());

        // Garbage content → 0, never an error (NZBGet convention).
        let params = json!(["x.nzb", "not-an-nzb", "", 0, false, false, "", 0, "SCORE"]);
        assert_eq!(dispatch(&state, "append", &params).await.unwrap(), json!(0));
        state.engine.shutdown().await;
    }

    #[tokio::test]
    async fn editqueue_pause_resume_delete_and_history() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let id = state
            .engine
            .add_nzb("editme", NZB.as_bytes(), None, 0)
            .await
            .unwrap();

        // v16+ 3-arg form.
        let r = dispatch(&state, "editqueue", &json!(["GroupPause", "", [id.0]]))
            .await
            .unwrap();
        assert_eq!(r, json!(true));
        let groups = dispatch(&state, "listgroups", &Value::Null).await.unwrap();
        assert_eq!(groups[0]["Status"], "PAUSED");

        // v13 4-arg form (offset ignored for non-move commands).
        let r = dispatch(&state, "editqueue", &json!(["GroupResume", 0, "", [id.0]]))
            .await
            .unwrap();
        assert_eq!(r, json!(true));

        let before_forbidden = state.engine.export_job(id).await.unwrap().unwrap().params;
        let forbidden = dispatch(
            &state,
            "editqueue",
            &json!(["GroupSetParameter", "*Runner:LifecycleEligible=1", [id.0]]),
        )
        .await
        .unwrap();
        assert_eq!(forbidden, json!(false));
        let job = state.engine.export_job(id).await.unwrap().unwrap();
        assert_eq!(
            job.params, before_forbidden,
            "reserved parameters cannot be changed by compat clients"
        );

        // Set a parameter then a category, verify via export.
        let r = dispatch(
            &state,
            "editqueue",
            &json!(["GroupSetParameter", "drone=abc123", [id.0]]),
        )
        .await
        .unwrap();
        assert_eq!(r, json!(true));
        let r = dispatch(
            &state,
            "editqueue",
            &json!(["GroupSetCategory", "tv", [id.0]]),
        )
        .await
        .unwrap();
        assert_eq!(r, json!(true));
        let job = export_fixture_job(&state.engine, id).await;
        assert!(job
            .params
            .iter()
            .any(|(k, v)| k == "drone" && v == "abc123"));
        assert_eq!(job.category.as_deref(), Some("tv"));

        // GroupDelete: gone from the queue, present in history as DELETED.
        let r = dispatch(&state, "editqueue", &json!(["GroupDelete", "", [id.0]]))
            .await
            .unwrap();
        assert_eq!(r, json!(true));
        let hist = dispatch(&state, "history", &json!([false])).await.unwrap();
        assert_eq!(hist[0]["NZBID"], id.0);
        assert_eq!(hist[0]["Status"], "DELETED/MANUAL");
        assert_eq!(hist[0]["Name"], "editme");
        let p = hist[0]["Parameters"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["Name"] == "drone")
            .expect("drone param surfaces in history");
        assert_eq!(p["Value"], "abc123");

        // HistoryDelete clears it.
        let r = dispatch(&state, "editqueue", &json!(["HistoryDelete", "", [id.0]]))
            .await
            .unwrap();
        assert_eq!(r, json!(true));
        let hist = dispatch(&state, "history", &json!([false])).await.unwrap();
        assert!(hist.as_array().unwrap().is_empty());
        state.engine.shutdown().await;
    }

    #[tokio::test]
    async fn handoff_pickup_tracking_and_hidden_semantics() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let db = state.history.as_ref().unwrap();
        db.record(&HistoryEntry {
            job: JobId(42),
            name: "picked-up".into(),
            category: Some("monarr".into()),
            final_dir: Some("/dest/picked-up".into()),
            status: "SUCCESS".into(),
            size: 1000,
            health: 1000,
            params: vec![],
            dupe_key: String::new(),
            dupe_score: 0,
            completed_at_unix: 100,
            hidden: false,
            first_seen_at_unix: None,
            last_seen_at_unix: None,
            seen_count: 0,
            removed_at_unix: None,
            picked_up_by: None,
            record: None,
            stages: Vec::new(),
            seq: 0,
        })
        .unwrap();

        // Radarr identifies itself, then polls history: nzbd records the
        // pull — this is the "seen by client" signal.
        state
            .clients
            .as_ref()
            .unwrap()
            .note(Some("Radarr/5.26.2"), "history", 500);
        let r = dispatch(&state, "history", &json!([])).await.unwrap();
        assert_eq!(r.as_array().unwrap().len(), 1);
        let _history_worker = db.start_worker().unwrap();
        let e = tokio::time::timeout(std::time::Duration::from_secs(6), async {
            loop {
                let e = db.list_filtered(10, true).unwrap().remove(0);
                if e.seen_count > 0 {
                    break e;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("background consumer observation");
        assert_eq!(e.seen_count, 1);
        assert!(e.first_seen_at_unix.is_some());
        assert_eq!(e.picked_up_by.as_deref(), Some("Radarr/5.26.2"));

        // Radarr imports and cleans up: HistoryDelete HIDES (not removes)
        // and stamps the imported-by signal.
        let r = dispatch(&state, "editqueue", &json!(["HistoryDelete", "", [42]]))
            .await
            .unwrap();
        assert_eq!(r, Value::Bool(true));
        let visible = dispatch(&state, "history", &json!([])).await.unwrap();
        assert_eq!(visible.as_array().unwrap().len(), 0, "hidden from clients");
        let with_hidden = dispatch(&state, "history", &json!([true])).await.unwrap();
        assert_eq!(
            with_hidden.as_array().unwrap().len(),
            1,
            "history(true) shows it"
        );
        let e = &db.list_filtered(10, true).unwrap()[0];
        assert!(e.hidden);
        assert!(e.removed_at_unix.is_some(), "imported-at recorded");

        // Manual restore: the entry is visible to clients again, so a
        // connected *arr re-imports on its next poll.
        assert!(db.restore(JobId(42)).unwrap());
        let visible = dispatch(&state, "history", &json!([])).await.unwrap();
        assert_eq!(visible.as_array().unwrap().len(), 1);
        let e = &db.list_filtered(10, true).unwrap()[0];
        assert!(!e.hidden && e.removed_at_unix.is_none());

        // FinalDelete really removes.
        let r = dispatch(
            &state,
            "editqueue",
            &json!(["HistoryFinalDelete", "", [42]]),
        )
        .await
        .unwrap();
        assert_eq!(r, Value::Bool(true));
        assert_eq!(db.list_filtered(10, true).unwrap().len(), 0);
    }

    #[tokio::test]
    async fn history_status_mapping_and_config() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let db = state.history.as_ref().unwrap();
        for (native, _wire) in [
            ("SUCCESS", "SUCCESS/ALL"),
            ("PAR_FAILURE", "FAILURE/PAR"),
            ("SCRIPT_FAILURE", "WARNING/SCRIPT"),
            ("FAILURE/HEALTH", "FAILURE/HEALTH"),
        ] {
            db.record(&HistoryEntry {
                job: JobId(native.len() as u32), // distinct ids
                name: native.into(),
                category: None,
                final_dir: Some("/dest/x".into()),
                status: native.into(),
                size: 5 << 32, // exercise the Hi word
                health: 1000,
                params: vec![],
                dupe_key: String::new(),
                dupe_score: 0,
                completed_at_unix: 100 + native.len() as i64,
                hidden: false,
                first_seen_at_unix: None,
                last_seen_at_unix: None,
                seen_count: 0,
                removed_at_unix: None,
                picked_up_by: None,
                record: None,
                stages: Vec::new(),
                seq: 0,
            })
            .unwrap();
        }
        let hist = dispatch(&state, "history", &json!([false])).await.unwrap();
        for h in hist.as_array().unwrap() {
            let name = h["Name"].as_str().unwrap();
            let expected = match name {
                "SUCCESS" => "SUCCESS/ALL",
                "PAR_FAILURE" => "FAILURE/PAR",
                "SCRIPT_FAILURE" => "WARNING/SCRIPT",
                other => other,
            };
            assert_eq!(h["Status"], expected, "{name}");
            assert_eq!(h["FileSizeHi"], 5);
            assert_eq!(h["FinalDir"], "/dest/x");
        }

        let cfg = dispatch(&state, "config", &Value::Null).await.unwrap();
        assert!(cfg
            .as_array()
            .unwrap()
            .iter()
            .any(|o| o["Name"] == "Category1.Name" && o["Value"] == "tv"));
        state.engine.shutdown().await;
    }

    #[tokio::test]
    async fn rate_and_pause_family() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        assert_eq!(
            dispatch(&state, "rate", &json!([2048])).await.unwrap(),
            json!(true)
        );
        assert_eq!(
            state.engine.snapshot().speed_limit_bps,
            Some(2048 * 1024),
            "rate is KiB/s"
        );
        assert_eq!(
            dispatch(&state, "rate", &json!([0])).await.unwrap(),
            json!(true)
        );
        assert_eq!(state.engine.snapshot().speed_limit_bps, None);

        dispatch(&state, "pausedownload", &Value::Null)
            .await
            .unwrap();
        assert!(state.engine.snapshot().download_paused);
        dispatch(&state, "resumedownload", &Value::Null)
            .await
            .unwrap();
        assert!(!state.engine.snapshot().download_paused);
        state.engine.shutdown().await;
    }

    #[tokio::test]
    async fn dupe_check_blocks_and_force_overrides() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let b64 = base64::engine::general_purpose::STANDARD.encode(NZB);

        // Seed history: a SUCCESS with key "show-s01e01" score 100.
        state
            .history
            .as_ref()
            .unwrap()
            .record(&HistoryEntry {
                job: JobId(90),
                name: "prior".into(),
                category: None,
                final_dir: Some("/dest/prior".into()),
                status: "SUCCESS".into(),
                size: 1,
                health: 1000,
                params: vec![],
                dupe_key: "show-s01e01".into(),
                dupe_score: 100,
                completed_at_unix: 50,
                hidden: false,
                first_seen_at_unix: None,
                last_seen_at_unix: None,
                seen_count: 0,
                removed_at_unix: None,
                picked_up_by: None,
                record: None,
                stages: Vec::new(),
                seq: 0,
            })
            .unwrap();

        // Same key, lower score, mode SCORE → rejected (returns 0) and a
        // DELETED/DUPE history row appears.
        let params = json!([
            "dup.nzb",
            b64,
            "",
            0,
            false,
            false,
            "show-s01e01",
            50,
            "SCORE"
        ]);
        assert_eq!(dispatch(&state, "append", &params).await.unwrap(), json!(0));
        let hist = dispatch(&state, "history", &json!([false])).await.unwrap();
        assert!(hist
            .as_array()
            .unwrap()
            .iter()
            .any(|h| h["Status"] == "DELETED/DUPE" && h["DeleteStatus"] == "DUPE"));

        // Higher score → accepted.
        let params = json!([
            "better.nzb",
            b64,
            "",
            0,
            false,
            false,
            "show-s01e01",
            150,
            "SCORE"
        ]);
        let id = dispatch(&state, "append", &params).await.unwrap();
        assert!(id.as_i64().unwrap() > 0, "higher score passes: {id}");

        // Same key again while the better one sits in the QUEUE → rejected.
        let params = json!([
            "again.nzb",
            b64,
            "",
            0,
            false,
            false,
            "show-s01e01",
            150,
            "SCORE"
        ]);
        assert_eq!(dispatch(&state, "append", &params).await.unwrap(), json!(0));

        // FORCE overrides everything.
        let params = json!([
            "forced.nzb",
            b64,
            "",
            0,
            false,
            false,
            "show-s01e01",
            1,
            "FORCE"
        ]);
        assert!(
            dispatch(&state, "append", &params)
                .await
                .unwrap()
                .as_i64()
                .unwrap()
                > 0
        );

        // Queue groups expose the dupe metadata.
        let groups = dispatch(&state, "listgroups", &Value::Null).await.unwrap();
        assert!(groups
            .as_array()
            .unwrap()
            .iter()
            .any(|g| g["DupeKey"] == "show-s01e01" && g["DupeScore"] == 150));
        state.engine.shutdown().await;
    }

    #[tokio::test]
    async fn listfiles_and_file_actions() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let id = state
            .engine
            .add_nzb("filed", NZB.as_bytes(), None, 0)
            .await
            .unwrap();

        let files = dispatch(&state, "listfiles", &json!([0, 0, id.0]))
            .await
            .unwrap();
        assert_eq!(files.as_array().unwrap().len(), 1);
        let fid = files[0]["ID"].as_i64().unwrap();
        assert_eq!(files[0]["Filename"], "f.bin");
        assert_eq!(files[0]["Paused"], false);
        assert_eq!(files[0]["TotalArticles"], 1);

        // FilePause via editqueue (file ids, not group ids).
        let r = dispatch(&state, "editqueue", &json!(["FilePause", "", [fid]]))
            .await
            .unwrap();
        assert_eq!(r, json!(true));
        let files = dispatch(&state, "listfiles", &json!([0, 0, id.0]))
            .await
            .unwrap();
        assert_eq!(files[0]["Paused"], true);

        let r = dispatch(&state, "editqueue", &json!(["FileDelete", "", [fid]]))
            .await
            .unwrap();
        assert_eq!(r, json!(true));
        let files = dispatch(&state, "listfiles", &json!([0, 0, id.0]))
            .await
            .unwrap();
        assert!(files.as_array().unwrap().is_empty());
        state.engine.shutdown().await;
    }

    #[tokio::test]
    async fn log_and_writelog_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        dispatch(&state, "writelog", &json!(["WARNING", "from a script"]))
            .await
            .unwrap();
        let v = dispatch(&state, "log", &json!([0, 50])).await.unwrap();
        let last = v.as_array().unwrap().last().unwrap();
        assert_eq!(last["Kind"], "WARNING");
        assert_eq!(last["Text"], "from a script");
        // Paged fetch from an id.
        let id = last["ID"].as_u64().unwrap();
        let v = dispatch(&state, "log", &json!([id + 1, 50])).await.unwrap();
        assert!(v.as_array().unwrap().is_empty());
        state.engine.shutdown().await;
    }

    #[tokio::test]
    async fn xmlrpc_multicall_end_to_end() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        // version + an unknown method in one multicall.
        let calls = json!([
            { "methodName": "version", "params": [] },
            { "methodName": "nope", "params": [] }
        ]);
        let mut xml = String::from(
            "<?xml version=\"1.0\"?><methodCall><methodName>system.multicall</methodName><params><param>",
        );
        crate::xmlrpc::to_xml(&json!([calls[0].clone(), calls[1].clone()]), &mut xml);
        xml.push_str("</param></params></methodCall>");
        let call = crate::xmlrpc::parse_call(&xml).unwrap();
        assert_eq!(call.name, "system.multicall");
        // Execute through the same dispatch path the endpoint uses.
        let list = call.params[0].as_array().unwrap();
        let mut results = Vec::new();
        for c in list {
            let name = c["methodName"].as_str().unwrap();
            let params = c["params"].clone();
            match dispatch(&state, name, &params).await {
                Ok(v) => results.push(json!([v])),
                Err((code, msg)) => results.push(json!({"faultCode": code, "faultString": msg})),
            }
        }
        assert_eq!(results[0], json!(["26.2"]));
        assert_eq!(results[1]["faultCode"], 1);
        state.engine.shutdown().await;
    }

    #[tokio::test]
    async fn c3_sysinfo_servervolumes_testserver() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;

        let v = dispatch(&state, "sysinfo", &Value::Null).await.unwrap();
        assert_eq!(v["OS"]["Name"], std::env::consts::OS);
        assert!(v["Tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["Name"] == "par2"));

        let v = dispatch(&state, "servervolumes", &Value::Null)
            .await
            .unwrap();
        assert!(v.is_array());

        // testserver against a live nserv: greeting + no auth.
        let post = nzbd_nserv::build_post("t", &[("a.bin", vec![1, 2, 3])], 3);
        let ns = nzbd_nserv::NservBuilder::new()
            .with_post(&post)
            .start()
            .await
            .unwrap();
        let v = dispatch(
            &state,
            "testserver",
            &json!(["127.0.0.1", ns.port(), "", "", false, "", 5]),
        )
        .await
        .unwrap();
        let text = v.as_str().unwrap();
        assert!(text.contains("established"), "{text}");

        // Dead port → failure text, not an RPC error.
        let v = dispatch(
            &state,
            "testserver",
            &json!(["127.0.0.1", 1, "", "", false, "", 2]),
        )
        .await
        .unwrap();
        assert!(v.as_str().unwrap().contains("failed"), "{v}");
        state.engine.shutdown().await;
    }

    #[tokio::test]
    async fn unknown_method_is_error_1() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        assert_eq!(
            dispatch(&state, "nope", &Value::Null).await,
            Err((1, "Invalid procedure"))
        );
        state.engine.shutdown().await;
    }
}
