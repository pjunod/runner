//! Worker role (CLUSTERING.md §6.2): poll the leader for download and
//! post-processing leases, execute them on the local engine (downloads
//! journal to the shared per-job files; PP runs the stage pipeline fenced
//! in `.pp.<lease>/` staging), heartbeat progress, report completions.
//! Leases survive leader failover — the next heartbeat to the new leader
//! adopts them.

use crate::election::LeaderView;
use crate::http::ClusterClient;
use crate::proto::*;
use crate::{ClusterConfig, PpSetup};
use nzbd_engine::{EngineHandle, MirrorStats};
use nzbd_post::manager::{process_job_ctx, PpCtx};
use nzbd_types::{JobId, JobStatus, ServerDef};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Debug, Clone)]
pub struct LeaseState {
    pub job: JobId,
    pub kind: LeaseKind,
    pub token: crate::control::LeaseToken,
    pub job_incarnation: String,
    pub job_revision: u64,
    pub control_revision: u64,
    /// Conservative process-local deadline. Failed HTTP calls never move it.
    pub deadline: Instant,
    /// Revokes owned PP subprocesses immediately when authority is lost.
    pub cancel: CancellationToken,
    /// PP leases only: the pipeline finished locally; the stamped job is
    /// ready to hand to the leader.
    pub pp_ready: bool,
    /// Assembly leases do not import into the download engine. Their
    /// completed, verified job is retained here until durable publication.
    pub ready_job: Option<Box<nzbd_types::Job>>,
}

/// Lease-id → state map, shared with the demotion path (`retain_jobs`).
pub type ActiveLeases = Arc<Mutex<HashMap<String, LeaseState>>>;
type BudgetReceipt = Arc<Mutex<Option<BudgetAck>>>;

#[allow(clippy::too_many_arguments)]
pub fn spawn_worker(
    cfg: ClusterConfig,
    servers: Vec<ServerDef>,
    engine: EngineHandle,
    view: watch::Receiver<LeaderView>,
    client: ClusterClient,
    active: ActiveLeases,
    pp: Option<PpSetup>,
    dest_dir: PathBuf,
    owner_incarnation: String,
    cancel: CancellationToken,
    tracker: &TaskTracker,
) {
    let budget_receipt = Arc::new(Mutex::new(None));
    {
        let cfg = cfg.clone();
        let engine = engine.clone();
        let client = client.clone();
        let active = active.clone();
        let pp = pp.clone();
        let dest_dir = dest_dir.clone();
        let view = view.clone();
        let cancel = cancel.clone();
        tracker.spawn(async move {
            loop {
                if cancel.is_cancelled(){break;}
                let current=view.borrow().clone();
                if !current.is_me {
                    if let Some(url)=current.leader_url().map(str::to_owned) {
                        replay_completion_outbox(&cfg,&engine,&client,&active,&pp,&url).await;
                        report_completions(&cfg,&engine,&client,&active,&pp,&url,&dest_dir).await;
                    }
                }
                tokio::select!{ _=cancel.cancelled()=>break,_=tokio::time::sleep(cfg.lease_interval)=>{} }
            }
        });
    }
    {
        let cfg = cfg.clone();
        let servers = servers.clone();
        let engine = engine.clone();
        let view = view.clone();
        let client = client.clone();
        let active = active.clone();
        let receipt = budget_receipt.clone();
        let cancel = cancel.clone();
        tracker.spawn(async move {
            loop {
                if cancel.is_cancelled(){break;}
                expire_local_leases(&engine,&servers,&active,&receipt).await;
                let current=view.borrow().clone();
                if current.is_me {
                    let cancelled:Vec<_>=active.lock().unwrap().drain().map(|(_,state)|state).collect();
                    for state in cancelled {state.cancel.cancel();}
                }else if let Some(url)=current.leader_url().map(str::to_owned) {
                    heartbeat_and_cancel(&cfg,&servers,&engine,&client,&active,&receipt,&url).await;
                }
                tokio::select!{_=cancel.cancelled()=>break,_=tokio::time::sleep(cfg.lease_interval)=>{}}
            }
            // Pending CRC preparation and held PP are also owned work. Joiners
            // can finish only after their exact authority is visibly revoked.
            let cancelled:Vec<_>=active.lock().unwrap().drain().map(|(_,state)|state).collect();
            for state in cancelled {state.cancel.cancel();let _=engine.remove_job_silent(state.job).await;}
        });
    }
    let work_tracker = tracker.clone();
    tracker.spawn(async move {
        loop {
            if cancel.is_cancelled(){break;}
            let current=view.borrow().clone();
            if !current.is_me {
                if let Some(url)=current.leader_url().map(str::to_owned) {
                    poll_for_work(&cfg,&servers,&engine,&client,&active,&pp,&dest_dir,&work_tracker,&url,&owner_incarnation,&budget_receipt).await;
                }
            }
            tokio::select!{_=cancel.cancelled()=>break,_=tokio::time::sleep(cfg.lease_interval)=>{}}
        }
    });
}

fn progress_of(engine: &EngineHandle, job: JobId) -> MirrorStats {
    engine
        .snapshot()
        .jobs
        .iter()
        .find(|j| j.id == job)
        .map(|j| MirrorStats {
            repair_progress: j.repair_progress.clone(),
            done_articles: j.done_articles,
            failed_articles: j.failed_articles,
            downloaded_bytes: j.downloaded_bytes,
            health: j.health,
            remaining_bytes: Some(j.remaining_bytes),
            stages: j.stages.clone(),
        })
        .unwrap_or_default()
}

/// Accepted immutable completion is durably recorded before the worker
/// forgets its lease/job. A rejected or timestamp-less response preserves scratch.
async fn accept_worker_retirement(
    engine: &EngineHandle,
    request: &CompleteRequest,
    response: &CompleteResponse,
) -> Result<(), String> {
    if !response.ok {
        return Err("authority did not accept completion".into());
    }
    let source = request
        .job
        .params
        .iter()
        .find(|(key, _)| key == "Artifact:Id")
        .map(|(_, value)| value.clone());
    let generation = request
        .job
        .params
        .iter()
        .find(|(key, _)| key == "Artifact:Generation")
        .map(|(_, value)| value.clone());
    let (Some(artifact), Some(generation)) = (source, generation) else {
        return Ok(());
    };
    let accepted_at_ms = response
        .accepted_at_unix_ms
        .ok_or("authority acceptance timestamp missing")?;
    let outcome = request
        .job
        .params
        .iter()
        .find(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
        .map(|(_, value)| value.clone())
        .ok_or("accepted job lacks terminal PP outcome")?;
    let source = nzbd_state::artifacts::SourceGeneration {
        artifact,
        generation,
    };
    let proof = nzbd_state::artifacts::FinalizationProof::Cluster {
        receipt: request.receipt_id.clone(),
        accepted_at_ms,
    };
    let inventory = engine.artifacts();
    let context = nzbd_state::artifacts::WorkerControlContext::for_job(&request.job);
    let job = request.job.id.0;
    tokio::task::spawn_blocking(move || {
        inventory.accept_cluster_completion(&source, &outcome, proof.clone())?;
        if let Some(context)=context {
            if !inventory.acknowledge_worker_completion(job,&context,&source,&proof)? {
                return Err(nzbd_state::artifacts::Error::Conflict("accepted worker completion checkpoint changed; retain outbox for reconciliation".into()));
            }
        }
        Ok(())
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| e.to_string())
}

async fn heartbeat_and_cancel(
    cfg: &ClusterConfig,
    servers: &[ServerDef],
    engine: &EngineHandle,
    client: &ClusterClient,
    active: &ActiveLeases,
    budget_receipt: &BudgetReceipt,
    leader_url: &str,
) {
    let leases: Vec<LeaseProgress> = active
        .lock()
        .unwrap()
        .iter()
        .map(|(id, st)| LeaseProgress {
            lease_id: id.clone(),
            token: st.token.clone(),
            job: st.job,
            kind: st.kind,
            job_revision: st.job_revision,
            control_revision: st.control_revision,
            stats: progress_of(engine, st.job),
        })
        .collect();
    let req = HeartbeatRequest {
        node: cfg.node_name.clone(),
        leases,
        budget_ack: budget_receipt.lock().unwrap().clone(),
    };
    match client
        .post_json::<_, HeartbeatResponse>(leader_url, "/cluster/v1/work/heartbeat", &req)
        .await
    {
        Ok(resp) => {
            let post_fetch_budgeted = resp.post_fetch_budgeted;
            let renewed_by_resource: HashMap<_, _> = resp
                .renewed
                .into_iter()
                .map(|token| (token.resource.clone(), token))
                .collect();
            {
                let mut leases = active.lock().unwrap();
                for (lease_id, state) in leases.iter_mut() {
                    if let Some(next) = renewed_by_resource.get(&state.token.resource) {
                        state.token = next.clone();
                        state.deadline = conservative_deadline(cfg);
                    }
                    if let Some(revision) = resp.controls.get(lease_id) {
                        state.control_revision = *revision;
                    }
                }
            }
            for lease_id in resp.cancel {
                let st = active.lock().unwrap().remove(&lease_id);
                if let Some(st) = st {
                    st.cancel.cancel();
                    tracing::info!(job = st.job.0, %lease_id, "lease cancelled by leader");
                    let _ = engine.remove_job_silent(st.job).await;
                }
            }
            if let Some(budgets) = resp.server_budgets {
                let (has_download, has_post) = {
                    let leases = active.lock().unwrap();
                    (
                        leases
                            .values()
                            .any(|lease| lease.kind == LeaseKind::Download),
                        leases.values().any(|lease| lease.kind == LeaseKind::Post),
                    )
                };
                if has_post && pp_budget_must_park(post_fetch_budgeted, has_download) {
                    apply_budgets(
                        engine,
                        servers,
                        &HashMap::new(),
                        resp.budget_generation,
                        budget_receipt,
                    )
                    .await;
                } else {
                    apply_budgets(
                        engine,
                        servers,
                        &budgets,
                        resp.budget_generation,
                        budget_receipt,
                    )
                    .await;
                }
            }
        }
        Err(e) => tracing::debug!(error = %e, "heartbeat failed (election in progress?)"),
    }
}

async fn report_completions(
    cfg: &ClusterConfig,
    engine: &EngineHandle,
    client: &ClusterClient,
    active: &ActiveLeases,
    pp: &Option<PpSetup>,
    leader_url: &str,
    dest_dir: &std::path::Path,
) {
    let snapshot = engine.snapshot();
    let finished: Vec<(String, LeaseState)> = active
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, st)| match st.kind {
            LeaseKind::Download => snapshot.jobs.iter().any(|j| {
                j.id == st.job && matches!(j.status, JobStatus::Completed | JobStatus::Failed)
            }),
            // A PP job arrives already Completed — only hand it back once
            // the pipeline stamped it.
            LeaseKind::Post => st.pp_ready,
            LeaseKind::Segment => snapshot.jobs.iter().any(|j| {
                j.id == st.job && matches!(j.status, JobStatus::Completed | JobStatus::Failed)
            }),
            LeaseKind::Assemble => st.ready_job.is_some(),
        })
        .map(|(id, st)| (id.clone(), st.clone()))
        .collect();

    for (lease_id, lease) in finished {
        let job_id = lease.job;
        tracing::debug!(node=%cfg.node_name,job=job_id.0,%lease_id,"sealing durable completion");
        let job = if let Some(job) = lease.ready_job.as_deref().cloned() {
            job
        } else {
            let Ok(Some(job)) = engine.export_job(job_id).await else {
                continue;
            };
            job
        };
        if lease.kind == LeaseKind::Segment && job.status != JobStatus::Completed {
            reject_remote_grant(cfg, client, leader_url, &lease_id, &lease.token).await;
            active.lock().unwrap().remove(&lease_id);
            let _ = engine.remove_job_silent(job_id).await;
            continue;
        }
        let sealed = match seal_generation(cfg, dest_dir, &job, &lease, engine.artifacts()).await {
            Ok(sealed) => sealed,
            Err(error) => {
                tracing::warn!(job = job_id.0, %error, "completion generation could not be sealed");
                continue;
            }
        };
        let job = match std::fs::read(sealed.path.join("job.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        {
            Some(job) => job,
            None => continue,
        };
        let receipt_id = format!(
            "complete:{}:{}:{}",
            lease.token.resource, lease.token.fence, sealed.result_id
        );
        let req = CompleteRequest {
            lifecycle_protocol: crate::proto::LIFECYCLE_PROTOCOL,
            node: cfg.node_name.clone(),
            lease_id: lease_id.clone(),
            token: lease.token.clone(),
            expected_job_revision: lease.job_revision,
            result_id: sealed.result_id,
            result_ref: sealed.path.to_string_lossy().into_owned(),
            receipt_id,
            job,
        };
        if let Err(error) = persist_completion(cfg, lease.kind, &req) {
            tracing::warn!(job=job_id.0,%error,"completion outbox commit failed");
            continue;
        }
        let req = std::fs::read(completion_path(cfg, &req))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<PendingCompletion>(&bytes).ok())
            .map(|pending| pending.request)
            .unwrap_or(req);
        if submit_completion(
            cfg, engine, client, active, pp, leader_url, lease.kind, &req,
        )
        .await
        {
            let _ = remove_completion(cfg, &req);
        }
        // One completion/copy/hash at a time; heartbeat and admission run on
        // their independent bounded control lane.
        break;
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PendingCompletion {
    kind: LeaseKind,
    request: CompleteRequest,
}
fn completion_path(cfg: &ClusterConfig, request: &CompleteRequest) -> PathBuf {
    use sha2::{Digest, Sha256};
    cfg.control_dir.join("completion-outbox").join(format!(
        "{:x}.json",
        Sha256::digest(request.receipt_id.as_bytes())
    ))
}
fn persist_completion(
    cfg: &ClusterConfig,
    kind: LeaseKind,
    request: &CompleteRequest,
) -> Result<(), String> {
    let path = completion_path(cfg, request);
    let parent = path.parent().unwrap();
    std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    if path.exists() {
        return Ok(());
    }
    replace_completion(cfg, kind, request)
}
fn replace_completion(
    cfg: &ClusterConfig,
    kind: LeaseKind,
    request: &CompleteRequest,
) -> Result<(), String> {
    let path = completion_path(cfg, request);
    let parent = path.parent().unwrap();
    let bytes = serde_json::to_vec(&serde_json::json!({"kind":kind,"request":request}))
        .map_err(|e| e.to_string())?;
    if bytes.len() > MAX_RPC_BYTES {
        return Err("completion descriptor exceeds durable outbox limit; reduce metadata".into());
    }
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, bytes).map_err(|e| e.to_string())?;
    std::fs::File::open(&temporary)
        .and_then(|file| file.sync_all())
        .map_err(|e| e.to_string())?;
    std::fs::rename(&temporary, &path).map_err(|e| e.to_string())?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|e| e.to_string())
}
fn remove_completion(cfg: &ClusterConfig, request: &CompleteRequest) -> Result<(), String> {
    let path = completion_path(cfg, request);
    std::fs::remove_file(&path).map_err(|e| e.to_string())?;
    std::fs::File::open(path.parent().unwrap())
        .and_then(|directory| directory.sync_all())
        .map_err(|e| e.to_string())
}
async fn replay_completion_outbox(
    cfg: &ClusterConfig,
    engine: &EngineHandle,
    client: &ClusterClient,
    active: &ActiveLeases,
    pp: &Option<PpSetup>,
    leader_url: &str,
) {
    let Ok(entries) = std::fs::read_dir(cfg.control_dir.join("completion-outbox")) else {
        return;
    };
    for entry in entries.flatten().take(32) {
        if entry
            .path()
            .extension()
            .is_none_or(|extension| extension != "json")
        {
            continue;
        }
        if entry
            .metadata()
            .map_or(true, |metadata| metadata.len() > MAX_RPC_BYTES as u64)
        {
            continue;
        }
        let Some(pending) = std::fs::read(entry.path())
            .ok()
            .and_then(|bytes| serde_json::from_slice::<PendingCompletion>(&bytes).ok())
        else {
            continue;
        };
        if pending.request.node != cfg.node_name {
            continue;
        }
        if submit_completion(
            cfg,
            engine,
            client,
            active,
            pp,
            leader_url,
            pending.kind,
            &pending.request,
        )
        .await
        {
            let _ = remove_completion(cfg, &pending.request);
        }
        break;
    }
}
async fn submit_completion(
    cfg: &ClusterConfig,
    engine: &EngineHandle,
    client: &ClusterClient,
    active: &ActiveLeases,
    pp: &Option<PpSetup>,
    leader_url: &str,
    kind: LeaseKind,
    req: &CompleteRequest,
) -> bool {
    let job_id = req.job.id;
    let lease_id = req.lease_id.clone();
    match client
        .post_json::<_, CompleteResponse>(leader_url, "/cluster/v1/work/complete", &req)
        .await
    {
        Ok(resp) if resp.ok => {
            if kind == LeaseKind::Post && !resp.history_recorded_by_authority {
                // Older authorities omit the concrete path. Their selected
                // immutable generation stores payloads under files/.
                let final_dir = resp.final_dir.clone().unwrap_or_else(|| {
                    std::path::Path::new(&req.result_ref)
                        .join("files")
                        .to_string_lossy()
                        .into_owned()
                });
                let mut history_job = req.job.clone();
                if resp.cleanup_only {
                    history_job.params.push((
                        "Cluster:CleanupId".into(),
                        format!("cluster-delivery:{}", req.result_id),
                    ));
                }
                let history_result = match (pp, resp.accepted_at_unix_ms) {
                    (Some(setup), Some(accepted_at_ms)) => {
                        record_published_pp_history(
                            setup.history.clone(),
                            &history_job,
                            (!resp.cleanup_only).then_some(final_dir.as_str()),
                            accepted_at_ms,
                        )
                        .await
                    }
                    _ => Err("durable PP history target or timestamp unavailable".into()),
                };
                if let Err(error) = history_result {
                    tracing::warn!(job = job_id.0, %error, "PP completion awaits durable history");
                    return false;
                }
            }
            if kind == LeaseKind::Post {
                if let Err(error) = accept_worker_retirement(engine, req, &resp).await {
                    tracing::warn!(job=job_id.0,%error,"remote workspace retirement awaits durable acceptance recording");
                    return false;
                }
            }
            tracing::info!(job = job_id.0, %lease_id, "completion handed to leader");
            let removed = {
                let mut leases = active.lock().unwrap();
                if leases.get(&lease_id).is_some_and(|state| {
                    state.token.fence == req.token.fence
                        && state.token.owner_incarnation == req.token.owner_incarnation
                }) {
                    leases.remove(&lease_id);
                    true
                } else {
                    false
                }
            };
            if removed {
                let _ = engine.remove_job_silent(job_id).await;
            }
            return true;
        }
        Ok(_) => tracing::debug!(
            job = job_id.0,
            "completion authority returned a negative receipt"
        ),
        Err(error) => {
            if error.contains("stale lease token or job revision") {
                let successor = active
                    .lock()
                    .unwrap()
                    .get(&lease_id)
                    .filter(|state| {
                        state.job_revision == req.expected_job_revision
                            && state.token.resource == req.token.resource
                            && state.token.fence == req.token.fence
                            && state.token.owner_incarnation == req.token.owner_incarnation
                            && state.token != req.token
                    })
                    .map(|state| state.token.clone());
                if let Some(token) = successor {
                    // A definite stale-token refusal proves this predecessor
                    // was not committed. Persist its exact renewed successor
                    // before retrying the unchanged completion receipt.
                    let mut renewed = req.clone();
                    renewed.token = token;
                    if let Err(error) = replace_completion(cfg, kind, &renewed) {
                        tracing::warn!(job=job_id.0,%error,"completion token refresh was not persisted");
                    }
                }
            }
            tracing::debug!(job=job_id.0,%error,"durable completion awaits authority acceptance");
        }
    }
    false
}

async fn record_published_pp_history(
    history: Arc<nzbd_state::history::HistoryDb>,
    job: &nzbd_types::Job,
    result_ref: Option<&str>,
    accepted_at_ms: i64,
) -> Result<(), String> {
    let status = job
        .params
        .iter()
        .find(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
        .map(|(_, value)| value.clone())
        .unwrap_or_else(|| "SUCCESS".into());
    let entry = nzbd_state::HistoryEntry {
        job: job.id,
        name: job.name.clone(),
        category: job.category.clone(),
        final_dir: result_ref.map(str::to_owned),
        status,
        size: job.totals.size,
        health: nzbd_types::Health::calc(&job.totals).0,
        params: job
            .params
            .iter()
            .filter(|(key, _)| !key.starts_with('*'))
            .cloned()
            .collect(),
        dupe_key: job.dupe.key.clone(),
        dupe_score: job.dupe.score,
        completed_at_unix: accepted_at_ms / 1000,
        hidden: false,
        first_seen_at_unix: None,
        last_seen_at_unix: None,
        seen_count: 0,
        removed_at_unix: None,
        picked_up_by: None,
        record: Some(nzbd_state::JobRecord::from_job(job)),
        stages: job.stages.clone(),
        seq: 0,
    };
    tokio::task::spawn_blocking(move || history.record_seq(&entry))
        .await
        .map_err(|error| format!("join durable history write: {error}"))?
        .map(|_| ())
        .map_err(|error| format!("write durable PP history: {error}"))
}

#[allow(clippy::too_many_arguments)]
async fn poll_for_work(
    cfg: &ClusterConfig,
    servers: &[ServerDef],
    engine: &EngineHandle,
    client: &ClusterClient,
    active: &ActiveLeases,
    pp: &Option<PpSetup>,
    dest_dir: &std::path::Path,
    tracker: &TaskTracker,
    leader_url: &str,
    owner_incarnation: &str,
    budget_receipt: &BudgetReceipt,
) {
    let disk_low = engine.snapshot().disk_low;
    let (dl_held, pp_held) = {
        let a = active.lock().unwrap();
        (
            a.values()
                .filter(|s| {
                    matches!(
                        s.kind,
                        LeaseKind::Download | LeaseKind::Segment | LeaseKind::Assemble
                    )
                })
                .count() as u32,
            a.values().filter(|s| s.kind == LeaseKind::Post).count() as u32,
        )
    };
    let free_dl = if cfg.download && !disk_low {
        cfg.max_download_jobs.saturating_sub(dl_held)
    } else {
        0
    };
    let free_pp = if cfg.post_process && pp.is_some() && !disk_low {
        cfg.pp_slots.saturating_sub(pp_held)
    } else {
        0
    };
    if free_dl == 0 && free_pp == 0 {
        return;
    }
    let req = PollRequest {
        lifecycle_protocol: crate::proto::LIFECYCLE_PROTOCOL,
        node: cfg.node_name.clone(),
        owner_incarnation: owner_incarnation.to_owned(),
        free_download_slots: free_dl,
        free_pp_slots: free_pp,
    };
    let resp = match client
        .post_json::<_, PollResponse>(leader_url, "/cluster/v1/work/poll", &req)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(error = %e, "work poll failed");
            return;
        }
    };
    for mut grant in resp.grants {
        let job_id = grant.job.id;
        if grant.job.delivery_lifecycle && grant.lifecycle_protocol < LIFECYCLE_PROTOCOL {
            tracing::warn!(job=job_id.0,"leader requires cluster lifecycle protocol 2; grant refused before payload mutation");
            reject_remote_grant(cfg, client, leader_url, &grant.lease_id, &grant.token).await;
            continue;
        }
        if engine.snapshot().disk_low {
            reject_remote_grant(cfg, client, leader_url, &grant.lease_id, &grant.token).await;
            continue;
        }
        match grant.kind {
            LeaseKind::Download => {
                tracing::info!(job = job_id.0, lease = %grant.lease_id, "download lease received");
                apply_budgets(
                    engine,
                    servers,
                    &grant.server_budgets,
                    grant.budget_generation,
                    budget_receipt,
                )
                .await;
                // Register before the off-actor CRC preparation so the
                // independent control lane renews/cancels this exact grant.
                let lease_id = grant.lease_id.clone();
                let original_token = grant.token.clone();
                let expected_job = grant.job_revision;
                let expected_control = grant.control_revision;
                active.lock().unwrap().insert(
                    lease_id.clone(),
                    LeaseState {
                        job: job_id,
                        kind: LeaseKind::Download,
                        token: grant.token.clone(),
                        job_incarnation: grant.job_incarnation,
                        job_revision: expected_job,
                        control_revision: expected_control,
                        deadline: conservative_deadline(cfg),
                        cancel: CancellationToken::new(),
                        pp_ready: false,
                        ready_job: None,
                    },
                );
                let checkpoint = {
                    let active = active.clone();
                    let lease_id = lease_id.clone();
                    let token = original_token.clone();
                    Arc::new(move || {
                        active.lock().unwrap().get(&lease_id).is_some_and(|state| {
                            state.job == job_id
                                && state.job_revision == expected_job
                                && state.control_revision == expected_control
                                && state.token.resource == token.resource
                                && state.token.owner_node_id == token.owner_node_id
                                && state.token.owner_incarnation == token.owner_incarnation
                                && state.token.fence == token.fence
                                && state.token.revision >= token.revision
                                && state.deadline > Instant::now()
                                && !state.cancel.is_cancelled()
                        })
                    })
                };
                if engine
                    .import_job_checked(grant.job, true, false, checkpoint)
                    .await
                    .is_err()
                    || engine.snapshot().disk_low
                {
                    let removed = {
                        let mut leases = active.lock().unwrap();
                        if leases.get(&lease_id).is_some_and(|state| {
                            state.token.fence == original_token.fence
                                && state.token.owner_incarnation == original_token.owner_incarnation
                        }) {
                            leases.remove(&lease_id)
                        } else {
                            None
                        }
                    };
                    if let Some(state) = removed {
                        state.cancel.cancel();
                        let _ = engine.remove_job_silent(job_id).await;
                        reject_remote_grant(cfg, client, leader_url, &lease_id, &state.token).await;
                    }
                }
            }
            LeaseKind::Post => {
                let Some(setup) = pp else { continue };
                tracing::info!(job = job_id.0, lease = %grant.lease_id, "pp lease received");
                if grant.post_fetch_budgeted {
                    apply_budgets(
                        engine,
                        servers,
                        &grant.server_budgets,
                        grant.budget_generation,
                        budget_receipt,
                    )
                    .await;
                } else if !active
                    .lock()
                    .unwrap()
                    .values()
                    .any(|lease| lease.kind == LeaseKind::Download)
                {
                    // Old leaders sent an empty PP grant and did not count PP
                    // in heartbeat budgets. Keep a PP-only recovery lane
                    // parked. A concurrent Download lease is safe: the old
                    // leader already counts this node through that lease.
                    apply_budgets(
                        engine,
                        servers,
                        &HashMap::new(),
                        grant.budget_generation,
                        budget_receipt,
                    )
                    .await;
                }
                grant
                    .job
                    .params
                    .retain(|(key, _)| key != nzbd_state::artifacts::WORKER_CONTROL_CONTEXT_PARAM);
                let context = nzbd_state::artifacts::WorkerControlContext {
                    lease_id: grant.lease_id.clone(),
                    job_incarnation: grant.job_incarnation.clone(),
                    job_revision: grant.job_revision,
                    control_revision: grant.control_revision,
                    epoch: grant.epoch,
                };
                grant.job.params.push((
                    nzbd_state::artifacts::WORKER_CONTROL_CONTEXT_PARAM.into(),
                    serde_json::to_string(&context).expect("worker context serializes"),
                ));
                if let Err(error) = prepare_pp_attempt(
                    engine.artifacts(),
                    dest_dir,
                    &mut grant.job,
                    grant.token.fence,
                )
                .await
                {
                    tracing::warn!(job = job_id.0, %error, "private PP attempt could not be prepared");
                    reject_remote_grant(cfg, client, leader_url, &grant.lease_id, &grant.token)
                        .await;
                    continue;
                }
                let restored_ready = grant
                    .job
                    .params
                    .iter()
                    .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM);
                if engine.import_job(grant.job, false, false).await.is_ok() {
                    if engine.snapshot().disk_low {
                        let _ = engine.remove_job_silent(job_id).await;
                        reject_remote_grant(cfg, client, leader_url, &grant.lease_id, &grant.token)
                            .await;
                        continue;
                    }
                    active.lock().unwrap().insert(
                        grant.lease_id.clone(),
                        LeaseState {
                            job: job_id,
                            kind: LeaseKind::Post,
                            token: grant.token.clone(),
                            job_incarnation: grant.job_incarnation,
                            job_revision: grant.job_revision,
                            control_revision: grant.control_revision,
                            deadline: conservative_deadline(cfg),
                            cancel: CancellationToken::new(),
                            pp_ready: restored_ready,
                            ready_job: None,
                        },
                    );
                    if !restored_ready {
                        run_pp_lease(
                            engine.clone(),
                            setup.clone(),
                            dest_dir.to_path_buf(),
                            active.clone(),
                            grant.lease_id,
                            job_id,
                            tracker,
                            cfg.node_name.clone(),
                            client.clone(),
                            leader_url.to_string(),
                        );
                    }
                }
            }
            LeaseKind::Segment => {
                tracing::info!(job = job_id.0, lease = %grant.lease_id, "article-range lease received");
                apply_budgets(
                    engine,
                    servers,
                    &grant.server_budgets,
                    grant.budget_generation,
                    budget_receipt,
                )
                .await;
                if engine.import_job(grant.job, false, false).await.is_ok() {
                    active.lock().unwrap().insert(
                        grant.lease_id,
                        LeaseState {
                            job: job_id,
                            kind: LeaseKind::Segment,
                            token: grant.token,
                            job_incarnation: grant.job_incarnation,
                            job_revision: grant.job_revision,
                            control_revision: grant.control_revision,
                            deadline: conservative_deadline(cfg),
                            cancel: CancellationToken::new(),
                            pp_ready: false,
                            ready_job: None,
                        },
                    );
                }
            }
            LeaseKind::Assemble => {
                let Ok(scope) = serde_json::from_value::<crate::range::AssembleScope>(grant.scope)
                else {
                    reject_remote_grant(cfg, client, leader_url, &grant.lease_id, &grant.token)
                        .await;
                    continue;
                };
                let lease_id = grant.lease_id.clone();
                let token = grant.token.clone();
                let fence = token.fence;
                active.lock().unwrap().insert(
                    lease_id.clone(),
                    LeaseState {
                        job: job_id,
                        kind: LeaseKind::Assemble,
                        token,
                        job_incarnation: grant.job_incarnation,
                        job_revision: grant.job_revision,
                        control_revision: grant.control_revision,
                        deadline: conservative_deadline(cfg),
                        cancel: CancellationToken::new(),
                        pp_ready: false,
                        ready_job: None,
                    },
                );
                run_assembly(
                    grant.job,
                    scope,
                    dest_dir.to_path_buf(),
                    fence,
                    active.clone(),
                    lease_id,
                    tracker,
                );
            }
        }
    }
}

async fn reject_remote_grant(
    cfg: &ClusterConfig,
    client: &ClusterClient,
    leader_url: &str,
    lease_id: &str,
    token: &crate::control::LeaseToken,
) {
    let req = RejectRequest {
        node: cfg.node_name.clone(),
        lease_id: lease_id.to_string(),
        token: token.clone(),
    };
    match client
        .post_json::<_, RejectResponse>(leader_url, "/cluster/v1/work/reject", &req)
        .await
    {
        Ok(response) if response.released => {
            tracing::info!(%lease_id, "rejected stale grant after disk guard changed")
        }
        Ok(_) => tracing::debug!(%lease_id, "stale grant was already released"),
        Err(error) => tracing::warn!(
            %lease_id,
            %error,
            "could not reject stale grant; it will expire without being started"
        ),
    }
}

/// Execute one PP lease: the stage pipeline fenced by the lease id, with a
/// commit check against the live lease map (a cancelled/reclaimed lease
/// must never publish results or stamp the job).
#[allow(clippy::too_many_arguments)]
fn run_pp_lease(
    engine: EngineHandle,
    mut setup: PpSetup,
    dest_dir: PathBuf,
    active: ActiveLeases,
    lease_id: String,
    job_id: JobId,
    tracker: &TaskTracker,
    node: String,
    client: ClusterClient,
    leader_url: String,
) {
    // A remote attempt may transform only its private generation. Category
    // publication is selected by the authority after the result receipt.
    setup.post.completed_dir = None;
    for rule in &mut setup.post.categories {
        rule.dest_dir = None;
    }
    tracker.spawn(async move {
        let Some(initial) = active.lock().unwrap().get(&lease_id).cloned() else {
            return;
        };
        if engine.snapshot().disk_low {
            active.lock().unwrap().remove(&lease_id);
            let _ = engine.remove_job_silent(job_id).await;
            let req = RejectRequest {
                node,
                lease_id: lease_id.clone(),
                token: initial.token,
            };
            let _ = client
                .post_json::<_, RejectResponse>(
                    &leader_url,
                    "/cluster/v1/work/reject",
                    &req,
                )
                .await;
            return;
        }
        let script_receipt: nzbd_post::manager::ScriptReceiptHook = Arc::new({
            let active = active.clone();
            let lease_id = lease_id.clone();
            let node = node.clone();
            let client = client.clone();
            let leader_url = leader_url.clone();
            let job_incarnation = initial.job_incarnation.clone();
            move |action| {
                let active = active.clone();
                let lease_id = lease_id.clone();
                let node = node.clone();
                let client = client.clone();
                let leader_url = leader_url.clone();
                let job_incarnation = job_incarnation.clone();
                Box::pin(async move {
                    use nzbd_post::manager::{ScriptReceiptAction, ScriptReceiptDecision};
                    use sha2::{Digest, Sha256};
                    let (script, finish) = match action {
                        ScriptReceiptAction::Begin { script } => (script, false),
                        ScriptReceiptAction::Finish { script } => (script, true),
                    };
                    let token = active
                        .lock()
                        .unwrap()
                        .get(&lease_id)
                        .map(|state| state.token.clone())
                        .ok_or_else(|| "PP lease is no longer active".to_owned())?;
                    let script_key = format!("{:x}", Sha256::digest(script.as_bytes()));
                    let request = ScriptReceiptRequest {
                        node,
                        lease_id,
                        token,
                        receipt_id: format!("script:{job_incarnation}:{script_key}"),
                        finish,
                    };
                    let response: ScriptReceiptResponse = client
                        .post_json(
                            &leader_url,
                            "/cluster/v1/work/script-receipt",
                            &request,
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                    Ok(match response.decision.as_str() {
                        "run" => ScriptReceiptDecision::Run,
                        "done" => ScriptReceiptDecision::AlreadyDone,
                        _ => ScriptReceiptDecision::Ambiguous,
                    })
                })
            }
        });
        let ctx = PpCtx {
            cancel: initial.cancel.child_token(),
            workers: TaskTracker::new(),
            tag: lease_id.clone(),
            publish_history: false,
            script_receipt: Some(script_receipt),
            extra_env: vec![
                ("NZBCLUSTER_JOB_INCARNATION".into(), initial.job_incarnation.clone()),
                ("NZBCLUSTER_LEASE_ID".into(), lease_id.clone()),
                ("NZBCLUSTER_LEASE_RESOURCE".into(), initial.token.resource.clone()),
                ("NZBCLUSTER_LEASE_FENCE".into(), initial.token.fence.to_string()),
                ("NZBCLUSTER_LEASE_REVISION".into(), initial.token.revision.to_string()),
            ],
            script_receipt_prefix: Some(format!("script:{}:", initial.job_incarnation)),
            commit_ok: Arc::new({
                let active = active.clone();
                let lease_id = lease_id.clone();
                move || {
                    active
                        .lock()
                        .unwrap()
                        .get(&lease_id)
                        .is_some_and(|state| state.deadline > Instant::now())
                }
            }),
        };
        loop {
            if initial.cancel.is_cancelled(){break;}
            // Restored checkpoints keep their original absolute deadline and
            // first blocked time. Heartbeats run independently while waiting.
            if let Some(control)=engine.export_job(job_id).await.ok().flatten().and_then(|job|job.control()).filter(|control|control.lifecycle=="held") {
                if control.retry_policy!="scheduled_retry" || control.manual_pause {
                    tokio::select!{_=initial.cancel.cancelled()=>break,_=tokio::time::sleep(std::time::Duration::from_secs(1))=>{}}
                    continue;
                }
                let now=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
                if control.next_retry_at_unix.is_some_and(|deadline|deadline>now) {
                    tokio::select!{_=initial.cancel.cancelled()=>break,_=tokio::time::sleep(std::time::Duration::from_millis(250))=>{}}
                    continue;
                }
                if !engine.retry_post_hold(job_id,control.revision).await.unwrap_or(false) {
                    tokio::select!{_=initial.cancel.cancelled()=>break,_=tokio::time::sleep(std::time::Duration::from_secs(1))=>{}}
                    continue;
                }
            }
            let mut attempt=ctx.clone();attempt.cancel=initial.cancel.child_token();attempt.workers=TaskTracker::new();
            let result=tokio::select!{
                _=initial.cancel.cancelled()=>Err(nzbd_post::PostError::Subprocess("PP lease authority expired".into())),
                result=process_job_ctx(&engine,&setup.post,&setup.history,&dest_dir,job_id,&attempt)=>result,
            };
            attempt.cancel.cancel();attempt.workers.close();attempt.workers.wait().await;
            match result {
                Ok(outcome)=>{
                    tracing::info!(job=job_id.0,%lease_id,outcome=outcome.as_str(),"pp lease finished");
                    if let Some(state)=active.lock().unwrap().get_mut(&lease_id){state.pp_ready=true;}
                    return;
                },
                Err(error)=>{
                    if !initial.cancel.is_cancelled() && nzbd_post::manager::retain_worker_post_retry(&engine,job_id,&error).await {
                        tracing::warn!(job=job_id.0,%lease_id,%error,"leased post attempt retained for scheduled retry");
                        continue;
                    }
                    if !initial.cancel.is_cancelled() {
                        // Review/Keep holds retain the owned attempt and remain
                        // visible; operator action or lease revocation decides.
                        tokio::select!{_=initial.cancel.cancelled()=>break,_=tokio::time::sleep(std::time::Duration::from_secs(1))=>{}}
                        continue;
                    }
                    break;
                }
            }
        }
        if active.lock().unwrap().remove(&lease_id).is_some(){let _=engine.remove_job_silent(job_id).await;}

    });
}

async fn prepare_pp_attempt(
    inventory: Arc<nzbd_state::artifacts::Inventory>,
    dest_dir: &std::path::Path,
    job: &mut nzbd_types::Job,
    fence: u64,
) -> Result<(), String> {
    let original_dir = nzbd_engine::queue::job_dir_name(job);
    let source = dest_dir.join(&original_dir);
    let relative = PathBuf::from(format!(
        ".nzbd-cluster/pp-work/job-{}/fence-{fence}",
        job.id.0
    ));
    let target = dest_dir.join(&relative);
    if let Some(context) = nzbd_state::artifacts::WorkerControlContext::for_job(job) {
        if let Some(adoption) = inventory
            .adopt_worker_retry(job.id.0, &context)
            .map_err(|error| error.to_string())?
        {
            if let Some(mut completed) = adoption.completed_job {
                completed
                    .params
                    .retain(|(key, _)| key != nzbd_state::artifacts::WORKER_CONTROL_CONTEXT_PARAM);
                completed.params.push((
                    nzbd_state::artifacts::WORKER_CONTROL_CONTEXT_PARAM.into(),
                    serde_json::to_string(&context).map_err(|error| error.to_string())?,
                ));
                *job = completed;
                return Ok(());
            }
            let private = adoption
                .source
                .path
                .strip_prefix(dest_dir)
                .map_err(|_| "retry source is outside configured processing root")?;
            if private
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
                || !private.starts_with(".nzbd-cluster/pp-work")
            {
                return Err("retry source is not a safe private cluster attempt".into());
            }
            job.dir_name = private.to_string_lossy().into_owned();
            job.set_control(&adoption.control);
            job.status = JobStatus::Paused;
            for (key, value) in adoption.probe_params {
                job.params.retain(|(existing, _)| existing != &key);
                job.params.push((key, value));
            }
            job.params
                .push(("*Cluster:original-dir".into(), original_dir));
            return Ok(());
        }
    }
    let limit = job
        .totals
        .size
        .saturating_mul(2)
        .saturating_add(64 * 1024 * 1024);
    let source_for_copy = source.clone();
    let target_for_copy = target.clone();
    let job_id = job.id.0;
    let lifecycle_eligible = job.lifecycle_eligible();
    let delivery_lifecycle = job.delivery_lifecycle;
    let keep_on_failure = job
        .params
        .iter()
        .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1");
    tokio::task::spawn_blocking(move || {
        let parent = target_for_copy
            .parent()
            .ok_or_else(|| "private PP target has no parent".to_owned())?;
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("create private PP parent: {error}"))?;
        let artifact = inventory
            .allocate_processing_successor_with_policy(
                job_id,
                parent,
                &target_for_copy,
                Some((lifecycle_eligible, delivery_lifecycle, keep_on_failure)),
            )
            .map_err(|error| format!("allocate owned private PP input: {error}"))?;
        // The actual copy worker retains custody even if its async caller drops.
        let _use_guard = inventory
            .acquire_attempt_use(job_id)
            .map_err(|error| format!("acquire private PP copy custody: {error}"))?;

        let mut bytes = 0;
        let mut files = Vec::new();
        if let Err(error) = copy_generation_tree(
            &source_for_copy,
            &target_for_copy,
            std::path::Path::new(""),
            limit,
            &mut bytes,
            &mut files,
        ) {
            let _ = inventory.note_error(&artifact.id, &error);
            return Err(error);
        }
        Ok::<(), String>(())
    })
    .await
    .map_err(|error| format!("private PP copy task failed: {error}"))??;
    job.dir_name = relative.to_string_lossy().into_owned();
    job.params
        .push(("*Cluster:original-dir".into(), original_dir));
    Ok(())
}

fn run_assembly(
    job: nzbd_types::Job,
    scope: crate::range::AssembleScope,
    dest_dir: PathBuf,
    fence: u64,
    active: ActiveLeases,
    lease_id: String,
    tracker: &TaskTracker,
) {
    tracker.spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            crate::range::assemble(job, &scope, &dest_dir, fence)
        })
        .await;
        match result {
            Ok(Ok(job)) => {
                if let Some(state) = active.lock().unwrap().get_mut(&lease_id) {
                    if state.deadline > Instant::now() {
                        state.ready_job = Some(Box::new(job));
                    }
                }
            }
            Ok(Err(error)) => tracing::warn!(%lease_id, %error, "range assembly failed"),
            Err(error) => tracing::warn!(%lease_id, %error, "range assembly task failed"),
        }
    });
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct GenerationManifest {
    pub(crate) job_id: u32,
    pub(crate) job_incarnation: String,
    pub(crate) owner_node_id: String,
    pub(crate) fence: u64,
    pub(crate) result_id: String,
    pub(crate) job_sha256: String,
    pub(crate) files: Vec<GenerationFile>,
    pub(crate) total_bytes: u64,
    pub(crate) sealed_at_unix_ms: i64,
    #[serde(default)]
    pub(crate) artifact_copy: Option<nzbd_state::artifacts::ClusterArtifactCopy>,
    #[serde(default)]
    pub(crate) directory_identity: Option<(u64, u64)>,
    #[serde(default)]
    pub(crate) file_outcomes: Vec<nzbd_state::artifacts::DeliveryFileOutcome>,
    #[serde(default)]
    pub(crate) identity_version: u8,
    #[serde(default)]
    pub(crate) source_disposition: Option<String>,
}

impl GenerationManifest {
    fn computed_identity(&self) -> Result<String, String> {
        use sha2::{Digest, Sha256};
        let bytes = if self.identity_version >= 3 {
            serde_json::to_vec(&(
                self.job_id,
                &self.job_incarnation,
                &self.owner_node_id,
                self.fence,
                &self.job_sha256,
                &self.files,
                &self.artifact_copy,
                &self.directory_identity,
                &self.file_outcomes,
                &self.source_disposition,
            ))
        } else if self.identity_version >= 2 {
            serde_json::to_vec(&(
                self.job_id,
                &self.job_incarnation,
                &self.owner_node_id,
                self.fence,
                &self.job_sha256,
                &self.files,
                &self.artifact_copy,
                &self.directory_identity,
                &self.file_outcomes,
            ))
        } else {
            serde_json::to_vec(&(
                self.job_id,
                &self.job_incarnation,
                self.fence,
                &self.job_sha256,
                &self.files,
            ))
        }
        .map_err(|error| error.to_string())?;
        Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
    }
    pub(crate) fn identity_matches(&self) -> bool {
        self.computed_identity()
            .is_ok_and(|computed| computed == self.result_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct GenerationFile {
    pub(crate) path: String,
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
}

struct SealedGeneration {
    result_id: String,
    path: PathBuf,
}

/// Owns the private staging tree until its atomic rename into the immutable
/// generation slot succeeds. Any copy, hashing, or sync error must release
/// this duplicate rather than leaving another full payload behind.
struct BuildingGeneration(PathBuf);

impl Drop for BuildingGeneration {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug)]
enum SealSource {
    Selected(nzbd_state::artifacts::SelectedPublication),
    Legacy(PathBuf),
}

fn resolve_post_seal_source(
    inventory: &nzbd_state::artifacts::Inventory,
    dest_dir: &std::path::Path,
    job: &nzbd_types::Job,
) -> Result<Option<SealSource>, String> {
    let expected_source = dest_dir.join(nzbd_engine::queue::job_dir_name(job));
    let source = job
        .params
        .iter()
        .find(|(key, _)| key == "Artifact:Id")
        .zip(
            job.params
                .iter()
                .find(|(key, _)| key == "Artifact:Generation"),
        )
        .map(
            |((_, artifact), (_, generation))| nzbd_state::artifacts::SourceGeneration {
                artifact: artifact.clone(),
                generation: generation.clone(),
            },
        );
    let publication = match &source {
        Some(source) => inventory.selected_publication_for_generation(source, &expected_source),
        None => inventory.selected_publication_for_source(job.id.0, &expected_source),
    };
    if let Some(publication) =
        publication.map_err(|error| format!("read selected PP publication journal: {error}"))?
    {
        if !publication.path.is_dir() {
            return Err(format!(
                "journaled selected PP publication is missing: {}",
                publication.path.display()
            ));
        }
        return Ok(Some(SealSource::Selected(publication)));
    }

    // Pre-lifecycle jobs retain their existing whole-directory move contract.
    // This compatibility path grants no new cleanup or delivery authority.
    if !job.lifecycle_eligible() {
        if let Some(artifact) = inventory
            .for_job(job.id.0)
            .map_err(|error| format!("read legacy PP artifact: {error}"))?
        {
            if artifact.state == "completed" && artifact.path.is_dir() {
                return Ok(Some(SealSource::Legacy(artifact.path)));
            }
        }
        if expected_source.is_dir() {
            return Ok(Some(SealSource::Legacy(expected_source)));
        }
        if matches!(job.status, JobStatus::Failed | JobStatus::Deleted) {
            return Ok(None);
        }
        return Err(format!(
            "legacy PP output directory {} is missing",
            expected_source.display()
        ));
    }

    if matches!(job.status, JobStatus::Failed | JobStatus::Deleted) {
        return Ok(None);
    }
    Err(format!("completed PP lease has no selected publication for private attempt {}; exact source {:?}; current {:?}",expected_source.display(),source,
        inventory.for_job(job.id.0).ok().flatten().map(|artifact|(artifact.id,artifact.generation,artifact.path,artifact.state))))
}

async fn seal_generation(
    cfg: &ClusterConfig,
    dest_dir: &std::path::Path,
    job: &nzbd_types::Job,
    lease: &LeaseState,
    inventory: std::sync::Arc<nzbd_state::artifacts::Inventory>,
) -> Result<SealedGeneration, String> {
    let cfg = cfg.clone();
    let dest_dir = dest_dir.to_path_buf();
    let job = job.clone();
    let lease = lease.clone();
    tokio::task::spawn_blocking(move || {
        use sha2::{Digest, Sha256};
        let resource_key = format!("{:x}", Sha256::digest(lease.token.resource.as_bytes()));
        let root = cfg
            .shared_dir
            .join(".nzbd-cluster/generations")
            .join(format!("job-{}", job.id.0))
            .join(format!("work-{}", &resource_key[..16]));
        let final_dir = root.join(format!("fence-{}", lease.token.fence));
        let manifest_path = final_dir.join("manifest.json");
        if manifest_path.exists() {
            let manifest: GenerationManifest =
                serde_json::from_slice(&read_bounded_file(&manifest_path, MAX_DESCRIPTOR_BYTES)?)
                    .map_err(|error| format!("decode sealed manifest: {error}"))?;
            return Ok(SealedGeneration {
                result_id: manifest.result_id,
                path: final_dir,
            });
        }

        std::fs::create_dir_all(&root)
            .map_err(|error| format!("create generation root: {error}"))?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let building = root.join(format!(
            ".building-{}-{}-{nonce}",
            cfg.node_name,
            std::process::id()
        ));
        std::fs::create_dir(&building)
            .map_err(|error| format!("create private generation: {error}"))?;
        let _building_cleanup = BuildingGeneration(building.clone());
        let files_root = building.join("files");
        std::fs::create_dir(&files_root)
            .map_err(|error| format!("create private generation files: {error}"))?;

        let source = if lease.kind == LeaseKind::Post {
            resolve_post_seal_source(&inventory, &dest_dir, &job)?
        } else {
            Some(SealSource::Legacy(
                dest_dir.join(nzbd_engine::queue::job_dir_name(&job)),
            ))
        };
        let copy_limit = job
            .totals
            .size
            .saturating_mul(2)
            .saturating_add(64 * 1024 * 1024);
        let mut files = Vec::new();
        let mut total = 0u64;
        if let Some(SealSource::Selected(publication)) = source.as_ref() {
            copy_generation_tree(
                &publication.path,
                &files_root,
                std::path::Path::new(""),
                copy_limit,
                &mut total,
                &mut files,
            )?;
            let expected: Vec<_> = publication
                .files
                .iter()
                .map(|file| GenerationFile {
                    path: file.path.clone(),
                    bytes: file.size_bytes,
                    sha256: file.sha256.clone(),
                })
                .collect();
            files.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
            if files != expected {
                return Err(
                    "selected PP publication changed after its durable journal committed".into(),
                );
            }
        } else if let Some(SealSource::Legacy(source)) = source.as_ref() {
            if !source.is_dir() {
                return Err(format!(
                    "completed output directory {} is missing",
                    source.display()
                ));
            }
            copy_generation_tree(
                source,
                &files_root,
                std::path::Path::new(""),
                copy_limit,
                &mut total,
                &mut files,
            )?;
        } else if lease.kind != LeaseKind::Post
            && !matches!(job.status, JobStatus::Failed | JobStatus::Deleted)
        {
            unreachable!("non-post seal source is always a legacy directory");
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));
        let job_bytes = serde_json::to_vec_pretty(&job)
            .map_err(|error| format!("encode generation job: {error}"))?;
        let job_sha256 = format!("{:x}", Sha256::digest(&job_bytes));
        let exact = job
            .params
            .iter()
            .find(|(key, _)| key == "Artifact:Id")
            .zip(
                job.params
                    .iter()
                    .find(|(key, _)| key == "Artifact:Generation"),
            )
            .map(
                |((_, artifact), (_, generation))| nzbd_state::artifacts::SourceGeneration {
                    artifact: artifact.clone(),
                    generation: generation.clone(),
                },
            );
        let selected_copy = matches!(source.as_ref(), Some(SealSource::Selected(_)));
        let artifact_copy = match exact.filter(|_| lease.kind == LeaseKind::Post) {
            Some(source) => {
                inventory.cluster_artifact_copy_for_generation(job.id.0, selected_copy, &source)
            }
            None => inventory.cluster_artifact_copy(job.id.0, selected_copy),
        }
        .map_err(|error| format!("capture cluster copy custody: {error}"))?;
        let file_outcomes = match &artifact_copy {
            Some(copy) if copy.selected => inventory
                .delivery_file_outcomes(&copy.publication.artifact, &copy.publication.generation)
                .map_err(|error| format!("capture cluster file outcomes: {error}"))?,
            _ => Vec::new(),
        };
        let directory_identity = Some(generation_directory_identity(&building)?);
        let mut manifest = GenerationManifest {
            identity_version: 3,
            source_disposition: inventory
                .for_job(job.id.0)
                .map_err(|error| error.to_string())?
                .and_then(|artifact| {
                    artifact
                        .failure_assessment
                        .map(|assessment| assessment.disposition)
                }),
            artifact_copy,
            directory_identity,
            file_outcomes,
            job_id: job.id.0,
            job_incarnation: lease.job_incarnation,
            owner_node_id: lease.token.owner_node_id,
            fence: lease.token.fence,
            result_id: String::new(),
            job_sha256,
            files,
            total_bytes: total,
            sealed_at_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .try_into()
                .unwrap_or(i64::MAX),
        };
        manifest.result_id = manifest.computed_identity()?;
        let result_id = manifest.result_id.clone();
        let job_tmp = building.join("job.json.tmp");
        std::fs::write(&job_tmp, &job_bytes)
            .map_err(|error| format!("write generation job: {error}"))?;
        std::fs::File::open(&job_tmp)
            .and_then(|file| file.sync_all())
            .map_err(|error| format!("flush generation job: {error}"))?;
        std::fs::rename(&job_tmp, building.join("job.json"))
            .map_err(|error| format!("seal generation job: {error}"))?;
        let manifest_tmp = building.join("manifest.json.tmp");
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|error| format!("encode generation manifest: {error}"))?;
        std::fs::write(&manifest_tmp, manifest_bytes)
            .map_err(|error| format!("write generation manifest: {error}"))?;
        std::fs::File::open(&manifest_tmp)
            .and_then(|file| file.sync_all())
            .map_err(|error| format!("flush generation manifest: {error}"))?;
        std::fs::rename(&manifest_tmp, building.join("manifest.json"))
            .map_err(|error| format!("seal generation manifest: {error}"))?;
        std::fs::File::open(&building)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("flush sealed generation directory: {error}"))?;
        match std::fs::rename(&building, &final_dir) {
            Ok(()) => {}
            Err(error) if final_dir.join("manifest.json").exists() => {
                std::fs::remove_dir_all(&building).map_err(|cleanup| {
                    format!("generation race cleanup failed after {error}: {cleanup}")
                })?;
            }
            Err(error) => return Err(format!("publish sealed generation directory: {error}")),
        }
        std::fs::File::open(&root)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("flush generation parent: {error}"))?;
        Ok(SealedGeneration {
            result_id,
            path: final_dir,
        })
    })
    .await
    .map_err(|error| format!("generation sealing task failed: {error}"))?
}

fn copy_generation_tree(
    source: &std::path::Path,
    target: &std::path::Path,
    relative: &std::path::Path,
    limit: u64,
    total: &mut u64,
    files: &mut Vec<GenerationFile>,
) -> Result<(), String> {
    for entry in std::fs::read_dir(source)
        .map_err(|error| format!("read generation source {}: {error}", source.display()))?
    {
        let entry = entry.map_err(|error| format!("read generation entry: {error}"))?;
        let name = entry.file_name();
        if name.to_string_lossy().starts_with(".pp.") {
            continue;
        }
        let from = entry.path();
        let rel = relative.join(&name);
        let to = target.join(&name);
        let metadata = std::fs::symlink_metadata(&from)
            .map_err(|error| format!("stat generation input {}: {error}", from.display()))?;
        if metadata.file_type().is_symlink() {
            return Err(format!("generation input {} is a symlink", from.display()));
        }
        if metadata.is_dir() {
            std::fs::create_dir(&to).map_err(|error| {
                format!("create generation directory {}: {error}", to.display())
            })?;
            copy_generation_tree(&from, &to, &rel, limit, total, files)?;
            continue;
        }
        if !metadata.is_file() {
            continue;
        }
        *total = total
            .checked_add(metadata.len())
            .ok_or_else(|| "generation byte accounting overflowed".to_owned())?;
        if *total > limit {
            return Err(format!(
                "generation copy exceeds bounded allowance of {limit} bytes"
            ));
        }
        std::fs::copy(&from, &to)
            .map_err(|error| format!("copy generation input {}: {error}", from.display()))?;
        use sha2::{Digest, Sha256};
        use std::io::Read;
        let mut input =
            std::fs::File::open(&to).map_err(|error| format!("open generation file: {error}"))?;
        let mut digest = Sha256::new();
        let mut buffer = [0u8; 65536];
        loop {
            let count = input.read(&mut buffer).map_err(|error| error.to_string())?;
            if count == 0 {
                break;
            }
            digest.update(&buffer[..count]);
        }
        files.push(GenerationFile {
            path: rel.to_string_lossy().into_owned(),
            bytes: metadata.len(),
            sha256: format!("{:x}", digest.finalize()),
        });
        std::fs::File::open(&to)
            .and_then(|file| file.sync_all())
            .map_err(|error| format!("flush generation file {}: {error}", to.display()))?;
    }
    std::fs::File::open(target)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("flush generation directory {}: {error}", target.display()))?;
    Ok(())
}

pub(crate) fn read_bounded_file(path: &std::path::Path, limit: usize) -> Result<Vec<u8>, String> {
    use std::io::Read;
    let file = std::fs::File::open(path).map_err(|error| error.to_string())?;
    if file.metadata().map_err(|error| error.to_string())?.len() > limit as u64 {
        return Err(
            "cluster descriptor file exceeds supported bound; source retained for review".into(),
        );
    }
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() > limit {
        return Err(
            "cluster descriptor grew past supported bound; source retained for review".into(),
        );
    }
    Ok(bytes)
}

pub(crate) fn generation_directory_identity(path: &std::path::Path) -> Result<(u64, u64), String> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err("generation directory changed type".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok((metadata.dev(), metadata.ino()))
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        Err("generation cleanup identity unsupported on this platform".into())
    }
}

fn conservative_deadline(cfg: &ClusterConfig) -> Instant {
    Instant::now()
        + cfg.worker_ttl.saturating_sub(
            cfg.lease_interval
                .max(std::time::Duration::from_millis(100)),
        )
}

async fn expire_local_leases(
    engine: &EngineHandle,
    servers: &[ServerDef],
    active: &ActiveLeases,
    budget_receipt: &BudgetReceipt,
) {
    let expired: Vec<_> = {
        let now = Instant::now();
        let mut leases = active.lock().unwrap();
        let ids: Vec<_> = leases
            .iter()
            .filter(|(_, state)| state.deadline <= now)
            .map(|(id, _)| id.clone())
            .collect();
        ids.into_iter()
            .filter_map(|id| leases.remove(&id).map(|state| (id, state)))
            .collect()
    };
    let had_expired = !expired.is_empty();
    for (lease_id, state) in expired {
        state.cancel.cancel();
        tracing::warn!(job = state.job.0, %lease_id, "local lease deadline expired; cancelling work");
        let _ = engine.remove_job_silent(state.job).await;
    }
    if had_expired && active.lock().unwrap().is_empty() {
        let generation = budget_receipt
            .lock()
            .unwrap()
            .as_ref()
            .map(|receipt| receipt.cluster_generation)
            .unwrap_or_default();
        apply_budgets(engine, servers, &HashMap::new(), generation, budget_receipt).await;
    }
}

async fn apply_budgets(
    engine: &EngineHandle,
    servers: &[ServerDef],
    by_name: &HashMap<String, u16>,
    cluster_generation: u64,
    budget_receipt: &BudgetReceipt,
) {
    // Missing is zero, not "keep whatever the previous leader granted".
    // Old leaders send an empty map on PP grants; failing closed is what
    // keeps a rolling-upgrade PP worker inside the account connection cap.
    let by_id = budgets_by_id(servers, by_name);
    if !by_id.is_empty() {
        match engine.set_server_budgets(by_id).await {
            Ok(receipt) => {
                *budget_receipt.lock().unwrap() = Some(BudgetAck {
                    cluster_generation,
                    engine_generation: receipt.generation,
                    drained: receipt.drained,
                });
            }
            Err(error) => tracing::warn!(%error, "connection budget application failed"),
        }
    }
}

fn budgets_by_id(
    servers: &[ServerDef],
    by_name: &HashMap<String, u16>,
) -> HashMap<nzbd_types::ServerId, u16> {
    servers
        .iter()
        .map(|s| (s.id, by_name.get(&s.name).copied().unwrap_or(0)))
        .collect()
}

fn pp_budget_must_park(post_fetch_budgeted: bool, has_download: bool) -> bool {
    !post_fetch_budgeted && !has_download
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::routing::post;
    use axum::{Json, Router};
    use nzbd_engine::{Engine, EngineConfig, Tuning};
    use nzbd_types::{CertLevel, DupeInfo, Job, JobKind, JobTotals, PostStage, ServerId, TlsMode};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn test_token() -> crate::control::LeaseToken {
        crate::control::LeaseToken {
            resource: "transition-lease".into(),
            owner_node_id: "worker".into(),
            owner_incarnation: "test-incarnation".into(),
            fence: 1,
            revision: 1,
            expires_at_unix_ms: i64::MAX,
        }
    }

    #[derive(Clone)]
    struct DelayedLeader {
        poll_seen: Arc<tokio::sync::Semaphore>,
        release_response: Arc<tokio::sync::Semaphore>,
        rejected: Arc<AtomicUsize>,
        job: Job,
    }

    async fn delayed_poll(State(state): State<DelayedLeader>) -> Json<PollResponse> {
        state.poll_seen.add_permits(1);
        let _ = state.release_response.acquire().await;
        Json(PollResponse {
            grants: vec![Grant {
                lifecycle_protocol: crate::proto::LIFECYCLE_PROTOCOL,
                lease_id: "transition-lease".into(),
                token: test_token(),
                job_incarnation: "test-job".into(),
                job_revision: 1,
                control_revision: 1,
                scope: serde_json::json!({"whole_job": true}),
                epoch: 1,
                kind: LeaseKind::Download,
                job: state.job,
                server_budgets: HashMap::new(),
                budget_generation: 0,
                post_fetch_budgeted: false,
            }],
        })
    }

    async fn record_reject(
        State(state): State<DelayedLeader>,
        Json(req): Json<RejectRequest>,
    ) -> Json<RejectResponse> {
        assert_eq!(req.node, "worker");
        assert_eq!(req.lease_id, "transition-lease");
        state.rejected.fetch_add(1, Ordering::SeqCst);
        Json(RejectResponse { released: true })
    }

    fn queued_test_job() -> Job {
        Job {
            id: JobId(77),
            kind: JobKind::Nzb,
            name: "transition".into(),
            dir_name: "transition".into(),
            name_provisional: false,
            delivery_lifecycle: false,
            queued_at_unix: 0,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: DupeInfo::default(),
            params: Vec::new(),
            files: Vec::new(),
            totals: JobTotals::default(),
            status: JobStatus::Queued,
            torrent: None,
            stages: Vec::new(),
        }
    }

    #[test]
    fn failed_generation_build_releases_private_staging_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let building = tmp.path().join(".building-failed-copy");
        std::fs::create_dir(&building).unwrap();
        std::fs::write(building.join("partial.mkv"), vec![0x5a; 4096]).unwrap();
        {
            let _cleanup = BuildingGeneration(building.clone());
            assert!(building.join("partial.mkv").exists());
            // Returning through any error path drops the same guard.
        }
        assert!(!building.exists());
    }

    #[test]
    fn post_sealer_uses_current_selected_generation_and_rejects_missing_target() {
        use nzbd_state::artifacts::DeliveryFileOutcome;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("downloads");
        std::fs::create_dir(&root).unwrap();
        let inventory = nzbd_state::artifacts::Inventory::open(&tmp.path().join("state")).unwrap();
        let source = root.join("private-attempt");
        inventory
            .allocate_for_delivery(77, &root, &source, true, true)
            .unwrap();
        std::fs::write(source.join("episode.mkv"), b"verified payload").unwrap();
        inventory.finish(77, &source, &root, "active").unwrap();
        let selected = inventory
            .for_job(77)
            .unwrap()
            .unwrap()
            .files
            .into_iter()
            .filter(|entry| !entry.identity.directory)
            .collect();
        let completed = root.join("completed");
        std::fs::create_dir(&completed).unwrap();
        let target = completed.join("episode.runner-generation");
        let result = inventory
            .publish_selected_for_delivery_checked(
                77,
                &target,
                selected,
                vec![DeliveryFileOutcome {
                    source_path: "episode.mkv".into(),
                    publication_path: Some("episode.mkv".into()),
                    classification: "media".into(),
                    outcome: "published".into(),
                    reason: Some("verified complete media".into()),
                    repair_dependency: false,
                    source_cleanup_status: "pending".into(),
                }],
                &|| Ok(()),
            )
            .unwrap();
        inventory
            .finish(77, &result.published_path, &completed, "completed")
            .unwrap();

        let mut job = queued_test_job();
        job.delivery_lifecycle = true;
        job.dir_name = "private-attempt".into();
        let resolved = resolve_post_seal_source(&inventory, &root, &job).unwrap();
        let Some(SealSource::Selected(snapshot)) = resolved else {
            panic!("expected exact selected publication snapshot");
        };
        assert_eq!(snapshot.path, result.published_path);
        assert_eq!(snapshot.generation, result.generation);
        assert_eq!(snapshot.files.len(), 1);
        assert_eq!(snapshot.files[0].path, "episode.mkv");

        std::fs::remove_dir_all(&result.published_path).unwrap();
        let error = resolve_post_seal_source(&inventory, &root, &job).unwrap_err();
        assert!(error.contains("journaled selected PP publication is missing"));
    }

    #[test]
    fn post_sealer_keeps_legacy_whole_directory_handoff() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("downloads");
        std::fs::create_dir(&root).unwrap();
        let inventory = nzbd_state::artifacts::Inventory::open(&tmp.path().join("state")).unwrap();
        let legacy = root.join("legacy-release");
        std::fs::create_dir(&legacy).unwrap();
        std::fs::write(legacy.join("payload.mkv"), b"legacy bytes").unwrap();
        let mut job = queued_test_job();
        job.dir_name = "legacy-release".into();
        let resolved = resolve_post_seal_source(&inventory, &root, &job).unwrap();
        assert!(matches!(resolved, Some(SealSource::Legacy(path)) if path == legacy));
    }

    #[tokio::test]
    async fn heartbeat_progress_carries_the_remote_post_stage() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = Engine::spawn(EngineConfig::single_node(
            Vec::new(),
            tmp.path().join("state"),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        engine
            .import_job(queued_test_job(), false, false)
            .await
            .unwrap();
        assert!(engine
            .enter_post_stage(JobId(77), PostStage::ParVerify, 1_000, None)
            .await
            .unwrap());

        engine
            .register_repair_attempt(JobId(77), "transition-lease:1".into())
            .await
            .unwrap();
        let repair = nzbd_engine::RepairProgress {
            attempt_id: "transition-lease:1".into(),
            phase: nzbd_engine::RepairPhase::Matching,
            files_done: 1,
            files_total: 2,
            bytes_scanned: 65536,
            round: 0,
            recovery_blocks_available: 3,
            additional_blocks_needed: Some(1),
            last_progress_at: 1000,
        };
        engine.update_repair_progress(JobId(77), repair).await;
        engine.export_job(JobId(77)).await.unwrap();
        let progress = progress_of(&engine, JobId(77));
        assert_eq!(
            progress.repair_progress.as_ref().unwrap().bytes_scanned,
            65536
        );
        let old: MirrorStats = serde_json::from_value(serde_json::json!({"done_articles":0,"failed_articles":0,"downloaded_bytes":0,"health":1000})).unwrap();
        assert!(old.repair_progress.is_none());
        assert_eq!(progress.remaining_bytes, Some(0));
        assert_eq!(progress.stages.len(), 1);
        assert_eq!(progress.stages[0].stage, PostStage::ParVerify);
        assert!(progress.stages[0].ms.is_none());

        engine.shutdown().await;
    }

    #[tokio::test]
    async fn suspended_pp_lease_past_deadline_revokes_its_subprocess_owner() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = Engine::spawn(EngineConfig::single_node(
            Vec::new(),
            tmp.path().join("state"),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let cancel = CancellationToken::new();
        let active: ActiveLeases = Default::default();
        active.lock().unwrap().insert(
            "expired".into(),
            LeaseState {
                job: JobId(77),
                kind: LeaseKind::Post,
                token: test_token(),
                job_incarnation: "job-incarnation".into(),
                job_revision: 1,
                control_revision: 1,
                deadline: Instant::now() - std::time::Duration::from_millis(1),
                cancel: cancel.clone(),
                pp_ready: false,
                ready_job: None,
            },
        );

        expire_local_leases(&engine, &[], &active, &Arc::new(Mutex::new(None))).await;

        assert!(active.lock().unwrap().is_empty());
        assert!(cancel.is_cancelled());
        engine.shutdown().await;
    }

    #[test]
    fn legacy_pp_budget_messages_fail_closed() {
        let legacy: HeartbeatResponse = serde_json::from_value(serde_json::json!({
            "cancel": [],
            "server_budgets": {"provider": 8}
        }))
        .unwrap();
        assert!(!legacy.post_fetch_budgeted);

        let provider = ServerDef {
            id: ServerId(1),
            name: "provider".into(),
            host: "127.0.0.1".into(),
            port: 119,
            tls: TlsMode::None,
            username: None,
            password: None,
            active: true,
            tier: 0,
            group: 0,
            fill: false,
            max_connections: 8,
            pipeline_depth: 1,
            retention_days: 0,
            cert_verification: CertLevel::Strict,
        };
        assert_eq!(budgets_by_id(&[provider], &HashMap::new())[&ServerId(1)], 0);
        assert!(pp_budget_must_park(false, false));
        assert!(
            !pp_budget_must_park(false, true),
            "an old leader already budgets a node that holds a Download lease"
        );
        assert!(!pp_budget_must_park(true, false));
    }

    #[tokio::test]
    async fn healthy_poll_then_disk_hold_rejects_grant_without_importing_it() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = Engine::spawn(EngineConfig::single_node(
            Vec::new(),
            tmp.path().join("state"),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        assert!(!engine.snapshot().disk_low);

        let state = DelayedLeader {
            poll_seen: Arc::new(tokio::sync::Semaphore::new(0)),
            release_response: Arc::new(tokio::sync::Semaphore::new(0)),
            rejected: Arc::new(AtomicUsize::new(0)),
            job: queued_test_job(),
        };
        let app = Router::new()
            .route("/cluster/v1/work/poll", post(delayed_poll))
            .route("/cluster/v1/work/reject", post(record_reject))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let leader_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let cfg = ClusterConfig {
            cluster_id: "test".into(),
            node_name: "worker".into(),
            shared_dir: tmp.path().to_path_buf(),
            advertise_url: "http://worker.invalid".into(),
            secret: "secret".into(),
            coordinator: false,
            priority: 0,
            download: true,
            max_download_jobs: 1,
            post_process: false,
            pp_slots: 0,
            lease_interval: std::time::Duration::from_secs(1),
            takeover_after: std::time::Duration::from_secs(2),
            worker_ttl: std::time::Duration::from_secs(3),
            control_dir: tmp.path().join("control"),
            control_node_id: 1,
            control_raft_bind: "127.0.0.1:38112".into(),
            control_api_bind: "127.0.0.1:38212".into(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: Vec::new(),
            torrent_payload_roots: Vec::new(),
        };
        let client = ClusterClient::new("secret".into());
        let active: ActiveLeases = Default::default();
        let tracker = TaskTracker::new();
        let dest = tmp.path().join("dest");
        let budget_receipt: BudgetReceipt = Default::default();

        let poll = poll_for_work(
            &cfg,
            &[],
            &engine,
            &client,
            &active,
            &None,
            &dest,
            &tracker,
            &leader_url,
            "test-incarnation",
            &budget_receipt,
        );
        let transition = async {
            let _ = state.poll_seen.acquire().await.unwrap();
            engine.report_out_of_space("injected between poll and response");
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            while !engine.snapshot().disk_low {
                assert!(tokio::time::Instant::now() < deadline);
                tokio::task::yield_now().await;
            }
            state.release_response.add_permits(1);
        };
        tokio::join!(poll, transition);

        assert_eq!(state.rejected.load(Ordering::SeqCst), 1);
        assert!(active.lock().unwrap().is_empty());
        assert!(engine.export_job(JobId(77)).await.unwrap().is_none());

        tracker.close();
        tracker.wait().await;
        server.abort();
        engine.shutdown().await;
    }
    #[tokio::test]
    async fn retirement_requires_authority_acceptance_and_replays_durably() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dest");
        std::fs::create_dir(&root).unwrap();
        let engine = Engine::spawn(EngineConfig::single_node(
            vec![],
            temp.path().join("state"),
            root.clone(),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let source = engine
            .artifacts()
            .allocate(77, &root, &root.join("transition"))
            .unwrap();
        std::fs::write(source.path.join("payload"), b"owned").unwrap();
        let workspace = engine
            .artifacts()
            .workspace(77, "par_repair", "abc")
            .unwrap();
        std::fs::write(workspace.scratch.path.join("temporary"), b"scratch").unwrap();
        engine.artifacts().finish_workspace(&workspace).unwrap();
        let mut job = queued_test_job();
        job.status = JobStatus::Completed;
        job.params.extend([
            ("Artifact:Id".into(), source.id.clone()),
            ("Artifact:Generation".into(), source.generation.clone()),
            (nzbd_types::PP_DONE_PARAM.into(), "SUCCESS".into()),
        ]);
        let request = CompleteRequest {
            lifecycle_protocol: crate::proto::LIFECYCLE_PROTOCOL,
            node: "worker".into(),
            lease_id: "transition-lease".into(),
            token: test_token(),
            expected_job_revision: 1,
            result_id: "result".into(),
            result_ref: "/private/result".into(),
            receipt_id: "accepted-receipt".into(),
            job,
        };
        let mut response = CompleteResponse {
            final_dir: None,
            ok: false,
            durable_receipt: None,
            accepted_at_unix_ms: Some(10),
            history_recorded_by_authority: true,
            cleanup_only: false,
        };
        assert!(accept_worker_retirement(&engine, &request, &response)
            .await
            .is_err());
        assert!(workspace.scratch.path.exists());
        response.ok = true;
        accept_worker_retirement(&engine, &request, &response)
            .await
            .unwrap();
        assert!(!workspace.scratch.path.exists());
        assert!(source.path.join("payload").exists());
        accept_worker_retirement(&engine, &request, &response)
            .await
            .unwrap();
        engine.shutdown().await;
    }
}
