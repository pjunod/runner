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
    let t2 = tracker.clone();
    tracker.spawn(worker_task(
        cfg,
        servers,
        engine,
        view,
        client,
        active,
        pp,
        dest_dir,
        owner_incarnation,
        budget_receipt,
        cancel,
        t2,
    ));
}

#[allow(clippy::too_many_arguments)]
async fn worker_task(
    cfg: ClusterConfig,
    servers: Vec<ServerDef>,
    engine: EngineHandle,
    view: watch::Receiver<LeaderView>,
    client: ClusterClient,
    active: ActiveLeases,
    pp: Option<PpSetup>,
    dest_dir: PathBuf,
    owner_incarnation: String,
    budget_receipt: BudgetReceipt,
    cancel: CancellationToken,
    tracker: TaskTracker,
) {
    loop {
        if cancel.is_cancelled() {
            break;
        }
        let v = view.borrow().clone();

        expire_local_leases(&engine, &servers, &active, &budget_receipt).await;

        if v.is_me {
            // We are the leader: granted leases dissolve into local jobs
            // (adopt_authority kept them); the scheduler takes over.
            for state in active.lock().unwrap().values() {
                state.cancel.cancel();
            }
            active.lock().unwrap().clear();
        } else if let Some(url) = v.leader_url().map(|s| s.to_string()) {
            heartbeat_and_cancel(
                &cfg,
                &servers,
                &engine,
                &client,
                &active,
                &budget_receipt,
                &url,
            )
            .await;
            report_completions(&cfg, &engine, &client, &active, &pp, &url, &dest_dir).await;
            poll_for_work(
                &cfg,
                &servers,
                &engine,
                &client,
                &active,
                &pp,
                &dest_dir,
                &tracker,
                &url,
                &owner_incarnation,
                &budget_receipt,
            )
            .await;
        }

        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(cfg.lease_interval) => {}
        }
    }
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
    tokio::task::spawn_blocking(move || {
        inventory.accept_cluster_completion(&source, &outcome, proof)
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
        let sealed = match seal_generation(cfg, dest_dir, &job, &lease).await {
            Ok(sealed) => sealed,
            Err(error) => {
                tracing::warn!(job = job_id.0, %error, "completion generation could not be sealed");
                continue;
            }
        };
        let receipt_id = format!(
            "complete:{}:{}:{}",
            lease.token.resource, lease.token.fence, sealed.result_id
        );
        let req = CompleteRequest {
            node: cfg.node_name.clone(),
            lease_id: lease_id.clone(),
            token: lease.token.clone(),
            expected_job_revision: lease.job_revision,
            result_id: sealed.result_id,
            result_ref: sealed.path.to_string_lossy().into_owned(),
            receipt_id,
            job,
        };
        match client
            .post_json::<_, CompleteResponse>(leader_url, "/cluster/v1/work/complete", &req)
            .await
        {
            Ok(resp) if resp.ok => {
                if lease.kind == LeaseKind::Post && !resp.history_recorded_by_authority {
                    // Older authorities omit the concrete path. Their selected
                    // immutable generation stores payloads under files/.
                    let final_dir = resp.final_dir.clone().unwrap_or_else(|| {
                        std::path::Path::new(&req.result_ref)
                            .join("files")
                            .to_string_lossy()
                            .into_owned()
                    });
                    let history_result = match (pp, resp.accepted_at_unix_ms) {
                        (Some(setup), Some(accepted_at_ms)) => {
                            record_published_pp_history(
                                setup.history.clone(),
                                &req.job,
                                &final_dir,
                                accepted_at_ms,
                            )
                            .await
                        }
                        _ => Err("durable PP history target or timestamp unavailable".into()),
                    };
                    if let Err(error) = history_result {
                        tracing::warn!(job = job_id.0, %error, "PP completion awaits durable history");
                        continue;
                    }
                }
                if lease.kind == LeaseKind::Post {
                    if let Err(error) = accept_worker_retirement(engine, &req, &resp).await {
                        tracing::warn!(job=job_id.0,%error,"remote workspace retirement awaits durable acceptance recording");
                        continue;
                    }
                }
                tracing::info!(job = job_id.0, %lease_id, "completion handed to leader");
                active.lock().unwrap().remove(&lease_id);
                let _ = engine.remove_job_silent(job_id).await;
            }
            Ok(_) | Err(_) => {
                // Leader unreachable or refused: retry next tick; a
                // reclaimed lease resolves via the journals either way.
            }
        }
    }
}

async fn record_published_pp_history(
    history: Arc<nzbd_state::history::HistoryDb>,
    job: &nzbd_types::Job,
    result_ref: &str,
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
        final_dir: Some(result_ref.to_owned()),
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
                // Fold shared journals on import: resume work another node did.
                if engine.import_job(grant.job, true, false).await.is_ok() {
                    if engine.snapshot().disk_low {
                        let _ = engine.remove_job_silent(job_id).await;
                        reject_remote_grant(cfg, client, leader_url, &grant.lease_id, &grant.token)
                            .await;
                        continue;
                    }
                    active.lock().unwrap().insert(
                        grant.lease_id,
                        LeaseState {
                            job: job_id,
                            kind: LeaseKind::Download,
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
                            pp_ready: false,
                            ready_job: None,
                        },
                    );
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
        let pp_cancel = initial.cancel.clone();
        let result = tokio::select! {
            _ = pp_cancel.cancelled() => Err(nzbd_post::PostError::Subprocess("PP lease authority expired".into())),
            result = process_job_ctx(&engine, &setup.post, &setup.history, &dest_dir, job_id, &ctx) => result,
        };
        ctx.cancel.cancel(); ctx.workers.close(); ctx.workers.wait().await;
        match result {
            Ok(outcome) => {
                tracing::info!(job = job_id.0, lease = %lease_id, outcome = outcome.as_str(), "pp lease finished");
                if let Some(st) = active.lock().unwrap().get_mut(&lease_id) {
                    st.pp_ready = true;
                }
            }
            Err(e) => {
                tracing::warn!(job = job_id.0, lease = %lease_id, error = %e, "pp lease aborted");
                // Drop the local copy; the leader reclaims and reschedules.
                if active.lock().unwrap().remove(&lease_id).is_some() {
                    let _ = engine.remove_job_silent(job_id).await;
                }
            }
        }
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
    let limit = job
        .totals
        .size
        .saturating_mul(2)
        .saturating_add(64 * 1024 * 1024);
    let source_for_copy = source.clone();
    let target_for_copy = target.clone();
    let job_id = job.id.0;
    tokio::task::spawn_blocking(move || {
        let parent = target_for_copy
            .parent()
            .ok_or_else(|| "private PP target has no parent".to_owned())?;
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("create private PP parent: {error}"))?;
        let artifact = inventory
            .allocate_processing_successor(job_id, parent, &target_for_copy)
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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct GenerationManifest {
    job_id: u32,
    job_incarnation: String,
    owner_node_id: String,
    fence: u64,
    result_id: String,
    job_sha256: String,
    files: Vec<GenerationFile>,
    total_bytes: u64,
    sealed_at_unix_ms: i64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct GenerationFile {
    path: String,
    bytes: u64,
    sha256: String,
}

struct SealedGeneration {
    result_id: String,
    path: PathBuf,
}

async fn seal_generation(
    cfg: &ClusterConfig,
    dest_dir: &std::path::Path,
    job: &nzbd_types::Job,
    lease: &LeaseState,
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
            let manifest: GenerationManifest = serde_json::from_slice(
                &std::fs::read(&manifest_path)
                    .map_err(|error| format!("read sealed manifest: {error}"))?,
            )
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
        let files_root = building.join("files");
        std::fs::create_dir(&files_root)
            .map_err(|error| format!("create private generation files: {error}"))?;

        let source = dest_dir.join(nzbd_engine::queue::job_dir_name(&job));
        let copy_limit = job
            .totals
            .size
            .saturating_mul(2)
            .saturating_add(64 * 1024 * 1024);
        let mut files = Vec::new();
        let mut total = 0u64;
        if source.exists() {
            copy_generation_tree(
                &source,
                &files_root,
                std::path::Path::new(""),
                copy_limit,
                &mut total,
                &mut files,
            )?;
        } else if !matches!(job.status, JobStatus::Failed | JobStatus::Deleted) {
            return Err(format!(
                "completed output directory {} is missing",
                source.display()
            ));
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));
        let job_bytes = serde_json::to_vec_pretty(&job)
            .map_err(|error| format!("encode generation job: {error}"))?;
        let job_sha256 = format!("{:x}", Sha256::digest(&job_bytes));
        let identity = serde_json::to_vec(&(
            job.id.0,
            &lease.job_incarnation,
            lease.token.fence,
            &job_sha256,
            &files,
        ))
        .map_err(|error| format!("encode generation identity: {error}"))?;
        let result_id = format!("sha256:{:x}", Sha256::digest(identity));
        let manifest = GenerationManifest {
            job_id: job.id.0,
            job_incarnation: lease.job_incarnation,
            owner_node_id: lease.token.owner_node_id,
            fence: lease.token.fence,
            result_id: result_id.clone(),
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
        let bytes = std::fs::read(&to)
            .map_err(|error| format!("hash generation file {}: {error}", to.display()))?;
        use sha2::{Digest, Sha256};
        files.push(GenerationFile {
            path: rel.to_string_lossy().into_owned(),
            bytes: metadata.len(),
            sha256: format!("{:x}", Sha256::digest(bytes)),
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
