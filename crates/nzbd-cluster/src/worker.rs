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
use std::path::{Path, PathBuf};
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
    /// Original imported identity; heartbeat revisions never rewrite it.
    pub context: nzbd_state::artifacts::WorkerControlContext,
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
#[derive(Clone, Default)]
pub struct ActiveLeases {
    states: Arc<Mutex<HashMap<String, LeaseState>>>,
    admissions: Arc<Mutex<HashMap<String, watch::Receiver<bool>>>>,
    operations: Arc<Mutex<HashMap<String, watch::Receiver<bool>>>>,
    retiring: Arc<Mutex<HashMap<String, LeaseKind>>>,
}
impl std::ops::Deref for ActiveLeases {
    type Target = Mutex<HashMap<String, LeaseState>>;
    fn deref(&self) -> &Self::Target {
        &self.states
    }
}
impl ActiveLeases {
    pub(crate) fn spawn_operation<F>(&self, tracker: &TaskTracker, id: String, work: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let (done, joined) = watch::channel(false);
        self.operations.lock().unwrap().insert(id, joined);
        tracker.spawn(async move {
            work.await;
            let _ = done.send(true);
        });
    }
    pub(crate) async fn join_execution(&self, id: &str) -> Result<bool, String> {
        let admission = self.admissions.lock().unwrap().get(id).cloned();
        let mut observed = admission.is_some();
        if let Some(mut joined) = admission {
            joined
                .wait_for(|done| *done)
                .await
                .map_err(|_| "admission stop acknowledgment lost; retain for review".to_owned())?;
        }
        // Admission may install the execution operation before acknowledging.
        let operation = self.operations.lock().unwrap().get(id).cloned();
        observed |= operation.is_some();
        if let Some(mut joined) = operation {
            joined
                .wait_for(|done| *done)
                .await
                .map_err(|_| "execution stop acknowledgment lost; retain for review".to_owned())?;
        }
        Ok(observed)
    }
    pub(crate) fn forget_joined(&self, id: &str) {
        self.admissions.lock().unwrap().remove(id);
        self.operations.lock().unwrap().remove(id);
    }
}
type BudgetReceipt = Arc<Mutex<Option<BudgetAck>>>;

/// A ready Post releases a scarce PP slot. Alternate attempts between that
/// lane and downloads, and rotate within each lane even after a failed RPC.
/// Durable outbox replay remains eligible after its live lease disappears.
struct CompletionOrder {
    // Separate turns prevent a repeatedly failed pending Post from forcing
    // every subsequent fresh-ready selection back onto downloads.
    prefer_post: [bool; 2],
    ready_after: [String; 2],
    pending_after: [String; 2],
    scan: OutboxScan,
}
impl Default for CompletionOrder {
    fn default() -> Self {
        Self {
            prefer_post: [true, true],
            ready_after: Default::default(),
            pending_after: Default::default(),
            scan: OutboxScan::default(),
        }
    }
}
impl CompletionOrder {
    fn rank<'a>(&self, kind: LeaseKind, key: &'a str, pending: bool) -> (bool, bool, &'a str) {
        let post = kind == LeaseKind::Post;
        let after = if pending {
            &self.pending_after
        } else {
            &self.ready_after
        };
        (
            post != self.prefer_post[usize::from(pending)],
            key <= after[usize::from(post)].as_str(),
            key,
        )
    }

    fn attempted(&mut self, kind: LeaseKind, key: &str, pending: bool) {
        let post = kind == LeaseKind::Post;
        self.prefer_post[usize::from(pending)] = !post;
        let after = if pending {
            &mut self.pending_after
        } else {
            &mut self.ready_after
        };
        after[usize::from(post)] = key.to_owned();
    }
}

#[derive(Clone)]
struct BudgetCommand {
    sequence: u64,
    generation: u64,
    budgets: HashMap<String, u16>,
}

/// One tracked application lane; heartbeat submits without waiting for drains.
#[derive(Clone)]
struct BudgetLane {
    commands: watch::Sender<Option<BudgetCommand>>,
    applied: watch::Receiver<(u64, bool)>,
    receipt: BudgetReceipt,
    cancel: CancellationToken,
}
impl BudgetLane {
    fn spawn(
        engine: EngineHandle,
        servers: Vec<ServerDef>,
        cancel: CancellationToken,
        tracker: &TaskTracker,
    ) -> Self {
        Self::spawn_with_apply(cancel, tracker, move |command| {
            let engine = engine.clone();
            let by_id = budgets_by_id(&servers, &command.budgets);
            async move {
                if by_id.is_empty() {
                    return Ok(None);
                }
                engine
                    .set_server_budgets(by_id)
                    .await
                    .map(|receipt| {
                        Some(BudgetAck {
                            cluster_generation: command.generation,
                            engine_generation: receipt.generation,
                            drained: receipt.drained,
                        })
                    })
                    .map_err(|error| error.to_string())
            }
        })
    }

    fn spawn_with_apply<F, Fut>(
        cancel: CancellationToken,
        tracker: &TaskTracker,
        mut apply: F,
    ) -> Self
    where
        F: FnMut(BudgetCommand) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Option<BudgetAck>, String>> + Send + 'static,
    {
        let (commands, mut pending) = watch::channel::<Option<BudgetCommand>>(None);
        let (complete, applied) = watch::channel((0, true));
        let receipt: BudgetReceipt = Default::default();
        let published = receipt.clone();
        let shutdown = cancel.clone();
        tracker.spawn(async move {
            loop {
                if cancel.is_cancelled() {
                    break;
                }
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    result = pending.changed() => if result.is_err() { break; },
                }
                let command = pending.borrow_and_update().clone();
                let Some(command) = command else {
                    continue;
                };
                let sequence = command.sequence;
                // Never abort a real drain: its acknowledgement and shutdown
                // ownership stay tied to the actual engine operation.
                let ok = match apply(command).await {
                    Ok(ack) => {
                        let drained = ack.as_ref().is_none_or(|ack| ack.drained);
                        if let Some(ack) = ack {
                            *published.lock().unwrap() = Some(ack);
                        }
                        drained
                    }
                    Err(error) => {
                        tracing::warn!(%error, "connection budget application failed");
                        false
                    }
                };
                complete.send_replace((sequence, ok));
            }
        });
        Self {
            commands,
            applied,
            receipt,
            cancel: shutdown,
        }
    }

    fn submit(&self, budgets: &HashMap<String, u16>, generation: u64) -> u64 {
        let mut sequence = 0;
        self.commands.send_if_modified(|pending| {
            if let Some(current) = pending {
                sequence = current.sequence;
                if generation < current.generation {
                    return false;
                }
                let (completed, ok) = *self.applied.borrow();
                if generation == current.generation
                    && budgets == &current.budgets
                    && (completed < current.sequence || ok)
                {
                    return false;
                }
            }
            sequence += 1;
            *pending = Some(BudgetCommand {
                sequence,
                generation,
                budgets: budgets.clone(),
            });
            true
        });
        sequence
    }

    async fn apply(&self, budgets: &HashMap<String, u16>, generation: u64) -> bool {
        if self.cancel.is_cancelled() {
            return false;
        }
        let sequence = self.submit(budgets, generation);
        let mut applied = self.applied.clone();
        applied
            .wait_for(|(completed, _)| *completed >= sequence)
            .await
            .is_ok_and(|result| result.1)
    }

    fn park(&self) {
        let generation = self
            .commands
            .borrow()
            .as_ref()
            .map(|command| command.generation)
            .unwrap_or(0);
        self.submit(&HashMap::new(), generation);
    }
}

fn held_slots(active: &ActiveLeases) -> (u32, u32) {
    let states = active.lock().unwrap();
    let retiring = active.retiring.lock().unwrap();
    states
        .values()
        .map(|state| state.kind)
        .chain(retiring.values().copied())
        .fold((0, 0), |(download, post), kind| match kind {
            LeaseKind::Post => (download, post + 1),
            _ => (download + 1, post),
        })
}

/// Transfer slots atomically before any caller can poll for more work.
fn revoke_leases(
    active: &ActiveLeases,
    mut revoke: impl FnMut(&str, &LeaseState) -> bool,
) -> Vec<(String, LeaseState)> {
    let mut states = active.lock().unwrap();
    let mut retiring = active.retiring.lock().unwrap();
    let ids: Vec<_> = states
        .iter()
        .filter(|(id, state)| revoke(id, state))
        .map(|(id, _)| id.clone())
        .collect();
    ids.into_iter()
        .filter_map(|id| {
            states.remove(&id).map(|state| {
                state.cancel.cancel();
                retiring.insert(id.clone(), state.kind);
                (id, state)
            })
        })
        .collect()
}

fn retire_lease(
    engine: &EngineHandle,
    active: &ActiveLeases,
    tracker: &TaskTracker,
    shutdown: &CancellationToken,
    lease_id: String,
    state: LeaseState,
) {
    state.cancel.cancel();
    let shutdown = shutdown.clone();
    let engine = engine.clone();
    let active = active.clone();
    tracker.spawn(async move {
        if let Err(error) = active.join_execution(&lease_id).await {
            tracing::error!(%lease_id, %error, "worker retirement retains unjoined custody");
            return;
        }
        if state.kind != LeaseKind::Assemble {
            let mut delay = std::time::Duration::from_millis(100);
            loop {
                match engine.retire_worker_job(state.job, state.context.clone()).await {
                    Ok(_) => break,
                    Err(error) => {
                        tracing::warn!(%lease_id, %error, "worker retirement awaits owner acknowledgement");
                        if matches!(error, nzbd_engine::EngineError::Closed) || shutdown.is_cancelled() { return; }
                        tokio::select! {
                            _ = shutdown.cancelled() => return,
                            _ = tokio::time::sleep(delay) => {},
                        }
                        delay = (delay * 2).min(std::time::Duration::from_secs(1));
                    }
                }
            }
        }
        // Replace the execution watches with an exact durable stopped receipt
        // before forgetting them. This also covers expiry before engine import,
        // when no actor writer observation exists yet. This does not compact
        // the shared article journal; only accepted completion may do that.
        let mut delay = std::time::Duration::from_millis(100);
        loop {
            match compact_joined_grant(&engine, &active, state.job, &state.context).await {
                Ok(()) => break,
                Err(error) => {
                    tracing::warn!(%lease_id, %error, "worker retirement awaits durable stopped receipt");
                    if shutdown.is_cancelled() { return; }
                    tokio::select! {
                        _ = shutdown.cancelled() => return,
                        _ = tokio::time::sleep(delay) => {},
                    }
                    delay = (delay * 2).min(std::time::Duration::from_secs(1));
                }
            }
        }
        active.forget_joined(&lease_id);
        active.retiring.lock().unwrap().remove(&lease_id);
    });
}

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
    let budgets = BudgetLane::spawn(engine.clone(), servers.clone(), cancel.clone(), tracker);
    {
        let cfg = cfg.clone();
        let engine = engine.clone();
        let client = client.clone();
        let active = active.clone();
        let pp = pp.clone();
        let dest_dir = dest_dir.clone();
        let retirement_tracker = tracker.clone();
        let view = view.clone();
        let cancel = cancel.clone();
        tracker.spawn(async move {
            let mut completion_order = CompletionOrder::default();
            loop {
                if cancel.is_cancelled(){break;}
                let current=view.borrow().clone();
                if !current.is_me {
                    if let Some(url)=current.leader_url().map(str::to_owned) {
                        replay_completion_outbox(&cfg,&engine,&client,&active,&pp,&url,&dest_dir,&mut completion_order).await;
                        report_completions(&cfg,&engine,&client,&active,&pp,&url,&dest_dir,&retirement_tracker,&cancel,&mut completion_order).await;
                    }
                }
                tokio::select!{ _=cancel.cancelled()=>break,_=tokio::time::sleep(cfg.lease_interval)=>{} }
            }
        });
    }
    {
        let cfg = cfg.clone();
        let engine = engine.clone();
        let view = view.clone();
        let client = client.clone();
        let active = active.clone();
        let budgets = budgets.clone();
        let retirement_tracker = tracker.clone();
        let cancel = cancel.clone();
        tracker.spawn(async move {
            loop {
                if cancel.is_cancelled(){break;}
                expire_local_leases(&engine,&active,&budgets,&retirement_tracker);
                let current=view.borrow().clone();
                if current.is_me {
                    let cancelled=revoke_leases(&active, |_,_| true);
                    for (id,state) in cancelled {retire_lease(&engine,&active,&retirement_tracker,&cancel,id,state);}
                }else if let Some(url)=current.leader_url().map(str::to_owned) {
                    heartbeat_and_cancel(&cfg,&engine,&client,&active,&budgets,&retirement_tracker,&url).await;
                }
                tokio::select!{_=cancel.cancelled()=>break,_=tokio::time::sleep(cfg.lease_interval)=>{}}
            }
            // Pending CRC preparation and held PP are also owned work. Joiners
            // can finish only after their exact authority is visibly revoked.
            let cancelled=revoke_leases(&active, |_,_| true);
            for (id,state) in cancelled {retire_lease(&engine,&active,&retirement_tracker,&cancel,id,state);}
        });
    }
    // Recovery probes can touch slow storage. They never occupy heartbeat or
    // completion lanes, and each release carries current in-memory grant proof.
    {
        let engine = engine.clone();
        let active = active.clone();
        let cancel = cancel.clone();
        let interval = cfg.lease_interval.max(std::time::Duration::from_secs(5));
        tracker.spawn(async move {
            let mut cursor = 0;
            let mut passes = 0u64;
            loop {
                if cancel.is_cancelled() {
                    break;
                }
                passes = passes.wrapping_add(1);
                recover_download_holds(&engine, &active, &mut cursor, passes.is_multiple_of(6))
                    .await;
                tokio::select! {_=cancel.cancelled()=>break,_=tokio::time::sleep(interval)=>{}}
            }
        });
    }
    let work_tracker = tracker.clone();
    tracker.spawn(async move {
        loop {
            if cancel.is_cancelled(){break;}
            let current=view.borrow().clone();
            if !current.is_me {
                if let Some(url)=current.leader_url().map(str::to_owned) {
                    poll_for_work(&cfg,&engine,&client,&active,&pp,&dest_dir,&work_tracker,&url,&owner_incarnation,&budgets).await;
                }
            }
            tokio::select!{_=cancel.cancelled()=>break,_=tokio::time::sleep(cfg.lease_interval)=>{}}
        }
    });
}

fn lease_authority(
    active: &ActiveLeases,
    lease_id: &str,
    expected: &LeaseState,
) -> Arc<dyn Fn() -> bool + Send + Sync> {
    let active = active.clone();
    let lease_id = lease_id.to_owned();
    let expected = expected.clone();
    Arc::new(move || {
        active.lock().unwrap().get(&lease_id).is_some_and(|state| {
            state.job == expected.job
                && state.kind == expected.kind
                && state.job_incarnation == expected.job_incarnation
                && state.job_revision == expected.job_revision
                && state.control_revision == expected.control_revision
                && state.token.resource == expected.token.resource
                && state.token.owner_node_id == expected.token.owner_node_id
                && state.token.owner_incarnation == expected.token.owner_incarnation
                && state.token.fence == expected.token.fence
                && state.token.revision >= expected.token.revision
                && state.deadline > Instant::now()
                && !state.cancel.is_cancelled()
        })
    })
}
async fn recover_download_holds(
    engine: &EngineHandle,
    active: &ActiveLeases,
    cursor: &mut usize,
    reobserve: bool,
) {
    let mut leases: Vec<_> = active
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, state)| state.kind == LeaseKind::Download)
        .map(|(id, state)| (id.clone(), state.clone()))
        .collect();
    leases.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    let count = leases.len();
    if count == 0 {
        return;
    }
    let start = *cursor % count;
    leases.rotate_left(start);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    for (offset, (lease_id, state)) in leases.into_iter().enumerate() {
        let Some(control) = engine
            .export_job(state.job)
            .await
            .ok()
            .flatten()
            .and_then(|job| job.control())
            .filter(|control| {
                control.lifecycle == "held"
                    && !control.manual_pause
                    && (control
                        .next_retry_at_unix
                        .is_some_and(|deadline| deadline <= now)
                        || (reobserve
                            && control.hold_cause().policy()
                                == nzbd_types::HoldPolicy::ReobserveMount))
            })
        else {
            continue;
        };
        *cursor = (start + offset + 1) % count;
        release_worker_hold(
            engine,
            state.job,
            control,
            lease_authority(active, &lease_id, &state),
        )
        .await;
        break;
    }
}

async fn release_worker_hold(
    engine: &EngineHandle,
    job: JobId,
    control: nzbd_types::JobControl,
    authority: Arc<dyn Fn() -> bool + Send + Sync>,
) -> bool {
    match engine
        .release_hold_checked(
            job,
            control.revision.clone(),
            nzbd_engine::RetryBy::Recovery,
            authority.clone(),
        )
        .await
    {
        Ok(Ok(())) => true,
        Ok(Err(reason)) => {
            if authority() {
                let _ = engine
                    .reschedule_hold_checked(job, control.revision, reason, authority)
                    .await;
            }
            false
        }
        Err(error) => {
            tracing::warn!(job=job.0,%error,"worker hold retry remains pending");
            false
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
    engine: &EngineHandle,
    client: &ClusterClient,
    active: &ActiveLeases,
    budgets: &BudgetLane,
    tracker: &TaskTracker,
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
        budget_ack: budgets.receipt.lock().unwrap().clone(),
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
                    if let Some(next) =
                        renewed_by_resource
                            .get(&state.token.resource)
                            .filter(|next| {
                                state.deadline > Instant::now()
                                    && !state.cancel.is_cancelled()
                                    && next.owner_node_id == state.token.owner_node_id
                                    && next.owner_incarnation == state.token.owner_incarnation
                                    && next.fence == state.token.fence
                                    && next.revision >= state.token.revision
                            })
                    {
                        state.token = next.clone();
                        state.deadline = conservative_deadline(cfg);
                    }
                    if let Some(revision) = resp.controls.get(lease_id) {
                        state.control_revision = *revision;
                    }
                }
            }
            for lease_id in resp.cancel {
                for (lease_id, st) in revoke_leases(active, |id, _| id == lease_id) {
                    tracing::info!(job = st.job.0, %lease_id, "lease cancelled by leader");
                    retire_lease(engine, active, tracker, &budgets.cancel, lease_id, st);
                }
            }
            if let Some(target) = resp.server_budgets {
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
                    budgets.submit(&HashMap::new(), resp.budget_generation);
                } else {
                    budgets.submit(&target, resp.budget_generation);
                }
            }
        }
        Err(e) => tracing::debug!(error = %e, "heartbeat failed (election in progress?)"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn report_completions(
    cfg: &ClusterConfig,
    engine: &EngineHandle,
    client: &ClusterClient,
    active: &ActiveLeases,
    pp: &Option<PpSetup>,
    leader_url: &str,
    dest_dir: &std::path::Path,
    tracker: &TaskTracker,
    shutdown: &CancellationToken,
    order: &mut CompletionOrder,
) {
    let snapshot = engine.snapshot();
    let mut finished: Vec<(String, LeaseState)> = active
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
    finished.sort_by(|(left_id, left), (right_id, right)| {
        order
            .rank(left.kind, left_id, false)
            .cmp(&order.rank(right.kind, right_id, false))
    });

    for (lease_id, lease) in finished {
        if active
            .admissions
            .lock()
            .unwrap()
            .get(&lease_id)
            .is_some_and(|joined| !*joined.borrow())
        {
            continue;
        }
        if !lease_authority(active, &lease_id, &lease)() {
            continue;
        }
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
        if nzbd_state::artifacts::WorkerControlContext::for_job(&job).as_ref()
            != Some(&lease.context)
        {
            continue;
        }
        order.attempted(lease.kind, &lease_id, false);
        if lease.kind == LeaseKind::Segment && job.status != JobStatus::Completed {
            reject_remote_grant(cfg, client, leader_url, &lease_id, &lease.token).await;
            for (id, state) in revoke_leases(active, |id, state| {
                id == lease_id && state.context == lease.context
            }) {
                retire_lease(engine, active, tracker, shutdown, id, state);
            }
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
            cfg,
            engine,
            client,
            active,
            pp,
            leader_url,
            dest_dir,
            (lease.kind, &req),
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

#[derive(serde::Deserialize)]
struct PendingHeader {
    kind: LeaseKind,
    request: RequestHeader,
}
#[derive(serde::Deserialize)]
struct RequestHeader {
    node: String,
    receipt_id: String,
}
#[derive(Default)]
struct OutboxScan {
    entries: Option<std::fs::ReadDir>,
    deferred: Option<PathBuf>,
    headers: std::collections::VecDeque<(PathBuf, PendingHeader)>,
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
fn next_pending_completion(
    directory: &Path,
    node: &str,
    order: &mut CompletionOrder,
) -> Option<PendingCompletion> {
    if order.scan.entries.is_none() {
        order.scan.entries = std::fs::read_dir(directory).ok();
    }
    let mut remaining = MAX_RPC_BYTES;
    for _ in 0..32 {
        if order.scan.headers.len() == 128 {
            break;
        }
        let path = match order.scan.deferred.take() {
            Some(path) => path,
            None => match order.scan.entries.as_mut()?.next() {
                Some(Ok(entry)) => entry.path(),
                Some(Err(_)) => continue,
                None => {
                    order.scan.entries = None;
                    break;
                }
            },
        };
        if path.extension().is_none_or(|extension| extension != "json")
            || order.scan.headers.iter().any(|(cached, _)| *cached == path)
        {
            continue;
        }
        let Ok(metadata) = path.metadata() else {
            continue;
        };
        if metadata.len() > MAX_RPC_BYTES as u64 {
            continue;
        }
        let bytes = metadata.len() as usize;
        if bytes > remaining {
            order.scan.deferred = Some(path);
            break;
        }
        remaining -= bytes;
        // Ignore the full job while decoding only the small routing header.
        // Read no more than the inspected length if the file changes.
        let header = std::fs::File::open(&path).ok().and_then(|file| {
            use std::io::Read;
            serde_json::from_reader::<_, PendingHeader>(std::io::BufReader::new(
                file.take(bytes as u64),
            ))
            .ok()
        });
        let Some(header) = header else { continue };
        if header.request.node != node
            || header.request.node.len() > 256
            || header.request.receipt_id.len() > 4096
        {
            continue;
        }
        order.scan.headers.push_back((path, header));
    }
    let index = order
        .scan
        .headers
        .iter()
        .enumerate()
        .min_by(|(_, (_, left)), (_, (_, right))| {
            order
                .rank(left.kind, &left.request.receipt_id, true)
                .cmp(&order.rank(right.kind, &right.request.receipt_id, true))
        })
        .map(|(index, _)| index)?;
    // Never evict unseen headers. Attempted receipts stay on disk for the next
    // directory cycle, after this cycle's unattempted backlog gets its turn.
    let (path, header) = order.scan.headers.remove(index)?;
    order.attempted(header.kind, &header.request.receipt_id, true);
    // Decode one full immutable request under the existing size cap. Recheck
    // its header; replacement never inherits cached routing classification.
    path.metadata()
        .ok()
        .filter(|metadata| metadata.len() <= MAX_RPC_BYTES as u64)
        .and_then(|_| std::fs::File::open(&path).ok())
        .and_then(|file| {
            use std::io::Read;
            serde_json::from_reader::<_, PendingCompletion>(std::io::BufReader::new(
                file.take(MAX_RPC_BYTES as u64),
            ))
            .ok()
        })
        .filter(|pending| {
            pending.kind == header.kind
                && pending.request.node == node
                && pending.request.receipt_id == header.request.receipt_id
        })
}

#[allow(clippy::too_many_arguments)]
async fn replay_completion_outbox(
    cfg: &ClusterConfig,
    engine: &EngineHandle,
    client: &ClusterClient,
    active: &ActiveLeases,
    pp: &Option<PpSetup>,
    leader_url: &str,
    dest_dir: &std::path::Path,
    order: &mut CompletionOrder,
) {
    let directory = cfg.control_dir.join("completion-outbox");
    let node = cfg.node_name.clone();
    let mut scan_order = std::mem::take(order);
    // Decoding bounded durable descriptors must not occupy a heartbeat worker.
    // The completion task owns and joins this read-only scan before moving on.
    let Ok((pending, scan_order)) = tokio::task::spawn_blocking(move || {
        let pending = next_pending_completion(&directory, &node, &mut scan_order);
        (pending, scan_order)
    })
    .await
    else {
        return;
    };
    *order = scan_order;
    if let Some(pending) = pending {
        if submit_completion(
            cfg,
            engine,
            client,
            active,
            pp,
            leader_url,
            dest_dir,
            (pending.kind, &pending.request),
        )
        .await
        {
            let _ = remove_completion(cfg, &pending.request);
        }
    }
}
#[allow(clippy::too_many_arguments)]
async fn submit_completion(
    cfg: &ClusterConfig,
    engine: &EngineHandle,
    client: &ClusterClient,
    active: &ActiveLeases,
    pp: &Option<PpSetup>,
    leader_url: &str,
    dest_dir: &std::path::Path,
    completion: (LeaseKind, &CompleteRequest),
) -> bool {
    let (kind, req) = completion;
    let job_id = req.job.id;
    let lease_id = req.lease_id.clone();
    match client
        .post_json::<_, CompleteResponse>(leader_url, "/cluster/v1/work/complete", &req)
        .await
    {
        Ok(resp) if resp.ok => {
            if let Some(context) = nzbd_state::artifacts::WorkerControlContext::for_job(&req.job) {
                if let Err(error) = compact_joined_grant(engine, active, job_id, &context).await {
                    tracing::warn!(job=job_id.0,%error,"accepted completion awaits actual writer/execution join");
                    return false;
                }
            }
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
                        // Contract §2.1: the authority holds no history
                        // store, so this node — which writes the history row —
                        // records the job's one terminal through the shared
                        // chain (row, history, stamp, job_pp_finished).
                        crate::leader::record_remote_terminal(
                            engine,
                            setup.history.clone(),
                            &history_job,
                            published_pp_history_entry(
                                &history_job,
                                (!resp.cleanup_only).then_some(final_dir.as_str()),
                                accepted_at_ms,
                            ),
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
                // Contract §2.6: the failure's terminal is durable on the
                // leader now; this node disposes of its own attempt copy.
                if let Some(setup) = pp {
                    if !nzbd_post::manager::dispose_remote_failure(
                        engine,
                        &setup.post,
                        dest_dir,
                        &req.job,
                    )
                    .await
                    {
                        tracing::warn!(job = job_id.0, "remote failure disposal remains pending; the cleanup ledger retries it");
                    }
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
                if let Some(context) =
                    nzbd_state::artifacts::WorkerControlContext::for_job(&req.job)
                {
                    let _ = engine.remove_accepted_worker_job(job_id, context).await;
                } else {
                    let _ = engine.remove_job_silent(job_id).await;
                }
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
    // A rejected publication remains in the outbox unless the authority has
    // durably recorded an exact removal disposition for this same result.
    let Some(context) = nzbd_state::artifacts::WorkerControlContext::for_job(&req.job) else {
        return false;
    };
    let cancellation = crate::cancellation::CancellationRequest {
        lifecycle_protocol: req.lifecycle_protocol,
        node: req.node.clone(),
        job_id: job_id.0,
        job_incarnation: context.job_incarnation.clone(),
        lease_id: req.lease_id.clone(),
        token: req.token.clone(),
        expected_job_revision: req.expected_job_revision,
        result_id: req.result_id.clone(),
        result_ref: req.result_ref.clone(),
        receipt_id: req.receipt_id.clone(),
    };
    let Ok(decision) = client
        .post_json::<_, crate::cancellation::CancellationDecision>(
            leader_url,
            "/cluster/v1/work/cancelled-completion",
            &cancellation,
        )
        .await
    else {
        return false;
    };
    if decision.receipt_id != req.receipt_id {
        return false;
    }
    if let Some(setup) = pp {
        let mut retired = req.job.clone();
        retired
            .params
            .retain(|(key, _)| key != nzbd_types::PP_DONE_PARAM);
        retired
            .params
            .push((nzbd_types::PP_DONE_PARAM.into(), "CANCELLED".into()));
        retired
            .params
            .push(("Cluster:CleanupId".into(), decision.cleanup_id.clone()));
        retired
            .params
            .push(("Cluster:RemovalReason".into(), decision.reason.clone()));
        if record_published_pp_history(
            setup.history.clone(),
            &retired,
            Some(req.job.dir_name.as_str()),
            decision.accepted_at_ms,
        )
        .await
        .is_err()
        {
            return false;
        }
    }
    let source = req
        .job
        .params
        .iter()
        .find(|(key, _)| key == "Artifact:Id")
        .map(|(_, id)| id.clone())
        .zip(
            req.job
                .params
                .iter()
                .find(|(key, _)| key == "Artifact:Generation")
                .map(|(_, generation)| generation.clone()),
        );
    if kind == LeaseKind::Post {
        if let Some((artifact, generation)) = source {
            let inventory = engine.artifacts();
            let proof = nzbd_state::artifacts::FinalizationProof::Cluster {
                receipt: req.receipt_id.clone(),
                accepted_at_ms: decision.accepted_at_ms,
            };
            let source = nzbd_state::artifacts::SourceGeneration {
                artifact,
                generation,
            };
            if !tokio::task::spawn_blocking(move || {
                inventory.acknowledge_worker_completion(job_id.0, &context, &source, &proof)
            })
            .await
            .is_ok_and(|result| result.is_ok_and(|accepted| accepted))
            {
                return false;
            }
        }
    }
    // The private ledger now preserves either exact cleanup obligations or
    // visible keep-files custody. Never activate generic workspace GC here.
    active.lock().unwrap().remove(&lease_id);
    let _ = engine.remove_job_silent(job_id).await;
    true
}

pub(crate) async fn cancel_and_join_execution(
    shared: &crate::leader::LeaderShared,
    context: &nzbd_state::artifacts::WorkerControlContext,
) -> Result<bool, String> {
    let active = shared
        .worker_leases
        .lock()
        .unwrap()
        .clone()
        .ok_or("worker execution registry unavailable")?;
    {
        let states = active.lock().unwrap();
        if let Some(state) = states.get(&context.lease_id) {
            if &state.context != context {
                return Err("worker grant intent changed before cancellation".into());
            }
            state.cancel.cancel();
        }
    }
    active.join_execution(&context.lease_id).await
}

pub(crate) async fn compact_joined_grant(
    engine: &EngineHandle,
    active: &ActiveLeases,
    job: JobId,
    context: &nzbd_state::artifacts::WorkerControlContext,
) -> Result<(), String> {
    let inventory = engine.artifacts();
    let context = context.clone();
    let request =
        tokio::task::spawn_blocking(move || inventory.worker_grant_request(job.0, &context))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
    let Some(request) = request else {
        return Ok(());
    }; // legacy never gains a stop proof
    let id = request.context.lease_id.clone();
    let execution_joined = active.join_execution(&id).await?;
    engine
        .join_cluster_writers(request, execution_joined)
        .await
        .map_err(|e| e.to_string())?;
    active.forget_joined(&id);
    Ok(())
}

async fn record_published_pp_history(
    history: Arc<nzbd_state::history::HistoryDb>,
    job: &nzbd_types::Job,
    result_ref: Option<&str>,
    accepted_at_ms: i64,
) -> Result<(), String> {
    let entry = published_pp_history_entry(job, result_ref, accepted_at_ms);
    tokio::task::spawn_blocking(move || history.record_seq(&entry))
        .await
        .map_err(|error| error.to_string())?
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn published_pp_history_entry(
    job: &nzbd_types::Job,
    result_ref: Option<&str>,
    accepted_at_ms: i64,
) -> nzbd_state::HistoryEntry {
    let status = job
        .params
        .iter()
        .find(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
        .map(|(_, value)| value.clone())
        .unwrap_or_else(|| "SUCCESS".into());
    nzbd_state::HistoryEntry {
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
    }
}

#[allow(clippy::too_many_arguments)]
async fn poll_for_work(
    cfg: &ClusterConfig,
    engine: &EngineHandle,
    client: &ClusterClient,
    active: &ActiveLeases,
    pp: &Option<PpSetup>,
    dest_dir: &std::path::Path,
    tracker: &TaskTracker,
    leader_url: &str,
    owner_incarnation: &str,
    budgets: &BudgetLane,
) {
    let disk_low = engine.snapshot().disk_low;
    let (dl_held, pp_held) = held_slots(active);
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
        storage_instance_id: engine.artifacts().installation.clone(),
        start_ordering: 1,
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
        {
            let states = active.lock().unwrap();
            if states.contains_key(&grant.lease_id)
                || active
                    .retiring
                    .lock()
                    .unwrap()
                    .contains_key(&grant.lease_id)
            {
                continue;
            }
        }
        let (admission_done, admission_joined) = watch::channel(false);
        active
            .admissions
            .lock()
            .unwrap()
            .insert(grant.lease_id.clone(), admission_joined);
        let mut registered = false;
        let mut admitted = false;
        let retiring_id = grant.lease_id.clone();
        let retiring_token = grant.token.clone();
        let job_id = grant.job.id;
        async {

        if grant.job.delivery_lifecycle && grant.lifecycle_protocol < LIFECYCLE_PROTOCOL {
            tracing::warn!(
                job = job_id.0,
                "leader requires cluster lifecycle protocol 2; grant refused before payload mutation"
            );
            reject_remote_grant(cfg, client, leader_url, &grant.lease_id, &grant.token).await;
            return;
        }
        if engine.snapshot().disk_low {
            reject_remote_grant(cfg, client, leader_url, &grant.lease_id, &grant.token).await;
            return;
        }
        let context = nzbd_state::artifacts::WorkerControlContext {
                lease_id: grant.lease_id.clone(),
                job_incarnation: grant.job_incarnation.clone(),
                job_revision: grant.job_revision,
                control_revision: grant.control_revision,
                epoch: grant.epoch,
            };
        let lease_state = LeaseState {
            job: job_id, kind: grant.kind, token: grant.token.clone(),
            job_incarnation: grant.job_incarnation.clone(), job_revision: grant.job_revision,
            control_revision: grant.control_revision, context: context.clone(),
            deadline: conservative_deadline(cfg), cancel: budgets.cancel.child_token(),
            pp_ready: grant.kind == LeaseKind::Post && grant.job.params.iter().any(|(key, _)| key == nzbd_types::PP_DONE_PARAM),
            ready_job: None,
        };
        let authority = lease_authority(active, &grant.lease_id, &lease_state);
        {
            grant
                .job
                .params
                .retain(|(key, _)| key != nzbd_state::artifacts::WORKER_CONTROL_CONTEXT_PARAM);
            grant.job.params.push((
                nzbd_state::artifacts::WORKER_CONTROL_CONTEXT_PARAM.into(),
                serde_json::to_string(&context).expect("grant context serializes"),
            ));
            if grant.token.owner_node_id!=cfg.node_name || grant.token.owner_incarnation!=owner_incarnation
                || grant.scope.get("_startOrdered").and_then(serde_json::Value::as_u64)!=Some(1)
                || grant.scope.get("_workerStorage").and_then(serde_json::Value::as_str)!=Some(engine.artifacts().installation.as_str()) {
                tracing::warn!(job=job_id.0,"grant lacks exact receiver/start ordering; refuse before payload mutation");
                reject_remote_grant(cfg,client,leader_url,&grant.lease_id,&grant.token).await;return;
            }
            let request=nzbd_state::artifacts::WorkerStopRequest{
                job:job_id.0,context:context.clone(),owner_incarnation:owner_incarnation.to_owned(),
                storage_instance_id:engine.artifacts().installation.clone(),removal_revision:None,
                authority_source_json:grant.job.params.iter().find(|(key,_)|key==crate::delivery::AUTHORITY_COPY_PARAM).map(|(_,value)|value.clone()),start_ordered:true,
            };
            {
                let mut leases = active.lock().unwrap();
                if leases.contains_key(&grant.lease_id) || active.retiring.lock().unwrap().contains_key(&grant.lease_id) { return; }
                leases.insert(grant.lease_id.clone(), lease_state.clone());
                registered = true;
            }
            let inventory=engine.artifacts();
            if let Err(error)=tokio::task::spawn_blocking(move||inventory.begin_worker_grant(&request)).await.map_err(|error|error.to_string()).and_then(|result|result.map_err(|error|error.to_string())) {
                tracing::warn!(job=job_id.0,%error,"durable grant start refused");reject_remote_grant(cfg,client,leader_url,&grant.lease_id,&grant.token).await;return;
            }
            if grant.kind == LeaseKind::Download
                && grant
                    .job
                    .params
                    .iter()
                    .any(|(key, _)| key == crate::delivery::AUTHORITY_COPY_PARAM)
            {
                let inventory = engine.artifacts();
                let incoming = grant.job.clone();
                match tokio::task::spawn_blocking(move || {
                    inventory.adopt_worker_retry_for_job(&incoming, &context)
                })
                .await
                {
                    Ok(Ok(Some(adoption))) => {
                        grant.job.set_control(&adoption.control);
                        grant.job.status = JobStatus::Paused;
                        for (key, value) in adoption.probe_params {
                            grant.job.params.retain(|(existing, _)| existing != &key);
                            grant.job.params.push((key, value));
                        }
                    }
                    Ok(Ok(None)) => {}
                    error => {
                        tracing::warn!(
                            job = job_id.0,
                            ?error,
                            "delegated retry checkpoint refused"
                        );
                        reject_remote_grant(cfg, client, leader_url, &grant.lease_id, &grant.token)
                            .await;
                        return;
                    }
                }
            }
        }
        if !authority() { return; }
        match grant.kind {
            LeaseKind::Download => {
                tracing::info!(job = job_id.0, lease = %grant.lease_id, "download lease received");
                if !budgets.apply(&grant.server_budgets, grant.budget_generation).await || !authority() { return; }
                #[cfg(test)]
                let checkpoint = {
                    let authority = authority.clone();
                    let test_node = cfg.node_name.clone();
                    Arc::new(move || { crc_tests::checkpoint(&test_node, job_id); authority() }) as Arc<dyn Fn() -> bool + Send + Sync>
                };
                #[cfg(not(test))]
                let checkpoint = authority.clone();
                let imported = engine.import_job_checked(grant.job, true, false, checkpoint).await;
                #[cfg(test)]
                crc_tests::record_result(&cfg.node_name, job_id, imported.is_ok());
                admitted = imported.is_ok() && authority() && !engine.snapshot().disk_low;
            }
            LeaseKind::Post => {
                let Some(setup) = pp else { return };
                tracing::info!(job = job_id.0, lease = %grant.lease_id, "pp lease received");
                let has_download = active.lock().unwrap().values().any(|lease| lease.kind == LeaseKind::Download);
                if grant.post_fetch_budgeted {
                    if !budgets.apply(&grant.server_budgets, grant.budget_generation).await { return; }
                } else if !has_download && !budgets.apply(&HashMap::new(), grant.budget_generation).await { return; }
                if !authority() { return; }
                if let Err(error) = prepare_pp_attempt(engine.artifacts(), dest_dir, &mut grant.job, grant.token.fence).await {
                    tracing::warn!(job = job_id.0, %error, "private PP attempt could not be prepared");
                    return;
                }
                if !authority() { return; }
                let restored_ready = grant.job.params.iter().any(|(key, _)| key == nzbd_types::PP_DONE_PARAM);
                if engine.import_job_checked(grant.job, false, false, authority.clone()).await.is_ok()
                    && authority() && !engine.snapshot().disk_low {
                    admitted = true;
                    if !restored_ready {
                        run_pp_lease(engine.clone(), setup.clone(), dest_dir.to_path_buf(), active.clone(),
                            grant.lease_id, job_id, tracker, budgets.cancel.clone(), cfg.node_name.clone(), client.clone(), leader_url.to_string());
                    }
                }
            }
            LeaseKind::Segment => {
                tracing::info!(job = job_id.0, lease = %grant.lease_id, "article-range lease received");
                if !budgets.apply(&grant.server_budgets, grant.budget_generation).await || !authority() { return; }
                admitted = engine.import_job_checked(grant.job, false, false, authority.clone()).await.is_ok()
                    && authority() && !engine.snapshot().disk_low;
            }
            LeaseKind::Assemble => {
                let Ok(scope) = serde_json::from_value::<crate::range::AssembleScope>(grant.scope) else { return; };
                if !authority() { return; }
                admitted = true;
                run_assembly(grant.job, scope, dest_dir.to_path_buf(), grant.token.fence, active.clone(), grant.lease_id, tracker);
            }
        }
        }.await;
        if registered && !admitted {
            for (id, state) in revoke_leases(active, |id, state| {
                id == retiring_id
                    && state.token.fence == retiring_token.fence
                    && state.token.owner_incarnation == retiring_token.owner_incarnation
            }) {
                retire_lease(engine, active, tracker, &budgets.cancel, id, state);
            }
            // Failed admission must lose authority before its join is visible
            // to the completion lane. Retirement waits this actual join.
            let _ = admission_done.send(true);
            reject_remote_grant(cfg, client, leader_url, &retiring_id, &retiring_token).await;
        } else {
            let _ = admission_done.send(true);
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

/// Lease renewals can extend the deadline while owner recovery is pending.
/// Recheck at the exact recorded deadline; never drop an issued owner request.
async fn wait_pp_owner_admission(
    engine: &EngineHandle,
    active: &ActiveLeases,
    lease_id: &str,
    job: JobId,
    cancellation: &CancellationToken,
) -> Result<Option<nzbd_engine::PostAdmission>, nzbd_engine::EngineError> {
    let wait_cancel = cancellation.child_token();
    let waiting = engine.wait_post_admission(job, &wait_cancel);
    tokio::pin!(waiting);
    loop {
        let deadline = active
            .lock()
            .unwrap()
            .get(lease_id)
            .filter(|state| state.job == job && !state.cancel.is_cancelled())
            .map(|state| state.deadline);
        let Some(deadline) = deadline.filter(|deadline| *deadline > Instant::now()) else {
            wait_cancel.cancel();
            return waiting.await;
        };
        tokio::select! {
            result = &mut waiting => {
                let admission = result?;
                let current = active.lock().unwrap().get(lease_id)
                    .is_some_and(|state| state.job == job && state.deadline > Instant::now()
                        && !state.cancel.is_cancelled());
                if !current || cancellation.is_cancelled() {
                    if let Some(admission) = admission { engine.end_post_admission(admission).await?; }
                    return Ok(None);
                }
                return Ok(admission);
            }
            _ = tokio::time::sleep_until(deadline) => {},
        }
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
    shutdown: CancellationToken,
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
    let retirement_tracker = tracker.clone();
    active.clone().spawn_operation(tracker,lease_id.clone(),async move {
        let Some(initial) = active.lock().unwrap().get(&lease_id).cloned() else {
            return;
        };
        let retirement_context = initial.context.clone();
        let retire_current = || {
            for (id, state) in revoke_leases(&active, |id, state| id == lease_id && state.context == retirement_context) {
                retire_lease(&engine, &active, &retirement_tracker, &shutdown, id, state);
            }
        };
        if engine.snapshot().disk_low {
            retire_current();
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
        let admission = match wait_pp_owner_admission(&engine, &active, &lease_id, job_id, &initial.cancel).await {
            Ok(Some(admission)) => admission,
            result => {
                tracing::debug!(job=job_id.0, lease=%lease_id, ?result, "PP owner admission ended before processing");
                retire_current();
                return;
            }
        };
        let admitted_engine = engine.for_post_admission(&admission);
        let admission_cancel = admission.cancellation.clone();
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
            commit_ok: {let live=lease_authority(&active,&lease_id,&initial);let cancel=admission_cancel.clone();Arc::new(move||!cancel.is_cancelled() && live())},
        };
        loop {
            if initial.cancel.is_cancelled(){break;}
            // Restored checkpoints keep their original absolute deadline and
            // first blocked time. Heartbeats run independently while waiting.
            if let Some(control)=engine.export_job(job_id).await.ok().flatten().and_then(|job|job.control()).filter(|control|control.lifecycle=="held") {
                if control.manual_pause {
                    tokio::select!{_=initial.cancel.cancelled()=>break,_=tokio::time::sleep(std::time::Duration::from_secs(1))=>{}}
                    continue;
                }
                let now=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
                if control.next_retry_at_unix.is_some_and(|deadline|deadline>now) {
                    tokio::select!{_=initial.cancel.cancelled()=>break,_=tokio::time::sleep(std::time::Duration::from_millis(250))=>{}}
                    continue;
                }
                if !release_worker_hold(&engine,job_id,control,lease_authority(&active,&lease_id,&initial)).await {
                    tokio::select!{_=initial.cancel.cancelled()=>break,_=tokio::time::sleep(std::time::Duration::from_secs(1))=>{}}
                    continue;

                }
            }
            let mut attempt=ctx.clone();attempt.cancel=initial.cancel.child_token();attempt.workers=TaskTracker::new();
            let processing=process_job_ctx(&admitted_engine,&setup.post,&setup.history,&dest_dir,job_id,&attempt);
            tokio::pin!(processing);
            let outcome=tokio::select!{
                _=initial.cancel.cancelled()=>None,
                _=admission_cancel.cancelled()=>None,
                result=&mut processing=>Some(result),
            };
            attempt.cancel.cancel();
            let result=match outcome{Some(result)=>result,None=>{let _=processing.await;Err(nzbd_post::PostError::Subprocess("PP lease authority expired".into()))}};
            attempt.cancel.cancel();attempt.workers.close();attempt.workers.wait().await;
            match result {
                Ok(outcome)=>{
                    tracing::info!(job=job_id.0,%lease_id,outcome=outcome.as_str(),"pp lease finished");
                    // Ending main's owner admission retires its queue row.
                    // Preserve the exact completed descriptor for sealing before
                    // that retirement; its worker checkpoint already persists
                    // restart recovery under this same grant/source context.
                    let ready=match admitted_engine.export_job(job_id).await {
                        Ok(Some(job)) if lease_authority(&active,&lease_id,&initial)()
                            && job.params.iter().any(|(key,_)|key==nzbd_types::PP_DONE_PARAM)
                            && nzbd_state::artifacts::WorkerControlContext::for_job(&job).is_some_and(|context|
                                context.lease_id==lease_id && context.job_incarnation==initial.job_incarnation
                                && context.job_revision==initial.job_revision && context.control_revision==initial.control_revision)=>job,
                        result=>{tracing::warn!(job=job_id.0,%lease_id,?result,"completed PP descriptor awaits exact owner/grant custody");break;}
                    };
                    if let Some(state)=active.lock().unwrap().get_mut(&lease_id){state.ready_job=Some(Box::new(ready));}
                    if let Err(error)=engine.end_post_admission(admission).await{tracing::warn!(job=job_id.0,%error,"PP custody release remains pending");return;}
                    if lease_authority(&active,&lease_id,&initial)(){
                        if let Some(state)=active.lock().unwrap().get_mut(&lease_id){state.pp_ready=true;}
                    }
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
        if let Err(error)=engine.end_post_admission(admission).await{tracing::warn!(job=job_id.0,%error,"PP custody release remains pending");}
        retire_current();

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
    active
        .clone()
        .spawn_operation(tracker, lease_id.clone(), async move {
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
        nzbd_state::artifacts::external_generation_identity(
            self.identity_version,
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
        )
        .map_err(|error| error.to_string())
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
    Err(format!(
        "completed PP lease has no selected publication for private attempt {}; exact source {:?}; current {:?}",
        expected_source.display(),
        source,
        inventory.for_job(job.id.0).ok().flatten().map(|artifact| (
            artifact.id,
            artifact.generation,
            artifact.path,
            artifact.state
        ))
    ))
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
        // Contract §2.5: `.runner-manifest.json` is the local publication's
        // commit marker, not payload; the sealed generation carries the
        // payload the leader verifies against the journaled file list.
        if relative.as_os_str().is_empty() && name == nzbd_state::artifacts::RUNNER_MANIFEST_NAME {
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

fn expire_local_leases(
    engine: &EngineHandle,
    active: &ActiveLeases,
    budgets: &BudgetLane,
    tracker: &TaskTracker,
) {
    let now = Instant::now();
    let expired = revoke_leases(active, |_, state| state.deadline <= now);
    let had_expired = !expired.is_empty();
    for (lease_id, state) in expired {
        tracing::warn!(job = state.job.0, %lease_id, "local lease deadline expired; cancelling work");
        retire_lease(engine, active, tracker, &budgets.cancel, lease_id, state);
    }
    if had_expired && active.lock().unwrap().is_empty() {
        budgets.park();
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

    #[test]
    fn ready_post_and_failed_outbox_attempts_alternate_with_download_progress() {
        let mut order = CompletionOrder::default();
        let mut ready: Vec<_> = (0..8)
            .map(|id| (LeaseKind::Download, format!("download-{id}")))
            .collect();
        ready.extend([
            (LeaseKind::Post, "post-a".into()),
            (LeaseKind::Post, "post-b".into()),
        ]);
        let mut seen = std::collections::HashSet::new();
        // Keep every candidate present, modeling failed attempts. Neither a
        // failed first Post nor an endless download backlog monopolizes IO.
        for turn in 0..32 {
            ready.sort_by(|(a_kind, a), (b_kind, b)| {
                order
                    .rank(*a_kind, a, false)
                    .cmp(&order.rank(*b_kind, b, false))
            });
            let (kind, key) = &ready[0];
            assert_eq!(*kind == LeaseKind::Post, turn % 2 == 0);
            seen.insert(key.clone());
            order.attempted(*kind, key, false);
        }
        assert_eq!(seen.len(), 10);

        let directory = tempfile::tempdir().unwrap();
        let mut original = HashMap::new();
        for (index, (kind, key)) in ready.iter().enumerate() {
            let request = CompleteRequest {
                lifecycle_protocol: LIFECYCLE_PROTOCOL,
                node: "worker".into(),
                lease_id: key.clone(),
                token: test_token(),
                expected_job_revision: 1,
                result_id: key.clone(),
                result_ref: "/retained-result".into(),
                receipt_id: key.clone(),
                job: queued_test_job(),
            };
            let bytes =
                serde_json::to_vec(&serde_json::json!({"kind":kind,"request":request})).unwrap();
            let path = directory.path().join(format!("{index}.json"));
            std::fs::write(&path, &bytes).unwrap();
            original.insert(path, bytes);
        }
        let mut order = CompletionOrder::default();
        let mut seen = std::collections::HashSet::new();
        let mut fresh_seen = std::collections::HashSet::new();
        for turn in 0..32 {
            let pending = next_pending_completion(directory.path(), "worker", &mut order).unwrap();
            assert_eq!(pending.kind == LeaseKind::Post, turn % 2 == 0);
            seen.insert(pending.request.receipt_id.clone());
            order.attempted(pending.kind, &pending.request.receipt_id, true);
            // Production runs replay then fresh-ready selection. Failed
            // pending Posts must not reset the ready lane's turn each loop.
            ready.sort_by(|(a_kind, a), (b_kind, b)| {
                order
                    .rank(*a_kind, a, false)
                    .cmp(&order.rank(*b_kind, b, false))
            });
            let (kind, key) = &ready[0];
            assert_eq!(*kind == LeaseKind::Post, turn % 2 == 0);
            fresh_seen.insert(key.clone());
            order.attempted(*kind, key, false);
        }
        assert_eq!(
            seen.len(),
            10,
            "failed pending receipts rotate without needing a live lease"
        );
        assert_eq!(
            fresh_seen.len(),
            10,
            "fresh Post and download progress survive failed outbox replay"
        );
        for (path, bytes) in original {
            assert_eq!(
                std::fs::read(path).unwrap(),
                bytes,
                "selection cannot change immutable outbox requests"
            );
        }
    }

    #[test]
    fn bounded_outbox_cursor_visits_every_retained_receipt_and_skips_malformed_turns() {
        let directory = tempfile::tempdir().unwrap();
        for index in 0..256 {
            let kind = if index % 16 == 0 {
                LeaseKind::Post
            } else {
                LeaseKind::Download
            };
            let request = CompleteRequest {
                lifecycle_protocol: LIFECYCLE_PROTOCOL,
                node: "worker".into(),
                lease_id: format!("lease-{index}"),
                token: test_token(),
                expected_job_revision: 1,
                result_id: format!("result-{index}"),
                result_ref: "/retained".into(),
                receipt_id: format!("receipt-{index:04}"),
                job: queued_test_job(),
            };
            std::fs::write(
                directory.path().join(format!("{index:04}.json")),
                serde_json::to_vec(&serde_json::json!({"kind":kind,"request":request})).unwrap(),
            )
            .unwrap();
        }
        let mut order = CompletionOrder::default();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1024 {
            if let Some(pending) = next_pending_completion(directory.path(), "worker", &mut order) {
                seen.insert(pending.request.receipt_id);
            }
            assert!(order.scan.headers.len() < 128);
            if seen.len() == 256 {
                break;
            }
        }
        assert_eq!(
            seen.len(),
            256,
            "retained failures larger than the header cache must all get a turn"
        );
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 256);

        let malformed = tempfile::tempdir().unwrap();
        std::fs::write(
            malformed.path().join("bad-post.json"),
            br#"{"kind":"post","request":{"node":"worker","receipt_id":"bad-post","job":null}}"#,
        )
        .unwrap();
        let download = std::fs::read(directory.path().join("0001.json")).unwrap();
        std::fs::write(malformed.path().join("good-download.json"), download).unwrap();
        let mut order = CompletionOrder::default();
        assert!(
            next_pending_completion(malformed.path(), "worker", &mut order).is_none(),
            "the selected malformed Post must not become a request"
        );
        assert_eq!(
            order.scan.headers.len(),
            1,
            "unselected job bodies are not decoded"
        );
        let next = next_pending_completion(malformed.path(), "worker", &mut order).unwrap();
        assert_eq!(
            next.kind,
            LeaseKind::Download,
            "failed body validation consumes its preferred turn"
        );
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
    fn post_commit_authority_accepts_renewal_but_refuses_same_id_new_intent() {
        let active: ActiveLeases = Default::default();
        let state = LeaseState {
            job: JobId(77),
            kind: LeaseKind::Post,
            token: test_token(),
            job_incarnation: "original-job".into(),
            job_revision: 4,
            control_revision: 4,
            context: nzbd_state::artifacts::WorkerControlContext {
                lease_id: "same-id".into(),
                job_incarnation: "original-job".into(),
                job_revision: 4,
                control_revision: 4,
                epoch: 1,
            },
            deadline: Instant::now() + std::time::Duration::from_secs(30),
            cancel: CancellationToken::new(),
            pp_ready: false,
            ready_job: None,
        };
        active
            .lock()
            .unwrap()
            .insert("same-id".into(), state.clone());
        let commit = lease_authority(&active, "same-id", &state);
        assert!(commit());
        active
            .lock()
            .unwrap()
            .get_mut("same-id")
            .unwrap()
            .token
            .revision += 1;
        assert!(commit(), "same operation heartbeat renewal remains valid");
        active
            .lock()
            .unwrap()
            .get_mut("same-id")
            .unwrap()
            .control_revision += 1;
        assert!(
            !commit(),
            "future deadline cannot authorize old PP after newer control intent"
        );
        active
            .lock()
            .unwrap()
            .insert("same-id".into(), state.clone());
        active
            .lock()
            .unwrap()
            .get_mut("same-id")
            .unwrap()
            .job_incarnation = "replacement-job".into();
        assert!(!commit());
        active.lock().unwrap().insert("same-id".into(), state);
        active
            .lock()
            .unwrap()
            .get_mut("same-id")
            .unwrap()
            .token
            .fence += 1;
        assert!(!commit());
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn blocked_retirement_and_budget_drain_do_not_starve_healthy_heartbeats() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        std::fs::create_dir(&dest).unwrap();
        let mut config = EngineConfig::single_node(
            Vec::new(),
            tmp.path().join("state"),
            dest,
            Tuning {
                min_free_disk_bytes: 0,
                ..Default::default()
            },
            None,
        );
        config.persist_queue = false;
        let engine = Engine::spawn(config).await.unwrap();
        let context = nzbd_state::artifacts::WorkerControlContext {
            lease_id: "expired".into(),
            job_incarnation: "job-incarnation".into(),
            job_revision: 1,
            control_revision: 1,
            epoch: 7,
        };
        let mut job = queued_test_job();
        job.status = JobStatus::Completed;
        job.params.push((
            nzbd_state::artifacts::WORKER_CONTROL_CONTEXT_PARAM.into(),
            serde_json::to_string(&context).unwrap(),
        ));
        engine.import_job(job, false, false).await.unwrap();
        let admission = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            engine.wait_post_admission(JobId(77), &CancellationToken::new()),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        let shutdown = CancellationToken::new();
        let tracker = TaskTracker::new();
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let drain = Arc::new(tokio::sync::Semaphore::new(0));
        let applied_targets = Arc::new(Mutex::new(Vec::new()));
        let budgets = BudgetLane::spawn_with_apply(shutdown.clone(), &tracker, {
            let entered = entered.clone();
            let drain = drain.clone();
            let applied_targets = applied_targets.clone();
            let engine = engine.clone();
            move |command| {
                let entered = entered.clone();
                let drain = drain.clone();
                let applied_targets = applied_targets.clone();
                let engine = engine.clone();
                async move {
                    applied_targets
                        .lock()
                        .unwrap()
                        .push((command.generation, command.budgets.clone()));
                    entered.add_permits(1);
                    drain.acquire().await.unwrap().forget();
                    // The lane must wait for this actual engine acknowledgement,
                    // including when a controlled drain has not yet completed.
                    let actual = engine
                        .set_server_budgets(HashMap::new())
                        .await
                        .map_err(|error| error.to_string())?;
                    Ok(Some(BudgetAck {
                        cluster_generation: command.generation,
                        engine_generation: actual.generation,
                        drained: actual.drained,
                    }))
                }
            }
        });
        let first = HashMap::from([("provider".into(), 4)]);
        budgets.submit(&first, 1);
        tokio::time::timeout(std::time::Duration::from_secs(2), entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let active = ActiveLeases::default();
        let expired_cancel = CancellationToken::new();
        active.lock().unwrap().insert(
            "expired".into(),
            LeaseState {
                job: JobId(77),
                kind: LeaseKind::Post,
                token: test_token(),
                job_incarnation: context.job_incarnation.clone(),
                job_revision: 1,
                control_revision: 1,
                context,
                deadline: Instant::now() - std::time::Duration::from_millis(1),
                cancel: expired_cancel.clone(),
                pp_ready: false,
                ready_job: None,
            },
        );
        let mut healthy_token = test_token();
        healthy_token.resource = "healthy".into();
        let original_deadline = Instant::now() + std::time::Duration::from_secs(2);
        active.lock().unwrap().insert(
            "healthy".into(),
            LeaseState {
                job: JobId(78),
                kind: LeaseKind::Download,
                token: healthy_token,
                job_incarnation: "healthy-job".into(),
                job_revision: 1,
                control_revision: 1,
                context: nzbd_state::artifacts::WorkerControlContext {
                    lease_id: "healthy".into(),
                    job_incarnation: "healthy-job".into(),
                    job_revision: 1,
                    control_revision: 1,
                    epoch: 7,
                },
                deadline: original_deadline,
                cancel: CancellationToken::new(),
                pp_ready: false,
                ready_job: None,
            },
        );
        expire_local_leases(&engine, &active, &budgets, &tracker);
        assert!(expired_cancel.is_cancelled());
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            admission.cancellation.cancelled(),
        )
        .await
        .unwrap();
        assert_eq!(
            active.retiring.lock().unwrap().len(),
            1,
            "actual PP custody still occupies its slot"
        );
        assert_eq!(
            held_slots(&active),
            (1, 1),
            "revoked but unjoined PP cannot advertise a free slot"
        );
        let seen = Arc::new(AtomicUsize::new(0));
        let observed = seen.clone();
        let app = Router::new().route(
            "/cluster/v1/work/heartbeat",
            post(move |Json(req): Json<HeartbeatRequest>| {
                let observed = observed.clone();
                async move {
                    assert_eq!(req.leases.len(), 1);
                    assert_eq!(req.leases[0].lease_id, "healthy");
                    assert!(
                        req.budget_ack.is_none(),
                        "no receipt before actual drain acknowledgement"
                    );
                    observed.fetch_add(1, Ordering::SeqCst);
                    let mut token = req.leases[0].token.clone();
                    token.revision += 1;
                    Json(HeartbeatResponse {
                        cancel: Vec::new(),
                        renewed: vec![token],
                        controls: HashMap::new(),
                        server_budgets: Some(HashMap::from([("provider".into(), 1)])),
                        budget_generation: 2,
                        post_fetch_budgeted: true,
                    })
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let cfg = ClusterConfig {
            cluster_id: "test".into(),
            node_name: "worker".into(),
            shared_dir: tmp.path().into(),
            advertise_url: "http://worker.invalid".into(),
            secret: "secret".into(),
            coordinator: false,
            priority: 0,
            download: true,
            max_download_jobs: 1,
            post_process: true,
            pp_slots: 1,
            lease_interval: std::time::Duration::from_millis(100),
            takeover_after: std::time::Duration::from_secs(2),
            worker_ttl: std::time::Duration::from_secs(2),
            control_dir: tmp.path().join("control"),
            control_node_id: 1,
            control_raft_bind: "127.0.0.1:1".into(),
            control_api_bind: "127.0.0.1:2".into(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: Vec::new(),
            torrent_payload_roots: Vec::new(),
        };
        let client = ClusterClient::new("secret".into());
        for _ in 0..8 {
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                heartbeat_and_cancel(&cfg, &engine, &client, &active, &budgets, &tracker, &url),
            )
            .await
            .expect("healthy renewal must not wait for another lease's retirement or budget drain");
            expire_local_leases(&engine, &active, &budgets, &tracker);
            assert_eq!(active.retiring.lock().unwrap().len(), 1);
            assert!(active.lock().unwrap()["healthy"].deadline > Instant::now());
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
        assert!(Instant::now() > original_deadline);
        assert_eq!(seen.load(Ordering::SeqCst), 8);
        assert!(budgets.receipt.lock().unwrap().is_none());
        assert_eq!(applied_targets.lock().unwrap().len(), 1);
        // Older grants cannot re-expand a newer target. Equal-generation
        // local parking is ordered, and the latest target is applied once.
        budgets.submit(&HashMap::from([("provider".into(), 8)]), 1);
        budgets.park();
        let target = HashMap::from([("provider".into(), 1)]);
        let applied = budgets.apply(&target, 2);
        tokio::pin!(applied);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut applied)
                .await
                .is_err()
        );
        drain.add_permits(2);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(3), &mut applied)
                .await
                .unwrap()
        );
        assert_eq!(
            applied_targets.lock().unwrap().as_slice(),
            &[(1, first.clone()), (2, target.clone())]
        );
        assert!(budgets
            .receipt
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|ack| ack.cluster_generation == 2 && ack.drained));
        engine.end_post_admission(admission).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while !active.retiring.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("slot releases only after real owner retirement acknowledgement");
        assert!(engine
            .recovery_document()
            .await
            .unwrap()
            .jobs
            .iter()
            .all(|job| job.id != JobId(77)));
        shutdown.cancel();
        tracker.close();
        tokio::time::timeout(std::time::Duration::from_secs(3), tracker.wait())
            .await
            .unwrap();
        server.abort();
        assert!(engine.shutdown().await);
    }

    #[tokio::test]
    async fn expiry_before_import_preserves_a_durable_stop_receipt_and_resume_journal() {
        let tmp = tempfile::tempdir().unwrap();
        let state_dir = tmp.path().join("state");
        let dest = tmp.path().join("dest");
        std::fs::create_dir(&dest).unwrap();
        let mut config = EngineConfig::single_node(
            Vec::new(),
            state_dir.clone(),
            dest,
            Tuning {
                min_free_disk_bytes: 0,
                ..Default::default()
            },
            None,
        );
        config.persist_queue = false;
        let engine = Engine::spawn(config).await.unwrap();
        let context = nzbd_state::artifacts::WorkerControlContext {
            lease_id: "expired-before-import".into(),
            job_incarnation: "job-incarnation".into(),
            job_revision: 1,
            control_revision: 1,
            epoch: 7,
        };
        let inventory = engine.artifacts();
        let mut request = nzbd_state::artifacts::WorkerStopRequest {
            job: 77,
            context: context.clone(),
            owner_incarnation: "worker-boot".into(),
            storage_instance_id: inventory.installation.clone(),
            removal_revision: None,
            authority_source_json: None,
            start_ordered: true,
        };
        inventory.begin_worker_grant(&request).unwrap();
        let record = nzbd_state::JournalRecord {
            job: JobId(77),
            file: nzbd_types::FileId(77),
            segment_number: 1,
            offset: 0,
            len: 4,
            crc32: crc32fast::hash(b"kept"),
            file_size: 4,
        };
        let mut journals =
            nzbd_state::JobJournals::open(&state_dir, "expired-before-import").unwrap();
        journals.append(&record).unwrap();
        journals.sync().unwrap();
        drop(journals);
        let active = ActiveLeases::default();
        let (done, joined) = watch::channel(false);
        active
            .admissions
            .lock()
            .unwrap()
            .insert(context.lease_id.clone(), joined);
        active.lock().unwrap().insert(
            context.lease_id.clone(),
            LeaseState {
                job: JobId(77),
                kind: LeaseKind::Post,
                token: test_token(),
                job_incarnation: context.job_incarnation.clone(),
                job_revision: 1,
                control_revision: 1,
                context: context.clone(),
                deadline: Instant::now() - std::time::Duration::from_millis(1),
                cancel: CancellationToken::new(),
                pp_ready: false,
                ready_job: None,
            },
        );
        let tracker = TaskTracker::new();
        let shutdown = CancellationToken::new();
        let budgets = BudgetLane::spawn(engine.clone(), Vec::new(), shutdown.clone(), &tracker);
        expire_local_leases(&engine, &active, &budgets, &tracker);
        assert_eq!(held_slots(&active), (0, 1));
        assert!(inventory
            .prepare_worker_stop(&request)
            .unwrap()
            .receipt
            .is_none());
        done.send_replace(true);
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while !active.retiring.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("joined pre-import grant must persist stop evidence");
        assert!(
            !active.join_execution(&context.lease_id).await.unwrap(),
            "volatile watches have been compacted"
        );
        // A later exact authority removal must find durable evidence even
        // though no queue job ever existed and no sealed completion was made.
        request.removal_revision = Some(2);
        let receipt = inventory
            .prepare_worker_stop(&request)
            .unwrap()
            .receipt
            .expect("actual join survives a later cancellation request");
        assert_eq!(receipt.context, context);
        assert!(engine.recovery_document().await.unwrap().jobs.is_empty());
        let resumed = nzbd_state::JobJournals::replay_all(&state_dir).unwrap();
        assert!(resumed.iter().any(|saved| saved.job == record.job
            && saved.file == record.file
            && saved.segment_number == record.segment_number
            && saved.crc32 == record.crc32));
        shutdown.cancel();
        tracker.close();
        tokio::time::timeout(std::time::Duration::from_secs(3), tracker.wait())
            .await
            .unwrap();
        assert!(engine.shutdown().await);
    }

    #[tokio::test]
    async fn shutdown_joins_budget_drain_and_wakes_pending_admission() {
        let tracker = TaskTracker::new();
        let shutdown = CancellationToken::new();
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let budgets = BudgetLane::spawn_with_apply(shutdown.clone(), &tracker, {
            let entered = entered.clone();
            let release = release.clone();
            let calls = calls.clone();
            move |command| {
                let entered = entered.clone();
                let release = release.clone();
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    entered.add_permits(1);
                    release.acquire().await.unwrap().forget();
                    Ok(Some(BudgetAck {
                        cluster_generation: command.generation,
                        engine_generation: 1,
                        drained: true,
                    }))
                }
            }
        });
        budgets.submit(&HashMap::new(), 1);
        tokio::time::timeout(std::time::Duration::from_secs(1), entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let target = HashMap::from([("provider".into(), 1)]);
        let admission = budgets.apply(&target, 2);
        tokio::pin!(admission);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut admission)
                .await
                .is_err()
        );
        shutdown.cancel();
        tracker.close();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), tracker.wait())
                .await
                .is_err(),
            "shutdown must retain ownership of the in-flight drain"
        );
        assert!(budgets.receipt.lock().unwrap().is_none());
        release.add_permits(1);
        assert!(
            !tokio::time::timeout(std::time::Duration::from_secs(1), admission)
                .await
                .unwrap(),
            "unapplied pending grant must wake refused after shutdown"
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), tracker.wait())
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            budgets
                .receipt
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .cluster_generation,
            1
        );
    }

    #[tokio::test]
    async fn joining_admission_waits_for_the_operation_it_installs() {
        let active = ActiveLeases::default();
        let tracker = TaskTracker::new();
        let (done, joined) = watch::channel(false);
        active
            .admissions
            .lock()
            .unwrap()
            .insert("late-operation".into(), joined);
        let joining = active.join_execution("late-operation");
        tokio::pin!(joining);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut joining)
                .await
                .is_err()
        );
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        active.spawn_operation(&tracker, "late-operation".into(), {
            let release = release.clone();
            async move {
                release.acquire().await.unwrap().forget();
            }
        });
        done.send_replace(true);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut joining)
                .await
                .is_err(),
            "admission acknowledgement is not its execution join"
        );
        release.add_permits(1);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), joining)
                .await
                .unwrap()
                .unwrap()
        );
        tracker.close();
        tracker.wait().await;
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
                context: nzbd_state::artifacts::WorkerControlContext {
                    lease_id: "expired".into(),
                    job_incarnation: "job-incarnation".into(),
                    job_revision: 1,
                    control_revision: 1,
                    epoch: 1,
                },
                deadline: Instant::now() - std::time::Duration::from_millis(1),
                cancel: cancel.clone(),
                pp_ready: false,
                ready_job: None,
            },
        );

        let tracker = TaskTracker::new();
        let shutdown = CancellationToken::new();
        let budgets = BudgetLane::spawn(engine.clone(), Vec::new(), shutdown.clone(), &tracker);
        expire_local_leases(&engine, &active, &budgets, &tracker);

        assert!(active.lock().unwrap().is_empty());
        assert!(cancel.is_cancelled());
        shutdown.cancel();
        tracker.close();
        tracker.wait().await;
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
        let shutdown = CancellationToken::new();
        let budgets = BudgetLane::spawn(engine.clone(), Vec::new(), shutdown.clone(), &tracker);

        let poll = poll_for_work(
            &cfg,
            &engine,
            &client,
            &active,
            &None,
            &dest,
            &tracker,
            &leader_url,
            "test-incarnation",
            &budgets,
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

        shutdown.cancel();
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
        // Receipt activation does not wait for independent inventory cleanup.
        // Drive real reconciliation and require its exact durable retirement.
        let inventory = engine.artifacts();
        let retirement = format!("retire-{}", workspace.scratch.generation);
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let retired = loop {
            let maintenance = inventory.clone();
            tokio::task::spawn_blocking(move || maintenance.tick())
                .await
                .unwrap()
                .unwrap();
            let operation = inventory.operation(&retirement).ok();
            if let Some(operation) = operation.filter(|operation| operation.state == "succeeded") {
                assert_eq!(
                    inventory.get(&workspace.scratch.id).unwrap().state,
                    "deleted"
                );
                assert!(!workspace.scratch.path.exists());
                break operation;
            }
            assert!(
                Instant::now() < deadline,
                "checked retirement did not finish: {:?}",
                inventory.operation(&retirement)
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };
        let proof: serde_json::Value = serde_json::from_str(&retired.request).unwrap();
        assert_eq!(proof["source"]["artifact"], source.id);
        assert_eq!(proof["source"]["generation"], source.generation);
        assert_eq!(proof["scratch"]["artifact"], workspace.scratch.id);
        assert_eq!(proof["scratch"]["generation"], workspace.scratch.generation);
        assert_eq!(proof["outcome"], "SUCCESS");
        assert_eq!(proof["proof"]["kind"], "cluster");
        assert_eq!(proof["proof"]["receipt"], request.receipt_id);
        assert_eq!(proof["proof"]["accepted_at_ms"], 10);
        assert!(source.path.join("payload").exists());
        accept_worker_retirement(&engine, &request, &response)
            .await
            .unwrap();
        engine.shutdown().await;
    }
}

#[cfg(test)]
#[path = "worker_crc_tests.rs"]
mod crc_tests;
