//! Leader role (CLUSTERING.md §6): the work-lease endpoints, the lease
//! table with TTL reclaim, the assignment scheduler and connection-budget
//! partitioning. Active only while this node's election view says `is_me`;
//! handlers reject otherwise (workers re-resolve the leader and retry).

use crate::control::{ControlStore, LeaseClaim, LeaseToken, MutationOutcome, ScriptReceiptOutcome};
use crate::election::LeaderView;
use crate::http::secret_matches;
use crate::proto::*;
use crate::registry::read_nodes;
use crate::{ClusterConfig, SharedLayout};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use nzbd_engine::EngineHandle;
use nzbd_types::{JobId, JobStatus, ServerDef};
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Debug, Clone)]
struct LeaseInfo {
    job: JobId,
    node: String,
    kind: LeaseKind,
    token: LeaseToken,
    job_revision: u64,
    control_revision: u64,
    last_hb: Instant,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct BudgetHandoff {
    revision: u64,
    generation: u64,
    grants: HashMap<String, HashMap<String, u16>>,
    commands: HashMap<String, HashMap<String, u16>>,
    pending_target: Option<HashMap<String, HashMap<String, u16>>>,
    awaiting_shrink: HashSet<String>,
}

pub struct LeaderShared {
    pub engine: EngineHandle,
    pub layout: SharedLayout,
    pub dest_dir: PathBuf,
    post: Option<nzbd_post::manager::PostConfig>,
    pub cfg: ClusterConfig,
    pub servers: Vec<ServerDef>,
    pub view: watch::Receiver<LeaderView>,
    pub control: Option<ControlStore>,
    pub history: Option<Arc<nzbd_state::history::HistoryDb>>,
    pub owner_incarnation: String,
    leases: Mutex<HashMap<String, LeaseInfo>>,
    /// Node liveness by observed seq progression: name → (seq, last change).
    node_seen: Mutex<HashMap<String, (u64, Instant)>>,
    budgets: Mutex<BudgetHandoff>,
    /// Serializes externally acknowledged queue mutations and publication
    /// commits. The engine is speculative until replicated control accepts
    /// the exact request-local delta.
    pub(crate) mutation_serial: tokio::sync::Mutex<()>,
    authority_ready: AtomicBool,
}

pub(crate) struct LeaderDurability {
    post: Option<nzbd_post::manager::PostConfig>,
    control: Option<ControlStore>,
    history: Option<Arc<nzbd_state::history::HistoryDb>>,
    owner_incarnation: String,
}

impl LeaderDurability {
    pub(crate) fn new(
        control: Option<ControlStore>,
        history: Option<Arc<nzbd_state::history::HistoryDb>>,
        owner_incarnation: String,
    ) -> Self {
        Self {
            post: None,
            control,
            history,
            owner_incarnation,
        }
    }
    pub(crate) fn with_post(mut self, post: Option<nzbd_post::manager::PostConfig>) -> Self {
        self.post = post;
        self
    }
}

impl LeaderShared {
    pub(crate) fn new(
        engine: EngineHandle,
        layout: SharedLayout,
        dest_dir: PathBuf,
        cfg: ClusterConfig,
        servers: Vec<ServerDef>,
        view: watch::Receiver<LeaderView>,
        durability: LeaderDurability,
    ) -> Arc<LeaderShared> {
        Arc::new(LeaderShared {
            engine,
            layout,
            dest_dir,
            post: durability.post,
            cfg,
            servers,
            view,
            control: durability.control,
            history: durability.history,
            owner_incarnation: durability.owner_incarnation,
            leases: Mutex::new(HashMap::new()),
            node_seen: Mutex::new(HashMap::new()),
            budgets: Mutex::new(BudgetHandoff::default()),
            mutation_serial: tokio::sync::Mutex::new(()),
            authority_ready: AtomicBool::new(false),
        })
    }

    fn publication_root(&self, job: &nzbd_types::Job, post: bool) -> PathBuf {
        if post {
            if let Some(cfg) = &self.post {
                return cfg
                    .categories
                    .iter()
                    .find(|r| {
                        job.category
                            .as_deref()
                            .is_some_and(|name| r.name.eq_ignore_ascii_case(name.trim()))
                    })
                    .and_then(|r| r.dest_dir.clone())
                    .or_else(|| cfg.completed_dir.clone())
                    .unwrap_or_else(|| self.dest_dir.clone());
            }
        }
        self.dest_dir.clone()
    }

    pub(crate) fn authority_ready(&self) -> bool {
        self.authority_ready.load(Ordering::Acquire)
    }

    pub(crate) async fn engine_projection(&self) -> Result<HashMap<u64, nzbd_types::Job>, String> {
        let mut jobs = HashMap::new();
        for summary in &self.engine.snapshot().jobs {
            if let Some(job) = self
                .engine
                .export_job(summary.id)
                .await
                .map_err(|error| format!("export queue projection: {error}"))?
            {
                jobs.insert(u64::from(job.id.0), job);
            }
        }
        Ok(jobs)
    }

    pub(crate) async fn restore_control_projection(&self) -> Result<(), String> {
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| "replicated control unavailable".to_owned())?;
        let jobs = control
            .load_jobs()
            .await?
            .into_iter()
            .map(|(job, _, _)| job)
            .collect();
        self.engine
            .adopt_replicated_authority(jobs)
            .await
            .map_err(|error| format!("replace local control projection: {error}"))
    }

    /// Commit only rows changed by one serialized HTTP request. This avoids
    /// treating an incomplete local snapshot as permission to delete any
    /// unrelated replicated work.
    pub(crate) async fn commit_mutation_delta(
        &self,
        before: &HashMap<u64, nzbd_types::Job>,
    ) -> Result<(), String> {
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| "replicated control unavailable".to_owned())?;
        let after = self.engine_projection().await?;
        let mut ids: HashSet<u64> = before.keys().copied().collect();
        ids.extend(after.keys().copied());
        let mut changes = Vec::new();
        for job_id in ids {
            let old = before
                .get(&job_id)
                .map(serde_json::to_vec)
                .transpose()
                .map_err(|error| format!("encode prior queue row: {error}"))?;
            let new = after
                .get(&job_id)
                .map(serde_json::to_vec)
                .transpose()
                .map_err(|error| format!("encode next queue row: {error}"))?;
            if old != new {
                changes.push((job_id, after.get(&job_id).cloned()));
            }
        }
        control.commit_job_delta(&changes).await?;
        self.restore_control_projection().await
    }

    fn is_leader(&self) -> bool {
        self.view.borrow().is_me
    }

    pub(crate) fn diagnostic_snapshot(&self) -> serde_json::Value {
        let now = Instant::now();
        let leases: Vec<_> = self
            .leases
            .lock()
            .unwrap()
            .iter()
            .map(|(id, lease)| {
                serde_json::json!({
                    "lease_id": id,
                    "resource": lease.token.resource,
                    "job": lease.job.0,
                    "node": lease.node,
                    "kind": lease.kind,
                    "fence": lease.token.fence,
                    "revision": lease.token.revision,
                    "age_ms": now.duration_since(lease.last_hb).as_millis(),
                    "expires_at_unix_ms": lease.token.expires_at_unix_ms,
                })
            })
            .collect();
        let budget = self.budgets.lock().unwrap();
        let mut lost: HashMap<String, u16> = HashMap::new();
        for node in &budget.awaiting_shrink {
            let old = budget.grants.get(node);
            let commanded = budget.commands.get(node);
            for server in &self.servers {
                let held = old
                    .and_then(|caps| caps.get(&server.name))
                    .copied()
                    .unwrap_or(0);
                let next = commanded
                    .and_then(|caps| caps.get(&server.name))
                    .copied()
                    .unwrap_or(0);
                *lost.entry(server.name.clone()).or_default() += held.saturating_sub(next);
            }
        }
        serde_json::json!({
            "leases": leases,
            "provider_budgets": {
                "generation": budget.generation,
                "desired": budget.pending_target.as_ref().unwrap_or(&budget.grants),
                "commanded": budget.commands,
                "awaiting_shrink": budget.awaiting_shrink,
                "uncertain_reserved_capacity": lost,
            }
        })
    }

    fn epoch(&self) -> u64 {
        self.view.borrow().epoch()
    }

    /// Every remote executor that may currently use provider connections.
    /// The authority itself never executes work. PP leases count
    /// because delayed PAR recovery can open NNTP connections. Remote nodes
    /// come first so a scarce remainder is not stranded on an idle leader.
    fn budget_nodes(&self) -> Vec<String> {
        let mut nodes: Vec<String> = self
            .leases
            .lock()
            .unwrap()
            .values()
            .map(|l| l.node.clone())
            .collect();
        nodes.sort();
        nodes.dedup();
        nodes.retain(|node| node != &self.cfg.node_name);
        nodes
    }

    /// This executor's exact share. Remainders go to the first stable node
    /// names and later nodes may receive zero: unlike `.max(1)`, the issued
    /// shares always sum to at most the provider account cap.
    fn budgets_for_node(&self, node: &str) -> HashMap<String, u16> {
        let nodes = self.budget_nodes();
        let position = nodes.iter().position(|candidate| candidate == node);
        let count = nodes.len() as u16;
        self.servers
            .iter()
            .map(|server| {
                let share = match (position, count) {
                    (Some(position), count) if count > 0 => {
                        let base = server.max_connections / count;
                        let remainder = server.max_connections % count;
                        base + u16::from((position as u16) < remainder)
                    }
                    _ => 0,
                };
                (server.name.clone(), share)
            })
            .collect()
    }

    fn budget_target(&self) -> HashMap<String, HashMap<String, u16>> {
        self.budget_nodes()
            .into_iter()
            .map(|node| {
                let share = self.budgets_for_node(&node);
                (node, share)
            })
            .collect()
    }

    /// Return the only safe command for a node. Reductions are issued first;
    /// increases remain withheld until every prior holder reports that all of
    /// its connection tasks observed the shrink between NNTP batches.
    fn budget_command(&self, node: &str, ack: Option<&BudgetAck>) -> (u64, HashMap<String, u16>) {
        let target = self.budget_target();
        let mut state = self.budgets.lock().unwrap();

        if let Some(ack) = ack {
            if ack.cluster_generation == state.generation && ack.drained {
                state.awaiting_shrink.remove(node);
            }
        }

        if state.pending_target.is_some() && state.awaiting_shrink.is_empty() {
            state.generation = state.generation.saturating_add(1);
            state.grants = state.pending_target.take().unwrap();
            state.commands = state.grants.clone();
        }

        if state.pending_target.is_none() && state.grants != target {
            if state.grants.is_empty() {
                state.generation = state.generation.saturating_add(1);
                state.grants = target;
                state.commands = state.grants.clone();
            } else {
                let mut commands = state.grants.clone();
                let mut awaiting = HashSet::new();
                for (holder, old) in &state.grants {
                    let desired = target.get(holder);
                    let mut command = old.clone();
                    let mut shrinks = false;
                    for (provider, old_cap) in old {
                        let new_cap = desired
                            .and_then(|caps| caps.get(provider))
                            .copied()
                            .unwrap_or(0);
                        if new_cap < *old_cap {
                            command.insert(provider.clone(), new_cap);
                            shrinks = true;
                        }
                    }
                    if shrinks {
                        awaiting.insert(holder.clone());
                    }
                    commands.insert(holder.clone(), command);
                }
                for newcomer in target.keys() {
                    commands.entry(newcomer.clone()).or_insert_with(|| {
                        self.servers
                            .iter()
                            .map(|server| (server.name.clone(), 0))
                            .collect()
                    });
                }
                state.generation = state.generation.saturating_add(1);
                state.commands = commands;
                state.awaiting_shrink = awaiting;
                state.pending_target = Some(target);
                if state.awaiting_shrink.is_empty() {
                    state.generation = state.generation.saturating_add(1);
                    state.grants = state.pending_target.take().unwrap();
                    state.commands = state.grants.clone();
                }
            }
        }

        let command = state.commands.get(node).cloned().unwrap_or_else(|| {
            self.servers
                .iter()
                .map(|server| (server.name.clone(), 0))
                .collect()
        });
        state.revision = state.revision.saturating_add(1).max(1);
        (state.generation, command)
    }

    async fn persist_budget_state(&self) {
        let Some(control) = &self.control else { return };
        let (revision, json) = {
            let state = self.budgets.lock().unwrap();
            (state.revision, serde_json::to_string(&*state))
        };
        let Ok(json) = json else { return };
        if let Err(error) = control
            .save_budget_state(&self.cfg.cluster_id, revision, &json)
            .await
        {
            tracing::warn!(%error, "provider budget state was not persisted");
        }
    }

    async fn restore_budget_state(&self) {
        let Some(control) = &self.control else { return };
        match control.load_budget_state(&self.cfg.cluster_id).await {
            Ok(Some(json)) => match serde_json::from_str(&json) {
                Ok(state) => *self.budgets.lock().unwrap() = state,
                Err(error) => {
                    tracing::error!(%error, "replicated provider budget state is invalid")
                }
            },
            Ok(None) => {}
            Err(error) => tracing::warn!(%error, "provider budget state could not be restored"),
        }
    }

    async fn apply_local_budgets(&self) {
        let by_id = self.servers.iter().map(|s| (s.id, 0u16)).collect();
        let _ = self.engine.set_server_budgets(by_id).await;
        self.persist_budget_state().await;
    }

    /// A holder that stopped acknowledging a shrink keeps its capacity
    /// reserved until all of its durable work leases have expired. Workers
    /// independently drain connections at that same bounded deadline, so the
    /// expired holder can then be removed without oversubscribing a provider.
    fn retire_expired_budget_holder(&self, node: &str) {
        if self
            .leases
            .lock()
            .unwrap()
            .values()
            .any(|lease| lease.node == node)
        {
            return;
        }
        let mut state = self.budgets.lock().unwrap();
        state.grants.remove(node);
        state.commands.remove(node);
        state.awaiting_shrink.remove(node);
        if state.pending_target.is_some() && state.awaiting_shrink.is_empty() {
            state.generation = state.generation.saturating_add(1);
            state.grants = state.pending_target.take().unwrap();
            state.commands = state.grants.clone();
        }
    }

    async fn grant_job(
        &self,
        node: &str,
        owner_incarnation: &str,
        kind: LeaseKind,
        mut job: nzbd_types::Job,
        scope: serde_json::Value,
    ) -> Option<Grant> {
        let control = self.control.as_ref()?;
        let (job_incarnation, job_revision) =
            control.job_identity(job.id.0.into()).await.ok().flatten()?;
        let kind_name = match kind {
            LeaseKind::Download => "download",
            LeaseKind::Post => "post",
            LeaseKind::Segment => "segment",
            LeaseKind::Assemble => "assemble",
        };
        let resource = match kind {
            LeaseKind::Segment => serde_json::from_value::<crate::range::RangeScope>(scope.clone())
                .map(|range| crate::range::resource(&range))
                .ok()?,
            LeaseKind::Assemble => {
                let assembly =
                    serde_json::from_value::<crate::range::AssembleScope>(scope.clone()).ok()?;
                crate::range::assembly_resource(
                    JobId(assembly.job_id),
                    nzbd_types::FileId(assembly.file_id),
                )
            }
            _ => format!("work/job/{}/{kind_name}", job.id.0),
        };
        let scope_json = serde_json::to_string(&scope).ok()?;
        let token = match control
            .acquire(
                &resource,
                &self.cfg.cluster_id,
                job.id.0.into(),
                &job_incarnation,
                node,
                owner_incarnation,
                kind_name,
                &scope_json,
                job_revision,
                self.cfg.worker_ttl,
            )
            .await
        {
            Ok(LeaseClaim::Acquired(token)) => token,
            Ok(LeaseClaim::Held { .. }) => return None,
            Err(error) => {
                tracing::warn!(job = job.id.0, %error, "replicated work lease acquire failed");
                return None;
            }
        };
        let lease_id = format!("{}@{}", token.resource, token.fence);
        if kind == LeaseKind::Segment {
            let range = serde_json::from_value::<crate::range::RangeScope>(scope.clone()).ok()?;
            job = crate::range::work_view(job, &range, token.fence).ok()?;
        }
        self.leases.lock().unwrap().insert(
            lease_id.clone(),
            LeaseInfo {
                job: job.id,
                node: node.to_owned(),
                kind,
                token: token.clone(),
                job_revision,
                control_revision: job_revision,
                last_hb: Instant::now(),
            },
        );
        let (budget_generation, server_budgets) = self.budget_command(node, None);
        self.persist_budget_state().await;
        Some(Grant {
            lease_id,
            token,
            job_incarnation,
            job_revision,
            control_revision: job_revision,
            scope,
            epoch: self.epoch(),
            kind,
            job,
            server_budgets,
            budget_generation,
            post_fetch_budgeted: true,
        })
    }

    /// Live nodes (seq progressed within 3 lease intervals), self excluded.
    fn live_workers(&self) -> Vec<NodeRecord> {
        let now = Instant::now();
        let ttl = self.cfg.lease_interval * 3;
        let mut seen = self.node_seen.lock().unwrap();
        let mut out = Vec::new();
        for rec in read_nodes(&self.layout) {
            if rec.name == self.cfg.node_name {
                continue;
            }
            let entry = seen.entry(rec.name.clone()).or_insert((rec.seq, now));
            if rec.seq != entry.0 {
                *entry = (rec.seq, now);
            }
            if now.duration_since(entry.1) <= ttl {
                out.push(rec);
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------
// HTTP endpoints (mounted on every node; answer only while leader)
// ---------------------------------------------------------------------------

pub fn router(shared: Arc<LeaderShared>) -> Router {
    Router::new()
        .route("/cluster/v1/leader", get(leader_info))
        .route("/cluster/v1/work/poll", post(work_poll))
        .route("/cluster/v1/work/heartbeat", post(work_heartbeat))
        .route("/cluster/v1/work/complete", post(work_complete))
        .route("/cluster/v1/work/reject", post(work_reject))
        .route("/cluster/v1/work/script-receipt", post(work_script_receipt))
        // Authenticate the peer namespace before extractors parse a request
        // body. Otherwise an unauthenticated malformed request can reveal
        // schema details through a 4xx extractor response.
        .route_layer(middleware::from_fn_with_state(
            shared.clone(),
            require_cluster_secret,
        ))
        .with_state(shared)
}

async fn require_cluster_secret(
    State(shared): State<Arc<LeaderShared>>,
    request: Request,
    next: Next,
) -> Response {
    if authed(&shared, request.headers()) {
        next.run(request).await
    } else {
        denied()
    }
}

async fn work_script_receipt(
    State(s): State<Arc<LeaderShared>>,
    headers: HeaderMap,
    Json(req): Json<ScriptReceiptRequest>,
) -> Response {
    if !authed(&s, &headers) {
        return denied();
    }
    if !s.is_leader() {
        return not_leader();
    }
    if !s.authority_ready() {
        return authority_not_ready();
    }
    let exact = s
        .leases
        .lock()
        .unwrap()
        .get(&req.lease_id)
        .is_some_and(|lease| lease.node == req.node && lease.token == req.token);
    if !exact || req.receipt_id.len() > 512 {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "stale script receipt lease"})),
        )
            .into_response();
    }
    let result = match &s.control {
        Some(control) if req.finish => control.finish_script(&req.receipt_id, &req.token).await,
        Some(control) => control.begin_script(&req.receipt_id, &req.token).await,
        None => Err("replicated control unavailable".into()),
    };
    let decision = match result {
        Ok(ScriptReceiptOutcome::Run) => "run",
        Ok(ScriptReceiptOutcome::AlreadyDone) => "done",
        Ok(ScriptReceiptOutcome::Ambiguous) => "ambiguous",
        Ok(ScriptReceiptOutcome::Conflict) => "conflict",
        Err(_) => "unknown",
    };
    Json(ScriptReceiptResponse {
        decision: decision.into(),
    })
    .into_response()
}

fn authed(shared: &LeaderShared, headers: &HeaderMap) -> bool {
    secret_matches(
        headers.get(SECRET_HEADER).and_then(|v| v.to_str().ok()),
        &shared.cfg.secret,
    )
}

fn worker_admits_new_work(worker: &NodeRecord) -> bool {
    worker.disk_guard_capable && !worker.disk_low
}

/// A heartbeat may carry a predecessor token when the prior leader committed
/// a renewal but died before its response reached the worker. Only the
/// durable token is renewed; ownership and the fencing generation must still
/// match exactly, and a worker can never move a token backwards or across a
/// takeover.
fn heartbeat_token_can_advance(provided: &LeaseToken, durable: &LeaseToken) -> bool {
    provided.resource == durable.resource
        && provided.owner_node_id == durable.owner_node_id
        && provided.owner_incarnation == durable.owner_incarnation
        && provided.fence == durable.fence
        && provided.revision <= durable.revision
        && provided.expires_at_unix_ms <= durable.expires_at_unix_ms
}

fn not_leader() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({"error": "not the leader"})),
    )
        .into_response()
}

fn authority_not_ready() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({
            "error": "queue authority is still being adopted; retry"
        })),
    )
        .into_response()
}

fn denied() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({"error": "bad cluster secret"})),
    )
        .into_response()
}

async fn leader_info(State(s): State<Arc<LeaderShared>>, headers: HeaderMap) -> Response {
    if !authed(&s, &headers) {
        return denied();
    }
    let v = s.view.borrow().clone();
    Json(serde_json::json!({
        "leader": v.record.as_ref().map(|r| &r.node),
        "api_url": v.record.as_ref().map(|r| &r.api_url),
        "epoch": v.epoch(),
        "is_me": v.is_me,
    }))
    .into_response()
}

async fn work_poll(
    State(s): State<Arc<LeaderShared>>,
    headers: HeaderMap,
    Json(req): Json<PollRequest>,
) -> Response {
    if !authed(&s, &headers) {
        return denied();
    }
    if !s.is_leader() {
        return not_leader();
    }
    if !s.authority_ready() {
        return authority_not_ready();
    }
    // The registry is the leader's independent admission fact. Do not trust
    // slot counts from a poll whose node is already known to be held.
    if read_nodes(&s.layout)
        .into_iter()
        .find(|node| node.name == req.node)
        .is_none_or(|node| !worker_admits_new_work(&node))
    {
        return Json(PollResponse::default()).into_response();
    }

    // Explicit range work is selected independently of whole-job
    // delegation. The authority keeps the complete job; each grant carries
    // only one bounded article view.
    let snap = s.engine.snapshot();
    let mut grants = Vec::new();
    if req.free_download_slots > 0 {
        let range_jobs: Vec<JobId> = snap
            .jobs
            .iter()
            .filter(|job| job.assigned_node.as_deref() == Some(crate::range::RANGE_ASSIGNEE))
            .map(|job| job.id)
            .collect();
        'jobs: for job_id in range_jobs {
            if s.leases
                .lock()
                .unwrap()
                .values()
                .any(|lease| lease.job == job_id && lease.node == req.node)
            {
                continue;
            }
            let Ok(Some(job)) = s.engine.export_job(job_id).await else {
                continue;
            };
            let scopes = crate::range::scopes(&job);
            if scopes.is_empty() {
                continue;
            }
            let accepted_rows = match &s.control {
                Some(control) => control
                    .accepted_ranges(job_id.0.into())
                    .await
                    .unwrap_or_default(),
                None => Vec::new(),
            };
            let accepted: HashMap<String, (String, String)> = accepted_rows
                .into_iter()
                .map(|(resource, scope, result_ref)| (resource, (scope, result_ref)))
                .collect();
            let active_resources: HashSet<String> = s
                .leases
                .lock()
                .unwrap()
                .values()
                .map(|lease| lease.token.resource.clone())
                .collect();
            for scope in &scopes {
                let resource = crate::range::resource(scope);
                if accepted.contains_key(&resource) || active_resources.contains(&resource) {
                    continue;
                }
                if range_owner(&s.live_workers(), scope.range_index) != Some(req.node.as_str()) {
                    continue;
                }
                if let Some(grant) = s
                    .grant_job(
                        &req.node,
                        &req.owner_incarnation,
                        LeaseKind::Segment,
                        job.clone(),
                        serde_json::to_value(scope).unwrap(),
                    )
                    .await
                {
                    tracing::info!(job = job_id.0, file = scope.file_id, first = scope.first_article, last = scope.last_article, node = %req.node, "article range granted");
                    grants.push(grant);
                    break 'jobs;
                }
            }
            if accepted.len() == scopes.len() {
                let assembly_file_id = scopes[0].file_id;
                let assembly_resource =
                    crate::range::assembly_resource(job_id, nzbd_types::FileId(assembly_file_id));
                if !active_resources.contains(&assembly_resource)
                    && range_owner(&s.live_workers(), scopes.len() as u32)
                        == Some(req.node.as_str())
                {
                    let mut ranges = Vec::new();
                    for scope in scopes {
                        let resource = crate::range::resource(&scope);
                        let Some((_, result_ref)) = accepted.get(&resource) else {
                            continue 'jobs;
                        };
                        ranges.push(crate::range::AcceptedRange {
                            scope,
                            result_ref: result_ref.clone(),
                        });
                    }
                    let scope = crate::range::AssembleScope {
                        job_id: job_id.0,
                        file_id: assembly_file_id,
                        ranges,
                    };
                    if let Some(grant) = s
                        .grant_job(
                            &req.node,
                            &req.owner_incarnation,
                            LeaseKind::Assemble,
                            job,
                            serde_json::to_value(scope).unwrap(),
                        )
                        .await
                    {
                        tracing::info!(job = job_id.0, node = %req.node, "assembly lease granted");
                        grants.push(grant);
                        break 'jobs;
                    }
                }
            }
        }
    }

    // Jobs delegated to this node without an active lease → grants.
    let assigned: Vec<JobId> = snap
        .jobs
        .iter()
        .filter(|j| j.assigned_node.as_deref() == Some(req.node.as_str()))
        .filter(|j| {
            !matches!(
                j.status,
                JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
            )
        })
        .map(|j| j.id)
        .collect();

    let leased_jobs: HashSet<JobId> = s.leases.lock().unwrap().values().map(|l| l.job).collect();

    for job_id in assigned {
        if grants.len() as u32 >= req.free_download_slots {
            break;
        }
        if leased_jobs.contains(&job_id) {
            continue;
        }
        let Ok(Some(job)) = s.engine.export_job(job_id).await else {
            continue;
        };
        if let Some(grant) = s
            .grant_job(
                &req.node,
                &req.owner_incarnation,
                LeaseKind::Download,
                job,
                serde_json::json!({"whole_job": true}),
            )
            .await
        {
            tracing::info!(job = job_id.0, node = %req.node, lease = %grant.lease_id, "download lease granted");
            grants.push(grant);
        }
    }

    // PP grants (C2): completed jobs the scheduler assigned to this node
    // for post-processing, not yet leased, PP not yet done.
    let mut pp_granted = 0u32;
    let pp_candidates: Vec<JobId> = snap
        .jobs
        .iter()
        .filter(|j| {
            matches!(j.status, JobStatus::Completed)
                && !j.pp_done
                && j.assigned_node.as_deref() == Some(req.node.as_str())
        })
        .map(|j| j.id)
        .collect();
    for job_id in pp_candidates {
        if pp_granted >= req.free_pp_slots {
            break;
        }
        let already = s.leases.lock().unwrap().values().any(|l| l.job == job_id);
        if already {
            continue;
        }
        let Ok(Some(job)) = s.engine.export_job(job_id).await else {
            continue;
        };
        if let Some(grant) = s
            .grant_job(
                &req.node,
                &req.owner_incarnation,
                LeaseKind::Post,
                job,
                serde_json::json!({"whole_job": true}),
            )
            .await
        {
            tracing::info!(job = job_id.0, node = %req.node, lease = %grant.lease_id, "pp lease granted");
            pp_granted += 1;
            grants.push(grant);
        }
    }

    if !grants.is_empty() {
        s.apply_local_budgets().await;
    }
    Json(PollResponse { grants }).into_response()
}

fn range_owner(workers: &[NodeRecord], index: u32) -> Option<&str> {
    let mut eligible: Vec<_> = workers
        .iter()
        .filter(|worker| {
            worker.download && worker.max_download_jobs > 0 && worker_admits_new_work(worker)
        })
        .collect();
    eligible.sort_by(|left, right| left.name.cmp(&right.name));
    let total: u32 = eligible
        .iter()
        .map(|worker| worker.download_weight.max(1))
        .sum();
    let mut slot = index % total.max(1);
    for worker in eligible {
        let weight = worker.download_weight.max(1);
        if slot < weight {
            return Some(worker.name.as_str());
        }
        slot -= weight;
    }
    None
}

async fn work_reject(
    State(s): State<Arc<LeaderShared>>,
    headers: HeaderMap,
    Json(req): Json<RejectRequest>,
) -> Response {
    if !authed(&s, &headers) {
        return denied();
    }
    if !s.is_leader() {
        return not_leader();
    }
    if !s.authority_ready() {
        return authority_not_ready();
    }
    let candidate = s.leases.lock().unwrap().get(&req.lease_id).cloned();
    let release_ok = if let Some(lease) = &candidate {
        if lease.node != req.node || lease.token != req.token {
            false
        } else if let Some(control) = &s.control {
            control.release(&req.token).await.unwrap_or(false)
        } else {
            true
        }
    } else {
        false
    };
    let released = if release_ok {
        s.leases.lock().unwrap().remove(&req.lease_id)
    } else {
        None
    };
    if let Some(lease) = released {
        tracing::info!(
            job = lease.job.0,
            node = %req.node,
            lease = %req.lease_id,
            "worker rejected grant after its local admission state changed"
        );
        let _ = s.engine.set_delegated(lease.job, None).await;
        s.apply_local_budgets().await;
        Json(RejectResponse { released: true }).into_response()
    } else {
        Json(RejectResponse { released: false }).into_response()
    }
}

async fn work_heartbeat(
    State(s): State<Arc<LeaderShared>>,
    headers: HeaderMap,
    Json(req): Json<HeartbeatRequest>,
) -> Response {
    if !authed(&s, &headers) {
        return denied();
    }
    if !s.is_leader() {
        return not_leader();
    }
    if !s.authority_ready() {
        return authority_not_ready();
    }

    let (budget_generation, server_budgets) = s.budget_command(&req.node, req.budget_ack.as_ref());
    s.persist_budget_state().await;
    let mut cancel = Vec::new();
    let mut renewed = Vec::new();
    let mut accepted_progress = HashMap::new();
    let mut controls = HashMap::new();
    let snap = s.engine.snapshot();
    for lp in &req.leases {
        let candidate = s.leases.lock().unwrap().get(&lp.lease_id).cloned();
        let info = if let Some(info) = candidate {
            info
        } else {
            // A new leader may rebuild this process-local projection only from
            // the exact live replicated token, never from the worker's claim.
            let stored = match &s.control {
                Some(control) => control
                    .current_lease_record(&lp.token.resource)
                    .await
                    .ok()
                    .flatten(),
                None => None,
            };
            let Some(stored) = stored else {
                cancel.push(lp.lease_id.clone());
                continue;
            };
            let stored_kind = match stored.kind.as_str() {
                "download" => LeaseKind::Download,
                "post" => LeaseKind::Post,
                "segment" => LeaseKind::Segment,
                "assemble" => LeaseKind::Assemble,
                _ => {
                    cancel.push(lp.lease_id.clone());
                    continue;
                }
            };
            if !heartbeat_token_can_advance(&lp.token, &stored.token)
                || stored.owner_node_id != req.node
                || stored.job_id != u64::from(lp.job.0)
                || stored_kind != lp.kind
                || stored.job_revision != lp.job_revision
                || format!("{}@{}", stored.token.resource, stored.token.fence) != lp.lease_id
            {
                cancel.push(lp.lease_id.clone());
                continue;
            }
            let restored = LeaseInfo {
                job: JobId(u32::try_from(stored.job_id).unwrap_or(u32::MAX)),
                node: stored.owner_node_id,
                kind: stored_kind,
                token: stored.token,
                job_revision: stored.job_revision,
                control_revision: stored.job_revision,
                last_hb: Instant::now(),
            };
            s.leases
                .lock()
                .unwrap()
                .insert(lp.lease_id.clone(), restored.clone());
            restored
        };
        if info.node != req.node
            || info.job != lp.job
            || !heartbeat_token_can_advance(&lp.token, &info.token)
            || !snap.jobs.iter().any(|job| job.id == lp.job)
        {
            cancel.push(lp.lease_id.clone());
            continue;
        }
        let authoritative = s.engine.export_job(lp.job).await.ok().flatten();
        let control_identity = match &s.control {
            Some(control) => control.job_identity(lp.job.0.into()).await.ok().flatten(),
            None => None,
        };
        let runnable = authoritative
            .as_ref()
            .is_some_and(|job| !matches!(job.status, JobStatus::Paused | JobStatus::Deleted));
        if !runnable
            || control_identity
                .as_ref()
                .is_none_or(|(_, revision)| *revision != lp.job_revision)
        {
            cancel.push(lp.lease_id.clone());
            continue;
        }
        let next = match &s.control {
            Some(control) => control.renew(&info.token, s.cfg.worker_ttl).await,
            None => Ok(None),
        };
        match next {
            Ok(Some(next)) => {
                let mut leases = s.leases.lock().unwrap();
                if let Some(current) = leases.get_mut(&lp.lease_id) {
                    if current.token == info.token {
                        current.token = next.clone();
                        current.last_hb = Instant::now();
                        controls.insert(lp.lease_id.clone(), current.control_revision);
                        accepted_progress.insert(lp.lease_id.clone(), next.clone());
                        renewed.push(next);
                    }
                }
            }
            Ok(None) | Err(_) => cancel.push(lp.lease_id.clone()),
        }
    }
    for lp in &req.leases {
        let leases = s.leases.lock().unwrap();
        if !cancel.contains(&lp.lease_id)
            && leases.get(&lp.lease_id).is_some_and(|lease| {
                lease.node == req.node
                    && lease.job == lp.job
                    && accepted_progress.get(&lp.lease_id) == Some(&lease.token)
            })
        {
            let mut stats = lp.stats.clone();
            if stats.repair_progress.as_ref().is_some_and(|progress| {
                !progress
                    .attempt_id
                    .starts_with(&format!("{}:", lp.lease_id))
            }) {
                stats.repair_progress = None;
            }
            s.engine
                .mirror_progress_from(lp.job, req.node.clone(), stats);
        } else if !cancel.contains(&lp.lease_id) {
            cancel.push(lp.lease_id.clone());
        }
    }
    Json(HeartbeatResponse {
        cancel,
        renewed,
        controls,
        server_budgets: Some(server_budgets),
        budget_generation,
        post_fetch_budgeted: true,
    })
    .into_response()
}

async fn work_complete(
    State(s): State<Arc<LeaderShared>>,
    headers: HeaderMap,
    Json(req): Json<CompleteRequest>,
) -> Response {
    if !authed(&s, &headers) {
        return denied();
    }
    if !s.is_leader() {
        return not_leader();
    }
    if !s.authority_ready() {
        return authority_not_ready();
    }
    let _serial = s.mutation_serial.lock().await;
    let job_id = req.job.id;
    let durable_receipt = match &s.control {
        Some(control) => control
            .publication_record(&req.receipt_id)
            .await
            .ok()
            .flatten(),
        None => None,
    };
    let candidate = if let Some(receipt) = durable_receipt.as_ref() {
        let kind = match receipt.kind.as_str() {
            "download" => LeaseKind::Download,
            "post" => LeaseKind::Post,
            "segment" => LeaseKind::Segment,
            "assemble" => LeaseKind::Assemble,
            _ => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({"error": "invalid durable receipt kind"})),
                )
                    .into_response()
            }
        };
        if !publication_retry_matches(receipt, &req, kind) {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"error": "receipt id reused with different completion"})),
            )
                .into_response();
        }
        Some(LeaseInfo {
            job: job_id,
            node: req.node.clone(),
            kind,
            token: req.token.clone(),
            job_revision: req.expected_job_revision,
            control_revision: req.expected_job_revision,
            last_hb: Instant::now(),
        })
    } else {
        s.leases.lock().unwrap().get(&req.lease_id).cloned()
    };
    let Some(info) = candidate else {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "unknown lease"})),
        )
            .into_response();
    };
    if info.node != req.node
        || info.job != job_id
        || info.token != req.token
        || info.job_revision != req.expected_job_revision
    {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "stale lease token or job revision"})),
        )
            .into_response();
    }
    if info.kind == LeaseKind::Segment
        && !segment_completion_is_exact(&req.job, &info.token.resource)
    {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({"error": "article range is not completely done"})),
        )
            .into_response();
    }
    let verify_root = s.cfg.shared_dir.clone();
    let verify_ref = req.result_ref.clone();
    let verify_id = req.result_id.clone();
    let publish_result_id = req.result_id.clone();
    let verify_incarnation = info.token.owner_incarnation.clone();
    let verify_job_incarnation = match &s.control {
        Some(control) => control
            .job_identity(job_id.0.into())
            .await
            .ok()
            .flatten()
            .map(|v| v.0),
        None => None,
    };
    let verify_job = req.job.clone();
    let verify_fence = req.token.fence;
    let verified = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        tokio::task::spawn_blocking(move || {
            verify_generation(
                &verify_root,
                &verify_ref,
                &verify_id,
                verify_job_incarnation.as_deref(),
                verify_fence,
                &verify_job,
            )
        }),
    )
    .await;
    if !matches!(verified, Ok(Ok(Ok(())))) {
        tracing::warn!(job = job_id.0, owner_incarnation = %verify_incarnation, "completion generation validation failed or timed out");
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({"error": "invalid completion generation"})),
        )
            .into_response();
    }
    let outcome = match &s.control {
        Some(control) => {
            let result_job_json =
                match durable_result_job_json(&req.job, info.kind, &req.result_ref) {
                    Ok(value) => value,
                    Err(error) => {
                        return (
                        StatusCode::UNPROCESSABLE_ENTITY,
                        Json(serde_json::json!({"error": format!("invalid result job: {error}")})),
                    )
                        .into_response();
                    }
                };
            control
                .publish_result(
                    &req.receipt_id,
                    &req.token,
                    req.expected_job_revision,
                    &req.result_id,
                    &req.result_ref,
                    &result_job_json,
                )
                .await
        }
        None => Err("replicated control unavailable".into()),
    };
    match outcome {
        Ok(MutationOutcome::Applied { .. } | MutationOutcome::Duplicate { .. }) => {}
        Ok(MutationOutcome::Conflict) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"error": "publication fence rejected"})),
            )
                .into_response();
        }
        Err(error) => {
            tracing::warn!(job = job_id.0, %error, "durable completion outcome unknown");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "completion outcome unknown"})),
            )
                .into_response();
        }
    }
    let accepted_at_ms = match durable_receipt
        .as_ref()
        .map(|receipt| receipt.accepted_at_ms)
    {
        Some(value) => value,
        None => match &s.control {
            Some(control) => control
                .publication_accepted_at(&req.receipt_id)
                .await
                .ok()
                .flatten()
                .unwrap_or_default(),
            None => 0,
        },
    };
    if accepted_at_ms <= 0 {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "durable completion timestamp unavailable"})),
        )
            .into_response();
    }
    if info.kind == LeaseKind::Segment {
        tracing::info!(job = job_id.0, node = %req.node, resource = %req.token.resource, "article range committed");
        s.leases.lock().unwrap().remove(&req.lease_id);
        s.apply_local_budgets().await;
        return Json(CompleteResponse {
            final_dir: None,
            ok: true,
            durable_receipt: Some(req.receipt_id),
            accepted_at_unix_ms: Some(accepted_at_ms),
            history_recorded_by_authority: false,
        })
        .into_response();
    }
    let mut final_publication = None;
    let mut published_job = req.job.clone();
    published_job
        .params
        .retain(|(key, _)| key != "*Cluster:result-ref");
    published_job
        .params
        .push(("*Cluster:result-ref".into(), req.result_ref.clone()));
    if matches!(info.kind, LeaseKind::Post | LeaseKind::Assemble) {
        let dest_dir = s.publication_root(&published_job, info.kind == LeaseKind::Post);
        let result_ref = req.result_ref.clone();
        let fence = req.token.fence;
        let original_dir = published_job
            .params
            .iter()
            .find(|(key, _)| key == "*Cluster:original-dir")
            .map(|(_, value)| value.clone());
        let Some(original_dir) = original_dir else {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({"error": "published generation lacks original directory"})),
            )
                .into_response();
        };
        let final_path = dest_dir.join(&original_dir);
        let publish_dir = original_dir.clone();
        let publish = tokio::task::spawn_blocking(move || {
            publish_generation(
                &dest_dir,
                &result_ref,
                &publish_dir,
                fence,
                &publish_result_id,
            )
        })
        .await;
        if !matches!(publish, Ok(Ok(()))) {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "generation selected but final directory publication is incomplete"})),
            )
                .into_response();
        }
        published_job.dir_name = original_dir;
        published_job
            .params
            .retain(|(k, _)| k != "*Cluster:final-dir");
        published_job.params.push((
            "*Cluster:final-dir".into(),
            final_path.to_string_lossy().into_owned(),
        ));
        final_publication = Some(final_path);
    }
    let history_recorded_by_authority = info.kind == LeaseKind::Post && s.history.is_some();
    if info.kind == LeaseKind::Post {
        if let Err(error) = record_pp_history(
            &s,
            &published_job,
            accepted_at_ms,
            final_publication.as_deref().expect("post publication path"),
        )
        .await
        {
            tracing::warn!(job = job_id.0, %error, "published PP result awaits durable history");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "published result awaits durable history"})),
            )
                .into_response();
        }
    }
    tracing::info!(job = job_id.0, node = %req.node, "job completed remotely");
    if let Err(error) = s.engine.import_job(published_job, false, true).await {
        tracing::error!(job = job_id.0, %error, "durable result accepted but local projection failed");
    }
    if info.kind == LeaseKind::Assemble {
        let _ = s.engine.set_delegated(job_id, None).await;
    }
    s.leases.lock().unwrap().remove(&req.lease_id);
    s.apply_local_budgets().await;
    Json(CompleteResponse {
        final_dir: final_publication.map(|p| p.to_string_lossy().into_owned()),
        ok: true,
        durable_receipt: Some(req.receipt_id),
        accepted_at_unix_ms: Some(accepted_at_ms),
        history_recorded_by_authority,
    })
    .into_response()
}

fn durable_result_job_json(
    job: &nzbd_types::Job,
    kind: LeaseKind,
    result_ref: &str,
) -> Result<String, serde_json::Error> {
    let mut durable = job.clone();
    if matches!(kind, LeaseKind::Post | LeaseKind::Assemble) {
        if let Some((_, original)) = durable
            .params
            .iter()
            .find(|(key, _)| key == "*Cluster:original-dir")
        {
            durable.dir_name = original.clone();
        }
    }
    durable
        .params
        .retain(|(key, _)| key != "*Cluster:result-ref");
    durable
        .params
        .push(("*Cluster:result-ref".into(), result_ref.into()));
    serde_json::to_string(&durable)
}

fn publication_retry_matches(
    receipt: &crate::control::PublicationReceipt,
    request: &CompleteRequest,
    kind: LeaseKind,
) -> bool {
    let result_job_json =
        durable_result_job_json(&request.job, kind, &request.result_ref).unwrap_or_default();
    receipt.resource == request.token.resource
        && receipt.owner_node_id == request.token.owner_node_id
        && receipt.owner_incarnation == request.token.owner_incarnation
        && receipt.fence == request.token.fence
        && receipt.lease_revision == request.token.revision
        && receipt.lease_expiry_ms == request.token.expires_at_unix_ms
        && receipt.expected_job_revision == request.expected_job_revision
        && receipt.result_id == request.result_id
        && receipt.result_ref == request.result_ref
        && receipt.result_job_json == result_job_json
}

fn segment_completion_is_exact(job: &nzbd_types::Job, resource: &str) -> bool {
    if job.status != JobStatus::Completed || job.files.len() != 1 {
        return false;
    }
    let file = &job.files[0];
    let (Some(first), Some(last)) = (file.segments.first(), file.segments.last()) else {
        return false;
    };
    let scope = crate::range::RangeScope {
        job_id: job.id.0,
        file_id: file.id.0,
        first_article: first.number,
        last_article: last.number,
        range_index: 0,
        range_count: 0,
    };
    crate::range::resource(&scope) == resource
        && file
            .segments
            .iter()
            .all(|segment| matches!(segment.state, nzbd_types::SegmentState::Done { .. }))
}

#[derive(serde::Deserialize)]
struct VerifyGenerationManifest {
    job_incarnation: String,
    fence: u64,
    result_id: String,
    job_sha256: String,
    files: Vec<VerifyGenerationFile>,
    total_bytes: u64,
}

#[derive(serde::Deserialize)]
struct VerifyGenerationFile {
    path: String,
    bytes: u64,
    sha256: String,
}

fn verify_generation(
    shared_dir: &Path,
    result_ref: &str,
    result_id: &str,
    job_incarnation: Option<&str>,
    fence: u64,
    expected_job: &nzbd_types::Job,
) -> Result<(), String> {
    let allowed = std::fs::canonicalize(shared_dir.join(".nzbd-cluster/generations"))
        .map_err(|error| format!("canonicalize generation root: {error}"))?;
    let selected = std::fs::canonicalize(result_ref)
        .map_err(|error| format!("canonicalize generation: {error}"))?;
    if !selected.starts_with(&allowed) {
        return Err("generation escaped shared namespace".into());
    }
    let manifest: VerifyGenerationManifest = serde_json::from_slice(
        &std::fs::read(selected.join("manifest.json"))
            .map_err(|error| format!("read generation manifest: {error}"))?,
    )
    .map_err(|error| format!("decode generation manifest: {error}"))?;
    if manifest.result_id != result_id
        || manifest.fence != fence
        || job_incarnation.is_some_and(|expected| expected != manifest.job_incarnation)
    {
        return Err("generation identity mismatch".into());
    }
    let job_bytes = std::fs::read(selected.join("job.json"))
        .map_err(|error| format!("read generation job: {error}"))?;
    use sha2::Digest;
    if format!("{:x}", sha2::Sha256::digest(&job_bytes)) != manifest.job_sha256 {
        return Err("generation job hash mismatch".into());
    }
    let sealed_job: serde_json::Value = serde_json::from_slice(&job_bytes)
        .map_err(|error| format!("decode generation job: {error}"))?;
    let expected_job = serde_json::to_value(expected_job)
        .map_err(|error| format!("encode expected generation job: {error}"))?;
    if sealed_job != expected_job {
        return Err("generation job does not match completion request".into());
    }
    let mut total = 0u64;
    for expected in manifest.files {
        let relative = PathBuf::from(&expected.path);
        if relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            return Err("generation manifest contains unsafe path".into());
        }
        let files_root = selected.join("files");
        let path = files_root.join(relative);
        if std::fs::symlink_metadata(&path)
            .map_err(|error| format!("stat generation payload: {error}"))?
            .file_type()
            .is_symlink()
        {
            return Err("generation payload is a symlink".into());
        }
        let canonical = std::fs::canonicalize(&path)
            .map_err(|error| format!("canonicalize generation payload: {error}"))?;
        if !canonical.starts_with(&files_root) {
            return Err("generation payload escaped files directory".into());
        }
        let mut file = std::fs::File::open(&canonical)
            .map_err(|error| format!("open generation payload: {error}"))?;
        if file.metadata().map_err(|error| error.to_string())?.len() != expected.bytes {
            return Err("generation payload length mismatch".into());
        }
        let mut hash = sha2::Sha256::new();
        let mut copied = 0u64;
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let read = file
                .read(&mut buffer)
                .map_err(|error| format!("hash generation payload: {error}"))?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
            copied += read as u64;
        }
        total = total
            .checked_add(copied)
            .ok_or_else(|| "generation size overflow".to_owned())?;
        if format!("{:x}", hash.finalize()) != expected.sha256 {
            return Err("generation payload hash mismatch".into());
        }
    }
    if total != manifest.total_bytes {
        return Err("generation total byte count mismatch".into());
    }
    Ok(())
}

/// Materialize a selected immutable generation. Existing output is moved to
/// a uniquely named recoverable sibling and is never deleted here. A marker
/// makes retries idempotent without repeatedly moving the selected output.
fn publish_generation(
    dest_dir: &Path,
    result_ref: &str,
    original_dir: &str,
    fence: u64,
    result_id: &str,
) -> Result<(), String> {
    let relative = PathBuf::from(original_dir);
    if relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err("original directory is not a safe relative path".into());
    }
    let source = PathBuf::from(result_ref).join("files");
    let target = dest_dir.join(&relative);
    let marker = target.join(".nzbd-generation");
    if std::fs::read_to_string(&marker)
        .ok()
        .is_some_and(|selected| selected == result_id)
    {
        return Ok(());
    }
    let parent = target
        .parent()
        .ok_or_else(|| "final directory has no parent".to_owned())?;
    std::fs::create_dir_all(parent).map_err(|error| format!("create final parent: {error}"))?;
    let leaf = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "final directory has no safe file name".to_owned())?;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let building = parent.join(format!(".{leaf}.publish-{fence}-{nonce}"));
    let backup = parent.join(format!(".{leaf}.superseded-{fence}-{nonce}"));
    std::fs::create_dir(&building)
        .map_err(|error| format!("create publication directory: {error}"))?;
    copy_publication_tree(&source, &building)?;
    std::fs::write(building.join(".nzbd-generation"), result_id)
        .map_err(|error| format!("write generation marker: {error}"))?;
    std::fs::File::open(&building)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("flush publication directory: {error}"))?;
    if target.exists() {
        std::fs::rename(&target, &backup)
            .map_err(|error| format!("preserve prior final directory: {error}"))?;
    }
    if let Err(error) = std::fs::rename(&building, &target) {
        if backup.exists() && !target.exists() {
            let _ = std::fs::rename(&backup, &target);
        }
        return Err(format!("select final generation: {error}"));
    }
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("flush final publication parent: {error}"))?;
    Ok(())
}

fn copy_publication_tree(source: &Path, target: &Path) -> Result<(), String> {
    for entry in
        std::fs::read_dir(source).map_err(|error| format!("read selected generation: {error}"))?
    {
        let entry = entry.map_err(|error| format!("read selected entry: {error}"))?;
        let metadata = entry
            .file_type()
            .map_err(|error| format!("inspect selected entry: {error}"))?;
        if metadata.is_symlink() {
            return Err("selected generation contains a symlink".into());
        }
        let destination = target.join(entry.file_name());
        if metadata.is_dir() {
            std::fs::create_dir(&destination)
                .map_err(|error| format!("create selected directory: {error}"))?;
            copy_publication_tree(&entry.path(), &destination)?;
        } else if metadata.is_file() {
            std::fs::copy(entry.path(), &destination)
                .map_err(|error| format!("copy selected file: {error}"))?;
            std::fs::File::open(&destination)
                .and_then(|file| file.sync_all())
                .map_err(|error| format!("flush selected file: {error}"))?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Sweeper + scheduler task
// ---------------------------------------------------------------------------

pub fn spawn_leader_task(
    shared: Arc<LeaderShared>,
    cancel: CancellationToken,
    tracker: &TaskTracker,
) {
    tracker.spawn(async move {
        let mut was_leader = false;
        let mut authority_ready = false;
        loop {
            if cancel.is_cancelled() {
                break;
            }
            let is_leader = shared.is_leader();
            if is_leader && !was_leader {
                // Taking office: discard leases inherited from the old view.
                // New leases arrive via worker heartbeats or fresh grants.
                shared.authority_ready.store(false, Ordering::Release);
                shared.leases.lock().unwrap().clear();
                shared.restore_budget_state().await;
            }
            if is_leader && !authority_ready {
                // Retry a refused adoption while this node remains leader.
                // The engine leaves both local and shared state unchanged on
                // refusal, so an operator can repair the snapshot in place
                // without restarting the daemon or forcing an election flap.
                match shared.engine.adopt_authority().await {
                    Ok(()) => {
                        let replicated = match &shared.control {
                            Some(control) => control.load_jobs().await,
                            None => Err("replicated control unavailable".into()),
                        };
                        match replicated {
                            Ok(rows) => {
                                let jobs = rows.into_iter().map(|(job, _, _)| job).collect();
                                match shared.engine.adopt_replicated_authority(jobs).await {
                                    Ok(()) => {
                                        if let Err(error) = shared.engine.resume_url_fetches().await {
                                            tracing::warn!(%error, "durable URL fetches could not resume");
                                        }
                                        match recover_selected_publications(&shared).await {
                                            Ok(()) => {
                                                authority_ready = true;
                                                shared
                                                    .authority_ready
                                                    .store(true, Ordering::Release);
                                                tracing::info!(epoch = shared.epoch(), "leader task active");
                                            }
                                            Err(error) => {
                                                authority_ready = false;
                                                tracing::error!(%error, "selected generation recovery is incomplete");
                                            }
                                        }
                                    }
                                    Err(error) => {
                                        authority_ready = false;
                                        tracing::error!(%error, "replicated queue projection failed");
                                    }
                                }
                            }
                            Err(error) => {
                                authority_ready = false;
                                tracing::error!(%error, "replicated queue authority unavailable");
                            }
                        }
                    }
                    Err(error) => {
                        authority_ready = false;
                        tracing::error!(
                            epoch = shared.epoch(),
                            error = %error,
                            "leader scheduling disabled because queue authority adoption failed"
                        );
                    }
                }
            } else if !is_leader {
                authority_ready = false;
                shared.authority_ready.store(false, Ordering::Release);
            }
            was_leader = is_leader;

            if is_leader && authority_ready {
                sweep_expired(&shared).await;
                schedule(&shared).await;
            }

            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(shared.cfg.lease_interval) => {}
            }
        }
    });
}

async fn recover_selected_publications(s: &LeaderShared) -> Result<(), String> {
    let Some(control) = &s.control else {
        return Ok(());
    };
    for (result_id, result_ref, job, fence, accepted_at_ms, kind) in
        control.published_results().await?
    {
        let Some((_, original_dir)) = job
            .params
            .iter()
            .find(|(key, _)| key == "*Cluster:original-dir")
        else {
            continue;
        };
        let dest_dir = s.publication_root(&job, kind == "post");
        let original_dir = original_dir.clone();
        let final_path = dest_dir.join(&original_dir);
        tokio::task::spawn_blocking(move || {
            publish_generation(&dest_dir, &result_ref, &original_dir, fence, &result_id)
        })
        .await
        .map_err(|error| format!("publication recovery task failed: {error}"))??;
        if kind == "post" {
            record_pp_history(s, &job, accepted_at_ms, &final_path).await?;
        }
    }
    Ok(())
}

async fn record_pp_history(
    s: &LeaderShared,
    job: &nzbd_types::Job,
    accepted_at_ms: i64,
    final_path: &Path,
) -> Result<(), String> {
    let Some(history) = &s.history else {
        return Ok(());
    };
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
        final_dir: Some(final_path.to_string_lossy().into_owned()),
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
    let history = history.clone();
    tokio::task::spawn_blocking(move || history.record_seq(&entry))
        .await
        .map_err(|error| format!("history recovery task failed: {error}"))?
        .map_err(|error| error.to_string())?;
    Ok(())
}

async fn sweep_expired(s: &Arc<LeaderShared>) {
    let ttl = s.cfg.worker_ttl;
    let expired: Vec<(String, LeaseInfo)> = {
        let mut leases = s.leases.lock().unwrap();
        let now = Instant::now();
        let dead: Vec<String> = leases
            .iter()
            .filter(|(_, l)| now.duration_since(l.last_hb) > ttl)
            .map(|(id, _)| id.clone())
            .collect();
        dead.into_iter()
            .filter_map(|id| leases.remove(&id).map(|l| (id, l)))
            .collect()
    };
    for (lease_id, info) in expired {
        tracing::warn!(job = info.job.0, node = %info.node, %lease_id, "lease expired; reclaiming");
        // Fold whatever the worker journaled, release the delegation; the
        // job re-enters scheduling (locally or re-delegated).
        let _ = s.engine.fold_job_journals(info.job).await;
        let _ = s.engine.set_delegated(info.job, None).await;
        s.retire_expired_budget_holder(&info.node);
    }
    s.apply_local_budgets().await;
}

async fn schedule(s: &Arc<LeaderShared>) {
    let _serial = s.mutation_serial.lock().await;
    let durable_status: HashMap<JobId, JobStatus> = match &s.control {
        Some(control) => match control.load_jobs().await {
            Ok(rows) => rows
                .into_iter()
                .map(|(job, _, _)| (job.id, job.status))
                .collect(),
            Err(error) => {
                tracing::warn!(%error, "scheduler paused: replicated queue unavailable");
                return;
            }
        },
        None => return,
    };
    let workers = s.live_workers();
    let snap = s.engine.snapshot();
    let mut split_jobs = HashSet::new();
    if workers
        .iter()
        .filter(|worker| {
            worker.download && worker.max_download_jobs > 0 && worker_admits_new_work(worker)
        })
        .count()
        >= 2
    {
        for summary in snap.jobs.iter().filter(|job| {
            job.assigned_node.is_none()
                && matches!(job.status, JobStatus::Queued)
                && durable_status.get(&job.id) == Some(&JobStatus::Queued)
        }) {
            if let Ok(Some(job)) = s.engine.export_job(summary.id).await {
                if crate::range::is_split_candidate(&job) {
                    split_jobs.insert(summary.id);
                    let _ = s
                        .engine
                        .set_delegated(summary.id, Some(crate::range::RANGE_ASSIGNEE.into()))
                        .await;
                    tracing::info!(
                        job = summary.id.0,
                        "reserved job for article-range distribution"
                    );
                }
            }
        }
    }

    // Retire: post-processed terminal jobs move out of the queue — their
    // record of existence is the history store (NZBGet parity). Applies to
    // jobs PP'd remotely (imported stamped via work/complete) and locally.
    for j in snap.jobs.iter() {
        if j.pp_done
            && matches!(j.status, JobStatus::Completed | JobStatus::Failed)
            && durable_status.get(&j.id) == Some(&j.status)
        {
            let committed = match &s.control {
                Some(control) => control
                    .commit_job_delta(&[(u64::from(j.id.0), None)])
                    .await
                    .is_ok(),
                None => false,
            };
            if committed {
                tracing::info!(job = j.id.0, "retiring finished job to history");
                let _ = s.engine.remove_job_silent(j.id).await;
            } else {
                tracing::warn!(
                    job = j.id.0,
                    "finished job retirement awaits replicated control"
                );
            }
        }
    }

    // Reconcile: a job assigned to a node that is no longer live and holds
    // no lease for it was delegated into the void (node died between
    // assignment and poll, or vanished entirely). Release it.
    {
        let live: HashSet<&str> = workers.iter().map(|w| w.name.as_str()).collect();
        let disk_held: HashSet<&str> = workers
            .iter()
            .filter(|worker| !worker_admits_new_work(worker))
            .map(|worker| worker.name.as_str())
            .collect();
        let leased: HashSet<JobId> = s.leases.lock().unwrap().values().map(|l| l.job).collect();
        for j in snap.jobs.iter() {
            if let Some(node) = j.assigned_node.as_deref() {
                if node != s.cfg.node_name
                    && node != crate::range::RANGE_ASSIGNEE
                    && (!live.contains(node) || disk_held.contains(node))
                    && !leased.contains(&j.id)
                    && !matches!(j.status, JobStatus::Deleted)
                {
                    tracing::warn!(
                        job = j.id.0,
                        %node,
                        disk_low = disk_held.contains(node),
                        "assigned node is unavailable; releasing delegation"
                    );
                    let _ = s.engine.set_delegated(j.id, None).await;
                }
            }
        }
    }

    let (leases_by_node, pp_leases_by_node): (HashMap<String, u32>, HashMap<String, u32>) = {
        let leases = s.leases.lock().unwrap();
        let mut dl = HashMap::new();
        let mut pp = HashMap::new();
        for l in leases.values() {
            match l.kind {
                LeaseKind::Download | LeaseKind::Segment | LeaseKind::Assemble => {
                    *dl.entry(l.node.clone()).or_insert(0) += 1
                }
                LeaseKind::Post => *pp.entry(l.node.clone()).or_insert(0) += 1,
            }
        }
        (dl, pp)
    };

    // Assigned-but-not-polled jobs count as load. Otherwise a slow poller can
    // accumulate the whole queue before its first lease exists.
    let mut download_targets: Vec<(String, u32, u32, u32, bool)> = workers
        .iter()
        .filter(|w| w.download && w.max_download_jobs > 0 && worker_admits_new_work(w))
        .map(|w| {
            let held = leases_by_node.get(&w.name).copied().unwrap_or(0);
            let backlog = snap
                .jobs
                .iter()
                .filter(|job| job.assigned_node.as_deref() == Some(w.name.as_str()))
                .count() as u32;
            (
                w.name.clone(),
                w.max_download_jobs.saturating_sub(backlog.max(held)),
                backlog.max(held),
                w.download_weight.max(1),
                false,
            )
        })
        .collect();

    // Assign untouched queued jobs across remote executors. The authority is
    // deliberately absent: every mutation-producing attempt needs a durable
    // lease and private generation.
    for job in snap.jobs.iter() {
        if job.assigned_node.is_some()
            || split_jobs.contains(&job.id)
            || !matches!(job.status, JobStatus::Queued)
            || durable_status.get(&job.id) != Some(&JobStatus::Queued)
        {
            continue;
        }
        download_targets.sort_by(|left, right| {
            (u64::from(left.2) * u64::from(right.3))
                .cmp(&(u64::from(right.2) * u64::from(left.3)))
                .then(left.0.cmp(&right.0))
        });
        let Some(slot) = download_targets.iter_mut().find(|target| target.1 > 0) else {
            break; // everyone is saturated
        };
        slot.1 -= 1;
        slot.2 += 1;
        let node = slot.0.clone();
        if !slot.4 {
            tracing::info!(job = job.id.0, %node, "delegating job by weighted load");
            let _ = s.engine.set_delegated(job.id, Some(node)).await;
        }
    }

    // ---- PP assignment (C2, CLUSTERING.md §13) ----------------------------
    // Anti-affinity: a node busy downloading is the LAST choice for par
    // repair / unpack — prefer idle PP-capable nodes so the same box never
    // runs both when the cluster has spare hands.
    let leased_jobs: HashSet<JobId> = s.leases.lock().unwrap().values().map(|l| l.job).collect();
    // (node, free_pp, downloading, assigned, weight)
    let mut pp_targets: Vec<(String, u32, bool, u32, u32)> = Vec::new();
    for w in workers
        .iter()
        .filter(|w| w.post_process && w.pp_slots > 0 && worker_admits_new_work(w))
    {
        let pp_held = pp_leases_by_node.get(&w.name).copied().unwrap_or(0)
            + assigned_pp_backlog(&snap, &w.name, &leased_jobs);
        let free = w.pp_slots.saturating_sub(pp_held);
        if free > 0 {
            let downloading =
                leases_by_node.get(&w.name).copied().unwrap_or(0) > 0 || w.active_download_jobs > 0;
            pp_targets.push((
                w.name.clone(),
                free,
                downloading,
                pp_held,
                w.pp_weight.max(1),
            ));
        }
    }
    // Idle nodes first, then lowest assigned/weight, then stable name.
    pp_targets.sort_by(|left, right| {
        left.2
            .cmp(&right.2)
            .then(
                (u64::from(left.3) * u64::from(right.4))
                    .cmp(&(u64::from(right.3) * u64::from(left.4))),
            )
            .then(left.0.cmp(&right.0))
    });

    for job in snap.jobs.iter() {
        if !matches!(job.status, JobStatus::Completed)
            || durable_status.get(&job.id) != Some(&JobStatus::Completed)
            || job.pp_done
            || job.assigned_node.is_some()
        {
            continue;
        }
        let Some(slot) = pp_targets.iter_mut().find(|target| target.1 > 0) else {
            break;
        };
        slot.1 -= 1;
        slot.3 += 1;
        let node = slot.0.clone();
        tracing::info!(job = job.id.0, %node, "assigning post-processing");
        let _ = s.engine.set_delegated(job.id, Some(node)).await;
    }
}

/// Completed-but-unprocessed jobs already assigned to `node` and not yet
/// leased count against its PP capacity (assignment-to-poll in flight).
fn assigned_pp_backlog(
    snap: &nzbd_engine::QueueSnapshot,
    node: &str,
    leased: &HashSet<JobId>,
) -> u32 {
    snap.jobs
        .iter()
        .filter(|j| {
            matches!(j.status, JobStatus::Completed)
                && !j.pp_done
                && !leased.contains(&j.id)
                && j.assigned_node.as_deref() == Some(node)
        })
        .count() as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::election::LeaderRecord;
    use axum::extract::State;
    use nzbd_engine::{Engine, EngineConfig, Tuning};
    use nzbd_types::{CertLevel, DupeInfo, Job, JobKind, JobTotals, ServerDef, ServerId, TlsMode};

    fn test_token(resource: &str) -> LeaseToken {
        LeaseToken {
            resource: resource.into(),
            owner_node_id: "worker".into(),
            owner_incarnation: "test-incarnation".into(),
            fence: 1,
            revision: 1,
            expires_at_unix_ms: i64::MAX,
        }
    }

    fn test_job(id: u32) -> Job {
        Job {
            id: JobId(id),
            kind: JobKind::Nzb,
            name: "job".into(),
            dir_name: "job".into(),
            name_provisional: false,
            queued_at_unix: 0,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: DupeInfo::default(),
            params: Vec::new(),
            files: Vec::new(),
            totals: JobTotals::default(),
            status: JobStatus::Completed,
            torrent: None,
            stages: Vec::new(),
        }
    }

    #[test]
    fn lost_completion_response_replays_only_the_exact_durable_receipt() {
        let token = test_token("work/job/7/download");
        let request = CompleteRequest {
            node: "worker".into(),
            lease_id: "work/job/7/download@1".into(),
            token: token.clone(),
            expected_job_revision: 3,
            result_id: "sha256:result".into(),
            result_ref: "/shared/generations/result".into(),
            receipt_id: "receipt".into(),
            job: test_job(7),
        };
        let receipt = crate::control::PublicationReceipt {
            resource: token.resource.clone(),
            owner_node_id: token.owner_node_id.clone(),
            owner_incarnation: token.owner_incarnation.clone(),
            fence: token.fence,
            lease_revision: token.revision,
            lease_expiry_ms: token.expires_at_unix_ms,
            expected_job_revision: 3,
            result_id: request.result_id.clone(),
            result_ref: request.result_ref.clone(),
            result_job_json: durable_result_job_json(
                &request.job,
                LeaseKind::Download,
                &request.result_ref,
            )
            .unwrap(),
            accepted_at_ms: 1,
            kind: "download".into(),
        };

        assert!(publication_retry_matches(
            &receipt,
            &request,
            LeaseKind::Download
        ));
        let mut changed = request;
        changed.result_ref.push_str("-different");
        assert!(!publication_retry_matches(
            &receipt,
            &changed,
            LeaseKind::Download
        ));
    }

    #[test]
    fn legacy_worker_without_disk_guard_capability_is_excluded() {
        let old = serde_json::json!({
            "name": "old",
            "api_url": "http://old",
            "download": true,
            "post_process": true,
            "max_download_jobs": 1,
            "active_download_jobs": 0,
            "pp_slots": 1,
            "rate_bps": 0,
            "seq": 1
        });
        let mut record: NodeRecord = serde_json::from_value(old).unwrap();
        assert!(!worker_admits_new_work(&record));
        record.disk_guard_capable = true;
        assert!(worker_admits_new_work(&record));
        record.disk_low = true;
        assert!(!worker_admits_new_work(&record));
    }

    #[tokio::test]
    async fn completed_cluster_history_reports_the_materialized_configured_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = SharedLayout::new(tmp.path(), "leader").unwrap();
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
            max_connections: 9,
            pipeline_depth: 1,
            retention_days: 0,
            cert_verification: CertLevel::Strict,
        };
        let mut scarce = provider.clone();
        scarce.id = ServerId(2);
        scarce.name = "scarce".into();
        scarce.max_connections = 1;
        let engine = Engine::spawn(EngineConfig::single_node(
            vec![provider.clone(), scarce.clone()],
            layout.state_dir(),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let (_view_tx, view) = watch::channel(LeaderView {
            record: Some(LeaderRecord {
                epoch: 1,
                node: "leader".into(),
                api_url: "http://leader.invalid".into(),
                seq: 1,
            }),
            is_me: true,
        });
        let cfg = ClusterConfig {
            cluster_id: "test".into(),
            node_name: "leader".into(),
            shared_dir: tmp.path().to_path_buf(),
            advertise_url: "http://leader.invalid".into(),
            secret: "secret".into(),
            coordinator: true,
            priority: 0,
            download: true,
            max_download_jobs: 1,
            post_process: true,
            pp_slots: 1,
            lease_interval: std::time::Duration::from_secs(1),
            takeover_after: std::time::Duration::from_secs(2),
            worker_ttl: std::time::Duration::from_secs(3),
            control_dir: tmp.path().join("control"),
            control_node_id: 1,
            control_raft_bind: "127.0.0.1:38110".into(),
            control_api_bind: "127.0.0.1:38210".into(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: Vec::new(),
            torrent_payload_roots: Vec::new(),
        };
        let hist = Arc::new(
            nzbd_state::history::HistoryDb::open(
                &tmp.path().join("history.sqlite"),
                Some(tmp.path()),
            )
            .unwrap(),
        );
        let post = nzbd_post::manager::PostConfig {
            completed_dir: Some(tmp.path().join("complete")),
            categories: vec![nzbd_post::manager::CategoryRule {
                name: "tv".into(),
                dest_dir: Some(tmp.path().join("category")),
                unpack: None,
                extensions: Vec::new(),
            }],
            ..Default::default()
        };
        let shared = LeaderShared::new(
            engine.clone(),
            layout,
            tmp.path().join("dest"),
            cfg,
            vec![provider, scarce],
            view,
            LeaderDurability::new(None, Some(hist.clone()), "test-incarnation".into())
                .with_post(Some(post)),
        );
        let mut job = test_job(812);
        job.params
            .push(("*Cluster:result-ref".into(), "/immutable/generation".into()));
        assert_eq!(
            shared.publication_root(&job, false),
            tmp.path().join("dest")
        );
        let ordinary = shared.publication_root(&job, true).join("job");
        assert_eq!(ordinary, tmp.path().join("complete/job"));
        job.category = Some(" TV ".into());
        let category = shared.publication_root(&job, true).join("job");
        assert_eq!(category, tmp.path().join("category/job"));
        // Both normal completion and takeover call this with the path just
        // materialized, never the immutable generation root from the worker.
        record_pp_history(&shared, &job, 123000, &category)
            .await
            .unwrap();
        record_pp_history(&shared, &job, 123000, &category)
            .await
            .unwrap();
        let entries = hist.list(10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].final_dir.as_deref(), category.to_str());
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn post_leases_share_the_provider_account_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = SharedLayout::new(tmp.path(), "leader").unwrap();
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
            max_connections: 9,
            pipeline_depth: 1,
            retention_days: 0,
            cert_verification: CertLevel::Strict,
        };
        let mut scarce = provider.clone();
        scarce.id = ServerId(2);
        scarce.name = "scarce".into();
        scarce.max_connections = 1;
        let engine = Engine::spawn(EngineConfig::single_node(
            vec![provider.clone(), scarce.clone()],
            layout.state_dir(),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let (_view_tx, view) = watch::channel(LeaderView {
            record: Some(LeaderRecord {
                epoch: 1,
                node: "leader".into(),
                api_url: "http://leader.invalid".into(),
                seq: 1,
            }),
            is_me: true,
        });
        let cfg = ClusterConfig {
            cluster_id: "test".into(),
            node_name: "leader".into(),
            shared_dir: tmp.path().to_path_buf(),
            advertise_url: "http://leader.invalid".into(),
            secret: "secret".into(),
            coordinator: true,
            priority: 0,
            download: true,
            max_download_jobs: 1,
            post_process: true,
            pp_slots: 1,
            lease_interval: std::time::Duration::from_secs(1),
            takeover_after: std::time::Duration::from_secs(2),
            worker_ttl: std::time::Duration::from_secs(3),
            control_dir: tmp.path().join("control"),
            control_node_id: 1,
            control_raft_bind: "127.0.0.1:38110".into(),
            control_api_bind: "127.0.0.1:38210".into(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: Vec::new(),
            torrent_payload_roots: Vec::new(),
        };
        let shared = LeaderShared::new(
            engine.clone(),
            layout,
            tmp.path().join("dest"),
            cfg,
            vec![provider, scarce],
            view,
            LeaderDurability::new(None, None, "test-incarnation".into()),
        );
        for (lease, node) in [("pp-a", "worker-a"), ("pp-b", "worker-b")] {
            shared.leases.lock().unwrap().insert(
                lease.into(),
                LeaseInfo {
                    job: JobId(if node == "worker-a" { 1 } else { 2 }),
                    node: node.into(),
                    kind: LeaseKind::Post,
                    token: test_token(lease),
                    job_revision: 1,
                    control_revision: 1,
                    last_hb: Instant::now(),
                },
            );
        }

        assert_eq!(shared.budget_nodes().len(), 2);
        let shares: Vec<_> = ["worker-a", "worker-b"]
            .iter()
            .map(|node| shared.budgets_for_node(node))
            .collect();
        assert_eq!(shares.iter().map(|share| share["provider"]).sum::<u16>(), 9);
        assert_eq!(shares.iter().map(|share| share["scarce"]).sum::<u16>(), 1);
        assert_eq!(
            shares.iter().filter(|share| share["scarce"] == 0).count(),
            1,
            "a one-connection account cannot issue one connection per executor"
        );
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn rejected_transition_grant_releases_lease_and_delegation() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = SharedLayout::new(tmp.path(), "leader").unwrap();
        let engine = Engine::spawn(EngineConfig::single_node(
            Vec::new(),
            layout.state_dir(),
            tmp.path().join("dest"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let job = Job {
            id: JobId(91),
            kind: JobKind::Nzb,
            name: "reject".into(),
            dir_name: "reject".into(),
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
        };
        engine.import_job(job, false, false).await.unwrap();
        assert!(engine
            .set_delegated(JobId(91), Some("worker".into()))
            .await
            .unwrap());

        let (_view_tx, view) = watch::channel(LeaderView {
            record: Some(LeaderRecord {
                epoch: 3,
                node: "leader".into(),
                api_url: "http://leader.invalid".into(),
                seq: 1,
            }),
            is_me: true,
        });
        let cfg = ClusterConfig {
            cluster_id: "test".into(),
            node_name: "leader".into(),
            shared_dir: tmp.path().to_path_buf(),
            advertise_url: "http://leader.invalid".into(),
            secret: "secret".into(),
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            pp_slots: 0,
            lease_interval: std::time::Duration::from_secs(1),
            takeover_after: std::time::Duration::from_secs(2),
            worker_ttl: std::time::Duration::from_secs(3),
            control_dir: tmp.path().join("control"),
            control_node_id: 1,
            control_raft_bind: "127.0.0.1:38111".into(),
            control_api_bind: "127.0.0.1:38211".into(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: Vec::new(),
            torrent_payload_roots: Vec::new(),
        };
        let shared = LeaderShared::new(
            engine.clone(),
            layout,
            tmp.path().join("dest"),
            cfg,
            Vec::new(),
            view,
            LeaderDurability::new(None, None, "test-incarnation".into()),
        );
        shared.authority_ready.store(true, Ordering::Release);
        shared.leases.lock().unwrap().insert(
            "transition-lease".into(),
            LeaseInfo {
                job: JobId(91),
                node: "worker".into(),
                kind: LeaseKind::Download,
                token: test_token("transition-lease"),
                job_revision: 1,
                control_revision: 1,
                last_hb: Instant::now(),
            },
        );
        let mut headers = HeaderMap::new();
        headers.insert(SECRET_HEADER, "secret".parse().unwrap());
        let response = work_reject(
            State(shared.clone()),
            headers,
            Json(RejectRequest {
                node: "worker".into(),
                lease_id: "transition-lease".into(),
                token: test_token("transition-lease"),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(shared.leases.lock().unwrap().is_empty());
        assert_eq!(
            engine
                .snapshot()
                .jobs
                .iter()
                .find(|job| job.id == JobId(91))
                .unwrap()
                .assigned_node,
            None
        );
        engine.shutdown().await;
    }
}
