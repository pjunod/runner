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
use axum::extract::{DefaultBodyLimit, Request, State};
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

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct BudgetHandoff {
    revision: u64,
    generation: u64,
    grants: HashMap<String, HashMap<String, u16>>,
    commands: HashMap<String, HashMap<String, u16>>,
    pending_target: Option<HashMap<String, HashMap<String, u16>>>,
    awaiting_shrink: HashSet<String>,
}

/// Serialize the actual writes, not just their SQL effects. A successful old
/// snapshot never acknowledges a newer handoff that changed while IO ran.
async fn persist_budget_snapshot<F, Fut>(
    budgets: &Mutex<BudgetHandoff>,
    durable: &tokio::sync::Mutex<u64>,
    save: F,
) -> Result<u64, String>
where
    F: FnOnce(u64, String) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let mut durable = durable.lock().await;
    let snapshot = {
        let state = budgets.lock().unwrap();
        if state.revision <= *durable {
            return Ok(*durable);
        }
        state.clone()
    };
    let json = serde_json::to_string(&snapshot).map_err(|error| error.to_string())?;
    save(snapshot.revision, json).await?;
    *durable = snapshot.revision;
    Ok(*durable)
}

type CompletionReply = (StatusCode, Vec<u8>);
#[derive(Default)]
pub(crate) struct CompletionOperations {
    entries: Mutex<HashMap<String, watch::Receiver<Option<CompletionReply>>>>,
}
impl CompletionOperations {
    /// Own the whole verified completion through its durable receipt and
    /// publication, independently of a caller's shorter network deadline.
    pub(crate) async fn run<F>(&self, key: String, work: F) -> Result<CompletionReply, String>
    where
        F: std::future::Future<Output = Response> + Send + 'static,
    {
        let mut receiver = {
            let mut entries = self.entries.lock().unwrap();
            if entries.get(&key).is_some_and(|receiver| {
                receiver
                    .borrow()
                    .as_ref()
                    .is_some_and(|(status, _)| !status.is_success())
            }) {
                entries.remove(&key);
            }
            if let Some(receiver) = entries.get(&key) {
                receiver.clone()
            } else {
                while entries.len() >= 32 {
                    let finished = entries
                        .iter()
                        .find(|(_, receiver)| receiver.borrow().is_some())
                        .map(|(key, _)| key.clone());
                    match finished {
                        Some(key) => {
                            entries.remove(&key);
                        }
                        None => {
                            return Err(
                                "32 completion operations are active; retry after one finishes"
                                    .into(),
                            );
                        }
                    }
                }
                let (sender, receiver) = watch::channel(None);
                entries.insert(key, receiver.clone());
                tokio::spawn(async move {
                    let response = work.await;
                    let status = response.status();
                    let reply = match axum::body::to_bytes(response.into_body(), MAX_RPC_BYTES)
                        .await
                    {
                        Ok(body) => (status, body.to_vec()),
                        Err(error) => (
                            StatusCode::SERVICE_UNAVAILABLE,
                            format!("completion reply could not be retained: {error}").into_bytes(),
                        ),
                    };
                    let _ = sender.send(Some(reply));
                });
                receiver
            }
        };
        loop {
            if let Some(reply) = receiver.borrow().clone() {
                return Ok(reply);
            }
            receiver
                .changed()
                .await
                .map_err(|_| "completion operation ended without a durable result".to_owned())?;
        }
    }
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
    budget_durable_revision: tokio::sync::Mutex<u64>,
    /// Serializes externally acknowledged queue mutations and publication
    /// commits. The engine is speculative until replicated control accepts
    /// the exact request-local delta.
    pub(crate) mutation_serial: tokio::sync::Mutex<()>,
    pub(crate) delivery_serial: tokio::sync::Mutex<()>,
    authority_ready: AtomicBool,
    /// With one descriptor per poll, alternate the first lane so a stream of
    /// downloads cannot starve a worker's free post-processing capacity.
    post_poll_turn: Mutex<HashSet<String>>,
    completion_operations: CompletionOperations,
    pub(crate) copy_operations: CompletionOperations,
    pub(crate) copy_guards: crate::cluster_keep::CopyGuards,
    pub(crate) control_proofs: crate::control_proofs::ControlProofs,
    pub(crate) worker_leases: Mutex<Option<crate::worker::ActiveLeases>>,
    #[cfg(test)]
    verification_gate: Mutex<Option<Arc<VerificationGate>>>,
    #[cfg(test)]
    projection_gate: Mutex<Option<Arc<VerificationGate>>>,
    #[cfg(test)]
    grant_gate: Mutex<Option<Arc<VerificationGate>>>,
}
#[cfg(test)]
struct VerificationGate {
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
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
            budget_durable_revision: tokio::sync::Mutex::new(0),
            mutation_serial: tokio::sync::Mutex::new(()),
            delivery_serial: tokio::sync::Mutex::new(()),
            authority_ready: AtomicBool::new(false),
            post_poll_turn: Mutex::new(HashSet::new()),
            completion_operations: CompletionOperations::default(),
            copy_operations: CompletionOperations::default(),
            copy_guards: Default::default(),
            control_proofs: Default::default(),
            worker_leases: Mutex::new(None),
            #[cfg(test)]
            verification_gate: Mutex::new(None),
            #[cfg(test)]
            projection_gate: Mutex::new(None),
            #[cfg(test)]
            grant_gate: Mutex::new(None),
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
            if let Some(job) = self.engine.export_job(summary.id).await.map_err(|error| {
                format!("export queue projection for job {}: {error}", summary.id.0)
            })? {
                jobs.insert(u64::from(job.id.0), job);
            }
        }
        Ok(jobs)
    }

    pub(crate) async fn restore_control_projection(&self) -> Result<(), String> {
        crate::control_recovery::reconcile(self).await?;
        let jobs = crate::control_recovery::projection(self).await?;
        self.engine
            .adopt_replicated_authority(jobs)
            .await
            .map_err(|error| format!("replace local control projection: {error}"))?;
        crate::admissions::adopt_counters(self).await
    }

    /// Commit only rows changed by one serialized HTTP request. This avoids
    /// treating an incomplete local snapshot as permission to delete any
    /// unrelated replicated work.
    pub(crate) async fn commit_mutation_delta(
        &self,
        before: &HashMap<u64, nzbd_types::Job>,
        fences: &HashMap<u64, u64>,
        removals: &std::collections::BTreeMap<u32, bool>,
    ) -> Result<(), String> {
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| "replicated control unavailable".to_owned())?;
        let mut after = self.engine_projection().await?;
        for job in after.values_mut() {
            if let Some(marker) = nzbd_engine::automatic_control::transition(job) {
                let old = before.get(&u64::from(job.id.0));
                let same_business = match old {
                    Some(old) => {
                        crate::control_recovery::business_intent(old)?
                            == crate::control_recovery::business_intent(job)?
                    }
                    None => false,
                };
                if let Some(old) = old.filter(|_| same_business) {
                    // Automatic/progress changes on another job belong to
                    // reconciliation, never this request's whole-job delta.
                    *job = old.clone();
                    continue;
                }
                if old
                    .and_then(nzbd_engine::automatic_control::transition)
                    .is_some_and(|prior| prior.identity == marker.identity)
                {
                    // Explicit Keep/name/source edits replace a pending result
                    // without accepting an unproved automatic release.
                    let (durable, _, _) = control
                        .current_job_intent(job.id.0)
                        .await?
                        .ok_or("pending job was removed by another authority")?;
                    job.params.retain(|(key, _)| {
                        key != nzbd_engine::automatic_control::AUTOMATIC_CONTROL_PARAM
                            && key != nzbd_types::CONTROL_PARAM
                    });
                    if let Some(current) = durable.control() {
                        job.set_control(&current);
                    }
                    job.status = durable.status;
                } else {
                    return Err("automatic recovery changed this job during the request; retry against current control".into());
                }
            }
            if !before.contains_key(&u64::from(job.id.0)) {
                crate::source_facts::bind_owned(self, job)?;
            }
        }
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
        let receipts = crate::admissions::pending_receipts(self).await?;
        control
            .commit_job_delta_guarded_with_removals(
                &changes,
                fences,
                &receipts,
                Some((&self.cfg.node_name, &self.owner_incarnation, self.epoch())),
                &removals
                    .iter()
                    .map(|(job, files)| {
                        (
                            *job,
                            if *files {
                                crate::cancellation::RemovalMode::DeleteFiles
                            } else {
                                crate::cancellation::RemovalMode::KeepFiles
                            },
                        )
                    })
                    .collect(),
            )
            .await?;
        if !receipts.is_empty()
            && !self
                .engine
                .adopt_url_admissions(receipts)
                .await
                .map_err(|error| error.to_string())?
        {
            self.fence_authority();
            return Err("engine refused committed admission projection".into());
        }
        self.restore_control_projection().await
    }

    pub(crate) fn authority_identity(&self) -> (&str, &str, u64) {
        (&self.cfg.node_name, &self.owner_incarnation, self.epoch())
    }

    pub(crate) fn fence_authority(&self) {
        self.authority_ready.store(false, Ordering::Release);
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
            "controlProofs":self.control_proofs.snapshot(),
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
    fn budget_command(
        &self,
        node: &str,
        ack: Option<&BudgetAck>,
    ) -> (u64, u64, HashMap<String, u16>) {
        let target = self.budget_target();
        let mut state = self.budgets.lock().unwrap();
        let before = state.clone();

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
        if *state != before {
            state.revision = state.revision.saturating_add(1).max(1);
        }
        (state.revision, state.generation, command)
    }

    async fn persist_budget_state(&self) -> Result<u64, String> {
        let control = self
            .control
            .as_ref()
            .ok_or("replicated control unavailable")?;
        let issuer = self.authority_identity();
        persist_budget_snapshot(
            &self.budgets,
            &self.budget_durable_revision,
            |revision, json| async move {
                control
                    .save_budget_state(&self.cfg.cluster_id, revision, &json, issuer)
                    .await
            },
        )
        .await
    }

    async fn restore_budget_state(&self) -> Result<(), String> {
        let control = self
            .control
            .as_ref()
            .ok_or("replicated control unavailable")?;
        let epoch = self.epoch();
        let mut durable = self.budget_durable_revision.lock().await;
        let before = self.budgets.lock().unwrap().clone();
        let restored = control
            .load_budget_state(&self.cfg.cluster_id)
            .await?
            .map(|json| {
                serde_json::from_str::<BudgetHandoff>(&json).map_err(|error| error.to_string())
            })
            .transpose()?
            .unwrap_or_default();
        let mut state = self.budgets.lock().unwrap();
        if !self.is_leader() || self.epoch() != epoch || *state != before {
            return Err("budget authority changed during durable restore; retry".into());
        }
        *durable = restored.revision;
        *state = restored;
        Ok(())
    }

    fn budget_command_is_current(
        &self,
        epoch: u64,
        generation: u64,
        node: &str,
        command: &HashMap<String, u16>,
    ) -> bool {
        if !self.is_leader() || self.epoch() != epoch {
            return false;
        }
        let state = self.budgets.lock().unwrap();
        state.generation == generation
            && match state.commands.get(node) {
                Some(current) => current == command,
                None => {
                    command
                        == &self
                            .servers
                            .iter()
                            .map(|server| (server.name.clone(), 0))
                            .collect()
                }
            }
    }

    async fn apply_local_budgets(&self) {
        let by_id = self.servers.iter().map(|s| (s.id, 0u16)).collect();
        let _ = self.engine.set_server_budgets(by_id).await;
        if let Err(error) = self.persist_budget_state().await {
            tracing::warn!(%error, "provider budget state was not persisted");
        }
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
        let before = state.clone();
        state.grants.remove(node);
        state.commands.remove(node);
        state.awaiting_shrink.remove(node);
        if let Some(target) = state.pending_target.as_mut() {
            target.remove(node);
        }
        if state.pending_target.is_some() && state.awaiting_shrink.is_empty() {
            state.generation = state.generation.saturating_add(1);
            state.grants = state.pending_target.take().unwrap();
            state.commands = state.grants.clone();
        }
        if *state != before {
            state.revision = state.revision.saturating_add(1).max(1);
        }
    }

    async fn grant_job(
        &self,
        node: &str,
        owner_incarnation: &str,
        kind: LeaseKind,
        mut job: nzbd_types::Job,
        mut scope: serde_json::Value,
    ) -> Option<Grant> {
        let (issuer_node, issuer_incarnation, issuer_fence) = self.authority_identity();
        scope.as_object_mut()?.insert("_authority".into(),serde_json::json!({"node":issuer_node,"incarnation":issuer_incarnation,"fence":issuer_fence}));
        let control = self.control.as_ref()?;
        let (durable, job_revision, job_incarnation) = match control
            .current_job_intent(job.id.0)
            .await
        {
            Ok(Some(row)) => row,
            Ok(None) => return None,
            Err(error) => {
                tracing::warn!(job=job.id.0,%error,"grant paused until current durable intent is available");
                return None;
            }
        };
        if !crate::control_recovery::grant_intent_matches(&job, &durable).ok()? {
            if tracing::enabled!(tracing::Level::DEBUG) {
                let differences = grant_intent_differences(&job, &durable);
                tracing::debug!(job=job.id.0,%node,?kind,?differences,"grant awaits exact replicated intent");
            }
            return None;
        }
        if serde_json::to_vec(&(&job, &scope)).ok()?.len() > MAX_DESCRIPTOR_BYTES {
            let message = format!(
                "job descriptor exceeds the {} MiB cluster limit; split the NZB or reduce its metadata before retrying",
                MAX_DESCRIPTOR_BYTES / 1024 / 1024
            );
            let _serial = self.mutation_serial.lock().await;
            // The descriptor was measured before acquiring the mutation lane.
            // A newer operator edit must be re-measured on its own request.
            if control
                .current_job_intent(job.id.0)
                .await
                .ok()
                .flatten()
                .is_none_or(|(_, revision, incarnation)| {
                    revision != job_revision || incarnation != job_incarnation
                })
            {
                return None;
            }
            if self
                .engine
                .hold_job(
                    job.id,
                    nzbd_types::HoldCause::parse("descriptor_oversize"),
                    "cluster",
                    &message,
                )
                .await
                .is_ok_and(|held| held)
            {
                if let Err(error) = crate::control_recovery::reconcile(self).await {
                    tracing::warn!(%error, job=job.id.0, "descriptor hold awaits replicated control");
                }
            }
            tracing::warn!(job=job.id.0, %message, "cluster work descriptor refused");
            return None;
        }
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
        #[cfg(test)]
        {
            let gate = self.grant_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.entered.add_permits(1);
                gate.release.acquire().await.unwrap().forget();
            }
        }
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
        if !self.is_leader() || self.epoch() != issuer_fence {
            return None;
        }
        let (budget_revision, budget_generation, server_budgets) = self.budget_command(node, None);
        match self.persist_budget_state().await {
            Ok(revision)
                if revision >= budget_revision
                    && self.budget_command_is_current(
                        issuer_fence,
                        budget_generation,
                        node,
                        &server_budgets,
                    ) => {}
            outcome => {
                tracing::warn!(job=job.id.0, error=?outcome.err(), "grant awaits durable current provider budget");
                return None;
            }
        }
        Some(Grant {
            lifecycle_protocol: crate::proto::LIFECYCLE_PROTOCOL,
            lease_id,
            token,
            job_incarnation,
            job_revision,
            control_revision: job_revision,
            scope,
            epoch: issuer_fence,
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
        .route(
            "/cluster/v1/keep/apply",
            post(crate::cluster_keep::apply_owner),
        )
        .route(
            "/cluster/v1/work/cancellation-seal",
            post(crate::cancellation_routes::seal),
        )
        .route(
            "/cluster/v1/work/cancellation-stop-authority",
            post(crate::cancellation_routes::stop_authority),
        )
        .route(
            "/cluster/v1/work/cancellation-seal-authority",
            post(crate::cancellation_routes::seal_authority),
        )
        .route(
            "/cluster/v1/work/cancelled-completion",
            post(crate::cancellation_routes::completed),
        )
        .route(
            "/cluster/v1/work/cancellation-stop",
            post(crate::cancellation_routes::stop),
        )
        .route(
            "/cluster/v1/work/relocation-proof",
            post(crate::control_recovery::relocation_proof).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/cluster/v1/deliveries/migrate-page",
            post(crate::delivery::local_delivery_page),
        )
        .route(
            "/cluster/v1/deliveries/copy-authority",
            post(crate::delivery::copy_authority),
        )
        .route(
            "/cluster/v1/deliveries/cleanup-copy",
            post(crate::delivery::cleanup_copy),
        )
        .route("/cluster/v1/work/script-receipt", post(work_script_receipt))
        // Authenticate the peer namespace before extractors parse a request
        // body. Otherwise an unauthenticated malformed request can reveal
        // schema details through a 4xx extractor response.
        .route_layer(middleware::from_fn_with_state(
            shared.clone(),
            require_cluster_secret,
        ))
        .layer(DefaultBodyLimit::max(MAX_RPC_BYTES))
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

fn poll_scope(request: &PollRequest, mut scope: serde_json::Value) -> serde_json::Value {
    scope["_workerStorage"] = serde_json::Value::String(request.storage_instance_id.clone());
    scope["_startOrdered"] = serde_json::json!(request.start_ordering);
    scope
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
    if req.start_ordering != 1
        || req.storage_instance_id.is_empty()
        || req.storage_instance_id.len() > 128
        || req.storage_instance_id.chars().any(char::is_control)
    {
        return (StatusCode::UPGRADE_REQUIRED,Json(serde_json::json!({"error":"worker requires durable start/stop ordering and receiver storage identity; upgrade before accepting work"}))).into_response();
    }
    if req.lifecycle_protocol < LIFECYCLE_PROTOCOL {
        return (StatusCode::UPGRADE_REQUIRED,Json(serde_json::json!({"error":"worker requires cluster lifecycle protocol 2 (durable completion outbox and exact copy cleanup); upgrade this worker"}))).into_response();
    }
    // The registry is the leader's independent admission fact. Do not trust
    // slot counts from a poll whose node is already known to be held.
    let registered = read_nodes(&s.layout)
        .into_iter()
        .find(|node| node.name == req.node);
    if registered
        .as_ref()
        .is_none_or(|node| !worker_admits_new_work(node))
    {
        tracing::debug!(node=%req.node,free_pp=req.free_pp_slots,registered=registered.is_some(),disk_low=?registered.as_ref().map(|node|node.disk_low),disk_guard=?registered.as_ref().map(|node|node.disk_guard_capable),"worker poll awaits registry admission");
        return Json(PollResponse::default()).into_response();
    }

    // Explicit range work is selected independently of whole-job
    // delegation. The authority keeps the complete job; each grant carries
    // only one bounded article view.
    let snap = s.engine.snapshot();
    let mut grants = Vec::new();
    let post_first =
        req.free_pp_slots > 0 && post_lane_first(&mut s.post_poll_turn.lock().unwrap(), &req.node);
    if post_first {
        if let Some(grant) = grant_post_candidate(&s, &req, &snap).await {
            grants.push(grant);
            s.apply_local_budgets().await;
            return Json(PollResponse { grants }).into_response();
        }
    }
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
                        poll_scope(&req, serde_json::to_value(scope).unwrap()),
                    )
                    .await
                {
                    tracing::info!(job = job_id.0, file = scope.file_id, first = scope.first_article, last = scope.last_article, node = %req.node, "article range granted");
                    grants.push(grant);
                    s.apply_local_budgets().await;
                    return Json(PollResponse { grants }).into_response();
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
                            poll_scope(&req, serde_json::to_value(scope).unwrap()),
                        )
                        .await
                    {
                        tracing::info!(job = job_id.0, node = %req.node, "assembly lease granted");
                        grants.push(grant);
                        s.apply_local_budgets().await;
                        return Json(PollResponse { grants }).into_response();
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
                poll_scope(&req, serde_json::json!({"whole_job": true})),
            )
            .await
        {
            tracing::info!(job = job_id.0, node = %req.node, lease = %grant.lease_id, "download lease granted");
            grants.push(grant);
            s.apply_local_budgets().await;
            return Json(PollResponse { grants }).into_response();
        }
    }

    if let Some(grant) = grant_post_candidate(&s, &req, &snap).await {
        grants.push(grant);
    }

    if !grants.is_empty() {
        s.apply_local_budgets().await;
    }
    Json(PollResponse { grants }).into_response()
}

async fn grant_post_candidate(
    s: &LeaderShared,
    req: &PollRequest,
    snap: &nzbd_engine::QueueSnapshot,
) -> Option<Grant> {
    // PP grants (C2): completed jobs the scheduler assigned to this node
    // for post-processing, not yet leased, PP not yet done.
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
        if req.free_pp_slots == 0 {
            tracing::debug!(job=job_id.0,node=%req.node,"POST grant awaits free worker slot");
            break;
        }
        let already = s.leases.lock().unwrap().values().any(|l| l.job == job_id);
        if already {
            tracing::debug!(job=job_id.0,node=%req.node,"POST grant awaits existing lease retirement");
            continue;
        }
        let job = match s.engine.export_job(job_id).await {
            Ok(Some(job)) => job,
            result => {
                tracing::debug!(job=job_id.0,node=%req.node,error=?result.err(),"POST grant awaits export readiness");
                continue;
            }
        };
        if let Some(grant) = s
            .grant_job(
                &req.node,
                &req.owner_incarnation,
                LeaseKind::Post,
                job,
                poll_scope(req, serde_json::json!({"whole_job": true})),
            )
            .await
        {
            return Some(grant);
        }
    }
    None
}

/// Field names only: never log a URL, descriptor or parameter value.
fn grant_intent_differences(local: &nzbd_types::Job, durable: &nzbd_types::Job) -> Vec<String> {
    let mut differences = Vec::new();
    if local.status != durable.status {
        differences.push("status".into());
    }
    if local.control() != durable.control() {
        differences.push("control".into());
    }
    if durable.held() {
        differences.push("durable-held".into());
    }
    if nzbd_engine::automatic_control::transition(local).is_some() {
        differences.push("local-automatic-transition".into());
    }
    let pair = crate::control_recovery::business_intent(local).and_then(|local| {
        crate::control_recovery::business_intent(durable).map(|durable| (local, durable))
    });
    let Ok((local, durable)) = pair else {
        differences.push("intent-serialization".into());
        return differences;
    };
    let pair = serde_json::from_slice::<serde_json::Value>(&local).and_then(|local| {
        serde_json::from_slice::<serde_json::Value>(&durable).map(|durable| (local, durable))
    });
    let Ok((local, durable)) = pair else {
        differences.push("intent-decoding".into());
        return differences;
    };
    for (index, name) in [
        "id",
        "kind",
        "name",
        "dir_name",
        "name_provisional",
        "queued_at_unix",
        "original_name",
        "category",
        "priority",
        "dupe",
        "delivery_lifecycle",
        "params",
        "files",
    ]
    .iter()
    .enumerate()
    {
        if local[index] == durable[index] {
            continue;
        }
        differences.push((*name).into());
        if *name == "params" {
            let map =
                |value: &serde_json::Value| -> std::collections::BTreeMap<String, Vec<String>> {
                    let mut map = std::collections::BTreeMap::new();
                    for pair in value.as_array().into_iter().flatten() {
                        if let (Some(key), Some(value)) = (pair[0].as_str(), pair[1].as_str()) {
                            map.entry(key.to_owned())
                                .or_insert_with(Vec::new)
                                .push(value.to_owned());
                        }
                    }
                    map
                };
            let left = map(&local[index]);
            let right = map(&durable[index]);
            for key in left
                .keys()
                .chain(right.keys())
                .collect::<std::collections::BTreeSet<_>>()
            {
                if left.get(key) != right.get(key) {
                    differences.push(format!(
                        "param:{}",
                        key.chars().take(96).collect::<String>()
                    ));
                    if differences.len() >= 48 {
                        return differences;
                    }
                }
            }
        }
    }
    differences
}

fn post_lane_first(turns: &mut HashSet<String>, node: &str) -> bool {
    if turns.remove(node) {
        false
    } else {
        turns.insert(node.to_owned());
        true
    }
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

struct HeartbeatIntent {
    status: String,
    revision: i64,
}
impl From<&mut hiqlite::Row<'_>> for HeartbeatIntent {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            status: row.get("status"),
            revision: row.get("revision"),
        }
    }
}
async fn heartbeat_job_intent(
    control: &ControlStore,
    job: JobId,
) -> Result<Option<(JobStatus, u64)>, String> {
    let row = control.client.query_consistent_map::<HeartbeatIntent, _>(
        "SELECT json_quote(json_extract(intent_json,'$.status')) AS status,revision FROM control_jobs WHERE job_id=?1 AND deleted=0",
        hiqlite::params!(i64::from(job.0)),
    ).await.map_err(|error| error.to_string())?.into_iter().next();
    row.map(|row| {
        Ok((
            serde_json::from_str(&row.status).map_err(|error| error.to_string())?,
            u64::try_from(row.revision).map_err(|error| error.to_string())?,
        ))
    })
    .transpose()
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

    let epoch = s.epoch();
    let (budget_revision, budget_generation, server_budgets) =
        s.budget_command(&req.node, req.budget_ack.as_ref());
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
        // Renewal continues an existing exact lease. Unrelated local payload
        // recovery is not an operator revocation; use the durable status and
        // revision together, without exporting unverified local file evidence.
        let intent = match &s.control {
            Some(control) => match heartbeat_job_intent(control, lp.job).await {
                Ok(intent) => intent,
                Err(error) => {
                    tracing::debug!(job = lp.job.0, lease = %lp.lease_id, %error,
                        "heartbeat cannot read current durable intent");
                    None
                }
            },
            None => None,
        };
        if !intent.is_some_and(|(status, revision)| {
            !matches!(
                status,
                JobStatus::Paused | JobStatus::Held | JobStatus::Deleted
            ) && revision == lp.job_revision
        }) {
            tracing::debug!(job = lp.job.0, lease = %lp.lease_id,
                expected_revision = lp.job_revision, current_intent = ?intent,
                "heartbeat refuses renewal without current runnable authority");
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
    // Existing lease renewals do not wait behind an unrelated capacity write.
    // A failed write withholds the new command; it never fabricates a drain or
    // increases capacity from state that a successor cannot recover.
    let server_budgets = match s.persist_budget_state().await {
        Ok(revision)
            if revision >= budget_revision
                && s.budget_command_is_current(
                    epoch,
                    budget_generation,
                    &req.node,
                    &server_budgets,
                ) =>
        {
            Some(server_budgets)
        }
        outcome => {
            tracing::warn!(error=?outcome.err(), "heartbeat withholds uncommitted provider budget");
            None
        }
    };
    Json(HeartbeatResponse {
        cancel,
        renewed,
        controls,
        server_budgets,
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
    if !s.is_leader() || !s.authority_ready() {
        return authority_not_ready();
    }
    use sha2::{Digest, Sha256};
    let key = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&req).expect("completion serializes"))
    );
    let operation_owner = s.clone();
    match s
        .completion_operations
        .run(key, async move {
            complete_operation(operation_owner, req).await
        })
        .await
    {
        Ok((status, body)) => (
            status,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":error})),
        )
            .into_response(),
    }
}

async fn complete_operation(s: Arc<LeaderShared>, req: CompleteRequest) -> Response {
    if !s.is_leader() {
        return not_leader();
    }
    if !s.authority_ready() {
        return authority_not_ready();
    }
    let job_id = req.job.id;
    if req.job.delivery_lifecycle && req.lifecycle_protocol < LIFECYCLE_PROTOCOL {
        return (StatusCode::UPGRADE_REQUIRED,Json(serde_json::json!({"error":"lifecycle completion requires protocol 2 copy custody/outbox metadata"}))).into_response();
    }
    if serde_json::to_vec(&req.job).map_or(true, |bytes| bytes.len() > MAX_DESCRIPTOR_BYTES) {
        return (StatusCode::PAYLOAD_TOO_LARGE,
            Json(serde_json::json!({"error": "completed job descriptor exceeds 16 MiB; reduce metadata before retrying"}))).into_response();
    }
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
                    .into_response();
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
        // Sealing/outbox persistence can span a heartbeat. The atomic
        // publish_result_renewable transaction below selects the current live
        // token for this exact grant; renewal never changes its result identity.
        || !heartbeat_token_can_advance(&req.token, &info.token)
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
    let ready_delivery_id = cluster_delivery_id(&req.result_id);
    let already_ready = match &s.control {
        Some(control) => match control.cluster_delivery(&ready_delivery_id).await {
            Ok(record) => record,
            Err(error) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({"error":error})),
                )
                    .into_response();
            }
        },
        None => None,
    };
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
    #[cfg(test)]
    {
        let gate = s.verification_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.entered.add_permits(1);
            gate.release.acquire().await.unwrap().forget();
        }
    }
    let verified = if let Some(record) = &already_ready {
        Ok(Ok(record
            .delivery
            .files
            .iter()
            .map(|file| nzbd_state::artifacts::ExternalGenerationFile {
                path: file.path.clone(),
                bytes: file.size_bytes,
                sha256: file.sha256.clone(),
            })
            .collect()))
    } else {
        tokio::task::spawn_blocking(move || {
            verify_generation(
                &verify_root,
                &verify_ref,
                &verify_id,
                verify_job_incarnation.as_deref(),
                verify_fence,
                &verify_job,
            )
        })
        .await
    };
    let verified_files = match verified {
        Ok(Ok(value)) => value,
        _ => {
            tracing::warn!(job=job_id.0,owner_incarnation=%verify_incarnation,"completion generation validation failed");
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({"error":"invalid completion generation"})),
            )
                .into_response();
        }
    };
    let has_payload = !verified_files.is_empty();
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
                .publish_result_renewable(
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
            cleanup_only: false,
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
        let publish_dir = cluster_publication_name(
            &original_dir,
            &req.result_id,
            info.kind == LeaseKind::Post && published_job.delivery_lifecycle,
        );
        let final_path = dest_dir.join(&publish_dir);
        published_job.dir_name = publish_dir;
        published_job
            .params
            .retain(|(key, _)| key != "*Cluster:final-dir");
        published_job.params.push((
            "*Cluster:final-dir".into(),
            final_path.to_string_lossy().into_owned(),
        ));
        let publication = nzbd_state::artifacts::ExternalGeneration {
            result_id: publish_result_id,
            fence,
            source: PathBuf::from(result_ref).join("files"),
            files: verified_files,
        };
        let history = (info.kind == LeaseKind::Post && s.history.is_some())
            .then(|| pp_history_entry(&published_job, accepted_at_ms, &final_path));
        if already_ready.is_none() && has_payload {
            if let Err(error) = s
                .engine
                .materialize_generation(
                    published_job.clone(),
                    dest_dir,
                    final_path.clone(),
                    publication,
                    history,
                    true,
                )
                .await
            {
                tracing::warn!(job=job_id.0, %error, "accepted generation materialization remains pending");
                return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "generation selected but final directory publication is incomplete"})),
            ).into_response();
            }
        }
        final_publication = Some(final_path);
    }
    if !has_payload {
        published_job
            .params
            .push(("*Cluster:cleanup-only".into(), "1".into()));
        published_job.params.push((
            "Cluster:CleanupId".into(),
            cluster_delivery_id(&req.result_id),
        ));
    }
    #[cfg(test)]
    {
        let gate = s.projection_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.entered.add_permits(1);
            gate.release.acquire().await.unwrap().forget();
        }
    }
    // Bulk verification/publication never holds the queue control mutex.
    // Rebase only completion metadata onto the current durable intent here.
    let _projection = s.mutation_serial.lock().await;
    let selected = match s
        .control
        .as_ref()
        .unwrap()
        .selected_publication_job(job_id.0, &req.result_id)
        .await
    {
        Ok(value) => value,
        Err(error) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error":error})),
            )
                .into_response();
        }
    };
    let Some((mut current_job, current_revision)) = selected else {
        let _ = s.restore_control_projection().await;
        if already_ready.is_none() {
            return (StatusCode::CONFLICT,Json(serde_json::json!({"error":"selected completion was superseded by durable cancellation before handoff"}))).into_response();
        }
        return Json(CompleteResponse {
            final_dir: final_publication.map(|path| path.to_string_lossy().into_owned()),
            ok: true,
            durable_receipt: Some(req.receipt_id),
            accepted_at_unix_ms: Some(accepted_at_ms),
            history_recorded_by_authority: true,
            cleanup_only: !has_payload,
        })
        .into_response();
    };
    for (key, value) in published_job.params.iter().filter(|(key, _)| {
        matches!(
            key.as_str(),
            "*Cluster:result-ref"
                | "*Cluster:final-dir"
                | "*Cluster:cleanup-only"
                | "Cluster:CleanupId"
        )
    }) {
        current_job.params.retain(|(existing, _)| existing != key);
        current_job.params.push((key.clone(), value.clone()));
    }
    published_job = current_job;
    if info.kind == LeaseKind::Post && published_job.delivery_lifecycle {
        match ensure_cluster_delivery(&s, &published_job, &req.result_id, &req.result_ref, &req.receipt_id,
            accepted_at_ms, final_publication.as_deref().expect("post path")).await {
            Ok(Some(delivery))=>attach_delivery_params(&mut published_job,&delivery),
            Ok(None)=>{},
            Err(error)=>return (StatusCode::SERVICE_UNAVAILABLE,Json(serde_json::json!({"error":format!("published result awaits durable Delivery: {error}")}))).into_response(),
        }
    }
    if info.kind == LeaseKind::Post {
        if let Some(path) = final_publication.as_deref() {
            if let Err(error) = record_pp_history(&s, &published_job, accepted_at_ms, path).await {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({"error":error})),
                )
                    .into_response();
            }
        }
    }
    let history_recorded_by_authority = info.kind == LeaseKind::Post && s.history.is_some();
    tracing::info!(job = job_id.0, node = %req.node, "job completed remotely");
    let next = serde_json::to_string(&published_job).expect("publication job serializes");
    let key = format!(
        "publication-projection:{}:{current_revision}",
        req.result_id
    );
    match s
        .control
        .as_ref()
        .unwrap()
        .apply_command(
            &key,
            u64::from(job_id.0),
            current_revision,
            r#"{"op":"publication_projection"}"#,
            &next,
            false,
        )
        .await
    {
        Ok(MutationOutcome::Applied { .. } | MutationOutcome::Duplicate { .. }) => {}
        Ok(MutationOutcome::Conflict) => {
            let _ = s.restore_control_projection().await;
            return (StatusCode::CONFLICT,Json(serde_json::json!({"error":"current intent changed while finalizing publication"}))).into_response();
        }
        Err(error) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error":error})),
            )
                .into_response();
        }
    }
    if let Err(error) = s.restore_control_projection().await {
        tracing::error!(job=job_id.0,%error,"durable completion projection awaits replay");
    }
    // Delegation is a local scheduler projection, not durable intent. Its
    // completed lease must release this slot after reloading current control.
    let _ = s.engine.set_delegated(job_id, None).await;
    s.leases.lock().unwrap().remove(&req.lease_id);
    s.apply_local_budgets().await;
    Json(CompleteResponse {
        final_dir: final_publication.map(|p| p.to_string_lossy().into_owned()),
        ok: true,
        durable_receipt: Some(req.receipt_id),
        accepted_at_unix_ms: Some(accepted_at_ms),
        history_recorded_by_authority,
        cleanup_only: !has_payload,
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
        && receipt.lease_revision >= request.token.revision
        && receipt.lease_expiry_ms >= request.token.expires_at_unix_ms
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

fn verify_generation(
    shared_dir: &Path,
    result_ref: &str,
    result_id: &str,
    job_incarnation: Option<&str>,
    fence: u64,
    expected_job: &nzbd_types::Job,
) -> Result<Vec<nzbd_state::artifacts::ExternalGenerationFile>, String> {
    let allowed = std::fs::canonicalize(shared_dir.join(".nzbd-cluster/generations"))
        .map_err(|error| format!("canonicalize generation root: {error}"))?;
    let selected = std::fs::canonicalize(result_ref)
        .map_err(|error| format!("canonicalize generation: {error}"))?;
    if !selected.starts_with(&allowed) {
        return Err("generation escaped shared namespace".into());
    }
    let manifest: crate::worker::GenerationManifest = serde_json::from_slice(
        &crate::worker::read_bounded_file(&selected.join("manifest.json"), MAX_DESCRIPTOR_BYTES)?,
    )
    .map_err(|error| format!("decode generation manifest: {error}"))?;
    if manifest.result_id != result_id
        || !manifest.identity_matches()
        || manifest.fence != fence
        || job_incarnation.is_some_and(|expected| expected != manifest.job_incarnation)
    {
        return Err("generation identity mismatch".into());
    }
    let job_bytes =
        crate::worker::read_bounded_file(&selected.join("job.json"), MAX_DESCRIPTOR_BYTES)?;
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
    for expected in &manifest.files {
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
    Ok(manifest
        .files
        .into_iter()
        .map(|file| nzbd_state::artifacts::ExternalGenerationFile {
            path: file.path,
            bytes: file.bytes,
            sha256: file.sha256,
        })
        .collect())
}

/// Materialize a selected immutable generation. Existing output is moved to
/// a uniquely named recoverable sibling and is never deleted here. A marker
/// makes retries idempotent without repeatedly moving the selected output.
fn cluster_publication_name(original: &str, result_id: &str, lifecycle: bool) -> String {
    if lifecycle {
        format!(
            "{original}.runner-{}",
            result_id.strip_prefix("sha256:").unwrap_or(result_id)
        )
    } else {
        original.to_owned()
    }
}

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
            if is_leader && authority_ready && !shared.authority_ready(){authority_ready=false;}
            if is_leader && !was_leader {
                // Taking office: discard leases inherited from the old view.
                // New leases arrive via worker heartbeats or fresh grants.
                shared.authority_ready.store(false, Ordering::Release);
                shared.leases.lock().unwrap().clear();
                if let Err(error) = shared.restore_budget_state().await {
                    tracing::warn!(%error, "provider budget restore awaits stable authority");
                    tokio::time::sleep(shared.cfg.lease_interval).await;
                    continue;
                }
            }
            if is_leader && !authority_ready {
                // Retry a refused adoption while this node remains leader.
                // The engine leaves both local and shared state unchanged on
                // refusal, so an operator can repair the snapshot in place
                // without restarting the daemon or forcing an election flap.
                match shared.engine.adopt_authority().await {
                    Ok(()) => {
                        // Preserve pending actor transitions before replacing the local projection.
                        let _serial = shared.mutation_serial.lock().await;
                        if let Err(error)=crate::control_recovery::reconcile(&shared).await {tracing::error!(%error,"automatic control adoption incomplete");continue;}
                        if let Err(error)=crate::admissions::initialize(&shared).await {tracing::error!(%error,"admission authority adoption incomplete");tokio::time::sleep(shared.cfg.lease_interval).await;continue;}
                        let replicated = match &shared.control {
                            Some(_) => crate::control_recovery::projection(&shared).await.map(|jobs|jobs.into_iter().map(|job|(job,0,String::new())).collect::<Vec<_>>()),
                            None => Err("replicated control unavailable".into()),
                        };
                        match replicated {
                            Ok(rows) => {
                                let jobs = rows.into_iter().map(|(job, _, _)| job).collect();
                                match shared.engine.adopt_replicated_authority(jobs).await {
                                    Ok(()) => {
                                        if let Err(error)=crate::admissions::adopt(&shared).await {tracing::error!(%error,"admission catalog projection incomplete");continue;}
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
                shared.control_proofs.invalidate();
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
    control
        .initialize_delivery_identity(&s.engine.artifacts().installation, &s.cfg.node_name)
        .await?;
    // A single bounded page per pass; interrupted adoption resumes its trusted cursor.
    if let Err(error) = crate::delivery::migrate_local_deliveries(s).await {
        tracing::warn!(%error,"legacy Delivery catalog adoption awaits original owner");
    }
    let mut cursor = 0;
    loop {
        let (rows, next) = control.publication_results_page(None, cursor, 16).await?;
        if next == cursor {
            break;
        }
        cursor = next;
        for (result_id, result_ref, job, fence, accepted_at_ms, kind) in rows {
            let ready = control
                .cluster_delivery(&cluster_delivery_id(&result_id))
                .await?;
            if ready.is_none()
                && control
                    .selected_publication_job(job.id.0, &result_id)
                    .await?
                    .is_none()
            {
                // Selection receipts remain cleanup evidence. A cancelled or
                // superseded result cannot regain publication after restart.
                continue;
            }
            let Some((_, original_dir)) = job
                .params
                .iter()
                .find(|(key, _)| key == "*Cluster:original-dir")
            else {
                continue;
            };
            let dest_dir = s.publication_root(&job, kind == "post");
            let original_dir = cluster_publication_name(
                original_dir,
                &result_id,
                kind == "post" && job.delivery_lifecycle,
            );
            let final_path = dest_dir.join(&original_dir);
            let ready = control
                .cluster_delivery(&cluster_delivery_id(&result_id))
                .await?;
            let cleanup_only = if let Some(record) = &ready {
                record.cleanup_only
            } else {
                let manifest_path = Path::new(&result_ref).join("manifest.json");
                tokio::task::spawn_blocking(move || -> Result<bool, String> {
                    let manifest: crate::worker::GenerationManifest = serde_json::from_slice(
                        &crate::worker::read_bounded_file(&manifest_path, MAX_DESCRIPTOR_BYTES)?,
                    )
                    .map_err(|error| error.to_string())?;
                    Ok(manifest.files.is_empty())
                })
                .await
                .map_err(|error| error.to_string())??
            };
            if ready.is_none() && !cleanup_only {
                let publication = nzbd_state::artifacts::ExternalGeneration {
                    result_id: result_id.clone(),
                    fence,
                    source: PathBuf::from(&result_ref).join("files"),
                    files: Vec::new(),
                };
                let mut selected = job.clone();
                selected.dir_name = original_dir.clone();
                let history = (kind == "post" && s.history.is_some())
                    .then(|| pp_history_entry(&selected, accepted_at_ms, &final_path));
                s.engine
                    .materialize_generation(
                        selected,
                        dest_dir,
                        final_path.clone(),
                        publication,
                        history,
                        false,
                    )
                    .await
                    .map_err(|error| format!("publication recovery remains pending: {error}"))?;
            }
            if kind == "post" {
                let mut job = job;
                if cleanup_only {
                    job.params
                        .push(("*Cluster:cleanup-only".into(), "1".into()));
                }
                let receipt_id = format!("complete:work/job/{}/post:{fence}:{result_id}", job.id.0);
                if let Some(delivery) = ensure_cluster_delivery(
                    s,
                    &job,
                    &result_id,
                    &result_ref,
                    &receipt_id,
                    accepted_at_ms,
                    &final_path,
                )
                .await?
                {
                    attach_delivery_params(&mut job, &delivery);
                }
                record_pp_history(s, &job, accepted_at_ms, &final_path).await?;
            }
        }
    }
    Ok(())
}

fn cluster_delivery_id(result_id: &str) -> String {
    format!("cluster-delivery:{result_id}")
}

fn attach_delivery_params(job: &mut nzbd_types::Job, delivery: &nzbd_state::artifacts::Delivery) {
    for (key, value) in [
        ("Delivery:Id", delivery.delivery_id.clone()),
        ("Delivery:Path", delivery.publication_path.clone()),
        (
            "Delivery:Generation",
            delivery.publication_generation.clone(),
        ),
        ("Delivery:ManifestDigest", delivery.manifest_digest.clone()),
        ("Delivery:Custody", "curator".into()),
        (
            "Delivery:SourceArtifactId",
            delivery.source_artifact_id.clone(),
        ),
        (
            "Delivery:SourceGeneration",
            delivery.source_generation.clone(),
        ),
        (
            "Delivery:SourceCleanup",
            delivery.source_cleanup.status.clone(),
        ),
    ] {
        job.params.retain(|(existing, _)| existing != key);
        job.params.push((key.into(), value));
    }
}

async fn ensure_cluster_delivery(
    s: &LeaderShared,
    job: &nzbd_types::Job,
    result_id: &str,
    result_ref: &str,
    receipt_id: &str,
    accepted_at_ms: i64,
    final_path: &Path,
) -> Result<Option<nzbd_state::artifacts::Delivery>, String> {
    if !job.delivery_lifecycle {
        return Ok(None);
    }
    let control = s.control.as_ref().ok_or("replicated control unavailable")?;
    let _serial = s.delivery_serial.lock().await;
    let id = cluster_delivery_id(result_id);
    if let Some(record) = control.cluster_delivery(&id).await? {
        return Ok((!record.cleanup_only).then_some(record.delivery));
    }
    if control.delivery_meta("local_imported").await?.is_none() {
        return Err("legacy Delivery cursor adoption is still pending; retry publication after catalog recovery".into());
    }
    let manifest: crate::worker::GenerationManifest =
        serde_json::from_slice(&crate::worker::read_bounded_file(
            &Path::new(result_ref).join("manifest.json"),
            MAX_DESCRIPTOR_BYTES,
        )?)
        .map_err(|e| e.to_string())?;
    if manifest.result_id != result_id || !manifest.identity_matches() {
        return Err("sealed copy metadata changed after selection".into());
    }
    let cleanup_only = manifest.files.is_empty();
    let sealed_job = {
        let path = Path::new(result_ref).join("job.json");
        let expected = manifest.job_sha256.clone();
        tokio::task::spawn_blocking(move || -> Result<nzbd_types::Job, String> {
            use sha2::{Digest, Sha256};
            let bytes = crate::worker::read_bounded_file(&path, MAX_DESCRIPTOR_BYTES)?;
            if format!("{:x}", Sha256::digest(&bytes)) != expected {
                return Err("sealed timing source changed".into());
            }
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())
        })
        .await
        .map_err(|error| error.to_string())??
    };
    let (_, selected_revision) = control
        .selected_publication_job(job.id.0, result_id)
        .await?
        .ok_or("selected publication was cancelled before READY")?;
    // New main materialization commits an exact Inventory operation and
    // selected owned artifact, rather than writing a legacy payload marker.
    if !cleanup_only {
        let inventory = s.engine.artifacts();
        let target = final_path.to_path_buf();
        let root = target
            .parent()
            .ok_or("materialized publication has no root")?
            .to_path_buf();
        let generation = nzbd_state::artifacts::ExternalGeneration {
            result_id: result_id.to_owned(),
            fence: manifest.fence,
            source: Path::new(result_ref).join("files"),
            files: manifest
                .files
                .iter()
                .map(|file| nzbd_state::artifacts::ExternalGenerationFile {
                    path: file.path.clone(),
                    bytes: file.bytes,
                    sha256: file.sha256.clone(),
                })
                .collect(),
        };
        let job_id = job.id.0;
        let matched = tokio::task::spawn_blocking(move || {
            inventory.materialized_generation_matches(job_id, &root, &target, &generation)
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
        if !matched {
            return Err("exact durable materialization receipt or selected publication identity is unavailable".into());
        }
    }
    let mut copies = Vec::new();
    let mut copy_cursor = 0;
    loop {
        let (rows, next) = control
            .publication_results_page(Some(job.id.0), copy_cursor, 16)
            .await?;
        if next == copy_cursor {
            break;
        }
        copy_cursor = next;
        for (copy_result, copy_ref, _copy_job, _, _, _) in rows {
            let copy_manifest_bytes = crate::worker::read_bounded_file(
                &Path::new(&copy_ref).join("manifest.json"),
                MAX_DESCRIPTOR_BYTES,
            )?;
            let copy_manifest: crate::worker::GenerationManifest =
                serde_json::from_slice(&copy_manifest_bytes).map_err(|error| error.to_string())?;
            if copy_manifest.result_id != copy_result || !copy_manifest.identity_matches() {
                return Err("accepted copy metadata changed".into());
            }
            if copy_manifest.job_incarnation != manifest.job_incarnation {
                continue;
            }
            if let Some(copy) = copy_manifest.artifact_copy {
                let raw_files = (!copy.selected).then(|| {
                    copy_manifest
                        .files
                        .iter()
                        .map(|file| nzbd_state::artifacts::DeliveryFile {
                            path: file.path.clone(),
                            size_bytes: file.bytes,
                            sha256: file.sha256.clone(),
                        })
                        .collect::<Vec<_>>()
                });
                let copy_key = format!(
                    "artifact:{}:{}:{}",
                    copy.storage_instance_id,
                    copy.publication.artifact,
                    copy.publication.generation
                );
                if !copies
                    .iter()
                    .any(|existing: &crate::delivery::CopyObligation| existing.id == copy_key)
                {
                    copies.push(crate::delivery::CopyObligation {
                        id: copy_key,
                        owner_node: copy_manifest.owner_node_id.clone(),
                        artifact: Some(copy),
                        local_delivery_id: None,
                        result_ref: None,
                        result_id: None,
                        directory_identity: None,
                        deletion_files: raw_files,
                        claim_id: None,
                        attempts: 0,
                        next_retry_at: None,
                        status: "pending".into(),
                        error: None,
                    });
                }
            }
            let mut deletion_files = copy_manifest
                .files
                .iter()
                .map(|file| nzbd_state::artifacts::DeliveryFile {
                    path: format!("files/{}", file.path),
                    size_bytes: file.bytes,
                    sha256: file.sha256.clone(),
                })
                .collect::<Vec<_>>();
            use sha2::{Digest, Sha256};
            deletion_files.push(nzbd_state::artifacts::DeliveryFile {
                path: "manifest.json".into(),
                size_bytes: copy_manifest_bytes.len() as u64,
                sha256: format!("{:x}", Sha256::digest(&copy_manifest_bytes)),
            });
            let copy_job_bytes = crate::worker::read_bounded_file(
                &Path::new(&copy_ref).join("job.json"),
                MAX_DESCRIPTOR_BYTES,
            )?;
            if format!("{:x}", Sha256::digest(&copy_job_bytes)) != copy_manifest.job_sha256 {
                return Err("accepted copy job changed before Delivery".into());
            }
            let sealed_job: nzbd_types::Job =
                serde_json::from_slice(&copy_job_bytes).map_err(|error| error.to_string())?;
            if let Some((_, value)) = sealed_job
                .params
                .iter()
                .find(|(key, _)| key == crate::delivery::AUTHORITY_COPY_PARAM)
            {
                let authority: crate::delivery::AuthoritySourceCopy =
                    serde_json::from_str(value).map_err(|error| error.to_string())?;
                if authority.copy.job_id != job.id.0
                    || authority.copy.selected
                    || authority.owner_node.is_empty()
                {
                    return Err("sealed admission copy identity is invalid".into());
                }
                let key = format!(
                    "artifact:{}:{}:{}",
                    authority.copy.storage_instance_id,
                    authority.copy.publication.artifact,
                    authority.copy.publication.generation
                );
                if !copies.iter().any(|copy| copy.id == key) {
                    copies.push(crate::delivery::CopyObligation {
                        id: key,
                        owner_node: authority.owner_node,
                        artifact: Some(authority.copy),
                        local_delivery_id: None,
                        result_ref: None,
                        result_id: None,
                        directory_identity: None,
                        deletion_files: Some(
                            copy_manifest
                                .files
                                .iter()
                                .map(|file| nzbd_state::artifacts::DeliveryFile {
                                    path: file.path.clone(),
                                    size_bytes: file.bytes,
                                    sha256: file.sha256.clone(),
                                })
                                .collect(),
                        ),
                        claim_id: None,
                        attempts: 0,
                        next_retry_at: None,
                        status: "pending".into(),
                        error: None,
                    });
                }
            }
            deletion_files.push(nzbd_state::artifacts::DeliveryFile {
                path: "job.json".into(),
                size_bytes: copy_job_bytes.len() as u64,
                sha256: copy_manifest.job_sha256.clone(),
            });
            copies.push(crate::delivery::CopyObligation {
            id:format!("generation:{copy_result}"),owner_node:copy_manifest.owner_node_id,
            artifact:None,local_delivery_id:None,result_ref:Some(copy_ref),result_id:Some(copy_result),directory_identity:copy_manifest.directory_identity,deletion_files:Some(deletion_files),claim_id:None,attempts:0,next_retry_at:None,
            status:if copy_manifest.directory_identity.is_some(){"pending"}else{"review_required"}.into(),
            error:copy_manifest.directory_identity.is_none().then(||"legacy generation lacks owned directory identity; inspect this exact copy before cleanup".into()),
        });
        }
    }
    copies.sort_by(|left, right| left.id.cmp(&right.id));
    let mut files = manifest
        .files
        .iter()
        .map(|file| nzbd_state::artifacts::DeliveryFile {
            path: file.path.clone(),
            size_bytes: file.bytes,
            sha256: file.sha256.clone(),
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
    use sha2::{Digest, Sha256};
    let mut canonical = b"runner-manifest-v1\n".to_vec();
    for file in &files {
        canonical.extend_from_slice(
            format!(
                "{}:{}\n{}\n{}\n",
                file.path.len(),
                file.path,
                file.size_bytes,
                file.sha256
            )
            .as_bytes(),
        );
    }
    let outcome = job
        .params
        .iter()
        .find(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
        .map(|(_, value)| value.as_str())
        .unwrap_or("SUCCESS");
    let source = manifest
        .artifact_copy
        .as_ref()
        .map(|copy| copy.source.clone());
    let keep = job
        .params
        .iter()
        .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1");
    let manifest_digest = format!("{:x}", Sha256::digest(canonical));
    if !cleanup_only {
        // Contract §2.5: a directory without `.runner-manifest.json` is not a
        // publication. The leader landed the sealed payload in `final_path`;
        // the manifest goes in last, through the single-node writer, and
        // its digest is the row's. The payload was copied, not renamed.
        let runner_manifest = nzbd_state::artifacts::RunnerManifest::new(
            result_id.to_string(),
            job.id.0,
            job.params
                .iter()
                .find(|(key, _)| key == "monarr-transfer")
                .map(|(_, value)| value.clone()),
            accepted_at_ms / 1000,
            true,
            files
                .iter()
                .map(|file| nzbd_state::artifacts::ManifestFile {
                    path: file.path.clone(),
                    bytes: file.size_bytes,
                    sha256: file.sha256.clone(),
                })
                .collect(),
        );
        if runner_manifest.digest != manifest_digest {
            return Err("cluster publication manifest digest disagrees with its row".into());
        }
        let dir = final_path.to_path_buf();
        tokio::task::spawn_blocking(move || {
            nzbd_state::artifacts::write_runner_manifest(&dir, runner_manifest)
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| format!("cluster publication manifest: {error}"))?;
    }
    let delivery:nzbd_state::artifacts::Delivery=serde_json::from_value(serde_json::json!({
        "protocolVersion":1,"sequence":0,"deliveryId":id,"publicationGeneration":result_id,
        "artifactId":format!("cluster-publication:{result_id}"),"sourceArtifactId":source.as_ref().map(|source|source.artifact.as_str()).unwrap_or(""),
        "sourceGeneration":source.as_ref().map(|source|source.generation.as_str()).unwrap_or(""),"jobId":job.id.0,
        "runnerInstanceId":control.delivery_instance().await?,
        "requestToken":job.params.iter().find(|(key,_)|key=="monarr-transfer").map(|(_,value)|value),
        "keepSourceOnFailure":keep,"terminalOutcome":outcome,
        "failureStage":job.params.iter().find(|(key,_)|key=="Failure:Stage").map(|(_,value)|value),
        "failureCause":job.params.iter().find(|(key,_)|key=="Failure:Reason"||key=="Failure:Files").map(|(_,value)|value),
        "publicationPath":final_path.to_string_lossy(),"manifestDigest":manifest_digest,
        "files":files,"fileOutcomes":manifest.file_outcomes,"readyAt":accepted_at_ms/1000,
        "runnerStages":{"nodeId":"unrecorded","clockDomain":"unrecorded","spans":sealed_job.stages.iter().take(128).collect::<Vec<_>>()},
        "lifecycleTiming":nzbd_engine::frozen_lifecycle_timing(&sealed_job),
        "sourceCleanup":{"status":"pending","error":null,"attempts":0,"nextRetryAt":null,"blockedSince":null,"warning":null}
    })).map_err(|e|e.to_string())?;
    let record = control
        .commit_delivery_selected(
            &crate::delivery::ClusterDelivery {
                revision: 0,
                delivery,
                receipt_id: receipt_id.into(),
                result_id: result_id.into(),
                copies,
                cleanup_only,
                source_disposition: manifest.source_disposition,
            },
            Some((job.id.0, selected_revision)),
        )
        .await?;
    Ok((!record.cleanup_only).then_some(record.delivery))
}

fn pp_history_entry(
    job: &nzbd_types::Job,
    accepted_at_ms: i64,
    final_path: &Path,
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
        final_dir: (!job
            .params
            .iter()
            .any(|(key, value)| key == "*Cluster:cleanup-only" && value == "1"))
        .then(|| final_path.to_string_lossy().into_owned()),
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

async fn record_pp_history(
    s: &LeaderShared,
    job: &nzbd_types::Job,
    accepted_at_ms: i64,
    final_path: &Path,
) -> Result<(), String> {
    let Some(history) = &s.history else {
        return Ok(());
    };
    // Contract §2.1 (review #280-6): a worker's post-processing result is the
    // job's terminal, recorded by the leader — which owns the shared history
    // and the event stream — through the same chain as every other
    // terminal: the `job_terminals` row with its salvage obligation, the
    // history row, the queue stamp and `job_pp_finished`. Idempotent: a
    // replay returns the stored row.
    let entry = pp_history_entry(job, accepted_at_ms, final_path);
    record_remote_terminal(&s.engine, history.clone(), job, entry).await
}

/// Record a remotely post-processed job's terminal on `engine` with the
/// node's history store (contract §2.1): the leader when it holds the
/// shared history, otherwise the executing worker that writes the history
/// row — exactly one node, so exactly one terminal per job.
pub(crate) async fn record_remote_terminal(
    engine: &nzbd_engine::EngineHandle,
    history: Arc<nzbd_state::history::HistoryDb>,
    job: &nzbd_types::Job,
    entry: nzbd_state::HistoryEntry,
) -> Result<(), String> {
    let mut stamped = job.clone();
    if !stamped
        .params
        .iter()
        .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
    {
        stamped
            .params
            .push((nzbd_types::PP_DONE_PARAM.into(), entry.status.clone()));
    }
    let publication = cluster_publication(&stamped, entry.final_dir.as_deref());
    let salvage_publication = stamped
        .params
        .iter()
        .any(|(key, _)| key == "Delivery:Id")
        .then(|| publication.clone())
        .flatten();
    let mut terminal =
        nzbd_engine::terminal::terminal_from_stamped(&stamped, entry, publication)
            .ok_or_else(|| "post-processing result carries no terminal stamp".to_string())?;
    let failed = terminal.outcome == nzbd_types::TerminalOutcome::Failed;
    if failed {
        // In cluster mode the executing worker has already salvaged (its
        // result carries the salvage Delivery when anything verified) and
        // the source copies are disposed through the cluster cleanup ledger
        // (`sourceCopies`, one owner per copy); nothing is owed on the
        // leader's own inventory.
        terminal.salvage = if salvage_publication.is_some() {
            nzbd_state::artifacts::SalvageState::Published
        } else {
            nzbd_state::artifacts::SalvageState::None
        };
    }
    let job_id = job.id;
    match engine.record_terminal_in(terminal, Some(history)).await {
        Ok(Some(row)) => {
            // First recording only (a replay finds the disposal noted).
            if failed && row.disposal.is_none() {
                let inventory = engine.artifacts();
                let _ = tokio::task::spawn_blocking(move || {
                    inventory.record_disposal(job_id.0, "cluster source cleanup ledger")
                })
                .await;
                if let Some(publication) = salvage_publication {
                    engine.emit_publication_ready(job_id, publication);
                }
            }
            Ok(())
        }
        Ok(None) => Err("the terminal record is not durable yet".into()),
        Err(error) => Err(error.to_string()),
    }
}

/// The `publication` of a remotely completed job: its Delivery's when one
/// was attached, else the materialized final directory.
fn cluster_publication(
    job: &nzbd_types::Job,
    final_dir: Option<&str>,
) -> Option<nzbd_types::Publication> {
    let param = |key: &str| {
        job.params
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.clone())
    };
    let path = param("Delivery:Path").or_else(|| final_dir.map(str::to_string))?;
    let cross_volume = {
        use std::os::unix::fs::MetadataExt;
        // The immutable worker generation the publication was materialized from.
        let original = param("*Cluster:result-ref");
        matches!(
            (std::fs::metadata(&path), original.map(std::fs::metadata)),
            (Ok(a), Some(Ok(b))) if a.dev() != b.dev()
        )
    };
    Some(nzbd_types::Publication {
        path,
        generation: param("Delivery:Generation").unwrap_or_default(),
        manifest_digest: param("Delivery:ManifestDigest").unwrap_or_default(),
        files: job
            .files
            .iter()
            .filter(|file| file.finalized)
            .count()
            .try_into()
            .unwrap_or(u32::MAX),
        cross_volume,
    })
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

struct SelectedDeliveryReady(bool);
impl From<&mut hiqlite::Row<'_>> for SelectedDeliveryReady {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self(row.get::<i64>("ready") != 0)
    }
}
async fn selected_result_has_delivery(control: &ControlStore, job: JobId) -> Result<bool, String> {
    // The exact selected result's immutable ledger is sufficient for both a
    // public READY and terminal empty-payload private cleanup; never demand
    // a public empty manifest. Primary-key lookup avoids historical scans.
    control.client.query_consistent_map::<SelectedDeliveryReady,_>(
        "SELECT EXISTS(SELECT 1 FROM control_jobs j JOIN cluster_deliveries d ON d.id='cluster-delivery:'||j.result_id WHERE j.job_id=?1 AND j.deleted=0) AS ready",
        hiqlite::params!(i64::from(job.0)),
    ).await.map(|rows|rows.into_iter().next().is_some_and(|row|row.0)).map_err(|error|error.to_string())
}

async fn checked_schedule_job(
    s: &LeaderShared,
    summary: &nzbd_engine::JobSummary,
) -> Option<nzbd_types::Job> {
    if summary.verification.is_some() {
        return None;
    }
    match s.engine.export_job(summary.id).await {
        Ok(Some(job)) if job.status == summary.status => Some(job),
        _ => None,
    }
}

async fn schedule(s: &Arc<LeaderShared>) {
    let _serial = s.mutation_serial.lock().await;
    if let Err(error) = crate::control_recovery::reconcile(s).await {
        tracing::warn!(%error,"scheduler awaits automatic control replication");
        return;
    }
    let source_pending = match crate::source_facts::reconcile(s).await {
        Ok(pending) => pending,
        Err(error) => {
            tracing::warn!(%error,"scheduler awaits durable source custody");
            return;
        }
    };
    let mut requires_delivery = HashSet::new();
    let durable_status: HashMap<JobId, JobStatus> = match &s.control {
        Some(control) => match control.load_jobs().await {
            Ok(rows) => rows
                .into_iter()
                .map(|(job, _, _)| {
                    if job.delivery_lifecycle {
                        requires_delivery.insert(job.id);
                    }
                    (job.id, job.status)
                })
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
                && !source_pending.contains(&job.id)
                && matches!(job.status, JobStatus::Queued)
                && durable_status.get(&job.id) == Some(&JobStatus::Queued)
        }) {
            if let Some(job) = checked_schedule_job(s, summary).await {
                if !job.held() && crate::range::is_split_candidate(&job) {
                    if !s
                        .engine
                        .set_delegated(summary.id, Some(crate::range::RANGE_ASSIGNEE.into()))
                        .await
                        .is_ok_and(|accepted| accepted)
                    {
                        continue;
                    }
                    split_jobs.insert(summary.id);
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
            && !source_pending.contains(&j.id)
            && matches!(j.status, JobStatus::Completed | JobStatus::Failed)
            && durable_status.get(&j.id) == Some(&j.status)
        {
            // Contract §2.6 (review #280-6): a row whose terminal is not yet
            // out, or whose failed terminal still owes salvage or disposal,
            // stays queued — salvage and disposal read it.
            let inventory = s.engine.artifacts();
            let id = j.id.0;
            let settled = tokio::task::spawn_blocking(move || match inventory.terminal(id) {
                Ok(None) => true, // stamped before job_terminals existed
                Ok(Some(terminal)) => {
                    terminal.emitted
                        && (terminal.outcome != nzbd_types::TerminalOutcome::Failed
                            || terminal.disposal.is_some())
                }
                Err(_) => false,
            })
            .await
            .unwrap_or(false);
            if !settled {
                continue;
            }
            let Some(checked) = checked_schedule_job(s, j).await else {
                continue;
            };
            let Some(control) = &s.control else { continue };
            let Ok(Some((durable, revision, _))) = control.current_job_intent(j.id.0).await else {
                continue;
            };
            if !checked
                .params
                .iter()
                .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
                || checked.status != durable.status
                || checked.control() != durable.control()
                || !crate::control_recovery::source_intent_matches(&checked, &durable)
                    .unwrap_or(false)
            {
                continue;
            }
            // Main's durable materialization can expose PP_DONE locally
            // before complete_operation commits its Delivery. Retirement must
            // not tombstone that still-selected result in the intervening gap.
            if requires_delivery.contains(&j.id) {
                let ready = match &s.control {
                    Some(control) => selected_result_has_delivery(control, j.id).await,
                    None => Ok(false),
                };
                if !matches!(ready, Ok(true)) {
                    tracing::debug!(job=j.id.0,error=?ready.err(),"finished lifecycle job awaits selected-result Delivery before retirement");
                    continue;
                }
            }
            let committed = match &s.control {
                Some(control) => {
                    let fences = HashMap::from([(u64::from(j.id.0), revision)]);
                    control
                        .commit_job_delta_guarded_with_removals(
                            &[(u64::from(j.id.0), None)],
                            &fences,
                            &[],
                            Some(s.authority_identity()),
                            &std::collections::BTreeMap::from([(
                                j.id.0,
                                crate::cancellation::RemovalMode::CompletedRetirement,
                            )]),
                        )
                        .await
                        .is_ok()
                }
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
            || source_pending.contains(&job.id)
            || split_jobs.contains(&job.id)
            || !matches!(job.status, JobStatus::Queued)
            || durable_status.get(&job.id) != Some(&JobStatus::Queued)
        {
            continue;
        }
        if !download_targets.iter().any(|target| target.1 > 0) {
            break;
        }
        let Some(checked) = checked_schedule_job(s, job).await else {
            continue;
        };
        if checked.held() {
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
        let node = slot.0.clone();
        if !slot.4
            && s.engine
                .set_delegated(job.id, Some(node.clone()))
                .await
                .is_ok_and(|accepted| accepted)
        {
            slot.1 -= 1;
            slot.2 += 1;
            tracing::info!(job = job.id.0, %node, "delegating job by weighted load");
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
            || source_pending.contains(&job.id)
            || durable_status.get(&job.id) != Some(&JobStatus::Completed)
            || job.pp_done
            || job.assigned_node.is_some()
        {
            continue;
        }
        let Some(slot) = pp_targets.iter_mut().find(|target| target.1 > 0) else {
            break;
        };
        let Some(checked) = checked_schedule_job(s, job).await else {
            continue;
        };
        if checked.held()
            || checked
                .params
                .iter()
                .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
        {
            continue;
        }
        let node = slot.0.clone();
        if s.engine
            .set_delegated(job.id, Some(node.clone()))
            .await
            .is_ok_and(|accepted| accepted)
        {
            slot.1 -= 1;
            slot.3 += 1;
            tracing::info!(job = job.id.0, %node, "assigning post-processing");
        }
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
            delivery_lifecycle: false,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn scheduler_progresses_verified_jobs_beside_unbound_and_terminal_fenced_rows() {
        let temp = tempfile::tempdir().unwrap();
        let raft = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let api = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let cfg = ClusterConfig {
            cluster_id: "per-job-custody".into(),
            node_name: "authority".into(),
            shared_dir: temp.path().join("shared"),
            advertise_url: "http://127.0.0.1:1".into(),
            secret: "secret".into(),
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            pp_slots: 0,
            lease_interval: std::time::Duration::from_millis(100),
            takeover_after: std::time::Duration::from_secs(1),
            worker_ttl: std::time::Duration::from_secs(30),
            control_dir: temp.path().join("control"),
            control_node_id: 1,
            control_raft_bind: raft.local_addr().unwrap().to_string(),
            control_api_bind: api.local_addr().unwrap().to_string(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: Vec::new(),
            torrent_payload_roots: Vec::new(),
        };
        drop(raft);
        drop(api);
        let dest = temp.path().join("dest");
        let runtime = crate::ClusterRuntime::start(
            cfg,
            Vec::new(),
            Tuning {
                min_free_disk_bytes: 0,
                ..Default::default()
            },
            dest.clone(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while !runtime.leader_shared.authority_ready() {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let shared = &runtime.leader_shared;
        let control = runtime.control.as_ref().unwrap();
        let serial = shared.mutation_serial.lock().await;
        let inventory = runtime.engine.artifacts();
        let mut blocked = test_job(1);
        blocked.name = "blocked".into();
        blocked.dir_name = "blocked".into();
        blocked.status = JobStatus::Queued;
        inventory
            .allocate_for_delivery(1, &dest, &dest.join("blocked"), false, false)
            .unwrap();
        let payload = b"actual complete bytes without expected publication length";
        std::fs::write(dest.join("blocked/payload.bin"), payload).unwrap();
        blocked.files.push(nzbd_types::FileEntry {
            id: nzbd_types::FileId(1),
            subject: "payload".into(),
            filename: "payload.bin".into(),
            filename_confirmed: true,
            is_par2: false,
            paused: false,
            groups: Vec::new(),
            date: None,
            crc32: Some(crc32fast::hash(payload)),
            finalized: true,
            segments: vec![nzbd_types::Segment {
                message_id: "blocked@example".into(),
                number: 1,
                size: payload.len() as u32,
                state: nzbd_types::SegmentState::Done {
                    offset: 0,
                    len: payload.len() as u32,
                    crc: crc32fast::hash(payload),
                },
            }],
        });
        let mut ready = test_job(2);
        ready.status = JobStatus::Queued;
        ready.name = "ready".into();
        ready.dir_name = "ready".into();
        inventory
            .allocate_for_delivery(2, &dest, &dest.join("ready"), false, false)
            .unwrap();
        let mut terminal = test_job(3);
        terminal.status = JobStatus::Failed;
        terminal.name = "fenced-terminal".into();
        terminal.dir_name = "fenced-terminal".into();
        terminal.params = vec![
            (nzbd_types::PP_DONE_PARAM.into(), "FAILURE/HEALTH".into()),
            (
                "*Recovery:failed-cleanup".into(),
                "[\"missing-source\",\"missing-generation\",\"missing-operation\"]".into(),
            ),
        ];
        let mut post = test_job(4);
        post.name = "ready-post".into();
        post.dir_name = "ready-post".into();
        inventory
            .allocate_for_delivery(4, &dest, &dest.join("ready-post"), false, false)
            .unwrap();
        let rows = vec![blocked, ready, terminal, post];
        control
            .commit_job_delta(
                &rows
                    .iter()
                    .map(|job| (u64::from(job.id.0), Some(job.clone())))
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
        runtime
            .engine
            .adopt_replicated_authority(rows)
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let snapshot = runtime.engine.snapshot();
            if snapshot
                .jobs
                .iter()
                .find(|job| job.id == JobId(1))
                .is_some_and(|job| {
                    job.verification.as_ref().is_some_and(|proof| {
                        proof.reason.as_deref() == Some("expected publication size is not recorded")
                    })
                })
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "real incomplete publication evidence must remain blocked: {snapshot:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(matches!(
            runtime.engine.export_job(JobId(1)).await,
            Err(nzbd_engine::EngineError::Lifecycle(_))
        ));
        let blocked_before = control.current_job_intent(1).await.unwrap().unwrap();
        let pending = crate::source_facts::reconcile(shared).await.unwrap();
        assert!(pending.contains(&JobId(1)));
        for id in [2, 4] {
            let (bound, _, _) = control.current_job_intent(id).await.unwrap().unwrap();
            assert!(
                bound
                    .params
                    .iter()
                    .any(|(key, _)| key == crate::delivery::AUTHORITY_COPY_PARAM),
                "ready owned source must get its real guarded custody binding"
            );
        }
        assert_eq!(
            control.current_job_intent(1).await.unwrap().unwrap().1,
            blocked_before.1
        );
        assert!(!control
            .current_job_intent(1)
            .await
            .unwrap()
            .unwrap()
            .0
            .params
            .iter()
            .any(|(key, _)| key == crate::delivery::AUTHORITY_COPY_PARAM));
        // The binding CAS restores the whole projection. Observe actual
        // export readiness before this test asks for a single scheduling pass.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            for id in [2, 4] {
                loop {
                    match runtime.engine.export_job(JobId(id)).await {
                        Ok(Some(_)) => break,
                        Err(nzbd_engine::EngineError::Lifecycle(reason))
                            if reason == "job verification or writer retirement is still in progress; retry later" => {
                                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                            }
                        other => panic!("ready source {id} must survive binding projection: {other:?}"),
                    }
                }
            }
        }).await.expect("bound sources must become checked-export ready within five seconds");
        drop(serial);
        let worker = NodeRecord {
            lifecycle_protocol: LIFECYCLE_PROTOCOL,
            name: "worker".into(),
            api_url: "http://worker.invalid".into(),
            download: true,
            post_process: true,
            max_download_jobs: 1,
            download_weight: 1,
            pp_weight: 1,
            active_download_jobs: 0,
            disk_guard_capable: true,
            disk_low: false,
            disk_guard_free_bytes: None,
            disk_guard_label: None,
            disk_guard_path: None,
            disk_guard_write_latched: false,
            disk_guard_all_roots_known: true,
            pp_slots: 1,
            rate_bps: 0,
            seq: 1,
        };
        shared
            .layout
            .write_json(&shared.layout.node_file("worker"), &worker)
            .unwrap();
        schedule(shared).await;
        let snapshot = runtime.engine.snapshot();
        for id in [2, 4] {
            assert_eq!(
                snapshot
                    .jobs
                    .iter()
                    .find(|job| job.id == JobId(id))
                    .unwrap()
                    .assigned_node
                    .as_deref(),
                Some("worker")
            );
        }
        for id in [1, 3] {
            assert!(snapshot
                .jobs
                .iter()
                .find(|job| job.id == JobId(id))
                .unwrap()
                .assigned_node
                .is_none());
            assert!(matches!(
                runtime.engine.export_job(JobId(id)).await,
                Err(nzbd_engine::EngineError::Lifecycle(_))
            ));
            assert!(
                control.current_job_intent(id).await.unwrap().is_some(),
                "metadata-only terminal stamp cannot authorize a tombstone"
            );
        }
        assert_eq!(
            std::fs::read(dest.join("blocked/payload.bin")).unwrap(),
            payload
        );
        let mut headers = HeaderMap::new();
        headers.insert(SECRET_HEADER, "secret".parse().unwrap());
        let response = work_poll(
            State(shared.clone()),
            headers,
            Json(PollRequest {
                lifecycle_protocol: LIFECYCLE_PROTOCOL,
                node: "worker".into(),
                owner_incarnation: "worker-boot".into(),
                storage_instance_id: "worker-storage".into(),
                start_ordering: 1,
                free_download_slots: 1,
                free_pp_slots: 1,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let response: PollResponse = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), MAX_RPC_BYTES)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(response.grants.len(), 1);
        assert_eq!(response.grants[0].job.id, JobId(4));
        assert_eq!(response.grants[0].kind, LeaseKind::Post);
        assert!(control
            .current_lease("work/job/1/download")
            .await
            .unwrap()
            .is_none());
        assert!(runtime.shutdown().await);
    }

    #[test]
    fn grant_allows_learned_journal_size_without_relaxing_durable_source_intent() {
        let mut durable = test_job(77);
        durable.status = JobStatus::Queued;
        durable.files.push(nzbd_types::FileEntry {
            id: nzbd_types::FileId(77),
            subject: "payload".into(),
            filename: "payload.bin".into(),
            filename_confirmed: true,
            is_par2: false,
            paused: false,
            groups: Vec::new(),
            date: None,
            crc32: None,
            finalized: false,
            segments: vec![nzbd_types::Segment {
                message_id: "payload@example".into(),
                number: 1,
                size: 100,
                state: nzbd_types::SegmentState::Pending,
            }],
        });
        let mut local = durable.clone();
        local.params.push(("*File:size:77".into(), "100".into()));
        // The exact hash still distinguishes new evidence for guarded writes.
        assert_ne!(
            crate::control_recovery::business_intent(&local).unwrap(),
            crate::control_recovery::business_intent(&durable).unwrap()
        );
        assert!(crate::control_recovery::grant_intent_matches(&local, &durable).unwrap());
        assert!(!crate::control_recovery::source_intent_matches(&durable, &local).unwrap());
        for (key, value) in [
            ("*File:size:78", "100"),
            ("*File:size:077", "100"),
            ("*File:size:77", "0"),
            ("*File:size:77", "bad"),
        ] {
            let mut invalid = durable.clone();
            invalid.params.push((key.into(), value.into()));
            assert!(!crate::control_recovery::source_intent_matches(&invalid, &durable).unwrap());
        }
        let mut duplicate = local.clone();
        duplicate
            .params
            .push(("*File:size:77".into(), "101".into()));
        assert!(!crate::control_recovery::source_intent_matches(&duplicate, &durable).unwrap());
        let mut changed_source = local.clone();
        changed_source.files[0].segments[0].message_id = "other@example".into();
        assert!(!crate::control_recovery::grant_intent_matches(&changed_source, &durable).unwrap());
        let mut changed_intent = local.clone();
        changed_intent.priority += 1;
        assert!(!crate::control_recovery::grant_intent_matches(&changed_intent, &durable).unwrap());
        durable.params.push(("*File:size:77".into(), "101".into()));
        assert!(!crate::control_recovery::grant_intent_matches(&local, &durable).unwrap());
        durable.params.last_mut().unwrap().1 = "100".into();
        assert!(crate::control_recovery::grant_intent_matches(&local, &durable).unwrap());
    }

    #[tokio::test]
    async fn completion_request_timeout_joins_one_whole_operation() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let operations = Arc::new(CompletionOperations::default());
        let count = Arc::new(AtomicUsize::new(0));
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, blocked) = tokio::sync::oneshot::channel();
        let first = operations.clone();
        let runs = count.clone();
        let caller = tokio::spawn(async move {
            first
                .run("exact-receipt-and-token".into(), async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    entered.send(()).unwrap();
                    blocked.await.unwrap();
                    // This represents the final durable receipt/publication response,
                    // never a cached standalone hash verdict.
                    (
                        StatusCode::OK,
                        Json(serde_json::json!({"durableReceipt":"committed"})),
                    )
                        .into_response()
                })
                .await
        });
        started.await.unwrap();
        caller.abort(); // Network deadline expires while verification owns work.
        let second = operations.clone();
        let runs = count.clone();
        let retry = tokio::spawn(async move {
            second
                .run("exact-receipt-and-token".into(), async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    panic!("retry must join the active completion instead of hashing again")
                })
                .await
        });
        release.send(()).unwrap();
        let (status, body) = retry.await.unwrap().unwrap();
        assert_eq!(status, StatusCode::OK);
        assert!(String::from_utf8(body).unwrap().contains("committed"));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn heartbeat_renews_durable_intent_without_exporting_recovering_payload() {
        let temp = tempfile::tempdir().unwrap();
        let layout = SharedLayout::new(temp.path(), "leader").unwrap();
        let raft = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let api = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let cfg = ClusterConfig {
            cluster_id: "test".into(),
            node_name: "leader".into(),
            shared_dir: temp.path().to_path_buf(),
            advertise_url: "http://leader.invalid".into(),
            secret: "secret".into(),
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            pp_slots: 0,
            lease_interval: std::time::Duration::from_millis(100),
            takeover_after: std::time::Duration::from_secs(2),
            worker_ttl: std::time::Duration::from_secs(30),
            control_dir: temp.path().join("control"),
            control_node_id: 1,
            control_raft_bind: raft.local_addr().unwrap().to_string(),
            control_api_bind: api.local_addr().unwrap().to_string(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: Vec::new(),
            torrent_payload_roots: Vec::new(),
        };
        drop(raft);
        drop(api);
        let control = ControlStore::start(&cfg).await.unwrap();
        let engine = Engine::spawn(EngineConfig::single_node(
            Vec::new(),
            temp.path().join("engine"),
            temp.path().join("offline"),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let (_view, view) = watch::channel(LeaderView {
            record: Some(LeaderRecord {
                epoch: 1,
                node: "leader".into(),
                api_url: "http://leader.invalid".into(),
                seq: 1,
            }),
            is_me: true,
        });
        let shared = LeaderShared::new(
            engine.clone(),
            layout,
            temp.path().join("offline"),
            cfg,
            Vec::new(),
            view,
            LeaderDurability::new(Some(control.clone()), None, "leader-boot".into()),
        );
        shared.authority_ready.store(true, Ordering::Release);
        let mut job = test_job(77);
        job.files.push(nzbd_types::FileEntry {
            id: nzbd_types::FileId(77),
            subject: "payload".into(),
            filename: "payload.bin".into(),
            filename_confirmed: true,
            is_par2: false,
            paused: false,
            groups: Vec::new(),
            date: None,
            segments: vec![nzbd_types::Segment {
                message_id: "payload@example".into(),
                number: 1,
                size: 100,
                state: nzbd_types::SegmentState::Done {
                    offset: 0,
                    len: 100,
                    crc: 1,
                },
            }],
            crc32: None,
            finalized: true,
        });
        engine.import_job(job.clone(), false, false).await.unwrap();
        assert!(matches!(
            engine.export_job(job.id).await,
            Err(nzbd_engine::EngineError::Lifecycle(_))
        ));
        control
            .seed_job(
                77,
                "heartbeat-incarnation",
                &serde_json::to_string(&job).unwrap(),
            )
            .await
            .unwrap();
        let LeaseClaim::Acquired(token) = control
            .acquire(
                "work/job/77/download",
                "test",
                77,
                "heartbeat-incarnation",
                "worker",
                "worker-boot",
                "download",
                "{}",
                1,
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap()
        else {
            panic!("fresh heartbeat lease")
        };
        let lease_id = format!("{}@{}", token.resource, token.fence);
        shared.leases.lock().unwrap().insert(
            lease_id.clone(),
            LeaseInfo {
                job: job.id,
                node: "worker".into(),
                kind: LeaseKind::Download,
                token: token.clone(),
                job_revision: 1,
                control_revision: 1,
                last_hb: Instant::now(),
            },
        );
        let mut headers = HeaderMap::new();
        headers.insert(SECRET_HEADER, "secret".parse().unwrap());
        let request = |token, revision| HeartbeatRequest {
            node: "worker".into(),
            budget_ack: None,
            leases: vec![LeaseProgress {
                lease_id: lease_id.clone(),
                token,
                job: job.id,
                kind: LeaseKind::Download,
                job_revision: revision,
                control_revision: revision,
                stats: Default::default(),
            }],
        };
        let response = work_heartbeat(
            State(shared.clone()),
            headers.clone(),
            Json(request(token, 1)),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let renewed: HeartbeatResponse = serde_json::from_slice(&body).unwrap();
        assert!(renewed.cancel.is_empty());
        assert_eq!(renewed.renewed.len(), 1);
        let renewed_token = renewed.renewed[0].clone();
        assert!(
            matches!(
                engine.export_job(job.id).await,
                Err(nzbd_engine::EngineError::Lifecycle(_))
            ),
            "renewal must not turn recovery metadata into verified export"
        );

        job.priority += 1;
        assert!(matches!(
            control
                .apply_command(
                    "heartbeat-new-intent",
                    77,
                    1,
                    r#"{"op":"priority"}"#,
                    &serde_json::to_string(&job).unwrap(),
                    false,
                )
                .await
                .unwrap(),
            MutationOutcome::Applied { .. }
        ));
        let response = work_heartbeat(
            State(shared.clone()),
            headers.clone(),
            Json(request(renewed_token.clone(), 1)),
        )
        .await;
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let refused: HeartbeatResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(refused.cancel, vec![lease_id.clone()]);
        assert!(
            refused.renewed.is_empty(),
            "new durable intent revokes the old grant"
        );

        let revision = control.job_identity(77).await.unwrap().unwrap().1;
        job.status = JobStatus::Paused;
        control
            .apply_command(
                "heartbeat-paused",
                77,
                revision,
                r#"{"op":"pause"}"#,
                &serde_json::to_string(&job).unwrap(),
                false,
            )
            .await
            .unwrap();
        // Even a request claiming the new revision cannot renew paused work.
        let revision = control.job_identity(77).await.unwrap().unwrap().1;
        let response = work_heartbeat(
            State(shared.clone()),
            headers,
            Json(request(renewed_token, revision)),
        )
        .await;
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let refused: HeartbeatResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(refused.cancel, vec![lease_id]);
        assert!(refused.renewed.is_empty());
        engine.shutdown().await;
        control.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn slow_actual_completion_accepts_prior_renewal_and_survives_caller_drop() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = SharedLayout::new(tmp.path(), "leader").unwrap();
        let mut cfg = ClusterConfig {
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
        let raft = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let api = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        cfg.control_raft_bind = raft.local_addr().unwrap().to_string();
        cfg.control_api_bind = api.local_addr().unwrap().to_string();
        cfg.control_dir = tmp.path().join("control");
        drop(raft);
        drop(api);
        let control = ControlStore::start(&cfg).await.unwrap();
        let engine = Engine::spawn(EngineConfig::single_node(
            Vec::new(),
            tmp.path().join("engine"),
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
        let shared = LeaderShared::new(
            engine.clone(),
            layout,
            tmp.path().join("dest"),
            cfg,
            Vec::new(),
            view,
            LeaderDurability::new(Some(control.clone()), None, "leader-boot".into()),
        );
        shared.authority_ready.store(true, Ordering::Release);
        let mut job = test_job(7);
        job.status = JobStatus::Completed;
        job.params
            .push(("large-metadata".into(), "x".repeat(2 * 1024 * 1024)));
        control
            .seed_job(
                7,
                "exact-job-incarnation",
                &serde_json::to_string(&job).unwrap(),
            )
            .await
            .unwrap();
        let claim = control
            .acquire(
                "work/job/7/download",
                "test",
                7,
                "exact-job-incarnation",
                "worker",
                "worker-boot",
                "download",
                "{}",
                1,
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap();
        let LeaseClaim::Acquired(token) = claim else {
            panic!("fresh lease")
        };
        let generation = tmp.path().join(".nzbd-cluster/generations/slow");
        std::fs::create_dir_all(generation.join("files")).unwrap();
        let payload = b"durable payload";
        std::fs::write(generation.join("files/payload.mkv"), payload).unwrap();
        let bytes = serde_json::to_vec(&job).unwrap();
        std::fs::write(generation.join("job.json"), &bytes).unwrap();
        use sha2::{Digest, Sha256};
        let files = vec![crate::worker::GenerationFile {
            path: "payload.mkv".into(),
            bytes: payload.len() as u64,
            sha256: format!("{:x}", Sha256::digest(payload)),
        }];
        let job_sha = format!("{:x}", Sha256::digest(&bytes));
        let id = format!(
            "sha256:{:x}",
            Sha256::digest(
                serde_json::to_vec(&(7u32, "exact-job-incarnation", token.fence, &job_sha, &files))
                    .unwrap()
            )
        );
        let manifest = crate::worker::GenerationManifest {
            job_id: 7,
            job_incarnation: "exact-job-incarnation".into(),
            owner_node_id: "worker".into(),
            fence: token.fence,
            result_id: id.clone(),
            job_sha256: job_sha,
            files,
            total_bytes: payload.len() as u64,
            sealed_at_unix_ms: 1,
            artifact_copy: None,
            directory_identity: None,
            file_outcomes: Vec::new(),
            identity_version: 0,
            source_disposition: None,
        };
        std::fs::write(
            generation.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        let lease_id = "work/job/7/download@1".to_owned();
        shared.leases.lock().unwrap().insert(
            lease_id.clone(),
            LeaseInfo {
                job: job.id,
                node: "worker".into(),
                kind: LeaseKind::Download,
                token: token.clone(),
                job_revision: 1,
                control_revision: 1,
                last_hb: Instant::now(),
            },
        );
        let gate = Arc::new(VerificationGate {
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        *shared.verification_gate.lock().unwrap() = Some(gate.clone());
        let request = CompleteRequest {
            lifecycle_protocol: LIFECYCLE_PROTOCOL,
            node: "worker".into(),
            lease_id,
            token: token.clone(),
            expected_job_revision: 1,
            result_id: id.clone(),
            result_ref: generation.to_string_lossy().into_owned(),
            receipt_id: "slow-receipt".into(),
            job,
        };
        let mut headers = HeaderMap::new();
        headers.insert(SECRET_HEADER, "secret".parse().unwrap());
        // The immutable outbox was sealed with the original token, but a
        // healthy heartbeat renewed it before the first completion arrived.
        let before_submit = control
            .renew(&token, std::time::Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        shared
            .leases
            .lock()
            .unwrap()
            .get_mut(&request.lease_id)
            .unwrap()
            .token = before_submit.clone();
        for change in 0..7 {
            let mut invalid = request.clone();
            match change {
                0 => invalid.token.owner_incarnation = "another-worker-boot".into(),
                1 => invalid.token.fence += 1,
                2 => invalid.token.revision = before_submit.revision + 1,
                3 => invalid.token.expires_at_unix_ms = before_submit.expires_at_unix_ms + 1,
                4 => invalid.expected_job_revision += 1,
                5 => invalid.token.resource = "another-grant".into(),
                _ => invalid.token.owner_node_id = "another-worker".into(),
            }
            let refused = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                work_complete(State(shared.clone()), headers.clone(), Json(invalid)),
            )
            .await
            .expect("unrelated or future token must reject before payload verification");
            assert_eq!(
                refused.status(),
                StatusCode::CONFLICT,
                "invalid token case {change}"
            );
        }
        let first = tokio::spawn(work_complete(
            State(shared.clone()),
            headers.clone(),
            Json(request.clone()),
        ));
        tokio::time::timeout(std::time::Duration::from_secs(3), gate.entered.acquire())
            .await
            .expect("same-grant predecessor must reach real generation verification")
            .unwrap()
            .forget();
        let other = tmp.path().join(".nzbd-cluster/generations/conflicting");
        std::fs::create_dir_all(other.join("files")).unwrap();
        let other_payload = b"conflicting payload";
        std::fs::write(other.join("files/payload.mkv"), other_payload).unwrap();
        std::fs::write(other.join("job.json"), &bytes).unwrap();
        let mut other_manifest = manifest.clone();
        other_manifest.files[0].bytes = other_payload.len() as u64;
        other_manifest.files[0].sha256 = format!("{:x}", Sha256::digest(other_payload));
        other_manifest.total_bytes = other_payload.len() as u64;
        other_manifest.result_id = format!(
            "sha256:{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    7u32,
                    "exact-job-incarnation",
                    token.fence,
                    &other_manifest.job_sha256,
                    &other_manifest.files
                ))
                .unwrap()
            )
        );
        std::fs::write(
            other.join("manifest.json"),
            serde_json::to_vec(&other_manifest).unwrap(),
        )
        .unwrap();
        let mut competing = request.clone();
        competing.result_id = other_manifest.result_id;
        competing.result_ref = other.to_string_lossy().into_owned();
        let competitor = tokio::spawn(work_complete(
            State(shared.clone()),
            headers.clone(),
            Json(competing),
        ));
        gate.entered.acquire().await.unwrap().forget();
        first.abort(); // Simulate network deadline while bulk verification owns work.
        let renewed = control
            .renew(&before_submit, std::time::Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        let renewed = control
            .renew(&renewed, std::time::Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        shared
            .leases
            .lock()
            .unwrap()
            .get_mut(&request.lease_id)
            .unwrap()
            .token = renewed.clone();
        let retry = tokio::spawn(work_complete(
            State(shared.clone()),
            headers.clone(),
            Json(request.clone()),
        ));
        gate.release.add_permits(1);
        let response = retry.await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        gate.release.add_permits(1);
        assert_eq!(
            competitor.await.unwrap().status(),
            StatusCode::CONFLICT,
            "different completion sharing receipt ID cannot reuse the first receipt"
        );
        assert_eq!(
            gate.entered.available_permits(),
            0,
            "retry did not start another verification"
        );
        let receipt = control
            .publication_record("slow-receipt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.lease_revision, renewed.revision);
        assert!(publication_retry_matches(
            &receipt,
            &request,
            LeaseKind::Download
        ));
        let mut stale = request.clone();
        stale.token.owner_incarnation.push_str("-different");
        assert!(!publication_retry_matches(
            &receipt,
            &stale,
            LeaseKind::Download
        ));
        // The original immutable request also replays from durable evidence
        // after both the active lease and process-local result cache are gone.
        assert!(!shared
            .leases
            .lock()
            .unwrap()
            .contains_key(&request.lease_id));
        shared.completion_operations.entries.lock().unwrap().clear();
        *shared.verification_gate.lock().unwrap() = None;
        let replay = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            work_complete(State(shared.clone()), headers, Json(request.clone())),
        )
        .await
        .expect("accepted original token must replay its exact durable receipt");
        assert_eq!(replay.status(), StatusCode::OK);
        // A newer Delete wins after durable selection and materialization,
        // before READY/current queue projection, including restart recovery.
        *shared.verification_gate.lock().unwrap() = None;
        let projection = Arc::new(VerificationGate {
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        *shared.projection_gate.lock().unwrap() = Some(projection.clone());
        let mut cancelled = request.clone();
        cancelled.job.id = JobId(8);
        cancelled.job.delivery_lifecycle = true;
        cancelled.job.queued_at_unix = 1;
        cancelled
            .job
            .params
            .push(("*Cluster:original-dir".into(), "cancelled-job".into()));
        cancelled
            .job
            .params
            .push((nzbd_types::PP_DONE_PARAM.into(), "SUCCESS".into()));
        control
            .seed_job(
                8,
                "cancelled-incarnation",
                &serde_json::to_string(&cancelled.job).unwrap(),
            )
            .await
            .unwrap();
        let claim = control
            .acquire(
                "work/job/8/post",
                "test",
                8,
                "cancelled-incarnation",
                "worker",
                "worker-boot",
                "post",
                "{}",
                1,
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap();
        let LeaseClaim::Acquired(cancel_token) = claim else {
            panic!("fresh cancel lease")
        };
        cancelled.token = cancel_token.clone();
        cancelled.lease_id = "work/job/8/post@1".into();
        cancelled.receipt_id = "cancel-receipt".into();
        let cancel_root = tmp.path().join(".nzbd-cluster/generations/cancelled");
        std::fs::create_dir_all(cancel_root.join("files")).unwrap();
        let cancel_bytes = serde_json::to_vec(&cancelled.job).unwrap();
        std::fs::write(cancel_root.join("job.json"), &cancel_bytes).unwrap();
        std::fs::write(cancel_root.join("files/payload.mkv"), payload).unwrap();
        let mut cancel_manifest = manifest.clone();
        cancel_manifest.job_id = 8;
        cancel_manifest.job_incarnation = "cancelled-incarnation".into();
        cancel_manifest.job_sha256 = format!("{:x}", Sha256::digest(&cancel_bytes));
        cancel_manifest.fence = cancel_token.fence;
        cancel_manifest.result_id = format!(
            "sha256:{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    8u32,
                    "cancelled-incarnation",
                    cancel_token.fence,
                    &cancel_manifest.job_sha256,
                    &cancel_manifest.files
                ))
                .unwrap()
            )
        );
        std::fs::write(
            cancel_root.join("manifest.json"),
            serde_json::to_vec(&cancel_manifest).unwrap(),
        )
        .unwrap();
        cancelled.result_id = cancel_manifest.result_id.clone();
        cancelled.result_ref = cancel_root.to_string_lossy().into_owned();
        shared.leases.lock().unwrap().insert(
            cancelled.lease_id.clone(),
            LeaseInfo {
                job: JobId(8),
                node: "worker".into(),
                kind: LeaseKind::Post,
                token: cancel_token,
                job_revision: 1,
                control_revision: 1,
                last_hb: Instant::now(),
            },
        );
        let mut headers = HeaderMap::new();
        headers.insert(SECRET_HEADER, "secret".parse().unwrap());
        let finishing = tokio::spawn(work_complete(
            State(shared.clone()),
            headers,
            Json(cancelled.clone()),
        ));
        projection.entered.acquire().await.unwrap().forget();
        let (current, revision) = control
            .selected_publication_job(8, &cancelled.result_id)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            control
                .apply_command(
                    "newer-delete",
                    8,
                    revision,
                    r#"{"op":"delete"}"#,
                    &serde_json::to_string(&current).unwrap(),
                    true
                )
                .await
                .unwrap(),
            MutationOutcome::Applied { .. }
        ));
        projection.release.add_permits(1);
        assert_eq!(finishing.await.unwrap().status(), StatusCode::CONFLICT);
        assert!(
            engine.export_job(JobId(8)).await.unwrap().is_none(),
            "late completion must not resurrect the deleted queue row"
        );
        assert!(
            control
                .cluster_delivery(&cluster_delivery_id(&cancelled.result_id))
                .await
                .unwrap()
                .is_none(),
            "newer cancellation prevents late READY"
        );
        let materialized = shared.dest_dir.join(cluster_publication_name(
            "cancelled-job",
            &cancelled.result_id,
            true,
        ));
        assert!(materialized.is_dir());
        std::fs::remove_dir_all(&materialized).unwrap();
        recover_selected_publications(&shared).await.unwrap();
        assert!(
            !materialized.exists(),
            "historical selection cannot undo cancellation on restart"
        );
        engine.shutdown().await;
        control.shutdown().await.unwrap();
    }

    #[test]
    fn lost_completion_response_replays_only_the_exact_durable_receipt() {
        let token = test_token("work/job/7/download");
        let request = CompleteRequest {
            lifecycle_protocol: crate::proto::LIFECYCLE_PROTOCOL,
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
        shared.authority_ready.store(true, Ordering::Release);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = router(shared);
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let mut large_job = test_job(7);
        large_job
            .params
            .push(("large-metadata".into(), "x".repeat(2 * 1024 * 1024)));
        let request = CompleteRequest {
            lifecycle_protocol: crate::proto::LIFECYCLE_PROTOCOL,
            node: "worker".into(),
            lease_id: "unknown".into(),
            token: test_token("work/job/7/post"),
            expected_job_revision: 1,
            result_id: "result".into(),
            result_ref: "/missing".into(),
            receipt_id: "receipt".into(),
            job: large_job,
        };
        let error = crate::http::ClusterClient::new("secret".into())
            .post_json::<_, serde_json::Value>(
                &format!("http://{address}"),
                "/cluster/v1/work/complete",
                &request,
            )
            .await
            .unwrap_err();
        assert!(
            error.contains("409") && error.contains("unknown lease"),
            "real cluster completion route must parse >1MiB before checking lease: {error}"
        );
        server.abort();
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
        let (revision, generation, command) = shared.budget_command("worker-a", None);
        for _ in 0..100 {
            assert_eq!(
                shared.budget_command("worker-a", None),
                (revision, generation, command.clone())
            );
        }
        let mut third = shared.leases.lock().unwrap()["pp-b"].clone();
        third.node = "worker-c".into();
        shared.leases.lock().unwrap().insert("pp-c".into(), third);
        let (shrinking_revision, shrinking_generation, newcomer) =
            shared.budget_command("worker-c", None);
        assert!(shrinking_revision > revision);
        assert!(shrinking_generation > generation);
        assert!(
            newcomer.values().all(|cap| *cap == 0),
            "new capacity waits for both real drains"
        );
        let ack = BudgetAck {
            cluster_generation: shrinking_generation,
            engine_generation: 7,
            drained: true,
        };
        let (first_ack_revision, first_ack_generation, _) =
            shared.budget_command("worker-a", Some(&ack));
        assert!(first_ack_revision > shrinking_revision);
        assert_eq!(first_ack_generation, shrinking_generation);
        assert_eq!(
            shared.budget_command("worker-a", Some(&ack)).0,
            first_ack_revision,
            "replayed acknowledgment is not a new durable change"
        );
        let (last_ack_revision, grown_generation, _) =
            shared.budget_command("worker-b", Some(&ack));
        assert!(last_ack_revision > first_ack_revision);
        assert!(grown_generation > shrinking_generation);
        let (_, _, captured_command) = shared.budget_command("worker-c", None);
        assert!(captured_command["provider"] > 0);
        assert!(shared.budget_command_is_current(
            shared.epoch(),
            grown_generation,
            "worker-c",
            &captured_command
        ));
        shared.leases.lock().unwrap().remove("pp-c");
        shared.retire_expired_budget_holder("worker-c");
        assert_eq!(shared.budgets.lock().unwrap().generation, grown_generation);
        assert!(
            !shared.budget_command_is_current(
                shared.epoch(),
                grown_generation,
                "worker-c",
                &captured_command
            ),
            "same-generation retirement cannot issue a captured old expansion"
        );
        let retired_revision = shared.budgets.lock().unwrap().revision;
        assert!(retired_revision > last_ack_revision);
        shared.retire_expired_budget_holder("worker-c");
        assert_eq!(shared.budgets.lock().unwrap().revision, retired_revision);

        // Expiry during a shrink must not resurrect the retired holder when
        // the pending target is promoted, or wait forever for its drain ACK.
        let mut third = shared.leases.lock().unwrap()["pp-b"].clone();
        third.node = "worker-c".into();
        shared
            .leases
            .lock()
            .unwrap()
            .insert("pp-c".into(), third.clone());
        shared.budget_command("worker-c", None);
        third.node = "worker-d".into();
        shared.leases.lock().unwrap().insert("pp-d".into(), third);
        shared.budget_command("worker-d", None);
        assert!(shared
            .budgets
            .lock()
            .unwrap()
            .awaiting_shrink
            .contains("worker-b"));
        shared.retire_expired_budget_holder("worker-b");
        assert!(
            shared
                .budgets
                .lock()
                .unwrap()
                .awaiting_shrink
                .contains("worker-b"),
            "a live lease still reserves its old capacity"
        );
        shared.leases.lock().unwrap().remove("pp-b");
        shared.retire_expired_budget_holder("worker-b");
        assert!(!shared
            .budgets
            .lock()
            .unwrap()
            .pending_target
            .as_ref()
            .unwrap()
            .contains_key("worker-b"));
        shared.leases.lock().unwrap().remove("pp-c");
        shared.retire_expired_budget_holder("worker-c");
        {
            let state = shared.budgets.lock().unwrap();
            assert!(state.pending_target.is_none());
            assert!(state.awaiting_shrink.is_empty());
            assert!(!state.commands.contains_key("worker-b"));
            assert!(!state.commands.contains_key("worker-c"));
        }
        let (_, _, survivor) = shared.budget_command("worker-d", None);
        assert_eq!(survivor["provider"], 4);
        assert!(shared.budgets.lock().unwrap().awaiting_shrink.is_empty());
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn budget_persistence_coalesces_writes_without_acknowledging_unsaved_changes() {
        let budgets = Arc::new(Mutex::new(BudgetHandoff::default()));
        let durable = Arc::new(tokio::sync::Mutex::new(0));
        assert_eq!(
            persist_budget_snapshot(&budgets, &durable, |_, _| async {
                panic!("empty state needs no Raft write")
            })
            .await
            .unwrap(),
            0
        );
        budgets.lock().unwrap().revision = 1;
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let first = {
            let budgets = budgets.clone();
            let durable = durable.clone();
            tokio::spawn(async move {
                persist_budget_snapshot(&budgets, &durable, |revision, json| async move {
                    assert_eq!(revision, 1);
                    assert_eq!(
                        serde_json::from_str::<BudgetHandoff>(&json)
                            .unwrap()
                            .revision,
                        1
                    );
                    entered_tx.send(()).unwrap();
                    release_rx.await.unwrap();
                    Ok(())
                })
                .await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx)
            .await
            .unwrap()
            .unwrap();
        {
            let mut state = budgets.lock().unwrap();
            state.revision = 2;
            state.generation = 1;
        }
        release_tx.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), first)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            1
        );
        assert_eq!(
            *durable.lock().await,
            1,
            "saving R must not acknowledge R+1"
        );
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let save = |revision, json: String| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                assert_eq!(revision, 2);
                assert_eq!(
                    serde_json::from_str::<BudgetHandoff>(&json)
                        .unwrap()
                        .generation,
                    1
                );
                Ok(())
            }
        };
        let (left, right) = tokio::join!(
            persist_budget_snapshot(&budgets, &durable, save),
            persist_budget_snapshot(&budgets, &durable, save),
        );
        assert_eq!(left.unwrap(), 2);
        assert_eq!(right.unwrap(), 2);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "concurrent unchanged snapshots issue one write"
        );
        budgets.lock().unwrap().revision = 3;
        assert!(persist_budget_snapshot(&budgets, &durable, |_, _| async {
            Err("injected durable write failure".into())
        })
        .await
        .is_err());
        assert_eq!(*durable.lock().await, 2);
        assert_eq!(
            persist_budget_snapshot(&budgets, &durable, |revision, _| async move {
                assert_eq!(revision, 3);
                Ok(())
            })
            .await
            .unwrap(),
            3,
            "the failed revision remains retryable"
        );
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
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn work_grant_uses_compared_intent_and_refuses_intervening_pause() {
        let temp = tempfile::tempdir().unwrap();
        let raft = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let api = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let cfg = ClusterConfig {
            cluster_id: "grant-cas".into(),
            node_name: "authority".into(),
            shared_dir: temp.path().join("shared"),
            advertise_url: "http://127.0.0.1:1".into(),
            secret: "secret".into(),
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            pp_slots: 0,
            lease_interval: std::time::Duration::from_millis(100),
            takeover_after: std::time::Duration::from_secs(1),
            worker_ttl: std::time::Duration::from_secs(3),
            control_dir: temp.path().join("control"),
            control_node_id: 1,
            control_raft_bind: raft.local_addr().unwrap().to_string(),
            control_api_bind: api.local_addr().unwrap().to_string(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: Vec::new(),
            torrent_payload_roots: Vec::new(),
        };
        drop(raft);
        drop(api);
        let runtime = crate::ClusterRuntime::start(
            cfg,
            Vec::new(),
            Tuning {
                min_free_disk_bytes: 0,
                ..Default::default()
            },
            temp.path().join("dest"),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while !runtime.leader_shared.authority_ready() {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let shared = &runtime.leader_shared;
        let control = runtime.control.as_ref().unwrap();
        let _serial = shared.mutation_serial.lock().await;
        let mut job = test_job(91);
        job.status = JobStatus::Queued;
        // Actual owned legacy writer source: provenance must not opt it into
        // automatic cleanup or consumer delivery.
        let inventory = runtime.engine.artifacts();
        let source = inventory
            .allocate_for_delivery(
                91,
                &temp.path().join("dest"),
                &temp.path().join("dest/job"),
                false,
                false,
            )
            .unwrap();
        runtime.engine.import_job(job, false, false).await.unwrap();
        let stale = runtime.engine.export_job(JobId(91)).await.unwrap().unwrap();
        control
            .commit_job_delta(&[(91, Some(stale.clone()))])
            .await
            .unwrap();
        crate::source_facts::reconcile(shared).await.unwrap();
        let stale = runtime.engine.export_job(JobId(91)).await.unwrap().unwrap();
        assert!(
            !inventory
                .get(&source.id)
                .unwrap()
                .automatic_cleanup_eligible
        );
        assert!(!inventory.get(&source.id).unwrap().delivery_lifecycle);
        let mut current = stale.clone();
        current.name = "renamed by operator".into();
        current
            .params
            .push(("Failure:KeepOnFailure".into(), "true".into()));
        control
            .commit_job_delta(&[(91, Some(current.clone()))])
            .await
            .unwrap();
        assert!(
            shared
                .grant_job(
                    "worker",
                    "worker-process",
                    LeaseKind::Download,
                    stale,
                    serde_json::json!({"whole_job":true})
                )
                .await
                .is_none(),
            "stale export cannot borrow the newer name/Keep revision"
        );
        assert!(control
            .current_lease("work/job/91/download")
            .await
            .unwrap()
            .is_none());
        shared.restore_control_projection().await.unwrap();
        let current = runtime.engine.export_job(JobId(91)).await.unwrap().unwrap();
        let gate = Arc::new(VerificationGate {
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        *shared.grant_gate.lock().unwrap() = Some(gate.clone());
        let attempt = tokio::spawn({
            let shared = shared.clone();
            let job = current.clone();
            async move {
                shared
                    .grant_job(
                        "worker",
                        "worker-process",
                        LeaseKind::Download,
                        job,
                        serde_json::json!({"whole_job":true}),
                    )
                    .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let mut paused = current.clone();
        paused.status = JobStatus::Paused;
        control
            .commit_job_delta(&[(91, Some(paused))])
            .await
            .unwrap();
        gate.release.add_permits(1);
        assert!(
            attempt.await.unwrap().is_none(),
            "pause committed after compare must defeat lease insertion CAS"
        );
        assert!(control
            .current_lease("work/job/91/download")
            .await
            .unwrap()
            .is_none());
        *shared.grant_gate.lock().unwrap() = None;
        current_job_resume(control, shared, &current).await;
        let current = runtime.engine.export_job(JobId(91)).await.unwrap().unwrap();
        let grant = shared
            .grant_job(
                "worker",
                "worker-process",
                LeaseKind::Download,
                current,
                serde_json::json!({"whole_job":true}),
            )
            .await
            .unwrap();
        assert_eq!(grant.job.name, "renamed by operator");
        assert!(grant
            .job
            .params
            .iter()
            .any(|(key, value)| key == "Failure:KeepOnFailure" && value == "true"));
        assert_eq!(
            grant.job_revision,
            control.job_identity(91).await.unwrap().unwrap().1
        );
        let historical = control
            .historical_grant(&grant.token.resource, grant.token.fence)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(historical.initial_token, grant.token);
        let fact: crate::delivery::AuthoritySourceCopy =
            serde_json::from_str(historical.source_binding_json.as_ref().unwrap()).unwrap();
        assert_eq!(fact.owner_node, "authority");
        assert_eq!(fact.copy.source.artifact, source.id);
        assert!(control.release(&grant.token).await.unwrap());
        let current = runtime.engine.export_job(JobId(91)).await.unwrap().unwrap();
        let next = shared
            .grant_job(
                "worker2",
                "worker-process2",
                LeaseKind::Download,
                current,
                serde_json::json!({"whole_job":true}),
            )
            .await
            .unwrap();
        assert!(next.token.fence > grant.token.fence);
        assert_eq!(
            control
                .historical_grant(&grant.token.resource, grant.token.fence)
                .await
                .unwrap()
                .unwrap(),
            historical,
            "regrant must preserve historical owner proof"
        );
        assert_eq!(
            control
                .historical_grant(&next.token.resource, next.token.fence)
                .await
                .unwrap()
                .unwrap()
                .initial_token,
            next.token
        );
        assert!(control.acquire("job/91/stale-issuer","grant-cas",91,&next.job_incarnation,"worker","process","download",&serde_json::json!({"_authority":{"node":"authority","incarnation":"stale-process","fence":shared.epoch()}}).to_string(),next.job_revision,std::time::Duration::from_secs(1)).await.is_err());
        drop(_serial);
        runtime.shutdown().await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn automatic_control_reconciliation_replays_ack_and_preserves_newer_pause() {
        let temp = tempfile::tempdir().unwrap();
        let raft = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let api = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let cfg = ClusterConfig {
            cluster_id: "grant-cas".into(),
            node_name: "authority".into(),
            shared_dir: temp.path().join("shared"),
            advertise_url: "http://127.0.0.1:1".into(),
            secret: "secret".into(),
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            pp_slots: 0,
            lease_interval: std::time::Duration::from_millis(100),
            takeover_after: std::time::Duration::from_secs(1),
            worker_ttl: std::time::Duration::from_secs(3),
            control_dir: temp.path().join("control"),
            control_node_id: 1,
            control_raft_bind: raft.local_addr().unwrap().to_string(),
            control_api_bind: api.local_addr().unwrap().to_string(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: Vec::new(),
            torrent_payload_roots: Vec::new(),
        };
        drop(raft);
        drop(api);
        let runtime = crate::ClusterRuntime::start(
            cfg,
            Vec::new(),
            Tuning {
                min_free_disk_bytes: 0,
                ..Default::default()
            },
            temp.path().join("dest"),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while !runtime.leader_shared.authority_ready() {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let shared = &runtime.leader_shared;
        let control = runtime.control.as_ref().unwrap();
        let _serial = shared.mutation_serial.lock().await;
        let mut base = test_job(92);
        base.status = JobStatus::Queued;
        base.dir_name = "automatic".into();
        let inventory = runtime.engine.artifacts();
        inventory
            .allocate_for_delivery(
                92,
                &temp.path().join("dest"),
                &temp.path().join("dest/automatic"),
                true,
                false,
            )
            .unwrap();
        runtime.engine.import_job(base, false, false).await.unwrap();
        let base = runtime.engine.export_job(JobId(92)).await.unwrap().unwrap();
        control.commit_job_delta(&[(92, Some(base))]).await.unwrap();
        crate::source_facts::reconcile(shared).await.unwrap();
        assert!(runtime
            .engine
            .hold_job(
                JobId(92),
                nzbd_types::HoldCause::Allocation,
                "download",
                "allocation temporarily unavailable"
            )
            .await
            .unwrap());
        let held = runtime.engine.export_job(JobId(92)).await.unwrap().unwrap();
        let marker = nzbd_engine::automatic_control::transition(&held)
            .expect("real actor hold has provenance");
        settle_automatic_control(shared, JobId(92)).await;
        let (durable, revision, _) = control.current_job_intent(92).await.unwrap().unwrap();
        assert_eq!(durable.control().as_ref(), Some(&marker.result_control));
        assert!(!durable
            .params
            .iter()
            .any(|(key, _)| key == nzbd_engine::automatic_control::AUTOMATIC_CONTROL_PARAM));
        assert!(nzbd_engine::automatic_control::transition(
            &runtime.engine.export_job(JobId(92)).await.unwrap().unwrap()
        )
        .is_none());
        // Simulate a lost local ACK by restoring its exact persisted marker.
        runtime.engine.import_job(held, false, false).await.unwrap();
        assert!(nzbd_engine::automatic_control::transition(
            &runtime.engine.export_job(JobId(92)).await.unwrap().unwrap()
        )
        .is_some());
        settle_automatic_control(shared, JobId(92)).await;
        assert_eq!(
            control.current_job_intent(92).await.unwrap().unwrap().1,
            revision,
            "ACK replay must not add another durable transition"
        );
        // Advance only this fixture's persisted retry clock; the following
        // release still runs the production Recovery actor path.
        let mut due = durable.clone();
        let mut due_control = due.control().unwrap();
        due_control.next_retry_at_unix = Some(0);
        due.set_control(&due_control);
        control.commit_job_delta(&[(92, Some(due))]).await.unwrap();
        shared.restore_control_projection().await.unwrap();
        let recovery = runtime
            .engine
            .release_hold(
                JobId(92),
                marker.result_control.revision,
                nzbd_engine::RetryBy::Recovery,
            )
            .await
            .unwrap();
        assert_eq!(recovery, Ok(()));
        let released = runtime.engine.export_job(JobId(92)).await.unwrap().unwrap();
        assert!(nzbd_engine::automatic_control::transition(&released).is_some());
        let mut paused = durable;
        let mut paused_control = paused.control().unwrap();
        paused_control.manual_pause = true;
        paused_control.revision = (paused_control.revision.parse::<u64>().unwrap() + 1).to_string();
        paused.set_control(&paused_control);
        // A pause inside a hold: the control records it, the job stays held.
        paused.status = JobStatus::Held;
        control
            .commit_job_delta(&[(92, Some(paused))])
            .await
            .unwrap();
        settle_automatic_control(shared, JobId(92)).await;
        let current = runtime.engine.export_job(JobId(92)).await.unwrap().unwrap();
        assert_eq!(current.status, JobStatus::Held);
        assert!(current.control().unwrap().manual_pause);
        assert!(nzbd_engine::automatic_control::transition(&current).is_none());
        assert_eq!(
            control
                .current_job_intent(92)
                .await
                .unwrap()
                .unwrap()
                .0
                .control(),
            Some(paused_control)
        );
        drop(_serial);
        runtime.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn blocked_source_proof_preserves_pending_job_but_allows_other_controls_and_delete() {
        let temp = tempfile::tempdir().unwrap();
        let raft = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let api = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let cfg = ClusterConfig {
            cluster_id: "grant-cas".into(),
            node_name: "authority".into(),
            shared_dir: temp.path().join("shared"),
            advertise_url: "http://127.0.0.1:1".into(),
            secret: "secret".into(),
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            pp_slots: 0,
            lease_interval: std::time::Duration::from_millis(100),
            takeover_after: std::time::Duration::from_secs(1),
            worker_ttl: std::time::Duration::from_secs(3),
            control_dir: temp.path().join("control"),
            control_node_id: 1,
            control_raft_bind: raft.local_addr().unwrap().to_string(),
            control_api_bind: api.local_addr().unwrap().to_string(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: Vec::new(),
            torrent_payload_roots: Vec::new(),
        };
        drop(raft);
        drop(api);
        let runtime = crate::ClusterRuntime::start(
            cfg,
            Vec::new(),
            Tuning {
                min_free_disk_bytes: 0,
                ..Default::default()
            },
            temp.path().join("dest"),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while !runtime.leader_shared.authority_ready() {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let shared = &runtime.leader_shared;
        let control = runtime.control.as_ref().unwrap();
        let serial = shared.mutation_serial.lock().await;
        let mut blocked = test_job(92);
        blocked.status = JobStatus::Queued;
        blocked.dir_name = "proof-pending".into();
        let inventory = runtime.engine.artifacts();
        let source = inventory
            .allocate_for_delivery(
                92,
                &temp.path().join("dest"),
                &temp.path().join("dest/proof-pending"),
                true,
                false,
            )
            .unwrap();
        let mut other = test_job(93);
        other.status = JobStatus::Queued;
        other.dir_name = "other".into();
        runtime
            .engine
            .import_job(blocked, false, false)
            .await
            .unwrap();
        runtime
            .engine
            .import_job(other, false, false)
            .await
            .unwrap();
        let blocked = runtime.engine.export_job(JobId(92)).await.unwrap().unwrap();
        let other = runtime.engine.export_job(JobId(93)).await.unwrap().unwrap();
        control
            .commit_job_delta(&[(92, Some(blocked)), (93, Some(other))])
            .await
            .unwrap();
        crate::source_facts::reconcile(shared).await.unwrap();
        let gate = Arc::new((
            tokio::sync::Semaphore::new(0),
            tokio::sync::Semaphore::new(0),
        ));
        *shared.control_proofs.gate.lock().unwrap() = Some(gate.clone());
        assert!(runtime
            .engine
            .hold_job(
                JobId(92),
                nzbd_types::HoldCause::Allocation,
                "download",
                "source proof delayed"
            )
            .await
            .unwrap());
        crate::control_recovery::reconcile(shared).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), gate.0.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        shared.restore_control_projection().await.unwrap();
        assert!(
            nzbd_engine::automatic_control::transition(
                &runtime.engine.export_job(JobId(92)).await.unwrap().unwrap()
            )
            .is_some(),
            "unrelated projection rebuild cannot erase pending provenance"
        );
        drop(serial);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = runtime.router("test", Vec::new());
        let serve = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = crate::http::ClusterClient::new("secret".into());
        use http_body_util::BodyExt;
        let detail = client
            .forward(
                &format!("http://{address}"),
                hyper::Request::get("/api/v1/jobs/92")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(detail.status(), StatusCode::OK);
        let detail: serde_json::Value =
            serde_json::from_slice(&detail.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(detail["controlPending"]["state"], "pending");
        assert_eq!(detail["controlPending"]["owner"], "authority");
        for (job, action) in [(93, "pause"), (92, "delete"), (93, "resume")] {
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                client.forward(
                    &format!("http://{address}"),
                    hyper::Request::post(format!("/api/v1/jobs/{job}/actions/{action}"))
                        .body(axum::body::Body::empty())
                        .unwrap(),
                ),
            )
            .await
            .expect("a slow source proof must not monopolize mutation_serial")
            .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{job}/{action}");
            if job == 93 && action == "pause" {
                assert!(nzbd_engine::automatic_control::transition(
                    &runtime.engine.export_job(JobId(92)).await.unwrap().unwrap()
                )
                .is_some());
                assert_eq!(
                    control
                        .current_job_intent(93)
                        .await
                        .unwrap()
                        .unwrap()
                        .0
                        .status,
                    JobStatus::Paused
                );
            }
        }
        assert!(runtime
            .engine
            .export_job(JobId(92))
            .await
            .unwrap()
            .is_none());
        assert!(control.current_job_intent(92).await.unwrap().is_none());
        assert!(
            source.path.exists(),
            "ordinary removal keeps the blocked job's bytes"
        );
        crate::control_recovery::reconcile(shared).await.unwrap();
        gate.1.add_permits(1);
        tokio::task::yield_now().await;
        assert!(shared
            .control_proofs
            .snapshot()
            .iter()
            .all(|value| value["jobId"] != 92));
        assert!(
            runtime
                .engine
                .export_job(JobId(92))
                .await
                .unwrap()
                .is_none(),
            "late proof cannot resurrect explicit removal"
        );
        // A reconciler can retain this lock while a quorum request is pending.
        // Admission must reject promptly, without running the mutation handler.
        let serial = shared.mutation_serial.lock().await;
        let before = runtime.engine.export_job(JobId(93)).await.unwrap().unwrap();
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.forward(
                &format!("http://{address}"),
                hyper::Request::post("/api/v1/jobs/93/actions/pause")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            ),
        )
        .await
        .expect("mutation admission cannot wait indefinitely for reconciliation")
        .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let refusal = response.into_body().collect().await.unwrap().to_bytes();
        assert!(String::from_utf8_lossy(&refusal).contains("reconciliation is busy"));
        let after = runtime.engine.export_job(JobId(93)).await.unwrap().unwrap();
        assert_eq!(after.status, before.status);
        assert_eq!(after.control(), before.control());
        drop(serial);
        serve.abort();
        let _ = serve.await;
        runtime.shutdown().await;
    }

    async fn settle_automatic_control(shared: &LeaderShared, job: JobId) {
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        loop {
            crate::control_recovery::reconcile(shared).await.unwrap();
            if shared
                .engine
                .export_job(job)
                .await
                .unwrap()
                .is_none_or(|job| nzbd_engine::automatic_control::transition(&job).is_none())
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "automatic proof/commit did not settle"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    async fn current_job_resume(control: &ControlStore, shared: &LeaderShared, current: &Job) {
        control
            .commit_job_delta(&[(u64::from(current.id.0), Some(current.clone()))])
            .await
            .unwrap();
        shared.restore_control_projection().await.unwrap();
    }

    #[test]
    fn bounded_poll_alternates_download_and_post_backlogs() {
        let mut turns = HashSet::new();
        let lanes = (0..10)
            .map(|_| post_lane_first(&mut turns, "busy-worker"))
            .collect::<Vec<_>>();
        assert_eq!(
            lanes,
            vec![true, false, true, false, true, false, true, false, true, false]
        );
        assert!(post_lane_first(&mut turns, "independent-worker"));
    }
}
