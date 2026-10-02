//! Cluster work-lease wire types (CLUSTERING.md §6.1). Server credentials
//! never cross this channel — budgets are keyed by server *name*, resolved
//! against each node's local `[[server]]` config.

use crate::control::LeaseToken;
use nzbd_engine::MirrorStats;
use nzbd_types::{Job, JobId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const SECRET_HEADER: &str = "x-nzbd-cluster-secret";

#[derive(Debug, Serialize, Deserialize)]
pub struct PollRequest {
    pub node: String,
    pub owner_incarnation: String,
    pub free_download_slots: u32,
    /// Free post-processing slots (C2). Absent/0 = not a PP executor.
    #[serde(default)]
    pub free_pp_slots: u32,
}

/// What a lease authorizes (C2 adds stage-level PP work).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LeaseKind {
    #[default]
    Download,
    /// Post-processing: par verify/repair → unpack → cleanup → scripts,
    /// fenced in `.pp.<lease_id>/` staging (CLUSTERING.md §6.4).
    Post,
    /// A bounded inclusive article-index range within one file.
    Segment,
    /// The sole authority allowed to assemble and select a completed file.
    Assemble,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Grant {
    pub lease_id: String,
    pub token: LeaseToken,
    pub job_incarnation: String,
    pub job_revision: u64,
    pub control_revision: u64,
    /// Exact authorized work scope. Whole-job grants use `{"whole_job":true}`;
    /// segment grants name file and article range.
    pub scope: serde_json::Value,
    pub epoch: u64,
    #[serde(default)]
    pub kind: LeaseKind,
    pub job: Job,
    /// Per-server-name connection allowance (cluster-wide account cap
    /// partitioning, §6.3). PP grants carry it too because delayed PAR
    /// recovery may fetch explicitly selected recovery volumes.
    pub server_budgets: HashMap<String, u16>,
    #[serde(default)]
    pub budget_generation: u64,
    /// Rolling-upgrade capability: true only when the granting leader counts
    /// PP leases in those budgets. Missing/false must keep PP NNTP disabled.
    #[serde(default)]
    pub post_fetch_budgeted: bool,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct PollResponse {
    pub grants: Vec<Grant>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LeaseProgress {
    pub lease_id: String,
    pub token: LeaseToken,
    pub job: JobId,
    pub kind: LeaseKind,
    pub job_revision: u64,
    pub control_revision: u64,
    pub stats: MirrorStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub node: String,
    pub leases: Vec<LeaseProgress>,
    /// Last node-local engine generation that observed the complete budget
    /// update between NNTP batches.
    #[serde(default)]
    pub budget_ack: Option<BudgetAck>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetAck {
    pub cluster_generation: u64,
    pub engine_generation: u64,
    pub drained: bool,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct HeartbeatResponse {
    /// Leases the worker must abort (job deleted / reassigned / unknown).
    pub cancel: Vec<String>,
    /// Exact successor tokens. A retry never extends authority unless the
    /// leader durably renewed the predecessor token.
    #[serde(default)]
    pub renewed: Vec<LeaseToken>,
    /// Latest desired job-control revision for each retained lease.
    #[serde(default)]
    pub controls: HashMap<String, u64>,
    /// Refreshed connection budgets (membership changed since the grant).
    pub server_budgets: Option<HashMap<String, u16>>,
    #[serde(default)]
    pub budget_generation: u64,
    /// Same rolling-upgrade capability as [`Grant::post_fetch_budgeted`].
    #[serde(default)]
    pub post_fetch_budgeted: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompleteRequest {
    pub node: String,
    pub lease_id: String,
    pub token: LeaseToken,
    pub expected_job_revision: u64,
    pub result_id: String,
    pub result_ref: String,
    pub receipt_id: String,
    /// The finished job's full final state (ids preserved).
    pub job: Job,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompleteResponse {
    /// Concrete publication path chosen by the authority, after materialization.
    #[serde(default)]
    pub final_dir: Option<String>,
    pub ok: bool,
    pub durable_receipt: Option<String>,
    /// Replicated receipt time used as the one authoritative history key.
    #[serde(default)]
    pub accepted_at_unix_ms: Option<i64>,
    /// True when the authority already durably wrote the logical history row.
    #[serde(default)]
    pub history_recorded_by_authority: bool,
}

/// A worker's local guard changed after it advertised capacity but before it
/// could safely accept the returned grant.
#[derive(Debug, Serialize, Deserialize)]
pub struct RejectRequest {
    pub node: String,
    pub lease_id: String,
    pub token: LeaseToken,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RejectResponse {
    pub released: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScriptReceiptRequest {
    pub node: String,
    pub lease_id: String,
    pub token: LeaseToken,
    pub receipt_id: String,
    pub finish: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScriptReceiptResponse {
    pub decision: String,
}

/// Node presence record (registry file on the shared volume).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRecord {
    pub name: String,
    pub api_url: String,
    pub download: bool,
    pub post_process: bool,
    pub max_download_jobs: u32,
    #[serde(default = "default_weight")]
    pub download_weight: u32,
    #[serde(default = "default_weight")]
    pub pp_weight: u32,
    pub active_download_jobs: u32,
    /// Missing/false on pre-multi-root workers. A new leader excludes those
    /// workers until they upgrade because their disk admission state is
    /// unknowable during a rolling restart.
    #[serde(default)]
    pub disk_guard_capable: bool,
    /// This node is refusing new download and PP leases because at least one
    /// configured write volume is constrained.
    #[serde(default)]
    pub disk_low: bool,
    #[serde(default)]
    pub disk_guard_free_bytes: Option<u64>,
    #[serde(default)]
    pub disk_guard_label: Option<String>,
    #[serde(default)]
    pub disk_guard_path: Option<String>,
    #[serde(default)]
    pub disk_guard_write_latched: bool,
    #[serde(default)]
    pub disk_guard_all_roots_known: bool,
    /// PP executor capacity (C2 anti-affinity scheduling input).
    #[serde(default)]
    pub pp_slots: u32,
    pub rate_bps: u64,
    pub seq: u64,
}

fn default_weight() -> u32 {
    1
}
