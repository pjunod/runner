//! Authoritative payload inventory, deliberately independent of History.
//!
//! SQLite owns intent and policy; external sidecars own allocation identity.
//! Nothing in a media payload is a marker or grants permission to remove it.
mod cleanup_review;
pub use cleanup_review::CleanupReview;
mod cluster;
mod cluster_keep;
pub use cluster::{remove_cluster_generation_copy, ClusterArtifactCopy, ClusterCopyCleanup};
pub use cluster_keep::{ClusterFailureKeepStatus, ClusterFailureKeepTarget};
pub(crate) mod fs;

/// Name prefix of the engine's out-of-space directory probe files (W5a):
/// excluded from every payload manifest.
pub const SPACE_PROBE_PREFIX: &str = ".nzbd-space-probe-";
mod policy;
mod publication;
pub use policy::{RetentionChange, RetentionPreview};
mod delivery;
pub mod exits;
mod materialization;
pub use materialization::{
    external_generation_identity, ExternalGeneration, ExternalGenerationFile,
};
mod intents;
pub use intents::{
    write_runner_manifest, IntentFile, IntentState, ManifestFile, RelocationIntent, RunnerManifest,
    RUNNER_MANIFEST_NAME,
};
mod recovery;
mod relocation;
pub use delivery::{
    Delivery, DeliveryFile, DeliveryFileOutcome, DeliveryRunnerStages, PreparedDelivery,
};
pub use relocation::{RelocationResult, SelectedPublication, SupersededOutcome, SupersededSource};
mod startup;
mod terminals;
pub use startup::StartupReconciliation;
pub use terminals::{
    DisposalLadder, JobTerminal, SalvageOutcome, SalvageState, TerminalStep,
    DISPOSAL_ATTEMPT_BUDGET,
};
#[cfg(test)]
mod operation_index_tests;
mod operation_queries;
mod tasks;
mod torrent;
pub use torrent::TorrentSourceReader;
mod torrent_seed;
pub use torrent_seed::{TorrentSeedRetirementIntent, TorrentSeedRetirementReason};
mod transforms;
mod url_resolutions;
mod worker_controls;
mod worker_stops;
pub mod writer_naming;
pub use url_resolutions::{
    url_source_identity, url_storage_retry_deadline, UrlFileReservation, UrlResolutionFault,
    UrlResolutionRecord, UrlResolutionStore, UrlStorageRetry, MAX_SOURCE as MAX_URL_SOURCE_BYTES,
};
pub use worker_stops::{
    same_worker_operation, WorkerStopRequest, WorkerStopTicket, WorkerStoppedReceipt,
};
mod workspace_lifecycle;
pub use recovery::{Receipt, ReceiptFile, Recovery, RecoveryFile};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
pub use transforms::{CustodyEvidence, Workspace};
pub use worker_controls::{
    WorkerCompletionProof, WorkerControlCheckpoint, WorkerControlContext, WorkerRetryAdoption,
    WORKER_CONTROL_CONTEXT_PARAM,
};
pub use workspace_lifecycle::{
    AttemptUse, FailedCleanupProof, FinalizationProof, SourceGeneration, StagedCompletion,
    WorkspaceAssessment,
};

#[cfg(test)]
thread_local! {static DELETE_FAIL_AFTER:std::cell::Cell<Option<usize>>=const{std::cell::Cell::new(None)};}
pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("inventory database: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("filesystem: {0}")]
    Io(#[from] std::io::Error),
    #[error("inventory data: {0}")]
    Json(#[from] serde_json::Error),
    /// The operation did not acquire execution capacity; retry without
    /// treating unchanged artifact metadata as an ownership conflict.
    #[error("operation deferred: {0}")]
    Busy(&'static str),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    CleanupBlocked(String),
    #[error("{reason}")]
    CleanupRetry { next_retry: i64, reason: String },
    #[error("artifact not found")]
    NotFound,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Identity {
    pub device: u64,
    pub inode: u64,
    /// Stable mount source/type/root identity, used to recover after the same
    /// volume is remounted with a new device number. Empty on unsupported
    /// platforms and for legacy records; those records require device equality.
    #[serde(default)]
    pub mount_id: String,
    pub bytes: u64,
    pub modified: String,
    /// Change time where the host exposes it. This detects same-inode,
    /// same-size replacements that preserve modification time.
    #[serde(default)]
    pub changed: String,
    pub directory: bool,
}
impl Identity {
    pub fn same_object(&self, other: &Self) -> bool {
        (if self.mount_id.is_empty() || other.mount_id.is_empty() {
            // Pre-upgrade records have no mount tuple. Keep their old
            // device/inode manifest available for inspection and explicit,
            // user-reviewed actions; automatic cleanup is cohort-gated.
            self.device == other.device
        } else {
            self.mount_id == other.mount_id || same_mount_rebound(&self.mount_id, &other.mount_id)
        }) && self.inode == other.inode
            && self.directory == other.directory
    }
}

/// The fields of a `fs::mount_id` tuple: platform, then length-prefixed
/// filesystem type, mount source, root within the filesystem, mount point.
fn mount_fields(mount_id: &str) -> Option<Vec<&str>> {
    let (platform, mut rest) = mount_id.split_once('\n')?;
    let mut fields = vec![platform];
    while !rest.is_empty() {
        let (length, tail) = rest.split_once(':')?;
        let length: usize = length.parse().ok()?;
        let value = tail.get(..length)?;
        fields.push(value);
        rest = tail
            .get(length..)?
            .strip_prefix('\n')
            .unwrap_or(tail.get(length..)?);
    }
    Some(fields)
}

/// A remount that changed only the mount *source* string — an NFS server
/// alias (`qnap:/working` → `qnap-storage:/working`), a re-resolved FUSE
/// source — is the same filesystem when the platform, filesystem type, root
/// within it and mount point are unchanged. The inode (and the directory
/// flag) are still compared by the caller, and every payload identity under
/// it is compared the same way, so a different filesystem mounted in its
/// place does not pass. Without this, every row recorded before such a
/// remount reports "configured root identity changed" forever.
fn same_mount_rebound(left: &str, right: &str) -> bool {
    match (mount_fields(left), mount_fields(right)) {
        (Some(a), Some(b)) if a.len() == 5 && b.len() == 5 => {
            a[0] == b[0] && a[1] == b[1] && a[3] == b[3] && a[4] == b[4]
        }
        _ => false,
    }
}
impl Identity {
    /// Contract §2.5 / principle 5: a published file's identity is its
    /// object, size and modification time (plus the journaled digest the
    /// caller carries). Never ctime: link/unlink, NFS silly-rename removal
    /// and chmod move ctime without touching the bytes.
    pub fn same_publication(&self, other: &Self) -> bool {
        self.same_object(other) && self.bytes == other.bytes && self.modified == other.modified
    }
}
impl PartialEq for Identity {
    fn eq(&self, other: &Self) -> bool {
        self.same_object(other)
            && self.bytes == other.bytes
            && self.modified == other.modified
            && (self.changed.is_empty()
                || other.changed.is_empty()
                || self.changed == other.changed)
    }
}
impl Eq for Identity {}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileEntry {
    pub path: String,
    pub identity: Identity,
    pub digest: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Artifact {
    pub id: String,
    pub generation: String,
    pub revision: u64,
    pub job: Option<u32>,
    pub path: PathBuf,
    /// Immutable path assigned when this generation was first allocated.
    /// Unlike `path`, it never changes across relocation.
    #[serde(default)]
    pub origin_path: PathBuf,
    pub root: PathBuf,
    pub root_identity: Identity,
    pub identity: Option<Identity>,
    pub state: String,
    pub owned: bool,
    /// True only for artifacts allocated by a new queue admission. Old rows
    /// default false so stale automatic deletion/retirement work is review-only.
    #[serde(default)]
    pub automatic_cleanup_eligible: bool,
    /// Immutable admission cohort for automatic Curator delivery/retirement.
    /// Existing inventory rows default false and require separate review.
    #[serde(default)]
    pub delivery_lifecycle: bool,
    pub keep: bool,
    /// Per-download failure retention intent. Separate from the inventory
    /// Keep control so enabling it cannot fabricate a user-wide Keep state.
    #[serde(default)]
    pub keep_on_failure: bool,
    /// Bounded, generation-pinned terminal assessment for a new failed
    /// source retained by Keep. Missing legacy evidence is never deletion
    /// authority; only `unusable` may be disposed when Keep is later cleared.
    #[serde(default)]
    pub failure_assessment: Option<FailureAssessment>,
    pub hold: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub retention_seconds: u64,
    /// Wall-clock expiry (W3): due when `deadline <= now`, across restarts.
    pub deadline: Option<i64>,
    #[serde(default)]
    pub files: Vec<FileEntry>,
    pub error: Option<String>,
    /// When a walk last measured `files`. Discovery records a directory
    /// without walking it, so an unknown folder with no entries is
    /// "not measured yet" until this is set — never "empty".
    #[serde(default)]
    pub inspected_at: Option<i64>,
    /// Contract §2.2/§2.6: the salvage obligation the job's terminal record
    /// put on this source (`pending`, then `published` or `settled_none`).
    /// Mirrored from `job_terminals`, which is the authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub salvage: Option<String>,
    /// Contract §2.2: the `FailureReason` wire string of the terminal that
    /// parked this source (`unpack`, `hold_expired`, …), mirrored from
    /// `job_terminals` so the Files tab can say why the bytes are here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    /// Why a `source_gone` row is gone when Runner did not remove it and the
    /// finding rests on weaker evidence than a verified root (for example
    /// `legacy_root_rebind`). Custody reports it as gone, not removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gone_reason: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FailureAssessment {
    pub disposition: String,
    pub reason: String,
    pub assessed_at: i64,
}
impl Artifact {
    pub fn terminal(&self) -> bool {
        matches!(self.state.as_str(), "deleted" | "source_gone")
    }
    /// Regular files and their bytes; directories are structure, not payload.
    pub fn summary(&self) -> (u64, u64) {
        let files = self.files.iter().filter(|f| !f.identity.directory);
        (
            files.clone().count() as u64,
            files.map(|f| f.identity.bytes).sum(),
        )
    }
    /// True once a walk has populated `files` (a finalized job, a restore,
    /// or an inspection). A discovered-but-unmeasured folder reports false.
    pub fn measured(&self) -> bool {
        self.inspected_at.is_some() || !self.files.is_empty()
    }
    fn eligible(&self) -> bool {
        self.owned
            && matches!(self.state.as_str(), "parked_failed" | "recovery_imported")
            && !self.keep
            && self.hold.is_none()
            && self.retention_seconds > 0
    }
    pub fn earliest_expiry(&self) -> Option<i64> {
        if !self.eligible() {
            return None;
        }
        self.deadline
    }
}
/// Exact receipt-custody metadata from the current artifact row. This does
/// not contain file manifests and cannot serve as payload or identity proof.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactCustody {
    pub generation: String,
    pub state: String,
    pub owned: bool,
    pub keep: bool,
    pub keep_on_failure: bool,
    /// See `Artifact::gone_reason`: gone, not removed by Runner.
    pub gone_reason: Option<String>,
}

impl ArtifactCustody {
    fn from_header(raw: &str) -> Result<Self> {
        let header: Artifact = serde_json::from_str(raw)?;
        Ok(Self {
            generation: header.generation,
            state: header.state,
            owned: header.owned,
            keep: header.keep,
            keep_on_failure: header.keep_on_failure,
            gone_reason: header.gone_reason,
        })
    }
}

// The expression must match in the index and SELECT. Invalid JSON remains
// invalid instead of making one corrupt row prevent index creation on reopen.
const RECEIPT_CUSTODY_INDEX_SQL: &str = "CREATE INDEX IF NOT EXISTS artifacts_receipt_custody ON artifacts(id,CASE WHEN typeof(data)='text' AND json_valid(data) THEN json_remove(data,'$.files') ELSE data END)";
const RECEIPT_CUSTODY_QUERY: &str = "SELECT CASE WHEN typeof(data)='text' AND json_valid(data) THEN json_remove(data,'$.files') ELSE data END FROM artifacts INDEXED BY artifacts_receipt_custody WHERE id=?1";
const RECEIPT_CUSTODIES_QUERY: &str = "SELECT id,CASE WHEN typeof(data)='text' AND json_valid(data) THEN json_remove(data,'$.files') ELSE data END FROM artifacts INDEXED BY artifacts_receipt_custody WHERE id IN (SELECT value FROM json_each(?1))";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Operation {
    pub id: String,
    pub artifact: String,
    pub kind: String,
    pub state: String,
    pub request: String,
    pub created_at: i64,
    pub not_before: i64,
    pub attempts: u32,
    pub next_retry: i64,
    pub error: Option<String>,
}

/// New deletes bind authorization to an allocation, never just its reusable
/// row ID. Old journals remain readable but cannot authorize startup repair
/// without generation evidence.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum DeleteRequest {
    ManagedSeed(torrent_seed::ManagedSeedDeleteRequest),
    // Exact cancellation receipt and immutable owned selection binding keep
    // explicit byte authority separate from mutable Keep/revision changes.
    ClusterCancellation((String, u64, u64, bool, String, bool, String, String)),
    Cluster((String, u64, u64, bool, String, bool)),
    Scoped((String, u64, u64, bool, String)),
    Legacy((String, u64, u64, bool)),
}
impl DeleteRequest {
    fn fields(&self) -> (&str, u64, u64, bool, Option<&str>) {
        match self {
            Self::ManagedSeed(request) => (
                &request.artifact,
                request.revision,
                0,
                false,
                Some(&request.generation),
            ),
            Self::ClusterCancellation((id, rev, undo, auto, generation, _, _, _)) => {
                (id, *rev, *undo, *auto, Some(generation))
            }
            Self::Cluster((id, rev, undo, auto, generation, _)) => {
                (id, *rev, *undo, *auto, Some(generation))
            }
            Self::Scoped((id, rev, undo, auto, generation)) => {
                (id, *rev, *undo, *auto, Some(generation))
            }
            Self::Legacy((id, rev, undo, auto)) => (id, *rev, *undo, *auto, None),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Settings {
    pub enabled: bool,
    pub failed_retention_days: u32,
    pub recovery_root: PathBuf,
    pub consumer_token: String,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            enabled: false,
            failed_retention_days: 7,
            recovery_root: PathBuf::new(),
            consumer_token: String::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ListFilter {
    /// Every record, cleared ones included.
    All,
    /// Everything still on disk (the default view).
    #[default]
    Live,
    /// Live Runner records a person must look at (`exits::ATTENTION_SQL`):
    /// a hold with no automatic exit, an error / blocked cleanup / open
    /// intent, or a row that has outlived its registered exit.
    Attention,
    /// Live content Runner did not create (unowned, not handed off by
    /// Runner): informational, never deleted automatically, blocks nothing.
    Foreign,
    /// Live records Runner owns outright.
    Owned,
    /// Deleted / source-gone records kept for the audit trail.
    Cleared,
}
/// Live content Runner did not create: unowned and not a publication
/// Runner handed to its consumer (`transferred`).
const FOREIGN_SQL: &str = "json_extract(data,'$.owned')=0 AND state<>'transferred'";
impl ListFilter {
    fn sql(self) -> String {
        match self {
            Self::All => "1=1".into(),
            Self::Live => "state NOT IN ('deleted','source_gone')".into(),
            Self::Attention => format!(
                "state NOT IN ('deleted','source_gone') AND NOT ({FOREIGN_SQL}) AND {}",
                exits::ATTENTION_SQL.as_str()
            ),
            Self::Foreign => format!("state NOT IN ('deleted','source_gone') AND {FOREIGN_SQL}"),
            Self::Owned => {
                "state NOT IN ('deleted','source_gone') AND json_extract(data,'$.owned')=1".into()
            }
            Self::Cleared => "state IN ('deleted','source_gone')".into(),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ListSort {
    #[default]
    Updated,
    Size,
    Files,
    Name,
}
#[derive(Clone, Debug, Default)]
pub struct ListQuery {
    pub offset: usize,
    pub limit: usize,
    pub filter: ListFilter,
    pub sort: ListSort,
    /// Case-insensitive substring of the path.
    pub q: String,
}
#[derive(Clone, Debug, Serialize)]
pub struct ListRow {
    /// The record without its manifest; ask `measured` here, not
    /// `artifact.measured()`, which cannot see the stripped file list.
    pub artifact: Artifact,
    pub files: u64,
    pub bytes: u64,
    pub measured: bool,
}
#[derive(Clone, Debug, Default, Serialize, PartialEq, Eq)]
pub struct ListCounts {
    pub all: u64,
    pub live: u64,
    pub attention: u64,
    pub foreign: u64,
    pub owned: u64,
    pub cleared: u64,
    pub live_bytes: u64,
}
#[derive(Clone, Debug, Serialize)]
pub struct ListPage {
    pub rows: Vec<ListRow>,
    pub total: u64,
    pub counts: ListCounts,
}

pub struct Inventory {
    writer_naming: Mutex<std::collections::VecDeque<writer_naming::NamingEvidence>>,
    naming_leases: Mutex<std::collections::HashMap<(u32, u32), writer_naming::NamingLease>>,
    retired_naming: Mutex<Vec<writer_naming::ProducerRead>>,
    db: Mutex<Connection>,
    allocation_uses: Mutex<std::collections::HashSet<u32>>,
    reconciliation_uses: Mutex<std::collections::HashSet<(String, String)>>,
    /// Signalled whenever a reconciliation/namespace use is released, so an
    /// operator request can wait out a maintenance pass's momentary lease
    /// instead of failing against it.
    reconciliation_released: std::sync::Condvar,
    workspace_uses: Mutex<std::collections::HashMap<(String, String), usize>>,
    workspace_cursor: Mutex<String>,
    _process_lock: File,
    worker_session: String,
    closed: std::sync::atomic::AtomicBool,
    state_dir: PathBuf,
    /// Root of transform workspaces (`paths.workspace_dir`); defaults to
    /// `<state_dir>/transform-workspaces`.
    workspace_dir: std::sync::RwLock<PathBuf>,
    pub installation: String,
    // Serialized mutation coordinator. Reads never wait for filesystem work.
    mutation: Mutex<()>,
    // Per-journal-operation single flight. Digesting may release `mutation`,
    // but a competing reconciler must never overwrite this operation's newer
    // publication checkpoint.
    publication_active: Mutex<std::collections::HashSet<String>>,
    relocation_active: Mutex<std::collections::HashSet<String>>,
    reconcile_cursor: Mutex<String>,
    empty_failed_cursor: Mutex<String>,
    // One task runner at a time: the maintenance tick and an API kick can
    // both drain the queue, and a task must never execute twice.
    tasks: Mutex<()>,
}

struct AllocationReservation<'a> {
    inventory: &'a Inventory,
    job: u32,
}
impl Drop for AllocationReservation<'_> {
    fn drop(&mut self) {
        let _coordinator = self.inventory.mutation.lock().unwrap();
        self.inventory
            .allocation_uses
            .lock()
            .unwrap()
            .remove(&self.job);
    }
}

/// The Operation retry ladder, seconds after the Nth failed attempt
/// (review #283-12: one ladder, not a copy per caller).
pub(crate) const RETRY_LADDER_SECONDS: [i64; 4] = [30, 60, 120, 300];

/// Backoff after `attempts` failed attempts (1-based; the last rung
/// repeats).
pub(crate) fn retry_delay(attempts: u32) -> i64 {
    RETRY_LADDER_SECONDS[(attempts.saturating_sub(1) as usize).min(RETRY_LADDER_SECONDS.len() - 1)]
}

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
pub(crate) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
fn id(db: &Connection) -> Result<String> {
    Ok(db.query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0))?)
}
fn read<T: serde::de::DeserializeOwned>(db: &Connection, table: &str, key: &str) -> Result<T> {
    let value: Option<String> = db
        .query_row(
            &format!("SELECT data FROM {table} WHERE id=?1"),
            [key],
            |r| r.get(0),
        )
        .optional()?;
    serde_json::from_str(&value.ok_or(Error::NotFound)?).map_err(Into::into)
}
fn save_artifact(db: &Connection, a: &Artifact) -> Result<()> {
    let (files, bytes) = a.summary();
    db.execute("INSERT INTO artifacts(id,job,path,state,updated_at,data,files,bytes) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
        ON CONFLICT(id) DO UPDATE SET job=excluded.job,path=excluded.path,state=excluded.state,updated_at=excluded.updated_at,data=excluded.data,files=excluded.files,bytes=excluded.bytes",
        params![a.id, a.job, a.path.to_string_lossy(), a.state, a.updated_at, serde_json::to_string(a)?, files as i64, bytes as i64])?;
    Ok(())
}
fn save_operation(db: &Connection, op: &Operation) -> Result<()> {
    db.execute(
        "INSERT INTO operations(id,artifact,state,data) VALUES(?1,?2,?3,?4)
       ON CONFLICT(id) DO UPDATE SET state=excluded.state,data=excluded.data",
        params![op.id, op.artifact, op.state, serde_json::to_string(op)?],
    )?;
    Ok(())
}
/// The transform workspace root when `paths.workspace_dir` is not set.
pub fn default_workspace_dir(state_dir: &Path) -> PathBuf {
    state_dir.join("transform-workspaces")
}

fn event(db: &Connection, artifact: &str, kind: &str, detail: &str) -> Result<()> {
    db.execute(
        "INSERT INTO events(artifact,at,kind,detail) VALUES(?1,?2,?3,?4)",
        params![artifact, now(), kind, detail],
    )?;
    Ok(())
}

impl Inventory {
    /// Whether generation-bound transform journals own the current payload shape.
    /// Recovery must not apply old download CRCs after these operations.
    pub fn has_transform_custody(&self, job: u32) -> Result<bool> {
        let Some(source) = self.for_job(job)? else {
            return Ok(false);
        };
        let raws = {
            let db = self.db.lock().unwrap();
            let mut stmt=db.prepare("SELECT data FROM operations WHERE artifact=?1 AND state NOT IN ('cancelled','preview') AND json_extract(data,'$.kind') IN ('par_restore','par_repair','extract')")?;
            let rows = stmt.query_map([&source.id], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in raws {
            let op: Operation = serde_json::from_str(&raw)?;
            let request: serde_json::Value = serde_json::from_str(&op.request)?;
            let generation = if op.kind == "par_restore" {
                request
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|v| v.get("generation"))
                    .and_then(|v| v.as_str())
            } else {
                request
                    .get("source")
                    .and_then(|v| v.get("generation"))
                    .and_then(|v| v.as_str())
            };
            match generation {
                Some(g) if g == source.generation => return Ok(true),
                Some(_) => (),
                None => {
                    return Err(Error::Conflict(
                        "transform journal lacks generation evidence".into(),
                    ));
                }
            }
        }
        Ok(false)
    }

    /// Check the enrolled identity before any component may initialize this
    /// storage root. No database, lock, WAL, SHM, or directory is created.
    /// The real open repeats the check immediately before schema mutation.
    pub fn validate_expected(state_dir: &Path, expected: Option<&str>) -> Result<()> {
        let Some(expected) = expected else {
            return Ok(());
        };
        if expected.is_empty() {
            return Err(Error::Conflict(
                "empty enrolled installation identity".into(),
            ));
        }
        let database = state_dir.join("artifacts.sqlite");
        let metadata = std::fs::metadata(&database)?;
        if !metadata.is_file() {
            return Err(Error::Conflict(
                "enrolled inventory is not an existing regular database".into(),
            ));
        }
        // This existing-file lock prevents a cooperating inventory owner from
        // checkpointing/removing WAL while the preflight observes it. It is
        // released before the normal open; two inventory locks are never held.
        let lock = File::open(state_dir.join("artifacts.lock"))?;
        lock.try_lock().map_err(|error| {
            Error::Conflict(format!(
                "inventory identity preflight cannot acquire existing ownership lock: {error}"
            ))
        })?;
        let rollback = state_dir.join("artifacts.sqlite-journal");
        match std::fs::metadata(&rollback) {
            Ok(metadata) if metadata.len() > 0 => return Err(Error::Conflict("inventory has unresolved rollback evidence; preflight refuses to recover an unvalidated store".into())),
            Ok(_) => {},
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
            Err(error) => return Err(error.into()),
        }
        let wal_exists = state_dir.join("artifacts.sqlite-wal").try_exists()?;
        let db = if wal_exists {
            // SQLite's ordinary READ_ONLY WAL open can initialize/write SHM.
            // Existing-file RW permits byte-range EXCLUSIVE locking; query_only
            // prevents SQL writes, EXCLUSIVE uses an in-memory WAL index, and
            // NO_CKPT_ON_CLOSE prevents checkpoint/WAL removal on every exit.
            // WAL already exists under the ownership lock, so SQLite cannot
            // create it. No CREATE or persistent pragma is used in this path.
            let db = Connection::open_with_flags(
                &database,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            )?;
            db.set_db_config(
                rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
                true,
            )?;
            db.pragma_update(None, "query_only", true)?;
            db.pragma_update(None, "locking_mode", "EXCLUSIVE")?;
            db
        } else {
            // Immutable mode avoids creation of a WAL even for a checkpointed
            // WAL-mode main file. It is safe here only because no WAL exists
            // and the existing ownership lock excludes cooperative writers.
            let absolute = std::fs::canonicalize(&database)?;
            let mut uri = String::from("file:");
            for byte in absolute.as_os_str().as_encoded_bytes() {
                if byte.is_ascii_alphanumeric()
                    || matches!(*byte, b'/' | b':' | b'-' | b'_' | b'.' | b'~')
                {
                    uri.push(*byte as char);
                } else {
                    uri.push_str(&format!("%{byte:02X}"));
                }
            }
            uri.push_str("?mode=ro&immutable=1");
            Connection::open_with_flags(
                uri,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
            )?
        };
        let actual: String = db.query_row(
            "SELECT value FROM meta WHERE key='installation'",
            [],
            |row| row.get(0),
        )?;
        if actual != expected {
            return Err(Error::Conflict(
                "inventory installation identity differs from the enrolled store".into(),
            ));
        }
        // Drop the no-checkpoint connection while the ownership lock is held.
        drop(db);
        drop(lock);
        Ok(())
    }

    pub fn open(state_dir: &Path) -> Result<Self> {
        Self::open_expected(state_dir, None)
    }

    /// An enrolled store must exist and match before schema creation or migration.
    pub fn open_expected(state_dir: &Path, expected: Option<&str>) -> Result<Self> {
        Self::validate_expected(state_dir, expected)?;
        if let Some(expected) = expected {
            if expected.is_empty()
                || !state_dir.try_exists()?
                || !state_dir.join("artifacts.sqlite").try_exists()?
            {
                return Err(Error::Conflict("enrolled inventory is unavailable; refusing to create a replacement installation".into()));
            }
        } else {
            std::fs::create_dir_all(state_dir)?;
        }
        let process_lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(expected.is_none())
            .truncate(false)
            .open(state_dir.join("artifacts.lock"))?;
        process_lock.try_lock().map_err(|e| {
            Error::Conflict(format!(
                "inventory is already open; stop the daemon before restore: {e}"
            ))
        })?;
        let mut db = if expected.is_some() {
            Connection::open_with_flags(
                state_dir.join("artifacts.sqlite"),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            )?
        } else {
            Connection::open(state_dir.join("artifacts.sqlite"))?
        };
        db.busy_timeout(Duration::from_secs(5))?;
        // W5b: EXCLUSIVE before the first access, so WAL never creates or
        // maps `-shm` (unsupported on network filesystems; a truncated one
        // crash-looped nzbd with SIGBUS). The inventory is one connection
        // behind `artifacts.lock`; the CLI's backup/restore open it the same
        // way and refuse while the daemon holds it.
        db.pragma_update(None, "locking_mode", "EXCLUSIVE")?;
        if let Some(expected) = expected {
            let actual: String = db.query_row(
                "SELECT value FROM meta WHERE key='installation'",
                [],
                |row| row.get(0),
            )?;
            if actual != expected {
                return Err(Error::Conflict(
                    "inventory installation identity differs from the enrolled store".into(),
                ));
            }
        }
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
          CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS artifacts(id TEXT PRIMARY KEY,job INTEGER,path TEXT NOT NULL,state TEXT NOT NULL,updated_at INTEGER NOT NULL,data TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS artifacts_recent ON artifacts(updated_at DESC,id);
          CREATE INDEX IF NOT EXISTS artifacts_live_recent ON artifacts(updated_at DESC,id) WHERE state NOT IN ('deleted','source_gone');
          CREATE INDEX IF NOT EXISTS artifacts_job ON artifacts(job);
          CREATE INDEX IF NOT EXISTS artifacts_path ON artifacts(path);
          CREATE INDEX IF NOT EXISTS artifacts_state ON artifacts(state,updated_at);
          CREATE TABLE IF NOT EXISTS operations(id TEXT PRIMARY KEY,artifact TEXT NOT NULL,state TEXT NOT NULL,data TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS worker_controls(job INTEGER PRIMARY KEY,data TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS worker_authority(job INTEGER PRIMARY KEY,data TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS worker_grant_stops(id TEXT PRIMARY KEY,job INTEGER NOT NULL,incarnation TEXT NOT NULL,lease TEXT NOT NULL,revision INTEGER NOT NULL,control_revision INTEGER NOT NULL,phase TEXT NOT NULL,data TEXT NOT NULL CHECK(length(CAST(data AS BLOB))<=16384));
          CREATE INDEX IF NOT EXISTS worker_grant_stops_job ON worker_grant_stops(job,incarnation,lease);
          CREATE TABLE IF NOT EXISTS worker_removal_fences(job INTEGER NOT NULL,incarnation TEXT NOT NULL,revision INTEGER NOT NULL,PRIMARY KEY(job,incarnation));
          CREATE TABLE IF NOT EXISTS worker_completions(job INTEGER PRIMARY KEY,data TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS worker_completion_receipts(id TEXT PRIMARY KEY,job INTEGER NOT NULL,data TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS operations_state ON operations(state);
          CREATE INDEX IF NOT EXISTS operations_artifact ON operations(artifact);
          CREATE INDEX IF NOT EXISTS operations_transform_scratch ON operations(json_extract(json_extract(data,'$.request'),'$.scratch.id'),json_extract(json_extract(data,'$.request'),'$.scratch.generation')) WHERE json_extract(data,'$.kind') IN ('extract','par_repair');
          CREATE INDEX IF NOT EXISTS operations_scan_recent ON operations(json_extract(data,'$.kind'),json_extract(data,'$.created_at') DESC) WHERE json_extract(data,'$.kind')='scan';
          CREATE INDEX IF NOT EXISTS operations_kind ON operations(json_extract(data,'$.kind'));
          CREATE TABLE IF NOT EXISTS recoveries(id TEXT PRIMARY KEY,artifact TEXT NOT NULL,state TEXT NOT NULL,data TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS workspace_finalizations(source TEXT NOT NULL,generation TEXT NOT NULL,outcome TEXT NOT NULL,proof TEXT NOT NULL,job INTEGER,activated INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(source,generation));
          CREATE TABLE IF NOT EXISTS deliveries(sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,artifact TEXT NOT NULL,generation TEXT NOT NULL,data TEXT NOT NULL,cleanup_status TEXT NOT NULL DEFAULT 'pending',cleanup_error TEXT,cleanup_attempts INTEGER NOT NULL DEFAULT 0,cleanup_next_retry_at INTEGER,cleanup_blocked_since INTEGER,cleanup_started_at INTEGER,cleanup_last_attempt_at INTEGER,cleanup_completed_at INTEGER,UNIQUE(artifact,generation));
          CREATE TABLE IF NOT EXISTS delivery_cleanup_attempts(delivery_id TEXT NOT NULL,attempt INTEGER NOT NULL,started_at INTEGER NOT NULL,ended_at INTEGER,wait_ms INTEGER NOT NULL DEFAULT 0,outcome TEXT NOT NULL,error TEXT,PRIMARY KEY(delivery_id,attempt));
          CREATE INDEX IF NOT EXISTS deliveries_recent ON deliveries(sequence);
          CREATE TABLE IF NOT EXISTS job_terminals(job INTEGER PRIMARY KEY,transfer TEXT,outcome TEXT NOT NULL,reason TEXT,fetch_cause TEXT,stage TEXT,message TEXT,blames_release INTEGER NOT NULL DEFAULT 0,salvage TEXT NOT NULL,keep_on_failure INTEGER NOT NULL DEFAULT 0,fetched TEXT NOT NULL,publication TEXT,markers TEXT NOT NULL,entry TEXT NOT NULL,history_seq INTEGER,history_incarnation TEXT,markers_stamped INTEGER NOT NULL DEFAULT 0,emitted INTEGER NOT NULL DEFAULT 0,disposal TEXT,salvage_error TEXT,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
          CREATE INDEX IF NOT EXISTS job_terminals_salvage ON job_terminals(salvage);
          CREATE TABLE IF NOT EXISTS relocation_intents(generation TEXT PRIMARY KEY,job INTEGER NOT NULL,transfer TEXT,artifact TEXT NOT NULL,operation TEXT NOT NULL,source_dir TEXT NOT NULL,target_dir TEXT NOT NULL,cross_volume INTEGER NOT NULL,files TEXT NOT NULL,state TEXT NOT NULL,evidence TEXT,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
          CREATE INDEX IF NOT EXISTS relocation_intents_open ON relocation_intents(state);
          CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT,artifact TEXT NOT NULL,at INTEGER NOT NULL,kind TEXT NOT NULL,detail TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS events_artifact ON events(artifact,seq);
          INSERT OR IGNORE INTO meta VALUES('schema','1');
          INSERT OR IGNORE INTO meta VALUES('installation',lower(hex(randomblob(16))));")?;
        db.execute_batch(RECEIPT_CUSTODY_INDEX_SQL)?;
        let backlog_migrated: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM meta WHERE key='delivery_backlog_migration')",
            [],
            |row| row.get(0),
        )?;
        if !backlog_migrated {
            let tx = db.transaction()?;
            let mut statement = tx.prepare(
                "SELECT o.data FROM operations o LEFT JOIN artifacts a ON a.id=o.artifact WHERE o.state NOT IN ('succeeded','cancelled','review') AND json_extract(o.data,'$.kind') IN ('delete','prune') AND COALESCE(json_extract(a.data,'$.automatic_cleanup_eligible'),0)=0",
            )?;
            let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
            let pending = rows.collect::<std::result::Result<Vec<_>, _>>()?;
            drop(statement);
            for raw in pending {
                let mut operation: Operation = serde_json::from_str(&raw)?;
                operation.state = "review".into();
                operation.error =
                    Some("pre-upgrade destructive work requires reviewed migration".into());
                save_operation(&tx, &operation)?;
            }
            tx.execute(
                "INSERT INTO meta(key,value) VALUES('delivery_backlog_migration','1')",
                [],
            )?;
            tx.commit()?;
        }
        // M6b: there is no acceptance (contract §2.5). The v1 columns an M1a
        // or older build wrote are dropped; nothing reads them.
        for column in ["accepted_at", "consumer_receipt_id"] {
            let present: bool = db.query_row(
                "SELECT COUNT(*) FROM pragma_table_info('deliveries') WHERE name=?1",
                [column],
                |r| r.get::<_, i64>(0).map(|n| n != 0),
            )?;
            if present {
                db.execute_batch(&format!("ALTER TABLE deliveries DROP COLUMN {column}"))?;
            }
        }
        let has_cleanup_blocked_since: bool = db.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('deliveries') WHERE name='cleanup_blocked_since'",
            [],
            |r| r.get::<_, i64>(0).map(|n| n != 0),
        )?;
        if !has_cleanup_blocked_since {
            db.execute(
                "ALTER TABLE deliveries ADD COLUMN cleanup_blocked_since INTEGER",
                [],
            )?;
        }
        for column in [
            "cleanup_started_at",
            "cleanup_last_attempt_at",
            "cleanup_completed_at",
        ] {
            let exists: bool = db.query_row(
                "SELECT COUNT(*) FROM pragma_table_info('deliveries') WHERE name=?1",
                [column],
                |r| r.get::<_, i64>(0).map(|n| n != 0),
            )?;
            if !exists {
                db.execute(
                    &format!("ALTER TABLE deliveries ADD COLUMN {column} INTEGER"),
                    [],
                )?;
            }
        }
        // 0.9.0: the committed manifest lives in the move journal so a
        // delivery row never re-reads a publication its consumer may have
        // taken (contract §2.5). Additive and idempotent.
        let has_intent_manifest: bool = db.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('relocation_intents') WHERE name='manifest'",
            [],
            |r| r.get::<_, i64>(0).map(|n| n != 0),
        )?;
        if !has_intent_manifest {
            db.execute(
                "ALTER TABLE relocation_intents ADD COLUMN manifest TEXT",
                [],
            )?;
        }
        // 0.9.2: a publication handed to its consumer carries no earlier
        // attempt's error (the successful delivery is the later evidence).
        // Idempotent.
        db.execute(
            "UPDATE artifacts SET data=json_set(data,'$.error',NULL) WHERE state='transferred' AND json_extract(data,'$.error') IS NOT NULL",
            [],
        )?;
        {
            let tx = db.transaction()?;
            terminals::reopen_salvage_for_committed_publications(&tx, None)?;
            // 0.9.0 closed scope (W4): a source cleanup an older build
            // blocked on retained review files or per-file identity drift is
            // retried once under the new rule. Once per store.
            let migrated: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM meta WHERE key='closed_scope_cleanup_migration')",
                [],
                |row| row.get(0),
            )?;
            if !migrated {
                tx.execute("UPDATE operations SET data=json_set(data,'$.request',(json_set(json_extract(data,'$.request'),'$.cleanup.state','retry','$.cleanup.next_retry',0,'$.cleanup.error',NULL) || '')) WHERE state='succeeded' AND json_extract(data,'$.kind')='relocate' AND json_extract(json_extract(data,'$.request'),'$.cleanup.state')='blocked'", [])?;
                tx.execute("UPDATE deliveries SET cleanup_status='retry',cleanup_error=NULL,cleanup_next_retry_at=0,cleanup_blocked_since=NULL WHERE cleanup_status='blocked'", [])?;
                tx.execute(
                    "INSERT INTO meta(key,value) VALUES('closed_scope_cleanup_migration','1')",
                    [],
                )?;
            }
            tx.commit()?;
        }
        // A `running` cleanup belongs to a worker from the previous process.
        // The inventory lock was acquired above, so no live worker can race
        // this one-time boot recovery.
        let interrupted = db.transaction()?;
        interrupted.execute(
            "UPDATE delivery_cleanup_attempts SET ended_at=?1,outcome='interrupted',error='previous cleanup worker interrupted' WHERE ended_at IS NULL AND delivery_id IN (SELECT id FROM deliveries WHERE cleanup_status='running')",
            [now_millis()],
        )?;
        interrupted.execute(
            "UPDATE deliveries SET cleanup_status='retry',cleanup_error='previous cleanup worker interrupted',cleanup_next_retry_at=unixepoch() WHERE cleanup_status='running'",
            [],
        )?;
        // Only this process holds the inventory lock. Recover abandoned
        // non-Delivery claims through the same serialized journal format.
        interrupted.execute("UPDATE operations SET data=json_set(data,'$.request',(json_set(json_extract(data,'$.request'),'$.cleanup.state','retry','$.cleanup.next_retry',0) || '')) WHERE state='succeeded' AND json_extract(data,'$.kind')='relocate' AND json_extract(json_extract(data,'$.request'),'$.cleanup.state')='running'", [])?;
        interrupted.commit()?;
        let schema: String =
            db.query_row("SELECT value FROM meta WHERE key='schema'", [], |r| {
                r.get(0)
            })?;
        if schema != "1" {
            return Err(Error::Conflict(format!(
                "unsupported inventory schema {schema}"
            )));
        }
        let installation =
            db.query_row("SELECT value FROM meta WHERE key='installation'", [], |r| {
                r.get(0)
            })?;
        // Summary columns (schema 1, additive): the list sorts and counts by
        // size without deserializing every manifest. Backfilled once from
        // the JSON so an inventory written before the columns existed is
        // sortable the moment it opens.
        let has_summary: bool = db.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('artifacts') WHERE name='bytes'",
            [],
            |r| r.get::<_, i64>(0).map(|n| n > 0),
        )?;
        if !has_summary {
            db.execute_batch(
                "BEGIN;
                 ALTER TABLE artifacts ADD COLUMN files INTEGER NOT NULL DEFAULT 0;
                 ALTER TABLE artifacts ADD COLUMN bytes INTEGER NOT NULL DEFAULT 0;
                 UPDATE artifacts SET
                   files=(SELECT COUNT(*) FROM json_each(json_extract(artifacts.data,'$.files')) f
                          WHERE json_extract(f.value,'$.identity.directory')=0),
                   bytes=(SELECT COALESCE(SUM(json_extract(f.value,'$.identity.bytes')),0)
                          FROM json_each(json_extract(artifacts.data,'$.files')) f
                          WHERE json_extract(f.value,'$.identity.directory')=0);
                 CREATE INDEX IF NOT EXISTS artifacts_bytes ON artifacts(bytes DESC,updated_at DESC,id);
                 COMMIT;",
            )?;
        }
        fs::sync_directory(&fs::open_dir(state_dir)?)?;
        let worker_session = id(&db)?;
        let inventory = Self {
            writer_naming: Mutex::new(std::collections::VecDeque::new()),
            naming_leases: Mutex::new(std::collections::HashMap::new()),
            retired_naming: Mutex::new(Vec::new()),
            db: Mutex::new(db),
            allocation_uses: Mutex::new(std::collections::HashSet::new()),
            reconciliation_uses: Mutex::new(std::collections::HashSet::new()),
            reconciliation_released: std::sync::Condvar::new(),
            workspace_uses: Mutex::new(std::collections::HashMap::new()),
            workspace_cursor: Mutex::new(String::new()),
            _process_lock: process_lock,
            worker_session,
            closed: std::sync::atomic::AtomicBool::new(false),
            state_dir: state_dir.into(),
            workspace_dir: std::sync::RwLock::new(default_workspace_dir(state_dir)),
            installation,
            mutation: Mutex::new(()),
            publication_active: Mutex::new(std::collections::HashSet::new()),
            relocation_active: Mutex::new(std::collections::HashSet::new()),
            reconcile_cursor: Mutex::new(String::new()),
            empty_failed_cursor: Mutex::new(String::new()),
            tasks: Mutex::new(()),
        };
        inventory.abandon_reviewed_transforms()?;
        // No-wedge: upgrade and restart conversion of review/retained rows
        // (database-only exits; the tick runs the rest).
        if let Err(error) = inventory.exit_wedged_states(0, false) {
            tracing::warn!(%error, "automatic exits at open deferred to the tick");
        }
        Ok(inventory)
    }
    fn mutation_guard(&self) -> Result<std::sync::MutexGuard<'_, ()>> {
        let guard = self.mutation.lock().unwrap();
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::Conflict("inventory has stopped".into()));
        }
        Ok(guard)
    }
    fn reserve_allocation(&self, job: u32) -> Result<AllocationReservation<'_>> {
        let _coordinator = self.mutation_guard()?;
        if self.for_job(job)?.is_some_and(|artifact| {
            self.source_in_use(&SourceGeneration {
                artifact: artifact.id,
                generation: artifact.generation,
            })
        }) || !self.allocation_uses.lock().unwrap().insert(job)
        {
            return Err(Error::Conflict(
                "allocation or processing custody is already active".into(),
            ));
        }
        Ok(AllocationReservation {
            inventory: self,
            job,
        })
    }
    /// Called only after the engine's writers and maintenance tasks finish.
    /// Old API handles can no longer read or mutate a restarted daemon's
    /// store: the connection (and its EXCLUSIVE lock) is released.
    pub fn close(&self) -> Result<()> {
        let first_close = {
            let _guard = self.mutation.lock().unwrap();
            !self.closed.swap(true, std::sync::atomic::Ordering::AcqRel)
        };
        self.invalidate_all_writer_naming();
        self.drain_writer_naming();
        if first_close {
            // W5b: the store's EXCLUSIVE lock goes with its connection, so a
            // replacement backend in this process can open it. A stale
            // handle's reads now fail instead of answering from a store
            // another owner is writing.
            *self.db.lock().unwrap() = Connection::open_in_memory()?;
            self._process_lock.unlock()?;
        }
        Ok(())
    }
    pub fn settings(&self) -> Result<Settings> {
        let db = self.db.lock().unwrap();
        let raw: Option<String> = db
            .query_row("SELECT value FROM meta WHERE key='settings'", [], |r| {
                r.get(0)
            })
            .optional()?;
        Ok(raw
            .map(|s| serde_json::from_str(&s))
            .transpose()?
            .unwrap_or_default())
    }
    /// Advisory prerequisites never prevent accepting the enable switch.
    pub fn set_settings(&self, s: &Settings) -> Result<()> {
        let _guard = self.mutation_guard()?;
        if s.failed_retention_days > 3650 {
            return Err(Error::Conflict(
                "retention must be at most 3650 days".into(),
            ));
        }
        self.db.lock().unwrap().execute("INSERT INTO meta VALUES('settings',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [serde_json::to_string(s)?])?;
        Ok(())
    }
    pub fn get(&self, key: &str) -> Result<Artifact> {
        read(&self.db.lock().unwrap(), "artifacts", key)
    }
    /// Read fresh custody metadata using the transactionally maintained
    /// covering expression index, without loading or decoding file manifests.
    /// Validate the remaining full Artifact header so malformed fields and
    /// legacy defaults keep their existing semantics. No custody is cached.
    pub fn receipt_custody(&self, key: &str) -> Result<ArtifactCustody> {
        let raw: Option<String> = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare_cached(RECEIPT_CUSTODY_QUERY)?;
            statement.query_row([key], |row| row.get(0)).optional()?
        };
        ArtifactCustody::from_header(&raw.ok_or(Error::NotFound)?)
    }
    /// Read all requested custody headers in one consistent SQLite statement
    /// and one database-lock round trip. Missing keys are absent; malformed
    /// rows retain their own errors without hiding unrelated valid rows.
    pub fn receipt_custodies(
        &self,
        keys: &[String],
    ) -> Result<std::collections::HashMap<String, Result<ArtifactCustody>>> {
        if keys.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let keys = serde_json::to_string(keys)?;
        let rows = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare_cached(RECEIPT_CUSTODIES_QUERY)?;
            let rows = statement.query_map([keys], |row| {
                // A non-text corrupt header is a per-row error too. Failure
                // to step the statement itself still fails the whole read.
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        Ok(rows
            .into_iter()
            .map(|(key, raw)| {
                (
                    key,
                    raw.map_err(Error::Sql)
                        .and_then(|raw| ArtifactCustody::from_header(&raw)),
                )
            })
            .collect())
    }
    pub fn for_job(&self, job: u32) -> Result<Option<Artifact>> {
        let db = self.db.lock().unwrap();
        let raw: Option<String> = db
            .query_row(
                "SELECT data FROM artifacts WHERE job=?1 ORDER BY updated_at DESC LIMIT 1",
                [job],
                |r| r.get(0),
            )
            .optional()?;
        Ok(raw.map(|s| serde_json::from_str(&s)).transpose()?)
    }
    pub fn for_path(&self, path: &Path) -> Result<Option<Artifact>> {
        let db = self.db.lock().unwrap();
        let raw: Option<String> = db
            .query_row(
                "SELECT data FROM artifacts WHERE path=?1 ORDER BY updated_at DESC LIMIT 1",
                [path.to_string_lossy()],
                |r| r.get(0),
            )
            .optional()?;
        Ok(raw.map(|s| serde_json::from_str(&s)).transpose()?)
    }
    pub fn list(&self, offset: usize, limit: usize) -> Result<Vec<Artifact>> {
        self.list_visible(offset, limit, true)
    }
    pub fn list_visible(
        &self,
        offset: usize,
        limit: usize,
        include_terminal: bool,
    ) -> Result<Vec<Artifact>> {
        let db = self.db.lock().unwrap();
        let query = if include_terminal {
            "SELECT data FROM artifacts ORDER BY updated_at DESC,id LIMIT ?1 OFFSET ?2"
        } else {
            "SELECT data FROM artifacts WHERE state NOT IN ('deleted','source_gone') ORDER BY updated_at DESC,id LIMIT ?1 OFFSET ?2"
        };
        let mut stmt = db.prepare(query)?;
        let rows = stmt.query_map(params![limit.min(200) as i64, offset as i64], |r| {
            r.get::<_, String>(0)
        })?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }
    /// One page of the inventory, shaped for a list: manifests are stripped
    /// in SQL (`json_remove`) so a 100k-entry folder costs the same as an
    /// empty one, and every filter has a count so the UI can say how many
    /// rows sit behind each view instead of making the operator page to find out.
    pub fn list_page(&self, q: &ListQuery) -> Result<ListPage> {
        let db = self.db.lock().unwrap();
        let mut sql_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        let mut where_sql = q.filter.sql();
        let needle = q.q.trim();
        if !needle.is_empty() {
            let escaped = needle
                .replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_");
            where_sql.push_str(" AND path LIKE ?1 ESCAPE '\\'");
            sql_params.push(Box::new(format!("%{escaped}%")));
        }
        let order = match q.sort {
            ListSort::Updated => "updated_at DESC,id",
            ListSort::Size => "bytes DESC,updated_at DESC,id",
            ListSort::Files => "files DESC,updated_at DESC,id",
            ListSort::Name => "path COLLATE NOCASE ASC,id",
        };
        let limit = q.limit.clamp(1, 200) as i64;
        let total: i64 = db.query_row(
            &format!("SELECT COUNT(*) FROM artifacts WHERE {where_sql}"),
            rusqlite::params_from_iter(sql_params.iter().map(|p| p.as_ref())),
            |r| r.get(0),
        )?;
        let mut stmt = db.prepare(&format!(
            "SELECT json_remove(data,'$.files'),files,bytes,json_array_length(json_extract(data,'$.files')) FROM artifacts WHERE {where_sql} ORDER BY {order} LIMIT {limit} OFFSET {}",
            q.offset as i64
        ))?;
        let rows = stmt
            .query_map(
                rusqlite::params_from_iter(sql_params.iter().map(|p| p.as_ref())),
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, Option<i64>>(3)?.unwrap_or(0),
                    ))
                },
            )?
            .map(|r| {
                let (raw, files, bytes, entries) = r?;
                let artifact: Artifact = serde_json::from_str(&raw)?;
                Ok(ListRow {
                    measured: artifact.inspected_at.is_some() || entries > 0,
                    artifact,
                    files: files.max(0) as u64,
                    bytes: bytes.max(0) as u64,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut counts = ListCounts::default();
        for (filter, slot) in [
            (ListFilter::All, &mut counts.all),
            (ListFilter::Live, &mut counts.live),
            (ListFilter::Attention, &mut counts.attention),
            (ListFilter::Foreign, &mut counts.foreign),
            (ListFilter::Owned, &mut counts.owned),
            (ListFilter::Cleared, &mut counts.cleared),
        ] {
            *slot = db.query_row(
                &format!("SELECT COUNT(*) FROM artifacts WHERE {}", filter.sql()),
                [],
                |r| r.get::<_, i64>(0),
            )? as u64;
        }
        counts.live_bytes = db.query_row(
            &format!(
                "SELECT COALESCE(SUM(bytes),0) FROM artifacts WHERE {}",
                ListFilter::Live.sql()
            ),
            [],
            |r| r.get::<_, i64>(0),
        )? as u64;
        Ok(ListPage {
            rows,
            total: total as u64,
            counts,
        })
    }
    pub fn events(&self, key: &str, after: i64) -> Result<Vec<serde_json::Value>> {
        let db = self.db.lock().unwrap();
        let mut stmt = db.prepare("SELECT seq,at,kind,detail FROM events WHERE artifact=?1 AND seq>?2 ORDER BY seq LIMIT 100")?;
        let rows = stmt.query_map(params![key,after], |r| Ok(serde_json::json!({"seq":r.get::<_,i64>(0)?,"at":r.get::<_,i64>(1)?,"kind":r.get::<_,String>(2)?,"detail":r.get::<_,String>(3)?})))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }
    fn sidecar(&self, a: &Artifact) -> Result<()> {
        let dir = self.state_dir.join("artifact-identities").join(&a.id);
        std::fs::create_dir_all(&dir)?;
        let target = dir.join(format!("{}.json", a.generation));
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let tmp = dir.join(format!(
            "{}.{}.{nonce}.tmp",
            a.generation,
            std::process::id()
        ));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        let result = (|| {
            file.write_all(&serde_json::to_vec(&serde_json::json!({"installation":self.installation,"artifact":a.id,"generation":a.generation,"path":a.path,"root":a.root,"root_identity":a.root_identity,"identity":a.identity}))?)?;
            file.sync_all()?;
            std::fs::rename(&tmp, &target)?;
            fs::sync_directory(&fs::open_dir(&dir)?)?;
            fs::sync_directory(&fs::open_dir(dir.parent().unwrap())?)?;
            fs::sync_directory(&fs::open_dir(&self.state_dir)?)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(tmp);
        }
        result
    }
    /// Commit allocation intent before the directory exists or a writer opens
    /// a file. A directory already on disk must be explicitly adopted instead.
    pub fn allocate(&self, job: u32, root: &Path, path: &Path) -> Result<Artifact> {
        let _reservation = self.reserve_allocation(job)?;
        self.allocate_unlocked(job, root, path, false, false, false)
    }
    pub fn allocate_for_delivery(
        &self,
        job: u32,
        root: &Path,
        path: &Path,
        automatic_cleanup_eligible: bool,
        delivery_lifecycle: bool,
    ) -> Result<Artifact> {
        let _reservation = self.reserve_allocation(job)?;
        self.allocate_for_delivery_unlocked(
            job,
            root,
            path,
            automatic_cleanup_eligible,
            delivery_lifecycle,
            false,
        )
    }
    /// Allocate a fresh delivery-owned source while atomically seeding the
    /// opted-in per-job failure Keep setting. Existing artifacts are returned
    /// unchanged, so a stale queue snapshot cannot overwrite later controls.
    pub fn allocate_for_delivery_with_keep(
        &self,
        job: u32,
        root: &Path,
        path: &Path,
        automatic_cleanup_eligible: bool,
        delivery_lifecycle: bool,
        keep_on_failure: bool,
    ) -> Result<Artifact> {
        let _reservation = self.reserve_allocation(job)?;
        self.allocate_for_delivery_unlocked(
            job,
            root,
            path,
            automatic_cleanup_eligible,
            delivery_lifecycle,
            keep_on_failure,
        )
    }
    fn allocate_for_delivery_unlocked(
        &self,
        job: u32,
        root: &Path,
        path: &Path,
        automatic_cleanup_eligible: bool,
        delivery_lifecycle: bool,
        keep_on_failure: bool,
    ) -> Result<Artifact> {
        self.allocate_unlocked(
            job,
            root,
            path,
            automatic_cleanup_eligible,
            delivery_lifecycle,
            keep_on_failure,
        )
    }
    /// Allocate a fresh private executor generation, preserving prior custody
    /// and operator holds. Existing paths never confer ownership.
    pub fn allocate_processing_successor(
        &self,
        job: u32,
        root: &Path,
        path: &Path,
    ) -> Result<Artifact> {
        self.allocate_processing_successor_with_policy(job, root, path, None)
    }

    /// Allocate a private processing successor using immutable policy stamped
    /// on the admitted queue row. Cluster grants remain non-owned and
    /// ineligible; only the newly created, owned processing copy receives the
    /// persisted admission cohort.
    pub fn allocate_processing_successor_with_policy(
        &self,
        job: u32,
        root: &Path,
        path: &Path,
        admitted_policy: Option<(bool, bool, bool)>,
    ) -> Result<Artifact> {
        let _reservation = self.reserve_allocation(job)?;
        fs::absolute(path)?;
        if path.parent() != Some(root) || std::fs::symlink_metadata(path).is_ok() {
            return Err(Error::Conflict(
                "private processing generation requires a fresh path".into(),
            ));
        }
        fs::open_dir(root)?;
        let coordinator = self.mutation_guard()?;
        let previous = self.for_job(job)?;
        // A cluster grant is shared, non-owned input, so its private successor
        // can inherit the immutable admission cohort. An already-owned source
        // is authoritative for mutable controls (especially Keep); a stale
        // queue snapshot must never overwrite a later user choice or upgrade
        // that source's legacy eligibility.
        let cohort = match (&previous, admitted_policy) {
            (Some(previous), _) if previous.owned => (
                previous.automatic_cleanup_eligible,
                previous.delivery_lifecycle,
                previous.keep_on_failure,
            ),
            (_, Some(policy)) => policy,
            (Some(previous), None) => (
                previous.automatic_cleanup_eligible,
                previous.delivery_lifecycle,
                previous.keep_on_failure,
            ),
            (None, None) => (false, false, false),
        };
        if let Some(mut previous) = previous {
            if self.source_in_use(&SourceGeneration {
                artifact: previous.id.clone(),
                generation: previous.generation.clone(),
            }) {
                return Err(Error::Conflict(
                    "previous processing workers have not quiesced".into(),
                ));
            }
            previous.job = None;
            if matches!(previous.state.as_str(), "active" | "transitioning") {
                previous.state = "retained".into();
                previous
                    .hold
                    .get_or_insert_with(|| "review: superseded private processing source".into());
            }
            previous.revision += 1;
            save_artifact(&self.db.lock().unwrap(), &previous)?;
        }
        drop(coordinator);
        self.allocate_unlocked(job, root, path, cohort.0, cohort.1, cohort.2)
    }
    fn allocate_unlocked(
        &self,
        job: u32,
        root: &Path,
        path: &Path,
        automatic_cleanup_eligible: bool,
        delivery_lifecycle: bool,
        keep_on_failure: bool,
    ) -> Result<Artifact> {
        if let Some(a) = self.for_job(job)? {
            if a.path == path && a.state == "active" {
                self.verify(&a)?;
                let _coordinator = self.mutation_guard()?;
                let current = self.get(&a.id)?;
                if current.generation != a.generation
                    || current.path != a.path
                    || current.state != "active"
                    || current.hold.is_some()
                {
                    return Err(Error::Conflict(
                        "allocation changed while validating root".into(),
                    ));
                }
                return Ok(current);
            }
            return Err(Error::Conflict(
                "job already owns a different or terminal allocation".into(),
            ));
        }
        fs::absolute(path)?;
        if path.parent() != Some(root) {
            return Err(Error::Conflict(
                "allocation must be an immediate child of its root".into(),
            ));
        }
        let root_file = fs::open_dir(root)?;
        let root_identity = fs::identity_for_file(&root_file)?;
        if std::fs::symlink_metadata(path).is_ok() {
            return Err(Error::Conflict(
                "existing payload needs explicit ownership review".into(),
            ));
        }
        let coordinator = self.mutation_guard()?;
        if self.for_job(job)?.is_some() {
            return Err(Error::Conflict(
                "allocation appeared during root validation".into(),
            ));
        }
        let db = self.db.lock().unwrap();
        let mut a = Artifact {
            id: id(&db)?,
            generation: id(&db)?,
            revision: 1,
            job: Some(job),
            path: path.into(),
            origin_path: path.into(),
            root: root.into(),
            root_identity,
            identity: None,
            state: "allocating".into(),
            owned: cfg!(unix),
            automatic_cleanup_eligible,
            delivery_lifecycle,
            keep: !cfg!(unix),
            keep_on_failure,
            failure_assessment: None,
            hold: None,
            created_at: now(),
            updated_at: now(),
            retention_seconds: 0,
            deadline: None,
            files: Vec::new(),
            error: None,
            inspected_at: None,
            salvage: None,
            failure_reason: None,
            gone_reason: None,
        };
        save_artifact(&db, &a)?;
        drop(db);
        drop(coordinator);
        std::fs::create_dir(path)?;
        fs::sync_directory(&root_file)?;
        a.identity = Some(fs::identity_for_file(&fs::open_dir(path)?)?);
        self.sidecar(&a)?;
        let _coordinator = self.mutation_guard()?;
        let current = self.get(&a.id)?;
        if current.generation != a.generation
            || current.revision != a.revision
            || current.state != "allocating"
        {
            return Err(Error::Conflict(
                "allocation intent changed before completion".into(),
            ));
        }
        a.state = "active".into();
        save_artifact(&self.db.lock().unwrap(), &a)?;
        Ok(a)
    }
    /// Upgrade a live queue record without claiming its pre-existing bytes.
    /// It may resume writing; destructive actions still need operator adoption.
    pub fn register_legacy_active(&self, job: u32, root: &Path, path: &Path) -> Result<()> {
        let previous = self.for_job(job)?;
        if let Some(previous) = previous.as_ref().filter(|previous| previous.path == path) {
            // I/O uncertainty cannot revoke an established allocation. Capture
            // and verify outside the coordinator, then preserve newer controls.
            self.verify(previous)?;
            let _coordinator = self.mutation_guard()?;
            self.check_payload_capture(previous)?;
            if !previous.owned
                && previous.keep
                && previous.state == "active"
                && previous.hold.as_deref() == Some("review")
            {
                let mut admitted = previous.clone();
                admitted.hold = None;
                admitted.revision += 1;
                save_artifact(&self.db.lock().unwrap(), &admitted)?;
            }
            return Ok(());
        }
        // Inspect the destination before detaching the prior association. Even
        // a proven missing old directory requires a verified enrolled root.
        let previous_present = if let Some(previous) = &previous {
            self.verify_root(previous)?;
            let present = previous.path.try_exists()?;
            if present {
                self.verify(previous)?;
            }
            present
        } else {
            false
        };
        if !path.try_exists()? {
            return Ok(());
        }
        let mut admitted = self.discover(root, path, true)?;
        self.verify(&admitted)?;
        let captured = admitted.clone();
        let _coordinator = self.mutation_guard()?;
        self.check_payload_capture(&captured)?;
        match (&previous, self.for_job(job)?) {
            (Some(expected), Some(current)) if current.id == expected.id => {
                self.check_payload_capture(expected)?;
                if self.artifact_in_use(expected)? {
                    return Err(Error::Conflict("legacy allocation remains in use".into()));
                }
            }
            (None, None) => (),
            _ => {
                return Err(Error::Conflict(
                    "legacy job association changed during enrollment".into(),
                ));
            }
        }
        if admitted.job.is_some_and(|owner| owner != job) {
            return Err(Error::Conflict("legacy path belongs to another job".into()));
        }
        // This generic hold belongs to automatic unowned discovery. Queue
        // admission permits operational verification/writes, while Keep and
        // unowned custody continue to prohibit automatic payload cleanup.
        if !admitted.owned
            && admitted.keep
            && matches!(admitted.state.as_str(), "active_unverified" | "unknown")
            && admitted.hold.as_deref() == Some("review")
        {
            admitted.hold = None;
        }
        admitted.job = Some(job);
        admitted.state = "active".into();
        admitted.revision += 1;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        if let Some(mut previous) = previous {
            previous.job = None;
            previous.state = if previous_present {
                "retained"
            } else {
                "source_gone"
            }
            .into();
            previous.keep = true;
            previous
                .hold
                .get_or_insert_with(|| "review: previous external job generation".into());
            previous.revision += 1;
            save_artifact(&tx, &previous)?;
        }
        save_artifact(&tx, &admitted)?;
        tx.commit()?;
        Ok(())
    }

    /// Compatibility entry point for callers without a payload scheduler.
    /// Engine startup dispatches these roots independently instead.
    pub fn reconcile_startup(&self, live_jobs: &[u32]) -> Result<()> {
        self.reconcile_torrent_stage_cleanups(&|| Ok(()))?;
        self.reconcile_torrent_copy_compaction(&|| Ok(()))?;
        for root in self.startup_reconciliation_roots()? {
            self.reconcile_startup_root(&root, live_jobs)?;
        }
        Ok(())
    }

    pub fn prepare_forget(&self, job: u32) -> Result<Option<Artifact>> {
        let _guard = self.mutation_guard()?;
        let Some(mut a) = self.for_job(job)? else {
            return Ok(None);
        };
        if a.state == "retiring" {
            return Ok(Some(a));
        }
        if a.state != "active" {
            return Ok(None);
        }
        a.state = "retiring".into();
        a.hold = Some("waiting for writer stop".into());
        a.revision += 1;
        a.updated_at = now();
        save_artifact(&self.db.lock().unwrap(), &a)?;
        Ok(Some(a))
    }

    pub fn reconcile_missing(&self) -> Result<()> {
        let raws = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare("SELECT data FROM artifacts WHERE state IN ('completed','retained','unknown','retiring') AND id>?1 ORDER BY id LIMIT 1000")?;
            let cursor = self.reconcile_cursor.lock().unwrap().clone();
            let rows = stmt.query_map([cursor], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        if raws.is_empty() {
            self.reconcile_cursor.lock().unwrap().clear();
        }
        for raw in raws {
            let original: Artifact = serde_json::from_str(&raw)?;
            *self.reconcile_cursor.lock().unwrap() = original.id.clone();
            let Some(_lease) = self.reserve_reconciliation(std::slice::from_ref(&original))? else {
                continue;
            };
            if self.verify_root(&original).is_err() {
                continue;
            }
            if matches!(std::fs::symlink_metadata(&original.path), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
            {
                let _coordinator = self.mutation_guard()?;
                let mut current = self.get(&original.id)?;
                if current.generation != original.generation
                    || current.path != original.path
                    || current.root != original.root
                    || current.revision != original.revision
                {
                    continue;
                }
                current.state = "source_gone".into();
                current.updated_at = now();
                current.revision += 1;
                save_artifact(&self.db.lock().unwrap(), &current)?;
            }
        }
        Ok(())
    }

    /// Dispose of retained failed sources that hold no files.
    ///
    /// Retention exists to keep bytes that may still become media. A
    /// retained generation whose complete manifest walk finds no regular file
    /// keeps nothing, yet under Runner custody it is exactly what Curator
    /// waits on before it may replace the failed release. Releases that failed
    /// health before any file finished were retained this way "for recovery"
    /// (field report 2026-11-02): eleven empty directories froze a whole
    /// season. Classification now disposes them at failure time; this sweep
    /// settles generations retained before that, and any whose bytes were
    /// removed by hand, through the ordinary audited delete operation.
    pub fn reconcile_empty_failed_sources(&self) -> Result<()> {
        let raws = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare(
                "SELECT data FROM artifacts WHERE state='retained' AND job IS NOT NULL AND id>?1 ORDER BY id LIMIT 200",
            )?;
            let cursor = self.empty_failed_cursor.lock().unwrap().clone();
            let rows = stmt.query_map([cursor], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        if raws.is_empty() {
            self.empty_failed_cursor.lock().unwrap().clear();
        }
        for raw in raws {
            let artifact: Artifact = serde_json::from_str(&raw)?;
            *self.empty_failed_cursor.lock().unwrap() = artifact.id.clone();
            if !artifact.owned
                || !artifact.automatic_cleanup_eligible
                || artifact.keep
                || artifact.keep_on_failure
                || artifact.hold.is_some()
            {
                continue;
            }
            let empty = {
                let Some(_lease) = self.reserve_reconciliation(std::slice::from_ref(&artifact))?
                else {
                    continue;
                };
                if self.verify_root(&artifact).is_err() {
                    continue;
                }
                let Ok(dir) = self.verify(&artifact) else {
                    continue;
                };
                // One directory read per tick, not a manifest walk: the
                // sweep runs every 30 s over /processing, which may be NFS.
                // Only a directory with no entries at all qualifies; any
                // entry, or an error reading it, leaves it for a person.
                fs::names(&dir).is_ok_and(|names| names.is_empty())
            };
            if !empty {
                continue;
            }
            // The same authority Keep-off uses for a no-Delivery failed
            // source: a persisted `unusable` assessment pinned to this
            // generation, then the generation-bound failed-delete operation
            // (which a later Keep still cancels).
            let assessed = match self.set_failure_assessment(
                &artifact.id,
                &artifact.generation,
                "unusable",
                "the failed attempt left no source files; nothing to retain",
            ) {
                Ok(assessed) => assessed,
                Err(error) => {
                    tracing::warn!(artifact = %artifact.id, %error,
                        "empty retained failed source could not be assessed");
                    continue;
                }
            };
            let request = format!("failed-delete-{}", assessed.id);
            match self.request_delete(&assessed.id, assessed.revision, &request, 0) {
                Ok(_) => tracing::info!(
                    artifact = %artifact.id,
                    job = ?artifact.job,
                    "retained failed source holds no files; disposing it"
                ),
                Err(error) => tracing::warn!(
                    artifact = %artifact.id,
                    %error,
                    "empty retained failed source could not be queued for disposal"
                ),
            }
        }
        Ok(())
    }

    pub fn protect_roots(&self, roots: &[PathBuf]) -> Result<()> {
        let _guard = self.mutation_guard()?;
        self.db.lock().unwrap().execute("INSERT INTO meta VALUES('protected_roots',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",[serde_json::to_string(roots)?])?;
        Ok(())
    }
    fn verify_root(&self, a: &Artifact) -> Result<File> {
        #[cfg(test)]
        startup::before_root_open(&a.root);
        let raw: Option<String> = self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT value FROM meta WHERE key='protected_roots'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(raw) = raw {
            let roots: Vec<PathBuf> = serde_json::from_str(&raw)?;
            if roots
                .iter()
                .any(|root| root == &a.path || root.starts_with(&a.path))
            {
                return Err(Error::Conflict(
                    "payload overlaps a configured directory role".into(),
                ));
            }
        }
        let root = fs::open_dir(&a.root)?;
        if !a.root_identity.same_object(&fs::identity_for_file(&root)?) {
            return Err(Error::Conflict(
                "configured root identity changed or volume is unavailable".into(),
            ));
        }
        Ok(root)
    }
    /// Where transform workspaces are created.
    pub fn workspace_dir(&self) -> PathBuf {
        self.workspace_dir.read().unwrap().clone()
    }

    /// Use `dir` for transform workspaces (`paths.workspace_dir`). Rows
    /// recorded under the default root (`<state_dir>/transform-workspaces`)
    /// while `dir` is elsewhere — the operator moved the directory — are
    /// rewritten to the same relative path under `dir` in every table. While
    /// the old root still holds workspaces (not moved), nothing is rewritten:
    /// they finish where they are. Then every workspace recorded under `dir`
    /// whose recorded root is not the live root object (the root was moved
    /// or copied there) is re-stamped — the artifact row, the transform
    /// journal's own copy of the scratch and its attempt directories, and the
    /// identity sidecar (Runner's private scratch: the move is the
    /// operator's, not evidence against the payload). That re-stamp is
    /// driven by the mismatch, not by this run's rewrite, so a crash between
    /// the commit and the sidecar writes is finished by the next start.
    /// Idempotent: a second run changes nothing. Returns the number of
    /// rewritten values.
    pub fn configure_workspace_dir(&self, dir: &Path) -> Result<usize> {
        fs::absolute(dir)?;
        let old = default_workspace_dir(&self.state_dir);
        *self.workspace_dir.write().unwrap() = dir.to_path_buf();
        let new_text = dir
            .to_str()
            .ok_or_else(|| Error::Conflict("non-UTF-8 workspace path".into()))?
            .to_owned();
        let mut rewritten = 0;
        if dir != old {
            if dir.starts_with(&old) || old.starts_with(dir) {
                return Err(Error::Conflict(format!(
                    "paths.workspace_dir {} must not be nested with the default {}",
                    dir.display(),
                    old.display()
                )));
            }
            // Not moved (the old root still holds workspaces): the recorded
            // absolute paths stay valid where they are, and only new
            // workspaces go to `dir`. Rewriting them now would point them at
            // nothing.
            if std::fs::read_dir(&old).is_ok_and(|mut entries| entries.next().is_some()) {
                tracing::warn!(old = %old.display(), new = %dir.display(),
                    "workspace_dir changed but the default workspace root still holds workspaces; \
                     existing ones finish where they are");
            } else {
                let old_text = old
                    .to_str()
                    .ok_or_else(|| Error::Conflict("non-UTF-8 workspace path".into()))?
                    .to_owned();
                rewritten = self.rewrite_workspace_paths(&old_text, &new_text)?;
            }
        }
        // The previous root is not always `<state_dir>/transform-workspaces`
        // of *this* state_dir: when the state directory itself moved, the
        // workspaces were recorded under the old state_dir's default (or an
        // older explicit `workspace_dir`). The roots the transform journals
        // recorded are authoritative; each is resolved to where its
        // workspaces actually are now.
        for recorded in self.recorded_workspace_roots()? {
            if recorded == dir || dir.starts_with(&recorded) || recorded.starts_with(dir) {
                continue;
            }
            rewritten += self.migrate_recorded_workspace_root(&recorded, dir, &new_text)?;
        }
        let restamped = self.restamp_moved_workspaces(dir, &new_text)?;
        if rewritten > 0 || restamped > 0 {
            tracing::info!(from = %old.display(), to = %dir.display(), rewritten, restamped,
                "transform workspace paths migrated");
        }
        Ok(rewritten)
    }

    /// Every root a transform journal recorded its scratch workspace under.
    fn recorded_workspace_roots(&self) -> Result<Vec<PathBuf>> {
        let db = self.db.lock().unwrap();
        let mut statement = db.prepare(
            "SELECT DISTINCT json_extract(json_extract(data,'$.request'),'$.scratch.root') FROM operations WHERE json_extract(data,'$.kind') IN ('extract','par_repair') AND json_extract(json_extract(data,'$.request'),'$.scratch.root') IS NOT NULL",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut roots = rows
            .map(|root| root.map(PathBuf::from))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        roots.sort();
        roots.dedup();
        Ok(roots)
    }

    /// Resolve one recorded workspace root (`recorded`, not `dir`) to where
    /// its live workspaces are now: still there, under `dir`, or under this
    /// state directory's default (the state directory moved wholesale).
    /// Workspaces found on `dir`'s filesystem are renamed into `dir` (cheap,
    /// same inodes; a crash midway is finished by the next start), so they
    /// end up under the configured root. The recorded paths are rewritten
    /// only to a location that holds every live workspace that still exists
    /// anywhere (a workspace gone everywhere does not block it). Any root
    /// that cannot be read (EACCES, EIO, a mount that is down) makes the
    /// answer unknown: nothing is moved or rewritten. Returns the rewritten
    /// values.
    fn migrate_recorded_workspace_root(
        &self,
        recorded: &Path,
        dir: &Path,
        dir_text: &str,
    ) -> Result<usize> {
        use std::os::unix::fs::MetadataExt;
        let names = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare(
                "SELECT path FROM artifacts WHERE json_extract(data,'$.root')=?1 AND state NOT IN ('deleted','source_gone')",
            )?;
            let rows = statement.query_map([recorded.to_string_lossy()], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        }
        .into_iter()
        .filter_map(|path| Path::new(&path).file_name().map(|name| name.to_owned()))
        .collect::<Vec<_>>();
        // `Some(true)` present, `Some(false)` absent, `None` unknown.
        let probe = |path: &Path| match path.symlink_metadata() {
            Ok(_) => Some(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Some(false),
            Err(_) => None,
        };
        let default = default_workspace_dir(&self.state_dir);
        let mut places = vec![recorded.to_path_buf(), dir.to_path_buf()];
        if default != recorded && default != dir {
            places.push(default.clone());
        }
        let mut recorded_exists = false;
        for (index, place) in places.iter().enumerate() {
            match probe(place) {
                Some(exists) => recorded_exists |= index == 0 && exists,
                None => {
                    tracing::warn!(root = %place.display(),
                        "workspace root cannot be read; recorded workspaces are left as they are");
                    return Ok(0);
                }
            }
        }
        if names.is_empty() {
            // History only: point it at `dir` once the recorded root is gone.
            return if recorded_exists {
                Ok(0)
            } else {
                self.rewrite_workspace_paths(&recorded.to_string_lossy(), dir_text)
            };
        }
        std::fs::create_dir_all(dir)?;
        let dir_device = std::fs::metadata(dir)?.dev();
        let mut moved = 0usize;
        for place in places.iter().filter(|place| place.as_path() != dir) {
            let Ok(meta) = std::fs::metadata(place) else {
                continue;
            };
            if meta.dev() != dir_device {
                continue;
            }
            for name in &names {
                let from = place.join(name);
                let to = dir.join(name);
                if probe(&from) == Some(true) && probe(&to) == Some(false) {
                    std::fs::rename(&from, &to)?;
                    moved += 1;
                }
            }
        }
        if moved > 0 {
            fs::sync_directory(&fs::open_dir(dir)?)?;
            tracing::info!(from = %recorded.display(), to = %dir.display(), moved,
                "transform workspaces renamed into workspace_dir");
        }
        let mut location: Option<PathBuf> = None;
        for place in &places {
            let mut found = 0;
            let mut elsewhere = false;
            for name in &names {
                match probe(&place.join(name)) {
                    Some(true) => found += 1,
                    Some(false) => {
                        elsewhere |= places
                            .iter()
                            .any(|other| other != place && probe(&other.join(name)) == Some(true));
                    }
                    None => return Ok(0),
                }
            }
            if found > 0 && !elsewhere {
                location = Some(place.clone());
                break;
            }
        }
        match location {
            Some(location) if location != recorded => {
                let Some(text) = location.to_str() else {
                    return Ok(0);
                };
                let count = self.rewrite_workspace_paths(&recorded.to_string_lossy(), text)?;
                tracing::info!(from = %recorded.display(), to = %location.display(), rewritten = count,
                    "transform workspace paths migrated from a recorded root");
                Ok(count)
            }
            Some(_) => Ok(0),
            None => {
                let anywhere = names.iter().any(|name| {
                    places
                        .iter()
                        .any(|place| probe(&place.join(name)) == Some(true))
                });
                if !anywhere {
                    // Every live workspace is gone everywhere: the history
                    // points at `dir`.
                    return self.rewrite_workspace_paths(&recorded.to_string_lossy(), dir_text);
                }
                tracing::warn!(root = %recorded.display(),
                    "recorded workspaces are split across roots; left as they are");
                Ok(0)
            }
        }
    }

    /// Rewrite every recorded path under `old_text` to `new_text` in every
    /// table: a whole value that is the root or under it, and every JSON
    /// string that is, at any nesting depth. Counts only values that changed
    /// (a value mentioning the old path unquoted mid-string, such as error
    /// text, matches `instr` forever but is never rewritten or counted).
    fn rewrite_workspace_paths(&self, old_text: &str, new_text: &str) -> Result<usize> {
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let tables = {
            let mut statement = tx.prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            )?;
            let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let mut rewritten = 0usize;
        for table in tables {
            let columns = {
                let mut statement = tx.prepare(&format!("PRAGMA table_info(\"{table}\")"))?;
                let rows = statement.query_map([], |row| {
                    Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
                })?;
                rows.collect::<std::result::Result<Vec<_>, _>>()?
            };
            for (column, kind) in columns {
                if !(kind.eq_ignore_ascii_case("TEXT") || kind.is_empty()) {
                    continue;
                }
                // A request is JSON inside JSON, so its quotes arrive escaped.
                let mut expression = format!("\"{column}\"");
                let mut binds: Vec<String> = Vec::new();
                for quote in ["\"", "\\\"", "\\\\\\\""] {
                    for (from, to) in [
                        (format!("{quote}{old_text}/"), format!("{quote}{new_text}/")),
                        (
                            format!("{quote}{old_text}{quote}"),
                            format!("{quote}{new_text}{quote}"),
                        ),
                    ] {
                        binds.push(from);
                        binds.push(to);
                        expression = format!(
                            "replace({expression},?{},?{})",
                            binds.len() + 1,
                            binds.len() + 2
                        );
                    }
                }
                let value = format!(
                    "CASE \
                     WHEN \"{column}\" = ?1 THEN ?2 \
                     WHEN substr(\"{column}\",1,length(?1)+1) = ?1||'/' THEN ?2||substr(\"{column}\",length(?1)+1) \
                     ELSE {expression} END"
                );
                let sql = format!(
                    "UPDATE \"{table}\" SET \"{column}\" = {value} \
                     WHERE typeof(\"{column}\")='text' AND instr(\"{column}\",?1) > 0 \
                     AND ({value}) IS NOT \"{column}\""
                );
                let mut values: Vec<&dyn rusqlite::ToSql> = vec![&old_text, &new_text];
                values.extend(binds.iter().map(|value| value as &dyn rusqlite::ToSql));
                rewritten += tx.execute(&sql, values.as_slice())?;
            }
        }
        tx.commit()?;
        Ok(rewritten)
    }

    /// Re-stamp workspaces recorded under `dir` whose recorded root is not
    /// the live root object, in one transaction: the artifact row (root and
    /// scratch identity, manifest) and every transform journal naming that
    /// scratch (its copy of the scratch and its attempt directories), so
    /// startup reconcile still recognises the workspace as a transform's
    /// and a resume verifies. Then repair any of this installation's
    /// sidecars still recording a different root for a row that matches
    /// disk. A missing root re-stamps nothing. Returns the rows re-stamped.
    fn restamp_moved_workspaces(&self, dir: &Path, dir_text: &str) -> Result<usize> {
        let Ok(root) = fs::open_dir(dir) else {
            return Ok(0);
        };
        let live_root = fs::identity_for_file(&root)?;
        let live_scratch = |path: &Path| -> Option<(File, Identity)> {
            let file = fs::open_dir(path).ok()?;
            let identity = fs::identity_for_file(&file).ok()?;
            Some((file, identity))
        };
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let rows = {
            let mut statement = tx.prepare("SELECT data FROM artifacts WHERE json_extract(data,'$.root')=?1 AND state NOT IN ('deleted','source_gone')")?;
            let rows = statement.query_map([dir_text], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let mut current = std::collections::HashMap::new();
        let mut restamped = 0usize;
        for raw in rows {
            let mut a: Artifact = serde_json::from_str(&raw)?;
            if !a.root_identity.same_object(&live_root) {
                a.root_identity = live_root.clone();
                if let Some((scratch, identity)) = live_scratch(&a.path) {
                    if a.identity.is_some() {
                        a.identity = Some(identity);
                    }
                    if let Ok(files) = fs::manifest(&scratch, 100_000) {
                        a.files = files;
                    }
                }
                a.revision += 1;
                a.updated_at = now();
                save_artifact(&tx, &a)?;
                event(
                    &tx,
                    &a.id,
                    "workspace_moved",
                    &format!("workspace root moved to {dir_text}"),
                )?;
                restamped += 1;
            }
            current.insert(a.id.clone(), a);
        }
        let operations = {
            let mut statement = tx.prepare("SELECT data FROM operations WHERE json_extract(data,'$.kind') IN ('extract','par_repair') AND json_extract(json_extract(data,'$.request'),'$.scratch.root')=?1")?;
            let rows = statement.query_map([dir_text], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in operations {
            let mut op: Operation = serde_json::from_str(&raw)?;
            let mut workspace: Workspace = serde_json::from_str(&op.request)?;
            if workspace.scratch.root_identity.same_object(&live_root) {
                continue;
            }
            workspace.scratch.root_identity = live_root.clone();
            if let Some((scratch, identity)) = live_scratch(&workspace.scratch.path) {
                if workspace.scratch.identity.is_some() {
                    // Same value as the row's, so the journal and the row
                    // agree exactly (`is_transform_scratch`).
                    let row = current
                        .get(&workspace.scratch.id)
                        .filter(|row| row.generation == workspace.scratch.generation)
                        .and_then(|row| row.identity.clone())
                        .filter(|row| row.same_object(&identity));
                    workspace.scratch.identity = Some(row.unwrap_or(identity));
                }
                for attempt in &mut workspace.attempts {
                    if attempt.identity.is_some() {
                        if let Ok(directory) = fs::open_relative(&scratch, &attempt.directory) {
                            attempt.identity = Some(fs::identity_for_file(&directory)?);
                        }
                    }
                }
            }
            op.request = serde_json::to_string(&workspace)?;
            save_operation(&tx, &op)?;
        }
        tx.commit()?;
        drop(db);
        drop(_guard);
        for a in current.values() {
            if a.owned && a.identity.is_some() && self.sidecar_records_other_root(a)? {
                self.sidecar(a)?;
            }
        }
        Ok(restamped)
    }

    /// This installation's sidecar for `a` exists but records a different
    /// root (path or object) than the row, while the row's root is the live
    /// one: left behind by a root move whose re-stamp committed but whose
    /// sidecar write did not. A foreign or unreadable sidecar is not ours to
    /// rewrite.
    fn sidecar_records_other_root(&self, a: &Artifact) -> Result<bool> {
        let marker = self
            .state_dir
            .join("artifact-identities")
            .join(&a.id)
            .join(format!("{}.json", a.generation));
        let Ok(bytes) = std::fs::read(marker) else {
            return Ok(false);
        };
        let Ok(data) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
            return Ok(false);
        };
        if data["installation"] != self.installation
            || data["artifact"] != a.id
            || data["generation"] != a.generation
        {
            return Ok(false);
        }
        let recorded_root: Option<Identity> =
            serde_json::from_value(data["root_identity"].clone()).ok();
        Ok(data["root"] != a.root.to_string_lossy().as_ref()
            || data["path"] != a.path.to_string_lossy().as_ref()
            || !recorded_root.is_some_and(|root| root.same_object(&a.root_identity)))
    }

    fn verify(&self, a: &Artifact) -> Result<File> {
        self.verify_root(a)?;
        let file = fs::open_dir(&a.path)?;
        let observed = fs::identity_for_file(&file)?;
        if !a
            .identity
            .as_ref()
            .is_some_and(|i| i.same_object(&observed))
        {
            return Err(Error::Conflict(
                "payload identity changed; review required".into(),
            ));
        }
        if a.owned {
            let marker = self
                .state_dir
                .join("artifact-identities")
                .join(&a.id)
                .join(format!("{}.json", a.generation));
            let data: serde_json::Value = serde_json::from_slice(&std::fs::read(marker)?)?;
            // Normalize schema defaults on both sides. Pre-upgrade sidecars
            // omit mount_id/changed, while the same inventory identity gains
            // their empty defaults when deserialized. Compare every recorded
            // field exactly; filesystem identity equality intentionally has
            // looser remount semantics and is not sidecar authority.
            let marker_identity: Option<Identity> =
                serde_json::from_value(data["identity"].clone()).map_err(|_| {
                    Error::Conflict("external allocation identity does not match inventory".into())
                })?;
            if data["installation"] != self.installation
                || data["artifact"] != a.id
                || data["generation"] != a.generation
                || data["path"] != a.path.to_string_lossy().as_ref()
                || serde_json::to_value(&marker_identity)? != serde_json::to_value(&a.identity)?
            {
                return Err(Error::Conflict(
                    "external allocation identity does not match inventory".into(),
                ));
            }
        }
        Ok(file)
    }
    /// Called at a writer/PP quiescence boundary, before publishing History.
    pub fn finish(&self, job: u32, path: &Path, root: &Path, state: &str) -> Result<Artifact> {
        let a = self.for_job(job)?.ok_or(Error::NotFound)?;
        self.finish_artifact_unlocked(a, path, root, state)
    }

    /// Refresh the full Runner-owned source snapshot at the post-script
    /// quiescence boundary before selecting files for publication.
    pub fn refresh_for_delivery(&self, job: u32) -> Result<Artifact> {
        let mut artifact = self.for_job(job)?.ok_or(Error::NotFound)?;
        let original = artifact.clone();
        if !artifact.automatic_cleanup_eligible
            || !artifact.owned
            || artifact.state != "active"
            || artifact.hold.is_some()
        {
            return Err(Error::Conflict(
                "publication source is not an active eligible generation".into(),
            ));
        }
        let dir = self.verify(&artifact)?;
        artifact.files = fs::manifest(&dir, 100_000)?;
        artifact.identity = Some(fs::identity_for_file(&dir)?);
        artifact.inspected_at = Some(now());
        artifact.revision += 1;
        artifact.updated_at = now();
        let _coordinator = self.mutation_guard()?;
        self.check_payload_capture(&original)?;
        self.sidecar(&artifact)?;
        save_artifact(&self.db.lock().unwrap(), &artifact)?;
        Ok(artifact)
    }
    // Namespace mutation changes the generation/revision under the coordinator;
    // a read-only capture may run without it and commit only its exact snapshot.
    fn check_payload_capture(&self, original: &Artifact) -> Result<()> {
        let current = self.get(&original.id)?;
        if current.generation != original.generation
            || current.revision != original.revision
            || current.path != original.path
            || current.root != original.root
        {
            return Err(Error::Conflict(
                "source changed during payload capture".into(),
            ));
        }
        Ok(())
    }
    pub(super) fn finish_artifact_unlocked(
        &self,
        mut a: Artifact,
        path: &Path,
        root: &Path,
        state: &str,
    ) -> Result<Artifact> {
        let original = a.clone();
        // History may record a bounded PP failure while its independent
        // deletion journal keeps retrying. Do not erase that pending authority.
        if matches!(a.state.as_str(), "deleting" | "delete_failed")
            && state == "retained"
            && a.path == path
            && a.root == root
        {
            return Ok(a);
        }
        // Contract §2.5: a committed per-file publication is finalized from
        // its journal. It is visible, so its consumer may already have
        // taken files out of it; re-reading it would only find that.
        if matches!(state, "active" | "completed")
            && matches!(a.state.as_str(), "active" | "completed")
            && a.owned
            && a.path == path
            && a.root == root
            && self.committed_publication_intent(&a)?.is_some()
        {
            if a.state == state {
                return Ok(a);
            }
            a.state = state.into();
            a.revision += 1;
            a.updated_at = now();
            let _coordinator = self.mutation_guard()?;
            self.check_payload_capture(&original)?;
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            save_artifact(&tx, &a)?;
            event(&tx, &a.id, "finalized", state)?;
            tx.commit()?;
            return Ok(a);
        }
        if a.path == path && a.state == state {
            let dir = self.verify(&a)?;
            if a.state == "active" {
                let root_file = fs::open_dir(root)?;
                let root_identity = fs::identity_for_file(&root_file)?;
                let identity = fs::identity_for_file(&dir)?;
                let files = fs::manifest(&dir, 100_000)?;
                if a.root_identity != root_identity
                    || a.identity.as_ref() != Some(&identity)
                    || a.files != files
                {
                    a.root_identity = root_identity;
                    a.identity = Some(identity);
                    a.files = files;
                    a.inspected_at = Some(now());
                    a.revision += 1;
                    a.updated_at = now();
                    let _coordinator = self.mutation_guard()?;
                    self.check_payload_capture(&original)?;
                    self.sidecar(&a)?;
                    save_artifact(&self.db.lock().unwrap(), &a)?;
                }
            }
            return Ok(a);
        }
        if !matches!(
            a.state.as_str(),
            "active" | "transitioning" | "retiring" | "retained"
        ) {
            return Err(Error::Conflict("allocation is not active".into()));
        }
        if a.path != path || a.root != root {
            return Err(Error::Conflict(
                "finalization requires a committed relocation".into(),
            ));
        }
        let dir = self.verify(&a)?;
        let root_file = fs::open_dir(root)?;
        a.path = path.into();
        a.root = root.into();
        a.root_identity = fs::identity_for_file(&root_file)?;
        a.identity = Some(fs::identity_for_file(&dir)?);
        a.files = fs::manifest(&dir, 100_000)?;
        a.inspected_at = Some(now());
        // The finalization commits a fresh snapshot: an earlier attempt's
        // failure (a full volume) is history, not this row's state.
        a.error = None;
        if a.state == "retiring" {
            a.hold = if a.owned { None } else { Some("review".into()) };
        }
        a.state = state.into();
        a.revision += 1;
        a.updated_at = now();
        if state == "parked_failed" && a.owned {
            a.retention_seconds = u64::from(self.settings()?.failed_retention_days) * 86400;
            a.deadline = Some(now().saturating_add(a.retention_seconds as i64));
        }
        let _coordinator = self.mutation_guard()?;
        self.check_payload_capture(&original)?;
        self.sidecar(&a)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &a)?;
        event(&tx, &a.id, "finalized", state)?;
        tx.commit()?;
        Ok(a)
    }
    /// Capture an unknown directory without granting deletion authority.
    pub fn discover(&self, root: &Path, path: &Path, active: bool) -> Result<Artifact> {
        self.discover_record(root, path, active)
    }
    /// Observe a script-selected result without associating it with a job or
    /// granting ownership. Existing discoveries must still name the captured
    /// directory and remain unowned and unassociated at this boundary.
    pub fn observe_external_output(&self, root: &Path, path: &Path) -> Result<Artifact> {
        let observed = self.discover(root, path, false)?;
        if observed.owned || observed.job.is_some() {
            return Err(Error::Conflict(
                "external output is already owned or associated with a job".into(),
            ));
        }
        self.verify(&observed)?;
        let _guard = self.mutation_guard()?;
        self.check_payload_capture(&observed)?;
        let current = self.get(&observed.id)?;
        if current.owned || current.job.is_some() {
            return Err(Error::Conflict(
                "external output ownership changed during observation".into(),
            ));
        }
        Ok(current)
    }
    /// Whether a scan must leave `path` alone because it is Runner's own
    /// publication still landing: an open per-file intent's target, an
    /// unresolved relocation's destination, or a directory containing one
    /// (a category directory under the root), and Runner's private staging
    /// (`.runner-*`). Recording such a directory as foreign content is what
    /// left Runner publications as `unknown`/Keep/review duplicates.
    pub(super) fn publication_in_flight(&self, path: &Path) -> Result<bool> {
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(".runner-"))
        {
            return Ok(true);
        }
        let path = path.to_string_lossy();
        let db = self.db.lock().unwrap();
        let intent: bool = db.query_row(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM relocation_intents WHERE (target_dir=?1 OR substr(target_dir,1,length(?1)+1)=?1||'/') AND state IN {})",
                intents::OPEN_STATES
            ),
            [&path],
            |row| row.get(0),
        )?;
        if intent {
            return Ok(true);
        }
        Ok(db.query_row(
            "SELECT EXISTS(SELECT 1 FROM operations WHERE state NOT IN ('succeeded','cancelled','failed','transferred') AND json_extract(data,'$.kind')='relocate' AND (json_extract(json_extract(data,'$.request'),'$.destination')=?1 OR substr(json_extract(json_extract(data,'$.request'),'$.destination'),1,length(?1)+1)=?1||'/'))",
            [&path],
            |row| row.get(0),
        )?)
    }

    /// Runner's own publications recorded as discovered foreign content are
    /// folded back into Runner's custody record (contract §2.5: a directory
    /// carrying a valid `.runner-manifest.json` is a Runner publication
    /// handed to its consumer — custody transferred, never unknown, Keep or
    /// review). Two cases, both idempotent:
    ///
    /// * Runner's own row names the same directory object (path and
    ///   `same_object` identity; the scan walked the destination before the
    ///   publication committed): the duplicate observation is closed
    ///   (`deleted` — a record, no bytes touched) with an event pointing at
    ///   Runner's row; its first-seen time, Keep and manifest stay on it.
    /// * no row names it, but the directory's manifest verifies, lists
    ///   exactly its files, and names a generation this installation
    ///   recorded: the row becomes `transferred` (unowned, no Keep, no
    ///   hold) — the consumer's; Runner never deletes it.
    ///
    /// Content without such a manifest stays foreign; each unknown row's
    /// manifest is read once per revision, not on every scan. Returns how
    /// many rows changed.
    pub fn fold_rediscovered_publications(&self) -> Result<usize> {
        let candidates = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare(
                "SELECT u.data, c.data FROM artifacts u JOIN artifacts c ON c.path=u.path AND c.id<>u.id \
                 WHERE u.state='unknown' AND u.job IS NULL AND json_extract(u.data,'$.owned')=0 \
                 AND c.state NOT IN ('unknown','deleted','source_gone') \
                 AND (c.job IS NOT NULL OR json_extract(c.data,'$.owned')=1 OR c.state='transferred') \
                 AND NOT EXISTS(SELECT 1 FROM operations o WHERE o.artifact=u.id) \
                 AND NOT EXISTS(SELECT 1 FROM recoveries r WHERE r.artifact=u.id)",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let mut changed = 0;
        let mut folded = std::collections::HashSet::new();
        for (duplicate, runner) in candidates {
            let duplicate: Artifact = serde_json::from_str(&duplicate)?;
            let runner: Artifact = serde_json::from_str(&runner)?;
            if folded.contains(&duplicate.id) {
                continue;
            }
            let same = match (&duplicate.identity, &runner.identity) {
                (Some(a), Some(b)) => a.same_object(b),
                _ => false,
            };
            if !same {
                continue;
            }
            let _guard = self.mutation_guard()?;
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            let current: Option<String> = tx
                .query_row(
                    "SELECT data FROM artifacts WHERE id=?1 AND state='unknown' AND job IS NULL",
                    [&duplicate.id],
                    |row| row.get(0),
                )
                .optional()?;
            let Some(current) = current else {
                continue;
            };
            let mut current: Artifact = serde_json::from_str(&current)?;
            if current.revision != duplicate.revision || current.owned {
                continue;
            }
            current.state = "deleted".into();
            current.hold = None;
            current.revision += 1;
            current.updated_at = now();
            save_artifact(&tx, &current)?;
            if tx.changes() != 1 {
                continue;
            }
            event(
                &tx,
                &current.id,
                "rediscovery_folded",
                &format!("the same directory is Runner's publication {}; this observation is closed (no bytes touched)", runner.id),
            )?;
            event(
                &tx,
                &runner.id,
                "rediscovery_folded",
                &format!(
                    "a scan had recorded this publication as unknown content ({}); folded",
                    current.id
                ),
            )?;
            tx.commit()?;
            folded.insert(duplicate.id);
            changed += 1;
        }
        let orphans = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare(
                "SELECT a.data FROM artifacts a WHERE a.state='unknown' AND a.job IS NULL AND json_extract(a.data,'$.owned')=0 \
                 AND NOT EXISTS(SELECT 1 FROM meta m WHERE m.key='publication-fold-checked:'||a.id AND m.value=CAST(json_extract(a.data,'$.revision') AS TEXT))",
            )?;
            let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in orphans {
            let candidate: Artifact = serde_json::from_str(&raw)?;
            let checked = |inventory: &Self| -> Result<()> {
                inventory.db.lock().unwrap().execute(
                    "INSERT INTO meta(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    params![format!("publication-fold-checked:{}", candidate.id), candidate.revision.to_string()],
                )?;
                Ok(())
            };
            let Some(manifest) = fs::open_dir(&candidate.path).ok().and_then(|dir| {
                intents::read_manifest(&dir)
                    .ok()
                    .flatten()
                    .map(|m| (dir, m))
            }) else {
                checked(self)?;
                continue;
            };
            let (dir, manifest) = manifest;
            let Ok(files) = fs::manifest(&dir, 100_000) else {
                continue;
            };
            let payload = files
                .iter()
                .filter(|entry| {
                    !entry.identity.directory && entry.path != intents::RUNNER_MANIFEST_NAME
                })
                .collect::<Vec<_>>();
            let matches = payload.len() == manifest.files.len()
                && manifest.files.iter().all(|listed| {
                    payload.iter().any(|entry| {
                        entry.path == listed.path && entry.identity.bytes == listed.bytes
                    })
                });
            // Installation check: the generation is one this inventory
            // recorded (an artifact generation, a delivery, an intent).
            let ours: bool = self.db.lock().unwrap().query_row(
                "SELECT EXISTS(SELECT 1 FROM artifacts WHERE json_extract(data,'$.generation')=?1 AND id<>?2) \
                 OR EXISTS(SELECT 1 FROM deliveries WHERE generation=?1) \
                 OR EXISTS(SELECT 1 FROM relocation_intents WHERE generation=?1)",
                params![manifest.generation, candidate.id],
                |row| row.get(0),
            )?;
            if !matches || !ours {
                checked(self)?;
                continue;
            }
            let _guard = self.mutation_guard()?;
            let mut current = self.get(&candidate.id)?;
            if current.revision != candidate.revision || current.state != "unknown" {
                continue;
            }
            current.state = "transferred".into();
            current.owned = false;
            current.keep = false;
            current.keep_on_failure = false;
            current.hold = None;
            current.deadline = None;
            current.automatic_cleanup_eligible = false;
            current.files = files;
            current.revision += 1;
            current.updated_at = now();
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            save_artifact(&tx, &current)?;
            event(
                &tx,
                &current.id,
                "rediscovery_folded",
                &format!(
                    "Runner publication of job {} (generation {}); custody transferred to its consumer",
                    manifest.job, manifest.generation
                ),
            )?;
            tx.commit()?;
            changed += 1;
        }
        if changed > 0 {
            tracing::info!(
                changed,
                "rediscovered Runner publications folded into custody records"
            );
        }
        Ok(changed)
    }

    fn discover_unlocked(&self, root: &Path, path: &Path) -> Result<Artifact> {
        self.discover_record(root, path, false)
    }
    fn discover_record(&self, root: &Path, path: &Path, active: bool) -> Result<Artifact> {
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::Conflict("inventory has stopped".into()));
        }
        fs::absolute(path)?;
        if path.parent() != Some(root) {
            return Err(Error::Conflict(
                "discovery must be an immediate child".into(),
            ));
        }
        // Discovery observes a candidate; it never grants namespace or
        // cleanup authority. Check first to avoid unnecessary mount access,
        // then recheck under the mutation coordinator before recording it.
        let existing = self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT data FROM artifacts WHERE path=?1 AND state NOT IN ('deleted','source_gone') LIMIT 1",
                [path.to_string_lossy()],
                |r| r.get::<_, String>(0),
            )
            .optional()?;
        if let Some(raw) = existing {
            return Ok(serde_json::from_str(&raw)?);
        }
        let dir = fs::open_dir(path)?;
        let root_file = fs::open_dir(root)?;
        let root_identity = fs::identity_for_file(&root_file)?;
        let identity = Some(fs::identity_for_file(&dir)?);
        drop(root_file);
        drop(dir);

        let _guard = self.mutation_guard()?;
        let db = self.db.lock().unwrap();
        let existing: Option<String> = db.query_row("SELECT data FROM artifacts WHERE path=?1 AND state NOT IN ('deleted','source_gone') LIMIT 1", [path.to_string_lossy()], |r| r.get(0)).optional()?;
        if let Some(raw) = existing {
            return Ok(serde_json::from_str(&raw)?);
        }
        let a = Artifact {
            id: id(&db)?,
            generation: id(&db)?,
            revision: 1,
            job: None,
            path: path.into(),
            origin_path: path.into(),
            root: root.into(),
            root_identity,
            identity,
            state: if active {
                "active_unverified"
            } else {
                "unknown"
            }
            .into(),
            owned: false,
            automatic_cleanup_eligible: false,
            delivery_lifecycle: false,
            keep: true,
            keep_on_failure: false,
            failure_assessment: None,
            hold: Some("review".into()),
            created_at: now(),
            updated_at: now(),
            retention_seconds: 0,
            deadline: None,
            files: Vec::new(),
            error: None,
            inspected_at: None,
            salvage: None,
            failure_reason: None,
            gone_reason: None,
        };
        save_artifact(&db, &a)?;
        Ok(a)
    }
    pub fn inspect(&self, key: &str) -> Result<Artifact> {
        let original = self.get(key)?;
        if matches!(
            original.state.as_str(),
            "active"
                | "active_unverified"
                | "transitioning"
                | "retiring"
                | "deleting"
                | "transferred"
        ) {
            return Err(Error::Conflict(
                "writers must finish before inspection".into(),
            ));
        }
        // Reserve this exact metadata generation briefly, then inspect the
        // payload without either the mutation coordinator or SQLite held.
        // The final revision check below rejects an obsolete observation.
        // A background metadata probe can briefly own this same lease.
        // Wait off-actor without holding the coordinator, always reserving
        // the original revision so changed policy or custody cannot slip in.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        let _lease = loop {
            if let Some(lease) = self.reserve_reconciliation(std::slice::from_ref(&original))? {
                break lease;
            }
            if std::time::Instant::now() >= deadline {
                return Err(Error::Conflict(
                    "artifact changed or is in use; retry inspection".into(),
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        let dir = self.verify(&original)?;
        let files = fs::manifest(&dir, 100_000)?;
        drop(dir);

        let _guard = self.mutation_guard()?;
        let mut a = self.get(key)?;
        if a.id != original.id
            || a.generation != original.generation
            || a.path != original.path
            || a.root != original.root
            || a.revision != original.revision
        {
            return Err(Error::Conflict(
                "artifact policy or identity changed during inspection; retry".into(),
            ));
        }
        if a.owned && files != a.files {
            a.owned = false;
            a.keep = true;
            a.hold = Some("review: files changed; adoption required".into());
        }
        a.files = files;
        a.inspected_at = Some(now());
        a.error = None;
        a.updated_at = now();
        a.revision += 1;
        save_artifact(&self.db.lock().unwrap(), &a)?;
        Ok(a)
    }
    /// Record why an automatic walk could not measure a folder. The record
    /// stays; the operator sees the reason on the row instead of a zero.
    pub fn note_error(&self, key: &str, message: &str) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let mut a = self.get(key)?;
        a.error = Some(message.into());
        a.updated_at = now();
        save_artifact(&self.db.lock().unwrap(), &a)
    }
    pub fn adopt(&self, key: &str, revision: u64) -> Result<Artifact> {
        if !cfg!(unix) {
            return Err(Error::Conflict(
                "checked destructive ownership requires descriptor-relative filesystem support"
                    .into(),
            ));
        }
        let original = self.get(key)?;
        if original.revision != revision
            || original.owned
            || !matches!(
                original.state.as_str(),
                "unknown" | "retained" | "parked_failed" | "completed"
            )
        {
            return Err(Error::Conflict(
                "stale preview or artifact is not unknown".into(),
            ));
        }
        let Some(_lease) = self.reserve_reconciliation(std::slice::from_ref(&original))? else {
            return Err(Error::Conflict(
                "artifact changed or is in use; retry adoption".into(),
            ));
        };
        let dir = self.verify(&original)?;
        let files = fs::manifest(&dir, 100_000)?;
        drop(dir);
        if files != original.files {
            return Err(Error::Conflict(
                "files changed; inspect before adopting".into(),
            ));
        }
        let mut a = original.clone();
        a.owned = true;
        a.keep = true;
        a.hold = None;
        a.state = "retained".into();
        a.revision += 1;
        self.sidecar(&a)?;

        let _guard = self.mutation_guard()?;
        self.check_payload_capture(&original)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &a)?;
        event(&tx, key, "adopted", "kept indefinitely")?;
        tx.commit()?;
        Ok(a)
    }
    pub fn retention(
        &self,
        key: &str,
        revision: u64,
        keep: bool,
        seconds: Option<u64>,
    ) -> Result<Artifact> {
        let _guard = self.mutation_guard()?;
        let mut a = self.get(key)?;
        if self.reconciliation_in_use(&a) {
            return Err(Error::Conflict(
                "payload lifecycle custody is active".into(),
            ));
        }
        if a.revision != revision
            || !a.owned
            || a.state == "transferred"
            || matches!(a.state.as_str(), "deleting" | "deleted")
        {
            return Err(Error::Conflict(
                "stale revision or deletion already started".into(),
            ));
        }
        a.keep = keep;
        if let Some(seconds) = seconds {
            if seconds > 315_360_000 {
                return Err(Error::Conflict("retention exceeds 3650 days".into()));
            }
            a.retention_seconds = seconds;
        }
        a.deadline = Some(now().saturating_add(a.retention_seconds as i64));
        a.revision += 1;
        a.updated_at = now();
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        if keep {
            let mut stmt =
                tx.prepare("SELECT data FROM operations WHERE artifact=?1 AND state='queued'")?;
            let ops = stmt
                .query_map([key], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            drop(stmt);
            for raw in ops {
                let mut op: Operation = serde_json::from_str(&raw)?;
                if op.kind == "delete"
                    && matches!(serde_json::from_str::<DeleteRequest>(&op.request),Ok(DeleteRequest::ClusterCancellation((_,_,_,false,ref generation,false,ref receipt,_))) if generation==&a.generation && !receipt.is_empty())
                {
                    // The executor still rechecks its independent receipt and
                    // exact owned manifest after any Keep-only revision change.
                    continue;
                }
                op.state = "cancelled".into();
                save_operation(&tx, &op)?;
            }
        }
        save_artifact(&tx, &a)?;
        event(
            &tx,
            key,
            "retention",
            if keep {
                "Keep"
            } else {
                "fresh retention period"
            },
        )?;
        if !keep {
            Self::wake_relocation_cleanup(&tx, &a.id, &a.generation)?;
        }
        tx.commit()?;
        Ok(a)
    }

    /// Current exact Runner-owned source; Delivery's admission Keep is immutable.
    pub fn failure_keep_source(&self, job: u32) -> Result<Option<Artifact>> {
        let mut artifact = self.for_job(job)?;
        // After publication, the job handle resolves to the transferred
        // publication artifact. Keep governs the separate Runner-owned
        // processing source, so follow the durable relocation relation and
        // mutate that exact generation while leaving Delivery's original
        // admission intent immutable.
        let delivery_raw = {
            self.db.lock().unwrap().query_row(
                "SELECT data FROM deliveries WHERE json_extract(data,'$.jobId')=?1 ORDER BY sequence DESC LIMIT 1",
                [job],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        };
        if let Some(raw) = delivery_raw {
            let delivery: Delivery = serde_json::from_str(&raw)?;
            if !delivery.source_artifact_id.is_empty() {
                if let Ok(source) = self.get(&delivery.source_artifact_id) {
                    if source.generation == delivery.source_generation {
                        artifact = Some(source);
                    }
                }
            }
        }
        Ok(artifact.filter(|artifact| artifact.owned && !artifact.terminal()))
    }

    /// Persist the per-job failed-source choice in the same inventory journal
    /// consulted at the physical-delete boundary. Explicit artifact deletion
    /// remains a separate user action and is not blocked by this preference.
    pub fn set_keep_on_failure(&self, job: u32, keep: bool) -> Result<bool> {
        loop {
            // An operator's Keep serializes behind a maintenance pass's
            // momentary reconciliation lease on the source rather than
            // failing against it (the lease is released under the mutation
            // guard, so the wait happens without it).
            if let Some(artifact) = self.failure_keep_source(job)? {
                self.wait_reconciliation_released(&artifact);
            }
            let guard = self.mutation_guard()?;
            let Some(artifact) = self.failure_keep_source(job)? else {
                return Ok(false);
            };
            if self.reconciliation_in_use(&artifact) {
                drop(guard);
                continue;
            }
            let result = self.set_failure_keep_artifact_locked(job, artifact, keep, false);
            drop(guard);
            return result;
        }
    }

    /// Block until no reconciliation/namespace use holds `artifact`'s
    /// current generation. Call without the mutation guard.
    pub(super) fn wait_reconciliation_released(&self, artifact: &Artifact) {
        let key = (artifact.id.clone(), artifact.generation.clone());
        let mut uses = self.reconciliation_uses.lock().unwrap();
        while uses.contains(&key) {
            uses = self.reconciliation_released.wait(uses).unwrap();
        }
    }

    fn set_failure_keep_artifact_locked(
        &self,
        job: u32,
        mut artifact: Artifact,
        keep: bool,
        existing_only: bool,
    ) -> Result<bool> {
        if self.reconciliation_in_use(&artifact) {
            return Err(Error::Conflict(
                "payload lifecycle custody is active".into(),
            ));
        }
        if !artifact.owned || artifact.terminal() {
            return Ok(false);
        }
        if artifact.keep_on_failure == keep && keep {
            return Ok(true);
        }
        if artifact.keep_on_failure != keep {
            artifact.keep_on_failure = keep;
            artifact.revision = artifact.revision.saturating_add(1);
            artifact.updated_at = now();
        }
        {
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            if keep {
                let raws = {
                    let mut statement = tx.prepare(
                        "SELECT data FROM operations WHERE artifact=?1 AND state IN ('queued','retry','running') AND id LIKE 'failed-delete-%'",
                    )?;
                    let rows =
                        statement.query_map([&artifact.id], |row| row.get::<_, String>(0))?;
                    rows.collect::<std::result::Result<Vec<_>, _>>()?
                };
                let cancelled_failed_delete = !raws.is_empty();
                for raw in raws {
                    let mut operation: Operation = serde_json::from_str(&raw)?;
                    operation.state = "cancelled".into();
                    operation.error = Some("per-download Keep on failure was enabled".into());
                    save_operation(&tx, &operation)?;
                }
                if cancelled_failed_delete && artifact.state == "deleting" {
                    // A persisted running delete may be from a process that
                    // stopped mid-operation. Keep pauses future work; it does
                    // not claim that already removed bytes were restored.
                    artifact.state = "retained".into();
                }
            } else if artifact.automatic_cleanup_eligible
                && artifact.owned
                && !artifact.terminal()
                && artifact.hold.is_none()
                && matches!(
                    artifact.state.as_str(),
                    "retained" | "completed" | "delete_failed" | "deleting"
                )
            {
                // A durable failed-delete operation is already exact
                // generation-bound authority, even when it came from the
                // default Keep-off terminal pipeline (which has no separate
                // failure assessment). Minting a new operation still
                // requires positive unusable evidence below.
                let operation_id = format!("failed-delete-{}", artifact.id);
                let fresh_request = || {
                    serde_json::to_string(&(
                        artifact.id.as_str(),
                        artifact.revision,
                        0u64,
                        false,
                        artifact.generation.as_str(),
                    ))
                };
                let existing: Option<String> = tx
                    .query_row(
                        "SELECT data FROM operations WHERE id=?1",
                        [&operation_id],
                        |row| row.get(0),
                    )
                    .optional()?;
                let mut operation = if let Some(raw) = existing {
                    let mut operation = serde_json::from_str::<Operation>(&raw)?;
                    if operation.artifact != artifact.id
                        || operation.kind != "delete"
                        || operation.id != operation_id
                    {
                        return Err(Error::Conflict(
                            "failed-source deletion id is bound to another operation".into(),
                        ));
                    }
                    let request: DeleteRequest = serde_json::from_str(&operation.request)?;
                    let (id, _, _, automatic, generation) = request.fields();
                    if id != artifact.id
                        || automatic
                        || generation != Some(artifact.generation.as_str())
                    {
                        return Err(Error::Conflict(
                            "failed-source deletion authorization changed".into(),
                        ));
                    }
                    if !matches!(
                        operation.state.as_str(),
                        "cancelled" | "retry" | "queued" | "running"
                    ) {
                        // A succeeded or review-held journal is not revived
                        // by Keep-off. Review and conflict states require an
                        // explicit inspection/re-authorization path.
                        None
                    } else {
                        if operation.attempts == 0 {
                            operation.request = fresh_request()?;
                        }
                        Some(operation)
                    }
                } else {
                    // New authorization requires a persisted assessment for
                    // this generation. A failed/in-flight deletion without
                    // its journal cannot be reconstructed from Keep-off.
                    if !existing_only
                        && artifact
                            .failure_assessment
                            .as_ref()
                            .is_some_and(|assessment| assessment.disposition == "unusable")
                        && matches!(artifact.state.as_str(), "retained" | "completed")
                    {
                        Some(Operation {
                            id: operation_id,
                            artifact: artifact.id.clone(),
                            kind: "delete".into(),
                            state: "queued".into(),
                            request: fresh_request()?,
                            created_at: now(),
                            not_before: now(),
                            attempts: 0,
                            next_retry: 0,
                            error: None,
                        })
                    } else {
                        None
                    }
                };
                if let Some(mut operation) = operation.take() {
                    operation.state = "queued".into();
                    operation.not_before = now();
                    operation.next_retry = 0;
                    operation.error = None;
                    save_operation(&tx, &operation)?;
                }
            }
            save_artifact(&tx, &artifact)?;
            if !keep {
                if !existing_only {
                    Self::wake_relocation_cleanup(&tx, &artifact.id, &artifact.generation)?;
                }
                // Read the mutable cleanup columns directly. The serialized
                // Delivery is the immutable receipt snapshot and may lag a
                // worker's latest cleanup status.
                let delivery: Option<(String, String)> = tx
                    .query_row(
                        "SELECT id,cleanup_status FROM deliveries WHERE json_extract(data,'$.jobId')=?1 AND json_extract(data,'$.sourceArtifactId')=?2 AND json_extract(data,'$.sourceGeneration')=?3 ORDER BY sequence DESC LIMIT 1",
                        params![job, artifact.id, artifact.generation],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                if let Some((delivery_id, cleanup_status)) = delivery {
                    if matches!(cleanup_status.as_str(), "pending" | "retry" | "blocked") {
                        if existing_only {
                            // Retry the existing obligation without erasing a
                            // review/identity reason or creating byte authority.
                            tx.execute("UPDATE deliveries SET cleanup_next_retry_at=0 WHERE id=?1 AND cleanup_status IN ('pending','retry','blocked')", [&delivery_id])?;
                        } else {
                            tx.execute(
                                "UPDATE deliveries SET cleanup_status='retry',cleanup_error=NULL,cleanup_next_retry_at=0 WHERE id=?1 AND cleanup_status IN ('pending','retry','blocked')",
                                [&delivery_id],
                            )?;
                        }
                    }
                }
            }
            event(
                &tx,
                &artifact.id,
                "failure_keep",
                if keep { "enabled" } else { "disabled" },
            )?;
            tx.commit()?;
        }
        Ok(true)
    }

    /// Persist bounded post-failure evidence before PP_DONE. This record is
    /// pinned to the artifact generation and is the only automatic
    /// Keep-off deletion authority for a no-Delivery failed source.
    pub fn set_failure_assessment(
        &self,
        artifact_id: &str,
        generation: &str,
        disposition: &str,
        reason: &str,
    ) -> Result<Artifact> {
        if !matches!(disposition, "unusable" | "recoverable" | "unknown")
            || reason.trim().is_empty()
            || reason.len() > 2048
        {
            return Err(Error::Conflict("invalid failed-source assessment".into()));
        }
        let _guard = self.mutation_guard()?;
        let mut artifact = self.get(artifact_id)?;
        if self.reconciliation_in_use(&artifact) {
            return Err(Error::Conflict(
                "payload lifecycle custody is active".into(),
            ));
        }
        if artifact.generation != generation
            || !artifact.automatic_cleanup_eligible
            || !artifact.owned
            || artifact.terminal()
        {
            return Err(Error::Conflict(
                "failed-source assessment does not match an owned eligible generation".into(),
            ));
        }
        artifact.failure_assessment = Some(FailureAssessment {
            disposition: disposition.into(),
            reason: reason.into(),
            assessed_at: now(),
        });
        artifact.revision = artifact.revision.saturating_add(1);
        artifact.updated_at = now();
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &artifact)?;
        Self::wake_relocation_cleanup(&tx, &artifact.id, &artifact.generation)?;
        event(&tx, artifact_id, "failure_assessment", disposition)?;
        tx.commit()?;
        Ok(artifact)
    }
    pub fn release_review(&self, key: &str, revision: u64) -> Result<Artifact> {
        let original = self.get(key)?;
        if original.revision != revision
            || !original
                .hold
                .as_deref()
                .is_some_and(|h| h.starts_with("review") || h.starts_with("restored backup"))
        {
            return Err(Error::Conflict(
                "stale revision or hold is owned by an active operation".into(),
            ));
        }
        let Some(_lease) = self.reserve_reconciliation(std::slice::from_ref(&original))? else {
            return Err(Error::Conflict(
                "artifact changed or is in use; retry releasing review hold".into(),
            ));
        };
        let dir = self.verify(&original)?;
        let files = fs::manifest(&dir, 100_000)?;
        drop(dir);
        if files != original.files {
            return Err(Error::Conflict(
                "files changed; inspect before releasing review hold".into(),
            ));
        }

        let _guard = self.mutation_guard()?;
        self.check_payload_capture(&original)?;
        let pending:bool=self.db.lock().unwrap().query_row("SELECT EXISTS(SELECT 1 FROM recoveries WHERE artifact=?1 AND state NOT IN ('imported','cancelled','partial'))",[key],|r|r.get(0))?;
        if pending {
            return Err(Error::Conflict(
                "recovery is still active or ambiguous".into(),
            ));
        }
        let mut a = self.get(key)?;
        a.hold = None;
        a.deadline = Some(now().saturating_add(a.retention_seconds as i64));
        a.revision += 1;
        save_artifact(&self.db.lock().unwrap(), &a)?;
        Self::wake_relocation_cleanup(&self.db.lock().unwrap(), &a.id, &a.generation)?;
        Ok(a)
    }

    pub fn operation(&self, key: &str) -> Result<Operation> {
        read(&self.db.lock().unwrap(), "operations", key)
    }
    pub fn request_delete(
        &self,
        key: &str,
        revision: u64,
        request_id: &str,
        undo_seconds: u64,
    ) -> Result<Operation> {
        self.request_delete_authorized(key, revision, request_id, undo_seconds, false)
    }
    fn request_delete_authorized(
        &self,
        key: &str,
        revision: u64,
        request_id: &str,
        undo_seconds: u64,
        automatic: bool,
    ) -> Result<Operation> {
        self.request_delete_with_use(key, revision, request_id, undo_seconds, automatic, None)
    }
    fn request_delete_with_use(
        &self,
        key: &str,
        revision: u64,
        request_id: &str,
        undo_seconds: u64,
        automatic: bool,
        authority: Option<(&AttemptUse, &dyn Fn() -> std::io::Result<()>)>,
    ) -> Result<Operation> {
        self.request_delete_with_policy(
            key,
            revision,
            request_id,
            undo_seconds,
            automatic,
            authority,
            false,
            None,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn request_delete_with_policy(
        &self,
        key: &str,
        revision: u64,
        request_id: &str,
        undo_seconds: u64,
        automatic: bool,
        authority: Option<(&AttemptUse, &dyn Fn() -> std::io::Result<()>)>,
        failure_keep_guard: bool,
        cancellation_receipt: Option<&str>,
    ) -> Result<Operation> {
        let _guard = self.mutation_guard()?;
        if let Some((_, checkpoint)) = authority {
            checkpoint()?;
        }
        let use_guard = authority.map(|(guard, _)| guard);
        if request_id.is_empty() || request_id.len() > 128 || undo_seconds > 60 {
            return Err(Error::Conflict(
                "valid idempotency key and undo of 0–60 seconds required".into(),
            ));
        }
        let a = self.get(key)?;
        if self.open_intent_for_artifact(&a.id)? {
            // Contract §2.5: neither side of an open per-file move may be
            // claimed until restart reconciliation finishes or unwinds it.
            return Err(Error::Conflict(
                "an open publication move owns this source; reconciliation must finish or unwind it first".into(),
            ));
        }
        let explicit_cancellation = cancellation_receipt
            .map(|receipt| self.cluster_cancellation_delete_accepted(&a.id, &a.generation, receipt))
            .transpose()?
            .unwrap_or(false);
        if cancellation_receipt.is_some()
            && (!explicit_cancellation || automatic || failure_keep_guard)
        {
            return Err(Error::Conflict(
                "explicit cancellation lacks its exact durable receipt".into(),
            ));
        }
        let request = if let Some(receipt) = cancellation_receipt {
            serde_json::to_string(&(
                key,
                revision,
                undo_seconds,
                automatic,
                &a.generation,
                false,
                receipt,
                self.cluster_cancellation_delete_binding(&a)?,
            ))?
        } else if request_id.starts_with("cluster-copy-") {
            serde_json::to_string(&(
                key,
                revision,
                undo_seconds,
                automatic,
                &a.generation,
                failure_keep_guard,
            ))?
        } else {
            serde_json::to_string(&(key, revision, undo_seconds, automatic, &a.generation))?
        };
        match self.operation(request_id) {
            Ok(op) => {
                let previous: DeleteRequest = serde_json::from_str(&op.request)?;
                let (id, rev, undo, auto, generation) = previous.fields();
                return if (id, rev, undo, auto) == (key, revision, undo_seconds, automatic)
                    && generation.is_none_or(|g| g == a.generation)
                {
                    Ok(op)
                } else {
                    Err(Error::Conflict(
                        "idempotency key reused with different request".into(),
                    ))
                };
            }
            Err(Error::NotFound) => {}
            Err(e) => return Err(e),
        }
        if a.revision != revision
            || (self.artifact_in_use(&a)? && !self.authorized_processing_use(&a, use_guard))
            || !a.owned
            || (a.keep && !explicit_cancellation)
            || a.hold.is_some()
            || !matches!(
                a.state.as_str(),
                "parked_failed"
                    | "retained"
                    | "completed"
                    | "recovery_imported"
                    | "recovery_staged"
            )
        {
            return Err(Error::Conflict(
                "stale revision, active payload, Keep or recovery/review hold".into(),
            ));
        }
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let pending:Option<String>=tx.query_row("SELECT data FROM operations WHERE artifact=?1 AND state IN ('queued','running','retry') AND json_extract(data,'$.kind')='delete' LIMIT 1",[key],|r|r.get(0)).optional()?;
        if let Some(raw) = pending {
            return Ok(serde_json::from_str(&raw)?);
        }
        let op = Operation {
            id: request_id.into(),
            artifact: key.into(),
            kind: "delete".into(),
            state: "queued".into(),
            request,
            created_at: now(),
            not_before: now() + undo_seconds as i64,
            attempts: 0,
            next_retry: 0,
            error: None,
        };
        save_operation(&tx, &op)?;
        event(&tx, key, "delete_requested", request_id)?;
        tx.commit()?;
        self.invalidate_source_writer_naming(&a.id, &a.generation);
        Ok(op)
    }
    pub fn cancel_delete(&self, key: &str) -> Result<Operation> {
        let _guard = self.mutation_guard()?;
        let mut op = self.operation(key)?;
        if op.state != "queued" || !matches!(op.kind.as_str(), "delete" | "prune") {
            return Err(Error::Conflict(
                "deletion already started or finished".into(),
            ));
        }
        op.state = "cancelled".into();
        save_operation(&self.db.lock().unwrap(), &op)?;
        Ok(op)
    }
    pub fn execute_delete(&self, key: &str) -> Result<Operation> {
        self.execute_delete_with_use(key, None)
    }
    fn execute_delete_with_use(
        &self,
        key: &str,
        authority: Option<(&AttemptUse, &dyn Fn() -> std::io::Result<()>)>,
    ) -> Result<Operation> {
        // Declared first so error unwinding drops the coordinator before the
        // reservation's Drop needs it.
        let _lease;
        let guard = self.mutation_guard()?;
        if let Some((_, checkpoint)) = authority {
            checkpoint()?;
        }
        let use_guard = authority.map(|(guard, _)| guard);
        let mut op = self.operation(key)?;
        if op.kind != "delete" {
            return Err(Error::Conflict("not a deletion operation".into()));
        }
        if torrent_seed::is_seed_delete_key(&op.id)
            && authority.is_none()
            && !matches!(op.state.as_str(), "succeeded" | "cancelled" | "review")
        {
            return Err(Error::Conflict(
                "managed seed deletion requires fresh backend-stop and dependency proof".into(),
            ));
        }
        // A deferred move may have a stale/legacy retirement operation in the
        // journal. Its task row is not authority: require the durable delivery
        // bound to the exact source generation at execution time as well.
        if let Some(relocation_id) = op.id.strip_prefix("retire-") {
            if self
                .deferred_retirement_ready(relocation_id)?
                .is_some_and(|ready| !ready)
            {
                return Ok(op);
            }
        }
        if matches!(op.state.as_str(), "succeeded" | "cancelled" | "review")
            || op.not_before > now()
            || op.next_retry > now()
        {
            return Ok(op);
        }
        let mut a = self.get(&op.artifact)?;
        if op.id.starts_with("failed-delete-") && a.keep_on_failure {
            op.state = "cancelled".into();
            op.error = Some("per-download Keep on failure is enabled".into());
            save_operation(&self.db.lock().unwrap(), &op)?;
            return Ok(op);
        }
        let request: DeleteRequest = serde_json::from_str(&op.request)?;
        let (_, revision, _, automatic, generation) = request.fields();
        let explicit_cancellation = if let DeleteRequest::ClusterCancellation((
            _,
            _,
            _,
            _,
            _,
            failure_guard,
            receipt,
            binding,
        )) = &request
        {
            if automatic
                || *failure_guard
                || !self.cluster_cancellation_delete_accepted(&a.id, &a.generation, receipt)?
                || self.cluster_cancellation_delete_binding(&a)? != *binding
            {
                op.state = "review".into();
                op.error = Some(
                    "explicit cancellation receipt changed; review this exact source generation"
                        .into(),
                );
                save_operation(&self.db.lock().unwrap(), &op)?;
                return Ok(op);
            }
            true
        } else {
            false
        };
        let failure_guard = match &request {
            DeleteRequest::ManagedSeed(request) => {
                request.intent.reason == TorrentSeedRetirementReason::Failure
            }
            DeleteRequest::ClusterCancellation(_) => false,
            DeleteRequest::Cluster((_, _, _, _, _, guard)) => *guard,
            _ => {
                op.id.starts_with("cluster-copy-")
                    && !self.cluster_retirement_success(&a.id, &a.generation)?
            }
        };
        if failure_guard && a.keep_on_failure {
            op.state = "retry".into();
            op.error = Some(
                "per-download Keep on failure is enabled; exact cluster source retained".into(),
            );
            op.next_retry = now().saturating_add(30);
            save_operation(&self.db.lock().unwrap(), &op)?;
            return Ok(op);
        }
        if automatic && !a.automatic_cleanup_eligible {
            op.state = "review".into();
            op.error = Some("pre-existing artifact is exempt from automatic deletion pending reviewed migration".into());
            save_operation(&self.db.lock().unwrap(), &op)?;
            return Ok(op);
        }
        if generation != Some(a.generation.as_str()) {
            op.state = "review".into();
            op.error = Some("deletion generation changed or legacy authorization lacks generation; request deletion again after review".into());
            save_operation(&self.db.lock().unwrap(), &op)?;
            return Ok(op);
        }
        if op.attempts == 0
            && ((a.revision != revision && !explicit_cancellation)
                || (automatic && (!a.eligible() || !a.deadline.is_some_and(|d| d <= now()))))
        {
            op.state = "cancelled".into();
            op.error = Some("deletion authorization changed; a fresh request is required".into());
            save_operation(&self.db.lock().unwrap(), &op)?;
            return Ok(op);
        }
        if self.artifact_in_use(&a)? && !self.authorized_processing_use(&a, use_guard) {
            return Err(std::io::Error::from(std::io::ErrorKind::WouldBlock).into());
        }
        if (a.keep && !explicit_cancellation) || a.hold.is_some() || !a.owned {
            return Err(Error::Conflict("payload acquired a hold".into()));
        }
        _lease = self
            .reserve_reconciliation_locked(std::slice::from_ref(&a), use_guard, &guard)?
            .ok_or_else(|| Error::Io(std::io::Error::from(std::io::ErrorKind::WouldBlock)))?;
        op.state = "running".into();
        op.attempts += 1;
        a.state = "deleting".into();
        {
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            // Deletion now owns this generation. A failed or interrupted move
            // cannot regain authority on restart, even if deletion needs retry.
            Self::cancel_relocations(&tx, &a.id, &a.generation, "source deletion started")?;
            save_operation(&tx, &op)?;
            save_artifact(&tx, &a)?;
            tx.commit()?;
        }
        drop(guard);
        let result = authority
            .map_or(Ok(()), |(_, checkpoint)| checkpoint().map_err(Error::from))
            .and_then(|()| self.delete_manifest_checked(&a, op.attempts));
        let _coordinator = self.mutation_guard()?;
        let current = self.get(&a.id)?;
        let saved = self.operation(&op.id)?;
        if current.generation != a.generation
            || current.path != a.path
            || current.root != a.root
            || saved.state != op.state
            || saved.request != op.request
        {
            return Ok(saved);
        }
        // Retain policy edits made while the actual worker owned the bytes.
        a = current;
        match result {
            Ok(()) => {
                op.state = "succeeded".into();
                op.error = None;
                a.state = "deleted".into();
                a.error = None;
            }
            Err(e) => {
                let conflict = matches!(e, Error::Conflict(_));
                op.error = Some(e.to_string());
                a.error = op.error.clone();
                op.state = if conflict { "review" } else { "retry" }.into();
                a.state = "delete_failed".into();
                if conflict {
                    a.hold = Some("review".into());
                }
                let delay = if a.automatic_cleanup_eligible {
                    retry_delay(op.attempts)
                } else {
                    [60, 300, 1800, 21600][(op.attempts.saturating_sub(1) as usize).min(3)]
                };
                op.next_retry = now().saturating_add(delay);
            }
        }
        a.updated_at = now();
        a.revision += 1;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_operation(&tx, &op)?;
        save_artifact(&tx, &a)?;
        event(
            &tx,
            &a.id,
            &op.state,
            op.error.as_deref().unwrap_or("deletion confirmed"),
        )?;
        tx.commit()?;
        Ok(op)
    }
    /// Shared identity/manifest-checked deletion. Call only with a live
    /// exact-generation reservation and generation-specific authorization.
    fn delete_manifest_checked(&self, a: &Artifact, attempts: u32) -> Result<()> {
        let root = self.verify_root(a)?;
        if attempts > 1 {
            match std::fs::symlink_metadata(&a.path) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(e) => return Err(e.into()),
                Ok(_) => (),
            }
        }
        let dir = self.verify(a)?;
        let observed = fs::manifest(&dir, 100_000)?;
        // A restarted partial deletion may have fewer entries, never more
        // or different ones. Every surviving regular file is revalidated.
        let owned: std::collections::HashMap<_, _> = a
            .files
            .iter()
            .map(|entry| (entry.path.as_str(), entry))
            .collect();
        if owned.len() != a.files.len() {
            return Err(Error::Conflict(
                "deletion manifest has duplicate paths".into(),
            ));
        }
        for f in &observed {
            if !owned.get(f.path.as_str()).is_some_and(|owned| {
                owned.identity.same_object(&f.identity)
                    && (f.identity.directory || owned.identity == f.identity)
            }) {
                return Err(Error::Conflict(format!(
                    "unowned or changed entry: {}",
                    f.path
                )));
            }
        }
        let mut files = observed;
        files.sort_by_key(|f| std::cmp::Reverse(f.path.matches('/').count()));
        for f in &files {
            if !f.identity.directory {
                fs::remove_entry(&dir, f)?;
                #[cfg(test)]
                DELETE_FAIL_AFTER.with(|counter| {
                    if let Some(remaining) = counter.get() {
                        if remaining <= 1 {
                            counter.set(None);
                            return Err(Error::Io(std::io::Error::from_raw_os_error(libc::EIO)));
                        }
                        counter.set(Some(remaining - 1));
                    }
                    Ok(())
                })?;
            }
        }
        for f in &files {
            if f.identity.directory {
                fs::remove_entry(&dir, f)?;
            }
        }
        if !fs::names(&dir)?.is_empty() {
            return Err(Error::Conflict("new files appeared during deletion".into()));
        }
        if a.path.parent() != Some(a.root.as_path()) {
            return Err(Error::Conflict(
                "payload no longer an immediate child of root".into(),
            ));
        }
        // Recheck the entry itself; unlinkat cannot follow a replacement.
        let current = fs::open_at(&root, Path::new(a.path.file_name().unwrap()), true)?;
        if !fs::identity_for_file(&current)?.same_object(a.identity.as_ref().unwrap()) {
            return Err(Error::Conflict("payload directory replaced".into()));
        }
        fs::unlink(&root, Path::new(a.path.file_name().unwrap()), true)?;
        Ok(())
    }

    /// Failed-payload expiry is a wall-clock deadline (W3): it survives
    /// restarts and does not wait on daemon uptime or an opt-in toggle.
    pub fn tick(&self) -> Result<()> {
        self.tick_with_history(None)
    }

    /// History is used only to repair missing receipts for confirmed queue
    /// deletions. Normal ownership, generation and payload deletion checks stay
    /// in the inventory journal, independent of history retention.
    pub fn tick_with_history(&self, history: Option<&crate::history::HistoryDb>) -> Result<()> {
        self.drain_writer_naming();
        let due = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare(
                "SELECT data FROM artifacts WHERE state IN ('parked_failed','recovery_imported')",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            let mut due = Vec::new();
            for raw in rows {
                let a: Artifact = serde_json::from_str(&raw?)?;
                if a.eligible() && a.deadline.is_some_and(|d| d <= now()) {
                    due.push((a.id, a.revision));
                }
            }
            due
        };
        for (key, revision) in due {
            // A request id is at most 128 bytes; Runner's own derived rows
            // (`source-move-…`, `scratch-…`) have ids longer than that, and
            // their retention was refused on every tick. Those get a digest.
            let mut request = format!("retention-{key}-{revision}");
            if request.len() > 128 {
                use sha2::Digest as _;
                request = format!("retention-{:x}", sha2::Sha256::digest(request.as_bytes()));
            }
            if let Err(e) = self.request_delete_authorized(&key, revision, &request, 0, true) {
                tracing::warn!(artifact=%key,error=%e,"expiry changed before admission");
            }
        }
        self.run_due_deletes()?;
        self.reconcile_due_relocations()?;
        self.reconcile_workspace_retirements_with_history(history)?;
        self.run_workspace_retirements()?;
        self.reconcile_recoveries()?;
        if let Err(error) = self.exit_wedged_states(exits::HOLD_EXIT_SECONDS, true) {
            tracing::warn!(%error, "automatic exits for review/retained states deferred");
        }
        self.schedule_discovery()?;
        self.run_tasks()?;
        self.reconcile_missing()?;
        self.compact()?;
        // Last, so it never delays the ordered retention, relocation and
        // recovery steps above. Its failed-delete operations run on the next
        // tick's run_due_deletes.
        if let Err(error) = self.reconcile_empty_failed_sources() {
            tracing::warn!(%error, "empty failed-source reconciliation did not complete");
        }
        Ok(())
    }
    /// Execute every delete whose undo window has closed. Part of the
    /// maintenance tick, and kicked by the API once a requested delete's
    /// window expires so the files go when the toast said they would.
    pub fn run_due_deletes(&self) -> Result<()> {
        // No runner lock: execute_delete is serialized by the mutation guard
        // and idempotent, and a delete must not queue behind a scan drain.
        let pending = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare(
                "SELECT id FROM operations WHERE state IN ('queued','running','retry') AND id NOT LIKE 'failed-delete-torrent-seed-%' AND id NOT LIKE 'managed-delete-torrent-seed-%' AND json_extract(data,'$.kind')='delete' AND json_extract(data,'$.not_before')<=unixepoch() AND json_extract(data,'$.next_retry')<=unixepoch() ORDER BY json_extract(data,'$.next_retry'),json_extract(data,'$.created_at') LIMIT 25",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for key in pending {
            if let Err(e) = self.execute_delete(&key) {
                tracing::warn!(operation=%key,error=%e,"artifact deletion remains pending");
            }
        }
        self.reconcile_torrent_stage_cleanups(&|| Ok(()))?;
        self.reconcile_torrent_copy_compaction(&|| Ok(()))?;
        Ok(())
    }
    pub fn compact(&self) -> Result<usize> {
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let rows = {
            let mut stmt = tx.prepare("SELECT data FROM artifacts WHERE state IN ('deleted','source_gone') AND updated_at<?1 AND json_array_length(json_extract(data,'$.files'))>0 LIMIT 100")?;
            let rows = stmt.query_map([now() - 90 * 86400], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let count = rows.len();
        for raw in rows {
            let mut a: Artifact = serde_json::from_str(&raw)?;
            // No live operation consults sidecars for terminal payloads. Keep
            // the small SQL identity/idempotency record, retire bulky manifests
            // and internal sidecar scratch after the diagnostic window.
            let identities = self.state_dir.join("artifact-identities");
            let directory = identities.join(&a.id);
            if directory.try_exists()? {
                let dir = fs::open_dir(&directory)?;
                for entry in fs::manifest(&dir, 100)? {
                    if entry.identity.directory
                        || !(entry.path.ends_with(".json") || entry.path.ends_with(".tmp"))
                    {
                        return Err(Error::Conflict(
                            "unexpected allocation sidecar entry".into(),
                        ));
                    }
                    fs::remove_entry(&dir, &entry)?;
                }
                fs::unlink(&fs::open_dir(&identities)?, Path::new(&a.id), true)?;
            }
            a.files.clear();
            // The manifest goes; the summary stays so the Cleared view still
            // knows how big the payload was.
            tx.execute(
                "UPDATE artifacts SET data=?2, updated_at=?3 WHERE id=?1",
                params![a.id, serde_json::to_string(&a)?, a.updated_at],
            )?;
        }
        tx.execute(
            "DELETE FROM events WHERE seq IN (SELECT seq FROM events WHERE at<?1 LIMIT 1000)",
            [now() - 90 * 86400],
        )?;
        tx.commit()?;
        Ok(count)
    }

    fn register_publication(&self, recovery: &Recovery) -> Result<()> {
        if let Ok(existing) = self.get(&format!("recovery-{}", recovery.id)) {
            self.verify(&existing)?;
            let _coordinator = self.mutation_guard()?;
            self.check_payload_capture(&existing)?;
            let current = self.recovery(&recovery.id)?;
            if current.generation != recovery.generation || current.published != recovery.published
            {
                return Err(Error::Conflict(
                    "recovery publication generation changed".into(),
                ));
            }
            return Ok(());
        }
        let source = self.get(&recovery.artifact)?;
        let root = recovery
            .published
            .parent()
            .ok_or_else(|| Error::Conflict("invalid publication root".into()))?;
        let dir = fs::open_dir(&recovery.published)?;
        let observed = fs::identity_for_file(&dir)?;
        if !recovery
            .publication_identity
            .as_ref()
            .or(recovery.scratch_identity.as_ref())
            .is_some_and(|expected| expected.same_object(&observed))
        {
            return Err(Error::Conflict(
                "recovery publication identity changed".into(),
            ));
        }
        let root_dir = fs::open_dir(root)?;
        let a = Artifact {
            id: format!("recovery-{}", recovery.id),
            generation: recovery.generation.clone(),
            revision: 1,
            job: None,
            path: recovery.published.clone(),
            origin_path: recovery.published.clone(),
            root: root.into(),
            root_identity: fs::identity_for_file(&root_dir)?,
            identity: Some(fs::identity_for_file(&dir)?),
            state: "recovery_staged".into(),
            owned: true,
            automatic_cleanup_eligible: false,
            delivery_lifecycle: false,
            keep: false,
            keep_on_failure: false,
            failure_assessment: None,
            hold: Some("awaiting complete import receipt".into()),
            created_at: now(),
            updated_at: now(),
            retention_seconds: 86400,
            deadline: None,
            files: fs::manifest(&dir, 100_000)?,
            error: None,
            inspected_at: Some(now()),
            salvage: None,
            failure_reason: None,
            gone_reason: None,
        };
        self.sidecar(&a)?;
        let _coordinator = self.mutation_guard()?;
        self.check_payload_capture(&source)?;
        let current = self.recovery(&recovery.id)?;
        if current.generation != recovery.generation || current.published != recovery.published {
            return Err(Error::Conflict(
                "recovery publication generation changed before enrollment".into(),
            ));
        }
        match self.get(&a.id) {
            Ok(existing)
                if existing.generation == a.generation
                    && existing.path == a.path
                    && existing.root == a.root =>
            {
                Ok(())
            }
            Ok(_) => Err(Error::Conflict(
                "recovery publication enrollment was superseded".into(),
            )),
            Err(Error::NotFound) => save_artifact(&self.db.lock().unwrap(), &a),
            Err(error) => Err(error),
        }
    }

    /// Explicit restore quarantine; run before serving requests from a restored
    /// backup. Old retention and deletion intents cannot become active again.
    pub fn quarantine_restore(&self) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let raws = {
            let mut stmt = tx.prepare(
                "SELECT data FROM artifacts WHERE state NOT IN ('deleted','source_gone')",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in raws {
            let mut a: Artifact = serde_json::from_str(&raw)?;
            // Finding 8: record the Keep the row had, so releasing the
            // quarantine restores it instead of leaving Keep on everything.
            // A row already quarantined keeps its first record.
            if a.hold.as_deref() != Some(exits::RESTORE_HOLD) {
                event(
                    &tx,
                    &a.id,
                    "restore_quarantine",
                    if a.keep { "keep=true" } else { "keep=false" },
                )?;
            }
            a.hold = Some(exits::RESTORE_HOLD.into());
            a.keep = true;
            a.deadline = None;
            a.revision += 1;
            save_artifact(&tx, &a)?;
        }
        tx.execute("UPDATE recoveries SET state='review', data=json_set(data,'$.state','review','$.error','Restored backup: reconcile consumer and create a fresh handoff') WHERE state NOT IN ('imported','cancelled')", [])?;
        let ops = {
            let mut stmt = tx.prepare(
                "SELECT data FROM operations WHERE state IN ('queued','running','retry')",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in ops {
            let mut op: Operation = serde_json::from_str(&raw)?;
            op.state = "review".into();
            op.error = Some("restored backup; old deletion invalidated".into());
            save_operation(&tx, &op)?;
        }
        event(
            &tx,
            "installation",
            "restore",
            "all outstanding operations quarantined",
        )?;
        tx.commit()?;
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests;

#[cfg(test)]
mod relocation_failure_tests;

#[cfg(feature = "test-support")]
pub use relocation::RelocationTestFault;

#[cfg(test)]
mod identity_preflight_tests {
    use super::*;

    fn contents(root: &Path) -> std::collections::BTreeMap<std::ffi::OsString, Vec<u8>> {
        std::fs::read_dir(root)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.file_name(), std::fs::read(entry.path()).unwrap())
            })
            .collect()
    }

    /// R3 / W5b: neither state database ever has a `-shm`; a truncated one
    /// left by an older build (the nuc3 SIGBUS) is never mapped.
    #[test]
    fn state_databases_run_without_shm_even_over_a_truncated_one() {
        let tmp = tempfile::tempdir().unwrap();
        let inventory = Inventory::open(tmp.path()).unwrap();
        inventory
            .configure_scan(serde_json::json!({"roots": []}))
            .unwrap();
        assert!(!tmp.path().join("artifacts.sqlite-shm").exists());
        inventory.close().unwrap();
        drop(inventory);
        std::fs::write(tmp.path().join("artifacts.sqlite-shm"), b"abc").unwrap();
        let history_path = tmp.path().join("history.sqlite");
        let history = crate::history::HistoryDb::open(&history_path, None).unwrap();
        drop(history);
        std::fs::write(tmp.path().join("history.sqlite-shm"), b"abc").unwrap();
        let inventory = Inventory::open(tmp.path()).unwrap();
        inventory
            .configure_scan(serde_json::json!({"roots": []}))
            .unwrap();
        let history = crate::history::HistoryDb::open(&history_path, None).unwrap();
        assert_eq!(history.count_filtered(true).unwrap(), 0);
        for shm in ["artifacts.sqlite-shm", "history.sqlite-shm"] {
            assert_eq!(
                std::fs::read(tmp.path().join(shm)).unwrap(),
                b"abc",
                "{shm} untouched"
            );
        }
    }

    #[test]
    fn expected_identity_checks_wal_without_creating_shm_or_checkpointing() {
        let tmp = tempfile::tempdir().unwrap();
        File::create(tmp.path().join("artifacts.lock")).unwrap();
        let path = tmp.path().join("artifacts.sqlite");
        let db = Connection::open(&path).unwrap();
        db.pragma_update(None, "journal_mode", "WAL").unwrap();
        db.set_db_config(
            rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
            true,
        )
        .unwrap();
        db.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL); INSERT INTO meta VALUES('installation','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');").unwrap();
        drop(db);
        let shm = tmp.path().join("artifacts.sqlite-shm");
        if shm.try_exists().unwrap() {
            std::fs::remove_file(shm).unwrap();
        }
        assert!(tmp
            .path()
            .join("artifacts.sqlite-wal")
            .try_exists()
            .unwrap());
        let before = contents(tmp.path());
        Inventory::validate_expected(tmp.path(), Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")).unwrap();
        assert_eq!(
            contents(tmp.path()),
            before,
            "successful preflight must not checkpoint or create shared-memory files"
        );
        assert!(
            Inventory::validate_expected(tmp.path(), Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))
                .is_err()
        );
        assert_eq!(
            contents(tmp.path()),
            before,
            "wrong installation must leave all durable bytes and names unchanged"
        );
    }

    #[test]
    fn missing_enrolled_root_is_never_initialized_by_preflight() {
        let tmp = tempfile::tempdir().unwrap();
        let absent = tmp.path().join("wrong-mount");
        assert!(
            Inventory::validate_expected(&absent, Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
                .is_err()
        );
        assert!(!absent.try_exists().unwrap());
    }
}
