//! The queue-owner task (ARCHITECTURE.md §8.1): the single serialization
//! point for all queue mutation. Inputs: a bounded command/event channel and
//! a 1 Hz tick. Outputs: leases granted to connection tasks (pull model),
//! `arc-swap` snapshots for lock-free readers, broadcast events, journal
//! appends and debounced snapshot saves.
//!
//! Every handler is synchronous — the owner never awaits while reasoning
//! about state. Sends toward writer tasks use `try_send` with a
//! retry-on-tick fallback so owner ⇄ writer backpressure can never deadlock.

#[path = "owner_transitions.rs"]
mod transitions;

use crate::backend::{BackendCommand, BackendFact, BackendOwnerPort, RemovalOutcome};
use crate::events::Event;
use crate::failover::{AttemptOutcome, Ladder, SegmentAttempt, Verdict};
use crate::queue::{
    active_set, final_status, job_dir_name, next_for_server, pick_par_files, recompute_job_totals,
    QueueState, SegRef, SelectionCtx, CLIENT_PARAM,
};
use crate::rate::{RateLimiter, SpeedMeter};
use crate::recovery::{DurableFileProof, ProvenRange, WRITER_PROTOCOL};
use crate::snapshot::{JobSummary, QueueSnapshot, SharedSnapshot, StorageVolumeSnapshot};
use crate::volumes::DiskGuardReading;
use crate::writer::{spawn_writer_scoped, WriteCmd, WriterFact, WriterHandle, WriterScope};
#[path = "owner_recovery.rs"]
mod recovery_worker;
use recovery_worker::{VerificationResult, VerificationState};
#[path = "owner_persistence.rs"]
mod persistence_worker;
use crate::{AddOpts, Tuning};
use arc_swap::ArcSwap;
use nzbd_nzb::ParsedNzb;
use nzbd_state::{
    FsJournal, JobJournals, JournalRecord, SnapshotStore, UncleanMarker, UrlAdmissionReceipt,
};
use nzbd_types::{
    FileId, Health, Job, JobId, JobKind, JobStatus, PostStage, SegmentState, ServerDef, ServerId,
    StageSpan, TorrentControlIntent, TorrentPayloadDisposition, TorrentPhase, TorrentRemovalIntent,
};
use persistence_worker::{IoResult, PersistResult, Persistence};
#[cfg(test)]
#[path = "owner_batch_tests.rs"]
mod batch_tests;
#[path = "owner_payload.rs"]
mod payload_worker;
use payload_worker::PayloadWork;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::{Duration, Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

/// Retained commands exist only to bridge a temporarily full adapter FIFO.
/// Refuse a new control request once this bound is reached rather than growing
/// owner memory without limit while no adapter is attached.
const MAX_PENDING_BACKEND_COMMANDS: usize = 64;

// Only an unenrolled configured root may be initialized. A missing enrolled
// mount remains unavailable; creating a replacement directory cannot prove it.
fn prepare_unenrolled_payload_root(
    inventory: &nzbd_state::artifacts::Inventory,
    root: &Path,
) -> Result<(), nzbd_state::StateError> {
    let enrolled = inventory
        .startup_reconciliation_roots()
        .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?;
    if !enrolled.iter().any(|known| known == root) {
        std::fs::create_dir_all(root)
            .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?;
    }
    Ok(())
}

fn set_owner_param(params: &mut Vec<(String, String)>, key: &str, value: Option<String>) {
    if let Some((_, old)) = params.iter_mut().find(|(name, _)| name == key) {
        if let Some(value) = value {
            *old = value;
        } else {
            params.retain(|(name, _)| name != key);
        }
    } else if let Some(value) = value {
        params.push((key.to_owned(), value));
    }
}

fn mark_new_lifecycle_cohort(job: &mut Job) {
    if !job
        .params
        .iter()
        .any(|(key, value)| key == "*Runner:LifecycleEligible" && value == "1")
    {
        job.params
            .push(("*Runner:LifecycleEligible".into(), "1".into()));
    }
}
use tokio_util::task::TaskTracker;

#[derive(Debug, Clone, Copy)]
struct SeedCheckpoint {
    uploaded_bytes: u64,
    seeding_seconds: u64,
}

fn durable_seed_checkpoints(state: &QueueState) -> HashMap<JobId, SeedCheckpoint> {
    state
        .jobs
        .iter()
        .filter_map(|job| {
            job.torrent.as_ref().map(|torrent| {
                (
                    job.id,
                    SeedCheckpoint {
                        uploaded_bytes: torrent.uploaded_bytes,
                        seeding_seconds: torrent.seeding_seconds,
                    },
                )
            })
        })
        .collect()
}

fn fresh_epoch() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct GenerationMaterialization {
    pub job: Job,
    pub root: PathBuf,
    pub target: PathBuf,
    pub generation: nzbd_state::artifacts::ExternalGeneration,
    pub history: Option<nzbd_state::HistoryEntry>,
    pub emit_finished: bool,
}

/// External commands (wrapped by `EngineHandle` methods).
#[derive(Debug)]
pub(crate) enum QueueCommand {
    MaterializeGeneration {
        request: Box<GenerationMaterialization>,
        reply: oneshot::Sender<Result<(), nzbd_state::StateError>>,
    },
    ApplyPostRecord {
        admission: crate::PostAdmission,
        job: Box<Job>,
        require_snapshot: bool,
        reply: oneshot::Sender<bool>,
    },
    FinishPostAdmission {
        admission: crate::PostAdmission,
        reply: oneshot::Sender<bool>,
    },
    BeginPostAdmission {
        job: JobId,
        reply: oneshot::Sender<Option<crate::PostAdmission>>,
    },
    EndPostAdmission {
        admission: crate::PostAdmission,
        reply: oneshot::Sender<()>,
    },
    RecoveryDocument {
        reply: oneshot::Sender<nzbd_state::QueueSnapshotDoc>,
    },
    RemoveOrphanTorrentSource {
        job: JobId,
        reply: oneshot::Sender<bool>,
    },
    ReserveTorrentAdmission {
        source: nzbd_types::TorrentSource,
        secret: Vec<u8>,
        opts: AddOpts,
        reply: oneshot::Sender<Result<JobId, nzbd_state::StateError>>,
    },
    CommitTorrentAdmission {
        job: JobId,
        commit: Box<crate::queue::TorrentAdmissionCommit>,
        reply: oneshot::Sender<Option<Result<JobId, JobId>>>,
    },
    CancelTorrentAdmission {
        job: JobId,
        reply: oneshot::Sender<Result<bool, nzbd_state::StateError>>,
    },
    AddParsed {
        name: String,
        parsed: Box<ParsedNzb>,
        category: Option<String>,
        priority: i32,
        dupe: Option<nzbd_types::DupeInfo>,
        paused: bool,
        /// Caller-supplied job params, applied in this same turn.
        params: Vec<(String, String)>,
        reply: oneshot::Sender<Result<JobId, nzbd_state::StateError>>,
    },
    AddUrl {
        name: String,
        url: String,
        category: Option<String>,
        priority: i32,
        dupe: Option<nzbd_types::DupeInfo>,
        paused: bool,
        params: Vec<(String, String)>,
        /// `created == false` means the request was already admitted — the
        /// caller must not spawn a second fetch task.
        reply: oneshot::Sender<Result<(JobId, bool), crate::EngineError>>,
    },
    GetUrlAdmission {
        request_token: String,
        reply: oneshot::Sender<Option<UrlAdmissionReceipt>>,
    },
    CompleteUrlFetch {
        job: JobId,
        parsed: Box<ParsedNzb>,
        reply: oneshot::Sender<bool>,
    },
    FailUrlFetch {
        job: JobId,
        error: String,
        reply: oneshot::Sender<()>,
    },
    UpdateUrlFetchProgress {
        job: JobId,
        progress: crate::fetch::FetchRetryState,
        reply: oneshot::Sender<bool>,
    },
    SetFilePaused {
        job: JobId,
        file: FileId,
        paused: bool,
        reply: oneshot::Sender<bool>,
    },
    DeleteFile {
        job: JobId,
        file: FileId,
        reply: oneshot::Sender<bool>,
    },
    Pause {
        job: JobId,
        reply: oneshot::Sender<bool>,
    },
    Resume {
        job: JobId,
        reply: oneshot::Sender<bool>,
    },
    QuiescePayload {
        job: JobId,
        reply: oneshot::Sender<Result<Vec<WriterHandle>, String>>,
    },
    Delete {
        job: JobId,
        delete_files: bool,
        reply: oneshot::Sender<bool>,
    },
    SetPriority {
        job: JobId,
        priority: i32,
        reply: oneshot::Sender<bool>,
    },
    SetKeepOnFailure {
        job: JobId,
        keep: bool,
        reply: oneshot::Sender<bool>,
    },
    SetCategory {
        job: JobId,
        category: Option<String>,
        reply: oneshot::Sender<bool>,
    },
    SetTorrentSeedPolicy {
        job: JobId,
        policy: nzbd_types::SeedPolicy,
        reply: oneshot::Sender<bool>,
    },
    /// Reorder within the queue vec — the scheduling tiebreaker inside a
    /// priority band, and the order the UI displays.
    Move {
        job: JobId,
        op: MoveOp,
        reply: oneshot::Sender<bool>,
    },
    PauseAll {
        /// Requesting client (UA / "web-ui") — logged and carried on the
        /// event so pause flapping is attributable.
        source: String,
        reply: oneshot::Sender<()>,
    },
    ResumeAll {
        source: String,
        reply: oneshot::Sender<()>,
    },
    SetSpeedLimit {
        bytes_per_sec: Option<u64>,
        reply: oneshot::Sender<()>,
    },
    SetTorrentUploadLimit {
        bytes_per_sec: Option<u64>,
        reply: oneshot::Sender<()>,
    },
    /// How many jobs may download at once. Replies with the value
    /// actually applied, which is the request clamped into range — the
    /// caller learns what happened rather than having to re-read.
    SetMaxActiveDownloads {
        n: u32,
        reply: oneshot::Sender<u32>,
    },
    /// Operator-set per-server connection counts. Replies with the values
    /// actually applied after clamping to each server's spawned ceiling.
    SetServerConnectionCaps {
        caps: HashMap<ServerId, u16>,
        reply: oneshot::Sender<HashMap<ServerId, u16>>,
    },

    // -- cluster commands (CLUSTERING.md §7) --------------------------------
    /// Insert (or replace) a job with its ids preserved — the cluster grant
    /// / completion path. Optionally folds the job's shared journals
    /// (cross-node resume).
    ImportJob {
        job: Box<Job>,
        fold_journals: bool,
        emit_finished: bool,
        reply: oneshot::Sender<Result<(), nzbd_state::StateError>>,
    },
    /// Replace only an authority copy that still exists. This is the fenced
    /// terminal-update path: a demoted worker must never resurrect a job that
    /// `RetainJobs` already removed while an external disposition awaited.
    ImportJobIfPresent {
        job: Box<Job>,
        require_snapshot: bool,
        reply: oneshot::Sender<bool>,
    },
    /// Clone a job's full current state out (for grants and completion
    /// reports).
    ExportJob {
        job: JobId,
        reply: oneshot::Sender<Option<Box<Job>>>,
    },
    /// Remove a job from this engine without touching disk artifacts,
    /// history or events — the executor-side handoff cleanup.
    RemoveJobSilent {
        job: JobId,
        reply: oneshot::Sender<bool>,
    },
    /// Mark a job as executing on another node: the local scheduler skips
    /// it; summaries carry the assignee.
    SetDelegated {
        job: JobId,
        node: Option<String>,
        reply: oneshot::Sender<bool>,
    },
    /// Overlay remote progress and post-processing stages onto a delegated
    /// job's summary without changing the authority's durable control state.
    MirrorProgress {
        job: JobId,
        node: Option<String>,
        stats: MirrorStats,
    },
    /// Union-fold the job's shared journal files into local state (reclaim
    /// after a worker died, or adoption after taking office).
    FoldJobJournals {
        job: JobId,
        reply: oneshot::Sender<()>,
    },
    /// Cap connection concurrency per server (cluster-wide provider
    /// account budgets). Absent entry = local config limit.
    SetServerBudgets {
        budgets: HashMap<ServerId, u16>,
        reply: oneshot::Sender<(u64, HashMap<ServerId, u16>)>,
    },
    /// Enable or park ordinary Usenet selection without restarting. Cluster
    /// nodes use this at role transitions so the elected authority remains a
    /// projection/scheduler and never executes unfenced local work.
    SetDownloadEnabled {
        enabled: bool,
        reply: oneshot::Sender<()>,
    },
    /// Become the queue authority: load the shared snapshot (local jobs
    /// win on conflict — the executor copy is fresher), fold all journals,
    /// enable persistence. Unsupported durable rows refuse the transition
    /// without changing the local queue or shared snapshot.
    AdoptAuthority {
        reply: oneshot::Sender<Result<(), nzbd_state::StateError>>,
    },
    /// Replace the queue projection from replicated control. Local-only rows
    /// must not survive takeover or rollback after a failed quorum commit.
    AdoptReplicatedAuthority {
        jobs: Vec<Job>,
        reply: oneshot::Sender<Result<(), nzbd_state::StateError>>,
    },
    /// Crash-only demotion: drop authority persistence and every job not
    /// in `keep` (the leases this node still executes).
    RetainJobs {
        keep: Vec<JobId>,
        reply: oneshot::Sender<()>,
    },

    // -- post-processing hooks (phase 2) ------------------------------------
    /// Post-processing state transitions (PostQueued / Post{stage} /
    /// terminal). Only meaningful on jobs whose download already finished.
    AbandonRelocation {
        operation: String,
        revision: u64,
        generation: String,
        reply: oneshot::Sender<Result<nzbd_state::artifacts::Artifact, String>>,
    },
    BeginResourceProbe {
        job: JobId,
        revision: String,
        by: RetryBy,
        reply: oneshot::Sender<bool>,
    },
    ReleaseResourceHold {
        job: JobId,
        revision: String,
        by: RetryBy,
        reply: oneshot::Sender<bool>,
    },
    /// End a non-storage hold (`Owner::retry_hold`); the error says why not.
    /// A post-side hold needs `custody_checked`: the payload custody check
    /// touches the mount, so the caller runs it off this loop first.
    RetryHold {
        job: JobId,
        revision: String,
        by: RetryBy,
        custody_checked: bool,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Move an automatic hold's next attempt out on its backoff, recording
    /// why the last one was refused.
    RescheduleHold {
        job: JobId,
        revision: String,
        reason: String,
        reply: oneshot::Sender<bool>,
    },
    HoldJob {
        job: JobId,
        cause: nzbd_types::HoldCause,
        stage: String,
        message: String,
        reply: oneshot::Sender<bool>,
    },
    SetJobStatus {
        job: JobId,
        status: JobStatus,
        reply: oneshot::Sender<bool>,
    },
    /// Enter a post-processing stage: set `Post { stage }` AND append the
    /// stage span, closing the previous one. Separate from `SetJobStatus`
    /// because the status and the timeline must not be settable apart —
    /// see [`close_span`].
    RegisterRepairAttempt {
        job: JobId,
        attempt: String,
        reply: oneshot::Sender<bool>,
    },
    RepairProgress {
        job: JobId,
        progress: crate::RepairProgress,
    },
    CloseRepairAttempt {
        job: JobId,
        attempt: String,
    },
    EnterPostStage {
        job: JobId,
        stage: PostStage,
        at_unix: i64,
        /// The manager's monotonic measurement of the stage being left.
        prev_ms: Option<u64>,
        reply: oneshot::Sender<bool>,
    },
    /// Close the running stage span without opening another — the pipeline
    /// ended (success, failure, or an early return). Fire-and-forget: the
    /// job is on its way to history and nothing waits on the timing.
    ClosePostStage {
        job: JobId,
        at_unix: i64,
        ms: Option<u64>,
    },
    /// Delayed-par download (§3.2): unpause the smallest set of paused
    /// par2 files covering `blocks`. Replies with the number of recovery
    /// blocks now downloading (0 = nothing left to unpause).
    ///
    /// `block_size` is the recovery-set slice size read from the job's own
    /// par2 index, and is what lets a hash-named volume — no `.volXX+NN`
    /// marker anywhere — be priced by its size.
    UnpauseParBlocks {
        job: JobId,
        blocks: u32,
        block_size: Option<u64>,
        reply: oneshot::Sender<u32>,
    },
}

/// Bank the running stage's duration onto the job's timeline.
///
/// `measured` is the post manager's monotonic figure and is preferred
/// whenever it exists: wall-clock subtraction is subject to NTP steps, and
/// a repair that reads as negative time is worse than no figure at all.
/// The wall-clock fallback covers the one case the manager cannot measure
/// — a span that outlived the process, where the `Instant` is gone but
/// `started_at_unix` survived in the snapshot.
///
/// Idempotent: a span that already has a duration keeps it, so a close
/// following a close cannot overwrite a good measurement with a stale one.
fn close_span(job: &mut Job, measured: Option<u64>, at_unix: i64) {
    if let Some(prev) = job.stages.last_mut() {
        if prev.ms.is_none() {
            prev.ms = Some(measured.unwrap_or_else(|| {
                at_unix.saturating_sub(prev.started_at_unix).max(0) as u64 * 1000
            }));
        }
    }
}

/// Queue reorder operations (NZBGet GroupMoveTop/Up/Down/Bottom).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveOp {
    Top,
    Up,
    Down,
    Bottom,
}

/// Sliding per-job rate from downloaded-byte deltas between snapshots.
struct JobRateMeter {
    last_bytes: u64,
    last_at: std::time::Instant,
    ema_bps: f64,
}

impl JobRateMeter {
    /// EMA over ~5s; ignores sub-250ms deltas (snapshot bursts).
    fn update(&mut self, bytes_now: u64) -> u64 {
        let now = std::time::Instant::now();
        let dt = now.duration_since(self.last_at).as_secs_f64();
        if dt >= 0.25 {
            let delta = bytes_now.saturating_sub(self.last_bytes) as f64;
            let inst = delta / dt;
            let alpha = (dt / 5.0).min(1.0);
            self.ema_bps += alpha * (inst - self.ema_bps);
            self.last_bytes = bytes_now;
            self.last_at = now;
        }
        self.ema_bps.max(0.0) as u64
    }
}

/// Remote progress mirrored into a delegated job's summary. `stages` is
/// presentation/observability state only: the authority keeps its durable
/// `Completed` status while a remote PP lease runs, so lease adoption and
/// scheduling continue to reason from local control state.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct MirrorStats {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repair_progress: Option<crate::RepairProgress>,
    pub done_articles: u32,
    pub failed_articles: u32,
    pub downloaded_bytes: u64,
    pub health: u16,
    /// `None` is a rolling-upgrade worker that predates exact remaining-byte
    /// mirroring; the authority falls back to its legacy estimate.
    #[serde(default)]
    pub remaining_bytes: Option<u64>,
    #[serde(default)]
    pub stages: Vec<StageSpan>,
}

/// A granted segment lease: everything a connection task needs.
#[derive(Debug, Clone)]
pub(crate) struct Lease {
    pub r: SegRef,
    pub message_id: String,
    pub writer: mpsc::Sender<WriteCmd>,
    pub generation: u64,
}

/// Everything that reaches the owner task.
type Continuation = Box<dyn FnOnce(&mut Owner, bool) + Send>;
type IoContinuation = Box<dyn FnOnce(&mut Owner, IoResult) + Send>;

#[derive(Debug)]
pub(crate) enum EngineMsg {
    PayloadFinished {
        root: PathBuf,
        revision: u64,
        result: IoResult,
    },
    WritersStopped {
        job: JobId,
        generation: u64,
    },
    ScopedNameInspected {
        job: JobId,
        generation: u64,
        name: Option<String>,
    },
    PersistenceFinished {
        revision: u64,
        result: PersistResult,
    },
    IoFinished {
        revision: u64,
        result: IoResult,
    },
    ScopedCommand {
        admission: crate::PostAdmission,
        command: Box<QueueCommand>,
    },
    ScopedEvent {
        admission: crate::PostAdmission,
        event: Event,
    },
    ScopedWriter {
        job: JobId,
        file: FileId,
        generation: u64,
        fact: crate::writer::WriterFact,
    },
    ScopedOutcome {
        job: JobId,
        file: FileId,
        generation: u64,
        message: Box<EngineMsg>,
    },
    VerificationFinished {
        job: JobId,
        generation: u64,
        result: Box<VerificationResult>,
    },
    Command(QueueCommand),
    WorkRequest {
        server: ServerId,
        max: usize,
        reply: oneshot::Sender<Vec<Lease>>,
    },
    FileMetadata {
        job: JobId,
        file: FileId,
        name: String,
        size: u64,
    },
    SegmentWritten {
        job: JobId,
        file: FileId,
        seg_number: u32,
        offset: u64,
        len: u32,
        crc: u32,
        file_size: u64,
        server: ServerId,
    },
    SegmentFailed {
        job: JobId,
        file: FileId,
        seg_number: u32,
        server: ServerId,
        outcome: AttemptOutcome,
    },
    ConnectFailed {
        server: ServerId,
    },
    WriterFinalized {
        job: JobId,
        file: FileId,
        ok: bool,
        final_path: Option<PathBuf>,
        combined_crc: Option<u32>,
    },
    WriterError {
        job: JobId,
        file: FileId,
        error: String,
    },
    /// A real write ran out of space (ENOSPC/EDQUOT). Reported from
    /// wherever it happened — writer, finalize, post-processing — and
    /// latches the disk guard immediately.
    OutOfSpace {
        whence: String,
    },
}

// ---------------------------------------------------------------------------
// Owner
// ---------------------------------------------------------------------------

pub(crate) struct Owner {
    state: QueueState,
    attempts: HashMap<SegRef, SegmentAttempt>,
    blocked: HashMap<ServerId, Instant>,
    writers: HashMap<FileId, WriterHandle>,
    writer_epochs: HashMap<FileId, u64>,
    job_epochs: HashMap<JobId, u64>,
    control_epochs: HashMap<JobId, u64>,
    post_admissions: HashMap<JobId, u64>,
    post_cancellations: HashMap<JobId, CancellationToken>,
    record_transitions: HashSet<JobId>,
    projection_transition: bool,
    startup_reconciliation: HashMap<PathBuf, transitions::StartupRoot>,
    startup_snapshot: Arc<nzbd_state::artifacts::StartupReconciliation>,
    startup_roots_loading: bool,
    startup_roots_ready: bool,
    startup_roots_retry_at: Instant,
    verification: HashMap<JobId, VerificationState>,
    verification_roots: HashSet<PathBuf>,
    allocation_cache: HashMap<JobId, nzbd_state::artifacts::Artifact>,
    owner_heartbeat: Arc<std::sync::atomic::AtomicU64>,
    pub(crate) shutdown_complete: Arc<std::sync::atomic::AtomicBool>,
    retiring_writers: HashMap<JobId, Vec<WriterHandle>>,
    retirement_epochs: HashMap<JobId, u64>,
    deferred_replacements: VecDeque<QueueCommand>,
    pending_writer_birth: HashMap<FileId, u64>,
    allocating_jobs: HashSet<JobId>,
    pending_journal_loads: HashMap<JobId, (u64, usize)>,
    pending_control_commits: HashSet<JobId>,
    /// Exact queue control revision currently awaiting its durable snapshot.
    /// Unlike the unrelated set of commit barriers, this protects overlapping
    /// Pause/Resume intents from releasing one another's operational fence.
    pending_control_revisions: HashMap<JobId, u64>,
    allocated_jobs: HashSet<JobId>,
    finalize_sent: HashSet<FileId>,
    pending_finalize: Vec<(JobId, FileId)>,
    /// Background par2-name inspections already started for each job.
    /// A terminal event must not overtake them: history and post-processing
    /// consume that event immediately and would otherwise preserve the
    /// requestor placeholder even when the par2 answer was milliseconds away.
    pending_name_inspections: HashMap<JobId, u32>,
    file_sizes: HashMap<FileId, u64>,
    /// Jobs whose bytes did not survive the trip to disk, and why.
    ///
    /// Health cannot express this. Health is computed from segment state —
    /// how much of the article set arrived off the wire — and a file whose
    /// segments all downloaded perfectly and then failed to `set_len`,
    /// `sync_data` or `rename` has a PERFECT health score and a wrong file on
    /// disk. On NFS in particular, a deferred writeback error (ENOSPC,
    /// EDQUOT, EIO) surfaces at `sync_data()` and nowhere else, so this is
    /// the ONLY place the daemon can learn the download did not land.
    ///
    /// Before this existed, `ok: false` from the writer set `finalized =
    /// true`, touched no totals, and the job completed as SUCCESS with 500 MiB
    /// of a 48 GiB remux on disk. A downloader may report that it failed. It
    /// may not report that it succeeded when it did not.
    write_failures: HashMap<JobId, String>,
    /// Jobs executing on another node (job → node name).
    delegated: HashMap<JobId, String>,
    mirror: HashMap<JobId, MirrorStats>,
    /// Explicit delayed-PAR download authorization. Presence marks a job as
    /// a post-processing recovery job; only the listed files may receive
    /// leases, even if a user resumes another file while verification waits.
    post_fetch_files: HashMap<JobId, HashSet<FileId>>,
    repair_progress: HashMap<JobId, (String, Option<crate::RepairProgress>)>,
    /// Per-job download-rate EMA, fed from downloaded-byte deltas at
    /// snapshot time (job id → meter).
    job_rates: HashMap<u32, JobRateMeter>,
    /// Wire-byte EMA per job (B/s), folded from the meter's time-stamped
    /// drains — the same measurement as the header rate.
    job_wire_ema: HashMap<u32, f64>,
    /// The same, per server. Same counters, same drains, same alpha — and
    /// the header rate IS the sum of these, so the chips sum to the tile
    /// exactly instead of to a second, differently-derived figure.
    server_wire_ema: HashMap<u32, f64>,
    /// Article retries per job (any failed attempt that goes back on the
    /// ladder). Surfaced in the snapshot: this is the gap between wire
    /// throughput and completed bytes, and it must be visible.
    retry_counts: HashMap<u32, u32>,
    /// The owner is the only queue-state writer and therefore the only
    /// producer of protocol-neutral backend commands.
    backend: BackendOwnerPort,
    /// Commands retained when the bounded backend FIFO is full. They are
    /// retried in order from the next owner tick, never awaited inline. The
    /// queue is bounded by [`MAX_PENDING_BACKEND_COMMANDS`].
    pending_backend_commands: VecDeque<BackendCommand>,
    /// Latest counters known to be present in the durable queue snapshot.
    /// Seed accounting bypasses the adaptive five-minute save ceiling once
    /// either the 30-second or 8 MiB crash-loss bound is reached.
    seed_checkpoints: HashMap<JobId, SeedCheckpoint>,
    /// Wall-clock edge for active seeds in this process. It intentionally
    /// starts empty after restart so offline time is never counted as upload
    /// service and an unclean stop can only extend, never shorten, a limit.
    seed_clock_unix: HashMap<JobId, i64>,
    /// Torrent handles that have received a start/resume request. Admission
    /// creates handles paused, so the shared scheduler is the only component
    /// allowed to make a newly queued transfer live.
    backend_started: HashSet<JobId>,
    /// Latest volatile torrent samples. Durable counters are folded into the
    /// queue record; rates and peers live here so snapshot traffic does not
    /// rewrite the queue once per second.
    torrent_progress: HashMap<JobId, crate::backend::TransferProgress>,
    applied_usenet_limit: Option<Option<u64>>,
    applied_torrent_limit: Option<Option<u64>>,
    history: Option<Arc<nzbd_state::history::HistoryDb>>,

    state_dir: PathBuf,
    persistence: Persistence,
    persistence_callbacks: HashMap<u64, Continuation>,
    journal_suffix: String,
    payload_waiting: VecDeque<PayloadWork>,
    payload_active: HashSet<PathBuf>,
    io_callbacks: HashMap<u64, IoContinuation>,
    pending_sources: nzbd_state::torrent_sources::PendingSourceStore,
    /// How long the last snapshot write took — feeds [`save_spacing`], so a
    /// slow state volume stretches the save cadence instead of consuming
    /// the owner loop.
    last_save_ms: u64,
    background_save_pending: bool,
    /// Lowest free-space reading across every configured write root, updated
    /// by a dedicated prober task (engine spawn wires it). The owner NEVER
    /// calls statvfs itself: on a
    /// write-saturated FUSE/network destination that syscall blocks for
    /// seconds, and it used to run inline every 10th tick — starving lease
    /// handout and cutting throughput 30% on a sawtooth (field report
    /// 2026-07-26). An initially unknown reading never trips the guard; an
    /// incomplete later cycle cannot clear a hold that already exists.
    disk_guard: Arc<ArcSwap<DiskGuardReading>>,
    /// The exact reading used for the most recent admission decision.
    /// Snapshot publication must not reload `disk_guard`: the prober may
    /// publish a newer value between decision and status publication, which
    /// would pair one cycle's hold with another cycle's evidence.
    evaluated_disk_guard: Arc<DiskGuardReading>,
    /// Queue-authority persistence (snapshot save/compact). Worker-mode
    /// engines run with this off; journals stay on regardless.
    persist: bool,
    worker_mode: bool,
    persist_guard: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    budget_tx: watch::Sender<crate::pool::BudgetEnvelope>,
    budget_generation: u64,
    /// Cluster's share-out of a provider's account-wide connection limit.
    cluster_budgets: HashMap<ServerId, u16>,
    /// The operator's per-server connection count, changed from Settings
    /// without a restart.
    ///
    /// Kept apart from the cluster budget rather than both writing
    /// `budget_tx`, because they answer different questions — "how many
    /// may this NODE open" and "how many do I WANT open" — and whichever
    /// wrote last would erase the other's answer. What the connection
    /// tasks see is the smaller of the two.
    user_conn_caps: HashMap<ServerId, u16>,

    shared: SharedSnapshot,
    events: broadcast::Sender<Event>,
    epoch_tx: watch::Sender<u64>,
    meter: Arc<SpeedMeter>,
    limiter: Arc<RateLimiter>,

    servers: Arc<Vec<ServerDef>>,
    tuning: Tuning,
    /// Ordinary queue downloads are disabled on cluster PP-only nodes. The
    /// explicit `post_fetch_files` lane remains available there.
    download_enabled: bool,
    dest_dir: PathBuf,
    pub(crate) artifacts: Arc<nzbd_state::artifacts::Inventory>,
    torrent_payload_roots: Vec<PathBuf>,

    engine_tx: mpsc::Sender<EngineMsg>,
    tracker: TaskTracker,
    cancel: CancellationToken,

    up_since_unix: i64,
    dirty: bool,
    last_save: Instant,

    /// Per-server volume counters + quota/disk guards.
    volumes: crate::volumes::VolumeBook,
    quota_probe_pending: bool,
    quota_reached: bool,
    disk_low: bool,
    /// Latched by an out-of-space error observed on a real write. statvfs
    /// is the forecast; this is the ground truth, and it wins.
    enospc_latched: bool,
    enospc_observed: u64,
    enospc_where: Option<String>,
    guard_tick: u32,
    /// Round-robin cursor over the active set, advanced per granted
    /// lease. See `SelectionCtx::rotate`.
    rotate: usize,
}

fn disk_guard_decision(
    was_held: bool,
    mut write_latched: bool,
    reading: &DiskGuardReading,
    floor: u64,
) -> (bool, bool) {
    let free = reading.available_bytes;
    let clear_at = floor.saturating_mul(2);
    if write_latched
        && clear_at > 0
        && reading.all_roots_known
        && free.is_some_and(|available| available >= clear_at)
    {
        write_latched = false;
    }
    let below_floor = floor > 0 && free.is_some_and(|available| available < floor);
    // Once held, an incomplete cycle is not recovery evidence. Retain the
    // hold until every configured root was measured and the minimum is above
    // the threshold (or twice it for an observed-write latch).
    let incomplete_hold = was_held && !reading.all_roots_known;
    (
        write_latched,
        write_latched || below_floor || incomplete_hold,
    )
}

/// The latest unfinished PP stage, unless a durable final stamp says the whole
/// pipeline already ended. Older spans cannot reopen a later closed pipeline.
fn open_post_stage(job: &Job) -> Option<PostStage> {
    if job
        .params
        .iter()
        .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
    {
        return None;
    }
    job.stages
        .last()
        .filter(|span| span.ms.is_none())
        .map(|span| span.stage)
}

/// Rebuild the narrow delayed-PAR scheduler lane from facts that survive a
/// restart. The in-memory authorization map is intentionally exact: an open PP
/// span permits only unpaused unfinished PAR files, never unrelated payload
/// files that happen to be unpaused in imported or stale state.
fn recovered_post_fetch_files(job: &Job) -> Option<HashSet<FileId>> {
    if !matches!(
        job.status,
        JobStatus::Queued | JobStatus::Downloading | JobStatus::Paused
    ) || open_post_stage(job).is_none()
    {
        return None;
    }
    let files: HashSet<FileId> = job
        .files
        .iter()
        .filter(|file| file.is_par2 && !file.paused && !file.is_terminal())
        .map(|file| file.id)
        .collect();
    (!files.is_empty()).then_some(files)
}

fn recovered_post_fetch_map(state: &QueueState) -> HashMap<JobId, HashSet<FileId>> {
    state
        .jobs
        .iter()
        .filter_map(|job| recovered_post_fetch_files(job).map(|files| (job.id, files)))
        .collect()
}

#[allow(clippy::too_many_arguments)]
impl Owner {
    /// Synchronous construction incl. crash recovery. Authority mode
    /// (`persist = true`): load snapshot, union-replay every per-job
    /// journal (plus a legacy phase-1 global journal, once), fold. Worker
    /// mode: start empty — jobs arrive as leases and fold their own
    /// journals on import.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn recover(
        state_dir: &Path,
        artifact_dir: Option<&Path>,
        expected_installation_id: Option<&str>,
        dest_dir: PathBuf,
        torrent_payload_roots: Vec<PathBuf>,
        history: Option<Arc<nzbd_state::history::HistoryDb>>,
        servers: Arc<Vec<ServerDef>>,
        tuning: Tuning,
        download_enabled: bool,
        persist: bool,
        journal_suffix: &str,
        persist_guard: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
        budget_tx: watch::Sender<crate::pool::BudgetEnvelope>,
        shared: SharedSnapshot,
        events: broadcast::Sender<Event>,
        epoch_tx: watch::Sender<u64>,
        meter: Arc<SpeedMeter>,
        limiter: Arc<RateLimiter>,
        config_speed_limit: Option<u64>,
        config_max_active: Option<u32>,
        backend: BackendOwnerPort,
        engine_tx: mpsc::Sender<EngineMsg>,
        tracker: TaskTracker,
        cancel: CancellationToken,
        owner_heartbeat: Arc<std::sync::atomic::AtomicU64>,
    ) -> Result<Owner, nzbd_state::StateError> {
        let artifacts = Arc::new(
            nzbd_state::artifacts::Inventory::open_expected(
                artifact_dir.unwrap_or(state_dir),
                expected_installation_id,
            )
            .map_err(|e| nzbd_state::StateError::Corrupt(e.to_string()))?,
        );
        // Capture the metadata boundary before runtime Inventory handles can
        // admit allocations or journals while payload workers are delayed.
        let startup_snapshot = Arc::new(
            artifacts
                .capture_startup_reconciliation()
                .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?,
        );
        let marker = UncleanMarker::new(state_dir, journal_suffix);
        let was_unclean = marker.check_and_arm()?;
        let snap_store = SnapshotStore::open(state_dir)?;
        let pending_sources = nzbd_state::torrent_sources::PendingSourceStore::open(state_dir)?;
        let journal = JobJournals::open(state_dir, journal_suffix)?;

        let mut state = QueueState::default();
        let mut file_sizes = HashMap::new();
        let mut replay_count = 0usize;

        if persist {
            if let Some(doc) = snap_store.load()? {
                // Single-node startup restores torrent records before the
                // daemon associates their paused backend handles. Cluster
                // takeover retains its separate unsupported-protocol guard.
                state = QueueState::from_doc(doc);
            }

            // Payload roots and lifecycle reconciliation are runtime work.
            // Only state-volume metadata is touched before the owner exists.
            // Legacy phase-1 global journal: fold once, then retire it.
            let legacy = FsJournal::open(state_dir)?;
            let mut replayed = legacy
                .replay()?
                .into_iter()
                .map(|record| nzbd_state::ProvenJournalRecord {
                    record,
                    provenance: None,
                })
                .collect::<Vec<_>>();
            drop(legacy);
            if !replayed.is_empty() {
                tracing::info!(records = replayed.len(), "migrating legacy global journal");
                let _ = std::fs::remove_file(state_dir.join("segments.journal"));
            }
            replayed.extend(JobJournals::replay_all_with_provenance(state_dir)?);
            replay_count = replayed.len();
            for envelope in replayed {
                let rec = envelope.record;
                if !merge_replayed_proof(
                    &mut state,
                    &rec,
                    envelope.provenance.as_ref(),
                    &artifacts.installation,
                ) {
                    continue;
                }
                let r = SegRef {
                    job: rec.job,
                    file: rec.file,
                    seg_number: rec.segment_number,
                };
                if rec.file_size > 0 {
                    file_sizes.insert(rec.file, rec.file_size);
                }
                if let Some(seg) = state.segment_mut(r) {
                    if !matches!(seg.state, SegmentState::Done { .. }) {
                        seg.state = SegmentState::Done {
                            offset: rec.offset,
                            len: rec.len,
                            crc: rec.crc32,
                        };
                    }
                }
            }
            for j in &mut state.jobs {
                for f in &j.files {
                    let key = format!("*File:size:{}", f.id.0);
                    if let Some(size) = j
                        .params
                        .iter()
                        .find(|(k, _)| k == &key)
                        .and_then(|(_, v)| v.parse::<u64>().ok())
                    {
                        file_sizes.insert(f.id, size);
                    }
                }
                if j.held() {
                    j.status = JobStatus::Paused;
                }
            }
            state.recompute_all_totals();
            // Holds re-derive their recovery from their cause on every load
            // of authority state; a due one is released on the first
            // recovery pass.
            rearm_holds(&mut state, unix_now());
        }

        if was_unclean || replay_count > 0 {
            tracing::info!(
                was_unclean,
                journal_records = replay_count,
                jobs = state.jobs.len(),
                "recovered queue state"
            );
        }

        // Speed limit: the config wins whenever it sets one (a config
        // edit must take effect on reload); the runtime-set persisted
        // value applies only when the config is silent.
        if config_speed_limit.is_some() {
            state.speed_limit_bps = config_speed_limit;
        }
        limiter.set(state.speed_limit_bps);
        // Same rule for concurrency: a value in the file is the operator
        // stating an intent, and it outranks whatever the last runtime
        // nudge happened to leave in the snapshot.
        if let Some(n) = config_max_active {
            state.max_active_downloads = crate::queue::clamp_active_downloads(n);
        }

        let post_fetch_files = recovered_post_fetch_map(&state);
        let seed_checkpoints = durable_seed_checkpoints(&state);
        if !post_fetch_files.is_empty() {
            tracing::info!(
                jobs = post_fetch_files.len(),
                "recovered delayed PAR download authorization"
            );
        }

        let allocated_jobs = HashSet::new();
        let retiring_writers = HashMap::new();
        let mut allocation_cache = HashMap::new();
        // Cache authoritative metadata without treating it as a capability.
        // Active root/path identity and legacy registration are established
        // asynchronously, independently for each payload root after spawn.
        for job in &state.jobs {
            if job.torrent.is_none() {
                if let Some(allocation) = artifacts
                    .for_job(job.id.0)
                    .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?
                {
                    allocation_cache.insert(job.id, allocation);
                }
            }
        }
        let initial_disk_hold = tuning.min_free_disk_bytes > 0;
        let initial_quota_hold = tuning.daily_quota_bytes > 0 || tuning.monthly_quota_bytes > 0;
        let mut owner = Owner {
            state,
            attempts: HashMap::new(),
            blocked: HashMap::new(),
            writers: HashMap::new(),
            writer_epochs: HashMap::new(),
            job_epochs: HashMap::new(),
            control_epochs: HashMap::new(),
            post_admissions: HashMap::new(),
            post_cancellations: HashMap::new(),
            record_transitions: HashSet::new(),
            projection_transition: false,
            startup_reconciliation: HashMap::new(),
            startup_snapshot,
            startup_roots_loading: false,
            startup_roots_ready: false,
            startup_roots_retry_at: Instant::now(),
            verification: HashMap::new(),
            verification_roots: HashSet::new(),
            allocation_cache,
            owner_heartbeat,
            shutdown_complete: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            retiring_writers,
            retirement_epochs: HashMap::new(),
            deferred_replacements: VecDeque::new(),
            pending_writer_birth: HashMap::new(),
            allocating_jobs: HashSet::new(),
            pending_journal_loads: HashMap::new(),
            pending_control_commits: HashSet::new(),
            pending_control_revisions: HashMap::new(),
            allocated_jobs,
            finalize_sent: HashSet::new(),
            pending_finalize: Vec::new(),
            pending_name_inspections: HashMap::new(),
            file_sizes,
            write_failures: HashMap::new(),
            delegated: HashMap::new(),
            mirror: HashMap::new(),
            post_fetch_files,
            repair_progress: HashMap::new(),
            job_rates: HashMap::new(),
            job_wire_ema: HashMap::new(),
            server_wire_ema: HashMap::new(),
            retry_counts: HashMap::new(),
            backend,
            pending_backend_commands: VecDeque::new(),
            seed_checkpoints,
            seed_clock_unix: HashMap::new(),
            backend_started: HashSet::new(),
            torrent_progress: HashMap::new(),
            applied_usenet_limit: None,
            applied_torrent_limit: None,
            history,
            state_dir: state_dir.to_path_buf(),
            persistence: Persistence::start(
                journal,
                SnapshotStore::open(state_dir)?,
                SnapshotStore::open(
                    &state_dir.join("workers").join(
                        journal_suffix
                            .chars()
                            .map(|c| {
                                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                                    c
                                } else {
                                    '_'
                                }
                            })
                            .collect::<String>(),
                    ),
                )?,
                engine_tx.clone(),
            ),
            persistence_callbacks: HashMap::new(),
            io_callbacks: HashMap::new(),
            payload_waiting: VecDeque::new(),
            payload_active: HashSet::new(),
            journal_suffix: journal_suffix.to_owned(),
            pending_sources,
            persist,
            worker_mode: !persist,
            persist_guard,
            budget_tx,
            budget_generation: 0,
            cluster_budgets: HashMap::new(),
            user_conn_caps: HashMap::new(),
            shared,
            events,
            epoch_tx,
            meter,
            limiter,
            servers,
            tuning,
            download_enabled,
            dest_dir,
            artifacts,
            torrent_payload_roots,
            engine_tx,
            tracker,
            cancel,
            up_since_unix: unix_now(),
            dirty: false,
            last_save: Instant::now(),
            last_save_ms: 0,
            background_save_pending: false,
            disk_guard: Arc::new(ArcSwap::from_pointee(DiskGuardReading::default())),
            evaluated_disk_guard: Arc::new(DiskGuardReading::default()),
            volumes: crate::volumes::VolumeBook::load(state_dir, journal_suffix),
            quota_probe_pending: false,
            quota_reached: initial_quota_hold,
            // A configured floor starts fail-safe until the first complete
            // asynchronous probe. Startup remains nonblocking, but no work
            // can slip through the measurement window.
            disk_low: initial_disk_hold,
            enospc_latched: false,
            enospc_observed: 0,
            enospc_where: None,
            rotate: 0,
            guard_tick: 0,
        };
        owner.schedule_startup_reconciliation();
        Ok(owner)
    }

    pub(crate) async fn run(mut self, mut rx: mpsc::Receiver<EngineMsg>) {
        // Fold replayed journal into a fresh snapshot, then finish anything
        // that completed just before the crash.
        self.save_snapshot();
        self.startup_pass();
        self.drain_replacements();
        self.start_verifications();
        self.publish_snapshot(0);
        self.bump_epoch();

        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = tick.tick() => self.on_tick(),
                msg = rx.recv() => match msg {
                    Some(m) => self.on_msg(m),
                    None => break,
                },
            }
        }

        self.drain_shutdown_results(&mut rx).await;
        // The ordered persistence lane captures only after every accepted
        // acknowledgement and its durability continuation has been applied.
        // It owns journal sync, final snapshot and marker disarm; no actor
        // filesystem operation can deadlock an acknowledgement producer.
        let durable = self.flush_shutdown_state().await;
        self.shutdown_complete
            .store(durable, std::sync::atomic::Ordering::Release);
        self.writers.clear();
        self.retiring_writers.clear();
        if durable {
            tracing::info!("queue owner stopped after worker and acknowledgement drain");
        } else {
            tracing::error!("queue owner stopped with final durability unresolved; unclean marker and recovery evidence retained");
        }
    }

    /// Fence new work, then keep consuming the result channel until every
    /// actual producer has returned. Cancellation alone does not release a
    /// writer or verifier's custody, and awaiting producers without receiving
    /// would deadlock any final acknowledgement behind a full channel.
    async fn drain_shutdown_results(&mut self, rx: &mut mpsc::Receiver<EngineMsg>) {
        self.cancel.cancel();
        self.download_enabled = false;
        self.cancel_payload_work();
        self.post_admissions.clear();
        for verification in self.verification.values() {
            verification.cancel.cancel();
        }
        let writers: Vec<WriterHandle> = self
            .writers
            .values()
            .chain(
                self.retiring_writers
                    .values()
                    .flat_map(|writers| writers.iter()),
            )
            .cloned()
            .collect();
        for writer in &writers {
            writer.stop.cancel();
        }
        let mut poll = tokio::time::interval(Duration::from_millis(20));
        poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            // A producer publishes `stopped` only after its last send. Drain
            // buffered results before checking that all producers are gone.
            while let Ok(message) = rx.try_recv() {
                self.on_shutdown_message(message);
            }
            let writers_stopped = writers.iter().all(|writer| *writer.stopped.borrow());
            let workers_stopped = self.tracker.len() <= 1;
            let callbacks_drained = self.persistence_callbacks.is_empty()
                && self.io_callbacks.is_empty()
                && self.payload_active.is_empty()
                && self.payload_waiting.is_empty()
                && self.persistence.in_flight() == 0;
            if writers_stopped
                && workers_stopped
                && callbacks_drained
                && self.verification_roots.is_empty()
                && self.pending_name_inspections.is_empty()
            {
                break;
            }
            tokio::select! {
                message = rx.recv() => match message {
                    Some(message) => self.on_shutdown_message(message),
                    None => { poll.tick().await; },
                },
                _ = poll.tick() => {},
            }
        }
    }

    fn on_shutdown_message(&mut self, message: EngineMsg) {
        match message {
            // An unacknowledged control request cannot acquire new work or
            // persist a new user intent once shutdown has fenced admission.
            EngineMsg::Command(_)
            | EngineMsg::ScopedCommand { .. }
            | EngineMsg::ScopedEvent { .. } => {}
            EngineMsg::WorkRequest { reply, .. } => {
                let _ = reply.send(Vec::new());
            }
            EngineMsg::ScopedOutcome {
                job,
                file,
                generation,
                message,
            } => {
                if self.state.job(job).is_some()
                    && self.writer_epochs.get(&file) == Some(&generation)
                {
                    self.on_shutdown_message(*message);
                }
            }
            // Includes writer facts, verification/name results, and ordered
            // persistence/IO completions. Their ordinary generation checks
            // still apply, while cancelled scheduling paths admit no work.
            message => self.on_msg(message),
        }
    }

    /// Return jobs to `Queued` once they are neither in the active set
    /// nor holding a lease.
    ///
    /// `Downloading` is set the moment a job receives its first lease and
    /// was never once set back, so a job that caught a single segment
    /// during the spill-over at the tail of another job stayed labelled
    /// `Downloading` forever while receiving no work — a permanent claim
    /// made on the strength of one article. Harmless while the UI showed
    /// a flat list; the queue now groups rows by what they are doing, so
    /// the label is a heading a job sits under, and a job sitting under
    /// DOWNLOADING with no rate is the page telling a lie.
    ///
    /// Scans segments only for jobs currently claiming to download, and
    /// only until the first lease is found, so the expensive full scan
    /// happens once per stale job — after which it is `Queued` and no
    /// longer a candidate.
    fn settle_download_labels(&mut self) {
        let demoted = crate::queue::jobs_to_requeue_with_recovery(
            &self.state,
            &self.delegated,
            &self.post_fetch_files,
            self.download_enabled,
            self.quota_reached,
            unix_now(),
        );
        if demoted.is_empty() {
            return;
        }
        for id in demoted {
            if let Some(job) = self.state.job_mut(id) {
                job.status = JobStatus::Queued;
            }
        }
        self.dirty = true;
    }

    /// Publish the effective per-server connection allowance: the smaller
    /// of what the cluster grants this node and what the operator asked
    /// for. An absent entry on either side means "no opinion".
    fn publish_conn_budgets(&mut self) -> HashMap<ServerId, u16> {
        let mut out: HashMap<ServerId, u16> = self.cluster_budgets.clone();
        for (id, want) in &self.user_conn_caps {
            let eff = match out.get(id) {
                Some(cluster) => (*cluster).min(*want),
                None => *want,
            };
            out.insert(*id, eff);
        }
        self.budget_generation = self.budget_generation.saturating_add(1);
        let _ = self.budget_tx.send(crate::pool::BudgetEnvelope {
            generation: self.budget_generation,
            allowances: out.clone(),
        });
        // Raising an allowance unparks tasks asleep on the budget
        // channel; they also need telling there may be work.
        self.bump_epoch();
        out
    }

    fn startup_pass(&mut self) {
        if self.cancel.is_cancelled() {
            return;
        }
        let mut to_finalize = Vec::new();
        let mut to_check = Vec::new();
        for job in &self.state.jobs {
            if self.pending_control_commits.contains(&job.id)
                || self.verification.contains_key(&job.id)
                || self.retiring_writers.contains_key(&job.id)
            {
                continue;
            }
            if !matches!(job.status, JobStatus::Queued | JobStatus::Downloading) {
                continue;
            }
            for file in &job.files {
                if file.is_terminal() && file.has_any_done() && !file.finalized {
                    to_finalize.push((job.id, file.id));
                }
            }
            to_check.push(job.id);
        }
        for (j, f) in to_finalize {
            self.send_finalize(j, f);
        }
        for j in to_check {
            self.check_job_complete(j);
        }
    }

    // -- message dispatch ----------------------------------------------------

    fn on_msg(&mut self, msg: EngineMsg) {
        self.owner_heartbeat
            .store(unix_now() as u64, std::sync::atomic::Ordering::Relaxed);
        match msg {
            EngineMsg::PayloadFinished {
                root,
                revision,
                result,
            } => self.payload_finished(root, revision, result),
            EngineMsg::ScopedNameInspected {
                job,
                generation,
                name,
            } => {
                if self.job_epochs.get(&job) == Some(&generation)
                    && !self.verification.contains_key(&job)
                {
                    self.on_job_name_inspected(job, name);
                } else {
                    match self.pending_name_inspections.get_mut(&job) {
                        Some(count) if *count > 1 => *count -= 1,
                        Some(_) => {
                            self.pending_name_inspections.remove(&job);
                        }
                        None => {}
                    }
                }
                self.drain_replacements();
            }
            EngineMsg::WritersStopped { job, generation } => {
                if self.retirement_epochs.get(&job) == Some(&generation)
                    && self.retiring_writers.get(&job).is_some_and(|writers| {
                        writers.iter().all(|writer| *writer.stopped.borrow())
                    })
                {
                    self.retirement_epochs.remove(&job);
                    self.retiring_writers.remove(&job);
                    self.drain_replacements();
                    self.startup_pass();
                    self.publish_now();
                    self.bump_epoch();
                }
            }
            EngineMsg::PersistenceFinished { revision, result } => {
                self.persistence_finished(revision, result)
            }
            EngineMsg::IoFinished { revision, result } => self.io_finished(revision, result),
            EngineMsg::VerificationFinished {
                job,
                generation,
                result,
            } => self.on_verification_result(job, generation, *result),
            EngineMsg::ScopedCommand { admission, command } => {
                if self.post_scope_current(&admission) {
                    match *command {
                        QueueCommand::ImportJobIfPresent {
                            job,
                            require_snapshot,
                            reply,
                        } => self.apply_post_record(
                            admission,
                            *job,
                            require_snapshot,
                            move |_, ok| {
                                let _ = reply.send(ok);
                            },
                        ),
                        QueueCommand::RemoveJobSilent { reply, .. } => {
                            self.finish_post_admission(admission, move |_, ok| {
                                let _ = reply.send(ok);
                            })
                        }
                        QueueCommand::ApplyPostRecord {
                            admission: scope,
                            job,
                            require_snapshot,
                            reply,
                        } => self.apply_post_record(scope, *job, require_snapshot, move |_, ok| {
                            let _ = reply.send(ok);
                        }),
                        QueueCommand::FinishPostAdmission {
                            admission: scope,
                            reply,
                        } => self.finish_post_admission(scope, move |_, ok| {
                            let _ = reply.send(ok);
                        }),
                        // Replacing an allocation is never an admitted PP update.
                        QueueCommand::MaterializeGeneration { .. }
                        | QueueCommand::ImportJob { .. }
                        | QueueCommand::AdoptAuthority { .. }
                        | QueueCommand::AdoptReplicatedAuthority { .. }
                        | QueueCommand::Delete { .. } => {}
                        command => self.on_command(command),
                    }
                }
            }
            EngineMsg::ScopedEvent { admission, event } => {
                if self.post_scope_current(&admission) {
                    self.emit(event);
                }
            }
            EngineMsg::ScopedOutcome {
                job,
                file,
                generation,
                message,
            } => {
                if self.state.job(job).is_some()
                    && !self.verification.contains_key(&job)
                    && self.writer_epochs.get(&file) == Some(&generation)
                {
                    self.on_msg(*message);
                }
            }
            EngineMsg::ScopedWriter {
                job,
                file,
                generation,
                fact,
            } => self.on_scoped_writer(job, file, generation, fact),
            EngineMsg::Command(cmd) => self.on_command(cmd),
            EngineMsg::WorkRequest { server, max, reply } => {
                let leases = self.grant_work(server, max.max(1));
                let _ = reply.send(leases);
            }
            EngineMsg::SegmentWritten {
                job,
                file,
                seg_number,
                offset,
                len,
                crc,
                file_size,
                server,
            } => {
                tracing::trace!(
                    job = job.0,
                    file = file.0,
                    seg = seg_number,
                    server = server.0,
                    len,
                    "segment written"
                );
                self.on_segment_written(job, file, seg_number, offset, len, crc, file_size, server)
            }
            EngineMsg::SegmentFailed {
                job,
                file,
                seg_number,
                server,
                outcome,
            } => self.on_segment_failed(
                SegRef {
                    job,
                    file,
                    seg_number,
                },
                server,
                outcome,
            ),
            EngineMsg::FileMetadata {
                job,
                file,
                name,
                size,
            } => self.accept_file_metadata(job, file, &name, size),
            EngineMsg::ConnectFailed { server } => self.block_server(server),
            EngineMsg::WriterFinalized {
                job,
                file,
                ok,
                final_path,
                combined_crc,
            } => self.on_writer_finalized(job, file, ok, final_path, combined_crc),
            EngineMsg::OutOfSpace { whence } => self.observe_out_of_space(&whence),
            EngineMsg::WriterError { job, file, error } => {
                tracing::warn!(job = job.0, file = file.0, %error, "writer error; failing file");
                // The errno decides whether retrying can help: a full disk
                // waits for space, a dropped mount retries on its own, and
                // anything else (a digest or size mismatch) waits for a
                // person rather than re-checking a whole file forever.
                if crate::is_out_of_space(&error) {
                    self.observe_out_of_space(&error);
                }
                let stage = if error.starts_with("finalize") {
                    "finalize"
                } else {
                    "download_write"
                };
                self.hold_job(job, crate::io_hold_cause(&error), stage, &error);
            }
        }
    }

    fn on_command(&mut self, cmd: QueueCommand) {
        let replacement = match &cmd {
            QueueCommand::MaterializeGeneration { request, .. } => Some(request.job.id),
            QueueCommand::ImportJob { job, .. } | QueueCommand::ImportJobIfPresent { job, .. } => {
                Some(job.id)
            }
            QueueCommand::RemoveJobSilent { job, .. } | QueueCommand::Delete { job, .. } => {
                Some(*job)
            }
            _ => None,
        };
        if self.projection_transition
            && (replacement.is_some()
                || matches!(
                    &cmd,
                    QueueCommand::AdoptReplicatedAuthority { .. }
                        | QueueCommand::AdoptAuthority { .. }
                ))
        {
            self.deferred_replacements.push_back(cmd);
            return;
        }
        if let Some(job) = replacement {
            let busy = self
                .pending_name_inspections
                .get(&job)
                .is_some_and(|count| *count > 0)
                || self.retiring_writers.contains_key(&job)
                || self.post_admissions.contains_key(&job)
                || self.allocating_jobs.contains(&job)
                || self.record_transitions.contains(&job)
                || self.pending_control_revisions.contains_key(&job)
                || self
                    .verification
                    .get(&job)
                    .is_some_and(|state| state.running)
                || self.state.job(job).is_some_and(|job| {
                    job.files.iter().any(|file| {
                        self.writers
                            .get(&file.id)
                            .is_some_and(|writer| !*writer.stopped.borrow())
                    })
                });
            if busy {
                if let Some(cancel) = self.post_cancellations.get(&job) {
                    cancel.cancel();
                }
                if let Some(state) = self.verification.get(&job) {
                    state.cancel.cancel();
                }
                self.retire_job_writers(job);
                if !self.pending_control_revisions.contains_key(&job) {
                    self.control_epochs.insert(job, fresh_epoch());
                }
                self.deferred_replacements.push_back(cmd);
                self.publish_now();
                self.bump_epoch();
                return;
            }
        }
        if matches!(
            cmd,
            QueueCommand::AdoptReplicatedAuthority { .. } | QueueCommand::AdoptAuthority { .. }
        ) {
            let busy_jobs = self
                .state
                .jobs
                .iter()
                .filter_map(|job| {
                    let id = job.id;
                    let busy = self.retiring_writers.contains_key(&id)
                        || self.post_admissions.contains_key(&id)
                        || self.allocating_jobs.contains(&id)
                        || self.record_transitions.contains(&id)
                        || self.pending_control_revisions.contains_key(&id)
                        || self
                            .verification
                            .get(&id)
                            .is_some_and(|state| state.running)
                        || job.files.iter().any(|file| {
                            self.writers
                                .get(&file.id)
                                .is_some_and(|writer| !*writer.stopped.borrow())
                        });
                    busy.then_some(id)
                })
                .collect::<Vec<_>>();
            if !busy_jobs.is_empty() {
                for job in busy_jobs {
                    if let Some(cancel) = self.post_cancellations.get(&job) {
                        cancel.cancel();
                    }
                    if let Some(state) = self.verification.get(&job) {
                        state.cancel.cancel();
                    }
                    self.retire_job_writers(job);
                    if !self.pending_control_revisions.contains_key(&job) {
                        self.job_epochs.insert(job, fresh_epoch());
                    }
                }
                self.deferred_replacements.push_back(cmd);
                self.publish_now();
                self.bump_epoch();
                return;
            }
        }
        match cmd {
            QueueCommand::MaterializeGeneration { request, reply } => {
                self.materialize_generation_then(*request, move |_, result| {
                    let _ = reply.send(result);
                });
            }
            QueueCommand::ApplyPostRecord {
                admission,
                job,
                require_snapshot,
                reply,
            } => self.apply_post_record(admission, *job, require_snapshot, move |_, ok| {
                let _ = reply.send(ok);
            }),
            QueueCommand::FinishPostAdmission { admission, reply } => {
                self.finish_post_admission(admission, move |_, ok| {
                    let _ = reply.send(ok);
                })
            }
            QueueCommand::BeginPostAdmission { job, reply } => {
                let allowed = !self.cancel.is_cancelled()
                    && self.state.job(job).is_some_and(|j| {
                        !j.held()
                            && matches!(
                                j.status,
                                JobStatus::Completed
                                    | JobStatus::Failed
                                    | JobStatus::PostQueued
                                    | JobStatus::Post { .. }
                            )
                    })
                    && !self.post_admissions.contains_key(&job)
                    && !self.pending_control_commits.contains(&job)
                    && !self.verification.contains_key(&job)
                    && !self.retiring_writers.contains_key(&job);
                if !allowed {
                    let _ = reply.send(None);
                    return;
                }
                if self.state.job(job).is_some_and(|job| {
                    job.files.iter().any(|file| {
                        self.writers
                            .get(&file.id)
                            .is_some_and(|writer| !*writer.stopped.borrow())
                    })
                }) {
                    self.retire_job_writers(job);
                    let _ = reply.send(None);
                    return;
                }
                let generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                let file_ids = self
                    .state
                    .job(job)
                    .unwrap()
                    .files
                    .iter()
                    .map(|file| file.id)
                    .collect::<Vec<_>>();
                for file in file_ids {
                    self.writer_epochs.remove(&file);
                    self.pending_writer_birth.remove(&file);
                    self.writers.remove(&file);
                }
                self.post_admissions.insert(job, generation);
                let cancellation = self.cancel.child_token();
                self.post_cancellations.insert(job, cancellation.clone());
                if let Some(record) = self.state.job_mut(job) {
                    let proofs = record
                        .params
                        .iter()
                        .filter(|(key, _)| key.starts_with("*File:proof:"))
                        .map(|(key, value)| {
                            (
                                key.replacen("*File:proof:", "*File:publication-proof:", 1),
                                value.clone(),
                            )
                        })
                        .collect::<Vec<_>>();
                    record
                        .params
                        .retain(|(key, _)| !key.starts_with("*File:proof:"));
                    for (key, value) in proofs {
                        set_owner_param(&mut record.params, &key, Some(value));
                    }
                    set_owner_param(
                        &mut record.params,
                        "*Recovery:download-retired",
                        Some(generation.to_string()),
                    );
                }
                self.dirty = true;
                self.persist_then(move |owner, ok| {
                    let current = owner
                        .post_cancellations
                        .get(&job)
                        .is_some_and(|cancel| !cancel.is_cancelled())
                        && owner.post_admissions.get(&job) == Some(&generation)
                        && owner.job_epochs.get(&job) == Some(&generation)
                        && !owner.verification.contains_key(&job)
                        && owner.state.job(job).is_some()
                        && !owner.cancel.is_cancelled();
                    if ok && current {
                        if reply
                            .send(Some(crate::PostAdmission {
                                job,
                                generation,
                                cancellation,
                            }))
                            .is_err()
                        {
                            owner.post_admissions.remove(&job);
                            owner.post_cancellations.remove(&job);
                        }
                    } else {
                        owner.post_admissions.remove(&job);
                        owner.post_cancellations.remove(&job);
                        let _ = reply.send(None);
                    }
                });
            }
            QueueCommand::EndPostAdmission { admission, reply } => {
                if self.post_admissions.get(&admission.job) == Some(&admission.generation) {
                    self.post_admissions.remove(&admission.job);
                    if let Some(cancel) = self.post_cancellations.remove(&admission.job) {
                        cancel.cancel();
                    }
                }
                self.drain_replacements();
                let _ = reply.send(());
            }
            QueueCommand::RecoveryDocument { reply } => {
                let _ = reply.send(self.state.to_doc());
            }
            QueueCommand::RemoveOrphanTorrentSource { job, reply } => {
                let linked = self
                    .state
                    .pending_admissions
                    .iter()
                    .any(|pending| pending.job_id == job)
                    || self.state.job(job).is_some();
                if linked {
                    let _ = reply.send(false);
                } else {
                    let sources = self.pending_sources.clone();
                    self.io_then(
                        move || sources.remove(job),
                        move |_, result| {
                            let _ = reply.send(result.is_ok());
                        },
                    );
                }
            }
            QueueCommand::ReserveTorrentAdmission {
                source,
                secret,
                opts,
                reply,
            } => {
                // Reserve from the queue owner first: it alone allocates ids. The
                // reservation is not durable until the protected sidecar exists.
                let id = self.state.reserve_torrent_admission(source, opts);
                let generation = fresh_epoch();
                self.job_epochs.insert(id, generation);
                let sources = self.pending_sources.clone();
                self.io_then(
                    move || sources.write(id, &secret),
                    move |owner, written| match written {
                        Ok(secret_ref) => {
                            if owner.job_epochs.get(&id) != Some(&generation) {
                                let sources = owner.pending_sources.clone();
                                owner.io_then(move || sources.remove(id), move |_, _| {
                                    let _ = reply.send(Err(nzbd_state::StateError::Corrupt(
                                        "torrent admission was superseded before reservation".into(),
                                    )));
                                });
                                return;
                            }
                            if let Some(pending) = owner
                                .state
                                .pending_admissions
                                .iter_mut()
                                .find(|pending| pending.job_id == id)
                            {
                                pending.secret_ref = secret_ref;
                            } else {
                                let sources = owner.pending_sources.clone();
                                owner.io_then(move || sources.remove(id), move |_, _| {
                                    let _ = reply.send(Err(nzbd_state::StateError::Corrupt(
                                        "torrent admission reservation disappeared".into(),
                                    )));
                                });
                                return;
                            }
                            owner.persist_then(move |owner, committed| {
                                let current = owner.job_epochs.get(&id) == Some(&generation);
                                if committed && current {
                                    owner.bump_epoch();
                                    owner.publish_now();
                                    let _ = reply.send(Ok(id));
                                    return;
                                }
                                if current {
                                    owner.state.cancel_torrent_admission(id);
                                    owner.job_epochs.remove(&id);
                                    owner.dirty = true;
                                }
                                let sources = owner.pending_sources.clone();
                                owner.io_then(move || sources.remove(id), move |_, removed| {
                                    if let Err(error) = removed {
                                        tracing::warn!(job=id.0,%error,"could not remove uncommitted torrent source");
                                    }
                                    let _ = reply.send(Err(nzbd_state::StateError::Corrupt(
                                        "torrent admission requires durable queue ownership".into(),
                                    )));
                                });
                            });
                        }
                        Err(error) => {
                            owner.state.cancel_torrent_admission(id);
                            owner.job_epochs.remove(&id);
                            let _ = reply.send(Err(error));
                        }
                    },
                );
            }
            QueueCommand::CommitTorrentAdmission { job, commit, reply } => {
                let pending_index = self
                    .state
                    .pending_admissions
                    .iter()
                    .position(|pending| pending.job_id == job);
                let pending_before = pending_index
                    .and_then(|index| self.state.pending_admissions.get(index).cloned());
                let info_hash = commit.record.info_hash_v1.clone();
                let existing_before = self
                    .state
                    .jobs
                    .iter()
                    .find(|record| {
                        record.torrent.as_ref().is_some_and(|torrent| {
                            torrent.info_hash_v1 == info_hash
                                && !matches!(record.status, JobStatus::Failed | JobStatus::Deleted)
                        })
                    })
                    .cloned();
                let result = self.state.commit_torrent_admission(job, *commit);
                if let Some(result) = result {
                    let generation = fresh_epoch();
                    self.job_epochs.insert(job, generation);
                    self.dirty = true;
                    self.persist_then(move |owner, committed| {
                        if owner.job_epochs.get(&job) != Some(&generation) {
                            let _ = reply.send(None);
                            return;
                        }
                        if !committed {
                            match &result {
                                Ok(created) => owner.state.jobs.retain(|record| record.id != *created),
                                Err(existing) => {
                                    if let Some(before) = existing_before {
                                        if let Some(record) = owner.state.job_mut(*existing) { *record = before; }
                                    }
                                }
                            }
                            if let (Some(index), Some(before)) = (pending_index, pending_before) {
                                let index = index.min(owner.state.pending_admissions.len());
                                owner.state.pending_admissions.insert(index, before);
                            }
                            owner.job_epochs.remove(&job);
                            owner.dirty = true;
                            let _ = reply.send(None);
                            return;
                        }
                        owner.bump_epoch();
                        let sources = owner.pending_sources.clone();
                        owner.io_then(move || sources.remove(job), move |owner, removed| {
                            if let Err(error) = removed {
                                tracing::warn!(job=job.0,%error,"could not remove committed torrent source");
                            }
                            owner.publish_now();
                            let _ = reply.send(Some(result));
                        });
                    });
                } else {
                    let _ = reply.send(None);
                }
            }
            QueueCommand::CancelTorrentAdmission { job, reply } => {
                let before = self
                    .state
                    .pending_admissions
                    .iter()
                    .position(|pending| pending.job_id == job)
                    .and_then(|index| self.state.pending_admissions.get(index).cloned());
                let removed = self.state.cancel_torrent_admission(job);
                if !removed {
                    let _ = reply.send(Ok(false));
                    return;
                }
                let generation = fresh_epoch();
                self.job_epochs.insert(job, generation);
                self.dirty = true;
                self.persist_then(move |owner, committed| {
                    if owner.job_epochs.get(&job) != Some(&generation) {
                        let _ = reply.send(Err(nzbd_state::StateError::Corrupt(
                            "torrent admission cancellation was superseded".into(),
                        )));
                        return;
                    }
                    if !committed {
                        if let Some(before) = before {
                            owner.state.pending_admissions.push(before);
                            owner
                                .state
                                .pending_admissions
                                .sort_by_key(|pending| pending.job_id.0);
                        }
                        owner.job_epochs.remove(&job);
                        owner.dirty = true;
                        let _ = reply.send(Err(nzbd_state::StateError::Corrupt(
                            "torrent admission cancellation requires durable queue ownership"
                                .into(),
                        )));
                        return;
                    }
                    owner.bump_epoch();
                    owner.publish_now();
                    let sources = owner.pending_sources.clone();
                    owner.io_then(
                        move || sources.remove(job),
                        move |_, removed| {
                            let result = removed.map(|()| true);
                            let _ = reply.send(result);
                        },
                    );
                });
            }
            QueueCommand::AddParsed {
                name,
                parsed,
                category,
                priority,
                dupe,
                paused,
                params,
                reply,
            } => {
                if params
                    .iter()
                    .any(|(key, _)| key.starts_with('*') && key != CLIENT_PARAM)
                {
                    let _ = reply.send(Err(nzbd_state::StateError::Corrupt(
                        "job parameters cannot set Runner internal keys".into(),
                    )));
                    return;
                }
                let id = self.state.admit_nzb(
                    name.clone(),
                    &parsed,
                    category,
                    priority,
                    self.tuning.pause_extra_pars,
                );
                if let Some(j) = self.state.job_mut(id) {
                    mark_new_lifecycle_cohort(j);
                    if let Some(dupe) = dupe {
                        j.dupe = dupe;
                    }
                    if paused {
                        j.status = JobStatus::Paused;
                    }
                    j.params.extend(params);
                    j.delivery_lifecycle = nzbd_types::has_delivery_intent(&j.params);
                }
                // Nothing has been written yet, so a better name may take
                // the directory with it.
                let name = self.name_from_requestor(id, true).unwrap_or(name);
                let generation = fresh_epoch();
                self.job_epochs.insert(id, generation);
                let persistent = self.persist;
                self.allocate_new_payload_then(id, move |owner, allocated| {
                    if let Err(error) = allocated {
                        owner.state.jobs.retain(|record| record.id != id);
                        owner.allocated_jobs.remove(&id);
                        owner.allocation_cache.remove(&id);
                        owner.job_epochs.remove(&id);
                        let _ = reply.send(Err(error));
                        return;
                    }
                    if owner.job_epochs.get(&id) != Some(&generation) {
                        let _ = reply.send(Err(nzbd_state::StateError::Corrupt(
                            "job admission was superseded before allocation completed".into(),
                        )));
                        return;
                    }
                    let name = owner
                        .state
                        .job(id)
                        .map(|record| record.name.clone())
                        .unwrap_or(name);
                    owner.persist_then(move |owner, committed| {
                        let current = owner.job_epochs.get(&id) == Some(&generation);
                        if (!persistent || committed) && current {
                            tracing::info!(job = id.0, %name, "job added");
                            owner.publish_now();
                            owner.emit(Event::JobAdded { job: id, name });
                            owner.bump_epoch();
                            let _ = reply.send(Ok(id));
                        } else {
                            if current {
                                owner.state.jobs.retain(|record| record.id != id);
                                owner.allocated_jobs.remove(&id);
                                owner.allocation_cache.remove(&id);
                            }
                            let _ = reply.send(Err(nzbd_state::StateError::Corrupt(
                                "job admission requires durable queue ownership".into(),
                            )));
                        }
                    });
                });
            }
            QueueCommand::AddUrl {
                name,
                url,
                category,
                priority,
                dupe,
                paused,
                params,
                reply,
            } => {
                if params
                    .iter()
                    .any(|(key, _)| key.starts_with('*') && key != CLIENT_PARAM)
                {
                    let _ = reply.send(Err(crate::EngineError::RequestConflict(
                        "job parameters cannot set Runner internal keys".into(),
                    )));
                    return;
                }
                // A transfer token has a durable tombstone in queue.json,
                // separate from visible history and the live job row.
                let transfer_tokens: Vec<&str> = params
                    .iter()
                    .find(|(key, _)| key == "monarr-transfer")
                    .map(|(_, value)| value.as_str())
                    .into_iter()
                    .collect();
                let transfer_count = params
                    .iter()
                    .filter(|(key, _)| key == "monarr-transfer")
                    .count();
                if transfer_count > 1
                    || transfer_tokens
                        .first()
                        .is_some_and(|token| token.is_empty())
                {
                    let _ = reply.send(Err(crate::EngineError::RequestConflict(
                        "monarr-transfer must be exactly one nonempty value".into(),
                    )));
                    return;
                }
                let request_token = transfer_tokens.first().map(|token| (*token).to_string());
                let manual_additional = params
                    .iter()
                    .any(|(key, value)| key == "monarr-manual-additional" && value == "1");
                let delivery_intent = params
                    .iter()
                    .any(|(key, value)| key == "monarr-delivery" && value == "1");
                let submission_sha256 = request_token.as_ref().map(|_| {
                    url_submission_sha256(
                        &name,
                        &url,
                        category.as_deref(),
                        priority,
                        dupe.as_ref(),
                        paused,
                        &params,
                    )
                });
                if let (Some(token), Some(fingerprint)) =
                    (request_token.as_ref(), submission_sha256.as_ref())
                {
                    if !self.persist {
                        let _ = reply.send(Err(crate::EngineError::AdmissionNotDurable));
                        return;
                    }
                    if let Some(receipt) = self
                        .state
                        .url_admission_receipts
                        .iter()
                        .find(|receipt| receipt.request_token == *token)
                    {
                        if receipt.submission_sha256 != *fingerprint {
                            let _ = reply.send(Err(crate::EngineError::RequestConflict(
                                "transfer token was already used for a different submission".into(),
                            )));
                        } else {
                            let _ = reply.send(Ok((JobId(receipt.job_id), false)));
                        }
                        return;
                    }
                }
                let existing = find_url_request(
                    &self.state.jobs,
                    &url,
                    request_token.as_deref(),
                    manual_additional,
                )
                .map(|j| j.id);
                if let Some(id) = existing {
                    if let Some(token) = request_token.as_ref() {
                        let existing_job = self.state.job(id).expect("matched URL job");
                        if !url_request_matches(
                            existing_job,
                            &url,
                            manual_additional,
                            delivery_intent,
                        ) {
                            let _ = reply.send(Err(crate::EngineError::RequestConflict(
                                "transfer token was already used for a different submission".into(),
                            )));
                            return;
                        }
                        let receipt = UrlAdmissionReceipt {
                            protocol_version: 1,
                            request_token: token.clone(),
                            submission_sha256: submission_sha256.clone().unwrap_or_default(),
                            job_id: id.0,
                            generation: url_admission_generation(
                                id,
                                token,
                                &self.artifacts.installation,
                            ),
                            runner_instance_id: self.artifacts.installation.clone(),
                            terminal_outcome: None,
                            outcome: job_status_name(existing_job.status).into(),
                            failure_stage: None,
                            failure_cause: None,
                            custody_state: "runner_owned".into(),
                            keep_source_on_failure: false,
                            source_cleanup_status: None,
                            publication_path: None,
                            source_artifact_id: None,
                            source_generation: None,
                        };
                        self.state.url_admission_receipts.push(receipt);
                        let token = token.clone();
                        let generation = fresh_epoch();
                        self.job_epochs.insert(id, generation);
                        self.persist_then(move |owner, committed| {
                            let current = owner.job_epochs.get(&id) == Some(&generation);
                            if committed && current {
                                let _ = reply.send(Ok((id, false)));
                            } else {
                                if current {
                                    owner
                                        .state
                                        .url_admission_receipts
                                        .retain(|receipt| receipt.request_token != token);
                                }
                                let _ = reply.send(Err(crate::EngineError::AdmissionNotDurable));
                            }
                        });
                        return;
                    }
                    tracing::info!(job = id.0, %url, "url add deduplicated by request identity");
                    if !params.is_empty() {
                        let before = self.state.job(id).cloned();
                        if let Some(j) = self.state.job_mut(id) {
                            for (k, v) in params {
                                if k == "monarr-transfer"
                                    && j.params.iter().any(|(key, value)| key == &k && value != &v)
                                {
                                    continue;
                                }
                                match j.params.iter_mut().find(|(pk, _)| *pk == k) {
                                    Some(slot)
                                        if k != "monarr-transfer"
                                            && k != "monarr-delivery"
                                            && k != "monarr-manual-additional" =>
                                    {
                                        slot.1 = v
                                    }
                                    Some(_) => (),
                                    None if k != "monarr-transfer"
                                        && k != "monarr-delivery"
                                        && k != "monarr-manual-additional" =>
                                    {
                                        j.params.push((k, v))
                                    }
                                    None => (),
                                }
                            }
                        }
                        let generation = fresh_epoch();
                        self.job_epochs.insert(id, generation);
                        let persistent = self.persist;
                        self.persist_then(move |owner, committed| {
                            let current = owner.job_epochs.get(&id) == Some(&generation);
                            if (!persistent || committed) && current {
                                owner.publish_now();
                                owner.bump_epoch();
                                let _ = reply.send(Ok((id, false)));
                            } else {
                                if current {
                                    if let Some(before) = before {
                                        if let Some(record) = owner.state.job_mut(id) {
                                            *record = before;
                                        }
                                    }
                                }
                                let _ = reply.send(Err(crate::EngineError::AdmissionNotDurable));
                            }
                        });
                        return;
                    }
                    let _ = reply.send(Ok((id, false)));
                    return;
                }
                if request_token.is_some() && !self.persist {
                    let _ = reply.send(Err(crate::EngineError::AdmissionNotDurable));
                    return;
                }
                let id = self.state.admit_url(name.clone(), &url, category, priority);
                if let Some(j) = self.state.job_mut(id) {
                    mark_new_lifecycle_cohort(j);
                    if let Some(dupe) = dupe {
                        j.dupe = dupe;
                    }
                    if paused {
                        j.params.push(("*AddPaused".into(), "yes".into()));
                    }
                    j.params.extend(params);
                    j.delivery_lifecycle = nzbd_types::has_delivery_intent(&j.params);
                }
                let name = self.name_from_requestor(id, true).unwrap_or(name);
                if let (Some(token), Some(fingerprint)) =
                    (request_token.as_ref(), submission_sha256.as_ref())
                {
                    self.state.url_admission_receipts.push(UrlAdmissionReceipt {
                        protocol_version: 1,
                        request_token: token.clone(),
                        submission_sha256: fingerprint.clone(),
                        job_id: id.0,
                        generation: url_admission_generation(
                            id,
                            token,
                            &self.artifacts.installation,
                        ),
                        runner_instance_id: self.artifacts.installation.clone(),
                        terminal_outcome: None,
                        outcome: "fetching".into(),
                        failure_stage: None,
                        failure_cause: None,
                        custody_state: "runner_owned".into(),
                        keep_source_on_failure: false,
                        source_cleanup_status: None,
                        publication_path: None,
                        source_artifact_id: None,
                        source_generation: None,
                    });
                }
                let generation = fresh_epoch();
                self.job_epochs.insert(id, generation);
                let persistent = self.persist;
                self.persist_then(move |owner, committed| {
                    let current = owner.job_epochs.get(&id) == Some(&generation);
                    if (!persistent || committed) && current {
                        tracing::info!(job = id.0, %name, %url, "url job added (fetching)");
                        owner.publish_now();
                        owner.emit(Event::JobAdded { job: id, name });
                        owner.bump_epoch();
                        let _ = reply.send(Ok((id, true)));
                    } else {
                        if current {
                            owner.state.jobs.retain(|record| record.id != id);
                            owner
                                .state
                                .url_admission_receipts
                                .retain(|receipt| receipt.job_id != id.0);
                        }
                        let _ = reply.send(Err(crate::EngineError::AdmissionNotDurable));
                    }
                });
            }
            QueueCommand::GetUrlAdmission {
                request_token,
                reply,
            } => {
                let index = self
                    .state
                    .url_admission_receipts
                    .iter()
                    .position(|receipt| receipt.request_token == request_token);
                let Some(index) = index else {
                    let _ = reply.send(None);
                    return;
                };
                let before_refresh = self.state.url_admission_receipts[index].clone();
                let mut refreshed = before_refresh.clone();
                let inventory = self.artifacts.clone();
                let request_token = request_token.clone();
                let job = JobId(before_refresh.job_id);
                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                self.io_then(
                    move || {
                        // A tombstone outlives the queue row, but its source may still
                        // be explicitly removed or kept from the Files UI. Refresh
                        // only from the exact durable artifact generation; absence or
                        // generation mismatch is not evidence that bytes were deleted.
                        let changed = refresh_url_admission_custody(&mut refreshed, &inventory);
                        Ok((refreshed, changed))
                    },
                    move |owner, refreshed| {
                        let Ok((refreshed, changed)) = refreshed else {
                            let _ = reply.send(Some(before_refresh));
                            return;
                        };
                        if !changed {
                            let _ = reply.send(Some(refreshed));
                            return;
                        }
                        let Some(index) = owner.state.url_admission_receipts.iter()
                            .position(|receipt| receipt.request_token == request_token) else {
                                let _ = reply.send(None);
                                return;
                            };
                        owner.state.url_admission_receipts[index] = refreshed.clone();
                let generation = fresh_epoch();
                        owner.control_epochs.insert(job, generation);
                        let persistent = owner.persist;
                        owner.persist_then(move |owner, committed| {
                            let current = owner.control_epochs.get(&job) == Some(&generation) && owner.job_epochs.get(&job) == Some(&scope_generation);
                            if (!persistent || committed) && current {
                                let _ = reply.send(Some(refreshed));
                            } else {
                                if current {
                                    if let Some(index) = owner.state.url_admission_receipts.iter()
                                        .position(|receipt| receipt.request_token == request_token) {
                                        owner.state.url_admission_receipts[index] = before_refresh.clone();
                                    }
                                }
                                tracing::error!(%request_token, "could not persist refreshed URL admission custody");
                                let _ = reply.send(Some(before_refresh));
                            }
                        });
                    },
                );
            }
            QueueCommand::CompleteUrlFetch { job, parsed, reply } => {
                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                let ok = self
                    .state
                    .complete_url_fetch(job, &parsed, self.tuning.pause_extra_pars);
                if !ok {
                    let _ = reply.send(false);
                    return;
                }
                if let Some(record) = self.state.job_mut(job) {
                    if let Some(pos) = record
                        .params
                        .iter()
                        .position(|(key, _)| key == "*AddPaused")
                    {
                        record.params.remove(pos);
                        record.status = JobStatus::Paused;
                    }
                }
                // `complete_url_fetch` has just re-run the evidence pass
                // against the fetched NZB. Allocation is asynchronous and
                // must finish before this completion is acknowledged.
                self.name_from_requestor(job, true);
                let generation = fresh_epoch();
                self.control_epochs.insert(job, generation);
                let persistent = self.persist;
                self.allocate_new_payload_then(job, move |owner, allocated| {
                    if let Err(error) = allocated {
                        let message = format!("payload allocation: {error}");
                        owner.hold_job(
                            job,
                            allocation_hold_cause(&message),
                            "download_write",
                            &message,
                        );
                    }
                    if owner.control_epochs.get(&job) != Some(&generation)
                        || owner.job_epochs.get(&job) != Some(&scope_generation)
                    {
                        let _ = reply.send(false);
                        return;
                    }
                    owner.persist_then(move |owner, committed| {
                        let current = owner.control_epochs.get(&job) == Some(&generation)
                            && owner.job_epochs.get(&job) == Some(&scope_generation);
                        if (!persistent || committed) && current {
                            tracing::info!(job = job.0, "url fetch complete");
                            owner.publish_now();
                            owner.emit(Event::UrlFetchResolved { job });
                            owner.bump_epoch();
                            let _ = reply.send(true);
                        } else {
                            if current {
                                owner.hold_job(
                                    job,
                                    nzbd_types::HoldCause::StorageUnavailable,
                                    "snapshot",
                                    "URL fetch completion could not be persisted",
                                );
                            }
                            let _ = reply.send(false);
                        }
                    });
                });
            }
            QueueCommand::FailUrlFetch { job, error, reply } => {
                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                let before_status = self.state.job(job).map(|record| record.status);
                let name = match self.state.job_mut(job) {
                    Some(j) if matches!(j.status, JobStatus::Fetching) => {
                        j.status = JobStatus::Failed;
                        Some(j.name.clone())
                    }
                    _ => None,
                };
                if let Some(name) = name {
                    tracing::warn!(job = job.0, %error, "url fetch failed");
                    let generation = fresh_epoch();
                    self.control_epochs.insert(job, generation);
                    let persistent = self.persist;
                    self.persist_then(move |owner, committed| {
                        let current = owner.control_epochs.get(&job) == Some(&generation)
                            && owner.job_epochs.get(&job) == Some(&scope_generation);
                        if (!persistent || committed) && current {
                            owner.publish_now();
                            owner.emit(Event::JobFinished {
                                job,
                                name,
                                status: JobStatus::Failed,
                                health: 0,
                            });
                            owner.emit(Event::UrlFetchResolved { job });
                            owner.bump_epoch();
                        } else if current {
                            if let (Some(status), Some(record)) =
                                (before_status, owner.state.job_mut(job))
                            {
                                record.status = status;
                            }
                            owner.dirty = true;
                        }
                        let _ = reply.send(());
                    });
                    return;
                }
                let _ = reply.send(());
            }
            QueueCommand::UpdateUrlFetchProgress {
                job,
                progress,
                reply,
            } => {
                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                let changed = if let Some(record) = self
                    .state
                    .job_mut(job)
                    .filter(|record| record.status == JobStatus::Fetching)
                {
                    set_owner_param(
                        &mut record.params,
                        crate::fetch::RETRY_ATTEMPT_PARAM,
                        Some(progress.attempts.to_string()),
                    );
                    set_owner_param(
                        &mut record.params,
                        crate::fetch::NEXT_RETRY_PARAM,
                        progress.next_retry_at.map(|at| at.to_string()),
                    );
                    set_owner_param(
                        &mut record.params,
                        crate::fetch::BLOCKED_SINCE_PARAM,
                        progress.blocked_since.map(|at| at.to_string()),
                    );
                    set_owner_param(
                        &mut record.params,
                        crate::fetch::LAST_ERROR_PARAM,
                        progress.last_error,
                    );
                    true
                } else {
                    false
                };
                if !changed {
                    let _ = reply.send(false);
                    return;
                }
                let generation = fresh_epoch();
                self.control_epochs.insert(job, generation);
                let persistent = self.persist;
                self.persist_then(move |owner, committed| {
                    let current = owner.control_epochs.get(&job) == Some(&generation)
                        && owner.job_epochs.get(&job) == Some(&scope_generation);
                    let durable = (!persistent || committed) && current;
                    if durable {
                        owner.publish_now();
                    } else if current {
                        owner.dirty = true;
                        owner.hold_job(
                            job,
                            nzbd_types::HoldCause::StorageUnavailable,
                            "snapshot",
                            "URL fetch progress could not be persisted",
                        );
                    }
                    let _ = reply.send(durable);
                });
            }
            QueueCommand::SetFilePaused {
                job,
                file,
                paused,
                reply,
            } => {
                // During delayed-PAR recovery, the verifier owns file
                // eligibility. A generic FileResume must not smuggle an
                // archive (or any other file) into the recovery lane while
                // the job temporarily carries a download status.
                let recovery_allows = self
                    .post_fetch_files
                    .get(&job)
                    .is_none_or(|allowed| paused || allowed.contains(&file));
                let ok = recovery_allows
                    && match self.state.job_mut(job) {
                        Some(j) => match j.files.iter_mut().find(|f| f.id == file) {
                            Some(f) => {
                                f.paused = paused;
                                true
                            }
                            None => false,
                        },
                        None => false,
                    };
                if ok {
                    if let Some(j) = self.state.job_mut(job) {
                        recompute_job_totals(j);
                    }
                    self.dirty = true;
                    self.bump_epoch();
                    self.publish_now();
                    self.check_job_complete(job);
                }
                let _ = reply.send(ok);
            }
            QueueCommand::DeleteFile { job, file, reply } => {
                let ok = match self.state.job_mut(job) {
                    Some(j) => {
                        let before = j.files.len();
                        j.files.retain(|f| f.id != file);
                        if j.files.len() != before {
                            recompute_job_totals(j);
                            true
                        } else {
                            false
                        }
                    }
                    None => false,
                };
                if ok {
                    self.writers.remove(&file);
                    if let Some(allowed) = self.post_fetch_files.get_mut(&job) {
                        allowed.remove(&file);
                    }
                    self.dirty = true;
                    self.bump_epoch();
                    self.publish_now();
                    self.check_job_complete(job);
                }
                let _ = reply.send(ok);
            }
            QueueCommand::Pause { job, reply } => {
                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                if let Some(mut control) = self
                    .state
                    .job(job)
                    .and_then(|j| j.control())
                    .filter(|c| c.lifecycle == "held")
                {
                    control.manual_pause = true;
                    let Some(revision) = control
                        .revision
                        .parse::<u64>()
                        .ok()
                        .and_then(|r| r.checked_add(1))
                    else {
                        let _ = reply.send(false);
                        return;
                    };
                    control.revision = revision.to_string();
                    self.state.job_mut(job).unwrap().set_control(&control);
                    self.dirty = true;
                    let generation = fresh_epoch();
                    self.control_epochs.insert(job, generation);
                    self.pending_control_revisions.insert(job, generation);
                    let persistent = self.persist;
                    self.persist_then(move |owner, committed| {
                        let lifecycle_current = owner.control_epochs.get(&job) == Some(&generation)
                            && owner.job_epochs.get(&job) == Some(&scope_generation);
                        let current = lifecycle_current
                            && owner.pending_control_revisions.get(&job) == Some(&generation);
                        let ok = (!persistent || committed) && current;
                        if ok {
                            owner.pending_control_revisions.remove(&job);
                            owner.publish_now();
                            owner.emit(Event::JobControlChanged { job, control });
                        } else if owner.pending_control_revisions.get(&job) == Some(&generation) {
                            owner.pending_control_revisions.remove(&job);
                            if lifecycle_current {
                                owner.dirty = true;
                            }
                        }
                        let _ = reply.send(ok);
                    });
                    return;
                }
                let prior = self.state.job(job).map(|record| {
                    (
                        record.status,
                        record
                            .torrent
                            .as_ref()
                            .map(|torrent| (torrent.control_intent, torrent.stop_reason)),
                    )
                });
                let torrent = match self.state.job_mut(job) {
                    Some(j)
                        if j.kind == JobKind::Torrent
                            && matches!(j.status, JobStatus::Queued | JobStatus::Downloading)
                            && j.torrent.is_some() =>
                    {
                        j.status = JobStatus::Paused;
                        j.torrent.as_mut().unwrap().control_intent = TorrentControlIntent::Paused;
                        j.torrent.as_mut().unwrap().stop_reason =
                            Some(nzbd_types::TorrentStopReason::Manual);
                        true
                    }
                    _ => false,
                };
                let ok = if torrent {
                    true
                } else {
                    self.state.job_mut(job).is_some_and(|record| {
                        if matches!(record.status, JobStatus::Queued | JobStatus::Downloading) {
                            record.status = JobStatus::Paused;
                            true
                        } else {
                            false
                        }
                    })
                };
                if !ok {
                    let _ = reply.send(false);
                    return;
                }
                self.dirty = true;
                let generation = fresh_epoch();
                self.control_epochs.insert(job, generation);
                self.pending_control_revisions.insert(job, generation);
                let persistent = self.persist;
                self.persist_then(move |owner, committed| {
                    let lifecycle_current = owner.control_epochs.get(&job) == Some(&generation)
                        && owner.job_epochs.get(&job) == Some(&scope_generation);
                    let current = lifecycle_current
                        && owner.pending_control_revisions.get(&job) == Some(&generation);
                    let ok = (!persistent || committed) && current;
                    if ok {
                        if torrent && !owner.enqueue_backend_command(BackendCommand::Pause { job })
                        {
                            owner.pending_control_revisions.remove(&job);
                            owner.hold_job(
                                job,
                                nzbd_types::HoldCause::StorageUnavailable,
                                "snapshot",
                                "Pause could not be delivered to the torrent backend",
                            );
                            let _ = reply.send(false);
                            return;
                        }
                        owner.pending_control_revisions.remove(&job);
                        owner.bump_epoch();
                        owner.publish_now();
                    } else if owner.pending_control_revisions.get(&job) == Some(&generation) {
                        owner.pending_control_revisions.remove(&job);
                        if lifecycle_current {
                            owner.dirty = true;
                            if let (Some(record), Some((status, prior_torrent))) =
                                (owner.state.job_mut(job), prior)
                            {
                                // Restore only this request's provisional fields;
                                // a newer hold or backend fact remains authoritative.
                                if !record.held() {
                                    if record.status == JobStatus::Paused {
                                        record.status = status;
                                    }
                                    if let (Some(torrent), Some((intent, reason))) =
                                        (record.torrent.as_mut(), prior_torrent)
                                    {
                                        if torrent.control_intent == TorrentControlIntent::Paused
                                            && torrent.stop_reason
                                                == Some(nzbd_types::TorrentStopReason::Manual)
                                        {
                                            torrent.control_intent = intent;
                                            torrent.stop_reason = reason;
                                        }
                                    }
                                }
                            }
                            owner.bump_epoch();
                            owner.publish_now();
                        }
                    }
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::Resume { job, reply } => {
                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                // Resource/custody release has its own admission protocol. Manual
                // resume cannot erase a hold merely because statvfs looks healthy.
                if self.state.job(job).is_some_and(|j| j.held()) {
                    let _ = reply.send(false);
                    return;
                }
                if self.retiring_writers.contains_key(&job) {
                    let _ = reply.send(false);
                    return;
                }
                if self
                    .state
                    .job(job)
                    .and_then(|j| j.torrent.as_ref())
                    .is_some_and(|t| {
                        matches!(t.phase, TorrentPhase::Seeding | TorrentPhase::PausedSeed)
                            && t.ready_at_unix.is_some()
                            && crate::torrent_runtime::seed_policy_reached(t)
                    })
                {
                    let _ = reply.send(false);
                    return;
                }
                let torrent = match self.state.job_mut(job) {
                    Some(j)
                        if j.kind == JobKind::Torrent
                            && matches!(j.status, JobStatus::Paused)
                            && j.torrent.is_some() =>
                    {
                        j.status = JobStatus::Queued;
                        let torrent = j.torrent.as_mut().unwrap();
                        torrent.control_intent = TorrentControlIntent::Running;
                        torrent.stop_reason = None;
                        // Historical completion must not stop missing-file recovery
                        // before a new Ready fact verifies the selected payload.
                        if torrent.phase == TorrentPhase::MissingFiles {
                            torrent.ready_at_unix = None;
                            torrent.content_path = None;
                        }
                        if torrent.ready_at_unix.is_none() {
                            torrent.phase = nzbd_types::TorrentPhase::Queued;
                            torrent.last_activity_unix = Some(unix_now());
                        }
                        true
                    }
                    _ => false,
                };
                let ok = if torrent {
                    true
                } else {
                    self.state.job_mut(job).is_some_and(|record| {
                        if matches!(record.status, JobStatus::Paused) {
                            record.status = JobStatus::Queued;
                            true
                        } else {
                            false
                        }
                    })
                };
                if !ok {
                    let _ = reply.send(false);
                    return;
                }
                self.dirty = true;
                let generation = fresh_epoch();
                self.control_epochs.insert(job, generation);
                self.pending_control_revisions.insert(job, generation);
                let persistent = self.persist;
                self.persist_then(move |owner, committed| {
                    let lifecycle_current = owner.control_epochs.get(&job) == Some(&generation)
                        && owner.job_epochs.get(&job) == Some(&scope_generation);
                    let current = lifecycle_current
                        && owner.pending_control_revisions.get(&job) == Some(&generation);
                    let ok = (!persistent || committed) && current;
                    if ok {
                        owner.pending_control_revisions.remove(&job);
                        owner.bump_epoch();
                        owner.publish_now();
                    } else if owner.pending_control_revisions.get(&job) == Some(&generation) {
                        owner.pending_control_revisions.remove(&job);
                        if lifecycle_current {
                            owner.dirty = true;
                            owner.hold_job(
                                job,
                                nzbd_types::HoldCause::StorageUnavailable,
                                "snapshot",
                                "Resume intent could not be persisted",
                            );
                        }
                    }
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::QuiescePayload { job, reply } => {
                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                if self.delegated.contains_key(&job) {
                    let _ = reply.send(Err(
                        "remote writer lease must be retired before payload deletion".into(),
                    ));
                    return;
                }
                let Some(record) = self.state.job(job) else {
                    let _ = reply.send(Err("job not found".into()));
                    return;
                };
                if record.torrent.is_some()
                    || matches!(
                        record.status,
                        JobStatus::Post { .. } | JobStatus::PostQueued
                    )
                {
                    let _ = reply.send(Err(
                        "post-processing or torrent ownership must quiesce before deletion".into(),
                    ));
                    return;
                }
                let generation = self.control_epochs.get(&job).copied();
                let artifacts = self.artifacts.clone();
                self.io_then(
                    move || {
                        artifacts.prepare_forget(job.0).map(|_| ()).map_err(|e| {
                            nzbd_state::StateError::Corrupt(format!("artifact inventory: {e}"))
                        })
                    },
                    move |owner, result| {
                        if let Err(error) = result {
                            let _ = reply.send(Err(error.to_string()));
                            return;
                        }
                        if owner.control_epochs.get(&job).copied() != generation
                            || owner.job_epochs.get(&job) != Some(&scope_generation)
                            || owner.state.job(job).is_none()
                        {
                            let _ = reply.send(Err(
                                "job changed while payload retirement was being prepared".into(),
                            ));
                            return;
                        }
                        let record = owner.state.job_mut(job).unwrap();
                        record.status = JobStatus::Paused;
                        let file_ids = record.files.iter().map(|file| file.id).collect::<Vec<_>>();
                        owner.allocated_jobs.remove(&job);
                        let writers = owner.retiring_writers.entry(job).or_default();
                        for file in file_ids {
                            if let Some(writer) = owner.writers.remove(&file) {
                                writer.stop.cancel();
                                writers.push(writer);
                            }
                        }
                        let writers = writers.clone();
                        owner.attempts.retain(|r, _| r.job != job);
                        owner.pending_finalize.retain(|(j, _)| *j != job);
                        let generation = fresh_epoch();
                        owner.control_epochs.insert(job, generation);
                        owner.dirty = true;
                        let persistent = owner.persist;
                        owner.persist_then(move |owner, committed| {
                            let current = owner.control_epochs.get(&job) == Some(&generation)
                                && owner.job_epochs.get(&job) == Some(&scope_generation);
                            if (!persistent || committed) && current {
                                owner.publish_now();
                                let _ = reply.send(Ok(writers));
                            } else {
                                owner.dirty = true;
                                let _ =
                                    reply.send(Err("could not persist payload retirement".into()));
                            }
                        });
                    },
                );
            }
            QueueCommand::Delete {
                job,
                delete_files,
                reply,
            } => {
                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                let previous_removal_intent = self
                    .state
                    .job(job)
                    .and_then(|record| record.torrent.as_ref())
                    .and_then(|torrent| torrent.removal_intent);
                let torrent_command = self.state.job_mut(job).and_then(|record| {
                    let torrent = record.torrent.as_mut()?;
                    torrent.removal_intent = Some(TorrentRemovalIntent {
                        delete_data: delete_files,
                    });
                    Some(BackendCommand::Remove {
                        job,
                        delete_data: delete_files,
                        content_path: torrent.content_path.clone(),
                        files: torrent.files.clone(),
                        allowed_roots: self.torrent_payload_roots.clone(),
                    })
                });
                if let Some(command) = torrent_command {
                    self.dirty = true;
                    let generation = fresh_epoch();
                    self.control_epochs.insert(job, generation);
                    let persistent = self.persist;
                    self.persist_then(move |owner, committed| {
                        let current = owner.control_epochs.get(&job) == Some(&generation)
                            && owner.job_epochs.get(&job) == Some(&scope_generation);
                        let ok = (!persistent || committed) && current;
                        if ok {
                            if !owner.enqueue_backend_command(command) {
                                if let Some(torrent) = owner
                                    .state
                                    .job_mut(job)
                                    .and_then(|record| record.torrent.as_mut())
                                {
                                    torrent.removal_intent = previous_removal_intent;
                                }
                                let _ = reply.send(false);
                                return;
                            }
                            owner.bump_epoch();
                            owner.publish_now();
                        } else if current {
                            if let Some(torrent) = owner
                                .state
                                .job_mut(job)
                                .and_then(|record| record.torrent.as_mut())
                            {
                                torrent.removal_intent = previous_removal_intent;
                            }
                        }
                        let _ = reply.send(ok);
                    });
                    return;
                }
                self.delete_job_then(job, delete_files, move |_, ok| {
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::SetPriority {
                job,
                priority,
                reply,
            } => {
                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                let previous_priority = self
                    .state
                    .job(job)
                    .map(|record| record.priority)
                    .unwrap_or(priority);
                let torrent = match self.state.job_mut(job) {
                    Some(j) if j.kind == JobKind::Torrent && j.priority != priority => {
                        j.priority = priority;
                        true
                    }
                    _ => false,
                };
                let ok = if torrent {
                    self.dirty = true;
                    let generation = fresh_epoch();
                    self.control_epochs.insert(job, generation);
                    let persistent = self.persist;
                    self.persist_then(move |owner, committed| {
                        let current = owner.control_epochs.get(&job) == Some(&generation)
                            && owner.job_epochs.get(&job) == Some(&scope_generation);
                        let ok = (!persistent || committed) && current;
                        if ok {
                            if !owner.enqueue_backend_command(BackendCommand::SetPriority {
                                job,
                                priority,
                            }) {
                                if let Some(record) = owner.state.job_mut(job) {
                                    record.priority = previous_priority;
                                }
                                let _ = reply.send(false);
                                return;
                            }
                            owner.bump_epoch();
                            owner.publish_now();
                        } else if current {
                            if let Some(record) = owner.state.job_mut(job) {
                                record.priority = previous_priority;
                            }
                        }
                        let _ = reply.send(ok);
                    });
                    return;
                } else {
                    match self.state.job_mut(job) {
                        Some(j) => {
                            j.priority = priority;
                            self.dirty = true;
                            self.bump_epoch();
                            self.publish_now();
                            true
                        }
                        None => false,
                    }
                };
                let _ = reply.send(ok);
            }
            QueueCommand::SetKeepOnFailure { job, keep, reply } => {
                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                let before = self.state.job(job).cloned();
                let prior_keep = before.as_ref().is_some_and(|record| {
                    record
                        .params
                        .iter()
                        .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1")
                });
                let allowed = before.as_ref().is_some_and(|record| {
                    if matches!(record.status, JobStatus::Completed | JobStatus::Failed) {
                        true
                    } else {
                        record.status != JobStatus::Deleted && !record.held()
                    }
                });
                if !allowed {
                    let _ = reply.send(false);
                    return;
                }
                let generation = fresh_epoch();
                self.control_epochs.insert(job, generation);
                let artifacts = self.artifacts.clone();
                let receipts_before = self
                    .state
                    .url_admission_receipts
                    .iter()
                    .filter(|receipt| receipt.job_id == job.0)
                    .cloned()
                    .collect::<Vec<_>>();
                let mut refreshed_receipts = receipts_before.clone();
                let early_queue_job = before.as_ref().is_some_and(|record| {
                    matches!(
                        record.status,
                        JobStatus::Fetching | JobStatus::Queued | JobStatus::Paused
                    )
                });
                self.io_then(
                    move || {
                        let existing = artifacts.for_job(job.0).map_err(|e| nzbd_state::StateError::Corrupt(format!("artifact inventory: {e}")))?;
                        match existing {
                            Some(_) => {
                                let updated = artifacts.set_keep_on_failure(job.0, keep)
                                    .map_err(|e| nzbd_state::StateError::Corrupt(format!("artifact inventory: {e}")))?;
                                if updated {
                                    for receipt in &mut refreshed_receipts { refresh_url_admission_custody(receipt, &artifacts); }
                                }
                                Ok((updated, updated, refreshed_receipts))
                            },
                            None if early_queue_job => Ok((true, false, refreshed_receipts)),
                            None => Ok((false, false, refreshed_receipts)),
                        }
                    },
                    move |owner, inventory_result| {
                        let (valid, inventory_updated, refreshed_receipts) = match inventory_result {
                            Ok(value) => value,
                            Err(error) => {
                                tracing::warn!(job = job.0, %error, "could not update source Keep intent");
                                let _ = reply.send(false);
                                return;
                            }
                        };
                        if !valid {
                            let _ = reply.send(false);
                            return;
                        }
                        if owner.control_epochs.get(&job) != Some(&generation)
                            || owner.job_epochs.get(&job) != Some(&scope_generation)
                            || owner.state.job(job).is_none()
                        {
                            if inventory_updated {
                                let artifacts = owner.artifacts.clone();
                                owner.io_then(
                                    move || artifacts.set_keep_on_failure(job.0, prior_keep)
                                        .map(|_| ())
                                        .map_err(|error| nzbd_state::StateError::Corrupt(format!("artifact inventory rollback: {error}"))),
                                    move |_, result| {
                                        if let Err(error) = result {
                                            tracing::error!(job = job.0, %error, "could not roll back stale source Keep intent");
                                        }
                                        let _ = reply.send(false);
                                    },
                                );
                            } else {
                                let _ = reply.send(false);
                            }
                            return;
                        }
                        let Some(record) = owner.state.job_mut(job) else {
                            let _ = reply.send(false);
                            return;
                        };
                        record.params.retain(|(key, _)| key != "*Runner:KeepOnFailure");
                        if keep {
                            record.params.push(("*Runner:KeepOnFailure".into(), "1".into()));
                        }
                        for refreshed in refreshed_receipts {
                            if let Some(receipt) = owner.state.url_admission_receipts.iter_mut().find(|receipt|
                                receipt.request_token == refreshed.request_token
                                    && receipt.source_artifact_id == refreshed.source_artifact_id
                                    && receipt.source_generation == refreshed.source_generation) {
                                receipt.keep_source_on_failure = refreshed.keep_source_on_failure;
                                receipt.custody_state = refreshed.custody_state;
                                receipt.source_cleanup_status = refreshed.source_cleanup_status;
                            }
                        }
                        owner.dirty = true;
                        owner.persist_then(move |owner, committed| {
                            let current = owner.control_epochs.get(&job) == Some(&generation) && owner.job_epochs.get(&job) == Some(&scope_generation);
                            if (!owner.persist || committed) && current {
                                owner.dirty = false;
                                owner.bump_epoch();
                                owner.publish_now();
                                let _ = reply.send(true);
                            } else {
                                if current {
                                    if let Some(record) = owner.state.job_mut(job) {
                                        record.params.retain(|(key, _)| key != "*Runner:KeepOnFailure");
                                        if prior_keep {
                                            record.params.push(("*Runner:KeepOnFailure".into(), "1".into()));
                                        }
                                    }
                                    for before in &receipts_before {
                                        if let Some(receipt) = owner.state.url_admission_receipts.iter_mut().find(|receipt|
                                            receipt.request_token == before.request_token
                                                && receipt.source_artifact_id == before.source_artifact_id
                                                && receipt.source_generation == before.source_generation) {
                                            receipt.keep_source_on_failure = before.keep_source_on_failure;
                                            receipt.custody_state = before.custody_state.clone();
                                            receipt.source_cleanup_status = before.source_cleanup_status.clone();
                                        }
                                    }
                                    owner.dirty = true;
                                }
                                if inventory_updated {
                                    let artifacts = owner.artifacts.clone();
                                    owner.io_then(
                                        move || artifacts.set_keep_on_failure(job.0, prior_keep)
                                            .map(|_| ())
                                            .map_err(|e| nzbd_state::StateError::Corrupt(format!("artifact inventory rollback: {e}"))),
                                        move |_, result| {
                                            if let Err(error) = result {
                                                tracing::error!(job = job.0, %error, "could not roll back source Keep intent");
                                            }
                                            let _ = reply.send(false);
                                        },
                                    );
                                } else {
                                    let _ = reply.send(false);
                                }
                            }
                        });
                    },
                );
            }
            QueueCommand::SetCategory {
                job,
                category,
                reply,
            } => {
                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                let before_category = self.state.job(job).map(|record| record.category.clone());
                let ok = self.state.job_mut(job).is_some_and(|record| {
                    record.category = category;
                    true
                });
                if !ok {
                    let _ = reply.send(false);
                    return;
                }
                let generation = fresh_epoch();
                self.control_epochs.insert(job, generation);
                let persistent = self.persist;
                self.persist_then(move |owner, committed| {
                    let current = owner.control_epochs.get(&job) == Some(&generation)
                        && owner.job_epochs.get(&job) == Some(&scope_generation);
                    let ok = (!persistent || committed) && current;
                    if ok {
                        owner.publish_now();
                        owner.bump_epoch();
                    } else if current {
                        if let (Some(category), Some(record)) =
                            (before_category, owner.state.job_mut(job))
                        {
                            record.category = category;
                        }
                    }
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::SetTorrentSeedPolicy { job, policy, reply } => {
                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                if policy
                    .ratio_limit
                    .is_some_and(|v| !v.is_finite() || v <= 0.0)
                    || policy.time_limit_secs == Some(0)
                {
                    let _ = reply.send(false);
                    return;
                }
                let before_policy = self
                    .state
                    .job(job)
                    .and_then(|record| record.torrent.as_ref())
                    .map(|torrent| torrent.seed_policy);
                let ok = self.state.job_mut(job).is_some_and(|record| {
                    let Some(torrent) = record.torrent.as_mut() else {
                        return false;
                    };
                    torrent.seed_policy = policy;
                    true
                });
                if !ok {
                    let _ = reply.send(false);
                    return;
                }
                self.dirty = true;
                let generation = fresh_epoch();
                self.control_epochs.insert(job, generation);
                let persistent = self.persist;
                self.persist_then(move |owner, committed| {
                    let current = owner.control_epochs.get(&job) == Some(&generation)
                        && owner.job_epochs.get(&job) == Some(&scope_generation);
                    let ok = (!persistent || committed) && current;
                    if ok {
                        owner.update_seed_policies(unix_now());
                        owner.publish_now();
                        owner.bump_epoch();
                    } else if current {
                        if let (Some(policy), Some(torrent)) = (
                            before_policy,
                            owner
                                .state
                                .job_mut(job)
                                .and_then(|record| record.torrent.as_mut()),
                        ) {
                            torrent.seed_policy = policy;
                        }
                    }
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::Move { job, op, reply } => {
                let ok = self.move_job(job, op);
                let _ = reply.send(ok);
            }
            QueueCommand::PauseAll { source, reply } => {
                // Always log WHO, at info: "the queue keeps unpausing
                // itself" is always some client sending these — make the
                // culprit readable straight from `docker logs`.
                tracing::info!(
                    %source,
                    was_paused = self.state.download_paused,
                    "queue pause requested"
                );
                self.state.download_paused = true;
                self.dirty = true;
                self.emit(Event::QueuePauseChanged {
                    paused: true,
                    source,
                });
                self.publish_now();
                let _ = reply.send(());
            }
            QueueCommand::ResumeAll { source, reply } => {
                tracing::info!(
                    %source,
                    was_paused = self.state.download_paused,
                    "queue resume requested"
                );
                self.state.download_paused = false;
                // An operator resuming the queue is the operator override
                // for the out-of-space latch: they have seen the banner.
                self.clear_enospc_latch();
                self.dirty = true;
                self.emit(Event::QueuePauseChanged {
                    paused: false,
                    source,
                });
                self.bump_epoch();
                self.publish_now();
                let _ = reply.send(());
            }
            QueueCommand::SetSpeedLimit {
                bytes_per_sec,
                reply,
            } => {
                self.state.speed_limit_bps = bytes_per_sec;
                self.applied_usenet_limit = None;
                self.applied_torrent_limit = None;
                self.rebalance_download_budget(self.shared.load().download_rate_bps);
                self.dirty = true;
                self.emit(Event::SpeedLimitChanged { bytes_per_sec });
                self.publish_now();
                let _ = reply.send(());
            }
            QueueCommand::SetTorrentUploadLimit {
                bytes_per_sec,
                reply,
            } => {
                self.enqueue_backend_command(BackendCommand::SetUploadLimit { bytes_per_sec });
                let _ = reply.send(());
            }
            QueueCommand::SetMaxActiveDownloads { n, reply } => {
                let n = crate::queue::clamp_active_downloads(n);
                self.state.max_active_downloads = n;
                self.dirty = true;
                // Lowering the cap leaves jobs labelled Downloading that
                // are no longer in the set; settle them now rather than
                // up to a second later, so the queue redraws once with
                // the right answer instead of twice.
                self.settle_download_labels();
                self.emit(Event::MaxActiveDownloadsChanged { n });
                // Wake the parked connection tasks: raising the cap makes
                // work available that the last scan said did not exist,
                // and nothing else would tell them for up to `idle_hold`.
                self.bump_epoch();
                self.publish_now();
                let _ = reply.send(n);
            }
            QueueCommand::ImportJob {
                job,
                fold_journals,
                emit_finished,
                reply,
            } => {
                self.replace_job_then(
                    *job,
                    fold_journals,
                    emit_finished,
                    false,
                    false,
                    move |_, result| {
                        let _ = reply.send(result);
                    },
                );
            }
            QueueCommand::ImportJobIfPresent {
                job,
                require_snapshot,
                reply,
            } => {
                self.replace_job_then(
                    *job,
                    false,
                    false,
                    true,
                    require_snapshot,
                    move |_, result| {
                        let _ = reply.send(result.is_ok());
                    },
                );
            }
            QueueCommand::ExportJob { job, reply } => {
                let replacement_pending =
                    self.deferred_replacements
                        .iter()
                        .any(|command| match command {
                            QueueCommand::ImportJob { job: pending, .. }
                            | QueueCommand::ImportJobIfPresent { job: pending, .. } => {
                                pending.id == job
                            }
                            QueueCommand::RemoveJobSilent { job: pending, .. } => *pending == job,
                            QueueCommand::AdoptReplicatedAuthority { .. } => true,
                            _ => false,
                        });
                let exportable = !self.verification.contains_key(&job)
                    && !self.retiring_writers.contains_key(&job)
                    && !self.pending_control_revisions.contains_key(&job)
                    && !replacement_pending;
                let result = if exportable {
                    self.state.job(job).cloned().map(Box::new)
                } else {
                    None
                };
                let _ = reply.send(result);
            }
            QueueCommand::RemoveJobSilent { job, reply } => {
                let _ = reply.send(self.remove_job_silent(job));
            }
            QueueCommand::SetDelegated { job, node, reply } => {
                let replacement_pending =
                    self.deferred_replacements
                        .iter()
                        .any(|command| match command {
                            QueueCommand::ImportJob { job: pending, .. }
                            | QueueCommand::ImportJobIfPresent { job: pending, .. } => {
                                pending.id == job
                            }
                            QueueCommand::RemoveJobSilent { job: pending, .. } => *pending == job,
                            QueueCommand::AdoptReplicatedAuthority { .. } => true,
                            _ => false,
                        });
                let ok = self.state.job(job).is_some()
                    && (node.is_none()
                        || (!self.verification.contains_key(&job)
                            && !self.retiring_writers.contains_key(&job)
                            && !self.pending_control_revisions.contains_key(&job)
                            && !replacement_pending));
                if ok {
                    match &node {
                        Some(n) => {
                            if self.delegated.get(&job) != Some(n) {
                                self.mirror.remove(&job);
                            }
                            self.delegated.insert(job, n.clone());
                        }
                        None => {
                            self.delegated.remove(&job);
                            self.mirror.remove(&job);
                            self.post_fetch_files.remove(&job);
                        }
                    }
                    self.emit(Event::JobAssigned {
                        job,
                        node: node.clone(),
                    });
                    self.bump_epoch();
                    self.publish_now();
                }
                let _ = reply.send(ok);
            }
            QueueCommand::MirrorProgress { job, node, stats } => {
                if self
                    .delegated
                    .get(&job)
                    .is_some_and(|current| node.as_ref().is_none_or(|node| node == current))
                {
                    self.mirror.insert(job, stats);
                    self.publish_now();
                }
            }
            QueueCommand::FoldJobJournals { job, reply } => {
                self.fold_job_journals_then(job, move |_, result| {
                    if let Err(error) = result {
                        tracing::warn!(job = job.0, %error, "journal fold did not complete durably");
                    }
                    let _ = reply.send(());
                });
            }
            QueueCommand::SetServerBudgets { budgets, reply } => {
                tracing::info!(?budgets, "connection budgets updated");
                self.cluster_budgets = budgets;
                let applied = self.publish_conn_budgets();
                let _ = reply.send((self.budget_generation, applied));
            }
            QueueCommand::SetDownloadEnabled { enabled, reply } => {
                self.download_enabled = enabled;
                self.bump_epoch();
                self.publish_now();
                let _ = reply.send(());
            }
            QueueCommand::SetServerConnectionCaps { caps, reply } => {
                // Clamp to what was actually spawned at boot: there are
                // `max_connections` tasks per server and no more, so a
                // higher number here would be a promise nothing keeps.
                // The caller is told the effective values, so the UI can
                // say "restart to go above this" instead of showing a
                // figure the daemon is quietly ignoring.
                let mut applied = HashMap::new();
                for (id, want) in caps {
                    let ceiling = self
                        .servers
                        .iter()
                        .find(|s| s.id == id)
                        .map(|s| s.max_connections)
                        .unwrap_or(0);
                    if ceiling == 0 {
                        continue;
                    }
                    applied.insert(id, want.clamp(1, ceiling));
                }
                tracing::info!(?applied, "connection counts updated");
                self.user_conn_caps = applied.clone();
                self.publish_conn_budgets();
                let _ = reply.send(applied);
            }
            QueueCommand::AdoptAuthority { reply } => {
                self.adopt_authority_then(move |_, result| {
                    let _ = reply.send(result);
                });
            }
            QueueCommand::AdoptReplicatedAuthority { jobs, reply } => {
                self.replace_projection_then(jobs, move |_, result| {
                    let _ = reply.send(result);
                });
            }
            QueueCommand::AbandonRelocation {
                operation,
                revision,
                generation,
                reply,
            } => {
                let artifacts = self.artifacts.clone();
                let job_facts = self
                    .state
                    .jobs
                    .iter()
                    .map(|job| {
                        let held_move = job.control().is_some_and(|control| {
                            control.lifecycle == "held" && control.stage == "move"
                        });
                        let quiescent = job.files.iter().all(|file| {
                            self.writers
                                .get(&file.id)
                                .is_none_or(|writer| *writer.stopped.borrow())
                        });
                        (
                            job.id,
                            (
                                self.retiring_writers.contains_key(&job.id),
                                held_move,
                                quiescent,
                            ),
                        )
                    })
                    .collect::<HashMap<_, _>>();
                let lookup = artifacts.clone();
                let lookup_operation = operation.clone();
                self.io_then(move || {
                    let operation = lookup.operation(&lookup_operation)
                        .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?;
                    lookup.get(&operation.artifact)
                        .map(|artifact| artifact.root)
                        .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))
                }, move |owner, root| {
                    let root = match root {
                        Ok(root) => root,
                        Err(error) => {let _ = reply.send(Err(error.to_string())); return;}
                    };
                    owner.payload_then(root, move || {
                        let result = (|| {
                            let op = artifacts
                                .operation(&operation)
                                .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?;
                            let artifact = artifacts
                                .get(&op.artifact)
                                .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?;
                            if let Some(job) = artifact.job.map(JobId) {
                                if let Some((retiring, held_move, quiescent)) = job_facts.get(&job) {
                                    if *retiring {
                                        return Err(nzbd_state::StateError::Corrupt(
                                            "payload retirement is in progress".into(),
                                        ));
                                    }
                                    if !held_move || !quiescent {
                                        return Err(nzbd_state::StateError::Corrupt(
                                            "only a quiescent post-processing move hold can be abandoned while the job is queued".into(),
                                        ));
                                    }
                                }
                            }
                            artifacts
                                .abandon_relocation(&operation, revision, &generation)
                                .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))
                        })();
                        result
                    },
                    move |_, result| {
                        let result = result.map_err(|error| error.to_string());
                        let _ = reply.send(result);
                    },
                    );
                });
            }
            QueueCommand::RetryHold {
                job,
                revision,
                by,
                custody_checked,
                reply,
            } => {
                let publication_repair = self.state.job(job).is_some_and(|record| {
                    record
                        .params
                        .iter()
                        .any(|(key, _)| key == "*Recovery:publication-repair")
                });
                if publication_repair {
                    self.retry_publication_repair_then(job, revision, by, move |_, result| {
                        let _ = reply.send(result);
                    });
                } else {
                    let _ = reply.send(self.retry_hold(job, &revision, by, custody_checked));
                }
            }
            QueueCommand::RescheduleHold {
                job,
                revision,
                reason,
                reply,
            } => {
                let _ = reply.send(self.reschedule_hold(job, &revision, &reason));
            }
            QueueCommand::BeginResourceProbe {
                job,
                revision,
                by,
                reply,
            } => {
                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                let quiescent = self.state.job(job).is_some_and(|j| {
                    j.files
                        .iter()
                        .all(|f| self.writers.get(&f.id).is_none_or(|w| *w.stopped.borrow()))
                });
                let now = unix_now();
                let mut accepted = false;
                if quiescent {
                    if let Some(j) = self.state.job_mut(job) {
                        let current = j.control();
                        let after = j
                            .params
                            .iter()
                            .find(|(k, _)| k == "*Control:probe_after")
                            .and_then(|(_, v)| v.parse::<i64>().ok())
                            .unwrap_or(0);
                        let count = j
                            .params
                            .iter()
                            .find(|(k, _)| k == "*Control:attempts")
                            .and_then(|(_, v)| v.parse::<u32>().ok())
                            .unwrap_or(0);
                        if current.is_some_and(|c| {
                            c.revision == revision
                                && c.lifecycle == "held"
                                && matches!(c.cause.as_str(), "capacity" | "quota")
                        }) && (after <= now || by == RetryBy::Operator)
                        {
                            // The spacing paces the recovery task; a person
                            // asking is the health probe's to answer, not
                            // the backoff's.
                            j.params.retain(|(k, _)| {
                                k != "*Control:probe_after" && k != "*Control:attempts"
                            });
                            j.params.push((
                                "*Control:probe_after".into(),
                                (now + 30 * (1i64 << count.min(6))).to_string(),
                            ));
                            j.params
                                .push(("*Control:attempts".into(), (count + 1).to_string()));
                            self.dirty = true;
                            accepted = true;
                        }
                    }
                }
                if !accepted {
                    let _ = reply.send(false);
                    return;
                }
                let generation = fresh_epoch();
                self.control_epochs.insert(job, generation);
                let persistent = self.persist;
                self.persist_then(move |owner, committed| {
                    let current = owner.control_epochs.get(&job) == Some(&generation)
                        && owner.job_epochs.get(&job) == Some(&scope_generation);
                    let ok = (!persistent || committed) && current;
                    if ok {
                        owner.bump_epoch();
                        owner.publish_now();
                    } else if current {
                        owner.dirty = true;
                        owner.hold_job(
                            job,
                            nzbd_types::HoldCause::StorageUnavailable,
                            "snapshot",
                            "Resource probe request could not be persisted",
                        );
                    }
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::ReleaseResourceHold {
                job,
                revision,
                by,
                reply,
            } => {
                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                let mut changed = None;
                if let Some(j) = self.state.job_mut(job) {
                    if let Some(mut control) = j.control().filter(|c| {
                        c.revision == revision
                            && c.lifecycle == "held"
                            && matches!(c.cause.as_str(), "capacity" | "quota" | "allocation")
                    }) {
                        if let Some(next) = control
                            .revision
                            .parse::<u64>()
                            .ok()
                            .and_then(|r| r.checked_add(1))
                        {
                            control.revision = next.to_string();
                            control.lifecycle = "running".into();
                            control.message = if control.cause == "allocation" {
                                "Allocation retry requested; checking this job again"
                            } else {
                                "Storage admitted; resuming this job"
                            }
                            .into();
                            control.next_retry_at_unix = None;
                            control.released_at_unix = Some(unix_now());
                            // An operator retry runs the job, as every other
                            // hold's does; only the automatic path keeps a
                            // pause the operator set before the fault.
                            if by == RetryBy::Operator {
                                control.manual_pause = false;
                                control.retry_attempt = 0;
                            }
                            // The probe's spacing belongs to this episode.
                            j.params.retain(|(k, _)| {
                                k != "*Control:probe_after" && k != "*Control:attempts"
                            });
                            j.status = if control.manual_pause {
                                JobStatus::Paused
                            } else {
                                match control.previous_status {
                                    Some(
                                        JobStatus::Post { .. }
                                        | JobStatus::PostQueued
                                        | JobStatus::Completed,
                                    ) => JobStatus::PostQueued,
                                    _ => JobStatus::Queued,
                                }
                            };
                            j.set_control(&control);
                            for f in &j.files {
                                self.writers.remove(&f.id);
                            }
                            changed = Some(control);
                        }
                    }
                }
                self.dirty |= changed.is_some();
                if changed.is_none() {
                    let _ = reply.send(false);
                    return;
                }
                let control = changed.unwrap();
                let generation = fresh_epoch();
                self.control_epochs.insert(job, generation);
                let persistent = self.persist;
                self.persist_then(move |owner, committed| {
                    let current = owner.control_epochs.get(&job) == Some(&generation)
                        && owner.job_epochs.get(&job) == Some(&scope_generation);
                    let ok = (!persistent || committed) && current;
                    if ok {
                        owner.bump_epoch();
                        owner.publish_now();
                        owner.emit(Event::JobControlChanged { job, control });
                        let files: Vec<_> = owner
                            .state
                            .job(job)
                            .into_iter()
                            .flat_map(|record| record.files.iter().map(|file| file.id))
                            .collect();
                        for file in files {
                            owner.after_file_change(job, file);
                        }
                    } else if current {
                        owner.dirty = true;
                        owner.hold_job(
                            job,
                            nzbd_types::HoldCause::StorageUnavailable,
                            "snapshot",
                            "Resource hold release could not be persisted",
                        );
                    }
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::HoldJob {
                job,
                cause,
                stage,
                message,
                reply,
            } => {
                let committed = self.hold_job(job, cause, &stage, &message);
                let _ = reply.send(committed);
            }
            QueueCommand::SetJobStatus { job, status, reply } => {
                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                if self.state.job(job).is_some_and(|j| j.held()) {
                    let _ = reply.send(false);
                    return;
                }
                if self.retiring_writers.contains_key(&job) {
                    let _ = reply.send(false);
                    return;
                }
                let before_status = self.state.job(job).map(|record| record.status);
                let ok = match self.state.job_mut(job) {
                    Some(j) => {
                        j.status = status;
                        true
                    }
                    None => false,
                };
                if !ok {
                    let _ = reply.send(false);
                    return;
                }
                self.dirty = true;
                if self.persist
                    && matches!(
                        status,
                        JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
                    )
                {
                    let generation = fresh_epoch();
                    self.control_epochs.insert(job, generation);
                    self.persist_then(move |owner, committed| {
                        let current = owner.control_epochs.get(&job) == Some(&generation)
                            && owner.job_epochs.get(&job) == Some(&scope_generation);
                        let accepted = committed && current;
                        if accepted {
                            owner.publish_now();
                        } else if current {
                            if let (Some(status), Some(record)) =
                                (before_status, owner.state.job_mut(job))
                            {
                                record.status = status;
                            }
                            owner.dirty = true;
                        }
                        let _ = reply.send(accepted);
                    });
                } else {
                    self.publish_now();
                    let _ = reply.send(true);
                }
            }
            QueueCommand::RegisterRepairAttempt {
                job,
                attempt,
                reply,
            } => {
                let ok = self.state.job(job).is_some_and(|job| {
                    !job.held()
                        && !job.ready()
                        && !job
                            .params
                            .iter()
                            .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
                });
                if ok {
                    self.repair_progress.insert(job, (attempt, None));
                }
                let _ = reply.send(ok);
            }
            QueueCommand::RepairProgress { job, progress } => {
                if let Some((attempt, current)) = self.repair_progress.get_mut(&job) {
                    if *attempt == progress.attempt_id {
                        let changed = current
                            .as_ref()
                            .is_none_or(|old| old.phase != progress.phase);
                        *current = Some(progress);
                        if changed {
                            self.publish_now();
                        }
                    }
                }
            }
            QueueCommand::CloseRepairAttempt { job, attempt } => {
                if self
                    .repair_progress
                    .get(&job)
                    .is_some_and(|(current, _)| *current == attempt)
                {
                    self.repair_progress.remove(&job);
                    self.publish_now();
                }
            }
            QueueCommand::EnterPostStage {
                job,
                stage,
                at_unix,
                prev_ms,
                reply,
            } => {
                let ok = match self.state.job_mut(job) {
                    Some(j) if !j.held() => {
                        close_span(j, prev_ms, at_unix);
                        j.stages.push(StageSpan {
                            stage,
                            started_at_unix: at_unix,
                            ms: None,
                        });
                        // Status and timeline move together, under the
                        // owner task's single mutable borrow. That is what
                        // makes "one stage at a time" structural: a caller
                        // cannot enter a stage without leaving the previous
                        // one, because it never gets to do only half of it.
                        j.status = JobStatus::Post { stage };
                        true
                    }
                    _ => false,
                };
                if ok {
                    self.dirty = true;
                    self.publish_now();
                }
                let _ = reply.send(ok);
            }
            QueueCommand::ClosePostStage { job, at_unix, ms } => {
                if let Some(j) = self.state.job_mut(job) {
                    close_span(j, ms, at_unix);
                    self.dirty = true;
                    self.publish_now();
                }
            }
            QueueCommand::UnpauseParBlocks {
                job,
                blocks,
                block_size,
                reply,
            } => {
                let unpaused = self.unpause_par_blocks(job, blocks, block_size);
                let _ = reply.send(unpaused);
            }
            QueueCommand::RetainJobs { keep, reply } => {
                self.persist = false;
                let keep: HashSet<JobId> = keep.into_iter().collect();
                let drop_ids: Vec<JobId> = self
                    .state
                    .jobs
                    .iter()
                    .map(|j| j.id)
                    .filter(|id| !keep.contains(id))
                    .collect();
                for id in drop_ids {
                    let (reply, _) = oneshot::channel();
                    self.on_command(QueueCommand::RemoveJobSilent { job: id, reply });
                }
                self.delegated.clear();
                self.mirror.clear();
                self.post_fetch_files.clear();
                self.publish_now();
                self.bump_epoch();
                let _ = reply.send(());
            }
        }
    }

    // -- cluster: import / export / delegation / adoption --------------------

    #[cfg(test)]
    fn import_job(&mut self, job: Job, fold_journals: bool, emit_finished: bool) -> bool {
        self.replace_job_then(
            job,
            fold_journals,
            emit_finished,
            false,
            false,
            |_, result| {
                if let Err(error) = result {
                    tracing::warn!(%error,"job replacement was not committed");
                }
            },
        );
        true
    }

    fn remove_job_silent(&mut self, job: JobId) -> bool {
        if self.post_admissions.contains_key(&job)
            || self
                .verification
                .get(&job)
                .is_some_and(|state| state.running)
            || self.state.job(job).is_some_and(|record| {
                record.files.iter().any(|file| {
                    self.writers
                        .get(&file.id)
                        .is_some_and(|writer| !*writer.stopped.borrow())
                })
            })
        {
            if let Some(state) = self.verification.get(&job) {
                state.cancel.cancel();
            }
            self.retire_job_writers(job);
            return false;
        }
        let Some(index) = self.state.jobs.iter().position(|record| record.id == job) else {
            return false;
        };
        let record = self.state.jobs.remove(index);
        self.job_epochs.insert(job, fresh_epoch());
        if let Some(state) = self.verification.remove(&job) {
            state.cancel.cancel();
        }
        for file in &record.files {
            self.writer_epochs.remove(&file.id);
            self.pending_writer_birth.remove(&file.id);
            if let Some(writer) = self.writers.remove(&file.id) {
                self.retire_writer_handle(job, writer);
            }
            self.file_sizes.remove(&file.id);
            self.finalize_sent.remove(&file.id);
        }
        self.pending_name_inspections.remove(&job);
        self.allocated_jobs.remove(&job);
        self.allocation_cache.remove(&job);
        self.pending_finalize
            .retain(|(candidate, _)| *candidate != job);
        self.attempts.retain(|reference, _| reference.job != job);
        self.delegated.remove(&job);
        self.mirror.remove(&job);
        self.post_fetch_files.remove(&job);
        if !matches!(
            record.status,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
        ) {
            for receipt in &mut self.state.url_admission_receipts {
                if receipt.job_id == job.0 {
                    receipt.outcome = "deleted".into();
                }
            }
        }
        // Persist the retirement before clearing old journals or allowing a
        // same-ID import to open another descriptor at this namespace.
        let retirement = fresh_epoch();
        self.retirement_epochs.insert(job, retirement);
        self.retiring_writers.entry(job).or_default();
        self.dirty = true;
        self.persist_then(move |owner, committed| {
            if owner.retirement_epochs.get(&job) != Some(&retirement) {
                return;
            }
            if committed {
                owner.remove_journal(job);
                owner.retirement_epochs.remove(&job);
                owner.retiring_writers.remove(&job);
                owner.drain_replacements();
                owner.bump_epoch();
            } else {
                tracing::error!(
                    job = job.0,
                    "job generation retirement is not durable; replacement remains fenced"
                );
            }
            owner.publish_now();
        });
        self.publish_now();
        self.bump_epoch();
        true
    }

    fn fold_job_journals(&mut self, job: JobId) {
        self.fold_job_journals_then(job, |_, result| {
            if let Err(error) = result {
                tracing::warn!(%error,"journal fold deferred");
            }
        });
    }

    fn fold_job_journals_then(
        &mut self,
        job: JobId,
        callback: impl FnOnce(&mut Owner, Result<(), nzbd_state::StateError>) + Send + 'static,
    ) {
        let state_dir = self.state_dir.clone();
        let epoch = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
        let loading = self.pending_journal_loads.entry(job).or_insert((epoch, 0));
        if loading.0 != epoch {
            *loading = (epoch, 0);
        }
        loading.1 += 1;
        if let Some(state) = self.verification.get(&job) {
            state.cancel.cancel();
        }
        self.retire_job_writers(job);
        self.queue_verification(job);
        if let Some(state) = self.verification.get_mut(&job) {
            state.phase = "loading".into();
        }
        if let Some(record) = self.state.job_mut(job) {
            set_owner_param(
                &mut record.params,
                "*Recovery:journal-loading",
                Some("1".into()),
            );
        }
        self.io_then(
            move || JobJournals::replay_job_with_provenance(&state_dir, job),
            move |owner, result| {
                if let Some(loading) = owner
                    .pending_journal_loads
                    .get_mut(&job)
                    .filter(|loading| loading.0 == epoch)
                {
                    loading.1 -= 1;
                    if loading.1 == 0 {
                        owner.pending_journal_loads.remove(&job);
                    }
                }
                if owner.job_epochs.get(&job) != Some(&epoch) {
                    callback(
                        owner,
                        Err(nzbd_state::StateError::Corrupt(
                            "journal fold generation retired".into(),
                        )),
                    );
                    return;
                }
                let records = match result {
                    Ok(records) => records,
                    Err(error) => {
                        owner.hold_job(
                            job,
                            nzbd_types::HoldCause::StorageUnavailable,
                            "recovery",
                            &error.to_string(),
                        );
                        callback(owner, Err(error));
                        return;
                    }
                };
                for envelope in records {
                    let record = envelope.record;
                    if record.job != job
                        || !merge_replayed_proof(
                            &mut owner.state,
                            &record,
                            envelope.provenance.as_ref(),
                            &owner.artifacts.installation,
                        )
                    {
                        continue;
                    }
                    if record.file_size > 0 {
                        owner.file_sizes.insert(record.file, record.file_size);
                    }
                    let reference = SegRef {
                        job,
                        file: record.file,
                        seg_number: record.segment_number,
                    };
                    if let Some(segment) = owner.state.segment_mut(reference) {
                        if !matches!(segment.state, SegmentState::Done { .. }) {
                            segment.state = SegmentState::Done {
                                offset: record.offset,
                                len: record.len,
                                crc: record.crc32,
                            };
                        }
                    }
                }
                if let Some(record) = owner.state.job_mut(job) {
                    recompute_job_totals(record);
                    set_owner_param(&mut record.params, "*Recovery:journal-loading", None);
                }
                owner.queue_verification(job);
                owner.dirty = true;
                owner.publish_now();
                owner.bump_epoch();
                owner.persist_then(move |owner, ok| {
                    callback(
                        owner,
                        if ok {
                            Ok(())
                        } else {
                            Err(nzbd_state::StateError::Corrupt(
                                "journal recovery transition is not durable".into(),
                            ))
                        },
                    )
                });
            },
        );
    }

    #[cfg(test)]
    fn adopt_authority(&mut self) -> Result<(), nzbd_state::StateError> {
        self.adopt_authority_then(|_, result| {
            if let Err(error) = result {
                tracing::error!(%error,"authority adoption failed");
            }
        });
        Ok(())
    }

    fn adopt_authority_then(
        &mut self,
        callback: impl FnOnce(&mut Owner, Result<(), nzbd_state::StateError>) + Send + 'static,
    ) {
        let state_dir = self.state_dir.clone();
        self.io_then(
            move || SnapshotStore::open(&state_dir)?.load(),
            move |owner, result| {
                let recovered = match result
                    .and_then(|doc| doc.map(QueueState::from_runtime_doc).transpose())
                {
                    Ok(state) => state,
                    Err(error) => {
                        owner.persist = false;
                        callback(owner, Err(error));
                        return;
                    }
                };
                let mut jobs = owner.state.jobs.clone();
                if let Some(state) = recovered {
                    owner.state.next_job_id = owner.state.next_job_id.max(state.next_job_id);
                    owner.state.next_file_id = owner.state.next_file_id.max(state.next_file_id);
                    owner.state.download_paused = state.download_paused;
                    for job in state.jobs {
                        if !jobs.iter().any(|current| current.id == job.id) {
                            jobs.push(job);
                        }
                    }
                }
                owner.persist = true;
                owner.replace_projection_then(jobs, callback);
            },
        );
    }

    // -- scheduling ----------------------------------------------------------

    fn grant_work(&mut self, server_id: ServerId, max: usize) -> Vec<Lease> {
        if self.cancel.is_cancelled() || self.disk_low || self.is_blocked(server_id) {
            return Vec::new();
        }
        let servers = self.servers.clone();
        let Some(server) = servers.iter().find(|s| s.id == server_id) else {
            return Vec::new();
        };
        let now = Instant::now();
        let blocked_now: HashSet<ServerId> = self
            .blocked
            .iter()
            .filter(|(_, until)| **until > now)
            .map(|(id, _)| *id)
            .collect();
        let is_blocked = move |id: ServerId| blocked_now.contains(&id);

        let mut excluded = self.delegated.clone();
        for job in self
            .verification
            .keys()
            .chain(self.retiring_writers.keys())
            .chain(self.pending_control_commits.iter())
            .chain(self.pending_control_revisions.keys())
            .chain(self.allocating_jobs.iter())
        {
            excluded.insert(*job, "verification/replacement".into());
        }
        let allowed_post = self
            .post_fetch_files
            .iter()
            .filter(|(job, _)| {
                !self.verification.contains_key(job)
                    && !self.retiring_writers.contains_key(job)
                    && !self.pending_control_commits.contains(job)
                    && !self.pending_control_revisions.contains_key(job)
                    && !self.allocating_jobs.contains(job)
            })
            .map(|(job, files)| (*job, files.clone()))
            .collect::<HashMap<_, _>>();
        for job in self.post_admissions.keys() {
            if !allowed_post.contains_key(job) {
                excluded.insert(*job, "post-processing custody".into());
            }
        }
        let mut leases = Vec::new();
        // A refused allocation removes one job from eligibility; it must not
        // consume the lease budget or prevent another eligible job progressing.
        for _ in 0..max.saturating_add(self.state.jobs.len()) {
            if leases.len() >= max {
                break;
            }
            let ladder = Ladder::new(&servers);
            let mut ctx = SelectionCtx {
                ladder: &ladder,
                attempts: &mut self.attempts,
                is_blocked: &is_blocked,
                delegated: &excluded,
                post_fetch_files: &allowed_post,
                regular_downloads: self.download_enabled,
                article_retries: self.tuning.article_retries,
                now_unix: unix_now(),
                propagation_delay_secs: self.tuning.propagation_delay.as_secs() as i64,
                soft_hold: self.quota_reached,
                rotate: self.rotate,
            };
            let result = next_for_server(&self.state, server, &mut ctx);
            let exhausted = result.exhausted;
            let lease = result.lease;
            for r in exhausted {
                self.fail_segment(r);
            }
            let Some(r) = lease else { break };

            if let Err(e) = self.ensure_allocation(r.job, false) {
                if self.retiring_writers.contains_key(&r.job)
                    || self.allocating_jobs.contains(&r.job)
                {
                    // Writer retirement owns its own completion/wake-up. It is
                    // transient, not an allocation needing operator review.
                    // Exclude just this job for the rest of this bounded pass.
                    excluded.insert(r.job, "allocation/retirement pending".into());
                    continue;
                }
                let message = format!("payload allocation: {e}");
                self.hold_job(
                    r.job,
                    allocation_hold_cause(&message),
                    "download_write",
                    &message,
                );
                continue;
            }
            let writer = self.writer_for(r.job, r.file);
            let Some(generation) = self.writers.get(&r.file).map(|writer| writer.generation) else {
                continue;
            };
            let Some(seg) = self.state.segment_mut(r) else {
                break;
            };
            seg.state = SegmentState::Leased { server: server_id };
            let message_id = seg.message_id.to_string();
            if let Some(job) = self.state.job_mut(r.job) {
                if matches!(job.status, JobStatus::Queued) {
                    job.status = JobStatus::Downloading;
                }
            }
            // Advance the cursor for the NEXT lease, whichever connection
            // asks for it. One counter for the whole owner, not one per
            // server: the active set is a queue-wide notion, and per-
            // server cursors would let two servers settle into lockstep
            // on the same job.
            self.rotate = self.rotate.wrapping_add(1);
            leases.push(Lease {
                r,
                message_id,
                writer,
                generation,
            });
        }
        leases
    }

    // -- outcomes ------------------------------------------------------------

    fn on_scoped_writer(&mut self, job: JobId, file: FileId, generation: u64, fact: WriterFact) {
        if self.writer_epochs.get(&file) != Some(&generation)
            || self.state.job(job).is_none()
            || self.verification.contains_key(&job)
        {
            return;
        }
        match fact {
            WriterFact::Written {
                seg_number,
                offset,
                len,
                crc,
                file_size,
                server,
                identity,
            } => {
                let reference = SegRef {
                    job,
                    file,
                    seg_number,
                };
                let Some(segment) = self.state.segment_mut(reference) else {
                    return;
                };
                if let SegmentState::Done {
                    offset: old,
                    len: old_len,
                    crc: old_crc,
                } = segment.state
                {
                    if (old, old_len, old_crc) != (offset, len, crc) {
                        self.hold_job(
                            job,
                            nzbd_types::HoldCause::IdentityConflict,
                            "download_write",
                            "writer acknowledgement contradicts completed logical segment",
                        );
                    }
                    return;
                }
                let Some(allocation) = self.allocation_cache.get(&job) else {
                    return;
                };
                let record = self.state.job(job).unwrap();
                let key = format!("*File:proof:{}", file.0);
                let file_generation = record
                    .params
                    .iter()
                    .find(|(key, _)| key == &format!("*File:generation:{}", file.0))
                    .map(|(_, value)| value.clone())
                    .unwrap_or_default();
                if file_generation != generation.to_string() {
                    return;
                }
                let mut proof = record
                    .params
                    .iter()
                    .find(|(candidate, _)| candidate == &key)
                    .and_then(|(_, value)| serde_json::from_str::<DurableFileProof>(value).ok())
                    .filter(|proof| {
                        proof.protocol == WRITER_PROTOCOL
                            && proof.generation == file_generation
                            && proof.allocation_generation == allocation.generation
                            && proof.installation == self.artifacts.installation
                            && proof.identity.same_object(&identity)
                    })
                    .unwrap_or_else(|| DurableFileProof {
                        protocol: WRITER_PROTOCOL,
                        generation: file_generation.clone(),
                        allocation_generation: allocation.generation.clone(),
                        installation: self.artifacts.installation.clone(),
                        identity: identity.clone(),
                        ranges: vec![],
                        publication_name: None,
                    });
                let range = ProvenRange {
                    seg_number,
                    offset,
                    len,
                    crc,
                };
                if proof
                    .ranges
                    .iter()
                    .any(|prior| prior.seg_number == seg_number || prior.overlaps(&range))
                {
                    self.hold_job(
                        job,
                        nzbd_types::HoldCause::IdentityConflict,
                        "download_write",
                        "writer acknowledgement overlaps durable evidence",
                    );
                    return;
                }
                proof.identity = identity;
                proof.ranges.push(range.clone());
                let mut journal_proof = proof.clone();
                journal_proof.ranges = vec![range];
                self.append_journal(
                    JournalRecord {
                        job,
                        file,
                        segment_number: seg_number,
                        offset,
                        len,
                        crc32: crc,
                        file_size,
                    },
                    serde_json::to_value(&journal_proof).ok(),
                );
                if let Some(segment) = self.state.segment_mut(reference) {
                    segment.state = SegmentState::Done { offset, len, crc };
                }
                if let Some(record) = self.state.job_mut(job) {
                    set_owner_param(&mut record.params, &key, serde_json::to_string(&proof).ok());
                    recompute_job_totals(record);
                }
                if file_size > 0 {
                    self.file_sizes.insert(file, file_size);
                }
                self.volumes.add(
                    server,
                    u64::from(len),
                    unix_now(),
                    self.tuning.quota_start_day,
                );
                self.attempts.remove(&reference);
                self.dirty = true;
                self.after_file_change(job, file);
            }
            WriterFact::Finalized {
                ok,
                final_path,
                combined_crc,
                identity,
            } => {
                if !ok {
                    self.on_writer_finalized(job, file, false, final_path, combined_crc);
                    return;
                }
                let proof_key = format!("*File:proof:{}", file.0);
                if let Some(record) = self.state.job_mut(job) {
                    if let Some(identity) = identity {
                        if let Some((_, raw)) =
                            record.params.iter_mut().find(|(key, _)| key == &proof_key)
                        {
                            if let Ok(mut proof) = serde_json::from_str::<DurableFileProof>(raw) {
                                proof.identity = identity;
                                proof.publication_name = final_path
                                    .as_ref()
                                    .and_then(|path| path.file_name())
                                    .map(|name| name.to_string_lossy().into_owned());
                                if let Ok(json) = serde_json::to_string(&proof) {
                                    *raw = json;
                                }
                            }
                        }
                    }
                    if let Some(entry) = record.files.iter_mut().find(|entry| entry.id == file) {
                        entry.finalized = true;
                        entry.crc32 = combined_crc;
                    }
                }
                self.dirty = true;
                self.persist_then(move |owner, committed| {
                    if owner.writer_epochs.get(&file) != Some(&generation)
                        || owner.state.job(job).is_none()
                    {
                        return;
                    }
                    if committed {
                        owner.on_writer_finalized(job, file, true, final_path, combined_crc);
                    } else {
                        if let Some(entry) = owner.state.file_mut(job, file) {
                            entry.finalized = false;
                        }
                        owner.hold_job(
                            job,
                            nzbd_types::HoldCause::StorageUnavailable,
                            "finalize",
                            "publication evidence could not be persisted",
                        );
                    }
                });
            }
            WriterFact::Error { error } => {
                if let Some(writer) = self.writers.remove(&file) {
                    self.retire_writer_handle(job, writer);
                }
                self.writer_epochs.remove(&file);
                self.on_msg(EngineMsg::WriterError { job, file, error });
            }
            WriterFact::Rejected {
                seg_number,
                server,
                reason,
            } => {
                tracing::warn!(job=job.0,file=file.0,seg_number,%reason,"malformed segment rejected before disk mutation");
                self.on_segment_failed(
                    SegRef {
                        job,
                        file,
                        seg_number,
                    },
                    server,
                    AttemptOutcome::Other,
                );
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_segment_written(
        &mut self,
        job: JobId,
        file: FileId,
        seg_number: u32,
        offset: u64,
        len: u32,
        crc: u32,
        file_size: u64,
        server: ServerId,
    ) {
        self.volumes
            .add(server, len as u64, unix_now(), self.tuning.quota_start_day);
        let r = SegRef {
            job,
            file,
            seg_number,
        };
        if file_size > 0 {
            self.file_sizes.insert(file, file_size);
        }
        let Some(seg) = self.state.segment_mut(r) else {
            return; // job deleted while the write was in flight
        };
        if matches!(seg.state, SegmentState::Done { .. }) {
            return; // duplicate (e.g. recovery overlap)
        }
        seg.state = SegmentState::Done { offset, len, crc };
        self.append_journal(
            JournalRecord {
                job,
                file,
                segment_number: seg_number,
                offset,
                len,
                crc32: crc,
                file_size,
            },
            None,
        );
        self.attempts.remove(&r);
        if let Some(j) = self.state.job_mut(job) {
            recompute_job_totals(j);
        }
        self.after_file_change(job, file);
    }

    fn on_segment_failed(&mut self, r: SegRef, server: ServerId, outcome: AttemptOutcome) {
        // Every failed attempt is a retry the wire pays for again — count
        // it per job so "header rate ≫ row progress" is explained on the
        // dashboard instead of looking like a stale page.
        *self.retry_counts.entry(r.job.0).or_insert(0) += 1;
        let servers = self.servers.clone();
        let ladder = Ladder::new(&servers);
        let att = self
            .attempts
            .entry(r)
            .or_insert_with(|| SegmentAttempt::new(self.tuning.article_retries));
        let verdict = ladder.on_outcome(att, server, outcome);

        let age_days = self
            .state
            .job(r.job)
            .and_then(|j| j.files.iter().find(|f| f.id == r.file))
            .and_then(|f| f.date)
            .map(|d| ((unix_now() - d).max(0) / 86_400) as u32);

        let exhausted = match verdict {
            Verdict::NextServer => {
                let att = self.attempts.get_mut(&r).unwrap();
                ladder.is_exhausted(att, age_days)
            }
            Verdict::Failed => true,
            _ => false,
        };

        // Release the lease back to pending.
        if let Some(seg) = self.state.segment_mut(r) {
            if matches!(seg.state, SegmentState::Leased { .. }) {
                seg.state = SegmentState::Pending;
            }
        }

        match verdict {
            Verdict::RetrySame { block_server } if block_server => self.block_server(server),
            _ => {}
        }
        if exhausted {
            self.fail_segment(r);
        }
        self.bump_epoch(); // work may now be eligible for other servers
    }

    fn fail_segment(&mut self, r: SegRef) {
        let Some(seg) = self.state.segment_mut(r) else {
            return;
        };
        if matches!(seg.state, SegmentState::Done { .. } | SegmentState::Failed) {
            return;
        }
        seg.state = SegmentState::Failed;
        self.attempts.remove(&r);
        if let Some(j) = self.state.job_mut(r.job) {
            recompute_job_totals(j);
        }
        tracing::debug!(
            job = r.job.0,
            file = r.file.0,
            seg = r.seg_number,
            "segment exhausted"
        );
        self.emit(Event::SegmentExhausted {
            job: r.job,
            file: r.file,
            segment: r.seg_number,
        });
        self.after_file_change(r.job, r.file);
        self.maybe_abort_unhealthy(r.job);
    }

    /// NZBGet-style critical-health abort: once enough articles have
    /// failed that the job can't be repaired even with every par2 block
    /// (`health < critical_health`), stop wasting bandwidth. Pending
    /// segments are failed outright; leased ones finish their in-flight
    /// attempt honestly. The job then completes as Failed through the
    /// normal path, and the PP health gate parks/deletes per policy.
    fn maybe_abort_unhealthy(&mut self, job_id: JobId) {
        if !self.tuning.health_abort {
            return;
        }
        let Some(job) = self.state.job(job_id) else {
            return;
        };
        if !matches!(
            job.status,
            JobStatus::Queued | JobStatus::Downloading | JobStatus::Paused
        ) {
            return;
        }
        let health = Health::calc(&job.totals);
        let critical = Health::calc_critical(&job.totals, true);
        if health.0 >= critical.0 {
            return;
        }
        let name = job.name.clone();
        let pending: Vec<(FileId, Vec<u32>)> = job
            .files
            .iter()
            .filter(|f| !f.is_terminal())
            .map(|f| {
                (
                    f.id,
                    f.segments
                        .iter()
                        .filter(|s| matches!(s.state, SegmentState::Pending))
                        .map(|s| s.number)
                        .collect(),
                )
            })
            .collect();
        tracing::warn!(
            job = job_id.0,
            %name,
            health = health.0,
            critical = critical.0,
            "aborting download: health below critical (unrepairable even with all par2)"
        );
        for (file, segs) in pending {
            for seg_number in segs {
                let r = SegRef {
                    job: job_id,
                    file,
                    seg_number,
                };
                if let Some(seg) = self.state.segment_mut(r) {
                    seg.state = SegmentState::Failed;
                }
                self.attempts.remove(&r);
            }
            if let Some(j) = self.state.job_mut(job_id) {
                recompute_job_totals(j);
            }
            self.after_file_change(job_id, file);
        }
        self.bump_epoch();
    }

    fn accept_file_metadata(&mut self, job: JobId, file: FileId, name: &str, size: u64) {
        if name.is_empty()
            || name.len() > 255
            || name.contains(['/', '\\', ':', '\0'])
            || name == "."
            || name == ".."
            || size == 0
        {
            self.hold_job(
                job,
                nzbd_types::HoldCause::IdentityConflict,
                "download_write",
                "unsafe or incomplete yEnc metadata",
            );
            return;
        }
        let Some(j) = self.state.job_mut(job) else {
            return;
        };
        let Some(index) = j.files.iter().position(|f| f.id == file) else {
            return;
        };
        let size_key = format!("*File:size:{}", file.0);
        let previous_size = j
            .params
            .iter()
            .find(|(k, _)| k == &size_key)
            .and_then(|(_, v)| v.parse::<u64>().ok());
        let name_key = format!("*File:name:{}", file.0);
        // Obfuscated multipart posts can carry a different yEnc name in
        // every article. The NZB file/segment mapping identifies the file;
        // keep the first safe name stable and require a consistent size.
        if previous_size.is_some_and(|s| s != size) {
            self.hold_job(
                job,
                nzbd_types::HoldCause::IdentityConflict,
                "download_write",
                "conflicting yEnc declared size",
            );
            return;
        }
        if previous_size.is_some() && j.files[index].filename_confirmed {
            return; // retain the first confirmed name, including collision suffixes
        }
        if previous_size.is_none() {
            j.params.push((size_key, size.to_string()));
            j.params.push((name_key, name.into()));
        }
        // Collision suffix precedes extension. The raw confirmed name is stored
        // separately from the output name.
        let mut chosen = name.to_string();
        if j.files.iter().any(|f| f.id != file && f.filename == chosen) {
            let p = Path::new(name);
            chosen = match p.extension() {
                Some(ext) => format!(
                    "{}-{}.{}",
                    p.file_stem().unwrap_or_default().to_string_lossy(),
                    file.0,
                    ext.to_string_lossy()
                ),
                None => format!("{name}-{}", file.0),
            };
        }
        j.files[index].filename = chosen;
        j.files[index].filename_confirmed = true;
        j.files[index].is_par2 = name.to_ascii_lowercase().ends_with(".par2");
        self.file_sizes.insert(file, size);
        self.dirty = true;
        // Name authority is committed before any Finalize can publish it.
        let epoch = self.job_epochs.get(&job).copied();
        self.persist_then(move |owner, ok| {
            if !ok && owner.job_epochs.get(&job).copied() == epoch {
                owner.hold_job(
                    job,
                    nzbd_types::HoldCause::StorageUnavailable,
                    "download_write",
                    "filename metadata could not be persisted",
                );
            }
        });
    }

    fn hold_job(
        &mut self,
        job: JobId,
        cause: nzbd_types::HoldCause,
        stage: &str,
        message: &str,
    ) -> bool {
        let cause_str = cause.as_str().to_string();
        let Some(j) = self.state.job_mut(job) else {
            return false;
        };
        if j.params.iter().any(|(k, _)| k == nzbd_types::PP_DONE_PARAM) {
            return false;
        }
        let prior = j.control();
        if prior.as_ref().is_some_and(|c| {
            c.lifecycle == "held" && !matches!(c.cause.as_str(), "capacity" | "quota")
        }) {
            return true;
        }
        if prior
            .as_ref()
            .is_some_and(|c| c.lifecycle == "held" && c.cause == cause_str && c.stage == stage)
        {
            return true;
        }
        let revision = prior
            .as_ref()
            .and_then(|c| c.revision.parse::<u64>().ok())
            .unwrap_or(0)
            .checked_add(1);
        let Some(revision) = revision else {
            return false;
        };
        // The recovery is part of the hold, decided by the cause and written
        // in the same save. A follow-up "now schedule it" write is lost
        // exactly when the state volume is what failed, and an import of the
        // row cannot carry it at all (`import_job` keeps a held fence).
        let policy = cause.policy();
        let now = unix_now();
        // Only a hold still in force speaks for where the job is now. A
        // released control keeps the status from its own hold, and reading
        // that back would route a post-processing failure to the download
        // queue (or the reverse) and skip the custody check.
        let prior_held = prior.as_ref().filter(|c| c.lifecycle == "held");
        let same_episode = prior_held.is_some()
            || prior.as_ref().is_some_and(|c| {
                c.released_at_unix
                    .is_some_and(|at| now.saturating_sub(at) < nzbd_types::HOLD_EPISODE_SECONDS)
            });
        let episode = prior.as_ref().filter(|_| same_episode);
        let attempts_made = episode.map_or(0, |c| c.retry_attempt);
        let timed = policy.timed();
        let control = nzbd_types::JobControl {
            version: 1,
            revision: revision.to_string(),
            lifecycle: "held".into(),
            cause: cause_str.clone(),
            stage: stage.into(),
            retry_policy: policy.as_str().into(),
            message: message.chars().take(2048).collect(),
            instance: prior
                .as_ref()
                .map(|c| c.instance.clone())
                .unwrap_or_else(|| {
                    format!(
                        "{:x}-{}",
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_nanos(),
                        job.0
                    )
                }),
            previous_status: prior_held
                .and_then(|c| c.previous_status)
                .or(Some(j.status)),
            manual_pause: prior_held.map_or(j.status == JobStatus::Paused, |c| c.manual_pause),
            retry_attempt: if timed {
                attempts_made.saturating_add(1)
            } else {
                attempts_made
            },
            next_retry_at_unix: timed
                .then(|| now.saturating_add(nzbd_types::hold_retry_delay_seconds(attempts_made))),
            blocked_since_unix: episode.and_then(|c| c.blocked_since_unix).or(Some(now)),
            released_at_unix: None,
        };
        j.set_control(&control);
        j.status = JobStatus::Paused;
        for f in &mut j.files {
            for s in &mut f.segments {
                if matches!(s.state, SegmentState::Leased { .. }) {
                    s.state = SegmentState::Pending;
                }
            }
        }
        for f in &j.files {
            if let Some(w) = self.writers.get(&f.id) {
                w.stop.cancel();
            }
            self.finalize_sent.remove(&f.id);
        }
        self.pending_finalize.retain(|(id, _)| *id != job);
        self.dirty = true;
        self.bump_epoch();
        // The local fence is immediate. Emit only the durably acknowledged
        // control revision; stale callbacks cannot describe a newer hold.
        let epoch = self.job_epochs.get(&job).copied();
        self.persist_then(move |owner, ok| {
            if ok
                && owner.job_epochs.get(&job).copied() == epoch
                && owner
                    .state
                    .job(job)
                    .and_then(Job::control)
                    .is_some_and(|current| current.revision == control.revision)
            {
                owner.publish_now();
                owner.emit(Event::JobControlChanged { job, control });
            }
        });
        self.publish_now();
        true
    }

    /// End a non-storage hold by sending the job back to the step it stopped
    /// in. Owner state only — no filesystem work runs here: a post-side hold
    /// requires `custody_checked`, which `EngineHandle::release_hold` earns
    /// by running `validate_post_retry` in `spawn_blocking` first, so a
    /// wedged mount can never stall this loop.
    ///
    /// Download side: back to `Queued`; leased segments were already returned
    /// to `Pending` by `hold_job`, journaled `Done` ranges stay. Post side:
    /// `PostQueued`, which the post scan claims through its own guards and
    /// runs from the beginning. Storage causes release through
    /// `ReleaseResourceHold` after their capacity probe instead.
    pub(crate) fn retry_hold(
        &mut self,
        job: JobId,
        revision: &str,
        by: RetryBy,
        custody_checked: bool,
    ) -> Result<(), String> {
        if by == RetryBy::Recovery && !self.persist {
            return Err("this node is not the queue authority".into());
        }
        let before = self
            .state
            .job(job)
            .cloned()
            .ok_or_else(|| "job not found".to_string())?;
        let mut control = before
            .control()
            .filter(|c| c.lifecycle == "held")
            .ok_or_else(|| "job is not held".to_string())?;
        if control.revision != revision {
            return Err("the hold changed since it was read; reload and retry".into());
        }
        if control.hold_cause().is_storage() {
            return Err("storage holds are released by the capacity check".into());
        }
        if before.torrent.is_some() {
            return Err("torrent holds are released from the torrent's own actions".into());
        }
        if before
            .params
            .iter()
            .any(|(k, _)| k == nzbd_types::PP_DONE_PARAM)
        {
            return Err("post-processing already recorded a result for this job".into());
        }
        if by == RetryBy::Recovery && control.manual_pause {
            return Err("paused by the operator".into());
        }
        let post_side = control.post_side();
        if post_side && !custody_checked {
            return Err("post-processing retry needs its payload custody checked first".into());
        }
        let quiescent = !self.retiring_writers.contains_key(&job)
            && before
                .files
                .iter()
                .all(|f| self.writers.get(&f.id).is_none_or(|w| *w.stopped.borrow()));
        if !quiescent {
            return Err("the job's writers are still stopping; retry in a moment".into());
        }
        let next = control
            .revision
            .parse::<u64>()
            .ok()
            .and_then(|r| r.checked_add(1))
            .ok_or_else(|| "hold revision is exhausted".to_string())?;
        control.revision = next.to_string();
        control.lifecycle = "running".into();
        control.next_retry_at_unix = None;
        control.released_at_unix = Some(unix_now());
        control.message = match by {
            RetryBy::Operator => format!(
                "Retry requested by the operator after {} at {}",
                control.cause, control.stage
            ),
            RetryBy::Recovery => format!(
                "Automatic retry {} after {} at {}",
                control.retry_attempt, control.cause, control.stage
            ),
        };
        if by == RetryBy::Operator {
            control.manual_pause = false;
            control.retry_attempt = 0;
        }
        {
            let j = self.state.job_mut(job).unwrap();
            j.status = if post_side {
                JobStatus::PostQueued
            } else {
                JobStatus::Queued
            };
            j.set_control(&control);
        }
        if !post_side {
            for f in &before.files {
                self.writers.remove(&f.id);
            }
        }
        self.dirty = true;
        self.pending_control_commits.insert(job);
        let epoch = self.job_epochs.get(&job).copied();
        self.persist_then(move |owner, ok| {
            if owner.job_epochs.get(&job).copied() != epoch {
                return;
            }
            owner.pending_control_commits.remove(&job);
            let still_current = owner
                .state
                .job(job)
                .and_then(Job::control)
                .is_some_and(|current| current.revision == control.revision);
            if !still_current {
                return;
            }
            if !ok {
                if let Some(current) = owner.state.job_mut(job) {
                    let paused = current.status == JobStatus::Paused;
                    current.set_control(&before.control().unwrap());
                    current.status = if paused {
                        JobStatus::Paused
                    } else {
                        before.status
                    };
                }
            } else {
                owner.emit(Event::JobControlChanged { job, control });
                if !post_side {
                    for file in before.files {
                        owner.after_file_change(job, file.id);
                    }
                }
            }
            owner.publish_now();
            owner.bump_epoch();
        });
        self.publish_now();
        Ok(())
    }

    /// Push an automatic hold's next attempt out on the same backoff, keeping
    /// the original reason and recording the refusal beside it. The new time
    /// lives in memory even if this save fails; the next snapshot carries it.
    fn reschedule_hold(&mut self, job: JobId, revision: &str, reason: &str) -> bool {
        if !self.persist {
            return false;
        }
        let Some(j) = self.state.job_mut(job) else {
            return false;
        };
        let Some(mut control) = j.control().filter(|c| {
            c.lifecycle == "held" && c.revision == revision && c.hold_cause().policy().automatic()
        }) else {
            return false;
        };
        // A timed hold moves its next attempt out; a re-observed one keeps
        // its cadence and only records why it is still held.
        let timed = control.hold_cause().policy().timed();
        if timed {
            let delay = nzbd_types::hold_retry_delay_seconds(control.retry_attempt);
            control.retry_attempt = control.retry_attempt.saturating_add(1);
            control.next_retry_at_unix = Some(unix_now().saturating_add(delay));
        }
        let original = control
            .message
            .split("; retry refused: ")
            .next()
            .unwrap_or("")
            .to_string();
        let message: String = format!("{original}; retry refused: {reason}")
            .chars()
            .take(2048)
            .collect();
        if !timed && message == control.message {
            return true; // nothing new to say; no save every pass
        }
        control.message = message;
        // A changed hold is a new revision, so a second actor that read the
        // old one (another pass, an operator click) cannot apply on top.
        let Some(next) = control
            .revision
            .parse::<u64>()
            .ok()
            .and_then(|r| r.checked_add(1))
        else {
            return false;
        };
        control.revision = next.to_string();
        j.set_control(&control);
        self.dirty = true;
        if self.persist {
            self.save_snapshot();
        }
        self.publish_now();
        self.emit(Event::JobControlChanged { job, control });
        true
    }

    // -- completion cascade --------------------------------------------------

    fn after_file_change(&mut self, job: JobId, file: FileId) {
        if self.pending_control_commits.contains(&job)
            || self.pending_control_revisions.contains_key(&job)
            || self.allocating_jobs.contains(&job)
            || self.cancel.is_cancelled()
            || self.verification.contains_key(&job)
            || self.retiring_writers.contains_key(&job)
        {
            return;
        }
        if self.state.job(job).is_some_and(|j| j.held()) {
            return;
        }
        let Some(f) = self.state.file_mut(job, file) else {
            return;
        };
        if !f.is_terminal() || f.finalized {
            self.check_job_complete(job);
            return;
        }
        if f.has_any_done() {
            if !self.finalize_sent.contains(&file) {
                self.send_finalize(job, file);
            }
        } else {
            // Nothing on disk; complete trivially.
            f.finalized = true;
            let filename = f.filename.clone();
            self.emit(Event::FileFinished {
                job,
                file,
                filename,
                ok: false,
            });
            self.dirty = true;
            self.check_job_complete(job);
        }
    }

    fn send_finalize(&mut self, job: JobId, file: FileId) {
        if self.pending_control_commits.contains(&job)
            || self.cancel.is_cancelled()
            || self.verification.contains_key(&job)
            || self.retiring_writers.contains_key(&job)
        {
            return;
        }
        let publication_key = format!("*File:publication:{}", file.0);
        let Some(record) = self.state.job(job) else {
            return;
        };
        let Some(entry) = record.files.iter().find(|entry| entry.id == file) else {
            return;
        };
        let filename = entry.filename.clone();
        if !record
            .params
            .iter()
            .any(|(key, value)| key == &publication_key && value == &filename)
        {
            let owner_epoch = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
            set_owner_param(
                &mut self.state.job_mut(job).unwrap().params,
                &publication_key,
                Some(filename),
            );
            self.dirty = true;
            self.persist_then(move |owner, ok| {
                if owner.job_epochs.get(&job) == Some(&owner_epoch) {
                    if ok {
                        owner.send_finalize(job, file);
                    } else {
                        owner.hold_job(
                            job,
                            nzbd_types::HoldCause::StorageUnavailable,
                            "finalize",
                            "publication intent could not be persisted",
                        );
                    }
                }
            });
            return;
        }
        let Some(f) = self.state.file_mut(job, file) else {
            return;
        };
        // Combined whole-file CRC when coverage is complete and contiguous.
        let mut segs: Vec<(u64, u32, u32)> = Vec::with_capacity(f.segments.len());
        let mut all_done = true;
        for s in &f.segments {
            match s.state {
                SegmentState::Done { offset, len, crc } => segs.push((offset, len, crc)),
                _ => all_done = false,
            }
        }
        segs.sort_by_key(|(off, _, _)| *off);
        let mut combined_crc = if all_done { combine_crcs(&segs) } else { None };

        // The yEnc-declared size, or nothing.
        //
        // This used to fall back to "the highest offset we actually wrote",
        // which finalize then applied with set_len — a silent truncation to
        // whatever happened to arrive, performed by the daemon itself, with
        // health and totals left untouched. It is reachable whenever
        // `file_sizes` has no entry for this file: the map is in-memory and
        // rebuilt from journal records that carry a size, so a restart or an
        // authority takeover mid-file lands squarely in it.
        //
        // A size we do not know is a size we must not invent. Zero means "do
        // not resize", which leaves the preallocated length alone rather than
        // cutting the file down to the part that made it.
        let file_size = self
            .file_sizes
            .get(&file)
            .copied()
            .filter(|s| *s > 0)
            .unwrap_or(0);

        let coverage: u64 = segs.iter().map(|(_, len, _)| *len as u64).sum();
        // Segment leases seal private sparse checkpoints. Their authenticated
        // scope is not media completeness; the cluster assembler validates the
        // union before normal publication. Authority mode cannot use this lane.
        let range_checkpoint = !self.persist
            && all_done
            && file_size > 0
            && self.state.job(job).is_some_and(|j| {
                transitions::imported_payload_plan(j, &self.dest_dir)
                    .is_ok_and(|plan| plan.private_range)
            });
        if range_checkpoint {
            combined_crc = None;
        }
        // Missing articles and holes in an NZB are repair inputs, not identity
        // conflicts. Reject unknown sizes and overlapping/out-of-bounds ranges.
        let mut end = 0;
        let valid_ranges = file_size > 0
            && segs.iter().all(|(offset, len, _)| {
                let valid = *offset >= end && *len > 0;
                end = offset.checked_add(u64::from(*len)).unwrap_or(u64::MAX);
                valid && end <= file_size
            });
        if !range_checkpoint && !valid_ranges {
            self.hold_job(
                job,
                nzbd_types::HoldCause::IdentityConflict,
                "finalize",
                "file coverage or expected size is unverified; partial retained",
            );
            return;
        }
        if !range_checkpoint && (combined_crc.is_none() || coverage != file_size) {
            let key = format!("*File:repair:{}", file.0);
            let j = self.state.job_mut(job).unwrap();
            if !j.params.iter().any(|(k, _)| k == &key) {
                j.params.push((key, file_size.to_string()));
                self.dirty = true;
                let epoch = self.job_epochs.get(&job).copied();
                self.persist_then(move |owner, ok| {
                    if owner.job_epochs.get(&job).copied() == epoch {
                        if ok {
                            owner.send_finalize(job, file);
                        } else {
                            owner.hold_job(
                                job,
                                nzbd_types::HoldCause::StorageUnavailable,
                                "finalize",
                                "partial checkpoint metadata could not be persisted",
                            );
                        }
                    }
                });
                return;
            }
            let tx = self.writer_for(job, file);
            match tx.try_send(WriteCmd::SealPartial {
                file_size,
                ranges: segs,
            }) {
                Ok(()) => {
                    self.finalize_sent.insert(file);
                }
                Err(mpsc::error::TrySendError::Full(_)) => self.pending_finalize.push((job, file)),
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    if let Some(writer) = self.writers.remove(&file) {
                        self.retire_writer_handle(job, writer);
                    }
                    self.pending_finalize.push((job, file));
                }
            }
            return;
        }
        if !range_checkpoint
            && self
                .state
                .job(job)
                .is_some_and(|j| j.file_needs_repair(file))
        {
            let key = format!("*File:repair:{}", file.0);
            self.state
                .job_mut(job)
                .unwrap()
                .params
                .retain(|(name, _)| name != &key);
            self.dirty = true;
            let epoch = self.job_epochs.get(&job).copied();
            self.persist_then(move |owner, ok| {
                if owner.job_epochs.get(&job).copied() == epoch {
                    if ok {
                        owner.send_finalize(job, file);
                    } else {
                        owner.hold_job(
                            job,
                            nzbd_types::HoldCause::StorageUnavailable,
                            "finalize",
                            "complete checkpoint metadata could not be persisted",
                        );
                    }
                }
            });
            return;
        }
        let filename = self
            .state
            .file_mut(job, file)
            .map(|f| f.filename.clone())
            .unwrap_or_default();
        let tx = self.writer_for(job, file);
        if tx.try_send(WriteCmd::PublicationName(filename)).is_err() {
            self.pending_finalize.push((job, file));
            return;
        }
        match tx.try_send(WriteCmd::Finalize {
            file_size,
            combined_crc,
        }) {
            Ok(()) => {
                self.finalize_sent.insert(file);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.pending_finalize.push((job, file)); // retried on tick
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                if let Some(writer) = self.writers.remove(&file) {
                    self.retire_writer_handle(job, writer);
                }
                self.pending_finalize.push((job, file)); // respawn on retry
            }
        }
    }

    fn on_writer_finalized(
        &mut self,
        job: JobId,
        file: FileId,
        ok: bool,
        final_path: Option<PathBuf>,
        combined_crc: Option<u32>,
    ) {
        if let Some(writer) = self.writers.remove(&file) {
            self.retire_writer_handle(job, writer);
        }
        self.writer_epochs.remove(&file);
        self.finalize_sent.remove(&file);
        self.file_sizes.remove(&file);
        let Some(f) = self.state.file_mut(job, file) else {
            return;
        };
        f.finalized = true;
        if ok {
            f.crc32 = combined_crc;
        }
        let filename = f.filename.clone();
        if !ok {
            // The bytes did not land. Record it against the job so that
            // check_job_complete cannot call this a success — see the field's
            // comment for why health is incapable of catching it.
            tracing::error!(job = job.0, file = file.0, %filename,
                "finalize failed; the job cannot be reported as successful");
            self.write_failures
                .entry(job)
                .or_insert_with(|| format!("could not write {filename} to disk"));
        }
        tracing::info!(job = job.0, file = file.0, %filename, ok, path = ?final_path, "file finished");
        // A completed file may be the recovery index that knows what this
        // whole job actually is. Only worth asking when the job still has
        // no real name.
        if ok {
            if let Some(path) = final_path.clone() {
                self.maybe_learn_name_from(job, path);
            }
        }
        self.emit(Event::FileFinished {
            job,
            file,
            filename,
            ok,
        });
        self.dirty = true;
        self.check_job_complete(job);
    }

    /// Ask a just-completed file whether it is a par2 index, and if so
    /// whether it names the job.
    ///
    /// This is the "as soon as possible" half of naming an obfuscated
    /// post. The evidence exists from the moment the recovery index lands
    /// — for the 4.8 GiB job #182 that was **one minute** into the
    /// download — and waiting for post-processing to discover it means the
    /// queue shows a 40-character hash for the entire hour it runs.
    ///
    /// Content-sniffed, never extension-matched: an obfuscated post hides
    /// its par2 files exactly like everything else (#182's index arrived
    /// as `LKKp171CWZ3IrtvUyiLuNWIqWtos`), so `is_par2` — which is a guess
    /// off the NZB subject — is `false` for precisely the files this needs
    /// to read. The read happens on a blocking thread: the owner loop is
    /// the single writer for the whole queue and must never sit on a
    /// network mount's I/O.
    fn maybe_learn_name_from(&mut self, job: JobId, path: PathBuf) {
        let Some(j) = self.state.job(job) else { return };
        if !crate::queue::name_is_open(j) {
            return; // already named by something real
        }
        *self.pending_name_inspections.entry(job).or_default() += 1;
        let tx = self.engine_tx.clone();
        let generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
        let inventory = self.artifacts.clone();
        let cancellation = self.cancel.clone();
        self.tracker.spawn_blocking(move || {
            let checkpoint = || {
                if cancellation.is_cancelled() {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "name inspection cancelled",
                    ))
                } else {
                    Ok(())
                }
            };
            let guard = inventory.acquire_terminal_use(job.0, &checkpoint);
            let name = match guard {
                Ok(Some(_guard)) if checkpoint().is_ok() => read_par2_name(&path),
                _ => None,
            };
            // This runs on the blocking pool, so waiting for channel room
            // cannot stall the queue owner. Every inspection must answer,
            // including a negative one, or completion could wait forever.
            let _ = tx.blocking_send(EngineMsg::ScopedNameInspected {
                job,
                generation,
                name,
            });
        });
    }

    fn on_job_name_inspected(&mut self, job: JobId, name: Option<String>) {
        match self.pending_name_inspections.get_mut(&job) {
            Some(pending) if *pending > 1 => *pending -= 1,
            Some(_) => {
                self.pending_name_inspections.remove(&job);
            }
            None => {}
        }
        if let Some(name) = name {
            self.adopt_learned_name(job, name);
        }
        // The last writer may already have finalized while this blocking
        // read was in flight. Re-evaluate now that it can no longer be
        // overtaken by the terminal event.
        self.check_job_complete(job);
    }

    /// Fall back to naming a job by who asked for it, when its own
    /// documents named it nothing. Returns the new name if it changed.
    fn name_from_requestor(&mut self, job: JobId, storage_too: bool) -> Option<String> {
        let j = self.state.job_mut(job)?;
        if !crate::queue::name_is_open(j) {
            return None;
        }
        let better = crate::queue::requestor_name(j)?;
        let was = j.name.clone();
        // Provisional: this says who asked, not what arrived. The job's own
        // par2 metadata supersedes it the moment that lands.
        //
        // Through `set_job_name`, not the free function: a requestor name
        // is built from the client and the indexer and so is *shared* by
        // every job that client adds — exactly the shape that must never
        // be allowed to become a shared download directory.
        self.state
            .set_job_name(job, better.clone(), storage_too, true);
        tracing::info!(job = job.0, from = %was, to = %better,
            "job named from its requestor — the NZB carried no name of its own");
        Some(better)
    }

    /// Adopt a name recovered from par2 metadata mid-download.
    ///
    /// The DISPLAY name only: files are already on disk under the storage
    /// name, and moving that out from under the writers is the bug the
    /// `name`/`dir_name` split exists to prevent. Post-processing moves
    /// the finished job into a directory named from the display name, so
    /// what lands in the destination still reads correctly.
    fn adopt_learned_name(&mut self, job: JobId, name: String) {
        let Some(j) = self.state.job_mut(job) else {
            return;
        };
        // The race this closes: between the blocking read and this turn,
        // the job may have been named by its own NZB metadata or by a
        // second par2 file. First real name wins; a rename that flickers
        // is worse than one that is late.
        if !crate::queue::name_is_open(j) {
            return;
        }
        let was = j.name.clone();
        // Final: the recovery set is the document naming itself.
        self.state.set_job_name(job, name.clone(), false, false);
        tracing::info!(job = job.0, from = %was, to = %name,
            "job named from its own par2 metadata");
        self.dirty = true;
        self.publish_now();
        self.bump_epoch();
    }

    fn check_job_complete(&mut self, job_id: JobId) {
        if self.pending_control_commits.contains(&job_id)
            || self.cancel.is_cancelled()
            || self.verification.contains_key(&job_id)
            || self.retiring_writers.contains_key(&job_id)
        {
            return;
        }
        if self.state.job(job_id).is_some_and(|j| j.held()) {
            return;
        }
        if self.delegated.contains_key(&job_id) {
            return; // completes via the executor's report, not locally
        }
        if self
            .pending_name_inspections
            .get(&job_id)
            .is_some_and(|pending| *pending > 0)
        {
            return; // a par2 answer already in flight gets the final word
        }
        let Some(job) = self.state.job_mut(job_id) else {
            return;
        };
        if !matches!(
            job.status,
            JobStatus::Queued | JobStatus::Downloading | JobStatus::Paused
        ) {
            return;
        }
        let complete = !job.files.is_empty()
            && job
                .files
                .iter()
                .all(|f| f.paused || (f.is_terminal() && (!f.has_any_done() || f.finalized)));
        if !complete {
            return;
        }
        let (mut status, health) = final_status(job);
        // A delayed-PAR fetch temporarily returns a live post-processing job
        // to the download scheduler. The open stage is persisted while the
        // in-memory post_fetch_files authorization is not, so the span—not
        // that map—is the recovery fact that also survives a daemon restart.
        // Restore the PP state when the supplemental download ends and do not
        // emit a second JobFinished while verification/repair still owns it.
        let resume_post_stage = open_post_stage(job);
        // A write that did not land overrides a healthy article set. The two
        // measure different things: health says the bytes arrived off the
        // wire, this says they reached the disk. Reporting SUCCESS on the
        // strength of the first while the second failed is how a consumer
        // ends up importing 500 MiB of a 48 GiB file.
        let mut recovery_write_failed = false;
        if let Some(reason) = self.write_failures.remove(&job_id) {
            if let Some(stage) = resume_post_stage {
                // The active PP task will re-verify the filesystem and either
                // request another recovery volume or fail once, through its
                // normal durable history path. Emitting JobFinished(Failed)
                // here would race that task and could dispose its directory
                // while it is still using it.
                status = JobStatus::Post { stage };
                job.status = status;
                recovery_write_failed = true;
                tracing::error!(job = job_id.0, %reason, stage = stage.as_str(),
                    "post-processing recovery download did not reach disk; resuming verification");
            } else {
                status = JobStatus::Failed;
                job.status = status;
                tracing::error!(job = job_id.0, %reason,
                    "job failed: the download completed but the files did not");
            }
        } else if let Some(stage) = resume_post_stage {
            status = JobStatus::Post { stage };
            job.status = status;
        } else {
            job.status = status;
        }
        let name = job.name.clone();
        let file_ids: Vec<FileId> = job.files.iter().map(|f| f.id).collect();
        if let Some(stage) = resume_post_stage {
            if !recovery_write_failed {
                tracing::info!(
                    job = job_id.0,
                    %name,
                    stage = stage.as_str(),
                    health = health.0,
                    "post-processing recovery download finished; resuming post-processing"
                );
            }
        } else {
            tracing::info!(
                job = job_id.0,
                %name,
                ?status,
                health = health.0,
                "download phase finished"
            );
        }
        self.attempts.retain(|r, _| r.job != job_id);
        self.post_fetch_files.remove(&job_id);
        for fid in &file_ids {
            self.writers.remove(fid);
            self.file_sizes.remove(fid);
        }
        // Persist and publish BEFORE emitting: an event subscriber that
        // immediately reads the snapshot must see the resulting state.
        let epoch = self.job_epochs.get(&job_id).copied();
        self.pending_control_commits.insert(job_id);
        self.persist_then(move |owner, ok| {
            if owner.job_epochs.get(&job_id).copied() != epoch {
                return;
            }
            owner.pending_control_commits.remove(&job_id);
            if ok
                && owner
                    .state
                    .job(job_id)
                    .is_some_and(|job| job.status == status && !job.held())
            {
                owner.publish_now();
                if resume_post_stage.is_none() {
                    owner.emit(Event::JobFinished {
                        job: job_id,
                        name,
                        status,
                        health: health.0,
                    });
                }
            } else if !ok {
                owner.hold_job(
                    job_id,
                    nzbd_types::HoldCause::StorageUnavailable,
                    "completion",
                    "download completion checkpoint is not durable",
                );
            }
        });
        self.publish_now();
    }

    // -- servers -------------------------------------------------------------

    fn is_blocked(&self, server: ServerId) -> bool {
        self.blocked
            .get(&server)
            .is_some_and(|until| *until > Instant::now())
    }

    fn block_server(&mut self, server: ServerId) {
        let until = Instant::now() + self.tuning.retry_interval;
        let newly = self
            .blocked
            .insert(server, until)
            .is_none_or(|prev| prev <= Instant::now());
        if newly {
            tracing::warn!(
                server = server.0,
                secs = self.tuning.retry_interval.as_secs(),
                "server blocked after connection failure — retrying on a timer"
            );
            self.emit(Event::ServerBlocked {
                server,
                seconds: self.tuning.retry_interval.as_secs(),
            });
        }
    }

    // -- writers -------------------------------------------------------------

    /// Only for new admissions, before any segments or writers exist. Existing
    /// jobs must retain their committed allocation and go through ownership
    /// review if it becomes unavailable.
    fn allocate_new_payload_then(
        &mut self,
        job: JobId,
        callback: impl FnOnce(&mut Owner, Result<(), nzbd_state::StateError>) + Send + 'static,
    ) {
        let Some(record) = self.state.job(job).cloned() else {
            callback(
                self,
                Err(nzbd_state::StateError::Corrupt(
                    "job vanished before allocation".into(),
                )),
            );
            return;
        };
        let inventory = self.artifacts.clone();
        let root = self.dest_dir.clone();
        let occupied = self
            .state
            .jobs
            .iter()
            .filter(|candidate| candidate.id != job)
            .map(job_dir_name)
            .collect::<HashSet<_>>();
        let epoch = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
        self.allocating_jobs.insert(job);
        self.payload_then(
            root.clone(),
            move || {
                // Admission can overtake the state-lane startup-root snapshot.
                // Prepare its own prerequisite on the bounded payload worker;
                // an enrolled missing mount must still remain unavailable.
                prepare_unenrolled_payload_root(&inventory, &root)?;
                let base = job_dir_name(&record);
                let mut suffix = 0_u32;
                let (name, path) = loop {
                    let name = if suffix == 0 {
                        base.clone()
                    } else {
                        format!("{base}.job-{}-{}", job.0, suffix - 1)
                    };
                    let path = root.join(&name);
                    let on_disk = path
                        .try_exists()
                        .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?;
                    let inventoried = inventory
                        .for_path(&path)
                        .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?
                        .is_some();
                    if !occupied.contains(&name) && !on_disk && !inventoried {
                        break (name, path);
                    }
                    suffix = suffix.checked_add(1).ok_or_else(|| {
                        nzbd_state::StateError::Corrupt("no free payload directory name".into())
                    })?;
                };
                let keep = record
                    .params
                    .iter()
                    .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1");
                let allocation = inventory
                    .allocate_for_delivery_with_keep(
                        job.0,
                        &root,
                        &path,
                        record.lifecycle_eligible(),
                        record.delivery_lifecycle,
                        keep,
                    )
                    .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?;
                Ok((name, allocation))
            },
            move |owner, result| {
                owner.allocating_jobs.remove(&job);
                if owner.job_epochs.get(&job) != Some(&epoch) {
                    callback(
                        owner,
                        Err(nzbd_state::StateError::Corrupt(
                            "allocation generation retired".into(),
                        )),
                    );
                    return;
                }
                let result = result.map(|(name, allocation)| {
                    if let Some(record) = owner.state.job_mut(job) {
                        record.dir_name = name;
                    }
                    owner.allocation_cache.insert(job, allocation);
                    owner.allocated_jobs.insert(job);
                    owner.bump_epoch();
                });
                callback(owner, result);
            },
        );
    }

    #[cfg(test)]
    fn allocate_new_payload(&mut self, job: JobId) -> Result<(), nzbd_state::artifacts::Error> {
        if self.allocated_jobs.contains(&job) {
            return Ok(());
        }
        self.allocate_new_payload_then(job, |owner, result| {
            if let Err(error) = result {
                tracing::warn!(%error,"payload allocation failed");
                owner.publish_now();
            }
        });
        Err(nzbd_state::artifacts::Error::Conflict(
            "payload allocation pending".into(),
        ))
    }

    fn ensure_allocation(
        &mut self,
        job: JobId,
        _new_admission: bool,
    ) -> Result<(), nzbd_state::artifacts::Error> {
        if self.retiring_writers.contains_key(&job) {
            return Err(nzbd_state::artifacts::Error::Conflict(
                "writer retirement in progress".into(),
            ));
        }
        let record = self
            .state
            .job(job)
            .cloned()
            .ok_or(nzbd_state::artifacts::Error::NotFound)?;
        let plan = transitions::imported_payload_plan(&record, &self.dest_dir)
            .map_err(|error| nzbd_state::artifacts::Error::Conflict(error.to_string()))?;
        if self.allocated_jobs.contains(&job)
            && self.allocation_cache.get(&job).is_some_and(|allocation| {
                !plan.private_range
                    || (allocation.path == plan.path && allocation.root == plan.root)
            })
        {
            return Ok(());
        }
        if self.allocating_jobs.contains(&job) {
            return Err(nzbd_state::artifacts::Error::Conflict(
                "payload allocation pending".into(),
            ));
        }
        let inventory = self.artifacts.clone();
        let root = self.dest_dir.clone();
        let epoch = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
        self.allocating_jobs.insert(job);
        let lane_root = if plan.private_range {
            plan.root.clone()
        } else {
            self.allocation_cache
                .get(&job)
                .map(|allocation| allocation.root.clone())
                .unwrap_or_else(|| root.clone())
        };
        self.payload_then(
            lane_root,
            move || {
                let allocation = inventory
                    .for_job(job.0)
                    .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?;
                let allocation = if allocation.is_none() && !plan.private_range {
                    prepare_unenrolled_payload_root(&inventory, &plan.root)?;
                    let keep = record
                        .params
                        .iter()
                        .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1");
                    inventory
                        .allocate_for_delivery_with_keep(
                            record.id.0,
                            &plan.root,
                            &plan.path,
                            record.lifecycle_eligible(),
                            record.delivery_lifecycle,
                            keep,
                        )
                        .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?
                } else {
                    transitions::allocate_imported_payload(
                        &inventory, &record, &root, &plan, allocation,
                    )?
                };
                if allocation.state != "active" {
                    return Err(nzbd_state::StateError::Corrupt(
                        "download allocation is retained or retiring".into(),
                    ));
                }
                Ok(allocation)
            },
            move |owner, result| {
                owner.allocating_jobs.remove(&job);
                if owner.job_epochs.get(&job) != Some(&epoch) {
                    return;
                }
                match result {
                    Ok(allocation) => {
                        owner.allocation_cache.insert(job, allocation);
                        owner.allocated_jobs.insert(job);
                        owner.bump_epoch();
                    }
                    Err(error) => {
                        owner.hold_job(
                            job,
                            allocation_hold_cause(&error.to_string()),
                            "download_write",
                            &error.to_string(),
                        );
                    }
                }
                owner.publish_now();
            },
        );
        Err(nzbd_state::artifacts::Error::Conflict(
            "payload allocation pending".into(),
        ))
    }

    fn writer_for(&mut self, job: JobId, file: FileId) -> mpsc::Sender<WriteCmd> {
        let closed = || {
            let (tx, rx) = mpsc::channel(1);
            drop(rx);
            tx
        };
        if self.pending_control_commits.contains(&job)
            || self.pending_control_revisions.contains_key(&job)
            || self.cancel.is_cancelled()
            || self.verification.contains_key(&job)
            || self.retiring_writers.contains_key(&job)
        {
            return closed();
        }
        if let Some(writer) = self.writers.get(&file) {
            if !writer.tx.is_closed() && !writer.stop.is_cancelled() {
                return writer.tx.clone();
            }
            if !*writer.stopped.borrow() {
                self.retire_job_writers(job);
                return closed();
            }
        }
        if self.pending_writer_birth.contains_key(&file) {
            return closed();
        }
        self.writers.remove(&file);
        self.writer_epochs.remove(&file);
        if let Err(error) = self.ensure_allocation(job, false) {
            tracing::warn!(job=job.0,%error,"writer allocation unavailable");
            return closed();
        }
        let Some(allocation) = self.allocation_cache.get(&job).cloned() else {
            return closed();
        };
        let Some(record) = self.state.job(job) else {
            return closed();
        };
        let Some(entry) = record.files.iter().find(|entry| entry.id == file) else {
            return closed();
        };
        let proven = entry
            .segments
            .iter()
            .filter_map(|segment| match segment.state {
                SegmentState::Done { offset, len, crc } => Some(ProvenRange {
                    seg_number: segment.number,
                    offset,
                    len,
                    crc,
                }),
                _ => None,
            })
            .collect::<Vec<_>>();
        let proof_key = format!("*File:proof:{}", file.0);
        let generation_key = format!("*File:generation:{}", file.0);
        let mut proof = record
            .params
            .iter()
            .find(|(key, _)| key == &proof_key)
            .and_then(|(_, value)| serde_json::from_str::<DurableFileProof>(value).ok());
        let old_generation = record
            .params
            .iter()
            .find(|(key, _)| key == &generation_key)
            .map(|(_, value)| value.as_str())
            .unwrap_or("");
        if !proven.is_empty()
            && !proof.as_ref().is_some_and(|proof| {
                proof.covers(
                    old_generation,
                    &allocation.generation,
                    &self.artifacts.installation,
                    &proof.identity,
                    &proven,
                )
            })
        {
            self.queue_verification(job);
            return closed();
        }
        let expected_identity = proof.as_ref().map(|proof| proof.identity.clone());
        let filename = record
            .params
            .iter()
            .find(|(key, _)| key == &format!("*File:publication:{}", file.0))
            .map(|(_, value)| value.clone())
            .unwrap_or_else(|| entry.filename.clone());
        let generation = fresh_epoch();
        let owner_epoch = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
        if let Some(proof) = proof.as_mut() {
            proof.generation = generation.to_string();
            proof.ranges = proven.clone();
        }
        let record = self.state.job_mut(job).unwrap();
        set_owner_param(
            &mut record.params,
            &generation_key,
            Some(generation.to_string()),
        );
        if let Some(proof) = proof {
            set_owner_param(
                &mut record.params,
                &proof_key,
                serde_json::to_string(&proof).ok(),
            );
        }
        self.pending_writer_birth.insert(file, generation);
        self.dirty = true;
        self.persist_then(move |owner, ok| {
            if owner.pending_writer_birth.get(&file) != Some(&generation) {
                return;
            }
            owner.pending_writer_birth.remove(&file);
            if !ok
                || owner.cancel.is_cancelled()
                || owner.job_epochs.get(&job) != Some(&owner_epoch)
                || owner.verification.contains_key(&job)
                || owner.retiring_writers.contains_key(&job)
                || owner
                    .state
                    .job(job)
                    .is_none_or(|record| !record.files.iter().any(|entry| entry.id == file))
            {
                return;
            }
            let scope = WriterScope {
                generation,
                proven,
                expected_identity,
            };
            let writer = spawn_writer_scoped(
                &owner.tracker,
                job,
                file,
                allocation.path,
                filename,
                owner.engine_tx.clone(),
                scope,
            );
            owner.writer_epochs.insert(file, generation);
            owner.writers.insert(file, writer);
            owner.bump_epoch();
        });
        closed()
    }

    fn retire_writer_handle(&mut self, job: JobId, writer: WriterHandle) {
        writer.stop.cancel();
        if *writer.stopped.borrow() {
            return;
        }
        let mut writers = self.retiring_writers.remove(&job).unwrap_or_default();
        writers.push(writer);
        let generation = fresh_epoch();
        self.retirement_epochs.insert(job, generation);
        self.retiring_writers.insert(job, writers.clone());
        let engine = self.engine_tx.clone();
        self.tracker.spawn(async move {
            for mut writer in writers {
                if writer.stopped.wait_for(|stopped| *stopped).await.is_err() {
                    return;
                }
            }
            let _ = engine
                .send(EngineMsg::WritersStopped { job, generation })
                .await;
        });
    }

    fn retire_job_writers(&mut self, job: JobId) {
        let files = self
            .state
            .job(job)
            .map(|job| job.files.iter().map(|file| file.id).collect::<Vec<_>>())
            .unwrap_or_default();
        let had_retirement = self.retiring_writers.contains_key(&job);
        let mut writers = self.retiring_writers.remove(&job).unwrap_or_default();
        for file in files {
            self.writer_epochs.remove(&file);
            self.pending_writer_birth.remove(&file);
            if let Some(writer) = self.writers.remove(&file) {
                writer.stop.cancel();
                writers.push(writer);
            }
        }
        if writers.is_empty() {
            if had_retirement {
                self.retiring_writers.insert(job, vec![]);
            }
            return;
        }
        let generation = fresh_epoch();
        self.retirement_epochs.insert(job, generation);
        self.retiring_writers.insert(job, writers.clone());
        let engine = self.engine_tx.clone();
        self.tracker.spawn(async move {
            for mut writer in writers {
                writer.stop.cancel();
                if writer.stopped.wait_for(|stopped| *stopped).await.is_err() {
                    return;
                }
            }
            let _ = engine
                .send(EngineMsg::WritersStopped { job, generation })
                .await;
        });
    }

    fn drain_replacements(&mut self) {
        if self.cancel.is_cancelled() || self.projection_transition {
            return;
        }
        let count = self.deferred_replacements.len();
        for _ in 0..count {
            let Some(command) = self.deferred_replacements.pop_front() else {
                break;
            };
            if matches!(
                &command,
                QueueCommand::AdoptReplicatedAuthority { .. } | QueueCommand::AdoptAuthority { .. }
            ) {
                let busy = self.state.jobs.iter().any(|job| {
                    self.pending_name_inspections
                        .get(&job.id)
                        .is_some_and(|count| *count > 0)
                        || self.retiring_writers.contains_key(&job.id)
                        || self.post_admissions.contains_key(&job.id)
                        || self.allocating_jobs.contains(&job.id)
                        || self.record_transitions.contains(&job.id)
                        || self.pending_control_revisions.contains_key(&job.id)
                        || self
                            .verification
                            .get(&job.id)
                            .is_some_and(|state| state.running)
                        || job.files.iter().any(|file| {
                            self.writers
                                .get(&file.id)
                                .is_some_and(|writer| !*writer.stopped.borrow())
                        })
                });
                if busy {
                    self.deferred_replacements.push_back(command);
                } else {
                    self.on_command(command);
                }
                continue;
            }
            let job = match &command {
                QueueCommand::MaterializeGeneration { request, .. } => request.job.id,
                QueueCommand::ImportJob { job, .. }
                | QueueCommand::ImportJobIfPresent { job, .. } => job.id,
                QueueCommand::RemoveJobSilent { job, .. } | QueueCommand::Delete { job, .. } => {
                    *job
                }
                _ => continue,
            };
            if self
                .pending_name_inspections
                .get(&job)
                .is_some_and(|count| *count > 0)
                || self.retiring_writers.contains_key(&job)
                || self.post_admissions.contains_key(&job)
                || self.allocating_jobs.contains(&job)
                || self.record_transitions.contains(&job)
                || self.pending_control_revisions.contains_key(&job)
                || self
                    .verification
                    .get(&job)
                    .is_some_and(|state| state.running)
            {
                self.deferred_replacements.push_back(command);
                continue;
            }
            self.on_command(command);
        }
    }

    // -- jobs ----------------------------------------------------------------

    /// Delayed-par: unpause the smallest covering set; returns blocks freed.
    ///
    /// Returning 0 here is what repair reads as "nothing left to fetch",
    /// so every path that can return 0 while paused recovery data is
    /// sitting in the queue says why — that branch was silent through
    /// months of PAR_FAILUREs on jobs with 5 GB of recovery blocks on hand.
    fn unpause_par_blocks(&mut self, job_id: JobId, blocks: u32, block_size: Option<u64>) -> u32 {
        if self.state.job(job_id).is_some_and(|j| j.held()) {
            return 0;
        }
        let Some(job) = self.state.job_mut(job_id) else {
            tracing::warn!(
                job = job_id.0,
                "delayed par blocks requested for a job that is no longer in the queue"
            );
            return 0;
        };
        let paused_pars = job.files.iter().filter(|f| f.paused && f.is_par2).count();
        if paused_pars == 0 {
            tracing::info!(
                job = job_id.0,
                blocks,
                "no paused par2 files left — every recovery volume this job has is already fetched"
            );
            return 0;
        }
        let (priced, unpriceable) = crate::queue::price_paused_pars(job, block_size);
        let (candidates, last_resort) = if priced.is_empty() {
            // Nothing carried a vol marker and no block size was available:
            // unpause the cheapest volume rather than give up. One round
            // costs one file; the next round prices better or escalates.
            tracing::warn!(
                job = job_id.0,
                blocks,
                paused_pars,
                "no paused par2 file could be priced in recovery blocks (no .volXX+NN marker, \
                 no par2 block size) — unpausing the smallest one as a probe"
            );
            (Vec::new(), crate::queue::smallest_paused_par(job))
        } else {
            if !unpriceable.is_empty() {
                tracing::warn!(
                    job = job_id.0,
                    unpriced = unpriceable.len(),
                    "some paused par2 files could not be priced and were left paused"
                );
            }
            let pairs: Vec<(FileId, u32)> = priced.iter().map(|c| (c.id, c.blocks)).collect();
            (pick_par_files(&pairs, blocks.max(1)), None)
        };
        let estimated = priced.iter().any(|c| c.estimated);
        let mut freed = 0u32;
        let mut authorized = HashSet::new();
        for c in &priced {
            if candidates.contains(&c.id) {
                if let Some(f) = job.files.iter_mut().find(|f| f.id == c.id) {
                    f.paused = false;
                    freed += c.blocks;
                    authorized.insert(c.id);
                }
            }
        }
        if let Some(id) = last_resort {
            if let Some(f) = job.files.iter_mut().find(|f| f.id == id) {
                f.paused = false;
                freed = freed.max(1);
                authorized.insert(id);
            }
        }
        if freed == 0 {
            tracing::warn!(
                job = job_id.0,
                blocks,
                paused_pars,
                priced = priced.len(),
                "repair asked for recovery blocks and nothing could be unpaused — the paused par2 \
                 files were priced but none were selected"
            );
        } else if estimated {
            tracing::info!(
                job = job_id.0,
                freed,
                block_size,
                "delayed par blocks priced by file size — this post's recovery volumes carry no \
                 .volXX+NN marker"
            );
        }
        if freed > 0 {
            // The job likely finished its download phase; make it
            // schedulable again for the par files.
            if matches!(
                job.status,
                JobStatus::Completed | JobStatus::PostQueued | JobStatus::Post { .. }
            ) {
                job.status = JobStatus::Queued;
            }
            recompute_job_totals(job);
            self.post_fetch_files
                .entry(job_id)
                .or_default()
                .extend(authorized);
            tracing::info!(job = job_id.0, freed, "delayed par files unpaused");
            self.dirty = true;
            self.bump_epoch();
            self.publish_now();
        }
        freed
    }

    fn delete_job_then(
        &mut self,
        job: JobId,
        delete_files: bool,
        callback: impl FnOnce(&mut Owner, bool) + Send + 'static,
    ) {
        if delete_files
            || self.state.job(job).is_none()
            || self.post_admissions.contains_key(&job)
            || self
                .verification
                .get(&job)
                .is_some_and(|state| state.running)
        {
            callback(self, false);
            return;
        }
        self.retire_job_writers(job);
        if self
            .retiring_writers
            .get(&job)
            .is_some_and(|writers| writers.iter().any(|writer| !*writer.stopped.borrow()))
        {
            callback(self, false);
            return;
        }
        let epoch = self.job_epochs.get(&job).copied();
        let inventory = self.artifacts.clone();
        self.pending_control_commits.insert(job);
        let lane_root = self
            .allocation_cache
            .get(&job)
            .map(|allocation| allocation.root.clone())
            .unwrap_or_else(|| self.dest_dir.clone());
        self.payload_then(
            lane_root,
            move || {
                let retiring = inventory
                    .prepare_forget(job.0)
                    .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?;
                if let Some(artifact) = retiring {
                    inventory
                        .finish(job.0, &artifact.path, &artifact.root, "retained")
                        .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?;
                }
                Ok(())
            },
            move |owner, result| {
                owner.pending_control_commits.remove(&job);
                if result.is_err() || owner.job_epochs.get(&job).copied() != epoch {
                    callback(owner, false);
                    return;
                }
                if !owner.remove_job_silent(job) {
                    callback(owner, false);
                    return;
                }
                owner.persist_then(move |owner, ok| {
                    if ok {
                        owner.publish_now();
                        owner.emit(Event::JobDeleted { job });
                        owner.bump_epoch();
                    }
                    callback(owner, ok);
                });
            },
        );
    }

    // -- tick / snapshot -----------------------------------------------------

    /// Disk + quota guards, evaluated every 10 s (cluster-aware quota:
    /// peers' volume files on the shared state dir are summed in).
    /// Reposition a job in the queue vec. Position is the scheduler's
    /// tiebreaker within a priority band and persists via the snapshot.
    fn move_job(&mut self, job_id: JobId, op: MoveOp) -> bool {
        let Some(idx) = self.state.jobs.iter().position(|j| j.id == job_id) else {
            return false;
        };
        let last = self.state.jobs.len() - 1;
        let target = match op {
            MoveOp::Top => 0,
            MoveOp::Up => idx.saturating_sub(1),
            MoveOp::Down => (idx + 1).min(last),
            MoveOp::Bottom => last,
        };
        if target != idx {
            let job = self.state.jobs.remove(idx);
            self.state.jobs.insert(target, job);
            self.dirty = true;
            self.bump_epoch();
            self.publish_now();
        }
        true
    }

    /// The prober task's write side, handed out at spawn (lib.rs) so the
    /// statvfs syscall lives on a blocking thread, never in this loop.
    pub(crate) fn disk_guard_handle(&self) -> Arc<ArcSwap<DiskGuardReading>> {
        self.disk_guard.clone()
    }

    /// A write path hit ENOSPC/EDQUOT. Hold intake NOW.
    ///
    /// The statvfs prober is a forecast, and on a quota-backed mount it
    /// can be wrong for hours: nuc3 answered `disk_low: false` and kept
    /// downloading at wire speed while writers, finalize and PP were all
    /// being told there was no space — 725 GB burned in a day. A failed
    /// write is not a forecast, so it does not go through the threshold:
    /// it latches the guard directly and only a statvfs reading of twice
    /// the floor (or an operator resume) clears it.
    pub(crate) fn observe_out_of_space(&mut self, whence: &str) {
        self.enospc_observed += 1;
        self.enospc_where = Some(whence.to_string());
        if !self.enospc_latched {
            self.enospc_latched = true;
            tracing::error!(
                whence,
                observed = self.enospc_observed,
                "out of space on a configured write volume — holding all downloads (observed from a \
                 write, not from the free-space probe)"
            );
        }
        if !self.disk_low {
            self.disk_low = true;
            self.pause_incomplete_torrents_for_storage();
            self.publish_now();
        }
    }

    /// Operator resume clears the latch: they have been told what
    /// happened, and only they can know the volume is usable again when
    /// statvfs is the thing that is lying.
    pub(crate) fn clear_enospc_latch(&mut self) {
        if self.enospc_latched {
            self.enospc_latched = false;
            // Drop only the previous latch-derived hold before reevaluating.
            // A current below-floor reading immediately restores disk_low;
            // an unknown reading does not defeat the explicit override.
            self.disk_low = false;
            tracing::info!(
                observed = self.enospc_observed,
                "out-of-space latch cleared by the operator"
            );
            self.update_disk_guard();
            self.publish_now();
        }
    }

    fn update_disk_guard(&mut self) {
        let reading = self.disk_guard.load_full();
        let free = reading.available_bytes;
        // Hysteresis: a mount that lied once has to prove itself with room
        // to spare before intake resumes, so a volume hovering at the
        // floor cannot flap the whole fleet. With no floor configured the
        // latch is the operator's to clear.
        let was = self.disk_low;
        let was_latched = self.enospc_latched;
        (self.enospc_latched, self.disk_low) = disk_guard_decision(
            was,
            self.enospc_latched,
            &reading,
            self.tuning.min_free_disk_bytes,
        );
        self.evaluated_disk_guard = reading.clone();
        if was_latched && !self.enospc_latched {
            tracing::info!(
                free = free.unwrap_or_default(),
                clear_at = self.tuning.min_free_disk_bytes.saturating_mul(2),
                limiting_path = ?reading.limiting_path,
                "out-of-space latch cleared — every volume reports twice the floor free"
            );
        }
        if self.disk_low != was {
            if self.disk_low {
                self.pause_incomplete_torrents_for_storage();
                tracing::warn!(
                    free = free.unwrap_or_default(),
                    floor = self.tuning.min_free_disk_bytes,
                    limiting_label = ?reading.limiting_label,
                    limiting_path = ?reading.limiting_path,
                    observed_enospc = self.enospc_observed,
                    "configured write volume low on space — downloads held"
                );
            } else {
                self.release_torrents_after_storage_recovery();
                tracing::info!(
                    free = free.unwrap_or_default(),
                    limiting_path = ?reading.limiting_path,
                    "disk space recovered — downloads resume"
                );
            }
            self.publish_guard_change(was, self.disk_low);
        }
    }

    fn pause_incomplete_torrents_for_storage(&mut self) {
        let jobs = self
            .state
            .jobs
            .iter()
            .filter(|job| {
                job.kind == JobKind::Torrent
                    && job.torrent.as_ref().is_some_and(|torrent| {
                        torrent.ready_at_unix.is_none()
                            && torrent.removal_intent.is_none()
                            && torrent.control_intent == TorrentControlIntent::Running
                    })
            })
            .map(|job| job.id)
            .collect::<Vec<_>>();
        for job in jobs {
            if self.enqueue_backend_command(BackendCommand::PauseForStorage { job }) {
                self.backend_started.remove(&job);
            }
        }
    }

    fn release_torrents_after_storage_recovery(&mut self) {
        let mut changed = false;
        for job in &mut self.state.jobs {
            let Some(torrent) = job.torrent.as_mut() else {
                continue;
            };
            if torrent.control_intent == TorrentControlIntent::Running
                && torrent.phase == TorrentPhase::PausedDownload
                && torrent.last_error.as_deref() == Some("storage full")
            {
                torrent.phase = TorrentPhase::Queued;
                torrent.last_error = None;
                torrent.stop_reason = None;
                job.status = JobStatus::Queued;
                changed = true;
            }
        }
        if changed {
            self.dirty = true;
            self.bump_epoch();
        }
    }

    fn update_quota_guard(&mut self) {
        if self.quota_probe_pending
            || (self.tuning.daily_quota_bytes == 0 && self.tuning.monthly_quota_bytes == 0)
        {
            return;
        }
        self.quota_probe_pending = true;
        let book = self.volumes.clone();
        let start_day = self.tuning.quota_start_day;
        self.io_then(
            move || Ok(book.peer_totals(unix_now(), start_day)),
            move |owner, result| {
                owner.quota_probe_pending = false;
                let Ok((peers_day, peers_month)) = result else {
                    return;
                };
                let day = peers_day.saturating_add(owner.volumes.doc().day_total(unix_now()));
                let month = peers_month
                    .saturating_add(owner.volumes.doc().month_total(unix_now(), start_day));
                let was = owner.quota_reached;
                owner.quota_reached = (owner.tuning.daily_quota_bytes > 0
                    && day >= owner.tuning.daily_quota_bytes)
                    || (owner.tuning.monthly_quota_bytes > 0
                        && month >= owner.tuning.monthly_quota_bytes);
                if was != owner.quota_reached {
                    owner.publish_guard_change(was, owner.quota_reached);
                }
            },
        );
    }

    fn save_volumes(&mut self) {
        let book = self.volumes.clone();
        let revision = book.revision();
        self.io_then(
            move || book.save_result(),
            move |owner, result| {
                if result.is_ok() {
                    owner.volumes.acknowledge_save(revision);
                } else {
                    tracing::warn!(error=?result.err(),"volume counters save failed");
                }
            },
        );
    }

    /// Publish every admission-guard transition. Releasing a hold also
    /// advances the work epoch: connection tasks that received an empty
    /// lease batch are parked on that watch and otherwise have no reason to
    /// ask the owner for newly eligible work again.
    fn publish_guard_change(&mut self, was_held: bool, is_held: bool) {
        self.publish_now();
        if was_held && !is_held {
            self.bump_epoch();
        }
    }

    fn on_tick(&mut self) {
        self.owner_heartbeat
            .store(unix_now() as u64, std::sync::atomic::Ordering::Relaxed);
        self.drain_replacements();
        self.retry_payload_recoveries();
        self.start_verifications();
        let tick_started = Instant::now();
        self.flush_backend_commands();
        // The adapter publishes its latest-value progress sample before a
        // structural Ready fact. Fold that sample first so Ready can prove
        // completion in the same owner tick.
        self.fold_backend_progress();
        self.fold_backend_structural();
        self.finalize_confirmed_torrent_removals();
        let seed_checkpoint_due = self.update_seed_policies(unix_now());
        self.guard_tick = self.guard_tick.wrapping_add(1);
        self.settle_download_labels();
        self.schedule_torrent_starts();
        // Reading the enforcing disk cache is memory-only, so do it every
        // owner tick. Quota totals still touch peer files and retain their
        // 10-second cadence. NOT `is_multiple_of`: stabilized in 1.87;
        // the workspace MSRV is 1.85.
        let t = Instant::now();
        self.update_disk_guard();
        let mut guards_ms = t.elapsed().as_millis() as u64;
        if self.guard_tick % 10 == 1 {
            let t = Instant::now();
            self.update_quota_guard();
            guards_ms = guards_ms.saturating_add(t.elapsed().as_millis() as u64);
        }
        let mut volumes_ms = 0u64;
        if self.guard_tick.is_multiple_of(30) {
            let t = Instant::now();
            self.save_volumes();
            volumes_ms = t.elapsed().as_millis() as u64;
        }

        // ONE wire measurement. The drain is stamped with the wall time it
        // covers, per-job and per-server EMAs fold the same bytes at the
        // same alpha, and the header rate is the SUM of the per-server
        // EMAs — so the tile, the chips and the rows can only disagree by
        // integer rounding, at any tick cadence (field report 2026-07-26:
        // 24.8 MiB/s header vs 9.9 rows was the old ring-vs-EMA split,
        // inflated by fsync-delayed ticks counting >1 s of bytes as 1 s).
        let drained = self.meter.drain();
        crate::rate::fold_wire_ema(&mut self.job_wire_ema, &drained.per_job, drained.secs);
        crate::rate::fold_wire_ema(&mut self.server_wire_ema, &drained.per_server, drained.secs);
        // Ghost entries (removed servers) decay; drop them below display
        // resolution so they cannot linger in the snapshot forever.
        self.server_wire_ema.retain(|_, v| *v > 0.4);
        let rate: u64 = self
            .server_wire_ema
            .values()
            .map(|e| e.max(0.0) as u64)
            .sum();
        self.rebalance_download_budget(rate);

        let now = Instant::now();
        let before = self.blocked.len();
        self.blocked.retain(|_, until| *until > now);
        if self.blocked.len() != before {
            self.bump_epoch(); // blocked servers came back: hand out work
        }

        // Publish BEFORE the durability work. Journal fsync and snapshot
        // saves can block for seconds on a slow state volume, and the read
        // model the dashboard lives on must not queue behind them — that
        // was the "page shows nothing for a while, then everything jumps"
        // field report.
        let t = Instant::now();
        self.publish_snapshot(rate);
        let publish_ms = t.elapsed().as_millis() as u64;

        let sync_started = Instant::now();
        self.sync_journal();
        let sync_ms = sync_started.elapsed().as_millis() as u64;

        let t = Instant::now();
        let pending = std::mem::take(&mut self.pending_finalize);
        for (j, f) in pending {
            if !self.finalize_sent.contains(&f) {
                self.send_finalize(j, f);
            }
        }
        let finalize_ms = t.elapsed().as_millis() as u64;

        let mut save_ms = 0u64;
        if self.dirty
            && (seed_checkpoint_due || self.last_save.elapsed() > save_spacing(self.last_save_ms))
        {
            let save_started = Instant::now();
            self.save_snapshot();
            save_ms = save_started.elapsed().as_millis() as u64;
        }

        // A tick that runs long stalls everything behind this loop:
        // commands answer late (a delete can time out client-side and its
        // row spring back), lease handout starves (throughput dives), and
        // publishes queue (the UI goes quiet, then jumps). Every section is
        // timed separately — the first field report under this warn had the
        // stall hiding in the ONE untimed section (a statvfs against a
        // saturated destination volume), so nothing here goes unmeasured.
        let total_ms = tick_started.elapsed().as_millis() as u64;
        if total_ms > 1500 {
            tracing::warn!(
                total_ms,
                guards_ms,
                volumes_ms,
                publish_ms,
                journal_sync_ms = sync_ms,
                finalize_ms,
                snapshot_save_ms = save_ms,
                "engine tick ran long — the breakdown names the stall; commands and UI updates queue behind this"
            );
        }
    }

    /// Try retained commands in FIFO order. A full channel leaves its head in
    /// place for the next tick; a closed channel has no executor and is not a
    /// reason to spin or reorder intent.
    fn flush_backend_commands(&mut self) {
        while let Some(command) = self.pending_backend_commands.pop_front() {
            match self.backend.try_command(command) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(command)) => {
                    self.pending_backend_commands.push_front(command);
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => break,
            }
        }
    }

    /// Progress is a latest-value map, never a FIFO input. Folding at the
    /// owner tick keeps peer-stat floods from delaying user controls.
    fn fold_backend_progress(&mut self) {
        let latest = self.backend.latest_progress();
        let mut changed = false;
        for (job_id, progress) in latest {
            let verified_delta = {
                let Some(job) = self.state.job_mut(job_id) else {
                    continue;
                };
                let before = job
                    .torrent
                    .as_ref()
                    .map_or(0, |torrent| torrent.downloaded_bytes);
                changed |=
                    crate::torrent_runtime::reconcile_progress(job, &progress).durable_changed;
                job.torrent
                    .as_ref()
                    .map_or(0, |torrent| torrent.downloaded_bytes.saturating_sub(before))
            };
            if verified_delta > 0 {
                self.volumes.add(
                    crate::volumes::TORRENT_SOURCE_ID,
                    verified_delta,
                    unix_now(),
                    self.tuning.quota_start_day,
                );
            }
            self.torrent_progress.insert(job_id, progress);
        }
        let live: HashSet<JobId> = self.state.jobs.iter().map(|job| job.id).collect();
        self.torrent_progress.retain(|job, _| live.contains(job));
        if changed {
            self.dirty = true;
        }
    }

    fn schedule_torrent_starts(&mut self) {
        let mut unavailable = self.delegated.clone();
        for job in self.pending_control_revisions.keys() {
            unavailable.insert(*job, "control persistence pending".into());
        }
        let mut desired = active_set(
            &self.state,
            &unavailable,
            self.quota_reached || self.disk_low,
            unix_now(),
        )
        .into_iter()
        .collect::<HashSet<_>>();
        // Verified payloads seed outside the download-slot cap. They remain
        // live until their seed policy, an operator pause, or removal stops
        // them.
        desired.extend(self.state.jobs.iter().filter_map(|job| {
            let torrent = job.torrent.as_ref()?;
            (torrent.ready_at_unix.is_some()
                && !self.pending_control_revisions.contains_key(&job.id)
                && torrent.control_intent == TorrentControlIntent::Running
                && torrent.removal_intent.is_none())
            .then_some(job.id)
        }));

        for job_id in desired.iter().copied() {
            let runnable = self.state.job(job_id).is_some_and(|job| {
                job.kind == JobKind::Torrent
                    && !self.pending_control_revisions.contains_key(&job_id)
                    && !self.pending_control_commits.contains(&job_id)
                    && job.status != JobStatus::Paused
                    && job.torrent.as_ref().is_some_and(|torrent| {
                        torrent.control_intent == TorrentControlIntent::Running
                            && torrent.removal_intent.is_none()
                    })
            });
            if !runnable || self.backend_started.contains(&job_id) {
                continue;
            }
            let queued = self.enqueue_backend_command(BackendCommand::Start { job: job_id });
            if queued {
                self.backend_started.insert(job_id);
            }
        }

        // A stalled or lower-priority incomplete torrent must actually yield
        // its backend bandwidth, not merely disappear from the accounting
        // set while continuing to transfer underneath it.
        let yielding = self
            .backend_started
            .iter()
            .copied()
            .filter(|job_id| !desired.contains(job_id))
            .filter(|job_id| {
                self.state.job(*job_id).is_some_and(|job| {
                    job.torrent
                        .as_ref()
                        .is_some_and(|torrent| torrent.ready_at_unix.is_none())
                })
            })
            .collect::<Vec<_>>();
        for job in yielding {
            let queued = self.enqueue_backend_command(BackendCommand::PauseForScheduler { job });
            if queued {
                self.backend_started.remove(&job);
            }
        }
    }

    fn rebalance_download_budget(&mut self, usenet_rate_bps: u64) {
        let active = active_set(
            &self.state,
            &self.delegated,
            self.quota_reached || self.disk_low,
            unix_now(),
        );
        let usenet_active = active.iter().any(|job| {
            self.state
                .job(*job)
                .is_some_and(|job| job.kind != JobKind::Torrent)
        });
        let torrent_jobs = active
            .iter()
            .filter(|job| {
                self.state
                    .job(**job)
                    .is_some_and(|job| job.kind == JobKind::Torrent)
            })
            .copied()
            .collect::<HashSet<_>>();
        let torrent_active = !torrent_jobs.is_empty();
        let torrent_rate_bps = self
            .torrent_progress
            .iter()
            .filter(|(job, _)| torrent_jobs.contains(job))
            .map(|(_, progress)| progress.download_bps)
            .sum();
        let (usenet_limit, torrent_limit) = crate::backend::allocate_download_budget(
            self.state.speed_limit_bps,
            usenet_active,
            torrent_active,
            usenet_rate_bps,
            torrent_rate_bps,
        );
        if self.applied_usenet_limit != Some(usenet_limit) {
            self.limiter.set(usenet_limit);
            self.applied_usenet_limit = Some(usenet_limit);
        }
        if self.applied_torrent_limit == Some(torrent_limit) {
            return;
        }
        let command = BackendCommand::SetDownloadLimit {
            bytes_per_sec: torrent_limit,
        };
        let queued = self.enqueue_backend_command(command);
        if queued {
            self.applied_torrent_limit = Some(torrent_limit);
        }
    }

    /// Accrue only time spent in the live seeding phase and enforce the first
    /// configured ratio/time boundary through the same durable control seam
    /// as an operator pause. Returning `true` requests an immediate snapshot
    /// rather than the normal adaptive debounce.
    fn update_seed_policies(&mut self, now_unix: i64) -> bool {
        let mut changed = false;
        let mut reached = Vec::new();
        let mut live = HashSet::new();

        for job in &mut self.state.jobs {
            let Some(torrent) = job.torrent.as_mut() else {
                continue;
            };
            if torrent.phase != TorrentPhase::Seeding
                || torrent.control_intent != TorrentControlIntent::Running
            {
                continue;
            }
            live.insert(job.id);
            let last = self.seed_clock_unix.entry(job.id).or_insert(now_unix);
            let elapsed = now_unix.saturating_sub(*last) as u64;
            *last = now_unix;
            if elapsed > 0 {
                torrent.seeding_seconds = torrent.seeding_seconds.saturating_add(elapsed);
                changed = true;
            }
            if let Some(reason) = crate::torrent_runtime::seed_policy_stop_reason(torrent) {
                reached.push((job.id, reason));
            }
        }
        self.seed_clock_unix.retain(|job, _| live.contains(job));
        self.dirty |= changed;

        for (job_id, reason) in reached {
            let Some(before) = self.state.job(job_id).cloned() else {
                continue;
            };
            let Some(job) = self.state.job_mut(job_id) else {
                continue;
            };
            job.status = JobStatus::Paused;
            job.torrent.as_mut().unwrap().control_intent = TorrentControlIntent::Paused;
            job.torrent.as_mut().unwrap().stop_reason = Some(reason);
            self.dirty = true;
            if self.persist_then_command(BackendCommand::PauseForSeedPolicy { job: job_id }) {
                self.bump_epoch();
                self.publish_now();
            } else if let Some(job) = self.state.job_mut(job_id) {
                *job = before;
            }
        }

        self.state.jobs.iter().any(|job| {
            let Some(torrent) = &job.torrent else {
                return false;
            };
            let checkpoint =
                self.seed_checkpoints
                    .get(&job.id)
                    .copied()
                    .unwrap_or(SeedCheckpoint {
                        uploaded_bytes: 0,
                        seeding_seconds: 0,
                    });
            crate::torrent_runtime::seed_checkpoint_due(
                torrent,
                checkpoint.uploaded_bytes,
                checkpoint.seeding_seconds,
            )
        })
    }

    fn fold_backend_structural(&mut self) {
        while let Ok(fact) = self.backend.try_structural() {
            match &fact {
                BackendFact::Resumed { job } => {
                    self.backend_started.insert(*job);
                }
                BackendFact::Stopped { job, .. }
                | BackendFact::Removed { job, .. }
                | BackendFact::Failed { job, .. } => {
                    self.backend_started.remove(job);
                }
                BackendFact::MetadataReady { .. } | BackendFact::Ready { .. } => {}
            }
            let job_id = match &fact {
                BackendFact::Removed { job, outcome } => {
                    let disposition = match outcome {
                        RemovalOutcome::DataDeleted => Some(TorrentPayloadDisposition::Deleted),
                        RemovalOutcome::DataKept => Some(TorrentPayloadDisposition::Retained),
                        RemovalOutcome::RefusedUnsafeRoot
                        | RemovalOutcome::RefusedInventoryMismatch => None,
                    };
                    if let Some(disposition) = disposition {
                        if let Some(torrent) = self
                            .state
                            .job_mut(*job)
                            .and_then(|record| record.torrent.as_mut())
                        {
                            torrent.removal_outcome = Some(disposition);
                            torrent.removal_confirmed_at_unix = Some(unix_now());
                            self.dirty = true;
                        }
                        continue;
                    }
                    *job
                }
                BackendFact::MetadataReady { job, .. }
                | BackendFact::Ready { job, .. }
                | BackendFact::Stopped { job, .. }
                | BackendFact::Resumed { job }
                | BackendFact::Failed { job, .. } => *job,
            };
            let terminal_failure = matches!(&fact, BackendFact::Failed { .. });
            let latest = self.torrent_progress.get(&job_id).cloned();
            if let Some(job) = self.state.job_mut(job_id) {
                let outcome = crate::torrent_runtime::reconcile_fact_with_roots(
                    job,
                    fact,
                    latest.as_ref(),
                    unix_now(),
                    &self.torrent_payload_roots,
                );
                self.dirty |= outcome.durable_changed;
                if terminal_failure {
                    if let Some(torrent) = job.torrent.as_mut() {
                        torrent.removal_outcome = Some(TorrentPayloadDisposition::Retained);
                        torrent.removal_confirmed_at_unix = Some(unix_now());
                        self.dirty = true;
                    }
                }
                if outcome.storage_hold {
                    let whence = self
                        .torrent_payload_roots
                        .first()
                        .map(|root| format!("torrent payload write under {}", root.display()))
                        .unwrap_or_else(|| "torrent payload write".to_owned());
                    self.observe_out_of_space(&whence);
                }
            }
        }
    }

    /// Complete the ordered torrent terminal transition:
    ///
    /// backend outcome -> durable queue checkpoint -> durable history ->
    /// queue retirement. A crash at any edge restarts from the persisted
    /// outcome and never repeats payload deletion merely because history was
    /// temporarily unavailable.
    fn finalize_confirmed_torrent_removals(&mut self) {
        let pending = self
            .state
            .jobs
            .iter()
            .filter(|job| {
                !self.pending_control_commits.contains(&job.id)
                    && job
                        .torrent
                        .as_ref()
                        .is_some_and(|torrent| torrent.removal_outcome.is_some())
            })
            .cloned()
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return;
        }

        for job in pending {
            let Some(torrent) = job.torrent.as_ref() else {
                continue;
            };
            let Some(completed_at_unix) = torrent.removal_confirmed_at_unix else {
                tracing::error!(
                    job = job.id.0,
                    "confirmed torrent removal has no stable history timestamp"
                );
                continue;
            };
            let final_dir = matches!(
                torrent.removal_outcome,
                Some(TorrentPayloadDisposition::Retained)
            )
            .then(|| {
                torrent
                    .content_path
                    .as_ref()
                    .map(|path| path.to_string_lossy().into_owned())
            })
            .flatten();
            let entry = nzbd_state::HistoryEntry {
                job: job.id,
                name: job.name.clone(),
                category: job.category.clone(),
                final_dir,
                status: if job.status == JobStatus::Failed {
                    "FAILURE/TORRENT".to_owned()
                } else {
                    "DELETED".to_owned()
                },
                size: torrent.selected_bytes,
                health: 1000,
                params: job.params.clone(),
                dupe_key: job.dupe.key.clone(),
                dupe_score: job.dupe.score,
                completed_at_unix,
                hidden: false,
                first_seen_at_unix: None,
                last_seen_at_unix: None,
                seen_count: 0,
                removed_at_unix: None,
                picked_up_by: None,
                record: Some(nzbd_state::JobRecord::from_job(&job)),
                stages: job.stages.clone(),
                seq: 0,
            };

            let job_id = job.id;
            let epoch = self.job_epochs.get(&job_id).copied();
            self.pending_control_commits.insert(job_id);
            self.persist_then(move|owner,committed| {
                if !committed || owner.job_epochs.get(&job_id).copied()!=epoch {
                    owner.pending_control_commits.remove(&job_id);return;
                }
                let history=owner.history.clone();
                owner.io_then(move||{
                    if let Some(history)=history{history.record_seq_durable(&entry).map_err(|error|nzbd_state::StateError::Corrupt(error.to_string()))?;}
                    Ok(())
                },move|owner,result|{
                    owner.pending_control_commits.remove(&job_id);
                    if owner.job_epochs.get(&job_id).copied()!=epoch{return;}
                    if result.is_ok(){owner.delete_job_then(job_id,false,|_,_|{});}
                    else if let Err(error)=result{tracing::warn!(job=job_id.0,%error,"confirmed torrent remains queued until history is durable");}
                });
            });
        }
    }

    /// Persist a torrent's requested state before placing its matching backend
    /// command on the bounded FIFO. A failed persistence attempt is restored
    /// by the caller, so an unrecorded request cannot become idempotently
    /// stuck or reach the engine.
    fn persist_then_command(&mut self, command: BackendCommand) -> bool {
        if self.pending_backend_commands.len() >= MAX_PENDING_BACKEND_COMMANDS {
            return false;
        }
        let job = match &command {
            BackendCommand::Start { job }
            | BackendCommand::Pause { job }
            | BackendCommand::PauseForSeedPolicy { job }
            | BackendCommand::PauseForStorage { job }
            | BackendCommand::PauseForScheduler { job }
            | BackendCommand::Resume { job }
            | BackendCommand::Remove { job, .. }
            | BackendCommand::SetPriority { job, .. } => Some(*job),
            _ => None,
        };
        let epoch = job.and_then(|job| self.job_epochs.get(&job).copied());
        let control_epoch = job.and_then(|job| self.control_epochs.get(&job).copied());
        let requested = job
            .and_then(|job| self.state.job(job))
            .and_then(|record| record.torrent.as_ref())
            .cloned();
        let policy_pause = matches!(command, BackendCommand::PauseForSeedPolicy { .. });
        self.persist_then(move |owner, ok| {
            let lifetime_current = job.is_none_or(|job| {
                owner.state.job(job).is_some() && owner.job_epochs.get(&job).copied() == epoch
            });
            let control_current =
                job.is_none_or(|job| owner.control_epochs.get(&job).copied() == control_epoch);
            let desired_current = job.is_none_or(|job| {
                match (
                    owner
                        .state
                        .job(job)
                        .and_then(|record| record.torrent.as_ref()),
                    requested.as_ref(),
                ) {
                    (Some(current), Some(requested)) => {
                        current.control_intent == requested.control_intent
                            && current.stop_reason == requested.stop_reason
                            && current.removal_intent == requested.removal_intent
                            && current.seed_policy == requested.seed_policy
                    }
                    (None, None) => true,
                    _ => false,
                }
            });
            if lifetime_current
                && control_current
                && desired_current
                && ok
                && !owner.cancel.is_cancelled()
                && owner.enqueue_backend_command(command)
            {
                return;
            }
            // The only automatic caller is seed policy. A failed/stale barrier
            // must make that policy eligible for retry, while preserving every
            // newer user control, byte counter, receipt, category and stage.
            if policy_pause && lifetime_current && desired_current {
                if let Some(job) = job {
                    if let Some(record) = owner.state.job_mut(job) {
                        if let Some(torrent) = record.torrent.as_mut() {
                            if torrent.control_intent == TorrentControlIntent::Paused
                                && torrent.phase == TorrentPhase::Seeding
                            {
                                torrent.control_intent = TorrentControlIntent::Running;
                                torrent.stop_reason = None;
                                if record.status == JobStatus::Paused {
                                    record.status = JobStatus::Completed;
                                }
                                owner.dirty = true;
                            }
                        }
                    }
                }
                owner.publish_now();
                owner.bump_epoch();
            }
            if !ok {
                tracing::warn!(
                    "backend policy control remains eligible for retry after persistence recovers"
                );
            }
        });
        true
    }

    /// Preserve the one backend FIFO's ordering even when a previous send had
    /// to be retained locally. Scheduler, policy, and operator commands all
    /// pass through this seam; none may jump ahead by writing directly to the
    /// channel while the retained queue is non-empty.
    fn enqueue_backend_command(&mut self, command: BackendCommand) -> bool {
        if !self.pending_backend_commands.is_empty() {
            if self.pending_backend_commands.len() == MAX_PENDING_BACKEND_COMMANDS {
                return false;
            }
            self.pending_backend_commands.push_back(command);
            return true;
        }
        match self.backend.try_command(command) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(command)) => {
                if self.pending_backend_commands.len() == MAX_PENDING_BACKEND_COMMANDS {
                    return false;
                }
                self.pending_backend_commands.push_back(command);
                true
            }
            // The durable request remains authoritative if a runtime has
            // stopped; a later restart can converge it rather than silently
            // rolling back a successfully saved operator action.
            Err(mpsc::error::TrySendError::Closed(_)) => true,
        }
    }

    fn publish_now(&mut self) {
        self.repair_progress.retain(|id, _| {
            self.state.job(*id).is_some_and(|job| {
                !job.held()
                    && !job
                        .params
                        .iter()
                        .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
            })
        });
        let rate = self
            .server_wire_ema
            .values()
            .map(|e| e.max(0.0) as u64)
            .sum();
        self.publish_snapshot(rate);
    }

    /// Publish the recovered queue into the shared snapshot *before* the
    /// async run loop is scheduled, so the very first API read reflects
    /// real state — otherwise `/api/v1/jobs` can serve the empty initial
    /// snapshot in the startup window and the UI flashes "queue is empty".
    pub(crate) fn seed_snapshot(&mut self) {
        self.publish_snapshot(0);
    }

    /// URL jobs recovered in `Fetching`: their fetch tasks lived in the
    /// previous process and died with it, so without a re-spawn they sit at
    /// "FETCHING · 0 B of 0 B" forever. [`crate::Engine::spawn`] feeds this
    /// list through [`plan_url_refetches`] right after recovery.
    pub(crate) fn pending_url_fetches(&self) -> Vec<UrlFetchRequest> {
        self.state
            .jobs
            .iter()
            .filter(|j| matches!(j.status, JobStatus::Fetching))
            .filter_map(|j| {
                j.params
                    .iter()
                    .find(|(k, _)| k == "*URL")
                    .map(|(_, url)| UrlFetchRequest {
                        job: j.id,
                        url: url.clone(),
                        request_token: j
                            .params
                            .iter()
                            .find(|(key, _)| key == "monarr-transfer")
                            .map(|(_, value)| value.clone()),
                        manual_additional: j
                            .params
                            .iter()
                            .any(|(key, value)| key == "monarr-manual-additional" && value == "1"),
                    })
            })
            .collect()
    }

    fn publish_snapshot(&mut self, rate: u64) {
        let jobs = self
            .state
            .jobs
            .iter()
            .map(|j| {
                let health = Health::calc(&j.totals);
                let critical = Health::calc_critical(&j.totals, true);
                let allowed_files = self.post_fetch_files.get(&j.id);
                let remaining: u64 = j
                    .files
                    .iter()
                    .filter(|f| {
                        !f.paused && allowed_files.is_none_or(|allowed| allowed.contains(&f.id))
                    })
                    .flat_map(|f| &f.segments)
                    .filter(|s| {
                        matches!(s.state, SegmentState::Pending | SegmentState::Leased { .. })
                    })
                    .map(|s| s.size as u64)
                    .sum();
                let mut summary = JobSummary {
                    verification: self
                        .verification
                        .get(&j.id)
                        .map(VerificationState::progress),
                    repair_progress: if !j.held()
                        && !j
                            .params
                            .iter()
                            .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
                    {
                        self.repair_progress
                            .get(&j.id)
                            .and_then(|(_, progress)| progress.clone())
                    } else {
                        None
                    },
                    control: j.control(),
                    lifecycle_eligible: j.lifecycle_eligible(),
                    url_fetch: crate::snapshot::url_fetch_progress(j),
                    failed_cleanup: crate::snapshot::failed_cleanup_progress(j),
                    post_timing: crate::snapshot::post_timing(j, crate::snapshot::unix_now()),
                    id: j.id,
                    kind: j.kind,
                    name: j.name.clone(),
                    status: j.status,
                    category: j.category.clone(),
                    priority: j.priority,
                    size_bytes: j.totals.size,
                    downloaded_bytes: j.totals.success_size,
                    failed_bytes: j.totals.failed_size,
                    remaining_bytes: remaining,
                    total_articles: j.totals.total_articles,
                    done_articles: j.totals.success_articles,
                    failed_articles: j.totals.failed_articles,
                    files_total: j.files.len() as u32,
                    files_done: j.files.iter().filter(|f| f.is_terminal()).count() as u32,
                    health: health.0,
                    critical_health: critical.0,
                    critical_health_estimated: Health::calc_critical(&j.totals, false) != critical,
                    assigned_node: self.delegated.get(&j.id).cloned(),
                    pp_done: j.params.iter().any(|(k, _)| k == nzbd_types::PP_DONE_PARAM),
                    ready: j.ready(),
                    ready_at_unix: j.ready_at_unix(),
                    torrent_phase: j.torrent.as_ref().map(|t| t.phase),
                    torrent_control_intent: j.torrent.as_ref().map(|t| t.control_intent),
                    seed_policy: j.torrent.as_ref().map(|t| t.seed_policy),
                    seed_stop_reason: j.torrent.as_ref().and_then(|t| t.stop_reason),
                    torrent_error: j.torrent.as_ref().and_then(|t| t.last_error.clone()),
                    uploaded_bytes: j
                        .torrent
                        .as_ref()
                        .map_or(0, |torrent| torrent.uploaded_bytes),
                    upload_rate_bps: self
                        .torrent_progress
                        .get(&j.id)
                        .map_or(0, |progress| progress.upload_bps),
                    ratio: j.torrent.as_ref().map_or(0.0, |torrent| {
                        if torrent.selected_bytes == 0 {
                            0.0
                        } else {
                            torrent.uploaded_bytes as f64 / torrent.selected_bytes as f64
                        }
                    }),
                    seeding_seconds: j
                        .torrent
                        .as_ref()
                        .map_or(0, |torrent| torrent.seeding_seconds),
                    useful_peers: self
                        .torrent_progress
                        .get(&j.id)
                        .map_or(0, |progress| progress.useful_peers),
                    dupe_key: j.dupe.key.clone(),
                    dupe_score: j.dupe.score,
                    params: j
                        .params
                        .iter()
                        .filter(|(k, _)| !k.starts_with('*'))
                        .cloned()
                        .collect(),
                    rate_bps: 0,
                    retried_articles: 0,
                    stages: j.stages.clone(),
                };
                if let Some(torrent) = &j.torrent {
                    summary.size_bytes = torrent.selected_bytes;
                    summary.downloaded_bytes = torrent.downloaded_bytes.min(torrent.selected_bytes);
                    summary.remaining_bytes = torrent
                        .selected_bytes
                        .saturating_sub(summary.downloaded_bytes);
                    summary.files_total =
                        torrent.files.iter().filter(|file| file.selected).count() as u32;
                    summary.files_done = torrent
                        .files
                        .iter()
                        .filter(|file| file.selected && file.downloaded_bytes >= file.length)
                        .count() as u32;
                }
                // Delegated jobs progress remotely; overlay heartbeat stats.
                if let Some(m) = self.mirror.get(&j.id) {
                    summary.repair_progress = if !j.held() && !summary.pp_done {
                        m.repair_progress.clone()
                    } else {
                        None
                    };
                    summary.done_articles = m.done_articles;
                    summary.failed_articles = m.failed_articles;
                    summary.downloaded_bytes = m.downloaded_bytes;
                    summary.remaining_bytes = match m.remaining_bytes {
                        Some(exact) => exact,
                        None if matches!(
                            summary.status,
                            JobStatus::Queued | JobStatus::Downloading | JobStatus::Paused
                        ) =>
                        {
                            summary
                                .size_bytes
                                .saturating_sub(m.downloaded_bytes)
                                .saturating_sub(summary.failed_bytes)
                        }
                        // Old workers do not report exact remaining bytes.
                        // During remote PP the authority's paused-aware zero
                        // is more truthful than treating every delayed volume
                        // as outstanding download work.
                        None => summary.remaining_bytes,
                    };
                    summary.health = m.health;
                    // Remote PP is intentionally an overlay, not a status
                    // mutation. The open span gives native clients the live
                    // stage while the authority remains Completed for lease
                    // accounting and failover adoption.
                    if !m.stages.is_empty() {
                        summary.stages = m.stages.clone();
                    }
                    if m.done_articles > 0 && summary.status == JobStatus::Queued {
                        summary.status = JobStatus::Downloading;
                    }
                }
                if self.verification.contains_key(&j.id)
                    || self.retiring_writers.contains_key(&j.id)
                    || self.pending_control_commits.contains(&j.id)
                {
                    summary.ready = false;
                    if j.status != JobStatus::Paused && !j.held() {
                        summary.status = JobStatus::Queued;
                    }
                }
                summary
            })
            .collect::<Vec<_>>();
        let jobs: Vec<JobSummary> = {
            let mut jobs = jobs;
            for summary in &mut jobs {
                let meter = self
                    .job_rates
                    .entry(summary.id.0)
                    .or_insert_with(|| JobRateMeter {
                        last_bytes: summary.downloaded_bytes,
                        last_at: std::time::Instant::now(),
                        ema_bps: 0.0,
                    });
                summary.rate_bps = if summary.kind == JobKind::Torrent {
                    self.torrent_progress
                        .get(&summary.id)
                        .map_or(0, |progress| progress.download_bps)
                } else if summary.status == JobStatus::Downloading {
                    // Local jobs: wire-fed EMA — the SAME bytes the header
                    // meter counts, attributed per job, so the row rate and
                    // the header rate can never structurally disagree
                    // (completed-article deltas lag the wire by whatever is
                    // being retried, which read as a stale, slower row).
                    // Delegated jobs have no local wire; their heartbeat
                    // byte deltas keep using the completed-bytes meter.
                    match self.job_wire_ema.get(&summary.id.0) {
                        Some(ema) if summary.assigned_node.is_none() => ema.max(0.0) as u64,
                        _ => meter.update(summary.downloaded_bytes),
                    }
                } else {
                    // Reset the baseline so a resume doesn't spike.
                    meter.last_bytes = summary.downloaded_bytes;
                    meter.ema_bps = 0.0;
                    0
                };
                summary.retried_articles =
                    self.retry_counts.get(&summary.id.0).copied().unwrap_or(0);
            }
            let live: HashSet<u32> = jobs.iter().map(|s| s.id.0).collect();
            self.job_rates.retain(|id, _| live.contains(id));
            self.job_wire_ema.retain(|id, _| live.contains(id));
            self.retry_counts.retain(|id, _| live.contains(id));
            jobs
        };
        // Summaries include executor heartbeat overlays, so the queue-wide
        // figure must be derived from them too. Reading authority state here
        // would drop remote delayed-PAR bytes because those files are only
        // unpaused on the PP executor.
        let remaining_bytes = jobs.iter().map(|job| job.remaining_bytes).sum();
        let torrent_rate: u64 = jobs
            .iter()
            .filter(|job| job.kind == JobKind::Torrent)
            .map(|job| job.rate_bps)
            .sum();
        let now_block = Instant::now();
        let mut blocked_servers: Vec<u32> = self
            .blocked
            .iter()
            .filter(|(_, until)| **until > now_block)
            .map(|(id, _)| id.0)
            .collect();
        blocked_servers.sort_unstable();
        let disk_guard = &self.evaluated_disk_guard;
        let snap = QueueSnapshot {
            up_since_unix: self.up_since_unix,
            queue_authority: self.persist,
            download_paused: self.state.download_paused,
            quota_reached: self.quota_reached,
            disk_low: self.disk_low,
            disk_guard_free_bytes: disk_guard.available_bytes,
            disk_guard_label: disk_guard.limiting_label.clone(),
            disk_guard_path: disk_guard
                .limiting_path
                .as_ref()
                .map(|path| path.to_string_lossy().into_owned()),
            disk_guard_write_latched: self.enospc_latched,
            disk_guard_all_roots_known: disk_guard.all_roots_known,
            storage_volumes: disk_guard
                .volumes
                .iter()
                .map(|volume| StorageVolumeSnapshot {
                    label: volume.label.clone(),
                    path: volume.path.to_string_lossy().into_owned(),
                    available_bytes: volume.available_bytes,
                    total_bytes: volume.total_bytes,
                    current: volume.current,
                })
                .collect(),
            enospc_observed: self.enospc_observed,
            enospc_where: self.enospc_where.clone(),
            blocked_servers,
            health_abort: self.tuning.health_abort,
            server_volumes: {
                let now_day = unix_now().div_euclid(86_400);
                let name_of = |id: u32| {
                    if id == crate::volumes::TORRENT_SOURCE_ID.0 {
                        return "BitTorrent".to_owned();
                    }
                    self.servers
                        .iter()
                        .find(|s| s.id.0 == id)
                        .map(|s| s.name.clone())
                        .unwrap_or_else(|| format!("server {id}"))
                };
                // Every configured server appears, even one that has never
                // moved a byte: "which provider is quiet?" is exactly the
                // question these rows exist to answer.
                let mut ids: std::collections::BTreeSet<u32> =
                    self.servers.iter().map(|s| s.id.0).collect();
                ids.extend(self.volumes.doc().servers.keys().copied());
                ids.extend(self.server_wire_ema.keys().copied());
                if jobs.iter().any(|job| job.kind == JobKind::Torrent) {
                    ids.insert(crate::volumes::TORRENT_SOURCE_ID.0);
                }
                let mut v: Vec<crate::snapshot::ServerVolume> = ids
                    .into_iter()
                    .map(|id| {
                        let w = self.volumes.doc().servers.get(&id).cloned();
                        crate::snapshot::ServerVolume {
                            server: id,
                            name: name_of(id),
                            total_bytes: w.as_ref().map(|w| w.total_bytes).unwrap_or(0),
                            day_bytes: w
                                .as_ref()
                                .filter(|w| w.day_key == now_day)
                                .map(|w| w.day_bytes)
                                .unwrap_or(0),
                            month_bytes: w.as_ref().map(|w| w.month_bytes).unwrap_or(0),
                            rate_bps: if id == crate::volumes::TORRENT_SOURCE_ID.0 {
                                torrent_rate
                            } else {
                                self.server_wire_ema
                                    .get(&id)
                                    .map(|e| e.max(0.0) as u64)
                                    .unwrap_or(0)
                            },
                        }
                    })
                    .collect();
                v.sort_by_key(|x| x.server);
                v
            },
            speed_limit_bps: self.state.speed_limit_bps,
            max_active_downloads: self.state.max_active_downloads,
            download_rate_bps: rate.saturating_add(torrent_rate),
            session_downloaded_bytes: self.meter.total(),
            remaining_bytes,
            jobs,
        };
        self.shared.store(Arc::new(snap));
    }

    fn save_snapshot(&mut self) {
        if self.background_save_pending {
            return;
        }
        self.background_save_pending = true;
        let checkpoints = durable_seed_checkpoints(&self.state);
        self.persist_then(move |owner, ok| {
            owner.background_save_pending = false;
            if ok {
                owner.seed_checkpoints = checkpoints;
            }
        });
    }

    fn on_snapshot_save_error(&mut self, error: &nzbd_state::StateError) {
        tracing::error!(%error,"persistence failed; durable operations remain blocked");
        if matches!(error, nzbd_state::StateError::Corrupt(_)) {
            self.persist = false;
        }
    }

    fn emit(&self, ev: Event) {
        let _ = self.events.send(ev);
    }

    fn bump_epoch(&self) {
        self.epoch_tx.send_modify(|v| *v += 1);
    }
}

/// Replay retains unknown evidence; only matching incarnation provenance can
/// extend a current proof. An older generation can never resurrect retired bits.
fn merge_replayed_proof(
    state: &mut QueueState,
    record: &JournalRecord,
    raw: Option<&serde_json::Value>,
    installation: &str,
) -> bool {
    let Some(job) = state.job_mut(record.job) else {
        return false;
    };
    let Some(proof) =
        raw.and_then(|raw| serde_json::from_value::<DurableFileProof>(raw.clone()).ok())
    else {
        return true;
    };
    let generation_key = format!("*File:generation:{}", record.file.0);
    if job
        .params
        .iter()
        .find(|(key, _)| key == &generation_key)
        .is_some_and(|(_, generation)| generation != &proof.generation)
    {
        return false;
    }
    let range = ProvenRange {
        seg_number: record.segment_number,
        offset: record.offset,
        len: record.len,
        crc: record.crc32,
    };
    if proof.protocol != WRITER_PROTOCOL
        || proof.installation != installation
        || !proof.ranges.contains(&range)
        || proof.generation.is_empty()
        || proof.allocation_generation.is_empty()
    {
        return true;
    }
    let proof_key = format!("*File:proof:{}", record.file.0);
    let existing = job
        .params
        .iter()
        .find(|(key, _)| key == &proof_key)
        .and_then(|(_, raw)| serde_json::from_str::<DurableFileProof>(raw).ok());
    let merged = if let Some(mut existing) = existing {
        if existing.generation != proof.generation
            || existing.installation != proof.installation
            || existing.allocation_generation != proof.allocation_generation
            || !existing.identity.same_object(&proof.identity)
        {
            set_owner_param(&mut job.params, &proof_key, None);
            return true;
        }
        for range in &proof.ranges {
            if existing.ranges.contains(range) {
                continue;
            }
            if existing
                .ranges
                .iter()
                .any(|old| old.seg_number == range.seg_number || old.overlaps(range))
            {
                set_owner_param(&mut job.params, &proof_key, None);
                return true;
            }
            existing.ranges.push(range.clone());
        }
        existing.identity = proof.identity;
        existing
    } else {
        proof
    };
    set_owner_param(
        &mut job.params,
        &generation_key,
        Some(merged.generation.clone()),
    );
    set_owner_param(
        &mut job.params,
        &proof_key,
        serde_json::to_string(&merged).ok(),
    );
    true
}

fn refresh_url_admission_custody(
    receipt: &mut UrlAdmissionReceipt,
    inventory: &nzbd_state::artifacts::Inventory,
) -> bool {
    let Some((artifact_id, source_generation)) = receipt
        .source_artifact_id
        .as_deref()
        .zip(receipt.source_generation.as_deref())
    else {
        return false;
    };
    let Ok(artifact) = inventory.get(artifact_id) else {
        return false;
    };
    if artifact.generation != source_generation {
        return false;
    }
    let keep_changed = receipt.keep_source_on_failure != artifact.keep_on_failure;
    receipt.keep_source_on_failure = artifact.keep_on_failure;
    if artifact.state == "deleted" {
        let changed = receipt.custody_state != "deleted_by_runner"
            || receipt.source_cleanup_status.as_deref() != Some("removed")
            || keep_changed;
        receipt.custody_state = "deleted_by_runner".into();
        receipt.source_cleanup_status = Some("removed".into());
        changed
    } else if artifact.keep || artifact.keep_on_failure {
        let changed = receipt.custody_state != "kept_by_user"
            || receipt.source_cleanup_status.as_deref() != Some("retained")
            || keep_changed;
        receipt.custody_state = "kept_by_user".into();
        receipt.source_cleanup_status = Some("retained".into());
        changed
    } else if artifact.owned && receipt.custody_state != "curator" {
        let changed = receipt.custody_state != "runner_owned" || keep_changed;
        receipt.custody_state = "runner_owned".into();
        changed
    } else {
        false
    }
}

/// How long after a save the next debounced save may run: 10× the last
/// save's duration, floored at the historical 2 s, capped at 5 min. A
/// fast volume keeps the old cadence; a slow one gets its saves spaced so
/// the owner loop spends at most ~10% of wall time blocked in them
/// (field report 2026-07-26: 65 s saves fired back-to-back and consumed
/// the loop — commands, publishes and the dashboard all queued behind
/// storage). Stretching the cadence risks no data: every completed
/// segment is in the fsync'd journal, and recovery is snapshot + replay.
/// Structural saves (delete, import) stay immediate — they don't come
/// through the debounce.
fn save_spacing(last_save_ms: u64) -> Duration {
    Duration::from_millis(last_save_ms.saturating_mul(10).clamp(2_000, 300_000))
}

/// Combine per-segment CRCs into the whole-file CRC. Requires contiguous
/// coverage from offset 0 (yEnc parts are contiguous by construction).
fn combine_crcs(sorted: &[(u64, u32, u32)]) -> Option<u32> {
    let mut expect = 0u64;
    let mut acc: Option<u32> = None;
    for (off, len, crc) in sorted {
        if *off != expect {
            return None;
        }
        expect += *len as u64;
        acc = Some(match acc {
            None => *crc,
            Some(prev) => nzbd_yenc::crc32_combine(prev, *crc, *len as u64),
        });
    }
    acc
}

pub(crate) fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// `control` re-derived from the table, or `None` when it already agrees.
/// Runs on every load of authority state (boot and cluster adoption), so a
/// hold written by an older build — or whose policy this build changed —
/// gets the recovery its cause earns:
///
/// 1. An `io` hold whose own message carries a storage errno is reclassified
///    `storage_unavailable` — the evidence was on the hold all along.
/// 2. The policy is re-derived; an automatic timed hold with no time is due
///    `now`.
///
/// The revision moves so a client holding the old one re-reads.
pub(crate) fn rearmed_hold(
    mut control: nzbd_types::JobControl,
    now: i64,
) -> Option<nzbd_types::JobControl> {
    let mut cause = control.hold_cause();
    if cause == nzbd_types::HoldCause::Io && crate::is_storage_unavailable(&control.message) {
        cause = nzbd_types::HoldCause::StorageUnavailable;
    }
    let policy = cause.policy();
    let unscheduled = policy.timed() && control.next_retry_at_unix.is_none();
    if control.cause == cause.as_str() && control.retry_policy == policy.as_str() && !unscheduled {
        return None;
    }
    control.revision = control
        .revision
        .parse::<u64>()
        .ok()?
        .checked_add(1)?
        .to_string();
    control.cause = cause.as_str().into();
    control.retry_policy = policy.as_str().into();
    control.next_retry_at_unix = if policy.timed() {
        control.next_retry_at_unix.or(Some(now))
    } else {
        None
    };
    Some(control)
}

/// An allocation that failed because the volume stopped answering retries on
/// its own; any other allocation refusal waits for space or a person.
fn allocation_hold_cause(message: &str) -> nzbd_types::HoldCause {
    if crate::is_storage_unavailable(message) {
        nzbd_types::HoldCause::StorageUnavailable
    } else {
        nzbd_types::HoldCause::Allocation
    }
}

/// Apply [`rearmed_hold`] to every held job; returns how many changed.
pub(crate) fn rearm_holds(state: &mut QueueState, now: i64) -> usize {
    let mut changed = 0;
    for j in &mut state.jobs {
        if let Some(control) = j
            .control()
            .filter(|c| c.version == 1 && c.lifecycle == "held")
            .and_then(|c| rearmed_hold(c, now))
        {
            tracing::info!(job = j.id.0, cause = %control.cause, policy = %control.retry_policy, "re-armed held job");
            j.set_control(&control);
            changed += 1;
        }
    }
    changed
}

/// Who is ending a hold. Only an operator clears a manual pause and resets
/// the backoff; only the recovery task is refused on a paused hold and on a
/// node that is not the queue authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryBy {
    Operator,
    Recovery,
}

fn url_submission_sha256(
    name: &str,
    url: &str,
    category: Option<&str>,
    priority: i32,
    dupe: Option<&nzbd_types::DupeInfo>,
    paused: bool,
    params: &[(String, String)],
) -> String {
    let mut canonical_params: Vec<_> = params
        .iter()
        .filter(|(key, _)| key != "monarr-transfer" && key != CLIENT_PARAM)
        .collect();
    canonical_params.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    let bytes = serde_json::to_vec(&(
        name,
        url,
        category,
        priority,
        dupe,
        paused,
        canonical_params,
    ))
    .expect("canonical URL admission fields serialize");
    format!("{:x}", Sha256::digest(bytes))
}

fn url_admission_generation(job: JobId, token: &str, instance: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    format!("{}-url-{}-{:x}", instance, job.0, digest)
}

fn job_status_name(status: JobStatus) -> &'static str {
    match status {
        JobStatus::Fetching => "fetching",
        JobStatus::Queued => "queued",
        JobStatus::Downloading => "downloading",
        JobStatus::Paused => "paused",
        JobStatus::PostQueued => "post_queued",
        JobStatus::Post { .. } => "post_processing",
        JobStatus::Completed => "completed",
        JobStatus::Failed => "failed",
        JobStatus::Deleted => "deleted",
    }
}

/// A recovered-`Fetching` job that duplicates another job's URL: `job`
/// should fail as a duplicate of `original`.
pub(crate) type DupeOf = (JobId, JobId);

fn find_url_request<'a>(
    jobs: &'a [Job],
    url: &str,
    request_token: Option<&str>,
    manual_additional: bool,
) -> Option<&'a Job> {
    jobs.iter().find(|job| {
        let saved_token = job
            .params
            .iter()
            .find(|(key, _)| key == "monarr-transfer")
            .map(|(_, value)| value.as_str());
        if let Some(token) = request_token {
            saved_token == Some(token)
        } else if manual_additional {
            false
        } else {
            saved_token.is_none()
                && matches!(job.status, JobStatus::Fetching)
                && job
                    .params
                    .iter()
                    .any(|(key, value)| key == "*URL" && value == url)
        }
    })
}

fn url_request_matches(job: &Job, url: &str, manual_additional: bool, delivery: bool) -> bool {
    let saved_url = job
        .params
        .iter()
        .find(|(key, _)| key == "*URL")
        .map(|(_, value)| value.as_str());
    let saved_additional = job
        .params
        .iter()
        .any(|(key, value)| key == "monarr-manual-additional" && value == "1");
    let saved_delivery = job
        .params
        .iter()
        .any(|(key, value)| key == "monarr-delivery" && value == "1");
    saved_url == Some(url) && saved_additional == manual_additional && saved_delivery == delivery
}

#[derive(Clone, Debug)]
pub(crate) struct UrlFetchRequest {
    pub job: JobId,
    pub url: String,
    pub request_token: Option<String>,
    pub manual_additional: bool,
}

/// Split recovered `Fetching` jobs into fetches to re-spawn and duplicates
/// to fail. The first job (queue order) of each URL refetches; later
/// same-URL jobs — the pile-up left by clients re-adding while the original
/// fetch was invisible after a restart — fail as duplicates of it, instead
/// of N copies of the same download racing to completion.
#[cfg(test)]
pub(crate) fn plan_url_refetches(
    pending: Vec<(JobId, String)>,
) -> (Vec<(JobId, String)>, Vec<DupeOf>) {
    let mut first_for_url: HashMap<String, JobId> = HashMap::new();
    let mut refetch = Vec::new();
    let mut duplicates = Vec::new();
    for (job, url) in pending {
        match first_for_url.get(&url) {
            None => {
                first_for_url.insert(url.clone(), job);
                refetch.push((job, url));
            }
            Some(original) => duplicates.push((job, *original)),
        }
    }
    (refetch, duplicates)
}

/// Refetch recovered URL requests while preserving explicit request identity.
/// Distinct transfer tokens for one URL represent deliberate separate grabs;
/// an additional request without a token is never collapsed into another job.
pub(crate) fn plan_url_refetches_scoped(
    pending: Vec<UrlFetchRequest>,
) -> (Vec<(JobId, String)>, Vec<DupeOf>) {
    let mut first_for_request: HashMap<(String, Option<String>), JobId> = HashMap::new();
    let mut refetch = Vec::new();
    let mut duplicates = Vec::new();
    for request in pending {
        if request.manual_additional && request.request_token.is_none() {
            refetch.push((request.job, request.url));
            continue;
        }
        let key = if let Some(token) = request.request_token.clone() {
            ("token".to_string(), Some(token))
        } else {
            (request.url.clone(), None)
        };
        match first_for_request.get(&key) {
            None => {
                first_for_request.insert(key, request.job);
                refetch.push((request.job, request.url));
            }
            Some(original) => duplicates.push((request.job, *original)),
        }
    }
    (refetch, duplicates)
}

/// The recovery-set name a par2 file implies, or `None` if this file is
/// not a par2 file or its packets name nothing useful.
///
/// Reads the head first so that the overwhelmingly common case — a payload
/// file, not a par2 file — costs 8 bytes rather than a whole rar volume.
fn read_par2_name(path: &Path) -> Option<String> {
    use std::io::Read as _;
    let mut f = std::fs::File::open(path).ok()?;
    let mut head = [0u8; 8];
    let mut got = 0;
    while got < head.len() {
        match f.read(&mut head[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(_) => return None,
        }
    }
    if !nzbd_par2::is_par2(&head[..got]) {
        return None;
    }
    // par2 index files are small (job #182's was 20.5 KiB). Cap the read
    // anyway: a recovery VOLUME carries the same FileDesc packets up front
    // and can be hundreds of MiB, and we only ever want its header.
    const MAX: u64 = 8 * 1024 * 1024;
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .ok()?
        .take(MAX)
        .read_to_end(&mut bytes)
        .ok()?;
    crate::queue::name_from_par2(&nzbd_par2::scan(&bytes).descs)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Execute genuine ordered persistence/IO completions before observing a
    /// direct Owner fixture. The worker remains asynchronous and its bounded
    /// result channel is drained exactly as the production owner loop does.
    pub(super) async fn pump(owner: &mut Owner, messages: &mut mpsc::Receiver<EngineMsg>) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                while let Ok(message) = messages.try_recv() { owner.on_msg(message); }
                if owner.persistence.in_flight() == 0 && owner.persistence_callbacks.is_empty() && owner.io_callbacks.is_empty() {
                    tokio::task::yield_now().await;
                    match messages.try_recv() {
                        Ok(message) => { owner.on_msg(message); continue; },
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => panic!("fixture result channel disconnected before worker completion"),
                    }
                }
                tokio::select! {
                    message = messages.recv() => owner.on_msg(message.expect("fixture worker result channel")),
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {},
                }
            }
        }).await.expect("real persistence/IO worker did not reach an idle acknowledgement boundary");
    }

    async fn allocate_for_test(
        owner: &mut Owner,
        messages: &mut mpsc::Receiver<EngineMsg>,
        job: JobId,
        fresh: bool,
    ) {
        let first = if fresh {
            owner.allocate_new_payload(job)
        } else {
            owner.ensure_allocation(job, false)
        };
        assert!(
            first.is_ok() || owner.allocating_jobs.contains(&job),
            "allocation was refused rather than scheduled: {first:?}"
        );
        pump(owner, messages).await;
        let completed = if fresh {
            owner.allocate_new_payload(job)
        } else {
            owner.ensure_allocation(job, false)
        };
        completed
            .expect("real allocation callback must establish the requested fixture allocation");
        pump(owner, messages).await;
    }

    /// Drive prerequisites through real actor completions until a work
    /// request has a definitive answer; loading is not a successful lease.
    async fn grant_work_for_test(
        owner: &mut Owner,
        messages: &mut mpsc::Receiver<EngineMsg>,
        server: ServerId,
        max: usize,
    ) -> Vec<Lease> {
        for _ in 0..(owner.state.jobs.len().saturating_mul(3) + 4) {
            let leases = owner.grant_work(server, max);
            let pending = owner.persistence.in_flight() != 0
                || !owner.persistence_callbacks.is_empty()
                || !owner.io_callbacks.is_empty();
            pump(owner, messages).await;
            if !leases.is_empty() || !pending {
                return leases;
            }
        }
        panic!("work prerequisites did not reach a definitive fixture transition");
    }

    #[tokio::test]
    async fn blocked_io_lane_preserves_actor_reads_and_noop_controls_without_faking_durable_pause()
    {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        let mut checking = pending_job(1);
        checking.status = JobStatus::Paused;
        owner.state.jobs.push(checking);
        owner.state.jobs.push(pending_job(2));
        owner.queue_verification(JobId(1));
        let before = serde_json::to_value(owner.state.to_doc()).unwrap();
        let verification_before =
            serde_json::to_value(owner.verification[&JobId(1)].progress()).unwrap();
        let (started, started_rx) = oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let completion = completed.clone();
        owner.io_then(
            move || {
                let _ = started.send(());
                held.recv().expect("explicit IO release barrier");
                Ok(())
            },
            move |_, result| {
                result.expect("real blocked worker result");
                completion.store(true, std::sync::atomic::Ordering::Release);
            },
        );
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .unwrap()
            .unwrap();

        let (reply, snapshot) = oneshot::channel();
        owner.on_command(QueueCommand::RecoveryDocument { reply });
        let document = tokio::time::timeout(Duration::from_millis(100), snapshot)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(serde_json::to_value(document).unwrap(), before);
        let (reply, noop) = oneshot::channel();
        owner.on_command(QueueCommand::Pause {
            job: JobId(1),
            reply,
        });
        assert!(!tokio::time::timeout(Duration::from_millis(100), noop)
            .await
            .unwrap()
            .unwrap());
        assert_eq!(serde_json::to_value(owner.state.to_doc()).unwrap(), before);
        assert_eq!(
            serde_json::to_value(owner.verification[&JobId(1)].progress()).unwrap(),
            verification_before
        );
        assert!(!completed.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(
            owner.persistence.in_flight(),
            1,
            "the blocked actual worker retains its slot"
        );

        let (reply, mut pause) = oneshot::channel();
        owner.on_command(QueueCommand::Pause {
            job: JobId(2),
            reply,
        });
        assert!(
            matches!(pause.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "a valid persistent intent cannot acknowledge through a blocked durability lane"
        );
        assert_eq!(owner.state.job(JobId(2)).unwrap().status, JobStatus::Paused);
        assert_eq!(
            serde_json::to_value(owner.verification[&JobId(1)].progress()).unwrap(),
            verification_before
        );
        assert!(!completed.load(std::sync::atomic::Ordering::Acquire));

        release.send(()).unwrap();
        pump(&mut owner, &mut messages).await;
        assert!(pause.await.unwrap());
        assert!(completed.load(std::sync::atomic::Ordering::Acquire));
        let saved = SnapshotStore::open(&owner.state_dir)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert_eq!(
            saved
                .jobs
                .iter()
                .find(|job| job.id == JobId(2))
                .unwrap()
                .status,
            JobStatus::Paused
        );
        let verification_after = owner.verification[&JobId(1)].progress();
        assert_eq!(verification_after.reason, None);
        assert_eq!(verification_after.checked_bytes, 0);
        assert_eq!(verification_after.total_bytes, 0);
    }

    #[tokio::test]
    async fn engine_shutdown_drains_real_writer_acknowledgement_behind_full_result_channel() {
        let (tmp, mut owner, adapter, mut messages) = control_test_owner();
        let mut job = pending_job(1);
        job.files[0].segments[0].size = 5;
        recompute_job_totals(&mut job);
        let file = job.files[0].id;
        owner.state.jobs.push(job);
        allocate_for_test(&mut owner, &mut messages, JobId(1), false).await;
        let directory = owner.allocation_cache[&JobId(1)].path.clone();
        let tracker = owner.tracker.clone();
        let writer = crate::writer::spawn_writer(
            &tracker,
            JobId(1),
            file,
            directory.clone(),
            "payload.bin".into(),
            owner.engine_tx.clone(),
        );
        owner.writers.insert(file, writer.clone());
        owner.writer_epochs.insert(file, writer.generation);
        // No owner is receiving yet, so this fills the real one-slot channel.
        owner
            .engine_tx
            .try_send(EngineMsg::ConnectFailed {
                server: ServerId(99),
            })
            .unwrap();
        assert_eq!(owner.engine_tx.capacity(), 0);
        writer
            .tx
            .send(WriteCmd::Segment {
                seg_number: 1,
                offset: 0,
                data: b"hello".to_vec(),
                crc: crc32fast::hash(b"hello"),
                file_size: 5,
                server: ServerId(1),
            })
            .await
            .unwrap();
        let partial = directory.join(format!(".runner-file-{}.part", file.0));
        tokio::time::timeout(Duration::from_secs(5), async {
            while std::fs::read(&partial).ok().as_deref() != Some(b"hello".as_slice()) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("real writer did not reach the acknowledgement barrier");
        writer.stop.cancel();
        assert!(
            !*writer.stopped.borrow(),
            "cancellation cannot stop a writer whose acknowledgement has no channel room"
        );
        assert_eq!(owner.engine_tx.capacity(), 0);

        let handle = crate::EngineHandle {
            artifacts: owner.artifacts.clone(),
            cmd_tx: owner.engine_tx.clone(),
            shared: owner.shared.clone(),
            events: owner.events.clone(),
            cancel: owner.cancel.clone(),
            tracker: tracker.clone(),
            backend_adapter: Arc::new(std::sync::Mutex::new(Some(adapter))),
            budget_tracker: Arc::new(crate::pool::BudgetTracker::new(0)),
            url_fetches: Arc::new(std::sync::Mutex::new(HashSet::new())),
            post_scope: None,
            queue_changes: owner.epoch_tx.subscribe(),
            owner_heartbeat: owner.owner_heartbeat.clone(),
            shutdown_complete: owner.shutdown_complete.clone(),
        };
        // Enter the actual owner task already cancelled; its shutdown drain
        // must create channel room before it can wait for the writer.
        handle.cancel.cancel();
        tracker.spawn(owner.run(messages));
        tracker.close();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), handle.shutdown())
                .await
                .expect("shutdown deadlocked behind a final writer acknowledgement")
        );
        assert!(*writer.stopped.borrow());
        let saved = SnapshotStore::open(&tmp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert!(
            matches!(saved.jobs[0].files[0].segments[0].state, SegmentState::Done { offset: 0, len: 5, crc } if crc == crc32fast::hash(b"hello"))
        );
        assert_eq!(std::fs::read(partial).unwrap(), b"hello");
        assert!(
            !tmp.path()
                .join("state/unclean.control-test")
                .try_exists()
                .unwrap(),
            "marker clears only after the accepted acknowledgement is durably captured"
        );
    }

    #[tokio::test]
    async fn fresh_default_root_initializes_in_payload_worker_but_enrolled_missing_root_stays_missing(
    ) {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        assert!(
            !owner.dest_dir.exists(),
            "metadata recovery must not create the payload root inline"
        );
        pump(&mut owner, &mut messages).await;
        assert!(owner.dest_dir.is_dir());
        owner.state.jobs.push(pending_job(1));
        allocate_for_test(&mut owner, &mut messages, JobId(1), true).await;
        let allocation = owner.allocation_cache[&JobId(1)].clone();
        std::fs::remove_dir(&allocation.path).unwrap();
        std::fs::remove_dir(&allocation.root).unwrap();
        prepare_unenrolled_payload_root(&owner.artifacts, &allocation.root).unwrap();
        assert!(
            !allocation.root.exists(),
            "an absent enrolled mount cannot be recreated as an empty directory"
        );
    }

    #[tokio::test]
    async fn fresh_admission_prepares_root_before_startup_callback_and_refuses_missing_enrolled_root(
    ) {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        // Hold the completed state-lane enumeration before the owner can queue
        // root reconciliation. Immediate user admission must work in this order.
        let startup = tokio::time::timeout(Duration::from_secs(5), messages.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(!owner.startup_roots_ready);
        assert!(!owner.dest_dir.exists());
        owner.state.jobs.push(pending_job(1));
        let (reply, mut allocated) = oneshot::channel();
        owner.allocate_new_payload_then(JobId(1), move |_, result| {
            reply.send(result).unwrap();
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    result = &mut allocated => break result.unwrap().unwrap(),
                    message = messages.recv() => owner.on_msg(message.unwrap()),
                }
            }
        })
        .await
        .expect("fresh admission must not depend on startup enumeration delivery");
        assert!(!owner.startup_roots_ready);
        let allocation = owner.allocation_cache[&JobId(1)].clone();
        assert_eq!(allocation.root, owner.dest_dir);
        assert!(allocation.path.is_dir());
        let bound = owner.artifacts.for_job(1).unwrap().unwrap();
        assert_eq!(bound.id, allocation.id);
        assert_eq!(bound.generation, allocation.generation);
        assert_eq!(bound.path, allocation.path);

        owner.on_msg(startup);
        pump(&mut owner, &mut messages).await;
        std::fs::remove_dir_all(&allocation.path).unwrap();
        std::fs::remove_dir(&allocation.root).unwrap();
        owner.state.jobs.push(pending_job(2));
        let (reply, refused) = oneshot::channel();
        owner.allocate_new_payload_then(JobId(2), move |_, result| {
            reply.send(result).unwrap();
        });
        pump(&mut owner, &mut messages).await;
        assert!(refused.await.unwrap().is_err());
        assert!(
            !allocation.root.exists(),
            "missing enrolled custody cannot become a fresh root"
        );
        let retained = owner.artifacts.for_job(1).unwrap().unwrap();
        assert_eq!(retained.path, allocation.path);
        assert_eq!(retained.generation, allocation.generation);
        assert_eq!(retained.root_identity, allocation.root_identity);
        assert!(owner.artifacts.for_job(2).unwrap().is_none());
        assert!(!owner.allocation_cache.contains_key(&JobId(2)));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metadata_boot_and_other_root_controls_do_not_wait_for_blocked_active_allocation() {
        use std::io::Write;
        let (tmp, mut owner, _adapter, mut messages) = control_test_owner();
        let state_dir = owner.state_dir.clone();
        let root_a = owner.dest_dir.clone();
        let root_b = tmp.path().join("healthy-payload");
        std::fs::create_dir_all(&root_a).unwrap();
        std::fs::create_dir_all(&root_b).unwrap();
        let mut first = pending_job(1);
        first.status = JobStatus::Paused;
        owner.state.jobs = vec![first, pending_job(2)];
        let first = owner
            .artifacts
            .allocate_for_delivery(1, &root_a, &root_a.join("job-1"), false, false)
            .unwrap();
        owner
            .artifacts
            .allocate_for_delivery(2, &root_b, &root_b.join("job-2"), false, false)
            .unwrap();
        owner.save_snapshot();
        pump(&mut owner, &mut messages).await;
        let marker = state_dir
            .join("artifact-identities")
            .join(&first.id)
            .join(format!("{}.json", first.generation));
        let original = std::fs::read(&marker).unwrap();
        owner.artifacts.close().unwrap();
        drop(owner);
        drop(messages);
        std::fs::remove_file(&marker).unwrap();
        assert!(std::process::Command::new("mkfifo")
            .arg(&marker)
            .status()
            .unwrap()
            .success());
        let (opened, reader_started) = oneshot::channel();
        let opening_path = marker.clone();
        let writer = std::thread::spawn(move || {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(opening_path)
                .unwrap();
            let _ = opened.send(file);
        });
        let config = crate::EngineConfig::single_node(
            Vec::new(),
            state_dir.clone(),
            root_a.clone(),
            Tuning::default(),
            None,
        );
        let starting = tokio::spawn(crate::Engine::spawn(config));
        let engine = tokio::time::timeout(Duration::from_secs(2), starting)
            .await
            .expect("payload I/O blocked healthy queue metadata construction")
            .unwrap()
            .unwrap();
        let blocked = tokio::time::timeout(Duration::from_secs(2), reader_started).await
            .expect("active-allocation capability worker did not reach the explicit sidecar read barrier").unwrap();
        writer.join().unwrap();
        // Restore the pathname before releasing the existing descriptor, so
        // later verification observes ordinary identity material rather than
        // accidentally creating a second FIFO barrier.
        struct Release {
            path: PathBuf,
            bytes: Vec<u8>,
            writer: Option<std::fs::File>,
        }
        impl Drop for Release {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.path);
                let _ = std::fs::write(&self.path, &self.bytes);
                if let Some(mut writer) = self.writer.take() {
                    let _ = writer.write_all(&self.bytes);
                }
            }
        }
        let release = Release {
            path: marker,
            bytes: original,
            writer: Some(blocked),
        };
        assert_eq!(
            engine.snapshot().jobs.len(),
            2,
            "metadata must retain both jobs while root A is blocked"
        );
        let document = tokio::time::timeout(Duration::from_secs(1), engine.recovery_document())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(document.jobs.len(), 2);
        let first = document.jobs.iter().find(|job| job.id == JobId(1)).unwrap();
        assert_eq!(first.status, JobStatus::Paused);
        assert!(matches!(
            first.files[0].segments[0].state,
            SegmentState::Pending
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if engine
                    .snapshot()
                    .jobs
                    .iter()
                    .find(|job| job.id == JobId(2))
                    .is_some_and(|job| job.verification.is_none())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("root B capability was held behind unrelated root A");
        assert!(
            tokio::time::timeout(Duration::from_secs(1), engine.pause_job(JobId(2)))
                .await
                .unwrap()
                .unwrap(),
            "healthy-state durable metadata control cannot wait for root A"
        );
        assert!(
            release.writer.is_some(),
            "the barrier remains closed through every metadata/control assertion"
        );
        drop(release);
        assert!(
            tokio::time::timeout(Duration::from_secs(5), engine.shutdown())
                .await
                .unwrap()
        );
    }

    fn guard_test_owner(
        tuning: Tuning,
    ) -> (
        tempfile::TempDir,
        Owner,
        watch::Receiver<u64>,
        mpsc::Receiver<EngineMsg>,
    ) {
        let (tmp, owner, epoch, _adapter, engine_messages) = guard_test_owner_with_backend(tuning);
        (tmp, owner, epoch, engine_messages)
    }

    fn guard_test_owner_with_backend(
        tuning: Tuning,
    ) -> (
        tempfile::TempDir,
        Owner,
        watch::Receiver<u64>,
        crate::backend::BackendAdapterPort,
        mpsc::Receiver<EngineMsg>,
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let (budget_tx, _) = watch::channel(crate::pool::BudgetEnvelope::default());
        let (events, _) = broadcast::channel(1);
        let (epoch_tx, epoch_rx) = watch::channel(0);
        let (engine_tx, engine_messages) = mpsc::channel(1);
        let (backend, adapter) = crate::backend::backend_channel(1, 1);
        let owner = Owner::recover(
            &tmp.path().join("state"),
            None,
            None,
            tmp.path().join("dest"),
            Vec::new(),
            None,
            Arc::new(Vec::new()),
            tuning,
            true,
            false,
            "guard-test",
            None,
            budget_tx,
            crate::new_shared_snapshot(),
            events,
            epoch_tx,
            Arc::new(SpeedMeter::new()),
            Arc::new(RateLimiter::new(None)),
            None,
            None,
            backend,
            engine_tx,
            TaskTracker::new(),
            CancellationToken::new(),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
        )
        .unwrap();
        (tmp, owner, epoch_rx, adapter, engine_messages)
    }

    pub(super) fn control_test_owner() -> (
        tempfile::TempDir,
        Owner,
        crate::backend::BackendAdapterPort,
        mpsc::Receiver<EngineMsg>,
    ) {
        control_test_owner_with_persistence(true, None)
    }

    #[tokio::test]
    async fn lifecycle_cohort_is_stamped_at_admission_not_later_url_allocation() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        let old = owner.state.admit_url(
            "old-fetching".into(),
            "http://example.test/old.nzb",
            None,
            0,
        );
        allocate_for_test(&mut owner, &mut messages, old, true).await;
        let old_row = owner.state.job(old).unwrap();
        assert!(!old_row.lifecycle_eligible());
        assert!(
            !owner
                .artifacts
                .for_job(old.0)
                .unwrap()
                .unwrap()
                .automatic_cleanup_eligible
        );

        let new = owner.state.admit_url(
            "new-fetching".into(),
            "http://example.test/new.nzb",
            None,
            0,
        );
        mark_new_lifecycle_cohort(owner.state.job_mut(new).unwrap());
        allocate_for_test(&mut owner, &mut messages, new, true).await;
        let new_row = owner.state.job(new).unwrap();
        assert!(new_row.lifecycle_eligible());
        assert!(
            owner
                .artifacts
                .for_job(new.0)
                .unwrap()
                .unwrap()
                .automatic_cleanup_eligible
        );

        let mut torrent = new_row.clone();
        torrent.kind = nzbd_types::JobKind::Torrent;
        torrent
            .params
            .retain(|(key, _)| key != "*Runner:LifecycleEligible");
        assert!(
            !torrent.lifecycle_eligible(),
            "torrent seeding sources stay outside the NZB cohort"
        );
    }

    fn control_test_owner_with_persistence(
        persist: bool,
        persist_guard: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    ) -> (
        tempfile::TempDir,
        Owner,
        crate::backend::BackendAdapterPort,
        mpsc::Receiver<EngineMsg>,
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let (budget_tx, _) = watch::channel(crate::pool::BudgetEnvelope::default());
        let (events, _) = broadcast::channel(1);
        let (epoch_tx, _) = watch::channel(0);
        let (engine_tx, engine_messages) = mpsc::channel(1);
        let (backend, adapter) = crate::backend::backend_channel(1, 1);
        let owner = Owner::recover(
            &tmp.path().join("state"),
            None,
            None,
            tmp.path().join("dest"),
            Vec::new(),
            None,
            Arc::new(Vec::new()),
            Tuning::default(),
            true,
            persist,
            "control-test",
            persist_guard,
            budget_tx,
            crate::new_shared_snapshot(),
            events,
            epoch_tx,
            Arc::new(SpeedMeter::new()),
            Arc::new(RateLimiter::new(None)),
            None,
            None,
            backend,
            engine_tx,
            TaskTracker::new(),
            CancellationToken::new(),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
        )
        .unwrap();
        (tmp, owner, adapter, engine_messages)
    }

    #[tokio::test]
    async fn keep_on_failure_is_opt_in_and_snapshot_committed_before_ack() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(pending_job(1));
        let (reply, result) = oneshot::channel();
        owner.on_command(QueueCommand::SetKeepOnFailure {
            job: JobId(1),
            keep: true,
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(result.await.unwrap());
        let saved = SnapshotStore::open(&owner.state_dir)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert!(saved.jobs[0]
            .params
            .iter()
            .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1"));

        // A preallocation opt-in must be copied atomically into the first
        // source generation, then consulted by the real failed-payload
        // disposal path. The queue snapshot remains the authority only until
        // that generation exists.
        allocate_for_test(&mut owner, &mut messages, JobId(1), true).await;
        let source = owner.artifacts.for_job(1).unwrap().unwrap();
        assert!(source.keep_on_failure);
        std::fs::write(source.path.join("partial.bin"), b"failed partial payload").unwrap();
        owner
            .artifacts
            .finish(1, &source.path, &source.root, "active")
            .unwrap();
        let attempt = owner.artifacts.acquire_attempt_use(1).unwrap().unwrap();
        assert!(owner
            .artifacts
            .delete_failed_payload(&attempt, 1, &|| Ok(()))
            .is_err());
        assert!(source.path.join("partial.bin").exists());

        let (reply, result) = oneshot::channel();
        owner.on_command(QueueCommand::SetKeepOnFailure {
            job: JobId(1),
            keep: false,
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(result.await.unwrap());
        let saved = SnapshotStore::open(&owner.state_dir)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert!(!saved.jobs[0]
            .params
            .iter()
            .any(|(key, _)| key == "*Runner:KeepOnFailure"));
        assert!(owner
            .artifacts
            .delete_failed_payload(&attempt, 1, &|| Ok(()))
            .is_ok());
    }

    #[tokio::test]
    async fn terminal_failed_job_can_keep_only_its_retained_delivery_source() {
        let (tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        let mut job = pending_job(57);
        job.status = JobStatus::Failed;
        owner.state.jobs.push(job);

        let source = owner.dest_dir.join("failed-source-57");
        owner
            .artifacts
            .allocate_for_delivery(57, &owner.dest_dir, &source, true, true)
            .unwrap();
        std::fs::write(source.join("episode.mkv"), b"verified payload").unwrap();
        owner
            .artifacts
            .finish(57, &source, &owner.dest_dir, "active")
            .unwrap();
        let selected = owner
            .artifacts
            .for_job(57)
            .unwrap()
            .unwrap()
            .files
            .into_iter()
            .filter(|entry| entry.path == "episode.mkv")
            .collect::<Vec<_>>();
        let completed = tmp.path().join("completed");
        std::fs::create_dir(&completed).unwrap();
        owner
            .artifacts
            .publish_selected_for_delivery_checked(
                57,
                &completed.join("published-generation"),
                selected,
                vec![nzbd_state::artifacts::DeliveryFileOutcome {
                    source_path: "episode.mkv".into(),
                    publication_path: Some("episode.mkv".into()),
                    classification: "video".into(),
                    outcome: "published".into(),
                    reason: Some("verified complete media".into()),
                    repair_dependency: false,
                    source_cleanup_status: "pending".into(),
                }],
                &|| Ok(()),
            )
            .unwrap();
        let delivery = owner
            .artifacts
            .create_delivery_with_outcome_and_keep(
                57,
                true,
                Some("keep-terminal-source-test"),
                "FAILURE/HEALTH",
                Some("health"),
                Some("partial salvage"),
                false,
            )
            .unwrap();
        let source_id = delivery.source_artifact_id.clone();
        let source_generation = delivery.source_generation.clone();
        owner
            .state
            .url_admission_receipts
            .push(UrlAdmissionReceipt {
                protocol_version: 1,
                request_token: "keep-source-tombstone-57".into(),
                submission_sha256: "submission-hash".into(),
                job_id: 57,
                generation: "admission-generation".into(),
                runner_instance_id: "runner-instance".into(),
                terminal_outcome: Some("FAILURE/HEALTH".into()),
                outcome: "failed".into(),
                failure_stage: Some("health".into()),
                failure_cause: Some("partial salvage".into()),
                custody_state: "runner_owned".into(),
                keep_source_on_failure: false,
                source_cleanup_status: Some("pending".into()),
                publication_path: Some(
                    completed.join("published-generation").display().to_string(),
                ),
                source_artifact_id: Some(source_id.clone()),
                source_generation: Some(source_generation.clone()),
            });

        let (reply, result) = oneshot::channel();
        owner.on_command(QueueCommand::SetKeepOnFailure {
            job: JobId(57),
            keep: true,
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(result.await.unwrap());
        assert!(owner.artifacts.get(&source_id).unwrap().keep_on_failure);
        assert!(owner.state.url_admission_receipts[0].keep_source_on_failure);
        assert_eq!(
            owner.artifacts.get(&delivery.artifact_id).unwrap().state,
            "transferred"
        );

        let (reply, result) = oneshot::channel();
        owner.on_command(QueueCommand::SetKeepOnFailure {
            job: JobId(57),
            keep: false,
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(result.await.unwrap());
        assert!(!owner.artifacts.get(&source_id).unwrap().keep_on_failure);
        assert!(!owner.state.url_admission_receipts[0].keep_source_on_failure);

        // Once the visible queue/history row is gone, the durable source
        // generation still refreshes the receipt, and stale queue params no
        // longer overwrite its live Keep preference.
        owner.state.jobs.clear();
        owner.artifacts.set_keep_on_failure(57, true).unwrap();
        let (reply, result) = oneshot::channel();
        owner.on_command(QueueCommand::GetUrlAdmission {
            request_token: "keep-source-tombstone-57".into(),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(result.await.unwrap().unwrap().keep_source_on_failure);
        owner.artifacts.set_keep_on_failure(57, false).unwrap();
        let (reply, result) = oneshot::channel();
        owner.on_command(QueueCommand::GetUrlAdmission {
            request_token: "keep-source-tombstone-57".into(),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(!result.await.unwrap().unwrap().keep_source_on_failure);
        assert!(
            !owner
                .artifacts
                .delivery(&delivery.delivery_id)
                .unwrap()
                .keep_source_on_failure,
            "the original Delivery choice remains immutable"
        );
    }

    #[tokio::test]
    async fn keep_request_does_not_succeed_after_source_generation_is_deleted() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        let mut job = pending_job(63);
        job.status = JobStatus::Failed;
        owner.state.jobs.push(job);
        let source = owner.dest_dir.join("deleted-source-63");
        owner
            .artifacts
            .allocate_for_delivery(63, &owner.dest_dir, &source, true, false)
            .unwrap();
        std::fs::write(source.join("payload.mkv"), b"failed payload").unwrap();
        owner
            .artifacts
            .finish(63, &source, &owner.dest_dir, "parked_failed")
            .unwrap();
        let artifact = owner.artifacts.for_job(63).unwrap().unwrap();
        let deletion = owner
            .artifacts
            .request_delete(&artifact.id, artifact.revision, "delete-source-63", 0)
            .unwrap();
        owner.artifacts.execute_delete(&deletion.id).unwrap();

        let (reply, result) = oneshot::channel();
        owner.on_command(QueueCommand::SetKeepOnFailure {
            job: JobId(63),
            keep: true,
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(!result.await.unwrap());
        assert!(!owner.state.jobs[0]
            .params
            .iter()
            .any(|(key, _)| key == "*Runner:KeepOnFailure"));
    }

    #[tokio::test]
    async fn intact_publication_clears_previous_private_repair_marker() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        let mut job = pending_job(1);
        let file = job.files[0].id;
        job.params
            .push((format!("*File:repair:{}", file.0), "5".into()));
        job.files[0].segments[0].state = SegmentState::Done {
            offset: 0,
            len: 5,
            crc: crc32fast::hash(b"hello"),
        };
        owner.state.jobs.push(job);
        owner.file_sizes.insert(file, 5);
        let (tx, mut rx) = mpsc::channel(8);
        let (_done, stopped) = watch::channel(false);
        owner.writers.insert(
            file,
            WriterHandle {
                tx,
                stop: CancellationToken::new(),
                stopped,
                generation: 0,
            },
        );
        owner.send_finalize(JobId(1), file);
        pump(&mut owner, &mut messages).await;
        assert!(!owner.state.job(JobId(1)).unwrap().file_needs_repair(file));
        assert!(matches!(
            rx.try_recv().unwrap(),
            WriteCmd::PublicationName(_)
        ));
        assert!(matches!(
            rx.try_recv().unwrap(),
            WriteCmd::Finalize {
                combined_crc: Some(_),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn yenc_name_variations_keep_the_first_name_but_size_and_path_conflicts_hold() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        for id in 1..=3 {
            owner.state.jobs.push(pending_job(id));
        }
        let file = owner.state.job(JobId(1)).unwrap().files[0].id;
        owner.accept_file_metadata(JobId(1), file, "first.bin", 100);
        pump(&mut owner, &mut messages).await;
        owner.accept_file_metadata(JobId(1), file, "different-obfuscated-name", 100);
        pump(&mut owner, &mut messages).await;
        assert!(!owner.state.job(JobId(1)).unwrap().held());
        assert_eq!(
            owner.state.job(JobId(1)).unwrap().files[0].filename,
            "first.bin"
        );
        owner.accept_file_metadata(JobId(1), file, "third-name", 101);
        pump(&mut owner, &mut messages).await;
        let control = owner.state.job(JobId(1)).unwrap().control().unwrap();
        assert_eq!(control.cause, "identity_conflict");
        assert_eq!(control.message, "conflicting yEnc declared size");

        for (id, name, size) in [(2, "../unsafe", 100), (3, "empty.bin", 0)] {
            let file = owner.state.job(JobId(id)).unwrap().files[0].id;
            owner.accept_file_metadata(JobId(id), file, name, size);
            pump(&mut owner, &mut messages).await;
            assert!(owner.state.job(JobId(id)).unwrap().held());
        }
    }

    #[tokio::test]
    async fn payload_retirement_retries_keep_original_acknowledgements_and_fence_resume() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        std::fs::create_dir_all(&owner.dest_dir).unwrap();
        let job = pending_job(1);
        let file = job.files[0].id;
        owner.state.jobs.push(job);
        allocate_for_test(&mut owner, &mut messages, JobId(1), false).await;
        let (tx, _rx) = mpsc::channel(1);
        let (done, stopped) = watch::channel(false);
        owner.writers.insert(
            file,
            WriterHandle {
                tx,
                stop: CancellationToken::new(),
                stopped,
                generation: 0,
            },
        );
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::QuiescePayload {
            job: JobId(1),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        let first = rx.await.unwrap().unwrap();
        assert_eq!(first.len(), 1);
        assert!(!*first[0].stopped.borrow());
        drop(first); // the timed-out HTTP request loses its receivers
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::Resume {
            job: JobId(1),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(!rx.await.unwrap());
        assert!(owner.ensure_allocation(JobId(1), false).is_err());
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::QuiescePayload {
            job: JobId(1),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        let mut retry = rx.await.unwrap().unwrap();
        assert_eq!(retry.len(), 1);
        assert!(!*retry[0].stopped.borrow());
        done.send(true).unwrap();
        retry[0].stopped.wait_for(|v| *v).await.unwrap();
        assert_eq!(
            owner.artifacts.for_job(1).unwrap().unwrap().state,
            "retiring"
        );
    }

    fn control_torrent_job() -> Job {
        let mut job = bare_job();
        job.kind = JobKind::Torrent;
        job.status = JobStatus::Queued;
        job.torrent = Some(nzbd_types::TorrentRecord {
            info_hash_v1: "0123456789abcdef0123456789abcdef01234567".into(),
            source: nzbd_types::TorrentSource::Metainfo,
            metadata_file: "meta/control.torrent".into(),
            payload_root: PathBuf::new(),
            phase: nzbd_types::TorrentPhase::Queued,
            control_intent: TorrentControlIntent::Running,
            removal_intent: None,
            removal_outcome: None,
            removal_confirmed_at_unix: None,
            stop_reason: None,
            files: Vec::new(),
            total_bytes: 1,
            selected_bytes: 1,
            downloaded_bytes: 0,
            uploaded_bytes: 0,
            seeding_seconds: 0,
            ready_at_unix: None,
            content_path: None,
            seed_policy: Default::default(),
            last_activity_unix: None,
            last_error: None,
        });
        job
    }

    #[tokio::test]
    async fn torrent_snapshot_reports_payload_progress_and_live_rates() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        let mut job = control_torrent_job();
        job.status = JobStatus::Downloading;
        let id = job.id;
        let torrent = job.torrent.as_mut().unwrap();
        torrent.total_bytes = 8192;
        torrent.selected_bytes = 4096;
        torrent.downloaded_bytes = 1024;
        torrent.files = vec![
            nzbd_types::TorrentFileRecord {
                path: "first.iso".into(),
                length: 1024,
                selected: true,
                downloaded_bytes: 1024,
            },
            nzbd_types::TorrentFileRecord {
                path: "second.iso".into(),
                length: 3072,
                selected: true,
                downloaded_bytes: 0,
            },
            nzbd_types::TorrentFileRecord {
                path: "unselected.iso".into(),
                length: 4096,
                selected: false,
                downloaded_bytes: 0,
            },
        ];
        owner.state.jobs.push(job);
        owner.torrent_progress.insert(
            id,
            crate::backend::TransferProgress {
                download_bps: 512,
                upload_bps: 128,
                useful_peers: 3,
                ..Default::default()
            },
        );
        owner.server_wire_ema.insert(1, 256.0);
        owner.publish_now();
        owner.publish_now(); // Structural publications must not add the torrent rate twice.
        let snapshot = owner.shared.load();
        let row = &snapshot.jobs[0];
        assert_eq!(
            (row.size_bytes, row.downloaded_bytes, row.remaining_bytes),
            (4096, 1024, 3072)
        );
        assert_eq!((row.files_total, row.files_done), (2, 1));
        assert_eq!(
            (row.rate_bps, row.upload_rate_bps, row.useful_peers),
            (512, 128, 3)
        );
        assert_eq!(snapshot.remaining_bytes, 3072);
        assert_eq!(snapshot.download_rate_bps, 768);
        assert_eq!(
            snapshot
                .server_volumes
                .iter()
                .map(|v| v.rate_bps)
                .sum::<u64>(),
            768
        );
    }

    #[tokio::test]
    async fn seed_policy_stop_survives_restart_and_requires_a_new_policy_to_resume() {
        let (_tmp, mut owner, mut adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        let mut job = control_torrent_job();
        job.status = JobStatus::Downloading;
        let torrent = job.torrent.as_mut().unwrap();
        torrent.phase = TorrentPhase::Seeding;
        torrent.ready_at_unix = Some(100);
        torrent.downloaded_bytes = 1;
        owner.state.jobs.push(job);
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::SetTorrentSeedPolicy {
            job: JobId(1),
            policy: nzbd_types::SeedPolicy {
                stop_on_complete: true,
                ..Default::default()
            },
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(rx.await.unwrap());
        assert_eq!(
            adapter.next_command().await,
            Some(BackendCommand::PauseForSeedPolicy { job: JobId(1) })
        );
        let persisted = SnapshotStore::open(&owner.state_dir)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        let saved = persisted.jobs[0].torrent.as_ref().unwrap();
        assert!(saved.seed_policy.stop_on_complete);
        assert_eq!(
            saved.stop_reason,
            Some(nzbd_types::TorrentStopReason::DownloadComplete)
        );
        assert_eq!(saved.control_intent, TorrentControlIntent::Paused);
        assert_eq!(
            owner.shared.load().jobs[0].seed_stop_reason,
            saved.stop_reason
        );
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::Resume {
            job: JobId(1),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(!rx.await.unwrap());

        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::SetTorrentSeedPolicy {
            job: JobId(1),
            policy: Default::default(),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(rx.await.unwrap());
        assert_eq!(
            owner.state.jobs[0].status,
            JobStatus::Paused,
            "editing a policy must not restart a stopped seed"
        );
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::Resume {
            job: JobId(1),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(rx.await.unwrap());
        assert_eq!(
            owner.state.jobs[0].torrent.as_ref().unwrap().stop_reason,
            None
        );
    }

    #[tokio::test]
    async fn torrent_missing_file_recovery_is_not_blocked_by_a_completed_seed_policy() {
        let (tmp, mut owner, mut adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        let mut job = control_torrent_job();
        job.status = JobStatus::Paused;
        let torrent = job.torrent.as_mut().unwrap();
        torrent.phase = TorrentPhase::MissingFiles;
        torrent.ready_at_unix = Some(100);
        torrent.content_path = Some(PathBuf::from("/payload"));
        torrent.seed_policy.stop_on_complete = true;
        owner.state.jobs.push(job);
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::Resume {
            job: JobId(1),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(rx.await.unwrap());
        owner.schedule_torrent_starts();
        pump(&mut owner, &mut messages).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), adapter.next_command())
                .await
                .unwrap(),
            Some(BackendCommand::Start { job: JobId(1) })
        );
        let torrent = owner.state.jobs[0].torrent.as_ref().unwrap();
        assert_eq!(torrent.phase, TorrentPhase::Queued);
        assert_eq!(torrent.ready_at_unix, None);
        assert_eq!(torrent.content_path, None);
        assert!(torrent.seed_policy.stop_on_complete);
        let persisted = SnapshotStore::open(&owner.state_dir)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert_eq!(
            persisted.jobs[0].torrent.as_ref().unwrap().ready_at_unix,
            None
        );
        let payload_root = tmp.path().join("payload");
        std::fs::create_dir_all(&payload_root).unwrap();
        let content_path = payload_root.join("recovered.bin");
        std::fs::write(&content_path, b"x").unwrap();
        owner.torrent_payload_roots = vec![payload_root];
        adapter.progress(
            JobId(1),
            crate::backend::TransferProgress {
                verified_bytes: 1,
                ..Default::default()
            },
        );
        adapter
            .structural(BackendFact::Ready {
                job: JobId(1),
                content_path,
            })
            .await
            .unwrap();
        owner.fold_backend_progress();
        pump(&mut owner, &mut messages).await;
        owner.fold_backend_structural();
        pump(&mut owner, &mut messages).await;
        owner.update_seed_policies(200);
        pump(&mut owner, &mut messages).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), adapter.next_command())
                .await
                .unwrap(),
            Some(BackendCommand::PauseForSeedPolicy { job: JobId(1) })
        );
        let torrent = owner.state.jobs[0].torrent.as_ref().unwrap();
        assert!(torrent.ready_at_unix.is_some());
        assert_eq!(
            torrent.stop_reason,
            Some(nzbd_types::TorrentStopReason::DownloadComplete)
        );
    }

    #[tokio::test]
    async fn torrent_storage_recovery_clears_the_stop_reason() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        let mut job = control_torrent_job();
        job.status = JobStatus::Paused;
        let torrent = job.torrent.as_mut().unwrap();
        torrent.phase = TorrentPhase::PausedDownload;
        torrent.last_error = Some("storage full".into());
        torrent.stop_reason = Some(nzbd_types::TorrentStopReason::StorageFull);
        owner.state.jobs.push(job);
        owner.release_torrents_after_storage_recovery();
        pump(&mut owner, &mut messages).await;
        let torrent = owner.state.jobs[0].torrent.as_ref().unwrap();
        assert_eq!(torrent.phase, TorrentPhase::Queued);
        assert_eq!(torrent.stop_reason, None);
        assert_eq!(torrent.last_error, None);
    }

    #[tokio::test]
    async fn seed_policy_save_failure_rolls_back_without_stopping_the_torrent() {
        let (_tmp, mut owner, mut adapter, mut messages) =
            control_test_owner_with_persistence(true, Some(Arc::new(|| false)));
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(control_torrent_job());
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::SetTorrentSeedPolicy {
            job: JobId(1),
            policy: nzbd_types::SeedPolicy {
                stop_on_complete: true,
                ..Default::default()
            },
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(!rx.await.unwrap());
        assert_eq!(
            owner.state.jobs[0].torrent.as_ref().unwrap().seed_policy,
            Default::default()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), adapter.next_command())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn torrent_control_is_persisted_before_fifo_delivery_and_is_idempotent() {
        let (tmp, mut owner, mut adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(control_torrent_job());

        let (reply, mut rx) = oneshot::channel();
        owner.on_command(QueueCommand::Pause {
            job: JobId(1),
            reply,
        });
        assert!(
            matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "control acknowledgement must wait for the actual worker completion"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(1), adapter.next_command())
                .await
                .is_err(),
            "backend delivery must wait for durable owner acceptance"
        );
        pump(&mut owner, &mut messages).await;
        assert!(rx.await.unwrap());
        assert_eq!(
            adapter.next_command().await,
            Some(BackendCommand::Pause { job: JobId(1) })
        );

        let persisted = SnapshotStore::open(&owner.state_dir)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        let torrent = persisted.jobs[0].torrent.as_ref().unwrap();
        assert_eq!(persisted.jobs[0].status, JobStatus::Paused);
        assert_eq!(torrent.control_intent, TorrentControlIntent::Paused);
        assert_eq!(
            torrent.stop_reason,
            Some(nzbd_types::TorrentStopReason::Manual)
        );

        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::Pause {
            job: JobId(1),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(!rx.await.unwrap());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), adapter.next_command())
                .await
                .is_err()
        );

        let paused = owner.state.jobs[0].torrent.as_mut().unwrap();
        paused.phase = nzbd_types::TorrentPhase::PausedDownload;
        paused.last_activity_unix = Some(1);
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::Resume {
            job: JobId(1),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(rx.await.unwrap());
        owner.schedule_torrent_starts();
        pump(&mut owner, &mut messages).await;
        assert_eq!(
            adapter.next_command().await,
            Some(BackendCommand::Start { job: JobId(1) })
        );
        assert!(tmp.path().join("state/queue.json").exists());
    }

    #[tokio::test]
    async fn failed_persistence_never_emits_a_torrent_control() {
        let (_tmp, mut owner, mut adapter, mut messages) =
            control_test_owner_with_persistence(true, Some(Arc::new(|| false)));
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(control_torrent_job());

        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::Pause {
            job: JobId(1),
            reply,
        });
        pump(&mut owner, &mut messages).await;

        assert!(!rx.await.unwrap());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), adapter.next_command())
                .await
                .is_err(),
            "a failed durability barrier must prevent FIFO delivery"
        );
        let job = owner.state.job(JobId(1)).unwrap();
        assert_eq!(job.status, JobStatus::Queued);
        assert_eq!(
            job.torrent.as_ref().unwrap().control_intent,
            TorrentControlIntent::Running
        );
    }

    #[tokio::test]
    async fn full_backend_fifo_retries_retained_torrent_commands_in_order() {
        let (_tmp, mut owner, mut adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(control_torrent_job());

        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::Pause {
            job: JobId(1),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(rx.await.unwrap());
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::SetPriority {
            job: JobId(1),
            priority: 77,
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(rx.await.unwrap());
        assert_eq!(owner.pending_backend_commands.len(), 1);

        assert_eq!(
            adapter.next_command().await,
            Some(BackendCommand::Pause { job: JobId(1) })
        );
        owner.flush_backend_commands();
        assert_eq!(
            adapter.next_command().await,
            Some(BackendCommand::SetPriority {
                job: JobId(1),
                priority: 77
            })
        );
    }

    #[tokio::test]
    async fn retained_backend_commands_precede_later_control_requests() {
        let (_tmp, mut owner, mut adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(control_torrent_job());

        for command in [
            QueueCommand::Pause {
                job: JobId(1),
                reply: oneshot::channel().0,
            },
            QueueCommand::SetPriority {
                job: JobId(1),
                priority: 77,
                reply: oneshot::channel().0,
            },
        ] {
            owner.on_command(command);
            pump(&mut owner, &mut messages).await;
        }
        assert_eq!(owner.pending_backend_commands.len(), 1);
        assert_eq!(
            adapter.next_command().await,
            Some(BackendCommand::Pause { job: JobId(1) })
        );

        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::Resume {
            job: JobId(1),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(rx.await.unwrap());
        owner.schedule_torrent_starts();
        pump(&mut owner, &mut messages).await;
        assert_eq!(owner.pending_backend_commands.len(), 2);

        owner.flush_backend_commands();
        assert_eq!(
            adapter.next_command().await,
            Some(BackendCommand::SetPriority {
                job: JobId(1),
                priority: 77
            })
        );
        owner.flush_backend_commands();
        assert_eq!(
            adapter.next_command().await,
            Some(BackendCommand::Start { job: JobId(1) })
        );
    }

    #[tokio::test]
    async fn worker_mode_torrent_controls_do_not_require_queue_persistence() {
        let (_tmp, mut owner, mut adapter, mut messages) =
            control_test_owner_with_persistence(false, None);
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(control_torrent_job());

        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::Pause {
            job: JobId(1),
            reply,
        });
        pump(&mut owner, &mut messages).await;

        assert!(rx.await.unwrap());
        assert_eq!(
            adapter.next_command().await,
            Some(BackendCommand::Pause { job: JobId(1) })
        );
    }

    #[tokio::test]
    async fn retained_backend_commands_are_bounded_before_persisting_new_intent() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(control_torrent_job());

        for priority in 1..=MAX_PENDING_BACKEND_COMMANDS + 1 {
            let (reply, rx) = oneshot::channel();
            owner.on_command(QueueCommand::SetPriority {
                job: JobId(1),
                priority: priority as i32,
                reply,
            });
            pump(&mut owner, &mut messages).await;
            assert!(rx.await.unwrap());
        }
        assert_eq!(
            owner.pending_backend_commands.len(),
            MAX_PENDING_BACKEND_COMMANDS
        );

        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::SetPriority {
            job: JobId(1),
            priority: (MAX_PENDING_BACKEND_COMMANDS + 2) as i32,
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(!rx.await.unwrap());
        assert_eq!(
            owner.pending_backend_commands.len(),
            MAX_PENDING_BACKEND_COMMANDS
        );
        assert_eq!(
            owner.state.job(JobId(1)).unwrap().priority,
            (MAX_PENDING_BACKEND_COMMANDS + 1) as i32
        );
    }

    #[tokio::test]
    async fn latest_backend_progress_is_folded_without_a_fifo_backlog() {
        let (_tmp, mut owner, adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(control_torrent_job());
        for verified in 0..50_000 {
            adapter.progress(
                JobId(1),
                crate::backend::TransferProgress {
                    verified_bytes: verified,
                    ..Default::default()
                },
            );
        }
        owner.fold_backend_progress();
        pump(&mut owner, &mut messages).await;
        assert_eq!(
            owner
                .state
                .job(JobId(1))
                .unwrap()
                .torrent
                .as_ref()
                .unwrap()
                .downloaded_bytes,
            1,
            "the latest sample is folded once and clamped to selected bytes"
        );
    }

    fn bare_job() -> Job {
        Job {
            id: JobId(1),
            kind: nzbd_types::JobKind::Nzb,
            name: "x".into(),
            dir_name: String::new(),
            name_provisional: false,
            delivery_lifecycle: false,
            queued_at_unix: 0,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: Default::default(),
            params: vec![],
            files: vec![],
            totals: Default::default(),
            status: JobStatus::PostQueued,
            torrent: None,
            stages: vec![],
        }
    }

    fn pending_job(id: u32) -> Job {
        let mut job = bare_job();
        job.id = JobId(id);
        job.name = format!("job-{id}");
        job.dir_name = job.name.clone();
        job.status = JobStatus::Queued;
        job.files = vec![nzbd_types::FileEntry {
            id: FileId(id * 10),
            subject: "payload.bin".into(),
            filename: "payload.bin".into(),
            filename_confirmed: true,
            is_par2: false,
            paused: false,
            groups: vec!["alt.test".into()],
            date: None,
            segments: vec![nzbd_types::Segment {
                message_id: "part@example".into(),
                number: 1,
                size: 100,
                state: SegmentState::Pending,
            }],
            crc32: None,
            finalized: false,
        }];
        recompute_job_totals(&mut job);
        job
    }

    fn install_control_test_server(owner: &mut Owner) {
        owner.servers = Arc::new(vec![ServerDef {
            id: ServerId(1),
            name: "test".into(),
            host: "localhost".into(),
            port: 119,
            tls: nzbd_types::TlsMode::None,
            username: None,
            password: None,
            active: true,
            tier: 0,
            group: 0,
            fill: false,
            max_connections: 2,
            pipeline_depth: 1,
            retention_days: 0,
            cert_verification: nzbd_types::CertLevel::Strict,
        }]);
    }

    async fn block_control_test_io(owner: &mut Owner) -> std::sync::mpsc::Sender<()> {
        let (started, started_rx) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        owner.io_then(
            move || {
                let _ = started.send(());
                wait.recv().expect("release blocked control test IO");
                Ok(())
            },
            |_, result| result.expect("blocked control test IO completes"),
        );
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .expect("IO worker started")
            .expect("worker start signal");
        release
    }

    #[tokio::test]
    async fn pending_payload_allocation_does_not_starve_an_allocated_job() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        install_control_test_server(&mut owner);
        owner.state.jobs = vec![pending_job(1), pending_job(2)];
        allocate_for_test(&mut owner, &mut messages, JobId(2), false).await;
        owner.allocating_jobs.insert(JobId(1));

        let leases = grant_work_for_test(&mut owner, &mut messages, ServerId(1), 1).await;
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].r.job, JobId(2));
        assert!(matches!(
            &owner
                .state
                .segment_mut(SegRef {
                    job: JobId(1),
                    file: FileId(10),
                    seg_number: 1,
                })
                .unwrap()
                .state,
            SegmentState::Pending
        ));
    }

    #[tokio::test]
    async fn resume_waits_for_durable_revision_and_failure_keeps_both_schedulers_fenced() {
        // NZB lease path: a queued in-memory Resume stays ineligible while its
        // exact control revision is waiting on the ordered IO/snapshot lane.
        let (_tmp, mut owner, _adapter, mut messages) =
            control_test_owner_with_persistence(true, Some(Arc::new(|| false)));
        pump(&mut owner, &mut messages).await;
        install_control_test_server(&mut owner);
        let mut job = pending_job(1);
        job.status = JobStatus::Paused;
        owner.state.jobs.push(job);
        allocate_for_test(&mut owner, &mut messages, JobId(1), false).await;
        let release = block_control_test_io(&mut owner).await;
        let (reply, resumed) = oneshot::channel();
        owner.on_command(QueueCommand::Resume {
            job: JobId(1),
            reply,
        });
        assert!(owner.pending_control_revisions.contains_key(&JobId(1)));
        assert!(owner.grant_work(ServerId(1), 1).is_empty());
        assert!(
            owner.writer_for(JobId(1), FileId(10)).is_closed(),
            "direct writer acquisition must honor the same durable control fence"
        );
        release.send(()).unwrap();
        pump(&mut owner, &mut messages).await;
        assert!(!resumed.await.unwrap());
        assert!(owner.state.job(JobId(1)).unwrap().held());
        assert!(!owner.pending_control_revisions.contains_key(&JobId(1)));
        assert!(owner.grant_work(ServerId(1), 1).is_empty());

        // Torrent Start follows the same fence and remains stopped on failed
        // persistence, even though the in-memory control intent is Running.
        let (_tmp, mut owner, mut adapter, mut messages) =
            control_test_owner_with_persistence(true, Some(Arc::new(|| false)));
        pump(&mut owner, &mut messages).await;
        let mut torrent = control_torrent_job();
        torrent.status = JobStatus::Paused;
        let control = torrent.torrent.as_mut().unwrap();
        control.phase = TorrentPhase::PausedDownload;
        control.control_intent = TorrentControlIntent::Paused;
        owner.state.jobs.push(torrent);
        let release = block_control_test_io(&mut owner).await;
        let (reply, resumed) = oneshot::channel();
        owner.on_command(QueueCommand::Resume {
            job: JobId(1),
            reply,
        });
        owner.schedule_torrent_starts();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), adapter.next_command())
                .await
                .is_err()
        );
        release.send(()).unwrap();
        pump(&mut owner, &mut messages).await;
        assert!(!resumed.await.unwrap());
        assert!(owner.state.job(JobId(1)).unwrap().held());
        owner.schedule_torrent_starts();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), adapter.next_command())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn obsolete_resume_callback_cannot_release_newer_pause_revision() {
        let (_tmp, mut owner, mut adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        let mut torrent = control_torrent_job();
        torrent.status = JobStatus::Paused;
        let control = torrent.torrent.as_mut().unwrap();
        control.phase = TorrentPhase::PausedDownload;
        control.control_intent = TorrentControlIntent::Paused;
        owner.state.jobs.push(torrent);
        let release = block_control_test_io(&mut owner).await;

        let (resume_reply, resumed) = oneshot::channel();
        owner.on_command(QueueCommand::Resume {
            job: JobId(1),
            reply: resume_reply,
        });
        let resume_revision = *owner.pending_control_revisions.get(&JobId(1)).unwrap();
        let (pause_reply, paused) = oneshot::channel();
        owner.on_command(QueueCommand::Pause {
            job: JobId(1),
            reply: pause_reply,
        });
        let pause_revision = *owner.pending_control_revisions.get(&JobId(1)).unwrap();
        assert_ne!(resume_revision, pause_revision);
        owner.schedule_torrent_starts();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), adapter.next_command())
                .await
                .is_err()
        );

        release.send(()).unwrap();
        pump(&mut owner, &mut messages).await;
        assert!(!resumed.await.unwrap());
        assert!(paused.await.unwrap());
        assert!(!owner.pending_control_revisions.contains_key(&JobId(1)));
        assert_eq!(owner.state.job(JobId(1)).unwrap().status, JobStatus::Paused);
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_secs(1), adapter.next_command())
                    .await
                    .expect("durable Pause reaches the backend"),
                Some(BackendCommand::Pause { job: JobId(1) })
            ),
            "only the newer durable Pause may reach the backend"
        );
        owner.schedule_torrent_starts();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), adapter.next_command())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn url_download_allocates_fresh_directory_beside_retained_payload() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        let post = nzbd_nserv::build_post("release", &[("payload.bin", vec![7; 100])], 100);
        let parsed = nzbd_nzb::parse(post.nzb.as_bytes()).unwrap();
        let id = owner
            .state
            .admit_url("release".into(), "http://example.test/file.nzb", None, 0);
        let retained = owner.dest_dir.join("release");
        std::fs::create_dir_all(&retained).unwrap();
        std::fs::write(retained.join("old.bin"), b"retained bytes").unwrap();
        let collision = owner.dest_dir.join(format!("release.job-{}-0", id.0));
        std::fs::create_dir(&collision).unwrap();

        let (reply, result) = oneshot::channel();
        owner.on_command(QueueCommand::CompleteUrlFetch {
            job: id,
            parsed: Box::new(parsed),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(result.await.unwrap());
        let job = owner.state.job(id).unwrap();
        assert_eq!(job.name, "release");
        assert_eq!(job.status, JobStatus::Queued);
        let expected = format!("release.job-{}-1", id.0);
        assert_eq!(job.dir_name, expected);
        let allocation = owner.artifacts.for_job(id.0).unwrap().unwrap();
        assert_eq!(allocation.path, owner.dest_dir.join(&expected));
        owner
            .artifacts
            .allocate(id.0, &owner.dest_dir, &allocation.path)
            .unwrap();
        assert_eq!(
            std::fs::read(retained.join("old.bin")).unwrap(),
            b"retained bytes"
        );
        assert!(collision.is_dir());
        let saved = SnapshotStore::open(&owner.state_dir)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert_eq!(
            saved.jobs.iter().find(|j| j.id == id).unwrap().dir_name,
            expected
        );
    }

    #[tokio::test]
    async fn allocation_refusal_holds_only_that_job_and_release_retries_it() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        owner.servers = Arc::new(vec![ServerDef {
            id: ServerId(1),
            name: "test".into(),
            host: "localhost".into(),
            port: 119,
            tls: nzbd_types::TlsMode::None,
            username: None,
            password: None,
            active: true,
            tier: 0,
            group: 0,
            fill: false,
            max_connections: 1,
            pipeline_depth: 1,
            retention_days: 0,
            cert_verification: nzbd_types::CertLevel::Strict,
        }]);
        owner.state.jobs = (1..=3).map(pending_job).collect();
        let conflict = owner.dest_dir.join("job-1");
        std::fs::create_dir_all(&conflict).unwrap();
        let leases = grant_work_for_test(&mut owner, &mut messages, ServerId(1), 1).await;
        assert_eq!(leases.len(), 1, "a hold must not consume the lease budget");
        assert_eq!(leases[0].r.job, JobId(2));
        assert_eq!(owner.state.jobs[0].control().unwrap().cause, "allocation");
        assert_eq!(
            grant_work_for_test(&mut owner, &mut messages, ServerId(1), 1).await[0]
                .r
                .job,
            JobId(3)
        );
        let revision = owner.state.jobs[0].control().unwrap().revision;
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::ReleaseResourceHold {
            by: RetryBy::Operator,
            job: JobId(1),
            revision: revision.clone(),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(rx.await.unwrap());
        assert!(
            grant_work_for_test(&mut owner, &mut messages, ServerId(1), 1)
                .await
                .is_empty()
        );
        let held = owner.state.jobs[0].control().unwrap();
        assert_eq!(held.lifecycle, "held");
        assert!(held.revision.parse::<u64>().unwrap() > revision.parse::<u64>().unwrap());
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::ReleaseResourceHold {
            by: RetryBy::Operator,
            job: JobId(1),
            revision,
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(!rx.await.unwrap(), "stale release must be rejected");
        std::fs::remove_dir(&conflict).unwrap();
        let mut epoch = owner.epoch_tx.subscribe();
        epoch.borrow_and_update();
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::ReleaseResourceHold {
            by: RetryBy::Operator,
            job: JobId(1),
            revision: owner.state.jobs[0].control().unwrap().revision,
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(rx.await.unwrap());
        assert!(epoch.has_changed().unwrap());
        assert_eq!(
            grant_work_for_test(&mut owner, &mut messages, ServerId(1), 1).await[0]
                .r
                .job,
            JobId(1)
        );
    }

    use nzbd_types::HoldCause;

    /// The release an operator `resume` runs, minus the off-loop part: the
    /// custody check is done here exactly as `EngineHandle::release_hold`
    /// does it in `spawn_blocking`.
    async fn operator_retry(
        owner: &mut Owner,
        messages: &mut mpsc::Receiver<EngineMsg>,
        job: JobId,
    ) -> Result<(), String> {
        let control = owner.state.job(job).unwrap().control().unwrap();
        let custody_checked =
            control.post_side() && owner.artifacts.validate_post_retry(job.0).is_ok();
        let result = owner.retry_hold(job, &control.revision, RetryBy::Operator, custody_checked);
        pump(owner, messages).await;
        result
    }

    #[tokio::test]
    async fn post_hold_retry_checks_revision_custody_and_stage_then_persists() {
        let (tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        let mut job = bare_job();
        job.dir_name = "pp".into();
        owner.state.jobs.push(job);
        allocate_for_test(&mut owner, &mut messages, JobId(1), false).await;
        assert!(owner.hold_job(JobId(1), HoldCause::PostFailure, "unpack", "tool failed"));
        pump(&mut owner, &mut messages).await;
        let revision = owner.state.jobs[0].control().unwrap().revision;
        assert!(owner
            .retry_hold(JobId(1), "stale", RetryBy::Operator, true)
            .is_err());
        pump(&mut owner, &mut messages).await;
        assert!(
            owner
                .retry_hold(JobId(1), &revision, RetryBy::Operator, false)
                .is_err(),
            "a post-side release needs its custody checked off-loop first"
        );
        pump(&mut owner, &mut messages).await;
        assert_eq!(
            owner.retry_hold(JobId(1), &revision, RetryBy::Operator, true),
            Ok(())
        );
        pump(&mut owner, &mut messages).await;
        assert_eq!(owner.state.jobs[0].status, JobStatus::PostQueued);
        let persisted = SnapshotStore::open(&tmp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        let restored = QueueState::from_doc(persisted);
        assert!(!restored.jobs[0].held());
        // A post-side retry always restarts post-processing from the
        // beginning, so the stage a hold names never makes it unsafe; one
        // this build does not recognise must still have an exit.
        assert!(owner.hold_job(
            JobId(1),
            HoldCause::Unrecognized("from_a_newer_build".into()),
            "unrecognised_stage",
            "why"
        ));
        pump(&mut owner, &mut messages).await;
        assert_eq!(
            operator_retry(&mut owner, &mut messages, JobId(1)).await,
            Ok(())
        );
        assert_eq!(owner.state.jobs[0].status, JobStatus::PostQueued);
    }

    /// Field report 2026-11-01: `/processing` lost its transport (ENOTCONN)
    /// mid-download and the job sat HELD with only delete to click. The
    /// errno makes it `storage_unavailable`, and its retry time is in the
    /// same save as the hold.
    #[tokio::test]
    async fn storage_errno_writer_error_schedules_itself() {
        let (tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(pending_job(1));
        let before = unix_now();
        owner.on_msg(EngineMsg::WriterError {
            job: JobId(1),
            file: FileId(10),
            error: "write /processing/x/.runner-file-10.part: Transport endpoint is not connected (os error 107)".into(),
        });
        pump(&mut owner, &mut messages).await;
        let held = owner.state.jobs[0].control().unwrap();
        assert_eq!(held.cause, "storage_unavailable");
        assert_eq!(held.retry_policy, "scheduled_retry");
        assert_eq!(held.retry_attempt, 1);
        let at = held
            .next_retry_at_unix
            .expect("scheduled in the hold itself");
        assert!(
            at >= before + 30 && at <= unix_now() + 30,
            "first retry after 30 s: {at}"
        );
        let persisted = SnapshotStore::open(&tmp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert_eq!(
            QueueState::from_doc(persisted).jobs[0]
                .control()
                .unwrap()
                .next_retry_at_unix,
            Some(at),
            "the schedule is durable with the hold, not a second write"
        );

        assert_eq!(
            owner.retry_hold(JobId(1), &held.revision, RetryBy::Recovery, false),
            Ok(())
        );
        pump(&mut owner, &mut messages).await;
        let job = &owner.state.jobs[0];
        assert!(!job.held());
        assert_eq!(
            job.status,
            JobStatus::Queued,
            "a download-side hold resumes the download"
        );
        assert!(job
            .control()
            .unwrap()
            .message
            .starts_with("Automatic retry 1"));

        // Still broken: held again, one step further out, same outage start.
        assert!(owner.hold_job(
            JobId(1),
            HoldCause::StorageUnavailable,
            "download_write",
            "still ENOTCONN"
        ));
        pump(&mut owner, &mut messages).await;
        let again = owner.state.jobs[0].control().unwrap();
        assert_eq!(again.retry_attempt, 2);
        assert!(again.next_retry_at_unix.unwrap() >= unix_now() + 59);
        assert_eq!(again.blocked_since_unix, held.blocked_since_unix);

        // A refusal pushes it out again and says why.
        let revision = again.revision.clone();
        assert!(owner.reschedule_hold(JobId(1), &revision, "volume still unavailable"));
        let pushed = owner.state.jobs[0].control().unwrap();
        assert_eq!(pushed.retry_attempt, 3);
        assert!(pushed
            .message
            .ends_with("; retry refused: volume still unavailable"));
        assert!(
            !owner.reschedule_hold(JobId(1), &revision, "stale"),
            "a reschedule is a new revision; a second actor on the old one is refused"
        );
        assert!(owner.reschedule_hold(JobId(1), &pushed.revision, "and again"));
        assert_eq!(
            owner.state.jobs[0]
                .control()
                .unwrap()
                .message
                .matches("retry refused")
                .count(),
            1,
            "refusals replace each other instead of piling up"
        );
    }

    /// A digest or size mismatch fails the same way on every attempt: it
    /// waits for a person instead of re-checking a whole file forever.
    #[tokio::test]
    async fn permanent_writer_error_waits_for_review() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(pending_job(1));
        owner.on_msg(EngineMsg::WriterError {
            job: JobId(1),
            file: FileId(10),
            error: "finalize: complete checkpoint digest differs".into(),
        });
        pump(&mut owner, &mut messages).await;
        let held = owner.state.jobs[0].control().unwrap();
        assert_eq!(
            (held.cause.as_str(), held.stage.as_str()),
            ("io", "finalize")
        );
        assert_eq!(held.retry_policy, "review");
        assert_eq!(held.next_retry_at_unix, None);
        assert!(
            owner
                .retry_hold(JobId(1), &held.revision, RetryBy::Recovery, false)
                .is_ok(),
            "the owner itself does not police policy; the recovery task never asks"
        );
        pump(&mut owner, &mut messages).await;
    }

    #[tokio::test]
    async fn paused_hold_waits_for_the_operator_and_followers_never_auto_release() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(pending_job(1));
        assert!(owner.hold_job(
            JobId(1),
            HoldCause::StorageUnavailable,
            "download_write",
            "EIO (os error 5)"
        ));
        pump(&mut owner, &mut messages).await;
        let mut c = owner.state.jobs[0].control().unwrap();
        c.manual_pause = true;
        owner.state.jobs[0].set_control(&c);
        assert!(owner
            .retry_hold(JobId(1), &c.revision, RetryBy::Recovery, false)
            .is_err());
        pump(&mut owner, &mut messages).await;
        owner.persist = false;
        c.manual_pause = false;
        owner.state.jobs[0].set_control(&c);
        assert_eq!(
            owner.retry_hold(JobId(1), &c.revision, RetryBy::Recovery, false),
            Err("this node is not the queue authority".into())
        );
        pump(&mut owner, &mut messages).await;
        owner.persist = true;
        c.manual_pause = true;
        c.retry_attempt = 4;
        owner.state.jobs[0].set_control(&c);
        assert_eq!(
            owner.retry_hold(JobId(1), &c.revision, RetryBy::Operator, false),
            Ok(())
        );
        pump(&mut owner, &mut messages).await;
        let c = owner.state.jobs[0].control().unwrap();
        assert!(
            !c.manual_pause && c.retry_attempt == 0,
            "an operator retry clears both"
        );
        assert_eq!(owner.state.jobs[0].status, JobStatus::Queued);
    }

    /// The closed set: every cause this build writes, on both sides of the
    /// pipeline, stamps its policy (with a time when the policy is timed)
    /// and answers an operator release. Storage causes take
    /// `ReleaseResourceHold` after their probe; the rest take `retry_hold`.
    #[tokio::test]
    async fn every_hold_cause_has_an_exit() {
        let causes = HoldCause::ALL
            .iter()
            .cloned()
            .chain([HoldCause::Unrecognized("from_a_newer_build".into())]);
        for cause in causes {
            for post in [false, true] {
                let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
                pump(&mut owner, &mut messages).await;
                let mut job = bare_job();
                job.dir_name = "exit".into();
                job.status = if post {
                    JobStatus::Post {
                        stage: nzbd_types::PostStage::Unpack,
                    }
                } else {
                    JobStatus::Downloading
                };
                owner.state.jobs.push(job);
                allocate_for_test(&mut owner, &mut messages, JobId(1), false).await;
                assert!(
                    owner.hold_job(JobId(1), cause.clone(), "stage", "why"),
                    "{cause}"
                );
                pump(&mut owner, &mut messages).await;
                let control = owner.state.jobs[0].control().unwrap();
                let policy = cause.policy();
                assert_eq!(control.retry_policy, policy.as_str());
                assert_eq!(
                    control.next_retry_at_unix.is_some(),
                    policy.timed(),
                    "{cause}"
                );
                if cause.is_storage() {
                    let (reply, rx) = oneshot::channel();
                    owner.on_command(QueueCommand::ReleaseResourceHold {
                        by: RetryBy::Operator,
                        job: JobId(1),
                        revision: control.revision,
                        reply,
                    });
                    pump(&mut owner, &mut messages).await;
                    assert!(rx.await.unwrap(), "{cause} post={post} has no exit");
                } else {
                    assert_eq!(
                        operator_retry(&mut owner, &mut messages, JobId(1)).await,
                        Ok(()),
                        "{cause} post={post} has no exit"
                    );
                }
                assert!(!owner.state.jobs[0].held(), "{cause} post={post}");
                assert_eq!(
                    owner.state.jobs[0].status,
                    if post {
                        JobStatus::PostQueued
                    } else {
                        JobStatus::Queued
                    },
                    "{cause} post={post} goes back to the step it stopped in"
                );
            }
        }
    }

    /// §3.3: `import_job` keeps a held fence, which is why a scheduling
    /// write that went through it could never land. The fence stays; the
    /// schedule now lives in `hold_job`, and only owner methods change it.
    #[tokio::test]
    async fn held_control_survives_only_owner_writes() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(pending_job(1));
        assert!(owner.hold_job(JobId(1), HoldCause::Io, "download_write", "digest differs"));
        pump(&mut owner, &mut messages).await;
        let held = owner.state.jobs[0].control().unwrap();
        let mut row = owner.state.jobs[0].clone();
        let mut forged = held.clone();
        forged.retry_policy = "scheduled_retry".into();
        forged.next_retry_at_unix = Some(1);
        row.set_control(&forged);
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::ImportJobIfPresent {
            job: Box::new(row),
            require_snapshot: false,
            reply,
        });
        pump(&mut owner, &mut messages).await;
        let _ = rx.await;
        assert_eq!(
            owner.state.jobs[0].control().unwrap(),
            held,
            "imports cannot rewrite a fence"
        );
        // RescheduleHold is an owner write: it does change a fence.
        let mut storage = held.clone();
        storage.cause = "storage_unavailable".into();
        storage.retry_policy = "scheduled_retry".into();
        owner.state.jobs[0].set_control(&storage);
        assert!(owner.reschedule_hold(JobId(1), &storage.revision, "still down"));
        assert_ne!(owner.state.jobs[0].control().unwrap(), storage);
    }

    /// Review finding: `previous_status` was sticky across holds, so a job
    /// first held while downloading and later held in post-processing was
    /// released to the download queue without its custody check.
    #[tokio::test]
    async fn rehold_after_release_routes_by_where_the_job_is_now() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(pending_job(1));
        assert!(owner.hold_job(
            JobId(1),
            HoldCause::StorageUnavailable,
            "download_write",
            "x (os error 107)"
        ));
        pump(&mut owner, &mut messages).await;
        let first = owner.state.jobs[0].control().unwrap();
        assert!(!first.post_side());
        owner
            .retry_hold(JobId(1), &first.revision, RetryBy::Recovery, false)
            .unwrap();
        pump(&mut owner, &mut messages).await;
        // The download finished; post-processing hits the dead mount.
        owner.state.jobs[0].status = JobStatus::Post {
            stage: nzbd_types::PostStage::PostUnpackRename,
        };
        assert!(owner.hold_job(
            JobId(1),
            HoldCause::StorageUnavailable,
            "post_unpack_rename",
            "y (os error 5)"
        ));
        pump(&mut owner, &mut messages).await;
        let second = owner.state.jobs[0].control().unwrap();
        assert!(second.post_side(), "this hold happened in post-processing");
        assert!(
            owner
                .retry_hold(JobId(1), &second.revision, RetryBy::Recovery, false)
                .is_err(),
            "so its release must go through the custody check"
        );
        pump(&mut owner, &mut messages).await;
        assert_eq!(
            second.retry_attempt, 2,
            "a re-hold right after a release is the same outage"
        );
        assert_eq!(second.blocked_since_unix, first.blocked_since_unix);
        assert!(!second.manual_pause);

        // An hour after the last release, a new hold is a new episode.
        let mut released = second.clone();
        released.lifecycle = "running".into();
        released.released_at_unix = Some(unix_now() - nzbd_types::HOLD_EPISODE_SECONDS - 1);
        released.blocked_since_unix = Some(1);
        owner.state.jobs[0].set_control(&released);
        owner.state.jobs[0].status = JobStatus::Downloading;
        assert!(owner.hold_job(
            JobId(1),
            HoldCause::StorageUnavailable,
            "download_write",
            "z (os error 107)"
        ));
        pump(&mut owner, &mut messages).await;
        let fresh = owner.state.jobs[0].control().unwrap();
        assert_eq!(fresh.retry_attempt, 1, "backoff starts over");
        assert_ne!(
            fresh.blocked_since_unix,
            Some(1),
            "so does the outage clock"
        );
        assert!(!fresh.post_side());
    }

    /// Review finding: an operator retry on a paused storage hold left the
    /// job paused, and the probe's spacing outlived its episode.
    #[tokio::test]
    async fn operator_storage_release_runs_the_job_and_resets_the_probe() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(pending_job(1));
        assert!(owner.hold_job(JobId(1), HoldCause::Capacity, "download_write", "full"));
        pump(&mut owner, &mut messages).await;
        let mut c = owner.state.jobs[0].control().unwrap();
        c.manual_pause = true;
        owner.state.jobs[0].set_control(&c);
        // A probe just ran: the recovery task must wait, a person need not.
        owner.state.jobs[0].params.extend([
            (
                "*Control:probe_after".to_string(),
                (unix_now() + 999).to_string(),
            ),
            ("*Control:attempts".to_string(), "6".to_string()),
        ]);
        for (by, expected) in [(RetryBy::Recovery, false), (RetryBy::Operator, true)] {
            let (reply, rx) = oneshot::channel();
            owner.on_command(QueueCommand::BeginResourceProbe {
                job: JobId(1),
                revision: c.revision.clone(),
                by,
                reply,
            });
            pump(&mut owner, &mut messages).await;
            assert_eq!(rx.await.unwrap(), expected, "{by:?}");
        }
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::ReleaseResourceHold {
            job: JobId(1),
            revision: c.revision.clone(),
            by: RetryBy::Operator,
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(rx.await.unwrap());
        let job = &owner.state.jobs[0];
        assert_eq!(
            job.status,
            JobStatus::Queued,
            "an operator retry runs the job"
        );
        let c = job.control().unwrap();
        assert!(!c.manual_pause && c.retry_attempt == 0 && c.next_retry_at_unix.is_none());
        assert!(c.released_at_unix.is_some());
        assert!(
            !job.params
                .iter()
                .any(|(k, _)| k.starts_with("*Control:probe_after")
                    || k.starts_with("*Control:attempts")),
            "the probe's spacing belongs to the episode that ended"
        );
    }

    #[tokio::test]
    async fn reobserve_refusal_records_why_without_moving_its_cadence() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(pending_job(1));
        assert!(owner.hold_job(
            JobId(1),
            HoldCause::MountUnavailable,
            "extract",
            "root changed"
        ));
        pump(&mut owner, &mut messages).await;
        let c = owner.state.jobs[0].control().unwrap();
        assert_eq!(c.next_retry_at_unix, None);
        assert!(owner.reschedule_hold(JobId(1), &c.revision, "wrong root mounted"));
        let after = owner.state.jobs[0].control().unwrap();
        assert_eq!(
            after.next_retry_at_unix, None,
            "re-observed every pass, not timed"
        );
        assert_eq!(after.retry_attempt, c.retry_attempt);
        assert!(after
            .message
            .ends_with("; retry refused: wrong root mounted"));
        owner.persist = false;
        assert!(
            !owner.reschedule_hold(JobId(1), &c.revision, "follower"),
            "a node that is not the authority writes nothing"
        );
    }

    #[test]
    fn rearm_reclassifies_by_errno_and_derives_policy() {
        let hold = |cause: &str, policy: &str, message: &str| nzbd_types::JobControl {
            version: 1,
            revision: "4".into(),
            lifecycle: "held".into(),
            cause: cause.into(),
            stage: "post_unpack_rename".into(),
            retry_policy: policy.into(),
            message: message.into(),
            instance: "i".into(),
            previous_status: None,
            manual_pause: false,
            retry_attempt: 0,
            next_retry_at_unix: None,
            blocked_since_unix: Some(1),
            released_at_unix: None,
        };
        // #2181 exactly.
        let fixed = rearmed_hold(
            hold("io", "review", "io: Input/output error (os error 5)"),
            1000,
        )
        .expect("re-armed");
        assert_eq!(fixed.cause, "storage_unavailable");
        assert_eq!(fixed.retry_policy, "scheduled_retry");
        assert_eq!(
            fixed.next_retry_at_unix,
            Some(1000),
            "due on the first pass"
        );
        assert_eq!(fixed.revision, "5");
        // #2183 exactly.
        assert!(rearmed_hold(
            hold(
                "io",
                "review",
                "write /processing/x.part: Transport endpoint is not connected (os error 107)"
            ),
            1000
        )
        .is_some_and(|c| c.cause == "storage_unavailable"));
        // A permanent io failure stays with a person.
        assert!(rearmed_hold(
            hold("io", "review", "complete checkpoint digest differs"),
            1000
        )
        .is_none());
        // Policy this build changed: password_required used to claim resume_same_job.
        assert_eq!(
            rearmed_hold(hold("password_required", "resume_same_job", "x"), 1000)
                .unwrap()
                .retry_policy,
            "review"
        );
        // Capacity used to carry no time.
        assert_eq!(
            rearmed_hold(hold("capacity", "resume_same_job", "x"), 1000)
                .unwrap()
                .next_retry_at_unix,
            Some(1000)
        );
        // Already right: untouched.
        assert!(rearmed_hold(hold("identity_conflict", "review", "x"), 1000).is_none());
        let mut scheduled = hold("storage_unavailable", "scheduled_retry", "x");
        scheduled.next_retry_at_unix = Some(5);
        assert!(rearmed_hold(scheduled, 1000).is_none());
    }

    /// Cluster engines start without `persist`, so `Owner::new` recovery
    /// never runs for them; adoption must re-arm too.
    #[tokio::test]
    async fn adopting_authority_rearms_holds() {
        let (tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        let mut job = pending_job(1);
        job.set_control(&nzbd_types::JobControl {
            version: 1,
            revision: "1".into(),
            lifecycle: "held".into(),
            cause: "io".into(),
            stage: "download_write".into(),
            retry_policy: "review".into(),
            message: "write x: Transport endpoint is not connected (os error 107)".into(),
            instance: "i".into(),
            previous_status: Some(JobStatus::Downloading),
            manual_pause: false,
            retry_attempt: 0,
            next_retry_at_unix: None,
            blocked_since_unix: Some(1),
            released_at_unix: None,
        });
        job.status = JobStatus::Paused;
        let mut doc_state = QueueState::default();
        doc_state.jobs.push(job);
        SnapshotStore::open(&tmp.path().join("state"))
            .unwrap()
            .save(&doc_state.to_doc())
            .unwrap();
        owner.persist = false;
        owner.state.jobs.clear();
        owner.adopt_authority().unwrap();
        pump(&mut owner, &mut messages).await;
        let c = owner.state.jobs[0].control().unwrap();
        assert_eq!(
            (c.cause.as_str(), c.retry_policy.as_str()),
            ("storage_unavailable", "scheduled_retry")
        );
        assert!(c.next_retry_at_unix.is_some());
    }

    #[tokio::test]
    async fn mount_unavailable_hold_resumes_only_after_expected_root_returns() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        let mut job = bare_job();
        job.dir_name = "remount".into();
        job.status = JobStatus::Completed;
        job.params.push(("monarr-delivery".into(), "1".into()));
        owner.state.jobs.push(job);
        allocate_for_test(&mut owner, &mut messages, JobId(1), false).await;
        assert!(owner.hold_job(
            JobId(1),
            HoldCause::MountUnavailable,
            "extract",
            "configured root identity changed or volume is unavailable",
        ));
        pump(&mut owner, &mut messages).await;
        let root = owner.dest_dir.clone();
        let offline = root.with_extension("expected-volume-offline");
        std::fs::rename(&root, &offline).unwrap();
        std::fs::create_dir(&root).unwrap();
        assert!(
            operator_retry(&mut owner, &mut messages, JobId(1))
                .await
                .is_err(),
            "wrong root must stay fenced"
        );
        assert!(owner.state.jobs[0].held());

        std::fs::remove_dir(&root).unwrap();
        std::fs::rename(&offline, &root).unwrap();
        assert_eq!(
            operator_retry(&mut owner, &mut messages, JobId(1)).await,
            Ok(()),
            "restored expected root should resume"
        );
        assert_eq!(owner.state.jobs[0].status, JobStatus::PostQueued);
    }

    #[tokio::test]
    async fn admission_tombstone_refreshes_exact_source_keep_and_delete_evidence() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        let mut job = bare_job();
        job.dir_name = "admission-source".into();
        owner.state.jobs.push(job);
        allocate_for_test(&mut owner, &mut messages, JobId(1), false).await;
        let source = owner.artifacts.for_job(1).unwrap().unwrap();
        owner
            .artifacts
            .finish(1, &source.path, &source.root, "retained")
            .unwrap();
        let source = owner.artifacts.for_job(1).unwrap().unwrap();
        owner
            .state
            .url_admission_receipts
            .push(UrlAdmissionReceipt {
                protocol_version: 1,
                request_token: "receipt-token".into(),
                submission_sha256: "hash".into(),
                job_id: 1,
                generation: "admission-generation".into(),
                runner_instance_id: "runner".into(),
                terminal_outcome: Some("FAILURE/UNPACK".into()),
                outcome: "failed".into(),
                failure_stage: Some("unpack".into()),
                failure_cause: Some("corrupt_archive".into()),
                custody_state: "runner_owned".into(),
                keep_source_on_failure: false,
                source_cleanup_status: None,
                publication_path: None,
                source_artifact_id: Some(source.id.clone()),
                source_generation: Some(source.generation.clone()),
            });

        let kept = owner
            .artifacts
            .retention(&source.id, source.revision, true, None)
            .unwrap();
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::GetUrlAdmission {
            request_token: "receipt-token".into(),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        let receipt = rx.await.unwrap().unwrap();
        assert_eq!(receipt.custody_state, "kept_by_user");
        assert_eq!(
            receipt.source_generation.as_deref(),
            Some(kept.generation.as_str())
        );

        let released = owner
            .artifacts
            .retention(&kept.id, kept.revision, false, None)
            .unwrap();
        let deletion = owner
            .artifacts
            .request_delete(
                &released.id,
                released.revision,
                "delete-admission-source",
                0,
            )
            .unwrap();
        owner.artifacts.execute_delete(&deletion.id).unwrap();
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::GetUrlAdmission {
            request_token: "receipt-token".into(),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        let receipt = rx.await.unwrap().unwrap();
        assert_eq!(receipt.custody_state, "deleted_by_runner");
        assert_eq!(receipt.source_cleanup_status.as_deref(), Some("removed"));

        // A missing or reused artifact ID is not proof of deletion.
        let mut receipt = owner.state.url_admission_receipts[0].clone();
        receipt.custody_state = "runner_owned".into();
        receipt.source_cleanup_status = None;
        receipt.source_generation = Some("different-generation".into());
        let unchanged = refresh_url_admission_custody(&mut receipt, &owner.artifacts);
        assert!(!unchanged);
        assert_eq!(receipt.custody_state, "runner_owned");
    }

    #[tokio::test]
    async fn writer_retirement_does_not_create_an_allocation_review_hold() {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(pending_job(1));
        owner.retiring_writers.insert(JobId(1), Vec::new());
        assert!(owner.ensure_allocation(JobId(1), false).is_err());
        assert!(!owner.state.jobs[0].held());
    }

    fn restarted_recovery_job(id: u32, state: SegmentState) -> Job {
        let mut job = pending_job(id);
        job.status = JobStatus::Queued;
        job.stages.push(StageSpan {
            stage: PostStage::ParVerify,
            started_at_unix: 1_000,
            ms: None,
        });
        job.files[0].filename = "release.vol00+01.par2".into();
        job.files[0].is_par2 = true;
        job.files[0].segments[0].state = state;
        job.files[0].finalized = matches!(state, SegmentState::Done { .. });
        recompute_job_totals(&mut job);
        job
    }

    #[tokio::test]
    async fn writer_resource_errors_hold_pending_segments_and_latch_out_of_space() {
        let (_tmp, mut owner, _epoch, mut messages) = guard_test_owner(Tuning::default());
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(pending_job(2));

        owner.on_msg(EngineMsg::WriterError {
            job: JobId(2),
            file: FileId(20),
            error: "write payload.bin: No space left on device (os error 28)".into(),
        });
        pump(&mut owner, &mut messages).await;
        let job = owner.state.job(JobId(2)).unwrap();
        assert_eq!(job.files[0].segments[0].state, SegmentState::Pending);
        assert!(!job.files[0].finalized);
        assert_eq!(job.status, JobStatus::Paused);
        assert!(job.held());
        assert!(!job.ready());
        assert!(owner.disk_low);
        assert!(owner.enospc_latched);
        assert_eq!(owner.enospc_observed, 1);

        // Re-reporting an already-terminal segment is idempotent, while a
        // missing file/job is ignored rather than corrupting another row.
        owner.on_msg(EngineMsg::WriterError {
            job: JobId(2),
            file: FileId(20),
            error: "permission denied".into(),
        });
        pump(&mut owner, &mut messages).await;
        owner.on_msg(EngineMsg::WriterError {
            job: JobId(2),
            file: FileId(999),
            error: "gone".into(),
        });
        pump(&mut owner, &mut messages).await;
        owner.on_msg(EngineMsg::SegmentWritten {
            job: JobId(999),
            file: FileId(999),
            seg_number: 1,
            offset: 0,
            len: 1,
            crc: 0,
            file_size: 1,
            server: ServerId(1),
        });
        pump(&mut owner, &mut messages).await;
        owner.on_msg(EngineMsg::WriterFinalized {
            job: JobId(999),
            file: FileId(999),
            ok: true,
            final_path: None,
            combined_crc: None,
        });
        pump(&mut owner, &mut messages).await;

        owner.disk_low = false;
        let (reply, received) = oneshot::channel();
        owner.on_msg(EngineMsg::WorkRequest {
            server: ServerId(999),
            max: 1,
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(received.await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_fenced_replace_restores_delegation_and_mirror_overlay() {
        let (_tmp, mut owner, _adapter, mut messages) =
            control_test_owner_with_persistence(true, Some(Arc::new(|| false)));
        pump(&mut owner, &mut messages).await;
        let original = pending_job(3);
        owner.state.jobs.push(original.clone());
        owner.delegated.insert(JobId(3), "worker-a".into());
        owner.mirror.insert(
            JobId(3),
            MirrorStats {
                repair_progress: None,
                done_articles: 4,
                failed_articles: 1,
                downloaded_bytes: 40,
                health: 800,
                remaining_bytes: Some(60),
                stages: vec![StageSpan {
                    stage: PostStage::ParVerify,
                    started_at_unix: 1_000,
                    ms: None,
                }],
            },
        );
        let mut replacement = original;
        replacement.priority = 99;
        let (reply, mut received) = oneshot::channel();
        owner.on_command(QueueCommand::ImportJobIfPresent {
            job: Box::new(replacement),
            require_snapshot: true,
            reply,
        });
        pump(&mut owner, &mut messages).await;

        assert!(!received.try_recv().unwrap());
        assert_eq!(owner.state.job(JobId(3)).unwrap().priority, 0);
        assert_eq!(
            owner.delegated.get(&JobId(3)).map(String::as_str),
            Some("worker-a")
        );
        assert_eq!(owner.mirror[&JobId(3)].downloaded_bytes, 40);
        assert_eq!(
            owner.mirror[&JobId(3)].stages[0].stage,
            PostStage::ParVerify
        );
    }

    #[tokio::test]
    async fn legacy_remote_pp_remaining_fallback_stays_paused_aware() {
        let (_tmp, mut owner, _epoch, mut messages) = guard_test_owner(Tuning::default());
        pump(&mut owner, &mut messages).await;
        let mut job = pending_job(4);
        job.status = JobStatus::Completed;
        job.files[0].paused = true;
        recompute_job_totals(&mut job);
        owner.state.jobs.push(job);
        owner.delegated.insert(JobId(4), "old-worker".into());
        owner.mirror.insert(
            JobId(4),
            MirrorStats {
                repair_progress: None,
                done_articles: 1,
                failed_articles: 0,
                downloaded_bytes: 0,
                health: 999,
                remaining_bytes: None,
                stages: vec![StageSpan {
                    stage: PostStage::ParVerify,
                    started_at_unix: 1_000,
                    ms: None,
                }],
            },
        );

        owner.publish_now();
        let snapshot = owner.shared.load();
        assert_eq!(snapshot.jobs[0].remaining_bytes, 0);
        assert_eq!(snapshot.remaining_bytes, 0);
    }

    /// The monotonic figure the post manager measured is what lands on the
    /// span. Wall-clock is a fallback, not the primary — an NTP step
    /// during a long repair must not produce a duration nobody can trust.
    #[test]
    fn close_span_prefers_the_measured_duration() {
        let mut j = bare_job();
        j.stages.push(StageSpan {
            stage: PostStage::ParRepair,
            started_at_unix: 1_000,
            ms: None,
        });
        // Wall clock says 60s; the manager measured 4500ms.
        close_span(&mut j, Some(4500), 1_060);
        assert_eq!(j.stages[0].ms, Some(4500));
    }

    /// A span that outlived the process has no `Instant` left to consult,
    /// so the seconds that survived in the snapshot are all there is.
    #[test]
    fn close_span_falls_back_to_wall_clock() {
        let mut j = bare_job();
        j.stages.push(StageSpan {
            stage: PostStage::Unpack,
            started_at_unix: 1_000,
            ms: None,
        });
        close_span(&mut j, None, 1_007);
        assert_eq!(j.stages[0].ms, Some(7_000));
    }

    /// `Stages::finish` closes the span and `Drop` closes it again. The
    /// second close must not overwrite the first — otherwise every job's
    /// last stage would report the few milliseconds between the two.
    #[test]
    fn close_span_is_idempotent() {
        let mut j = bare_job();
        j.stages.push(StageSpan {
            stage: PostStage::Move,
            started_at_unix: 1_000,
            ms: None,
        });
        close_span(&mut j, Some(9_000), 1_009);
        close_span(&mut j, Some(3), 1_009);
        assert_eq!(j.stages[0].ms, Some(9_000));
    }

    /// A clock that went backwards between entering and leaving a stage
    /// yields 0, never a wrapped-around u64.
    #[test]
    fn close_span_survives_a_backwards_clock() {
        let mut j = bare_job();
        j.stages.push(StageSpan {
            stage: PostStage::ParVerify,
            started_at_unix: 2_000,
            ms: None,
        });
        close_span(&mut j, None, 1_900);
        assert_eq!(j.stages[0].ms, Some(0));
    }

    #[test]
    fn close_span_on_a_job_that_never_post_processed() {
        let mut j = bare_job();
        close_span(&mut j, Some(5), 1_000);
        assert!(j.stages.is_empty());
    }

    /// Delayed PAR fetching temporarily returns the download half of the job
    /// to Queued/Downloading, but verification keeps running in parallel. The
    /// selected fetch returns to the open Post stage when it ends. Only the
    /// verifier-selected PAR may use that lane; generic FileResume and a
    /// PP-only node cannot broaden it to ordinary files.
    #[tokio::test]
    async fn delayed_par_download_keeps_the_open_post_span() {
        let (_tmp, mut owner, _epoch, mut messages) = guard_test_owner(Tuning::default());
        pump(&mut owner, &mut messages).await;
        let mut job = pending_job(402);
        job.status = JobStatus::Post {
            stage: PostStage::ParVerify,
        };
        job.stages.push(StageSpan {
            stage: PostStage::ParVerify,
            started_at_unix: 1_000,
            ms: None,
        });
        job.files[0].filename = "release.vol00+01.par2".into();
        job.files[0].is_par2 = true;
        job.files[0].paused = true;
        let recovery_file = job.files[0].id;
        let mut unrelated = job.files[0].clone();
        unrelated.id = FileId(4021);
        unrelated.filename = "release.part001.rar".into();
        unrelated.is_par2 = false;
        unrelated.paused = true;
        job.files.push(unrelated);
        recompute_job_totals(&mut job);
        owner.state.jobs.push(job);

        assert_eq!(owner.unpause_par_blocks(JobId(402), 1, None), 1);
        pump(&mut owner, &mut messages).await;
        assert_eq!(
            owner.state.job(JobId(402)).unwrap().status,
            JobStatus::Queued
        );
        assert!(owner.state.job(JobId(402)).unwrap().stages[0].ms.is_none());
        assert_eq!(
            owner.post_fetch_files[&JobId(402)],
            HashSet::from([recovery_file])
        );

        let (reply, mut result) = oneshot::channel();
        owner.on_command(QueueCommand::SetFilePaused {
            job: JobId(402),
            file: FileId(4021),
            paused: false,
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(
            !result.try_recv().unwrap(),
            "FileResume is not recovery authority"
        );

        // Even if an unrelated file is already unpaused in imported/stale
        // state, selector enforcement still admits only the explicit PAR.
        owner.state.job_mut(JobId(402)).unwrap().files[1].paused = false;
        owner.delegated.insert(JobId(402), "this-node".into());
        owner.download_enabled = false;
        let servers = vec![ServerDef {
            id: ServerId(1),
            name: "provider".into(),
            host: "127.0.0.1".into(),
            port: 119,
            tls: nzbd_types::TlsMode::None,
            username: None,
            password: None,
            active: true,
            tier: 0,
            group: 0,
            fill: false,
            max_connections: 1,
            pipeline_depth: 1,
            retention_days: 0,
            cert_verification: nzbd_types::CertLevel::Strict,
        }];
        let ladder = Ladder::new(&servers);
        let not_blocked = |_: ServerId| false;
        let mut ctx = SelectionCtx {
            ladder: &ladder,
            attempts: &mut owner.attempts,
            is_blocked: &not_blocked,
            delegated: &owner.delegated,
            post_fetch_files: &owner.post_fetch_files,
            regular_downloads: owner.download_enabled,
            article_retries: 3,
            now_unix: 1_800_000_000,
            propagation_delay_secs: 0,
            soft_hold: false,
            rotate: 0,
        };
        let selected = next_for_server(&owner.state, &servers[0], &mut ctx)
            .lease
            .unwrap();
        assert_eq!(selected.file, recovery_file);
        owner.delegated.remove(&JobId(402));

        let job = owner.state.job_mut(JobId(402)).unwrap();
        job.files[1].paused = true;
        job.files[0].segments[0].state = SegmentState::Done {
            offset: 0,
            len: 100,
            crc: 0,
        };
        job.files[0].finalized = true;
        recompute_job_totals(job);
        let mut events = owner.events.subscribe();
        owner.check_job_complete(JobId(402));
        pump(&mut owner, &mut messages).await;
        let job = owner.state.job(JobId(402)).unwrap();
        assert_eq!(
            job.status,
            JobStatus::Post {
                stage: PostStage::ParVerify
            },
            "finishing a recovery-volume download resumes the open PP stage"
        );
        assert!(!owner.post_fetch_files.contains_key(&JobId(402)));
        assert!(
            job.stages[0].ms.is_none(),
            "the post manager, not delayed download completion, closes verification"
        );
        assert!(
            matches!(
                events.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ),
            "a delayed PAR fetch must not announce a second download completion"
        );
    }

    #[tokio::test]
    async fn restarted_delayed_par_completion_recovers_from_the_persisted_open_span() {
        let (_tmp, mut owner, _epoch, mut messages) = guard_test_owner(Tuning::default());
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(restarted_recovery_job(
            403,
            SegmentState::Done {
                offset: 0,
                len: 100,
                crc: 0,
            },
        ));
        assert!(
            !owner.post_fetch_files.contains_key(&JobId(403)),
            "the authorization map is intentionally absent after restart"
        );

        let mut events = owner.events.subscribe();
        owner.check_job_complete(JobId(403));
        pump(&mut owner, &mut messages).await;
        assert_eq!(
            owner.state.job(JobId(403)).unwrap().status,
            JobStatus::Post {
                stage: PostStage::ParVerify
            }
        );
        assert!(matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn restarted_pp_only_node_reauthorizes_only_the_pending_par_file() {
        let (_tmp, mut owner, _epoch, mut messages) = guard_test_owner(Tuning::default());
        pump(&mut owner, &mut messages).await;
        owner.download_enabled = false;
        let mut job = restarted_recovery_job(406, SegmentState::Pending);
        let recovery_file = job.files[0].id;
        let mut unrelated = job.files[0].clone();
        unrelated.id = FileId(4061);
        unrelated.filename = "release.part001.rar".into();
        unrelated.is_par2 = false;
        job.files.push(unrelated);
        owner.import_job(job, false, false);
        pump(&mut owner, &mut messages).await;

        assert_eq!(
            owner.post_fetch_files[&JobId(406)],
            HashSet::from([recovery_file]),
            "restart recovery may authorize the selected PAR and nothing else"
        );
        let servers = vec![ServerDef {
            id: ServerId(1),
            name: "provider".into(),
            host: "127.0.0.1".into(),
            port: 119,
            tls: nzbd_types::TlsMode::None,
            username: None,
            password: None,
            active: true,
            tier: 0,
            group: 0,
            fill: false,
            max_connections: 1,
            pipeline_depth: 1,
            retention_days: 0,
            cert_verification: nzbd_types::CertLevel::Strict,
        }];
        let ladder = Ladder::new(&servers);
        let not_blocked = |_: ServerId| false;
        let mut ctx = SelectionCtx {
            ladder: &ladder,
            attempts: &mut owner.attempts,
            is_blocked: &not_blocked,
            delegated: &owner.delegated,
            post_fetch_files: &owner.post_fetch_files,
            regular_downloads: owner.download_enabled,
            article_retries: 3,
            now_unix: 1_800_000_000,
            propagation_delay_secs: 0,
            soft_hold: false,
            rotate: 0,
        };
        let selected = next_for_server(&owner.state, &servers[0], &mut ctx)
            .lease
            .expect("the PP-only node should resume its recovery download");
        assert_eq!(selected.file, recovery_file);
    }

    #[tokio::test]
    async fn failed_recovery_articles_return_to_verification_instead_of_disposition() {
        let (_tmp, mut owner, _epoch, mut messages) = guard_test_owner(Tuning::default());
        pump(&mut owner, &mut messages).await;
        let mut job = restarted_recovery_job(404, SegmentState::Failed);
        let mut failed_payload = job.files[0].clone();
        failed_payload.id = FileId(4041);
        failed_payload.filename = "release.part001.rar".into();
        failed_payload.is_par2 = false;
        failed_payload.segments[0].state = SegmentState::Failed;
        job.files.push(failed_payload);
        recompute_job_totals(&mut job);
        assert_eq!(final_status(&job).0, JobStatus::Failed);
        owner.state.jobs.push(job);

        let mut events = owner.events.subscribe();
        owner.check_job_complete(JobId(404));
        pump(&mut owner, &mut messages).await;
        assert_eq!(
            owner.state.job(JobId(404)).unwrap().status,
            JobStatus::Post {
                stage: PostStage::ParVerify
            }
        );
        assert!(matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn recovery_write_failure_stays_owned_by_the_active_post_task() {
        let (_tmp, mut owner, _epoch, mut messages) = guard_test_owner(Tuning::default());
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(restarted_recovery_job(
            405,
            SegmentState::Done {
                offset: 0,
                len: 100,
                crc: 0,
            },
        ));
        owner
            .write_failures
            .insert(JobId(405), "write recovery volume: no space left".into());

        let mut events = owner.events.subscribe();
        owner.check_job_complete(JobId(405));
        pump(&mut owner, &mut messages).await;
        assert_eq!(
            owner.state.job(JobId(405)).unwrap().status,
            JobStatus::Post {
                stage: PostStage::ParVerify
            }
        );
        assert!(matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn url_refetch_plan_keeps_first_fails_dupes() {
        let a = "https://indexer.example/getnzb/aaa.nzb&i=1&r=k".to_string();
        let b = "https://indexer.example/getnzb/bbb.nzb&i=1&r=k".to_string();
        let pending = vec![
            (JobId(4), a.clone()),
            (JobId(5), a.clone()),
            (JobId(7), b.clone()),
            (JobId(9), a.clone()),
        ];
        let (refetch, dupes) = plan_url_refetches(pending);
        assert_eq!(refetch, vec![(JobId(4), a), (JobId(7), b)]);
        assert_eq!(dupes, vec![(JobId(5), JobId(4)), (JobId(9), JobId(4))]);
    }

    #[test]
    fn url_refetch_plan_empty() {
        let (refetch, dupes) = plan_url_refetches(Vec::new());
        assert!(refetch.is_empty());
        assert!(dupes.is_empty());
    }

    #[test]
    fn scoped_url_refetch_preserves_distinct_manual_tokens_and_replays_same_token() {
        let url = "https://indexer.example/release.nzb".to_string();
        let request = |job, token: &str| UrlFetchRequest {
            job: JobId(job),
            url: url.clone(),
            request_token: Some(token.into()),
            manual_additional: true,
        };
        let (refetch, dupes) = plan_url_refetches_scoped(vec![
            request(10, "transfer-a"),
            request(11, "transfer-b"),
            request(12, "transfer-a"),
        ]);
        assert_eq!(refetch, vec![(JobId(10), url.clone()), (JobId(11), url)]);
        assert_eq!(dupes, vec![(JobId(12), JobId(10))]);
    }

    #[test]
    fn scoped_url_refetch_does_not_collapse_manual_additional_without_token() {
        let url = "https://indexer.example/release.nzb".to_string();
        let request = |job| UrlFetchRequest {
            job: JobId(job),
            url: url.clone(),
            request_token: None,
            manual_additional: true,
        };
        let (refetch, dupes) = plan_url_refetches_scoped(vec![request(20), request(21)]);
        assert_eq!(refetch, vec![(JobId(20), url.clone()), (JobId(21), url)]);
        assert!(dupes.is_empty());
    }

    #[test]
    fn token_replay_matches_terminal_job_without_overwriting_changed_submission() {
        let url = "https://indexer.example/release.nzb";
        let mut original = bare_job();
        original.id = JobId(31);
        original.status = JobStatus::Failed;
        original.params = vec![
            ("*URL".into(), url.into()),
            ("monarr-transfer".into(), "transfer-31".into()),
            ("monarr-delivery".into(), "1".into()),
            ("monarr-manual-additional".into(), "1".into()),
        ];
        let jobs = vec![original.clone()];
        let replay = find_url_request(&jobs, url, Some("transfer-31"), true).unwrap();
        assert_eq!(replay.id, JobId(31));
        assert!(url_request_matches(replay, url, true, true));
        assert!(!url_request_matches(
            replay,
            "https://indexer.example/another.nzb",
            true,
            true
        ));
        assert!(!url_request_matches(replay, url, false, true));
        assert!(find_url_request(&jobs, url, Some("new-transfer"), true).is_none());
    }

    #[test]
    fn save_spacing_scales_with_save_cost() {
        // Fast volume: the historical 2 s cadence.
        assert_eq!(save_spacing(0), Duration::from_secs(2));
        assert_eq!(save_spacing(120), Duration::from_secs(2));
        // A 1 s save gets 10 s between saves (~10% duty cycle)…
        assert_eq!(save_spacing(1_000), Duration::from_secs(10));
        // …the field report's 65 s save gets spaced way out…
        assert_eq!(save_spacing(65_000), Duration::from_secs(300));
        // …and the cap holds even for absurd values.
        assert_eq!(save_spacing(u64::MAX / 20), Duration::from_secs(300));
    }

    #[test]
    fn incomplete_probe_cannot_clear_a_forecast_hold() {
        let reading = DiskGuardReading {
            available_bytes: Some(10_000),
            all_roots_known: false,
            ..Default::default()
        };
        assert_eq!(
            disk_guard_decision(true, false, &reading, 100),
            (false, true)
        );
    }

    #[test]
    fn incomplete_probe_cannot_clear_an_observed_write_latch() {
        let reading = DiskGuardReading {
            available_bytes: Some(10_000),
            all_roots_known: false,
            ..Default::default()
        };
        assert_eq!(disk_guard_decision(true, true, &reading, 100), (true, true));
    }

    #[test]
    fn recovered_high_known_root_plus_unknown_sibling_retains_hold() {
        let reading = DiskGuardReading {
            available_bytes: Some(1_000_000_000),
            all_roots_known: false,
            ..Default::default()
        };
        assert_eq!(
            disk_guard_decision(true, false, &reading, 100),
            (false, true)
        );
    }

    #[test]
    fn complete_high_probe_releases_a_forecast_hold() {
        let reading = DiskGuardReading {
            available_bytes: Some(101),
            all_roots_known: true,
            ..Default::default()
        };
        assert_eq!(
            disk_guard_decision(true, false, &reading, 100),
            (false, false)
        );
    }

    #[test]
    fn write_latch_requires_twice_the_floor_from_a_complete_probe() {
        let below_clear = DiskGuardReading {
            available_bytes: Some(199),
            all_roots_known: true,
            ..Default::default()
        };
        let at_clear = DiskGuardReading {
            available_bytes: Some(200),
            all_roots_known: true,
            ..Default::default()
        };
        assert_eq!(
            disk_guard_decision(true, true, &below_clear, 100),
            (true, true)
        );
        assert_eq!(
            disk_guard_decision(true, true, &at_clear, 100),
            (false, false)
        );
    }

    #[tokio::test]
    async fn clearing_disk_guard_wakes_parked_connection_tasks() {
        let tuning = Tuning {
            min_free_disk_bytes: 100,
            ..Tuning::default()
        };
        let (_tmp, mut owner, mut epoch, mut messages) = guard_test_owner(tuning);
        pump(&mut owner, &mut messages).await;
        assert!(owner.disk_low, "a configured floor starts fail-safe");
        let _ = epoch.borrow_and_update();
        assert!(!epoch.has_changed().unwrap());

        owner.disk_guard.store(Arc::new(DiskGuardReading {
            available_bytes: Some(101),
            all_roots_known: true,
            ..Default::default()
        }));
        owner.update_disk_guard();

        assert!(!owner.disk_low, "the complete high probe clears the hold");
        assert!(
            epoch.has_changed().unwrap(),
            "a task parked on the work epoch must be woken"
        );
    }

    #[tokio::test]
    async fn clearing_quota_guard_wakes_parked_connection_tasks() {
        let tuning = Tuning {
            daily_quota_bytes: 1,
            ..Tuning::default()
        };
        let (_tmp, mut owner, mut epoch, mut messages) = guard_test_owner(tuning);
        pump(&mut owner, &mut messages).await;
        owner.quota_reached = true;
        let _ = epoch.borrow_and_update();
        assert!(!epoch.has_changed().unwrap());

        // The empty volume book is deterministically below the configured
        // quota, directly exercising the rollover/recovery transition.
        owner.update_quota_guard();
        pump(&mut owner, &mut messages).await;

        assert!(
            !owner.quota_reached,
            "the fresh quota period clears the hold"
        );
        assert!(
            epoch.has_changed().unwrap(),
            "a task parked on the work epoch must be woken"
        );
    }
}
