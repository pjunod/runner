//! The queue-owner task (ARCHITECTURE.md §8.1): the single serialization
//! point for all queue mutation. Inputs: a bounded command/event channel and
//! a 1 Hz tick. Outputs: leases granted to connection tasks (pull model),
//! `arc-swap` snapshots for lock-free readers, broadcast events, journal
//! appends and debounced snapshot saves.
//!
//! Every handler is synchronous — the owner never awaits while reasoning
//! about state. Sends toward writer tasks use `try_send` with a
//! retry-on-tick fallback so owner ⇄ writer backpressure can never deadlock.

#[path = "owner_native_failure.rs"]
mod native_failure;
#[path = "owner_lifecycle.rs"]
mod native_lifecycle;
#[path = "owner_native_seed.rs"]
mod native_seed;
#[path = "owner_terminal.rs"]
mod terminal_owner;
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
    if job.lifecycle_ledger().is_none() {
        job.set_lifecycle_ledger(&nzbd_types::LifecycleLedger::default());
    }
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

#[derive(Debug)]
pub(crate) enum WorkerRetirement {
    Revoked(nzbd_state::artifacts::WorkerControlContext),
    Accepted(nzbd_state::artifacts::WorkerControlContext),
}

impl WorkerRetirement {
    fn context(&self) -> &nzbd_state::artifacts::WorkerControlContext {
        match self {
            Self::Revoked(context) | Self::Accepted(context) => context,
        }
    }
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
    BeginFailedCleanupAdmission {
        job: JobId,
        reply: oneshot::Sender<Option<crate::FailedCleanupAdmission>>,
    },
    FailedCleanupMetadata {
        admission: crate::PostAdmission,
        reply: oneshot::Sender<Option<Job>>,
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
        reply: oneshot::Sender<Result<Option<Result<JobId, JobId>>, crate::EngineError>>,
    },
    NoteSeedKeepBlock {
        job: JobId,
        marker: Box<crate::torrent_acquisitions::ManagedSeedRetirement>,
        operation: Option<String>,
        reply: oneshot::Sender<bool>,
    },
    RearmSeedKeepCleared {
        job: JobId,
        reply: oneshot::Sender<bool>,
    },
    SyncManagedSeedKeep {
        donor: JobId,
        reply: oneshot::Sender<bool>,
    },
    BeginManagedSeedRetirement {
        job: JobId,
        reply: oneshot::Sender<bool>,
    },
    CompleteManagedSeedRetirement {
        job: JobId,
        identity: String,
        reply: oneshot::Sender<bool>,
    },
    ClaimNativeFailureCleanup {
        job: JobId,
        reply: oneshot::Sender<Option<Job>>,
    },
    FailNativeTorrent {
        job: JobId,
        donor: Option<JobId>,
        reason: nzbd_types::FailureReason,
        fetch_cause: Option<nzbd_types::FetchCause>,
        stage: String,
        message: String,
        cleanup_complete: bool,
        reply: oneshot::Sender<bool>,
    },
    NativeTorrentPending {
        job: JobId,
        reply: oneshot::Sender<Option<nzbd_state::PendingAdmission>>,
    },
    ReserveManagedTorrentAdmission {
        source: nzbd_types::TorrentSource,
        submission_sha256: String,
        opts: AddOpts,
        defaults: crate::torrent_acquisitions::ManagedTorrentDefaults,
        reply: oneshot::Sender<
            Result<crate::torrent_acquisitions::ManagedTorrentReservation, crate::EngineError>,
        >,
    },
    ConfirmManagedTorrentSource {
        job: JobId,
        submission_sha256: String,
        reply: oneshot::Sender<bool>,
    },
    CommitManagedTorrentAdmission {
        job: JobId,
        commit: Box<crate::queue::TorrentAdmissionCommit>,
        submission_sha256: String,
        descriptor: crate::torrent_acquisitions::NativeDescriptorRef,
        reply: oneshot::Sender<
            Result<crate::torrent_acquisitions::ManagedTorrentAdmission, crate::EngineError>,
        >,
    },
    StartTorrentAcquisition {
        job: JobId,
        reply: oneshot::Sender<bool>,
    },
    CompleteTorrentAcquisition {
        job: JobId,
        donor: JobId,
        source: Box<nzbd_state::artifacts::Artifact>,
        reply: oneshot::Sender<bool>,
    },
    HoldTorrentAcquisition {
        job: JobId,
        cause: nzbd_types::HoldCause,
        message: String,
        reply: oneshot::Sender<bool>,
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
    ReserveUrlFiles {
        job: JobId,
        source_identity: String,
        count: u32,
        reply: oneshot::Sender<Result<u32, crate::EngineError>>,
    },
    CompleteUrlFetch {
        job: JobId,
        source_identity: String,
        prepared: Box<nzbd_state::artifacts::UrlResolutionRecord>,
        reply: oneshot::Sender<Result<bool, crate::EngineError>>,
    },
    UpdateUrlFetchProgress {
        job: JobId,
        progress: crate::fetch::FetchRetryState,
        reply: oneshot::Sender<Result<bool, crate::EngineError>>,
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
    PrepareClusterWritersStop {
        ticket: nzbd_state::artifacts::WorkerStopTicket,
        execution_joined: bool,
        reply: oneshot::Sender<Result<crate::cluster_stop::StopPreparation, String>>,
    },
    ForgetClusterWriterStops {
        context: nzbd_state::artifacts::WorkerControlContext,
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
    ImportFence {
        job: JobId,
        reply: oneshot::Sender<Result<Option<String>, crate::EngineError>>,
    },
    EndImportPreparation {
        job: JobId,
        reply: oneshot::Sender<()>,
    },
    ImportJob {
        job: Box<Job>,
        preparation: Option<crate::imports::PreparedImport>,
        authority: crate::imports::ImportAuthority,
        emit_finished: bool,
        reply: oneshot::Sender<Result<(), crate::EngineError>>,
    },
    /// Replace only an authority copy that still exists. This is the fenced
    /// terminal-update path: a demoted worker must never resurrect a job that
    /// `RetainJobs` already removed while an external disposition awaited.
    ImportJobIfPresent {
        job: Box<Job>,
        require_snapshot: bool,
        completion_proof: Option<nzbd_state::artifacts::WorkerCompletionProof>,
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
        retirement: Option<WorkerRetirement>,
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
    UnacceptedUrlAdmissions {
        limit: usize,
        reply: oneshot::Sender<Result<Vec<UrlAdmissionReceipt>, String>>,
    },
    UrlAdmissionsPage {
        after: String,
        limit: usize,
        reply: oneshot::Sender<Result<Vec<UrlAdmissionReceipt>, String>>,
    },
    BeginUrlAdmissionAdoption {
        reply: oneshot::Sender<bool>,
    },
    AdoptUrlAdmissions {
        rows: Vec<UrlAdmissionReceipt>,
        reply: oneshot::Sender<bool>,
    },
    FinishUrlAdmissionAdoption {
        reply: oneshot::Sender<bool>,
    },
    CheckDiscardUrlAdmission {
        expected: Box<UrlAdmissionReceipt>,
        reply: oneshot::Sender<bool>,
    },
    DiscardUrlAdmission {
        expected: Box<UrlAdmissionReceipt>,
        reply: oneshot::Sender<bool>,
    },
    AdoptReplicatedHighwater {
        next_job_id: u32,
        next_file_id: u32,
        reply: oneshot::Sender<bool>,
    },
    SetConsumerInstanceId {
        instance: String,
        reply: oneshot::Sender<bool>,
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
        guard: crate::hold_release::HoldReleaseGuard,
        reply: oneshot::Sender<bool>,
    },
    ReleaseResourceHold {
        job: JobId,
        revision: String,
        by: RetryBy,
        guard: crate::hold_release::HoldReleaseGuard,
        reply: oneshot::Sender<bool>,
    },
    /// End a non-storage hold (`Owner::retry_hold`); the error says why not.
    /// A post-side hold needs `custody_checked`: the payload custody check
    /// touches the mount, so the caller runs it off this loop first.
    RetryHold {
        job: JobId,
        revision: String,
        by: RetryBy,
        guard: crate::hold_release::HoldReleaseGuard,
        custody_checked: bool,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Move an automatic hold's next attempt out on its backoff, recording
    /// why the last one was refused.
    AckAutomaticControl {
        job: JobId,
        identity: String,
        reply: oneshot::Sender<bool>,
    },
    RescheduleHold {
        job: JobId,
        revision: String,
        reason: String,
        guard: crate::hold_release::HoldReleaseGuard,
        reply: oneshot::Sender<bool>,
    },
    /// Contract §2.3: end an expired hold as `failed/hold_expired`.
    ExpireHold {
        job: JobId,
        revision: String,
        reply: oneshot::Sender<bool>,
    },
    /// Contract §2.2: end a job as `failed/<reason>`.
    FailJob {
        job: JobId,
        reason: nzbd_types::FailureReason,
        fetch_cause: Option<nzbd_types::FetchCause>,
        stage: String,
        message: String,
        reply: oneshot::Sender<Option<nzbd_state::artifacts::JobTerminal>>,
    },
    /// Contract §2.1: run the terminal chain for a producer-built row.
    RecordTerminal {
        terminal: Box<nzbd_state::artifacts::JobTerminal>,
        /// The producer's history store when it has its own (post-processing
        /// is handed one); otherwise the engine's.
        history: Option<crate::TerminalHistory>,
        reply: oneshot::Sender<Option<nzbd_state::artifacts::JobTerminal>>,
    },
    ReplayTerminals {
        reply: oneshot::Sender<()>,
    },
    HoldJob {
        job: JobId,
        cause: nzbd_types::HoldCause,
        stage: String,
        message: String,
        not_before: Option<i64>,
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
        /// The hold the writer's `io::Error` named by errno
        /// ([`crate::io_error_hold_cause`]); `None` is a terminal `failed/io`.
        cause: Option<nzbd_types::HoldCause>,
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

#[derive(Clone, Copy, PartialEq, Eq)]
struct PostRollback {
    generation: u64,
    control_revision: Option<u64>,
    in_flight: bool,
}

type JobTerminalRow = nzbd_state::artifacts::JobTerminal;

pub(crate) struct Owner {
    state: QueueState,
    admission_adoption: Option<HashSet<String>>,
    admission_catalog_adopted: bool,
    authoritative_admissions: HashMap<String, String>,
    unaccepted_admissions: HashMap<String, UrlAdmissionReceipt>,
    consumer_instance_id: String,
    timing_cache: crate::lifecycle::TimingCache,
    attempts: HashMap<SegRef, SegmentAttempt>,
    blocked: HashMap<ServerId, Instant>,
    writers: HashMap<FileId, WriterHandle>,
    writer_epochs: HashMap<FileId, u64>,
    job_epochs: HashMap<JobId, u64>,
    control_epochs: HashMap<JobId, u64>,
    post_admissions: HashMap<JobId, u64>,
    post_cancellations: HashMap<JobId, CancellationToken>,
    failed_cleanup_admissions: HashMap<JobId, nzbd_state::artifacts::FailedCleanupProof>,
    record_transitions: HashSet<JobId>,
    post_rollbacks: HashMap<JobId, PostRollback>,
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
    #[cfg(test)]
    work_selection_attempts: usize,
    allocating_jobs: HashSet<JobId>,
    pending_journal_loads: HashMap<JobId, (u64, usize)>,
    pending_control_commits: HashSet<JobId>,
    /// Jobs whose terminal (contract §2.1) is being recorded: the row,
    /// history, markers and emit run as one chain and nothing else may
    /// transition the job meanwhile.
    terminals_in_flight: HashSet<JobId>,
    /// Contract §2.1 (review #280-4): a terminal whose row never landed (the
    /// `job_terminals` insert failed, or a transient fence refused it) is
    /// kept here and retried on the replay tick. Nothing may complete,
    /// admit to post-processing or re-terminate the job meanwhile, so a
    /// released storage hold cannot turn a recorded `io` into a `completed`.
    deferred_terminals:
        HashMap<JobId, (JobTerminalRow, Option<Arc<nzbd_state::history::HistoryDb>>)>,
    last_terminal_replay: Option<Instant>,
    /// The last projected status/stage published per job, so every
    /// observable transition emits one `job_status_changed` (§2.4).
    last_status: HashMap<JobId, (&'static str, Option<&'static str>)>,
    /// Exact queue control revision currently awaiting its durable snapshot.
    /// Unlike the unrelated set of commit barriers, this protects overlapping
    /// Pause/Resume intents from releasing one another's operational fence.
    pending_control_revisions: HashMap<JobId, u64>,
    cluster_writer_stops: crate::cluster_stop::WriterStops,
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
    import_preparations: HashMap<JobId, u32>,
    foreign_journal_holds: HashSet<JobId>,
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
    /// An operator rate edit fences asynchronous authority-load rollback.
    speed_limit_revision: u64,
    applied_usenet_limit: Option<Option<u64>>,
    applied_torrent_limit: Option<Option<u64>>,
    history: Option<Arc<nzbd_state::history::HistoryDb>>,

    state_dir: PathBuf,
    persistence: Persistence,
    persistence_callbacks: HashMap<u64, Continuation>,
    // Saturated journal admission is covered by one coalesced owner snapshot.
    // Stop leasing until that snapshot durably covers the latest accepted fact.
    journal_checkpoint: Option<u64>,
    journal_checkpoint_inflight: bool,
    journal_checkpoint_retry_at: Instant,
    journal_suffix: String,
    payload_waiting: VecDeque<PayloadWork>,
    payload_active: HashSet<PathBuf>,
    io_callbacks: HashMap<u64, IoContinuation>,
    pending_sources: nzbd_state::torrent_sources::PendingSourceStore,
    pub(crate) url_resolutions: Arc<nzbd_state::artifacts::UrlResolutionStore>,
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
    persistence_fenced: bool,
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
        JobStatus::Queued | JobStatus::Downloading | JobStatus::Paused | JobStatus::Held
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
        let url_resolutions = Arc::new(
            nzbd_state::artifacts::UrlResolutionStore::open(state_dir)
                .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?,
        );
        let journal = JobJournals::open(state_dir, journal_suffix)?;
        // Contract §2.1: every terminal has a durable history row. The daemon
        // always passes its store; an engine built without one (tests, tools)
        // keeps a private one beside its queue, so no event carries a
        // made-up `history_seq`.
        let history = match history {
            Some(history) => history,
            None => Arc::new(nzbd_state::history::HistoryDb::open(
                &state_dir.join("engine-history.sqlite"),
                None,
            )?),
        };
        let history = Some(history);

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
            let mut retired = HashSet::new();
            for job in &state.jobs {
                if url_resolutions
                    .is_retired(job)
                    .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?
                {
                    retired.insert(job.id);
                }
            }
            state.jobs.retain(|job| !retired.contains(&job.id));
            for receipt in &mut state.url_admission_receipts {
                if retired.contains(&JobId(receipt.job_id)) && receipt.custody_state != "curator" {
                    receipt.outcome = "deleted".into();
                }
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
                    if let Some(size) = merge_replayed_size(&mut state, &rec) {
                        file_sizes.insert(rec.file, size);
                    }
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
                // A 0.5.0 snapshot stores a hold as `paused` + a held control.
                crate::normalize_stored_hold_status(j, "queue snapshot");
                drop_retired_params(j);
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
        state.next_file_id = state.next_file_id.max(
            url_resolutions
                .reserved_file_id()
                .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?,
        );
        let initial_disk_hold = tuning.min_free_disk_bytes > 0;
        let initial_quota_hold = tuning.daily_quota_bytes > 0 || tuning.monthly_quota_bytes > 0;
        let mut owner = Owner {
            state,
            admission_adoption: None,
            admission_catalog_adopted: false,
            authoritative_admissions: HashMap::new(),
            unaccepted_admissions: HashMap::new(),
            consumer_instance_id: artifacts.installation.clone(),
            attempts: HashMap::new(),
            blocked: HashMap::new(),
            writers: HashMap::new(),
            writer_epochs: HashMap::new(),
            job_epochs: HashMap::new(),
            control_epochs: HashMap::new(),
            post_admissions: HashMap::new(),
            post_cancellations: HashMap::new(),
            failed_cleanup_admissions: HashMap::new(),
            record_transitions: HashSet::new(),
            post_rollbacks: HashMap::new(),
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
            #[cfg(test)]
            work_selection_attempts: 0,
            allocating_jobs: HashSet::new(),
            pending_journal_loads: HashMap::new(),
            pending_control_commits: HashSet::new(),
            terminals_in_flight: HashSet::new(),
            deferred_terminals: HashMap::new(),
            last_terminal_replay: None,
            last_status: HashMap::new(),
            pending_control_revisions: HashMap::new(),
            cluster_writer_stops: Default::default(),
            allocated_jobs,
            finalize_sent: HashSet::new(),
            pending_finalize: Vec::new(),
            pending_name_inspections: HashMap::new(),
            file_sizes,
            write_failures: HashMap::new(),
            delegated: HashMap::new(),
            mirror: HashMap::new(),
            post_fetch_files,
            import_preparations: HashMap::new(),
            foreign_journal_holds: HashSet::new(),
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
            speed_limit_revision: fresh_epoch(),
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
            journal_checkpoint: None,
            journal_checkpoint_inflight: false,
            journal_checkpoint_retry_at: Instant::now(),
            io_callbacks: HashMap::new(),
            payload_waiting: VecDeque::new(),
            payload_active: HashSet::new(),
            journal_suffix: journal_suffix.to_owned(),
            pending_sources,
            url_resolutions,
            persistence_fenced: false,
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
            timing_cache: crate::lifecycle::TimingCache::default(),
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
        // Contract §2.1/§2.3: finish interrupted terminal chains, give every
        // stored hold its deadline and convert legacy terminal holds before
        // hold recovery's first pass or the post manager can act on them.
        self.arm_hold_deadlines();
        self.convert_legacy_holds();
        self.replay_terminals();
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
            tracing::error!(
                "queue owner stopped with final durability unresolved; unclean marker and recovery evidence retained"
            );
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
                    if self.failed_cleanup_admissions.contains_key(&admission.job)
                        && matches!(command.as_ref(), QueueCommand::ExportJob { .. })
                    {
                        if let QueueCommand::ExportJob { reply, .. } = *command {
                            let _ = reply.send(None);
                        }
                        return;
                    }
                    if self.failed_cleanup_admissions.contains_key(&admission.job)
                        && !matches!(
                            command.as_ref(),
                            QueueCommand::ImportJobIfPresent { .. }
                                | QueueCommand::ApplyPostRecord { .. }
                                | QueueCommand::FailedCleanupMetadata { .. }
                                | QueueCommand::ExportJob { .. }
                                | QueueCommand::EndPostAdmission { .. }
                        )
                    {
                        return;
                    }
                    match *command {
                        QueueCommand::ImportJobIfPresent {
                            job,
                            require_snapshot,
                            completion_proof,
                            reply,
                        } => self.apply_post_record_proven(
                            admission,
                            *job,
                            require_snapshot,
                            completion_proof,
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
                let cleanup_event = !self.failed_cleanup_admissions.contains_key(&admission.job)
                    || (matches!(&event, Event::JobPpFinished { job, outcome, reason, .. }
                            if *job == admission.job
                                && *outcome == nzbd_types::TerminalOutcome::Failed
                                && reason.is_some_and(|reason| reason.history_status().starts_with("FAILURE/")))
                        && self.state.job(admission.job).is_some_and(|job| {
                            job.params
                                .iter()
                                .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
                        }));
                if self.post_scope_current(&admission) && cleanup_event {
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
            EngineMsg::WriterError {
                job,
                file,
                error,
                cause,
            } => {
                tracing::warn!(job = job.0, file = file.0, %error, "writer error; failing file");
                // The errno decides whether retrying can help: a full disk
                // waits for space, a dropped mount retries on its own, and
                // anything else (a digest or size mismatch) is the job's
                // terminal failure rather than a re-check forever.
                if crate::out_of_space(cause.as_ref()) {
                    self.observe_out_of_space(&error);
                }
                let stage = if error.starts_with("finalize") {
                    "finalize"
                } else {
                    "download_write"
                };
                match cause {
                    Some(cause) => {
                        self.hold_job(job, cause, stage, &error);
                    }
                    // Contract §2.2: a write failure retrying cannot fix is
                    // the job's terminal `failed/io`, parked, never blamed.
                    None => {
                        self.fail_job_terminal(
                            job,
                            nzbd_types::FailureReason::Io,
                            None,
                            stage,
                            &error,
                        );
                    }
                }
            }
        }
    }

    fn on_command(&mut self, cmd: QueueCommand) {
        // Rejection must precede every busy-path cancellation, writer stop or
        // deferral: an old grant cannot revoke custody belonging to its successor.
        if self.stale_worker_retirement(&cmd) {
            if let QueueCommand::RemoveJobSilent { reply, .. } = cmd {
                let _ = reply.send(false);
            }
            return;
        }
        let pending_job = match &cmd {
            QueueCommand::Pause { job, .. }
            | QueueCommand::Resume { job, .. }
            | QueueCommand::SetPriority { job, .. }
            | QueueCommand::SetCategory { job, .. }
            | QueueCommand::SetKeepOnFailure { job, .. }
            | QueueCommand::HoldJob { job, .. }
            | QueueCommand::ExpireHold { job, .. }
            | QueueCommand::FailJob { job, .. }
            | QueueCommand::RetryHold { job, .. }
            | QueueCommand::RescheduleHold { job, .. }
            | QueueCommand::BeginResourceProbe { job, .. }
            | QueueCommand::ReleaseResourceHold { job, .. }
            | QueueCommand::SetJobStatus { job, .. }
            | QueueCommand::NoteSeedKeepBlock { job, .. }
            | QueueCommand::RearmSeedKeepCleared { job, .. }
            | QueueCommand::BeginManagedSeedRetirement { job, .. }
            | QueueCommand::CompleteManagedSeedRetirement { job, .. }
            | QueueCommand::ClaimNativeFailureCleanup { job, .. }
            | QueueCommand::FailNativeTorrent { job, .. }
            | QueueCommand::ConfirmManagedTorrentSource { job, .. }
            | QueueCommand::CommitManagedTorrentAdmission { job, .. }
            | QueueCommand::StartTorrentAcquisition { job, .. }
            | QueueCommand::CompleteTorrentAcquisition { job, .. }
            | QueueCommand::HoldTorrentAcquisition { job, .. } => Some(*job),
            _ => None,
        };
        let admission_busy = match &cmd {
            QueueCommand::ReserveManagedTorrentAdmission { opts, .. } => {
                crate::torrent_acquisitions::managed_token(&opts.params)
                    .ok()
                    .flatten()
                    .is_some_and(|token| {
                        self.state.url_admission_receipts.iter().any(|receipt| {
                            receipt.request_token == token
                                && self
                                    .pending_control_commits
                                    .contains(&JobId(receipt.job_id))
                        })
                    })
            }
            _ => false,
        };
        if admission_busy
            || pending_job.is_some_and(|job| self.pending_control_commits.contains(&job))
        {
            self.deferred_replacements.push_back(cmd);
            return;
        }
        let replacement = match &cmd {
            QueueCommand::MaterializeGeneration { request, .. } => Some(request.job.id),
            QueueCommand::ImportJob { job, .. } | QueueCommand::ImportJobIfPresent { job, .. } => {
                Some(job.id)
            }
            QueueCommand::RemoveJobSilent { job, .. } if self.state.job(*job).is_none() => None,
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
                        || (self.record_transitions.contains(&id)
                            && !self.settled_post_rollback(id))
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
        let Some(cmd) = self.on_native_lifecycle_command(cmd) else {
            return;
        };
        match cmd {
            command @ (QueueCommand::NoteSeedKeepBlock { .. }
            | QueueCommand::RearmSeedKeepCleared { .. }
            | QueueCommand::SyncManagedSeedKeep { .. }
            | QueueCommand::BeginManagedSeedRetirement { .. }
            | QueueCommand::CompleteManagedSeedRetirement { .. }
            | QueueCommand::ClaimNativeFailureCleanup { .. }
            | QueueCommand::FailNativeTorrent { .. }
            | QueueCommand::NativeTorrentPending { .. }
            | QueueCommand::ReserveManagedTorrentAdmission { .. }
            | QueueCommand::ConfirmManagedTorrentSource { .. }
            | QueueCommand::CommitManagedTorrentAdmission { .. }
            | QueueCommand::StartTorrentAcquisition { .. }
            | QueueCommand::CompleteTorrentAcquisition { .. }
            | QueueCommand::HoldTorrentAcquisition { .. }) => {
                let _ = self.on_native_lifecycle_command(command);
            }
            QueueCommand::ReserveUrlFiles {
                job,
                source_identity,
                count,
                reply,
            } => {
                let result = if self.persistence_fenced
                    || !self.persist
                    || self
                        .state
                        .job(job)
                        .filter(|row| row.status == JobStatus::Fetching)
                        .and_then(|row| nzbd_state::artifacts::url_source_identity(row).ok())
                        .as_deref()
                        != Some(&source_identity)
                {
                    Err(crate::EngineError::OwnershipLost)
                } else if let Some(maximum) = self.state.next_file_id.checked_add(count) {
                    let first = self.state.next_file_id.saturating_add(1);
                    self.state.next_file_id = maximum;
                    self.dirty = true;
                    Ok(first)
                } else {
                    Err(crate::EngineError::Lifecycle(
                        "NZB file IDs exhausted".into(),
                    ))
                };
                let _ = reply.send(result);
            }
            QueueCommand::ImportFence { job, reply } => {
                if self.persistence_fenced {
                    let _ = reply.send(Err(crate::EngineError::OwnershipLost));
                } else {
                    self.foreign_journal_holds.insert(job);
                    *self.import_preparations.entry(job).or_default() += 1;
                    let _ = reply.send(Ok(crate::imports::intent(self.state.job(job))));
                }
            }
            QueueCommand::EndImportPreparation { job, reply } => {
                self.finish_import_preparation(job, false);
                let _ = reply.send(());
            }
            QueueCommand::UnacceptedUrlAdmissions { limit, reply } => {
                let result = if !(1..=100).contains(&limit)
                    || self.unaccepted_admissions.len() > limit
                {
                    Err("unaccepted admission index exceeds supported page bound; reconcile before accepting more requests".into())
                } else {
                    let mut rows = self
                        .unaccepted_admissions
                        .values()
                        .cloned()
                        .collect::<Vec<_>>();
                    rows.sort_unstable_by(|left, right| {
                        left.request_token.cmp(&right.request_token)
                    });
                    if crate::admissions::valid_page(&rows) {
                        Ok(rows)
                    } else {
                        Err("unaccepted admission index exceeds supported 1MiB bound".into())
                    }
                };
                let _ = reply.send(result);
            }
            QueueCommand::UrlAdmissionsPage {
                after,
                limit,
                reply,
            } => {
                let _ = reply.send(crate::admissions::page(
                    &self.state.url_admission_receipts,
                    &after,
                    limit,
                ));
            }
            QueueCommand::BeginUrlAdmissionAdoption { reply } => {
                let ok = !self.persistence_fenced;
                if ok {
                    self.admission_adoption = Some(HashSet::new());
                }
                let _ = reply.send(ok);
            }
            QueueCommand::AdoptUrlAdmissions { rows, reply } => {
                let before = (
                    self.state.url_admission_receipts.clone(),
                    self.state.next_job_id,
                    self.authoritative_admissions.clone(),
                    self.unaccepted_admissions.clone(),
                    self.admission_adoption.clone(),
                    self.admission_catalog_adopted,
                );
                let ok = self.adopt_url_admissions(rows);
                if !ok {
                    let _ = reply.send(false);
                    return;
                }
                self.persist_then(move |owner, ok| {
                    if !ok {
                        owner.state.url_admission_receipts = before.0;
                        owner.state.next_job_id = owner.state.next_job_id.max(before.1);
                        owner.authoritative_admissions = before.2;
                        owner.unaccepted_admissions = before.3;
                        owner.admission_adoption = before.4;
                        owner.admission_catalog_adopted = before.5;
                    }
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::FinishUrlAdmissionAdoption { reply } => {
                let before = (
                    self.state.url_admission_receipts.clone(),
                    self.state.next_job_id,
                    self.authoritative_admissions.clone(),
                    self.unaccepted_admissions.clone(),
                    self.admission_adoption.clone(),
                    self.admission_catalog_adopted,
                );
                let ok = self.finish_url_admission_adoption();
                if !ok {
                    let _ = reply.send(false);
                    return;
                }
                self.persist_then(move |owner, ok| {
                    if !ok {
                        owner.state.url_admission_receipts = before.0;
                        owner.state.next_job_id = owner.state.next_job_id.max(before.1);
                        owner.authoritative_admissions = before.2;
                        owner.unaccepted_admissions = before.3;
                        owner.admission_adoption = before.4;
                        owner.admission_catalog_adopted = before.5;
                    }
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::CheckDiscardUrlAdmission { expected, reply } => {
                let _ = reply.send(self.can_discard_url_admission(&expected));
            }
            QueueCommand::DiscardUrlAdmission { expected, reply } => {
                if !self.can_discard_url_admission(&expected) {
                    let _ = reply.send(false);
                    return;
                }
                let before = self
                    .state
                    .url_admission_receipts
                    .iter()
                    .filter(|row| row.request_token == expected.request_token)
                    .cloned()
                    .collect::<Vec<_>>();
                let pending = self
                    .state
                    .pending_admissions
                    .iter()
                    .filter(|row| row.job_id.0 == expected.job_id)
                    .cloned()
                    .collect::<Vec<_>>();
                self.state
                    .url_admission_receipts
                    .retain(|row| row.request_token != expected.request_token);
                self.state
                    .pending_admissions
                    .retain(|row| row.job_id.0 != expected.job_id);
                self.persist_then(move |owner, ok| {
                    if !ok {
                        owner.state.url_admission_receipts.extend(before);
                        owner.state.pending_admissions.extend(pending);
                        let _ = reply.send(false);
                        return;
                    }
                    owner.unaccepted_admissions.remove(&expected.request_token);
                    if !pending.is_empty() {
                        let sources = owner.pending_sources.clone();
                        owner.io_then(
                            move || sources.remove(JobId(expected.job_id)),
                            move |_, result| {
                                let _ = reply.send(result.is_ok());
                            },
                        );
                    } else {
                        let _ = reply.send(true);
                    }
                });
            }
            QueueCommand::AdoptReplicatedHighwater {
                next_job_id,
                next_file_id,
                reply,
            } => {
                if self.persistence_fenced {
                    let _ = reply.send(false);
                    return;
                }
                self.state.next_job_id = self.state.next_job_id.max(next_job_id);
                self.state.next_file_id = self.state.next_file_id.max(next_file_id);
                self.persist_then(move |_, ok| {
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::SetConsumerInstanceId { instance, reply } => {
                let ok = !self.persistence_fenced
                    && !instance.is_empty()
                    && instance.len() <= 128
                    && !instance.chars().any(char::is_control);
                if ok {
                    self.consumer_instance_id = instance;
                }
                let _ = reply.send(ok);
            }
            QueueCommand::AckAutomaticControl {
                job,
                identity,
                reply,
            } => {
                let before = self.state.job(job).cloned();
                let marker = before
                    .as_ref()
                    .and_then(crate::automatic_control::transition);
                let source = self.artifacts.for_job(job.0);
                let exact_source = marker.as_ref().is_some_and(|marker| {
                    source.as_ref().is_ok_and(|source| {
                        marker.storage_instance_id == self.artifacts.installation
                            && source.as_ref().map(|source| {
                                nzbd_state::artifacts::SourceGeneration {
                                    artifact: source.id.clone(),
                                    generation: source.generation.clone(),
                                }
                            }) == marker.source
                            && source.as_ref().map(|source| source.path.clone())
                                == marker.source_path
                    })
                });
                let eligible = self.automatic_control_scope(job)
                    && !self.persistence_fenced
                    && exact_source
                    && marker
                        .as_ref()
                        .is_some_and(|marker| marker.identity == identity);
                if eligible {
                    self.state.job_mut(job).unwrap().params.retain(|(key, _)| {
                        key != crate::automatic_control::AUTOMATIC_CONTROL_PARAM
                    });
                }

                if !eligible {
                    let _ = reply.send(false);
                    return;
                }
                self.pending_control_commits.insert(job);
                self.persist_then(move |owner, ok| {
                    owner.pending_control_commits.remove(&job);
                    if !ok {
                        if let Some(before) = before {
                            if let Some(current) = owner.state.job_mut(job) {
                                *current = before;
                            }
                        }
                    } else {
                        owner.publish_now();
                        owner.bump_epoch();
                    }
                    let _ = reply.send(ok);
                });
            }
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
                            && !transitions::failed_cleanup_only(j)
                            && matches!(
                                j.status,
                                JobStatus::Completed
                                    | JobStatus::Failed
                                    | JobStatus::PostQueued
                                    | JobStatus::Post { .. }
                            )
                    })
                    && !self.post_admissions.contains_key(&job)
                    && !self.record_transitions.contains(&job)
                    && !self.projection_transition
                    && !self.pending_control_commits.contains(&job)
                    && !self.deferred_terminals.contains_key(&job)
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
            QueueCommand::BeginFailedCleanupAdmission { job, reply } => {
                self.begin_failed_cleanup_admission_then(job, move |owner, admission| {
                    if let Err(Some(scope)) = reply.send(admission) {
                        owner.failed_cleanup_admissions.remove(&scope.admission.job);
                        owner.post_admissions.remove(&scope.admission.job);
                        if let Some(cancel) = owner.post_cancellations.remove(&scope.admission.job)
                        {
                            cancel.cancel();
                        }
                        owner.drain_replacements();
                    }
                });
            }
            QueueCommand::FailedCleanupMetadata { admission, reply } => {
                let record = (self.post_scope_current(&admission)
                    && self.failed_cleanup_admissions.contains_key(&admission.job))
                .then(|| self.state.job(admission.job).cloned())
                .flatten();
                let _ = reply.send(record);
            }
            QueueCommand::EndPostAdmission { admission, reply } => {
                if self.post_admissions.get(&admission.job) == Some(&admission.generation) {
                    self.failed_cleanup_admissions.remove(&admission.job);
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
                            let _ = reply.send(Ok(None));
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
                            let _ = reply.send(Ok(None));
                            return;
                        }
                        owner.bump_epoch();
                        let sources = owner.pending_sources.clone();
                        owner.io_then(move || sources.remove(job), move |owner, removed| {
                            if let Err(error) = removed {
                                tracing::warn!(job=job.0,%error,"could not remove committed torrent source");
                            }
                            owner.publish_now();
                            let _ = reply.send(Ok(Some(result)));
                        });
                    });
                } else {
                    let _ = reply.send(Ok(None));
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
                                &self.consumer_instance_id,
                            ),
                            runner_instance_id: self.consumer_instance_id.clone(),
                            terminal_outcome: None,
                            outcome: existing_job.status.as_str().into(),
                            failure_stage: None,
                            failure_cause: None,
                            custody_state: "runner_owned".into(),
                            keep_source_on_failure: false,
                            source_cleanup_status: None,
                            publication_path: None,
                            source_artifact_id: None,
                            source_generation: None,
                            failure_reason: None,
                            fetch_cause: None,
                            blames_release: None,
                            salvage: None,
                        };
                        self.state.url_admission_receipts.push(receipt.clone());
                        if self.persist_guard.is_some() {
                            self.unaccepted_admissions.insert(token.clone(), receipt);
                        }
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
                                    owner.unaccepted_admissions.remove(&token);
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
                    if let Some(token) = request_token.as_deref() {
                        // Public consumer identity can change across authority
                        // adoption; the fetch origin remains bound to this
                        // storage installation for its complete lifetime.
                        j.params.push((
                            "*URL:AdmissionGeneration".into(),
                            url_admission_generation(id, token, &self.artifacts.installation),
                        ));
                    }
                }
                let name = self.name_from_requestor(id, true).unwrap_or(name);
                if let (Some(token), Some(fingerprint)) =
                    (request_token.as_ref(), submission_sha256.as_ref())
                {
                    let receipt = UrlAdmissionReceipt {
                        protocol_version: 1,
                        request_token: token.clone(),
                        submission_sha256: fingerprint.clone(),
                        job_id: id.0,
                        generation: url_admission_generation(id, token, &self.consumer_instance_id),
                        runner_instance_id: self.consumer_instance_id.clone(),
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
                        failure_reason: None,
                        fetch_cause: None,
                        blames_release: None,
                        salvage: None,
                    };
                    self.state.url_admission_receipts.push(receipt.clone());
                    if self.persist_guard.is_some() {
                        self.unaccepted_admissions.insert(token.clone(), receipt);
                    }
                }
                let generation = fresh_epoch();
                self.job_epochs.insert(id, generation);
                // Snapshot publication can race the admission save. Reuse the
                // export barrier until the durable acceptance callback runs.
                self.pending_control_revisions.insert(id, generation);
                let persistent = self.persist;
                self.persist_then(move |owner, committed| {
                    let current = owner.job_epochs.get(&id) == Some(&generation);
                    if owner.pending_control_revisions.get(&id) == Some(&generation) {
                        owner.pending_control_revisions.remove(&id);
                    }
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
                            owner
                                .unaccepted_admissions
                                .retain(|_, receipt| receipt.job_id != id.0);
                        }
                        owner.publish_now();
                        owner.bump_epoch();
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
            QueueCommand::CompleteUrlFetch {
                job,
                source_identity,
                prepared,
                reply,
            } => {
                self.apply_url_resolution_then(
                    job,
                    source_identity,
                    *prepared,
                    move |_, result| {
                        let _ = reply.send(result);
                    },
                );
            }

            QueueCommand::UpdateUrlFetchProgress {
                job,
                progress,
                reply,
            } => {
                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                let before = self.state.job(job).cloned();
                let timing_before = self.timing_cache.checkpoint(job);
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
                    let _ = reply.send(Ok(false));
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
                        // The URL fetcher owns its storage retry schedule.
                        // A failed progress write must not replace Fetching
                        // with a hold that prevents its next durable retry.
                        if let Some(before) = before {
                            if let Some(record) = owner.state.job_mut(job) {
                                *record = before;
                            }
                        }
                        owner.timing_cache.restore(job, timing_before);
                        owner.dirty = true;
                    }
                    let _ = reply.send(if durable {
                        Ok(true)
                    } else {
                        Err(crate::EngineError::AdmissionNotDurable)
                    });
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
                let prior_url_paused = self
                    .state
                    .job(job)
                    .is_some_and(|record| record.params.iter().any(|(key, _)| key == "*AddPaused"));
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
                        } else if record.status == JobStatus::Fetching {
                            set_owner_param(&mut record.params, "*AddPaused", Some("yes".into()));
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
                                    if record.status == JobStatus::Fetching {
                                        set_owner_param(
                                            &mut record.params,
                                            "*AddPaused",
                                            prior_url_paused.then(|| "yes".into()),
                                        );
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
                if self
                    .state
                    .job(job)
                    .is_some_and(|record| record.status == JobStatus::Fetching && !record.held())
                {
                    let changed = self.state.job_mut(job).is_some_and(|record| {
                        let old = record.params.len();
                        record.params.retain(|(key, _)| key != "*AddPaused");
                        record.params.len() != old
                    });
                    if !changed {
                        let _ = reply.send(false);
                        return;
                    }
                    let generation = fresh_epoch();
                    self.control_epochs.insert(job, generation);
                    self.pending_control_revisions.insert(job, generation);
                    self.dirty = true;
                    let persistent = self.persist;
                    self.persist_then(move |owner, committed| {
                        let current = owner.control_epochs.get(&job) == Some(&generation)
                            && owner.job_epochs.get(&job) == Some(&scope_generation);
                        let ok = (!persistent || committed) && current;
                        if owner.pending_control_revisions.get(&job) == Some(&generation) {
                            owner.pending_control_revisions.remove(&job);
                        }
                        if current {
                            if !ok {
                                if let Some(record) = owner
                                    .state
                                    .job_mut(job)
                                    .filter(|record| record.status == JobStatus::Fetching)
                                {
                                    set_owner_param(
                                        &mut record.params,
                                        "*AddPaused",
                                        Some("yes".into()),
                                    );
                                }
                            }
                            owner.publish_now();
                            owner.bump_epoch();
                        }
                        let _ = reply.send(ok);
                    });
                    return;
                }
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
                            if !owner.hold_job(
                                job,
                                nzbd_types::HoldCause::StorageUnavailable,
                                "snapshot",
                                "Resume intent could not be persisted",
                            ) && owner.persistence_fenced {
                                // The failed save has revoked persistence authority.
                                // Expose the stopped state without claiming another
                                // durable transition or clearing that fence.
                                if let Some(record) = owner.state.job_mut(job) {
                                    let prior = record.control();
                                    let revision = prior.as_ref().and_then(|control| control.revision.parse::<u64>().ok()).unwrap_or(0).saturating_add(1);
                                    record.set_control(&nzbd_types::JobControl {
                                        version: 1,
                                        revision: revision.to_string(),
                                        lifecycle: "held".into(),
                                        cause: "storage_unavailable".into(),
                                        stage: "snapshot".into(),
                                        retry_policy: "review".into(),
                                        message: "Resume intent could not be persisted; queue ownership is fenced until recovery".into(),
                                        instance: prior.as_ref().map(|control| control.instance.clone()).unwrap_or_else(|| format!("snapshot-{}", job.0)),
                                        previous_status: Some(JobStatus::Paused),
                                        manual_pause: true,
                                        retry_attempt: prior.as_ref().map_or(0, |control| control.retry_attempt),
                                        next_retry_at_unix: None,
                                        blocked_since_unix: Some(unix_now()),
                                        released_at_unix: None,
                                        deadline_in_s: None,
                                        deadline_unix: None,
                                    });
                                    record.status = JobStatus::Held;
                                }
                                owner.publish_now();
                            }
                        }
                    }
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::PrepareClusterWritersStop {
                ticket,
                execution_joined,
                reply,
            } => {
                let job = JobId(ticket.request.job);
                let result = (|| -> Result<crate::cluster_stop::StopPreparation, String> {
                    let current = self.state.job(job).cloned().filter(|record| {
                        nzbd_state::artifacts::WorkerControlContext::for_job(record).is_some_and(
                            |context| {
                                nzbd_state::artifacts::same_worker_operation(
                                    &context,
                                    &ticket.request.context,
                                )
                            },
                        )
                    });
                    if let Some(record) = current {
                        if record.torrent.is_some() {
                            return Err(
                                "torrent backend must provide its own actual stop acknowledgment"
                                    .into(),
                            );
                        }
                        if ticket.never_started {
                            return Err("live queue state contradicts absent start receipt; retain for review".into());
                        }
                        let source = self.artifacts.for_job(job.0).map_err(|e| e.to_string())?;
                        self.cluster_writer_stops
                            .observe(&record, source.as_ref(), None);
                        for file in &record.files {
                            if let Some(writer) = self.writers.remove(&file.id) {
                                writer.stop.cancel();
                                self.cluster_writer_stops.observe(
                                    &record,
                                    source.as_ref(),
                                    Some(&writer),
                                );
                            }
                            self.finalize_sent.remove(&file.id);
                        }
                        self.allocated_jobs.remove(&job);
                        self.attempts.retain(|reference, _| reference.job != job);
                        self.pending_finalize.retain(|(id, _)| *id != job);
                        if let Some(record) = self.state.job_mut(job) {
                            // A held job stays held: the writers stop, the hold
                            // is not this stop's to clear.
                            if !matches!(
                                record.status,
                                JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
                            ) {
                                record.stop();
                            }
                        }
                        self.bump_epoch();
                        self.publish_now();
                    } else if ticket.never_started
                        && self.cluster_writer_stops.known(&ticket.request.context)
                    {
                        return Err(
                            "tracked writer contradicts absent start receipt; retain for review"
                                .into(),
                        );
                    }
                    self.cluster_writer_stops.prepare(&ticket, execution_joined)
                })();
                let _ = reply.send(result);
            }
            QueueCommand::ForgetClusterWriterStops { context, reply } => {
                let _ = reply.send(self.cluster_writer_stops.forget_joined(&context));
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
                        // Fenced for deletion; a hold stays a hold.
                        record.stop();
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
                                // The empty fence has no writer acknowledgement
                                // task to clear it. Its durable quiesce commit
                                // is the complete retirement boundary.
                                if writers.is_empty()
                                    && !owner.retirement_epochs.contains_key(&job)
                                    && owner.retiring_writers.get(&job).is_some_and(Vec::is_empty)
                                {
                                    owner.retiring_writers.remove(&job);
                                    owner.drain_replacements();
                                }
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
                if self.state.job(job).is_some_and(|row| row.torrent.is_some())
                    && self
                        .state
                        .jobs
                        .iter()
                        .any(|row| crate::torrent_acquisitions::source_dependency(row) == Some(job))
                {
                    let _ = reply.send(false);
                    return;
                }

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
                if before.is_none() {
                    // Queue retirement does not retire separately owned source
                    // custody. Its inventory journal remains the exact authority
                    // for this preference; no queue row needs reconstruction.
                    if self.persistence_fenced {
                        let _ = reply.send(false);
                        return;
                    }
                    let artifacts = self.artifacts.clone();
                    self.io_then(
                        move || artifacts.set_keep_on_failure(job.0, keep)
                            .map_err(|error| nzbd_state::StateError::Corrupt(format!("artifact inventory: {error}"))),
                        move |_, result| {
                            if let Err(error) = &result {
                                tracing::warn!(job = job.0, %error, "could not update retired source Keep intent");
                            }
                            let _ = reply.send(result.unwrap_or(false));
                        },
                    );
                    return;
                }
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
                        JobStatus::Fetching
                            | JobStatus::Queued
                            | JobStatus::Paused
                            | JobStatus::Held
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
                                    refresh_url_admission_custodies(&mut refreshed_receipts, &artifacts);
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
                self.speed_limit_revision = fresh_epoch();
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
                preparation,
                authority,
                emit_finished,
                reply,
            } => {
                let id = job.id;
                let prepared = preparation.is_some();
                if !authority.valid()
                    || preparation
                        .as_ref()
                        .is_some_and(|proof| !proof.valid(id, self.state.job(id), &self.artifacts))
                {
                    if prepared {
                        self.finish_import_preparation(id, false);
                    }
                    let _ = reply.send(Err(crate::EngineError::StaleImport));
                    return;
                }
                let mut job = *job;
                if let Some(proof) = preparation.as_ref() {
                    proof.merge_current_progress(&mut job, self.state.job(id));
                }
                self.replace_job_then(
                    job,
                    false,
                    emit_finished,
                    false,
                    false,
                    move |owner, result| {
                        if result.is_ok() {
                            if let Some(proof) = preparation.as_ref() {
                                owner.file_sizes.extend(
                                    proof.file_sizes.iter().map(|(key, value)| (*key, *value)),
                                );
                            }
                        }
                        if prepared {
                            owner.finish_import_preparation(id, result.is_ok());
                        }
                        drop(preparation);
                        let _ = reply.send(result.map_err(crate::EngineError::State));
                    },
                );
            }
            QueueCommand::ImportJobIfPresent {
                job,
                require_snapshot,
                completion_proof,
                reply,
            } => {
                self.replace_job_proven_then(
                    *job,
                    false,
                    false,
                    true,
                    require_snapshot,
                    completion_proof,
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
                    && !self
                        .state
                        .job(job)
                        .is_some_and(transitions::failed_cleanup_only)
                    && !self.retiring_writers.contains_key(&job)
                    && !self.pending_control_revisions.contains_key(&job)
                    && !self.post_rollbacks.contains_key(&job)
                    && !self.failed_cleanup_admissions.contains_key(&job)
                    && !self.projection_transition
                    && !replacement_pending;
                let result = if exportable {
                    self.state.job(job).cloned().map(Box::new)
                } else {
                    None
                };
                let _ = reply.send(result);
            }
            QueueCommand::RemoveJobSilent {
                job,
                retirement,
                reply,
            } => {
                let current = retirement.as_ref().is_some_and(|expected| {
                    self.state.job(job).is_some_and(|record| {
                        nzbd_state::artifacts::WorkerControlContext::for_job(record).as_ref()
                            == Some(expected.context())
                    })
                });
                if retirement.is_some() && !current {
                    // A delayed completion must not retire another grant's journal.
                    let _ = reply.send(false);
                } else {
                    let compact = match retirement {
                        Some(WorkerRetirement::Accepted(_)) => true,
                        Some(WorkerRetirement::Revoked(_)) => false,
                        None => self.persist,
                    };
                    let _ = reply.send(self.remove_job_silent_with_journal(job, compact));
                }
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
                self.replace_projection_then(jobs, None, move |_, result| {
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
                guard,
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
                    let before = self.state.job(job).cloned();
                    if let Err(error) =
                        self.retry_hold_proven(job, &revision, by, custody_checked, &guard)
                    {
                        let _ = reply.send(Err(error));
                        return;
                    }
                    self.pending_control_commits.insert(job);
                    self.commit_control_change_then(job, before.clone(), move |owner, ok| {
                        owner.pending_control_commits.remove(&job);
                        if !ok {
                            if let Some(before) = before {
                                if let Some(row) = owner.state.job_mut(job) {
                                    *row = before;
                                }
                            }
                        } else {
                            owner.publish_now();
                            owner.bump_epoch();
                        }
                        let _ = reply.send(if ok {
                            Ok(())
                        } else {
                            Err("retry intent is not durable".into())
                        });
                    });
                }
            }
            QueueCommand::RescheduleHold {
                job,
                revision,
                reason,
                guard,
                reply,
            } => {
                let before = self.state.job(job).cloned();
                if !self.reschedule_hold_proven(job, &revision, &reason, &guard) {
                    let _ = reply.send(false);
                    return;
                }
                self.pending_control_commits.insert(job);
                self.commit_control_change_then(job, before.clone(), move |owner, ok| {
                    owner.pending_control_commits.remove(&job);
                    if !ok {
                        if let Some(before) = before {
                            if let Some(row) = owner.state.job_mut(job) {
                                *row = before;
                            }
                        }
                    } else {
                        owner.publish_now();
                        owner.bump_epoch();
                    }
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::BeginResourceProbe {
                job,
                revision,
                by,
                guard,
                reply,
            } => {
                if self.persistence_fenced
                    || (by == RetryBy::Recovery && self.automatic_control_pending(job))
                    || (by == RetryBy::Recovery
                        && !guard.worker_checked()
                        && self.delegated.contains_key(&job))
                    || !self.state.job(job).is_some_and(|current| {
                        guard.matches(&self.artifacts, current, self.persist)
                    })
                {
                    let _ = reply.send(false);
                    return;
                }

                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                let before = self.state.job(job).cloned();
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
                self.pending_control_commits.insert(job);
                self.commit_control_change_then(job, before, move |owner, committed| {
                    owner.pending_control_commits.remove(&job);
                    let current = owner.control_epochs.get(&job) == Some(&generation)
                        && owner.job_epochs.get(&job) == Some(&scope_generation);
                    let ok = committed && current;
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
                guard,
                reply,
            } => {
                if self.persistence_fenced
                    || (by == RetryBy::Recovery && self.automatic_control_pending(job))
                    || (by == RetryBy::Recovery
                        && !guard.worker_checked()
                        && self.delegated.contains_key(&job))
                    || !self.state.job(job).is_some_and(|current| {
                        guard.matches(&self.artifacts, current, self.persist)
                    })
                {
                    let _ = reply.send(false);
                    return;
                }

                let scope_generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
                let before = self.state.job(job).cloned();
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
                if by == RetryBy::Recovery
                    && !self.prepare_automatic_control_change(job, before.as_ref())
                {
                    let _ = reply.send(false);
                    return;
                }
                self.pending_control_commits.insert(job);
                self.commit_control_change_then(job, before, move |owner, committed| {
                    owner.pending_control_commits.remove(&job);
                    let current = owner.control_epochs.get(&job) == Some(&generation)
                        && owner.job_epochs.get(&job) == Some(&scope_generation);
                    let ok = committed && current;
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
                            if owner
                                .writers
                                .get(&file)
                                .is_some_and(|writer| *writer.stopped.borrow())
                            {
                                owner.writers.remove(&file);
                                owner.writer_epochs.remove(&file);
                            }
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
            QueueCommand::ExpireHold {
                job,
                revision,
                reply,
            } => {
                let started = self.expire_hold(job, &revision);
                let _ = reply.send(started);
            }
            QueueCommand::FailJob {
                job,
                reason,
                fetch_cause,
                stage,
                message,
                reply,
            } => {
                self.fail_job_terminal_then(
                    job,
                    reason,
                    fetch_cause,
                    &stage,
                    &message,
                    Box::new(move |_, row| {
                        let _ = reply.send(row);
                    }),
                );
            }
            QueueCommand::RecordTerminal {
                terminal,
                history,
                reply,
            } => {
                let job = JobId(terminal.job);
                if self.terminals_in_flight.contains(&job) {
                    self.deferred_replacements
                        .push_back(QueueCommand::RecordTerminal {
                            terminal,
                            history,
                            reply,
                        });
                    return;
                }
                self.record_terminal_with_then(
                    *terminal,
                    history.map(|history| history.0),
                    Box::new(move |_, row| {
                        let _ = reply.send(row);
                    }),
                );
            }
            QueueCommand::ReplayTerminals { reply } => {
                self.replay_terminals();
                let _ = reply.send(());
            }
            QueueCommand::HoldJob {
                job,
                cause,
                stage,
                message,
                not_before,
                reply,
            } => {
                let before = self.state.job(job).cloned();
                if !self.prepare_hold_job_until(job, cause, &stage, &message, not_before) {
                    let _ = reply.send(false);
                    return;
                }
                self.pending_control_commits.insert(job);
                self.commit_control_change_then(job, before, move |owner, ok| {
                    owner.pending_control_commits.remove(&job);
                    if ok {
                        owner.publish_now();
                        owner.bump_epoch();
                    }
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::SetJobStatus { job, status, reply } => {
                if self.automatic_control_pending(job) {
                    let _ = reply.send(false);
                    return;
                }
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
                    self.on_command(QueueCommand::RemoveJobSilent {
                        job: id,
                        retirement: None,
                        reply,
                    });
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

    fn apply_url_resolution_then(
        &mut self,
        job: JobId,
        identity: String,
        prepared: nzbd_state::artifacts::UrlResolutionRecord,
        callback: impl FnOnce(&mut Owner, Result<bool, crate::EngineError>) + Send + 'static,
    ) {
        if self.persistence_fenced || !self.persist {
            callback(self, Err(crate::EngineError::OwnershipLost));
            return;
        }
        let Some(before) = self
            .state
            .job(job)
            .filter(|row| row.status == JobStatus::Fetching)
            .cloned()
        else {
            callback(self, Ok(false));
            return;
        };
        let source_identity = match nzbd_state::artifacts::url_source_identity(&before) {
            Ok(identity) => identity,
            Err(error) => {
                callback(self, Err(crate::EngineError::Lifecycle(error.to_string())));
                return;
            }
        };
        if identity != source_identity
            || prepared.resolution_id != identity
            || prepared.source_identity != identity
            || prepared.origin.id != job
            || prepared.resolved.id != job
        {
            callback(self, Ok(false));
            return;
        }
        let timing_checkpoint = self.timing_cache.checkpoint(job);
        let receipts = self.state.url_admission_receipts.clone();
        let prior_allocated = self.allocated_jobs.clone();
        let failed = prepared.resolved.status == JobStatus::Failed;
        let row = self.state.job_mut(job).unwrap();
        // Bulk parsing, hashes and outbox fsync completed off the actor. Only
        // immutable file facts are applied here; current user intent wins.
        if row.name == prepared.origin.name
            && row.name_provisional == prepared.origin.name_provisional
        {
            row.name = prepared.resolved.name;
            row.name_provisional = prepared.resolved.name_provisional;
        }
        if row.category.is_none() && prepared.origin.category.is_none() {
            row.category = prepared.resolved.category;
        }
        if row.dir_name == prepared.origin.dir_name {
            row.dir_name = prepared.resolved.dir_name;
        }
        row.files = prepared.resolved.files;
        row.totals = prepared.resolved.totals;
        row.status = if failed {
            JobStatus::Failed
        } else if row.params.iter().any(|(key, _)| key == "*AddPaused") {
            JobStatus::Paused
        } else {
            JobStatus::Queued
        };
        row.params.retain(|(key, _)| key != "*AddPaused");
        for (key, value) in prepared.resolved.params.into_iter().filter(|(key, _)| {
            matches!(
                key.as_str(),
                "Failure:Stage" | "Failure:Reason" | crate::FETCH_CAUSE_PARAM
            )
        }) {
            set_owner_param(&mut row.params, &key, Some(value));
        }
        let maximum_file_id = row.files.iter().map(|file| file.id.0).max().unwrap_or(0);
        self.state.next_file_id = self.state.next_file_id.max(maximum_file_id);
        let generation = fresh_epoch();
        self.job_epochs.insert(job, generation);
        let finish = move |owner: &mut Owner| {
            owner.persist_then(move |owner, ok| {
                let current = owner.job_epochs.get(&job) == Some(&generation);
                if !ok || !current {
                    if current {
                        owner.timing_cache.restore(job, timing_checkpoint);
                        if let Some(latest) = owner.state.job(job) {
                            let mut restored = before;
                            restored.category = latest.category.clone();
                            restored.priority = latest.priority;
                            if let Some(control) = latest.control() {
                                restored.set_control(&control);
                            }
                            if let Some(row) = owner.state.job_mut(job) {
                                *row = restored;
                            }
                        }
                        owner
                            .state
                            .url_admission_receipts
                            .retain(|receipt| receipt.job_id != job.0);
                        owner.state.url_admission_receipts.extend(
                            receipts
                                .into_iter()
                                .filter(|receipt| receipt.job_id == job.0),
                        );
                        owner.allocated_jobs = prior_allocated;
                    }
                    callback(
                        owner,
                        Err(if owner.persistence_fenced {
                            crate::EngineError::OwnershipLost
                        } else {
                            crate::EngineError::AdmissionNotDurable
                        }),
                    );
                    return;
                }
                owner.publish_now();
                owner.emit(Event::UrlFetchResolved { job });
                if failed {
                    // Contract §2.1: the fetcher's terminal, typed at its
                    // own site. Without a fetch cause the source failed
                    // locally (a storage refusal), which is `io`.
                    let row = owner.state.job(job);
                    let param = |key: &str| {
                        row.and_then(|row| {
                            row.params
                                .iter()
                                .find(|(name, _)| name == key)
                                .map(|(_, value)| value.clone())
                        })
                    };
                    let cause = param(crate::FETCH_CAUSE_PARAM)
                        .as_deref()
                        .and_then(nzbd_types::FetchCause::parse);
                    let message =
                        param("Failure:Reason").unwrap_or_else(|| "URL fetch failed".into());
                    let reason = if cause.is_some() {
                        nzbd_types::FailureReason::Fetch
                    } else {
                        nzbd_types::FailureReason::Io
                    };
                    let name = owner
                        .state
                        .job(job)
                        .map(|row| row.name.clone())
                        .unwrap_or_default();
                    // The advisory `job_finished` follows the terminal.
                    owner.fail_job_terminal_then(
                        job,
                        reason,
                        cause,
                        "fetch",
                        &message,
                        Box::new(move |owner, _| {
                            owner.emit(Event::JobFinished {
                                job,
                                name,
                                status: JobStatus::Failed,
                                health: 0,
                            });
                        }),
                    );
                }
                owner.bump_epoch();
                callback(owner, Ok(true));
            });
        };
        if failed {
            finish(self);
        } else {
            self.allocate_new_payload_then(job, move |owner, result| {
                if let Err(error) = result {
                    owner.hold_job(
                        job,
                        allocation_hold_cause(&error),
                        "download_write",
                        &format!("payload allocation: {error}"),
                    );
                }
                finish(owner);
            });
        }
    }

    /// A worker owns no shared queue snapshot. Its retry intent commits to
    /// the local inventory under the exact lease/source tuple instead.
    fn automatic_control_scope(&self, job: JobId) -> bool {
        self.persist
            && self.persist_guard.is_some()
            && !self.delegated.contains_key(&job)
            && self.state.job(job).is_some_and(|job| {
                nzbd_state::artifacts::WorkerControlContext::for_job(job).is_none()
            })
    }
    fn automatic_control_pending(&self, job: JobId) -> bool {
        self.automatic_control_scope(job)
            && self
                .state
                .job(job)
                .is_some_and(crate::automatic_control::pending)
    }
    fn prepare_automatic_control_change(&mut self, job: JobId, before: Option<&Job>) -> bool {
        if self.automatic_control_scope(job) {
            let Some(before) = before else {
                return false;
            };
            let Ok(source) = self.artifacts.for_job(job.0) else {
                return false;
            };
            let installation = self.artifacts.installation.clone();
            if let Some(current) = self.state.job_mut(job) {
                if let Err(error) =
                    crate::automatic_control::stamp(before, current, &installation, source.as_ref())
                {
                    tracing::warn!(job=job.0,%error,"automatic control awaits durable authority");
                    return false;
                }
            }
        }
        !self.persistence_fenced
    }

    pub(super) fn commit_control_change_then(
        &mut self,
        job: JobId,
        before: Option<Job>,
        callback: impl FnOnce(&mut Owner, bool) + Send + 'static,
    ) {
        if self.persistence_fenced {
            callback(self, false);
            return;
        }
        if self.persist {
            self.persist_then(callback);
            return;
        }
        let Some(record) = self.state.job(job).cloned() else {
            callback(self, false);
            return;
        };
        let expected = before
            .as_ref()
            .and_then(Job::control)
            .map(|control| control.revision)
            .unwrap_or_default();
        let artifacts = self.artifacts.clone();
        let current_control = record.control();
        let context = nzbd_state::artifacts::WorkerControlContext::for_job(&record);
        self.io_then(
            move || {
                artifacts
                    .checkpoint_worker_control(&record, &expected)
                    .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))
            },
            move |owner, result| {
                let current = owner.state.job(job).is_some_and(|row| {
                    row.control() == current_control
                        && nzbd_state::artifacts::WorkerControlContext::for_job(row) == context
                });
                callback(owner, result.is_ok() && current);
            },
        );
    }

    fn adopt_url_admissions(&mut self, rows: Vec<UrlAdmissionReceipt>) -> bool {
        // A rejected authority must not mutate even the provisional catalog
        // or its highwater before the asynchronous persistence lane refuses it.
        if self.persist && self.persist_guard.as_ref().is_some_and(|guard| !guard()) {
            self.persistence_fenced = true;
            self.persist = false;
            return false;
        }
        if self.persistence_fenced || !crate::admissions::valid_page(&rows) {
            return false;
        }
        for row in &rows {
            if self
                .authoritative_admissions
                .get(&row.request_token)
                .is_some_and(|accepted| accepted != &crate::admissions::identity(row))
            {
                return false;
            }
            if self
                .admission_adoption
                .as_ref()
                .is_some_and(|seen| seen.contains(&row.request_token))
                && self.state.url_admission_receipts.iter().any(|current| {
                    current.request_token == row.request_token
                        && crate::admissions::identity(current) != crate::admissions::identity(row)
                })
            {
                return false;
            }
        }
        for row in rows {
            self.state.next_job_id = self.state.next_job_id.max(row.job_id);
            if let Some(current) = self
                .state
                .url_admission_receipts
                .iter_mut()
                .find(|current| current.request_token == row.request_token)
            {
                *current = crate::admissions::merge_current(current, &row);
            } else {
                self.state.url_admission_receipts.push(row.clone());
            }
            self.authoritative_admissions
                .insert(row.request_token.clone(), crate::admissions::identity(&row));
            self.unaccepted_admissions.remove(&row.request_token);
            if let Some(seen) = self.admission_adoption.as_mut() {
                seen.insert(row.request_token);
            }
        }
        true
    }
    fn finish_url_admission_adoption(&mut self) -> bool {
        if self.persistence_fenced {
            return false;
        }
        let Some(seen) = self.admission_adoption.clone() else {
            return false;
        };
        self.state
            .url_admission_receipts
            .retain(|row| seen.contains(&row.request_token));
        self.authoritative_admissions
            .retain(|token, _| seen.contains(token));
        self.admission_adoption = None;
        self.admission_catalog_adopted = true;
        self.unaccepted_admissions.clear();
        true
    }
    fn can_discard_url_admission(&self, expected: &UrlAdmissionReceipt) -> bool {
        !self.persistence_fenced
            && self.admission_adoption.is_none()
            && !self
                .authoritative_admissions
                .contains_key(&expected.request_token)
            && self.state.job(JobId(expected.job_id)).is_none_or(|job| {
                !job.params.iter().any(|(key, value)| {
                    key == "monarr-transfer" && value == &expected.request_token
                })
            })
            && self
                .state
                .url_admission_receipts
                .iter()
                .find(|row| row.request_token == expected.request_token)
                .is_none_or(|current| {
                    crate::admissions::identity(current) == crate::admissions::identity(expected)
                        && current.custody_state != "curator"
                        && current.terminal_outcome.is_none()
                })
    }

    fn finish_import_preparation(&mut self, job: JobId, applied: bool) {
        if let Some(count) = self.import_preparations.get_mut(&job) {
            if applied && *count == 1 {
                self.foreign_journal_holds.remove(&job);
            }
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.import_preparations.remove(&job);
            }
        }
    }

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
        self.remove_job_silent_with_journal(job, self.persist)
    }

    fn remove_job_silent_with_journal(&mut self, job: JobId, compact_journal: bool) -> bool {
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
        self.artifacts.invalidate_writer_naming(job.0);
        if self.post_rollbacks.remove(&job).is_some() {
            self.record_transitions.remove(&job);
            self.pending_control_commits.remove(&job);
        }
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
                if receipt.job_id == job.0 && receipt.custody_state != "curator" {
                    receipt.outcome = "deleted".into();
                }
            }
        }
        // Persist the retirement before clearing old journals or allowing a
        // same-ID import to open another descriptor at this namespace.
        let retirement = fresh_epoch();
        self.retirement_epochs.insert(job, retirement);
        self.retiring_writers.entry(job).or_default();
        let source = self.artifacts.for_job(job.0).ok().flatten();
        self.cluster_writer_stops
            .observe(&record, source.as_ref(), None);
        self.dirty = true;
        self.persist_then(move |owner, committed| {
            if owner.retirement_epochs.get(&job) != Some(&retirement) {
                return;
            }
            if committed {
                // A worker's local snapshot does not cover the authority's
                // pending download. Preserve its shared verified ranges on
                // stop/expiry; only acknowledged completion or authority
                // retirement permits journal compaction.
                if compact_journal {
                    owner.remove_journal(job);
                }
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
                        if let Some(size) = merge_replayed_size(&mut owner.state, &record) {
                            owner.file_sizes.insert(record.file, size);
                        }
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
        let speed_revision = self.speed_limit_revision;
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
                let mut adopted_speed = None;
                if let Some(state) = recovered {
                    // The first cluster authority may boot without loading the
                    // shared queue. Preserve retired admissions for the one-time
                    // catalog migration before replacing the projection. Once
                    // adoption/pruning completed, a stale snapshot cannot revive
                    // an orphan that the authoritative catalog removed.
                    if !owner.admission_catalog_adopted {
                        let mut receipts = owner.state.url_admission_receipts.clone();
                        let mut positions = receipts.iter().enumerate()
                            .map(|(index, receipt)| (receipt.request_token.clone(), index))
                            .collect::<HashMap<_, _>>();
                        for incoming in state.url_admission_receipts {
                            if owner.authoritative_admissions.get(&incoming.request_token)
                                .is_some_and(|identity| identity != &crate::admissions::identity(&incoming)) {
                                owner.persist = false;
                                callback(owner, Err(nzbd_state::StateError::Corrupt(
                                    "shared snapshot admission conflicts with accepted authority catalog".into()
                                )));
                                return;
                            }
                            if let Some(index) = positions.get(&incoming.request_token).copied() {
                                receipts[index] = crate::admissions::merge_current(&receipts[index], &incoming);
                            } else {
                                positions.insert(incoming.request_token.clone(), receipts.len());
                                receipts.push(incoming);
                            }
                        }
                        owner.state.next_job_id = receipts.iter().fold(owner.state.next_job_id,
                            |next, receipt| next.max(receipt.job_id));
                        owner.state.url_admission_receipts = receipts;
                    }
                    owner.state.next_job_id = owner.state.next_job_id.max(state.next_job_id);
                    owner.state.next_file_id = owner.state.next_file_id.max(state.next_file_id);
                    owner.state.download_paused = state.download_paused;
                    adopted_speed = Some(state.speed_limit_bps);
                    for job in state.jobs {
                        if !jobs.iter().any(|current| current.id == job.id) {
                            jobs.push(job);
                        }
                    }
                }
                let was_fenced = owner.persistence_fenced;
                // Only explicit authority adoption may reopen a fenced writer,
                // after loading its snapshot and validating the current epoch.
                if owner.persist_guard.as_ref().is_some_and(|guard| !guard())
                    || (was_fenced && owner.persist_guard.is_none())
                {
                    owner.persist = false;
                    callback(owner, Err(nzbd_state::StateError::Corrupt(
                        "authority snapshot fence is not current".into(),
                    )));
                    return;
                }
                owner.persistence_fenced = false;
                owner.persist = true;
                owner.replace_projection_then(jobs, adopted_speed.map(|speed| (speed_revision, speed)), move |owner, result| {
                    if was_fenced && result.is_err() {
                        owner.persistence_fenced = true;
                        owner.persist = false;
                    }
                    callback(owner, result);
                });
            },
        );
    }

    // -- scheduling ----------------------------------------------------------

    fn grant_work(&mut self, server_id: ServerId, max: usize) -> Vec<Lease> {
        if self.cancel.is_cancelled()
            || self.disk_low
            || self.is_blocked(server_id)
            || self.journal_checkpoint.is_some()
        {
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
            .state
            .jobs
            .iter()
            .filter(|job| transitions::failed_cleanup_only(job))
        {
            excluded.insert(job.id, "failed cleanup metadata only".into());
        }
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
            #[cfg(test)]
            {
                self.work_selection_attempts += 1;
            }
            let result = next_for_server(&self.state, server, &mut ctx);
            let exhausted = result.exhausted;
            let lease = result.lease;
            for r in exhausted {
                self.fail_segment(r);
            }
            let Some(r) = lease else { break };

            if let Err(e) = self.ensure_allocation(r.job, false) {
                if matches!(e, nzbd_state::artifacts::Error::Busy(_)) {
                    // No allocation was admitted and no callback owns this
                    // row. Retain it in the queue and yield this bounded batch
                    // until the payload-capacity wakeup; preserve prior leases.
                    break;
                }
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
                self.hold_job(r.job, allocation_hold_cause(&e), "download_write", &message);
                continue;
            }
            let writer = self.writer_for(r.job, r.file);
            let Some(generation) = self.writers.get(&r.file).map(|writer| writer.generation) else {
                // Only an owner-handled persistence completion can publish the
                // writer. Yield this batch instead of re-selecting its unchanged
                // Pending segment once per queued job. Keep any leases already
                // granted and preserve the active-download cap.
                break;
            };
            let Some(seg) = self.state.segment_mut(r) else {
                break;
            };
            seg.state = SegmentState::Leased { server: server_id };
            let message_id = seg.message_id.to_string();
            let started = if let Some(job) = self.state.job_mut(r.job) {
                if matches!(job.status, JobStatus::Queued) {
                    job.status = JobStatus::Downloading;
                    true
                } else {
                    false
                }
            } else {
                false
            };
            if started {
                if let Some(job) = self.state.job_mut(r.job) {
                    if self
                        .timing_cache
                        .sync(job, &self.artifacts.installation, false)
                    {
                        self.dirty = true;
                    }
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
            || self
                .state
                .job(job)
                .is_some_and(transitions::failed_cleanup_only)
            || self.verification.contains_key(&job)
        {
            if let WriterFact::Finalized {
                naming: Some(naming),
                ..
            } = fact
            {
                self.artifacts.discard_writer_naming(*naming);
            }
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
                        self.fail_job_terminal(
                            job,
                            nzbd_types::FailureReason::Identity,
                            None,
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
                    self.fail_job_terminal(
                        job,
                        nzbd_types::FailureReason::Identity,
                        None,
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
                naming,
            } => {
                if !ok {
                    if let Some(naming) = naming {
                        self.artifacts.discard_writer_naming(*naming);
                    }
                    self.on_writer_finalized(job, file, false, final_path, combined_crc);
                    return;
                }
                let proof_key = format!("*File:proof:{}", file.0);
                let naming_allocation = self.allocation_cache.get(&job).cloned();
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
                        if let Some(naming) = naming {
                            owner.artifacts.discard_writer_naming(*naming);
                        }
                        return;
                    }
                    if committed {
                        if let Some(naming) = naming {
                            if let Some(allocation) = naming_allocation {
                                owner.artifacts.remember_writer_naming(
                                    job.0,
                                    file.0,
                                    generation,
                                    &allocation,
                                    *naming,
                                );
                            } else {
                                owner.artifacts.discard_writer_naming(*naming);
                            }
                        }
                        owner.on_writer_finalized(job, file, true, final_path, combined_crc);
                    } else {
                        if let Some(naming) = naming {
                            owner.artifacts.discard_writer_naming(*naming);
                        }
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
            WriterFact::Error { error, cause } => {
                if let Some(writer) = self.writers.remove(&file) {
                    self.retire_writer_handle(job, writer);
                }
                self.writer_epochs.remove(&file);
                self.on_msg(EngineMsg::WriterError {
                    job,
                    file,
                    error,
                    cause,
                });
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
        // A held download still counts its late article failures.
        if !matches!(
            job.status,
            JobStatus::Queued | JobStatus::Downloading | JobStatus::Paused | JobStatus::Held
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
            self.fail_job_terminal(
                job,
                nzbd_types::FailureReason::Identity,
                None,
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
            self.fail_job_terminal(
                job,
                nzbd_types::FailureReason::Identity,
                None,
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
        self.hold_job_until(job, cause, stage, message, None)
    }
    pub(super) fn prepare_hold_job_until(
        &mut self,
        job: JobId,
        cause: nzbd_types::HoldCause,
        stage: &str,
        message: &str,
        not_before: Option<i64>,
    ) -> bool {
        if self.persistence_fenced || self.automatic_control_pending(job) {
            return false;
        }
        let before = self.state.job(job).cloned();
        let cause_str = cause.as_str().to_string();
        let Some(j) = self.state.job_mut(job) else {
            return false;
        };
        if j.params.iter().any(|(k, _)| k == nzbd_types::PP_DONE_PARAM) {
            return false;
        }
        let prior = j.control();
        // A newer hold replaces the control (revision + 1): the first cause
        // does not win (contract §2.3). Only the same cause at the same stage
        // is already recorded.
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
            manual_pause: prior_held.map_or(
                j.status == JobStatus::Paused
                    && !(matches!(stage, "torrent_seed_retire" | "torrent_seed_forget")
                        && crate::torrent_acquisitions::is_managed_seed(j)
                        && j.torrent.as_ref().is_some_and(|torrent| {
                            torrent.stop_reason != Some(nzbd_types::TorrentStopReason::Manual)
                        })),
                |c| c.manual_pause,
            ),
            retry_attempt: if timed {
                attempts_made.saturating_add(1)
            } else {
                attempts_made
            },
            next_retry_at_unix: timed.then(|| {
                now.saturating_add(hold_retry_delay_with_jitter(job, attempts_made, now))
                    .max(not_before.unwrap_or(0))
            }),
            blocked_since_unix: episode.and_then(|c| c.blocked_since_unix).or(Some(now)),
            released_at_unix: None,
            deadline_in_s: None,
            deadline_unix: None,
        };
        let deadline_secs = self.tuning.hold_deadline_secs(&cause);
        // The deadline runs from when this episode began, so a hold that
        // changes cause mid-outage does not restart the clock.
        let deadline_unix = control
            .blocked_since_unix
            .unwrap_or(now)
            .saturating_add(deadline_secs);
        let mut control = control;
        // Seed retirement runs after the job's terminal and never ends it,
        // so its holds carry no deadline.
        if !crate::terminal::seed_retirement_stage(stage) {
            control.deadline_unix = Some(deadline_unix);
            control.deadline_in_s = Some(deadline_unix.saturating_sub(now));
        }
        if control
            .blocked_since_unix
            .is_some_and(|since| now.saturating_sub(since) >= 1800)
        {
            control.message = format!(
                "{}; warning: blocked for at least 30 minutes",
                control.message.chars().take(1900).collect::<String>()
            );
        }
        j.set_control(&control);
        if stage == "torrent_seed_retire" {
            if let Some(mut marker) =
                crate::torrent_acquisitions::seed_retirement(j).filter(|marker| {
                    marker.keep_blocked_revision.is_some()
                        && marker.keep_blocked_control_revision.is_none()
                })
            {
                marker.keep_blocked_control_revision = Some(control.revision.clone());
                set_owner_param(
                    &mut j.params,
                    crate::torrent_acquisitions::SEED_RETIREMENT_PARAM,
                    Some(serde_json::to_string(&marker).unwrap()),
                );
            }
        }
        j.status = JobStatus::Held;
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
        if !self.prepare_automatic_control_change(job, before.as_ref()) {
            return false;
        }
        true
    }

    pub(super) fn hold_job_until(
        &mut self,
        job: JobId,
        cause: nzbd_types::HoldCause,
        stage: &str,
        message: &str,
        not_before: Option<i64>,
    ) -> bool {
        let before = self.state.job(job).cloned();
        if !self.prepare_hold_job_until(job, cause, stage, message, not_before) {
            return false;
        }
        self.pending_control_commits.insert(job);
        self.commit_control_change_then(job, before, move |owner, ok| {
            owner.pending_control_commits.remove(&job);
            if ok {
                owner.publish_now();
                if let Some(control) = owner.state.job(job).and_then(Job::control) {
                    owner.emit(Event::JobControlChanged { job, control });
                }
            }
        });
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
    pub(crate) fn retry_hold_proven(
        &mut self,
        job: JobId,
        revision: &str,
        by: RetryBy,
        custody_checked: bool,
        guard: &crate::hold_release::HoldReleaseGuard,
    ) -> Result<(), String> {
        if self.persistence_fenced
            || (by == RetryBy::Recovery && self.automatic_control_pending(job))
        {
            return Err("durable queue ownership is fenced".into());
        }
        let before = self
            .state
            .job(job)
            .cloned()
            .ok_or_else(|| "job not found".to_string())?;
        if by == RetryBy::Recovery && !guard.worker_checked() && self.delegated.contains_key(&job) {
            return Err("this job is assigned to its worker execution lane".into());
        }
        if !self.persist && !guard.worker_checked() {
            return Err("this node is not the queue authority".into());
        }
        if !guard.matches(&self.artifacts, &before, self.persist) {
            return Err("the source, control authority or live worker grant changed".into());
        }
        let mut control = before
            .control()
            .filter(|c| c.lifecycle == "held")
            .ok_or_else(|| "job is not held".to_string())?;
        if control.revision != revision {
            return Err("the hold changed since it was read; reload and retry".into());
        }
        let native_preparation = crate::torrent_acquisitions::preparation_hold(&before, &control);
        if control.hold_cause().is_storage() && !control.post_side() && !native_preparation {
            return Err("storage holds are released by the capacity check".into());
        }
        if by == RetryBy::Recovery
            && control.hold_cause().policy().timed()
            && control.next_retry_at_unix.is_none_or(|at| at > unix_now())
        {
            return Err("the hold is not due for automatic recovery".into());
        }
        if before.torrent.is_some() && !native_preparation {
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
            j.status = if native_preparation && control.stage == "torrent_metadata" {
                JobStatus::Fetching
            } else if post_side {
                JobStatus::PostQueued
            } else {
                JobStatus::Queued
            };
            j.set_control(&control);
            if by == RetryBy::Operator && control.stage == "torrent_seed_retire" {
                if let Some(mut marker) = crate::torrent_acquisitions::seed_retirement(j) {
                    marker.retry_revision = control.revision.clone();
                    marker.keep_blocked_revision = None;
                    marker.keep_blocked_control_revision = None;
                    set_owner_param(
                        &mut j.params,
                        crate::torrent_acquisitions::SEED_RETIREMENT_PARAM,
                        Some(serde_json::to_string(&marker).unwrap()),
                    );
                }
            }
        }
        self.dirty = true;
        if by == RetryBy::Recovery && !self.prepare_automatic_control_change(job, Some(&before)) {
            if let Some(row) = self.state.job_mut(job) {
                *row = before;
            }
            return Err("automatic transition could not be prepared".into());
        }
        Ok(())
    }

    /// Push an automatic hold's next attempt out on the same backoff, keeping
    /// the original reason and recording the refusal beside it. The new time
    /// lives in memory even if this save fails; the next snapshot carries it.
    fn reschedule_hold_proven(
        &mut self,
        job: JobId,
        revision: &str,
        reason: &str,
        guard: &crate::hold_release::HoldReleaseGuard,
    ) -> bool {
        if self.persistence_fenced
            || self.automatic_control_pending(job)
            || (!guard.worker_checked() && self.delegated.contains_key(&job))
            || !self
                .state
                .job(job)
                .is_some_and(|current| guard.matches(&self.artifacts, current, self.persist))
        {
            return false;
        }
        let before = self.state.job(job).cloned();
        let Some(j) = self.state.job_mut(job) else {
            return false;
        };
        let Some(mut control) = j
            .control()
            .filter(|c| c.lifecycle == "held" && c.revision == revision)
        else {
            return false;
        };
        // A timed hold moves its next attempt out; a re-observed one keeps
        // its cadence and only records why it is still held.
        let timed = control.hold_cause().policy().timed();
        if timed {
            let delay = hold_retry_delay_with_jitter(job, control.retry_attempt, unix_now());
            control.retry_attempt = control.retry_attempt.saturating_add(1);
            control.next_retry_at_unix = Some(unix_now().saturating_add(delay));
        }
        let original = control
            .message
            .split("; retry refused: ")
            .next()
            .unwrap_or("")
            .to_string();
        let warning = if control
            .blocked_since_unix
            .is_some_and(|since| unix_now().saturating_sub(since) >= 1800)
        {
            "; warning: blocked for at least 30 minutes"
        } else {
            ""
        };
        let message: String = format!("{original}; retry refused: {reason}{warning}")
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
        if !self.prepare_automatic_control_change(job, before.as_ref()) {
            if let Some(before) = before {
                *self.state.job_mut(job).unwrap() = before;
            }
            return false;
        }
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
            // The partial's recorded coverage cannot be proven against an
            // expected size. That is not contradictory evidence (contract
            // §2.2 `identity`): verification re-checks the retained partial
            // and reopens its invalid ranges for download, releasing this
            // hold (`accepted_hold_rewrite`). Bounded by the hold deadline.
            self.hold_job(
                job,
                nzbd_types::HoldCause::StorageUnavailable,
                "finalize",
                crate::owner::COVERAGE_UNVERIFIED,
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
            || self.journal_checkpoint.is_some()
            || self.cancel.is_cancelled()
            || self.verification.contains_key(&job_id)
            || self.retiring_writers.contains_key(&job_id)
        {
            return;
        }
        if self.state.job(job_id).is_some_and(|j| j.held()) {
            return;
        }
        if self.terminals_in_flight.contains(&job_id)
            || self.deferred_terminals.contains_key(&job_id)
        {
            return; // the job's terminal is being (or waits to be) recorded
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
        // Contract §2.2: the owner types its own terminal. A write that did
        // not land is `io` (parked, never the release's fault); articles
        // below critical are `health` (blamed, salvageable). Neither passes
        // through post-processing admission.
        let mut terminal: Option<(nzbd_types::FailureReason, &'static str, String)> = None;
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
                tracing::error!(job = job_id.0, %reason,
                    "job failed: the download completed but the files did not");
                terminal = Some((nzbd_types::FailureReason::Io, "download", reason));
            }
        } else if let Some(stage) = resume_post_stage {
            status = JobStatus::Post { stage };
            job.status = status;
        } else if status == JobStatus::Failed {
            let critical = Health::calc_critical(&job.totals, true);
            terminal = Some((
                nzbd_types::FailureReason::Health,
                "health",
                format!(
                    "download health {} below critical health {}",
                    health.0, critical.0
                ),
            ));
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
        if let Some((reason, stage, message)) = terminal {
            // The terminal record, history and markers come first; the
            // advisory `job_finished` follows the stamp.
            self.fail_job_terminal_then(
                job_id,
                reason,
                None,
                stage,
                &message,
                Box::new(move |owner, row| {
                    if row.is_some() {
                        owner.emit(Event::JobFinished {
                            job: job_id,
                            name,
                            status: JobStatus::Failed,
                            health: health.0,
                        });
                    }
                }),
            );
            self.publish_now();
            return;
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
        if let Some(context) = self
            .state
            .job(job)
            .and_then(nzbd_state::artifacts::WorkerControlContext::for_job)
        {
            if self.artifacts.worker_grant_is_stopped(job.0, &context)? {
                return Err(nzbd_state::artifacts::Error::Conflict(
                    "worker grant was durably stopped".into(),
                ));
            }
        }
        if self.retiring_writers.contains_key(&job) {
            return Err(nzbd_state::artifacts::Error::Conflict(
                "writer retirement in progress".into(),
            ));
        }
        let record = self
            .state
            .job(job)
            .ok_or(nzbd_state::artifacts::Error::NotFound)?;
        let plan = transitions::imported_payload_plan(record, &self.dest_dir)
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
        if !self.payload_capacity_available() {
            return Err(nzbd_state::artifacts::Error::Busy(
                "payload allocation awaits lane capacity",
            ));
        }
        // Cache hits need only the plan above, not a clone of every file and
        // segment. Own the descriptor only when scheduling an actual allocation.
        let record = record.clone();
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
                    Err(nzbd_state::StateError::Busy(_)) => {
                        // The action never started. The scheduler retains the
                        // row and will retry on the payload capacity wakeup.
                    }
                    Err(error) => {
                        owner.hold_job(
                            job,
                            allocation_hold_cause(&error),
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
        self.artifacts.invalidate_writer_naming_file(job.0, file.0);
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
        let collect_naming = !entry.is_par2 && record.files.iter().any(|file| file.is_par2);
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
                collect_naming,
                naming_inventory: collect_naming.then(|| owner.artifacts.clone()),
                #[cfg(test)]
                naming_test_hook: None,
                #[cfg(test)]
                sync_test_hook: None,
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
            if let Some(record) = owner.state.job(job) {
                let source = owner.artifacts.for_job(job.0).ok().flatten();
                owner
                    .cluster_writer_stops
                    .observe(record, source.as_ref(), Some(&writer));
            }
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

    fn stale_worker_retirement(&self, command: &QueueCommand) -> bool {
        match command {
            QueueCommand::RemoveJobSilent {
                job,
                retirement: Some(retirement),
                ..
            } => self.state.job(*job).is_none_or(|record| {
                nzbd_state::artifacts::WorkerControlContext::for_job(record).as_ref()
                    != Some(retirement.context())
            }),
            _ => false,
        }
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
            if self.stale_worker_retirement(&command) {
                self.on_command(command);
                continue;
            }
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
                        || (self.record_transitions.contains(&job.id)
                            && !self.settled_post_rollback(job.id))
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
                _ => {
                    self.on_command(command);
                    continue;
                }
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
        let retirement_record = self.state.job(job).cloned().unwrap();
        let retirements = self.url_resolutions.clone();

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
                retirements
                    .retire_job(&retirement_record)
                    .map_err(|error| nzbd_state::StateError::Corrupt(error.to_string()))?;
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
                owner.persist_then(move |owner, _snapshot_ok| {
                    // The exact cancellation tombstone is already fsync'd.
                    let ok = true;
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
        if self
            .last_terminal_replay
            .is_none_or(|at| at.elapsed() >= terminal_owner::TERMINAL_REPLAY_EVERY)
        {
            self.last_terminal_replay = Some(Instant::now());
            self.replay_terminals();
        }
        self.convert_legacy_holds();
        self.persist_post_rollbacks_then(|_| {});
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
        self.retry_journal_checkpoint();
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
                    && !matches!(job.status, JobStatus::Paused | JobStatus::Held)
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
            // Seed policy stops the backend; a hold on the seed stays a hold.
            job.stop();
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
            let fact_job = match &fact {
                BackendFact::MetadataReady { job, .. }
                | BackendFact::Ready { job, .. }
                | BackendFact::Stopped { job, .. }
                | BackendFact::Resumed { job }
                | BackendFact::Removed { job, .. }
                | BackendFact::Failed { job, .. } => *job,
            };
            if self
                .state
                .job(fact_job)
                .and_then(crate::torrent_acquisitions::seed_retirement)
                .is_some_and(|marker| matches!(marker.phase.as_str(), "detached" | "deleted"))
            {
                continue;
            }
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
                        let before = self.state.job(*job).cloned();
                        let managed_retiring = before.as_ref().is_some_and(|row| {
                            crate::torrent_acquisitions::seed_retirement(row).is_some()
                        });
                        if let Some(torrent) = self
                            .state
                            .job_mut(*job)
                            .and_then(|record| record.torrent.as_mut())
                        {
                            torrent.removal_outcome = Some(disposition);
                            torrent.removal_confirmed_at_unix = Some(unix_now());
                            if let Some(row) = self.state.job_mut(*job) {
                                if let Some(mut marker) =
                                    crate::torrent_acquisitions::seed_retirement(row)
                                        .filter(|marker| marker.phase == "requested")
                                {
                                    marker.phase = "detached".into();
                                    if let Some(mut control) = row.control().filter(|control| {
                                        control.lifecycle == "held"
                                            && control.stage == "torrent_seed_forget"
                                            && !control.manual_pause
                                    }) {
                                        if let Some(next) = control
                                            .revision
                                            .parse::<u64>()
                                            .ok()
                                            .and_then(|value| value.checked_add(1))
                                        {
                                            control.revision = next.to_string();
                                            control.lifecycle = "running".into();
                                            control.next_retry_at_unix = None;
                                            row.set_control(&control);
                                            // The detached seed stays stopped,
                                            // no longer held.
                                            row.normalize_hold_status();
                                        }
                                    }
                                    set_owner_param(
                                        &mut row.params,
                                        crate::torrent_acquisitions::SEED_RETIREMENT_PARAM,
                                        Some(serde_json::to_string(&marker).unwrap()),
                                    );
                                }
                            }
                            self.dirty = true;
                        }
                        if managed_retiring {
                            let id = *job;
                            self.pending_control_commits.insert(id);
                            self.persist_then(move |owner, ok| {
                                owner.pending_control_commits.remove(&id);
                                if !ok {
                                    if let Some(before) = before {
                                        if let Some(row) = owner.state.job_mut(id) {
                                            *row = before;
                                        }
                                    }
                                }
                                owner.publish_now();
                            });
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
                if terminal_failure && !crate::torrent_acquisitions::is_managed_seed(job) {
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
                    && crate::torrent_acquisitions::seed_retirement(job)
                        .is_none_or(|marker| marker.phase == "deleted")
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
                params: job
                    .params
                    .iter()
                    .filter(|(key, _)| !key.starts_with("*Torrent:"))
                    .cloned()
                    .collect(),
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

    fn sync_lifecycle_timings(&mut self) {
        let node = self.artifacts.installation.as_str();
        let ids = self.state.jobs.iter().map(|job| job.id).collect();
        self.timing_cache.retain(&ids);
        for job in &mut self.state.jobs {
            if self
                .timing_cache
                .sync(job, node, self.delegated.contains_key(&job.id))
            {
                self.dirty = true;
            }
        }
    }

    fn publish_snapshot(&mut self, rate: u64) {
        self.sync_lifecycle_timings();
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
                    control: j.control().map(|mut control| {
                        // Relative on the wire: hosts share no clock.
                        control.deadline_in_s = control
                            .deadline_unix
                            .filter(|_| control.lifecycle == "held")
                            .map(|deadline| deadline.saturating_sub(unix_now()));
                        control
                    }),
                    status_projected: nzbd_types::projected_status(j).into(),
                    transfer: nzbd_types::transfer_of(&j.params),
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
                    torrent_role: if crate::torrent_acquisitions::is_acquisition(j) {
                        Some("acquisition")
                    } else if j.torrent.is_some() {
                        Some("seed")
                    } else {
                        None
                    },
                    seed_donor_job_id: crate::torrent_acquisitions::donor(j),
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
                            JobStatus::Queued
                                | JobStatus::Downloading
                                | JobStatus::Paused
                                | JobStatus::Held
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
                    if !matches!(j.status, JobStatus::Paused | JobStatus::Held)
                        && !j.held()
                        && !summary.pp_done
                        && !transitions::failed_cleanup_only(j)
                    {
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
            pending_automatic_controls: self
                .state
                .jobs
                .iter()
                .filter(|job| self.automatic_control_pending(job.id))
                .map(|job| job.id)
                .collect(),
            native_descriptor_references: self
                .state
                .jobs
                .iter()
                .filter_map(|job| {
                    crate::torrent_acquisitions::descriptor_reference(job)
                        .and_then(|reference| reference.source_identity(job).ok())
                })
                .collect(),
            native_seed_dependencies: self
                .state
                .jobs
                .iter()
                .filter_map(crate::torrent_acquisitions::source_dependency)
                .collect(),
            native_seed_retirements: self
                .state
                .jobs
                .iter()
                .filter_map(|job| {
                    crate::torrent_acquisitions::seed_retirement(job).map(|marker| (job.id, marker))
                })
                .collect(),
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
        // The status mirrors the hold record (M6b): every path that writes
        // a held control writes `Held`, every release leaves it.
        // (A control of an unknown version on a running job is fenced like a
        // hold but keeps its status; only `Paused`/`Held` must agree.)
        #[cfg(debug_assertions)]
        {
            let agrees = |j: &Job| {
                (j.status != JobStatus::Held || j.held())
                    && !(j.status == JobStatus::Paused && j.held())
            };
            debug_assert!(
                self.state.jobs.iter().all(agrees),
                "status disagrees with hold record: {:?}",
                self.state
                    .jobs
                    .iter()
                    .filter(|j| !agrees(j))
                    .map(|j| (j.id, j.status, j.control().map(|c| c.lifecycle)))
                    .collect::<Vec<_>>()
            );
        }
        let projected = snap
            .jobs
            .iter()
            .map(|summary| (summary.id, summary.status))
            .collect::<Vec<_>>();
        self.shared.store(Arc::new(snap));
        // Review #283-1: status changes are announced from the projection
        // just published, after it is readable. A consumer that wakes on the
        // event and reads the snapshot sees the status the event names — in
        // particular a job boot verification returns from `queued` to
        // `post_processing` (the owner's own status never left `Post`).
        self.emit_status_changes(&projected);
    }

    fn save_snapshot(&mut self) {
        if self.background_save_pending || self.journal_checkpoint.is_some() {
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
            self.persistence_fenced = true;
        }
    }

    fn emit(&self, ev: Event) {
        let _ = self.events.send(ev);
    }

    fn bump_epoch(&self) {
        self.epoch_tx.send_modify(|v| *v += 1);
    }
}

/// Accepted journal replay carries the expected publication length into the
/// durable row used by payload verification, without replacing prior evidence.
fn merge_replayed_size(state: &mut QueueState, record: &JournalRecord) -> Option<u64> {
    let job = state.job_mut(record.job)?;
    if !job.files.iter().any(|file| {
        file.id == record.file
            && file
                .segments
                .iter()
                .any(|segment| segment.number == record.segment_number)
    }) {
        return None;
    }
    let key = format!("*File:size:{}", record.file.0);
    if let Some((_, size)) = job.params.iter().find(|(name, _)| name == &key) {
        return size.parse().ok().filter(|size| *size > 0);
    }
    if record.file_size > 0 {
        job.params.push((key, record.file_size.to_string()));
        return Some(record.file_size);
    }
    None
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
    let pinned = pin_unpinned_failed_source(receipt, inventory);
    refresh_pinned_url_admission_custody(receipt, inventory) || pinned
}

/// Failures retained before Runner pinned their source generation into the
/// receipt reported `runner_owned` with no artifact to refresh from, so no
/// later delete or Keep could ever reach Curator, which holds replacement
/// while Runner owns failed bytes. Adopt the allocation this installation
/// recorded for the exact job. This pins identity only; custody still comes
/// from that artifact's own state below.
fn pin_unpinned_failed_source(
    receipt: &mut UrlAdmissionReceipt,
    inventory: &nzbd_state::artifacts::Inventory,
) -> bool {
    if receipt.source_artifact_id.is_some()
        || receipt.source_generation.is_some()
        || receipt.custody_state != "runner_owned"
        || receipt.outcome != "failed"
        || receipt.runner_instance_id != inventory.installation
    {
        return false;
    }
    let Ok(Some(artifact)) = inventory.for_job(receipt.job_id) else {
        return false;
    };
    if artifact.job != Some(receipt.job_id) || (!artifact.owned && !artifact.terminal()) {
        return false;
    }
    receipt.source_artifact_id = Some(artifact.id);
    receipt.source_generation = Some(artifact.generation);
    true
}

fn refresh_pinned_url_admission_custody(
    receipt: &mut UrlAdmissionReceipt,
    inventory: &nzbd_state::artifacts::Inventory,
) -> bool {
    let Some(artifact_id) = receipt.source_artifact_id.as_deref() else {
        return false;
    };
    if receipt.source_generation.is_none() {
        return false;
    }
    let Ok(artifact) = inventory.receipt_custody(artifact_id) else {
        return false;
    };
    apply_receipt_custody(receipt, &artifact)
}

/// Refresh a checkpoint's pinned receipts in one current inventory read.
/// A single statement keeps one SQLite read transaction across all headers,
/// avoiding a Gluster file-lock round trip for every individual receipt.
fn refresh_url_admission_custodies(
    receipts: &mut [UrlAdmissionReceipt],
    inventory: &nzbd_state::artifacts::Inventory,
) -> bool {
    let mut changed = false;
    for receipt in receipts.iter_mut() {
        changed |= pin_unpinned_failed_source(receipt, inventory);
    }
    let keys = receipts
        .iter()
        .filter(|receipt| receipt.source_generation.is_some())
        .filter_map(|receipt| receipt.source_artifact_id.clone())
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return changed;
    }
    let Ok(custodies) = inventory.receipt_custodies(&keys) else {
        return changed;
    };
    for receipt in receipts {
        let Some(Ok(custody)) = receipt
            .source_artifact_id
            .as_ref()
            .and_then(|id| custodies.get(id))
        else {
            continue;
        };
        changed |= apply_receipt_custody(receipt, custody);
    }
    changed
}

fn apply_receipt_custody(
    receipt: &mut UrlAdmissionReceipt,
    artifact: &nzbd_state::artifacts::ArtifactCustody,
) -> bool {
    // Consumer custody is terminal for this receipt. Retention of a separate
    // processing source cannot reclaim the accepted publication.
    let Some(source_generation) = receipt.source_generation.as_deref() else {
        return false;
    };
    if artifact.generation != source_generation {
        return false;
    }
    let keep_changed = receipt.keep_source_on_failure != artifact.keep_on_failure;
    receipt.keep_source_on_failure = artifact.keep_on_failure;
    if receipt.custody_state == "curator" {
        let cleanup = if artifact.state == "deleted" {
            Some("removed")
        } else if artifact.keep || artifact.keep_on_failure {
            Some("retained")
        } else {
            None
        };
        let changed =
            cleanup.is_some_and(|status| receipt.source_cleanup_status.as_deref() != Some(status));
        if let Some(status) = cleanup {
            receipt.source_cleanup_status = Some(status.into());
        }
        return keep_changed || changed;
    }
    // `source_gone` is reconcile_missing's positive finding: the storage root
    // verified and the exact source directory is absent. Nothing remains in
    // Runner custody, so it settles the receipt exactly like a Runner delete;
    // reporting runner_owned for it held Curator's replacement forever.
    if artifact.state == "deleted" || (artifact.state == "source_gone" && artifact.owned) {
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
/// The hold message for a partial whose coverage cannot be proven; startup
/// verification owns its exit.
///
/// One-release bridge (remove in M6): pre-v2 builds persisted this partial
/// as `identity_conflict` and told it apart only by this exact message, so
/// `rearmed_hold` and `legacy_coverage_hold` match the constant to keep
/// those rows on the verification path. Once no `identity_conflict` control
/// can remain on disk (M6 drops the legacy cause strings), match the cause
/// alone and delete the message comparison.
pub(crate) const COVERAGE_UNVERIFIED: &str =
    "file coverage or expected size is unverified; partial retained";

/// The revision moves so a client holding the old one re-reads.
fn hold_retry_delay_with_jitter(job: JobId, attempt: u8, now: i64) -> i64 {
    let base = nzbd_types::hold_retry_delay_seconds(attempt);
    base.saturating_add(
        ((job.0 as u64).wrapping_mul(0x9e3779b97f4a7c15) ^ now as u64 ^ u64::from(attempt))
            .wrapping_rem(base.max(1) as u64 / 10 + 1) as i64,
    )
}

pub(crate) fn rearmed_hold(
    mut control: nzbd_types::JobControl,
    now: i64,
) -> Option<nzbd_types::JobControl> {
    let mut cause = control.hold_cause();
    // A pre-v2 `io` hold whose message names an unavailable volume was
    // environmental all along; keep it a hold. Every other legacy terminal
    // cause is left for the boot conversion (`convert_legacy_holds`).
    // The pre-v2 message is rendered text with no errno attached; the
    // stderr rule reads its `(os error N)` suffix (removed with the legacy
    // causes in M6b).
    if control.cause == "io"
        && crate::stderr_hold_cause(&control.message)
            == Some(nzbd_types::HoldCause::StorageUnavailable)
    {
        cause = nzbd_types::HoldCause::StorageUnavailable;
    }
    // A pre-v2 unverified-coverage hold is a re-verification, not a
    // contradiction: it keeps its verification exit (`COVERAGE_UNVERIFIED`).
    if control.cause == "identity_conflict"
        && control.stage == "finalize"
        && control.message == COVERAGE_UNVERIFIED
    {
        cause = nzbd_types::HoldCause::StorageUnavailable;
    }
    // Older builds called a control-request timeout a shutdown and turned
    // this exact failed-repair cleanup error into a permanent review hold.
    // Do not infer retryability from arbitrary subprocess output or titles.
    if control.cause == "post_failure"
        && control.message.split("; retry ").next()
            == Some("subprocess failed: read live Keep setting: engine is shutting down")
    {
        cause = nzbd_types::HoldCause::EngineUnavailable;
        control.message = "read live Keep setting: engine request was unavailable; retrying failed-job finalization".into();
    }
    if nzbd_types::HoldCause::legacy_terminal(&control.cause).is_some()
        && cause.as_str() == control.cause
    {
        return None;
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
fn allocation_hold_cause(error: &(dyn std::error::Error + 'static)) -> nzbd_types::HoldCause {
    if crate::error_hold_cause(error) == Some(nzbd_types::HoldCause::StorageUnavailable) {
        nzbd_types::HoldCause::StorageUnavailable
    } else {
        nzbd_types::HoldCause::Allocation
    }
}

/// Params earlier builds persisted for a clock this build no longer runs.
/// The failed-source cleanup retry (`*PP:failed-cleanup-*`) lives on the
/// disposal `Operation.next_retry` since M6; a row an older build wrote
/// still carries these, so they are dropped when the queue loads and are
/// never written or read again.
pub(crate) const RETIRED_PARAMS: [&str; 3] = [
    "*PP:failed-cleanup-next-retry",
    "*PP:failed-cleanup-blocked-since",
    "*PP:failed-cleanup-last-error",
];

/// Drop [`RETIRED_PARAMS`] from a loaded row; true when any was present.
pub(crate) fn drop_retired_params(job: &mut Job) -> bool {
    let before = job.params.len();
    job.params
        .retain(|(key, _)| !RETIRED_PARAMS.contains(&key.as_str()));
    job.params.len() != before
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
#[path = "owner_tests.rs"]
mod tests;
