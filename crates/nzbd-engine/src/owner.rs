//! The queue-owner task (ARCHITECTURE.md §8.1): the single serialization
//! point for all queue mutation. Inputs: a bounded command/event channel and
//! a 1 Hz tick. Outputs: leases granted to connection tasks (pull model),
//! `arc-swap` snapshots for lock-free readers, broadcast events, journal
//! appends and debounced snapshot saves.
//!
//! Every handler is synchronous — the owner never awaits while reasoning
//! about state. Sends toward writer tasks use `try_send` with a
//! retry-on-tick fallback so owner ⇄ writer backpressure can never deadlock.

use crate::backend::{BackendCommand, BackendFact, BackendOwnerPort, RemovalOutcome};
use crate::events::Event;
use crate::failover::{AttemptOutcome, Ladder, SegmentAttempt, Verdict};
use crate::queue::{
    active_set, final_status, job_dir_name, next_for_server, pick_par_files, recompute_job_totals,
    QueueState, SegRef, SelectionCtx,
};
use crate::rate::{RateLimiter, SpeedMeter};
use crate::snapshot::{JobSummary, QueueSnapshot, SharedSnapshot, StorageVolumeSnapshot};
use crate::volumes::DiskGuardReading;
use crate::writer::{spawn_writer, WriteCmd, WriterHandle};
use crate::{AddOpts, Tuning};
use arc_swap::ArcSwap;
use nzbd_nzb::ParsedNzb;
use nzbd_state::{FsJournal, JobJournals, JournalRecord, SnapshotStore, UncleanMarker};
use nzbd_types::{
    FileId, Health, Job, JobId, JobKind, JobStatus, PostStage, SegmentState, ServerDef, ServerId,
    StageSpan, TorrentControlIntent, TorrentPayloadDisposition, TorrentPhase, TorrentRemovalIntent,
};
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

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

/// External commands (wrapped by `EngineHandle` methods).
#[derive(Debug)]
pub(crate) enum QueueCommand {
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
        /// Replies `(id, created)`. `created == false` means the same URL is
        /// already fetching — the caller must NOT spawn a second fetch task.
        reply: oneshot::Sender<(JobId, bool)>,
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
        reply: oneshot::Sender<()>,
    },
    /// Replace only an authority copy that still exists. This is the fenced
    /// terminal-update path: a demoted worker must never resurrect a job that
    /// `RetainJobs` already removed while an external disposition awaited.
    ImportJobIfPresent {
        job: Box<Job>,
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
        reply: oneshot::Sender<()>,
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
    RetryPostHold {
        job: JobId,
        revision: String,
        reply: oneshot::Sender<bool>,
    },
    BeginResourceProbe {
        job: JobId,
        revision: String,
        reply: oneshot::Sender<bool>,
    },
    ReleaseResourceHold {
        job: JobId,
        revision: String,
        reply: oneshot::Sender<bool>,
    },
    HoldJob {
        job: JobId,
        cause: String,
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
}

/// Everything that reaches the owner task.
#[derive(Debug)]
pub(crate) enum EngineMsg {
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
    /// A completed file has finished its background par2-name inspection.
    /// `None` matters: terminal completion waits for every inspection that
    /// was already started, so it needs an explicit negative result too.
    JobNameInspected {
        job: JobId,
        name: Option<String>,
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
    retiring_writers: HashMap<JobId, Vec<WriterHandle>>,
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
    journal: JobJournals,
    snap_store: SnapshotStore,
    pending_sources: nzbd_state::torrent_sources::PendingSourceStore,
    /// How long the last snapshot write took — feeds [`save_spacing`], so a
    /// slow state volume stretches the save cadence instead of consuming
    /// the owner loop.
    last_save_ms: u64,
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
    marker: UncleanMarker,
    /// Queue-authority persistence (snapshot save/compact). Worker-mode
    /// engines run with this off; journals stay on regardless.
    persist: bool,
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
    ) -> Result<Owner, nzbd_state::StateError> {
        let artifacts = Arc::new(
            nzbd_state::artifacts::Inventory::open(artifact_dir.unwrap_or(state_dir))
                .map_err(|e| nzbd_state::StateError::Corrupt(e.to_string()))?,
        );
        std::fs::create_dir_all(&dest_dir).map_err(|source| nzbd_state::StateError::Io {
            op: "create download root",
            path: dest_dir.clone(),
            source,
        })?;
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

            artifacts
                .reconcile_startup(&state.jobs.iter().map(|job| job.id.0).collect::<Vec<_>>())
                .map_err(|e| nzbd_state::StateError::Corrupt(e.to_string()))?;
            for job in &state.jobs {
                if job.torrent.is_none() && artifacts.for_job(job.id.0).ok().flatten().is_none() {
                    artifacts
                        .register_legacy_active(
                            job.id.0,
                            &dest_dir,
                            &dest_dir.join(job_dir_name(job)),
                        )
                        .map_err(|e| nzbd_state::StateError::Corrupt(e.to_string()))?;
                }
            }
            // Legacy phase-1 global journal: fold once, then retire it.
            let legacy = FsJournal::open(state_dir)?;
            let mut replayed = legacy.replay()?;
            drop(legacy);
            if !replayed.is_empty() {
                tracing::info!(records = replayed.len(), "migrating legacy global journal");
                let _ = std::fs::remove_file(state_dir.join("segments.journal"));
            }
            replayed.extend(JobJournals::replay_all(state_dir)?);
            replay_count = replayed.len();
            for rec in replayed {
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
                let dir = artifacts
                    .for_job(j.id.0)
                    .ok()
                    .flatten()
                    .map(|a| a.path)
                    .unwrap_or_else(|| dest_dir.join(job_dir_name(j)));
                for f in &mut j.files {
                    let key = format!("*File:size:{}", f.id.0);
                    if let Some(size) = j
                        .params
                        .iter()
                        .find(|(k, _)| k == &key)
                        .and_then(|(_, v)| v.parse::<u64>().ok())
                    {
                        file_sizes.insert(f.id, size);
                    }
                    // Revalidate even old page-cache acknowledgements. Sparse length
                    // alone proves nothing; only matching range bytes retain Done.
                    let stable = dir.join(format!(".runner-file-{}.part", f.id.0));
                    let repair_key = format!("*File:repair:{}", f.id.0);
                    let repair_pending = j.params.iter().any(|(key, _)| key == &repair_key);
                    let path = if f.finalized && !repair_pending {
                        dir.join(&f.filename)
                    } else if stable.exists() {
                        stable
                    } else {
                        dir.join(format!("{}.part", f.filename))
                    };
                    for segment in &mut f.segments {
                        if let SegmentState::Done { offset, len, crc } = segment.state {
                            if !crate::writer::validate_range(&path, offset, len as u64, crc) {
                                segment.state = SegmentState::Pending;
                                f.finalized = false;
                                f.crc32 = None;
                            }
                        }
                    }
                }
                if let Some(mut control) = j.control().filter(|c| {
                    c.version == 1
                        && c.lifecycle == "held"
                        && !c.manual_pause
                        && c.cause == "identity_conflict"
                        && c.stage == "finalize"
                        && c.message
                            == "file coverage or expected size is unverified; partial retained"
                }) {
                    if let Some(revision) = control
                        .revision
                        .parse::<u64>()
                        .ok()
                        .and_then(|r| r.checked_add(1))
                    {
                        control.revision = revision.to_string();
                        control.lifecycle = "running".into();
                        control.message = "rechecking retained download for repair".into();
                        j.set_control(&control);
                        j.status = JobStatus::Queued;
                    }
                }
                if let Some(mut control) = j.control().filter(|c|
                    c.version == 1 && c.lifecycle == "held" && !c.manual_pause
                    && matches!(c.cause.as_str(), "unknown" | "post_failure")
                    && c.stage == "rar_rename"
                    && c.message == "subprocess failed: RAR set lacks checked order/membership evidence; filenames preserved")
                {
                    if artifacts.validate_post_retry(j.id.0).is_ok() {
                        if let Some(revision) = control.revision.parse::<u64>().ok().and_then(|r| r.checked_add(1)) {
                            control.revision = revision.to_string();
                            control.lifecycle = "running".into();
                            control.message = "retrying archive restoration with SFV evidence".into();
                            j.set_control(&control);
                            j.status = JobStatus::PostQueued;
                        }
                    }
                }
                if j.held() {
                    j.status = JobStatus::Paused;
                }
            }
            state.recompute_all_totals();
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

        let mut allocated_jobs = HashSet::new();
        let mut retiring_writers = HashMap::new();
        // Validate recovered allocations before the actor starts. Segment
        // scheduling subsequently consults only actor-owned admission state.
        for job in &mut state.jobs {
            if job.torrent.is_some() {
                continue;
            }
            if let Some(a) = artifacts
                .for_job(job.id.0)
                .map_err(|e| nzbd_state::StateError::Corrupt(e.to_string()))?
            {
                if a.state == "active" {
                    artifacts
                        .allocate(job.id.0, &a.root, &a.path)
                        .map_err(|e| nzbd_state::StateError::Corrupt(e.to_string()))?;
                    allocated_jobs.insert(job.id);
                } else if a.state == "retained" {
                    job.status = JobStatus::Paused;
                    retiring_writers.insert(job.id, Vec::new());
                }
            }
        }
        let initial_disk_hold = tuning.min_free_disk_bytes > 0;
        Ok(Owner {
            state,
            attempts: HashMap::new(),
            blocked: HashMap::new(),
            writers: HashMap::new(),
            retiring_writers,
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
            journal,
            snap_store,
            pending_sources,
            marker,
            persist,
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
            disk_guard: Arc::new(ArcSwap::from_pointee(DiskGuardReading::default())),
            evaluated_disk_guard: Arc::new(DiskGuardReading::default()),
            volumes: crate::volumes::VolumeBook::load(state_dir, journal_suffix),
            quota_reached: false,
            // A configured floor starts fail-safe until the first complete
            // asynchronous probe. Startup remains nonblocking, but no work
            // can slip through the measurement window.
            disk_low: initial_disk_hold,
            enospc_latched: false,
            enospc_observed: 0,
            enospc_where: None,
            rotate: 0,
            guard_tick: 0,
        })
    }

    pub(crate) async fn run(mut self, mut rx: mpsc::Receiver<EngineMsg>) {
        // Fold replayed journal into a fresh snapshot, then finish anything
        // that completed just before the crash.
        self.save_snapshot();
        self.startup_pass();
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

        // Graceful shutdown = fast crash with a flush (§4.6).
        if let Err(e) = self.journal.sync() {
            tracing::warn!(error = %e, "journal sync at shutdown failed");
        }
        self.volumes.save_if_dirty();
        self.save_snapshot();
        if let Err(e) = self.marker.disarm() {
            tracing::warn!(error = %e, "could not clear unclean marker");
        }
        self.writers.clear(); // writers drain and exit
        tracing::info!("queue owner stopped");
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
        let mut to_finalize = Vec::new();
        let mut to_check = Vec::new();
        for job in &self.state.jobs {
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
        match msg {
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
            EngineMsg::JobNameInspected { job, name } => self.on_job_name_inspected(job, name),
            EngineMsg::OutOfSpace { whence } => self.observe_out_of_space(&whence),
            EngineMsg::WriterError { job, file, error } => {
                tracing::warn!(job = job.0, file = file.0, %error, "writer error; failing file");
                if crate::is_out_of_space(&error) {
                    self.observe_out_of_space(&error);
                    let cause = if error.to_lowercase().contains("quota") {
                        "quota"
                    } else {
                        "capacity"
                    };
                    self.hold_job(
                        job,
                        cause,
                        if error.starts_with("finalize") {
                            "finalize"
                        } else {
                            "download_write"
                        },
                        &error,
                    );
                    return;
                }
                self.hold_job(job, "io", "download_write", &error);
            }
        }
    }

    fn on_command(&mut self, cmd: QueueCommand) {
        match cmd {
            QueueCommand::ReserveTorrentAdmission {
                source,
                secret,
                opts,
                reply,
            } => {
                // Reserve from the queue owner first: it alone allocates ids. The
                // reservation is not durable until the protected sidecar exists.
                let id = self.state.reserve_torrent_admission(source, opts);
                let result = match self.pending_sources.write(id, &secret) {
                    Ok(secret_ref) => {
                        // The reference is derived from the allocated id, never a
                        // separately predicted identity.
                        if let Some(pending) = self
                            .state
                            .pending_admissions
                            .iter_mut()
                            .find(|pending| pending.job_id == id)
                        {
                            pending.secret_ref = secret_ref;
                        }
                        self.save_snapshot();
                        self.publish_now();
                        Ok(id)
                    }
                    Err(error) => {
                        self.state.cancel_torrent_admission(id);
                        Err(error)
                    }
                };
                let _ = reply.send(result);
            }
            QueueCommand::CommitTorrentAdmission { job, commit, reply } => {
                let result = self.state.commit_torrent_admission(job, *commit);
                if result.is_some() {
                    self.save_snapshot();
                    let _ = self.pending_sources.remove(job);
                    self.publish_now();
                    self.bump_epoch();
                }
                let _ = reply.send(result);
            }
            QueueCommand::CancelTorrentAdmission { job, reply } => {
                let before = self.state.pending_admissions.clone();
                let removed = self.state.cancel_torrent_admission(job);
                let result = if !removed {
                    Ok(false)
                } else {
                    match self.save_snapshot_result() {
                        Ok(true) => {
                            self.publish_now();
                            self.pending_sources.remove(job).map(|()| true)
                        }
                        Ok(false) => {
                            self.state.pending_admissions = before;
                            Err(nzbd_state::StateError::Corrupt(
                                "torrent admission cancellation requires durable queue ownership"
                                    .into(),
                            ))
                        }
                        Err(error) => {
                            self.state.pending_admissions = before;
                            self.on_snapshot_save_error(&error);
                            Err(error)
                        }
                    }
                };
                let _ = reply.send(result);
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
                let id = self.state.admit_nzb(
                    name.clone(),
                    &parsed,
                    category,
                    priority,
                    self.tuning.pause_extra_pars,
                );
                if let Some(j) = self.state.job_mut(id) {
                    if let Some(dupe) = dupe {
                        j.dupe = dupe;
                    }
                    if paused {
                        j.status = JobStatus::Paused;
                    }
                    j.params.extend(params);
                }
                // Nothing has been written yet, so a better name may take
                // the directory with it.
                let name = self.name_from_requestor(id, true).unwrap_or(name);
                let allocation = self.allocate_new_payload(id);
                if let Err(e) = allocation {
                    self.state.jobs.retain(|j| j.id != id);
                    let _ = reply.send(Err(nzbd_state::StateError::Corrupt(format!(
                        "payload allocation: {e}"
                    ))));
                    return;
                }
                tracing::info!(job = id.0, %name, "job added");
                self.save_snapshot(); // adds are durable immediately
                self.publish_now();
                self.emit(Event::JobAdded { job: id, name });
                self.bump_epoch();
                let _ = reply.send(Ok(id));
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
                // Same URL already mid-fetch? Return that job instead of
                // piling up identical downloads — a client that retries an
                // add because the fetch looks slow (or a daemon restart hid
                // its progress) would otherwise queue N copies of the same
                // 50 GiB download. Failed fetches are NOT matched: re-adding
                // one is a deliberate retry.
                let existing = self
                    .state
                    .jobs
                    .iter()
                    .find(|j| {
                        matches!(j.status, JobStatus::Fetching)
                            && j.params.iter().any(|(k, v)| k == "*URL" && *v == url)
                    })
                    .map(|j| j.id);
                if let Some(id) = existing {
                    tracing::info!(
                        job = id.0,
                        %url,
                        "url add deduplicated: same URL is already fetching"
                    );
                    // The retry still gets its params applied. A client
                    // that re-adds because the fetch looked stalled is the
                    // same client that will later look for its tracking id
                    // on the job; dropping the params here loses the id on
                    // exactly the adds most likely to be retried, and the
                    // caller has no way to tell it happened.
                    if !params.is_empty() {
                        if let Some(j) = self.state.job_mut(id) {
                            for (k, v) in params {
                                match j.params.iter_mut().find(|(pk, _)| *pk == k) {
                                    Some(slot) => slot.1 = v,
                                    None => j.params.push((k, v)),
                                }
                            }
                        }
                        self.save_snapshot();
                        self.publish_now();
                    }
                    let _ = reply.send((id, false));
                    return;
                }
                let id = self.state.admit_url(name.clone(), &url, category, priority);
                if let Some(j) = self.state.job_mut(id) {
                    if let Some(dupe) = dupe {
                        j.dupe = dupe;
                    }
                    if paused {
                        j.params.push(("*AddPaused".into(), "yes".into()));
                    }
                    j.params.extend(params);
                }
                let name = self.name_from_requestor(id, true).unwrap_or(name);
                tracing::info!(job = id.0, %name, %url, "url job added (fetching)");
                self.save_snapshot();
                self.publish_now();
                self.emit(Event::JobAdded { job: id, name });
                self.bump_epoch();
                let _ = reply.send((id, true));
            }
            QueueCommand::CompleteUrlFetch { job, parsed, reply } => {
                let ok = self
                    .state
                    .complete_url_fetch(job, &parsed, self.tuning.pause_extra_pars);
                if ok {
                    if let Some(j) = self.state.job_mut(job) {
                        if let Some(pos) = j.params.iter().position(|(k, _)| k == "*AddPaused") {
                            j.params.remove(pos);
                            j.status = JobStatus::Paused;
                        }
                    }
                    // `complete_url_fetch` has just re-run the evidence
                    // pass against the fetched NZB. If that still came up
                    // with nothing, fall back to who asked. Still no files
                    // on disk, so the directory follows the name.
                    self.name_from_requestor(job, true);
                    // URL admissions need the same fresh allocation as direct
                    // uploads. A retained release directory belongs to the old
                    // job, even when the newly fetched NZB has the same name.
                    if let Err(error) = self.allocate_new_payload(job) {
                        self.hold_job(
                            job,
                            "allocation",
                            "download_write",
                            &format!("payload allocation: {error}"),
                        );
                    }
                    tracing::info!(job = job.0, "url fetch complete");
                    self.save_snapshot();
                    self.publish_now();
                    self.emit(Event::UrlFetchResolved { job });
                    self.bump_epoch();
                }
                let _ = reply.send(ok);
            }
            QueueCommand::FailUrlFetch { job, error, reply } => {
                let name = match self.state.job_mut(job) {
                    Some(j) if matches!(j.status, JobStatus::Fetching) => {
                        j.status = JobStatus::Failed;
                        Some(j.name.clone())
                    }
                    _ => None,
                };
                if let Some(name) = name {
                    tracing::warn!(job = job.0, %error, "url fetch failed");
                    self.save_snapshot();
                    self.publish_now();
                    self.emit(Event::JobFinished {
                        job,
                        name,
                        status: JobStatus::Failed,
                        health: 0,
                    });
                    self.emit(Event::UrlFetchResolved { job });
                    self.bump_epoch();
                }
                let _ = reply.send(());
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
                    let committed = !self.persist || self.save_snapshot();
                    if committed {
                        self.publish_now();
                        self.emit(Event::JobControlChanged { job, control });
                    }
                    let _ = reply.send(committed);
                    return;
                }
                let before = self.state.job(job).cloned();
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
                    self.dirty = true;
                    if self.persist_then_command(BackendCommand::Pause { job }) {
                        self.bump_epoch();
                        self.publish_now();
                        true
                    } else {
                        if let Some(before) = before {
                            *self.state.job_mut(job).unwrap() = before;
                        }
                        false
                    }
                } else {
                    match self.state.job_mut(job) {
                        Some(j)
                            if matches!(j.status, JobStatus::Queued | JobStatus::Downloading) =>
                        {
                            j.status = JobStatus::Paused;
                            self.dirty = true;
                            self.bump_epoch();
                            self.publish_now();
                            true
                        }
                        _ => false,
                    }
                };
                let _ = reply.send(ok);
            }
            QueueCommand::Resume { job, reply } => {
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
                let before = self.state.job(job).cloned();
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
                    self.dirty = true;
                    if !self.persist || self.save_snapshot() {
                        self.bump_epoch();
                        self.publish_now();
                        true
                    } else {
                        if let Some(before) = before {
                            *self.state.job_mut(job).unwrap() = before;
                        }
                        false
                    }
                } else {
                    match self.state.job_mut(job) {
                        Some(j) if matches!(j.status, JobStatus::Paused) => {
                            j.status = JobStatus::Queued;
                            self.dirty = true;
                            self.bump_epoch();
                            self.publish_now();
                            true
                        }
                        _ => false,
                    }
                };
                let _ = reply.send(ok);
            }
            QueueCommand::QuiescePayload { job, reply } => {
                let result = (|| {
                    if self.delegated.contains_key(&job) {
                        return Err(
                            "remote writer lease must be retired before payload deletion".into(),
                        );
                    }
                    let record = self.state.job_mut(job).ok_or("job not found")?;
                    if record.torrent.is_some()
                        || matches!(
                            record.status,
                            JobStatus::Post { .. } | JobStatus::PostQueued
                        )
                    {
                        return Err(
                            "post-processing or torrent ownership must quiesce before deletion"
                                .to_string(),
                        );
                    }
                    self.artifacts
                        .prepare_forget(job.0)
                        .map_err(|e| e.to_string())?;
                    record.status = JobStatus::Paused;
                    self.allocated_jobs.remove(&job);
                    let writers = self.retiring_writers.entry(job).or_default();
                    for file in &record.files {
                        if let Some(writer) = self.writers.remove(&file.id) {
                            writer.stop.cancel();
                            writers.push(writer);
                        }
                    }
                    let writers = writers.clone();
                    self.attempts.retain(|r, _| r.job != job);
                    self.pending_finalize.retain(|(j, _)| *j != job);
                    self.save_snapshot();
                    self.publish_now();
                    Ok(writers)
                })();
                let _ = reply.send(result);
            }
            QueueCommand::Delete {
                job,
                delete_files,
                reply,
            } => {
                let before = self.state.job(job).cloned();
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
                let ok = if let Some(command) = torrent_command {
                    self.dirty = true;
                    if self.persist_then_command(command) {
                        self.bump_epoch();
                        self.publish_now();
                        true
                    } else {
                        if let Some(before) = before {
                            *self.state.job_mut(job).unwrap() = before;
                        }
                        false
                    }
                } else {
                    self.delete_job(job, delete_files)
                };
                let _ = reply.send(ok);
            }
            QueueCommand::SetPriority {
                job,
                priority,
                reply,
            } => {
                let before = self.state.job(job).cloned();
                let torrent = match self.state.job_mut(job) {
                    Some(j) if j.kind == JobKind::Torrent && j.priority != priority => {
                        j.priority = priority;
                        true
                    }
                    _ => false,
                };
                let ok = if torrent {
                    self.dirty = true;
                    if self.persist_then_command(BackendCommand::SetPriority { job, priority }) {
                        self.bump_epoch();
                        self.publish_now();
                        true
                    } else {
                        if let Some(before) = before {
                            *self.state.job_mut(job).unwrap() = before;
                        }
                        false
                    }
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
            QueueCommand::SetCategory {
                job,
                category,
                reply,
            } => {
                let ok = self.state.job_mut(job).is_some_and(|record| {
                    record.category = category;
                    true
                });
                if ok {
                    self.save_snapshot();
                    self.publish_now();
                    self.bump_epoch();
                }
                let _ = reply.send(ok);
            }
            QueueCommand::SetTorrentSeedPolicy { job, policy, reply } => {
                if policy
                    .ratio_limit
                    .is_some_and(|v| !v.is_finite() || v <= 0.0)
                    || policy.time_limit_secs == Some(0)
                {
                    let _ = reply.send(false);
                    return;
                }
                let before = self.state.job(job).cloned();
                let ok = self.state.job_mut(job).is_some_and(|record| {
                    let Some(torrent) = record.torrent.as_mut() else {
                        return false;
                    };
                    torrent.seed_policy = policy;
                    true
                });
                let ok = if ok {
                    self.dirty = true;
                    if self.persist && !self.save_snapshot() {
                        if let Some(before) = before {
                            *self.state.job_mut(job).unwrap() = before;
                        }
                        false
                    } else {
                        self.update_seed_policies(unix_now());
                        self.publish_now();
                        self.bump_epoch();
                        true
                    }
                } else {
                    false
                };
                let _ = reply.send(ok);
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
                let _ = self.import_job(*job, fold_journals, emit_finished);
                let _ = reply.send(());
            }
            QueueCommand::ImportJobIfPresent { job, reply } => {
                let id = job.id;
                let previous = self.state.job(id).cloned();
                let previous_delegated = self.delegated.get(&id).cloned();
                let previous_mirror = self.mirror.get(&id).cloned();
                let previous_post_fetch = self.post_fetch_files.get(&id).cloned();
                let committed = previous.is_some() && self.import_job(*job, false, false);
                if !committed {
                    // Atomic means memory and disk agree. A failed snapshot
                    // commit must not leave an in-memory-only PP_DONE or
                    // finalization key that later scans mistake for durable.
                    let _ = self.remove_job_silent(id);
                    if let Some(previous) = previous {
                        self.state.jobs.push(previous);
                    }
                    match previous_delegated {
                        Some(node) => {
                            self.delegated.insert(id, node);
                        }
                        None => {
                            self.delegated.remove(&id);
                        }
                    }
                    match previous_mirror {
                        Some(stats) => {
                            self.mirror.insert(id, stats);
                        }
                        None => {
                            self.mirror.remove(&id);
                        }
                    }
                    match previous_post_fetch {
                        Some(files) => {
                            self.post_fetch_files.insert(id, files);
                        }
                        None => {
                            self.post_fetch_files.remove(&id);
                        }
                    }
                    self.publish_now();
                    self.bump_epoch();
                }
                let _ = reply.send(committed);
            }
            QueueCommand::ExportJob { job, reply } => {
                let _ = reply.send(self.state.job(job).cloned().map(Box::new));
            }
            QueueCommand::RemoveJobSilent { job, reply } => {
                let _ = reply.send(self.remove_job_silent(job));
            }
            QueueCommand::SetDelegated { job, node, reply } => {
                let ok = self.state.job(job).is_some();
                if ok {
                    match &node {
                        Some(n) => {
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
            QueueCommand::MirrorProgress { job, stats } => {
                if self.delegated.contains_key(&job) {
                    self.mirror.insert(job, stats);
                    self.publish_now();
                }
            }
            QueueCommand::FoldJobJournals { job, reply } => {
                self.fold_job_journals(job);
                let _ = reply.send(());
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
                let result = self.adopt_authority();
                let _ = reply.send(result);
            }
            QueueCommand::AdoptReplicatedAuthority { jobs, reply } => {
                self.state.jobs = jobs;
                self.state.pending_admissions.clear();
                self.state.next_job_id = self
                    .state
                    .jobs
                    .iter()
                    .map(|job| job.id.0)
                    .max()
                    .unwrap_or(0);
                self.state.next_file_id = self
                    .state
                    .jobs
                    .iter()
                    .flat_map(|job| job.files.iter().map(|file| file.id.0))
                    .max()
                    .unwrap_or(0);
                self.state.recompute_all_totals();
                self.dirty = true;
                self.save_snapshot();
                self.publish_now();
                self.bump_epoch();
                let _ = reply.send(());
            }
            QueueCommand::AbandonRelocation {
                operation,
                revision,
                generation,
                reply,
            } => {
                let result = (|| {
                    let op = self
                        .artifacts
                        .operation(&operation)
                        .map_err(|e| e.to_string())?;
                    let artifact = self
                        .artifacts
                        .get(&op.artifact)
                        .map_err(|e| e.to_string())?;
                    if let Some(job) = artifact.job.map(JobId) {
                        if self.retiring_writers.contains_key(&job) {
                            return Err("payload retirement is in progress".into());
                        }
                        if let Some(j) = self.state.job(job) {
                            let held_move = j.control().is_some_and(|c| {
                                c.lifecycle == "held"
                                    && matches!(c.cause.as_str(), "unknown" | "post_failure")
                                    && c.stage == "move"
                            });
                            if !held_move
                                || j.files.iter().any(|f| {
                                    self.writers
                                        .get(&f.id)
                                        .is_some_and(|w| !*w.stopped.borrow())
                                })
                            {
                                return Err("only a quiescent post-processing move hold can be abandoned while the job is queued".into());
                            }
                        }
                    }
                    self.artifacts
                        .abandon_relocation(&operation, revision, &generation)
                        .map_err(|e| e.to_string())
                })();
                let _ = reply.send(result);
            }
            QueueCommand::RetryPostHold {
                job,
                revision,
                reply,
            } => {
                let before = self.state.job(job).cloned();
                let control = before.as_ref().and_then(|j| j.control()).filter(|c| {
                    c.lifecycle == "held"
                        && c.revision == revision
                        && matches!(c.cause.as_str(), "unknown" | "post_failure")
                        && matches!(
                            c.previous_status,
                            Some(
                                JobStatus::Post { .. }
                                    | JobStatus::PostQueued
                                    | JobStatus::Completed
                            )
                        )
                        && matches!(
                            c.stage.as_str(),
                            "par_rename"
                                | "rar_rename"
                                | "par_verify"
                                | "par_repair"
                                | "unpack"
                                | "extract"
                                | "cleanup"
                                | "post_unpack_rename"
                                | "move"
                        )
                });
                let mut changed = None;
                if !self.retiring_writers.contains_key(&job)
                    && before.as_ref().is_some_and(|j| {
                        !j.params.iter().any(|(k, _)| k == nzbd_types::PP_DONE_PARAM)
                            && j.files.iter().all(|f| {
                                self.writers.get(&f.id).is_none_or(|w| *w.stopped.borrow())
                            })
                    })
                {
                    if let Some(mut c) = control {
                        if let Some(next) = c
                            .revision
                            .parse::<u64>()
                            .ok()
                            .and_then(|r| r.checked_add(1))
                        {
                            match self.artifacts.validate_post_retry(job.0) {
                                Ok(()) => {
                                    c.revision = next.to_string();
                                    c.lifecycle = "running".into();
                                    c.manual_pause = false;
                                    c.message =
                                        "Post-processing retry admitted after custody validation"
                                            .into();
                                    let j = self.state.job_mut(job).unwrap();
                                    j.status = JobStatus::PostQueued;
                                    j.set_control(&c);
                                    changed = Some(c);
                                }
                                Err(e) => {
                                    tracing::warn!(job=job.0, error=%e, "post-processing retry refused")
                                }
                            }
                        }
                    }
                }
                self.dirty |= changed.is_some();
                let ok = changed.is_some() && (!self.persist || self.save_snapshot());
                if ok {
                    self.bump_epoch();
                    self.publish_now();
                    self.emit(Event::JobControlChanged {
                        job,
                        control: changed.unwrap(),
                    });
                } else if let Some(before) = before {
                    *self.state.job_mut(job).unwrap() = before;
                }
                let _ = reply.send(ok);
            }
            QueueCommand::BeginResourceProbe {
                job,
                revision,
                reply,
            } => {
                let quiescent = self.state.job(job).is_some_and(|j| {
                    j.files
                        .iter()
                        .all(|f| self.writers.get(&f.id).is_none_or(|w| *w.stopped.borrow()))
                });
                let now = unix_now();
                let mut ok = false;
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
                        }) && after <= now
                            && count < 8
                        {
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
                            ok = !self.persist || self.save_snapshot();
                        }
                    }
                }
                let _ = reply.send(ok);
            }
            QueueCommand::ReleaseResourceHold {
                job,
                revision,
                reply,
            } => {
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
                let ok = changed.is_some() && (!self.persist || self.save_snapshot());
                if ok {
                    self.bump_epoch();
                    self.publish_now();
                    self.emit(Event::JobControlChanged {
                        job,
                        control: changed.unwrap(),
                    });
                    let files: Vec<_> = self
                        .state
                        .job(job)
                        .unwrap()
                        .files
                        .iter()
                        .map(|f| f.id)
                        .collect();
                    for file in files {
                        self.after_file_change(job, file);
                    }
                } else if let Some(before) = before {
                    *self.state.job_mut(job).unwrap() = before;
                }
                let _ = reply.send(ok);
            }
            QueueCommand::HoldJob {
                job,
                cause,
                stage,
                message,
                reply,
            } => {
                let committed = self.hold_job(job, &cause, &stage, &message);
                let _ = reply.send(committed);
            }
            QueueCommand::SetJobStatus { job, status, reply } => {
                if self.state.job(job).is_some_and(|j| j.held()) {
                    let _ = reply.send(false);
                    return;
                }
                if self.retiring_writers.contains_key(&job) {
                    let _ = reply.send(false);
                    return;
                }
                let ok = match self.state.job_mut(job) {
                    Some(j) => {
                        j.status = status;
                        true
                    }
                    None => false,
                };
                if ok {
                    self.dirty = true;
                    if self.persist
                        && matches!(
                            status,
                            JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
                        )
                    {
                        self.save_snapshot();
                    }
                    self.publish_now();
                }
                let _ = reply.send(ok);
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
                    self.remove_job_silent(id);
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

    fn import_job(&mut self, mut job: Job, fold_journals: bool, emit_finished: bool) -> bool {
        if let Some(control) = self
            .state
            .job(job.id)
            .and_then(|j| j.control())
            .filter(|c| c.lifecycle == "held")
        {
            // Remote/exported legacy snapshots cannot erase a newer local fence.
            job.set_control(&control);
            job.status = JobStatus::Paused;
            job.params.retain(|(k, _)| k != nzbd_types::PP_DONE_PARAM);
        }
        // Normalize transient state from the wire.
        for f in &mut job.files {
            for s in &mut f.segments {
                if matches!(s.state, SegmentState::Leased { .. }) {
                    s.state = SegmentState::Pending;
                }
            }
        }
        if matches!(job.status, JobStatus::Downloading) {
            job.status = JobStatus::Queued;
        }
        if job.torrent.is_none()
            && !matches!(
                job.status,
                JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
            )
        {
            let relative = std::path::PathBuf::from(job_dir_name(&job));
            if relative
                .components()
                .any(|c| !matches!(c, std::path::Component::Normal(_)))
            {
                return false;
            }
            let path = self.dest_dir.join(relative);
            // Cluster grants may use nested, fenced generation directories.
            // Register their bytes without local deletion authority.
            if let Err(error) = std::fs::create_dir_all(&path) {
                tracing::error!(job=job.id.0,%error,"cannot create granted payload directory");
                return false;
            }
            self.allocated_jobs.remove(&job.id);
            if let Err(error) =
                self.artifacts
                    .register_legacy_active(job.id.0, path.parent().unwrap(), &path)
            {
                tracing::error!(job=job.id.0, %error, "cluster payload inventory unavailable");
                return false;
            }
        }
        if job.torrent.is_none() && self.artifacts.for_job(job.id.0).ok().flatten().is_none() {
            let path = self.dest_dir.join(job_dir_name(&job));
            if path.is_dir() {
                if let Err(error) =
                    self.artifacts
                        .register_legacy_active(job.id.0, path.parent().unwrap(), &path)
                {
                    tracing::error!(%error,"cannot inventory legacy completed payload");
                    return false;
                }
            }
        }
        let job_id = job.id;
        let max_file = job.files.iter().map(|f| f.id.0).max().unwrap_or(0);
        self.state.next_job_id = self.state.next_job_id.max(job_id.0);
        self.state.next_file_id = self.state.next_file_id.max(max_file);

        // Replace any existing copy (idempotent re-grant / completion).
        if self.state.job(job_id).is_some() {
            self.remove_job_silent(job_id);
        }
        let terminal = matches!(
            job.status,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
        );
        let name = job.name.clone();
        let status = job.status;
        let health = Health::calc(&job.totals).0;
        self.state.jobs.push(job);
        if let Some(j) = self.state.job_mut(job_id) {
            recompute_job_totals(j);
        }
        self.delegated.remove(&job_id);
        self.mirror.remove(&job_id);
        if fold_journals {
            self.fold_job_journals(job_id);
        }
        if let Some(files) = self.state.job(job_id).and_then(recovered_post_fetch_files) {
            self.post_fetch_files.insert(job_id, files);
        } else {
            self.post_fetch_files.remove(&job_id);
        }
        tracing::info!(job = job_id.0, %name, ?status, "job imported");
        self.dirty = true;
        let committed = self.persist && self.save_snapshot();
        self.publish_now();
        if terminal && emit_finished {
            self.emit(Event::JobFinished {
                job: job_id,
                name,
                status,
                health,
            });
        }
        self.bump_epoch();
        committed
    }

    fn remove_job_silent(&mut self, job_id: JobId) -> bool {
        let Some(idx) = self.state.jobs.iter().position(|j| j.id == job_id) else {
            return false;
        };
        let job = self.state.jobs.remove(idx);
        for f in &job.files {
            self.writers.remove(&f.id);
            self.file_sizes.remove(&f.id);
            self.finalize_sent.remove(&f.id);
        }
        self.pending_finalize.retain(|(j, _)| *j != job_id);
        self.attempts.retain(|r, _| r.job != job_id);
        self.delegated.remove(&job_id);
        self.mirror.remove(&job_id);
        self.post_fetch_files.remove(&job_id);
        self.publish_now();
        self.bump_epoch();
        true
    }

    fn fold_job_journals(&mut self, job_id: JobId) {
        let recs = match JobJournals::replay_job(&self.state_dir, job_id) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(job = job_id.0, error = %e, "journal fold failed");
                return;
            }
        };
        let mut touched: HashSet<FileId> = HashSet::new();
        let mut applied = 0usize;
        for rec in recs {
            if rec.job != job_id {
                continue;
            }
            let r = SegRef {
                job: rec.job,
                file: rec.file,
                seg_number: rec.segment_number,
            };
            if rec.file_size > 0 {
                self.file_sizes.insert(rec.file, rec.file_size);
            }
            let path = self.state.job(job_id).and_then(|j| {
                j.files.iter().find(|f| f.id == rec.file).map(|f| {
                    let root = self.dest_dir.join(job_dir_name(j));
                    let stable = root.join(format!(".runner-file-{}.part", rec.file.0));
                    if stable.exists() {
                        stable
                    } else {
                        root.join(&f.filename)
                    }
                })
            });
            if !path.is_some_and(|p| {
                crate::writer::validate_range(&p, rec.offset, rec.len as u64, rec.crc32)
            }) {
                self.hold_job(
                    job_id,
                    "identity_conflict",
                    "download_write",
                    "foreign journal bytes require revalidation; evidence retained",
                );
                continue;
            }
            let Some(seg) = self.state.segment_mut(r) else {
                continue;
            };
            if !matches!(seg.state, SegmentState::Done { .. }) {
                seg.state = SegmentState::Done {
                    offset: rec.offset,
                    len: rec.len,
                    crc: rec.crc32,
                };
                self.attempts.remove(&r);
                touched.insert(rec.file);
                applied += 1;
            }
        }
        if applied > 0 {
            tracing::info!(job = job_id.0, applied, "folded journal records");
            if let Some(j) = self.state.job_mut(job_id) {
                recompute_job_totals(j);
            }
            for file in touched {
                self.after_file_change(job_id, file);
            }
            self.bump_epoch();
        }
    }

    fn adopt_authority(&mut self) -> Result<(), nzbd_state::StateError> {
        match self.snap_store.load() {
            Ok(Some(doc)) => {
                // Cluster takeover is a production recovery path just like
                // daemon startup. Validate before flipping persistence or
                // touching local state: an M1b node must not generically
                // recover, publish, or rewrite a dormant torrent row.
                let snapshot_state = match QueueState::from_runtime_doc(doc) {
                    Ok(state) => state,
                    Err(error) => {
                        self.persist = false;
                        tracing::error!(
                            error = %error,
                            "authority adoption refused; shared snapshot and local queue unchanged"
                        );
                        return Err(error);
                    }
                };
                self.state.next_job_id = self.state.next_job_id.max(snapshot_state.next_job_id);
                self.state.next_file_id = self.state.next_file_id.max(snapshot_state.next_file_id);
                self.state.download_paused = snapshot_state.download_paused;
                if self.state.speed_limit_bps.is_none() {
                    self.state.speed_limit_bps = snapshot_state.speed_limit_bps;
                    self.limiter.set(snapshot_state.speed_limit_bps);
                }
                for job in snapshot_state.jobs {
                    // Local executor copies are fresher than the late
                    // leader's snapshot — keep them.
                    if self.state.job(job.id).is_none() {
                        self.state.jobs.push(job);
                    }
                }
            }
            Ok(None) => {}
            Err(error) => {
                self.persist = false;
                tracing::error!(
                    error = %error,
                    "authority adoption refused; unreadable shared snapshot left unchanged"
                );
                return Err(error);
            }
        }

        self.persist = true;

        // Fold every job's journals (union across lease files).
        let ids: Vec<JobId> = self.state.jobs.iter().map(|j| j.id).collect();
        for id in &ids {
            self.fold_job_journals(*id);
        }
        self.state.recompute_all_totals();

        // Finish anything that completed right before the takeover.
        self.startup_pass();
        self.save_snapshot();
        self.publish_now();
        self.bump_epoch();
        tracing::info!(jobs = self.state.jobs.len(), "adopted queue authority");
        Ok(())
    }

    // -- scheduling ----------------------------------------------------------

    fn grant_work(&mut self, server_id: ServerId, max: usize) -> Vec<Lease> {
        if self.disk_low || self.is_blocked(server_id) {
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
                delegated: &self.delegated,
                post_fetch_files: &self.post_fetch_files,
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

            if let Err(e) = self.ensure_allocation(r.job) {
                if self.retiring_writers.contains_key(&r.job) {
                    // Writer retirement owns its own completion/wake-up. It is
                    // transient, not an allocation needing operator review.
                    break;
                }
                self.hold_job(
                    r.job,
                    "allocation",
                    "download_write",
                    &format!("payload allocation: {e}"),
                );
                continue;
            }
            let (message_id, writer) = {
                let Some(seg) = self.state.segment_mut(r) else {
                    break;
                };
                seg.state = SegmentState::Leased { server: server_id };
                let msgid = seg.message_id.to_string();
                (msgid, self.writer_for(r.job, r.file))
            };
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
            });
        }
        leases
    }

    // -- outcomes ------------------------------------------------------------

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
        if let Err(e) = self.journal.append(&JournalRecord {
            job,
            file,
            segment_number: seg_number,
            offset,
            len,
            crc32: crc,
            file_size,
        }) {
            tracing::error!(error = %e, "journal append failed");
        }
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
                "identity_conflict",
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
                "identity_conflict",
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
        if self.persist && !self.save_snapshot() {
            self.hold_job(
                job,
                "io",
                "download_write",
                "filename metadata could not be persisted",
            );
        }
    }

    fn hold_job(&mut self, job: JobId, cause: &str, stage: &str, message: &str) -> bool {
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
            .is_some_and(|c| c.lifecycle == "held" && c.cause == cause && c.stage == stage)
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
        let control = nzbd_types::JobControl {
            version: 1,
            revision: revision.to_string(),
            lifecycle: "held".into(),
            cause: cause.into(),
            stage: stage.into(),
            retry_policy: if matches!(cause, "capacity" | "quota" | "allocation") {
                "resume_same_job"
            } else {
                "review"
            }
            .into(),
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
            previous_status: prior
                .as_ref()
                .and_then(|c| c.previous_status)
                .or(Some(j.status)),
            manual_pause: prior
                .as_ref()
                .map_or(j.status == JobStatus::Paused, |c| c.manual_pause),
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
        // Failed persistence leaves the local fence in place, but emits no fact.
        if self.persist && !self.save_snapshot() {
            return false;
        }
        self.publish_now();
        tracing::warn!(job = job.0, cause, stage, message = %control.message, "job held");
        self.emit(Event::JobControlChanged { job, control });
        true
    }

    // -- completion cascade --------------------------------------------------

    fn after_file_change(&mut self, job: JobId, file: FileId) {
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
                j.dir_name.starts_with(".nzbd-cluster/range-work/")
                    && j.params.iter().any(|(k, _)| k == "*Cluster:range")
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
                "identity_conflict",
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
                if self.persist && !self.save_snapshot() {
                    self.hold_job(
                        job,
                        "io",
                        "finalize",
                        "partial checkpoint metadata could not be persisted",
                    );
                    return;
                }
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
                    self.writers.remove(&file);
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
            if self.persist && !self.save_snapshot() {
                self.hold_job(
                    job,
                    "io",
                    "finalize",
                    "complete checkpoint metadata could not be persisted",
                );
                return;
            }
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
                self.writers.remove(&file);
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
        self.writers.remove(&file);
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
        self.tracker.spawn_blocking(move || {
            let name = read_par2_name(&path);
            // This runs on the blocking pool, so waiting for channel room
            // cannot stall the queue owner. Every inspection must answer,
            // including a negative one, or completion could wait forever.
            let _ = tx.blocking_send(EngineMsg::JobNameInspected { job, name });
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
        self.save_snapshot();
        self.publish_now();
        if resume_post_stage.is_some() {
            return;
        }
        self.emit(Event::JobFinished {
            job: job_id,
            name,
            status,
            health: health.0,
        });
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
    fn allocate_new_payload(&mut self, job: JobId) -> Result<(), nzbd_state::artifacts::Error> {
        let record = self
            .state
            .job(job)
            .ok_or(nzbd_state::artifacts::Error::NotFound)?;
        let base = job_dir_name(record);
        let occupied = |name: &str| {
            self.dest_dir.join(name).symlink_metadata().is_ok()
                || self
                    .state
                    .jobs
                    .iter()
                    .any(|j| j.id != job && job_dir_name(j) == name)
        };
        if occupied(&base) {
            let mut suffix = 0u32;
            loop {
                let candidate = format!("{base}.job-{}-{suffix}", job.0);
                if !occupied(&candidate) {
                    self.state.job_mut(job).unwrap().dir_name = candidate;
                    break;
                }
                suffix = suffix.checked_add(1).ok_or_else(|| {
                    nzbd_state::artifacts::Error::Conflict("no free payload directory name".into())
                })?;
            }
        }
        self.ensure_allocation(job)
    }

    fn ensure_allocation(&mut self, job: JobId) -> Result<(), nzbd_state::artifacts::Error> {
        if self.retiring_writers.contains_key(&job) {
            return Err(nzbd_state::artifacts::Error::Conflict(
                "writer retirement in progress".into(),
            ));
        }
        if self.allocated_jobs.contains(&job) {
            return Ok(());
        }
        let record = self
            .state
            .job(job)
            .ok_or(nzbd_state::artifacts::Error::NotFound)?;
        if let Some(existing) = self.artifacts.for_job(job.0)? {
            self.artifacts
                .allocate(job.0, &existing.root, &existing.path)?;
            self.allocated_jobs.insert(job);
            return Ok(());
        }
        let dir = self.dest_dir.join(job_dir_name(record));
        self.artifacts.allocate(job.0, &self.dest_dir, &dir)?;
        self.allocated_jobs.insert(job);
        Ok(())
    }

    fn writer_for(&mut self, job: JobId, file: FileId) -> mpsc::Sender<WriteCmd> {
        if let Some(h) = self.writers.get(&file) {
            if !h.tx.is_closed() {
                return h.tx.clone();
            }
        }
        let (job_dir, filename) = match self.state.job(job) {
            Some(j) => (
                job_dir_name(j),
                j.files
                    .iter()
                    .find(|f| f.id == file)
                    .map(|f| f.filename.clone())
                    .unwrap_or_else(|| format!("file-{}", file.0)),
            ),
            None => (format!("job-{}", job.0), format!("file-{}", file.0)),
        };
        // `job_dir`, never `job.name`: a job that renamed itself from its
        // par2 metadata mid-download must not split its files across two
        // directories.
        let dir = self
            .artifacts
            .for_job(job.0)
            .ok()
            .flatten()
            .map(|a| a.path)
            .unwrap_or_else(|| self.dest_dir.join(job_dir));
        if let Err(e) = self.ensure_allocation(job) {
            tracing::error!(job = job.0, error = %e, "writer held: allocation not committed");
            let (tx, _) = mpsc::channel(1);
            return tx;
        }
        let h = spawn_writer(
            &self.tracker,
            job,
            file,
            dir,
            filename,
            self.engine_tx.clone(),
        );
        let tx = h.tx.clone();
        self.writers.insert(file, h);
        tx
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

    fn delete_job(&mut self, job_id: JobId, delete_files: bool) -> bool {
        if delete_files {
            return false;
        }
        let Some(idx) = self.state.jobs.iter().position(|j| j.id == job_id) else {
            return false;
        };
        let retiring = match self.artifacts.prepare_forget(job_id.0) {
            Ok(a) => a,
            Err(e) => {
                tracing::error!(job=job_id.0,error=%e,"cannot forget payload ownership");
                return false;
            }
        };
        let mut stopping = self.retiring_writers.remove(&job_id).unwrap_or_default();
        self.allocated_jobs.remove(&job_id);
        let job = self.state.jobs.remove(idx);
        for f in &job.files {
            if let Some(writer) = self.writers.remove(&f.id) {
                writer.stop.cancel();
                stopping.push(writer);
            }
            self.file_sizes.remove(&f.id);
            self.finalize_sent.remove(&f.id);
        }
        if let Some(artifact) = retiring {
            let inventory = self.artifacts.clone();
            self.tracker.spawn(async move {
                for mut writer in stopping {
                    if writer.stopped.wait_for(|done| *done).await.is_err() { return; }
                }
                let _ = tokio::task::spawn_blocking(move || {
                    if let Err(e) = inventory.finish(job_id.0, &artifact.path, &artifact.root, "retained") {
                        tracing::error!(job=job_id.0,error=%e,"retired payload remains on review hold");
                    }
                }).await;
            });
        }
        self.pending_finalize.retain(|(j, _)| *j != job_id);
        self.attempts.retain(|r, _| r.job != job_id);
        self.delegated.remove(&job_id);
        self.mirror.remove(&job_id);
        self.post_fetch_files.remove(&job_id);
        if self.persist {
            // A delete whose journal removal AND snapshot save both fail
            // is resurrected by the next restart's recovery. The save
            // failure already logs loudly; this one must too — if a
            // deleted job "came back", this line is the receipt.
            if let Err(e) = self.journal.remove_job(job_id) {
                tracing::error!(
                    job = job_id.0, error = %e,
                    "could not remove the deleted job's journal — if the snapshot save also fails, this delete will not survive a restart"
                );
            }
        }
        // Destructive requests are completed by EngineHandle's inventory
        // coordinator before this queue retirement command is sent.
        tracing::info!(job = job_id.0, name = %job.name, delete_files, "job deleted");
        self.save_snapshot();
        self.publish_now();
        self.emit(Event::JobDeleted { job: job_id });
        self.bump_epoch();
        true
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
        if self.tuning.daily_quota_bytes > 0 || self.tuning.monthly_quota_bytes > 0 {
            let (day, month) = self
                .volumes
                .cluster_totals(unix_now(), self.tuning.quota_start_day);
            let was = self.quota_reached;
            self.quota_reached = (self.tuning.daily_quota_bytes > 0
                && day >= self.tuning.daily_quota_bytes)
                || (self.tuning.monthly_quota_bytes > 0
                    && month >= self.tuning.monthly_quota_bytes);
            if self.quota_reached != was {
                tracing::warn!(
                    day,
                    month,
                    reached = self.quota_reached,
                    "download quota state changed"
                );
                self.publish_guard_change(was, self.quota_reached);
            }
        }
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
            self.volumes.save_if_dirty();
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
        if let Err(e) = self.journal.sync() {
            tracing::error!(error = %e, "journal fsync failed");
        }
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
        let mut desired = active_set(
            &self.state,
            &self.delegated,
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
                && torrent.control_intent == TorrentControlIntent::Running
                && torrent.removal_intent.is_none())
            .then_some(job.id)
        }));

        for job_id in desired.iter().copied() {
            let runnable = self.state.job(job_id).is_some_and(|job| {
                job.kind == JobKind::Torrent
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
                job.torrent
                    .as_ref()
                    .is_some_and(|torrent| torrent.removal_outcome.is_some())
            })
            .cloned()
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return;
        }

        if self.persist && !self.save_snapshot() {
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

            let recorded = match &self.history {
                Some(history) => match history.record_seq_durable(&entry) {
                    Ok(_) => true,
                    Err(error) => {
                        tracing::warn!(
                            job = job.id.0,
                            error = %error,
                            "torrent removal confirmed but terminal history is not durable; retaining queue record for retry"
                        );
                        false
                    }
                },
                // Embedded engine users may omit history. The daemon always
                // supplies it; preserving the old behavior here keeps the
                // engine's standalone test and library surface usable.
                None => true,
            };
            if recorded {
                self.delete_job(job.id, false);
            }
        }
    }

    /// Persist a torrent's requested state before placing its matching backend
    /// command on the bounded FIFO. A failed persistence attempt is restored
    /// by the caller, so an unrecorded request cannot become idempotently
    /// stuck or reach the engine.
    fn persist_then_command(&mut self, command: BackendCommand) -> bool {
        // Never let a later request bypass one already retained by a full
        // adapter FIFO. Refusing at the bound happens before persistence, so
        // the durable queue remains consistent with the reported result.
        if !self.pending_backend_commands.is_empty() {
            if self.pending_backend_commands.len() == MAX_PENDING_BACKEND_COMMANDS {
                return false;
            }
            if self.persist && !self.save_snapshot() {
                return false;
            }
            self.pending_backend_commands.push_back(command);
            return true;
        }

        // Worker-mode engines do not own queue persistence. That is an
        // intentional configuration, not a failed durability barrier.
        if self.persist && !self.save_snapshot() {
            return false;
        }
        self.enqueue_backend_command(command)
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
    pub(crate) fn pending_url_fetches(&self) -> Vec<(JobId, String)> {
        self.state
            .jobs
            .iter()
            .filter(|j| matches!(j.status, JobStatus::Fetching))
            .filter_map(|j| {
                j.params
                    .iter()
                    .find(|(k, _)| k == "*URL")
                    .map(|(_, url)| (j.id, url.clone()))
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

    fn save_snapshot(&mut self) -> bool {
        match self.save_snapshot_result() {
            Ok(saved) => saved,
            Err(error) => {
                self.on_snapshot_save_error(&error);
                false
            }
        }
    }

    fn on_snapshot_save_error(&mut self, error: &nzbd_state::StateError) {
        tracing::error!(
            error = %error,
            "snapshot save failed (fenced or io); demoting persistence until re-adopted"
        );
        if matches!(error, nzbd_state::StateError::Corrupt(_)) {
            self.persist = false;
        }
    }

    fn save_snapshot_result(&mut self) -> Result<bool, nzbd_state::StateError> {
        if !self.persist {
            self.dirty = false;
            return Ok(false);
        }
        let doc = self.state.to_doc();
        let write_started = Instant::now();
        let bytes = match &self.persist_guard {
            Some(g) => {
                let g = g.clone();
                self.snap_store.save_guarded(&doc, &move || g())
            }
            None => self.snap_store.save(&doc),
        }?;
        // Duration and throughput, remembered for the adaptive spacing and
        // said out loud when slow — "how fast is the state volume really?"
        // must be answerable from the log, with numbers.
        let ms = write_started.elapsed().as_millis() as u64;
        self.last_save_ms = ms;
        let mib_s = (bytes as f64 / (1 << 20) as f64) / (ms.max(1) as f64 / 1000.0);
        if ms > 1000 {
            tracing::warn!(
                bytes,
                ms,
                throughput_mib_s = format!("{mib_s:.1}"),
                "snapshot save is slow — saves are spaced out to compensate (the journal still protects progress)"
            );
        } else {
            tracing::debug!(bytes, ms, "snapshot saved");
        }
        // The snapshot now embodies every folded segment: compact journals
        // of jobs we own outright, plus orphaned job dirs (deleted jobs,
        // stale zombies). Delegated jobs have live foreign writers — skip.
        let known: HashSet<JobId> = self.state.jobs.iter().map(|j| j.id).collect();
        if let Ok(entries) = std::fs::read_dir(self.journal.jobs_dir()) {
            for entry in entries.flatten() {
                let Some(id) = entry
                    .file_name()
                    .to_string_lossy()
                    .parse::<u32>()
                    .ok()
                    .map(JobId)
                else {
                    continue;
                };
                // Known & not delegated: folded into the snapshot just
                // written. Unknown: orphan from a deleted job or a stale
                // lease. Delegated: live foreign writer — leave alone.
                let _ = known; // clarity: both known and orphan compact
                if !self.delegated.contains_key(&id) {
                    if let Err(e) = self.journal.remove_job(id) {
                        tracing::debug!(job = id.0, error = %e, "journal compact skip");
                    }
                }
            }
        }
        self.dirty = false;
        self.seed_checkpoints = durable_seed_checkpoints(&self.state);
        self.last_save = Instant::now();
        Ok(true)
    }

    fn emit(&self, ev: Event) {
        let _ = self.events.send(ev);
    }

    fn bump_epoch(&self) {
        self.epoch_tx.send_modify(|v| *v += 1);
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

/// A recovered-`Fetching` job that duplicates another job's URL: `job`
/// should fail as a duplicate of `original`.
pub(crate) type DupeOf = (JobId, JobId);

/// Split recovered `Fetching` jobs into fetches to re-spawn and duplicates
/// to fail. The first job (queue order) of each URL refetches; later
/// same-URL jobs — the pile-up left by clients re-adding while the original
/// fetch was invisible after a restart — fail as duplicates of it, instead
/// of N copies of the same download racing to completion.
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

    fn guard_test_owner(tuning: Tuning) -> (tempfile::TempDir, Owner, watch::Receiver<u64>) {
        let (tmp, owner, epoch, _adapter) = guard_test_owner_with_backend(tuning);
        (tmp, owner, epoch)
    }

    fn guard_test_owner_with_backend(
        tuning: Tuning,
    ) -> (
        tempfile::TempDir,
        Owner,
        watch::Receiver<u64>,
        crate::backend::BackendAdapterPort,
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let (budget_tx, _) = watch::channel(crate::pool::BudgetEnvelope::default());
        let (events, _) = broadcast::channel(1);
        let (epoch_tx, epoch_rx) = watch::channel(0);
        let (engine_tx, _) = mpsc::channel(1);
        let (backend, adapter) = crate::backend::backend_channel(1, 1);
        let owner = Owner::recover(
            &tmp.path().join("state"),
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
        )
        .unwrap();
        (tmp, owner, epoch_rx, adapter)
    }

    fn control_test_owner() -> (tempfile::TempDir, Owner, crate::backend::BackendAdapterPort) {
        control_test_owner_with_persistence(true, None)
    }

    fn control_test_owner_with_persistence(
        persist: bool,
        persist_guard: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    ) -> (tempfile::TempDir, Owner, crate::backend::BackendAdapterPort) {
        let tmp = tempfile::tempdir().unwrap();
        let (budget_tx, _) = watch::channel(crate::pool::BudgetEnvelope::default());
        let (events, _) = broadcast::channel(1);
        let (epoch_tx, _) = watch::channel(0);
        let (engine_tx, _) = mpsc::channel(1);
        let (backend, adapter) = crate::backend::backend_channel(1, 1);
        let owner = Owner::recover(
            &tmp.path().join("state"),
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
        )
        .unwrap();
        (tmp, owner, adapter)
    }

    #[tokio::test]
    async fn intact_publication_clears_previous_private_repair_marker() {
        let (_tmp, mut owner, _adapter) = control_test_owner();
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
            },
        );
        owner.send_finalize(JobId(1), file);
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
        let (_tmp, mut owner, _adapter) = control_test_owner();
        for id in 1..=3 {
            owner.state.jobs.push(pending_job(id));
        }
        let file = owner.state.job(JobId(1)).unwrap().files[0].id;
        owner.accept_file_metadata(JobId(1), file, "first.bin", 100);
        owner.accept_file_metadata(JobId(1), file, "different-obfuscated-name", 100);
        assert!(!owner.state.job(JobId(1)).unwrap().held());
        assert_eq!(
            owner.state.job(JobId(1)).unwrap().files[0].filename,
            "first.bin"
        );
        owner.accept_file_metadata(JobId(1), file, "third-name", 101);
        let control = owner.state.job(JobId(1)).unwrap().control().unwrap();
        assert_eq!(control.cause, "identity_conflict");
        assert_eq!(control.message, "conflicting yEnc declared size");

        for (id, name, size) in [(2, "../unsafe", 100), (3, "empty.bin", 0)] {
            let file = owner.state.job(JobId(id)).unwrap().files[0].id;
            owner.accept_file_metadata(JobId(id), file, name, size);
            assert!(owner.state.job(JobId(id)).unwrap().held());
        }
    }

    #[tokio::test]
    async fn payload_retirement_retries_keep_original_acknowledgements_and_fence_resume() {
        let (_tmp, mut owner, _adapter) = control_test_owner();
        std::fs::create_dir_all(&owner.dest_dir).unwrap();
        let job = pending_job(1);
        let file = job.files[0].id;
        owner.state.jobs.push(job);
        owner.ensure_allocation(JobId(1)).unwrap();
        let (tx, _rx) = mpsc::channel(1);
        let (done, stopped) = watch::channel(false);
        owner.writers.insert(
            file,
            WriterHandle {
                tx,
                stop: CancellationToken::new(),
                stopped,
            },
        );
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::QuiescePayload {
            job: JobId(1),
            reply,
        });
        let first = rx.await.unwrap().unwrap();
        assert_eq!(first.len(), 1);
        assert!(!*first[0].stopped.borrow());
        drop(first); // the timed-out HTTP request loses its receivers
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::Resume {
            job: JobId(1),
            reply,
        });
        assert!(!rx.await.unwrap());
        assert!(owner.ensure_allocation(JobId(1)).is_err());
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::QuiescePayload {
            job: JobId(1),
            reply,
        });
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
        let (_tmp, mut owner, _adapter) = control_test_owner();
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
        let (_tmp, mut owner, mut adapter) = control_test_owner();
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
        assert!(rx.await.unwrap());
        assert_eq!(
            adapter.next_command().await,
            Some(BackendCommand::PauseForSeedPolicy { job: JobId(1) })
        );
        let persisted = owner.snap_store.load().unwrap().unwrap();
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
        assert!(!rx.await.unwrap());

        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::SetTorrentSeedPolicy {
            job: JobId(1),
            policy: Default::default(),
            reply,
        });
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
        assert!(rx.await.unwrap());
        assert_eq!(
            owner.state.jobs[0].torrent.as_ref().unwrap().stop_reason,
            None
        );
    }

    #[tokio::test]
    async fn torrent_missing_file_recovery_is_not_blocked_by_a_completed_seed_policy() {
        let (tmp, mut owner, mut adapter) = control_test_owner();
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
        assert!(rx.await.unwrap());
        owner.schedule_torrent_starts();
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
        let persisted = owner.snap_store.load().unwrap().unwrap();
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
        owner.fold_backend_structural();
        owner.update_seed_policies(200);
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
        let (_tmp, mut owner, _adapter) = control_test_owner();
        let mut job = control_torrent_job();
        job.status = JobStatus::Paused;
        let torrent = job.torrent.as_mut().unwrap();
        torrent.phase = TorrentPhase::PausedDownload;
        torrent.last_error = Some("storage full".into());
        torrent.stop_reason = Some(nzbd_types::TorrentStopReason::StorageFull);
        owner.state.jobs.push(job);
        owner.release_torrents_after_storage_recovery();
        let torrent = owner.state.jobs[0].torrent.as_ref().unwrap();
        assert_eq!(torrent.phase, TorrentPhase::Queued);
        assert_eq!(torrent.stop_reason, None);
        assert_eq!(torrent.last_error, None);
    }

    #[tokio::test]
    async fn seed_policy_save_failure_rolls_back_without_stopping_the_torrent() {
        let (_tmp, mut owner, mut adapter) =
            control_test_owner_with_persistence(true, Some(Arc::new(|| false)));
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
        let (tmp, mut owner, mut adapter) = control_test_owner();
        owner.state.jobs.push(control_torrent_job());

        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::Pause {
            job: JobId(1),
            reply,
        });
        assert!(rx.await.unwrap());
        assert_eq!(
            adapter.next_command().await,
            Some(BackendCommand::Pause { job: JobId(1) })
        );

        let persisted = owner.snap_store.load().unwrap().unwrap();
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
        assert!(rx.await.unwrap());
        owner.schedule_torrent_starts();
        assert_eq!(
            adapter.next_command().await,
            Some(BackendCommand::Start { job: JobId(1) })
        );
        assert!(tmp.path().join("state/queue.json").exists());
    }

    #[tokio::test]
    async fn failed_persistence_never_emits_a_torrent_control() {
        let (_tmp, mut owner, mut adapter) =
            control_test_owner_with_persistence(true, Some(Arc::new(|| false)));
        owner.state.jobs.push(control_torrent_job());

        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::Pause {
            job: JobId(1),
            reply,
        });

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
        let (_tmp, mut owner, mut adapter) = control_test_owner();
        owner.state.jobs.push(control_torrent_job());

        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::Pause {
            job: JobId(1),
            reply,
        });
        assert!(rx.await.unwrap());
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::SetPriority {
            job: JobId(1),
            priority: 77,
            reply,
        });
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
        let (_tmp, mut owner, mut adapter) = control_test_owner();
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
        assert!(rx.await.unwrap());
        owner.schedule_torrent_starts();
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
        let (_tmp, mut owner, mut adapter) = control_test_owner_with_persistence(false, None);
        owner.state.jobs.push(control_torrent_job());

        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::Pause {
            job: JobId(1),
            reply,
        });

        assert!(rx.await.unwrap());
        assert_eq!(
            adapter.next_command().await,
            Some(BackendCommand::Pause { job: JobId(1) })
        );
    }

    #[tokio::test]
    async fn retained_backend_commands_are_bounded_before_persisting_new_intent() {
        let (_tmp, mut owner, _adapter) = control_test_owner();
        owner.state.jobs.push(control_torrent_job());

        for priority in 1..=MAX_PENDING_BACKEND_COMMANDS + 1 {
            let (reply, rx) = oneshot::channel();
            owner.on_command(QueueCommand::SetPriority {
                job: JobId(1),
                priority: priority as i32,
                reply,
            });
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

    #[test]
    fn latest_backend_progress_is_folded_without_a_fifo_backlog() {
        let (_tmp, mut owner, adapter) = control_test_owner();
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

    #[tokio::test]
    async fn url_download_allocates_fresh_directory_beside_retained_payload() {
        let (_tmp, mut owner, _adapter) = control_test_owner();
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
        let saved = owner.snap_store.load().unwrap().unwrap();
        assert_eq!(
            saved.jobs.iter().find(|j| j.id == id).unwrap().dir_name,
            expected
        );
    }

    #[tokio::test]
    async fn allocation_refusal_holds_only_that_job_and_release_retries_it() {
        let (_tmp, mut owner, _adapter) = control_test_owner();
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
        let leases = owner.grant_work(ServerId(1), 1);
        assert_eq!(leases.len(), 1, "a hold must not consume the lease budget");
        assert_eq!(leases[0].r.job, JobId(2));
        assert_eq!(owner.state.jobs[0].control().unwrap().cause, "allocation");
        assert_eq!(owner.grant_work(ServerId(1), 1)[0].r.job, JobId(3));
        let revision = owner.state.jobs[0].control().unwrap().revision;
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::ReleaseResourceHold {
            job: JobId(1),
            revision: revision.clone(),
            reply,
        });
        assert!(rx.await.unwrap());
        assert!(owner.grant_work(ServerId(1), 1).is_empty());
        let held = owner.state.jobs[0].control().unwrap();
        assert_eq!(held.lifecycle, "held");
        assert!(held.revision.parse::<u64>().unwrap() > revision.parse::<u64>().unwrap());
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::ReleaseResourceHold {
            job: JobId(1),
            revision,
            reply,
        });
        assert!(!rx.await.unwrap(), "stale release must be rejected");
        std::fs::remove_dir(&conflict).unwrap();
        let mut epoch = owner.epoch_tx.subscribe();
        epoch.borrow_and_update();
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::ReleaseResourceHold {
            job: JobId(1),
            revision: owner.state.jobs[0].control().unwrap().revision,
            reply,
        });
        assert!(rx.await.unwrap());
        assert!(epoch.has_changed().unwrap());
        assert_eq!(owner.grant_work(ServerId(1), 1)[0].r.job, JobId(1));
    }

    #[tokio::test]
    async fn post_hold_retry_checks_revision_custody_and_stage_then_persists() {
        let (tmp, mut owner, _adapter) = control_test_owner();
        let mut job = bare_job();
        job.dir_name = "pp".into();
        owner.state.jobs.push(job);
        owner.ensure_allocation(JobId(1)).unwrap();
        assert!(owner.hold_job(JobId(1), "post_failure", "unpack", "tool failed"));
        let revision = owner.state.jobs[0].control().unwrap().revision;
        for expected_revision in ["stale".to_string(), revision.clone()] {
            let (reply, rx) = oneshot::channel();
            owner.on_command(QueueCommand::RetryPostHold {
                job: JobId(1),
                revision: expected_revision.clone(),
                reply,
            });
            assert_eq!(rx.await.unwrap(), expected_revision == revision);
        }
        assert_eq!(owner.state.jobs[0].status, JobStatus::PostQueued);
        let persisted = SnapshotStore::open(&tmp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        let restored = QueueState::from_doc(persisted);
        assert!(!restored.jobs[0].held());
        assert!(owner.hold_job(JobId(1), "post_failure", "script", "uncertain script"));
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::RetryPostHold {
            job: JobId(1),
            revision: owner.state.jobs[0].control().unwrap().revision,
            reply,
        });
        assert!(!rx.await.unwrap());
        assert!(owner.state.jobs[0].held());
    }

    #[tokio::test]
    async fn writer_retirement_does_not_create_an_allocation_review_hold() {
        let (_tmp, mut owner, _adapter) = control_test_owner();
        owner.state.jobs.push(pending_job(1));
        owner.retiring_writers.insert(JobId(1), Vec::new());
        assert!(owner.ensure_allocation(JobId(1)).is_err());
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

    #[test]
    fn writer_resource_errors_hold_pending_segments_and_latch_out_of_space() {
        let (_tmp, mut owner, _epoch) = guard_test_owner(Tuning::default());
        owner.state.jobs.push(pending_job(2));

        owner.on_msg(EngineMsg::WriterError {
            job: JobId(2),
            file: FileId(20),
            error: "write payload.bin: No space left on device (os error 28)".into(),
        });
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
        owner.on_msg(EngineMsg::WriterError {
            job: JobId(2),
            file: FileId(999),
            error: "gone".into(),
        });
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
        owner.on_msg(EngineMsg::WriterFinalized {
            job: JobId(999),
            file: FileId(999),
            ok: true,
            final_path: None,
            combined_crc: None,
        });

        owner.disk_low = false;
        let (reply, received) = oneshot::channel();
        owner.on_msg(EngineMsg::WorkRequest {
            server: ServerId(999),
            max: 1,
            reply,
        });
        assert!(received.blocking_recv().unwrap().is_empty());
    }

    #[test]
    fn failed_fenced_replace_restores_delegation_and_mirror_overlay() {
        let (_tmp, mut owner, _epoch) = guard_test_owner(Tuning::default());
        let original = pending_job(3);
        owner.state.jobs.push(original.clone());
        owner.delegated.insert(JobId(3), "worker-a".into());
        owner.mirror.insert(
            JobId(3),
            MirrorStats {
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
            reply,
        });

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

    #[test]
    fn legacy_remote_pp_remaining_fallback_stays_paused_aware() {
        let (_tmp, mut owner, _epoch) = guard_test_owner(Tuning::default());
        let mut job = pending_job(4);
        job.status = JobStatus::Completed;
        job.files[0].paused = true;
        recompute_job_totals(&mut job);
        owner.state.jobs.push(job);
        owner.delegated.insert(JobId(4), "old-worker".into());
        owner.mirror.insert(
            JobId(4),
            MirrorStats {
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
    #[test]
    fn delayed_par_download_keeps_the_open_post_span() {
        let (_tmp, mut owner, _epoch) = guard_test_owner(Tuning::default());
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

    #[test]
    fn restarted_delayed_par_completion_recovers_from_the_persisted_open_span() {
        let (_tmp, mut owner, _epoch) = guard_test_owner(Tuning::default());
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

    #[test]
    fn restarted_pp_only_node_reauthorizes_only_the_pending_par_file() {
        let (_tmp, mut owner, _epoch) = guard_test_owner(Tuning::default());
        owner.download_enabled = false;
        let mut job = restarted_recovery_job(406, SegmentState::Pending);
        let recovery_file = job.files[0].id;
        let mut unrelated = job.files[0].clone();
        unrelated.id = FileId(4061);
        unrelated.filename = "release.part001.rar".into();
        unrelated.is_par2 = false;
        job.files.push(unrelated);
        owner.import_job(job, false, false);

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

    #[test]
    fn failed_recovery_articles_return_to_verification_instead_of_disposition() {
        let (_tmp, mut owner, _epoch) = guard_test_owner(Tuning::default());
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

    #[test]
    fn recovery_write_failure_stays_owned_by_the_active_post_task() {
        let (_tmp, mut owner, _epoch) = guard_test_owner(Tuning::default());
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

    #[test]
    fn clearing_disk_guard_wakes_parked_connection_tasks() {
        let tuning = Tuning {
            min_free_disk_bytes: 100,
            ..Tuning::default()
        };
        let (_tmp, mut owner, mut epoch) = guard_test_owner(tuning);
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

    #[test]
    fn clearing_quota_guard_wakes_parked_connection_tasks() {
        let tuning = Tuning {
            daily_quota_bytes: 1,
            ..Tuning::default()
        };
        let (_tmp, mut owner, mut epoch) = guard_test_owner(tuning);
        owner.quota_reached = true;
        let _ = epoch.borrow_and_update();
        assert!(!epoch.has_changed().unwrap());

        // The empty volume book is deterministically below the configured
        // quota, directly exercising the rollover/recovery transition.
        owner.update_quota_guard();

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
