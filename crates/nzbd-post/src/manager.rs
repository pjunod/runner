//! The post-processing orchestrator + manager (ARCHITECTURE.md §9).
//!
//! The manager watches the engine for finished downloads and drives each
//! job through the stage graph: par verify (native quick path first) →
//! repair (with delayed-par fetching) → unpack → cleanup → scripts —
//! recording the outcome in history and stamping the job so restarts never
//! re-process. Stage parallelism follows `PostStrategy`.

use crate::rename::{naming_candidates, par_rename_owned_with_candidates, rar_rename_owned};
use crate::script::{discover, ScriptHost};
use crate::tools::{archive_input_set, detect_archives, Extractors, Par2Tool};
use crate::{par2, DownloadEvidence, PostError, RepairResult, VerifyResult};
use nzbd_engine::{EngineHandle, Event};
use nzbd_state::artifacts::{Artifact, DeliveryFileOutcome, FileEntry};
use nzbd_state::history::HistoryDb;

use nzbd_types::metrics::PpStageStats;
use nzbd_types::{Health, HoldCause, Job, JobId, JobStatus, PostStage};
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Param stamped onto a job once post-processing finished (defined in
/// `nzbd-types` so the engine snapshot and cluster scheduler see it too).
pub use nzbd_types::PP_DONE_PARAM;

/// What to do with the FILES of a job that ended in a terminal failure —
/// par failure, unpack failure, script failure, health abort, post crash.
///
/// Was `HealthAction` and governed the health gate alone, which left
/// every *other* terminal failure's directory sitting in the category
/// tree an importer watches, forever. Nothing ever removed them: ~523 GB
/// of known-bad downloads in one field report
/// (docs/archive/REGRAB_LOOP_PLAN.md D2).
///
/// `Delete` is the default. The bytes are known-bad by definition, and
/// the job's NZB is parked with its history entry, so `requeue` fetches
/// them again — deleting loses nothing but the disk space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FailureAction {
    /// Leave the files where they are. What produced the terabyte.
    None,
    Park,
    #[default]
    Delete,
}

impl FailureAction {
    pub fn parse(s: &str) -> FailureAction {
        match s.to_ascii_lowercase().as_str() {
            "none" | "keep" | "" => FailureAction::None,
            "park" | "pause" => FailureAction::Park,
            _ => FailureAction::Delete,
        }
    }
}

/// What a `[[category]]` block actually changes about post-processing.
///
/// These keys were parsed and advertised to compat clients as
/// `CategoryN.DestDir` / `CategoryN.Unpack` long before anything applied
/// them, so an operator who set a per-category destination got files
/// somewhere else and an *arr that path-mapped off the advertised value
/// found nothing. Advertised must equal actual; that is what this type
/// carries into the pipeline.
#[derive(Debug, Clone, Default)]
pub struct CategoryRule {
    pub name: String,
    /// Completed files are moved here (under `<dest_dir>/<job name>`)
    /// instead of the global destination.
    pub dest_dir: Option<PathBuf>,
    /// Per-category override of `[post] unpack`.
    pub unpack: Option<bool>,
    /// Extension scripts to run for this category, by file name or stem.
    /// Empty = every discovered script, which is the global behavior.
    pub extensions: Vec<String>,
}

#[derive(Clone)]
pub struct PostConfig {
    /// Final publication root; None keeps the input root (private cluster attempts).
    pub completed_dir: Option<PathBuf>,
    pub par2_cmd: String,
    pub unrar_cmd: String,
    pub sevenzip_cmd: String,
    pub scripts_dir: Option<PathBuf>,
    pub unpack: bool,
    pub cleanup: bool,
    /// Rename still-obfuscated files to the job name after unpack
    /// Exact mappings win; multi-file episode order is never guessed.
    pub deobfuscate_final: bool,
    /// What happens to the files of a job that ends in a terminal
    /// failure (par, unpack, script, health abort, crash).
    pub failure_action: FailureAction,
    /// Where `FailureAction::Park` moves a failed job's directory.
    /// `None` falls back to a `.failed` dir beside the downloads, which
    /// is at least out of an importer's way (dot-prefixed).
    pub failed_dir: Option<PathBuf>,
    /// Concurrent PP jobs (PostStrategy: sequential=1, balanced=2,
    /// aggressive=3, rocket=6).
    pub slots: usize,
    pub tool_timeout: Duration,
    pub script_timeout: Duration,
    /// How long to wait for delayed par files to download.
    pub par_fetch_timeout: Duration,
    /// `[[category]]` rules, matched against the job's category name.
    pub categories: Vec<CategoryRule>,
    /// Stage-duration counters for `/metrics`, when the daemon wired one.
    pub stats: Option<Arc<PpStageStats>>,
}

impl Default for PostConfig {
    fn default() -> Self {
        PostConfig {
            completed_dir: None,
            par2_cmd: "par2".into(),
            unrar_cmd: "unrar".into(),
            sevenzip_cmd: "7z".into(),
            scripts_dir: None,
            unpack: true,
            cleanup: true,
            deobfuscate_final: true,
            failure_action: FailureAction::default(),
            failed_dir: None,
            slots: 1,
            tool_timeout: Duration::from_secs(3600),
            script_timeout: Duration::from_secs(3600),
            par_fetch_timeout: Duration::from_secs(600),
            categories: Vec::new(),
            stats: None,
        }
    }
}

impl PostConfig {
    /// The `[[category]]` rule for a job's category, if one is configured.
    /// Matched case-insensitively, like the compat shim's category
    /// handling — an *arr sending "TV" must land on `name = "tv"`.
    fn rule_for(&self, category: Option<&str>) -> Option<&CategoryRule> {
        let want = category?.trim();
        if want.is_empty() {
            return None;
        }
        self.categories
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(want))
    }
}

pub fn strategy_slots(name: &str) -> usize {
    match name {
        "balanced" => 2,
        "aggressive" => 3,
        "rocket" => 6,
        _ => 1,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PpFinal {
    Success,
    ParFailure,
    UnpackFailure,
    ScriptFailure,
    HealthFailure,
    FetchFailure,
}

impl PpFinal {
    pub fn as_str(&self) -> &'static str {
        match self {
            PpFinal::Success => "SUCCESS",
            PpFinal::ParFailure => "PAR_FAILURE",
            PpFinal::UnpackFailure => "UNPACK_FAILURE",
            PpFinal::ScriptFailure => "SCRIPT_FAILURE",
            PpFinal::HealthFailure => "FAILURE/HEALTH",
            PpFinal::FetchFailure => "FAILURE/FETCH",
        }
    }
}

/// Per-job claim gate. `None` = process everything (single node). In
/// cluster mode the closure consults election state + the PP assignment
/// the leader scheduler made (C2 anti-affinity): a node only processes
/// jobs assigned to it, and only the leader records health-failures.
pub type PpGate = Option<Arc<dyn Fn(JobId) -> bool + Send + Sync>>;

#[derive(Clone, Debug)]
pub enum ScriptReceiptAction {
    Begin { script: String },
    Finish { script: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScriptReceiptDecision {
    Run,
    AlreadyDone,
    Ambiguous,
}

pub type ScriptReceiptHook = Arc<
    dyn Fn(
            ScriptReceiptAction,
        ) -> Pin<
            Box<dyn std::future::Future<Output = Result<ScriptReceiptDecision, String>> + Send>,
        > + Send
        + Sync,
>;

/// Fencing context for one PP execution (CLUSTERING.md §6.4): `tag` names
/// the staging dir (`.pp.<tag>/`) unpack extracts into, and `commit_ok`
/// is re-checked immediately before every commit rename and before the
/// final stamp — a lease that was cancelled or reclaimed must not publish.
#[derive(Clone)]
pub struct PpCtx {
    #[cfg(test)]
    pub(crate) repair_observer: Option<crate::attempt::RepairObserver>,
    pub cancel: CancellationToken,
    pub workers: TaskTracker,
    pub tag: String,
    pub commit_ok: Arc<dyn Fn() -> bool + Send + Sync>,
    /// Remote cluster attempts publish history only after the replicated
    /// result receipt is accepted by the authority.
    pub publish_history: bool,
    pub script_receipt: Option<ScriptReceiptHook>,
    /// Cluster execution identity exposed to operator scripts.
    pub extra_env: Vec<(String, String)>,
    /// Prefix used to derive the exact per-script durable receipt id.
    pub script_receipt_prefix: Option<String>,
}

impl Default for PpCtx {
    fn default() -> Self {
        PpCtx {
            #[cfg(test)]
            repair_observer: None,
            cancel: CancellationToken::new(),
            workers: TaskTracker::new(),
            tag: "local".into(),
            commit_ok: Arc::new(|| true),
            publish_history: true,
            script_receipt: None,
            extra_env: Vec::new(),
            script_receipt_prefix: None,
        }
    }
}

/// How often the manager rescans the queue for un-processed finished jobs
/// (covers leadership takeover, lagged event streams, crashed PP attempts
/// on a prior run — the `*PP:done` stamp keeps it idempotent).
const RESCAN_INTERVAL: Duration = Duration::from_secs(30);
const PP_SELECTED_OUTCOME_PARAM: &str = "*PP:selected-publication-outcome";
const PP_SELECTED_FAILURE_CAUSE_PARAM: &str = "*PP:selected-publication-failure-cause";

/// A safe point to resume a post-processing pipeline from.
///
/// These are deliberately coarser than [`PostStage`]. Verification owns
/// repair, extraction owns its repair-and-retry loop, and cleanup owns the
/// final rename pass; starting in the middle of one of those coupled pieces
/// would manufacture state the rest of the pipeline cannot prove.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartPoint {
    Beginning,
    Verify,
    Unpack,
    Cleanup,
    Move,
    Scripts,
}

impl RestartPoint {
    pub fn as_str(self) -> &'static str {
        match self {
            RestartPoint::Beginning => "beginning",
            RestartPoint::Verify => "verify",
            RestartPoint::Unpack => "unpack",
            RestartPoint::Cleanup => "cleanup",
            RestartPoint::Move => "move",
            RestartPoint::Scripts => "scripts",
        }
    }

    fn includes(self, point: RestartPoint) -> bool {
        let order = |p| match p {
            RestartPoint::Beginning => 0,
            RestartPoint::Verify => 1,
            RestartPoint::Unpack => 2,
            RestartPoint::Cleanup => 3,
            RestartPoint::Move => 4,
            RestartPoint::Scripts => 5,
        };
        order(self) <= order(point)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RestartPostError {
    #[error("no such job")]
    NotFound,
    #[error("post-processing already finished for this job")]
    AlreadyFinished,
    #[error("the job is not ready for post-processing")]
    NotReady,
    #[error("cannot skip past the job's current post-processing stage")]
    UnsafePoint,
    #[error("post-processing for this job is owned by another node")]
    NotOwned,
    #[error("the post-processing manager is shutting down")]
    Closed,
}

enum ManagerCommand {
    Restart {
        job: JobId,
        from: RestartPoint,
        reply: tokio::sync::oneshot::Sender<Result<(), RestartPostError>>,
    },
    RetryNow {
        job: JobId,
        reply: tokio::sync::oneshot::Sender<Result<(), RestartPostError>>,
    },
}

/// Reliable control path from the API to the post manager.
///
/// Engine events remain the observation stream; recovery commands use a
/// bounded mailbox and a reply so a button click cannot be silently lost to
/// broadcast lag.
#[derive(Clone)]
pub struct PostManagerHandle {
    tx: tokio::sync::mpsc::Sender<ManagerCommand>,
}

impl PostManagerHandle {
    pub async fn restart(&self, job: JobId, from: RestartPoint) -> Result<(), RestartPostError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(ManagerCommand::Restart {
                job,
                from,
                reply: tx,
            })
            .await
            .map_err(|_| RestartPostError::Closed)?;
        rx.await.map_err(|_| RestartPostError::Closed)?
    }

    pub async fn retry_now(&self, job: JobId) -> Result<(), RestartPostError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(ManagerCommand::RetryNow { job, reply: tx })
            .await
            .map_err(|_| RestartPostError::Closed)?;
        rx.await.map_err(|_| RestartPostError::Closed)?
    }
}

struct ActiveAttempt {
    cancel: CancellationToken,
}

fn restart_limit(status: JobStatus) -> Option<RestartPoint> {
    match status {
        JobStatus::Completed => Some(RestartPoint::Beginning),
        JobStatus::PostQueued => Some(RestartPoint::Beginning),
        JobStatus::Post { stage } => Some(match stage {
            PostStage::ParRename | PostStage::RarRename => RestartPoint::Beginning,
            PostStage::ParVerify | PostStage::ParRepair => RestartPoint::Verify,
            PostStage::Unpack => RestartPoint::Unpack,
            PostStage::Cleanup | PostStage::PostUnpackRename => RestartPoint::Cleanup,
            PostStage::Move => RestartPoint::Move,
            PostStage::Script | PostStage::PublicationVerify => RestartPoint::Scripts,
        }),
        _ => None,
    }
}

pub fn spawn_post_manager(
    engine: EngineHandle,
    cfg: PostConfig,
    history: Arc<HistoryDb>,
    dest_dir: PathBuf,
    gate: PpGate,
    cancel: CancellationToken,
    tracker: &TaskTracker,
) -> PostManagerHandle {
    let (control_tx, control_rx) = tokio::sync::mpsc::channel(32);
    let t2 = tracker.clone();
    tracker.spawn(manager_task(
        engine, cfg, history, dest_dir, gate, cancel, t2, control_rx,
    ));
    PostManagerHandle { tx: control_tx }
}

#[allow(clippy::too_many_arguments)]
// The async retry-control branch deliberately holds a single-flight map slot
// across awaited owner commands; using Entry would borrow the map across await.
#[allow(clippy::map_entry)]
async fn manager_task(
    engine: EngineHandle,
    cfg: PostConfig,
    history: Arc<HistoryDb>,
    dest_dir: PathBuf,
    gate: PpGate,
    cancel: CancellationToken,
    tracker: TaskTracker,
    mut control: tokio::sync::mpsc::Receiver<ManagerCommand>,
) {
    let mut rx = engine.subscribe();
    let mut active: HashMap<JobId, ActiveAttempt> = HashMap::new();
    let mut pending_restarts: HashMap<JobId, RestartPoint> = HashMap::new();
    let (finished_tx, mut finished_rx) = tokio::sync::mpsc::unbounded_channel();
    let sem = Arc::new(tokio::sync::Semaphore::new(cfg.slots.max(1)));
    let claim = |g: &PpGate, job: JobId| g.as_ref().map(|f| f(job)).unwrap_or(true);

    // Startup scan + periodic rescan: finished downloads that were never
    // post-processed (crash between completion and PP, or authority adopted
    // from a dead leader mid-way).
    let mut rescan = tokio::time::interval(RESCAN_INTERVAL);
    rescan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Review #283-1: the owner defers export/admission while a job is
    // fenced (boot verification, journal loading); a scan that met such a
    // deferral runs again on the owner's next queue change.
    let mut queue_changes = engine.queue_changes();
    let mut deferred = false;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = rescan.tick() => {
                queue_changes.borrow_and_update();
                deferred = scan_queue(
                    &tracker, &engine, &cfg, &history, &dest_dir, &gate, &sem,
                    &cancel, &finished_tx, &mut active,
                ).await;
            }
            changed = queue_changes.changed(), if deferred => {
                if changed.is_err() {
                    break;
                }
                queue_changes.borrow_and_update();
                deferred = scan_queue(
                    &tracker, &engine, &cfg, &history, &dest_dir, &gate, &sem,
                    &cancel, &finished_tx, &mut active,
                ).await;
            }
            ev = rx.recv() => match ev {
                Ok(Event::JobFinished { job, status, .. }) => match status {
                    JobStatus::Completed if claim(&gate, job) => {
                        if let Ok(Some(exported)) = engine.export_job(job).await {
                            if !pp_done(&exported) && !active.contains_key(&job) {
                                let attempt = spawn_job(
                                    &tracker,
                                    &engine,
                                    &cfg,
                                    &history,
                                    &dest_dir,
                                    &sem,
                                    &cancel,
                                    &finished_tx,
                                    job,
                                    gate.clone(),
                                    gate.is_none(),
                                    RestartPoint::Beginning,
                                );
                                active.insert(job, attempt);
                            }
                        }
                    }
                    // A failed download's terminal is recorded by the owner
                    // itself (contract §2.1) before this advisory event; its
                    // salvage and disposal start from `job_pp_finished`
                    // below. A failed row with no stamp was failed by an
                    // earlier build (or imported): record its terminal.
                    JobStatus::Failed if claim(&gate, job) => {
                        if let Ok(Some(row)) = engine.export_job(job).await {
                            if !pp_done(&row) && !active.contains_key(&job) {
                                let (reason, fetch_cause) = unstamped_failure(&row);
                                let _ = fail_job_in(
                                    &engine, &history, job, reason, fetch_cause,
                                    reason.as_str(), "failed before its terminal was recorded",
                                ).await;
                            }
                        }
                    }
                    _ => {}
                },
                Ok(Event::JobPpFinished { job, outcome: nzbd_types::TerminalOutcome::Failed, .. })
                    if claim(&gate, job) =>
                {
                    if failure_followup(
                        &engine, &cfg, &dest_dir, &gate, job, true,
                    )
                    .await
                        && gate.is_none()
                        && !active.contains_key(&job)
                    {
                        // Salvage settled and the source is disposed: the
                        // row's history is final, retire it now.
                        if reconcile_stamped_completion(&engine, &history, job).await.is_ok() {
                            let _ = engine.remove_job_silent(job).await;
                        }
                    }
                }
                // Review #283-1: a `completed` terminal emitted by the
                // owner's replay (a restart after the history write) owes
                // its delivery and retirement; the stamped follow-up runs
                // on this scan, not at the next periodic one.
                Ok(Event::JobPpFinished { outcome: nzbd_types::TerminalOutcome::Completed, .. }) => {
                    rescan.reset_immediately();
                }
                Ok(Event::JobDeleted { job }) => {
                    pending_restarts.remove(&job);
                    if let Some(attempt) = active.get(&job) {
                        attempt.cancel.cancel();
                    }
                }
                // Review #283-1: a job the manager is not processing became
                // eligible for post-processing — typically boot verification
                // returning an interrupted job to `Post`/`PostQueued` after
                // the manager's first scan saw it `Queued`. Scan now rather
                // than at the next periodic rescan.
                Ok(Event::JobStatusChanged { job, status, .. })
                    if !active.contains_key(&job)
                        && matches!(status.as_str(), "post_queued" | "post_processing" | "completed") =>
                {
                    rescan.reset_immediately();
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            },
            Some(job) = finished_rx.recv() => {
                // Releasing this claim is the automatic-recovery fix: a
                // crashed tool attempt remains eligible for the next rescan
                // instead of being remembered as active forever.
                active.remove(&job);
                // A post-processing failure's terminal went out while the
                // attempt still held admission; its salvage and disposal run
                // now that the attempt has released it, and the row retires
                // once nothing is left to do.
                let terminal = load_terminal(&engine, job).await;
                // Review #283-1: an attempt that ended while the owner
                // replayed the job's `completed` terminal (a restart after
                // the history write) leaves its delivery and retirement to
                // the stamped follow-up; run that now, not at the next
                // periodic rescan. Status events met while the attempt held
                // the job were skipped above.
                if terminal.as_ref().is_some_and(|terminal| {
                    terminal.outcome == nzbd_types::TerminalOutcome::Completed && terminal.emitted
                }) {
                    rescan.reset_immediately();
                }
                if let Some(terminal) = terminal.filter(|terminal| {
                    terminal.outcome == nzbd_types::TerminalOutcome::Failed && terminal.emitted
                }) {
                    let settled = terminal.disposal.is_some()
                        || (claim(&gate, job)
                            && failure_followup(
                                &engine, &cfg, &dest_dir, &gate, job, true,
                            )
                            .await);
                    if settled
                        && gate.is_none()
                        && reconcile_stamped_completion(&engine, &history, job).await.is_ok()
                    {
                        let _ = engine.remove_job_silent(job).await;
                    }
                }
                if let Some(from) = pending_restarts.remove(&job) {
                    let restartable = engine.snapshot().jobs.iter().any(|j| {
                        j.id == job
                            && !j.pp_done
                            && matches!(
                                j.status,
                                JobStatus::Completed | JobStatus::PostQueued | JobStatus::Post { .. }
                            )
                    });
                    if restartable && claim(&gate, job) {
                        let attempt = spawn_job(
                            &tracker, &engine, &cfg, &history, &dest_dir, &sem,
                            &cancel, &finished_tx, job, gate.clone(), gate.is_none(), from,
                        );
                        active.insert(job, attempt);
                    }
                }
            }
            Some(cmd) = control.recv() => match cmd {
                ManagerCommand::RetryNow { job, reply } => {
                    let result = match engine.export_job(job).await {
                        Err(_) => Err(RestartPostError::Closed),
                        Ok(None) => Err(RestartPostError::NotFound),
                        Ok(Some(row)) if !row.lifecycle_eligible() => {
                            Err(RestartPostError::NotOwned)
                        }
                        // A failed job's terminal is final; Retry now runs its
                        // owed salvage/disposal immediately.
                        Ok(Some(row)) if row.status == JobStatus::Failed => {
                            if failure_followup(
                                &engine, &cfg, &dest_dir, &gate, job, true,
                            ).await {
                                Ok(())
                            } else {
                                Err(RestartPostError::NotReady)
                            }
                        }
                        Ok(Some(row)) => {
                            // Retry is an operator request: every hold answers it.
                            let control = row.control().filter(|control| control.lifecycle == "held");
                            if active.contains_key(&job) {
                                Err(RestartPostError::UnsafePoint)
                            } else if let Some(control) = control {
                                if !claim(&gate, job) {
                                    Err(RestartPostError::NotOwned)
                                } else if !matches!(engine.release_hold(job, control.revision.clone(), nzbd_engine::RetryBy::Operator).await, Ok(Ok(()))) {
                                    Err(RestartPostError::NotReady)
                                } else if !control.post_side() {
                                    // A download-side hold went back to the
                                    // download queue; nothing to spawn here.
                                    Ok(())
                                } else {
                                    let ready = engine.snapshot().jobs.iter().any(|summary| {
                                        summary.id == job
                                            && !summary.pp_done
                                            && matches!(summary.status, JobStatus::Completed | JobStatus::PostQueued | JobStatus::Post { .. })
                                    });
                                    if ready {
                                        active.insert(job, spawn_job(
                                            &tracker, &engine, &cfg, &history, &dest_dir, &sem,
                                            &cancel, &finished_tx, job, gate.clone(), gate.is_none(), RestartPoint::Beginning,
                                        ));
                                        Ok(())
                                    } else {
                                        Err(RestartPostError::NotReady)
                                    }
                                }
                            } else {
                                Err(RestartPostError::NotReady)
                            }
                        }
                    };
                    let _ = reply.send(result);
                }
                ManagerCommand::Restart { job, from, reply } => {
                    let held = engine.export_job(job).await.ok().flatten()
                        .and_then(|j| j.control()).filter(|c| c.lifecycle == "held");
                    if let Some(control) = held {
                        // Held exceptions retry from the beginning, never past
                        // uncertain work or scripts. A live attempt must finish
                        // before its persisted hold can be released.
                        if from != RestartPoint::Beginning || active.contains_key(&job) {
                            let _ = reply.send(Err(RestartPostError::UnsafePoint));
                            continue;
                        }
                        if !claim(&gate, job) {
                            let _ = reply.send(Err(RestartPostError::NotOwned));
                            continue;
                        }
                        // A post-processing restart cannot release a hold
                        // that stopped the download; `resume` is its exit.
                        if !control.post_side() {
                            let _ = reply.send(Err(RestartPostError::NotReady));
                            continue;
                        }
                        if !engine.retry_post_hold(job, control.revision).await.unwrap_or(false) {
                            let _ = reply.send(Err(RestartPostError::NotReady));
                            continue;
                        }
                    }
                    let result = match engine.snapshot().jobs.iter().find(|j| j.id == job) {
                        None => Err(RestartPostError::NotFound),
                        Some(j) if j.pp_done => Err(RestartPostError::AlreadyFinished),
                        Some(j) if !matches!(
                            j.status,
                            JobStatus::Completed | JobStatus::PostQueued | JobStatus::Post { .. }
                        ) => Err(RestartPostError::NotReady),
                        Some(_) if !claim(&gate, job) => Err(RestartPostError::NotOwned),
                        Some(j) if !from.includes(restart_limit(j.status).unwrap()) => {
                            Err(RestartPostError::UnsafePoint)
                        }
                        Some(_) => {
                            tracing::warn!(
                                job = job.0,
                                from = from.as_str(),
                                "operator requested a post-processing restart"
                            );
                            if let Some(attempt) = active.get(&job) {
                                // Latest click wins while the old subprocess
                                // is being killed and its stage span closes.
                                pending_restarts.insert(job, from);
                                attempt.cancel.cancel();
                            } else {
                                let attempt = spawn_job(
                                    &tracker, &engine, &cfg, &history, &dest_dir, &sem,
                                    &cancel, &finished_tx, job, gate.clone(), gate.is_none(), from,
                                );
                                active.insert(job, attempt);
                            }
                            Ok(())
                        }
                    };
                    let _ = reply.send(result);
                }
            },
        }
    }

    // Per-job tokens stop attempts before the tracker is awaited. The global
    // token normally does this too; being explicit keeps shutdown behavior
    // correct if the manager's own select loop is the first task to observe it.
    for attempt in active.values() {
        attempt.cancel.cancel();
    }
}

/// Contract §2.2/§2.6: what happens to a failed job's bytes after its
/// terminal is out — salvage first, then disposal — driven from the job's
/// `job_terminals` row, never from queue status. Returns true once nothing
/// is left to do (the row may then retire).
#[allow(clippy::too_many_arguments)]
/// Where a terminal's follow-up stands after one pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Settle {
    /// Nothing is left to do.
    Done,
    /// Still owed; its own ladder (or the next event) brings it back.
    Pending,
    /// The owner deferred its answer (boot verification, journal loading, a
    /// transition in flight): retry on the next queue change.
    Deferred,
}

impl From<bool> for Settle {
    fn from(done: bool) -> Self {
        if done {
            Settle::Done
        } else {
            Settle::Pending
        }
    }
}

async fn failure_followup(
    engine: &EngineHandle,
    cfg: &PostConfig,
    dest_dir: &Path,
    gate: &PpGate,
    job: JobId,
    force: bool,
) -> bool {
    failure_followup_settle(engine, cfg, dest_dir, gate, job, force).await == Settle::Done
}

async fn failure_followup_settle(
    engine: &EngineHandle,
    cfg: &PostConfig,
    dest_dir: &Path,
    gate: &PpGate,
    job: JobId,
    force: bool,
) -> Settle {
    if gate.as_ref().is_some_and(|claim| !claim(job)) {
        return Settle::Pending;
    }
    let Some(terminal) = load_terminal(engine, job).await else {
        return Settle::Pending;
    };
    if terminal.outcome != nzbd_types::TerminalOutcome::Failed || !terminal.emitted {
        return Settle::from(
            terminal.outcome != nzbd_types::TerminalOutcome::Failed && terminal.emitted,
        );
    }
    if terminal.disposal.is_some() {
        return Settle::Done;
    }
    // Review #280-7: the retry ladder is the disposal operation's own
    // (durable, visible on the artifact); nothing here keeps a second one.
    if !force {
        let inventory = engine.artifacts();
        let retry_at = tokio::task::spawn_blocking(move || inventory.disposal_retry_at(job.0))
            .await
            .ok()
            .and_then(Result::ok)
            .flatten();
        if retry_at.is_some_and(|at| at > now()) {
            return Settle::Pending;
        }
    }
    let row = match engine.export_job(job).await {
        Ok(row) => row,
        // A busy owner (a lifecycle transition in flight) is not a removed
        // job: retry later rather than settle the obligation as gone.
        Err(error) => {
            tracing::debug!(job = job.0, %error, "failed-job follow-up waits for the owner");
            return if error.is_deferred() {
                Settle::Deferred
            } else {
                Settle::Pending
            };
        }
    };
    let Some(row) = row else {
        // The queue row is gone (an operator delete, which owns its bytes):
        // nothing remains for runner to salvage or dispose.
        let inventory = engine.artifacts();
        let _ = tokio::task::spawn_blocking(move || {
            inventory.settle_salvage(
                job.0,
                &nzbd_state::artifacts::SalvageOutcome::None {
                    error: Some("the job left the queue before salvage".into()),
                },
            )?;
            inventory.record_disposal(job.0, "job removed from the queue")
        })
        .await;
        return Settle::Done;
    };
    let admission = match engine.begin_post_admission(job).await {
        Ok(Some(admission)) => admission,
        // Refused for owner state (verification, a transition in flight):
        // answerable once the queue changes.
        Ok(None) => return Settle::Deferred,
        Err(error) => {
            tracing::warn!(job = job.0, %error, "failed-job follow-up awaits owner admission");
            return Settle::Pending;
        }
    };
    let scoped = engine.for_post_admission(&admission);
    let cancellation = admission.cancellation.clone();
    let upstream = gate.clone();
    let admitted_gate: PpGate = Some(Arc::new(move |job| {
        !cancellation.is_cancelled() && upstream.as_ref().is_none_or(|claim| claim(job))
    }));
    let mut terminal = terminal;
    if terminal.salvage == nzbd_state::artifacts::SalvageState::Pending {
        assess_and_publish_salvage(&scoped, cfg, dest_dir, &admitted_gate, &row, &terminal).await;
        if let Some(reloaded) = load_terminal(engine, job).await {
            terminal = reloaded;
        }
    }
    // Review #280-7: the source's assessment after salvage. A published
    // salvage took the verified payload, so the remainder is redundant;
    // otherwise assess what is left — unverified-but-intact payload or a
    // repairable input is `recoverable` (the pre-v2 "absent proof is not
    // cleanup authority" rule), nothing usable is `unusable`.
    if terminal.salvage != nzbd_state::artifacts::SalvageState::Pending {
        if terminal.salvage == nzbd_state::artifacts::SalvageState::Published {
            let inventory = engine.artifacts();
            let _ = tokio::task::spawn_blocking(move || {
                if let Some(source) = inventory.job_source(job.0)? {
                    inventory.set_failure_assessment(
                        &source.id,
                        &source.generation,
                        "unusable",
                        "verified payload was published as salvage; the remainder is redundant",
                    )?;
                }
                Ok::<_, nzbd_state::artifacts::Error>(())
            })
            .await;
        } else if let Err(error) = record_failure_assessment(
            &scoped,
            &row,
            &Default::default(),
            &Default::default(),
            &Default::default(),
            &Default::default(),
            &Default::default(),
            None,
        )
        .await
        {
            tracing::warn!(job = job.0, %error, "failed-source assessment did not complete; the source is retained");
        }
    }
    // Forced (Retry now, `job_pp_finished`): the disposal operation's own
    // ladder is brought forward, so a park that moved its bytes but did not
    // commit is completed now rather than at its `next_retry`.
    if force && terminal.salvage != nzbd_state::artifacts::SalvageState::Pending {
        let inventory = engine.artifacts();
        let in_flight = inventory.clone();
        if tokio::task::spawn_blocking(move || in_flight.disposal_in_flight(job.0))
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or(false)
        {
            if let Ok(Err(error)) =
                tokio::task::spawn_blocking(move || inventory.retry_disposal_now(job.0)).await
            {
                tracing::warn!(job = job.0, %error, "failed-source disposal stays on its ladder");
            }
        }
    }
    let done = if terminal.salvage == nzbd_state::artifacts::SalvageState::Pending {
        false
    } else {
        dispose_failed_source(&scoped, cfg, dest_dir, &admitted_gate, &row, &terminal).await
    };
    if let Err(error) = engine.end_post_admission(admission).await {
        tracing::warn!(job = job.0, %error, "failed-job follow-up custody release remains pending");
    }
    Settle::from(done)
}

/// Test support: fail a job as the owner would (a health terminal) and run
/// its salvage/disposal follow-up, the way the manager does on the event.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
async fn fail_and_follow_up(
    engine: &EngineHandle,
    cfg: &PostConfig,
    history: &Arc<HistoryDb>,
    dest_dir: &Path,
    gate: &PpGate,
    job: JobId,
    _name: String,
    _health: u16,
    _force: bool,
) -> bool {
    if let Ok(Some(row)) = engine.export_job(job).await {
        if !pp_done(&row) {
            let terminal = nzbd_engine::terminal::failed_terminal(
                &row,
                nzbd_types::FailureReason::Health,
                None,
                "health",
                "download health below critical",
            );
            let _ = engine
                .record_terminal_in(terminal, Some(history.clone()))
                .await;
        }
    }
    failure_followup(engine, cfg, dest_dir, gate, job, true).await
}

/// Cluster worker, after the leader accepted a failed post-processing result
/// (so the job's terminal is durable there, contract §2.1): dispose of this
/// node's own attempt copy by the same rule as single-node disposal —
/// Keep retains, park-always reasons retain in place, a source assessed `recoverable`
/// (or not assessed) is retained, anything else follows `failure_action`.
/// The cluster cleanup ledger then finds the copy already settled. Before
/// v2 this ran inside the attempt, ahead of the stamp.
pub async fn dispose_remote_failure(
    engine: &EngineHandle,
    cfg: &PostConfig,
    dest_dir: &Path,
    job: &Job,
) -> bool {
    let param = |key: &str| {
        job.params
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    };
    if !param(PP_DONE_PARAM).is_some_and(|status| !status.starts_with("SUCCESS")) {
        return true;
    }
    let reason = param("Failure:Reason").and_then(nzbd_types::FailureReason::parse);
    let inventory = engine.artifacts();
    let assessed = inventory
        .for_job(job.id.0)
        .ok()
        .flatten()
        .and_then(|artifact| artifact.failure_assessment)
        .map(|assessment| assessment.disposition);
    let action = if nzbd_engine::terminal::keeps_failed_source(job) {
        FailureAction::None
    } else if reason.is_some_and(|reason| reason.park_always()) {
        // Contract §2.2 park-always: the copy stays under runner custody
        // where it is; the cluster ledger retains it (`artifacts::cluster`).
        FailureAction::None
    } else if assessed.as_deref() != Some("unusable") {
        // `recoverable` holds payload; no assessment proves nothing.
        FailureAction::None
    } else {
        cfg.failure_action
    };
    if action == FailureAction::None {
        return true;
    }
    let mut failure_cfg = cfg.clone();
    failure_cfg.failure_action = action;
    let dir_name = nzbd_engine::queue::job_dir_name(job);
    let source = dest_dir.join(&dir_name);
    let observed = dispose_failed(
        inventory,
        &failure_cfg,
        job.id,
        &source,
        dest_dir,
        &dir_name,
        "remote",
    )
    .await;
    observed.files_at.is_none() || action == FailureAction::Park
}

/// Run a failed job's salvage and disposal now (contract §2.2/§2.6), as
/// the manager does on its `job_pp_finished`. Returns true once nothing is
/// left to do.
pub async fn follow_up_failure(
    engine: &EngineHandle,
    cfg: &PostConfig,
    dest_dir: &Path,
    job: JobId,
) -> bool {
    failure_followup(engine, cfg, dest_dir, &None, job, true).await
}

async fn load_terminal(
    engine: &EngineHandle,
    job: JobId,
) -> Option<nzbd_state::artifacts::JobTerminal> {
    let inventory = engine.artifacts();
    tokio::task::spawn_blocking(move || inventory.terminal(job.0))
        .await
        .ok()?
        .ok()?
}

/// Contract §2.2: assess the failed source and publish whatever verified
/// output it holds under its own generation, announce it with
/// `job_publication_ready`, and settle the obligation. An environmental
/// error leaves the obligation pending (disposal stays blocked) and the
/// next rescan retries; any other error settles it as `none` — a salvage
/// attempt never produces a second terminal.
async fn assess_and_publish_salvage(
    engine: &EngineHandle,
    cfg: &PostConfig,
    dest_dir: &Path,
    gate: &PpGate,
    row: &Job,
    terminal: &nzbd_state::artifacts::JobTerminal,
) {
    let job = row.id;
    let settle = |outcome: nzbd_state::artifacts::SalvageOutcome| {
        let inventory = engine.artifacts();
        async move {
            match tokio::task::spawn_blocking(move || inventory.settle_salvage(job.0, &outcome))
                .await
            {
                Ok(Ok(())) => true,
                Ok(Err(error)) => {
                    tracing::warn!(job = job.0, %error, "salvage outcome not recorded; retrying on the next rescan");
                    false
                }
                Err(_) => false,
            }
        }
    };
    let unsettled = |error: PostError| async move {
        if storage_hold_cause(&error).is_some() {
            tracing::warn!(job = job.0, %error, "salvage waits for storage; obligation stays pending");
            None
        } else {
            tracing::warn!(job = job.0, %error, "salvage assessment failed; nothing published");
            Some(error.to_string())
        }
    };
    // A crash between a salvage publication's relocation and its delivery
    // row (or a row an earlier build left mid-salvage) finishes that same
    // publication from its durable journal instead of salvaging again.
    if row
        .params
        .iter()
        .any(|(key, _)| key == PP_SELECTED_OUTCOME_PARAM)
    {
        let inventory = engine.artifacts();
        let committed = row.clone();
        match tokio::task::spawn_blocking(move || {
            finish_committed_publication_receipt(&inventory, &committed)
        })
        .await
        {
            Ok(Ok(Some(delivery))) => {
                let publication = nzbd_types::Publication {
                    path: delivery.publication_path.clone(),
                    generation: delivery.publication_generation.clone(),
                    manifest_digest: delivery.manifest_digest.clone(),
                    files: u32::try_from(delivery.files.len()).unwrap_or(u32::MAX),
                    cross_volume: engine.artifacts().delivery_cross_volume(&delivery),
                };
                if settle(nzbd_state::artifacts::SalvageOutcome::Published(
                    publication.clone(),
                ))
                .await
                {
                    engine.emit_publication_ready(job, publication);
                }
                return;
            }
            Ok(Ok(None)) => {}
            Ok(Err(error)) => {
                if let Some(error) = unsettled(error).await {
                    settle(nzbd_state::artifacts::SalvageOutcome::None { error: Some(error) })
                        .await;
                }
                return;
            }
            Err(_) => return,
        }
    }
    let mut archive = EarlyArchiveAssessment::default();
    if terminal.reason == Some(nzbd_types::FailureReason::Health) {
        // A health failure never reached post-processing: repair what PAR can
        // and extract complete archives before selecting verified output.
        let par_failures = match repair_failed_health_par_sets(engine, cfg, row, gate).await {
            Ok(failures) => failures,
            Err(error) => {
                if let Some(error) = unsettled(error).await {
                    settle(nzbd_state::artifacts::SalvageOutcome::None { error: Some(error) })
                        .await;
                }
                return;
            }
        };
        archive = match extract_complete_failed_archives(engine, cfg, row, gate).await {
            Ok(assessment) => assessment,
            Err(error) => {
                if let Some(error) = unsettled(error).await {
                    settle(nzbd_state::artifacts::SalvageOutcome::None { error: Some(error) })
                        .await;
                }
                return;
            }
        };
        archive.par_failures = par_failures;
    }
    if gate.as_ref().is_some_and(|claim| !claim(job)) {
        return;
    }
    let inventory = engine.artifacts();
    let completed_root = cfg
        .completed_dir
        .clone()
        .unwrap_or_else(|| dest_dir.to_path_buf());
    let status = terminal.status().to_string();
    let stage = terminal.stage.clone().unwrap_or_default();
    let message = terminal.message.clone().unwrap_or_default();
    let salvage_row = row.clone();
    let result = tokio::task::spawn_blocking(move || {
        salvage_failed_delivery(
            &inventory,
            &salvage_row,
            &completed_root,
            &status,
            &stage,
            &message,
            &archive,
        )
    })
    .await;
    match result {
        Ok(Ok(Some(salvage))) => {
            let publication = nzbd_types::Publication {
                path: salvage.published_path.to_string_lossy().into_owned(),
                generation: salvage.generation.clone(),
                manifest_digest: salvage
                    .delivery
                    .as_ref()
                    .map(|delivery| delivery.manifest_digest.clone())
                    .unwrap_or_default(),
                files: salvage
                    .delivery
                    .as_ref()
                    .map(|delivery| u32::try_from(delivery.files.len()).unwrap_or(u32::MAX))
                    .unwrap_or(0),
                cross_volume: salvage
                    .delivery
                    .as_ref()
                    .is_some_and(|delivery| engine.artifacts().delivery_cross_volume(delivery)),
            };
            if settle(nzbd_state::artifacts::SalvageOutcome::Published(
                publication.clone(),
            ))
            .await
            {
                tracing::info!(job = job.0, path = %publication.path, "salvage publication ready");
                engine.emit_publication_ready(job, publication);
            }
        }
        Ok(Ok(None)) => {
            settle(nzbd_state::artifacts::SalvageOutcome::None { error: None }).await;
        }
        Ok(Err(error)) => {
            if let Some(error) = unsettled(error).await {
                settle(nzbd_state::artifacts::SalvageOutcome::None { error: Some(error) }).await;
            }
        }
        Err(error) => {
            tracing::warn!(job = job.0, %error, "salvage worker stopped; retrying on the next rescan");
        }
    }
}

/// Contract §2.6: dispose of a failed source once its salvage obligation has
/// settled. The job's own keep-on-failure retains it; `publish`, `io`,
/// `identity` and `hold_expired` always park; otherwise `[post]
/// failure_action` means what it says. Returns true once the disposal is
/// recorded on the terminal row.
async fn dispose_failed_source(
    engine: &EngineHandle,
    cfg: &PostConfig,
    dest_dir: &Path,
    gate: &PpGate,
    row: &Job,
    terminal: &nzbd_state::artifacts::JobTerminal,
) -> bool {
    let job = row.id;
    let inventory = engine.artifacts();
    let gate_inventory = inventory.clone();
    match tokio::task::spawn_blocking(move || gate_inventory.disposal_allowed(job.0)).await {
        Ok(Ok(Some(_))) => {}
        _ => return false,
    }
    let action = disposal_action(cfg, terminal);
    let dir_name = nzbd_engine::queue::job_dir_name(row);
    let source = dest_dir.join(&dir_name);
    let record = |note: String| {
        let inventory = inventory.clone();
        async move {
            tracing::info!(job = job.0, %note, "failed source disposed");
            matches!(
                tokio::task::spawn_blocking(move || inventory.record_disposal(job.0, &note)).await,
                Ok(Ok(()))
            )
        }
    };
    let has_source = inventory
        .for_job(job.0)
        .ok()
        .flatten()
        .is_some_and(|artifact| !artifact.terminal() && artifact.path.exists())
        || source.exists()
        // A disposal operation that moved the bytes but did not commit
        // still holds them; replaying it below records where they are.
        || inventory.disposal_in_flight(job.0).unwrap_or(true);
    if !has_source {
        // A fetch failure, or an attempt that never wrote a byte.
        return record("no payload".into()).await;
    }
    if terminal.salvage == nzbd_state::artifacts::SalvageState::Published
        && action != FailureAction::Delete
    {
        // The job's newest artifact is now its salvage publication, which
        // curator owns: nothing may relocate it. What remains of the source
        // stays where it is, retained under runner's custody (park/keep).
        return record("salvage published; source retained".into()).await;
    }
    if terminal.salvage == nzbd_state::artifacts::SalvageState::Published {
        // The salvage publication took the verified files; its delivery
        // retires what remains of the source.
        let delivery_inventory = inventory.clone();
        let retired = tokio::task::spawn_blocking(move || {
            let delivery = delivery_inventory
                .delivery_for_job(job.0)?
                .ok_or(nzbd_state::artifacts::Error::NotFound)?;
            delivery_inventory.retire_delivery_source(&delivery.delivery_id)
        })
        .await;
        return match retired {
            Ok(Ok(_)) => record("salvage published; source retired".into()).await,
            Ok(Err(error)) => {
                tracing::warn!(job = job.0, %error, "salvaged source retirement remains pending");
                false
            }
            Err(_) => false,
        };
    }
    // Review #280-7 (the pre-v2 "recoverable/unknown → retain" rule): only a
    // source positively assessed `unusable` may be deleted; a recoverable
    // or unassessed one is parked for recovery instead.
    let assessed_unusable = {
        let inventory = inventory.clone();
        tokio::task::spawn_blocking(move || inventory.job_source(job.0))
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
            .and_then(|source| source.failure_assessment)
            .is_some_and(|assessment| assessment.disposition == "unusable")
    };
    let action = if action == FailureAction::Delete && !assessed_unusable {
        FailureAction::Park
    } else {
        action
    };
    let mut failure_cfg = cfg.clone();
    failure_cfg.failure_action = action;
    if gate.as_ref().is_some_and(|claim| !claim(job)) {
        return false;
    }
    let observed = dispose_failed(
        inventory.clone(),
        &failure_cfg,
        job,
        &source,
        dest_dir,
        &dir_name,
        "local",
    )
    .await;
    // The move/delete ran under the admission; if the admission was lost
    // meanwhile (an owner fence, shutdown) the outcome is not recorded by a
    // holder that no longer owns the job. The next follow-up finds the
    // canonical result and records it once.
    if gate.as_ref().is_some_and(|claim| !claim(job))
        || inventory.take_injected_disposal_admission_loss(job.0)
    {
        tracing::warn!(
            job = job.0,
            "failed-source disposal lost its admission; the next follow-up records it"
        );
        return false;
    }
    let Some(disposition) =
        canonical_failed_disposition(&failure_cfg, dest_dir, &dir_name, &source).or_else(|| {
            // Nothing was ever written for this attempt: there is nothing to
            // move or delete.
            (!source.exists() && observed.note != "kept").then(|| Disposition {
                note: "already gone".into(),
                files_at: None,
                ..Default::default()
            })
        })
    else {
        if observed.out_of_space {
            engine.report_out_of_space(format!(
                "failed-file disposition for job {}: {}",
                job.0, observed.note
            ));
        }
        tracing::warn!(job = job.0, outcome = %observed.note,
            "failed-source disposition is incomplete; its operation retries on its own ladder");
        return false;
    };
    if let Some(path) = disposition.files_at.as_deref() {
        let state = if disposition.note.starts_with("parked at ") {
            "parked_failed"
        } else {
            "retained"
        };
        if let Err(error) = inventory.finish(job.0, path, path.parent().unwrap_or(dest_dir), state)
        {
            tracing::warn!(job = job.0, %error, "failed-source inventory record remains pending");
            return false;
        }
    }
    record(disposition.note).await
}

/// Contract §2.6 precedence: the job's keep-on-failure, then the
/// park-always reasons, then `[post] failure_action`.
fn disposal_action(
    cfg: &PostConfig,
    terminal: &nzbd_state::artifacts::JobTerminal,
) -> FailureAction {
    if terminal.keep_on_failure {
        FailureAction::None
    } else if terminal.reason.is_some_and(|reason| reason.park_always()) {
        FailureAction::Park
    } else {
        cfg.failure_action
    }
}

#[derive(Clone, Default)]
struct EarlyArchiveAssessment {
    terminal: std::collections::HashSet<String>,
    password_required: std::collections::HashSet<String>,
    par_failures: HashMap<String, String>,
}

/// A health failure does not preclude repair of an independent PAR set. Only
/// sets whose validated recovery blocks cover the measured damaged slices
/// enter the normal private repair workspace and durable transform journal.
/// A salvage input that resolved outside its own source directory.
fn outside_source(error: std::path::StripPrefixError) -> PostError {
    PostError::Artifact(nzbd_state::artifacts::Error::Conflict(format!(
        "salvage input outside its source: {error}"
    )))
}

/// A salvage step refused because the job's post-processing claim moved.
fn authority_changed(during: &str) -> PostError {
    PostError::Artifact(nzbd_state::artifacts::Error::Conflict(format!(
        "post-processing authority changed during {during}"
    )))
}

async fn repair_failed_health_par_sets(
    engine: &EngineHandle,
    cfg: &PostConfig,
    job: &Job,
    gate: &PpGate,
) -> Result<HashMap<String, String>, PostError> {
    let inventory = engine.artifacts();
    let source = inventory
        .for_job(job.id.0)?
        .ok_or(PostError::Artifact(nzbd_state::artifacts::Error::NotFound))?;
    let root = source.path.clone();
    let entries = source.files.clone();
    let partial_paths = sealed_partial_paths(job, &source);
    let gate_for_scan = gate.clone();
    let id = job.id;
    let sets = tokio::task::spawn_blocking(move || {
        par_repairable_evidence_with_partials(&root, &entries, &partial_paths, &|| {
            crate::attempt::checkpoint()?;
            if gate_for_scan.as_ref().is_some_and(|claim| !claim(id)) {
                return Err(std::io::ErrorKind::Interrupted.into());
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| PostError::Subprocess(error.to_string()))??
    .1;
    let tool = Par2Tool {
        cmd: cfg.par2_cmd.clone(),
        timeout: cfg.tool_timeout,
    };
    let partials: Vec<_> =
        job.files
            .iter()
            .filter(|file| {
                !file.paused
                    && !file.is_par2
                    && (job.file_needs_repair(file.id)
                        || file.segments.iter().any(|segment| {
                            matches!(segment.state, nzbd_types::SegmentState::Failed)
                        }))
            })
            .map(|file| {
                (
                    source.path.join(format!(".runner-file-{}.part", file.id.0)),
                    file.filename.clone(),
                )
            })
            .collect();
    let mut restored = std::collections::HashSet::new();
    let mut failures = HashMap::new();
    for set in sets {
        if gate.as_ref().is_some_and(|claim| !claim(job.id)) {
            return Err(authority_changed("PAR salvage"));
        }
        let result = crate::repair_workspace::repair(
            &inventory,
            job.id.0,
            &set,
            &tool,
            &partials,
            &mut restored,
        )
        .await;
        let reason = match result {
            Ok(VerifyResult::Intact) => continue,
            Ok(VerifyResult::Repairable { .. } | VerifyResult::NeedMoreBlocks { .. }) => {
                "PAR tool needs additional verified recovery blocks; acquire them and retry repair"
                    .to_string()
            }
            Ok(VerifyResult::Unrepairable) => {
                "PAR tool could not restore this measured repairable set; inspect tool output and retry repair"
                    .to_string()
            }
            Err(error) => return Err(error),
        };
        for file in &set.files {
            let relative = crate::namespace::relative(&file.name)?;
            let source_relative = set
                .root
                .join(relative)
                .strip_prefix(&source.path)
                .map_err(outside_source)?
                .to_string_lossy()
                .into_owned();
            failures.insert(source_relative, reason.clone());
        }
        for path in &set.par_paths {
            if let Ok(relative) = path.strip_prefix(&source.path) {
                failures.insert(relative.to_string_lossy().into_owned(), reason.clone());
            }
        }
    }
    Ok(failures)
}

async fn extract_complete_failed_archives(
    engine: &EngineHandle,
    cfg: &PostConfig,
    job: &Job,
    gate: &PpGate,
) -> Result<EarlyArchiveAssessment, PostError> {
    let mut assessment = EarlyArchiveAssessment::default();
    if !cfg.unpack {
        return Ok(assessment);
    }
    let inventory = engine.artifacts();
    let source = inventory
        .for_job(job.id.0)?
        .ok_or(PostError::Artifact(nzbd_state::artifacts::Error::NotFound))?;
    let evidence = inventory.verified_custody_evidence(job.id.0, || Ok(()))?;
    let mut complete = verified_direct_payload_paths(job, &source, &evidence.restored, || Ok(()))?;
    let already_consumed = evidence.consumed_inputs;
    complete.extend(evidence.verified_outputs);
    let extractors = Extractors {
        unrar_cmd: cfg.unrar_cmd.clone(),
        sevenzip_cmd: cfg.sevenzip_cmd.clone(),
        timeout: cfg.tool_timeout,
    };
    let password = job
        .params
        .iter()
        .find(|(key, _)| key == "*Unpack:Password")
        .map(|(_, value)| value.as_str());
    for (archive, kind) in detect_archives(&source.path) {
        use sha2::{Digest, Sha256};
        if gate.as_ref().is_some_and(|claim| !claim(job.id)) {
            return Err(authority_changed("archive salvage"));
        }
        let inputs = archive_input_set(&source.path, &archive, kind);
        let paths = inputs
            .iter()
            .map(|path| {
                path.strip_prefix(&source.path)
                    .map(|relative| relative.to_string_lossy().into_owned())
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(outside_source)?;
        if paths.is_empty()
            || !paths.iter().all(|path| complete.contains(path))
            || paths.iter().all(|path| already_consumed.contains(path))
        {
            continue;
        }
        let token = format!("{:x}", Sha256::digest(archive.to_string_lossy().as_bytes()));
        let mut workspace = inventory.workspace(job.id.0, "extract", &token)?;
        inventory.bind_extraction_inputs(&mut workspace, &inputs)?;
        if inventory.extraction_published(&workspace)? {
            continue;
        }
        let _capacity = nzbd_state::capacity::reserve(&workspace.scratch.path, job.totals.size)?;
        let attempt = inventory.begin_workspace_attempt(&mut workspace)?;
        let staging = attempt.join("output");
        std::fs::create_dir_all(&staging)?;
        let result = extractors
            .extract(&archive, kind, &staging, password)
            .await?;
        if result.disk_space_error {
            inventory.abandon_workspace(&workspace)?;
            // The extractor reported a full volume: a capacity wait, not a
            // verdict on the bytes (the obligation stays pending).
            return Err(PostError::Io(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "archive salvage needs free capacity; retry extraction when space is available",
            )));
        }
        if result.password_error {
            assessment.password_required.extend(paths);
            inventory.abandon_workspace(&workspace)?;
            continue;
        }
        if result.retryable_error {
            inventory.abandon_workspace(&workspace)?;
            let detail = result
                .attempts
                .iter()
                .map(|attempt| attempt.diagnostic.as_str())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(PostError::Subprocess(format!(
                "archive extractor storage or tool failure requires retry: {detail}"
            )));
        }
        let shortfall = result
            .success
            .then(|| crate::tools::unpack_shortfall(&source.path, &archive, &result.output_dir))
            .flatten();
        if !result.success || shortfall.is_some() {
            assessment.terminal.extend(paths);
            inventory.abandon_workspace(&workspace)?;
            continue;
        }
        let output = result.output_dir;
        let source_path = source.path.clone();
        let gate_for_commit = gate.clone();
        let id = job.id;
        terminal_worker(inventory.clone(), id, move |inventory, _, checkpoint| {
            checkpoint()?;
            if gate_for_commit.as_ref().is_some_and(|claim| !claim(id)) {
                return Err(PostError::Io(std::io::ErrorKind::Interrupted.into()));
            }
            commit_staging(&output, &source_path)?;
            checkpoint()?;
            inventory.finish_extraction(&workspace, &output)?;
            Ok(())
        })
        .await?;
    }
    Ok(assessment)
}

fn salvage_failed_delivery(
    inventory: &nzbd_state::artifacts::Inventory,
    job: &Job,
    completed_root: &Path,
    terminal_outcome: &str,
    failure_stage: &str,
    failure_cause: &str,
    archive_assessment: &EarlyArchiveAssessment,
) -> Result<Option<FailedSalvage>, PostError> {
    // Contract §2.5: a salvage move a crash left open is finished or unwound
    // first; one that committed is completed from its journal, never
    // selected again from what is now the publication.
    inventory.reconcile_job_intents(job.id.0)?;
    if let Some(moved) = inventory.committed_intent_publication(job.id.0)? {
        return finish_salvage_receipt(
            inventory,
            job,
            completed_root,
            moved,
            terminal_outcome,
            failure_stage,
            failure_cause,
        )
        .map(Some);
    }
    let source = inventory.refresh_for_delivery(job.id.0)?;
    if !source.automatic_cleanup_eligible || !source.owned {
        return Err(PostError::Artifact(nzbd_state::artifacts::Error::Conflict(
            "failed source is not an owned eligible generation for bounded recovery assessment"
                .into(),
        )));
    }
    let evidence = inventory.verified_custody_evidence(job.id.0, || Ok(()))?;
    let verified_direct =
        verified_direct_payload_paths(job, &source, &evidence.restored, || Ok(()))?;
    let (consumed, extracted) = (evidence.consumed_inputs, evidence.verified_outputs);
    let verified = verified_direct.into_iter().chain(extracted).collect();
    let probed = probe_payload_kinds(&source.path, &source.files, &verified);
    let mut diagnostics = assess_file_inputs(job, &source, &verified)?;
    for path in &archive_assessment.password_required {
        diagnostics.insert(
            path.clone(),
            FileAssessment::Repairable(
                "archive requires a password; set *Unpack:Password and retry extraction".into(),
            ),
        );
    }
    for (path, reason) in &archive_assessment.par_failures {
        diagnostics.insert(path.clone(), FileAssessment::Repairable(reason.clone()));
    }
    let (selected, outcomes) = select_delivery_payload(
        &source.files,
        &consumed,
        &verified,
        &archive_assessment.terminal,
        &diagnostics,
        &probed,
    );
    if selected.is_empty() {
        return Ok(None);
    }
    std::fs::create_dir_all(completed_root)?;
    let name = nzbd_engine::queue::sanitize_name(&job.name);
    let target = completed_root.join(format!(
        "{name}.job-{}.runner-{}",
        job.id.0, source.generation
    ));
    let delivery_lifecycle = job.delivery_lifecycle;
    let transfer = job
        .params
        .iter()
        .find(|(key, _)| key == "monarr-transfer")
        .map(|(_, value)| value.as_str());
    let moved = inventory.publish_selected_with_transfer(
        job.id.0,
        &target,
        selected,
        outcomes,
        transfer,
        delivery_lifecycle,
        &|| Ok(()),
    )?;
    finish_salvage_receipt(
        inventory,
        job,
        completed_root,
        moved,
        terminal_outcome,
        failure_stage,
        failure_cause,
    )
    .map(Some)
}

/// The receipt half of a salvage publication: mark it completed and write
/// its delivery row (idempotent for the same generation).
fn finish_salvage_receipt(
    inventory: &nzbd_state::artifacts::Inventory,
    job: &Job,
    completed_root: &Path,
    moved: nzbd_state::artifacts::RelocationResult,
    terminal_outcome: &str,
    failure_stage: &str,
    failure_cause: &str,
) -> Result<FailedSalvage, PostError> {
    let delivery_lifecycle = job.delivery_lifecycle;
    if let Some(delivery) = inventory
        .delivery_for_job(job.id.0)?
        .filter(|delivery| delivery.publication_generation == moved.generation)
    {
        return Ok(FailedSalvage {
            delivery: Some(delivery),
            published_path: moved.published_path,
            generation: moved.generation,
        });
    }
    inventory.finish(
        job.id.0,
        &moved.published_path,
        moved.published_path.parent().unwrap_or(completed_root),
        "completed",
    )?;
    let delivery = if delivery_lifecycle {
        let token = job
            .params
            .iter()
            .find(|(key, _)| key == "monarr-transfer")
            .map(|(_, value)| value.as_str());
        Some(inventory.create_delivery_with_lifecycle_snapshot(
            job.id.0,
            true,
            token,
            terminal_outcome,
            Some(failure_stage),
            Some(failure_cause),
            job_keeps_failed_source(job),
            delivery_stage_snapshot(inventory, job),
            nzbd_engine::frozen_lifecycle_timing(job),
        )?)
    } else {
        None
    };
    Ok(FailedSalvage {
        delivery,
        published_path: moved.published_path,
        generation: moved.generation,
    })
}

struct FailedSalvage {
    delivery: Option<nzbd_state::artifacts::Delivery>,
    published_path: PathBuf,
    generation: String,
}

fn job_keeps_failed_source(job: &Job) -> bool {
    job.params
        .iter()
        .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1")
}

/// Retry only a read: an owner deadline does not cancel an issued mutation.
/// In particular, a brief owner stall at the Keep boundary must neither
/// authorize disposal from a stale snapshot nor restart a failed PAR repair.
async fn read_post_job<F, Fut>(
    job: JobId,
    operation: &'static str,
    mut read: F,
) -> Result<Option<Job>, PostError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<Job>, nzbd_engine::EngineError>>,
{
    for attempt in 0..3 {
        if let Some(control) = crate::attempt::current() {
            control.checkpoint()?;
        }
        match read().await {
            Ok(job) => return Ok(job),
            Err(nzbd_engine::EngineError::Timeout(_)) if attempt < 2 => {
                tracing::warn!(
                    job = job.0,
                    operation,
                    "engine read timed out; retaining source and retrying the read"
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(source) => return Err(PostError::Engine { operation, source }),
        }
    }
    unreachable!("last read returns its result")
}

/// Convert a completed failure action into a node-independent history fact.
/// A successor that observes an already-completed Delete/Park must emit the
/// same immutable payload as the authority that performed it. Failed actions
/// return `None`; the caller retries transient failures without a bound and
/// retires stable local failures only after its durable attempt budget.
fn canonical_failed_disposition(
    cfg: &PostConfig,
    dest_dir: &Path,
    dir_name: &str,
    source: &Path,
) -> Option<Disposition> {
    match cfg.failure_action {
        FailureAction::None => match source.try_exists() {
            Ok(true) => Some(Disposition {
                note: "kept".into(),
                files_at: Some(source.to_path_buf()),
                ..Default::default()
            }),
            Ok(false) => Some(Disposition {
                note: "already gone".into(),
                files_at: None,
                ..Default::default()
            }),
            Err(_) => None,
        },
        FailureAction::Delete => match source.try_exists() {
            Ok(false) => Some(Disposition {
                note: "deleted".into(),
                files_at: None,
                ..Default::default()
            }),
            _ => None,
        },
        FailureAction::Park => {
            let target = cfg
                .failed_dir
                .clone()
                .unwrap_or_else(|| dest_dir.join(".failed"))
                .join(dir_name);
            match (source.try_exists(), target.try_exists()) {
                (Ok(false), Ok(true)) => Some(Disposition {
                    note: format!("parked at {}", target.display()),
                    files_at: Some(target),
                    ..Default::default()
                }),
                _ => None,
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn scan_queue(
    tracker: &TaskTracker,
    engine: &EngineHandle,
    cfg: &PostConfig,
    history: &Arc<HistoryDb>,
    dest_dir: &Path,
    gate: &PpGate,
    sem: &Arc<tokio::sync::Semaphore>,
    cancel: &CancellationToken,
    finished: &tokio::sync::mpsc::UnboundedSender<JobId>,
    active: &mut HashMap<JobId, ActiveAttempt>,
) -> bool {
    let claim = |job: JobId| gate.as_ref().map(|f| f(job)).unwrap_or(true);
    let inventory = engine.artifacts();
    let snapshot = engine.snapshot();
    // Review #283-1: a follow-up the owner deferred (boot verification or
    // journal loading fences export and admission) is retried on the next
    // queue change, not at the next periodic rescan.
    let mut deferred = false;
    // Salvage and disposal are driven from `job_terminals`, not from queue
    // status (contract §2.6): a terminal whose queue row is already gone
    // still settles its obligations.
    // Review #280-6: in cluster mode too — each owed terminal is followed up
    // only by the node whose gate claims the job, so exactly one disposes.
    {
        let queued = snapshot
            .jobs
            .iter()
            .map(|job| job.id)
            .collect::<std::collections::HashSet<_>>();
        let lookup = inventory.clone();
        let owed = tokio::task::spawn_blocking(move || {
            let mut owed = lookup.pending_salvage()?;
            owed.extend(lookup.pending_disposal()?);
            Ok::<_, nzbd_state::artifacts::Error>(owed)
        })
        .await;
        if let Ok(Ok(owed)) = owed {
            for terminal in owed {
                let job = JobId(terminal.job);
                if !queued.contains(&job) && claim(job) {
                    deferred |= failure_followup_settle(engine, cfg, dest_dir, gate, job, false)
                        .await
                        == Settle::Deferred;
                }
            }
        }
    }
    for j in snapshot.jobs.iter() {
        // Holds are released by the engine's recovery task (mount
        // re-observation, scheduled retries, storage) and by operator
        // retries; post-processing only runs what that leaves PostQueued.
        // NZBGet parity: finished-and-processed jobs live in history, not
        // the queue. Single-node the manager IS the authority; in cluster
        // mode the leader sweep retires (the gate stays out of hygiene).
        if j.pp_done && matches!(j.status, JobStatus::Completed | JobStatus::Failed) {
            if active.contains_key(&j.id) {
                continue;
            }
            // Contract §2.6: a failed row stays queued until its terminal's
            // salvage obligation has settled and its source is disposed —
            // salvage and disposal read it.
            let terminal = {
                let inventory = inventory.clone();
                let id = j.id.0;
                tokio::task::spawn_blocking(move || inventory.terminal(id))
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .flatten()
            };
            let settled = match &terminal {
                None => Settle::Done, // stamped before job_terminals existed
                // The owner's terminal chain is still in flight (its event
                // can precede its durable `emitted` flag); it ends with a
                // queue change.
                Some(t) if !t.emitted => Settle::Deferred,
                Some(t) if t.outcome == nzbd_types::TerminalOutcome::Failed => {
                    if claim(j.id) {
                        failure_followup_settle(engine, cfg, dest_dir, gate, j.id, false).await
                    } else {
                        Settle::Pending
                    }
                }
                Some(terminal) => finish_stamped_delivery(engine, j.id, terminal).await,
            };
            deferred |= settled == Settle::Deferred;
            if settled != Settle::Done || gate.is_some() {
                continue;
            }
            // Contract §2.5: the row is not retired while a publication move
            // of the job is open — the salvage above is what finishes it.
            let open_intent = {
                let inventory = inventory.clone();
                let id = j.id.0;
                tokio::task::spawn_blocking(move || inventory.open_intent_for_job(id))
                    .await
                    .ok()
                    .and_then(Result::ok)
                    // A read error counts as open: the row waits.
                    .unwrap_or(true)
            };
            if open_intent {
                continue;
            }
            if let Err(error) = reconcile_stamped_completion(engine, history, j.id).await {
                tracing::warn!(job=j.id.0,%error,"terminal workspace receipt remains pending");
                continue;
            }
            let _ = engine.remove_job_silent(j.id).await;
            if let Some(attempt) = active.remove(&j.id) {
                attempt.cancel.cancel();
            }
            continue;
        }
        if !j.pp_done && j.status == JobStatus::Failed && claim(j.id) && !active.contains_key(&j.id)
        {
            // A failed row with no stamp is one an earlier build failed
            // before recording it, or one whose terminal is in flight (the
            // owner refuses a second). Record its terminal now.
            if let Ok(Some(row)) = engine.export_job(j.id).await {
                let (reason, fetch_cause) = unstamped_failure(&row);
                let _ = fail_job_in(
                    engine,
                    history,
                    j.id,
                    reason,
                    fetch_cause,
                    reason.as_str(),
                    "failed before its terminal was recorded",
                )
                .await;
            }
            continue;
        }
        let needs_pp = !j.pp_done
            && matches!(
                j.status,
                JobStatus::Completed | JobStatus::PostQueued | JobStatus::Post { .. }
            );
        if needs_pp && claim(j.id) && !active.contains_key(&j.id) {
            let attempt = spawn_job(
                tracker,
                engine,
                cfg,
                history,
                dest_dir,
                sem,
                cancel,
                finished,
                j.id,
                gate.clone(),
                gate.is_none(),
                RestartPoint::Beginning,
            );
            active.insert(j.id, attempt);
        }
    }
    deferred
}

/// Retain the exact leased worker attempt through recoverable post failures.
/// Scheduling itself is an owner CAS and, on cluster workers, a durable local
/// inventory checkpoint rather than a volatile queue import.
pub async fn retain_worker_post_retry(
    engine: &EngineHandle,
    job: JobId,
    error: &PostError,
) -> bool {
    if !matches!(error, PostError::Held) {
        let (stage, stage_name) = current_stage(engine, job);
        match pp_verdict(error, stage) {
            PpVerdict::Hold(cause) => {
                if !engine
                    .hold_job(job, cause, &stage_name, &error.to_string())
                    .await
                    .unwrap_or(false)
                {
                    return false;
                }
            }
            // A terminal failure is not retried: record it and stop.
            PpVerdict::Fail(reason) => {
                let _ = engine
                    .fail_job(job, reason, None, &stage_name, &error.to_string())
                    .await;
                return false;
            }
        }
    }
    engine
        .export_job(job)
        .await
        .ok()
        .flatten()
        .and_then(|job| job.control())
        .is_some_and(|control| {
            control.lifecycle == "held"
                && control.next_retry_at_unix.is_some()
                && !control.manual_pause
        })
}

/// Replay a crash after the terminal stamp using the exact history cursor,
/// outcome and current source generation, never inferred stage completion.
async fn reconcile_stamped_completion(
    engine: &EngineHandle,
    history: &Arc<HistoryDb>,
    job: JobId,
) -> Result<(), PostError> {
    let inventory = engine.artifacts();
    let lookup = inventory.clone();
    let pending = tokio::task::spawn_blocking(move || lookup.staged_completions(job.0))
        .await
        .map_err(|e| PostError::Subprocess(e.to_string()))?
        .map_err(|e| PostError::Subprocess(e.to_string()))?;
    if pending.is_empty() {
        return Ok(());
    }
    let cleanup = engine
        .begin_failed_cleanup_admission(job)
        .await
        .map_err(|error| PostError::Subprocess(error.to_string()))?;
    let scoped = cleanup
        .as_ref()
        .map(|scope| engine.for_post_admission(&scope.admission));
    let result = reconcile_stamped_records(
        scoped.as_ref().unwrap_or(engine),
        history,
        job,
        pending,
        cleanup.is_some(),
    )
    .await;
    if let Some(scope) = cleanup {
        engine
            .end_post_admission(scope.admission)
            .await
            .map_err(|error| PostError::Subprocess(error.to_string()))?;
    }
    result
}

async fn reconcile_stamped_records(
    engine: &EngineHandle,
    history: &Arc<HistoryDb>,
    job: JobId,
    pending: Vec<nzbd_state::artifacts::StagedCompletion>,
    cleanup_only: bool,
) -> Result<(), PostError> {
    let inventory = engine.artifacts();
    let current = if cleanup_only {
        engine.failed_cleanup_metadata().await
    } else {
        engine.export_job(job).await
    }
    .map_err(|e| PostError::Subprocess(e.to_string()))?
    .ok_or_else(|| PostError::Subprocess("terminal job stamp vanished".into()))?;
    let stamped_outcome = current
        .params
        .iter()
        .find(|(key, _)| key == PP_DONE_PARAM)
        .map(|(_, value)| value.clone());
    let sequence = current
        .params
        .iter()
        .find(|(key, _)| key == "*PP:history-seq")
        .and_then(|(_, value)| value.parse::<i64>().ok());
    for completion in pending {
        let nzbd_state::artifacts::FinalizationProof::Local { history_seq } = completion.proof
        else {
            continue;
        };
        if sequence != Some(history_seq as i64)
            || stamped_outcome.as_deref() != Some(completion.outcome.as_str())
            || !matches!(current.status, JobStatus::Completed | JobStatus::Failed)
        {
            return Err(PostError::Subprocess(
                "pending completion disagrees with terminal job stamp".into(),
            ));
        }
        let history = history.clone();
        let expected = completion.outcome.clone();
        let receipt = completion.clone();
        let inventory = inventory.clone();
        tokio::task::spawn_blocking(move || {
            let recorded = history
                .get_sequence(history_seq as i64)
                .map_err(|e| PostError::Subprocess(e.to_string()))?
                .ok_or_else(|| {
                    PostError::Subprocess("pending completion history cursor missing".into())
                })?;
            if recorded.job != job || recorded.status != expected {
                return Err(PostError::Subprocess(
                    "pending completion disagrees with durable history".into(),
                ));
            }
            inventory
                .confirm_stamped_finalization(&receipt)
                .map_err(|e| PostError::Subprocess(e.to_string()))
            // Durable activation is the replay acknowledgement. Inventory
            // maintenance independently checks custody and retires scratch;
            // a concurrent reconciliation lease must not delay queue hygiene.
        })
        .await
        .map_err(|e| PostError::Subprocess(e.to_string()))??;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn spawn_job(
    tracker: &TaskTracker,
    engine: &EngineHandle,
    cfg: &PostConfig,
    history: &Arc<HistoryDb>,
    dest_dir: &Path,
    sem: &Arc<tokio::sync::Semaphore>,
    global_cancel: &CancellationToken,
    finished: &tokio::sync::mpsc::UnboundedSender<JobId>,
    job: JobId,
    gate: PpGate,
    retire_local: bool,
    from: RestartPoint,
) -> ActiveAttempt {
    let base_engine = engine.clone();
    let cfg = cfg.clone();
    let history = history.clone();
    let dest = dest_dir.to_path_buf();
    let sem = sem.clone();
    let global_cancel = global_cancel.clone();
    let attempt_cancel = CancellationToken::new();
    let task_cancel = attempt_cancel.clone();
    let finished = finished.clone();
    tracker.spawn(async move {
        // Cancellation revokes further side effects. Once admitted, retain
        // custody until the processing future and every blocking worker drain.
        let permit =
            acquire_admitted_permit(sem, global_cancel.clone(), task_cancel.clone(), gate, job)
                .await;
        if let Some(permit) = permit {
            let _permit = permit;
            // Queue status and placement alone do not authorize reading or
            // modifying recovered inputs. Acquire owner evidence at this common
            // entry point, after waiting for capacity and before pipeline I/O.
            // Do not drop an in-flight admission request on cancellation: an
            // issued lease must be received and released explicitly.
            let admission = match base_engine.begin_post_admission(job).await {
                Ok(Some(admission)) => admission,
                Ok(None) => {
                    tracing::debug!(job = job.0, "post-processing admission deferred until inputs are verified");
                    let _ = finished.send(job);
                    return;
                }
                Err(error) => {
                    tracing::warn!(job = job.0, %error, "post-processing admission unavailable; rescan may retry");
                    let _ = finished.send(job);
                    return;
                }
            };
            let engine = base_engine.for_post_admission(&admission);
            let admission_cancel=admission.cancellation.clone();
            let ctx = PpCtx {
                cancel: task_cancel.child_token(),
                commit_ok: Arc::new({ let global = global_cancel.clone(); let task = task_cancel.clone(); let admission=admission_cancel.clone(); move || !global.is_cancelled() && !task.is_cancelled() && !admission.is_cancelled() }),
                ..PpCtx::default()
            };
            let processing = process_job_ctx_from(&engine, &cfg, &history, &dest, job, &ctx, from);
            tokio::pin!(processing);
            let outcome = tokio::select! {
                _ = global_cancel.cancelled() => None,
                _ = task_cancel.cancelled() => None,
                _ = admission_cancel.cancelled() => None,
                result = &mut processing => Some(result),
            };
            ctx.cancel.cancel();
            if outcome.is_none() {
                // Some recovery/finalization steps await blocking work outside
                // the attempt tracker. Drive the entire future to completion
                // before releasing custody, including those already-started
                // steps; dropping it would detach their blocking workers.
                let _ = processing.await;
            }
            ctx.workers.close();
            ctx.workers.wait().await;
            match outcome {
                None => {
                    tracing::info!(
                        job = job.0,
                        "post-processing attempt cancelled; a requested replacement may now start"
                    );
                }
                Some(Ok(outcome)) => {
                    tracing::info!(
                        job = job.0,
                        outcome = outcome.as_str(),
                        "post-processing finished"
                    );
                    if retire_local && outcome == PpFinal::Success {
                        // NZBGet parity: the finished job's record IS the
                        // history entry — move it out of the queue now. A
                        // failed row stays until its salvage and disposal
                        // settle (`scan_queue`).
                        let _ = engine.remove_job_silent(job).await;
                    }
                }
                Some(Err(PostError::Held)) => {
                    // The stage owns the persisted cause and revision; the
                    // hold carries its own retry schedule (owner policy table).
                }
                Some(Err(e)) => {
                    // Contract §2.2: environmental errors hold (with their
                    // retry schedule and deadline in the same commit);
                    // everything else is the job's terminal failure.
                    if !apply_pp_verdict(&engine, &history, job, &e).await {
                        // Neither a hold nor a terminal landed (the owner
                        // refused: a terminal or control commit in flight,
                        // persistence fenced). The row is still in Post, so
                        // the next rescan runs the attempt again.
                        tracing::warn!(job = job.0, error = %e,
                            "post-processing verdict not recorded; the next rescan retries");
                    }
                    // PP writes as much as the downloader does — unpack,
                    // move, script output. An ENOSPC here is the same ground
                    // truth about the volume and must stop intake too, not
                    // just fail this job.
                    if nzbd_engine::out_of_space(storage_hold_cause(&e).as_ref()) {
                        engine.report_out_of_space(format!("post-processing job {}: {e}", job.0));
                    }
                    tracing::error!(job = job.0, error = %e, "post-processing crashed");
                }
            }
            // Keep admission custody until actual reader/tool worker shutdown,
            // including cancelled attempts. Release through the unscoped handle.
            if let Err(error) = base_engine.end_post_admission(admission).await {
                tracing::warn!(job = job.0, %error, "post-processing admission release remains unresolved");
            }
        }
        let _ = finished.send(job);
    });
    ActiveAttempt {
        cancel: attempt_cancel,
    }
}

async fn acquire_admitted_permit(
    sem: Arc<tokio::sync::Semaphore>,
    global_cancel: CancellationToken,
    task_cancel: CancellationToken,
    gate: PpGate,
    job: JobId,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    let permit = tokio::select! {
        _ = global_cancel.cancelled() => None,
        _ = task_cancel.cancelled() => None,
        p = sem.acquire_owned() => p.ok(),
    }?;
    // Placement and the filesystem guard can change while this attempt is
    // waiting for a slot. Revalidate at the last admission boundary before
    // repair/unpack/move is allowed to touch storage.
    if gate.as_ref().is_some_and(|claim| !claim(job)) {
        return None;
    }
    Some(permit)
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn gate_transition_while_waiting_for_slot_prevents_pipeline_start() {
        let sem = Arc::new(tokio::sync::Semaphore::new(0));
        let admitted = Arc::new(AtomicBool::new(true));
        let admitted_for_gate = admitted.clone();
        let gate: PpGate = Some(Arc::new(move |_| admitted_for_gate.load(Ordering::Acquire)));
        let wait = tokio::spawn(acquire_admitted_permit(
            sem.clone(),
            CancellationToken::new(),
            CancellationToken::new(),
            gate,
            JobId(7),
        ));
        tokio::task::yield_now().await;
        admitted.store(false, Ordering::Release);
        sem.add_permits(1);
        assert!(wait.await.unwrap().is_none());
        assert_eq!(sem.available_permits(), 1);
    }

    #[tokio::test]
    async fn refused_owner_admission_finishes_without_pipeline_or_history() {
        let temp = tempfile::tempdir().unwrap();
        let history = Arc::new(
            HistoryDb::open(&temp.path().join("history.sqlite"), Some(temp.path())).unwrap(),
        );
        let engine = nzbd_engine::Engine::spawn(nzbd_engine::EngineConfig::single_node(
            vec![],
            temp.path().join("state"),
            temp.path().join("downloads"),
            nzbd_engine::Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let tracker = TaskTracker::new();
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let cancel = CancellationToken::new();
        let (finished, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let dest = temp.path().join("pipeline-output");
        let job = JobId(404);
        // Simulate a queued attempt whose job disappeared before owner admission.
        // Every spawn path must return a completion notification even though no
        // processing authority was issued, allowing the manager to clear active.
        let _attempt = spawn_job(
            &tracker,
            &engine,
            &PostConfig::default(),
            &history,
            &dest,
            &sem,
            &cancel,
            &finished,
            job,
            None,
            true,
            RestartPoint::Beginning,
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), receiver.recv())
                .await
                .unwrap(),
            Some(job)
        );
        tracker.close();
        tracker.wait().await;
        assert_eq!(sem.available_permits(), 1);
        assert!(history.get(job).unwrap().is_none());
        assert!(!dest.exists());
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn common_spawn_success_durably_stamps_before_releasing_admission() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("downloads");
        let dir = root.join("job");
        std::fs::create_dir_all(&dir).unwrap();
        let bytes = b"complete media fixture";
        std::fs::write(dir.join("episode.mkv"), bytes).unwrap();
        let history = Arc::new(
            HistoryDb::open(&temp.path().join("history.sqlite"), Some(temp.path())).unwrap(),
        );
        let engine = nzbd_engine::Engine::spawn(nzbd_engine::EngineConfig::single_node(
            vec![],
            temp.path().join("state"),
            root.clone(),
            nzbd_engine::Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let job = Job {
            id: JobId(41),
            kind: nzbd_types::JobKind::Nzb,
            name: "job".into(),
            dir_name: "job".into(),
            name_provisional: false,
            delivery_lifecycle: false,
            queued_at_unix: 0,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: Default::default(),
            params: vec![("*File:size:1".into(), bytes.len().to_string())],
            files: vec![nzbd_types::FileEntry {
                id: nzbd_types::FileId(1),
                subject: "episode.mkv".into(),
                filename: "episode.mkv".into(),
                filename_confirmed: true,
                is_par2: false,
                paused: false,
                groups: vec![],
                date: None,
                crc32: Some(crc32fast::hash(bytes)),
                finalized: true,
                segments: vec![nzbd_types::Segment {
                    message_id: "fixture".into(),
                    number: 1,
                    size: bytes.len() as u32,
                    state: nzbd_types::SegmentState::Done {
                        offset: 0,
                        len: bytes.len() as u32,
                        crc: crc32fast::hash(bytes),
                    },
                }],
            }],
            totals: Default::default(),
            status: JobStatus::Completed,
            torrent: None,
            stages: vec![],
        };
        engine.import_job(job, false, false).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match engine.export_job(JobId(41)).await {
                    Ok(Some(job)) => {
                        assert_eq!(job.status, JobStatus::Completed);
                        assert!(engine
                            .snapshot()
                            .jobs
                            .iter()
                            .find(|summary| summary.id == JobId(41))
                            .is_some_and(|summary| summary.verification.is_none()));
                        break;
                    }
                    Err(nzbd_engine::EngineError::Lifecycle(message))
                        if message
                            .contains("verification or writer retirement is still in progress") =>
                    {
                        tokio::task::yield_now().await;
                    }
                    other => panic!("imported legacy job did not become post-ready: {other:?}"),
                }
            }
        })
        .await
        .expect("legacy inputs should verify before the common spawn entry");
        let tracker = TaskTracker::new();
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let cancel = CancellationToken::new();
        let (finished, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let cfg = PostConfig {
            unpack: false,
            cleanup: false,
            deobfuscate_final: false,
            failure_action: FailureAction::None,
            ..Default::default()
        };
        let _attempt = spawn_job(
            &tracker,
            &engine,
            &cfg,
            &history,
            &root,
            &sem,
            &cancel,
            &finished,
            JobId(41),
            None,
            false,
            RestartPoint::Beginning,
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), receiver.recv())
                .await
                .expect("common spawn must not await its own admission release"),
            Some(JobId(41))
        );
        tracker.close();
        tracker.wait().await;
        assert_eq!(sem.available_permits(), 1);
        let row = engine.export_job(JobId(41)).await.unwrap().unwrap();
        assert!(row
            .params
            .iter()
            .any(|(key, value)| key == PP_DONE_PARAM && value == "SUCCESS"));
        assert_eq!(history.get(JobId(41)).unwrap().unwrap().status, "SUCCESS");
        let durable = engine.recovery_document().await.unwrap();
        assert!(durable.jobs[0]
            .params
            .iter()
            .any(|(key, _)| key == PP_DONE_PARAM));
        engine.shutdown().await;
    }

    #[test]
    fn failover_recomputes_identical_committed_dispositions() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        let source = dest.join("job");
        std::fs::create_dir_all(&source).unwrap();
        let delete = PostConfig {
            failure_action: FailureAction::Delete,
            ..PostConfig::default()
        };
        assert!(canonical_failed_disposition(&delete, &dest, "job", &source).is_none());
        std::fs::remove_dir_all(&source).unwrap();
        let first = canonical_failed_disposition(&delete, &dest, "job", &source).unwrap();
        let successor = canonical_failed_disposition(&delete, &dest, "job", &source).unwrap();
        assert_eq!(first.note, successor.note);
        assert_eq!(first.files_at, successor.files_at);

        let failed = tmp.path().join("failed");
        let park = PostConfig {
            failure_action: FailureAction::Park,
            failed_dir: Some(failed.clone()),
            ..PostConfig::default()
        };
        std::fs::create_dir_all(failed.join("job")).unwrap();
        let first = canonical_failed_disposition(&park, &dest, "job", &source).unwrap();
        let successor = canonical_failed_disposition(&park, &dest, "job", &source).unwrap();
        assert_eq!(first.note, successor.note);
        assert_eq!(first.files_at, successor.files_at);
    }
}

fn pp_done(job: &Job) -> bool {
    job.params.iter().any(|(k, _)| k == PP_DONE_PARAM)
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn set_param(params: &mut Vec<(String, String)>, key: &str, value: String) {
    if let Some((_, existing)) = params.iter_mut().find(|(name, _)| name == key) {
        *existing = value;
    } else {
        params.push((key.to_owned(), value));
    }
}

/// Evidence for quick verification straight from the engine's export,
/// with paths remapped through any de-obfuscation renames.
fn evidence_of(
    job: &Job,
    dir: &Path,
    renames: &std::collections::HashMap<PathBuf, PathBuf>,
) -> Vec<DownloadEvidence> {
    job.files
        .iter()
        .map(|f| {
            let orig = dir.join(&f.filename);
            let path = renames.get(&orig).cloned().unwrap_or(orig);
            DownloadEvidence {
                path,
                crc32: f.crc32,
                segment_crcs: Vec::new(),
            }
        })
        .collect()
}

/// Drives one job's stage transitions: sets the queue status (as before),
/// publishes `job_pp_stage` on the engine stream, and times each stage for
/// `/metrics`.
///
/// This is the one place stage changes happen, because the three things
/// have to stay in lockstep — a status the UI shows, an event a consumer
/// reacts to and a timing an operator graphs must never disagree about
/// which stage the job is in.
struct Stages<'a> {
    engine: &'a EngineHandle,
    job: JobId,
    name: String,
    stats: Option<Arc<PpStageStats>>,
    current: Option<(PostStage, std::time::Instant)>,
}

impl<'a> Stages<'a> {
    fn new(engine: &'a EngineHandle, job: JobId, name: String, cfg: &PostConfig) -> Stages<'a> {
        Stages {
            engine,
            job,
            name,
            stats: cfg.stats.clone(),
            current: None,
        }
    }

    async fn enter(&mut self, stage: PostStage) {
        let prev_ms = self.close();
        // Status, timeline and event in that order. The first two are one
        // command precisely so a consumer that reacts to the event can
        // read the job back and find the stage already on it.
        let _ = self
            .engine
            .enter_post_stage(self.job, stage, now(), prev_ms)
            .await;
        self.engine.emit(Event::JobPpStage {
            job: self.job,
            name: self.name.clone(),
            stage,
        });
        self.current = Some((stage, std::time::Instant::now()));
    }

    /// Bank the running stage's duration into the process histogram and
    /// hand the same measurement back for the job's own timeline.
    ///
    /// Returning it is the whole point: this figure used to be computed
    /// here, averaged into `PpStageStats`, and dropped — so "how long does
    /// unpack take across every job" was answerable and "how long did
    /// unpack take for THIS job" was not, from the same measurement.
    fn close(&mut self) -> Option<u64> {
        let (stage, started) = self.current.take()?;
        let elapsed = started.elapsed();
        if let Some(stats) = self.stats.as_ref() {
            stats.record(stage, elapsed);
        }
        Some(elapsed.as_millis() as u64)
    }

    /// End the pipeline cleanly: close the last span on the job and wait
    /// for the engine to have applied it.
    ///
    /// Called before the finalize export, so the `Job` that becomes the
    /// history entry carries a *closed* final stage. Leaving it to `Drop`
    /// would be too late by exactly one export — history would show the
    /// last stage of every successful job as still running, which is the
    /// one entry you are most likely to be reading.
    ///
    /// Commands reach the owner task on one ordered channel, so the export
    /// queued after this observes the close. Safe to call more than once.
    async fn finish(&mut self) {
        let ms = self.close();
        self.engine.close_post_stage(self.job, now(), ms).await;
    }
}

impl Drop for Stages<'_> {
    /// A job that returns early (lease lost, tool error, cancelled) still
    /// spent time in its last stage. Closing on drop means "how long does
    /// unpack take" is not silently biased toward the runs that succeeded
    /// — and now that the span is on the job too, an abandoned job's last
    /// stage does not sit in history looking like it never ended.
    ///
    /// Drop cannot await. The engine offers an ordered non-blocking enqueue
    /// specifically for this path: spawning a detached sender here lets an
    /// old cancelled attempt's close arrive after its replacement entered a
    /// stage, incorrectly closing the replacement span.
    fn drop(&mut self) {
        let ms = self.close();
        self.engine.close_post_stage_now(self.job, now(), ms);
    }
}

// ---------------------------------------------------------------------------
// The stage pipeline for one job
// ---------------------------------------------------------------------------

pub async fn process_job(
    engine: &EngineHandle,
    cfg: &PostConfig,
    history: &Arc<HistoryDb>,
    dest_dir: &Path,
    job_id: JobId,
) -> Result<PpFinal, PostError> {
    process_job_from(
        engine,
        cfg,
        history,
        dest_dir,
        job_id,
        RestartPoint::Beginning,
    )
    .await
}

/// Run a local pipeline from one of the operator-safe recovery boundaries.
pub async fn process_job_from(
    engine: &EngineHandle,
    cfg: &PostConfig,
    history: &Arc<HistoryDb>,
    dest_dir: &Path,
    job_id: JobId,
    from: RestartPoint,
) -> Result<PpFinal, PostError> {
    process_job_ctx_from(
        engine,
        cfg,
        history,
        dest_dir,
        job_id,
        &PpCtx::default(),
        from,
    )
    .await
}

/// The stage pipeline with explicit fencing (cluster PP leases pass their
/// lease id as the staging tag and a live lease check as `commit_ok`).
pub async fn process_job_ctx(
    engine: &EngineHandle,
    cfg: &PostConfig,
    history: &Arc<HistoryDb>,
    dest_dir: &Path,
    job_id: JobId,
    ctx: &PpCtx,
) -> Result<PpFinal, PostError> {
    process_job_ctx_from(
        engine,
        cfg,
        history,
        dest_dir,
        job_id,
        ctx,
        RestartPoint::Beginning,
    )
    .await
}

async fn process_job_ctx_from(
    engine: &EngineHandle,
    cfg: &PostConfig,
    history: &Arc<HistoryDb>,
    dest_dir: &Path,
    job_id: JobId,
    ctx: &PpCtx,
    from: RestartPoint,
) -> Result<PpFinal, PostError> {
    if ctx.publish_history {
        if let Some(result) = resume_durable_delivery(engine, history, job_id).await? {
            return Ok(result);
        }
        if let Some(result) = resume_committed_publication(engine, history, job_id).await? {
            return Ok(result);
        }
    }
    let control = crate::attempt::AttemptControl::new(engine, ctx, job_id).await?;
    let cancel_on_drop = control.cancel.clone().drop_guard();
    let result = crate::attempt::CURRENT
        .scope(
            control.clone(),
            process_job_ctx_from_inner(engine, cfg, history, dest_dir, job_id, ctx, from),
        )
        .await;
    control.cancel.cancel();
    control.workers.close();
    control.workers.wait().await;
    let completion = control.completion.lock().unwrap().take();
    drop(cancel_on_drop);
    drop(control);
    if let Some(completion) = completion {
        let inventory = engine.artifacts();
        let local_completion = matches!(
            &completion.proof,
            nzbd_state::artifacts::FinalizationProof::Local { .. }
        );
        // Actual tools/readers have stopped and their AttemptUse is gone.
        // A selected publication may have deferred duplicate-source retirement
        // while that same generation's processing custody was still active.
        // Retry only its succeeded journals after the local terminal stamp was
        // confirmed, retaining every existing identity, Keep and hold check.
        match tokio::task::spawn_blocking(move || {
            let workspace = inventory.finalize_workspaces(
                &completion.source,
                &completion.outcome,
                completion.proof,
            );
            let relocation = if local_completion {
                inventory.retire_completed_relocations(&completion.source)
            } else {
                Ok(())
            };
            (workspace, relocation)
        })
        .await
        {
            Ok((workspace, relocation)) => {
                if let Err(error) = workspace {
                    tracing::warn!(job = job_id.0, %error, "workspace finalization remains pending");
                }
                if let Err(error) = relocation {
                    tracing::warn!(job = job_id.0, %error, "publication is committed; generation source cleanup remains pending");
                }
            }
            Err(error) => {
                tracing::warn!(job = job_id.0, %error, "post-processing finalization worker failed; workspace and source cleanup remain pending");
            }
        }
    }
    result
}

/// A selected relocation can commit before the Delivery transaction. Its
/// durable journal and the outcome stamped before publication are enough to
/// finish the same publication without rerunning extraction or scripts.
async fn resume_committed_publication(
    engine: &EngineHandle,
    history: &Arc<HistoryDb>,
    job_id: JobId,
) -> Result<Option<PpFinal>, PostError> {
    let Some(job) = engine
        .export_job(job_id)
        .await
        .map_err(|error| PostError::Subprocess(error.to_string()))?
    else {
        return Ok(None);
    };
    let inventory = engine.artifacts();
    let snapshot = job.clone();
    let prepared =
        tokio::task::spawn_blocking(move || prepare_committed_publication(&inventory, &snapshot))
            .await
            .map_err(|error| PostError::Subprocess(error.to_string()))??;
    let Some(committed) = prepared else {
        return Ok(None);
    };
    // Contract §2.1 / review #283-1 follow-up: the terminal (history) is
    // recorded before the delivery row, as on the first attempt; a crash
    // between them is finished by `finish_stamped_delivery` from the
    // terminal's `Delivery:*` markers.
    let facts = ResumedDelivery {
        terminal_outcome: committed.outcome.clone(),
        failure_stage: committed.failure_stage.clone(),
        failure_cause: committed.failure_cause.clone(),
        keep_source_on_failure: committed.keep_source_on_failure,
        source_artifact_id: committed.prepared.source_artifact_id().into(),
        source_generation: committed.prepared.source_generation().into(),
        publication_path: committed.prepared.publication_path.clone(),
        artifact_id: committed.artifact_id.clone(),
        publication_generation: committed.prepared.generation().into(),
        delivery_id: format!(
            "{}:{}",
            committed.artifact_id,
            committed.prepared.generation()
        ),
        manifest_digest: committed.prepared.manifest_digest.clone(),
        files: committed.prepared.files.len(),
        cross_volume: committed.prepared.cross_volume,
        ready_at: now(),
    };
    let outcome = record_resumed_terminal(engine, history, job_id, job, facts).await?;
    // Outside an attempt's scope (this runs before one is opened): a plain
    // blocking task, like `finish_stamped_delivery`.
    let inventory = engine.artifacts();
    tokio::task::spawn_blocking(move || -> Result<(), PostError> {
        let delivery = inventory.commit_prepared_delivery(
            committed.prepared,
            committed.request_token.as_deref(),
            &committed.outcome,
            committed.failure_stage.as_deref(),
            committed.failure_cause.as_deref(),
            committed.keep_source_on_failure,
            committed.runner_stages,
            committed.lifecycle_timing,
        )?;
        if let Err(error) = inventory.retire_delivery_source(&delivery.delivery_id) {
            tracing::warn!(job = job_id.0, %error, "delivery ready; source cleanup remains pending");
        }
        Ok(())
    })
    .await
    .map_err(|error| PostError::Subprocess(error.to_string()))??;
    Ok(Some(outcome))
}

/// A selected publication that committed before its terminal, prepared for
/// the terminal-then-delivery order.
struct CommittedPublication {
    prepared: nzbd_state::artifacts::PreparedDelivery,
    artifact_id: String,
    outcome: String,
    failure_stage: Option<String>,
    failure_cause: Option<String>,
    keep_source_on_failure: bool,
    request_token: Option<String>,
    runner_stages: Option<nzbd_state::artifacts::DeliveryRunnerStages>,
    lifecycle_timing: Option<nzbd_types::LifecycleTiming>,
}

fn delivery_stage_snapshot(
    _inventory: &nzbd_state::artifacts::Inventory,
    job: &Job,
) -> Option<nzbd_state::artifacts::DeliveryRunnerStages> {
    (!job.stages.is_empty()).then(|| nzbd_state::artifacts::DeliveryRunnerStages {
        // StageSpan persists without per-span node/boot context. A later
        // process must not relabel mixed historical spans as its own clock.
        node_id: "unrecorded".into(),
        clock_domain: "unrecorded".into(),
        spans: job.stages.iter().take(128).cloned().collect(),
    })
}

/// A salvage's committed publication (its failed terminal already
/// recorded): the delivery row, now.
fn finish_committed_publication_receipt(
    inventory: &Arc<nzbd_state::artifacts::Inventory>,
    job: &Job,
) -> Result<Option<nzbd_state::artifacts::Delivery>, PostError> {
    let Some(committed) = prepare_committed_publication(inventory, job)? else {
        return Ok(None);
    };
    inventory
        .commit_prepared_delivery(
            committed.prepared,
            committed.request_token.as_deref(),
            &committed.outcome,
            committed.failure_stage.as_deref(),
            committed.failure_cause.as_deref(),
            committed.keep_source_on_failure,
            committed.runner_stages,
            committed.lifecycle_timing,
        )
        .map(Some)
        .map_err(PostError::from)
}

fn prepare_committed_publication(
    inventory: &Arc<nzbd_state::artifacts::Inventory>,
    job: &Job,
) -> Result<Option<CommittedPublication>, PostError> {
    if !job.delivery_lifecycle {
        return Ok(None);
    }
    let Some(outcome) = job
        .params
        .iter()
        .find(|(key, _)| key == PP_SELECTED_OUTCOME_PARAM)
        .map(|(_, value)| value.clone())
    else {
        return Ok(None);
    };
    let published = inventory
        .selected_publication_for_job(job.id.0)
        .map_err(PostError::from)?;
    let Some(published) = published else {
        return Ok(None);
    };
    let request_token = job
        .params
        .iter()
        .find(|(key, _)| key == "monarr-transfer")
        .map(|(_, value)| value.clone());
    let failure_stage = (outcome == PpFinal::HealthFailure.as_str()).then(|| "health".to_string());
    let failure_cause = job
        .params
        .iter()
        .find(|(key, _)| key == PP_SELECTED_FAILURE_CAUSE_PARAM)
        .map(|(_, value)| value.clone());
    if failure_stage.is_some() && failure_cause.is_none() {
        return Err(PostError::Subprocess(
            "committed failed-health publication lacks its durable failure cause".into(),
        ));
    }
    let artifact = inventory
        .finish(
            job.id.0,
            &published,
            published.parent().unwrap_or(&published),
            "completed",
        )
        .map_err(PostError::from)?;
    let prepared = inventory
        .prepare_delivery(job.id.0, true)
        .map_err(PostError::from)?;
    Ok(Some(CommittedPublication {
        prepared,
        artifact_id: artifact.id,
        keep_source_on_failure: outcome != PpFinal::Success.as_str()
            && job_keeps_failed_source(job),
        outcome,
        failure_stage,
        failure_cause,
        request_token,
        runner_stages: delivery_stage_snapshot(inventory, job),
        lifecycle_timing: nzbd_engine::frozen_lifecycle_timing(job),
    }))
}

/// A consumer can accept a ready delivery before Runner has appended terminal
/// History. Recover that handoff from its durable manifest without opening the
/// publication path: Curator may already have imported and removed it.
async fn resume_durable_delivery(
    engine: &EngineHandle,
    history: &Arc<HistoryDb>,
    job_id: JobId,
) -> Result<Option<PpFinal>, PostError> {
    let Some(job) = engine
        .export_job(job_id)
        .await
        .map_err(|e| PostError::Subprocess(e.to_string()))?
    else {
        return Ok(None);
    };
    if !job.delivery_lifecycle {
        return Ok(None);
    }
    let inventory = engine.artifacts();
    let delivery = tokio::task::spawn_blocking(move || inventory.delivery_for_job(job_id.0))
        .await
        .map_err(|e| PostError::Subprocess(e.to_string()))?
        .map_err(|e| PostError::Subprocess(e.to_string()))?;
    let Some(delivery) = delivery else {
        return Ok(None);
    };
    let facts = ResumedDelivery {
        cross_volume: engine.artifacts().delivery_cross_volume(&delivery),
        terminal_outcome: delivery.terminal_outcome,
        failure_stage: delivery.failure_stage,
        failure_cause: delivery.failure_cause,
        keep_source_on_failure: delivery.keep_source_on_failure,
        source_artifact_id: delivery.source_artifact_id,
        source_generation: delivery.source_generation,
        publication_path: delivery.publication_path,
        artifact_id: delivery.artifact_id,
        publication_generation: delivery.publication_generation,
        delivery_id: delivery.delivery_id,
        manifest_digest: delivery.manifest_digest,
        files: delivery.files.len(),
        ready_at: delivery.ready_at,
    };
    record_resumed_terminal(engine, history, job_id, job, facts)
        .await
        .map(Some)
}

/// What a resumed terminal needs from its publication — read from the
/// durable delivery row, or from a prepared (not yet committed) one.
struct ResumedDelivery {
    terminal_outcome: String,
    failure_stage: Option<String>,
    failure_cause: Option<String>,
    keep_source_on_failure: bool,
    source_artifact_id: String,
    source_generation: String,
    publication_path: String,
    artifact_id: String,
    publication_generation: String,
    delivery_id: String,
    manifest_digest: String,
    files: usize,
    cross_volume: bool,
    ready_at: i64,
}

/// Record the terminal of a job whose publication already exists, through
/// the one terminal chain (row, history, markers, event).
async fn record_resumed_terminal(
    engine: &EngineHandle,
    history: &Arc<HistoryDb>,
    job_id: JobId,
    mut job: Job,
    delivery: ResumedDelivery,
) -> Result<PpFinal, PostError> {
    job.params.retain(|(key, _)| {
        !matches!(
            key.as_str(),
            PP_DONE_PARAM
                | PP_SELECTED_OUTCOME_PARAM
                | PP_SELECTED_FAILURE_CAUSE_PARAM
                | "*PP:history-seq"
                | "Delivery:Id"
                | "Delivery:Generation"
                | "Delivery:ManifestDigest"
        )
    });
    let final_outcome = match delivery.terminal_outcome.as_str() {
        "SUCCESS" => PpFinal::Success,
        "PAR_FAILURE" => PpFinal::ParFailure,
        "UNPACK_FAILURE" => PpFinal::UnpackFailure,
        "SCRIPT_FAILURE" => PpFinal::ScriptFailure,
        "FAILURE/HEALTH" => PpFinal::HealthFailure,
        "FAILURE/FETCH" => PpFinal::FetchFailure,
        other => {
            return Err(PostError::Subprocess(format!(
                "unknown durable delivery outcome {other:?}"
            )));
        }
    };
    job.params
        .push((PP_DONE_PARAM.into(), delivery.terminal_outcome.clone()));
    if let Some(stage) = &delivery.failure_stage {
        job.params.push(("Failure:Stage".into(), stage.clone()));
    }
    if let Some(cause) = &delivery.failure_cause {
        job.params.push(("Failure:Reason".into(), cause.clone()));
    }
    if delivery.keep_source_on_failure {
        job.params
            .push(("Failure:KeepOnFailure".into(), "true".into()));
    }
    job.params
        .push(("Delivery:Custody".into(), "curator".into()));
    job.params.push((
        "Delivery:SourceArtifactId".into(),
        delivery.source_artifact_id.clone(),
    ));
    job.params.push((
        "Delivery:SourceGeneration".into(),
        delivery.source_generation.clone(),
    ));
    job.params
        .push(("Delivery:Path".into(), delivery.publication_path.clone()));
    job.params
        .push(("Artifact:Id".into(), delivery.artifact_id.clone()));
    job.params.push((
        "Artifact:Generation".into(),
        delivery.publication_generation.clone(),
    ));
    job.params
        .push(("Delivery:Id".into(), delivery.delivery_id.clone()));
    job.params.push((
        "Delivery:Generation".into(),
        delivery.publication_generation.clone(),
    ));
    job.params.push((
        "Delivery:ManifestDigest".into(),
        delivery.manifest_digest.clone(),
    ));
    // Contract §2.1: replayed through the one terminal chain (row, history,
    // markers, event), never a fresh emit.
    let live = engine
        .export_job(job_id)
        .await
        .map_err(|e| PostError::Subprocess(e.to_string()))?
        .ok_or_else(|| PostError::Subprocess("job vanished before its terminal".into()))?;
    let markers = job
        .params
        .iter()
        .filter(|(key, value)| {
            !live
                .params
                .iter()
                .any(|(live_key, live_value)| live_key == key && live_value == value)
        })
        .cloned()
        .collect::<Vec<_>>();
    let success = final_outcome == PpFinal::Success;
    let reason = (!success)
        .then(|| nzbd_engine::terminal::reason_for_status(final_outcome.as_str()))
        .flatten();
    let publication = nzbd_types::Publication {
        path: delivery.publication_path.clone(),
        generation: delivery.publication_generation.clone(),
        manifest_digest: delivery.manifest_digest.clone(),
        files: u32::try_from(delivery.files).unwrap_or(u32::MAX),
        cross_volume: delivery.cross_volume,
    };
    let terminal = nzbd_engine::terminal::terminal_from_job(
        &job,
        if success {
            nzbd_types::TerminalOutcome::Completed
        } else {
            nzbd_types::TerminalOutcome::Failed
        },
        final_outcome.as_str(),
        Some(delivery.publication_path.clone()),
        markers,
        nzbd_engine::terminal::TerminalFacts {
            reason,
            fetch_cause: None,
            stage: delivery.failure_stage.clone(),
            message: delivery.failure_cause.clone(),
            blames_release: reason.is_some_and(|reason| reason.blames_release(None)),
            // The delivery already is the salvage publication of a failure.
            salvage: if success {
                nzbd_state::artifacts::SalvageState::None
            } else {
                nzbd_state::artifacts::SalvageState::Published
            },
            keep_on_failure: delivery.keep_source_on_failure,
            publication: success.then_some(publication),
        },
        delivery.ready_at,
    );
    record_pp_terminal(engine, history, terminal).await?;
    Ok(final_outcome)
}

async fn process_job_ctx_from_inner(
    engine: &EngineHandle,
    cfg: &PostConfig,
    history: &Arc<HistoryDb>,
    dest_dir: &Path,
    job_id: JobId,
    ctx: &PpCtx,
    from: RestartPoint,
) -> Result<PpFinal, PostError> {
    if !engine
        .set_job_status(job_id, JobStatus::PostQueued)
        .await
        .map_err(|e| PostError::Subprocess(e.to_string()))?
    {
        return Err(PostError::Subprocess(
            "job vanished or payload retirement started".into(),
        ));
    }
    let Some(job) = engine
        .export_job(job_id)
        .await
        .map_err(|e| PostError::Subprocess(e.to_string()))?
    else {
        return Err(PostError::Subprocess("job vanished".into()));
    };
    // The SOURCE directory is the job's storage name, which never changes;
    // `job.name` is a display name that a job can improve on itself (par2
    // metadata naming an obfuscated post). Deriving the input directory
    // from a mutable name is how the pipeline would come looking for a
    // directory the engine never wrote.
    let sanitized = nzbd_engine::queue::job_dir_name(&job);
    // Custody survives settings changes and committed relocations. Never adopt a
    // similarly named directory merely because it exists at the new destination.
    let mut dir = engine
        .artifacts()
        .for_job(job_id.0)
        .map_err(|e| PostError::Subprocess(e.to_string()))?
        .map(|a| a.path)
        .unwrap_or_else(|| dest_dir.join(&sanitized));
    let rule = cfg.rule_for(job.category.as_deref()).cloned();
    let unpack_enabled = rule.as_ref().and_then(|r| r.unpack).unwrap_or(cfg.unpack);
    // Superseded staging dirs (a reclaimed lease's leftovers) are garbage
    // by definition — this lease is now the only live executor.
    let staging = dir.join(format!(".pp.{}", ctx.tag));
    remove_stale_staging(&dir, &staging);
    let mut stages = Stages::new(engine, job_id, job.name.clone(), cfg);

    // ---- RENAME stage (par-rename, then rar-rename) ------------------------
    // Obfuscated posts get their real names back before anything verifies
    // or unpacks. Whole-file CRCs are content-addressed, so download
    // evidence just needs its paths remapped.
    let mut renames = Vec::new();
    if from == RestartPoint::Beginning {
        stages.enter(PostStage::ParRename).await;
        let rename_dir = dir.clone();
        let inventory = engine.artifacts();
        let candidates = naming_candidates(&job, &rename_dir);
        let outcome = crate::attempt::current()
            .expect("processing control")
            .blocking(move |_| {
                par_rename_owned_with_candidates(
                    &rename_dir,
                    Some((&inventory, job_id.0)),
                    &candidates,
                )
            })
            .await?;
        tracing::info!(job = job_id.0, metrics = ?outcome.metrics, "PAR naming work");
        renames = outcome.renames;
    }
    let rename_map: std::collections::HashMap<PathBuf, PathBuf> = renames.into_iter().collect();

    // ---- PAR stage ---------------------------------------------------------
    let par_tool = Par2Tool {
        cmd: cfg.par2_cmd.clone(),
        timeout: cfg.tool_timeout,
    };
    let partials: Vec<_> =
        job.files
            .iter()
            .filter(|file| {
                !file.paused
                    && !file.is_par2
                    && (job.file_needs_repair(file.id)
                        || file.segments.iter().any(|segment| {
                            matches!(segment.state, nzbd_types::SegmentState::Failed)
                        }))
            })
            .map(|file| {
                (
                    dir.join(format!(".runner-file-{}.part", file.id.0)),
                    file.filename.clone(),
                )
            })
            .collect();
    let mut restored = std::collections::HashSet::new();
    let mut par_ok = true;
    let mut par_did_repair = !from.includes(RestartPoint::Verify)
        && job.stages.iter().any(|s| s.stage == PostStage::ParRepair);
    // Names the par2 set vouches for: proven correct, off-limits to the
    // heuristic deobfuscation pass at the end.
    let mut par2_names: std::collections::HashSet<String> = Default::default();
    if from.includes(RestartPoint::Cleanup) {
        let control = crate::attempt::current().expect("processing control");
        let scan_dir = dir.clone();
        let sets = control
            .blocking(move |_| par2::load_sets(&scan_dir))
            .await?;
        for set in sets {
            par2_names.extend(set.files.iter().filter_map(|f| {
                set.root
                    .join(&f.name)
                    .strip_prefix(&dir)
                    .ok()
                    .map(|path| path.to_string_lossy().into_owned())
            }));
            if from.includes(RestartPoint::Verify) {
                stages.enter(PostStage::ParVerify).await;
                let quick = par2::quick_verify(&set, &evidence_of(&job, &dir, &rename_map));
                if quick == VerifyResult::Intact && partials.is_empty() {
                    tracing::info!(job = job_id.0, "par quick-verify: intact (no data re-read)");
                } else if set.main_path.is_some() {
                    let repaired = repair_isolated_loop(
                        engine,
                        cfg,
                        &par_tool,
                        &mut stages,
                        job_id,
                        set,
                        &partials,
                        &mut restored,
                    )
                    .await?;
                    par_ok &= repaired;
                    par_did_repair |= repaired;
                } else {
                    par_ok = false;
                }
            }
        }
    }

    if from.includes(RestartPoint::Verify) {
        par_ok &= partials.iter().all(|(path, _)| restored.contains(path));
    }
    if !par_ok {
        tracing::warn!(
            job = job_id.0,
            "missing data could not be reconstructed from available PAR2 recovery files"
        );
    }
    // Damaged archive headers are not naming evidence. Repair first, then
    // restore archive names using the verified files.
    if from.includes(RestartPoint::Unpack) && unpack_enabled && par_ok {
        stages.enter(PostStage::RarRename).await;
        let archive_dir = dir.clone();
        let inventory = engine.artifacts();
        crate::attempt::current()
            .expect("processing control")
            .blocking(move |_| rar_rename_owned(&archive_dir, Some((&inventory, job_id.0))))
            .await?;
    }

    // ---- UNPACK stage ------------------------------------------------------
    let mut unpack_ok = true;
    let mut consumed_archive_paths = std::collections::HashSet::new();
    let mut terminal_unusable_archive_paths = std::collections::HashSet::new();
    let mut password_required_archive_paths = std::collections::HashSet::new();
    let mut unpacked_any = !from.includes(RestartPoint::Unpack)
        && job.stages.iter().any(|s| s.stage == PostStage::Unpack);
    if unpack_enabled && par_ok && from.includes(RestartPoint::Unpack) {
        let archives = detect_archives(&dir);
        if !archives.is_empty() {
            stages.enter(PostStage::Unpack).await;
            let ex = Extractors {
                unrar_cmd: cfg.unrar_cmd.clone(),
                sevenzip_cmd: cfg.sevenzip_cmd.clone(),
                timeout: cfg.tool_timeout,
            };
            let password = job
                .params
                .iter()
                .find(|(k, _)| k == "*Unpack:Password")
                .map(|(_, v)| v.as_str());
            // A retry that restarts before unpack meets volumes the final
            // deobfuscation pass already renamed. The extraction workspace is
            // keyed by archive path, so the renamed set would look new and be
            // extracted a second time. The custody journals say which exact
            // files a successful extraction already consumed.
            let already_consumed = {
                let inventory = engine.artifacts();
                crate::attempt::current()
                    .expect("processing control")
                    .blocking(move |control| {
                        inventory
                            .verified_custody_evidence(job_id.0, || control.checkpoint())
                            .map(|evidence| evidence.consumed_inputs)
                            .map_err(PostError::from)
                    })
                    .await?
            };
            for (archive, kind) in &archives {
                use sha2::{Digest, Sha256};
                let token = format!("{:x}", Sha256::digest(archive.to_string_lossy().as_bytes()));
                let archive_inputs = archive_input_set(&dir, archive, *kind);
                let relative_inputs = source_relative(&dir, &archive_inputs);
                if !relative_inputs.is_empty()
                    && relative_inputs.len() == archive_inputs.len()
                    && relative_inputs
                        .iter()
                        .all(|path| already_consumed.contains(path))
                {
                    unpacked_any = true;
                    consumed_archive_paths.extend(relative_inputs);
                    continue;
                }
                let mut workspace = engine
                    .artifacts()
                    .workspace(job_id.0, "extract", &token)
                    .map_err(|e| PostError::Subprocess(e.to_string()))?;
                engine
                    .artifacts()
                    .bind_extraction_inputs(&mut workspace, &archive_inputs)
                    .map_err(|e| PostError::Subprocess(e.to_string()))?;
                if engine
                    .artifacts()
                    .extraction_published(&workspace)
                    .map_err(|e| PostError::Subprocess(e.to_string()))?
                {
                    unpacked_any = true;
                    consumed_archive_paths.extend(workspace.input_paths.iter().cloned());
                    continue;
                }
                let _capacity = nzbd_state::capacity::reserve(&dir, job.totals.size)?;
                let _workspace_capacity =
                    nzbd_state::capacity::reserve(&workspace.scratch.path, job.totals.size)?;
                let attempt = engine
                    .artifacts()
                    .begin_workspace_attempt(&mut workspace)
                    .map_err(|e| PostError::Subprocess(e.to_string()))?;
                let mut staging = attempt.join("output");
                // Extraction is fenced: everything lands in the lease's
                // staging dir and is renamed into place only on success
                // with the lease still live (double-unpack can't happen).
                std::fs::create_dir_all(&staging)?;
                let mut r = ex.extract(archive, *kind, &staging, password).await?;
                staging = r.output_dir.clone();
                if r.disk_space_error {
                    let cause = if r.quota_error {
                        HoldCause::Quota
                    } else {
                        HoldCause::Capacity
                    };
                    let detail = r
                        .attempts
                        .iter()
                        .map(|a| format!("{} exit {}: {}", a.executable, a.exit_code, a.diagnostic))
                        .collect::<Vec<_>>()
                        .join("; ");
                    let _ = engine.hold_job(job_id, cause, "unpack", &detail).await;
                    return Err(PostError::Held);
                }
                if r.retryable_error {
                    let detail = r
                        .attempts
                        .iter()
                        .map(|attempt| attempt.diagnostic.as_str())
                        .collect::<Vec<_>>()
                        .join("; ");
                    engine
                        .artifacts()
                        .abandon_workspace(&workspace)
                        .map_err(PostError::from)?;
                    // A retryable extractor failure retries on its own only
                    // when the volume stopped answering; re-running an
                    // extractor on the same bytes forever is not recovery.
                    if nzbd_engine::stderr_hold_cause(&detail)
                        == Some(HoldCause::StorageUnavailable)
                    {
                        let _ = engine
                            .hold_job(job_id, HoldCause::StorageUnavailable, "unpack", &detail)
                            .await;
                        return Err(PostError::Held);
                    }
                    // Contract §2.2: an extractor that failed on the bytes,
                    // not the disk, is `unpack` — recorded by the attempt's
                    // verdict (stage `unpack`), which reports whether the
                    // owner took it.
                    return Err(PostError::Subprocess(detail));
                }
                if !r.success && !par_did_repair && par_ok {
                    // The unpack↔repair loop: a broken archive that quick
                    // verification couldn't see; force a repair and retry once.
                    if let Some(set) = par2::load_dir(&dir)? {
                        if let Some(main) = set.main_path.clone() {
                            tracing::warn!(
                                job = job_id.0,
                                "unpack failed; forcing par repair + retry"
                            );
                            stages.enter(PostStage::ParRepair).await;
                            engine
                                .artifacts()
                                .abandon_workspace(&workspace)
                                .map_err(|e| PostError::Subprocess(e.to_string()))?;
                            if repair_loop(engine, cfg, &par_tool, &mut stages, job_id, &main)
                                .await?
                            {
                                par_did_repair = true;
                                stages.enter(PostStage::Unpack).await;
                                workspace = engine
                                    .artifacts()
                                    .workspace(job_id.0, "extract", &token)
                                    .map_err(|e| PostError::Subprocess(e.to_string()))?;
                                let attempt = engine
                                    .artifacts()
                                    .begin_workspace_attempt(&mut workspace)
                                    .map_err(|e| PostError::Subprocess(e.to_string()))?;
                                staging = attempt.join("output");
                                r = ex.extract(archive, *kind, &staging, password).await?;
                                staging = r.output_dir.clone();
                            }
                        }
                    }
                }
                if r.disk_space_error {
                    let cause = if r.quota_error {
                        HoldCause::Quota
                    } else {
                        HoldCause::Capacity
                    };
                    let _ = engine
                        .hold_job(job_id, cause, "unpack", "extractor retry exhausted storage")
                        .await;
                    return Err(PostError::Held);
                }
                if r.retryable_error {
                    let detail = r
                        .attempts
                        .iter()
                        .map(|attempt| attempt.diagnostic.as_str())
                        .collect::<Vec<_>>()
                        .join("; ");
                    engine
                        .artifacts()
                        .abandon_workspace(&workspace)
                        .map_err(PostError::from)?;
                    // A retryable extractor failure retries on its own only
                    // when the volume stopped answering; re-running an
                    // extractor on the same bytes forever is not recovery.
                    if nzbd_engine::stderr_hold_cause(&detail)
                        == Some(HoldCause::StorageUnavailable)
                    {
                        let _ = engine
                            .hold_job(job_id, HoldCause::StorageUnavailable, "unpack", &detail)
                            .await;
                        return Err(PostError::Held);
                    }
                    // Contract §2.2: an extractor that failed on the bytes,
                    // not the disk, is `unpack` — recorded by the attempt's
                    // verdict (stage `unpack`), which reports whether the
                    // owner took it.
                    return Err(PostError::Subprocess(detail));
                }
                if r.password_error {
                    engine
                        .artifacts()
                        .abandon_workspace(&workspace)
                        .map_err(|e| PostError::Subprocess(e.to_string()))?;
                    let _ = engine
                        .hold_job(
                            job_id,
                            HoldCause::PasswordRequired,
                            "unpack",
                            "encrypted archive needs a corrected password; source files are retained",
                        )
                        .await;
                    return Err(PostError::Held);
                }
                // "It said OK" is not the same as "it read the whole set".
                // Checked BEFORE the commit, so a short extraction is thrown
                // away with the staging dir instead of being renamed over the
                // top of the volumes it failed to read.
                let short = if r.success {
                    crate::tools::unpack_shortfall(&dir, archive, &staging)
                } else {
                    None
                };
                if let Some(why) = &short {
                    tracing::error!(
                        job = job_id.0,
                        archive = %archive.display(),
                        reason = %why,
                        "unpack claimed success without consuming the volume set"
                    );
                }
                if r.success && short.is_none() {
                    if !(ctx.commit_ok)() {
                        return Err(PostError::Subprocess(
                            "pp lease lost before commit; workspace retained".into(),
                        ));
                    }
                    let output = staging.clone();
                    let source = dir.clone();
                    let inventory = engine.artifacts();
                    let completed = workspace.clone();
                    crate::attempt::current()
                        .expect("processing control")
                        .blocking(move |_| {
                            commit_staging(&output, &source)?;
                            inventory
                                .finish_extraction(&completed, &output)
                                .map_err(|e| PostError::Subprocess(e.to_string()))
                        })
                        .await?;
                    unpacked_any = true;
                    consumed_archive_paths.extend(workspace.input_paths.iter().cloned());
                } else {
                    tracing::warn!(
                        job = job_id.0,
                        archive = %archive.display(),
                        password_error = r.password_error,
                        "unpack failed"
                    );
                    engine
                        .artifacts()
                        .abandon_workspace(&workspace)
                        .map_err(|e| PostError::Subprocess(e.to_string()))?;
                    unpack_ok = false;
                    if r.password_error {
                        password_required_archive_paths
                            .extend(workspace.input_paths.iter().cloned());
                    } else {
                        terminal_unusable_archive_paths
                            .extend(workspace.input_paths.iter().cloned());
                    }
                }
                // Retained workspace belongs to the operation journal.
            }
        }
    }

    // ---- CLEANUP stage -----------------------------------------------------
    if from.includes(RestartPoint::Cleanup) && cfg.cleanup && par_ok && unpack_ok && unpacked_any {
        stages.enter(PostStage::Cleanup).await;
        tracing::info!(
            job = job_id.0,
            "archive inputs retained pending manifest retirement"
        );
    }

    // ---- DEOBFUSCATE stage -------------------------------------------------
    // Exact PAR naming evidence outranks this final basename heuristic.
    // Archives may contain additional naming metadata; this pass does not
    // establish that such evidence is absent. Scripts run after this, so
    // they see the final names. Discrete status: the queue shows the
    // PostUnpackRename stage (compat: "RENAMING") while the pass runs, and
    // the applied renames are recorded on the job as `Deobfuscate:*`
    // parameters, which persist into history.
    let mut deobfuscated: Vec<(PathBuf, PathBuf)> = Vec::new();
    if from.includes(RestartPoint::Cleanup) && cfg.deobfuscate_final && par_ok && unpack_ok {
        stages.enter(PostStage::PostUnpackRename).await;
        deobfuscated = crate::deobfuscate::restore_media_extensions_owned(
            &dir,
            &par2_names,
            Some((&engine.artifacts(), job_id.0)),
        )?;
        deobfuscated.extend(crate::deobfuscate::deobfuscate_dir_owned(
            &dir,
            job.params
                .iter()
                .find(|(key, _)| key == "*Cluster:original-dir")
                .map_or(sanitized.as_str(), |(_, name)| name.as_str()),
            &par2_names,
            Some((&engine.artifacts(), job_id.0)),
        )?);
        for (from, to) in &deobfuscated {
            tracing::info!(
                job = job_id.0,
                from = %from.display(),
                to = %to.display(),
                "deobfuscate: renamed"
            );
        }
        if !deobfuscated.is_empty() {
            tracing::info!(
                job = job_id.0,
                count = deobfuscated.len(),
                "deobfuscate: pass applied renames"
            );
        }
    }

    // ---- MOVE stage --------------------------------------------------------
    // `[[category]] dest_dir` finally means something. The download landed
    // under the global destination (that is where the engine writes); if
    // the category names another root, the finished folder moves there
    // now, before scripts run and before anything is reported — so the
    // path in the event, in history, and in compat `FinalDir` is the path
    // the files are actually at. Moving earlier would fight the unpack
    // staging; moving later would report a path that is about to change.
    if from.includes(RestartPoint::Move) && par_ok && unpack_ok && !job.lifecycle_eligible() {
        if let Some(target_root) = rule
            .as_ref()
            .and_then(|r| r.dest_dir.as_ref())
            .or(cfg.completed_dir.as_ref())
        {
            // Artifact generation is immutable across retries and unique
            // across restored/reused queue IDs, so same-title publications
            // never share a destination path.
            let target_name = if job.delivery_lifecycle {
                let generation = engine
                    .artifacts()
                    .for_job(job_id.0)
                    .map_err(|e| PostError::Subprocess(e.to_string()))?
                    .ok_or_else(|| PostError::Subprocess("job artifact missing".into()))?
                    .generation;
                format!("{sanitized}.runner-{generation}")
            } else {
                sanitized.clone()
            };
            let target = target_root.join(target_name);
            if target != dir && dir.exists() {
                let _ = std::fs::remove_dir_all(&staging); // never move the lease's scratch
                stages.enter(PostStage::Move).await;
                if !(ctx.commit_ok)() {
                    return Err(PostError::Subprocess("pp lease lost before move".into()));
                }
                let inventory = engine.artifacts();
                let to = target.clone();
                let delivery_eligible = job.delivery_lifecycle;
                let moved = crate::attempt::current()
                    .expect("processing control")
                    .blocking(move |control| {
                        let result = if delivery_eligible {
                            inventory.relocate_for_delivery_checked(job_id.0, &to, &|| {
                                control.checkpoint()
                            })
                        } else {
                            inventory.relocate_checked(job_id.0, &to, &|| control.checkpoint())
                        };
                        result.map_err(|e| PostError::Subprocess(e.to_string()))
                    })
                    .await;
                stages.finish().await;
                match moved {
                    Ok(result) => {
                        tracing::info!(
                            job = job_id.0,
                            category = job.category.as_deref().unwrap_or(""),
                            to = %target.display(),
                            "moved to the category destination"
                        );
                        dir = result.published_path;
                    }
                    Err(e) => {
                        return Err(PostError::Subprocess(format!(
                            "publish completed payload to {}: {e}",
                            target.display()
                        )));
                    }
                }
            }
        }
    }

    // ---- SCRIPT stage ------------------------------------------------------
    let mut script_ok = true;
    let mut script_receipts = Vec::new();
    let mut final_dir = dir.to_string_lossy().into_owned();
    if let Some(scripts_dir) = &cfg.scripts_dir {
        let scripts = select_scripts(
            discover(scripts_dir),
            rule.as_ref()
                .map(|r| r.extensions.as_slice())
                .unwrap_or(&[]),
        );
        if !scripts.is_empty() {
            stages.enter(PostStage::Script).await;
            let host = ScriptHost {
                timeout: cfg.script_timeout,
            };
            let base_env = script_env(&job, &dir, par_ok, par_did_repair, unpack_ok, unpacked_any);
            for script in scripts {
                let script_name = script
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|| script.to_string_lossy().into_owned());
                let mut env = base_env.clone();
                env.extend(ctx.extra_env.clone());
                if let Some(prefix) = &ctx.script_receipt_prefix {
                    use sha2::{Digest, Sha256};
                    let script_key = format!("{:x}", Sha256::digest(script_name.as_bytes()));
                    env.push((
                        "NZBCLUSTER_SCRIPT_RECEIPT".into(),
                        format!("{prefix}{script_key}"),
                    ));
                }
                if let Some(receipt) = &ctx.script_receipt {
                    match receipt(ScriptReceiptAction::Begin {
                        script: script_name.clone(),
                    })
                    .await
                    {
                        Ok(ScriptReceiptDecision::Run) => {}
                        Ok(ScriptReceiptDecision::AlreadyDone) => {
                            script_receipts.push((
                                format!("ClusterScript:{script_name}"),
                                "already-done".into(),
                            ));
                            continue;
                        }
                        Ok(ScriptReceiptDecision::Ambiguous) | Err(_) => {
                            script_receipts.push((
                                format!("ClusterScript:{script_name}"),
                                "unknown-not-replayed".into(),
                            ));
                            script_ok = false;
                            continue;
                        }
                    }
                }
                match host.run(&script, &dir, &env).await {
                    Ok(out) => {
                        for (k, v) in &out.commands {
                            if k == "FINALDIR" || k == "DIRECTORY" {
                                final_dir = v.clone();
                            }
                        }
                        match out.exit_code {
                            crate::script_exit::SUCCESS | crate::script_exit::NONE => {}
                            crate::script_exit::PAR_CHECK => {
                                if let Some(set) = par2::load_dir(&dir)? {
                                    if let Some(main) = set.main_path {
                                        let _ = repair_loop(
                                            engine,
                                            cfg,
                                            &par_tool,
                                            &mut stages,
                                            job_id,
                                            &main,
                                        )
                                        .await;
                                    }
                                }
                            }
                            _ => script_ok = false,
                        }
                    }
                    Err(e) => {
                        tracing::error!(job = job_id.0, error = %e, "script failed to run");
                        if job.lifecycle_eligible() && matches!(&e, PostError::ToolMissing(_)) {
                            engine
                                .hold_job(job_id, HoldCause::MissingTool, "script", &e.to_string())
                                .await
                                .map_err(|hold_error| {
                                    PostError::Subprocess(format!(
                                        "persist missing script tool hold: {hold_error}"
                                    ))
                                })?;
                            return Err(PostError::Held);
                        }
                        script_ok = false;
                    }
                }
                if let Some(receipt) = &ctx.script_receipt {
                    match receipt(ScriptReceiptAction::Finish {
                        script: script_name.clone(),
                    })
                    .await
                    {
                        Ok(ScriptReceiptDecision::AlreadyDone) => script_receipts
                            .push((format!("ClusterScript:{script_name}"), "done".into())),
                        _ => {
                            script_receipts.push((
                                format!("ClusterScript:{script_name}"),
                                "unknown-after-run".into(),
                            ));
                            script_ok = false;
                        }
                    }
                }
            }
        }
    }

    // ---- finalize ----------------------------------------------------------
    let mut outcome = if !par_ok {
        PpFinal::ParFailure
    } else if !unpack_ok {
        PpFinal::UnpackFailure
    } else if !script_ok {
        PpFinal::ScriptFailure
    } else {
        PpFinal::Success
    };

    if !(ctx.commit_ok)() {
        return Err(PostError::Subprocess(
            "pp lease lost before finalize".into(),
        ));
    }
    // Repair, extraction and scripts have all returned. Join their actual
    // workers before terminal disposition can move or delete the payload.
    if job.lifecycle_eligible() {
        stages.enter(PostStage::PublicationVerify).await;
    }
    if let Some(control) = crate::attempt::current() {
        control.finish_filesystem_work().await?;
    }
    // Publication is allowed only for bytes with positive integrity evidence.
    // Successful extraction journals prove their outputs; directly downloaded
    // files require both a completed writer CRC and a fresh CRC over the
    // post-script source bytes.
    let (journal_inputs, extraction_outputs, verified_direct, probed_payload_paths) =
        if job.lifecycle_eligible() {
            let inventory = engine.artifacts();
            // The active artifact is intentionally only an allocation record
            // until this quiescence boundary. Refresh its file identities now so
            // direct-media CRC proof is bound to the files actually on disk.
            let source = inventory
                .refresh_for_delivery(job_id.0)
                .map_err(PostError::from)?;
            let evidence_inventory = inventory.clone();
            let job = job.clone();
            crate::attempt::current()
                .expect("processing control")
                .blocking(move |control| {
                    // One pass over all custody journals: the final deobfuscation
                    // pass above renames extraction outputs, and only the rename
                    // journal can carry their extraction proof to the new name.
                    let evidence = evidence_inventory
                        .verified_custody_evidence(job_id.0, || control.checkpoint())
                        .map_err(PostError::from)?;
                    let direct = verified_direct_payload_paths_excluding(
                        &job,
                        &source,
                        &evidence.restored,
                        &evidence.consumed_inputs,
                        || control.checkpoint(),
                    )?;
                    let verified = evidence
                        .verified_outputs
                        .iter()
                        .chain(direct.iter())
                        .cloned()
                        .collect::<std::collections::HashSet<_>>();
                    let probed = probe_payload_kinds(&source.path, &source.files, &verified);
                    Ok::<_, PostError>((
                        evidence.consumed_inputs,
                        evidence.verified_outputs,
                        direct,
                        probed,
                    ))
                })
                .await?
        } else {
            (
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
            )
        };
    consumed_archive_paths.extend(journal_inputs);
    let verified_payload_paths = extraction_outputs
        .into_iter()
        .chain(verified_direct)
        .collect::<std::collections::HashSet<_>>();
    // A completed download containing only positively disposable inputs is
    // a terminal unusable attempt, rather than an endless publication hold.
    // Recognizable usable inputs still have an explicit review action below.
    let mut unusable_source_reason = None;
    let mut assessed_inputs = None;
    if job.lifecycle_eligible() && outcome == PpFinal::Success {
        if let Some(source) = engine.artifacts().for_job(job_id.0)? {
            // Writer CRCs can prove a readme or sidecar intact without making
            // it publishable. Only a positive selected payload skips the
            // checkpointed diagnostic pass; the selector itself is cheap.
            let (publishable, _) = select_delivery_payload(
                &source.files,
                &consumed_archive_paths,
                &verified_payload_paths,
                &terminal_unusable_archive_paths,
                &Default::default(),
                &probed_payload_paths,
            );
            if publishable.is_empty() {
                let assessment_job = job.clone();
                let assessment_source = source.clone();
                let complete_paths = verified_payload_paths.clone();
                let diagnostics = crate::attempt::current()
                    .expect("processing control")
                    .blocking(move |control| {
                        control.checkpoint()?;
                        assess_file_inputs(&assessment_job, &assessment_source, &complete_paths)
                    })
                    .await?;
                assessed_inputs = Some(diagnostics.clone());
                let (selected, outcomes) = select_delivery_payload(
                    &source.files,
                    &consumed_archive_paths,
                    &verified_payload_paths,
                    &terminal_unusable_archive_paths,
                    &diagnostics,
                    &probed_payload_paths,
                );
                if selected.is_empty() && recovery_reason(&outcomes).is_none() {
                    outcome = PpFinal::UnpackFailure;
                    unusable_source_reason = Some("no publishable payload or verified recovery path remains after bounded assessment".to_string());
                }
            }
        }
    }
    // ---- a terminal failure ------------------------------------------------
    // Contract §2.1: the failure is recorded — terminal row, history row,
    // queue markers, `job_pp_finished` — before anything touches the bytes.
    // Salvage (a `job_publication_ready`) and disposal of the source run
    // afterwards, from the terminal row, in `failure_followup`; neither can
    // delay the fact a consumer needs to pick the next release.
    if outcome != PpFinal::Success {
        stages.finish().await;
        let mut fin = read_post_job(job_id, "read terminal job", || engine.export_job(job_id))
            .await?
            .ok_or_else(|| PostError::Subprocess("job vanished before its terminal".into()))?;
        record_deobfuscation(&mut fin.params, &deobfuscated);
        fin.params.extend(script_receipts);
        let (reason, stage, message) = pp_failure(outcome, unusable_source_reason.as_deref());
        // Review #280-7: the failed source's assessment (`recoverable` /
        // `unusable`) is what the file lifecycle (Keep-off disposal), the
        // cluster's raw-copy cleanup and disposal below read to decide
        // whether these bytes are redundant. It is recorded here, with the
        // terminal, as before v2 — only the disposal that follows moved.
        if job.lifecycle_eligible() {
            if let Err(error) = record_failure_assessment(
                engine,
                &job,
                &consumed_archive_paths,
                &verified_payload_paths,
                &terminal_unusable_archive_paths,
                &password_required_archive_paths,
                &probed_payload_paths,
                assessed_inputs.as_ref(),
            )
            .await
            {
                tracing::warn!(job = job_id.0, %error, "failed-source assessment not recorded; disposal treats the source as unknown");
            }
        }
        if !ctx.publish_history {
            // A cluster worker reports the outcome to the leader, which owns
            // the terminal record and history.
            fin.params.retain(|(key, _)| key != PP_DONE_PARAM);
            fin.params
                .push((PP_DONE_PARAM.into(), reason.history_status().into()));
            fin.params.push(("Failure:Stage".into(), stage.into()));
            fin.params
                .push(("Failure:Reason".into(), reason.as_str().into()));
            fin.params.push(("Failure:Message".into(), message.clone()));
            fin.status = JobStatus::Failed;
            if !engine
                .import_worker_job_if_present(fin)
                .await
                .map_err(|source| PostError::Engine {
                    operation: "remote terminal stamp",
                    source,
                })?
            {
                return Err(PostError::Subprocess(
                    "remote terminal stamp was not committed".into(),
                ));
            }
            stages.close();
            return Ok(outcome);
        }
        let terminal = nzbd_engine::terminal::failed_terminal(&fin, reason, None, stage, &message);
        record_pp_terminal(engine, history, terminal).await?;
        stages.close();
        return Ok(outcome);
    }

    // ---- success -----------------------------------------------------------
    // Close the last stage span BEFORE the export below reads the job:
    // the timeline that lands in history has to be complete, and the
    // final stage is the one an operator looks at first.
    stages.finish().await;
    if let Some(mut fin) =
        read_post_job(job_id, "read terminal job", || engine.export_job(job_id)).await?
    {
        record_deobfuscation(&mut fin.params, &deobfuscated);
        fin.params.extend(script_receipts);
        let mut final_dir = Some(final_dir);
        // Curator receives a separately journaled selected-payload tree only
        // after scripts have finished mutating the Runner-owned source.
        if job.lifecycle_eligible() {
            let inventory = engine.artifacts();
            // Contract §2.5: a publication move left open by a crash or an
            // I/O error is finished or unwound before deciding whether it
            // committed — never selected from again.
            let reconcile_inventory = inventory.clone();
            crate::attempt::current()
                .expect("processing control")
                .blocking(move |_| {
                    reconcile_inventory
                        .reconcile_job_intents(job_id.0)
                        .map_err(PostError::from)
                })
                .await?;
            let current = inventory
                .for_job(job_id.0)
                .map_err(|e| PostError::Subprocess(e.to_string()))?
                .ok_or_else(|| PostError::Subprocess("job artifact missing".into()))?;
            let prior_outcomes = if job.delivery_lifecycle {
                inventory
                    .delivery_file_outcomes(&current.id, &current.generation)
                    .map_err(|e| PostError::Subprocess(e.to_string()))?
            } else {
                Vec::new()
            };
            let committed_selected_path = if job.delivery_lifecycle {
                None
            } else {
                inventory
                    .selected_publication_for_job(job_id.0)
                    .map_err(|e| PostError::Subprocess(e.to_string()))?
            };
            if !prior_outcomes.is_empty() || committed_selected_path.is_some() {
                // The selected publication is already durable. This is the
                // restart path for a crash before the READY transaction.
                final_dir = Some(
                    committed_selected_path
                        .unwrap_or_else(|| current.path.clone())
                        .to_string_lossy()
                        .into_owned(),
                );
            } else {
                let source = inventory
                    .refresh_for_delivery(job_id.0)
                    .map_err(PostError::from)?;
                let mut diagnostics = match &assessed_inputs {
                    Some(diagnostics) => diagnostics.clone(),
                    None => publication_diagnostics(
                        &job,
                        &source,
                        &consumed_archive_paths,
                        &verified_payload_paths,
                        &terminal_unusable_archive_paths,
                        &probed_payload_paths,
                    )?,
                };
                for path in &password_required_archive_paths {
                    diagnostics.insert(path.clone(), FileAssessment::Repairable(
                        "archive requires a password; set *Unpack:Password and retry extraction".into(),
                    ));
                }
                let (selected_files, file_outcomes) = select_delivery_payload(
                    &source.files,
                    &consumed_archive_paths,
                    &verified_payload_paths,
                    &terminal_unusable_archive_paths,
                    &diagnostics,
                    &probed_payload_paths,
                );
                if selected_files.is_empty() {
                    return Err(PostError::NothingPublishable(unpublishable_summary(
                        &file_outcomes,
                    )));
                }
                let configured_root = rule
                    .as_ref()
                    .and_then(|r| r.dest_dir.as_ref())
                    .or(cfg.completed_dir.as_ref());
                let target_root = configured_root
                    .cloned()
                    .unwrap_or_else(|| source.path.parent().unwrap_or(dest_dir).to_path_buf());
                let publication_name = if ctx.publish_history {
                    sanitized.as_str()
                } else {
                    "private-payload"
                };
                // Contract §1: `<name>.job-<id>.runner-<gen>` — two attempts
                // at the same release can never collide.
                let target = target_root.join(format!(
                    "{publication_name}.job-{}.runner-{}",
                    job_id.0, source.generation
                ));
                stages.enter(PostStage::Move).await;
                if !(ctx.commit_ok)() {
                    return Err(PostError::Subprocess(
                        "pp lease lost before selected publication".into(),
                    ));
                }
                if job.delivery_lifecycle {
                    let Some(mut marker) =
                        read_post_job(job_id, "read publication outcome marker", || {
                            engine.export_job(job_id)
                        })
                        .await?
                    else {
                        return Err(PostError::Subprocess(
                            "job vanished before selected publication".into(),
                        ));
                    };
                    set_param(
                        &mut marker.params,
                        PP_SELECTED_OUTCOME_PARAM,
                        outcome.as_str().into(),
                    );
                    let marker_commit = if ctx.publish_history {
                        engine.import_job_if_present(marker).await
                    } else {
                        engine.import_worker_job_if_present(marker).await
                    };
                    if !marker_commit.map_err(|source| PostError::Engine {
                        operation: "commit publication outcome marker",
                        source,
                    })? {
                        return Err(PostError::Subprocess(
                            "publication outcome was not committed".into(),
                        ));
                    }
                }
                let inventory_for_publish = inventory.clone();
                let target_for_publish = target.clone();
                let delivery_eligible = job.delivery_lifecycle;
                let transfer = job
                    .params
                    .iter()
                    .find(|(key, _)| key == "monarr-transfer")
                    .map(|(_, value)| value.clone());
                let published = crate::attempt::current()
                    .expect("processing control")
                    .blocking(move |control| {
                        inventory_for_publish
                            .publish_selected_with_transfer(
                                job_id.0,
                                &target_for_publish,
                                selected_files,
                                file_outcomes,
                                transfer.as_deref(),
                                delivery_eligible,
                                &|| control.checkpoint(),
                            )
                            .map_err(PostError::from)
                    })
                    .await;
                stages.finish().await;
                let published = published?;
                final_dir = Some(published.published_path.to_string_lossy().into_owned());
            }
        }
        // The selected publication starts after `fin` was exported. Preserve
        // accumulated terminal parameters, but take the closed attempt timeline
        // before either the worker stamp, Delivery, or history freezes it.
        let timing = read_post_job(job_id, "read final timing snapshot", || {
            engine.export_job(job_id)
        })
        .await?
        .ok_or_else(|| PostError::Subprocess("job vanished before final timing snapshot".into()))?;
        fin.stages = timing.stages.clone();
        if let Some(source) = crate::attempt::current().and_then(|control| control.source.clone()) {
            fin.params
                .retain(|(key, _)| key != "Artifact:Id" && key != "Artifact:Generation");
            fin.params.push(("Artifact:Id".into(), source.artifact));
            fin.params
                .push(("Artifact:Generation".into(), source.generation));
        }
        if !ctx.publish_history {
            fin.params.retain(|(key, _)| key != PP_DONE_PARAM);
            fin.params
                .push((PP_DONE_PARAM.into(), outcome.as_str().into()));
            fin.status = JobStatus::Completed;
            if let Some(path) = &final_dir {
                fin.params.push(("*Cluster:final-dir".into(), path.clone()));
            }
            if !engine
                .import_worker_job_if_present(fin)
                .await
                .map_err(|source| PostError::Engine {
                    operation: "remote terminal stamp",
                    source,
                })?
            {
                return Err(PostError::Subprocess(
                    "remote terminal stamp was not committed".into(),
                ));
            }
            stages.close();
            return Ok(outcome);
        }
        let mut pending_delivery: Option<PendingDelivery> = None;
        let mut cross_volume = final_dir.as_deref().is_some_and(|path| {
            use std::os::unix::fs::MetadataExt;
            matches!(
                (std::fs::metadata(path), std::fs::metadata(dest_dir)),
                (Ok(a), Ok(b)) if a.dev() != b.dev()
            )
        });
        if let Some(path) = &final_dir {
            let path = Path::new(path);
            let state = "completed";
            // A script may select an external path, but that is not a
            // relocation or proof that Runner owns the returned bytes. Keep
            // the admitted source associated with the job and its terminal
            // receipt; record the external result independently.
            let inventory = engine.artifacts();
            let final_path = path.to_path_buf();
            let final_root = path.parent().unwrap_or(dest_dir).to_path_buf();
            let final_inventory = inventory.clone();
            let (artifact, external_output) = crate::attempt::current()
                .expect("processing control")
                .blocking(move |control| {
                    let current = final_inventory
                        .for_job(job_id.0)?
                        .ok_or(nzbd_state::artifacts::Error::NotFound)?;
                    if current.path == final_path {
                        return final_inventory
                            .finish(job_id.0, &final_path, &final_root, state)
                            .map(|artifact| (artifact, None))
                            .map_err(PostError::from);
                    }
                    let source = control.source.as_ref().ok_or_else(|| {
                        PostError::Subprocess(
                            "external output lacks its admitted source generation".into(),
                        )
                    })?;
                    if current.id != source.artifact
                        || current.generation != source.generation
                        || current.job != Some(job_id.0)
                    {
                        return Err(PostError::Subprocess(
                            "external output source association changed".into(),
                        ));
                    }
                    let output =
                        final_inventory.observe_external_output(&final_root, &final_path)?;
                    control.checkpoint()?;
                    // Preserve the operator's Keep choice. Retained sources do
                    // not expire automatically; adding Keep would also prevent
                    // retirement of independently verified private scratch.
                    let retained = final_inventory.finish(
                        job_id.0,
                        &current.path,
                        &current.root,
                        "retained",
                    )?;
                    Ok((retained, Some(output)))
                })
                .await?;
            set_param(&mut fin.params, "Artifact:Id", artifact.id.clone());
            set_param(
                &mut fin.params,
                "Artifact:Generation",
                artifact.generation.clone(),
            );
            if let Some(output) = external_output {
                set_param(&mut fin.params, "Artifact:ExternalOutputId", output.id);
                set_param(
                    &mut fin.params,
                    "Artifact:ExternalOutputGeneration",
                    output.generation,
                );
            }
            if job.delivery_lifecycle && artifact.owned && artifact.path == path {
                let request_token = job
                    .params
                    .iter()
                    .find(|(key, _)| key == "monarr-transfer")
                    .map(|(_, value)| value.as_str());
                let request_token = request_token.map(str::to_owned);
                let terminal_outcome = outcome.as_str().to_owned();
                let failure_stage = fin
                    .params
                    .iter()
                    .find(|(key, _)| key == "Failure:Stage")
                    .map(|(_, value)| value.clone());
                let failure_cause = fin
                    .params
                    .iter()
                    .find(|(key, _)| key == "Failure:Reason" || key == "Failure:Files")
                    .map(|(_, value)| value.clone());
                let runner_stages = delivery_stage_snapshot(&inventory, &timing);
                let lifecycle_timing = nzbd_engine::frozen_lifecycle_timing(&timing);
                // Contract §2.1 / IMPL §5.6: read and digest the publication
                // now, record the `completed` terminal carrying it, and only
                // then create the delivery row and retire the source.
                let prepare_inventory = inventory.clone();
                let prepared = crate::attempt::current()
                    .expect("processing control")
                    .blocking(move |_| {
                        prepare_inventory
                            .prepare_delivery(job_id.0, true)
                            .map_err(PostError::from)
                    })
                    .await?;
                let delivery_id = format!("{}:{}", artifact.id, prepared.generation());
                for (key, value) in [
                    ("Delivery:Id", delivery_id.clone()),
                    ("Delivery:Custody", "curator".into()),
                    (
                        "Delivery:SourceArtifactId",
                        prepared.source_artifact_id().into(),
                    ),
                    (
                        "Delivery:SourceGeneration",
                        prepared.source_generation().into(),
                    ),
                    ("Delivery:Path", prepared.publication_path.clone()),
                    ("Delivery:Generation", prepared.generation().into()),
                    ("Delivery:ManifestDigest", prepared.manifest_digest.clone()),
                    ("Delivery:Files", prepared.files.len().to_string()),
                    ("Delivery:SourceCleanup", "pending".into()),
                ] {
                    fin.params.push((key.into(), value));
                }
                cross_volume = prepared.cross_volume;
                pending_delivery = Some(PendingDelivery {
                    prepared,
                    request_token,
                    terminal_outcome,
                    failure_stage,
                    failure_cause,
                    runner_stages,
                    lifecycle_timing,
                });
            }
        }
        if !fin.params.iter().any(|(k, _)| k == "Artifact:Id") {
            if let Ok(Some(a)) = engine.artifacts().for_job(job_id.0) {
                fin.params.push(("Artifact:Id".into(), a.id));
            }
        }
        // Park the NZB with the entry, so a finished job can be put back.
        //
        // Until now `spool_nzb` had exactly ONE caller — the delete path —
        // so a job that completed normally kept no requeue source and its
        // history row reported `can_requeue: false` forever. "That nzb for
        // that job can be downloaded again if necessary" (field report
        // 2026-07-29) was simply not true of any job that succeeded.
        // Gzipped, and reaped with its entry by retention, so keeping one
        // per job stays bounded.
        if !fin.files.is_empty() {
            let h = history.clone();
            let nzb = nzbd_engine::queue::job_to_nzb(&fin).into_bytes();
            let _ = tokio::task::spawn_blocking(move || {
                if let Err(e) = h.spool_nzb(job_id, &nzb) {
                    // Not worth failing the job over: the files are
                    // downloaded and post-processed either way. It costs
                    // the undo, and it says so.
                    tracing::warn!(job = job_id.0, error = %e,
                        "could not park the finished job's NZB — it will not be re-downloadable from history");
                }
            })
            .await;
        }
        // Contract §2.1: the terminal row, the history row, the queue stamp
        // and `job_pp_finished` — in that order, replayed from the row after
        // a crash. The publication was committed above; the event carries it.
        let publication = success_publication(&fin, final_dir.as_deref(), cross_volume);
        let live = read_post_job(job_id, "read live terminal row", || {
            engine.export_job(job_id)
        })
        .await?
        .ok_or_else(|| PostError::Subprocess("job vanished before its terminal".into()))?;
        let mut markers = fin
            .params
            .iter()
            .filter(|(key, value)| {
                !live
                    .params
                    .iter()
                    .any(|(live_key, live_value)| live_key == key && live_value == value)
            })
            .cloned()
            .collect::<Vec<_>>();
        markers.retain(|(key, _)| key != PP_DONE_PARAM);
        markers.push((PP_DONE_PARAM.into(), outcome.as_str().into()));
        let terminal = nzbd_engine::terminal::terminal_from_job(
            &fin,
            nzbd_types::TerminalOutcome::Completed,
            outcome.as_str(),
            final_dir.clone(),
            markers,
            nzbd_engine::terminal::TerminalFacts {
                reason: None,
                fetch_cause: None,
                stage: None,
                message: None,
                blames_release: false,
                salvage: nzbd_state::artifacts::SalvageState::None,
                keep_on_failure: nzbd_engine::terminal::keeps_failed_source(&fin),
                publication,
            },
            now(),
        );
        let recorded = record_pp_terminal(engine, history, terminal).await?;
        let history_seq = recorded
            .history_seq
            .ok_or_else(|| PostError::Subprocess("terminal recorded without history".into()))?;
        if let Some(pending) = pending_delivery {
            commit_delivery(engine, job_id, pending).await?;
        }
        let completion = crate::attempt::current()
            .and_then(|control| control.source.clone())
            .map(|source| nzbd_state::artifacts::StagedCompletion {
                source,
                outcome: outcome.as_str().into(),
                proof: nzbd_state::artifacts::FinalizationProof::Local {
                    history_seq: history_seq as u64,
                },
            });
        if let Some(completion) = completion {
            let stamped = completion.clone();
            let inventory = engine.artifacts();
            let control = crate::attempt::current().expect("processing control");
            control
                .blocking(move |_| {
                    inventory
                        .stage_workspace_finalization(
                            &stamped.source,
                            &stamped.outcome,
                            &stamped.proof,
                        )
                        .and_then(|()| inventory.confirm_stamped_finalization(&stamped))
                        .map_err(|e| PostError::Subprocess(e.to_string()))
                })
                .await?;
            *control.completion.lock().unwrap() = Some(crate::attempt::Completion {
                source: completion.source,
                outcome: completion.outcome,
                proof: completion.proof,
            });
        }
        stages.close();
    }
    Ok(outcome)
}

/// The plain (non-`*`) deobfuscation record: it survives into history and
/// the compat `Parameters` array.
fn record_deobfuscation(params: &mut Vec<(String, String)>, deobfuscated: &[(PathBuf, PathBuf)]) {
    if deobfuscated.is_empty() {
        return;
    }
    params.push(("Deobfuscate:Count".into(), deobfuscated.len().to_string()));
    let base = |p: &PathBuf| {
        p.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default()
    };
    let mut list = deobfuscated
        .iter()
        .take(10)
        .map(|(f, t)| format!("{} → {}", base(f), base(t)))
        .collect::<Vec<_>>()
        .join("; ");
    if deobfuscated.len() > 10 {
        list.push_str("; …");
    }
    params.push(("Deobfuscate:Files".into(), list));
}

/// Record whether a failed attempt's source still holds usable payload
/// (`recoverable`: a verified sibling or a repairable input), payload whose
/// integrity is unproven (`unknown`), or nothing (`unusable`), for the
/// lifecycle, cluster cleanup and disposal. Only `unusable` may be deleted.
#[allow(clippy::too_many_arguments)]
async fn record_failure_assessment(
    engine: &EngineHandle,
    job: &Job,
    consumed: &std::collections::HashSet<String>,
    verified: &std::collections::HashSet<String>,
    terminal_unusable: &std::collections::HashSet<String>,
    password_required: &std::collections::HashSet<String>,
    probed: &std::collections::HashSet<String>,
    assessed: Option<&std::collections::HashMap<String, FileAssessment>>,
) -> Result<(), PostError> {
    let inventory = engine.artifacts();
    let Some(source) = inventory.job_source(job.id.0)? else {
        return Ok(());
    };
    let mut diagnostics = match assessed {
        Some(diagnostics) => diagnostics.clone(),
        None => {
            publication_diagnostics(job, &source, consumed, verified, terminal_unusable, probed)?
        }
    };
    for path in password_required {
        diagnostics.insert(
            path.clone(),
            FileAssessment::Repairable(
                "archive requires a password; set *Unpack:Password and retry extraction".into(),
            ),
        );
    }
    let (selected, outcomes) = select_delivery_payload(
        &source.files,
        consumed,
        verified,
        terminal_unusable,
        &diagnostics,
        probed,
    );
    let recovery = recovery_reason(&outcomes);
    let usable = !selected.is_empty() || recovery.is_some();
    // Absent integrity proof is not cleanup authority: payload whose bytes
    // were never verified is `unknown`, which retains like `recoverable`.
    let unproven = outcomes
        .iter()
        .any(|outcome| outcome.outcome == "unverified_review");
    let (disposition, default_reason) = if usable {
        (
            "recoverable",
            "verified sibling payload remains available for publication",
        )
    } else if unproven {
        (
            "unknown",
            "payload integrity is unproven; it is not deletion authority",
        )
    } else {
        (
            "unusable",
            "bounded post-failure assessment found no usable payload or repairable input",
        )
    };
    let mut reason = recovery.unwrap_or_else(|| default_reason.into());
    while reason.len() > 2048 {
        reason.pop();
    }
    inventory.set_failure_assessment(&source.id, &source.generation, disposition, &reason)?;
    Ok(())
}

/// Contract §2.2: the typed reason, stage and message for a PP outcome.
/// A completed run that left nothing publishable is `publish` (parked,
/// never the release's fault), not an unpack failure.
fn pp_failure(
    outcome: PpFinal,
    nothing_publishable: Option<&str>,
) -> (nzbd_types::FailureReason, &'static str, String) {
    use nzbd_types::FailureReason;
    if let Some(reason) = nothing_publishable {
        return (FailureReason::Publish, "publication_verify", reason.into());
    }
    match outcome {
        PpFinal::ParFailure => (
            FailureReason::Par,
            "par_verify",
            "Missing data could not be reconstructed from available PAR2 recovery files".into(),
        ),
        PpFinal::UnpackFailure => (
            FailureReason::Unpack,
            "unpack",
            "the extractor failed on the downloaded archive".into(),
        ),
        PpFinal::ScriptFailure => (
            FailureReason::Script,
            "script",
            "a post-processing script exited non-zero".into(),
        ),
        PpFinal::HealthFailure => (
            FailureReason::Health,
            "health",
            "download health below critical".into(),
        ),
        PpFinal::FetchFailure => (FailureReason::Fetch, "fetch", "NZB fetch failed".into()),
        PpFinal::Success => unreachable!("success is not a failure"),
    }
}

/// Hand a post-processing terminal to the owner and wait for its event.
/// A terminal whose write did not land has already held the job
/// `storage_unavailable` on `persistence` and is replayed from its row, so
/// this attempt ends as handled.
async fn record_pp_terminal(
    engine: &EngineHandle,
    history: &Arc<HistoryDb>,
    terminal: nzbd_state::artifacts::JobTerminal,
) -> Result<nzbd_state::artifacts::JobTerminal, PostError> {
    match engine
        .record_terminal_in(terminal, Some(history.clone()))
        .await
    {
        Ok(Some(row)) => Ok(row),
        Ok(None) => Err(PostError::Held),
        Err(source) => Err(PostError::Engine {
            operation: "record terminal",
            source,
        }),
    }
}

/// The `publication` of a `completed` terminal (contract §2.1): the
/// delivery record's when there is one, else the final directory.
fn success_publication(
    job: &Job,
    final_dir: Option<&str>,
    cross_volume: bool,
) -> Option<nzbd_types::Publication> {
    let param = |key: &str| {
        job.params
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.clone())
    };
    let path = param("Delivery:Path").or_else(|| final_dir.map(str::to_string))?;
    Some(nzbd_types::Publication {
        path,
        generation: param("Delivery:Generation")
            .or_else(|| param("Artifact:Generation"))
            .unwrap_or_default(),
        manifest_digest: param("Delivery:ManifestDigest").unwrap_or_default(),
        files: param("Delivery:Files")
            .and_then(|files| files.parse().ok())
            .unwrap_or(0),
        cross_volume,
    })
}

/// Contract §2.1 crash repair: a `completed` terminal is recorded before its
/// delivery row. If the process stopped in between, the stamped
/// `Delivery:*` markers name the delivery to create; create it from the
/// still-owned publication (refusing if its digest changed) and retire the
/// source. True once nothing is owed.
async fn finish_stamped_delivery(
    engine: &EngineHandle,
    job: JobId,
    terminal: &nzbd_state::artifacts::JobTerminal,
) -> Settle {
    let row = match engine.export_job(job).await {
        Ok(Some(row)) => row,
        // The row is gone: nothing is owed.
        Ok(None) => return Settle::Done,
        // A transient owner state (verification or journal loading): the
        // delivery may still be owed; never retire the row on a non-answer.
        Err(error) if error.is_deferred() => return Settle::Deferred,
        Err(_) => return Settle::Pending,
    };
    // Contract §2.1: the terminal row is authoritative. Its markers name
    // the delivery the publication owes even before replay has stamped them
    // onto the queue row (review #283-1: a restart after the history write).
    let param = |key: &str| {
        terminal
            .markers
            .iter()
            .chain(row.params.iter())
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.clone())
    };
    let Some(delivery_id) = param("Delivery:Id") else {
        return Settle::Done;
    };
    let expected_digest = param("Delivery:ManifestDigest");
    let request_token = nzbd_types::transfer_of(&row.params);
    let terminal_outcome = param(PP_DONE_PARAM).unwrap_or_else(|| "SUCCESS".into());
    let inventory = engine.artifacts();
    let result = tokio::task::spawn_blocking(move || {
        match inventory.delivery(&delivery_id) {
            Ok(_) => {}
            Err(nzbd_state::artifacts::Error::NotFound) => {
                let prepared = inventory.prepare_delivery(job.0, true)?;
                if expected_digest.as_deref() != Some(prepared.manifest_digest.as_str()) {
                    return Err(nzbd_state::artifacts::Error::Conflict(
                        "stamped publication changed before its delivery row".into(),
                    ));
                }
                inventory.commit_prepared_delivery(
                    prepared,
                    request_token.as_deref(),
                    &terminal_outcome,
                    None,
                    None,
                    false,
                    None,
                    None,
                )?;
            }
            Err(error) => return Err(error),
        }
        // Idempotent: a retired or retiring source answers its status.
        let _ = inventory.retire_delivery_source(&delivery_id);
        Ok(())
    })
    .await;
    match result {
        Ok(Ok(())) => Settle::Done,
        Ok(Err(error)) => {
            tracing::warn!(job = job.0, %error, "stamped completion's delivery remains pending");
            Settle::Pending
        }
        Err(_) => Settle::Pending,
    }
}

/// A publication prepared before the terminal, committed after it.
struct PendingDelivery {
    prepared: nzbd_state::artifacts::PreparedDelivery,
    request_token: Option<String>,
    terminal_outcome: String,
    failure_stage: Option<String>,
    failure_cause: Option<String>,
    runner_stages: Option<nzbd_state::artifacts::DeliveryRunnerStages>,
    lifecycle_timing: Option<nzbd_types::LifecycleTiming>,
}

/// After the `completed` terminal: the delivery row (custody to Curator)
/// and the source retirement. A crash before this is finished by
/// [`finish_stamped_delivery`] from the stamped `Delivery:*` markers.
async fn commit_delivery(
    engine: &EngineHandle,
    job_id: JobId,
    pending: PendingDelivery,
) -> Result<(), PostError> {
    let inventory = engine.artifacts();
    let delivery_inventory = inventory.clone();
    let delivery = crate::attempt::current()
        .expect("processing control")
        .blocking(move |_| {
            delivery_inventory
                .commit_prepared_delivery(
                    pending.prepared,
                    pending.request_token.as_deref(),
                    &pending.terminal_outcome,
                    pending.failure_stage.as_deref(),
                    pending.failure_cause.as_deref(),
                    false,
                    pending.runner_stages,
                    pending.lifecycle_timing,
                )
                .map_err(PostError::from)
        })
        .await?;
    let cleanup_id = delivery.delivery_id.clone();
    let cleanup = crate::attempt::current()
        .expect("processing control")
        .blocking(move |_| {
            inventory
                .retire_delivery_source(&cleanup_id)
                .map_err(PostError::from)
        })
        .await;
    if let Err(error) = cleanup {
        tracing::warn!(job = job_id.0, %error, "delivery ready; source cleanup remains pending");
    }
    Ok(())
}

/// Narrow the discovered post-processing scripts to a category's
/// `extensions` list. An empty list means "all of them" — the global
/// behavior, and what every category without the key gets. Matching is on
/// the file name or its stem, case-insensitively, so both
/// `Extensions=Clean.py` and `Extensions=clean` name the same script.
fn select_delivery_payload(
    entries: &[FileEntry],
    consumed_archives: &std::collections::HashSet<String>,
    verified_payload_paths: &std::collections::HashSet<String>,
    terminal_unusable_archives: &std::collections::HashSet<String>,
    diagnostics: &std::collections::HashMap<String, FileAssessment>,
    probed: &std::collections::HashSet<String>,
) -> (Vec<FileEntry>, Vec<DeliveryFileOutcome>) {
    let files: Vec<_> = entries
        .iter()
        .filter(|entry| !entry.identity.directory)
        .collect();
    let mut selected = Vec::new();
    let mut selected_paths = std::collections::HashSet::new();
    let payload_names: Vec<_> = files
        .iter()
        .filter_map(|entry| {
            let path = Path::new(&entry.path);
            let extension = path.extension()?.to_str()?.to_ascii_lowercase();
            let stem = path.file_stem()?.to_str()?.to_ascii_lowercase();
            ((payload_kind(&extension) || probed.contains(&entry.path))
                && verified_payload_paths.contains(&entry.path))
            .then(|| {
                (
                    path.parent()
                        .unwrap_or(Path::new(""))
                        .to_string_lossy()
                        .to_ascii_lowercase(),
                    stem,
                )
            })
        })
        .collect();
    let retained_payload_names: Vec<_> = files
        .iter()
        .filter_map(|entry| {
            let path = Path::new(&entry.path);
            let extension = path.extension()?.to_str()?.to_ascii_lowercase();
            let stem = path.file_stem()?.to_str()?.to_ascii_lowercase();
            ((payload_kind(&extension) || probed.contains(&entry.path))
                && !is_sample_or_proof(path, &stem)
                && !verified_payload_paths.contains(&entry.path)
                && !matches!(
                    diagnostics.get(&entry.path),
                    Some(FileAssessment::Incomplete(_) | FileAssessment::Corrupt(_))
                ))
            .then(|| {
                (
                    path.parent()
                        .unwrap_or(Path::new(""))
                        .to_string_lossy()
                        .to_ascii_lowercase(),
                    stem,
                )
            })
        })
        .collect();
    let mut retained_payload_names = retained_payload_names;
    for (name, assessment) in diagnostics {
        let path = Path::new(name);
        if matches!(assessment, FileAssessment::Repairable(_))
            && path
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| payload_kind(&extension.to_ascii_lowercase()))
            || matches!(assessment, FileAssessment::Repairable(_)) && probed.contains(name)
        {
            retained_payload_names.push((
                path.parent()
                    .unwrap_or(Path::new(""))
                    .to_string_lossy()
                    .to_ascii_lowercase(),
                path.file_stem()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_ascii_lowercase(),
            ));
        }
    }
    let mut classifications = Vec::with_capacity(files.len());

    for entry in &files {
        let path = Path::new(&entry.path);
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        let lower = name.to_ascii_lowercase();
        let extension = path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let excluded = is_sample_or_proof(path, &stem);
        // R3: a fixed extension list first, else what the bytes say (magic
        // number or ffprobe, `probe_payload_kinds`).
        let kind = payload_kind(&extension) || probed.contains(&entry.path);
        if !excluded && kind && verified_payload_paths.contains(&entry.path) {
            selected.push((*entry).clone());
            selected_paths.insert(entry.path.as_str());
            classifications.push((payload_classification(&extension), "published", None));
        } else if !excluded && kind {
            match diagnostics.get(&entry.path) {
                Some(FileAssessment::Repairable(action)) => classifications.push((
                    payload_classification(&extension), "retained_processing", Some(action.as_str()),
                )),
                Some(FileAssessment::Incomplete(action) | FileAssessment::Corrupt(action)) => classifications.push((
                    payload_classification(&extension), "excluded_source_cleanup", Some(action.as_str()),
                )),
                Some(FileAssessment::Recognized(action)) => classifications.push((
                    payload_classification(&extension), "unverified_review",
                    Some(action.as_str()),
                )),
                _ => classifications.push((
                    payload_classification(&extension), "unverified_review",
                    Some("no completed NZB/CRC or verified transform evidence identifies this payload"),
                )),
            }
        } else if !excluded && (is_sidecar_extension(&extension) || is_cover_name(&lower)) {
            let parent = path
                .parent()
                .unwrap_or(Path::new(""))
                .to_string_lossy()
                .to_ascii_lowercase();
            let sidecar_stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            let paired = payload_names.iter().any(|(media_parent, media_stem)| {
                media_parent == &parent
                    && (sidecar_stem == *media_stem
                        || sidecar_stem.starts_with(&format!("{media_stem}.")))
            });
            let folder_art = is_cover_name(&lower)
                && payload_names
                    .iter()
                    .any(|(media_parent, _)| media_parent == &parent);
            let retained_sibling =
                retained_payload_names
                    .iter()
                    .any(|(media_parent, media_stem)| {
                        media_parent == &parent
                            && (sidecar_stem == *media_stem
                                || sidecar_stem.starts_with(&format!("{media_stem}."))
                                || is_cover_name(&lower))
                    });
            if retained_sibling {
                classifications.push(("sidecar", "retained_processing",
                    Some("sidecar belongs to a retained payload; resolve that sibling before source cleanup")));
            } else if paired || folder_art {
                selected.push((*entry).clone());
                selected_paths.insert(entry.path.as_str());
                classifications.push((
                    "sidecar",
                    "published",
                    Some(if paired {
                        "paired sidecar"
                    } else {
                        "established folder art"
                    }),
                ));
            } else {
                classifications.push((
                    "sidecar",
                    "excluded_source_cleanup",
                    Some("unpaired sidecar is not part of a verified published payload"),
                ));
            }
        } else if is_archive_name(&lower) && consumed_archives.contains(&entry.path) {
            classifications.push((
                "archive_input",
                "consumed",
                Some("verified extraction published"),
            ));
        } else if is_archive_name(&lower)
            && terminal_unusable_archives.contains(&entry.path)
            && !matches!(
                diagnostics.get(&entry.path),
                Some(FileAssessment::Repairable(_))
            )
        {
            classifications.push((
                "archive_input",
                "excluded_source_cleanup",
                Some("archive extraction failed after configured tools and available repair; no verified output remains"),
            ));
        } else if is_archive_name(&lower) {
            match diagnostics.get(&entry.path) {
                Some(FileAssessment::Recognized(action) | FileAssessment::Repairable(action)) => {
                    classifications.push(("archive_input", "archive_review", Some(action.as_str())))
                }
                Some(FileAssessment::Incomplete(action) | FileAssessment::Corrupt(action)) => {
                    classifications.push((
                        "archive_input",
                        "excluded_source_cleanup",
                        Some(action.as_str()),
                    ))
                }
                Some(FileAssessment::NoSignature) => classifications.push((
                    "archive_input",
                    "excluded_source_cleanup",
                    Some(
                        "bounded assessment found no archive signature and no verified repair path",
                    ),
                )),
                None => classifications.push((
                    "archive_input",
                    "archive_review",
                    Some("source entry was not assessed; retry bounded diagnostics before cleanup"),
                )),
            }
        } else if let Some(FileAssessment::Repairable(action)) = diagnostics.get(&entry.path) {
            classifications.push(("repair_input", "retained_processing", Some(action.as_str())));
        } else if excluded || is_incidental_extension(&extension) || is_disposable_name(&lower) {
            classifications.push((
                "incidental",
                "excluded_source_cleanup",
                Some("known sample, readme, or processing input"),
            ));
        } else {
            match diagnostics.get(&entry.path) {
                Some(FileAssessment::Recognized(action) | FileAssessment::Repairable(action)) => {
                    classifications.push((
                        "diagnostic_input",
                        "unsupported_review",
                        Some(action.as_str()),
                    ))
                }
                Some(FileAssessment::Incomplete(action) | FileAssessment::Corrupt(action)) => {
                    classifications.push((
                        "unknown",
                        "excluded_source_cleanup",
                        Some(action.as_str()),
                    ))
                }
                Some(FileAssessment::NoSignature) => classifications.push((
                    "unknown",
                    "excluded_source_cleanup",
                    Some("bounded assessment found no supported payload or actionable format"),
                )),
                None => classifications.push((
                    "unknown",
                    "unsupported_review",
                    Some("source entry was not assessed; retry bounded diagnostics before cleanup"),
                )),
            }
        }
    }

    let outcomes = files
        .iter()
        .zip(classifications)
        .map(
            |(entry, (classification, outcome, reason))| DeliveryFileOutcome {
                source_path: entry.path.clone(),
                publication_path: selected_paths
                    .contains(entry.path.as_str())
                    .then(|| entry.path.clone()),
                classification: classification.to_string(),
                outcome: outcome.to_string(),
                reason: reason.map(str::to_owned),
                repair_dependency: matches!(
                    diagnostics.get(&entry.path),
                    Some(FileAssessment::Repairable(_))
                ),
                source_cleanup_status: if matches!(
                    outcome,
                    "retained_processing"
                        | "unverified_review"
                        | "archive_review"
                        | "unsupported_review"
                ) {
                    "review_required".into()
                } else {
                    "pending".into()
                },
            },
        )
        .collect();
    (selected, outcomes)
}

/// What a post-processing error means (contract §2.2–2.3): an
/// environmental cause still holds — it heals without a code change — and
/// everything else is the job's terminal failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PpVerdict {
    Hold(HoldCause),
    Fail(nzbd_types::FailureReason),
}

/// The environmental hold a post-processing error's errno names, whatever
/// wrapper it crossed: a typed error by its `source()` chain (io errno,
/// SQLite result code); a subprocess's words — and an engine lifecycle
/// error, which arrives already rendered — by the stderr rule.
pub fn storage_hold_cause(error: &PostError) -> Option<HoldCause> {
    match error {
        PostError::Subprocess(text)
        | PostError::Unquiesced(text)
        | PostError::Engine {
            source: nzbd_engine::EngineError::Lifecycle(text),
            ..
        } => nzbd_engine::stderr_hold_cause(text),
        other => nzbd_engine::error_hold_cause(other),
    }
}

/// Decided by the error's type and, where one exists, its errno. A release
/// title can never steer it: `NothingPublishable` names payload files and
/// is matched by variant before any message is read.
pub fn pp_verdict(error: &PostError, stage: Option<PostStage>) -> PpVerdict {
    use nzbd_types::FailureReason;
    if matches!(
        error,
        PostError::Engine {
            source: nzbd_engine::EngineError::Timeout(_) | nzbd_engine::EngineError::Closed,
            ..
        }
    ) {
        return PpVerdict::Hold(HoldCause::EngineUnavailable);
    }
    if matches!(error, PostError::NothingPublishable(_)) {
        return PpVerdict::Fail(FailureReason::Publish);
    }
    if matches!(error, PostError::Identity(_)) {
        return PpVerdict::Fail(FailureReason::Identity);
    }
    if matches!(error, PostError::ToolMissing(_)) {
        return PpVerdict::Hold(HoldCause::MissingTool);
    }
    if let Some(cause) = storage_hold_cause(error) {
        return PpVerdict::Hold(cause);
    }
    match error {
        PostError::Io(_)
        | PostError::Artifact(
            nzbd_state::artifacts::Error::Io(_) | nzbd_state::artifacts::Error::Sql(_),
        ) => PpVerdict::Fail(FailureReason::Io),
        PostError::Artifact(nzbd_state::artifacts::Error::Conflict(message))
            if message.contains("configured root identity changed or volume is unavailable") =>
        {
            PpVerdict::Hold(HoldCause::MountUnavailable)
        }
        // An inventory conflict refuses an unsafe step; the bytes are intact
        // or uncertain, so it parks and does not blame the release.
        PostError::Artifact(_) => PpVerdict::Fail(FailureReason::Publish),
        _ => PpVerdict::Fail(match stage {
            Some(PostStage::ParRename | PostStage::ParVerify | PostStage::ParRepair) => {
                FailureReason::Par
            }
            Some(PostStage::RarRename | PostStage::Unpack) => FailureReason::Unpack,
            Some(PostStage::Script) => FailureReason::Script,
            _ => FailureReason::Publish,
        }),
    }
}

/// The stage a job's last span names. Before the first span there is no
/// stage to blame; such an error is a `publish` refusal (`pp_verdict` with no
/// stage), so it carries that reason's contract stage, `publication_verify`.
fn current_stage(engine: &EngineHandle, job: JobId) -> (Option<PostStage>, String) {
    let stage = engine
        .snapshot()
        .jobs
        .iter()
        .find(|row| row.id == job)
        .and_then(|row| row.stages.last())
        .map(|span| span.stage);
    (
        stage,
        stage
            .map(|stage| stage.as_str().to_owned())
            .unwrap_or_else(|| PostStage::PublicationVerify.as_str().into()),
    )
}

/// Apply a post-processing error's verdict: hold it, or record the job's
/// terminal failure. Returns whether the owner accepted it.
async fn apply_pp_verdict(
    engine: &EngineHandle,
    history: &Arc<HistoryDb>,
    job: JobId,
    error: &PostError,
) -> bool {
    let (stage, stage_name) = current_stage(engine, job);
    match pp_verdict(error, stage) {
        PpVerdict::Hold(cause) => engine
            .hold_job(job, cause, &stage_name, &error.to_string())
            .await
            .unwrap_or(false),
        PpVerdict::Fail(reason) => {
            fail_job_in(
                engine,
                history,
                job,
                reason,
                None,
                &stage_name,
                &error.to_string(),
            )
            .await
        }
    }
}

/// The reason for a `Failed` row that carries no stamp (failed by an earlier
/// build, or imported): what its own `Failure:Reason` / fetch-cause params
/// say (review #280-8). Without them, a URL job with no files failed its
/// fetch — with an unknown cause, which never blames the release — and
/// anything else failed on health.
fn unstamped_failure(row: &Job) -> (nzbd_types::FailureReason, Option<nzbd_types::FetchCause>) {
    let param = |key: &str| {
        row.params
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    };
    let fetch_cause = param(nzbd_engine::FETCH_CAUSE_PARAM)
        .or_else(|| param("Failure:FetchCause"))
        .and_then(nzbd_types::FetchCause::parse);
    let reason = param("Failure:Reason")
        .and_then(nzbd_types::FailureReason::parse)
        .unwrap_or(
            if row.kind == nzbd_types::JobKind::Url && row.files.is_empty() {
                nzbd_types::FailureReason::Fetch
            } else {
                nzbd_types::FailureReason::Health
            },
        );
    let fetch_cause = (reason == nzbd_types::FailureReason::Fetch)
        .then_some(fetch_cause)
        .flatten();
    (reason, fetch_cause)
}

/// Record a post-processing-side failed terminal (contract §2.1) with the
/// manager's history store. False when the job is gone or already terminal.
async fn fail_job_in(
    engine: &EngineHandle,
    history: &Arc<HistoryDb>,
    job: JobId,
    reason: nzbd_types::FailureReason,
    fetch_cause: Option<nzbd_types::FetchCause>,
    stage: &str,
    message: &str,
) -> bool {
    let Ok(Some(row)) = engine.export_job(job).await else {
        return false;
    };
    if pp_done(&row) {
        return false;
    }
    let terminal =
        nzbd_engine::terminal::failed_terminal(&row, reason, fetch_cause, stage, message);
    matches!(
        engine
            .record_terminal_in(terminal, Some(history.clone()))
            .await,
        Ok(Some(_))
    )
}

/// Source-relative spellings of `paths`, dropping any outside `dir`.
fn source_relative(dir: &Path, paths: &[PathBuf]) -> Vec<String> {
    paths
        .iter()
        .filter_map(|path| path.strip_prefix(dir).ok())
        .map(|relative| relative.to_string_lossy().into_owned())
        .collect()
}

/// Why nothing in the source could be published, in the operator's terms:
/// the payload files that exist and what each one lacks.
fn unpublishable_summary(outcomes: &[DeliveryFileOutcome]) -> String {
    const SHOWN: usize = 3;
    let payloads: Vec<_> = outcomes
        .iter()
        .filter(|file| {
            matches!(
                file.classification.as_str(),
                "video" | "native_book" | "audio"
            ) || matches!(
                file.outcome.as_str(),
                "unsupported_review"
                    | "archive_review"
                    | "unverified_review"
                    | "retained_processing"
            )
        })
        .collect();
    if payloads.is_empty() {
        return "no media, book, or audio file was found in the download; source retained".into();
    }
    let mut detail = payloads
        .iter()
        .take(SHOWN)
        .map(|file| {
            let name = Path::new(&file.source_path)
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| file.source_path.clone());
            match &file.reason {
                Some(reason) => format!("{name}: {reason}"),
                None => name,
            }
        })
        .collect::<Vec<_>>()
        .join("; ");
    if payloads.len() > SHOWN {
        detail.push_str(&format!("; and {} more", payloads.len() - SHOWN));
    }
    format!("no file has integrity evidence for publication; source retained ({detail})")
}

fn recovery_reason(outcomes: &[DeliveryFileOutcome]) -> Option<String> {
    outcomes.iter().find_map(|file| {
        matches!(
            file.outcome.as_str(),
            "retained_processing" | "archive_review" | "unverified_review" | "unsupported_review"
        )
        .then(|| {
            format!(
                "{}: {}",
                file.source_path,
                file.reason
                    .as_deref()
                    .unwrap_or("inspect source in Resolve")
            )
        })
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FileAssessment {
    Recognized(String),
    Repairable(String),
    Incomplete(String),
    Corrupt(String),
    NoSignature,
}

fn publication_diagnostics(
    job: &Job,
    source: &Artifact,
    consumed_archives: &std::collections::HashSet<String>,
    verified_paths: &std::collections::HashSet<String>,
    terminal_unusable_archives: &std::collections::HashSet<String>,
    probed: &std::collections::HashSet<String>,
) -> Result<std::collections::HashMap<String, FileAssessment>, PostError> {
    let (_, outcomes) = select_delivery_payload(
        &source.files,
        consumed_archives,
        verified_paths,
        terminal_unusable_archives,
        &Default::default(),
        probed,
    );
    // Positive transform/direct proofs and explicitly disposable processing
    // inputs need no recovery diagnosis. Any unproven payload, archive,
    // sidecar or unknown sibling still receives the existing full assessment.
    if outcomes.iter().all(|file| {
        (file.outcome == "consumed" && consumed_archives.contains(&file.source_path))
            || (file.outcome == "published" && verified_paths.contains(&file.source_path))
            || (file.classification == "incidental" && file.outcome == "excluded_source_cleanup")
    }) {
        return Ok(Default::default());
    }
    assess_file_inputs(job, source, verified_paths)
}

fn assess_file_inputs(
    job: &Job,
    source: &Artifact,
    verified_paths: &std::collections::HashSet<String>,
) -> Result<std::collections::HashMap<String, FileAssessment>, PostError> {
    let mut results = bounded_file_diagnostics(&source.path, &source.files, verified_paths)?;
    for (path, reason) in par_repairable_evidence_with_partials(
        &source.path,
        &source.files,
        &sealed_partial_paths(job, source),
        &crate::attempt::checkpoint,
    )?
    .0
    {
        results.insert(path, FileAssessment::Repairable(reason));
    }
    for file in &job.files {
        let Ok(relative) = crate::namespace::relative(&file.filename) else {
            continue;
        };
        let path = relative.to_string_lossy().into_owned();
        if verified_paths.contains(&path)
            || matches!(results.get(&path), Some(FileAssessment::Repairable(_)))
        {
            continue;
        }
        let extension = relative
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let unfinished = job.file_needs_repair(file.id)
            || !file.finalized
            || file.segments.is_empty()
            || !file
                .segments
                .iter()
                .all(|segment| matches!(segment.state, nzbd_types::SegmentState::Done { .. }));
        if unfinished && payload_kind(&extension) {
            results.insert(
                path,
                FileAssessment::Incomplete(
                    "download has unfinished segments and no matching PAR recovery path".into(),
                ),
            );
        } else if !unfinished
            && !payload_kind(&extension)
            && file.crc32.is_none()
            && matches!(results.get(&path), Some(FileAssessment::Incomplete(_)))
        {
            results.insert(path, FileAssessment::Recognized(
                "every NZB segment is finalized but no whole-file CRC was supplied; verify container integrity, then retry extraction or rename in Resolve".into(),
            ));
        } else if payload_kind(&extension) {
            if file.crc32.is_none() {
                results.insert(path, FileAssessment::Recognized(
                    "all NZB segments are finalized but the writer supplied no whole-file CRC; verify integrity in Resolve or delete the source".into(),
                ));
            } else if let (Some(expected), Some(entry)) = (
                file.crc32,
                source
                    .files
                    .iter()
                    .find(|entry| !entry.identity.directory && entry.path == path),
            ) {
                let mut input = std::fs::File::open(source.path.join(&path))?;
                let before = input.metadata()?;
                if !diagnostic_metadata_matches(&before, entry) {
                    return Err(PostError::Io(std::io::Error::other(format!(
                        "{path} changed identity during integrity assessment"
                    ))));
                }
                let mut crc = crc32fast::Hasher::new();
                let mut buffer = [0u8; 64 * 1024];
                loop {
                    crate::attempt::checkpoint()?;
                    let read = input.read(&mut buffer)?;
                    if read == 0 {
                        break;
                    }
                    crc.update(&buffer[..read]);
                }
                let after = input.metadata()?;
                if !diagnostic_metadata_unchanged(&before, &after) {
                    return Err(PostError::Io(std::io::Error::other(format!(
                        "{path} changed during integrity assessment"
                    ))));
                }
                if crc.finalize() != expected {
                    results.insert(path, FileAssessment::Corrupt(
                        "whole-file CRC differs from finalized NZB file and no matching PAR repair path remains".into(),
                    ));
                }
            }
        }
    }
    // Old-style continuation volumes cannot be extracted when the first
    // volume is unfinished and no measured PAR path can restore it.
    for file in &job.files {
        let path = Path::new(&file.filename);
        if !path
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("rar"))
            || !(job.file_needs_repair(file.id)
                || !file.finalized
                || file
                    .segments
                    .iter()
                    .any(|segment| !matches!(segment.state, nzbd_types::SegmentState::Done { .. })))
            || matches!(
                results.get(&file.filename),
                Some(FileAssessment::Repairable(_))
            )
        {
            continue;
        }
        for entry in &source.files {
            let sibling = Path::new(&entry.path);
            if sibling.parent() == path.parent()
                && sibling.file_stem() == path.file_stem()
                && sibling.extension().is_some_and(|extension| {
                    let extension = extension.to_string_lossy().to_ascii_lowercase();
                    extension.len() == 3
                        && extension.starts_with('r')
                        && extension[1..].bytes().all(|byte| byte.is_ascii_digit())
                })
                && !matches!(
                    results.get(&entry.path),
                    Some(FileAssessment::Repairable(_))
                )
            {
                results.insert(entry.path.clone(), FileAssessment::Incomplete(
                    "archive first volume is incomplete and no measured PAR repair path remains; acquire a complete release".into(),
                ));
            }
        }
    }
    Ok(results)
}

/// Only exact job IDs and safe NZB names may map a sealed writer file to a
/// catalog member. Never infer this mapping from an arbitrary .part suffix.
fn sealed_partial_paths(job: &Job, source: &Artifact) -> HashMap<String, String> {
    job.files
        .iter()
        .filter_map(|file| {
            let relative = crate::namespace::relative(&file.filename).ok()?;
            let partial = format!(".runner-file-{}.part", file.id.0);
            source
                .files
                .iter()
                .any(|entry| !entry.identity.directory && entry.path == partial)
                .then(|| (relative.to_string_lossy().into_owned(), partial))
        })
        .collect()
}

/// Count the actual damaged slices of each PAR set. A recovery volume's
/// presence alone is not repair evidence: the distinct block count must
/// cover every missing or corrupt slice in the same set.
#[cfg(test)]
fn par_repairable_paths(
    root: &Path,
    entries: &[FileEntry],
) -> Result<std::collections::HashMap<String, String>, PostError> {
    Ok(par_repairable_evidence(root, entries)?.0)
}

#[cfg(test)]
fn par_repairable_evidence(
    root: &Path,
    entries: &[FileEntry],
) -> Result<
    (
        std::collections::HashMap<String, String>,
        Vec<par2::Par2Set>,
    ),
    PostError,
> {
    par_repairable_evidence_checked(root, entries, &crate::attempt::checkpoint)
}

#[cfg(test)]
fn par_repairable_evidence_checked(
    root: &Path,
    entries: &[FileEntry],
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<
    (
        std::collections::HashMap<String, String>,
        Vec<par2::Par2Set>,
    ),
    PostError,
> {
    par_repairable_evidence_with_partials(root, entries, &HashMap::new(), checkpoint)
}

fn par_repairable_evidence_with_partials(
    root: &Path,
    entries: &[FileEntry],
    partials: &HashMap<String, String>,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<(HashMap<String, String>, Vec<par2::Par2Set>), PostError> {
    let mut repairable = std::collections::HashMap::new();
    let mut repair_sets = Vec::new();
    let (sets, rejected_par_inputs) = par2::load_recovery_sets_checked(root, checkpoint)?;
    if !rejected_par_inputs.is_empty() {
        tracing::warn!(
            count = rejected_par_inputs.len(),
            "invalid PAR inputs cannot establish a repair path"
        );
    }
    for set in sets {
        checkpoint()?;
        if set.slice_size == 0 || set.main_path.is_none() {
            continue;
        }
        let mut damaged = 0u64;
        let mut affected = Vec::new();
        let mut members = Vec::new();
        let mut assessable = true;
        for descriptor in &set.files {
            checkpoint()?;
            let relative = crate::namespace::relative(&descriptor.name)?;
            let absolute = set.root.join(relative);
            let Ok(source_relative) = absolute.strip_prefix(root) else {
                return Err(PostError::Subprocess(
                    "PAR descriptor escaped the owned source tree".into(),
                ));
            };
            let source_relative = source_relative.to_string_lossy().into_owned();
            members.push(source_relative.clone());
            let actual_relative = if entries
                .iter()
                .any(|entry| !entry.identity.directory && entry.path == source_relative)
            {
                source_relative.as_str()
            } else {
                partials
                    .get(&source_relative)
                    .map_or(source_relative.as_str(), String::as_str)
            };
            // Every measured member is a dependency, including intact sealed
            // aliases needed to repair a different damaged catalog member.
            members.push(actual_relative.to_string());
            let entry = entries
                .iter()
                .find(|entry| !entry.identity.directory && entry.path == actual_relative);
            let absolute = root.join(actual_relative);
            let slices = descriptor.length.div_ceil(set.slice_size);
            if descriptor.slice_crcs.len() as u64 != slices {
                assessable = false;
                break;
            }
            let Some(entry) = entry else {
                damaged = damaged.saturating_add(slices);
                continue;
            };
            let mut input = match std::fs::File::open(&absolute) {
                Ok(input) => input,
                Err(error) => return Err(PostError::Io(error)),
            };
            let before = input.metadata()?;
            if !diagnostic_metadata_matches(&before, entry) {
                return Err(PostError::Io(std::io::Error::other(format!(
                    "{} changed identity during PAR assessment",
                    entry.path
                ))));
            }
            let mut file_damaged = 0u64;
            let mut buffer = [0u8; 64 * 1024];
            for (index, expected) in descriptor.slice_crcs.iter().enumerate() {
                let slice_start = index as u64 * set.slice_size;
                let mut remaining = descriptor
                    .length
                    .saturating_sub(slice_start)
                    .min(set.slice_size);
                let mut crc = crc32fast::Hasher::new();
                let mut short = false;
                while remaining > 0 {
                    checkpoint()?;
                    let amount = remaining.min(buffer.len() as u64) as usize;
                    let read = input.read(&mut buffer[..amount])?;
                    if read == 0 {
                        short = true;
                        break;
                    }
                    crc.update(&buffer[..read]);
                    remaining -= read as u64;
                }
                if short {
                    file_damaged += (descriptor.slice_crcs.len() - index) as u64;
                    break;
                }
                let mut padding = set.slice_size
                    - descriptor
                        .length
                        .saturating_sub(slice_start)
                        .min(set.slice_size);
                let zeroes = [0u8; 64 * 1024];
                while padding > 0 {
                    checkpoint()?;
                    let amount = padding.min(zeroes.len() as u64) as usize;
                    crc.update(&zeroes[..amount]);
                    padding -= amount as u64;
                }
                if crc.finalize() != *expected {
                    file_damaged += 1;
                }
            }
            let after = input.metadata()?;
            if !diagnostic_metadata_unchanged(&before, &after) {
                return Err(PostError::Io(std::io::Error::other(format!(
                    "{} changed during PAR assessment",
                    entry.path
                ))));
            }
            if before.len() != descriptor.length {
                file_damaged = file_damaged.max(1);
            }
            if file_damaged > 0 {
                damaged = damaged.saturating_add(file_damaged);
                affected.push(entry.path.clone());
            }
        }
        if !assessable || damaged == 0 || damaged > u64::from(set.recovery_blocks) {
            continue;
        }
        let reason = format!(
            "PAR set has {damaged} damaged slices and {} distinct recovery blocks; retry PAR repair with the configured tool",
            set.recovery_blocks
        );
        for path in affected.into_iter().chain(members) {
            repairable.insert(path, reason.clone());
        }
        for path in &set.par_paths {
            if let Ok(relative) = path.strip_prefix(root) {
                repairable.insert(relative.to_string_lossy().into_owned(), reason.clone());
            }
        }
        repair_sets.push(set);
    }
    Ok((repairable, repair_sets))
}

/// Inspect only a bounded prefix of unrecognized regular files. This is
/// diagnostic evidence, not payload integrity proof: output still needs a
/// transform journal or complete NZB/CRC proof before publication.
fn bounded_file_diagnostics(
    root: &Path,
    entries: &[FileEntry],
    complete_paths: &std::collections::HashSet<String>,
) -> std::io::Result<std::collections::HashMap<String, FileAssessment>> {
    bounded_file_diagnostics_with_limit(root, entries, complete_paths, 100_000)
}

fn bounded_file_diagnostics_with_limit(
    root: &Path,
    entries: &[FileEntry],
    complete_paths: &std::collections::HashSet<String>,
    max_entries: usize,
) -> std::io::Result<std::collections::HashMap<String, FileAssessment>> {
    const PREFIX_BYTES: usize = 4 * 1024;
    let mut results = std::collections::HashMap::new();
    if entries.len() > max_entries {
        return Err(std::io::Error::other(format!(
            "source has more than {max_entries} entries; inspect source inventory before cleanup"
        )));
    }
    if !std::fs::symlink_metadata(root)?.is_dir() {
        return Err(std::io::Error::other(
            "source root is not an accessible directory",
        ));
    }
    for entry in entries {
        crate::attempt::checkpoint()?;
        if entry.identity.directory {
            continue;
        }
        let path = Path::new(&entry.path);
        let metadata = std::fs::symlink_metadata(root.join(path))?;
        if !metadata.is_file() || !diagnostic_metadata_matches(&metadata, entry) {
            return Err(std::io::Error::other(format!(
                "{} no longer matches the owned source inventory; refresh it before cleanup",
                entry.path
            )));
        }
        let extension = path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if payload_kind(&extension)
            || is_sidecar_extension(&extension)
            || is_incidental_extension(&extension)
        {
            continue;
        }
        let mut file = std::fs::File::open(root.join(path))?;
        let before = file.metadata()?;
        if !diagnostic_metadata_matches(&before, entry) {
            return Err(std::io::Error::other(format!(
                "{} changed identity during assessment; refresh source inventory before cleanup",
                entry.path
            )));
        }
        let mut prefix = vec![0u8; PREFIX_BYTES];
        let read = file.read(&mut prefix)?;
        prefix.truncate(read);
        let after = file.metadata()?;
        if !diagnostic_metadata_unchanged(&before, &after) {
            return Err(std::io::Error::other(format!(
                "{} changed during assessment; refresh source inventory before cleanup",
                entry.path
            )));
        }
        let evidence = if prefix.starts_with(b"\x1a\x45\xdf\xa3") {
            Some(
                "EBML/Matroska signature; rename to .mkv after confirming the file, then Retry publication",
            )
        } else if prefix.len() >= 8 && &prefix[4..8] == b"ftyp" {
            Some(
                "ISO base media signature; rename to a supported video/audio extension after confirmation, then Retry publication",
            )
        } else if prefix.starts_with(b"ID3")
            || prefix.starts_with(b"fLaC")
            || prefix.starts_with(b"OggS")
        {
            Some(
                "recognized audio container signature; rename to a supported audio extension after confirmation, then Retry publication",
            )
        } else if prefix.starts_with(b"%PDF-") {
            Some("PDF signature; rename to .pdf after confirmation, then Retry publication")
        } else if prefix.starts_with(b"PK\x03\x04") {
            Some("ZIP container signature; retry extraction with the configured archive tool")
        } else if prefix.starts_with(b"Rar!\x1a\x07") {
            Some("RAR container signature; retry extraction with the configured archive tool")
        } else if prefix.starts_with(b"7z\xbc\xaf\x27\x1c") {
            Some("7-Zip container signature; retry extraction with the configured archive tool")
        } else {
            None
        };
        let result = match (evidence, complete_paths.contains(&entry.path)) {
            (Some(evidence), true) => FileAssessment::Recognized(evidence.to_string()),
            (Some(_), false) => FileAssessment::Incomplete(
                "recognized signature alone cannot repair an incomplete file; no complete NZB/CRC or transform proof".into(),
            ),
            (None,true)=>FileAssessment::Recognized("completed NZB/CRC file has no supported format signature; review its intended content and choose a supported publication format".into()),
            (None,false)=>FileAssessment::NoSignature,
        };
        results.insert(entry.path.clone(), result);
    }
    Ok(results)
}

fn diagnostic_metadata_matches(metadata: &std::fs::Metadata, entry: &FileEntry) -> bool {
    if metadata.len() != entry.identity.bytes || metadata.is_dir() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        metadata.dev() == entry.identity.device
            && metadata.ino() == entry.identity.inode
            && format!("{}:{}", metadata.mtime(), metadata.mtime_nsec()) == entry.identity.modified
            && (entry.identity.changed.is_empty()
                || format!("{}:{}", metadata.ctime(), metadata.ctime_nsec())
                    == entry.identity.changed)
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn diagnostic_metadata_unchanged(before: &std::fs::Metadata, after: &std::fs::Metadata) -> bool {
    if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        before.dev() == after.dev()
            && before.ino() == after.ino()
            && before.ctime() == after.ctime()
            && before.ctime_nsec() == after.ctime_nsec()
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn verified_direct_payload_paths(
    job: &Job,
    source: &Artifact,
    restored_paths: &std::collections::HashMap<String, String>,
    checkpoint: impl FnMut() -> std::io::Result<()>,
) -> std::result::Result<std::collections::HashSet<String>, PostError> {
    verified_direct_payload_paths_excluding(
        job,
        source,
        restored_paths,
        &Default::default(),
        checkpoint,
    )
}

fn verified_direct_payload_paths_excluding(
    job: &Job,
    source: &Artifact,
    restored_paths: &std::collections::HashMap<String, String>,
    consumed_inputs: &std::collections::HashSet<String>,
    mut checkpoint: impl FnMut() -> std::io::Result<()>,
) -> std::result::Result<std::collections::HashSet<String>, PostError> {
    let mut verified = std::collections::HashSet::new();
    let repair_markers = job
        .params
        .iter()
        .filter(|(key, _)| key.starts_with("*File:repair:"))
        .filter_map(|(key, _)| key.strip_prefix("*File:repair:")?.parse::<u32>().ok())
        .collect::<std::collections::HashSet<_>>();
    for file in &job.files {
        if !file.finalized
            || file.segments.is_empty()
            || !file
                .segments
                .iter()
                .all(|segment| matches!(segment.state, nzbd_types::SegmentState::Done { .. }))
            || repair_markers.contains(&file.id.0)
        {
            continue;
        }
        let expected_crc = file.crc32;
        let Ok(relative) = crate::namespace::relative(&file.filename) else {
            continue;
        };
        let relative = relative.to_string_lossy().into_owned();
        let actual_relative = restored_paths
            .get(&relative)
            .map_or(relative.as_str(), String::as_str);
        // Fresh custody evidence already proved these archives were consumed
        // by successful extraction. They are not direct publication payloads.
        if consumed_inputs.contains(actual_relative) {
            continue;
        }
        let Some(entry) = source
            .files
            .iter()
            .find(|entry| !entry.identity.directory && entry.path == actual_relative)
        else {
            continue;
        };
        let Some(expected_crc) = expected_crc else {
            // R3: every segment is `Done` and the inventory journaled the
            // file's digest — that is integrity proof without a yEnc CRC.
            if entry.digest.is_some() {
                verified.insert(entry.path.clone());
            }
            continue;
        };
        let path = source.path.join(actual_relative);
        let mut input = std::fs::File::open(&path)?;
        let before = input.metadata()?;
        if !diagnostic_metadata_matches(&before, entry) {
            return Err(PostError::Io(std::io::Error::other(format!(
                "{} changed identity before CRC verification",
                entry.path
            ))));
        }
        let mut crc = crc32fast::Hasher::new();
        let mut buffer = [0u8; 64 * 1024];
        loop {
            checkpoint()?;
            let count = input.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            crc.update(&buffer[..count]);
        }
        let after = input.metadata()?;
        if !diagnostic_metadata_unchanged(&before, &after) {
            return Err(PostError::Io(std::io::Error::other(format!(
                "{} changed during CRC verification",
                entry.path
            ))));
        }
        if crc.finalize() != expected_crc {
            continue;
        }
        verified.insert(entry.path.clone());
    }
    Ok(verified)
}

fn is_sidecar_extension(extension: &str) -> bool {
    matches!(
        extension,
        "srt" | "ass" | "ssa" | "sub" | "idx" | "nfo" | "jpg" | "jpeg" | "png" | "webp" | "tbn"
    )
}
fn payload_kind(extension: &str) -> bool {
    matches!(
        extension,
        "mkv"
            | "mp4"
            | "avi"
            | "m4v"
            | "mov"
            | "wmv"
            | "ts"
            | "m2ts"
            | "mts"
            | "vob"
            | "mpg"
            | "mpeg"
            | "iso"
            | "mka"
            | "webm"
            | "m4b"
            | "m4a"
            | "mp3"
            | "aac"
            | "flac"
            | "ogg"
            | "opus"
            | "wav"
            | "wma"
            | "epub"
            | "azw3"
            | "azw"
            | "mobi"
            | "pdf"
            | "cbz"
            | "cbr"
            | "djvu"
    )
}
/// R3 (contract §2.2): which verified files whose extension is not on the
/// fixed payload list are media by their bytes. An obfuscated or unusual
/// name must not make a cleanly verified payload unpublishable. Archives,
/// PAR2, sidecars and incidental files are never probed.
fn probe_payload_kinds(
    root: &Path,
    entries: &[FileEntry],
    verified: &std::collections::HashSet<String>,
) -> std::collections::HashSet<String> {
    entries
        .iter()
        .filter(|entry| !entry.identity.directory && verified.contains(&entry.path))
        .filter(|entry| {
            let path = Path::new(&entry.path);
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            let extension = path
                .extension()
                .and_then(|extension| extension.to_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            !payload_kind(&extension)
                && !is_archive_name(&name)
                && !is_sidecar_extension(&extension)
                && !is_incidental_extension(&extension)
                && extension != "par2"
        })
        .filter(|entry| media_by_content(&root.join(&entry.path)))
        .map(|entry| entry.path.clone())
        .collect()
}

/// What the file's bytes say it is: a magic-number sniff, then `ffprobe`
/// (10 s bound) when the sniff is inconclusive and ffprobe is installed.
fn media_by_content(path: &Path) -> bool {
    match sniff_media(path) {
        Some(verdict) => verdict,
        None => ffprobe_media(path),
    }
}

/// `Some(true)` for a recognised media container, `Some(false)` for a
/// recognised non-media format (archives), `None` when inconclusive.
fn sniff_media(path: &Path) -> Option<bool> {
    let mut file = std::fs::File::open(path).ok()?;
    let mut head = vec![0u8; 0x8006];
    let mut filled = 0;
    while filled < head.len() {
        match file.read(&mut head[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(_) => return None,
        }
    }
    head.truncate(filled);
    let at = |offset: usize, magic: &[u8]| head.get(offset..offset + magic.len()) == Some(magic);
    // Archives are inputs, never payloads.
    if at(0, b"Rar!\x1a\x07") || at(0, b"7z\xbc\xaf\x27\x1c") || at(0, b"PK\x03\x04") {
        return Some(false);
    }
    let media = at(0, &[0x1a, 0x45, 0xdf, 0xa3]) // EBML: Matroska / WebM
        || at(4, b"ftyp") // ISO base media: MP4 / MOV / M4V
        || (at(0, b"RIFF") && (at(8, b"AVI ") || at(8, b"WAVE")))
        || (at(0, &[0x47]) && at(188, &[0x47]) && at(376, &[0x47])) // MPEG-TS
        || (at(4, &[0x47]) && at(196, &[0x47])) // M2TS (4-byte timecode prefix)
        || at(0, &[0x00, 0x00, 0x01, 0xba]) // MPEG-PS / VOB
        || at(0x8001, b"CD001") // ISO 9660
        || at(0, b"fLaC")
        || at(0, b"OggS")
        || at(0, b"ID3");
    media.then_some(true)
}

fn ffprobe_media(path: &Path) -> bool {
    let child = std::process::Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=format_name",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
        ])
        .arg(path)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn();
    let Ok(mut child) = child else {
        return false; // not installed: the sniff's "inconclusive" stands
    };
    // Drain stdout to EOF on its own thread while this one enforces the
    // deadline: a child blocked on a full pipe would otherwise never exit
    // and every probe would end as a timeout. Only the first 4 KiB is kept
    // (the answer is one short line).
    let Some(mut stdout) = child.stdout.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return false;
    };
    let reader = std::thread::spawn(move || {
        let mut kept = Vec::new();
        let mut buf = [0u8; 8192];
        loop {
            match stdout.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let room = 4096usize.saturating_sub(kept.len());
                    kept.extend_from_slice(&buf[..n.min(room)]);
                }
            }
        }
        kept
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let succeeded = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status.success(),
            Err(_) => break false,
            Ok(None) if std::time::Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                break false;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
        }
    };
    // The child has exited (or was killed), so its stdout is closed and the
    // reader reaches EOF.
    let out = String::from_utf8_lossy(&reader.join().unwrap_or_default()).into_owned();
    if !succeeded {
        return false;
    }
    let format = out.trim().to_ascii_lowercase();
    !format.is_empty()
        && !format.contains("image")
        && !format.contains("_pipe")
        && !matches!(format.as_str(), "tty" | "txt")
        && [
            "matroska", "webm", "mov", "mp4", "mpegts", "avi", "asf", "mpeg", "flv", "wav", "flac",
            "mp3", "ogg",
        ]
        .iter()
        .any(|known| format.contains(known))
}
fn payload_classification(extension: &str) -> &'static str {
    if matches!(
        extension,
        "mkv"
            | "mp4"
            | "avi"
            | "m4v"
            | "mov"
            | "wmv"
            | "ts"
            | "m2ts"
            | "mts"
            | "vob"
            | "mpg"
            | "mpeg"
            | "iso"
            | "webm"
    ) {
        "video"
    } else if matches!(
        extension,
        "epub" | "azw3" | "azw" | "mobi" | "pdf" | "cbz" | "cbr" | "djvu"
    ) {
        "native_book"
    } else {
        "audio"
    }
}
fn is_sample_or_proof(path: &Path, stem: &str) -> bool {
    let token = |s: &str| {
        ["sample"].iter().any(|word| {
            s.eq_ignore_ascii_case(word)
                || [".", "-", "_"].iter().any(|separator| {
                    s.to_ascii_lowercase()
                        .ends_with(&format!("{separator}{word}"))
                })
        })
    };
    path.components().any(|component| {
        component.as_os_str().to_str().is_some_and(|name| {
            name.eq_ignore_ascii_case("sample") || name.eq_ignore_ascii_case("proof")
        })
    }) || token(stem)
        || stem.to_ascii_lowercase().ends_with("[proof]")
        || stem.to_ascii_lowercase().ends_with("(proof)")
}
fn is_cover_name(name: &str) -> bool {
    matches!(
        name,
        "cover.jpg"
            | "cover.jpeg"
            | "cover.png"
            | "folder.jpg"
            | "folder.png"
            | "poster.jpg"
            | "poster.png"
            | "season.jpg"
            | "season.png"
            | "fanart.jpg"
            | "fanart.png"
    )
}
fn is_archive_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    [
        ".rar", ".7z", ".zip", ".tar", ".tar.gz", ".tgz", ".gz", ".bz2", ".xz", ".zst", ".iso",
    ]
    .iter()
    .any(|extension| lower.ends_with(extension))
        || lower.rsplit_once('.').is_some_and(|(_, suffix)| {
            suffix.len() == 3
                && suffix.starts_with('r')
                && suffix[1..].bytes().all(|b| b.is_ascii_digit())
        })
}
fn is_incidental_extension(extension: &str) -> bool {
    matches!(
        extension,
        "par2" | "nzb" | "sfv" | "srr" | "tmp" | "temp" | "part" | "!ut" | "crdownload"
    )
}
pub(crate) fn is_disposable_name(name: &str) -> bool {
    name.starts_with("sample.")
        || matches!(
            name,
            "readme" | "readme.txt" | "info.txt" | "thumbs.db" | ".ds_store"
        )
}

fn select_scripts(found: Vec<PathBuf>, extensions: &[String]) -> Vec<PathBuf> {
    if extensions.is_empty() {
        return found;
    }
    found
        .into_iter()
        .filter(|p| {
            let name = |f: Option<&std::ffi::OsStr>| {
                f.map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default()
            };
            let file = name(p.file_name());
            let stem = name(p.file_stem());
            extensions
                .iter()
                .any(|e| e.eq_ignore_ascii_case(&file) || e.eq_ignore_ascii_case(&stem))
        })
        .collect()
}

/// Preserve isolated repair while fetching delayed recovery blocks for this set.
#[allow(clippy::too_many_arguments)]
async fn repair_isolated_loop(
    engine: &EngineHandle,
    cfg: &PostConfig,
    par: &Par2Tool,
    stages: &mut Stages<'_>,
    job_id: JobId,
    set: par2::Par2Set,
    partials: &[(PathBuf, String)],
    restored: &mut std::collections::HashSet<PathBuf>,
) -> Result<bool, PostError> {
    let control = crate::attempt::current().expect("processing control");
    let inventory = engine.artifacts();
    let owned_partials = partials.to_vec();
    control
        .phase(nzbd_engine::RepairPhase::Validating, set.files.len())
        .await;
    let session = control
        .blocking(move |_| {
            if crate::repair_workspace::published_repair_reusable(
                &inventory,
                job_id.0,
                &set,
                &owned_partials,
            )? {
                return Ok(None);
            }
            crate::repair_workspace::RepairSession::new(inventory, job_id.0, set, owned_partials)
                .map(Some)
        })
        .await?;
    let Some(mut session) = session else {
        tracing::info!(
            job = job_id.0,
            "reusing completed PAR repair after output verification"
        );
        return Ok(true);
    };
    let deadline = tokio::time::Instant::now() + cfg.par_fetch_timeout;
    #[cfg(test)]
    let (mut verified_calls, mut fetched_batches) = (0, 0);
    let mut verify_required = true;
    let mut needed = 1;
    let mut last_reason = "no additional recovery files remain".to_string();
    for round in 0..8 {
        control.checkpoint()?;
        stages.enter(PostStage::ParRepair).await;
        control
            .round(round, session.set.recovery_blocks, Some(needed))
            .await;
        if verify_required {
            let authorized: std::collections::HashSet<_> = engine
                .export_job(job_id)
                .await
                .map_err(|e| PostError::Subprocess(e.to_string()))?
                .into_iter()
                .flat_map(|job| job.files)
                .filter(|file| file.finalized)
                .filter_map(|file| crate::namespace::relative(&file.filename).ok())
                .map(|path| session.set.root.join(path))
                .collect();
            control.phase(nzbd_engine::RepairPhase::Matching, 0).await;
            session = control
                .blocking(move |_| {
                    session.match_candidates()?;
                    Ok(session)
                })
                .await?;
            control
                .phase(
                    nzbd_engine::RepairPhase::Preparing,
                    session.set.files.len() + session.set.par_paths.len() + 1,
                )
                .await;
            session = control
                .blocking(move |_| {
                    session.prepare(&authorized)?;
                    Ok(session)
                })
                .await?;
            control.phase(nzbd_engine::RepairPhase::Verifying, 0).await;
            let verified = par.verify_full(&session.main()).await?;
            #[cfg(test)]
            {
                verified_calls += 1;
                if let Some(observer) = &control.repair_observer {
                    observer(session.matching_bytes, verified_calls, fetched_batches);
                }
            }
            let ready = match verified {
                VerifyResult::Intact => true,
                VerifyResult::Repairable { .. } => {
                    control
                        .phase(nzbd_engine::RepairPhase::Reconstructing, 0)
                        .await;
                    if par.repair(&session.main()).await? != RepairResult::Repaired {
                        last_reason =
                            "PAR tool reported repairable data but reconstruction failed".into();
                        break;
                    }
                    true
                }
                VerifyResult::NeedMoreBlocks { blocks_needed } => {
                    needed = blocks_needed.max(1);
                    false
                }
                VerifyResult::Unrepairable => {
                    last_reason = "PAR tool could not verify the set".into();
                    break;
                }
            };
            if ready {
                control
                    .phase(
                        nzbd_engine::RepairPhase::Validating,
                        session.set.files.len(),
                    )
                    .await;
                let recovered = control
                    .blocking(move |_| session.validate_publish())
                    .await?;
                restored.extend(recovered);
                return Ok(true);
            }
        }
        if tokio::time::Instant::now() >= deadline {
            last_reason = "recovery fetch deadline exhausted".into();
            break;
        }
        control
            .phase(nzbd_engine::RepairPhase::FetchingRecovery, 0)
            .await;
        control
            .round(round, session.set.recovery_blocks, Some(needed))
            .await;
        let freed = engine
            .unpause_par_blocks(job_id, needed, Some(session.set.slice_size))
            .await
            .unwrap_or(0);
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if freed == 0 || !wait_par_files(engine, job_id, remaining).await {
            break;
        }
        #[cfg(test)]
        {
            fetched_batches += 1;
            if let Some(observer) = &control.repair_observer {
                observer(session.matching_bytes, verified_calls, fetched_batches);
            }
        }
        let root = session.set.root.clone();
        let (sets, rejected) = control
            .blocking(move |_| par2::load_recovery_sets(&root))
            .await?;
        if !rejected.is_empty() {
            last_reason = format!("rejected recovery files: {}", rejected.join("; "));
        }
        let Some(mut refreshed) = sets
            .into_iter()
            .find(|candidate| candidate.set_id == session.set.set_id)
        else {
            last_reason = "recovery files no longer contain this PAR set".into();
            break;
        };
        if refreshed.slice_size != session.set.slice_size
            || refreshed.files.len() != session.set.files.len()
            || session.set.files.iter().any(|old| {
                !refreshed.files.iter().any(|new| {
                    old.id == new.id
                        && old.name == new.name
                        && old.length == new.length
                        && old.md5_full == new.md5_full
                        && old.md5_16k == new.md5_16k
                        && (old.slice_crcs.is_empty() || old.slice_crcs == new.slice_crcs)
                })
            })
        {
            return Err(PostError::Subprocess(
                "PAR catalog changed within the repair attempt; review required".into(),
            ));
        }
        if session
            .set
            .main_path
            .as_ref()
            .is_some_and(|path| refreshed.par_paths.contains(path))
        {
            refreshed.main_path = session.set.main_path.clone();
        }
        let new_exponents = refreshed
            .recovery_exponents
            .difference(&session.set.recovery_exponents)
            .next()
            .is_some();
        let (returned, changed) = control
            .blocking(move |_| {
                let changed = session.sources_changed()?;
                Ok((session, changed))
            })
            .await?;
        session = returned;
        verify_required = new_exponents || changed;
        if !verify_required && rejected.is_empty() {
            last_reason = "recovery files added no unique recovery blocks".into();
        }
        session.set = refreshed;
    }
    tracing::warn!(job = job_id.0, reason = %last_reason, "PAR repair exhausted recovery inputs");
    control.blocking(move |_| session.abandon()).await?;
    Ok(false)
}

/// verify_full → (unpause delayed pars → wait → re-verify)* → repair.
async fn repair_loop(
    engine: &EngineHandle,
    cfg: &PostConfig,
    par: &Par2Tool,
    stages: &mut Stages<'_>,
    job_id: JobId,
    main: &Path,
) -> Result<bool, PostError> {
    let control = crate::attempt::current().expect("processing control");
    let root = main
        .parent()
        .ok_or_else(|| PostError::Subprocess("PAR catalog root missing".into()))?
        .to_path_buf();
    let wanted = main.to_path_buf();
    let set = control
        .blocking(move |_| {
            par2::load_sets(&root)?
                .into_iter()
                .find(|set| set.par_paths.contains(&wanted))
                .ok_or_else(|| PostError::Subprocess("PAR catalog disappeared".into()))
        })
        .await?;
    repair_isolated_loop(
        engine,
        cfg,
        par,
        stages,
        job_id,
        set,
        &[],
        &mut Default::default(),
    )
    .await
}

/// What became of a failed job's files: what to tell the history row, and
/// where the bytes are now (`None` = gone).
#[derive(Debug, Clone, Default)]
pub struct Disposition {
    pub note: String,
    pub files_at: Option<PathBuf>,
    /// The step stopped on a full volume or exhausted quota (by errno):
    /// intake stops too, not just this disposal.
    pub out_of_space: bool,
}

async fn dispose_failed(
    inventory: Arc<nzbd_state::artifacts::Inventory>,
    cfg: &PostConfig,
    job: JobId,
    dir: &Path,
    dest: &Path,
    name: &str,
    _tag: &str,
) -> Disposition {
    if cfg.failure_action == FailureAction::Delete
        && inventory
            .for_job(job.0)
            .ok()
            .flatten()
            .is_some_and(|a| a.state == "deleted")
    {
        return Disposition {
            note: "deleted".into(),
            files_at: None,
            ..Default::default()
        };
    }
    if cfg.failure_action == FailureAction::Delete {
        let result = terminal_worker(inventory, job, move |inventory, guard, checkpoint| {
            checkpoint()?;
            inventory
                .delete_failed_payload(guard, job.0, checkpoint)
                .map_err(PostError::from)
        })
        .await;
        return match result {
            Ok(op) if op.state == "succeeded" => Disposition {
                note: "deleted".into(),
                files_at: None,
                ..Default::default()
            },
            other => Disposition {
                out_of_space: other.as_ref().is_err_and(|error| {
                    nzbd_engine::out_of_space(storage_hold_cause(error).as_ref())
                }),
                note: format!("checked deletion pending: {other:?}"),
                files_at: Some(dir.into()),
            },
        };
    }
    if cfg.failure_action == FailureAction::Park {
        let to = cfg
            .failed_dir
            .clone()
            .unwrap_or_else(|| dest.join(".failed"))
            .join(name);
        let moved = terminal_worker(inventory, job, move |inventory, _, checkpoint| {
            inventory
                .relocate_checked(job.0, &to, checkpoint)
                .map_err(PostError::from)
        })
        .await;
        return match moved {
            Ok(result) => Disposition {
                note: format!("parked at {}", result.published_path.display()),
                files_at: Some(result.published_path),
                ..Default::default()
            },
            other => Disposition {
                out_of_space: other.as_ref().is_err_and(|error| {
                    nzbd_engine::out_of_space(storage_hold_cause(error).as_ref())
                }),
                note: format!("checked move pending: {other:?}"),
                files_at: Some(dir.into()),
            },
        };
    }
    Disposition {
        note: "kept".into(),
        files_at: Some(dir.into()),
        ..Default::default()
    }
}

/// The terminal actor keeps its generation guard through actual filesystem
/// exit. Outside the stage pipeline cancellation still fences publication.
async fn terminal_worker<T: Send + 'static>(
    inventory: Arc<nzbd_state::artifacts::Inventory>,
    job: JobId,
    action: impl FnOnce(
            &nzbd_state::artifacts::Inventory,
            &nzbd_state::artifacts::AttemptUse,
            &dyn Fn() -> std::io::Result<()>,
        ) -> Result<T, PostError>
        + Send
        + 'static,
) -> Result<T, PostError> {
    if let Some(control) = crate::attempt::current() {
        return control
            .blocking(move |control| {
                let guard = control
                    .use_guard
                    .lock()
                    .unwrap()
                    .clone()
                    .ok_or_else(|| PostError::Subprocess("terminal owner missing".into()))?;
                action(&inventory, &guard, &|| control.checkpoint())
            })
            .await;
    }
    let cancel = CancellationToken::new();
    let _cancel_on_drop = cancel.clone().drop_guard();
    tokio::task::spawn_blocking(move || {
        let checkpoint = || {
            if cancel.is_cancelled() {
                Err(std::io::ErrorKind::Interrupted.into())
            } else {
                Ok(())
            }
        };
        let guard = inventory
            .acquire_terminal_use(job.0, &checkpoint)
            .map_err(|e| PostError::Subprocess(e.to_string()))?
            .ok_or_else(|| PostError::Subprocess("terminal payload custody missing".into()))?;
        action(&inventory, &guard, &checkpoint)
    })
    .await
    .map_err(|e| PostError::Subprocess(e.to_string()))?
}

/// The recovery-set slice size from the job's main par2 index, if it can
/// be read.
///
/// This is what prices a hash-named recovery volume: no `.volXX+NN` marker
/// exists on an obfuscated post, but `bytes / slice_size` is a good enough
/// block count to pick with. The index is small (tens of KiB) and the read
/// is capped and off the async threads regardless.
#[cfg(test)]
async fn par2_block_size(main: &Path) -> Option<u64> {
    let path = main.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::io::Read as _;
        const MAX: u64 = 8 * 1024 * 1024;
        let mut bytes = Vec::new();
        std::fs::File::open(&path)
            .ok()?
            .take(MAX)
            .read_to_end(&mut bytes)
            .ok()?;
        let slice = nzbd_par2::scan(&bytes).slice_size;
        (slice > 0).then_some(slice)
    })
    .await
    .ok()
    .flatten()
}

/// Wait until every non-paused par2 file of the job is terminal again
/// (the freshly unpaused vol files finished downloading).
async fn wait_par_files(engine: &EngineHandle, job_id: JobId, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        match engine.export_job(job_id).await {
            Ok(Some(job)) => {
                let pending = job
                    .files
                    .iter()
                    .any(|f| f.is_par2 && !f.paused && !f.is_terminal());
                if !pending {
                    // Give writers a beat to finalize renames.
                    let all_finalized = job
                        .files
                        .iter()
                        .filter(|f| f.is_par2 && !f.paused && f.has_any_done())
                        .all(|f| f.finalized);
                    if all_finalized {
                        return true;
                    }
                }
            }
            _ => return false,
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Remove every `.pp.*` staging dir except this lease's own.
fn remove_stale_staging(_dir: &Path, _own: &Path) {
    // A name is not ownership. Retain abandoned generations for reconciliation.
}

/// Publish staged extraction output: rename each entry into the job dir,
/// replacing existing targets (identical content by construction — same
/// archive, same extractor).
fn commit_staging(staging: &Path, dir: &Path) -> std::io::Result<()> {
    fn publish(source: &Path, target: &Path) -> std::io::Result<()> {
        crate::attempt::checkpoint()?;
        let meta = std::fs::symlink_metadata(source)?;
        if meta.file_type().is_symlink() {
            return Err(std::io::Error::other("symlink extraction output"));
        }
        if meta.is_dir() {
            match std::fs::create_dir(target) {
                Ok(()) => (),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    if !std::fs::symlink_metadata(target)?.is_dir() {
                        return Err(e);
                    }
                }
                Err(e) => return Err(e),
            }
            for entry in std::fs::read_dir(source)? {
                let entry = entry?;
                publish(&entry.path(), &target.join(entry.file_name()))?;
            }
        } else if meta.is_file() {
            // Hard-link publication is exclusive. Replays can reuse equal bytes;
            // different existing bytes are never overwritten.
            match nzbd_state::fileops::copy_publish_checked(
                source,
                target,
                &crate::attempt::checkpoint,
            )
            .map_err(|e| match e {
                nzbd_state::artifacts::Error::Io(io) => io,
                other => std::io::Error::other(other),
            }) {
                Ok(()) => {
                    std::fs::File::open(source)?.sync_all()?;
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    let target_meta = std::fs::symlink_metadata(target)?;
                    if !target_meta.is_file()
                        || target_meta.file_type().is_symlink()
                        || !crate::namespace::equal_files(source, target)?
                    {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::AlreadyExists,
                            "extraction output conflict; review required",
                        ));
                    }
                }
                Err(e) => return Err(e),
            }
        } else {
            return Err(std::io::Error::other("unsupported extraction output"));
        }
        Ok(())
    }
    for entry in std::fs::read_dir(staging)? {
        let entry = entry?;
        publish(&entry.path(), &dir.join(entry.file_name()))?;
    }
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
fn cleanup_dir(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().to_lowercase();
        let junk = name.ends_with(".par2")
            || name.ends_with(".rar")
            || name.ends_with(".zip")
            || name.ends_with(".7z")
            || name.ends_with(".sfv")
            || name
                .split('.')
                .next_back()
                .map(|e| e.chars().all(|c| c.is_ascii_digit()) && e.len() == 3)
                .unwrap_or(false);
        if junk {
            let _ = std::fs::remove_file(e.path());
        }
    }
}

/// The NZBGet-compatible script environment (the adoption-critical subset).
fn script_env(
    job: &Job,
    dir: &Path,
    par_ok: bool,
    par_repaired: bool,
    unpack_ok: bool,
    unpacked_any: bool,
) -> Vec<(String, String)> {
    let par_status = if !par_ok {
        "4" // repair failed
    } else if par_repaired {
        "2" // repaired
    } else {
        "1" // checked, no repair needed (0 = not checked)
    };
    let unpack_status = if !unpack_ok {
        "1" // failed
    } else if unpacked_any {
        "2" // unpacked
    } else {
        "0" // nothing to unpack
    };
    let total = if par_ok && unpack_ok {
        "SUCCESS"
    } else {
        "FAILURE"
    };
    let mut env = vec![
        ("NZBPP_DIRECTORY".into(), dir.to_string_lossy().into_owned()),
        ("NZBPP_FINALDIR".into(), String::new()),
        ("NZBPP_NZBNAME".into(), job.name.clone()),
        ("NZBPP_NZBFILENAME".into(), format!("{}.nzb", job.name)),
        (
            "NZBPP_CATEGORY".into(),
            job.category.clone().unwrap_or_default(),
        ),
        ("NZBPP_PARSTATUS".into(), par_status.into()),
        ("NZBPP_UNPACKSTATUS".into(), unpack_status.into()),
        ("NZBPP_TOTALSTATUS".into(), total.into()),
        ("NZBPP_STATUS".into(), format!("{total}/ALL")),
        (
            "NZBPP_HEALTH".into(),
            Health::calc(&job.totals).0.to_string(),
        ),
        ("NZBPP_NZBID".into(), job.id.0.to_string()),
        (
            "NZBOP_DESTDIR".into(),
            dir.parent()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default(),
        ),
        ("NZBOP_VERSION".into(), env!("CARGO_PKG_VERSION").into()),
    ];
    for (k, v) in &job.params {
        if !k.starts_with('*') {
            env.push((format!("NZBPR_{k}"), v.clone()));
        }
    }
    env
}

/// The manager's decision helpers, exercised directly.
///
/// These are the parts of the orchestrator that decide *what* happens —
/// which failure action an operator's spelling means, where a failed job's
/// files end up, which restart points a stage still permits, what a script
/// is told about the run. Reaching them through `process_job` needs a live
/// engine, a history database and real archive tools, which is why they
/// went untested: the pipeline tests that do stand all that up assert on
/// the pipeline's outcome, not on these decisions.
#[cfg(test)]
mod decision_tests {
    use super::*;
    use nzbd_types::{DupeInfo, FileEntry, FileId, JobKind, JobTotals, Segment, SegmentState};
    use std::os::unix::fs::PermissionsExt;

    // Direct pipeline fixtures use the same owner admission as the manager.
    async fn process_job_ctx(
        engine: &EngineHandle,
        cfg: &PostConfig,
        history: &Arc<HistoryDb>,
        dest_dir: &Path,
        job: JobId,
        ctx: &PpCtx,
    ) -> Result<PpFinal, PostError> {
        let admission = tokio::time::timeout(
            Duration::from_secs(5),
            engine.wait_post_admission(job, &ctx.cancel),
        )
        .await
        .expect("fixture processing admission must settle")
        .map_err(|error| PostError::Subprocess(error.to_string()))?
        .expect("fixture must be eligible for processing");
        let scoped = engine.for_post_admission(&admission);
        let result = super::process_job_ctx(&scoped, cfg, history, dest_dir, job, ctx).await;
        engine
            .end_post_admission(admission)
            .await
            .map_err(|error| PostError::Subprocess(error.to_string()))?;
        result
    }

    trait FixtureImport {
        async fn import_fixture_job(
            &self,
            job: Job,
            fold: bool,
            emit: bool,
        ) -> Result<(), nzbd_engine::EngineError>;
    }
    impl FixtureImport for EngineHandle {
        async fn import_fixture_job(
            &self,
            mut job: Job,
            fold: bool,
            emit: bool,
        ) -> Result<(), nzbd_engine::EngineError> {
            // Real completed downloads record the publication length alongside
            // their durable segment ranges. Preserve that evidence in fixtures.
            for file in &job.files {
                let key = format!("*File:size:{}", file.id.0);
                if !file.segments.is_empty()
                    && file
                        .segments
                        .iter()
                        .all(|segment| matches!(segment.state, SegmentState::Done { .. }))
                    && !job.params.iter().any(|(name, _)| name == &key)
                {
                    let size = file
                        .segments
                        .iter()
                        .map(|segment| match segment.state {
                            SegmentState::Done { offset, len, .. } => offset + u64::from(len),
                            _ => unreachable!(),
                        })
                        .max()
                        .unwrap();
                    job.params.push((key, size.to_string()));
                }
            }
            let id = job.id;
            self.import_job(job, fold, emit).await?;
            let settled = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match self.export_job(id).await {
                        Err(nzbd_engine::EngineError::Lifecycle(reason))
                            if reason == "job verification or writer retirement is still in progress; retry later" => {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        Ok(Some(_)) => return Ok(()),
                        Ok(None) => panic!("imported fixture job disappeared"),
                        Err(error) => return Err(error),
                    }
                }
            })
            .await;
            match settled {
                Ok(result) => result,
                Err(_) => panic!(
                    "fixture verification must settle: {:?}",
                    self.recovery_document().await.unwrap().jobs
                ),
            }
        }
    }

    fn payload_entry(path: &str) -> nzbd_state::artifacts::FileEntry {
        nzbd_state::artifacts::FileEntry {
            path: path.into(),
            identity: nzbd_state::artifacts::Identity {
                device: 1,
                inode: 2,
                mount_id: String::new(),
                bytes: 17,
                modified: "0:0".into(),
                changed: "0:0".into(),
                directory: false,
            },
            digest: None,
        }
    }

    fn real_entry(root: &Path, name: &str) -> nzbd_state::artifacts::FileEntry {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::metadata(root.join(name)).unwrap();
        nzbd_state::artifacts::FileEntry {
            path: name.into(),
            identity: nzbd_state::artifacts::Identity {
                device: metadata.dev(),
                inode: metadata.ino(),
                mount_id: String::new(),
                bytes: metadata.len(),
                modified: format!("{}:{}", metadata.mtime(), metadata.mtime_nsec()),
                changed: format!("{}:{}", metadata.ctime(), metadata.ctime_nsec()),
                directory: false,
            },
            digest: None,
        }
    }

    #[test]
    fn delivery_selection_publishes_verified_payload_and_classifies_unresolved_inputs() {
        let entries = [
            payload_entry("Season 1/ep01.en.srt"),
            payload_entry("Season 1/ep01.mkv"),
            payload_entry("Season 1/ep02-sample.mkv"),
            payload_entry("release.rar"),
            payload_entry("nested/bonus.zip"),
            payload_entry("readme.txt"),
        ];
        let consumed = ["release.rar".to_string()].into_iter().collect();
        let verified = ["Season 1/ep01.mkv".to_string()].into_iter().collect();
        let (selected, outcomes) = select_delivery_payload(
            &entries,
            &consumed,
            &verified,
            &Default::default(),
            &Default::default(),
            &Default::default(),
        );
        let paths: std::collections::HashSet<_> =
            selected.iter().map(|entry| entry.path.as_str()).collect();
        assert_eq!(
            paths,
            ["Season 1/ep01.en.srt", "Season 1/ep01.mkv"]
                .into_iter()
                .collect()
        );
        let outcome = |path: &str| {
            outcomes
                .iter()
                .find(|outcome| outcome.source_path == path)
                .unwrap()
        };
        assert_eq!(outcome("release.rar").outcome, "consumed");
        assert_eq!(outcome("nested/bonus.zip").outcome, "archive_review");
        assert_eq!(outcome("readme.txt").outcome, "excluded_source_cleanup");
        assert_eq!(
            outcome("Season 1/ep02-sample.mkv").outcome,
            "excluded_source_cleanup"
        );
    }

    #[test]
    fn direct_media_is_publishable_without_archive_transform_evidence() {
        let entries = [
            payload_entry("Album/track.flac"),
            payload_entry("Books/title.epub"),
        ];
        let verified = [
            "Album/track.flac".to_string(),
            "Books/title.epub".to_string(),
        ]
        .into_iter()
        .collect();
        let (selected, outcomes) = select_delivery_payload(
            &entries,
            &Default::default(),
            &verified,
            &Default::default(),
            &Default::default(),
            &Default::default(),
        );
        assert_eq!(selected.len(), 2);
        assert!(outcomes
            .iter()
            .all(|outcome| outcome.outcome == "published"));
    }

    #[test]
    fn renamed_direct_media_requires_and_uses_finalized_writer_crc() {
        let temp = tempfile::tempdir().unwrap();
        let downloads = temp.path().join("downloads");
        std::fs::create_dir(&downloads).unwrap();
        let source_path = downloads.join("renamed");
        let inventory = nzbd_state::artifacts::Inventory::open(&temp.path().join("state")).unwrap();
        inventory
            .allocate_for_delivery(41, &downloads, &source_path, true, true)
            .unwrap();
        let bytes = b"complete media bytes with a known writer CRC";
        std::fs::write(source_path.join("episode.bin"), bytes).unwrap();
        inventory
            .finish(41, &source_path, &downloads, "active")
            .unwrap();
        inventory
            .restore_file(
                41,
                &source_path.join("episode.bin"),
                &source_path.join("episode.restored"),
            )
            .unwrap();
        inventory
            .restore_file(
                41,
                &source_path.join("episode.restored"),
                &source_path.join("episode.mkv"),
            )
            .unwrap();
        let source = inventory.refresh_for_delivery(41).unwrap();
        let restored = inventory.verified_restore_paths(41).unwrap();
        assert_eq!(
            restored.get("episode.bin").map(String::as_str),
            Some("episode.mkv")
        );
        assert_eq!(
            restored.get("episode.restored").map(String::as_str),
            Some("episode.mkv")
        );

        let mut direct = file(1, "episode.bin", Some(crc32fast::hash(bytes)));
        direct.segments = vec![Segment {
            message_id: "complete".into(),
            number: 1,
            size: bytes.len() as u32,
            state: SegmentState::Done {
                offset: 0,
                len: bytes.len() as u32,
                crc: crc32fast::hash(bytes),
            },
        }];
        assert_eq!(
            verified_direct_payload_paths(
                &job(vec![direct.clone()], vec![]),
                &source,
                &restored,
                || Ok(())
            )
            .unwrap(),
            ["episode.mkv".to_string()].into_iter().collect()
        );

        direct.crc32 = Some(crc32fast::hash(b"different complete bytes"));
        assert!(verified_direct_payload_paths(
            &job(vec![direct], vec![]),
            &source,
            &restored,
            || Ok(())
        )
        .unwrap()
        .is_empty());

        let mut incomplete = file(2, "episode.bin", Some(crc32fast::hash(bytes)));
        incomplete.segments.clear();
        assert!(verified_direct_payload_paths(
            &job(vec![incomplete], vec![]),
            &source,
            &restored,
            || Ok(())
        )
        .unwrap()
        .is_empty());
    }

    #[test]
    fn partial_archive_failure_publishes_verified_media_and_exposes_unresolved_pack() {
        let entries = [
            payload_entry("episode-01.mkv"),
            payload_entry("episode-02.rar"),
        ];
        let verified = ["episode-01.mkv".to_string()].into_iter().collect();
        let diagnostics = [(
            "episode-02.rar".to_string(),
            FileAssessment::Recognized(
                "RAR signature and complete CRC; retry extraction with configured tool".into(),
            ),
        )]
        .into_iter()
        .collect();
        let (selected, outcomes) = select_delivery_payload(
            &entries,
            &Default::default(),
            &verified,
            &Default::default(),
            &diagnostics,
            &Default::default(),
        );
        assert_eq!(
            selected
                .iter()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
            ["episode-01.mkv"]
        );
        assert_eq!(outcomes[1].outcome, "archive_review");
    }

    #[test]
    fn positively_failed_archive_is_disposed_while_good_episode_is_salvaged() {
        let entries = [
            payload_entry("episode-01.mkv"),
            payload_entry("episode-02.rar"),
        ];
        let verified = ["episode-01.mkv".to_string()].into_iter().collect();
        let terminal = ["episode-02.rar".to_string()].into_iter().collect();
        let (selected, outcomes) = select_delivery_payload(
            &entries,
            &Default::default(),
            &verified,
            &terminal,
            &Default::default(),
            &Default::default(),
        );
        assert_eq!(selected.len(), 1);
        let failed = outcomes
            .iter()
            .find(|outcome| outcome.source_path == "episode-02.rar")
            .unwrap();
        assert_eq!(failed.outcome, "excluded_source_cleanup");
        assert_eq!(
            failed.reason.as_deref(),
            Some(
                "archive extraction failed after configured tools and available repair; no verified output remains"
            )
        );
    }

    /// Review #280-8: an unstamped failed row's reason comes from its own
    /// params; a fetch failure with no recorded cause never guesses `gone`.
    #[test]
    fn unstamped_failure_reads_params_instead_of_guessing_gone() {
        use nzbd_types::{FailureReason, FetchCause};
        let mut url = job(vec![], vec![]);
        url.kind = JobKind::Url;
        assert_eq!(unstamped_failure(&url), (FailureReason::Fetch, None));
        url.params.push((
            nzbd_engine::FETCH_CAUSE_PARAM.into(),
            "indexer_unavailable".into(),
        ));
        assert_eq!(
            unstamped_failure(&url),
            (FailureReason::Fetch, Some(FetchCause::IndexerUnavailable))
        );
        assert!(!FailureReason::Fetch.blames_release(Some(FetchCause::IndexerUnavailable)));
        let recorded = job(
            vec![file(1, "episode.mkv", None)],
            vec![("Failure:Reason".into(), "io".into())],
        );
        assert_eq!(unstamped_failure(&recorded), (FailureReason::Io, None));
        assert_eq!(
            unstamped_failure(&job(vec![file(1, "episode.mkv", None)], vec![])),
            (FailureReason::Health, None)
        );
    }

    #[test]
    fn delivery_selection_requires_integrity_and_matches_curator_formats_and_sample_tokens() {
        let entries = [
            payload_entry("The Sample Film.mkv"),
            payload_entry("film.sample.mkv"),
            payload_entry("Sample/film.mkv"),
            payload_entry("Proof/cover.jpg"),
            payload_entry("book.cbz"),
            payload_entry("book.djvu"),
            payload_entry("unsupported.xyz"),
            payload_entry("unverified.mp4"),
        ];
        let verified = [
            "The Sample Film.mkv".to_string(),
            "film.sample.mkv".to_string(),
            "Sample/film.mkv".to_string(),
            "book.cbz".to_string(),
            "book.djvu".to_string(),
            "unsupported.xyz".to_string(),
        ]
        .into_iter()
        .collect();
        let (selected, outcomes) = select_delivery_payload(
            &entries,
            &Default::default(),
            &verified,
            &Default::default(),
            &Default::default(),
            &Default::default(),
        );
        let paths = selected
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            paths,
            ["The Sample Film.mkv", "book.cbz", "book.djvu"]
                .into_iter()
                .collect()
        );
        let outcome = |path: &str| outcomes.iter().find(|row| row.source_path == path).unwrap();
        assert_eq!(
            outcome("film.sample.mkv").outcome,
            "excluded_source_cleanup"
        );
        assert_eq!(
            outcome("Sample/film.mkv").outcome,
            "excluded_source_cleanup"
        );
        assert_eq!(
            outcome("Proof/cover.jpg").outcome,
            "excluded_source_cleanup"
        );
        assert_eq!(outcome("unsupported.xyz").outcome, "unsupported_review");
        assert_eq!(outcome("unverified.mp4").outcome, "unverified_review");

        // R3: MPEG program streams are payload by name, and an unknown
        // extension whose verified bytes the sniff/ffprobe identified as
        // media publishes too (review: the `.mpg` case stays covered).
        let entries = [payload_entry("film.mpg"), payload_entry("unsupported.xyz")];
        let verified = ["film.mpg".to_string(), "unsupported.xyz".to_string()]
            .into_iter()
            .collect();
        let probed = ["unsupported.xyz".to_string()].into_iter().collect();
        let (selected, _) = select_delivery_payload(
            &entries,
            &Default::default(),
            &verified,
            &Default::default(),
            &Default::default(),
            &probed,
        );
        let paths = selected
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            paths,
            ["film.mpg", "unsupported.xyz"].into_iter().collect(),
            "a verified .mpg and a sniffed unknown both publish"
        );
    }

    #[test]
    fn bounded_diagnostic_faults_never_become_disposal_evidence() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("one.rar"), b"Rar!\x1a\x07incomplete").unwrap();
        std::fs::write(temp.path().join("two.bin"), b"unknown").unwrap();
        let entries = [
            real_entry(temp.path(), "one.rar"),
            real_entry(temp.path(), "two.bin"),
        ];
        let complete = std::collections::HashSet::new();
        let budget = bounded_file_diagnostics_with_limit(temp.path(), &entries, &complete, 1);
        assert!(budget.is_err(), "a capped walk cannot authorize deletion");
        std::fs::remove_file(temp.path().join("two.bin")).unwrap();
        let read_fault = bounded_file_diagnostics_with_limit(temp.path(), &entries, &complete, 2);
        assert!(
            read_fault.is_err(),
            "a vanished manifest entry must trigger inspection retry"
        );
    }

    #[test]
    fn failed_health_salvages_verified_episode_and_retires_unrepairable_archive() {
        let temp = tempfile::tempdir().unwrap();
        let inventory = nzbd_state::artifacts::Inventory::open(&temp.path().join("state")).unwrap();
        let root = temp.path().join("downloads");
        std::fs::create_dir(&root).unwrap();
        let source = root.join("failed-pack");
        inventory
            .allocate_for_delivery(41, &root, &source, true, true)
            .unwrap();
        let good = b"complete episode";
        std::fs::write(source.join("episode-01.mkv"), good).unwrap();
        std::fs::write(source.join(".runner-file-2.part"), b"Rar!\x1a\x07partial").unwrap();
        let continuation = b"Rar!\x1a\x07complete continuation";
        std::fs::write(source.join("episode-02.r00"), continuation).unwrap();
        inventory.finish(41, &source, &root, "active").unwrap();
        let mut good_file = file(1, "episode-01.mkv", Some(crc32fast::hash(good)));
        good_file.segments = vec![Segment {
            message_id: "good".into(),
            number: 1,
            size: good.len() as u32,
            state: SegmentState::Done {
                offset: 0,
                len: good.len() as u32,
                crc: crc32fast::hash(good),
            },
        }];
        let mut bad_file = file(2, "episode-02.rar", None);
        bad_file.segments = vec![Segment {
            message_id: "missing".into(),
            number: 1,
            size: 20,
            state: SegmentState::Failed,
        }];
        let mut continuation_file = file(3, "episode-02.r00", Some(crc32fast::hash(continuation)));
        continuation_file.segments = vec![Segment {
            message_id: "continuation".into(),
            number: 1,
            size: continuation.len() as u32,
            state: SegmentState::Done {
                offset: 0,
                len: continuation.len() as u32,
                crc: crc32fast::hash(continuation),
            },
        }];
        let mut request = job(
            vec![good_file, bad_file, continuation_file],
            vec![("monarr-transfer".into(), "failed-pack-token".into())],
        );
        request.delivery_lifecycle = true;
        let completed = temp.path().join("completed");
        let salvage = salvage_failed_delivery(
            &inventory,
            &request,
            &completed,
            "FAILURE/HEALTH",
            "health",
            "missing episode segments",
            &EarlyArchiveAssessment::default(),
        )
        .unwrap()
        .expect("verified episode should publish");
        let delivery = salvage.delivery.expect("Curator delivery");
        assert!(salvage.published_path.join("episode-01.mkv").exists());
        assert!(!salvage.published_path.join("episode-02.rar").exists());
        let outcomes = inventory
            .delivery_file_outcomes(&delivery.artifact_id, &delivery.publication_generation)
            .unwrap();
        assert_eq!(
            outcomes
                .iter()
                .find(|row| row.source_path == "episode-02.r00")
                .unwrap()
                .outcome,
            "excluded_source_cleanup"
        );
        assert!(outcomes
            .iter()
            .find(|row| row.source_path == "episode-02.r00")
            .unwrap()
            .reason
            .as_deref()
            .unwrap()
            .contains("first volume is incomplete"));
        inventory
            .retire_delivery_source(&delivery.delivery_id)
            .unwrap();
        assert!(!source.join("episode-02.rar").exists());
    }

    #[tokio::test]
    async fn failed_job_manager_salvages_finalized_media_before_terminal_history() {
        let temp = tempfile::tempdir().unwrap();
        let history = Arc::new(
            HistoryDb::open(&temp.path().join("history.sqlite"), Some(temp.path())).unwrap(),
        );
        let downloads = temp.path().join("downloads");
        std::fs::create_dir(&downloads).unwrap();
        let completed = temp.path().join("completed");
        let mut config = nzbd_engine::EngineConfig::single_node(
            Vec::new(),
            temp.path().join("queue"),
            downloads.clone(),
            nzbd_engine::Tuning::default(),
            None,
        );
        config.artifact_dir = Some(temp.path().join("artifacts"));
        config.history = Some(history.clone());
        let engine = nzbd_engine::Engine::spawn(config).await.unwrap();
        let good_bytes = b"verified episode recovered from a failed job";
        let mut good = file(1, "episode-01.mkv", Some(crc32fast::hash(good_bytes)));
        good.segments = vec![Segment {
            message_id: "good-episode".into(),
            number: 1,
            size: good_bytes.len() as u32,
            state: SegmentState::Done {
                offset: 0,
                len: good_bytes.len() as u32,
                crc: crc32fast::hash(good_bytes),
            },
        }];
        let mut incomplete = file(2, "episode-02.mkv", None);
        incomplete.segments = vec![Segment {
            message_id: "missing-episode".into(),
            number: 1,
            size: 100,
            state: SegmentState::Failed,
        }];
        let mut request = job(
            vec![good, incomplete],
            vec![
                ("monarr-transfer".into(), "live-health-salvage".into()),
                ("monarr-delivery".into(), "1".into()),
            ],
        );
        request.delivery_lifecycle = true;
        request.status = JobStatus::Failed;
        let source_path = downloads.join(nzbd_engine::queue::job_dir_name(&request));
        let inventory = engine.artifacts();
        inventory
            .allocate_for_delivery(41, &downloads, &source_path, true, true)
            .unwrap();

        let source = inventory.for_job(41).unwrap().unwrap();
        std::fs::write(source.path.join("episode-01.mkv"), good_bytes).unwrap();
        std::fs::write(source.path.join("episode-02.mkv"), b"partial").unwrap();
        inventory
            .finish(41, &source.path, &downloads, "active")
            .unwrap();
        engine
            .import_fixture_job(request, false, false)
            .await
            .unwrap();

        let cfg = PostConfig {
            completed_dir: Some(completed),
            unpack: false,
            cleanup: false,
            deobfuscate_final: false,
            ..PostConfig::default()
        };
        fail_and_follow_up(
            &engine,
            &cfg,
            &history,
            &downloads,
            &None,
            JobId(41),
            "Some.Release".into(),
            0,
            false,
        )
        .await;

        let delivery = inventory.delivery_for_job(41).unwrap().unwrap();
        assert_eq!(delivery.terminal_outcome, "FAILURE/HEALTH");
        assert_eq!(delivery.files.len(), 1);
        assert_eq!(delivery.files[0].path, "episode-01.mkv");
        assert!(PathBuf::from(&delivery.publication_path)
            .join("episode-01.mkv")
            .exists());
        assert!(!PathBuf::from(&delivery.publication_path)
            .join("episode-02.mkv")
            .exists());
        assert_eq!(
            history.get(JobId(41)).unwrap().unwrap().status,
            "FAILURE/HEALTH"
        );
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn failed_health_extracts_complete_archive_sibling_before_partial_delivery() {
        if !crate::tools::require_tool("7z") {
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let downloads = temp.path().join("downloads");
        std::fs::create_dir(&downloads).unwrap();
        let mut config = nzbd_engine::EngineConfig::single_node(
            Vec::new(),
            temp.path().join("queue"),
            downloads.clone(),
            nzbd_engine::Tuning::default(),
            None,
        );
        config.artifact_dir = Some(temp.path().join("artifacts"));
        let engine = nzbd_engine::Engine::spawn(config).await.unwrap();
        let inventory = engine.artifacts();
        let source = downloads.join("failed-pack");
        inventory
            .allocate_for_delivery(41, &downloads, &source, true, true)
            .unwrap();
        let payload = b"independent archived episode";
        std::fs::write(temp.path().join("episode-01.mkv"), payload).unwrap();
        let generated = std::process::Command::new("7z")
            .args([
                "a",
                "-tzip",
                "-y",
                source.join("episode-01.zip").to_str().unwrap(),
                "episode-01.mkv",
            ])
            .current_dir(temp.path())
            .status()
            .unwrap();
        assert!(generated.success());
        std::fs::write(source.join("episode-02.rar"), b"Rar!\x1a\x07partial").unwrap();
        inventory.finish(41, &source, &downloads, "active").unwrap();
        let archive_bytes = std::fs::read(source.join("episode-01.zip")).unwrap();
        let mut complete_archive = file(1, "episode-01.zip", Some(crc32fast::hash(&archive_bytes)));
        complete_archive.segments = vec![Segment {
            message_id: "complete-zip".into(),
            number: 1,
            size: archive_bytes.len() as u32,
            state: SegmentState::Done {
                offset: 0,
                len: archive_bytes.len() as u32,
                crc: crc32fast::hash(&archive_bytes),
            },
        }];
        let mut incomplete_archive = file(2, "episode-02.rar", None);
        incomplete_archive.segments = vec![Segment {
            message_id: "missing-rar".into(),
            number: 1,
            size: 20,
            state: SegmentState::Failed,
        }];
        let mut request = job(
            vec![complete_archive, incomplete_archive],
            vec![("monarr-transfer".into(), "failed-archive-token".into())],
        );
        request.delivery_lifecycle = true;
        // Own the source throughout extraction and commit, as the real stage
        // pipeline does. Startup inventory reconciliation can still be joining.
        let ctx = PpCtx::default();
        let attempt = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match crate::attempt::AttemptControl::new(&engine, &ctx, request.id).await {
                    Err(PostError::Subprocess(reason))
                        if reason
                            == "previous processing or allocation workers have not quiesced" =>
                    {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    other => break other.unwrap(),
                }
            }
        })
        .await
        .expect("archive fixture must acquire exact source custody after startup joins");
        let assessed = crate::attempt::CURRENT
            .scope(attempt.clone(), async {
                extract_complete_failed_archives(&engine, &PostConfig::default(), &request, &None)
                    .await
            })
            .await;
        attempt.finish_filesystem_work().await.unwrap();
        drop(attempt);
        let assessed = assessed.unwrap();
        let completed = temp.path().join("completed");
        let salvage = salvage_failed_delivery(
            &inventory,
            &request,
            &completed,
            "FAILURE/HEALTH",
            "health",
            "missing episode segments",
            &assessed,
        )
        .unwrap()
        .expect("journaled independent archive output should publish");
        assert!(salvage.published_path.join("episode-01.mkv").exists());
        assert!(!salvage.published_path.join("episode-01.zip").exists());
        assert!(!salvage.published_path.join("episode-02.rar").exists());
    }

    #[tokio::test]
    async fn failed_health_extractor_io_retries_without_discarding_archive() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let downloads = temp.path().join("downloads");
        std::fs::create_dir(&downloads).unwrap();
        let mut config = nzbd_engine::EngineConfig::single_node(
            Vec::new(),
            temp.path().join("queue"),
            downloads.clone(),
            nzbd_engine::Tuning::default(),
            None,
        );
        config.artifact_dir = Some(temp.path().join("artifacts"));
        let engine = nzbd_engine::Engine::spawn(config).await.unwrap();
        let inventory = engine.artifacts();
        let source = downloads.join("failed-pack");
        inventory
            .allocate_for_delivery(41, &downloads, &source, true, true)
            .unwrap();
        let bytes = b"PK\x03\x04archive";
        std::fs::write(source.join("episode.zip"), bytes).unwrap();
        inventory.finish(41, &source, &downloads, "active").unwrap();
        let mut archive = file(1, "episode.zip", Some(crc32fast::hash(bytes)));
        archive.segments = vec![Segment {
            message_id: "archive".into(),
            number: 1,
            size: bytes.len() as u32,
            state: SegmentState::Done {
                offset: 0,
                len: bytes.len() as u32,
                crc: crc32fast::hash(bytes),
            },
        }];
        let request = job(vec![archive], vec![]);
        let fake = temp.path().join("fake-7z");
        std::fs::write(&fake, "#!/bin/sh\necho 'Input/output error' >&2\nexit 2\n").unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cfg = PostConfig {
            sevenzip_cmd: fake.to_string_lossy().into_owned(),
            ..PostConfig::default()
        };
        // Production runs every transform inside an admitted attempt, whose
        // use fences the engine's artifact tick off the source. Without it
        // the tick's workspace reconciliation (`reconcile_workspace_
        // retirements_with_history`) can hold the source's lease exactly when
        // the transform completes ("transform output remains in use"), which
        // made this test fail about 1 run in 12 under a loaded suite.
        let attempt = admitted_attempt(&engine, request.id).await;
        let error = crate::attempt::CURRENT
            .scope(attempt, async {
                extract_complete_failed_archives(&engine, &cfg, &request, &None).await
            })
            .await
            .err()
            .expect("storage failure must enter retry lane");
        assert!(error.to_string().contains("requires retry"), "{error}");
        assert!(source.join("episode.zip").exists());
        assert!(inventory.delivery_for_job(41).unwrap().is_none());
    }

    #[test]
    fn par_recovery_requires_enough_blocks_for_actual_damaged_slices() {
        if !crate::tools::require_tool("par2") {
            return;
        }
        use std::io::{Seek, SeekFrom, Write};
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        std::fs::write(root.join("episode-02.rar"), vec![b'a'; 50_000]).unwrap();
        let generated = std::process::Command::new("par2")
            .args([
                "create",
                "-q",
                "-q",
                "-s8192",
                "-c1",
                "set.par2",
                "episode-02.rar",
            ])
            .current_dir(root)
            .status()
            .unwrap();
        assert!(generated.success());
        let mut input = std::fs::OpenOptions::new()
            .write(true)
            .open(root.join("episode-02.rar"))
            .unwrap();
        input.write_all(b"b").unwrap();
        input.sync_all().unwrap();
        let entries = [real_entry(root, "episode-02.rar")];
        let one_bad = par_repairable_paths(root, &entries).unwrap();
        assert!(one_bad.contains_key("episode-02.rar"));
        input.seek(SeekFrom::Start(8192)).unwrap();
        input.write_all(b"c").unwrap();
        input.sync_all().unwrap();
        let entries = [real_entry(root, "episode-02.rar")];
        let two_bad = par_repairable_paths(root, &entries).unwrap();
        assert!(!two_bad.contains_key("episode-02.rar"));
    }

    #[tokio::test]
    async fn failed_health_repairs_sufficient_par_set_and_salvages_both_episodes() {
        if !crate::tools::require_tool("par2") {
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let downloads = temp.path().join("downloads");
        std::fs::create_dir(&downloads).unwrap();
        let mut config = nzbd_engine::EngineConfig::single_node(
            Vec::new(),
            temp.path().join("queue"),
            downloads.clone(),
            nzbd_engine::Tuning::default(),
            None,
        );
        config.artifact_dir = Some(temp.path().join("artifacts"));
        let engine = nzbd_engine::Engine::spawn(config).await.unwrap();
        let inventory = engine.artifacts();
        let source = downloads.join("failed-par-pack");
        inventory
            .allocate_for_delivery(41, &downloads, &source, true, true)
            .unwrap();
        let good = vec![b'a'; 30_000];
        // par2cmdline 0.8.1 on Linux cannot reconstruct this intentionally
        // low-entropy repeated-byte fixture from a single recovery block.
        // Use deterministic varied data, as real media does, while keeping
        // one damaged 8192-byte slice and exactly one recovery block.
        let damaged_original: Vec<_> = (0..50_000).map(|n| ((n * 7) % 251) as u8).collect();
        std::fs::write(source.join("episode-01.mkv"), &good).unwrap();
        std::fs::write(source.join("episode-02.mkv"), &damaged_original).unwrap();
        let generated = std::process::Command::new("par2")
            .args([
                "create",
                "-q",
                "-q",
                "-s8192",
                "-c1",
                "set.par2",
                "episode-02.mkv",
            ])
            .current_dir(&source)
            .status()
            .unwrap();
        assert!(generated.success());
        use std::io::Write;
        let mut damaged = std::fs::OpenOptions::new()
            .write(true)
            .open(source.join("episode-02.mkv"))
            .unwrap();
        damaged.write_all(b"x").unwrap();
        damaged.sync_all().unwrap();
        drop(damaged);
        std::fs::rename(
            source.join("episode-02.mkv"),
            source.join(".runner-file-2.part"),
        )
        .unwrap();
        inventory.finish(41, &source, &downloads, "active").unwrap();
        let mut good_file = file(1, "episode-01.mkv", Some(crc32fast::hash(&good)));
        good_file.segments = vec![Segment {
            message_id: "good".into(),
            number: 1,
            size: good.len() as u32,
            state: SegmentState::Done {
                offset: 0,
                len: good.len() as u32,
                crc: crc32fast::hash(&good),
            },
        }];
        let mut damaged_file = file(2, "episode-02.mkv", None);
        damaged_file.segments = vec![Segment {
            message_id: "failed".into(),
            number: 1,
            size: damaged_original.len() as u32,
            state: SegmentState::Failed,
        }];
        let mut request = job(
            vec![good_file, damaged_file],
            vec![("monarr-transfer".into(), "failed-par-token".into())],
        );
        request.delivery_lifecycle = true;
        let source_files = inventory.for_job(request.id.0).unwrap().unwrap().files;
        let (repairable_paths, repair_sets) = par_repairable_evidence_with_partials(
            &source,
            &source_files,
            &sealed_partial_paths(&request, &inventory.for_job(41).unwrap().unwrap()),
            &|| Ok(()),
        )
        .unwrap();
        assert_eq!(repair_sets.len(), 1, "expected one valid PAR set");
        assert_eq!(
            repair_sets[0].recovery_blocks, 1,
            "fixture must provide one recovery block"
        );
        assert!(
            repairable_paths.contains_key("episode-02.mkv"),
            "fixture must measure the damaged source slice"
        );
        let history = Arc::new(
            HistoryDb::open(&temp.path().join("history.sqlite"), Some(temp.path())).unwrap(),
        );
        request.status = JobStatus::Failed;
        engine
            .import_fixture_job(request.clone(), false, false)
            .await
            .unwrap();
        fail_and_follow_up(
            &engine,
            &PostConfig {
                completed_dir: Some(temp.path().join("completed")),
                ..PostConfig::default()
            },
            &history,
            &downloads,
            &None,
            request.id,
            request.name.clone(),
            100,
            false,
        )
        .await;
        let delivery = inventory
            .delivery_for_job(41)
            .unwrap()
            .expect("manager must publish repair output");
        let salvage = FailedSalvage {
            published_path: PathBuf::from(&delivery.publication_path),
            generation: delivery.publication_generation.clone(),
            delivery: Some(delivery),
        };
        assert_eq!(
            history.get(request.id).unwrap().unwrap().status,
            "FAILURE/HEALTH"
        );
        assert!(engine
            .export_job(request.id)
            .await
            .unwrap()
            .unwrap()
            .params
            .iter()
            .any(|(key, _)| key == PP_DONE_PARAM));
        assert!(salvage.published_path.join("episode-01.mkv").exists());
        assert_eq!(
            std::fs::read(salvage.published_path.join("episode-02.mkv")).unwrap(),
            damaged_original
        );
    }

    #[tokio::test]
    async fn failed_health_par_tool_failure_still_publishes_good_sibling_and_retains_repair_set() {
        if !crate::tools::require_tool("par2") {
            return;
        }
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let downloads = temp.path().join("downloads");
        std::fs::create_dir(&downloads).unwrap();
        let mut config = nzbd_engine::EngineConfig::single_node(
            Vec::new(),
            temp.path().join("queue"),
            downloads.clone(),
            nzbd_engine::Tuning::default(),
            None,
        );
        config.artifact_dir = Some(temp.path().join("artifacts"));
        let engine = nzbd_engine::Engine::spawn(config).await.unwrap();
        let inventory = engine.artifacts();
        let source = downloads.join("mixed-pack");
        inventory
            .allocate_for_delivery(41, &downloads, &source, true, true)
            .unwrap();
        let good = vec![b'g'; 51_200];
        let damaged_original = vec![b'x'; 51_200];
        std::fs::write(source.join("episode-01.mkv"), &good).unwrap();
        std::fs::write(source.join("episode-02.mkv"), &damaged_original).unwrap();
        let intact_partial = vec![b'z'; 60_000];
        std::fs::write(source.join("episode-03.mkv"), &intact_partial).unwrap();
        assert!(std::process::Command::new("par2")
            .args([
                "create",
                "-q",
                "-q",
                "-s8192",
                "-c1",
                "set.par2",
                "episode-01.mkv",
                "episode-02.mkv",
                "episode-03.mkv"
            ])
            .current_dir(&source)
            .status()
            .unwrap()
            .success());
        let mut damaged = std::fs::OpenOptions::new()
            .write(true)
            .open(source.join("episode-02.mkv"))
            .unwrap();
        damaged.write_all(b"y").unwrap();
        damaged.sync_all().unwrap();
        drop(damaged);
        std::fs::rename(
            source.join("episode-02.mkv"),
            source.join(".runner-file-2.part"),
        )
        .unwrap();
        std::fs::write(
            source.join("episode-02.en.srt"),
            b"subtitle for retained episode",
        )
        .unwrap();
        std::fs::write(source.join("readme.txt"), b"junk").unwrap();
        std::fs::rename(
            source.join("episode-03.mkv"),
            source.join(".runner-file-3.part"),
        )
        .unwrap();
        inventory.finish(41, &source, &downloads, "active").unwrap();
        let mut good_file = file(1, "episode-01.mkv", Some(crc32fast::hash(&good)));
        good_file.segments = vec![Segment {
            message_id: "good".into(),
            number: 1,
            size: good.len() as u32,
            state: SegmentState::Done {
                offset: 0,
                len: good.len() as u32,
                crc: crc32fast::hash(&good),
            },
        }];
        let mut bad_file = file(2, "episode-02.mkv", None);
        bad_file.segments = vec![Segment {
            message_id: "failed".into(),
            number: 1,
            size: damaged_original.len() as u32,
            state: SegmentState::Failed,
        }];
        let mut intact_file = file(3, "episode-03.mkv", None);
        intact_file.segments = vec![Segment {
            message_id: "failed-but-intact-bytes".into(),
            number: 1,
            size: intact_partial.len() as u32,
            state: SegmentState::Failed,
        }];
        let mut request = job(
            vec![good_file, bad_file, intact_file],
            vec![("monarr-transfer".into(), "mixed-token".into())],
        );
        request.delivery_lifecycle = true;
        let fake = temp.path().join("failed-par-tool");
        std::fs::write(
            &fake,
            "#!/bin/sh\nif [ \"$1\" = verify ]; then echo 'Repair is possible'; exit 1; fi\necho 'repair failed' >&2\nexit 1\n",
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cfg = PostConfig {
            par2_cmd: fake.to_string_lossy().into_owned(),
            ..PostConfig::default()
        };
        let attempt = admitted_attempt(&engine, request.id).await;
        let failures = crate::attempt::CURRENT
            .scope(attempt, async {
                repair_failed_health_par_sets(&engine, &cfg, &request, &None).await
            })
            .await
            .expect("local repair failure must not block independent payload");
        assert!(failures.contains_key("episode-02.mkv"));
        let assessment = EarlyArchiveAssessment {
            par_failures: failures,
            ..EarlyArchiveAssessment::default()
        };
        let salvage = salvage_failed_delivery(
            &inventory,
            &request,
            &temp.path().join("completed"),
            "FAILURE/HEALTH",
            "health",
            "failed segment with measured PAR recovery",
            &assessment,
        )
        .unwrap()
        .expect("good sibling must publish");
        assert!(salvage.published_path.join("episode-01.mkv").exists());
        assert!(!salvage.published_path.join("episode-02.mkv").exists());
        let delivery = salvage.delivery.unwrap();
        let outcomes = inventory
            .delivery_file_outcomes(&delivery.artifact_id, &delivery.publication_generation)
            .unwrap();
        assert_eq!(
            outcomes
                .iter()
                .find(|outcome| outcome.source_path == "episode-01.mkv")
                .unwrap()
                .outcome,
            "published"
        );
        assert!(
            outcomes
                .iter()
                .find(|outcome| outcome.source_path == "episode-01.mkv")
                .unwrap()
                .repair_dependency
        );
        assert!(
            outcomes
                .iter()
                .find(|outcome| outcome.source_path == ".runner-file-2.part")
                .unwrap()
                .repair_dependency
        );
        assert!(
            outcomes
                .iter()
                .find(|outcome| outcome.source_path == "set.par2")
                .unwrap()
                .repair_dependency
        );
        assert_eq!(
            outcomes
                .iter()
                .find(|outcome| outcome.source_path == ".runner-file-2.part")
                .unwrap()
                .outcome,
            "retained_processing"
        );
        let cleanup = inventory
            .retire_delivery_source(&delivery.delivery_id)
            .unwrap();
        assert_ne!(cleanup.source_cleanup.status, "succeeded");
        // A published repair input keeps its source name (contract §2.5 as
        // built: linked, not renamed), so the set stays repairable.
        assert!(source.join("episode-01.mkv").exists());
        assert!(salvage.published_path.join("episode-01.mkv").exists());
        assert!(source.join(".runner-file-2.part").exists());
        assert_eq!(
            std::fs::read(source.join(".runner-file-3.part")).unwrap(),
            intact_partial
        );
        assert!(outcomes
            .iter()
            .any(|outcome| outcome.source_path == ".runner-file-3.part"
                && outcome.outcome == "retained_processing"
                && outcome.repair_dependency));
        assert!(source.join("set.par2").exists());
        assert!(source.join("episode-02.en.srt").exists());
        assert!(!source.join("readme.txt").exists());
        std::fs::remove_dir_all(&salvage.published_path).unwrap();
        let verify = std::process::Command::new("par2")
            .args([
                "verify",
                "set.par2",
                ".runner-file-2.part",
                ".runner-file-3.part",
            ])
            .current_dir(&source)
            .output()
            .unwrap();
        let report = format!(
            "{}{}",
            String::from_utf8_lossy(&verify.stdout),
            String::from_utf8_lossy(&verify.stderr)
        );
        assert!(
            verify.status.success()
                || verify.status.code() == Some(1)
                || report.to_ascii_lowercase().contains("repair is possible"),
            "shared PAR set must remain repairable after publication deletion: {report}"
        );
    }

    #[test]
    fn failed_source_keep_is_explicit_and_separate_from_recovery_evidence() {
        let mut request = job(vec![], vec![]);
        assert!(!job_keeps_failed_source(&request));
        let (_, disposable) = select_delivery_payload(
            &[payload_entry("incomplete.rar")],
            &Default::default(),
            &Default::default(),
            &Default::default(),
            &[(
                "incomplete.rar".into(),
                FileAssessment::Incomplete(
                    "unfinished segments without matching PAR recovery".into(),
                ),
            )]
            .into_iter()
            .collect(),
            &Default::default(),
        );
        assert!(recovery_reason(&disposable).is_none());
        request
            .params
            .push(("*Runner:KeepOnFailure".into(), "1".into()));
        assert!(job_keeps_failed_source(&request));
        let (_, repairable) = select_delivery_payload(
            &[payload_entry("incomplete.rar")],
            &Default::default(),
            &Default::default(),
            &Default::default(),
            &[(
                "incomplete.rar".into(),
                FileAssessment::Repairable(
                    "PAR repair: one damaged slice and two recovery blocks".into(),
                ),
            )]
            .into_iter()
            .collect(),
            &Default::default(),
        );
        assert!(recovery_reason(&repairable).is_some());
    }

    fn file(id: u32, name: &str, crc32: Option<u32>) -> FileEntry {
        FileEntry {
            id: FileId(id),
            subject: name.into(),
            filename: name.into(),
            filename_confirmed: true,
            is_par2: false,
            paused: false,
            groups: vec![],
            date: None,
            segments: vec![],
            crc32,
            finalized: true,
        }
    }

    /// Own the job's source as the real stage pipeline does: an admitted
    /// attempt fences the engine's artifact tick off it. Startup inventory
    /// reconciliation can still be joining, which the attempt reports as
    /// not quiesced.
    async fn admitted_attempt(
        engine: &nzbd_engine::EngineHandle,
        job: JobId,
    ) -> Arc<crate::attempt::AttemptControl> {
        let ctx = PpCtx::default();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match crate::attempt::AttemptControl::new(engine, &ctx, job).await {
                    Err(PostError::Subprocess(reason))
                        if reason
                            == "previous processing or allocation workers have not quiesced" =>
                    {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    other => break other.unwrap(),
                }
            }
        })
        .await
        .expect("fixture must acquire exact source custody after startup joins")
    }

    fn job(files: Vec<FileEntry>, params: Vec<(String, String)>) -> Job {
        Job {
            id: JobId(41),
            kind: JobKind::Nzb,
            name: "Some.Release".into(),
            dir_name: "Some.Release".into(),
            name_provisional: false,
            delivery_lifecycle: false,
            queued_at_unix: 0,
            original_name: String::new(),
            category: Some("tv".into()),
            priority: 0,
            dupe: DupeInfo::default(),
            params,
            files,
            totals: JobTotals::default(),
            status: JobStatus::Completed,
            torrent: None,
            stages: Vec::new(),
        }
    }

    #[test]
    fn historical_post_spans_do_not_acquire_a_current_process_origin() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("state");
        let inventory = nzbd_state::artifacts::Inventory::open(&path).unwrap();
        let mut row = job(Vec::new(), Vec::new());
        row.stages = vec![
            nzbd_types::StageSpan {
                stage: nzbd_types::PostStage::ParVerify,
                started_at_unix: 100,
                ms: Some(17),
            },
            nzbd_types::StageSpan {
                stage: nzbd_types::PostStage::Move,
                started_at_unix: 200,
                ms: None,
            },
        ];
        let first = delivery_stage_snapshot(&inventory, &row).unwrap();
        inventory.close().unwrap();
        drop(inventory);
        let restarted = nzbd_state::artifacts::Inventory::open(&path).unwrap();
        let replay = delivery_stage_snapshot(&restarted, &row).unwrap();
        assert_eq!(first, replay);
        assert_eq!(replay.node_id, "unrecorded");
        assert_eq!(replay.clock_domain, "unrecorded");
        assert_eq!(replay.spans, row.stages);
    }

    #[tokio::test]
    async fn durable_delivery_recovers_history_after_consumer_removes_publication() {
        let tmp = tempfile::tempdir().unwrap();
        let history = Arc::new(
            HistoryDb::open(&tmp.path().join("history.sqlite"), Some(tmp.path())).unwrap(),
        );
        let mut config = nzbd_engine::EngineConfig::single_node(
            Vec::new(),
            tmp.path().join("queue"),
            tmp.path().join("downloads"),
            nzbd_engine::Tuning::default(),
            None,
        );
        config.artifact_dir = Some(tmp.path().join("artifacts"));
        config.history = Some(history.clone());
        let engine = nzbd_engine::Engine::spawn(config).await.unwrap();
        let mut job = job(vec![file(1, "movie.mkv", None)], vec![]);
        job.delivery_lifecycle = true;
        job.status = JobStatus::PostQueued;
        job.params
            .push(("monarr-transfer".into(), "transfer-41".into()));
        job.params.push(("monarr-delivery".into(), "1".into()));
        let inventory = engine.artifacts();
        let downloads = tmp.path().join("downloads");
        std::fs::create_dir(&downloads).unwrap();
        let source_path = downloads.join(nzbd_engine::queue::job_dir_name(&job));
        inventory
            .allocate_for_delivery(41, &downloads, &source_path, true, true)
            .unwrap();

        let source = inventory.for_job(41).unwrap().unwrap();
        std::fs::write(source.path.join("movie.mkv"), b"payload").unwrap();
        inventory
            .finish(41, &source.path, &source.root, "active")
            .unwrap();
        engine
            .import_fixture_job(job.clone(), false, false)
            .await
            .unwrap();
        let source = inventory.refresh_for_delivery(41).unwrap();
        let root = tmp.path().join("completed");
        std::fs::create_dir_all(&root).unwrap();
        let published = root.join(format!("release.runner-{}", source.generation));
        let selected = source
            .files
            .iter()
            .filter(|entry| entry.path == "movie.mkv")
            .cloned()
            .collect();
        inventory
            .publish_selected_for_delivery_checked(41, &published, selected, Vec::new(), &|| Ok(()))
            .unwrap();
        inventory
            .finish(41, &published, &root, "completed")
            .unwrap();
        let delivery = inventory
            .create_delivery(41, true, Some("transfer-41"))
            .unwrap();
        std::fs::remove_dir_all(&published).unwrap();

        assert_eq!(
            resume_durable_delivery(&engine, &history, JobId(41))
                .await
                .unwrap(),
            Some(PpFinal::Success)
        );
        let entry = history.get(JobId(41)).unwrap().unwrap();
        assert_eq!(entry.status, "SUCCESS");
        assert_eq!(
            entry.final_dir.as_deref(),
            Some(published.to_str().unwrap())
        );
        assert!(entry
            .params
            .iter()
            .any(|(key, value)| key == "Delivery:Id" && value == &delivery.delivery_id));
        assert_eq!(
            engine.export_job(JobId(41)).await.unwrap().unwrap().status,
            JobStatus::Completed
        );
        assert!(!published.exists());
        engine.shutdown().await;
    }

    struct ObfuscatedRelease {
        _temp: tempfile::TempDir,
        engine: EngineHandle,
        history: Arc<HistoryDb>,
        downloads: PathBuf,
        completed: PathBuf,
        source: PathBuf,
        hash: &'static str,
        media: Vec<u8>,
    }

    /// The field shape: an obfuscated archive (`<hash>.zip`, standing in for
    /// `<hash>.partNN.rar`) holding obfuscated media (`<hash>.mkv`), on a
    /// Curator-lifecycle job, optionally with a PAR2 set whose catalog names
    /// the obfuscated archive.
    async fn obfuscated_release(with_par2: bool) -> ObfuscatedRelease {
        let temp = tempfile::tempdir().unwrap();
        let downloads = temp.path().join("downloads");
        std::fs::create_dir(&downloads).unwrap();
        let completed = temp.path().join("completed");
        let history = Arc::new(
            HistoryDb::open(&temp.path().join("history.sqlite"), Some(temp.path())).unwrap(),
        );
        let mut config = nzbd_engine::EngineConfig::single_node(
            Vec::new(),
            temp.path().join("queue"),
            downloads.clone(),
            nzbd_engine::Tuning::default(),
            None,
        );
        config.artifact_dir = Some(temp.path().join("artifacts"));
        config.history = Some(history.clone());
        let engine = nzbd_engine::Engine::spawn(config).await.unwrap();
        let inventory = engine.artifacts();
        let hash = "9abad49b5b2859fa648bbb2a5286671e";
        let media = vec![b'M'; 64 * 1024];
        let staging = temp.path().join("posted");
        std::fs::create_dir(&staging).unwrap();
        std::fs::write(staging.join(format!("{hash}.mkv")), &media).unwrap();
        assert!(std::process::Command::new("7z")
            .args(["a", "-tzip", "-y"])
            .arg(format!("{hash}.zip"))
            .arg(format!("{hash}.mkv"))
            .current_dir(&staging)
            .status()
            .unwrap()
            .success());
        std::fs::remove_file(staging.join(format!("{hash}.mkv"))).unwrap();
        if with_par2 {
            assert!(std::process::Command::new("par2")
                .args(["create", "-q", "-q", "-s8192", "-c4"])
                .arg(format!("{hash}.par2"))
                .arg(format!("{hash}.zip"))
                .current_dir(&staging)
                .status()
                .unwrap()
                .success());
        }
        let mut files = Vec::new();
        let mut names = std::fs::read_dir(&staging)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        names.sort();
        for (index, name) in names.iter().enumerate() {
            let bytes = std::fs::read(staging.join(name)).unwrap();
            let mut entry = file(index as u32 + 1, name, Some(crc32fast::hash(&bytes)));
            entry.is_par2 = name.ends_with(".par2");
            entry.segments = vec![Segment {
                message_id: format!("posted-{index}").into(),
                number: 1,
                size: bytes.len() as u32,
                state: SegmentState::Done {
                    offset: 0,
                    len: bytes.len() as u32,
                    crc: crc32fast::hash(&bytes),
                },
            }];
            files.push(entry);
        }
        let mut request = job(
            files,
            vec![
                ("monarr-transfer".into(), "obfuscated-token".into()),
                ("monarr-delivery".into(), "1".into()),
            ],
        );
        request.delivery_lifecycle = true;
        request.status = JobStatus::PostQueued;
        let source_path = downloads.join(nzbd_engine::queue::job_dir_name(&request));
        inventory
            .allocate_for_delivery(41, &downloads, &source_path, true, true)
            .unwrap();

        let source = inventory.for_job(41).unwrap().unwrap().path;
        for name in &names {
            std::fs::copy(staging.join(name), source.join(name)).unwrap();
        }
        inventory.finish(41, &source, &downloads, "active").unwrap();
        engine
            .import_fixture_job(request.clone(), false, false)
            .await
            .unwrap();
        ObfuscatedRelease {
            _temp: temp,
            engine,
            history,
            downloads,
            completed,
            source,
            hash,
            media,
        }
    }

    impl ObfuscatedRelease {
        fn cfg(&self) -> PostConfig {
            PostConfig {
                completed_dir: Some(self.completed.clone()),
                ..PostConfig::default()
            }
        }

        async fn process(&self) -> Result<PpFinal, PostError> {
            process_job_ctx(
                &self.engine,
                &self.cfg(),
                &self.history,
                &self.downloads,
                JobId(41),
                &PpCtx::default(),
            )
            .await
        }

        fn publication(&self) -> PathBuf {
            let generation = self
                .engine
                .artifacts()
                .for_job(41)
                .unwrap()
                .unwrap()
                .generation;
            self.completed
                .join(format!("Some.Release.job-41.runner-{generation}"))
        }

        /// The publication holds exactly the release-named media.
        fn assert_published_once(&self, move_attempts: usize) {
            let published = self.publication();
            assert_eq!(
                std::fs::read(published.join("Some.Release.mkv")).unwrap(),
                self.media
            );
            let media = std::fs::read_dir(&published)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .filter(|name| name.ends_with(".mkv"))
                .collect::<Vec<_>>();
            assert_eq!(media, vec!["Some.Release.mkv".to_string()]);
            assert_eq!(
                self.history.get(JobId(41)).unwrap().unwrap().status,
                "SUCCESS"
            );
            let history = self.history.get(JobId(41)).unwrap().unwrap();
            let delivery = self
                .engine
                .artifacts()
                .delivery_for_job(41)
                .unwrap()
                .unwrap();
            assert_eq!(
                delivery.runner_stages.unwrap().spans,
                history.stages,
                "Delivery and history must freeze the same closed timeline"
            );
            let moves: Vec<_> = history
                .stages
                .iter()
                .filter(|span| span.stage == PostStage::Move)
                .collect();
            assert_eq!(
                moves.len(),
                move_attempts,
                "each publication attempt must survive the terminal snapshot"
            );
            assert!(
                moves.iter().all(|span| span.ms.is_some()),
                "history must freeze every Move span closed"
            );
        }
    }

    /// Field report 2026-10-27: obfuscated releases (`<hash>.partNN.rar`
    /// holding `<hash>.mkv`) extracted cleanly, then the final deobfuscation
    /// pass renamed the media to the release name. The rename changed the
    /// file's ctime, the extraction proof stopped matching, and every such
    /// download was held with nothing publishable.
    #[tokio::test]
    async fn deobfuscated_extraction_output_is_published_under_its_release_name() {
        if !crate::tools::require_tool("7z") {
            return;
        }
        let release = obfuscated_release(false).await;
        let outcome = release.process().await;
        assert!(
            matches!(outcome, Ok(PpFinal::Success)),
            "deobfuscated extraction must publish: {outcome:?}"
        );
        release.assert_published_once(1);
        assert!(!release
            .publication()
            .join(format!("{}.mkv", release.hash))
            .exists());
        let inventory = release.engine.artifacts();
        let delivery = inventory
            .delivery_for_job(41)
            .unwrap()
            .expect("Curator delivery");
        let outcomes = inventory
            .delivery_file_outcomes(&delivery.artifact_id, &delivery.publication_generation)
            .unwrap();
        let archive_row = outcomes
            .iter()
            .find(|row| row.source_path == "Some.Release.zip")
            .expect("renamed archive input is classified");
        assert_eq!(archive_row.outcome, "consumed");
        release.engine.shutdown().await;
    }

    /// The held field jobs are retried with the operator's Retry, which
    /// restarts post-processing from the beginning. By then the volumes carry
    /// the release name (and a PAR2 catalog may rename them back to the
    /// obfuscated name). Neither spelling may cause a second extraction.
    #[tokio::test]
    async fn retry_from_beginning_after_deobfuscation_does_not_extract_twice() {
        if !crate::tools::require_tool("7z") || !crate::tools::require_tool("par2") {
            return;
        }
        for with_par2 in [false, true] {
            let release = obfuscated_release(with_par2).await;
            // Hold the first attempt after deobfuscation and before
            // publication, as the field jobs were.
            let blocker = release.publication();
            std::fs::create_dir_all(&blocker).unwrap();
            std::fs::write(blocker.join("occupied"), b"x").unwrap();
            assert!(
                release.process().await.is_err(),
                "occupied publication target must stop the first attempt"
            );
            assert!(release.source.join("Some.Release.mkv").exists());
            std::fs::remove_dir_all(&blocker).unwrap();
            // Re-extraction would fail: the consumed set must be recognized.
            let outcome = process_job_ctx(
                &release.engine,
                &PostConfig {
                    sevenzip_cmd: "/no-such-extractor".into(),
                    ..release.cfg()
                },
                &release.history,
                &release.downloads,
                JobId(41),
                &PpCtx::default(),
            )
            .await;
            assert!(
                matches!(outcome, Ok(PpFinal::Success)),
                "retry (par2={with_par2}) must publish without re-extracting: {outcome:?}"
            );
            release.assert_published_once(2);
            release.engine.shutdown().await;
        }
    }

    fn rusqlite_disk_io_error() -> rusqlite::Error {
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
            Some("disk I/O error".into()),
        )
    }

    #[test]
    fn dropped_volume_errors_hold_as_storage_unavailable_through_any_wrapper() {
        for error in [
            PostError::Subprocess(
                "restore /processing/x -> /processing/y: Transport endpoint is not connected (os error 107)".into(),
            ),
            PostError::Io(std::io::Error::from_raw_os_error(libc::EIO)),
            PostError::Artifact(nzbd_state::artifacts::Error::Io(
                std::io::Error::from_raw_os_error(libc::ESTALE),
            )),
        ] {
            assert_eq!(
                pp_verdict(&error, Some(PostStage::Unpack)),
                PpVerdict::Hold(HoldCause::StorageUnavailable),
                "{error}"
            );
        }
        let sqlite_ioerr = rusqlite_disk_io_error();
        assert_eq!(
            pp_verdict(
                &PostError::Artifact(nzbd_state::artifacts::Error::Sql(sqlite_ioerr)),
                None
            ),
            PpVerdict::Hold(HoldCause::StorageUnavailable),
            "SQLITE_IOERR on the state volume carries no errno suffix"
        );
        assert_eq!(
            pp_verdict(
                &PostError::Io(std::io::Error::other(
                    "No such file or directory (os error 2)"
                )),
                Some(PostStage::Move)
            ),
            PpVerdict::Fail(nzbd_types::FailureReason::Io),
            "a missing file is not a dropped volume: it is a terminal failed/io"
        );
        // M6: an io::Error is classified by its errno, never its message.
        assert_eq!(
            pp_verdict(
                &PostError::Io(std::io::Error::other(
                    "read /processing/x: Input/output error (os error 5)"
                )),
                Some(PostStage::Move)
            ),
            PpVerdict::Fail(nzbd_types::FailureReason::Io),
        );
        assert_eq!(
            pp_verdict(
                &PostError::Io(std::io::Error::from_raw_os_error(libc::ENOSPC)),
                Some(PostStage::Unpack)
            ),
            PpVerdict::Hold(HoldCause::Capacity),
        );
        assert_eq!(
            pp_verdict(
                &PostError::Io(std::io::Error::from_raw_os_error(libc::EDQUOT)),
                Some(PostStage::Unpack)
            ),
            PpVerdict::Hold(HoldCause::Quota),
        );
    }

    /// Contract §2.2: a non-environmental post-processing error is the
    /// job's terminal failure, typed by the stage it stopped in; a release
    /// title never steers the verdict.
    #[test]
    fn nothing_publishable_is_a_terminal_publish_failure_whatever_the_title() {
        use nzbd_types::FailureReason;
        for title in [
            "The.Bourne.Identity.2002.mkv: no integrity evidence",
            "Unsafe.2026.mkv: collision",
            "No space left on device.mkv: missing",
        ] {
            assert_eq!(
                pp_verdict(&PostError::NothingPublishable(title.into()), None),
                PpVerdict::Fail(FailureReason::Publish)
            );
        }
        for (stage, reason) in [
            (PostStage::ParVerify, FailureReason::Par),
            (PostStage::ParRepair, FailureReason::Par),
            (PostStage::Unpack, FailureReason::Unpack),
            (PostStage::Script, FailureReason::Script),
            (PostStage::Move, FailureReason::Publish),
            (PostStage::PublicationVerify, FailureReason::Publish),
        ] {
            assert_eq!(
                pp_verdict(&PostError::Subprocess("tool failed".into()), Some(stage)),
                PpVerdict::Fail(reason),
                "{stage:?}"
            );
        }
        assert_eq!(
            pp_verdict(
                &PostError::ToolMissing("unrar".into()),
                Some(PostStage::Unpack)
            ),
            PpVerdict::Hold(HoldCause::MissingTool)
        );
        assert_eq!(
            pp_verdict(
                &PostError::Artifact(nzbd_state::artifacts::Error::Conflict(
                    "configured root identity changed or volume is unavailable".into()
                )),
                None
            ),
            PpVerdict::Hold(HoldCause::MountUnavailable)
        );
        assert_eq!(
            pp_verdict(
                &PostError::Artifact(nzbd_state::artifacts::Error::Conflict(
                    "restore source changed identity".into()
                )),
                None
            ),
            PpVerdict::Fail(FailureReason::Publish),
            "an inventory refusal parks without blaming the release"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_reads_retry_timeouts_and_use_the_live_keep_setting() {
        for keep in [false, true] {
            let mut live = job(vec![], vec![]);
            if keep {
                live.params
                    .push(("*Runner:KeepOnFailure".into(), "1".into()));
            }
            let mut calls = 0;
            let current = read_post_job(live.id, "read live Keep setting", || {
                calls += 1;
                std::future::ready(if calls < 3 {
                    Err(nzbd_engine::EngineError::Timeout(
                        "waiting for the owner reply",
                    ))
                } else {
                    Ok(Some(live.clone()))
                })
            })
            .await
            .unwrap()
            .unwrap();
            assert_eq!(calls, 3);
            assert_eq!(job_keeps_failed_source(&current), keep);
        }
        let mut calls = 0;
        let error = read_post_job(JobId(41), "read live Keep setting", || {
            calls += 1;
            std::future::ready(Err(nzbd_engine::EngineError::Timeout(
                "waiting for the owner reply",
            )))
        })
        .await
        .unwrap_err();
        assert_eq!(calls, 3, "persistent stalls leave the bounded read loop");
        assert_eq!(
            pp_verdict(&error, None),
            PpVerdict::Hold(HoldCause::EngineUnavailable)
        );
        assert!(HoldCause::EngineUnavailable.policy().timed());
        assert!(!error.to_string().contains("subprocess"));
        assert!(!error.to_string().contains("shutting down"));
        for source in [
            nzbd_engine::EngineError::Closed,
            nzbd_engine::EngineError::OwnershipLost,
        ] {
            let mut source = Some(source);
            let mut calls = 0;
            let error = read_post_job(JobId(41), "read terminal job", || {
                calls += 1;
                std::future::ready(Err(source.take().expect("no retry of non-timeout errors")))
            })
            .await
            .unwrap_err();
            assert_eq!(calls, 1);
            assert!(matches!(error, PostError::Engine { .. }));
        }
        let absent = read_post_job(JobId(41), "read live Keep setting", || {
            std::future::ready(Ok(None))
        })
        .await
        .unwrap();
        assert!(
            absent.as_ref().is_none_or(job_keeps_failed_source),
            "a disappearing job must never authorize disposal"
        );
    }

    #[test]
    fn nothing_publishable_names_the_payload_and_what_it_lacks() {
        let row = |path: &str, classification: &str, reason: Option<&str>| DeliveryFileOutcome {
            source_path: path.into(),
            publication_path: None,
            classification: classification.into(),
            outcome: "unverified_review".into(),
            reason: reason.map(str::to_owned),
            repair_dependency: false,
            source_cleanup_status: String::new(),
        };
        let summary = unpublishable_summary(&[
            row("Show.S01E01.part01.rar", "archive_input", Some("archive")),
            row(
                "sub/Show.S01E01.mkv",
                "video",
                Some("no integrity evidence"),
            ),
        ]);
        assert_eq!(
            summary,
            "no file has integrity evidence for publication; source retained (Show.S01E01.part01.rar: archive; Show.S01E01.mkv: no integrity evidence)"
        );
        assert_eq!(
            unpublishable_summary(&[row("x.rar", "archive_input", None)]),
            "no file has integrity evidence for publication; source retained (x.rar)"
        );
        assert_eq!(
            PostError::NothingPublishable(summary).to_string(),
            "nothing publishable: no file has integrity evidence for publication; source retained (Show.S01E01.part01.rar: archive; Show.S01E01.mkv: no integrity evidence)"
        );
    }

    #[tokio::test]
    async fn process_restart_finishes_committed_extracted_publication_without_rerunning_tools() {
        if !crate::tools::require_tool("7z") {
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let downloads = temp.path().join("downloads");
        std::fs::create_dir(&downloads).unwrap();
        let completed = temp.path().join("completed");
        let history = Arc::new(
            HistoryDb::open(&temp.path().join("history.sqlite"), Some(temp.path())).unwrap(),
        );
        let mut config = nzbd_engine::EngineConfig::single_node(
            Vec::new(),
            temp.path().join("queue"),
            downloads.clone(),
            nzbd_engine::Tuning::default(),
            None,
        );
        config.artifact_dir = Some(temp.path().join("artifacts"));
        config.history = Some(history.clone());
        let engine = nzbd_engine::Engine::spawn(config).await.unwrap();
        let inventory = engine.artifacts();
        std::fs::write(temp.path().join("episode.mkv"), b"verified extracted media").unwrap();
        assert!(std::process::Command::new("7z")
            .args(["a", "-tzip", "-y", "episode.zip", "episode.mkv"])
            .current_dir(temp.path())
            .status()
            .unwrap()
            .success());
        let archive_bytes = std::fs::read(temp.path().join("episode.zip")).unwrap();
        let mut archive = file(1, "episode.zip", Some(crc32fast::hash(&archive_bytes)));
        archive.segments = vec![Segment {
            message_id: "complete-archive".into(),
            number: 1,
            size: archive_bytes.len() as u32,
            state: SegmentState::Done {
                offset: 0,
                len: archive_bytes.len() as u32,
                crc: crc32fast::hash(&archive_bytes),
            },
        }];
        let mut request = job(
            vec![archive],
            vec![
                ("monarr-transfer".into(), "restart-token".into()),
                ("monarr-delivery".into(), "1".into()),
                (PP_SELECTED_OUTCOME_PARAM.into(), "SUCCESS".into()),
            ],
        );
        request.delivery_lifecycle = true;
        request.status = JobStatus::PostQueued;
        let source_path = downloads.join(nzbd_engine::queue::job_dir_name(&request));
        inventory
            .allocate_for_delivery(41, &downloads, &source_path, true, true)
            .unwrap();

        let source = inventory.for_job(41).unwrap().unwrap().path;
        std::fs::copy(temp.path().join("episode.zip"), source.join("episode.zip")).unwrap();
        inventory.finish(41, &source, &downloads, "active").unwrap();
        engine
            .import_fixture_job(request.clone(), false, false)
            .await
            .unwrap();
        let attempt = admitted_attempt(&engine, request.id).await;
        crate::attempt::CURRENT
            .scope(attempt, async {
                extract_complete_failed_archives(&engine, &PostConfig::default(), &request, &None)
                    .await
            })
            .await
            .unwrap();
        let source_artifact = inventory.refresh_for_delivery(41).unwrap();
        let (consumed, verified) = inventory.verified_transform_evidence(41).unwrap();
        let diagnostics = assess_file_inputs(&request, &source_artifact, &verified).unwrap();
        let (selected, outcomes) = select_delivery_payload(
            &source_artifact.files,
            &consumed,
            &verified,
            &Default::default(),
            &diagnostics,
            &Default::default(),
        );
        assert!(!selected.is_empty());
        let published = completed.join(format!(
            "restart-pack.runner-{}",
            source_artifact.generation
        ));
        inventory
            .publish_selected_for_delivery_checked(41, &published, selected, outcomes, &|| Ok(()))
            .unwrap();
        assert!(inventory.delivery_for_job(41).unwrap().is_none());
        assert!(source.join("episode.zip").exists());
        let broken_tool = temp.path().join("no-such-7z");
        let cfg = PostConfig {
            completed_dir: Some(completed),
            sevenzip_cmd: broken_tool.to_string_lossy().into_owned(),
            ..PostConfig::default()
        };
        assert_eq!(
            process_job_ctx(
                &engine,
                &cfg,
                &history,
                &downloads,
                JobId(41),
                &PpCtx::default()
            )
            .await
            .unwrap(),
            PpFinal::Success
        );
        assert!(published.join("episode.mkv").exists());
        assert!(inventory.delivery_for_job(41).unwrap().is_some());
        // The terminal is recorded first, then the delivery row, then the
        // source retires with it — the first attempt's order (review #283).
        assert!(
            !source.join("episode.zip").exists(),
            "the consumed archive retires with the delivery"
        );
        assert_eq!(history.get(JobId(41)).unwrap().unwrap().status, "SUCCESS");
    }

    #[tokio::test]
    async fn failed_health_restart_reuses_extracted_selected_publication_before_delivery() {
        if !crate::tools::require_tool("7z") {
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let downloads = temp.path().join("downloads");
        std::fs::create_dir(&downloads).unwrap();
        let completed = temp.path().join("completed");
        let history = Arc::new(
            HistoryDb::open(&temp.path().join("history.sqlite"), Some(temp.path())).unwrap(),
        );
        let mut config = nzbd_engine::EngineConfig::single_node(
            Vec::new(),
            temp.path().join("queue"),
            downloads.clone(),
            nzbd_engine::Tuning::default(),
            None,
        );
        config.artifact_dir = Some(temp.path().join("artifacts"));
        config.history = Some(history.clone());
        let engine = nzbd_engine::Engine::spawn(config).await.unwrap();
        let inventory = engine.artifacts();
        std::fs::write(temp.path().join("episode.mkv"), b"complete episode").unwrap();
        assert!(std::process::Command::new("7z")
            .args(["a", "-tzip", "-y", "episode.zip", "episode.mkv"])
            .current_dir(temp.path())
            .status()
            .unwrap()
            .success());
        let archive_bytes = std::fs::read(temp.path().join("episode.zip")).unwrap();
        let mut archive = file(1, "episode.zip", Some(crc32fast::hash(&archive_bytes)));
        archive.segments = vec![Segment {
            message_id: "complete".into(),
            number: 1,
            size: archive_bytes.len() as u32,
            state: SegmentState::Done {
                offset: 0,
                len: archive_bytes.len() as u32,
                crc: crc32fast::hash(&archive_bytes),
            },
        }];
        let mut incomplete = file(2, "incomplete.rar", None);
        incomplete.segments = vec![Segment {
            message_id: "failed".into(),
            number: 1,
            size: 100,
            state: SegmentState::Failed,
        }];
        let cause = "download health 0 did not meet the configured completion threshold";
        let mut request = job(
            vec![archive, incomplete],
            vec![
                ("monarr-transfer".into(), "health-restart-token".into()),
                ("monarr-delivery".into(), "1".into()),
                ("*Runner:KeepOnFailure".into(), "1".into()),
                (PP_SELECTED_OUTCOME_PARAM.into(), "FAILURE/HEALTH".into()),
                (PP_SELECTED_FAILURE_CAUSE_PARAM.into(), cause.into()),
            ],
        );
        request.delivery_lifecycle = true;
        request.status = JobStatus::Failed;
        let source_path = downloads.join(nzbd_engine::queue::job_dir_name(&request));
        inventory
            .allocate_for_delivery_with_keep(41, &downloads, &source_path, true, true, true)
            .unwrap();

        let source = inventory.for_job(41).unwrap().unwrap().path;
        std::fs::copy(temp.path().join("episode.zip"), source.join("episode.zip")).unwrap();
        std::fs::write(source.join("incomplete.rar"), b"Rar!\x1a\x07partial").unwrap();
        inventory.finish(41, &source, &downloads, "active").unwrap();
        engine
            .import_fixture_job(request.clone(), false, false)
            .await
            .unwrap();
        let attempt = admitted_attempt(&engine, request.id).await;
        crate::attempt::CURRENT
            .scope(attempt, async {
                extract_complete_failed_archives(&engine, &PostConfig::default(), &request, &None)
                    .await
            })
            .await
            .unwrap();
        let source_artifact = inventory.refresh_for_delivery(41).unwrap();
        let original_generation = source_artifact.generation.clone();
        let (consumed, verified) = inventory.verified_transform_evidence(41).unwrap();
        let diagnostics = assess_file_inputs(&request, &source_artifact, &verified).unwrap();
        let (selected, outcomes) = select_delivery_payload(
            &source_artifact.files,
            &consumed,
            &verified,
            &Default::default(),
            &diagnostics,
            &Default::default(),
        );
        let published = completed.join(format!("failed-archive-pack.runner-{original_generation}"));
        let relocation = inventory
            .publish_selected_for_delivery_checked(41, &published, selected, outcomes, &|| Ok(()))
            .unwrap();
        let retained_source_id = format!("source-{}", relocation.operation_id);
        let retained_source = inventory.get(&retained_source_id).unwrap();
        assert_eq!(retained_source.path, source);
        assert_eq!(retained_source.generation, original_generation);
        assert!(inventory.delivery_for_job(41).unwrap().is_none());
        assert!(source.join("incomplete.rar").exists());
        let cfg = PostConfig {
            completed_dir: Some(completed),
            sevenzip_cmd: temp
                .path()
                .join("missing-7z")
                .to_string_lossy()
                .into_owned(),
            ..PostConfig::default()
        };
        fail_and_follow_up(
            &engine,
            &cfg,
            &history,
            &downloads,
            &None,
            JobId(41),
            "failed-archive-pack".into(),
            0,
            false,
        )
        .await;
        let delivery = inventory.delivery_for_job(41).unwrap().unwrap();
        assert_eq!(delivery.source_artifact_id, retained_source_id);
        assert_eq!(delivery.source_generation, original_generation);
        assert_eq!(delivery.terminal_outcome, "FAILURE/HEALTH");
        assert_eq!(delivery.failure_cause.as_deref(), Some(cause));
        assert!(published.join("episode.mkv").exists());
        assert!(source.join("incomplete.rar").exists());
        assert_eq!(
            history.get(JobId(41)).unwrap().unwrap().status,
            "FAILURE/HEALTH"
        );
        fail_and_follow_up(
            &engine,
            &cfg,
            &history,
            &downloads,
            &None,
            JobId(41),
            "failed-archive-pack".into(),
            0,
            false,
        )
        .await;
        let replayed = inventory.delivery_for_job(41).unwrap().unwrap();
        assert_eq!(replayed.delivery_id, delivery.delivery_id);
        assert_eq!(replayed.sequence, delivery.sequence);
    }

    #[tokio::test]
    async fn partial_health_delivery_replays_failure_after_consumer_removes_payload() {
        let tmp = tempfile::tempdir().unwrap();
        let history = Arc::new(
            HistoryDb::open(&tmp.path().join("history.sqlite"), Some(tmp.path())).unwrap(),
        );
        let mut config = nzbd_engine::EngineConfig::single_node(
            Vec::new(),
            tmp.path().join("queue"),
            tmp.path().join("downloads"),
            nzbd_engine::Tuning::default(),
            None,
        );
        config.artifact_dir = Some(tmp.path().join("artifacts"));
        config.history = Some(history.clone());
        let engine = nzbd_engine::Engine::spawn(config).await.unwrap();
        let mut job = job(vec![file(1, "good-episode.mkv", None)], vec![]);
        job.id = JobId(42);
        job.delivery_lifecycle = true;
        job.status = JobStatus::PostQueued;
        job.params
            .push(("monarr-transfer".into(), "transfer-42".into()));
        job.params.push(("monarr-delivery".into(), "1".into()));
        let inventory = engine.artifacts();
        let downloads = tmp.path().join("downloads");
        std::fs::create_dir(&downloads).unwrap();
        let source_path = downloads.join(nzbd_engine::queue::job_dir_name(&job));
        inventory
            .allocate_for_delivery(42, &downloads, &source_path, true, true)
            .unwrap();

        let source = inventory.for_job(42).unwrap().unwrap();
        std::fs::write(
            source.path.join("good-episode.mkv"),
            b"verified partial payload",
        )
        .unwrap();
        inventory
            .finish(42, &source.path, &source.root, "active")
            .unwrap();
        engine.import_fixture_job(job, false, false).await.unwrap();
        let source = inventory.refresh_for_delivery(42).unwrap();
        let root = tmp.path().join("completed");
        std::fs::create_dir_all(&root).unwrap();
        let published = root.join(format!("release.runner-{}", source.generation));
        let selected = source
            .files
            .iter()
            .filter(|entry| entry.path == "good-episode.mkv")
            .cloned()
            .collect();
        inventory
            .publish_selected_for_delivery_checked(42, &published, selected, Vec::new(), &|| Ok(()))
            .unwrap();
        inventory
            .finish(42, &published, &root, "completed")
            .unwrap();
        let delivery = inventory
            .create_delivery_with_outcome_and_keep(
                42,
                true,
                Some("transfer-42"),
                "FAILURE/HEALTH",
                Some("health"),
                Some("health threshold failed after one episode completed"),
                true,
            )
            .unwrap();
        assert_eq!(delivery.kind(), "salvage");
        std::fs::remove_dir_all(&published).unwrap();

        assert_eq!(
            resume_durable_delivery(&engine, &history, JobId(42))
                .await
                .unwrap(),
            Some(PpFinal::HealthFailure)
        );
        let entry = history.get(JobId(42)).unwrap().unwrap();
        assert_eq!(entry.status, "FAILURE/HEALTH");
        assert!(entry
            .params
            .iter()
            .any(|(key, value)| key == "Failure:KeepOnFailure" && value == "true"));
        assert!(entry.params.iter().any(|(key, value)| {
            key == "Failure:Reason"
                && value == "health threshold failed after one episode completed"
        }));
        engine.shutdown().await;
    }

    /// `failure_action` is operator-typed, so its parse decides whether a
    /// known-bad download is deleted or kept. An unrecognised spelling must
    /// land on `Delete` — the documented default — and never leave the
    /// files behind by accident, which is what filled the terabyte.
    #[test]
    fn failure_action_parses_every_documented_spelling_and_defaults_to_delete() {
        for keep in ["none", "None", "NONE", "keep", "Keep", ""] {
            assert_eq!(FailureAction::parse(keep), FailureAction::None, "{keep:?}");
        }
        for park in ["park", "Park", "PARK", "pause", "Pause"] {
            assert_eq!(FailureAction::parse(park), FailureAction::Park, "{park:?}");
        }
        for delete in ["delete", "Delete", "remove", "yes", "   "] {
            assert_eq!(
                FailureAction::parse(delete),
                FailureAction::Delete,
                "an unrecognised value must fall to the delete default, not to keep: {delete:?}"
            );
        }
        assert_eq!(FailureAction::default(), FailureAction::Delete);
    }

    #[test]
    fn recoverable_post_retry_backoff_caps_at_five_minutes_without_expiring() {
        assert_eq!(nzbd_types::hold_retry_delay_seconds(0), 30);
        assert_eq!(nzbd_types::hold_retry_delay_seconds(1), 60);
        assert_eq!(nzbd_types::hold_retry_delay_seconds(2), 120);
        assert_eq!(nzbd_types::hold_retry_delay_seconds(3), 300);
        assert_eq!(nzbd_types::hold_retry_delay_seconds(4), 300);
        assert_eq!(nzbd_types::hold_retry_delay_seconds(u8::MAX), 300);
    }

    #[test]
    fn strategy_slots_and_status_names_are_stable() {
        assert_eq!(strategy_slots("balanced"), 2);
        assert_eq!(strategy_slots("aggressive"), 3);
        assert_eq!(strategy_slots("rocket"), 6);
        // Unknown strategies run one job at a time rather than guessing.
        assert_eq!(strategy_slots("economy"), 1);
        assert_eq!(strategy_slots(""), 1);

        // These strings reach history rows, the API and NZBGet-compatible
        // scripts, so they are a wire format, not a display detail.
        assert_eq!(PpFinal::Success.as_str(), "SUCCESS");
        assert_eq!(PpFinal::ParFailure.as_str(), "PAR_FAILURE");
        assert_eq!(PpFinal::UnpackFailure.as_str(), "UNPACK_FAILURE");
        assert_eq!(PpFinal::ScriptFailure.as_str(), "SCRIPT_FAILURE");
        assert_eq!(PpFinal::HealthFailure.as_str(), "FAILURE/HEALTH");

        assert_eq!(RestartPoint::Beginning.as_str(), "beginning");
        assert_eq!(RestartPoint::Verify.as_str(), "verify");
        assert_eq!(RestartPoint::Unpack.as_str(), "unpack");
        assert_eq!(RestartPoint::Cleanup.as_str(), "cleanup");
        assert_eq!(RestartPoint::Move.as_str(), "move");
        assert_eq!(RestartPoint::Scripts.as_str(), "scripts");
    }

    /// A restart may rewind but never skip: restarting from a point the job
    /// has not reached would manufacture state the rest of the pipeline
    /// cannot prove.
    #[test]
    fn a_stage_permits_only_restart_points_at_or_before_itself() {
        use RestartPoint::*;
        let all = [Beginning, Verify, Unpack, Cleanup, Move, Scripts];
        // A limit of Beginning permits every point; a limit of Scripts
        // permits only Scripts.
        assert!(all.iter().all(|p| Beginning.includes(*p)));
        assert_eq!(
            all.iter().filter(|p| Scripts.includes(**p)).count(),
            1,
            "the last stage may only be restarted from itself"
        );
        assert!(Unpack.includes(Cleanup) && !Unpack.includes(Verify));

        assert_eq!(restart_limit(JobStatus::Completed), Some(Beginning));
        assert_eq!(restart_limit(JobStatus::PostQueued), Some(Beginning));
        for (stage, want) in [
            (PostStage::ParRename, Beginning),
            (PostStage::RarRename, Beginning),
            (PostStage::ParVerify, Verify),
            (PostStage::ParRepair, Verify),
            (PostStage::Unpack, Unpack),
            (PostStage::Cleanup, Cleanup),
            (PostStage::PostUnpackRename, Cleanup),
            (PostStage::Move, Move),
            (PostStage::Script, Scripts),
            (PostStage::PublicationVerify, Scripts),
        ] {
            assert_eq!(
                restart_limit(JobStatus::Post { stage }),
                Some(want),
                "{stage:?}"
            );
        }
        // A job that has not finished downloading has nothing to restart.
        for status in [
            JobStatus::Queued,
            JobStatus::Downloading,
            JobStatus::Paused,
            JobStatus::Fetching,
            JobStatus::Failed,
            JobStatus::Deleted,
        ] {
            assert_eq!(restart_limit(status), None, "{status:?}");
        }
    }

    /// The control path is a bounded mailbox precisely so a button click is
    /// never silently lost. During shutdown it must report that, not hang
    /// and not succeed.
    #[tokio::test]
    async fn restart_after_the_manager_is_gone_reports_closed() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let handle = PostManagerHandle { tx };
        drop(rx);
        assert_eq!(
            handle.restart(JobId(1), RestartPoint::Verify).await,
            Err(RestartPostError::Closed)
        );

        // A live receiver that never replies is the other half of the same
        // guarantee: the reply channel closing must surface as Closed too.
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let handle = PostManagerHandle { tx };
        let waiter =
            tokio::spawn(async move { handle.restart(JobId(2), RestartPoint::Move).await });
        let (job, from, reply) = match rx.recv().await.unwrap() {
            ManagerCommand::Restart { job, from, reply } => (job, from, reply),
            ManagerCommand::RetryNow { .. } => panic!("restart sent retry-now command"),
        };
        assert_eq!(job, JobId(2));
        assert_eq!(from, RestartPoint::Move);
        drop(reply);
        assert_eq!(waiter.await.unwrap(), Err(RestartPostError::Closed));
    }

    /// An *arr sending "TV" must land on `name = "tv"`.
    #[test]
    fn category_rules_match_case_insensitively_and_ignore_blanks() {
        let cfg = PostConfig {
            categories: vec![CategoryRule {
                name: "tv".into(),
                dest_dir: Some(PathBuf::from("/library/tv")),
                unpack: Some(false),
                extensions: vec!["clean".into()],
            }],
            ..PostConfig::default()
        };
        assert_eq!(cfg.rule_for(Some("TV")).unwrap().name, "tv");
        assert_eq!(cfg.rule_for(Some("  tv  ")).unwrap().name, "tv");
        assert!(cfg.rule_for(Some("movies")).is_none());
        assert!(cfg.rule_for(Some("   ")).is_none());
        assert!(cfg.rule_for(Some("")).is_none());
        assert!(cfg.rule_for(None).is_none());
    }

    /// An empty `extensions` list means "all of them", and a named one
    /// matches the file name or its stem, case-insensitively — both
    /// `Extensions=Clean.py` and `Extensions=clean` name the same script.
    #[test]
    fn category_extensions_select_scripts_by_name_or_stem() {
        let found = vec![
            PathBuf::from("/s/clean.py"),
            PathBuf::from("/s/notify.sh"),
            PathBuf::from("/s/archive"),
        ];
        assert_eq!(select_scripts(found.clone(), &[]), found);
        assert_eq!(
            select_scripts(found.clone(), &["Clean.py".into()]),
            vec![PathBuf::from("/s/clean.py")]
        );
        assert_eq!(
            select_scripts(found.clone(), &["CLEAN".into(), "archive".into()]),
            vec![PathBuf::from("/s/clean.py"), PathBuf::from("/s/archive")]
        );
        assert!(select_scripts(found, &["absent".into()]).is_empty());
    }

    #[tokio::test]
    async fn failure_action_none_keeps_the_files_where_they_are() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        let dir = dest.join("job");
        std::fs::create_dir_all(&dest).unwrap();
        let inventory =
            Arc::new(nzbd_state::artifacts::Inventory::open(&tmp.path().join("state")).unwrap());
        inventory.allocate(3, &dest, &dir).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("payload.bin"), b"bytes").unwrap();

        let cfg = PostConfig {
            failure_action: FailureAction::None,
            ..PostConfig::default()
        };
        let d = dispose_failed(inventory.clone(), &cfg, JobId(3), &dir, &dest, "job", "tag").await;
        assert_eq!(d.note, "kept");
        assert_eq!(d.files_at.as_deref(), Some(dir.as_path()));
        assert!(dir.join("payload.bin").exists());
    }

    #[tokio::test]
    async fn failure_action_delete_removes_the_tree_and_reports_no_files() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        let dir = dest.join("job");
        std::fs::create_dir_all(&dest).unwrap();
        let inventory =
            Arc::new(nzbd_state::artifacts::Inventory::open(&tmp.path().join("state")).unwrap());
        inventory.allocate(3, &dest, &dir).unwrap();
        std::fs::create_dir_all(dir.join("Subs")).unwrap();
        std::fs::write(dir.join("Subs/en.srt"), b"subs").unwrap();

        let cfg = PostConfig {
            failure_action: FailureAction::Delete,
            ..PostConfig::default()
        };
        let d = dispose_failed(inventory.clone(), &cfg, JobId(3), &dir, &dest, "job", "tag").await;
        assert_eq!(d.note, "deleted");
        assert_eq!(
            d.files_at, None,
            "a deleted job's row must not point at a directory"
        );
        assert!(!dir.exists());

        // Re-running the same disposition is the failover case: it must be
        // idempotent, not a second error.
        let again =
            dispose_failed(inventory.clone(), &cfg, JobId(3), &dir, &dest, "job", "tag").await;
        assert_eq!(again.note, "deleted");
        assert_eq!(again.files_at, None);
    }

    /// Park moves the files off the destination tree an importer watches,
    /// and defaults its target to `<dest>/.failed/<dir>` when no
    /// `failed_dir` is configured.
    #[tokio::test]
    async fn failure_action_park_moves_the_files_and_is_idempotent_after_a_lost_fence() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        let dir = dest.join("job");
        std::fs::create_dir_all(&dest).unwrap();
        let inventory =
            Arc::new(nzbd_state::artifacts::Inventory::open(&tmp.path().join("state")).unwrap());
        inventory.allocate(3, &dest, &dir).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("payload.bin"), b"bytes").unwrap();

        let cfg = PostConfig {
            failure_action: FailureAction::Park,
            ..PostConfig::default()
        };
        let parked = dest.join(".failed/job");
        let d = dispose_failed(inventory.clone(), &cfg, JobId(3), &dir, &dest, "job", "tag").await;
        assert_eq!(d.note, format!("parked at {}", parked.display()));
        assert_eq!(d.files_at.as_deref(), Some(parked.as_path()));
        assert_eq!(std::fs::read(parked.join("payload.bin")).unwrap(), b"bytes");
        assert!(!dir.exists());

        // A prior attempt completed the move and then lost its fence. The
        // successor sees no source and must recognise the existing target
        // as the witness rather than reporting the files gone.
        let successor =
            dispose_failed(inventory.clone(), &cfg, JobId(3), &dir, &dest, "job", "tag").await;
        assert_eq!(successor.note, d.note);
        assert_eq!(successor.files_at, d.files_at);

        // With neither source nor target, the files really are gone.
        std::fs::remove_dir_all(&parked).unwrap();
        let gone =
            dispose_failed(inventory.clone(), &cfg, JobId(3), &dir, &dest, "job", "tag").await;
        assert!(gone.note.starts_with("checked move pending:"));
        assert!(gone.files_at.is_some());
    }

    /// An explicit `failed_dir` overrides the default location.
    #[tokio::test]
    async fn park_honours_a_configured_failed_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        let dir = dest.join("job");
        std::fs::create_dir_all(&dest).unwrap();
        let inventory =
            Arc::new(nzbd_state::artifacts::Inventory::open(&tmp.path().join("state")).unwrap());
        inventory.allocate(3, &dest, &dir).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("payload.bin"), b"bytes").unwrap();
        let quarantine = tmp.path().join("quarantine");

        let cfg = PostConfig {
            failure_action: FailureAction::Park,
            failed_dir: Some(quarantine.clone()),
            ..PostConfig::default()
        };
        let d = dispose_failed(inventory.clone(), &cfg, JobId(3), &dir, &dest, "job", "tag").await;
        assert_eq!(
            d.files_at.as_deref(),
            Some(quarantine.join("job").as_path())
        );
        assert!(!dest.join(".failed").exists());
        assert_eq!(
            std::fs::read(quarantine.join("job/payload.bin")).unwrap(),
            b"bytes"
        );
    }

    /// A delete the filesystem refuses must leave the files and say so. The
    /// history row claiming "deleted" over files still on the volume is
    /// exactly the report an operator would act on wrongly.
    #[tokio::test]
    async fn a_refused_delete_keeps_the_files_and_names_the_error() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("dest");
        let dir = dest.join("job");
        std::fs::create_dir_all(&dest).unwrap();
        let inventory =
            Arc::new(nzbd_state::artifacts::Inventory::open(&tmp.path().join("state")).unwrap());
        inventory.allocate(3, &dest, &dir).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("payload.bin"), b"bytes").unwrap();

        // Take write permission off the job directory so its entries cannot
        // be unlinked. Root ignores the bit, so probe before relying on it.
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        if std::fs::remove_file(dir.join("payload.bin")).is_ok() {
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
            eprintln!("SKIPPED: this user unlinks through a read-only directory (running as root)");
            return;
        }

        let cfg = PostConfig {
            failure_action: FailureAction::Delete,
            ..PostConfig::default()
        };
        let d = dispose_failed(inventory.clone(), &cfg, JobId(3), &dir, &dest, "job", "tag").await;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(
            d.note.starts_with("checked deletion pending:"),
            "the note must carry the reason, was {:?}",
            d.note
        );
        assert_eq!(
            d.files_at.as_deref(),
            Some(dir.as_path()),
            "files that survived a failed delete must still be findable"
        );
        assert!(dir.join("payload.bin").exists());
    }

    /// Quick verification is handed the engine's own file list, remapped
    /// through de-obfuscation renames — a rename that happened before
    /// verification would otherwise point every piece of evidence at a path
    /// that no longer exists.
    #[test]
    fn evidence_follows_deobfuscation_renames() {
        let dir = Path::new("/downloads/Some.Release");
        let j = job(
            vec![
                file(1, "abc123.bin", Some(0xdead)),
                file(2, "plain.mkv", None),
            ],
            vec![],
        );
        let mut renames = HashMap::new();
        renames.insert(dir.join("abc123.bin"), dir.join("Some.Release.mkv"));

        let evidence = evidence_of(&j, dir, &renames);
        assert_eq!(evidence.len(), 2);
        assert_eq!(evidence[0].path, dir.join("Some.Release.mkv"));
        assert_eq!(evidence[0].crc32, Some(0xdead));
        assert_eq!(
            evidence[1].path,
            dir.join("plain.mkv"),
            "a file nothing renamed keeps its original path"
        );
        assert!(evidence.iter().all(|e| e.segment_crcs.is_empty()));
    }

    /// The `*`-prefixed params are nzbd's own bookkeeping (the PP-done
    /// stamp, the unpack password). They must not reach history rows or
    /// script environments as user params.
    #[test]
    fn internal_params_stay_out_of_history_and_script_environments() {
        let j = job(
            vec![],
            vec![
                ("mykey".into(), "myval".into()),
                (PP_DONE_PARAM.into(), "SUCCESS".into()),
                ("*Unpack:Password".into(), "hunter2".into()),
            ],
        );
        assert_eq!(
            nzbd_engine::terminal::history_params(&j),
            vec![("mykey".into(), "myval".into())]
        );
        assert!(
            pp_done(&j),
            "the stamp is what stops a re-run after a restart"
        );

        let env = script_env(&j, Path::new("/downloads/job"), true, false, true, true);
        assert!(env.iter().any(|(k, v)| k == "NZBPR_mykey" && v == "myval"));
        assert!(
            !env.iter()
                .any(|(k, v)| k.contains("Password") || v == "hunter2"),
            "the unpack password must never be exported to an operator script"
        );

        let clean = job(vec![], vec![]);
        assert!(!pp_done(&clean));
    }

    /// The NZBGet-compatible status codes existing scripts branch on.
    #[test]
    fn script_environment_reports_nzbget_status_codes() {
        let j = job(vec![], vec![]);
        let dir = Path::new("/downloads/Some.Release");
        let get = |env: &Vec<(String, String)>, key: &str| {
            env.iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| panic!("{key} missing"))
        };

        // par repair failed → 4, and the run as a whole is a FAILURE.
        let env = script_env(&j, dir, false, false, true, true);
        assert_eq!(get(&env, "NZBPP_PARSTATUS"), "4");
        assert_eq!(get(&env, "NZBPP_TOTALSTATUS"), "FAILURE");
        assert_eq!(get(&env, "NZBPP_STATUS"), "FAILURE/ALL");

        // repaired → 2; unpack failed → 1.
        let env = script_env(&j, dir, true, true, false, false);
        assert_eq!(get(&env, "NZBPP_PARSTATUS"), "2");
        assert_eq!(get(&env, "NZBPP_UNPACKSTATUS"), "1");
        assert_eq!(get(&env, "NZBPP_TOTALSTATUS"), "FAILURE");

        // checked and clean → 1; nothing to unpack → 0; overall SUCCESS.
        let env = script_env(&j, dir, true, false, true, false);
        assert_eq!(get(&env, "NZBPP_PARSTATUS"), "1");
        assert_eq!(get(&env, "NZBPP_UNPACKSTATUS"), "0");
        assert_eq!(get(&env, "NZBPP_TOTALSTATUS"), "SUCCESS");

        // unpacked → 2.
        let env = script_env(&j, dir, true, false, true, true);
        assert_eq!(get(&env, "NZBPP_UNPACKSTATUS"), "2");

        assert_eq!(get(&env, "NZBPP_DIRECTORY"), dir.to_string_lossy());
        assert_eq!(get(&env, "NZBOP_DESTDIR"), "/downloads");
        assert_eq!(get(&env, "NZBPP_NZBNAME"), "Some.Release");
        assert_eq!(get(&env, "NZBPP_NZBFILENAME"), "Some.Release.nzb");
        assert_eq!(get(&env, "NZBPP_CATEGORY"), "tv");
        assert_eq!(get(&env, "NZBPP_NZBID"), "41");
        assert_eq!(get(&env, "NZBOP_VERSION"), env!("CARGO_PKG_VERSION"));
    }

    /// Cleanup removes the archive scaffolding and nothing else. A media
    /// file that happens to sit beside it is the whole point of the job.
    #[test]
    fn cleanup_removes_archive_scaffolding_and_keeps_the_payload() {
        let tmp = tempfile::tempdir().unwrap();
        let junk = [
            "set.par2",
            "set.vol00+01.PAR2",
            "set.rar",
            "set.part2.rar",
            "set.zip",
            "set.7z",
            "set.sfv",
            "set.001",
        ];
        // `set.r00` is deliberately in the keep list: the numeric rule
        // matches a three-character all-digit extension, so old-style RAR
        // volumes survive cleanup today. Recorded here as the behavior it
        // is — changing it is a cleanup decision, not a test fix.
        let keep = [
            "film.mkv",
            "readme.nfo",
            "cover.jpg",
            "notes.1234",
            "set.r00",
        ];
        for n in junk.iter().chain(keep.iter()) {
            std::fs::write(tmp.path().join(n), b"x").unwrap();
        }
        cleanup_dir(tmp.path());
        for n in junk {
            assert!(!tmp.path().join(n).exists(), "{n} should have been cleaned");
        }
        for n in keep {
            assert!(tmp.path().join(n).exists(), "{n} must survive cleanup");
        }
        // A directory that cannot be read is not an error worth failing on.
        cleanup_dir(&tmp.path().join("absent"));
    }

    /// Staged output is published by rename, replacing whatever the earlier
    /// attempt left at the target.
    #[test]
    fn extraction_shared_parent_preserves_siblings_and_conflicts() {
        let tmp = tempfile::tempdir().unwrap();
        let staging = tmp.path().join("staging");
        let dir = tmp.path().join("job");
        std::fs::create_dir_all(staging.join("Subs")).unwrap();
        std::fs::write(staging.join("Subs/en.srt"), b"new subs").unwrap();
        std::fs::write(staging.join("film.mkv"), b"new film").unwrap();
        // Stale output from an earlier attempt, both shapes.
        std::fs::create_dir_all(dir.join("Subs")).unwrap();
        std::fs::write(dir.join("Subs/old.srt"), b"stale").unwrap();
        std::fs::write(dir.join("film.mkv"), b"new film").unwrap();

        commit_staging(&staging, &dir).unwrap();

        assert_eq!(std::fs::read(dir.join("film.mkv")).unwrap(), b"new film");
        assert_eq!(std::fs::read(dir.join("Subs/en.srt")).unwrap(), b"new subs");
        assert!(
            dir.join("Subs/old.srt").exists(),
            "publication must preserve pre-existing siblings"
        );
        std::fs::write(staging.join("film.mkv"), b"conflicting film").unwrap();
        assert!(commit_staging(&staging, &dir).is_err());
        assert_eq!(std::fs::read(dir.join("film.mkv")).unwrap(), b"new film");
        assert!(commit_staging(&tmp.path().join("absent"), &dir).is_err());
    }

    /// The block size prices a hash-named recovery volume. Anything that is
    /// not a readable par2 index has no price, and must say so rather than
    /// guess one.
    #[tokio::test]
    async fn par2_block_size_declines_anything_that_is_not_a_par2_index() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(par2_block_size(&tmp.path().join("absent.par2")).await, None);
        let garbage = tmp.path().join("garbage.par2");
        std::fs::write(&garbage, vec![0u8; 4096]).unwrap();
        assert_eq!(par2_block_size(&garbage).await, None);
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn delayed_duplicate_and_unusable_batches_do_not_repeat_matching_or_verification() {
        use nzbd_types::{FileEntry, FileId, Segment, SegmentState};
        use std::os::unix::fs::PermissionsExt;
        if std::process::Command::new("par2")
            .arg("-V")
            .output()
            .is_err()
        {
            assert!(std::env::var_os("NZBD_REQUIRE_TOOLS").is_none());
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("processing");
        std::fs::create_dir(&root).unwrap();
        let engine = nzbd_engine::Engine::spawn(nzbd_engine::EngineConfig::single_node(
            vec![],
            temp.path().join("state"),
            root.clone(),
            Default::default(),
            None,
        ))
        .await
        .unwrap();
        let dir = root.join("job");
        engine.artifacts().allocate(401, &root, &dir).unwrap();
        let bytes: Vec<_> = (0..50000).map(|n| ((n * 7) % 251) as u8).collect();
        std::fs::write(dir.join("payload.bin"), &bytes).unwrap();
        let status = std::process::Command::new("par2")
            .current_dir(&dir)
            .args([
                "create",
                "-q",
                "-s8192",
                "-c3",
                "-n3",
                "catalog.par2",
                "payload.bin",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(status.success());
        let mut volumes: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.file_name().unwrap().to_string_lossy().contains(".vol"))
            .collect();
        volumes.sort();
        assert!(volumes.len() >= 2);
        let duplicate = std::fs::read(&volumes[0]).unwrap();
        let unique = std::fs::read(&volumes[1]).unwrap();
        for path in volumes.iter().skip(1) {
            std::fs::remove_file(path).unwrap();
        }
        let mut invalid = unique.clone();
        *invalid.last_mut().unwrap() ^= 1;
        let delayed = [
            ("delayed0.vol100+001.par2", duplicate),
            ("delayed1.vol200+001.par2", invalid),
            ("delayed2.vol300+001.par2", unique),
        ];
        let file = |id: u32, name: String, par: bool, paused: bool, size: u32| FileEntry {
            id: FileId(id),
            subject: name.clone(),
            filename: name,
            filename_confirmed: true,
            is_par2: par,
            paused,
            groups: vec![],
            date: None,
            segments: if paused {
                vec![Segment {
                    message_id: format!("{id}@fixture").into_boxed_str(),
                    number: 1,
                    size,
                    state: SegmentState::Pending,
                }]
            } else {
                vec![]
            },
            crc32: None,
            finalized: !paused,
        };
        let mut files = vec![file(1, "payload.bin".into(), false, false, 50000)];
        files.extend(
            std::fs::read_dir(&dir)
                .unwrap()
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|e| e == "par2"))
                .enumerate()
                .map(|(i, p)| {
                    file(
                        10 + i as u32,
                        p.file_name().unwrap().to_string_lossy().into_owned(),
                        true,
                        false,
                        0,
                    )
                }),
        );
        files.extend(delayed.iter().enumerate().map(|(i, (name, data))| {
            file(20 + i as u32, (*name).into(), true, true, data.len() as u32)
        }));
        let job = Job {
            id: JobId(401),
            kind: nzbd_types::JobKind::Nzb,
            name: "job".into(),
            dir_name: "job".into(),
            name_provisional: false,
            delivery_lifecycle: false,
            queued_at_unix: 0,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: Default::default(),
            params: vec![],
            files,
            totals: Default::default(),
            status: JobStatus::Completed,
            torrent: None,
            stages: vec![],
        };
        let mut damaged = bytes;
        damaged[..8192].fill(0);
        std::fs::write(dir.join("payload.bin"), damaged).unwrap();
        engine.import_fixture_job(job, false, false).await.unwrap();
        let count = temp.path().join("verifications");
        let fake = temp.path().join("par-tool.sh");
        std::fs::write(&fake,format!("#!/bin/sh\necho x >> '{}'\necho 'Repair is not possible.'\necho 'You need 1 more recovery blocks to be able to repair.'\nexit 2\n",count.display())).unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let admission = engine
            .begin_post_admission(JobId(401))
            .await
            .unwrap()
            .unwrap();
        let scoped = engine.for_post_admission(&admission);
        let feeder_engine = scoped.clone();
        let feeder_dir = dir.clone();
        let feeder = tokio::spawn(async move {
            for (name, data) in delayed {
                loop {
                    let mut job = feeder_engine.export_job(JobId(401)).await.unwrap().unwrap();
                    let entry = job
                        .files
                        .iter_mut()
                        .find(|file| file.filename == name)
                        .unwrap();
                    if !entry.paused {
                        std::fs::write(feeder_dir.join(name), &data).unwrap();
                        entry.finalized = true;
                        entry.segments[0].state = SegmentState::Done {
                            offset: 0,
                            len: data.len() as u32,
                            crc: crc32fast::hash(&data),
                        };
                        feeder_engine
                            .import_fixture_job(job, false, false)
                            .await
                            .unwrap();
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }
        });
        let observations = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = observations.clone();
        let ctx = PpCtx {
            repair_observer: Some(Arc::new(move |bytes, checks, fetches| {
                recorded.lock().unwrap().push((bytes, checks, fetches))
            })),
            ..Default::default()
        };
        let history = Arc::new(
            HistoryDb::open(&temp.path().join("history.sqlite"), Some(temp.path())).unwrap(),
        );
        let cfg = PostConfig {
            par2_cmd: fake.to_string_lossy().into_owned(),
            unpack: false,
            cleanup: false,
            deobfuscate_final: false,
            failure_action: FailureAction::None,
            par_fetch_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let result = tokio::time::timeout(
            Duration::from_secs(8),
            super::process_job_ctx(&scoped, &cfg, &history, &root, JobId(401), &ctx),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result, PpFinal::ParFailure);
        tokio::time::timeout(Duration::from_secs(3), feeder)
            .await
            .unwrap()
            .unwrap();
        engine.end_post_admission(admission).await.unwrap();
        assert_eq!(std::fs::read_to_string(count).unwrap().lines().count(), 2);
        {
            let observed = observations.lock().unwrap();
            assert!(
                observed.iter().all(|(bytes, _, _)| *bytes == 50000),
                "payload scanned once: {observed:?}"
            );
            assert_eq!(
                observed
                    .last()
                    .map(|(_, checks, fetches)| (*checks, *fetches)),
                Some((2, 3))
            );
        }
        engine.shutdown().await;
    }
    #[tokio::test]
    async fn deleted_failed_source_replays_terminal_history_without_diagnostics() {
        let temp = tempfile::tempdir().unwrap();
        let downloads = temp.path().join("downloads");
        std::fs::create_dir(&downloads).unwrap();
        let config = nzbd_engine::EngineConfig::single_node(
            Vec::new(),
            temp.path().join("queue"),
            downloads.clone(),
            nzbd_engine::Tuning::default(),
            None,
        );
        let engine = nzbd_engine::Engine::spawn(config).await.unwrap();
        let history = Arc::new(
            HistoryDb::open(&temp.path().join("history.sqlite"), Some(temp.path())).unwrap(),
        );
        let mut request = job(vec![file(1, "missing.mkv", None)], vec![]);
        request.status = JobStatus::Failed;
        request.delivery_lifecycle = true;
        let source = downloads.join(nzbd_engine::queue::job_dir_name(&request));
        let inventory = engine.artifacts();
        inventory
            .allocate_for_delivery(41, &downloads, &source, true, true)
            .unwrap();
        std::fs::write(source.join(".runner-file-1.part"), b"unrecoverable").unwrap();
        inventory.finish(41, &source, &downloads, "active").unwrap();
        engine
            .import_fixture_job(request.clone(), false, false)
            .await
            .unwrap();
        let cfg = PostConfig {
            failure_action: FailureAction::Delete,
            ..PostConfig::default()
        };
        assert_eq!(
            dispose_failed(
                inventory.clone(),
                &cfg,
                request.id,
                &source,
                &downloads,
                "job",
                "local"
            )
            .await
            .note,
            "deleted"
        );
        // Exact crash boundary: deletion committed, neither history nor PP_DONE.
        fail_and_follow_up(
            &engine,
            &cfg,
            &history,
            &downloads,
            &None,
            request.id,
            request.name.clone(),
            0,
            false,
        )
        .await;
        let row = engine.export_job(request.id).await.unwrap().unwrap();
        assert!(pp_done(&row));
        let entry = history.get(request.id).unwrap().unwrap();
        assert!(entry.final_dir.is_none());
        assert_eq!(entry.status, "FAILURE/HEALTH");
        // Contract §2.6: disposal is recorded on the terminal row, after the
        // history row; a source already deleted has nothing left to dispose.
        let terminal = engine.artifacts().terminal(request.id.0).unwrap().unwrap();
        assert!(terminal.disposal.is_some());
        assert!(!source.exists());
        engine.shutdown().await;
    }

    #[test]
    fn ordinary_proof_titles_remain_payloads() {
        for name in ["Burden.of.Proof.mkv", "Living_Proof.flac", "Proof.epub"] {
            let path = Path::new(name);
            assert!(!is_sample_or_proof(
                path,
                path.file_stem().unwrap().to_str().unwrap()
            ));
            assert!(!is_disposable_name(&name.to_ascii_lowercase()));
        }
        assert!(is_sample_or_proof(
            Path::new("release.[proof].mkv"),
            "release.[proof]"
        ));
    }

    #[test]
    fn resolved_publication_does_not_reopen_consumed_archives_for_diagnostics() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("downloads");
        std::fs::create_dir(&root).unwrap();
        let inventory = nzbd_state::artifacts::Inventory::open(&temp.path().join("state")).unwrap();
        let mut source = inventory
            .allocate_for_delivery(41, &root, &root.join("job"), true, true)
            .unwrap();
        // This helper runs after the positive proof pass. No payload files
        // exist here: resolved inputs must not be opened again for diagnosis.
        source.files = [
            "movie.mkv",
            "movie.jpg",
            "release.part1.rar",
            "release.par2",
        ]
        .into_iter()
        .map(payload_entry)
        .collect();
        let request = job(Vec::new(), Vec::new());
        let verified = ["movie.mkv".into(), "movie.jpg".into()]
            .into_iter()
            .collect();
        let consumed = ["release.part1.rar".into()].into_iter().collect();
        assert!(publication_diagnostics(
            &request,
            &source,
            &consumed,
            &verified,
            &Default::default(),
            &Default::default(),
        )
        .unwrap()
        .is_empty());
        for sibling in ["unknown.bin", "other.mkv", "movie.en.srt", "other.rar"] {
            source.files.push(payload_entry(sibling));
            let result = publication_diagnostics(
                &request,
                &source,
                &consumed,
                &verified,
                &Default::default(),
                &Default::default(),
            );
            assert!(
                result.is_err() || result.is_ok_and(|diagnostics| !diagnostics.is_empty()),
                "{sibling} still needs diagnostics"
            );
            source.files.pop();
        }
        // A CRC-proven archive that has not been extracted still needs
        // recovery assessment; download integrity alone is not consumption.
        let mut archive_verified = verified.clone();
        archive_verified.insert("release.part1.rar".into());
        let result = publication_diagnostics(
            &request,
            &source,
            &Default::default(),
            &archive_verified,
            &Default::default(),
            &Default::default(),
        );
        assert!(result.is_err() || result.is_ok_and(|diagnostics| !diagnostics.is_empty()));
    }

    #[test]
    fn final_crc_checks_skip_only_proven_consumed_inputs_and_check_direct_media() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("downloads");
        std::fs::create_dir(&root).unwrap();
        let inventory = nzbd_state::artifacts::Inventory::open(&temp.path().join("state")).unwrap();
        let path = root.join("job");
        inventory
            .allocate_for_delivery(41, &root, &path, true, true)
            .unwrap();
        let archive = vec![7; 1024 * 1024];
        let media = b"direct media";
        std::fs::write(path.join("archive.rar"), &archive).unwrap();
        std::fs::write(path.join("movie.mkv"), media).unwrap();
        let source = inventory.finish(41, &path, &root, "active").unwrap();
        let mut files = Vec::new();
        for (id, name, bytes) in [
            (1, "archive.bin", archive.as_slice()),
            (2, "movie.mkv", media.as_slice()),
        ] {
            let mut entry = file(id, name, Some(crc32fast::hash(bytes)));
            entry.segments = vec![Segment {
                message_id: "part".into(),
                number: 1,
                size: bytes.len() as u32,
                state: SegmentState::Done {
                    offset: 0,
                    len: bytes.len() as u32,
                    crc: crc32fast::hash(bytes),
                },
            }];
            files.push(entry);
        }
        let request = job(files, Vec::new());
        let restored = [("archive.bin".into(), "archive.rar".into())]
            .into_iter()
            .collect();
        let consumed = ["archive.rar".into()].into_iter().collect();
        let mut reads = 0;
        let checked = verified_direct_payload_paths_excluding(
            &request,
            &source,
            &restored,
            &consumed,
            || {
                reads += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(checked, ["movie.mkv".into()].into_iter().collect());
        assert_eq!(
            reads, 2,
            "only the direct media read and EOF should be visited"
        );
        let mut all_reads = 0;
        let checked = verified_direct_payload_paths(&request, &source, &restored, || {
            all_reads += 1;
            Ok(())
        })
        .unwrap();
        assert!(checked.contains("archive.rar"));
        assert!(
            all_reads > reads,
            "unconsumed archives retain their CRC check"
        );
    }

    #[test]
    fn repair_markers_match_exact_file_ids() {
        let temp = tempfile::tempdir().unwrap();
        let inventory = nzbd_state::artifacts::Inventory::open(&temp.path().join("state")).unwrap();
        let root = temp.path().join("downloads");
        std::fs::create_dir(&root).unwrap();
        let source = root.join("job");
        inventory
            .allocate_for_delivery(41, &root, &source, true, true)
            .unwrap();
        let bytes = b"verified file";
        std::fs::write(source.join("episode.mkv"), bytes).unwrap();
        let source = inventory.finish(41, &source, &root, "active").unwrap();
        let mut media = file(1, "episode.mkv", Some(crc32fast::hash(bytes)));
        media.segments = vec![Segment {
            message_id: "one".into(),
            number: 1,
            size: bytes.len() as u32,
            state: SegmentState::Done {
                offset: 0,
                len: bytes.len() as u32,
                crc: crc32fast::hash(bytes),
            },
        }];
        let mut request = job(vec![media], vec![("*File:repair:11".into(), "1".into())]);
        assert!(
            verified_direct_payload_paths(&request, &source, &Default::default(), || Ok(()))
                .unwrap()
                .contains("episode.mkv")
        );
        request.params.push(("*File:repair:1".into(), "1".into()));
        assert!(
            verified_direct_payload_paths(&request, &source, &Default::default(), || Ok(()))
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn native_book_containers_and_sniffed_media_publish_without_rename_or_extraction() {
        let temp = tempfile::tempdir().unwrap();
        let downloads = temp.path().join("downloads");
        std::fs::create_dir(&downloads).unwrap();
        let completed = temp.path().join("completed");
        let config = nzbd_engine::EngineConfig::single_node(
            Vec::new(),
            temp.path().join("queue"),
            downloads.clone(),
            nzbd_engine::Tuning::default(),
            None,
        );
        let engine = nzbd_engine::Engine::spawn(config).await.unwrap();
        let history = Arc::new(
            HistoryDb::open(&temp.path().join("history.sqlite"), Some(temp.path())).unwrap(),
        );
        let payloads: [(&str, &[u8]); 3] = [
            ("Proof.epub", b"PK\x03\x04native epub bytes"),
            ("comic.cbz", b"PK\x03\x04native cbz bytes"),
            ("comic.cbr", b"Rar!\x1a\x07\x00native cbr bytes"),
        ];
        let mut files: Vec<_> = payloads
            .iter()
            .enumerate()
            .map(|(index, (name, bytes))| {
                let mut media = file(index as u32 + 1, name, Some(crc32fast::hash(bytes)));
                media.segments = vec![Segment {
                    message_id: format!("book-{index}").into(),
                    number: 1,
                    size: bytes.len() as u32,
                    state: SegmentState::Done {
                        offset: 0,
                        len: bytes.len() as u32,
                        crc: crc32fast::hash(bytes),
                    },
                }];
                media
            })
            .collect();
        let opaque = b"\x1a\x45\xdf\xa3recognized complete container";
        let mut unknown = file(4, "opaque-media", Some(crc32fast::hash(opaque)));
        unknown.segments = vec![Segment {
            message_id: "opaque".into(),
            number: 1,
            size: opaque.len() as u32,
            state: SegmentState::Done {
                offset: 0,
                len: opaque.len() as u32,
                crc: crc32fast::hash(opaque),
            },
        }];
        files.push(unknown);
        let mut request = job(files, vec![("monarr-transfer".into(), "books".into())]);
        request.delivery_lifecycle = true;
        request.status = JobStatus::PostQueued;
        let source = downloads.join(nzbd_engine::queue::job_dir_name(&request));
        let inventory = engine.artifacts();
        inventory
            .allocate_for_delivery(41, &downloads, &source, true, true)
            .unwrap();

        for (name, bytes) in payloads {
            std::fs::write(source.join(name), bytes).unwrap();
        }
        std::fs::write(source.join("readme.txt"), b"junk").unwrap();
        std::fs::write(source.join("unmatched.srt"), b"unmatched subtitle").unwrap();
        std::fs::write(
            source.join("opaque-media"),
            b"\x1a\x45\xdf\xa3recognized complete container",
        )
        .unwrap();
        inventory.finish(41, &source, &downloads, "active").unwrap();
        engine
            .import_fixture_job(request, false, false)
            .await
            .unwrap();
        let cfg = PostConfig {
            completed_dir: Some(completed),
            deobfuscate_final: false,
            cleanup: false,
            unrar_cmd: "missing-extractor-must-not-run".into(),
            sevenzip_cmd: "missing-extractor-must-not-run".into(),
            ..PostConfig::default()
        };
        assert_eq!(
            process_job_ctx(
                &engine,
                &cfg,
                &history,
                &downloads,
                JobId(41),
                &PpCtx::default()
            )
            .await
            .unwrap(),
            PpFinal::Success
        );
        let delivery = inventory.delivery_for_job(41).unwrap().unwrap();
        // R3 (contract §2.2): a verified file with no recognised extension
        // is media by its bytes (Matroska EBML here) and publishes.
        assert_eq!(delivery.files.len(), 4);
        assert!(delivery
            .files
            .iter()
            .any(|entry| entry.path == "opaque-media"));
        for (name, bytes) in payloads {
            assert_eq!(
                std::fs::read(Path::new(&delivery.publication_path).join(name)).unwrap(),
                bytes
            );
            assert!(delivery.files.iter().any(|entry| entry.path == name));
        }
        let outcomes = inventory
            .delivery_file_outcomes(&delivery.artifact_id, &delivery.publication_generation)
            .unwrap();
        assert!(outcomes
            .iter()
            .any(|entry| entry.source_path == "opaque-media" && entry.outcome == "published"));
        inventory
            .retire_delivery_source(&delivery.delivery_id)
            .unwrap();
        assert!(!source.join("readme.txt").exists());
        assert!(!source.join("unmatched.srt").exists());
        engine.shutdown().await;
    }
    #[tokio::test]
    async fn completed_junk_only_attempt_is_a_parked_publish_failure() {
        let temp = tempfile::tempdir().unwrap();
        let downloads = temp.path().join("downloads");
        std::fs::create_dir(&downloads).unwrap();
        let config = nzbd_engine::EngineConfig::single_node(
            Vec::new(),
            temp.path().join("queue"),
            downloads.clone(),
            nzbd_engine::Tuning::default(),
            None,
        );
        let engine = nzbd_engine::Engine::spawn(config).await.unwrap();
        let history = Arc::new(
            HistoryDb::open(&temp.path().join("history.sqlite"), Some(temp.path())).unwrap(),
        );
        let mut request = job(vec![], vec![]);
        request.delivery_lifecycle = true;
        request.status = JobStatus::PostQueued;
        let source = downloads.join(nzbd_engine::queue::job_dir_name(&request));
        let inventory = engine.artifacts();
        inventory
            .allocate_for_delivery(41, &downloads, &source, true, true)
            .unwrap();

        std::fs::write(source.join("readme.txt"), b"junk").unwrap();
        std::fs::write(source.join("orphan.srt"), b"unmatched sidecar").unwrap();
        inventory.finish(41, &source, &downloads, "active").unwrap();
        engine
            .import_fixture_job(request, false, false)
            .await
            .unwrap();
        let cfg = PostConfig {
            deobfuscate_final: true,
            cleanup: false,
            ..PostConfig::default()
        };
        assert_eq!(
            process_job_ctx(
                &engine,
                &cfg,
                &history,
                &downloads,
                JobId(41),
                &PpCtx::default()
            )
            .await
            .unwrap(),
            PpFinal::UnpackFailure
        );
        // Contract §2.2: nothing publishable is `failed/publish`, recorded
        // before anything touches the bytes, and always parked.
        let terminal = inventory.terminal(41).unwrap().unwrap();
        assert_eq!(terminal.reason, Some(nzbd_types::FailureReason::Publish));
        assert!(!terminal.blames_release);
        assert!(terminal.emitted);
        assert!(source.exists(), "the terminal precedes disposal");
        let entry = history.get(JobId(41)).unwrap().unwrap();
        assert_eq!(entry.status, "FAILURE/PUBLISH");
        assert!(entry.final_dir.is_none());
        assert!(entry
            .params
            .iter()
            .any(|(key, value)| key == "Failure:Reason" && value == "publish"));
        assert!(entry.params.iter().any(
            |(key, value)| key == "Failure:Message" && value.contains("no publishable payload")
        ));
        // `failure_action` is delete; publish parks anyway.
        assert!(failure_followup(&engine, &cfg, &downloads, &None, JobId(41), true,).await);
        let disposal = inventory.terminal(41).unwrap().unwrap().disposal.unwrap();
        assert!(!source.exists(), "{disposal}");
        assert!(disposal.starts_with("parked at "), "{disposal}");
        engine.shutdown().await;
    }
    #[tokio::test]
    async fn held_rows_keep_the_owner_schedule_on_import_and_restart() {
        // M6: the owner's hold ladder is the only clock a held PP job runs
        // on; importing a row re-derives its policy without moving it.
        let temp = tempfile::tempdir().unwrap();
        let config = || {
            nzbd_engine::EngineConfig::single_node(
                Vec::new(),
                temp.path().join("queue"),
                temp.path().join("downloads"),
                nzbd_engine::Tuning::default(),
                None,
            )
        };
        let engine = nzbd_engine::Engine::spawn(config()).await.unwrap();
        let first_block = now() - 1801;
        let deadline = now() + 900;
        for (index, cause) in ["storage_unavailable", "missing_tool", "capacity", "quota"]
            .iter()
            .enumerate()
        {
            let mut request = job(vec![], vec![]);
            request.id = JobId(41 + index as u32);
            request.delivery_lifecycle = true;
            request.status = JobStatus::Paused;
            request.set_control(&nzbd_types::JobControl {
                version: 1,
                revision: "1".into(),
                lifecycle: "held".into(),
                cause: (*cause).into(),
                stage: "move".into(),
                retry_policy: "review".into(),
                message: "temporarily blocked".into(),
                instance: "retry-test".into(),
                previous_status: Some(JobStatus::PostQueued),
                manual_pause: false,
                retry_attempt: 0,
                next_retry_at_unix: Some(deadline),
                blocked_since_unix: Some(first_block),
                released_at_unix: None,
                deadline_in_s: None,
                deadline_unix: None,
            });
            engine
                .import_fixture_job(request.clone(), false, false)
                .await
                .unwrap();
            let scheduled = engine
                .export_job(request.id)
                .await
                .unwrap()
                .unwrap()
                .control()
                .unwrap();
            assert_eq!(scheduled.cause, *cause);
            assert_eq!(scheduled.revision, "2");
            assert_eq!(
                scheduled.next_retry_at_unix,
                Some(deadline),
                "an existing deadline cannot be shortened"
            );
            assert_eq!(scheduled.blocked_since_unix, Some(first_block));
            assert_eq!(
                scheduled.retry_policy,
                HoldCause::parse(cause).policy().as_str()
            );
            assert_eq!(scheduled.retry_attempt, 0);
            assert_eq!(scheduled.message, "temporarily blocked");
        }
        let expected = engine
            .export_job(JobId(44))
            .await
            .unwrap()
            .unwrap()
            .control()
            .unwrap();
        let mut paused = job(vec![], vec![]);
        paused.id = JobId(45);
        paused.delivery_lifecycle = true;
        paused.status = JobStatus::Paused;
        let mut manual = expected.clone();
        manual.manual_pause = true;
        paused.set_control(&manual);
        engine
            .import_fixture_job(paused, false, false)
            .await
            .unwrap();
        assert_eq!(
            engine
                .export_job(JobId(45))
                .await
                .unwrap()
                .unwrap()
                .control()
                .unwrap(),
            manual
        );
        engine.shutdown().await;
        let reopened = nzbd_engine::Engine::spawn(config()).await.unwrap();
        let control = reopened
            .export_job(JobId(44))
            .await
            .unwrap()
            .unwrap()
            .control()
            .unwrap();
        assert_eq!(control.next_retry_at_unix, expected.next_retry_at_unix);
        assert_eq!(control.blocked_since_unix, expected.blocked_since_unix);
        assert_eq!(control.retry_attempt, expected.retry_attempt);
        reopened.shutdown().await;
    }
}
