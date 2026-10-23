//! The post-processing orchestrator + manager (ARCHITECTURE.md §9).
//!
//! The manager watches the engine for finished downloads and drives each
//! job through the stage graph: par verify (native quick path first) →
//! repair (with delayed-par fetching) → unpack → cleanup → scripts —
//! recording the outcome in history and stamping the job so restarts never
//! re-process. Stage parallelism follows `PostStrategy`.

use crate::rename::{par_rename_owned, rar_rename_owned};
use crate::script::{discover, ScriptHost};
use crate::tools::{archive_input_set, detect_archives, Extractors, Par2Tool};
use crate::{par2, DownloadEvidence, PostError, RepairResult, VerifyResult};
use nzbd_engine::{EngineHandle, Event};
use nzbd_state::artifacts::{Artifact, DeliveryFileOutcome, FileEntry};
use nzbd_state::history::HistoryDb;
use nzbd_state::HistoryEntry;
use nzbd_types::metrics::PpStageStats;
use nzbd_types::{Health, Job, JobId, JobStatus, PostStage};
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
/// (docs/REGRAB_LOOP_PLAN.md D2).
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
const POST_RETRY_DELAYS_SECONDS: [i64; 4] = [30, 60, 120, 300];
const PP_FAILURE_AT_PARAM: &str = "*PP:failure-at";
const PP_SELECTED_OUTCOME_PARAM: &str = "*PP:selected-publication-outcome";
const PP_SELECTED_FAILURE_CAUSE_PARAM: &str = "*PP:selected-publication-failure-cause";
const PP_FAILURE_DISPOSITION_ATTEMPTS_PARAM: &str = "*PP:failure-disposition-attempts";
const FAILURE_DISPOSITION_ATTEMPT_LIMIT: u32 = 3;
const PP_FAILED_CLEANUP_NEXT_RETRY_PARAM: &str = "*PP:failed-cleanup-next-retry";
const PP_FAILED_CLEANUP_BLOCKED_SINCE_PARAM: &str = "*PP:failed-cleanup-blocked-since";
const PP_FAILED_CLEANUP_LAST_ERROR_PARAM: &str = "*PP:failed-cleanup-last-error";

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
            PostStage::Script => RestartPoint::Scripts,
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

    // When each job last spent a failed-file disposition attempt, so that one
    // occasion spends one attempt — see `handle_failed_job`.
    let mut last_disposition: HashMap<JobId, tokio::time::Instant> = HashMap::new();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = rescan.tick() => {
                // Anything older than a full period can no longer suppress.
                last_disposition.retain(|_, at| at.elapsed() < RESCAN_INTERVAL);
                scan_queue(
                    &tracker, &engine, &cfg, &history, &dest_dir, &gate, &sem,
                    &cancel, &finished_tx, &mut active, &mut last_disposition,
                ).await;
            }
            ev = rx.recv() => match ev {
                Ok(Event::JobFinished { job, status, name, health }) => match status {
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
                    JobStatus::Failed if claim(&gate, job) => {
                        handle_failed_job(
                            &engine, &cfg, &history, &dest_dir, &gate, job, name, health,
                            &mut last_disposition,
                        )
                        .await;
                    }
                    _ => {}
                },
                Ok(Event::JobDeleted { job }) => {
                    pending_restarts.remove(&job);
                    if let Some(attempt) = active.get(&job) {
                        attempt.cancel.cancel();
                    }
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
                        Ok(Some(mut row)) if row.status == JobStatus::Failed => {
                            let has_cleanup_retry = row.params.iter().any(|(key, _)| {
                                key == PP_FAILED_CLEANUP_NEXT_RETRY_PARAM
                                    || key == PP_FAILED_CLEANUP_LAST_ERROR_PARAM
                            });
                            if !has_cleanup_retry {
                                Err(RestartPostError::NotReady)
                            } else {
                                set_param(&mut row.params, PP_FAILED_CLEANUP_NEXT_RETRY_PARAM, "0".into());
                                match engine.import_job_if_present(row.clone()).await {
                                    Ok(true) => {
                                        handle_failed_job(
                                            &engine,
                                            &cfg,
                                            &history,
                                            &dest_dir,
                                            &gate,
                                            job,
                                            row.name,
                                            Health::calc(&row.totals).0,
                                            &mut last_disposition,
                                        ).await;
                                        Ok(())
                                    }
                                    Ok(false) => Err(RestartPostError::NotFound),
                                    Err(_) => Err(RestartPostError::Closed),
                                }
                            }
                        }
                        Ok(Some(row)) => {
                            let control = row.control().filter(|control| {
                                control.lifecycle == "held"
                                    && !control.manual_pause
                                    && matches!(
                                        control.retry_policy.as_str(),
                                        "scheduled_retry" | "resume_same_job" | "reobserve_mount"
                                    )
                            });
                            if active.contains_key(&job) {
                                Err(RestartPostError::UnsafePoint)
                            } else if let Some(control) = control {
                                if !claim(&gate, job) {
                                    Err(RestartPostError::NotOwned)
                                } else if !engine.retry_post_hold(job, control.revision).await.unwrap_or(false) {
                                    Err(RestartPostError::NotReady)
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

#[allow(clippy::too_many_arguments)]
async fn handle_failed_job(
    engine: &EngineHandle,
    cfg: &PostConfig,
    history: &Arc<HistoryDb>,
    dest_dir: &Path,
    gate: &PpGate,
    job: JobId,
    name: String,
    health: u16,
    last_disposition: &mut HashMap<JobId, tokio::time::Instant>,
) {
    // Below critical health: no PP; record history and stamp so the retire
    // sweep moves the job out. Cluster disk admission gates this whole
    // operation because FailureAction::Park can copy across filesystems.
    let mut exported = engine.export_job(job).await.ok().flatten();
    if exported.as_ref().is_some_and(pp_done) {
        return; // already handled (event/rescan overlap)
    }
    if exported
        .as_ref()
        .is_some_and(|row| row.lifecycle_eligible())
        && exported.as_ref().is_some_and(|row| {
            param_i64(&row.params, PP_FAILED_CLEANUP_NEXT_RETRY_PARAM).is_some_and(|at| at > now())
        })
    {
        return;
    }
    // Authority and disk admission can change while export awaits the owner.
    // Recheck at the last boundary before Park may copy across filesystems.
    if gate.as_ref().is_some_and(|claim| !claim(job)) {
        return;
    }
    // Persist the logical finalization key on the queue row before any
    // external side effect. History append failure or fence loss can then
    // retry seconds later without manufacturing a second completion row.
    let completed_at_unix = exported
        .as_ref()
        .and_then(|job| {
            job.params
                .iter()
                .find(|(key, _)| key == PP_FAILURE_AT_PARAM)
                .and_then(|(_, value)| value.parse::<i64>().ok())
        })
        .unwrap_or_else(now);
    if let Some(finalizing) = exported.as_mut() {
        let needs_failure_at_commit = !finalizing
            .params
            .iter()
            .any(|(key, _)| key == PP_FAILURE_AT_PARAM);
        if needs_failure_at_commit {
            finalizing
                .params
                .push((PP_FAILURE_AT_PARAM.into(), completed_at_unix.to_string()));
            // This result means the atomic replacement reached the queue
            // snapshot, not merely that the in-memory row existed: no
            // Park/Delete, history append, or terminal stamp may precede this
            // durable commit. A key exported on a later retry is already
            // durable, so do not rewrite the whole queue snapshot again.
            match engine.import_job_if_present(finalizing.clone()).await {
                Ok(true) => {}
                Ok(false) | Err(_) => return,
            }
        }
    }
    if gate.as_ref().is_some_and(|claim| !claim(job)) {
        return;
    }
    let fetch_failure = exported
        .as_ref()
        .map(|j| matches!(j.kind, nzbd_types::JobKind::Url) && j.files.is_empty())
        .unwrap_or(false);
    let mut durable_delivery = if exported.as_ref().is_some_and(|job| job.delivery_lifecycle) {
        let inventory = engine.artifacts();
        match tokio::task::spawn_blocking(move || inventory.delivery_for_job(job.0)).await {
            Ok(Ok(delivery)) => delivery,
            Ok(Err(error)) => {
                tracing::warn!(job = job.0, %error, "failed-job delivery replay lookup failed");
                // A failed lookup cannot prove that no durable ready handoff
                // exists. Leave the source untouched for replay/reconciliation.
                return;
            }
            Err(error) => {
                tracing::warn!(job = job.0, %error, "failed-job delivery replay worker stopped");
                return;
            }
        }
    } else {
        None
    };
    if durable_delivery.is_none() {
        if let Some(row) = exported.as_ref().filter(|row| {
            row.delivery_lifecycle
                && row.params.iter().any(|(key, value)| {
                    key == PP_SELECTED_OUTCOME_PARAM && value == PpFinal::HealthFailure.as_str()
                })
        }) {
            if gate.as_ref().is_some_and(|claim| !claim(job)) {
                return;
            }
            let inventory = engine.artifacts();
            let row = row.clone();
            let gate_for_replay = gate.clone();
            let result = tokio::task::spawn_blocking(move || {
                if gate_for_replay.as_ref().is_some_and(|claim| !claim(row.id)) {
                    return Err(PostError::Io(std::io::ErrorKind::Interrupted.into()));
                }
                finish_committed_publication_receipt(&inventory, &row)
            })
            .await;
            match result {
                Ok(Ok(delivery)) => durable_delivery = delivery,
                Ok(Err(error)) => {
                    record_failed_cleanup_retry(engine, job, error.to_string()).await;
                    return;
                }
                Err(error) => {
                    record_failed_cleanup_retry(engine, job, error.to_string()).await;
                    return;
                }
            }
        }
    }
    let mut early_archive_assessment = EarlyArchiveAssessment::default();
    let salvage = match (
        durable_delivery,
        exported
            .as_ref()
            .filter(|j| j.lifecycle_eligible() && !fetch_failure),
    ) {
        (Some(delivery), _) => Some(FailedSalvage {
            published_path: PathBuf::from(&delivery.publication_path),
            artifact_id: delivery.artifact_id.clone(),
            generation: delivery.publication_generation.clone(),
            source_artifact_id: delivery.source_artifact_id.clone(),
            source_generation: delivery.source_generation.clone(),
            delivery: Some(delivery),
        }),
        (None, Some(job)) => {
            let mut job = job.clone();
            let salvage_job_id = job.id.0;
            let par_failures = match repair_failed_health_par_sets(engine, cfg, &job, gate).await {
                Ok(failures) => failures,
                Err(error) => {
                    record_failed_cleanup_retry(engine, JobId(salvage_job_id), error).await;
                    return;
                }
            };
            let mut archive_assessment =
                match extract_complete_failed_archives(engine, cfg, &job, gate).await {
                    Ok(assessment) => assessment,
                    Err(error) => {
                        record_failed_cleanup_retry(engine, JobId(salvage_job_id), error).await;
                        return;
                    }
                };
            archive_assessment.par_failures = par_failures;
            early_archive_assessment = archive_assessment.clone();
            let failure_cause = format!(
                "download health {health} did not meet the configured completion threshold"
            );
            let Some(mut marker) = engine.export_job(job.id).await.ok().flatten() else {
                record_failed_cleanup_retry(
                    engine,
                    JobId(salvage_job_id),
                    "failed-health publication outcome could not be read".into(),
                )
                .await;
                return;
            };
            set_param(
                &mut marker.params,
                PP_SELECTED_OUTCOME_PARAM,
                PpFinal::HealthFailure.as_str().into(),
            );
            set_param(
                &mut marker.params,
                PP_SELECTED_FAILURE_CAUSE_PARAM,
                failure_cause.clone(),
            );
            match engine.import_job_if_present(marker.clone()).await {
                Ok(true) => job = marker,
                Ok(false) | Err(_) => {
                    record_failed_cleanup_retry(
                        engine,
                        JobId(salvage_job_id),
                        "failed-health publication outcome could not be committed".into(),
                    )
                    .await;
                    return;
                }
            }
            exported = Some(job.clone());
            let inventory = engine.artifacts();
            let completed_root = cfg
                .completed_dir
                .clone()
                .unwrap_or_else(|| dest_dir.to_path_buf());
            let result = tokio::task::spawn_blocking(move || {
                salvage_failed_delivery(
                    &inventory,
                    &job,
                    &completed_root,
                    "FAILURE/HEALTH",
                    "health",
                    &failure_cause,
                    &archive_assessment,
                )
            })
            .await;
            match result {
                Ok(Ok(delivery)) => delivery,
                Ok(Err(error)) => {
                    tracing::warn!(job = salvage_job_id, %error, "verified failed-download salvage unavailable");
                    // A blocked read, publication, or durable receipt write is
                    // not evidence that the source is disposable.
                    if exported
                        .as_ref()
                        .is_some_and(|row| row.lifecycle_eligible())
                    {
                        record_failed_cleanup_retry(
                            engine,
                            JobId(salvage_job_id),
                            error.to_string(),
                        )
                        .await;
                    }
                    return;
                }
                Err(error) => {
                    tracing::warn!(job = salvage_job_id, %error, "verified failed-download salvage worker stopped");
                    if exported
                        .as_ref()
                        .is_some_and(|row| row.lifecycle_eligible())
                    {
                        record_failed_cleanup_retry(
                            engine,
                            JobId(salvage_job_id),
                            error.to_string(),
                        )
                        .await;
                    }
                    return;
                }
            }
        }
        (None, None) => None,
    };
    let fail_status = if fetch_failure {
        "FAILURE/FETCH"
    } else {
        "FAILURE/HEALTH"
    };
    let keep_on_failure = exported.as_ref().is_some_and(job_keeps_failed_source);
    let failed_source_assessment = if salvage.is_none() && !fetch_failure {
        match exported.as_ref().filter(|row| row.lifecycle_eligible()) {
            Some(row) => {
                let row = row.clone();
                let inventory = engine.artifacts();
                let archive_assessment = early_archive_assessment.clone();
                match tokio::task::spawn_blocking(move || {
                    failed_source_recovery(&inventory, &row, &archive_assessment)
                })
                .await
                {
                    Ok(Ok(assessment)) => Some(assessment),
                    Ok(Err(error)) => {
                        record_failed_cleanup_retry(engine, job, error).await;
                        return;
                    }
                    Err(error) => {
                        record_failed_cleanup_retry(engine, job, error.to_string()).await;
                        return;
                    }
                }
            }
            None => None,
        }
    } else {
        None
    };
    let recoverable_failed_source =
        failed_source_assessment
            .as_ref()
            .and_then(|(path, assessment)| match assessment.disposition.as_str() {
                "recoverable" | "unknown" => Some((path.clone(), assessment.reason.clone())),
                _ => None,
            });
    let mut failure_cfg = cfg.clone();
    if keep_on_failure {
        failure_cfg.failure_action = FailureAction::None;
    } else if exported
        .as_ref()
        .is_some_and(|job| job.lifecycle_eligible())
    {
        // New Runner attempts use reason-based per-download custody; legacy
        // global None/Park settings must not retain terminally unusable work.
        failure_cfg.failure_action = FailureAction::Delete;
    }
    if let Some(delivery) = salvage
        .as_ref()
        .and_then(|salvage| salvage.delivery.as_ref())
    {
        let delivery_id = delivery.delivery_id.clone();
        let source_id = delivery.source_artifact_id.clone();
        let source_generation = delivery.source_generation.clone();
        let publication_id = delivery.artifact_id.clone();
        let inventory = engine.artifacts();
        let _ = tokio::task::spawn_blocking(move || {
            if source_id != publication_id {
                let source = inventory.get(&source_id)?;
                if source.generation != source_generation {
                    return Err(nzbd_state::artifacts::Error::Conflict(
                        "retained Runner source generation changed".into(),
                    ));
                }
            }
            inventory.retire_delivery_source(&delivery_id)?;
            Ok::<_, nzbd_state::artifacts::Error>(())
        })
        .await;
    }
    let disposition = if salvage.is_some() || recoverable_failed_source.is_some() {
        None
    } else {
        match exported.as_ref() {
            Some(j) if !j.files.is_empty() => {
                let dir_name = nzbd_engine::queue::job_dir_name(j);
                let source = dest_dir.join(&dir_name);
                let observed = dispose_failed(
                    engine.artifacts(),
                    &failure_cfg,
                    job,
                    &source,
                    dest_dir,
                    &dir_name,
                    "local",
                )
                .await;
                match canonical_failed_disposition(&failure_cfg, dest_dir, &dir_name, &source) {
                    Some(committed) => Some(committed),
                    None => {
                        // A stale authority must never spend the retry budget. In
                        // particular, Park can finish its external move just as
                        // the lease or disk-admission gate closes.
                        if gate.as_ref().is_some_and(|claim| !claim(job)) {
                            return;
                        }
                        // ENOSPC/EDQUOT is not a permanently broken disposition:
                        // it is admission evidence. Hold intake and keep retrying
                        // after recovery, regardless of earlier local failures.
                        if nzbd_engine::is_out_of_space(&observed.note) {
                            engine.report_out_of_space(format!(
                                "failed-file disposition for job {}: {}",
                                job.0, observed.note
                            ));
                            tracing::warn!(job = job.0, outcome = %observed.note,
                            "failed-file disposition is waiting for disk admission");
                            return;
                        }

                        // One attempt per job per rescan period. The queue scan and
                        // a `JobFinished { Failed }` event routinely observe the
                        // same still-retryable row — it carries no `*PP:done`
                        // stamp for the overlap guard above to skip on — and
                        // counting both spends two of the three attempts in one
                        // instant, against the documented cadence (ARCHITECTURE.md
                        // §9: "immediately, then on the next two 30-second
                        // rescans", ~60s).
                        //
                        // Debounced on elapsed time rather than keyed to the tick:
                        // the event arrives *before* the startup scan's tick, so
                        // per-tick state would still let that scan spend a second
                        // attempt, and a rescan firing a hair early must still
                        // count as the next period. Half an interval separates the
                        // two cases by three orders of magnitude — the overlap is
                        // milliseconds, a real retry is 30 seconds.
                        //
                        // Only this counter is debounced, and only here: a
                        // disposition that resolves, or a finalization retrying
                        // after a failed commit, is a different path and stays
                        // immediate.
                        if last_disposition
                            .get(&job)
                            .is_some_and(|at| at.elapsed() < RESCAN_INTERVAL / 2)
                        {
                            return;
                        }

                        let Some(finalizing) = exported.as_mut() else {
                            return;
                        };
                        let attempts = finalizing
                            .params
                            .iter()
                            .find(|(key, _)| key == PP_FAILURE_DISPOSITION_ATTEMPTS_PARAM)
                            .and_then(|(_, value)| value.parse::<u32>().ok())
                            .unwrap_or(0)
                            .saturating_add(1);
                        match finalizing
                            .params
                            .iter_mut()
                            .find(|(key, _)| key == PP_FAILURE_DISPOSITION_ATTEMPTS_PARAM)
                        {
                            Some((_, value)) => *value = attempts.to_string(),
                            None => finalizing.params.push((
                                PP_FAILURE_DISPOSITION_ATTEMPTS_PARAM.into(),
                                attempts.to_string(),
                            )),
                        }
                        // The counter and the stable completion timestamp share
                        // the queue row's atomic snapshot. A daemon restart then
                        // resumes this budget instead of manufacturing a fresh
                        // set of attempts.
                        match engine.import_job_if_present(finalizing.clone()).await {
                            Ok(true) => {}
                            Ok(false) | Err(_) => return,
                        }
                        if finalizing.lifecycle_eligible() {
                            set_param(
                                &mut finalizing.params,
                                PP_FAILED_CLEANUP_NEXT_RETRY_PARAM,
                                now()
                                    .saturating_add(failed_cleanup_retry_delay(attempts))
                                    .to_string(),
                            );
                            if !finalizing
                                .params
                                .iter()
                                .any(|(key, _)| key == PP_FAILED_CLEANUP_BLOCKED_SINCE_PARAM)
                            {
                                finalizing.params.push((
                                    PP_FAILED_CLEANUP_BLOCKED_SINCE_PARAM.into(),
                                    now().to_string(),
                                ));
                            }
                            set_param(
                                &mut finalizing.params,
                                PP_FAILED_CLEANUP_LAST_ERROR_PARAM,
                                observed.note.clone(),
                            );
                            if !matches!(
                                engine.import_job_if_present(finalizing.clone()).await,
                                Ok(true)
                            ) {
                                return;
                            }
                        }
                        // Recorded only now, because the debounce stands for a
                        // *spent* attempt. A commit that did not reach the queue
                        // snapshot restored the previous row and spent nothing;
                        // debouncing on the try instead would hold the row
                        // un-retryable for half a rescan period and quietly cost
                        // the operator a retry that never happened — the same
                        // "retrying after a failed commit stays immediate" rule
                        // the paragraph above states.
                        last_disposition.insert(job, tokio::time::Instant::now());
                        if gate.as_ref().is_some_and(|claim| !claim(job)) {
                            return;
                        }
                        if !finalizing.lifecycle_eligible()
                            && attempts < FAILURE_DISPOSITION_ATTEMPT_LIMIT
                        {
                            tracing::warn!(
                                job = job.0,
                                attempts,
                                limit = FAILURE_DISPOSITION_ATTEMPT_LIMIT,
                                outcome = %observed.note,
                                "failed-file disposition is incomplete; leaving the queue row retryable"
                            );
                            return;
                        }

                        if finalizing.lifecycle_eligible() {
                            tracing::warn!(job = job.0, attempts, outcome = %observed.note,
                                "failed-file disposition remains retryable; preserving source");
                            return;
                        }

                        tracing::error!(
                            job = job.0,
                            attempts,
                            outcome = %observed.note,
                            "failed-file disposition exhausted its local retry bound; recording the observed failure"
                        );
                        Some(Disposition {
                            note: observed.note,
                            // A failed Delete/Park leaves operator-cleanup work.
                            // Preserve the path observed by dispose_failed; it is
                            // evidence of where the bytes remain, not a claim that
                            // the configured action succeeded.
                            files_at: observed.files_at,
                        })
                    }
                }
            }
            _ => None,
        }
    };
    // Park may be a cross-filesystem copy. If authority or disk admission
    // changed while it awaited, do not publish a terminal decision. A later
    // authority retry recognizes the already-moved target idempotently.
    if gate.as_ref().is_some_and(|claim| !claim(job)) {
        return;
    }
    let mut fail_params = exported.as_ref().map(terminal_params).unwrap_or_default();
    fail_params.push((
        "Failure:Stage".into(),
        salvage
            .as_ref()
            .and_then(|salvage| salvage.delivery.as_ref())
            .and_then(|delivery| delivery.failure_stage.clone())
            .unwrap_or_else(|| if fetch_failure { "fetch" } else { "health" }.into()),
    ));
    if keep_on_failure {
        fail_params.push(("Failure:KeepOnFailure".into(), "true".into()));
    }
    if let Some(d) = disposition.as_ref() {
        fail_params.push(("Failure:Files".into(), d.note.clone()));
    }
    if let Some((_, reason)) = &recoverable_failed_source {
        fail_params.push((
            "Failure:Files".into(),
            format!("retained for recovery: {reason}"),
        ));
    }
    if let Some(delivery) = salvage
        .as_ref()
        .and_then(|salvage| salvage.delivery.as_ref())
    {
        let source_cleanup_status = engine
            .artifacts()
            .delivery(&delivery.delivery_id)
            .map(|current| current.source_cleanup.status)
            .unwrap_or_else(|_| delivery.source_cleanup.status.clone());
        fail_params.extend([
            ("Delivery:Id".into(), delivery.delivery_id.clone()),
            ("Delivery:Path".into(), delivery.publication_path.clone()),
            ("Delivery:Custody".into(), "curator".into()),
            (
                "Delivery:SourceArtifactId".into(),
                delivery.source_artifact_id.clone(),
            ),
            (
                "Delivery:SourceGeneration".into(),
                delivery.source_generation.clone(),
            ),
            (
                "Delivery:Generation".into(),
                delivery.publication_generation.clone(),
            ),
            (
                "Delivery:ManifestDigest".into(),
                delivery.manifest_digest.clone(),
            ),
            ("Artifact:Id".into(), delivery.artifact_id.clone()),
            ("Delivery:SourceCleanup".into(), source_cleanup_status),
        ]);
        if delivery.keep_source_on_failure {
            fail_params.push(("Failure:KeepOnFailure".into(), "true".into()));
        }
        if let Some(cause) = &delivery.failure_cause {
            fail_params.push(("Failure:Reason".into(), cause.clone()));
        }
    } else if fetch_failure {
        fail_params.push(("Artifact:Custody".into(), "no_payload".into()));
    } else if let Some(salvage) = &salvage {
        fail_params.extend([
            ("Artifact:Id".into(), salvage.artifact_id.clone()),
            ("Artifact:Generation".into(), salvage.generation.clone()),
            (
                "Artifact:SourceId".into(),
                salvage.source_artifact_id.clone(),
            ),
            (
                "Artifact:SourceGeneration".into(),
                salvage.source_generation.clone(),
            ),
            ("Artifact:Custody".into(), "runner_owned".into()),
        ]);
    } else if disposition.as_ref().is_some_and(|d| d.note == "deleted") {
        fail_params.push(("Artifact:Custody".into(), "deleted_by_runner".into()));
    } else if keep_on_failure {
        fail_params.push(("Artifact:Custody".into(), "kept_by_user".into()));
    } else if recoverable_failed_source.is_some() || disposition.is_some() {
        fail_params.push(("Artifact:Custody".into(), "runner_owned".into()));
    }
    if salvage.is_none() && disposition.as_ref().is_some_and(|d| d.files_at.is_some()) {
        // A retained failed source remains addressable in Resolve after the
        // queue row is retired. Record its exact artifact generation rather
        // than asking the consumer to infer ownership from a path.
        if let Ok(Some(source)) = engine.artifacts().for_job(job.0) {
            if let Ok(retained) =
                engine
                    .artifacts()
                    .finish(job.0, &source.path, &source.root, "retained")
            {
                fail_params.extend([
                    ("Artifact:Id".into(), retained.id),
                    ("Artifact:Generation".into(), retained.generation),
                    (
                        "Artifact:Custody".into(),
                        if keep_on_failure {
                            "kept_by_user"
                        } else {
                            "runner_owned"
                        }
                        .into(),
                    ),
                ]);
            }
        }
    }
    let mut entry = HistoryEntry {
        job,
        name: name.clone(),
        category: exported.as_ref().and_then(|j| j.category.clone()),
        final_dir: salvage
            .as_ref()
            .map(|salvage| salvage.published_path.to_string_lossy().into_owned())
            .or_else(|| {
                recoverable_failed_source
                    .as_ref()
                    .map(|(path, _)| path.to_string_lossy().into_owned())
            })
            .or_else(|| {
                disposition
                    .as_ref()
                    .and_then(|d| d.files_at.as_ref())
                    .map(|p| p.to_string_lossy().into_owned())
            }),
        status: fail_status.into(),
        size: exported.as_ref().map(|j| j.totals.size).unwrap_or(0),
        health,
        params: fail_params.clone(),
        dupe_key: exported
            .as_ref()
            .map(|j| j.dupe.key.clone())
            .unwrap_or_default(),
        dupe_score: exported.as_ref().map(|j| j.dupe.score).unwrap_or(0),
        completed_at_unix,
        hidden: false,
        first_seen_at_unix: None,
        last_seen_at_unix: None,
        seen_count: 0,
        removed_at_unix: None,
        picked_up_by: None,
        record: exported.as_ref().map(nzbd_state::JobRecord::from_job),
        stages: exported
            .as_ref()
            .map(|j| j.stages.clone())
            .unwrap_or_default(),
        seq: 0,
    };
    if let Some(path) = entry.final_dir.as_deref().filter(|_| salvage.is_none()) {
        let path = Path::new(path);
        let state = if entry
            .params
            .iter()
            .any(|(k, v)| k == "Failure:Files" && v.starts_with("parked at "))
        {
            "parked_failed"
        } else {
            "retained"
        };
        match engine
            .artifacts()
            .finish(job.0, path, path.parent().unwrap_or(dest_dir), state)
        {
            Ok(artifact) => {
                // Keep-on-failure is a separate mutable source policy, not
                // the generic artifact Keep control. Persist the terminal
                // classifier result before PP_DONE so clearing Keep after a
                // restart can dispose only positively unusable data.
                if keep_on_failure
                    && exported
                        .as_ref()
                        .is_some_and(|job| job.lifecycle_eligible())
                {
                    let (disposition, reason) = failed_source_assessment
                        .as_ref()
                        .map(|(_, assessment)| {
                            (assessment.disposition.as_str(), assessment.reason.as_str())
                        })
                        .unwrap_or((
                            "unknown",
                            "bounded failed-source assessment did not complete; retry diagnostics before cleanup",
                        ));
                    if let Err(error) = engine.artifacts().set_failure_assessment(
                        &artifact.id,
                        &artifact.generation,
                        disposition,
                        reason,
                    ) {
                        tracing::error!(job=job.0,error=%error,"failed source assessment was not committed");
                        return;
                    }
                }
                entry.params.push(("Artifact:Id".into(), artifact.id));
            }
            Err(e) => {
                tracing::error!(job=job.0,error=%e,"failed payload inventory not committed; finalization held");
                return;
            }
        }
    }
    if !entry.params.iter().any(|(k, _)| k == "Artifact:Id") {
        if let Ok(Some(a)) = engine.artifacts().for_job(job.0) {
            entry.params.push(("Artifact:Id".into(), a.id));
        }
    }
    let h = history.clone();
    let record = entry.clone();
    let (history_seq, entry) =
        match tokio::task::spawn_blocking(move || h.record_seq_durable(&record)).await {
            Ok(Ok(recorded)) => recorded,
            Ok(Err(error)) => {
                if nzbd_engine::is_out_of_space(&error.to_string()) {
                    engine.report_out_of_space(format!("record failed-job history: {error}"));
                }
                tracing::error!(job = job.0, error = %error,
                "failed job was not stamped because durable history failed");
                return;
            }
            Err(error) => {
                tracing::error!(job = job.0, error = %error,
                "failed job was not stamped because history task failed");
                return;
            }
        };
    if gate.as_ref().is_some_and(|claim| !claim(job)) {
        return;
    }
    // These jobs never enter the stage pipeline, but they are terminal, and
    // a consumer waiting for `job_pp_finished` must not wait forever after
    // disk admission recovers.
    let pending_completion = engine
        .artifacts()
        .for_job(job.0)
        .ok()
        .flatten()
        .filter(|_| exported.is_some())
        .map(|artifact| nzbd_state::artifacts::StagedCompletion {
            source: nzbd_state::artifacts::SourceGeneration {
                artifact: artifact.id,
                generation: artifact.generation,
            },
            outcome: fail_status.to_string(),
            proof: nzbd_state::artifacts::FinalizationProof::Local {
                history_seq: history_seq as u64,
            },
        });
    if let Some(pending) = &pending_completion {
        let pending = pending.clone();
        let inventory = engine.artifacts();
        if !matches!(
            tokio::task::spawn_blocking(move || inventory.stage_workspace_finalization(
                &pending.source,
                &pending.outcome,
                &pending.proof
            ))
            .await,
            Ok(Ok(()))
        ) {
            return;
        }
    }
    if let Some(mut fin) = exported {
        for (key, value) in &entry.params {
            if matches!(
                key.as_str(),
                "Failure:Stage"
                    | "Failure:Reason"
                    | "Failure:KeepOnFailure"
                    | "Failure:Files"
                    | "Delivery:Id"
                    | "Delivery:Path"
                    | "Delivery:Custody"
                    | "Delivery:Generation"
                    | "Delivery:ManifestDigest"
                    | "Delivery:SourceCleanup"
                    | "Delivery:SourceArtifactId"
                    | "Delivery:SourceGeneration"
                    | "Artifact:Custody"
                    | "Artifact:Id"
                    | "Artifact:Generation"
            ) {
                fin.params.retain(|(existing, _)| existing != key);
                fin.params.push((key.clone(), value.clone()));
            }
        }
        fin.params.retain(|(key, _)| {
            key != PP_DONE_PARAM
                && key != PP_SELECTED_OUTCOME_PARAM
                && key != PP_SELECTED_FAILURE_CAUSE_PARAM
        });
        fin.params.push((PP_DONE_PARAM.into(), fail_status.into()));
        fin.params.retain(|(key, _)| key != "*PP:history-seq");
        fin.params
            .push(("*PP:history-seq".into(), history_seq.to_string()));
        match engine.import_job_if_present(fin).await {
            Ok(true) => {}
            Ok(false) => return,
            Err(error) => {
                tracing::error!(job = job.0, error = %error,
                    "failed job history is durable but its terminal stamp was not committed");
                return;
            }
        }
    }
    if let Some(pending) = pending_completion {
        let inventory = engine.artifacts();
        if !matches!(
            tokio::task::spawn_blocking(move || {
                inventory.confirm_stamped_finalization(&pending)?;
                inventory.finalize_workspaces(&pending.source, &pending.outcome, pending.proof)
            })
            .await,
            Ok(Ok(()))
        ) {
            tracing::warn!(job = job.0, "failed job workspace receipt remains pending");
        }
    }
    engine.emit(Event::JobPpFinished {
        job,
        name,
        category: entry.category.clone(),
        pp_status: fail_status.into(),
        final_dir: entry.final_dir.clone(),
        size_bytes: entry.size,
        health,
        params: entry.params.clone(),
        history_seq,
    });
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
async fn repair_failed_health_par_sets(
    engine: &EngineHandle,
    cfg: &PostConfig,
    job: &Job,
    gate: &PpGate,
) -> Result<HashMap<String, String>, String> {
    let inventory = engine.artifacts();
    let source = inventory
        .for_job(job.id.0)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "failed job has no source artifact".to_string())?;
    let root = source.path.clone();
    let entries = source.files.clone();
    let gate_for_scan = gate.clone();
    let id = job.id;
    let sets = tokio::task::spawn_blocking(move || {
        par_repairable_evidence_checked(&root, &entries, &|| {
            crate::attempt::checkpoint()?;
            if gate_for_scan.as_ref().is_some_and(|claim| !claim(id)) {
                return Err(std::io::ErrorKind::Interrupted.into());
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())?
    .map_err(|error| error.to_string())?
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
            return Err("post-processing authority changed during PAR salvage".into());
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
            Err(error) => return Err(format!("PAR repair requires retry: {error}")),
        };
        for file in &set.files {
            let relative =
                crate::namespace::relative(&file.name).map_err(|error| error.to_string())?;
            let source_relative = set
                .root
                .join(relative)
                .strip_prefix(&source.path)
                .map_err(|error| error.to_string())?
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
) -> Result<EarlyArchiveAssessment, String> {
    let mut assessment = EarlyArchiveAssessment::default();
    if !cfg.unpack {
        return Ok(assessment);
    }
    let inventory = engine.artifacts();
    let source = inventory
        .for_job(job.id.0)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "failed job has no source artifact".to_string())?;
    let restored_paths = inventory
        .verified_restore_paths(job.id.0)
        .map_err(|error| error.to_string())?;
    let mut complete = verified_direct_payload_paths(job, &source, &restored_paths, || Ok(()))
        .map_err(|error| error.to_string())?;
    let (_, repaired_or_extracted) = inventory
        .verified_transform_evidence(job.id.0)
        .map_err(|error| error.to_string())?;
    complete.extend(repaired_or_extracted);
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
            return Err("post-processing authority changed during archive salvage".into());
        }
        let inputs = archive_input_set(&source.path, &archive, kind);
        let paths = inputs
            .iter()
            .map(|path| {
                path.strip_prefix(&source.path)
                    .map(|relative| relative.to_string_lossy().into_owned())
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        if paths.is_empty() || !paths.iter().all(|path| complete.contains(path)) {
            continue;
        }
        let token = format!("{:x}", Sha256::digest(archive.to_string_lossy().as_bytes()));
        let mut workspace = inventory
            .workspace(job.id.0, "extract", &token)
            .map_err(|error| error.to_string())?;
        inventory
            .bind_extraction_inputs(&mut workspace, &inputs)
            .map_err(|error| error.to_string())?;
        if inventory
            .extraction_published(&workspace)
            .map_err(|error| error.to_string())?
        {
            continue;
        }
        let _capacity = nzbd_state::capacity::reserve(&workspace.scratch.path, job.totals.size)
            .map_err(|error| error.to_string())?;
        let attempt = inventory
            .begin_workspace_attempt(&mut workspace)
            .map_err(|error| error.to_string())?;
        let staging = attempt.join("output");
        std::fs::create_dir_all(&staging).map_err(|error| error.to_string())?;
        let result = extractors
            .extract(&archive, kind, &staging, password)
            .await
            .map_err(|error| error.to_string())?;
        if result.disk_space_error {
            inventory
                .abandon_workspace(&workspace)
                .map_err(|error| error.to_string())?;
            return Err(
                "archive salvage needs free capacity; retry extraction when space is available"
                    .into(),
            );
        }
        if result.password_error {
            assessment.password_required.extend(paths);
            inventory
                .abandon_workspace(&workspace)
                .map_err(|error| error.to_string())?;
            continue;
        }
        if result.retryable_error {
            inventory
                .abandon_workspace(&workspace)
                .map_err(|error| error.to_string())?;
            let detail = result
                .attempts
                .iter()
                .map(|attempt| attempt.diagnostic.as_str())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(format!(
                "archive extractor storage or tool failure requires retry: {detail}"
            ));
        }
        let shortfall = result
            .success
            .then(|| crate::tools::unpack_shortfall(&source.path, &archive, &result.output_dir))
            .flatten();
        if !result.success || shortfall.is_some() {
            assessment.terminal.extend(paths);
            inventory
                .abandon_workspace(&workspace)
                .map_err(|error| error.to_string())?;
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
        .await
        .map_err(|error| error.to_string())?;
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
) -> Result<Option<FailedSalvage>, String> {
    let source = inventory
        .refresh_for_delivery(job.id.0)
        .map_err(|error| error.to_string())?;
    if !source.automatic_cleanup_eligible || !source.owned {
        return Err(
            "failed source is not an owned eligible generation for bounded recovery assessment"
                .into(),
        );
    }
    let restored_paths = inventory
        .verified_restore_paths(job.id.0)
        .map_err(|error| error.to_string())?;
    let verified_direct = verified_direct_payload_paths(job, &source, &restored_paths, || Ok(()))
        .map_err(|error| error.to_string())?;
    let (consumed, extracted) = inventory
        .verified_transform_evidence(job.id.0)
        .map_err(|error| error.to_string())?;
    let verified = verified_direct.into_iter().chain(extracted).collect();
    let mut diagnostics = assess_file_inputs(job, &source, &verified)
        .map_err(|error| format!("source diagnostic read requires retry: {error}"))?;
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
    );
    if selected.is_empty() {
        return Ok(None);
    }
    std::fs::create_dir_all(completed_root).map_err(|error| error.to_string())?;
    let name = nzbd_engine::queue::sanitize_name(&job.name);
    let target = completed_root.join(format!("{name}.runner-{}", source.generation));
    let delivery_lifecycle = job.delivery_lifecycle;
    let moved = if delivery_lifecycle {
        inventory.publish_selected_for_delivery_checked(
            job.id.0,
            &target,
            selected,
            outcomes,
            &|| Ok(()),
        )
    } else {
        inventory.publish_selected_checked(job.id.0, &target, selected, outcomes, &|| Ok(()))
    }
    .map_err(|error| error.to_string())?;
    inventory
        .finish(
            job.id.0,
            &moved.published_path,
            moved.published_path.parent().unwrap_or(completed_root),
            "completed",
        )
        .map_err(|error| error.to_string())?;
    let delivery = if delivery_lifecycle {
        let token = job
            .params
            .iter()
            .find(|(key, _)| key == "monarr-transfer")
            .map(|(_, value)| value.as_str());
        Some(
            inventory
                .create_delivery_with_outcome_and_keep(
                    job.id.0,
                    true,
                    token,
                    terminal_outcome,
                    Some(failure_stage),
                    Some(failure_cause),
                    job_keeps_failed_source(job),
                )
                .map_err(|error| error.to_string())?,
        )
    } else {
        None
    };
    Ok(Some(FailedSalvage {
        delivery,
        published_path: moved.published_path,
        artifact_id: moved.artifact_id,
        generation: moved.generation,
        source_artifact_id: source.id,
        source_generation: source.generation,
    }))
}

fn failed_source_recovery(
    inventory: &nzbd_state::artifacts::Inventory,
    job: &Job,
    archive_assessment: &EarlyArchiveAssessment,
) -> Result<(PathBuf, FailedSourceAssessment), String> {
    let source = inventory
        .refresh_for_delivery(job.id.0)
        .map_err(|error| error.to_string())?;
    if !source.automatic_cleanup_eligible || !source.owned {
        return Err(
            "bounded failed-source assessment requires the exact owned eligible generation".into(),
        );
    }
    let restored_paths = inventory
        .verified_restore_paths(job.id.0)
        .map_err(|error| error.to_string())?;
    let verified_direct = verified_direct_payload_paths(job, &source, &restored_paths, || Ok(()))
        .map_err(|error| error.to_string())?;
    let (consumed, extracted) = inventory
        .verified_transform_evidence(job.id.0)
        .map_err(|error| error.to_string())?;
    let verified = verified_direct.into_iter().chain(extracted).collect();
    let mut diagnostics =
        assess_file_inputs(job, &source, &verified).map_err(|error| error.to_string())?;
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
    let (_, outcomes) = select_delivery_payload(
        &source.files,
        &consumed,
        &verified,
        &archive_assessment.terminal,
        &diagnostics,
    );
    let assessment = failed_source_assessment(&outcomes);
    Ok((source.path, assessment))
}

fn failed_source_assessment(outcomes: &[DeliveryFileOutcome]) -> FailedSourceAssessment {
    if outcomes
        .iter()
        .any(|file| file.outcome == "unverified_review")
    {
        return FailedSourceAssessment {
            disposition: "unknown".into(),
            reason: "bounded diagnostics could not establish source integrity; retry assessment or inspect the retained source".into(),
        };
    }
    if let Some(reason) = recovery_reason(outcomes) {
        return FailedSourceAssessment {
            disposition: "recoverable".into(),
            reason,
        };
    }
    if outcomes.is_empty() {
        return FailedSourceAssessment {
            disposition: "unknown".into(),
            reason: "no regular source files were available to the bounded integrity/format assessment; inspect or retry diagnostics".into(),
        };
    }
    if outcomes.iter().all(|file| {
        matches!(
            file.outcome.as_str(),
            "excluded_source_cleanup" | "consumed"
        ) && file
            .reason
            .as_deref()
            .is_some_and(|reason| !reason.trim().is_empty())
    }) {
        return FailedSourceAssessment {
            disposition: "unusable".into(),
            reason: outcomes
                .iter()
                .filter_map(|file| file.reason.as_deref())
                .take(3)
                .collect::<Vec<_>>()
                .join("; "),
        };
    }
    FailedSourceAssessment {
        disposition: "unknown".into(),
        reason: "bounded assessment did not establish either a concrete recovery action or terminal unusability; inspect source or retry diagnostics".into(),
    }
}

struct FailedSourceAssessment {
    disposition: String,
    reason: String,
}

struct FailedSalvage {
    delivery: Option<nzbd_state::artifacts::Delivery>,
    published_path: PathBuf,
    artifact_id: String,
    generation: String,
    source_artifact_id: String,
    source_generation: String,
}

fn job_keeps_failed_source(job: &Job) -> bool {
    job.params
        .iter()
        .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1")
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
            }),
            Ok(false) => Some(Disposition {
                note: "already gone".into(),
                files_at: None,
            }),
            Err(_) => None,
        },
        FailureAction::Delete => match source.try_exists() {
            Ok(false) => Some(Disposition {
                note: "deleted".into(),
                files_at: None,
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
    last_disposition: &mut HashMap<JobId, tokio::time::Instant>,
) {
    let claim = |job: JobId| gate.as_ref().map(|f| f(job)).unwrap_or(true);
    for j in engine.snapshot().jobs.iter() {
        // A temporarily absent configured volume may expose the underlying
        // directory at the same path. Keep all writes blocked while its mount
        // tuple differs, but revalidate periodically so restoring the expected
        // mount resumes the same attempt without a manual Retry click.
        let delivery_lifecycle = j.lifecycle_eligible;
        if delivery_lifecycle
            && !j.pp_done
            && !active.contains_key(&j.id)
            && j.control.as_ref().is_some_and(|control| {
                control.lifecycle == "held"
                    && control.cause == "mount_unavailable"
                    && !control.manual_pause
            })
        {
            if let Some(control) = &j.control {
                let _ = engine.retry_post_hold(j.id, control.revision.clone()).await;
            }
            continue;
        }
        if delivery_lifecycle
            && !j.pp_done
            && !active.contains_key(&j.id)
            && j.control.as_ref().is_some_and(|control| {
                control.lifecycle == "held"
                    && control.retry_policy == "scheduled_retry"
                    && !control.manual_pause
                    && control.next_retry_at_unix.is_some_and(|at| at <= now())
            })
        {
            if let Some(control) = &j.control {
                if !engine
                    .retry_post_hold(j.id, control.revision.clone())
                    .await
                    .unwrap_or(false)
                {
                    // Failed storage rebinding is still retryable. Move the
                    // next attempt out using the same durable backoff state.
                    schedule_recoverable_post_retry(engine, j.id).await;
                }
            }
            continue;
        }
        // NZBGet parity: finished-and-processed jobs live in history, not
        // the queue. Single-node the manager IS the authority; in cluster
        // mode the leader sweep retires (the gate stays out of hygiene).
        if gate.is_none()
            && j.pp_done
            && matches!(j.status, JobStatus::Completed | JobStatus::Failed)
        {
            if active.contains_key(&j.id) {
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
        if !j.pp_done && j.status == JobStatus::Failed && claim(j.id) {
            handle_failed_job(
                engine,
                cfg,
                history,
                dest_dir,
                gate,
                j.id,
                j.name.clone(),
                j.health,
                last_disposition,
            )
            .await;
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
}

async fn schedule_recoverable_post_retry(engine: &EngineHandle, job: JobId) {
    let Ok(Some(mut row)) = engine.export_job(job).await else {
        return;
    };
    if !row.lifecycle_eligible() {
        return;
    }
    let Some(mut control) = row.control().filter(|control| {
        control.lifecycle == "held"
            && matches!(control.cause.as_str(), "io" | "missing_tool")
            && !control.manual_pause
    }) else {
        return;
    };

    let delay = post_retry_delay_seconds(control.retry_attempt);
    control.retry_attempt = control.retry_attempt.saturating_add(1);
    control.next_retry_at_unix = Some(now().saturating_add(delay));
    control.blocked_since_unix.get_or_insert_with(now);
    control.retry_policy = "scheduled_retry".into();
    let reason = control
        .message
        .split("; retry ")
        .next()
        .unwrap_or("recoverable post-processing error")
        .chars()
        .take(1200)
        .collect::<String>();
    control.message = format!(
        "{reason}; retry {} scheduled in {} seconds; source retained while Runner retries",
        control.retry_attempt, delay,
    );
    row.set_control(&control);
    if !engine.import_job_if_present(row).await.unwrap_or(false) {
        tracing::warn!(job = job.0, "recoverable retry schedule was not committed");
    }
}

fn post_retry_delay_seconds(retry_attempt: u8) -> i64 {
    POST_RETRY_DELAYS_SECONDS[usize::from(retry_attempt).min(POST_RETRY_DELAYS_SECONDS.len() - 1)]
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
    let current = engine
        .export_job(job)
        .await
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
                .map_err(|e| PostError::Subprocess(e.to_string()))?;
            inventory
                .finalize_workspaces(&receipt.source, &receipt.outcome, receipt.proof)
                .map_err(|e| PostError::Subprocess(e.to_string()))
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
    let engine = engine.clone();
    let cfg = cfg.clone();
    let history = history.clone();
    let dest = dest_dir.to_path_buf();
    let sem = sem.clone();
    let global_cancel = global_cancel.clone();
    let attempt_cancel = CancellationToken::new();
    let task_cancel = attempt_cancel.clone();
    let finished = finished.clone();
    tracker.spawn(async move {
        // Cancel-aware at both await points. Without this, one PP job —
        // a par2 repair, an unpack, an extension script — kept the PP
        // tracker's shutdown wait (and with it the whole daemon restart)
        // hostage until it finished: "clicked restart, page never came
        // back" (field report 2026-07-25, second of the day). Aborting is
        // safe by the pipeline's own crash model: no `*PP:done` stamp has
        // been written, subprocesses are kill-on-drop, and the next pass's
        // rescan re-runs the job from scratch.
        let permit =
            acquire_admitted_permit(sem, global_cancel.clone(), task_cancel.clone(), gate, job)
                .await;
        if let Some(permit) = permit {
            let _permit = permit;
            let ctx = PpCtx {
                cancel: task_cancel.child_token(),
                commit_ok: Arc::new({ let global = global_cancel.clone(); let task = task_cancel.clone(); move || !global.is_cancelled() && !task.is_cancelled() }),
                ..PpCtx::default()
            };
            let outcome = tokio::select! {
                _ = global_cancel.cancelled() => None,
                _ = task_cancel.cancelled() => None,
                r = process_job_ctx_from(&engine, &cfg, &history, &dest, job, &ctx, from) => Some(r),
            };
            ctx.cancel.cancel(); ctx.workers.close(); ctx.workers.wait().await;
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
                    if retire_local {
                        // NZBGet parity: the finished job's record IS the
                        // history entry — move it out of the queue now.
                        let _ = engine.remove_job_silent(job).await;
                    }
                }
                Some(Err(PostError::Held)) => {
                    // The stage owns the persisted cause and revision. I/O or
                    // missing-tool holds get a durable capped-backoff retry;
                    // capacity/quota holds use the owner's resource recovery.
                    schedule_recoverable_post_retry(&engine, job).await;
                }
                Some(Err(e)) => {
                    let detail = e.to_string();
                    let cause = if nzbd_engine::is_out_of_space(&detail) {
                        if detail.to_lowercase().contains("quota") {
                            "quota"
                        } else {
                            "capacity"
                        }
                    } else if matches!(&e, PostError::ToolMissing(_)) {
                        "missing_tool"
                    } else if matches!(
                        &e,
                        PostError::Io(_)
                            | PostError::Artifact(
                                nzbd_state::artifacts::Error::Io(_)
                                    | nzbd_state::artifacts::Error::Sql(_)
                            )
                    ) {
                        "io"
                    } else if detail.contains("identity")
                        || detail.contains("collision")
                        || detail.contains("unsafe")
                    {
                        if detail.contains("configured root identity changed or volume is unavailable") {
                            "mount_unavailable"
                        } else {
                            "identity_conflict"
                        }
                    } else {
                        "post_failure"
                    };
                    let stage = engine
                        .snapshot()
                        .jobs
                        .iter()
                        .find(|j| j.id == job)
                        .and_then(|j| j.stages.last())
                        .map(|s| s.stage.as_str().to_string())
                        .unwrap_or_else(|| "extract".into());
                    let _ = engine.hold_job(job, cause, &stage, &detail).await;
                    if matches!(cause, "io" | "missing_tool") {
                        schedule_recoverable_post_retry(&engine, job).await;
                    }
                    // PP writes as much as the downloader does — unpack,
                    // move, script output. An ENOSPC here is the same ground
                    // truth about the volume and must stop intake too, not
                    // just fail this job.
                    if nzbd_engine::is_out_of_space(&e.to_string()) {
                        engine.report_out_of_space(format!("post-processing job {}: {e}", job.0));
                    }
                    tracing::error!(job = job.0, error = %e, "post-processing crashed");
                }
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

/// Parameters exposed in history: everything except `*`-internal ones.
fn user_params(job: &Job) -> Vec<(String, String)> {
    job.params
        .iter()
        .filter(|(k, _)| !k.starts_with('*'))
        .cloned()
        .collect()
}

/// Terminal history/events retain the last authoritative control revision.
fn terminal_params(job: &Job) -> Vec<(String, String)> {
    let mut params = user_params(job);
    params.extend(
        job.params
            .iter()
            .filter(|(key, _)| key == nzbd_types::CONTROL_PARAM)
            .cloned(),
    );
    params
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn param_i64(params: &[(String, String)], key: &str) -> Option<i64> {
    params
        .iter()
        .find(|(name, _)| name == key)
        .and_then(|(_, value)| value.parse().ok())
}

fn set_param(params: &mut Vec<(String, String)>, key: &str, value: String) {
    if let Some((_, existing)) = params.iter_mut().find(|(name, _)| name == key) {
        *existing = value;
    } else {
        params.push((key.to_owned(), value));
    }
}

fn failed_cleanup_retry_delay(attempt: u32) -> i64 {
    let index = attempt
        .saturating_sub(1)
        .min((POST_RETRY_DELAYS_SECONDS.len() - 1) as u32);
    POST_RETRY_DELAYS_SECONDS[index as usize]
}

async fn record_failed_cleanup_retry(engine: &EngineHandle, id: JobId, error: String) {
    let Ok(Some(mut row)) = engine.export_job(id).await else {
        return;
    };
    if !row.lifecycle_eligible() {
        return;
    }
    let attempts = param_i64(&row.params, PP_FAILURE_DISPOSITION_ATTEMPTS_PARAM)
        .unwrap_or(0)
        .max(0) as u32
        + 1;
    set_param(
        &mut row.params,
        PP_FAILURE_DISPOSITION_ATTEMPTS_PARAM,
        attempts.to_string(),
    );
    set_param(
        &mut row.params,
        PP_FAILED_CLEANUP_NEXT_RETRY_PARAM,
        now()
            .saturating_add(failed_cleanup_retry_delay(attempts))
            .to_string(),
    );
    if param_i64(&row.params, PP_FAILED_CLEANUP_BLOCKED_SINCE_PARAM).is_none() {
        set_param(
            &mut row.params,
            PP_FAILED_CLEANUP_BLOCKED_SINCE_PARAM,
            now().to_string(),
        );
    }
    set_param(
        &mut row.params,
        PP_FAILED_CLEANUP_LAST_ERROR_PARAM,
        error.clone(),
    );
    if let Err(commit_error) = engine.import_job_if_present(row.clone()).await {
        tracing::warn!(job = id.0, %commit_error, "could not persist failed-source retry state");
        return;
    }
    let blocked_since =
        param_i64(&row.params, PP_FAILED_CLEANUP_BLOCKED_SINCE_PARAM).unwrap_or_else(now);
    tracing::warn!(
        job = id.0,
        attempts,
        next_retry_at = now().saturating_add(failed_cleanup_retry_delay(attempts)),
        warning = now().saturating_sub(blocked_since) >= 30 * 60,
        %error,
        "failed-source cleanup remains retryable"
    );
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
        if let Ok(Err(error)) = tokio::task::spawn_blocking(move || {
            inventory.finalize_workspaces(&completion.source, &completion.outcome, completion.proof)
        })
        .await
        {
            tracing::warn!(job = job_id.0, %error, "workspace finalization remains pending");
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
    let resumed =
        tokio::task::spawn_blocking(move || finish_committed_publication_receipt(&inventory, &job))
            .await
            .map_err(|error| PostError::Subprocess(error.to_string()))??;
    if resumed.is_none() {
        return Ok(None);
    }
    resume_durable_delivery(engine, history, job_id).await
}

fn finish_committed_publication_receipt(
    inventory: &Arc<nzbd_state::artifacts::Inventory>,
    job: &Job,
) -> Result<Option<nzbd_state::artifacts::Delivery>, PostError> {
    if !job.delivery_lifecycle {
        return Ok(None);
    }
    let Some(outcome) = job
        .params
        .iter()
        .find(|(key, _)| key == PP_SELECTED_OUTCOME_PARAM)
        .map(|(_, value)| value.as_str())
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
        .map(|(_, value)| value.as_str());
    let failure_stage = (outcome == PpFinal::HealthFailure.as_str()).then_some("health");
    let failure_cause = job
        .params
        .iter()
        .find(|(key, _)| key == PP_SELECTED_FAILURE_CAUSE_PARAM)
        .map(|(_, value)| value.as_str());
    if failure_stage.is_some() && failure_cause.is_none() {
        return Err(PostError::Subprocess(
            "committed failed-health publication lacks its durable failure cause".into(),
        ));
    }
    inventory
        .finish(
            job.id.0,
            &published,
            published.parent().unwrap_or(&published),
            "completed",
        )
        .map_err(PostError::from)?;
    let delivery = inventory
        .create_delivery_with_outcome_and_keep(
            job.id.0,
            true,
            request_token,
            outcome,
            failure_stage,
            failure_cause,
            outcome != PpFinal::Success.as_str() && job_keeps_failed_source(job),
        )
        .map_err(PostError::from)?;
    Ok(Some(delivery))
}

/// A consumer can accept a ready delivery before Runner has appended terminal
/// History. Recover that handoff from its durable manifest without opening the
/// publication path: Curator may already have imported and removed it.
async fn resume_durable_delivery(
    engine: &EngineHandle,
    history: &Arc<HistoryDb>,
    job_id: JobId,
) -> Result<Option<PpFinal>, PostError> {
    let Some(mut job) = engine
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
            )))
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
    job.status = if final_outcome == PpFinal::Success {
        JobStatus::Completed
    } else {
        JobStatus::Failed
    };

    let entry = HistoryEntry {
        job: job_id,
        name: job.name.clone(),
        category: job.category.clone(),
        final_dir: Some(delivery.publication_path.clone()),
        status: final_outcome.as_str().into(),
        size: job.totals.size,
        health: Health::calc(&job.totals).0,
        params: terminal_params(&job),
        dupe_key: job.dupe.key.clone(),
        dupe_score: job.dupe.score,
        completed_at_unix: delivery.ready_at,
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
    let history_db = history.clone();
    let (history_seq, _) =
        tokio::task::spawn_blocking(move || history_db.record_seq_durable(&entry))
            .await
            .map_err(|e| PostError::Subprocess(e.to_string()))?
            .map_err(|e| PostError::Subprocess(e.to_string()))?;
    job.params
        .push(("*PP:history-seq".into(), history_seq.to_string()));
    let _ = engine
        .import_job_if_present(job.clone())
        .await
        .map_err(|e| PostError::Subprocess(format!("terminal delivery stamp: {e}")))?;
    engine.emit(Event::JobPpFinished {
        job: job_id,
        name: job.name.clone(),
        category: job.category.clone(),
        pp_status: final_outcome.as_str().into(),
        final_dir: Some(delivery.publication_path),
        size_bytes: job.totals.size,
        health: Health::calc(&job.totals).0,
        params: terminal_params(&job),
        history_seq,
    });
    Ok(Some(final_outcome))
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
        renames = crate::attempt::current()
            .expect("processing control")
            .blocking(move |_| par_rename_owned(&rename_dir, Some((&inventory, job_id.0))))
            .await?;
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
            let _capacity = nzbd_state::capacity::reserve(&dir, job.totals.size)?;
            for (archive, kind) in &archives {
                use sha2::{Digest, Sha256};
                let token = format!("{:x}", Sha256::digest(archive.to_string_lossy().as_bytes()));
                let archive_inputs = archive_input_set(&dir, archive, *kind);
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
                    let cause = if r.quota_error { "quota" } else { "capacity" };
                    let detail = r
                        .attempts
                        .iter()
                        .map(|a| format!("{} exit {}: {}", a.executable, a.exit_code, a.diagnostic))
                        .collect::<Vec<_>>()
                        .join("; ");
                    let _ = engine.hold_job(job_id, cause, "extract", &detail).await;
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
                    let _ = engine.hold_job(job_id, "io", "extract", &detail).await;
                    return Err(PostError::Held);
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
                    let cause = if r.quota_error { "quota" } else { "capacity" };
                    let _ = engine
                        .hold_job(
                            job_id,
                            cause,
                            "extract",
                            "extractor retry exhausted storage",
                        )
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
                    let _ = engine.hold_job(job_id, "io", "extract", &detail).await;
                    return Err(PostError::Held);
                }
                if r.password_error {
                    engine
                        .artifacts()
                        .abandon_workspace(&workspace)
                        .map_err(|e| PostError::Subprocess(e.to_string()))?;
                    let _ = engine
                        .hold_job(
                            job_id,
                            "password_required",
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
            &sanitized,
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
                                .hold_job(job_id, "missing_tool", "script", &e.to_string())
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
    let outcome = if !par_ok {
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
    if let Some(control) = crate::attempt::current() {
        control.finish_filesystem_work().await?;
    }
    // Publication is allowed only for bytes with positive integrity evidence.
    // Successful extraction journals prove their outputs; directly downloaded
    // files require both a completed writer CRC and a fresh CRC over the
    // post-script source bytes.
    let (journal_inputs, extraction_outputs, verified_direct) = if job.lifecycle_eligible() {
        let inventory = engine.artifacts();
        let (inputs, outputs) = inventory
            .verified_transform_evidence(job_id.0)
            .map_err(PostError::from)?;
        // The active artifact is intentionally only an allocation record
        // until this quiescence boundary. Refresh its file identities now so
        // direct-media CRC proof is bound to the files actually on disk.
        let source = inventory
            .refresh_for_delivery(job_id.0)
            .map_err(PostError::from)?;
        let restore_inventory = inventory.clone();
        let job = job.clone();
        let direct = crate::attempt::current()
            .expect("processing control")
            .blocking(move |control| {
                let restored_paths = restore_inventory
                    .verified_restore_paths_with_checkpoint(job_id.0, || control.checkpoint())
                    .map_err(PostError::from)?;
                verified_direct_payload_paths(&job, &source, &restored_paths, || {
                    control.checkpoint()
                })
            })
            .await?;
        (inputs, outputs, direct)
    } else {
        (Default::default(), Default::default(), Default::default())
    };
    consumed_archive_paths.extend(journal_inputs);
    let verified_payload_paths = extraction_outputs
        .into_iter()
        .chain(verified_direct)
        .collect::<std::collections::HashSet<_>>();
    // ---- disposition of a terminal failure ---------------------------------
    // A failed job's bytes are known-bad, and until this existed nothing
    // ever removed them: the directory stayed in the very tree the
    // importer watches, one ~90 GB corpse per failed grab, until the
    // volume filled (docs/REGRAB_LOOP_PLAN.md D2). Before the history row
    // is written, so the row can say where the files went.
    let (has_durable_salvage, recoverable_source_reason) = if job.lifecycle_eligible()
        && outcome != PpFinal::Success
    {
        let inventory = engine.artifacts();
        match inventory.for_job(job_id.0).map_err(PostError::from)? {
            Some(source) => {
                let mut diagnostics = assess_file_inputs(&job, &source, &verified_payload_paths)?;
                for path in &password_required_archive_paths {
                    diagnostics.insert(path.clone(), FileAssessment::Repairable(
                        "archive requires a password; set *Unpack:Password and retry extraction".into(),
                    ));
                }
                let (selected, outcomes) = select_delivery_payload(
                    &source.files,
                    &consumed_archive_paths,
                    &verified_payload_paths,
                    &terminal_unusable_archive_paths,
                    &diagnostics,
                );
                (!selected.is_empty(), recovery_reason(&outcomes))
            }
            None => (false, None),
        }
    } else {
        (false, None)
    };
    // Keep is mutable while tools are running. Read it from the owner at the
    // disposal boundary so a stale PP snapshot cannot silently discard a
    // newly requested retention (or preserve a source after Keep was cleared).
    let keep_at_disposition = if outcome != PpFinal::Success {
        engine
            .export_job(job_id)
            .await
            .map_err(|e| PostError::Subprocess(format!("read live Keep setting: {e}")))?
            .as_ref()
            // A disappearing job cannot authorize deleting its source.
            .is_none_or(job_keeps_failed_source)
    } else {
        false
    };
    let mut failure_cfg = cfg.clone();
    if job.lifecycle_eligible() && outcome != PpFinal::Success && !keep_at_disposition {
        // Keep the old global policy for legacy work only. New attempts either
        // publish verified salvage or purge the unusable source by default.
        failure_cfg.failure_action = FailureAction::Delete;
    }
    let disposition = if outcome == PpFinal::Success
        || has_durable_salvage
        || recoverable_source_reason.is_some()
        || keep_at_disposition
    {
        None
    } else {
        Some(
            dispose_failed(
                engine.artifacts(),
                &failure_cfg,
                job_id,
                &dir,
                dest_dir,
                &sanitized,
                &ctx.tag,
            )
            .await,
        )
    };

    // Close the last stage span BEFORE the export below reads the job:
    // the timeline that lands in history has to be complete, and the
    // final stage is the one an operator looks at first.
    stages.finish().await;
    // Stamp + set final status in one import (replaces the job atomically).
    if let Ok(Some(mut fin)) = engine.export_job(job_id).await {
        fin.params.retain(|(key, _)| key != PP_DONE_PARAM);
        fin.params
            .push((PP_DONE_PARAM.into(), outcome.as_str().into()));
        if outcome != PpFinal::Success && keep_at_disposition {
            fin.params
                .push(("Failure:KeepOnFailure".into(), "true".into()));
        }
        if let Some(reason) = &recoverable_source_reason {
            fin.params.push((
                "Failure:Files".into(),
                format!("retained for recovery: {reason}"),
            ));
        }
        if outcome == PpFinal::ParFailure {
            fin.params.push((
                "Failure:Reason".into(),
                "Missing data could not be reconstructed from available PAR2 recovery files".into(),
            ));
        }
        if outcome != PpFinal::Success {
            let failure_stage = match outcome {
                PpFinal::ParFailure => "par_verify",
                PpFinal::UnpackFailure => "unpack",
                PpFinal::ScriptFailure => "script",
                PpFinal::HealthFailure => "health",
                PpFinal::FetchFailure => "fetch",
                PpFinal::Success => unreachable!(),
            };
            fin.params
                .push(("Failure:Stage".into(), failure_stage.into()));
        }
        // Durable deobfuscation record: plain (non-`*`) params survive
        // into history and the compat `Parameters` array.
        if !deobfuscated.is_empty() {
            fin.params
                .push(("Deobfuscate:Count".into(), deobfuscated.len().to_string()));
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
            fin.params.push(("Deobfuscate:Files".into(), list));
        }
        fin.params.extend(script_receipts);
        fin.status = if outcome == PpFinal::Success {
            JobStatus::Completed
        } else {
            JobStatus::Failed
        };
        // Plain param: it survives into history and the compat
        // `Parameters` array, so the row itself answers "where did the
        // files go" — the question every one of these corpses raised.
        if let Some(d) = disposition.as_ref() {
            fin.params.push(("Failure:Files".into(), d.note.clone()));
        }
        let health = Health::calc(&fin.totals).0;
        // A row must not point at a directory that is no longer there:
        // deleted means no final dir, parked means the parked one.
        let mut final_dir = match disposition.as_ref() {
            None => Some(final_dir),
            Some(d) => d
                .files_at
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned()),
        };
        // Curator receives a separately journaled selected-payload tree only
        // after scripts have finished mutating the Runner-owned source.
        if job.lifecycle_eligible()
            && disposition.is_none()
            && (outcome == PpFinal::Success || has_durable_salvage)
        {
            let inventory = engine.artifacts();
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
                let mut diagnostics = assess_file_inputs(&job, &source, &verified_payload_paths)?;
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
                );
                if selected_files.is_empty() {
                    return Err(PostError::Subprocess(
                        "no verified media, native book, or paired sidecar is eligible for publication; source retained".into(),
                    ));
                }
                let configured_root = rule
                    .as_ref()
                    .and_then(|r| r.dest_dir.as_ref())
                    .or(cfg.completed_dir.as_ref());
                let target_root = configured_root
                    .cloned()
                    .unwrap_or_else(|| source.path.parent().unwrap_or(dest_dir).to_path_buf());
                let target = target_root.join(format!("{sanitized}.runner-{}", source.generation));
                stages.enter(PostStage::Move).await;
                if !(ctx.commit_ok)() {
                    return Err(PostError::Subprocess(
                        "pp lease lost before selected publication".into(),
                    ));
                }
                if job.delivery_lifecycle {
                    let Some(mut marker) = engine
                        .export_job(job_id)
                        .await
                        .map_err(|error| PostError::Subprocess(error.to_string()))?
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
                    if !engine
                        .import_job_if_present(marker)
                        .await
                        .map_err(|error| PostError::Subprocess(error.to_string()))?
                    {
                        return Err(PostError::Subprocess(
                            "publication outcome was not committed".into(),
                        ));
                    }
                }
                let inventory_for_publish = inventory.clone();
                let target_for_publish = target.clone();
                let delivery_eligible = job.delivery_lifecycle;
                let published = crate::attempt::current()
                    .expect("processing control")
                    .blocking(move |control| {
                        let publish = if delivery_eligible {
                            inventory_for_publish.publish_selected_for_delivery_checked(
                                job_id.0,
                                &target_for_publish,
                                selected_files,
                                file_outcomes,
                                &|| control.checkpoint(),
                            )
                        } else {
                            inventory_for_publish.publish_selected_checked(
                                job_id.0,
                                &target_for_publish,
                                selected_files,
                                file_outcomes,
                                &|| control.checkpoint(),
                            )
                        };
                        publish.map_err(PostError::from)
                    })
                    .await?;
                final_dir = Some(published.published_path.to_string_lossy().into_owned());
            }
        }
        if let Some(source) = crate::attempt::current().and_then(|control| control.source.clone()) {
            fin.params
                .retain(|(key, _)| key != "Artifact:Id" && key != "Artifact:Generation");
            fin.params.push(("Artifact:Id".into(), source.artifact));
            fin.params
                .push(("Artifact:Generation".into(), source.generation));
        }
        if !ctx.publish_history {
            if let Some(path) = &final_dir {
                fin.params.push(("*Cluster:final-dir".into(), path.clone()));
            }
            if !engine
                .import_worker_job_if_present(fin)
                .await
                .map_err(|e| PostError::Subprocess(format!("remote terminal stamp: {e}")))?
            {
                return Err(PostError::Subprocess(
                    "remote terminal stamp was not committed".into(),
                ));
            }
            stages.close();
            return Ok(outcome);
        }
        if let Some(path) = &final_dir {
            let path = Path::new(path);
            let state = if outcome == PpFinal::Success || has_durable_salvage {
                "completed"
            } else if disposition
                .as_ref()
                .is_some_and(|d| d.note.starts_with("parked at "))
            {
                "parked_failed"
            } else {
                "retained"
            };
            // Script-selected or legacy moved output is visible, but never
            // inherits deletion authority solely from a returned path.
            let inventory = engine.artifacts();
            if inventory
                .for_job(job_id.0)
                .ok()
                .flatten()
                .is_none_or(|a| a.path != path)
            {
                // A script-selected external output does not erase custody
                // of the completed owned processing source or its scratch.
                if let Some(control) = crate::attempt::current() {
                    if let Some(source) = &control.source {
                        let original = inventory
                            .get(&source.artifact)
                            .map_err(|e| PostError::Subprocess(e.to_string()))?;
                        if original.generation == source.generation
                            && original.job == Some(job_id.0)
                            && matches!(original.state.as_str(), "active" | "transitioning")
                        {
                            inventory
                                .finish(job_id.0, &original.path, &original.root, "retained")
                                .map_err(|e| PostError::Subprocess(e.to_string()))?;
                        }
                    }
                }
                inventory
                    .register_legacy_active(job_id.0, path.parent().unwrap_or(dest_dir), path)
                    .map_err(|e| {
                        PostError::Subprocess(format!("external result inventory: {e}"))
                    })?;
            }
            let artifact = inventory
                .finish(job_id.0, path, path.parent().unwrap_or(dest_dir), state)
                .map_err(PostError::from)?;
            fin.params.retain(|(k, _)| k != "Artifact:Id");
            fin.params.push(("Artifact:Id".into(), artifact.id.clone()));
            if (outcome == PpFinal::Success || has_durable_salvage)
                && job.delivery_lifecycle
                && artifact.owned
                && artifact.path == path
            {
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
                let delivery_inventory = inventory.clone();
                let delivery = crate::attempt::current()
                    .expect("processing control")
                    .blocking(move |_| {
                        delivery_inventory
                            .create_delivery_with_outcome_and_keep(
                                job_id.0,
                                true,
                                request_token.as_deref(),
                                &terminal_outcome,
                                failure_stage.as_deref(),
                                failure_cause.as_deref(),
                                outcome != PpFinal::Success && job_keeps_failed_source(&job),
                            )
                            .map_err(PostError::from)
                    })
                    .await?;
                fin.params
                    .push(("Delivery:Id".into(), delivery.delivery_id.clone()));
                fin.params
                    .push(("Delivery:Custody".into(), "curator".into()));
                fin.params.push((
                    "Delivery:SourceArtifactId".into(),
                    delivery.source_artifact_id.clone(),
                ));
                fin.params.push((
                    "Delivery:SourceGeneration".into(),
                    delivery.source_generation.clone(),
                ));
                fin.params
                    .push(("Delivery:Path".into(), delivery.publication_path.clone()));
                fin.params.push((
                    "Delivery:Generation".into(),
                    delivery.publication_generation,
                ));
                fin.params
                    .push(("Delivery:ManifestDigest".into(), delivery.manifest_digest));
                let cleanup_id = delivery.delivery_id.clone();
                let cleanup_inventory = inventory.clone();
                let cleanup = crate::attempt::current()
                    .expect("processing control")
                    .blocking(move |_| {
                        cleanup_inventory
                            .retire_delivery_source(&cleanup_id)
                            .map_err(PostError::from)
                    })
                    .await;
                if let Err(error) = cleanup {
                    tracing::warn!(job = job_id.0, %error,
                        "delivery ready; source cleanup remains pending");
                }
                let cleanup_status = inventory
                    .delivery(&delivery.delivery_id)
                    .map(|current| current.source_cleanup.status)
                    .unwrap_or_else(|_| "retry".into());
                fin.params
                    .push(("Delivery:SourceCleanup".into(), cleanup_status));
            }
        }
        if !fin.params.iter().any(|(k, _)| k == "Artifact:Id") {
            if let Ok(Some(a)) = engine.artifacts().for_job(job_id.0) {
                fin.params.push(("Artifact:Id".into(), a.id));
            }
        }
        let entry = HistoryEntry {
            job: job_id,
            name: fin.name.clone(),
            category: fin.category.clone(),
            final_dir: final_dir.clone(),
            status: outcome.as_str().into(),
            size: fin.totals.size,
            health,
            params: terminal_params(&fin),
            dupe_key: fin.dupe.key.clone(),
            dupe_score: fin.dupe.score,
            completed_at_unix: now(),
            hidden: false,
            first_seen_at_unix: None,
            last_seen_at_unix: None,
            seen_count: 0,
            removed_at_unix: None,
            picked_up_by: None,
            // The job as it was, kept with the job: which files, how many
            // articles, where it came from, who asked. Those questions are
            // asked AFTER it leaves the queue, which is the one moment the
            // old summary stopped being able to answer them.
            record: Some(nzbd_state::JobRecord::from_job(&fin)),
            stages: fin.stages.clone(),
            seq: 0,
        };
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
        // History first, stamp second: a crash in between re-runs PP (the
        // stages are idempotent) — the reverse would lose the entry forever.
        let h = history.clone();
        let (history_seq, _) = tokio::task::spawn_blocking(move || h.record_seq_durable(&entry))
            .await
            .map_err(|e| PostError::Subprocess(format!("history worker: {e}")))?
            .map_err(|e| PostError::Subprocess(format!("durable history: {e}")))?;
        let completion = crate::attempt::current()
            .and_then(|control| control.source.clone())
            .map(|source| nzbd_state::artifacts::StagedCompletion {
                source,
                outcome: outcome.as_str().into(),
                proof: nzbd_state::artifacts::FinalizationProof::Local {
                    history_seq: history_seq as u64,
                },
            });
        if let Some(completion) = &completion {
            let staged = completion.clone();
            let inventory = engine.artifacts();
            crate::attempt::current()
                .expect("processing control")
                .blocking(move |_| {
                    inventory
                        .stage_workspace_finalization(
                            &staged.source,
                            &staged.outcome,
                            &staged.proof,
                        )
                        .map_err(|e| PostError::Subprocess(e.to_string()))
                })
                .await?;
        }
        fin.params.retain(|(key, _)| key != "*PP:history-seq");
        fin.params
            .push(("*PP:history-seq".into(), history_seq.to_string()));
        if !engine
            .import_job_if_present(fin.clone())
            .await
            .map_err(|e| PostError::Subprocess(format!("durable terminal stamp: {e}")))?
        {
            return Err(PostError::Subprocess(
                "terminal stamp was not durably committed".into(),
            ));
        }
        if let Some(completion) = completion {
            let stamped = completion.clone();
            let inventory = engine.artifacts();
            let control = crate::attempt::current().expect("processing control");
            control
                .blocking(move |_| {
                    inventory
                        .confirm_stamped_finalization(&stamped)
                        .map_err(|e| PostError::Subprocess(e.to_string()))
                })
                .await?;
            *control.completion.lock().unwrap() = Some(crate::attempt::Completion {
                source: completion.source,
                outcome: completion.outcome,
                proof: completion.proof,
            });
        }
        // Only now — after the history row is durably written — does the
        // completion become public. A consumer may react to this event by
        // reading `?since_seq=history_seq-1` and is guaranteed to find the
        // row; announcing first would hand every consumer an invisible
        // race against our own write.
        stages.close();
        engine.emit(Event::JobPpFinished {
            job: job_id,
            name: fin.name.clone(),
            category: fin.category.clone(),
            pp_status: outcome.as_str().into(),
            final_dir,
            size_bytes: fin.totals.size,
            health,
            params: terminal_params(&fin),
            history_seq,
        });
    }
    Ok(outcome)
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
            (payload_kind(&extension) && verified_payload_paths.contains(&entry.path)).then(|| {
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
        let kind = payload_kind(&extension);
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
            if paired || folder_art {
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

fn assess_file_inputs(
    job: &Job,
    source: &Artifact,
    verified_paths: &std::collections::HashSet<String>,
) -> Result<std::collections::HashMap<String, FileAssessment>, PostError> {
    let mut results = bounded_file_diagnostics(&source.path, &source.files, verified_paths)?;
    for (path, reason) in par_repairable_paths(&source.path, &source.files)? {
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
    Ok(results)
}

/// Count the actual damaged slices of each PAR set. A recovery volume's
/// presence alone is not repair evidence: the distinct block count must
/// cover every missing or corrupt slice in the same set.
fn par_repairable_paths(
    root: &Path,
    entries: &[FileEntry],
) -> Result<std::collections::HashMap<String, String>, PostError> {
    Ok(par_repairable_evidence(root, entries)?.0)
}

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
            let entry = entries
                .iter()
                .find(|entry| !entry.identity.directory && entry.path == source_relative);
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
            Some("EBML/Matroska signature; rename to .mkv after confirming the file, then Retry publication")
        } else if prefix.len() >= 8 && &prefix[4..8] == b"ftyp" {
            Some("ISO base media signature; rename to a supported video/audio extension after confirmation, then Retry publication")
        } else if prefix.starts_with(b"ID3")
            || prefix.starts_with(b"fLaC")
            || prefix.starts_with(b"OggS")
        {
            Some("recognized audio container signature; rename to a supported audio extension after confirmation, then Retry publication")
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
            (None, _) => FileAssessment::NoSignature,
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
    mut checkpoint: impl FnMut() -> std::io::Result<()>,
) -> std::result::Result<std::collections::HashSet<String>, PostError> {
    let mut verified = std::collections::HashSet::new();
    let repair_markers = job
        .params
        .iter()
        .filter(|(key, _)| key.starts_with("*File:repair:"))
        .map(|(key, _)| key.as_str())
        .collect::<std::collections::HashSet<_>>();
    for file in &job.files {
        if !file.finalized
            || file.segments.is_empty()
            || !file
                .segments
                .iter()
                .all(|segment| matches!(segment.state, nzbd_types::SegmentState::Done { .. }))
            || repair_markers
                .iter()
                .any(|marker| marker.ends_with(&file.id.0.to_string()))
        {
            continue;
        }
        let Some(expected_crc) = file.crc32 else {
            continue;
        };
        let Ok(relative) = crate::namespace::relative(&file.filename) else {
            continue;
        };
        let relative = relative.to_string_lossy().into_owned();
        let actual_relative = restored_paths
            .get(&relative)
            .map_or(relative.as_str(), String::as_str);
        let Some(entry) = source
            .files
            .iter()
            .find(|entry| !entry.identity.directory && entry.path == actual_relative)
        else {
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
fn payload_classification(extension: &str) -> &'static str {
    if matches!(
        extension,
        "mkv" | "mp4" | "avi" | "m4v" | "mov" | "wmv" | "ts" | "webm"
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
        ["sample", "proof"].iter().any(|word| {
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
fn is_disposable_name(name: &str) -> bool {
    name.starts_with("sample.")
        || name.starts_with("proof.")
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
    let mut session = control
        .blocking(move |_| {
            crate::repair_workspace::RepairSession::new(inventory, job_id.0, set, owned_partials)
        })
        .await?;
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
#[derive(Debug, Clone)]
pub struct Disposition {
    pub note: String,
    pub files_at: Option<PathBuf>,
}

/// Apply a terminal failure disposition through the ownership journal.
/// Missing paths, failed copies and incomplete deletion retain a visible
/// record; only a durable deletion result can report that no files remain.
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
        };
    }
    if cfg.failure_action == FailureAction::Delete {
        let result = terminal_worker(inventory, job, move |inventory, guard, checkpoint| {
            checkpoint()?;
            inventory
                .delete_failed_payload(guard, job.0, checkpoint)
                .map_err(|e| PostError::Subprocess(e.to_string()))
        })
        .await;
        return match result {
            Ok(op) if op.state == "succeeded" => Disposition {
                note: "deleted".into(),
                files_at: None,
            },
            other => Disposition {
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
                .map_err(|e| PostError::Subprocess(e.to_string()))
        })
        .await;
        return match moved {
            Ok(result) => Disposition {
                note: format!("parked at {}", result.published_path.display()),
                files_at: Some(result.published_path),
            },
            other => Disposition {
                note: format!("checked move pending: {other:?}"),
                files_at: Some(dir.into()),
            },
        };
    }
    Disposition {
        note: "kept".into(),
        files_at: Some(dir.into()),
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

    fn assessment_outcome(outcome: &str, reason: &str) -> DeliveryFileOutcome {
        DeliveryFileOutcome {
            source_path: "episode.mkv".into(),
            publication_path: None,
            classification: "payload".into(),
            outcome: outcome.into(),
            reason: Some(reason.into()),
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        }
    }

    #[test]
    fn absent_integrity_proof_is_unknown_not_cleanup_authority() {
        let outcome = assessment_outcome("unverified_review", "any display text");
        let assessment = failed_source_assessment(&[outcome]);
        assert_eq!(assessment.disposition, "unknown");
    }

    #[test]
    fn partial_publication_retains_unverified_media_sibling_during_source_cleanup() {
        let temp = tempfile::tempdir().unwrap();
        let downloads = temp.path().join("downloads");
        std::fs::create_dir(&downloads).unwrap();
        let source_path = downloads.join("partial-release");
        let inventory = nzbd_state::artifacts::Inventory::open(&temp.path().join("state")).unwrap();
        inventory
            .allocate_for_delivery(94, &downloads, &source_path, true, true)
            .unwrap();
        std::fs::write(source_path.join("episode-01.mkv"), b"verified episode").unwrap();
        std::fs::write(source_path.join("episode-02.mkv"), b"unverified sibling").unwrap();
        inventory
            .finish(94, &source_path, &downloads, "active")
            .unwrap();
        let source = inventory.for_job(94).unwrap().unwrap();
        let verified = ["episode-01.mkv".to_string()].into_iter().collect();
        let (selected, outcomes) = select_delivery_payload(
            &source.files,
            &Default::default(),
            &verified,
            &Default::default(),
            &Default::default(),
        );
        let unknown = outcomes
            .iter()
            .find(|outcome| outcome.source_path == "episode-02.mkv")
            .unwrap();
        assert_eq!(unknown.outcome, "unverified_review");
        assert_eq!(failed_source_assessment(&outcomes).disposition, "unknown");

        let completed = temp.path().join("completed");
        std::fs::create_dir(&completed).unwrap();
        let target = completed.join("partial-release-generation");
        inventory
            .publish_selected_for_delivery_checked(94, &target, selected, outcomes, &|| Ok(()))
            .unwrap();
        let delivery = inventory
            .create_delivery(94, true, Some("partial-unknown-integrity"))
            .unwrap();
        let cleanup = inventory
            .retire_delivery_source(&delivery.delivery_id)
            .unwrap();

        assert_eq!(cleanup.source_cleanup.status, "blocked");
        assert!(!source_path.join("episode-01.mkv").exists());
        assert!(source_path.join("episode-02.mkv").exists());
        assert!(target.join("episode-01.mkv").exists());
        assert!(!target.join("episode-02.mkv").exists());
        assert_eq!(
            cleanup
                .file_outcomes
                .iter()
                .find(|outcome| outcome.source_path == "episode-02.mkv")
                .unwrap()
                .source_cleanup_status,
            "retained"
        );
    }

    #[test]
    fn positive_terminal_corruption_can_authorize_cleanup() {
        let outcome = assessment_outcome(
            "excluded_source_cleanup",
            "bounded assessment proved the finalized source entry incomplete and unrepaired",
        );
        let assessment = failed_source_assessment(&[outcome]);
        assert_eq!(assessment.disposition, "unusable");
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
        );
        assert_eq!(selected.len(), 1);
        let failed = outcomes
            .iter()
            .find(|outcome| outcome.source_path == "episode-02.rar")
            .unwrap();
        assert_eq!(failed.outcome, "excluded_source_cleanup");
        assert_eq!(
            failed.reason.as_deref(),
            Some("archive extraction failed after configured tools and available repair; no verified output remains")
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
            payload_entry("unsupported.mpg"),
            payload_entry("unverified.mp4"),
        ];
        let verified = [
            "The Sample Film.mkv".to_string(),
            "film.sample.mkv".to_string(),
            "Sample/film.mkv".to_string(),
            "book.cbz".to_string(),
            "book.djvu".to_string(),
            "unsupported.mpg".to_string(),
        ]
        .into_iter()
        .collect();
        let (selected, outcomes) = select_delivery_payload(
            &entries,
            &Default::default(),
            &verified,
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
        assert_eq!(outcome("unsupported.mpg").outcome, "unsupported_review");
        assert_eq!(outcome("unverified.mp4").outcome, "unverified_review");
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
        std::fs::write(source.join("episode-02.rar"), b"Rar!\x1a\x07partial").unwrap();
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
        let mut request = job(
            vec![good_file, bad_file],
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
                .find(|row| row.source_path == "episode-02.rar")
                .unwrap()
                .outcome,
            "excluded_source_cleanup"
        );
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
        engine.import_job(request, false, false).await.unwrap();
        let source = inventory.for_job(41).unwrap().unwrap();
        std::fs::write(source.path.join("episode-01.mkv"), good_bytes).unwrap();
        std::fs::write(source.path.join("episode-02.mkv"), b"partial").unwrap();
        inventory
            .finish(41, &source.path, &downloads, "active")
            .unwrap();

        let cfg = PostConfig {
            completed_dir: Some(completed),
            unpack: false,
            cleanup: false,
            deobfuscate_final: false,
            ..PostConfig::default()
        };
        handle_failed_job(
            &engine,
            &cfg,
            &history,
            &downloads,
            &None,
            JobId(41),
            "Some.Release".into(),
            0,
            &mut HashMap::new(),
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
    async fn retry_now_releases_a_due_failed_source_and_runs_manager_salvage() {
        let temp = tempfile::tempdir().unwrap();
        let history = Arc::new(
            HistoryDb::open(&temp.path().join("history.sqlite"), Some(temp.path())).unwrap(),
        );
        let downloads = temp.path().join("downloads");
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
        let bytes = b"complete media retained through a scheduled failure hold";
        let mut media = file(1, "retry-episode.mkv", Some(crc32fast::hash(bytes)));
        media.segments = vec![Segment {
            message_id: "retry-media".into(),
            number: 1,
            size: bytes.len() as u32,
            state: SegmentState::Done {
                offset: 0,
                len: bytes.len() as u32,
                crc: crc32fast::hash(bytes),
            },
        }];
        let mut request = job(
            vec![media],
            vec![
                ("monarr-transfer".into(), "retry-now-health".into()),
                ("monarr-delivery".into(), "1".into()),
                (PP_FAILURE_DISPOSITION_ATTEMPTS_PARAM.into(), "1".into()),
                (
                    PP_FAILED_CLEANUP_NEXT_RETRY_PARAM.into(),
                    now().saturating_add(3600).to_string(),
                ),
                (
                    PP_FAILED_CLEANUP_BLOCKED_SINCE_PARAM.into(),
                    now().to_string(),
                ),
                (
                    PP_FAILED_CLEANUP_LAST_ERROR_PARAM.into(),
                    "mount unavailable".into(),
                ),
            ],
        );
        request.delivery_lifecycle = true;
        request.status = JobStatus::Failed;
        let source_path = downloads.join(nzbd_engine::queue::job_dir_name(&request));
        let inventory = engine.artifacts();
        inventory
            .allocate_for_delivery(41, &downloads, &source_path, true, true)
            .unwrap();
        engine.import_job(request, false, false).await.unwrap();
        let source = inventory.for_job(41).unwrap().unwrap();
        std::fs::write(source.path.join("retry-episode.mkv"), bytes).unwrap();
        inventory
            .finish(41, &source.path, &downloads, "active")
            .unwrap();
        let offline = temp.path().join("downloads-offline");
        std::fs::rename(&downloads, &offline).unwrap();

        let cfg = PostConfig {
            completed_dir: Some(completed),
            unpack: false,
            cleanup: false,
            deobfuscate_final: false,
            ..PostConfig::default()
        };
        let cancel = CancellationToken::new();
        let tracker = tokio_util::task::TaskTracker::new();
        let manager = spawn_post_manager(
            engine.clone(),
            cfg,
            history.clone(),
            downloads.clone(),
            None,
            cancel.clone(),
            &tracker,
        );
        // Let the startup scan observe the future retry deadline and hold.
        tokio::time::sleep(Duration::from_millis(50)).await;
        manager.retry_now(JobId(41)).await.unwrap();

        let held = engine.export_job(JobId(41)).await.unwrap().unwrap();
        assert_eq!(
            param_i64(&held.params, PP_FAILURE_DISPOSITION_ATTEMPTS_PARAM),
            Some(2),
            "an unavailable source is persisted as a recoverable retry"
        );
        assert!(offline
            .join(nzbd_engine::queue::job_dir_name(&held))
            .join("retry-episode.mkv")
            .exists());
        std::fs::rename(&offline, &downloads).unwrap();
        manager.retry_now(JobId(41)).await.unwrap();

        let delivery = inventory.delivery_for_job(41).unwrap().unwrap();
        assert_eq!(delivery.terminal_outcome, "FAILURE/HEALTH");
        assert_eq!(delivery.request_token.as_deref(), Some("retry-now-health"));
        assert!(PathBuf::from(&delivery.publication_path)
            .join("retry-episode.mkv")
            .exists());
        assert_eq!(
            history.get(JobId(41)).unwrap().unwrap().status,
            "FAILURE/HEALTH"
        );

        cancel.cancel();
        tracker.close();
        tracker.wait().await;
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn failed_health_extracts_complete_archive_sibling_before_partial_delivery() {
        if !crate::tools::require_tool("7z") {
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let downloads = temp.path().join("downloads");
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
        let assessed =
            extract_complete_failed_archives(&engine, &PostConfig::default(), &request, &None)
                .await
                .unwrap();
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
        let error = extract_complete_failed_archives(&engine, &cfg, &request, &None)
            .await
            .err()
            .expect("storage failure must enter retry lane");
        assert!(error.contains("requires retry"));
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
        let damaged_original = vec![b'b'; 50_000];
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
        repair_failed_health_par_sets(&engine, &PostConfig::default(), &request, &None)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(source.join("episode-02.mkv")).unwrap(),
            damaged_original
        );
        let salvage = salvage_failed_delivery(
            &inventory,
            &request,
            &temp.path().join("completed"),
            "FAILURE/HEALTH",
            "health",
            "one failed segment covered by PAR",
            &EarlyArchiveAssessment::default(),
        )
        .unwrap()
        .expect("journaled repaired episode should publish");
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
        assert!(std::process::Command::new("par2")
            .args([
                "create",
                "-q",
                "-q",
                "-s8192",
                "-c1",
                "set.par2",
                "episode-01.mkv",
                "episode-02.mkv"
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
        let mut request = job(
            vec![good_file, bad_file],
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
        let failures = repair_failed_health_par_sets(&engine, &cfg, &request, &None)
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
                .find(|outcome| outcome.source_path == "episode-02.mkv")
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
                .find(|outcome| outcome.source_path == "episode-02.mkv")
                .unwrap()
                .outcome,
            "retained_processing"
        );
        let cleanup = inventory
            .retire_delivery_source(&delivery.delivery_id)
            .unwrap();
        assert_ne!(cleanup.source_cleanup.status, "succeeded");
        assert!(source.join("episode-01.mkv").exists());
        assert!(source.join("episode-02.mkv").exists());
        assert!(source.join("set.par2").exists());
        std::fs::remove_dir_all(&salvage.published_path).unwrap();
        let verify = std::process::Command::new("par2")
            .args(["verify", "set.par2"])
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
        let source_path = downloads.join(nzbd_engine::queue::job_dir_name(&job));
        inventory
            .allocate_for_delivery(41, &downloads, &source_path, true, true)
            .unwrap();
        engine.import_job(job.clone(), false, false).await.unwrap();

        let source = inventory.for_job(41).unwrap().unwrap();
        std::fs::write(source.path.join("movie.mkv"), b"payload").unwrap();
        inventory
            .finish(41, &source.path, &source.root, "active")
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
        inventory
            .accept_delivery(
                &delivery.delivery_id,
                &delivery.publication_generation,
                &delivery.manifest_digest,
                "curator-receipt-41",
            )
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

    #[tokio::test]
    async fn process_restart_finishes_committed_extracted_publication_without_rerunning_tools() {
        if !crate::tools::require_tool("7z") {
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let downloads = temp.path().join("downloads");
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
        engine
            .import_job(request.clone(), false, false)
            .await
            .unwrap();
        let source = inventory.for_job(41).unwrap().unwrap().path;
        std::fs::copy(temp.path().join("episode.zip"), source.join("episode.zip")).unwrap();
        inventory.finish(41, &source, &downloads, "active").unwrap();
        extract_complete_failed_archives(&engine, &PostConfig::default(), &request, &None)
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
        assert!(source.join("episode.zip").exists());
        assert!(inventory.delivery_for_job(41).unwrap().is_some());
        assert_eq!(history.get(JobId(41)).unwrap().unwrap().status, "SUCCESS");
    }

    #[tokio::test]
    async fn failed_health_restart_reuses_extracted_selected_publication_before_delivery() {
        if !crate::tools::require_tool("7z") {
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let downloads = temp.path().join("downloads");
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
        engine
            .import_job(request.clone(), false, false)
            .await
            .unwrap();
        let source = inventory.for_job(41).unwrap().unwrap().path;
        std::fs::copy(temp.path().join("episode.zip"), source.join("episode.zip")).unwrap();
        std::fs::write(source.join("incomplete.rar"), b"Rar!\x1a\x07partial").unwrap();
        inventory.finish(41, &source, &downloads, "active").unwrap();
        extract_complete_failed_archives(&engine, &PostConfig::default(), &request, &None)
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
        let mut last_disposition = HashMap::new();
        handle_failed_job(
            &engine,
            &cfg,
            &history,
            &downloads,
            &None,
            JobId(41),
            "failed-archive-pack".into(),
            0,
            &mut last_disposition,
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
        handle_failed_job(
            &engine,
            &cfg,
            &history,
            &downloads,
            &None,
            JobId(41),
            "failed-archive-pack".into(),
            0,
            &mut last_disposition,
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
        let source_path = downloads.join(nzbd_engine::queue::job_dir_name(&job));
        inventory
            .allocate_for_delivery(42, &downloads, &source_path, true, true)
            .unwrap();
        engine.import_job(job, false, false).await.unwrap();

        let source = inventory.for_job(42).unwrap().unwrap();
        std::fs::write(
            source.path.join("good-episode.mkv"),
            b"verified partial payload",
        )
        .unwrap();
        inventory
            .finish(42, &source.path, &source.root, "active")
            .unwrap();
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
        inventory
            .accept_delivery(
                &delivery.delivery_id,
                &delivery.publication_generation,
                &delivery.manifest_digest,
                "curator-receipt-42",
            )
            .unwrap();
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
        assert_eq!(post_retry_delay_seconds(0), 30);
        assert_eq!(post_retry_delay_seconds(1), 60);
        assert_eq!(post_retry_delay_seconds(2), 120);
        assert_eq!(post_retry_delay_seconds(3), 300);
        assert_eq!(post_retry_delay_seconds(4), 300);
        assert_eq!(post_retry_delay_seconds(u8::MAX), 300);
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
        assert_eq!(user_params(&j), vec![("mykey".into(), "myval".into())]);
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
        engine.import_job(job, false, false).await.unwrap();
        let count = temp.path().join("verifications");
        let fake = temp.path().join("par-tool.sh");
        std::fs::write(&fake,format!("#!/bin/sh\necho x >> '{}'\necho 'Repair is not possible.'\necho 'You need 1 more recovery blocks to be able to repair.'\nexit 2\n",count.display())).unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let feeder_engine = engine.clone();
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
                        feeder_engine.import_job(job, false, false).await.unwrap();
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
            process_job_ctx(&engine, &cfg, &history, &root, JobId(401), &ctx),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result, PpFinal::ParFailure);
        tokio::time::timeout(Duration::from_secs(3), feeder)
            .await
            .unwrap()
            .unwrap();
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
}
