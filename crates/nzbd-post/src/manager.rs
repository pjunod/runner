//! The post-processing orchestrator + manager (ARCHITECTURE.md §9).
//!
//! The manager watches the engine for finished downloads and drives each
//! job through the stage graph: par verify (native quick path first) →
//! repair (with delayed-par fetching) → unpack → cleanup → scripts —
//! recording the outcome in history and stamping the job so restarts never
//! re-process. Stage parallelism follows `PostStrategy`.

use crate::rename::{par_rename_owned, rar_rename_owned};
use crate::script::{discover, ScriptHost};
use crate::tools::{detect_archives, Extractors, Par2Tool};
use crate::{par2, DownloadEvidence, PostError, RepairResult, VerifyResult};
use nzbd_engine::{EngineHandle, Event};
use nzbd_state::history::HistoryDb;
use nzbd_state::HistoryEntry;
use nzbd_types::metrics::PpStageStats;
use nzbd_types::{Health, Job, JobId, JobStatus, PostStage};
use std::collections::HashMap;
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
}

impl PpFinal {
    pub fn as_str(&self) -> &'static str {
        match self {
            PpFinal::Success => "SUCCESS",
            PpFinal::ParFailure => "PAR_FAILURE",
            PpFinal::UnpackFailure => "UNPACK_FAILURE",
            PpFinal::ScriptFailure => "SCRIPT_FAILURE",
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
const PP_FAILURE_AT_PARAM: &str = "*PP:failure-at";
const PP_FAILURE_DISPOSITION_ATTEMPTS_PARAM: &str = "*PP:failure-disposition-attempts";
const FAILURE_DISPOSITION_ATTEMPT_LIMIT: u32 = 3;

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
    let fail_status = if fetch_failure {
        "FAILURE/FETCH"
    } else {
        "FAILURE/HEALTH"
    };
    let disposition = match exported.as_ref() {
        Some(j) if !j.files.is_empty() => {
            let dir_name = nzbd_engine::queue::job_dir_name(j);
            let source = dest_dir.join(&dir_name);
            let observed = dispose_failed(
                engine.artifacts(),
                cfg,
                job,
                &source,
                dest_dir,
                &dir_name,
                "local",
            )
            .await;
            match canonical_failed_disposition(cfg, dest_dir, &dir_name, &source) {
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
                        .saturating_add(1)
                        .min(FAILURE_DISPOSITION_ATTEMPT_LIMIT);
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
                    if attempts < FAILURE_DISPOSITION_ATTEMPT_LIMIT {
                        tracing::warn!(
                            job = job.0,
                            attempts,
                            limit = FAILURE_DISPOSITION_ATTEMPT_LIMIT,
                            outcome = %observed.note,
                            "failed-file disposition is incomplete; leaving the queue row retryable"
                        );
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
    };
    // Park may be a cross-filesystem copy. If authority or disk admission
    // changed while it awaited, do not publish a terminal decision. A later
    // authority retry recognizes the already-moved target idempotently.
    if gate.as_ref().is_some_and(|claim| !claim(job)) {
        return;
    }
    let mut fail_params = exported.as_ref().map(terminal_params).unwrap_or_default();
    if let Some(d) = disposition.as_ref() {
        fail_params.push(("Failure:Files".into(), d.note.clone()));
    }
    let mut entry = HistoryEntry {
        job,
        name: name.clone(),
        category: exported.as_ref().and_then(|j| j.category.clone()),
        final_dir: disposition
            .as_ref()
            .and_then(|d| d.files_at.as_ref())
            .map(|p| p.to_string_lossy().into_owned()),
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
    if let Some(path) = entry.final_dir.as_deref() {
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
            Ok(artifact) => entry.params.push(("Artifact:Id".into(), artifact.id)),
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
    if let Some(mut fin) = exported {
        if let Some((_, note)) = entry.params.iter().find(|(key, _)| key == "Failure:Files") {
            fin.params.push(("Failure:Files".into(), note.clone()));
        }
        fin.params.push((PP_DONE_PARAM.into(), fail_status.into()));
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
        // NZBGet parity: finished-and-processed jobs live in history, not
        // the queue. Single-node the manager IS the authority; in cluster
        // mode the leader sweep retires (the gate stays out of hygiene).
        if gate.is_none()
            && j.pp_done
            && matches!(j.status, JobStatus::Completed | JobStatus::Failed)
        {
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
            let outcome = tokio::select! {
                _ = global_cancel.cancelled() => None,
                _ = task_cancel.cancelled() => None,
                r = process_job_from(&engine, &cfg, &history, &dest, job, from) => Some(r),
            };
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
                    // The stage owns the persisted cause, revision and retry policy.
                }
                Some(Err(e)) => {
                    let detail = e.to_string();
                    let cause = if nzbd_engine::is_out_of_space(&detail) {
                        if detail.to_lowercase().contains("quota") {
                            "quota"
                        } else {
                            "capacity"
                        }
                    } else if detail.contains("identity")
                        || detail.contains("collision")
                        || detail.contains("unsafe")
                    {
                        "identity_conflict"
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
        renames = par_rename_owned(&dir, Some((&engine.artifacts(), job_id.0)))?;
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
        for set in par2::load_sets(&dir)? {
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
        rar_rename_owned(&dir, Some((&engine.artifacts(), job_id.0)))?;
    }

    // ---- UNPACK stage ------------------------------------------------------
    let mut unpack_ok = true;
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
                let mut workspace = engine
                    .artifacts()
                    .workspace(job_id.0, "extract", &token)
                    .map_err(|e| PostError::Subprocess(e.to_string()))?;
                if engine
                    .artifacts()
                    .extraction_published(&workspace)
                    .map_err(|e| PostError::Subprocess(e.to_string()))?
                {
                    unpacked_any = true;
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
                            if repair_loop(engine, cfg, &par_tool, &mut stages, job_id, &main)
                                .await?
                            {
                                par_did_repair = true;
                                stages.enter(PostStage::Unpack).await;
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
                    commit_staging(&staging, &dir)?;
                    engine
                        .artifacts()
                        .finish_extraction(&workspace, &staging)
                        .map_err(|e| PostError::Subprocess(e.to_string()))?;
                    unpacked_any = true;
                } else {
                    tracing::warn!(
                        job = job_id.0,
                        archive = %archive.display(),
                        password_error = r.password_error,
                        "unpack failed"
                    );
                    unpack_ok = false;
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
    if from.includes(RestartPoint::Move) && par_ok && unpack_ok {
        if let Some(target_root) = rule
            .as_ref()
            .and_then(|r| r.dest_dir.as_ref())
            .or(cfg.completed_dir.as_ref())
        {
            let target = target_root.join(&sanitized);
            if target != dir && dir.exists() {
                let _ = std::fs::remove_dir_all(&staging); // never move the lease's scratch
                stages.enter(PostStage::Move).await;
                if !(ctx.commit_ok)() {
                    return Err(PostError::Subprocess("pp lease lost before move".into()));
                }
                let inventory = engine.artifacts();
                let to = target.clone();
                let moved = tokio::task::spawn_blocking(move || inventory.relocate(job_id.0, &to))
                    .await
                    .unwrap_or_else(|e| Err(nzbd_state::artifacts::Error::Conflict(e.to_string())));
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
    // ---- disposition of a terminal failure ---------------------------------
    // A failed job's bytes are known-bad, and until this existed nothing
    // ever removed them: the directory stayed in the very tree the
    // importer watches, one ~90 GB corpse per failed grab, until the
    // volume filled (docs/REGRAB_LOOP_PLAN.md D2). Before the history row
    // is written, so the row can say where the files went.
    let disposition = if outcome == PpFinal::Success {
        None
    } else {
        Some(
            dispose_failed(
                engine.artifacts(),
                cfg,
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
        fin.params
            .push((PP_DONE_PARAM.into(), outcome.as_str().into()));
        if outcome == PpFinal::ParFailure {
            fin.params.push((
                "Failure:Reason".into(),
                "Missing data could not be reconstructed from available PAR2 recovery files".into(),
            ));
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
        let final_dir = match disposition.as_ref() {
            None => Some(final_dir),
            Some(d) => d
                .files_at
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned()),
        };
        if !ctx.publish_history {
            if let Some(path) = &final_dir {
                fin.params.push(("*Cluster:final-dir".into(), path.clone()));
            }
            let _ = engine.import_job(fin, false, false).await;
            stages.close();
            return Ok(outcome);
        }
        if let Some(path) = &final_dir {
            let path = Path::new(path);
            let state = if outcome == PpFinal::Success {
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
                inventory
                    .register_legacy_active(job_id.0, path.parent().unwrap_or(dest_dir), path)
                    .map_err(|e| {
                        PostError::Subprocess(format!("external result inventory: {e}"))
                    })?;
            }
            let artifact = inventory
                .finish(job_id.0, path, path.parent().unwrap_or(dest_dir), state)
                .map_err(|e| PostError::Subprocess(format!("file lifecycle finalization: {e}")))?;
            fin.params.retain(|(k, _)| k != "Artifact:Id");
            fin.params.push(("Artifact:Id".into(), artifact.id));
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
        let history_seq = tokio::task::spawn_blocking(move || h.record_seq(&entry))
            .await
            .ok()
            .and_then(|r| r.ok())
            .unwrap_or(0);
        let _ = engine.import_job(fin.clone(), false, false).await;
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
async fn repair_isolated_loop(
    engine: &EngineHandle,
    cfg: &PostConfig,
    par: &Par2Tool,
    stages: &mut Stages<'_>,
    job_id: JobId,
    mut set: par2::Par2Set,
    partials: &[(PathBuf, String)],
    restored: &mut std::collections::HashSet<PathBuf>,
) -> Result<bool, PostError> {
    for _ in 0..8 {
        stages.enter(PostStage::ParRepair).await;
        match crate::repair_workspace::repair(
            &engine.artifacts(),
            job_id.0,
            &set,
            par,
            partials,
            restored,
        )
        .await?
        {
            VerifyResult::Intact => return Ok(true),
            VerifyResult::NeedMoreBlocks { blocks_needed } => {
                let freed = engine
                    .unpause_par_blocks(job_id, blocks_needed, Some(set.slice_size))
                    .await
                    .unwrap_or(0);
                if freed == 0 || !wait_par_files(engine, job_id, cfg.par_fetch_timeout).await {
                    return Ok(false);
                }
                let refreshed = par2::load_sets(&set.root)?
                    .into_iter()
                    .find(|candidate| candidate.set_id == set.set_id);
                let Some(refreshed) = refreshed else {
                    return Ok(false);
                };
                set = refreshed;
            }
            _ => return Ok(false),
        }
    }
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
    for round in 0..8 {
        match par.verify_full(main).await? {
            VerifyResult::Intact => return Ok(true),
            VerifyResult::Repairable { .. } => {
                stages.enter(PostStage::ParRepair).await;
                return Ok(par.repair(main).await? == RepairResult::Repaired);
            }
            VerifyResult::NeedMoreBlocks { blocks_needed } => {
                let block_size = par2_block_size(main).await;
                tracing::info!(
                    job = job_id.0,
                    blocks_needed,
                    round,
                    block_size,
                    "requesting delayed par blocks"
                );
                let freed = engine
                    .unpause_par_blocks(job_id, blocks_needed, block_size)
                    .await
                    .unwrap_or(0);
                if freed == 0 {
                    // Not "nothing left to fetch" until it says so out
                    // loud: this branch was silent, and a repair that
                    // never started reads exactly like a repair that ran
                    // and failed.
                    tracing::warn!(
                        job = job_id.0,
                        blocks_needed,
                        round,
                        block_size,
                        "repair needs recovery blocks and none could be unpaused — giving up; \
                         the engine log names which paused par2 files it could not price"
                    );
                    return Ok(false);
                }
                if !wait_par_files(engine, job_id, cfg.par_fetch_timeout).await {
                    return Ok(false);
                }
            }
            VerifyResult::Unrepairable => return Ok(false),
        }
    }
    Ok(false)
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
        let from = dir.to_path_buf();
        let root = dir.parent().unwrap_or(dest).to_path_buf();
        let result = tokio::task::spawn_blocking(move || {
            let artifact = inventory.finish(job.0, &from, &root, "retained")?;
            let key = format!("failed-delete-{}", artifact.id);
            let op = match inventory.operation(&key) {
                Ok(op) => op,
                Err(nzbd_state::artifacts::Error::NotFound) => {
                    inventory.request_delete(&artifact.id, artifact.revision, &key, 0)?
                }
                Err(e) => return Err(e),
            };
            inventory.execute_delete(&op.id)
        })
        .await;
        return match result {
            Ok(Ok(op)) if op.state == "succeeded" => Disposition {
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
        let target = cfg
            .failed_dir
            .clone()
            .unwrap_or_else(|| dest.join(".failed"))
            .join(name);
        let to = target.clone();
        return match tokio::task::spawn_blocking(move || inventory.relocate(job.0, &to)).await {
            Ok(Ok(result)) => Disposition {
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

/// The recovery-set slice size from the job's main par2 index, if it can
/// be read.
///
/// This is what prices a hash-named recovery volume: no `.volXX+NN` marker
/// exists on an obfuscated post, but `bytes / slice_size` is a good enough
/// block count to pick with. The index is small (tens of KiB) and the read
/// is capped and off the async threads regardless.
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
            match nzbd_state::fileops::copy_publish(source, target).map_err(|e| match e {
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
    use nzbd_types::{DupeInfo, FileEntry, FileId, JobKind, JobTotals};
    use std::os::unix::fs::PermissionsExt;

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
        let ManagerCommand::Restart { job, from, reply } = rx.recv().await.unwrap();
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
}
