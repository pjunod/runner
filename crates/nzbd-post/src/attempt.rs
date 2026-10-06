//! One attempt owns cancellation, worker custody and bounded transient progress.
use crate::{manager::PpCtx, PostError};
use nzbd_engine::{EngineHandle, RepairPhase, RepairProgress};
use nzbd_state::artifacts::{AttemptUse, FinalizationProof, SourceGeneration};
use nzbd_types::JobId;
use std::cell::RefCell;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use tokio::sync::watch;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

tokio::task_local! { pub(crate) static CURRENT: Arc<AttemptControl>; }
thread_local! { static BLOCKING: RefCell<Option<Arc<AttemptControl>>> = const { RefCell::new(None) }; }
static NEXT_ATTEMPT: AtomicU64 = AtomicU64::new(1);

pub(crate) struct Completion {
    pub source: SourceGeneration,
    pub outcome: String,
    pub proof: FinalizationProof,
}
#[cfg(test)]
pub(crate) type RepairObserver = Arc<dyn Fn(u64, u32, u32) + Send + Sync>;
pub(crate) struct AttemptControl {
    #[cfg(test)]
    pub repair_observer: Option<RepairObserver>,
    pub cancel: CancellationToken,
    pub workers: TaskTracker,
    pub use_guard: Mutex<Option<AttemptUse>>,
    pub source: Option<SourceGeneration>,
    pub completion: Mutex<Option<Completion>>,
    pub par_cache: Mutex<crate::par2::DiscoveryCache>,
    engine: EngineHandle,
    job: JobId,
    authority: Arc<dyn Fn() -> bool + Send + Sync>,
    progress: Mutex<RepairProgress>,
    latest: watch::Sender<RepairProgress>,
}
impl Drop for AttemptControl {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.engine
            .close_repair_attempt(self.job, self.progress.lock().unwrap().attempt_id.clone());
    }
}
impl AttemptControl {
    pub async fn new(
        engine: &EngineHandle,
        ctx: &PpCtx,
        job: JobId,
    ) -> Result<Arc<Self>, PostError> {
        let inventory = engine.artifacts();
        let cancel = ctx.cancel.child_token();
        let acquire_cancel = cancel.clone();
        let acquire_authority = ctx.commit_ok.clone();
        let guard = ctx
            .workers
            .spawn_blocking(move || {
                if acquire_cancel.is_cancelled() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "processing cancelled",
                    )
                    .into());
                }
                inventory.acquire_attempt_use_checked(job.0, &|| {
                    if acquire_cancel.is_cancelled() || !acquire_authority() {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "processing attempt no longer authorized",
                        ))
                    } else {
                        Ok(())
                    }
                })
            })
            .await
            .map_err(|e| PostError::Subprocess(e.to_string()))?
            .map_err(|e| PostError::Subprocess(e.to_string()))?;
        let progress = RepairProgress {
            attempt_id: format!(
                "{}:{}",
                ctx.tag,
                NEXT_ATTEMPT.fetch_add(1, Ordering::Relaxed)
            ),
            phase: RepairPhase::Matching,
            files_done: 0,
            files_total: 0,
            bytes_scanned: 0,
            round: 0,
            recovery_blocks_available: 0,
            additional_blocks_needed: None,
            last_progress_at: 0,
        };
        let _ = engine
            .register_repair_attempt(job, progress.attempt_id.clone())
            .await;
        let (latest, _) = watch::channel(progress.clone());
        Ok(Arc::new(Self {
            #[cfg(test)]
            repair_observer: ctx.repair_observer.clone(),
            cancel,
            workers: ctx.workers.clone(),
            source: guard.as_ref().map(|guard| guard.source().clone()),
            use_guard: Mutex::new(guard),
            completion: Mutex::new(None),
            par_cache: Mutex::new(Default::default()),
            engine: engine.clone(),
            job,
            authority: ctx.commit_ok.clone(),
            progress: Mutex::new(progress),
            latest,
        }))
    }
    pub async fn finish_filesystem_work(&self) -> Result<(), PostError> {
        self.workers.close();
        self.workers.wait().await;
        self.checkpoint()?;
        Ok(())
    }
    pub fn checkpoint(&self) -> std::io::Result<()> {
        if self.cancel.is_cancelled() || !(self.authority)() {
            Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "processing attempt cancelled or lease lost",
            ))
        } else {
            Ok(())
        }
    }
    pub fn bytes(&self, bytes: u64) {
        let mut progress = self.progress.lock().unwrap();
        progress.bytes_scanned = progress.bytes_scanned.saturating_add(bytes);
        if bytes > 0 {
            progress.last_progress_at = now();
        }
        self.latest.send_replace(progress.clone());
    }
    pub fn matching_total(&self, total: usize) {
        let mut progress = self.progress.lock().unwrap();
        if progress.phase != RepairPhase::Matching {
            return;
        }
        progress.files_total = total.min(u32::MAX as usize) as u32;
        self.latest.send_replace(progress.clone());
    }
    pub fn file_done(&self) {
        let mut progress = self.progress.lock().unwrap();
        progress.files_done = progress.files_done.saturating_add(1);
        progress.last_progress_at = now();
        self.latest.send_replace(progress.clone());
    }
    pub async fn phase(&self, phase: RepairPhase, files_total: usize) {
        self.checkpoint().ok();
        let progress = {
            let mut progress = self.progress.lock().unwrap();
            progress.phase = phase;
            progress.files_done = 0;
            progress.files_total = files_total.min(u32::MAX as usize) as u32;
            self.latest.send_replace(progress.clone());
            progress.clone()
        };
        self.engine.update_repair_progress(self.job, progress).await;
    }
    pub async fn round(&self, round: u32, recovery: u32, needed: Option<u32>) {
        let progress = {
            let mut progress = self.progress.lock().unwrap();
            if recovery > progress.recovery_blocks_available {
                progress.last_progress_at = now();
            }
            progress.round = round;
            progress.recovery_blocks_available = recovery;
            progress.additional_blocks_needed = needed;
            self.latest.send_replace(progress.clone());
            progress.clone()
        };
        self.engine.update_repair_progress(self.job, progress).await;
    }
    pub async fn blocking<T, F>(self: &Arc<Self>, action: F) -> Result<T, PostError>
    where
        T: Send + 'static,
        F: FnOnce(&Arc<Self>) -> Result<T, PostError> + Send + 'static,
    {
        self.checkpoint()?;
        let control = self.clone();
        let use_guard = self.use_guard.lock().unwrap().clone();
        let mut task = self.workers.spawn_blocking(move || {
            let _use_guard = use_guard;
            let previous = BLOCKING.with(|slot| slot.replace(Some(control.clone())));
            struct Restore(Option<Arc<AttemptControl>>);
            impl Drop for Restore {
                fn drop(&mut self) {
                    BLOCKING.with(|slot| slot.replace(self.0.take()));
                }
            }
            let _restore = Restore(previous);
            control.checkpoint()?;
            let result = action(&control);
            control.checkpoint()?;
            result
        });
        let mut latest = self.latest.subscribe();
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                result = &mut task => {
                    let progress = latest.borrow_and_update().clone();
                    if !self.cancel.is_cancelled() { self.engine.update_repair_progress(self.job, progress).await; }
                    return result.map_err(|e| PostError::Subprocess(format!("processing worker: {e}")))?;
                }
                _ = tick.tick() => {
                    if latest.has_changed().unwrap_or(false) && !self.cancel.is_cancelled() {
                        let progress = latest.borrow_and_update().clone();
                        self.engine.update_repair_progress(self.job, progress).await;
                    }
                }
            }
        }
    }
}

pub(crate) fn current() -> Option<Arc<AttemptControl>> {
    CURRENT
        .try_with(Arc::clone)
        .ok()
        .or_else(|| BLOCKING.with(|slot| slot.borrow().clone()))
}
pub(crate) fn checkpoint() -> std::io::Result<()> {
    current().map_or(Ok(()), |control| control.checkpoint())
}
pub(crate) fn scanned(bytes: u64) {
    if let Some(control) = current() {
        control.bytes(bytes);
    }
}
fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |t| t.as_secs() as i64)
}

/// Retain the original group id: `Child::wait` clears `Child::id` while
/// descendants may still own pipes or write the private workspace.
pub(crate) async fn kill_child(
    child: &mut tokio::process::Child,
    group: Option<u32>,
) -> Result<(), PostError> {
    #[cfg(unix)]
    if let Some(id) = group {
        // SAFETY: every runner creates a process group owned by this child.
        unsafe {
            libc::kill(-(id as i32), libc::SIGKILL);
        }
    }
    let _ = child.kill().await;
    let _ = child.wait().await;
    #[cfg(unix)]
    if let Some(id) = group {
        for _ in 0..50 {
            // SAFETY: signal zero observes process-group existence only.
            if unsafe { libc::kill(-(id as i32), 0) } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        return Err(PostError::Unquiesced(format!(
            "owned process group {id} remains present after SIGKILL"
        )));
    }
    Ok(())
}
impl AttemptControl {
    pub async fn preserve_uncertainty(self: &Arc<Self>, result: &Result<impl Send, PostError>) {
        if let (Err(PostError::Unquiesced(reason)), Some(source)) = (result, &self.source) {
            let source = source.clone();
            let reason = reason.clone();
            let inventory = self.engine.artifacts();
            if let Ok(Err(error)) = self
                .workers
                .spawn_blocking(move || inventory.hold_processing_uncertainty(&source, &reason))
                .await
            {
                tracing::error!(%error, "could not persist subprocess custody hold");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn fixture() -> (tempfile::TempDir, EngineHandle, PpCtx, Arc<AttemptControl>) {
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
        engine
            .artifacts()
            .allocate(4, &root, &root.join("job"))
            .unwrap();
        let ctx = PpCtx::default();
        let control = AttemptControl::new(&engine, &ctx, JobId(4)).await.unwrap();
        (temp, engine, ctx, control)
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_async_caller_keeps_custody_until_blocking_worker_exits() {
        let (_temp, engine, ctx, control) = fixture().await;
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let task = tokio::spawn(async move {
            let _cancel_on_drop = control.cancel.clone().drop_guard();
            control
                .blocking(move |control| {
                    let _ = started.send(());
                    wait.recv().unwrap();
                    control.checkpoint()?;
                    Ok(())
                })
                .await
        });
        ready.await.unwrap();
        task.abort();
        let _ = task.await;
        assert!(engine.artifacts().processing_in_use(4).unwrap());
        assert!(engine.artifacts().acquire_attempt_use(4).is_err());
        release.send(()).unwrap();
        ctx.workers.close();
        ctx.workers.wait().await;
        assert!(!engine.artifacts().processing_in_use(4).unwrap());
        engine.shutdown().await;
    }
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_tool_is_killed_and_joined_before_worker_custody_releases() {
        let (_temp, engine, ctx, control) = fixture().await;
        let root = engine.artifacts().for_job(4).unwrap().unwrap().path;
        let cwd = root.clone();
        let task = tokio::spawn(CURRENT.scope(control, async move {
            crate::tools::run_tool(
                "sh",
                &["-c", "echo $$ > ready; sleep 30 & echo $! >> ready; wait"],
                &cwd,
                std::time::Duration::from_secs(40),
            )
            .await
        }));
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        let pids = loop {
            if let Ok(text) = std::fs::read_to_string(root.join("ready")) {
                let ids: Vec<i32> = text.lines().filter_map(|line| line.parse().ok()).collect();
                if ids.len() == 2 {
                    break ids;
                }
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };
        task.abort();
        let _ = task.await;
        ctx.workers.close();
        tokio::time::timeout(std::time::Duration::from_secs(8), ctx.workers.wait())
            .await
            .unwrap();
        // Direct child is reaped, and any uncertain group keeps an explicit
        // review hold instead of granting retirement authority.
        assert_eq!(unsafe { libc::kill(pids[0], 0) }, -1);
        assert!(!engine.artifacts().processing_in_use(4).unwrap());
        let artifact = engine.artifacts().for_job(4).unwrap().unwrap();
        if unsafe { libc::kill(pids[1], 0) } == 0 {
            assert!(artifact.hold.is_some());
        }
        engine.shutdown().await;
    }
}
