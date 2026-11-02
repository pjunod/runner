//! Bounded read-only proof work, outside the queue mutation lane.
//! A verdict authorizes only one exact automatic control CAS, never bytes.
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

struct Entry {
    marker: String,
    scope: String,
    job: u32,
    owner: String,
    cancel: CancellationToken,
    result: Option<(Result<bool, String>, Instant)>,
}
pub(crate) struct ControlProofs {
    entries: Arc<Mutex<HashMap<String, Entry>>>,
    slots: Arc<tokio::sync::Semaphore>,
    tracker: TaskTracker,
    closed: AtomicBool,
    pub(crate) cursor: AtomicU32,
    #[cfg(test)]
    pub(crate) gate: Mutex<Option<Arc<(tokio::sync::Semaphore, tokio::sync::Semaphore)>>>,
    #[cfg(test)]
    registration_gate: Mutex<Option<Arc<(std::sync::Barrier, std::sync::Barrier)>>>,
}
impl Default for ControlProofs {
    fn default() -> Self {
        Self {
            entries: Default::default(),
            slots: Arc::new(tokio::sync::Semaphore::new(4)),
            tracker: TaskTracker::new(),
            closed: AtomicBool::new(false),
            cursor: AtomicU32::new(0),
            #[cfg(test)]
            gate: Mutex::new(None),
            #[cfg(test)]
            registration_gate: Mutex::new(None),
        }
    }
}
impl ControlProofs {
    pub(crate) fn prune(&self, markers: &HashSet<String>, scope: &str) {
        self.entries.lock().unwrap().retain(|_, entry| {
            let keep = entry.scope == scope && markers.contains(&entry.marker);
            if !keep {
                entry.cancel.cancel();
            }
            keep
        });
    }
    pub(crate) fn invalidate(&self) {
        for (_, entry) in self.entries.lock().unwrap().drain() {
            entry.cancel.cancel();
        }
    }
    pub(crate) fn resolve<F>(
        &self,
        key: String,
        marker: String,
        scope: String,
        job: u32,
        owner: String,
        work: F,
    ) -> Result<bool, String>
    where
        F: Future<Output = Result<bool, String>> + Send + 'static,
    {
        let mut registry = self.entries.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            return Err("source proof lane is shutting down".into());
        }
        if let Some(entry) = registry.get(&key) {
            match &entry.result {
                None => {
                    return Err(
                        "source-owner proof is pending; unrelated controls remain available".into(),
                    )
                }
                Some((Ok(true), _)) => {
                    registry.remove(&key);
                    return Ok(true);
                }
                Some((result, at)) if at.elapsed() < Duration::from_secs(5) => {
                    return result.clone()
                }
                _ => {
                    registry.remove(&key);
                }
            }
        }
        if registry.len() >= 64 {
            // Ready negative observations cannot occupy the bounded lane
            // forever. Cursor rotation admits later jobs before retrying them.
            let evict = registry
                .iter()
                .filter_map(|(key, entry)| match &entry.result {
                    Some((Ok(true), _)) | None => None,
                    Some((_, at)) => Some((key.clone(), *at)),
                })
                .min_by_key(|(_, at)| *at)
                .map(|(key, _)| key);
            if let Some(key) = evict {
                registry.remove(&key);
            } else {
                return Err("source proof lane is full; queued for a later pass".into());
            }
        }
        let cancel = CancellationToken::new();
        registry.insert(
            key.clone(),
            Entry {
                marker,
                scope,
                job,
                owner,
                cancel: cancel.clone(),
                result: None,
            },
        );
        self.cursor.store(job, Ordering::Release);
        let entries = self.entries.clone();
        let slots = self.slots.clone();
        #[cfg(test)]
        let gate = self.gate.lock().unwrap().clone();
        #[cfg(test)]
        if let Some(gate) = self.registration_gate.lock().unwrap().clone() {
            gate.0.wait();
            gate.1.wait();
        }
        self.tracker.spawn(async move{
            let permit=tokio::select!{_=cancel.cancelled()=>return,permit=slots.acquire_owned()=>match permit{Ok(permit)=>permit,Err(_)=>return}};
            if cancel.is_cancelled(){return}
            #[cfg(test)] if let Some(gate)=gate{
                gate.0.add_permits(1);
                tokio::select!{_=cancel.cancelled()=>return,release=gate.1.acquire()=>{if let Ok(release)=release{release.forget();}}}
            }
            // Own any blocking metadata operation until it actually joins.
            // Cancellation cannot detach it or release the bounded permit.
            let result=work.await.map_err(|error|error.chars().take(2048).collect());
            drop(permit);
            if let Some(entry)=entries.lock().unwrap().get_mut(&key){
                if !cancel.is_cancelled(){entry.result=Some((result,Instant::now()));}
            }
        });
        drop(registry);
        Err("source-owner proof is pending; unrelated controls remain available".into())
    }
    pub(crate) async fn shutdown(&self) {
        {
            let mut registry = self.entries.lock().unwrap();
            self.closed.store(true, Ordering::Release);
            for (_, entry) in registry.drain() {
                entry.cancel.cancel();
            }
            self.tracker.close();
        }
        self.tracker.wait().await;
    }
    pub(crate) fn snapshot(&self) -> Vec<serde_json::Value> {
        let now = crate::control::unix_ms_public();
        let mut rows=self.entries.lock().unwrap().values().map(|entry|{
            let (state,error,retry)=match &entry.result{
                None=>("pending",Some("Waiting for exact source-owner proof".to_owned()),None),
                Some((Ok(true),_))=>("verified",None,None),
                Some((Ok(false),_))=>("review_required",Some("Exact source identity changed; review source custody or remove this job".into()),None),
                Some((Err(error),at))=>("retry",Some(error.clone()),Some(now.saturating_add(5000_i64.saturating_sub(at.elapsed().as_millis() as i64)))),
            };
            serde_json::json!({"jobId":entry.job,"owner":entry.owner,"state":state,"error":error,"nextRetryAtMs":retry})
        }).collect::<Vec<_>>();
        rows.sort_by_key(|row| row["jobId"].as_u64());
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn registration_is_fenced_against_shutdown_join() {
        let proofs = Arc::new(ControlProofs::default());
        let gate = Arc::new((std::sync::Barrier::new(2), std::sync::Barrier::new(2)));
        *proofs.registration_gate.lock().unwrap() = Some(gate.clone());
        let registration = tokio::task::spawn_blocking({
            let proofs = proofs.clone();
            move || {
                proofs.resolve(
                    "key".into(),
                    "marker".into(),
                    "authority".into(),
                    1,
                    "owner".into(),
                    async { Ok(true) },
                )
            }
        });
        gate.0.wait();
        let (started, ready) = tokio::sync::oneshot::channel();
        let mut shutdown = tokio::spawn({
            let proofs = proofs.clone();
            async move {
                let _ = started.send(());
                proofs.shutdown().await
            }
        });
        ready.await.unwrap();
        let returned_early = tokio::time::timeout(Duration::from_millis(50), &mut shutdown)
            .await
            .is_ok();
        gate.1.wait();
        assert!(registration.await.unwrap().is_err());
        if !returned_early {
            shutdown.await.unwrap();
        }
        assert!(
            !returned_early,
            "shutdown cannot return between insertion and tracked registration"
        );
        assert!(proofs.tracker.is_empty());
        assert!(proofs.snapshot().is_empty());
        assert!(proofs
            .resolve(
                "late".into(),
                "late".into(),
                "authority".into(),
                2,
                "owner".into(),
                async { panic!("closed proof lane must not start work") }
            )
            .is_err());
    }
    #[tokio::test]
    async fn negative_proof_capacity_yields_to_later_jobs_and_shutdown_joins() {
        let proofs = ControlProofs::default();
        for job in 1..=64 {
            assert!(proofs
                .resolve(
                    job.to_string(),
                    job.to_string(),
                    "authority".into(),
                    job,
                    "offline".into(),
                    async { Err("owner offline".into()) }
                )
                .is_err());
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while proofs
            .snapshot()
            .iter()
            .any(|entry| entry["state"] == "pending")
        {
            assert!(Instant::now() < deadline);
            tokio::task::yield_now().await;
        }
        assert!(proofs
            .resolve(
                "65".into(),
                "65".into(),
                "authority".into(),
                65,
                "online".into(),
                async { Ok(true) }
            )
            .is_err());
        loop {
            let result = proofs.resolve(
                "65".into(),
                "65".into(),
                "authority".into(),
                65,
                "online".into(),
                async { panic!("existing proof must join") },
            );
            if result == Ok(true) {
                break;
            }
            assert!(Instant::now() < deadline);
            tokio::task::yield_now().await;
        }
        assert_eq!(proofs.cursor.load(Ordering::Acquire), 65);
        assert!(proofs
            .snapshot()
            .iter()
            .all(|entry| entry["state"] == "retry" && entry["nextRetryAtMs"].is_number()));
        assert!(proofs
            .resolve(
                "66".into(),
                "66".into(),
                "authority".into(),
                66,
                "changed-source".into(),
                async { Ok(false) }
            )
            .is_err());
        while !proofs
            .snapshot()
            .iter()
            .any(|entry| entry["jobId"] == 66 && entry["state"] == "review_required")
        {
            assert!(Instant::now() < deadline);
            tokio::task::yield_now().await;
        }
        assert!(
            proofs
                .snapshot()
                .iter()
                .find(|entry| entry["jobId"] == 66)
                .unwrap()["nextRetryAtMs"]
                .is_null(),
            "changed exact identity must not advertise automatic successful recovery"
        );
        proofs.shutdown().await;
        assert!(proofs.snapshot().is_empty());
    }
}
