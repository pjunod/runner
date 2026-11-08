//! One bounded, ordered filesystem lane. Snapshot compaction runs before any
//! later append, so it can never erase records outside the captured document.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub(crate) struct PersistResult {
    pub result: Result<(), nzbd_state::StateError>,
    pub elapsed_ms: u64,
    pub snapshot: bool,
}

type IoValue = Box<dyn std::any::Any + Send>;
pub(crate) type IoResult = Result<IoValue, nzbd_state::StateError>;

enum Request {
    Io {
        revision: u64,
        action: Box<dyn FnOnce() -> IoResult + Send>,
    },
    Snapshot {
        revision: u64,
        doc: nzbd_state::QueueSnapshotDoc,
        delegated: HashSet<JobId>,
        authoritative: bool,
        guard: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    },
    Append {
        revision: u64,
        record: JournalRecord,
        provenance: Option<serde_json::Value>,
    },
    Sync {
        revision: u64,
    },
    Remove {
        revision: u64,
        job: JobId,
    },
    Shutdown {
        doc: Option<nzbd_state::QueueSnapshotDoc>,
        authoritative: bool,
        guard: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
        marker: UncleanMarker,
        volumes: crate::volumes::VolumeBook,
        reply: oneshot::Sender<bool>,
    },
}

pub(super) struct Persistence {
    tx: mpsc::Sender<Request>,
    pending: Arc<AtomicUsize>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Persistence {
    pub(super) fn start(
        mut journal: JobJournals,
        store: SnapshotStore,
        worker_store: SnapshotStore,
        engine: mpsc::Sender<EngineMsg>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel(256);
        let pending = Arc::new(AtomicUsize::new(0));
        let count = pending.clone();
        let thread=std::thread::Builder::new().name("nzbd-persistence".into()).spawn(move || {
            // A failed append is a barrier failure until a complete snapshot
            // successfully captures the owner state. Never compact on failure.
            let mut append_failed = false;
            while let Some(request) = rx.blocking_recv() {
                let started = std::time::Instant::now();
                let snapshot=matches!(&request,Request::Snapshot{..});
                let (revision, result) = match request {
                    Request::Io {revision,action} => {
                        let result=action();
                        let delivered=engine.blocking_send(EngineMsg::IoFinished {revision,result});
                        count.fetch_sub(1,Ordering::Release);
                        if delivered.is_err() {break;}
                        continue;
                    }
                    Request::Append { revision, record, provenance } => {
                        let result = match provenance {
                            Some(proof) => journal.append_with_provenance(&record, &proof),
                            None => journal.append(&record),
                        }.and_then(|()| journal.sync());
                        append_failed |= result.is_err();
                        (revision, result)
                    }
                    Request::Snapshot {revision, doc, delegated, authoritative, guard} => {
                        let result = (|| {
                            journal.sync()?;
                            let target=if authoritative {&store} else {&worker_store};
                            match guard { Some(g) => target.save_guarded(&doc, &move || g()), None => target.save(&doc) }?;
                            // Only this lane appends local records. Foreign live
                            // journals remain untouched, including unknown jobs.
                            for job in doc.jobs.iter().filter(|_|authoritative) {
                                if !delegated.contains(&job.id) { if let Err(error)=journal.compact_local(job.id) {tracing::warn!(job=job.id.0,%error,"snapshot durable; journal compaction deferred");} }
                            }
                            Ok(())
                        })();
                        if result.is_ok() { append_failed = false; }
                        (revision,result)
                    }
                    Request::Sync {revision} => (revision, journal.sync()),
                    Request::Remove {revision,job} => (revision,journal.compact_local(job)),
                    Request::Shutdown {doc,authoritative,guard,marker,volumes,reply} => {
                        let result = (|| {
                            journal.sync()?;
                            if let Some(doc) = doc {
                                let target=if authoritative {&store} else {&worker_store};
                                match guard {Some(g)=>target.save_guarded(&doc,&move||g()),None=>target.save(&doc)}?;
                            } else if append_failed {
                                return Err(nzbd_state::StateError::Corrupt("unresolved journal persistence failure".into()));
                            }
                            volumes.save_result()?;
                            marker.disarm()?;
                            Ok::<(),nzbd_state::StateError>(())
                        })();
                        if let Err(error)=&result {tracing::error!(%error,"final persistence failed; unclean marker retained");}
                        count.fetch_sub(1,Ordering::Release);
                        let _=reply.send(result.is_ok());
                        break;
                    }
                };
                // The owner drains this channel even during shutdown. Count
                // stays nonzero until completion has been delivered.
                let delivered=engine.blocking_send(EngineMsg::PersistenceFinished {revision,result:PersistResult {result,snapshot,elapsed_ms:started.elapsed().as_millis() as u64}});
                count.fetch_sub(1,Ordering::Release);
                if delivered.is_err() {break;}
            }
        }).expect("start persistence lane");
        Self {
            tx,
            pending,
            thread: Some(thread),
        }
    }
    fn send(&self, request: Request) -> bool {
        self.pending.fetch_add(1, Ordering::AcqRel);
        if self.tx.try_send(request).is_err() {
            self.pending.fetch_sub(1, Ordering::Release);
            false
        } else {
            true
        }
    }
    pub(super) fn in_flight(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }
}

impl Owner {
    pub(super) fn io_then<T: Send + 'static>(
        &mut self,
        action: impl FnOnce() -> Result<T, nzbd_state::StateError> + Send + 'static,
        continuation: impl FnOnce(&mut Owner, Result<T, nzbd_state::StateError>) + Send + 'static,
    ) {
        let revision = fresh_epoch();
        let operation = Box::new(move || action().map(|value| Box::new(value) as IoValue));
        let callback: IoContinuation = Box::new(move |owner: &mut Owner, result: IoResult| {
            let typed = result.and_then(|value| {
                value.downcast::<T>().map(|v| *v).map_err(|_| {
                    nzbd_state::StateError::Corrupt("persistence response type mismatch".into())
                })
            });
            continuation(owner, typed);
        });
        if self.persistence.send(Request::Io {
            revision,
            action: operation,
        }) {
            self.io_callbacks.insert(revision, callback);
        } else {
            callback(
                self,
                Err(nzbd_state::StateError::Busy(
                    "bounded persistence lane unavailable",
                )),
            );
        }
    }
    pub(super) fn io_finished(&mut self, revision: u64, result: IoResult) {
        if let Some(callback) = self.io_callbacks.remove(&revision) {
            callback(self, result);
        }
        // State-lane completions free admission capacity too. Give retained
        // journal facts priority, then resume bounded recovery prerequisites.
        self.retry_journal_checkpoint();
        self.retry_payload_recoveries();
        self.start_verifications();
    }

    pub(super) fn persist_then(
        &mut self,
        continuation: impl FnOnce(&mut Owner, bool) + Send + 'static,
    ) {
        if self.persistence_fenced || (!self.persist && !self.worker_mode) {
            continuation(self, false);
            return;
        }
        self.sync_lifecycle_timings();
        for job in &mut self.state.jobs {
            crate::automatic_control::invalidate_changed(job);
        }
        self.state.sync_url_admission_receipts();
        for receipt in &mut self.state.url_admission_receipts {
            refresh_url_admission_custody(receipt, &self.artifacts);
        }

        let revision = fresh_epoch();
        let request = Request::Snapshot {
            revision,
            doc: self.state.to_doc(),
            delegated: self
                .delegated
                .keys()
                .copied()
                .chain(
                    self.state
                        .jobs
                        .iter()
                        .filter(|job| {
                            job.params
                                .iter()
                                .any(|(key, _)| key == "*Recovery:journal-loading")
                        })
                        .map(|job| job.id),
                )
                .collect(),
            authoritative: self.persist,
            guard: self.persist.then(|| self.persist_guard.clone()).flatten(),
        };
        if !self.persistence.send(request) {
            self.dirty = true;
            tracing::error!("bounded persistence lane unavailable");
            continuation(self, false);
            return;
        }
        self.persistence_callbacks
            .insert(revision, Box::new(continuation));
        self.dirty = false;
        self.last_save = Instant::now();
    }
    pub(super) fn append_journal(
        &mut self,
        record: JournalRecord,
        provenance: Option<serde_json::Value>,
    ) {
        if self.journal_checkpoint.is_some() {
            // The actor's segment/proof state retains every accepted fact.
            // A later checkpoint must include this one before leasing resumes.
            self.journal_checkpoint = Some(fresh_epoch());
            self.dirty = true;
            return;
        }
        let job = record.job;
        let epoch = self.job_epochs.get(&job).copied();
        let revision = fresh_epoch();
        let completion: Continuation = Box::new(move |owner, ok| {
            if !ok && owner.job_epochs.get(&job).copied() == epoch && owner.state.job(job).is_some()
            {
                owner.hold_job(
                    job,
                    nzbd_types::HoldCause::StorageUnavailable,
                    "persistence",
                    "segment journal is not durable; evidence retained",
                );
            }
        });
        if self.persistence.send(Request::Append {
            revision,
            record,
            provenance,
        }) {
            self.persistence_callbacks.insert(revision, completion);
        } else {
            // Admission pressure is not a failed write. Retain the fact in
            // owner state, fence new leases, and checkpoint when the ordered
            // lane has capacity. No second queue of segment records is needed.
            self.journal_checkpoint = Some(fresh_epoch());
            self.dirty = true;
            tracing::warn!(
                job = job.0,
                "journal admission saturated; progress retained for a coalesced checkpoint"
            );
        }
    }
    pub(super) fn retry_journal_checkpoint(&mut self) {
        let Some(epoch) = self.journal_checkpoint else {
            return;
        };
        if self.journal_checkpoint_inflight
            || self.persistence_fenced
            || self.cancel.is_cancelled()
            || self.persistence.tx.capacity() == 0
        {
            return;
        }
        self.journal_checkpoint_inflight = true;
        self.persist_then(move |owner, committed| {
            owner.journal_checkpoint_inflight = false;
            if committed && owner.journal_checkpoint == Some(epoch) {
                owner.journal_checkpoint = None;
                let jobs = owner
                    .state
                    .jobs
                    .iter()
                    .map(|job| job.id)
                    .collect::<Vec<_>>();
                for job in jobs {
                    owner.check_job_complete(job);
                }
                owner.publish_now();
                owner.bump_epoch();
            }
            // A newer completion while this save was in flight needs another
            // checkpoint. persistence_finished retries after this callback.
        });
    }

    pub(super) fn sync_journal(&mut self) {
        if self.persistence.in_flight() != 0 {
            return;
        }
        let revision = fresh_epoch();
        if self.persistence.send(Request::Sync { revision }) {
            self.persistence_callbacks
                .insert(revision, Box::new(|_, _| {}));
        }
    }
    pub(super) fn remove_journal(&mut self, job: JobId) {
        let revision = fresh_epoch();
        if self.persistence.send(Request::Remove { revision, job }) {
            self.persistence_callbacks
                .insert(revision, Box::new(|_, _| {}));
        }
    }
    pub(super) fn persistence_finished(&mut self, revision: u64, result: PersistResult) {
        if result.snapshot {
            self.last_save_ms = result.elapsed_ms;
        }
        let ok = result.result.is_ok();
        if let Err(error) = result.result {
            self.on_snapshot_save_error(&error);
            self.dirty = true;
        }
        if let Some(callback) = self.persistence_callbacks.remove(&revision) {
            callback(self, ok);
        }
        self.retry_journal_checkpoint();
    }
    pub(super) async fn flush_shutdown_state(&mut self) -> bool {
        self.state.sync_url_admission_receipts();
        let (tx, rx) = oneshot::channel();
        let request = Request::Shutdown {
            doc: (self.persist || self.worker_mode).then(|| self.state.to_doc()),
            authoritative: self.persist,
            guard: self.persist.then(|| self.persist_guard.clone()).flatten(),
            marker: UncleanMarker::new(&self.state_dir, &self.journal_suffix),
            volumes: self.volumes.clone(),
            reply: tx,
        };
        if !self.persistence.send(request) {
            return false;
        }
        let durable = rx.await.unwrap_or(false);
        let joined = match self.persistence.thread.take() {
            Some(thread) => tokio::task::spawn_blocking(move || thread.join().is_ok())
                .await
                .unwrap_or(false),
            None => false,
        };
        durable && joined
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lane(path: &Path) -> (Persistence, mpsc::Receiver<EngineMsg>) {
        let (tx, rx) = mpsc::channel(512);
        (
            Persistence::start(
                JobJournals::open(path, "test").unwrap(),
                SnapshotStore::open(path).unwrap(),
                SnapshotStore::open(&path.join("worker")).unwrap(),
                tx,
            ),
            rx,
        )
    }
    fn record(job: JobId, number: u32) -> JournalRecord {
        JournalRecord {
            job,
            file: FileId(1),
            segment_number: number,
            offset: u64::from(number) * 4,
            len: 4,
            crc32: 42,
            file_size: 32,
        }
    }
    async fn completion(rx: &mut mpsc::Receiver<EngineMsg>) -> PersistResult {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap()
        {
            EngineMsg::PersistenceFinished { result, .. } => result,
            _ => panic!("expected persistence completion"),
        }
    }
    async fn stop(lane: &mut Persistence, path: &Path) {
        let (reply, done) = oneshot::channel();
        assert!(lane.send(Request::Shutdown {
            doc: None,
            authoritative: true,
            guard: None,
            marker: UncleanMarker::new(path, "test"),
            volumes: crate::volumes::VolumeBook::load(path, "test"),
            reply
        }));
        assert!(done.await.unwrap());
        lane.thread.take().unwrap().join().unwrap();
    }

    #[tokio::test]
    async fn snapshot_compaction_keeps_appends_after_its_capture() {
        let dir = tempfile::tempdir().unwrap();
        let (mut lane, mut rx) = lane(dir.path());
        let mut state = QueueState::default();
        let job = state.admit_url("test".into(), "http://example.invalid/test", None, 0);
        assert!(lane.send(Request::Append {
            revision: 1,
            record: record(job, 1),
            provenance: None
        }));
        assert!(lane.send(Request::Snapshot {
            revision: 2,
            doc: state.to_doc(),
            delegated: HashSet::new(),
            authoritative: true,
            guard: None
        }));
        assert!(lane.send(Request::Append {
            revision: 3,
            record: record(job, 2),
            provenance: None
        }));
        for _ in 0..3 {
            assert!(completion(&mut rx).await.result.is_ok());
        }
        let remaining = JobJournals::replay_job(dir.path(), job).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].segment_number, 2);
        stop(&mut lane, dir.path()).await;
    }

    #[tokio::test]
    async fn snapshot_never_unlinks_a_foreign_incarnation_append_descriptor() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = QueueState::default();
        let job = state.admit_url("test".into(), "http://example.invalid/test", None, 0);
        // Even the same logical node suffix gets a distinct exclusive inode.
        let mut foreign = JobJournals::open(dir.path(), "test").unwrap();
        foreign.append(&record(job, 10)).unwrap();
        foreign.sync().unwrap();
        let (mut lane, mut rx) = lane(dir.path());
        assert!(lane.send(Request::Append {
            revision: 1,
            record: record(job, 1),
            provenance: None
        }));
        assert!(lane.send(Request::Snapshot {
            revision: 2,
            doc: state.to_doc(),
            delegated: HashSet::new(),
            authoritative: true,
            guard: None
        }));
        assert!(completion(&mut rx).await.result.is_ok());
        assert!(completion(&mut rx).await.result.is_ok());
        foreign.append(&record(job, 11)).unwrap();
        foreign.sync().unwrap();
        let records = JobJournals::replay_job(dir.path(), job).unwrap();
        assert_eq!(
            records
                .iter()
                .map(|record| record.segment_number)
                .collect::<HashSet<_>>(),
            HashSet::from([10, 11])
        );
        stop(&mut lane, dir.path()).await;
    }

    #[tokio::test]
    async fn fenced_snapshot_never_compacts_unaccepted_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let (mut lane, mut rx) = lane(dir.path());
        let mut state = QueueState::default();
        let job = state.admit_url("test".into(), "http://example.invalid/test", None, 0);
        assert!(lane.send(Request::Append {
            revision: 1,
            record: record(job, 1),
            provenance: None
        }));
        assert!(lane.send(Request::Snapshot {
            revision: 2,
            doc: state.to_doc(),
            delegated: HashSet::new(),
            authoritative: true,
            guard: Some(Arc::new(|| false))
        }));
        assert!(completion(&mut rx).await.result.is_ok());
        assert!(completion(&mut rx).await.result.is_err());
        assert_eq!(JobJournals::replay_job(dir.path(), job).unwrap().len(), 1);
        assert!(SnapshotStore::open(dir.path())
            .unwrap()
            .load()
            .unwrap()
            .is_none());
        stop(&mut lane, dir.path()).await;
    }

    #[tokio::test]
    async fn worker_checkpoint_preserves_shared_queue_and_foreign_journals() {
        let dir = tempfile::tempdir().unwrap();
        let mut authority = QueueState::default();
        authority.admit_url(
            "authority".into(),
            "http://example.invalid/authority",
            None,
            0,
        );
        SnapshotStore::open(dir.path())
            .unwrap()
            .save(&authority.to_doc())
            .unwrap();
        let mut worker = QueueState::default();
        let job = worker.admit_url("worker".into(), "http://example.invalid/worker", None, 0);
        let (mut lane, mut rx) = lane(dir.path());
        assert!(lane.send(Request::Append {
            revision: 1,
            record: record(job, 1),
            provenance: None
        }));
        assert!(lane.send(Request::Snapshot {
            revision: 2,
            doc: worker.to_doc(),
            delegated: HashSet::new(),
            authoritative: false,
            guard: None
        }));
        assert!(completion(&mut rx).await.result.is_ok());
        assert!(completion(&mut rx).await.result.is_ok());
        assert_eq!(
            SnapshotStore::open(dir.path())
                .unwrap()
                .load()
                .unwrap()
                .unwrap()
                .jobs[0]
                .name,
            "authority"
        );
        assert_eq!(
            SnapshotStore::open(&dir.path().join("worker"))
                .unwrap()
                .load()
                .unwrap()
                .unwrap()
                .jobs[0]
                .name,
            "worker"
        );
        assert_eq!(JobJournals::replay_job(dir.path(), job).unwrap().len(), 1);
        stop(&mut lane, dir.path()).await;
    }

    #[tokio::test]
    async fn blocked_worker_retains_its_slot_and_bounds_admission() {
        let dir = tempfile::tempdir().unwrap();
        let (mut lane, mut rx) = lane(dir.path());
        let (entered, started) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        assert!(lane.send(Request::Io {
            revision: 1,
            action: Box::new(move || {
                let _ = entered.send(());
                wait.recv().unwrap();
                Ok(Box::new(()) as IoValue)
            })
        }));
        started.await.unwrap();
        for revision in 2..258 {
            assert!(lane.send(Request::Sync { revision }));
        }
        assert!(!lane.send(Request::Sync { revision: 258 }));
        assert_eq!(lane.in_flight(), 257);
        release.send(()).unwrap();
        for _ in 0..257 {
            tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .unwrap()
                .unwrap();
        }
        stop(&mut lane, dir.path()).await;
    }
}
