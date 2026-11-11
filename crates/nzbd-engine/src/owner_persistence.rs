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

// Only work already queued behind the first append may share its sync. A
// non-append request is retained as the next FIFO barrier, never overtaken.
const JOURNAL_BATCH_RECORDS: usize = 32;

struct JournalAppend {
    revision: u64,
    record: JournalRecord,
    provenance: Option<serde_json::Value>,
}

fn append_prefix(
    first: JournalAppend,
    rx: &mut mpsc::Receiver<Request>,
    deferred: &mut Option<Request>,
) -> Vec<JournalAppend> {
    let mut batch = vec![first];
    while batch.len() < JOURNAL_BATCH_RECORDS {
        match rx.try_recv() {
            Ok(Request::Append {
                revision,
                record,
                provenance,
            }) => {
                batch.push(JournalAppend {
                    revision,
                    record,
                    provenance,
                });
            }
            Ok(barrier) => {
                *deferred = Some(barrier);
                break;
            }
            Err(_) => break,
        }
    }
    batch
}

trait JournalCommit {
    fn append_record(
        &mut self,
        record: &JournalRecord,
        provenance: Option<&serde_json::Value>,
    ) -> Result<(), nzbd_state::StateError>;
    fn sync_records(&mut self) -> Result<(), nzbd_state::StateError>;
}

impl JournalCommit for JobJournals {
    fn append_record(
        &mut self,
        record: &JournalRecord,
        provenance: Option<&serde_json::Value>,
    ) -> Result<(), nzbd_state::StateError> {
        match provenance {
            Some(proof) => self.append_with_provenance(record, proof),
            None => self.append(record),
        }
    }
    fn sync_records(&mut self) -> Result<(), nzbd_state::StateError> {
        self.sync()
    }
}

// A shared sync failure must reach every affected continuation without losing
// the original filesystem operation, path, or errno.
fn copy_commit_error(error: &nzbd_state::StateError) -> nzbd_state::StateError {
    use nzbd_state::StateError;
    match error {
        StateError::Io { op, path, source } => StateError::Io {
            op,
            path: path.clone(),
            source: source
                .raw_os_error()
                .map(std::io::Error::from_raw_os_error)
                .unwrap_or_else(|| std::io::Error::new(source.kind(), source.to_string())),
        },
        StateError::Serde(error) => StateError::Serde(serde_json::Error::io(
            std::io::Error::other(error.to_string()),
        )),
        StateError::Busy(reason) => StateError::Busy(reason),
        StateError::Corrupt(reason) => StateError::Corrupt(reason.clone()),
    }
}

/// Test hook: the persistence lane dies after appending a record of this job
/// and before the journal sync (scoped by job id; tests use unique ids).
#[cfg(test)]
pub(super) static JOURNAL_KILL_AFTER_APPEND: std::sync::Mutex<Option<JobId>> =
    std::sync::Mutex::new(None);

fn commit_append_batch(
    journal: &mut impl JournalCommit,
    batch: Vec<JournalAppend>,
) -> Vec<(u64, Result<(), nzbd_state::StateError>)> {
    let mut results = Vec::with_capacity(batch.len());
    let mut append_error = None;
    #[cfg(test)]
    let batch_jobs = batch
        .iter()
        .map(|append| append.record.job)
        .collect::<Vec<_>>();
    for append in batch {
        // A failed append may leave a torn tail. Do not append more records
        // behind it and then incorrectly acknowledge them as replayable.
        let result = match &append_error {
            Some(error) => Err(copy_commit_error(error)),
            None => journal.append_record(&append.record, append.provenance.as_ref()),
        };
        if let Err(error) = &result {
            if append_error.is_none() {
                append_error = Some(copy_commit_error(error));
            }
        }
        results.push((append.revision, result));
    }
    #[cfg(test)]
    let armed = *JOURNAL_KILL_AFTER_APPEND
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    #[cfg(test)]
    if let Some(job) = armed {
        if results.iter().any(|(_, result)| result.is_ok()) && batch_jobs.contains(&job) {
            // A process killed between the append and its sync.
            panic!("simulated kill -9 after journal append, before sync");
        }
    }
    // No continuation can observe success before the shared durability point.
    if results.iter().any(|(_, result)| result.is_ok()) {
        if let Err(error) = journal.sync_records() {
            for (_, result) in &mut results {
                if result.is_ok() {
                    *result = Err(copy_commit_error(&error));
                }
            }
        }
    }
    results
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
            let mut append_failure: Option<nzbd_state::StateError> = None;
            let mut deferred = None;
            let mut authority_encoder = nzbd_state::SnapshotEncoder::default();
            let mut worker_encoder = nzbd_state::SnapshotEncoder::with_budget(64 * 1024 * 1024);
            'requests: while let Some(request) = deferred.take().or_else(|| rx.blocking_recv()) {
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
                        let batch = append_prefix(JournalAppend { revision, record, provenance }, &mut rx, &mut deferred);
                        let results = match &append_failure {
                            Some(error) => batch.into_iter().map(|append| (append.revision, Err(copy_commit_error(error)))).collect(),
                            None => commit_append_batch(&mut journal, batch),
                        };
                        // After a torn append, later batches must not claim
                        // replayable progress behind its invalid tail. Only a
                        // complete durable snapshot can reset this barrier.
                        if append_failure.is_none() {
                            append_failure = results.iter().find_map(|(_, result)| result.as_ref().err().map(copy_commit_error));
                        }
                        for (revision, result) in results {
                            let delivered = engine.blocking_send(EngineMsg::PersistenceFinished { revision, result: PersistResult { result, snapshot: false, elapsed_ms: started.elapsed().as_millis() as u64 } });
                            count.fetch_sub(1, Ordering::Release);
                            if delivered.is_err() { break 'requests; }
                        }
                        continue;
                    }
                    Request::Snapshot {revision, doc, delegated, authoritative, guard} => {
                        let result = (|| {
                            journal.sync()?;
                            let jobs = doc.jobs.iter().map(|job| job.id).collect::<Vec<_>>();
                            let (target, encoder)=if authoritative {(&store, &mut authority_encoder)} else {(&worker_store, &mut worker_encoder)};
                            match guard { Some(g) => target.save_cached_guarded(doc, encoder, &move || g()), None => target.save_cached_guarded(doc, encoder, &|| true) }?;
                            // Only this lane appends local records. Foreign live
                            // journals remain untouched, including unknown jobs.
                            // Preserve the failed incarnation as evidence;
                            // normal healthy checkpoints still compact locally.
                            for job in jobs.into_iter().filter(|_|authoritative && append_failure.is_none()) {
                                if !delegated.contains(&job) { if let Err(error)=journal.compact_local(job) {tracing::warn!(job=job.0,%error,"snapshot durable; journal compaction deferred");} }
                            }
                            if append_failure.is_some() {
                                // Worker saves and delegated rows intentionally
                                // retain old files. Never reopen admission onto
                                // a retained torn tail after the checkpoint.
                                journal.rotate_after_checkpoint()?;
                            }
                            Ok(())
                        })();
                        if result.is_ok() { append_failure = None; }
                        (revision,result)
                    }
                    Request::Sync {revision} => (revision, journal.sync()),
                    Request::Remove {revision,job} => (revision,journal.compact_local(job)),
                    Request::Shutdown {doc,authoritative,guard,marker,volumes,reply} => {
                        let result = (|| {
                            journal.sync()?;
                            if let Some(doc) = doc {
                                let (target, encoder)=if authoritative {(&store, &mut authority_encoder)} else {(&worker_store, &mut worker_encoder)};
                                match guard {Some(g)=>target.save_cached_guarded(doc,encoder,&move||g()),None=>target.save_cached_guarded(doc,encoder,&||true)}?;
                            } else if let Some(error) = &append_failure {
                                return Err(copy_commit_error(error));
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
        refresh_url_admission_custodies(&mut self.state.url_admission_receipts, &self.artifacts);

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
            || Instant::now() < self.journal_checkpoint_retry_at
        {
            return;
        }
        self.journal_checkpoint_inflight = true;
        self.persist_then(move |owner, committed| {
            owner.journal_checkpoint_inflight = false;
            if !committed {
                // A genuine storage refusal must not create an immediate
                // failed-save loop. Keep the fence and use the existing
                // recovery cadence before another checkpoint attempt.
                owner.journal_checkpoint_retry_at = Instant::now() + Duration::from_secs(30);
                return;
            }
            owner.journal_checkpoint_retry_at = Instant::now();
            if owner.journal_checkpoint == Some(epoch) {
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

    fn append(revision: u64) -> JournalAppend {
        JournalAppend {
            revision,
            record: record(JobId(1), revision as u32),
            provenance: Some(serde_json::json!({"revision":revision})),
        }
    }
    fn request(revision: u64) -> Request {
        let append = append(revision);
        Request::Append {
            revision,
            record: append.record,
            provenance: append.provenance,
        }
    }

    #[test]
    fn append_prefix_preserves_every_non_append_barrier_and_batch_bound() {
        let dir = tempfile::tempdir().unwrap();
        let (reply, _done) = oneshot::channel();
        let barriers = vec![
            Request::Io {
                revision: 90,
                action: Box::new(|| Ok(Box::new(()) as IoValue)),
            },
            Request::Snapshot {
                revision: 91,
                doc: Default::default(),
                delegated: HashSet::new(),
                authoritative: true,
                guard: None,
            },
            Request::Sync { revision: 92 },
            Request::Remove {
                revision: 93,
                job: JobId(1),
            },
            Request::Shutdown {
                doc: None,
                authoritative: true,
                guard: None,
                marker: UncleanMarker::new(dir.path(), "test"),
                volumes: crate::volumes::VolumeBook::load(dir.path(), "test"),
                reply,
            },
        ];
        for barrier in barriers {
            let kind = std::mem::discriminant(&barrier);
            let (tx, mut rx) = mpsc::channel(64);
            tx.try_send(request(2))
                .unwrap_or_else(|_| panic!("queue append"));
            tx.try_send(barrier)
                .unwrap_or_else(|_| panic!("queue barrier"));
            tx.try_send(request(3))
                .unwrap_or_else(|_| panic!("queue later append"));
            let mut deferred = None;
            let batch = append_prefix(append(1), &mut rx, &mut deferred);
            assert_eq!(
                batch.iter().map(|item| item.revision).collect::<Vec<_>>(),
                vec![1, 2]
            );
            assert_eq!(std::mem::discriminant(deferred.as_ref().unwrap()), kind);
            assert!(matches!(
                rx.try_recv().unwrap(),
                Request::Append { revision: 3, .. }
            ));
        }
        let (tx, mut rx) = mpsc::channel(64);
        for revision in 2..=64 {
            tx.try_send(request(revision))
                .unwrap_or_else(|_| panic!("queue append"));
        }
        let mut deferred = None;
        let batch = append_prefix(append(1), &mut rx, &mut deferred);
        assert_eq!(batch.len(), JOURNAL_BATCH_RECORDS);
        assert!(deferred.is_none());
        assert!(matches!(
            rx.try_recv().unwrap(),
            Request::Append { revision: 33, .. }
        ));
    }

    #[derive(Default)]
    struct ProbeJournal {
        appended: Vec<u32>,
        proofs: Vec<serde_json::Value>,
        syncs: usize,
        fail_append: Option<u32>,
        fail_sync: bool,
        sync_gate: Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>,
    }
    fn injected_error() -> nzbd_state::StateError {
        nzbd_state::StateError::Io {
            op: "fdatasync",
            path: PathBuf::from("/state/jobs/1/journal.test"),
            source: std::io::Error::from_raw_os_error(5),
        }
    }
    impl JournalCommit for ProbeJournal {
        fn append_record(
            &mut self,
            record: &JournalRecord,
            provenance: Option<&serde_json::Value>,
        ) -> Result<(), nzbd_state::StateError> {
            self.appended.push(record.segment_number);
            if self.fail_append == Some(record.segment_number) {
                return Err(injected_error());
            }
            self.proofs.push(provenance.unwrap().clone());
            Ok(())
        }
        fn sync_records(&mut self) -> Result<(), nzbd_state::StateError> {
            self.syncs += 1;
            if let Some((entered, release)) = &self.sync_gate {
                entered.send(()).unwrap();
                release.recv().unwrap();
            }
            if self.fail_sync {
                Err(injected_error())
            } else {
                Ok(())
            }
        }
    }
    fn assert_original_error(error: &nzbd_state::StateError) {
        match error {
            nzbd_state::StateError::Io { op, path, source } => {
                assert_eq!(*op, "fdatasync");
                assert_eq!(path, Path::new("/state/jobs/1/journal.test"));
                assert_eq!(source.raw_os_error(), Some(5));
            }
            _ => panic!("filesystem error lost: {error}"),
        }
    }

    #[test]
    fn journal_batch_waits_for_one_sync_before_any_success() {
        let (entered, started) = std::sync::mpsc::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let (finished, results) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let mut journal = ProbeJournal {
                sync_gate: Some((entered, wait)),
                ..Default::default()
            };
            let committed = commit_append_batch(&mut journal, (1..=32).map(append).collect());
            finished.send((committed, journal)).unwrap();
        });
        started.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(
            results.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        release.send(()).unwrap();
        let (committed, journal) = results.recv_timeout(Duration::from_secs(5)).unwrap();
        worker.join().unwrap();
        assert_eq!(journal.syncs, 1);
        assert_eq!(journal.appended, (1..=32).collect::<Vec<_>>());
        assert_eq!(journal.proofs.len(), 32);
        assert!(committed.iter().all(|(_, result)| result.is_ok()));
        assert_eq!(
            committed
                .iter()
                .map(|(revision, _)| *revision)
                .collect::<Vec<_>>(),
            (1..=32).collect::<Vec<_>>()
        );
    }

    #[test]
    fn journal_batch_failure_preserves_errno_path_and_never_acknowledges_unsynced_rows() {
        let mut journal = ProbeJournal {
            fail_sync: true,
            ..Default::default()
        };
        let results = commit_append_batch(&mut journal, (1..=3).map(append).collect());
        assert_eq!(journal.syncs, 1);
        for (_, result) in results {
            assert_original_error(&result.unwrap_err());
        }
        let mut journal = ProbeJournal {
            fail_append: Some(2),
            ..Default::default()
        };
        let results = commit_append_batch(&mut journal, (1..=3).map(append).collect());
        assert_eq!(
            journal.appended,
            vec![1, 2],
            "nothing may be written behind a torn append"
        );
        assert_eq!(journal.syncs, 1);
        assert!(results[0].1.is_ok());
        assert_original_error(results[1].1.as_ref().unwrap_err());
        assert_original_error(results[2].1.as_ref().unwrap_err());
    }

    #[tokio::test]
    async fn queued_journal_batch_preserves_callback_order_provenance_and_pending_count() {
        let dir = tempfile::tempdir().unwrap();
        let (mut lane, mut rx) = lane(dir.path());
        let (entered, started) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        assert!(lane.send(Request::Io {
            revision: 0,
            action: Box::new(move || {
                entered.send(()).unwrap();
                wait.recv().unwrap();
                Ok(Box::new(()) as IoValue)
            })
        }));
        started.await.unwrap();
        for revision in 1..=64 {
            assert!(lane.send(request(revision)));
        }
        assert_eq!(lane.in_flight(), 65);
        assert!(rx.try_recv().is_err());
        release.send(()).unwrap();
        assert!(matches!(
            rx.recv().await.unwrap(),
            EngineMsg::IoFinished { revision: 0, .. }
        ));
        for expected in 1..=64 {
            match rx.recv().await.unwrap() {
                EngineMsg::PersistenceFinished { revision, result } => {
                    assert_eq!(revision, expected);
                    assert!(result.result.is_ok());
                }
                _ => panic!("expected ordered append acknowledgement"),
            }
        }
        let records = JobJournals::replay_job_with_provenance(dir.path(), JobId(1)).unwrap();
        assert_eq!(records.len(), 64);
        assert!(records.iter().all(|item| item.provenance.is_some()));
        stop(&mut lane, dir.path()).await;
        assert_eq!(lane.in_flight(), 0);
    }

    #[tokio::test]
    async fn failed_append_batch_keeps_the_unclean_shutdown_barrier() {
        let dir = tempfile::tempdir().unwrap();
        let (mut lane, mut rx) = lane(dir.path());
        std::fs::write(dir.path().join("jobs/1"), b"not a directory").unwrap();
        let (entered, started) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        assert!(lane.send(Request::Io {
            revision: 0,
            action: Box::new(move || {
                entered.send(()).unwrap();
                wait.recv().unwrap();
                Ok(Box::new(()) as IoValue)
            })
        }));
        started.await.unwrap();
        for revision in 1..=3 {
            assert!(lane.send(request(revision)));
        }
        release.send(()).unwrap();
        assert!(matches!(
            rx.recv().await.unwrap(),
            EngineMsg::IoFinished { revision: 0, .. }
        ));
        for _ in 1..=3 {
            let error = completion(&mut rx).await.result.unwrap_err();
            assert!(error.io_kind().is_some());
            assert_eq!(error.path(), Some(dir.path().join("jobs/1").as_path()));
        }
        // Repairing the filesystem does not make records behind a torn tail
        // replayable. A following batch retains the original failure.
        std::fs::remove_file(dir.path().join("jobs/1")).unwrap();
        assert!(lane.send(request(4)));
        assert!(completion(&mut rx).await.result.is_err());
        assert!(JobJournals::replay_job(dir.path(), JobId(1))
            .unwrap()
            .is_empty());
        // Sync alone cannot claim that failed appends were durably captured.
        assert!(lane.send(Request::Sync { revision: 5 }));
        assert!(completion(&mut rx).await.result.is_ok());
        assert!(lane.send(Request::Snapshot {
            revision: 6,
            doc: Default::default(),
            delegated: HashSet::new(),
            authoritative: true,
            guard: Some(Arc::new(|| false))
        }));
        assert!(completion(&mut rx).await.result.is_err());
        assert!(lane.send(request(7)));
        assert!(completion(&mut rx).await.result.is_err());
        let (reply, done) = oneshot::channel();
        assert!(lane.send(Request::Shutdown {
            doc: None,
            authoritative: true,
            guard: None,
            marker: UncleanMarker::new(dir.path(), "test"),
            volumes: crate::volumes::VolumeBook::load(dir.path(), "test"),
            reply
        }));
        assert!(!done.await.unwrap());
        lane.thread.take().unwrap().join().unwrap();
        assert_eq!(lane.in_flight(), 0);
    }

    #[tokio::test]
    async fn successful_snapshot_releases_a_failed_append_batch() {
        let dir = tempfile::tempdir().unwrap();
        let (mut lane, mut rx) = lane(dir.path());
        std::fs::write(dir.path().join("jobs/1"), b"not a directory").unwrap();
        assert!(lane.send(request(1)));
        assert!(completion(&mut rx).await.result.is_err());
        std::fs::remove_file(dir.path().join("jobs/1")).unwrap();
        assert!(lane.send(Request::Snapshot {
            revision: 2,
            doc: Default::default(),
            delegated: HashSet::new(),
            authoritative: true,
            guard: None
        }));
        assert!(completion(&mut rx).await.result.is_ok());
        assert!(lane.send(request(3)));
        assert!(completion(&mut rx).await.result.is_ok());
        assert_eq!(
            JobJournals::replay_job(dir.path(), JobId(1)).unwrap().len(),
            1
        );
        stop(&mut lane, dir.path()).await;
    }

    #[tokio::test]
    async fn worker_checkpoint_recovery_rotates_past_torn_and_retains_foreign_journals() {
        use std::io::Write;
        for (authoritative, delegated) in [(false, false), (true, true), (true, false)] {
            let dir = tempfile::tempdir().unwrap();
            let mut journal = JobJournals::open(dir.path(), "local").unwrap();
            journal.append(&record(JobId(1), 1)).unwrap();
            journal.sync().unwrap();
            let poisoned = std::fs::read_dir(dir.path().join("jobs/1"))
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path();
            std::fs::OpenOptions::new()
                .append(true)
                .open(&poisoned)
                .unwrap()
                .write_all(b"{torn")
                .unwrap();
            let retained = std::fs::read(&poisoned).unwrap();
            let mut foreign = JobJournals::open(dir.path(), "foreign").unwrap();
            foreign.append(&record(JobId(1), 10)).unwrap();
            foreign.sync().unwrap();
            // A real append failure establishes the lane's recovery fence. The
            // poisoned job remains cached/open, and worker saves cannot compact it.
            std::fs::write(dir.path().join("jobs/2"), b"not a directory").unwrap();
            let (tx, mut rx) = mpsc::channel(32);
            let mut lane = Persistence::start(
                journal,
                SnapshotStore::open(dir.path()).unwrap(),
                SnapshotStore::open(&dir.path().join("worker")).unwrap(),
                tx,
            );
            assert!(lane.send(Request::Append {
                revision: 1,
                record: record(JobId(2), 1),
                provenance: None
            }));
            assert!(completion(&mut rx).await.result.is_err());
            let mut state = QueueState::default();
            assert_eq!(
                state.admit_url(
                    "retained".into(),
                    "http://example.invalid/retained",
                    None,
                    0
                ),
                JobId(1)
            );
            assert!(lane.send(Request::Snapshot {
                revision: 2,
                doc: state.to_doc(),
                delegated: if delegated {
                    HashSet::from([JobId(1)])
                } else {
                    HashSet::new()
                },
                authoritative,
                guard: None
            }));
            assert!(completion(&mut rx).await.result.is_ok());
            assert!(lane.send(Request::Append {
                revision: 3,
                record: record(JobId(1), 3),
                provenance: None
            }));
            assert!(completion(&mut rx).await.result.is_ok());
            foreign.append(&record(JobId(1), 11)).unwrap();
            foreign.sync().unwrap();
            let replayed = JobJournals::replay_job(dir.path(), JobId(1)).unwrap();
            assert_eq!(
                replayed
                    .iter()
                    .map(|record| record.segment_number)
                    .collect::<HashSet<_>>(),
                HashSet::from([1, 3, 10, 11])
            );
            assert_eq!(
                std::fs::read(&poisoned).unwrap(),
                retained,
                "old torn evidence must remain untouched"
            );
            stop(&mut lane, dir.path()).await;
        }
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
        state
            .job_mut(job)
            .unwrap()
            .params
            .push(("opaque".into(), "metadata".repeat(160_000)));
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
        assert!(std::fs::read(dir.path().join("queue.json.tmp"))
            .unwrap()
            .starts_with(b"NZBDQZ02"));
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
