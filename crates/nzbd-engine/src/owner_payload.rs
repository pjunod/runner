//! Bounded payload I/O, independent from ordered state persistence.
//! A syscall retains its root slot and custody until its actual thread exits.
use super::*;

pub(super) struct PayloadWork {
    root: PathBuf,
    revision: u64,
    action: Box<dyn FnOnce() -> IoResult + Send>,
}
const PAYLOAD_WORKERS: usize = 2;
const PAYLOAD_QUEUE: usize = 64;

impl Owner {
    pub(super) fn payload_capacity_available(&self) -> bool {
        !self.cancel.is_cancelled() && self.payload_waiting.len() < PAYLOAD_QUEUE
    }

    pub(super) fn payload_recovery_capacity_available(&self) -> bool {
        // Metadata lookups already in flight will also need payload slots.
        // Count those reservations conservatively before admitting more work.
        self.payload_capacity_available()
            && self.allocating_jobs.len() + self.payload_waiting.len() < PAYLOAD_QUEUE
    }

    pub(super) fn payload_then<T: Send + 'static>(
        &mut self,
        root: PathBuf,
        action: impl FnOnce() -> Result<T, nzbd_state::StateError> + Send + 'static,
        continuation: impl FnOnce(&mut Owner, Result<T, nzbd_state::StateError>) + Send + 'static,
    ) {
        if !self.payload_capacity_available() {
            continuation(
                self,
                Err(nzbd_state::StateError::Busy(
                    "payload lane capacity unavailable; operation remains unadmitted",
                )),
            );
            return;
        }
        let revision = fresh_epoch();
        self.io_callbacks.insert(
            revision,
            Box::new(move |owner, result| {
                let result = result.and_then(|value| {
                    value.downcast::<T>().map(|value| *value).map_err(|_| {
                        nzbd_state::StateError::Corrupt("payload response type mismatch".into())
                    })
                });
                continuation(owner, result);
            }),
        );
        self.payload_waiting.push_back(PayloadWork {
            root,
            revision,
            action: Box::new(move || {
                action().map(|value| Box::new(value) as Box<dyn std::any::Any + Send>)
            }),
        });
        self.start_payload_work();
    }

    pub(super) fn start_payload_work(&mut self) {
        if self.cancel.is_cancelled() {
            return;
        }
        while self.payload_active.len() < PAYLOAD_WORKERS {
            let Some(index) = self
                .payload_waiting
                .iter()
                .position(|work| !self.payload_active.contains(&work.root))
            else {
                break;
            };
            let work = self.payload_waiting.remove(index).unwrap();
            let root = work.root;
            let revision = work.revision;
            self.payload_active.insert(root.clone());
            let (reply, result) = oneshot::channel();
            let worker = std::thread::Builder::new()
                .name(format!("payload-{revision}"))
                .spawn(move || {
                    let result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(work.action))
                            .unwrap_or_else(|_| {
                                Err(nzbd_state::StateError::Corrupt(
                                    "payload worker panicked".into(),
                                ))
                            });
                    let _ = reply.send(result);
                });
            match worker {
                Ok(worker) => {
                    let engine = self.engine_tx.clone();
                    self.tracker.spawn(async move {
                        let result = result.await.unwrap_or_else(|_| {
                            Err(nzbd_state::StateError::Corrupt(
                                "payload worker returned no result".into(),
                            ))
                        });
                        let result = if worker.join().is_err() {
                            Err(nzbd_state::StateError::Corrupt(
                                "payload worker failed while stopping".into(),
                            ))
                        } else {
                            result
                        };
                        let _ = engine
                            .send(EngineMsg::PayloadFinished {
                                root,
                                revision,
                                result,
                            })
                            .await;
                    });
                }
                Err(error) => {
                    self.payload_active.remove(&root);
                    self.io_finished(
                        revision,
                        Err(nzbd_state::StateError::Corrupt(format!(
                            "start payload worker: {error}"
                        ))),
                    );
                }
            }
        }
    }

    pub(super) fn payload_finished(&mut self, root: PathBuf, revision: u64, result: IoResult) {
        self.payload_active.remove(&root);
        self.io_finished(revision, result);
        self.start_payload_work();
        // Completion is the capacity wakeup. Pending durable rows should not
        // wait for a timer or be converted into allocation/corruption holds.
        self.retry_payload_recoveries();
        self.start_verifications();
        self.bump_epoch();
    }
    pub(super) fn cancel_payload_work(&mut self) {
        while let Some(work) = self.payload_waiting.pop_front() {
            self.io_finished(
                work.revision,
                Err(nzbd_state::StateError::Busy(
                    "payload operation cancelled before admission",
                )),
            );
        }
    }
}
