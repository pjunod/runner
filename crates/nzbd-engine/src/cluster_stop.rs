//! Exact payload-writer stop/join. The cluster caller separately joins PP or
//! assembly operations and proves removal authority before disposing any byte.
use crate::{owner::QueueCommand, writer::WriterHandle, EngineError, EngineHandle};
use nzbd_state::artifacts::{
    same_worker_operation, Artifact, SourceGeneration, WorkerControlContext, WorkerStopRequest,
    WorkerStopTicket, WorkerStoppedReceipt,
};
use nzbd_types::Job;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub(crate) struct WriterWatch {
    pub(crate) stop: tokio_util::sync::CancellationToken,
    pub(crate) stopped: tokio::sync::watch::Receiver<bool>,
}
#[derive(Clone, Debug)]
pub(crate) struct StopPreparation {
    pub(crate) sources: Vec<SourceGeneration>,
    pub(crate) writers: Vec<WriterWatch>,
}
#[derive(Default)]
pub(crate) struct WriterStops {
    work: HashMap<String, Tracked>,
}
struct Tracked {
    context: WorkerControlContext,
    sources: Vec<SourceGeneration>,
    writers: Vec<WriterWatch>,
}
fn key(context: &WorkerControlContext) -> String {
    serde_json::to_string(&(
        &context.lease_id,
        &context.job_incarnation,
        context.job_revision,
        context.control_revision,
    ))
    .expect("grant identity serializes")
}
impl WriterStops {
    pub(crate) fn observe(
        &mut self,
        job: &Job,
        source: Option<&Artifact>,
        writer: Option<&WriterHandle>,
    ) {
        let Some(context) = WorkerControlContext::for_job(job) else {
            return;
        };
        let work = self.work.entry(key(&context)).or_insert_with(|| Tracked {
            context,
            sources: Vec::new(),
            writers: Vec::new(),
        });
        if let Some(source) = source {
            let source = SourceGeneration {
                artifact: source.id.clone(),
                generation: source.generation.clone(),
            };
            if !work.sources.contains(&source) {
                work.sources.push(source);
            }
        }
        work.writers.retain(|writer| !*writer.stopped.borrow());
        if let Some(writer) = writer.filter(|writer| !*writer.stopped.borrow()) {
            work.writers.push(WriterWatch {
                stop: writer.stop.clone(),
                stopped: writer.stopped.clone(),
            });
        }
    }
    pub(crate) fn known(&self, context: &WorkerControlContext) -> bool {
        self.work.contains_key(&key(context))
    }
    pub(crate) fn prepare(&mut self, ticket: &WorkerStopTicket) -> Result<StopPreparation, String> {
        let Some(work) = self.work.get_mut(&key(&ticket.request.context)) else {
            return if ticket.never_started
                || ticket.recovered_session
                || (execution_joined && ticket.request.start_ordered)
            {
                Ok(StopPreparation {
                    sources: Vec::new(),
                    writers: Vec::new(),
                })
            } else {
                Err(
                    "grant disappeared without joined writer evidence; source retained for review"
                        .into(),
                )
            };
        };
        if !same_worker_operation(&work.context, &ticket.request.context) || ticket.never_started {
            return Err(
                "observed work contradicts the recorded start ordering; retain for review".into(),
            );
        }
        if work.sources.len() > 32 {
            return Err("worker source set exceeds supported stop bound".into());
        }
        work.writers.retain(|writer| !*writer.stopped.borrow());
        for writer in &work.writers {
            writer.stop.cancel();
        }
        Ok(StopPreparation {
            sources: work.sources.clone(),
            writers: work.writers.clone(),
        })
    }
    pub(crate) fn forget_joined(&mut self, context: &WorkerControlContext) -> bool {
        let key = key(context);
        if self
            .work
            .get(&key)
            .is_some_and(|work| work.writers.iter().any(|writer| !*writer.stopped.borrow()))
        {
            return false;
        }
        self.work.remove(&key);
        true
    }
}
impl EngineHandle {
    /// Durably fence all late starts first, then join actual payload-writer
    /// acknowledgments. Queue absence is never stop evidence. This receipt
    /// covers Rust payload writers only, not external PP subprocesses.
    pub async fn join_cluster_writers(
        &self,
        request: WorkerStopRequest,
        execution_joined: bool,
    ) -> Result<WorkerStoppedReceipt, EngineError> {
        let inventory = self.artifacts();
        let ticket = tokio::task::spawn_blocking(move || inventory.prepare_worker_stop(&request))
            .await
            .map_err(|e| EngineError::Lifecycle(e.to_string()))?
            .map_err(|e| EngineError::Lifecycle(e.to_string()))?;
        if let Some(receipt) = ticket.receipt {
            return Ok(receipt);
        }
        let sources = loop {
            let (reply, receive) = tokio::sync::oneshot::channel();
            self.send(QueueCommand::PrepareClusterWritersStop {
                ticket: ticket.clone(),
                execution_joined,
                reply,
            })
            .await?;
            let prepared = receive
                .await
                .map_err(|_| EngineError::Closed)?
                .map_err(EngineError::Lifecycle)?;
            if prepared.writers.is_empty() {
                break prepared.sources;
            }
            for mut writer in prepared.writers {
                if !*writer.stopped.borrow() {
                    writer
                        .stopped
                        .wait_for(|stopped| *stopped)
                        .await
                        .map_err(|_| {
                            EngineError::Lifecycle(
                                "writer stop acknowledgment was lost; retain source for review"
                                    .into(),
                            )
                        })?;
                }
            }
            // Recheck on the actor after actual joins. The durable fence
            // prevents any old-context import/allocation from reopening it.
        };
        let inventory = self.artifacts();
        let commit = ticket.clone();
        let receipt =
            tokio::task::spawn_blocking(move || inventory.commit_worker_stopped(&commit, &sources))
                .await
                .map_err(|e| EngineError::Lifecycle(e.to_string()))?
                .map_err(|e| EngineError::Lifecycle(e.to_string()))?;
        self.roundtrip_bool(|reply| QueueCommand::ForgetClusterWriterStops {
            context: ticket.request.context,
            reply,
        })
        .await?;
        Ok(receipt)
    }
}
