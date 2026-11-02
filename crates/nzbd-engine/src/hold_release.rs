//! The short owner-side half of a prepared hold release.
use crate::imports::ImportAuthority;
use nzbd_state::artifacts::{Artifact, Inventory, WorkerControlContext};
use nzbd_types::Job;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
struct SourceVersion {
    id: String,
    generation: String,
    revision: u64,
    path: PathBuf,
}
impl From<&Artifact> for SourceVersion {
    fn from(source: &Artifact) -> Self {
        Self {
            id: source.id.clone(),
            generation: source.generation.clone(),
            revision: source.revision,
            path: source.path.clone(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct HoldReleaseGuard(Arc<HoldReleaseProof>);

#[derive(Debug, Default)]
struct HoldReleaseProof {
    worker_checked: bool,
    authority: Option<ImportAuthority>,
    context: Option<WorkerControlContext>,
    source_bound: bool,
    source: Option<SourceVersion>,
}
impl HoldReleaseGuard {
    pub(crate) fn prepare(
        record: &Job,
        source: Option<&Artifact>,
        authority: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    ) -> Self {
        let worker_checked = authority.is_some();
        Self::build(record, source, authority, worker_checked)
    }
    pub(crate) fn prepare_queue_owner(
        record: &Job,
        source: Option<&Artifact>,
        authority: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    ) -> Self {
        Self::build(record, source, authority, false)
    }
    fn build(
        record: &Job,
        source: Option<&Artifact>,
        authority: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
        worker_checked: bool,
    ) -> Self {
        Self(Arc::new(HoldReleaseProof {
            worker_checked,
            authority: authority.map(ImportAuthority),
            context: WorkerControlContext::for_job(record),
            source_bound: true,
            source: source.map(SourceVersion::from),
        }))
    }
    pub(crate) fn rebind_source(&self, record: &Job, source: Option<&Artifact>) -> Self {
        Self::build(
            record,
            source,
            self.authority_callback(),
            self.0.worker_checked,
        )
    }
    pub(crate) fn worker_checked(&self) -> bool {
        self.0.worker_checked
    }
    pub(crate) fn checked(&self) -> bool {
        self.0.authority.is_some()
    }
    pub(crate) fn authority_callback(&self) -> Option<Arc<dyn Fn() -> bool + Send + Sync>> {
        self.0
            .authority
            .as_ref()
            .map(|authority| authority.0.clone())
    }
    pub(crate) fn live(&self) -> bool {
        self.0.authority.as_ref().is_none_or(ImportAuthority::valid)
    }
    pub(crate) fn matches(&self, inventory: &Inventory, current: &Job, persistent: bool) -> bool {
        if !self.live()
            || ((self.0.source_bound || self.0.authority.is_some())
                && self.0.context != WorkerControlContext::for_job(current))
        {
            return false;
        }
        if self.0.source_bound {
            let Ok(source) = inventory.for_job(current.id.0) else {
                return false;
            };
            if source.as_ref().map(SourceVersion::from) != self.0.source {
                return false;
            }
        }
        if WorkerControlContext::for_job(current).is_some() && !self.0.worker_checked {
            return false;
        }
        if persistent {
            return true;
        }
        // A durable old context is a checkpoint, not evidence that its lease
        // is still live. The execution lane supplies the live grant check.
        if !self.0.worker_checked || self.0.authority.is_none() {
            return false;
        }
        let Some(context) = &self.0.context else {
            return false;
        };
        inventory
            .worker_control_for_job_metadata(current, context)
            .ok()
            .flatten()
            .is_some_and(|checkpoint| {
                current.control().is_some_and(|control| {
                    checkpoint.control.instance == control.instance
                        && checkpoint.control.revision == control.revision
                })
            })
    }
}
