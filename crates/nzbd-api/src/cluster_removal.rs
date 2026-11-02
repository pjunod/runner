//! Process-local context installed only by cluster control middleware.
//! HTTP clients cannot request deferred deletion through a header or body.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Clone, Default)]
pub struct ClusterRemovalContext(Arc<Mutex<BTreeMap<u32, bool>>>);
impl ClusterRemovalContext {
    /// Called by an authenticated handler only after this removal succeeded.
    pub fn record(&self, job: u32, delete_files: bool) {
        self.0.lock().unwrap().insert(job, delete_files);
    }
    pub fn confirmed(&self) -> BTreeMap<u32, bool> {
        self.0.lock().unwrap().clone()
    }
}
