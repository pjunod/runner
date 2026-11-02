//! Read-only observations supplied by the cluster's bounded proof lane.
use std::collections::BTreeMap;
#[derive(Clone, Default)]
pub struct ControlPendingObservations(pub BTreeMap<u32, serde_json::Value>);

pub(crate) fn render(
    job: &nzbd_engine::JobSummary,
    pending: bool,
    observations: Option<&ControlPendingObservations>,
) -> serde_json::Value {
    let mut value = serde_json::to_value(job).unwrap_or_else(|_| serde_json::json!({}));
    if pending {
        let observation=observations.and_then(|observations|observations.0.get(&job.id.0)).cloned().unwrap_or_else(||serde_json::json!({
            "jobId":job.id.0,"state":"pending","owner":null,
            "error":"Waiting for exact source custody proof or durable cluster control acceptance; Pause and removal remain available",
            "nextRetryAtMs":null
        }));
        value
            .as_object_mut()
            .unwrap()
            .insert("controlPending".into(), observation);
    }
    value
}
