//! Provenance for the cluster's bounded reconciliation of automatic controls.
//! The cluster compares business intent and source custody against Raft; this
//! marker records only the exact control/status transition made by the actor.
use nzbd_state::artifacts::{Artifact, SourceGeneration};
use nzbd_types::{Job, JobControl, JobId, JobStatus};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

pub const AUTOMATIC_CONTROL_PARAM: &str = "*Cluster:AutomaticControl";
pub const MAX_AUTOMATIC_CONTROL_BYTES: usize = 128 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutomaticControlTransition {
    pub identity: String,
    pub job: JobId,
    pub base_control: Option<JobControl>,
    pub base_status: JobStatus,
    pub result_control: JobControl,
    pub result_status: JobStatus,
    pub storage_instance_id: String,
    pub source: Option<SourceGeneration>,
    pub source_path: Option<PathBuf>,
}
impl AutomaticControlTransition {
    fn identity(&self) -> String {
        format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    1u8,
                    self.job,
                    &self.base_control,
                    self.base_status,
                    &self.result_control,
                    self.result_status,
                    &self.storage_instance_id,
                    &self.source,
                    &self.source_path
                ))
                .expect("bounded control transition serializes")
            )
        )
    }
    fn valid_identity(&self) -> bool {
        !self.storage_instance_id.is_empty()
            && self.storage_instance_id.len() <= 128
            && self.identity == self.identity()
    }
}
fn read(job: &Job) -> Option<AutomaticControlTransition> {
    let mut values = job
        .params
        .iter()
        .filter(|(key, _)| key == AUTOMATIC_CONTROL_PARAM);
    let (_, value) = values.next()?;
    if value.len() > MAX_AUTOMATIC_CONTROL_BYTES || values.next().is_some() {
        return None;
    }
    let marker: AutomaticControlTransition = serde_json::from_str(value).ok()?;
    (marker.job == job.id && marker.valid_identity()).then_some(marker)
}
/// A marker is actionable only while its exact result is still current.
pub fn transition(job: &Job) -> Option<AutomaticControlTransition> {
    let marker = read(job)?;
    (job.control().as_ref() == Some(&marker.result_control) && job.status == marker.result_status)
        .then_some(marker)
}
pub(crate) fn pending(job: &Job) -> bool {
    job.params
        .iter()
        .any(|(key, _)| key == AUTOMATIC_CONTROL_PARAM)
}
pub(crate) fn invalidate_changed(job: &mut Job) {
    if read(job).is_some_and(|marker| {
        job.control().as_ref() != Some(&marker.result_control) || job.status != marker.result_status
    }) {
        job.params.retain(|(key, _)| key != AUTOMATIC_CONTROL_PARAM);
    }
}
pub(crate) fn stamp(
    before: &Job,
    result: &mut Job,
    installation: &str,
    source: Option<&Artifact>,
) -> Result<(), String> {
    if pending(before) {
        return Err("previous automatic control is awaiting durable cluster acknowledgment".into());
    }
    let control = result
        .control()
        .ok_or("automatic transition has no result control")?;
    let mut marker = AutomaticControlTransition {
        identity: String::new(),
        job: result.id,
        base_control: before.control(),
        base_status: before.status,
        result_control: control,
        result_status: result.status,
        storage_instance_id: installation.into(),
        source: source.map(|source| SourceGeneration {
            artifact: source.id.clone(),
            generation: source.generation.clone(),
        }),
        source_path: source.map(|source| source.path.clone()),
    };
    marker.identity = marker.identity();
    let encoded = serde_json::to_string(&marker).map_err(|error| error.to_string())?;
    if encoded.len() > MAX_AUTOMATIC_CONTROL_BYTES {
        return Err("automatic control provenance exceeds supported 128KiB bound".into());
    }
    result
        .params
        .retain(|(key, _)| key != AUTOMATIC_CONTROL_PARAM);
    result
        .params
        .push((AUTOMATIC_CONTROL_PARAM.into(), encoded));
    Ok(())
}

impl crate::EngineHandle {
    /// Clear only the exact transition already accepted by the cluster.
    /// Absent/changed markers return false without changing current intent.
    pub async fn ack_automatic_control(
        &self,
        job: JobId,
        identity: String,
    ) -> Result<bool, crate::EngineError> {
        if identity.len() != 64 || !identity.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Ok(false);
        }
        self.roundtrip_bool(|reply| crate::owner::QueueCommand::AckAutomaticControl {
            job,
            identity,
            reply,
        })
        .await
    }
}
