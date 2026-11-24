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
    let mut marker: AutomaticControlTransition = serde_json::from_str(value).ok()?;
    if marker.job != job.id || !marker.valid_identity() {
        return None;
    }
    // Every marker records a hold as `Paused` with its held control (0.5.0
    // stamps it so, and so does this build — see `stamp`); the rows it is
    // compared with load as `Held`. The identity hashes what was stamped,
    // so the statuses are read as the projection after it is checked, not
    // rewritten in the param. Markers live only until the cluster
    // acknowledges them.
    let held = |control: Option<&JobControl>| {
        control.is_some_and(|c| c.lifecycle == "held" || c.version != 1)
    };
    if marker.base_status == JobStatus::Paused && held(marker.base_control.as_ref()) {
        marker.base_status = JobStatus::Held;
    }
    if marker.result_status == JobStatus::Paused && held(Some(&marker.result_control)) {
        marker.result_status = JobStatus::Held;
    }
    Some(marker)
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
    // Stamped in the 0.5.0 form: `Held` as `Paused`, the held control beside
    // it carrying the hold. The identity hashes the statuses, so a 0.5.0
    // node (rolled back, or not yet upgraded) recomputes the same identity
    // and can still read, apply or drop the marker; `read` restores `Held`
    // from the control. Stamping `held` would leave a marker 0.5.0 sees as
    // pending but cannot read — a job nothing can move (review #297 r2-1).
    let stamped = |status: JobStatus| {
        if status == JobStatus::Held {
            JobStatus::Paused
        } else {
            status
        }
    };
    let mut marker = AutomaticControlTransition {
        identity: String::new(),
        job: result.id,
        base_control: before.control(),
        base_status: stamped(before.status),
        result_control: control,
        result_status: stamped(result.status),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn job(status: JobStatus) -> Job {
        Job {
            id: JobId(5),
            kind: nzbd_types::JobKind::Nzb,
            name: "job".into(),
            dir_name: "job".into(),
            name_provisional: false,
            delivery_lifecycle: false,
            queued_at_unix: 0,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: nzbd_types::DupeInfo::default(),
            params: Vec::new(),
            files: Vec::new(),
            totals: nzbd_types::JobTotals::default(),
            status,
            torrent: None,
            stages: Vec::new(),
        }
    }

    fn control(revision: &str, lifecycle: &str) -> JobControl {
        JobControl {
            expiry_pending: false,
            version: 1,
            revision: revision.into(),
            lifecycle: lifecycle.into(),
            cause: "capacity".into(),
            stage: "download_write".into(),
            retry_policy: "scheduled_retry".into(),
            message: String::new(),
            instance: "i".into(),
            previous_status: Some(JobStatus::Downloading),
            manual_pause: false,
            retry_attempt: 1,
            next_retry_at_unix: None,
            blocked_since_unix: None,
            released_at_unix: None,
            deadline_in_s: None,
            deadline_unix: None,
        }
    }

    /// Review #297 r2-1: a marker this build stamps on a hold validates on a
    /// 0.5.0 node (which hashes `paused`) and reads back as `Held` here.
    #[test]
    fn a_held_marker_is_stamped_in_the_050_form_and_reads_back_held() {
        let mut before = job(JobStatus::Held);
        before.set_control(&control("1", "held"));
        let mut result = job(JobStatus::Held);
        result.set_control(&control("2", "held"));
        stamp(&before, &mut result, "installation", None).unwrap();
        let (_, raw) = result
            .params
            .iter()
            .find(|(key, _)| key == AUTOMATIC_CONTROL_PARAM)
            .unwrap();
        let wire: serde_json::Value = serde_json::from_str(raw).unwrap();
        assert_eq!(wire["baseStatus"], "paused");
        assert_eq!(wire["resultStatus"], "paused");
        // What a 0.5.0 node decodes: the stamped statuses, as written.
        let as_050: AutomaticControlTransition = serde_json::from_str(raw).unwrap();
        assert!(
            as_050.valid_identity(),
            "0.5.0 recomputes the same identity"
        );
        assert_eq!(as_050.result_status, JobStatus::Paused);
        // This build: valid, current, and projected back to `Held`.
        let marker = transition(&result).expect("the marker is current");
        assert_eq!(marker.base_status, JobStatus::Held);
        assert_eq!(marker.result_status, JobStatus::Held);
        // A release is not a hold: its status is stamped as it is.
        let mut released = job(JobStatus::Queued);
        released.set_control(&control("3", "running"));
        let mut released_before = result.clone();
        released_before
            .params
            .retain(|(key, _)| key != AUTOMATIC_CONTROL_PARAM);
        stamp(&released_before, &mut released, "installation", None).unwrap();
        let marker = transition(&released).unwrap();
        assert_eq!(marker.base_status, JobStatus::Held);
        assert_eq!(marker.result_status, JobStatus::Queued);
    }
}
