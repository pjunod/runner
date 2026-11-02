//! Native torrent acquisitions own a processing copy, independently of a seed donor.
use nzbd_types::{Job, JobId};
use serde::{Deserialize, Serialize};

pub const ACQUISITION_PARAM: &str = "*Torrent:Acquisition";
pub const SUBMISSION_PARAM: &str = "*Torrent:Submission";
pub const DEFAULTS_PARAM: &str = "*Torrent:ReservedDefaults";
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManagedTorrentDefaults {
    pub category: Option<String>,
    pub seed_policy: nzbd_types::SeedPolicy,
    pub payload_root: std::path::PathBuf,
}
pub fn reserved_defaults(job: &Job) -> Option<ManagedTorrentDefaults> {
    let raw = job
        .params
        .iter()
        .find(|(key, _)| key == DEFAULTS_PARAM)?
        .1
        .as_str();
    (raw.len() <= 32 * 1024)
        .then(|| serde_json::from_str(raw).ok())
        .flatten()
}
pub const SOURCE_READY_PARAM: &str = "*Torrent:SourceReady";
pub const ADMISSION_GENERATION_PARAM: &str = "*Torrent:AdmissionGeneration";
pub const METADATA_ATTEMPT_PARAM: &str = "*Torrent:MetadataAttempt";

pub fn is_acquisition(job: &Job) -> bool {
    job.kind == nzbd_types::JobKind::Torrent
        && job.torrent.is_none()
        && job
            .params
            .iter()
            .any(|(key, value)| key == ACQUISITION_PARAM && value == "1")
        && managed_token(&job.params).ok().flatten().is_some()
}
pub fn metadata_attempt(job: &Job) -> u32 {
    job.params
        .iter()
        .find(|(key, _)| key == METADATA_ATTEMPT_PARAM)
        .and_then(|(_, value)| value.parse().ok())
        .unwrap_or(0)
}
pub const DONOR_PARAM: &str = "*Torrent:Donor";
pub const INFO_HASH_PARAM: &str = "*Torrent:InfoHash";
pub const OWNED_SEED_PARAM: &str = "*Torrent:OwnedSeed";

pub fn donor(job: &Job) -> Option<JobId> {
    if !is_acquisition(job) {
        return None;
    }
    job.params
        .iter()
        .find(|(key, _)| key == DONOR_PARAM)
        .and_then(|(_, value)| value.parse().ok())
        .map(JobId)
}

pub fn managed_token(params: &[(String, String)]) -> Result<Option<&str>, crate::EngineError> {
    if !nzbd_types::has_delivery_intent(params) {
        return Ok(None);
    }
    let tokens = params
        .iter()
        .filter(|(key, _)| key == "monarr-transfer")
        .collect::<Vec<_>>();
    if tokens.len() != 1 || tokens[0].1.is_empty() || tokens[0].1.len() > 128 {
        return Err(crate::EngineError::RequestConflict(
            "native torrent Delivery requires exactly one nonempty monarr-transfer request token (max 128 bytes)".into(),
        ));
    }
    Ok(Some(&tokens[0].1))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedTorrentAdmission {
    pub id: JobId,
    pub donor_id: JobId,
    pub created: bool,
    pub seed_created: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedTorrentReservation {
    pub id: JobId,
    pub created: bool,
    pub pending: bool,
    pub source_ready: bool,
}

/// Hash the original request before URL/magnet metadata can change it.
pub fn submission_fingerprint(
    source: nzbd_types::TorrentSource,
    bytes: &[u8],
    opts: &crate::AddOpts,
) -> Result<String, crate::EngineError> {
    use sha2::{Digest, Sha256};
    let mut params = opts.params.clone();
    params.sort();
    let original = serde_json::to_vec(&(
        "native-torrent-admission-v1",
        source,
        format!("{:x}", Sha256::digest(bytes)),
        &opts.category,
        opts.priority,
        opts.paused,
        opts.seed_ratio_limit,
        opts.seed_time_limit_secs,
        opts.stop_seeding_on_complete,
        &opts.dupe,
        params,
    ))
    .map_err(|_| {
        crate::EngineError::RequestConflict(
            "native torrent submission contains unsupported options".into(),
        )
    })?;
    Ok(format!("{:x}", Sha256::digest(original)))
}

/// Reuse the existing durable opaque source cache with a separate identity.
/// The actual acquisition never carries *URL, so NZB restoration cannot see it.
pub(crate) fn blob_origin(job: &Job, metadata: bool) -> Result<Job, crate::EngineError> {
    if !is_acquisition(job) {
        return Err(crate::EngineError::RequestConflict(
            "not a native torrent acquisition".into(),
        ));
    }
    let generation = job
        .params
        .iter()
        .find(|(key, _)| key == ADMISSION_GENERATION_PARAM)
        .map(|(_, value)| value.clone())
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .ok_or_else(|| {
            crate::EngineError::RequestConflict("native torrent source generation missing".into())
        })?;
    let mut origin = job.clone();
    origin
        .params
        .retain(|(key, _)| key != "*URL" && key != "*URL:AdmissionGeneration");
    origin.params.push((
        "*URL".into(),
        if metadata {
            format!("native-torrent-metadata-{}", metadata_attempt(job))
        } else {
            "native-torrent-submission".into()
        },
    ));
    origin
        .params
        .push(("*URL:AdmissionGeneration".into(), generation));
    Ok(origin)
}

impl crate::EngineHandle {
    pub fn native_torrent_delivery_supported(&self) -> bool {
        !self.url_outbox_external && self.snapshot().queue_authority
    }
    pub async fn native_torrent_pending(
        &self,
        job: JobId,
    ) -> Result<Option<nzbd_state::PendingAdmission>, crate::EngineError> {
        let (reply, result) = tokio::sync::oneshot::channel();
        self.send(crate::owner::QueueCommand::NativeTorrentPending { job, reply })
            .await?;
        result.await.map_err(|_| crate::EngineError::Closed)
    }

    pub async fn reserve_managed_torrent_admission(
        &self,
        source: nzbd_types::TorrentSource,
        submission_sha256: String,
        opts: crate::AddOpts,
        defaults: ManagedTorrentDefaults,
    ) -> Result<ManagedTorrentReservation, crate::EngineError> {
        crate::validate_external_params(&opts.params)?;
        managed_token(&opts.params)?.ok_or_else(|| {
            crate::EngineError::RequestConflict(
                "native torrent admission requires Delivery intent".into(),
            )
        })?;
        let (reply, result) = tokio::sync::oneshot::channel();
        self.send(crate::owner::QueueCommand::ReserveManagedTorrentAdmission {
            source,
            submission_sha256,
            opts,
            defaults,
            reply,
        })
        .await?;
        result.await.map_err(|_| crate::EngineError::Closed)?
    }
    pub async fn confirm_managed_torrent_source(
        &self,
        job: JobId,
        submission_sha256: String,
    ) -> Result<bool, crate::EngineError> {
        self.roundtrip_bool(
            |reply| crate::owner::QueueCommand::ConfirmManagedTorrentSource {
                job,
                submission_sha256,
                reply,
            },
        )
        .await
    }
    pub async fn native_torrent_blob(
        &self,
        job: &Job,
        metadata: bool,
    ) -> Result<Option<Vec<u8>>, crate::EngineError> {
        let origin = blob_origin(job, metadata)?;
        let store = self.url_resolutions.clone();
        tokio::task::spawn_blocking(move || store.source(&origin))
            .await
            .map_err(|_| crate::EngineError::Closed)?
            .map_err(|error| crate::EngineError::Lifecycle(error.to_string()))
    }
    pub async fn cache_native_torrent_blob(
        &self,
        job: &Job,
        metadata: bool,
        bytes: Vec<u8>,
    ) -> Result<Vec<u8>, crate::EngineError> {
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(crate::EngineError::RequestConflict(
                "native torrent source exceeds supported 16MiB limit".into(),
            ));
        }
        let origin = blob_origin(job, metadata)?;
        let store = self.url_resolutions.clone();
        tokio::task::spawn_blocking(move || store.cache_source(&origin, &bytes))
            .await
            .map_err(|_| crate::EngineError::Closed)?
            .map_err(|error| crate::EngineError::Lifecycle(error.to_string()))
    }
    pub async fn retire_native_torrent_blobs(&self, job: &Job) -> Result<(), crate::EngineError> {
        if !is_acquisition(job) {
            return Ok(());
        }
        let source = blob_origin(job, false)?;
        let metadata = blob_origin(job, true)?;
        let store = self.url_resolutions.clone();
        tokio::task::spawn_blocking(move || {
            store.cancel(&source)?;
            store.cancel(&metadata)
        })
        .await
        .map_err(|_| crate::EngineError::Closed)?
        .map_err(|error| crate::EngineError::Lifecycle(error.to_string()))
    }
}
