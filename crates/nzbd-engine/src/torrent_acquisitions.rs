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
pub const SEED_GATE_PARAM: &str = "*Torrent:AcquisitionGate";
pub const COPY_COMPLETE_PARAM: &str = "*Torrent:CopyComplete";
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
        &opts.torrent_tags,
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
    pub async fn claim_native_failure_cleanup(
        &self,
        job: JobId,
    ) -> Result<Option<Job>, crate::EngineError> {
        let (reply, result) = tokio::sync::oneshot::channel();
        self.send(crate::owner::QueueCommand::ClaimNativeFailureCleanup { job, reply })
            .await?;
        result.await.map_err(|_| crate::EngineError::Closed)
    }

    /// End a native torrent acquisition as a typed failed terminal
    /// (contract §2.1/§2.2): its `job_terminals` row, history and
    /// `job_pp_finished` come from the same chain as every other terminal.
    #[allow(clippy::too_many_arguments)]
    pub async fn fail_native_torrent(
        &self,
        job: JobId,
        donor: Option<JobId>,
        reason: nzbd_types::FailureReason,
        fetch_cause: Option<nzbd_types::FetchCause>,
        stage: String,
        message: String,
        cleanup_complete: bool,
    ) -> Result<bool, crate::EngineError> {
        self.roundtrip_bool(|reply| crate::owner::QueueCommand::FailNativeTorrent {
            job,
            donor,
            reason,
            fetch_cause,
            stage,
            message,
            cleanup_complete,
            reply,
        })
        .await
    }

    pub fn native_torrent_delivery_supported(&self) -> bool {
        cfg!(unix) && !self.url_outbox_external && self.snapshot().queue_authority
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
        if !self.native_torrent_delivery_supported() {
            return Err(crate::EngineError::RequestConflict(
                "native torrent Deliveries require standalone Runner".into(),
            ));
        }
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
        let pinned = self.snapshot().native_descriptor_references.clone();
        tokio::task::spawn_blocking(move || -> nzbd_state::artifacts::Result<()> {
            for origin in [source, metadata] {
                if !pinned.contains(&nzbd_state::artifacts::url_source_identity(&origin)?) {
                    store.cancel(&origin)?;
                }
            }
            Ok(())
        })
        .await
        .map_err(|_| crate::EngineError::Closed)?
        .map_err(|error| crate::EngineError::Lifecycle(error.to_string()))
    }
}

pub const SEED_RETIREMENT_PARAM: &str = "*Torrent:SeedRetirement";
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedSeedRetirement {
    pub identity: String,
    pub storage_instance_id: String,
    pub reason: nzbd_state::artifacts::TorrentSeedRetirementReason,
    pub retry_revision: String,
    pub keep_blocked_revision: Option<u64>,
    pub keep_blocked_control_revision: Option<String>,
    pub source: nzbd_state::artifacts::SourceGeneration,
    pub payload_root: std::path::PathBuf,
    pub phase: String,
}
pub fn seed_retirement(job: &Job) -> Option<ManagedSeedRetirement> {
    let raw = job
        .params
        .iter()
        .find(|(key, _)| key == SEED_RETIREMENT_PARAM)?
        .1
        .as_str();
    if raw.len() > 16 * 1024 {
        return None;
    }
    let marker: ManagedSeedRetirement = serde_json::from_str(raw).ok()?;
    (marker.identity == marker.expected_identity(job.id)?
        && marker.storage_instance_id.len() <= 128
        && marker.retry_revision.len() <= 20
        && marker.retry_revision.parse::<u64>().is_ok()
        && matches!(marker.phase.as_str(), "requested" | "detached" | "deleted"))
    .then_some(marker)
}
impl ManagedSeedRetirement {
    pub fn expected_identity(&self, job: JobId) -> Option<String> {
        use sha2::{Digest, Sha256};
        Some(format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    1u8,
                    job,
                    &self.storage_instance_id,
                    &self.source,
                    &self.payload_root,
                    &self.reason
                ))
                .ok()?
            )
        ))
    }
    pub fn intent(&self) -> nzbd_state::artifacts::TorrentSeedRetirementIntent {
        nzbd_state::artifacts::TorrentSeedRetirementIntent {
            identity: self.identity.clone(),
            reason: self.reason.clone(),
            retry_revision: self.retry_revision.clone(),
            source_path: self.payload_root.clone(),
        }
    }
}

pub fn is_managed_seed(job: &Job) -> bool {
    job.torrent.is_some()
        && job
            .params
            .iter()
            .any(|(key, value)| key == OWNED_SEED_PARAM && value == "1")
}
pub fn preparation_hold(job: &Job, control: &nzbd_types::JobControl) -> bool {
    (is_acquisition(job) && matches!(control.stage.as_str(), "torrent_copy" | "torrent_metadata"))
        || (is_managed_seed(job)
            && matches!(
                control.stage.as_str(),
                "torrent_attach" | "torrent_seed_retire" | "torrent_seed_forget"
            ))
}
impl crate::EngineHandle {
    pub async fn sync_managed_seed_keep(&self, donor: JobId) -> Result<bool, crate::EngineError> {
        self.roundtrip_bool(|reply| crate::owner::QueueCommand::SyncManagedSeedKeep {
            donor,
            reply,
        })
        .await
    }
    pub async fn begin_managed_seed_retirement(
        &self,
        job: JobId,
    ) -> Result<bool, crate::EngineError> {
        self.roundtrip_bool(
            |reply| crate::owner::QueueCommand::BeginManagedSeedRetirement { job, reply },
        )
        .await
    }
    pub async fn complete_managed_seed_retirement(
        &self,
        job: JobId,
        identity: String,
    ) -> Result<bool, crate::EngineError> {
        self.roundtrip_bool(
            |reply| crate::owner::QueueCommand::CompleteManagedSeedRetirement {
                job,
                identity,
                reply,
            },
        )
        .await
    }
}

pub(crate) fn source_dependency(job: &Job) -> Option<JobId> {
    let donor = donor(job)?;
    if job
        .params
        .iter()
        .any(|(key, value)| key == COPY_COMPLETE_PARAM && value == "1")
    {
        return None;
    }
    let failed = job
        .params
        .iter()
        .any(|(key, value)| key == "*Torrent:SourceFailed" && value == "1");
    let keep = job
        .params
        .iter()
        .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1");
    (!failed || keep).then_some(donor)
}

pub const DESCRIPTOR_PARAM: &str = "*Torrent:Descriptor";
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NativeDescriptorRef {
    pub job: JobId,
    pub queued_at_unix: i64,
    pub request_token: String,
    pub generation: String,
    pub metadata: bool,
    pub metadata_attempt: u32,
    pub sha256: String,
}
impl NativeDescriptorRef {
    pub fn prepare(job: &Job, metadata: bool, bytes: &[u8]) -> Result<Self, crate::EngineError> {
        use sha2::{Digest, Sha256};
        let origin = blob_origin(job, metadata)?;
        Ok(Self {
            job: job.id,
            queued_at_unix: job.queued_at_unix,
            request_token: managed_token(&job.params)?
                .ok_or_else(|| {
                    crate::EngineError::RequestConflict(
                        "native descriptor has no request token".into(),
                    )
                })?
                .into(),
            generation: origin
                .params
                .iter()
                .find(|(key, _)| key == "*URL:AdmissionGeneration")
                .unwrap()
                .1
                .clone(),
            metadata,
            metadata_attempt: metadata_attempt(job),
            sha256: format!("{:x}", Sha256::digest(bytes)),
        })
    }
    pub fn origin(&self, template: &Job) -> Result<Job, crate::EngineError> {
        if self.job.0 == 0
            || self.generation.is_empty()
            || self.generation.len() > 256
            || self.request_token.is_empty()
            || self.request_token.len() > 128
            || self.sha256.len() != 64
            || !self.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(crate::EngineError::RequestConflict(
                "invalid native descriptor reference".into(),
            ));
        }
        let mut origin = template.clone();
        origin.id = self.job;
        origin.queued_at_unix = self.queued_at_unix;
        origin.kind = nzbd_types::JobKind::Torrent;
        origin.files.clear();
        origin.torrent = None;
        origin.params = vec![
            ("monarr-transfer".into(), self.request_token.clone()),
            ("*URL:AdmissionGeneration".into(), self.generation.clone()),
            (
                "*URL".into(),
                if self.metadata {
                    format!("native-torrent-metadata-{}", self.metadata_attempt)
                } else {
                    "native-torrent-submission".into()
                },
            ),
        ];
        Ok(origin)
    }
    pub fn source_identity(&self, template: &Job) -> Result<String, crate::EngineError> {
        nzbd_state::artifacts::url_source_identity(&self.origin(template)?)
            .map_err(|error| crate::EngineError::Lifecycle(error.to_string()))
    }
}
pub fn descriptor_reference(job: &Job) -> Option<NativeDescriptorRef> {
    let raw = job
        .params
        .iter()
        .find(|(key, _)| key == DESCRIPTOR_PARAM)?
        .1
        .as_str();
    if raw.len() > 4096 {
        return None;
    }
    let reference: NativeDescriptorRef = serde_json::from_str(raw).ok()?;
    if job.torrent.as_ref()?.metadata_file
        != std::path::Path::new(&format!(
            "torrents/native-cache/admission-{}.torrent",
            reference.job.0
        ))
    {
        return None;
    }
    reference.origin(job).ok()?;
    Some(reference)
}
impl crate::EngineHandle {
    pub async fn native_torrent_descriptor(
        &self,
        job: &Job,
    ) -> Result<Option<Vec<u8>>, crate::EngineError> {
        use sha2::{Digest, Sha256};
        let Some(reference) = descriptor_reference(job) else {
            return Ok(None);
        };
        let origin = reference.origin(job)?;
        let store = self.url_resolutions.clone();
        tokio::task::spawn_blocking(move || {
            let bytes = store
                .source(&origin)?
                .ok_or(nzbd_state::artifacts::Error::NotFound)?;
            if format!("{:x}", Sha256::digest(&bytes)) != reference.sha256 {
                return Err(nzbd_state::artifacts::Error::Conflict(
                    "native descriptor identity changed".into(),
                ));
            }
            Ok(Some(bytes))
        })
        .await
        .map_err(|_| crate::EngineError::Closed)?
        .map_err(|error| crate::EngineError::Lifecycle(error.to_string()))
    }
}

impl crate::EngineHandle {
    pub(crate) async fn reconcile_native_seed_retirements(&self) -> Result<(), crate::EngineError> {
        let snapshot = self.snapshot();
        if !snapshot.queue_authority {
            return Ok(());
        }
        let cursor = *self.native_seed_recovery_cursor.lock().unwrap();
        let mut ids = snapshot
            .native_seed_retirements
            .iter()
            .filter(|(_, marker)| marker.phase == "detached")
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        ids.sort_by_key(|id| (id.0 <= cursor, id.0));
        let mut tasks = tokio::task::JoinSet::new();
        for id in ids.into_iter().take(8) {
            *self.native_seed_recovery_cursor.lock().unwrap() = id.0;
            let engine = self.clone();
            tasks.spawn(async move { engine.retire_native_seed_once(id).await });
        }
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(crate::EngineError::Closed)) => return Err(crate::EngineError::Closed),
                Ok(Err(error)) => tracing::warn!(%error,"native seed retirement remains pending"),
                Err(error) => {
                    tracing::warn!(%error,"native seed retirement worker did not complete")
                }
            }
        }
        Ok(())
    }
    async fn retire_native_seed_once(&self, id: JobId) -> Result<(), crate::EngineError> {
        let Some(job) = self.export_job(id).await? else {
            return Ok(());
        };
        let Some(marker) = seed_retirement(&job).filter(|marker| marker.phase == "detached") else {
            return Ok(());
        };
        if marker.keep_blocked_revision.is_some() {
            self.roundtrip_bool(|reply| crate::owner::QueueCommand::RearmSeedKeepCleared {
                job: id,
                reply,
            })
            .await?;
        }
        if marker.storage_instance_id != self.artifacts().installation
            || job.held()
            || self.snapshot().native_seed_dependencies.contains(&id)
        {
            return Ok(());
        }
        let Some(record) = job.torrent.as_ref() else {
            return Ok(());
        };
        let expected = record
            .files
            .iter()
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        let inventory = self.artifacts();
        let engine = self.clone();
        let proof = marker.clone();
        let result = crate::bounded_probe(self.hold_probes.clone(), id, move || {
            inventory.retire_managed_torrent_seed(
                id.0,
                &proof.source,
                &expected,
                &proof.intent(),
                &|| {
                    let current = engine.snapshot();
                    if current.queue_authority
                        && !current.native_seed_dependencies.contains(&id)
                        && current.native_seed_retirements.get(&id) == Some(&proof)
                    {
                        Ok(())
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "native seed retirement intent or source dependency changed",
                        ))
                    }
                },
            )
        })
        .await;
        match result {
            Ok(Ok(operation)) if operation.state == "succeeded" => {
                self.complete_managed_seed_retirement(id, marker.identity)
                    .await?;
            }
            Ok(Ok(operation)) => {
                if operation.state == "cancelled"
                    && operation.error.as_deref() == Some("per-download Keep on failure is enabled")
                {
                    self.roundtrip_bool(|reply| crate::owner::QueueCommand::NoteSeedKeepBlock {
                        job: id,
                        marker: Box::new(marker.clone()),
                        operation: Some(operation.id.clone()),
                        reply,
                    })
                    .await?;
                    return Ok(());
                }
                let message = operation.error.unwrap_or_else(|| {
                    format!(
                        "seed retirement {}; Retry to authorize another attempt",
                        operation.state
                    )
                });
                // Seed retirement is a lifecycle after the job's terminal; it
                // never ends the job. Anything that is not space is retried
                // as storage (seed-retirement holds carry no deadline).
                // The operation's error is persisted text with no errno
                // attached; the stderr rule reads it.
                let cause = crate::stderr_hold_cause(&message)
                    .unwrap_or(nzbd_types::HoldCause::StorageUnavailable);
                self.hold_job_until(
                    id,
                    cause,
                    "torrent_seed_retire",
                    &message,
                    Some(operation.next_retry),
                )
                .await?;
            }
            Ok(Err(nzbd_state::artifacts::Error::Io(error)))
                if error.kind() == std::io::ErrorKind::Interrupted => {}
            Ok(Err(nzbd_state::artifacts::Error::Io(error))) => {
                let cause = if error.kind() == std::io::ErrorKind::WouldBlock {
                    nzbd_types::HoldCause::StorageUnavailable
                } else {
                    crate::io_error_hold_cause(&error)
                        .unwrap_or(nzbd_types::HoldCause::StorageUnavailable)
                };
                self.hold_job(id, cause, "torrent_seed_retire", &error.to_string())
                    .await?;
            }
            Ok(Err(nzbd_state::artifacts::Error::Conflict(message)))
                if message == "current Keep retains the managed seed source" =>
            {
                self.roundtrip_bool(|reply| crate::owner::QueueCommand::NoteSeedKeepBlock {
                    job: id,
                    marker: Box::new(marker.clone()),
                    operation: None,
                    reply,
                })
                .await?;
            }
            Ok(Err(error)) => {
                self.hold_job(
                    id,
                    nzbd_types::HoldCause::StorageUnavailable,
                    "torrent_seed_retire",
                    &error.to_string(),
                )
                .await?;
            }
            Err(message) => {
                self.hold_job(
                    id,
                    nzbd_types::HoldCause::StorageUnavailable,
                    "torrent_seed_retire",
                    &message,
                )
                .await?;
            }
        }
        Ok(())
    }
}
