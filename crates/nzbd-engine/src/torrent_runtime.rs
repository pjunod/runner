//! BitTorrent runtime ownership and restore reconciliation.
//!
//! This module intentionally knows `JobId` but has no engine dependency. The
//! maintained adapter receives only [`RestoreRequest`] values and returns
//! engine identities, keeping raw rqbit handles and queue identities apart.

use crate::backend::{BackendFact, RemovalOutcome, SafeError, StopReason, TransferProgress};
use crate::queue::rename_job;
use nzbd_types::{
    Job, JobId, JobKind, JobStatus, TorrentControlIntent, TorrentFileRecord, TorrentPhase,
    TorrentRecord,
};
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};

pub const MAX_RESTORE_DIAGNOSTICS: usize = 64;
pub const SEED_CHECKPOINT_SECS: u64 = 30;
pub const SEED_CHECKPOINT_BYTES: u64 = 8 * 1024 * 1024;
const STORAGE_FULL_ERROR: &str = "storage full";

/// A zero ratio or time value is the public "unlimited" spelling. Normalize
/// it once at admission so every later boundary can reason in `Option`s.
pub fn normalized_seed_policy(
    ratio: Option<f64>,
    time_secs: Option<u64>,
) -> nzbd_types::SeedPolicy {
    nzbd_types::SeedPolicy {
        stop_on_complete: false,
        ratio_limit: ratio.filter(|ratio| ratio.is_finite() && *ratio > 0.0),
        time_limit_secs: time_secs.filter(|seconds| *seconds > 0),
    }
}

/// Evaluate the durable cumulative counters, never a volatile instantaneous
/// rate. Exact equality reaches the limit, and an empty selection cannot
/// manufacture an infinite ratio.
pub fn seed_policy_reached(torrent: &TorrentRecord) -> bool {
    seed_policy_stop_reason(torrent).is_some()
}

pub fn seed_policy_stop_reason(torrent: &TorrentRecord) -> Option<nzbd_types::TorrentStopReason> {
    use nzbd_types::TorrentStopReason;
    if torrent.seed_policy.stop_on_complete && torrent.ready_at_unix.is_some() {
        return Some(TorrentStopReason::DownloadComplete);
    }
    if torrent.seed_policy.ratio_limit.is_some_and(|limit| {
        torrent.selected_bytes > 0
            && (torrent.uploaded_bytes as f64) >= (torrent.selected_bytes as f64 * limit)
    }) {
        return Some(TorrentStopReason::RatioLimit);
    }
    if torrent
        .seed_policy
        .time_limit_secs
        .is_some_and(|limit| torrent.seeding_seconds >= limit)
    {
        return Some(TorrentStopReason::TimeLimit);
    }
    None
}

/// Return whether the unsaved accounting window reached its durable bound.
/// A crash can therefore only extend seeding by the latest 30 seconds or
/// 8 MiB window; it can never make a limit fire early.
pub fn seed_checkpoint_due(
    torrent: &TorrentRecord,
    saved_uploaded_bytes: u64,
    saved_seeding_seconds: u64,
) -> bool {
    torrent.uploaded_bytes.saturating_sub(saved_uploaded_bytes) >= SEED_CHECKPOINT_BYTES
        || torrent
            .seeding_seconds
            .saturating_sub(saved_seeding_seconds)
            >= SEED_CHECKPOINT_SECS
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EngineIdentity {
    pub id: usize,
    pub info_hash_v1: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreRequest {
    pub job: JobId,
    pub info_hash_v1: String,
    pub metadata_file: PathBuf,
    pub selected_files: Vec<usize>,
    pub preferred_engine_id: Option<usize>,
    pub start_paused: bool,
    pub resume_after_restore: bool,
    pub force_recheck: bool,
    pub trusted_downloaded_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreDiagnostic {
    MissingTorrentRecord,
    InvalidInfoHash,
    UnsafeMetadataPath,
    DuplicateInfoHash,
    DuplicatePreferredIdentity,
    DeletedRecord,
    RemovalIntent,
    UnsafePayloadRoot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemovalRefusal {
    UnsafeRoot,
    InventoryMismatch,
}

/// Revalidate persisted payload facts immediately before data deletion.
pub fn validate_removal_payload(
    content_path: &Path,
    files: &[TorrentFileRecord],
    allowed_roots: &[PathBuf],
) -> Result<(), RemovalRefusal> {
    if !allowed_roots
        .iter()
        .any(|root| payload_is_within_root(content_path, root))
    {
        return Err(RemovalRefusal::UnsafeRoot);
    }
    for file in files {
        if !safe_relative_path(&file.path)
            || !payload_is_within_root(&content_path.join(&file.path), content_path)
        {
            return Err(RemovalRefusal::InventoryMismatch);
        }
    }
    Ok(())
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct RestorePlan {
    pub requests: Vec<RestoreRequest>,
    pub diagnostics: Vec<RestoreDiagnostic>,
}

/// Build the only set the engine may restore. Diagnostics are categorical,
/// bounded, and contain no names, paths, magnets, tracker URLs, or passkeys.
pub fn plan_restore(
    jobs: &[Job],
    observed: &HashMap<String, ObservedResumeState>,
    torrent_root: &Path,
    scheduler_allowed: &HashSet<JobId>,
) -> RestorePlan {
    plan_restore_with_roots(
        jobs,
        observed,
        std::slice::from_ref(&torrent_root.to_path_buf()),
        scheduler_allowed,
    )
}

/// Build a restore plan while accepting every configured torrent payload
/// root. Category roots are first-class storage boundaries and must survive a
/// restart without weakening containment to arbitrary persisted paths.
pub fn plan_restore_with_roots(
    jobs: &[Job],
    observed: &HashMap<String, ObservedResumeState>,
    torrent_roots: &[PathBuf],
    scheduler_allowed: &HashSet<JobId>,
) -> RestorePlan {
    let mut plan = RestorePlan::default();
    let mut hashes = HashSet::new();
    let mut preferred_ids = HashSet::new();
    for job in jobs.iter().filter(|job| job.kind == JobKind::Torrent) {
        let Some(record) = &job.torrent else {
            push_diagnostic(&mut plan, RestoreDiagnostic::MissingTorrentRecord);
            continue;
        };
        if record.removal_intent.is_some() {
            push_diagnostic(&mut plan, RestoreDiagnostic::RemovalIntent);
            continue;
        }
        if job.status == JobStatus::Deleted {
            push_diagnostic(&mut plan, RestoreDiagnostic::DeletedRecord);
            continue;
        }
        if !valid_hash(&record.info_hash_v1) {
            push_diagnostic(&mut plan, RestoreDiagnostic::InvalidInfoHash);
            continue;
        }
        if !safe_relative_path(&record.metadata_file) {
            push_diagnostic(&mut plan, RestoreDiagnostic::UnsafeMetadataPath);
            continue;
        }
        if record.content_path.as_ref().is_some_and(|path| {
            !torrent_roots
                .iter()
                .any(|root| payload_is_within_root(path, root))
        }) {
            push_diagnostic(&mut plan, RestoreDiagnostic::UnsafePayloadRoot);
            continue;
        }
        if !hashes.insert(record.info_hash_v1.clone()) {
            push_diagnostic(&mut plan, RestoreDiagnostic::DuplicateInfoHash);
            continue;
        }
        let state = observed.get(&record.info_hash_v1);
        let preferred_engine_id = state.map(|state| state.engine_id);
        if preferred_engine_id.is_some_and(|id| !preferred_ids.insert(id)) {
            push_diagnostic(&mut plan, RestoreDiagnostic::DuplicatePreferredIdentity);
            continue;
        }
        let readiness_disagrees = state.is_some_and(|state| {
            state.finished != record.ready_at_unix.is_some()
                || state.verified_bytes != record.downloaded_bytes
        });
        let resume_after_restore = record.control_intent == TorrentControlIntent::Running
            && !matches!(record.phase, TorrentPhase::Failed)
            && (matches!(
                record.phase,
                TorrentPhase::Seeding | TorrentPhase::PausedSeed
            ) || scheduler_allowed.contains(&job.id));
        plan.requests.push(RestoreRequest {
            job: job.id,
            info_hash_v1: record.info_hash_v1.clone(),
            metadata_file: record.metadata_file.clone(),
            selected_files: record
                .files
                .iter()
                .enumerate()
                .filter_map(|(index, file)| file.selected.then_some(index))
                .collect(),
            preferred_engine_id,
            start_paused: true,
            resume_after_restore,
            force_recheck: readiness_disagrees,
            trusted_downloaded_bytes: record.downloaded_bytes,
        });
    }
    plan
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservedResumeState {
    pub engine_id: usize,
    pub verified_bytes: u64,
    pub finished: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DisabledWithLiveTorrents {
    pub count: usize,
}

pub fn refuse_disabled_with_live_torrents(jobs: &[Job]) -> Result<(), DisabledWithLiveTorrents> {
    let count = jobs
        .iter()
        .filter(|job| job.kind == JobKind::Torrent && job.status != JobStatus::Deleted)
        .count();
    if count == 0 {
        Ok(())
    } else {
        Err(DisabledWithLiveTorrents { count })
    }
}

#[derive(Debug, Default)]
pub struct RuntimeAssociations {
    jobs: HashMap<JobId, EngineIdentity>,
    hashes: HashMap<String, JobId>,
    ids: HashMap<usize, JobId>,
}

impl RuntimeAssociations {
    pub fn associate(
        &mut self,
        job: JobId,
        identity: EngineIdentity,
    ) -> Result<(), AssociationError> {
        if self.jobs.contains_key(&job)
            || self.hashes.contains_key(&identity.info_hash_v1)
            || self.ids.contains_key(&identity.id)
        {
            return Err(AssociationError::Duplicate);
        }
        self.hashes.insert(identity.info_hash_v1.clone(), job);
        self.ids.insert(identity.id, job);
        self.jobs.insert(job, identity);
        Ok(())
    }

    pub fn engine_for_job(&self, job: JobId) -> Option<&EngineIdentity> {
        self.jobs.get(&job)
    }

    pub fn job_for_hash(&self, hash: &str) -> Option<JobId> {
        self.hashes.get(hash).copied()
    }

    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    pub fn translate_fact(
        &self,
        identity: &EngineIdentity,
        fact: EngineStructuralFact,
    ) -> Option<BackendFact> {
        let job = *self.ids.get(&identity.id)?;
        if self.hashes.get(&identity.info_hash_v1) != Some(&job) {
            return None;
        }
        Some(match fact {
            EngineStructuralFact::Ready { content_path } => {
                BackendFact::Ready { job, content_path }
            }
            EngineStructuralFact::Stopped { reason } => BackendFact::Stopped { job, reason },
            EngineStructuralFact::StorageFull => BackendFact::Stopped {
                job,
                reason: StopReason::StorageFull,
            },
            EngineStructuralFact::MissingContent => BackendFact::Stopped {
                job,
                reason: StopReason::MissingContent,
            },
            EngineStructuralFact::Transient => BackendFact::Stopped {
                job,
                reason: StopReason::Transient,
            },
            EngineStructuralFact::Unrecoverable { error } => BackendFact::Failed { job, error },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineStructuralFact {
    Ready {
        content_path: PathBuf,
    },
    Stopped {
        reason: StopReason,
    },
    StorageFull,
    MissingContent,
    Transient,
    /// The adapter must explicitly classify an error as unrecoverable before
    /// it is allowed to cross the backend boundary as `Failed`.
    Unrecoverable {
        error: SafeError,
    },
}

/// Queue-owner side effects produced while folding backend traffic. The
/// caller persists `durable_changed` at its normal snapshot cadence and
/// latches the shared disk guard when `storage_hold` is set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconcileOutcome {
    pub durable_changed: bool,
    pub storage_hold: bool,
}

/// Fold a replaceable progress sample into durable queue state. Volatile
/// rates and peer counts deliberately remain outside [`nzbd_types::Job`].
pub fn reconcile_progress(job: &mut Job, progress: &TransferProgress) -> ReconcileOutcome {
    let Some(torrent) = torrent_mut(job) else {
        return ReconcileOutcome::default();
    };
    // Only verified bytes are a trusted restart checkpoint. Keep the durable
    // checkpoint monotonic: an engine may report zero while rechecking a
    // restored payload, and regressing here would charge those local bytes to
    // download quota a second time as verification catches back up.
    let downloaded = torrent
        .downloaded_bytes
        .max(progress.verified_bytes.min(torrent.selected_bytes));
    let uploaded = torrent.uploaded_bytes.max(progress.uploaded_bytes);
    let activity = match (torrent.last_activity_unix, progress.last_activity_unix) {
        (Some(old), Some(new)) => Some(old.max(new)),
        (old, new) => old.or(new),
    };
    let mut files_changed = false;
    for (file, verified) in torrent.files.iter_mut().zip(&progress.file_progress_bytes) {
        let verified = (*verified).min(file.length);
        if verified > file.downloaded_bytes {
            file.downloaded_bytes = verified;
            files_changed = true;
        }
    }
    let changed = torrent.downloaded_bytes != downloaded
        || torrent.uploaded_bytes != uploaded
        || torrent.last_activity_unix != activity
        || files_changed;
    torrent.downloaded_bytes = downloaded;
    torrent.uploaded_bytes = uploaded;
    torrent.last_activity_unix = activity;
    ReconcileOutcome {
        durable_changed: changed,
        storage_hold: false,
    }
}

/// Fold one reliable structural fact. Readiness is accepted only when the
/// latest engine sample proves every selected byte hash-verified; a bare
/// engine "finished" phase can therefore never make queue state ready.
pub fn reconcile_fact(
    job: &mut Job,
    fact: BackendFact,
    latest: Option<&TransferProgress>,
    now_unix: i64,
    torrent_root: &Path,
) -> ReconcileOutcome {
    reconcile_fact_with_roots(
        job,
        fact,
        latest,
        now_unix,
        std::slice::from_ref(&torrent_root.to_path_buf()),
    )
}

/// Fold a structural backend fact while accepting every configured payload
/// root. Category-specific roots are equal storage boundaries, not children
/// of the default root.
pub fn reconcile_fact_with_roots(
    job: &mut Job,
    fact: BackendFact,
    latest: Option<&TransferProgress>,
    now_unix: i64,
    torrent_roots: &[PathBuf],
) -> ReconcileOutcome {
    if fact_job(&fact) != job.id {
        return ReconcileOutcome::default();
    }
    if job.kind != JobKind::Torrent || job.torrent.is_none() || job.status == JobStatus::Deleted {
        return ReconcileOutcome::default();
    }
    let before = job.clone();
    match fact {
        BackendFact::MetadataReady {
            torrent: metadata, ..
        } => {
            let finalize_name = job.name_provisional;
            let torrent = job.torrent.as_mut().unwrap();
            torrent.info_hash_v1 = metadata.info_hash_v1;
            torrent.total_bytes = metadata.total_bytes;
            torrent.selected_bytes = metadata.selected_bytes;
            if matches!(
                torrent.phase,
                TorrentPhase::FetchingSource | TorrentPhase::FetchingMetadata
            ) {
                torrent.phase = TorrentPhase::Queued;
            }
            if finalize_name {
                if job.name == metadata.name {
                    job.name_provisional = false;
                } else {
                    rename_job(job, metadata.name, true, false);
                }
            }
            ReconcileOutcome {
                durable_changed: fact_state_changed(&before, job),
                storage_hold: false,
            }
        }
        BackendFact::Ready { content_path, .. } => {
            if !torrent_roots
                .iter()
                .any(|root| payload_is_within_root(&content_path, root))
            {
                return ReconcileOutcome::default();
            }
            let torrent = job.torrent.as_mut().unwrap();
            let verified = latest.map_or(0, |progress| progress.verified_bytes);
            if verified < torrent.selected_bytes || torrent.selected_bytes == 0 {
                return ReconcileOutcome::default();
            }
            torrent.downloaded_bytes = torrent.selected_bytes;
            for file in &mut torrent.files {
                if file.selected {
                    file.downloaded_bytes = file.length;
                }
            }
            torrent.ready_at_unix.get_or_insert(now_unix);
            torrent.content_path = Some(content_path);
            torrent.phase = if job.status == JobStatus::Paused {
                TorrentPhase::PausedSeed
            } else {
                TorrentPhase::Seeding
            };
            torrent.last_error = None;
            if job.status != JobStatus::Paused {
                job.status = JobStatus::Downloading;
            }
            ReconcileOutcome {
                durable_changed: fact_state_changed(&before, job),
                storage_hold: false,
            }
        }
        BackendFact::Stopped { reason, .. } => {
            let torrent = job.torrent.as_mut().unwrap();
            let storage_hold = reason == StopReason::StorageFull;
            match reason {
                StopReason::Paused | StopReason::StorageFull => {
                    torrent.phase = if torrent.ready_at_unix.is_some() {
                        TorrentPhase::PausedSeed
                    } else {
                        TorrentPhase::PausedDownload
                    };
                    torrent.last_error =
                        (reason == StopReason::StorageFull).then(|| STORAGE_FULL_ERROR.to_owned());
                    if storage_hold {
                        torrent.stop_reason = Some(nzbd_types::TorrentStopReason::StorageFull);
                    }
                    job.status = JobStatus::Paused;
                }
                StopReason::SchedulerYield => {
                    torrent.phase = TorrentPhase::Queued;
                    torrent.last_error = None;
                    job.status = JobStatus::Queued;
                }
                StopReason::MissingContent => {
                    torrent.phase = TorrentPhase::MissingFiles;
                    job.status = JobStatus::Paused;
                }
                StopReason::Transient => {
                    if job.status != JobStatus::Paused {
                        if torrent.ready_at_unix.is_some() {
                            torrent.phase = TorrentPhase::Seeding;
                            job.status = JobStatus::Downloading;
                        } else if matches!(
                            torrent.phase,
                            TorrentPhase::Queued | TorrentPhase::Downloading
                        ) {
                            torrent.phase = TorrentPhase::Downloading;
                            job.status = JobStatus::Downloading;
                        }
                    }
                }
                StopReason::SeedPolicyReached => {
                    torrent.stop_reason = torrent
                        .stop_reason
                        .or_else(|| seed_policy_stop_reason(torrent));
                    torrent.phase = TorrentPhase::PausedSeed;
                    job.status = JobStatus::Paused;
                }
                StopReason::Removed | StopReason::Shutdown => {}
            }
            ReconcileOutcome {
                durable_changed: fact_state_changed(&before, job),
                storage_hold,
            }
        }
        BackendFact::Resumed { .. } => {
            let torrent = job.torrent.as_mut().unwrap();
            torrent.last_error = None;
            if job.status != JobStatus::Paused {
                torrent.stop_reason = None;
                // A newly started backend needs a fresh discovery window;
                // the previous run's idle clock cannot immediately yield it.
                torrent.last_activity_unix = Some(now_unix);
                torrent.phase = if torrent.ready_at_unix.is_some() {
                    TorrentPhase::Seeding
                } else {
                    TorrentPhase::Downloading
                };
                job.status = JobStatus::Downloading;
            }
            ReconcileOutcome {
                durable_changed: fact_state_changed(&before, job),
                storage_hold: false,
            }
        }
        BackendFact::Removed { outcome, .. } => {
            let torrent = job.torrent.as_mut().unwrap();
            if matches!(
                outcome,
                RemovalOutcome::RefusedUnsafeRoot | RemovalOutcome::RefusedInventoryMismatch
            ) {
                torrent.last_error = Some("torrent removal refused unsafe payload".to_owned());
            }
            ReconcileOutcome {
                durable_changed: fact_state_changed(&before, job),
                storage_hold: false,
            }
        }
        BackendFact::Failed { error, .. } => {
            let torrent = job.torrent.as_mut().unwrap();
            torrent.phase = TorrentPhase::Failed;
            torrent.last_error = Some(error.as_str().to_owned());
            job.status = JobStatus::Failed;
            ReconcileOutcome {
                durable_changed: fact_state_changed(&before, job),
                storage_hold: false,
            }
        }
    }
}

fn fact_state_changed(before: &Job, after: &Job) -> bool {
    before.name != after.name
        || before.dir_name != after.dir_name
        || before.name_provisional != after.name_provisional
        || before.original_name != after.original_name
        || before.status != after.status
        || before.torrent != after.torrent
}

fn torrent_mut(job: &mut Job) -> Option<&mut nzbd_types::TorrentRecord> {
    (job.kind == JobKind::Torrent).then_some(())?;
    job.torrent.as_mut()
}

fn fact_job(fact: &BackendFact) -> JobId {
    match fact {
        BackendFact::MetadataReady { job, .. }
        | BackendFact::Ready { job, .. }
        | BackendFact::Stopped { job, .. }
        | BackendFact::Resumed { job }
        | BackendFact::Removed { job, .. }
        | BackendFact::Failed { job, .. } => *job,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssociationError {
    Duplicate,
}

fn valid_hash(hash: &str) -> bool {
    hash.len() == 40
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn safe_relative_path(path: &Path) -> bool {
    !path.as_os_str().is_empty()
        && !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

/// Resolve the existing portion of a payload path so a symlink cannot make a
/// lexically contained path escape. The leaf may not exist yet during restore,
/// so canonicalization deliberately stops at its nearest existing ancestor.
pub fn payload_is_within_root(path: &Path, root: &Path) -> bool {
    let Some(path) = normalize_absolute(path) else {
        return false;
    };
    let Some(root) = normalize_absolute(root) else {
        return false;
    };
    if !path.starts_with(&root) {
        return false;
    }

    let Ok(canonical_root) = root.canonicalize() else {
        return false;
    };
    let mut existing = path.as_path();
    loop {
        match existing.canonicalize() {
            Ok(canonical_existing) => return canonical_existing.starts_with(&canonical_root),
            Err(_) => {
                let Some(parent) = existing.parent() else {
                    return false;
                };
                existing = parent;
            }
        }
    }
}

fn normalize_absolute(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
        }
    }
    Some(normalized)
}

fn push_diagnostic(plan: &mut RestorePlan, diagnostic: RestoreDiagnostic) {
    if plan.diagnostics.len() < MAX_RESTORE_DIAGNOSTICS {
        plan.diagnostics.push(diagnostic);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn torrent_mobile_queue_storage_full_sentinel_matches_fixture() {
        let fixtures: serde_json::Value = serde_json::from_str(include_str!(
            "../../nzbd-types/fixtures/mobile-queue-parity.json"
        ))
        .unwrap();
        let hold = fixtures
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "storage hold")
            .unwrap();
        assert_eq!(hold["job"]["torrent_error"], super::STORAGE_FULL_ERROR);
    }

    use super::*;
    use nzbd_types::{
        DupeInfo, JobTotals, SeedPolicy, TorrentFileRecord, TorrentRecord, TorrentSource,
    };

    fn job(id: u32, hash: &str, status: JobStatus) -> Job {
        Job {
            id: JobId(id),
            kind: JobKind::Torrent,
            name: "not logged".into(),
            dir_name: "torrent".into(),
            name_provisional: false,
            queued_at_unix: 1,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: DupeInfo::default(),
            params: Vec::new(),
            files: Vec::new(),
            totals: JobTotals::default(),
            status,
            torrent: Some(TorrentRecord {
                info_hash_v1: hash.into(),
                source: TorrentSource::Metainfo,
                metadata_file: PathBuf::from("meta/selected.torrent"),
                payload_root: PathBuf::new(),
                phase: TorrentPhase::Downloading,
                control_intent: nzbd_types::TorrentControlIntent::Running,
                removal_intent: None,
                removal_outcome: None,
                removal_confirmed_at_unix: None,
                stop_reason: None,
                files: vec![
                    TorrentFileRecord {
                        path: "one".into(),
                        length: 1,
                        selected: true,
                        downloaded_bytes: 1,
                    },
                    TorrentFileRecord {
                        path: "two".into(),
                        length: 1,
                        selected: false,
                        downloaded_bytes: 0,
                    },
                ],
                total_bytes: 2,
                selected_bytes: 1,
                downloaded_bytes: 1,
                uploaded_bytes: 0,
                seeding_seconds: 0,
                ready_at_unix: None,
                content_path: None,
                seed_policy: SeedPolicy::default(),
                last_activity_unix: None,
                last_error: None,
            }),
            stages: Vec::new(),
        }
    }

    #[test]
    fn seed_policy_normalizes_unlimited_and_stops_on_exact_boundaries() {
        assert_eq!(
            normalized_seed_policy(Some(0.0), Some(0)),
            SeedPolicy::default()
        );

        let mut record = job(
            10,
            "0123456789abcdef0123456789abcdef01234567",
            JobStatus::Downloading,
        );
        let torrent = record.torrent.as_mut().unwrap();
        torrent.selected_bytes = 100;
        torrent.seed_policy = normalized_seed_policy(Some(1.5), Some(90));
        torrent.uploaded_bytes = 149;
        torrent.seeding_seconds = 89;
        assert!(!seed_policy_reached(torrent));
        torrent.uploaded_bytes = 150;
        assert!(seed_policy_reached(torrent));
        assert_eq!(
            seed_policy_stop_reason(torrent),
            Some(nzbd_types::TorrentStopReason::RatioLimit)
        );
        torrent.uploaded_bytes = 0;
        torrent.seeding_seconds = 90;
        assert!(seed_policy_reached(torrent));
        assert_eq!(
            seed_policy_stop_reason(torrent),
            Some(nzbd_types::TorrentStopReason::TimeLimit)
        );
        torrent.seed_policy = SeedPolicy {
            stop_on_complete: true,
            ..Default::default()
        };
        assert!(
            !seed_policy_reached(torrent),
            "unverified bytes must not trigger completion policy"
        );
        torrent.ready_at_unix = Some(123);
        assert_eq!(
            seed_policy_stop_reason(torrent),
            Some(nzbd_types::TorrentStopReason::DownloadComplete)
        );
    }

    #[test]
    fn seed_checkpoint_bound_is_thirty_seconds_or_eight_mib() {
        let mut record = job(
            10,
            "0123456789abcdef0123456789abcdef01234567",
            JobStatus::Downloading,
        );
        let torrent = record.torrent.as_mut().unwrap();
        torrent.uploaded_bytes = SEED_CHECKPOINT_BYTES - 1;
        torrent.seeding_seconds = SEED_CHECKPOINT_SECS - 1;
        assert!(!seed_checkpoint_due(torrent, 0, 0));
        torrent.uploaded_bytes += 1;
        assert!(seed_checkpoint_due(torrent, 0, 0));
        torrent.uploaded_bytes = 0;
        torrent.seeding_seconds += 1;
        assert!(seed_checkpoint_due(torrent, 0, 0));
    }

    #[test]
    fn durable_queue_selects_one_paused_restore_and_queue_control_resumes_it() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let unknown = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
        let observed = HashMap::from([
            (
                hash.to_string(),
                ObservedResumeState {
                    engine_id: 7,
                    verified_bytes: 1,
                    finished: false,
                },
            ),
            (
                unknown,
                ObservedResumeState {
                    engine_id: 8,
                    verified_bytes: 99,
                    finished: true,
                },
            ),
        ]);
        let plan = plan_restore(
            &[job(10, hash, JobStatus::Queued)],
            &observed,
            Path::new("/torrents"),
            &HashSet::from([JobId(10)]),
        );
        assert_eq!(plan.requests.len(), 1);
        let request = &plan.requests[0];
        assert!(request.start_paused);
        assert!(request.resume_after_restore);
        assert_eq!(request.preferred_engine_id, Some(7));
        assert_eq!(request.selected_files, vec![0]);
        assert!(plan.diagnostics.is_empty());
    }

    #[test]
    fn restore_obeys_durable_pause_and_resume_intent_over_stale_status() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let mut paused = job(10, hash, JobStatus::Downloading);
        paused.torrent.as_mut().unwrap().control_intent = TorrentControlIntent::Paused;
        let paused_plan = plan_restore(
            &[paused],
            &HashMap::new(),
            Path::new("/torrents"),
            &HashSet::from([JobId(10)]),
        );
        assert!(paused_plan.requests[0].start_paused);
        assert!(!paused_plan.requests[0].resume_after_restore);

        let mut resumed = job(10, hash, JobStatus::Paused);
        let torrent = resumed.torrent.as_mut().unwrap();
        torrent.phase = TorrentPhase::PausedDownload;
        torrent.control_intent = TorrentControlIntent::Running;
        torrent.downloaded_bytes = 64;
        let resumed_plan = plan_restore(
            &[resumed],
            &HashMap::from([(
                hash.to_owned(),
                ObservedResumeState {
                    engine_id: 7,
                    verified_bytes: 64,
                    finished: false,
                },
            )]),
            Path::new("/torrents"),
            &HashSet::from([JobId(10)]),
        );
        assert!(resumed_plan.requests[0].resume_after_restore);
        assert!(!resumed_plan.requests[0].force_recheck);
        assert_eq!(resumed_plan.requests[0].trusted_downloaded_bytes, 64);
    }

    #[test]
    fn restore_skips_every_durable_removal_intent_before_engine_recovery() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        for delete_data in [false, true] {
            for (index, (status, phase)) in [
                (JobStatus::Queued, TorrentPhase::Queued),
                (JobStatus::Downloading, TorrentPhase::Downloading),
                (JobStatus::Paused, TorrentPhase::PausedDownload),
                (JobStatus::Downloading, TorrentPhase::Seeding),
                (JobStatus::Paused, TorrentPhase::PausedSeed),
            ]
            .into_iter()
            .enumerate()
            {
                let job_id = JobId((index + 10) as u32);
                let mut pending_removal = job(job_id.0, hash, status);
                let torrent = pending_removal.torrent.as_mut().unwrap();
                torrent.phase = phase;
                torrent.removal_intent = Some(nzbd_types::TorrentRemovalIntent { delete_data });
                let plan = plan_restore(
                    &[pending_removal],
                    &HashMap::new(),
                    Path::new("/torrents"),
                    &HashSet::from([job_id]),
                );
                assert!(plan.requests.is_empty());
                assert_eq!(plan.diagnostics, vec![RestoreDiagnostic::RemovalIntent]);
            }
        }
    }

    #[test]
    fn seeding_family_restores_without_a_download_slot_according_to_durable_intent() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let mut running_seed = job(10, hash, JobStatus::Downloading);
        let torrent = running_seed.torrent.as_mut().unwrap();
        torrent.phase = TorrentPhase::Seeding;
        torrent.ready_at_unix = Some(1);
        let running_plan = plan_restore(
            &[running_seed],
            &HashMap::new(),
            Path::new("/torrents"),
            &HashSet::new(),
        );
        assert_eq!(running_plan.requests.len(), 1);
        assert!(running_plan.requests[0].resume_after_restore);

        let mut paused_seed = job(10, hash, JobStatus::Paused);
        let torrent = paused_seed.torrent.as_mut().unwrap();
        torrent.phase = TorrentPhase::PausedSeed;
        torrent.ready_at_unix = Some(1);
        torrent.control_intent = TorrentControlIntent::Paused;
        let paused_plan = plan_restore(
            &[paused_seed],
            &HashMap::new(),
            Path::new("/torrents"),
            &HashSet::new(),
        );
        assert_eq!(paused_plan.requests.len(), 1);
        assert!(!paused_plan.requests[0].resume_after_restore);

        let mut resumed_seed = job(10, hash, JobStatus::Queued);
        let torrent = resumed_seed.torrent.as_mut().unwrap();
        torrent.phase = TorrentPhase::PausedSeed;
        torrent.ready_at_unix = Some(1);
        torrent.control_intent = TorrentControlIntent::Running;
        torrent.downloaded_bytes = 1;
        let resumed_plan = plan_restore(
            &[resumed_seed],
            &HashMap::from([(
                hash.to_owned(),
                ObservedResumeState {
                    engine_id: 7,
                    verified_bytes: 1,
                    finished: true,
                },
            )]),
            Path::new("/torrents"),
            &HashSet::new(),
        );
        let request = &resumed_plan.requests[0];
        assert!(request.start_paused);
        assert!(request.resume_after_restore);
        assert!(!request.force_recheck);
    }

    #[test]
    fn readiness_or_checkpoint_disagreement_forces_recheck_before_ready_claim() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let observed = HashMap::from([(
            hash.to_string(),
            ObservedResumeState {
                engine_id: 7,
                verified_bytes: 2,
                finished: true,
            },
        )]);
        let plan = plan_restore(
            &[job(10, hash, JobStatus::Paused)],
            &observed,
            Path::new("/torrents"),
            &HashSet::new(),
        );
        assert!(plan.requests[0].force_recheck);
        assert!(!plan.requests[0].resume_after_restore);
        assert_eq!(plan.requests[0].trusted_downloaded_bytes, 1);
    }

    #[test]
    fn association_owner_rejects_duplicate_engine_handles() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let mut associations = RuntimeAssociations::default();
        associations
            .associate(
                JobId(10),
                EngineIdentity {
                    id: 7,
                    info_hash_v1: hash.into(),
                },
            )
            .unwrap();
        assert_eq!(associations.len(), 1);
        assert_eq!(associations.job_for_hash(hash), Some(JobId(10)));
        assert!(matches!(
            associations.translate_fact(
                associations.engine_for_job(JobId(10)).unwrap(),
                EngineStructuralFact::Stopped {
                    reason: crate::backend::StopReason::Paused
                }
            ),
            Some(crate::backend::BackendFact::Stopped { job: JobId(10), .. })
        ));
        assert_eq!(
            associations.associate(
                JobId(11),
                EngineIdentity {
                    id: 7,
                    info_hash_v1: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()
                }
            ),
            Err(AssociationError::Duplicate)
        );
    }

    #[test]
    fn completion_requires_all_selected_bytes_verified_and_ready_seed_stays_live() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let mut job = job(10, hash, JobStatus::Downloading);
        let temp = tempfile::tempdir().unwrap();
        let torrent_root = temp.path().join("torrents");
        std::fs::create_dir(&torrent_root).unwrap();
        let content_path = torrent_root.join("example");
        let incomplete = TransferProgress {
            downloaded_bytes: 1,
            verified_bytes: 0,
            ..Default::default()
        };

        assert_eq!(
            reconcile_fact(
                &mut job,
                BackendFact::Ready {
                    job: JobId(10),
                    content_path: content_path.clone(),
                },
                Some(&incomplete),
                100,
                &torrent_root,
            ),
            ReconcileOutcome::default(),
            "an engine completion phase is not verification evidence"
        );
        assert!(!job.ready());

        let verified = TransferProgress {
            downloaded_bytes: 1,
            verified_bytes: 1,
            ..Default::default()
        };
        assert!(
            reconcile_fact(
                &mut job,
                BackendFact::Ready {
                    job: JobId(10),
                    content_path: content_path.clone(),
                },
                Some(&verified),
                101,
                &torrent_root,
            )
            .durable_changed
        );
        assert!(job.ready());
        assert_eq!(job.status, JobStatus::Downloading);
        let torrent = job.torrent.as_ref().unwrap();
        assert_eq!(torrent.phase, TorrentPhase::Seeding);
        assert_eq!(torrent.ready_at_unix, Some(101));
        assert_eq!(torrent.content_path.as_ref(), Some(&content_path));

        let persisted = serde_json::to_vec(&job).unwrap();
        let restored: Job = serde_json::from_slice(&persisted).unwrap();
        assert!(
            restored.ready(),
            "the next durable snapshot keeps readiness"
        );
        assert_eq!(restored.torrent.unwrap().phase, TorrentPhase::Seeding);
    }

    #[test]
    fn progress_persists_only_coalescible_counters_and_activity() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let mut job = job(10, hash, JobStatus::Downloading);
        let progress = TransferProgress {
            downloaded_bytes: 1,
            verified_bytes: 1,
            file_progress_bytes: vec![1, 0],
            uploaded_bytes: 7,
            download_bps: 900,
            upload_bps: 800,
            useful_peers: 12,
            last_activity_unix: Some(55),
        };
        assert!(reconcile_progress(&mut job, &progress).durable_changed);
        let torrent = job.torrent.unwrap();
        assert_eq!(torrent.downloaded_bytes, 1);
        assert_eq!(torrent.uploaded_bytes, 7);
        assert_eq!(torrent.last_activity_unix, Some(55));
        // TorrentRecord intentionally has no rate or peer fields: those
        // volatile values cannot force full-list structural publication.
    }

    #[test]
    fn recoverable_engine_conditions_never_translate_to_failed() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let identity = EngineIdentity {
            id: 7,
            info_hash_v1: hash.into(),
        };
        let mut associations = RuntimeAssociations::default();
        associations.associate(JobId(10), identity.clone()).unwrap();

        for (engine, expected) in [
            (EngineStructuralFact::StorageFull, StopReason::StorageFull),
            (
                EngineStructuralFact::MissingContent,
                StopReason::MissingContent,
            ),
            (EngineStructuralFact::Transient, StopReason::Transient),
        ] {
            assert_eq!(
                associations.translate_fact(&identity, engine),
                Some(BackendFact::Stopped {
                    job: JobId(10),
                    reason: expected,
                })
            );
        }

        let error = SafeError::from_redacted("named unrecoverable corruption");
        assert_eq!(
            associations.translate_fact(
                &identity,
                EngineStructuralFact::Unrecoverable {
                    error: error.clone()
                }
            ),
            Some(BackendFact::Failed {
                job: JobId(10),
                error,
            })
        );
    }

    #[test]
    fn storage_full_latches_hold_and_missing_content_remains_recoverable() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let mut storage = job(10, hash, JobStatus::Downloading);
        let outcome = reconcile_fact(
            &mut storage,
            BackendFact::Stopped {
                job: JobId(10),
                reason: StopReason::StorageFull,
            },
            None,
            100,
            Path::new("/unused"),
        );
        assert!(outcome.storage_hold);
        assert_eq!(storage.status, JobStatus::Paused);
        let persisted = serde_json::to_vec(&storage).unwrap();
        let restored: Job = serde_json::from_slice(&persisted).unwrap();
        assert_ne!(
            restored.torrent.as_ref().unwrap().phase,
            TorrentPhase::Failed
        );
        assert_eq!(
            restored.torrent.as_ref().unwrap().last_error.as_deref(),
            Some(STORAGE_FULL_ERROR)
        );

        let mut missing = job(10, hash, JobStatus::Downloading);
        let outcome = reconcile_fact(
            &mut missing,
            BackendFact::Stopped {
                job: JobId(10),
                reason: StopReason::MissingContent,
            },
            None,
            100,
            Path::new("/unused"),
        );
        assert!(!outcome.storage_hold);
        assert_eq!(missing.status, JobStatus::Paused);
        assert_eq!(missing.torrent.unwrap().phase, TorrentPhase::MissingFiles);
    }

    #[test]
    fn progress_checkpoints_only_verified_bytes_and_restores_without_recheck() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let mut record = job(10, hash, JobStatus::Downloading);
        record.torrent.as_mut().unwrap().selected_bytes = 100;
        reconcile_progress(
            &mut record,
            &TransferProgress {
                downloaded_bytes: 80,
                verified_bytes: 64,
                ..Default::default()
            },
        );
        assert_eq!(record.torrent.as_ref().unwrap().downloaded_bytes, 64);

        reconcile_progress(&mut record, &TransferProgress::default());
        assert_eq!(record.torrent.as_ref().unwrap().downloaded_bytes, 64);

        let observed = HashMap::from([(
            hash.to_owned(),
            ObservedResumeState {
                engine_id: 7,
                verified_bytes: 64,
                finished: false,
            },
        )]);
        let plan = plan_restore(
            &[record],
            &observed,
            Path::new("/torrents"),
            &HashSet::from([JobId(10)]),
        );
        assert!(!plan.requests[0].force_recheck);
        assert_eq!(plan.requests[0].trusted_downloaded_bytes, 64);
    }

    #[test]
    fn transient_fact_preserves_seed_and_pre_download_phases() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let mut seed = job(10, hash, JobStatus::Downloading);
        let torrent = seed.torrent.as_mut().unwrap();
        torrent.ready_at_unix = Some(50);
        torrent.phase = TorrentPhase::Seeding;
        let outcome = reconcile_fact(
            &mut seed,
            BackendFact::Stopped {
                job: JobId(10),
                reason: StopReason::Transient,
            },
            None,
            100,
            Path::new("/unused"),
        );
        assert!(!outcome.durable_changed);
        assert_eq!(seed.torrent.as_ref().unwrap().phase, TorrentPhase::Seeding);
        assert!(!seed.torrent.as_ref().unwrap().phase.wants_download_slot());

        for phase in [TorrentPhase::FetchingMetadata, TorrentPhase::Checking] {
            let mut pending = job(10, hash, JobStatus::Queued);
            pending.torrent.as_mut().unwrap().phase = phase;
            reconcile_fact(
                &mut pending,
                BackendFact::Stopped {
                    job: JobId(10),
                    reason: StopReason::Transient,
                },
                None,
                100,
                Path::new("/unused"),
            );
            assert_eq!(pending.torrent.as_ref().unwrap().phase, phase);
            assert_eq!(pending.status, JobStatus::Queued);
        }
    }

    #[test]
    fn late_ready_ignores_deleted_and_preserves_user_pause() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("torrents");
        std::fs::create_dir(&root).unwrap();
        let fact = || BackendFact::Ready {
            job: JobId(10),
            content_path: root.join("example"),
        };
        let verified = TransferProgress {
            verified_bytes: 1,
            ..Default::default()
        };

        let mut deleted = job(10, hash, JobStatus::Deleted);
        let before = deleted.clone();
        assert_eq!(
            reconcile_fact(&mut deleted, fact(), Some(&verified), 100, &root),
            ReconcileOutcome::default()
        );
        assert_eq!(
            serde_json::to_value(deleted).unwrap(),
            serde_json::to_value(before).unwrap()
        );

        let mut paused = job(10, hash, JobStatus::Paused);
        paused.torrent.as_mut().unwrap().phase = TorrentPhase::PausedDownload;
        assert!(reconcile_fact(&mut paused, fact(), Some(&verified), 100, &root).durable_changed);
        assert_eq!(paused.status, JobStatus::Paused);
        assert_eq!(
            paused.torrent.as_ref().unwrap().phase,
            TorrentPhase::PausedSeed
        );
        assert!(paused.ready());
    }

    #[test]
    fn resume_fact_restores_the_phase_that_pause_preserved() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let root = tempfile::tempdir().unwrap();

        let mut downloading = job(10, hash, JobStatus::Queued);
        downloading.torrent.as_mut().unwrap().phase = TorrentPhase::PausedDownload;
        downloading.torrent.as_mut().unwrap().last_activity_unix = Some(1);
        assert!(
            reconcile_fact(
                &mut downloading,
                BackendFact::Resumed { job: JobId(10) },
                None,
                100,
                root.path(),
            )
            .durable_changed
        );
        assert_eq!(downloading.status, JobStatus::Downloading);
        assert!(crate::backend::torrent_wants_download_slot(
            downloading.torrent.as_ref().unwrap(),
            downloading.queued_at_unix,
            101,
        ));
        assert_eq!(
            downloading.torrent.as_ref().unwrap().phase,
            TorrentPhase::Downloading
        );

        let mut seeding = job(10, hash, JobStatus::Queued);
        let torrent = seeding.torrent.as_mut().unwrap();
        torrent.phase = TorrentPhase::PausedSeed;
        torrent.ready_at_unix = Some(99);
        assert!(
            reconcile_fact(
                &mut seeding,
                BackendFact::Resumed { job: JobId(10) },
                None,
                100,
                root.path(),
            )
            .durable_changed
        );
        assert_eq!(seeding.status, JobStatus::Downloading);
        assert_eq!(
            seeding.torrent.as_ref().unwrap().phase,
            TorrentPhase::Seeding
        );

        let mut storage_full = job(10, hash, JobStatus::Paused);
        storage_full.torrent.as_mut().unwrap().last_error = Some(STORAGE_FULL_ERROR.to_owned());
        reconcile_fact(
            &mut storage_full,
            BackendFact::Resumed { job: JobId(10) },
            None,
            100,
            root.path(),
        );
        assert_eq!(storage_full.torrent.as_ref().unwrap().last_error, None);
    }

    #[test]
    fn ready_rejects_content_path_outside_root() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("torrents");
        std::fs::create_dir(&root).unwrap();
        let mut record = job(10, hash, JobStatus::Downloading);
        let before = record.clone();
        let outcome = reconcile_fact(
            &mut record,
            BackendFact::Ready {
                job: JobId(10),
                content_path: temp.path().join("outside"),
            },
            Some(&TransferProgress {
                verified_bytes: 1,
                ..Default::default()
            }),
            100,
            &root,
        );
        assert_eq!(outcome, ReconcileOutcome::default());
        assert_eq!(
            serde_json::to_value(record).unwrap(),
            serde_json::to_value(before).unwrap()
        );
    }

    #[test]
    fn removal_validation_refuses_symlink_escape_and_inventory_escape() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let payload = root.join("payload");
        std::fs::create_dir_all(&payload).unwrap();
        std::fs::write(payload.join("owned.bin"), b"owned").unwrap();
        assert_eq!(
            validate_removal_payload(
                &payload,
                &[TorrentFileRecord {
                    path: "owned.bin".into(),
                    length: 5,
                    selected: true,
                    downloaded_bytes: 5,
                }],
                std::slice::from_ref(&root),
            ),
            Ok(())
        );
        assert_eq!(
            validate_removal_payload(
                &payload,
                &[TorrentFileRecord {
                    path: "../sibling.bin".into(),
                    length: 1,
                    selected: true,
                    downloaded_bytes: 0,
                }],
                std::slice::from_ref(&root),
            ),
            Err(RemovalRefusal::InventoryMismatch)
        );
        assert_eq!(
            validate_removal_payload(&payload, &[], &[temp.path().join("other")]),
            Err(RemovalRefusal::UnsafeRoot)
        );
    }

    #[test]
    fn metadata_renames_only_provisional_jobs_without_regressing_phase() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let metadata = || BackendFact::MetadataReady {
            job: JobId(10),
            torrent: crate::backend::TorrentMetadata {
                info_hash_v1: hash.into(),
                name: "engine name".into(),
                total_bytes: 2,
                selected_bytes: 1,
            },
        };
        let mut final_name = job(10, hash, JobStatus::Downloading);
        final_name.torrent.as_mut().unwrap().phase = TorrentPhase::Seeding;
        reconcile_fact(&mut final_name, metadata(), None, 100, Path::new("/unused"));
        assert_eq!(final_name.name, "not logged");
        assert_eq!(
            final_name.torrent.as_ref().unwrap().phase,
            TorrentPhase::Seeding
        );

        let mut provisional = job(10, hash, JobStatus::Downloading);
        provisional.name_provisional = true;
        provisional.torrent.as_mut().unwrap().phase = TorrentPhase::FetchingMetadata;
        reconcile_fact(
            &mut provisional,
            metadata(),
            None,
            100,
            Path::new("/unused"),
        );
        assert_eq!(provisional.name, "engine name");
        assert_eq!(provisional.original_name, "not logged");
        assert_eq!(provisional.dir_name, "engine name");
        assert!(!provisional.name_provisional);
        assert_eq!(
            provisional.torrent.as_ref().unwrap().phase,
            TorrentPhase::Queued
        );
    }

    #[test]
    fn no_op_stop_does_not_request_snapshot() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        for reason in [StopReason::Removed, StopReason::Shutdown] {
            let mut record = job(10, hash, JobStatus::Downloading);
            assert_eq!(
                reconcile_fact(
                    &mut record,
                    BackendFact::Stopped {
                        job: JobId(10),
                        reason,
                    },
                    None,
                    100,
                    Path::new("/unused"),
                ),
                ReconcileOutcome::default()
            );
        }
    }

    #[test]
    fn disabled_runtime_names_live_row_count() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        assert_eq!(
            refuse_disabled_with_live_torrents(&[job(10, hash, JobStatus::Paused)]),
            Err(DisabledWithLiveTorrents { count: 1 })
        );
    }

    #[test]
    fn restore_rejects_parent_escape_from_payload_root() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("torrents");
        std::fs::create_dir(&root).unwrap();
        let mut escaped = job(
            10,
            "0123456789abcdef0123456789abcdef01234567",
            JobStatus::Paused,
        );
        escaped.torrent.as_mut().unwrap().content_path =
            Some(root.join("category").join("..").join("..").join("outside"));

        let plan = plan_restore(&[escaped], &HashMap::new(), &root, &HashSet::new());

        assert!(plan.requests.is_empty());
        assert_eq!(plan.diagnostics, vec![RestoreDiagnostic::UnsafePayloadRoot]);
    }

    #[cfg(unix)]
    #[test]
    fn restore_rejects_symlink_escape_from_payload_root() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("torrents");
        let outside = temp.path().join("outside");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&outside).unwrap();
        symlink(&outside, root.join("redirect")).unwrap();
        let mut escaped = job(
            10,
            "0123456789abcdef0123456789abcdef01234567",
            JobStatus::Paused,
        );
        escaped.torrent.as_mut().unwrap().content_path =
            Some(root.join("redirect").join("not-created-yet"));

        let plan = plan_restore(&[escaped], &HashMap::new(), &root, &HashSet::new());

        assert!(plan.requests.is_empty());
        assert_eq!(plan.diagnostics, vec![RestoreDiagnostic::UnsafePayloadRoot]);
    }
}
