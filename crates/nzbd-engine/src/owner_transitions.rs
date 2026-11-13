//! Durable actor transitions. PP updates retain custody; replacements retire it.
use super::*;

fn failure(message: impl ToString) -> nzbd_state::StateError {
    nzbd_state::StateError::Corrupt(message.to_string())
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterRangeScope {
    job_id: u32,
    file_id: u32,
    first_article: u32,
    last_article: u32,
    range_index: u32,
    range_count: u32,
}

pub(super) struct PayloadPlan {
    pub(super) path: PathBuf,
    pub(super) root: PathBuf,
    pub(super) private_range: bool,
}

/// Only an exact typed article scope can select the nested private root.
pub(super) fn imported_payload_plan(
    job: &Job,
    destination: &Path,
) -> Result<PayloadPlan, nzbd_state::StateError> {
    let relative = PathBuf::from(job_dir_name(job));
    if relative
        .components()
        .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err(failure("unsafe payload storage namespace"));
    }
    let path = destination.join(&relative);
    let mut ranges = job.params.iter().filter(|(key, _)| key == "*Cluster:range");
    let Some((_, encoded)) = ranges.next() else {
        return Ok(PayloadPlan {
            path,
            root: destination.to_path_buf(),
            private_range: false,
        });
    };
    if ranges.next().is_some() {
        return Err(failure("duplicate cluster article scope"));
    }
    let scope: ClusterRangeScope = serde_json::from_str(encoded).map_err(failure)?;
    let file = job
        .files
        .first()
        .filter(|file| job.files.len() == 1 && file.id.0 == scope.file_id)
        .ok_or_else(|| failure("cluster article scope names another file"))?;
    let span = scope
        .last_article
        .checked_sub(scope.first_article)
        .and_then(|span| span.checked_add(1));
    let mut articles = file
        .segments
        .iter()
        .map(|segment| segment.number)
        .collect::<Vec<_>>();
    articles.sort_unstable();
    if scope.job_id != job.id.0
        || scope.first_article == 0
        || scope.range_count == 0
        || scope.range_index >= scope.range_count
        || span != u32::try_from(articles.len()).ok()
        || articles.first() != Some(&scope.first_article)
        || articles.last() != Some(&scope.last_article)
        || articles
            .windows(2)
            .any(|pair| pair[0].checked_add(1) != Some(pair[1]))
    {
        return Err(failure(
            "cluster article scope does not exactly cover its file",
        ));
    }
    let prefix = format!(
        ".nzbd-cluster/range-work/job-{}/file-{}/{}-{}-fence-",
        scope.job_id, scope.file_id, scope.first_article, scope.last_article
    );
    let namespace = relative
        .to_str()
        .ok_or_else(|| failure("cluster article namespace is not UTF-8"))?;
    let fence = namespace
        .strip_prefix(&prefix)
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|fence| *fence > 0)
        .ok_or_else(|| failure("cluster article namespace does not match its fenced scope"))?;
    if namespace != format!("{prefix}{fence}") {
        return Err(failure("noncanonical cluster article fence namespace"));
    }
    let root = path
        .parent()
        .ok_or_else(|| failure("cluster article namespace has no private parent"))?
        .to_path_buf();
    Ok(PayloadPlan {
        path,
        root,
        private_range: true,
    })
}

/// Called only on the Engine's bounded payload lane. Descriptor-relative
/// parent creation preserves the immediate-child Inventory allocation rule.
pub(super) fn allocate_imported_payload(
    inventory: &Arc<nzbd_state::artifacts::Inventory>,
    record: &Job,
    destination: &Path,
    plan: &PayloadPlan,
    allocation: Option<nzbd_state::artifacts::Artifact>,
) -> Result<nzbd_state::artifacts::Artifact, nzbd_state::StateError> {
    if plan.private_range {
        let enrolled = inventory.startup_reconciliation_roots().map_err(failure)?;
        if !enrolled.iter().any(|root| root == &plan.root) {
            prepare_unenrolled_payload_root(inventory, destination)?;
            let parent = plan.root.strip_prefix(destination).map_err(failure)?;
            nzbd_state::fileops::parents(destination, parent).map_err(failure)?;
        }
    }
    let keep = record
        .params
        .iter()
        .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1");
    let allocation = match allocation {
        Some(previous)
            if plan.private_range && (previous.path != plan.path || previous.root != plan.root) =>
        {
            inventory
                .allocate_processing_successor_with_policy(
                    record.id.0,
                    &plan.root,
                    &plan.path,
                    Some((record.lifecycle_eligible(), record.delivery_lifecycle, keep)),
                )
                .map_err(failure)?
        }
        Some(previous) => inventory
            .reclaim_recovered_orphan(record.id.0)
            .map_err(failure)?
            .unwrap_or(previous),
        None => {
            if !plan.path.try_exists().map_err(failure)? {
                if record.files.iter().any(|file| file.has_any_done()) {
                    return Err(failure(
                        "recovered payload directory is unavailable; completion evidence retained",
                    ));
                }
                prepare_unenrolled_payload_root(inventory, &plan.root)?;
                inventory
                    .allocate_for_delivery_with_keep(
                        record.id.0,
                        &plan.root,
                        &plan.path,
                        record.lifecycle_eligible(),
                        record.delivery_lifecycle,
                        keep,
                    )
                    .map_err(failure)?
            } else {
                inventory
                    .register_legacy_active(record.id.0, &plan.root, &plan.path)
                    .map_err(failure)?;
                inventory
                    .for_job(record.id.0)
                    .map_err(failure)?
                    .ok_or_else(|| failure("legacy enrollment returned no allocation"))?
            }
        }
    };
    if allocation.state == "active" {
        inventory
            .allocate(record.id.0, &allocation.root, &allocation.path)
            .map_err(failure)?;
    } else {
        // Review #283-1: behind a restart's reconciliation pass on this
        // source (its publication move being finished), not against it.
        let _custody = inventory
            .acquire_terminal_use_after_reconciliation(record.id.0, &|| Ok(()))
            .map_err(failure)?
            .ok_or_else(|| failure("retained allocation has no custody evidence"))?;
    }
    Ok(allocation)
}

/// This classification is used only after journal replay and fresh allocation
/// reconciliation. A display byte counter is never sufficient evidence.
fn pristine_download(job: &Job) -> bool {
    matches!(
        job.status,
        JobStatus::Queued | JobStatus::Paused | JobStatus::Held
    ) && job.stages.is_empty()
        && !job.files.iter().any(|file| {
            file.finalized
                || file.crc32.is_some()
                || file
                    .segments
                    .iter()
                    .any(|segment| matches!(segment.state, SegmentState::Done { .. }))
        })
        && !job.params.iter().any(|(key, _)| {
            key.starts_with("*File:proof:")
                || key.starts_with("*File:retired-proof:")
                || key.starts_with("*File:publication:")
                || key.starts_with("*Recovery:publication-repair")
        })
}

fn needs_payload(job: &Job) -> bool {
    job.torrent.is_none()
        && !failed_cleanup_only(job)
        && !job.files.is_empty()
        && !job
            .params
            .iter()
            .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
        && job.status != JobStatus::Deleted
}

/// Remove `.runner-file-<id>.part` names left behind by a finalize that
/// published the file (link, then unlink of the partial) but was killed
/// before the unlink. A partial is removed only when its file is finalized
/// and the published name is the *same object* (device + inode), so the
/// removal drops a second name and never the only copy of the payload.
/// Every other partial — unfinalized, unknown to the job, or a different
/// object than the publication — stays for the job's own lifecycle
/// (resume, salvage, closed-scope cleanup or disposal).
pub(super) fn sweep_stale_parts(dir: &std::path::Path, finalized: &HashMap<u32, String>) -> usize {
    use std::os::unix::fs::MetadataExt;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(id) = name
            .to_str()
            .and_then(|name| name.strip_prefix(".runner-file-"))
            .and_then(|rest| rest.strip_suffix(".part"))
            .and_then(|id| id.parse::<u32>().ok())
        else {
            continue;
        };
        let Some(published) = finalized.get(&id) else {
            continue;
        };
        if !crate::recovery::safe_name(published) {
            continue;
        }
        let (Ok(part), Ok(publication)) = (
            std::fs::symlink_metadata(entry.path()),
            std::fs::symlink_metadata(dir.join(published)),
        ) else {
            continue;
        };
        if !part.is_file()
            || !publication.is_file()
            || part.dev() != publication.dev()
            || part.ino() != publication.ino()
        {
            continue;
        }
        match std::fs::remove_file(entry.path()) {
            Ok(()) => {
                removed += 1;
                tracing::info!(path = %entry.path().display(), "removed the leftover partial name of a published file");
            }
            Err(error) => {
                tracing::warn!(path = %entry.path().display(), %error, "leftover partial name remains");
            }
        }
    }
    removed
}

pub(super) fn failed_cleanup_only(job: &Job) -> bool {
    job.params
        .iter()
        .any(|(key, _)| key == "*Recovery:failed-cleanup")
}

fn failed_cleanup_marker(proof: &nzbd_state::artifacts::FailedCleanupProof) -> String {
    serde_json::to_string(&(
        proof.source().artifact.as_str(),
        proof.source().generation.as_str(),
        proof.operation_id(),
    ))
    .unwrap()
}

fn failed_cleanup_marker_matches(
    job: &Job,
    proof: &nzbd_state::artifacts::FailedCleanupProof,
) -> bool {
    let markers = job
        .params
        .iter()
        .filter(|(key, _)| key == "*Recovery:failed-cleanup")
        .collect::<Vec<_>>();
    markers.is_empty() || (markers.len() == 1 && markers[0].1 == failed_cleanup_marker(proof))
}

fn failed_cleanup_record(job: &Job) -> bool {
    job.kind == JobKind::Nzb
        && job.status == JobStatus::Failed
        && !job.held()
        && job
            .params
            .iter()
            .filter(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
            .all(|(_, value)| failed_cleanup_only(job) && value.starts_with("FAILURE/"))
        && ["*PP:failure-at", "*PP:failure-disposition-attempts"]
            .iter()
            .all(|name| {
                job.params
                    .iter()
                    .find(|(key, _)| key == name)
                    .and_then(|(_, value)| value.parse::<u64>().ok())
                    .is_some_and(|value| value > 0)
            })
}

fn cleanup_metadata_key(key: &str) -> bool {
    matches!(
        key,
        nzbd_types::PP_DONE_PARAM
            | "*PP:history-seq"
            | "*PP:failure-disposition-attempts"
            | "Failure:Stage"
            | "Failure:Reason"
            | "Failure:KeepOnFailure"
            | "Failure:Files"
            | "Artifact:Id"
            | "Artifact:Generation"
            | "Artifact:Custody"
            | "*PP:selected-publication-outcome"
            | "*PP:selected-publication-failure-cause"
    )
}

fn cleanup_update_matches(
    previous: &Job,
    candidate: &Job,
    proof: &nzbd_state::artifacts::FailedCleanupProof,
) -> bool {
    if previous.status != JobStatus::Failed
        || candidate.status != JobStatus::Failed
        || candidate
            .params
            .iter()
            .any(|(key, value)| key == nzbd_types::PP_DONE_PARAM && !value.starts_with("FAILURE/"))
    {
        return false;
    }
    if candidate
        .params
        .iter()
        .any(|(key, value)| match key.as_str() {
            "Artifact:Id" => value != &proof.source().artifact,
            "Artifact:Generation" => value != &proof.source().generation,
            "Artifact:Custody" => !matches!(
                value.as_str(),
                "runner_owned" | "kept_by_user" | "deleted_by_runner"
            ),
            "*PP:selected-publication-outcome" | "*PP:selected-publication-failure-cause" => {
                !previous.params.contains(&(key.clone(), value.clone()))
            }
            _ => false,
        })
    {
        return false;
    }
    let attempts = |row: &Job| {
        row.params
            .iter()
            .find(|(key, _)| key == "*PP:failure-disposition-attempts")
            .and_then(|(_, value)| value.parse::<u64>().ok())
    };
    if !attempts(previous)
        .zip(attempts(candidate))
        .is_some_and(|(old, new)| new >= old)
    {
        return false;
    }
    let mut previous = previous.clone();
    let mut candidate = candidate.clone();
    candidate.category = previous.category.clone();
    candidate.priority = previous.priority;
    candidate
        .params
        .retain(|(key, _)| key != nzbd_types::CONTROL_PARAM);
    previous
        .params
        .retain(|(key, _)| key != nzbd_types::CONTROL_PARAM);
    previous
        .params
        .retain(|(key, _)| !cleanup_metadata_key(key));
    candidate
        .params
        .retain(|(key, _)| !cleanup_metadata_key(key));
    serde_json::to_value(previous).ok() == serde_json::to_value(candidate).ok()
}
fn preserve_user_intent(old: &mut Job, current: &Job) {
    old.category = current.category.clone();
    old.priority = current.priority;
    if let Some(control) = current.control() {
        old.set_control(&control);
    }
    if current.held() {
        old.status = JobStatus::Held;
    } else if current.status == JobStatus::Paused {
        old.status = JobStatus::Paused;
    }
    for file in &mut old.files {
        if let Some(current) = current
            .files
            .iter()
            .find(|candidate| candidate.id == file.id)
        {
            file.paused = current.paused;
        }
    }
}
/// Rollback starts from the last durable row. Only intent changed after the
/// provisional row was installed belongs to a concurrent operator command;
/// unchanged candidate fields must not leak back into the restored record.
pub(super) fn preserve_intent_changes(restored: &mut Job, current: &Job, provisional: &Job) {
    if current.category != provisional.category {
        restored.category = current.category.clone();
    }
    if current.priority != provisional.priority {
        restored.priority = current.priority;
    }
    if current.control() != provisional.control() {
        restored
            .params
            .retain(|(key, _)| key != nzbd_types::CONTROL_PARAM);
        if let Some(control) = current.control() {
            restored.set_control(&control);
        }
    }
    if current.status != provisional.status
        && matches!(current.status, JobStatus::Paused | JobStatus::Queued)
    {
        restored.status = current.status;
    }
    if restored.held() {
        restored.status = JobStatus::Held;
    } else {
        // The durable row's hold may have been released since.
        restored.normalize_hold_status();
    }
    for file in &mut restored.files {
        if let Some((current, provisional)) = current
            .files
            .iter()
            .find(|candidate| candidate.id == file.id)
            .zip(
                provisional
                    .files
                    .iter()
                    .find(|candidate| candidate.id == file.id),
            )
        {
            if current.paused != provisional.paused {
                file.paused = current.paused;
            }
        }
    }
}

fn normalize(job: &mut Job) {
    for file in &mut job.files {
        for segment in &mut file.segments {
            if matches!(segment.state, SegmentState::Leased { .. }) {
                segment.state = SegmentState::Pending;
            }
        }
    }
    if job.status == JobStatus::Downloading {
        job.status = JobStatus::Queued;
    }
    recompute_job_totals(job);
    if let Some(control) = job
        .control()
        .filter(|control| control.version == 1 && control.lifecycle == "held")
        .and_then(|control| rearmed_hold(control, unix_now()))
    {
        job.set_control(&control);
    }
    if needs_payload(job) {
        set_owner_param(&mut job.params, "*Recovery:pending", Some("1".into()));
        set_owner_param(
            &mut job.params,
            "*Recovery:journal-loading",
            Some("1".into()),
        );
    }
}

pub(super) struct StartupRoot {
    ready: bool,
    running: bool,
    retry_at: Instant,
    reason: Option<String>,
}

impl Owner {
    pub(super) fn begin_failed_cleanup_admission_then(
        &mut self,
        job: JobId,
        callback: impl FnOnce(&mut Owner, Option<crate::FailedCleanupAdmission>) + Send + 'static,
    ) {
        if self.cancel.is_cancelled()
            || !self.persist
            || self.persistence_fenced
            || !self.state.job(job).is_some_and(failed_cleanup_record)
            || self.post_admissions.contains_key(&job)
            || self.allocating_jobs.contains(&job)
            || self.pending_control_commits.contains(&job)
            || self.record_transitions.contains(&job)
            || self.projection_transition
            || self.retiring_writers.contains_key(&job)
            || self
                .verification
                .get(&job)
                .is_some_and(|state| state.running)
            || self.state.job(job).is_some_and(|record| {
                record.files.iter().any(|file| {
                    self.pending_writer_birth.contains_key(&file.id)
                        || self
                            .writers
                            .get(&file.id)
                            .is_some_and(|writer| !*writer.stopped.borrow())
                })
            })
        {
            callback(self, None);
            return;
        }
        let generation = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
        let control_revision = self.control_epochs.get(&job).copied();
        self.record_transitions.insert(job);
        self.pending_control_commits.insert(job);
        let inventory = self.artifacts.clone();
        self.io_then(
            move || inventory.failed_cleanup_proof(job.0).map_err(failure),
            move |owner, proof| {
                owner.record_transitions.remove(&job);
                owner.pending_control_commits.remove(&job);
                let proof = match proof {
                    Ok(Some(proof))
                        if owner.job_epochs.get(&job) == Some(&generation)
                            && owner.control_epochs.get(&job).copied() == control_revision
                            && owner.state.job(job).is_some_and(failed_cleanup_record)
                            && owner
                                .state
                                .job(job)
                                .is_some_and(|row| failed_cleanup_marker_matches(row, &proof))
                            && !owner.cancel.is_cancelled()
                            && owner.persist
                            && !owner.persistence_fenced
                            && !owner.post_admissions.contains_key(&job)
                            && owner.persist_guard.as_ref().is_none_or(|guard| guard()) =>
                    {
                        proof
                    }
                    _ => {
                        owner.drain_replacements();
                        callback(owner, None);
                        return;
                    }
                };
                let cancellation = owner.cancel.child_token();
                let files = owner
                    .state
                    .job(job)
                    .unwrap()
                    .files
                    .iter()
                    .map(|file| file.id)
                    .collect::<Vec<_>>();
                for file in files {
                    owner.writer_epochs.remove(&file);
                    owner.writers.remove(&file);
                }
                let admission = crate::PostAdmission {
                    job,
                    generation,
                    cancellation: cancellation.clone(),
                };
                owner.post_admissions.insert(job, generation);
                owner.post_cancellations.insert(job, cancellation);
                owner.failed_cleanup_admissions.insert(job, proof.clone());
                let row = owner.state.job(job).unwrap().clone();
                callback(
                    owner,
                    Some(crate::FailedCleanupAdmission {
                        admission,
                        job: row,
                        cleanup: proof,
                    }),
                );
            },
        );
    }

    pub(super) fn materialize_generation_then(
        &mut self,
        request: GenerationMaterialization,
        callback: impl FnOnce(&mut Owner, Result<(), nzbd_state::StateError>) + Send + 'static,
    ) {
        let job = request.job.id;
        self.boot_jobs.remove(&job);
        if !self.persist
            || self.cancel.is_cancelled()
            || self.allocating_jobs.contains(&job)
            || self.record_transitions.contains(&job)
            || self.pending_control_commits.contains(&job)
            || self.post_admissions.contains_key(&job)
            || self.retiring_writers.contains_key(&job)
            || self
                .verification
                .get(&job)
                .is_some_and(|state| state.running)
        {
            callback(
                self,
                Err(failure(
                    "generation publication has not acquired quiescent authority",
                )),
            );
            return;
        }
        if !matches!(
            request.job.status,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
        ) || !request.root.is_absolute()
            || request.target.parent() != Some(request.root.as_path())
            || request.target != request.root.join(job_dir_name(&request.job))
            || request.root.components().any(|part| {
                matches!(
                    part,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
        {
            callback(
                self,
                Err(failure(
                    "accepted generation must be terminal with an immediate-child publication target",
                )),
            );
            return;
        }
        if request.history.is_some() && self.history.is_none() {
            callback(
                self,
                Err(failure("accepted generation history store is unavailable")),
            );
            return;
        }
        let import_existing = self.state.job(job).is_some();
        let record = self.state.job(job).unwrap_or(&request.job);
        let policy = (
            record.lifecycle_eligible(),
            record.delivery_lifecycle,
            record
                .params
                .iter()
                .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1"),
        );
        let epoch = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
        self.allocating_jobs.insert(job);
        self.record_transitions.insert(job);
        self.pending_control_commits.insert(job);
        let inventory = self.artifacts.clone();
        let root = request.root.clone();
        let canonical = request.job.clone();
        let history = request.history;
        let emit_finished = request.emit_finished;
        let authority = self.persist_guard.clone();
        let cancellation = self.cancel.clone();
        let history_authority = authority.clone();
        let history_cancel = cancellation.clone();
        self.payload_then(root, move || {
            let checkpoint = || {
                if cancellation.is_cancelled() || authority.as_ref().is_some_and(|guard| !guard()) {
                    Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "generation publication authority changed"))
                } else { Ok(()) }
            };
            inventory.materialize_generation_checked(job.0, &request.root, &request.target,
                &request.generation, policy, &checkpoint).map_err(failure)
        }, move |owner, result| {
            owner.allocating_jobs.remove(&job);
            let allocation = match result {
                Ok(allocation) => allocation,
                Err(error) => {
                    owner.record_transitions.remove(&job);
                    if !owner.pending_control_revisions.contains_key(&job) { owner.pending_control_commits.remove(&job); }
                    if owner.state.job(job).is_some() {
                        owner.queue_verification(job);
                        owner.payload_recovery_failed(job, failure(&error));
                    }
                    callback(owner, Err(error));
                    owner.drain_replacements();
                    return;
                }
            };
            owner.allocation_cache.insert(job, allocation);
            owner.allocated_jobs.insert(job);
            if let Some(verification) = owner.verification.remove(&job) { verification.cancel.cancel(); }
            // Keep the operational fence through durable history and canonical
            // queue import. Takeover may have normalized the current row Queued.
            let finish = move |owner: &mut Owner, history_result: Result<(), nzbd_state::StateError>| {
                owner.record_transitions.remove(&job);
                if !owner.pending_control_revisions.contains_key(&job) { owner.pending_control_commits.remove(&job); }
                if let Err(error) = history_result {
                    if owner.state.job(job).is_some() {
                        owner.queue_verification(job);
                        owner.payload_recovery_failed(job, failure(&error));
                    }
                    callback(owner, Err(error));
                    owner.drain_replacements();
                    return;
                }
                if !owner.persist || owner.cancel.is_cancelled() || owner.job_epochs.get(&job) != Some(&epoch)
                    || owner.pending_control_revisions.contains_key(&job) {
                    if owner.state.job(job).is_some() { owner.queue_payload_recovery(job); }
                    callback(owner, Err(failure("generation publication authority or control revision changed before queue import")));
                    owner.drain_replacements();
                    return;
                }
                if import_existing && owner.state.job(job).is_some() {
                    owner.replace_job_then(canonical, false, emit_finished, true, true, callback);
                } else {
                    // Historical accepted results can rebuild custody/history,
                    // but never resurrect a retired queue job.
                    callback(owner, Ok(()));
                    owner.drain_replacements();
                }
            };
            if let Some(entry) = history {
                let history = owner.history.clone().unwrap();
                owner.io_then(move || {
                    if history_cancel.is_cancelled() || history_authority.as_ref().is_some_and(|guard| !guard()) {
                        return Err(failure("generation publication authority changed before history"));
                    }
                    history.record_seq_durable(&entry).map(|_| ()).map_err(failure)
                }, finish);
            } else { finish(owner, Ok(())); }
        });
    }

    pub(super) fn post_scope_current(&self, admission: &crate::PostAdmission) -> bool {
        self.post_admissions.get(&admission.job) == Some(&admission.generation)
            && self.job_epochs.get(&admission.job) == Some(&admission.generation)
            && !admission.cancellation.is_cancelled()
            && self
                .post_cancellations
                .get(&admission.job)
                .is_some_and(|cancel| !cancel.is_cancelled())
    }

    pub(super) fn apply_post_record(
        &mut self,
        admission: crate::PostAdmission,
        candidate: Job,
        require_snapshot: bool,
        callback: impl FnOnce(&mut Owner, bool) + Send + 'static,
    ) {
        self.apply_post_record_proven(admission, candidate, require_snapshot, None, callback);
    }

    pub(super) fn apply_post_record_proven(
        &mut self,
        admission: crate::PostAdmission,
        candidate: Job,
        require_snapshot: bool,
        completion_proof: Option<nzbd_state::artifacts::WorkerCompletionProof>,
        callback: impl FnOnce(&mut Owner, bool) + Send + 'static,
    ) {
        if let Some(proof) = self.failed_cleanup_admissions.get(&admission.job).cloned() {
            let inventory = self.artifacts.clone();
            let custody = candidate
                .params
                .iter()
                .find(|(key, _)| key == "Artifact:Custody")
                .map(|(_, value)| value.clone());
            self.io_then(
                move || {
                    inventory
                        .validate_failed_cleanup_proof(&proof)
                        .map_err(failure)?;
                    let source = inventory.get(&proof.source().artifact).map_err(failure)?;
                    if custody.as_deref() == Some("deleted_by_runner")
                        && !matches!(source.state.as_str(), "deleted" | "source_gone")
                    {
                        return Err(failure("failed cleanup source deletion is not recorded"));
                    }
                    if custody.as_deref() == Some("kept_by_user")
                        && !source.keep
                        && !source.keep_on_failure
                    {
                        return Err(failure(
                            "failed cleanup source has no user retention choice",
                        ));
                    }
                    Ok(())
                },
                move |owner, validated| {
                    if validated.is_err() {
                        callback(owner, false);
                        return;
                    }
                    owner.apply_post_record_proven_ready(
                        admission,
                        candidate,
                        require_snapshot,
                        completion_proof,
                        callback,
                    );
                },
            );
        } else {
            self.apply_post_record_proven_ready(
                admission,
                candidate,
                require_snapshot,
                completion_proof,
                callback,
            );
        }
    }

    fn apply_post_record_proven_ready(
        &mut self,
        admission: crate::PostAdmission,
        mut candidate: Job,
        require_snapshot: bool,
        completion_proof: Option<nzbd_state::artifacts::WorkerCompletionProof>,
        callback: impl FnOnce(&mut Owner, bool) + Send + 'static,
    ) {
        if !self.post_scope_current(&admission)
            || self.record_transitions.contains(&admission.job)
            || self.pending_control_revisions.contains_key(&admission.job)
            || self.projection_transition
            || admission.job != candidate.id
            || (require_snapshot && !self.persist && !self.worker_mode)
        {
            callback(self, false);
            return;
        }
        if completion_proof.is_some() {
            match self.prepare_replacement_control(&mut candidate, completion_proof.as_ref()) {
                Ok((true, _)) => {
                    callback(self, true);
                    return;
                }
                Ok((false, _)) => {}
                Err(_) => {
                    callback(self, false);
                    return;
                }
            }
        }
        let Some(previous) = self.state.job(candidate.id).cloned() else {
            callback(self, false);
            return;
        };
        if let Some(proof) = self.failed_cleanup_admissions.get(&admission.job) {
            if completion_proof.is_some() || !cleanup_update_matches(&previous, &candidate, proof) {
                callback(self, false);
                return;
            }
            candidate
                .params
                .retain(|(key, _)| key != nzbd_types::CONTROL_PARAM);
            if let Some(control) = previous.control() {
                candidate.set_control(&control);
            }
            set_owner_param(
                &mut candidate.params,
                "*Recovery:failed-cleanup",
                Some(failed_cleanup_marker(proof)),
            );
        }
        if previous.kind != candidate.kind
            || job_dir_name(&previous) != job_dir_name(&candidate)
            || previous
                .files
                .iter()
                .map(|file| file.id)
                .collect::<HashSet<_>>()
                != candidate
                    .files
                    .iter()
                    .map(|file| file.id)
                    .collect::<HashSet<_>>()
        {
            callback(self, false);
            return;
        }
        if previous.held()
            && candidate
                .params
                .iter()
                .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
        {
            callback(self, false);
            return;
        }
        // An earlier PP export cannot erase newer download acknowledgements or
        // operator control while the same admission awaits tools.
        for file in &mut candidate.files {
            if let Some(current) = previous.files.iter().find(|current| current.id == file.id) {
                for segment in &mut file.segments {
                    if let Some(current) = current
                        .segments
                        .iter()
                        .find(|current| current.number == segment.number)
                    {
                        if matches!(current.state, SegmentState::Done { .. }) {
                            segment.state = current.state;
                        }
                    }
                }
                file.finalized |= current.finalized;
            }
        }
        preserve_user_intent(&mut candidate, &previous);
        recompute_job_totals(&mut candidate);
        let job = candidate.id;
        let revision = fresh_epoch();
        self.control_epochs.insert(job, revision);
        let lifetime = admission.generation;
        let provisional = candidate.clone();
        *self.state.job_mut(job).unwrap() = candidate;
        self.pending_control_commits.insert(job);
        self.record_transitions.insert(job);
        self.dirty = true;
        self.persist_then(move |owner, committed| {
            let current = owner.post_scope_current(&admission)
                && owner.control_epochs.get(&job) == Some(&revision);
            if committed && current {
                owner.pending_control_commits.remove(&job);
                owner.record_transitions.remove(&job);
                owner.drain_replacements();
                owner.publish_now();
                callback(owner, true);
                return;
            }
            // The record transition still excludes replacements. Deferred
            // removal/adoption may invalidate the admission's epoch, but the
            // provisional row must still be rolled back before they run.
            if let Some(current) = owner.state.job(job).cloned() {
                let mut restore = previous;
                preserve_intent_changes(&mut restore, &current, &provisional);
                *owner.state.job_mut(job).unwrap() = restore;
                owner.post_rollbacks.insert(
                    job,
                    PostRollback {
                        generation: owner.job_epochs.get(&job).copied().unwrap_or(lifetime),
                        control_revision: owner.control_epochs.get(&job).copied(),
                        in_flight: false,
                    },
                );
                owner.dirty = true;
                owner.persist_post_rollbacks_then(move |owner| {
                    callback(owner, false);
                });
            } else {
                owner.pending_control_commits.remove(&job);
                owner.record_transitions.remove(&job);
                owner.drain_replacements();
                callback(owner, false);
            }
        });
    }

    /// Failed rollback writes retain their operational fence until a later
    /// snapshot acknowledges the restored row. Batch all waiting rows into
    /// one document, and never enqueue another retry while one is in flight.
    pub(super) fn persist_post_rollbacks_then(
        &mut self,
        callback: impl FnOnce(&mut Owner) + Send + 'static,
    ) {
        if self.projection_transition
            || self.persistence_fenced
            || (!self.persist && !self.worker_mode)
        {
            callback(self);
            return;
        }
        let waiting = self
            .post_rollbacks
            .iter()
            .filter_map(|(job, rollback)| (!rollback.in_flight).then_some(*job))
            .collect::<Vec<_>>();
        let mut dispatched = Vec::new();
        for job in waiting {
            let Some(generation) = self
                .job_epochs
                .get(&job)
                .copied()
                .filter(|_| self.state.job(job).is_some())
            else {
                self.post_rollbacks.remove(&job);
                self.record_transitions.remove(&job);
                self.pending_control_commits.remove(&job);
                continue;
            };
            let fence = PostRollback {
                generation,
                control_revision: self.control_epochs.get(&job).copied(),
                in_flight: true,
            };
            self.post_rollbacks.insert(job, fence);
            dispatched.push((job, fence));
        }
        if dispatched.is_empty() {
            callback(self);
            return;
        }
        self.persist_then(move |owner, committed| {
            let mut released = false;
            for (job, fence) in dispatched {
                if owner.post_rollbacks.get(&job) != Some(&fence) {
                    continue;
                }
                let current = owner.job_epochs.get(&job) == Some(&fence.generation)
                    && owner.control_epochs.get(&job).copied() == fence.control_revision;
                if committed && current {
                    owner.post_rollbacks.remove(&job);
                    owner.record_transitions.remove(&job);
                    owner.pending_control_commits.remove(&job);
                    released = true;
                } else if let Some(rollback) = owner.post_rollbacks.get_mut(&job) {
                    rollback.in_flight = false;
                }
            }
            owner.drain_replacements();
            owner.publish_now();
            if released {
                owner.bump_epoch();
            }
            callback(owner);
        });
    }

    pub(super) fn settled_post_rollback(&self, job: JobId) -> bool {
        self.post_rollbacks
            .get(&job)
            .is_some_and(|rollback| !rollback.in_flight)
    }

    /// Authority replacement can supersede a failed local rollback, but only
    /// its own durable acknowledgment can release the inherited barriers.
    fn clear_post_rollback_barriers(&mut self) {
        for (job, _) in self.post_rollbacks.drain() {
            self.record_transitions.remove(&job);
            self.pending_control_commits.remove(&job);
        }
    }

    pub(super) fn finish_post_admission(
        &mut self,
        admission: crate::PostAdmission,
        callback: impl FnOnce(&mut Owner, bool) + Send + 'static,
    ) {
        if !self.post_scope_current(&admission)
            || self.failed_cleanup_admissions.contains_key(&admission.job)
        {
            callback(self, false);
            return;
        }
        // Manager sends this only after its actual tool/reader worker drain.
        self.post_admissions.remove(&admission.job);
        self.post_cancellations.remove(&admission.job);
        if !self.remove_job_silent(admission.job) {
            callback(self, false);
            return;
        }
        self.persist_then(move |owner, ok| {
            owner.drain_replacements();
            callback(owner, ok);
        });
    }

    /// Validate the imported grant and completion against the current local
    /// generation before entering the asynchronous durable replacement lane.
    fn prepare_replacement_control(
        &self,
        job: &mut Job,
        completion_proof: Option<&nzbd_state::artifacts::WorkerCompletionProof>,
    ) -> Result<(bool, bool), nzbd_state::StateError> {
        if self.persistence_fenced
            || (self.persist && self.persist_guard.as_ref().is_some_and(|guard| !guard()))
        {
            return Err(failure(
                "import authority, source generation, or worker control changed",
            ));
        }
        let preserve_publication = if let Some(proof) = completion_proof {
            let exact_context = nzbd_state::artifacts::WorkerControlContext::for_job(job).as_ref()
                == Some(&proof.context)
                && self
                    .state
                    .job(job.id)
                    .and_then(nzbd_state::artifacts::WorkerControlContext::for_job)
                    .as_ref()
                    == Some(&proof.context);
            let exact_source =
                self.artifacts
                    .for_job(job.id.0)
                    .ok()
                    .flatten()
                    .is_some_and(|artifact| {
                        artifact.id == proof.source.artifact
                            && artifact.generation == proof.source.generation
                            && artifact.owned
                    });
            if proof.job != job.id || !exact_context || !exact_source {
                return Err(failure(
                    "import authority, source generation, or worker control changed",
                ));
            }
            // Acceptance is terminal. Late selected-outcome preparations may
            // acknowledge it but must never regress PP_DONE or queue state.
            match self
                .artifacts
                .worker_completion_accepted(&proof.context, &proof.source)
            {
                Ok(true) => return Ok((true, true)),
                Ok(false) => {}
                Err(_) => return Err(failure("worker completion acceptance unavailable")),
            }
            if proof.accepted {
                return Ok((true, true));
            }
            if let Some(done) = self.state.job(job.id).and_then(|current| {
                current
                    .params
                    .iter()
                    .find(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
                    .map(|(_, value)| value)
            }) {
                return if job
                    .params
                    .iter()
                    .find(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
                    .is_none_or(|(_, value)| value == done)
                {
                    Ok((true, true))
                } else {
                    Err(failure("worker completion conflicts with terminal outcome"))
                };
            }
            true
        } else {
            false
        };
        let mut worker_control_restored = false;
        let incoming_context = nzbd_state::artifacts::WorkerControlContext::for_job(job);
        let current_context = self
            .state
            .job(job.id)
            .and_then(nzbd_state::artifacts::WorkerControlContext::for_job);
        if let (Some(incoming), Some(current)) = (&incoming_context, &current_context) {
            if incoming.job_incarnation == current.job_incarnation
                && (incoming.epoch < current.epoch
                    || incoming.job_revision < current.job_revision
                    || incoming.control_revision < current.control_revision)
            {
                return Err(failure(
                    "import authority, source generation, or worker control changed",
                ));
            }
        }
        if self.worker_mode {
            if let Some(context) = incoming_context.as_ref() {
                if let Err(error) = self.artifacts.bind_worker_context(job.id.0, context) {
                    tracing::warn!(job=job.id.0, %error, "worker grant binding refused");
                    return Err(failure(
                        "import authority, source generation, or worker control changed",
                    ));
                }
                match self.artifacts.worker_control_for_job_metadata(job, context) {
                    Ok(Some(checkpoint)) => {
                        worker_control_restored = true;
                        job.set_control(&checkpoint.control);
                        job.status = if checkpoint.control.lifecycle == "held" {
                            JobStatus::Held
                        } else if checkpoint.control.manual_pause {
                            JobStatus::Paused
                        } else if matches!(
                            checkpoint.control.previous_status,
                            Some(
                                JobStatus::Post { .. }
                                    | JobStatus::PostQueued
                                    | JobStatus::Completed
                            )
                        ) {
                            JobStatus::PostQueued
                        } else {
                            JobStatus::Queued
                        };
                        job.params.retain(|(key, _)| {
                            !matches!(key.as_str(), "*Control:attempts" | "*Control:probe_after")
                        });
                        job.params.extend(checkpoint.probe_params);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(job=job.id.0, %error, "worker control recovery refused");
                        return Err(failure(
                            "import authority, source generation, or worker control changed",
                        ));
                    }
                }
            }
        }
        if let Some(control) = self
            .state
            .job(job.id)
            .and_then(|j| j.control())
            .filter(|c| {
                c.lifecycle == "held"
                    && !worker_control_restored
                    && (incoming_context.is_none() && current_context.is_none()
                        || incoming_context == current_context)
            })
        {
            // Remote/exported legacy snapshots cannot erase a newer local fence.
            job.set_control(&control);
            job.status = JobStatus::Held;
            job.params.retain(|(k, _)| k != nzbd_types::PP_DONE_PARAM);
        }
        Ok((false, preserve_publication))
    }

    pub(super) fn replace_job_then(
        &mut self,
        candidate: Job,
        fold: bool,
        emit_finished: bool,
        require_existing: bool,
        require_snapshot: bool,
        callback: impl FnOnce(&mut Owner, Result<(), nzbd_state::StateError>) + Send + 'static,
    ) {
        self.replace_job_proven_then(
            candidate,
            fold,
            emit_finished,
            require_existing,
            require_snapshot,
            None,
            callback,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn replace_job_proven_then(
        &mut self,
        mut candidate: Job,
        fold: bool,
        emit_finished: bool,
        require_existing: bool,
        require_snapshot: bool,
        completion_proof: Option<nzbd_state::artifacts::WorkerCompletionProof>,
        callback: impl FnOnce(&mut Owner, Result<(), nzbd_state::StateError>) + Send + 'static,
    ) {
        let job = candidate.id;
        // An imported row's ranges were not written by this boot's
        // snapshot: they are verified, never stamped (no-wedge item 5).
        self.boot_jobs.remove(&job);
        // An imported row may come from a 0.5.0 node (a hold as `paused`).
        crate::normalize_stored_hold_status(&mut candidate, "imported job");
        if require_existing && self.state.job(job).is_none() {
            callback(self, Err(failure("replacement target no longer exists")));
            return;
        }
        if require_snapshot && !self.persist {
            callback(
                self,
                Err(failure("replacement requires durable queue ownership")),
            );
            return;
        }
        if self.post_admissions.contains_key(&job)
            || self.allocating_jobs.contains(&job)
            || self.record_transitions.contains(&job)
            || self.projection_transition
            || self.retiring_writers.contains_key(&job)
            || self
                .verification
                .get(&job)
                .is_some_and(|state| state.running)
        {
            callback(self, Err(failure("replacement scope has not quiesced")));
            return;
        }
        let relative = PathBuf::from(job_dir_name(&candidate));
        if relative
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
        {
            callback(self, Err(failure("unsafe replacement storage namespace")));
            return;
        }
        let (already_accepted, preserve_publication) =
            match self.prepare_replacement_control(&mut candidate, completion_proof.as_ref()) {
                Ok(result) => result,
                Err(error) => {
                    callback(self, Err(error));
                    return;
                }
            };
        if already_accepted {
            callback(self, Ok(()));
            return;
        }
        let previous = self.state.job(job).cloned();
        let prior_receipts = self
            .state
            .url_admission_receipts
            .iter()
            .filter(|receipt| receipt.job_id == job.0)
            .cloned()
            .collect::<Vec<_>>();
        normalize(&mut candidate);
        let epoch = fresh_epoch();
        self.job_epochs.insert(job, epoch);
        if let Some(state) = self.verification.remove(&job) {
            state.cancel.cancel();
        }
        for file in previous.as_ref().into_iter().flat_map(|job| &job.files) {
            self.writer_epochs.remove(&file.id);
            self.pending_writer_birth.remove(&file.id);
            self.writers.remove(&file.id);
        }
        let old_allocation = self.allocation_cache.remove(&job);
        self.artifacts.invalidate_writer_naming(job.0);
        if preserve_publication {
            if let Some(allocation) = old_allocation.as_ref() {
                self.allocation_cache.insert(job, allocation.clone());
            }
        }
        self.allocated_jobs.remove(&job);
        let old_delegated = self.delegated.remove(&job);
        let old_mirror = self.mirror.remove(&job);
        let old_post = self.post_fetch_files.remove(&job);
        self.state.next_job_id = self.state.next_job_id.max(job.0);
        self.state.next_file_id = self.state.next_file_id.max(
            candidate
                .files
                .iter()
                .map(|file| file.id.0)
                .max()
                .unwrap_or(0),
        );
        if let Some(index) = self.state.jobs.iter().position(|current| current.id == job) {
            self.state.jobs[index] = candidate.clone();
        } else {
            self.state.jobs.push(candidate.clone());
        }
        self.pending_control_commits.insert(job);
        self.record_transitions.insert(job);
        if !preserve_publication && needs_payload(&candidate) {
            self.queue_verification(job);
            if let Some(state) = self.verification.get_mut(&job) {
                state.phase = "saving".into();
            }
        }
        self.dirty = true;
        self.persist_then(move |owner, committed| {
            if owner.job_epochs.get(&job) != Some(&epoch) {
                owner.record_transitions.remove(&job);
                owner.drain_replacements();
                callback(owner, Err(failure("replacement generation was superseded")));
                return;
            }
            if !committed {
                let current = owner.state.job(job).cloned();
                match previous {
                    Some(mut previous) => {
                        if let Some(current) = &current {
                            preserve_intent_changes(&mut previous, current, &candidate);
                        }
                        if let Some(index) = owner
                            .state
                            .jobs
                            .iter()
                            .position(|current| current.id == job)
                        {
                            owner.state.jobs[index] = previous;
                        }
                    }
                    None => owner.state.jobs.retain(|current| current.id != job),
                }
                owner
                    .state
                    .url_admission_receipts
                    .retain(|receipt| receipt.job_id != job.0);
                owner.state.url_admission_receipts.extend(prior_receipts);
                owner.verification.remove(&job);
                if let Some(allocation) = old_allocation {
                    owner.allocation_cache.insert(job, allocation);
                }
                if let Some(node) = old_delegated {
                    owner.delegated.insert(job, node);
                }
                if let Some(mirror) = old_mirror {
                    owner.mirror.insert(job, mirror);
                }
                if let Some(post) = old_post {
                    owner.post_fetch_files.insert(job, post);
                }
                owner.dirty = true;
                owner.persist_then(move |owner, restored| {
                    if restored {
                        owner.pending_control_commits.remove(&job);
                        owner.queue_payload_recovery(job);
                    }
                    owner.record_transitions.remove(&job);
                    owner.drain_replacements();
                    owner.publish_now();
                    callback(
                        owner,
                        Err(failure(
                            "replacement snapshot was not durable; prior record retained",
                        )),
                    );
                });
                return;
            }
            owner.pending_control_commits.remove(&job);
            owner.record_transitions.remove(&job);
            for file in previous.as_ref().into_iter().flat_map(|row| &row.files) {
                owner.file_sizes.remove(&file.id);
                owner.finalize_sent.remove(&file.id);
            }
            owner.pending_finalize.retain(|(id, _)| *id != job);
            owner.attempts.retain(|reference, _| reference.job != job);
            if !preserve_publication && needs_payload(&candidate) {
                owner.queue_payload_recovery(job);
            }
            if fold {
                owner.fold_job_journals(job);
            }
            if let Some(files) = owner.state.job(job).and_then(recovered_post_fetch_files) {
                owner.post_fetch_files.insert(job, files);
            }
            if emit_finished
                && !needs_payload(&candidate)
                && matches!(
                    candidate.status,
                    JobStatus::Completed | JobStatus::Failed | JobStatus::Deleted
                )
            {
                owner.emit(Event::JobFinished {
                    job,
                    name: candidate.name,
                    status: candidate.status,
                    health: Health::calc(&candidate.totals).0,
                });
            }
            owner.publish_now();
            owner.bump_epoch();
            callback(owner, Ok(()));
            owner.drain_replacements();
        });
    }

    pub(super) fn replace_projection_then(
        &mut self,
        mut jobs: Vec<Job>,
        adopted_speed: Option<(u64, Option<u64>)>,
        callback: impl FnOnce(&mut Owner, Result<(), nzbd_state::StateError>) + Send + 'static,
    ) {
        // Replicated rows are verified, never stamped (no-wedge item 5).
        self.boot_jobs.clear();
        // Replicated rows may come from a 0.5.0 node (a hold as `paused`).
        for job in &mut jobs {
            crate::normalize_stored_hold_status(job, "replicated authority");
        }
        if !self.post_admissions.is_empty()
            || !self.allocating_jobs.is_empty()
            || self
                .record_transitions
                .iter()
                .any(|job| !self.settled_post_rollback(*job))
            || self.projection_transition
            || !self.retiring_writers.is_empty()
            || self.verification.values().any(|state| state.running)
            || self
                .writers
                .values()
                .any(|writer| !*writer.stopped.borrow())
        {
            callback(
                self,
                Err(failure(
                    "authority projection still has live payload custody",
                )),
            );
            return;
        }
        let mut ids = HashSet::new();
        if jobs.iter().any(|job| !ids.insert(job.id)) {
            callback(
                self,
                Err(failure("replicated projection contains duplicate jobs")),
            );
            return;
        }
        let previous_speed = self.state.speed_limit_bps;
        self.artifacts.invalidate_all_writer_naming();
        let speed_revision = adopted_speed
            .filter(|(revision, _)| *revision == self.speed_limit_revision)
            .map(|(revision, speed)| {
                self.state.speed_limit_bps = speed;
                revision
            });
        self.projection_transition = true;
        let previous = self.state.jobs.clone();
        let old_delegated = std::mem::take(&mut self.delegated);
        let old_mirror = std::mem::take(&mut self.mirror);
        let old_post = std::mem::take(&mut self.post_fetch_files);
        let old_cache = std::mem::take(&mut self.allocation_cache);
        for job in &mut jobs {
            normalize(job);
        }
        for job in &previous {
            self.job_epochs.insert(job.id, fresh_epoch());
            if let Some(state) = self.verification.remove(&job.id) {
                state.cancel.cancel();
            }
        }
        self.writer_epochs.clear();
        self.pending_writer_birth.clear();
        self.allocated_jobs.clear();
        self.delegated.clear();
        self.mirror.clear();
        self.post_fetch_files.clear();
        for job in &jobs {
            self.job_epochs.insert(job.id, fresh_epoch());
            self.pending_control_commits.insert(job.id);
        }
        self.state.next_job_id = self
            .state
            .next_job_id
            .max(jobs.iter().map(|job| job.id.0).max().unwrap_or(0));
        self.state.next_file_id = self.state.next_file_id.max(
            jobs.iter()
                .flat_map(|job| &job.files)
                .map(|file| file.id.0)
                .max()
                .unwrap_or(0),
        );
        let provisional = jobs.clone();
        self.state.jobs = jobs;
        let fence = self
            .state
            .jobs
            .iter()
            .map(|job| (job.id, self.job_epochs[&job.id]))
            .collect::<Vec<_>>();
        let checking = self
            .state
            .jobs
            .iter()
            .filter(|job| needs_payload(job))
            .map(|job| job.id)
            .collect::<Vec<_>>();
        for job in &checking {
            self.queue_verification(*job);
            if let Some(state) = self.verification.get_mut(job) {
                state.phase = "saving".into();
            }
        }
        self.dirty = true;
        self.persist_then(move |owner, committed| {
            let current = fence
                .iter()
                .all(|(job, epoch)| owner.job_epochs.get(job) == Some(epoch));
            if !current {
                if speed_revision == Some(owner.speed_limit_revision) {
                    owner.state.speed_limit_bps = previous_speed;
                }
                owner.persist_then(move |owner, _| {
                    owner.projection_transition = false;
                    owner.drain_replacements();
                    callback(owner, Err(failure("authority projection was superseded")));
                });
                return;
            }
            if !committed {
                // Restore rate intent before serializing the rollback snapshot.
                // A later operator edit owns its own revision and survives.
                if speed_revision == Some(owner.speed_limit_revision) {
                    owner.state.speed_limit_bps = previous_speed;
                }
                let mut restored = previous;
                for old in &mut restored {
                    if let Some((current, provisional)) = owner
                        .state
                        .job(old.id)
                        .zip(provisional.iter().find(|candidate| candidate.id == old.id))
                    {
                        preserve_intent_changes(old, current, provisional);
                    }
                }
                owner.state.jobs = restored;
                owner.allocation_cache = old_cache;
                owner.delegated = old_delegated;
                owner.mirror = old_mirror;
                owner.post_fetch_files = old_post;
                owner.verification.clear();
                owner.dirty = true;
                let old_jobs = owner
                    .state
                    .jobs
                    .iter()
                    .map(|job| job.id)
                    .collect::<Vec<_>>();
                for job in &old_jobs {
                    owner.pending_control_commits.insert(*job);
                    if owner.state.job(*job).is_some_and(needs_payload) {
                        owner.queue_verification(*job);
                        if let Some(state) = owner.verification.get_mut(job) {
                            state.phase = "saving".into();
                        }
                    }
                }
                owner.persist_then(move |owner, restored| {
                    if restored {
                        owner.clear_post_rollback_barriers();
                        for (job, _) in &fence {
                            owner.pending_control_commits.remove(job);
                        }
                        for job in old_jobs {
                            owner.pending_control_commits.remove(&job);
                            owner.queue_payload_recovery(job);
                        }
                    }
                    owner.projection_transition = false;
                    owner.drain_replacements();
                    owner.publish_now();
                    if restored {
                        owner.bump_epoch();
                    }
                    callback(
                        owner,
                        Err(failure(
                            "authority projection was not durably adopted; prior rows retained",
                        )),
                    );
                });
                return;
            }
            if speed_revision.is_some() {
                owner.applied_usenet_limit = None;
                owner.applied_torrent_limit = None;
                let rate = owner.shared.load().download_rate_bps;
                owner.rebalance_download_budget(rate);
            }
            owner.projection_transition = false;
            owner.clear_post_rollback_barriers();
            for (job, _) in &fence {
                owner.pending_control_commits.remove(job);
            }
            for job in checking {
                owner.queue_payload_recovery(job);
                owner.fold_job_journals(job);
            }
            owner.publish_now();
            owner.bump_epoch();
            callback(owner, Ok(()));
            owner.drain_replacements();
        });
    }

    /// Enumerate lifecycle roots and open lifecycle intents on the state
    /// lane (metadata only), classify every recovered job, then reconcile
    /// each root in the background.
    ///
    /// No-wedge (Paul, 2026-11-11): the only file that may need anything at
    /// a restart is the one being written; resume or re-fetch it and keep
    /// moving. Until classification lands every job holds a "loading"
    /// ticket (one SQLite pass). Then a job keeps its ticket only for
    /// unproven ranges, an identity conflict or an open lifecycle intent;
    /// every other job — proven, paused, legacy — is released untouched and
    /// checked lazily at writer spawn or post-processing admission.
    pub(super) fn schedule_startup_reconciliation(&mut self) {
        if self.startup_roots_loading
            || self.startup_roots_ready
            || Instant::now() < self.startup_roots_retry_at
            || self.cancel.is_cancelled()
        {
            return;
        }
        self.startup_roots_loading = true;
        let jobs = self
            .state
            .jobs
            .iter()
            .filter(|job| needs_payload(job))
            .map(|job| job.id)
            .collect::<Vec<_>>();
        // One snapshot publication for the whole queue, not one per job.
        self.publish_batched = true;
        for job in &jobs {
            self.queue_verification(*job);
            if let Some(state) = self.verification.get_mut(job) {
                state.phase = "loading".into();
            }
        }
        self.publish_batched = false;
        self.publish_now();
        let snapshot = self.startup_snapshot.clone();
        let inventory = self.artifacts.clone();
        let candidates = jobs.iter().map(|job| job.0).collect::<Vec<_>>();
        self.io_then(move || {
            let roots = snapshot.roots().to_vec();
            let mut intents = inventory
                .open_relocation_intents()
                .map_err(failure)?
                .into_iter()
                .map(|intent| intent.job)
                .collect::<HashSet<u32>>();
            for job in candidates {
                if inventory.has_transform_custody(job).map_err(failure)? {
                    intents.insert(job);
                }
            }
            Ok::<_, nzbd_state::StateError>((roots, intents))
        }, move |owner, result| {
            owner.startup_roots_loading = false;
            match result {
                Ok((mut roots, intents)) => {
                    roots.push(owner.dest_dir.clone());
                    roots.push(owner.artifacts.workspace_dir());
                    roots.extend(owner.configured_payload_roots.iter().cloned());
                    roots.extend(owner.allocation_cache.values().map(|allocation| allocation.root.clone()));
                    roots.sort();
                    roots.dedup();
                    owner.startup_intent_jobs = intents.into_iter().map(JobId).collect();
                    // A retiring, retained or still-allocating allocation is
                    // an open lifecycle intent of its own.
                    owner.startup_intent_jobs.extend(
                        owner.allocation_cache.iter()
                            .filter(|(_, allocation)| allocation.state != "active")
                            .map(|(job, _)| *job),
                    );
                    owner.startup_roots_ready = true;
                    owner.classify_startup_jobs();
                    for root in roots { owner.reconcile_payload_root(root); }
                }
                Err(error) => {
                    owner.startup_roots_retry_at = Instant::now() + Duration::from_secs(30);
                    let jobs = owner.state.jobs.iter().filter(|job| needs_payload(job))
                        .map(|job| job.id).collect::<Vec<_>>();
                    for job in jobs { owner.payload_recovery_failed(job, failure(&error)); }
                    tracing::warn!(%error, "startup lifecycle root inventory remains unavailable");
                }
            }
        });
    }

    /// Release every startup ticket the job does not need; hand the rest
    /// to recovery now (a live active allocation does not wait for its
    /// root's reconciliation).
    fn classify_startup_jobs(&mut self) {
        let mut released = 0u32;
        let mut ticketed = Vec::new();
        self.publish_batched = true;
        let jobs = self
            .state
            .jobs
            .iter()
            .filter(|job| {
                self.verification
                    .get(&job.id)
                    .is_some_and(|state| !state.running && state.phase == "loading")
            })
            .map(|job| (job.id, self.startup_ticket_needed(job)))
            .collect::<Vec<_>>();
        for (job, needed) in jobs {
            if needed {
                ticketed.push(job);
            } else {
                self.release_startup_ticket(job);
                released += 1;
            }
        }
        self.publish_batched = false;
        self.startup_phases.classified_ms = Some(crate::startup_phases::since_boot_ms());
        self.startup_phases.startup_tickets = ticketed.len() as u32;
        self.startup_phases.startup_untouched = released;
        tracing::info!(
            released,
            ticketed = ticketed.len(),
            intents = self.startup_intent_jobs.len(),
            "startup classification: proven and paused jobs released without verification"
        );
        for job in ticketed {
            if !self.pending_control_commits.contains(&job) {
                self.queue_payload_recovery(job);
            }
        }
        self.publish_now();
        self.bump_epoch();
    }

    fn reconcile_payload_root(&mut self, root: PathBuf) {
        if self
            .startup_reconciliation
            .get(&root)
            .is_some_and(|state| state.ready || state.running)
        {
            return;
        }
        self.startup_reconciliation.insert(
            root.clone(),
            StartupRoot {
                ready: false,
                running: true,
                retry_at: Instant::now(),
                reason: None,
            },
        );
        let inventory = self.artifacts.clone();
        let live = self
            .state
            .jobs
            .iter()
            .map(|job| job.id.0)
            .collect::<Vec<_>>();
        let worker_root = root.clone();
        let default_root = self.dest_dir.clone();
        let snapshot = self.startup_snapshot.clone();
        // W9: a `.runner-file-<id>.part` that is a second name of its
        // finalized file's publication is removed at startup.
        let parts = self
            .state
            .jobs
            .iter()
            .filter(|job| needs_payload(job))
            .filter_map(|job| {
                let allocation = self.allocation_cache.get(&job.id)?;
                (allocation.root == root).then(|| {
                    (
                        allocation.path.clone(),
                        job.files
                            .iter()
                            .filter(|file| file.finalized)
                            .map(|file| {
                                let key = format!("*File:proof:{}", file.id.0);
                                let published = job
                                    .params
                                    .iter()
                                    .find(|(name, _)| name == &key)
                                    .and_then(|(_, raw)| {
                                        serde_json::from_str::<crate::recovery::DurableFileProof>(
                                            raw,
                                        )
                                        .ok()
                                    })
                                    .and_then(|proof| proof.publication_name)
                                    .unwrap_or_else(|| file.filename.clone());
                                (file.id.0, published)
                            })
                            .collect::<HashMap<u32, String>>(),
                    )
                })
            })
            .collect::<Vec<_>>();
        // No-wedge item 6: reconciliation runs off the payload lane, so a slow
        // (FUSE/NFS) root never queues the lease-time allocation revalidation
        // of its live active jobs behind it. It touches only allocations that
        // are not live and active; jobs with open intents wait for it.
        let started_ms = crate::startup_phases::since_boot_ms();
        let started = Instant::now();
        let label = root.display().to_string();
        self.startup_phases
            .roots
            .retain(|entry| entry.root != label);
        self.startup_phases
            .roots
            .push(crate::startup_phases::RootReconciliation {
                root: label.clone(),
                started_ms,
                duration_ms: None,
                ok: None,
            });
        let configured = worker_root == self.artifacts.workspace_dir()
            || self.configured_payload_roots.contains(&worker_root);
        self.background_then(format!("reconcile-{}", root.display()), move || {
            if worker_root == default_root { prepare_unenrolled_payload_root(&inventory, &worker_root)?; }
            // A configured root nothing has used yet (no row names it, the
            // directory does not exist) has nothing to reconcile. An
            // enrolled root that is missing stays a failure: its mount may
            // be unavailable.
            if configured && worker_root != default_root && matches!(
                std::fs::symlink_metadata(&worker_root),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound
            ) && !inventory.startup_reconciliation_roots().map_err(failure)?.contains(&worker_root) {
                return Ok(());
            }
            inventory.reconcile_startup_root_snapshot(&worker_root, &live, &snapshot).map_err(failure)?;
            for (dir, finalized) in &parts {
                sweep_stale_parts(dir, finalized);
            }
            // W5a: directory probe files a killed run left behind.
            crate::space_probe::sweep_probe_files(&worker_root);
            Ok(())
        }, move |owner, result| {
            let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            if let Some(entry) = owner.startup_phases.roots.iter_mut().find(|entry| entry.root == label) {
                entry.duration_ms = Some(elapsed);
                entry.ok = Some(result.is_ok());
            }
            tracing::info!(root=%root.display(), ms = elapsed, ok = result.is_ok(), "startup root reconciliation finished");
            let state = owner.startup_reconciliation.get_mut(&root).unwrap();
            state.running = false;
            state.ready = result.is_ok();
            state.reason = result.as_ref().err().map(ToString::to_string);
            state.retry_at = Instant::now() + Duration::from_secs(30);
            if let Err(error) = &result {
                tracing::warn!(root=%root.display(), %error, "payload lifecycle reconciliation remains blocked");
            }
            let jobs = owner.state.jobs.iter().filter(|job| needs_payload(job)
                && !owner.pending_control_commits.contains(&job.id)
                && owner.verification.get(&job.id).is_some_and(|state|
                    !state.running && matches!(state.phase.as_str(), "loading" | "blocked")))
                .map(|job| job.id).collect::<Vec<_>>();
            for job in jobs { owner.queue_payload_recovery(job); }
            owner.publish_now();
            owner.bump_epoch();
        });
    }

    /// One classification path for boot, import and both authority adoptions.
    pub(super) fn queue_payload_recovery(&mut self, job: JobId) {
        if job.0 <= 3 {
            tracing::warn!(target:"nzbd_engine::verification_diag",job=job.0,needs=self.state.job(job).is_some_and(needs_payload), pending=self.pending_control_commits.contains(&job), allocating=self.allocating_jobs.contains(&job),ticket=?self.verification.get(&job).map(|s|(s.generation,s.phase.as_str(),s.running)),"queue payload recovery diagnostic");
        }
        if !self.state.job(job).is_some_and(needs_payload) {
            return;
        }
        if self.post_admissions.contains_key(&job)
            || self.pending_control_commits.contains(&job)
            || self
                .verification
                .get(&job)
                .is_some_and(|state| state.running)
        {
            return;
        }
        if !self.startup_roots_ready {
            self.queue_verification(job);
            if let Some(state) = self.verification.get_mut(&job) {
                state.phase = "loading".into();
            }
            return;
        }
        if self.allocating_jobs.contains(&job) {
            return;
        }
        // Recovery invalidates the cached capability even when admission must
        // wait. Otherwise startup's advisory cache makes the retry selector
        // mistake a deferred row for an already reconciled allocation.
        self.allocation_cache.remove(&job);
        self.allocated_jobs.remove(&job);
        if !self.payload_recovery_capacity_available() {
            // Keep the row as the deferred work obligation. Avoid recursively
            // asking queue_verification to allocate while creating its ticket.
            if !self.verification.contains_key(&job) {
                self.allocating_jobs.insert(job);
                self.queue_verification(job);
                self.allocating_jobs.remove(&job);
            }
            if let Some(state) = self.verification.get_mut(&job) {
                state.phase = "loading".into();
                state.reason = None;
            }
            return;
        }
        // Deferred rows retain their ticket without repeatedly copying a full
        // NZB descriptor on every lane completion.
        let Some(record) = self.state.job(job).cloned() else {
            return;
        };
        let plan = match imported_payload_plan(&record, &self.dest_dir) {
            Ok(plan) => plan,
            Err(error) => {
                self.queue_verification(job);
                self.payload_recovery_failed(job, error);
                return;
            }
        };
        let inventory = self.artifacts.clone();
        let fallback_root = self.dest_dir.clone();
        let epoch = *self.job_epochs.entry(job).or_insert_with(fresh_epoch);
        self.allocating_jobs.insert(job);
        self.queue_verification(job);
        if let Some(state) = self.verification.get_mut(&job) {
            state.phase = "loading".into();
        }
        // This first lane touches only local Inventory metadata. The payload
        // worker can then reserve the actual allocation root, including imports
        // whose root differs from the configured default.
        self.io_then(
            move || inventory.for_job(job.0).map_err(failure),
            move |owner, metadata| {
                if owner.job_epochs.get(&job) != Some(&epoch) {
                    owner.allocating_jobs.remove(&job);
                    return;
                }
                let allocation = match metadata {
                    Ok(allocation) => allocation,
                    Err(error) => {
                        owner.payload_recovery_failed(job, error);
                        return;
                    }
                };
                let root = if plan.private_range {
                    plan.root.clone()
                } else {
                    allocation
                        .as_ref()
                        .map(|allocation| allocation.root.clone())
                        .unwrap_or_else(|| fallback_root.clone())
                };
                // Only an open lifecycle intent (or an allocation that is not
                // live and active) waits for its root's reconciliation.
                let gated = owner.startup_intent_jobs.contains(&job)
                    || allocation
                        .as_ref()
                        .is_none_or(|allocation| allocation.state != "active");
                if gated
                    && !owner
                        .startup_reconciliation
                        .get(&root)
                        .is_some_and(|state| state.ready)
                {
                    owner.allocating_jobs.remove(&job);
                    if !owner.startup_reconciliation.contains_key(&root) {
                        owner.reconcile_payload_root(root.clone());
                    }
                    if let Some(state) = owner.startup_reconciliation.get(&root) {
                        let reason = state.reason.clone();
                        let running = state.running;
                        if let Some(verification) = owner.verification.get_mut(&job) {
                            verification.phase = if running { "loading" } else { "blocked" }.into();
                            verification.reason = reason;
                            verification.retry_at = state.retry_at;
                        }
                    }
                    owner.publish_now();
                    owner.drain_replacements();
                    return;
                }
                let inventory = owner.artifacts.clone();
                owner.payload_then(
                    root.clone(),
                    move || {
                        let allocation = allocate_imported_payload(
                            &inventory,
                            &record,
                            &fallback_root,
                            &plan,
                            allocation,
                        )?;
                        let transformed = inventory
                            .has_transform_custody(record.id.0)
                            .map_err(failure)?;
                        if !transformed && pristine_download(&record) {
                            // Keep the verifier's fresh allocation and anchored
                            // custody checks even when there are no ranges to hash.
                            let current = inventory
                                .for_job(record.id.0)
                                .map_err(failure)?
                                .ok_or_else(|| {
                                    failure("allocation disappeared during preparation")
                                })?;
                            if current.id != allocation.id
                                || current.generation != allocation.generation
                                || current.path != allocation.path
                                || current.root != allocation.root
                            {
                                return Err(failure(
                                    "allocation authority changed during preparation",
                                ));
                            }
                            let custody = inventory
                                .acquire_terminal_use(record.id.0, &|| Ok(()))
                                .map_err(failure)?
                                .ok_or_else(|| failure("allocation custody is unavailable"))?;
                            if custody.source().artifact != allocation.id
                                || custody.source().generation != allocation.generation
                            {
                                return Err(failure("allocation custody generation differs"));
                            }
                        }
                        Ok((allocation, transformed))
                    },
                    move |owner, result| {
                        owner.allocating_jobs.remove(&job);
                        if owner.job_epochs.get(&job) != Some(&epoch) {
                            return;
                        }
                        match result {
                            Ok((allocation, transformed)) => {
                                if allocation.state == "active" {
                                    owner.allocated_jobs.insert(job);
                                }
                                owner.allocation_cache.insert(job, allocation);
                                let journal_pending = owner
                                    .pending_journal_loads
                                    .get(&job)
                                    .is_some_and(|loading| loading.0 == epoch);
                                if !transformed
                                    && !journal_pending
                                    && owner.state.job(job).is_some_and(pristine_download)
                                {
                                    // Journal replay and allocation/custody reconciliation
                                    // established that there are no payload claims to read.
                                    // No proof or segment transition needs a per-job save.
                                    owner.verification.remove(&job);
                                    if let Some(record) = owner.state.job_mut(job) {
                                        set_owner_param(
                                            &mut record.params,
                                            "*Recovery:pending",
                                            None,
                                        );
                                        set_owner_param(
                                            &mut record.params,
                                            "*Recovery:reason",
                                            None,
                                        );
                                    }
                                    owner.dirty = true;
                                } else {
                                    owner.queue_verification(job);
                                }
                            }
                            Err(error) => owner.payload_recovery_failed(job, error),
                        }
                        owner.publish_now();
                        owner.bump_epoch();
                        owner.drain_replacements();
                    },
                );
            },
        );
    }

    fn payload_recovery_failed(&mut self, job: JobId, error: nzbd_state::StateError) {
        self.allocating_jobs.remove(&job);
        if matches!(error, nzbd_state::StateError::Busy(_)) {
            if let Some(state) = self.verification.get_mut(&job) {
                state.phase = "loading".into();
                state.reason = None;
                state.retry_at = Instant::now();
            }
            return;
        }
        if let Some(state) = self.verification.get_mut(&job) {
            state.phase = "blocked".into();
            state.reason = Some(error.to_string());
            state.retry_at = Instant::now() + Duration::from_secs(30);
            state.review_required = false;
        }
        if let Some(record) = self.state.job_mut(job) {
            set_owner_param(
                &mut record.params,
                "*Recovery:reason",
                Some(error.to_string()),
            );
        }
        self.publish_now();
        self.bump_epoch();
        self.drain_replacements();
    }

    pub(super) fn retry_payload_recoveries(&mut self) {
        let now = Instant::now();
        if !self.startup_roots_ready {
            self.schedule_startup_reconciliation();
            return;
        }
        let roots = self
            .startup_reconciliation
            .iter()
            .filter(|(_, state)| !state.ready && !state.running && state.retry_at <= now)
            .map(|(root, _)| root.clone())
            .collect::<Vec<_>>();
        for root in roots {
            self.reconcile_payload_root(root);
        }
        let jobs = self
            .state
            .jobs
            .iter()
            .filter(|job| {
                needs_payload(job)
                    && !self.allocation_cache.contains_key(&job.id)
                    && !self.allocating_jobs.contains(&job.id)
                    && !self.pending_control_commits.contains(&job.id)
                    && self.verification.get(&job.id).is_some_and(|state| {
                        !state.running
                            && matches!(state.phase.as_str(), "blocked" | "loading")
                            && state.retry_at <= now
                    })
            })
            .map(|job| job.id)
            .collect::<Vec<_>>();
        for job in jobs {
            self.queue_payload_recovery(job);
        }
    }
}

#[cfg(test)]
mod stale_part_tests {
    use super::*;
    /// Finding 1: the sweep removes only a partial that is a second name of
    /// its finalized file's publication. A finalized file whose publication
    /// is missing or a different object keeps its partial (it may be the only
    /// copy), and so do unfinalized and unknown files.
    #[test]
    fn startup_sweep_removes_only_a_second_name_of_the_publication() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        // 1: finalized, published, killed before unlink → same object.
        std::fs::write(d.join("one.mkv"), b"x").unwrap();
        std::fs::hard_link(d.join("one.mkv"), d.join(".runner-file-1.part")).unwrap();
        // 2: "finalized" but the rename never happened → only copy.
        std::fs::write(d.join(".runner-file-2.part"), b"only").unwrap();
        // 3: finalized, publication exists but is a different object.
        std::fs::write(d.join("three.mkv"), b"y").unwrap();
        std::fs::write(d.join(".runner-file-3.part"), b"z").unwrap();
        // 4: not finalized (not in the map) → resumable.
        std::fs::write(d.join("four.mkv"), b"w").unwrap();
        std::fs::hard_link(d.join("four.mkv"), d.join(".runner-file-4.part")).unwrap();
        std::fs::write(d.join(".runner-file-x.part"), b"x").unwrap();
        let finalized: HashMap<u32, String> = [
            (1, "one.mkv".to_string()),
            (2, "two.mkv".to_string()),
            (3, "three.mkv".to_string()),
        ]
        .into_iter()
        .collect();
        assert_eq!(sweep_stale_parts(d, &finalized), 1);
        assert!(!d.join(".runner-file-1.part").exists());
        assert!(d.join("one.mkv").exists());
        assert_eq!(
            std::fs::read(d.join(".runner-file-2.part")).unwrap(),
            b"only"
        );
        assert!(d.join(".runner-file-3.part").exists());
        assert!(d.join(".runner-file-4.part").exists());
        assert!(d.join(".runner-file-x.part").exists());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn record(name: &str, status: JobStatus) -> Job {
        Job {
            id: JobId(1),
            kind: JobKind::Nzb,
            name: name.into(),
            dir_name: "fixture".into(),
            name_provisional: false,
            delivery_lifecycle: false,
            queued_at_unix: 0,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: Default::default(),
            params: vec![],
            files: vec![],
            totals: Default::default(),
            status,
            torrent: None,
            stages: vec![],
        }
    }
    fn config(temp: &tempfile::TempDir) -> crate::EngineConfig {
        crate::EngineConfig::single_node(
            vec![],
            temp.path().join("state"),
            temp.path().join("payload"),
            Tuning::default(),
            None,
        )
    }

    #[test]
    fn rollback_keeps_concurrent_intent_without_leaking_provisional_fields() {
        let mut previous = record("previous", JobStatus::Queued);
        previous.priority = 1;
        previous.category = Some("original".into());
        let mut provisional = previous.clone();
        provisional.priority = 99;
        provisional.category = Some("incoming".into());
        let mut unchanged = previous.clone();
        preserve_intent_changes(&mut unchanged, &provisional, &provisional);
        assert_eq!(unchanged.priority, 1);
        assert_eq!(unchanged.category.as_deref(), Some("original"));
        let mut current = provisional.clone();
        current.priority = 7;
        current.status = JobStatus::Paused;
        preserve_intent_changes(&mut previous, &current, &provisional);
        assert_eq!(previous.priority, 7);
        assert_eq!(previous.category.as_deref(), Some("original"));
        assert_eq!(previous.status, JobStatus::Paused);
    }

    #[tokio::test]
    async fn admitted_post_stamp_keeps_its_lifetime_and_does_not_wait_for_itself() {
        let temp = tempfile::tempdir().unwrap();
        let engine = crate::Engine::spawn(config(&temp)).await.unwrap();
        engine
            .import_job(record("original", JobStatus::Completed), false, false)
            .await
            .unwrap();
        let admission = engine
            .begin_post_admission(JobId(1))
            .await
            .unwrap()
            .unwrap();
        let scoped = engine.for_post_admission(&admission);
        let mut stamp = scoped.export_job(JobId(1)).await.unwrap().unwrap();
        stamp
            .params
            .push((nzbd_types::PP_DONE_PARAM.into(), "SUCCESS/ALL".into()));
        assert!(
            tokio::time::timeout(Duration::from_secs(2), scoped.import_job_if_present(stamp))
                .await
                .unwrap()
                .unwrap()
        );
        assert!(!admission.cancellation.is_cancelled());
        assert!(engine.recovery_document().await.unwrap().jobs[0]
            .params
            .iter()
            .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM));
        engine.end_post_admission(admission).await.unwrap();
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn external_replacement_revokes_post_work_and_stale_stamp_is_never_deferred() {
        let temp = tempfile::tempdir().unwrap();
        let engine = crate::Engine::spawn(config(&temp)).await.unwrap();
        engine
            .import_job(record("original", JobStatus::Completed), false, false)
            .await
            .unwrap();
        let admission = engine
            .begin_post_admission(JobId(1))
            .await
            .unwrap()
            .unwrap();
        let scoped = engine.for_post_admission(&admission);
        let mut old = record("stale stamp", JobStatus::Completed);
        old.params
            .push((nzbd_types::PP_DONE_PARAM.into(), "SUCCESS/ALL".into()));
        let replacement_engine = engine.clone();
        let replacement = tokio::spawn(async move {
            replacement_engine
                .import_job(record("replacement", JobStatus::Queued), false, false)
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), admission.cancellation.cancelled())
            .await
            .unwrap();
        assert!(!replacement.is_finished());
        assert!(!matches!(
            tokio::time::timeout(Duration::from_secs(2), scoped.import_job_if_present(old))
                .await
                .unwrap(),
            Ok(true)
        ));
        // This represents the manager's actual tool/reader drain, not merely a
        // cancellation request. Only then may replacement enter the namespace.
        engine.end_post_admission(admission).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), replacement)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let doc = engine.recovery_document().await.unwrap();
        assert_eq!(doc.jobs.len(), 1);
        assert_eq!(doc.jobs[0].name, "replacement");
        assert!(!doc.jobs[0]
            .params
            .iter()
            .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM));
        engine.shutdown().await;
    }

    struct ReleaseSave(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
    impl Drop for ReleaseSave {
        fn drop(&mut self) {
            let mut ready = self.0 .0.lock().unwrap();
            *ready = true;
            self.0 .1.notify_all();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replacement_ack_waits_for_one_document_and_never_publishes_a_missing_row() {
        let temp = tempfile::tempdir().unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let armed = Arc::new(AtomicBool::new(false));
        let release = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let _release_on_panic = ReleaseSave(release.clone());
        let mut cfg = config(&temp);
        cfg.persist_guard = Some({
            let entered = entered.clone();
            let armed = armed.clone();
            let release = release.clone();
            Arc::new(move || {
                if armed.load(Ordering::Acquire) {
                    entered.notify_one();
                    let mut ready = release.0.lock().unwrap();
                    while !*ready {
                        ready = release.1.wait(ready).unwrap();
                    }
                }
                true
            })
        });
        let engine = crate::Engine::spawn(cfg).await.unwrap();
        engine
            .import_job(record("original", JobStatus::Queued), false, false)
            .await
            .unwrap();
        armed.store(true, Ordering::Release);
        let worker = engine.clone();
        let pending = tokio::spawn(async move {
            worker
                .import_job(record("replacement", JobStatus::Queued), false, false)
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        assert!(!pending.is_finished());
        let disk = SnapshotStore::open(&temp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert_eq!(disk.jobs.len(), 1);
        assert_eq!(disk.jobs[0].name, "original");
        {
            let mut ready = release.0.lock().unwrap();
            *ready = true;
            release.1.notify_all();
        }
        tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let disk = SnapshotStore::open(&temp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert_eq!(disk.jobs.len(), 1);
        assert_eq!(disk.jobs[0].name, "replacement");
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn failed_replacement_and_replicated_adoption_report_failure_and_retain_prior_row() {
        let temp = tempfile::tempdir().unwrap();
        let allowed = Arc::new(AtomicBool::new(true));
        let mut cfg = config(&temp);
        cfg.persist_guard = Some({
            let allowed = allowed.clone();
            Arc::new(move || allowed.load(Ordering::Acquire))
        });
        let engine = crate::Engine::spawn(cfg).await.unwrap();
        engine
            .import_job(record("original", JobStatus::Queued), false, false)
            .await
            .unwrap();
        allowed.store(false, Ordering::Release);
        assert!(engine
            .import_job(record("replacement", JobStatus::Completed), false, false)
            .await
            .is_err());
        assert_eq!(
            engine.recovery_document().await.unwrap().jobs[0].name,
            "original"
        );
        assert!(engine
            .adopt_replicated_authority(vec![record("replicated", JobStatus::Completed)])
            .await
            .is_err());
        let disk = SnapshotStore::open(&temp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert_eq!(disk.jobs.len(), 1);
        assert_eq!(disk.jobs[0].name, "original");
        allowed.store(true, Ordering::Release);
        engine.shutdown().await;
    }

    fn payload_record(status: JobStatus, bytes: &[u8], pending_tail: bool) -> Job {
        let mut job = record("replicated payload", status);
        job.params.push((
            "*File:size:1".into(),
            if pending_tail { "6" } else { "3" }.into(),
        ));
        let mut segments = vec![nzbd_types::Segment {
            message_id: "first".into(),
            number: 1,
            size: 3,
            state: SegmentState::Done {
                offset: 0,
                len: 3,
                crc: crc32fast::hash(bytes),
            },
        }];
        if pending_tail {
            segments.push(nzbd_types::Segment {
                message_id: "second".into(),
                number: 2,
                size: 3,
                state: SegmentState::Pending,
            });
        }
        job.files.push(nzbd_types::FileEntry {
            id: FileId(1),
            subject: "payload.bin".into(),
            filename: "payload.bin".into(),
            filename_confirmed: true,
            is_par2: false,
            paused: false,
            groups: vec![],
            date: None,
            crc32: None,
            finalized: false,
            segments,
        });
        job
    }

    async fn await_export(engine: &crate::EngineHandle, job: JobId) -> Job {
        let mut changes = engine.queue_changes.clone();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let _ = changes.borrow_and_update();
                match engine.export_job(job).await {
                    Ok(Some(job)) => return job,
                    Ok(None) => panic!("recovered job disappeared before becoming exportable"),
                    Err(crate::EngineError::Lifecycle(_)) => {
                        // A pending recovery ticket is an explicit retryable
                        // refusal. Wait for owner evidence to change before
                        // requiring the full recovered record below.
                    }
                    Err(error) => panic!("recovered job export failed: {error}"),
                }
                changes
                    .changed()
                    .await
                    .expect("owner stopped during recovery");
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "recovered job should become exportable: {:?}",
                engine.snapshot().jobs
            )
        })
    }

    async fn finish_fixture_verification(
        owner: &mut Owner,
        messages: &mut mpsc::Receiver<EngineMsg>,
        job: JobId,
    ) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while owner.verification.contains_key(&job) {
                owner.start_verifications();
                if let Some(state) = owner.verification.get(&job) {
                    assert_ne!(
                        state.phase, "blocked",
                        "recovery unexpectedly blocked: {:?}",
                        state.reason
                    );
                }
                owner.on_msg(
                    messages
                        .recv()
                        .await
                        .expect("verification producer must complete"),
                );
            }
        })
        .await
        .expect("real verification and durable acceptance must complete");
    }

    #[tokio::test]
    async fn journal_fold_before_root_reconciliation_keeps_recovery_loading() {
        let (_temp, mut owner, _adapter, mut messages) = super::super::tests::control_test_owner();
        super::super::tests::pump(&mut owner, &mut messages).await;
        let root = owner.dest_dir.clone();
        let path = root.join("fixture");
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join(".runner-file-1.part"), b"BAD").unwrap();
        owner.startup_reconciliation.clear();
        let held = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let _release_on_panic = ReleaseSave(held.clone());
        let (started, entered) = oneshot::channel();
        let worker_gate = held.clone();
        owner.payload_then(
            root,
            move || {
                let _ = started.send(());
                let mut released = worker_gate.0.lock().unwrap();
                while !*released {
                    released = worker_gate.1.wait(released).unwrap();
                }
                Ok(())
            },
            |_, result| result.unwrap(),
        );
        entered.await.unwrap();
        let (reply, mut adopted) = oneshot::channel();
        owner.on_command(QueueCommand::AdoptReplicatedAuthority {
            jobs: vec![payload_record(JobStatus::Completed, b"abc", false)],
            reply,
        });
        // Force journal replay to finish while the actual root worker still
        // owns its slot. This is the completion order that failed under CI.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                owner.on_msg(
                    messages
                        .recv()
                        .await
                        .expect("adoption/fold producer must complete"),
                );
                if owner.state.job(JobId(1)).is_some_and(|job| {
                    !job.params
                        .iter()
                        .any(|(key, _)| key == "*Recovery:journal-loading")
                }) && !owner.pending_journal_loads.contains_key(&JobId(1))
                {
                    break;
                }
            }
        })
        .await
        .expect("journal fold must complete independently of held payload root");
        assert!(adopted.try_recv().unwrap().is_ok());
        owner.start_verifications();
        let ticket = owner.verification.get(&JobId(1)).unwrap();
        assert_eq!(ticket.phase, "loading");
        assert!(!ticket.running && ticket.reason.is_none());
        assert!(!owner.allocation_cache.contains_key(&JobId(1)));
        assert!(matches!(
            owner.state.job(JobId(1)).unwrap().files[0].segments[0].state,
            SegmentState::Done { .. }
        ));
        let (reply, admission) = oneshot::channel();
        owner.on_command(QueueCommand::BeginPostAdmission {
            job: JobId(1),
            reply,
        });
        assert!(admission.await.unwrap().is_none());
        {
            let mut released = held.0.lock().unwrap();
            *released = true;
            held.1.notify_all();
        }
        super::super::tests::pump(&mut owner, &mut messages).await;
        finish_fixture_verification(&mut owner, &mut messages, JobId(1)).await;
        let recovered = owner.state.job(JobId(1)).unwrap();
        assert_eq!(recovered.status, JobStatus::Queued);
        assert_eq!(recovered.files[0].segments[0].state, SegmentState::Pending);
        assert!(owner.allocation_cache.contains_key(&JobId(1)));
    }

    #[tokio::test]
    async fn active_journal_fold_cannot_be_skipped_by_requeued_verification() {
        let (_temp, mut owner, _adapter, mut messages) = super::super::tests::control_test_owner();
        super::super::tests::pump(&mut owner, &mut messages).await;
        let path = owner.dest_dir.join("fixture");
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join(".runner-file-1.part"), b"abc").unwrap();
        owner
            .state
            .jobs
            .push(payload_record(JobStatus::Paused, b"abc", false));
        owner
            .artifacts
            .register_legacy_active(1, &owner.dest_dir, &path)
            .unwrap();
        owner
            .allocation_cache
            .insert(JobId(1), owner.artifacts.for_job(1).unwrap().unwrap());
        owner.allocated_jobs.insert(JobId(1));
        let held = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let _release_on_panic = ReleaseSave(held.clone());
        let (started, entered) = oneshot::channel();
        let worker_gate = held.clone();
        owner.io_then(
            move || {
                let _ = started.send(());
                let mut released = worker_gate.0.lock().unwrap();
                while !*released {
                    released = worker_gate.1.wait(released).unwrap();
                }
                Ok(())
            },
            |_, result| result.unwrap(),
        );
        entered.await.unwrap();
        owner.fold_job_journals(JobId(1));
        // Simulate the independently finishing allocation path requeuing a
        // ticket with a valid cache while journal replay still owns its slot.
        owner.queue_verification(JobId(1));
        owner.start_verifications();
        let ticket = owner.verification.get(&JobId(1)).unwrap();
        assert_eq!(ticket.phase, "loading");
        assert!(!ticket.running && ticket.reason.is_none());
        assert!(owner.verification_roots.is_empty());
        {
            let mut released = held.0.lock().unwrap();
            *released = true;
            held.1.notify_all();
        }
        super::super::tests::pump(&mut owner, &mut messages).await;
        assert!(!owner.pending_journal_loads.contains_key(&JobId(1)));
        finish_fixture_verification(&mut owner, &mut messages, JobId(1)).await;
        assert_eq!(owner.state.job(JobId(1)).unwrap().status, JobStatus::Paused);
        assert!(matches!(
            owner.state.job(JobId(1)).unwrap().files[0].segments[0].state,
            SegmentState::Done { .. }
        ));
        assert!(owner
            .state
            .job(JobId(1))
            .unwrap()
            .params
            .iter()
            .any(|(key, _)| key == "*File:proof:1"));
    }

    #[tokio::test]
    async fn replicated_only_partial_rebuilds_allocation_and_verifies_legacy_ranges() {
        let temp = tempfile::tempdir().unwrap();
        let cfg = config(&temp);
        let path = cfg.dest_dir.join("fixture");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join(".runner-file-1.part"), b"abc\0\0\0").unwrap();
        let engine = crate::Engine::spawn(cfg).await.unwrap();
        engine
            .adopt_replicated_authority(vec![payload_record(JobStatus::Paused, b"abc", true)])
            .await
            .unwrap();
        let recovered = await_export(&engine, JobId(1)).await;
        assert_eq!(recovered.status, JobStatus::Paused);
        assert!(matches!(
            recovered.files[0].segments[0].state,
            SegmentState::Done { .. }
        ));
        assert_eq!(recovered.files[0].segments[1].state, SegmentState::Pending);
        assert!(recovered
            .params
            .iter()
            .any(|(key, _)| key == "*File:proof:1"));
        assert!(engine.artifacts().for_job(1).unwrap().is_some());
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn replicated_completed_without_post_receipt_cannot_skip_bad_range_verification() {
        let temp = tempfile::tempdir().unwrap();
        let cfg = config(&temp);
        let path = cfg.dest_dir.join("fixture");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join(".runner-file-1.part"), b"BAD").unwrap();
        let engine = crate::Engine::spawn(cfg).await.unwrap();
        engine
            .adopt_replicated_authority(vec![payload_record(JobStatus::Completed, b"abc", false)])
            .await
            .unwrap();
        assert!(engine
            .begin_post_admission(JobId(1))
            .await
            .unwrap()
            .is_none());
        let recovered = await_export(&engine, JobId(1)).await;
        assert_eq!(recovered.status, JobStatus::Queued);
        assert_eq!(recovered.files[0].segments[0].state, SegmentState::Pending);
        assert!(!recovered
            .params
            .iter()
            .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM));
        assert!(engine
            .begin_post_admission(JobId(1))
            .await
            .unwrap()
            .is_none());
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn post_admission_waits_for_imported_payload_recovery_changes() {
        let temp = tempfile::tempdir().unwrap();
        let cfg = config(&temp);
        let path = cfg.dest_dir.join("fixture");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("payload.bin"), b"abc").unwrap();
        let engine = crate::Engine::spawn(cfg).await.unwrap();
        let mut job = payload_record(JobStatus::Completed, b"abc", false);
        job.files[0].finalized = true;
        engine.import_job(job, false, false).await.unwrap();
        let cancellation = CancellationToken::new();
        let admission = tokio::time::timeout(
            Duration::from_secs(5),
            engine.wait_post_admission(JobId(1), &cancellation),
        )
        .await
        .expect("owner verification changes should wake PP admission")
        .unwrap()
        .unwrap();
        let scoped = engine.for_post_admission(&admission);
        let mut stamp = scoped.export_job(JobId(1)).await.unwrap().unwrap();
        stamp
            .params
            .push((nzbd_types::PP_DONE_PARAM.into(), "SUCCESS/ALL".into()));
        assert!(scoped.import_job_if_present(stamp).await.unwrap());
        engine.end_post_admission(admission).await.unwrap();
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn cancelled_post_admission_wait_does_not_require_another_queue_change() {
        let temp = tempfile::tempdir().unwrap();
        let engine = crate::Engine::spawn(config(&temp)).await.unwrap();
        engine
            .import_job(record("queued", JobStatus::Queued), false, false)
            .await
            .unwrap();
        let cancellation = CancellationToken::new();
        let waiter_engine = engine.clone();
        let waiter_cancel = cancellation.clone();
        let waiter = tokio::spawn(async move {
            waiter_engine
                .wait_post_admission(JobId(1), &waiter_cancel)
                .await
        });
        tokio::task::yield_now().await;
        cancellation.cancel();
        assert!(tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .is_none());
        assert!(engine.export_job(JobId(1)).await.unwrap().is_some());
        engine.shutdown().await;
    }

    fn article_range_record(first: u32, index: u32, fence: u64) -> Job {
        let last = first + 31;
        let mut job = payload_record(JobStatus::Queued, b"abc", false);
        job.dir_name =
            format!(".nzbd-cluster/range-work/job-1/file-1/{first}-{last}-fence-{fence}");
        job.params.retain(|(key, _)| key != "*File:size:1");
        job.params.push((
            "*Cluster:range".into(),
            serde_json::json!({
                "job_id": 1, "file_id": 1, "first_article": first, "last_article": last,
                "range_index": index, "range_count": 3,
            })
            .to_string(),
        ));
        job.files[0].segments = (first..=last)
            .map(|number| nzbd_types::Segment {
                message_id: format!("article-{number}").into_boxed_str(),
                number,
                size: 1,
                state: SegmentState::Pending,
            })
            .collect();
        job
    }

    #[test]
    fn private_article_parent_requires_exact_typed_scope_and_fenced_namespace() {
        let destination = Path::new("/configured/payload");
        let valid = article_range_record(1, 0, 7);
        let plan = imported_payload_plan(&valid, destination).unwrap();
        assert!(plan.private_range);
        assert_eq!(
            plan.root,
            destination.join(".nzbd-cluster/range-work/job-1/file-1")
        );
        assert_eq!(plan.path.parent(), Some(plan.root.as_path()));
        let flat = record("ordinary", JobStatus::Queued);
        assert_eq!(
            imported_payload_plan(&flat, destination).unwrap().root,
            destination
        );
        let mut untyped = valid.clone();
        untyped.params.retain(|(key, _)| key != "*Cluster:range");
        let ordinary = imported_payload_plan(&untyped, destination).unwrap();
        assert!(!ordinary.private_range);
        assert_eq!(ordinary.root, destination);
        for namespace in [
            ".nzbd-cluster/range-work/job-1/file-1/1-32-fence-0",
            ".nzbd-cluster/range-work/job-2/file-1/1-32-fence-7",
            ".nzbd-cluster/range-work/job-1/file-1/1-32-fence-07",
            ".nzbd-cluster/range-work/job-1/file-1/../1-32-fence-7",
        ] {
            let mut malformed = valid.clone();
            malformed.dir_name = namespace.into();
            assert!(
                imported_payload_plan(&malformed, destination).is_err(),
                "{namespace}"
            );
        }
        let mut missing = valid.clone();
        missing.files[0].segments.pop();
        assert!(imported_payload_plan(&missing, destination).is_err());
        let mut duplicate = valid;
        duplicate.files[0].segments[1].number = 1;
        assert!(imported_payload_plan(&duplicate, destination).is_err());
    }

    #[tokio::test]
    async fn sequential_private_ranges_rebuild_nested_allocations_without_reusing_old_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let mut cfg = config(&temp);
        cfg.persist_queue = false;
        let destination = cfg.dest_dir.clone();
        let engine = crate::Engine::spawn(cfg).await.unwrap();
        let mut first = article_range_record(1, 0, 7);
        first.delivery_lifecycle = true;
        first
            .params
            .push(("*Runner:KeepOnFailure".into(), "1".into()));
        engine
            .import_job(first.clone(), false, false)
            .await
            .unwrap();
        let recovered = await_export(&engine, JobId(1)).await;
        assert_eq!(recovered.dir_name, first.dir_name);
        let inventory = engine.artifacts();
        let old = inventory.for_job(1).unwrap().unwrap();
        assert_eq!(
            old.root,
            destination.join(".nzbd-cluster/range-work/job-1/file-1")
        );
        assert_eq!(old.path, destination.join(&first.dir_name));
        assert_eq!(old.state, "active");
        assert!(old.owned && old.keep_on_failure && old.delivery_lifecycle);
        let old_payload = old.path.join(".runner-file-1.part");
        std::fs::write(&old_payload, b"first range bytes remain retained").unwrap();
        assert!(engine.remove_job_silent(JobId(1)).await.unwrap());

        let second = article_range_record(65, 2, 9);
        engine
            .import_job(second.clone(), false, false)
            .await
            .unwrap();
        let recovered = await_export(&engine, JobId(1)).await;
        assert_eq!(recovered.dir_name, second.dir_name);
        let current = inventory.for_job(1).unwrap().unwrap();
        assert_eq!(current.path, destination.join(&second.dir_name));
        assert_eq!(current.path.parent(), Some(current.root.as_path()));
        assert_ne!(current.id, old.id);
        assert_ne!(current.generation, old.generation);
        assert!(current.keep_on_failure && current.delivery_lifecycle);
        assert!(!current.path.join(".runner-file-1.part").exists());
        assert_eq!(
            std::fs::read(old_payload).unwrap(),
            b"first range bytes remain retained"
        );
        let retained = inventory.get(&old.id).unwrap();
        assert_eq!(retained.state, "retained");
        assert!(retained.job.is_none() && retained.hold.is_some());
        engine.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn private_range_parent_creation_refuses_symlink_escape() {
        let temp = tempfile::tempdir().unwrap();
        let cfg = config(&temp);
        std::fs::create_dir_all(&cfg.dest_dir).unwrap();
        let outside = temp.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, cfg.dest_dir.join(".nzbd-cluster")).unwrap();
        let engine = crate::Engine::spawn(cfg).await.unwrap();
        engine
            .import_job(article_range_record(1, 0, 7), false, false)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = engine.snapshot();
                if snapshot
                    .jobs
                    .first()
                    .and_then(|job| job.verification.as_ref())
                    .is_some_and(|progress| progress.state == "blocked")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("unsafe private parent must remain visibly blocked");
        assert!(matches!(
            engine.export_job(JobId(1)).await,
            Err(crate::EngineError::Lifecycle(_))
        ));
        assert!(engine.artifacts().for_job(1).unwrap().is_none());
        assert!(!outside.join("range-work").exists());
        engine.shutdown().await;
    }

    #[test]
    fn completed_download_without_post_receipt_uses_the_same_recovery_classification() {
        let mut job = record("completed", JobStatus::Completed);
        job.files.push(nzbd_types::FileEntry {
            id: FileId(1),
            subject: "payload".into(),
            filename: "payload".into(),
            filename_confirmed: true,
            is_par2: false,
            paused: false,
            groups: vec![],
            date: None,
            crc32: None,
            finalized: true,
            segments: vec![nzbd_types::Segment {
                message_id: "article".into(),
                number: 1,
                size: 3,
                state: SegmentState::Done {
                    offset: 0,
                    len: 3,
                    crc: 7,
                },
            }],
        });
        assert!(needs_payload(&job));
        normalize(&mut job);
        assert!(job.params.iter().any(|(key, _)| key == "*Recovery:pending"));
        assert!(job
            .params
            .iter()
            .any(|(key, _)| key == "*Recovery:journal-loading"));
        job.params
            .push((nzbd_types::PP_DONE_PARAM.into(), "SUCCESS/ALL".into()));
        assert!(!needs_payload(&job));
    }
}
