//! Durable managed seed and acquisition controls.
use super::*;
impl Owner {
    pub(super) fn note_seed_keep_block_then(
        &mut self,
        id: JobId,
        expected: crate::torrent_acquisitions::ManagedSeedRetirement,
        operation: Option<String>,
        callback: impl FnOnce(&mut Owner, bool) + Send + 'static,
    ) {
        use crate::torrent_acquisitions::*;
        if self.persistence_fenced || !self.persist {
            callback(self, false);
            return;
        }
        let Some(before) = self.state.job(id).cloned() else {
            callback(self, false);
            return;
        };
        let Some(mut marker) = seed_retirement(&before)
            .filter(|marker| marker == &expected && marker.phase == "detached")
        else {
            callback(self, false);
            return;
        };
        let Ok(source) = self.artifacts.get(&marker.source.artifact) else {
            callback(self, false);
            return;
        };
        if source.generation != marker.source.generation
            || source.job != Some(id.0)
            || !source.owned
        {
            callback(self, false);
            return;
        }
        let blocked_revision = if let Some(operation) = operation.as_deref() {
            let Ok(op) = self.artifacts.operation(operation) else {
                callback(self, false);
                return;
            };
            let Ok(request) = serde_json::from_str::<serde_json::Value>(&op.request) else {
                callback(self, false);
                return;
            };
            if op.state != "cancelled"
                || op.error.as_deref() != Some("per-download Keep on failure is enabled")
                || op.artifact != source.id
                || request["generation"].as_str() != Some(source.generation.as_str())
                || request["intent"]["identity"].as_str() != Some(marker.identity.as_str())
            {
                callback(self, false);
                return;
            }
            let Some(revision) = request["revision"].as_u64() else {
                callback(self, false);
                return;
            };
            revision
        } else {
            if !(source.keep
                || (marker.reason == nzbd_state::artifacts::TorrentSeedRetirementReason::Failure
                    && source.keep_on_failure))
            {
                callback(self, false);
                return;
            }
            source.revision
        };
        marker.keep_blocked_revision = Some(blocked_revision);
        marker.keep_blocked_control_revision = before
            .control()
            .filter(|control| control.lifecycle == "held" && control.stage == "torrent_seed_retire")
            .map(|control| control.revision);
        set_owner_param(
            &mut self.state.job_mut(id).unwrap().params,
            SEED_RETIREMENT_PARAM,
            Some(serde_json::to_string(&marker).unwrap()),
        );
        if marker.keep_blocked_control_revision.is_none() && !self.prepare_hold_job_until(
            id,nzbd_types::HoldCause::Io,"torrent_seed_retire",
            "Current Keep retains this seed source; clearing Keep will requeue eligible cleanup",None,
        ) { *self.state.job_mut(id).unwrap()=before;callback(self,false);return; }
        self.native_commit_then(vec![before], callback);
    }
    pub(super) fn rearm_seed_keep_cleared_then(
        &mut self,
        id: JobId,
        callback: impl FnOnce(&mut Owner, bool) + Send + 'static,
    ) {
        use crate::torrent_acquisitions::*;
        let Some(row) = self.state.job(id).cloned() else {
            callback(self, false);
            return;
        };
        let Some(marker) = seed_retirement(&row).filter(|marker| marker.phase == "detached") else {
            callback(self, false);
            return;
        };
        let Some(blocked) = marker.keep_blocked_revision else {
            callback(self, false);
            return;
        };
        let Some(control) = row.control().filter(|control| {
            control.lifecycle == "held"
                && control.stage == "torrent_seed_retire"
                && !control.manual_pause
                && Some(&control.revision) == marker.keep_blocked_control_revision.as_ref()
        }) else {
            callback(self, false);
            return;
        };
        if self
            .state
            .jobs
            .iter()
            .any(|child| source_dependency(child) == Some(id))
        {
            callback(self, false);
            return;
        }
        let Ok(source) = self.artifacts.get(&marker.source.artifact) else {
            callback(self, false);
            return;
        };
        if source.generation != marker.source.generation
            || source.revision <= blocked
            || source.keep
            || (marker.reason == nzbd_state::artifacts::TorrentSeedRetirementReason::Failure
                && source.keep_on_failure)
        {
            callback(self, false);
            return;
        }
        let guard =
            crate::hold_release::HoldReleaseGuard::prepare_queue_owner(&row, Some(&source), None);
        if self
            .retry_hold_proven(id, &control.revision, RetryBy::Operator, false, &guard)
            .is_err()
        {
            callback(self, false);
            return;
        }
        self.native_commit_then(vec![row], callback);
    }
    pub(super) fn sync_managed_seed_keep(&self, donor: JobId) -> bool {
        let Some(seed) = self
            .state
            .job(donor)
            .filter(|row| crate::torrent_acquisitions::is_managed_seed(row))
        else {
            return true;
        };
        let desired = seed
            .params
            .iter()
            .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1")
            || self.state.jobs.iter().any(|row| {
                crate::torrent_acquisitions::source_dependency(row) == Some(donor)
                    && row
                        .params
                        .iter()
                        .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1")
            });
        match self.artifacts.for_job(donor.0) {
            Ok(Some(source)) if source.owned && !source.terminal() => self
                .artifacts
                .set_keep_on_failure(donor.0, desired)
                .unwrap_or(false),
            Ok(Some(source)) if source.terminal() => !desired,
            Ok(_) => true,
            Err(_) => false,
        }
    }
    pub(super) fn begin_managed_seed_retirement_then(
        &mut self,
        id: JobId,
        callback: impl FnOnce(&mut Owner, bool) + Send + 'static,
    ) {
        use crate::torrent_acquisitions::*;
        if self.persistence_fenced || !self.persist {
            callback(self, false);
            return;
        }
        let Some(before) = self
            .state
            .job(id)
            .filter(|job| is_managed_seed(job))
            .cloned()
        else {
            callback(self, false);
            return;
        };
        if let Some(marker) = seed_retirement(&before) {
            if marker.phase != "requested" || before.held() {
                callback(self, true);
                return;
            }
            let record = before.torrent.as_ref().unwrap();
            let command = BackendCommand::Remove {
                job: id,
                delete_data: false,
                content_path: record.content_path.clone(),
                files: record.files.clone(),
                allowed_roots: self.torrent_payload_roots.clone(),
            };
            if !self.prepare_hold_job_until(
                id,
                nzbd_types::HoldCause::StorageUnavailable,
                "torrent_seed_forget",
                "Waiting for backend stop and forget acknowledgment",
                None,
            ) {
                callback(self, false);
                return;
            }
            self.native_commit_then(vec![before], move |owner, committed| {
                let queued = committed && owner.enqueue_backend_command(command);
                callback(owner, queued);
            });
            return;
        }
        let torrent = before.torrent.as_ref().unwrap();
        if before.held()
            || !(torrent.phase == TorrentPhase::Failed
                || (torrent.phase == TorrentPhase::PausedSeed
                    && matches!(
                        torrent.stop_reason,
                        Some(
                            nzbd_types::TorrentStopReason::DownloadComplete
                                | nzbd_types::TorrentStopReason::RatioLimit
                                | nzbd_types::TorrentStopReason::TimeLimit
                        )
                    )))
        {
            callback(self, false);
            return;
        }
        let Ok(Some(source)) = self.artifacts.for_job(id.0) else {
            callback(self, false);
            return;
        };
        if !source.owned
            || !source.automatic_cleanup_eligible
            || source.path != torrent.payload_root
            || source.terminal()
        {
            callback(self, false);
            return;
        }
        let mut marker = ManagedSeedRetirement {
            identity: String::new(),
            storage_instance_id: self.artifacts.installation.clone(),
            reason: if torrent.phase == TorrentPhase::Failed {
                nzbd_state::artifacts::TorrentSeedRetirementReason::Failure
            } else {
                nzbd_state::artifacts::TorrentSeedRetirementReason::SuccessfulPolicy
            },
            retry_revision: "0".into(),
            keep_blocked_revision: None,
            keep_blocked_control_revision: None,
            source: nzbd_state::artifacts::SourceGeneration {
                artifact: source.id,
                generation: source.generation,
            },
            payload_root: source.path,
            phase: "requested".into(),
        };
        let Some(identity) = marker.expected_identity(id) else {
            callback(self, false);
            return;
        };
        marker.identity = identity;
        let row = self.state.job_mut(id).unwrap();
        set_owner_param(
            &mut row.params,
            SEED_RETIREMENT_PARAM,
            Some(serde_json::to_string(&marker).unwrap()),
        );
        row.torrent.as_mut().unwrap().removal_intent =
            Some(TorrentRemovalIntent { delete_data: false });
        row.torrent.as_mut().unwrap().removal_outcome = None;
        row.torrent.as_mut().unwrap().removal_confirmed_at_unix = None;
        let command = BackendCommand::Remove {
            job: id,
            delete_data: false,
            content_path: torrent.content_path.clone(),
            files: torrent.files.clone(),
            allowed_roots: self.torrent_payload_roots.clone(),
        };
        if !self.prepare_hold_job_until(
            id,
            nzbd_types::HoldCause::StorageUnavailable,
            "torrent_seed_forget",
            "Waiting for backend stop and forget acknowledgment",
            None,
        ) {
            *self.state.job_mut(id).unwrap() = before;
            callback(self, false);
            return;
        }
        self.native_commit_then(vec![before], move |owner, committed| {
            let queued = committed && owner.enqueue_backend_command(command);
            callback(owner, queued);
        });
    }
    pub(super) fn set_native_acquisition_paused_then(
        &mut self,
        id: JobId,
        paused: bool,
        callback: impl FnOnce(&mut Owner, bool) + Send + 'static,
    ) {
        let Some(child) = self
            .state
            .job(id)
            .filter(|row| {
                crate::torrent_acquisitions::is_acquisition(row)
                    && !row.held()
                    && matches!(
                        row.status,
                        JobStatus::Fetching
                            | JobStatus::Queued
                            | JobStatus::Downloading
                            | JobStatus::Paused
                    )
            })
            .cloned()
        else {
            callback(self, false);
            return;
        };
        if self.persistence_fenced || !self.persist {
            callback(self, false);
            return;
        }
        let donor_before = crate::torrent_acquisitions::donor(&child)
            .and_then(|donor| self.state.job(donor))
            .filter(|donor| {
                donor.params.iter().any(|(key, value)| {
                    key == crate::torrent_acquisitions::SEED_GATE_PARAM
                        && value == &id.0.to_string()
                })
            })
            .cloned();
        let pending = self
            .state
            .pending_admissions
            .iter()
            .any(|pending| pending.job_id == id);
        self.state.job_mut(id).unwrap().status = if paused {
            JobStatus::Paused
        } else if pending {
            JobStatus::Fetching
        } else {
            JobStatus::Queued
        };
        if !paused {
            if let Some(before) = &donor_before {
                let donor = self.state.job_mut(before.id).unwrap();
                donor
                    .params
                    .retain(|(key, _)| key != crate::torrent_acquisitions::SEED_GATE_PARAM);
                donor.status = JobStatus::Queued;
                donor.torrent.as_mut().unwrap().control_intent = TorrentControlIntent::Running;
            }
        }
        let mut before = vec![child];
        if let Some(donor) = donor_before {
            before.push(donor);
        }
        self.native_commit_then(before, callback);
    }
}
