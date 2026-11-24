//! Durable native acquisition failure and history recording.
use super::*;
impl Owner {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn fail_native_torrent_then(
        &mut self,
        id: JobId,
        donor: Option<JobId>,
        (reason, fetch_cause): (nzbd_types::FailureReason, Option<nzbd_types::FetchCause>),
        stage: String,
        message: String,
        cleanup_complete: bool,
        callback: impl FnOnce(&mut Owner, bool) + Send + 'static,
    ) {
        if self.persistence_fenced
            || !self.persist
            || !matches!(stage.as_str(), "torrent_metadata" | "torrent_source")
        {
            callback(self, false);
            return;
        }
        let Some(before) = self
            .state
            .job(id)
            .filter(|row| {
                crate::torrent_acquisitions::is_acquisition(row)
                    && !row.params.iter().any(|(key, value)| {
                        key == crate::torrent_acquisitions::COPY_COMPLETE_PARAM && value == "1"
                    })
                    && !matches!(row.status, JobStatus::Deleted)
            })
            .cloned()
        else {
            callback(self, false);
            return;
        };
        let already_failed = before.status == JobStatus::Failed
            && before
                .params
                .iter()
                .any(|(key, value)| key == "Failure:Stage" && value == &stage);
        if !already_failed
            && if stage == "torrent_metadata" {
                donor.is_some()
                    || !self
                        .state
                        .pending_admissions
                        .iter()
                        .any(|pending| pending.job_id == id)
            } else {
                donor.is_none() || crate::torrent_acquisitions::donor(&before) != donor
            }
        {
            callback(self, false);
            return;
        }
        let previous_pending = self
            .state
            .pending_admissions
            .iter()
            .filter(|pending| pending.job_id == id)
            .cloned()
            .collect::<Vec<_>>();
        let previous_receipts = self
            .state
            .url_admission_receipts
            .iter()
            .filter(|receipt| receipt.job_id == id.0)
            .cloned()
            .collect::<Vec<_>>();
        let before_value = serde_json::to_value(&before).ok();
        let keep = before
            .params
            .iter()
            .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1");
        let row = self.state.job_mut(id).unwrap();
        row.status = JobStatus::Failed;
        row.params
            .retain(|(key, _)| key != nzbd_types::CONTROL_PARAM);
        set_owner_param(
            &mut row.params,
            nzbd_types::PP_DONE_PARAM,
            Some("FAILURE/TORRENT_SOURCE".into()),
        );
        set_owner_param(&mut row.params, "Failure:Stage", Some(stage.clone()));
        set_owner_param(
            &mut row.params,
            "Failure:Reason",
            Some(reason.as_str().into()),
        );
        set_owner_param(
            &mut row.params,
            "Failure:Message",
            Some(message.chars().take(2048).collect()),
        );
        set_owner_param(
            &mut row.params,
            "Failure:KeepOnFailure",
            Some(keep.to_string()),
        );
        set_owner_param(&mut row.params, "*Torrent:SourceFailed", Some("1".into()));
        if !already_failed {
            set_owner_param(
                &mut row.params,
                "*Torrent:FailedAt",
                Some(unix_now().to_string()),
            );
        }
        if cleanup_complete {
            set_owner_param(
                &mut row.params,
                "Delivery:SourceCleanup",
                Some(if keep { "kept" } else { "deleted" }.into()),
            );
        }
        self.state
            .pending_admissions
            .retain(|pending| pending.job_id != id);
        self.state.sync_url_admission_receipts();
        let expected_receipts = serde_json::to_value(
            self.state
                .url_admission_receipts
                .iter()
                .filter(|receipt| receipt.job_id == id.0)
                .map(|receipt| {
                    (
                        &receipt.request_token,
                        &receipt.submission_sha256,
                        receipt.job_id,
                        &receipt.generation,
                        &receipt.runner_instance_id,
                        &receipt.source_artifact_id,
                        &receipt.source_generation,
                    )
                })
                .collect::<Vec<_>>(),
        )
        .ok();
        self.native_commit_then(vec![before], move |owner, committed| {
            if !committed {
                // The shared commit helper rolls back only unchanged rows.
                // Restore only this admission's metadata, and only while it
                // still matches the failed proposal; never overwrite peers.
                let restored = before_value.as_ref().is_some_and(|before| {
                    owner.state.job(id).is_some_and(|current| {
                        serde_json::to_value(current).ok().as_ref() == Some(before)
                    })
                });
                if restored
                    && !owner
                        .state
                        .pending_admissions
                        .iter()
                        .any(|pending| pending.job_id == id)
                {
                    owner.state.pending_admissions.extend(previous_pending);
                    let current = serde_json::to_value(
                        owner
                            .state
                            .url_admission_receipts
                            .iter()
                            .filter(|receipt| receipt.job_id == id.0)
                            .map(|receipt| {
                                (
                                    &receipt.request_token,
                                    &receipt.submission_sha256,
                                    receipt.job_id,
                                    &receipt.generation,
                                    &receipt.runner_instance_id,
                                    &receipt.source_artifact_id,
                                    &receipt.source_generation,
                                )
                            })
                            .collect::<Vec<_>>(),
                    )
                    .ok();
                    if current == expected_receipts {
                        owner
                            .state
                            .url_admission_receipts
                            .retain(|receipt| receipt.job_id != id.0);
                        owner.state.url_admission_receipts.extend(previous_receipts);
                    }
                    owner.dirty = true;
                    owner.pending_control_commits.insert(id);
                    owner.record_transitions.insert(id);
                    owner.persist_then(move |owner, _| {
                        owner.pending_control_commits.remove(&id);
                        owner.record_transitions.remove(&id);
                        callback(owner, false);
                        owner.drain_replacements();
                    });
                    return;
                }
                callback(owner, false);
                return;
            }
            let Some(row) = owner.state.job(id).cloned() else {
                callback(owner, false);
                return;
            };
            if row
                .params
                .iter()
                .any(|(key, value)| key == "*Torrent:FailureRecorded" && value == "1")
            {
                owner.bump_epoch();
                owner.publish_now();
                callback(owner, true);
                return;
            }
            // Contract §2.1: the acquisition's failure is the job's one
            // terminal — row, history, queue stamp and `job_pp_finished` from
            // the shared chain (the chain's markers replace the provisional
            // `*PP:done`).
            let mut terminal =
                crate::terminal::failed_terminal(&row, reason, fetch_cause, &stage, &message);
            if let Some(at) = row
                .params
                .iter()
                .find(|(key, _)| key == "*Torrent:FailedAt")
                .and_then(|(_, value)| value.parse::<i64>().ok())
            {
                terminal.entry.completed_at_unix = at;
            }
            owner.record_terminal_then(
                terminal,
                Box::new(move |owner, recorded| {
                    if recorded.is_none() || owner.state.job(id).is_none() {
                        owner.publish_now();
                        callback(owner, false);
                        return;
                    }
                    let before_marker = owner.state.job(id).unwrap().clone();
                    set_owner_param(
                        &mut owner.state.job_mut(id).unwrap().params,
                        "*Torrent:FailureRecorded",
                        Some("1".into()),
                    );
                    owner.native_commit_then(vec![before_marker], move |owner, recorded| {
                        owner.pending_control_commits.remove(&id);
                        owner.record_transitions.remove(&id);
                        if recorded {
                            owner.bump_epoch();
                        }
                        owner.publish_now();
                        callback(owner, recorded);
                    });
                }),
            );
        });
    }
}
