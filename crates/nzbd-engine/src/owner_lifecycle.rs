//! Native lifecycle admissions and exact managed seed transitions.
use super::*;
impl Owner {
    pub(super) fn native_commit_then(
        &mut self,
        before: Vec<Job>,
        callback: impl FnOnce(&mut Owner, bool) + Send + 'static,
    ) {
        let provisional = before
            .iter()
            .filter_map(|old| self.state.job(old.id).cloned())
            .collect::<Vec<_>>();
        let ids = before.iter().map(|old| old.id).collect::<Vec<_>>();
        let receipts = self
            .state
            .url_admission_receipts
            .iter()
            .filter(|row| ids.contains(&JobId(row.job_id)))
            .cloned()
            .collect::<Vec<_>>();
        for id in &ids {
            self.pending_control_commits.insert(*id);
            self.record_transitions.insert(*id);
        }
        self.dirty = true;
        self.persist_then(move |owner, committed| {
            let current = ids.iter().all(|id| owner.state.job(*id).is_some());
            let finish = move |owner: &mut Owner, ok: bool| {
                for id in &ids {
                    owner.record_transitions.remove(id);
                    if !owner.pending_control_revisions.contains_key(id) {
                        owner.pending_control_commits.remove(id);
                    }
                }
                owner.publish_now();
                owner.bump_epoch();
                callback(owner, ok);
                owner.drain_replacements();
            };
            if !committed {
                for mut old in before {
                    if let Some(current) = owner.state.job_mut(old.id) {
                        if let Some(staged) = provisional.iter().find(|row| row.id == old.id) {
                            super::transitions::preserve_intent_changes(&mut old, current, staged);
                        }
                        *current = old;
                    }
                }
                let rollback_ids = provisional.iter().map(|row| row.id.0).collect::<Vec<_>>();
                owner
                    .state
                    .url_admission_receipts
                    .retain(|row| !rollback_ids.contains(&row.job_id));
                owner.state.url_admission_receipts.extend(receipts);
                owner.dirty = true;
                owner.persist_then(move |owner, _restored| finish(owner, false));
            } else {
                finish(owner, current);
            }
        });
    }
    pub(super) fn on_native_lifecycle_command(
        &mut self,
        cmd: QueueCommand,
    ) -> Option<QueueCommand> {
        match cmd {
            QueueCommand::NoteSeedKeepBlock {
                job,
                marker,
                operation,
                reply,
            } => {
                self.note_seed_keep_block_then(job, marker, operation, move |_, ok| {
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::RearmSeedKeepCleared { job, reply } => {
                self.rearm_seed_keep_cleared_then(job, move |_, ok| {
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::SyncManagedSeedKeep { donor, reply } => {
                let _ = reply.send(self.sync_managed_seed_keep(donor));
            }
            QueueCommand::BeginManagedSeedRetirement { job, reply } => {
                self.begin_managed_seed_retirement_then(job, move |_, ok| {
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::CompleteManagedSeedRetirement {
                job,
                identity,
                reply,
            } => {
                let before = self.state.job(job).cloned();
                let retirement = before
                    .as_ref()
                    .and_then(crate::torrent_acquisitions::seed_retirement);
                let eligible = !self.persistence_fenced
                    && self.persist
                    && retirement.as_ref().is_some_and(|retirement| {
                        retirement.identity == identity
                            && retirement.storage_instance_id == self.artifacts.installation
                            && retirement.phase == "detached"
                            && self
                                .artifacts
                                .get(&retirement.source.artifact)
                                .is_ok_and(|source| {
                                    source.generation == retirement.source.generation
                                        && source.state == "deleted"
                                })
                    });
                if eligible {
                    let row = self.state.job_mut(job).unwrap();
                    let mut marker = retirement.unwrap();
                    marker.phase = "deleted".into();
                    set_owner_param(
                        &mut row.params,
                        crate::torrent_acquisitions::SEED_RETIREMENT_PARAM,
                        Some(serde_json::to_string(&marker).unwrap()),
                    );
                    row.torrent.as_mut().unwrap().removal_outcome =
                        Some(TorrentPayloadDisposition::Deleted);
                }
                if !eligible {
                    let _ = reply.send(false);
                    return None;
                }
                self.native_commit_then(before.into_iter().collect(), move |_, ok| {
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::ClaimNativeFailureCleanup { job, reply } => {
                let before =
                    self.state
                        .job(job)
                        .filter(|row| {
                            crate::torrent_acquisitions::is_acquisition(row)
                                && row.status == JobStatus::Failed
                                && row.params.iter().any(|(key, value)| {
                                    key == "Failure:Stage" && value == "torrent_metadata"
                                })
                                && !row.params.iter().any(|(key, value)| {
                                    key == "*Runner:KeepOnFailure" && value == "1"
                                })
                                && !row.params.iter().any(|(key, value)| {
                                    key == "Delivery:SourceCleanup" && value == "deleted"
                                })
                        })
                        .cloned();
                let Some(before) = before.filter(|_| !self.persistence_fenced && self.persist)
                else {
                    let _ = reply.send(None);
                    return None;
                };
                set_owner_param(
                    &mut self.state.job_mut(job).unwrap().params,
                    "Delivery:SourceCleanup",
                    Some("deleting".into()),
                );
                self.native_commit_then(vec![before], move |owner, ok| {
                    let _ = reply.send(ok.then(|| owner.state.job(job).cloned()).flatten());
                });
            }
            QueueCommand::FailNativeTorrent {
                job,
                donor,
                stage,
                message,
                cleanup_complete,
                reply,
            } => {
                self.fail_native_torrent_then(
                    job,
                    donor,
                    stage,
                    message,
                    cleanup_complete,
                    move |_, ok| {
                        let _ = reply.send(ok);
                    },
                );
            }
            QueueCommand::NativeTorrentPending { job, reply } => {
                let _ = reply.send(
                    self.state
                        .pending_admissions
                        .iter()
                        .find(|pending| pending.job_id == job)
                        .cloned(),
                );
            }
            QueueCommand::ReserveManagedTorrentAdmission {
                source,
                submission_sha256,
                opts,
                defaults,
                reply,
            } => {
                if self.persistence_fenced || !self.persist {
                    let _ = reply.send(Err(crate::EngineError::AdmissionNotDurable));
                    return None;
                }
                let jobs = self.state.jobs.len();
                let result = self.state.reserve_managed_torrent_admission(
                    source,
                    submission_sha256,
                    opts,
                    defaults,
                    &self.consumer_instance_id,
                    &self.artifacts.installation,
                );
                let created = result.as_ref().is_ok_and(|result| result.created);
                if created {
                    mark_new_lifecycle_cohort(self.state.jobs.last_mut().unwrap());
                }
                if !created {
                    let _ = reply.send(result);
                    return None;
                }
                let created_ids = self.state.jobs[jobs..]
                    .iter()
                    .map(|row| row.id)
                    .collect::<Vec<_>>();
                for id in &created_ids {
                    self.pending_control_commits.insert(*id);
                    self.record_transitions.insert(*id);
                }
                self.persist_then(move |owner, ok| {
                    if !ok {
                        owner
                            .state
                            .jobs
                            .retain(|row| !created_ids.contains(&row.id));
                        owner
                            .state
                            .pending_admissions
                            .retain(|row| !created_ids.contains(&row.job_id));
                        owner
                            .state
                            .url_admission_receipts
                            .retain(|row| !created_ids.contains(&JobId(row.job_id)));
                        owner.dirty = true;
                    }
                    for id in &created_ids {
                        owner.pending_control_commits.remove(id);
                        owner.record_transitions.remove(id);
                    }
                    owner.publish_now();
                    owner.bump_epoch();
                    owner.drain_replacements();
                    let _ = reply.send(if ok {
                        result
                    } else {
                        Err(crate::EngineError::AdmissionNotDurable)
                    });
                });
            }
            QueueCommand::ConfirmManagedTorrentSource {
                job,
                submission_sha256,
                reply,
            } => {
                let before = self.state.job(job).cloned();
                let eligible = !self.persistence_fenced
                    && self.persist
                    && self
                        .state
                        .pending_admissions
                        .iter()
                        .any(|pending| pending.job_id == job)
                    && before.as_ref().is_some_and(|row| {
                        crate::torrent_acquisitions::is_acquisition(row)
                            && row.params.iter().any(|(key, value)| {
                                key == crate::torrent_acquisitions::SUBMISSION_PARAM
                                    && value == &submission_sha256
                            })
                    });
                if eligible {
                    let row = self.state.job_mut(job).unwrap();
                    row.params
                        .retain(|(key, _)| key != crate::torrent_acquisitions::SOURCE_READY_PARAM);
                    row.params.push((
                        crate::torrent_acquisitions::SOURCE_READY_PARAM.into(),
                        "1".into(),
                    ));
                }
                if !eligible {
                    let _ = reply.send(false);
                    return None;
                }
                self.native_commit_then(before.into_iter().collect(), move |_, ok| {
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::CommitManagedTorrentAdmission {
                job,
                commit,
                submission_sha256,
                descriptor,
                reply,
            } => {
                if self.persistence_fenced || !self.persist {
                    let _ = reply.send(Err(crate::EngineError::AdmissionNotDurable));
                    return None;
                }
                let length = self.state.jobs.len();
                let previous_child = self.state.job(job).cloned();
                let before_pending = self.state.pending_admissions.clone();
                let before_receipts = self.state.url_admission_receipts.clone();
                let result = self.state.commit_managed_torrent_admission(
                    job,
                    *commit,
                    submission_sha256,
                    descriptor,
                    &self.artifacts.installation,
                );
                if result.is_ok() {
                    for row in &mut self.state.jobs[length..] {
                        mark_new_lifecycle_cohort(row);
                    }
                }
                if result.is_err() {
                    self.state.jobs.truncate(length);
                    if let Some(previous) = previous_child {
                        if let Some(current) = self.state.job_mut(job) {
                            *current = previous;
                        }
                    }
                    self.state.pending_admissions = before_pending;
                    self.state.url_admission_receipts = before_receipts;
                    let _ = reply.send(result);
                    return None;
                }
                let new_ids = self.state.jobs[length..]
                    .iter()
                    .map(|row| row.id)
                    .collect::<Vec<_>>();
                for id in &new_ids {
                    self.pending_control_commits.insert(*id);
                    self.record_transitions.insert(*id);
                }
                self.native_commit_then(previous_child.into_iter().collect(), move |owner, ok| {
                    if !ok {
                        owner.state.jobs.retain(|row| !new_ids.contains(&row.id));
                        owner
                            .state
                            .pending_admissions
                            .retain(|row| row.job_id != job);
                        owner
                            .state
                            .pending_admissions
                            .extend(before_pending.into_iter().filter(|row| row.job_id == job));
                        owner.state.url_admission_receipts.retain(|row| {
                            row.job_id != job.0 && !new_ids.contains(&JobId(row.job_id))
                        });
                        owner.state.url_admission_receipts.extend(
                            before_receipts
                                .into_iter()
                                .filter(|row| row.job_id == job.0),
                        );
                        owner.dirty = true;
                        owner.save_snapshot();
                    } else {
                        let _ = owner.pending_sources.remove(job);
                    }
                    for id in &new_ids {
                        owner.pending_control_commits.remove(id);
                        owner.record_transitions.remove(id);
                    }
                    owner.publish_now();
                    owner.bump_epoch();
                    owner.drain_replacements();
                    let _ = reply.send(if ok {
                        result
                    } else {
                        Err(crate::EngineError::AdmissionNotDurable)
                    });
                });
            }
            QueueCommand::StartTorrentAcquisition { job, reply } => {
                let before = self.state.job(job).cloned();
                let ok = !self.persistence_fenced
                    && self.persist
                    && before.as_ref().is_some_and(|row| {
                        crate::torrent_acquisitions::donor(row).is_some()
                            && !row.held()
                            && matches!(row.status, JobStatus::Queued | JobStatus::Downloading)
                    });
                if ok {
                    self.state.job_mut(job).unwrap().status = JobStatus::Downloading;
                }
                if !ok {
                    let _ = reply.send(false);
                    return None;
                }
                self.native_commit_then(before.into_iter().collect(), move |_, ok| {
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::CompleteTorrentAcquisition {
                job,
                donor,
                source,
                reply,
            } => {
                let before = self.state.job(job).cloned();
                let eligible = !self.persistence_fenced
                    && self.persist
                    && before.as_ref().is_some_and(|row| {
                        crate::torrent_acquisitions::donor(row) == Some(donor)
                            && !row.held()
                            && matches!(row.status, JobStatus::Queued | JobStatus::Downloading)
                            && !row
                                .params
                                .iter()
                                .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
                    })
                    && self.artifacts.for_job(job.0).is_ok_and(|current| {
                        current.is_some_and(|current| {
                            current.id == source.id
                                && current.generation == source.generation
                                && current.path == source.path
                                && current.owned
                                && current.state == "active"
                        })
                    })
                    && self
                        .artifacts
                        .torrent_copy_ready(
                            job.0,
                            &nzbd_state::artifacts::SourceGeneration {
                                artifact: source.id.clone(),
                                generation: source.generation.clone(),
                            },
                        )
                        .unwrap_or(false);
                if !eligible {
                    let _ = reply.send(false);
                    return None;
                }
                let count = source
                    .files
                    .iter()
                    .filter(|entry| !entry.identity.directory)
                    .count();
                if u32::try_from(count)
                    .ok()
                    .and_then(|count| self.state.next_file_id.checked_add(count))
                    .is_none()
                {
                    let _ = reply.send(false);
                    return None;
                }
                let timing_checkpoint = self.timing_cache.checkpoint(job);
                let mut file_params = Vec::new();
                let mut files = Vec::new();
                let mut file_sizes = Vec::new();
                for entry in source
                    .files
                    .iter()
                    .filter(|entry| !entry.identity.directory)
                {
                    self.state.next_file_id += 1;
                    let id = FileId(self.state.next_file_id);
                    file_sizes.push((id, entry.identity.bytes));
                    file_params.push((
                        format!("*File:size:{}", id.0),
                        entry.identity.bytes.to_string(),
                    ));
                    files.push(nzbd_types::FileEntry {
                        id,
                        subject: entry.path.clone(),
                        filename: entry.path.clone(),
                        filename_confirmed: true,
                        is_par2: entry.path.to_ascii_lowercase().ends_with(".par2"),
                        paused: false,
                        groups: Vec::new(),
                        date: None,
                        segments: Vec::new(),
                        crc32: None,
                        finalized: true,
                    });
                }
                let row = self.state.job_mut(job).unwrap();
                row.params.extend(file_params);
                set_owner_param(
                    &mut row.params,
                    crate::torrent_acquisitions::COPY_COMPLETE_PARAM,
                    Some("1".into()),
                );
                row.files = files;
                row.totals.success_size = source.summary().1;
                let donor_failed = self
                    .state
                    .job(donor)
                    .and_then(|row| row.torrent.as_ref())
                    .is_some_and(|record| record.phase == TorrentPhase::Failed);
                let row = self.state.job_mut(job).unwrap();
                row.status = if donor_failed {
                    JobStatus::Failed
                } else {
                    JobStatus::Completed
                };
                if donor_failed {
                    row.totals.failed_size =
                        row.totals.size.saturating_sub(row.totals.success_size);
                    set_owner_param(
                        &mut row.params,
                        "Failure:Stage",
                        Some("torrent_download".into()),
                    );
                    set_owner_param(&mut row.params,"Failure:Reason",Some("Torrent failed; hash-verified files were preserved for post-processing".into()));
                }
                self.dirty = true;
                self.native_commit_then(before.into_iter().collect(), move |owner, ok| {
                    if !ok {
                        owner.timing_cache.restore(job, timing_checkpoint);
                        let _ = reply.send(false);
                        return;
                    }
                    owner.file_sizes.extend(file_sizes);
                    let _ = owner.sync_managed_seed_keep(donor);
                    owner.publish_now();
                    owner.bump_epoch();
                    let row = owner.state.job(job).unwrap();
                    owner.emit(Event::JobFinished {
                        job,
                        status: row.status,
                        name: row.name.clone(),
                        health: if row.totals.size == 0 {
                            0
                        } else {
                            ((row.totals.success_size as u128 * 1000) / row.totals.size as u128)
                                .min(1000) as u16
                        },
                    });
                    let _ = reply.send(true);
                });
            }
            QueueCommand::HoldTorrentAcquisition {
                job,
                cause,
                message,
                reply,
            } => {
                let eligible = self.state.job(job).is_some_and(|row| {
                    crate::torrent_acquisitions::donor(row).is_some()
                        && matches!(row.status, JobStatus::Queued | JobStatus::Downloading)
                });
                let before = self.state.job(job).cloned();
                if !eligible || !self.hold_job(job, cause, "torrent_copy", &message) {
                    let _ = reply.send(false);
                    return None;
                }
                self.native_commit_then(before.into_iter().collect(), move |_, ok| {
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::Pause { job, reply }
                if self
                    .state
                    .job(job)
                    .is_some_and(crate::torrent_acquisitions::is_acquisition) =>
            {
                self.set_native_acquisition_paused_then(job, true, move |_, ok| {
                    let _ = reply.send(ok);
                });
            }
            QueueCommand::Resume { job, reply }
                if self
                    .state
                    .job(job)
                    .is_some_and(crate::torrent_acquisitions::is_acquisition) =>
            {
                self.set_native_acquisition_paused_then(job, false, move |_, ok| {
                    let _ = reply.send(ok);
                });
            }
            command => return Some(command),
        }
        None
    }
}
