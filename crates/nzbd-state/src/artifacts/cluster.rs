//! Exact local-copy retirement after replicated cluster publication acceptance.
use super::*;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ClusterArtifactCopy {
    pub storage_instance_id: String,
    pub publication: SourceGeneration,
    pub source: SourceGeneration,
    pub job_id: u32,
    pub selected: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ClusterCopyCleanup {
    pub status: String,
    pub error: Option<String>,
}

impl Inventory {
    /// Grant provenance is separate from cleanup eligibility. Legacy/new
    /// nonconsumer jobs keep their existing retention policy while a worker
    /// can persist an exact unowned shadow retry under this owner generation.
    pub fn cluster_download_authority_source(
        &self,
        job: u32,
    ) -> Result<Option<ClusterArtifactCopy>> {
        let Some(artifact) = self.for_job(job)? else {
            return Ok(None);
        };
        if !artifact.owned || artifact.terminal() {
            return Ok(None);
        }
        let source = SourceGeneration {
            artifact: artifact.id,
            generation: artifact.generation,
        };
        Ok(Some(ClusterArtifactCopy {
            storage_instance_id: self.installation.clone(),
            publication: source.clone(),
            source,
            job_id: job,
            selected: false,
        }))
    }

    /// Capture existing custody only. A path supplied by a peer cannot create
    /// inventory ownership or upgrade legacy work into automatic retirement.
    pub fn cluster_artifact_copy(
        &self,
        job: u32,
        selected: bool,
    ) -> Result<Option<ClusterArtifactCopy>> {
        let Some(artifact) = self.for_job(job)? else {
            return Ok(None);
        };
        self.cluster_artifact_copy_for_generation(
            job,
            selected,
            &SourceGeneration {
                artifact: artifact.id,
                generation: artifact.generation,
            },
        )
    }

    pub fn cluster_artifact_copy_for_generation(
        &self,
        job: u32,
        selected: bool,
        source: &SourceGeneration,
    ) -> Result<Option<ClusterArtifactCopy>> {
        let artifact = self.get(&source.artifact)?;
        if artifact.generation != source.generation || artifact.job != Some(job) {
            return Err(Error::Conflict(
                "cluster copy belongs to another job generation".into(),
            ));
        }
        if !artifact.owned || !artifact.automatic_cleanup_eligible || !artifact.delivery_lifecycle {
            return Ok(None);
        }
        let (source_id, source_generation) = if selected {
            self.delivery_source_ref(
                &artifact.id,
                &artifact.generation,
                &artifact.path.to_string_lossy(),
            )?
        } else {
            (artifact.id.clone(), artifact.generation.clone())
        };
        Ok(Some(ClusterArtifactCopy {
            storage_instance_id: self.installation.clone(),
            publication: SourceGeneration {
                artifact: artifact.id,
                generation: artifact.generation,
            },
            source: SourceGeneration {
                artifact: source_id,
                generation: source_generation,
            },
            job_id: job,
            selected,
        }))
    }

    pub(super) fn cluster_retirement_accepted(
        &self,
        artifact: &str,
        generation: &str,
    ) -> Result<bool> {
        Ok(self.db.lock().unwrap().query_row(
            "SELECT EXISTS(SELECT 1 FROM workspace_finalizations WHERE source=?1 AND generation=?2 AND activated=1 AND json_extract(proof,'$.kind')='cluster')",
            params![artifact, generation], |row| row.get(0))?)
    }
    pub(super) fn cluster_retirement_success(
        &self,
        artifact: &str,
        generation: &str,
    ) -> Result<bool> {
        Ok(self.db.lock().unwrap().query_row(
            "SELECT EXISTS(SELECT 1 FROM workspace_finalizations WHERE source=?1 AND generation=?2 AND activated=1 AND outcome='SUCCESS' AND json_extract(proof,'$.kind')='cluster')",
            params![artifact, generation], |row| row.get(0))?)
    }

    pub(super) fn cluster_cancellation_delete_binding(
        &self,
        artifact: &Artifact,
    ) -> Result<String> {
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(
                &artifact.origin_path,
                &artifact.root,
                &artifact.root_identity,
                &artifact.path,
                &artifact.identity,
                artifact.job,
                &artifact.files
            ))?)
        ))
    }
    pub(super) fn cluster_cancellation_delete_accepted(
        &self,
        artifact: &str,
        generation: &str,
        receipt: &str,
    ) -> Result<bool> {
        Ok(!receipt.is_empty() && self.db.lock().unwrap().query_row("SELECT EXISTS(SELECT 1 FROM workspace_finalizations WHERE source=?1 AND generation=?2 AND activated=1 AND outcome='CANCELLED' AND json_extract(proof,'$.kind')='cluster' AND json_extract(proof,'$.receipt')=?3 AND EXISTS(SELECT 1 FROM artifacts a WHERE a.id=?1 AND a.state<>'transferred') AND NOT EXISTS(SELECT 1 FROM deliveries d WHERE d.artifact=?1 AND d.generation=?2))",params![artifact,generation,receipt],|row|row.get(0))?)
    }

    /// The authenticated peer handler proves exact DeleteFiles cancellation
    /// and source-owner quiescence before entering. Keep is overridden only
    /// for this raw generation, without changing its mutable Keep preference.
    /// Accepted consumer publication, active writers and recovery holds remain
    /// separate custody and are never promoted by cancellation.
    pub fn delete_cancelled_cluster_artifact_copy(
        &self,
        copy: &ClusterArtifactCopy,
        proof: FinalizationProof,
        raw_files: &[DeliveryFile],
    ) -> Result<ClusterCopyCleanup> {
        if copy.selected || copy.publication != copy.source {
            return Err(Error::Conflict("selected publication needs separate exact custody review before cancellation deletion".into()));
        }
        let accepted: bool = self.db.lock().unwrap().query_row(
            "SELECT EXISTS(SELECT 1 FROM deliveries WHERE artifact=?1 AND generation=?2)",
            params![copy.publication.artifact, copy.publication.generation],
            |row| row.get(0),
        )?;
        if accepted || self.get(&copy.publication.artifact)?.state == "transferred" {
            return Err(Error::Conflict(
                "consumer custody cannot be replaced by cancellation deletion".into(),
            ));
        }
        self.retire_cluster_artifact_copy_policy(
            copy,
            "CANCELLED",
            proof,
            true,
            Some(raw_files),
            true,
            None,
        )
    }

    /// DeleteFiles may dispose this exact selected generation only before any
    /// local Delivery/handoff exists. Its predecessor is a separate obligation:
    /// normal selected retirement would otherwise retire that source too.
    pub fn delete_cancelled_cluster_unpublished_publication(
        &self,
        copy: &ClusterArtifactCopy,
        proof: FinalizationProof,
        files: &[DeliveryFile],
    ) -> Result<ClusterCopyCleanup> {
        if !copy.selected {
            return Err(Error::Conflict(
                "unpublished cancellation requires an exact selected generation".into(),
            ));
        }
        let private = ClusterArtifactCopy {
            storage_instance_id: copy.storage_instance_id.clone(),
            publication: copy.publication.clone(),
            source: copy.publication.clone(),
            job_id: copy.job_id,
            selected: false,
        };
        self.delete_cancelled_cluster_artifact_copy(&private, proof, files)
    }

    /// The peer handler first proves this exact copy is a replicated cleanup
    /// obligation of a READY delivery. Local storage identity, generation,
    /// current Keep/holds and the existing relocation journal remain decisive.
    pub fn retire_cluster_artifact_copy(
        &self,
        copy: &ClusterArtifactCopy,
        outcome: &str,
        proof: FinalizationProof,
    ) -> Result<ClusterCopyCleanup> {
        self.retire_cluster_artifact_copy_with_disposition(copy, outcome, proof, false, None)
    }
    pub fn retire_cluster_artifact_copy_with_disposition(
        &self,
        copy: &ClusterArtifactCopy,
        outcome: &str,
        proof: FinalizationProof,
        unusable: bool,
        raw_files: Option<&[DeliveryFile]>,
    ) -> Result<ClusterCopyCleanup> {
        self.retire_cluster_artifact_copy_policy(
            copy, outcome, proof, unusable, raw_files, false, None,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub fn retire_cluster_artifact_copy_with_result(
        &self,
        copy: &ClusterArtifactCopy,
        outcome: &str,
        proof: FinalizationProof,
        unusable: bool,
        raw_files: Option<&[DeliveryFile]>,
        result: &str,
    ) -> Result<ClusterCopyCleanup> {
        self.retire_cluster_artifact_copy_policy(
            copy,
            outcome,
            proof,
            unusable,
            raw_files,
            false,
            Some(result),
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn retire_cluster_artifact_copy_policy(
        &self,
        copy: &ClusterArtifactCopy,
        outcome: &str,
        proof: FinalizationProof,
        unusable: bool,
        raw_files: Option<&[DeliveryFile]>,
        explicit_cancellation: bool,
        result: Option<&str>,
    ) -> Result<ClusterCopyCleanup> {
        if copy.storage_instance_id != self.installation
            || !matches!(proof, FinalizationProof::Cluster { .. })
        {
            return Err(Error::Conflict(
                "cluster cleanup belongs to another storage instance".into(),
            ));
        }
        let mut artifact = self.get(&copy.publication.artifact)?;
        let predecessor = if artifact.job.is_none() && outcome == "SUCCESS" && !copy.selected {
            match result {
                Some(result) => self.cluster_materialized_predecessor(copy, result)?,
                None => false,
            }
        } else {
            false
        };
        if predecessor {
            artifact = self.get(&copy.publication.artifact)?;
        }
        if artifact.generation != copy.publication.generation
            || (artifact.job != Some(copy.job_id) && !predecessor)
            || !artifact.owned
            || !artifact.automatic_cleanup_eligible
            || (!artifact.delivery_lifecycle && !explicit_cancellation)
        {
            return Err(Error::Conflict(
                "cluster cleanup copy generation or admission changed".into(),
            ));
        }
        let key = format!(
            "{}{:x}",
            if explicit_cancellation {
                "cluster-cancel-copy-"
            } else {
                "cluster-copy-"
            },
            Sha256::digest(serde_json::to_vec(&(copy, &proof))?)
        );
        let existing = match self.operation(&key) {
            Ok(operation) => Some(operation),
            Err(Error::NotFound) => None,
            Err(error) => return Err(error),
        };
        if !artifact.terminal()
            && ((artifact.keep && !explicit_cancellation)
                || artifact.hold.is_some()
                || (!explicit_cancellation
                    && !copy.selected
                    && outcome != "SUCCESS"
                    && artifact.keep_on_failure))
        {
            return Ok(ClusterCopyCleanup {
                status: "retained".into(),
                error: Some(artifact.hold.clone().unwrap_or_else(|| {
                    "this exact source is retained by its current Keep setting".into()
                })),
            });
        }
        if let Some(operation) = existing.as_ref().filter(|_| !copy.selected) {
            let request: DeleteRequest = serde_json::from_str(&operation.request)?;
            if operation.kind != "delete"
                || operation.artifact != artifact.id
                || request.fields().0 != artifact.id
                || request.fields().4 != Some(copy.publication.generation.as_str())
            {
                return Err(Error::Conflict(
                    "cluster deletion journal identity changed".into(),
                ));
            }
            let operation = self.execute_delete(&key)?;
            return Ok(ClusterCopyCleanup {
                status: match operation.state.as_str() {
                    "succeeded" => "removed",
                    "cancelled" | "review" => "retained",
                    _ => "retry",
                }
                .into(),
                error: operation.error,
            });
        }
        if !copy.selected && outcome != "SUCCESS" && !unusable && !artifact.terminal() {
            return Ok(ClusterCopyCleanup { status: "retained".into(), error: Some(
                "partial failure publication does not prove every raw source dependency redundant; retain this exact source until recovery is resolved".into()) });
        }
        // A delegated download writes into the admission owner's allocation.
        // READY is reached only after that writer's durable completion, so seal
        // its final manifest now; imported/legacy paths never reach this branch.
        let mut raw_observed = None;
        if !copy.selected && !artifact.terminal() {
            if let Some(expected) = raw_files {
                if expected.len() > 100_000 {
                    return Err(Error::Conflict(
                        "raw byte seal exceeds100000 members; retained for review".into(),
                    ));
                }
                let mut by_path = std::collections::HashMap::with_capacity(expected.len());
                let mut directories = std::collections::HashSet::new();
                for file in expected {
                    if by_path.insert(file.path.as_str(), file).is_some() {
                        return Err(Error::Conflict(
                            "raw byte seal has duplicate paths; retained for review".into(),
                        ));
                    }
                    let mut parent = std::path::Path::new(&file.path).parent();
                    while let Some(path) = parent.filter(|path| !path.as_os_str().is_empty()) {
                        directories.insert(path.to_string_lossy().into_owned());
                        parent = path.parent();
                    }
                }
                let directory = self.verify(&artifact)?;
                let observed = fs::manifest(&directory, 100_000)?;
                for entry in &observed {
                    if entry.identity.directory {
                        if !directories.contains(&entry.path) {
                            return Ok(ClusterCopyCleanup {
                                status: "retained".into(),
                                error: Some(format!(
                                    "review required: raw directory {} was not in the accepted transfer seal",
                                    entry.path
                                )),
                            });
                        }
                        continue;
                    }
                    let Some(file) = by_path.get(entry.path.as_str()) else {
                        return Ok(ClusterCopyCleanup {
                            status: "retained".into(),
                            error: Some(format!(
                                "review required: raw file {} was not in the accepted transfer seal",
                                entry.path
                            )),
                        });
                    };
                    let mut input = fs::open_relative(&directory, &entry.path)?;
                    use std::io::Read;
                    let mut hash = Sha256::new();
                    let mut buffer = [0u8; 65536];
                    loop {
                        let count = input.read(&mut buffer)?;
                        if count == 0 {
                            break;
                        }
                        hash.update(&buffer[..count]);
                    }
                    if file.size_bytes != entry.identity.bytes
                        || format!("{:x}", hash.finalize()) != file.sha256
                        || fs::identity_for_file(&input)? != entry.identity
                    {
                        return Ok(ClusterCopyCleanup {
                            status: "retained".into(),
                            error: Some(format!(
                                "review required: raw file {} differs from its accepted transfer seal",
                                entry.path
                            )),
                        });
                    }
                }
                if observed
                    .iter()
                    .filter(|entry| !entry.identity.directory)
                    .count()
                    != expected.len()
                {
                    return Ok(ClusterCopyCleanup{status:"retained".into(),error:Some("review required: raw copy no longer matches every accepted transfer member".into())});
                }
                raw_observed = Some(observed);
            } else if artifact.files.is_empty() {
                return Ok(ClusterCopyCleanup {
                    status: "retained".into(),
                    error: Some(
                        "review required: delegated raw allocation lacks its accepted byte seal"
                            .into(),
                    ),
                });
            }
            if raw_files.is_some() && raw_observed.is_none() {
                return Err(Error::Conflict(
                    "raw retirement proof was not captured".into(),
                ));
            }
        }
        if !copy.selected && artifact.state == "active" {
            if let Some(observed) = raw_observed {
                let finished =
                    self.finish(copy.job_id, &artifact.path, &artifact.root, "completed")?;
                if finished.files != observed {
                    return Ok(ClusterCopyCleanup {
                        status: "retained".into(),
                        error: Some(
                            "review required: raw manifest changed while sealing retirement".into(),
                        ),
                    });
                }
            }
        }
        self.accept_cluster_completion(&copy.publication, outcome, proof.clone())?;
        let retained = if copy.selected {
            match self.retire_source_for_delivery(
                &artifact.id,
                &artifact.generation,
                &artifact.path,
            ) {
                Ok(true) => None,
                Ok(false) => Some("source retirement remains pending".to_string()),
                Err(Error::Conflict(reason))
                    if reason.starts_with("blocked:")
                        || reason.starts_with(
                            "source retirement is blocked by ownership/hold state",
                        ) =>
                {
                    Some(reason)
                }
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        if !artifact.terminal() && existing.is_none() {
            let revision = {
                let _guard = self.mutation_guard()?;
                let mut current = self.get(&artifact.id)?;
                if current.generation != copy.publication.generation
                    || !current.owned
                    || (current.keep && !explicit_cancellation)
                    || current.hold.is_some()
                    || self.source_in_use(&copy.publication)
                    || (!explicit_cancellation
                        && !copy.selected
                        && outcome != "SUCCESS"
                        && current.keep_on_failure)
                {
                    return Err(Error::Conflict(
                        "cluster copy retained by current ownership, Keep or active use".into(),
                    ));
                }
                if current.state == "active" {
                    current.state = "completed".into();
                    current.revision += 1;
                    current.updated_at = now();
                    self.sidecar(&current)?;
                    save_artifact(&self.db.lock().unwrap(), &current)?;
                }
                current.revision
            };
            let cancellation_receipt = if explicit_cancellation {
                match &proof {
                    FinalizationProof::Cluster { receipt, .. } => Some(receipt.as_str()),
                    _ => {
                        return Err(Error::Conflict(
                            "explicit cancellation requires a cluster receipt".into(),
                        ));
                    }
                }
            } else {
                None
            };
            self.request_delete_with_policy(
                &artifact.id,
                revision,
                &key,
                0,
                false,
                None,
                !explicit_cancellation && !copy.selected && outcome != "SUCCESS",
                cancellation_receipt,
            )?;
        }
        if !self.get(&artifact.id)?.terminal() {
            self.execute_delete(&key)?;
            let operation = self.operation(&key)?;
            if operation.state != "succeeded" {
                return Ok(ClusterCopyCleanup {
                    status: "retry".into(),
                    error: operation.error,
                });
            }
        }
        Ok(ClusterCopyCleanup {
            status: if retained.is_some() {
                "retained"
            } else {
                "removed"
            }
            .into(),
            error: retained,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (
        tempfile::TempDir,
        Inventory,
        std::path::PathBuf,
        ClusterArtifactCopy,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        let root = temp.path().join("processing");
        std::fs::create_dir(&root).unwrap();
        let source = root.join("job");
        inventory
            .allocate_for_delivery(7, &root, &source, true, true)
            .unwrap();
        std::fs::write(source.join("episode.mkv"), b"verified payload").unwrap();
        std::fs::write(source.join("readme.txt"), b"junk").unwrap();
        inventory.finish(7, &source, &root, "active").unwrap();
        let copy = inventory.cluster_artifact_copy(7, false).unwrap().unwrap();
        (temp, inventory, source, copy)
    }
    fn proof() -> FinalizationProof {
        FinalizationProof::Cluster {
            receipt: "exact-replicated-ready".into(),
            accepted_at_ms: 1,
        }
    }
    fn raw_seal() -> Vec<DeliveryFile> {
        [
            ("episode.mkv", b"verified payload".as_slice()),
            ("readme.txt", b"junk".as_slice()),
        ]
        .into_iter()
        .map(|(path, bytes)| DeliveryFile {
            path: path.into(),
            size_bytes: bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(bytes)),
        })
        .collect()
    }
    #[test]
    fn explicit_cancel_overrides_only_selected_keep_and_replays_partial_delete_after_restart() {
        let (temp, mut inventory, source, copy) = fixture();
        let artifact = inventory.get(&copy.publication.artifact).unwrap();
        inventory
            .retention(&artifact.id, artifact.revision, true, None)
            .unwrap();
        inventory.set_keep_on_failure(7, true).unwrap();
        let unrelated = source.parent().unwrap().join("unrelated-kept");
        let other = inventory
            .allocate_for_delivery(8, source.parent().unwrap(), &unrelated, true, true)
            .unwrap();
        std::fs::write(unrelated.join("keep.bin"), b"unrelated kept bytes").unwrap();
        inventory
            .finish(8, &unrelated, source.parent().unwrap(), "completed")
            .unwrap();
        let other = inventory.get(&other.id).unwrap();
        inventory
            .retention(&other.id, other.revision, true, None)
            .unwrap();
        inventory.set_keep_on_failure(8, true).unwrap();
        DELETE_FAIL_AFTER.with(|counter| counter.set(Some(1)));
        let result = inventory
            .delete_cancelled_cluster_artifact_copy(&copy, proof(), &raw_seal())
            .unwrap();
        assert_eq!(result.status, "retry");
        assert_eq!(std::fs::read_dir(&source).unwrap().count(), 1);
        let key = format!(
            "cluster-cancel-copy-{:x}",
            Sha256::digest(serde_json::to_vec(&(&copy, &proof())).unwrap())
        );
        let mut operation = inventory.operation(&key).unwrap();
        let request: DeleteRequest = serde_json::from_str(&operation.request).unwrap();
        assert!(
            matches!(request,DeleteRequest::ClusterCancellation((_,_,_,false,_,false,ref receipt,_)) if receipt=="exact-replicated-ready")
        );
        assert!(inventory.get(&copy.publication.artifact).unwrap().keep);
        operation.next_retry = 0;
        save_operation(&inventory.db.lock().unwrap(), &operation).unwrap();
        drop(inventory);
        inventory = Inventory::open(&temp.path().join("state")).unwrap();
        inventory.reconcile_startup(&[]).unwrap();
        assert_eq!(inventory.execute_delete(&key).unwrap().state, "succeeded");
        assert!(!source.exists());
        assert_eq!(
            std::fs::read(unrelated.join("keep.bin")).unwrap(),
            b"unrelated kept bytes"
        );
        let other = inventory.for_job(8).unwrap().unwrap();
        assert!(other.keep && other.keep_on_failure);
    }
    #[test]
    fn queued_explicit_cancel_survives_keep_revision_change_before_first_unlink_and_restart() {
        let (temp, mut inventory, source, copy) = fixture();
        inventory
            .finish(7, &source, source.parent().unwrap(), "completed")
            .unwrap();
        inventory
            .accept_cluster_completion(&copy.publication, "CANCELLED", proof())
            .unwrap();
        let artifact = inventory.get(&copy.publication.artifact).unwrap();
        let op = inventory
            .request_delete_with_policy(
                &artifact.id,
                artifact.revision,
                "cluster-cancel-copy-queued-keep-change",
                0,
                false,
                None,
                false,
                Some("exact-replicated-ready"),
            )
            .unwrap();
        inventory
            .retention(&artifact.id, artifact.revision, true, None)
            .unwrap();
        inventory.set_keep_on_failure(7, true).unwrap();
        assert_eq!(inventory.operation(&op.id).unwrap().state, "queued");
        assert_ne!(
            inventory.get(&artifact.id).unwrap().revision,
            artifact.revision
        );
        drop(inventory);
        inventory = Inventory::open(&temp.path().join("state")).unwrap();
        inventory.reconcile_startup(&[]).unwrap();
        let current = inventory.get(&artifact.id).unwrap();
        assert!(current.keep && current.keep_on_failure);
        assert_eq!(inventory.execute_delete(&op.id).unwrap().state, "succeeded");
        assert!(!source.exists());
    }
    #[test]
    fn queued_explicit_cancel_rejects_changed_owned_manifest_even_when_keep_changed() {
        let (_temp, inventory, source, copy) = fixture();
        inventory
            .finish(7, &source, source.parent().unwrap(), "completed")
            .unwrap();
        inventory
            .accept_cluster_completion(&copy.publication, "CANCELLED", proof())
            .unwrap();
        let artifact = inventory.get(&copy.publication.artifact).unwrap();
        let op = inventory
            .request_delete_with_policy(
                &artifact.id,
                artifact.revision,
                "cluster-cancel-copy-manifest-change",
                0,
                false,
                None,
                false,
                Some("exact-replicated-ready"),
            )
            .unwrap();
        let mut changed = artifact.clone();
        changed.files[0].digest = Some("changed receipt".into());
        changed.revision += 1;
        save_artifact(&inventory.db.lock().unwrap(), &changed).unwrap();
        assert_eq!(inventory.execute_delete(&op.id).unwrap().state, "review");
        assert!(source.exists());
    }
    #[test]
    fn explicit_cancel_still_refuses_active_use_recovery_hold_and_unknown_members() {
        for block in ["active", "recovery", "unknown"] {
            let (_temp, inventory, source, copy) = fixture();
            let inventory = std::sync::Arc::new(inventory);
            let guard = if block == "active" {
                inventory.acquire_terminal_use(7, &|| Ok(())).unwrap()
            } else {
                None
            };
            if block == "recovery" {
                inventory
                    .finish(7, &source, source.parent().unwrap(), "completed")
                    .unwrap();
                let artifact = inventory.get(&copy.publication.artifact).unwrap();
                let root = source.parent().unwrap().join("recovery-copies");
                std::fs::create_dir(&root).unwrap();
                let recovery = inventory
                    .stage_recovery(
                        &artifact.id,
                        artifact.revision,
                        "shared-dependent-recovery",
                        &["episode.mkv".into()],
                        &root,
                    )
                    .unwrap();
                assert!(inventory
                    .get(&artifact.id)
                    .unwrap()
                    .hold
                    .as_ref()
                    .is_some_and(|hold| hold.contains(&recovery.id)));
            }
            if block == "unknown" {
                std::fs::write(source.join("user-added.txt"), b"unowned bytes").unwrap();
            }
            let result =
                inventory.delete_cancelled_cluster_artifact_copy(&copy, proof(), &raw_seal());
            assert!(
                result.is_err() || result.unwrap().status != "removed",
                "bypassed {block}"
            );
            assert_eq!(
                std::fs::read(source.join("episode.mkv")).unwrap(),
                b"verified payload"
            );
            drop(guard);
        }
    }
    #[test]
    fn explicit_cancel_refuses_missing_receipt_on_replay_and_selected_consumer_custody() {
        let (_temp, inventory, source, copy) = fixture();
        let artifact = inventory.get(&copy.publication.artifact).unwrap();
        inventory
            .retention(&artifact.id, artifact.revision, true, None)
            .unwrap();
        DELETE_FAIL_AFTER.with(|counter| counter.set(Some(1)));
        assert_eq!(
            inventory
                .delete_cancelled_cluster_artifact_copy(&copy, proof(), &raw_seal())
                .unwrap()
                .status,
            "retry"
        );
        let key = format!(
            "cluster-cancel-copy-{:x}",
            Sha256::digest(serde_json::to_vec(&(&copy, &proof())).unwrap())
        );
        let mut operation = inventory.operation(&key).unwrap();
        operation.next_retry = 0;
        save_operation(&inventory.db.lock().unwrap(), &operation).unwrap();
        inventory
            .db
            .lock()
            .unwrap()
            .execute(
                "DELETE FROM workspace_finalizations WHERE source=?1",
                [&copy.publication.artifact],
            )
            .unwrap();
        assert_eq!(inventory.execute_delete(&key).unwrap().state, "review");
        assert!(source.exists());
        let mut selected = copy.clone();
        selected.selected = true;
        assert!(inventory
            .delete_cancelled_cluster_artifact_copy(&selected, proof(), &raw_seal())
            .is_err());
    }
    #[test]
    fn queued_explicit_cancel_rechecks_consumer_acceptance_before_first_unlink() {
        let (_temp, inventory, source, copy) = fixture();
        inventory
            .finish(7, &source, source.parent().unwrap(), "completed")
            .unwrap();
        inventory
            .accept_cluster_completion(&copy.publication, "CANCELLED", proof())
            .unwrap();
        let artifact = inventory.get(&copy.publication.artifact).unwrap();
        let op = inventory
            .request_delete_with_policy(
                &artifact.id,
                artifact.revision,
                "cluster-cancel-copy-late-consumer",
                0,
                false,
                None,
                false,
                Some("exact-replicated-ready"),
            )
            .unwrap();
        let receipt = inventory
            .create_delivery_with_outcome_and_keep(
                7,
                true,
                Some("consumer-transfer"),
                "SUCCESS",
                None,
                None,
                false,
            )
            .unwrap();
        inventory
            .accept_delivery(
                &receipt.delivery_id,
                &receipt.publication_generation,
                &receipt.manifest_digest,
                "durable-consumer-mapping",
            )
            .unwrap();
        assert_eq!(inventory.execute_delete(&op.id).unwrap().state, "review");
        assert!(source.exists());
        assert!(inventory
            .delete_cancelled_cluster_artifact_copy(&copy, proof(), &raw_seal())
            .is_err());
    }
    #[test]
    fn explicit_cancel_supports_new_nonconsumer_copies_but_not_legacy_or_unowned() {
        for cohort in ["new", "legacy", "unowned"] {
            let temp = tempfile::tempdir().unwrap();
            let inventory = Inventory::open(&temp.path().join("state")).unwrap();
            let root = temp.path().join("processing");
            std::fs::create_dir(&root).unwrap();
            let path = root.join("nonconsumer");
            inventory
                .allocate_for_delivery(9, &root, &path, cohort != "legacy", false)
                .unwrap();
            std::fs::write(path.join("raw.bin"), b"nonconsumer bytes").unwrap();
            inventory.finish(9, &path, &root, "completed").unwrap();
            let copy = inventory
                .cluster_download_authority_source(9)
                .unwrap()
                .unwrap();
            let artifact = inventory.get(&copy.publication.artifact).unwrap();
            inventory
                .retention(&artifact.id, artifact.revision, true, None)
                .unwrap();
            if cohort == "unowned" {
                let mut artifact = inventory.get(&copy.publication.artifact).unwrap();
                artifact.owned = false;
                save_artifact(&inventory.db.lock().unwrap(), &artifact).unwrap();
            }
            let files = vec![DeliveryFile {
                path: "raw.bin".into(),
                size_bytes: 17,
                sha256: format!("{:x}", Sha256::digest(b"nonconsumer bytes")),
            }];
            assert!(inventory
                .retire_cluster_artifact_copy_with_disposition(
                    &copy,
                    "SUCCESS",
                    proof(),
                    false,
                    Some(&files)
                )
                .is_err());
            let result = inventory.delete_cancelled_cluster_artifact_copy(&copy, proof(), &files);
            if cohort == "new" {
                assert_eq!(result.unwrap().status, "removed");
                assert!(!path.exists());
            } else {
                assert!(result.is_err());
                assert_eq!(
                    std::fs::read(path.join("raw.bin")).unwrap(),
                    b"nonconsumer bytes"
                );
            }
        }
    }
    fn unpublished_selected_fixture() -> (
        tempfile::TempDir,
        Inventory,
        std::path::PathBuf,
        std::path::PathBuf,
        ClusterArtifactCopy,
        Vec<DeliveryFile>,
    ) {
        let (temp, inventory, source, _) = fixture();
        let selected = inventory
            .for_job(7)
            .unwrap()
            .unwrap()
            .files
            .into_iter()
            .filter(|entry| entry.path == "episode.mkv")
            .collect();
        let completed = temp.path().join("completed");
        std::fs::create_dir(&completed).unwrap();
        let target = completed.join("private-selected");
        let outcomes = raw_seal()
            .iter()
            .map(|file| DeliveryFileOutcome {
                source_path: file.path.clone(),
                publication_path: (file.path == "episode.mkv").then(|| file.path.clone()),
                classification: "video".into(),
                outcome: if file.path == "episode.mkv" {
                    "published"
                } else {
                    "excluded_source_cleanup"
                }
                .into(),
                reason: None,
                repair_dependency: false,
                source_cleanup_status: "pending".into(),
            })
            .collect();
        inventory
            .publish_selected_for_delivery_checked(7, &target, selected, outcomes, &|| Ok(()))
            .unwrap();
        let copy = inventory.cluster_artifact_copy(7, true).unwrap().unwrap();
        let files = raw_seal()
            .into_iter()
            .filter(|file| file.path == "episode.mkv")
            .collect();
        (temp, inventory, source, target, copy, files)
    }
    #[test]
    fn explicit_cancel_unpublished_selection_removes_only_private_generation_and_replays() {
        let (temp, inventory, source, target, copy, files) = unpublished_selected_fixture();
        let original = inventory.get(&copy.source.artifact).unwrap();
        inventory
            .retention(&original.id, original.revision, true, None)
            .unwrap();
        let selected = inventory.get(&copy.publication.artifact).unwrap();
        inventory
            .retention(&selected.id, selected.revision, true, None)
            .unwrap();
        inventory.set_keep_on_failure(7, true).unwrap();
        assert_eq!(
            inventory
                .delete_cancelled_cluster_unpublished_publication(&copy, proof(), &files)
                .unwrap()
                .status,
            "removed"
        );
        assert!(!target.exists());
        assert_eq!(
            std::fs::read(source.join("episode.mkv")).unwrap(),
            b"verified payload"
        );
        assert_eq!(std::fs::read(source.join("readme.txt")).unwrap(), b"junk");
        assert!(inventory.get(&copy.source.artifact).unwrap().keep);
        drop(inventory);
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        assert_eq!(
            inventory
                .delete_cancelled_cluster_unpublished_publication(&copy, proof(), &files)
                .unwrap()
                .status,
            "removed"
        );
        assert!(source.join("episode.mkv").exists());
    }
    #[test]
    fn explicit_cancel_private_selection_refuses_any_durable_unaccepted_handoff() {
        let (_temp, inventory, source, target, copy, files) = unpublished_selected_fixture();
        inventory
            .finish(7, &target, target.parent().unwrap(), "completed")
            .unwrap();
        inventory
            .accept_cluster_completion(&copy.publication, "CANCELLED", proof())
            .unwrap();
        let artifact = inventory.get(&copy.publication.artifact).unwrap();
        let operation = inventory
            .request_delete_with_policy(
                &artifact.id,
                artifact.revision,
                "cluster-cancel-copy-private-before-handoff",
                0,
                false,
                None,
                false,
                Some("exact-replicated-ready"),
            )
            .unwrap();
        let delivery = inventory
            .create_delivery_with_outcome_and_keep(
                7,
                true,
                Some("private-consumer"),
                "SUCCESS",
                None,
                None,
                false,
            )
            .unwrap();
        assert!(delivery.accepted_at.is_none() && delivery.consumer_receipt_id.is_none());
        // A historical partially-applied state update cannot erase the durable
        // handoff. The executor must recheck the receipt, not only ownership.
        let mut stale_owned = inventory.get(&artifact.id).unwrap();
        stale_owned.owned = true;
        stale_owned.state = "completed".into();
        save_artifact(&inventory.db.lock().unwrap(), &stale_owned).unwrap();
        assert_eq!(
            inventory.execute_delete(&operation.id).unwrap().state,
            "review"
        );
        assert!(inventory
            .delete_cancelled_cluster_unpublished_publication(&copy, proof(), &files)
            .is_err());
        assert_eq!(
            std::fs::read(target.join("episode.mkv")).unwrap(),
            b"verified payload"
        );
        assert_eq!(
            std::fs::read(source.join("episode.mkv")).unwrap(),
            b"verified payload"
        );
    }
    #[test]
    fn accepted_selected_cluster_copy_drains_and_replays_without_payload() {
        let (temp, inventory, source, _) = fixture();
        let selected = inventory
            .for_job(7)
            .unwrap()
            .unwrap()
            .files
            .into_iter()
            .filter(|entry| entry.path == "episode.mkv")
            .collect();
        let completed = temp.path().join("completed");
        std::fs::create_dir(&completed).unwrap();
        let target = completed.join("private-payload");
        let outcomes = [
            ("episode.mkv", "published"),
            ("readme.txt", "excluded_source_cleanup"),
        ]
        .into_iter()
        .map(|(path, outcome)| DeliveryFileOutcome {
            source_path: path.into(),
            publication_path: (outcome == "published").then(|| path.into()),
            classification: "video".into(),
            outcome: outcome.into(),
            reason: None,
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        })
        .collect();
        inventory
            .publish_selected_for_delivery_checked(7, &target, selected, outcomes, &|| Ok(()))
            .unwrap();
        let copy = inventory.cluster_artifact_copy(7, true).unwrap().unwrap();
        let original = inventory.get(&copy.source.artifact).unwrap();
        assert_eq!(original.path, source);
        inventory
            .retention(&original.id, original.revision, true, None)
            .unwrap();
        assert_eq!(
            inventory
                .retire_cluster_artifact_copy(&copy, "SUCCESS", proof())
                .unwrap()
                .status,
            "retained"
        );
        assert!(source.exists());
        assert!(
            !target.exists(),
            "selected duplicate can drain while original source is blocked"
        );
        assert_eq!(
            inventory
                .retire_cluster_artifact_copy(&copy, "SUCCESS", proof())
                .unwrap()
                .status,
            "retained",
            "succeeded selected journal cannot hide blocked original source"
        );
        let original = inventory.get(&copy.source.artifact).unwrap();
        inventory
            .retention(&original.id, original.revision, false, None)
            .unwrap();
        assert_eq!(
            inventory
                .retire_cluster_artifact_copy(&copy, "SUCCESS", proof())
                .unwrap()
                .status,
            "removed"
        );
        assert!(!source.exists());
        assert!(!target.exists());
        assert_eq!(
            inventory
                .retire_cluster_artifact_copy(&copy, "SUCCESS", proof())
                .unwrap()
                .status,
            "removed"
        );
    }
    #[test]
    fn raw_copy_partial_unlink_replays_owned_journal_and_honors_current_keep() {
        for reopen in [false, true] {
            let (temp, mut inventory, source, copy) = fixture();
            let expected = [
                ("episode.mkv", b"verified payload".as_slice()),
                ("readme.txt", b"junk".as_slice()),
            ]
            .into_iter()
            .map(|(path, bytes)| DeliveryFile {
                path: path.into(),
                size_bytes: bytes.len() as u64,
                sha256: format!("{:x}", Sha256::digest(bytes)),
            })
            .collect::<Vec<_>>();
            DELETE_FAIL_AFTER.with(|counter| counter.set(Some(1)));
            let first = inventory
                .retire_cluster_artifact_copy_with_disposition(
                    &copy,
                    "FAILURE",
                    proof(),
                    true,
                    Some(&expected),
                )
                .unwrap();
            assert_eq!(first.status, "retry");
            let remaining = std::fs::read_dir(&source).unwrap().count();
            assert_eq!(remaining, 1, "fault must occur after one real unlink");
            let key = format!(
                "cluster-copy-{:x}",
                Sha256::digest(serde_json::to_vec(&(&copy, &proof())).unwrap())
            );
            let mut operation = inventory.operation(&key).unwrap();
            operation.next_retry = 0;
            save_operation(&inventory.db.lock().unwrap(), &operation).unwrap();
            assert!(inventory.set_keep_on_failure(7, true).unwrap());
            assert_eq!(inventory.execute_delete(&key).unwrap().state, "retry");
            assert_eq!(
                std::fs::read_dir(&source).unwrap().count(),
                remaining,
                "executor must recheck Keep after wrapper validation"
            );
            inventory.set_keep_on_failure(7, false).unwrap();
            let mut operation = inventory.operation(&key).unwrap();
            operation.next_retry = 0;
            save_operation(&inventory.db.lock().unwrap(), &operation).unwrap();
            if reopen {
                drop(inventory);
                inventory = Inventory::open(&temp.path().join("state")).unwrap();
            }
            let resumed = inventory
                .retire_cluster_artifact_copy_with_disposition(
                    &copy,
                    "FAILURE",
                    proof(),
                    true,
                    Some(&expected),
                )
                .unwrap();
            assert_eq!(resumed.status, "removed", "reopen={reopen}: {resumed:?}");
            assert!(!source.exists());
        }
        let (_temp, inventory, source, copy) = fixture();
        let expected = [
            ("episode.mkv", b"verified payload".as_slice()),
            ("readme.txt", b"junk".as_slice()),
        ]
        .into_iter()
        .map(|(path, bytes)| DeliveryFile {
            path: path.into(),
            size_bytes: bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(bytes)),
        })
        .collect::<Vec<_>>();
        DELETE_FAIL_AFTER.with(|counter| counter.set(Some(1)));
        assert_eq!(
            inventory
                .retire_cluster_artifact_copy_with_disposition(
                    &copy,
                    "SUCCESS",
                    proof(),
                    false,
                    Some(&expected)
                )
                .unwrap()
                .status,
            "retry"
        );
        let remaining = std::fs::read_dir(&source)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::write(&remaining, b"replacement bytes").unwrap();
        let key = format!(
            "cluster-copy-{:x}",
            Sha256::digest(serde_json::to_vec(&(&copy, &proof())).unwrap())
        );
        let mut operation = inventory.operation(&key).unwrap();
        operation.next_retry = 0;
        save_operation(&inventory.db.lock().unwrap(), &operation).unwrap();
        let resumed = inventory
            .retire_cluster_artifact_copy_with_disposition(
                &copy,
                "SUCCESS",
                proof(),
                false,
                Some(&expected),
            )
            .unwrap();
        assert_eq!(resumed.status, "retained");
        assert_eq!(std::fs::read(remaining).unwrap(), b"replacement bytes");
    }
    #[test]
    fn raw_copy_seal_refuses_added_or_changed_bytes_then_drains_exact_match() {
        let (_temp, inventory, source, copy) = fixture();
        let expected = [
            ("episode.mkv", b"verified payload".as_slice()),
            ("readme.txt", b"junk".as_slice()),
        ]
        .into_iter()
        .map(|(path, bytes)| DeliveryFile {
            path: path.into(),
            size_bytes: bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(bytes)),
        })
        .collect::<Vec<_>>();
        std::fs::write(source.join("manual.pdf"), b"recognizable user file").unwrap();
        assert_eq!(
            inventory
                .retire_cluster_artifact_copy_with_disposition(
                    &copy,
                    "SUCCESS",
                    proof(),
                    false,
                    Some(&expected)
                )
                .unwrap()
                .status,
            "retained"
        );
        assert!(source.join("manual.pdf").exists());
        std::fs::remove_file(source.join("manual.pdf")).unwrap();
        std::fs::write(source.join("episode.mkv"), b"changed payload!").unwrap();
        assert_eq!(
            inventory
                .retire_cluster_artifact_copy_with_disposition(
                    &copy,
                    "SUCCESS",
                    proof(),
                    false,
                    Some(&expected)
                )
                .unwrap()
                .status,
            "retained"
        );
        std::fs::write(source.join("episode.mkv"), b"verified payload").unwrap();
        assert_eq!(
            inventory
                .retire_cluster_artifact_copy_with_disposition(
                    &copy,
                    "SUCCESS",
                    proof(),
                    false,
                    Some(&expected)
                )
                .unwrap()
                .status,
            "removed"
        );
        assert!(!source.exists());
    }

    #[test]
    fn raw_partial_copy_and_changed_storage_never_gain_deletion_authority() {
        let (_temp, inventory, source, copy) = fixture();
        assert_eq!(
            inventory
                .retire_cluster_artifact_copy(&copy, "FAILURE/HEALTH", proof())
                .unwrap()
                .status,
            "retained"
        );
        assert!(source.join("episode.mkv").exists());
        let mut stale = copy.clone();
        stale.storage_instance_id.push_str("-replacement");
        assert!(inventory
            .retire_cluster_artifact_copy(&stale, "SUCCESS", proof())
            .is_err());
        assert!(source.exists());
        let mut stale = copy;
        stale.publication.generation.push_str("-stale");
        assert!(inventory
            .retire_cluster_artifact_copy(&stale, "SUCCESS", proof())
            .is_err());
        assert!(source.exists());
    }
}

/// Delete one newly-created sealed generation through directory descriptors.
/// The caller supplies its committed result identity and exact byte manifest;
/// unknown entries or a replacement directory remain untouched for review.
pub fn remove_cluster_generation_copy(
    root: &Path,
    relative: &Path,
    expected_identity: (u64, u64),
    files: &[DeliveryFile],
) -> Result<()> {
    if relative.as_os_str().is_empty()
        || relative.is_absolute()
        || relative
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err(Error::Conflict("unsafe cluster generation path".into()));
    }
    let anchor = fs::open_dir(root)?;
    let parent_relative = relative.parent().unwrap_or(Path::new(""));
    let parent = if parent_relative.as_os_str().is_empty() {
        anchor.try_clone()?
    } else {
        fs::open_relative(&anchor, &parent_relative.to_string_lossy())?
    };
    let name = Path::new(
        relative
            .file_name()
            .ok_or_else(|| Error::Conflict("generation name missing".into()))?,
    );
    let directory = fs::open_at(&parent, name, true)?;
    let identity = fs::identity_for_file(&directory)?;
    if (identity.device, identity.inode) != expected_identity {
        return Err(Error::Conflict(
            "sealed generation directory identity changed".into(),
        ));
    }
    let mut observed = fs::manifest(&directory, 100_000)?;
    for entry in observed.iter().filter(|entry| entry.identity.directory) {
        if entry.path != "files"
            && !files
                .iter()
                .any(|file| file.path.starts_with(&format!("{}/", entry.path)))
        {
            return Err(Error::Conflict(format!(
                "unexpected generation directory: {}",
                entry.path
            )));
        }
    }
    for entry in observed.iter().filter(|entry| !entry.identity.directory) {
        let expected = files
            .iter()
            .find(|file| file.path == entry.path)
            .ok_or_else(|| {
                Error::Conflict(format!("unexpected generation entry: {}", entry.path))
            })?;
        if expected.size_bytes != entry.identity.bytes {
            return Err(Error::Conflict("sealed generation length changed".into()));
        }
        let mut input = fs::open_relative(&directory, &entry.path)?;
        use std::io::Read;
        let mut hash = Sha256::new();
        let mut buffer = [0u8; 65536];
        loop {
            let count = input.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hash.update(&buffer[..count]);
        }
        if format!("{:x}", hash.finalize()) != expected.sha256
            || fs::identity_for_file(&input)? != entry.identity
        {
            return Err(Error::Conflict(
                "sealed generation bytes changed before retirement".into(),
            ));
        }
    }
    // Missing entries can be the exact crash-after-unlink boundary of this
    // durable copy obligation. Unexpected surviving entries cannot.
    observed.sort_by_key(|entry| std::cmp::Reverse(entry.path.matches('/').count()));
    for entry in &observed {
        fs::remove_entry(&directory, entry)?;
    }
    fs::remove_entry(
        &parent,
        &FileEntry {
            path: name.to_string_lossy().into_owned(),
            digest: None,
            identity,
        },
    )
}

#[cfg(test)]
mod generation_tests {
    use super::*;
    #[test]
    fn sealed_copy_retirement_replays_after_metadata_unlink_and_refuses_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("generations");
        let path = root.join("job/fence-1");
        std::fs::create_dir_all(path.join("files")).unwrap();
        let inputs = [
            ("files/episode.mkv", b"payload".as_slice()),
            ("job.json", b"job descriptor".as_slice()),
            ("manifest.json", b"manifest descriptor".as_slice()),
        ];
        let mut files = Vec::new();
        for (name, bytes) in inputs {
            std::fs::write(path.join(name), bytes).unwrap();
            files.push(DeliveryFile {
                path: name.into(),
                size_bytes: bytes.len() as u64,
                sha256: format!("{:x}", Sha256::digest(bytes)),
            });
        }
        let identity = fs::identity_for_file(&fs::open_dir(&path).unwrap()).unwrap();
        // The exact copy claim/deletion manifest is durable outside this tree.
        std::fs::remove_file(path.join("manifest.json")).unwrap();
        remove_cluster_generation_copy(
            &root,
            Path::new("job/fence-1"),
            (identity.device, identity.inode),
            &files,
        )
        .unwrap();
        assert!(!path.exists());
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("replacement.mkv"), b"unowned replacement").unwrap();
        assert!(remove_cluster_generation_copy(
            &root,
            Path::new("job/fence-1"),
            (identity.device, identity.inode),
            &files
        )
        .is_err());
        assert!(path.join("replacement.mkv").exists());
    }

    #[test]
    fn empty_sealed_payload_retirement_replays_after_metadata_unlink() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("generations");
        let path = root.join("job/fence-1");
        std::fs::create_dir_all(path.join("files")).unwrap();
        let bytes = b"empty generation descriptor";
        std::fs::write(path.join("manifest.json"), bytes).unwrap();
        let files = vec![DeliveryFile {
            path: "manifest.json".into(),
            size_bytes: bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(bytes)),
        }];
        let identity = fs::identity_for_file(&fs::open_dir(&path).unwrap()).unwrap();
        std::fs::remove_file(path.join("manifest.json")).unwrap();
        remove_cluster_generation_copy(
            &root,
            Path::new("job/fence-1"),
            (identity.device, identity.inode),
            &files,
        )
        .unwrap();
        assert!(!path.exists());
    }
}
