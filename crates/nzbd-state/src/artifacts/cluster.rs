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

    /// The peer handler first proves this exact copy is a replicated cleanup
    /// obligation of a READY delivery. Local storage identity, generation,
    /// current Keep/holds and the existing relocation journal remain decisive.
    pub fn retire_cluster_artifact_copy(
        &self,
        copy: &ClusterArtifactCopy,
        outcome: &str,
        proof: FinalizationProof,
    ) -> Result<ClusterCopyCleanup> {
        if copy.storage_instance_id != self.installation
            || !matches!(proof, FinalizationProof::Cluster { .. })
        {
            return Err(Error::Conflict(
                "cluster cleanup belongs to another storage instance".into(),
            ));
        }
        let artifact = self.get(&copy.publication.artifact)?;
        if artifact.generation != copy.publication.generation
            || artifact.job != Some(copy.job_id)
            || !artifact.automatic_cleanup_eligible
            || !artifact.delivery_lifecycle
        {
            return Err(Error::Conflict(
                "cluster cleanup copy generation or admission changed".into(),
            ));
        }
        if !copy.selected && outcome != "SUCCESS" && !artifact.terminal() {
            return Ok(ClusterCopyCleanup { status: "retained".into(), error: Some(
                "partial failure publication does not prove every raw source dependency redundant; retain this exact source until recovery is resolved".into()) });
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
                Err(Error::Conflict(reason)) if reason.starts_with("blocked:") => Some(reason),
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        let key = format!(
            "cluster-copy-{:x}",
            Sha256::digest(serde_json::to_vec(&(copy, &proof))?)
        );
        let existing = match self.operation(&key) {
            Ok(operation) => Some(operation),
            Err(Error::NotFound) => None,
            Err(error) => return Err(error),
        };
        if !artifact.terminal() && existing.is_none() {
            let revision = {
                let _guard = self.mutation_guard()?;
                let mut current = self.get(&artifact.id)?;
                if current.generation != copy.publication.generation
                    || !current.owned
                    || current.keep
                    || current.hold.is_some()
                    || self.source_in_use(&copy.publication)
                    || (!copy.selected && outcome != "SUCCESS" && current.keep_on_failure)
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
            self.request_delete(&artifact.id, revision, &key, 0)?;
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
