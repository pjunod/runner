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
        if !artifact.terminal()
            && (artifact.keep
                || artifact.hold.is_some()
                || (!copy.selected && outcome != "SUCCESS" && artifact.keep_on_failure))
        {
            return Ok(ClusterCopyCleanup {
                status: "retained".into(),
                error: Some(artifact.hold.clone().unwrap_or_else(|| {
                    "this exact source is retained by its current Keep setting".into()
                })),
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
                let directory = self.verify(&artifact)?;
                let observed = fs::manifest(&directory, 100_000)?;
                for entry in &observed {
                    if entry.identity.directory {
                        if !expected
                            .iter()
                            .any(|file| file.path.starts_with(&format!("{}/", entry.path)))
                        {
                            return Ok(ClusterCopyCleanup{status:"retained".into(),error:Some(format!("review required: raw directory {} was not in the accepted transfer seal",entry.path))});
                        }
                        continue;
                    }
                    let Some(file) = expected.iter().find(|file| file.path == entry.path) else {
                        return Ok(ClusterCopyCleanup{status:"retained".into(),error:Some(format!("review required: raw file {} was not in the accepted transfer seal",entry.path))});
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
                        return Ok(ClusterCopyCleanup{status:"retained".into(),error:Some(format!("review required: raw file {} differs from its accepted transfer seal",entry.path))});
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
