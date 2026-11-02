//! Exact owner-local failure preference. This creates no deletion authority.
use super::*;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClusterFailureKeepTarget {
    Artifact {
        copy: ClusterArtifactCopy,
    },
    LocalDelivery {
        job: u32,
        delivery_id: String,
    },
    Sealed {
        root: PathBuf,
        path: PathBuf,
        identity: (u64, u64),
        result_id: String,
        initial_keep: bool,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterFailureKeepStatus {
    pub available: bool,
    pub keep: bool,
}

impl Inventory {
    /// `revision=None` observes current policy. A mutation must have an exact
    /// still-current replicated desired revision, verified by the peer caller.
    pub fn cluster_copy_failure_keep(
        &self,
        target: &ClusterFailureKeepTarget,
        desired: Option<(u64, bool)>,
    ) -> Result<ClusterFailureKeepStatus> {
        let raw = serde_json::to_vec(target)?;
        if raw.len() > 16384 || desired.is_some_and(|(rev, _)| rev == 0 || rev > i64::MAX as u64) {
            return Err(Error::Conflict(
                "invalid exact copy Keep descriptor or revision".into(),
            ));
        }
        let key = format!("{:x}", Sha256::digest(&raw));
        let _guard = self.mutation_guard()?;
        {
            let db = self.db.lock().unwrap();
            db.execute_batch("CREATE TABLE IF NOT EXISTS cluster_failure_keep(key TEXT PRIMARY KEY,revision INTEGER NOT NULL CHECK(revision>0),keep INTEGER NOT NULL CHECK(keep IN(0,1))) STRICT")?;
            if let Some((revision, keep)) = desired {
                let old: Option<(i64, bool)> = db
                    .query_row(
                        "SELECT revision,keep FROM cluster_failure_keep WHERE key=?1",
                        [&key],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .optional()?;
                if old.is_some_and(|(rev, old_keep)| {
                    rev > revision as i64 || (rev == revision as i64 && old_keep != keep)
                }) {
                    return Err(Error::Conflict("copy Keep desired revision changed".into()));
                }
            }
        }
        let source = match target {
            ClusterFailureKeepTarget::Artifact { copy } => {
                if copy.storage_instance_id != self.installation {
                    return Err(Error::Conflict(
                        "copy Keep belongs to another storage instance".into(),
                    ));
                }
                let publication = self.get(&copy.publication.artifact)?;
                if publication.generation != copy.publication.generation
                    || publication.job != Some(copy.job_id)
                {
                    return Err(Error::Conflict(
                        "copy Keep publication generation changed".into(),
                    ));
                }
                if copy.selected {
                    let source = self.delivery_source_ref(
                        &publication.id,
                        &publication.generation,
                        &publication.path.to_string_lossy(),
                    )?;
                    if source != (copy.source.artifact.clone(), copy.source.generation.clone()) {
                        return Err(Error::Conflict("copy Keep source relation changed".into()));
                    }
                } else if copy.source != copy.publication {
                    return Err(Error::Conflict(
                        "unselected copy source relation changed".into(),
                    ));
                }
                Some((copy.job_id, copy.source.clone()))
            }
            ClusterFailureKeepTarget::LocalDelivery { job, delivery_id } => {
                if delivery_id.is_empty() || delivery_id.len() > 4096 {
                    return Err(Error::Conflict("invalid local Delivery identity".into()));
                }
                let source:Option<(u32,String,String)>=self.db.lock().unwrap().query_row(
                    "SELECT json_extract(data,'$.jobId'),json_extract(data,'$.sourceArtifactId'),json_extract(data,'$.sourceGeneration') FROM deliveries WHERE id=?1 AND length(CAST(json_extract(data,'$.sourceArtifactId') AS BLOB))<=256 AND length(CAST(json_extract(data,'$.sourceGeneration') AS BLOB))<=256",[delivery_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
                let Some((bound_job, artifact, generation)) = source else {
                    return Err(Error::Conflict(
                        "local Delivery source proof unavailable".into(),
                    ));
                };
                if *job != bound_job {
                    return Err(Error::Conflict("local Delivery Keep job changed".into()));
                }
                Some((
                    *job,
                    SourceGeneration {
                        artifact,
                        generation,
                    },
                ))
            }
            ClusterFailureKeepTarget::Sealed {
                root,
                path,
                identity,
                result_id,
                ..
            } => {
                if result_id.is_empty() || result_id.len() > 256 {
                    return Err(Error::Conflict("invalid sealed result identity".into()));
                }
                let relative = path.strip_prefix(root).map_err(|_| {
                    Error::Conflict("sealed Keep outside configured generations root".into())
                })?;
                if relative.as_os_str().is_empty()
                    || relative
                        .components()
                        .any(|c| !matches!(c, std::path::Component::Normal(_)))
                {
                    return Err(Error::Conflict("invalid sealed Keep path".into()));
                }
                // No-follow rooted directory descent. A missing target is gone
                // only after the configured generations root itself is open.
                let mut dir = fs::open_dir(root)?;
                for component in relative.components() {
                    match fs::open_at(&dir, Path::new(component.as_os_str()), true) {
                        Ok(next) => dir = next,
                        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                            return Ok(ClusterFailureKeepStatus {
                                available: false,
                                keep: false,
                            });
                        }
                        Err(error) => return Err(error),
                    }
                }
                let observed = fs::identity_for_file(&dir)?;
                if (observed.device, observed.inode) != *identity {
                    return Err(Error::Conflict(
                        "sealed copy identity changed; review required".into(),
                    ));
                }
                None
            }
        };
        let available_keep = if let Some((job, source)) = source {
            let artifact = self.get(&source.artifact)?;
            if artifact.generation != source.generation
                || artifact.job != Some(job)
                || !artifact.owned
            {
                return Err(Error::Conflict(
                    "source Keep generation or custody changed; review required".into(),
                ));
            }
            if artifact.terminal() {
                return Ok(ClusterFailureKeepStatus {
                    available: false,
                    keep: false,
                });
            }
            self.verify_root(&artifact)?;
            match self.verify(&artifact) {
                Ok(_) => {}
                Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(ClusterFailureKeepStatus {
                        available: false,
                        keep: false,
                    });
                }
                Err(error) => return Err(error),
            }
            if let Some((_, keep)) = desired {
                self.set_failure_keep_artifact_locked(job, artifact, keep, true)?;
                keep
            } else {
                artifact.keep_on_failure
            }
        } else {
            let db = self.db.lock().unwrap();
            let old: Option<bool> = db
                .query_row(
                    "SELECT keep FROM cluster_failure_keep WHERE key=?1",
                    [&key],
                    |r| r.get(0),
                )
                .optional()?;
            desired
                .map(|(_, keep)| keep)
                .or(old)
                .unwrap_or_else(|| match target {
                    ClusterFailureKeepTarget::Sealed { initial_keep, .. } => *initial_keep,
                    _ => false,
                })
        };
        if let Some((revision, keep)) = desired {
            self.db.lock().unwrap().execute("INSERT INTO cluster_failure_keep(key,revision,keep) VALUES(?1,?2,?3) ON CONFLICT(key) DO UPDATE SET revision=excluded.revision,keep=excluded.keep WHERE cluster_failure_keep.revision<=excluded.revision",params![key,revision as i64,keep])?;
        }
        Ok(ClusterFailureKeepStatus {
            available: true,
            keep: available_keep,
        })
    }
}

#[cfg(test)]
#[path = "cluster_keep_tests.rs"]
mod tests;
