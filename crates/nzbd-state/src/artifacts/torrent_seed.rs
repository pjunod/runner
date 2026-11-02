//! Managed native seed retirement under current actor/backend dependency proof.
//! No queue absence, metainfo path or historical job kind grants ownership.
use super::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::sync::Arc;

const MAX_SEED_METADATA: usize = 16 * 1024 * 1024;
fn seed_selection(expected: &[PathBuf]) -> Result<(BTreeSet<String>, BTreeSet<String>)> {
    if expected.is_empty() || expected.len() > 100_000 {
        return Err(Error::Conflict(
            "managed seed metainfo selection is empty or exceeds100000 entries".into(),
        ));
    }
    let mut files = BTreeSet::new();
    let mut dirs = BTreeSet::new();
    let mut bytes = 0usize;
    for path in expected {
        let text = path
            .to_str()
            .ok_or_else(|| Error::Conflict("managed seed metainfo path is not UTF8".into()))?;
        if text.is_empty()
            || text.len() > 4096
            || !path
                .components()
                .all(|part| matches!(part, std::path::Component::Normal(_)))
            || !files.insert(text.to_owned())
        {
            return Err(Error::Conflict(
                "managed seed metainfo has unsafe or duplicate paths".into(),
            ));
        }
        bytes = bytes
            .checked_add(text.len())
            .ok_or_else(|| Error::Conflict("managed seed metainfo size overflow".into()))?;
        let mut parent = path.parent();
        while let Some(path) = parent.filter(|path| !path.as_os_str().is_empty()) {
            let text = path.to_str().unwrap();
            if dirs.insert(text.to_owned()) {
                bytes = bytes.checked_add(text.len()).ok_or_else(|| {
                    Error::Conflict("managed seed directory metadata size overflow".into())
                })?;
            }
            if files.len() + dirs.len() > 100_000 || bytes > MAX_SEED_METADATA {
                return Err(Error::Conflict(
                    "managed seed metainfo exceeds supported manifest bounds; retained for review"
                        .into(),
                ));
            }
            parent = path.parent();
        }
        if bytes > MAX_SEED_METADATA {
            return Err(Error::Conflict(
                "managed seed metainfo exceeds supported path metadata bound".into(),
            ));
        }
    }
    if files.iter().any(|path| dirs.contains(path)) {
        return Err(Error::Conflict(
            "managed seed metainfo file overlaps a parent directory".into(),
        ));
    }
    Ok((files, dirs))
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentSeedRetirementReason {
    SuccessfulPolicy,
    Failure,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentSeedRetirementIntent {
    pub identity: String,
    pub reason: TorrentSeedRetirementReason,
    pub retry_revision: String,
    pub source_path: PathBuf,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ManagedSeedDeleteRequest {
    pub format: String,
    pub artifact: String,
    pub revision: u64,
    pub generation: String,
    pub intent: TorrentSeedRetirementIntent,
    pub manifest_sha256: String,
}
fn manifest_identity(artifact: &Artifact) -> Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&artifact.files)?)
    ))
}
fn seed_delete_key(
    job: u32,
    source: &SourceGeneration,
    files: &BTreeSet<String>,
    intent: &TorrentSeedRetirementIntent,
) -> Result<String> {
    if intent.identity.len() != 64
        || !intent.identity.bytes().all(|byte| byte.is_ascii_hexdigit())
        || intent.retry_revision.len() > 20
        || intent.retry_revision.parse::<u64>().is_err()
    {
        return Err(Error::Conflict(
            "invalid managed seed retirement intent".into(),
        ));
    }
    let prefix = if intent.reason == TorrentSeedRetirementReason::Failure {
        "failed-delete-torrent-seed-"
    } else {
        "managed-delete-torrent-seed-"
    };
    Ok(format!(
        "{prefix}{:x}",
        Sha256::digest(serde_json::to_vec(&(
            job,
            source,
            files,
            &intent.identity,
            &intent.reason
        ))?)
    ))
}
pub(super) fn is_seed_delete_key(key: &str) -> bool {
    key.starts_with("failed-delete-torrent-seed-")
        || key.starts_with("managed-delete-torrent-seed-")
}
fn seed_journal_matches(
    op: &Operation,
    source: &SourceGeneration,
    intent: &TorrentSeedRetirementIntent,
) -> Result<ManagedSeedDeleteRequest> {
    let DeleteRequest::ManagedSeed(request) = serde_json::from_str(&op.request)? else {
        return Err(Error::Conflict(
            "managed seed deletion lacks its immutable intent/manifest receipt".into(),
        ));
    };
    if op.kind != "delete"
        || op.artifact != source.artifact
        || request.format != "managed-seed-v1"
        || request.artifact != source.artifact
        || request.generation != source.generation
        || request.intent.identity != intent.identity
        || request.intent.reason != intent.reason
        || request.intent.source_path != intent.source_path
    {
        return Err(Error::Conflict(
            "managed seed deletion intent/source generation changed".into(),
        ));
    }
    Ok(request)
}
impl Inventory {
    /// The actor checkpoint proves the exact durable managed retirement marker,
    /// Detached/backend Remove(false)/forget ACK, no unsecured child refs and
    /// current retention choice. It must be valid at every invocation. Local
    /// ownership/cohort/root/use/Keep/member checks remain independent here.
    /// Generic maintenance is forbidden to execute this recipe without proof.
    pub fn retire_managed_torrent_seed(
        self: &Arc<Self>,
        job: u32,
        source: &SourceGeneration,
        expected_files: &[PathBuf],
        intent: &TorrentSeedRetirementIntent,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Operation> {
        let (expected, dirs) = seed_selection(expected_files)?;
        let key = seed_delete_key(job, source, &expected, intent)?;
        let existing = {
            let db = self.db.lock().unwrap();
            let raw:Option<String>=db.query_row("SELECT data FROM operations WHERE artifact=?1 AND (id=?2 OR id LIKE ?3) AND json_extract(data,'$.kind')='delete' ORDER BY rowid DESC LIMIT 1",params![source.artifact,key,format!("{key}.%")],|row|row.get(0)).optional()?;
            raw.map(|raw| serde_json::from_str::<Operation>(&raw))
                .transpose()?
        };
        let prior_request = existing
            .as_ref()
            .map(|op| seed_journal_matches(op, source, intent))
            .transpose()?;
        if let Some(op) = existing.as_ref().filter(|op| op.state == "succeeded") {
            return Ok(op.clone());
        }
        checkpoint()?;
        let artifact = self.get(&source.artifact)?;
        if artifact.path != intent.source_path
            || artifact.generation != source.generation
            || artifact.job != Some(job)
            || !artifact.owned
            || !artifact.automatic_cleanup_eligible
            || artifact.hold.is_some()
        {
            return Err(Error::Conflict(
                "managed seed source ownership/cohort or generation changed".into(),
            ));
        }
        if artifact.keep
            || (intent.reason == TorrentSeedRetirementReason::Failure && artifact.keep_on_failure)
        {
            return Err(Error::Conflict(
                "current Keep retains the managed seed source".into(),
            ));
        }
        let guard = self.acquire_seed_retirement_use(job, source, checkpoint)?;
        if let (Some(op), Some(request)) = (&existing, &prior_request) {
            if manifest_identity(&artifact)? != request.manifest_sha256 {
                return Err(Error::Conflict(
                    "managed seed immutable deletion manifest changed".into(),
                ));
            }
            if !matches!(op.state.as_str(), "cancelled" | "review")
                && request.intent.retry_revision == intent.retry_revision
            {
                return self.execute_delete_with_use(&op.id, Some((&guard, checkpoint)));
            }
            if request.intent.retry_revision == intent.retry_revision {
                return Ok(op.clone());
            }
            // A new actor-authorized retry carries the original seal and
            // partial-unlink attempt proof, never a reseal of surviving bytes.
            return self.fresh_seed_delete(
                job,
                source,
                intent,
                &key,
                &artifact,
                Some(op),
                &guard,
                checkpoint,
            );
        }
        let artifact = {
            let _mutation = self.mutation_guard()?;
            checkpoint()?;
            let artifact = self.get(&source.artifact)?;
            if artifact.path != intent.source_path
                || artifact.generation != source.generation
                || artifact.job != Some(job)
                || !artifact.owned
                || !artifact.automatic_cleanup_eligible
                || artifact.keep
                || (intent.reason == TorrentSeedRetirementReason::Failure
                    && artifact.keep_on_failure)
                || artifact.hold.is_some()
                || !self.authorized_processing_use(&artifact, Some(&guard))
            {
                return Err(Error::Conflict(
                    "managed seed retirement custody changed before sealing".into(),
                ));
            }
            let directory = self.verify(&artifact)?;
            let observed = fs::manifest(&directory, 100_000)?;
            for entry in &observed {
                if !(if entry.identity.directory {
                    dirs.contains(&entry.path)
                } else {
                    expected.contains(&entry.path)
                }) {
                    let reason = format!(
                        "managed seed contains an unrecorded metainfo member; retained for review: {}",
                        entry.path
                    );
                    let mut held = artifact.clone();
                    held.hold = Some("review: managed seed metainfo mismatch".into());
                    held.error = Some(reason.clone());
                    held.revision += 1;
                    held.updated_at = now();
                    let mut db = self.db.lock().unwrap();
                    let tx = db.transaction()?;
                    save_artifact(&tx, &held)?;
                    event(&tx, &held.id, "managed_seed_review", &reason)?;
                    tx.commit()?;
                    return Err(Error::Conflict(reason));
                }
            }
            checkpoint()?;
            let path = artifact.path.clone();
            let root = artifact.root.clone();
            let mut finished = self.finish_artifact_unlocked(artifact, &path, &root, "retained")?;
            if finished.files != observed {
                let reason =
                    "managed seed membership changed during final sealing; retained for review";
                finished.hold = Some("review: managed seed metainfo mismatch".into());
                finished.error = Some(reason.into());
                finished.revision += 1;
                finished.updated_at = now();
                let mut db = self.db.lock().unwrap();
                let tx = db.transaction()?;
                save_artifact(&tx, &finished)?;
                event(&tx, &finished.id, "managed_seed_review", reason)?;
                tx.commit()?;
                return Err(Error::Conflict(reason.into()));
            }
            finished
        };
        checkpoint()?;
        self.fresh_seed_delete(
            job, source, intent, &key, &artifact, None, &guard, checkpoint,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn fresh_seed_delete(
        &self,
        job: u32,
        source: &SourceGeneration,
        intent: &TorrentSeedRetirementIntent,
        key: &str,
        artifact: &Artifact,
        previous: Option<&Operation>,
        guard: &AttemptUse,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Operation> {
        let operation = {
            let _mutation = self.mutation_guard()?;
            checkpoint()?;
            let current = self.get(&source.artifact)?;
            if current.path != intent.source_path
                || current.job != Some(job)
                || !current.owned
                || !current.automatic_cleanup_eligible
                || current.generation != source.generation
                || current.keep
                || current.hold.is_some()
                || (intent.reason == TorrentSeedRetirementReason::Failure
                    && current.keep_on_failure)
                || !self.authorized_processing_use(&current, Some(guard))
                || manifest_identity(&current)? != manifest_identity(artifact)?
            {
                return Err(Error::Conflict(
                    "managed seed retry custody or immutable seal changed".into(),
                ));
            }
            let number = previous
                .and_then(|op| op.id.rsplit('.').next()?.parse::<u64>().ok())
                .map(|number| {
                    number.checked_add(1).ok_or_else(|| {
                        Error::Conflict("managed seed retry identity exhausted".into())
                    })
                })
                .transpose()?
                .unwrap_or(0);
            let operation = Operation {
                id: format!("{key}.{number}"),
                artifact: source.artifact.clone(),
                kind: "delete".into(),
                state: "queued".into(),
                request: serde_json::to_string(&ManagedSeedDeleteRequest {
                    format: "managed-seed-v1".into(),
                    artifact: source.artifact.clone(),
                    generation: source.generation.clone(),
                    revision: current.revision,
                    intent: intent.clone(),
                    manifest_sha256: manifest_identity(&current)?,
                })?,
                created_at: now(),
                not_before: now(),
                attempts: previous.map_or(0, |op| op.attempts),
                next_retry: 0,
                error: None,
            };
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            if let Some(previous) = previous {
                if previous.state == "running" {
                    return Err(Error::Conflict(
                        "previous seed retirement is still running".into(),
                    ));
                }
                if previous.state == "retry" {
                    let mut superseded = previous.clone();
                    superseded.state = "cancelled".into();
                    superseded.error = Some("superseded by exact actor-authorized retry".into());
                    save_operation(&tx, &superseded)?;
                }
            }
            let other:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM operations WHERE artifact=?1 AND state IN ('queued','running','retry') AND json_extract(data,'$.kind')='delete')",[&source.artifact],|row|row.get(0))?;
            if other {
                return Err(Error::Conflict(
                    "another exact deletion owns this seed generation".into(),
                ));
            }
            save_operation(&tx, &operation)?;
            event(
                &tx,
                &source.artifact,
                "managed_seed_delete_requested",
                &operation.id,
            )?;
            tx.commit()?;
            operation
        };
        self.execute_delete_with_use(&operation.id, Some((guard, checkpoint)))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::cell::Cell;

    struct Fixture {
        _temp: tempfile::TempDir,
        inventory: Arc<Inventory>,
        source: SourceGeneration,
        intent: TorrentSeedRetirementIntent,
    }
    impl Fixture {
        fn new(reason: TorrentSeedRetirementReason) -> Self {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("seeds");
            std::fs::create_dir(&root).unwrap();
            let path = root.join("managed");
            let inventory = Arc::new(Inventory::open(&temp.path().join("state")).unwrap());
            inventory
                .allocate_for_delivery(91, &root, &path, true, false)
                .unwrap();
            std::fs::write(path.join("first.bin"), b"first").unwrap();
            std::fs::write(path.join("second.bin"), b"second").unwrap();
            let a = inventory.for_job(91).unwrap().unwrap();
            Self {
                _temp: temp,
                inventory,
                source: SourceGeneration {
                    artifact: a.id,
                    generation: a.generation,
                },
                intent: TorrentSeedRetirementIntent {
                    identity: "a".repeat(64),
                    reason,
                    retry_revision: "0".into(),
                    source_path: path,
                },
            }
        }
        fn retire(&self, checkpoint: &dyn Fn() -> std::io::Result<()>) -> Result<Operation> {
            self.inventory.retire_managed_torrent_seed(
                91,
                &self.source,
                &["first.bin".into(), "second.bin".into()],
                &self.intent,
                checkpoint,
            )
        }
    }
    #[test]
    fn seed_policy_success_ignores_failure_keep_but_failure_retains_source() {
        let success = Fixture::new(TorrentSeedRetirementReason::SuccessfulPolicy);
        success.inventory.set_keep_on_failure(91, true).unwrap();
        let op = success.retire(&|| Ok(())).unwrap();
        assert_eq!(op.state, "succeeded");
        assert!(!success.intent.source_path.exists());
        assert_eq!(
            success
                .retire(&|| panic!("completed receipt is read-only"))
                .unwrap()
                .id,
            op.id
        );
        let failure = Fixture::new(TorrentSeedRetirementReason::Failure);
        failure.inventory.set_keep_on_failure(91, true).unwrap();
        assert!(failure.retire(&|| Ok(())).is_err());
        assert!(failure.intent.source_path.join("first.bin").exists());
    }
    #[test]
    fn seed_keep_at_delete_boundary_and_partial_retry_preserve_original_seal() {
        let mut f = Fixture::new(TorrentSeedRetirementReason::Failure);
        let injected = Cell::new(false);
        let inv = f.inventory.clone();
        let source = f.source.clone();
        let checkpoint = || {
            let queued: bool = inv
                .db
                .lock()
                .unwrap()
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM operations WHERE artifact=?1 AND state='queued')",
                    [&source.artifact],
                    |row| row.get(0),
                )
                .unwrap();
            if queued && !injected.replace(true) {
                // Simulate the operator write winning immediately before the
                // real generic-delete authorization check (no FS mock).
                let mut a = inv.get(&source.artifact).unwrap();
                a.keep_on_failure = true;
                a.revision += 1;
                save_artifact(&inv.db.lock().unwrap(), &a).unwrap();
            }
            Ok(())
        };
        let cancelled = f.retire(&checkpoint).unwrap();
        assert_eq!(cancelled.state, "cancelled");
        assert!(f.intent.source_path.join("first.bin").exists());
        f.inventory.set_keep_on_failure(91, false).unwrap();
        f.intent.retry_revision = "1".into();
        DELETE_FAIL_AFTER.with(|hook| hook.set(Some(1)));
        let partial = f.retire(&|| Ok(())).unwrap();
        assert_eq!(partial.state, "retry");
        assert_eq!(partial.attempts, 1);
        assert_ne!(partial.id, cancelled.id);
        let manifest = f.inventory.get(&f.source.artifact).unwrap().files;
        f.intent.retry_revision = "2".into();
        let done = f.retire(&|| Ok(())).unwrap();
        assert_eq!(done.state, "succeeded");
        assert_eq!(done.attempts, 2);
        assert_eq!(f.inventory.get(&f.source.artifact).unwrap().files, manifest);
        assert!(!f.intent.source_path.exists());
    }
}
