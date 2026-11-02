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
fn seed_delete_key(
    job: u32,
    source: &SourceGeneration,
    files: &BTreeSet<String>,
) -> Result<String> {
    Ok(format!(
        "failed-delete-torrent-seed-{:x}",
        Sha256::digest(serde_json::to_vec(&(job, source, files))?)
    ))
}
fn seed_journal_matches(op: &Operation, source: &SourceGeneration) -> Result<()> {
    let request: DeleteRequest = serde_json::from_str(&op.request)?;
    let (artifact, _, _, automatic, generation) = request.fields();
    if op.kind != "delete"
        || op.artifact != source.artifact
        || artifact != source.artifact
        || automatic
        || generation != Some(source.generation.as_str())
        || matches!(request, DeleteRequest::ClusterCancellation(_))
    {
        return Err(Error::Conflict(
            "managed seed deletion journal does not match its exact source generation".into(),
        ));
    }
    Ok(())
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
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Operation> {
        let (expected, dirs) = seed_selection(expected_files)?;
        let key = seed_delete_key(job, source, &expected)?;
        let existing = match self.operation(&key) {
            Ok(op) => {
                seed_journal_matches(&op, source)?;
                Some(op)
            }
            Err(Error::NotFound) => None,
            Err(error) => return Err(error),
        };
        // A completed receipt is a read-only no-op. In particular do not
        // reopen, measure or reconstruct a donor removed by this journal.
        if let Some(op) = existing
            .as_ref()
            .filter(|op| matches!(op.state.as_str(), "succeeded" | "cancelled" | "review"))
        {
            return Ok(op.clone());
        }
        checkpoint()?;
        let artifact = self.get(&source.artifact)?;
        if artifact.generation != source.generation
            || artifact.job != Some(job)
            || !artifact.owned
            || !artifact.automatic_cleanup_eligible
            || artifact.hold.is_some()
        {
            return Err(Error::Conflict(
                "managed seed source ownership/cohort or generation changed".into(),
            ));
        }
        if artifact.keep || artifact.keep_on_failure {
            return Err(Error::Conflict(
                "current Keep retains the managed seed source".into(),
            ));
        }
        let guard = self.acquire_seed_retirement_use(job, source, checkpoint)?;
        if let Some(op) = existing {
            // Its original identity/file receipt remains authoritative across
            // partial unlink. Never finish/re-hash a shrinking donor anew.
            return self.execute_delete_with_use(&op.id, Some((&guard, checkpoint)));
        }
        let artifact = {
            let _mutation = self.mutation_guard()?;
            checkpoint()?;
            let artifact = self.get(&source.artifact)?;
            if artifact.generation != source.generation
                || artifact.job != Some(job)
                || !artifact.owned
                || !artifact.automatic_cleanup_eligible
                || artifact.keep
                || artifact.keep_on_failure
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
        // This is terminal-disposal authority from the managed actor, not the
        // global failed-retention timer. The prefix independently checks live
        // KeepOnFailure at execution, including restarted partial journals.
        let op = self.request_delete_with_use(
            &artifact.id,
            artifact.revision,
            &key,
            0,
            false,
            Some((&guard, checkpoint)),
        )?;
        seed_journal_matches(&op, source)?;
        self.execute_delete_with_use(&op.id, Some((&guard, checkpoint)))
    }
}
