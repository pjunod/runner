//! Durable publication receipts, independent of visible history retention.
use super::*;
use sha2::{Digest, Sha256};
use std::io::Read;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryFile {
    pub path: String,
    pub size_bytes: u64,
    pub sha256: String,
}

/// Classification of every source-relative regular file at publication time.
/// `publicationPath` is present only for selected files copied to the
/// completed payload tree; source files remain separately Runner-owned.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryFileOutcome {
    pub source_path: String,
    pub publication_path: Option<String>,
    pub classification: String,
    pub outcome: String,
    pub reason: Option<String>,
    /// This source file is an evidence-backed member of a measured PAR set
    /// still needed to retry repair for the same source generation. It is
    /// independent of publication classification: a verified sibling may be
    /// published while remaining a repair input until a later attempt proves
    /// the set resolved.
    #[serde(default)]
    pub repair_dependency: bool,
    #[serde(default)]
    pub source_cleanup_status: String,
}

/// Immutable description of one published generation plus its independent
/// consumer-acceptance receipt. The payload bytes never contain this record.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Delivery {
    pub protocol_version: u8,
    pub sequence: i64,
    pub status: String,
    pub delivery_id: String,
    pub publication_generation: String,
    pub artifact_id: String,
    /// Runner-owned processing source, distinct from the immutable published
    /// artifact once a cross-filesystem copy created a retained source row.
    #[serde(default)]
    pub source_artifact_id: String,
    #[serde(default)]
    pub source_generation: String,
    pub job_id: u32,
    pub runner_instance_id: String,
    pub request_token: Option<String>,
    /// Explicit per-download choice to preserve unresolved Runner source bytes
    /// after a failed publication. Resolved duplicate source files remain safe
    /// to retire independently.
    #[serde(default)]
    pub keep_source_on_failure: bool,
    #[serde(default = "default_terminal_outcome")]
    pub terminal_outcome: String,
    #[serde(default)]
    pub failure_stage: Option<String>,
    #[serde(default)]
    pub failure_cause: Option<String>,
    pub publication_path: String,
    pub manifest_digest: String,
    pub files: Vec<DeliveryFile>,
    #[serde(default)]
    pub file_outcomes: Vec<DeliveryFileOutcome>,
    pub ready_at: i64,
    pub accepted_at: Option<i64>,
    pub consumer_receipt_id: Option<String>,
    pub source_cleanup: SourceCleanup,
}

fn default_terminal_outcome() -> String {
    "SUCCESS".into()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceCleanup {
    pub status: String,
    pub error: Option<String>,
    pub attempts: u32,
    pub next_retry_at: Option<i64>,
    pub blocked_since: Option<i64>,
    pub warning: Option<String>,
    #[serde(default)]
    pub started_at: Option<i64>,
    #[serde(default)]
    pub last_attempt_at: Option<i64>,
    #[serde(default)]
    pub completed_at: Option<i64>,
    #[serde(default)]
    pub attempt_history: Vec<SourceCleanupAttempt>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceCleanupAttempt {
    pub attempt: u32,
    pub started_at_ms: i64,
    pub ended_at_ms: Option<i64>,
    pub work_ms: Option<u64>,
    pub wait_ms: u64,
    pub outcome: String,
    pub error: Option<String>,
}

fn digest_files(root: &File, entries: &[FileEntry]) -> Result<Vec<DeliveryFile>> {
    let mut files = Vec::new();
    for entry in entries.iter().filter(|entry| !entry.identity.directory) {
        let mut file = fs::open_relative(root, &entry.path)?;
        if fs::identity_for_file(&file)? != entry.identity {
            return Err(Error::Conflict(format!(
                "delivery source identity changed: {}",
                entry.path
            )));
        }
        let mut hasher = Sha256::new();
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        if fs::identity_for_file(&file)? != entry.identity {
            return Err(Error::Conflict(format!(
                "delivery source changed while hashing: {}",
                entry.path
            )));
        }
        files.push(DeliveryFile {
            path: entry.path.clone(),
            size_bytes: entry.identity.bytes,
            sha256: format!("{:x}", hasher.finalize()),
        });
    }
    files.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
    Ok(files)
}

fn manifest_digest(files: &[DeliveryFile]) -> String {
    let mut canonical = b"runner-manifest-v1\n".to_vec();
    for file in files {
        let path = file.path.as_bytes();
        canonical.extend_from_slice(path.len().to_string().as_bytes());
        canonical.push(b':');
        canonical.extend_from_slice(path);
        canonical.push(b'\n');
        canonical.extend_from_slice(file.size_bytes.to_string().as_bytes());
        canonical.push(b'\n');
        canonical.extend_from_slice(file.sha256.as_bytes());
        canonical.push(b'\n');
    }
    format!("{:x}", Sha256::digest(canonical))
}

type DeliveryRow = (
    String,
    Option<i64>,
    Option<String>,
    String,
    Option<String>,
    u32,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
);

fn read_delivery(db: &Connection, id: &str) -> Result<Delivery> {
    let (data, accepted_at, receipt, cleanup_status, cleanup_error, cleanup_attempts, next_retry, blocked_since, cleanup_started, cleanup_last_attempt, cleanup_completed): DeliveryRow = db
        .query_row(
            "SELECT data,accepted_at,consumer_receipt_id,cleanup_status,cleanup_error,cleanup_attempts,cleanup_next_retry_at,cleanup_blocked_since,cleanup_started_at,cleanup_last_attempt_at,cleanup_completed_at FROM deliveries WHERE id=?1",
            [id],
            |row| Ok((
                row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?,
                row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?, row.get(9)?,
                row.get(10)?,
            )),
        )
        .optional()?
        .ok_or(Error::NotFound)?;
    let mut delivery: Delivery = serde_json::from_str(&data)?;
    let attempt_history = {
        let mut statement = db.prepare("SELECT attempt,started_at,ended_at,wait_ms,outcome,error FROM delivery_cleanup_attempts WHERE delivery_id=?1 ORDER BY attempt")?;
        let rows = statement.query_map([id], |row| {
            let started_at_ms: i64 = row.get(1)?;
            let ended_at_ms: Option<i64> = row.get(2)?;
            Ok(SourceCleanupAttempt {
                attempt: row.get(0)?,
                started_at_ms,
                ended_at_ms,
                work_ms: ended_at_ms.map(|ended| ended.saturating_sub(started_at_ms).max(0) as u64),
                wait_ms: row.get::<_, i64>(3)?.max(0) as u64,
                outcome: row.get(4)?,
                error: row.get(5)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()?
    };
    delivery.accepted_at = accepted_at;
    delivery.consumer_receipt_id = receipt;
    delivery.status = if accepted_at.is_some() {
        "accepted".into()
    } else {
        "ready".into()
    };
    delivery.source_cleanup = SourceCleanup {
        status: cleanup_status,
        error: cleanup_error,
        attempts: cleanup_attempts,
        next_retry_at: next_retry,
        blocked_since,
        warning: blocked_since
            .filter(|at| now().saturating_sub(*at) >= 1800)
            .map(|_| {
                "source cleanup has been blocked for at least 30 minutes; retries continue".into()
            }),
        started_at: cleanup_started,
        last_attempt_at: cleanup_last_attempt,
        completed_at: cleanup_completed,
        attempt_history,
    };
    Ok(delivery)
}

impl Inventory {
    pub(super) fn retry_delivery_cleanup(&self) -> Result<()> {
        let ids = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare(
                "SELECT id FROM deliveries WHERE cleanup_status IN ('pending','retry') AND COALESCE(cleanup_next_retry_at,0)<=?1 ORDER BY sequence LIMIT 5",
            )?;
            let rows = stmt.query_map([now()], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for id in ids {
            let _ = self.retire_delivery_source(&id);
        }
        Ok(())
    }

    /// Record the immutable, durable handoff obligation for a published job.
    /// Call only after publication has completed and its directory has been
    /// synced. Replays for the same artifact generation return the same row.
    pub fn create_delivery(
        &self,
        job: u32,
        admitted_for_delivery: bool,
        request_token: Option<&str>,
    ) -> Result<Delivery> {
        self.create_delivery_with_outcome(
            job,
            admitted_for_delivery,
            request_token,
            "SUCCESS",
            None,
            None,
        )
    }

    pub fn create_delivery_with_outcome(
        &self,
        job: u32,
        admitted_for_delivery: bool,
        request_token: Option<&str>,
        terminal_outcome: &str,
        failure_stage: Option<&str>,
        failure_cause: Option<&str>,
    ) -> Result<Delivery> {
        self.create_delivery_with_outcome_and_keep(
            job,
            admitted_for_delivery,
            request_token,
            terminal_outcome,
            failure_stage,
            failure_cause,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_delivery_with_outcome_and_keep(
        &self,
        job: u32,
        admitted_for_delivery: bool,
        request_token: Option<&str>,
        terminal_outcome: &str,
        failure_stage: Option<&str>,
        failure_cause: Option<&str>,
        keep_source_on_failure: bool,
    ) -> Result<Delivery> {
        if !admitted_for_delivery {
            return Err(Error::Conflict(
                "pre-existing job is exempt from automatic delivery lifecycle".into(),
            ));
        }
        let (artifact, root, observed, publication_path, existing_delivery) = {
            let _guard = self.mutation_guard()?;
            let artifact = self.for_job(job)?.ok_or(Error::NotFound)?;
            let id = format!("{}:{}", artifact.id, artifact.generation);
            let existing = match self.delivery(&id) {
                Ok(delivery) => Some(delivery),
                Err(Error::NotFound) => None,
                Err(error) => return Err(error),
            };
            let replay_of_transfer =
                artifact.state == "transferred" && !artifact.owned && existing.is_some();
            if !replay_of_transfer
                && (!artifact.owned
                    || !artifact.automatic_cleanup_eligible
                    || !artifact.delivery_lifecycle
                    || !matches!(artifact.state.as_str(), "active" | "completed")
                    || artifact.hold.is_some())
            {
                return Err(Error::Conflict(
                    "delivery requires an active, owned, unheld publication".into(),
                ));
            }
            let root = self.verify(&artifact)?;
            let observed = fs::manifest(&root, 100_000)?;
            if observed != artifact.files {
                return Err(Error::Conflict(
                    "published payload changed before delivery receipt".into(),
                ));
            }
            let publication_path = artifact.path.to_string_lossy().into_owned();
            (artifact, root, observed, publication_path, existing)
        };
        let file_outcomes = self.delivery_file_outcomes(&artifact.id, &artifact.generation)?;
        let (source_artifact_id, source_generation) =
            self.delivery_source_ref(&artifact.id, &artifact.generation, &publication_path)?;
        // Hashing may read a large media payload. Do it without holding the
        // inventory mutation lease; the caller runs this method on a bounded
        // blocking worker.
        let files = digest_files(&root, &observed)?;
        if files.is_empty() {
            return Err(Error::Conflict(
                "cannot publish a delivery with an empty file manifest".into(),
            ));
        }
        let manifest_digest = manifest_digest(&files);
        let id = format!("{}:{}", artifact.id, artifact.generation);
        let _guard = self.mutation_guard()?;
        let mut current = self.get(&artifact.id)?;
        if current.generation != artifact.generation
            || current.path != artifact.path
            || current.root != artifact.root
            || !current.root_identity.same_object(&artifact.root_identity)
            || !(current.owned
                && matches!(current.state.as_str(), "active" | "completed")
                && current.hold.is_none()
                || current.state == "transferred" && !current.owned && existing_delivery.is_some())
        {
            return Err(Error::Conflict(
                "publication changed while preparing delivery manifest".into(),
            ));
        }
        let current_root = self.verify(&current)?;
        if fs::manifest(&current_root, 100_000)? != observed {
            return Err(Error::Conflict(
                "published payload changed while preparing delivery manifest".into(),
            ));
        }
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let existing = tx
            .query_row("SELECT 1 FROM deliveries WHERE id=?1", [&id], |_| Ok(()))
            .optional()?;
        if existing.is_some() {
            let delivery = read_delivery(&tx, &id)?;
            if delivery.manifest_digest != manifest_digest
                || delivery.publication_path != publication_path
                || delivery.files != files
                || delivery.file_outcomes != file_outcomes
                || delivery.terminal_outcome != terminal_outcome
                || delivery.failure_stage.as_deref() != failure_stage
                || delivery.failure_cause.as_deref() != failure_cause
                || delivery.keep_source_on_failure != keep_source_on_failure
            {
                return Err(Error::Conflict(
                    "immutable delivery generation changed".into(),
                ));
            }
            tx.commit()?;
            return Ok(delivery);
        }
        let sequence: i64 = tx.query_row(
            "SELECT COALESCE(MAX(sequence),0)+1 FROM deliveries",
            [],
            |row| row.get(0),
        )?;
        let delivery = Delivery {
            protocol_version: 1,
            sequence,
            status: "ready".into(),
            delivery_id: id,
            publication_generation: artifact.generation,
            artifact_id: artifact.id,
            source_artifact_id,
            source_generation,
            job_id: job,
            runner_instance_id: self.installation.clone(),
            request_token: request_token.map(str::to_owned),
            keep_source_on_failure,
            terminal_outcome: terminal_outcome.to_owned(),
            failure_stage: failure_stage.map(str::to_owned),
            failure_cause: failure_cause.map(str::to_owned),
            publication_path,
            manifest_digest,
            files,
            file_outcomes,
            ready_at: now(),
            accepted_at: None,
            consumer_receipt_id: None,
            source_cleanup: SourceCleanup {
                status: "pending".into(),
                error: None,
                attempts: 0,
                next_retry_at: None,
                blocked_since: None,
                warning: None,
                started_at: None,
                last_attempt_at: None,
                completed_at: None,
                attempt_history: Vec::new(),
            },
        };
        current.owned = false;
        current.state = "transferred".into();
        current.hold = None;
        current.deadline = None;
        current.revision += 1;
        current.updated_at = now();
        self.sidecar(&current)?;
        super::save_artifact(&tx, &current)?;
        tx.execute(
            "INSERT INTO deliveries(sequence,id,artifact,generation,data) VALUES(?1,?2,?3,?4,?5)",
            params![
                delivery.sequence,
                delivery.delivery_id,
                delivery.artifact_id,
                delivery.publication_generation,
                serde_json::to_string(&delivery)?
            ],
        )?;
        tx.commit()?;
        Ok(delivery)
    }

    /// Scan a bounded sequence window. The cursor advances over accepted rows
    /// too, while every unaccepted row remains independently discoverable to
    /// callers that have not advanced past it.
    pub fn deliveries_after(&self, after: i64, limit: u32) -> Result<(Vec<Delivery>, i64)> {
        self.deliveries_page(after, limit, false)
    }

    pub fn deliveries_page(
        &self,
        after: i64,
        limit: u32,
        include_accepted: bool,
    ) -> Result<(Vec<Delivery>, i64)> {
        if after < 0 || !(1..=100).contains(&limit) {
            return Err(Error::Conflict("invalid delivery cursor or limit".into()));
        }
        let db = self.db.lock().unwrap();
        let mut statement =
            db.prepare("SELECT id FROM deliveries WHERE sequence>?1 ORDER BY sequence LIMIT ?2")?;
        let rows = statement.query_map(params![after, limit], |row| row.get::<_, String>(0))?;
        let ids = rows.collect::<std::result::Result<Vec<_>, _>>()?;
        let mut deliveries = Vec::new();
        let mut cursor = after;
        for id in ids {
            let delivery = read_delivery(&db, &id)?;
            cursor = delivery.sequence;
            if include_accepted || delivery.accepted_at.is_none() {
                deliveries.push(delivery);
            }
        }
        Ok((deliveries, cursor))
    }

    /// Persist Curator's durable mapping receipt. Repeating the exact tuple is
    /// safe; a conflicting receipt cannot take over an accepted generation.
    pub fn accept_delivery(
        &self,
        delivery_id: &str,
        publication_generation: &str,
        manifest_digest: &str,
        consumer_receipt_id: &str,
    ) -> Result<Delivery> {
        if delivery_id.is_empty()
            || delivery_id.len() > 256
            || consumer_receipt_id.is_empty()
            || consumer_receipt_id.len() > 256
        {
            return Err(Error::Conflict("invalid delivery receipt identity".into()));
        }
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let mut delivery = read_delivery(&tx, delivery_id)?;
        if delivery.publication_generation != publication_generation
            || delivery.manifest_digest != manifest_digest
        {
            return Err(Error::Conflict(
                "delivery generation or manifest digest changed".into(),
            ));
        }
        if let Some(receipt) = &delivery.consumer_receipt_id {
            if receipt != consumer_receipt_id {
                return Err(Error::Conflict(
                    "delivery already has a different consumer receipt".into(),
                ));
            }
        } else {
            delivery.accepted_at = Some(now());
            delivery.consumer_receipt_id = Some(consumer_receipt_id.into());
            tx.execute(
                "UPDATE deliveries SET accepted_at=?2,consumer_receipt_id=?3 WHERE id=?1 AND accepted_at IS NULL",
                params![delivery_id, delivery.accepted_at, delivery.consumer_receipt_id],
            )?;
        }
        tx.commit()?;
        drop(db);
        self.delivery(delivery_id)
    }

    pub fn delivery(&self, delivery_id: &str) -> Result<Delivery> {
        let mut delivery = read_delivery(&self.db.lock().unwrap(), delivery_id)?;
        let current_outcomes =
            self.delivery_file_outcomes(&delivery.artifact_id, &delivery.publication_generation)?;
        if !current_outcomes.is_empty() {
            delivery.file_outcomes = current_outcomes;
        }
        Ok(delivery)
    }

    /// Resolve recovery against the artifact generation currently attached
    /// to this queue job. This remains unambiguous if numeric job IDs are
    /// reused, and does not depend on an external Curator correlation token.
    pub fn delivery_for_job(&self, job: u32) -> Result<Option<Delivery>> {
        let Some(artifact) = self.for_job(job)? else {
            return Ok(None);
        };
        let id = format!("{}:{}", artifact.id, artifact.generation);
        match self.delivery(&id) {
            Ok(delivery) if delivery.job_id == job => Ok(Some(delivery)),
            Ok(_) => Err(Error::Conflict(
                "delivery generation is not attached to this queue job".into(),
            )),
            Err(Error::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_runner_delivery_fixture_matches_rust_wire_dtos() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/curator_delivery_v1.json"
        ))
        .unwrap();
        let delivery: Delivery = serde_json::from_value(fixture["delivery"].clone()).unwrap();
        assert_eq!(
            serde_json::to_value(&delivery).unwrap(),
            fixture["delivery"]
        );
        assert_eq!(
            manifest_digest(&delivery.files),
            delivery.manifest_digest,
            "the shared fixture must contain a valid manifest digest"
        );
        let admission: crate::UrlAdmissionReceipt =
            serde_json::from_value(fixture["requestAdmission"].clone()).unwrap();
        assert_eq!(
            serde_json::to_value(admission).unwrap(),
            fixture["requestAdmission"]
        );
    }

    #[test]
    fn manifest_digest_has_cross_language_unicode_golden_vector() {
        let files = vec![
            DeliveryFile {
                path: "z.mkv".into(),
                size_bytes: 1,
                sha256: "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881".into(),
            },
            DeliveryFile {
                path: "é<&.mkv".into(),
                size_bytes: 1,
                sha256: "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881".into(),
            },
        ];
        assert_eq!(
            manifest_digest(&files),
            "a0acee1d6757529c7deca3dc2ac2fa2f0b6e96db781d83b8d85d732cedd6459f"
        );
    }
}
