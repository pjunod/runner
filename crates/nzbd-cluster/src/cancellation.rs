//! Exact, replicated disposition of a sealed result rejected after removal.
//! A timeout, expired lease or peer-supplied path creates no byte authority.
use crate::control::ControlStore;
use hiqlite::{Row, macros::params};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RemovalMode {
    KeepFiles,
    DeleteFiles,
    CompletedRetirement,
}
impl RemovalMode {
    pub(crate) fn native_action(action: &str) -> Option<Self> {
        match action {
            "delete-files" => Some(Self::DeleteFiles),
            "delete" => Some(Self::KeepFiles),
            _ => None,
        }
    }
    pub(crate) fn compat_action(action: &str) -> Option<Self> {
        match action {
            "GroupFinalDelete" => Some(Self::DeleteFiles),
            "GroupDelete" | "GroupParkDelete" | "GroupTrashDelete" => Some(Self::KeepFiles),
            _ => None,
        }
    }
    fn as_str(self) -> &'static str {
        match self {
            Self::KeepFiles => "keep_files",
            Self::DeleteFiles => "delete_files",
            Self::CompletedRetirement => "completed_retirement",
        }
    }
}

const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS cluster_job_removals(job_id INTEGER NOT NULL CHECK(job_id BETWEEN 1 AND 4294967295),incarnation TEXT NOT NULL CHECK(length(CAST(incarnation AS BLOB)) BETWEEN 1 AND 256),prior_revision INTEGER NOT NULL CHECK(prior_revision>0),applied_revision INTEGER NOT NULL CHECK(applied_revision>prior_revision AND applied_revision=prior_revision+1),mode TEXT NOT NULL CHECK(mode IN ('keep_files','delete_files','completed_retirement')),at_ms INTEGER NOT NULL CHECK(at_ms>=0),PRIMARY KEY(job_id,incarnation)) STRICT",
    "CREATE TABLE IF NOT EXISTS cluster_cancellation_decisions(receipt_id TEXT PRIMARY KEY CHECK(length(CAST(receipt_id AS BLOB)) BETWEEN 1 AND 256),fingerprint TEXT NOT NULL,job_id INTEGER NOT NULL,incarnation TEXT NOT NULL,removal_revision INTEGER NOT NULL,cleanup_id TEXT NOT NULL UNIQUE,data TEXT NOT NULL CHECK(length(CAST(data AS BLOB))<=8192)) STRICT",
    "CREATE TABLE IF NOT EXISTS cluster_cancellation_owner_stops(cleanup_id TEXT NOT NULL,owner_node TEXT NOT NULL,storage_instance TEXT NOT NULL,artifact TEXT NOT NULL,generation TEXT NOT NULL,job_id INTEGER NOT NULL,incarnation TEXT NOT NULL,removal_revision INTEGER NOT NULL,at_ms INTEGER NOT NULL,digest TEXT NOT NULL,PRIMARY KEY(cleanup_id,owner_node,storage_instance,artifact,generation),FOREIGN KEY(cleanup_id) REFERENCES cluster_cancellation_decisions(cleanup_id)) STRICT",
    "CREATE TRIGGER IF NOT EXISTS cancellation_decision_exact BEFORE INSERT ON cluster_cancellation_decisions BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM cluster_job_removals r JOIN control_jobs j ON j.job_id=r.job_id AND j.incarnation=r.incarnation AND j.revision=r.applied_revision AND j.deleted=1 WHERE r.job_id=NEW.job_id AND r.incarnation=NEW.incarnation AND r.applied_revision=NEW.removal_revision) THEN RAISE(ABORT,'cancellation removal changed before decision') END; SELECT CASE WHEN EXISTS(SELECT 1 FROM cluster_cancellation_decisions WHERE receipt_id=NEW.receipt_id AND (fingerprint<>NEW.fingerprint OR cleanup_id<>NEW.cleanup_id)) THEN RAISE(ABORT,'cancellation receipt identity conflict') END; END",
    "CREATE TRIGGER IF NOT EXISTS cluster_removal_exact BEFORE INSERT ON cluster_job_removals BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM control_jobs WHERE job_id=NEW.job_id AND incarnation=NEW.incarnation AND revision=NEW.applied_revision AND deleted=1) THEN RAISE(ABORT,'removal is not the exact committed job tombstone') END; SELECT CASE WHEN EXISTS(SELECT 1 FROM cluster_job_removals WHERE job_id=NEW.job_id AND incarnation=NEW.incarnation AND (prior_revision<>NEW.prior_revision OR applied_revision<>NEW.applied_revision OR mode<>NEW.mode)) THEN RAISE(ABORT,'removal mode cannot change after retirement') END; END",
];

pub(crate) async fn install_schema(control: &ControlStore) -> Result<(), String> {
    control
        .client
        .txn(
            SCHEMA
                .iter()
                .map(|sql| ((*sql).to_owned(), params!()))
                .collect::<Vec<_>>(),
        )
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Append after the exact command receipt in the SAME guarded Raft transaction.
/// A generic/legacy removal must explicitly use KeepFiles, never infer deletion.
pub(crate) fn tombstone_statements(
    job_id: u32,
    incarnation: &str,
    prior_revision: u64,
    applied_revision: u64,
    mode: RemovalMode,
    at_ms: i64,
) -> Vec<(String, hiqlite::Params)> {
    vec![("INSERT INTO cluster_job_removals(job_id,incarnation,prior_revision,applied_revision,mode,at_ms) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(job_id,incarnation) DO NOTHING".into(),params!(i64::from(job_id),incarnation,i64::try_from(prior_revision).unwrap_or(-1),i64::try_from(applied_revision).unwrap_or(-1),mode.as_str(),at_ms))]
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CancellationRequest {
    pub lifecycle_protocol: u8,
    pub node: String,
    pub job_id: u32,
    pub job_incarnation: String,
    pub lease_id: String,
    pub token: crate::control::LeaseToken,
    pub expected_job_revision: u64,
    pub result_id: String,
    pub result_ref: String,
    pub receipt_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RemovalRecord {
    pub job_id: u32,
    pub incarnation: String,
    pub prior_revision: u64,
    pub applied_revision: u64,
    pub mode: RemovalMode,
    pub at_ms: i64,
}
impl From<&mut Row<'_>> for RemovalRecord {
    fn from(row: &mut Row<'_>) -> Self {
        let mode: String = row.get("mode");
        Self {
            job_id: row.get::<i64>("job_id") as u32,
            incarnation: row.get("incarnation"),
            prior_revision: row.get::<i64>("prior_revision") as u64,
            applied_revision: row.get::<i64>("applied_revision") as u64,
            mode: match mode.as_str() {
                "delete_files" => RemovalMode::DeleteFiles,
                "completed_retirement" => RemovalMode::CompletedRetirement,
                _ => RemovalMode::KeepFiles,
            },
            at_ms: row.get("at_ms"),
        }
    }
}
impl ControlStore {
    pub(crate) async fn cancellation_removal(
        &self,
        job: u32,
        incarnation: &str,
    ) -> Result<Option<RemovalRecord>, String> {
        Ok(self.client.query_consistent_map::<RemovalRecord,_>("SELECT r.job_id,r.incarnation,r.prior_revision,r.applied_revision,r.mode,r.at_ms FROM cluster_job_removals r JOIN control_jobs j ON j.job_id=r.job_id AND j.incarnation=r.incarnation AND j.revision=r.applied_revision AND j.deleted=1 WHERE r.job_id=?1 AND r.incarnation=?2",params!(i64::from(job),incarnation)).await.map_err(|error|error.to_string())?.into_iter().next())
    }
}

struct SealedProof {
    manifest: crate::worker::GenerationManifest,
    job: nzbd_types::Job,
    manifest_bytes: Vec<u8>,
    job_bytes: Vec<u8>,
}
fn bounded_identity(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}
fn validate_request(request: &CancellationRequest) -> Result<(), String> {
    if request.lifecycle_protocol < 2
        || request.job_id == 0
        || request.expected_job_revision == 0
        || request.expected_job_revision > i64::MAX as u64
        || request.token.fence == 0
        || request.token.fence > i64::MAX as u64
        || request.token.revision == 0
        || request.token.revision > i64::MAX as u64
        || request.node != request.token.owner_node_id
        || request.lease_id != format!("{}@{}", request.token.resource, request.token.fence)
    {
        return Err("cancellation requires an exact current-protocol job/grant identity".into());
    }
    for (value, max) in [
        (&request.node, 128),
        (&request.job_incarnation, 256),
        (&request.token.resource, 256),
        (&request.token.owner_incarnation, 128),
        (&request.result_id, 128),
        (&request.result_ref, 4096),
        (&request.receipt_id, 256),
    ] {
        if !bounded_identity(value, max) {
            return Err("cancellation identity exceeds supported bounds".into());
        }
    }
    Ok(())
}
fn sealed_proof(
    generations_root: &std::path::Path,
    request: &CancellationRequest,
) -> Result<SealedProof, String> {
    use sha2::{Digest, Sha256};
    use std::path::{Component, Path};
    validate_request(request)?;
    let resource_key = format!("{:x}", Sha256::digest(request.token.resource.as_bytes()));
    let expected = generations_root
        .join(format!("job-{}", request.job_id))
        .join(format!("work-{}", &resource_key[..16]))
        .join(format!("fence-{}", request.token.fence));
    if !generations_root.is_absolute() || Path::new(&request.result_ref) != expected {
        return Err("sealed result is outside its exact configured generation slot".into());
    }
    let canonical_root = generations_root
        .canonicalize()
        .map_err(|error| error.to_string())?;
    let canonical = expected.canonicalize().map_err(|error| error.to_string())?;
    if !canonical.starts_with(&canonical_root) {
        return Err("sealed generation escaped configured root".into());
    }
    let observed = crate::worker::generation_directory_identity(&expected)?;
    for name in ["manifest.json", "job.json"] {
        let metadata =
            std::fs::symlink_metadata(expected.join(name)).map_err(|error| error.to_string())?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err("sealed descriptor changed type".into());
        }
    }
    let manifest_bytes = crate::worker::read_bounded_file(
        &expected.join("manifest.json"),
        crate::proto::MAX_DESCRIPTOR_BYTES,
    )?;
    let manifest: crate::worker::GenerationManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|error| error.to_string())?;
    if manifest.identity_version < 2
        || !manifest.identity_matches()
        || manifest.job_id != request.job_id
        || manifest.job_incarnation != request.job_incarnation
        || manifest.owner_node_id != request.node
        || manifest.fence != request.token.fence
        || manifest.result_id != request.result_id
        || manifest.directory_identity.is_none()
    {
        return Err("sealed result does not match exact owned cancellation proof".into());
    }
    let job_bytes = crate::worker::read_bounded_file(
        &expected.join("job.json"),
        crate::proto::MAX_DESCRIPTOR_BYTES,
    )?;
    if format!("{:x}", Sha256::digest(&job_bytes)) != manifest.job_sha256 {
        return Err("sealed job digest changed".into());
    }
    let job: nzbd_types::Job =
        serde_json::from_slice(&job_bytes).map_err(|error| error.to_string())?;
    let context = nzbd_state::artifacts::WorkerControlContext::for_job(&job)
        .ok_or("sealed job lacks exact worker control context")?;
    if job.id.0 != request.job_id
        || context.job_incarnation != request.job_incarnation
        || context.job_revision != request.expected_job_revision
        || context.lease_id != request.lease_id
        || context.control_revision < context.job_revision
    {
        return Err("sealed worker context differs from cancellation grant".into());
    }
    let mut paths = std::collections::BTreeSet::new();
    let mut total = 0u64;
    for file in &manifest.files {
        if file.path.is_empty()
            || !Path::new(&file.path)
                .components()
                .all(|part| matches!(part, Component::Normal(_)))
            || !paths.insert(file.path.as_str())
            || file.sha256.len() != 64
            || !file.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(
                "sealed cancellation manifest has unsafe or duplicate file identity".into(),
            );
        }
        total = total
            .checked_add(file.bytes)
            .ok_or("sealed cancellation file size overflow")?;
    }
    if paths.len() > 100_000
        || total != manifest.total_bytes
        || crate::worker::generation_directory_identity(&expected)? != observed
    {
        return Err("sealed cancellation membership or directory changed".into());
    }
    for copy in manifest.artifact_copy.iter() {
        if copy.job_id != request.job_id
            || !bounded_identity(&copy.storage_instance_id, 128)
            || !bounded_identity(&copy.source.artifact, 256)
            || !bounded_identity(&copy.source.generation, 256)
            || !bounded_identity(&copy.publication.artifact, 256)
            || !bounded_identity(&copy.publication.generation, 256)
        {
            return Err("sealed artifact copy identity is invalid".into());
        }
    }
    Ok(SealedProof {
        manifest,
        job,
        manifest_bytes,
        job_bytes,
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CancellationDecision {
    pub receipt_id: String,
    pub cleanup_id: String,
    pub accepted_at_ms: i64,
    pub disposition: String,
    pub reason: String,
}
struct DecisionRow {
    fingerprint: String,
    data: String,
}
impl From<&mut Row<'_>> for DecisionRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            fingerprint: row.get("fingerprint"),
            data: row.get("data"),
        }
    }
}
fn fingerprint(request: &CancellationRequest) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    Ok(format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                request.job_id,
                &request.job_incarnation,
                &request.node,
                &request.lease_id,
                &request.token.resource,
                &request.token.owner_incarnation,
                request.token.fence,
                request.expected_job_revision,
                &request.result_id,
                &request.result_ref,
                &request.receipt_id
            ))
            .map_err(|error| error.to_string())?
        )
    ))
}
async fn existing_decision(
    control: &ControlStore,
    request: &CancellationRequest,
) -> Result<Option<CancellationDecision>, String> {
    let rows = control
        .client
        .query_consistent_map::<DecisionRow, _>(
            "SELECT fingerprint,data FROM cluster_cancellation_decisions WHERE receipt_id=?1",
            params!(&request.receipt_id),
        )
        .await
        .map_err(|error| error.to_string())?;
    match rows.into_iter().next() {
        Some(row) => {
            if row.fingerprint != fingerprint(request)? {
                return Err("cancellation receipt replay changed exact result identity".into());
            }
            Ok(Some(
                serde_json::from_str(&row.data).map_err(|error| error.to_string())?,
            ))
        }
        None => Ok(None),
    }
}
fn same_copy(
    left: &crate::delivery::AuthoritySourceCopy,
    right: &crate::delivery::AuthoritySourceCopy,
) -> bool {
    left.owner_node == right.owner_node && left.copy == right.copy
}
fn authority_source(
    job: &nzbd_types::Job,
) -> Result<Option<crate::delivery::AuthoritySourceCopy>, String> {
    let mut values = job
        .params
        .iter()
        .filter(|(key, _)| key == crate::delivery::AUTHORITY_COPY_PARAM);
    let Some((_, value)) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() || value.len() > 8192 {
        return Err("sealed authority source binding is ambiguous or oversized".into());
    }
    let source: crate::delivery::AuthoritySourceCopy =
        serde_json::from_str(value).map_err(|error| error.to_string())?;
    if source.copy.job_id != job.id.0
        || source.copy.selected
        || source.copy.source != source.copy.publication
        || !bounded_identity(&source.owner_node, 128)
        || !bounded_identity(&source.copy.storage_instance_id, 128)
        || !bounded_identity(&source.copy.source.artifact, 256)
        || !bounded_identity(&source.copy.source.generation, 256)
    {
        return Err("sealed authority source binding is invalid".into());
    }
    Ok(Some(source))
}
fn validate_grant_token(
    request: &CancellationRequest,
    grant: &crate::grant_receipts::HistoricalGrant,
) -> Result<(), String> {
    let original = &grant.initial_token;
    if grant.job_id != request.job_id
        || grant.job_incarnation != request.job_incarnation
        || grant.job_revision != request.expected_job_revision
        || original.resource != request.token.resource
        || original.owner_node_id != request.node
        || original.owner_incarnation != request.token.owner_incarnation
        || original.fence != request.token.fence
        || request.token.revision < original.revision
        || request.token.expires_at_unix_ms < original.expires_at_unix_ms
    {
        return Err("cancellation does not match its immutable grant token".into());
    }
    Ok(())
}
async fn prove_grant(
    control: &ControlStore,
    request: &CancellationRequest,
    proof: &SealedProof,
    removal: &RemovalRecord,
) -> Result<(), String> {
    let grant = control
        .historical_grant(&request.token.resource, request.token.fence)
        .await?
        .ok_or("historical grant proof is unavailable; retain the sealed result for review")?;
    validate_grant_token(request, &grant)?;
    let original = &grant.initial_token;
    if grant.job_id != request.job_id
        || grant.job_incarnation != request.job_incarnation
        || grant.job_revision != request.expected_job_revision
        || grant.job_revision > removal.prior_revision
        || original.resource != request.token.resource
        || original.owner_node_id != request.node
        || original.owner_incarnation != request.token.owner_incarnation
        || original.fence != request.token.fence
        || request.token.revision < original.revision
        || request.token.expires_at_unix_ms < original.expires_at_unix_ms
        || !matches!(grant.kind.as_str(), "download" | "post" | "assemble")
        || grant.issued_at_unix_ms > removal.at_ms
    {
        return Err("cancellation does not match the immutable historical grant".into());
    }
    let context = nzbd_state::artifacts::WorkerControlContext::for_job(&proof.job)
        .ok_or("missing sealed control context")?;
    if context.control_revision > removal.prior_revision {
        return Err("sealed control context postdates removal".into());
    }
    let sealed = authority_source(&proof.job)?;
    let historical: Option<crate::delivery::AuthoritySourceCopy> = grant
        .source_binding_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| error.to_string())?;
    if !match (&sealed, &historical) {
        (None, None) => true,
        (Some(a), Some(b)) => same_copy(a, b),
        _ => false,
    } {
        return Err("sealed authority source differs from immutable grant binding".into());
    }
    Ok(())
}

fn cancellation_record(
    request: &CancellationRequest,
    proof: &SealedProof,
    removal: &RemovalRecord,
    decision: &CancellationDecision,
    instance: String,
) -> Result<crate::delivery::ClusterDelivery, String> {
    use nzbd_state::artifacts::DeliveryFile;
    use sha2::{Digest, Sha256};
    let removing = removal.mode == RemovalMode::DeleteFiles;
    let files: Vec<DeliveryFile> = proof
        .manifest
        .files
        .iter()
        .map(|file| DeliveryFile {
            path: file.path.clone(),
            size_bytes: file.bytes,
            sha256: file.sha256.clone(),
        })
        .collect();
    let mut sealed_files: Vec<DeliveryFile> = files
        .iter()
        .map(|file| DeliveryFile {
            path: format!("files/{}", file.path),
            ..file.clone()
        })
        .collect();
    sealed_files.push(DeliveryFile {
        path: "manifest.json".into(),
        size_bytes: proof.manifest_bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(&proof.manifest_bytes)),
    });
    sealed_files.push(DeliveryFile {
        path: "job.json".into(),
        size_bytes: proof.job_bytes.len() as u64,
        sha256: proof.manifest.job_sha256.clone(),
    });
    let mut copies = std::collections::BTreeMap::new();
    for (owner, copy) in proof
        .manifest
        .artifact_copy
        .iter()
        .map(|copy| (request.node.clone(), copy.clone()))
        .chain(
            authority_source(&proof.job)?
                .into_iter()
                .map(|source| (source.owner_node, source.copy)),
        )
    {
        let id = format!(
            "artifact:{}:{}:{}",
            copy.storage_instance_id, copy.publication.artifact, copy.publication.generation
        );
        let reason = if !removing {
            "Removal keeps these source files; no cleanup was authorized."
        } else if copy.selected {
            "Selected publication may have separate consumer custody; review its exact receipt before removal."
        } else {
            "Waiting for exact source-owner confirmation that all writers stopped and its current files were sealed."
        };
        let obligation = crate::delivery::CopyObligation {
            id: id.clone(),
            owner_node: owner,
            artifact: Some(copy),
            local_delivery_id: None,
            result_ref: None,
            result_id: None,
            directory_identity: None,
            deletion_files: None,
            status: "review_required".into(),
            error: Some(reason.into()),
            claim_id: None,
            attempts: 0,
            next_retry_at: None,
        };
        if let Some(old) = copies.insert(id, obligation.clone()) {
            if old != obligation {
                return Err("conflicting owned copy identities in cancellation proof".into());
            }
        }
    }
    let id = format!("generation:{}", request.result_id);
    copies.insert(
        id.clone(),
        crate::delivery::CopyObligation {
            id,
            owner_node: request.node.clone(),
            artifact: None,
            local_delivery_id: None,
            result_ref: Some(request.result_ref.clone()),
            result_id: Some(request.result_id.clone()),
            directory_identity: proof.manifest.directory_identity,
            deletion_files: Some(sealed_files),
            status: if removing {
                "pending"
            } else {
                "review_required"
            }
            .into(),
            error: (!removing).then(|| {
                "Removal keeps this sealed result; retained for recovery or explicit review.".into()
            }),
            claim_id: None,
            attempts: 0,
            next_retry_at: None,
        },
    );
    let source = proof
        .manifest
        .artifact_copy
        .as_ref()
        .map(|copy| copy.source.clone());
    Ok(crate::delivery::ClusterDelivery {
        revision: 0,
        receipt_id: request.receipt_id.clone(),
        result_id: request.result_id.clone(),
        cleanup_only: true,
        source_disposition: Some(
            if removing {
                "cancellation_delete_files"
            } else {
                "cancellation_keep_files"
            }
            .into(),
        ),
        copies: copies.into_values().collect(),
        delivery: nzbd_state::artifacts::Delivery {
            protocol_version: 1,
            sequence: 0,
            status: "internal_cleanup".into(),
            delivery_id: decision.cleanup_id.clone(),
            publication_generation: request.result_id.clone(),
            artifact_id: format!("cancelled-generation:{}", request.result_id),
            source_artifact_id: source
                .as_ref()
                .map(|source| source.artifact.clone())
                .unwrap_or_default(),
            source_generation: source.map(|source| source.generation).unwrap_or_default(),
            job_id: request.job_id,
            runner_instance_id: instance,
            request_token: proof
                .job
                .params
                .iter()
                .find(|(key, _)| key == "monarr-transfer")
                .map(|(_, value)| value.clone()),
            keep_source_on_failure: proof
                .job
                .params
                .iter()
                .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1"),
            terminal_outcome: "CANCELLED".into(),
            failure_stage: None,
            failure_cause: Some(decision.reason.clone()),
            publication_path: String::new(),
            manifest_digest: fingerprint(request)?,
            files: Vec::new(),
            file_outcomes: Vec::new(),
            runner_stages: None,
            lifecycle_timing: None,
            ready_at: decision.accepted_at_ms / 1000,
            accepted_at: None,
            consumer_receipt_id: None,
            source_cleanup:serde_json::from_value(serde_json::json!({"status":if removing{"pending"}else{"retained"},"error":decision.reason,"attempts":0,"nextRetryAt":null,"blockedSince":null,"warning":null,"startedAt":null,"lastAttemptAt":null,"completedAt":null,"attemptHistory":[]})).map_err(|error|error.to_string())?,
        },
    })
}

/// The route authenticates request.node before entering this helper. The issuer
/// is captured once by its current authority handler and fenced in the commit.
pub(crate) async fn decide(
    control: &ControlStore,
    generations_root: &std::path::Path,
    request: &CancellationRequest,
    issuer: (&str, &str, u64),
) -> Result<CancellationDecision, String> {
    validate_request(request)?;
    if let Some(decision) = existing_decision(control, request).await? {
        let grant = control
            .historical_grant(&request.token.resource, request.token.fence)
            .await?
            .ok_or("historical grant receipt is missing from cancellation replay")?;
        validate_grant_token(request, &grant)?;
        let record = control
            .cluster_delivery(&decision.cleanup_id)
            .await?
            .ok_or("cancellation replay has no durable private cleanup ledger")?;
        if !record.cleanup_only
            || record.receipt_id != request.receipt_id
            || record.result_id != request.result_id
        {
            return Err("cancellation replay ledger changed identity".into());
        }
        return Ok(decision);
    }
    let removal = control
        .cancellation_removal(request.job_id, &request.job_incarnation)
        .await?
        .ok_or("exact durable user removal is unavailable; retain the sealed result for review")?;
    if removal.mode == RemovalMode::CompletedRetirement {
        return Err("completed history retirement grants no cancellation cleanup authority".into());
    }
    let root = generations_root.to_owned();
    let req = request.clone();
    let proof = tokio::task::spawn_blocking(move || sealed_proof(&root, &req))
        .await
        .map_err(|error| error.to_string())??;
    prove_grant(control, request, &proof, &removal).await?;
    let id = format!("cancellation:{}", fingerprint(request)?);
    let now = crate::control::unix_ms_public();
    let decision=CancellationDecision{receipt_id:request.receipt_id.clone(),cleanup_id:id,accepted_at_ms:now,disposition:if removal.mode==RemovalMode::DeleteFiles{"cleanup_pending"}else{"retained"}.into(),reason:if removal.mode==RemovalMode::DeleteFiles{"Exact removal was recorded. Cleanup is authorized only for this sealed result and exact source copies whose owners confirm quiescence and their existing owned file identity."}else{"The removal requested that files be kept. This receipt authorizes no byte cleanup; review or recover the exact recorded copies separately."}.into()};
    let record = cancellation_record(
        request,
        &proof,
        &removal,
        &decision,
        control.delivery_instance().await?,
    )?;
    let (data, public) = crate::delivery::encode_record(&record)?;
    let identity = fingerprint(request)?;
    let writes=vec![crate::control::authority_statement(issuer)?,
        ("INSERT INTO cluster_cancellation_decisions(receipt_id,fingerprint,job_id,incarnation,removal_revision,cleanup_id,data) SELECT ?1,?2,?3,?4,?5,?6,?7 WHERE NOT EXISTS(SELECT 1 FROM cluster_deliveries WHERE cleanup_only=0 AND json_extract(data,'$.result_id')=?8) ON CONFLICT(receipt_id) DO NOTHING".into(),params!(&request.receipt_id,&identity,i64::from(request.job_id),&request.job_incarnation,removal.applied_revision as i64,&decision.cleanup_id,serde_json::to_string(&decision).map_err(|error|error.to_string())?,&request.result_id)),
        ("INSERT INTO cluster_deliveries(id,data,public_data,accepted_at,cleanup_only,request_token) SELECT ?1,?2,?3,?4,1,?5 WHERE EXISTS(SELECT 1 FROM cluster_cancellation_decisions WHERE receipt_id=?6 AND fingerprint=?7 AND cleanup_id=?1) ON CONFLICT(id) DO NOTHING".into(),params!(&decision.cleanup_id,data,public,record.delivery.accepted_at,&record.delivery.request_token,&request.receipt_id,&identity))];
    crate::delivery::bounded_transaction(&writes)?;
    control
        .client
        .txn(writes)
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let committed = existing_decision(control, request).await?.ok_or(
        "this result already has consumer publication custody; cancellation cannot replace it",
    )?;
    let current = control
        .cluster_delivery(&committed.cleanup_id)
        .await?
        .ok_or("cancellation decision has no durable private cleanup ledger")?;
    if !current.cleanup_only
        || current.receipt_id != request.receipt_id
        || current.result_id != request.result_id
        || current.delivery.manifest_digest != identity
    {
        return Err("cancellation private cleanup identity changed".into());
    }
    Ok(committed)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OwnerStopAck {
    pub cleanup_id: String,
    pub copy_id: String,
    pub job_id: u32,
    pub job_incarnation: String,
    pub removal_applied_revision: u64,
    pub copy: nzbd_state::artifacts::ClusterArtifactCopy,
    pub files: Vec<nzbd_state::artifacts::DeliveryFile>,
}
struct StopRow {
    digest: String,
}
impl From<&mut Row<'_>> for StopRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            digest: row.get("digest"),
        }
    }
}
/// Only an authenticated exact source owner may call this AFTER joining every
/// writer for this generation and obtaining its existing owned byte seal. A new
/// directory walk, queue absence or lease expiry is not that proof. The caller
/// keeps active-use, recovery-dependency and unknown-member guards. This exact
/// explicit DeleteFiles scope overrides Keep only for its selected raw copy.
pub(crate) async fn acknowledge_owner_stop(
    control: &ControlStore,
    ack: &OwnerStopAck,
    authenticated_owner: &str,
    issuer: (&str, &str, u64),
) -> Result<bool, String> {
    use sha2::{Digest, Sha256};
    if !bounded_identity(authenticated_owner, 128)
        || !bounded_identity(&ack.cleanup_id, 256)
        || !bounded_identity(&ack.copy_id, 1024)
        || !bounded_identity(&ack.job_incarnation, 256)
        || ack.files.len() > 100_000
        || ack.copy.job_id != ack.job_id
        || ack.copy.selected
        || ack.copy.source != ack.copy.publication
    {
        return Err("owner-stop acknowledgment has invalid exact source identity".into());
    }
    let mut record = control
        .cluster_delivery(&ack.cleanup_id)
        .await?
        .ok_or("cancellation cleanup ledger is missing")?;
    if !record.cleanup_only
        || record.source_disposition.as_deref() != Some("cancellation_delete_files")
        || record.delivery.job_id != ack.job_id
    {
        return Err("this receipt grants no cancellation byte cleanup".into());
    }
    let removal = control
        .cancellation_removal(ack.job_id, &ack.job_incarnation)
        .await?
        .ok_or("owner stop has no current exact removal")?;
    if removal.mode != RemovalMode::DeleteFiles
        || removal.applied_revision != ack.removal_applied_revision
    {
        return Err("owner-stop removal scope changed".into());
    }
    let index = record
        .copies
        .iter()
        .position(|copy| copy.id == ack.copy_id)
        .ok_or("owner stop is not a recorded copy obligation")?;
    let current = &record.copies[index];
    if current.owner_node != authenticated_owner || current.artifact.as_ref() != Some(&ack.copy) {
        return Err("owner stop belongs to another source owner or generation".into());
    }
    if current.status == "removed" {
        return Ok(false);
    }
    if current.status == "running" {
        return Err("copy cleanup is executing; retry owner confirmation after it settles".into());
    }
    let mut metadata_bytes = 0usize;
    for file in &ack.files {
        metadata_bytes = metadata_bytes
            .checked_add(file.path.len())
            .and_then(|bytes| bytes.checked_add(file.sha256.len() + 128))
            .ok_or("owner stop metadata size overflow")?;
        if metadata_bytes > crate::proto::MAX_DESCRIPTOR_BYTES {
            return Err("owner stop file seal exceeds descriptor bounds; retain for review".into());
        }
    }
    let mut files = ack.files.clone();
    files.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
    let mut seen = std::collections::BTreeSet::new();
    for file in &files {
        if file.path.is_empty()
            || !std::path::Path::new(&file.path)
                .components()
                .all(|part| matches!(part, std::path::Component::Normal(_)))
            || !seen.insert(&file.path)
            || file.sha256.len() != 64
            || !file.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err("owner stop lacks a safe exact file seal".into());
        }
    }
    let raw = serde_json::to_vec(&files).map_err(|error| error.to_string())?;
    if raw.len() > crate::proto::MAX_DESCRIPTOR_BYTES {
        return Err("owner stop file seal exceeds descriptor bounds; retain for review".into());
    }
    let digest = format!("{:x}", Sha256::digest(raw));
    let prior=control.client.query_consistent_map::<StopRow,_>("SELECT digest FROM cluster_cancellation_owner_stops WHERE cleanup_id=?1 AND owner_node=?2 AND storage_instance=?3 AND artifact=?4 AND generation=?5",params!(&ack.cleanup_id,authenticated_owner,&ack.copy.storage_instance_id,&ack.copy.source.artifact,&ack.copy.source.generation)).await.map_err(|error|error.to_string())?.into_iter().next();
    if let Some(prior) = prior {
        if prior.digest != digest {
            return Err("owner-stop replay changed its owned byte seal; review required".into());
        }
        if current.deletion_files.as_ref() == Some(&files) {
            return Ok(false);
        }
    }
    record.copies[index].deletion_files = Some(files);
    record.copies[index].status = "pending".into();
    record.copies[index].claim_id = None;
    record.copies[index].error = None;
    record.copies[index].next_retry_at = None;
    let (data, public) = crate::delivery::encode_record(&record)?;
    let writes=vec![crate::control::authority_statement(issuer)?,
        ("INSERT INTO cluster_cancellation_owner_stops(cleanup_id,owner_node,storage_instance,artifact,generation,job_id,incarnation,removal_revision,at_ms,digest) SELECT ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10 WHERE EXISTS(SELECT 1 FROM cluster_cancellation_decisions WHERE cleanup_id=?1 AND job_id=?6 AND incarnation=?7 AND removal_revision=?8) AND EXISTS(SELECT 1 FROM cluster_deliveries WHERE id=?1 AND revision=?11) ON CONFLICT(cleanup_id,owner_node,storage_instance,artifact,generation) DO NOTHING".into(),params!(&ack.cleanup_id,authenticated_owner,&ack.copy.storage_instance_id,&ack.copy.source.artifact,&ack.copy.source.generation,i64::from(ack.job_id),&ack.job_incarnation,removal.applied_revision as i64,crate::control::unix_ms_public(),&digest,record.revision)),
        ("UPDATE cluster_deliveries SET data=?2,public_data=?3,revision=revision+1 WHERE id=?1 AND revision=?4 AND EXISTS(SELECT 1 FROM cluster_cancellation_owner_stops WHERE cleanup_id=?1 AND owner_node=?5 AND storage_instance=?6 AND artifact=?7 AND generation=?8 AND digest=?9)".into(),params!(&ack.cleanup_id,data,public,record.revision,authenticated_owner,&ack.copy.storage_instance_id,&ack.copy.source.artifact,&ack.copy.source.generation,&digest))];
    crate::delivery::bounded_transaction(&writes)?;
    let results = control
        .client
        .txn(writes)
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    if results.last().copied() != Some(1) {
        return Err("cancellation copy changed during owner confirmation; reload and retry".into());
    }
    Ok(true)
}

#[cfg(test)]
#[path = "cancellation_tests.rs"]
mod tests;
