//! Replicated deletion of one consumer admission. The queue tombstone fences
//! new grants; exact owner stops and the existing copy ledger prove disposal.
use crate::{
    cancellation::RemovalMode,
    control::{authority_statement, ControlStore},
    delivery::{AuthoritySourceCopy, ClusterDelivery, CopyObligation},
    grant_receipts::HistoricalGrant,
    leader::LeaderShared,
};
use axum::{
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use hiqlite::{params, Row};
use nzbd_state::artifacts::{
    ClusterArtifactCopy, JobDispositionRequest, JobDispositionState, WorkerAssemblyRetirement,
    WorkerStoppedReceipt,
};
use nzbd_types::JobId;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS cluster_job_dispositions(
         sequence INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT NOT NULL UNIQUE,
         job_id INTEGER NOT NULL, incarnation TEXT NOT NULL, removal_revision INTEGER NOT NULL,
         transfer TEXT NOT NULL, generation TEXT NOT NULL, instance TEXT NOT NULL,
         data TEXT NOT NULL CHECK(length(CAST(data AS BLOB))<=1048576),
         revision INTEGER NOT NULL DEFAULT 1, verified INTEGER NOT NULL DEFAULT 0,
         UNIQUE(job_id,incarnation)) STRICT",
    "CREATE TRIGGER IF NOT EXISTS cluster_job_disposition_exact BEFORE INSERT ON cluster_job_dispositions BEGIN
         SELECT CASE WHEN NOT EXISTS(
             SELECT 1 FROM control_jobs j JOIN cluster_job_removals r
               ON r.job_id=j.job_id AND r.incarnation=j.incarnation AND r.applied_revision=j.revision
             JOIN request_admissions a ON a.job_id=j.job_id AND a.token=NEW.transfer
             JOIN cluster_delivery_meta m ON m.key='instance' AND m.value=NEW.instance
             WHERE j.job_id=NEW.job_id AND j.incarnation=NEW.incarnation
               AND j.revision=NEW.removal_revision AND j.deleted=1
               AND json_extract(a.data,'$.generation')=NEW.generation
               AND json_extract(a.data,'$.runnerInstanceId')=NEW.instance
               AND EXISTS(SELECT 1 FROM json_each(j.intent_json,'$.params') p
                          WHERE json_extract(p.value,'$[0]')='monarr-transfer'
                            AND json_extract(p.value,'$[1]')=NEW.transfer))
         THEN RAISE(ABORT,'job disposition requires exact committed admission and tombstone') END;
     END",
    "CREATE TRIGGER IF NOT EXISTS cluster_job_disposition_fence BEFORE UPDATE ON control_jobs
     BEGIN
         SELECT CASE WHEN EXISTS(SELECT 1 FROM cluster_job_dispositions d
            WHERE d.job_id=OLD.job_id AND d.incarnation=OLD.incarnation)
         THEN RAISE(ABORT,'job has an immutable deletion disposition') END;
     END",
    "CREATE TRIGGER IF NOT EXISTS cluster_job_disposition_preserve BEFORE DELETE ON control_jobs
     BEGIN
         SELECT CASE WHEN EXISTS(SELECT 1 FROM cluster_job_dispositions d
            WHERE d.job_id=OLD.job_id AND d.incarnation=OLD.incarnation)
         THEN RAISE(ABORT,'job has an immutable deletion disposition') END;
     END",
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

#[derive(Clone, Serialize, Deserialize)]
struct Disposition {
    request: JobDispositionRequest,
    owner: String,
    storage: String,
    source: Option<AuthoritySourceCopy>,
    owners: BTreeMap<String, OwnerObservation>,
    owners_joined: bool,
    grants: BTreeMap<String, GrantObservation>,
    state: JobDispositionState,
    reason: Option<String>,
}

struct Receipt {
    sequence: i64,
    job: u32,
    incarnation: String,
    removal_revision: u64,
    revision: i64,
    data: Disposition,
}
struct ReceiptRow {
    sequence: i64,
    job: i64,
    incarnation: String,
    removal_revision: i64,
    revision: i64,
    data: String,
}
impl From<&mut Row<'_>> for ReceiptRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            sequence: row.get("sequence"),
            job: row.get("job_id"),
            incarnation: row.get("incarnation"),
            removal_revision: row.get("removal_revision"),
            revision: row.get("revision"),
            data: row.get("data"),
        }
    }
}
impl TryFrom<ReceiptRow> for Receipt {
    type Error = String;
    fn try_from(row: ReceiptRow) -> Result<Self, String> {
        Ok(Self {
            sequence: row.sequence,
            job: u32::try_from(row.job).map_err(|error| error.to_string())?,
            incarnation: row.incarnation,
            removal_revision: u64::try_from(row.removal_revision)
                .map_err(|error| error.to_string())?,
            revision: row.revision,
            data: serde_json::from_str(&row.data).map_err(|error| error.to_string())?,
        })
    }
}

struct JobRow {
    job: String,
    incarnation: String,
    revision: i64,
    deleted: bool,
    result_id: Option<String>,
}
impl From<&mut Row<'_>> for JobRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            job: row.get("intent_json"),
            incarnation: row.get("incarnation"),
            revision: row.get("revision"),
            deleted: row.get::<i64>("deleted") != 0,
            result_id: row.get("result_id"),
        }
    }
}
impl ControlStore {
    async fn disposition(&self, id: &str) -> Result<Option<Receipt>, String> {
        self.client
            .query_consistent_map::<ReceiptRow, _>(
                "SELECT sequence,job_id,incarnation,removal_revision,revision,data
             FROM cluster_job_dispositions WHERE request_id=?1",
                params!(id),
            )
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .next()
            .map(TryInto::try_into)
            .transpose()
    }

    async fn next_disposition(&self, cursor: i64) -> Result<Option<Receipt>, String> {
        self.client.query_consistent_map::<ReceiptRow, _>(
            "SELECT sequence,job_id,incarnation,removal_revision,revision,data
             FROM cluster_job_dispositions WHERE sequence>?1 AND verified=0 ORDER BY sequence LIMIT 1",
            params!(cursor),
        ).await.map_err(|error| error.to_string())?.into_iter().next().map(TryInto::try_into).transpose()
    }

    async fn disposition_job(&self, job: u32) -> Result<Option<JobRow>, String> {
        self.client.query_consistent_map::<JobRow, _>(
            "SELECT CASE WHEN length(CAST(intent_json AS BLOB))<=?2 THEN intent_json ELSE '' END AS intent_json,
                    incarnation,revision,deleted,result_id FROM control_jobs WHERE job_id=?1",
            params!(i64::from(job), crate::proto::MAX_DESCRIPTOR_BYTES as i64),
        ).await.map_err(|error| error.to_string()).map(|rows| rows.into_iter().next())
    }

    async fn save_disposition(
        &self,
        record: &Receipt,
        authority: (&str, &str, u64),
    ) -> Result<(), String> {
        let data = serde_json::to_string(&record.data).map_err(|error| error.to_string())?;
        let writes = vec![
            authority_statement(authority)?,
            (
                "UPDATE cluster_job_dispositions SET data=?1,revision=revision+1,verified=?2
              WHERE sequence=?3 AND revision=?4 AND verified=0"
                    .into(),
                params!(
                    data,
                    i64::from(record.data.state == JobDispositionState::Verified),
                    record.sequence,
                    record.revision
                ),
            ),
        ];
        self.client
            .txn(writes)
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

pub(crate) fn job_id(request: &Request) -> Option<u32> {
    if request.method() != Method::POST {
        return None;
    }
    request
        .uri()
        .path()
        .strip_prefix("/api/v1/jobs/")?
        .strip_suffix("/disposition")?
        .parse()
        .ok()
}

fn failure(status: StatusCode, why: impl AsRef<str>) -> Response {
    (status, Json(json!({"error":why.as_ref()}))).into_response()
}

fn status(record: Receipt) -> Response {
    let request = &record.data.request;
    let code = if record.data.state == JobDispositionState::Pending {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    (
        code,
        Json(json!({
            "requestId":request.request_id, "jobId":record.job.to_string(),
            "transfer":request.transfer, "runnerInstanceId":request.runner_instance_id,
            "generation":request.generation, "state":record.data.state,
            "reason":record.data.reason, "operationId":operation_id(&request.request_id),
        })),
    )
        .into_response()
}

fn operation_id(id: &str) -> String {
    format!("job-disposition:{:x}", Sha256::digest(id.as_bytes()))
}

pub(crate) async fn serve(
    State(shared): State<Arc<LeaderShared>>,
    request: Request,
    next: Next,
) -> Response {
    let Some(job) = job_id(&request) else {
        return next.run(request).await;
    };
    let Ok(body) = axum::body::to_bytes(request.into_body(), 8192).await else {
        return failure(
            StatusCode::PAYLOAD_TOO_LARGE,
            "job disposition request exceeds its size limit",
        );
    };
    let request = match serde_json::from_slice::<JobDispositionRequest>(&body) {
        Ok(request) if job != 0 && request.valid() => request,
        _ => return failure(StatusCode::BAD_REQUEST, "invalid job disposition identity"),
    };
    let Some(control) = &shared.control else {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "replicated control unavailable",
        );
    };
    if !shared.view.borrow().is_me || !shared.authority_ready() || !control.is_healthy().await {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "replicated disposition authority unavailable",
        );
    }
    let Ok(_serial) =
        tokio::time::timeout(Duration::from_secs(2), shared.mutation_serial.lock()).await
    else {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "replicated control is busy; retry this request",
        );
    };
    match admit(&shared, control, job, request).await {
        Ok(record) => status(record),
        Err((code, reason)) => failure(code, reason),
    }
}

type AdmissionError = (StatusCode, String);

fn unavailable(error: impl ToString) -> AdmissionError {
    (StatusCode::SERVICE_UNAVAILABLE, error.to_string())
}
fn conflict(error: impl ToString) -> AdmissionError {
    (StatusCode::CONFLICT, error.to_string())
}

async fn admit(
    shared: &LeaderShared,
    control: &ControlStore,
    job: u32,
    request: JobDispositionRequest,
) -> Result<Receipt, AdmissionError> {
    if let Some(record) = control
        .disposition(&request.request_id)
        .await
        .map_err(unavailable)?
    {
        if record.job != job || record.data.request != request {
            return Err(conflict("disposition request ID has a different binding"));
        }
        return Ok(record);
    }
    let instance = control.delivery_instance().await.map_err(unavailable)?;
    let admission = control
        .admission(&request.transfer)
        .await
        .map_err(unavailable)?
        .filter(|admission| {
            admission.job_id == job
                && admission.generation == request.generation
                && admission.runner_instance_id == request.runner_instance_id
                && instance == request.runner_instance_id
        })
        .ok_or_else(|| conflict("transfer, Runner instance or admission generation changed"))?;
    let observed_source = admission
        .source_artifact_id
        .zip(admission.source_generation);
    let records = control
        .disposition_deliveries(&request.transfer, job)
        .await
        .map_err(unavailable)?;
    if let Some(source) = request.source() {
        let matches_admission = observed_source.as_ref().is_some_and(|(id, generation)| {
            id == &source.artifact && generation == &source.generation
        });
        let matches_delivery = records.iter().any(|record| {
            !is_cancellation(record)
                && record.delivery.source_artifact_id == source.artifact
                && record.delivery.source_generation == source.generation
        });
        if !matches_admission && !matches_delivery {
            return Err(conflict("source artifact or generation changed"));
        }
    }
    let row = control
        .disposition_job(job)
        .await
        .map_err(unavailable)?
        .ok_or_else(|| conflict("exact durable job incarnation is unavailable"))?;
    let mut prior = crate::control::decode_intent(&row.job).map_err(unavailable)?;
    let transfers: Vec<_> = prior
        .params
        .iter()
        .filter(|(key, _)| key == "monarr-transfer")
        .collect();
    if prior.id.0 != job || transfers.len() != 1 || transfers[0].1 != request.transfer {
        return Err(conflict("numeric job handle belongs to another admission"));
    }
    // A selected post result may already have entered materialization, whose
    // filesystem work is outside mutation_serial. Let the normal READY path
    // settle ownership before admitting an explicit deletion of its source.
    if prior
        .params
        .iter()
        .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
        && row.result_id.as_ref().is_some_and(|result| {
            !records
                .iter()
                .any(|record| record.result_id == *result && !is_cancellation(record))
        })
        && !row.deleted
    {
        return Err(unavailable(
            "selected publication is awaiting its durable custody receipt",
        ));
    }
    if !row.deleted && !prior.files.is_empty() {
        crate::source_facts::bind_owned(shared, &mut prior).map_err(unavailable)?;
    }
    let source = crate::cancellation::authority_source(&prior).map_err(conflict)?;
    let data = Disposition {
        request,
        owner: shared.cfg.node_name.clone(),
        storage: shared.engine.artifacts().installation.clone(),
        source,
        owners: BTreeMap::new(),
        owners_joined: false,
        grants: BTreeMap::new(),
        state: JobDispositionState::Pending,
        reason: Some("waiting for joined owners and exact source cleanup".into()),
    };
    let request = &data.request;
    let removal_revision = if row.deleted {
        row.revision
    } else {
        row.revision
            .checked_add(1)
            .ok_or_else(|| unavailable("job revision exhausted"))?
    };
    if row.deleted
        && control
            .cancellation_removal(job, &row.incarnation)
            .await
            .map_err(unavailable)?
            .is_none()
    {
        return Err(conflict(
            "historical removal lacks an exact retirement receipt",
        ));
    }
    let statement = (
        "INSERT INTO cluster_job_dispositions(request_id,job_id,incarnation,removal_revision,transfer,generation,instance,data)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8)".into(),
        params!(&request.request_id, i64::from(job), &row.incarnation, removal_revision, &request.transfer,
                &request.generation, &request.runner_instance_id, serde_json::to_string(&data).map_err(unavailable)?),
    );
    let changes = if row.deleted {
        Vec::new()
    } else {
        vec![(u64::from(job), None)]
    };
    let result = control
        .commit_job_delta_with_statements(
            &changes,
            &std::collections::HashMap::from([(u64::from(job), row.revision as u64)]),
            &[],
            Some(shared.authority_identity()),
            &BTreeMap::from([(job, RemovalMode::DeleteFiles)]),
            vec![statement],
        )
        .await;
    if let Err(error) = result {
        if error.contains("UNIQUE constraint failed: cluster_job_dispositions") {
            return Err(conflict(
                "this admission already has another disposition request",
            ));
        }
        return Err(unavailable(error));
    }
    control
        .disposition(&request.request_id)
        .await
        .map_err(unavailable)?
        .ok_or_else(|| unavailable("job disposition commit is awaiting confirmation"))
}

fn is_cancellation(record: &ClusterDelivery) -> bool {
    record
        .source_disposition
        .as_deref()
        .is_some_and(|value| value.starts_with("cancellation_"))
}

#[derive(Clone, Serialize, Deserialize)]
struct GrantObservation {
    stopped: WorkerStoppedReceipt,
    result: Option<crate::cancellation::CancellationRequest>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct OwnerQuery {
    request_id: String,
    node: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct OwnerAuthority {
    job: u32,
    transfer: String,
    storage: String,
    protected_results: Vec<String>,
    stopped: Vec<nzbd_state::artifacts::WorkerControlContext>,
    assembly_stops: Vec<WorkerStoppedReceipt>,
    removal_revision: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct OwnerObservation {
    node: String,
    storage: String,
    candidates: Vec<nzbd_state::artifacts::SourceGeneration>,
    #[serde(default)]
    assembly_retirements: Vec<WorkerAssemblyRetirement>,
}

fn grant_key(grant: &HistoricalGrant) -> String {
    format!(
        "{}@{}",
        grant.initial_token.resource, grant.initial_token.fence
    )
}

fn grant_matches(grant: &HistoricalGrant, observed: &GrantObservation) -> bool {
    let stopped = &observed.stopped;
    stopped.job == grant.job_id
        && stopped.context.lease_id == grant_key(grant)
        && stopped.context.job_incarnation == grant.job_incarnation
        && stopped.context.job_revision == grant.job_revision
        && stopped.owner_incarnation == grant.initial_token.owner_incarnation
        && Some(stopped.storage_instance_id.as_str()) == grant.storage_instance_id.as_deref()
        && stopped.authority_source_json == grant.source_binding_json
}

fn sealed_result(
    shared: &LeaderShared,
    grant: &HistoricalGrant,
) -> Result<Option<crate::cancellation::CancellationRequest>, String> {
    let token = &grant.initial_token;
    let path = crate::worker::generation_slot(
        &shared.cfg.shared_dir,
        grant.job_id,
        &token.resource,
        token.fence,
    );
    // Missing an entire shared layout is an unavailable store, not proof
    // that a previously started worker deposited no copy.
    std::fs::metadata(shared.cfg.shared_dir.join(".nzbd-cluster"))
        .map_err(|error| format!("shared generation namespace unavailable: {error}"))?;
    match std::fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.to_string()),
        Ok(meta) if !meta.is_dir() || meta.file_type().is_symlink() => {
            return Err("sealed generation slot changed identity".into())
        }
        Ok(_) => {}
    }
    let raw = crate::worker::read_bounded_file(
        &path.join("manifest.json"),
        crate::proto::MAX_DESCRIPTOR_BYTES,
    )?;
    let manifest: crate::worker::GenerationManifest =
        serde_json::from_slice(&raw).map_err(|error| error.to_string())?;
    let result = crate::cancellation::CancellationRequest {
        lifecycle_protocol: crate::proto::LIFECYCLE_PROTOCOL,
        node: token.owner_node_id.clone(),
        job_id: grant.job_id,
        job_incarnation: grant.job_incarnation.clone(),
        lease_id: grant_key(grant),
        token: token.clone(),
        expected_job_revision: grant.job_revision,
        receipt_id: format!(
            "complete:{}:{}:{}",
            token.resource, token.fence, manifest.result_id
        ),
        result_id: manifest.result_id,
        result_ref: path
            .to_str()
            .ok_or("sealed generation path is not UTF-8")?
            .into(),
    };
    crate::cancellation::sealed_operation_finished(
        &shared.cfg.shared_dir.join(".nzbd-cluster/generations"),
        &result,
    )?;
    Ok(Some(result))
}

async fn inspect_grant(
    shared: &Arc<LeaderShared>,
    mut request: crate::cancellation_routes::StopRequest,
) -> Result<GrantObservation, String> {
    let active = shared
        .worker_leases
        .lock()
        .unwrap()
        .clone()
        .ok_or("worker execution registry unavailable")?;
    let (grant, _) = if shared.control.is_some() && shared.view.borrow().is_me {
        crate::cancellation_routes::stop_authority_exact(shared, &request).await?
    } else {
        let url = shared
            .view
            .borrow()
            .leader_url()
            .map(str::to_owned)
            .ok_or("current removal authority unavailable")?;
        crate::http::ClusterClient::new(shared.cfg.secret.clone())
            .post_json(
                &url,
                "/cluster/v1/work/cancellation-stop-authority",
                &request,
            )
            .await?
    };
    // An already-sealed result can prove an external process exited after a
    // worker restart. At this point it grants no delete authority.
    let inspect = |shared: &Arc<LeaderShared>, grant: &HistoricalGrant| {
        let shared = shared.clone();
        let grant = grant.clone();
        tokio::task::spawn_blocking(move || sealed_result(&shared, &grant))
    };
    {
        let _seal = active.sealing.lock().await;
        request.sealed = inspect(shared, &grant)
            .await
            .map_err(|error| error.to_string())??;
    }
    let stopped = crate::cancellation_routes::stop_exact(shared, &request).await?;
    // The fence is installed before this last observation. A seal already in
    // progress must finish first, and any later seal rechecks the same fence.
    let _seal = active.sealing.lock().await;
    let result = inspect(shared, &grant)
        .await
        .map_err(|error| error.to_string())??;
    let slot = crate::worker::generation_slot(
        &shared.cfg.shared_dir,
        grant.job_id,
        &grant.initial_token.resource,
        grant.initial_token.fence,
    );
    tokio::task::spawn_blocking(move || check_interrupted_seal(&slot))
        .await
        .map_err(|error| error.to_string())??;
    let observed = GrantObservation { stopped, result };
    if !grant_matches(&grant, &observed) {
        return Err("joined worker receipt differs from the immutable grant".into());
    }
    Ok(observed)
}

fn check_interrupted_seal(slot: &std::path::Path) -> Result<(), String> {
    let directory = slot
        .parent()
        .ok_or("generation slot lacks its configured parent")?;
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("generation directory unavailable: {error}")),
    };
    for entry in entries {
        if entry
            .map_err(|error| error.to_string())?
            .file_name()
            .to_string_lossy()
            .starts_with(".building-")
        {
            return Err("an interrupted sealed generation remains in its private workspace; review the exact work slot".into());
        }
    }
    Ok(())
}

fn peer_reply<T: Serialize>(result: Result<T, String>) -> Response {
    match result {
        Ok(value) => Json(value).into_response(),
        Err(reason) => failure(StatusCode::CONFLICT, reason),
    }
}

pub(crate) async fn observe_grant(
    State(shared): State<Arc<LeaderShared>>,
    Json(request): Json<crate::cancellation_routes::StopRequest>,
) -> Response {
    peer_reply(inspect_grant(&shared, request).await)
}

fn required_owners(
    receipt: &Receipt,
    grants: &[HistoricalGrant],
) -> Result<BTreeMap<String, String>, String> {
    let mut owners = BTreeMap::from([(receipt.data.owner.clone(), receipt.data.storage.clone())]);
    let sources = receipt.data.source.clone().into_iter().chain(
        grants
            .iter()
            .filter_map(|grant| grant.source_binding_json.as_deref())
            .map(serde_json::from_str::<AuthoritySourceCopy>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?,
    );
    for source in sources {
        let storage = source.copy.storage_instance_id;
        if owners
            .insert(source.owner_node, storage.clone())
            .is_some_and(|previous| previous != storage)
        {
            return Err(
                "a required source owner changed storage instance; reconnect the original storage"
                    .into(),
            );
        }
    }
    for grant in grants {
        let Some(storage) = &grant.storage_instance_id else {
            return Err(
                "historical writer has no bound storage instance; review its source custody".into(),
            );
        };
        if owners
            .insert(grant.initial_token.owner_node_id.clone(), storage.clone())
            .is_some_and(|previous| previous != *storage)
        {
            return Err(
                "a required owner changed storage instance; reconnect the original storage".into(),
            );
        }
    }
    Ok(owners)
}

async fn lookup_owner(
    shared: &LeaderShared,
    request: &OwnerQuery,
) -> Result<OwnerAuthority, String> {
    if !shared.view.borrow().is_me || !shared.authority_ready() {
        return Err("current disposition authority unavailable".into());
    }
    let control = shared
        .control
        .as_ref()
        .ok_or("replicated disposition unavailable")?;
    let receipt = control
        .disposition(&request.request_id)
        .await?
        .ok_or("unknown job disposition")?;
    control
        .cancellation_removal(receipt.job, &receipt.incarnation)
        .await?
        .filter(|removal| removal.applied_revision == receipt.removal_revision)
        .ok_or("job disposition tombstone changed")?;
    let grants = control
        .historical_grants_for_removal(receipt.job, &receipt.incarnation)
        .await?;
    if grants.len() != receipt.data.grants.len()
        || grants.iter().any(|grant| {
            receipt
                .data
                .grants
                .get(&grant_key(grant))
                .is_none_or(|observed| !grant_matches(grant, observed))
        })
    {
        return Err("some exact historical grants are still stopping".into());
    }
    let storage = required_owners(&receipt, &grants)?
        .remove(&request.node)
        .ok_or("node is outside the exact disposition owner set")?;
    let deliveries = control
        .disposition_deliveries(&receipt.data.request.transfer, receipt.job)
        .await?;
    let node_grants: Vec<_> = grants
        .iter()
        .filter(|grant| grant.initial_token.owner_node_id == request.node)
        .filter_map(|grant| {
            receipt
                .data
                .grants
                .get(&grant_key(grant))
                .map(|observed| (grant, observed))
        })
        .collect();
    Ok(OwnerAuthority {
        job: receipt.job,
        transfer: receipt.data.request.transfer,
        storage,
        protected_results: deliveries
            .into_iter()
            .filter(|record| !record.cleanup_only)
            .map(|record| record.result_id)
            .filter(|id| !id.is_empty())
            .collect(),
        stopped: node_grants
            .iter()
            .map(|(_, observed)| observed.stopped.context.clone())
            .collect(),
        assembly_stops: node_grants
            .into_iter()
            .filter(|(grant, _)| grant.kind == "assemble")
            .map(|(_, observed)| observed.stopped.clone())
            .collect(),
        removal_revision: receipt.removal_revision,
    })
}

pub(crate) async fn owner_authority(
    State(shared): State<Arc<LeaderShared>>,
    Json(request): Json<OwnerQuery>,
) -> Response {
    peer_reply(lookup_owner(&shared, &request).await)
}

async fn inspect_owner(
    shared: &Arc<LeaderShared>,
    request: &OwnerQuery,
) -> Result<OwnerObservation, String> {
    if request.node != shared.cfg.node_name {
        return Err("disposition was addressed to a different owner".into());
    }
    let authority = if shared.control.is_some() && shared.view.borrow().is_me {
        lookup_owner(shared, request).await?
    } else {
        let url = shared
            .view
            .borrow()
            .leader_url()
            .map(str::to_owned)
            .ok_or("current disposition authority unavailable")?;
        crate::http::ClusterClient::new(shared.cfg.secret.clone())
            .post_json(
                &url,
                "/cluster/v1/work/disposition-owner-authority",
                request,
            )
            .await?
    };
    let inventory = shared.engine.artifacts();
    if authority.storage != inventory.installation {
        return Err("disposition owner storage changed; reconnect the recorded storage".into());
    }
    let job = JobId(authority.job);
    // Only the replicated tombstone can authorize removing a projection. The
    // queue actor joins payload writers and PP admissions before this returns.
    // A grant with a worker context has already been joined and independently
    // fenced. It can still occupy the projection while its outbox is replayed.
    let recovery = shared
        .engine
        .recovery_document()
        .await
        .map_err(|error| error.to_string())?;
    if let Some(local) = recovery.jobs.iter().find(|local| local.id == job) {
        if !local
            .params
            .iter()
            .any(|(key, token)| key == "monarr-transfer" && token == &authority.transfer)
        {
            return Err("local queue handle belongs to another transfer".into());
        }
        if let Some(context) = nzbd_state::artifacts::WorkerControlContext::for_job(local) {
            if !authority
                .stopped
                .iter()
                .any(|stopped| nzbd_state::artifacts::same_worker_operation(stopped, &context))
                || !inventory
                    .worker_grant_is_stopped(authority.job, &context)
                    .map_err(|error| error.to_string())?
            {
                return Err("the local projection belongs to an unjoined worker generation".into());
            }
        } else if shared.view.borrow().is_me {
            shared
                .engine
                .remove_job_silent(job)
                .await
                .map_err(|error| error.to_string())?;
            let recovery = shared
                .engine
                .recovery_document()
                .await
                .map_err(|error| error.to_string())?;
            if recovery.jobs.iter().any(|local| local.id == job) {
                return Err("local authority projection is still retiring".into());
            }
        } else {
            return Err("previous authority is still retiring its local projection".into());
        }
    }
    let assembly_retirements = if authority.assembly_stops.is_empty() {
        Vec::new()
    } else {
        // lookup_owner checked all historical joins and the exact explicit
        // Delete receipt. The seal reader is independently joined here.
        let active = shared
            .worker_leases
            .lock()
            .unwrap()
            .clone()
            .ok_or("worker execution registry unavailable")?;
        let _seal = active.sealing.lock().await;
        let inv = inventory.clone();
        let root = shared.dest_dir.clone();
        let stops = authority.assembly_stops;
        let revision = authority.removal_revision;
        tokio::task::spawn_blocking(move || {
            let mut retirements = Vec::new();
            for stop in stops {
                if let Some(retirement) = inv.retire_worker_assembly(&stop, revision, &root)? {
                    retirements.push(retirement);
                }
            }
            Ok::<_, nzbd_state::artifacts::Error>(retirements)
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?
    };
    let candidates = tokio::task::spawn_blocking(move || {
        inventory.cluster_disposition_candidates(
            authority.job,
            &authority.transfer,
            &authority.protected_results,
        )
    })
    .await
    .map_err(|error| error.to_string())?
    .map_err(|error| error.to_string())?;
    Ok(OwnerObservation {
        node: request.node.clone(),
        storage: authority.storage,
        candidates,
        assembly_retirements,
    })
}

pub(crate) async fn observe_owner(
    State(shared): State<Arc<LeaderShared>>,
    Json(request): Json<OwnerQuery>,
) -> Response {
    peer_reply(inspect_owner(&shared, &request).await)
}

impl ControlStore {
    async fn disposition_deliveries(
        &self,
        transfer: &str,
        job: u32,
    ) -> Result<Vec<ClusterDelivery>, String> {
        let rows = self.client.query_consistent_map::<IdRow, _>(
            "SELECT id FROM cluster_deliveries WHERE request_token=?1 ORDER BY sequence LIMIT 129", params!(transfer),
        ).await.map_err(|error| error.to_string())?;
        if rows.len() > 128 {
            return Err(
                "job disposition delivery receipt bound exceeded; review source custody".into(),
            );
        }
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let record = self
                .cluster_delivery(&row.id)
                .await?
                .ok_or("delivery receipt disappeared during disposition")?;
            if record.delivery.job_id != job {
                return Err(
                    "delivery receipt transfer is attached to a different job handle".into(),
                );
            }
            records.push(record);
        }
        Ok(records)
    }
}
struct IdRow {
    id: String,
}
impl From<&mut Row<'_>> for IdRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self { id: row.get("id") }
    }
}

fn copy_id(copy: &ClusterArtifactCopy) -> String {
    format!(
        "artifact:{}:{}:{}",
        copy.storage_instance_id, copy.publication.artifact, copy.publication.generation
    )
}

fn owned_copy(owner: &str, copy: ClusterArtifactCopy) -> CopyObligation {
    CopyObligation {
        id: copy_id(&copy),
        owner_node: owner.into(),
        artifact: Some(copy),
        local_delivery_id: None,
        result_ref: None,
        result_id: None,
        directory_identity: None,
        deletion_files: None,
        status: "review_required".into(),
        error: Some("waiting for exact owner file seal".into()),
        claim_id: None,
        attempts: 0,
        next_retry_at: None,
    }
}

fn add_raw(
    copies: &mut BTreeMap<String, CopyObligation>,
    owner: &str,
    copy: ClusterArtifactCopy,
) -> Result<(), String> {
    let copy = owned_copy(owner, copy);
    if let Some(previous) = copies.insert(copy.id.clone(), copy.clone()) {
        if previous != copy {
            return Err("historical owners disagree about an exact source copy".into());
        }
    }
    Ok(())
}

fn raw_copies(
    receipt: &Receipt,
    grants: &[HistoricalGrant],
) -> Result<BTreeMap<String, CopyObligation>, String> {
    let mut copies = BTreeMap::new();
    if let Some(source) = &receipt.data.source {
        add_raw(&mut copies, &source.owner_node, source.copy.clone())?;
    }
    for grant in grants {
        if let Some(source) = &grant.source_binding_json {
            let source: AuthoritySourceCopy =
                serde_json::from_str(source).map_err(|error| error.to_string())?;
            if source.copy.job_id != receipt.job
                || source.copy.selected
                || source.copy.source != source.copy.publication
            {
                return Err("historical authority source is outside this exact removal".into());
            }
            add_raw(&mut copies, &source.owner_node, source.copy)?;
        }
        let observed = receipt
            .data
            .grants
            .get(&grant_key(grant))
            .ok_or("missing exact grant receipt")?;
        for source in &observed.stopped.sources {
            add_raw(
                &mut copies,
                &grant.initial_token.owner_node_id,
                ClusterArtifactCopy {
                    storage_instance_id: observed.stopped.storage_instance_id.clone(),
                    publication: source.clone(),
                    source: source.clone(),
                    job_id: receipt.job,
                    selected: false,
                    raw_files: None,
                },
            )?;
        }
    }
    Ok(copies)
}

fn private_record(
    receipt: &Receipt,
    copies: Vec<CopyObligation>,
    accepted_at_ms: i64,
) -> Result<ClusterDelivery, String> {
    let id = operation_id(&receipt.data.request.request_id);
    let request = &receipt.data.request;
    let delivery = serde_json::from_value(json!({
        "protocolVersion":1, "sequence":0, "deliveryId":id,
        "publicationGeneration":id, "artifactId":id,
        "sourceArtifactId":request.source_artifact_id.as_deref().unwrap_or_default(),
        "sourceGeneration":request.source_generation.as_deref().unwrap_or_default(),
        "jobId":receipt.job, "runnerInstanceId":request.runner_instance_id,
        "requestToken":request.transfer, "keepSourceOnFailure":false,
        "terminalOutcome":"CANCELLED", "publicationPath":"", "manifestDigest":id,
        "files":[], "fileOutcomes":[], "readyAt":accepted_at_ms / 1000,
        "sourceCleanup":{"status":"pending","attempts":0,"attemptHistory":[]}
    }))
    .map_err(|error| error.to_string())?;
    Ok(ClusterDelivery {
        receipt_id: id.clone(),
        result_id: id,
        cleanup_only: true,
        source_disposition: Some("cancellation_delete_files".into()),
        revision: 0,
        copies,
        delivery,
    })
}

impl ControlStore {
    async fn publish_disposition_copies(
        &self,
        receipt: &Receipt,
        copies: Vec<CopyObligation>,
        authority: (&str, &str, u64),
    ) -> Result<(), String> {
        let accepted_at_ms = crate::control::unix_ms_public();
        let record = private_record(receipt, copies, accepted_at_ms)?;
        let id = &record.delivery.delivery_id;
        let (data, public) = crate::delivery::encode_record(&record)?;
        let decision = crate::cancellation::CancellationDecision {
            receipt_id: id.clone(),
            cleanup_id: id.clone(),
            accepted_at_ms,
            disposition: "cleanup_pending".into(),
            reason: "exact job owner disposition".into(),
        };
        let writes = vec![authority_statement(authority)?,
            ("INSERT INTO cluster_cancellation_decisions(receipt_id,fingerprint,job_id,incarnation,removal_revision,cleanup_id,data)
              SELECT ?1,?1,?2,?3,?4,?1,?5 WHERE EXISTS(
                  SELECT 1 FROM cluster_job_dispositions WHERE sequence=?6 AND revision=?7 AND verified=0)
              ON CONFLICT(receipt_id) DO NOTHING".into(),
             params!(id, i64::from(receipt.job), &receipt.incarnation, receipt.removal_revision as i64,
                 serde_json::to_string(&decision).map_err(|error| error.to_string())?, receipt.sequence, receipt.revision)),
            ("INSERT INTO cluster_deliveries(id,data,public_data,cleanup_only,request_token)
              SELECT ?1,?2,?3,1,?4 WHERE EXISTS(SELECT 1 FROM cluster_cancellation_decisions WHERE cleanup_id=?1)
              ON CONFLICT(id) DO NOTHING".into(),
             params!(id, data, public, &receipt.data.request.transfer)),
        ];
        crate::delivery::bounded_transaction(&writes)?;
        self.client
            .txn(writes)
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub(crate) async fn disposition_cleanup_ready(&self, cleanup_id: &str) -> Result<bool, String> {
        let record = self.client.query_consistent_map::<ReceiptRow, _>(
            "SELECT d.sequence,d.job_id,d.incarnation,d.removal_revision,d.revision,d.data
             FROM cluster_job_dispositions d JOIN cluster_cancellation_decisions c
               ON c.job_id=d.job_id AND c.incarnation=d.incarnation AND c.removal_revision=d.removal_revision
             WHERE c.cleanup_id=?1", params!(cleanup_id),
        ).await.map_err(|error| error.to_string())?.into_iter().next().map(Receipt::try_from).transpose()?;
        Ok(record.is_none_or(|record| record.data.owners_joined))
    }

    pub(crate) async fn explicit_disposition_covers(
        &self,
        cleanup_id: &str,
        job: u32,
        incarnation: &str,
        revision: u64,
    ) -> Result<bool, String> {
        let Some(id) = cleanup_id.strip_prefix("job-disposition:") else {
            return Ok(false);
        };
        if id.len() != 64 {
            return Ok(false);
        }
        let row = self.client.query_consistent_map::<IdRow, _>(
            "SELECT d.request_id AS id FROM cluster_job_dispositions d JOIN cluster_cancellation_decisions c
               ON c.job_id=d.job_id AND c.incarnation=d.incarnation AND c.removal_revision=d.removal_revision
             WHERE c.cleanup_id=?1 AND d.job_id=?2 AND d.incarnation=?3 AND d.removal_revision=?4",
            params!(cleanup_id, i64::from(job), incarnation, revision as i64),
        ).await.map_err(|error| error.to_string())?.into_iter().next();
        Ok(row.is_some_and(|row| operation_id(&row.id) == cleanup_id))
    }

    async fn accepted_grant_result(
        &self,
        grant: &HistoricalGrant,
    ) -> Result<Option<String>, String> {
        Ok(self
            .client
            .query_consistent_map::<IdRow, _>(
                "SELECT result_id AS id FROM publication_receipts WHERE resource=?1 AND fence=?2
             AND owner_node_id=?3 AND owner_incarnation=?4 AND expected_job_revision=?5",
                params!(
                    &grant.initial_token.resource,
                    grant.initial_token.fence as i64,
                    &grant.initial_token.owner_node_id,
                    &grant.initial_token.owner_incarnation,
                    grant.job_revision as i64
                ),
            )
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .next()
            .map(|row| row.id))
    }
}

async fn observe_remote<T: serde::de::DeserializeOwned, R: Serialize>(
    shared: &LeaderShared,
    node: &str,
    route: &str,
    request: &R,
) -> Result<T, String> {
    let node_info = crate::registry::read_nodes(&shared.layout)
        .into_iter()
        .find(|info| info.name == node)
        .ok_or_else(|| {
            format!("required owner {node} is offline; waiting for its original storage")
        })?;
    crate::http::ClusterClient::new(shared.cfg.secret.clone())
        .post_json(&node_info.api_url, route, request)
        .await
        .map_err(|error| format!("required owner {node}: {error}"))
}

async fn check_owner(
    shared: &Arc<LeaderShared>,
    receipt: &Receipt,
    node: &str,
    storage: &str,
) -> Result<OwnerObservation, String> {
    let request = OwnerQuery {
        request_id: receipt.data.request.request_id.clone(),
        node: node.into(),
    };
    let reply = if node == shared.cfg.node_name {
        inspect_owner(shared, &request).await?
    } else {
        observe_remote(shared, node, "/cluster/v1/work/disposition-owner", &request).await?
    };
    if reply.node != node || reply.storage != storage {
        return Err(format!(
            "required owner {node} returned a mismatched storage observation"
        ));
    }
    Ok(reply)
}

fn waiting(receipt: &mut Receipt, reason: impl ToString) {
    receipt.data.state = JobDispositionState::Pending;
    receipt.data.reason = Some(reason.to_string().chars().take(1024).collect());
}

fn blocked(receipt: &mut Receipt, reason: impl ToString) {
    waiting(receipt, reason);
    receipt.data.state = JobDispositionState::Blocked;
}

fn same_target(copy: &CopyObligation, other: &CopyObligation) -> bool {
    copy.id == other.id
        && copy.owner_node == other.owner_node
        && match (&copy.artifact, &other.artifact) {
            (Some(copy), Some(other)) => {
                copy.storage_instance_id == other.storage_instance_id
                    && copy.publication == other.publication
                    && copy.job_id == other.job_id
            }
            (None, None) => {
                copy.result_ref == other.result_ref
                    && copy.result_id == other.result_id
                    && copy.local_delivery_id == other.local_delivery_id
                    && copy.directory_identity == other.directory_identity
            }
            _ => false,
        }
}

fn copy_removed(copy: &CopyObligation, deliveries: &[ClusterDelivery]) -> bool {
    deliveries
        .iter()
        .flat_map(|record| &record.copies)
        .any(|other| other.status == "removed" && same_target(copy, other))
}

async fn reconcile(
    shared: &Arc<LeaderShared>,
    control: &ControlStore,
    receipt: &mut Receipt,
) -> Result<(), String> {
    let removal = control
        .cancellation_removal(receipt.job, &receipt.incarnation)
        .await?
        .filter(|removal| removal.applied_revision == receipt.removal_revision)
        .ok_or("job disposition tombstone changed; preserve source custody")?;
    let grants = control
        .historical_grants_for_removal(receipt.job, &receipt.incarnation)
        .await?;
    let mut pending = None;
    for grant in &grants {
        let key = grant_key(grant);
        if grant.start_ordering != 1 || grant.storage_instance_id.is_none() {
            blocked(receipt, "historical writer lacks durable start ordering or storage identity; review this exact owner");
            return Ok(());
        }
        if receipt
            .data
            .grants
            .get(&key)
            .is_some_and(|observed| grant_matches(grant, observed))
        {
            continue;
        }
        let stop = crate::cancellation_routes::StopRequest {
            job: receipt.job,
            incarnation: receipt.incarnation.clone(),
            resource: grant.initial_token.resource.clone(),
            fence: grant.initial_token.fence,
            sealed: None,
        };
        let node = &grant.initial_token.owner_node_id;
        let observed = if *node == shared.cfg.node_name {
            inspect_grant(shared, stop).await
        } else {
            observe_remote(shared, node, "/cluster/v1/work/disposition-grant", &stop).await
        };
        match observed {
            Ok(observed) if grant_matches(grant, &observed) => {
                receipt.data.grants.insert(key, observed);
                waiting(receipt, "waiting for the remaining exact source owners");
                return Ok(());
            }
            Ok(_) => {
                pending.get_or_insert_with(|| {
                    format!("owner {node} returned a mismatched worker receipt")
                });
            }
            Err(error) => {
                pending.get_or_insert(error);
            }
        }
    }
    if let Some(reason) = pending {
        waiting(receipt, reason);
        return Ok(());
    }

    let owners = match required_owners(receipt, &grants) {
        Ok(owners) => owners,
        Err(reason) => {
            blocked(receipt, reason);
            return Ok(());
        }
    };
    for (node, storage) in &owners {
        if receipt.data.owners.contains_key(node) {
            continue;
        }
        match check_owner(shared, receipt, node, storage).await {
            Ok(observed) => {
                receipt.data.owners.insert(node.clone(), observed);
                waiting(receipt, "checking recorded owner and source copies");
                return Ok(());
            }
            Err(error) => {
                pending.get_or_insert(error);
            }
        }
    }
    if let Some(reason) = pending {
        waiting(receipt, reason);
        return Ok(());
    }

    if !receipt.data.owners_joined {
        receipt.data.owners_joined = true;
        waiting(
            receipt,
            "all exact owners joined; checking their recorded source copies",
        );
        return Ok(());
    }

    let records = control
        .disposition_deliveries(&receipt.data.request.transfer, receipt.job)
        .await?;
    for grant in &grants {
        let observed = &receipt.data.grants[&grant_key(grant)];
        let accepted = control.accepted_grant_result(grant).await?;
        if let Some(result) = &observed.result {
            let has_ledger = records.iter().any(|record| {
                record.copies.iter().any(|copy| {
                    copy.result_id.as_deref() == Some(&result.result_id)
                        && copy.owner_node == grant.initial_token.owner_node_id
                })
            });
            if !has_ledger {
                if removal.mode != RemovalMode::DeleteFiles {
                    blocked(
                        receipt,
                        "a sealed historical result needs its exact source retirement receipt",
                    );
                    return Ok(());
                }
                crate::cancellation::decide(
                    control,
                    &shared.cfg.shared_dir.join(".nzbd-cluster/generations"),
                    result,
                    shared.authority_identity(),
                )
                .await?;
                waiting(
                    receipt,
                    "sealed worker result is recorded for exact source cleanup",
                );
                return Ok(());
            }
        } else if let Some(result) = accepted {
            if !records.iter().any(|record| {
                record.copies.iter().any(|copy| {
                    copy.result_id.as_deref() == Some(&result)
                        && copy.owner_node == grant.initial_token.owner_node_id
                        && copy.status == "removed"
                })
            }) {
                blocked(receipt, "an accepted worker result lacks proof that its exact sealed source was disposed");
                return Ok(());
            }
        }
        if grant.kind == "assemble"
            && !receipt
                .data
                .owners
                .get(&grant.initial_token.owner_node_id)
                .is_some_and(|owner| {
                    owner.assembly_retirements.iter().any(|proof| {
                        proof.stop_id == observed.stopped.id
                            && proof.removal_revision == receipt.removal_revision
                            && proof.storage_instance_id == observed.stopped.storage_instance_id
                            && (!observed.stopped.never_started || !proof.had_output)
                    })
                })
        {
            let node = &grant.initial_token.owner_node_id;
            match check_owner(shared, receipt, node, &owners[node]).await {
                Ok(owner) => {
                    let retired = owner
                        .assembly_retirements
                        .iter()
                        .any(|proof| proof.stop_id == observed.stopped.id);
                    receipt.data.owners.insert(node.clone(), owner);
                    if retired {
                        waiting(
                            receipt,
                            "checking the refreshed assembly retirement receipt",
                        );
                    } else {
                        blocked(receipt, "assembly workspace lacks an exact owner retirement receipt; review its historical custody");
                    }
                }
                Err(reason) => waiting(receipt, reason),
            }
            return Ok(());
        }
    }

    let raw = raw_copies(receipt, &grants)?;
    if raw.is_empty() && records.is_empty() {
        let row = control
            .disposition_job(receipt.job)
            .await?
            .ok_or("retired job descriptor unavailable")?;
        if row.incarnation != receipt.incarnation || row.revision as u64 != receipt.removal_revision
        {
            return Err("retired job descriptor no longer matches explicit disposition".into());
        }
        let job = crate::control::decode_intent(&row.job).map_err(|error| error.to_string())?;
        if !job.files.is_empty() {
            blocked(
                receipt,
                "download payload has no exact storage owner record; review its custody",
            );
            return Ok(());
        }
    }
    let unknown_owner = receipt.data.owners.iter().find_map(|(owner, observed)| {
        let storage = &owners[owner];
        observed
            .candidates
            .iter()
            .any(|candidate| {
                !raw.values()
                    .chain(records.iter().flat_map(|record| &record.copies))
                    .any(|copy| {
                        copy.owner_node == *owner
                            && copy.artifact.as_ref().is_some_and(|artifact| {
                                artifact.storage_instance_id == *storage
                                    && artifact.publication == *candidate
                            })
                    })
            })
            .then_some(owner.clone())
    });
    if let Some(owner) = unknown_owner {
        match check_owner(shared, receipt, &owner, &owners[&owner]).await {
            Ok(observation) => {
                receipt.data.owners.insert(owner.clone(), observation);
                blocked(receipt, format!("owner {owner} has unbound source or workspace custody; review that inventory"));
            }
            Err(error) => waiting(receipt, error),
        }
        return Ok(());
    }

    let id = operation_id(&receipt.data.request.request_id);
    let missing: Vec<_> = raw
        .values()
        .filter(|copy| {
            !copy_removed(copy, &records)
                && !records.iter().any(|record| {
                    record.source_disposition.as_deref() != Some("cancellation_keep_files")
                        && record.copies.iter().any(|other| {
                            same_target(copy, other)
                                && (record.delivery.delivery_id == id
                                    || other.status != "retained"
                                    || other
                                        .artifact
                                        .as_ref()
                                        .is_some_and(|artifact| artifact.selected))
                        })
                })
        })
        .cloned()
        .collect();
    if !missing.is_empty() {
        let _serial = shared.delivery_serial.lock().await;
        if let Some(mut current) = control.cluster_delivery(&id).await? {
            // A normal retirement may settle as retained after another raw
            // copy already entered the explicit ledger. Preserve the active
            // claims and add only those newly eligible exact obligations.
            for copy in missing {
                if !current.copies.iter().any(|other| same_target(&copy, other)) {
                    current.copies.push(copy);
                }
            }
            current.delivery.source_cleanup.status = "pending".into();
            current.delivery.source_cleanup.completed_at = None;
            control.update_cluster_delivery(&current).await?;
        } else {
            control
                .publish_disposition_copies(receipt, missing, shared.authority_identity())
                .await?;
        }
        waiting(
            receipt,
            "joined source owners are sealing their exact files",
        );
        return Ok(());
    }
    let unresolved = records
        .iter()
        .flat_map(|record| record.copies.iter())
        .chain(raw.values())
        .find(|copy| !copy_removed(copy, &records));
    if let Some(copy) = unresolved {
        if copy.status == "retained" || copy.status == "review_required" && copy.artifact.is_none()
        {
            blocked(
                receipt,
                format!("source copy on {} requires owner review", copy.owner_node),
            );
        } else {
            waiting(
                receipt,
                format!(
                    "waiting for exact source disposition on {}",
                    copy.owner_node
                ),
            );
        }
        return Ok(());
    }
    receipt.data.state = JobDispositionState::Verified;
    receipt.data.reason = None;
    Ok(())
}

pub(crate) async fn reconcile_one(
    shared: &Arc<LeaderShared>,
    cursor: &mut i64,
) -> Result<(), String> {
    let control = shared
        .control
        .as_ref()
        .ok_or("replicated control unavailable")?;
    let Some(mut receipt) = control.next_disposition(*cursor).await? else {
        *cursor = 0;
        return Ok(());
    };
    *cursor = receipt.sequence;
    let prior = serde_json::to_vec(&receipt.data).map_err(|error| error.to_string())?;
    if let Err(error) = reconcile(shared, control, &mut receipt).await {
        waiting(&mut receipt, error);
    }
    if prior != serde_json::to_vec(&receipt.data).map_err(|error| error.to_string())? {
        control
            .save_disposition(&receipt, shared.authority_identity())
            .await?;
    }
    Ok(())
}
