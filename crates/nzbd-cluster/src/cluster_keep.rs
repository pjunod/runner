//! Replicated desired failure preference, confirmed separately by exact owners.
//! The immutable Delivery and deletion authority never change with this policy.
use crate::{control::ControlStore, delivery::CopyObligation, leader::LeaderShared};
use axum::{
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use hiqlite::{macros::params, Row};
use nzbd_state::artifacts::{
    ClusterArtifactCopy, ClusterFailureKeepStatus, ClusterFailureKeepTarget,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex, Weak},
};

const MAX_TARGETS: usize = 64;
const MAX_RECORD: usize = 1024 * 1024;
const PEER_PATH: &str = "/cluster/v1/keep/apply";

#[derive(Default)]
pub(crate) struct CopyGuards(Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>);
impl CopyGuards {
    pub(crate) fn lock_for(&self, copy: &CopyObligation) -> Arc<tokio::sync::Mutex<()>> {
        let key = physical_key(copy);
        let mut entries = self.0.lock().unwrap();
        entries.retain(|_, weak| weak.strong_count() != 0);
        if let Some(guard) = entries.get(&key).and_then(Weak::upgrade) {
            return guard;
        }
        let guard = Arc::new(tokio::sync::Mutex::new(()));
        entries.insert(key, Arc::downgrade(&guard));
        guard
    }
}
fn physical_key(copy: &CopyObligation) -> String {
    let value = if let Some(artifact) = &copy.artifact {
        serde_json::json!([
            "artifact",
            artifact.storage_instance_id,
            artifact.publication
        ])
    } else if let Some(local) = &copy.local_delivery_id {
        serde_json::json!(["local", copy.owner_node, local])
    } else {
        serde_json::json!([
            "sealed",
            copy.owner_node,
            copy.result_id,
            copy.directory_identity
        ])
    };
    format!("{:x}", Sha256::digest(serde_json::to_vec(&value).unwrap()))
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Target {
    id: String,
    owner: String,
    job: u32,
    delivery: Option<String>,
    copy_id: String,
    artifact: Option<ClusterArtifactCopy>,
    local_delivery: Option<String>,
    result_ref: Option<String>,
    result_id: Option<String>,
    directory_identity: Option<(u64, u64)>,
    initial_keep: bool,
    #[serde(default)]
    removed: bool,
}
impl Target {
    fn obligation(&self) -> CopyObligation {
        CopyObligation {
            id: self.copy_id.clone(),
            owner_node: self.owner.clone(),
            artifact: self.artifact.clone(),
            local_delivery_id: self.local_delivery.clone(),
            result_ref: self.result_ref.clone(),
            result_id: self.result_id.clone(),
            directory_identity: self.directory_identity,
            deletion_files: None,
            status: "pending".into(),
            error: None,
            claim_id: None,
            attempts: 0,
            next_retry_at: None,
        }
    }
    fn validate(&self) -> Result<(), String> {
        let id_ok = |s: &str, max: usize| {
            !s.is_empty() && s.len() <= max && !s.chars().any(char::is_control)
        };
        if self.job == 0
            || !id_ok(&self.owner, 128)
            || !id_ok(&self.copy_id, 4096)
            || self.result_ref.as_ref().is_some_and(|s| !id_ok(s, 4096))
            || self.result_id.as_ref().is_some_and(|s| !id_ok(s, 256))
            || self
                .local_delivery
                .as_ref()
                .is_some_and(|s| !id_ok(s, 4096))
            || serde_json::to_vec(self).map_err(|e| e.to_string())?.len() > 16384
        {
            return Err(
                "copy Keep descriptor exceeds supported bounds; review source inventory".into(),
            );
        }
        if let Some(copy) = &self.artifact {
            if copy.job_id != self.job
                || !id_ok(&copy.storage_instance_id, 256)
                || [
                    &copy.publication.artifact,
                    &copy.publication.generation,
                    &copy.source.artifact,
                    &copy.source.generation,
                ]
                .iter()
                .any(|s| !id_ok(s, 256))
            {
                return Err("copy Keep artifact/job/generation identity is invalid".into());
            }
        } else if self.local_delivery.is_none()
            && (self.result_ref.is_none()
                || self.result_id.is_none()
                || self.directory_identity.is_none())
        {
            return Err("sealed copy has no exact Keep identity; review required".into());
        }
        Ok(())
    }
    fn identity(&self) -> Result<String, String> {
        let mut value = self.clone();
        value.id.clear();
        value.removed = false;
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&value).map_err(|e| e.to_string())?)
        ))
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct TargetState {
    target: Target,
    ack_revision: Option<u64>,
    available: Option<bool>,
    keep: Option<bool>,
    observed_at: i64,
    attempts: u32,
    next_retry: i64,
    blocked_since: Option<i64>,
    error: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Preference {
    job: u32,
    desired: Option<bool>,
    desired_revision: u64,
    queue_keep: Option<bool>,
    targets: Vec<TargetState>,
    #[serde(default)]
    cursor: usize,
    #[serde(skip)]
    version: i64,
}
struct Stored {
    data: String,
    version: i64,
}
impl From<&mut Row<'_>> for Stored {
    fn from(r: &mut Row<'_>) -> Self {
        Self {
            data: r.get("data"),
            version: r.get("version"),
        }
    }
}
struct RawTarget {
    data: String,
    bytes: i64,
}
impl From<&mut Row<'_>> for RawTarget {
    fn from(r: &mut Row<'_>) -> Self {
        Self {
            data: r.get("data"),
            bytes: r.get("bytes"),
        }
    }
}
const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS cluster_failure_keep(job_id INTEGER PRIMARY KEY,version INTEGER NOT NULL CHECK(version>0),data TEXT NOT NULL CHECK(length(CAST(data AS BLOB))<=1048576)) STRICT",
    "DROP INDEX IF EXISTS cluster_deliveries_keep_job",
    "CREATE INDEX IF NOT EXISTS cluster_deliveries_keep_job_valid ON cluster_deliveries(CASE WHEN json_valid(data) THEN json_extract(data,'$.delivery.jobId') END)",
    "CREATE TABLE IF NOT EXISTS cluster_failure_keep_meta(key TEXT PRIMARY KEY,value INTEGER NOT NULL) STRICT",
    "CREATE TRIGGER IF NOT EXISTS cluster_failure_keep_cas BEFORE INSERT ON cluster_failure_keep BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM cluster_failure_keep WHERE job_id=NEW.job_id AND version<>NEW.version-1) OR (NOT EXISTS(SELECT 1 FROM cluster_failure_keep WHERE job_id=NEW.job_id) AND NEW.version<>1) THEN RAISE(ABORT,'copy Keep observation changed; retry') END; END",
];
pub(crate) async fn install_schema(control: &ControlStore) -> Result<(), String> {
    txn(
        control,
        SCHEMA.iter().map(|s| ((*s).into(), params!())).collect(),
    )
    .await
}
async fn txn(control: &ControlStore, writes: Vec<(String, hiqlite::Params)>) -> Result<(), String> {
    crate::delivery::bounded_transaction(&writes)?;
    control
        .client
        .txn(writes)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    Ok(())
}
async fn load(control: &ControlStore, job: u32) -> Result<Option<Preference>, String> {
    let row=control.client.query_consistent_map::<Stored,_>("SELECT CASE WHEN length(CAST(data AS BLOB))<=1048576 THEN data ELSE '{}' END AS data,version FROM cluster_failure_keep WHERE job_id=?1",params!(i64::from(job))).await.map_err(|e|e.to_string())?.into_iter().next();
    row.map(|row| {
        let mut value: Preference = serde_json::from_str(&row.data).map_err(|e| e.to_string())?;
        if value.targets.len() > MAX_TARGETS || value.job != job {
            return Err("copy Keep record exceeds supported scope".into());
        }
        value.version = row.version;
        Ok(value)
    })
    .transpose()
}
fn write(value: &Preference) -> Result<(String, hiqlite::Params), String> {
    let data = serde_json::to_string(value).map_err(|e| e.to_string())?;
    if data.len() > MAX_RECORD || value.targets.len() > MAX_TARGETS || value.version == i64::MAX {
        return Err("copy Keep record exceeds supported bounds".into());
    }
    Ok(("INSERT INTO cluster_failure_keep(job_id,version,data) VALUES(?1,?2,?3) ON CONFLICT(job_id) DO UPDATE SET version=excluded.version,data=excluded.data".into(),params!(i64::from(value.job),value.version+1,data)))
}
async fn targets(control: &ControlStore, job: u32) -> Result<Vec<Target>, String> {
    // Project only small exact proof; never materialize per-file manifests.
    let projection = "json_object('id','','owner',json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.ownerNode'),'job',json_extract(CASE WHEN json_valid(d.data) THEN d.data ELSE '{}' END,'$.delivery.jobId'),'delivery',d.id,'copy_id',json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.id'),'artifact',json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.artifact'),'local_delivery',json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.localDeliveryId'),'result_ref',json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.resultRef'),'result_id',json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.resultId'),'directory_identity',json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.directoryIdentity'),'initial_keep',json(CASE WHEN json_extract(CASE WHEN json_valid(d.data) THEN d.data ELSE '{}' END,'$.delivery.keepSourceOnFailure') THEN 'true' ELSE 'false' END),'removed',json(CASE WHEN json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.status')='removed' THEN 'true' ELSE 'false' END))";
    let sql = format!(
        "WITH selected AS (SELECT {projection} AS raw FROM cluster_deliveries d,json_each(CASE WHEN json_valid(d.data) THEN d.data ELSE '{{}}' END,'$.copies') c WHERE (CASE WHEN json_valid(d.data) THEN json_extract(d.data,'$.delivery.jobId') END)=?1) SELECT CASE WHEN length(CAST(raw AS BLOB))<=16384 THEN raw ELSE '{{}}' END AS data,length(CAST(raw AS BLOB)) AS bytes FROM selected LIMIT 65"
    );
    let rows = control
        .client
        .query_consistent_map::<RawTarget, _>(sql, params!(i64::from(job)))
        .await
        .map_err(|e| e.to_string())?;
    if rows.len() > MAX_TARGETS {
        return Err(
            "more than 64 source copies; review exact owner inventory before changing Keep".into(),
        );
    }
    let mut result = Vec::new();
    for row in rows {
        if row.bytes > 16384 {
            return Err("copy Keep proof exceeds supported bounds".into());
        }
        let mut target: Target = serde_json::from_str(&row.data).map_err(|e| e.to_string())?;
        target.validate()?;
        target.id = target.identity()?;
        result.push(target);
    }
    if let Some((job, _, _)) = control.current_job_intent(job).await? {
        if let Some((_, raw)) = job
            .params
            .iter()
            .find(|(k, _)| k == crate::delivery::AUTHORITY_COPY_PARAM)
        {
            if raw.len() > 8192 {
                return Err("current source binding exceeds supported bounds".into());
            }
            let bound: crate::delivery::AuthoritySourceCopy =
                serde_json::from_str(raw).map_err(|e| e.to_string())?;
            if !result.iter().any(|target| {
                target.artifact.as_ref().is_some_and(|copy| {
                    copy.storage_instance_id == bound.copy.storage_instance_id
                        && copy.source == bound.copy.source
                }) && target.owner == bound.owner_node
            }) {
                let mut target = Target {
                    id: String::new(),
                    owner: bound.owner_node,
                    job: job.id.0,
                    delivery: None,
                    copy_id: format!("current-job-{}", job.id.0),
                    artifact: Some(bound.copy),
                    local_delivery: None,
                    result_ref: None,
                    result_id: None,
                    directory_identity: None,
                    initial_keep: false,
                    removed: false,
                };
                target.validate()?;
                target.id = target.identity()?;
                result.push(target);
            }
        }
    }
    if result.len() > MAX_TARGETS {
        return Err(
            "more than 64 source copies; review exact owner inventory before changing Keep".into(),
        );
    }
    Ok(result)
}
async fn refreshed(control: &ControlStore, job: u32) -> Result<Preference, String> {
    let mut value = load(control, job).await?.unwrap_or(Preference {
        job,
        desired: None,
        desired_revision: 0,
        queue_keep: None,
        targets: Vec::new(),
        cursor: 0,
        version: 0,
    });
    value.queue_keep = control
        .current_job_intent(job)
        .await?
        .filter(|(job, _, _)| {
            matches!(
                job.status,
                nzbd_types::JobStatus::Fetching
                    | nzbd_types::JobStatus::Queued
                    | nzbd_types::JobStatus::Paused
            )
        })
        .map(|(job, _, _)| {
            job.params
                .iter()
                .any(|(k, v)| k == "*Runner:KeepOnFailure" && v == "1")
        });
    for target in targets(control, job).await? {
        if let Some(entry) = value
            .targets
            .iter_mut()
            .find(|entry| entry.target.id == target.id)
        {
            entry.target.removed = target.removed;
        } else {
            value.targets.push(TargetState {
                target,
                ack_revision: None,
                available: None,
                keep: None,
                observed_at: 0,
                attempts: 0,
                next_retry: 0,
                blocked_since: None,
                error: None,
            });
        }
    }
    for entry in &mut value.targets {
        if entry.target.removed {
            entry.available = Some(false);
            entry.keep = Some(false);
            entry.ack_revision = Some(value.desired_revision);
            entry.error = None;
            entry.observed_at = crate::control::unix_ms_public();
        }
    }
    if value.targets.len() > MAX_TARGETS {
        return Err(
            "more than 64 exact source copies remain; review owner inventory before changing Keep"
                .into(),
        );
    }
    Ok(value)
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct KeepReply {
    pub available: bool,
    pub keep: bool,
    pub applied: bool,
    pub pending_owners: Vec<String>,
    pub message: String,
}
fn reply(value: &Preference, _now: i64) -> KeepReply {
    let mut pending = BTreeSet::new();
    for entry in &value.targets {
        if entry.ack_revision != Some(value.desired_revision) || entry.error.is_some() {
            pending.insert(entry.target.owner.clone());
        }
    }
    let available = value.queue_keep.is_some()
        || value
            .targets
            .iter()
            .any(|entry| entry.available != Some(false));
    let keep = available
        && value.desired.unwrap_or_else(|| {
            value.queue_keep.unwrap_or(false)
                || value
                    .targets
                    .iter()
                    .any(|entry| entry.available == Some(true) && entry.keep == Some(true))
        });
    let mixed = value.desired.is_none()
        && value
            .targets
            .iter()
            .filter_map(|entry| entry.keep.filter(|_| entry.available == Some(true)))
            .collect::<BTreeSet<_>>()
            .len()
            > 1;
    let applied = pending.is_empty() && !mixed;
    let message = if !available {
        "Exact source copies are already gone; Keep cannot restore removed bytes".into()
    } else if mixed {
        "Source owners report different current preferences; choose the desired Keep setting".into()
    } else if !applied {
        value
            .targets
            .iter()
            .find_map(|entry| entry.error.as_deref())
            .map(str::to_owned)
            .unwrap_or_else(|| "The change is not confirmed on every source owner yet".into())
    } else if value.desired.is_some() {
        "Current preference confirmed by all available source owners".into()
    } else {
        "Current source-owner preference".into()
    };
    KeepReply {
        available,
        keep,
        applied,
        pending_owners: pending.into_iter().collect(),
        message: bounded_message(&message),
    }
}
fn job_path(path: &str) -> Option<u32> {
    path.strip_prefix("/api/v1/jobs/")?
        .strip_suffix("/failure-keep")?
        .parse::<u32>()
        .ok()
        .filter(|id| *id > 0)
}
pub(crate) fn serves(request: &Request) -> bool {
    matches!(
        *request.method(),
        axum::http::Method::GET | axum::http::Method::PUT
    ) && job_path(request.uri().path()).is_some()
}
async fn save(
    control: &ControlStore,
    value: &Preference,
    authority: (&str, &str, u64),
) -> Result<(), String> {
    txn(
        control,
        vec![
            crate::control::authority_statement(authority)?,
            write(value)?,
        ],
    )
    .await
}

pub(crate) async fn serve(
    State(shared): State<Arc<LeaderShared>>,
    request: Request,
    next: Next,
) -> Response {
    if !serves(&request) {
        return next.run(request).await;
    }
    if !shared.view.borrow().is_me || !shared.authority_ready() {
        return error("current Keep authority unavailable");
    }
    let Some(control) = shared.control.as_ref() else {
        return error("replicated Keep control unavailable");
    };
    let job = job_path(request.uri().path()).unwrap();
    let desired = if request.method() == axum::http::Method::PUT {
        let body = match axum::body::to_bytes(request.into_body(), 128).await {
            Ok(body) => body,
            Err(_) => return error("Keep request exceeds supported size"),
        };
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Body {
            keep: bool,
        }
        match serde_json::from_slice::<Body>(&body) {
            Ok(body) => Some(body.keep),
            Err(_) => return error("Keep request must contain a boolean keep"),
        }
    } else {
        None
    };
    let result=async {
        let _serial=shared.mutation_serial.lock().await;
        let _delivery=shared.delivery_serial.lock().await;
        let mut value=refreshed(control,job).await?;
        if value.version==0&&value.targets.is_empty()&&value.queue_keep.is_none(){return Ok(reply(&value,crate::control::unix_ms_public()));}
        let mut queue_changed=false;
        let mut writes=vec![crate::control::authority_statement(shared.authority_identity())?];
        if let Some(keep)=desired {
            if value.desired!=Some(keep)&&(!value.targets.is_empty()||value.queue_keep.is_some()){
                value.desired=Some(keep);value.desired_revision=value.desired_revision.checked_add(1).filter(|rev|*rev<=i64::MAX as u64).ok_or("Keep revision exhausted")?;
                for entry in &mut value.targets{entry.next_retry=0;entry.attempts=0;entry.blocked_since=None;}
                if let Some((mut current,revision,_))=control.current_job_intent(job).await?{
                    current.params.retain(|(key,_)|key!="*Runner:KeepOnFailure");if keep{current.params.push(("*Runner:KeepOnFailure".into(),"1".into()));}
                    let intent=serde_json::to_string(&current).map_err(|e|e.to_string())?;
                    let command=serde_json::json!({"command":{"op":"replace_intent","job_id":job},"next_intent":intent,"deleted":false});
                    writes.push(("INSERT INTO command_receipts(command_id,job_id,expected_revision,applied_revision,command_json,applied_at_ms) VALUES(?1,?2,?3,?4,?5,?6)".into(),params!(format!("keep:{job}:{}",value.desired_revision),i64::from(job),revision as i64,(revision+1)as i64,command.to_string(),crate::control::unix_ms_public())));
                    if value.queue_keep.is_some(){value.queue_keep=Some(keep);}
                    queue_changed=true;
                }
            }
        }
        writes.push(write(&value)?);txn(control,writes).await?;
        drop(_delivery);
        if queue_changed{shared.restore_control_projection().await?;}
        Ok::<_,String>(reply(&value,crate::control::unix_ms_public()))
    }.await;
    match result {
        Ok(value) => (
            if value.applied {
                axum::http::StatusCode::OK
            } else {
                axum::http::StatusCode::ACCEPTED
            },
            Json(value),
        )
            .into_response(),
        Err(reason) => error(&reason),
    }
}
fn bounded_message(value: &str) -> String {
    let mut end = value.len().min(2048);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}
fn error(reason: &str) -> Response {
    (
        axum::http::StatusCode::CONFLICT,
        Json(serde_json::json!({"error":bounded_message(reason)})),
    )
        .into_response()
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OwnerRequest {
    pub job_id: u32,
    pub target_id: String,
    pub desired_revision: u64,
    pub mode: String,
    #[serde(default)]
    pub copy: Option<PolicyCopy>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PolicyCopy {
    id: String,
    owner_node: String,
    artifact: Option<ClusterArtifactCopy>,
    local_delivery_id: Option<String>,
    result_ref: Option<String>,
    result_id: Option<String>,
    directory_identity: Option<(u64, u64)>,
}
impl PolicyCopy {
    fn from_copy(copy: &CopyObligation) -> Self {
        Self {
            id: copy.id.clone(),
            owner_node: copy.owner_node.clone(),
            artifact: copy.artifact.clone(),
            local_delivery_id: copy.local_delivery_id.clone(),
            result_ref: copy.result_ref.clone(),
            result_id: copy.result_id.clone(),
            directory_identity: copy.directory_identity,
        }
    }
    fn obligation(&self) -> CopyObligation {
        CopyObligation {
            id: self.id.clone(),
            owner_node: self.owner_node.clone(),
            artifact: self.artifact.clone(),
            local_delivery_id: self.local_delivery_id.clone(),
            result_ref: self.result_ref.clone(),
            result_id: self.result_id.clone(),
            directory_identity: self.directory_identity,
            deletion_files: None,
            status: "pending".into(),
            error: None,
            claim_id: None,
            attempts: 0,
            next_retry_at: None,
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
struct Authority {
    target: Target,
    desired: Option<bool>,
    revision: u64,
}
#[derive(Serialize, Deserialize)]
struct OwnerReply {
    job_id: u32,
    target_id: String,
    revision: u64,
    status: ClusterFailureKeepStatus,
}
async fn authority(shared: &LeaderShared, request: &OwnerRequest) -> Result<Authority, String> {
    if !shared.view.borrow().is_me || !shared.authority_ready() {
        return Err("not the current source Keep authority".into());
    }
    let control = shared.control.as_ref().ok_or("Keep control unavailable")?;
    let value = load(control, request.job_id)
        .await?
        .ok_or("Keep desired revision not found")?;
    if value.desired_revision != request.desired_revision {
        return Err("Keep desired revision changed".into());
    }
    let target = value
        .targets
        .into_iter()
        .find(|entry| entry.target.id == request.target_id)
        .ok_or("exact source Keep target changed")?
        .target;
    Ok(Authority {
        target,
        desired: value.desired,
        revision: value.desired_revision,
    })
}
async fn fetch_authority(
    shared: &LeaderShared,
    request: &OwnerRequest,
) -> Result<Authority, String> {
    if shared.view.borrow().is_me {
        return authority(shared, request).await;
    }
    let leader = shared
        .view
        .borrow()
        .leader_url()
        .map(str::to_owned)
        .ok_or("source Keep leader unavailable")?;
    let mut request = request.clone();
    request.mode = "authority".into();
    crate::http::ClusterClient::new(shared.cfg.secret.clone())
        .post_json(&leader, PEER_PATH, &request)
        .await
}
fn owner_target(
    shared: &LeaderShared,
    target: &Target,
) -> Result<ClusterFailureKeepTarget, String> {
    if target.owner != shared.cfg.node_name {
        return Err("source Keep belongs to another owner".into());
    }
    Ok(if let Some(copy) = target.artifact.clone() {
        ClusterFailureKeepTarget::Artifact { copy }
    } else if let Some(delivery_id) = target.local_delivery.clone() {
        ClusterFailureKeepTarget::LocalDelivery {
            job: target.job,
            delivery_id,
        }
    } else {
        ClusterFailureKeepTarget::Sealed {
            root: shared.cfg.shared_dir.join(".nzbd-cluster/generations"),
            path: target
                .result_ref
                .clone()
                .ok_or("sealed Keep path missing")?
                .into(),
            identity: target
                .directory_identity
                .ok_or("sealed Keep directory proof missing")?,
            result_id: target
                .result_id
                .clone()
                .ok_or("sealed Keep result proof missing")?,
            initial_keep: target.initial_keep,
        }
    })
}
pub(crate) async fn apply_owner(
    State(shared): State<Arc<LeaderShared>>,
    Json(request): Json<OwnerRequest>,
) -> Response {
    if request.mode == "policy" {
        return match policy_authority(&shared, &request).await {
            Ok(value) => Json(value).into_response(),
            Err(reason) => error(&reason),
        };
    }
    if request.job_id == 0
        || request.target_id.len() != 64
        || request.desired_revision > i64::MAX as u64
    {
        return error("invalid source Keep request identity");
    }
    if request.mode == "authority" {
        return match authority(&shared, &request).await {
            Ok(value) => Json(value).into_response(),
            Err(reason) => error(&reason),
        };
    }
    if request.mode != "apply" {
        return error("unknown source Keep request mode");
    }
    let result = async {
        let initial = fetch_authority(&shared, &request).await?;
        if initial.target.owner != shared.cfg.node_name {
            return Err("source Keep belongs to another node".into());
        }
        let guard = shared.copy_guards.lock_for(&initial.target.obligation());
        let _owned = guard.lock().await;
        // A queued stale command never overrides a newer desired revision.
        let proof = fetch_authority(&shared, &request).await?;
        proof.target.validate()?;
        let target = owner_target(&shared, &proof.target)?;
        let inventory = shared.engine.artifacts();
        let desired = proof.desired.map(|keep| (proof.revision, keep));
        let status = tokio::task::spawn_blocking(move || {
            inventory
                .cluster_copy_failure_keep(&target, desired)
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| e.to_string())??;
        Ok::<_, String>(OwnerReply {
            job_id: request.job_id,
            target_id: request.target_id,
            revision: proof.revision,
            status,
        })
    }
    .await;
    match result {
        Ok(value) => Json(value).into_response(),
        Err(reason) => error(&reason),
    }
}

/// Called under the same physical-copy guard as the unlink operation. Network
/// failure is unknown authority, never permission to ignore a requested Keep.
async fn policy_authority(
    shared: &LeaderShared,
    request: &OwnerRequest,
) -> Result<Authority, String> {
    if !shared.view.borrow().is_me || !shared.authority_ready() {
        return Err("Keep policy authority unavailable".into());
    }
    let copy = request
        .copy
        .as_ref()
        .ok_or("Keep policy copy proof missing")?;
    if serde_json::to_vec(copy).map_err(|e| e.to_string())?.len() > 16384 {
        return Err("Keep policy copy descriptor too large".into());
    }
    let job = if let Some(artifact) = &copy.artifact {
        artifact.job_id
    } else if let Some(path) = &copy.result_ref {
        std::path::Path::new(path)
            .components()
            .find_map(|component| {
                component
                    .as_os_str()
                    .to_str()
                    .and_then(|value| value.strip_prefix("job-"))
                    .and_then(|id| id.parse::<u32>().ok())
            })
            .ok_or("sealed Keep job scope unavailable")?
    } else if let Some(local) = &copy.local_delivery_id {
        let control = shared.control.as_ref().ok_or("Keep control unavailable")?;
        let record = control
            .cluster_delivery(local)
            .await?
            .ok_or("local Delivery Keep scope unavailable")?;
        record.delivery.job_id
    } else {
        return Err("Keep policy has no exact job scope".into());
    };
    let control = shared.control.as_ref().ok_or("Keep control unavailable")?;
    let target = targets(control, job)
        .await?
        .into_iter()
        .find(|target| {
            target.owner == copy.owner_node
                && target.copy_id == copy.id
                && physical_key(&target.obligation()) == physical_key(&copy.obligation())
        })
        .ok_or("Keep copy is not an exact committed obligation")?;
    let value = load(control, job).await?;
    Ok(Authority {
        target,
        desired: value.as_ref().and_then(|value| value.desired),
        revision: value.map_or(0, |value| value.desired_revision),
    })
}
pub(crate) async fn check_copy_policy(
    shared: &LeaderShared,
    copy: &CopyObligation,
    outcome: &str,
) -> Result<Option<String>, String> {
    if matches!(outcome, "SUCCESS" | "CANCELLED") {
        return Ok(None);
    }
    let small = PolicyCopy::from_copy(copy);
    let request = OwnerRequest {
        job_id: 0,
        target_id: physical_key(copy),
        desired_revision: 0,
        mode: "policy".into(),
        copy: Some(small),
    };
    let proof =
        if shared.view.borrow().is_me {
            policy_authority(shared, &request).await?
        } else {
            let leader =
                shared.view.borrow().leader_url().map(str::to_owned).ok_or(
                    "Keep authority offline; retain source until current preference is known",
                )?;
            crate::http::ClusterClient::new(shared.cfg.secret.clone())
                .post_json::<_, Authority>(&leader, PEER_PATH, &request)
                .await?
        };
    if proof.desired == Some(true) {
        return Ok(Some(
            "Current Keep on failure is enabled for this exact source copy".into(),
        ));
    }
    let target = owner_target(shared, &proof.target)?;
    let inventory = shared.engine.artifacts();
    let status = tokio::task::spawn_blocking(move || {
        inventory
            .cluster_copy_failure_keep(&target, None)
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| e.to_string())??;
    Ok((status.available && status.keep)
        .then(|| "Current owner Keep on failure is enabled for this exact source copy".into()))
}

/// One exact owner per pass; cursor advances before remote IO, including errors.
pub(crate) async fn reconcile_one(shared: &Arc<LeaderShared>) -> Result<(), String> {
    if !shared.view.borrow().is_me || !shared.authority_ready() {
        return Ok(());
    }
    let control = shared.control.as_ref().ok_or("Keep control unavailable")?;
    struct Cursor {
        job: i64,
    }
    impl From<&mut Row<'_>> for Cursor {
        fn from(r: &mut Row<'_>) -> Self {
            Self { job: r.get("job") }
        }
    }
    let selected = {
        let _serial = shared.mutation_serial.lock().await;
        let _delivery = shared.delivery_serial.lock().await;
        let cursor=control.client.query_consistent_map::<Cursor,_>("SELECT coalesce((SELECT value FROM cluster_failure_keep_meta WHERE key='cursor'),0) AS job",params!()).await.map_err(|e|e.to_string())?.into_iter().next().map_or(0,|r|r.job);
        let next=control.client.query_consistent_map::<Cursor,_>("SELECT job_id AS job FROM cluster_failure_keep ORDER BY CASE WHEN job_id>?1 THEN 0 ELSE 1 END,job_id LIMIT 1",params!(cursor)).await.map_err(|e|e.to_string())?.into_iter().next();
        let Some(next) = next else {
            return Ok(());
        };
        let mut value = refreshed(control, next.job as u32).await?;
        let now = crate::control::unix_ms_public();
        let selected = (0..value.targets.len())
            .map(|offset| (value.cursor + offset) % value.targets.len())
            .find(|index| {
                let entry = &value.targets[*index];
                !entry.target.removed
                    && entry.next_retry <= now
                    && (entry.ack_revision != Some(value.desired_revision)
                        || now.saturating_sub(entry.observed_at) > 30000)
            });
        let selected = selected.map(|index| {
            value.cursor = (index + 1) % value.targets.len();
            (
                value.job,
                value.desired_revision,
                value.targets[index].target.clone(),
            )
        });
        txn(control,vec![crate::control::authority_statement(shared.authority_identity())?,write(&value)?,("INSERT INTO cluster_failure_keep_meta(key,value) VALUES('cursor',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value".into(),params!(next.job))]).await?;
        selected
    };
    let Some((job, revision, target)) = selected else {
        return Ok(());
    };
    let url = if target.owner == shared.cfg.node_name {
        Some(shared.cfg.advertise_url.clone())
    } else {
        crate::registry::read_nodes(&shared.layout)
            .into_iter()
            .find(|node| node.name == target.owner)
            .map(|node| node.api_url)
    };
    let outcome = match url {
        Some(url) => {
            crate::http::ClusterClient::new(shared.cfg.secret.clone())
                .post_json::<_, OwnerReply>(
                    &url,
                    PEER_PATH,
                    &OwnerRequest {
                        job_id: job,
                        target_id: target.id.clone(),
                        desired_revision: revision,
                        mode: "apply".into(),
                        copy: None,
                    },
                )
                .await
        }
        None => Err(format!(
            "Source owner {} is offline; waiting for exact application",
            target.owner
        )),
    };
    let _delivery = shared.delivery_serial.lock().await;
    let Some(mut value) = load(control, job).await? else {
        return Ok(());
    };
    if value.desired_revision != revision {
        return Ok(());
    }
    let Some(entry) = value
        .targets
        .iter_mut()
        .find(|entry| entry.target.id == target.id)
    else {
        return Ok(());
    };
    let now = crate::control::unix_ms_public();
    entry.attempts = entry.attempts.saturating_add(1);
    match outcome {
        Ok(reply)
            if reply.job_id == job
                && reply.revision == revision
                && reply.target_id == target.id =>
        {
            entry.available = Some(reply.status.available);
            entry.keep = Some(reply.status.keep);
            entry.ack_revision = Some(revision);
            entry.observed_at = now;
            entry.error = None;
            entry.blocked_since = None;
            entry.next_retry = now.saturating_add(5000);
        }
        outcome => {
            let reason = outcome.err().unwrap_or_else(|| {
                "source owner returned a mismatched Keep acknowledgement".into()
            });
            entry.error = Some(bounded_message(&reason));
            entry.blocked_since.get_or_insert(now);
            if entry
                .blocked_since
                .is_some_and(|since| now.saturating_sub(since) >= 30 * 60 * 1000)
            {
                entry.error = Some(bounded_message(&format!(
                    "Source-owner Keep change has waited at least 30 minutes: {}",
                    entry
                        .error
                        .as_deref()
                        .unwrap_or("current owner proof unavailable")
                )));
            }
            let delay =
                nzbd_types::hold_retry_delay_seconds(entry.attempts.saturating_sub(1).min(3) as u8);
            let jitter = i64::from(Sha256::digest(target.id.as_bytes())[0]) % (delay / 5 + 1);
            entry.next_retry = now.saturating_add((delay + jitter) * 1000);
        }
    }
    save(control, &value, shared.authority_identity()).await
}

#[cfg(test)]
#[path = "cluster_keep_tests.rs"]
mod tests;
