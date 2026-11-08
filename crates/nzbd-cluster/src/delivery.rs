//! Cluster-visible Delivery receipts. Payload authority is the committed
//! publication receipt; mutable per-copy cleanup never changes its identity.
use crate::control::ControlStore;
use hiqlite::{macros::params, Row};
use nzbd_state::artifacts::{ClusterArtifactCopy, Delivery};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub(crate) const AUTHORITY_COPY_PARAM: &str = "*Cluster:AuthoritySourceCopy";
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct AuthoritySourceCopy {
    pub owner_node: String,
    pub copy: ClusterArtifactCopy,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CopyObligation {
    pub id: String,
    pub owner_node: String,
    pub artifact: Option<ClusterArtifactCopy>,
    #[serde(default)]
    pub local_delivery_id: Option<String>,
    pub result_ref: Option<String>,
    pub result_id: Option<String>,
    pub directory_identity: Option<(u64, u64)>,
    #[serde(default)]
    pub deletion_files: Option<Vec<nzbd_state::artifacts::DeliveryFile>>,
    pub status: String,
    pub error: Option<String>,
    #[serde(default)]
    pub claim_id: Option<String>,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default)]
    pub next_retry_at: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ClusterDelivery {
    pub delivery: Delivery,
    pub receipt_id: String,
    pub result_id: String,
    pub copies: Vec<CopyObligation>,
    #[serde(default)]
    pub cleanup_only: bool,
    #[serde(default)]
    pub source_disposition: Option<String>,
    #[serde(skip)]
    pub revision: i64,
}

struct DeliveryRow {
    sequence: i64,
    data: String,
    revision: i64,
}
impl From<&mut Row<'_>> for DeliveryRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            sequence: row.get("sequence"),
            data: row.get("data"),
            revision: row.get("revision"),
        }
    }
}
struct ValueRow {
    value: String,
}
impl From<&mut Row<'_>> for ValueRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            value: row.get("value"),
        }
    }
}

impl ControlStore {
    /// Only the first authoritative leader imports local receipts. The pinned
    /// installation ID and existing sequences survive later leader changes.
    pub(crate) async fn initialize_delivery_identity(
        &self,
        installation: &str,
        owner: &str,
    ) -> Result<(), String> {
        self.initialize_delivery_identity_guarded(installation, owner, None)
            .await
    }
    pub(crate) async fn initialize_delivery_identity_guarded(
        &self,
        installation: &str,
        owner: &str,
        authority: Option<(&str, &str, u64)>,
    ) -> Result<(), String> {
        let mut writes:Vec<(String,hiqlite::Params)>=vec![
            ("INSERT INTO cluster_delivery_meta(key,value) VALUES('instance',?1) ON CONFLICT(key) DO NOTHING".into(),params!(installation)),
            ("INSERT INTO cluster_delivery_meta(key,value) VALUES('migration_owner',?1) ON CONFLICT(key) DO NOTHING".into(),params!(owner)),
            ("INSERT INTO cluster_delivery_meta(key,value) VALUES('migration_storage',?1) ON CONFLICT(key) DO NOTHING".into(),params!(installation)),
            ("INSERT INTO cluster_delivery_meta(key,value) VALUES('migration_cursor','0') ON CONFLICT(key) DO NOTHING".into(),params!())];
        if let Some(authority) = authority {
            writes.insert(0, crate::control::authority_statement(authority)?);
        }
        bounded_transaction(&writes)?;
        if let Some(authority) = authority {
            writes[0] = crate::control::authority_statement(authority)?;
        }
        self.client
            .txn(writes)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    pub(crate) async fn delivery_meta(&self, key: &str) -> Result<Option<String>, String> {
        Ok(self
            .client
            .query_consistent_map::<ValueRow, _>(
                "SELECT value FROM cluster_delivery_meta WHERE key=?1",
                params!(key),
            )
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .next()
            .map(|row| row.value))
    }
    pub(crate) async fn import_delivery_page(
        &self,
        installation: &str,
        owner_node: &str,
        after: i64,
        values: &[Value],
        next: i64,
        done: bool,
    ) -> Result<(), String> {
        if self.delivery_meta("migration_storage").await?.as_deref() != Some(installation)
            || self.delivery_meta("migration_owner").await?.as_deref() != Some(owner_node)
        {
            return Err(
                "legacy delivery migration belongs to another exact node storage instance".into(),
            );
        }
        if values.len() > 100
            || serde_json::to_vec(values).map_err(|e| e.to_string())?.len() > 16 * 1024 * 1024
        {
            return Err("migration page exceeds discovery bounds".into());
        }
        let mut writes: Vec<(String, hiqlite::Params)> = Vec::new();
        for value in values {
            let sequence = value
                .get("sequence")
                .and_then(Value::as_i64)
                .filter(|sequence| *sequence > after)
                .ok_or("untrusted migration sequence")?;
            let id = value
                .get("deliveryId")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty() && id.len() <= 4096)
                .ok_or("untrusted migration identity")?;
            let (data, public, error) = match serde_json::from_value::<Delivery>(value.clone()) {
                Ok(delivery) => {
                    let record = ClusterDelivery {
                        revision: 0,
                        cleanup_only: false,
                        source_disposition: None,
                        receipt_id: String::new(),
                        result_id: String::new(),
                        copies: vec![CopyObligation {
                            id: format!("local-delivery:{}", delivery.delivery_id),
                            owner_node: owner_node.into(),
                            artifact: None,
                            local_delivery_id: Some(delivery.delivery_id.clone()),
                            result_ref: None,
                            result_id: None,
                            directory_identity: None,
                            deletion_files: None,
                            claim_id: None,
                            attempts: 0,
                            next_retry_at: None,
                            status: if delivery.source_cleanup.status == "succeeded" {
                                "removed"
                            } else {
                                "pending"
                            }
                            .into(),
                            error: delivery.source_cleanup.error.clone(),
                        }],
                        delivery,
                    };
                    let (data, public) = encode_record(&record)?;
                    (data, Some(public), None)
                }
                Err(_) => (
                    "{}".into(),
                    None,
                    Some(
                        value
                            .get("discoveryError")
                            .and_then(Value::as_str)
                            .unwrap_or(
                                "publication manifest is invalid; source retained for review",
                            )
                            .chars()
                            .take(4096)
                            .collect::<String>(),
                    ),
                ),
            };
            writes.push(("INSERT INTO cluster_deliveries(sequence,id,data,discovery_error,public_data,accepted_at,request_token) SELECT ?1,?2,?3,?4,?6,?7,?8 WHERE (SELECT value FROM cluster_delivery_meta WHERE key='migration_cursor')=?5 ON CONFLICT(id) DO NOTHING".into(),params!(sequence,id,data,error,after.to_string(),public,None::<i64>,value.get("requestToken").and_then(Value::as_str))));
        }
        writes.push((
            "UPDATE cluster_delivery_meta SET value=?2 WHERE key='migration_cursor' AND value=?1"
                .into(),
            params!(after.to_string(), next.to_string()),
        ));
        if done {
            writes.push(("INSERT INTO cluster_delivery_meta(key,value) SELECT 'local_imported','1' WHERE (SELECT value FROM cluster_delivery_meta WHERE key='migration_cursor')=?1 ON CONFLICT(key) DO NOTHING".into(),params!(next.to_string())));
        }
        bounded_transaction(&writes)?;
        self.client
            .txn(writes)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    pub(crate) async fn delivery_instance(&self) -> Result<String, String> {
        self.client
            .query_consistent_map::<ValueRow, _>(
                "SELECT value FROM cluster_delivery_meta WHERE key='instance'",
                params!(),
            )
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .next()
            .map(|row| row.value)
            .ok_or_else(|| "cluster consumer identity is not initialized".into())
    }
    pub(crate) async fn cluster_delivery(
        &self,
        id: &str,
    ) -> Result<Option<ClusterDelivery>, String> {
        let rows = self
            .client
            .query_consistent_map::<DeliveryRow, _>(
                "SELECT sequence,CASE WHEN length(CAST(data AS BLOB))<=33554432 THEN data ELSE '{}' END AS data,revision FROM cluster_deliveries WHERE id=?1",
                params!(id),
            )
            .await
            .map_err(|e| e.to_string())?;
        rows.into_iter().next().map(decode).transpose()
    }
    pub(crate) async fn cluster_delivery_for_token(
        &self,
        token: &str,
    ) -> Result<Option<ClusterDelivery>, String> {
        let row=self.client.query_consistent_map::<ValueRow,_>("SELECT id AS value FROM cluster_deliveries WHERE request_token=?1 ORDER BY sequence DESC LIMIT 1",params!(token)).await.map_err(|error|error.to_string())?.into_iter().next();
        match row {
            Some(row) => self.cluster_delivery(&row.value).await,
            None => Ok(None),
        }
    }
    #[cfg(test)]
    pub(crate) async fn commit_delivery(
        &self,
        record: &ClusterDelivery,
    ) -> Result<ClusterDelivery, String> {
        self.commit_delivery_selected(record, None).await
    }
    pub(crate) async fn commit_delivery_selected(
        &self,
        record: &ClusterDelivery,
        selected: Option<(u32, u64)>,
    ) -> Result<ClusterDelivery, String> {
        // The row is written only after publication and fsync. Repeated READY
        // creation cannot replace cleanup progress. There is no acceptance
        // (contract §2.5); the legacy `accepted_at` column is written NULL.
        let (data, public) = encode_record(record)?;
        let sql = "INSERT INTO cluster_deliveries(id,data,public_data,accepted_at,cleanup_only,request_token) SELECT ?1,?2,?3,?4,?8,?9 WHERE ?5 IS NULL OR EXISTS(SELECT 1 FROM control_jobs WHERE job_id=?5 AND revision=?6 AND result_id=?7 AND deleted=0) ON CONFLICT(id) DO NOTHING";
        let params = params!(
            &record.delivery.delivery_id,
            data,
            public,
            None::<i64>,
            selected.map(|(job, _)| i64::from(job)),
            selected.map(|(_, revision)| revision as i64),
            &record.result_id,
            i64::from(record.cleanup_only),
            &record.delivery.request_token
        );
        bounded_statement(sql, &params)?;
        self.client
            .execute(sql, params)
            .await
            .map_err(|error| error.to_string())?;
        let current = self
            .cluster_delivery(&record.delivery.delivery_id)
            .await?
            .ok_or("selected publication changed before READY; current cancellation/control must be rechecked")?;
        if immutable_delivery(&current) != immutable_delivery(record) {
            return Err("immutable cluster delivery identity changed".into());
        }
        Ok(current)
    }
    pub(crate) async fn cluster_deliveries(
        &self,
        after: i64,
        limit: u32,
    ) -> Result<(Vec<ClusterDelivery>, i64), String> {
        let pointers=self.client.query_consistent_map::<DiscoveryRow,_>("SELECT sequence,id,length(CAST(data AS BLOB)) AS bytes,discovery_error FROM cluster_deliveries WHERE sequence>?1 ORDER BY sequence LIMIT ?2",params!(after,i64::from(limit.min(100)))).await.map_err(|error|error.to_string())?;
        let mut rows = Vec::new();
        let mut bytes = 0;
        let mut cursor = after;
        for pointer in pointers {
            if pointer.bytes > crate::proto::MAX_RPC_BYTES as i64
                || pointer.discovery_error.is_some()
            {
                cursor = pointer.sequence;
                continue;
            }
            if !rows.is_empty() && bytes + pointer.bytes > crate::proto::MAX_RPC_BYTES as i64 {
                break;
            }
            if let Ok(Some(record)) = self.cluster_delivery(&pointer.id).await {
                bytes += pointer.bytes;
                rows.push(record);
            }
            cursor = pointer.sequence;
        }
        Ok((rows, cursor))
    }
    /// Callers serialize mutable receipt updates with the leader's short
    /// delivery mutex, independently of queue mutation and heartbeat lanes.
    pub(crate) async fn update_cluster_delivery(
        &self,
        record: &ClusterDelivery,
    ) -> Result<(), String> {
        let (data, public) = encode_record(record)?;
        let sql = "UPDATE cluster_deliveries SET data=?2,public_data=?4,revision=revision+1 WHERE id=?1 AND revision=?3";
        let params = params!(&record.delivery.delivery_id, data, record.revision, public);
        bounded_statement(sql, &params)?;
        let changed = self
            .client
            .execute(sql, params)
            .await
            .map_err(|error| error.to_string())?;
        if changed != 1 {
            return Err(
                "delivery changed concurrently; reload and retry this progress update".into(),
            );
        }
        Ok(())
    }
}
// JSON serialization is a conservative upper bound for these SQL TEXT/INT
// parameters (it adds escaping absent from binary Raft encoding). Leave another
// 64 MiB for framing below the 128 MiB WAL segment before submitting any write.
const MAX_DELIVERY_TRANSACTION_BYTES: usize = 64 * 1024 * 1024;
fn bounded_statement(sql: &str, params: &hiqlite::Params) -> Result<(), String> {
    bounded_transaction(&[(sql.to_owned(), params.clone())])
}
pub(crate) fn bounded_transaction(writes: &[(String, hiqlite::Params)]) -> Result<(), String> {
    let bytes = serde_json::to_vec(writes)
        .map_err(|error| error.to_string())?
        .len();
    if bytes > MAX_DELIVERY_TRANSACTION_BYTES {
        return Err("Delivery transaction exceeds 64 MiB encoded limit; source retained, reduce metadata before retrying".into());
    }
    Ok(())
}
pub(crate) fn encode_record(record: &ClusterDelivery) -> Result<(String, String), String> {
    let private = serde_json::to_value(record).map_err(|error| error.to_string())?;
    let data = serde_json::to_string(&private).map_err(|error| error.to_string())?;
    let public =
        serde_json::to_string(&discovery_value(record)).map_err(|error| error.to_string())?;
    if data.len() > crate::proto::MAX_RPC_BYTES || public.len() > crate::proto::MAX_RPC_BYTES {
        return Err("private copy proof or public projection exceeds 32 MiB; retain source and review manifest size".into());
    }
    Ok((data, public))
}

fn decode(row: DeliveryRow) -> Result<ClusterDelivery, String> {
    let mut record: ClusterDelivery = serde_json::from_str(&row.data).map_err(|e| e.to_string())?;
    record.delivery.sequence = row.sequence;
    record.revision = row.revision;
    Ok(record)
}
pub(crate) fn immutable_delivery(record: &ClusterDelivery) -> Value {
    let mut value = serde_json::to_value(&record.delivery).expect("Delivery serializes");
    for key in ["sequence", "sourceCleanup"] {
        value.as_object_mut().unwrap().remove(key);
    }
    if let Some(outcomes) = value.get_mut("fileOutcomes").and_then(Value::as_array_mut) {
        for outcome in outcomes {
            outcome
                .as_object_mut()
                .unwrap()
                .remove("sourceCleanupStatus");
        }
    }
    json!({"receiptId":record.receipt_id,"resultId":record.result_id,"cleanupOnly":record.cleanup_only,"sourceDisposition":record.source_disposition,"delivery":value,
        "copies":record.copies.iter().map(|copy| {let mut value=serde_json::to_value(copy).unwrap();value.as_object_mut().unwrap().remove("status");value.as_object_mut().unwrap().remove("error");
            for key in ["claimId","attempts","nextRetryAt"] {value.as_object_mut().unwrap().remove(key);}value}).collect::<Vec<_>>()})
}
pub(crate) fn discovery_value(record: &ClusterDelivery) -> Value {
    if record.copies.len() > 64 {
        return discovery_stub(
            record.delivery.sequence,
            &record.delivery.delivery_id,
            "publication has more than 64 source copies; source retained for review; private cleanup obligations remain available to the owning Runner nodes",
        );
    }
    let mut value = if record.cleanup_only {
        json!({"protocolVersion":1,"cleanupOnly":true,"cleanupId":record.delivery.delivery_id,"jobId":record.delivery.job_id,"sourceCleanup":record.delivery.source_cleanup})
    } else {
        serde_json::to_value(&record.delivery).unwrap_or(Value::Null)
    };
    value["sourceCopies"] = json!(record
        .copies
        .iter()
        .map(|copy| json!({
            "id":copy.id,"ownerNode":copy.owner_node,"status":copy.status,
            "error":copy.error,"attempts":copy.attempts,"nextRetryAt":copy.next_retry_at
        }))
        .collect::<Vec<_>>());
    if serde_json::to_vec(&value).map_or(true, |bytes| bytes.len() > 8 * 1024 * 1024) {
        discovery_stub(
            record.delivery.sequence,
            &record.delivery.delivery_id,
            "publication manifest exceeds supported discovery size; source retained for review",
        )
    } else {
        value
    }
}
fn discovery_stub(sequence: i64, id: &str, error: &str) -> Value {
    json!({"protocolVersion":1,"sequence":sequence,"deliveryId":id,"discoveryError":error})
}
struct DiscoveryRow {
    sequence: i64,
    id: String,
    bytes: i64,
    discovery_error: Option<String>,
}
impl From<&mut Row<'_>> for DiscoveryRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            sequence: row.get("sequence"),
            id: row.get("id"),
            bytes: row.get("bytes"),
            discovery_error: row.get("discovery_error"),
        }
    }
}
impl ControlStore {
    async fn public_delivery_value(&self, id: &str) -> Result<Option<Value>, String> {
        let rows=self.client.query_consistent_map::<ValueRow,_>("SELECT CASE WHEN public_data IS NOT NULL AND json_valid(public_data) THEN json_set(public_data,'$.sequence',sequence,'$.deliveryId',id) ELSE 'null' END AS value FROM cluster_deliveries WHERE id=?1 AND length(CAST(COALESCE(public_data,'null') AS BLOB))<=8388608",params!(id)).await.map_err(|error|error.to_string())?;
        if let Some(row) = rows.into_iter().next() {
            let value: Value =
                serde_json::from_str(&row.value).map_err(|error| error.to_string())?;
            if !value.is_null() {
                return Ok(Some(value));
            }
        }
        self.cluster_delivery(id)
            .await
            .map(|record| record.map(|record| discovery_value(&record)))
    }
    /// The read-only reconciliation feed (contract §2.5): every publication
    /// row, in sequence order. There is no acceptance to filter on.
    pub(crate) async fn cluster_discovery_page(
        &self,
        after: i64,
        limit: u32,
    ) -> Result<(Vec<Value>, i64), String> {
        let rows=self.client.query_consistent_map::<DiscoveryRow,_>("SELECT sequence,id,length(CAST(COALESCE(public_data,data) AS BLOB)) AS bytes,discovery_error FROM cluster_deliveries WHERE cleanup_only=0 AND sequence>?1 ORDER BY sequence LIMIT ?2",params!(after,i64::from(limit))).await.map_err(|e|e.to_string())?;
        let mut values = Vec::new();
        let mut cursor = after;
        let mut bytes = 0;
        for row in rows {
            let value = if let Some(error) = row.discovery_error {
                discovery_stub(row.sequence, &row.id, &error)
            } else if row.bytes > 8 * 1024 * 1024 {
                discovery_stub(
                    row.sequence,
                    &row.id,
                    "publication manifest exceeds supported discovery size; source retained for review",
                )
            } else {
                match self.public_delivery_value(&row.id).await {
                    Ok(Some(value)) => value,
                    _ => discovery_stub(
                        row.sequence,
                        &row.id,
                        "publication manifest is invalid; source retained for review",
                    ),
                }
            };
            let mut value = value;
            value["sequence"] = json!(row.sequence);
            value["deliveryId"] = json!(row.id);
            let size = serde_json::to_vec(&value).map_err(|e| e.to_string())?.len();
            if !values.is_empty() && bytes + size + 4096 > 16 * 1024 * 1024 {
                break;
            }
            bytes += size;
            cursor = row.sequence;
            values.push(value);
        }
        Ok((values, cursor))
    }
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Page {
    #[serde(default)]
    after: i64,
    limit: Option<u32>,
}

fn decode_delivery_segment(raw: &str) -> Result<String, String> {
    if raw.len() > 12288 {
        return Err("delivery identity exceeds supported size".into());
    }
    let bytes = raw.as_bytes();
    let mut value = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let byte = if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err("malformed delivery identity encoding".into());
            }
            let digit = |byte: u8| match byte {
                b'0'..=b'9' => Some(byte - b'0'),
                b'a'..=b'f' => Some(byte - b'a' + 10),
                b'A'..=b'F' => Some(byte - b'A' + 10),
                _ => None,
            };
            let high = digit(bytes[index + 1]).ok_or("malformed delivery identity encoding")?;
            let low = digit(bytes[index + 2]).ok_or("malformed delivery identity encoding")?;
            index += 3;
            (high << 4) | low
        } else {
            let byte = bytes[index];
            index += 1;
            byte
        };
        if byte == b'/' || byte == b'\\' || byte.is_ascii_control() {
            return Err("delivery identity must be one path segment".into());
        }
        value.push(byte);
    }
    if value.is_empty() || value.len() > 4096 {
        return Err("invalid delivery identity size".into());
    }
    String::from_utf8(value).map_err(|_| "delivery identity is not valid UTF-8".into())
}

/// These two mutations commit only the replicated Delivery catalog. They do
/// not change queue intent and must not re-adopt a local payload projection.
pub(crate) fn is_catalog_mutation(request: &axum::extract::Request) -> bool {
    if request.method() != axum::http::Method::POST {
        return false;
    }
    let Some(relative) = request.uri().path().strip_prefix("/api/v1/deliveries/") else {
        return false;
    };
    let Some((id, action)) = relative.split_once('/') else {
        return false;
    };
    matches!(action, "accept" | "source-cleanup/retry")
        && !id.is_empty()
        && decode_delivery_segment(id).is_ok()
}

pub(crate) async fn serve_cluster_delivery(
    axum::extract::State(shared): axum::extract::State<std::sync::Arc<crate::leader::LeaderShared>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::http::{Method, StatusCode};
    use axum::response::IntoResponse;
    let path = request.uri().path().to_owned();
    if path != "/api/v1/deliveries"
        && !path.starts_with("/api/v1/deliveries/")
        && !path.starts_with("/api/v1/request-admissions/")
    {
        return next.run(request).await;
    }
    let Some(control) = shared.control.as_ref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    if !shared.view.borrow().is_me || !shared.authority_ready() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if let Some(raw) = path.strip_prefix("/api/v1/request-admissions/") {
        if request.method() != Method::GET {
            return next.run(request).await;
        }
        let token = match decode_delivery_segment(raw) {
            Ok(token) => token,
            Err(error) => {
                return (StatusCode::BAD_REQUEST, axum::Json(json!({"error":error})))
                    .into_response();
            }
        };
        let mut receipt = match control.admission(&token).await {
            Ok(Some(receipt)) => receipt,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    axum::Json(
                        json!({"error":"request admission not found in authoritative catalog"}),
                    ),
                )
                    .into_response();
            }
            Err(error) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    axum::Json(json!({"error":error})),
                )
                    .into_response();
            }
        };
        // Terminal progress is derived from replicated intent/tombstones;
        // no local orphan or stale projection may override that authority.
        let record = match control.cluster_delivery_for_token(&token).await {
            Ok(record) => record,
            Err(error) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    axum::Json(json!({"error":error})),
                )
                    .into_response();
            }
        };
        if let Some(record) = record {
            if receipt.job_id == record.delivery.job_id {
                let done = record.delivery.source_cleanup.status == "succeeded";
                receipt.runner_instance_id = record.delivery.runner_instance_id.clone();
                receipt.terminal_outcome = Some(record.delivery.terminal_outcome.clone());
                receipt.outcome = if record.delivery.terminal_outcome == "SUCCESS" {
                    "completed"
                } else {
                    "failed"
                }
                .into();
                receipt.failure_stage = record.delivery.failure_stage.clone();
                receipt.failure_cause = record.delivery.failure_cause.clone();
                receipt.keep_source_on_failure = record.delivery.keep_source_on_failure && !done;
                receipt.source_artifact_id = Some(record.delivery.source_artifact_id.clone());
                receipt.source_generation = Some(record.delivery.source_generation.clone());
                receipt.source_cleanup_status = Some(
                    if done {
                        "removed"
                    } else if record.copies.iter().any(|copy| copy.status == "retained") {
                        "retained"
                    } else {
                        "pending"
                    }
                    .into(),
                );
                if record.cleanup_only {
                    receipt.publication_path = None;
                    receipt.custody_state = if done {
                        "deleted_by_runner"
                    } else if receipt.keep_source_on_failure {
                        "kept_by_user"
                    } else {
                        "runner_owned"
                    }
                    .into();
                } else {
                    receipt.publication_path = Some(record.delivery.publication_path.clone());
                    receipt.custody_state = "curator".into();
                }
                return axum::Json(receipt).into_response();
            }
        }
        return axum::Json(receipt).into_response();
    }
    let _serial = shared.delivery_serial.lock().await;
    let method = request.method().clone();
    let uri = request.uri().clone();
    drop(request);
    let result:Result<Value,(StatusCode,String)>=async {
        if path=="/api/v1/deliveries" && method==Method::GET {
            let axum::extract::Query(page)=axum::extract::Query::<Page>::try_from_uri(&uri).map_err(|e|(StatusCode::BAD_REQUEST,e.to_string()))?;
            let limit=page.limit.unwrap_or(50);
            if page.after<0 || !(1..=100).contains(&limit) {return Err((StatusCode::BAD_REQUEST,"invalid delivery cursor or limit".into()));}
            let (values,cursor)=control.cluster_discovery_page(page.after,limit).await.map_err(|e|(StatusCode::SERVICE_UNAVAILABLE,e))?;
            let adopted=control.delivery_meta("local_imported").await.map_err(|error|(StatusCode::SERVICE_UNAVAILABLE,error))?.is_some();
            return Ok(json!({"protocolVersion":1,"deliveries":values,"nextCursor":cursor,"catalogAdoptionComplete":adopted,
                "cursorRule":"Rescan with after=0 each sweep, or durably persist every returned delivery before advancing the cursor."}));
        }
        let relative=path.strip_prefix("/api/v1/deliveries/").ok_or((StatusCode::NOT_FOUND,"delivery not found".into()))?;
        let mut pieces=relative.split('/');let id=decode_delivery_segment(pieces.next().unwrap_or("")).map_err(|error|(StatusCode::BAD_REQUEST,error))?;let action=pieces.collect::<Vec<_>>().join("/");
        if method==Method::POST && action=="accept" {
            // Contract §2.5, IMPL §7.4: there is no acceptance; M1a's
            // one-release 200 no-op is over. Review #283-11: 410 for every
            // id, before any lookup, exactly like the local route.
            return Err((StatusCode::GONE,"delivery acceptance was removed (pipeline contract v2 §2.5)".into()));
        }
        let mut record=control.cluster_delivery(&id).await.map_err(|e|(StatusCode::SERVICE_UNAVAILABLE,e))?.ok_or((StatusCode::NOT_FOUND,"delivery not found".into()))?;
        if method==Method::GET && action.is_empty() {return Ok(discovery_value(&record));}
        if method==Method::POST && action=="source-cleanup/retry" {
            let mut queued=false;
            for copy in &mut record.copies {if copy.status!="removed" && copy.status!="review_required" {copy.next_retry_at=None;queued=true;}}
            if queued {
                record.delivery.source_cleanup.status="pending".into();record.delivery.source_cleanup.next_retry_at=None;
                control.update_cluster_delivery(&record).await.map_err(|e|(StatusCode::SERVICE_UNAVAILABLE,e))?;
            }
            return Ok(discovery_value(&record));
        }
        Err((StatusCode::METHOD_NOT_ALLOWED,"unsupported delivery operation".into()))
    }.await;
    match result {
        Ok(value) => axum::Json(value).into_response(),
        Err((status, error)) => (status, axum::Json(json!({"error":error}))).into_response(),
    }
}

#[derive(Deserialize, Serialize)]
pub(crate) struct MigrationPageRequest {
    after: i64,
    installation: String,
}
#[derive(Deserialize, Serialize)]
pub(crate) struct MigrationPage {
    values: Vec<Value>,
    next_cursor: i64,
    done: bool,
    installation: String,
}
pub(crate) async fn local_delivery_page(
    axum::extract::State(shared): axum::extract::State<std::sync::Arc<crate::leader::LeaderShared>>,
    axum::Json(request): axum::Json<MigrationPageRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let inventory = shared.engine.artifacts();
    if request.after < 0 || inventory.installation != request.installation {
        return (
            axum::http::StatusCode::CONFLICT,
            "migration storage instance changed",
        )
            .into_response();
    }
    match tokio::task::spawn_blocking(move || {
        let (values, next_cursor) = inventory.deliveries_discovery_page(request.after, 32)?;
        Ok::<_, nzbd_state::artifacts::Error>(MigrationPage {
            done: values.is_empty() && next_cursor == request.after,
            values,
            next_cursor,
            installation: inventory.installation.clone(),
        })
    })
    .await
    {
        Ok(Ok(page)) => axum::Json(page).into_response(),
        _ => axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}
pub(crate) async fn migrate_local_deliveries(
    shared: &crate::leader::LeaderShared,
) -> Result<(), String> {
    let control = shared.control.as_ref().ok_or("control unavailable")?;
    if control.delivery_meta("local_imported").await?.is_some() {
        return Ok(());
    }
    let owner = control
        .delivery_meta("migration_owner")
        .await?
        .ok_or("migration owner missing")?;
    let installation = control
        .delivery_meta("migration_storage")
        .await?
        .ok_or("migration storage identity missing")?;
    let after = control
        .delivery_meta("migration_cursor")
        .await?
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let page = if owner == shared.cfg.node_name {
        let inventory = shared.engine.artifacts();
        if inventory.installation != installation {
            return Err(
                "original migration storage was replaced; retain catalog for review".into(),
            );
        }
        tokio::task::spawn_blocking(move || {
            let (values, next_cursor) = inventory.deliveries_discovery_page(after, 32)?;
            Ok::<_, nzbd_state::artifacts::Error>(MigrationPage {
                done: values.is_empty() && next_cursor == after,
                values,
                next_cursor,
                installation: inventory.installation.clone(),
            })
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?
    } else {
        let node = crate::registry::read_nodes(&shared.layout)
            .into_iter()
            .find(|node| node.name == owner)
            .ok_or("original Delivery catalog owner is offline; retry when it returns")?;
        crate::http::ClusterClient::new(shared.cfg.secret.clone())
            .post_json::<_, MigrationPage>(
                &node.api_url,
                "/cluster/v1/deliveries/migrate-page",
                &MigrationPageRequest {
                    after,
                    installation: installation.clone(),
                },
            )
            .await?
    };
    if page.installation != installation {
        return Err("migration reply storage identity changed".into());
    }
    tracing::debug!(owner=%owner,after,next=page.next_cursor,done=page.done,rows=page.values.len(),"adopting bounded Delivery catalog page");
    control
        .import_delivery_page(
            &installation,
            &owner,
            after,
            &page.values,
            page.next_cursor,
            page.done,
        )
        .await
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CopyRequest {
    delivery_id: String,
    copy_id: String,
    claim_id: String,
}
#[derive(Serialize, Deserialize)]
struct CopyAuthority {
    #[serde(default)]
    result_id: String,
    copy: CopyObligation,
    outcome: String,
    receipt: String,
    accepted_at_ms: i64,
    source_disposition: Option<String>,
}
#[derive(Serialize, Deserialize)]
struct CopyResult {
    copy_id: String,
    claim_id: String,
    status: String,
    error: Option<String>,
}

async fn authorized_copy(
    shared: &crate::leader::LeaderShared,
    request: &CopyRequest,
) -> Result<CopyAuthority, String> {
    if !shared.view.borrow().is_me || !shared.authority_ready() {
        return Err("not the active Delivery authority".into());
    }
    let control = shared.control.as_ref().ok_or("control unavailable")?;
    let record = control
        .cluster_delivery(&request.delivery_id)
        .await?
        .ok_or("Delivery not found")?;
    let copy = record
        .copies
        .iter()
        .find(|copy| {
            copy.id == request.copy_id
                && copy.claim_id.as_deref() == Some(request.claim_id.as_str())
                && copy.status == "running"
        })
        .cloned()
        .ok_or("copy cleanup claim changed")?;
    Ok(CopyAuthority {
        result_id: record.result_id,
        copy,
        outcome: record.delivery.terminal_outcome,
        receipt: if record.receipt_id.is_empty() {
            record.delivery.delivery_id
        } else {
            record.receipt_id
        },
        accepted_at_ms: record.delivery.ready_at.saturating_mul(1000),
        source_disposition: record.source_disposition,
    })
}
pub(crate) async fn copy_authority(
    axum::extract::State(shared): axum::extract::State<std::sync::Arc<crate::leader::LeaderShared>>,
    axum::Json(request): axum::Json<CopyRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match authorized_copy(&shared, &request).await {
        Ok(value) => axum::Json(value).into_response(),
        Err(error) => (
            axum::http::StatusCode::CONFLICT,
            axum::Json(json!({"error":error})),
        )
            .into_response(),
    }
}
pub(crate) async fn cleanup_copy(
    axum::extract::State(shared): axum::extract::State<std::sync::Arc<crate::leader::LeaderShared>>,
    axum::Json(request): axum::Json<CopyRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let authority = if shared.view.borrow().is_me {
        authorized_copy(&shared, &request).await
    } else {
        let url = shared.view.borrow().leader_url().map(str::to_owned);
        match url {
            Some(url) => {
                crate::http::ClusterClient::new(shared.cfg.secret.clone())
                    .post_json::<_, CopyAuthority>(
                        &url,
                        "/cluster/v1/deliveries/copy-authority",
                        &request,
                    )
                    .await
            }
            None => Err("Delivery authority unavailable".into()),
        }
    };
    let authority = match authority {
        Ok(authority) => authority,
        Err(error) => {
            return (
                axum::http::StatusCode::CONFLICT,
                axum::Json(json!({"error":error})),
            )
                .into_response();
        }
    };
    let copy = authority.copy.clone();
    {
        if copy.owner_node != shared.cfg.node_name {
            return (
                axum::http::StatusCode::CONFLICT,
                "copy belongs to another node",
            )
                .into_response();
        }
    }
    let inventory = shared.engine.artifacts();
    let root = shared.cfg.shared_dir.join(".nzbd-cluster/generations");
    let operation_key = format!("{}:{}", request.delivery_id, request.copy_id);
    let guarded = shared.clone();
    let result = shared
        .copy_operations
        .run(operation_key, async move {
            let guard = guarded.copy_guards.lock_for(&copy);
            let _physical = guard.lock().await;
            match crate::cluster_keep::check_copy_policy(&guarded, &copy, &authority.outcome).await
            {
                Ok(None) => {}
                policy => {
                    return (
                        axum::http::StatusCode::CONFLICT,
                        axum::Json(nzbd_state::artifacts::ClusterCopyCleanup {
                            status: "retained".into(),
                            error: Some(match policy {
                                Ok(Some(reason)) => reason,
                                Err(error) => error,
                                _ => unreachable!(),
                            }),
                        }),
                    )
                        .into_response();
                }
            }
            let result = tokio::task::spawn_blocking(
                move || -> Result<nzbd_state::artifacts::ClusterCopyCleanup, String> {
                    if let Some(local) = copy.local_delivery_id {
                        let delivery = inventory
                            .retire_delivery_source(&local)
                            .map_err(|e| e.to_string())?;
                        return Ok(nzbd_state::artifacts::ClusterCopyCleanup {
                            status: if delivery.source_cleanup.status == "succeeded" {
                                "removed"
                            } else {
                                "retained"
                            }
                            .into(),
                            error: delivery.source_cleanup.error,
                        });
                    }
                    if let Some(artifact) = copy.artifact {
                        if authority.source_disposition.as_deref()
                            == Some("cancellation_delete_files")
                        {
                            if artifact.selected {
                                return inventory
                                    .delete_cancelled_cluster_unpublished_publication(
                                        &artifact,
                                        nzbd_state::artifacts::FinalizationProof::Cluster {
                                            receipt: authority.receipt,
                                            accepted_at_ms: authority.accepted_at_ms,
                                        },
                                        copy.deletion_files
                                            .as_deref()
                                            .ok_or("unpublished copy awaits exact owner seal")?,
                                    )
                                    .map_err(|e| e.to_string());
                            }
                            return inventory
                                .delete_cancelled_cluster_artifact_copy(
                                    &artifact,
                                    nzbd_state::artifacts::FinalizationProof::Cluster {
                                        receipt: authority.receipt,
                                        accepted_at_ms: authority.accepted_at_ms,
                                    },
                                    copy.deletion_files
                                        .as_deref()
                                        .ok_or("cancelled copy awaits exact owner byte seal")?,
                                )
                                .map_err(|e| e.to_string());
                        }
                        return inventory
                            .retire_cluster_artifact_copy_with_result(
                                &artifact,
                                &authority.outcome,
                                nzbd_state::artifacts::FinalizationProof::Cluster {
                                    receipt: authority.receipt,
                                    accepted_at_ms: authority.accepted_at_ms,
                                },
                                authority.source_disposition.as_deref() == Some("unusable"),
                                copy.deletion_files.as_deref(),
                                &authority.result_id,
                            )
                            .map_err(|e| e.to_string());
                    }
                    let path =
                        std::path::PathBuf::from(copy.result_ref.ok_or("copy has no result path")?);
                    let relative = path
                        .strip_prefix(&root)
                        .map_err(|_| "copy is outside sealed generation namespace")?;
                    let identity = copy
                        .directory_identity
                        .ok_or("legacy copy has no owned directory identity; review required")?;
                    if !path.try_exists().map_err(|e| e.to_string())? {
                        return Ok(nzbd_state::artifacts::ClusterCopyCleanup {
                            status: "removed".into(),
                            error: None,
                        });
                    }
                    let files = copy
                        .deletion_files
                        .ok_or("copy lacks a durable deletion manifest; retain it for review")?;
                    nzbd_state::artifacts::remove_cluster_generation_copy(
                        &root, relative, identity, &files,
                    )
                    .map_err(|e| e.to_string())?;
                    Ok(nzbd_state::artifacts::ClusterCopyCleanup {
                        status: "removed".into(),
                        error: None,
                    })
                },
            )
            .await;
            let (status, error) = match result {
                Ok(Ok(result)) => (result.status, result.error),
                Ok(Err(error)) => ("retry".into(), Some(error)),
                Err(error) => ("retry".into(), Some(error.to_string())),
            };
            // Retained/retry answers may be rechecked after a control change;
            // a removed answer is exact immutable-copy completion evidence.
            let code = if status == "removed" {
                axum::http::StatusCode::OK
            } else {
                axum::http::StatusCode::CONFLICT
            };
            (
                code,
                axum::Json(nzbd_state::artifacts::ClusterCopyCleanup { status, error }),
            )
                .into_response()
        })
        .await;
    let (status, error) = match result {
        Ok((_, body)) => {
            match serde_json::from_slice::<nzbd_state::artifacts::ClusterCopyCleanup>(&body) {
                Ok(result) => (result.status, result.error),
                Err(error) => (
                    "retry".into(),
                    Some(format!("invalid owned copy completion: {error}")),
                ),
            }
        }
        Err(error) => ("retry".into(), Some(error)),
    };
    if status != "removed" {
        tracing::debug!(copy=%request.copy_id,owner=%shared.cfg.node_name,%status,error=?error.as_ref().map(|error|error.chars().take(1024).collect::<String>()),"exact source-copy cleanup awaits owner disposition");
    }
    axum::Json(CopyResult {
        copy_id: request.copy_id,
        claim_id: request.claim_id,
        status,
        error,
    })
    .into_response()
}

/// One copy at a time, outside queue mutation/heartbeat serialization. Every
/// ACK merges only its own still-current claim through replicated row CAS.
pub(crate) async fn cleanup_one_copy(
    shared: &std::sync::Arc<crate::leader::LeaderShared>,
    cursor: &mut i64,
) -> Result<(), String> {
    let control = shared.control.as_ref().ok_or("control unavailable")?;
    let (rows, highwater) = control.cluster_deliveries(*cursor, 16).await?;
    if rows.is_empty() {
        *cursor = if highwater > *cursor { highwater } else { 0 };
        return Ok(());
    }
    let now = crate::control::unix_ms_public() / 1000;
    let mut selected = None;
    for mut record in rows {
        *cursor = record.delivery.sequence;
        if record.source_disposition.as_deref() == Some("cancellation_delete_files")
            && record
                .copies
                .iter()
                .any(|copy| copy.artifact.is_some() && copy.status == "review_required")
        {
            crate::cancellation_routes::refresh_owner_seals(shared, &record).await?;
            record = control
                .cluster_delivery(&record.delivery.delivery_id)
                .await?
                .ok_or("cancellation ledger disappeared")?;
        }
        let Some(index) = record.copies.iter().position(|copy| {
            copy.status != "removed"
                && copy.status != "review_required"
                && copy.next_retry_at.is_none_or(|deadline| deadline <= now)
        }) else {
            continue;
        };
        let mut copy = record.copies[index].clone();
        let claim = format!("{}:{}:{}", shared.owner_incarnation, record.revision, index);
        copy.claim_id = Some(claim.clone());
        copy.status = "running".into();
        copy.attempts = copy.attempts.saturating_add(1);
        record.copies[index] = copy.clone();
        {
            let _serial = shared.delivery_serial.lock().await;
            control.update_cluster_delivery(&record).await?;
        }
        selected = Some((record.delivery.delivery_id, copy, claim));
        break;
    }
    let Some((delivery_id, copy, claim_id)) = selected else {
        *cursor = highwater;
        return Ok(());
    };
    let request = CopyRequest {
        delivery_id: delivery_id.clone(),
        copy_id: copy.id.clone(),
        claim_id: claim_id.clone(),
    };
    let client = crate::http::ClusterClient::new(shared.cfg.secret.clone());
    let url = if copy.owner_node == shared.cfg.node_name {
        Some(shared.cfg.advertise_url.clone())
    } else {
        crate::registry::read_nodes(&shared.layout)
            .into_iter()
            .find(|node| node.name == copy.owner_node)
            .map(|node| node.api_url)
    };
    let result = match url {
        Some(url) => {
            client
                .post_json::<_, CopyResult>(&url, "/cluster/v1/deliveries/cleanup-copy", &request)
                .await
        }
        None => Err(format!(
            "copy owner {} is offline; retry when that exact node storage returns",
            copy.owner_node
        )),
    };
    if !shared.view.borrow().is_me {
        return Ok(());
    }
    for _ in 0..3 {
        let _serial = shared.delivery_serial.lock().await;
        let mut current = control
            .cluster_delivery(&delivery_id)
            .await?
            .ok_or("Delivery disappeared")?;
        let Some(index) = current.copies.iter().position(|copy| {
            copy.id == request.copy_id && copy.claim_id.as_deref() == Some(claim_id.as_str())
        }) else {
            return Ok(());
        };
        let item = &mut current.copies[index];
        match &result {
            Ok(reply)
                if reply.copy_id == request.copy_id
                    && reply.claim_id == claim_id
                    && ["removed", "retained", "retry"].contains(&reply.status.as_str()) =>
            {
                item.status = reply.status.clone();
                item.error = reply.error.clone();
            }
            Ok(_) => {
                item.status = "retry".into();
                item.error =
                    Some("copy owner returned a mismatched cleanup acknowledgement".into());
            }
            Err(error) => {
                item.status = "retry".into();
                item.error = Some(error.clone());
            }
        }
        item.next_retry_at = (item.status != "removed").then(|| {
            let base = match item.attempts {
                0 | 1 => 30i64,
                2 => 60,
                3 => 120,
                _ => 300,
            };
            use sha2::{Digest, Sha256};
            let digest = Sha256::digest(format!("{}:{}:{now}", item.id, item.attempts));
            now + base + i64::from(digest[0]) % (base / 10 + 1)
        });
        let all_removed = current.copies.iter().all(|copy| copy.status == "removed");
        let active = current
            .copies
            .iter()
            .any(|copy| matches!(copy.status.as_str(), "pending" | "running" | "retry"));
        let cleanup = &mut current.delivery.source_cleanup;
        cleanup.attempts = cleanup.attempts.saturating_add(1);
        cleanup.started_at.get_or_insert(now);
        cleanup.last_attempt_at = Some(now);
        cleanup.status = if all_removed {
            "succeeded"
        } else if active {
            "retry"
        } else {
            "blocked"
        }
        .into();
        cleanup.error = current.copies.iter().find_map(|copy| {
            copy.error
                .as_ref()
                .map(|error| format!("{} on {}: {error}", copy.id, copy.owner_node))
        });
        cleanup.next_retry_at = current
            .copies
            .iter()
            .filter_map(|copy| copy.next_retry_at)
            .min();
        if all_removed {
            cleanup.completed_at = Some(now);
            cleanup.blocked_since = None;
            cleanup.warning = None;
        } else if active {
            cleanup.blocked_since.get_or_insert(now);
            cleanup.warning = cleanup
                .blocked_since
                .filter(|since| now - *since >= 1800)
                .map(|_| "Source-copy cleanup has retried for at least 30 minutes".into());
        }
        match control.update_cluster_delivery(&current).await {
            Ok(()) => return Ok(()),
            Err(_) => continue,
        }
    }
    Err("copy progress raced a newer receipt update; owner result will replay".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;
    use std::{collections::HashMap, time::Duration};
    #[test]
    fn catalog_mutation_excludes_other_routes_and_malformed_ids() {
        for (method, path, expected) in [
            ("POST", "/api/v1/deliveries/exact%3Aid/accept", true),
            (
                "POST",
                "/api/v1/deliveries/exact/source-cleanup/retry",
                true,
            ),
            ("DELETE", "/api/v1/deliveries/exact/accept", false),
            ("POST", "/api/v1/deliveries/exact/accept/extra", false),
            ("POST", "/api/v1/deliveries/exact/unknown", false),
            ("POST", "/api/v1/deliveries//accept", false),
            ("POST", "/api/v1/deliveries/bad%2Fid/accept", false),
            ("POST", "/api/v1/jobs/7", false),
        ] {
            let request = hyper::Request::builder()
                .method(method)
                .uri(path)
                .body(axum::body::Body::empty())
                .unwrap();
            assert_eq!(is_catalog_mutation(&request), expected, "{method} {path}");
        }
    }
    fn record(instance: &str, id: &str) -> ClusterDelivery {
        let delivery:Delivery=serde_json::from_value(json!({
            "protocolVersion":1,"sequence":0,"deliveryId":id,"publicationGeneration":"generation",
            "artifactId":"publication","sourceArtifactId":"source","sourceGeneration":"source-generation","jobId":7,
            "runnerInstanceId":instance,"requestToken":"request","keepSourceOnFailure":false,"terminalOutcome":"SUCCESS",
            "publicationPath":"/completed/payload","manifestDigest":"digest","files":[{"path":"episode.mkv","sizeBytes":7,"sha256":"00".repeat(32)}],
            "fileOutcomes":[],"readyAt":1,
            "sourceCleanup":{"status":"pending","error":null,"attempts":0,"nextRetryAt":null,"blockedSince":null,"warning":null}
        })).unwrap();
        ClusterDelivery {
            delivery,
            receipt_id: format!("receipt-{id}"),
            result_id: format!("result-{id}"),
            copies: Vec::new(),
            revision: 0,
            cleanup_only: false,
            source_disposition: None,
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn legacy_wal_reopens_preserving_authority_and_accepts_bounded_large_delivery() {
        let temp = tempfile::tempdir().unwrap();
        let address = "127.0.0.1:1";
        let raft = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let raft_address = raft.local_addr().unwrap();
        let api = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let api_address = api.local_addr().unwrap();
        let cfg = crate::ClusterConfig {
            cluster_id: "receipt-test".into(),
            node_name: "leader".into(),
            shared_dir: temp.path().join("shared"),
            advertise_url: format!("http://{address}"),
            secret: "secret".into(),
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            pp_slots: 0,
            lease_interval: Duration::from_millis(100),
            takeover_after: Duration::from_secs(1),
            worker_ttl: Duration::from_secs(2),
            control_dir: temp.path().join("control"),
            control_node_id: 1,
            control_raft_bind: raft_address.to_string(),
            control_api_bind: api_address.to_string(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: Vec::new(),
            torrent_payload_roots: Vec::new(),
        };
        drop(raft);
        drop(api);
        let old = ControlStore::start_with_wal_size(&cfg, 8 * 1024 * 1024)
            .await
            .unwrap();
        old.seed_job(7, "old-incarnation", "{}").await.unwrap();
        let claim = old
            .acquire(
                "work/job/7/post",
                "receipt-test",
                7,
                "old-incarnation",
                "worker",
                "worker-boot",
                "post",
                "{}",
                1,
                Duration::from_secs(180),
            )
            .await
            .unwrap();
        let crate::control::LeaseClaim::Acquired(token) = claim else {
            panic!("fresh lease")
        };
        assert!(matches!(
            old.publish_result(
                "old-publication",
                &token,
                1,
                "old-result",
                "/sealed/old",
                "{}"
            )
            .await
            .unwrap(),
            crate::control::MutationOutcome::Applied { .. }
        ));
        old.seed_job(8, "live-incarnation", "{}").await.unwrap();
        let claim = old
            .acquire(
                "work/job/8/download",
                "receipt-test",
                8,
                "live-incarnation",
                "worker",
                "worker-boot",
                "download",
                "{}",
                1,
                Duration::from_secs(180),
            )
            .await
            .unwrap();
        let crate::control::LeaseClaim::Acquired(live) = claim else {
            panic!("fresh live lease")
        };
        old.initialize_delivery_identity("old-installation", "leader")
            .await
            .unwrap();
        old.commit_delivery(&record("old-installation", "old-delivery"))
            .await
            .unwrap();
        old.shutdown().await.unwrap();
        drop(old);
        let store = ControlStore::start(&cfg).await.unwrap();
        assert_eq!(
            store.job_identity(7).await.unwrap(),
            Some(("old-incarnation".into(), 2))
        );
        assert_eq!(
            store.current_lease("work/job/8/download").await.unwrap(),
            Some(live)
        );
        assert_eq!(
            store
                .publication_record("old-publication")
                .await
                .unwrap()
                .unwrap()
                .result_id,
            "old-result"
        );
        assert_eq!(
            store
                .cluster_delivery("old-delivery")
                .await
                .unwrap()
                .unwrap()
                .delivery
                .publication_generation,
            "generation"
        );
        let mut large = record("old-installation", "large-after-upgrade");
        large.copies.push(CopyObligation {
            id: "large-proof".into(),
            owner_node: "offline-owner".into(),
            artifact: None,
            local_delivery_id: None,
            result_ref: Some("/sealed/large".into()),
            result_id: Some("large-result".into()),
            directory_identity: Some((1, 2)),
            deletion_files: Some(
                (0..60000)
                    .map(|index| nzbd_state::artifacts::DeliveryFile {
                        path: format!("files/{index:08}-{}", "p".repeat(80)),
                        size_bytes: 1,
                        sha256: "00".repeat(32),
                    })
                    .collect(),
            ),
            status: "review_required".into(),
            error: None,
            claim_id: None,
            attempts: 0,
            next_retry_at: None,
        });
        assert!(serde_json::to_vec(&large).unwrap().len() > 8 * 1024 * 1024);
        store.commit_delivery(&large).await.unwrap();
        assert_eq!(
            store
                .cluster_delivery("large-after-upgrade")
                .await
                .unwrap()
                .unwrap()
                .copies[0]
                .deletion_files
                .as_ref()
                .unwrap()
                .len(),
            60000
        );
        let escaped = params!("\n".repeat(33 * 1024 * 1024));
        assert!(bounded_statement(
            "INSERT INTO cluster_delivery_meta(key,value) VALUES('oversize',?1)",
            &escaped
        )
        .unwrap_err()
        .contains("64 MiB"));
        let mut oversized = store
            .cluster_delivery("old-delivery")
            .await
            .unwrap()
            .unwrap();
        oversized.delivery.failure_cause = Some("x".repeat(crate::proto::MAX_RPC_BYTES));
        assert!(store
            .update_cluster_delivery(&oversized)
            .await
            .unwrap_err()
            .contains("32 MiB"));
        assert_eq!(
            store
                .cluster_delivery("old-delivery")
                .await
                .unwrap()
                .unwrap()
                .delivery
                .publication_generation,
            "generation"
        );
        store.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn replicated_delivery_cas_and_real_discovery_preserve_progress_past_bad_rows() {
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let raft = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let raft_address = raft.local_addr().unwrap();
        let api = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let api_address = api.local_addr().unwrap();
        let cfg = crate::ClusterConfig {
            cluster_id: "receipt-test".into(),
            node_name: "leader".into(),
            shared_dir: temp.path().join("shared"),
            advertise_url: format!("http://{address}"),
            secret: "secret".into(),
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            pp_slots: 0,
            lease_interval: Duration::from_millis(100),
            takeover_after: Duration::from_secs(1),
            worker_ttl: Duration::from_secs(2),
            control_dir: temp.path().join("control"),
            control_node_id: 1,
            control_raft_bind: raft_address.to_string(),
            control_api_bind: api_address.to_string(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: Vec::new(),
            torrent_payload_roots: Vec::new(),
        };
        drop(raft);
        drop(api);
        let runtime = crate::ClusterRuntime::start(
            cfg,
            Vec::new(),
            nzbd_engine::Tuning {
                min_free_disk_bytes: 0,
                ..Default::default()
            },
            temp.path().join("completed"),
            None,
            Some(1),
            None,
        )
        .await
        .unwrap();
        let app = runtime.router("test", Vec::new());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while !runtime.leader_shared.authority_ready() {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let store = runtime.control.as_ref().unwrap();
        let instance = store.delivery_instance().await.unwrap();
        // No broadcast is emitted: the cluster must poll/replay durable facts.
        let origin = nzbd_types::Job {
            id: nzbd_types::JobId(800),
            kind: nzbd_types::JobKind::Nzb,
            name: "provisional".into(),
            dir_name: "provisional".into(),
            name_provisional: true,
            delivery_lifecycle: false,
            queued_at_unix: 123,
            original_name: String::new(),
            category: Some("old-category".into()),
            priority: 0,
            dupe: Default::default(),
            params: vec![
                (
                    "*URL".into(),
                    "https://invalid.example/admission.nzb".into(),
                ),
                ("*URL:AdmissionGeneration".into(), "exact-generation".into()),
            ],
            files: Vec::new(),
            totals: Default::default(),
            status: nzbd_types::JobStatus::Fetching,
            torrent: None,
            stages: Vec::new(),
        };
        // Fresh admission + job is one transaction. A stale request cannot
        // occupy a later leader's ID, and failed job CAS commits no receipt.
        let admission:nzbd_state::UrlAdmissionReceipt=serde_json::from_value(json!({"protocolVersion":1,"requestToken":"catalog-cas","submissionSha256":"exact-submission","jobId":5000,"generation":"exact-generation","runnerInstanceId":instance,"custodyState":"runner_owned"})).unwrap();
        let mut fresh = origin.clone();
        fresh.id = nzbd_types::JobId(5000);
        fresh
            .params
            .push(("monarr-transfer".into(), admission.request_token.clone()));
        let fences = store.job_revision_fences().await.unwrap();
        store
            .commit_job_delta_guarded(
                &[(5000, Some(fresh.clone()))],
                &fences,
                std::slice::from_ref(&admission),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .admission("catalog-cas")
                .await
                .unwrap()
                .unwrap()
                .job_id,
            5000
        );
        let mut fork = admission.clone();
        fork.job_id = 5001;
        assert!(store.import_admissions(&[fork]).await.is_err());
        let stale = store.job_revision_fences().await.unwrap();
        fresh.name = "newer durable intent".into();
        store
            .commit_job_delta(&[(5000, Some(fresh.clone()))])
            .await
            .unwrap();
        let mut orphan = admission.clone();
        orphan.request_token = "uncommitted-local-only".into();
        fresh.name = "stale request must not win".into();
        assert!(store
            .commit_job_delta_guarded(
                &[(5000, Some(fresh.clone()))],
                &stale,
                std::slice::from_ref(&orphan),
                None
            )
            .await
            .is_err());
        assert!(store
            .admission(&orphan.request_token)
            .await
            .unwrap()
            .is_none());
        // Absence in the caller's pre-request fence means INSERT-only, even
        // when a later owner has since occupied this same integer ID.
        assert!(store
            .commit_job_delta_guarded(
                &[(5000, Some(fresh))],
                &std::collections::HashMap::new(),
                std::slice::from_ref(&orphan),
                None
            )
            .await
            .is_err());
        let (jobs, _) = store.highwater().await.unwrap();
        assert!(jobs >= 5000);
        let mut resolved = origin.clone();
        resolved.name = "provider-name".into();
        resolved.dir_name = "provider-dir".into();
        resolved.name_provisional = false;
        resolved.category = Some("provider-category".into());
        resolved.status = nzbd_types::JobStatus::Failed;
        resolved
            .params
            .push(("Failure:Reason".into(), "unsupported NZB".into()));
        let mut current = origin.clone();
        current.name = "user-name".into();
        current.dir_name = "user-dir".into();
        current.name_provisional = false;
        current.category = None;
        current.priority = 9;
        current
            .params
            .push(("*Runner:KeepOnFailure".into(), "1".into()));
        let serial = runtime.leader_shared.mutation_serial.lock().await;
        store
            .seed_job(
                800,
                "url-incarnation",
                &serde_json::to_string(&current).unwrap(),
            )
            .await
            .unwrap();
        let spool = nzbd_state::artifacts::UrlResolutionStore::open(
            &runtime.leader_shared.layout.state_dir(),
        )
        .unwrap();
        let prepared = spool.stage(&origin, &resolved).unwrap();
        drop(spool);
        let reopened = nzbd_state::artifacts::UrlResolutionStore::open(
            &runtime.leader_shared.layout.state_dir(),
        )
        .unwrap();
        assert!(reopened
            .pending(8)
            .unwrap()
            .iter()
            .any(|row| row.resolution_id == prepared.resolution_id));
        // Crash boundary: replicated commit won, old process died before ACK.
        assert!(store
            .commit_url_outbox_with_source(&prepared, None)
            .await
            .unwrap());
        drop(serial);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while runtime
            .engine
            .url_resolution_outbox(8)
            .await
            .unwrap()
            .iter()
            .any(|row| row.resolution_id == prepared.resolution_id)
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "startup/poll outbox did not acknowledge durable replay"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let selected = store
            .load_jobs()
            .await
            .unwrap()
            .into_iter()
            .find(|(job, _, _)| job.id.0 == 800)
            .unwrap()
            .0;
        assert_eq!(selected.name, "user-name");
        assert_eq!(selected.dir_name, "user-dir");
        assert_eq!(selected.category, None);
        assert_eq!(selected.priority, 9);
        assert!(selected
            .params
            .contains(&("*Runner:KeepOnFailure".into(), "1".into())));
        assert_eq!(selected.status, nzbd_types::JobStatus::Failed);
        // A row an M1a (or older) node wrote with acceptance fields still
        // imports and is listed: the feed returns every row (contract §2.5).
        let mut legacy =
            serde_json::to_value(&record(&instance, "accepted-local").delivery).unwrap();
        legacy["sequence"] = json!(41);
        legacy["status"] = json!("accepted");
        legacy["acceptedAt"] = json!(5);
        legacy["consumerReceiptId"] = json!("existing-local-receipt");
        store
            .import_delivery_page(&instance, "leader", 0, &[legacy], 41, true)
            .await
            .unwrap();
        assert_eq!(
            store
                .delivery_meta("migration_cursor")
                .await
                .unwrap()
                .as_deref(),
            Some("41")
        );
        let (all, _) = store.cluster_discovery_page(0, 100).await.unwrap();
        let imported = all
            .iter()
            .find(|row| row["deliveryId"] == "accepted-local")
            .unwrap();
        assert_eq!(imported["sequence"], 41);
        assert!(imported.get("consumerReceiptId").is_none());
        assert!(imported.get("status").is_none());
        assert!(imported["sourceCopies"].is_array());
        let committed = store
            .commit_delivery(&record(&instance, "first"))
            .await
            .unwrap();
        let mut stale = store.cluster_delivery("first").await.unwrap().unwrap();
        let mut newer = store.cluster_delivery("first").await.unwrap().unwrap();
        newer.delivery.source_cleanup.attempts = 1;
        store.update_cluster_delivery(&newer).await.unwrap();
        stale.delivery.source_cleanup.status = "succeeded".into();
        assert!(
            store.update_cluster_delivery(&stale).await.is_err(),
            "old leader cleanup cannot overwrite a newer update"
        );
        assert_eq!(
            store
                .cluster_delivery("first")
                .await
                .unwrap()
                .unwrap()
                .delivery
                .source_cleanup
                .attempts,
            1
        );
        let mut changed = record(&instance, "first");
        changed.delivery.source_generation = "changed".into();
        assert!(store.commit_delivery(&changed).await.is_err());
        assert_eq!(
            store
                .cluster_delivery("first")
                .await
                .unwrap()
                .unwrap()
                .delivery
                .sequence,
            committed.delivery.sequence
        );
        store.client.execute("INSERT INTO cluster_deliveries(id,data,public_data) VALUES('invalid','{bad','{bad')",params!()).await.unwrap();
        store
            .client
            .execute(
                "INSERT INTO cluster_deliveries(id,data,public_data) VALUES('oversized','{}',?1)",
                params!(json!({"large":"x".repeat(8*1024*1024+1)}).to_string()),
            )
            .await
            .unwrap();
        let mut private = record(&instance, "private-proof");
        private.copies.push(CopyObligation {
            id: "private-copy".into(),
            owner_node: "offline-owner".into(),
            artifact: None,
            local_delivery_id: None,
            result_ref: Some("/retained/sealed-copy".into()),
            result_id: Some("private-result".into()),
            directory_identity: Some((1, 2)),
            deletion_files: Some(
                (0..60_000)
                    .map(|index| nzbd_state::artifacts::DeliveryFile {
                        path: format!("files/{index:08}-{}", "p".repeat(80)),
                        size_bytes: 1,
                        sha256: "00".repeat(32),
                    })
                    .collect(),
            ),
            status: "review_required".into(),
            error: Some("retained proof".into()),
            claim_id: None,
            attempts: 0,
            next_retry_at: None,
        });
        assert!(serde_json::to_vec(&private).unwrap().len() > 8 * 1024 * 1024);
        store.commit_delivery(&private).await.unwrap();
        let mut excess = record(&instance, "too-many-copies");
        excess.copies = (0..65)
            .map(|index| {
                let mut copy = private.copies[0].clone();
                copy.id = format!("small-copy-{index}");
                copy.deletion_files = None;
                copy
            })
            .collect();
        store.commit_delivery(&excess).await.unwrap();
        assert_eq!(
            store
                .cluster_delivery("too-many-copies")
                .await
                .unwrap()
                .unwrap()
                .copies
                .len(),
            65
        );
        let mut encoded = record(&instance, "encoded:sha256:receipt");
        encoded.delivery.source_cleanup.status = "succeeded".into();
        store.commit_delivery(&encoded).await.unwrap();
        let client = crate::http::ClusterClient::new("secret".into());
        // Queue reconciliation may be blocked by unrelated payload custody.
        // The receipt lane must remain independent while preserving its CAS.
        let queue_serial = runtime.leader_shared.mutation_serial.lock().await;
        for (method, action, body) in [
            ("GET", "", String::new()),
            ("POST", "/source-cleanup/retry", String::new()),
        ] {
            let request = hyper::Request::builder()
                .method(method)
                .uri(format!(
                    "/api/v1/deliveries/encoded%3Asha256%3Areceipt{action}"
                ))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap();
            let response = client
                .forward(&format!("http://{address}"), request)
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                axum::http::StatusCode::OK,
                "encoded {method} {action}"
            );
        }
        let response = client
            .forward(
                &format!("http://{address}"),
                hyper::Request::post("/api/v1/deliveries/encoded%3Asha256%3Areceipt/accept")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        json!({
                            "protocolVersion":1,"publicationGeneration":"wrong-generation",
                            "manifestDigest":"digest","consumerReceiptId":"encoded-consumer"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        // Contract §2.5 / IMPL §7.4: there is no acceptance; the route is gone.
        assert_eq!(response.status(), axum::http::StatusCode::GONE);
        // Review #283-11: gone for an unknown id too, as on the local route.
        let response = client
            .forward(
                &format!("http://{address}"),
                hyper::Request::post("/api/v1/deliveries/no-such-delivery/accept")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::GONE);
        drop(queue_serial);
        for id in ["bad%ZZ", "bad%2Fsegment", "bad%FF"] {
            let response = client
                .forward(
                    &format!("http://{address}"),
                    hyper::Request::get(format!("/api/v1/deliveries/{id}"))
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
        }
        {
            let _serial = runtime.leader_shared.mutation_serial.lock().await;
            assert!(runtime
                .engine
                .adopt_replicated_highwater(6000, 900)
                .await
                .unwrap());
            let before = runtime.leader_shared.engine_projection().await.unwrap();
            let fences = store
                .job_revision_fences_for(&before.keys().copied().collect::<Vec<_>>())
                .await
                .unwrap();
            let token = "failed-native-admission";
            let opts = nzbd_engine::AddOpts {
                params: vec![
                    ("monarr-transfer".into(), token.into()),
                    ("monarr-delivery".into(), "1".into()),
                ],
                paused: true,
                ..Default::default()
            };
            let local = runtime
                .engine
                .add_url(
                    "uncommitted local",
                    "http://127.0.0.1:9/no-descriptor",
                    opts,
                )
                .await
                .unwrap();
            let mut occupied = origin.clone();
            occupied.id = local;
            occupied.name = "different authoritative job".into();
            occupied
                .params
                .push(("monarr-transfer".into(), "different-request".into()));
            store
                .seed_job(
                    u64::from(local.0),
                    "foreign-committed-job",
                    &serde_json::to_string(&occupied).unwrap(),
                )
                .await
                .unwrap();
            assert!(
                runtime
                    .leader_shared
                    .commit_mutation_delta(&before, &fences, &Default::default())
                    .await
                    .is_err(),
                "fresh admission cannot overwrite later occupied ID"
            );
            crate::admissions::rollback_pending(&runtime.leader_shared)
                .await
                .unwrap();
            assert!(runtime.engine.url_admission(token).await.unwrap().is_none());
            assert!(store.admission(token).await.unwrap().is_none());
            assert_eq!(
                runtime
                    .engine
                    .export_job(local)
                    .await
                    .unwrap()
                    .unwrap()
                    .name,
                occupied.name
            );
            assert!(
                !runtime
                    .engine
                    .url_resolution_outbox(100)
                    .await
                    .unwrap()
                    .iter()
                    .any(|record| record.origin.id == local),
                "exact unaccepted URL cache must be cancelled"
            );
            // Another leader may have committed the same token on a different
            // ID while this local admission was uncertain. Cancel A before
            // adopting B, preserving B and every unrelated occupied row.
            let token = "replacement-admission";
            let opts = nzbd_engine::AddOpts {
                params: vec![
                    ("monarr-transfer".into(), token.into()),
                    ("monarr-delivery".into(), "1".into()),
                ],
                paused: true,
                ..Default::default()
            };
            let old = runtime
                .engine
                .add_url("old local", "http://127.0.0.1:9/old", opts)
                .await
                .unwrap();
            let mut accepted = runtime.engine.url_admission(token).await.unwrap().unwrap();
            let replacement = nzbd_types::JobId(old.0 + 10);
            accepted.job_id = replacement.0;
            accepted.generation = format!(
                "{}-url-{}-{:x}",
                accepted.runner_instance_id,
                replacement.0,
                sha2::Sha256::digest(token.as_bytes())
            );
            let mut replacement_job = runtime.engine.export_job(old).await.unwrap().unwrap();
            replacement_job.id = replacement;
            replacement_job.name = "accepted replacement".into();
            let fences = store
                .job_revision_fences_for(&[u64::from(replacement.0)])
                .await
                .unwrap();
            store
                .commit_job_delta_guarded(
                    &[(u64::from(replacement.0), Some(replacement_job))],
                    &fences,
                    std::slice::from_ref(&accepted),
                    None,
                )
                .await
                .unwrap();
            crate::admissions::rollback_pending(&runtime.leader_shared)
                .await
                .unwrap();
            assert_eq!(
                runtime
                    .engine
                    .url_admission(token)
                    .await
                    .unwrap()
                    .unwrap()
                    .job_id,
                replacement.0
            );
            assert!(runtime.engine.export_job(old).await.unwrap().is_none());
            assert_eq!(
                runtime
                    .engine
                    .export_job(replacement)
                    .await
                    .unwrap()
                    .unwrap()
                    .name,
                "accepted replacement"
            );
            assert!(!runtime
                .engine
                .url_resolution_outbox(100)
                .await
                .unwrap()
                .iter()
                .any(|record| record.origin.id == old));
        }
        let final_record = store
            .commit_delivery(&record(&instance, "valid-after-bad"))
            .await
            .unwrap();
        let client = crate::http::ClusterClient::new("secret".into());
        let response = client
            .forward(
                &format!("http://{address}"),
                hyper::Request::get("/api/v1/deliveries?after=0&includeAccepted=true")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        use http_body_util::BodyExt;
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(body.len() < 16 * 1024 * 1024);
        let page: Value = serde_json::from_slice(&body).unwrap();
        let rows = page["deliveries"].as_array().unwrap();
        let by_id: HashMap<_, _> = rows
            .iter()
            .map(|row| (row["deliveryId"].as_str().unwrap(), row))
            .collect();
        assert!(by_id["invalid"]["discoveryError"].is_string());
        assert!(by_id["oversized"]["discoveryError"].is_string());
        assert!(by_id["too-many-copies"]["discoveryError"]
            .as_str()
            .unwrap()
            .contains("64 source copies"));
        assert!(
            by_id["private-proof"].get("discoveryError").is_none(),
            "private deletion proof bytes cannot reject a small public envelope"
        );
        assert!(by_id["private-proof"]["sourceCopies"][0]
            .get("deletionFiles")
            .is_none());
        assert_eq!(page["nextCursor"], final_record.delivery.sequence);
        assert_eq!(by_id["first"]["sequence"], committed.delivery.sequence);
        assert!(
            by_id["first"].get("consumerReceiptId").is_none(),
            "no acceptance on the feed"
        );
        // Force an actual false actor commit after its snapshot fence fails.
        // A directory at the projection path also defeats concurrent election
        // projection rewrites until we repair it; no sleep-based race proof.
        let leader_file = runtime.leader_shared.layout.leader_file();
        let leader_bytes = std::fs::read(&leader_file).unwrap();
        std::fs::remove_file(&leader_file).unwrap();
        std::fs::create_dir(&leader_file).unwrap();
        assert!(!runtime
            .engine
            .adopt_replicated_highwater(9000, 9000)
            .await
            .unwrap());
        assert!(
            crate::admissions::adopt(&runtime.leader_shared)
                .await
                .is_err(),
            "false engine boundary must refuse admission adoption"
        );
        runtime.leader_shared.fence_authority();
        assert!(!runtime.leader_shared.authority_ready());
        std::fs::remove_dir(&leader_file).unwrap();
        std::fs::write(&leader_file, leader_bytes).unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !runtime.leader_shared.authority_ready() {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            runtime
                .engine
                .url_admission("replacement-admission")
                .await
                .unwrap()
                .unwrap()
                .job_id,
            store
                .admission("replacement-admission")
                .await
                .unwrap()
                .unwrap()
                .job_id
        );
        // A superseded initializer cannot commit even no-op migration writes.
        runtime.cancel.cancel();
        runtime.tracker.wait().await;
        store.client.execute("UPDATE work_leases SET owner_incarnation='replacement-authority' WHERE resource='cluster/leader'",params!()).await.unwrap();
        assert!(store
            .initialize_delivery_identity_guarded(
                "wrong-storage",
                "wrong-owner",
                Some(runtime.leader_shared.authority_identity())
            )
            .await
            .is_err());
        assert_eq!(store.delivery_instance().await.unwrap(), instance);
        server.abort();
        runtime.shutdown().await;
    }
}
