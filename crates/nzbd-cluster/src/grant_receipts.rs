//! Immutable bounded grant provenance; never current permission to mutate bytes.
use crate::control::{ControlStore, LeaseToken};
use hiqlite::{params, Row};

pub(crate) const SCHEMA:&[&str]=&[
    "CREATE TABLE IF NOT EXISTS work_grant_receipts(resource TEXT NOT NULL CHECK(length(CAST(resource AS BLOB))<=256),fence INTEGER NOT NULL,job_id INTEGER NOT NULL,job_incarnation TEXT NOT NULL CHECK(length(CAST(job_incarnation AS BLOB))<=256),job_revision INTEGER NOT NULL,owner_node_id TEXT NOT NULL CHECK(length(CAST(owner_node_id AS BLOB))<=128),owner_incarnation TEXT NOT NULL CHECK(length(CAST(owner_incarnation AS BLOB))<=128),initial_revision INTEGER NOT NULL,initial_expiry_ms INTEGER NOT NULL,kind TEXT NOT NULL CHECK(length(kind)<=32),source_binding_json TEXT CHECK(source_binding_json IS NULL OR length(CAST(source_binding_json AS BLOB))<=8192),issued_at_ms INTEGER NOT NULL,PRIMARY KEY(resource,fence)) STRICT",
    "CREATE INDEX IF NOT EXISTS work_grant_receipts_job ON work_grant_receipts(job_id,job_incarnation)",
    "CREATE TRIGGER IF NOT EXISTS grant_receipt_insert AFTER INSERT ON work_leases WHEN NEW.resource<>'cluster/leader' BEGIN INSERT INTO work_grant_receipts(resource,fence,job_id,job_incarnation,job_revision,owner_node_id,owner_incarnation,initial_revision,initial_expiry_ms,kind,source_binding_json,issued_at_ms) VALUES(NEW.resource,NEW.fence,NEW.job_id,NEW.job_incarnation,NEW.job_revision,NEW.owner_node_id,NEW.owner_incarnation,NEW.revision,NEW.expires_at_ms,NEW.kind,(SELECT json_extract(p.value,'$[1]') FROM control_jobs j,json_each(j.intent_json,'$.params') p WHERE j.job_id=NEW.job_id AND json_extract(p.value,'$[0]')='*Cluster:AuthoritySourceCopy' LIMIT 1),NEW.updated_at_ms); END",
    "CREATE TRIGGER IF NOT EXISTS grant_receipt_regrant AFTER UPDATE OF fence ON work_leases WHEN NEW.resource<>'cluster/leader' AND NEW.fence<>OLD.fence BEGIN INSERT INTO work_grant_receipts(resource,fence,job_id,job_incarnation,job_revision,owner_node_id,owner_incarnation,initial_revision,initial_expiry_ms,kind,source_binding_json,issued_at_ms) VALUES(NEW.resource,NEW.fence,NEW.job_id,NEW.job_incarnation,NEW.job_revision,NEW.owner_node_id,NEW.owner_incarnation,NEW.revision,NEW.expires_at_ms,NEW.kind,(SELECT json_extract(p.value,'$[1]') FROM control_jobs j,json_each(j.intent_json,'$.params') p WHERE j.job_id=NEW.job_id AND json_extract(p.value,'$[0]')='*Cluster:AuthoritySourceCopy' LIMIT 1),NEW.updated_at_ms); END",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HistoricalGrant {
    /// Original token at claim; later renewals may advance revision/expiry.
    pub initial_token: LeaseToken,
    pub job_id: u32,
    pub job_incarnation: String,
    pub job_revision: u64,
    pub kind: String,
    /// Exact authority-owned source fact from the job row, not a foreign inode.
    pub source_binding_json: Option<String>,
    pub issued_at_unix_ms: i64,
}
struct GrantRow {
    resource: String,
    fence: i64,
    job_id: i64,
    job_incarnation: String,
    job_revision: i64,
    owner_node_id: String,
    owner_incarnation: String,
    initial_revision: i64,
    initial_expiry_ms: i64,
    kind: String,
    source_binding_json: Option<String>,
    issued_at_ms: i64,
}
impl From<&mut Row<'_>> for GrantRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            resource: row.get("resource"),
            fence: row.get("fence"),
            job_id: row.get("job_id"),
            job_incarnation: row.get("job_incarnation"),
            job_revision: row.get("job_revision"),
            owner_node_id: row.get("owner_node_id"),
            owner_incarnation: row.get("owner_incarnation"),
            initial_revision: row.get("initial_revision"),
            initial_expiry_ms: row.get("initial_expiry_ms"),
            kind: row.get("kind"),
            source_binding_json: row.get("source_binding_json"),
            issued_at_ms: row.get("issued_at_ms"),
        }
    }
}
impl ControlStore {
    pub(crate) async fn historical_grant(
        &self,
        resource: &str,
        fence: u64,
    ) -> Result<Option<HistoricalGrant>, String> {
        if resource.is_empty() || resource.len() > 256 || fence == 0 {
            return Err("invalid historical grant identity".into());
        }
        let fence = i64::try_from(fence).map_err(|e| e.to_string())?;
        self.client.query_consistent_map::<GrantRow,_>("SELECT resource,fence,job_id,job_incarnation,job_revision,owner_node_id,owner_incarnation,initial_revision,initial_expiry_ms,kind,source_binding_json,issued_at_ms FROM work_grant_receipts WHERE resource=?1 AND fence=?2",params!(resource,fence)).await.map_err(|e|e.to_string())?.into_iter().next().map(|row|Ok(HistoricalGrant{initial_token:LeaseToken{resource:row.resource,owner_node_id:row.owner_node_id,owner_incarnation:row.owner_incarnation,fence:u64::try_from(row.fence).map_err(|e|e.to_string())?,revision:u64::try_from(row.initial_revision).map_err(|e|e.to_string())?,expires_at_unix_ms:row.initial_expiry_ms},job_id:u32::try_from(row.job_id).map_err(|e|e.to_string())?,job_incarnation:row.job_incarnation,job_revision:u64::try_from(row.job_revision).map_err(|e|e.to_string())?,kind:row.kind,source_binding_json:row.source_binding_json,issued_at_unix_ms:row.issued_at_ms})).transpose()
    }
}
