//! Replicated cluster-control authority.
//!
//! Hiqlite owns the durable ordering boundary. The shared payload volume is
//! deliberately absent from this module: it stores only identities, exact
//! leases, command receipts, accepted generation references, and migration
//! fingerprints.

use crate::ClusterConfig;
use hiqlite::macros::params;
use hiqlite::{Client, Node, NodeConfig, Row};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CONTROL_SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS cluster_delivery_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL) STRICT",
    "CREATE TABLE IF NOT EXISTS cluster_deliveries(sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,data TEXT NOT NULL,revision INTEGER NOT NULL DEFAULT 1,discovery_error TEXT,public_data TEXT,accepted_at INTEGER,cleanup_only INTEGER NOT NULL DEFAULT 0,request_token TEXT) STRICT",
    "CREATE INDEX IF NOT EXISTS cluster_deliveries_request_token ON cluster_deliveries(request_token)",

    "CREATE TABLE IF NOT EXISTS cluster_meta (
        cluster_id TEXT PRIMARY KEY,
        source_fingerprint TEXT NOT NULL,
        migrated_at_ms INTEGER NOT NULL
    ) STRICT",
    "CREATE TABLE IF NOT EXISTS control_counters (
        name TEXT PRIMARY KEY,
        value INTEGER NOT NULL CHECK (value >= 0)
    ) STRICT",
    "CREATE TABLE IF NOT EXISTS provider_budget_state (
        cluster_id TEXT PRIMARY KEY,
        revision INTEGER NOT NULL CHECK (revision > 0),
        state_json TEXT NOT NULL,
        updated_at_ms INTEGER NOT NULL
    ) STRICT",
    "CREATE TABLE IF NOT EXISTS control_jobs (
        job_id INTEGER PRIMARY KEY,
        incarnation TEXT NOT NULL,
        revision INTEGER NOT NULL CHECK (revision > 0),
        intent_json TEXT NOT NULL,
        result_ref TEXT,
        result_id TEXT UNIQUE,
        deleted INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL
    ) STRICT",
    "CREATE TABLE IF NOT EXISTS request_admissions(token TEXT PRIMARY KEY,identity TEXT NOT NULL,job_id INTEGER NOT NULL,data TEXT NOT NULL CHECK(length(CAST(data AS BLOB))<=8192)) STRICT",
    "CREATE TRIGGER IF NOT EXISTS admission_identity BEFORE INSERT ON request_admissions BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM request_admissions WHERE token=NEW.token AND identity<>NEW.identity) THEN RAISE(ABORT,'admission identity conflict') END; END",
    "CREATE TRIGGER IF NOT EXISTS admission_job_binding BEFORE INSERT ON request_admissions BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM control_jobs j,json_each(j.intent_json,'$.params') p WHERE j.job_id=NEW.job_id AND json_extract(p.value,'$[0]')='monarr-transfer' AND json_extract(p.value,'$[1]')=NEW.token) THEN RAISE(ABORT,'admission job binding conflict') END; END",
    "CREATE TRIGGER IF NOT EXISTS control_job_highwater_insert AFTER INSERT ON control_jobs BEGIN
       INSERT INTO control_counters(name,value) VALUES('next_job_id',NEW.job_id) ON CONFLICT(name) DO UPDATE SET value=max(value,excluded.value);
       INSERT INTO control_counters(name,value) VALUES('next_file_id',coalesce((SELECT max(CAST(json_extract(value,'$.id') AS INTEGER)) FROM json_each(NEW.intent_json,'$.files')),0)) ON CONFLICT(name) DO UPDATE SET value=max(value,excluded.value); END",
    "CREATE TRIGGER IF NOT EXISTS control_job_highwater_update AFTER UPDATE OF intent_json ON control_jobs BEGIN
       INSERT INTO control_counters(name,value) VALUES('next_file_id',coalesce((SELECT max(CAST(json_extract(value,'$.id') AS INTEGER)) FROM json_each(NEW.intent_json,'$.files')),0)) ON CONFLICT(name) DO UPDATE SET value=max(value,excluded.value); END",
    "CREATE TRIGGER IF NOT EXISTS admission_highwater AFTER INSERT ON request_admissions BEGIN INSERT INTO control_counters(name,value) VALUES('next_job_id',NEW.job_id) ON CONFLICT(name) DO UPDATE SET value=max(value,excluded.value); END",
    "CREATE TABLE IF NOT EXISTS work_leases (
        resource TEXT PRIMARY KEY,
        cluster_id TEXT NOT NULL,
        job_id INTEGER NOT NULL,
        job_incarnation TEXT NOT NULL,
        owner_node_id TEXT NOT NULL,
        owner_incarnation TEXT NOT NULL,
        kind TEXT NOT NULL,
        scope_json TEXT NOT NULL,
        fence INTEGER NOT NULL CHECK (fence > 0),
        revision INTEGER NOT NULL CHECK (revision > 0),
        job_revision INTEGER NOT NULL CHECK (job_revision > 0),
        expires_at_ms INTEGER NOT NULL,
        terminal INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL
    ) STRICT",
    "CREATE INDEX IF NOT EXISTS work_leases_job ON work_leases(job_id)",
    "CREATE TRIGGER IF NOT EXISTS work_job_intent_validate BEFORE INSERT ON work_leases WHEN NEW.resource<>'cluster/leader' BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM control_jobs WHERE job_id=NEW.job_id AND incarnation=NEW.job_incarnation AND revision=NEW.job_revision AND deleted=0) THEN RAISE(ABORT,'stale job intent at lease claim') END; SELECT CASE WHEN json_type(NEW.scope_json,'$._authority') IS NOT NULL AND NOT EXISTS(SELECT 1 FROM work_leases WHERE resource='cluster/leader' AND terminal=0 AND owner_node_id=json_extract(NEW.scope_json,'$._authority.node') AND owner_incarnation=json_extract(NEW.scope_json,'$._authority.incarnation') AND fence=json_extract(NEW.scope_json,'$._authority.fence') AND expires_at_ms>NEW.updated_at_ms) THEN RAISE(ABORT,'stale grant issuer authority') END; END",
    "CREATE TRIGGER IF NOT EXISTS coordinator_protocol_insert BEFORE INSERT ON work_leases WHEN NEW.resource='cluster/leader' AND (json_type(NEW.scope_json,'$.lifecycleProtocol') IS NOT 'integer' OR coalesce(json_extract(NEW.scope_json,'$.lifecycleProtocol'),0)<2) BEGIN SELECT RAISE(ABORT,'coordinator requires lifecycleProtocol 2; upgrade this node before claiming authority'); END",
    "CREATE TRIGGER IF NOT EXISTS coordinator_protocol_update BEFORE UPDATE ON work_leases WHEN NEW.resource='cluster/leader' AND NEW.terminal=0 AND (json_type(NEW.scope_json,'$.lifecycleProtocol') IS NOT 'integer' OR coalesce(json_extract(NEW.scope_json,'$.lifecycleProtocol'),0)<2) BEGIN SELECT RAISE(ABORT,'coordinator requires lifecycleProtocol 2; upgrade this node before renewing authority'); END",
    "CREATE TABLE IF NOT EXISTS request_authority(id INTEGER PRIMARY KEY CHECK(id=1),owner TEXT NOT NULL,incarnation TEXT NOT NULL,fence INTEGER NOT NULL,checked_at_ms INTEGER NOT NULL) STRICT",
    "CREATE TRIGGER IF NOT EXISTS request_authority_validate BEFORE INSERT ON request_authority BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM work_leases WHERE resource='cluster/leader' AND owner_node_id=NEW.owner AND owner_incarnation=NEW.incarnation AND fence=NEW.fence AND expires_at_ms>NEW.checked_at_ms AND terminal=0) THEN RAISE(ABORT,'stale request authority') END; END",
    "CREATE TABLE IF NOT EXISTS command_receipts (
        command_id TEXT PRIMARY KEY,
        job_id INTEGER NOT NULL,
        expected_revision INTEGER NOT NULL,
        applied_revision INTEGER NOT NULL,
        command_json TEXT NOT NULL,
        applied_at_ms INTEGER NOT NULL
    ) STRICT",
    "CREATE TABLE IF NOT EXISTS publication_receipts (
        receipt_id TEXT PRIMARY KEY,
        resource TEXT NOT NULL,
        owner_node_id TEXT NOT NULL,
        owner_incarnation TEXT NOT NULL,
        fence INTEGER NOT NULL,
        lease_revision INTEGER NOT NULL,
        lease_expiry_ms INTEGER NOT NULL,
        expected_job_revision INTEGER NOT NULL,
        result_id TEXT NOT NULL UNIQUE,
        result_ref TEXT NOT NULL,
        result_job_json TEXT NOT NULL,
        accepted_at_ms INTEGER NOT NULL
    ) STRICT",
    "CREATE TRIGGER IF NOT EXISTS command_validate
     BEFORE INSERT ON command_receipts
     BEGIN
       SELECT CASE WHEN NOT EXISTS (
         SELECT 1 FROM control_jobs
          WHERE job_id = NEW.job_id AND revision = NEW.expected_revision
       ) THEN RAISE(ABORT, 'stale job revision') END;
     END",
    "CREATE TRIGGER IF NOT EXISTS command_apply
     AFTER INSERT ON command_receipts
     BEGIN
       UPDATE control_jobs SET
         revision = NEW.applied_revision,
         intent_json = json_extract(NEW.command_json, '$.next_intent'),
         deleted = json_extract(NEW.command_json, '$.deleted'),
         updated_at_ms = NEW.applied_at_ms
       WHERE job_id = NEW.job_id AND revision = NEW.expected_revision;
     END",
    "CREATE TABLE IF NOT EXISTS range_results (
        resource TEXT PRIMARY KEY,
        job_id INTEGER NOT NULL,
        scope_json TEXT NOT NULL,
        result_id TEXT NOT NULL UNIQUE,
        result_ref TEXT NOT NULL,
        accepted_at_ms INTEGER NOT NULL
    ) STRICT",
    "CREATE TABLE IF NOT EXISTS script_receipts (
        receipt_id TEXT PRIMARY KEY,
        resource TEXT NOT NULL,
        job_id INTEGER NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('started','done')),
        started_at_ms INTEGER NOT NULL,
        finished_at_ms INTEGER
    ) STRICT",
    "CREATE TRIGGER IF NOT EXISTS publication_validate
     BEFORE INSERT ON publication_receipts
     BEGIN
       SELECT CASE WHEN NOT EXISTS (
         SELECT 1 FROM work_leases
         WHERE resource = NEW.resource
           AND owner_node_id = NEW.owner_node_id
           AND owner_incarnation = NEW.owner_incarnation
           AND fence = NEW.fence
           AND revision = NEW.lease_revision
           AND expires_at_ms = NEW.lease_expiry_ms
           AND expires_at_ms > NEW.accepted_at_ms
           AND terminal = 0
       ) THEN RAISE(ABORT, 'stale lease token') END;
       SELECT CASE WHEN NOT EXISTS (
         SELECT 1 FROM control_jobs j JOIN work_leases l ON l.job_id = j.job_id
         WHERE l.resource = NEW.resource
           AND j.incarnation = l.job_incarnation
           AND j.revision = NEW.expected_job_revision
           AND j.deleted = 0
       ) THEN RAISE(ABORT, 'stale job revision') END;
     END",
    "DROP TRIGGER IF EXISTS publication_apply",
    "CREATE TRIGGER publication_apply
     AFTER INSERT ON publication_receipts
     BEGIN
       INSERT INTO range_results
         (resource,job_id,scope_json,result_id,result_ref,accepted_at_ms)
       SELECT l.resource,l.job_id,l.scope_json,NEW.result_id,NEW.result_ref,NEW.accepted_at_ms
         FROM work_leases l
        WHERE l.resource = NEW.resource AND l.kind = 'segment';
       UPDATE control_jobs SET
         result_ref = NEW.result_ref,
         result_id = NEW.result_id,
         intent_json = NEW.result_job_json,
         revision = revision + 1,
         updated_at_ms = NEW.accepted_at_ms
       WHERE job_id = (SELECT job_id FROM work_leases
                         WHERE resource = NEW.resource AND kind <> 'segment');
       UPDATE work_leases SET terminal = 1, revision = revision + 1,
         updated_at_ms = NEW.accepted_at_ms
       WHERE resource = NEW.resource;
     END",
];

#[derive(Clone)]
pub struct ControlStore {
    pub(crate) client: Client,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseToken {
    pub resource: String,
    pub owner_node_id: String,
    pub owner_incarnation: String,
    pub fence: u64,
    pub revision: u64,
    pub expires_at_unix_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurableLease {
    pub token: LeaseToken,
    pub job_id: u64,
    pub job_incarnation: String,
    pub owner_node_id: String,
    pub kind: String,
    pub scope_json: String,
    pub job_revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseClaim {
    Acquired(LeaseToken),
    Held {
        owner_node_id: String,
        fence: u64,
        expires_at_unix_ms: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MutationOutcome {
    Applied { revision: u64 },
    Duplicate { revision: u64 },
    Conflict,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScriptReceiptOutcome {
    Run,
    AlreadyDone,
    Ambiguous,
    Conflict,
}

#[derive(Clone, Debug)]
pub struct PublicationReceipt {
    pub resource: String,
    pub owner_node_id: String,
    pub owner_incarnation: String,
    pub fence: u64,
    pub lease_revision: u64,
    pub lease_expiry_ms: i64,
    pub expected_job_revision: u64,
    pub result_id: String,
    pub result_ref: String,
    pub result_job_json: String,
    pub accepted_at_ms: i64,
    pub kind: String,
}

pub(crate) fn authority_statement(
    (owner, incarnation, fence): (&str, &str, u64),
) -> Result<(String, hiqlite::Params), String> {
    Ok(("INSERT INTO request_authority(id,owner,incarnation,fence,checked_at_ms) VALUES(1,?1,?2,?3,?4) ON CONFLICT(id) DO UPDATE SET owner=excluded.owner,incarnation=excluded.incarnation,fence=excluded.fence,checked_at_ms=excluded.checked_at_ms".into(),params!(owner,incarnation,i64_value("authority fence",fence)?,unix_ms()?)))
}

impl ControlStore {
    pub async fn start(cfg: &ClusterConfig) -> Result<Self, String> {
        Self::start_with_wal_size(cfg, (crate::proto::MAX_RPC_BYTES * 4) as u32).await
    }
    pub(crate) async fn start_with_wal_size(
        cfg: &ClusterConfig,
        wal_size: u32,
    ) -> Result<Self, String> {
        std::fs::create_dir_all(&cfg.control_dir)
            .map_err(|error| format!("create control directory: {error}"))?;
        let local_raft: SocketAddr = cfg
            .control_raft_bind
            .parse()
            .map_err(|error| format!("invalid control_raft_bind: {error}"))?;
        let local_api: SocketAddr = cfg
            .control_api_bind
            .parse()
            .map_err(|error| format!("invalid control_api_bind: {error}"))?;
        let nodes = if cfg.control_peers.is_empty() {
            vec![Node {
                id: cfg.control_node_id,
                addr_raft: cfg.control_raft_bind.clone(),
                addr_api: cfg.control_api_bind.clone(),
            }]
        } else {
            cfg.control_peers
                .iter()
                .map(|peer| Node {
                    id: peer.id,
                    addr_raft: peer.raft_addr.clone(),
                    addr_api: peer.api_addr.clone(),
                })
                .collect()
        };
        let secret = format!("{:x}", Sha256::digest(cfg.secret.as_bytes()));
        let mut raft_config = NodeConfig::default_raft_config(10_000);
        raft_config.heartbeat_interval = 1_000;
        raft_config.election_timeout_min = 2_500;
        raft_config.election_timeout_max = 5_000;
        let node = NodeConfig {
            node_id: cfg.control_node_id,
            nodes,
            listen_addr_api: Cow::Owned(local_api.ip().to_string()),
            listen_addr_raft: Cow::Owned(local_raft.ip().to_string()),
            data_dir: Cow::Owned(cfg.control_dir.to_string_lossy().into_owned()),
            filename_db: Cow::Borrowed("nzbd-control.db"),
            secret_raft: format!("raft-{secret}"),
            secret_api: format!("api-{secret}"),
            health_check_delay_secs: 0,
            // One bounded 32 MiB private copy record plus its bounded public
            // projection/transaction overhead must fit a Raft entry. The WAL
            // writer cannot split an entry across segments.
            wal_size,
            raft_config,
            ..NodeConfig::default()
        };
        let client = tokio::time::timeout(Duration::from_secs(20), hiqlite::start_node(node))
            .await
            .map_err(|_| "starting replicated control node timed out".to_owned())?
            .map_err(|error| format!("start replicated control node: {error}"))?;
        tokio::time::timeout(Duration::from_secs(20), client.wait_until_healthy_db())
            .await
            .map_err(|_| "replicated control quorum did not become healthy".to_owned())?;
        client
            .txn(
                CONTROL_SCHEMA
                    .iter()
                    .chain(crate::grant_receipts::SCHEMA.iter())
                    .map(|statement| ((*statement).to_owned(), params!()))
                    .collect::<Vec<(String, hiqlite::Params)>>(),
            )
            .await
            .map_err(|error| format!("install control schema transaction: {error}"))?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("install control schema statement: {error}"))?;
        let store = Self { client };
        crate::cancellation::install_schema(&store).await?;
        crate::cluster_keep::install_schema(&store).await?;
        Ok(store)
    }

    pub async fn shutdown(&self) -> Result<(), String> {
        self.client
            .shutdown()
            .await
            .map_err(|error| format!("shutdown replicated control node: {error}"))
    }

    pub async fn is_healthy(&self) -> bool {
        // A local server/leader observation is not enough to authorize a
        // mutation: a minority can retain a stale leader view after losing
        // its peers. Force a linearizable quorum round trip so callers fail
        // closed until a majority has acknowledged the current term.
        let Ok(metrics) = self.client.metrics_db().await else {
            return false;
        };
        // A learner can forward a successful quorum read before joining the
        // fixed voter set. That is not evidence this voter can survive a peer
        // loss; wait for its own committed membership before advertising ready.
        if metrics.running_state.is_err()
            || metrics.state.is_learner()
            || !metrics
                .membership_config
                .voter_ids()
                .any(|id| id == metrics.id)
        {
            return false;
        }
        tokio::time::timeout(Duration::from_secs(2), self.client.db_quorum_watermark())
            .await
            .is_ok_and(|result| result.is_ok())
    }

    pub async fn migration_identity(&self) -> Result<Option<(String, String)>, String> {
        let rows = self
            .client
            .query_consistent_map::<MigrationRow, _>(
                "SELECT cluster_id,source_fingerprint FROM cluster_meta LIMIT 1",
                params!(),
            )
            .await
            .map_err(|error| format!("read control migration identity: {error}"))?;
        Ok(rows
            .into_iter()
            .next()
            .map(|row| (row.cluster_id, row.source_fingerprint)))
    }

    /// Import the stopped legacy queue exactly once. The caller owns making a
    /// read-only backup before this transaction begins.
    pub async fn migrate_legacy_snapshot(
        &self,
        cluster_id: &str,
        fingerprint: &str,
        snapshot: &nzbd_state::QueueSnapshotDoc,
    ) -> Result<bool, String> {
        let existing = self
            .client
            .query_consistent_map::<MigrationRow, _>(
                "SELECT cluster_id,source_fingerprint FROM cluster_meta LIMIT 1",
                params!(),
            )
            .await
            .map_err(|error| format!("read control migration: {error}"))?;
        if let Some(row) = existing.into_iter().next() {
            if row.cluster_id == cluster_id {
                if row.source_fingerprint != fingerprint {
                    tracing::debug!(
                        imported = %row.source_fingerprint,
                        current = %fingerprint,
                        "legacy source changed after its one-time control migration"
                    );
                }
                return Ok(false);
            }
            return Err(format!(
                "control store already belongs to cluster {} imported from {}; refusing {} from {}",
                row.cluster_id, row.source_fingerprint, cluster_id, fingerprint
            ));
        }

        let now = unix_ms()?;
        let mut statements: Vec<(String, hiqlite::Params)> = Vec::new();
        for job in &snapshot.jobs {
            let intent_json = serde_json::to_string(job)
                .map_err(|error| format!("encode legacy job {}: {error}", job.id.0))?;
            let incarnation = format!("legacy-{:x}", Sha256::digest(intent_json.as_bytes()));
            statements.push((
                "INSERT INTO control_jobs
                 (job_id,incarnation,revision,intent_json,updated_at_ms,deleted)
                 VALUES($1,$2,1,$3,$4,$5)"
                    .into(),
                params!(
                    i64::from(job.id.0),
                    incarnation,
                    intent_json,
                    now,
                    job.status == nzbd_types::JobStatus::Deleted
                ),
            ));
        }
        statements.push((
            "INSERT INTO control_counters(name,value) VALUES('next_job_id',$1) ON CONFLICT(name) DO UPDATE SET value=max(value,excluded.value)".into(),
            params!(i64::from(snapshot.next_job_id)),
        ));
        statements.push((
            "INSERT INTO control_counters(name,value) VALUES('next_file_id',$1) ON CONFLICT(name) DO UPDATE SET value=max(value,excluded.value)".into(),
            params!(i64::from(snapshot.next_file_id)),
        ));
        statements.push((
            "INSERT INTO cluster_meta(cluster_id,source_fingerprint,migrated_at_ms)
             VALUES($1,$2,$3)"
                .into(),
            params!(cluster_id, fingerprint, now),
        ));
        let committed = self.client.txn(statements).await;
        match committed {
            Ok(results) => {
                results
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| format!("legacy control migration statement: {error}"))?;
                Ok(true)
            }
            Err(error) => {
                // Concurrent voter startup may race the initial read. Treat
                // the winner's identical fingerprint as the same idempotent
                // migration, never as a startup failure.
                let rows = self
                    .client
                    .query_consistent_map::<MigrationRow, _>(
                        "SELECT cluster_id,source_fingerprint FROM cluster_meta LIMIT 1",
                        params!(),
                    )
                    .await
                    .map_err(|read| format!("migration raced ({error}); reread failed: {read}"))?;
                if rows
                    .into_iter()
                    .next()
                    .is_some_and(|row| row.cluster_id == cluster_id)
                {
                    Ok(false)
                } else {
                    Err(format!("commit legacy control migration: {error}"))
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn acquire(
        &self,
        resource: &str,
        cluster_id: &str,
        job_id: u64,
        job_incarnation: &str,
        owner_node_id: &str,
        owner_incarnation: &str,
        kind: &str,
        scope_json: &str,
        job_revision: u64,
        ttl: Duration,
    ) -> Result<LeaseClaim, String> {
        validate_identity(resource, owner_node_id, owner_incarnation)?;
        let now = unix_ms()?;
        let expiry = expiry(now, ttl)?;
        let sql = "INSERT INTO work_leases
            (resource, cluster_id, job_id, job_incarnation, owner_node_id,
             owner_incarnation, kind, scope_json, fence, revision, job_revision,
             expires_at_ms, terminal, updated_at_ms)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1,1,$9,$10,0,$11)
            ON CONFLICT(resource) DO UPDATE SET
              cluster_id=excluded.cluster_id, job_id=excluded.job_id,
              job_incarnation=excluded.job_incarnation,
              owner_node_id=excluded.owner_node_id,
              owner_incarnation=excluded.owner_incarnation, kind=excluded.kind,
              scope_json=excluded.scope_json, fence=work_leases.fence+1,
              revision=work_leases.revision+1, job_revision=excluded.job_revision,
              expires_at_ms=excluded.expires_at_ms, terminal=0,
              updated_at_ms=excluded.updated_at_ms
            WHERE work_leases.expires_at_ms <= $11
              AND work_leases.fence < 9223372036854775807
              AND work_leases.revision < 9223372036854775807
            RETURNING resource,owner_node_id,owner_incarnation,fence,revision,expires_at_ms";
        let rows = self
            .client
            .execute_returning_map::<_, LeaseRow>(
                sql,
                params!(
                    resource,
                    cluster_id,
                    i64_value("job id", job_id)?,
                    job_incarnation,
                    owner_node_id,
                    owner_incarnation,
                    kind,
                    scope_json,
                    i64_value("job revision", job_revision)?,
                    expiry,
                    now
                ),
            )
            .await
            .map_err(|error| format!("acquire lease: {error}"))?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("decode acquired lease: {error}"))?;
        if let Some(row) = rows.into_iter().next() {
            let token: LeaseToken = row.try_into()?;
            if resource != "cluster/leader" {
                let receipt = self.historical_grant(resource, token.fence).await?.ok_or(
                    "new lease lacks immutable grant provenance; do not execute this attempt",
                )?;
                if receipt.initial_token != token
                    || u64::from(receipt.job_id) != job_id
                    || receipt.job_incarnation != job_incarnation
                    || receipt.job_revision != job_revision
                    || receipt.kind != kind
                    || receipt.issued_at_unix_ms != now
                    || receipt.source_binding_json.as_ref().is_some_and(|value| {
                        value.len() > 8192
                            || serde_json::from_str::<serde_json::Value>(value).is_err()
                    })
                {
                    return Err("committed grant provenance differs from its claimed operation; retain and review".into());
                }
            }
            return Ok(LeaseClaim::Acquired(token));
        }
        let held = self.current_lease(resource).await?.ok_or_else(|| {
            "lease acquire changed no row and the resource disappeared".to_owned()
        })?;
        Ok(LeaseClaim::Held {
            owner_node_id: held.owner_node_id,
            fence: held.fence,
            expires_at_unix_ms: held.expires_at_unix_ms,
        })
    }

    pub async fn renew(
        &self,
        token: &LeaseToken,
        ttl: Duration,
    ) -> Result<Option<LeaseToken>, String> {
        validate_identity(
            &token.resource,
            &token.owner_node_id,
            &token.owner_incarnation,
        )?;
        let now = unix_ms()?;
        let expiry = expiry(now, ttl)?;
        let rows = self
            .client
            .execute_returning_map::<_, LeaseRow>(
                "UPDATE work_leases SET expires_at_ms=$1,revision=revision+1,updated_at_ms=$2
             WHERE resource=$3 AND owner_node_id=$4 AND owner_incarnation=$5
               AND fence=$6 AND revision=$7 AND expires_at_ms=$8
               AND terminal=0 AND expires_at_ms>$2
             RETURNING resource,owner_node_id,owner_incarnation,fence,revision,expires_at_ms",
                params!(
                    expiry,
                    now,
                    &token.resource,
                    &token.owner_node_id,
                    &token.owner_incarnation,
                    i64_value("fence", token.fence)?,
                    i64_value("lease revision", token.revision)?,
                    token.expires_at_unix_ms
                ),
            )
            .await
            .map_err(|error| format!("renew lease: {error}"))?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("decode renewed lease: {error}"))?;
        rows.into_iter().next().map(TryInto::try_into).transpose()
    }

    pub async fn release(&self, token: &LeaseToken) -> Result<bool, String> {
        let now = unix_ms()?;
        Ok(self
            .client
            .execute(
                "UPDATE work_leases SET terminal=1,expires_at_ms=$1,
             revision=revision+1,updated_at_ms=$1
             WHERE resource=$2 AND owner_node_id=$3 AND owner_incarnation=$4
               AND fence=$5 AND revision=$6 AND expires_at_ms=$7 AND terminal=0",
                params!(
                    now,
                    &token.resource,
                    &token.owner_node_id,
                    &token.owner_incarnation,
                    i64_value("fence", token.fence)?,
                    i64_value("lease revision", token.revision)?,
                    token.expires_at_unix_ms
                ),
            )
            .await
            .map_err(|error| format!("release lease: {error}"))?
            == 1)
    }

    pub async fn current_lease(&self, resource: &str) -> Result<Option<LeaseToken>, String> {
        let rows = self
            .client
            .query_consistent_map::<LeaseRow, _>(
                "SELECT resource,owner_node_id,owner_incarnation,fence,revision,expires_at_ms
             FROM work_leases WHERE resource=$1 AND terminal=0",
                params!(resource),
            )
            .await
            .map_err(|error| format!("read lease: {error}"))?;
        rows.into_iter().next().map(TryInto::try_into).transpose()
    }

    pub async fn current_lease_record(
        &self,
        resource: &str,
    ) -> Result<Option<DurableLease>, String> {
        let rows = self
            .client
            .query_consistent_map::<DurableLeaseRow, _>(
                "SELECT resource,job_id,job_incarnation,owner_node_id,owner_incarnation,
                        kind,scope_json,fence,revision,job_revision,expires_at_ms
                   FROM work_leases WHERE resource=$1 AND terminal=0",
                params!(resource),
            )
            .await
            .map_err(|error| format!("read durable lease record: {error}"))?;
        rows.into_iter().next().map(TryInto::try_into).transpose()
    }

    pub async fn seed_job(
        &self,
        job_id: u64,
        incarnation: &str,
        intent_json: &str,
    ) -> Result<(), String> {
        let now = unix_ms()?;
        self.client
            .execute(
                "INSERT INTO control_jobs
             (job_id,incarnation,revision,intent_json,updated_at_ms)
             VALUES($1,$2,1,$3,$4)
             ON CONFLICT(job_id) DO NOTHING",
                params!(i64_value("job id", job_id)?, incarnation, intent_json, now),
            )
            .await
            .map_err(|error| format!("seed control job: {error}"))?;
        Ok(())
    }

    pub async fn job_identity(&self, job_id: u64) -> Result<Option<(String, u64)>, String> {
        let rows = self
            .client
            .query_consistent_map::<JobIdentityRow, _>(
                "SELECT incarnation,revision FROM control_jobs WHERE job_id=$1 AND deleted=0",
                params!(i64_value("job id", job_id)?),
            )
            .await
            .map_err(|error| format!("read replicated job identity: {error}"))?;
        rows.into_iter()
            .next()
            .map(|row| {
                Ok((
                    row.incarnation,
                    u64::try_from(row.revision)
                        .map_err(|error| format!("invalid replicated job revision: {error}"))?,
                ))
            })
            .transpose()
    }

    pub(crate) async fn selected_publication_job(
        &self,
        job: u32,
        result: &str,
    ) -> Result<Option<(nzbd_types::Job, u64)>, String> {
        let row=self.client.query_consistent_map::<ControlJobRow,_>("SELECT intent_json,revision,incarnation FROM control_jobs WHERE job_id=?1 AND result_id=?2 AND deleted=0",params!(i64::from(job),result)).await.map_err(|error|error.to_string())?.into_iter().next();
        row.map(|row| {
            Ok((
                serde_json::from_str(&row.intent_json).map_err(|error| error.to_string())?,
                u64::try_from(row.revision).map_err(|error| error.to_string())?,
            ))
        })
        .transpose()
    }

    pub async fn load_jobs(&self) -> Result<Vec<(nzbd_types::Job, u64, String)>, String> {
        let rows = self
            .client
            .query_consistent_map::<ControlJobRow, _>(
                "SELECT intent_json,revision,incarnation FROM control_jobs WHERE deleted=0 ORDER BY job_id",
                params!(),
            )
            .await
            .map_err(|error| format!("load replicated jobs: {error}"))?;
        rows.into_iter()
            .map(|row| {
                let job = serde_json::from_str(&row.intent_json)
                    .map_err(|error| format!("decode replicated job: {error}"))?;
                let revision = u64::try_from(row.revision)
                    .map_err(|error| format!("invalid replicated job revision: {error}"))?;
                Ok((job, revision, row.incarnation))
            })
            .collect()
    }

    /// Reconcile the queue owner's current intent into the replicated row.
    /// The receipt id is content-addressed, so repeated event delivery or a
    /// crash after commit reuses the same logical command.
    pub async fn reconcile_job(
        &self,
        job_id: u64,
        job: Option<&nzbd_types::Job>,
    ) -> Result<Option<(String, u64, bool)>, String> {
        let desired = job
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| format!("encode replicated job intent: {error}"))?;
        for _ in 0..4 {
            let rows = self
                .client
                .query_consistent_map::<ControlJobStateRow, _>(
                    "SELECT incarnation,revision,intent_json,deleted FROM control_jobs WHERE job_id=$1",
                    params!(i64_value("job id", job_id)?),
                )
                .await
                .map_err(|error| format!("read replicated job for reconciliation: {error}"))?;
            let Some(row) = rows.into_iter().next() else {
                let Some(intent) = desired.as_deref() else {
                    return Ok(None);
                };
                let incarnation = format!("job-{:x}", Sha256::digest(intent.as_bytes()));
                self.seed_job(job_id, &incarnation, intent).await?;
                continue;
            };
            let deleted = desired.is_none();
            if row.deleted == deleted
                && desired
                    .as_deref()
                    .is_none_or(|intent| intent == row.intent_json)
            {
                return Ok(Some((
                    row.incarnation,
                    u64::try_from(row.revision).map_err(|error| error.to_string())?,
                    row.deleted,
                )));
            }
            let expected = u64::try_from(row.revision).map_err(|error| error.to_string())?;
            let next = desired.as_deref().unwrap_or(&row.intent_json);
            let command_json = serde_json::json!({
                "op": if deleted { "delete" } else { "replace_intent" },
                "job_id": job_id,
            })
            .to_string();
            let receipt_material = format!("{job_id}:{expected}:{deleted}:{next}");
            let command_id = format!(
                "reconcile:{:x}",
                Sha256::digest(receipt_material.as_bytes())
            );
            match self
                .apply_command(&command_id, job_id, expected, &command_json, next, deleted)
                .await?
            {
                MutationOutcome::Applied { revision } | MutationOutcome::Duplicate { revision } => {
                    return Ok(Some((row.incarnation, revision, deleted)));
                }
                MutationOutcome::Conflict => continue,
            }
        }
        Err("replicated job reconciliation remained conflicted".into())
    }

    /// Atomically commit every queue row changed by one externally visible
    /// API request. Existing rows use the same revision-checked receipt
    /// trigger as other control mutations; new rows are inserted in the same
    /// replicated transaction. Any stale row aborts the entire request.
    pub async fn commit_job_delta(
        &self,
        changes: &[(u64, Option<nzbd_types::Job>)],
    ) -> Result<(), String> {
        let expected = self
            .job_revision_fences_for(&changes.iter().map(|(id, _)| *id).collect::<Vec<_>>())
            .await?;
        self.commit_job_delta_guarded(changes, &expected, &[], None)
            .await
    }

    pub(crate) async fn commit_job_delta_guarded(
        &self,
        changes: &[(u64, Option<nzbd_types::Job>)],
        expected_rows: &std::collections::HashMap<u64, u64>,
        admissions: &[nzbd_state::UrlAdmissionReceipt],
        authority: Option<(&str, &str, u64)>,
    ) -> Result<(), String> {
        self.commit_job_delta_guarded_with_removals(
            changes,
            expected_rows,
            admissions,
            authority,
            &Default::default(),
        )
        .await
    }
    pub(crate) async fn commit_job_delta_guarded_with_removals(
        &self,
        changes: &[(u64, Option<nzbd_types::Job>)],
        expected_rows: &std::collections::HashMap<u64, u64>,
        admissions: &[nzbd_state::UrlAdmissionReceipt],
        authority: Option<(&str, &str, u64)>,
        removals: &std::collections::BTreeMap<u32, crate::cancellation::RemovalMode>,
    ) -> Result<(), String> {
        let now = unix_ms()?;
        let mut statements: Vec<(String, hiqlite::Params)> = Vec::new();
        if let Some(authority) = authority {
            statements.push(authority_statement(authority)?);
        }
        for (job_id, desired_job) in changes {
            let rows = self
                .client
                .query_consistent_map::<ControlJobStateRow, _>(
                    "SELECT incarnation,revision,intent_json,deleted FROM control_jobs WHERE job_id=$1",
                    params!(i64_value("job id", *job_id)?),
                )
                .await
                .map_err(|error| format!("read replicated job before request commit: {error}"))?;
            let desired = desired_job
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| format!("encode replicated request row: {error}"))?;
            let row = rows.into_iter().next();
            let actual = row.as_ref().map(|row| row.revision as u64);
            if actual != expected_rows.get(job_id).copied() {
                return Err(format!(
                    "job {job_id} changed during request; retry against current authority"
                ));
            }
            match row {
                None => {
                    let intent = desired.as_deref().ok_or_else(|| {
                        format!("request attempted to delete unknown job {job_id}")
                    })?;
                    let incarnation = format!("job-{:x}", Sha256::digest(intent.as_bytes()));
                    statements.push((
                        "INSERT INTO control_jobs
                         (job_id,incarnation,revision,intent_json,updated_at_ms)
                         VALUES($1,$2,1,$3,$4)"
                            .into(),
                        params!(i64_value("job id", *job_id)?, incarnation, intent, now),
                    ));
                }
                Some(row) => {
                    let deleted = desired.is_none();
                    let next = desired.as_deref().unwrap_or(&row.intent_json);
                    if row.deleted == deleted && (deleted || next == row.intent_json) {
                        continue;
                    }
                    let expected = u64::try_from(row.revision)
                        .map_err(|error| format!("invalid job revision: {error}"))?;
                    let next_revision = expected
                        .checked_add(1)
                        .ok_or_else(|| "job revision exhausted".to_owned())?;
                    let command_id = format!(
                        "request:{:x}",
                        Sha256::digest(format!("{job_id}:{expected}:{deleted}:{next}").as_bytes())
                    );
                    let envelope = serde_json::json!({
                        "command": {"op": if deleted { "delete" } else { "replace_intent" }, "job_id": job_id},
                        "next_intent": next,
                        "deleted": deleted,
                    })
                    .to_string();
                    statements.push((
                        "INSERT INTO command_receipts
                         (command_id,job_id,expected_revision,applied_revision,command_json,applied_at_ms)
                         VALUES($1,$2,$3,$4,$5,$6)"
                            .into(),
                        params!(
                            command_id,
                            i64_value("job id", *job_id)?,
                            i64_value("expected revision", expected)?,
                            i64_value("applied revision", next_revision)?,
                            envelope,
                            now
                        ),
                    ));
                    if deleted {
                        let job = u32::try_from(*job_id).map_err(|e| e.to_string())?;
                        statements.extend(crate::cancellation::tombstone_statements(
                            job,
                            &row.incarnation,
                            expected,
                            next_revision,
                            removals
                                .get(&job)
                                .copied()
                                .unwrap_or(crate::cancellation::RemovalMode::KeepFiles),
                            now,
                        ));
                    }
                }
            }
        }
        statements.extend(crate::admissions::admission_statements(admissions)?);
        if statements.is_empty() {
            return Ok(());
        }
        crate::delivery::bounded_transaction(&statements)?;
        // Per-job reads and bounded encoding can be slow. Refresh only this
        // deterministic proposal timestamp immediately before submission.
        if let Some(authority) = authority {
            statements[0] = authority_statement(authority)?;
        }
        self.client
            .txn(statements)
            .await
            .map_err(|error| format!("commit replicated request delta: {error}"))?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("replicated request statement: {error}"))?;
        Ok(())
    }

    /// Commit the sole asynchronous queue mutation: resolving an already
    /// durable URL-fetch placeholder. The prior control row must still be
    /// `Fetching`, so this cannot overwrite an unrelated operator command.
    pub async fn commit_url_resolution(&self, job: &nzbd_types::Job) -> Result<(), String> {
        let rows = self
            .client
            .query_consistent_map::<ControlJobStateRow, _>(
                "SELECT incarnation,revision,intent_json,deleted FROM control_jobs WHERE job_id=$1",
                params!(i64_value("job id", job.id.0.into())?),
            )
            .await
            .map_err(|error| format!("read URL placeholder: {error}"))?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| "URL placeholder is not durable".to_owned())?;
        let prior: nzbd_types::Job = serde_json::from_str(&row.intent_json)
            .map_err(|error| format!("decode URL placeholder: {error}"))?;
        if row.deleted
            || prior.status != nzbd_types::JobStatus::Fetching
            || job.status == nzbd_types::JobStatus::Fetching
            || prior.id != job.id
            || prior.params.iter().find(|(key, _)| key == "*URL")
                != job.params.iter().find(|(key, _)| key == "*URL")
        {
            return Err("URL resolution no longer matches its durable placeholder".into());
        }
        let expected = u64::try_from(row.revision)
            .map_err(|error| format!("invalid URL placeholder revision: {error}"))?;
        let next = serde_json::to_string(job)
            .map_err(|error| format!("encode URL resolution: {error}"))?;
        let command_id = format!(
            "url-resolution:{:x}",
            Sha256::digest(format!("{}:{expected}:{next}", job.id.0).as_bytes())
        );
        match self
            .apply_command(
                &command_id,
                job.id.0.into(),
                expected,
                r#"{"op":"url_resolution"}"#,
                &next,
                false,
            )
            .await?
        {
            MutationOutcome::Applied { .. } | MutationOutcome::Duplicate { .. } => Ok(()),
            MutationOutcome::Conflict => Err("URL resolution lost a revision race".into()),
        }
    }

    /// Drain the durable URL outbox against current replicated intent. Events
    /// are wake hints only; the immutable resolution ID is the command key.
    /// True means durable acceptance/replay or a definite durable cancellation.
    #[cfg(test)]
    pub(crate) async fn commit_url_outbox(
        &self,
        record: &nzbd_state::artifacts::UrlResolutionRecord,
    ) -> Result<bool, String> {
        self.commit_url_outbox_with_source(record, None).await
    }
    pub(crate) async fn commit_url_outbox_with_source(
        &self,
        record: &nzbd_state::artifacts::UrlResolutionRecord,
        source: Option<&crate::source_facts::OwnedSourceFact>,
    ) -> Result<bool, String> {
        use nzbd_state::artifacts::url_source_identity;
        if record.resolution_id != record.source_identity
            || url_source_identity(&record.origin).map_err(|error| error.to_string())?
                != record.source_identity
            || record.origin.id != record.resolved.id
        {
            return Err("URL outbox identity is invalid; retain record for review".into());
        }
        let command_id = format!("url-resolution-outbox:{}", record.resolution_id);
        if self.command_receipt(&command_id).await?.is_some() {
            return Ok(true);
        }
        let row = self
            .client
            .query_consistent_map::<ControlJobStateRow, _>(
                "SELECT incarnation,revision,intent_json,deleted FROM control_jobs WHERE job_id=?1",
                params!(i64::from(record.origin.id.0)),
            )
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .next();
        let Some(row) = row else { return Ok(false) }; // Admission may still be committing.
        if row.deleted {
            return Ok(true);
        }
        let prior: nzbd_types::Job =
            serde_json::from_str(&row.intent_json).map_err(|error| error.to_string())?;
        if url_source_identity(&prior).ok().as_deref() != Some(record.source_identity.as_str()) {
            return Ok(true); // The durable row names another request/incarnation.
        }
        if !matches!(
            prior.status,
            nzbd_types::JobStatus::Fetching | nzbd_types::JobStatus::Paused
        ) || !prior.files.is_empty()
        {
            // A previous authority already resolved this exact admission (or
            // durable user intent superseded it). Never overwrite later work.
            return Ok(true);
        }
        let mut next = prior.clone();
        if prior.name == record.origin.name
            && prior.name_provisional == record.origin.name_provisional
        {
            next.name = record.resolved.name.clone();
            next.name_provisional = record.resolved.name_provisional;
        }
        if prior.category.is_none() && record.origin.category.is_none() {
            next.category = record.resolved.category.clone();
        }
        if prior.dir_name == record.origin.dir_name {
            next.dir_name = record.resolved.dir_name.clone();
        }
        next.files = record.resolved.files.clone();
        next.totals = record.resolved.totals;
        next.status = if record.resolved.status == nzbd_types::JobStatus::Failed {
            nzbd_types::JobStatus::Failed
        } else if prior.status == nzbd_types::JobStatus::Paused
            || prior.params.iter().any(|(key, _)| key == "*AddPaused")
            || prior
                .control()
                .is_some_and(|control| control.manual_pause || control.lifecycle == "held")
        {
            nzbd_types::JobStatus::Paused
        } else {
            nzbd_types::JobStatus::Queued
        };
        next.params.retain(|(key, _)| key != "*AddPaused");
        for (key, value) in record
            .resolved
            .params
            .iter()
            .filter(|(key, _)| matches!(key.as_str(), "Failure:Stage" | "Failure:Reason"))
        {
            next.params.retain(|(existing, _)| existing != key);
            next.params.push((key.clone(), value.clone()));
        }
        if let Some(source) = source {
            if source.dirname == nzbd_engine::queue::job_dir_name(&next)
                && !next
                    .params
                    .iter()
                    .any(|(key, _)| key == crate::delivery::AUTHORITY_COPY_PARAM)
            {
                next.params.push((
                    crate::delivery::AUTHORITY_COPY_PARAM.into(),
                    source.value.clone(),
                ));
            }
        }
        let next = serde_json::to_string(&next).map_err(|error| error.to_string())?;
        if next.len() > crate::proto::MAX_DESCRIPTOR_BYTES {
            return Err("resolved URL descriptor exceeds 16 MiB; retain source for review".into());
        }
        let expected = u64::try_from(row.revision).map_err(|error| error.to_string())?;
        match self.apply_command(&command_id,u64::from(prior.id.0),expected,&serde_json::json!({"op":"url_resolution_outbox","resolutionId":record.resolution_id,"sourceIdentity":record.source_identity}).to_string(),&next,false).await? {
            MutationOutcome::Applied{..}|MutationOutcome::Duplicate{..}=>Ok(true),
            MutationOutcome::Conflict=>Ok(false),
        }
    }

    pub async fn apply_command(
        &self,
        command_id: &str,
        job_id: u64,
        expected_revision: u64,
        command_json: &str,
        next_intent_json: &str,
        deleted: bool,
    ) -> Result<MutationOutcome, String> {
        if let Some(revision) = self.command_receipt(command_id).await? {
            return Ok(MutationOutcome::Duplicate { revision });
        }
        let now = unix_ms()?;
        let next_revision = expected_revision
            .checked_add(1)
            .ok_or_else(|| "job revision exhausted".to_owned())?;
        let envelope = serde_json::to_string(&serde_json::json!({
            "command": serde_json::from_str::<serde_json::Value>(command_json)
                .unwrap_or_else(|_| serde_json::Value::String(command_json.to_owned())),
            "next_intent": next_intent_json,
            "deleted": deleted,
        }))
        .map_err(|error| format!("encode command receipt: {error}"))?;
        let inserted = self
            .client
            .execute(
                "INSERT INTO command_receipts
             (command_id,job_id,expected_revision,applied_revision,command_json,applied_at_ms)
             VALUES($1,$2,$3,$4,$5,$6)",
                params!(
                    command_id,
                    i64_value("job id", job_id)?,
                    i64_value("expected revision", expected_revision)?,
                    i64_value("applied revision", next_revision)?,
                    envelope,
                    now
                ),
            )
            .await;
        match inserted {
            Ok(1) => Ok(MutationOutcome::Applied {
                revision: next_revision,
            }),
            Ok(_) => Ok(MutationOutcome::Conflict),
            Err(error) if error.to_string().contains("stale") => Ok(MutationOutcome::Conflict),
            Err(error) => Err(format!("apply command transaction: {error}")),
        }
    }

    pub async fn publish_result(
        &self,
        receipt_id: &str,
        token: &LeaseToken,
        expected_job_revision: u64,
        result_id: &str,
        result_ref: &str,
        result_job_json: &str,
    ) -> Result<MutationOutcome, String> {
        if let Some(revision) = self.publication_receipt(receipt_id).await? {
            return Ok(MutationOutcome::Duplicate { revision });
        }
        let now = unix_ms()?;
        let inserted = self
            .client
            .execute(
                "INSERT INTO publication_receipts
             (receipt_id,resource,owner_node_id,owner_incarnation,fence,
              lease_revision,lease_expiry_ms,expected_job_revision,result_id,
              result_ref,result_job_json,accepted_at_ms)
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
                params!(
                    receipt_id,
                    &token.resource,
                    &token.owner_node_id,
                    &token.owner_incarnation,
                    i64_value("fence", token.fence)?,
                    i64_value("lease revision", token.revision)?,
                    token.expires_at_unix_ms,
                    i64_value("expected job revision", expected_job_revision)?,
                    result_id,
                    result_ref,
                    result_job_json,
                    now
                ),
            )
            .await;
        match inserted {
            Ok(1) => Ok(MutationOutcome::Applied {
                revision: expected_job_revision.saturating_add(1),
            }),
            Ok(_) => Ok(MutationOutcome::Conflict),
            Err(error) if error.to_string().contains("stale") => Ok(MutationOutcome::Conflict),
            Err(error) => Err(format!("publish result: {error}")),
        }
    }

    /// Bulk verification may span heartbeat renewals. The short receipt
    /// transaction selects the latest exact same operation token, while the
    /// existing trigger still fences job intent, owner/incarnation and expiry.
    pub async fn publish_result_renewable(
        &self,
        receipt_id: &str,
        token: &LeaseToken,
        expected_job_revision: u64,
        result_id: &str,
        result_ref: &str,
        result_job_json: &str,
    ) -> Result<MutationOutcome, String> {
        if let Some(receipt) = self.publication_record(receipt_id).await? {
            return Ok(
                if renewable_receipt_matches(
                    &receipt,
                    token,
                    expected_job_revision,
                    result_id,
                    result_ref,
                    result_job_json,
                ) {
                    MutationOutcome::Duplicate {
                        revision: expected_job_revision.saturating_add(1),
                    }
                } else {
                    MutationOutcome::Conflict
                },
            );
        }
        let now = unix_ms()?;
        let sql="INSERT INTO publication_receipts(receipt_id,resource,owner_node_id,owner_incarnation,fence,lease_revision,lease_expiry_ms,expected_job_revision,result_id,result_ref,result_job_json,accepted_at_ms)
            SELECT ?1,l.resource,l.owner_node_id,l.owner_incarnation,l.fence,l.revision,l.expires_at_ms,?2,?3,?4,?5,?6 FROM work_leases l
            WHERE l.resource=?7 AND l.owner_node_id=?8 AND l.owner_incarnation=?9 AND l.fence=?10 AND l.revision>=?11 AND l.expires_at_ms>=?12 AND l.expires_at_ms>?6 AND l.terminal=0 AND l.job_revision=?2";
        let parameters = params!(
            receipt_id,
            i64_value("expected job revision", expected_job_revision)?,
            result_id,
            result_ref,
            result_job_json,
            now,
            &token.resource,
            &token.owner_node_id,
            &token.owner_incarnation,
            i64_value("fence", token.fence)?,
            i64_value("lease revision", token.revision)?,
            token.expires_at_unix_ms
        );
        // Binary Raft encoding has no JSON string escaping; this conservative
        // bound leaves at least half the 128 MiB segment for framing.
        if serde_json::to_vec(&(sql, &parameters))
            .map_err(|error| error.to_string())?
            .len()
            > 64 * 1024 * 1024
        {
            return Err("completion receipt exceeds 64 MiB encoded transaction limit; retain source and reduce metadata".into());
        }
        match self.client.execute(sql, parameters).await {
            Ok(1) => Ok(MutationOutcome::Applied {
                revision: expected_job_revision.saturating_add(1),
            }),
            Ok(_) => Ok(MutationOutcome::Conflict),
            Err(error) => {
                if let Some(receipt) = self.publication_record(receipt_id).await? {
                    return Ok(
                        if renewable_receipt_matches(
                            &receipt,
                            token,
                            expected_job_revision,
                            result_id,
                            result_ref,
                            result_job_json,
                        ) {
                            MutationOutcome::Duplicate {
                                revision: expected_job_revision.saturating_add(1),
                            }
                        } else {
                            MutationOutcome::Conflict
                        },
                    );
                }
                if error.to_string().contains("stale") {
                    Ok(MutationOutcome::Conflict)
                } else {
                    Err(format!("publish renewed result: {error}"))
                }
            }
        }
    }

    pub async fn accepted_ranges(
        &self,
        job_id: u64,
    ) -> Result<Vec<(String, String, String)>, String> {
        let rows = self
            .client
            .query_consistent_map::<RangeResultRow, _>(
                "SELECT resource,scope_json,result_ref FROM range_results WHERE job_id=$1 ORDER BY resource",
                params!(i64_value("job id", job_id)?),
            )
            .await
            .map_err(|error| format!("read accepted ranges: {error}"))?;
        Ok(rows
            .into_iter()
            .map(|row| (row.resource, row.scope_json, row.result_ref))
            .collect())
    }

    pub async fn begin_script(
        &self,
        receipt_id: &str,
        token: &LeaseToken,
    ) -> Result<ScriptReceiptOutcome, String> {
        let now = unix_ms()?;
        let inserted = self
            .client
            .execute(
                "INSERT INTO script_receipts(receipt_id,resource,job_id,state,started_at_ms)
                 SELECT $1,resource,job_id,'started',$2 FROM work_leases
                  WHERE resource=$3 AND owner_node_id=$4 AND owner_incarnation=$5
                    AND fence=$6 AND revision=$7 AND expires_at_ms=$8
                    AND expires_at_ms>$2 AND terminal=0
                 ON CONFLICT(receipt_id) DO NOTHING",
                params!(
                    receipt_id,
                    now,
                    &token.resource,
                    &token.owner_node_id,
                    &token.owner_incarnation,
                    i64_value("fence", token.fence)?,
                    i64_value("lease revision", token.revision)?,
                    token.expires_at_unix_ms
                ),
            )
            .await
            .map_err(|error| format!("begin script receipt: {error}"))?;
        if inserted == 1 {
            return Ok(ScriptReceiptOutcome::Run);
        }
        Ok(match self.script_state(receipt_id).await? {
            Some(state) if state == "done" => ScriptReceiptOutcome::AlreadyDone,
            Some(_) => ScriptReceiptOutcome::Ambiguous,
            None => ScriptReceiptOutcome::Conflict,
        })
    }

    pub async fn finish_script(
        &self,
        receipt_id: &str,
        token: &LeaseToken,
    ) -> Result<ScriptReceiptOutcome, String> {
        let now = unix_ms()?;
        let updated = self
            .client
            .execute(
                "UPDATE script_receipts SET state='done',finished_at_ms=$1
                  WHERE receipt_id=$2 AND resource=$3 AND state='started'
                    AND EXISTS (SELECT 1 FROM work_leases
                      WHERE resource=$3 AND owner_node_id=$4 AND owner_incarnation=$5
                        AND fence=$6 AND revision=$7 AND expires_at_ms=$8
                        AND expires_at_ms>$1 AND terminal=0)",
                params!(
                    now,
                    receipt_id,
                    &token.resource,
                    &token.owner_node_id,
                    &token.owner_incarnation,
                    i64_value("fence", token.fence)?,
                    i64_value("lease revision", token.revision)?,
                    token.expires_at_unix_ms
                ),
            )
            .await
            .map_err(|error| format!("finish script receipt: {error}"))?;
        if updated == 1 || self.script_state(receipt_id).await?.as_deref() == Some("done") {
            Ok(ScriptReceiptOutcome::AlreadyDone)
        } else {
            Ok(ScriptReceiptOutcome::Conflict)
        }
    }

    async fn script_state(&self, receipt_id: &str) -> Result<Option<String>, String> {
        let rows = self
            .client
            .query_consistent_map::<ScriptStateRow, _>(
                "SELECT state FROM script_receipts WHERE receipt_id=$1",
                params!(receipt_id),
            )
            .await
            .map_err(|error| format!("read script receipt: {error}"))?;
        Ok(rows.into_iter().next().map(|row| row.state))
    }

    pub async fn save_budget_state(
        &self,
        cluster_id: &str,
        revision: u64,
        state_json: &str,
    ) -> Result<(), String> {
        self.client
            .execute(
                "INSERT INTO provider_budget_state(cluster_id,revision,state_json,updated_at_ms)
                 VALUES($1,$2,$3,$4)
                 ON CONFLICT(cluster_id) DO UPDATE SET
                   revision=excluded.revision,state_json=excluded.state_json,
                   updated_at_ms=excluded.updated_at_ms
                 WHERE provider_budget_state.revision < excluded.revision",
                params!(
                    cluster_id,
                    i64_value("budget revision", revision)?,
                    state_json,
                    unix_ms()?
                ),
            )
            .await
            .map_err(|error| format!("persist provider budget state: {error}"))?;
        Ok(())
    }

    pub async fn load_budget_state(&self, cluster_id: &str) -> Result<Option<String>, String> {
        let rows = self
            .client
            .query_consistent_map::<BudgetStateRow, _>(
                "SELECT state_json FROM provider_budget_state WHERE cluster_id=$1",
                params!(cluster_id),
            )
            .await
            .map_err(|error| format!("load provider budget state: {error}"))?;
        Ok(rows.into_iter().next().map(|row| row.state_json))
    }

    async fn command_receipt(&self, command_id: &str) -> Result<Option<u64>, String> {
        self.scalar_revision(
            "SELECT applied_revision AS revision FROM command_receipts WHERE command_id=$1",
            command_id,
        )
        .await
    }

    async fn publication_receipt(&self, receipt_id: &str) -> Result<Option<u64>, String> {
        self.scalar_revision(
            "SELECT expected_job_revision+1 AS revision FROM publication_receipts WHERE receipt_id=$1",
            receipt_id,
        ).await
    }

    pub async fn publication_accepted_at(&self, receipt_id: &str) -> Result<Option<i64>, String> {
        let rows = self
            .client
            .query_consistent_map::<AcceptedAtRow, _>(
                "SELECT accepted_at_ms FROM publication_receipts WHERE receipt_id=$1",
                params!(receipt_id),
            )
            .await
            .map_err(|error| format!("read publication timestamp: {error}"))?;
        Ok(rows.into_iter().next().map(|row| row.accepted_at_ms))
    }

    pub async fn publication_record(
        &self,
        receipt_id: &str,
    ) -> Result<Option<PublicationReceipt>, String> {
        let rows = self
            .client
            .query_consistent_map::<PublicationReceiptRow, _>(
                "SELECT p.resource,p.owner_node_id,p.owner_incarnation,p.fence,
                        p.lease_revision,p.lease_expiry_ms,p.expected_job_revision,
                        p.result_id,p.result_ref,p.result_job_json,p.accepted_at_ms,l.kind
                   FROM publication_receipts p
                   JOIN work_leases l ON l.resource=p.resource
                  WHERE p.receipt_id=$1",
                params!(receipt_id),
            )
            .await
            .map_err(|error| format!("read publication receipt: {error}"))?;
        rows.into_iter().next().map(TryInto::try_into).transpose()
    }

    /// Metadata-first bounded receipt scan. Supplying a job avoids decoding
    /// unrelated historical descriptors while assembling that job's copies.
    pub(crate) async fn publication_results_page(
        &self,
        job: Option<u32>,
        after: i64,
        limit: u32,
    ) -> Result<
        (
            Vec<(String, String, nzbd_types::Job, u64, i64, String)>,
            i64,
        ),
        String,
    > {
        let pointers=self.client.query_consistent_map::<PublicationPointer,_>("SELECT p.rowid AS sequence,length(CAST(p.result_job_json AS BLOB)) AS bytes FROM publication_receipts p JOIN work_leases l ON l.resource=p.resource WHERE (?1 IS NULL OR l.job_id=?1) AND p.rowid>?2 AND l.kind<>'segment' ORDER BY p.rowid LIMIT ?3",params!(job.map(i64::from),after,i64::from(limit.clamp(1,16)))).await.map_err(|error|error.to_string())?;
        let mut values = Vec::new();
        let mut cursor = after;
        let mut bytes = 0;
        for pointer in pointers {
            if pointer.bytes > crate::proto::MAX_DESCRIPTOR_BYTES as i64 {
                tracing::warn!(sequence=pointer.sequence,"historical cluster completion exceeds descriptor limit; source retained for review");
                cursor = pointer.sequence;
                continue;
            }
            if !values.is_empty() && bytes + pointer.bytes > crate::proto::MAX_RPC_BYTES as i64 {
                break;
            }
            let row=self.client.query_consistent_map::<PublishedResultRow,_>("SELECT p.result_id,p.result_ref,p.result_job_json,p.fence,p.accepted_at_ms,l.kind FROM publication_receipts p JOIN work_leases l ON l.resource=p.resource WHERE p.rowid=?1",params!(pointer.sequence)).await.map_err(|error|error.to_string())?.into_iter().next();
            cursor = pointer.sequence;
            if let Some(row) = row {
                match serde_json::from_str::<nzbd_types::Job>(&row.result_job_json) {
                    Ok(job) => {
                        bytes += pointer.bytes;
                        values.push((
                            row.result_id,
                            row.result_ref,
                            job,
                            u64::try_from(row.fence).map_err(|error| error.to_string())?,
                            row.accepted_at_ms,
                            row.kind,
                        ));
                    }
                    Err(error) => {
                        tracing::warn!(sequence=pointer.sequence,%error,"historical cluster descriptor is invalid; source retained for review")
                    }
                }
            }
        }
        Ok((values, cursor))
    }

    async fn scalar_revision(&self, sql: &'static str, id: &str) -> Result<Option<u64>, String> {
        let rows = self
            .client
            .query_consistent_map::<RevisionRow, _>(sql, params!(id))
            .await
            .map_err(|error| format!("read durable receipt: {error}"))?;
        rows.into_iter()
            .next()
            .map(|row| {
                u64::try_from(row.revision)
                    .map_err(|error| format!("invalid durable revision: {error}"))
            })
            .transpose()
    }
}

struct LeaseRow {
    resource: String,
    owner_node_id: String,
    owner_incarnation: String,
    fence: i64,
    revision: i64,
    expires_at_unix_ms: i64,
}

struct DurableLeaseRow {
    resource: String,
    job_id: i64,
    job_incarnation: String,
    owner_node_id: String,
    owner_incarnation: String,
    kind: String,
    scope_json: String,
    fence: i64,
    revision: i64,
    job_revision: i64,
    expires_at_ms: i64,
}

impl From<&mut Row<'_>> for DurableLeaseRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            resource: row.get("resource"),
            job_id: row.get("job_id"),
            job_incarnation: row.get("job_incarnation"),
            owner_node_id: row.get("owner_node_id"),
            owner_incarnation: row.get("owner_incarnation"),
            kind: row.get("kind"),
            scope_json: row.get("scope_json"),
            fence: row.get("fence"),
            revision: row.get("revision"),
            job_revision: row.get("job_revision"),
            expires_at_ms: row.get("expires_at_ms"),
        }
    }
}

impl TryFrom<DurableLeaseRow> for DurableLease {
    type Error = String;
    fn try_from(row: DurableLeaseRow) -> Result<Self, Self::Error> {
        Ok(Self {
            token: LeaseToken {
                resource: row.resource,
                owner_node_id: row.owner_node_id.clone(),
                owner_incarnation: row.owner_incarnation,
                fence: u64::try_from(row.fence).map_err(|error| error.to_string())?,
                revision: u64::try_from(row.revision).map_err(|error| error.to_string())?,
                expires_at_unix_ms: row.expires_at_ms,
            },
            job_id: u64::try_from(row.job_id).map_err(|error| error.to_string())?,
            job_incarnation: row.job_incarnation,
            owner_node_id: row.owner_node_id,
            kind: row.kind,
            scope_json: row.scope_json,
            job_revision: u64::try_from(row.job_revision).map_err(|error| error.to_string())?,
        })
    }
}

impl From<&mut Row<'_>> for LeaseRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            resource: row.get("resource"),
            owner_node_id: row.get("owner_node_id"),
            owner_incarnation: row.get("owner_incarnation"),
            fence: row.get("fence"),
            revision: row.get("revision"),
            expires_at_unix_ms: row.get("expires_at_ms"),
        }
    }
}

impl TryFrom<LeaseRow> for LeaseToken {
    type Error = String;
    fn try_from(row: LeaseRow) -> Result<Self, Self::Error> {
        Ok(Self {
            resource: row.resource,
            owner_node_id: row.owner_node_id,
            owner_incarnation: row.owner_incarnation,
            fence: u64::try_from(row.fence).map_err(|error| error.to_string())?,
            revision: u64::try_from(row.revision).map_err(|error| error.to_string())?,
            expires_at_unix_ms: row.expires_at_unix_ms,
        })
    }
}

struct RevisionRow {
    revision: i64,
}
struct AcceptedAtRow {
    accepted_at_ms: i64,
}
struct PublicationPointer {
    sequence: i64,
    bytes: i64,
}
impl From<&mut Row<'_>> for PublicationPointer {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            sequence: row.get("sequence"),
            bytes: row.get("bytes"),
        }
    }
}
struct PublishedResultRow {
    result_id: String,
    result_ref: String,
    result_job_json: String,
    fence: i64,
    accepted_at_ms: i64,
    kind: String,
}
struct PublicationReceiptRow {
    resource: String,
    owner_node_id: String,
    owner_incarnation: String,
    fence: i64,
    lease_revision: i64,
    lease_expiry_ms: i64,
    expected_job_revision: i64,
    result_id: String,
    result_ref: String,
    result_job_json: String,
    accepted_at_ms: i64,
    kind: String,
}
impl From<&mut Row<'_>> for PublicationReceiptRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            resource: row.get("resource"),
            owner_node_id: row.get("owner_node_id"),
            owner_incarnation: row.get("owner_incarnation"),
            fence: row.get("fence"),
            lease_revision: row.get("lease_revision"),
            lease_expiry_ms: row.get("lease_expiry_ms"),
            expected_job_revision: row.get("expected_job_revision"),
            result_id: row.get("result_id"),
            result_ref: row.get("result_ref"),
            result_job_json: row.get("result_job_json"),
            accepted_at_ms: row.get("accepted_at_ms"),
            kind: row.get("kind"),
        }
    }
}
impl TryFrom<PublicationReceiptRow> for PublicationReceipt {
    type Error = String;
    fn try_from(row: PublicationReceiptRow) -> Result<Self, Self::Error> {
        Ok(Self {
            resource: row.resource,
            owner_node_id: row.owner_node_id,
            owner_incarnation: row.owner_incarnation,
            fence: u64::try_from(row.fence).map_err(|error| error.to_string())?,
            lease_revision: u64::try_from(row.lease_revision).map_err(|error| error.to_string())?,
            lease_expiry_ms: row.lease_expiry_ms,
            expected_job_revision: u64::try_from(row.expected_job_revision)
                .map_err(|error| error.to_string())?,
            result_id: row.result_id,
            result_ref: row.result_ref,
            result_job_json: row.result_job_json,
            accepted_at_ms: row.accepted_at_ms,
            kind: row.kind,
        })
    }
}
impl From<&mut Row<'_>> for PublishedResultRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            result_id: row.get("result_id"),
            result_ref: row.get("result_ref"),
            result_job_json: row.get("result_job_json"),
            fence: row.get("fence"),
            accepted_at_ms: row.get("accepted_at_ms"),
            kind: row.get("kind"),
        }
    }
}
impl From<&mut Row<'_>> for AcceptedAtRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            accepted_at_ms: row.get("accepted_at_ms"),
        }
    }
}

struct MigrationRow {
    cluster_id: String,
    source_fingerprint: String,
}
impl From<&mut Row<'_>> for MigrationRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            cluster_id: row.get("cluster_id"),
            source_fingerprint: row.get("source_fingerprint"),
        }
    }
}

struct ControlJobRow {
    intent_json: String,
    revision: i64,
    incarnation: String,
}
struct ControlJobStateRow {
    incarnation: String,
    revision: i64,
    intent_json: String,
    deleted: bool,
}
impl From<&mut Row<'_>> for ControlJobStateRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            incarnation: row.get("incarnation"),
            revision: row.get("revision"),
            intent_json: row.get("intent_json"),
            deleted: row.get::<i64>("deleted") != 0,
        }
    }
}
impl From<&mut Row<'_>> for ControlJobRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            intent_json: row.get("intent_json"),
            revision: row.get("revision"),
            incarnation: row.get("incarnation"),
        }
    }
}

struct JobIdentityRow {
    incarnation: String,
    revision: i64,
}
struct RangeResultRow {
    resource: String,
    scope_json: String,
    result_ref: String,
}
struct BudgetStateRow {
    state_json: String,
}
struct ScriptStateRow {
    state: String,
}
impl From<&mut Row<'_>> for ScriptStateRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            state: row.get("state"),
        }
    }
}
impl From<&mut Row<'_>> for BudgetStateRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            state_json: row.get("state_json"),
        }
    }
}
impl From<&mut Row<'_>> for RangeResultRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            resource: row.get("resource"),
            scope_json: row.get("scope_json"),
            result_ref: row.get("result_ref"),
        }
    }
}
impl From<&mut Row<'_>> for JobIdentityRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            incarnation: row.get("incarnation"),
            revision: row.get("revision"),
        }
    }
}
impl From<&mut Row<'_>> for RevisionRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            revision: row.get("revision"),
        }
    }
}

fn renewable_receipt_matches(
    receipt: &PublicationReceipt,
    token: &LeaseToken,
    expected: u64,
    result_id: &str,
    result_ref: &str,
    result_job_json: &str,
) -> bool {
    receipt.resource == token.resource
        && receipt.owner_node_id == token.owner_node_id
        && receipt.owner_incarnation == token.owner_incarnation
        && receipt.fence == token.fence
        && receipt.lease_revision >= token.revision
        && receipt.lease_expiry_ms >= token.expires_at_unix_ms
        && receipt.expected_job_revision == expected
        && receipt.result_id == result_id
        && receipt.result_ref == result_ref
        && receipt.result_job_json == result_job_json
}

fn validate_identity(resource: &str, node: &str, incarnation: &str) -> Result<(), String> {
    for (label, value, max) in [
        ("resource", resource, 256usize),
        ("owner node", node, 128usize),
        ("owner incarnation", incarnation, 128usize),
    ] {
        if value.is_empty() || value.len() > max {
            return Err(format!("{label} must contain 1..={max} bytes"));
        }
    }
    Ok(())
}

fn i64_value(label: &str, value: u64) -> Result<i64, String> {
    if value == 0 {
        return Err(format!("{label} must be greater than zero"));
    }
    i64::try_from(value).map_err(|error| format!("{label} is too large: {error}"))
}

fn unix_ms() -> Result<i64, String> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock precedes unix epoch: {error}"))?
        .as_millis();
    i64::try_from(millis).map_err(|error| format!("unix millisecond overflow: {error}"))
}

fn expiry(now: i64, ttl: Duration) -> Result<i64, String> {
    let ttl = ttl.clamp(Duration::from_secs(1), Duration::from_secs(300));
    now.checked_add(i64::try_from(ttl.as_millis()).map_err(|error| error.to_string())?)
        .ok_or_else(|| "lease expiry overflow".to_owned())
}

pub(crate) fn unix_ms_public() -> i64 {
    unix_ms().unwrap_or(0)
}

#[cfg(test)]
mod coordinator_protocol_tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn legacy_coordinator_scope_cannot_claim_or_renew_but_current_scope_takes_over() {
        let temp = tempfile::tempdir().unwrap();
        let raft = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let api = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let cfg = ClusterConfig {
            cluster_id: "protocol-test".into(),
            node_name: "new".into(),
            shared_dir: temp.path().join("shared"),
            advertise_url: "http://127.0.0.1:1".into(),
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
            control_raft_bind: raft.local_addr().unwrap().to_string(),
            control_api_bind: api.local_addr().unwrap().to_string(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: Vec::new(),
            torrent_payload_roots: Vec::new(),
        };
        drop(raft);
        drop(api);
        let store = ControlStore::start(&cfg).await.unwrap();
        for scope in [
            "{}",
            r#"{"lifecycleProtocol":1}"#,
            r#"{"lifecycleProtocol":"2"}"#,
        ] {
            let error = store
                .acquire(
                    "cluster/leader",
                    "protocol-test",
                    1,
                    "cluster-leader",
                    "old",
                    "old-process",
                    "leader",
                    scope,
                    1,
                    Duration::from_secs(10),
                )
                .await
                .unwrap_err();
            assert!(
                error.contains("coordinator requires lifecycleProtocol 2"),
                "{error}"
            );
        }
        // This exact row models a lease committed before the schema upgrade.
        store
            .client
            .execute("DROP TRIGGER coordinator_protocol_insert", params!())
            .await
            .unwrap();
        store
            .client
            .execute("DROP TRIGGER coordinator_protocol_update", params!())
            .await
            .unwrap();
        let LeaseClaim::Acquired(old) = store
            .acquire(
                "cluster/leader",
                "protocol-test",
                1,
                "cluster-leader",
                "old",
                "old-process",
                "leader",
                "{}",
                1,
                Duration::from_secs(10),
            )
            .await
            .unwrap()
        else {
            panic!("old fixture lease not committed")
        };
        for sql in CONTROL_SCHEMA
            .iter()
            .filter(|sql| sql.contains("CREATE TRIGGER IF NOT EXISTS coordinator_protocol_"))
        {
            store.client.execute(*sql, params!()).await.unwrap();
        }
        assert!(store
            .renew(&old, Duration::from_secs(10))
            .await
            .unwrap_err()
            .contains("coordinator requires lifecycleProtocol 2"));
        // Safe relinquishment remains available to the old node; no timer is
        // mistaken for worker cleanup or destructive-work quiescence.
        assert!(store.release(&old).await.unwrap());
        let scope =
            serde_json::json!({"lifecycleProtocol":crate::proto::LIFECYCLE_PROTOCOL}).to_string();
        let LeaseClaim::Acquired(current) = store
            .acquire(
                "cluster/leader",
                "protocol-test",
                1,
                "cluster-leader",
                "new",
                "new-process",
                "leader",
                &scope,
                1,
                Duration::from_secs(10),
            )
            .await
            .unwrap()
        else {
            panic!("current coordinator did not take over")
        };
        assert!(current.fence > old.fence);
        assert_eq!(current.owner_node_id, "new");
        let renewed = store
            .renew(&current, Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        assert!(renewed.revision > current.revision);
        store.seed_job(2, "ordinary", "{}").await.unwrap();
        assert!(
            matches!(
                store
                    .acquire(
                        "job/ordinary",
                        "protocol-test",
                        2,
                        "ordinary",
                        "worker",
                        "worker-process",
                        "download",
                        "{}",
                        1,
                        Duration::from_secs(10)
                    )
                    .await
                    .unwrap(),
                LeaseClaim::Acquired(_)
            ),
            "only coordinator authority requires the coordinator capability"
        );
        store.shutdown().await.unwrap();
    }
}
