//! Small replicated admission identities; URL bytes stay in the shared outbox.
use crate::control::ControlStore;
use crate::leader::LeaderShared;
use hiqlite::{params, Row};
use nzbd_state::UrlAdmissionReceipt;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

fn same_identity(left: &UrlAdmissionReceipt, right: &UrlAdmissionReceipt) -> bool {
    left.protocol_version == right.protocol_version
        && left.request_token == right.request_token
        && left.submission_sha256 == right.submission_sha256
        && left.job_id == right.job_id
        && left.generation == right.generation
        && left.runner_instance_id == right.runner_instance_id
}

pub(crate) fn admission_statements(
    rows: &[UrlAdmissionReceipt],
) -> Result<Vec<(String, hiqlite::Params)>, String> {
    if rows.len() > 100 {
        return Err("admission page exceeds100 rows".into());
    }
    let mut size = 0;
    let mut writes = Vec::new();
    for row in rows {
        let data = serde_json::to_string(row).map_err(|e| e.to_string())?;
        size += data.len();
        if data.len() > 8192
            || size > 1024 * 1024
            || row.protocol_version != 1
            || row.job_id == 0
            || row.request_token.is_empty()
            || row.request_token.len() > 4096
        {
            return Err("admission receipt exceeds supported8KiB identity/1MiB page bounds; retained for review".into());
        }
        let identity = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    row.protocol_version,
                    &row.request_token,
                    &row.submission_sha256,
                    row.job_id,
                    &row.generation,
                    &row.runner_instance_id
                ))
                .map_err(|e| e.to_string())?
            )
        );
        writes.push(("INSERT INTO request_admissions(token,identity,job_id,data) VALUES(?1,?2,?3,?4) ON CONFLICT(token) DO NOTHING".into(),params!(&row.request_token,identity,i64::from(row.job_id),data)));
    }
    Ok(writes)
}
struct ReceiptRow {
    data: String,
}
impl From<&mut Row<'_>> for ReceiptRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            data: row.get("data"),
        }
    }
}
struct FenceRow {
    id: i64,
    revision: i64,
}
impl From<&mut Row<'_>> for FenceRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            id: row.get("job_id"),
            revision: row.get("revision"),
        }
    }
}
struct CounterRow {
    name: String,
    value: i64,
}
impl From<&mut Row<'_>> for CounterRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            name: row.get("name"),
            value: row.get("value"),
        }
    }
}
impl ControlStore {
    #[cfg(test)]
    pub(crate) async fn job_revision_fences(&self) -> Result<HashMap<u64, u64>, String> {
        self.client
            .query_consistent_map::<FenceRow, _>(
                "SELECT job_id,revision FROM control_jobs",
                params!(),
            )
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|row| {
                Ok((
                    u64::try_from(row.id).map_err(|e| e.to_string())?,
                    u64::try_from(row.revision).map_err(|e| e.to_string())?,
                ))
            })
            .collect()
    }
    pub(crate) async fn job_revision_fences_for(
        &self,
        ids: &[u64],
    ) -> Result<HashMap<u64, u64>, String> {
        let ids = serde_json::to_string(ids).map_err(|e| e.to_string())?;
        self.client.query_consistent_map::<FenceRow,_>("SELECT job_id,revision FROM control_jobs WHERE job_id IN(SELECT CAST(value AS INTEGER) FROM json_each(?1))",params!(ids)).await.map_err(|e|e.to_string())?.into_iter().map(|row|Ok((u64::try_from(row.id).map_err(|e|e.to_string())?,u64::try_from(row.revision).map_err(|e|e.to_string())?))).collect()
    }
    pub(crate) async fn admission(
        &self,
        token: &str,
    ) -> Result<Option<UrlAdmissionReceipt>, String> {
        let mut receipt: Option<UrlAdmissionReceipt> = self
            .client
            .query_consistent_map::<ReceiptRow, _>(
                "SELECT data FROM request_admissions WHERE token=?1",
                params!(token),
            )
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .next()
            .map(|row| serde_json::from_str(&row.data).map_err(|e| e.to_string()))
            .transpose()?;
        if let Some(receipt) = receipt.as_mut() {
            self.project_admission_progress(receipt).await?;
        }
        Ok(receipt)
    }
    async fn project_admission_progress(
        &self,
        receipt: &mut UrlAdmissionReceipt,
    ) -> Result<(), String> {
        if receipt.custody_state == "curator" {
            return Ok(());
        }
        let rows=self.client.query_consistent_map::<ReceiptRow,_>("SELECT json_object('deleted',j.deleted,'removalMode',(SELECT r.mode FROM cluster_job_removals r WHERE r.job_id=j.job_id AND r.incarnation=j.incarnation AND r.applied_revision=j.revision),'cancellationRecorded',EXISTS(SELECT 1 FROM cluster_cancellation_decisions d WHERE d.job_id=j.job_id AND d.incarnation=j.incarnation AND d.removal_revision=j.revision),'status',json_extract(j.intent_json,'$.status'),'files',coalesce(json_array_length(j.intent_json,'$.files'),0),'params',json(coalesce((SELECT json_group_array(json_array(json_extract(p.value,'$[0]'),substr(json_extract(p.value,'$[1]'),1,4096))) FROM json_each(j.intent_json,'$.params') p WHERE json_extract(p.value,'$[0]') IN('monarr-transfer','Failure:Stage','Failure:Reason','*Failure:Reason','Failure:KeepOnFailure','Artifact:Id','Artifact:Generation','Artifact:Custody','Delivery:Custody','*PP:done','*Cluster:AuthoritySourceCopy')),'[]'))) AS data FROM control_jobs j WHERE job_id=?1",params!(i64::from(receipt.job_id))).await.map_err(|e|e.to_string())?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(());
        };
        let value: serde_json::Value =
            serde_json::from_str(&row.data).map_err(|e| e.to_string())?;
        let params: Vec<(String, String)> =
            serde_json::from_value(value["params"].clone()).map_err(|e| e.to_string())?;
        let param = |key: &str| {
            params
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value.clone())
        };
        if param("monarr-transfer").as_deref() != Some(receipt.request_token.as_str()) {
            return Ok(());
        }
        if value["deleted"] == 1 {
            if value["status"] == "failed" {
                receipt.outcome = "failed".into();
                receipt.terminal_outcome = Some("FAILURE".into());
            } else {
                receipt.outcome = "deleted".into();
                receipt.terminal_outcome = Some("DELETED".into());
            }
        } else {
            receipt.outcome = value["status"].as_str().unwrap_or("post_processing").into();
            if receipt.outcome == "failed" {
                receipt.terminal_outcome = Some("FAILURE".into());
            }
            if let Some(outcome) = param(nzbd_types::PP_DONE_PARAM) {
                receipt.terminal_outcome = Some(outcome);
            }
        }
        receipt.failure_stage = param("Failure:Stage").or_else(|| receipt.failure_stage.clone());
        receipt.failure_cause = param("Failure:Reason")
            .or_else(|| param("*Failure:Reason"))
            .or_else(|| receipt.failure_cause.clone());
        receipt.keep_source_on_failure =
            param("Failure:KeepOnFailure").is_some_and(|keep| keep == "true");
        receipt.source_artifact_id =
            param("Artifact:Id").or_else(|| receipt.source_artifact_id.clone());
        receipt.source_generation =
            param("Artifact:Generation").or_else(|| receipt.source_generation.clone());
        if let Some(custody) = param("Delivery:Custody").or_else(|| param("Artifact:Custody")) {
            receipt.custody_state = custody;
        }
        // A queue tombstone does not prove byte disposal. Preserve the exact
        // owned-source identity and expose the raw-only cancellation limit
        // until a sealed cancellation decision supplies actual cleanup facts.
        let authority_source = param(crate::delivery::AUTHORITY_COPY_PARAM)
            .and_then(|value| {
                serde_json::from_str::<crate::delivery::AuthoritySourceCopy>(&value).ok()
            })
            .filter(|source| source.copy.job_id == receipt.job_id);
        if let Some(source) = authority_source.as_ref() {
            receipt
                .source_artifact_id
                .get_or_insert_with(|| source.copy.source.artifact.clone());
            receipt
                .source_generation
                .get_or_insert_with(|| source.copy.source.generation.clone());
        }
        if value["deleted"] == 1
            && value["cancellationRecorded"] == 0
            && receipt.source_cleanup_status.as_deref() != Some("removed")
            && receipt.custody_state != "deleted_by_runner"
            && receipt.source_artifact_id.is_some()
        {
            receipt.source_cleanup_status = Some("retained".into());
            receipt.failure_stage = Some("source_cleanup".into());
            let keep_files = value["removalMode"] == "keep_files";
            receipt.custody_state = if keep_files { "kept_by_user" } else { "runner" }.into();
            receipt.failure_cause=Some(if keep_files {
                "Removal kept the exact source files; no byte cleanup was authorized."
            }else{
                "Raw source retained for review: removal was recorded without a sealed cleanup receipt. The exact source owner must confirm stopped writers and file custody before cleanup."
            }.into());
        }
        if receipt.terminal_outcome.is_some()
            && value["files"] == 0
            && receipt.source_artifact_id.is_none()
        {
            receipt.custody_state = "no_payload".into();
            receipt.source_cleanup_status = Some("not_required".into());
        }
        Ok(())
    }
    pub(crate) async fn admissions_page(
        &self,
        after: &str,
    ) -> Result<Vec<UrlAdmissionReceipt>, String> {
        self.client
            .query_consistent_map::<ReceiptRow, _>(
                "SELECT data FROM request_admissions WHERE token>?1 ORDER BY token LIMIT 100",
                params!(after),
            )
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|row| serde_json::from_str(&row.data).map_err(|e| e.to_string()))
            .collect()
    }
    async fn legacy_receipt_authorized(
        &self,
        receipt: &UrlAdmissionReceipt,
    ) -> Result<bool, String> {
        let rows=self.client.query_consistent_map::<ReceiptRow,_>("SELECT json_extract(p.value,'$[1]') AS data FROM control_jobs j,json_each(j.intent_json,'$.params') p WHERE j.job_id=?1 AND json_extract(p.value,'$[0]')='monarr-transfer'",params!(i64::from(receipt.job_id))).await.map_err(|e|e.to_string())?;
        let generation = format!(
            "{}-url-{}-{:x}",
            receipt.runner_instance_id,
            receipt.job_id,
            Sha256::digest(receipt.request_token.as_bytes())
        );
        Ok(rows.len() == 1
            && rows[0].data == receipt.request_token
            && receipt.generation == generation)
    }
    #[cfg(test)]
    pub(crate) async fn import_admissions(
        &self,
        rows: &[UrlAdmissionReceipt],
    ) -> Result<(), String> {
        self.import_admissions_guarded(rows, None).await
    }
    pub(crate) async fn import_admissions_guarded(
        &self,
        rows: &[UrlAdmissionReceipt],
        authority: Option<(&str, &str, u64)>,
    ) -> Result<(), String> {
        let mut writes = admission_statements(rows)?;
        if let Some(authority) = authority {
            writes.insert(0, crate::control::authority_statement(authority)?);
        }
        if writes.is_empty() {
            return Ok(());
        }
        crate::delivery::bounded_transaction(&writes)?;
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
    pub(crate) async fn highwater(&self) -> Result<(u32, u32), String> {
        let mut ids = (0, 0);
        for row in self.client.query_consistent_map::<CounterRow,_>("SELECT name,value FROM control_counters WHERE name IN('next_job_id','next_file_id')",params!()).await.map_err(|e|e.to_string())? {
            let value=u32::try_from(row.value).map_err(|e|e.to_string())?;match row.name.as_str(){"next_job_id"=>ids.0=value,"next_file_id"=>ids.1=value,_=>{}}
        }
        Ok(ids)
    }
}

pub(crate) async fn initialize(shared: &LeaderShared) -> Result<(), String> {
    let authority = shared.authority_identity();
    let control = shared
        .control
        .as_ref()
        .ok_or("replicated control unavailable")?;
    if control.delivery_meta("instance").await?.is_none() {
        let mut cursor = String::new();
        let mut historic = None;
        loop {
            let page = shared
                .engine
                .url_admissions_page(&cursor, 100)
                .await
                .map_err(|e| e.to_string())?;
            if page.is_empty() {
                break;
            }
            cursor = page.last().unwrap().request_token.clone();
            for receipt in page {
                if control.admission(&receipt.request_token).await?.is_none()
                    && !control.legacy_receipt_authorized(&receipt).await?
                {
                    return Err(preserve_unproven_legacy(shared, authority, &receipt).await?);
                }
                if receipt.runner_instance_id.is_empty() || receipt.runner_instance_id.len() > 128 {
                    return Err("historic consumer identity unsupported; preserve snapshot and review before upgrade".into());
                }
                if historic
                    .as_ref()
                    .is_some_and(|instance| instance != &receipt.runner_instance_id)
                {
                    return Err("historic consumer identities are ambiguous; preserve accepted receipts and review cluster pin before upgrade".into());
                }
                historic = Some(receipt.runner_instance_id);
            }
        }
        if let Some(instance) = historic {
            fenced_write(shared,authority,vec![("INSERT INTO cluster_delivery_meta(key,value) VALUES('instance',?1) ON CONFLICT(key) DO NOTHING".into(),params!(instance))]).await?;
        }
    }
    control
        .initialize_delivery_identity_guarded(
            &shared.engine.artifacts().installation,
            &shared.cfg.node_name,
            Some(authority),
        )
        .await?;
    if control
        .delivery_meta("admissions_imported")
        .await?
        .is_none()
    {
        // Engine adopt_authority loaded the prior shared snapshot, including
        // receipts with no live job. Seed these before pruning any projection.
        let mut cursor = control
            .delivery_meta("admissions_import_cursor")
            .await?
            .unwrap_or_default();
        loop {
            let page = shared
                .engine
                .url_admissions_page(&cursor, 100)
                .await
                .map_err(|e| e.to_string())?;
            if page.is_empty() {
                break;
            }
            let mut accepted = Vec::new();
            for receipt in &page {
                if let Some(existing) = control.admission(&receipt.request_token).await? {
                    accepted.push(existing);
                } else if control.legacy_receipt_authorized(receipt).await? {
                    accepted.push(receipt.clone());
                } else {
                    return Err(preserve_unproven_legacy(shared, authority, receipt).await?);
                }
            }
            control
                .import_admissions_guarded(&accepted, Some(authority))
                .await?;
            cursor = page.last().unwrap().request_token.clone();
            fenced_write(shared,authority,vec![("INSERT INTO cluster_delivery_meta(key,value) VALUES('admissions_import_cursor',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value".into(),params!(&cursor))]).await?;
        }
        fenced_write(shared,authority,vec![("INSERT INTO cluster_delivery_meta(key,value) VALUES('admissions_imported','1') ON CONFLICT(key) DO NOTHING".into(),params!())]).await?;
        fenced_write(
            shared,
            authority,
            vec![(
                "DELETE FROM cluster_delivery_meta WHERE key='admission_migration_review'".into(),
                params!(),
            )],
        )
        .await?;
    }
    if control
        .delivery_meta("counter_triggers_backfilled")
        .await?
        .is_none()
    {
        // Old stores may predate the triggers. Their retained tombstones and
        // descriptors still provide authoritative monotonic allocation facts.
        fenced_write(shared,authority,vec![("INSERT INTO control_counters(name,value) SELECT 'next_job_id',coalesce(max(job_id),0) FROM control_jobs WHERE true ON CONFLICT(name) DO UPDATE SET value=max(value,excluded.value)".into(),params!())]).await?;
        fenced_write(shared,authority,vec![("INSERT INTO control_counters(name,value) SELECT 'next_file_id',coalesce(max(CAST(json_extract(f.value,'$.id') AS INTEGER)),0) FROM control_jobs j,json_each(j.intent_json,'$.files') f WHERE true ON CONFLICT(name) DO UPDATE SET value=max(value,excluded.value)".into(),params!())]).await?;
        fenced_write(shared,authority,vec![("INSERT INTO cluster_delivery_meta(key,value) VALUES('counter_triggers_backfilled','1') ON CONFLICT(key) DO NOTHING".into(),params!())]).await?;
    }
    // First process adoption reconciles recovered snapshot receipts, including
    // local-only admissions absent from the newly-local pending index.
    let mut cursor = String::new();
    loop {
        let page = shared
            .engine
            .url_admissions_page(&cursor, 100)
            .await
            .map_err(|e| e.to_string())?;
        if page.is_empty() {
            break;
        }
        cursor = page.last().unwrap().request_token.clone();
        for receipt in page {
            if let Some(accepted) = control.admission(&receipt.request_token).await? {
                if !same_identity(&receipt, &accepted) {
                    if receipt.job_id == accepted.job_id {
                        return Err("conflicting admission identities reuse the same job ID; preserve exact sources for review".into());
                    }
                    let origin = shared
                        .engine
                        .export_job(nzbd_types::JobId(receipt.job_id))
                        .await
                        .map_err(|e| e.to_string())?
                        .filter(|job| {
                            job.params.iter().any(|(key, value)| {
                                key == "monarr-transfer" && value == &receipt.request_token
                            })
                        });
                    if origin.is_some()
                        && !shared
                            .engine
                            .remove_job_silent(nzbd_types::JobId(receipt.job_id))
                            .await
                            .map_err(|e| e.to_string())?
                    {
                        return Err("conflicting local origin changed before cancellation".into());
                    }
                    shared
                        .engine
                        .discard_url_admission(receipt, origin)
                        .await
                        .map_err(|e| e.to_string())
                        .and_then(require_commit)?;
                }
                shared
                    .engine
                    .adopt_url_admissions(vec![accepted])
                    .await
                    .map_err(|e| e.to_string())
                    .and_then(require_commit)?;
                continue;
            }
            let origin = shared
                .engine
                .export_job(nzbd_types::JobId(receipt.job_id))
                .await
                .map_err(|e| e.to_string())?
                .filter(|job| {
                    job.params.iter().any(|(key, value)| {
                        key == "monarr-transfer" && value == &receipt.request_token
                    })
                });
            if origin.is_some()
                && !shared
                    .engine
                    .remove_job_silent(nzbd_types::JobId(receipt.job_id))
                    .await
                    .map_err(|e| e.to_string())?
            {
                return Err(
                    "unaccepted local origin changed during authority reconciliation".into(),
                );
            }
            shared
                .engine
                .discard_url_admission(receipt, origin)
                .await
                .map_err(|e| e.to_string())
                .and_then(require_commit)?;
        }
    }
    adopt(shared).await
}
pub(crate) async fn adopt(shared: &LeaderShared) -> Result<(), String> {
    let control = shared
        .control
        .as_ref()
        .ok_or("replicated control unavailable")?;
    shared
        .engine
        .begin_url_admission_adoption()
        .await
        .map_err(|e| e.to_string())
        .and_then(require_commit)?;
    let mut cursor = String::new();
    loop {
        let page = control.admissions_page(&cursor).await?;
        if page.is_empty() {
            break;
        }
        cursor = page.last().unwrap().request_token.clone();
        shared
            .engine
            .adopt_url_admissions(page)
            .await
            .map_err(|e| e.to_string())
            .and_then(require_commit)?;
    }
    shared
        .engine
        .finish_url_admission_adoption()
        .await
        .map_err(|e| e.to_string())
        .and_then(require_commit)?;
    let (job, file) = control.highwater().await?;
    shared
        .engine
        .adopt_replicated_highwater(job, file)
        .await
        .map_err(|e| e.to_string())
        .and_then(require_commit)?;
    shared
        .engine
        .set_consumer_instance_id(control.delivery_instance().await?)
        .await
        .map_err(|e| e.to_string())
        .and_then(require_commit)?;
    Ok(())
}
pub(crate) async fn pending_receipts(
    shared: &LeaderShared,
) -> Result<Vec<UrlAdmissionReceipt>, String> {
    shared
        .engine
        .url_unaccepted_admissions(100)
        .await
        .map_err(|e| e.to_string())
}

pub(crate) async fn adopt_counters(shared: &LeaderShared) -> Result<(), String> {
    let control = shared
        .control
        .as_ref()
        .ok_or("replicated control unavailable")?;
    let (job, file) = control.highwater().await?;
    shared
        .engine
        .adopt_replicated_highwater(job, file)
        .await
        .map_err(|e| e.to_string())
        .and_then(require_commit)
}

/// Only a linearizable catalog absence permits discarding a local uncommitted
/// admission. Transport/unknown-commit failures preserve it for later resolution.
pub(crate) async fn rollback_pending(shared: &LeaderShared) -> Result<(), String> {
    let control = shared
        .control
        .as_ref()
        .ok_or("replicated control unavailable")?;
    let mut absent = Vec::new();
    let mut replacements = Vec::new();
    for receipt in pending_receipts(shared).await? {
        if let Some(accepted) = control.admission(&receipt.request_token).await? {
            if same_identity(&receipt, &accepted) {
                shared
                    .engine
                    .adopt_url_admissions(vec![accepted])
                    .await
                    .map_err(|e| e.to_string())
                    .and_then(require_commit)?;
                continue;
            }
            if receipt.job_id == accepted.job_id {
                return Err("conflicting admission identity on reused job ID; retain exact source for review".into());
            }
            let origin = shared
                .engine
                .export_job(nzbd_types::JobId(receipt.job_id))
                .await
                .map_err(|e| e.to_string())?
                .filter(|job| {
                    job.params.iter().any(|(key, value)| {
                        key == "monarr-transfer" && value == &receipt.request_token
                    })
                });
            absent.push((receipt, origin));
            replacements.push(accepted);
        } else {
            // Capture exact origin before restoring authoritative jobs; discard
            // cannot remove a receipt while its matching local job is present.
            let origin = shared
                .engine
                .export_job(nzbd_types::JobId(receipt.job_id))
                .await
                .map_err(|e| e.to_string())?
                .filter(|job| {
                    job.params.iter().any(|(key, value)| {
                        key == "monarr-transfer" && value == &receipt.request_token
                    })
                });
            absent.push((receipt, origin));
        }
    }
    if !absent.is_empty() {
        let jobs = crate::control_recovery::projection(shared).await?;
        shared
            .engine
            .adopt_replicated_authority(jobs)
            .await
            .map_err(|e| e.to_string())?;
        adopt_counters(shared).await?;
        for (receipt, origin) in absent {
            shared
                .engine
                .discard_url_admission(receipt, origin)
                .await
                .map_err(|e| e.to_string())
                .and_then(require_commit)?;
        }
    }
    if !replacements.is_empty() {
        shared
            .engine
            .adopt_url_admissions(replacements)
            .await
            .map_err(|e| e.to_string())
            .and_then(require_commit)?;
    }
    Ok(())
}

fn require_commit(committed: bool) -> Result<(), String> {
    if committed {
        Ok(())
    } else {
        Err("engine refused durable admission projection; authority remains fenced until storage/intent is repaired".into())
    }
}

async fn fenced_write(
    shared: &LeaderShared,
    authority: (&str, &str, u64),
    mut writes: Vec<(String, hiqlite::Params)>,
) -> Result<(), String> {
    writes.insert(0, crate::control::authority_statement(authority)?);
    crate::delivery::bounded_transaction(&writes)?;
    writes[0] = crate::control::authority_statement(authority)?;
    shared
        .control
        .as_ref()
        .ok_or("replicated control unavailable")?
        .client
        .txn(writes)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    Ok(())
}

async fn preserve_unproven_legacy(
    shared: &LeaderShared,
    authority: (&str, &str, u64),
    receipt: &UrlAdmissionReceipt,
) -> Result<String, String> {
    let token_digest = format!("{:x}", Sha256::digest(receipt.request_token.as_bytes()));
    let message = format!(
        "legacy admission job {}, token SHA-256 {} lacks matching durable job/tombstone authority; accepted receipt preserved. Restore the original replicated control backup or reconcile this exact receipt identity before enabling the cluster",
        receipt.job_id,
        &token_digest[..16]
    );
    fenced_write(shared,authority,vec![("INSERT INTO cluster_delivery_meta(key,value) VALUES('admission_migration_review',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value".into(),params!(&message))]).await?;
    Ok(message)
}
