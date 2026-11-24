//! Exact business/source intent shared by grant and automatic-control CAS.
use crate::control::ControlStore;
use hiqlite::{params, Row};
use nzbd_types::{Job, JobStatus};
use std::collections::{HashMap, HashSet};

/// Progress is not operator intent. Keep file identities/pauses/source params,
/// while allowing validated segment completion and measured timing to advance.
pub(crate) fn business_intent(job: &Job) -> Result<Vec<u8>, String> {
    serialize_business_intent(job, business_params(job).collect())
}

fn business_params(job: &Job) -> impl Iterator<Item = &(String, String)> {
    job.params.iter().filter(|(key, _)| {
        key != nzbd_types::CONTROL_PARAM
                // Verified file proof/generation belongs to the observing
                // node's recovery lane, not operator or source intent. The
                // exact file descriptor and custody checks remain below.
                && !key.starts_with("*File:proof:")
                && !key.starts_with("*File:generation:")
                && !matches!(
                    key.as_str(),
                    "*Control:attempts"
                        | "*Control:probe_after"
                        | "*Cluster:AutomaticControl"
                        | "*Runner:LifecycleTiming"
                        | "*Recovery:journal-loading"
                        // The leader's own terminal chain stamps its history
                        // cursor on the row (contract §2.1); it is not
                        // source intent.
                        | "*PP:history-seq"
                )
    })
}

fn serialize_business_intent(
    job: &Job,
    mut params: Vec<&(String, String)>,
) -> Result<Vec<u8>, String> {
    params.sort();
    let files = job
        .files
        .iter()
        .map(|file| {
            (
                file.id,
                &file.subject,
                &file.filename,
                file.filename_confirmed,
                file.is_par2,
                file.paused,
                &file.groups,
                file.date,
                file.segments
                    .iter()
                    .map(|segment| (segment.number, &segment.message_id, segment.size))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&(
        job.id,
        job.kind,
        &job.name,
        &job.dir_name,
        job.name_provisional,
        job.queued_at_unix,
        &job.original_name,
        &job.category,
        job.priority,
        &job.dupe,
        job.delivery_lifecycle,
        params,
        files,
    ))
    .map_err(|e| e.to_string())
}
/// Accepted journal replay can learn a publication length before that value
/// exists in replicated intent. This enrichment is recovery evidence, not a
/// new operator edit. An existing durable length must still match exactly.
pub(crate) fn source_intent_matches(local: &Job, durable: &Job) -> Result<bool, String> {
    let file_ids: HashSet<_> = local.files.iter().map(|file| file.id.0).collect();
    let durable_keys: HashSet<_> = durable.params.iter().map(|(key, _)| key.as_str()).collect();
    let mut local_key_counts = HashMap::new();
    for (key, _) in &local.params {
        *local_key_counts.entry(key.as_str()).or_insert(0_usize) += 1;
    }
    let params = business_params(local)
        .filter(|(key, value)| {
            let learned_size = key
                .strip_prefix("*File:size:")
                .and_then(|id| id.parse::<u32>().ok())
                .is_some_and(|id| {
                    key == &format!("*File:size:{id}")
                        && file_ids.contains(&id)
                        && value.parse::<u64>().is_ok_and(|size| size > 0)
                        && local_key_counts.get(key.as_str()) == Some(&1)
                        && !durable_keys.contains(key.as_str())
                });
            !learned_size
        })
        .collect();
    Ok(serialize_business_intent(local, params)? == business_intent(durable)?)
}

pub(crate) fn grant_intent_matches(local: &Job, durable: &Job) -> Result<bool, String> {
    Ok(!local
        .params
        .iter()
        .any(|(key, _)| key == nzbd_engine::automatic_control::AUTOMATIC_CONTROL_PARAM)
        && !durable.held()
        && !matches!(durable.status, JobStatus::Paused | JobStatus::Deleted)
        && durable.status == local.status
        && durable.control() == local.control()
        && source_intent_matches(local, durable)?)
}
struct IntentRow {
    data: String,
    bytes: i64,
    revision: i64,
    incarnation: String,
}
impl From<&mut Row<'_>> for IntentRow {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            data: row.get("data"),
            bytes: row.get("bytes"),
            revision: row.get("revision"),
            incarnation: row.get("incarnation"),
        }
    }
}
impl ControlStore {
    pub(crate) async fn job_has_work_grant(&self, job: u32) -> Result<bool, String> {
        let rows=self.client.query_consistent_map::<IntentRow,_>("SELECT '' AS data,0 AS bytes,0 AS revision,'' AS incarnation FROM work_leases WHERE job_id=?1 AND resource<>'cluster/leader' LIMIT 1",params!(i64::from(job))).await.map_err(|e|e.to_string())?;
        Ok(!rows.is_empty())
    }
    pub(crate) async fn current_job_intent(
        &self,
        job: u32,
    ) -> Result<Option<(Job, u64, String)>, String> {
        let limit = crate::proto::MAX_DESCRIPTOR_BYTES as i64;
        let row=self.client.query_consistent_map::<IntentRow,_>("SELECT CASE WHEN length(CAST(intent_json AS BLOB))<=?2 THEN intent_json ELSE '' END AS data,length(CAST(intent_json AS BLOB)) AS bytes,revision,incarnation FROM control_jobs WHERE job_id=?1 AND deleted=0",params!(i64::from(job),limit)).await.map_err(|e|e.to_string())?.into_iter().next();
        row.map(|row|{if row.bytes>limit{return Err("current job descriptor exceeds16MiB; source retained, reduce metadata before retrying".into());}Ok((crate::control::decode_intent(&row.data).map_err(|e|e.to_string())?,u64::try_from(row.revision).map_err(|e|e.to_string())?,row.incarnation))}).transpose()
    }
}

/// Reconcile only actor-proven automatic controls; caller holds mutation_serial.
/// A local queue snapshot is never a replacement for current replicated intent.
pub(crate) async fn reconcile(shared: &crate::leader::LeaderShared) -> Result<(), String> {
    use nzbd_engine::automatic_control::{transition, AUTOMATIC_CONTROL_PARAM};
    use std::collections::HashMap;
    let control = shared
        .control
        .as_ref()
        .ok_or("replicated control unavailable")?;
    let issuer = shared.authority_identity();
    let mut restore_current = false;
    let scope = format!("{}:{}:{}", issuer.0, issuer.1, issuer.2);
    // A private automatic-control marker is metadata, not permission to
    // execute unverified payload. Public export deliberately refuses jobs
    // whose recovery/writer joins are pending; unrelated pending work must
    // not prevent a successor from adopting control authority. Only the
    // exact actor-stamped transitions enter the source proof and Raft CAS.
    let mut locals = shared
        .engine
        .recovery_document()
        .await
        .map_err(|error| format!("read automatic control metadata: {error}"))?
        .jobs
        .into_iter()
        .filter(|job| transition(job).is_some())
        .collect::<Vec<_>>();
    shared.control_proofs.prune(
        &locals
            .iter()
            .filter_map(nzbd_engine::automatic_control::transition)
            .map(|marker| marker.identity)
            .collect(),
        &scope,
    );
    locals.sort_by_key(|job| job.id.0);
    let cursor = shared
        .control_proofs
        .cursor
        .load(std::sync::atomic::Ordering::Acquire);
    let rotate = locals.partition_point(|job| job.id.0 <= cursor);
    locals.rotate_left(rotate);
    for local in locals {
        let Some(marker) = transition(&local) else {
            continue;
        };
        let Some((mut durable, revision, _)) = control.current_job_intent(local.id.0).await? else {
            // A committed tombstone wins. The caller's projection restore drops
            // this local row; the marker cannot resurrect it.
            restore_current = true;
            continue;
        };
        if !source_intent_matches(&local, &durable)? {
            // New operator/source intent wins, including an uncommitted local
            // operator edit. Never infer automatic authority from revisions.
            restore_current = true;
            continue;
        }
        let applied = durable.control().as_ref() == Some(&marker.result_control)
            && durable.status == marker.result_status;
        if !applied {
            if durable.control() != marker.base_control || durable.status != marker.base_status {
                restore_current = true;
                continue;
            }
            match source_valid(shared, &durable, &marker, revision, &scope).await {
                Ok(true) => {}
                outcome => {
                    tracing::warn!(job=local.id.0,reason=?outcome,"automatic control remains pending on this job; source proof unavailable");
                    continue;
                }
            }
            durable.set_control(&marker.result_control);
            durable.status = marker.result_status;
            // These are recovery bookkeeping, never user/source parameters.
            for key in ["*Control:attempts", "*Control:probe_after"] {
                durable.params.retain(|(existing, _)| existing != key);
                durable.params.extend(
                    local
                        .params
                        .iter()
                        .filter(|(existing, _)| existing == key)
                        .cloned(),
                );
            }
            durable
                .params
                .retain(|(key, _)| key != AUTOMATIC_CONTROL_PARAM);
            control
                .commit_job_delta_guarded(
                    &[(u64::from(local.id.0), Some(durable.clone()))],
                    &HashMap::from([(u64::from(local.id.0), revision)]),
                    &[],
                    Some(issuer),
                )
                .await?;
        }
        if applied
            && durable
                .params
                .iter()
                .any(|(key, _)| key == AUTOMATIC_CONTROL_PARAM)
        {
            // Upgrade cleanup of a previously copied private marker. Only
            // metadata is removed; its exact accepted control is unchanged.
            durable
                .params
                .retain(|(key, _)| key != AUTOMATIC_CONTROL_PARAM);
            control
                .commit_job_delta_guarded(
                    &[(u64::from(local.id.0), Some(durable))],
                    &HashMap::from([(u64::from(local.id.0), revision)]),
                    &[],
                    Some(issuer),
                )
                .await?;
        }
        // Lost ACK after a successful Raft commit is an exact replay, not a
        // second transition. False means local intent changed meanwhile.
        shared
            .engine
            .ack_automatic_control(local.id, marker.identity)
            .await
            .map_err(|e| e.to_string())?;
    }
    if restore_current {
        // Only after all valid markers were committed/ACKed may a current
        // operator/source projection replace stale local automatic intent.
        let jobs = projection(shared).await?;
        shared
            .engine
            .adopt_replicated_authority(jobs)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct RelocationProofRequest {
    storage: String,
    source: nzbd_state::artifacts::SourceGeneration,
    from: std::path::PathBuf,
    to: std::path::PathBuf,
}
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct RelocationProofReply {
    storage: String,
    valid: bool,
}

async fn source_valid(
    shared: &crate::leader::LeaderShared,
    durable: &Job,
    marker: &nzbd_engine::automatic_control::AutomaticControlTransition,
    revision: u64,
    scope: &str,
) -> Result<bool, String> {
    let inventory = shared.engine.artifacts();
    let expected = shared
        .dest_dir
        .join(nzbd_engine::queue::job_dir_name(durable));
    let source = marker.source.clone();
    let result = marker.source_path.clone();
    let job_id = durable.id.0;
    use sha2::{Digest, Sha256};
    let key = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(marker, revision, scope, business_intent(durable)?))
                .map_err(|e| e.to_string())?
        )
    );
    if marker.storage_instance_id == inventory.installation {
        return shared.control_proofs.resolve(
            key,
            marker.identity.clone(),
            scope.into(),
            job_id,
            shared.cfg.node_name.clone(),
            async move {
                tokio::task::spawn_blocking(move || -> Result<bool, String> {
                    match (
                        source,
                        result,
                        inventory.for_job(job_id).map_err(|e| e.to_string())?,
                    ) {
                        (None, None, None) => Ok(true),
                        (Some(source), Some(path), Some(current)) => {
                            if current.terminal()
                                || current.id != source.artifact
                                || current.generation != source.generation
                                || current.path != path
                            {
                                return Ok(false);
                            }
                            if path == expected {
                                return Ok(true);
                            }
                            inventory
                                .has_succeeded_relocation(&source, &expected, &path)
                                .map_err(|e| e.to_string())
                        }
                        _ => Ok(false),
                    }
                })
                .await
                .map_err(|e| e.to_string())?
            },
        );
    }
    let fact = durable
        .params
        .iter()
        .find(|(key, _)| key == crate::delivery::AUTHORITY_COPY_PARAM)
        .filter(|(_, value)| value.len() <= 8192)
        .and_then(|(_, value)| {
            serde_json::from_str::<crate::delivery::AuthoritySourceCopy>(value).ok()
        });
    let Some(fact) = fact else {
        if source.is_none() && result.is_none() {
            return Ok(true);
        }
        return shared.control_proofs.resolve(
            key,
            marker.identity.clone(),
            scope.into(),
            job_id,
            marker.storage_instance_id.chars().take(128).collect(),
            async { Ok(false) },
        );
    };
    let (Some(source), Some(path)) = (source, result) else {
        return shared.control_proofs.resolve(
            key,
            marker.identity.clone(),
            scope.into(),
            job_id,
            fact.owner_node.chars().take(128).collect(),
            async { Ok(false) },
        );
    };
    if fact.copy.storage_instance_id != marker.storage_instance_id
        || fact.copy.source != source
        || fact.copy.job_id != durable.id.0
    {
        return shared.control_proofs.resolve(
            key,
            marker.identity.clone(),
            scope.into(),
            job_id,
            fact.owner_node.chars().take(128).collect(),
            async { Ok(false) },
        );
    }
    if path == expected {
        return Ok(true);
    }
    // Only a journal on the original storage owner can attest relocation.
    // Failure/offline is retried by the existing startup/scheduler loop.
    let layout = shared.layout.clone();
    let owner_name = fact.owner_node.clone();
    let client = crate::http::ClusterClient::new(shared.cfg.secret.clone());
    let storage = marker.storage_instance_id.clone();
    let marker_id = marker.identity.clone();
    shared.control_proofs.resolve(
        key,
        marker_id,
        scope.into(),
        job_id,
        fact.owner_node,
        async move {
            let owner = tokio::task::spawn_blocking(move || {
                crate::registry::read_nodes(&layout)
                    .into_iter()
                    .find(|node| node.name == owner_name)
            })
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| {
                format!("job {job_id} source owner is offline; retry when it returns")
            })?;
            let reply = client
                .post_json::<_, RelocationProofReply>(
                    &owner.api_url,
                    "/cluster/v1/work/relocation-proof",
                    &RelocationProofRequest {
                        storage: storage.clone(),
                        source,
                        from: expected,
                        to: path,
                    },
                )
                .await?;
            Ok(reply.storage == storage && reply.valid)
        },
    )
}

pub(crate) async fn relocation_proof(
    axum::extract::State(shared): axum::extract::State<std::sync::Arc<crate::leader::LeaderShared>>,
    axum::Json(req): axum::Json<RelocationProofRequest>,
) -> Result<axum::Json<RelocationProofReply>, (axum::http::StatusCode, String)> {
    let inventory = shared.engine.artifacts();
    let storage = inventory.installation.clone();
    if req.storage != storage
        || req.source.artifact.len() > 256
        || req.source.generation.len() > 256
        || req.from.as_os_str().len() > 4096
        || req.to.as_os_str().len() > 4096
    {
        return Err((
            axum::http::StatusCode::CONFLICT,
            "relocation owner/source identity changed".into(),
        ));
    }
    let valid = tokio::task::spawn_blocking(move || {
        inventory.has_succeeded_relocation(&req.source, &req.from, &req.to)
    })
    .await
    .map_err(|e| (axum::http::StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?
    .map_err(|e| (axum::http::StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
    Ok(axum::Json(RelocationProofReply { storage, valid }))
}

/// Preserve one exact locally pending automatic result in the local projection
/// only. A newer durable operator/source edit or tombstone always replaces it.
pub(crate) async fn projection(shared: &crate::leader::LeaderShared) -> Result<Vec<Job>, String> {
    let control = shared
        .control
        .as_ref()
        .ok_or("replicated control unavailable")?;
    // Adoption needs only the actor-stamped control metadata. Payload export
    // is deliberately fenced during recovery; requiring it here can restart
    // adoption on every pass and starve the pending recovery work.
    // Source authority is still checked by reconcile/source_valid before CAS.
    let local = shared
        .engine
        .recovery_document()
        .await
        .map_err(|error| format!("read control projection metadata: {error}"))?
        .jobs
        .into_iter()
        .map(|job| (u64::from(job.id.0), job))
        .collect::<std::collections::HashMap<_, _>>();
    let mut jobs = Vec::new();
    for (mut durable, _, _) in control.load_jobs().await? {
        if let Some(current) = local.get(&u64::from(durable.id.0)) {
            if let Some(marker) = nzbd_engine::automatic_control::transition(current) {
                if durable.control() == marker.base_control
                    && durable.status == marker.base_status
                    && source_intent_matches(current, &durable)?
                {
                    durable.set_control(&marker.result_control);
                    durable.status = marker.result_status;
                    for key in [
                        nzbd_engine::automatic_control::AUTOMATIC_CONTROL_PARAM,
                        "*Control:attempts",
                        "*Control:probe_after",
                    ] {
                        durable.params.retain(|(old, _)| old != key);
                        durable
                            .params
                            .extend(current.params.iter().filter(|(old, _)| old == key).cloned());
                    }
                }
            }
        }
        jobs.push(durable);
    }
    Ok(jobs)
}
