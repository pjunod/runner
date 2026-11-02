//! Authenticated cancellation of an exact, removed grant. Actual joins precede
//! a cleanup decision; deadlines and empty queue rows never stand in for joins.
use crate::{
    cancellation::{CancellationDecision, CancellationRequest},
    leader::LeaderShared,
};
use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use nzbd_state::artifacts::{WorkerControlContext, WorkerStopRequest, WorkerStoppedReceipt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StopRequest {
    pub job: u32,
    pub incarnation: String,
    pub resource: String,
    pub fence: u64,
    #[serde(default)]
    pub sealed: Option<CancellationRequest>,
}
fn reply<T: Serialize>(result: Result<T, String>) -> Response {
    match result {
        Ok(value) => Json(value).into_response(),
        Err(error) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error":error})),
        )
            .into_response(),
    }
}
pub(crate) async fn stop(
    State(shared): State<Arc<LeaderShared>>,
    Json(request): Json<StopRequest>,
) -> Response {
    let key = format!("stop:{}:{}", request.resource, request.fence);
    let operations = shared.clone();
    match operations
        .copy_operations
        .run(
            key,
            async move { reply(stop_exact(&shared, &request).await) },
        )
        .await
    {
        Ok((status, body)) => (
            status,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(error) => reply::<WorkerStoppedReceipt>(Err(error)),
    }
}
async fn stop_exact(
    shared: &Arc<LeaderShared>,
    request: &StopRequest,
) -> Result<WorkerStoppedReceipt, String> {
    let (grant, removal) = if shared.control.is_some() && shared.view.borrow().is_me {
        stop_authority_exact(shared, request).await?
    } else {
        let url = shared
            .view
            .borrow()
            .leader_url()
            .map(str::to_owned)
            .ok_or("removal authority unavailable")?;
        crate::http::ClusterClient::new(shared.cfg.secret.clone())
            .post_json(
                &url,
                "/cluster/v1/work/cancellation-stop-authority",
                request,
            )
            .await?
    };
    let inventory = shared.engine.artifacts();
    if grant.job_id != request.job
        || grant.job_incarnation != request.incarnation
        || grant.job_revision > removal.prior_revision
        || grant.initial_token.owner_node_id != shared.cfg.node_name
        || grant.storage_instance_id.as_deref() != Some(inventory.installation.as_str())
        || grant.start_ordering != 1
    {
        return Err(
            "grant has no exact receiver storage/start-ordering proof; retain for review".into(),
        );
    }
    let lease = format!("{}@{}", request.resource, request.fence);
    let inv = inventory.clone();
    let job = request.job;
    let inc = request.incarnation.clone();
    let id = lease.clone();
    let existing =
        tokio::task::spawn_blocking(move || inv.worker_grant_request_by_lease(job, &inc, &id))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
    let mut stop = existing.unwrap_or(WorkerStopRequest {
        job: request.job,
        context: WorkerControlContext {
            lease_id: lease,
            job_incarnation: request.incarnation.clone(),
            job_revision: grant.job_revision,
            control_revision: grant.job_revision,
            epoch: 0,
        },
        owner_incarnation: grant.initial_token.owner_incarnation.clone(),
        storage_instance_id: inventory.installation.clone(),
        removal_revision: None,
        authority_source_json: grant.source_binding_json.clone(),
        start_ordered: true,
    });
    if stop.owner_incarnation != grant.initial_token.owner_incarnation
        || stop.authority_source_json != grant.source_binding_json
    {
        return Err("recorded receiver start differs from immutable authority grant".into());
    }
    stop.removal_revision = Some(removal.applied_revision);
    // Persist the late-start fence before waiting for any operation. A request
    // which was issued but never received now cannot start behind the join.
    let inv = inventory.clone();
    let fence = stop.clone();
    let ticket = tokio::task::spawn_blocking(move || inv.prepare_worker_stop(&fence))
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
    if let Some(receipt) = ticket.receipt {
        return Ok(receipt);
    }
    let mut joined = if ticket.never_started {
        false
    } else {
        crate::worker::cancel_and_join_execution(shared, &stop.context).await?
    };
    if !joined && !ticket.never_started && grant.kind != "download" {
        // Exclusive inventory reopening covers Rust writer lifetime, not an
        // external child. Only its exact sealed completion may substitute.
        let sealed = request
            .sealed
            .as_ref()
            .ok_or("external execution join unavailable; retained for review")?;
        if sealed.token.resource != request.resource
            || sealed.token.fence != request.fence
            || sealed.job_id != request.job
        {
            return Err("sealed completion belongs to another execution".into());
        }
        let root = shared.cfg.shared_dir.join(".nzbd-cluster/generations");
        let sealed = sealed.clone();
        tokio::task::spawn_blocking(move || {
            crate::cancellation::sealed_operation_finished(&root, &sealed)
        })
        .await
        .map_err(|e| e.to_string())??;
        joined = true;
    }
    shared
        .engine
        .join_cluster_writers(stop, joined)
        .await
        .map_err(|e| e.to_string())
}
pub(crate) async fn completed(
    State(shared): State<Arc<LeaderShared>>,
    Json(request): Json<CancellationRequest>,
) -> Response {
    let key = format!("cancel:{}", request.receipt_id);
    let operations = shared.clone();
    match operations
        .copy_operations
        .run(key, async move {
            reply(complete_exact(&shared, &request).await)
        })
        .await
    {
        Ok((status, body)) => (
            status,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(error) => reply::<CancellationDecision>(Err(error)),
    }
}
async fn complete_exact(
    shared: &Arc<LeaderShared>,
    request: &CancellationRequest,
) -> Result<CancellationDecision, String> {
    if !shared.view.borrow().is_me || !shared.authority_ready() {
        return Err("cancellation authority unavailable".into());
    }
    let control = shared.control.as_ref().ok_or("control unavailable")?;
    let removal = control
        .cancellation_removal(request.job_id, &request.job_incarnation)
        .await?
        .ok_or("exact committed removal unavailable")?;
    // Stop every recorded writer of this incarnation, including an earlier
    // delegated download owner. A local empty queue cannot acknowledge it.
    if removal.mode == crate::cancellation::RemovalMode::DeleteFiles {
        for grant in control
            .historical_grants_for_removal(request.job_id, &request.job_incarnation)
            .await?
        {
            let stop = StopRequest {
                job: request.job_id,
                incarnation: request.job_incarnation.clone(),
                resource: grant.initial_token.resource.clone(),
                fence: grant.initial_token.fence,
                sealed: Some(request.clone()),
            };
            if grant.initial_token.owner_node_id == shared.cfg.node_name {
                stop_exact(shared, &stop).await?;
            } else {
                let node = crate::registry::read_nodes(&shared.layout)
                    .into_iter()
                    .find(|node| node.name == grant.initial_token.owner_node_id)
                    .ok_or("writer owner is offline; cancellation waits for its actual stop ACK")?;
                crate::http::ClusterClient::new(shared.cfg.secret.clone())
                    .post_json::<_, WorkerStoppedReceipt>(
                        &node.api_url,
                        "/cluster/v1/work/cancellation-stop",
                        &stop,
                    )
                    .await?;
            }
        }
    }
    let root = shared.cfg.shared_dir.join(".nzbd-cluster/generations");
    let decision =
        crate::cancellation::decide(control, &root, request, shared.authority_identity()).await?;
    if decision.disposition == "cleanup_pending" {
        let record = control
            .cluster_delivery(&decision.cleanup_id)
            .await?
            .ok_or("cancellation ledger unavailable")?;
        refresh_owner_seals(shared, &record).await?;
    }
    Ok(decision)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SealRequest {
    cleanup_id: String,
    copy_id: String,
}
#[derive(Debug)]
struct RemovedJob {
    incarnation: String,
    intent: String,
}
impl From<&mut hiqlite::Row<'_>> for RemovedJob {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            incarnation: row.get("incarnation"),
            intent: row.get("intent_json"),
        }
    }
}
pub(crate) async fn seal(
    State(shared): State<Arc<LeaderShared>>,
    Json(request): Json<SealRequest>,
) -> Response {
    let operations = shared.clone();
    let key = format!("cancel-seal:{}:{}", request.cleanup_id, request.copy_id);
    match operations
        .copy_operations
        .run(
            key,
            async move { reply(seal_exact(&shared, &request).await) },
        )
        .await
    {
        Ok((status, body)) => (
            status,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(error) => reply::<crate::cancellation::OwnerStopAck>(Err(error)),
    }
}
async fn seal_exact(
    shared: &Arc<LeaderShared>,
    request: &SealRequest,
) -> Result<crate::cancellation::OwnerStopAck, String> {
    let authority: SealAuthority = if shared.control.is_some() && shared.view.borrow().is_me {
        seal_authority_exact(shared, request).await?
    } else {
        let url = shared
            .view
            .borrow()
            .leader_url()
            .map(str::to_owned)
            .ok_or("removal authority unavailable")?;
        crate::http::ClusterClient::new(shared.cfg.secret.clone())
            .post_json(
                &url,
                "/cluster/v1/work/cancellation-seal-authority",
                request,
            )
            .await?
    };
    let copy = &authority.copy;
    if copy.owner_node != shared.cfg.node_name {
        return Err("copy belongs to another owner".into());
    }
    let artifact = copy.artifact.as_ref().ok_or("copy has no owned artifact")?;
    let job = authority.job;
    let mut names = Vec::with_capacity(job.files.len() * 2);
    for file in &job.files {
        names.push(format!(".runner-file-{}.part", file.id.0));
        if file.filename_confirmed {
            names.push(file.filename.clone());
        }
    }
    let guard = shared.copy_guards.lock_for(copy);
    let _guard = guard.lock().await;
    let inventory = shared.engine.artifacts();
    let target = artifact.clone();
    let files = tokio::task::spawn_blocking(move || {
        inventory.cluster_cancellation_file_seal(&target, &names)
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| e.to_string())?;
    Ok(crate::cancellation::OwnerStopAck {
        cleanup_id: request.cleanup_id.clone(),
        copy_id: request.copy_id.clone(),
        job_id: job.id.0,
        job_incarnation: authority.incarnation,
        removal_applied_revision: authority.revision,
        copy: artifact.clone(),
        files,
    })
}

pub(crate) async fn refresh_owner_seals(
    shared: &Arc<LeaderShared>,
    record: &crate::delivery::ClusterDelivery,
) -> Result<(), String> {
    let control = shared.control.as_ref().ok_or("control unavailable")?;
    for copy in record
        .copies
        .iter()
        .filter(|copy| copy.artifact.is_some() && copy.status == "review_required")
    {
        let seal = SealRequest {
            cleanup_id: record.delivery.delivery_id.clone(),
            copy_id: copy.id.clone(),
        };
        let ack = if copy.owner_node == shared.cfg.node_name {
            seal_exact(shared, &seal).await
        } else {
            match crate::registry::read_nodes(&shared.layout)
                .into_iter()
                .find(|node| node.name == copy.owner_node)
            {
                Some(node) => {
                    crate::http::ClusterClient::new(shared.cfg.secret.clone())
                        .post_json::<_, crate::cancellation::OwnerStopAck>(
                            &node.api_url,
                            "/cluster/v1/work/cancellation-seal",
                            &seal,
                        )
                        .await
                }
                None => Err("source owner offline; source retained".into()),
            }
        };
        match ack {
            Ok(ack) => {
                let result = if ack.copy.selected {
                    crate::cancellation::acknowledge_unpublished_owner_stop(
                        control,
                        &ack,
                        &copy.owner_node,
                        shared.authority_identity(),
                    )
                    .await
                } else {
                    crate::cancellation::acknowledge_owner_stop(
                        control,
                        &ack,
                        &copy.owner_node,
                        shared.authority_identity(),
                    )
                    .await
                };
                if let Err(error) = result {
                    tracing::warn!(%error,"cancelled copy awaits exact owner acknowledgment");
                }
            }
            Err(error) => {
                tracing::warn!(%error,copy=%copy.id,"cancelled copy awaits exact owner seal")
            }
        }
    }

    Ok(())
}

type StopAuthority = (
    crate::grant_receipts::HistoricalGrant,
    crate::cancellation::RemovalRecord,
);
pub(crate) async fn stop_authority(
    State(shared): State<Arc<LeaderShared>>,
    Json(request): Json<StopRequest>,
) -> Response {
    reply(stop_authority_exact(&shared, &request).await)
}
async fn stop_authority_exact(
    shared: &Arc<LeaderShared>,
    request: &StopRequest,
) -> Result<StopAuthority, String> {
    if !shared.view.borrow().is_me {
        return Err("current removal authority required".into());
    }
    let control = shared.control.as_ref().ok_or("control unavailable")?;
    let removal = control
        .cancellation_removal(request.job, &request.incarnation)
        .await?
        .ok_or("exact removal tombstone unavailable")?;
    let grant = control
        .historical_grant(&request.resource, request.fence)
        .await?
        .ok_or("historical grant unavailable")?;
    if grant.job_id != request.job
        || grant.job_incarnation != request.incarnation
        || grant.job_revision > removal.prior_revision
    {
        return Err("grant differs from exact removal".into());
    }
    Ok((grant, removal))
}
#[derive(Serialize, Deserialize)]
struct SealAuthority {
    copy: crate::delivery::CopyObligation,
    job: nzbd_types::Job,
    incarnation: String,
    revision: u64,
}
pub(crate) async fn seal_authority(
    State(shared): State<Arc<LeaderShared>>,
    Json(request): Json<SealRequest>,
) -> Response {
    reply(seal_authority_exact(&shared, &request).await)
}
async fn seal_authority_exact(
    shared: &Arc<LeaderShared>,
    request: &SealRequest,
) -> Result<SealAuthority, String> {
    use hiqlite::params;
    if !shared.view.borrow().is_me {
        return Err("current removal authority required".into());
    }
    let control = shared.control.as_ref().ok_or("control unavailable")?;
    let record = control
        .cluster_delivery(&request.cleanup_id)
        .await?
        .ok_or("cancelled copy ledger unavailable")?;
    if !record.cleanup_only
        || record.source_disposition.as_deref() != Some("cancellation_delete_files")
    {
        return Err("this removal keeps files; no cleanup authority".into());
    }
    let copy = record
        .copies
        .iter()
        .find(|copy| copy.id == request.copy_id)
        .cloned()
        .ok_or("exact copy unavailable")?;
    let removed=control.client.query_consistent_map::<RemovedJob,_>("SELECT j.incarnation,j.intent_json FROM control_jobs j JOIN cluster_cancellation_decisions d ON d.job_id=j.job_id AND d.incarnation=j.incarnation AND d.removal_revision=j.revision WHERE d.cleanup_id=?1 AND j.deleted=1 AND length(CAST(j.intent_json AS BLOB))<=16777216",params!(&request.cleanup_id)).await.map_err(|e|e.to_string())?.into_iter().next().ok_or("exact cancelled job authority changed")?;
    let job: nzbd_types::Job = serde_json::from_str(&removed.intent).map_err(|e| e.to_string())?;
    let removal = control
        .cancellation_removal(job.id.0, &removed.incarnation)
        .await?
        .ok_or("exact removal unavailable")?;
    Ok(SealAuthority {
        copy,
        job,
        incarnation: removed.incarnation,
        revision: removal.applied_revision,
    })
}
