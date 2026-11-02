//! Authenticated cancellation of an exact, removed grant. Actual joins precede
//! a cleanup decision; deadlines and empty queue rows never stand in for joins.
use crate::{
    cancellation::{CancellationDecision, CancellationRequest},
    leader::LeaderShared,
};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
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
    let control = shared.control.as_ref().ok_or("control unavailable")?;
    let removal = control
        .cancellation_removal(request.job, &request.incarnation)
        .await?
        .ok_or("exact removal tombstone unavailable")?;
    let grant = control
        .historical_grant(&request.resource, request.fence)
        .await?
        .ok_or("historical grant unavailable; retain for review")?;
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
    crate::cancellation::decide(control, &root, request, shared.authority_identity()).await
}
