use crate::Node;
use crate::app_state::RaftType;
use crate::helpers::{deserialize, get_raft_metrics};
use crate::network::frame_io::{
    CLOSE_WRITE_TIMEOUT, write_close_frame_flushed, write_frame_flushed,
    write_socket_close_frame_flushed,
};
use crate::network::handshake::HandshakeSecret;
use crate::network::{AppStateExt, Error, serialize_network, validate_secret};
use axum::extract::Path;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use chrono::Utc;
use fastwebsockets::{FragmentCollectorRead, Frame, OpCode, Payload, upgrade};
use openraft::{ServerState, StoredMembership};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::ops::{Deref, Sub};
#[cfg(feature = "sqlite")]
use std::sync::Arc;
#[cfg(feature = "sqlite")]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio::{task, time};
use tracing::{debug, error, info, warn};

#[cfg(feature = "cache")]
use crate::store::state_machine::memory::{
    kv_handler::CacheRequestHandler,
    state_machine::{CacheRequest, CacheResponse},
};

#[cfg(feature = "dlock")]
use crate::store::state_machine::memory::dlock_handler::{
    LockAwaitPayload, LockRequest, LockState,
};

#[cfg(feature = "sqlite")]
use crate::{
    client::{
        DB_QUORUM_WATERMARK_COMPAT_PROBE, DB_QUORUM_WATERMARK_MARKER, db_quorum_watermark_local,
    },
    migration::Migration,
    query::{query_consistent_local, query_owned_local, rows::RowOwned},
    store::state_machine::sqlite::state_machine::{Query, QueryWrite},
};

#[cfg(feature = "listen_notify")]
use crate::store::state_machine::memory::notify_handler::NotifyRequest;
#[cfg(feature = "listen_notify")]
use axum::response::sse;
#[cfg(feature = "listen_notify")]
use futures_util::stream::Stream;

pub async fn health(state: AppStateExt) -> Result<(), Error> {
    #[cfg(all(not(feature = "sqlite"), not(feature = "cache")))]
    panic!("neither `sqlite` nor `cache` feature enabled");

    #[cfg(any(feature = "sqlite", feature = "cache"))]
    {
        if check_health(&state).await.is_err() {
            // after at least 3 seconds, we should have a new leader
            time::sleep(Duration::from_secs(3)).await;
            check_health(&state).await?;
        }
    }

    Ok(())
}

#[cfg(any(feature = "sqlite", feature = "cache"))]
async fn check_health(state: &AppStateExt) -> Result<(), Error> {
    if Utc::now().sub(state.app_start).num_seconds() < state.health_check_delay_secs as i64 {
        info!(
            "Early health check within the HQL_HEALTH_CHECK_DELAY_SECS timeframe - returning true"
        );
        return Ok(());
    }

    #[cfg(feature = "sqlite")]
    {
        let metrics = state.raft_db.raft.metrics().borrow().clone();
        metrics.running_state?;
        if metrics.current_leader.is_none() {
            return Err(Error::LeaderChange(
                "The leader voting process has not finished yet for Raft DB".into(),
            ));
        }
    }
    #[cfg(feature = "cache")]
    {
        let metrics = state.raft_cache.raft.metrics().borrow().clone();
        metrics.running_state?;
        if metrics.current_leader.is_none() {
            return Err(Error::LeaderChange(
                "The leader voting process has not finished yet Raft Cache".into(),
            ));
        }
    }

    Ok(())
}

#[tracing::instrument(skip_all)]
pub async fn ready(state: AppStateExt) -> Result<(), Error> {
    #[cfg(all(not(feature = "sqlite"), not(feature = "cache")))]
    panic!("neither `sqlite` nor `cache` feature enabled");

    if state.is_shutting_down.load(Ordering::Relaxed) {
        return Err(Error::Error("Node is shutting down".into()));
    }

    let secs_since_start = Utc::now().sub(state.app_start).num_seconds();

    #[cfg(feature = "sqlite")]
    {
        if !state.raft_db.is_startup_finished.load(Ordering::Relaxed) {
            warn!("Node is still starting up (sqlite)");
            return Err(Error::Error("Node is still starting up (sqlite)".into()));
        }

        // to avoid a chicken-and-egg problem, a pristine node 1 should always return ready
        let is_pristine_node_1 =
            state.id == 1 && !state.raft_db.raft.is_initialized().await? && secs_since_start > 10;

        if !is_pristine_node_1 {
            if state.raft_db.is_raft_stopped.load(Ordering::Relaxed) {
                warn!("sqlite raft is not running");
                return Err(Error::Error("sqlite raft is not running".into()));
            }

            let metrics = get_raft_metrics(&state, &RaftType::Sqlite).await;
            ensure_ready_member(
                state.id,
                state.learner_only,
                metrics.state,
                &metrics.membership_config,
                "sqlite",
            )?;

            if metrics.current_leader.is_none() && (state.id != 1 || secs_since_start < 10) {
                warn!("sqlite raft leader vote in progress - secs_since_start: {secs_since_start}");
                return Err(Error::Error("sqlite raft leader vote in progress".into()));
            }
        }
    }

    #[cfg(feature = "cache")]
    {
        if !state.raft_cache.is_startup_finished.load(Ordering::Relaxed) {
            warn!("Node is still starting up (cache)");
            return Err(Error::Error("Node is still starting up (cache)".into()));
        }

        let is_pristine_node_1 = state.id == 1
            && !state.raft_cache.raft.is_initialized().await?
            && secs_since_start > 10;

        if !is_pristine_node_1 {
            if state.raft_cache.is_raft_stopped.load(Ordering::Relaxed) {
                warn!("cache raft is not running");
                return Err(Error::Error("cache raft is not running".into()));
            }

            let metrics = get_raft_metrics(&state, &RaftType::Cache).await;
            ensure_ready_member(
                state.id,
                state.learner_only,
                metrics.state,
                &metrics.membership_config,
                "cache",
            )?;

            if metrics.current_leader.is_none() && (state.id != 1 || secs_since_start < 10) {
                warn!("cache raft leader vote in progress");
                return Err(Error::Error(
                    "cache raft leader vote in progress - secs_since_start: {secs_since_start}"
                        .into(),
                ));
            }
        }
    }

    Ok(())
}

fn ensure_ready_member(
    id: u64,
    learner_only: bool,
    state: ServerState,
    membership_config: &StoredMembership<u64, Node>,
    raft_label: &'static str,
) -> Result<(), Error> {
    if state == ServerState::Shutdown {
        warn!("not yet a ready member of the {raft_label} raft");
        return Err(Error::Error(
            format!("not yet a ready member of the {raft_label} raft").into(),
        ));
    }

    if state != ServerState::Learner && membership_config.voter_ids().any(|voter_id| voter_id == id)
    {
        return Ok(());
    }

    if learner_only
        && state == ServerState::Learner
        && membership_config
            .nodes()
            .any(|(member_id, _)| *member_id == id)
    {
        return Ok(());
    }

    warn!("not yet a ready member of the {raft_label} raft");
    Err(Error::Error(
        format!("not yet a ready member of the {raft_label} raft").into(),
    ))
}

pub async fn post_create_backup(state: AppStateExt, headers: HeaderMap) -> Result<(), Error> {
    validate_secret(&state, &headers)?;

    #[cfg(all(feature = "backup", feature = "sqlite"))]
    {
        let mut leader = 0;
        for _ in 0..5 {
            match state.raft_db.raft.current_leader().await {
                None => {
                    time::sleep(Duration::from_secs(1)).await;
                }
                Some(current) => {
                    leader = current;
                    break;
                }
            }
        }
        if leader == 0 {
            return Err(Error::LeaderChange("Leader election ongoing".into()));
        }

        let now = Utc::now().timestamp();
        if leader == state.id {
            state
                .raft_db
                .raft
                .client_write(QueryWrite::Backup((state.id, now)))
                .await?;
        } else {
            let (ack, rx) = tokio::sync::oneshot::channel();
            state
                .tx_client_stream
                .send_async(crate::client::stream::ClientStreamReq::Backup(
                    crate::client::stream::ClientBackupPayload {
                        request_id: state.new_request_id(),
                        node_id: leader,
                        ts: now,
                        ack,
                    },
                ))
                .await
                .map_err(|err| Error::Error(err.to_string().into()))?;
            rx.await
                .map_err(|err| Error::Error(err.to_string().into()))??;
        }
    }

    Ok(())
}

pub async fn ping() {}

#[cfg(test)]
mod tests {
    use super::{
        ApiStreamResponse, ApiStreamResponsePayload, WsWriteMsg, api_response_writer,
        ensure_ready_member, wait_for_raft_stop, write_api_response_frame,
    };
    use crate::Node;
    use crate::network::serialize_network;
    use fastwebsockets::Role;
    use openraft::{Membership, ServerState, StoredMembership};
    use std::collections::{BTreeMap, BTreeSet};

    #[tokio::test]
    async fn api_stream_stop_watch_ignores_live_updates_and_observes_terminal_states() {
        for terminal in [0, 1, 2] {
            let (sender, receiver) =
                tokio::sync::watch::channel(openraft::RaftMetrics::<u64, Node>::new_initial(1));
            let waiter = tokio::spawn(wait_for_raft_stop(receiver));
            sender.send_modify(|metrics| metrics.current_term += 1);
            tokio::task::yield_now().await;
            assert!(
                !waiter.is_finished(),
                "a live Raft update must keep the socket open"
            );
            match terminal {
                0 => sender.send_modify(|metrics| metrics.state = ServerState::Shutdown),
                1 => sender.send_modify(|metrics| {
                    metrics.running_state = Err(openraft::error::Fatal::Stopped)
                }),
                _ => drop(sender),
            }
            tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
                .await
                .expect("terminal Raft state closes an existing API stream")
                .unwrap();
        }
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn production_api_response_writer_flushes_serialized_response_through_tls() {
        let response = ApiStreamResponse {
            request_id: 17,
            result: ApiStreamResponsePayload::Query(Ok(Vec::new())),
        };
        let bytes = serialize_network(&response);
        crate::network::frame_io::tests::exercise_gated_tls_writer(
            Role::Server,
            bytes,
            true,
            |mut write, bytes| async move { write_api_response_frame(&mut write, &bytes).await },
        )
        .await;
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn production_api_response_writer_reports_flush_failure_and_closes() {
        let write = crate::network::frame_io::tests::split_writer(
            crate::network::frame_io::tests::TestIo::failing_flush(),
        );
        let (tx, rx) = flume::bounded(1);
        let (closed, closed_rx) = tokio::sync::watch::channel(false);
        let (finished, outcome) = tokio::sync::oneshot::channel();
        tx.send_async(WsWriteMsg::Payload(ApiStreamResponse {
            request_id: 17,
            result: ApiStreamResponsePayload::Query(Ok(Vec::new())),
        }))
        .await
        .expect("queue API response");

        api_response_writer(write, rx, closed, finished).await;

        assert!(*closed_rx.borrow());
        let error = outcome
            .await
            .expect("writer terminal outcome")
            .expect_err("flush failure must terminate the writer");
        assert!(error.contains("injected flush failure"));
    }

    #[test]
    fn learner_only_readiness_accepts_committed_learner_member() {
        let membership = membership_with_voters_and_learners([1, 2], [1, 2, 3]);

        assert!(ensure_ready_member(3, true, ServerState::Learner, &membership, "test").is_ok());
    }

    #[test]
    fn learner_only_readiness_rejects_non_member_learner() {
        let membership = membership_with_voters_and_learners([1, 2], [1, 2]);

        assert!(ensure_ready_member(3, true, ServerState::Learner, &membership, "test").is_err());
    }

    #[test]
    fn learner_only_readiness_rejects_learners_when_disabled() {
        let membership = membership_with_voters_and_learners([1, 2], [1, 2, 3]);

        assert!(ensure_ready_member(3, false, ServerState::Learner, &membership, "test").is_err());
    }

    #[test]
    fn member_readiness_rejects_learner_state_without_learner_only() {
        let membership = membership_with_voters_and_learners([1, 2, 3], [1, 2, 3]);

        assert!(ensure_ready_member(3, false, ServerState::Learner, &membership, "test").is_err());
    }

    #[test]
    fn member_readiness_accepts_voter_in_non_learner_state() {
        let membership = membership_with_voters_and_learners([1, 2, 3], [1, 2, 3]);

        assert!(ensure_ready_member(3, false, ServerState::Follower, &membership, "test").is_ok());
    }

    #[test]
    fn member_readiness_rejects_shutdown_voter() {
        let membership = membership_with_voters_and_learners([1, 2, 3], [1, 2, 3]);

        assert!(ensure_ready_member(3, false, ServerState::Shutdown, &membership, "test").is_err());
    }

    fn membership_with_voters_and_learners<const VOTERS: usize, const MEMBERS: usize>(
        voters: [u64; VOTERS],
        members: [u64; MEMBERS],
    ) -> StoredMembership<u64, Node> {
        let voters = BTreeSet::from(voters);
        let nodes = members
            .into_iter()
            .map(|id| {
                (
                    id,
                    Node {
                        id,
                        addr_raft: format!("localhost:{}", 8100 + id),
                        addr_api: format!("localhost:{}", 8200 + id),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();

        StoredMembership::new(None, Membership::new(vec![voters], nodes))
    }
}

#[cfg(feature = "listen_notify")]
pub async fn listen(
    state: AppStateExt,
    headers: HeaderMap,
) -> Result<sse::Sse<impl Stream<Item = Result<sse::Event, Error>>>, Error> {
    validate_secret(&state, &headers)?;

    let (tx, rx) = flume::bounded(1);
    state
        .raft_cache
        .tx_notify
        .send_async(NotifyRequest::Listen(tx))
        .await?;

    Ok(sse::Sse::new(rx.into_stream()).keep_alive(sse::KeepAlive::default()))
}

#[cfg(not(feature = "listen_notify"))]
pub async fn listen(state: AppStateExt, headers: HeaderMap) -> Result<(), Error> {
    validate_secret(&state, &headers)?;
    Err(Error::Config(
        "'listen_notify' feature is not active".into(),
    ))
}

/// This is the WebSocket stream a Raft client (Followers) connects to.
pub async fn stream(
    state: AppStateExt,
    Path(raft_type): Path<RaftType>,
    ws: upgrade::IncomingUpgrade,
) -> Result<impl IntoResponse, Error> {
    let (response, socket) = ws.upgrade()?;
    debug!("New Raft Stream for {:?}", raft_type);

    #[cfg(feature = "cache")]
    {
        if !state.raft_cache.is_startup_finished.load(Ordering::Relaxed) {
            warn!("Cache Raft still starting up - rejecting client streaming connection");
            return Err(Error::BadRequest("Raft is still starting up".into()));
        }
        if state.raft_cache.is_raft_stopped.load(Ordering::Relaxed) {
            warn!("Cache Raft has been stopped - rejecting client streaming connection");
            return Err(Error::BadRequest("Raft has been stopped".into()));
        }
    }
    #[cfg(feature = "sqlite")]
    {
        if !state.raft_db.is_startup_finished.load(Ordering::Relaxed) {
            warn!("Sqlite Raft still starting up - rejecting client streaming connection");
            return Err(Error::BadRequest("Raft is still starting up".into()));
        }
        if state.raft_db.is_raft_stopped.load(Ordering::Relaxed) {
            warn!("Sqlite Raft has been stopped - rejecting client streaming connection");
            return Err(Error::BadRequest("Raft has been stopped".into()));
        }
    }

    let task_status = state.snapshot_transport.clone();
    tokio::task::spawn(async move {
        let _task_guard = task_status.owned_async_task();
        if let Err(err) = handle_socket_concurrent(state, socket, raft_type).await {
            error!("Error in websocket connection: {}", err);
        }
    });

    Ok(response)
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ApiStreamRequest {
    pub(crate) request_id: usize,
    pub(crate) payload: ApiStreamRequestPayload,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ApiStreamRequestPayload {
    #[cfg(feature = "sqlite")]
    Execute(Query),
    #[cfg(feature = "sqlite")]
    ExecuteReturning(Query),
    #[cfg(feature = "sqlite")]
    Transaction(Vec<Query>),
    #[cfg(feature = "sqlite")]
    QueryConsistent(Query),
    #[cfg(feature = "sqlite")]
    Batch(std::borrow::Cow<'static, str>),
    #[cfg(feature = "sqlite")]
    Migrate(Vec<Migration>),

    #[cfg(feature = "backup")]
    Backup((crate::NodeId, i64)),

    #[cfg(feature = "cache")]
    KV(CacheRequest),

    // remote-only clients
    #[cfg(feature = "sqlite")]
    Query(Query),
    #[cfg(feature = "cache")]
    KVGet(CacheRequest),
    #[cfg(feature = "dlock")]
    LockAwait(CacheRequest),
    #[cfg(feature = "listen_notify_local")]
    Notify(CacheRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ApiStreamResponse {
    pub(crate) request_id: usize,
    pub(crate) result: ApiStreamResponsePayload,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ApiStreamResponsePayload {
    #[cfg(feature = "sqlite")]
    Execute(Result<usize, Error>),
    #[cfg(feature = "sqlite")]
    ExecuteReturning(Result<Vec<Result<RowOwned, Error>>, Error>),
    #[cfg(feature = "sqlite")]
    Transaction(Result<Vec<Result<usize, Error>>, Error>),
    #[cfg(feature = "sqlite")]
    Query(Result<Vec<RowOwned>, Error>),
    #[cfg(feature = "sqlite")]
    QueryConsistent(Result<Vec<RowOwned>, Error>),
    #[cfg(feature = "sqlite")]
    Batch(Result<Vec<Result<usize, Error>>, Error>),
    #[cfg(feature = "sqlite")]
    Migrate(Result<(), Error>),

    #[cfg(feature = "backup")]
    Backup(Result<(), Error>),

    #[cfg(feature = "cache")]
    KV(Result<CacheResponse, Error>),

    #[cfg(feature = "dlock")]
    Lock(LockState),

    #[cfg(feature = "listen_notify_local")]
    Notify(Result<(), Error>),
}

#[derive(Debug)]
pub(crate) enum WsWriteMsg {
    Payload(ApiStreamResponse),
    Break,
}

async fn handle_socket_concurrent(
    state: AppStateExt,
    socket: upgrade::UpgradeFut,
    raft_type: RaftType,
) -> Result<(), fastwebsockets::WebSocketError> {
    let mut ws = socket.await?;
    ws.set_auto_close(true);

    let _client_id = match HandshakeSecret::server(&mut ws, state.secret_api.as_bytes()).await {
        Ok(id) => id,
        Err(err) => {
            error!("Error during WebSocket handshake: {}", err);
            write_socket_close_frame_flushed(&mut ws, Frame::close(1000, b"Invalid Handshake"))
                .await?;
            return Ok(());
        }
    };

    let metrics = match raft_type {
        #[cfg(feature = "sqlite")]
        RaftType::Sqlite => state.raft_db.raft.metrics(),
        #[cfg(feature = "cache")]
        RaftType::Cache => state.raft_cache.raft.metrics(),
        RaftType::Unknown => return Ok(()),
    };
    let (tx_write, rx_write) = flume::bounded::<WsWriteMsg>(1);
    // The compatibility harness disables only the new marker interception on
    // one voter. This per-connection latch makes its follow-up ordinary query
    // prove that an old handler's serialized SQL error did not tear down or
    // replace the shared API stream.
    #[cfg(feature = "sqlite")]
    let old_watermark_marker_rejected = Arc::new(AtomicBool::new(false));
    #[cfg(feature = "sqlite")]
    let emulate_old_watermark_handler =
        std::env::var_os("HQLITE_TEST_OLD_DB_QUORUM_WATERMARK_HANDLER").is_some();
    #[cfg(feature = "sqlite")]
    let emulate_p3a_watermark_handler =
        std::env::var_os("HQLITE_TEST_P3A_DB_QUORUM_WATERMARK_HANDLER").is_some();
    // TODO splitting needs `unstable-split` feature right now but is about to be stabilized soon
    let (rx, write) = ws.split(tokio::io::split);
    // IMPORTANT: the reader is NOT CANCEL SAFE in v0.8!
    let mut read = FragmentCollectorRead::new(rx);

    let (tx_connection_closed, mut rx_connection_closed) = watch::channel(false);
    let (tx_writer_finished, mut rx_writer_finished) = oneshot::channel();
    let writer_connection_closed = tx_connection_closed.clone();
    let writer_task_status = state.snapshot_transport.clone();
    let handle_write = task::spawn(async move {
        let _task_guard = writer_task_status.owned_async_task();
        api_response_writer(
            write,
            rx_write,
            writer_connection_closed,
            tx_writer_finished,
        )
        .await;
    });

    let (tx_read, rx_read) = flume::bounded(1);
    let (tx_reader_finished, mut rx_reader_finished) = oneshot::channel();
    let reader_connection_closed = tx_connection_closed.clone();
    let reader_task_status = state.snapshot_transport.clone();
    let handle_read = task::spawn(async move {
        let _task_guard = reader_task_status.owned_async_task();
        let outcome = loop {
            let frame = match read
                .read_frame(&mut |frame| async move {
                    // TODO obligated sends should be auto ping / pong / close ? -> verify!
                    debug!(
                        "Received obligated send in stream client: OpCode: {:?}: {:?}",
                        frame.opcode.clone(),
                        frame.payload
                    );
                    Ok::<(), Error>(())
                })
                .await
            {
                Ok(frame) => frame,
                Err(err) => break Err(err.to_string()),
            };
            let req = match frame.opcode {
                OpCode::Close => {
                    debug!("received Close frame in server stream");
                    break Ok(());
                }
                OpCode::Binary => {
                    let bytes = frame.payload.deref();
                    match deserialize::<ApiStreamRequest>(bytes) {
                        Ok(req) => req,
                        Err(err) => break Err(format!("invalid API stream request: {err}")),
                    }
                }
                _ => break Err("non-binary API stream payload".to_owned()),
            };

            tokio::select! {
                _ = rx_connection_closed.changed() => break Ok(()),
                result = tx_read.send_async(req) => {
                    if result.is_err() {
                        break Ok(());
                    }
                }
            }
        };
        reader_connection_closed.send_replace(true);
        let _ = tx_reader_finished.send(outcome);
    });

    // Existing upgraded sockets outlive the HTTP listener. Close them when
    // their Raft group stops, otherwise peers can remain pinned to a stopped
    // leader and receive fatal responses forever instead of reconnecting.
    let raft_stopped = wait_for_raft_stop(metrics);
    tokio::pin!(raft_stopped);
    let mut writer_failed = false;
    loop {
        let req = tokio::select! {
            biased;
            _ = &mut raft_stopped => break,
            writer = &mut rx_writer_finished => {
                match writer {
                    Ok(Ok(())) => error!("API server WebSocket writer exited while connected"),
                    Ok(Err(err)) => error!("API server WebSocket writer failed: {err}"),
                    Err(_) => error!("API server WebSocket writer panicked or was cancelled"),
                }
                writer_failed = true;
                break;
            }
            reader = &mut rx_reader_finished => {
                match reader {
                    Ok(Ok(())) => debug!("API server WebSocket reader exited"),
                    Ok(Err(err)) => error!("API server WebSocket reader failed: {err}"),
                    Err(_) => error!("API server WebSocket reader panicked or was cancelled"),
                }
                break;
            }
            req = rx_read.recv_async() => match req {
                Ok(req) => req,
                Err(_) => break,
            },
        };

        let state = state.clone();
        let tx_write = tx_write.clone();
        let request_task_status = state.snapshot_transport.clone();
        #[cfg(feature = "sqlite")]
        let old_watermark_marker_rejected = old_watermark_marker_rejected.clone();
        task::spawn(async move {
            let _task_guard = request_task_status.owned_async_task();
            let request_id = req.request_id;

            let res = match req.payload {
                #[cfg(feature = "sqlite")]
                ApiStreamRequestPayload::Execute(sql) => {
                    match state
                        .raft_db
                        .raft
                        .client_write(QueryWrite::Execute(sql))
                        .await
                    {
                        Ok(resp) => {
                            let resp: crate::Response = resp.data;
                            let res = match resp {
                                crate::Response::Execute(res) => res.result,
                                _ => unreachable!(),
                            };
                            ApiStreamResponse {
                                request_id,
                                result: ApiStreamResponsePayload::Execute(res),
                            }
                        }
                        Err(err) => ApiStreamResponse {
                            request_id,
                            result: ApiStreamResponsePayload::Execute(Err(Error::from(err))),
                        },
                    }
                }

                #[cfg(feature = "sqlite")]
                ApiStreamRequestPayload::ExecuteReturning(sql) => {
                    match state
                        .raft_db
                        .raft
                        .client_write(QueryWrite::ExecuteReturning(sql))
                        .await
                    {
                        Ok(resp) => {
                            let resp: crate::Response = resp.data;
                            let res = match resp {
                                crate::Response::ExecuteReturning(res) => res.result,
                                _ => unreachable!(),
                            };
                            ApiStreamResponse {
                                request_id,
                                result: ApiStreamResponsePayload::ExecuteReturning(res),
                            }
                        }
                        Err(err) => ApiStreamResponse {
                            request_id,
                            result: ApiStreamResponsePayload::ExecuteReturning(Err(Error::from(
                                err,
                            ))),
                        },
                    }
                }

                #[cfg(feature = "sqlite")]
                ApiStreamRequestPayload::Transaction(queries) => {
                    match state
                        .raft_db
                        .raft
                        .client_write(QueryWrite::Transaction(queries))
                        .await
                    {
                        Ok(resp) => {
                            let resp: crate::Response = resp.data;
                            let res = match resp {
                                crate::Response::Transaction(res) => res,
                                _ => unreachable!(),
                            };
                            ApiStreamResponse {
                                request_id,
                                result: ApiStreamResponsePayload::Transaction(res),
                            }
                        }
                        Err(err) => ApiStreamResponse {
                            request_id,
                            result: ApiStreamResponsePayload::Transaction(Err(Error::from(err))),
                        },
                    }
                }

                #[cfg(feature = "sqlite")]
                ApiStreamRequestPayload::QueryConsistent(Query { sql, params }) => {
                    let is_watermark_marker =
                        sql == DB_QUORUM_WATERMARK_MARKER && params.is_empty();
                    let res = if is_watermark_marker && emulate_p3a_watermark_handler {
                        db_quorum_watermark_local(&state.0).await.map(|watermark| {
                            let mut row = RowOwned::from_db_quorum_watermark(watermark);
                            row.columns
                                .retain(|column| column.name != "local_read_protocol_version");
                            vec![row]
                        })
                    } else if is_watermark_marker && !emulate_old_watermark_handler {
                        db_quorum_watermark_local(&state.0)
                            .await
                            .map(|watermark| vec![RowOwned::from_db_quorum_watermark(watermark)])
                    } else if emulate_old_watermark_handler
                        && sql == DB_QUORUM_WATERMARK_COMPAT_PROBE
                        && params.is_empty()
                        && !old_watermark_marker_rejected.swap(false, Ordering::AcqRel)
                    {
                        Err(Error::Connect(
                            "rolling compatibility probe did not follow a rejected watermark marker on the same API stream"
                                .into(),
                        ))
                    } else {
                        let result = query_consistent_local(
                            &state.raft_db.raft,
                            state.raft_db.log_statements,
                            state.raft_db.read_pool.clone(),
                            sql,
                            params,
                        )
                        .await;
                        if emulate_old_watermark_handler && is_watermark_marker && result.is_err() {
                            old_watermark_marker_rejected.store(true, Ordering::Release);
                        }
                        result
                    };

                    ApiStreamResponse {
                        request_id,
                        result: ApiStreamResponsePayload::QueryConsistent(res),
                    }
                }

                #[cfg(feature = "sqlite")]
                ApiStreamRequestPayload::Batch(sql) => {
                    match state
                        .raft_db
                        .raft
                        .client_write(QueryWrite::Batch(sql))
                        .await
                    {
                        Ok(resp) => {
                            let resp: crate::Response = resp.data;
                            let res = match resp {
                                crate::Response::Batch(res) => res,
                                _ => unreachable!(),
                            };
                            ApiStreamResponse {
                                request_id,
                                result: ApiStreamResponsePayload::Batch(res.result),
                            }
                        }
                        Err(err) => ApiStreamResponse {
                            request_id,
                            result: ApiStreamResponsePayload::Batch(Err(Error::from(err))),
                        },
                    }
                }

                #[cfg(feature = "sqlite")]
                ApiStreamRequestPayload::Migrate(migrations) => {
                    match state
                        .raft_db
                        .raft
                        .client_write(QueryWrite::Migration(migrations))
                        .await
                    {
                        Ok(resp) => {
                            let resp: crate::Response = resp.data;
                            let res = match resp {
                                crate::Response::Migrate(res) => res,
                                _ => unreachable!(),
                            };
                            ApiStreamResponse {
                                request_id,
                                result: ApiStreamResponsePayload::Migrate(res),
                            }
                        }
                        Err(err) => ApiStreamResponse {
                            request_id,
                            result: ApiStreamResponsePayload::Migrate(Err(Error::from(err))),
                        },
                    }
                }

                #[cfg(feature = "backup")]
                ApiStreamRequestPayload::Backup((node_id, ts)) => {
                    match state
                        .raft_db
                        .raft
                        .client_write(QueryWrite::Backup((node_id, ts)))
                        .await
                    {
                        Ok(resp) => {
                            let resp: crate::Response = resp.data;
                            let res = match resp {
                                crate::Response::Backup(res) => res,
                                _ => unreachable!(),
                            };
                            ApiStreamResponse {
                                request_id,
                                result: ApiStreamResponsePayload::Backup(res),
                            }
                        }
                        Err(err) => ApiStreamResponse {
                            request_id,
                            result: ApiStreamResponsePayload::Backup(Err(Error::from(err))),
                        },
                    }
                }

                #[cfg(feature = "sqlite")]
                ApiStreamRequestPayload::Query(Query { sql, params }) => {
                    let res = query_owned_local(
                        state.raft_db.log_statements,
                        state.raft_db.read_pool.clone(),
                        sql,
                        params,
                    )
                    .await;

                    ApiStreamResponse {
                        request_id,
                        result: ApiStreamResponsePayload::Query(res),
                    }
                }

                #[cfg(feature = "cache")]
                ApiStreamRequestPayload::KV(cache_req) => {
                    match state.raft_cache.raft.client_write(cache_req).await {
                        Ok(resp) => {
                            let resp: CacheResponse = resp.data;
                            ApiStreamResponse {
                                request_id,
                                result: ApiStreamResponsePayload::KV(Ok(resp)),
                            }
                        }
                        Err(err) => ApiStreamResponse {
                            request_id,
                            result: ApiStreamResponsePayload::KV(Err(Error::from(err))),
                        },
                    }
                }

                #[cfg(feature = "cache")]
                ApiStreamRequestPayload::KVGet(cache_req) => {
                    let (cache_idx, key) = match cache_req {
                        CacheRequest::Get { cache_idx, key } => (cache_idx, key),
                        _ => unreachable!(),
                    };

                    let (ack, rx) = tokio::sync::oneshot::channel();
                    state
                        .raft_cache
                        .tx_caches
                        .get(cache_idx)
                        .unwrap()
                        .send(CacheRequestHandler::Get((key, ack)))
                        .expect("kv handler to always be running");
                    let value = rx.await.expect("to always get an answer from kv handler");
                    ApiStreamResponse {
                        request_id,
                        result: ApiStreamResponsePayload::KV(Ok(CacheResponse::Value(value))),
                    }
                }

                #[cfg(feature = "dlock")]
                ApiStreamRequestPayload::LockAwait(cache_req) => {
                    let (key, id) = match cache_req {
                        CacheRequest::LockAwait((key, id)) => (key, id),
                        _ => unreachable!(),
                    };

                    let (ack, rx) = tokio::sync::oneshot::channel();
                    state
                        .raft_cache
                        .tx_dlock
                        .send(LockRequest::Await(LockAwaitPayload { key, id, ack }))
                        .expect("kv handler to always be running");
                    let lock_state = rx
                        .await
                        .expect("to always get an answer from the kv handler");

                    ApiStreamResponse {
                        request_id,
                        result: ApiStreamResponsePayload::Lock(lock_state),
                    }
                }

                #[cfg(feature = "listen_notify_local")]
                ApiStreamRequestPayload::Notify(cache_req) => {
                    let (ts, data) = match cache_req {
                        CacheRequest::Notify((ts, data)) => (ts, data),
                        _ => unreachable!(),
                    };

                    match state
                        .raft_cache
                        .raft
                        .client_write(CacheRequest::Notify((ts, data)))
                        .await
                    {
                        Ok(_) => ApiStreamResponse {
                            request_id,
                            result: ApiStreamResponsePayload::Notify(Ok(())),
                        },
                        Err(err) => ApiStreamResponse {
                            request_id,
                            result: ApiStreamResponsePayload::Notify(Err(Error::from(err))),
                        },
                    }
                }
            };

            if let Err(err) = tx_write.send_async(WsWriteMsg::Payload(res)).await {
                error!("Error sending payload to tx_write - exiting: {}", err);
            }
        });
    }

    tx_connection_closed.send_replace(true);
    let mut handle_write = handle_write;
    let writer_finished = if writer_failed {
        false
    } else {
        time::timeout(CLOSE_WRITE_TIMEOUT, async {
            tx_write.send_async(WsWriteMsg::Break).await.ok()?;
            Some((&mut handle_write).await)
        })
        .await
        .ok()
        .flatten()
        .is_some()
    };
    drop(tx_write);
    if !writer_finished {
        handle_write.abort();
        let _ = handle_write.await;
    }
    handle_read.abort();
    let _ = handle_read.await;

    debug!("handle_socket_concurrent exiting");

    Ok(())
}

async fn wait_for_raft_stop(mut metrics: watch::Receiver<openraft::RaftMetrics<u64, Node>>) {
    loop {
        let stopped = {
            let current = metrics.borrow();
            current.running_state.is_err() || current.state == ServerState::Shutdown
        };
        if stopped || metrics.changed().await.is_err() {
            return;
        }
    }
}

async fn write_api_response_frame<S>(
    write: &mut fastwebsockets::WebSocketWrite<S>,
    bytes: &[u8],
) -> Result<(), fastwebsockets::WebSocketError>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    write_frame_flushed(write, Frame::binary(Payload::Borrowed(bytes))).await
}

async fn api_response_writer<S>(
    mut write: fastwebsockets::WebSocketWrite<S>,
    rx_write: flume::Receiver<WsWriteMsg>,
    connection_closed: watch::Sender<bool>,
    finished: oneshot::Sender<Result<(), String>>,
) where
    S: tokio::io::AsyncWrite + Unpin,
{
    let outcome = loop {
        let req = match rx_write.recv_async().await {
            Ok(req) => req,
            Err(_) => break Ok(()),
        };
        match req {
            WsWriteMsg::Payload(resp) => {
                let bytes = serialize_network(&resp);
                if let Err(err) = write_api_response_frame(&mut write, &bytes).await {
                    error!("Error during WebSocket write: {}", err);
                    break Err(err.to_string());
                }
            }
            WsWriteMsg::Break => {
                debug!("handle_socket_concurrent -> server stream break message");
                break Ok(());
            }
        }
    };

    let _ = write_close_frame_flushed(&mut write, Frame::close(1000, b"Invalid Request")).await;
    connection_closed.send_replace(true);
    let _ = finished.send(outcome);
    debug!("handle_socket_concurrent -> server stream exiting");
}
