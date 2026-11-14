use std::{
    cmp::Reverse,
    net::SocketAddr,
    str::FromStr,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    task::Poll,
    time::{Duration, Instant},
};

use crate::{
    bprotocol::{
        self, AnnouncePeer, CompactNodeInfo, ErrorDescription, FindNodeRequest, GetPeersRequest,
        Message, MessageKind, Node, PingRequest, Response,
    },
    peer_store::PeerStore,
    routing_table::{InsertResult, NodeStatus, RoutingTable},
    INACTIVITY_TIMEOUT, REQUERY_INTERVAL, RESPONSE_TIMEOUT,
};
use anyhow::{bail, Context};
use bencode::ByteBufOwned;
use dashmap::DashMap;
use futures::{
    future::BoxFuture, stream::FuturesUnordered, FutureExt, Stream, StreamExt, TryFutureExt,
};
use librqbit_core::retry::RetryPolicy;

use leaky_bucket::RateLimiter;
use librqbit_core::{
    crate_version,
    hash_id::Id20,
    peer_id::generate_azereus_style,
    spawn_utils::{spawn, spawn_with_cancel},
};
use parking_lot::RwLock;

use serde::Serialize;
use tokio::{
    net::UdpSocket,
    sync::mpsc::{channel, error::TrySendError, Receiver, Sender},
};

use tokio_util::sync::CancellationToken;
use tracing::{debug, debug_span, error, error_span, info, trace, warn, Instrument};

#[derive(Debug, Serialize)]
pub struct DhtStats {
    #[serde(serialize_with = "crate::utils::serialize_id20")]
    pub id: Id20,
    pub outstanding_requests: usize,
    pub routing_table_size: usize,
}

struct OutstandingRequest {
    done: tokio::sync::oneshot::Sender<anyhow::Result<ResponseOrError>>,
}

pub struct WorkerSendRequest {
    // If this is set, we are tracking the response in inflight_by_transaction_id
    our_tid: Option<u16>,
    message: Message<ByteBufOwned>,
    addr: SocketAddr,
}

const MAX_PENDING_DHT_SENDS: usize = 256;
const MAX_PENDING_DISCOVERED_PEERS: usize = 256;
const MAX_PENDING_RECURSIVE_NODES: usize = 256;
const MAX_CONCURRENT_RECURSIVE_REQUESTS: usize = 32;
const MAX_PENDING_MAINTENANCE_REQUESTS: usize = 256;
const MAX_CONCURRENT_MAINTENANCE_REQUESTS: usize = 32;

type RecursiveNode = (Option<Id20>, SocketAddr, usize);

fn worker_send_channel() -> (Sender<WorkerSendRequest>, Receiver<WorkerSendRequest>) {
    channel(MAX_PENDING_DHT_SENDS)
}

fn discovered_peer_channel() -> (Sender<SocketAddr>, Receiver<SocketAddr>) {
    channel(MAX_PENDING_DISCOVERED_PEERS)
}

fn recursive_node_channel() -> (Sender<RecursiveNode>, Receiver<RecursiveNode>) {
    channel(MAX_PENDING_RECURSIVE_NODES)
}

fn maintenance_channel<T>() -> (Sender<T>, Receiver<T>) {
    channel(MAX_PENDING_MAINTENANCE_REQUESTS)
}

struct BoundedFuturesUnordered<F> {
    futures: FuturesUnordered<F>,
    limit: usize,
}

impl<F> BoundedFuturesUnordered<F> {
    fn new(limit: usize) -> Self {
        Self {
            futures: FuturesUnordered::new(),
            limit,
        }
    }

    fn try_push(&self, future: F) -> Result<(), F> {
        if self.futures.len() >= self.limit {
            return Err(future);
        }
        self.futures.push(future);
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.futures.is_empty()
    }

    fn is_full(&self) -> bool {
        self.futures.len() >= self.limit
    }

    async fn next(&mut self) -> Option<F::Output>
    where
        F: std::future::Future,
    {
        self.futures.next().await
    }
}

#[derive(Debug, PartialEq, Eq)]
enum RootQueueStatus {
    Empty,
    Queued(usize),
    Saturated,
}

fn root_queue_delay(status: &RootQueueStatus) -> Duration {
    match status {
        RootQueueStatus::Empty => Duration::from_secs(1),
        RootQueueStatus::Queued(n) if *n < 8 => REQUERY_INTERVAL / 8 * (*n as u32),
        RootQueueStatus::Queued(_) | RootQueueStatus::Saturated => REQUERY_INTERVAL,
    }
}

#[derive(Debug)]
struct MaybeUsefulNode {
    id: Id20,
    addr: SocketAddr,
    last_request: Instant,
    last_response: Option<Instant>,
    errors_in_a_row: usize,
    returned_peers: bool,
}

fn make_rate_limiter() -> RateLimiter {
    // TODO: move to configuration, i'm lazy.
    let dht_queries_per_second = std::env::var("DHT_QUERIES_PER_SECOND")
        .map(|v| v.parse().expect("couldn't parse DHT_QUERIES_PER_SECOND"))
        .unwrap_or(250usize);

    let per_100_ms = dht_queries_per_second / 10;

    RateLimiter::builder()
        .initial(per_100_ms)
        .max(dht_queries_per_second)
        .interval(Duration::from_millis(100))
        .fair(false)
        .refill(per_100_ms)
        .build()
}

trait RecursiveRequestCallbacks: Sized + Send + Sync + 'static {
    fn on_request_start(&self, req: &RecursiveRequest<Self>, target_node: Id20, addr: SocketAddr);
    fn on_request_end(
        &self,
        req: &RecursiveRequest<Self>,
        target_node: Id20,
        addr: SocketAddr,
        resp: &anyhow::Result<ResponseOrError>,
    );
}

struct RecursiveRequestCallbacksGetPeers {
    // Id20::from_str("00000fffffffffffffffffffffffffffffffffff").unwrap()
    min_distance_to_announce: Id20,
    announce_port: Option<u16>,
}

impl RecursiveRequestCallbacks for RecursiveRequestCallbacksGetPeers {
    fn on_request_start(&self, _: &RecursiveRequest<Self>, _: Id20, _: SocketAddr) {}

    fn on_request_end(
        &self,
        req: &RecursiveRequest<Self>,
        target_node: Id20,
        addr: SocketAddr,
        resp: &anyhow::Result<ResponseOrError>,
    ) {
        let announce_port = match self.announce_port {
            Some(a) => a,
            None => return,
        };
        let resp = match resp {
            Ok(ResponseOrError::Response(resp)) => resp,
            _ => return,
        };
        let token = match &resp.token {
            Some(token) => token,
            None => return,
        };
        if req.info_hash.distance(&target_node) > self.min_distance_to_announce {
            trace!(
                "not announcing, {:?} is too far from {:?}",
                target_node,
                req.info_hash
            );
            return;
        }
        let (tid, message) = req.dht.create_request(Request::Announce {
            info_hash: req.info_hash,
            token: token.clone(),
            port: announce_port,
        });

        if let Err(error) = req.dht.try_send_worker(WorkerSendRequest {
            our_tid: Some(tid),
            message,
            addr,
        }) {
            debug!(?error, "failed to enqueue DHT announce");
        }
    }
}

struct RecursiveRequestCallbacksFindNodes {}
impl RecursiveRequestCallbacks for RecursiveRequestCallbacksFindNodes {
    fn on_request_start(&self, req: &RecursiveRequest<Self>, target_node: Id20, addr: SocketAddr) {
        let mut rt = req.dht.routing_table.write();
        match rt.add_node(target_node, addr) {
            InsertResult::WasExisting | InsertResult::ReplacedBad(_) | InsertResult::Added => {
                rt.mark_outgoing_request(&target_node);
            }
            InsertResult::Ignored => {}
        }
    }

    fn on_request_end(
        &self,
        req: &RecursiveRequest<Self>,
        target_node: Id20,
        _addr: SocketAddr,
        resp: &anyhow::Result<ResponseOrError>,
    ) {
        let mut table = req.dht.routing_table.write();
        if resp.is_ok() {
            table.mark_response(&target_node);
        } else {
            table.mark_error(&target_node);
        }
    }
}

struct RecursiveRequest<C: RecursiveRequestCallbacks> {
    max_depth: usize,
    useful_nodes_limit: usize,
    info_hash: Id20,
    request: Request,
    dht: Arc<DhtState>,
    useful_nodes: RwLock<Vec<MaybeUsefulNode>>,
    peer_tx: Sender<SocketAddr>,
    node_tx: Sender<RecursiveNode>,
    callbacks: C,
}

pub struct RequestPeersStream {
    rx: Receiver<SocketAddr>,
    cancel_join_handle: tokio::task::JoinHandle<()>,
}

impl RequestPeersStream {
    fn new(dht: Arc<DhtState>, info_hash: Id20, announce_port: Option<u16>) -> Self {
        let (peer_tx, peer_rx) = discovered_peer_channel();
        let (node_tx, node_rx) = recursive_node_channel();
        let rp = Arc::new(RecursiveRequest {
            max_depth: 4,
            info_hash,
            useful_nodes_limit: 256,
            request: Request::GetPeers(info_hash),
            dht,
            useful_nodes: RwLock::new(Vec::new()),
            peer_tx,
            node_tx,
            callbacks: RecursiveRequestCallbacksGetPeers {
                min_distance_to_announce: Id20::from_str(
                    "0000ffffffffffffffffffffffffffffffffffff",
                )
                .unwrap(),
                announce_port,
            },
        });
        let join_handle = rp.request_peers_forever(node_rx);
        Self {
            rx: peer_rx,
            cancel_join_handle: join_handle,
        }
    }
}

impl Drop for RequestPeersStream {
    fn drop(&mut self) {
        self.cancel_join_handle.abort();
    }
}

impl Stream for RequestPeersStream {
    type Item = SocketAddr;

    #[inline(never)]
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl RecursiveRequest<RecursiveRequestCallbacksFindNodes> {
    async fn find_node_for_routing_table(
        dht: Arc<DhtState>,
        target: Id20,
        addrs: impl Iterator<Item = SocketAddr>,
    ) -> anyhow::Result<()> {
        let (node_tx, mut node_rx) = recursive_node_channel();
        let req = RecursiveRequest {
            max_depth: 4,
            info_hash: target,
            request: Request::FindNode(target),
            dht,
            useful_nodes_limit: 32,
            useful_nodes: RwLock::new(Vec::new()),
            peer_tx: discovered_peer_channel().0,
            node_tx,
            callbacks: RecursiveRequestCallbacksFindNodes {},
        };

        let request_one = |id, addr, depth| {
            req.request_one(id, addr, depth)
                .map_err(|e| {
                    debug!("error: {e:#}");
                    e
                })
                .instrument(error_span!(
                    "find_node",
                    target = format!("{target:?}"),
                    addr = addr.to_string()
                ))
        };

        let mut initial_addrs = 0;
        for addr in addrs {
            match req.node_tx.try_send((None, addr, 0)) {
                Ok(()) => initial_addrs += 1,
                Err(TrySendError::Full(_)) => break,
                Err(TrySendError::Closed(_)) => bail!("recursive request queue closed"),
            }
        }

        let mut futs = BoundedFuturesUnordered::new(MAX_CONCURRENT_RECURSIVE_REQUESTS);
        let mut successes = 0;
        let mut errors = 0;

        loop {
            if futs.is_empty() && node_rx.is_empty() {
                break;
            }
            tokio::select! {
                biased;

                r = node_rx.recv(), if !futs.is_full() => {
                    let Some((id, addr, depth)) = r else {
                        break;
                    };
                    assert!(futs.try_push(request_one(id, addr, depth)).is_ok());
                },
                Some(f) = futs.next(), if !futs.is_empty() => {
                    if f.is_ok() {
                        successes += 1;
                    } else {
                        errors += 1;
                    }
                }
            }
        }
        if successes == 0 {
            bail!("no successful lookups, errors = {errors}");
        }
        debug!(
            "finished, successes = {successes}, errors = {errors}, initial_addrs = {initial_addrs}"
        );
        Ok(())
    }
}

impl RecursiveRequest<RecursiveRequestCallbacksGetPeers> {
    fn request_peers_forever(
        self: &Arc<Self>,
        mut node_rx: Receiver<RecursiveNode>,
    ) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        spawn(
            error_span!(parent: None, "get_peers", info_hash = format!("{:?}", self.info_hash)),
            async move {
                let this = &this;
                // Looper adds root nodes to the queue every 60 seconds.
                let looper = {
                    async move {
                        let mut iteration = 0;
                        loop {
                            trace!("iteration {}", iteration);
                            let sleep = match this.get_peers_root() {
                                Ok(status) => root_queue_delay(&status),
                                Err(e) => {
                                    error!("error in get_peers_root(): {e:#}");
                                    return Err::<(), anyhow::Error>(e);
                                }
                            };
                            tokio::time::sleep(sleep).await;
                            iteration += 1;
                        }
                    }
                };
                tokio::pin!(looper);

                let mut futs = BoundedFuturesUnordered::new(MAX_CONCURRENT_RECURSIVE_REQUESTS);
                loop {
                    tokio::select! {
                        addr = node_rx.recv(), if !futs.is_full() => {
                            let (id, addr, depth) = addr.unwrap();
                            assert!(futs.try_push(
                                this.request_one(id, addr, depth)
                                    .map_err(|e| debug!("error: {e:#}"))
                                    .instrument(error_span!("addr", addr=addr.to_string()))
                            ).is_ok());
                        }
                        Some(_) = futs.next(), if !futs.is_empty() => {}
                        r = &mut looper => {
                            return r
                        }
                    }
                }
            },
        )
    }

    fn get_peers_root(&self) -> anyhow::Result<RootQueueStatus> {
        let mut count = 0;
        for (id, addr) in self
            .dht
            .routing_table
            .read()
            .sorted_by_distance_from(self.info_hash)
            .iter()
            .map(|n| (n.id(), n.addr()))
            .take(8)
        {
            match self.node_tx.try_send((Some(id), addr, 0)) {
                Ok(()) => count += 1,
                Err(TrySendError::Full(_)) => return Ok(RootQueueStatus::Saturated),
                Err(TrySendError::Closed(_)) => bail!("recursive request queue closed"),
            }
        }
        if count == 0 {
            Ok(RootQueueStatus::Empty)
        } else {
            Ok(RootQueueStatus::Queued(count))
        }
    }
}

impl<C: RecursiveRequestCallbacks> RecursiveRequest<C> {
    async fn request_one(
        &self,
        id: Option<Id20>,
        addr: SocketAddr,
        depth: usize,
    ) -> anyhow::Result<()> {
        if let Some(id) = id {
            self.callbacks.on_request_start(self, id, addr);
        }

        // Send once: callbacks and traversal must consume the same response.
        let response = self
            .dht
            .request(self.request.clone(), addr)
            .await
            .inspect(|r| {
                self.mark_node_responded(addr, r);
            });
        if let Some(id) = id {
            self.callbacks.on_request_end(self, id, addr, &response);
        }

        let response = match response {
            Ok(ResponseOrError::Response(r)) => r,
            Ok(ResponseOrError::Error(e)) => bail!("error response: {:?}", e),
            Err(e) => {
                self.mark_node_error(addr);
                return Err(e);
            }
        };

        if let Some(nodes) = response.nodes {
            for node in nodes.nodes {
                let addr = SocketAddr::V4(node.addr);
                let should_request = self.should_request_node(node.id, addr, depth);
                trace!(
                    "should_request={}, id={:?}, addr={}, depth={}/{}",
                    should_request,
                    node.id,
                    addr,
                    depth,
                    self.max_depth
                );
                if should_request {
                    match self.node_tx.try_send((Some(node.id), addr, depth + 1)) {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => {
                            trace!("recursive request queue is full, dropping {addr}")
                        }
                        Err(TrySendError::Closed(_)) => bail!("recursive request queue closed"),
                    }
                }
            }
        }
        if let Some(peers) = response.values {
            for peer in peers {
                let addr = SocketAddr::V4(peer.addr);
                match self.peer_tx.try_send(addr) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        trace!(%addr, "discovered peer queue is full, dropping peer")
                    }
                    Err(TrySendError::Closed(_)) => bail!("discovered peer receiver closed"),
                }
            }
        }
        Ok(())
    }

    fn mark_node_error(&self, addr: SocketAddr) -> bool {
        self.useful_nodes
            .write()
            .iter_mut()
            .find(|n| n.addr == addr)
            .map(|n| {
                n.errors_in_a_row += 1;
            })
            .is_some()
    }

    fn mark_node_responded(&self, addr: SocketAddr, response: &ResponseOrError) -> bool {
        self.useful_nodes
            .write()
            .iter_mut()
            .find(|n| n.addr == addr)
            .map(|node| {
                node.last_response = Some(Instant::now());
                node.errors_in_a_row = 0;
                match response {
                    ResponseOrError::Response(r) => {
                        node.returned_peers =
                            r.values.as_ref().map(|c| !c.is_empty()).unwrap_or(false)
                    }
                    ResponseOrError::Error(_) => {
                        node.returned_peers = false;
                    }
                }
            })
            .is_some()
    }

    fn should_request_node(&self, node_id: Id20, addr: SocketAddr, depth: usize) -> bool {
        if depth >= self.max_depth {
            return false;
        }

        let mut closest_nodes = self.useful_nodes.write();

        // If recently requested, ignore
        if let Some(existing) = closest_nodes.iter_mut().find(|n| n.id == node_id) {
            if existing.last_request.elapsed() > Duration::from_secs(60) {
                existing.last_request = Instant::now();
                return true;
            }
            return false;
        }

        closest_nodes.push(MaybeUsefulNode {
            id: node_id,
            addr,
            last_request: Instant::now(),
            last_response: None,
            returned_peers: false,
            errors_in_a_row: 0,
        });

        closest_nodes.sort_by_key(|n| {
            let has_returned_peers_desc = Reverse(n.returned_peers);
            let has_responded_desc = Reverse(n.last_response.is_some() as u8);
            let distance = n.id.distance(&self.info_hash);
            let freshest_response = n
                .last_response
                .map(|r| r.elapsed())
                .unwrap_or(Duration::MAX);
            (
                has_returned_peers_desc,
                has_responded_desc,
                distance,
                freshest_response,
            )
        });
        if closest_nodes.len() > self.useful_nodes_limit {
            let popped = closest_nodes.pop().unwrap();
            if popped.id == node_id {
                return false;
            }
        }
        true
    }
}

pub struct DhtState {
    id: Id20,
    next_transaction_id: AtomicU16,

    // Created requests: (transaction_id, addr) => Requests.
    // If we get a response, it gets removed from here.
    inflight_by_transaction_id: DashMap<(u16, SocketAddr), OutstandingRequest>,

    routing_table: RwLock<RoutingTable>,
    listen_addr: SocketAddr,

    // Sending requests to the worker.
    rate_limiter: RateLimiter,
    // This is to send raw messages
    worker_sender: Sender<WorkerSendRequest>,

    cancellation_token: CancellationToken,

    pub(crate) peer_store: PeerStore,
}

impl DhtState {
    fn new_internal(
        id: Id20,
        sender: Sender<WorkerSendRequest>,
        routing_table: Option<RoutingTable>,
        listen_addr: SocketAddr,
        peer_store: PeerStore,
        cancellation_token: CancellationToken,
    ) -> Self {
        let routing_table = routing_table.unwrap_or_else(|| RoutingTable::new(id, None));
        Self {
            id,
            next_transaction_id: AtomicU16::new(0),
            inflight_by_transaction_id: Default::default(),
            routing_table: RwLock::new(routing_table),
            worker_sender: sender,
            listen_addr,
            rate_limiter: make_rate_limiter(),
            peer_store,
            cancellation_token,
        }
    }

    fn try_send_worker(&self, request: WorkerSendRequest) -> anyhow::Result<()> {
        match self.worker_sender.try_send(request) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                debug!("DHT send queue is full, dropping untracked datagram");
                Ok(())
            }
            Err(TrySendError::Closed(_)) => bail!("DHT worker is closed"),
        }
    }

    async fn request(&self, request: Request, addr: SocketAddr) -> anyhow::Result<ResponseOrError> {
        self.rate_limiter.acquire_one().await;
        let permit = self
            .worker_sender
            .reserve()
            .await
            .context("DHT worker is closed")?;
        let (tid, message) = self.create_request(request);
        let key = (tid, addr);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.inflight_by_transaction_id
            .insert(key, OutstandingRequest { done: tx });
        trace!("sending {message:?}");
        permit.send(WorkerSendRequest {
            our_tid: Some(tid),
            message,
            addr,
        });
        match tokio::time::timeout(RESPONSE_TIMEOUT, rx).await {
            Ok(Ok(r)) => r.map(|r| {
                trace!("received {r:?}");
                r
            }),
            Ok(Err(e)) => {
                self.inflight_by_transaction_id.remove(&key);
                warn!("recv error, did not expect this: {:?}", e);
                Err(e.into())
            }
            Err(_) => {
                self.inflight_by_transaction_id.remove(&key);
                bail!("timeout ({RESPONSE_TIMEOUT:?})")
            }
        }
    }

    fn create_request(&self, request: Request) -> (u16, Message<ByteBufOwned>) {
        let transaction_id = self.next_transaction_id.fetch_add(1, Ordering::Relaxed);
        let transaction_id_buf = [(transaction_id >> 8) as u8, (transaction_id & 0xff) as u8];

        let message = match request {
            Request::GetPeers(info_hash) => Message {
                transaction_id: ByteBufOwned::from(transaction_id_buf.as_ref()),
                version: None,
                ip: None,
                kind: MessageKind::GetPeersRequest(GetPeersRequest {
                    id: self.id,
                    info_hash,
                }),
            },
            Request::FindNode(target) => Message {
                transaction_id: ByteBufOwned::from(transaction_id_buf.as_ref()),
                version: None,
                ip: None,
                kind: MessageKind::FindNodeRequest(FindNodeRequest {
                    id: self.id,
                    target,
                }),
            },
            Request::Ping => Message {
                transaction_id: ByteBufOwned::from(transaction_id_buf.as_ref()),
                version: None,
                ip: None,
                kind: MessageKind::PingRequest(PingRequest { id: self.id }),
            },
            Request::Announce {
                info_hash,
                token,
                port,
            } => Message {
                kind: MessageKind::AnnouncePeer(AnnouncePeer {
                    id: self.id,
                    implied_port: 0,
                    info_hash,
                    port,
                    token,
                }),
                transaction_id: ByteBufOwned::from(transaction_id_buf.as_ref()),
                version: None,
                ip: None,
            },
        };
        (transaction_id, message)
    }

    fn on_received_message(
        self: &Arc<Self>,
        msg: Message<ByteBufOwned>,
        addr: SocketAddr,
    ) -> anyhow::Result<()> {
        let generate_compact_nodes = |target| {
            let nodes = self
                .routing_table
                .read()
                .sorted_by_distance_from(target)
                .into_iter()
                .filter_map(|r| {
                    Some(Node {
                        id: r.id(),
                        addr: match r.addr() {
                            SocketAddr::V4(v4) => v4,
                            SocketAddr::V6(_) => return None,
                        },
                    })
                })
                .take(8)
                .collect::<Vec<_>>();
            CompactNodeInfo { nodes }
        };

        match &msg.kind {
            // If it's a response to a request we made, find the request task, notify it with the response,
            // and let it handle it.
            MessageKind::Error(_) | MessageKind::Response(_) => {
                let tid = msg.get_our_transaction_id().context("bad transaction id")?;
                let request = match self
                    .inflight_by_transaction_id
                    .remove(&(tid, addr))
                    .map(|(_, v)| v)
                {
                    Some(req) => req,
                    None => {
                        bail!("outstanding request not found. Message: {:?}", msg)
                    }
                };

                let response_or_error = match msg.kind {
                    MessageKind::Error(e) => ResponseOrError::Error(e),
                    MessageKind::Response(r) => ResponseOrError::Response(r),
                    _ => unreachable!(),
                };
                match request.done.send(Ok(response_or_error)) {
                    Ok(_) => {}
                    Err(e) => {
                        debug!(
                            "recieved response, but the receiver task is closed: {:?}",
                            e
                        );
                    }
                }
                return Ok(());
            }
            _ => {}
        };

        trace!("received query from {addr}: {msg:?}");

        match &msg.kind {
            // Otherwise, respond to a query.
            MessageKind::PingRequest(req) => {
                let message = Message {
                    transaction_id: msg.transaction_id,
                    version: None,
                    ip: None,
                    kind: MessageKind::Response(bprotocol::Response {
                        id: self.id,
                        ..Default::default()
                    }),
                };
                self.routing_table.write().mark_last_query(&req.id);
                self.try_send_worker(WorkerSendRequest {
                    our_tid: None,
                    message,
                    addr,
                })?;
                Ok(())
            }
            MessageKind::AnnouncePeer(ann) => {
                self.routing_table.write().mark_last_query(&ann.id);
                let added = self.peer_store.store_peer(ann, addr);
                trace!("{addr}: added_peer={added}, announce={ann:?}");
                let message = Message {
                    transaction_id: msg.transaction_id,
                    version: None,
                    ip: None,
                    kind: MessageKind::Response(bprotocol::Response {
                        id: self.id,
                        ..Default::default()
                    }),
                };
                self.try_send_worker(WorkerSendRequest {
                    our_tid: None,
                    message,
                    addr,
                })?;
                Ok(())
            }
            MessageKind::GetPeersRequest(req) => {
                let compact_node_info = generate_compact_nodes(req.info_hash);
                let compact_peer_info = self.peer_store.get_for_info_hash(req.info_hash);
                self.routing_table.write().mark_last_query(&req.id);
                let message = Message {
                    transaction_id: msg.transaction_id,
                    version: None,
                    ip: None,
                    kind: MessageKind::Response(bprotocol::Response {
                        id: self.id,
                        nodes: Some(compact_node_info),
                        values: Some(compact_peer_info),
                        token: Some(ByteBufOwned::from(
                            &self.peer_store.gen_token_for(req.id, addr)[..],
                        )),
                    }),
                };
                self.try_send_worker(WorkerSendRequest {
                    our_tid: None,
                    message,
                    addr,
                })?;
                Ok(())
            }
            MessageKind::FindNodeRequest(req) => {
                let compact_node_info = generate_compact_nodes(req.target);
                self.routing_table.write().mark_last_query(&req.id);
                let message = Message {
                    transaction_id: msg.transaction_id,
                    version: None,
                    ip: None,
                    kind: MessageKind::Response(bprotocol::Response {
                        id: self.id,
                        nodes: Some(compact_node_info),
                        ..Default::default()
                    }),
                };
                self.try_send_worker(WorkerSendRequest {
                    our_tid: None,
                    message,
                    addr,
                })?;
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    pub fn get_stats(&self) -> DhtStats {
        DhtStats {
            id: self.id,
            outstanding_requests: self.inflight_by_transaction_id.len(),
            routing_table_size: self.routing_table.read().len(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Request {
    GetPeers(Id20),
    FindNode(Id20),
    Announce {
        info_hash: Id20,
        token: ByteBufOwned,
        port: u16,
    },
    Ping,
}

enum ResponseOrError {
    Response(Response<ByteBufOwned>),
    Error(ErrorDescription<ByteBufOwned>),
}

impl core::fmt::Debug for ResponseOrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Response(r) => write!(f, "{r:?}"),
            Self::Error(e) => write!(f, "{e:?}"),
        }
    }
}

struct DhtWorker {
    socket: UdpSocket,
    dht: Arc<DhtState>,
}

impl DhtWorker {
    fn on_send_error(&self, tid: u16, addr: SocketAddr, err: anyhow::Error) {
        if let Some((_, OutstandingRequest { done })) =
            self.dht.inflight_by_transaction_id.remove(&(tid, addr))
        {
            let _ = done.send(Err(err)).is_err();
        };
    }

    async fn bootstrap_hostname(&self, hostname: &str) -> anyhow::Result<()> {
        let addrs = tokio::net::lookup_host(hostname)
            .await
            .with_context(|| format!("error looking up {}", hostname))?;
        RecursiveRequest::find_node_for_routing_table(self.dht.clone(), self.dht.id, addrs).await
    }

    async fn bootstrap_hostname_with_backoff(&self, addr: &str) -> anyhow::Result<()> {
        let mut backoff = RetryPolicy::bootstrap();

        loop {
            let backoff = match self
                .bootstrap_hostname(addr)
                .instrument(error_span!("bootstrap", hostname = addr))
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    warn!("error: {}", e);
                    backoff.next_backoff()
                }
            };
            if let Some(backoff) = backoff {
                tokio::time::sleep(backoff).await;
                continue;
            }
            bail!("bootstrap failed")
        }
    }

    async fn bootstrap(&self, bootstrap_addrs: &[String]) -> anyhow::Result<()> {
        let mut futs = FuturesUnordered::new();
        for addr in bootstrap_addrs {
            futs.push(self.bootstrap_hostname_with_backoff(addr));
        }
        let mut successes = 0;
        while let Some(resp) = futs.next().await {
            if resp.is_ok() {
                successes += 1
            }
        }
        if successes == 0 {
            bail!("bootstrapping failed")
        }
        Ok(())
    }

    async fn bucket_refresher(&self) -> anyhow::Result<()> {
        let (tx, mut rx) = maintenance_channel();

        let mut futs = BoundedFuturesUnordered::new(MAX_CONCURRENT_MAINTENANCE_REQUESTS);
        let filler = async {
            let mut interval = tokio::time::interval(INACTIVITY_TIMEOUT);
            interval.tick().await;
            let mut iteration = 0;
            loop {
                interval.tick().await;
                let mut found = 0;
                for bucket in self.dht.routing_table.read().iter_buckets() {
                    if bucket.leaf.last_refreshed.elapsed() < INACTIVITY_TIMEOUT {
                        continue;
                    }
                    let random_id = bucket.random_within();
                    if tx.try_send(random_id).is_err() {
                        break;
                    }
                    found += 1;
                }
                trace!("iteration {}, refreshing {} buckets", iteration, found);
                iteration += 1;
            }
        };

        tokio::pin!(filler);

        loop {
            tokio::select! {
                _ = &mut filler => {},
                random_id = rx.recv(), if !futs.is_full() => {
                    let random_id = random_id.unwrap();
                    let addrs = self
                        .dht
                        .routing_table
                        .read()
                        .sorted_by_distance_from(random_id)
                        .iter()
                        .map(|n| n.addr())
                        .take(8).collect::<Vec<_>>();
                    assert!(futs.try_push(
                        RecursiveRequest::find_node_for_routing_table(
                            self.dht.clone(), random_id, addrs.into_iter()
                        ).instrument(error_span!("refresh_bucket"))
                    ).is_ok());
                },
                _ = futs.next(), if !futs.is_empty() => {},
            }
        }
    }

    async fn pinger(&self) -> anyhow::Result<()> {
        let mut futs = BoundedFuturesUnordered::new(MAX_CONCURRENT_MAINTENANCE_REQUESTS);
        let mut interval = tokio::time::interval(INACTIVITY_TIMEOUT / 4);
        let (tx, mut rx) = maintenance_channel();
        let looper = async {
            let mut iteration = 0;
            loop {
                interval.tick().await;
                let mut found = 0;
                let now = Instant::now();
                for node in self.dht.routing_table.read().iter() {
                    if matches!(
                        node.status(now),
                        NodeStatus::Questionable | NodeStatus::Unknown
                    ) {
                        if tx.try_send((node.id(), node.addr())).is_err() {
                            break;
                        }
                        found += 1;
                    }
                }
                trace!("iteration {}, pinging {} nodes", iteration, found);
                iteration += 1;
            }
        };

        tokio::pin!(looper);

        loop {
            tokio::select! {
                _ = &mut looper => {},
                r = rx.recv(), if !futs.is_full() => {
                    let (id, addr) = r.unwrap();
                    assert!(futs.try_push(async move {
                        self.dht.routing_table.write().mark_outgoing_request(&id);
                        match self.dht.request(Request::Ping, addr).await {
                            Ok(_) => {
                                self.dht.routing_table.write().mark_response(&id);
                            },
                            Err(e) => {
                                self.dht.routing_table.write().mark_error(&id);
                                debug!("error: {e:#}");
                            }
                        }
                    }.instrument(error_span!("ping", addr=addr.to_string()))).is_ok());
                },
                _ = futs.next(), if !futs.is_empty() => {},
            }
        }
    }

    async fn framer(
        &self,
        socket: &UdpSocket,
        mut input_rx: Receiver<WorkerSendRequest>,
        output_tx: Sender<(Message<ByteBufOwned>, SocketAddr)>,
    ) -> anyhow::Result<()> {
        let writer = async {
            let mut buf = Vec::new();
            while let Some(WorkerSendRequest {
                our_tid,
                message,
                addr,
            }) = input_rx.recv().await
            {
                if our_tid.is_none() {
                    trace!("{}: sending {:?}", addr, &message);
                }
                buf.clear();
                bprotocol::serialize_message(
                    &mut buf,
                    message.transaction_id,
                    message.version,
                    message.ip,
                    message.kind,
                )
                .unwrap();
                if let Err(e) = socket.send_to(&buf, addr).await {
                    debug!("error sending to {addr}: {e:#}");
                    if let Some(tid) = our_tid {
                        self.on_send_error(tid, addr, e.into());
                    }
                }
            }
            Err::<(), _>(anyhow::anyhow!(
                "DHT UDP socket writer over, nowhere to read messages from"
            ))
        };
        let reader = async {
            let mut buf = vec![0u8; 16384];
            loop {
                let (size, addr) = socket
                    .recv_from(&mut buf)
                    .await
                    .context("error reading from UDP socket")?;
                match bprotocol::deserialize_message::<ByteBufOwned>(&buf[..size]) {
                    Ok(msg) => match output_tx.send((msg, addr)).await {
                        Ok(_) => {}
                        Err(_) => break,
                    },
                    Err(e) => debug!("{}: error deserializing incoming message: {}", addr, e),
                }
            }
            Err::<(), _>(anyhow::anyhow!(
                "DHT UDP socket reader over, nowhere to send responses to"
            ))
        };
        let result = tokio::select! {
            err = writer => err,
            err = reader => err,
        };
        result.context("DHT UDP framer closed")
    }

    async fn start(
        self,
        in_rx: Receiver<WorkerSendRequest>,
        bootstrap_addrs: &[String],
    ) -> anyhow::Result<()> {
        let (out_tx, mut out_rx) = channel(1);
        let framer = self
            .framer(&self.socket, in_rx, out_tx)
            .instrument(debug_span!("dht_framer"));

        let bootstrap = self.bootstrap(bootstrap_addrs);
        let mut bootstrap_done = false;

        let response_reader = {
            let this = &self;
            async move {
                while let Some((response, addr)) = out_rx.recv().await {
                    if let Err(e) = this.dht.on_received_message(response, addr) {
                        debug!("error in on_response, addr={:?}: {}", addr, e)
                    }
                }
                Err::<(), _>(anyhow::anyhow!(
                    "closed response reader, nowhere to send results to, DHT closed"
                ))
            }
        }
        .instrument(debug_span!("dht_responese_reader"));

        let pinger = self.pinger().instrument(error_span!("pinger"));
        let bucket_refresher = self
            .bucket_refresher()
            .instrument(error_span!("bucket_refresher"));

        tokio::pin!(framer);
        tokio::pin!(bootstrap);
        tokio::pin!(response_reader);
        tokio::pin!(pinger);
        tokio::pin!(bucket_refresher);

        loop {
            tokio::select! {
                err = &mut framer => {
                    anyhow::bail!("framer quit: {:?}", err)
                },
                result = &mut bootstrap, if !bootstrap_done => {
                    bootstrap_done = true;
                    result?;
                },
                err = &mut pinger => {
                    anyhow::bail!("pinger quit: {:?}", err)
                },
                err = &mut bucket_refresher => {
                    anyhow::bail!("bucket_refresher quit: {:?}", err)
                },
                err = &mut response_reader => {anyhow::bail!("response reader quit: {:?}", err)}
            }
        }
    }
}

#[derive(Default)]
pub struct DhtConfig {
    pub peer_id: Option<Id20>,
    pub bootstrap_addrs: Option<Vec<String>>,
    pub routing_table: Option<RoutingTable>,
    pub listen_addr: Option<SocketAddr>,
    pub peer_store: Option<PeerStore>,
    pub cancellation_token: Option<CancellationToken>,
}

impl DhtState {
    pub async fn new() -> anyhow::Result<Arc<Self>> {
        Self::with_config(DhtConfig::default()).await
    }
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    #[inline(never)]
    pub fn with_config(mut config: DhtConfig) -> BoxFuture<'static, anyhow::Result<Arc<Self>>> {
        async move {
            let socket = match config.listen_addr {
                Some(addr) => UdpSocket::bind(addr)
                    .await
                    .with_context(|| format!("error binding socket, address {addr}")),
                None => UdpSocket::bind("0.0.0.0:0")
                    .await
                    .context("error binding socket, address 0.0.0.0:0"),
            }?;

            let listen_addr = socket
                .local_addr()
                .context("cannot determine UDP listen addr")?;
            info!("DHT listening on {:?}", listen_addr);

            let peer_id = config
                .peer_id
                .unwrap_or_else(|| (generate_azereus_style(*b"rQ", crate_version!())));
            info!("starting up DHT with peer id {:?}", peer_id);
            let bootstrap_addrs = config
                .bootstrap_addrs
                .unwrap_or_else(|| crate::DHT_BOOTSTRAP.iter().map(|v| v.to_string()).collect());

            let token = config.cancellation_token.take().unwrap_or_default();

            let (in_tx, in_rx) = worker_send_channel();
            let state = Arc::new(Self::new_internal(
                peer_id,
                in_tx,
                config.routing_table,
                listen_addr,
                config.peer_store.unwrap_or_else(|| PeerStore::new(peer_id)),
                token,
            ));

            spawn_with_cancel(error_span!("dht"), state.cancellation_token.clone(), {
                let state = state.clone();
                async move {
                    let worker = DhtWorker { socket, dht: state };
                    worker.start(in_rx, &bootstrap_addrs).await
                }
            });
            Ok(state)
        }
        .boxed()
    }

    pub fn get_peers(
        self: &Arc<Self>,
        info_hash: Id20,
        announce_port: Option<u16>,
    ) -> RequestPeersStream {
        RequestPeersStream::new(self.clone(), info_hash, announce_port)
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    pub fn stats(&self) -> DhtStats {
        self.get_stats()
    }

    pub fn with_routing_table<R, F: FnOnce(&RoutingTable) -> R>(&self, f: F) -> R {
        f(&self.routing_table.read())
    }

    pub fn clone_routing_table(&self) -> RoutingTable {
        self.routing_table.read().clone()
    }
}

#[cfg(test)]
mod queue_budget_tests {
    use super::*;

    fn worker_request() -> WorkerSendRequest {
        WorkerSendRequest {
            our_tid: None,
            message: Message {
                transaction_id: ByteBufOwned::from(&[0, 1][..]),
                version: None,
                ip: None,
                kind: MessageKind::PingRequest(PingRequest {
                    id: Id20::new([0; 20]),
                }),
            },
            addr: SocketAddr::from(([127, 0, 0, 1], 6881)),
        }
    }

    #[tokio::test]
    async fn recursive_lookup_dispatches_requests_without_debug_assertions() {
        let (worker_tx, mut worker_rx) = worker_send_channel();
        let id = Id20::new([1; 20]);
        let addr = SocketAddr::from(([127, 0, 0, 1], 6881));
        let dht = Arc::new(DhtState::new_internal(
            id,
            worker_tx,
            None,
            addr,
            PeerStore::new(id),
            CancellationToken::new(),
        ));
        let lookup = RecursiveRequest::find_node_for_routing_table(dht, id, std::iter::once(addr));
        tokio::pin!(lookup);
        tokio::select! {
            request = worker_rx.recv() => {
                let request = request.expect("lookup must dispatch a request");
                assert_eq!(request.addr, addr);
                assert!(matches!(request.message.kind, MessageKind::FindNodeRequest(_)));
            }
            result = &mut lookup => panic!("lookup exited before dispatch: {result:?}"),
            _ = tokio::time::sleep(Duration::from_secs(1)) => panic!("lookup did not dispatch"),
        }
    }

    #[tokio::test]
    async fn queue_budgets_close_at_exact_boundaries() {
        let (worker_tx, mut worker_rx) = worker_send_channel();
        for _ in 0..MAX_PENDING_DHT_SENDS {
            worker_tx.try_send(worker_request()).unwrap();
        }
        assert!(matches!(
            worker_tx.try_send(worker_request()),
            Err(TrySendError::Full(_))
        ));
        assert!(worker_rx.recv().await.is_some());
        worker_tx.try_send(worker_request()).unwrap();

        let addr = SocketAddr::from(([127, 0, 0, 1], 6881));
        let (peer_tx, mut peer_rx) = discovered_peer_channel();
        for _ in 0..MAX_PENDING_DISCOVERED_PEERS {
            peer_tx.try_send(addr).unwrap();
        }
        assert!(matches!(peer_tx.try_send(addr), Err(TrySendError::Full(_))));
        assert_eq!(peer_rx.recv().await, Some(addr));
        peer_tx.try_send(addr).unwrap();

        let (node_tx, mut node_rx) = recursive_node_channel();
        for _ in 0..MAX_PENDING_RECURSIVE_NODES {
            node_tx.try_send((None, addr, 0)).unwrap();
        }
        assert!(matches!(
            node_tx.try_send((None, addr, 0)),
            Err(TrySendError::Full(_))
        ));
        assert!(node_rx.recv().await.is_some());
        node_tx.try_send((None, addr, 0)).unwrap();

        let recursive = BoundedFuturesUnordered::new(MAX_CONCURRENT_RECURSIVE_REQUESTS);
        for _ in 0..MAX_CONCURRENT_RECURSIVE_REQUESTS {
            assert!(recursive.try_push(std::future::pending::<()>()).is_ok());
        }
        assert!(recursive.try_push(std::future::pending::<()>()).is_err());

        let (maintenance_tx, mut maintenance_rx) = maintenance_channel();
        for _ in 0..MAX_PENDING_MAINTENANCE_REQUESTS {
            maintenance_tx.try_send(Id20::new([0; 20])).unwrap();
        }
        assert!(matches!(
            maintenance_tx.try_send(Id20::new([0; 20])),
            Err(TrySendError::Full(_))
        ));
        assert!(maintenance_rx.recv().await.is_some());
        maintenance_tx.try_send(Id20::new([0; 20])).unwrap();

        let maintenance = BoundedFuturesUnordered::new(MAX_CONCURRENT_MAINTENANCE_REQUESTS);
        for _ in 0..MAX_CONCURRENT_MAINTENANCE_REQUESTS {
            assert!(maintenance.try_push(std::future::pending::<()>()).is_ok());
        }
        assert!(maintenance.try_push(std::future::pending::<()>()).is_err());

        assert_eq!(
            root_queue_delay(&RootQueueStatus::Saturated),
            REQUERY_INTERVAL
        );
    }
}
