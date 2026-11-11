//! Multi-node cluster tests (CLUSTERING.md §11): several full nodes in one
//! process sharing a tempdir "shared volume" and real loopback HTTP, with
//! nzbd-nserv as the provider. Lease intervals are time-compressed.

use nzbd_cluster::{ClusterConfig, ClusterRuntime, ControlPeer};
use nzbd_engine::Tuning;
use nzbd_nserv::{build_post, prng_bytes, NservBuilder};
use nzbd_types::{CertLevel, ServerDef, ServerId, TlsMode};
use sha2::Digest;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const SECRET: &str = "test-cluster-secret";

/// Probe for an external tool; on a miss the calling test self-skips with a
/// notice. `NZBD_REQUIRE_TOOLS` (set in CI) turns the miss into a loud
/// failure so CI can never silently lose coverage.
fn require_tool(tool: &str) -> bool {
    let found = std::process::Command::new(tool)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok();
    if found {
        return true;
    }
    if std::env::var_os("NZBD_REQUIRE_TOOLS").is_some() {
        panic!("`{tool}` is required because NZBD_REQUIRE_TOOLS is set — install it in this environment");
    }
    eprintln!(
        "SKIPPED: `{tool}` not installed — `brew install par2 p7zip` / `apt-get install par2 p7zip-full` for full local coverage"
    );
    false
}

fn server_def(port: u16, connections: u16) -> ServerDef {
    ServerDef {
        id: ServerId(1),
        name: "shared-account".into(), // same name on every node = one account
        host: "127.0.0.1".into(),
        port,
        tls: TlsMode::None,
        username: None,
        password: None,
        active: true,
        tier: 0,
        group: 0,
        fill: false,
        max_connections: connections,
        pipeline_depth: 2,
        retention_days: 0,
        cert_verification: CertLevel::Strict,
    }
}

struct NodeOpts {
    coordinator: bool,
    priority: u32,
    download: bool,
    max_download_jobs: u32,
    /// PP executor (C2). Slots default to 1 when enabled.
    post_process: bool,
    min_free_disk_bytes: u64,
}

struct Node {
    #[allow(dead_code)] // debugging aid
    name: String,
    url: String,
    runtime: ClusterRuntime,
    serve_cancel: CancellationToken,
    serve_task: JoinHandle<()>,
}

#[derive(Clone)]
struct TestControl {
    id: u64,
    http_bind: String,
    raft_bind: String,
    api_bind: String,
    peers: Vec<ControlPeer>,
    reservations: Arc<Mutex<HashMap<String, std::net::TcpListener>>>,
}

fn control_topology(count: u64) -> Vec<TestControl> {
    let mut reservations = HashMap::new();
    let addresses: Vec<_> = (0..count)
        .map(|_| {
            // Reserve the public API listeners for every node up front too.
            // Otherwise a later node's :0 bind can steal an earlier node's
            // Raft port while that node is awaiting database startup.
            let http = reserve_bind(&mut reservations);
            let raft = reserve_bind(&mut reservations);
            let api = reserve_bind(&mut reservations);
            (http, raft, api)
        })
        .collect();
    let reservations = Arc::new(Mutex::new(reservations));
    let peers: Vec<_> = addresses
        .iter()
        .enumerate()
        .map(|(index, (_, raft_addr, api_addr))| ControlPeer {
            id: index as u64 + 1,
            raft_addr: raft_addr.clone(),
            api_addr: api_addr.clone(),
        })
        .collect();
    addresses
        .into_iter()
        .enumerate()
        .map(|(index, (http_bind, raft_bind, api_bind))| TestControl {
            id: index as u64 + 1,
            http_bind,
            raft_bind,
            api_bind,
            peers: peers.clone(),
            reservations: reservations.clone(),
        })
        .collect()
}

async fn start_node(
    shared: &Path,
    name: &str,
    opts: NodeOpts,
    nserv_port: u16,
    connections: u16,
) -> Node {
    start_node_with_auth(
        shared,
        name,
        opts,
        nserv_port,
        connections,
        nzbd_api::AuthConfig::default(),
    )
    .await
}

async fn start_node_with_auth(
    shared: &Path,
    name: &str,
    opts: NodeOpts,
    nserv_port: u16,
    connections: u16,
    auth: nzbd_api::AuthConfig,
) -> Node {
    start_node_with_control(shared, name, opts, nserv_port, connections, auth, None).await
}

#[allow(clippy::too_many_arguments)]
async fn start_node_with_control(
    shared: &Path,
    name: &str,
    opts: NodeOpts,
    nserv_port: u16,
    connections: u16,
    auth: nzbd_api::AuthConfig,
    control: Option<TestControl>,
) -> Node {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let control = control.unwrap_or_else(|| control_topology(1).remove(0));
    let listener = control
        .reservations
        .lock()
        .unwrap()
        .remove(&control.http_bind)
        .unwrap_or_else(|| {
            // A stopped node may be restarted with the same TestControl. Its
            // original listener has been released, so reacquire that exact
            // address rather than choosing :0 and reintroducing port races.
            std::net::TcpListener::bind(&control.http_bind)
                .expect("stopped node's public API address is available for restart")
        });
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    let url = listener.local_addr().unwrap().to_string();
    // Keep every planned Raft/API port bound until its owning node is ready
    // to bind. Otherwise the HTTP listener above may receive a just-released
    // control port, or another node may claim it before Raft starts.
    {
        let mut reservations = control.reservations.lock().unwrap();
        reservations.remove(&control.raft_bind);
        reservations.remove(&control.api_bind);
    }
    let cfg = ClusterConfig {
        cluster_id: "e2e".into(),
        node_name: name.to_string(),
        shared_dir: shared.to_path_buf(),
        advertise_url: format!("http://{url}"),
        secret: SECRET.to_string(),
        coordinator: opts.coordinator,
        priority: opts.priority,
        download: opts.download,
        max_download_jobs: opts.max_download_jobs,
        post_process: opts.post_process,
        pp_slots: 1,
        lease_interval: Duration::from_millis(150),
        takeover_after: Duration::from_millis(900),
        worker_ttl: Duration::from_millis(1800),
        control_dir: shared.join(format!("control-{name}")),
        control_node_id: control.id,
        control_raft_bind: control.raft_bind,
        control_api_bind: control.api_bind,
        control_peers: control.peers,
        download_weight: 1,
        pp_weight: 1,
        disk_guard_roots: vec![nzbd_engine::volumes::DiskGuardRoot {
            label: "downloads".into(),
            path: shared.join("complete"),
        }],
        torrent_payload_roots: Vec::new(),
    };
    let tuning = Tuning {
        retry_interval: Duration::from_millis(400),
        connect_timeout: Duration::from_secs(5),
        article_timeout: Duration::from_secs(10),
        idle_hold: Duration::from_secs(1),
        min_free_disk_bytes: opts.min_free_disk_bytes,
        ..Tuning::default()
    };
    let pp = if opts.post_process {
        let local = shared.join(format!("local-{name}"));
        std::fs::create_dir_all(&local).unwrap();
        let jsonl = shared.join(".nzbd-cluster/history");
        std::fs::create_dir_all(&jsonl).unwrap();
        Some(nzbd_cluster::PpSetup {
            post: nzbd_post::manager::PostConfig {
                sevenzip_cmd: std::fs::read_to_string(shared.join(format!("test-sevenzip-{name}")))
                    .map(|value| value.trim().to_owned())
                    .unwrap_or_else(|_| "7z".into()),
                ..Default::default()
            },
            history: std::sync::Arc::new(
                nzbd_state::history::HistoryDb::open_tagged(
                    &local.join("history.sqlite"),
                    Some(&jsonl),
                    Some(name),
                )
                .unwrap(),
            ),
        })
    } else {
        None
    };
    let runtime = ClusterRuntime::start(
        cfg,
        vec![server_def(nserv_port, connections)],
        tuning,
        shared.join("complete"),
        None,
        None,
        pp,
    )
    .await
    .expect("cluster start");

    let app = runtime.router_with_auth("26.2", vec![], auth);
    let serve_cancel = CancellationToken::new();
    let sc = serve_cancel.clone();
    let serve_task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { sc.cancelled().await })
            .await
            .ok();
    });

    Node {
        name: name.to_string(),
        url,
        runtime,
        serve_cancel,
        serve_task,
    }
}

fn reserve_bind(reservations: &mut HashMap<String, std::net::TcpListener>) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    reservations.insert(address.clone(), listener);
    address
}

impl Node {
    /// Stop the node (serving + cluster tasks + engine flush). From the
    /// rest of the cluster's perspective this is a death: renewals,
    /// heartbeats and its API all stop.
    async fn kill(self) {
        self.serve_cancel.cancel();
        tokio::time::timeout(Duration::from_secs(60), self.runtime.shutdown())
            .await
            .expect("cluster shutdown must join even after quorum loss");
        self.serve_task.abort();
        let _ = self.serve_task.await;
    }
}

fn http(addr: &str, method: &str, path: &str, body: &[u8]) -> (u16, String) {
    http_with_headers(addr, method, path, body, &[])
}

fn http_with_headers(
    addr: &str,
    method: &str,
    path: &str,
    body: &[u8],
    headers: &[(&str, &str)],
) -> (u16, String) {
    let mut sock = TcpStream::connect(addr).expect("connect");
    sock.set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let extra: String = headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(req.as_bytes()).unwrap();
    sock.write_all(body).unwrap();
    let mut resp = Vec::new();
    sock.read_to_end(&mut resp).unwrap();
    let text = String::from_utf8_lossy(&resp).into_owned();
    let status = text.split_whitespace().nth(1).unwrap().parse().unwrap();
    let payload = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.trim().to_string())
        .unwrap_or_default();
    (status, payload)
}

fn get_json(addr: &str, path: &str) -> serde_json::Value {
    let (code, body) = http(addr, "GET", path, b"");
    assert_eq!(code, 200, "{path}: {body}");
    serde_json::from_str(&body).unwrap()
}

async fn wait_for<F: Fn() -> bool>(what: &str, secs: u64, f: F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Retry a request that a node may legitimately answer with the documented
/// election-gap 503 (`proxy.rs`: "leadership is changing" / "no leader elected
/// yet"). The proxy rejects *before* forwarding, so a 503 means the leader
/// never saw the request and re-sending cannot duplicate it.
///
/// The eventual `201` is still required: any other status fails immediately,
/// and a 503 that never clears fails on the deadline. Accepting "201 or 503"
/// instead would let a genuine proxy regression pass silently.
async fn post_until_created<F: FnMut() -> (u16, String)>(
    what: &str,
    secs: u64,
    mut send: F,
) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let (code, body) = send();
        if code == 201 {
            return body;
        }
        assert_eq!(code, 503, "{what}: unexpected status {code}: {body}");
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}: still 503 after {secs}s: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `POST /api/v1/jobs` against any node — leader or proxying non-leader —
/// riding out an election gap. Returns the 201 body.
async fn add_job(addr: &str, name: &str, nzb: &[u8]) -> String {
    post_until_created(&format!("add job `{name}` via {addr}"), 15, || {
        http(addr, "POST", &format!("/api/v1/jobs?name={name}"), nzb)
    })
    .await
}

fn journaled_segments(shared: &Path) -> Vec<u32> {
    nzbd_state::JobJournals::replay_all(&shared.join(".nzbd-cluster"))
        .unwrap_or_default()
        .into_iter()
        .map(|r| r.segment_number)
        .collect()
}

fn manifest_paths(root: &Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(root) else {
        return found;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(manifest_paths(&path));
        } else if entry.file_name() == "manifest.json" {
            found.push(path);
        }
    }
    found
}

// ---------------------------------------------------------------------------

// The add helper's retry contract, pinned without depending on real election
// timing: an injected response sequence stands in for the cluster.

#[tokio::test]
async fn add_retries_the_election_gap_503_and_still_requires_a_201() {
    let mut calls = 0;
    let body = post_until_created("injected add", 15, || {
        calls += 1;
        match calls {
            1 => (
                503,
                r#"{"error":"no leader elected yet; retry"}"#.to_string(),
            ),
            2 => (
                503,
                r#"{"error":"leadership is changing; retry"}"#.to_string(),
            ),
            _ => (201, r#"{"id":7}"#.to_string()),
        }
    })
    .await;
    assert_eq!(calls, 3, "both 503s must be retried");
    assert_eq!(body, r#"{"id":7}"#);
}

#[tokio::test]
#[should_panic(expected = "unexpected status 422")]
async fn add_does_not_retry_a_real_failure() {
    post_until_created("injected add", 15, || {
        (422, r#"{"error":"not an NZB"}"#.to_string())
    })
    .await;
}

#[tokio::test]
#[should_panic(expected = "still 503")]
async fn add_fails_when_the_election_gap_never_closes() {
    post_until_created("injected add", 0, || {
        (
            503,
            r#"{"error":"leadership is changing; retry"}"#.to_string(),
        )
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_diagnostics_route_requires_configured_user_auth() {
    let tmp = tempfile::tempdir().unwrap();
    let post = build_post("auth", &[("x.bin", prng_bytes(1, 1000))], 1000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
    let node = start_node_with_auth(
        tmp.path(),
        "auth-node",
        NodeOpts {
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        1,
        nzbd_api::AuthConfig {
            username: "admin".into(),
            password: Some("secret".into()),
            token: None,
        },
    )
    .await;

    wait_for("authenticated diagnostics register this node", 15, || {
        let (code, body) = http_with_headers(
            &node.url,
            "GET",
            "/api/v1/cluster",
            b"",
            &[("Authorization", "Basic YWRtaW46c2VjcmV0")],
        );
        code == 200
            && serde_json::from_str::<serde_json::Value>(&body)
                .is_ok_and(|value| value["self"] == "auth-node")
    })
    .await;

    let (without_code, _) = http(&node.url, "GET", "/api/v1/cluster", b"");
    assert_eq!(without_code, 401);
    let (with_code, body) = http_with_headers(
        &node.url,
        "GET",
        "/api/v1/cluster",
        b"",
        &[("Authorization", "Basic YWRtaW46c2VjcmV0")],
    );
    assert_eq!(with_code, 200, "authenticated diagnostics failed: {body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["self"],
        "auth-node"
    );

    // User credentials do not replace the independent cluster secret.
    let poll = br#"{"node":"auth-node","free_download_slots":1,"free_pp_slots":0}"#;
    let (peer_code, _) = http_with_headers(
        &node.url,
        "POST",
        "/cluster/v1/work/poll",
        poll,
        &[
            ("Authorization", "Basic YWRtaW46c2VjcmV0"),
            ("Content-Type", "application/json"),
        ],
    );
    assert_eq!(peer_code, 401);

    // Leader discovery is part of the same peer-only namespace and must not
    // leak node URLs or epochs without the independent cluster credential.
    let (leader_code, _) = http(&node.url, "GET", "/cluster/v1/leader", b"");
    assert_eq!(leader_code, 401);
    let (leader_authed_code, _) = http_with_headers(
        &node.url,
        "GET",
        "/cluster/v1/leader",
        b"",
        &[("x-nzbd-cluster-secret", SECRET)],
    );
    assert_eq!(leader_authed_code, 200);

    node.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_voters_keep_majority_and_reject_mutations_after_quorum_loss() {
    let tmp = tempfile::tempdir().unwrap();
    let post = build_post("idle", &[("x.bin", prng_bytes(1, 1000))], 1000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();

    let opts = |p| NodeOpts {
        coordinator: true,
        priority: p,
        download: true,
        max_download_jobs: 1,
        post_process: false,
        min_free_disk_bytes: 0,
    };
    let topology = control_topology(3);
    let (a, b, c) = tokio::join!(
        start_node_with_control(
            tmp.path(),
            "a",
            opts(0),
            ns.port(),
            4,
            Default::default(),
            Some(topology[0].clone())
        ),
        start_node_with_control(
            tmp.path(),
            "b",
            opts(1),
            ns.port(),
            4,
            Default::default(),
            Some(topology[1].clone())
        ),
        start_node_with_control(
            tmp.path(),
            "c",
            opts(2),
            ns.port(),
            4,
            Default::default(),
            Some(topology[2].clone())
        ),
    );

    wait_for("one agreed leader", 15, || {
        let views: Vec<_> = [&a, &b, &c]
            .iter()
            .map(|n| get_json(&n.url, "/api/v1/cluster"))
            .collect();
        let leaders = views
            .iter()
            .filter(|v| v["is_leader"].as_bool() == Some(true))
            .count();
        let names: Vec<_> = views
            .iter()
            .filter_map(|v| v["leader"]["node"].as_str().map(String::from))
            .collect();
        leaders == 1
            && names.len() == 3
            && names.windows(2).all(|w| w[0] == w[1])
            && views[0]["nodes"].as_array().is_some_and(|n| n.len() == 3)
    })
    .await;

    // The shared worker registry and app-level leader record can converge
    // before every fixed control-plane voter has joined Raft. Before testing
    // loss of one voter, require a successful linearizable quorum round trip
    // from each node; `quorum_commit_healthy` is sampled from the real control
    // client, not inferred from the app registry.
    wait_for(
        "all three control voters have committed through quorum",
        20,
        || {
            [&a, &b, &c].iter().all(|node| {
                let control = get_json(&node.url, "/api/v1/cluster")["control"].clone();
                control["mode"] == "hiqlite-fixed-voters"
                    && control["quorum_commit_healthy"].as_bool() == Some(true)
            })
        },
    )
    .await;

    let leader_name = get_json(&a.url, "/api/v1/cluster")["leader"]["node"]
        .as_str()
        .unwrap()
        .to_string();
    let mut nodes = vec![a, b, c];
    let first = nodes
        .iter()
        .position(|node| node.name != leader_name)
        .unwrap();
    nodes.remove(first).kill().await;
    let leader_url = nodes
        .iter()
        .find(|node| node.name == leader_name)
        .unwrap()
        .url
        .clone();
    wait_for("majority remains healthy", 15, || {
        get_json(&leader_url, "/api/v1/cluster")["control"]["quorum_commit_healthy"] == true
    })
    .await;
    let second = nodes
        .iter()
        .position(|node| node.name != leader_name)
        .unwrap();
    nodes.remove(second).kill().await;
    wait_for("minority reports quorum loss", 15, || {
        get_json(&leader_url, "/api/v1/cluster")["control"]["quorum_commit_healthy"] == false
    })
    .await;
    let (code, body) = http(
        &leader_url,
        "POST",
        "/api/v1/jobs?name=blocked",
        b"not-an-nzb",
    );
    assert_eq!(code, 503, "minority accepted a mutation: {body}");
    nodes.pop().unwrap().kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn leader_retries_authority_adoption_after_snapshot_repair() {
    let tmp = tempfile::tempdir().unwrap();
    let state_dir = tmp.path().join(".nzbd-cluster");
    let post = build_post("repaired", &[("payload.bin", prng_bytes(9, 16_000))], 4_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
    let control = control_topology(1).remove(0);
    let opts = || NodeOpts {
        coordinator: true,
        priority: 0,
        download: true,
        max_download_jobs: 1,
        post_process: false,
        min_free_disk_bytes: 0,
    };

    // Complete the one-time legacy migration before simulating a future
    // snapshot. Once the replicated migration identity exists, restarts must
    // not decode or overwrite a newer queue format they do not understand.
    let initial = start_node_with_control(
        tmp.path(),
        "a",
        opts(),
        ns.port(),
        2,
        nzbd_api::AuthConfig::default(),
        Some(control.clone()),
    )
    .await;
    wait_for("initial migration leader elected", 15, || {
        get_json(&initial.url, "/api/v1/cluster")["is_leader"].as_bool() == Some(true)
    })
    .await;
    initial.kill().await;

    let snapshot_path = state_dir.join("queue.json");
    let unreadable = br#"{"schema_version":4,"jobs":[{"kind":"future_transfer"}]}"#;
    std::fs::write(&snapshot_path, unreadable).unwrap();
    let node = start_node_with_control(
        tmp.path(),
        "a",
        opts(),
        ns.port(),
        2,
        nzbd_api::AuthConfig::default(),
        Some(control),
    )
    .await;

    wait_for("leader elected", 15, || {
        get_json(&node.url, "/api/v1/cluster")["is_leader"].as_bool() == Some(true)
    })
    .await;
    let worker = start_node(
        tmp.path(),
        "worker",
        NodeOpts {
            coordinator: false,
            priority: 100,
            download: true,
            max_download_jobs: 1,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        2,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        std::fs::read(&snapshot_path).unwrap(),
        unreadable,
        "failed adoption rewrote the future snapshot"
    );

    // Repair the snapshot while leadership remains stable. The leader task
    // must re-attempt adoption on its next tick and resume scheduling.
    nzbd_state::SnapshotStore::open(&state_dir)
        .unwrap()
        .save(&nzbd_state::QueueSnapshotDoc::default())
        .unwrap();
    add_job(&node.url, "repaired", post.nzb.as_bytes()).await;
    wait_for("scheduling after snapshot repair", 20, || {
        let (code, body) = http(&node.url, "GET", "/api/v1/jobs", b"");
        let value: serde_json::Value = serde_json::from_str(&body)
            .unwrap_or_else(|error| panic!("jobs after snapshot repair: {code}: {body}: {error}"));
        if code == 503 {
            assert!(
                value == serde_json::json!({"error": "leadership is changing; retry"})
                    || value == serde_json::json!({"error": "no leader elected yet; retry"}),
                "jobs after snapshot repair: {code}: {body}"
            );
            // Election gaps are not queue observations. Keep the original
            // deadline and require a real successful completion response.
            return false;
        }
        assert_eq!(code, 200, "jobs after snapshot repair: {body}");
        value["jobs"][0]["status"] == "completed"
    })
    .await;

    worker.kill().await;
    node.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn distributed_download_via_any_node_with_budgets() {
    let tmp = tempfile::tempdir().unwrap();
    let files = [("one.bin".to_string(), prng_bytes(11, 120_000))];
    let post = build_post(
        "clusterjob",
        &files
            .iter()
            .map(|(n, d)| (n.as_str(), d.clone()))
            .collect::<Vec<_>>(),
        20_000,
    );
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();

    // Leader cannot download; the worker must get the job.
    let a = start_node(
        tmp.path(),
        "a",
        NodeOpts {
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;
    let b = start_node(
        tmp.path(),
        "b",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: true,
            max_download_jobs: 2,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;

    wait_for("leader elected", 15, || {
        get_json(&a.url, "/api/v1/cluster")["is_leader"].as_bool() == Some(true)
    })
    .await;

    // Add via the WORKER's API: must proxy to the leader.
    add_job(&b.url, "clusterjob", post.nzb.as_bytes()).await;

    // The job gets delegated to b and completes.
    wait_for("delegation to b", 15, || {
        let v = get_json(&a.url, "/api/v1/jobs");
        v["jobs"][0]["assigned_node"].as_str() == Some("b") || v["jobs"][0]["status"] == "completed"
    })
    .await;
    wait_for("completion", 30, || {
        get_json(&a.url, "/api/v1/jobs")["jobs"][0]["status"] == "completed"
    })
    .await;

    // Bit-identical output on the shared volume; work done by b.
    let got = std::fs::read(tmp.path().join("complete/clusterjob/one.bin")).unwrap();
    assert_eq!(got, files[0].1);
    assert!(ns.total_hits() > 0);

    // The non-executing authority consumes no share, so the only worker may
    // use the full account cap of four.
    assert!(
        ns.max_concurrent_connections() <= 4,
        "budget exceeded: {} concurrent connections",
        ns.max_concurrent_connections()
    );

    // The worker's own API view (proxied) agrees.
    let v = get_json(&b.url, "/api/v1/status");
    assert_eq!(v["jobs_finished"], 1);

    a.kill().await;
    b.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn article_ranges_use_two_workers_and_one_authoritative_assembly() {
    let tmp = tempfile::tempdir().unwrap();
    let data = prng_bytes(19, 96 * 4096);
    let post = build_post("ranged", &[("range.bin", data.clone())], 4096);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();

    let leader = start_node(
        tmp.path(),
        "leader",
        NodeOpts {
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;
    let (b, c) = tokio::join!(
        start_node(
            tmp.path(),
            "b",
            NodeOpts {
                coordinator: false,
                priority: 9,
                download: true,
                max_download_jobs: 2,
                post_process: false,
                min_free_disk_bytes: 0,
            },
            ns.port(),
            4,
        ),
        start_node(
            tmp.path(),
            "c",
            NodeOpts {
                coordinator: false,
                priority: 9,
                download: true,
                max_download_jobs: 2,
                post_process: false,
                min_free_disk_bytes: 0,
            },
            ns.port(),
            4,
        ),
    );
    wait_for("range workers registered", 15, || {
        get_json(&leader.url, "/api/v1/cluster")["nodes"]
            .as_array()
            .is_some_and(|nodes| nodes.len() >= 3)
    })
    .await;

    add_job(&leader.url, "ranged", post.nzb.as_bytes()).await;
    let assembled = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let (code, body) = http(&leader.url, "GET", "/api/v1/jobs", b"");
            let value: serde_json::Value = serde_json::from_str(&body).unwrap_or_else(|error| {
                panic!("jobs during range assembly: {code}: {body}: {error}")
            });
            if code == 503 {
                assert!(
                    value == serde_json::json!({"error": "leadership is changing; retry"})
                        || value == serde_json::json!({"error": "no leader elected yet; retry"}),
                    "jobs during range assembly: {code}: {body}"
                );
                // Election gaps are not queue observations. Keep the original
                // deadline and require a real successful completion response.
            } else {
                assert_eq!(code, 200, "jobs during range assembly: {body}");
                if value["jobs"][0]["status"] == "completed"
                    && tmp.path().join("complete/ranged/range.bin").is_file()
                {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        assembled.is_ok(),
        "range assembly stalled: leader={} b={} c={} cluster={}",
        get_json(&leader.url, "/api/v1/jobs"),
        get_json(&b.url, "/api/v1/jobs"),
        get_json(&c.url, "/api/v1/jobs"),
        get_json(&leader.url, "/api/v1/cluster")
    );
    assert_eq!(
        std::fs::read(tmp.path().join("complete/ranged/range.bin")).unwrap(),
        data
    );
    for article in 1..=96 {
        assert_eq!(
            ns.hits(&post.message_id("range.bin", article)),
            1,
            "committed article {article} was fetched more than once"
        );
    }
    let mut range_owners = std::collections::HashSet::new();
    for manifest in manifest_paths(&tmp.path().join(".nzbd-cluster/generations")) {
        let root = manifest.parent().unwrap();
        let job: nzbd_types::Job =
            serde_json::from_slice(&std::fs::read(root.join("job.json")).unwrap()).unwrap();
        if job
            .files
            .first()
            .is_some_and(|file| file.segments.len() <= 32)
        {
            let value: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
            range_owners.insert(value["owner_node_id"].as_str().unwrap().to_string());
        }
    }
    assert_eq!(range_owners.len(), 2, "ranges did not use both workers");

    leader.kill().await;
    b.kill().await;
    c.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn low_disk_worker_is_visible_and_receives_no_new_lease() {
    let tmp = tempfile::tempdir().unwrap();
    let post = build_post("held", &[("one.bin", prng_bytes(12, 20_000))], 10_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();

    let leader = start_node(
        tmp.path(),
        "leader",
        NodeOpts {
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        2,
    )
    .await;
    let held = start_node(
        tmp.path(),
        "held",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: true,
            max_download_jobs: 1,
            post_process: true,
            min_free_disk_bytes: u64::MAX / 2,
        },
        ns.port(),
        2,
    )
    .await;

    wait_for("held worker publishes its limiting volume", 15, || {
        get_json(&leader.url, "/api/v1/cluster")["nodes"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|node| {
                node["name"] == "held"
                    && node["disk_low"] == true
                    && node["disk_guard_label"].as_str().is_some()
            })
    })
    .await;

    add_job(&held.url, "held", post.nzb.as_bytes()).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let jobs = get_json(&leader.url, "/api/v1/jobs");
    assert_eq!(jobs["jobs"][0]["assigned_node"], serde_json::Value::Null);
    assert_eq!(jobs["jobs"][0]["status"], "queued");
    assert_eq!(ns.total_hits(), 0);

    leader.kill().await;
    held.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn worker_death_reclaims_and_resumes_elsewhere_without_refetch() {
    let tmp = tempfile::tempdir().unwrap();
    let data = prng_bytes(21, 40 * 4096);
    let post = build_post("reclaimable", &[("big.bin", data.clone())], 4096);

    // Parts 1..=6 fast, the rest slow — b will journal a few then die.
    let mut builder = NservBuilder::new().with_post(&post);
    for part in 7..=40 {
        builder = builder.behavior(
            &post.message_id("big.bin", part),
            nzbd_nserv::Behavior::Delay(Duration::from_millis(400)),
        );
    }
    let ns = builder.start().await.unwrap();

    let a = start_node(
        tmp.path(),
        "a",
        NodeOpts {
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;
    let b = start_node(
        tmp.path(),
        "b",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: true,
            max_download_jobs: 2,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;

    wait_for("leader elected", 15, || {
        get_json(&a.url, "/api/v1/cluster")["is_leader"].as_bool() == Some(true)
    })
    .await;
    add_job(&a.url, "reclaimable", post.nzb.as_bytes()).await;

    // Wait until b journaled some segments, then kill it.
    let shared = tmp.path().to_path_buf();
    wait_for("progress on b", 20, || {
        journaled_segments(&shared).len() >= 3
    })
    .await;
    let done_before = journaled_segments(&shared);
    b.kill().await;
    eprintln!(
        "journaled before shutdown: {done_before:?}; request counts before successor starts: {:?}",
        done_before
            .iter()
            .map(|segment| (*segment, ns.hits(&post.message_id("big.bin", *segment))))
            .collect::<Vec<_>>()
    );

    // Third node joins; the lease expires; the job is reclaimed and
    // re-delegated to c, which must not re-fetch journaled segments.
    let c = start_node(
        tmp.path(),
        "c",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: true,
            max_download_jobs: 2,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;

    wait_for("completion after reclaim", 60, || {
        get_json(&a.url, "/api/v1/jobs")["jobs"][0]["status"] == "completed"
    })
    .await;

    let got = std::fs::read(tmp.path().join("complete/reclaimable/big.bin")).unwrap();
    assert_eq!(got, data, "resumed file must be bit-identical");
    for seg in &done_before {
        assert_eq!(
            ns.hits(&post.message_id("big.bin", *seg)),
            1,
            "segment {seg} was journaled by b and must not be re-fetched by c"
        );
    }

    a.kill().await;
    c.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn leader_death_fails_over_and_adopts_the_running_lease() {
    let tmp = tempfile::tempdir().unwrap();
    let data = prng_bytes(31, 40 * 4096);
    let post = build_post("failover", &[("f.bin", data.clone())], 4096);

    let mut builder = NservBuilder::new().with_post(&post);
    for part in 7..=40 {
        builder = builder.behavior(
            &post.message_id("f.bin", part),
            nzbd_nserv::Behavior::Delay(Duration::from_millis(300)),
        );
    }
    let ns = builder.start().await.unwrap();

    // Three fixed voters plus a dedicated executor. The voter that wins a
    // simultaneous cold start is deliberately not assumed: the invariant is
    // that any successor adopts the executor's durable running lease.
    let topology = control_topology(3);
    let (a, b, c) = tokio::join!(
        start_node_with_control(
            tmp.path(),
            "a",
            NodeOpts {
                coordinator: true,
                priority: 0,
                download: false,
                max_download_jobs: 0,
                post_process: false,
                min_free_disk_bytes: 0,
            },
            ns.port(),
            4,
            Default::default(),
            Some(topology[0].clone())
        ),
        start_node_with_control(
            tmp.path(),
            "b",
            NodeOpts {
                coordinator: true,
                priority: 9,
                download: false,
                max_download_jobs: 0,
                post_process: false,
                min_free_disk_bytes: 0,
            },
            ns.port(),
            4,
            Default::default(),
            Some(topology[1].clone())
        ),
        start_node_with_control(
            tmp.path(),
            "c",
            NodeOpts {
                coordinator: true,
                priority: 4,
                download: false,
                max_download_jobs: 0,
                post_process: false,
                min_free_disk_bytes: 0,
            },
            ns.port(),
            4,
            Default::default(),
            Some(topology[2].clone())
        ),
    );
    let mut voters = vec![a, b, c];

    wait_for("every voter agrees on one leader", 20, || {
        let views: Vec<_> = voters
            .iter()
            .map(|node| get_json(&node.url, "/api/v1/cluster"))
            .collect();
        let leader = views[0]["leader"]["node"].as_str();
        leader.is_some()
            && views
                .iter()
                .all(|view| view["leader"]["node"].as_str() == leader)
            && views
                .iter()
                .filter(|view| view["is_leader"].as_bool() == Some(true))
                .count()
                == 1
    })
    .await;
    let worker = start_node(
        tmp.path(),
        "worker",
        NodeOpts {
            coordinator: false,
            priority: 100,
            download: true,
            max_download_jobs: 2,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;
    // Add through the worker: the request must proxy to whichever voter won.
    add_job(&worker.url, "failover", post.nzb.as_bytes()).await;

    // The dedicated worker makes progress, then the leader dies.
    let shared = tmp.path().to_path_buf();
    wait_for("progress on worker", 20, || {
        journaled_segments(&shared).len() >= 3
    })
    .await;
    let done_before = journaled_segments(&shared);
    let before = get_json(&voters[0].url, "/api/v1/cluster");
    let epoch_before = before["epoch"].as_u64().unwrap();
    let old_leader = before["leader"]["node"].as_str().unwrap().to_string();
    let old_index = voters
        .iter()
        .position(|node| node.name == old_leader)
        .unwrap();
    voters.remove(old_index).kill().await;

    // A surviving voter takes over with a higher epoch; the worker keeps
    // executing and its lease is adopted via heartbeat, not restarted.
    wait_for("surviving voter takes office", 30, || {
        voters.iter().any(|node| {
            let view = get_json(&node.url, "/api/v1/cluster");
            view["is_leader"].as_bool() == Some(true)
                && view["epoch"].as_u64().unwrap_or(0) > epoch_before
        })
    })
    .await;
    let new_leader_url = voters
        .iter()
        .find(|node| get_json(&node.url, "/api/v1/cluster")["is_leader"].as_bool() == Some(true))
        .unwrap()
        .url
        .clone();
    wait_for("completion under the new leader", 60, || {
        let (code, body) = http(&new_leader_url, "GET", "/api/v1/jobs", b"");
        code == 200
            && serde_json::from_str::<serde_json::Value>(&body)
                .is_ok_and(|value| value["jobs"][0]["status"] == "completed")
    })
    .await;

    let got = std::fs::read(tmp.path().join("complete/failover/f.bin")).unwrap();
    assert_eq!(got, data);
    for seg in &done_before {
        assert_eq!(
            ns.hits(&post.message_id("f.bin", *seg)),
            1,
            "segment {seg} must not be re-fetched across the failover"
        );
    }
    // The worker's view agrees the job is done through the new leader.
    assert_eq!(get_json(&worker.url, "/api/v1/status")["jobs_finished"], 1);

    worker.kill().await;
    for voter in voters {
        voter.kill().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_cluster_restart_keeps_the_queue() {
    let tmp = tempfile::tempdir().unwrap();
    let data = prng_bytes(41, 50_000);
    let post = build_post("solo", &[("s.bin", data.clone())], 10_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();

    let opts = || NodeOpts {
        coordinator: true,
        priority: 0,
        download: true,
        max_download_jobs: 2,
        post_process: false,
        min_free_disk_bytes: 0,
    };
    let control = control_topology(1).remove(0);
    let a = start_node_with_control(
        tmp.path(),
        "solo",
        opts(),
        ns.port(),
        4,
        nzbd_api::AuthConfig::default(),
        Some(control.clone()),
    )
    .await;
    wait_for("self-election", 15, || {
        get_json(&a.url, "/api/v1/cluster")["is_leader"].as_bool() == Some(true)
    })
    .await;

    add_job(&a.url, "solo", post.nzb.as_bytes()).await;
    wait_for("queue committed", 15, || {
        get_json(&a.url, "/api/v1/jobs")["jobs"][0]["name"] == "solo"
    })
    .await;
    assert_eq!(
        ns.total_hits(),
        0,
        "authority must not execute unfenced work"
    );
    a.kill().await;

    // Restart: the queue authority state survives on the shared volume.
    let a2 = start_node_with_control(
        tmp.path(),
        "solo",
        opts(),
        ns.port(),
        4,
        nzbd_api::AuthConfig::default(),
        Some(control),
    )
    .await;
    wait_for("re-election after restart", 20, || {
        get_json(&a2.url, "/api/v1/cluster")["is_leader"].as_bool() == Some(true)
    })
    .await;
    wait_for("queue recovered", 15, || {
        let v = get_json(&a2.url, "/api/v1/jobs");
        v["jobs"][0]["name"] == "solo"
    })
    .await;
    a2.kill().await;
}

/// Plan M1a (i), review #280-6: a worker's post-processing failure is one
/// terminal, recorded by the leader — exactly one `job_terminals` row (on
/// the leader; the executing worker records none), one history row and one
/// `job_pp_finished` — and the PP lease is released. The failure's bytes are
/// disposed through the cluster cleanup ledger, not by a second node; the
/// non-executing nodes hold no terminal row to dispose from.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn worker_pp_failure_is_one_leader_terminal() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    if !require_tool("7z") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let files = vec![(
        "broken.zip",
        b"PK\x03\x04 this is not a complete zip archive".to_vec(),
    )];
    let post = build_post("brokenpp", &files, 20_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
    let node = |coordinator, priority, download, post_process| NodeOpts {
        coordinator,
        priority,
        download,
        max_download_jobs: if download { 2 } else { 0 },
        post_process,
        min_free_disk_bytes: 0,
    };
    // The authority runs post-processing too (the daemon's usual shape):
    // only a node with a PP setup holds the shared history, and the leader
    // must hold it to own the terminal.
    let a = start_node(tmp.path(), "a", node(true, 0, false, true), ns.port(), 4).await;
    let b = start_node(tmp.path(), "b", node(false, 5, true, false), ns.port(), 4).await;
    let c = start_node(tmp.path(), "c", node(false, 9, false, true), ns.port(), 4).await;
    wait_for("leader elected", 15, || {
        get_json(&a.url, "/api/v1/cluster")["is_leader"].as_bool() == Some(true)
    })
    .await;
    wait_for("all nodes registered", 15, || {
        get_json(&a.url, "/api/v1/cluster")["nodes"]
            .as_array()
            .is_some_and(|n| n.len() == 3)
    })
    .await;
    let mut events = a.runtime.engine.subscribe();
    add_job(&a.url, "brokenpp", post.nzb.as_bytes()).await;
    wait_for("worker PP failure recorded by the leader", 60, || {
        a.runtime
            .engine
            .artifacts()
            .terminal(1)
            .unwrap()
            .is_some_and(|terminal| terminal.emitted)
    })
    .await;
    let terminal = a.runtime.engine.artifacts().terminal(1).unwrap().unwrap();
    assert_eq!(terminal.outcome, nzbd_types::TerminalOutcome::Failed);
    assert_eq!(terminal.reason, Some(nzbd_types::FailureReason::Unpack));
    assert!(terminal.blames_release);
    assert!(terminal.markers_stamped);
    let seq = terminal.history_seq.expect("history row");
    for other in [&b, &c] {
        assert!(
            other
                .runtime
                .engine
                .artifacts()
                .terminal(1)
                .unwrap()
                .is_none(),
            "{}: only the leader records the terminal",
            other.url
        );
    }
    // One history row for the job across every node's shared history log.
    let rows = std::fs::read_dir(tmp.path().join(".nzbd-cluster/history"))
        .unwrap()
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".jsonl"))
        .flat_map(|entry| {
            std::fs::read_to_string(entry.path())
                .unwrap_or_default()
                .lines()
                .filter_map(|line| serde_json::from_str::<nzbd_state::HistoryEntry>(line).ok())
                .filter(|row| row.name == "brokenpp")
                .map(|row| (row.job, row.completed_at_unix, row.status))
                .collect::<Vec<_>>()
        })
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(rows.len(), 1, "exactly one history row: {rows:?}");
    wait_for("failed row retired from the leader's queue", 30, || {
        get_json(&a.url, "/api/v1/jobs")["jobs"]
            .as_array()
            .is_some_and(|jobs| jobs.is_empty())
    })
    .await;
    assert!(
        get_json(&a.url, "/api/v1/cluster")["leases"]
            .as_array()
            .is_none_or(|leases| leases.is_empty()),
        "the PP lease is released"
    );
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let mut finished = 0;
    while let Ok(event) = events.try_recv() {
        if let nzbd_engine::Event::JobPpFinished {
            job, history_seq, ..
        } = event
        {
            if job.0 == 1 {
                assert_eq!(history_seq, seq);
                finished += 1;
            }
        }
    }
    assert_eq!(finished, 1, "exactly one job_pp_finished");
    a.kill().await;
    b.kill().await;
    c.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn pp_runs_on_idle_node_via_anti_affinity() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    // C2: the leader downloads a job with a real par2 set; the scheduler
    // must hand post-processing to the idle non-download node, which
    // quick-verifies natively, stamps the job, appends shared-volume
    // history and returns the finished job to the leader.
    if !require_tool("par2") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();

    let src = tempfile::tempdir().unwrap();
    let payload = prng_bytes(77, 90_000);
    std::fs::write(src.path().join("payload.mkv"), &payload).unwrap();
    let ok = std::process::Command::new("par2")
        .args([
            "create",
            "-q",
            "-q",
            "-s8192",
            "-c4",
            "set.par2",
            "payload.mkv",
        ])
        .current_dir(src.path())
        .status()
        .expect("par2 binary required (apt-get install par2)")
        .success();
    assert!(ok, "par2 create failed");
    let mut files: Vec<(String, Vec<u8>)> = vec![("payload.mkv".into(), payload.clone())];
    let mut pars: Vec<_> = std::fs::read_dir(src.path())
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().map(|e| e == "par2").unwrap_or(false))
        .collect();
    pars.sort();
    for p in pars {
        files.push((
            p.file_name().unwrap().to_string_lossy().into_owned(),
            std::fs::read(&p).unwrap(),
        ));
    }
    let post = build_post(
        "pardl",
        &files
            .iter()
            .map(|(n, d)| (n.as_str(), d.clone()))
            .collect::<Vec<_>>(),
        20_000,
    );
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();

    // a: authority only. b: downloader. c: PP executor — the
    // anti-affinity target.
    let a = start_node(
        tmp.path(),
        "a",
        NodeOpts {
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;
    let b = start_node(
        tmp.path(),
        "b",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: true,
            max_download_jobs: 2,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;
    let c = start_node(
        tmp.path(),
        "c",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: false,
            max_download_jobs: 0,
            post_process: true,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;

    wait_for("leader elected", 15, || {
        get_json(&a.url, "/api/v1/cluster")["is_leader"].as_bool() == Some(true)
    })
    .await;
    wait_for("all nodes registered", 15, || {
        get_json(&a.url, "/api/v1/cluster")["nodes"]
            .as_array()
            .is_some_and(|n| n.len() == 3)
    })
    .await;

    // The authority's post pause holds remote post-processing too: the
    // download finishes on b, but no worker is granted the post lease
    // until the lane resumes.
    let (code, body) = http(&a.url, "POST", "/api/v1/queue/actions/pause-post", b"");
    assert_eq!(code, 200, "pause-post: {body}");
    add_job(&a.url, "pardl", post.nzb.as_bytes()).await;
    wait_for("download finished while post is paused", 45, || {
        get_json(&a.url, "/api/v1/jobs")["jobs"]
            .as_array()
            .and_then(|j| j.first().cloned())
            .is_some_and(|j| j["remaining_bytes"] == 0 && j["status"] == "completed")
    })
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        !tmp.path()
            .join(".nzbd-cluster/history/history.c.jsonl")
            .exists(),
        "no remote post-processing may run while the post lane is paused"
    );
    assert_eq!(get_json(&a.url, "/api/v1/status")["post_paused"], true);
    let (code, body) = http(&a.url, "POST", "/api/v1/queue/actions/resume-post", b"");
    assert_eq!(code, 200, "resume-post: {body}");

    // Download completes on b (c can't download); PP is assigned to c,
    // executes there, and the stamped job comes back and is RETIRED to
    // history by the leader sweep. Node a has NO PP manager
    // (post_process=false), so history appearing at all proves remote
    // execution; the per-node file below proves it was c.
    wait_for("pp done remotely + retired to history", 45, || {
        let empty = get_json(&a.url, "/api/v1/jobs")["jobs"]
            .as_array()
            .is_some_and(|j| j.is_empty());
        let hist =
            std::fs::read_to_string(tmp.path().join(".nzbd-cluster/history/history.c.jsonl"))
                .unwrap_or_default();
        empty && hist.contains("\"SUCCESS\"")
    })
    .await;

    // Payload survived PP bit-identically (no repair was needed).
    let got = std::fs::read(tmp.path().join("complete/pardl/payload.mkv")).unwrap();
    assert_eq!(got, payload);

    // No staging residue in the job dir.
    let residue: Vec<String> = std::fs::read_dir(tmp.path().join("complete/pardl"))
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".pp."))
        .collect();
    assert!(residue.is_empty(), "staging residue: {residue:?}");

    // History JSONL appended by node c on the shared volume.
    let hist = std::fs::read_to_string(tmp.path().join(".nzbd-cluster/history/history.c.jsonl"))
        .expect("node c must have appended shared history");
    assert!(hist.contains("\"SUCCESS\""), "history: {hist}");
    assert!(hist.contains("\"pardl\""), "history: {hist}");

    // Contract §2.1 with post-processing off on the authority: the worker
    // that writes the history row records the job's one terminal.
    let recorded = [&a, &b, &c]
        .iter()
        .filter_map(|node| {
            node.runtime
                .engine
                .artifacts()
                .terminal(1)
                .unwrap()
                .map(|terminal| (node.url.clone(), terminal))
        })
        .collect::<Vec<_>>();
    assert_eq!(recorded.len(), 1, "exactly one terminal: {recorded:?}");
    assert_eq!(
        recorded[0].0, c.url,
        "recorded where the history row is written"
    );
    assert_eq!(
        recorded[0].1.outcome,
        nzbd_types::TerminalOutcome::Completed
    );
    assert!(recorded[0].1.emitted);

    a.kill().await;
    b.kill().await;
    c.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn delivery_receipts_survive_authority_restart_and_consumer_removed_publication() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let tmp = tempfile::tempdir().unwrap();
    // Existing local receipts keep their original instance pin and cursor.
    let inventory =
        nzbd_state::artifacts::Inventory::open(&tmp.path().join("control-a/file-lifecycle"))
            .unwrap();
    let old_root = tmp.path().join("legacy-completed");
    std::fs::create_dir(&old_root).unwrap();
    let old_path = old_root.join("old-delivery");
    inventory
        .allocate_for_delivery(711, &old_root, &old_path, true, true)
        .unwrap();
    std::fs::write(old_path.join("old.mkv"), b"old committed payload").unwrap();
    inventory
        .finish(711, &old_path, &old_root, "completed")
        .unwrap();
    let old = inventory
        .create_delivery(711, true, Some("before-upgrade"))
        .unwrap();
    let installation = old.runner_instance_id.clone();
    drop(inventory);
    let bytes = prng_bytes(83, 90_000);
    let post = build_post("cluster-ready", &[("episode.mkv", bytes.clone())], 20_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
    let authority_opts = || NodeOpts {
        coordinator: true,
        priority: 0,
        download: false,
        max_download_jobs: 0,
        post_process: false,
        min_free_disk_bytes: 0,
    };
    let control = control_topology(1).remove(0);
    let authority = start_node_with_control(
        tmp.path(),
        "a",
        authority_opts(),
        ns.port(),
        4,
        nzbd_api::AuthConfig::default(),
        Some(control.clone()),
    )
    .await;
    let downloader = start_node(
        tmp.path(),
        "b",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: true,
            max_download_jobs: 2,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;
    let post_worker = start_node(
        tmp.path(),
        "c",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: false,
            max_download_jobs: 0,
            post_process: true,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;
    wait_for("three nodes and catalog adoption", 20, || {
        let cluster = get_json(&authority.url, "/api/v1/cluster");
        let (code, body) = http(
            &authority.url,
            "GET",
            "/api/v1/deliveries?after=0&includeAccepted=true",
            b"",
        );
        cluster["is_leader"] == true
            && cluster["nodes"]
                .as_array()
                .is_some_and(|nodes| nodes.len() == 3)
            && code == 200
            && body.contains(&old.delivery_id)
            && serde_json::from_str::<serde_json::Value>(&body)
                .is_ok_and(|page| page["catalogAdoptionComplete"] == true)
    })
    .await;
    let admitted=post_until_created("lifecycle NZB admission",15,||http(&authority.url,"POST",
        "/api/v1/jobs?name=cluster-ready&params=%7B%22monarr-delivery%22%3A%221%22%2C%22monarr-transfer%22%3A%22cluster-ready-token%22%7D",post.nzb.as_bytes())).await;
    assert!(admitted.contains("id"));
    let debug_started = tokio::time::Instant::now();
    wait_for("remote POST creates cluster-visible READY", 45, || {
        if debug_started.elapsed().as_secs() % 10 == 9 {
            eprintln!(
                "authority={} downloader={} pp={}",
                get_json(&authority.url, "/api/v1/jobs"),
                serde_json::to_value(&downloader.runtime.engine.snapshot().jobs).unwrap(),
                serde_json::to_value(&post_worker.runtime.engine.snapshot().jobs).unwrap()
            );
        }
        let (code, body) = http(
            &authority.url,
            "GET",
            "/api/v1/deliveries?after=0&includeAccepted=true",
            b"",
        );
        code == 200
            && serde_json::from_str::<serde_json::Value>(&body).is_ok_and(|page| {
                page["deliveries"].as_array().is_some_and(|rows| {
                    rows.iter()
                        .any(|row| row["requestToken"] == "cluster-ready-token")
                })
            })
    })
    .await;
    let page = get_json(
        &authority.url,
        "/api/v1/deliveries?after=0&includeAccepted=true",
    );
    let delivery = page["deliveries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["requestToken"] == "cluster-ready-token")
        .unwrap()
        .clone();
    assert_eq!(delivery["runnerInstanceId"], installation);
    assert!(delivery["sequence"].as_i64().unwrap() > old.sequence);
    assert_eq!(delivery["runnerStages"]["nodeId"], "unrecorded");
    assert!(delivery["sourceCopies"]
        .as_array()
        .is_some_and(|copies| copies.len() >= 3));
    let path = std::path::PathBuf::from(delivery["publicationPath"].as_str().unwrap());
    assert_eq!(
        std::fs::read(path.join(delivery["files"][0]["path"].as_str().unwrap())).unwrap(),
        bytes
    );
    // Contract §2.5: the leader's publication directory is a publication —
    // `.runner-manifest.json` written last, its digest the row's.
    let runner_manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(path.join(".runner-manifest.json")).expect("cluster publication manifest"),
    )
    .unwrap();
    assert_eq!(runner_manifest["digest"], delivery["manifestDigest"]);
    assert_eq!(
        runner_manifest["generation"],
        delivery["publicationGeneration"]
    );
    assert_eq!(runner_manifest["transfer"], "cluster-ready-token");
    assert_eq!(runner_manifest["cross_volume"], true);
    assert_eq!(
        runner_manifest["files"].as_array().map(Vec::len),
        delivery["files"].as_array().map(Vec::len)
    );
    let id = delivery["deliveryId"].as_str().unwrap();
    let acceptance=serde_json::to_vec(&serde_json::json!({"protocolVersion":1,"publicationGeneration":delivery["publicationGeneration"],"manifestDigest":delivery["manifestDigest"],"consumerReceiptId":"curator-accepted-cluster"})).unwrap();
    let (status, accepted) = http_with_headers(
        &post_worker.url,
        "POST",
        &format!("/api/v1/deliveries/{id}/accept"),
        &acceptance,
        &[("Content-Type", "application/json")],
    );
    // Contract §2.5 (v2, runner M3): there is no acceptance. The route is
    // gone, and answers so through the worker's proxy as well; source cleanup
    // does not wait for a consumer receipt.
    assert_eq!(status, 410, "acceptance through worker proxy: {accepted}");
    assert!(accepted.contains("acceptance was removed"), "{accepted}");
    // Consumer is entitled to remove its transferred publication immediately.
    std::fs::remove_dir_all(&path).unwrap();
    let last_cleanup_progress = std::cell::RefCell::new(String::new());
    wait_for(
        "all exact successful source copies acknowledged removed",
        30,
        || {
            let value = get_json(&authority.url, &format!("/api/v1/deliveries/{id}"));
            let progress=serde_json::json!({
                "status":value["sourceCleanup"]["status"],
                "error":value["sourceCleanup"]["error"].as_str().map(|error|error.chars().take(1024).collect::<String>()),
                "copies":value["sourceCopies"].as_array().map(|copies|copies.iter().take(16).map(|copy|serde_json::json!({
                    "id":copy["id"],"ownerNode":copy["ownerNode"],"status":copy["status"],"attempts":copy["attempts"],"nextRetryAt":copy["nextRetryAt"],
                    "error":copy["error"].as_str().map(|error|error.chars().take(1024).collect::<String>())
                })).collect::<Vec<_>>())
            }).to_string();
            if *last_cleanup_progress.borrow()!=progress {eprintln!("exact source cleanup: {progress}");*last_cleanup_progress.borrow_mut()=progress;}
            value["sourceCleanup"]["status"] == "succeeded"
                && value["sourceCopies"].as_array().is_some_and(|copies| {
                    !copies.is_empty() && copies.iter().all(|copy| copy["status"] == "removed")
                })
        },
    )
    .await;
    let (code, retried) = http_with_headers(
        &authority.url,
        "POST",
        &format!("/api/v1/deliveries/{id}/source-cleanup/retry"),
        b"",
        &[],
    );
    assert_eq!(code, 200, "{retried}");
    let retried: serde_json::Value = serde_json::from_str(&retried).unwrap();
    assert_eq!(retried["sourceCleanup"]["status"], "succeeded");
    assert!(
        !tmp.path().join("complete/cluster-ready").exists(),
        "download source must drain independently of the publication: {:?}; receipt {}",
        std::fs::read_dir(tmp.path().join("complete/cluster-ready"))
            .ok()
            .map(|entries| entries
                .flatten()
                .map(|entry| entry.file_name())
                .collect::<Vec<_>>()),
        get_json(&authority.url, &format!("/api/v1/deliveries/{id}"))
    );
    assert!(
        manifest_paths(&tmp.path().join(".nzbd-cluster/generations")).is_empty(),
        "all exact sealed duplicates must drain"
    );
    authority.kill().await;
    let successor = start_node_with_control(
        tmp.path(),
        "a",
        authority_opts(),
        ns.port(),
        4,
        nzbd_api::AuthConfig::default(),
        Some(control),
    )
    .await;
    wait_for(
        "authoritative receipt recovery without payload rehash",
        25,
        || {
            let (code, body) = http(
                &successor.url,
                "GET",
                &format!("/api/v1/deliveries/{id}"),
                b"",
            );
            // Contract v2: accept is a no-op, so the replayed row carries no
            // consumer receipt; recovery is the row answering unchanged.
            code == 200 && body.contains("deliveryId")
        },
    )
    .await;
    let replay = get_json(&successor.url, &format!("/api/v1/deliveries/{id}"));
    for key in [
        "deliveryId",
        "runnerInstanceId",
        "sequence",
        "publicationGeneration",
        "manifestDigest",
        "files",
        "runnerStages",
    ] {
        assert_eq!(
            replay[key], delivery[key],
            "immutable {key} changed across authority restart"
        );
    }
    assert!(
        !path.exists(),
        "replay must not republish a consumer-removed path"
    );
    let old_replay = get_json(
        &successor.url,
        &format!("/api/v1/deliveries/{}", old.delivery_id),
    );
    assert_eq!(old_replay["sequence"], old.sequence);
    assert_eq!(old_replay["runnerInstanceId"], installation);
    downloader.kill().await;
    post_worker.kill().await;
    successor.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_control_migration_decodes_compressed_snapshot_and_backs_up_original_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let job = nzbd_types::Job {
        id: nzbd_types::JobId(700),
        kind: nzbd_types::JobKind::Nzb,
        name: "paused legacy".into(),
        dir_name: "paused-legacy".into(),
        name_provisional: false,
        delivery_lifecycle: false,
        queued_at_unix: 1,
        original_name: String::new(),
        category: None,
        priority: 0,
        dupe: Default::default(),
        params: vec![("opaque-user-metadata".into(), "metadata".repeat(160_000))],
        files: Vec::new(),
        totals: Default::default(),
        status: nzbd_types::JobStatus::Paused,
        torrent: None,
        stages: Vec::new(),
    };
    let snapshot = nzbd_state::QueueSnapshotDoc {
        jobs: vec![job],
        next_job_id: 700,
        ..Default::default()
    };
    let state = tmp.path().join(".nzbd-cluster");
    nzbd_state::SnapshotStore::open(&state)
        .unwrap()
        .save(&snapshot)
        .unwrap();
    let original = std::fs::read(state.join("queue.json")).unwrap();
    assert!(original.starts_with(b"NZBDQZ01"));
    let mut fingerprint = sha2::Sha256::new();
    fingerprint.update(b"queue\0");
    fingerprint.update(&original);
    fingerprint.update(b"history\0");
    let backup = tmp
        .path()
        .join("control-a/migration-backup")
        .join(format!("{:x}", fingerprint.finalize()))
        .join("queue.json");
    let node = start_node(
        tmp.path(),
        "a",
        NodeOpts {
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        9,
        1,
    )
    .await;
    assert_eq!(std::fs::read(backup).unwrap(), original);
    wait_for("compressed legacy queue reaches authority", 15, || {
        node.runtime
            .engine
            .snapshot()
            .jobs
            .iter()
            .any(|job| job.id == nzbd_types::JobId(700))
    })
    .await;
    node.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_retired_receipt_without_control_proof_is_preserved_and_refuses_upgrade() {
    let tmp = tempfile::tempdir().unwrap();
    let token = "unproven-legacy-receipt";
    let receipt:nzbd_state::UrlAdmissionReceipt=serde_json::from_value(serde_json::json!({"protocolVersion":1,"requestToken":token,"submissionSha256":"accepted-original-fields","jobId":40,"generation":format!("prior-instance-url-40-{:x}",sha2::Sha256::digest(token.as_bytes())),"runnerInstanceId":"prior-instance","outcome":"completed","terminalOutcome":"SUCCESS","custodyState":"curator","sourceCleanupStatus":"removed"})).unwrap();
    let snapshot = nzbd_state::QueueSnapshotDoc {
        url_admission_receipts: vec![receipt.clone()],
        next_job_id: 40,
        ..Default::default()
    };
    let state = tmp.path().join(".nzbd-cluster");
    nzbd_state::SnapshotStore::open(&state)
        .unwrap()
        .save(&snapshot)
        .unwrap();
    let node = start_node(
        tmp.path(),
        "a",
        NodeOpts {
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        9,
        1,
    )
    .await;
    wait_for(
        "legacy ambiguity remains visible and fences admission",
        15,
        || {
            let (code, body) = http(
                &node.url,
                "POST",
                "/api/v1/jobs?name=refused",
                b"invalid NZB",
            );
            code == 503 && body.contains("legacy admission") && body.contains("receipt preserved")
        },
    )
    .await;
    let preserved: nzbd_state::QueueSnapshotDoc =
        serde_json::from_slice(&std::fs::read(state.join("queue.json")).unwrap()).unwrap();
    assert_eq!(preserved.url_admission_receipts.len(), 1);
    assert_eq!(
        serde_json::to_value(&preserved.url_admission_receipts[0]).unwrap(),
        serde_json::to_value(&receipt).unwrap()
    );
    assert!(node.runtime.engine.snapshot().jobs.is_empty());
    node.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn delivery_receipts_and_copy_acks_survive_actual_majority_authority_change() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let tmp = tempfile::tempdir().unwrap();
    // Pre-upgrade receipt has no live job. It must be captured before the
    // new authority rewrites the old shared queue snapshot.
    let mut legacy:nzbd_state::UrlAdmissionReceipt=serde_json::from_value(serde_json::json!({"protocolVersion":1,"requestToken":"retired-before-upgrade","submissionSha256":"original-submission","jobId":700,"generation":"original-generation","runnerInstanceId":"original-public-instance","outcome":"completed","terminalOutcome":"SUCCESS","custodyState":"curator","sourceCleanupStatus":"removed"})).unwrap();
    legacy.generation = format!(
        "{}-url-700-{:x}",
        legacy.runner_instance_id,
        sha2::Sha256::digest(legacy.request_token.as_bytes())
    );
    let retired = nzbd_types::Job {
        id: nzbd_types::JobId(700),
        kind: nzbd_types::JobKind::Nzb,
        name: "retired legacy".into(),
        dir_name: "retired-legacy".into(),
        name_provisional: false,
        delivery_lifecycle: true,
        queued_at_unix: 1,
        original_name: String::new(),
        category: None,
        priority: 0,
        dupe: Default::default(),
        params: vec![("monarr-transfer".into(), legacy.request_token.clone())],
        files: Vec::new(),
        totals: Default::default(),
        status: nzbd_types::JobStatus::Deleted,
        torrent: None,
        stages: Vec::new(),
    };
    let old = nzbd_state::QueueSnapshotDoc {
        jobs: vec![retired],
        url_admission_receipts: vec![legacy.clone()],
        next_job_id: 700,
        next_file_id: 900,
        ..Default::default()
    };
    nzbd_state::SnapshotStore::open(&tmp.path().join(".nzbd-cluster"))
        .unwrap()
        .save(&old)
        .unwrap();
    let bytes = prng_bytes(83, 90_000);
    let post = build_post("cluster-ready", &[("episode.mkv", bytes.clone())], 20_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
    let topology = control_topology(3);
    let opts = |priority| NodeOpts {
        coordinator: true,
        priority,
        download: true,
        max_download_jobs: 2,
        post_process: true,
        min_free_disk_bytes: 0,
    };
    let (authority, downloader, post_worker) = tokio::join!(
        start_node_with_control(
            tmp.path(),
            "a",
            opts(0),
            ns.port(),
            4,
            nzbd_api::AuthConfig::default(),
            Some(topology[0].clone())
        ),
        start_node_with_control(
            tmp.path(),
            "b",
            opts(1),
            ns.port(),
            4,
            nzbd_api::AuthConfig::default(),
            Some(topology[1].clone())
        ),
        start_node_with_control(
            tmp.path(),
            "c",
            opts(2),
            ns.port(),
            4,
            nzbd_api::AuthConfig::default(),
            Some(topology[2].clone())
        )
    );
    wait_for("three nodes and catalog adoption", 20, || {
        let cluster = get_json(&authority.url, "/api/v1/cluster");
        let (code, body) = http(
            &authority.url,
            "GET",
            "/api/v1/deliveries?after=0&includeAccepted=true",
            b"",
        );
        cluster["nodes"]
            .as_array()
            .is_some_and(|nodes| nodes.len() == 3)
            && code == 200
            && serde_json::from_str::<serde_json::Value>(&body)
                .is_ok_and(|page| page["catalogAdoptionComplete"] == true)
    })
    .await;
    let descriptor_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let descriptor_port = descriptor_listener.local_addr().unwrap().port();
    let descriptor = post.nzb.clone();
    let descriptor_app = axum::Router::new()
        .route(
            "/nzb",
            axum::routing::get(move || {
                let descriptor = descriptor.clone();
                async move { descriptor }
            }),
        )
        .route("/bad-nzb", axum::routing::get(|| async { "not an NZB" }))
        .route(
            "/slow-nzb",
            axum::routing::get(|| async { std::future::pending::<String>().await }),
        );
    let descriptor_server = tokio::spawn(async move {
        axum::serve(descriptor_listener, descriptor_app)
            .await
            .unwrap();
    });
    let admission_path=format!("/api/v1/jobs?name=cluster-ready&url=http%3A%2F%2F127.0.0.1%3A{descriptor_port}%2Fnzb&params=%7B%22monarr-delivery%22%3A%221%22%2C%22monarr-transfer%22%3A%22cluster-ready-token%22%7D");
    let admitted = post_until_created("lifecycle URL admission", 15, || {
        http(&authority.url, "POST", &admission_path, b"")
    })
    .await;
    let original_id = serde_json::from_str::<serde_json::Value>(&admitted).unwrap()["id"]
        .as_u64()
        .unwrap();
    let original_admission = get_json(
        &authority.url,
        "/api/v1/request-admissions/cluster-ready-token",
    );
    assert_eq!(original_admission["jobId"], original_id);
    assert_eq!(
        get_json(
            &authority.url,
            "/api/v1/request-admissions/retired-before-upgrade"
        ),
        serde_json::to_value(&legacy).unwrap()
    );
    assert!(original_id > 700);
    assert_eq!(
        original_admission["runnerInstanceId"],
        legacy.runner_instance_id
    );
    let debug_started = tokio::time::Instant::now();
    wait_for("remote POST creates cluster-visible READY", 45, || {
        if debug_started.elapsed().as_secs() % 10 == 9 {
            eprintln!(
                "authority={} downloader={} pp={}",
                get_json(&authority.url, "/api/v1/jobs"),
                serde_json::to_value(&downloader.runtime.engine.snapshot().jobs).unwrap(),
                serde_json::to_value(&post_worker.runtime.engine.snapshot().jobs).unwrap()
            );
        }
        let (code, body) = http(
            &authority.url,
            "GET",
            "/api/v1/deliveries?after=0&includeAccepted=true",
            b"",
        );
        code == 200
            && serde_json::from_str::<serde_json::Value>(&body).is_ok_and(|page| {
                page["deliveries"].as_array().is_some_and(|rows| {
                    rows.iter()
                        .any(|row| row["requestToken"] == "cluster-ready-token")
                })
            })
    })
    .await;
    let page = get_json(
        &authority.url,
        "/api/v1/deliveries?after=0&includeAccepted=true",
    );
    let delivery = page["deliveries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["requestToken"] == "cluster-ready-token")
        .unwrap()
        .clone();
    assert!(delivery["runnerInstanceId"].is_string());
    assert!(delivery["runnerStages"]["nodeId"].is_string());
    assert!(delivery["sourceCopies"]
        .as_array()
        .is_some_and(|copies| copies.len() >= 3));
    let path = std::path::PathBuf::from(delivery["publicationPath"].as_str().unwrap());
    assert_eq!(
        std::fs::read(path.join(delivery["files"][0]["path"].as_str().unwrap())).unwrap(),
        bytes
    );
    let id = delivery["deliveryId"].as_str().unwrap();
    let acceptance=serde_json::to_vec(&serde_json::json!({"protocolVersion":1,"publicationGeneration":delivery["publicationGeneration"],"manifestDigest":delivery["manifestDigest"],"consumerReceiptId":"curator-accepted-cluster"})).unwrap();
    let (status, accepted) = http_with_headers(
        &post_worker.url,
        "POST",
        &format!("/api/v1/deliveries/{id}/accept"),
        &acceptance,
        &[("Content-Type", "application/json")],
    );
    // Contract §2.5 (v2, runner M3): there is no acceptance. The route is
    // gone, and answers so through the worker's proxy as well; source cleanup
    // does not wait for a consumer receipt.
    assert_eq!(status, 410, "acceptance through worker proxy: {accepted}");
    assert!(accepted.contains("acceptance was removed"), "{accepted}");
    // Consumer is entitled to remove its transferred publication immediately.
    std::fs::remove_dir_all(&path).unwrap();
    let last_cleanup_progress = std::cell::RefCell::new(String::new());
    wait_for(
        "all exact successful source copies acknowledged removed",
        30,
        || {
            let value = get_json(&authority.url, &format!("/api/v1/deliveries/{id}"));
            let progress=serde_json::json!({
                "status":value["sourceCleanup"]["status"],
                "error":value["sourceCleanup"]["error"].as_str().map(|error|error.chars().take(1024).collect::<String>()),
                "copies":value["sourceCopies"].as_array().map(|copies|copies.iter().take(16).map(|copy|serde_json::json!({
                    "id":copy["id"],"ownerNode":copy["ownerNode"],"status":copy["status"],"attempts":copy["attempts"],"nextRetryAt":copy["nextRetryAt"],
                    "error":copy["error"].as_str().map(|error|error.chars().take(1024).collect::<String>())
                })).collect::<Vec<_>>())
            }).to_string();
            if *last_cleanup_progress.borrow()!=progress {eprintln!("exact source cleanup: {progress}");*last_cleanup_progress.borrow_mut()=progress;}
            value["sourceCleanup"]["status"] == "succeeded"
                && value["sourceCopies"].as_array().is_some_and(|copies| {
                    !copies.is_empty() && copies.iter().all(|copy| copy["status"] == "removed")
                })
        },
    )
    .await;
    let (code, retried) = http_with_headers(
        &authority.url,
        "POST",
        &format!("/api/v1/deliveries/{id}/source-cleanup/retry"),
        b"",
        &[],
    );
    assert_eq!(code, 200, "{retried}");
    let retried: serde_json::Value = serde_json::from_str(&retried).unwrap();
    assert_eq!(retried["sourceCleanup"]["status"], "succeeded");
    assert!(
        !tmp.path().join("complete/cluster-ready").exists(),
        "download source must drain independently of the publication: {:?}; receipt {}",
        std::fs::read_dir(tmp.path().join("complete/cluster-ready"))
            .ok()
            .map(|entries| entries
                .flatten()
                .map(|entry| entry.file_name())
                .collect::<Vec<_>>()),
        get_json(&authority.url, &format!("/api/v1/deliveries/{id}"))
    );
    assert!(
        manifest_paths(&tmp.path().join(".nzbd-cluster/generations")).is_empty(),
        "all exact sealed duplicates must drain"
    );
    // Admission replay outlives its live queue row and must preserve its exact
    // original identity on another authority after normal history retirement.
    wait_for("completed job retired before authority change", 15, || {
        !authority
            .runtime
            .engine
            .snapshot()
            .jobs
            .iter()
            .any(|job| u64::from(job.id.0) == original_id)
    })
    .await;
    let cancel_path=format!("/api/v1/jobs?name=cancel-before-fetch&paused=true&url=http%3A%2F%2F127.0.0.1%3A{descriptor_port}%2Fslow-nzb&params=%7B%22monarr-delivery%22%3A%221%22%2C%22monarr-transfer%22%3A%22cancel-before-fetch%22%7D");
    let cancelled = post_until_created("URL to cancel before payload", 15, || {
        http(&authority.url, "POST", &cancel_path, b"")
    })
    .await;
    let cancelled_id = serde_json::from_str::<serde_json::Value>(&cancelled).unwrap()["id"]
        .as_u64()
        .unwrap();
    let (code, body) = http(
        &authority.url,
        "POST",
        &format!("/api/v1/jobs/{cancelled_id}/actions/delete"),
        b"",
    );
    assert_eq!(code, 200, "{body}");
    let cancelled_receipt = get_json(
        &authority.url,
        "/api/v1/request-admissions/cancel-before-fetch",
    );
    assert_eq!(cancelled_receipt["outcome"], "deleted");
    assert_eq!(cancelled_receipt["terminalOutcome"], "DELETED");
    let failure_path=format!("/api/v1/jobs?name=bad-fetch&url=http%3A%2F%2F127.0.0.1%3A{descriptor_port}%2Fbad-nzb&params=%7B%22monarr-delivery%22%3A%221%22%2C%22monarr-transfer%22%3A%22failed-fetch%22%7D");
    let failed = post_until_created("terminal failed URL descriptor", 15, || {
        http(&authority.url, "POST", &failure_path, b"")
    })
    .await;
    let failed_id = serde_json::from_str::<serde_json::Value>(&failed).unwrap()["id"]
        .as_u64()
        .unwrap();
    wait_for("failed URL durable outcome", 15, || {
        get_json(&authority.url, "/api/v1/request-admissions/failed-fetch")["outcome"] == "failed"
    })
    .await;
    let (code, body) = http(
        &authority.url,
        "POST",
        &format!("/api/v1/jobs/{failed_id}/actions/delete"),
        b"",
    );
    assert_eq!(code, 200, "{body}");
    let failed_receipt = get_json(&authority.url, "/api/v1/request-admissions/failed-fetch");
    assert_eq!(failed_receipt["outcome"], "failed");
    assert_eq!(failed_receipt["terminalOutcome"], "FAILURE");
    let initial = get_json(&authority.url, "/api/v1/cluster")["leader"]["node"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut nodes = vec![authority, downloader, post_worker];
    let index = nodes.iter().position(|node| node.name == initial).unwrap();
    nodes.remove(index).kill().await;
    wait_for("different majority coordinator takes office", 25, || {
        nodes
            .iter()
            .any(|node| get_json(&node.url, "/api/v1/cluster")["is_leader"] == true)
    })
    .await;
    let successor = nodes
        .iter()
        .find(|node| get_json(&node.url, "/api/v1/cluster")["is_leader"] == true)
        .unwrap();
    assert_ne!(successor.name, initial);
    wait_for(
        "authoritative receipt recovery without payload rehash",
        25,
        || {
            let (code, body) = http(
                &successor.url,
                "GET",
                &format!("/api/v1/deliveries/{id}"),
                b"",
            );
            // Contract v2: accept is a no-op, so the replayed row carries no
            // consumer receipt; recovery is the row answering unchanged.
            code == 200 && body.contains("deliveryId")
        },
    )
    .await;
    let replay = get_json(&successor.url, &format!("/api/v1/deliveries/{id}"));
    for key in [
        "deliveryId",
        "runnerInstanceId",
        "sequence",
        "publicationGeneration",
        "manifestDigest",
        "files",
        "runnerStages",
    ] {
        assert_eq!(
            replay[key], delivery[key],
            "immutable {key} changed across authority restart"
        );
    }
    assert!(
        !path.exists(),
        "replay must not republish a consumer-removed path"
    );
    assert!(
        replay["consumerReceiptId"].is_null(),
        "accept records nothing"
    );
    assert_eq!(replay["sourceCleanup"]["status"], "succeeded");
    assert!(replay["sourceCopies"]
        .as_array()
        .unwrap()
        .iter()
        .all(|copy| copy["status"] == "removed"));
    assert_eq!(
        get_json(
            &successor.url,
            "/api/v1/request-admissions/retired-before-upgrade"
        ),
        serde_json::to_value(&legacy).unwrap(),
        "preupgrade retired receipt must survive different authority"
    );
    assert_eq!(
        get_json(
            &successor.url,
            "/api/v1/request-admissions/cancel-before-fetch"
        ),
        cancelled_receipt,
        "deleted prepublication receipt regressed after different authority"
    );
    assert_eq!(
        get_json(&successor.url, "/api/v1/request-admissions/failed-fetch"),
        failed_receipt,
        "terminal URL failure regressed after retirement/failover"
    );
    let replay_admission = get_json(
        &successor.url,
        "/api/v1/request-admissions/cluster-ready-token",
    );
    for key in [
        "requestToken",
        "submissionSha256",
        "jobId",
        "generation",
        "runnerInstanceId",
    ] {
        assert_eq!(
            replay_admission[key], original_admission[key],
            "admission {key} changed across authority"
        );
    }
    let replay = post_until_created("same URL token on successor", 15, || {
        http(&successor.url, "POST", &admission_path, b"")
    })
    .await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&replay).unwrap()["id"],
        original_id
    );
    assert!(
        !successor
            .runtime
            .engine
            .snapshot()
            .jobs
            .iter()
            .any(|job| u64::from(job.id.0) == original_id),
        "accepted retired token must not recreate queue work"
    );
    let next = post_until_created("fresh job after retired highwater", 15, || {
        http(
            &successor.url,
            "POST",
            "/api/v1/jobs?name=next-after-retirement&paused=true",
            post.nzb.as_bytes(),
        )
    })
    .await;
    assert!(
        serde_json::from_str::<serde_json::Value>(&next).unwrap()["id"]
            .as_u64()
            .unwrap()
            > original_id
    );
    for node in nodes {
        node.kill().await;
    }
    descriptor_server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn worker_restart_regrant_preserves_retry_deadline_and_private_generation() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    if !require_tool("7z") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let archive = tmp.path().join("archive-fixture");
    std::fs::create_dir(&archive).unwrap();
    std::fs::write(archive.join("episode.mkv"), prng_bytes(98, 90_000)).unwrap();
    let made = std::process::Command::new("7z")
        .args(["a", "-tzip", "payload.zip", "episode.mkv"])
        .current_dir(&archive)
        .output()
        .unwrap();
    assert!(made.status.success());
    let zipped = std::fs::read(archive.join("payload.zip")).unwrap();
    let post = build_post("retry-regrant", &[("payload.zip", zipped)], 20_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
    std::fs::write(
        tmp.path().join("test-sevenzip-c"),
        "/no-such-retry-extractor",
    )
    .unwrap();
    let authority = start_node(
        tmp.path(),
        "a",
        NodeOpts {
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;
    let downloader = start_node(
        tmp.path(),
        "b",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: true,
            max_download_jobs: 2,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;
    let control = control_topology(1).remove(0);
    let opts = || NodeOpts {
        coordinator: false,
        priority: 9,
        download: false,
        max_download_jobs: 0,
        post_process: true,
        min_free_disk_bytes: 0,
    };
    let worker = start_node_with_control(
        tmp.path(),
        "c",
        opts(),
        ns.port(),
        4,
        nzbd_api::AuthConfig::default(),
        Some(control.clone()),
    )
    .await;
    wait_for("retry fixture authority", 15, || {
        get_json(&authority.url, "/api/v1/cluster")["is_leader"] == true
    })
    .await;
    post_until_created("retry fixture admission",15,||http(&authority.url,"POST","/api/v1/jobs?name=retry-regrant&params=%7B%22monarr-delivery%22%3A%221%22%2C%22monarr-transfer%22%3A%22retry-regrant-token%22%7D",post.nzb.as_bytes())).await;
    let retry_debug = std::cell::Cell::new(false);
    let retry_started = tokio::time::Instant::now();
    wait_for("worker automatic extractor retry held", 25, || {
        if retry_started.elapsed().as_secs() > 12 && !retry_debug.replace(true) {
            eprintln!(
                "retry fixture a={} b={} c={}",
                get_json(&authority.url, "/api/v1/jobs"),
                serde_json::to_value(&downloader.runtime.engine.snapshot().jobs).unwrap(),
                serde_json::to_value(&worker.runtime.engine.snapshot().jobs).unwrap()
            );
        }
        worker.runtime.engine.snapshot().jobs.iter().any(|job| {
            job.control
                .as_ref()
                .is_some_and(|control| control.lifecycle == "held")
        })
    })
    .await;
    let held = worker
        .runtime
        .engine
        .snapshot()
        .jobs
        .iter()
        .find(|job| {
            job.control
                .as_ref()
                .is_some_and(|control| control.lifecycle == "held")
        })
        .unwrap()
        .clone();
    let held = worker
        .runtime
        .engine
        .export_job(held.id)
        .await
        .unwrap()
        .unwrap();
    let before = held.control().unwrap();
    let context = nzbd_state::artifacts::WorkerControlContext::for_job(&held).unwrap();
    let source = worker
        .runtime
        .engine
        .artifacts()
        .for_job(held.id.0)
        .unwrap()
        .unwrap();
    assert!(source.path.join("payload.zip").is_file());
    assert!(before.next_retry_at_unix.is_some());
    assert!(before.blocked_since_unix.is_some());
    worker.kill().await;
    let restarted = start_node_with_control(
        tmp.path(),
        "c",
        opts(),
        ns.port(),
        4,
        nzbd_api::AuthConfig::default(),
        Some(control),
    )
    .await;
    wait_for(
        "same operation regranted after actual worker restart",
        20,
        || {
            restarted.runtime.engine.snapshot().jobs.iter().any(|job| {
                job.control
                    .as_ref()
                    .is_some_and(|control| control.lifecycle == "held")
            })
        },
    )
    .await;
    // The held regrant becomes visible before main's imported source
    // verification is exportable. Read only after that exact barrier clears.
    let export_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let recovered = loop {
        match restarted.runtime.engine.export_job(held.id).await {
            Ok(Some(job))=>break job,
            Err(nzbd_engine::EngineError::Lifecycle(error)) if error=="job verification or writer retirement is still in progress; retry later"=>{
                assert!(tokio::time::Instant::now()<export_deadline,"regranted held source did not become exportable: {error}");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            result=>panic!("regranted held source was not preserved: {result:?}"),
        }
    };
    let after = recovered.control().unwrap();
    let fresh = nzbd_state::artifacts::WorkerControlContext::for_job(&recovered).unwrap();
    assert_ne!(fresh.lease_id, context.lease_id);
    assert_eq!(fresh.job_incarnation, context.job_incarnation);
    assert_eq!(fresh.job_revision, context.job_revision);
    assert_eq!(fresh.control_revision, context.control_revision);
    assert_eq!(after.next_retry_at_unix, before.next_retry_at_unix);
    assert_eq!(after.blocked_since_unix, before.blocked_since_unix);
    assert_eq!(after.retry_attempt, before.retry_attempt);
    let adopted = restarted
        .runtime
        .engine
        .artifacts()
        .for_job(held.id.0)
        .unwrap()
        .unwrap();
    assert_eq!(adopted.id, source.id);
    assert_eq!(adopted.generation, source.generation);
    assert_eq!(adopted.path, source.path);
    let private_parent = source.path.parent().unwrap();
    assert_eq!(
        std::fs::read_dir(private_parent)
            .unwrap()
            .flatten()
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir())
                && entry.file_name().to_string_lossy().starts_with("fence-"))
            .count(),
        1,
        "new lease must reuse exact private generation rather than leak another copied directory"
    );
    restarted.kill().await;
    downloader.kill().await;
    authority.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn terminal_unusable_cluster_completion_follows_failure_action_and_drains_sealed_generations()
{
    terminal_empty_cluster_fixture(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn terminal_junk_with_explicit_keep_retains_owned_sources_and_reports_custody() {
    terminal_empty_cluster_fixture(true).await;
}
async fn terminal_empty_cluster_fixture(keep: bool) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let tmp = tempfile::tempdir().unwrap();
    let post = build_post(
        "empty-disposition",
        &[("readme.txt", b"disposable release notes".to_vec())],
        20_000,
    );
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let descriptor_port = listener.local_addr().unwrap().port();
    let nzb = post.nzb.clone();
    let app = axum::Router::new().route(
        "/nzb",
        axum::routing::get(move || {
            let nzb = nzb.clone();
            async move { nzb }
        }),
    );
    let descriptor_server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let authority = start_node(
        tmp.path(),
        "a",
        NodeOpts {
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;
    let downloader = start_node(
        tmp.path(),
        "b",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: true,
            max_download_jobs: 2,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;
    let worker = start_node(
        tmp.path(),
        "c",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: false,
            max_download_jobs: 0,
            post_process: true,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;
    wait_for("empty outcome authority", 15, || {
        get_json(&authority.url, "/api/v1/cluster")["is_leader"] == true
    })
    .await;
    let params="%7B%22monarr-delivery%22%3A%221%22%2C%22monarr-transfer%22%3A%22empty-disposition-token%22%7D";
    let admission_path=format!("/api/v1/jobs?name=empty-disposition&paused=true&url=http%3A%2F%2F127.0.0.1%3A{descriptor_port}%2Fnzb&params={params}");
    let admitted = post_until_created("empty outcome URL admission", 15, || {
        http(&authority.url, "POST", &admission_path, b"")
    })
    .await;
    let job_id = serde_json::from_str::<serde_json::Value>(&admitted).unwrap()["id"]
        .as_u64()
        .unwrap();
    // A durable URL admission is still Fetching. AddPaused takes effect
    // only after its descriptor is resolved and verified; Resume is not a
    // valid action on the provisional Fetching row.
    async fn verified_paused(engine: &nzbd_engine::EngineHandle, id: nzbd_types::JobId) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            match engine.export_job(id).await {
                Ok(Some(job)) if job.status == nzbd_types::JobStatus::Paused => break,
                Ok(Some(job)) => assert_eq!(job.status, nzbd_types::JobStatus::Fetching,
                    "paused admission reached an unexpected state"),
                Ok(None) => panic!("paused admission disappeared before Resume"),
                Err(nzbd_engine::EngineError::Lifecycle(message))
                    if message == "job verification or writer retirement is still in progress; retry later" => {},
                Err(error) => panic!("export paused admission: {error}"),
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "URL admission did not resolve into a verified Paused job"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    verified_paused(&authority.runtime.engine, nzbd_types::JobId(job_id as u32)).await;
    if keep {
        let (code, body) = http_with_headers(
            &authority.url,
            "PUT",
            &format!("/api/v1/jobs/{job_id}/failure-keep"),
            br#"{"keep":true}"#,
            &[("content-type", "application/json")],
        );
        assert!(matches!(code, 200 | 202), "Keep setter: {code}: {body}");
        wait_for("exact source owner confirms Keep before Resume", 15, || {
            let (code, body) = http(
                &authority.url,
                "GET",
                &format!("/api/v1/jobs/{job_id}/failure-keep"),
                b"",
            );
            assert!(
                matches!(code, 200 | 202),
                "Keep observation: {code}: {body}"
            );
            let state: serde_json::Value = serde_json::from_str(&body).unwrap();
            code == 200
                && state["available"] == true
                && state["keep"] == true
                && state["applied"] == true
        })
        .await;
        // Keep updates the replicated job and restores its local projection,
        // which starts a fresh payload verification before controls may run.
        verified_paused(&authority.runtime.engine, nzbd_types::JobId(job_id as u32)).await;
    }
    // Reconciliation can start another verification after the readiness
    // observation above. Retry only this job's exact pre-handler verification
    // refusal while it still resolves to Paused; never replay an accepted Resume.
    let resume_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let (code, body) = http(
            &authority.url,
            "POST",
            &format!("/api/v1/jobs/{job_id}/actions/resume"),
            b"",
        );
        if code == 200 {
            break;
        }
        let expected = serde_json::json!({
            "error": "queue_projection_unavailable",
            "detail": format!("export queue projection for job {job_id}: file lifecycle: job verification or writer retirement is still in progress; retry later"),
        });
        assert!(
            code == 503
                && serde_json::from_str::<serde_json::Value>(&body)
                    .ok()
                    .as_ref()
                    == Some(&expected),
            "Resume: {code}: {body}"
        );
        assert!(
            tokio::time::Instant::now() < resume_deadline,
            "Resume remained blocked by verification"
        );
        verified_paused(&authority.runtime.engine, nzbd_types::JobId(job_id as u32)).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let history = tmp.path().join(".nzbd-cluster/history/history.c.jsonl");
    let empty_debug = std::cell::Cell::new(false);
    let empty_started = tokio::time::Instant::now();
    wait_for(
        "failure history names private exact cleanup receipt",
        25,
        || {
            if empty_started.elapsed().as_secs() > 12 && !empty_debug.replace(true) {
                eprintln!(
                    "empty a={} c={} history={}",
                    get_json(&authority.url, "/api/v1/jobs"),
                    serde_json::to_value(&worker.runtime.engine.snapshot().jobs).unwrap(),
                    std::fs::read_to_string(&history).unwrap_or_default()
                );
            }
            std::fs::read_to_string(&history)
                .unwrap_or_default()
                .contains("Cluster:CleanupId")
        },
    )
    .await;
    let entry = std::fs::read_to_string(&history)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<nzbd_state::HistoryEntry>(line).ok())
        .find(|row| row.params.iter().any(|(key, _)| key == "Cluster:CleanupId"))
        .unwrap();
    assert!(entry.status.contains("FAILURE"));
    assert_eq!(entry.final_dir, None);
    let id = entry
        .params
        .iter()
        .find(|(key, _)| key == "Cluster:CleanupId")
        .unwrap()
        .1
        .clone();
    let cleanup_debug = std::cell::Cell::new(false);
    let cleanup_started = tokio::time::Instant::now();
    wait_for("exact terminal source disposition acknowledged",30,||{
        let (code,body)=http(&authority.url,"GET",&format!("/api/v1/deliveries/{id}"),b"");
        if cleanup_started.elapsed().as_secs()>12&&!cleanup_debug.replace(true){eprintln!("private copy disposition code={code} {body}; authority source={:?}; worker source={:?}",authority.runtime.engine.artifacts().for_job(entry.job.0).ok().flatten().map(|a|(a.id,a.generation,a.path,a.state,a.error)),worker.runtime.engine.artifacts().for_job(entry.job.0).ok().flatten().map(|a|(a.id,a.generation,a.path,a.state,a.error)));}
        // Contract v2 §2.2 (0.9.0): an empty/junk-only completion is
        // `failed/publish`. Kept, its raw copies are retained; a curator-
        // admitted job without Keep follows `failure_action` (delete) like
        // single-node disposal, so every copy is removed.
        let expected = if keep { "blocked" } else { "succeeded" };
        code==200&&serde_json::from_str::<serde_json::Value>(&body).is_ok_and(|row|row["cleanupOnly"]==true&&row["sourceCleanup"]["status"]==expected)
    }).await;
    let raw = authority
        .runtime
        .engine
        .artifacts()
        .for_job(entry.job.0)
        .unwrap()
        .unwrap();
    assert_eq!(
        raw.path.exists(),
        keep,
        "a kept publish failure's raw custody is retained, an unkept one deleted: path={} state={} keep={} detail={}",
        raw.path.display(),
        raw.state,
        raw.keep_on_failure,
        get_json(&authority.url, &format!("/api/v1/deliveries/{id}"))
    );
    assert_eq!(raw.keep_on_failure, keep);
    assert_eq!(raw.state == "deleted", !keep);
    let remaining = manifest_paths(&tmp.path().join(".nzbd-cluster/generations"));
    let remaining_files = remaining
        .iter()
        .map(|path| {
            let value: serde_json::Value =
                serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            (path, value["files"].clone())
        })
        .collect::<Vec<_>>();
    assert!(
        remaining.is_empty(),
        "all redundant sealed transfer copies must drain: remaining={remaining_files:?}; cleanup={}",
        get_json(&authority.url, &format!("/api/v1/deliveries/{id}"))
    );
    let admission = get_json(
        &authority.url,
        "/api/v1/request-admissions/empty-disposition-token",
    );
    assert_eq!(admission["outcome"], "failed");
    assert_eq!(admission["terminalOutcome"], entry.status);
    assert_eq!(
        admission["custodyState"],
        if keep {
            "kept_by_user"
        } else {
            "deleted_by_runner"
        }
    );
    assert_eq!(
        admission["sourceCleanupStatus"],
        if keep { "retained" } else { "removed" }
    );
    assert_eq!(admission["keepSourceOnFailure"], keep);
    assert!(admission["publicationPath"].is_null());
    let discovery = get_json(
        &authority.url,
        "/api/v1/deliveries?after=0&includeAccepted=true",
    );
    assert!(discovery["deliveries"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["deliveryId"] != id));
    let detail = get_json(&authority.url, &format!("/api/v1/deliveries/{id}"));
    assert!(
        detail.get("files").is_none(),
        "private cleanup detail must not fabricate an empty consumer manifest"
    );
    let (code, _) = http(
        &authority.url,
        "POST",
        &format!("/api/v1/deliveries/{id}/accept"),
        b"{}",
    );
    // Contract §2.5: there is no acceptance; the route is gone for every id
    // (it answered 409 for a private cleanup row under v1).
    assert_eq!(code, 410);
    worker.kill().await;
    downloader.kill().await;
    authority.kill().await;
    descriptor_server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn completed_text_document_is_one_publish_failure_with_bytes_retained() {
    let tmp = tempfile::tempdir().unwrap();
    let text=b"An intended plain text book. Chapter one. This complete document must remain available for review.".to_vec();
    let post = build_post("Text.Book", &[("Novel.txt", text.clone())], 20_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
    let authority = start_node(
        tmp.path(),
        "a",
        NodeOpts {
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;
    let worker = start_node(
        tmp.path(),
        "b",
        NodeOpts {
            coordinator: false,
            priority: 9,
            download: true,
            max_download_jobs: 2,
            post_process: true,
            min_free_disk_bytes: 0,
        },
        ns.port(),
        4,
    )
    .await;
    wait_for("text fixture authority", 15, || {
        get_json(&authority.url, "/api/v1/cluster")["is_leader"] == true
    })
    .await;
    post_until_created("text admission",15,||http(&authority.url,"POST","/api/v1/jobs?name=Text.Book&params=%7B%22monarr-delivery%22%3A%221%22%2C%22monarr-transfer%22%3A%22intended-text-token%22%7D",post.nzb.as_bytes())).await;
    // Contract v2 §2.2/§2.3: a completed download with nothing publishable
    // is the terminal `failed/publish` (no review hold any more), recorded
    // once — here by the worker, which holds the history store because the
    // authority runs no post-processing — and its bytes always park.
    wait_for("unsupported text ends failed/publish", 25, || {
        [&authority, &worker].iter().any(|node| {
            node.runtime
                .engine
                .artifacts()
                .terminal(1)
                .unwrap()
                .is_some_and(|terminal| terminal.emitted)
        })
    })
    .await;
    let recorded = [&authority, &worker]
        .iter()
        .filter_map(|node| node.runtime.engine.artifacts().terminal(1).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(recorded.len(), 1, "exactly one terminal: {recorded:?}");
    let terminal = &recorded[0];
    assert_eq!(terminal.reason, Some(nzbd_types::FailureReason::Publish));
    assert!(!terminal.blames_release);
    assert!(
        terminal
            .message
            .as_deref()
            .is_some_and(|message| message.contains("supported publication format")),
        "{:?}",
        terminal.message
    );
    let held = nzbd_types::JobId(1);
    let source = worker
        .runtime
        .engine
        .artifacts()
        .for_job(held.0)
        .unwrap()
        .unwrap();
    assert!(source.path.exists());
    assert!(std::fs::read_dir(&source.path)
        .unwrap()
        .flatten()
        .any(|entry| std::fs::read(entry.path()).ok().as_deref() == Some(text.as_slice())));
    assert!(get_json(
        &authority.url,
        "/api/v1/deliveries?after=0&includeAccepted=true"
    )["deliveries"]
        .as_array()
        .unwrap()
        .is_empty());
    worker.kill().await;
    authority.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn automatic_hold_release_is_replicated_before_different_authority_adopts() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let tmp = tempfile::tempdir().unwrap();
    let topology = control_topology(3);
    let opts = |priority| NodeOpts {
        coordinator: true,
        priority,
        download: false,
        max_download_jobs: 0,
        post_process: false,
        min_free_disk_bytes: 0,
    };
    let (a, b, c) = tokio::join!(
        start_node_with_control(
            tmp.path(),
            "a",
            opts(0),
            9,
            0,
            nzbd_api::AuthConfig::default(),
            Some(topology[0].clone())
        ),
        start_node_with_control(
            tmp.path(),
            "b",
            opts(1),
            9,
            0,
            nzbd_api::AuthConfig::default(),
            Some(topology[1].clone())
        ),
        start_node_with_control(
            tmp.path(),
            "c",
            opts(2),
            9,
            0,
            nzbd_api::AuthConfig::default(),
            Some(topology[2].clone())
        )
    );
    let mut nodes = vec![a, b, c];
    wait_for("initial coordinator ready", 20, || {
        nodes.iter().filter(|node| node.runtime.is_leader()).count() == 1
            && nodes
                .iter()
                .find(|node| node.runtime.is_leader())
                .is_some_and(|node| {
                    http(&node.url, "GET", "/api/v1/deliveries?after=0", b"").0 == 200
                })
    })
    .await;
    let initial = nodes
        .iter()
        .position(|node| node.runtime.is_leader())
        .unwrap();
    let a = &nodes[initial];
    let post = build_post(
        "automatic-recovery",
        &[("episode.mkv", prng_bytes(202, 4096))],
        4096,
    );
    let body = post_until_created("automatic recovery admission", 20, || {
        http(
            &a.url,
            "POST",
            "/api/v1/jobs?name=automatic-recovery",
            post.nzb.as_bytes(),
        )
    })
    .await;
    let id = nzbd_types::JobId(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
            .as_u64()
            .unwrap() as u32,
    );
    async fn verified_job(
        engine: &nzbd_engine::EngineHandle,
        id: nzbd_types::JobId,
    ) -> nzbd_types::Job {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            match engine.export_job(id).await {
                Ok(job) => return job.expect("admitted job remains present"),
                Err(nzbd_engine::EngineError::Lifecycle(message))
                    if message == "job verification or writer retirement is still in progress; retry later" =>
                {
                    assert!(tokio::time::Instant::now() < deadline,
                        "job verification did not finish: {message}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => panic!("export admitted job: {error}"),
            }
        }
    }
    let job = verified_job(&a.runtime.engine, id).await;
    let root = tmp.path().join("complete");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(nzbd_engine::queue::job_dir_name(&job));
    let source = a
        .runtime
        .engine
        .artifacts()
        .allocate_for_delivery(id.0, &root, &path, true, false)
        .unwrap();
    assert!(a
        .runtime
        .engine
        .hold_job(
            id,
            nzbd_types::HoldCause::Allocation,
            "download",
            "test allocation retry"
        )
        .await
        .unwrap());
    let held = a
        .runtime
        .engine
        .export_job(id)
        .await
        .unwrap()
        .unwrap()
        .control()
        .unwrap();
    assert_eq!(held.lifecycle, "held");
    let due = held
        .next_retry_at_unix
        .expect("automatic hold persists deadline");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    assert!(
        due - now <= 65,
        "fixture must use bounded production retry schedule"
    );
    // The real authority-only recovery task releases it; scheduler must commit
    // the proven transition before a successor can observe running work.
    wait_for("automatic recovery and replicated projection", 80, || {
        let snap = a.runtime.engine.snapshot();
        snap.jobs
            .iter()
            .find(|job| job.id == id)
            .is_some_and(|job| job.status == nzbd_types::JobStatus::Queued)
            && !snap.pending_automatic_controls.contains(&id)
    })
    .await;
    let released = a.runtime.engine.export_job(id).await.unwrap().unwrap();
    let control = released.control().unwrap();
    assert_eq!(control.lifecycle, "running");
    assert_eq!(control.retry_attempt, held.retry_attempt);
    assert_eq!(control.blocked_since_unix, held.blocked_since_unix);
    assert!(released.params.iter().any(|(key,value)|key=="*Cluster:AuthoritySourceCopy" && value.contains(&source.id)),"raw custody fact must be durable before grant eligibility");
    let prior_name = a.name.clone();
    nodes.remove(initial).kill().await;
    wait_for("different coordinator adopts automatic result", 25, || {
        nodes.iter().any(|node| node.runtime.is_leader())
    })
    .await;
    let successor = nodes.iter().find(|node| node.runtime.is_leader()).unwrap();
    assert_ne!(successor.name, prior_name);
    wait_for("successor queue projection", 20, || {
        successor
            .runtime
            .engine
            .snapshot()
            .jobs
            .iter()
            .any(|job| job.id == id)
    })
    .await;
    let adopted = verified_job(&successor.runtime.engine, id).await;
    assert_eq!(adopted.status, nzbd_types::JobStatus::Queued);
    assert_eq!(adopted.control(), Some(control));
    assert!(!adopted
        .params
        .iter()
        .any(|(key, _)| key == nzbd_engine::automatic_control::AUTOMATIC_CONTROL_PARAM));
    assert!(adopted
        .params
        .iter()
        .any(|(key, value)| key == "*Cluster:AuthoritySourceCopy" && value.contains(&source.id)));
    for node in nodes {
        node.kill().await;
    }
}
