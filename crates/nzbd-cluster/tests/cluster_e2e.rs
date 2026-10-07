//! Multi-node cluster tests (CLUSTERING.md §11): several full nodes in one
//! process sharing a tempdir "shared volume" and real loopback HTTP, with
//! nzbd-nserv as the provider. Lease intervals are time-compressed.

use nzbd_cluster::{ClusterConfig, ClusterRuntime, ControlPeer};
use nzbd_engine::Tuning;
use nzbd_nserv::{build_post, prng_bytes, NservBuilder};
use nzbd_types::{CertLevel, ServerDef, ServerId, TlsMode};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
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
    raft_bind: String,
    api_bind: String,
    peers: Vec<ControlPeer>,
}

fn control_topology(count: u64) -> Vec<TestControl> {
    let addresses: Vec<_> = (0..count).map(|_| (free_bind(), free_bind())).collect();
    let peers: Vec<_> = addresses
        .iter()
        .enumerate()
        .map(|(index, (raft_addr, api_addr))| ControlPeer {
            id: index as u64 + 1,
            raft_addr: raft_addr.clone(),
            api_addr: api_addr.clone(),
        })
        .collect();
    addresses
        .into_iter()
        .enumerate()
        .map(|(index, (raft_bind, api_bind))| TestControl {
            id: index as u64 + 1,
            raft_bind,
            api_bind,
            peers: peers.clone(),
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
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let url = format!("127.0.0.1:{port}");

    let control = control.unwrap_or_else(|| control_topology(1).remove(0));
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
            post: nzbd_post::manager::PostConfig::default(),
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

fn free_bind() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

impl Node {
    /// Stop the node (serving + cluster tasks + engine flush). From the
    /// rest of the cluster's perspective this is a death: renewals,
    /// heartbeats and its API all stop.
    async fn kill(self) {
        self.serve_cancel.cancel();
        self.runtime.shutdown().await;
        self.serve_task.abort();
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
        get_json(&node.url, "/api/v1/jobs")["jobs"][0]["status"] == "completed"
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
    wait_for("authoritative range assembly", 60, || {
        get_json(&leader.url, "/api/v1/jobs")["jobs"][0]["status"] == "completed"
            && tmp.path().join("complete/ranged/range.bin").is_file()
    })
    .await;
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
    std::fs::write(src.path().join("payload.bin"), &payload).unwrap();
    let ok = std::process::Command::new("par2")
        .args([
            "create",
            "-q",
            "-q",
            "-s8192",
            "-c4",
            "set.par2",
            "payload.bin",
        ])
        .current_dir(src.path())
        .status()
        .expect("par2 binary required (apt-get install par2)")
        .success();
    assert!(ok, "par2 create failed");
    let mut files: Vec<(String, Vec<u8>)> = vec![("payload.bin".into(), payload.clone())];
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

    add_job(&a.url, "pardl", post.nzb.as_bytes()).await;

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
    let got = std::fs::read(tmp.path().join("complete/pardl/payload.bin")).unwrap();
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

    a.kill().await;
    b.kill().await;
    c.kill().await;
}
