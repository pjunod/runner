//! Actual mixed worker polling under fast download load. Instrumentation wraps
//! the real HTTP route; neither lane selector nor production state is patched.

use axum::body::{to_bytes, Body};
use axum::extract::Request;
use axum::middleware::{self, Next};
use axum::response::Response;
use nzbd_cluster::{ClusterConfig, ClusterRuntime, ControlPeer};
use nzbd_engine::{EngineHandle, Tuning};
use nzbd_nserv::{build_post, prng_bytes, NservBuilder};
use nzbd_types::{CertLevel, JobId, JobStatus, ServerDef, ServerId, TlsMode};
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Serialize)]
struct PollObservation {
    free_download_slots: u64,
    free_pp_slots: u64,
    queued_backlog: usize,
    eligible_post_jobs: usize,
    granted_kinds: Vec<String>,
}

#[derive(Debug, Default)]
struct ObservationLog {
    polls: Vec<PollObservation>,
    errors: Vec<String>,
    active_leases: HashMap<String, JobId>,
}
type Observations = Arc<Mutex<ObservationLog>>;

async fn checked_post_candidates(
    engine: &EngineHandle,
    node: &str,
    observations: &Observations,
) -> HashSet<JobId> {
    let snapshot = engine.snapshot();
    let mut ready = HashSet::new();
    if snapshot.post_paused {
        return ready;
    }
    let leased: HashSet<_> = observations
        .lock()
        .unwrap()
        .active_leases
        .values()
        .copied()
        .collect();
    for summary in snapshot.jobs.iter().filter(|job| {
        matches!(job.status, JobStatus::Completed)
            && !job.pp_done
            && !leased.contains(&job.id)
            && job.assigned_node.as_deref() == Some(node)
    }) {
        match engine.export_job(summary.id).await {
            Ok(Some(job)) => {
                if matches!(job.status, JobStatus::Completed)
                    && !job.params.iter().any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
                {
                    ready.insert(job.id);
                }
            }
            Ok(None) => {} // The candidate left the queue before the read.
            Err(nzbd_engine::EngineError::Lifecycle(reason))
                if reason == "job verification or writer retirement is still in progress; retry later" => {}
            Err(error) => {
                let error = format!("observe PP readiness for job {}: {error}", summary.id.0);
                // Connection-task panics need not propagate to the test task.
                observations.lock().unwrap().errors.push(error.clone());
                panic!("{error}");
            }
        }
    }
    ready
}

async fn observe_poll(
    request: Request,
    next: Next,
    engine: EngineHandle,
    observations: Observations,
) -> Response {
    if request.uri().path() == "/cluster/v1/work/complete" {
        let (parts, body) = request.into_parts();
        let body = to_bytes(body, 32 * 1024 * 1024).await.unwrap();
        let complete: Value = serde_json::from_slice(&body).unwrap();
        let lease = complete["lease_id"].as_str().unwrap().to_owned();
        let response = next.run(Request::from_parts(parts, Body::from(body))).await;
        if response.status().is_success() {
            observations.lock().unwrap().active_leases.remove(&lease);
        }
        return response;
    }
    if request.uri().path() != "/cluster/v1/work/poll" {
        return next.run(request).await;
    }
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, 64 * 1024).await.unwrap();
    let poll: Value = serde_json::from_slice(&body).unwrap();
    let snapshot = engine.snapshot();
    let node = poll["node"].as_str().unwrap();
    let queued_backlog = snapshot
        .jobs
        .iter()
        .filter(|job| matches!(job.status, JobStatus::Queued))
        .count();
    // A published Completed summary can precede an in-flight projection's
    // verification fence. The real grant path requires checked export too.
    let ready_before = checked_post_candidates(&engine, node, &observations).await;
    let response = next.run(Request::from_parts(parts, Body::from(body))).await;
    let (parts, body) = response.into_parts();
    let body = to_bytes(body, 32 * 1024 * 1024).await.unwrap();
    if parts.status.is_success() {
        let value: Value = serde_json::from_slice(&body).unwrap();
        let ready_after = checked_post_candidates(&engine, node, &observations).await;
        let mut eligible: HashSet<_> = ready_before.intersection(&ready_after).copied().collect();
        // Do not count a transient recovery block as a bypass. A returned
        // Post grant is itself positive evidence even if its row changed
        // while the request was running.
        for grant in value["grants"].as_array().unwrap() {
            if grant["kind"] == "post" {
                eligible.insert(serde_json::from_value(grant["job"]["id"].clone()).unwrap());
            }
        }
        // A checked Completed row with an outstanding Download/Post lease is
        // not grantable. Track actual grants and successful completion receipts
        // so the observer applies that same scheduler boundary.
        {
            let mut seen = observations.lock().unwrap();
            for grant in value["grants"].as_array().unwrap() {
                seen.active_leases.insert(
                    grant["lease_id"].as_str().unwrap().to_owned(),
                    serde_json::from_value(grant["job"]["id"].clone()).unwrap(),
                );
            }
        }
        let granted_kinds = value["grants"]
            .as_array()
            .unwrap()
            .iter()
            .map(|grant| grant["kind"].as_str().unwrap().to_string())
            .collect();
        observations.lock().unwrap().polls.push(PollObservation {
            free_download_slots: poll["free_download_slots"].as_u64().unwrap(),
            free_pp_slots: poll["free_pp_slots"].as_u64().unwrap(),
            queued_backlog,
            eligible_post_jobs: eligible.len(),
            granted_kinds,
        });
    }
    Response::from_parts(parts, Body::from(body))
}

struct Node {
    url: String,
    runtime: ClusterRuntime,
    cancel: CancellationToken,
    serve: JoinHandle<()>,
}

async fn start_node(
    shared: &Path,
    name: &str,
    mixed_worker: bool,
    nntp: u16,
    seen: Observations,
) -> Node {
    // Keep all three listeners reserved until the owning runtime can bind;
    // ephemeral API sockets must not steal a planned control-plane port.
    let api = TcpListener::bind("127.0.0.1:0").unwrap();
    let raft = TcpListener::bind("127.0.0.1:0").unwrap();
    let control_api = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = api.local_addr().unwrap().to_string();
    let raft_addr = raft.local_addr().unwrap().to_string();
    let control_addr = control_api.local_addr().unwrap().to_string();
    let config = ClusterConfig {
        cluster_id: "actual-fairness".into(),
        node_name: name.into(),
        shared_dir: shared.into(),
        advertise_url: format!("http://{url}"),
        secret: "fairness-test-secret".into(),
        coordinator: !mixed_worker,
        priority: 0,
        download: mixed_worker,
        max_download_jobs: if mixed_worker { 8 } else { 0 },
        post_process: mixed_worker,
        // Keep more than one Post slot available while an actual Post runs.
        // The scheduler counts assigned Post jobs in this node's download
        // load, so a single Post slot otherwise hides lane competition behind
        // the active Post occupying that only slot.
        pp_slots: 3,
        lease_interval: Duration::from_millis(100),
        takeover_after: Duration::from_millis(900),
        worker_ttl: Duration::from_secs(2),
        control_dir: shared.join(format!("control-{name}")),
        control_node_id: 1,
        control_raft_bind: raft_addr.clone(),
        control_api_bind: control_addr.clone(),
        control_peers: vec![ControlPeer {
            id: 1,
            raft_addr,
            api_addr: control_addr,
        }],
        download_weight: 1,
        pp_weight: 1,
        disk_guard_roots: vec![],
        torrent_payload_roots: vec![],
        workspace_dir: None,
    };
    let local = shared.join(format!("local-{name}"));
    std::fs::create_dir_all(&local).unwrap();
    let history = shared.join(".nzbd-cluster/history");
    std::fs::create_dir_all(&history).unwrap();
    let pp = mixed_worker.then(|| nzbd_cluster::PpSetup {
        post: nzbd_post::manager::PostConfig {
            unpack: false,
            cleanup: false,
            ..Default::default()
        },
        history: Arc::new(
            nzbd_state::history::HistoryDb::open_tagged(
                &local.join("history.sqlite"),
                Some(&history),
                Some(name),
            )
            .unwrap(),
        ),
    });
    let server = ServerDef {
        id: ServerId(1),
        name: "local-fairness-provider".into(),
        host: "127.0.0.1".into(),
        port: nntp,
        tls: TlsMode::None,
        username: None,
        password: None,
        active: true,
        tier: 0,
        group: 0,
        fill: false,
        max_connections: 4,
        pipeline_depth: 2,
        retention_days: 0,
        cert_verification: CertLevel::Strict,
    };
    drop(raft);
    drop(control_api);
    let runtime = ClusterRuntime::start(
        config,
        vec![server],
        Tuning {
            retry_interval: Duration::from_millis(200),
            min_free_disk_bytes: 0,
            ..Default::default()
        },
        shared.join("complete"),
        None,
        None,
        pp,
    )
    .await
    .unwrap();
    let engine = runtime.engine.clone();
    let app = runtime
        .router("fairness-test", vec![])
        .layer(middleware::from_fn(move |req: Request, next: Next| {
            observe_poll(req, next, engine.clone(), seen.clone())
        }));
    api.set_nonblocking(true).unwrap();
    let api = tokio::net::TcpListener::from_std(api).unwrap();
    let cancel = CancellationToken::new();
    let shutdown = cancel.clone();
    let serve = tokio::spawn(async move {
        axum::serve(api, app)
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await
            .unwrap();
    });
    Node {
        url,
        runtime,
        cancel,
        serve,
    }
}

impl Node {
    async fn stop(self) {
        self.cancel.cancel();
        self.runtime.shutdown().await;
        self.serve.abort();
        let _ = self.serve.await;
    }
}

fn http(addr: &str, method: &str, path: &str, body: &[u8]) -> (u16, String) {
    let mut socket = TcpStream::connect(addr).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write!(socket, "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
    socket.write_all(body).unwrap();
    let mut bytes = Vec::new();
    socket.read_to_end(&mut bytes).unwrap();
    let text = String::from_utf8(bytes).unwrap();
    (
        text.split_whitespace().nth(1).unwrap().parse().unwrap(),
        text.split_once("\r\n\r\n").unwrap().1.trim().to_owned(),
    )
}

async fn wait_for(mut condition: impl FnMut() -> bool, description: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out: {description}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn mixed_worker_grants_post_while_fast_downloads_keep_slots_and_backlog_available() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "nzbd_cluster=info,nzbd_cluster::leader=debug,nzbd_engine=warn".into()
            }),
        )
        .with_test_writer()
        .try_init();
    let tmp = tempfile::tempdir().unwrap();
    let payload = prng_bytes(9201, 16_000);
    let post = build_post("fairness", &[("payload.mkv", payload)], 20_000);
    let provider = NservBuilder::new().with_post(&post).start().await.unwrap();
    let seen = Observations::default();
    let authority = start_node(
        tmp.path(),
        "authority",
        false,
        provider.port(),
        seen.clone(),
    )
    .await;
    wait_for(
        || {
            let (status, body) = http(&authority.url, "GET", "/api/v1/cluster", b"");
            status == 200 && serde_json::from_str::<Value>(&body).unwrap()["is_leader"] == true
        },
        "authority elected",
    )
    .await;
    // Build a real Post backlog through the public pause control first.
    // Otherwise fast Post grants usually arrive while every download slot is
    // occupied, which proves progress but never exercises competing lanes.
    wait_for(
        || {
            let (status, body) = http(
                &authority.url,
                "POST",
                "/api/v1/queue/actions/pause-post",
                b"",
            );
            if status == 200 {
                return true;
            }
            assert!(
                status == 503
                    && serde_json::from_str::<Value>(&body)
                        .is_ok_and(|error| error["error"]
                            == "queue authority is still being adopted; retry"),
                "pause Post for readiness setup: {status}: {body}"
            );
            false
        },
        "pause Post after initial authority adoption",
    )
    .await;
    // No executor exists yet. Seed enough real jobs to keep the download lane
    // continuously runnable while several PP attempts complete on the same node.
    for index in 0..96 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (status, body) = http(
                &authority.url,
                "POST",
                &format!("/api/v1/jobs?name=fairness-{index}"),
                post.nzb.as_bytes(),
            );
            if status == 201 {
                break;
            }
            assert_eq!(
                status,
                503,
                "admission failed at seed {index}: {body}; snapshot={:?}",
                authority.runtime.engine.snapshot()
            );
            assert!(
                tokio::time::Instant::now() < deadline,
                "admission stayed unavailable at seed {index}: {body}; snapshot={:?}",
                authority.runtime.engine.snapshot()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let worker = start_node(tmp.path(), "mixed", true, provider.port(), seen.clone()).await;
    wait_for(
        || {
            let completed = authority
                .runtime
                .engine
                .snapshot()
                .jobs
                .iter()
                .filter(|job| matches!(job.status, JobStatus::Completed) && !job.pp_done)
                .count();
            completed >= 4
                && provider.total_hits() >= 8
                && seen.lock().unwrap().polls.last().is_some_and(|poll| {
                    poll.free_download_slots > 0
                        && poll.free_pp_slots > 0
                        && poll.queued_backlog > 0
                })
        },
        "actual completed Post backlog and free competing lanes",
    )
    .await;
    wait_for(
        || {
            let (status, body) = http(
                &authority.url,
                "POST",
                "/api/v1/queue/actions/resume-post",
                b"",
            );
            if status == 200 {
                return true;
            }
            assert!(
                status == 503
                    && serde_json::from_str::<Value>(&body)
                        .is_ok_and(|error| error["error"] == "queue_projection_unavailable"),
                "resume Post for competing load: {status}: {body}"
            );
            false
        },
        "resume Post after existing projection verification",
    )
    .await;
    let history_path = tmp.path().join(".nzbd-cluster/history/history.mixed.jsonl");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    loop {
        let history = std::fs::read_to_string(&history_path);
        let successes = history
            .as_ref()
            .map(|history| {
                history
                    .lines()
                    .filter(|line| line.contains("\"SUCCESS\""))
                    .count()
            })
            .unwrap_or(0);
        // A completed Post can have been granted while the download lane
        // was full. Keep observing until three grants actually competed with
        // a free download lane; all observed grant-order windows remain checked.
        let competing_post_grants = seen
            .lock()
            .unwrap()
            .polls
            .iter()
            .filter(|poll| {
                poll.free_download_slots > 0
                    && poll.free_pp_slots > 0
                    && poll.queued_backlog > 0
                    && poll.eligible_post_jobs > 0
                    && poll.granted_kinds.iter().any(|kind| kind == "post")
            })
            .count();
        if successes >= 3 && provider.total_hits() >= 8 && competing_post_grants >= 3 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out: three actual remote post-processing successes under mixed load; successes={successes}, provider_hits={}, history_path={history_path:?}, history={history:?}, authority={:?}, worker={:?}, polls={:?}",
            provider.total_hits(),
            authority.runtime.engine.snapshot(),
            worker.runtime.engine.snapshot(),
            seen.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let backlog = authority
        .runtime
        .engine
        .snapshot()
        .jobs
        .iter()
        .filter(|j| matches!(j.status, JobStatus::Queued))
        .count();
    let hits = provider.total_hits();
    worker.stop().await;
    authority.stop().await;
    let observed = seen.lock().unwrap();
    assert!(
        observed.errors.is_empty(),
        "poll observer failed: {:?}",
        observed.errors
    );
    let observed = &observed.polls;
    eprintln!(
        "actual mixed poll observations: {}",
        serde_json::to_string(observed).unwrap()
    );
    assert!(
        hits >= 8,
        "real fast NNTP downloads did not progress: {hits}"
    );
    assert!(backlog > 0, "load drained before PP proved progress");
    let eligible: Vec<_> = observed
        .iter()
        .filter(|poll| {
            poll.free_download_slots > 0
                && poll.free_pp_slots > 0
                && poll.queued_backlog > 0
                && poll.eligible_post_jobs > 0
        })
        .collect();
    assert!(
        eligible.len() >= 3,
        "PP never competed with a free download lane: {eligible:?}"
    );
    assert!(
        eligible
            .iter()
            .filter(|poll| poll.granted_kinds.iter().any(|kind| kind == "post"))
            .count()
            >= 3,
        "PP was not repeatedly granted under continuous competing load: {eligible:?}"
    );
    assert_bounded_post_progress(observed);
}

fn assert_bounded_post_progress(observed: &[PollObservation]) {
    let mut bypasses = 0;
    for poll in observed {
        // Every actual Post grant proves progress, including one issued while
        // the download lane is full. Filtering those grants out first would
        // incorrectly reject Download, Post, Download as two Post bypasses.
        if poll.granted_kinds.iter().any(|kind| kind == "post") {
            bypasses = 0;
        } else if poll.free_download_slots > 0
            && poll.free_pp_slots > 0
            && poll.queued_backlog > 0
            && poll.eligible_post_jobs > 0
        {
            bypasses += 1;
            assert!(
                bypasses < 2,
                "two eligible polls bypassed PP without an intervening Post grant: {observed:?}"
            );
        }
    }
}

#[test]
fn fairness_window_credits_actual_post_progress_and_refuses_repeated_bypass() {
    let poll = |free_download_slots, kinds: &[&str]| PollObservation {
        free_download_slots,
        free_pp_slots: 1,
        queued_backlog: 10,
        eligible_post_jobs: 1,
        granted_kinds: kinds.iter().map(|kind| (*kind).to_owned()).collect(),
    };
    assert_bounded_post_progress(&[
        poll(1, &["download"]),
        poll(0, &["post"]),
        poll(1, &["download"]),
    ]);
    assert!(std::panic::catch_unwind(|| assert_bounded_post_progress(&[
        poll(1, &["download"]),
        poll(1, &["download"]),
    ]))
    .is_err());
    assert!(std::panic::catch_unwind(|| assert_bounded_post_progress(&[
        poll(1, &["download"]),
        poll(0, &[]),
        poll(1, &["download"]),
    ]))
    .is_err());
}
