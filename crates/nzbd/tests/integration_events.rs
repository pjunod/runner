//! Acceptance tests for the monarr integration surface
//! (docs/INTEGRATION_PLAN.md N1–N6), driven against a real daemon over
//! real sockets.
//!
//! These assert the contract a consumer is entitled to rely on, not the
//! implementation that currently satisfies it: the event ORDER, the
//! ordering guarantee between `job_pp_finished` and the history row, the
//! resume protocol, and the rule that an advertised path is the actual
//! path. If a refactor breaks one of these, a downstream import silently
//! stops working — which is the exact failure this whole phase exists to
//! remove, so it gets tests at the outermost layer available.

#![cfg(unix)]

use nzbd_nserv::{build_post, prng_bytes, NservBuilder};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct KillOnDrop(Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-TERM", &self.0.id().to_string()])
            .status();
        for _ in 0..50 {
            if matches!(self.0.try_wait(), Ok(Some(_))) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn request(
    addr: &str,
    method: &str,
    path: &str,
    body: &[u8],
    headers: &[(&str, &str)],
) -> (u16, String) {
    let mut sock = TcpStream::connect(addr).expect("connect");
    sock.set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    let extra: String = headers
        .iter()
        .map(|(k, v)| format!("{k}: {v}\r\n"))
        .collect();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n\
         Content-Type: application/octet-stream\r\n{extra}Connection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(req.as_bytes()).unwrap();
    sock.write_all(body).unwrap();
    let mut resp = Vec::new();
    let _ = sock.read_to_end(&mut resp);
    let text = String::from_utf8_lossy(&resp).into_owned();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let payload = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    // Responses come back chunked; the JSON body is the only brace-run in
    // there, so take from the first `{` to the last `}`.
    let json = match (payload.find('{'), payload.rfind('}')) {
        (Some(a), Some(b)) if b > a => payload[a..=b].to_string(),
        _ => payload.trim().to_string(),
    };
    (status, json)
}

fn get(addr: &str, path: &str) -> (u16, serde_json::Value) {
    let (code, body) = request(addr, "GET", path, b"", &[]);
    let v = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    (code, v)
}

fn wait_healthy(addr: &str, deadline: Duration) {
    let start = Instant::now();
    loop {
        assert!(
            start.elapsed() < deadline,
            "daemon did not become healthy at {addr}"
        );
        if let Ok(mut s) = TcpStream::connect(addr) {
            s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            let _ = s.write_all(
                format!("GET /healthz HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            );
            let mut buf = String::new();
            if s.read_to_string(&mut buf).is_ok() && buf.contains("ok") {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// One frame off the SSE stream. `id` is the raw `<boot>-<seq>` wire
/// form; `seq` is its second half, which is what the assertions care
/// about.
#[derive(Debug, Clone)]
struct Frame {
    id: Option<String>,
    event: String,
    data: String,
}

impl Frame {
    fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.data).unwrap_or(serde_json::Value::Null)
    }
    fn seq(&self) -> Option<u64> {
        self.id.as_ref()?.split_once('-')?.1.parse().ok()
    }
}

/// A live `/api/v1/events` connection, read frame by frame.
struct Sse {
    lines: BufReader<TcpStream>,
}

impl Sse {
    fn open(addr: &str, headers: &[(&str, &str)]) -> Sse {
        let sock = TcpStream::connect(addr).expect("connect sse");
        sock.set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let extra: String = headers
            .iter()
            .map(|(k, v)| format!("{k}: {v}\r\n"))
            .collect();
        let mut w = sock.try_clone().unwrap();
        w.write_all(
            format!(
                "GET /api/v1/events HTTP/1.1\r\nHost: {addr}\r\n\
                 Accept: text/event-stream\r\n{extra}\r\n"
            )
            .as_bytes(),
        )
        .unwrap();
        let mut lines = BufReader::new(sock);
        // Drain the response head.
        loop {
            let mut line = String::new();
            let n = lines.read_line(&mut line).expect("sse head");
            assert!(n > 0, "sse stream closed during handshake");
            if line.trim().is_empty() {
                break;
            }
        }
        Sse { lines }
    }

    /// Next frame, or `None` at the deadline. Chunked-transfer size lines
    /// are interleaved with the SSE text; they are hex with no colon, so
    /// the field parse below simply ignores them.
    fn next(&mut self, deadline: Instant) -> Option<Frame> {
        let mut id = None;
        let mut event = None;
        let mut data: Option<String> = None;
        loop {
            if Instant::now() > deadline {
                return None;
            }
            let mut line = String::new();
            if self.lines.read_line(&mut line).ok()? == 0 {
                return None;
            }
            let line = line.trim_end_matches(['\r', '\n']);
            if line.is_empty() {
                if event.is_some() || data.is_some() {
                    return Some(Frame {
                        id,
                        event: event.take().unwrap_or_default(),
                        data: data.take().unwrap_or_default(),
                    });
                }
                continue;
            }
            match line.split_once(':') {
                Some(("id", v)) => id = Some(v.trim().to_string()),
                Some(("event", v)) => event = Some(v.trim().to_string()),
                Some(("data", v)) => data = Some(v.trim().to_string()),
                _ => {} // comment keep-alive, or a chunk-size line
            }
        }
    }

    /// Collect frames until `want` matches one, returning everything seen.
    fn until(&mut self, deadline: Instant, want: &str) -> Vec<Frame> {
        let mut out = Vec::new();
        while let Some(f) = self.next(deadline) {
            let hit = f.event == want;
            out.push(f);
            if hit {
                return out;
            }
        }
        panic!("never saw a {want:?} frame; got: {:?}", names(&out));
    }
}

fn names(frames: &[Frame]) -> Vec<&str> {
    frames.iter().map(|f| f.event.as_str()).collect()
}

struct Daemon {
    addr: String,
    tmp: tempfile::TempDir,
    log_path: PathBuf,
    _child: KillOnDrop,
}

impl Daemon {
    fn log(&self) -> String {
        std::fs::read_to_string(&self.log_path)
            .unwrap_or_else(|error| format!("could not read daemon log: {error}"))
    }
}

/// Boot a daemon against a mock news server, with `extra` appended to the
/// config.
fn boot(rt: &tokio::runtime::Runtime, post: &nzbd_nserv::GeneratedPost, extra: &str) -> Daemon {
    let ns = rt.block_on(async { NservBuilder::new().with_post(post).start().await.unwrap() });
    // Deliberately leaked: the mock provider must outlive this call, and
    // the runtime it was spawned on is owned by the caller.
    let nntp_port = ns.port();
    std::mem::forget(ns);

    let tmp = tempfile::tempdir().unwrap();
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let config = format!(
        r#"
[paths]
main_dir = "{main}"
dest_dir = "{dest}"

[[server]]
name = "mock"
host = "127.0.0.1"
port = {nntp_port}
tls = false
connections = 4

[api]
bind = "{addr}"
{extra}
"#,
        main = tmp.path().join("main").display(),
        dest = tmp.path().join("dest").display(),
    );
    let cfg_path = tmp.path().join("nzbd.toml");
    std::fs::write(&cfg_path, config).unwrap();

    let log_path = tmp.path().join("daemon.log");
    let daemon_log = std::fs::File::create(&log_path).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_nzbd"))
        .args(["run", "--config"])
        .arg(&cfg_path)
        .stdout(daemon_log.try_clone().unwrap())
        .stderr(daemon_log)
        .spawn()
        .expect("spawn nzbd");
    let child = KillOnDrop(child);
    wait_healthy(&addr, Duration::from_secs(20));
    Daemon {
        addr,
        tmp,
        log_path,
        _child: child,
    }
}

// ---------------------------------------------------------------------------
// N1 + N3 + N4 + N6 — one real download, end to end
// ---------------------------------------------------------------------------

/// The whole point of phase 1, in one download: a consumer subscribed to
/// the event stream learns that post-processing ran, which stages it
/// passed through, where the files ended up, and can immediately read the
/// history row the completion refers to.
#[test]
fn a_finished_download_announces_its_stages_its_final_dir_and_its_history_row() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = prng_bytes(7, 120_000);
    let post = build_post("Show.S01E01", &[("payload.bin", data.clone())], 25_000);

    // A category with its own destination: N6's honesty fix, exercised by
    // the same download rather than in isolation, because the thing that
    // matters is that the path in the EVENT equals the path on DISK.
    let tmp_marker = tempfile::tempdir().unwrap();
    let cat_dir = tmp_marker.path().join("library/tv");
    let d = boot(
        &rt,
        &post,
        &format!(
            "\n[[category]]\nname = \"tv\"\ndest_dir = \"{}\"\n",
            cat_dir.display()
        ),
    );

    let mut sse = Sse::open(&d.addr, &[("X-Nzbd-Client", "monarr/9.9.9")]);

    // Add with a consumer tracking id (N4). The param must survive all the
    // way into the completion event and the history row — that id is how a
    // stuck transfer gets traced across three applications.
    let params = urlencode(r#"{"monarr-transfer":"t-42-a3f9c1"}"#);
    let (code, body) = request(
        &d.addr,
        "POST",
        &format!("/api/v1/jobs?name=Show.S01E01&category=tv&params={params}"),
        post.nzb.as_bytes(),
        &[("X-Nzbd-Client", "monarr/9.9.9")],
    );
    assert_eq!(code, 201, "add rejected: {body}");

    let deadline = Instant::now() + Duration::from_secs(60);
    let frames = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sse.until(deadline, "job_pp_finished")
    }))
    .unwrap_or_else(|panic| {
        eprintln!("--- daemon log ---\n{}", d.log());
        std::panic::resume_unwind(panic)
    });
    let engine_frames: Vec<&Frame> = frames
        .iter()
        .filter(|f| !matches!(f.event.as_str(), "tick" | "hb" | "log"))
        .collect();

    // ---- ordering (N1) -----------------------------------------------
    let order: Vec<&str> = engine_frames.iter().map(|f| f.event.as_str()).collect();
    let pos = |name: &str| order.iter().position(|e| *e == name);
    let added = pos("job_added").expect("job_added");
    let finished = pos("job_finished").expect("job_finished");
    let first_stage = pos("job_pp_stage").expect("at least one job_pp_stage");
    let pp_finished = pos("job_pp_finished").expect("job_pp_finished");
    assert!(
        added < finished && finished < first_stage && first_stage < pp_finished,
        "expected job_added → job_finished → job_pp_stage+ → job_pp_finished, got {order:?}"
    );

    // Every engine frame is resumable (N2): stream-local views are not.
    for f in &engine_frames {
        assert!(
            f.seq().is_some(),
            "engine frame {:?} carried no id: — a consumer cannot resume from it",
            f.event
        );
        assert!(
            f.json().get("seq").is_some(),
            "frame body must carry seq too, for consumers that read only data:"
        );
    }
    for f in frames
        .iter()
        .filter(|f| f.event == "tick" || f.event == "hb")
    {
        assert!(
            f.id.is_none(),
            "{} is stream-local and must not be resumable",
            f.event
        );
    }

    // ---- the completion payload (N1 + N4 + N6) ------------------------
    let done = frames.last().unwrap().json();
    assert_eq!(done["pp_status"], "SUCCESS", "payload: {done}");
    assert_eq!(done["category"], "tv");
    let final_dir = done["final_dir"].as_str().expect("final_dir").to_string();
    assert!(
        final_dir.starts_with(cat_dir.to_str().unwrap()),
        "the category dest_dir must be honored, not merely advertised: {final_dir}"
    );
    assert!(
        std::path::Path::new(&final_dir).is_dir(),
        "the reported final_dir must be where the files ACTUALLY are: {final_dir}"
    );
    assert!(
        !d.tmp.path().join("dest/Show.S01E01").exists(),
        "the job must not be left behind in the global destination too"
    );
    let carried = done["params"]
        .as_array()
        .expect("params array")
        .iter()
        .any(|p| p[0] == "monarr-transfer" && p[1] == "t-42-a3f9c1");
    assert!(carried, "the transfer id must ride the completion: {done}");

    // ---- the ordering guarantee (N1 + N3) -----------------------------
    // Normative: the event is emitted only after the history row is
    // durably written, so this read — issued the instant the event
    // arrives, with no retry loop — must find it.
    let seq = done["history_seq"].as_i64().expect("history_seq");
    assert!(seq > 0, "a completion must name its history row");
    let (code, v) = get(&d.addr, &format!("/api/v1/history?since_seq={}", seq - 1));
    assert_eq!(code, 200);
    let rows = v["entries"].as_array().expect("entries");
    let row = rows
        .iter()
        .find(|e| e["seq"] == seq)
        .unwrap_or_else(|| panic!("history row {seq} was not readable when the event fired: {v}"));
    assert_eq!(row["status"], "SUCCESS");
    assert_eq!(
        row["final_dir"], final_dir,
        "history and the event must agree on where the files are"
    );

    // ---- N7: the stream is measurable ---------------------------------
    // "Is the pipeline still pushing, and is anything listening" must be
    // answerable from a dashboard — a silently-stopped stream otherwise
    // looks exactly like an idle one.
    let (code, metrics) = request(&d.addr, "GET", "/metrics", b"", &[]);
    assert_eq!(code, 200);
    for line in [
        "nzbd_events_emitted_total{event=\"job_pp_finished\"} 1",
        "nzbd_pp_stage_seconds_count{stage=\"par_rename\"}",
    ] {
        assert!(
            metrics.contains(line),
            "missing {line:?} from /metrics:\n{metrics}"
        );
    }
    assert!(
        metrics.contains("nzbd_sse_clients 1"),
        "the open subscription must be visible as a gauge:\n{metrics}"
    );

    // The compat surface agrees too: an *arr path-mapping off `DestDir`
    // and a consumer reading `final_dir` must not be sent to two places.
    let (code, body) = request(
        &d.addr,
        "POST",
        "/jsonrpc",
        br#"{"method":"history","params":[]}"#,
        &[("Content-Type", "application/json")],
    );
    assert_eq!(code, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let compat_dir = v["result"][0]["DestDir"].as_str().unwrap_or_default();
    assert_eq!(
        compat_dir, final_dir,
        "compat DestDir must equal the real final dir"
    );
}

// ---------------------------------------------------------------------------
// N2 — resume
// ---------------------------------------------------------------------------

/// A consumer that blinks must be able to catch up exactly, and must be
/// told plainly when it cannot. (Ring eviction itself is exercised in
/// `nzbd-api`'s unit tests, which can cheaply overflow 1024 events.)
#[test]
fn a_reconnecting_consumer_replays_exactly_what_it_missed() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let post = build_post("resume", &[("x.bin", prng_bytes(3, 4_000))], 4_000);
    let d = boot(&rt, &post, "");

    // One event, to learn where we are in the numbering.
    let mut sse = Sse::open(&d.addr, &[]);
    let deadline = Instant::now() + Duration::from_secs(30);
    let (code, _) = request(&d.addr, "POST", "/api/v1/queue/actions/pause", b"", &[]);
    assert_eq!(code, 200);
    let frames = sse.until(deadline, "queue_pause_changed");
    let last = frames.last().unwrap();
    let last_id = last.id.clone().expect("id on an engine frame");
    let last_seq = last.seq().expect("<boot>-<seq> wire form");
    drop(sse); // "the tab was closed"

    // Three events happen while nobody is listening.
    for action in ["resume", "pause", "resume"] {
        let (code, _) = request(
            &d.addr,
            "POST",
            &format!("/api/v1/queue/actions/{action}"),
            b"",
            &[],
        );
        assert_eq!(code, 200);
    }

    let mut resumed = Sse::open(&d.addr, &[("Last-Event-ID", &last_id)]);
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut replayed = Vec::new();
    while replayed.len() < 3 {
        let f = resumed
            .next(deadline)
            .expect("replay frames should arrive immediately");
        if f.event == "tick" || f.event == "hb" || f.event == "log" {
            continue;
        }
        replayed.push(f);
    }
    assert_eq!(
        names(&replayed),
        vec!["queue_pause_changed"; 3],
        "exactly the three missed events, in order"
    );
    assert_eq!(
        replayed.iter().filter_map(|f| f.seq()).collect::<Vec<_>>(),
        vec![last_seq + 1, last_seq + 2, last_seq + 3],
        "contiguous with where the consumer left off"
    );
    assert!(
        !replayed.iter().any(|f| f.event == "reset"),
        "a coverable gap must not be reported as a reset"
    );

    // An id from a DIFFERENT process. This is the case a bare sequence
    // number cannot catch: after a restart the daemon re-issues low
    // numbers for entirely different events, and serving those as the
    // continuation of an old stream is silent corruption rather than a
    // visible gap. The epoch in the id is what makes it detectable.
    let boot = last_id.split_once('-').unwrap().0.parse::<u64>().unwrap();
    let foreign = format!("{}-1", boot - 1);
    let mut stale = Sse::open(&d.addr, &[("Last-Event-ID", &foreign)]);
    let first = stale
        .next(Instant::now() + Duration::from_secs(15))
        .expect("a frame");
    assert_eq!(
        first.event, "reset",
        "a restart must not be served as a clean resume"
    );
    assert_eq!(first.json()["reason"], "gap");
    drop(stale);

    // A seq this process never issued is equally uncoverable.
    let mut ahead = Sse::open(&d.addr, &[("Last-Event-ID", &format!("{boot}-999999"))]);
    assert_eq!(
        ahead
            .next(Instant::now() + Duration::from_secs(15))
            .expect("a frame")
            .event,
        "reset"
    );
}

// ---------------------------------------------------------------------------
// N4 + N5 — params validation and attribution
// ---------------------------------------------------------------------------

#[test]
fn params_are_validated_and_native_consumers_are_visible() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let post = build_post("attrib", &[("x.bin", prng_bytes(4, 4_000))], 4_000);
    let d = boot(&rt, &post, "");

    // ---- N4: the internal namespace is not a write path ---------------
    let bad = urlencode(r#"{"*PP:done":"SUCCESS"}"#);
    let (code, body) = request(
        &d.addr,
        "POST",
        &format!("/api/v1/jobs?name=forge&params={bad}"),
        post.nzb.as_bytes(),
        &[],
    );
    assert_eq!(code, 422, "a `*` key must be rejected, not silently kept");
    assert!(
        body.contains("*PP:done"),
        "the error must name the offending key: {body}"
    );

    let (code, body) = request(
        &d.addr,
        "POST",
        &format!(
            "/api/v1/jobs?name=notanobject&params={}",
            urlencode("[1,2]")
        ),
        post.nzb.as_bytes(),
        &[],
    );
    assert_eq!(code, 422, "params must be an object of strings: {body}");

    // ---- N4: a good param reaches every surface -----------------------
    let ok = urlencode(r#"{"monarr-transfer":"t-1-abc123"}"#);
    let (code, body) = request(
        &d.addr,
        "POST",
        &format!("/api/v1/jobs?name=tracked&params={ok}"),
        post.nzb.as_bytes(),
        &[("X-Nzbd-Client", "monarr/9.9.9")],
    );
    assert_eq!(code, 201, "{body}");
    let id = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_u64()
        .expect("id");

    let (code, v) = get(&d.addr, &format!("/api/v1/jobs/{id}"));
    assert_eq!(code, 200);
    let params = v["params"].as_array().cloned().unwrap_or_default();
    assert!(
        params
            .iter()
            .any(|p| p[0] == "monarr-transfer" && p[1] == "t-1-abc123"),
        "param missing from the native job view: {v}"
    );

    // Same param, through the compat shim an *arr would read.
    let (code, body) = request(
        &d.addr,
        "POST",
        "/jsonrpc",
        br#"{"method":"listgroups","params":[]}"#,
        &[("Content-Type", "application/json")],
    );
    assert_eq!(code, 200);
    assert!(
        body.contains("monarr-transfer") && body.contains("t-1-abc123"),
        "params must propagate to compat Parameters with no second write path: {body}"
    );

    // ---- N5: the consumer is visible, and so is its subscription ------
    let sse = Sse::open(&d.addr, &[("X-Nzbd-Client", "monarr/9.9.9")]);
    let mut found = None;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let (_, v) = get(&d.addr, "/api/v1/clients");
        found = v["clients"]
            .as_array()
            .and_then(|cs| {
                cs.iter()
                    .find(|c| c["user_agent"] == "monarr/9.9.9")
                    .cloned()
            })
            .filter(|c| c["event_subscriptions"].as_u64().unwrap_or(0) > 0);
        if found.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let client = found.expect("a native consumer with an open event stream must be listed");
    assert_eq!(client["api"], "native");
    drop(sse);

    // ---- N5: a native history hide records who did it -----------------
    let (code, _) = request(
        &d.addr,
        "POST",
        &format!("/api/v1/jobs/{id}/actions/delete"),
        b"",
        &[],
    );
    assert_eq!(code, 200);
    let (code, _) = request(
        &d.addr,
        "POST",
        &format!("/api/v1/history/{id}/actions/hide"),
        b"",
        &[("X-Nzbd-Client", "monarr/9.9.9")],
    );
    assert_eq!(code, 200);
    let (_, v) = get(&d.addr, "/api/v1/history?limit=50");
    let row = v["entries"]
        .as_array()
        .and_then(|e| e.iter().find(|e| e["job"] == id))
        .expect("the hidden row is still listed to the UI");
    assert_eq!(
        row["picked_up_by"], "monarr/9.9.9",
        "a native hide is an import signal and must name the consumer: {row}"
    );

    // ---- N6: a native history READ is the handoff signal ---------------
    // Until this was wired, only the nzbget-compat `history` RPC wrote
    // `last_seen`, so a native consumer polling every 30 s left every
    // finished job reading "awaiting pickup" forever — the column that
    // answers "did my *arr take these?" said no while it was saying yes.
    let seen_count = |addr: &str| -> u64 {
        let (_, v) = get(addr, "/api/v1/history?limit=50");
        v["entries"]
            .as_array()
            .and_then(|e| e.iter().find(|e| e["job"] == id))
            .and_then(|r| r["seen_count"].as_u64())
            .expect("the row is listed")
    };
    // Every read above sent no User-Agent at all, so nothing has counted.
    assert_eq!(seen_count(&d.addr), 0, "an anonymous read is not a pickup");

    // A browser must never satisfy the handoff. If it did, opening the
    // History tab would flip every row to "seen" and the column would be
    // answering a different question than the one it asks.
    let (code, _) = request(
        &d.addr,
        "GET",
        "/api/v1/history?limit=50",
        b"",
        &[(
            "User-Agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )],
    );
    assert_eq!(code, 200);
    assert_eq!(
        seen_count(&d.addr),
        0,
        "the operator opening the History tab is not the consumer collecting the files"
    );

    // An application identifying itself is.
    let (code, _) = request(
        &d.addr,
        "GET",
        "/api/v1/history?limit=50",
        b"",
        &[("X-Nzbd-Client", "monarr/9.9.9")],
    );
    assert_eq!(code, 200);
    // History observations are queued to the owned sync worker. HTTP reads
    // intentionally do not wait behind that worker's filesystem transaction.
    let deadline = Instant::now() + Duration::from_secs(5);
    while seen_count(&d.addr) == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        seen_count(&d.addr),
        1,
        "a native consumer's history poll must record the pull"
    );
    let (_, v) = get(&d.addr, "/api/v1/history?limit=50");
    let row = v["entries"]
        .as_array()
        .and_then(|e| e.iter().find(|e| e["job"] == id))
        .expect("the row is listed");
    assert!(
        row["last_seen_at_unix"].as_i64().unwrap_or(0) > 0,
        "the handoff badge reads off last_seen_at_unix: {row}"
    );

    // The cursor form counts too — a catch-up walk is still the consumer
    // reading them, and a consumer that only ever uses ?since_seq= would
    // otherwise look like one that never polled at all.
    let (code, _) = request(
        &d.addr,
        "GET",
        "/api/v1/history?since_seq=0",
        b"",
        &[("X-Nzbd-Client", "monarr/9.9.9")],
    );
    assert_eq!(code, 200);
    assert_eq!(seen_count(&d.addr), 2, "a since_seq walk is also a pull");
}

/// Percent-encode a query value. (The daemon takes `params` as a query
/// field, so the test has to send it the way a client would.)
fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}
