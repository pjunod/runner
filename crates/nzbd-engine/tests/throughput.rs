//! Download throughput benchmark against the in-tree mock NNTP server.
//!
//! Ignored by default: it measures, it does not gate. Run it in release mode
//! when changing anything on the article path (pool, owner segment handling,
//! writer durability):
//!
//! ```text
//! cargo test --release -p nzbd-engine --test throughput -- --ignored --nocapture
//! ```
//!
//! Knobs (environment):
//!
//! - `NZBD_BENCH_MB` (default 600): payload size, one file.
//! - `NZBD_BENCH_SEGMENT_KB` (default 100): article size. Small articles over
//!   one large file stress the per-article cost of the owner and the writer,
//!   which is what grows with file size in production (a 4 GB file is ~5,700
//!   articles at the usual ~716 KB).
//! - `NZBD_BENCH_RTT_MS` (default 0): round trip added by an in-process proxy
//!   that delays every chunk in both directions without serializing them —
//!   a provider link's latency, not a bandwidth cap. Loopback has none, so
//!   without it the pipeline's round-trip behavior is invisible.
//! - `NZBD_BENCH_CONNECTIONS` (default 20), `NZBD_BENCH_DEPTH` (default 2,
//!   the shipped `pipeline_depth`).

use nzbd_engine::{Engine, EngineConfig, Event, Tuning};
use nzbd_nserv::{build_post, prng_bytes, NservBuilder};
use nzbd_types::{CertLevel, JobStatus, ServerDef, ServerId, TlsMode};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Copy `from` to `to`, delivering each chunk `delay` after it was read.
async fn delayed_pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    delay: Duration,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<(tokio::time::Instant, Vec<u8>)>();
    let reader = tokio::spawn(async move {
        let mut buffer = vec![0u8; 256 * 1024];
        loop {
            match from.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let due = tokio::time::Instant::now() + delay;
                    if tx.send((due, buffer[..n].to_vec())).is_err() {
                        break;
                    }
                }
            }
        }
    });
    while let Some((due, chunk)) = rx.recv().await {
        tokio::time::sleep_until(due).await;
        if to.write_all(&chunk).await.is_err() {
            break;
        }
    }
    let _ = to.shutdown().await;
    reader.abort();
}

/// A loopback proxy adding `rtt` (half each way) in front of `upstream`.
async fn latency_proxy(upstream: u16, rtt: Duration) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Ok(server) = TcpStream::connect(("127.0.0.1", upstream)).await else {
                    return;
                };
                client.set_nodelay(true).ok();
                server.set_nodelay(true).ok();
                let (client_read, client_write) = client.into_split();
                let (server_read, server_write) = server.into_split();
                let half = rtt / 2;
                tokio::join!(
                    delayed_pump(client_read, server_write, half),
                    delayed_pump(server_read, client_write, half),
                );
            });
        }
    });
    port
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark: run explicitly in release mode"]
async fn single_large_file_throughput() {
    let megabytes = env_usize("NZBD_BENCH_MB", 600);
    let segment = env_usize("NZBD_BENCH_SEGMENT_KB", 100) * 1024;
    let payload = prng_bytes(42, megabytes * 1024 * 1024);
    let post = build_post("bench", &[("big.bin", payload)], segment);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
    let rtt = Duration::from_millis(env_usize("NZBD_BENCH_RTT_MS", 0) as u64);
    let connections = env_usize("NZBD_BENCH_CONNECTIONS", 20) as u16;
    let depth = env_usize("NZBD_BENCH_DEPTH", 2) as u8;
    let port = if rtt.is_zero() {
        ns.port()
    } else {
        latency_proxy(ns.port(), rtt).await
    };

    let server = ServerDef {
        id: ServerId(1),
        name: "nserv-bench".into(),
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
        pipeline_depth: depth,
        retention_days: 0,
        cert_verification: CertLevel::Strict,
    };
    let tmp = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    std::fs::create_dir_all(tmp.path().join("dest")).unwrap();
    let engine = Engine::spawn(EngineConfig::single_node(
        vec![server],
        tmp.path().join("state"),
        tmp.path().join("dest"),
        Tuning {
            connect_timeout: Duration::from_secs(5),
            article_timeout: Duration::from_secs(30),
            ..Tuning::default()
        },
        None,
    ))
    .await
    .expect("engine spawn");
    let mut rx = engine.subscribe();
    let started = Instant::now();
    let job = engine
        .add_nzb("bench", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(1800), async {
        loop {
            match rx.recv().await {
                Ok(Event::JobFinished { job: j, status, .. }) if j == job => return status,
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(error) => panic!("event stream closed: {error}"),
            }
        }
    })
    .await
    .expect("benchmark job finished");
    let elapsed = started.elapsed();
    assert_eq!(status, JobStatus::Completed);
    let articles = post.files[0].articles.len();
    println!(
        "throughput: {megabytes} MiB in {articles} articles, rtt {rtt:?}, {connections}x depth {depth}: \
         {:.2}s, {:.1} MiB/s, {:.0} articles/s",
        elapsed.as_secs_f64(),
        megabytes as f64 / elapsed.as_secs_f64(),
        articles as f64 / elapsed.as_secs_f64(),
    );
    engine.shutdown().await;
}
