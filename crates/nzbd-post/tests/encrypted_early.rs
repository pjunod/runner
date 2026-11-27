//! An encrypted release is judged by its first archive volume while it
//! downloads (`nzbd_engine::archive_probe`, `nzbd_post::archive_lock`):
//! real provider, real writers, real owner, the configured extractor.
//!
//! * No known password opens it: the download stops after that volume and
//!   the job ends `failed/unpack`, blaming the release — the outcome unpack
//!   would have reached after downloading everything.
//! * The release's password opens it: it downloads and unpacks normally.
use nzbd_engine::{Engine, EngineConfig, EngineHandle, Event, Tuning};
use nzbd_nserv::{build_post, prng_bytes, Behavior, GeneratedPost, NservBuilder};
use nzbd_post::archive_lock::EncryptedArchiveProbe;
use nzbd_post::manager::{process_job, PostConfig, PpFinal};
use nzbd_state::history::HistoryDb;
use nzbd_types::{CertLevel, JobId, JobStatus, ServerDef, ServerId, TlsMode};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const LATER_VOLUMES: usize = 4;
const ARTICLE: usize = 16 * 1024;
const LATER_VOLUME_BYTES: usize = 16 * ARTICLE;

/// A RAR5 first volume whose headers are encrypted: the signature, then an
/// archive encryption header (type 4), then opaque bytes.
fn header_encrypted_rar5() -> Vec<u8> {
    let mut out = b"Rar!\x1a\x07\x01\x00".to_vec();
    // CRC32, header size, type 4, flags 0, version 0, flags 1, kdf 15, salt.
    out.extend_from_slice(&[0, 0, 0, 0, 21, 4, 0, 0, 1, 15]);
    out.extend_from_slice(&[0x5a; 16]);
    out.extend_from_slice(&[0; 1]);
    out.extend(prng_bytes(7, 4096));
    out
}

/// Stands in for unrar: `t`/`x` with `-pright` succeed (`x` writes the set's
/// bytes as the film), anything else is refused the way unrar refuses a
/// RAR5 password. Every password it is given is logged.
fn unrar(root: &Path) -> String {
    use std::os::unix::fs::PermissionsExt;
    let path = root.join("fake-unrar");
    let log = root.join("passwords-tried");
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\nmode=$1; pw=; last=\nfor a; do case \"$a\" in -p*) pw=\"${{a#-p}}\";; esac; last=\"$a\"; done\n\
             echo \"$mode $pw\" >> '{}'\n\
             if [ \"$pw\" != right ]; then echo 'The specified password is incorrect.' >&2; exit 11; fi\n\
             if [ \"$mode\" = x ]; then mkdir -p \"$last\"; cat release.part*.rar > \"$last/movie.mkv\"; fi\n\
             echo 'All OK'; exit 0\n",
            log.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path.to_string_lossy().into_owned()
}

fn post_config(root: &Path) -> PostConfig {
    let tool = unrar(root);
    PostConfig {
        unrar_cmd: tool.clone(),
        sevenzip_cmd: tool,
        deobfuscate_final: false,
        ..PostConfig::default()
    }
}

fn release() -> GeneratedPost {
    let mut inputs = vec![("release.part1.rar".to_owned(), header_encrypted_rar5())];
    inputs.extend((2..=LATER_VOLUMES + 1).map(|n| {
        (
            format!("release.part{n}.rar"),
            prng_bytes(100 + n as u64, LATER_VOLUME_BYTES),
        )
    }));
    let refs: Vec<_> = inputs
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    build_post("encrypted-release", &refs, ARTICLE)
}

/// Later volumes are slow, so the first one lands well before them.
fn provider(post: &GeneratedPost, delay: Duration) -> NservBuilder {
    let mut builder = NservBuilder::new().with_post(post);
    for n in 2..=LATER_VOLUMES + 1 {
        let file = post.file(&format!("release.part{n}.rar"));
        for part in 1..=file.articles.len() as u32 {
            builder = builder.behavior(
                &post.message_id(&format!("release.part{n}.rar"), part),
                Behavior::Delay(delay),
            );
        }
    }
    builder
}

async fn engine(root: &Path, port: u16, post: &PostConfig) -> (EngineHandle, Arc<HistoryDb>) {
    let history = Arc::new(HistoryDb::open(&root.join("history.sqlite"), Some(root)).unwrap());
    let mut cfg = EngineConfig::single_node(
        vec![ServerDef {
            id: ServerId(1),
            name: "provider".into(),
            host: "127.0.0.1".into(),
            port,
            tls: TlsMode::None,
            username: None,
            password: None,
            active: true,
            tier: 0,
            group: 0,
            fill: false,
            max_connections: 4,
            pipeline_depth: 1,
            retention_days: 0,
            cert_verification: CertLevel::Strict,
        }],
        root.join("state"),
        root.join("data"),
        Tuning {
            min_free_disk_bytes: 0,
            ..Tuning::default()
        },
        None,
    );
    cfg.history = Some(history.clone());
    cfg.archive_probe = Some(Arc::new(EncryptedArchiveProbe::new(post.clone())));
    (Engine::spawn(cfg).await.unwrap(), history)
}

async fn finished(events: &mut tokio::sync::broadcast::Receiver<Event>, job: JobId) -> JobStatus {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match events.recv().await {
                Ok(Event::JobFinished {
                    job: id, status, ..
                }) if id == job => return status,
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(error) => panic!("event stream: {error}"),
            }
        }
    })
    .await
    .expect("the job must finish")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn encrypted_release_without_a_working_password_stops_after_its_first_volume() {
    let tmp = tempfile::tempdir().unwrap();
    let post = release();
    let later_articles: u32 = (2..=LATER_VOLUMES + 1)
        .map(|n| post.file(&format!("release.part{n}.rar")).articles.len() as u32)
        .sum();
    let nserv = provider(&post, Duration::from_millis(250))
        .start()
        .await
        .unwrap();
    let cfg = post_config(tmp.path());
    let (engine, _history) = engine(tmp.path(), nserv.port(), &cfg).await;
    let mut events = engine.subscribe();
    let job = engine
        .add_nzb("encrypted{{wrong}}", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();

    assert_eq!(finished(&mut events, job).await, JobStatus::Failed);
    let terminal = engine.artifacts().terminal(job.0).unwrap().unwrap();
    assert_eq!(terminal.reason, Some(nzbd_types::FailureReason::Unpack));
    assert!(terminal.blames_release);
    assert!(
        terminal
            .message
            .as_deref()
            .is_some_and(|message| message.contains("encrypted")),
        "{:?}",
        terminal.message
    );
    // Only the release's own password was tried, by a test (`t`) run.
    let tried = std::fs::read_to_string(tmp.path().join("passwords-tried")).unwrap();
    assert_eq!(tried.trim(), "t wrong");
    // The download stopped: only the articles already requested when the
    // first volume landed were fetched (the provider counts a pipelined
    // request when it reads it, after the delay of the one before), never
    // the later volumes.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let first = post.file("release.part1.rar").articles.len() as u32;
    let fetched = nserv.total_hits();
    assert!(
        fetched < first + later_articles / 4,
        "fetched {fetched} of {} articles",
        first + later_articles
    );
    let snapshot = engine.snapshot();
    assert!(!snapshot
        .jobs
        .iter()
        .any(|record| record.id == job && record.status != JobStatus::Failed));
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn encrypted_release_whose_password_works_downloads_and_unpacks() {
    let tmp = tempfile::tempdir().unwrap();
    let post = release();
    let nserv = provider(&post, Duration::from_millis(20))
        .start()
        .await
        .unwrap();
    let cfg = post_config(tmp.path());
    let (engine, history) = engine(tmp.path(), nserv.port(), &cfg).await;
    let mut events = engine.subscribe();
    let job = engine
        .add_nzb("encrypted{{right}}", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();

    assert_eq!(finished(&mut events, job).await, JobStatus::Completed);
    let admission = tokio::time::timeout(
        Duration::from_secs(10),
        engine.wait_post_admission(job, &CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    let scoped = engine.for_post_admission(&admission);
    let out = process_job(&scoped, &cfg, &history, &tmp.path().join("data"), job).await;
    engine.end_post_admission(admission).await.unwrap();
    assert!(matches!(out, Ok(PpFinal::Success)), "{out:?}");
    // The first volume was tested once with the release's password, and
    // later volumes were not asked about again.
    let tried = std::fs::read_to_string(tmp.path().join("passwords-tried")).unwrap();
    let tests: Vec<_> = tried
        .lines()
        .filter(|line| line.starts_with("t "))
        .collect();
    assert_eq!(tests, ["t right"], "{tried}");
    assert!(tried.lines().any(|line| line == "x right"), "{tried}");
    engine.shutdown().await;
}
