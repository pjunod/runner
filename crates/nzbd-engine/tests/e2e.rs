//! End-to-end engine tests against the in-tree mock NNTP server
//! (ARCHITECTURE.md §14): bit-identical downloads, tier failover, CRC
//! retry, health gating, pause/resume/delete, and unclean-restart resume.

use nzbd_engine::{
    AddOpts, Engine, EngineConfig, EngineHandle, Event, MirrorStats, MoveOp, Tuning,
};
use nzbd_nserv::{build_post, prng_bytes, Behavior, GeneratedPost, Nserv, NservBuilder};
use nzbd_types::{
    CertLevel, FileEntry, FileId, Job, JobId, JobKind, JobStatus, PostStage, Segment, SegmentState,
    ServerDef, ServerId, StageSpan, TlsMode,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::broadcast;

fn dormant_torrent_job() -> nzbd_types::Job {
    nzbd_types::Job {
        id: JobId(77),
        kind: nzbd_types::JobKind::Torrent,
        name: "foreign-torrent".into(),
        dir_name: "foreign-torrent".into(),
        name_provisional: false,
        queued_at_unix: 1_800_000_000,
        original_name: String::new(),
        category: None,
        priority: 0,
        dupe: Default::default(),
        params: Vec::new(),
        files: Vec::new(),
        totals: Default::default(),
        status: JobStatus::Downloading,
        torrent: Some(nzbd_types::TorrentRecord {
            info_hash_v1: "0123456789abcdef0123456789abcdef01234567".into(),
            source: nzbd_types::TorrentSource::Magnet,
            metadata_file: "torrent/0123.torrent".into(),
            payload_root: PathBuf::new(),
            phase: nzbd_types::TorrentPhase::Downloading,
            control_intent: nzbd_types::TorrentControlIntent::Running,
            removal_intent: None,
            removal_outcome: None,
            removal_confirmed_at_unix: None,
            stop_reason: None,
            files: Vec::new(),
            total_bytes: 42,
            selected_bytes: 42,
            downloaded_bytes: 21,
            uploaded_bytes: 0,
            seeding_seconds: 0,
            ready_at_unix: None,
            content_path: None,
            seed_policy: Default::default(),
            last_activity_unix: Some(1_800_000_000),
            last_error: None,
        }),
        stages: Vec::new(),
    }
}

fn transfer_job(id: u32, name: &str, status: JobStatus) -> Job {
    Job {
        id: JobId(id),
        kind: JobKind::Nzb,
        name: name.into(),
        dir_name: name.into(),
        name_provisional: false,
        queued_at_unix: 1_800_000_000,
        original_name: name.into(),
        category: Some("test".into()),
        priority: 0,
        dupe: Default::default(),
        params: vec![("tracking".into(), format!("job-{id}"))],
        files: vec![FileEntry {
            id: FileId(id * 10),
            subject: format!("{name}.bin"),
            filename: format!("{name}.bin"),
            filename_confirmed: true,
            is_par2: false,
            paused: false,
            groups: vec!["alt.test".into()],
            date: None,
            segments: vec![Segment {
                message_id: format!("{name}@example").into_boxed_str(),
                number: 1,
                size: 100,
                state: SegmentState::Pending,
            }],
            crc32: None,
            finalized: false,
        }],
        totals: Default::default(),
        status,
        torrent: None,
        stages: Vec::new(),
    }
}

fn delayed_par_job(id: u32) -> Job {
    let mut job = transfer_job(id, "recovery", JobStatus::Completed);
    job.files = [("opaque-a.par2", 100u32), ("opaque-b.par2", 250u32)]
        .into_iter()
        .enumerate()
        .map(|(index, (name, size))| FileEntry {
            id: FileId(id * 10 + index as u32),
            subject: name.into(),
            filename: name.into(),
            filename_confirmed: true,
            is_par2: true,
            paused: true,
            groups: vec!["alt.test".into()],
            date: None,
            segments: vec![Segment {
                message_id: format!("{name}@example").into_boxed_str(),
                number: 1,
                size,
                state: SegmentState::Pending,
            }],
            crc32: None,
            finalized: false,
        })
        .collect();
    job
}

fn server_def(id: u32, port: u16, tier: u8, connections: u16, pipeline: u8) -> ServerDef {
    ServerDef {
        id: ServerId(id),
        name: format!("nserv-{id}"),
        host: "127.0.0.1".into(),
        port,
        tls: TlsMode::None,
        username: None,
        password: None,
        active: true,
        tier,
        group: 0,
        fill: false,
        max_connections: connections,
        pipeline_depth: pipeline,
        retention_days: 0,
        cert_verification: CertLevel::Strict,
    }
}

fn test_tuning() -> Tuning {
    Tuning {
        retry_interval: Duration::from_millis(500),
        connect_timeout: Duration::from_secs(5),
        article_timeout: Duration::from_secs(10),
        idle_hold: Duration::from_secs(1),
        ..Tuning::default()
    }
}

async fn spawn_engine(dir: &Path, servers: Vec<ServerDef>) -> EngineHandle {
    Engine::spawn(EngineConfig::single_node(
        servers,
        dir.join("state"),
        dir.join("dest"),
        test_tuning(),
        None,
    ))
    .await
    .expect("engine spawn")
}

#[tokio::test]
async fn single_node_startup_restores_torrent_rows_before_backend_attachment() {
    let tmp = tempfile::tempdir().unwrap();
    let state_dir = tmp.path().join("state");
    let store = nzbd_state::SnapshotStore::open(&state_dir).unwrap();
    let mut job = dormant_torrent_job();
    job.torrent.as_mut().unwrap().last_activity_unix = Some(1);
    let id = job.id;
    let mut torrent = job.torrent.clone();
    torrent.as_mut().unwrap().phase = nzbd_types::TorrentPhase::Queued;
    store
        .save(&nzbd_state::QueueSnapshotDoc {
            jobs: vec![job],
            next_job_id: 78,
            max_active_downloads: 1,
            ..Default::default()
        })
        .unwrap();
    let engine = spawn_engine(tmp.path(), Vec::new()).await;
    assert_eq!(engine.snapshot().jobs.len(), 1);
    assert_eq!(engine.snapshot().jobs[0].id, id);
    engine.shutdown().await;
    let saved = store.load().unwrap().unwrap();
    assert_eq!(saved.jobs.len(), 1);
    let recovered_activity = saved.jobs[0].torrent.as_ref().unwrap().last_activity_unix;
    assert!(recovered_activity > torrent.as_ref().unwrap().last_activity_unix);
    torrent.as_mut().unwrap().last_activity_unix = recovered_activity;
    assert_eq!(saved.jobs[0].torrent, torrent);
    assert!(nzbd_engine::backend::torrent_wants_download_slot(
        saved.jobs[0].torrent.as_ref().unwrap(),
        saved.jobs[0].queued_at_unix,
        1_800_001_000,
    ));
}

#[tokio::test]
async fn cluster_authority_adoption_refuses_dormant_torrent_rows_without_rewrite() {
    let tmp = tempfile::tempdir().unwrap();
    let state_dir = tmp.path().join("state");
    let store = nzbd_state::SnapshotStore::open(&state_dir).unwrap();
    store
        .save(&nzbd_state::QueueSnapshotDoc {
            jobs: vec![dormant_torrent_job()],
            next_job_id: 78,
            max_active_downloads: 1,
            ..Default::default()
        })
        .unwrap();
    // A cluster worker deliberately ignores the authority snapshot at boot;
    // it must apply the same dormant-row guard when it later takes office.
    let mut config = EngineConfig::single_node(
        Vec::new(),
        state_dir,
        tmp.path().join("dest"),
        test_tuning(),
        None,
    );
    config.persist_queue = false;
    config.journal_suffix = "worker".into();
    let engine = Engine::spawn(config).await.unwrap();
    assert!(engine.snapshot().jobs.is_empty());

    let error = engine.adopt_authority().await.unwrap_err().to_string();
    assert!(error.contains("no production torrent backend"));
    assert!(engine.snapshot().jobs.is_empty());
    engine.shutdown().await;
}

#[tokio::test]
async fn cluster_authority_adoption_refuses_unreadable_snapshots_without_rewrite() {
    for (name, bytes, expected) in [
        (
            "future schema",
            br#"{"schema_version":5,"jobs":[{"kind":"future_transfer"}]}"#.as_slice(),
            "queue.json schema version 5 is newer",
        ),
        (
            "corrupt JSON",
            br#"{"schema_version":3,"jobs":["#.as_slice(),
            "queue.json",
        ),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let state_dir = tmp.path().join("state");
        nzbd_state::SnapshotStore::open(&state_dir).unwrap();
        let snapshot_path = state_dir.join("queue.json");
        std::fs::write(&snapshot_path, bytes).unwrap();
        let before = std::fs::read(&snapshot_path).unwrap();

        let mut config = EngineConfig::single_node(
            Vec::new(),
            state_dir,
            tmp.path().join("dest"),
            test_tuning(),
            None,
        );
        config.persist_queue = false;
        config.journal_suffix = "worker".into();
        let engine = Engine::spawn(config).await.unwrap();

        let error = engine.adopt_authority().await.unwrap_err().to_string();
        assert!(
            error.contains(expected),
            "{name}: unexpected error: {error}"
        );
        assert!(
            engine.snapshot().jobs.is_empty(),
            "{name}: local queue changed"
        );
        assert_eq!(
            std::fs::read(&snapshot_path).unwrap(),
            before,
            "{name}: unreadable authority snapshot was rewritten"
        );
        engine.shutdown().await;
    }
}

async fn wait_finished(
    rx: &mut broadcast::Receiver<Event>,
    job: JobId,
    secs: u64,
) -> (JobStatus, u16) {
    tokio::time::timeout(Duration::from_secs(secs), async {
        loop {
            match rx.recv().await {
                Ok(Event::JobFinished {
                    job: j,
                    status,
                    health,
                    ..
                }) if j == job => return (status, health),
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(e) => panic!("event stream closed: {e}"),
            }
        }
    })
    .await
    .expect("timed out waiting for JobFinished")
}

#[tokio::test]
async fn queue_controls_apply_atomically_and_report_missing_targets() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path(), Vec::new()).await;
    let post = build_post(
        "controls",
        &[
            ("one.bin", prng_bytes(1, 100)),
            ("two.bin", prng_bytes(2, 100)),
        ],
        100,
    );

    let first = engine
        .add_nzb_opts(
            "controls",
            post.nzb.as_bytes(),
            AddOpts {
                category: Some("movies".into()),
                priority: 10,
                paused: true,
                params: vec![("tracking".into(), "alpha".into())],
                client: Some("coverage-client".into()),
                ..AddOpts::default()
            },
        )
        .await
        .unwrap();
    let summary = engine.snapshot().jobs[0].clone();
    assert_eq!(summary.id, first);
    assert_eq!(summary.status, JobStatus::Paused);
    assert_eq!(summary.category.as_deref(), Some("movies"));
    assert_eq!(summary.priority, 10);
    assert_eq!(summary.params, vec![("tracking".into(), "alpha".into())]);

    assert!(!engine.pause_job(first).await.unwrap());
    assert!(engine.resume_job(first).await.unwrap());
    assert!(engine.pause_job(first).await.unwrap());
    assert!(engine.resume_job(first).await.unwrap());
    assert!(!engine.resume_job(first).await.unwrap());
    assert!(!engine.pause_job(JobId(999_001)).await.unwrap());
    assert!(engine.set_priority(first, 37).await.unwrap());
    assert!(!engine.set_priority(JobId(999_001), 1).await.unwrap());
    assert_eq!(engine.snapshot().jobs[0].priority, 37);

    let exported = engine.export_job(first).await.unwrap().unwrap();
    let first_file = exported.files[0].id;
    assert!(engine
        .set_file_paused(first, first_file, true)
        .await
        .unwrap());
    assert!(engine.export_job(first).await.unwrap().unwrap().files[0].paused);
    assert!(engine
        .set_file_paused(first, first_file, false)
        .await
        .unwrap());
    assert!(!engine
        .set_file_paused(first, FileId(999_001), true)
        .await
        .unwrap());
    assert!(!engine
        .set_file_paused(JobId(999_001), first_file, true)
        .await
        .unwrap());
    assert!(engine.delete_file(first, first_file).await.unwrap());
    assert!(!engine.delete_file(first, first_file).await.unwrap());
    assert_eq!(
        engine.export_job(first).await.unwrap().unwrap().files.len(),
        1
    );

    let second = engine
        .add_nzb("second", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();
    let third = engine
        .add_nzb("third", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();
    assert!(engine.move_job(third, MoveOp::Top).await.unwrap());
    assert_eq!(engine.snapshot().jobs[0].id, third);
    assert!(engine.move_job(third, MoveOp::Up).await.unwrap());
    assert_eq!(engine.snapshot().jobs[0].id, third);
    assert!(engine.move_job(third, MoveOp::Down).await.unwrap());
    assert_eq!(engine.snapshot().jobs[1].id, third);
    assert!(engine.move_job(third, MoveOp::Bottom).await.unwrap());
    assert_eq!(engine.snapshot().jobs.last().unwrap().id, third);
    assert!(!engine.move_job(JobId(999_001), MoveOp::Top).await.unwrap());

    let mut events = engine.subscribe();
    engine.pause_all("coverage-client").await.unwrap();
    assert!(matches!(
        events.recv().await.unwrap(),
        Event::QueuePauseChanged {
            paused: true,
            ref source
        } if source == "coverage-client"
    ));
    engine.resume_all("coverage-client").await.unwrap();
    assert!(matches!(
        events.recv().await.unwrap(),
        Event::QueuePauseChanged {
            paused: false,
            ref source
        } if source == "coverage-client"
    ));

    engine.set_speed_limit(Some(12_345)).await.unwrap();
    assert_eq!(engine.snapshot().speed_limit_bps, Some(12_345));
    engine.set_speed_limit(None).await.unwrap();
    assert_eq!(engine.snapshot().speed_limit_bps, None);
    assert_eq!(engine.set_max_active_downloads(0).await.unwrap(), 1);
    assert_eq!(
        engine.set_max_active_downloads(u32::MAX).await.unwrap(),
        100
    );
    assert_eq!(engine.snapshot().max_active_downloads, 100);

    let shared = engine.shared_snapshot();
    assert_eq!(shared.load_full().jobs.len(), 3);
    let mut external = engine.subscribe();
    engine.emit(Event::SpeedLimitChanged {
        bytes_per_sec: Some(77),
    });
    assert!(matches!(
        external.recv().await.unwrap(),
        Event::SpeedLimitChanged {
            bytes_per_sec: Some(77)
        }
    ));

    assert!(engine.delete_job(second, false).await.unwrap());
    assert!(!engine.delete_job(second, false).await.unwrap());
    assert_eq!(engine.snapshot().jobs.len(), 2);
    engine.shutdown().await;
}

#[tokio::test]
async fn cluster_commands_normalize_delegate_retain_and_fence_jobs() {
    let tmp = tempfile::tempdir().unwrap();
    let mut inactive_a = server_def(1, 9, 0, 3, 1);
    inactive_a.active = false;
    let mut inactive_b = server_def(2, 9, 0, 2, 1);
    inactive_b.active = false;
    let engine = spawn_engine(tmp.path(), vec![inactive_a, inactive_b]).await;

    engine
        .set_server_budgets(HashMap::from([(ServerId(1), 2), (ServerId(2), 1)]))
        .await
        .unwrap();
    let applied = engine
        .set_server_connection_caps(HashMap::from([
            (ServerId(1), 0),
            (ServerId(2), u16::MAX),
            (ServerId(999), 4),
        ]))
        .await
        .unwrap();
    assert_eq!(applied, HashMap::from([(ServerId(1), 1), (ServerId(2), 2)]));

    let mut imported = transfer_job(40, "imported", JobStatus::Downloading);
    imported.files[0].segments[0].state = SegmentState::Leased {
        server: ServerId(1),
    };
    engine.import_job(imported, false, false).await.unwrap();
    let normalized = engine.export_job(JobId(40)).await.unwrap().unwrap();
    assert_eq!(normalized.status, JobStatus::Queued);
    assert_eq!(normalized.files[0].segments[0].state, SegmentState::Pending);

    let mut assigned = engine.subscribe();
    assert!(engine
        .set_delegated(JobId(40), Some("worker-a".into()))
        .await
        .unwrap());
    assert!(matches!(
        assigned.recv().await.unwrap(),
        Event::JobAssigned {
            job: JobId(40),
            node: Some(ref node)
        } if node == "worker-a"
    ));
    engine.mirror_progress(
        JobId(40),
        MirrorStats {
            done_articles: 7,
            failed_articles: 2,
            downloaded_bytes: 77,
            health: 750,
            remaining_bytes: Some(23),
            stages: vec![StageSpan {
                stage: PostStage::ParVerify,
                started_at_unix: 1_000,
                ms: None,
            }],
        },
    );
    // A round trip behind the fire-and-forget update is a deterministic
    // mailbox barrier; no polling sleep is needed.
    engine.export_job(JobId(40)).await.unwrap();
    let summary = engine.snapshot().jobs[0].clone();
    assert_eq!(summary.assigned_node.as_deref(), Some("worker-a"));
    assert_eq!(summary.done_articles, 7);
    assert_eq!(summary.failed_articles, 2);
    assert_eq!(summary.downloaded_bytes, 77);
    assert_eq!(summary.remaining_bytes, 23);
    assert_eq!(summary.health, 750);
    assert_eq!(engine.snapshot().remaining_bytes, 23);
    assert_eq!(summary.stages.len(), 1);
    assert_eq!(summary.stages[0].stage, PostStage::ParVerify);
    assert!(summary.stages[0].ms.is_none());
    assert_eq!(
        summary.status,
        JobStatus::Downloading,
        "mirrored stages are observability only, not authority control state"
    );
    assert!(engine.set_delegated(JobId(40), None).await.unwrap());
    assert!(!engine
        .set_delegated(JobId(999), Some("worker".into()))
        .await
        .unwrap());

    assert!(engine
        .enter_post_stage(JobId(40), PostStage::ParVerify, 1_000, None)
        .await
        .unwrap());
    engine.close_post_stage_now(JobId(40), 1_002, Some(1_500));
    engine.export_job(JobId(40)).await.unwrap();
    let staged = engine.export_job(JobId(40)).await.unwrap().unwrap();
    assert_eq!(
        staged.status,
        JobStatus::Post {
            stage: PostStage::ParVerify
        }
    );
    assert_eq!(staged.stages[0].ms, Some(1_500));
    assert!(!engine
        .enter_post_stage(JobId(999), PostStage::Move, 2_000, None)
        .await
        .unwrap());
    engine.close_post_stage(JobId(999), 2_001, None).await;
    assert!(engine
        .set_job_status(JobId(40), JobStatus::PostQueued)
        .await
        .unwrap());
    assert!(!engine
        .set_job_status(JobId(999), JobStatus::Completed)
        .await
        .unwrap());

    let mut replacement = engine.export_job(JobId(40)).await.unwrap().unwrap();
    replacement.priority = 44;
    assert!(engine.import_job_if_present(replacement).await.unwrap());
    assert_eq!(
        engine
            .export_job(JobId(40))
            .await
            .unwrap()
            .unwrap()
            .priority,
        44
    );
    let mut absent = transfer_job(999, "absent", JobStatus::Queued);
    absent.priority = 88;
    assert!(!engine.import_job_if_present(absent).await.unwrap());
    assert!(engine.export_job(JobId(999)).await.unwrap().is_none());

    let mut terminal = engine.export_job(JobId(40)).await.unwrap().unwrap();
    terminal.status = JobStatus::Completed;
    let mut finished = engine.subscribe();
    engine.import_job(terminal, false, true).await.unwrap();
    assert!(matches!(
        finished.recv().await.unwrap(),
        Event::JobFinished {
            job: JobId(40),
            status: JobStatus::Completed,
            ..
        }
    ));

    engine
        .import_job(delayed_par_job(41), false, false)
        .await
        .unwrap();
    let freed = engine
        .unpause_par_blocks(JobId(41), 2, Some(50))
        .await
        .unwrap();
    assert!(freed >= 2);
    let recovery = engine.export_job(JobId(41)).await.unwrap().unwrap();
    assert_eq!(recovery.status, JobStatus::Queued);
    assert!(recovery.files.iter().any(|file| !file.paused));
    assert_eq!(
        engine
            .unpause_par_blocks(JobId(999), 2, Some(50))
            .await
            .unwrap(),
        0
    );

    engine
        .import_job(transfer_job(42, "discard", JobStatus::Queued), false, false)
        .await
        .unwrap();
    assert!(engine
        .set_delegated(JobId(42), Some("worker-b".into()))
        .await
        .unwrap());
    engine
        .retain_jobs(vec![JobId(40), JobId(41)])
        .await
        .unwrap();
    assert!(engine.export_job(JobId(42)).await.unwrap().is_none());
    assert!(engine
        .snapshot()
        .jobs
        .iter()
        .all(|job| job.assigned_node.is_none()));

    // RetainJobs demotes persistence. A fenced replace must report false and
    // restore the previous in-memory copy instead of applying the mutation.
    let before = engine.export_job(JobId(40)).await.unwrap().unwrap();
    let mut fenced = before.clone();
    fenced.priority = 99;
    assert!(!engine.import_job_if_present(fenced).await.unwrap());
    assert_eq!(
        engine
            .export_job(JobId(40))
            .await
            .unwrap()
            .unwrap()
            .priority,
        before.priority
    );
    engine.fold_job_journals(JobId(40)).await.unwrap();
    assert!(engine.remove_job_silent(JobId(41)).await.unwrap());
    assert!(!engine.remove_job_silent(JobId(41)).await.unwrap());
    engine.shutdown().await;
}

#[tokio::test]
async fn successful_authority_adoption_merges_durable_and_local_state() {
    let tmp = tempfile::tempdir().unwrap();
    let authority = spawn_engine(tmp.path(), Vec::new()).await;
    authority
        .import_job(transfer_job(70, "durable", JobStatus::Queued), false, false)
        .await
        .unwrap();
    authority.pause_all("authority").await.unwrap();
    authority.set_speed_limit(Some(4_096)).await.unwrap();
    authority.shutdown().await;

    let mut config = EngineConfig::single_node(
        Vec::new(),
        tmp.path().join("state"),
        tmp.path().join("dest"),
        test_tuning(),
        None,
    );
    config.persist_queue = false;
    config.journal_suffix = "worker".into();
    let worker = Engine::spawn(config).await.unwrap();
    worker
        .import_job(transfer_job(71, "local", JobStatus::Queued), false, false)
        .await
        .unwrap();
    assert_eq!(worker.snapshot().jobs.len(), 1);

    worker.adopt_authority().await.unwrap();
    let snapshot = worker.snapshot();
    let mut ids: Vec<JobId> = snapshot.jobs.iter().map(|job| job.id).collect();
    ids.sort();
    assert_eq!(ids, vec![JobId(70), JobId(71)]);
    assert!(snapshot.download_paused);
    assert_eq!(snapshot.speed_limit_bps, Some(4_096));
    worker.shutdown().await;
}

#[tokio::test]
async fn folding_a_foreign_journal_recovers_progress_and_surfaces_missing_disk_data() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path(), Vec::new()).await;
    engine
        .import_job(transfer_job(80, "folded", JobStatus::Queued), false, false)
        .await
        .unwrap();

    let mut foreign = nzbd_state::JobJournals::open(&tmp.path().join("state"), "lease-7").unwrap();
    foreign
        .append(&nzbd_state::JournalRecord {
            job: JobId(80),
            file: FileId(800),
            segment_number: 1,
            offset: 0,
            len: 100,
            crc32: 0x1234_5678,
            file_size: 100,
        })
        .unwrap();
    foreign.sync().unwrap();
    drop(foreign);

    let mut events = engine.subscribe();
    engine.fold_job_journals(JobId(80)).await.unwrap();
    engine.export_job(JobId(80)).await.unwrap();
    let job = engine.export_job(JobId(80)).await.unwrap().unwrap();
    assert_eq!(job.files[0].segments[0].state, SegmentState::Pending);
    assert!(!job.files[0].finalized);
    assert_eq!(job.status, JobStatus::Paused);
    assert!(job.held());
    while let Ok(event) = events.try_recv() {
        assert!(!matches!(
            event,
            Event::FileFinished { .. } | Event::JobFinished { .. }
        ));
    }
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_inflight_url_add_updates_params_without_a_second_job() {
    use std::io::{Read as _, Write as _};

    let post = build_post("deduplicated", &[("one.bin", vec![1, 2, 3])], 3);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (accepted_tx, accepted_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let nzb = post.nzb.clone();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut request = [0u8; 2048];
        let _ = socket.read(&mut request);
        accepted_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{nzb}",
            nzb.len()
        );
        let _ = socket.write_all(response.as_bytes());
    });

    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path(), Vec::new()).await;
    let url = format!("http://127.0.0.1:{port}/opaque.nzb&i=1&r=secret");
    let first = engine
        .add_url(
            "",
            &url,
            AddOpts {
                params: vec![("tracking".into(), "first".into())],
                client: Some("first-client".into()),
                ..AddOpts::default()
            },
        )
        .await
        .unwrap();
    accepted_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let duplicate = engine
        .add_url(
            "",
            &url,
            AddOpts {
                params: vec![
                    ("tracking".into(), "second".into()),
                    ("new-key".into(), "new-value".into()),
                ],
                client: Some("second-client".into()),
                ..AddOpts::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(duplicate, first);
    assert_eq!(engine.snapshot().jobs.len(), 1);
    let fetching = engine.export_job(first).await.unwrap().unwrap();
    assert_eq!(fetching.status, JobStatus::Fetching);
    assert!(fetching
        .params
        .iter()
        .any(|(key, value)| key == "tracking" && value == "second"));
    assert!(fetching
        .params
        .iter()
        .any(|(key, value)| key == "new-key" && value == "new-value"));
    assert!(fetching
        .params
        .iter()
        .any(|(key, value)| key == "*Client" && value == "second-client"));

    release_tx.send(()).unwrap();
    server.join().unwrap();
    engine.shutdown().await;
}

/// Payload with dot-heavy and escape-heavy regions: encoded lines starting
/// with '.', plus every escaped character class, at awkward boundaries.
fn nasty_bytes(len: usize) -> Vec<u8> {
    let mut v = prng_bytes(4242, len);
    for chunk in v.chunks_mut(97) {
        if chunk.len() > 8 {
            chunk[0] = 0x04; // encodes to '.' (dot-stuffing)
            chunk[1] = 0xD6; // encodes to NUL -> escaped
            chunk[2] = 0xE0; // encodes to LF -> escaped
            chunk[3] = 0xE3; // encodes to CR -> escaped
            chunk[4] = 0x13; // encodes to '=' -> escaped
        }
    }
    v
}

// ---------------------------------------------------------------------------

/// Regression: the recovered queue must be visible in the shared snapshot
/// the instant `Engine::spawn` returns — not one async tick later. The
/// first publish used to happen inside the spawned run loop, so an API
/// read in that startup window saw an empty snapshot and the UI flashed
/// "queue is empty" before the jobs appeared.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovered_snapshot_is_visible_immediately_after_spawn() {
    let tmp = tempfile::tempdir().unwrap();
    let post = build_post("persisted", &[("p.bin", prng_bytes(9, 20_000))], 10_000);

    // First engine: queue a job (no servers, so it just sits), then shut
    // down — which persists the snapshot.
    let engine = spawn_engine(tmp.path(), vec![]).await;
    engine
        .add_nzb("persisted", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while engine.snapshot().jobs.is_empty() {
        assert!(std::time::Instant::now() < deadline, "job never queued");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    engine.shutdown().await;

    // Second engine on the same state dir: the recovered job must be in
    // the snapshot IMMEDIATELY — read it synchronously, with no tick, no
    // sleep, no await between spawn and read.
    let engine = spawn_engine(tmp.path(), vec![]).await;
    let snap = engine.snapshot();
    assert_eq!(
        snap.jobs.len(),
        1,
        "recovered queue must be visible the moment spawn returns"
    );
    assert_eq!(snap.jobs[0].name, "persisted");
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multipart_articles_with_different_yenc_names_download_bit_identically() {
    let tmp = tempfile::tempdir().unwrap();
    let data = prng_bytes(42, 150_000);
    let mut post = build_post("obfuscated", &[("payload.bin", data.clone())], 10_000);
    for (i, article) in post.files[0].articles.iter_mut().enumerate() {
        let wire = article.wire.as_ref();
        let header_end = wire.windows(2).position(|w| w == b"\r\n").unwrap();
        let header = std::str::from_utf8(&wire[..header_end]).unwrap();
        let mut changed = header
            .replace("name=payload.bin", &format!("name=obfuscated-part-{i}"))
            .into_bytes();
        changed.extend_from_slice(&wire[header_end..]);
        article.wire = std::sync::Arc::new(changed);
    }
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
    let engine = spawn_engine(tmp.path(), vec![server_def(1, ns.port(), 0, 4, 3)]).await;
    let mut rx = engine.subscribe();
    let job = engine
        .add_nzb("obfuscated", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();
    let (status, health) = wait_finished(&mut rx, job, 10).await;
    assert_eq!((status, health), (JobStatus::Completed, 1000));
    let record = engine.export_job(job).await.unwrap().unwrap();
    assert!(!record.held());
    assert!(record.files[0].filename.starts_with("obfuscated-part-"));
    assert_eq!(
        std::fs::read(
            tmp.path()
                .join("dest/obfuscated")
                .join(&record.files[0].filename)
        )
        .unwrap(),
        data
    );
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn downloads_bit_identical_with_auth_and_pipelining() {
    let tmp = tempfile::tempdir().unwrap();
    let files = vec![
        ("alpha.bin".to_string(), nasty_bytes(300_000)),
        ("beta.bin".to_string(), prng_bytes(7, 4095)),
        ("gamma.bin".to_string(), prng_bytes(8, 70_001)),
    ];
    let post = build_post(
        "demo post",
        &files
            .iter()
            .map(|(n, d)| (n.as_str(), d.clone()))
            .collect::<Vec<_>>(),
        30_000,
    );
    let ns = NservBuilder::new()
        .with_post(&post)
        .credentials("alice", "s3cret")
        .start()
        .await
        .unwrap();

    let mut server = server_def(1, ns.port(), 0, 4, 3);
    server.username = Some("alice".into());
    server.password = Some("s3cret".into());

    let engine = spawn_engine(tmp.path(), vec![server]).await;
    let mut rx = engine.subscribe();
    let job = engine
        .add_nzb("demo post", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();

    let (status, health) = wait_finished(&mut rx, job, 30).await;
    assert_eq!(status, JobStatus::Completed);
    assert_eq!(health, 1000);

    for (name, data) in &files {
        let path = tmp.path().join("dest").join("demo post").join(name);
        let got =
            std::fs::read(&path).unwrap_or_else(|e| panic!("missing {}: {e}", path.display()));
        assert_eq!(&got, data, "bit-exact: {name}");
        assert!(!tmp
            .path()
            .join("dest")
            .join("demo post")
            .join(format!("{name}.part"))
            .exists());
    }

    let snap = engine.snapshot();
    assert_eq!(snap.jobs.len(), 1);
    assert_eq!(snap.jobs[0].status, JobStatus::Completed);
    assert_eq!(snap.jobs[0].remaining_bytes, 0);
    assert!(snap.session_downloaded_bytes > 300_000);

    engine.shutdown().await;
    assert!(
        !tmp.path().join("state").join("unclean.local").exists(),
        "graceful shutdown clears the unclean marker"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failover_missing_and_corrupt_articles_escalate_tiers() {
    let tmp = tempfile::tempdir().unwrap();
    let post = build_post(
        "failover",
        &[
            ("one.bin", prng_bytes(11, 50_000)),
            ("two.bin", prng_bytes(12, 50_000)),
        ],
        20_000,
    );
    let missing = post.message_id("one.bin", 2);
    let corrupt = post.message_id("two.bin", 1);

    // Tier 0: has everything except one missing article and one corrupt one.
    let ns_a = NservBuilder::new()
        .with_post(&post)
        .behavior(&missing, Behavior::NotFound)
        .behavior(&corrupt, Behavior::CorruptCrc)
        .start()
        .await
        .unwrap();
    // Tier 1: clean copies of everything.
    let ns_b = NservBuilder::new().with_post(&post).start().await.unwrap();

    let engine = spawn_engine(
        tmp.path(),
        vec![
            server_def(1, ns_a.port(), 0, 2, 2),
            server_def(2, ns_b.port(), 1, 2, 2),
        ],
    )
    .await;
    let mut rx = engine.subscribe();
    let job = engine
        .add_nzb("failover", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();
    let (status, health) = wait_finished(&mut rx, job, 30).await;
    assert_eq!(status, JobStatus::Completed);
    assert_eq!(health, 1000);

    for name in ["one.bin", "two.bin"] {
        let got = std::fs::read(tmp.path().join("dest/failover").join(name)).unwrap();
        assert_eq!(got, post.file(name).data, "{name}");
    }

    // The backup tier served exactly the two bad articles.
    assert_eq!(ns_b.hits(&missing), 1, "missing article from tier 1");
    assert_eq!(
        ns_b.hits(&corrupt),
        1,
        "corrupt article re-fetched from tier 1"
    );
    assert_eq!(ns_b.total_hits(), 2, "tier 1 must not serve anything else");
    // Tier 0 was asked for the bad ones (and failed them) exactly once each.
    assert_eq!(ns_a.hits(&missing), 1);
    assert_eq!(ns_a.hits(&corrupt), 1);

    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_connections_retry_without_losing_the_article() {
    let tmp = tempfile::tempdir().unwrap();
    let post = build_post("droppy", &[("d.bin", prng_bytes(21, 60_000))], 15_000);
    let victim = post.message_id("d.bin", 2);

    // Server A drops mid-body for one article; same-tier server M is clean.
    let ns_a = NservBuilder::new()
        .with_post(&post)
        .behavior(&victim, Behavior::DropMid)
        .start()
        .await
        .unwrap();
    let ns_m = NservBuilder::new().with_post(&post).start().await.unwrap();

    let engine = spawn_engine(
        tmp.path(),
        vec![
            server_def(1, ns_a.port(), 0, 2, 1),
            server_def(2, ns_m.port(), 0, 2, 1),
        ],
    )
    .await;
    let mut rx = engine.subscribe();
    let job = engine
        .add_nzb("droppy", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();
    let (status, health) = wait_finished(&mut rx, job, 30).await;
    assert_eq!(status, JobStatus::Completed);
    assert_eq!(health, 1000);
    let got = std::fs::read(tmp.path().join("dest/droppy/d.bin")).unwrap();
    assert_eq!(got, post.file("d.bin").data);
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unverified_holes_remain_partial_and_nonterminal() {
    let tmp = tempfile::tempdir().unwrap();
    let data = prng_bytes(31, 10 * 5000);
    let post = build_post("damaged", &[("dmg.bin", data.clone())], 5000);

    // Segments 3..=6 missing everywhere: 40% failed -> health 600 < 850.
    let mut b = NservBuilder::new().with_post(&post);
    for part in 3..=6 {
        b = b.behavior(&post.message_id("dmg.bin", part), Behavior::NotFound);
    }
    let ns = b.start().await.unwrap();

    let engine = spawn_engine(tmp.path(), vec![server_def(1, ns.port(), 0, 2, 2)]).await;
    let mut rx = engine.subscribe();
    let job = engine
        .add_nzb("damaged", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();
    for _ in 0..1200 {
        if engine
            .snapshot()
            .jobs
            .iter()
            .any(|j| j.id == job && j.control.as_ref().is_some_and(|c| c.lifecycle == "held"))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let snap = engine.snapshot();
    let held = snap.jobs.iter().find(|j| j.id == job).unwrap();
    assert_eq!(held.status, JobStatus::Paused);
    assert_eq!(held.control.as_ref().unwrap().stage, "finalize");
    assert!(!held.ready && !held.pp_done);
    while let Ok(event) = rx.try_recv() {
        assert!(!matches!(event, Event::JobFinished {job: id, ..} if id == job));
    }
    let exported = engine.export_job(job).await.unwrap().unwrap();
    let file = &exported.files[0];
    let got = std::fs::read(
        tmp.path()
            .join(format!("dest/damaged/.runner-file-{}.part", file.id.0)),
    )
    .unwrap();
    let mut expected = data.clone();
    expected[2 * 5000..6 * 5000].fill(0);
    assert_eq!(got, expected);
    assert!(!tmp.path().join("dest/damaged/dmg.bin").exists());

    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn critical_health_abort_stops_wasting_bandwidth() {
    let tmp = tempfile::tempdir().unwrap();
    let data = prng_bytes(77, 10 * 5000);
    let post = build_post("doomed", &[("doomed.bin", data)], 5000);

    // Parts 1..=4 gone everywhere (40% of bytes): health 600 < critical
    // 850 (no par2 in the set). Parts 5..=10 are served only after a long
    // delay — without the abort, the job would sit through ~3s+ of
    // downloads it can never repair.
    let mut b = NservBuilder::new().with_post(&post);
    for part in 1..=4 {
        b = b.behavior(&post.message_id("doomed.bin", part), Behavior::NotFound);
    }
    for part in 5..=10 {
        b = b.behavior(
            &post.message_id("doomed.bin", part),
            Behavior::Delay(Duration::from_secs(3)),
        );
    }
    let ns = b.start().await.unwrap();

    let engine = Engine::spawn(EngineConfig::single_node(
        vec![server_def(1, ns.port(), 0, 2, 2)],
        tmp.path().join("state"),
        tmp.path().join("dest"),
        Tuning {
            health_abort: true,
            ..test_tuning()
        },
        None,
    ))
    .await
    .expect("engine spawn");

    let started = std::time::Instant::now();
    let mut rx = engine.subscribe();
    let job = engine
        .add_nzb("doomed", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();
    let (status, health) = wait_finished(&mut rx, job, 30).await;
    assert_eq!(status, JobStatus::Failed, "unrepairable -> failed");
    // Pending delayed parts were failed by the abort, not downloaded:
    // health lands below the no-abort value of 600.
    assert!(
        health < 600,
        "abort must fail pending segments, got {health}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "job should abort quickly, took {:?}",
        started.elapsed()
    );

    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_resume_and_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let post = build_post("pausable", &[("p.bin", prng_bytes(41, 40_000))], 10_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();

    let engine = spawn_engine(tmp.path(), vec![server_def(1, ns.port(), 0, 2, 2)]).await;
    engine.pause_all("test").await.unwrap();

    let mut rx = engine.subscribe();
    let job = engine
        .add_nzb("pausable", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(ns.total_hits(), 0, "paused queue must not download");
    assert!(engine.snapshot().download_paused);

    // The pause invariant behind the 2026-07-25 "it unpauses itself"
    // report: the engine NEVER flips this flag on its own — not on a
    // tick, not on a guard pass, not on a snapshot save. Only a client
    // command moves it. (The field flapping was another API client
    // re-asserting its own state; attribution now names it.)
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        engine.snapshot().download_paused,
        "pause must hold across owner ticks until a client resumes"
    );

    engine.resume_all("test").await.unwrap();
    let (status, _) = wait_finished(&mut rx, job, 30).await;
    assert_eq!(status, JobStatus::Completed);
    assert!(!engine.snapshot().download_paused);

    // Delete with files.
    assert!(engine.delete_job(job, true).await.unwrap());
    assert!(engine.snapshot().jobs.is_empty());
    let dir = tmp.path().join("dest/pausable");
    for _ in 0..100 {
        if !dir.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!dir.exists(), "job directory removed");

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Crash-resume: run 1 downloads part of a job on a runtime that is killed
// without shutdown; run 2 recovers from snapshot + journal and must not
// re-fetch journaled segments.
// ---------------------------------------------------------------------------

fn journaled_segments(state_dir: &Path) -> Vec<u32> {
    nzbd_state::JobJournals::replay_all(state_dir)
        .unwrap_or_default()
        .into_iter()
        .map(|r| r.segment_number)
        .collect()
}

#[test]
fn resume_after_unclean_restart_refetches_nothing_done() {
    let tmp = tempfile::tempdir().unwrap();
    let data = prng_bytes(99, 40 * 4096);
    let post = build_post("resume", &[("big.bin", data.clone())], 4096);
    let journal = tmp.path().join("state");

    // ---- run 1: first five parts servable, the rest stall ----
    let rt1 = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt1.block_on(async {
        let mut b = NservBuilder::new().with_post(&post);
        for part in 6..=40 {
            b = b.behavior(
                &post.message_id("big.bin", part),
                Behavior::Delay(Duration::from_secs(120)),
            );
        }
        let ns = b.start().await.unwrap();
        let engine = spawn_engine(tmp.path(), vec![server_def(1, ns.port(), 0, 2, 2)]).await;
        engine
            .add_nzb("resume", post.nzb.as_bytes(), None, 0)
            .await
            .unwrap();
        // Wait until at least three segments are journaled.
        for _ in 0..400 {
            if journaled_segments(&journal).len() >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            journaled_segments(&journal).len() >= 3,
            "no progress before the crash"
        );
        std::mem::forget(ns); // avoid Drop-side effects during the hard kill
        drop(engine); // runtime abortion drops the remaining owners, like process exit
    });
    rt1.shutdown_timeout(Duration::from_secs(5)); // abort tasks and release process resources without a graceful snapshot

    let done_before = journaled_segments(&journal);
    assert!(done_before.len() >= 3);
    assert!(
        tmp.path().join("state").join("unclean.local").exists(),
        "unclean marker must survive the crash"
    );

    // ---- run 2: everything servable; must finish and not re-fetch ----
    let rt2 = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt2.block_on(async {
        let ns2 = NservBuilder::new().with_post(&post).start().await.unwrap();
        let engine = spawn_engine(tmp.path(), vec![server_def(1, ns2.port(), 0, 3, 2)]).await;
        let mut rx = engine.subscribe();

        // The job must have been recovered from the snapshot.
        let mut job = None;
        for _ in 0..100 {
            let snap = engine.snapshot();
            if let Some(j) = snap.jobs.first() {
                job = Some(j.id);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let job = job.expect("recovered job in snapshot");

        let (status, health) = wait_finished(&mut rx, job, 60).await;
        assert_eq!(status, JobStatus::Completed);
        assert_eq!(health, 1000);
        engine.shutdown().await;

        for seg in &done_before {
            assert_eq!(
                ns2.hits(&post.message_id("big.bin", *seg)),
                0,
                "segment {seg} was journaled before the crash and must not be re-fetched"
            );
        }
        let got = std::fs::read(tmp.path().join("dest/resume/big.bin")).unwrap();
        assert_eq!(got, data, "resumed file must be bit-identical");
    });
    rt2.shutdown_background();
}

// Keep helper types referenced (silences unused warnings when individual
// tests are filtered out).
#[allow(dead_code)]
fn _hold(_: &Nserv, _: &GeneratedPost, _: PathBuf) {}

/// URL job: the NZB is fetched over HTTP (local listener), then the job
/// queues and downloads normally; a dead URL fails the job.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn url_jobs_fetch_then_download() {
    use std::io::{Read as _, Write as _};
    let data = prng_bytes(21, 60_000);
    let post = build_post("urljob", &[("u.bin", data.clone())], 20_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();

    // One-shot HTTP server handing out the NZB.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_port = listener.local_addr().unwrap().port();
    let nzb = post.nzb.clone();
    std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let mut buf = [0u8; 2048];
        let _ = s.read(&mut buf);
        let _ = s.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{nzb}",
                nzb.len()
            )
            .as_bytes(),
        );
    });

    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path(), vec![server_def(1, ns.port(), 0, 4, 2)]).await;
    let mut rx = engine.subscribe();
    let id = engine
        .add_url(
            "urljob",
            &format!("http://127.0.0.1:{http_port}/get.nzb"),
            nzbd_engine::AddOpts::default(),
        )
        .await
        .unwrap();
    // Registered instantly in Fetching state.
    assert!(engine
        .snapshot()
        .jobs
        .iter()
        .any(|j| j.id == id && matches!(j.status, JobStatus::Fetching)));

    let (status, _) = wait_finished(&mut rx, id, 30).await;
    assert_eq!(status, JobStatus::Completed);
    assert_eq!(
        std::fs::read(tmp.path().join("dest/urljob/u.bin")).unwrap(),
        data
    );

    // Dead URL: job fails (history classification is FAILURE/FETCH,
    // asserted at the PP layer).
    let dead = engine
        .add_url(
            "deadjob",
            "http://127.0.0.1:1/nope.nzb",
            nzbd_engine::AddOpts::default(),
        )
        .await
        .unwrap();
    let (status, _) = wait_finished(&mut rx, dead, 30).await;
    assert_eq!(status, JobStatus::Failed);
    engine.shutdown().await;
}

/// Field report 2026-07-25: URL jobs restarted mid-fetch sat at
/// "FETCHING · 0 B of 0 B" forever (the fetch task died with the old
/// process and recovery never re-spawned it), each showing the raw URL
/// tail — query junk, API key and all — as its title. This drives the
/// whole arc: junk-free placeholder name at add time, same-URL re-adds
/// deduplicated to the in-flight job, a shutdown that doesn't hang on the
/// stalled fetch, and a restart that re-spawns the fetch and completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn url_fetch_resumes_after_restart_with_clean_name() {
    use std::io::{Read as _, Write as _};
    let data = prng_bytes(33, 50_000);
    let post = build_post("restartfetch", &[("r.bin", data.clone())], 20_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();

    // HTTP server: connection #1 stalls (reads the request, then holds the
    // socket until the client goes away — the pre-restart fetch);
    // connection #2 serves the NZB (the post-restart re-fetch).
    //
    // `ready_tx` fires once connection #1 has delivered its request: the
    // test MUST NOT shut engine A down before then, or the cancel races
    // the fetch's connect — and if cancel wins, the pre-restart fetch
    // never dials, connection #1 goes to the POST-restart fetch instead,
    // which then hangs forever in the stall slot (caught on Paul's
    // machine: "did not complete after restart; status: Fetching").
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_port = listener.local_addr().unwrap().port();
    let nzb = post.nzb.clone();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let mut buf = [0u8; 2048];
        let _ = s.read(&mut buf); // request head
        let _ = ready_tx.send(()); // pre-restart fetch is pinned on this socket
                                   // Drain until EOF — hyper may split writes, and a non-zero read
                                   // must not be mistaken for the close.
        loop {
            match s.read(&mut buf) {
                Ok(0) | Err(_) => break, // fetch task cancelled → socket gone
                Ok(_) => {}
            }
        }
        drop(s);
        let (mut s, _) = listener.accept().unwrap();
        let _ = s.read(&mut buf);
        let _ = s.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{nzb}",
                nzb.len()
            )
            .as_bytes(),
        );
    });

    // Indexer-style URL with the query glued on after ".nzb" — the exact
    // shape that used to become the job title verbatim.
    let url = format!(
        "http://127.0.0.1:{http_port}/getnzb/af51ab64582e226f4bc8de91b7b757d8067ba8e6.nzb&i=136144&r=secretapikey"
    );

    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path(), vec![server_def(1, ns.port(), 0, 4, 2)]).await;
    let id = engine
        .add_url("", &url, nzbd_engine::AddOpts::default())
        .await
        .unwrap();

    // The Fetching placeholder is named from the URL *without* the glued
    // query string (which carried the user's API key).
    let snap = engine.snapshot();
    let job = snap.jobs.iter().find(|j| j.id == id).unwrap();
    assert!(matches!(job.status, JobStatus::Fetching));
    assert_eq!(job.name, "af51ab64582e226f4bc8de91b7b757d8067ba8e6");

    // A client re-adding the same URL while it fetches must not queue a
    // second copy of the download.
    let again = engine
        .add_url("", &url, nzbd_engine::AddOpts::default())
        .await
        .unwrap();
    assert_eq!(
        again, id,
        "same-URL add while fetching returns the same job"
    );
    assert_eq!(engine.snapshot().jobs.len(), 1);

    // Only shut down once the fetch is provably mid-flight on the stall
    // socket (see the listener comment: shutdown racing the connect flips
    // which fetch lands in the stall slot).
    tokio::task::spawn_blocking(move || ready_rx.recv_timeout(Duration::from_secs(10)))
        .await
        .unwrap()
        .expect("pre-restart fetch never reached the HTTP listener");

    // Graceful shutdown with the fetch mid-flight: the cancel-aware fetch
    // task must abort promptly instead of holding shutdown for its 60 s
    // hop timeout.
    tokio::time::timeout(Duration::from_secs(10), engine.shutdown())
        .await
        .expect("shutdown must not hang on an in-flight NZB fetch");

    // Restart on the same state: recovery re-spawns the fetch, the NZB
    // arrives, and the job downloads to completion.
    let engine = spawn_engine(tmp.path(), vec![server_def(1, ns.port(), 0, 4, 2)]).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let snap = engine.snapshot();
        let job = snap
            .jobs
            .iter()
            .find(|j| j.id == id)
            .expect("job survives restart");
        if matches!(job.status, JobStatus::Completed) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "job did not complete after restart; status: {:?}",
            job.status
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        std::fs::read(
            tmp.path()
                .join("dest/af51ab64582e226f4bc8de91b7b757d8067ba8e6/r.bin")
        )
        .unwrap(),
        data
    );
    engine.shutdown().await;
}

/// The disk-low guard reads a CACHED free-space value maintained by a
/// dedicated prober task — the owner loop never calls statvfs itself. On a
/// write-saturated FUSE/network destination that syscall blocks for
/// seconds, and running it inline every 10th tick starved lease handout
/// (throughput sawtoothed ~30% down and slowly back; field report
/// 2026-07-26). This drives the whole wired path: prober task → atomic →
/// guard tick → published snapshot.
#[tokio::test]
async fn disk_low_guard_flips_from_the_cached_probe() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join("dest")).unwrap();
    let mut tuning = test_tuning();
    tuning.min_free_disk_bytes = u64::MAX; // no real volume clears this bar
    let engine = Engine::spawn(EngineConfig::single_node(
        vec![],
        tmp.path().join("state"),
        tmp.path().join("dest"),
        tuning,
        None,
    ))
    .await
    .expect("engine spawn");
    // A nonzero floor starts fail-safe, while the first probe runs without
    // delaying Engine::spawn. Wait for the held snapshot to carry that
    // completed probe's exact evidence rather than accepting the intentional
    // initial held/unknown snapshot.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let snap = engine.snapshot();
        if snap.disk_low && snap.disk_guard_free_bytes.is_some() {
            assert_eq!(snap.disk_guard_label.as_deref(), Some("downloads"));
            assert_eq!(
                snap.disk_guard_path.as_deref(),
                Some(tmp.path().join("dest").to_string_lossy().as_ref())
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "disk_low never flipped from the cached probe"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    engine.shutdown().await;
}

#[tokio::test]
async fn storage_inventory_publishes_when_the_enforcing_floor_is_disabled() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join("dest")).unwrap();
    let mut tuning = test_tuning();
    tuning.min_free_disk_bytes = 0;
    let engine = Engine::spawn(EngineConfig::single_node(
        vec![],
        tmp.path().join("state"),
        tmp.path().join("dest"),
        tuning,
        None,
    ))
    .await
    .expect("engine spawn");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let snapshot = engine.snapshot();
        if !snapshot.storage_volumes.is_empty() {
            assert!(!snapshot.disk_low);
            assert!(snapshot.storage_volumes[0].current);
            assert_eq!(snapshot.storage_volumes[0].label, "downloads");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "storage inventory was not published with the floor disabled"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    engine.shutdown().await;
}

/// An observed ENOSPC is ground truth and the statvfs forecast is not.
///
/// Field report 2026-07-31: writers, finalize and post-processing all took
/// `No space left on device` for hours while `/api/v1/status` reported
/// `disk_low: false` and intake ran at wire speed — 725 GB downloaded in a
/// day, all of it onto a full volume. The probe's own slow-probe warning
/// fired 42 times. So the write path reports, the guard latches
/// immediately, and only an operator resume (or, elsewhere, twice the
/// floor free) lets go.
#[tokio::test]
async fn an_observed_enospc_latches_the_disk_guard() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join("dest")).unwrap();
    // No statvfs floor at all: the latch must hold intake on its own, or
    // a mount whose free-space answer is a lie stops nothing.
    let engine = Engine::spawn(EngineConfig::single_node(
        vec![],
        tmp.path().join("state"),
        tmp.path().join("dest"),
        test_tuning(),
        None,
    ))
    .await
    .expect("engine spawn");
    assert!(!engine.snapshot().disk_low);

    engine.report_out_of_space("write /working/x.part: No space left on device (os error 28)");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let snap = engine.snapshot();
        if snap.disk_low {
            assert_eq!(snap.enospc_observed, 1);
            assert!(snap.disk_guard_write_latched);
            assert!(
                snap.enospc_where
                    .as_deref()
                    .is_some_and(|w| w.contains("x.part")),
                "the banner has to name what failed: {:?}",
                snap.enospc_where
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "an observed ENOSPC never flipped the guard"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The operator has seen it; resuming the queue is the override.
    engine.resume_all("test").await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if !engine.snapshot().disk_low {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "resume never cleared the out-of-space latch"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // The count is a record, not a state: it survives the clear.
    assert_eq!(engine.snapshot().enospc_observed, 1);
    assert!(!engine.snapshot().disk_guard_write_latched);
    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Naming an obfuscated post from its own metadata
// ---------------------------------------------------------------------------

/// Minimal par2 file carrying only what naming needs: a Main packet (so it
/// is a well-formed set) and one FileDesc per real filename.
fn par2_bytes(names: &[&str]) -> Vec<u8> {
    use md5::Digest;
    fn packet(ptype: &[u8; 16], body: &[u8]) -> Vec<u8> {
        let len = 64 + body.len();
        let mut p = Vec::with_capacity(len);
        p.extend_from_slice(b"PAR2\0PKT");
        p.extend_from_slice(&(len as u64).to_le_bytes());
        p.extend_from_slice(&[0u8; 16]); // packet digest filled below
        p.extend_from_slice(&[0u8; 16]); // recovery set id
        p.extend_from_slice(ptype);
        p.extend_from_slice(body);
        while p.len() % 4 != 0 {
            p.push(0);
        }
        let length = p.len() as u64;
        p[8..16].copy_from_slice(&length.to_le_bytes());
        let digest = md5::Md5::digest(&p[32..]);
        p[16..32].copy_from_slice(&digest);
        p
    }
    let mut main = 384_000u64.to_le_bytes().to_vec();
    main.extend_from_slice(&(names.len() as u32).to_le_bytes());
    main.extend_from_slice(&[0u8; 8]);
    let mut out = packet(b"PAR 2.0\0Main\0\0\0\0", &main);
    for (i, n) in names.iter().enumerate() {
        let mut body = Vec::new();
        body.extend_from_slice(&[i as u8; 16]); // file id
        body.extend_from_slice(&[0u8; 16]); // full md5
        body.extend_from_slice(&[i as u8; 16]); // md5 of first 16k
        body.extend_from_slice(&1_000u64.to_le_bytes());
        body.extend_from_slice(n.as_bytes());
        while body.len() % 4 != 0 {
            body.push(0);
        }
        out.extend(packet(b"PAR 2.0\0FileDesc", &body));
    }
    out
}

/// Field report 2026-07-29 (job #182): a 4.8 GiB download whose title,
/// whose every payload filename, and whose par2 files were all random
/// tokens — "that is not useful at all". Nothing in the NZB names it.
///
/// But the recovery index does, and it lands early: #182's was 20.5 KiB
/// and finished one minute into the download. This asserts the whole arc —
/// the placeholder says who asked, the par2 file renames the job as soon
/// as it hits disk, and the storage directory does NOT move underneath the
/// writers when it happens.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_obfuscated_job_names_itself_from_its_par2_metadata() {
    let real = "Some.Movie.2024.1080p.WEB-DL.DDP5.1-GRP";
    let post = build_post(
        "obfuscated",
        &[
            // The recovery index is FIRST and tiny, so it finalizes while
            // the payload is still arriving — the whole point of doing this
            // during the download rather than at post-processing.
            (
                "LKKp171CWZ3IrtvUyiLuNWIqWtos",
                par2_bytes(&[&format!("{real}.part01.rar"), &format!("{real}.part02.rar")]),
            ),
            ("XyfmaV5wXwfrrrqbVHgvsqC8b2ztZK", prng_bytes(31, 120_000)),
        ],
        16_000,
    );
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path(), vec![server_def(1, ns.port(), 0, 4, 2)]).await;
    let mut rx = engine.subscribe();

    let hash = "cc310b9901757996b0bdfd880c666e3812e6531d";
    let id = engine
        .add_nzb_opts(
            hash,
            post.nzb.as_bytes(),
            nzbd_engine::AddOpts {
                client: Some("monarr".into()),
                ..nzbd_engine::AddOpts::default()
            },
        )
        .await
        .unwrap();

    // Before any file lands there is no evidence at all, so the job is
    // named by who asked for it — not by the hash.
    let snap = engine.snapshot();
    let job = snap.jobs.iter().find(|j| j.id == id).unwrap();
    assert_eq!(
        job.name, "monarr · cc310b99",
        "with no evidence, say who asked (got {:?})",
        job.name
    );

    let (status, _health) = wait_finished(&mut rx, id, 60).await;
    assert!(matches!(status, JobStatus::Completed), "{status:?}");

    // The par2 index named it, and it did so from metadata the NZB never
    // carried.
    let snap = engine.snapshot();
    let job = snap.jobs.iter().find(|j| j.id == id).unwrap();
    assert_eq!(job.name, real, "the job named itself from its par2 packets");

    // …and every file is in ONE directory, the one it started in. A rename
    // that moved the storage name would have split them.
    let exported = engine.export_job(id).await.unwrap().unwrap();
    assert_eq!(
        exported.dir_name,
        nzbd_engine::queue::sanitize_name("monarr · cc310b99"),
        "the storage directory must not move under the writers"
    );
    let dir = tmp.path().join("dest").join(&exported.dir_name);
    let on_disk: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        on_disk.len(),
        2,
        "both files landed in one directory: {on_disk:?}"
    );

    engine.shutdown().await;
}

#[tokio::test]
async fn public_resume_releases_allocation_hold_but_not_identity_hold() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path(), Vec::new()).await;
    let post = build_post(
        "allocation-control",
        &[("payload.bin", prng_bytes(1, 100))],
        100,
    );
    let job = engine
        .add_nzb_opts(
            "allocation-control",
            post.nzb.as_bytes(),
            AddOpts::default(),
        )
        .await
        .unwrap();
    assert!(engine
        .hold_job(job, "allocation", "download_write", "allocation refused")
        .await
        .unwrap());
    assert!(engine.resume_job(job).await.unwrap());
    let control = engine
        .export_job(job)
        .await
        .unwrap()
        .unwrap()
        .control()
        .unwrap();
    assert_eq!(control.lifecycle, "running");
    assert_eq!(control.retry_policy, "resume_same_job");
    assert!(engine
        .hold_job(job, "identity_conflict", "extract", "review required")
        .await
        .unwrap());
    assert!(!engine.resume_job(job).await.unwrap());
    engine.shutdown().await;
}
