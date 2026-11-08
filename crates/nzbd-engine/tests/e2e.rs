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

#[tokio::test]
async fn startup_maintenance_retires_preexisting_deleted_workspace_without_api_action() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    let processing = tmp.path().join("processing");
    std::fs::create_dir(&processing).unwrap();
    let inventory = nzbd_state::artifacts::Inventory::open(&state).unwrap();
    let path = processing.join("old-job");
    let source = inventory.allocate(12, &processing, &path).unwrap();
    std::fs::write(path.join("original"), b"recoverable original").unwrap();
    let workspace = inventory.workspace(12, "par_repair", "abc").unwrap();
    std::fs::write(workspace.scratch.path.join("scratch"), b"obsolete scratch").unwrap();
    inventory.finish_workspace(&workspace).unwrap();
    inventory
        .finish(12, &path, &processing, "retained")
        .unwrap();
    let history = std::sync::Arc::new(
        nzbd_state::history::HistoryDb::open(&state.join("history.sqlite"), None).unwrap(),
    );
    let entry = serde_json::from_value(serde_json::json!({
        "job": 12, "name": "old deleted job", "status": "DELETED", "size": 20,
        "completed_at_unix": source.created_at, "final_dir": path,
        "params": [["Artifact:Id", source.id]]
    }))
    .unwrap();
    history.record(&entry).unwrap();
    drop(inventory);

    let mut cfg = EngineConfig::single_node(vec![], state, processing, Tuning::default(), None);
    cfg.history = Some(history);
    let engine = Engine::spawn(cfg).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while engine.artifacts().get(&workspace.scratch.id).unwrap().state != "deleted" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("startup maintenance must reclaim the old scratch without another delete request");
    assert!(!workspace.scratch.path.exists());
    assert_eq!(
        engine.artifacts().get(&workspace.scratch.id).unwrap().state,
        "deleted"
    );
    assert_eq!(
        std::fs::read(path.join("original")).unwrap(),
        b"recoverable original"
    );
    engine.shutdown().await;
}

fn dormant_torrent_job() -> nzbd_types::Job {
    nzbd_types::Job {
        id: JobId(77),
        kind: nzbd_types::JobKind::Torrent,
        name: "foreign-torrent".into(),
        dir_name: "foreign-torrent".into(),
        name_provisional: false,
        delivery_lifecycle: false,
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
        delivery_lifecycle: false,
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

// Imports acknowledge durable queue replacement before runtime payload
// verification finishes. Fixtures wait for that real public fence to clear.
trait FixtureEngine {
    async fn export_ready(&self, job: JobId) -> Result<Option<Job>, nzbd_engine::EngineError>;
    async fn import_ready(
        &self,
        job: Job,
        fold: bool,
        emit: bool,
    ) -> Result<(), nzbd_engine::EngineError>;
}
impl FixtureEngine for EngineHandle {
    async fn export_ready(&self, job: JobId) -> Result<Option<Job>, nzbd_engine::EngineError> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match self.export_job(job).await {
                    Err(nzbd_engine::EngineError::Lifecycle(reason)) if reason ==
                        "job verification or writer retirement is still in progress; retry later" => {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    result => return result,
                }
            }
        }).await.expect("fixture payload verification must finish")
    }
    async fn import_ready(
        &self,
        job: Job,
        fold: bool,
        emit: bool,
    ) -> Result<(), nzbd_engine::EngineError> {
        let id = job.id;
        self.import_job(job, fold, emit).await?;
        assert!(
            self.export_ready(id).await?.is_some(),
            "imported fixture job must remain present"
        );
        Ok(())
    }
}

async fn spawn_engine(dir: &Path, servers: Vec<ServerDef>) -> EngineHandle {
    std::fs::create_dir_all(dir.join("dest")).unwrap();
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
    let future_version = nzbd_state::QUEUE_SCHEMA_VERSION + 1;
    let future_snapshot =
        format!(r#"{{"schema_version":{future_version},"jobs":[{{"kind":"future_transfer"}}]}}"#);
    let future_error = format!("queue.json schema version {future_version} is newer");
    for (name, bytes, expected) in [
        (
            "future schema",
            future_snapshot.as_bytes(),
            future_error.as_str(),
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

async fn next_fixture_event(events: &mut broadcast::Receiver<Event>) -> Event {
    tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("fixture event must arrive after its acknowledged command")
        .expect("fixture event stream must remain open")
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

async fn wait_finished_with_snapshot(
    engine: &EngineHandle,
    rx: &mut broadcast::Receiver<Event>,
    job: JobId,
    secs: u64,
) -> (JobStatus, u16) {
    let result = tokio::time::timeout(Duration::from_secs(secs), async {
        loop {
            match rx.recv().await {
                Ok(Event::JobFinished {
                    job: actual,
                    status,
                    health,
                    ..
                }) if actual == job => return (status, health),
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(error) => panic!("event stream closed: {error}"),
            }
        }
    })
    .await;
    match result {
        Ok(finished) => finished,
        Err(error) => {
            let job = engine.export_ready(job).await.unwrap_or(None);
            panic!("timed out waiting for JobFinished: {error}; current job: {job:#?}");
        }
    }
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

    let exported = engine.export_ready(first).await.unwrap().unwrap();
    let first_file = exported.files[0].id;
    assert!(engine
        .set_file_paused(first, first_file, true)
        .await
        .unwrap());
    assert!(engine.export_ready(first).await.unwrap().unwrap().files[0].paused);
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
        engine
            .export_ready(first)
            .await
            .unwrap()
            .unwrap()
            .files
            .len(),
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
        next_fixture_event(&mut events).await,
        Event::QueuePauseChanged {
            paused: true,
            ref source
        } if source == "coverage-client"
    ));
    engine.resume_all("coverage-client").await.unwrap();
    assert!(matches!(
        next_fixture_event(&mut events).await,
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
        next_fixture_event(&mut external).await,
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
    engine.import_ready(imported, false, false).await.unwrap();
    let normalized = engine.export_ready(JobId(40)).await.unwrap().unwrap();
    assert_eq!(normalized.status, JobStatus::Queued);
    assert_eq!(normalized.files[0].segments[0].state, SegmentState::Pending);

    let mut assigned = engine.subscribe();
    assert!(engine
        .set_delegated(JobId(40), Some("worker-a".into()))
        .await
        .unwrap());
    assert!(matches!(
        next_fixture_event(&mut assigned).await,
        Event::JobAssigned {
            job: JobId(40),
            node: Some(ref node)
        } if node == "worker-a"
    ));
    engine.mirror_progress(
        JobId(40),
        MirrorStats {
            repair_progress: None,
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
    engine.export_ready(JobId(40)).await.unwrap();
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
    engine.export_ready(JobId(40)).await.unwrap();
    let staged = engine.export_ready(JobId(40)).await.unwrap().unwrap();
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

    let mut replacement = engine.export_ready(JobId(40)).await.unwrap().unwrap();
    replacement.priority = 44;
    assert!(engine.import_job_if_present(replacement).await.unwrap());
    assert_eq!(
        engine
            .export_ready(JobId(40))
            .await
            .unwrap()
            .unwrap()
            .priority,
        44
    );
    let mut absent = transfer_job(999, "absent", JobStatus::Queued);
    absent.priority = 88;
    assert!(!engine.import_job_if_present(absent).await.unwrap());
    assert!(engine.export_ready(JobId(999)).await.unwrap().is_none());

    let mut terminal = engine.export_ready(JobId(40)).await.unwrap().unwrap();
    // This assertion exercises the metadata-only terminal import event.
    // Payload-bearing imports instead require asynchronous verification and
    // must never announce completion from an unverified imported status.
    terminal.files.clear();
    terminal.status = JobStatus::Completed;
    let mut finished = engine.subscribe();
    engine.import_ready(terminal, false, true).await.unwrap();
    assert!(matches!(
        next_fixture_event(&mut finished).await,
        Event::JobFinished {
            job: JobId(40),
            status: JobStatus::Completed,
            ..
        }
    ));

    engine
        .import_ready(delayed_par_job(41), false, false)
        .await
        .unwrap();
    let freed = engine
        .unpause_par_blocks(JobId(41), 2, Some(50))
        .await
        .unwrap();
    assert!(freed >= 2);
    let recovery = engine.export_ready(JobId(41)).await.unwrap().unwrap();
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
        .import_ready(transfer_job(42, "discard", JobStatus::Queued), false, false)
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
    assert!(engine.export_ready(JobId(42)).await.unwrap().is_none());
    assert!(engine
        .snapshot()
        .jobs
        .iter()
        .all(|job| job.assigned_node.is_none()));

    // RetainJobs demotes persistence. A fenced replace must report false and
    // restore the previous in-memory copy instead of applying the mutation.
    let before = engine.export_ready(JobId(40)).await.unwrap().unwrap();
    let mut fenced = before.clone();
    fenced.priority = 99;
    assert!(!engine.import_job_if_present(fenced).await.unwrap());
    assert_eq!(
        engine
            .export_ready(JobId(40))
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
async fn post_admission_waiter_wakes_when_failed_rollback_becomes_durable() {
    let temporary = tempfile::tempdir().unwrap();
    let engine = spawn_engine(temporary.path(), vec![]).await;
    let mut failed = transfer_job(79, "rollback-wakeup", JobStatus::Failed);
    failed.files.clear();
    failed.params.push(("*PP:failure-at".into(), "123".into()));
    engine.import_ready(failed, false, false).await.unwrap();
    let admission = engine
        .begin_post_admission(JobId(79))
        .await
        .unwrap()
        .unwrap();
    let scoped = engine.for_post_admission(&admission);
    let mut candidate = scoped.export_job(JobId(79)).await.unwrap().unwrap();
    candidate
        .params
        .push((nzbd_types::PP_DONE_PARAM.into(), "1".into()));
    let blocked = temporary.path().join("state/queue.json.tmp");
    std::fs::create_dir(&blocked).unwrap();
    assert!(!scoped.import_job_if_present(candidate).await.unwrap());
    engine.end_post_admission(admission).await.unwrap();
    let cancellation = tokio_util::sync::CancellationToken::new();
    let waiting = engine.wait_post_admission(JobId(79), &cancellation);
    tokio::pin!(waiting);
    tokio::select! {
        result = &mut waiting => panic!("rollback waiter completed before storage recovery: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(100)) => {},
    }
    std::fs::remove_dir(&blocked).unwrap();
    let admission = tokio::time::timeout(Duration::from_secs(5), &mut waiting)
        .await
        .expect("durable rollback must notify the public admission waiter")
        .unwrap()
        .unwrap();
    let row = engine
        .for_post_admission(&admission)
        .export_job(JobId(79))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, JobStatus::Failed);
    assert!(row
        .params
        .iter()
        .any(|(key, value)| key == "*PP:failure-at" && value == "123"));
    assert!(!row
        .params
        .iter()
        .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM));
    engine.end_post_admission(admission).await.unwrap();
    engine.shutdown().await;
}

#[tokio::test]
async fn worker_terminal_replace_is_ephemeral_and_never_resurrects_missing_jobs() {
    let tmp = tempfile::tempdir().unwrap();
    let downloads = tmp.path().join("dest");
    std::fs::create_dir(&downloads).unwrap();
    let mut config = EngineConfig::single_node(
        vec![],
        tmp.path().join("state"),
        tmp.path().join("dest"),
        Tuning::default(),
        None,
    );
    config.persist_queue = false;
    let engine = Engine::spawn(config).await.unwrap();
    let mut job = transfer_job(77, "worker", JobStatus::Completed);
    // Terminal worker stamps require the same owned source and bound lease
    // that a real worker receives, even when its queue stays ephemeral.
    job.files.clear();
    let context = nzbd_state::artifacts::WorkerControlContext {
        lease_id: "worker-test-lease".into(),
        job_incarnation: "worker-test-incarnation".into(),
        job_revision: 1,
        control_revision: 1,
        epoch: 1,
    };
    job.params.push((
        nzbd_state::artifacts::WORKER_CONTROL_CONTEXT_PARAM.into(),
        serde_json::to_string(&context).unwrap(),
    ));
    let inventory = engine.artifacts();
    inventory
        .allocate(job.id.0, &downloads, &downloads.join("worker"))
        .unwrap();
    inventory.bind_worker_context(job.id.0, &context).unwrap();
    engine
        .import_ready(job.clone(), false, false)
        .await
        .unwrap();
    job.params
        .push((nzbd_types::PP_DONE_PARAM.into(), "SUCCESS".into()));
    assert!(engine
        .import_worker_job_if_present(job.clone())
        .await
        .unwrap());
    assert!(engine.snapshot().jobs[0].pp_done);
    assert!(!tmp.path().join("state/queue.json").exists());
    // The authority replacement API must retain its durable-commit contract.
    let mut strict = job.clone();
    strict.priority = 99;
    assert!(!engine.import_job_if_present(strict).await.unwrap());
    assert_eq!(
        engine
            .export_ready(JobId(77))
            .await
            .unwrap()
            .unwrap()
            .priority,
        job.priority
    );
    assert!(engine.remove_job_silent(JobId(77)).await.unwrap());
    assert!(!engine.import_worker_job_if_present(job).await.unwrap());
    assert!(engine.export_ready(JobId(77)).await.unwrap().is_none());
    engine.shutdown().await;
}

#[tokio::test]
async fn successful_authority_adoption_merges_durable_and_local_state() {
    let tmp = tempfile::tempdir().unwrap();
    let authority = spawn_engine(tmp.path(), Vec::new()).await;
    authority
        .import_ready(transfer_job(70, "durable", JobStatus::Queued), false, false)
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
        .import_ready(transfer_job(71, "local", JobStatus::Queued), false, false)
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
async fn rejected_authority_adoption_preserves_live_speed_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    let store = nzbd_state::SnapshotStore::open(&state).unwrap();
    store
        .save(&nzbd_state::QueueSnapshotDoc {
            speed_limit_bps: Some(4096),
            ..Default::default()
        })
        .unwrap();
    let before = std::fs::read(state.join("queue.json")).unwrap();
    let mut config = EngineConfig::single_node(
        vec![],
        state.clone(),
        tmp.path().join("dest"),
        test_tuning(),
        None,
    );
    config.persist_queue = false;
    config.persist_guard = Some(std::sync::Arc::new(|| false));
    let engine = Engine::spawn(config).await.unwrap();
    engine.set_speed_limit(Some(777)).await.unwrap();
    assert!(engine.adopt_authority().await.is_err());
    assert_eq!(engine.snapshot().speed_limit_bps, Some(777));
    assert_eq!(std::fs::read(state.join("queue.json")).unwrap(), before);
    engine.shutdown().await;
}

#[tokio::test]
async fn folding_a_foreign_journal_recovers_progress_and_surfaces_missing_disk_data() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path(), Vec::new()).await;
    engine
        .import_ready(transfer_job(80, "folded", JobStatus::Queued), false, false)
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
    engine.export_ready(JobId(80)).await.unwrap();
    let job = engine.export_ready(JobId(80)).await.unwrap().unwrap();
    assert_eq!(job.files[0].segments[0].state, SegmentState::Pending);
    assert!(!job.files[0].finalized);
    assert_eq!(job.status, JobStatus::Queued);
    assert!(
        !job.held(),
        "definitively missing bytes can be downloaded again"
    );
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
                    // A duplicate request cannot promote this previously
                    // admitted legacy URL job into the new lifecycle cohort.
                    ("monarr-delivery".into(), "1".into()),
                ],
                client: Some("second-client".into()),
                ..AddOpts::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(duplicate, first);
    assert_eq!(engine.snapshot().jobs.len(), 1);
    let fetching = engine.export_ready(first).await.unwrap().unwrap();
    assert_eq!(fetching.status, JobStatus::Fetching);
    assert!(fetching
        .params
        .iter()
        .any(|(key, value)| key == "tracking" && value == "second"));
    assert!(fetching
        .params
        .iter()
        .any(|(key, value)| key == "new-key" && value == "new-value"));
    assert!(!fetching.delivery_lifecycle);
    assert!(!fetching
        .params
        .iter()
        .any(|(key, _)| key == "monarr-delivery"));
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
    let record = engine.export_ready(job).await.unwrap().unwrap();
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
async fn missing_articles_finish_download_but_keep_partial_private() {
    let tmp = tempfile::tempdir().unwrap();
    let data = prng_bytes(31, 10 * 5000);
    let post = build_post("damaged", &[("dmg.bin", data.clone())], 5000);

    // One missing article: 90% health still requires a repair handoff.
    let mut b = NservBuilder::new().with_post(&post);
    for part in 3..=3 {
        b = b.behavior(&post.message_id("dmg.bin", part), Behavior::NotFound);
    }
    let ns = b.start().await.unwrap();

    let engine = spawn_engine(tmp.path(), vec![server_def(1, ns.port(), 0, 2, 2)]).await;
    let mut rx = engine.subscribe();
    let job = engine
        .add_nzb("damaged", post.nzb.as_bytes(), None, 0)
        .await
        .unwrap();
    // Completion publishes the durable snapshot before emitting this event.
    // Waiting for the matching event avoids observing the publish/emit window.
    let (status, _) = wait_finished(&mut rx, job, 12).await;
    assert_eq!(status, JobStatus::Completed);
    let snap = engine.snapshot();
    let held = snap.jobs.iter().find(|j| j.id == job).unwrap();
    assert_eq!(held.status, JobStatus::Completed);
    assert!(held.control.is_none());
    assert!(!held.ready && !held.pp_done);
    let exported = engine.export_ready(job).await.unwrap().unwrap();
    let file = &exported.files[0];
    let got = std::fs::read(
        tmp.path()
            .join(format!("dest/damaged/.runner-file-{}.part", file.id.0)),
    )
    .unwrap();
    let mut expected = data.clone();
    expected[2 * 5000..3 * 5000].fill(0);
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
/// retries a rate limit, then queues and downloads normally; a dead URL fails the job.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn url_jobs_fetch_then_download() {
    use std::io::{Read as _, Write as _};
    let data = prng_bytes(21, 60_000);
    let post = build_post("urljob", &[("u.bin", data.clone())], 20_000);
    let ns = NservBuilder::new().with_post(&post).start().await.unwrap();

    // The indexer rate-limits the first request, then hands out the NZB.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_port = listener.local_addr().unwrap().port();
    let nzb = post.nzb.clone();
    std::thread::spawn(move || {
        {
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = [0u8; 2048];
            let _ = s.read(&mut buf);
            s.write_all(b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
        }
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

    // Retry-After: 0 cannot undercut the production 30-second first backoff.
    let (status, _) = wait_finished_with_snapshot(&engine, &mut rx, id, 90).await;
    assert_eq!(status, JobStatus::Completed);
    let payload = engine.artifacts().for_job(id.0).unwrap().unwrap().path;
    assert_eq!(std::fs::read(payload.join("u.bin")).unwrap(), data);

    // Dead URL: job fails (history classification is FAILURE/FETCH,
    // asserted at the PP layer).
    let dead_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_url = format!(
        "http://127.0.0.1:{}/nope.nzb",
        dead_listener.local_addr().unwrap().port()
    );
    let dead_response = std::thread::spawn(move || {
        let (mut stream, _) = dead_listener.accept().unwrap();
        let mut request = [0u8; 2048];
        let _ = stream.read(&mut request);
        stream
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
    });
    let dead = engine
        .add_url("deadjob", &dead_url, nzbd_engine::AddOpts::default())
        .await
        .unwrap();
    let (status, _) = wait_finished(&mut rx, dead, 30).await;
    assert_eq!(status, JobStatus::Failed);
    dead_response.join().unwrap();
    // Contract §2.2 / plan M1 (l): the fetcher types its own terminal. A
    // 404 is the indexer answering for this item: `fetch{gone}`, blamed.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let terminal = loop {
        if let Some(terminal) = engine
            .artifacts()
            .terminal(dead.0)
            .unwrap()
            .filter(|t| t.emitted)
        {
            break terminal;
        }
        assert!(tokio::time::Instant::now() < deadline, "no fetch terminal");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(terminal.reason, Some(nzbd_types::FailureReason::Fetch));
    assert_eq!(terminal.fetch_cause, Some(nzbd_types::FetchCause::Gone));
    assert!(terminal.blames_release);
    assert_eq!(terminal.salvage, nzbd_state::artifacts::SalvageState::None);
    assert_eq!(terminal.stage.as_deref(), Some("fetch"));
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
    let payload = engine.artifacts().for_job(id.0).unwrap().unwrap().path;
    assert_eq!(std::fs::read(payload.join("r.bin")).unwrap(), data);
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_token_is_durable_after_failed_job_history_removal_and_restart() {
    use std::io::{Read as _, Write as _};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!(
        "http://127.0.0.1:{}/not-present.nzb",
        listener.local_addr().unwrap().port()
    );
    let terminal_response = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0u8; 2048];
        let _ = stream.read(&mut request);
        stream
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
    });
    let tmp = tempfile::tempdir().unwrap();
    let token = "manual-transfer-unique-0001";
    let opts = || AddOpts {
        category: Some("tv".into()),
        priority: 7,
        params: vec![
            ("monarr-transfer".into(), token.into()),
            ("monarr-delivery".into(), "1".into()),
        ],
        ..AddOpts::default()
    };

    let first_engine = spawn_engine(tmp.path(), Vec::new()).await;
    let first = first_engine.add_url("one", &url, opts()).await.unwrap();
    let mut events = first_engine.subscribe();
    let (status, _) = wait_finished_with_snapshot(&first_engine, &mut events, first, 10).await;
    assert_eq!(status, JobStatus::Failed);
    terminal_response.join().unwrap();
    assert!(first_engine.remove_job_silent(first).await.unwrap());
    assert!(first_engine
        .snapshot()
        .jobs
        .iter()
        .all(|job| job.id != first));
    first_engine.shutdown().await;

    let restarted = spawn_engine(tmp.path(), Vec::new()).await;
    let replay = restarted.add_url("one", &url, opts()).await.unwrap();
    assert_eq!(
        replay, first,
        "replay returns the original admission receipt"
    );
    assert!(restarted.snapshot().jobs.iter().all(|job| job.id != first));
    let receipt = restarted.url_admission(token).await.unwrap().unwrap();
    assert_eq!(receipt.job_id, first.0);
    assert_eq!(receipt.outcome, "failed");
    assert!(!receipt.runner_instance_id.is_empty());

    let changed = restarted
        .add_url("one", "http://127.0.0.1:1/changed.nzb", opts())
        .await
        .unwrap_err();
    assert!(matches!(
        changed,
        nzbd_engine::EngineError::RequestConflict(_)
    ));
    let changed_intent = restarted
        .add_url(
            "one",
            &url,
            AddOpts {
                category: Some("tv".into()),
                priority: 7,
                params: vec![("monarr-transfer".into(), token.into())],
                ..AddOpts::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        changed_intent,
        nzbd_engine::EngineError::RequestConflict(_)
    ));
    restarted.shutdown().await;
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
    let exported = engine.export_ready(id).await.unwrap().unwrap();
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
async fn public_resume_releases_allocation_and_identity_holds() {
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
        .hold_job(
            job,
            nzbd_types::HoldCause::Allocation,
            "download_write",
            "allocation refused",
        )
        .await
        .unwrap());
    assert!(engine.resume_job(job).await.unwrap());
    let control = engine
        .export_ready(job)
        .await
        .unwrap()
        .unwrap()
        .control()
        .unwrap();
    assert_eq!(control.lifecycle, "running");
    assert_eq!(control.retry_policy, "resume_same_job");
    assert!(engine
        .hold_job(
            job,
            nzbd_types::HoldCause::PasswordRequired,
            "extract",
            "password required",
        )
        .await
        .unwrap());
    // An operator resume is always an exit (docs/archive/HELD_JOBS_PLAN.md §4.6).
    assert!(engine.resume_job(job).await.unwrap());
    assert!(!engine.export_ready(job).await.unwrap().unwrap().held());
    engine.shutdown().await;
}

#[tokio::test]
async fn legacy_coverage_hold_recovers_without_repeating_valid_articles() {
    for manual in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let engine = spawn_engine(tmp.path(), vec![]).await;
        let mut job = transfer_job(991, "legacy-partial", JobStatus::Queued);
        let file = job.files[0].id;
        job.params
            .push((format!("*File:size:{}", file.0), "10".into()));
        job.files[0].segments[0].state = SegmentState::Done {
            offset: 0,
            len: 5,
            crc: crc32fast::hash(b"hello"),
        };
        let dir = tmp.path().join("dest/legacy-partial");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join(format!(".runner-file-{}.part", file.0)),
            b"hello\0\0\0\0\0",
        )
        .unwrap();
        job.set_control(&nzbd_types::JobControl {
            version: 1,
            revision: "7".into(),
            lifecycle: "held".into(),
            cause: "identity_conflict".into(),
            stage: "finalize".into(),
            retry_policy: "review".into(),
            message: "file coverage or expected size is unverified; partial retained".into(),
            instance: "legacy".into(),
            previous_status: Some(JobStatus::Downloading),
            manual_pause: manual,
            retry_attempt: 0,
            next_retry_at_unix: None,
            blocked_since_unix: None,
            released_at_unix: None,
            deadline_in_s: None,
            deadline_unix: None,
        });
        job.status = JobStatus::Paused;
        engine.import_ready(job, false, false).await.unwrap();
        engine.shutdown().await;
        let engine = spawn_engine(tmp.path(), vec![]).await;
        for _ in 0..100 {
            if engine
                .export_ready(JobId(991))
                .await
                .unwrap()
                .is_some_and(|j| j.files[0].finalized)
            {
                break;
            }
            if manual {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let job = engine.export_ready(JobId(991)).await.unwrap().unwrap();
        assert_eq!(job.held(), manual);
        assert_eq!(job.files[0].finalized, !manual);
        assert!(matches!(
            job.files[0].segments[0].state,
            SegmentState::Done { .. }
        ));
        assert!(!dir.join("legacy-partial.bin").exists());
        assert_eq!(job.file_needs_repair(file), !manual);
        assert!(!job.ready());
        engine.shutdown().await;
    }
}

/// Contract §2.3 / plan M1 (g): a control persisted by a pre-v2 build with
/// the terminal cause `identity_conflict` is converted at boot — before hold
/// recovery — into the terminal `failed/identity`, with its history row and
/// `job_pp_finished`, whatever the operator's pause said. It is not retried.
#[tokio::test]
async fn legacy_identity_conflict_hold_converts_at_boot() {
    for manual in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let engine = spawn_engine(tmp.path(), vec![]).await;
        // Nothing downloaded yet: no partial for startup verification to
        // judge, so the conversion below is the boot conversion's alone.
        let mut job = transfer_job(991, "legacy-partial", JobStatus::Queued);
        job.set_control(&nzbd_types::JobControl {
            version: 1,
            revision: "7".into(),
            lifecycle: "held".into(),
            cause: "identity_conflict".into(),
            stage: "finalize".into(),
            retry_policy: "review".into(),
            message: "conflicting yEnc declared size".into(),
            instance: "legacy".into(),
            previous_status: Some(JobStatus::Downloading),
            manual_pause: manual,
            retry_attempt: 0,
            next_retry_at_unix: None,
            blocked_since_unix: None,
            released_at_unix: None,
            deadline_in_s: None,
            deadline_unix: None,
        });
        job.status = JobStatus::Paused;
        engine.import_ready(job, false, false).await.unwrap();
        engine.shutdown().await;

        let history = std::sync::Arc::new(
            nzbd_state::history::HistoryDb::open(
                &tmp.path().join("history.sqlite"),
                Some(tmp.path()),
            )
            .unwrap(),
        );
        let mut cfg = EngineConfig::single_node(
            vec![],
            tmp.path().join("state"),
            tmp.path().join("dest"),
            test_tuning(),
            None,
        );
        cfg.history = Some(history.clone());
        let engine = Engine::spawn(cfg).await.expect("engine spawn");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let terminal = loop {
            if let Some(terminal) = engine
                .artifacts()
                .terminal(991)
                .unwrap()
                .filter(|t| t.emitted)
            {
                break terminal;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "legacy hold was not converted: {:?}",
                engine.snapshot().jobs
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(terminal.outcome, nzbd_types::TerminalOutcome::Failed);
        assert_eq!(terminal.reason, Some(nzbd_types::FailureReason::Identity));
        assert_eq!(terminal.stage.as_deref(), Some("finalize"));
        assert_eq!(
            terminal.message.as_deref(),
            Some("conflicting yEnc declared size"),
            "converted from the stored control, not re-diagnosed"
        );
        assert!(terminal.blames_release);
        assert!(terminal.reason.unwrap().park_always());
        assert_eq!(terminal.salvage, nzbd_state::artifacts::SalvageState::None);
        let seq = terminal.history_seq.unwrap();
        assert!(seq > 0, "the event is sent only once history is durable");
        let row = history.get_sequence(seq).unwrap().unwrap();
        assert_eq!(row.status, "FAILURE/IDENTITY");
        assert!(row
            .params
            .iter()
            .any(|(key, value)| key == "Failure:Reason" && value == "identity"));
        let summary = engine
            .snapshot()
            .jobs
            .iter()
            .find(|job| job.id == JobId(991))
            .cloned()
            .unwrap();
        assert!(summary.pp_done);
        assert_eq!(summary.status, JobStatus::Failed);
        assert_ne!(summary.status_projected, "held");
        assert_eq!(engine.run_hold_recovery_pass().await.unwrap(), 0);
        engine.shutdown().await;
    }
}

/// Field report 2026-11-01, end to end: a job held `io` · `review` by a
/// build that never scheduled download-side retries (#2183). The restart
/// reclassifies it from its own errno and the recovery pass releases it with
/// no click; a paused one stays with the operator.
#[tokio::test]
async fn stranded_storage_hold_recovers_after_restart_without_a_click() {
    for (cause, policy, message, manual) in [
        ("io", "review", "write /processing/stranded/.runner-file-1.part: Transport endpoint is not connected (os error 107)", false),
        ("io", "review", "write /processing/stranded/.runner-file-1.part: Transport endpoint is not connected (os error 107)", true),
        // A full disk written before storage holds carried a time: released
        // by the recovery pass once the probe admits writes.
        ("capacity", "resume_same_job", "write x: No space left on device (os error 28)", false),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let engine = spawn_engine(tmp.path(), vec![]).await;
        let mut job = transfer_job(992, "stranded", JobStatus::Queued);
        job.set_control(&nzbd_types::JobControl {
            version: 1,
            revision: "1".into(),
            lifecycle: "held".into(),
            cause: cause.into(),
            stage: "download_write".into(),
            retry_policy: policy.into(),
            message: message.into(),
            instance: "field".into(),
            previous_status: Some(JobStatus::Downloading),
            manual_pause: manual,
            retry_attempt: 0,
            next_retry_at_unix: None,
            blocked_since_unix: Some(1),
            released_at_unix: None,
            deadline_in_s: None,
            deadline_unix: None,
        });
        job.status = JobStatus::Paused;
        engine.import_ready(job, false, false).await.unwrap();
        engine.shutdown().await;

        let engine = spawn_engine(tmp.path(), vec![]).await;
        let control = engine
            .export_ready(JobId(992))
            .await
            .unwrap()
            .unwrap()
            .control()
            .unwrap();
        if cause == "io" {
            assert_eq!(
                control.cause, "storage_unavailable",
                "reclassified from its own errno"
            );
            assert_eq!(control.retry_policy, "scheduled_retry");
        }
        assert!(
            control.next_retry_at_unix.is_some(),
            "{cause}: due on the first pass"
        );
        assert_eq!(
            engine.run_hold_recovery_pass().await.unwrap(),
            usize::from(!manual)
        );
        let job = engine.export_ready(JobId(992)).await.unwrap().unwrap();
        assert_eq!(job.held(), manual, "{cause}: a paused hold waits for the operator");
        if manual {
            assert!(
                engine.resume_job(JobId(992)).await.unwrap(),
                "…whose retry always works"
            );
            assert!(!engine.export_ready(JobId(992)).await.unwrap().unwrap().held());
        } else {
            assert_ne!(job.status, JobStatus::Paused);
        }
        engine.shutdown().await;
    }
}

/// The recovery pass end to end on the post side (#2181's path): a held
/// post-processing job whose payload passes the custody check goes back to
/// post-processing; one whose payload is gone stays held, rescheduled, with
/// the refusal on the job.
#[tokio::test]
async fn recovery_pass_releases_post_holds_through_custody() {
    for (cause, payload_present) in [
        ("storage_unavailable", true),
        ("storage_unavailable", false),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let engine = spawn_engine(tmp.path(), vec![]).await;
        // A real admission, so the payload is owned the way a live job's is.
        let nzb = r#"<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"><file poster="p" date="1720000000" subject="&quot;f.bin&quot; yEnc (1/1)"><groups><group>a.b</group></groups><segments><segment bytes="1000" number="1">post-held@x</segment></segments></file></nzb>"#;
        let id = engine
            .add_nzb_opts("post-held", nzb.as_bytes(), AddOpts::default())
            .await
            .unwrap();
        let dir = engine.artifacts().for_job(id.0).unwrap().unwrap().path;
        let mut job = engine.export_ready(id).await.unwrap().unwrap();
        job.set_control(&nzbd_types::JobControl {
            version: 1,
            revision: "1".into(),
            lifecycle: "held".into(),
            cause: cause.into(),
            stage: "post_unpack_rename".into(),
            retry_policy: "scheduled_retry".into(),
            message: "io: read /processing/post-held/x: Input/output error (os error 5)".into(),
            instance: "field".into(),
            previous_status: Some(JobStatus::Post {
                stage: nzbd_types::PostStage::PostUnpackRename,
            }),
            manual_pause: false,
            retry_attempt: 0,
            next_retry_at_unix: None,
            blocked_since_unix: Some(1),
            released_at_unix: None,
            deadline_in_s: None,
            deadline_unix: None,
        });
        job.status = JobStatus::Paused;
        engine.import_ready(job, false, false).await.unwrap();
        engine.shutdown().await;

        let engine = spawn_engine(tmp.path(), vec![]).await;
        engine.export_ready(id).await.unwrap().unwrap();
        if !payload_present {
            // The volume answers, but the payload is not what custody
            // recorded: the retry must be refused and say so.
            std::fs::remove_dir_all(&dir).unwrap();
        }
        let released = engine.run_hold_recovery_pass().await.unwrap();
        // A deliberately missing allocation keeps generic export fenced.
        // Inspect the recovery row without granting payload-read authority.
        let job = engine
            .recovery_document()
            .await
            .unwrap()
            .jobs
            .into_iter()
            .find(|job| job.id == id)
            .unwrap();
        let control = job.control().unwrap();
        match (cause, payload_present) {
            (_, true) => {
                assert_eq!(released, 1, "{}", control.message);
                assert!(!job.held());
                assert_eq!(job.status, JobStatus::PostQueued, "back to post-processing");
            }
            (_, false) => {
                assert_eq!(released, 0);
                assert!(job.held());
                assert!(
                    control
                        .message
                        .contains("retry refused: post-processing retry refused"),
                    "{}",
                    control.message
                );
                assert!(control.next_retry_at_unix.unwrap() > 0);
                assert_eq!(control.retry_attempt, 1, "pushed out on the backoff");
            }
        }
        engine.shutdown().await;
    }
}

#[tokio::test]
async fn transient_repair_progress_is_attempt_fenced_and_absent_after_terminal_stamp() {
    use nzbd_engine::{RepairPhase, RepairProgress};
    let temp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(temp.path(), vec![]).await;
    let job = transfer_job(501, "repair-progress", JobStatus::PostQueued);
    engine.import_ready(job, false, false).await.unwrap();
    assert!(engine
        .register_repair_attempt(JobId(501), "old".into())
        .await
        .unwrap());
    let mut progress = RepairProgress {
        attempt_id: "old".into(),
        phase: RepairPhase::Matching,
        files_done: 1,
        files_total: 2,
        bytes_scanned: 65536,
        round: 0,
        recovery_blocks_available: 4,
        additional_blocks_needed: Some(1),
        last_progress_at: 10,
    };
    engine
        .update_repair_progress(JobId(501), progress.clone())
        .await;
    // Export is an ordered owner barrier, so the snapshot is deterministic.
    engine.export_ready(JobId(501)).await.unwrap();
    assert_eq!(
        engine.snapshot().jobs[0]
            .repair_progress
            .as_ref()
            .unwrap()
            .bytes_scanned,
        65536
    );
    assert!(engine
        .register_repair_attempt(JobId(501), "new".into())
        .await
        .unwrap());
    engine
        .update_repair_progress(JobId(501), progress.clone())
        .await;
    engine.close_repair_attempt(JobId(501), "old".into());
    progress.attempt_id = "new".into();
    progress.phase = RepairPhase::Verifying;
    engine.update_repair_progress(JobId(501), progress).await;
    engine.export_ready(JobId(501)).await.unwrap();
    assert_eq!(
        engine.snapshot().jobs[0]
            .repair_progress
            .as_ref()
            .unwrap()
            .attempt_id,
        "new"
    );
    let mut job = engine.export_ready(JobId(501)).await.unwrap().unwrap();
    job.params
        .push((nzbd_types::PP_DONE_PARAM.into(), "SUCCESS".into()));
    engine.import_ready(job, false, false).await.unwrap();
    assert!(engine.snapshot().jobs[0].repair_progress.is_none());
    let wire = serde_json::to_value(&engine.snapshot().jobs[0]).unwrap();
    assert!(
        wire.get("repair_progress").is_none(),
        "old clients need no new field when idle"
    );
    engine.shutdown().await;
}

#[tokio::test]
async fn mirrored_repair_progress_is_optional_node_fenced_and_cleared_on_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(temp.path(), vec![]).await;
    engine
        .import_ready(
            transfer_job(502, "remote-progress", JobStatus::Completed),
            false,
            false,
        )
        .await
        .unwrap();
    engine
        .set_delegated(JobId(502), Some("worker-a".into()))
        .await
        .unwrap();
    let repair = nzbd_engine::RepairProgress {
        attempt_id: "lease-a:1".into(),
        phase: nzbd_engine::RepairPhase::Matching,
        files_done: 1,
        files_total: 3,
        bytes_scanned: 65536,
        round: 0,
        recovery_blocks_available: 4,
        additional_blocks_needed: None,
        last_progress_at: 1000,
    };
    let stats = MirrorStats {
        repair_progress: Some(repair),
        health: 1000,
        ..Default::default()
    };
    engine.mirror_progress_from(JobId(502), "worker-a".into(), stats.clone());
    engine.export_ready(JobId(502)).await.unwrap();
    assert_eq!(
        engine.snapshot().jobs[0]
            .repair_progress
            .as_ref()
            .unwrap()
            .bytes_scanned,
        65536
    );
    engine
        .set_delegated(JobId(502), Some("worker-b".into()))
        .await
        .unwrap();
    engine.mirror_progress_from(JobId(502), "worker-a".into(), stats.clone());
    engine.export_ready(JobId(502)).await.unwrap();
    assert!(engine.snapshot().jobs[0].repair_progress.is_none());
    engine.mirror_progress_from(JobId(502), "worker-b".into(), stats);
    engine.export_ready(JobId(502)).await.unwrap();
    assert!(engine.snapshot().jobs[0].repair_progress.is_some());
    engine.mirror_progress_from(JobId(502), "worker-b".into(), MirrorStats::default());
    engine.export_ready(JobId(502)).await.unwrap();
    assert!(engine.snapshot().jobs[0].repair_progress.is_none());
    engine.shutdown().await;
}

/// Review #297 r2: a persisted hold survives a restart as `held` and the
/// hold recovery task releases it; a 0.5.0 snapshot (`paused` + a held
/// control) loads as `held`; a `held` row that lost its control loads as a
/// plain pause.
#[tokio::test]
async fn a_persisted_hold_reloads_held_and_recovers_and_a_050_snapshot_loads_held() {
    fn rewrite_snapshot(dir: &Path, job: u32, edit: impl Fn(&mut serde_json::Value)) {
        let path = dir.join("state/queue.json");
        let mut doc: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let row = doc["jobs"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|row| row["id"] == job)
            .unwrap();
        edit(row);
        std::fs::write(&path, serde_json::to_vec(&doc).unwrap()).unwrap();
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    for legacy in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let engine = spawn_engine(tmp.path(), vec![]).await;
        let mut job = transfer_job(993, "held-restart", JobStatus::Held);
        job.set_control(&nzbd_types::JobControl {
            version: 1,
            revision: "3".into(),
            lifecycle: "held".into(),
            cause: "capacity".into(),
            stage: "download_write".into(),
            retry_policy: "scheduled_retry".into(),
            message: "No space left on device (os error 28)".into(),
            instance: "restart".into(),
            previous_status: Some(JobStatus::Downloading),
            manual_pause: false,
            retry_attempt: 1,
            // Due now: the first recovery pass after boot releases it.
            next_retry_at_unix: Some(now - 1),
            blocked_since_unix: Some(now - 60),
            released_at_unix: None,
            deadline_in_s: None,
            deadline_unix: Some(now + 86_400),
        });
        engine.import_ready(job, false, false).await.unwrap();
        engine.shutdown().await;
        let snapshot: serde_json::Value =
            serde_json::from_slice(&std::fs::read(tmp.path().join("state/queue.json")).unwrap())
                .unwrap();
        let row = snapshot["jobs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["id"] == 993)
            .unwrap();
        assert_eq!(
            row["status"], "held",
            "the hold is persisted as its own status"
        );
        if legacy {
            rewrite_snapshot(tmp.path(), 993, |row| row["status"] = "paused".into());
        }
        let engine = spawn_engine(tmp.path(), vec![]).await;
        let reloaded = engine.export_ready(JobId(993)).await.unwrap().unwrap();
        assert!(reloaded.held(), "legacy={legacy}");
        assert_eq!(reloaded.status, JobStatus::Held, "legacy={legacy}");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let job = engine.export_ready(JobId(993)).await.unwrap().unwrap();
            if !job.held() {
                assert_eq!(job.status, JobStatus::Queued, "legacy={legacy}");
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "hold recovery never released the reloaded hold (legacy={legacy}): {:?}",
                job.control()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        engine.shutdown().await;
    }

    // A `held` row whose control is gone: a stop without a hold.
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path(), vec![]).await;
    engine
        .import_ready(
            transfer_job(994, "lost-control", JobStatus::Paused),
            false,
            false,
        )
        .await
        .unwrap();
    engine.shutdown().await;
    rewrite_snapshot(tmp.path(), 994, |row| row["status"] = "held".into());
    let engine = spawn_engine(tmp.path(), vec![]).await;
    let job = engine.export_ready(JobId(994)).await.unwrap().unwrap();
    assert!(!job.held());
    assert_eq!(job.status, JobStatus::Paused);
    engine.shutdown().await;
}
