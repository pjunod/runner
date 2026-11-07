//! Pipeline contract v2 M1a acceptance: one terminal fact per job, first,
//! typed at the producer; salvage and disposal after it (contract §2.1–2.6,
//! plan M1 builder-runnable checks b, c, d, f, k).

use nzbd_engine::{AddOpts, Engine, EngineConfig, EngineHandle, Event, Tuning};
use nzbd_post::manager::{spawn_post_manager, FailureAction, PostConfig, PpFinal};
use nzbd_state::artifacts::{SalvageState, TerminalStep};
use nzbd_state::history::HistoryDb;
use nzbd_types::{FailureReason, JobId, JobStatus, SegmentState, TerminalOutcome};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

fn crc(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// The daemon shape: engine and post-processing share one history store.
async fn spawn(dir: &Path, tuning: Tuning) -> (EngineHandle, Arc<HistoryDb>) {
    std::fs::create_dir_all(dir.join("dest")).unwrap();
    let history = Arc::new(HistoryDb::open(&dir.join("history.sqlite"), Some(dir)).unwrap());
    let mut cfg =
        EngineConfig::single_node(vec![], dir.join("state"), dir.join("dest"), tuning, None);
    cfg.history = Some(history.clone());
    (Engine::spawn(cfg).await.expect("engine spawn"), history)
}

fn corrupt_archive_tool(root: &Path) -> String {
    use std::os::unix::fs::PermissionsExt;
    let path = root.join("corrupt-archive-tool");
    std::fs::write(&path, "#!/bin/sh\necho 'Data Error' >&2\nexit 2\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path.to_string_lossy().into_owned()
}

async fn export(engine: &EngineHandle, job: JobId) -> Option<nzbd_types::Job> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match engine.export_job(job).await {
                Err(nzbd_engine::EngineError::Lifecycle(_)) => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                result => return result.unwrap(),
            }
        }
    })
    .await
    .expect("export settles")
}

/// Admit a real delivery-lifecycle job (so its source artifact is owned the
/// way a live download's is) and write `files` as its downloaded payload.
/// `None` bytes mark a file whose article failed.
async fn admitted_job(
    engine: &EngineHandle,
    name: &str,
    files: &[(&str, Option<&[u8]>)],
    status: JobStatus,
) -> (JobId, std::path::PathBuf) {
    let mut nzb = String::from(
        r#"<?xml version="1.0" encoding="utf-8"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">"#,
    );
    for (index, (file, _)) in files.iter().enumerate() {
        nzb.push_str(&format!(
            r#"<file poster="p" date="1720000000" subject="&quot;{file}&quot; yEnc"><groups><group>alt.binaries.test</group></groups><segments><segment bytes="1" number="1">part{index}@example</segment></segments></file>"#
        ));
    }
    nzb.push_str("</nzb>");
    let job_id = engine
        .add_nzb_opts(
            name,
            nzb.as_bytes(),
            AddOpts {
                category: Some("test".into()),
                params: vec![
                    ("monarr-delivery".into(), "1".into()),
                    ("monarr-transfer".into(), format!("t-{name}")),
                ],
                ..AddOpts::default()
            },
        )
        .await
        .unwrap();
    let artifact = engine.artifacts().for_job(job_id.0).unwrap().unwrap();
    for (file, bytes) in files {
        if let Some(bytes) = bytes {
            std::fs::write(artifact.path.join(file), bytes).unwrap();
        }
    }
    engine.artifacts().refresh_for_delivery(job_id.0).unwrap();
    let mut job = export(engine, job_id).await.unwrap();
    job.status = status;
    for file in &mut job.files {
        let (name, bytes) = files
            .iter()
            .find(|(name, _)| file.subject.contains(name))
            .copied()
            .unwrap();
        file.filename = name.into();
        file.subject = name.into();
        match bytes {
            Some(bytes) => {
                file.finalized = true;
                file.crc32 = Some(crc(bytes));
                file.segments[0].state = SegmentState::Done {
                    offset: 0,
                    len: bytes.len() as u32,
                    crc: crc(bytes),
                };
                job.params
                    .push((format!("*File:size:{}", file.id.0), bytes.len().to_string()));
            }
            None => file.segments[0].state = SegmentState::Failed,
        }
    }
    engine.import_job(job, false, false).await.unwrap();
    export(engine, job_id).await.unwrap();
    (job_id, artifact.path)
}

async fn wait_terminal(
    engine: &EngineHandle,
    job: JobId,
    what: &str,
    done: impl Fn(&nzbd_state::artifacts::JobTerminal) -> bool,
) -> nzbd_state::artifacts::JobTerminal {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(terminal) = engine.artifacts().terminal(job.0).unwrap().filter(&done) {
            return terminal;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}: {:?}",
            engine.artifacts().terminal(job.0).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Plan M1 (b): a corrupt archive produces, in order, the `UNPACK_FAILURE`
/// history row and `job_pp_finished{failed, unpack, blames_release, salvage:
/// pending}` — within 5 s of the extractor exiting, before any salvage — then
/// `job_publication_ready` with the verified files, then disposal.
#[tokio::test]
async fn unpack_failure_is_terminal_before_salvage() {
    let tmp = tempfile::tempdir().unwrap();
    let (engine, history) = spawn(tmp.path(), Tuning::default()).await;
    let good = b"verified episode bytes";
    let (job_id, source) = admitted_job(
        &engine,
        "corrupt-pack",
        &[
            ("episode.mkv", Some(good)),
            ("broken.zip", Some(b"not a valid zip archive")),
        ],
        JobStatus::Completed,
    )
    .await;
    let mut events = engine.subscribe();
    let corrupt = corrupt_archive_tool(tmp.path());
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    let started = tokio::time::Instant::now();
    spawn_post_manager(
        engine.clone(),
        PostConfig {
            unrar_cmd: corrupt.clone(),
            sevenzip_cmd: corrupt,
            deobfuscate_final: false,
            failure_action: FailureAction::Delete,
            ..PostConfig::default()
        },
        history.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    let mut order = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while order.len() < 2 {
        let event = tokio::time::timeout_at(deadline, events.recv())
            .await
            .expect("terminal and salvage events")
            .unwrap();
        match event {
            Event::JobPpFinished {
                job,
                outcome,
                reason,
                blames_release,
                salvage,
                transfer,
                history_seq,
                pp_status,
                ..
            } if job == job_id => {
                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "the terminal does not wait on salvage or disposal"
                );
                assert_eq!(outcome, TerminalOutcome::Failed);
                assert_eq!(reason, Some(FailureReason::Unpack));
                assert!(blames_release);
                assert_eq!(salvage, nzbd_types::Salvage::Pending);
                assert_eq!(transfer.as_deref(), Some("t-corrupt-pack"));
                assert_eq!(pp_status, "UNPACK_FAILURE");
                let row = history.get_sequence(history_seq).unwrap().unwrap();
                assert_eq!(row.status, "UNPACK_FAILURE", "history is durable first");
                assert!(
                    engine
                        .artifacts()
                        .delivery_for_job(job_id.0)
                        .unwrap()
                        .is_none(),
                    "nothing was published before the terminal"
                );
                assert!(source.exists(), "nothing was disposed before the terminal");
                order.push("terminal");
            }
            Event::JobPublicationReady {
                job,
                publication,
                transfer,
            } if job == job_id => {
                assert_eq!(order, ["terminal"]);
                assert_eq!(transfer.as_deref(), Some("t-corrupt-pack"));
                assert_eq!(publication.files, 1);
                assert!(Path::new(&publication.path).join("episode.mkv").exists());
                assert!(!Path::new(&publication.path).join("broken.zip").exists());
                order.push("salvage");
            }
            _ => {}
        }
    }
    let terminal = wait_terminal(&engine, job_id, "disposal", |t| t.disposal.is_some()).await;
    assert_eq!(terminal.salvage, SalvageState::Published);
    let delivery = engine
        .artifacts()
        .delivery_for_job(job_id.0)
        .unwrap()
        .unwrap();
    assert_eq!(delivery.kind(), "salvage");
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
}

/// Plan M1 (c): a job held on a full disk with `hold_deadline = 2s` ends
/// `failed/hold_expired`, its source parked whatever `failure_action` says.
#[tokio::test]
async fn enospc_hold_expires_to_failed() {
    let tmp = tempfile::tempdir().unwrap();
    let tuning = Tuning {
        hold_deadline: Duration::from_secs(2),
        ..Tuning::default()
    };
    let (engine, history) = spawn(tmp.path(), tuning).await;
    let (job_id, source) = admitted_job(
        &engine,
        "full-disk",
        &[("episode.mkv", Some(b"episode"))],
        JobStatus::Completed,
    )
    .await;
    assert!(engine
        .hold_job(
            job_id,
            nzbd_types::HoldCause::Capacity,
            "unpack",
            "write staging: No space left on device (os error 28)",
        )
        .await
        .unwrap());
    let control = export(&engine, job_id).await.unwrap().control().unwrap();
    let deadline = control.deadline_unix.expect("every hold has a deadline");
    assert!(deadline - control.blocked_since_unix.unwrap() <= 2);
    let summary = engine
        .snapshot()
        .jobs
        .iter()
        .find(|job| job.id == job_id)
        .cloned()
        .unwrap();
    assert_eq!(summary.status_projected, "held");
    assert!(summary.control.unwrap().deadline_in_s.is_some());
    tokio::time::sleep(Duration::from_millis(3100)).await;
    engine.run_hold_recovery_pass().await.unwrap();
    let terminal = wait_terminal(&engine, job_id, "hold expiry", |t| t.emitted).await;
    assert_eq!(terminal.reason, Some(FailureReason::HoldExpired));
    assert_eq!(terminal.stage.as_deref(), Some("unpack"));
    assert!(!terminal.blames_release);
    assert!(terminal
        .message
        .as_deref()
        .unwrap()
        .contains("No space left on device"));
    assert_eq!(
        history
            .get_sequence(terminal.history_seq.unwrap())
            .unwrap()
            .unwrap()
            .status,
        "FAILURE/HOLD_EXPIRED"
    );
    let parked = tmp.path().join("parked");
    assert!(
        nzbd_post::manager::follow_up_failure(
            &engine,
            &PostConfig {
                failure_action: FailureAction::Delete,
                failed_dir: Some(parked.clone()),
                ..PostConfig::default()
            },
            &tmp.path().join("dest"),
            job_id,
        )
        .await
    );
    let terminal = engine.artifacts().terminal(job_id.0).unwrap().unwrap();
    let disposal = terminal.disposal.unwrap();
    // The verified episode was salvaged; what remains of the source is
    // retained (hold_expired parks — never deleted, whatever
    // failure_action says).
    assert_eq!(terminal.salvage, SalvageState::Published);
    assert_eq!(disposal, "salvage published; source retained");
    assert!(source.exists());
    engine.shutdown().await;
}

/// Plan M1 (d): a season pack with one bad episode fails `health` with
/// `salvage: pending`, then `job_publication_ready` carries the good one.
#[tokio::test]
async fn health_failure_one_bad_episode_salvages_the_rest() {
    let tmp = tempfile::tempdir().unwrap();
    let (engine, history) = spawn(tmp.path(), Tuning::default()).await;
    let good = b"S01E01 verified bytes";
    let (job_id, _) = admitted_job(
        &engine,
        "season-pack",
        &[("Show.S01E01.mkv", Some(good)), ("Show.S01E02.mkv", None)],
        JobStatus::Failed,
    )
    .await;
    let mut events = engine.subscribe();
    assert!(engine
        .fail_job(
            job_id,
            FailureReason::Health,
            None,
            "health",
            "download health below critical",
        )
        .await
        .unwrap());
    let terminal = engine.artifacts().terminal(job_id.0).unwrap().unwrap();
    assert_eq!(terminal.reason, Some(FailureReason::Health));
    assert!(terminal.blames_release);
    assert_eq!(terminal.salvage, SalvageState::Pending);
    assert_eq!(terminal.fetched.success_articles, 1);
    assert_eq!(terminal.fetched.total_articles, 2);
    assert_eq!(
        history
            .get_sequence(terminal.history_seq.unwrap())
            .unwrap()
            .unwrap()
            .status,
        "FAILURE/HEALTH"
    );
    assert!(
        nzbd_post::manager::follow_up_failure(
            &engine,
            &PostConfig {
                completed_dir: Some(tmp.path().join("completed")),
                ..PostConfig::default()
            },
            &tmp.path().join("dest"),
            job_id,
        )
        .await
    );
    let mut published = None;
    while let Ok(event) = events.try_recv() {
        if let Event::JobPublicationReady {
            job, publication, ..
        } = event
        {
            assert_eq!(job, job_id);
            published = Some(publication);
        }
    }
    let publication = published.expect("job_publication_ready");
    assert_eq!(publication.files, 1);
    assert_eq!(
        std::fs::read(Path::new(&publication.path).join("Show.S01E01.mkv")).unwrap(),
        good
    );
    let terminal = engine.artifacts().terminal(job_id.0).unwrap().unwrap();
    assert_eq!(terminal.salvage, SalvageState::Published);
    assert!(terminal.disposal.is_some());
    engine.shutdown().await;
}

/// Plan M1 (f), R3: a cleanly verified payload whose name says nothing (an
/// obfuscated Matroska file) and a disc image both publish.
#[tokio::test]
async fn obfuscated_mkv_and_iso_publish() {
    let tmp = tempfile::tempdir().unwrap();
    let (engine, history) = spawn(tmp.path(), Tuning::default()).await;
    let mkv = b"\x1a\x45\xdf\xa3 matroska payload behind an obfuscated name".to_vec();
    let mut iso = vec![0u8; 0x8010];
    iso[0x8001..0x8006].copy_from_slice(b"CD001");
    let (job_id, _) = admitted_job(
        &engine,
        "obfuscated",
        &[
            ("a8f3c91e2b7d", Some(mkv.as_slice())),
            ("Movie.2020.iso", Some(iso.as_slice())),
        ],
        JobStatus::Completed,
    )
    .await;
    let mut events = engine.subscribe();
    let cancel = CancellationToken::new();
    let admission = engine
        .wait_post_admission(job_id, &cancel)
        .await
        .unwrap()
        .expect("eligible for processing");
    let scoped = engine.for_post_admission(&admission);
    let outcome = nzbd_post::manager::process_job(
        &scoped,
        &PostConfig {
            completed_dir: Some(tmp.path().join("completed")),
            deobfuscate_final: false,
            ..PostConfig::default()
        },
        &history,
        &tmp.path().join("dest"),
        job_id,
    )
    .await
    .unwrap();
    engine.end_post_admission(admission).await.unwrap();
    assert_eq!(outcome, PpFinal::Success);
    let delivery = engine
        .artifacts()
        .delivery_for_job(job_id.0)
        .unwrap()
        .unwrap();
    assert_eq!(delivery.kind(), "completed");
    let mut paths = delivery
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<Vec<_>>();
    paths.sort();
    assert_eq!(paths, ["Movie.2020.iso", "a8f3c91e2b7d"]);
    let mut completed = None;
    while let Ok(event) = events.try_recv() {
        if let Event::JobPpFinished {
            job,
            outcome,
            publication,
            ..
        } = event
        {
            assert_eq!(job, job_id);
            assert_eq!(outcome, TerminalOutcome::Completed);
            completed = publication;
        }
    }
    let publication = completed.expect("completed terminal carries its publication");
    assert_eq!(publication.manifest_digest, delivery.manifest_digest);
    assert_eq!(publication.generation, delivery.publication_generation);
    assert_eq!(publication.files, 2);
    engine.shutdown().await;
}

/// Plan M1 (k): a crash after each step of the terminal chain — after the
/// `job_terminals` commit, after the history append, after the markers —
/// replays to exactly one history row, the markers stamped, one event, the
/// salvage obligation present and disposal not yet run.
#[tokio::test]
async fn terminal_crash_after_each_step_replays_once() {
    for (index, step) in [
        TerminalStep::Recorded,
        TerminalStep::HistoryWritten {
            seq: 0,
            incarnation: String::new(),
        },
        TerminalStep::MarkersStamped,
    ]
    .into_iter()
    .enumerate()
    {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, history) = spawn(tmp.path(), Tuning::default()).await;
        let (job_id, source) = admitted_job(
            &engine,
            &format!("crash-{index}"),
            &[("episode.mkv", Some(b"episode")), ("other.mkv", None)],
            JobStatus::Failed,
        )
        .await;
        let mut events = engine.subscribe();
        // The injection matches on the step kind.
        let injected = match &step {
            TerminalStep::HistoryWritten { .. } => {
                // The real step carries the seq it wrote; inject by kind.
                TerminalStep::HistoryWritten {
                    seq: 0,
                    incarnation: String::new(),
                }
            }
            other => other.clone(),
        };
        engine.artifacts().inject_terminal_crash(job_id.0, injected);
        assert!(
            !engine
                .fail_job(
                    job_id,
                    FailureReason::Health,
                    None,
                    "health",
                    "below critical"
                )
                .await
                .unwrap(),
            "{step:?}: the chain stopped"
        );
        let partial = engine.artifacts().terminal(job_id.0).unwrap().unwrap();
        assert!(!partial.emitted, "{step:?}");
        assert_eq!(partial.salvage, SalvageState::Pending, "{step:?}");
        // "Restart": the replay boot (and the tick) perform.
        engine.replay_terminals().await.unwrap();
        let terminal = wait_terminal(&engine, job_id, "replay", |t| t.emitted).await;
        assert!(terminal.markers_stamped);
        assert_eq!(
            terminal.salvage,
            SalvageState::Pending,
            "obligation present"
        );
        assert!(terminal.disposal.is_none(), "disposal not run");
        assert!(source.exists());
        let rows = history
            .list(10)
            .unwrap()
            .into_iter()
            .filter(|row| row.job == job_id)
            .count();
        assert_eq!(rows, 1, "{step:?}: exactly one history row");
        let row = export(&engine, job_id).await.unwrap();
        assert!(row
            .params
            .iter()
            .any(|(key, value)| key == nzbd_types::PP_DONE_PARAM && value == "FAILURE/HEALTH"));
        assert!(row
            .params
            .iter()
            .any(|(key, value)| key == "*PP:history-seq"
                && value == &terminal.history_seq.unwrap().to_string()));
        let mut emitted = 0;
        while let Ok(event) = events.try_recv() {
            if matches!(event, Event::JobPpFinished { job, .. } if job == job_id) {
                emitted += 1;
            }
        }
        assert_eq!(emitted, 1, "{step:?}: one event");
        // A second replay is a no-op.
        engine.replay_terminals().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            events.try_recv().is_err() || {
                let mut again = 0;
                while let Ok(event) = events.try_recv() {
                    if matches!(event, Event::JobPpFinished { job, .. } if job == job_id) {
                        again += 1;
                    }
                }
                again == 0
            }
        );
        engine.shutdown().await;
    }
}
