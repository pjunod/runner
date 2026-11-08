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
    // The job now has both a source and Curator's publication; the
    // settled obligation is on the source, by role (review of #282).
    assert_ne!(delivery.source_artifact_id, delivery.artifact_id);
    let source_row = engine
        .artifacts()
        .get(&delivery.source_artifact_id)
        .unwrap();
    let publication_row = engine.artifacts().get(&delivery.artifact_id).unwrap();
    assert_eq!(source_row.salvage.as_deref(), Some("published"));
    assert_eq!(publication_row.salvage, None);
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

/// Contract §2.6 / plan M1a: a `pp_done` `PAR_FAILURE` row whose terminal
/// owes salvage stays in the queue — salvage and disposal read it — until
/// `job_publication_ready` has gone out and its source is disposed; only
/// then does the manager's sweep retire it.
/// A salvage publication that crashed mid-move leaves an open relocation
/// intent (contract §2.5): neither disposal nor the sweep may claim either
/// side until reconciliation finishes it, which the next salvage attempt does.
#[tokio::test]
async fn retire_sweep_waits_for_salvage_and_open_intents() {
    let tmp = tempfile::tempdir().unwrap();
    let (engine, history) = spawn(tmp.path(), Tuning::default()).await;
    let (job_id, _) = admitted_job(
        &engine,
        "par-pack",
        &[
            ("Show.S01E01.mkv", Some(b"S01E01 bytes")),
            ("Show.S01E02.mkv", None),
        ],
        JobStatus::Failed,
    )
    .await;
    assert!(engine
        .fail_job(
            job_id,
            FailureReason::Par,
            None,
            "par_repair",
            "not enough recovery blocks"
        )
        .await
        .unwrap());
    let terminal = engine.artifacts().terminal(job_id.0).unwrap().unwrap();
    assert_eq!(terminal.salvage, SalvageState::Pending);
    assert!(
        engine
            .artifacts()
            .disposal_allowed(job_id.0)
            .unwrap()
            .is_none(),
        "an open salvage obligation blocks disposal"
    );
    let row = export(&engine, job_id).await.unwrap();
    assert!(row
        .params
        .iter()
        .any(|(key, value)| key == nzbd_types::PP_DONE_PARAM && value == "PAR_FAILURE"));
    // The salvage move of the verified episode stops after its first rename,
    // as a crash there would leave it.
    let source = engine.artifacts().for_job(job_id.0).unwrap().unwrap();
    let selected = source
        .files
        .iter()
        .filter(|entry| entry.path == "Show.S01E01.mkv")
        .cloned()
        .collect::<Vec<_>>();
    let outcomes = source
        .files
        .iter()
        .filter(|entry| !entry.identity.directory)
        .map(|entry| nzbd_state::artifacts::DeliveryFileOutcome {
            source_path: entry.path.clone(),
            publication_path: (entry.path == "Show.S01E01.mkv").then(|| entry.path.clone()),
            classification: "video".into(),
            outcome: if entry.path == "Show.S01E01.mkv" {
                "published"
            } else {
                "excluded_source_cleanup"
            }
            .into(),
            reason: None,
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        })
        .collect();
    std::fs::create_dir_all(tmp.path().join("completed")).unwrap();
    let crash = engine
        .artifacts()
        .inject_relocation_test_fault(job_id.0, "after_first_rename", libc::EIO)
        .unwrap();
    assert!(engine
        .artifacts()
        .publish_selected_with_transfer(
            job_id.0,
            &tmp.path().join("completed").join(format!(
                "par-pack.job-{}.runner-{}",
                job_id.0, source.generation
            )),
            selected,
            outcomes,
            Some("t-par-pack"),
            true,
            &|| Ok(()),
        )
        .is_err());
    drop(crash);
    assert!(engine.artifacts().open_intent_for_job(job_id.0).unwrap());
    assert!(
        engine
            .artifacts()
            .disposal_allowed(job_id.0)
            .unwrap()
            .is_none(),
        "an open relocation intent blocks disposal"
    );
    let mut events = engine.subscribe();
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        PostConfig {
            completed_dir: Some(tmp.path().join("completed")),
            failed_dir: Some(tmp.path().join("parked")),
            failure_action: FailureAction::Delete,
            ..PostConfig::default()
        },
        history,
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let Ok(event) = tokio::time::timeout_at(deadline, events.recv()).await else {
            panic!(
                "job_publication_ready: terminal {:?}, intents {:?}",
                engine.artifacts().terminal(job_id.0).unwrap().map(|t| (
                    t.salvage,
                    t.salvage_error,
                    t.disposal
                )),
                engine.artifacts().open_relocation_intents().unwrap()
            );
        };
        if let Event::JobPublicationReady { job, .. } = event.unwrap() {
            assert_eq!(job, job_id);
            break;
        }
    }
    assert!(
        engine.snapshot().jobs.iter().any(|job| job.id == job_id),
        "the row outlives the salvage it owes"
    );
    let terminal = wait_terminal(&engine, job_id, "disposal", |t| t.disposal.is_some()).await;
    assert_eq!(terminal.salvage, SalvageState::Published);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while engine.snapshot().jobs.iter().any(|job| job.id == job_id) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "retired once settled"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
}

/// Plan M1a / IMPL §5.6 (review #280-2): the `completed` terminal is
/// recorded before the delivery row exists and before the source is
/// retired. A crash right after the queue stamp leaves no delivery; the
/// replayed event carries the publication, and the manager's sweep then
/// creates exactly that delivery from the stamped markers.
#[tokio::test]
async fn completed_terminal_precedes_delivery_and_crash_repairs_it() {
    let tmp = tempfile::tempdir().unwrap();
    let (engine, history) = spawn(tmp.path(), Tuning::default()).await;
    let (job_id, _) = admitted_job(
        &engine,
        "ordered",
        &[(
            "Movie.2020.mkv",
            Some(b"\x1a\x45\xdf\xa3 matroska".as_slice()),
        )],
        JobStatus::Completed,
    )
    .await;
    let cfg = PostConfig {
        completed_dir: Some(tmp.path().join("completed")),
        deobfuscate_final: false,
        ..PostConfig::default()
    };
    engine
        .artifacts()
        .inject_terminal_crash(job_id.0, TerminalStep::MarkersStamped);
    let mut events = engine.subscribe();
    let cancel = CancellationToken::new();
    let admission = engine
        .wait_post_admission(job_id, &cancel)
        .await
        .unwrap()
        .expect("eligible for processing");
    let scoped = engine.for_post_admission(&admission);
    let outcome =
        nzbd_post::manager::process_job(&scoped, &cfg, &history, &tmp.path().join("dest"), job_id)
            .await;
    engine.end_post_admission(admission).await.unwrap();
    assert!(outcome.is_err(), "the chain stopped: {outcome:?}");
    assert!(
        engine
            .artifacts()
            .delivery_for_job(job_id.0)
            .unwrap()
            .is_none(),
        "no delivery before the terminal is out"
    );
    let partial = engine.artifacts().terminal(job_id.0).unwrap().unwrap();
    assert_eq!(partial.outcome, TerminalOutcome::Completed);
    assert!(partial.markers_stamped && !partial.emitted);
    engine.replay_terminals().await.unwrap();
    let publication = loop {
        let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("replayed terminal")
            .unwrap();
        if let Event::JobPpFinished {
            job,
            outcome,
            publication,
            ..
        } = event
        {
            assert_eq!(job, job_id);
            assert_eq!(outcome, TerminalOutcome::Completed);
            break publication.expect("completed carries its publication");
        }
    };
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        cfg,
        history,
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let delivery = loop {
        if let Some(delivery) = engine.artifacts().delivery_for_job(job_id.0).unwrap() {
            break delivery;
        }
        assert!(tokio::time::Instant::now() < deadline, "delivery repaired");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(delivery.manifest_digest, publication.manifest_digest);
    assert_eq!(delivery.publication_generation, publication.generation);
    assert_eq!(delivery.publication_path, publication.path);
    assert_eq!(delivery.kind(), "completed");
    while engine.snapshot().jobs.iter().any(|job| job.id == job_id) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "retired once delivered"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
}

/// Review #280-3 / contract §2.2: post-processing evidence that disagrees
/// with itself (an SFV naming one archive with two checksums) ends the job
/// `failed/identity` — blamed on the release, nothing salvageable, parked.
#[tokio::test]
async fn self_contradicting_evidence_is_identity() {
    let tmp = tempfile::tempdir().unwrap();
    let (engine, history) = spawn(tmp.path(), Tuning::default()).await;
    let (job_id, _) = admitted_job(
        &engine,
        "contradicting",
        &[
            (
                "a8f3c91e",
                Some(b"Rar!\x1a\x07\x00 obfuscated volume".as_slice()),
            ),
            (
                "release.sfv",
                Some(b"release.rar 11111111\nrelease.rar 22222222\n".as_slice()),
            ),
        ],
        JobStatus::Completed,
    )
    .await;
    let mut events = engine.subscribe();
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        PostConfig {
            failure_action: FailureAction::Delete,
            failed_dir: Some(tmp.path().join("parked")),
            ..PostConfig::default()
        },
        history.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let event = tokio::time::timeout_at(deadline, events.recv())
            .await
            .expect("identity terminal")
            .unwrap();
        if let Event::JobPpFinished {
            job,
            outcome,
            reason,
            blames_release,
            salvage,
            pp_status,
            ..
        } = event
        {
            assert_eq!(job, job_id);
            assert_eq!(outcome, TerminalOutcome::Failed);
            assert_eq!(reason, Some(FailureReason::Identity));
            assert!(blames_release);
            assert_eq!(salvage, nzbd_types::Salvage::None);
            assert_eq!(pp_status, "FAILURE/IDENTITY");
            break;
        }
    }
    let terminal = wait_terminal(&engine, job_id, "disposal", |t| t.disposal.is_some()).await;
    assert_eq!(terminal.salvage, SalvageState::None);
    assert!(
        engine
            .artifacts()
            .delivery_for_job(job_id.0)
            .unwrap()
            .is_none(),
        "nothing is salvaged from contradicting evidence"
    );
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
}

/// Review #280-4 / contract §2.1: a terminal whose `job_terminals` insert
/// fails is kept by the owner and retried, and the job cannot complete or
/// enter post-processing meanwhile — even after its storage hold releases.
#[tokio::test]
async fn unrecorded_terminal_is_retried_and_blocks_completion() {
    let tmp = tempfile::tempdir().unwrap();
    let (engine, history) = spawn(tmp.path(), Tuning::default()).await;
    let (job_id, _) = admitted_job(
        &engine,
        "unrecorded",
        &[
            ("episode.mkv", Some(b"episode".as_slice())),
            ("other.mkv", None),
        ],
        JobStatus::Failed,
    )
    .await;
    let mut events = engine.subscribe();
    engine.artifacts().inject_terminal_insert_failure(job_id.0);
    assert!(
        !engine
            .fail_job(job_id, FailureReason::Io, None, "download", "write refused")
            .await
            .unwrap(),
        "the insert failed"
    );
    assert!(engine.artifacts().terminal(job_id.0).unwrap().is_none());
    let control = export(&engine, job_id).await.unwrap().control().unwrap();
    assert_eq!(control.lifecycle, "held");
    assert_eq!(control.cause, "storage_unavailable");
    assert_eq!(control.stage, "persistence");
    // Storage answers again and the hold is released: the job still may
    // not be admitted to post-processing (it would re-complete on health).
    engine
        .release_hold(
            job_id,
            control.revision.clone(),
            nzbd_engine::RetryBy::Operator,
        )
        .await
        .unwrap()
        .expect("hold released");
    assert!(
        engine.begin_post_admission(job_id).await.unwrap().is_none(),
        "a deferred terminal fences post-processing"
    );
    // A second producer does not replace the waiting terminal.
    assert!(!engine
        .fail_job(job_id, FailureReason::Health, None, "health", "late")
        .await
        .unwrap());
    engine.replay_terminals().await.unwrap();
    let terminal = wait_terminal(&engine, job_id, "retried terminal", |t| t.emitted).await;
    assert_eq!(terminal.reason, Some(FailureReason::Io));
    assert_eq!(terminal.message.as_deref(), Some("write refused"));
    assert_eq!(
        history
            .get_sequence(terminal.history_seq.unwrap())
            .unwrap()
            .unwrap()
            .status,
        "FAILURE/IO"
    );
    let mut finished = 0;
    while let Ok(event) = events.try_recv() {
        if let Event::JobPpFinished { job, reason, .. } = event {
            assert_eq!(job, job_id);
            assert_eq!(reason, Some(FailureReason::Io));
            finished += 1;
        }
    }
    assert_eq!(finished, 1);
    engine.shutdown().await;
}

/// Plan M1a (e), contract §2.1: a torrent that stopped with a partially
/// verified payload is `failed/io` with `salvage: pending` — no blame, the
/// verified files owed to salvage. (The acquisition's completion command
/// records exactly this terminal when its donor seed failed; building a real
/// piece-verified torrent copy is out of reach for this fixture, so the job
/// row is the acquisition's post-copy shape.)
#[tokio::test]
async fn partial_torrent_is_io_with_salvage_pending() {
    let tmp = tempfile::tempdir().unwrap();
    let (engine, history) = spawn(tmp.path(), Tuning::default()).await;
    let (job_id, _) = admitted_job(
        &engine,
        "partial-torrent",
        &[("Show.S01E01.mkv", Some(b"verified piece".as_slice()))],
        JobStatus::Failed,
    )
    .await;
    let mut row = export(&engine, job_id).await.unwrap();
    row.kind = nzbd_types::JobKind::Torrent;
    for file in &mut row.files {
        file.segments.clear(); // torrent files carry no articles
    }
    engine.import_job(row, false, false).await.unwrap();
    assert!(engine
        .fail_job(
            job_id,
            FailureReason::Io,
            None,
            "torrent_download",
            "torrent stopped with a partially verified payload; hash-verified files are salvaged",
        )
        .await
        .unwrap());
    let terminal = engine.artifacts().terminal(job_id.0).unwrap().unwrap();
    assert_eq!(terminal.reason, Some(FailureReason::Io));
    assert!(!terminal.blames_release);
    assert_eq!(terminal.salvage, SalvageState::Pending);
    assert_eq!(
        terminal.fetched.success_articles, 0,
        "no articles: salvage is owed by kind"
    );
    assert_eq!(
        history
            .get_sequence(terminal.history_seq.unwrap())
            .unwrap()
            .unwrap()
            .status,
        "FAILURE/IO"
    );
    engine.shutdown().await;
}

/// v2 of the deleted `a_failed_attempt_that_left_no_files_retains_nothing`:
/// a failed attempt that never wrote a byte is assessed `unusable` after
/// salvage finds nothing, and its empty source is disposed by policy —
/// nothing is parked or retained.
#[tokio::test]
async fn a_failed_attempt_that_left_no_files_retains_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let (engine, _history) = spawn(tmp.path(), Tuning::default()).await;
    let (job_id, source) = admitted_job(
        &engine,
        "nothing-written",
        &[("episode.mkv", None)],
        JobStatus::Failed,
    )
    .await;
    assert!(engine
        .fail_job(
            job_id,
            FailureReason::Health,
            None,
            "health",
            "no article arrived"
        )
        .await
        .unwrap());
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
    assert!(terminal.disposal.is_some(), "{terminal:?}");
    assert!(
        !parked.join("nothing-written").exists(),
        "nothing is parked"
    );
    assert!(
        !source.exists() || std::fs::read_dir(&source).unwrap().next().is_none(),
        "nothing is retained"
    );
    engine.shutdown().await;
}

/// v2 of the deleted `absent_integrity_proof_is_unknown_not_cleanup_authority`:
/// payload bytes whose integrity was never proven are not deletion
/// authority — with `failure_action = delete` the source is parked, not
/// deleted (the pre-v2 "unknown → retain" rule, now after the terminal).
#[tokio::test]
async fn absent_integrity_proof_is_unknown_not_cleanup_authority() {
    let tmp = tempfile::tempdir().unwrap();
    let (engine, _history) = spawn(tmp.path(), Tuning::default()).await;
    let (job_id, source) = admitted_job(
        &engine,
        "unproven",
        &[(
            "Movie.2020.mkv",
            Some(b"\x1a\x45\xdf\xa3 unproven matroska".as_slice()),
        )],
        JobStatus::Failed,
    )
    .await;
    // Drop the proof: the bytes arrived, but no file CRC vouches for them.
    let mut row = export(&engine, job_id).await.unwrap();
    for file in &mut row.files {
        file.crc32 = None;
    }
    engine.import_job(row, false, false).await.unwrap();
    assert!(engine
        .fail_job(
            job_id,
            FailureReason::Health,
            None,
            "health",
            "below critical"
        )
        .await
        .unwrap());
    let parked = tmp.path().join("parked");
    let cfg = PostConfig {
        failure_action: FailureAction::Delete,
        failed_dir: Some(parked.clone()),
        ..PostConfig::default()
    };
    // As the manager's rescans do: follow up until the owner has settled the
    // re-imported row (its startup verification owns it first).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !nzbd_post::manager::follow_up_failure(&engine, &cfg, &tmp.path().join("dest"), job_id)
        .await
    {
        assert!(tokio::time::Instant::now() < deadline, "follow-up settles");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let terminal = engine.artifacts().terminal(job_id.0).unwrap().unwrap();
    let disposal = terminal.disposal.clone().unwrap();
    assert!(disposal.starts_with("parked at"), "{disposal}");
    let kept = parked.join("unproven").join("Movie.2020.mkv");
    assert!(
        kept.exists() || source.join("Movie.2020.mkv").exists(),
        "unproven payload is retained: {disposal}"
    );
    engine.shutdown().await;
}

/// v2 of the deleted `failed_park_losing_admission_after_move_is_fenced_and_retried`:
/// the park runs under the post-processing admission; when the admission is
/// lost right after the move, the outcome is not recorded by the holder that
/// lost it — the next follow-up finds the parked copy and records it once,
/// without moving it again.
#[tokio::test]
async fn failed_park_losing_admission_after_move_is_fenced_and_retried() {
    let tmp = tempfile::tempdir().unwrap();
    let (engine, _history) = spawn(tmp.path(), Tuning::default()).await;
    let (job_id, source) = admitted_job(
        &engine,
        "fenced-park",
        &[(
            "Movie.2020.mkv",
            Some(b"\x1a\x45\xdf\xa3 unproven matroska".as_slice()),
        )],
        JobStatus::Failed,
    )
    .await;
    let mut row = export(&engine, job_id).await.unwrap();
    for file in &mut row.files {
        file.crc32 = None; // unproven: the source is parked, never deleted
    }
    engine.import_job(row, false, false).await.unwrap();
    assert!(engine
        .fail_job(
            job_id,
            FailureReason::Health,
            None,
            "health",
            "below critical"
        )
        .await
        .unwrap());
    let parked = tmp.path().join("parked");
    let cfg = PostConfig {
        failure_action: FailureAction::Delete,
        failed_dir: Some(parked.clone()),
        ..PostConfig::default()
    };
    let dest = tmp.path().join("dest");
    engine.artifacts().inject_disposal_admission_loss(job_id.0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    // Until the owner settles the re-imported row, follow-up is refused
    // before it moves anything; the injected loss fires on the first move.
    while engine
        .artifacts()
        .terminal(job_id.0)
        .unwrap()
        .unwrap()
        .disposal
        .is_none()
        && !parked.join("fenced-park").exists()
    {
        assert!(!nzbd_post::manager::follow_up_failure(&engine, &cfg, &dest, job_id).await);
        assert!(tokio::time::Instant::now() < deadline, "the park runs");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let moved = parked.join("fenced-park").join("Movie.2020.mkv");
    assert!(moved.exists(), "the move itself happened");
    assert!(!source.join("Movie.2020.mkv").exists());
    assert!(
        engine
            .artifacts()
            .terminal(job_id.0)
            .unwrap()
            .unwrap()
            .disposal
            .is_none(),
        "a holder that lost its admission records nothing"
    );
    assert!(nzbd_post::manager::follow_up_failure(&engine, &cfg, &dest, job_id).await);
    let disposal = engine
        .artifacts()
        .terminal(job_id.0)
        .unwrap()
        .unwrap()
        .disposal
        .unwrap();
    assert!(disposal.starts_with("parked at"), "{disposal}");
    assert!(
        moved.exists(),
        "recorded where it already is — not moved twice"
    );
    let parked_entries = std::fs::read_dir(&parked).unwrap().count();
    assert_eq!(parked_entries, 1, "one parked copy");
    engine.shutdown().await;
}

/// v2 of the deleted `retry_now_releases_a_due_failed_source_and_runs_manager_salvage`:
/// a failed row whose disposal is waiting on its operation's durable
/// `next_retry` stays put on the manager's own rescans, and an operator
/// Retry now runs the follow-up immediately, past that deadline — salvage
/// settled, then the park replays and is recorded once.
#[tokio::test]
async fn retry_now_runs_a_failed_source_follow_up_past_its_disposal_ladder() {
    let tmp = tempfile::tempdir().unwrap();
    let (engine, history) = spawn(tmp.path(), Tuning::default()).await;
    let (job_id, source) = admitted_job(
        &engine,
        "retry-now",
        &[(
            "Movie.2020.mkv",
            Some(b"\x1a\x45\xdf\xa3 unproven matroska".as_slice()),
        )],
        JobStatus::Failed,
    )
    .await;
    let mut row = export(&engine, job_id).await.unwrap();
    for file in &mut row.files {
        file.crc32 = None; // unproven: nothing to salvage, the source is parked
    }
    engine.import_job(row, false, false).await.unwrap();
    assert!(engine
        .fail_job(
            job_id,
            FailureReason::Health,
            None,
            "health",
            "below critical"
        )
        .await
        .unwrap());
    // The park's relocate operation hits an I/O error (EIO) before it
    // commits, so it goes onto its own durable retry ladder.
    let parked = tmp.path().join("parked");
    let fault = engine
        .artifacts()
        .inject_relocation_test_fault(job_id.0, "before_commit", 5)
        .unwrap();
    let cfg = PostConfig {
        failure_action: FailureAction::Delete,
        failed_dir: Some(parked.clone()),
        ..PostConfig::default()
    };
    let dest = tmp.path().join("dest");
    let now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let retry_at = loop {
        assert!(!nzbd_post::manager::follow_up_failure(&engine, &cfg, &dest, job_id).await);
        if let Some(at) = engine.artifacts().disposal_retry_at(job_id.0).unwrap() {
            break at;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the park fails onto its ladder: {:?}",
            engine.artifacts().terminal(job_id.0).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(
        retry_at > now() + 5,
        "the ladder is in the future: {retry_at}"
    );
    let terminal = engine.artifacts().terminal(job_id.0).unwrap().unwrap();
    assert_ne!(terminal.salvage, SalvageState::Pending, "salvage ran first");
    assert!(terminal.disposal.is_none(), "{terminal:?}");

    // The storage comes back. The manager's startup rescan honours the
    // ladder: nothing is recorded before `next_retry`.
    drop(fault);
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    let manager = spawn_post_manager(
        engine.clone(),
        cfg.clone(),
        history,
        dest.clone(),
        None,
        cancel.clone(),
        &tracker,
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        engine
            .artifacts()
            .terminal(job_id.0)
            .unwrap()
            .unwrap()
            .disposal
            .is_none(),
        "a rescan waits for the ladder"
    );
    assert_eq!(
        engine.artifacts().disposal_retry_at(job_id.0).unwrap(),
        Some(retry_at),
        "the ladder is untouched by the rescan"
    );

    // Retry now runs the owed follow-up immediately.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match manager.retry_now(job_id).await {
            Ok(()) => break,
            // The owner may still be settling the re-imported row.
            Err(nzbd_post::manager::RestartPostError::NotReady)
                if tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(50)).await
            }
            Err(error) => panic!("retry now: {error}"),
        }
    }
    assert!(now() < retry_at, "ran ahead of the ladder");
    let terminal = engine.artifacts().terminal(job_id.0).unwrap().unwrap();
    let disposal = terminal.disposal.clone().unwrap();
    assert!(disposal.starts_with("parked at"), "{disposal}");
    assert!(parked.join("retry-now").join("Movie.2020.mkv").exists());
    assert!(!source.join("Movie.2020.mkv").exists());
    assert_eq!(
        std::fs::read_dir(&parked).unwrap().count(),
        1,
        "one parked copy"
    );
    assert_eq!(
        engine.artifacts().disposal_retry_at(job_id.0).unwrap(),
        None,
        "the ladder's operation settled"
    );
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
}
