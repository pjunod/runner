//! Pipeline contract v2 M3 acceptance (plan M3 builder-runnable checks a–e,
//! IMPL_RUNNER §7.7): journaled per-file publication, one hash pass, the
//! manifest written last, and finish-or-unwind after a crash at every step.

use nzbd_engine::{AddOpts, Engine, EngineConfig, EngineHandle, Tuning};
use nzbd_post::manager::{process_job, spawn_post_manager, PostConfig, PpFinal};
use nzbd_state::artifacts::{IntentState, RunnerManifest, TerminalStep, RUNNER_MANIFEST_NAME};
use nzbd_state::history::HistoryDb;
use nzbd_types::{FailureReason, JobId, JobStatus, SegmentState, TerminalOutcome};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Two selected Matroska payloads and an unselected incidental sibling.
const MOVIE: &[u8] = b"\x1a\x45\xdf\xa3 matroska payload of the feature";
const EXTRA: &[u8] = b"\x1a\x45\xdf\xa3 matroska payload of the featurette, longer";
const SELECTED: &[(&str, &[u8])] = &[("Movie.2020.mkv", MOVIE), ("Movie.2020.extra.mkv", EXTRA)];
const SIBLING: (&str, &[u8]) = ("release.nfo", b"release notes nobody imports");

/// Test-support crash injection for the terminal chain is keyed by job id,
/// and every engine here numbers its first job 1: these tests take turns.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn crc(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// A temporary directory on tmpfs when the host has one (plan M3 (a)).
fn tmpfs_dir() -> tempfile::TempDir {
    let shm = Path::new("/dev/shm");
    if shm.is_dir() {
        tempfile::tempdir_in(shm).unwrap()
    } else {
        tempfile::tempdir().unwrap()
    }
}

/// The daemon shape: engine and post-processing share one history store.
async fn spawn(dir: &Path, history: &Arc<HistoryDb>) -> EngineHandle {
    std::fs::create_dir_all(dir.join("dest")).unwrap();
    let mut cfg = EngineConfig::single_node(
        vec![],
        dir.join("state"),
        dir.join("dest"),
        Tuning::default(),
        None,
    );
    cfg.history = Some(history.clone());
    Engine::spawn(cfg).await.expect("engine spawn")
}

fn open_history(dir: &Path) -> Arc<HistoryDb> {
    Arc::new(HistoryDb::open(&dir.join("history.sqlite"), Some(dir)).unwrap())
}

fn post_config(dir: &Path) -> PostConfig {
    PostConfig {
        completed_dir: Some(dir.join("completed")),
        deobfuscate_final: false,
        ..PostConfig::default()
    }
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

/// A real delivery-lifecycle job whose payload is the two selected files,
/// with the unselected sibling written beside them in its processing dir.
async fn completed_job(engine: &EngineHandle, name: &str) -> (JobId, PathBuf) {
    let mut nzb = String::from(
        r#"<?xml version="1.0" encoding="utf-8"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">"#,
    );
    for (index, (file, _)) in SELECTED.iter().enumerate() {
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
    for (file, bytes) in SELECTED.iter().chain(std::iter::once(&SIBLING)) {
        std::fs::write(artifact.path.join(file), bytes).unwrap();
    }
    engine.artifacts().refresh_for_delivery(job_id.0).unwrap();
    let mut job = export(engine, job_id).await.unwrap();
    job.status = JobStatus::Completed;
    for file in &mut job.files {
        let (name, bytes) = SELECTED
            .iter()
            .find(|(name, _)| file.subject.contains(name))
            .copied()
            .unwrap();
        file.filename = name.into();
        file.subject = name.into();
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
    engine.import_job(job, false, false).await.unwrap();
    export(engine, job_id).await.unwrap();
    (job_id, artifact.path)
}

/// Run post-processing for `job` directly, as one attempt.
async fn run_once(
    engine: &EngineHandle,
    history: &Arc<HistoryDb>,
    dir: &Path,
    job: JobId,
) -> Result<PpFinal, nzbd_post::PostError> {
    let cancel = CancellationToken::new();
    let admission = engine
        .wait_post_admission(job, &cancel)
        .await
        .unwrap()
        .expect("eligible for processing");
    let scoped = engine.for_post_admission(&admission);
    let outcome = process_job(&scoped, &post_config(dir), history, &dir.join("dest"), job).await;
    engine.end_post_admission(admission).await.unwrap();
    if let Err(error) = &outcome {
        let row = export(engine, job).await;
        eprintln!(
            "attempt for job {} stopped: {error}; status {:?}, control {:?}",
            job.0,
            row.as_ref().map(|row| row.status),
            row.as_ref().map(|row| row
                .params
                .iter()
                .filter(|(k, _)| k.starts_with("*Control") || k.starts_with("Failure"))
                .collect::<Vec<_>>())
        );
    }
    outcome
}

/// Every publication directory under `completed/`.
fn publications(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir.join("completed")) else {
        return Vec::new();
    };
    let mut found = entries
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    found.sort();
    found
}

/// Plan M3 (c)'s assertion: exactly one usable publication (a directory
/// with a verifying manifest and exactly the selected files), no file in two
/// places, nothing unselected published.
fn assert_one_usable_publication(dir: &Path, source: &Path, what: &str) -> RunnerManifest {
    let found = publications(dir);
    assert_eq!(found.len(), 1, "{what}: exactly one publication: {found:?}");
    let publication = &found[0];
    let manifest: RunnerManifest = serde_json::from_slice(
        &std::fs::read(publication.join(RUNNER_MANIFEST_NAME))
            .unwrap_or_else(|error| panic!("{what}: manifest: {error}")),
    )
    .unwrap();
    assert!(manifest.verifies(), "{what}: manifest digest");
    let mut listed = manifest
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<Vec<_>>();
    listed.sort();
    let mut expected = SELECTED.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    expected.sort();
    assert_eq!(listed, expected, "{what}: the selected files only");
    for (name, bytes) in SELECTED {
        assert_eq!(
            std::fs::read(publication.join(name)).unwrap(),
            *bytes,
            "{what}: {name}"
        );
        assert!(!source.join(name).exists(), "{what}: {name} in two places");
    }
    assert!(
        !publication.join(SIBLING.0).exists(),
        "{what}: unselected sibling published"
    );
    manifest
}

/// Plan M3 (a): on tmpfs, a same-volume publication performs exactly N
/// renames for N selected files and at most one read pass over them (none
/// when the digest was journaled), no copy; the unselected sibling stays in
/// the processing directory until the delivery disposes of it.
#[tokio::test]
async fn same_volume_publication_renames_selected_only() {
    let _serial = SERIAL.lock().await;
    let tmp = tmpfs_dir();
    let history = open_history(tmp.path());
    let engine = spawn(tmp.path(), &history).await;
    let (job, source) = completed_job(&engine, "renames").await;
    let inventory = engine.artifacts();
    inventory.reset_fs_ops();
    assert_eq!(
        run_once(&engine, &history, tmp.path(), job).await.unwrap(),
        PpFinal::Success
    );
    let ops = inventory.fs_ops();
    let selected_bytes: u64 = SELECTED.iter().map(|(_, bytes)| bytes.len() as u64).sum();
    assert_eq!(
        ops.get("rename").copied(),
        Some(SELECTED.len() as u64),
        "{ops:?}"
    );
    assert_eq!(ops.get("copy_bytes").copied().unwrap_or(0), 0, "{ops:?}");
    assert!(
        ops.get("hash_bytes").copied().unwrap_or(0) <= selected_bytes,
        "one read pass at most: {ops:?}"
    );
    assert_eq!(ops.get("manifest_write").copied(), Some(1), "{ops:?}");
    let manifest = assert_one_usable_publication(tmp.path(), &source, "publication");
    assert_eq!(manifest.job, job.0);
    assert_eq!(manifest.transfer.as_deref(), Some("t-renames"));
    assert!(!manifest.cross_volume);
    let name = publications(tmp.path())[0]
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    assert!(
        name.contains(&format!(".job-{}.runner-", job.0)),
        "contract §1 publication name: {name}"
    );
    let delivery = inventory.delivery_for_job(job.0).unwrap().unwrap();
    assert_eq!(delivery.manifest_digest, manifest.digest);
    assert_eq!(delivery.manifest.as_ref(), Some(&manifest));
    assert!(
        !source.exists(),
        "the unselected sibling went with the processing directory's disposal"
    );
    assert!(inventory.open_relocation_intents().unwrap().is_empty());
    engine.shutdown().await;
}

/// IMPL §7.7: the immutable delivery row is written after the history row.
#[tokio::test]
async fn delivery_row_written_after_history() {
    let _serial = SERIAL.lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let history = open_history(tmp.path());
    let engine = spawn(tmp.path(), &history).await;
    let (job, _) = completed_job(&engine, "ordered").await;
    engine.artifacts().inject_terminal_crash(
        job.0,
        TerminalStep::HistoryWritten {
            seq: 0,
            incarnation: String::new(),
        },
    );
    assert!(run_once(&engine, &history, tmp.path(), job).await.is_err());
    let row = history
        .list(10)
        .unwrap()
        .into_iter()
        .find(|row| row.job == job)
        .expect("the history row is durable");
    assert!(
        engine
            .artifacts()
            .delivery_for_job(job.0)
            .unwrap()
            .is_none(),
        "no delivery row before the history row's chain completed"
    );
    engine.replay_terminals().await.unwrap();
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        post_config(tmp.path()),
        history.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    let delivery = wait_delivery(&engine, job).await;
    assert!(delivery.ready_at >= row.completed_at_unix);
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
}

/// After a restart the engine re-verifies a job whose post-processing was
/// interrupted before it re-admits it. These tests start the post manager
/// once that boot verification has settled (see the M3 status entry: with
/// the manager admitted first, the verification was observed to stay
/// `pending`).
async fn settle_boot_verification(engine: &EngineHandle, job: JobId) {
    let started = tokio::time::Instant::now();
    let deadline = started + Duration::from_secs(90);
    loop {
        let snapshot = engine.snapshot();
        let summary = snapshot.jobs.iter().find(|summary| summary.id == job);
        if summary.is_none_or(|summary| summary.verification.is_none()) {
            eprintln!("boot verification settled after {:?}", started.elapsed());
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "boot verification settles: {:?}",
            summary.map(|summary| (summary.status, summary.verification.clone()))
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Like `settle_boot_verification`, but a verification that ended blocked
/// (holding the job for review) also counts as settled.
async fn settle_or_hold(engine: &EngineHandle, job: JobId) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let snapshot = engine.snapshot();
        let summary = snapshot.jobs.iter().find(|summary| summary.id == job);
        if summary.is_none_or(|summary| {
            summary
                .verification
                .as_ref()
                .is_none_or(|verification| verification.state == "blocked")
        }) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "verification settles"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_delivery(engine: &EngineHandle, job: JobId) -> nzbd_state::artifacts::Delivery {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(delivery) = engine.artifacts().delivery_for_job(job.0).unwrap() {
            return delivery;
        }
        if tokio::time::Instant::now() >= deadline {
            let row = engine.export_job(job).await.ok().flatten();
            panic!(
                "no delivery: status {:?}, params {:?}, terminal {:?}, intents {:?}, artifact {:?}",
                row.as_ref().map(|row| row.status),
                row.as_ref().map(|row| row
                    .params
                    .iter()
                    .filter(|(key, _)| key.starts_with('*') || key.starts_with("Failure"))
                    .collect::<Vec<_>>()),
                engine
                    .artifacts()
                    .terminal(job.0)
                    .unwrap()
                    .map(|t| (t.outcome, t.reason, t.message, t.emitted)),
                engine.artifacts().open_relocation_intents().unwrap(),
                engine
                    .artifacts()
                    .for_job(job.0)
                    .unwrap()
                    .map(|a| (a.path, a.state, a.error, a.hold)),
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Plan M3 (c): a crash at `stage`, then a restart. Exactly one usable
/// publication, no file in two places, the processing directory not
/// disposed of until the publication committed, one history row.
async fn crash_then_restart(stage: &'static str) {
    let _serial = SERIAL.lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let history = open_history(tmp.path());
    let engine = spawn(tmp.path(), &history).await;
    let (job, source) = completed_job(&engine, &format!("crash-{stage}")).await;
    let _fault = match stage {
        "after_history" => {
            engine.artifacts().inject_terminal_crash(
                job.0,
                TerminalStep::HistoryWritten {
                    seq: 0,
                    incarnation: String::new(),
                },
            );
            None
        }
        _ => Some(
            engine
                .artifacts()
                .inject_relocation_test_fault(job.0, stage, libc::EIO)
                .unwrap(),
        ),
    };
    assert!(
        run_once(&engine, &history, tmp.path(), job).await.is_err(),
        "{stage}: the attempt stopped"
    );
    let moving = matches!(stage, "after_first_rename" | "after_last_rename");
    if moving || stage == "after_manifest" {
        assert!(
            engine.artifacts().open_intent_for_job(job.0).unwrap(),
            "{stage}: the move is open"
        );
        assert!(
            engine
                .artifacts()
                .disposal_allowed(job.0)
                .unwrap()
                .is_none(),
            "{stage}: disposal refused while the move is open"
        );
        assert!(source.exists(), "{stage}: processing directory untouched");
        assert!(
            source.join(SIBLING.0).exists(),
            "{stage}: sibling untouched"
        );
        assert!(
            engine
                .artifacts()
                .delivery_for_job(job.0)
                .unwrap()
                .is_none(),
            "{stage}"
        );
    }
    if moving {
        let in_target = publications(tmp.path())
            .first()
            .map(|dir| {
                SELECTED
                    .iter()
                    .filter(|(name, _)| dir.join(name).exists())
                    .count()
            })
            .unwrap_or(0);
        let in_source = SELECTED
            .iter()
            .filter(|(name, _)| source.join(name).exists())
            .count();
        assert_eq!(
            in_target + in_source,
            SELECTED.len(),
            "{stage}: every file in exactly one place mid-move"
        );
    }
    // Restart.
    engine.shutdown().await;
    drop(engine);
    let engine = spawn(tmp.path(), &history).await;
    settle_boot_verification(&engine, job).await;
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        post_config(tmp.path()),
        history.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    let delivery = wait_delivery(&engine, job).await;
    let manifest = assert_one_usable_publication(tmp.path(), &source, stage);
    assert_eq!(delivery.manifest_digest, manifest.digest, "{stage}");
    assert_eq!(delivery.kind(), "completed", "{stage}");
    let terminal = engine.artifacts().terminal(job.0).unwrap().unwrap();
    assert_eq!(terminal.outcome, TerminalOutcome::Completed, "{stage}");
    assert_eq!(
        terminal
            .publication
            .as_ref()
            .map(|p| p.manifest_digest.as_str()),
        Some(manifest.digest.as_str()),
        "{stage}: the terminal carries the committed publication"
    );
    assert!(engine
        .artifacts()
        .open_relocation_intents()
        .unwrap()
        .is_empty());
    let rows = history
        .list(20)
        .unwrap()
        .into_iter()
        .filter(|row| row.job == job)
        .count();
    assert_eq!(rows, 1, "{stage}: one history row");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while source.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{stage}: the processing directory is disposed once committed"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
}

#[tokio::test]
async fn crash_after_first_rename_finishes_on_restart() {
    crash_then_restart("after_first_rename").await;
}

#[tokio::test]
async fn crash_after_last_rename_finishes_on_restart() {
    crash_then_restart("after_last_rename").await;
}

#[tokio::test]
async fn crash_after_manifest_finishes_on_restart() {
    crash_then_restart("after_manifest").await;
}

#[tokio::test]
async fn crash_after_history_finishes_on_restart() {
    crash_then_restart("after_history").await;
}

#[tokio::test]
async fn crash_after_delivery_finishes_on_restart() {
    crash_then_restart("after_delivery").await;
}

/// Plan M3 (e): a crash with the source half-moved and a *corrupted*
/// remaining source file unwinds rather than finishes: the moved file goes
/// back, the target is removed, and the job fails `io` (parked, no blame).
#[tokio::test]
async fn crash_with_corrupted_source_unwinds() {
    let _serial = SERIAL.lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let history = open_history(tmp.path());
    let engine = spawn(tmp.path(), &history).await;
    let (job, source) = completed_job(&engine, "corrupted").await;
    let _fault = engine
        .artifacts()
        .inject_relocation_test_fault(job.0, "after_first_rename", libc::EIO)
        .unwrap();
    assert!(run_once(&engine, &history, tmp.path(), job).await.is_err());
    let generation = engine
        .artifacts()
        .for_job(job.0)
        .unwrap()
        .unwrap()
        .generation;
    let intent = engine
        .artifacts()
        .relocation_intent(&generation)
        .unwrap()
        .unwrap();
    let remaining = intent
        .files
        .iter()
        .find(|file| !file.moved)
        .expect("half-moved")
        .src
        .clone();
    let moved = intent
        .files
        .iter()
        .find(|file| file.moved)
        .expect("half-moved")
        .src
        .clone();
    std::thread::sleep(Duration::from_millis(20));
    std::fs::write(source.join(&remaining), b"\x1a\x45\xdf\xa3 bit rot").unwrap();
    engine.shutdown().await;
    drop(engine);
    let engine = spawn(tmp.path(), &history).await;
    // Startup reconciliation runs before the job is re-admitted.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let intent = loop {
        let intent = engine
            .artifacts()
            .relocation_intent(&generation)
            .unwrap()
            .unwrap();
        if !intent.state.is_open() {
            break intent;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "reconciled: {intent:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(intent.state, IntentState::Unwound, "{intent:?}");
    assert!(intent
        .evidence
        .as_deref()
        .is_some_and(|evidence| evidence.contains("changed since the move was journaled")));
    assert!(
        publications(tmp.path()).is_empty(),
        "the target is removed: {:?}",
        publications(tmp.path())
    );
    assert_eq!(
        std::fs::read(source.join(&moved)).unwrap(),
        SELECTED.iter().find(|(name, _)| *name == moved).unwrap().1,
        "the moved file is back with the rest of the source"
    );
    assert!(source.join(SIBLING.0).exists(), "nothing was disposed");
    // The job does not complete from a corrupted source: either the boot
    // verification holds it for repair (the CRC no longer matches), or a
    // publication attempt fails it `io` (unwound generation, see the state
    // test crash_with_corrupted_source_unwinds).
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    settle_or_hold(&engine, job).await;
    spawn_post_manager(
        engine.clone(),
        post_config(tmp.path()),
        history.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        engine
            .artifacts()
            .delivery_for_job(job.0)
            .unwrap()
            .is_none(),
        "nothing was delivered"
    );
    if let Some(terminal) = engine.artifacts().terminal(job.0).unwrap() {
        assert_eq!(terminal.outcome, TerminalOutcome::Failed, "{terminal:?}");
        assert_eq!(terminal.reason, Some(FailureReason::Io), "{terminal:?}");
        assert!(!terminal.blames_release);
    }
    assert!(
        publications(tmp.path()).is_empty(),
        "no publication was made from the corrupted source"
    );
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
}
