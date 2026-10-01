//! Post-processing pipeline integration tests against the REAL `par2` and
//! `7z` binaries (ARCHITECTURE.md §9): quick-verify fast path, damage →
//! subprocess repair, unpack + cleanup, extension scripts, failure
//! classification, and the event-driven manager.

use nzbd_engine::{Engine, EngineConfig, EngineHandle, Tuning};
use nzbd_post::manager::{
    process_job, spawn_post_manager, FailureAction, PostConfig, PpFinal, PpGate, RestartPoint,
    PP_DONE_PARAM,
};
use nzbd_state::history::HistoryDb;
use nzbd_types::{DupeInfo, FileEntry, FileId, Job, JobId, JobKind, JobStatus, JobTotals};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

fn crc(data: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(data);
    h.finalize()
}

async fn spawn_engine(dir: &Path) -> EngineHandle {
    Engine::spawn(EngineConfig::single_node(
        vec![], // no news servers: PP tests drive imported jobs only
        dir.join("state"),
        dir.join("dest"),
        Tuning::default(),
        None,
    ))
    .await
    .expect("engine spawn")
}

// Synthetic completed jobs stand in for engine-created downloads. Explicitly
// adopt their fixture bytes before import so destructive disposition tests
// exercise owned payloads rather than silently granting ownership in production.
trait FixtureImport {
    async fn import_fixture_job(
        &self,
        base: &Path,
        job: Job,
        fold: bool,
        emit: bool,
    ) -> Result<(), nzbd_engine::EngineError>;
}
impl FixtureImport for EngineHandle {
    async fn import_fixture_job(
        &self,
        base: &Path,
        job: Job,
        fold: bool,
        emit: bool,
    ) -> Result<(), nzbd_engine::EngineError> {
        let name = nzbd_engine::queue::job_dir_name(&job);
        let mut path = base.join("dest").join(&name);
        if !path.is_dir() {
            for entry in std::fs::read_dir(base).unwrap().flatten() {
                let candidate = entry.path().join(&name);
                if candidate.is_dir() {
                    path = candidate;
                    break;
                }
            }
        }
        let inventory = self.artifacts();
        if path.is_dir() && inventory.for_job(job.id.0).unwrap().is_none() {
            let a = inventory
                .discover(path.parent().unwrap(), &path, false)
                .unwrap();
            let a = inventory.inspect(&a.id).unwrap();
            let a = inventory.adopt(&a.id, a.revision).unwrap();
            inventory
                .retention(&a.id, a.revision, false, Some(0))
                .unwrap();
            inventory
                .register_legacy_active(job.id.0, path.parent().unwrap(), &path)
                .unwrap();
        }
        self.import_job(job, fold, emit).await
    }
}

fn file_entry(id: u32, name: &str, crc32: Option<u32>, is_par2: bool) -> FileEntry {
    FileEntry {
        id: FileId(id),
        subject: name.into(),
        filename: name.into(),
        filename_confirmed: true,
        is_par2,
        paused: false,
        groups: vec![],
        date: None,
        segments: vec![],
        crc32,
        finalized: true,
    }
}

fn completed_job(id: u32, name: &str, files: Vec<FileEntry>) -> Job {
    Job {
        id: JobId(id),
        kind: JobKind::Nzb,
        name: name.into(),
        dir_name: String::new(),
        name_provisional: false,
        queued_at_unix: 0,
        original_name: String::new(),
        category: Some("test".into()),
        priority: 0,
        dupe: DupeInfo::default(),
        params: vec![("mykey".into(), "myval".into())],
        files,
        totals: JobTotals::default(),
        status: JobStatus::Completed,
        torrent: None,
        stages: Vec::new(),
    }
}

fn history(dir: &Path) -> Arc<HistoryDb> {
    Arc::new(HistoryDb::open(&dir.join("history.sqlite"), Some(dir)).unwrap())
}

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

/// par2-create a recovery set for `files` inside `dir`.
fn par2_create(dir: &Path, blocks: u32, files: &[&str]) {
    let mut args = vec![
        "create".into(),
        "-q".into(),
        "-q".into(),
        "-s8192".into(),
        format!("-c{blocks}"),
        "set.par2".into(),
    ];
    args.extend(files.iter().map(|f| f.to_string()));
    let ok = std::process::Command::new("par2")
        .args(&args)
        .current_dir(dir)
        .status()
        .expect("par2 binary required (apt-get install par2)")
        .success();
    assert!(ok, "par2 create failed");
}

fn par2_entries(dir: &Path, first_id: u32) -> Vec<FileEntry> {
    let mut out = Vec::new();
    let mut names: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().map(|e| e == "par2").unwrap_or(false))
        .collect();
    names.sort();
    for (i, p) in names.iter().enumerate() {
        let bytes = std::fs::read(p).unwrap();
        out.push(file_entry(
            first_id + i as u32,
            &p.file_name().unwrap().to_string_lossy(),
            Some(crc(&bytes)),
            true,
        ));
    }
    out
}

// ---------------------------------------------------------------------------

/// Intact download: the native quick check proves the set without touching
/// par2; a post-processing script then runs with the NZBGet env and
/// redirects the final dir via `[NZB] FINALDIR=`.
#[tokio::test]
async fn intact_quick_path_then_script() {
    if !require_tool("par2") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/myjob");
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(dir.join("payload.bin"), &data).unwrap();
    par2_create(&dir, 8, &["payload.bin"]);

    let scripts = tmp.path().join("scripts");
    std::fs::create_dir_all(&scripts).unwrap();
    let script = scripts.join("notify.sh");
    std::fs::write(
        &script,
        "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\n\
         [ \"$NZBPP_PARSTATUS\" = 1 ] || exit 94\n\
         [ \"$NZBPP_TOTALSTATUS\" = SUCCESS ] || exit 94\n\
         [ \"$NZBPR_mykey\" = myval ] || exit 94\n\
         mkdir -p \"$NZBPP_DIRECTORY/final\"\n\
         echo \"[NZB] FINALDIR=$NZBPP_DIRECTORY/final\"\n\
         exit 93\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let mut files = vec![file_entry(1, "payload.bin", Some(crc(&data)), false)];
    files.extend(par2_entries(&dir, 2));
    engine
        .import_fixture_job(tmp.path(), completed_job(1, "myjob", files), false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        scripts_dir: Some(scripts),
        ..PostConfig::default()
    };
    let out = process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(1))
        .await
        .unwrap();
    assert_eq!(out, PpFinal::Success);

    let job = engine.export_job(JobId(1)).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Completed);
    assert!(job
        .params
        .iter()
        .any(|(k, v)| k == PP_DONE_PARAM && v == "SUCCESS"));

    let entries = hist.list(10).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].status, "SUCCESS");
    assert!(entries[0].final_dir.as_deref().unwrap().ends_with("/final"));
    engine.shutdown().await;
}

/// Damaged download: quick check spots the bad CRC, par2 verifies + repairs,
/// and the original bytes come back.
#[tokio::test]
async fn corrupt_nested_payload_after_prefix_gets_repaired_without_losing_original() {
    if !require_tool("par2") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/damaged");
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..60_000u32).map(|i| ((i * 7) % 253) as u8).collect();
    std::fs::create_dir(dir.join("Episode01")).unwrap();
    std::fs::write(dir.join("Episode01/payload.bin"), &data).unwrap();
    par2_create(&dir, 16, &["Episode01/payload.bin"]);
    assert_eq!(
        nzbd_post::par2::load_dir(&dir).unwrap().unwrap().files[0].name,
        "Episode01/payload.bin"
    );
    std::fs::rename(dir.join("Episode01/payload.bin"), dir.join("payload.bin")).unwrap();
    std::fs::remove_dir(dir.join("Episode01")).unwrap();

    // Corrupt one block's worth of bytes *as downloaded* (the engine's
    // whole-file CRC reflects the corruption).
    let mut bad = data.clone();
    for b in &mut bad[20_000..20_100] {
        *b ^= 0xA5;
    }
    std::fs::write(dir.join("payload.bin"), &bad).unwrap();

    let mut files = vec![file_entry(1, "payload.bin", Some(crc(&bad)), false)];
    files.extend(par2_entries(&dir, 2));
    engine
        .import_fixture_job(tmp.path(), completed_job(2, "damaged", files), false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig::default();
    let out = process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(2))
        .await
        .unwrap();
    assert_eq!(out, PpFinal::Success);
    assert_eq!(
        std::fs::read(dir.join("Episode01/payload.bin")).unwrap(),
        data,
        "repair must restore the original bytes"
    );
    assert_eq!(hist.list(10).unwrap()[0].status, "SUCCESS");
    engine.shutdown().await;
}

/// Damage beyond the recovery blocks on hand and nothing left to unpause:
/// PAR_FAILURE, job marked Failed.
#[tokio::test]
async fn insufficient_parity_holds_originals_without_terminal_history() {
    if !require_tool("par2") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/hopeless");
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..60_000u32).map(|i| ((i * 13) % 249) as u8).collect();
    std::fs::write(dir.join("payload.bin"), &data).unwrap();
    par2_create(&dir, 1, &["payload.bin"]); // one lonely recovery block

    // Trash well more than one 8 KiB slice.
    let mut bad = data.clone();
    for b in &mut bad[8_192..49_152] {
        *b = 0;
    }
    std::fs::write(dir.join("payload.bin"), &bad).unwrap();

    let mut files = vec![file_entry(1, "payload.bin", Some(crc(&bad)), false)];
    files.extend(par2_entries(&dir, 2));
    engine
        .import_fixture_job(
            tmp.path(),
            completed_job(3, "hopeless", files),
            false,
            false,
        )
        .await
        .unwrap();

    let hist = history(tmp.path());
    assert!(process_job(
        &engine,
        &PostConfig::default(),
        &hist,
        &tmp.path().join("dest"),
        JobId(3)
    )
    .await
    .is_err());
    let job = engine.export_job(JobId(3)).await.unwrap().unwrap();
    assert!(job.held());
    assert_eq!(job.control().unwrap().stage, "par_repair");
    assert!(hist.list(10).unwrap().is_empty());
    assert_eq!(std::fs::read(dir.join("payload.bin")).unwrap(), bad);
    assert!(!job.params.iter().any(|(k, _)| k == PP_DONE_PARAM));
    engine.shutdown().await;
}

/// `failure_action = park` moves the corpse off the tree the importer
/// watches, intact, and the row points at where it went. `= none` is the
/// old behaviour and stays available for an operator who wants the
/// forensics.
#[tokio::test]
async fn insufficient_parity_cannot_trigger_destructive_failure_disposition() {
    if !require_tool("par2") {
        return;
    }
    for (action, job_id) in [
        (nzbd_post::manager::FailureAction::Park, 41u32),
        (nzbd_post::manager::FailureAction::None, 42),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let engine = spawn_engine(tmp.path()).await;
        let dir = tmp.path().join("dest/hopeless");
        std::fs::create_dir_all(&dir).unwrap();

        let data: Vec<u8> = (0..60_000u32).map(|i| ((i * 13) % 249) as u8).collect();
        std::fs::write(dir.join("payload.bin"), &data).unwrap();
        par2_create(&dir, 1, &["payload.bin"]);
        let mut bad = data.clone();
        for b in &mut bad[8_192..49_152] {
            *b = 0;
        }
        std::fs::write(dir.join("payload.bin"), &bad).unwrap();

        let mut files = vec![file_entry(1, "payload.bin", Some(crc(&bad)), false)];
        files.extend(par2_entries(&dir, 2));
        engine
            .import_fixture_job(
                tmp.path(),
                completed_job(job_id, "hopeless", files),
                false,
                false,
            )
            .await
            .unwrap();

        let parked_root = tmp.path().join("failed");
        let hist = history(tmp.path());
        assert!(process_job(
            &engine,
            &PostConfig {
                failure_action: action,
                failed_dir: Some(parked_root.clone()),
                ..PostConfig::default()
            },
            &hist,
            &tmp.path().join("dest"),
            JobId(job_id)
        )
        .await
        .is_err());
        assert!(engine
            .export_job(JobId(job_id))
            .await
            .unwrap()
            .unwrap()
            .held());
        assert_eq!(std::fs::read(dir.join("payload.bin")).unwrap(), bad);
        assert!(hist.list(10).unwrap().is_empty());
        assert!(!parked_root.exists());
        engine.shutdown().await;
    }
}

/// Archive job: unpack extracts, cleanup removes the archive husks.
#[tokio::test]
async fn unpack_then_cleanup() {
    if !require_tool("7z") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/packed");
    std::fs::create_dir_all(&dir).unwrap();

    let inner = b"the actual release content";
    std::fs::write(dir.join("movie.mkv"), inner).unwrap();
    let ok = std::process::Command::new("7z")
        .args(["a", "-tzip", "-y", "release.zip", "movie.mkv"])
        .current_dir(&dir)
        .status()
        .expect("7z binary required (apt-get install p7zip-full)")
        .success();
    assert!(ok);
    std::fs::remove_file(dir.join("movie.mkv")).unwrap();

    let zip_bytes = std::fs::read(dir.join("release.zip")).unwrap();
    let files = vec![file_entry(1, "release.zip", Some(crc(&zip_bytes)), false)];
    engine
        .import_fixture_job(tmp.path(), completed_job(4, "packed", files), false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    // deobfuscate off: this test pins the unpack/cleanup contract; the
    // final-name pass has its own e2e coverage below.
    let cfg = PostConfig {
        deobfuscate_final: false,
        ..PostConfig::default()
    };
    let out = process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(4))
        .await
        .unwrap();
    assert_eq!(out, PpFinal::Success);
    assert_eq!(std::fs::read(dir.join("movie.mkv")).unwrap(), inner);
    assert!(
        dir.join("release.zip").exists(),
        "original archive remains held until independently journaled retirement is admitted"
    );
    engine.shutdown().await;
}

/// A script that exits 94 flips the outcome to SCRIPT_FAILURE and fails
/// the job.
#[tokio::test]
async fn script_error_is_script_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/scripted");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("plain.txt"), b"nothing to verify or unpack").unwrap();

    let scripts = tmp.path().join("scripts");
    std::fs::create_dir_all(&scripts).unwrap();
    let script = scripts.join("fail.sh");
    std::fs::write(
        &script,
        "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\nexit 94\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let files = vec![file_entry(
        1,
        "plain.txt",
        Some(crc(b"nothing to verify or unpack")),
        false,
    )];
    engine
        .import_fixture_job(
            tmp.path(),
            completed_job(5, "scripted", files),
            false,
            false,
        )
        .await
        .unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        scripts_dir: Some(scripts),
        ..PostConfig::default()
    };
    let out = process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(5))
        .await
        .unwrap();
    assert_eq!(out, PpFinal::ScriptFailure);
    assert_eq!(
        engine.export_job(JobId(5)).await.unwrap().unwrap().status,
        JobStatus::Failed
    );
    assert_eq!(hist.list(10).unwrap()[0].status, "SCRIPT_FAILURE");
    engine.shutdown().await;
}

/// The manager end-to-end: an imported finished job is picked up from the
/// event stream, processed, stamped, and never re-processed on a second
/// manager start (the crash-restart scan honors the stamp).
#[tokio::test]
async fn manager_event_driven_and_restart_safe() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/watched");
    std::fs::create_dir_all(&dir).unwrap();
    let data = b"watched payload".to_vec();
    std::fs::write(dir.join("payload.bin"), &data).unwrap();

    let hist = history(tmp.path());
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        PostConfig::default(),
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    // Let the manager subscribe before the finish event fires.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let files = vec![file_entry(1, "payload.bin", Some(crc(&data)), false)];
    engine
        .import_fixture_job(tmp.path(), completed_job(6, "watched", files), false, true)
        .await
        .unwrap();

    // The manager processes the job, records history, then retires it out
    // of the queue (NZBGet parity: finished jobs live in history).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let gone = engine.export_job(JobId(6)).await.unwrap().is_none();
        if gone && hist.list(10).unwrap().len() == 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "manager never processed + retired the job"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(hist.list(10).unwrap()[0].status, "SUCCESS");

    cancel.cancel();
    tracker.close();
    tracker.wait().await;

    // Second manager start: nothing left to process (the job was retired);
    // history stays at exactly one entry.
    let cancel2 = CancellationToken::new();
    let tracker2 = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        PostConfig::default(),
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel2.clone(),
        &tracker2,
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        hist.list(10).unwrap().len(),
        1,
        "restart must not re-process a finished job"
    );
    cancel2.cancel();
    tracker2.close();
    tracker2.wait().await;
    engine.shutdown().await;
}

/// A recovery click must kill the active subprocess before the replacement
/// starts, then resume at the selected boundary. This is the field shape that
/// prompted the control: a job can remain in `Post::Unpack`/`Post::Script`
/// forever unless an operator can reclaim that one attempt without
/// re-downloading the NZB.
#[tokio::test]
async fn manager_restarts_a_hung_stage_in_place() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/recover-me");
    std::fs::create_dir_all(&dir).unwrap();
    let data = b"already downloaded".to_vec();
    std::fs::write(dir.join("payload.bin"), &data).unwrap();

    let scripts = tmp.path().join("scripts");
    std::fs::create_dir_all(&scripts).unwrap();
    let script = scripts.join("hang-once.sh");
    std::fs::write(
        &script,
        "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\n\
         first=\"$NZBPP_DIRECTORY/.first-script-run\"\n\
         if [ ! -e \"$first\" ]; then\n\
           : > \"$first\"\n\
           exec sleep 60\n\
         fi\n\
         : > \"$NZBPP_DIRECTORY/.second-script-run\"\n\
         exit 93\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let hist = history(tmp.path());
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    let manager = spawn_post_manager(
        engine.clone(),
        PostConfig {
            scripts_dir: Some(scripts),
            deobfuscate_final: false,
            ..PostConfig::default()
        },
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    engine
        .import_fixture_job(
            tmp.path(),
            completed_job(
                60,
                "recover-me",
                vec![file_entry(1, "payload.bin", Some(crc(&data)), false)],
            ),
            false,
            true,
        )
        .await
        .unwrap();

    let first = dir.join(".first-script-run");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !first.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the first script attempt never started"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    manager
        .restart(JobId(60), RestartPoint::Scripts)
        .await
        .expect("operator restart accepted");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let entries = hist.list(10).unwrap();
        if engine.export_job(JobId(60)).await.unwrap().is_none() && entries.len() == 1 {
            let scripts_run = entries[0]
                .stages
                .iter()
                .filter(|s| s.stage == nzbd_types::PostStage::Script)
                .count();
            assert_eq!(
                scripts_run, 2,
                "cancelled attempt + replacement are recorded"
            );
            assert!(
                entries[0].stages.iter().all(|s| s.ms.is_some()),
                "the cancelled span must close before its replacement starts"
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the restarted script never finished and retired the job"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(dir.join(".second-script-run").exists());

    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
}

/// Fully obfuscated post: the payload arrives with a garbage name; the
/// rename stage recovers it via the par2 16k-MD5 catalog, evidence paths
/// remap, and the native quick check still proves the set.
#[tokio::test]
async fn obfuscated_names_recovered_then_quick_verified() {
    if !require_tool("par2") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/obfus");
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..45_000u32).map(|i| ((i * 3) % 250) as u8).collect();
    std::fs::write(dir.join("Real.Name.S01E02.mkv"), &data).unwrap();
    par2_create(&dir, 4, &["Real.Name.S01E02.mkv"]);
    // Obfuscate on disk, exactly as an obfuscated post downloads.
    std::fs::rename(dir.join("Real.Name.S01E02.mkv"), dir.join("d41d8cd9")).unwrap();

    let mut files = vec![file_entry(1, "d41d8cd9", Some(crc(&data)), false)];
    files.extend(par2_entries(&dir, 2));
    engine
        .import_fixture_job(tmp.path(), completed_job(7, "obfus", files), false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let out = process_job(
        &engine,
        &PostConfig::default(),
        &hist,
        &tmp.path().join("dest"),
        JobId(7),
    )
    .await
    .unwrap();
    assert_eq!(out, PpFinal::Success);
    assert_eq!(
        std::fs::read(dir.join("Real.Name.S01E02.mkv")).unwrap(),
        data,
        "true name restored, bytes intact"
    );
    assert!(!dir.join("d41d8cd9").exists());
    engine.shutdown().await;
}

/// A job whose only media file kept an obfuscated name through PP gets
/// renamed to the job name (SABnzbd-style final pass, no tools needed).
#[tokio::test]
async fn deobfuscate_final_renames_to_job_name() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/Great.Show.S02.1080p.WEB");
    std::fs::create_dir_all(&dir).unwrap();
    let data = vec![7u8; 60_000];
    std::fs::write(dir.join("a1b2c3d4e5f6a7b8.mkv"), &data).unwrap();
    std::fs::write(dir.join("a1b2c3d4e5f6a7b8.eng.srt"), b"subtitles").unwrap();

    let files = vec![file_entry(
        1,
        "a1b2c3d4e5f6a7b8.mkv",
        Some(crc(&data)),
        false,
    )];
    engine
        .import_fixture_job(
            tmp.path(),
            completed_job(11, "Great.Show.S02.1080p.WEB", files),
            false,
            false,
        )
        .await
        .unwrap();

    let hist = history(tmp.path());
    let out = process_job(
        &engine,
        &PostConfig::default(),
        &hist,
        &tmp.path().join("dest"),
        JobId(11),
    )
    .await
    .unwrap();
    assert_eq!(out, PpFinal::Success);
    assert!(dir.join("Great.Show.S02.1080p.WEB.mkv").exists());
    assert!(
        dir.join("Great.Show.S02.1080p.WEB.eng.srt").exists(),
        "companion subtitle follows the rename"
    );

    // Durable record: the renames land as job params and reach history.
    let job = engine.export_job(JobId(11)).await.unwrap().unwrap();
    assert!(job
        .params
        .iter()
        .any(|(k, v)| k == "Deobfuscate:Count" && v == "2"));
    let entry = &hist.list(10).unwrap()[0];
    let files = &entry
        .params
        .iter()
        .find(|(k, _)| k == "Deobfuscate:Files")
        .expect("history keeps the rename list")
        .1;
    assert!(files.contains("a1b2c3d4e5f6a7b8.mkv → Great.Show.S02.1080p.WEB.mkv"));
    engine.shutdown().await;
}

/// A pack without per-file naming evidence must not acquire invented order.
#[tokio::test]
async fn deobfuscate_final_preserves_unmapped_season_pack() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/Show.S03.1080p.WEB");
    std::fs::create_dir_all(&dir).unwrap();
    let mut files = Vec::new();
    for (i, stem) in ["9f8e7d6c5b4a3f2e", "1a2b3c4d5e6f7a8b", "deadbeefcafef00d"]
        .iter()
        .enumerate()
    {
        let data = vec![i as u8; 40_000];
        std::fs::write(dir.join(format!("{stem}.mkv")), &data).unwrap();
        files.push(file_entry(
            i as u32 + 1,
            &format!("{stem}.mkv"),
            Some(crc(&data)),
            false,
        ));
    }
    engine
        .import_fixture_job(
            tmp.path(),
            completed_job(12, "Show.S03.1080p.WEB", files),
            false,
            false,
        )
        .await
        .unwrap();

    let hist = history(tmp.path());
    let out = process_job(
        &engine,
        &PostConfig::default(),
        &hist,
        &tmp.path().join("dest"),
        JobId(12),
    )
    .await
    .unwrap();
    assert_eq!(out, PpFinal::Success);
    for stem in ["9f8e7d6c5b4a3f2e", "1a2b3c4d5e6f7a8b", "deadbeefcafef00d"] {
        assert!(dir.join(format!("{stem}.mkv")).exists());
    }
    engine.shutdown().await;
}

/// Per-job password (`*Unpack:Password` parameter, NZBGet convention)
/// reaches the extractor.
#[tokio::test]
async fn per_job_password_unlocks_archive() {
    if !require_tool("7z") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/locked");
    std::fs::create_dir_all(&dir).unwrap();

    let inner = b"secret contents";
    std::fs::write(dir.join("file.bin"), inner).unwrap();
    let ok = std::process::Command::new("7z")
        .args(["a", "-tzip", "-y", "-phunter2", "locked.zip", "file.bin"])
        .current_dir(&dir)
        .status()
        .unwrap()
        .success();
    assert!(ok);
    std::fs::remove_file(dir.join("file.bin")).unwrap();

    let zip = std::fs::read(dir.join("locked.zip")).unwrap();
    let files = vec![file_entry(1, "locked.zip", Some(crc(&zip)), false)];
    let mut job = completed_job(8, "locked", files);
    job.params
        .push(("*Unpack:Password".into(), "hunter2".into()));
    engine
        .import_fixture_job(tmp.path(), job, false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    // deobfuscate off: the password path is under test, not final naming.
    let cfg = PostConfig {
        deobfuscate_final: false,
        ..PostConfig::default()
    };
    let out = process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(8))
        .await
        .unwrap();
    assert_eq!(out, PpFinal::Success);
    assert_eq!(std::fs::read(dir.join("file.bin")).unwrap(), inner);
    engine.shutdown().await;
}

/// `failure_action = delete` removes the failed download's files from
/// disk — the health gate is one of the terminal failures it covers.
#[tokio::test]
async fn health_action_delete_removes_files() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/sick");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("partial.bin"), b"broken half-download").unwrap();

    let hist = history(tmp.path());
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        PostConfig {
            failure_action: FailureAction::Delete,
            ..PostConfig::default()
        },
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let files = vec![file_entry(1, "partial.bin", None, false)];
    let mut job = completed_job(9, "sick", files);
    job.status = JobStatus::Failed;
    engine
        .import_fixture_job(tmp.path(), job, false, true)
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let recorded = hist
            .list(10)
            .unwrap()
            .iter()
            .any(|e| e.status == "FAILURE/HEALTH");
        if recorded && !dir.exists() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "health delete never happened (dir exists: {})",
            dir.exists()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
}

/// A local disposition error must not leave a failed queue row warning on
/// every rescan forever. The attempt count is part of the durable queue row,
/// so a daemon restart consumes the remaining budget instead of resetting it.
#[cfg(unix)]
#[tokio::test]
async fn permanently_undeletable_failed_job_retires_after_durable_retry_bound() {
    use std::os::unix::fs::PermissionsExt;

    const ATTEMPTS_PARAM: &str = "*PP:failure-disposition-attempts";

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("dest/undeletable");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("partial.bin"), b"known bad bytes").unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let config = PostConfig {
        failure_action: FailureAction::Delete,
        ..PostConfig::default()
    };
    let hist = history(tmp.path());

    let engine = spawn_engine(tmp.path()).await;
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        config.clone(),
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut job = completed_job(
        95,
        "undeletable",
        vec![file_entry(1, "partial.bin", None, false)],
    );
    job.status = JobStatus::Failed;
    engine
        .import_fixture_job(tmp.path(), job, false, true)
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let attempts = engine.export_job(JobId(95)).await.unwrap().and_then(|job| {
            job.params
                .into_iter()
                .find(|(key, _)| key == ATTEMPTS_PARAM)
                .map(|(_, value)| value)
        });
        if attempts.as_deref() == Some("1") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the first failed disposition attempt was not persisted"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(hist.list(10).unwrap().is_empty());
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;

    let recovered = spawn_engine(tmp.path()).await;
    // Exactly one attempt: the import's `JobFinished` event and the manager's
    // startup scan both observe this row, but they are one retry occasion.
    let durable = recovered.export_job(JobId(95)).await.unwrap().unwrap();
    assert!(
        durable
            .params
            .iter()
            .any(|(key, value)| key == ATTEMPTS_PARAM && value == "1"),
        "one occasion must spend one attempt (durable params: {:?})",
        durable.params
    );
    let cancel2 = CancellationToken::new();
    let tracker2 = TaskTracker::new();
    spawn_post_manager(
        recovered.clone(),
        config.clone(),
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel2.clone(),
        &tracker2,
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let attempts = recovered
            .export_job(JobId(95))
            .await
            .unwrap()
            .and_then(|job| {
                job.params
                    .into_iter()
                    .find(|(key, _)| key == ATTEMPTS_PARAM)
                    .map(|(_, value)| value)
            });
        if attempts.as_deref() == Some("2") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the restart did not resume the durable disposition budget"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(hist.list(10).unwrap().is_empty());
    cancel2.cancel();
    tracker2.close();
    tracker2.wait().await;
    recovered.shutdown().await;

    // Third occasion — again a restart, so each attempt is a distinct one.
    // (Un-restarted, the remaining attempts arrive on the next 30s rescans;
    // driving them with extra `JobFinished` emits would model one occasion as
    // several and is exactly the miscount this bound is meant to resist.)
    let recovered = spawn_engine(tmp.path()).await;
    let cancel3 = CancellationToken::new();
    let tracker3 = TaskTracker::new();
    spawn_post_manager(
        recovered.clone(),
        config,
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel3.clone(),
        &tracker3,
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let stamped = recovered
            .export_job(JobId(95))
            .await
            .unwrap()
            .is_some_and(|job| {
                job.params
                    .iter()
                    .any(|(key, value)| key == PP_DONE_PARAM && value == "FAILURE/HEALTH")
            });
        if stamped && hist.list(10).unwrap().len() == 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the permanent disposition failure never reached history"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let row = hist.list(10).unwrap().remove(0);
    assert_eq!(
        row.final_dir.as_deref(),
        Some(dir.to_string_lossy().as_ref()),
        "history must name the leftover files that require operator cleanup"
    );
    assert!(row.params.iter().any(|(key, value)| {
        key == "Failure:Files" && value.starts_with("checked deletion pending:")
    }));
    assert!(dir.join("partial.bin").is_file());

    // A finish event models another rescan finding the same queue row. The
    // terminal stamp must make this a no-op: no fourth attempt or history row.
    recovered.emit(nzbd_engine::Event::JobFinished {
        job: JobId(95),
        name: "undeletable".into(),
        status: JobStatus::Failed,
        health: 0,
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let terminal = recovered.export_job(JobId(95)).await.unwrap().unwrap();
    assert!(terminal
        .params
        .iter()
        .any(|(key, value)| key == ATTEMPTS_PARAM && value == "3"));
    assert_eq!(hist.list(10).unwrap().len(), 1);

    cancel3.cancel();
    tracker3.close();
    tracker3.wait().await;
    recovered.shutdown().await;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// Regression (#107): the queue scan and a `JobFinished` event routinely
/// observe the *same* still-retryable failed row — a retryable row carries no
/// `*PP:done` stamp, so the overlap guard cannot collapse them. Counting both
/// spends two of the three attempts in one instant, which contradicts
/// ARCHITECTURE.md §9 ("immediately, then on the next two 30-second rescans",
/// ~60s) and silently halves the operator's retry window.
///
/// Ordering here is forced, not slept on: job 95 is in the queue before the
/// manager starts, so the startup scan is its first observation, and job 96 is
/// imported *after* that scan so its own event is always a first observation.
/// The manager awaits each `JobFinished` arm before receiving the next, so job
/// 96's attempt landing proves job 95's duplicate event was already handled.
#[cfg(unix)]
#[tokio::test]
async fn one_failed_row_seen_by_both_scan_and_event_spends_one_attempt() {
    use std::os::unix::fs::PermissionsExt;

    const ATTEMPTS_PARAM: &str = "*PP:failure-disposition-attempts";

    let attempts = |engine: EngineHandle, job: u32| async move {
        engine
            .export_job(JobId(job))
            .await
            .unwrap()
            .and_then(|job| {
                job.params
                    .into_iter()
                    .find(|(key, _)| key == ATTEMPTS_PARAM)
                    .map(|(_, value)| value)
            })
    };

    let tmp = tempfile::tempdir().unwrap();
    let undeletable = |name: &str| {
        let dir = tmp.path().join("dest").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("partial.bin"), b"known bad bytes").unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        dir
    };
    let dir_a = undeletable("seen-twice");
    let dir_b = undeletable("barrier");

    let config = PostConfig {
        failure_action: FailureAction::Delete,
        ..PostConfig::default()
    };
    let hist = history(tmp.path());
    let engine = spawn_engine(tmp.path()).await;

    // In the queue *before* the manager exists: the startup scan is this
    // row's first observation, and no event was ever emitted for it.
    let mut job = completed_job(
        95,
        "seen-twice",
        vec![file_entry(1, "partial.bin", None, false)],
    );
    job.status = JobStatus::Failed;
    engine
        .import_fixture_job(tmp.path(), job, false, false)
        .await
        .unwrap();

    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        config,
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );

    // These two waits are pure liveness — they poll for a step to happen, and
    // the bound only decides how long a genuinely stuck manager hangs before
    // reporting. Neither carries any part of the claim under test, which is
    // the *count* asserted below. 10s rather than this file's usual 5s
    // because instrumentation widens exactly these windows (#107), and a
    // deadline that fires early would redden the fail-closed Coverage gate
    // without anything being wrong.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while attempts(engine.clone(), 95).await.as_deref() != Some("1") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the startup scan never made the first disposition attempt"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Imported after that scan, so job 96 is unobserved until its own event.
    let mut barrier = completed_job(
        96,
        "barrier",
        vec![file_entry(2, "partial.bin", None, false)],
    );
    barrier.status = JobStatus::Failed;
    engine
        .import_fixture_job(tmp.path(), barrier, false, false)
        .await
        .unwrap();

    // The duplicate observation of job 95, then the barrier.
    engine.emit(nzbd_engine::Event::JobFinished {
        job: JobId(95),
        name: "seen-twice".into(),
        status: JobStatus::Failed,
        health: 0,
    });
    engine.emit(nzbd_engine::Event::JobFinished {
        job: JobId(96),
        name: "barrier".into(),
        status: JobStatus::Failed,
        health: 0,
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while attempts(engine.clone(), 96).await.as_deref() != Some("1") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the barrier job's own first observation was dropped"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert_eq!(
        attempts(engine.clone(), 95).await.as_deref(),
        Some("1"),
        "the scan and the event are one retry occasion, not two attempts"
    );
    assert!(
        hist.list(10).unwrap().is_empty(),
        "no row may retire while its budget is unspent"
    );

    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
    for dir in [dir_a, dir_b] {
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

/// Regression (#107): the debounce above stands for a *spent* attempt, so it
/// may only be recorded once the increment reaches the queue snapshot.
/// `import_job_if_present` deliberately reports `false` and restores the
/// previous row when persistence fails, and an attempt that never became
/// durable has cost the operator nothing — suppressing the next observation
/// for half a rescan period would delete a retry that was never made, against
/// the adjacent rule that a retry after a failed commit stays immediate.
///
/// The failure injected is an ordinary state-volume I/O error: the queue
/// snapshot directory is unwritable across the startup scan, so every commit
/// in it fails. Ordering is forced rather than slept on — the scan awaits each
/// failed row in turn, so the gate being consulted for the barrier row (96)
/// proves row 95's whole pass has already returned with its commit refused.
#[cfg(unix)]
#[tokio::test]
async fn an_uncommitted_attempt_does_not_debounce_the_next_observation() {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const ATTEMPTS_PARAM: &str = "*PP:failure-disposition-attempts";
    const FAILURE_AT_PARAM: &str = "*PP:failure-at";

    let attempts = |engine: EngineHandle, job: u32| async move {
        engine
            .export_job(JobId(job))
            .await
            .unwrap()
            .and_then(|job| {
                job.params
                    .into_iter()
                    .find(|(key, _)| key == ATTEMPTS_PARAM)
                    .map(|(_, value)| value)
            })
    };

    let tmp = tempfile::tempdir().unwrap();
    let undeletable = |name: &str| {
        let dir = tmp.path().join("dest").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("partial.bin"), b"known bad bytes").unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        dir
    };
    let dir_a = undeletable("commit-refused");
    let dir_b = undeletable("barrier");

    let hist = history(tmp.path());
    let engine = spawn_engine(tmp.path()).await;

    // Both rows already carry the finalization key, so each pass reaches its
    // counter commit instead of returning at the earlier `*PP:failure-at` one.
    for (id, name) in [(95u32, "commit-refused"), (96, "barrier")] {
        let mut job = completed_job(id, name, vec![file_entry(id, "partial.bin", None, false)]);
        job.status = JobStatus::Failed;
        job.params.push((FAILURE_AT_PARAM.into(), "1000".into()));
        engine
            .import_fixture_job(tmp.path(), job, false, false)
            .await
            .unwrap();
    }

    // The state volume stops accepting writes. `save_snapshot` reports the
    // I/O error and leaves persistence intact, which is exactly the
    // "restored the old row, spent nothing" case.
    let state = tmp.path().join("state");
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o555)).unwrap();

    let barrier_seen = Arc::new(AtomicUsize::new(0));
    let seen = barrier_seen.clone();
    let gate: PpGate = Some(Arc::new(move |job: JobId| {
        if job == JobId(96) {
            seen.fetch_add(1, Ordering::AcqRel);
        }
        true
    }));

    let config = PostConfig {
        failure_action: FailureAction::Delete,
        ..PostConfig::default()
    };
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        config,
        hist.clone(),
        tmp.path().join("dest"),
        gate,
        cancel.clone(),
        &tracker,
    );

    // Pure liveness, as elsewhere in this file: the bound only decides how
    // long a stuck manager hangs before saying so.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while barrier_seen.load(Ordering::Acquire) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the startup scan never reached the barrier row"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        attempts(engine.clone(), 95).await,
        None,
        "a counter that never committed must not appear on the durable row"
    );

    // The state volume comes back, and the row is observed again well inside
    // the debounce window.
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o755)).unwrap();
    engine.emit(nzbd_engine::Event::JobFinished {
        job: JobId(95),
        name: "commit-refused".into(),
        status: JobStatus::Failed,
        health: 0,
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while attempts(engine.clone(), 95).await.as_deref() != Some("1") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "an attempt that never committed suppressed the retry that replaces it"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        hist.list(10).unwrap().is_empty(),
        "no row may retire while its budget is unspent"
    );

    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
    for dir in [dir_a, dir_b] {
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

/// Losing authority after a cross-filesystem park must not let the stale
/// finalizer stamp or announce the job. The already-moved tree is an
/// idempotence witness for the next authority's startup scan.
#[tokio::test]
async fn failed_park_losing_admission_after_move_is_fenced_and_retried() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/held-failure");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("partial.bin"), b"known bad bytes").unwrap();
    let parked_root = tmp.path().join("failed");
    let hist = history(tmp.path());
    let admitted = Arc::new(AtomicBool::new(false));
    let admitted_for_gate = admitted.clone();
    let gate_checks = Arc::new(AtomicUsize::new(0));
    let gate_checks_for_gate = gate_checks.clone();
    let gate: PpGate = Some(Arc::new(move |_| {
        admitted_for_gate.load(Ordering::Acquire)
            || gate_checks_for_gate.fetch_add(1, Ordering::AcqRel) <= 3
    }));
    let config = PostConfig {
        failure_action: FailureAction::Park,
        failed_dir: Some(parked_root.clone()),
        ..PostConfig::default()
    };

    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        config.clone(),
        hist.clone(),
        tmp.path().join("dest"),
        gate.clone(),
        cancel.clone(),
        &tracker,
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut job = completed_job(
        91,
        "held-failure",
        vec![file_entry(1, "partial.bin", None, false)],
    );
    job.status = JobStatus::Failed;
    engine
        .import_fixture_job(tmp.path(), job, false, true)
        .await
        .unwrap();
    // Wait for the fence to be *reached* rather than assuming a fixed budget
    // covers reaching it (#107). Getting there means a cross-filesystem park
    // move plus a durable history write — real filesystem work — and a 200ms
    // sleep asserted only that a loaded machine finishes both in time, which
    // it does not: this failed 55 of 64 runs of an oversubscribed suite.
    //
    // The ordering claim is untouched, and it is what the assertions below
    // still state. `handle_failed_job` consults the gate again *after*
    // `record_seq_durable` and before the terminal stamp, so "history written
    // and the fence closed" is precisely the window in which a stale
    // finalizer would wrongly stamp — waiting for that state tests the fence
    // at its decision point instead of hoping the clock lands inside it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let checks = gate_checks.load(Ordering::Acquire);
        let recorded = hist.list(10).unwrap().len();
        if checks >= 5 && recorded == 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the park never reached the post-history fence \
             (gate checks: {checks}, history rows: {recorded})"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(!dir.exists(), "the move finished before authority changed");
    assert!(
        parked_root.join("held-failure/partial.bin").is_file(),
        "the external side effect remains visible"
    );
    assert_eq!(hist.list(10).unwrap().len(), 1);
    assert!(!engine
        .export_job(JobId(91))
        .await
        .unwrap()
        .unwrap()
        .params
        .iter()
        .any(|(key, _)| key == PP_DONE_PARAM));
    cancel.cancel();
    tracker.close();
    tracker.wait().await;

    tokio::time::sleep(Duration::from_millis(1100)).await;
    admitted.store(true, Ordering::Release);
    let cancel2 = CancellationToken::new();
    let tracker2 = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        config,
        hist.clone(),
        tmp.path().join("dest"),
        gate,
        cancel2.clone(),
        &tracker2,
    );
    let target = parked_root.join("held-failure/partial.bin");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let stamped = engine
            .export_job(JobId(91))
            .await
            .unwrap()
            .is_some_and(|job| job.params.iter().any(|(key, _)| key == PP_DONE_PARAM));
        if target.is_file() && stamped && hist.list(10).unwrap().len() == 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "recovered failed park was not finalized"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!dir.exists());
    assert_eq!(hist.list(10).unwrap().len(), 1, "one logical completion");
    let portable = std::fs::read_to_string(tmp.path().join("history.jsonl")).unwrap();
    assert!(portable.lines().count() >= 1);
    assert!(
        portable.lines().all(|line| line.contains("held-failure")),
        "cross-node retries may duplicate a physical line, but never its logical key"
    );
    cancel2.cancel();
    tracker2.close();
    tracker2.wait().await;
    engine.shutdown().await;
}

/// `*PP:done` is a retirement instruction, so it may be committed only after
/// the authoritative JSONL history append succeeds. A full state volume must
/// leave the failed queue row recoverable and retry it once history storage is
/// writable again.
#[cfg(unix)]
#[tokio::test]
async fn failed_job_is_not_stamped_when_durable_history_write_fails() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let portable = tmp.path().join("portable");
    std::fs::create_dir(&portable).unwrap();
    let hist =
        Arc::new(HistoryDb::open(&tmp.path().join("history.sqlite"), Some(&portable)).unwrap());
    let log = portable.join("history.jsonl");
    std::fs::set_permissions(&portable, std::fs::Permissions::from_mode(0o555)).unwrap();

    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        PostConfig {
            failure_action: FailureAction::None,
            ..PostConfig::default()
        },
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut job = completed_job(92, "history-full", Vec::new());
    job.status = JobStatus::Failed;
    engine
        .import_fixture_job(tmp.path(), job, false, true)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let retained = engine.export_job(JobId(92)).await.unwrap().unwrap();
    assert!(!retained.params.iter().any(|(key, _)| key == PP_DONE_PARAM));
    assert!(!log.exists(), "no durable JSONL record was possible");
    assert!(
        hist.list(10).unwrap().is_empty(),
        "failed portable append must roll back the derived SQLite row"
    );
    cancel.cancel();
    tracker.close();
    tracker.wait().await;

    std::fs::set_permissions(&portable, std::fs::Permissions::from_mode(0o700)).unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let cancel2 = CancellationToken::new();
    let tracker2 = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        PostConfig {
            failure_action: FailureAction::None,
            ..PostConfig::default()
        },
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel2.clone(),
        &tracker2,
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let stamped = engine
            .export_job(JobId(92))
            .await
            .unwrap()
            .is_some_and(|job| job.params.iter().any(|(key, _)| key == PP_DONE_PARAM));
        if stamped && log.is_file() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "failed finalization did not recover after history became writable"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(hist.list(10).unwrap().len(), 1);
    assert_eq!(
        std::fs::read_to_string(&log).unwrap().lines().count(),
        1,
        "the failed pre-write attempt leaves one durable portable completion"
    );
    cancel2.cancel();
    tracker2.close();
    tracker2.wait().await;
    engine.shutdown().await;
}

/// The retry key itself is part of the queue's commit protocol. If queue.json
/// cannot be replaced, no disposition or history append may happen; a restart
/// from the last durable snapshot then finalizes once under a newly committed
/// stable key.
#[cfg(unix)]
#[tokio::test]
async fn failed_job_waits_for_failure_key_snapshot_commit_before_side_effects() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    let engine = spawn_engine(tmp.path()).await;
    let hist = history(&tmp.path().join("history"));
    let mut job = completed_job(93, "snapshot-full", Vec::new());
    job.status = JobStatus::Failed;
    engine
        .import_fixture_job(tmp.path(), job, false, true)
        .await
        .unwrap();
    let durable_before = std::fs::read(state.join("queue.json")).unwrap();

    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o555)).unwrap();
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        PostConfig {
            failure_action: FailureAction::None,
            ..PostConfig::default()
        },
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(hist.list(10).unwrap().is_empty());
    let held = engine.export_job(JobId(93)).await.unwrap().unwrap();
    assert!(!held.params.iter().any(|(key, _)| key == PP_DONE_PARAM));
    cancel.cancel();
    tracker.close();
    tracker.wait().await;

    // Gracefully stop the test runtime, then restore the exact pre-attempt
    // snapshot to model a crash that could only recover the last committed
    // queue state.
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700)).unwrap();
    engine.shutdown().await;
    std::fs::write(state.join("queue.json"), durable_before).unwrap();

    let recovered = spawn_engine(tmp.path()).await;
    let recovered_job = recovered.export_job(JobId(93)).await.unwrap().unwrap();
    assert!(!recovered_job
        .params
        .iter()
        .any(|(key, _)| key == "*PP:failure-at" || key == PP_DONE_PARAM));
    let cancel2 = CancellationToken::new();
    let tracker2 = TaskTracker::new();
    spawn_post_manager(
        recovered.clone(),
        PostConfig {
            failure_action: FailureAction::None,
            ..PostConfig::default()
        },
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel2.clone(),
        &tracker2,
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let stamped = recovered
            .export_job(JobId(93))
            .await
            .unwrap()
            .is_some_and(|job| job.params.iter().any(|(key, _)| key == PP_DONE_PARAM));
        if stamped {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "recovered failure did not finalize after queue storage recovered"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(hist.list(10).unwrap().len(), 1);
    cancel2.cancel();
    tracker2.close();
    tracker2.wait().await;
    recovered.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn failed_final_stamp_commit_rolls_back_live_state_and_retries_once() {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    let engine = spawn_engine(tmp.path()).await;
    let hist = history(&tmp.path().join("history"));
    let mut events = engine.subscribe();
    let checks = Arc::new(AtomicUsize::new(0));
    let checks_for_gate = checks.clone();
    let state_for_gate = state.clone();
    let gate: PpGate = Some(Arc::new(move |_| {
        if checks_for_gate.fetch_add(1, Ordering::AcqRel) == 3 {
            std::fs::set_permissions(&state_for_gate, std::fs::Permissions::from_mode(0o555))
                .unwrap();
        }
        true
    }));
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        PostConfig {
            failure_action: FailureAction::None,
            ..PostConfig::default()
        },
        hist.clone(),
        tmp.path().join("dest"),
        gate,
        cancel.clone(),
        &tracker,
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut job = completed_job(94, "final-stamp-full", Vec::new());
    job.status = JobStatus::Failed;
    engine
        .import_fixture_job(tmp.path(), job, false, true)
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while hist.list(10).unwrap().is_empty() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let unstamped = engine.export_job(JobId(94)).await.unwrap().unwrap();
    assert!(unstamped
        .params
        .iter()
        .any(|(key, _)| key == "*PP:failure-at"));
    assert!(!unstamped.params.iter().any(|(key, _)| key == PP_DONE_PARAM));

    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700)).unwrap();
    engine.emit(nzbd_engine::Event::JobFinished {
        job: JobId(94),
        name: "final-stamp-full".into(),
        status: JobStatus::Failed,
        health: 0,
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut finished = 0;
    loop {
        while let Ok(event) = events.try_recv() {
            if matches!(
                event,
                nzbd_engine::Event::JobPpFinished { job: JobId(94), .. }
            ) {
                finished += 1;
            }
        }
        let stamped = engine
            .export_job(JobId(94))
            .await
            .unwrap()
            .is_some_and(|job| job.params.iter().any(|(key, _)| key == PP_DONE_PARAM));
        if stamped && finished == 1 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    while let Ok(event) = events.try_recv() {
        if matches!(
            event,
            nzbd_engine::Event::JobPpFinished { job: JobId(94), .. }
        ) {
            finished += 1;
        }
    }
    assert_eq!(finished, 1);
    assert_eq!(hist.list(10).unwrap().len(), 1);
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// N6 — category destination honesty (docs/INTEGRATION_PLAN.md)
// ---------------------------------------------------------------------------

/// `[[category]] dest_dir` was parsed and advertised to compat clients as
/// `CategoryN.DestDir` for a long time while post-processing quietly wrote
/// somewhere else. An *arr that path-maps off the advertised value then
/// looks in a folder that will never contain anything — a silent import
/// failure with nothing in any log to explain it. Advertised must equal
/// actual, and "actual" means: the files are there, and every place we
/// report the path agrees.
#[tokio::test]
async fn category_dest_dir_is_where_the_files_actually_land() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/catjob");
    std::fs::create_dir_all(&dir).unwrap();
    let data = b"payload".to_vec();
    std::fs::write(dir.join("payload.bin"), &data).unwrap();

    let library = tmp.path().join("library/tv");
    let mut job = completed_job(1, "catjob", vec![file_entry(1, "payload.bin", None, false)]);
    job.category = Some("TV".into()); // matched case-insensitively
    engine
        .import_fixture_job(tmp.path(), job, false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        // Off so the assertion below can name the file: the final
        // deobfuscation pass would rename it to the job name, which is a
        // different feature's business.
        deobfuscate_final: false,
        categories: vec![nzbd_post::manager::CategoryRule {
            name: "tv".into(),
            dest_dir: Some(library.clone()),
            ..Default::default()
        }],
        ..PostConfig::default()
    };
    let out = process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(1))
        .await
        .unwrap();
    assert_eq!(out, PpFinal::Success);

    let landed = library.join("catjob/payload.bin");
    assert!(
        std::fs::read(&landed).unwrap_or_default() == data,
        "files must be under the category destination: {}",
        landed.display()
    );
    assert!(
        !tmp.path().join("dest/catjob").exists(),
        "and must not be left behind in the global destination"
    );
    let entry = &hist.list(10).unwrap()[0];
    assert_eq!(
        entry.final_dir.as_deref(),
        library.join("catjob").to_str(),
        "history must report where the files are, not where they started"
    );
    engine.shutdown().await;
}

/// A category that turns unpacking off must actually leave the archive
/// alone. (The key was advertised as `CategoryN.Unpack` and ignored.)
#[tokio::test]
async fn category_unpack_false_leaves_the_archive_alone() {
    if !require_tool("7z") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/nounpack");
    std::fs::create_dir_all(&dir).unwrap();
    let inner = dir.join("inside.txt");
    std::fs::write(&inner, b"secret").unwrap();
    let archive = dir.join("bundle.7z");
    let ok = std::process::Command::new("7z")
        .arg("a")
        .arg(&archive)
        .arg(&inner)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    assert!(ok, "could not build the test archive");
    std::fs::remove_file(&inner).unwrap();

    let mut job = completed_job(1, "nounpack", vec![file_entry(1, "bundle.7z", None, false)]);
    job.category = Some("raw".into());
    engine
        .import_fixture_job(tmp.path(), job, false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        // Off, like the sibling category tests: the final deobfuscation
        // pass renames a lone leftover file to the job name, so with it on
        // the surviving archive is `nounpack.7z` and an assertion by name
        // fails for a reason that has nothing to do with unpacking.
        deobfuscate_final: false,
        categories: vec![nzbd_post::manager::CategoryRule {
            name: "raw".into(),
            unpack: Some(false),
            ..Default::default()
        }],
        ..PostConfig::default()
    };
    process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(1))
        .await
        .unwrap();

    assert!(archive.is_file(), "the archive must survive");
    assert!(
        !inner.exists(),
        "nothing should have been extracted for a category with unpack = false"
    );
    engine.shutdown().await;
}

/// `extensions` selects which post-processing scripts a category runs.
/// The key was parsed and then neither implemented nor removed; leaving it
/// half-done is the same lie as `dest_dir` was.
#[tokio::test]
async fn category_extensions_select_which_scripts_run() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/scripted");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("payload.bin"), b"x").unwrap();

    let scripts = tmp.path().join("scripts");
    std::fs::create_dir_all(&scripts).unwrap();
    let touched = tmp.path().join("touched");
    for name in ["wanted.sh", "unwanted.sh"] {
        let path = scripts.join(name);
        std::fs::write(
            &path,
            format!(
                "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\n\
                 echo {name} >> {}\nexit 93\n",
                touched.display()
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    let mut job = completed_job(
        1,
        "scripted",
        vec![file_entry(1, "payload.bin", None, false)],
    );
    job.category = Some("tv".into());
    engine
        .import_fixture_job(tmp.path(), job, false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        scripts_dir: Some(scripts),
        categories: vec![nzbd_post::manager::CategoryRule {
            name: "tv".into(),
            extensions: vec!["wanted".into()], // by stem; the file is wanted.sh
            ..Default::default()
        }],
        ..PostConfig::default()
    };
    process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(1))
        .await
        .unwrap();

    let ran = std::fs::read_to_string(&touched).unwrap_or_default();
    assert!(ran.contains("wanted.sh"), "the selected script must run");
    assert!(
        !ran.contains("unwanted.sh"),
        "a script outside the category's extensions must not run: {ran:?}"
    );
    engine.shutdown().await;
}

/// A job with no matching category behaves exactly as before — the global
/// destination, the global unpack setting, every discovered script.
#[tokio::test]
async fn a_job_without_a_category_rule_is_untouched_by_any_of_this() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/plain");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("payload.bin"), b"x").unwrap();

    let mut job = completed_job(1, "plain", vec![file_entry(1, "payload.bin", None, false)]);
    job.category = Some("movies".into()); // configured category is "tv"
    engine
        .import_fixture_job(tmp.path(), job, false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        deobfuscate_final: false,
        categories: vec![nzbd_post::manager::CategoryRule {
            name: "tv".into(),
            dest_dir: Some(tmp.path().join("library/tv")),
            ..Default::default()
        }],
        ..PostConfig::default()
    };
    process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(1))
        .await
        .unwrap();

    assert!(dir.join("payload.bin").is_file(), "stays put");
    assert_eq!(hist.list(10).unwrap()[0].final_dir.as_deref(), dir.to_str());
    engine.shutdown().await;
}

/// Crash between the category move and the `*PP:done` stamp. The files
/// are at the library, the global path is gone, and the next pass has to
/// pick up where the last one left off. Before this was handled, the
/// re-run drove the whole pipeline against a directory that no longer
/// existed: par2 load failed with ENOENT, `process_job` returned an
/// error, and the job was wedged forever — never stamped, never in
/// history, never announced, never retired from the queue. "The stages
/// are idempotent" is the assumption the entire crash model rests on, and
/// Move is the one stage that relocates its own input.
#[tokio::test]
async fn post_processing_resumes_after_a_crash_between_move_and_stamp() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let library = tmp.path().join("library/tv");

    // The state a crash right after the move leaves behind.
    std::fs::create_dir_all(library.join("crashjob")).unwrap();
    std::fs::write(library.join("crashjob/payload.bin"), b"already moved").unwrap();
    assert!(!tmp.path().join("dest/crashjob").exists());

    let mut job = completed_job(
        1,
        "crashjob",
        vec![file_entry(1, "payload.bin", None, false)],
    );
    job.category = Some("tv".into());
    engine
        .import_fixture_job(tmp.path(), job, false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        deobfuscate_final: false,
        categories: vec![nzbd_post::manager::CategoryRule {
            name: "tv".into(),
            dest_dir: Some(library.clone()),
            ..Default::default()
        }],
        ..PostConfig::default()
    };
    let out = process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(1))
        .await
        .expect("the re-run must complete, not error on the vanished source");
    assert_eq!(out, PpFinal::Success);

    assert_eq!(
        std::fs::read(library.join("crashjob/payload.bin")).unwrap(),
        b"already moved",
        "the moved files must survive the re-run untouched"
    );
    assert_eq!(
        hist.list(10).unwrap()[0].final_dir.as_deref(),
        library.join("crashjob").to_str(),
        "and history must name where they are"
    );
    let job = engine.export_job(JobId(1)).await.unwrap().unwrap();
    assert!(
        job.params.iter().any(|(k, _)| k == PP_DONE_PARAM),
        "the job must end up stamped rather than looping forever"
    );
    engine.shutdown().await;
}

/// An interrupted cross-filesystem move must leave the library either
/// untouched or complete — never a half-copied folder that a consumer
/// would happily import from and that every later `rename` would then
/// fail against with ENOTEMPTY.
#[tokio::test]
async fn legacy_move_scratch_is_kept_until_its_ownership_is_reviewed() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/atomic");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("payload.bin"), b"content").unwrap();

    // Simulate the scratch dir a killed copy leaves behind. The next
    // attempt must clear it rather than trip over it.
    let library = tmp.path().join("library/tv");
    std::fs::create_dir_all(library.join("atomic.pp-move.local")).unwrap();
    std::fs::write(library.join("atomic.pp-move.local/partial.bin"), b"half").unwrap();

    let mut job = completed_job(1, "atomic", vec![file_entry(1, "payload.bin", None, false)]);
    job.category = Some("tv".into());
    engine
        .import_fixture_job(tmp.path(), job, false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        deobfuscate_final: false,
        categories: vec![nzbd_post::manager::CategoryRule {
            name: "tv".into(),
            dest_dir: Some(library.clone()),
            ..Default::default()
        }],
        ..PostConfig::default()
    };
    process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(1))
        .await
        .unwrap();

    assert_eq!(
        std::fs::read(library.join("atomic/payload.bin")).unwrap(),
        b"content"
    );
    assert!(
        library.join("atomic.pp-move.local").exists(),
        "an unjournaled legacy scratch directory must not be deleted by name"
    );
    let unknown = engine
        .artifacts()
        .discover(&library, &library.join("atomic.pp-move.local"), false)
        .unwrap();
    assert!(!unknown.owned && unknown.keep);
    engine.shutdown().await;
}

/// A multi-volume archive must extract to the WHOLE file, and a set with a
/// hole in it must fail rather than deliver part of one.
///
/// This is the end-to-end guard on the worst defect this project has had: a
/// 48 GiB remux delivered as a 500 MiB file — exactly one volume, minus its
/// header — reported as a completed download. Three things had to line up
/// for that: the signature renamer severed an old-style volume chain
/// (pinned by `rename::tests::a_numbered_volume_set_is_never_renamed`),
/// unrar's result was judged on its exit code alone with its output
/// suppressed, and nothing ever compared what was extracted against what was
/// promised.
///
/// `rar` is not free software and CI will not have it, so this self-skips
/// rather than going through `require_tool` — which would turn a missing
/// non-free package into a CI failure. The unit test above carries the
/// regression in CI; this one carries the proof that the pipeline really
/// extracts a real multi-volume set.
#[tokio::test]
async fn a_multi_volume_archive_extracts_whole_or_fails() {
    if std::process::Command::new("rar")
        .arg("-iver")
        .output()
        .is_err()
    {
        eprintln!("SKIPPED: `rar` not installed — cannot build a multi-volume set");
        return;
    }
    if !require_tool("unrar") {
        return;
    }

    // A payload several volumes long, and incompressible so that -m0 volumes
    // are genuinely the size we asked for.
    let payload: Vec<u8> = (0..900_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
        .collect();

    for (case, drop_a_volume) in [("whole", false), ("holed", true)] {
        let tmp = tempfile::tempdir().unwrap();
        let engine = spawn_engine(tmp.path()).await;
        let dir = tmp.path().join(format!("dest/{case}"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("movie.mkv"), &payload).unwrap();

        let built = std::process::Command::new("rar")
            .args(["a", "-m0", "-v200k", "-idq", "-ep", "set.rar", "movie.mkv"])
            .current_dir(&dir)
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        assert!(built, "could not build the multi-volume set");
        std::fs::remove_file(dir.join("movie.mkv")).unwrap();

        let mut volumes: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "rar"))
            .collect();
        volumes.sort();
        assert!(
            volumes.len() >= 3,
            "the point of this test is more than one volume, got {volumes:?}"
        );
        if drop_a_volume {
            // Lose a middle volume: the chain now stops partway, which is
            // what a severed or incomplete set looks like to unrar.
            std::fs::remove_file(&volumes[1]).unwrap();
        }

        let files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .enumerate()
            .map(|(i, e)| {
                let name = e.file_name().to_string_lossy().into_owned();
                let bytes = std::fs::read(e.path()).unwrap();
                file_entry(i as u32 + 1, &name, Some(crc(&bytes)), false)
            })
            .collect();
        let job_id = if drop_a_volume { 61 } else { 60 };
        engine
            .import_fixture_job(tmp.path(), completed_job(job_id, case, files), false, false)
            .await
            .unwrap();

        let hist = history(tmp.path());
        let cfg = PostConfig {
            deobfuscate_final: false,
            ..PostConfig::default()
        };
        let out = process_job(
            &engine,
            &cfg,
            &hist,
            &tmp.path().join("dest"),
            JobId(job_id),
        )
        .await
        .unwrap();

        if drop_a_volume {
            assert_ne!(
                out,
                PpFinal::Success,
                "a set with a missing volume produced a SUCCESS — this is the \
                 shape that shipped 500 MiB of a 48 GiB film"
            );
            let extracted = dir.join("movie.mkv");
            assert!(
                !extracted.exists() || std::fs::metadata(&extracted).unwrap().len() == 0,
                "a failed unpack must not leave a truncated film in place"
            );
        } else {
            assert_eq!(out, PpFinal::Success, "a complete set must extract");
            let got = std::fs::read(dir.join("movie.mkv")).unwrap();
            assert_eq!(
                got.len(),
                payload.len(),
                "extracted {} bytes of a {}-byte file — one volume is not the film",
                got.len(),
                payload.len()
            );
            assert_eq!(got, payload, "the extracted bytes are not the original");
        }
        engine.shutdown().await;
    }
}

/// The stage timeline reaches history — every stage the job actually ran,
/// in order, each with a duration.
///
/// The post manager has always measured this. `Stages::enter` stamps an
/// `Instant` on every transition and `close` banks the elapsed time — into
/// the process-wide `PpStageStats` histogram, and nowhere else. So "how
/// long does unpack take across all jobs" was answerable from the same
/// measurement that could not answer "how long did unpack take for THIS
/// job". This test pins the second question.
///
/// The last stage matters most: it is closed by `Stages::finish` before
/// the finalize export rather than by `Drop` after it, so the entry that
/// lands in history does not show its final stage still running.
#[tokio::test]
async fn the_stage_timeline_reaches_history() {
    if !require_tool("par2") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/timed");
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(dir.join("payload.bin"), &data).unwrap();
    par2_create(&dir, 8, &["payload.bin"]);

    let mut files = vec![file_entry(1, "payload.bin", Some(crc(&data)), false)];
    files.extend(par2_entries(&dir, 2));
    engine
        .import_fixture_job(tmp.path(), completed_job(1, "timed", files), false, false)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let out = process_job(
        &engine,
        &PostConfig::default(),
        &hist,
        &tmp.path().join("dest"),
        JobId(1),
    )
    .await
    .unwrap();
    assert_eq!(out, PpFinal::Success);

    let entries = hist.list(10).unwrap();
    assert_eq!(entries.len(), 1);
    let stages = &entries[0].stages;
    assert!(
        !stages.is_empty(),
        "post-processing ran, so history must say where the time went"
    );

    // Every span is closed. An open span in history means the pipeline
    // ended without the timeline being told — the exact failure that
    // leaving this to `Drop` produces.
    for s in stages {
        assert!(
            s.ms.is_some(),
            "stage {:?} is still running in a finished job's history entry",
            s.stage
        );
    }

    let names: Vec<&str> = stages.iter().map(|s| s.stage.as_str()).collect();
    assert!(
        names.contains(&"par_verify"),
        "a par set was present, so verify must appear: {names:?}"
    );
    // Only the stages that actually ran. This job needs no repair, has
    // nothing to clean and is already in its destination, and the
    // timeline reflects that rather than listing the whole pipeline with
    // zeroes — an operator reading it should see what happened, not what
    // could have.
    assert!(
        !names.contains(&"par_repair"),
        "an intact job never entered repair: {names:?}"
    );
    assert!(
        !names.contains(&"script"),
        "no scripts were configured: {names:?}"
    );
    // Spans are appended in execution order, so their starts never go
    // backwards.
    let starts: Vec<i64> = stages.iter().map(|s| s.started_at_unix).collect();
    assert!(
        starts.windows(2).all(|w| w[0] <= w[1]),
        "spans must be in pipeline order, got {starts:?}"
    );
    // None left open — including the last, which is the one
    // `Stages::finish` exists to close in time.
    assert!(stages.last().unwrap().ms.is_some());

    // And the live queue view agrees with what history recorded.
    let job = engine.export_job(JobId(1)).await.unwrap().unwrap();
    assert_eq!(job.stages.len(), stages.len());
    engine.shutdown().await;
}

#[tokio::test]
async fn review_manager_preserves_extractor_capacity_hold() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/capacity-review");
    std::fs::create_dir_all(&dir).unwrap();
    let bytes = b"synthetic archive";
    std::fs::write(dir.join("payload.zip"), bytes).unwrap();
    let tool = tmp.path().join("seven");
    std::fs::write(
        &tool,
        "#!/bin/sh\necho 'ERROR: No space left on device' >&2\nexit 2\n",
    )
    .unwrap();
    std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o755)).unwrap();
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        engine.clone(),
        PostConfig {
            sevenzip_cmd: tool.display().to_string(),
            deobfuscate_final: false,
            ..Default::default()
        },
        history(tmp.path()),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    engine
        .import_fixture_job(
            tmp.path(),
            completed_job(
                998,
                "capacity-review",
                vec![file_entry(998, "payload.zip", Some(crc(bytes)), false)],
            ),
            false,
            true,
        )
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let control = loop {
        if let Some(c) = engine
            .export_job(JobId(998))
            .await
            .unwrap()
            .and_then(|j| j.control())
        {
            break c;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no control after extractor failure"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    let control = engine
        .export_job(JobId(998))
        .await
        .unwrap()
        .unwrap()
        .control()
        .unwrap_or(control);
    engine.shutdown().await;
    assert_eq!(
        control.cause, "capacity",
        "outer manager overwrote typed hold: {control:?}"
    );
    assert_eq!(control.retry_policy, "resume_same_job");
}

#[tokio::test]
async fn review_missing_parity_requests_available_paused_volume() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/delayed-review");
    std::fs::create_dir_all(&dir).unwrap();
    let data: Vec<u8> = (0..50_000u32).map(|i| ((i * 7) % 253) as u8).collect();
    std::fs::write(dir.join("payload.bin"), &data).unwrap();
    par2_create(&dir, 4, &["payload.bin"]);
    let mut pars = par2_entries(&dir, 910);
    for f in &mut pars {
        if f.filename.contains(".vol") {
            std::fs::remove_file(dir.join(&f.filename)).unwrap();
            f.paused = true;
            f.finalized = false;
        }
    }
    let paused: Vec<_> = pars.iter().filter(|f| f.paused).map(|f| f.id).collect();
    assert!(!paused.is_empty());
    let mut bad = data;
    bad[25_000] ^= 0xff;
    std::fs::write(dir.join("payload.bin"), &bad).unwrap();
    let mut files = vec![file_entry(909, "payload.bin", Some(crc(&bad)), false)];
    files.extend(pars);
    engine
        .import_fixture_job(
            tmp.path(),
            completed_job(997, "delayed-review", files),
            false,
            false,
        )
        .await
        .unwrap();
    let _ = process_job(
        &engine,
        &PostConfig {
            unpack: false,
            par_fetch_timeout: Duration::from_millis(20),
            ..Default::default()
        },
        &history(tmp.path()),
        &tmp.path().join("dest"),
        JobId(997),
    )
    .await;
    let job = engine.export_job(JobId(997)).await.unwrap().unwrap();
    engine.shutdown().await;
    assert!(
        job.files
            .iter()
            .any(|f| paused.contains(&f.id) && !f.paused),
        "repair gave up while all available recovery volumes remained paused; control={:?}",
        job.control()
    );
}

#[tokio::test]
async fn review_terminal_history_and_event_keep_resolved_control() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/resolved-review");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("payload.bin"), b"payload").unwrap();
    let control: nzbd_types::JobControl = serde_json::from_value(serde_json::json!({
     "version":1,"revision":"43","lifecycle":"running","cause":"capacity",
     "stage":"extract","retry_policy":"resume_same_job","message":"resumed","instance":"same"
    }))
    .unwrap();
    let mut job = completed_job(
        996,
        "resolved-review",
        vec![file_entry(996, "payload.bin", Some(crc(b"payload")), false)],
    );
    job.params.push((
        nzbd_types::CONTROL_PARAM.into(),
        serde_json::to_string(&control).unwrap(),
    ));
    engine
        .import_fixture_job(tmp.path(), job, false, false)
        .await
        .unwrap();
    let hist = history(tmp.path());
    let mut events = engine.subscribe();
    let result = process_job(
        &engine,
        &PostConfig {
            unpack: false,
            deobfuscate_final: false,
            ..Default::default()
        },
        &hist,
        &tmp.path().join("dest"),
        JobId(996),
    )
    .await
    .unwrap();
    assert_eq!(result, PpFinal::Success);
    let row = hist.get(JobId(996)).unwrap().unwrap();
    let saved = row
        .params
        .iter()
        .find(|(key, _)| key == nzbd_types::CONTROL_PARAM)
        .unwrap();
    assert_eq!(
        serde_json::from_str::<nzbd_types::JobControl>(&saved.1).unwrap(),
        control
    );
    let mut observed = false;
    while let Ok(event) = events.try_recv() {
        if let nzbd_engine::Event::JobPpFinished {
            job: JobId(996),
            params,
            ..
        } = event
        {
            assert!(params.contains(saved));
            observed = true;
        }
    }
    assert!(observed, "completion event omitted the resolving control");
    engine.shutdown().await;
}
