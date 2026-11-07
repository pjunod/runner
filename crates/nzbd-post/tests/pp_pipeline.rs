//! Post-processing pipeline integration tests against the REAL `par2` and
//! `7z` binaries (ARCHITECTURE.md §9): quick-verify fast path, damage →
//! subprocess repair, unpack + cleanup, extension scripts, failure
//! classification, and the event-driven manager.

use nzbd_engine::{AddOpts, Engine, EngineConfig, EngineHandle, Tuning};
use nzbd_post::manager::{
    process_job as process_job_unscoped, spawn_post_manager, FailureAction, PostConfig, PpFinal,
    RestartPoint, PP_DONE_PARAM,
};
use nzbd_state::history::HistoryDb;
use nzbd_types::{
    DupeInfo, FileEntry, FileId, Job, JobId, JobKind, JobStatus, JobTotals, SegmentState,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

async fn process_job(
    engine: &EngineHandle,
    cfg: &PostConfig,
    history: &Arc<HistoryDb>,
    dest_dir: &Path,
    job: JobId,
) -> Result<PpFinal, nzbd_post::PostError> {
    let cancel = CancellationToken::new();
    let admission = tokio::time::timeout(
        Duration::from_secs(5),
        engine.wait_post_admission(job, &cancel),
    )
    .await
    .expect("fixture processing admission must settle")
    .map_err(|error| nzbd_post::PostError::Subprocess(error.to_string()))?
    .expect("fixture must be eligible for processing");
    let scoped = engine.for_post_admission(&admission);
    let result = process_job_unscoped(&scoped, cfg, history, dest_dir, job).await;
    engine
        .end_post_admission(admission)
        .await
        .map_err(|error| nzbd_post::PostError::Subprocess(error.to_string()))?;
    result
}

async fn assert_workspace_retired(
    inventory: Arc<nzbd_state::artifacts::Inventory>,
    workspace: &nzbd_state::artifacts::Workspace,
    outcome: &str,
    history_seq: i64,
) {
    let retirement_id = format!("retire-{}", workspace.scratch.generation);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let retired = loop {
        let worker_inventory = inventory.clone();
        tokio::task::spawn_blocking(move || worker_inventory.tick())
            .await
            .unwrap()
            .unwrap();
        let operation = inventory.operation(&retirement_id).ok();
        if let Some(operation) = operation
            .as_ref()
            .filter(|operation| operation.state == "succeeded")
        {
            assert_eq!(
                inventory.get(&workspace.scratch.id).unwrap().state,
                "deleted"
            );
            assert!(!workspace.scratch.path.exists());
            break operation.clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "retirement={operation:?}, assessment={:?}",
            inventory
                .workspace_assessment(&workspace.scratch.id)
                .unwrap()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let receipt: serde_json::Value = serde_json::from_str(&retired.request).unwrap();
    assert_eq!(receipt["source"]["artifact"], workspace.source.id);
    assert_eq!(receipt["source"]["generation"], workspace.source.generation);
    assert_eq!(receipt["scratch"]["artifact"], workspace.scratch.id);
    assert_eq!(
        receipt["scratch"]["generation"],
        workspace.scratch.generation
    );
    assert_eq!(receipt["outcome"], outcome);
    assert_eq!(receipt["proof"]["kind"], "local");
    assert_eq!(receipt["proof"]["history_seq"], history_seq);
}

fn crc(data: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(data);
    h.finalize()
}

fn corrupt_archive_tool(root: &Path) -> String {
    use std::os::unix::fs::PermissionsExt;
    let path = root.join("corrupt-archive-tool");
    std::fs::write(&path, "#!/bin/sh\necho 'Data Error' >&2\nexit 2\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path.to_string_lossy().into_owned()
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
    async fn export_fixture_job(&self, job: JobId)
        -> Result<Option<Job>, nzbd_engine::EngineError>;
    async fn import_fixture_job(
        &self,
        base: &Path,
        job: Job,
        fold: bool,
        emit: bool,
    ) -> Result<(), nzbd_engine::EngineError>;
}
impl FixtureImport for EngineHandle {
    async fn export_fixture_job(
        &self,
        job: JobId,
    ) -> Result<Option<Job>, nzbd_engine::EngineError> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match self.export_job(job).await {
                    Err(nzbd_engine::EngineError::Lifecycle(reason)) if reason == "job verification or writer retirement is still in progress; retry later" => tokio::time::sleep(Duration::from_millis(5)).await,
                    result => return result,
                }
            }
        }).await.unwrap_or_else(|error| panic!("fixture verification must settle: {error}; snapshot={:?}; source={:?}", self.snapshot(), self.artifacts().for_job(job.0)))
    }
    async fn import_fixture_job(
        &self,
        base: &Path,
        mut job: Job,
        fold: bool,
        emit: bool,
    ) -> Result<(), nzbd_engine::EngineError> {
        // Real completed downloads record the publication length alongside
        // their durable segment ranges. Preserve that evidence in fixtures.
        for file in &job.files {
            let key = format!("*File:size:{}", file.id.0);
            if !file.segments.is_empty()
                && file
                    .segments
                    .iter()
                    .all(|segment| matches!(segment.state, SegmentState::Done { .. }))
                && !job.params.iter().any(|(name, _)| name == &key)
            {
                let size = file
                    .segments
                    .iter()
                    .map(|segment| match segment.state {
                        SegmentState::Done { offset, len, .. } => offset + u64::from(len),
                        _ => unreachable!(),
                    })
                    .max()
                    .unwrap();
                job.params.push((key, size.to_string()));
            }
        }
        let id = job.id;
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
        self.import_job(job, fold, false).await?;
        let verified = self
            .export_fixture_job(id)
            .await?
            .expect("imported fixture job disappeared");
        if emit {
            self.emit(nzbd_engine::Event::JobFinished {
                job: id,
                name: verified.name,
                status: verified.status,
                health: nzbd_types::Health::calc(&verified.totals).0,
            });
        }
        Ok(())
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
        delivery_lifecycle: false,
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
    check_script_external_result(false).await;
}

#[tokio::test]
async fn script_external_result_preserves_existing_keep_and_source_custody() {
    check_script_external_result(true).await;
}

async fn check_script_external_result(keep: bool) {
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

    let inventory = engine.artifacts();
    let mut original = inventory.for_job(1).unwrap().unwrap();
    if keep {
        original = inventory
            .retention(&original.id, original.revision, true, None)
            .unwrap();
    }
    let other_path = tmp.path().join("unrelated-output");
    std::fs::create_dir(&other_path).unwrap();
    let lease = inventory
        .acquire_terminal_use(1, &|| Ok(()))
        .unwrap()
        .unwrap();
    assert!(
        inventory
            .register_legacy_active(1, tmp.path(), &other_path)
            .is_err(),
        "unrelated enrollment cannot rebind a live processing generation"
    );
    assert_eq!(inventory.for_job(1).unwrap().unwrap().id, original.id);
    drop(lease);
    let workspace = inventory.workspace(1, "par_repair", "abc123").unwrap();
    std::fs::write(workspace.scratch.path.join("scratch"), b"temporary output").unwrap();
    inventory.finish_workspace(&workspace).unwrap();

    let hist = history(tmp.path());
    let cfg = PostConfig {
        scripts_dir: Some(scripts),
        ..PostConfig::default()
    };
    let out = process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(1))
        .await
        .unwrap();
    assert_eq!(out, PpFinal::Success);

    let job = engine.export_fixture_job(JobId(1)).await.unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Completed);
    assert!(job
        .params
        .iter()
        .any(|(k, v)| k == PP_DONE_PARAM && v == "SUCCESS"));

    let entries = hist.list(10).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].status, "SUCCESS");
    assert!(entries[0].final_dir.as_deref().unwrap().ends_with("/final"));
    let retained = inventory.for_job(1).unwrap().unwrap();
    assert_eq!(retained.id, original.id);
    assert_eq!(retained.generation, original.generation);
    assert_eq!(retained.path, dir);
    assert_eq!(retained.state, "retained");
    assert_eq!(retained.keep, keep);
    assert_eq!(std::fs::read(dir.join("payload.bin")).unwrap(), data);
    let param = |key: &str| {
        entries[0]
            .params
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
            .unwrap()
    };
    assert_eq!(param("Artifact:Id"), original.id);
    assert_eq!(param("Artifact:Generation"), original.generation);
    let output = inventory.get(param("Artifact:ExternalOutputId")).unwrap();
    assert_eq!(
        output.generation,
        param("Artifact:ExternalOutputGeneration")
    );
    assert_eq!(output.path, dir.join("final"));
    assert!(!output.owned);
    assert!(output.keep);
    assert_eq!(output.job, None);
    assert!(inventory.delivery_for_job(1).unwrap().is_none());
    if keep {
        assert!(workspace.scratch.path.exists());
        assert!(inventory
            .workspace_assessment(&workspace.scratch.id)
            .unwrap()
            .unwrap()
            .reasons
            .iter()
            .any(|reason| reason == "source_operator_keep"));
    } else {
        assert_workspace_retired(inventory.clone(), &workspace, "SUCCESS", entries[0].seq).await;
    }
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
async fn insufficient_parity_records_failure_and_retains_requested_originals() {
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
    let outcome = process_job(
        &engine,
        &PostConfig {
            failure_action: FailureAction::None,
            ..PostConfig::default()
        },
        &hist,
        &tmp.path().join("dest"),
        JobId(3),
    )
    .await
    .unwrap();
    assert_eq!(outcome, PpFinal::ParFailure);
    let job = engine.export_fixture_job(JobId(3)).await.unwrap().unwrap();
    assert!(!job.held() && !job.ready());
    assert_eq!(job.status, JobStatus::Failed);
    assert_eq!(hist.list(10).unwrap()[0].status, "PAR_FAILURE");
    assert_eq!(std::fs::read(dir.join("payload.bin")).unwrap(), bad);
    assert!(job.params.iter().any(|(k, _)| k == PP_DONE_PARAM));
    engine.shutdown().await;
}

/// The legacy cleanup timeout is re-armed on boot and an ordinary manager
/// pass completes the failed repair without an operator retry.
#[tokio::test]
async fn legacy_repair_timeout_recovers_automatically_and_records_terminal_failure() {
    if !require_tool("par2") {
        return;
    }
    for keep in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let engine = spawn_engine(tmp.path()).await;
        let dir = tmp.path().join("dest/hopeless");
        std::fs::create_dir_all(tmp.path().join("dest")).unwrap();
        engine
            .artifacts()
            .allocate_for_delivery_with_keep(2301, &tmp.path().join("dest"), &dir, true, true, keep)
            .unwrap();
        let data: Vec<u8> = (0..60_000u32).map(|i| ((i * 13) % 249) as u8).collect();
        std::fs::write(dir.join("payload.bin"), &data).unwrap();
        par2_create(&dir, 1, &["payload.bin"]);
        let mut bad = data;
        bad[8_192..49_152].fill(0);
        std::fs::write(dir.join("payload.bin"), &bad).unwrap();
        let mut files = vec![file_entry(1, "payload.bin", Some(crc(&bad)), false)];
        files.extend(par2_entries(&dir, 2));
        let mut job = completed_job(2301, "hopeless", files);
        job.params
            .push(("*Runner:LifecycleEligible".into(), "1".into()));
        if keep {
            job.params
                .push(("*Runner:KeepOnFailure".into(), "1".into()));
        }
        engine
            .artifacts()
            .finish(2301, &dir, &tmp.path().join("dest"), "active")
            .unwrap();
        // A control persisted by a pre-v2 build: a `post_failure` review
        // hold with no schedule.
        job.set_control(&nzbd_types::JobControl {
            version: 1,
            revision: "1".into(),
            lifecycle: "held".into(),
            cause: "post_failure".into(),
            stage: "par_repair".into(),
            retry_policy: "review".into(),
            message: "subprocess failed: read live Keep setting: engine is shutting down".into(),
            instance: "legacy".into(),
            previous_status: Some(JobStatus::Completed),
            manual_pause: false,
            retry_attempt: 0,
            next_retry_at_unix: None,
            blocked_since_unix: None,
            released_at_unix: None,
            deadline_in_s: None,
            deadline_unix: None,
        });
        job.status = JobStatus::Paused;
        engine
            .import_fixture_job(tmp.path(), job, false, false)
            .await
            .unwrap();
        assert!(engine.shutdown().await);

        let engine = spawn_engine(tmp.path()).await;
        let recovered = tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                if let Ok(Some(job)) = engine.export_fixture_job(JobId(2301)).await {
                    break job.control().unwrap();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("startup verifies the persisted source before release");
        assert_eq!(recovered.cause, "engine_unavailable");
        assert_eq!(recovered.retry_policy, "scheduled_retry");
        assert!(recovered.next_retry_at_unix.is_some());
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if engine.snapshot().jobs.iter().any(|job| {
                    job.id == JobId(2301)
                        && job.status == JobStatus::PostQueued
                        && job
                            .control
                            .as_ref()
                            .is_some_and(|control| control.lifecycle == "running")
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("recovery releases the legacy hold without an operator");
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
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if hist.get(JobId(2301)).unwrap().is_some()
                    && engine
                        .snapshot()
                        .jobs
                        .iter()
                        .all(|job| job.id != JobId(2301))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("failed repair must leave the queue without an operator retry: jobs={:?}, history={:?}", engine.snapshot().jobs, hist.list(10).unwrap()));
        let entry = hist.get(JobId(2301)).unwrap().unwrap();
        assert_eq!(entry.status, "PAR_FAILURE");
        assert!(entry
            .params
            .iter()
            .any(|(key, value)| key == "Failure:Reason" && value == "par"));
        assert!(entry
            .params
            .iter()
            .any(|(key, value)| key == "Failure:Message"
                && value.contains("could not be reconstructed")));
        assert_eq!(
            dir.exists(),
            keep,
            "disposition must honor the live Keep setting"
        );
        if keep {
            assert_eq!(std::fs::read(dir.join("payload.bin")).unwrap(), bad);
        }
        cancel.cancel();
        tracker.close();
        tracker.wait().await;
        assert!(engine.shutdown().await);
    }
}

/// `failure_action = park` moves the corpse off the tree the importer
/// watches, intact, and the row points at where it went. `= none` is the
/// old behaviour and stays available for an operator who wants the
/// forensics.
#[tokio::test]
async fn insufficient_parity_obeys_failure_disposition() {
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
        let outcome = process_job(
            &engine,
            &PostConfig {
                failure_action: action,
                failed_dir: Some(parked_root.clone()),
                ..PostConfig::default()
            },
            &hist,
            &tmp.path().join("dest"),
            JobId(job_id),
        )
        .await
        .unwrap();
        assert_eq!(outcome, PpFinal::ParFailure);
        assert!(!engine
            .export_fixture_job(JobId(job_id))
            .await
            .unwrap()
            .unwrap()
            .held());
        // Contract §2.1/§2.6: the terminal is recorded first; disposal
        // follows from the terminal row.
        assert!(
            dir.exists(),
            "nothing touched the bytes before the terminal"
        );
        assert!(
            nzbd_post::manager::follow_up_failure(
                &engine,
                &PostConfig {
                    failure_action: action,
                    failed_dir: Some(parked_root.clone()),
                    ..PostConfig::default()
                },
                &tmp.path().join("dest"),
                JobId(job_id),
            )
            .await
        );
        let retained = if action == FailureAction::Park {
            parked_root.join("hopeless")
        } else {
            dir.clone()
        };
        assert_eq!(std::fs::read(retained.join("payload.bin")).unwrap(), bad);
        assert_eq!(hist.list(10).unwrap()[0].status, "PAR_FAILURE");
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
        engine
            .export_fixture_job(JobId(5))
            .await
            .unwrap()
            .unwrap()
            .status,
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
        let gone = engine.export_fixture_job(JobId(6)).await.unwrap().is_none();
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
        if engine
            .export_fixture_job(JobId(60))
            .await
            .unwrap()
            .is_none()
            && entries.len() == 1
        {
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
    let job = engine.export_fixture_job(JobId(11)).await.unwrap().unwrap();
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

/// `*PP:done` is a retirement instruction, so it may be committed only after
/// the authoritative JSONL history append succeeds. A full state volume must
/// leave the failed queue row recoverable and retry it once history storage is
/// writable again.
#[cfg(unix)]
#[tokio::test]
async fn failed_job_is_not_stamped_when_durable_history_write_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let portable = tmp.path().join("portable");
    std::fs::create_dir(&portable).unwrap();
    let hist =
        Arc::new(HistoryDb::open(&tmp.path().join("history.sqlite"), Some(&portable)).unwrap());
    // The daemon shape: the engine and post-processing share one history.
    let mut cfg = EngineConfig::single_node(
        vec![],
        tmp.path().join("state"),
        tmp.path().join("dest"),
        Tuning::default(),
        None,
    );
    cfg.history = Some(hist.clone());
    let engine = Engine::spawn(cfg).await.expect("engine spawn");
    let log = portable.join("history.jsonl");
    // A directory where the log goes: the append is refused even as root
    // (a 0o555 parent is not, so the test used to pass only as non-root).
    std::fs::create_dir(&log).unwrap();
    let mut events = engine.subscribe();

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
    // Contract §2.1: write-then-emit. The terminal row lands; the history
    // append cannot, so the job holds storage_unavailable on `persistence`
    // and nothing is stamped or announced.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let held = loop {
        let row = engine.export_fixture_job(JobId(92)).await.unwrap().unwrap();
        if let Some(control) = row.control().filter(|c| c.lifecycle == "held") {
            break (row, control);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no persistence hold"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(held.1.cause, "storage_unavailable");
    assert_eq!(held.1.stage, "persistence");
    assert!(!held.0.params.iter().any(|(key, _)| key == PP_DONE_PARAM));
    assert!(!log.is_file(), "no durable JSONL record was possible");
    assert!(hist.list(10).unwrap().is_empty());
    let pending = engine.artifacts().terminal(92).unwrap().unwrap();
    assert!(pending.history_seq.is_none() && !pending.emitted);
    while let Ok(event) = events.try_recv() {
        assert!(
            !matches!(event, nzbd_engine::Event::JobPpFinished { .. }),
            "no terminal event before history is durable"
        );
    }

    // Storage answers again: the row is replayed (boot and the tick do this)
    // and the event goes out with a real history cursor.
    std::fs::remove_dir(&log).unwrap();
    engine.replay_terminals().await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let stamped = engine
            .export_fixture_job(JobId(92))
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
    let row = engine.artifacts().terminal(92).unwrap().unwrap();
    assert!(row.history_seq.unwrap() > 0);
    assert_eq!(hist.list(10).unwrap().len(), 1);
    assert_eq!(std::fs::read_to_string(&log).unwrap().lines().count(), 1);
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

    // Start with an owned download and commit its real relocation, stopping
    // before PP stamps completion. A matching destination name alone is not
    // evidence of a previous move and must never grant ownership.
    let source = tmp.path().join("dest/crashjob");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::write(source.join("payload.bin"), b"already moved").unwrap();

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

    std::fs::create_dir_all(&library).unwrap();
    engine
        .artifacts()
        .relocate(1, &library.join("crashjob"))
        .unwrap();
    assert!(!source.exists());

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
    let job = engine.export_fixture_job(JobId(1)).await.unwrap().unwrap();
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

/// A real UNPACK_FAILURE must still publish independently verified direct
/// media from the same failed attempt. The bad pack remains in the separate
/// Runner source generation with a concrete retained outcome; it must never
/// be copied into Curator's publication tree or deleted as a side effect.
#[tokio::test]
async fn failed_pack_salvages_verified_direct_media_and_excludes_invalid_archive() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let nzb = br#"<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="p" date="1720000000" subject="&quot;episode.mkv&quot; yEnc">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="1" number="1">episode@example</segment></segments>
  </file>
  <file poster="p" date="1720000001" subject="&quot;broken.zip&quot; yEnc">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="1" number="1">broken@example</segment></segments>
  </file>
</nzb>"#;
    let job_id = engine
        .add_nzb_opts(
            "partial-failed-pack",
            nzb,
            AddOpts {
                category: Some("test".into()),
                params: vec![("monarr-delivery".into(), "1".into())],
                ..AddOpts::default()
            },
        )
        .await
        .unwrap();
    let artifact = engine
        .artifacts()
        .for_job(job_id.0)
        .unwrap()
        .expect("new delivery admission allocates a source artifact");
    let good = b"verified episode bytes";
    let bad_pack = b"not a valid zip archive";
    std::fs::write(artifact.path.join("episode.mkv"), good).unwrap();
    std::fs::write(artifact.path.join("broken.zip"), bad_pack).unwrap();
    engine.artifacts().refresh_for_delivery(job_id.0).unwrap();

    let mut job = engine.export_fixture_job(job_id).await.unwrap().unwrap();
    job.status = JobStatus::Completed;
    for file in &mut job.files {
        let (name, bytes) = if file.filename.contains("episode") {
            file.filename = "episode.mkv".into();
            ("episode.mkv", good.as_slice())
        } else {
            file.filename = "broken.zip".into();
            ("broken.zip", bad_pack.as_slice())
        };
        file.subject = name.into();
        file.finalized = true;
        file.crc32 = Some(crc(bytes));
        assert_eq!(file.segments.len(), 1);
        file.segments[0].state = SegmentState::Done {
            offset: 0,
            len: bytes.len() as u32,
            crc: crc(bytes),
        };
    }
    engine
        .import_fixture_job(tmp.path(), job, false, false)
        .await
        .unwrap();

    let corrupt_tool = corrupt_archive_tool(tmp.path());
    let out = process_job(
        &engine,
        &PostConfig {
            unrar_cmd: corrupt_tool.clone(),
            sevenzip_cmd: corrupt_tool,
            failure_action: FailureAction::Delete,
            deobfuscate_final: false,
            ..PostConfig::default()
        },
        &history(tmp.path()),
        &tmp.path().join("dest"),
        job_id,
    )
    .await
    .unwrap();

    assert_eq!(out, PpFinal::UnpackFailure);
    // Contract §2.1: the terminal is recorded before any salvage.
    let terminal = engine.artifacts().terminal(job_id.0).unwrap().unwrap();
    assert_eq!(terminal.reason, Some(nzbd_types::FailureReason::Unpack));
    assert!(terminal.blames_release);
    assert_eq!(
        terminal.salvage,
        nzbd_state::artifacts::SalvageState::Pending
    );
    assert!(engine
        .artifacts()
        .delivery_for_job(job_id.0)
        .unwrap()
        .is_none());
    assert!(
        nzbd_post::manager::follow_up_failure(
            &engine,
            &PostConfig {
                failure_action: FailureAction::Delete,
                ..PostConfig::default()
            },
            &tmp.path().join("dest"),
            job_id,
        )
        .await
    );
    assert_eq!(
        engine
            .artifacts()
            .terminal(job_id.0)
            .unwrap()
            .unwrap()
            .salvage,
        nzbd_state::artifacts::SalvageState::Published
    );
    let delivery = engine
        .artifacts()
        .delivery_for_job(job_id.0)
        .unwrap()
        .expect("good direct media is durably salvaged");
    assert_eq!(delivery.terminal_outcome, "UNPACK_FAILURE");
    assert_eq!(
        delivery
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>(),
        ["episode.mkv"]
    );
    assert_eq!(
        std::fs::read(Path::new(&delivery.publication_path).join("episode.mkv")).unwrap(),
        good
    );
    assert!(!Path::new(&delivery.publication_path)
        .join("broken.zip")
        .exists());
    assert!(delivery
        .file_outcomes
        .iter()
        .any(|file| file.source_path == "broken.zip" && file.outcome != "published"));
    engine.shutdown().await;
}

/// Exercise the actual queue manager's failure path: the downloader job is
/// already complete, extraction fails, and a verified direct episode is
/// published before the manager retires the history row.
#[tokio::test]
async fn manager_salvages_verified_episode_from_failed_pack_before_history_retirement() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let nzb = br#"<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="p" date="1720000000" subject="&quot;episode.mkv&quot; yEnc"><groups><group>alt.binaries.test</group></groups><segments><segment bytes="1" number="1">episode@example</segment></segments></file>
  <file poster="p" date="1720000001" subject="&quot;broken.zip&quot; yEnc"><groups><group>alt.binaries.test</group></groups><segments><segment bytes="1" number="1">broken@example</segment></segments></file>
</nzb>"#;
    let job_id = engine
        .add_nzb_opts(
            "manager-partial-pack",
            nzb,
            AddOpts {
                category: Some("test".into()),
                params: vec![("monarr-delivery".into(), "1".into())],
                ..AddOpts::default()
            },
        )
        .await
        .unwrap();
    let artifact = engine.artifacts().for_job(job_id.0).unwrap().unwrap();
    let good = b"manager verified episode";
    let bad = b"not a valid zip archive";
    std::fs::write(artifact.path.join("episode.mkv"), good).unwrap();
    std::fs::write(artifact.path.join("broken.zip"), bad).unwrap();
    engine.artifacts().refresh_for_delivery(job_id.0).unwrap();
    let mut job = engine.export_fixture_job(job_id).await.unwrap().unwrap();
    job.status = JobStatus::Completed;
    for file in &mut job.files {
        let (name, bytes) = if file.filename.contains("episode") {
            file.filename = "episode.mkv".into();
            ("episode.mkv", good.as_slice())
        } else {
            file.filename = "broken.zip".into();
            ("broken.zip", bad.as_slice())
        };
        file.subject = name.into();
        file.finalized = true;
        file.crc32 = Some(crc(bytes));
        file.segments[0].state = SegmentState::Done {
            offset: 0,
            len: bytes.len() as u32,
            crc: crc(bytes),
        };
    }
    engine
        .import_fixture_job(tmp.path(), job, false, true)
        .await
        .unwrap();

    let hist = history(tmp.path());
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    let corrupt_tool = corrupt_archive_tool(tmp.path());
    spawn_post_manager(
        engine.clone(),
        PostConfig {
            unrar_cmd: corrupt_tool.clone(),
            sevenzip_cmd: corrupt_tool,
            deobfuscate_final: false,
            ..PostConfig::default()
        },
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let delivery = loop {
        if let Some(delivery) = engine
            .artifacts()
            .delivery_for_job(job_id.0)
            .unwrap()
            .as_ref()
        {
            if engine.export_fixture_job(job_id).await.unwrap().is_none()
                && !hist.list(10).unwrap().is_empty()
            {
                break delivery.clone();
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "manager did not finish failed-pack salvage: terminal={:?} job={:?} hist={}",
            engine.artifacts().terminal(job_id.0).unwrap().map(|t| (
                t.reason,
                t.salvage,
                t.emitted,
                t.disposal,
                t.salvage_error
            )),
            engine
                .snapshot()
                .jobs
                .iter()
                .find(|j| j.id == job_id)
                .map(|j| (j.status, j.pp_done, j.control.clone())),
            hist.list(10).unwrap().len()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(delivery.terminal_outcome, "UNPACK_FAILURE");
    assert_eq!(
        delivery
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>(),
        ["episode.mkv"]
    );
    assert!(Path::new(&delivery.publication_path)
        .join("episode.mkv")
        .exists());
    assert!(delivery
        .file_outcomes
        .iter()
        .any(|file| { file.source_path == "broken.zip" && file.outcome != "published" }));

    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
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
    let job = engine.export_fixture_job(JobId(1)).await.unwrap().unwrap();
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
            .export_fixture_job(JobId(998))
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
        .export_fixture_job(JobId(998))
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
            par_fetch_timeout: Duration::from_secs(1),
            ..Default::default()
        },
        &history(tmp.path()),
        &tmp.path().join("dest"),
        JobId(997),
    )
    .await;
    let job = engine
        .export_fixture_job(JobId(997))
        .await
        .unwrap()
        .unwrap();
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

#[tokio::test]
async fn configured_completed_root_receives_only_successful_publication() {
    for conflict in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let engine = spawn_engine(tmp.path()).await;
        let dir = tmp.path().join("dest/intermediate-job");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("deadbeefdeadbeef.mkv"), b"payload").unwrap();
        engine
            .import_fixture_job(
                tmp.path(),
                completed_job(
                    811,
                    "intermediate-job",
                    vec![file_entry(811, "deadbeefdeadbeef.mkv", None, false)],
                ),
                false,
                false,
            )
            .await
            .unwrap();
        let complete = tmp.path().join("complete");
        std::fs::create_dir(&complete).unwrap();
        if conflict {
            std::fs::create_dir(complete.join("intermediate-job")).unwrap();
        }
        let cfg = PostConfig {
            completed_dir: Some(complete.clone()),
            unpack: false,
            ..PostConfig::default()
        };
        let result = process_job(
            &engine,
            &cfg,
            &history(tmp.path()),
            &tmp.path().join("dest"),
            JobId(811),
        )
        .await;
        if conflict {
            assert!(result.is_err());
            assert!(dir.join("intermediate-job.mkv").exists());
            engine
                .artifacts()
                .validate_post_retry(811)
                .expect("final rename remains in the custody manifest after failed publication");
            assert!(!engine
                .export_fixture_job(JobId(811))
                .await
                .unwrap()
                .unwrap()
                .params
                .iter()
                .any(|(k, _)| k == PP_DONE_PARAM));
        } else {
            assert!(result.is_ok());
            assert!(!dir.exists());
            assert_eq!(
                std::fs::read(complete.join("intermediate-job/intermediate-job.mkv")).unwrap(),
                b"payload"
            );
        }
        engine.shutdown().await;
    }
}

#[tokio::test]
async fn manager_operator_retry_of_held_unpack_uses_existing_restart_action() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/held-job");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("Movie.S01E01.mkv"), b"payload").unwrap();
    engine
        .import_fixture_job(
            tmp.path(),
            completed_job(
                812,
                "held-job",
                vec![file_entry(812, "Movie.S01E01.mkv", None, false)],
            ),
            false,
            false,
        )
        .await
        .unwrap();
    assert!(engine
        .hold_job(
            JobId(812),
            nzbd_types::HoldCause::MissingTool,
            "unpack",
            "extraction failed"
        )
        .await
        .unwrap());
    let hist = history(tmp.path());
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    let manager = spawn_post_manager(
        engine.clone(),
        PostConfig::default(),
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    manager
        .restart(JobId(812), RestartPoint::Beginning)
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if engine
            .export_fixture_job(JobId(812))
            .await
            .unwrap()
            .is_none()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "held PP retry did not complete"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
}

#[tokio::test]
async fn held_move_retry_reuses_a_verified_successful_extraction() {
    if !require_tool("7z") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let dir = tmp.path().join("dest/retry-extracted");
    std::fs::create_dir_all(&dir).unwrap();
    let bytes = vec![b'X'; 32768];
    std::fs::write(dir.join("Movie.S01E01.mkv"), &bytes).unwrap();
    let result = std::process::Command::new("7z")
        .args(["a", "-tzip", "-y", "release.zip", "Movie.S01E01.mkv"])
        .current_dir(&dir)
        .output()
        .unwrap();
    assert!(result.status.success());
    std::fs::remove_file(dir.join("Movie.S01E01.mkv")).unwrap();
    engine
        .import_fixture_job(
            tmp.path(),
            completed_job(
                813,
                "retry-extracted",
                vec![file_entry(813, "release.zip", None, false)],
            ),
            false,
            false,
        )
        .await
        .unwrap();
    let completed = tmp.path().join("completed");
    let occupied = completed.join("retry-extracted");
    std::fs::create_dir_all(&occupied).unwrap();
    let hist = history(tmp.path());
    let mut cfg = PostConfig {
        completed_dir: Some(completed.clone()),
        ..Default::default()
    };
    assert!(
        process_job(&engine, &cfg, &hist, &tmp.path().join("dest"), JobId(813))
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(dir.join("Movie.S01E01.mkv")).unwrap(), bytes);
    assert!(engine
        .hold_job(
            JobId(813),
            nzbd_types::HoldCause::StorageUnavailable,
            "move",
            "destination conflict"
        )
        .await
        .unwrap());
    std::fs::remove_dir(&occupied).unwrap();
    // This exact source directory has less than the admission safety floor.
    // Reusing verified extraction output needs no additional source bytes.
    let _headroom =
        nzbd_state::capacity::inject_available_test_limit(&dir, 512 * 1024 * 1024).unwrap();
    // Re-extraction would fail: the successful transform must be reused.
    cfg.sevenzip_cmd = "/no-such-extractor".into();
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    let manager = spawn_post_manager(
        engine.clone(),
        cfg,
        hist.clone(),
        tmp.path().join("dest"),
        None,
        cancel.clone(),
        &tracker,
    );
    manager
        .restart(JobId(813), RestartPoint::Beginning)
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while engine
        .export_fixture_job(JobId(813))
        .await
        .unwrap()
        .is_some()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "retry of completed extraction stayed held"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(hist.list(10).unwrap()[0].status, "SUCCESS");
    assert_eq!(
        std::fs::read(completed.join("retry-extracted/Movie.S01E01.mkv")).unwrap(),
        bytes
    );
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    engine.shutdown().await;
}

/// A sealed file is usable by PAR, but never exposed as finished media first.
#[tokio::test]
async fn private_partial_repairs_with_par_and_fails_without_par() {
    if !require_tool("par2") {
        return;
    }
    for has_par in [true, false] {
        let tmp = tempfile::tempdir().unwrap();
        let engine = spawn_engine(tmp.path()).await;
        let dir = tmp.path().join("dest/partial");
        std::fs::create_dir_all(&dir).unwrap();
        let data: Vec<u8> = (0..60000u32).map(|i| ((i * 11) % 251) as u8).collect();
        std::fs::write(dir.join("payload.bin"), &data).unwrap();
        if has_par {
            par2_create(&dir, 4, &["payload.bin"]);
        }
        let mut partial = data.clone();
        partial[8192..16384].fill(0);
        std::fs::remove_file(dir.join("payload.bin")).unwrap();
        std::fs::write(dir.join(".runner-file-1.part"), &partial).unwrap();
        let mut files = vec![file_entry(1, "obfuscated.bin", None, false)];
        if has_par {
            files.extend(par2_entries(&dir, 2));
        }
        let mut job = completed_job(800, "partial", files);
        job.params
            .push(("*File:repair:1".into(), data.len().to_string()));
        engine
            .import_fixture_job(tmp.path(), job, false, false)
            .await
            .unwrap();
        let hist = history(tmp.path());
        let outcome = process_job(
            &engine,
            &PostConfig {
                unpack: false,
                deobfuscate_final: false,
                failure_action: FailureAction::None,
                ..PostConfig::default()
            },
            &hist,
            &tmp.path().join("dest"),
            JobId(800),
        )
        .await
        .unwrap();
        let job = engine
            .export_fixture_job(JobId(800))
            .await
            .unwrap()
            .unwrap();
        assert!(!job.held());
        assert_eq!(job.ready(), has_par);
        assert_eq!(
            outcome,
            if has_par {
                PpFinal::Success
            } else {
                PpFinal::ParFailure
            }
        );
        assert!(!dir.join("obfuscated.bin").exists());
        assert_eq!(
            std::fs::read(dir.join(".runner-file-1.part")).unwrap(),
            partial
        );
        if has_par {
            assert_eq!(std::fs::read(dir.join("payload.bin")).unwrap(), data);
        }
        assert_eq!(hist.list(10).unwrap().len(), 1);
        engine.shutdown().await;
    }
}

#[tokio::test]
async fn terminal_stamp_receipt_gap_replays_after_restart_without_reprocessing() {
    use nzbd_state::artifacts::{FinalizationProof, SourceGeneration};
    let tmp = tempfile::tempdir().unwrap();
    let engine = spawn_engine(tmp.path()).await;
    let root = tmp.path().join("dest");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("receipt-gap");
    engine.artifacts().allocate(1201, &root, &path).unwrap();
    std::fs::write(path.join("payload.bin"), b"owned original").unwrap();
    let workspace = engine
        .artifacts()
        .workspace(1201, "par_repair", "abc")
        .unwrap();
    std::fs::write(workspace.scratch.path.join("temporary"), b"scratch").unwrap();
    engine.artifacts().finish_workspace(&workspace).unwrap();
    engine
        .artifacts()
        .finish(1201, &path, &root, "completed")
        .unwrap();
    let mut job = completed_job(
        1201,
        "receipt-gap",
        vec![file_entry(1, "payload.bin", None, false)],
    );
    job.params.push((PP_DONE_PARAM.into(), "SUCCESS".into()));
    let hist = history(tmp.path());
    let entry = nzbd_state::HistoryEntry {
        job: job.id,
        name: job.name.clone(),
        category: job.category.clone(),
        final_dir: Some(path.to_string_lossy().into_owned()),
        status: "SUCCESS".into(),
        size: 14,
        health: 1000,
        params: Vec::new(),
        dupe_key: String::new(),
        dupe_score: 0,
        completed_at_unix: 1000,
        hidden: false,
        first_seen_at_unix: None,
        last_seen_at_unix: None,
        seen_count: 0,
        removed_at_unix: None,
        picked_up_by: None,
        record: Some(nzbd_state::JobRecord::from_job(&job)),
        stages: Vec::new(),
        seq: 0,
    };
    let (sequence, _) = hist.record_seq_durable(&entry).unwrap();
    let source = SourceGeneration {
        artifact: workspace.source.id.clone(),
        generation: workspace.source.generation.clone(),
    };
    engine
        .artifacts()
        .stage_workspace_finalization(
            &source,
            "SUCCESS",
            &FinalizationProof::Local {
                history_seq: sequence as u64,
            },
        )
        .unwrap();
    job.params
        .push(("*PP:history-seq".into(), sequence.to_string()));
    engine
        .import_fixture_job(tmp.path(), job, false, false)
        .await
        .unwrap();
    // Model a crash after the durable stamp, before receipt confirmation.
    engine.shutdown().await;
    assert!(workspace.scratch.path.exists());
    let recovered = spawn_engine(tmp.path()).await;
    assert!(
        recovered
            .export_fixture_job(JobId(1201))
            .await
            .unwrap()
            .is_some(),
        "terminal stamp survived restart"
    );
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    spawn_post_manager(
        recovered.clone(),
        PostConfig::default(),
        hist.clone(),
        root,
        None,
        cancel.clone(),
        &tracker,
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while recovered
        .export_fixture_job(JobId(1201))
        .await
        .unwrap()
        .is_some()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "terminal receipt was not replayed: snapshot={:?}, receipts={:?}",
            recovered.snapshot(),
            recovered.artifacts().staged_completions(1201)
        );
        tokio::task::yield_now().await;
    }
    // Queue removal and independent scratch retirement have different acknowledgements.
    assert_workspace_retired(recovered.artifacts(), &workspace, "SUCCESS", sequence).await;
    assert!(path.join("payload.bin").exists());
    assert_eq!(
        hist.list(10).unwrap().len(),
        1,
        "receipt replay does not rerun PP/history"
    );
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    recovered.shutdown().await;
}
