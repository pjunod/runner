use super::*;

/// Execute genuine ordered persistence/IO completions before observing a
/// direct Owner fixture. The worker remains asynchronous and its bounded
/// result channel is drained exactly as the production owner loop does.
pub(super) async fn pump(owner: &mut Owner, messages: &mut mpsc::Receiver<EngineMsg>) {
    tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                while let Ok(message) = messages.try_recv() { owner.on_msg(message); }
                if owner.persistence.in_flight() == 0 && owner.persistence_callbacks.is_empty() && owner.io_callbacks.is_empty() {
                    tokio::task::yield_now().await;
                    match messages.try_recv() {
                        Ok(message) => { owner.on_msg(message); continue; },
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => panic!("fixture result channel disconnected before worker completion"),
                    }
                }
                tokio::select! {
                    message = messages.recv() => owner.on_msg(message.expect("fixture worker result channel")),
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {},
                }
            }
        }).await.expect("real persistence/IO worker did not reach an idle acknowledgement boundary");
}

/// Public export remains fenced until the real imported payload verification
/// completes. Only its explicit busy result is retryable in these fixtures.
async fn export_job_ready_for_test(
    engine: &crate::EngineHandle,
    job: JobId,
) -> Result<Option<Job>, crate::EngineError> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match engine.export_job(job).await {
                Err(crate::EngineError::Lifecycle(reason))
                    if reason == "job verification or writer retirement is still in progress; retry later" => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                result => return result,
            }
        }
    }).await.expect("actual imported job verification did not complete")
}

async fn persist_snapshot_for_test(owner: &mut Owner, messages: &mut mpsc::Receiver<EngineMsg>) {
    let (reply, result) = oneshot::channel();
    owner.persist_then(move |_, ok| {
        let _ = reply.send(ok);
    });
    pump(owner, messages).await;
    assert!(
        result.await.unwrap(),
        "fixture snapshot did not cross its durable acknowledgement"
    );
}

async fn import_job_for_test(
    owner: &mut Owner,
    messages: &mut mpsc::Receiver<EngineMsg>,
    job: Job,
) -> bool {
    let (reply, result) = oneshot::channel();
    owner.replace_job_then(job, false, false, false, false, move |_, outcome| {
        let _ = reply.send(outcome.is_ok());
    });
    pump(owner, messages).await;
    result.await.unwrap()
}

async fn allocate_for_test(
    owner: &mut Owner,
    messages: &mut mpsc::Receiver<EngineMsg>,
    job: JobId,
    fresh: bool,
) {
    let first = if fresh {
        owner.allocate_new_payload(job)
    } else {
        owner.ensure_allocation(job, false)
    };
    assert!(
        first.is_ok() || owner.allocating_jobs.contains(&job),
        "allocation was refused rather than scheduled: {first:?}"
    );
    pump(owner, messages).await;
    let completed = if fresh {
        owner.allocate_new_payload(job)
    } else {
        owner.ensure_allocation(job, false)
    };
    completed.expect("real allocation callback must establish the requested fixture allocation");
    pump(owner, messages).await;
}

/// Drive prerequisites through real actor completions until a work
/// request has a definitive answer; loading is not a successful lease.
async fn grant_work_for_test(
    owner: &mut Owner,
    messages: &mut mpsc::Receiver<EngineMsg>,
    server: ServerId,
    max: usize,
) -> Vec<Lease> {
    for _ in 0..(owner.state.jobs.len().saturating_mul(3) + 4) {
        let leases = owner.grant_work(server, max);
        let pending = owner.persistence.in_flight() != 0
            || !owner.persistence_callbacks.is_empty()
            || !owner.io_callbacks.is_empty();
        pump(owner, messages).await;
        if !leases.is_empty() || !pending {
            return leases;
        }
    }
    panic!("work prerequisites did not reach a definitive fixture transition");
}

#[tokio::test]
async fn blocked_io_lane_preserves_actor_reads_and_noop_controls_without_faking_durable_pause() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    let mut checking = pending_job(1);
    checking.status = JobStatus::Paused;
    owner.state.jobs.push(checking);
    owner.state.jobs.push(pending_job(2));
    owner.queue_verification(JobId(1));
    let before = serde_json::to_value(owner.state.to_doc()).unwrap();
    let verification_before =
        serde_json::to_value(owner.verification[&JobId(1)].progress()).unwrap();
    let (started, started_rx) = oneshot::channel();
    let (release, held) = std::sync::mpsc::channel();
    let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let completion = completed.clone();
    owner.io_then(
        move || {
            let _ = started.send(());
            held.recv().expect("explicit IO release barrier");
            Ok(())
        },
        move |_, result| {
            result.expect("real blocked worker result");
            completion.store(true, std::sync::atomic::Ordering::Release);
        },
    );
    tokio::time::timeout(Duration::from_secs(1), started_rx)
        .await
        .unwrap()
        .unwrap();

    let (reply, snapshot) = oneshot::channel();
    owner.on_command(QueueCommand::RecoveryDocument { reply });
    let document = tokio::time::timeout(Duration::from_millis(100), snapshot)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(serde_json::to_value(document).unwrap(), before);
    let (reply, noop) = oneshot::channel();
    owner.on_command(QueueCommand::Pause {
        job: JobId(1),
        reply,
    });
    assert!(!tokio::time::timeout(Duration::from_millis(100), noop)
        .await
        .unwrap()
        .unwrap());
    assert_eq!(serde_json::to_value(owner.state.to_doc()).unwrap(), before);
    assert_eq!(
        serde_json::to_value(owner.verification[&JobId(1)].progress()).unwrap(),
        verification_before
    );
    assert!(!completed.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(
        owner.persistence.in_flight(),
        1,
        "the blocked actual worker retains its slot"
    );

    let (reply, mut pause) = oneshot::channel();
    owner.on_command(QueueCommand::Pause {
        job: JobId(2),
        reply,
    });
    assert!(
        matches!(pause.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
        "a valid persistent intent cannot acknowledge through a blocked durability lane"
    );
    assert_eq!(owner.state.job(JobId(2)).unwrap().status, JobStatus::Paused);
    assert_eq!(
        serde_json::to_value(owner.verification[&JobId(1)].progress()).unwrap(),
        verification_before
    );
    assert!(!completed.load(std::sync::atomic::Ordering::Acquire));

    release.send(()).unwrap();
    pump(&mut owner, &mut messages).await;
    assert!(pause.await.unwrap());
    assert!(completed.load(std::sync::atomic::Ordering::Acquire));
    let saved = SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    assert_eq!(
        saved
            .jobs
            .iter()
            .find(|job| job.id == JobId(2))
            .unwrap()
            .status,
        JobStatus::Paused
    );
    let verification_after = owner.verification[&JobId(1)].progress();
    assert_eq!(verification_after.reason, None);
    assert_eq!(verification_after.checked_bytes, 0);
    assert_eq!(verification_after.total_bytes, 0);
}

#[tokio::test]
async fn engine_shutdown_drains_real_writer_acknowledgement_behind_full_result_channel() {
    let (tmp, mut owner, adapter, mut messages) = control_test_owner();
    let mut job = pending_job(1);
    job.files[0].segments[0].size = 5;
    recompute_job_totals(&mut job);
    let file = job.files[0].id;
    owner.state.jobs.push(job);
    allocate_for_test(&mut owner, &mut messages, JobId(1), false).await;
    let directory = owner.allocation_cache[&JobId(1)].path.clone();
    let tracker = owner.tracker.clone();
    let writer = crate::writer::spawn_writer(
        &tracker,
        JobId(1),
        file,
        directory.clone(),
        "payload.bin".into(),
        owner.engine_tx.clone(),
    );
    owner.writers.insert(file, writer.clone());
    owner.writer_epochs.insert(file, writer.generation);
    // No owner is receiving yet, so this fills the real one-slot channel.
    owner
        .engine_tx
        .try_send(EngineMsg::ConnectFailed {
            server: ServerId(99),
        })
        .unwrap();
    assert_eq!(owner.engine_tx.capacity(), 0);
    writer
        .tx
        .send(WriteCmd::Segment {
            seg_number: 1,
            offset: 0,
            data: b"hello".to_vec(),
            crc: crc32fast::hash(b"hello"),
            file_size: 5,
            server: ServerId(1),
        })
        .await
        .unwrap();
    let partial = directory.join(format!(".runner-file-{}.part", file.0));
    tokio::time::timeout(Duration::from_secs(5), async {
        while std::fs::read(&partial).ok().as_deref() != Some(b"hello".as_slice()) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("real writer did not reach the acknowledgement barrier");
    writer.stop.cancel();
    assert!(
        !*writer.stopped.borrow(),
        "cancellation cannot stop a writer whose acknowledgement has no channel room"
    );
    assert_eq!(owner.engine_tx.capacity(), 0);

    let handle = crate::EngineHandle {
        artifacts: owner.artifacts.clone(),
        url_resolutions: owner.url_resolutions.clone(),
        url_dest_dir: owner.dest_dir.clone(),
        import_state_dir: owner.state_dir.clone(),
        url_history: owner.history.clone(),
        url_outbox_external: owner.persist_guard.is_some(),
        owner_persist_guard: owner.persist_guard.clone(),
        native_seed_recovery_cursor: Default::default(),
        hold_probes: Default::default(),
        recovery_cursor: Default::default(),
        cmd_tx: owner.engine_tx.clone(),
        shared: owner.shared.clone(),
        events: owner.events.clone(),
        cancel: owner.cancel.clone(),
        tracker: tracker.clone(),
        backend_adapter: Arc::new(std::sync::Mutex::new(Some(adapter))),
        budget_tracker: Arc::new(crate::pool::BudgetTracker::new(0)),
        url_fetches: Arc::new(std::sync::Mutex::new(HashSet::new())),
        post_scope: None,
        queue_changes: owner.epoch_tx.subscribe(),
        owner_heartbeat: owner.owner_heartbeat.clone(),
        shutdown_complete: owner.shutdown_complete.clone(),
    };
    // Enter the actual owner task already cancelled; its shutdown drain
    // must create channel room before it can wait for the writer.
    handle.cancel.cancel();
    tracker.spawn(owner.run(messages));
    tracker.close();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), handle.shutdown())
            .await
            .expect("shutdown deadlocked behind a final writer acknowledgement")
    );
    assert!(*writer.stopped.borrow());
    let saved = SnapshotStore::open(&tmp.path().join("state"))
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    assert!(
        matches!(saved.jobs[0].files[0].segments[0].state, SegmentState::Done { offset: 0, len: 5, crc } if crc == crc32fast::hash(b"hello"))
    );
    assert_eq!(std::fs::read(partial).unwrap(), b"hello");
    assert!(
        !tmp.path()
            .join("state/unclean.control-test")
            .try_exists()
            .unwrap(),
        "marker clears only after the accepted acknowledgement is durably captured"
    );
}

#[tokio::test]
async fn fresh_default_root_initializes_in_payload_worker_but_enrolled_missing_root_stays_missing()
{
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    // Unlike the shared fixture's configured-root default, this case
    // deliberately starts before any processing root has been created.
    std::fs::remove_dir(&owner.dest_dir).unwrap();
    assert!(
        !owner.dest_dir.exists(),
        "metadata recovery must not create the payload root inline"
    );
    pump(&mut owner, &mut messages).await;
    assert!(owner.dest_dir.is_dir());
    owner.state.jobs.push(pending_job(1));
    allocate_for_test(&mut owner, &mut messages, JobId(1), true).await;
    let allocation = owner.allocation_cache[&JobId(1)].clone();
    std::fs::remove_dir(&allocation.path).unwrap();
    std::fs::remove_dir(&allocation.root).unwrap();
    prepare_unenrolled_payload_root(&owner.artifacts, &allocation.root).unwrap();
    assert!(
        !allocation.root.exists(),
        "an absent enrolled mount cannot be recreated as an empty directory"
    );
}

#[tokio::test]
async fn fresh_admission_prepares_root_before_startup_callback_and_refuses_missing_enrolled_root() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    // Unlike the shared fixture's configured-root default, this case
    // deliberately starts before any processing root has been created.
    std::fs::remove_dir(&owner.dest_dir).unwrap();
    // Hold the completed state-lane enumeration before the owner can queue
    // root reconciliation. Immediate user admission must work in this order.
    let startup = tokio::time::timeout(Duration::from_secs(5), messages.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(!owner.startup_roots_ready);
    assert!(!owner.dest_dir.exists());
    owner.state.jobs.push(pending_job(1));
    let (reply, mut allocated) = oneshot::channel();
    owner.allocate_new_payload_then(JobId(1), move |_, result| {
        reply.send(result).unwrap();
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                result = &mut allocated => break result.unwrap().unwrap(),
                message = messages.recv() => owner.on_msg(message.unwrap()),
            }
        }
    })
    .await
    .expect("fresh admission must not depend on startup enumeration delivery");
    assert!(!owner.startup_roots_ready);
    let allocation = owner.allocation_cache[&JobId(1)].clone();
    assert_eq!(allocation.root, owner.dest_dir);
    assert!(allocation.path.is_dir());
    let bound = owner.artifacts.for_job(1).unwrap().unwrap();
    assert_eq!(bound.id, allocation.id);
    assert_eq!(bound.generation, allocation.generation);
    assert_eq!(bound.path, allocation.path);

    owner.on_msg(startup);
    pump(&mut owner, &mut messages).await;
    std::fs::remove_dir_all(&allocation.path).unwrap();
    std::fs::remove_dir(&allocation.root).unwrap();
    owner.state.jobs.push(pending_job(2));
    let (reply, refused) = oneshot::channel();
    owner.allocate_new_payload_then(JobId(2), move |_, result| {
        reply.send(result).unwrap();
    });
    pump(&mut owner, &mut messages).await;
    assert!(refused.await.unwrap().is_err());
    assert!(
        !allocation.root.exists(),
        "missing enrolled custody cannot become a fresh root"
    );
    let retained = owner.artifacts.for_job(1).unwrap().unwrap();
    assert_eq!(retained.path, allocation.path);
    assert_eq!(retained.generation, allocation.generation);
    assert_eq!(retained.root_identity, allocation.root_identity);
    assert!(owner.artifacts.for_job(2).unwrap().is_none());
    assert!(!owner.allocation_cache.contains_key(&JobId(2)));
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_boot_and_other_root_controls_do_not_wait_for_blocked_active_allocation() {
    use std::io::Write;
    let (tmp, mut owner, _adapter, mut messages) = control_test_owner();
    let state_dir = owner.state_dir.clone();
    let root_a = owner.dest_dir.clone();
    let root_b = tmp.path().join("healthy-payload");
    std::fs::create_dir_all(&root_a).unwrap();
    std::fs::create_dir_all(&root_b).unwrap();
    let mut first = pending_job(1);
    first.status = JobStatus::Paused;
    owner.state.jobs = vec![first, pending_job(2)];
    let first = owner
        .artifacts
        .allocate_for_delivery(1, &root_a, &root_a.join("job-1"), false, false)
        .unwrap();
    owner
        .artifacts
        .allocate_for_delivery(2, &root_b, &root_b.join("job-2"), false, false)
        .unwrap();
    owner.save_snapshot();
    pump(&mut owner, &mut messages).await;
    let marker = state_dir
        .join("artifact-identities")
        .join(&first.id)
        .join(format!("{}.json", first.generation));
    let original = std::fs::read(&marker).unwrap();
    owner.artifacts.close().unwrap();
    drop(owner);
    drop(messages);
    std::fs::remove_file(&marker).unwrap();
    assert!(std::process::Command::new("mkfifo")
        .arg(&marker)
        .status()
        .unwrap()
        .success());
    let (opened, reader_started) = oneshot::channel();
    let opening_path = marker.clone();
    let writer = std::thread::spawn(move || {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(opening_path)
            .unwrap();
        let _ = opened.send(file);
    });
    let config = crate::EngineConfig::single_node(
        Vec::new(),
        state_dir.clone(),
        root_a.clone(),
        Tuning::default(),
        None,
    );
    let starting = tokio::spawn(crate::Engine::spawn(config));
    let engine = tokio::time::timeout(Duration::from_secs(2), starting)
        .await
        .expect("payload I/O blocked healthy queue metadata construction")
        .unwrap()
        .unwrap();
    let blocked = tokio::time::timeout(Duration::from_secs(2), reader_started)
        .await
        .expect(
            "active-allocation capability worker did not reach the explicit sidecar read barrier",
        )
        .unwrap();
    writer.join().unwrap();
    // Restore the pathname before releasing the existing descriptor, so
    // later verification observes ordinary identity material rather than
    // accidentally creating a second FIFO barrier.
    struct Release {
        path: PathBuf,
        bytes: Vec<u8>,
        writer: Option<std::fs::File>,
    }
    impl Drop for Release {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::write(&self.path, &self.bytes);
            if let Some(mut writer) = self.writer.take() {
                let _ = writer.write_all(&self.bytes);
            }
        }
    }
    let release = Release {
        path: marker,
        bytes: original,
        writer: Some(blocked),
    };
    assert_eq!(
        engine.snapshot().jobs.len(),
        2,
        "metadata must retain both jobs while root A is blocked"
    );
    let document = tokio::time::timeout(Duration::from_secs(1), engine.recovery_document())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(document.jobs.len(), 2);
    let first = document.jobs.iter().find(|job| job.id == JobId(1)).unwrap();
    assert_eq!(first.status, JobStatus::Paused);
    assert!(matches!(
        first.files[0].segments[0].state,
        SegmentState::Pending
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if engine
                .snapshot()
                .jobs
                .iter()
                .find(|job| job.id == JobId(2))
                .is_some_and(|job| job.verification.is_none())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("root B capability was held behind unrelated root A");
    assert!(
        tokio::time::timeout(Duration::from_secs(1), engine.pause_job(JobId(2)))
            .await
            .unwrap()
            .unwrap(),
        "healthy-state durable metadata control cannot wait for root A"
    );
    assert!(
        release.writer.is_some(),
        "the barrier remains closed through every metadata/control assertion"
    );
    drop(release);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), engine.shutdown())
            .await
            .unwrap()
    );
}

fn guard_test_owner(
    tuning: Tuning,
) -> (
    tempfile::TempDir,
    Owner,
    watch::Receiver<u64>,
    mpsc::Receiver<EngineMsg>,
) {
    let (tmp, owner, epoch, _adapter, engine_messages) = guard_test_owner_with_backend(tuning);
    (tmp, owner, epoch, engine_messages)
}

fn guard_test_owner_with_backend(
    tuning: Tuning,
) -> (
    tempfile::TempDir,
    Owner,
    watch::Receiver<u64>,
    crate::backend::BackendAdapterPort,
    mpsc::Receiver<EngineMsg>,
) {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir(tmp.path().join("dest")).unwrap();
    let (budget_tx, _) = watch::channel(crate::pool::BudgetEnvelope::default());
    let (events, _) = broadcast::channel(1);
    let (epoch_tx, epoch_rx) = watch::channel(0);
    let (engine_tx, engine_messages) = mpsc::channel(1);
    let (backend, adapter) = crate::backend::backend_channel(1, 1);
    let owner = Owner::recover(
        &tmp.path().join("state"),
        None,
        None,
        tmp.path().join("dest"),
        Vec::new(),
        None,
        Arc::new(Vec::new()),
        tuning,
        true,
        false,
        "guard-test",
        None,
        budget_tx,
        crate::new_shared_snapshot(),
        events,
        epoch_tx,
        Arc::new(SpeedMeter::new()),
        Arc::new(RateLimiter::new(None)),
        None,
        None,
        backend,
        engine_tx,
        TaskTracker::new(),
        CancellationToken::new(),
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
    )
    .unwrap();
    (tmp, owner, epoch_rx, adapter, engine_messages)
}

pub(super) fn control_test_owner() -> (
    tempfile::TempDir,
    Owner,
    crate::backend::BackendAdapterPort,
    mpsc::Receiver<EngineMsg>,
) {
    control_test_owner_with_persistence(true, None)
}

#[tokio::test]
async fn authority_adoption_keeps_an_operator_rate_edit_queued_during_snapshot_io() {
    let (_temporary, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .save(&nzbd_state::QueueSnapshotDoc {
            speed_limit_bps: Some(4096),
            ..Default::default()
        })
        .unwrap();
    owner.persist = false;
    owner.state.speed_limit_bps = Some(777);
    let (started, started_rx) = oneshot::channel();
    let (release, held) = std::sync::mpsc::channel();
    owner.io_then(
        move || {
            let _ = started.send(());
            held.recv().expect("snapshot IO release barrier");
            Ok(())
        },
        |_, result| result.unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(1), started_rx)
        .await
        .unwrap()
        .unwrap();
    let (completed, result) = oneshot::channel();
    owner.adopt_authority_then(move |_, outcome| {
        let _ = completed.send(outcome);
    });
    let (reply, changed) = oneshot::channel();
    owner.on_command(QueueCommand::SetSpeedLimit {
        bytes_per_sec: Some(888),
        reply,
    });
    changed.await.unwrap();
    release.send(()).unwrap();
    pump(&mut owner, &mut messages).await;
    result.await.unwrap().unwrap();
    assert_eq!(owner.state.speed_limit_bps, Some(888));
    assert_eq!(
        SnapshotStore::open(&owner.state_dir)
            .unwrap()
            .load()
            .unwrap()
            .unwrap()
            .speed_limit_bps,
        Some(888)
    );
}

#[tokio::test]
async fn failed_authority_projection_persists_the_restored_or_newer_rate() {
    for newer in [None, Some(888)] {
        let (_temporary, mut owner, _adapter, mut messages) = control_test_owner();
        pump(&mut owner, &mut messages).await;
        SnapshotStore::open(&owner.state_dir)
            .unwrap()
            .save(&nzbd_state::QueueSnapshotDoc {
                speed_limit_bps: Some(4096),
                ..Default::default()
            })
            .unwrap();
        owner.persist = false;
        owner.state.speed_limit_bps = Some(777);
        // Make the first real snapshot write fail, then remove the obstacle
        // before its acknowledgement starts the real rollback snapshot.
        let blocked = owner.state_dir.join("queue.json.tmp");
        std::fs::create_dir(&blocked).unwrap();
        let (completed, result) = oneshot::channel();
        owner.adopt_authority_then(move |_, outcome| {
            let _ = completed.send(outcome);
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let message = messages.recv().await.expect("real authority worker result");
                if matches!(&message, EngineMsg::PersistenceFinished { result, .. } if result.snapshot && result.result.is_err()) {
                    std::fs::remove_dir(&blocked).unwrap();
                    if let Some(limit) = newer {
                        let (reply, changed) = oneshot::channel();
                        owner.on_command(QueueCommand::SetSpeedLimit { bytes_per_sec: Some(limit), reply });
                        changed.await.unwrap();
                    }
                    owner.on_msg(message);
                    break;
                }
                owner.on_msg(message);
            }
        }).await.expect("first authority snapshot must fail at the injected filesystem boundary");
        pump(&mut owner, &mut messages).await;
        assert!(result.await.unwrap().is_err());
        let expected = newer.or(Some(777));
        assert_eq!(owner.state.speed_limit_bps, expected);
        assert_eq!(
            SnapshotStore::open(&owner.state_dir)
                .unwrap()
                .load()
                .unwrap()
                .unwrap()
                .speed_limit_bps,
            expected
        );
    }
}

#[tokio::test]
async fn failed_post_rollback_retries_durably_before_reopening_admission() {
    let (_temporary, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let mut failed = pending_job(1);
    failed.files.clear();
    failed.status = JobStatus::Failed;
    failed.params.push(("*PP:failure-at".into(), "123".into()));
    owner.state.jobs.push(failed);
    let (reply, admitted) = oneshot::channel();
    owner.on_command(QueueCommand::BeginPostAdmission {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    let admission = admitted.await.unwrap().unwrap();
    let previous = owner.state.job(JobId(1)).unwrap().clone();
    let mut candidate = previous.clone();
    candidate.status = JobStatus::Completed;
    candidate.params.retain(|(key, _)| key != "*PP:failure-at");
    candidate
        .params
        .push((nzbd_types::PP_DONE_PARAM.into(), "1".into()));
    // Both the candidate snapshot and the immediate rollback really fail.
    let blocked = owner.state_dir.join("queue.json.tmp");
    std::fs::create_dir(&blocked).unwrap();
    let (completed, result) = oneshot::channel();
    owner.apply_post_record(admission.clone(), candidate, true, move |_, ok| {
        let _ = completed.send(ok);
    });
    pump(&mut owner, &mut messages).await;
    assert!(!result.await.unwrap());
    assert_eq!(owner.state.job(JobId(1)).unwrap().status, JobStatus::Failed);
    assert_eq!(owner.state.job(JobId(1)).unwrap().params, previous.params);
    assert!(owner.pending_control_commits.contains(&JobId(1)));
    assert!(owner.record_transitions.contains(&JobId(1)));
    let (reply, released) = oneshot::channel();
    owner.on_command(QueueCommand::EndPostAdmission { admission, reply });
    released.await.unwrap();
    owner.persist_post_rollbacks_then(|_| {});
    pump(&mut owner, &mut messages).await;
    assert!(owner.pending_control_commits.contains(&JobId(1)));
    let (reply, mut changed) = oneshot::channel();
    owner.on_command(QueueCommand::SetCategory {
        job: JobId(1),
        category: Some("operator-category".into()),
        reply,
    });
    assert!(matches!(
        changed.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    std::fs::remove_dir(&blocked).unwrap();
    // Retry uses the production owner tick. An enqueued write is still fenced
    // until its real persistence acknowledgment has reached the owner.
    owner.on_tick();
    let (reply, refused) = oneshot::channel();
    owner.on_command(QueueCommand::BeginPostAdmission {
        job: JobId(1),
        reply,
    });
    assert!(refused.await.unwrap().is_none());
    pump(&mut owner, &mut messages).await;
    assert!(changed.await.unwrap());
    assert!(!owner.pending_control_commits.contains(&JobId(1)));
    assert!(!owner.record_transitions.contains(&JobId(1)));
    assert!(owner.post_rollbacks.is_empty());
    let durable = SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    let durable = durable.jobs.iter().find(|job| job.id == JobId(1)).unwrap();
    assert_eq!(durable.status, JobStatus::Failed);
    assert_eq!(durable.params, previous.params);
    assert_eq!(durable.category.as_deref(), Some("operator-category"));
    let (reply, admitted) = oneshot::channel();
    owner.on_command(QueueCommand::BeginPostAdmission {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(admitted.await.unwrap().is_some());
}

#[tokio::test]
async fn explicit_authority_adoption_recovers_a_guard_fenced_post_rollback() {
    let authorized = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let guard = authorized.clone();
    let (_temporary, mut owner, _adapter, mut messages) = control_test_owner_with_persistence(
        true,
        Some(Arc::new(move || {
            guard.load(std::sync::atomic::Ordering::Acquire)
        })),
    );
    pump(&mut owner, &mut messages).await;
    let mut failed = pending_job(1);
    failed.files.clear();
    failed.status = JobStatus::Failed;
    failed.params.push(("*PP:failure-at".into(), "123".into()));
    owner.state.jobs.push(failed);
    let (reply, admitted) = oneshot::channel();
    owner.on_command(QueueCommand::BeginPostAdmission {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    let admission = admitted.await.unwrap().unwrap();
    let previous = owner.state.job(JobId(1)).unwrap().clone();
    let mut candidate = previous.clone();
    candidate
        .params
        .push((nzbd_types::PP_DONE_PARAM.into(), "1".into()));
    authorized.store(false, std::sync::atomic::Ordering::Release);
    let (completed, result) = oneshot::channel();
    owner.apply_post_record(admission.clone(), candidate, true, move |_, ok| {
        let _ = completed.send(ok);
    });
    pump(&mut owner, &mut messages).await;
    assert!(!result.await.unwrap());
    assert!(owner.persistence_fenced);
    assert!(!owner.persist);
    let (reply, released) = oneshot::channel();
    owner.on_command(QueueCommand::EndPostAdmission { admission, reply });
    released.await.unwrap();
    authorized.store(true, std::sync::atomic::Ordering::Release);
    // A valid guard alone cannot silently reopen the fenced persistence lane.
    owner.on_tick();
    assert!(owner.persistence_fenced && owner.pending_control_commits.contains(&JobId(1)));
    let (reply, adopted) = oneshot::channel();
    owner.on_command(QueueCommand::AdoptAuthority { reply });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !owner.projection_transition {
            owner.on_msg(
                messages
                    .recv()
                    .await
                    .expect("authority snapshot read result"),
            );
        }
    })
    .await
    .expect("explicit adoption must cross only the settled rollback barrier");
    let (reply, export) = oneshot::channel();
    owner.on_command(QueueCommand::ExportJob {
        job: JobId(1),
        reply,
    });
    assert!(export.await.unwrap().is_none());
    let (reply, refused) = oneshot::channel();
    owner.on_command(QueueCommand::BeginPostAdmission {
        job: JobId(1),
        reply,
    });
    assert!(refused.await.unwrap().is_none());
    assert!(owner.pending_control_commits.contains(&JobId(1)));
    let outstanding = owner.persistence_callbacks.len();
    owner.persist_post_rollbacks_then(|_| {});
    assert_eq!(owner.persistence_callbacks.len(), outstanding);
    pump(&mut owner, &mut messages).await;
    adopted.await.unwrap().unwrap();
    assert!(!owner.persistence_fenced && owner.persist);
    assert!(owner.post_rollbacks.is_empty());
    assert!(!owner.pending_control_commits.contains(&JobId(1)));
    let durable = SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    assert_eq!(durable.jobs[0].status, JobStatus::Failed);
    assert_eq!(durable.jobs[0].params, previous.params);
    let (reply, admitted) = oneshot::channel();
    owner.on_command(QueueCommand::BeginPostAdmission {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(admitted.await.unwrap().is_some());
}

#[tokio::test]
async fn stale_worker_retirement_cannot_cancel_a_successors_custody() {
    let (_temporary, mut owner, _adapter, mut messages) =
        control_test_owner_with_persistence(false, None);
    pump(&mut owner, &mut messages).await;
    let old = nzbd_state::artifacts::WorkerControlContext {
        lease_id: "old-grant".into(),
        job_incarnation: "incarnation".into(),
        job_revision: 1,
        control_revision: 1,
        epoch: 7,
    };
    let mut current = old.clone();
    current.lease_id = "successor-grant".into();
    current.job_revision = 2;
    let mut job = pending_job(1);
    job.files.clear();
    job.params.push((
        nzbd_state::artifacts::WORKER_CONTROL_CONTEXT_PARAM.into(),
        serde_json::to_string(&current).unwrap(),
    ));
    owner.state.jobs.push(job);
    owner.job_epochs.insert(JobId(1), 17);
    owner.control_epochs.insert(JobId(1), 23);
    let custody = CancellationToken::new();
    owner.post_admissions.insert(JobId(1), 17);
    owner.post_cancellations.insert(JobId(1), custody.clone());
    for retirement in [
        WorkerRetirement::Revoked(old.clone()),
        WorkerRetirement::Accepted(old),
    ] {
        let (reply, rejected) = oneshot::channel();
        owner.on_command(QueueCommand::RemoveJobSilent {
            job: JobId(1),
            retirement: Some(retirement),
            reply,
        });
        assert!(!tokio::time::timeout(Duration::from_secs(1), rejected)
            .await
            .expect("stale retirement must reject before waiting for successor custody")
            .unwrap());
        assert!(!custody.is_cancelled());
        assert_eq!(owner.control_epochs.get(&JobId(1)), Some(&23));
        assert!(owner.deferred_replacements.is_empty());
        assert!(owner.retiring_writers.is_empty());
        assert_eq!(
            nzbd_state::artifacts::WorkerControlContext::for_job(
                owner.state.job(JobId(1)).unwrap()
            ),
            Some(current.clone())
        );
    }
}

#[tokio::test]
async fn deferred_worker_retirement_rechecks_context_after_successor_import() {
    let (_temporary, mut owner, _adapter, mut messages) =
        control_test_owner_with_persistence(false, None);
    pump(&mut owner, &mut messages).await;
    let old = nzbd_state::artifacts::WorkerControlContext {
        lease_id: "old-grant".into(),
        job_incarnation: "incarnation".into(),
        job_revision: 1,
        control_revision: 1,
        epoch: 7,
    };
    let mut job = pending_job(1);
    job.files.clear();
    job.status = JobStatus::Completed;
    job.params.push((
        nzbd_state::artifacts::WORKER_CONTROL_CONTEXT_PARAM.into(),
        serde_json::to_string(&old).unwrap(),
    ));
    owner.state.jobs.push(job.clone());
    allocate_for_test(&mut owner, &mut messages, JobId(1), true).await;
    owner.artifacts.bind_worker_context(1, &old).unwrap();
    let (reply, admitted) = oneshot::channel();
    owner.on_command(QueueCommand::BeginPostAdmission {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    let admission = admitted.await.unwrap().unwrap();
    let mut successor = old.clone();
    successor.lease_id = "successor-grant".into();
    successor.job_revision += 1;
    job.params
        .retain(|(key, _)| key != nzbd_state::artifacts::WORKER_CONTROL_CONTEXT_PARAM);
    job.params.push((
        nzbd_state::artifacts::WORKER_CONTROL_CONTEXT_PARAM.into(),
        serde_json::to_string(&successor).unwrap(),
    ));
    let (reply, imported) = oneshot::channel();
    owner.on_command(QueueCommand::ImportJob {
        job: Box::new(job),
        preparation: None,
        authority: crate::imports::ImportAuthority(Arc::new(|| true)),
        emit_finished: false,
        reply,
    });
    let (reply, retired) = oneshot::channel();
    owner.on_command(QueueCommand::RemoveJobSilent {
        job: JobId(1),
        retirement: Some(WorkerRetirement::Revoked(old)),
        reply,
    });
    assert_eq!(owner.deferred_replacements.len(), 2);
    let (reply, ended) = oneshot::channel();
    owner.on_command(QueueCommand::EndPostAdmission { admission, reply });
    ended.await.unwrap();
    assert!(
        !tokio::time::timeout(Duration::from_secs(1), retired)
            .await
            .expect(
                "deferred stale retirement must reject without waiting for successor persistence"
            )
            .unwrap(),
        "deferred old retirement must reject before waiting on successor persistence"
    );
    pump(&mut owner, &mut messages).await;
    imported.await.unwrap().unwrap();
    assert_eq!(
        nzbd_state::artifacts::WorkerControlContext::for_job(owner.state.job(JobId(1)).unwrap()),
        Some(successor)
    );
    assert!(owner.retirement_epochs.is_empty());
}

#[tokio::test]
async fn revoked_worker_retirement_preserves_journal_for_the_next_grant() {
    let temporary = tempfile::tempdir().unwrap();
    let state = temporary.path().join("state");
    let destination = temporary.path().join("dest");
    std::fs::create_dir(&destination).unwrap();
    let mut config = crate::EngineConfig::single_node(
        Vec::new(),
        state.clone(),
        destination,
        Tuning::default(),
        None,
    );
    config.persist_queue = false;
    let engine = crate::Engine::spawn(config).await.unwrap();
    let context = nzbd_state::artifacts::WorkerControlContext {
        lease_id: "expired-grant".into(),
        job_incarnation: "incarnation".into(),
        job_revision: 1,
        control_revision: 1,
        epoch: 7,
    };
    let mut job = pending_job(1);
    job.params.push((
        nzbd_state::artifacts::WORKER_CONTROL_CONTEXT_PARAM.into(),
        serde_json::to_string(&context).unwrap(),
    ));
    engine.import_job(job.clone(), false, false).await.unwrap();
    export_job_ready_for_test(&engine, job.id)
        .await
        .unwrap()
        .unwrap();
    let source = engine.artifacts().for_job(1).unwrap().unwrap();
    std::fs::write(source.path.join(".runner-file-10.part"), [7; 100]).unwrap();
    let record = JournalRecord {
        job: job.id,
        file: FileId(10),
        segment_number: 1,
        offset: 0,
        len: 100,
        crc32: crc32fast::hash(&[7; 100]),
        file_size: 100,
    };
    let mut journal = JobJournals::open(&state, "expired-worker").unwrap();
    journal.append(&record).unwrap();
    journal.sync().unwrap();
    drop(journal);
    assert!(engine
        .retire_worker_job(job.id, context.clone())
        .await
        .unwrap());
    let mut successor = context;
    successor.lease_id = "successor-grant".into();
    successor.job_revision += 1;
    job.params
        .retain(|(key, _)| key != nzbd_state::artifacts::WORKER_CONTROL_CONTEXT_PARAM);
    job.params.push((
        nzbd_state::artifacts::WORKER_CONTROL_CONTEXT_PARAM.into(),
        serde_json::to_string(&successor).unwrap(),
    ));
    // Import must cross the real durable retirement barrier, then earn Done
    // again by folding and checking the retained journal against actual bytes.
    engine.import_job(job.clone(), true, false).await.unwrap();
    let resumed = export_job_ready_for_test(&engine, job.id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        resumed.files[0].segments[0].state,
        SegmentState::Done { len: 100, .. }
    ));
    assert_eq!(
        nzbd_state::artifacts::WorkerControlContext::for_job(&resumed),
        Some(successor)
    );
    assert!(engine.shutdown().await);
}

#[tokio::test]
async fn failed_cleanup_scope_grants_only_exact_failure_metadata() {
    let (_temporary, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let mut job = pending_job(1);
    job.status = JobStatus::Failed;
    job.params.extend([
        ("*PP:failure-at".into(), "123".into()),
        ("*PP:failure-disposition-attempts".into(), "1".into()),
    ]);
    job.files[0].segments[0].state = SegmentState::Done {
        offset: 0,
        len: 100,
        crc: 7,
    };
    owner.state.jobs.push(job);
    allocate_for_test(&mut owner, &mut messages, JobId(1), true).await;
    let source = owner.artifacts.for_job(1).unwrap().unwrap();
    // A partially deleted source cannot substantiate these retained Done bits.
    std::fs::write(source.path.join("payload.bin"), b"partial").unwrap();
    let source = owner
        .artifacts
        .finish(1, &source.path, &source.root, "retained")
        .unwrap();
    owner
        .artifacts
        .request_delete(
            &source.id,
            source.revision,
            &format!("failed-delete-{}", source.id),
            0,
        )
        .unwrap();
    owner.queue_verification(JobId(1));
    let (reply, normal) = oneshot::channel();
    owner.on_command(QueueCommand::BeginPostAdmission {
        job: JobId(1),
        reply,
    });
    assert!(normal.await.unwrap().is_none());
    owner.pending_writer_birth.insert(FileId(10), 99);
    let (reply, blocked) = oneshot::channel();
    owner.on_command(QueueCommand::BeginFailedCleanupAdmission {
        job: JobId(1),
        reply,
    });
    assert!(blocked.await.unwrap().is_none());
    owner.pending_writer_birth.remove(&FileId(10));
    owner.writer_epochs.insert(FileId(10), 99);
    let (reply, cleanup) = oneshot::channel();
    owner.on_command(QueueCommand::BeginFailedCleanupAdmission {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    let cleanup = cleanup.await.unwrap().unwrap();
    assert!(!owner.writer_epochs.contains_key(&FileId(10)));
    owner.on_msg(EngineMsg::ScopedWriter {
        job: JobId(1),
        file: FileId(10),
        generation: 99,
        fact: WriterFact::Finalized {
            ok: true,
            final_path: Some(source.path.join("payload.bin")),
            combined_crc: Some(7),
            identity: None,
            naming: None,
        },
    });
    assert!(
        !owner.state.job(JobId(1)).unwrap().files[0].finalized,
        "delayed writer completion cannot publish source facts through cleanup scope"
    );
    assert_eq!(
        cleanup.cleanup.operation_id(),
        format!("failed-delete-{}", source.id)
    );
    let admission = cleanup.admission;
    let (reply, exported) = oneshot::channel();
    owner.on_msg(EngineMsg::ScopedCommand {
        admission: admission.clone(),
        command: Box::new(QueueCommand::ExportJob {
            job: JobId(1),
            reply,
        }),
    });
    assert!(exported.await.unwrap().is_none());
    let (reply, metadata) = oneshot::channel();
    owner.on_msg(EngineMsg::ScopedCommand {
        admission: admission.clone(),
        command: Box::new(QueueCommand::FailedCleanupMetadata {
            admission: admission.clone(),
            reply,
        }),
    });
    let original = metadata.await.unwrap().unwrap();
    assert!(matches!(
        original.files[0].segments[0].state,
        SegmentState::Done { len: 100, .. }
    ));
    for mutation in 0..7 {
        let mut candidate = original.clone();
        match mutation {
            0 => candidate.status = JobStatus::Completed,
            1 => candidate.files.clear(),
            2 => candidate.params.push(("*File:size:10".into(), "7".into())),
            3 => candidate
                .params
                .push((nzbd_types::PP_DONE_PARAM.into(), "SUCCESS".into())),
            4 => candidate
                .params
                .push(("Artifact:Id".into(), "another-source".into())),
            5 => candidate
                .params
                .push(("Artifact:Custody".into(), "deleted_by_runner".into())),
            _ => {
                candidate
                    .params
                    .iter_mut()
                    .find(|(key, _)| key == "*PP:failure-disposition-attempts")
                    .unwrap()
                    .1 = "0".into()
            }
        }
        let (completed, result) = oneshot::channel();
        owner.apply_post_record(admission.clone(), candidate, true, move |_, ok| {
            let _ = completed.send(ok);
        });
        pump(&mut owner, &mut messages).await;
        assert!(
            !result.await.unwrap(),
            "cleanup admission accepted mutation {mutation}"
        );
    }
    let (reply, stage) = oneshot::channel();
    owner.on_msg(EngineMsg::ScopedCommand {
        admission: admission.clone(),
        command: Box::new(QueueCommand::SetJobStatus {
            job: JobId(1),
            status: JobStatus::Completed,
            reply,
        }),
    });
    assert!(
        stage.await.is_err(),
        "cleanup cannot enter normal PP or download states"
    );
    let mut candidate = original.clone();
    let mut other = pending_job(2);
    other.files.clear();
    other.status = JobStatus::Completed;
    owner.state.jobs.push(other);
    candidate.params.extend([
        ("Artifact:Id".into(), source.id.clone()),
        ("Artifact:Generation".into(), source.generation.clone()),
        ("Artifact:Custody".into(), "runner_owned".into()),
        ("*PP:history-seq".into(), "42".into()),
        (nzbd_types::PP_DONE_PARAM.into(), "FAILURE/HEALTH".into()),
    ]);
    let (completed, result) = oneshot::channel();
    owner.apply_post_record(admission.clone(), candidate, true, move |_, ok| {
        let _ = completed.send(ok);
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    assert!(owner.verification.contains_key(&JobId(1)));
    assert_eq!(owner.state.job(JobId(1)).unwrap().status, JobStatus::Failed);
    assert_eq!(
        std::fs::read(source.path.join("payload.bin")).unwrap(),
        b"partial"
    );
    let durable = SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    assert!(matches!(
        durable.jobs[0].files[0].segments[0].state,
        SegmentState::Done { len: 100, .. }
    ));
    assert!(durable.jobs[0]
        .params
        .iter()
        .any(|(key, value)| key == nzbd_types::PP_DONE_PARAM && value == "FAILURE/HEALTH"));
    let (reply, ended) = oneshot::channel();
    owner.on_command(QueueCommand::EndPostAdmission { admission, reply });
    ended.await.unwrap();
    assert!(owner.failed_cleanup_admissions.is_empty());
    let state = owner.state_dir.clone();
    let destination = owner.dest_dir.clone();
    drop(owner);
    let restarted = crate::Engine::spawn(crate::EngineConfig::single_node(
        Vec::new(),
        state,
        destination,
        Tuning::default(),
        None,
    ))
    .await
    .unwrap();
    // A stamp-to-removal crash cannot turn preserved, unverified Done data
    // into a generic verified export when PP_DONE suppresses payload recovery.
    assert!(matches!(
        restarted.export_job(JobId(1)).await,
        Err(crate::EngineError::Lifecycle(_))
    ));
    assert!(restarted
        .begin_post_admission(JobId(1))
        .await
        .unwrap()
        .is_none());
    let replay = restarted
        .begin_failed_cleanup_admission(JobId(1))
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(
            restarted
                .for_post_admission(&replay.admission)
                .export_job(JobId(1))
                .await,
            Err(crate::EngineError::Lifecycle(_))
        ),
        "cleanup-scoped export reports its fence without reading the whole recovery document"
    );
    assert!(restarted.export_job(JobId(2)).await.unwrap().is_some());
    assert!(
        matches!(
            restarted
                .for_post_admission(&replay.admission)
                .export_job(JobId(2))
                .await,
            Err(crate::EngineError::Lifecycle(_))
        ),
        "cleanup scope cannot export another job's verified payload record"
    );
    let metadata = restarted
        .for_post_admission(&replay.admission)
        .failed_cleanup_metadata()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metadata.status, JobStatus::Failed);
    assert!(matches!(
        metadata.files[0].segments[0].state,
        SegmentState::Done { len: 100, .. }
    ));
    assert!(metadata
        .params
        .iter()
        .any(|(key, value)| key == nzbd_types::PP_DONE_PARAM && value == "FAILURE/HEALTH"));
    restarted
        .end_post_admission(replay.admission)
        .await
        .unwrap();
    restarted.shutdown().await;
}

#[tokio::test]
async fn quiescing_without_writer_handles_releases_its_fence_after_durable_ack() {
    let (_temporary, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let mut job = pending_job(1);
    job.files.clear();
    job.status = JobStatus::Completed;
    owner.state.jobs.push(job);
    allocate_for_test(&mut owner, &mut messages, JobId(1), true).await;
    let source = owner.artifacts.for_job(1).unwrap().unwrap();
    std::fs::write(source.path.join("retained"), b"owned bytes").unwrap();
    let (reply, quiesced) = oneshot::channel();
    owner.on_command(QueueCommand::QuiescePayload {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(quiesced.await.unwrap().unwrap().is_empty());
    assert!(!owner.retiring_writers.contains_key(&JobId(1)));
    let (reply, deleted) = oneshot::channel();
    owner.on_command(QueueCommand::Delete {
        job: JobId(1),
        delete_files: false,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(deleted.await.unwrap());
    assert!(owner.state.job(JobId(1)).is_none());
    assert_eq!(
        std::fs::read(source.path.join("retained")).unwrap(),
        b"owned bytes"
    );
}

#[tokio::test]
async fn lifecycle_cohort_is_stamped_at_admission_not_later_url_allocation() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let old = owner.state.admit_url(
        "old-fetching".into(),
        "http://example.test/old.nzb",
        None,
        0,
    );
    allocate_for_test(&mut owner, &mut messages, old, true).await;
    let old_row = owner.state.job(old).unwrap();
    assert!(!old_row.lifecycle_eligible());
    assert!(
        !owner
            .artifacts
            .for_job(old.0)
            .unwrap()
            .unwrap()
            .automatic_cleanup_eligible
    );

    let new = owner.state.admit_url(
        "new-fetching".into(),
        "http://example.test/new.nzb",
        None,
        0,
    );
    mark_new_lifecycle_cohort(owner.state.job_mut(new).unwrap());
    allocate_for_test(&mut owner, &mut messages, new, true).await;
    let new_row = owner.state.job(new).unwrap();
    assert!(new_row.lifecycle_eligible());
    assert!(
        owner
            .artifacts
            .for_job(new.0)
            .unwrap()
            .unwrap()
            .automatic_cleanup_eligible
    );

    let mut torrent = new_row.clone();
    torrent.kind = nzbd_types::JobKind::Torrent;
    torrent
        .params
        .retain(|(key, _)| key != "*Runner:LifecycleEligible");
    assert!(
        !torrent.lifecycle_eligible(),
        "torrent seeding sources stay outside the NZB cohort"
    );
}

fn control_test_owner_with_persistence(
    persist: bool,
    persist_guard: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
) -> (
    tempfile::TempDir,
    Owner,
    crate::backend::BackendAdapterPort,
    mpsc::Receiver<EngineMsg>,
) {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir(tmp.path().join("dest")).unwrap();
    let (budget_tx, _) = watch::channel(crate::pool::BudgetEnvelope::default());
    let (events, _) = broadcast::channel(1);
    let (epoch_tx, _) = watch::channel(0);
    let (engine_tx, engine_messages) = mpsc::channel(1);
    let (backend, adapter) = crate::backend::backend_channel(1, 1);
    let owner = Owner::recover(
        &tmp.path().join("state"),
        None,
        None,
        tmp.path().join("dest"),
        Vec::new(),
        None,
        Arc::new(Vec::new()),
        Tuning::default(),
        true,
        persist,
        "control-test",
        persist_guard,
        budget_tx,
        crate::new_shared_snapshot(),
        events,
        epoch_tx,
        Arc::new(SpeedMeter::new()),
        Arc::new(RateLimiter::new(None)),
        None,
        None,
        backend,
        engine_tx,
        TaskTracker::new(),
        CancellationToken::new(),
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
    )
    .unwrap();
    (tmp, owner, adapter, engine_messages)
}

#[tokio::test]
async fn keep_on_failure_is_opt_in_and_snapshot_committed_before_ack() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(pending_job(1));
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::SetKeepOnFailure {
        job: JobId(1),
        keep: true,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    let saved = SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    assert!(saved.jobs[0]
        .params
        .iter()
        .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1"));

    // A preallocation opt-in must be copied atomically into the first
    // source generation, then consulted by the real failed-payload
    // disposal path. The queue snapshot remains the authority only until
    // that generation exists.
    allocate_for_test(&mut owner, &mut messages, JobId(1), true).await;
    let source = owner.artifacts.for_job(1).unwrap().unwrap();
    assert!(source.keep_on_failure);
    std::fs::write(source.path.join("partial.bin"), b"failed partial payload").unwrap();
    owner
        .artifacts
        .finish(1, &source.path, &source.root, "active")
        .unwrap();
    let attempt = owner.artifacts.acquire_attempt_use(1).unwrap().unwrap();
    assert!(owner
        .artifacts
        .delete_failed_payload(&attempt, 1, &|| Ok(()))
        .is_err());
    assert!(source.path.join("partial.bin").exists());

    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::SetKeepOnFailure {
        job: JobId(1),
        keep: false,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    let saved = SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    assert!(!saved.jobs[0]
        .params
        .iter()
        .any(|(key, _)| key == "*Runner:KeepOnFailure"));
    assert!(owner
        .artifacts
        .delete_failed_payload(&attempt, 1, &|| Ok(()))
        .is_ok());
}

#[tokio::test]
async fn terminal_failed_job_can_keep_only_its_retained_delivery_source() {
    let (tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let mut job = pending_job(57);
    job.status = JobStatus::Failed;
    owner.state.jobs.push(job);

    let source = owner.dest_dir.join("failed-source-57");
    owner
        .artifacts
        .allocate_for_delivery(57, &owner.dest_dir, &source, true, true)
        .unwrap();
    std::fs::write(source.join("episode.mkv"), b"verified payload").unwrap();
    owner
        .artifacts
        .finish(57, &source, &owner.dest_dir, "active")
        .unwrap();
    let selected = owner
        .artifacts
        .for_job(57)
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .filter(|entry| entry.path == "episode.mkv")
        .collect::<Vec<_>>();
    let completed = tmp.path().join("completed");
    std::fs::create_dir(&completed).unwrap();
    owner
        .artifacts
        .publish_selected_for_delivery_checked(
            57,
            &completed.join("published-generation"),
            selected,
            vec![nzbd_state::artifacts::DeliveryFileOutcome {
                source_path: "episode.mkv".into(),
                publication_path: Some("episode.mkv".into()),
                classification: "video".into(),
                outcome: "published".into(),
                reason: Some("verified complete media".into()),
                repair_dependency: false,
                source_cleanup_status: "pending".into(),
            }],
            &|| Ok(()),
        )
        .unwrap();
    let delivery = owner
        .artifacts
        .create_delivery_with_outcome_and_keep(
            57,
            true,
            Some("keep-terminal-source-test"),
            "FAILURE/HEALTH",
            Some("health"),
            Some("partial salvage"),
            false,
        )
        .unwrap();
    let source_id = delivery.source_artifact_id.clone();
    let source_generation = delivery.source_generation.clone();
    owner
        .state
        .url_admission_receipts
        .push(UrlAdmissionReceipt {
            protocol_version: 1,
            request_token: "keep-source-tombstone-57".into(),
            submission_sha256: "submission-hash".into(),
            job_id: 57,
            generation: "admission-generation".into(),
            runner_instance_id: "runner-instance".into(),
            terminal_outcome: Some("FAILURE/HEALTH".into()),
            outcome: "failed".into(),
            failure_stage: Some("health".into()),
            failure_cause: Some("partial salvage".into()),
            custody_state: "runner_owned".into(),
            keep_source_on_failure: false,
            source_cleanup_status: Some("pending".into()),
            publication_path: Some(completed.join("published-generation").display().to_string()),
            source_artifact_id: Some(source_id.clone()),
            source_generation: Some(source_generation.clone()),
            failure_reason: None,
            fetch_cause: None,
            blames_release: None,
            salvage: None,
        });

    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::SetKeepOnFailure {
        job: JobId(57),
        keep: true,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    assert!(owner.artifacts.get(&source_id).unwrap().keep_on_failure);
    assert!(owner.state.url_admission_receipts[0].keep_source_on_failure);
    assert_eq!(
        owner.artifacts.get(&delivery.artifact_id).unwrap().state,
        "transferred"
    );

    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::SetKeepOnFailure {
        job: JobId(57),
        keep: false,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    assert!(!owner.artifacts.get(&source_id).unwrap().keep_on_failure);
    assert!(!owner.state.url_admission_receipts[0].keep_source_on_failure);

    // Once the visible queue/history row is gone, the durable source
    // generation still refreshes the receipt, and stale queue params no
    // longer overwrite its live Keep preference.
    owner.state.jobs.clear();
    owner.artifacts.set_keep_on_failure(57, true).unwrap();
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::GetUrlAdmission {
        request_token: "keep-source-tombstone-57".into(),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap().unwrap().keep_source_on_failure);
    owner.artifacts.set_keep_on_failure(57, false).unwrap();
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::GetUrlAdmission {
        request_token: "keep-source-tombstone-57".into(),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(!result.await.unwrap().unwrap().keep_source_on_failure);
    assert!(
        !owner
            .artifacts
            .delivery(&delivery.delivery_id)
            .unwrap()
            .keep_source_on_failure,
        "the original Delivery choice remains immutable"
    );
}

#[tokio::test]
async fn keep_request_does_not_succeed_after_source_generation_is_deleted() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let mut job = pending_job(63);
    job.status = JobStatus::Failed;
    owner.state.jobs.push(job);
    let source = owner.dest_dir.join("deleted-source-63");
    owner
        .artifacts
        .allocate_for_delivery(63, &owner.dest_dir, &source, true, false)
        .unwrap();
    std::fs::write(source.join("payload.mkv"), b"failed payload").unwrap();
    owner
        .artifacts
        .finish(63, &source, &owner.dest_dir, "parked_failed")
        .unwrap();
    let artifact = owner.artifacts.for_job(63).unwrap().unwrap();
    let deletion = owner
        .artifacts
        .request_delete(&artifact.id, artifact.revision, "delete-source-63", 0)
        .unwrap();
    owner.artifacts.execute_delete(&deletion.id).unwrap();

    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::SetKeepOnFailure {
        job: JobId(63),
        keep: true,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(!result.await.unwrap());
    assert!(!owner.state.jobs[0]
        .params
        .iter()
        .any(|(key, _)| key == "*Runner:KeepOnFailure"));
}

#[tokio::test]
async fn intact_publication_clears_previous_private_repair_marker() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let mut job = pending_job(1);
    let file = job.files[0].id;
    job.params
        .push((format!("*File:repair:{}", file.0), "5".into()));
    job.files[0].segments[0].state = SegmentState::Done {
        offset: 0,
        len: 5,
        crc: crc32fast::hash(b"hello"),
    };
    owner.state.jobs.push(job);
    owner.file_sizes.insert(file, 5);
    let (tx, mut rx) = mpsc::channel(8);
    let (_done, stopped) = watch::channel(false);
    owner.writers.insert(
        file,
        WriterHandle {
            tx,
            stop: CancellationToken::new(),
            stopped,
            generation: 0,
        },
    );
    owner.send_finalize(JobId(1), file);
    pump(&mut owner, &mut messages).await;
    assert!(!owner.state.job(JobId(1)).unwrap().file_needs_repair(file));
    assert!(matches!(
        rx.try_recv().unwrap(),
        WriteCmd::PublicationName(_)
    ));
    assert!(matches!(
        rx.try_recv().unwrap(),
        WriteCmd::Finalize {
            combined_crc: Some(_),
            ..
        }
    ));
}

#[tokio::test]
async fn yenc_name_variations_keep_the_first_name_but_size_and_path_conflicts_fail_identity() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    for id in 1..=3 {
        owner.state.jobs.push(pending_job(id));
    }
    let file = owner.state.job(JobId(1)).unwrap().files[0].id;
    owner.accept_file_metadata(JobId(1), file, "first.bin", 100);
    pump(&mut owner, &mut messages).await;
    owner.accept_file_metadata(JobId(1), file, "different-obfuscated-name", 100);
    pump(&mut owner, &mut messages).await;
    assert!(!owner.state.job(JobId(1)).unwrap().held());
    assert_eq!(
        owner.state.job(JobId(1)).unwrap().files[0].filename,
        "first.bin"
    );
    owner.accept_file_metadata(JobId(1), file, "third-name", 101);
    // Contract §2.2: self-contradicting yEnc metadata is the release's
    // fault — a terminal `failed/identity`, blamed, never salvaged.
    let terminal_of = |owner: &Owner, id: u32| owner.artifacts.terminal(id).unwrap();
    for _ in 0..20 {
        pump(&mut owner, &mut messages).await;
        if terminal_of(&owner, 1).is_some_and(|t| t.emitted) {
            break;
        }
    }
    let terminal = terminal_of(&owner, 1).unwrap();
    assert_eq!(terminal.reason, Some(nzbd_types::FailureReason::Identity));
    assert_eq!(
        terminal.message.as_deref(),
        Some("conflicting yEnc declared size")
    );
    assert!(terminal.blames_release);
    assert_eq!(terminal.salvage, nzbd_state::artifacts::SalvageState::None);
    assert_eq!(owner.state.job(JobId(1)).unwrap().status, JobStatus::Failed);

    for (id, name, size) in [(2, "../unsafe", 100), (3, "empty.bin", 0)] {
        let file = owner.state.job(JobId(id)).unwrap().files[0].id;
        owner.accept_file_metadata(JobId(id), file, name, size);
        for _ in 0..20 {
            pump(&mut owner, &mut messages).await;
            if terminal_of(&owner, id).is_some_and(|t| t.emitted) {
                break;
            }
        }
        assert_eq!(
            terminal_of(&owner, id).unwrap().reason,
            Some(nzbd_types::FailureReason::Identity)
        );
    }
}

#[tokio::test]
async fn payload_retirement_retries_keep_original_acknowledgements_and_fence_resume() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    std::fs::create_dir_all(&owner.dest_dir).unwrap();
    let job = pending_job(1);
    let file = job.files[0].id;
    owner.state.jobs.push(job);
    allocate_for_test(&mut owner, &mut messages, JobId(1), false).await;
    let (tx, _rx) = mpsc::channel(1);
    let (done, stopped) = watch::channel(false);
    owner.writers.insert(
        file,
        WriterHandle {
            tx,
            stop: CancellationToken::new(),
            stopped,
            generation: 0,
        },
    );
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::QuiescePayload {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    let first = rx.await.unwrap().unwrap();
    assert_eq!(first.len(), 1);
    assert!(!*first[0].stopped.borrow());
    drop(first); // the timed-out HTTP request loses its receivers
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::Resume {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(!rx.await.unwrap());
    assert!(owner.ensure_allocation(JobId(1), false).is_err());
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::QuiescePayload {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    let mut retry = rx.await.unwrap().unwrap();
    assert_eq!(retry.len(), 1);
    assert!(!*retry[0].stopped.borrow());
    done.send(true).unwrap();
    retry[0].stopped.wait_for(|v| *v).await.unwrap();
    assert_eq!(
        owner.artifacts.for_job(1).unwrap().unwrap().state,
        "retiring"
    );
}

fn control_torrent_job() -> Job {
    let mut job = bare_job();
    job.kind = JobKind::Torrent;
    job.status = JobStatus::Queued;
    job.torrent = Some(nzbd_types::TorrentRecord {
        info_hash_v1: "0123456789abcdef0123456789abcdef01234567".into(),
        source: nzbd_types::TorrentSource::Metainfo,
        metadata_file: "meta/control.torrent".into(),
        payload_root: PathBuf::new(),
        phase: nzbd_types::TorrentPhase::Queued,
        control_intent: TorrentControlIntent::Running,
        removal_intent: None,
        removal_outcome: None,
        removal_confirmed_at_unix: None,
        stop_reason: None,
        files: Vec::new(),
        total_bytes: 1,
        selected_bytes: 1,
        downloaded_bytes: 0,
        uploaded_bytes: 0,
        seeding_seconds: 0,
        ready_at_unix: None,
        content_path: None,
        seed_policy: Default::default(),
        last_activity_unix: None,
        last_error: None,
    });
    job
}

#[tokio::test]
async fn torrent_snapshot_reports_payload_progress_and_live_rates() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let mut job = control_torrent_job();
    job.status = JobStatus::Downloading;
    let id = job.id;
    let torrent = job.torrent.as_mut().unwrap();
    torrent.total_bytes = 8192;
    torrent.selected_bytes = 4096;
    torrent.downloaded_bytes = 1024;
    torrent.files = vec![
        nzbd_types::TorrentFileRecord {
            path: "first.iso".into(),
            length: 1024,
            selected: true,
            downloaded_bytes: 1024,
        },
        nzbd_types::TorrentFileRecord {
            path: "second.iso".into(),
            length: 3072,
            selected: true,
            downloaded_bytes: 0,
        },
        nzbd_types::TorrentFileRecord {
            path: "unselected.iso".into(),
            length: 4096,
            selected: false,
            downloaded_bytes: 0,
        },
    ];
    owner.state.jobs.push(job);
    owner.torrent_progress.insert(
        id,
        crate::backend::TransferProgress {
            download_bps: 512,
            upload_bps: 128,
            useful_peers: 3,
            ..Default::default()
        },
    );
    owner.server_wire_ema.insert(1, 256.0);
    owner.publish_now();
    owner.publish_now(); // Structural publications must not add the torrent rate twice.
    let snapshot = owner.shared.load();
    let row = &snapshot.jobs[0];
    assert_eq!(
        (row.size_bytes, row.downloaded_bytes, row.remaining_bytes),
        (4096, 1024, 3072)
    );
    assert_eq!((row.files_total, row.files_done), (2, 1));
    assert_eq!(
        (row.rate_bps, row.upload_rate_bps, row.useful_peers),
        (512, 128, 3)
    );
    assert_eq!(snapshot.remaining_bytes, 3072);
    assert_eq!(snapshot.download_rate_bps, 768);
    assert_eq!(
        snapshot
            .server_volumes
            .iter()
            .map(|v| v.rate_bps)
            .sum::<u64>(),
        768
    );
}

#[tokio::test]
async fn seed_policy_stop_survives_restart_and_requires_a_new_policy_to_resume() {
    let (_tmp, mut owner, mut adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let mut job = control_torrent_job();
    job.status = JobStatus::Downloading;
    let torrent = job.torrent.as_mut().unwrap();
    torrent.phase = TorrentPhase::Seeding;
    torrent.ready_at_unix = Some(100);
    torrent.downloaded_bytes = 1;
    owner.state.jobs.push(job);
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::SetTorrentSeedPolicy {
        job: JobId(1),
        policy: nzbd_types::SeedPolicy {
            stop_on_complete: true,
            ..Default::default()
        },
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(rx.await.unwrap());
    assert_eq!(
        adapter.next_command().await,
        Some(BackendCommand::PauseForSeedPolicy { job: JobId(1) })
    );
    let persisted = SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    let saved = persisted.jobs[0].torrent.as_ref().unwrap();
    assert!(saved.seed_policy.stop_on_complete);
    assert_eq!(
        saved.stop_reason,
        Some(nzbd_types::TorrentStopReason::DownloadComplete)
    );
    assert_eq!(saved.control_intent, TorrentControlIntent::Paused);
    assert_eq!(
        owner.shared.load().jobs[0].seed_stop_reason,
        saved.stop_reason
    );
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::Resume {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(!rx.await.unwrap());

    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::SetTorrentSeedPolicy {
        job: JobId(1),
        policy: Default::default(),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(rx.await.unwrap());
    assert_eq!(
        owner.state.jobs[0].status,
        JobStatus::Paused,
        "editing a policy must not restart a stopped seed"
    );
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::Resume {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(rx.await.unwrap());
    assert_eq!(
        owner.state.jobs[0].torrent.as_ref().unwrap().stop_reason,
        None
    );
}

#[tokio::test]
async fn torrent_missing_file_recovery_is_not_blocked_by_a_completed_seed_policy() {
    let (tmp, mut owner, mut adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let mut job = control_torrent_job();
    job.status = JobStatus::Paused;
    let torrent = job.torrent.as_mut().unwrap();
    torrent.phase = TorrentPhase::MissingFiles;
    torrent.ready_at_unix = Some(100);
    torrent.content_path = Some(PathBuf::from("/payload"));
    torrent.seed_policy.stop_on_complete = true;
    owner.state.jobs.push(job);
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::Resume {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(rx.await.unwrap());
    owner.schedule_torrent_starts();
    pump(&mut owner, &mut messages).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), adapter.next_command())
            .await
            .unwrap(),
        Some(BackendCommand::Start { job: JobId(1) })
    );
    let torrent = owner.state.jobs[0].torrent.as_ref().unwrap();
    assert_eq!(torrent.phase, TorrentPhase::Queued);
    assert_eq!(torrent.ready_at_unix, None);
    assert_eq!(torrent.content_path, None);
    assert!(torrent.seed_policy.stop_on_complete);
    let persisted = SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    assert_eq!(
        persisted.jobs[0].torrent.as_ref().unwrap().ready_at_unix,
        None
    );
    let payload_root = tmp.path().join("payload");
    std::fs::create_dir_all(&payload_root).unwrap();
    let content_path = payload_root.join("recovered.bin");
    std::fs::write(&content_path, b"x").unwrap();
    owner.torrent_payload_roots = vec![payload_root];
    adapter.progress(
        JobId(1),
        crate::backend::TransferProgress {
            verified_bytes: 1,
            ..Default::default()
        },
    );
    adapter
        .structural(BackendFact::Ready {
            job: JobId(1),
            content_path,
        })
        .await
        .unwrap();
    owner.fold_backend_progress();
    pump(&mut owner, &mut messages).await;
    owner.fold_backend_structural();
    pump(&mut owner, &mut messages).await;
    owner.update_seed_policies(200);
    pump(&mut owner, &mut messages).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), adapter.next_command())
            .await
            .unwrap(),
        Some(BackendCommand::PauseForSeedPolicy { job: JobId(1) })
    );
    let torrent = owner.state.jobs[0].torrent.as_ref().unwrap();
    assert!(torrent.ready_at_unix.is_some());
    assert_eq!(
        torrent.stop_reason,
        Some(nzbd_types::TorrentStopReason::DownloadComplete)
    );
}

#[tokio::test]
async fn torrent_storage_recovery_clears_the_stop_reason() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let mut job = control_torrent_job();
    job.status = JobStatus::Paused;
    let torrent = job.torrent.as_mut().unwrap();
    torrent.phase = TorrentPhase::PausedDownload;
    torrent.last_error = Some("storage full".into());
    torrent.stop_reason = Some(nzbd_types::TorrentStopReason::StorageFull);
    owner.state.jobs.push(job);
    owner.release_torrents_after_storage_recovery();
    pump(&mut owner, &mut messages).await;
    let torrent = owner.state.jobs[0].torrent.as_ref().unwrap();
    assert_eq!(torrent.phase, TorrentPhase::Queued);
    assert_eq!(torrent.stop_reason, None);
    assert_eq!(torrent.last_error, None);
}

#[tokio::test]
async fn seed_policy_save_failure_rolls_back_without_stopping_the_torrent() {
    let (_tmp, mut owner, mut adapter, mut messages) =
        control_test_owner_with_persistence(true, Some(Arc::new(|| false)));
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(control_torrent_job());
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::SetTorrentSeedPolicy {
        job: JobId(1),
        policy: nzbd_types::SeedPolicy {
            stop_on_complete: true,
            ..Default::default()
        },
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(!rx.await.unwrap());
    assert_eq!(
        owner.state.jobs[0].torrent.as_ref().unwrap().seed_policy,
        Default::default()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), adapter.next_command())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn torrent_control_is_persisted_before_fifo_delivery_and_is_idempotent() {
    let (tmp, mut owner, mut adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(control_torrent_job());

    let (reply, mut rx) = oneshot::channel();
    owner.on_command(QueueCommand::Pause {
        job: JobId(1),
        reply,
    });
    assert!(
        matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
        "control acknowledgement must wait for the actual worker completion"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(1), adapter.next_command())
            .await
            .is_err(),
        "backend delivery must wait for durable owner acceptance"
    );
    pump(&mut owner, &mut messages).await;
    assert!(rx.await.unwrap());
    assert_eq!(
        adapter.next_command().await,
        Some(BackendCommand::Pause { job: JobId(1) })
    );

    let persisted = SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    let torrent = persisted.jobs[0].torrent.as_ref().unwrap();
    assert_eq!(persisted.jobs[0].status, JobStatus::Paused);
    assert_eq!(torrent.control_intent, TorrentControlIntent::Paused);
    assert_eq!(
        torrent.stop_reason,
        Some(nzbd_types::TorrentStopReason::Manual)
    );

    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::Pause {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(!rx.await.unwrap());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), adapter.next_command())
            .await
            .is_err()
    );

    let paused = owner.state.jobs[0].torrent.as_mut().unwrap();
    paused.phase = nzbd_types::TorrentPhase::PausedDownload;
    paused.last_activity_unix = Some(1);
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::Resume {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(rx.await.unwrap());
    owner.schedule_torrent_starts();
    pump(&mut owner, &mut messages).await;
    assert_eq!(
        adapter.next_command().await,
        Some(BackendCommand::Start { job: JobId(1) })
    );
    assert!(tmp.path().join("state/queue.json").exists());
}

#[tokio::test]
async fn failed_persistence_never_emits_a_torrent_control() {
    let (_tmp, mut owner, mut adapter, mut messages) =
        control_test_owner_with_persistence(true, Some(Arc::new(|| false)));
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(control_torrent_job());

    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::Pause {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;

    assert!(!rx.await.unwrap());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), adapter.next_command())
            .await
            .is_err(),
        "a failed durability barrier must prevent FIFO delivery"
    );
    let job = owner.state.job(JobId(1)).unwrap();
    assert_eq!(job.status, JobStatus::Queued);
    assert_eq!(
        job.torrent.as_ref().unwrap().control_intent,
        TorrentControlIntent::Running
    );
}

#[tokio::test]
async fn full_backend_fifo_retries_retained_torrent_commands_in_order() {
    let (_tmp, mut owner, mut adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(control_torrent_job());

    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::Pause {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(rx.await.unwrap());
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::SetPriority {
        job: JobId(1),
        priority: 77,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(rx.await.unwrap());
    assert_eq!(owner.pending_backend_commands.len(), 1);

    assert_eq!(
        adapter.next_command().await,
        Some(BackendCommand::Pause { job: JobId(1) })
    );
    owner.flush_backend_commands();
    assert_eq!(
        adapter.next_command().await,
        Some(BackendCommand::SetPriority {
            job: JobId(1),
            priority: 77
        })
    );
}

#[tokio::test]
async fn retained_backend_commands_precede_later_control_requests() {
    let (_tmp, mut owner, mut adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(control_torrent_job());

    for command in [
        QueueCommand::Pause {
            job: JobId(1),
            reply: oneshot::channel().0,
        },
        QueueCommand::SetPriority {
            job: JobId(1),
            priority: 77,
            reply: oneshot::channel().0,
        },
    ] {
        owner.on_command(command);
        pump(&mut owner, &mut messages).await;
    }
    assert_eq!(owner.pending_backend_commands.len(), 1);
    assert_eq!(
        adapter.next_command().await,
        Some(BackendCommand::Pause { job: JobId(1) })
    );

    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::Resume {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(rx.await.unwrap());
    owner.schedule_torrent_starts();
    pump(&mut owner, &mut messages).await;
    assert_eq!(owner.pending_backend_commands.len(), 2);

    owner.flush_backend_commands();
    assert_eq!(
        adapter.next_command().await,
        Some(BackendCommand::SetPriority {
            job: JobId(1),
            priority: 77
        })
    );
    owner.flush_backend_commands();
    assert_eq!(
        adapter.next_command().await,
        Some(BackendCommand::Start { job: JobId(1) })
    );
}

#[tokio::test]
async fn worker_mode_torrent_controls_do_not_require_queue_persistence() {
    let (_tmp, mut owner, mut adapter, mut messages) =
        control_test_owner_with_persistence(false, None);
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(control_torrent_job());

    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::Pause {
        job: JobId(1),
        reply,
    });
    pump(&mut owner, &mut messages).await;

    assert!(rx.await.unwrap());
    assert_eq!(
        adapter.next_command().await,
        Some(BackendCommand::Pause { job: JobId(1) })
    );
}

#[tokio::test]
async fn retained_backend_commands_are_bounded_before_persisting_new_intent() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(control_torrent_job());

    for priority in 1..=MAX_PENDING_BACKEND_COMMANDS + 1 {
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::SetPriority {
            job: JobId(1),
            priority: priority as i32,
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(rx.await.unwrap());
    }
    assert_eq!(
        owner.pending_backend_commands.len(),
        MAX_PENDING_BACKEND_COMMANDS
    );

    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::SetPriority {
        job: JobId(1),
        priority: (MAX_PENDING_BACKEND_COMMANDS + 2) as i32,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(!rx.await.unwrap());
    assert_eq!(
        owner.pending_backend_commands.len(),
        MAX_PENDING_BACKEND_COMMANDS
    );
    assert_eq!(
        owner.state.job(JobId(1)).unwrap().priority,
        (MAX_PENDING_BACKEND_COMMANDS + 1) as i32
    );
}

#[tokio::test]
async fn latest_backend_progress_is_folded_without_a_fifo_backlog() {
    let (_tmp, mut owner, adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(control_torrent_job());
    for verified in 0..50_000 {
        adapter.progress(
            JobId(1),
            crate::backend::TransferProgress {
                verified_bytes: verified,
                ..Default::default()
            },
        );
    }
    owner.fold_backend_progress();
    pump(&mut owner, &mut messages).await;
    assert_eq!(
        owner
            .state
            .job(JobId(1))
            .unwrap()
            .torrent
            .as_ref()
            .unwrap()
            .downloaded_bytes,
        1,
        "the latest sample is folded once and clamped to selected bytes"
    );
}

fn bare_job() -> Job {
    Job {
        id: JobId(1),
        kind: nzbd_types::JobKind::Nzb,
        name: "x".into(),
        dir_name: String::new(),
        name_provisional: false,
        delivery_lifecycle: false,
        queued_at_unix: 0,
        original_name: String::new(),
        category: None,
        priority: 0,
        dupe: Default::default(),
        params: vec![],
        files: vec![],
        totals: Default::default(),
        status: JobStatus::PostQueued,
        torrent: None,
        stages: vec![],
    }
}

fn pending_job(id: u32) -> Job {
    let mut job = bare_job();
    job.id = JobId(id);
    job.name = format!("job-{id}");
    job.dir_name = job.name.clone();
    job.status = JobStatus::Queued;
    job.files = vec![nzbd_types::FileEntry {
        id: FileId(id * 10),
        subject: "payload.bin".into(),
        filename: "payload.bin".into(),
        filename_confirmed: true,
        is_par2: false,
        paused: false,
        groups: vec!["alt.test".into()],
        date: None,
        segments: vec![nzbd_types::Segment {
            message_id: "part@example".into(),
            number: 1,
            size: 100,
            state: SegmentState::Pending,
        }],
        crc32: None,
        finalized: false,
    }];
    recompute_job_totals(&mut job);
    job
}

fn install_control_test_server(owner: &mut Owner) {
    owner.servers = Arc::new(vec![ServerDef {
        id: ServerId(1),
        name: "test".into(),
        host: "localhost".into(),
        port: 119,
        tls: nzbd_types::TlsMode::None,
        username: None,
        password: None,
        active: true,
        tier: 0,
        group: 0,
        fill: false,
        max_connections: 2,
        pipeline_depth: 1,
        retention_days: 0,
        cert_verification: nzbd_types::CertLevel::Strict,
    }]);
}

async fn block_control_test_io(owner: &mut Owner) -> std::sync::mpsc::Sender<()> {
    let (started, started_rx) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    owner.io_then(
        move || {
            let _ = started.send(());
            wait.recv().expect("release blocked control test IO");
            Ok(())
        },
        |_, result| result.expect("blocked control test IO completes"),
    );
    tokio::time::timeout(Duration::from_secs(1), started_rx)
        .await
        .expect("IO worker started")
        .expect("worker start signal");
    release
}

#[tokio::test]
async fn pending_payload_allocation_does_not_starve_an_allocated_job() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    install_control_test_server(&mut owner);
    owner.state.jobs = vec![pending_job(1), pending_job(2)];
    allocate_for_test(&mut owner, &mut messages, JobId(2), false).await;
    owner.allocating_jobs.insert(JobId(1));

    let leases = grant_work_for_test(&mut owner, &mut messages, ServerId(1), 1).await;
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].r.job, JobId(2));
    assert!(matches!(
        &owner
            .state
            .segment_mut(SegRef {
                job: JobId(1),
                file: FileId(10),
                seg_number: 1,
            })
            .unwrap()
            .state,
        SegmentState::Pending
    ));
}

#[tokio::test]
async fn resume_waits_for_durable_revision_and_failure_keeps_both_schedulers_fenced() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let allowed = Arc::new(AtomicBool::new(true));
    let observed = allowed.clone();
    // NZB lease path: a queued in-memory Resume stays ineligible while its
    // exact control revision is waiting on the ordered IO/snapshot lane.
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner_with_persistence(
        true,
        Some(Arc::new(move || observed.load(Ordering::SeqCst))),
    );
    pump(&mut owner, &mut messages).await;
    install_control_test_server(&mut owner);
    let mut job = pending_job(1);
    job.status = JobStatus::Paused;
    owner.state.jobs.push(job);
    allocate_for_test(&mut owner, &mut messages, JobId(1), false).await;
    persist_snapshot_for_test(&mut owner, &mut messages).await;
    let release = block_control_test_io(&mut owner).await;
    let (reply, resumed) = oneshot::channel();
    owner.on_command(QueueCommand::Resume {
        job: JobId(1),
        reply,
    });
    assert!(owner.pending_control_revisions.contains_key(&JobId(1)));
    assert!(owner.grant_work(ServerId(1), 1).is_empty());
    assert!(
        owner.writer_for(JobId(1), FileId(10)).is_closed(),
        "direct writer acquisition must honor the same durable control fence"
    );
    allowed.store(false, Ordering::SeqCst);
    release.send(()).unwrap();
    pump(&mut owner, &mut messages).await;
    assert!(!resumed.await.unwrap());
    assert!(owner.state.job(JobId(1)).unwrap().held());
    assert!(!owner.pending_control_revisions.contains_key(&JobId(1)));
    assert!(owner.grant_work(ServerId(1), 1).is_empty());

    // Torrent Start follows the same fence and remains stopped on failed
    // persistence, even though the in-memory control intent is Running.
    let allowed = Arc::new(AtomicBool::new(true));
    let observed = allowed.clone();
    let (_tmp, mut owner, mut adapter, mut messages) = control_test_owner_with_persistence(
        true,
        Some(Arc::new(move || observed.load(Ordering::SeqCst))),
    );
    pump(&mut owner, &mut messages).await;
    let mut torrent = control_torrent_job();
    torrent.status = JobStatus::Paused;
    let control = torrent.torrent.as_mut().unwrap();
    control.phase = TorrentPhase::PausedDownload;
    control.control_intent = TorrentControlIntent::Paused;
    owner.state.jobs.push(torrent);
    persist_snapshot_for_test(&mut owner, &mut messages).await;
    let release = block_control_test_io(&mut owner).await;
    let (reply, resumed) = oneshot::channel();
    owner.on_command(QueueCommand::Resume {
        job: JobId(1),
        reply,
    });
    owner.schedule_torrent_starts();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), adapter.next_command())
            .await
            .is_err()
    );
    allowed.store(false, Ordering::SeqCst);
    release.send(()).unwrap();
    pump(&mut owner, &mut messages).await;
    assert!(!resumed.await.unwrap());
    assert!(owner.state.job(JobId(1)).unwrap().held());
    owner.schedule_torrent_starts();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), adapter.next_command())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn obsolete_resume_callback_cannot_release_newer_pause_revision() {
    let (_tmp, mut owner, mut adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let mut torrent = control_torrent_job();
    torrent.status = JobStatus::Paused;
    let control = torrent.torrent.as_mut().unwrap();
    control.phase = TorrentPhase::PausedDownload;
    control.control_intent = TorrentControlIntent::Paused;
    owner.state.jobs.push(torrent);
    let release = block_control_test_io(&mut owner).await;

    let (resume_reply, resumed) = oneshot::channel();
    owner.on_command(QueueCommand::Resume {
        job: JobId(1),
        reply: resume_reply,
    });
    let resume_revision = *owner.pending_control_revisions.get(&JobId(1)).unwrap();
    let (pause_reply, paused) = oneshot::channel();
    owner.on_command(QueueCommand::Pause {
        job: JobId(1),
        reply: pause_reply,
    });
    let pause_revision = *owner.pending_control_revisions.get(&JobId(1)).unwrap();
    assert_ne!(resume_revision, pause_revision);
    owner.schedule_torrent_starts();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), adapter.next_command())
            .await
            .is_err()
    );

    release.send(()).unwrap();
    pump(&mut owner, &mut messages).await;
    assert!(!resumed.await.unwrap());
    assert!(paused.await.unwrap());
    assert!(!owner.pending_control_revisions.contains_key(&JobId(1)));
    assert_eq!(owner.state.job(JobId(1)).unwrap().status, JobStatus::Paused);
    assert!(
        matches!(
            tokio::time::timeout(Duration::from_secs(1), adapter.next_command())
                .await
                .expect("durable Pause reaches the backend"),
            Some(BackendCommand::Pause { job: JobId(1) })
        ),
        "only the newer durable Pause may reach the backend"
    );
    owner.schedule_torrent_starts();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), adapter.next_command())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn url_download_allocates_fresh_directory_beside_retained_payload() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let post = nzbd_nserv::build_post("release", &[("payload.bin", vec![7; 100])], 100);
    let parsed = nzbd_nzb::parse(post.nzb.as_bytes()).unwrap();
    let id = owner
        .state
        .admit_url("release".into(), "http://example.test/file.nzb", None, 0);
    let retained = owner.dest_dir.join("release");
    std::fs::create_dir_all(&retained).unwrap();
    std::fs::write(retained.join("old.bin"), b"retained bytes").unwrap();
    let collision = owner.dest_dir.join(format!("release.job-{}-0", id.0));
    std::fs::create_dir(&collision).unwrap();

    let origin = owner.state.job(id).unwrap().clone();
    let identity = nzbd_state::artifacts::url_source_identity(&origin).unwrap();
    owner
        .url_resolutions
        .cache_source(&origin, post.nzb.as_bytes())
        .unwrap();
    owner
        .url_resolutions
        .reserve_files(
            &origin,
            owner.state.next_file_id + 1,
            parsed.files.len() as u32,
        )
        .unwrap();
    let mut staging = QueueState {
        next_file_id: owner.state.next_file_id,
        ..Default::default()
    };
    staging.jobs.push(origin.clone());
    staging.complete_url_fetch(id, &parsed, owner.tuning.pause_extra_pars);
    let prepared = owner
        .url_resolutions
        .stage(&origin, &staging.jobs[0])
        .unwrap();
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::CompleteUrlFetch {
        job: id,
        source_identity: identity,
        prepared: Box::new(prepared),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap().unwrap());
    let job = owner.state.job(id).unwrap();
    assert_eq!(job.name, "release");
    assert_eq!(job.status, JobStatus::Queued);
    let expected = format!("release.job-{}-1", id.0);
    assert_eq!(job.dir_name, expected);
    let allocation = owner.artifacts.for_job(id.0).unwrap().unwrap();
    assert_eq!(allocation.path, owner.dest_dir.join(&expected));
    owner
        .artifacts
        .allocate(id.0, &owner.dest_dir, &allocation.path)
        .unwrap();
    assert_eq!(
        std::fs::read(retained.join("old.bin")).unwrap(),
        b"retained bytes"
    );
    assert!(collision.is_dir());
    let saved = SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    assert_eq!(
        saved.jobs.iter().find(|j| j.id == id).unwrap().dir_name,
        expected
    );
}

#[tokio::test]
async fn allocation_refusal_holds_only_that_job_and_release_retries_it() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    owner.servers = Arc::new(vec![ServerDef {
        id: ServerId(1),
        name: "test".into(),
        host: "localhost".into(),
        port: 119,
        tls: nzbd_types::TlsMode::None,
        username: None,
        password: None,
        active: true,
        tier: 0,
        group: 0,
        fill: false,
        max_connections: 1,
        pipeline_depth: 1,
        retention_days: 0,
        cert_verification: nzbd_types::CertLevel::Strict,
    }]);
    owner.state.jobs = (1..=3).map(pending_job).collect();
    let conflict = owner.dest_dir.join("job-1");
    std::fs::create_dir_all(&conflict).unwrap();
    let leases = grant_work_for_test(&mut owner, &mut messages, ServerId(1), 1).await;
    assert_eq!(leases.len(), 1, "a hold must not consume the lease budget");
    assert_eq!(leases[0].r.job, JobId(2));
    assert_eq!(owner.state.jobs[0].control().unwrap().cause, "allocation");
    assert_eq!(
        grant_work_for_test(&mut owner, &mut messages, ServerId(1), 1).await[0]
            .r
            .job,
        JobId(3)
    );
    let revision = owner.state.jobs[0].control().unwrap().revision;
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::ReleaseResourceHold {
        guard: crate::hold_release::HoldReleaseGuard::default(),
        by: RetryBy::Operator,
        job: JobId(1),
        revision: revision.clone(),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(rx.await.unwrap());
    assert!(
        grant_work_for_test(&mut owner, &mut messages, ServerId(1), 1)
            .await
            .is_empty()
    );
    let held = owner.state.jobs[0].control().unwrap();
    assert_eq!(held.lifecycle, "held");
    assert!(held.revision.parse::<u64>().unwrap() > revision.parse::<u64>().unwrap());
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::ReleaseResourceHold {
        guard: crate::hold_release::HoldReleaseGuard::default(),
        by: RetryBy::Operator,
        job: JobId(1),
        revision,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(!rx.await.unwrap(), "stale release must be rejected");
    std::fs::remove_dir(&conflict).unwrap();
    let mut epoch = owner.epoch_tx.subscribe();
    epoch.borrow_and_update();
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::ReleaseResourceHold {
        guard: crate::hold_release::HoldReleaseGuard::default(),
        by: RetryBy::Operator,
        job: JobId(1),
        revision: owner.state.jobs[0].control().unwrap().revision,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(rx.await.unwrap());
    assert!(epoch.has_changed().unwrap());
    assert_eq!(
        grant_work_for_test(&mut owner, &mut messages, ServerId(1), 1).await[0]
            .r
            .job,
        JobId(1)
    );
}

use nzbd_types::HoldCause;

/// The release an operator `resume` runs, minus the off-loop part: the
/// custody check is done here exactly as `EngineHandle::release_hold`
/// does it in `spawn_blocking`.
async fn operator_retry(
    owner: &mut Owner,
    messages: &mut mpsc::Receiver<EngineMsg>,
    job: JobId,
) -> Result<(), String> {
    let control = owner.state.job(job).unwrap().control().unwrap();
    let custody_checked = control.post_side() && owner.artifacts.validate_post_retry(job.0).is_ok();
    let result = owner.retry_hold_proven(
        job,
        &control.revision,
        RetryBy::Operator,
        custody_checked,
        &Default::default(),
    );
    pump(owner, messages).await;
    result
}

#[tokio::test]
async fn post_hold_retry_checks_revision_custody_and_stage_then_persists() {
    let (tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let mut job = bare_job();
    job.dir_name = "pp".into();
    owner.state.jobs.push(job);
    allocate_for_test(&mut owner, &mut messages, JobId(1), false).await;
    assert!(owner.hold_job(JobId(1), HoldCause::MissingTool, "unpack", "tool failed"));
    pump(&mut owner, &mut messages).await;
    let revision = owner.state.jobs[0].control().unwrap().revision;
    assert!(owner
        .retry_hold_proven(
            JobId(1),
            "stale",
            RetryBy::Operator,
            true,
            &Default::default()
        )
        .is_err());
    pump(&mut owner, &mut messages).await;
    assert!(
        owner
            .retry_hold_proven(
                JobId(1),
                &revision,
                RetryBy::Operator,
                false,
                &Default::default()
            )
            .is_err(),
        "a post-side release needs its custody checked off-loop first"
    );
    pump(&mut owner, &mut messages).await;
    assert_eq!(
        owner.retry_hold_proven(
            JobId(1),
            &revision,
            RetryBy::Operator,
            true,
            &Default::default()
        ),
        Ok(())
    );
    pump(&mut owner, &mut messages).await;
    assert_eq!(owner.state.jobs[0].status, JobStatus::PostQueued);
    persist_snapshot_for_test(&mut owner, &mut messages).await;
    let persisted = SnapshotStore::open(&tmp.path().join("state"))
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    let restored = QueueState::from_doc(persisted);
    assert!(!restored.jobs[0].held());
    // A post-side retry always restarts post-processing from the
    // beginning, so the stage a hold names never makes it unsafe; one
    // this build does not recognise must still have an exit.
    assert!(owner.hold_job(
        JobId(1),
        HoldCause::Unrecognized("from_a_newer_build".into()),
        "unrecognised_stage",
        "why"
    ));
    pump(&mut owner, &mut messages).await;
    assert_eq!(
        operator_retry(&mut owner, &mut messages, JobId(1)).await,
        Ok(())
    );
    assert_eq!(owner.state.jobs[0].status, JobStatus::PostQueued);
}

/// Field report 2026-11-01: `/processing` lost its transport (ENOTCONN)
/// mid-download and the job sat HELD with only delete to click. The
/// errno makes it `storage_unavailable`, and its retry time is in the
/// same save as the hold.
#[tokio::test]
async fn storage_errno_writer_error_schedules_itself() {
    let (tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(pending_job(1));
    let before = unix_now();
    owner.on_msg(EngineMsg::WriterError {
            job: JobId(1),
            file: FileId(10),
            error: "write /processing/x/.runner-file-10.part: Transport endpoint is not connected (os error 107)".into(),
        });
    pump(&mut owner, &mut messages).await;
    let held = owner.state.jobs[0].control().unwrap();
    assert_eq!(held.cause, "storage_unavailable");
    assert_eq!(held.retry_policy, "scheduled_retry");
    assert_eq!(held.retry_attempt, 1);
    let at = held
        .next_retry_at_unix
        .expect("scheduled in the hold itself");
    assert!(
        at >= before + 30 && at <= unix_now() + 33,
        "first retry after 30 s plus at most 10% positive jitter: {at}"
    );
    let persisted = SnapshotStore::open(&tmp.path().join("state"))
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    assert_eq!(
        QueueState::from_doc(persisted).jobs[0]
            .control()
            .unwrap()
            .next_retry_at_unix,
        Some(at),
        "the schedule is durable with the hold, not a second write"
    );

    let mut due = held.clone();
    due.next_retry_at_unix = Some(0);
    owner.state.jobs[0].set_control(&due);
    assert_eq!(
        owner.retry_hold_proven(
            JobId(1),
            &held.revision,
            RetryBy::Recovery,
            false,
            &Default::default()
        ),
        Ok(())
    );
    pump(&mut owner, &mut messages).await;
    let job = &owner.state.jobs[0];
    assert!(!job.held());
    assert_eq!(
        job.status,
        JobStatus::Queued,
        "a download-side hold resumes the download"
    );
    assert!(job
        .control()
        .unwrap()
        .message
        .starts_with("Automatic retry 1"));

    // Still broken: held again, one step further out, same outage start.
    let retry_started = unix_now();
    assert!(owner.hold_job(
        JobId(1),
        HoldCause::StorageUnavailable,
        "download_write",
        "still ENOTCONN"
    ));
    pump(&mut owner, &mut messages).await;
    let again = owner.state.jobs[0].control().unwrap();
    assert_eq!(again.retry_attempt, 2);
    assert!(again.next_retry_at_unix.unwrap() >= retry_started + 60);
    assert_eq!(again.blocked_since_unix, held.blocked_since_unix);

    // A refusal pushes it out again and says why.
    let revision = again.revision.clone();
    assert!(owner.reschedule_hold(JobId(1), &revision, "volume still unavailable"));
    let pushed = owner.state.jobs[0].control().unwrap();
    assert_eq!(pushed.retry_attempt, 3);
    assert!(pushed
        .message
        .ends_with("; retry refused: volume still unavailable"));
    assert!(
        !owner.reschedule_hold(JobId(1), &revision, "stale"),
        "a reschedule is a new revision; a second actor on the old one is refused"
    );
    assert!(owner.reschedule_hold(JobId(1), &pushed.revision, "and again"));
    assert_eq!(
        owner.state.jobs[0]
            .control()
            .unwrap()
            .message
            .matches("retry refused")
            .count(),
        1,
        "refusals replace each other instead of piling up"
    );
}

/// Contract §2.2: a digest or size mismatch fails the same way on every
/// attempt, so it is not a hold — it is the job's terminal `failed/io`
/// (parked, never the release's fault), recorded before anything else.
#[tokio::test]
async fn permanent_writer_error_is_a_terminal_io_failure() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(pending_job(1));
    owner.on_msg(EngineMsg::WriterError {
        job: JobId(1),
        file: FileId(10),
        error: "finalize: complete checkpoint digest differs".into(),
    });
    for _ in 0..20 {
        pump(&mut owner, &mut messages).await;
        if owner.state.jobs[0]
            .params
            .iter()
            .any(|(key, _)| key == nzbd_types::PP_DONE_PARAM)
        {
            break;
        }
    }
    let job = &owner.state.jobs[0];
    assert!(!job.held(), "a terminal is not a hold");
    assert_eq!(job.status, JobStatus::Failed);
    assert!(job
        .params
        .iter()
        .any(|(key, value)| key == nzbd_types::PP_DONE_PARAM && value == "FAILURE/IO"));
    let terminal = owner.artifacts.terminal(1).unwrap().unwrap();
    assert_eq!(terminal.reason, Some(nzbd_types::FailureReason::Io));
    assert_eq!(terminal.stage.as_deref(), Some("finalize"));
    assert!(!terminal.blames_release);
    assert!(terminal.reason.unwrap().park_always());
    assert!(terminal.emitted && terminal.markers_stamped);
}

#[tokio::test]
async fn paused_hold_waits_for_the_operator_and_followers_never_auto_release() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(pending_job(1));
    assert!(owner.hold_job(
        JobId(1),
        HoldCause::StorageUnavailable,
        "download_write",
        "EIO (os error 5)"
    ));
    pump(&mut owner, &mut messages).await;
    let mut c = owner.state.jobs[0].control().unwrap();
    c.manual_pause = true;
    owner.state.jobs[0].set_control(&c);
    assert!(owner
        .retry_hold_proven(
            JobId(1),
            &c.revision,
            RetryBy::Recovery,
            false,
            &Default::default()
        )
        .is_err());
    pump(&mut owner, &mut messages).await;
    owner.persist = false;
    c.manual_pause = false;
    owner.state.jobs[0].set_control(&c);
    assert_eq!(
        owner.retry_hold_proven(
            JobId(1),
            &c.revision,
            RetryBy::Recovery,
            false,
            &Default::default()
        ),
        Err("this node is not the queue authority".into())
    );
    pump(&mut owner, &mut messages).await;
    owner.persist = true;
    c.manual_pause = true;
    c.retry_attempt = 4;
    owner.state.jobs[0].set_control(&c);
    assert_eq!(
        owner.retry_hold_proven(
            JobId(1),
            &c.revision,
            RetryBy::Operator,
            false,
            &Default::default()
        ),
        Ok(())
    );
    pump(&mut owner, &mut messages).await;
    let c = owner.state.jobs[0].control().unwrap();
    assert!(
        !c.manual_pause && c.retry_attempt == 0,
        "an operator retry clears both"
    );
    assert_eq!(owner.state.jobs[0].status, JobStatus::Queued);
}

/// The closed set: every cause this build writes, on both sides of the
/// pipeline, stamps its policy (with a time when the policy is timed)
/// and answers an operator release. Storage causes take
/// `ReleaseResourceHold` after their probe; the rest take `retry_hold`.
#[tokio::test]
async fn every_hold_cause_has_an_exit() {
    let causes = HoldCause::ALL
        .iter()
        .cloned()
        .chain([HoldCause::Unrecognized("from_a_newer_build".into())]);
    for cause in causes {
        for post in [false, true] {
            let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
            pump(&mut owner, &mut messages).await;
            let mut job = bare_job();
            job.dir_name = "exit".into();
            job.status = if post {
                JobStatus::Post {
                    stage: nzbd_types::PostStage::Unpack,
                }
            } else {
                JobStatus::Downloading
            };
            owner.state.jobs.push(job);
            allocate_for_test(&mut owner, &mut messages, JobId(1), false).await;
            assert!(
                owner.hold_job(JobId(1), cause.clone(), "stage", "why"),
                "{cause}"
            );
            pump(&mut owner, &mut messages).await;
            let control = owner.state.jobs[0].control().unwrap();
            let policy = cause.policy();
            assert_eq!(control.retry_policy, policy.as_str());
            assert_eq!(
                control.next_retry_at_unix.is_some(),
                policy.timed(),
                "{cause}"
            );
            if cause.is_storage() {
                let (reply, rx) = oneshot::channel();
                owner.on_command(QueueCommand::ReleaseResourceHold {
                    guard: crate::hold_release::HoldReleaseGuard::default(),
                    by: RetryBy::Operator,
                    job: JobId(1),
                    revision: control.revision,
                    reply,
                });
                pump(&mut owner, &mut messages).await;
                assert!(rx.await.unwrap(), "{cause} post={post} has no exit");
            } else {
                assert_eq!(
                    operator_retry(&mut owner, &mut messages, JobId(1)).await,
                    Ok(()),
                    "{cause} post={post} has no exit"
                );
            }
            assert!(!owner.state.jobs[0].held(), "{cause} post={post}");
            assert_eq!(
                owner.state.jobs[0].status,
                if post {
                    JobStatus::PostQueued
                } else {
                    JobStatus::Queued
                },
                "{cause} post={post} goes back to the step it stopped in"
            );
        }
    }
}

#[tokio::test]
async fn automatic_hold_recovery_survives_an_owner_request_timeout() {
    let temp = tempfile::tempdir().unwrap();
    let mut handle = crate::Engine::spawn(crate::EngineConfig::single_node(
        vec![],
        temp.path().join("state"),
        temp.path().join("dest"),
        crate::Tuning::default(),
        None,
    ))
    .await
    .unwrap();
    handle.import_job(bare_job(), false, false).await.unwrap();
    assert!(handle
        .hold_job(
            JobId(1),
            HoldCause::EngineUnavailable,
            "par_repair",
            "owner reply timed out"
        )
        .await
        .unwrap());
    let mut snapshot = (*handle.snapshot()).clone();
    snapshot.jobs[0]
        .control
        .as_mut()
        .unwrap()
        .next_retry_at_unix = Some(0);
    assert!(handle.shutdown().await);
    // Retain a due hold and a live command channel whose receiver stalls.
    // Only the reply deadline, rather than a closed channel, may end a pass.
    handle.shared = Arc::new(ArcSwap::from_pointee(snapshot));
    let (sender, mut receiver) = mpsc::channel(1);
    handle.cmd_tx = sender;
    let cancel = CancellationToken::new();
    tokio::time::pause();
    let recovery = tokio::spawn(crate::hold_recovery::run(handle, cancel.clone()));
    let first = tokio::time::timeout(Duration::from_secs(20), receiver.recv())
        .await
        .unwrap()
        .expect("first recovery pass sends an owner request");
    let second = tokio::time::timeout(Duration::from_secs(30), receiver.recv())
        .await
        .expect("recovery must run another pass after the owner deadline")
        .expect("the recovery service must remain alive after a timeout");
    assert!(!recovery.is_finished());
    cancel.cancel();
    drop((first, second));
    recovery.await.unwrap();
}

/// §3.3: `import_job` keeps a held fence, which is why a scheduling
/// write that went through it could never land. The fence stays; the
/// schedule now lives in `hold_job`, and only owner methods change it.
#[tokio::test]
async fn held_control_survives_only_owner_writes() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(pending_job(1));
    assert!(owner.hold_job(
        JobId(1),
        HoldCause::MountUnavailable,
        "download_write",
        "digest differs"
    ));
    pump(&mut owner, &mut messages).await;
    let held = owner.state.jobs[0].control().unwrap();
    let mut row = owner.state.jobs[0].clone();
    let mut forged = held.clone();
    forged.retry_policy = "scheduled_retry".into();
    forged.next_retry_at_unix = Some(1);
    row.set_control(&forged);
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::ImportJobIfPresent {
        completion_proof: None,
        job: Box::new(row),
        require_snapshot: false,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    let _ = rx.await;
    assert_eq!(
        owner.state.jobs[0].control().unwrap(),
        held,
        "imports cannot rewrite a fence"
    );
    // RescheduleHold is an owner write: it does change a fence.
    let mut storage = held.clone();
    storage.cause = "storage_unavailable".into();
    storage.retry_policy = "scheduled_retry".into();
    owner.state.jobs[0].set_control(&storage);
    assert!(owner.reschedule_hold(JobId(1), &storage.revision, "still down"));
    assert_ne!(owner.state.jobs[0].control().unwrap(), storage);
}

/// Review finding: `previous_status` was sticky across holds, so a job
/// first held while downloading and later held in post-processing was
/// released to the download queue without its custody check.
#[tokio::test]
async fn rehold_after_release_routes_by_where_the_job_is_now() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(pending_job(1));
    assert!(owner.hold_job(
        JobId(1),
        HoldCause::StorageUnavailable,
        "download_write",
        "x (os error 107)"
    ));
    pump(&mut owner, &mut messages).await;
    let first = owner.state.jobs[0].control().unwrap();
    assert!(!first.post_side());
    let mut due = first.clone();
    due.next_retry_at_unix = Some(0);
    owner.state.jobs[0].set_control(&due);
    owner
        .retry_hold_proven(
            JobId(1),
            &first.revision,
            RetryBy::Recovery,
            false,
            &Default::default(),
        )
        .unwrap();
    pump(&mut owner, &mut messages).await;
    // The download finished; post-processing hits the dead mount.
    owner.state.jobs[0].status = JobStatus::Post {
        stage: nzbd_types::PostStage::PostUnpackRename,
    };
    assert!(owner.hold_job(
        JobId(1),
        HoldCause::StorageUnavailable,
        "post_unpack_rename",
        "y (os error 5)"
    ));
    pump(&mut owner, &mut messages).await;
    let second = owner.state.jobs[0].control().unwrap();
    assert!(second.post_side(), "this hold happened in post-processing");
    assert!(
        owner
            .retry_hold_proven(
                JobId(1),
                &second.revision,
                RetryBy::Recovery,
                false,
                &Default::default()
            )
            .is_err(),
        "so its release must go through the custody check"
    );
    pump(&mut owner, &mut messages).await;
    assert_eq!(
        second.retry_attempt, 2,
        "a re-hold right after a release is the same outage"
    );
    assert_eq!(second.blocked_since_unix, first.blocked_since_unix);
    assert!(!second.manual_pause);

    // An hour after the last release, a new hold is a new episode.
    let mut released = second.clone();
    released.lifecycle = "running".into();
    released.released_at_unix = Some(unix_now() - nzbd_types::HOLD_EPISODE_SECONDS - 1);
    released.blocked_since_unix = Some(1);
    owner.state.jobs[0].set_control(&released);
    owner.state.jobs[0].status = JobStatus::Downloading;
    assert!(owner.hold_job(
        JobId(1),
        HoldCause::StorageUnavailable,
        "download_write",
        "z (os error 107)"
    ));
    pump(&mut owner, &mut messages).await;
    let fresh = owner.state.jobs[0].control().unwrap();
    assert_eq!(fresh.retry_attempt, 1, "backoff starts over");
    assert_ne!(
        fresh.blocked_since_unix,
        Some(1),
        "so does the outage clock"
    );
    assert!(!fresh.post_side());
}

/// Review finding: an operator retry on a paused storage hold left the
/// job paused, and the probe's spacing outlived its episode.
#[tokio::test]
async fn operator_storage_release_runs_the_job_and_resets_the_probe() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(pending_job(1));
    assert!(owner.hold_job(JobId(1), HoldCause::Capacity, "download_write", "full"));
    pump(&mut owner, &mut messages).await;
    let mut c = owner.state.jobs[0].control().unwrap();
    c.manual_pause = true;
    owner.state.jobs[0].set_control(&c);
    // A probe just ran: the recovery task must wait, a person need not.
    owner.state.jobs[0].params.extend([
        (
            "*Control:probe_after".to_string(),
            (unix_now() + 999).to_string(),
        ),
        ("*Control:attempts".to_string(), "6".to_string()),
    ]);
    for (by, expected) in [(RetryBy::Recovery, false), (RetryBy::Operator, true)] {
        let (reply, rx) = oneshot::channel();
        owner.on_command(QueueCommand::BeginResourceProbe {
            guard: crate::hold_release::HoldReleaseGuard::default(),
            job: JobId(1),
            revision: c.revision.clone(),
            by,
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert_eq!(rx.await.unwrap(), expected, "{by:?}");
    }
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::ReleaseResourceHold {
        guard: crate::hold_release::HoldReleaseGuard::default(),
        job: JobId(1),
        revision: c.revision.clone(),
        by: RetryBy::Operator,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(rx.await.unwrap());
    let job = &owner.state.jobs[0];
    assert_eq!(
        job.status,
        JobStatus::Queued,
        "an operator retry runs the job"
    );
    let c = job.control().unwrap();
    assert!(!c.manual_pause && c.retry_attempt == 0 && c.next_retry_at_unix.is_none());
    assert!(c.released_at_unix.is_some());
    assert!(
        !job.params
            .iter()
            .any(|(k, _)| k.starts_with("*Control:probe_after")
                || k.starts_with("*Control:attempts")),
        "the probe's spacing belongs to the episode that ended"
    );
}

#[tokio::test]
async fn reobserve_refusal_records_why_without_moving_its_cadence() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(pending_job(1));
    assert!(owner.hold_job(
        JobId(1),
        HoldCause::MountUnavailable,
        "extract",
        "root changed"
    ));
    pump(&mut owner, &mut messages).await;
    let c = owner.state.jobs[0].control().unwrap();
    assert_eq!(c.next_retry_at_unix, None);
    assert!(owner.reschedule_hold(JobId(1), &c.revision, "wrong root mounted"));
    let after = owner.state.jobs[0].control().unwrap();
    assert_eq!(
        after.next_retry_at_unix, None,
        "re-observed every pass, not timed"
    );
    assert_eq!(after.retry_attempt, c.retry_attempt);
    assert!(after
        .message
        .ends_with("; retry refused: wrong root mounted"));
    owner.persist = false;
    assert!(
        !owner.reschedule_hold(JobId(1), &c.revision, "follower"),
        "a node that is not the authority writes nothing"
    );
}

#[test]
fn rearm_reclassifies_by_errno_and_derives_policy() {
    let hold = |cause: &str, policy: &str, message: &str| nzbd_types::JobControl {
        version: 1,
        revision: "4".into(),
        lifecycle: "held".into(),
        cause: cause.into(),
        stage: "post_unpack_rename".into(),
        retry_policy: policy.into(),
        message: message.into(),
        instance: "i".into(),
        previous_status: None,
        manual_pause: false,
        retry_attempt: 0,
        next_retry_at_unix: None,
        blocked_since_unix: Some(1),
        released_at_unix: None,
        deadline_in_s: None,
        deadline_unix: None,
    };
    // #2181 exactly.
    let fixed = rearmed_hold(
        hold("io", "review", "io: Input/output error (os error 5)"),
        1000,
    )
    .expect("re-armed");
    assert_eq!(fixed.cause, "storage_unavailable");
    assert_eq!(fixed.retry_policy, "scheduled_retry");
    assert_eq!(
        fixed.next_retry_at_unix,
        Some(1000),
        "due on the first pass"
    );
    assert_eq!(fixed.revision, "5");
    // #2301: a control deadline at the failed-repair Keep boundary.
    let legacy = hold(
        "post_failure",
        "review",
        "subprocess failed: read live Keep setting: engine is shutting down",
    );
    let fixed = rearmed_hold(legacy.clone(), 1000).expect("legacy timeout is re-armed");
    assert_eq!(fixed.cause, "engine_unavailable");
    assert_eq!(fixed.retry_policy, "scheduled_retry");
    assert_eq!(fixed.next_retry_at_unix, Some(1000));
    assert_eq!(fixed.blocked_since_unix, legacy.blocked_since_unix);
    assert_eq!(fixed.revision, "5");
    assert!(!fixed.message.contains("shutting down"));
    let mut manual = legacy.clone();
    manual.manual_pause = true;
    assert!(rearmed_hold(manual, 1000).unwrap().manual_pause);
    for cause in ["identity_conflict", "io"] {
        let mut other = legacy.clone();
        other.cause = cause.into();
        assert!(rearmed_hold(other, 1000).is_none());
    }
    let mut unrelated = legacy;
    unrelated.message.push_str(" in a payload filename");
    assert!(rearmed_hold(unrelated, 1000).is_none());
    // #2183 exactly.
    assert!(rearmed_hold(
        hold(
            "io",
            "review",
            "write /processing/x.part: Transport endpoint is not connected (os error 107)"
        ),
        1000
    )
    .is_some_and(|c| c.cause == "storage_unavailable"));
    // A permanent io failure stays with a person.
    assert!(rearmed_hold(
        hold("io", "review", "complete checkpoint digest differs"),
        1000
    )
    .is_none());
    // Policy this build changed: password_required retries on the ladder
    // until its deadline (contract §2.3).
    assert_eq!(
        rearmed_hold(hold("password_required", "resume_same_job", "x"), 1000)
            .unwrap()
            .retry_policy,
        "scheduled_retry"
    );
    // Capacity used to carry no time.
    assert_eq!(
        rearmed_hold(hold("capacity", "resume_same_job", "x"), 1000)
            .unwrap()
            .next_retry_at_unix,
        Some(1000)
    );
    // Already right: untouched.
    assert!(rearmed_hold(hold("identity_conflict", "review", "x"), 1000).is_none());
    let mut scheduled = hold("storage_unavailable", "scheduled_retry", "x");
    scheduled.next_retry_at_unix = Some(5);
    assert!(rearmed_hold(scheduled, 1000).is_none());
}

/// Cluster engines start without `persist`, so `Owner::new` recovery
/// never runs for them; adoption must re-arm too.
#[tokio::test]
async fn adopting_authority_rearms_holds() {
    let (tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let mut job = pending_job(1);
    job.set_control(&nzbd_types::JobControl {
        version: 1,
        revision: "1".into(),
        lifecycle: "held".into(),
        cause: "io".into(),
        stage: "download_write".into(),
        retry_policy: "review".into(),
        message: "write x: Transport endpoint is not connected (os error 107)".into(),
        instance: "i".into(),
        previous_status: Some(JobStatus::Downloading),
        manual_pause: false,
        retry_attempt: 0,
        next_retry_at_unix: None,
        blocked_since_unix: Some(1),
        released_at_unix: None,
        deadline_in_s: None,
        deadline_unix: None,
    });
    job.status = JobStatus::Paused;
    let mut doc_state = QueueState::default();
    doc_state.jobs.push(job);
    SnapshotStore::open(&tmp.path().join("state"))
        .unwrap()
        .save(&doc_state.to_doc())
        .unwrap();
    owner.persist = false;
    owner.state.jobs.clear();
    owner.adopt_authority().unwrap();
    pump(&mut owner, &mut messages).await;
    let c = owner.state.jobs[0].control().unwrap();
    assert_eq!(
        (c.cause.as_str(), c.retry_policy.as_str()),
        ("storage_unavailable", "scheduled_retry")
    );
    assert!(c.next_retry_at_unix.is_some());
}

#[tokio::test]
async fn mount_unavailable_hold_resumes_only_after_expected_root_returns() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let mut job = bare_job();
    job.dir_name = "remount".into();
    job.status = JobStatus::Completed;
    job.params.push(("monarr-delivery".into(), "1".into()));
    owner.state.jobs.push(job);
    allocate_for_test(&mut owner, &mut messages, JobId(1), false).await;
    assert!(owner.hold_job(
        JobId(1),
        HoldCause::MountUnavailable,
        "extract",
        "configured root identity changed or volume is unavailable",
    ));
    pump(&mut owner, &mut messages).await;
    let root = owner.dest_dir.clone();
    let offline = root.with_extension("expected-volume-offline");
    std::fs::rename(&root, &offline).unwrap();
    std::fs::create_dir(&root).unwrap();
    assert!(
        operator_retry(&mut owner, &mut messages, JobId(1))
            .await
            .is_err(),
        "wrong root must stay fenced"
    );
    assert!(owner.state.jobs[0].held());

    std::fs::remove_dir(&root).unwrap();
    std::fs::rename(&offline, &root).unwrap();
    assert_eq!(
        operator_retry(&mut owner, &mut messages, JobId(1)).await,
        Ok(()),
        "restored expected root should resume"
    );
    assert_eq!(owner.state.jobs[0].status, JobStatus::PostQueued);
}

#[tokio::test]
async fn admission_tombstone_refreshes_exact_source_keep_and_delete_evidence() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let mut job = bare_job();
    job.dir_name = "admission-source".into();
    owner.state.jobs.push(job);
    allocate_for_test(&mut owner, &mut messages, JobId(1), false).await;
    let source = owner.artifacts.for_job(1).unwrap().unwrap();
    owner
        .artifacts
        .finish(1, &source.path, &source.root, "retained")
        .unwrap();
    let source = owner.artifacts.for_job(1).unwrap().unwrap();
    owner
        .state
        .url_admission_receipts
        .push(UrlAdmissionReceipt {
            protocol_version: 1,
            request_token: "receipt-token".into(),
            submission_sha256: "hash".into(),
            job_id: 1,
            generation: "admission-generation".into(),
            runner_instance_id: "runner".into(),
            terminal_outcome: Some("FAILURE/UNPACK".into()),
            outcome: "failed".into(),
            failure_stage: Some("unpack".into()),
            failure_cause: Some("corrupt_archive".into()),
            custody_state: "runner_owned".into(),
            keep_source_on_failure: false,
            source_cleanup_status: None,
            publication_path: None,
            source_artifact_id: Some(source.id.clone()),
            source_generation: Some(source.generation.clone()),
            failure_reason: None,
            fetch_cause: None,
            blames_release: None,
            salvage: None,
        });

    let kept = owner
        .artifacts
        .retention(&source.id, source.revision, true, None)
        .unwrap();
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::GetUrlAdmission {
        request_token: "receipt-token".into(),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    let receipt = rx.await.unwrap().unwrap();
    assert_eq!(receipt.custody_state, "kept_by_user");
    assert_eq!(
        receipt.source_generation.as_deref(),
        Some(kept.generation.as_str())
    );

    let released = owner
        .artifacts
        .retention(&kept.id, kept.revision, false, None)
        .unwrap();
    let deletion = owner
        .artifacts
        .request_delete(
            &released.id,
            released.revision,
            "delete-admission-source",
            0,
        )
        .unwrap();
    owner.artifacts.execute_delete(&deletion.id).unwrap();
    let (reply, rx) = oneshot::channel();
    owner.on_command(QueueCommand::GetUrlAdmission {
        request_token: "receipt-token".into(),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    let receipt = rx.await.unwrap().unwrap();
    assert_eq!(receipt.custody_state, "deleted_by_runner");
    assert_eq!(receipt.source_cleanup_status.as_deref(), Some("removed"));

    // A missing or reused artifact ID is not proof of deletion.
    let mut receipt = owner.state.url_admission_receipts[0].clone();
    receipt.custody_state = "runner_owned".into();
    receipt.source_cleanup_status = None;
    receipt.source_generation = Some("different-generation".into());
    let unchanged = refresh_url_admission_custody(&mut receipt, &owner.artifacts);
    assert!(!unchanged);
    assert_eq!(receipt.custody_state, "runner_owned");
}

#[tokio::test]
async fn writer_retirement_does_not_create_an_allocation_review_hold() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(pending_job(1));
    owner.retiring_writers.insert(JobId(1), Vec::new());
    assert!(owner.ensure_allocation(JobId(1), false).is_err());
    assert!(!owner.state.jobs[0].held());
}

fn restarted_recovery_job(id: u32, state: SegmentState) -> Job {
    let mut job = pending_job(id);
    job.status = JobStatus::Queued;
    job.stages.push(StageSpan {
        stage: PostStage::ParVerify,
        started_at_unix: 1_000,
        ms: None,
    });
    job.files[0].filename = "release.vol00+01.par2".into();
    job.files[0].is_par2 = true;
    job.files[0].segments[0].state = state;
    job.files[0].finalized = matches!(state, SegmentState::Done { .. });
    recompute_job_totals(&mut job);
    job
}

#[tokio::test]
async fn writer_resource_errors_hold_pending_segments_and_latch_out_of_space() {
    let (_tmp, mut owner, _epoch, mut messages) = guard_test_owner(Tuning::default());
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(pending_job(2));

    owner.on_msg(EngineMsg::WriterError {
        job: JobId(2),
        file: FileId(20),
        error: "write payload.bin: No space left on device (os error 28)".into(),
    });
    pump(&mut owner, &mut messages).await;
    let job = owner.state.job(JobId(2)).unwrap();
    assert_eq!(job.files[0].segments[0].state, SegmentState::Pending);
    assert!(!job.files[0].finalized);
    assert_eq!(job.status, JobStatus::Paused);
    assert!(job.held());
    assert!(!job.ready());
    assert!(owner.disk_low);
    assert!(owner.enospc_latched);
    assert_eq!(owner.enospc_observed, 1);

    // Re-reporting an already-terminal segment is idempotent, while a
    // missing file/job is ignored rather than corrupting another row.
    owner.on_msg(EngineMsg::WriterError {
        job: JobId(2),
        file: FileId(20),
        error: "permission denied".into(),
    });
    pump(&mut owner, &mut messages).await;
    owner.on_msg(EngineMsg::WriterError {
        job: JobId(2),
        file: FileId(999),
        error: "gone".into(),
    });
    pump(&mut owner, &mut messages).await;
    owner.on_msg(EngineMsg::SegmentWritten {
        job: JobId(999),
        file: FileId(999),
        seg_number: 1,
        offset: 0,
        len: 1,
        crc: 0,
        file_size: 1,
        server: ServerId(1),
    });
    pump(&mut owner, &mut messages).await;
    owner.on_msg(EngineMsg::WriterFinalized {
        job: JobId(999),
        file: FileId(999),
        ok: true,
        final_path: None,
        combined_crc: None,
    });
    pump(&mut owner, &mut messages).await;

    owner.disk_low = false;
    let (reply, received) = oneshot::channel();
    owner.on_msg(EngineMsg::WorkRequest {
        server: ServerId(999),
        max: 1,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(received.await.unwrap().is_empty());
}

#[tokio::test]
async fn failed_fenced_replace_restores_delegation_and_mirror_overlay() {
    let (_tmp, mut owner, _adapter, mut messages) =
        control_test_owner_with_persistence(true, Some(Arc::new(|| false)));
    pump(&mut owner, &mut messages).await;
    let original = pending_job(3);
    owner.state.jobs.push(original.clone());
    owner.delegated.insert(JobId(3), "worker-a".into());
    owner.mirror.insert(
        JobId(3),
        MirrorStats {
            repair_progress: None,
            done_articles: 4,
            failed_articles: 1,
            downloaded_bytes: 40,
            health: 800,
            remaining_bytes: Some(60),
            stages: vec![StageSpan {
                stage: PostStage::ParVerify,
                started_at_unix: 1_000,
                ms: None,
            }],
        },
    );
    let mut replacement = original;
    replacement.priority = 99;
    let (reply, mut received) = oneshot::channel();
    owner.on_command(QueueCommand::ImportJobIfPresent {
        completion_proof: None,
        job: Box::new(replacement),
        require_snapshot: true,
        reply,
    });
    pump(&mut owner, &mut messages).await;

    assert!(!received.try_recv().unwrap());
    assert_eq!(owner.state.job(JobId(3)).unwrap().priority, 0);
    assert_eq!(
        owner.delegated.get(&JobId(3)).map(String::as_str),
        Some("worker-a")
    );
    assert_eq!(owner.mirror[&JobId(3)].downloaded_bytes, 40);
    assert_eq!(
        owner.mirror[&JobId(3)].stages[0].stage,
        PostStage::ParVerify
    );
}

#[tokio::test]
async fn legacy_remote_pp_remaining_fallback_stays_paused_aware() {
    let (_tmp, mut owner, _epoch, mut messages) = guard_test_owner(Tuning::default());
    pump(&mut owner, &mut messages).await;
    let mut job = pending_job(4);
    job.status = JobStatus::Completed;
    job.files[0].paused = true;
    recompute_job_totals(&mut job);
    owner.state.jobs.push(job);
    owner.delegated.insert(JobId(4), "old-worker".into());
    owner.mirror.insert(
        JobId(4),
        MirrorStats {
            repair_progress: None,
            done_articles: 1,
            failed_articles: 0,
            downloaded_bytes: 0,
            health: 999,
            remaining_bytes: None,
            stages: vec![StageSpan {
                stage: PostStage::ParVerify,
                started_at_unix: 1_000,
                ms: None,
            }],
        },
    );

    owner.publish_now();
    let snapshot = owner.shared.load();
    assert_eq!(snapshot.jobs[0].remaining_bytes, 0);
    assert_eq!(snapshot.remaining_bytes, 0);
}

/// The monotonic figure the post manager measured is what lands on the
/// span. Wall-clock is a fallback, not the primary — an NTP step
/// during a long repair must not produce a duration nobody can trust.
#[test]
fn close_span_prefers_the_measured_duration() {
    let mut j = bare_job();
    j.stages.push(StageSpan {
        stage: PostStage::ParRepair,
        started_at_unix: 1_000,
        ms: None,
    });
    // Wall clock says 60s; the manager measured 4500ms.
    close_span(&mut j, Some(4500), 1_060);
    assert_eq!(j.stages[0].ms, Some(4500));
}

/// A span that outlived the process has no `Instant` left to consult,
/// so the seconds that survived in the snapshot are all there is.
#[test]
fn close_span_falls_back_to_wall_clock() {
    let mut j = bare_job();
    j.stages.push(StageSpan {
        stage: PostStage::Unpack,
        started_at_unix: 1_000,
        ms: None,
    });
    close_span(&mut j, None, 1_007);
    assert_eq!(j.stages[0].ms, Some(7_000));
}

/// `Stages::finish` closes the span and `Drop` closes it again. The
/// second close must not overwrite the first — otherwise every job's
/// last stage would report the few milliseconds between the two.
#[test]
fn close_span_is_idempotent() {
    let mut j = bare_job();
    j.stages.push(StageSpan {
        stage: PostStage::Move,
        started_at_unix: 1_000,
        ms: None,
    });
    close_span(&mut j, Some(9_000), 1_009);
    close_span(&mut j, Some(3), 1_009);
    assert_eq!(j.stages[0].ms, Some(9_000));
}

/// A clock that went backwards between entering and leaving a stage
/// yields 0, never a wrapped-around u64.
#[test]
fn close_span_survives_a_backwards_clock() {
    let mut j = bare_job();
    j.stages.push(StageSpan {
        stage: PostStage::ParVerify,
        started_at_unix: 2_000,
        ms: None,
    });
    close_span(&mut j, None, 1_900);
    assert_eq!(j.stages[0].ms, Some(0));
}

#[test]
fn close_span_on_a_job_that_never_post_processed() {
    let mut j = bare_job();
    close_span(&mut j, Some(5), 1_000);
    assert!(j.stages.is_empty());
}

/// Delayed PAR fetching temporarily returns the download half of the job
/// to Queued/Downloading, but verification keeps running in parallel. The
/// selected fetch returns to the open Post stage when it ends. Only the
/// verifier-selected PAR may use that lane; generic FileResume and a
/// PP-only node cannot broaden it to ordinary files.
#[tokio::test]
async fn delayed_par_download_keeps_the_open_post_span() {
    let (_tmp, mut owner, _epoch, mut messages) = guard_test_owner(Tuning::default());
    pump(&mut owner, &mut messages).await;
    let mut job = pending_job(402);
    job.status = JobStatus::Post {
        stage: PostStage::ParVerify,
    };
    job.stages.push(StageSpan {
        stage: PostStage::ParVerify,
        started_at_unix: 1_000,
        ms: None,
    });
    job.files[0].filename = "release.vol00+01.par2".into();
    job.files[0].is_par2 = true;
    job.files[0].paused = true;
    let recovery_file = job.files[0].id;
    let mut unrelated = job.files[0].clone();
    unrelated.id = FileId(4021);
    unrelated.filename = "release.part001.rar".into();
    unrelated.is_par2 = false;
    unrelated.paused = true;
    job.files.push(unrelated);
    recompute_job_totals(&mut job);
    owner.state.jobs.push(job);

    assert_eq!(owner.unpause_par_blocks(JobId(402), 1, None), 1);
    pump(&mut owner, &mut messages).await;
    assert_eq!(
        owner.state.job(JobId(402)).unwrap().status,
        JobStatus::Queued
    );
    assert!(owner.state.job(JobId(402)).unwrap().stages[0].ms.is_none());
    assert_eq!(
        owner.post_fetch_files[&JobId(402)],
        HashSet::from([recovery_file])
    );

    let (reply, mut result) = oneshot::channel();
    owner.on_command(QueueCommand::SetFilePaused {
        job: JobId(402),
        file: FileId(4021),
        paused: false,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(
        !result.try_recv().unwrap(),
        "FileResume is not recovery authority"
    );

    // Even if an unrelated file is already unpaused in imported/stale
    // state, selector enforcement still admits only the explicit PAR.
    owner.state.job_mut(JobId(402)).unwrap().files[1].paused = false;
    owner.delegated.insert(JobId(402), "this-node".into());
    owner.download_enabled = false;
    let servers = vec![ServerDef {
        id: ServerId(1),
        name: "provider".into(),
        host: "127.0.0.1".into(),
        port: 119,
        tls: nzbd_types::TlsMode::None,
        username: None,
        password: None,
        active: true,
        tier: 0,
        group: 0,
        fill: false,
        max_connections: 1,
        pipeline_depth: 1,
        retention_days: 0,
        cert_verification: nzbd_types::CertLevel::Strict,
    }];
    let ladder = Ladder::new(&servers);
    let not_blocked = |_: ServerId| false;
    let mut ctx = SelectionCtx {
        ladder: &ladder,
        attempts: &mut owner.attempts,
        is_blocked: &not_blocked,
        delegated: &owner.delegated,
        post_fetch_files: &owner.post_fetch_files,
        regular_downloads: owner.download_enabled,
        article_retries: 3,
        now_unix: 1_800_000_000,
        propagation_delay_secs: 0,
        soft_hold: false,
        rotate: 0,
    };
    let selected = next_for_server(&owner.state, &servers[0], &mut ctx)
        .lease
        .unwrap();
    assert_eq!(selected.file, recovery_file);
    owner.delegated.remove(&JobId(402));

    let job = owner.state.job_mut(JobId(402)).unwrap();
    job.files[1].paused = true;
    job.files[0].segments[0].state = SegmentState::Done {
        offset: 0,
        len: 100,
        crc: 0,
    };
    job.files[0].finalized = true;
    recompute_job_totals(job);
    let mut events = owner.events.subscribe();
    owner.check_job_complete(JobId(402));
    pump(&mut owner, &mut messages).await;
    let job = owner.state.job(JobId(402)).unwrap();
    assert_eq!(
        job.status,
        JobStatus::Post {
            stage: PostStage::ParVerify
        },
        "finishing a recovery-volume download resumes the open PP stage"
    );
    assert!(!owner.post_fetch_files.contains_key(&JobId(402)));
    assert!(
        job.stages[0].ms.is_none(),
        "the post manager, not delayed download completion, closes verification"
    );
    assert!(
        no_completion_event(&mut events),
        "a delayed PAR fetch must not announce a second download completion"
    );
}

#[tokio::test]
async fn restarted_delayed_par_completion_recovers_from_the_persisted_open_span() {
    let (_tmp, mut owner, _epoch, mut messages) = guard_test_owner(Tuning::default());
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(restarted_recovery_job(
        403,
        SegmentState::Done {
            offset: 0,
            len: 100,
            crc: 0,
        },
    ));
    assert!(
        !owner.post_fetch_files.contains_key(&JobId(403)),
        "the authorization map is intentionally absent after restart"
    );

    let mut events = owner.events.subscribe();
    owner.check_job_complete(JobId(403));
    pump(&mut owner, &mut messages).await;
    assert_eq!(
        owner.state.job(JobId(403)).unwrap().status,
        JobStatus::Post {
            stage: PostStage::ParVerify
        }
    );
    assert!(no_completion_event(&mut events));
}

#[tokio::test]
async fn restarted_pp_only_node_reauthorizes_only_the_pending_par_file() {
    let (_tmp, mut owner, _epoch, mut messages) = guard_test_owner(Tuning::default());
    pump(&mut owner, &mut messages).await;
    owner.download_enabled = false;
    let mut job = restarted_recovery_job(406, SegmentState::Pending);
    let recovery_file = job.files[0].id;
    let mut unrelated = job.files[0].clone();
    unrelated.id = FileId(4061);
    unrelated.filename = "release.part001.rar".into();
    unrelated.is_par2 = false;
    job.files.push(unrelated);
    owner.import_job(job, false, false);
    pump(&mut owner, &mut messages).await;

    assert_eq!(
        owner.post_fetch_files[&JobId(406)],
        HashSet::from([recovery_file]),
        "restart recovery may authorize the selected PAR and nothing else"
    );
    let servers = vec![ServerDef {
        id: ServerId(1),
        name: "provider".into(),
        host: "127.0.0.1".into(),
        port: 119,
        tls: nzbd_types::TlsMode::None,
        username: None,
        password: None,
        active: true,
        tier: 0,
        group: 0,
        fill: false,
        max_connections: 1,
        pipeline_depth: 1,
        retention_days: 0,
        cert_verification: nzbd_types::CertLevel::Strict,
    }];
    let ladder = Ladder::new(&servers);
    let not_blocked = |_: ServerId| false;
    let mut ctx = SelectionCtx {
        ladder: &ladder,
        attempts: &mut owner.attempts,
        is_blocked: &not_blocked,
        delegated: &owner.delegated,
        post_fetch_files: &owner.post_fetch_files,
        regular_downloads: owner.download_enabled,
        article_retries: 3,
        now_unix: 1_800_000_000,
        propagation_delay_secs: 0,
        soft_hold: false,
        rotate: 0,
    };
    let selected = next_for_server(&owner.state, &servers[0], &mut ctx)
        .lease
        .expect("the PP-only node should resume its recovery download");
    assert_eq!(selected.file, recovery_file);
}

#[tokio::test]
async fn failed_recovery_articles_return_to_verification_instead_of_disposition() {
    let (_tmp, mut owner, _epoch, mut messages) = guard_test_owner(Tuning::default());
    pump(&mut owner, &mut messages).await;
    let mut job = restarted_recovery_job(404, SegmentState::Failed);
    let mut failed_payload = job.files[0].clone();
    failed_payload.id = FileId(4041);
    failed_payload.filename = "release.part001.rar".into();
    failed_payload.is_par2 = false;
    failed_payload.segments[0].state = SegmentState::Failed;
    job.files.push(failed_payload);
    recompute_job_totals(&mut job);
    assert_eq!(final_status(&job).0, JobStatus::Failed);
    owner.state.jobs.push(job);

    let mut events = owner.events.subscribe();
    owner.check_job_complete(JobId(404));
    pump(&mut owner, &mut messages).await;
    assert_eq!(
        owner.state.job(JobId(404)).unwrap().status,
        JobStatus::Post {
            stage: PostStage::ParVerify
        }
    );
    assert!(no_completion_event(&mut events));
}

#[tokio::test]
async fn recovery_write_failure_stays_owned_by_the_active_post_task() {
    let (_tmp, mut owner, _epoch, mut messages) = guard_test_owner(Tuning::default());
    pump(&mut owner, &mut messages).await;
    owner.state.jobs.push(restarted_recovery_job(
        405,
        SegmentState::Done {
            offset: 0,
            len: 100,
            crc: 0,
        },
    ));
    owner
        .write_failures
        .insert(JobId(405), "write recovery volume: no space left".into());

    let mut events = owner.events.subscribe();
    owner.check_job_complete(JobId(405));
    pump(&mut owner, &mut messages).await;
    assert_eq!(
        owner.state.job(JobId(405)).unwrap().status,
        JobStatus::Post {
            stage: PostStage::ParVerify
        }
    );
    assert!(no_completion_event(&mut events));
}

#[test]
fn url_refetch_plan_keeps_first_fails_dupes() {
    let a = "https://indexer.example/getnzb/aaa.nzb&i=1&r=k".to_string();
    let b = "https://indexer.example/getnzb/bbb.nzb&i=1&r=k".to_string();
    let pending = vec![
        (JobId(4), a.clone()),
        (JobId(5), a.clone()),
        (JobId(7), b.clone()),
        (JobId(9), a.clone()),
    ];
    let (refetch, dupes) = plan_url_refetches(pending);
    assert_eq!(refetch, vec![(JobId(4), a), (JobId(7), b)]);
    assert_eq!(dupes, vec![(JobId(5), JobId(4)), (JobId(9), JobId(4))]);
}

#[test]
fn url_refetch_plan_empty() {
    let (refetch, dupes) = plan_url_refetches(Vec::new());
    assert!(refetch.is_empty());
    assert!(dupes.is_empty());
}

#[test]
fn scoped_url_refetch_preserves_distinct_manual_tokens_and_replays_same_token() {
    let url = "https://indexer.example/release.nzb".to_string();
    let request = |job, token: &str| UrlFetchRequest {
        job: JobId(job),
        url: url.clone(),
        request_token: Some(token.into()),
        manual_additional: true,
    };
    let (refetch, dupes) = plan_url_refetches_scoped(vec![
        request(10, "transfer-a"),
        request(11, "transfer-b"),
        request(12, "transfer-a"),
    ]);
    assert_eq!(refetch, vec![(JobId(10), url.clone()), (JobId(11), url)]);
    assert_eq!(dupes, vec![(JobId(12), JobId(10))]);
}

#[test]
fn scoped_url_refetch_does_not_collapse_manual_additional_without_token() {
    let url = "https://indexer.example/release.nzb".to_string();
    let request = |job| UrlFetchRequest {
        job: JobId(job),
        url: url.clone(),
        request_token: None,
        manual_additional: true,
    };
    let (refetch, dupes) = plan_url_refetches_scoped(vec![request(20), request(21)]);
    assert_eq!(refetch, vec![(JobId(20), url.clone()), (JobId(21), url)]);
    assert!(dupes.is_empty());
}

#[test]
fn token_replay_matches_terminal_job_without_overwriting_changed_submission() {
    let url = "https://indexer.example/release.nzb";
    let mut original = bare_job();
    original.id = JobId(31);
    original.status = JobStatus::Failed;
    original.params = vec![
        ("*URL".into(), url.into()),
        ("monarr-transfer".into(), "transfer-31".into()),
        ("monarr-delivery".into(), "1".into()),
        ("monarr-manual-additional".into(), "1".into()),
    ];
    let jobs = vec![original.clone()];
    let replay = find_url_request(&jobs, url, Some("transfer-31"), true).unwrap();
    assert_eq!(replay.id, JobId(31));
    assert!(url_request_matches(replay, url, true, true));
    assert!(!url_request_matches(
        replay,
        "https://indexer.example/another.nzb",
        true,
        true
    ));
    assert!(!url_request_matches(replay, url, false, true));
    assert!(find_url_request(&jobs, url, Some("new-transfer"), true).is_none());
}

#[test]
fn save_spacing_scales_with_save_cost() {
    // Fast volume: the historical 2 s cadence.
    assert_eq!(save_spacing(0), Duration::from_secs(2));
    assert_eq!(save_spacing(120), Duration::from_secs(2));
    // A 1 s save gets 10 s between saves (~10% duty cycle)…
    assert_eq!(save_spacing(1_000), Duration::from_secs(10));
    // …the field report's 65 s save gets spaced way out…
    assert_eq!(save_spacing(65_000), Duration::from_secs(300));
    // …and the cap holds even for absurd values.
    assert_eq!(save_spacing(u64::MAX / 20), Duration::from_secs(300));
}

#[test]
fn incomplete_probe_cannot_clear_a_forecast_hold() {
    let reading = DiskGuardReading {
        available_bytes: Some(10_000),
        all_roots_known: false,
        ..Default::default()
    };
    assert_eq!(
        disk_guard_decision(true, false, &reading, 100),
        (false, true)
    );
}

#[test]
fn incomplete_probe_cannot_clear_an_observed_write_latch() {
    let reading = DiskGuardReading {
        available_bytes: Some(10_000),
        all_roots_known: false,
        ..Default::default()
    };
    assert_eq!(disk_guard_decision(true, true, &reading, 100), (true, true));
}

#[test]
fn recovered_high_known_root_plus_unknown_sibling_retains_hold() {
    let reading = DiskGuardReading {
        available_bytes: Some(1_000_000_000),
        all_roots_known: false,
        ..Default::default()
    };
    assert_eq!(
        disk_guard_decision(true, false, &reading, 100),
        (false, true)
    );
}

#[test]
fn complete_high_probe_releases_a_forecast_hold() {
    let reading = DiskGuardReading {
        available_bytes: Some(101),
        all_roots_known: true,
        ..Default::default()
    };
    assert_eq!(
        disk_guard_decision(true, false, &reading, 100),
        (false, false)
    );
}

#[test]
fn write_latch_requires_twice_the_floor_from_a_complete_probe() {
    let below_clear = DiskGuardReading {
        available_bytes: Some(199),
        all_roots_known: true,
        ..Default::default()
    };
    let at_clear = DiskGuardReading {
        available_bytes: Some(200),
        all_roots_known: true,
        ..Default::default()
    };
    assert_eq!(
        disk_guard_decision(true, true, &below_clear, 100),
        (true, true)
    );
    assert_eq!(
        disk_guard_decision(true, true, &at_clear, 100),
        (false, false)
    );
}

#[tokio::test]
async fn clearing_disk_guard_wakes_parked_connection_tasks() {
    let tuning = Tuning {
        min_free_disk_bytes: 100,
        ..Tuning::default()
    };
    let (_tmp, mut owner, mut epoch, mut messages) = guard_test_owner(tuning);
    pump(&mut owner, &mut messages).await;
    assert!(owner.disk_low, "a configured floor starts fail-safe");
    let _ = epoch.borrow_and_update();
    assert!(!epoch.has_changed().unwrap());

    owner.disk_guard.store(Arc::new(DiskGuardReading {
        available_bytes: Some(101),
        all_roots_known: true,
        ..Default::default()
    }));
    owner.update_disk_guard();

    assert!(!owner.disk_low, "the complete high probe clears the hold");
    assert!(
        epoch.has_changed().unwrap(),
        "a task parked on the work epoch must be woken"
    );
}

#[tokio::test]
async fn clearing_quota_guard_wakes_parked_connection_tasks() {
    let tuning = Tuning {
        daily_quota_bytes: 1,
        ..Tuning::default()
    };
    let (_tmp, mut owner, mut epoch, mut messages) = guard_test_owner(tuning);
    pump(&mut owner, &mut messages).await;
    owner.quota_reached = true;
    let _ = epoch.borrow_and_update();
    assert!(!epoch.has_changed().unwrap());

    // The empty volume book is deterministically below the configured
    // quota, directly exercising the rollover/recovery transition.
    owner.update_quota_guard();
    pump(&mut owner, &mut messages).await;

    assert!(
        !owner.quota_reached,
        "the fresh quota period clears the hold"
    );
    assert!(
        epoch.has_changed().unwrap(),
        "a task parked on the work epoch must be woken"
    );
}

// Lifecycle regressions retained across the asynchronous owner split.
#[tokio::test]
async fn url_bulk_preparation_does_not_block_live_controls() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    let post = nzbd_nserv::build_post("bulk", &[("payload.bin", vec![7; 100])], 100);
    let id = owner.state.admit_url(
        "bulk".into(),
        "http://127.0.0.1:9/source.nzb",
        Some("before".into()),
        0,
    );
    let origin = owner.state.job(id).unwrap().clone();
    let identity = nzbd_state::artifacts::url_source_identity(&origin).unwrap();
    persist_snapshot_for_test(&mut owner, &mut messages).await;
    owner
        .url_resolutions
        .cache_source(&origin, post.nzb.as_bytes())
        .unwrap();
    let gate = Arc::new((std::sync::Barrier::new(2), std::sync::Barrier::new(2)));
    *crate::URL_PREPARATION_GATE.lock().unwrap() = Some((identity, gate.clone()));
    let state = owner.state_dir.clone();
    let dest = owner.dest_dir.clone();
    drop(owner);
    let engine = crate::Engine::spawn(crate::EngineConfig::single_node(
        Vec::new(),
        state,
        dest,
        Tuning::default(),
        None,
    ))
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while crate::URL_PREPARATION_GATE.lock().unwrap().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("recovered URL must reach off-actor bulk preparation");
    let entered = gate.clone();
    tokio::task::spawn_blocking(move || entered.0.wait())
        .await
        .unwrap();
    let changed = tokio::time::timeout(Duration::from_secs(2), async {
        Ok::<_, crate::EngineError>([
            engine.set_priority(id, 99).await?,
            engine.set_category(id, None).await?,
            engine.set_keep_on_failure(id, true).await?,
            engine.pause_job(id).await?,
            engine.resume_job(id).await?,
            engine.pause_job(id).await?,
        ])
    })
    .await;
    let release = gate.clone();
    tokio::task::spawn_blocking(move || release.1.wait())
        .await
        .unwrap();
    let changed = changed
        .expect("bulk preparation must leave the queue actor responsive")
        .unwrap();
    assert_eq!(
        changed, [true; 6],
        "priority/category/Keep/pause/resume/pause must all succeed"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if engine
                .export_job(id)
                .await
                .unwrap()
                .is_some_and(|job| job.status == JobStatus::Paused && !job.files.is_empty())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let job = engine.export_job(id).await.unwrap().unwrap();
    assert_eq!(job.priority, 99);
    assert!(job.category.is_none());
    assert!(job
        .params
        .iter()
        .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1"));
    engine.shutdown().await;
}

#[tokio::test]
async fn normal_delete_tombstone_survives_snapshot_failure_without_reclaiming_consumer_custody() {
    for transferred in [false, true] {
        let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
        let token = if transferred {
            "consumer-owned"
        } else {
            "pending-request"
        };
        let (reply, result) = oneshot::channel();
        owner.on_command(QueueCommand::AddUrl {
            name: "request".into(),
            url: "http://127.0.0.1:9/source.nzb".into(),
            category: None,
            priority: 0,
            dupe: None,
            paused: false,
            params: vec![("monarr-transfer".into(), token.into())],
            reply,
        });
        pump(&mut owner, &mut messages).await;
        let (id, _) = result.await.unwrap().unwrap();
        if transferred {
            owner.state.job_mut(id).unwrap().status = JobStatus::Completed;
            let receipt = &mut owner.state.url_admission_receipts[0];
            receipt.custody_state = "curator".into();
            receipt.outcome = "completed".into();
            receipt.terminal_outcome = Some("SUCCESS".into());
            receipt.publication_path = Some("/verified/publication".into());
            persist_snapshot_for_test(&mut owner, &mut messages).await;
        }
        std::fs::create_dir(owner.state_dir.join("queue.json.tmp")).unwrap();
        let (reply, result) = oneshot::channel();
        owner.delete_job_then(id, false, move |_, ok| {
            let _ = reply.send(ok);
        });
        pump(&mut owner, &mut messages).await;
        assert!(
            result.await.unwrap(),
            "exact retirement tombstone makes cancellation durable despite snapshot I/O failure"
        );
        assert!(owner.state.jobs.is_empty());
        let receipt = &owner.state.url_admission_receipts[0];
        assert_eq!(
            receipt.outcome,
            if transferred { "completed" } else { "deleted" }
        );
        std::fs::remove_dir(owner.state_dir.join("queue.json.tmp")).unwrap();
        let state = owner.state_dir.clone();
        let dest = owner.dest_dir.clone();
        drop(owner);
        let engine = crate::Engine::spawn(crate::EngineConfig::single_node(
            Vec::new(),
            state,
            dest,
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        assert!(
            engine.snapshot().jobs.is_empty(),
            "old queue snapshot cannot resurrect the retired admission"
        );
        let receipt = engine.url_admission(token).await.unwrap().unwrap();
        assert_eq!(
            receipt.outcome,
            if transferred { "completed" } else { "deleted" }
        );
        if transferred {
            assert_eq!(receipt.custody_state, "curator");
            assert_eq!(
                receipt.publication_path.as_deref(),
                Some("/verified/publication")
            );
            assert_eq!(receipt.terminal_outcome.as_deref(), Some("SUCCESS"));
        }
        engine.shutdown().await;
    }
}

#[tokio::test]
async fn prepared_url_resolution_preserves_later_clear_rename_pause_priority_and_keep() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    let post = nzbd_nserv::build_post("resolved", &[("payload.bin", vec![7; 100])], 100);
    let parsed = nzbd_nzb::parse(post.nzb.as_bytes()).unwrap();
    let id = owner.state.admit_url(
        "original".into(),
        "http://example.test/source.nzb",
        Some("old-category".into()),
        0,
    );
    mark_new_lifecycle_cohort(owner.state.job_mut(id).unwrap());
    owner.sync_lifecycle_timings();
    let origin = owner.state.job(id).unwrap().clone();
    let identity = nzbd_state::artifacts::url_source_identity(&origin).unwrap();
    owner
        .url_resolutions
        .cache_source(&origin, post.nzb.as_bytes())
        .unwrap();
    owner
        .url_resolutions
        .reserve_files(&origin, 1, parsed.files.len() as u32)
        .unwrap();
    let mut staging = QueueState::default();
    staging.jobs.push(origin.clone());
    staging.complete_url_fetch(id, &parsed, true);
    let prepared = owner
        .url_resolutions
        .stage(&origin, &staging.jobs[0])
        .unwrap();
    let current = owner.state.job_mut(id).unwrap();
    current.category = None;
    current.priority = 99;
    current.name = "user rename".into();
    current.dir_name = "user-private-name".into();
    current.params.extend([
        ("*AddPaused".into(), "yes".into()),
        ("*Runner:KeepOnFailure".into(), "1".into()),
    ]);
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::CompleteUrlFetch {
        job: id,
        source_identity: identity,
        prepared: Box::new(prepared),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap().unwrap());
    let job = owner.state.job(id).unwrap();
    assert!(job.category.is_none());
    assert_eq!(job.priority, 99);
    assert_eq!(job.name, "user rename");
    assert_eq!(job.dir_name, "user-private-name");
    assert_eq!(job.status, JobStatus::Paused);
    let timing = job.lifecycle_ledger().unwrap().snapshot();
    assert_eq!(
        timing
            .intervals
            .iter()
            .filter(|interval| interval.stage == nzbd_types::LifecycleStage::UrlFetch)
            .count(),
        1
    );
    assert!(timing.intervals.iter().any(|interval| interval.stage
        == nzbd_types::LifecycleStage::UrlFetch
        && interval.state == nzbd_types::LifecycleIntervalState::Completed));
    assert!(timing.intervals.iter().any(|interval| interval.stage
        == nzbd_types::LifecycleStage::Queue
        && interval.state == nzbd_types::LifecycleIntervalState::Running));
    assert!(job
        .params
        .iter()
        .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1"));
}

#[tokio::test]
async fn storage_retry_record_failure_keeps_provider_floor_in_live_fallback() {
    let temporary = tempfile::tempdir().unwrap();
    let state = temporary.path().join("state");
    let dest = temporary.path().join("dest");
    let engine = crate::Engine::spawn(crate::EngineConfig::single_node(
        Vec::new(),
        state.clone(),
        dest,
        Tuning::default(),
        None,
    ))
    .await
    .unwrap();
    let mut job = pending_job(87);
    job.kind = JobKind::Url;
    job.status = JobStatus::Fetching;
    job.files.clear();
    job.params
        .push(("*URL".into(), "http://example.test/source.nzb".into()));
    engine.import_job(job.clone(), false, false).await.unwrap();
    let identity = nzbd_state::artifacts::url_source_identity(&job).unwrap();
    std::fs::create_dir(state.join("url-resolutions/retries").join(&identity)).unwrap();
    let floor = unix_now() + 600;
    let caller = engine.clone();
    let retrying = tokio::spawn(async move {
        let mut fallback = nzbd_state::artifacts::UrlStorageRetry::default();
        caller
            .wait_url_storage_retry_after(
                &job,
                &identity,
                "storage retry record cannot be written",
                &mut fallback,
                Some(floor),
            )
            .await;
        fallback
    });
    tokio::time::timeout(Duration::from_secs(3), async {
            loop { let job = engine.export_job(JobId(87)).await.unwrap().unwrap(); if job.params.iter().any(|(key,value)| key == crate::fetch::NEXT_RETRY_PARAM && value.parse::<i64>().ok().is_some_and(|deadline| deadline >= floor)) { break; } tokio::task::yield_now().await; }
        }).await.expect("failed retry-record persistence must still honor the provider floor in the live scheduled progress");
    engine.cancel.cancel();
    let fallback = retrying.await.unwrap();
    assert_eq!(fallback.attempts, 1);
    assert!(fallback.next_retry_at >= floor);
    engine.shutdown().await;
}

#[tokio::test]
async fn transient_cached_nzb_read_recovers_in_process_without_becoming_failed() {
    let (temporary, mut owner, _adapter, mut messages) = control_test_owner();
    let post = nzbd_nserv::build_post("read-recovery", &[("payload.bin", vec![7; 100])], 100);
    let id = owner.state.admit_url(
        "read-recovery".into(),
        "http://127.0.0.1:9/source.nzb",
        None,
        0,
    );
    let origin = owner.state.job(id).unwrap().clone();
    let identity = nzbd_state::artifacts::url_source_identity(&origin).unwrap();
    persist_snapshot_for_test(&mut owner, &mut messages).await;
    owner
        .url_resolutions
        .cache_source(&origin, post.nzb.as_bytes())
        .unwrap();
    let source = owner
        .state_dir
        .join("url-resolutions/sources")
        .join(&identity);
    let saved = temporary.path().join("saved-source");
    std::fs::rename(&source, &saved).unwrap();
    std::fs::create_dir(&source).unwrap();
    let state = owner.state_dir.clone();
    let dest = owner.dest_dir.clone();
    drop(owner);
    let engine = crate::Engine::spawn(crate::EngineConfig::single_node(
        Vec::new(),
        state,
        dest,
        Tuning::default(),
        None,
    ))
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let job = engine.export_job(id).await.unwrap().unwrap();
            if job.params.iter().any(|(key, value)| {
                key == crate::fetch::LAST_ERROR_PARAM && value.contains("cached NZB source read")
            }) {
                assert_eq!(job.status, JobStatus::Fetching);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    std::fs::remove_dir(&source).unwrap();
    std::fs::rename(saved, source).unwrap();
    tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            let job = engine.export_job(id).await.unwrap().unwrap();
            assert_ne!(
                job.status,
                JobStatus::Failed,
                "transient read I/O must not be converted into permanent fetch failure"
            );
            if job.status == JobStatus::Queued && !job.files.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("cached source must resume after its real persisted 30-second storage backoff");
    engine.shutdown().await;
}

#[tokio::test]
async fn cached_url_resolution_survives_failed_snapshot_and_offline_restart() {
    let (tmp, mut owner, _adapter, mut messages) = control_test_owner();
    let post = nzbd_nserv::build_post("offline", &[("payload.bin", vec![7; 100])], 100);
    let parsed = nzbd_nzb::parse(post.nzb.as_bytes()).unwrap();
    let id = owner
        .state
        .admit_url("offline".into(), "http://127.0.0.1:9/expired.nzb", None, 0);
    let origin = owner.state.job(id).unwrap().clone();
    let identity = nzbd_state::artifacts::url_source_identity(&origin).unwrap();
    persist_snapshot_for_test(&mut owner, &mut messages).await;
    let timing_before = owner.state.job(id).unwrap().lifecycle_ledger().unwrap();
    owner
        .url_resolutions
        .cache_source(&origin, post.nzb.as_bytes())
        .unwrap();
    owner
        .url_resolutions
        .reserve_files(&origin, 1, parsed.files.len() as u32)
        .unwrap();
    let mut staging = QueueState::default();
    staging.jobs.push(origin.clone());
    staging.complete_url_fetch(id, &parsed, true);
    let prepared = owner
        .url_resolutions
        .stage(&origin, &staging.jobs[0])
        .unwrap();
    let blocked = owner.state_dir.join("queue.json.tmp");
    std::fs::create_dir(&blocked).unwrap();
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::CompleteUrlFetch {
        job: id,
        source_identity: identity.clone(),
        prepared: Box::new(prepared),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(matches!(
        result.await.unwrap(),
        Err(crate::EngineError::AdmissionNotDurable)
    ));
    assert_eq!(owner.state.job(id).unwrap().status, JobStatus::Fetching);
    owner.sync_lifecycle_timings();
    assert_eq!(
        owner.state.job(id).unwrap().lifecycle_ledger().unwrap(),
        timing_before,
        "failed prepared apply must not commit an ephemeral queue stage or reopen a closed observation"
    );
    assert_eq!(owner.url_resolutions.pending(100).unwrap().len(), 1);
    std::fs::remove_dir(blocked).unwrap();
    let state_dir = owner.state_dir.clone();
    let dest_dir = owner.dest_dir.clone();
    drop(owner);
    let engine = crate::Engine::spawn(crate::EngineConfig::single_node(
        Vec::new(),
        state_dir,
        dest_dir,
        Tuning::default(),
        None,
    ))
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if engine
                .export_job(id)
                .await
                .unwrap()
                .is_some_and(|job| job.status == JobStatus::Queued && !job.files.is_empty())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("restart must use the durable NZB even while the original URL is offline");
    let files = engine.export_job(id).await.unwrap().unwrap().files;
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].id, FileId(1));
    engine.shutdown().await;
    assert!(tmp
        .path()
        .join("state/url-resolutions/acknowledged")
        .is_dir());
}

#[tokio::test]
async fn url_progress_storage_failure_is_error_and_preserves_fetching_for_retry() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    let id = owner.state.admit_url(
        "progress".into(),
        "http://example.test/progress.nzb",
        None,
        0,
    );
    persist_snapshot_for_test(&mut owner, &mut messages).await;
    let before = owner.state.job(id).unwrap().clone();
    let blocked = owner.state_dir.join("queue.json.tmp");
    std::fs::create_dir(&blocked).unwrap();
    let progress = crate::fetch::FetchRetryState {
        attempts: 1,
        next_retry_at: Some(999),
        blocked_since: Some(1),
        last_error: Some("retryable server error".into()),
    };
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::UpdateUrlFetchProgress {
        job: id,
        progress: progress.clone(),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(
        matches!(
            result.await.unwrap(),
            Err(crate::EngineError::AdmissionNotDurable)
        ),
        "storage failure must never look like user cancellation"
    );
    assert_eq!(
        serde_json::to_value(owner.state.job(id)).unwrap(),
        serde_json::to_value(Some(&before)).unwrap()
    );
    std::fs::remove_dir(blocked).unwrap();
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::UpdateUrlFetchProgress {
        job: id,
        progress,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap().unwrap());
}

#[tokio::test]
async fn consumer_receipt_refresh_pins_publication_while_source_progress_remains_mutable() {
    let (_tmp, mut owner, _adapter, _messages) = control_test_owner();
    let mut stale = pending_job(78);
    stale.params.extend([
        ("monarr-transfer".into(), "request-78".into()),
        ("Delivery:Custody".into(), "runner_owned".into()),
        ("Delivery:SourceCleanup".into(), "removed".into()),
    ]);
    owner.state.jobs.push(stale);
    owner
        .state
        .url_admission_receipts
        .push(UrlAdmissionReceipt {
            protocol_version: 1,
            request_token: "request-78".into(),
            submission_sha256: "hash".into(),
            job_id: 78,
            generation: "admission-78".into(),
            runner_instance_id: "runner".into(),
            terminal_outcome: Some("SUCCESS".into()),
            outcome: "completed".into(),
            failure_stage: None,
            failure_cause: None,
            custody_state: "curator".into(),
            keep_source_on_failure: true,
            source_cleanup_status: Some("pending".into()),
            publication_path: Some("/verified/publication".into()),
            source_artifact_id: Some("source-78".into()),
            source_generation: Some("generation-78".into()),
            failure_reason: None,
            fetch_cause: None,
            blames_release: None,
            salvage: None,
        });
    owner.state.sync_url_admission_receipts();
    let receipt = &owner.state.url_admission_receipts[0];
    assert_eq!(receipt.custody_state, "curator");
    assert_eq!(receipt.outcome, "completed");
    assert_eq!(receipt.terminal_outcome.as_deref(), Some("SUCCESS"));
    assert_eq!(
        receipt.publication_path.as_deref(),
        Some("/verified/publication")
    );
    assert_eq!(receipt.source_cleanup_status.as_deref(), Some("removed"));
    assert!(receipt.keep_source_on_failure);
}

fn catalog_receipt(token: &str, id: u32) -> UrlAdmissionReceipt {
    serde_json::from_value(serde_json::json!({"protocolVersion":1,"requestToken":token,"submissionSha256":"catalog-fingerprint","jobId":id,"generation":format!("generation-{id}"),"runnerInstanceId":"cluster-consumer"})).unwrap()
}

#[tokio::test]
async fn unaccepted_admission_read_uses_only_pending_index_and_refuses_overflow() {
    let (_temporary, mut owner, _adapter, mut messages) = control_test_owner();
    owner.state.url_admission_receipts = (1..=1000)
        .map(|id| catalog_receipt(&format!("accepted-history-{id}"), id))
        .collect();
    let pending = catalog_receipt("new-pending", 1001);
    owner.state.url_admission_receipts.push(pending.clone());
    owner
        .unaccepted_admissions
        .insert(pending.request_token.clone(), pending.clone());
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::UnacceptedUrlAdmissions { limit: 100, reply });
    pump(&mut owner, &mut messages).await;
    let rows = result.await.unwrap().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].request_token, "new-pending");
    assert!(owner.adopt_url_admissions(vec![pending]));
    assert!(owner.unaccepted_admissions.is_empty());
    for id in 1002..1103 {
        let row = catalog_receipt(&format!("pending-{id}"), id);
        owner
            .unaccepted_admissions
            .insert(row.request_token.clone(), row);
    }
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::UnacceptedUrlAdmissions { limit: 100, reply });
    pump(&mut owner, &mut messages).await;
    assert!(
        result.await.unwrap().is_err(),
        "overflow cannot silently omit pending admissions"
    );
}

#[tokio::test]
async fn first_authority_upgrade_preserves_retired_snapshot_receipts_without_resurrection() {
    let (_temporary, mut owner, _adapter, mut messages) =
        control_test_owner_with_persistence(false, None);
    assert!(owner.state.jobs.is_empty());
    assert!(owner.state.url_admission_receipts.is_empty());
    let mut receipt = catalog_receipt("retired-before-catalog-upgrade", 88);
    receipt.custody_state = "curator".into();
    receipt.publication_path = Some("/publication/already-imported".into());
    receipt.terminal_outcome = Some("SUCCESS".into());
    receipt.outcome = "completed".into();
    let mut old = owner.state.to_doc();
    old.url_admission_receipts.push(receipt.clone());
    SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .save(&old)
        .unwrap();
    let (reply, result) = oneshot::channel();
    owner.adopt_authority_then(move |_, value| {
        let _ = reply.send(value);
    });
    pump(&mut owner, &mut messages).await;
    result.await.unwrap().unwrap();
    assert!(owner.state.jobs.is_empty());
    assert_eq!(owner.state.url_admission_receipts.len(), 1);
    assert_eq!(
        crate::admissions::identity(&owner.state.url_admission_receipts[0]),
        crate::admissions::identity(&receipt)
    );
    assert_eq!(
        owner.state.url_admission_receipts[0].publication_path,
        receipt.publication_path
    );
    assert!(owner.state.next_job_id >= 88);
    assert_eq!(
        SnapshotStore::open(&owner.state_dir)
            .unwrap()
            .load()
            .unwrap()
            .unwrap()
            .url_admission_receipts
            .len(),
        1
    );
    assert!(
        owner.authoritative_admissions.is_empty(),
        "legacy snapshot projection is not yet a replicated catalog acceptance"
    );
    owner.admission_adoption = Some(HashSet::new());
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::FinishUrlAdmissionAdoption { reply });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    assert!(owner.state.url_admission_receipts.is_empty());
    // Even if an old shared projection reappears, the completed catalog
    // has proved this token absent and is authoritative for this owner.
    SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .save(&old)
        .unwrap();
    owner.persist = false;
    let (reply, result) = oneshot::channel();
    owner.adopt_authority_then(move |_, value| {
        let _ = reply.send(value);
    });
    pump(&mut owner, &mut messages).await;
    result.await.unwrap().unwrap();
    assert!(owner.state.url_admission_receipts.is_empty());
    assert!(SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .load()
        .unwrap()
        .unwrap()
        .url_admission_receipts
        .is_empty());
    assert!(owner.state.next_job_id >= 88);
}

#[tokio::test]
async fn catalog_adoption_replaces_only_unaccepted_identity_and_rolls_back_on_fence() {
    let (temporary, mut owner, _adapter, mut messages) = control_test_owner();
    owner.state.url_admission_receipts =
        vec![catalog_receipt("token", 4), catalog_receipt("orphan", 5)];
    owner.admission_adoption = Some(HashSet::new());
    let accepted = catalog_receipt("token", 8);
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::AdoptUrlAdmissions {
        rows: vec![accepted.clone()],
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    assert_eq!(
        owner
            .state
            .url_admission_receipts
            .iter()
            .find(|row| row.request_token == "token")
            .unwrap()
            .job_id,
        8
    );
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::FinishUrlAdmissionAdoption { reply });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    assert_eq!(owner.state.url_admission_receipts.len(), 1);
    assert!(owner.state.next_job_id >= 8);
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::AdoptUrlAdmissions {
        rows: vec![catalog_receipt("token", 9)],
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(!result.await.unwrap(), "accepted tuple cannot fork");
    let before = serde_json::to_value(&owner.state.url_admission_receipts).unwrap();
    owner.persist_guard = Some(Arc::new(|| false));
    for _ in 0..2 {
        let (reply, result) = oneshot::channel();
        owner.on_command(QueueCommand::AdoptUrlAdmissions {
            rows: vec![catalog_receipt("later", 99)],
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(!result.await.unwrap());
        assert_eq!(
            serde_json::to_value(&owner.state.url_admission_receipts).unwrap(),
            before
        );
        assert!(owner.state.next_job_id < 99);
    }
    let persisted = SnapshotStore::open(&temporary.path().join("state"))
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(persisted.url_admission_receipts).unwrap(),
        before
    );
}

#[tokio::test]
async fn orphan_receipt_discard_never_restores_or_changes_an_authoritative_id_collision() {
    let (_temporary, mut owner, _adapter, mut messages) = control_test_owner();
    let expected = catalog_receipt("unaccepted", 6);
    owner.state.url_admission_receipts.push(expected.clone());
    let mut occupied = pending_job(6);
    occupied.params.push((
        "monarr-transfer".into(),
        "different-accepted-request".into(),
    ));
    occupied.priority = 88;
    owner.state.jobs.push(occupied.clone());
    owner.state.next_job_id = 90;
    // A metadata reservation for the orphan must disappear with its
    // receipt, without removing a different reservation at the same ID.
    let pending_id = owner.state.reserve_torrent_admission(
        nzbd_types::TorrentSource::Metainfo,
        AddOpts {
            params: vec![("monarr-transfer".into(), "unaccepted".into())],
            ..Default::default()
        },
    );
    owner.state.pending_admissions.last_mut().unwrap().job_id = JobId(6);
    owner
        .pending_sources
        .write(JobId(6), b"orphan descriptor")
        .unwrap();
    owner.state.next_job_id = 90;
    assert!(pending_id.0 > 90);
    // Snapshot bookkeeping can observe an existing job; establish that
    // observation before checking that discard preserves its contents.
    owner.sync_lifecycle_timings();
    occupied = owner.state.job(JobId(6)).unwrap().clone();
    assert!(owner.can_discard_url_admission(&expected));
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::DiscardUrlAdmission {
        expected: Box::new(expected),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    assert!(owner.state.url_admission_receipts.is_empty());
    assert!(owner.state.pending_admissions.is_empty());
    assert!(owner.pending_sources.read(JobId(6)).is_err());
    assert_eq!(
        serde_json::to_value(owner.state.job(JobId(6)).unwrap()).unwrap(),
        serde_json::to_value(occupied).unwrap()
    );
    assert_eq!(owner.state.next_job_id, 90);
    let accepted = catalog_receipt("accepted", 6);
    assert!(owner.adopt_url_admissions(vec![accepted.clone()]));
    assert!(!owner.can_discard_url_admission(&accepted));
}

#[tokio::test]
async fn public_delete_cancels_pending_verification_and_retains_payload() {
    let (_temporary, mut owner, adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    let mut row = pending_job(41);
    row.status = JobStatus::Paused;
    let source = owner.dest_dir.join(&row.dir_name);
    let payload = source.join("payload.bin");
    owner
        .artifacts
        .allocate_for_delivery(41, &owner.dest_dir, &source, true, false)
        .unwrap();
    std::fs::write(&payload, b"retained download").unwrap();
    owner.state.jobs.push(row);
    owner.queue_verification(JobId(41));
    pump(&mut owner, &mut messages).await;
    let cancellation = owner.verification[&JobId(41)].cancel.clone();
    owner.publish_snapshot(0);
    let tracker = owner.tracker.clone();
    let engine = crate::EngineHandle {
        artifacts: owner.artifacts.clone(),
        url_resolutions: owner.url_resolutions.clone(),
        url_dest_dir: owner.dest_dir.clone(),
        import_state_dir: owner.state_dir.clone(),
        url_history: owner.history.clone(),
        url_outbox_external: owner.persist_guard.is_some(),
        owner_persist_guard: owner.persist_guard.clone(),
        native_seed_recovery_cursor: Default::default(),
        hold_probes: Default::default(),
        recovery_cursor: Default::default(),
        cmd_tx: owner.engine_tx.clone(),
        shared: owner.shared.clone(),
        events: owner.events.clone(),
        cancel: owner.cancel.clone(),
        tracker: tracker.clone(),
        backend_adapter: Arc::new(std::sync::Mutex::new(Some(adapter))),
        budget_tracker: Arc::new(crate::pool::BudgetTracker::new(0)),
        url_fetches: Arc::new(std::sync::Mutex::new(HashSet::new())),
        post_scope: None,
        queue_changes: owner.epoch_tx.subscribe(),
        owner_heartbeat: owner.owner_heartbeat.clone(),
        shutdown_complete: owner.shutdown_complete.clone(),
    };
    let operation = tokio::spawn(async move {
        // Payload export stays fenced; metadata is sufficient for delete/undo.
        assert!(matches!(
            engine.export_job(JobId(41)).await,
            Err(crate::EngineError::Lifecycle(_))
        ));
        let metadata = engine.queue_job_metadata(JobId(41)).await.unwrap().unwrap();
        assert_eq!(metadata.name, "job-41");
        assert_eq!(metadata.files.len(), 1);
        assert!(engine.delete_job(JobId(41), false).await.unwrap());
        assert!(!engine.delete_job(JobId(41), false).await.unwrap());
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !operation.is_finished() {
            tokio::select! {
                Some(message) = messages.recv() => owner.on_msg(message),
                _ = tokio::time::sleep(Duration::from_millis(1)) => {},
            }
        }
        operation.await.unwrap();
    })
    .await
    .expect("public Delete must reach the owner while verification is pending");
    pump(&mut owner, &mut messages).await;
    assert!(cancellation.is_cancelled());
    assert!(owner.state.job(JobId(41)).is_none());
    assert!(!owner.verification.contains_key(&JobId(41)));
    assert_eq!(std::fs::read(payload).unwrap(), b"retained download");
    assert!(SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .load()
        .unwrap()
        .unwrap()
        .jobs
        .is_empty());
}

#[tokio::test]
async fn automatic_hold_recovery_bounds_each_pass_and_rotates_all_due_jobs() {
    // This test drives each bounded pass itself. Start the genuine owner
    // after setup, without also starting Engine::spawn's periodic recovery
    // task, which could otherwise release fixtures while they are imported.
    let (_temporary, mut owner, adapter, mut messages) = control_test_owner();
    pump(&mut owner, &mut messages).await;
    for id in 101..122 {
        let mut row = pending_job(id);
        mark_new_lifecycle_cohort(&mut row);
        row.status = JobStatus::Paused;
        row.dir_name = format!("allocation-{id}");
        row.set_control(&nzbd_types::JobControl {
            version: 1,
            revision: "1".into(),
            lifecycle: "held".into(),
            cause: "allocation".into(),
            stage: "download_write".into(),
            retry_policy: nzbd_types::HoldCause::Allocation.policy().as_str().into(),
            message: "allocation unavailable".into(),
            instance: format!("held-{id}"),
            previous_status: Some(JobStatus::Queued),
            manual_pause: false,
            retry_attempt: 1,
            next_retry_at_unix: Some(0),
            blocked_since_unix: Some(0),
            released_at_unix: None,
            deadline_in_s: None,
            deadline_unix: None,
        });
        let root = owner.dest_dir.clone();
        owner
            .artifacts
            .allocate_for_delivery(id, &root, &root.join(&row.dir_name), true, false)
            .unwrap();
        owner.state.jobs.push(row);
    }
    persist_snapshot_for_test(&mut owner, &mut messages).await;
    owner.publish_snapshot(0);
    let tracker = owner.tracker.clone();
    let engine = crate::EngineHandle {
        artifacts: owner.artifacts.clone(),
        url_resolutions: owner.url_resolutions.clone(),
        url_dest_dir: owner.dest_dir.clone(),
        import_state_dir: owner.state_dir.clone(),
        url_history: owner.history.clone(),
        url_outbox_external: owner.persist_guard.is_some(),
        owner_persist_guard: owner.persist_guard.clone(),
        native_seed_recovery_cursor: Default::default(),
        hold_probes: Default::default(),
        recovery_cursor: Default::default(),
        cmd_tx: owner.engine_tx.clone(),
        shared: owner.shared.clone(),
        events: owner.events.clone(),
        cancel: owner.cancel.clone(),
        tracker: tracker.clone(),
        backend_adapter: Arc::new(std::sync::Mutex::new(Some(adapter))),
        budget_tracker: Arc::new(crate::pool::BudgetTracker::new(0)),
        url_fetches: Arc::new(std::sync::Mutex::new(HashSet::new())),
        post_scope: None,
        queue_changes: owner.epoch_tx.subscribe(),
        owner_heartbeat: owner.owner_heartbeat.clone(),
        shutdown_complete: owner.shutdown_complete.clone(),
    };
    tracker.spawn(owner.run(messages));
    tracker.close();
    assert!(engine
        .set_delegated(JobId(121), Some("lease-worker".into()))
        .await
        .unwrap());
    for expected in [8, 8, 4] {
        assert_eq!(
            crate::hold_recovery::pass(&engine, false).await.unwrap(),
            expected
        );
    }
    assert!(engine
        .snapshot()
        .jobs
        .iter()
        .filter(|row| row.id != JobId(121))
        .all(|row| row
            .control
            .as_ref()
            .is_none_or(|control| control.lifecycle != "held")));
    assert_eq!(
        export_job_ready_for_test(&engine, JobId(121))
            .await
            .unwrap()
            .unwrap()
            .control()
            .unwrap()
            .lifecycle,
        "held",
        "delegated holds belong to the live execution lane"
    );
    engine.shutdown().await;
}

#[tokio::test]
async fn unified_post_release_recovers_exact_retry_and_moved_source_but_preserves_review() {
    use nzbd_state::artifacts::DeliveryFileOutcome;
    for phase in [
        "repeated_enospc",
        "source_already_moved",
        "review",
        "delete",
    ] {
        let temporary = tempfile::tempdir().unwrap();
        let state = temporary.path().join("state");
        let destination = temporary.path().join("working");
        std::fs::create_dir_all(destination.as_path()).unwrap();
        let engine = crate::Engine::spawn(crate::EngineConfig::single_node(
            Vec::new(),
            state,
            destination.clone(),
            Tuning::default(),
            None,
        ))
        .await
        .unwrap();
        let mut row = pending_job(92);
        row.status = JobStatus::PostQueued;
        row.delivery_lifecycle = true;
        row.dir_name = "owned-source".into();
        mark_new_lifecycle_cohort(&mut row);
        let inventory = engine.artifacts();
        let source = destination.join(&row.dir_name);
        inventory
            .allocate_for_delivery(92, &destination, &source, true, true)
            .unwrap();
        std::fs::write(source.join("episode.mkv"), vec![7; 131072]).unwrap();
        inventory
            .finish(92, &source, &destination, "active")
            .unwrap();
        engine.import_job(row, false, false).await.unwrap();
        export_job_ready_for_test(&engine, JobId(92))
            .await
            .unwrap()
            .unwrap();
        let target_root = temporary.path().join("completed");
        std::fs::create_dir(&target_root).unwrap();
        let target = target_root.join("release");
        let original = inventory.for_job(92).unwrap().unwrap();
        let selected = original.files.clone();
        let outcomes = vec![DeliveryFileOutcome {
            source_path: "episode.mkv".into(),
            publication_path: Some("episode.mkv".into()),
            classification: "media".into(),
            outcome: "published".into(),
            reason: None,
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        }];
        if phase == "delete" {
            let retained = inventory
                .finish(92, &source, &destination, "retained")
                .unwrap();
            inventory
                .request_delete(&retained.id, retained.revision, "pending-delete", 60)
                .unwrap();
        } else {
            let _fault = inventory
                .inject_relocation_test_fault(
                    92,
                    if phase == "source_already_moved" {
                        "before_commit"
                    } else {
                        "copy_chunk"
                    },
                    libc::ENOSPC,
                )
                .unwrap();
            let failed = if phase == "source_already_moved" {
                inventory.relocate(92, &target)
            } else {
                inventory.publish_selected_for_delivery_checked(
                    92,
                    &target,
                    selected.clone(),
                    outcomes.clone(),
                    &|| Ok(()),
                )
            };
            assert!(failed.is_err());
            if phase == "source_already_moved" {
                assert!(!source.exists());
                assert!(target.join("episode.mkv").exists());
            }
        }
        engine
            .hold_job(
                JobId(92),
                nzbd_types::HoldCause::Capacity,
                "move",
                "publication volume full (os error 28)",
            )
            .await
            .unwrap();
        let revision = export_job_ready_for_test(&engine, JobId(92))
            .await
            .unwrap()
            .unwrap()
            .control()
            .unwrap()
            .revision;
        if phase == "repeated_enospc" {
            let _fault = inventory
                .inject_relocation_test_fault(92, "copy_chunk", libc::ENOSPC)
                .unwrap();
            assert!(engine
                .release_hold(JobId(92), revision.clone(), RetryBy::Operator)
                .await
                .unwrap()
                .is_err());
            assert!(inventory.for_job(92).unwrap().unwrap().hold.is_none());
            assert!(export_job_ready_for_test(&engine, JobId(92))
                .await
                .unwrap()
                .unwrap()
                .held());
        }
        if phase == "review" {
            std::fs::rename(&target_root, temporary.path().join("retained-old-volume")).unwrap();
            std::fs::create_dir(&target_root).unwrap();
        }
        let released = engine
            .release_hold(JobId(92), revision, RetryBy::Operator)
            .await
            .unwrap();
        if matches!(phase, "review" | "delete") {
            assert!(released.is_err(), "{phase} must remain fenced");
            assert!(export_job_ready_for_test(&engine, JobId(92))
                .await
                .unwrap()
                .unwrap()
                .held());
            assert!(source.join("episode.mkv").exists());
        } else {
            assert!(released.is_ok(), "{phase}: {released:?}");
            assert_eq!(
                export_job_ready_for_test(&engine, JobId(92))
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                JobStatus::PostQueued
            );
            let delivery = inventory.create_delivery(92, true, Some(phase)).unwrap();
            assert_eq!(delivery.files.len(), 1);
            assert_eq!(
                std::fs::read(target.join("episode.mkv")).unwrap(),
                vec![7; 131072]
            );
        }
        engine.shutdown().await;
    }
}

#[tokio::test]
async fn journal_replay_preserves_expected_size_for_fold_and_restart_verification() {
    for restart in [false, true] {
        let temporary = tempfile::tempdir().unwrap();
        let state = temporary.path().join("state");
        let destination = temporary.path().join("dest");
        std::fs::create_dir(&destination).unwrap();
        let config = || {
            crate::EngineConfig::single_node(
                Vec::new(),
                state.clone(),
                destination.clone(),
                Tuning::default(),
                None,
            )
        };
        let mut engine = crate::Engine::spawn(config()).await.unwrap();
        let job = pending_job(87);
        let file = job.files[0].id;
        engine.import_job(job.clone(), false, false).await.unwrap();
        export_job_ready_for_test(&engine, job.id)
            .await
            .unwrap()
            .unwrap();
        let source = engine.artifacts().for_job(job.id.0).unwrap().unwrap();
        std::fs::write(source.path.join("payload.bin"), [7; 100]).unwrap();
        if restart {
            engine.shutdown().await;
        }
        let mut journal = JobJournals::open(&state, "remote-grant").unwrap();
        journal
            .append(&JournalRecord {
                job: job.id,
                file,
                segment_number: 1,
                offset: 0,
                len: 100,
                crc32: crc32fast::hash(&[7; 100]),
                file_size: 100,
            })
            .unwrap();
        journal.sync().unwrap();
        drop(journal);
        if restart {
            engine = crate::Engine::spawn(config()).await.unwrap();
        } else {
            engine.fold_job_journals(job.id).await.unwrap();
        }
        let recovered = export_job_ready_for_test(&engine, job.id)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            recovered.files[0].segments[0].state,
            SegmentState::Done { .. }
        ));
        assert!(recovered.files[0].finalized);
        assert!(recovered
            .params
            .iter()
            .any(|(key, value)| key == &format!("*File:size:{}", file.0) && value == "100"));
        let durable = SnapshotStore::open(&state)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert!(durable.jobs[0]
            .params
            .iter()
            .any(|(key, value)| key == &format!("*File:size:{}", file.0) && value == "100"));
        engine.shutdown().await;
    }
}

#[test]
fn replayed_size_requires_matching_segment_and_preserves_existing_evidence() {
    let mut state = QueueState::default();
    state.jobs.push(pending_job(1));
    let mut record = JournalRecord {
        job: JobId(1),
        file: FileId(10),
        segment_number: 1,
        offset: 0,
        len: 100,
        crc32: 0,
        file_size: 0,
    };
    assert_eq!(merge_replayed_size(&mut state, &record), None);
    record.file_size = 100;
    record.segment_number = 2;
    assert_eq!(merge_replayed_size(&mut state, &record), None);
    record.segment_number = 1;
    assert_eq!(merge_replayed_size(&mut state, &record), Some(100));
    record.file_size = 200;
    assert_eq!(merge_replayed_size(&mut state, &record), Some(100));
    assert_eq!(
        state.jobs[0]
            .params
            .iter()
            .find(|(key, _)| key == "*File:size:10")
            .unwrap()
            .1,
        "100"
    );
}

#[tokio::test]
async fn foreign_journal_crc_preparation_keeps_actor_responsive_and_rejects_stale_intent() {
    let temporary = tempfile::tempdir().unwrap();
    let state = temporary.path().join("state");
    let destination = temporary.path().join("dest");
    std::fs::create_dir_all(destination.as_path()).unwrap();
    let engine = crate::Engine::spawn(crate::EngineConfig::single_node(
        Vec::new(),
        state.clone(),
        destination.clone(),
        Tuning::default(),
        None,
    ))
    .await
    .unwrap();
    let mut job = pending_job(88);
    job.dir_name = "restored-input".into();
    let file = job.files[0].id;
    let source = destination.join(&job.dir_name);
    let inventory = engine.artifacts();
    inventory
        .allocate_for_delivery(88, &destination, &source, true, false)
        .unwrap();
    let partial = source.join(format!(".runner-file-{}.part", file.0));
    std::fs::write(&partial, vec![7; 100]).unwrap();
    inventory
        .finish(88, &source, &destination, "active")
        .unwrap();
    engine.import_job(job.clone(), false, false).await.unwrap();
    export_job_ready_for_test(&engine, job.id)
        .await
        .unwrap()
        .unwrap();
    let mut journal = JobJournals::open(&state, "foreign").unwrap();
    journal
        .append(&JournalRecord {
            job: job.id,
            file,
            segment_number: 1,
            offset: 0,
            len: 100,
            crc32: crc32fast::hash(&[7; 100]),
            file_size: 100,
        })
        .unwrap();
    journal.sync().unwrap();
    let gate = Arc::new((std::sync::Barrier::new(2), std::sync::Barrier::new(2)));
    *crate::imports::PREPARATION_GATE.lock().unwrap() = Some((source.clone(), gate.clone()));
    let importer = engine.clone();
    let incoming = job.clone();
    let pending = tokio::spawn(async move { importer.import_job(incoming, true, false).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while crate::imports::PREPARATION_GATE.lock().unwrap().is_some() {
            assert!(
                !pending.is_finished(),
                "import returned before CRC preparation gate"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("import must reach off-actor CRC preparation");
    let entered = gate.clone();
    tokio::task::spawn_blocking(move || entered.0.wait())
        .await
        .unwrap();
    let controls = tokio::time::timeout(Duration::from_secs(2), async {
        Ok::<_, crate::EngineError>([
            engine.pause_job(job.id).await?,
            engine.set_priority(job.id, 77).await?,
            engine.set_keep_on_failure(job.id, true).await?,
        ])
    })
    .await;
    let release = gate.clone();
    tokio::task::spawn_blocking(move || release.1.wait())
        .await
        .unwrap();
    let controls = controls
        .expect("CRC disk work must not block actor controls")
        .unwrap();
    assert_eq!(controls, [true; 3], "pause/priority/Keep must all succeed");
    assert!(matches!(
        pending.await.unwrap(),
        Err(crate::EngineError::StaleImport)
    ));
    let current = engine.export_job(job.id).await.unwrap().unwrap();
    assert_eq!(current.priority, 77);
    assert_eq!(current.status, JobStatus::Paused);
    assert!(current
        .params
        .iter()
        .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1"));
    assert_eq!(std::fs::read(&partial).unwrap(), vec![7; 100]);
    // A control snapshot taken during preparation must not compact foreign
    // facts that the rejected import never committed.
    assert_eq!(JobJournals::replay_job(&state, job.id).unwrap().len(), 1);
    assert!(engine
        .set_category(job.id, Some("after-stale".into()))
        .await
        .unwrap());
    assert_eq!(JobJournals::replay_job(&state, job.id).unwrap().len(), 1);
    engine
        .import_job(
            engine.export_job(job.id).await.unwrap().unwrap(),
            true,
            false,
        )
        .await
        .unwrap();
    let committed = export_job_ready_for_test(&engine, job.id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        committed.files[0].segments[0].state,
        SegmentState::Done { .. }
    ));
    assert_eq!(committed.priority, 77);
    engine.shutdown().await;
}

#[tokio::test]
async fn prepared_import_rechecks_live_authority_at_actor_apply() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (temporary, mut owner, _adapter, mut messages) = control_test_owner();
    let mut job = pending_job(91);
    job.dir_name = "expired-grant".into();
    let live = Arc::new(AtomicBool::new(true));
    let check = live.clone();
    let authority = crate::imports::ImportAuthority(Arc::new(move || check.load(Ordering::SeqCst)));
    let (prepared, proof) = crate::imports::prepare(
        job,
        None,
        &temporary.path().join("state"),
        &owner.dest_dir,
        owner.artifacts.clone(),
        authority.clone(),
    )
    .unwrap();
    live.store(false, Ordering::SeqCst);
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::ImportJob {
        job: Box::new(prepared),
        preparation: Some(proof),
        authority,
        emit_finished: false,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(matches!(
        result.await.unwrap(),
        Err(crate::EngineError::StaleImport)
    ));
    assert!(owner.state.job(JobId(91)).is_none());
    assert!(!owner.dest_dir.join("expired-grant").exists());
}

#[tokio::test]
async fn terminal_or_stale_import_preparation_never_creates_an_absent_namespace() {
    let temporary = tempfile::tempdir().unwrap();
    let state = temporary.path().join("state");
    let destination = temporary.path().join("dest");
    std::fs::create_dir_all(destination.as_path()).unwrap();
    let engine = crate::Engine::spawn(crate::EngineConfig::single_node(
        Vec::new(),
        state.clone(),
        destination.clone(),
        Tuning::default(),
        None,
    ))
    .await
    .unwrap();
    let mut terminal = pending_job(89);
    terminal.status = JobStatus::Completed;
    terminal.dir_name = "already-cleaned".into();
    let (_, proof) = crate::imports::prepare(
        terminal,
        None,
        &state,
        &destination,
        engine.artifacts(),
        Default::default(),
    )
    .unwrap();
    drop(proof);
    assert!(!destination.join("already-cleaned").exists());
    let mut incoming = pending_job(90);
    incoming.dir_name = "rejected-namespace".into();
    let gate = Arc::new((std::sync::Barrier::new(2), std::sync::Barrier::new(2)));
    *crate::imports::PREPARATION_GATE.lock().unwrap() =
        Some((destination.join(&incoming.dir_name), gate.clone()));
    let importer = engine.clone();
    let pending = tokio::spawn(async move { importer.import_job(incoming, true, false).await });
    let entered = gate.clone();
    tokio::task::spawn_blocking(move || entered.0.wait())
        .await
        .unwrap();
    let mut newer = pending_job(90);
    newer.dir_name = "accepted-other-source".into();
    newer.priority = 42;
    engine.import_job(newer, false, false).await.unwrap();
    let release = gate.clone();
    tokio::task::spawn_blocking(move || release.1.wait())
        .await
        .unwrap();
    assert!(matches!(
        pending.await.unwrap(),
        Err(crate::EngineError::StaleImport)
    ));
    assert!(!destination.join("rejected-namespace").exists());
    assert_eq!(
        export_job_ready_for_test(&engine, JobId(90))
            .await
            .unwrap()
            .unwrap()
            .priority,
        42
    );
    engine.shutdown().await;
}

#[tokio::test]
async fn acknowledged_worker_api_import_is_noop_for_late_preterminal_descriptor() {
    use nzbd_state::artifacts::{
        DeliveryFileOutcome, FinalizationProof, WorkerControlContext, WORKER_CONTROL_CONTEXT_PARAM,
    };
    let temporary = tempfile::tempdir().unwrap();
    let state = temporary.path().join("state");
    let root = temporary.path().join("private");
    std::fs::create_dir_all(root.as_path()).unwrap();
    let mut config =
        crate::EngineConfig::single_node(Vec::new(), state, root.clone(), Tuning::default(), None);
    config.persist_queue = false;
    let engine = crate::Engine::spawn(config).await.unwrap();
    let mut marker = pending_job(85);
    marker.status = JobStatus::PostQueued;
    marker.dir_name = "input".into();
    mark_new_lifecycle_cohort(&mut marker);
    marker.delivery_lifecycle = true;
    let context = WorkerControlContext {
        lease_id: "lease-85".into(),
        job_incarnation: "incarnation-85".into(),
        job_revision: 1,
        control_revision: 1,
        epoch: 1,
    };
    marker.params.push((
        WORKER_CONTROL_CONTEXT_PARAM.into(),
        serde_json::to_string(&context).unwrap(),
    ));
    let inventory = engine.artifacts();
    let original = root.join("input");
    inventory
        .allocate_for_delivery(85, &root, &original, true, true)
        .unwrap();
    std::fs::write(original.join("episode.mkv"), b"verified").unwrap();
    inventory.finish(85, &original, &root, "active").unwrap();
    engine
        .import_job(marker.clone(), false, false)
        .await
        .unwrap();
    let target = temporary.path().join("publication/85");
    let selected = inventory.for_job(85).unwrap().unwrap().files;
    inventory
        .publish_selected_for_delivery_checked(
            85,
            &target,
            selected,
            vec![DeliveryFileOutcome {
                source_path: "episode.mkv".into(),
                publication_path: Some("episode.mkv".into()),
                classification: "video".into(),
                outcome: "published".into(),
                reason: None,
                repair_dependency: false,
                source_cleanup_status: "pending".into(),
            }],
            &|| Ok(()),
        )
        .unwrap();
    marker
        .params
        .push(("*PP:selected-publication-outcome".into(), "SUCCESS".into()));
    assert!(engine
        .import_worker_job_if_present(marker.clone())
        .await
        .unwrap());
    let prepared_before_acceptance = inventory.prepare_worker_completion(&marker).unwrap();
    assert!(!prepared_before_acceptance.accepted);
    let mut terminal = marker.clone();
    terminal.status = JobStatus::Completed;
    terminal
        .params
        .push((nzbd_types::PP_DONE_PARAM.into(), "SUCCESS".into()));
    assert!(engine.import_worker_job_if_present(terminal).await.unwrap());
    let publication = inventory.for_job(85).unwrap().unwrap();
    let source = nzbd_state::artifacts::SourceGeneration {
        artifact: publication.id,
        generation: publication.generation,
    };
    inventory
        .acknowledge_worker_completion(
            85,
            &context,
            &source,
            &FinalizationProof::Cluster {
                receipt: "durable-cluster-receipt".into(),
                accepted_at_ms: 1000,
            },
        )
        .unwrap();
    let (reply, result) = oneshot::channel();
    engine
        .send(QueueCommand::ImportJobIfPresent {
            job: Box::new(marker.clone()),
            require_snapshot: false,
            completion_proof: Some(prepared_before_acceptance),
            reply,
        })
        .await
        .unwrap();
    assert!(
        result.await.unwrap(),
        "actor must recheck acceptance committed after preparation"
    );
    assert!(engine.import_worker_job_if_present(marker).await.unwrap());
    let job = export_job_ready_for_test(&engine, JobId(85))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.status, JobStatus::Completed);
    assert!(job
        .params
        .iter()
        .any(|(key, value)| key == nzbd_types::PP_DONE_PARAM && value == "SUCCESS"));
    assert_eq!(inventory.for_job(85).unwrap().unwrap().path, target);
    engine.shutdown().await;
}

#[tokio::test]
async fn proven_worker_completion_preserves_selected_publication_and_separate_source() {
    use nzbd_state::artifacts::{
        DeliveryFileOutcome, WorkerControlContext, WORKER_CONTROL_CONTEXT_PARAM,
    };
    let (tmp, mut owner, _adapter, mut messages) = control_test_owner_with_persistence(false, None);
    let mut job = pending_job(84);
    job.status = JobStatus::PostQueued;
    job.dir_name = "worker-source-84".into();
    mark_new_lifecycle_cohort(&mut job);
    job.delivery_lifecycle = true;
    let context = WorkerControlContext {
        lease_id: "lease-84".into(),
        job_incarnation: "incarnation-84".into(),
        job_revision: 1,
        control_revision: 1,
        epoch: 1,
    };
    job.params.push((
        WORKER_CONTROL_CONTEXT_PARAM.into(),
        serde_json::to_string(&context).unwrap(),
    ));
    let original = owner.dest_dir.join(&job.dir_name);
    owner
        .artifacts
        .allocate_for_delivery(84, &owner.dest_dir, &original, true, true)
        .unwrap();
    std::fs::write(original.join("episode.mkv"), b"selected verified bytes").unwrap();
    owner
        .artifacts
        .finish(84, &original, &owner.dest_dir, "active")
        .unwrap();
    owner.artifacts.bind_worker_context(84, &context).unwrap();
    owner.state.jobs.push(job.clone());
    let selected = owner.artifacts.for_job(84).unwrap().unwrap().files;
    let target = tmp.path().join("published/private-84");
    let movement = owner
        .artifacts
        .publish_selected_for_delivery_checked(
            84,
            &target,
            selected,
            vec![DeliveryFileOutcome {
                source_path: "episode.mkv".into(),
                publication_path: Some("episode.mkv".into()),
                classification: "video".into(),
                outcome: "published".into(),
                reason: None,
                repair_dependency: false,
                source_cleanup_status: "pending".into(),
            }],
            &|| Ok(()),
        )
        .unwrap();
    job.params
        .push(("*PP:selected-publication-outcome".into(), "SUCCESS".into()));
    let proof = owner.artifacts.prepare_worker_completion(&job).unwrap();
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::ImportJobIfPresent {
        job: Box::new(job),
        require_snapshot: false,
        completion_proof: Some(proof),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    let publication = owner.artifacts.for_job(84).unwrap().unwrap();
    assert_eq!(publication.path, target);
    assert!(publication.owned);
    assert_eq!(publication.job, Some(84));
    let retained = owner
        .artifacts
        .get(&format!("source-{}", movement.operation_id))
        .unwrap();
    assert_eq!(retained.path, original);
    assert!(retained.owned);
    assert!(retained.job.is_none());
    assert!(original.join("episode.mkv").exists());
    assert!(target.join("episode.mkv").exists());
}

#[tokio::test]
async fn authority_fence_stops_relocation_midcopy_before_publication_and_recovers() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let temporary = tempfile::tempdir().unwrap();
    let destination = temporary.path().join("working");
    std::fs::create_dir_all(destination.as_path()).unwrap();
    let completed = temporary.path().join("completed");
    std::fs::create_dir(&completed).unwrap();
    let valid = Arc::new(AtomicBool::new(true));
    let armed = Arc::new(AtomicBool::new(false));
    let scope = Arc::new(std::sync::Mutex::new(None::<PathBuf>));
    let observed = valid.clone();
    let watching = scope.clone();
    let copy_started = armed.clone();
    let mut config = crate::EngineConfig::single_node(
        Vec::new(),
        temporary.path().join("state"),
        destination.clone(),
        Tuning::default(),
        None,
    );
    config.persist_guard = Some(Arc::new(move || {
        if !observed.load(Ordering::SeqCst) {
            return false;
        }
        if let Some(path) = watching.lock().unwrap().as_ref() {
            for entry in std::fs::read_dir(path).unwrap().flatten() {
                if entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".runner-copy-")
                {
                    let length = entry.metadata().unwrap().len();
                    if length == 0 {
                        copy_started.store(true, Ordering::SeqCst);
                    }
                    if copy_started.load(Ordering::SeqCst) && length >= 65536 {
                        observed.store(false, Ordering::SeqCst);
                        return false;
                    }
                }
            }
        }
        true
    }));
    let engine = crate::Engine::spawn(config).await.unwrap();
    let inventory = engine.artifacts();
    let mut row = pending_job(95);
    row.status = JobStatus::PostQueued;
    row.delivery_lifecycle = true;
    mark_new_lifecycle_cohort(&mut row);
    let source = destination.join(&row.dir_name);
    inventory
        .allocate_for_delivery(95, &destination, &source, true, true)
        .unwrap();
    let payload = vec![17u8; 256 * 1024];
    std::fs::write(source.join("episode.mkv"), &payload).unwrap();
    let artifact = inventory
        .finish(95, &source, &destination, "active")
        .unwrap();
    engine.import_job(row, false, false).await.unwrap();
    // Relocation deliberately mutates this namespace. Finish the imported
    // source verification first so it cannot race that injected mutation.
    export_job_ready_for_test(&engine, JobId(95))
        .await
        .unwrap()
        .unwrap();
    let target = completed.join("episode");
    {
        let _fault = inventory
            .inject_relocation_test_fault(95, "copy_chunk", libc::ENOSPC)
            .unwrap();
        assert!(inventory
            .publish_selected_for_delivery_checked(
                95,
                &target,
                artifact.files,
                vec![nzbd_state::artifacts::DeliveryFileOutcome {
                    source_path: "episode.mkv".into(),
                    publication_path: Some("episode.mkv".into()),
                    classification: "media".into(),
                    outcome: "published".into(),
                    reason: None,
                    repair_dependency: false,
                    source_cleanup_status: "pending".into()
                }],
                &|| Ok(())
            )
            .is_err());
    }
    engine
        .hold_job(
            JobId(95),
            nzbd_types::HoldCause::Capacity,
            "move",
            "volume full",
        )
        .await
        .unwrap();
    let operation = inventory
        .pending_relocations(&artifact.id)
        .unwrap()
        .pop()
        .unwrap();
    let movement: serde_json::Value = serde_json::from_str(&operation.request).unwrap();
    *scope.lock().unwrap() = Some(PathBuf::from(movement["scratch"]["path"].as_str().unwrap()));
    let revision = export_job_ready_for_test(&engine, JobId(95))
        .await
        .unwrap()
        .unwrap()
        .control()
        .unwrap()
        .revision;
    assert!(engine
        .release_hold(JobId(95), revision, RetryBy::Operator)
        .await
        .unwrap()
        .is_err());
    assert!(!valid.load(Ordering::SeqCst));
    assert!(!target.exists());
    assert_eq!(std::fs::read(source.join("episode.mkv")).unwrap(), payload);
    assert_eq!(inventory.operation(&operation.id).unwrap().state, "retry");
    *scope.lock().unwrap() = None;
    valid.store(true, Ordering::SeqCst);
    engine.adopt_authority().await.unwrap();
    let revision = export_job_ready_for_test(&engine, JobId(95))
        .await
        .unwrap()
        .unwrap()
        .control()
        .unwrap()
        .revision;
    assert!(engine
        .release_hold(JobId(95), revision, RetryBy::Operator)
        .await
        .unwrap()
        .is_ok());
    let delivery = inventory
        .create_delivery(95, true, Some("authority-fence-recovered"))
        .unwrap();
    assert_eq!(delivery.files.len(), 1);
    assert_eq!(
        inventory.operation(&operation.id).unwrap().state,
        "succeeded"
    );
    engine.shutdown().await;
}

#[tokio::test]
async fn checked_release_public_api_refuses_expiry_after_custody_probe() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let temporary = tempfile::tempdir().unwrap();
    let destination = temporary.path().join("working");
    std::fs::create_dir_all(destination.as_path()).unwrap();
    let engine = crate::Engine::spawn(crate::EngineConfig::single_node(
        Vec::new(),
        temporary.path().join("state"),
        destination.clone(),
        Tuning::default(),
        None,
    ))
    .await
    .unwrap();
    let mut row = pending_job(94);
    row.status = JobStatus::PostQueued;
    mark_new_lifecycle_cohort(&mut row);
    let path = destination.join(&row.dir_name);
    let inventory = engine.artifacts();
    inventory
        .allocate_for_delivery(94, &destination, &path, true, true)
        .unwrap();
    std::fs::write(path.join("payload.bin"), b"retained processing source").unwrap();
    inventory.finish(94, &path, &destination, "active").unwrap();
    engine.import_job(row, false, false).await.unwrap();
    // This test expires authority during the later custody probe, not during
    // import recovery. Establish the verified input before installing its hold.
    export_job_ready_for_test(&engine, JobId(94))
        .await
        .unwrap()
        .unwrap();
    engine
        .hold_job(
            JobId(94),
            nzbd_types::HoldCause::MissingTool,
            "unpack",
            "7z unavailable",
        )
        .await
        .unwrap();
    let revision = export_job_ready_for_test(&engine, JobId(94))
        .await
        .unwrap()
        .unwrap()
        .control()
        .unwrap()
        .revision;
    let checks = Arc::new(AtomicUsize::new(0));
    let observed = checks.clone();
    let expired = engine
        .release_hold_checked(
            JobId(94),
            revision.clone(),
            RetryBy::Operator,
            Arc::new(move || observed.fetch_add(1, Ordering::SeqCst) < 6),
        )
        .await
        .unwrap();
    assert!(expired.is_err());
    assert_eq!(
        checks.load(Ordering::SeqCst),
        7,
        "checked throughout custody preparation and again at actor commit"
    );
    assert_eq!(
        export_job_ready_for_test(&engine, JobId(94))
            .await
            .unwrap()
            .unwrap()
            .control()
            .unwrap()
            .lifecycle,
        "held"
    );
    assert_eq!(
        std::fs::read(path.join("payload.bin")).unwrap(),
        b"retained processing source"
    );
    assert!(engine
        .release_hold_checked(JobId(94), revision, RetryBy::Operator, Arc::new(|| true))
        .await
        .unwrap()
        .is_ok());
    assert_eq!(
        export_job_ready_for_test(&engine, JobId(94))
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::PostQueued
    );
    engine.shutdown().await;
}

#[tokio::test]
async fn cluster_automatic_control_waits_for_exact_ack_and_operator_intent_is_not_replayed() {
    use crate::automatic_control::{transition, AUTOMATIC_CONTROL_PARAM};
    let (_tmp, mut owner, _adapter, mut messages) =
        control_test_owner_with_persistence(true, Some(Arc::new(|| true)));
    let mut base = pending_job(82);
    base.status = JobStatus::PostQueued;
    owner.state.jobs.push(base.clone());
    assert!(owner.hold_job(
        base.id,
        nzbd_types::HoldCause::MissingTool,
        "unpack",
        "7z missing"
    ));
    // Represent a durable hold whose first deadline has elapsed. Its
    // provenance and row remain one exact persisted result.
    let mut held = owner.state.job(base.id).unwrap().control().unwrap();
    held.next_retry_at_unix = Some(0);
    owner.state.job_mut(base.id).unwrap().set_control(&held);
    assert!(owner.prepare_automatic_control_change(base.id, Some(&base)));
    let (reply, committed) = oneshot::channel();
    owner.commit_control_change_then(base.id, Some(base.clone()), move |_, ok| {
        let _ = reply.send(ok);
    });
    pump(&mut owner, &mut messages).await;
    assert!(committed.await.unwrap());
    owner.publish_snapshot(0);
    let first = transition(owner.state.job(base.id).unwrap()).unwrap();
    assert_eq!(first.base_control, None);
    assert_eq!(first.base_status, JobStatus::PostQueued);
    assert!(owner
        .shared
        .load()
        .pending_automatic_controls
        .contains(&base.id));
    assert!(!owner.reschedule_hold(base.id, &held.revision, "still unavailable"));
    assert!(owner
        .retry_hold_proven(
            base.id,
            &held.revision,
            RetryBy::Recovery,
            true,
            &Default::default()
        )
        .is_err());
    assert_eq!(
        transition(owner.state.job(base.id).unwrap())
            .unwrap()
            .identity,
        first.identity
    );
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::AckAutomaticControl {
        job: base.id,
        identity: "0".repeat(64),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(!result.await.unwrap());
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::AckAutomaticControl {
        job: base.id,
        identity: first.identity.clone(),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    assert!(owner
        .retry_hold_proven(
            base.id,
            &held.revision,
            RetryBy::Recovery,
            true,
            &Default::default()
        )
        .is_ok());
    let released = transition(owner.state.job(base.id).unwrap()).unwrap();
    assert_eq!(released.base_control, Some(held.clone()));
    assert_eq!(released.result_status, JobStatus::PostQueued);
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::SetJobStatus {
        job: base.id,
        status: JobStatus::Post {
            stage: nzbd_types::PostStage::Unpack,
        },
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(
        !result.await.unwrap(),
        "POST must not enter IO before Raft acknowledgment"
    );
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::AckAutomaticControl {
        job: base.id,
        identity: first.identity,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(
        !result.await.unwrap(),
        "old acknowledgment cannot clear the new release marker"
    );
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::AckAutomaticControl {
        job: base.id,
        identity: released.identity.clone(),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    let control = owner.state.job(base.id).unwrap().control().unwrap();
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::AckAutomaticControl {
        job: base.id,
        identity: released.identity,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(
        !result.await.unwrap(),
        "lost local ACK may be ignored once marker is absent"
    );
    assert_eq!(owner.state.job(base.id).unwrap().control(), Some(control));
    assert!(owner.hold_job(
        base.id,
        nzbd_types::HoldCause::MissingTool,
        "unpack",
        "missing again"
    ));
    let held = owner.state.job(base.id).unwrap().control().unwrap();
    assert!(owner
        .retry_hold_proven(
            base.id,
            &held.revision,
            RetryBy::Operator,
            true,
            &Default::default()
        )
        .is_ok());
    assert!(
        transition(owner.state.job(base.id).unwrap()).is_none(),
        "uncommitted operator resume must not acquire automatic provenance"
    );
    pump(&mut owner, &mut messages).await;
    persist_snapshot_for_test(&mut owner, &mut messages).await;
    let durable = SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    assert!(!durable.jobs[0]
        .params
        .iter()
        .any(|(key, _)| key == AUTOMATIC_CONTROL_PARAM));
}

#[tokio::test]
async fn automatic_control_ack_snapshot_failure_preserves_pending_transition_repeatedly() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let allowed = Arc::new(AtomicBool::new(true));
    let observed = allowed.clone();
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner_with_persistence(
        true,
        Some(Arc::new(move || observed.load(Ordering::SeqCst))),
    );
    let row = pending_job(83);
    owner.state.jobs.push(row.clone());
    assert!(owner.hold_job(
        row.id,
        nzbd_types::HoldCause::Allocation,
        "download_write",
        "allocation blocked"
    ));
    let marker = crate::automatic_control::transition(owner.state.job(row.id).unwrap()).unwrap();
    persist_snapshot_for_test(&mut owner, &mut messages).await;
    allowed.store(false, Ordering::SeqCst);
    for _ in 0..2 {
        let (reply, result) = oneshot::channel();
        owner.on_command(QueueCommand::AckAutomaticControl {
            job: row.id,
            identity: marker.identity.clone(),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(!result.await.unwrap());
        assert_eq!(
            crate::automatic_control::transition(owner.state.job(row.id).unwrap()).unwrap(),
            marker
        );
    }
    let saved = SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    assert_eq!(
        crate::automatic_control::transition(&saved.jobs[0]).unwrap(),
        marker
    );
}

#[tokio::test]
async fn worker_reschedule_refuses_expiry_and_same_revision_regrant() {
    use nzbd_state::artifacts::{WorkerControlContext, WORKER_CONTROL_CONTEXT_PARAM};
    use std::sync::atomic::{AtomicBool, Ordering};
    let (_tmp, mut owner, _adapter, mut messages) =
        control_test_owner_with_persistence(false, None);
    let mut row = pending_job(77);
    mark_new_lifecycle_cohort(&mut row);
    let context = WorkerControlContext {
        lease_id: "live-77".into(),
        job_incarnation: "incarnation-77".into(),
        job_revision: 1,
        control_revision: 1,
        epoch: 1,
    };
    row.params.push((
        WORKER_CONTROL_CONTEXT_PARAM.into(),
        serde_json::to_string(&context).unwrap(),
    ));
    let path = owner.dest_dir.join(&row.dir_name);
    owner
        .artifacts
        .allocate_for_delivery(77, &owner.dest_dir, &path, true, true)
        .unwrap();
    owner.artifacts.bind_worker_context(77, &context).unwrap();
    owner.state.jobs.push(row);
    assert!(owner.hold_job(
        JobId(77),
        nzbd_types::HoldCause::Capacity,
        "download_write",
        "destination full"
    ));
    pump(&mut owner, &mut messages).await;
    let row = owner.state.job(JobId(77)).unwrap().clone();
    let source = owner.artifacts.for_job(77).unwrap().unwrap();
    let live = Arc::new(AtomicBool::new(true));
    let observed = live.clone();
    let guard = crate::hold_release::HoldReleaseGuard::prepare(
        &row,
        Some(&source),
        Some(Arc::new(move || observed.load(Ordering::SeqCst))),
    );
    let original = row.control().unwrap();
    let authority_guard = crate::hold_release::HoldReleaseGuard::prepare_queue_owner(
        &row,
        Some(&source),
        Some(Arc::new(|| true)),
    );
    assert!(!authority_guard.worker_checked());
    assert!(
        !authority_guard.matches(&owner.artifacts, &row, false),
        "an authority fence is not a worker grant"
    );
    assert!(!owner.reschedule_hold(
        row.id,
        &original.revision,
        "unchecked worker cannot reschedule"
    ));
    live.store(false, Ordering::SeqCst);
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::RescheduleHold {
        job: row.id,
        revision: original.revision.clone(),
        reason: "expired probe".into(),
        guard: guard.clone(),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(!result.await.unwrap());
    live.store(true, Ordering::SeqCst);
    let mut next = context.clone();
    next.epoch += 1;
    next.lease_id = "new-live-77".into();
    owner.artifacts.bind_worker_context(77, &next).unwrap();
    let current = owner.state.job_mut(row.id).unwrap();
    current
        .params
        .retain(|(key, _)| key != WORKER_CONTROL_CONTEXT_PARAM);
    current.params.push((
        WORKER_CONTROL_CONTEXT_PARAM.into(),
        serde_json::to_string(&next).unwrap(),
    ));
    owner
        .artifacts
        .checkpoint_worker_control(current, &original.revision)
        .unwrap();
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::RescheduleHold {
        job: row.id,
        revision: original.revision.clone(),
        reason: "late old probe".into(),
        guard,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(
        !result.await.unwrap(),
        "new lease retained the same control revision but replaced its authority"
    );
    assert_eq!(
        owner
            .state
            .job(row.id)
            .unwrap()
            .control()
            .unwrap()
            .retry_attempt,
        original.retry_attempt
    );
    let current = owner.state.job(row.id).unwrap().clone();
    let guard = crate::hold_release::HoldReleaseGuard::prepare(
        &current,
        Some(&source),
        Some(Arc::new(|| true)),
    );
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::RescheduleHold {
        job: row.id,
        revision: original.revision.clone(),
        reason: "current probe still blocked".into(),
        guard,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    let durable = owner
        .artifacts
        .worker_control_metadata(77, &next)
        .unwrap()
        .unwrap();
    assert_eq!(
        durable.control.retry_attempt,
        original.retry_attempt.saturating_add(1)
    );
    assert_ne!(durable.control.revision, original.revision);
    assert_eq!(
        durable.control.blocked_since_unix,
        original.blocked_since_unix
    );
    // A recovery pass that started before delegation also refuses at
    // actor apply, even after the engine became persistent authority.
    owner.persist = true;
    owner.delegated.insert(row.id, "new-worker".into());
    let mut held = owner.state.job(row.id).unwrap().control().unwrap();
    held.next_retry_at_unix = Some(0);
    owner.state.job_mut(row.id).unwrap().set_control(&held);
    assert!(!owner.reschedule_hold(row.id, &held.revision, "late authority pass"));
    let current = owner.state.job(row.id).unwrap().clone();
    let authority_guard = crate::hold_release::HoldReleaseGuard::prepare_queue_owner(
        &current,
        Some(&source),
        Some(Arc::new(|| true)),
    );
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::ReleaseResourceHold {
        job: row.id,
        revision: held.revision,
        by: RetryBy::Recovery,
        guard: authority_guard,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(!result.await.unwrap());
}

#[tokio::test]
async fn worker_release_rechecks_live_grant_and_source_at_actor_commit() {
    use nzbd_state::artifacts::{WorkerControlContext, WORKER_CONTROL_CONTEXT_PARAM};
    use std::sync::atomic::{AtomicBool, Ordering};
    let (_tmp, mut owner, _adapter, mut messages) =
        control_test_owner_with_persistence(false, None);
    let mut row = pending_job(77);
    mark_new_lifecycle_cohort(&mut row);
    let context = WorkerControlContext {
        lease_id: "live-77".into(),
        job_incarnation: "incarnation-77".into(),
        job_revision: 1,
        control_revision: 1,
        epoch: 1,
    };
    row.params.push((
        WORKER_CONTROL_CONTEXT_PARAM.into(),
        serde_json::to_string(&context).unwrap(),
    ));
    let path = owner.dest_dir.join(&row.dir_name);
    owner
        .artifacts
        .allocate_for_delivery(77, &owner.dest_dir, &path, true, true)
        .unwrap();
    owner.artifacts.bind_worker_context(77, &context).unwrap();
    owner.state.jobs.push(row);
    assert!(owner.hold_job(
        JobId(77),
        nzbd_types::HoldCause::Capacity,
        "download_write",
        "destination full"
    ));
    pump(&mut owner, &mut messages).await;
    let row = owner.state.job(JobId(77)).unwrap().clone();
    let source = owner.artifacts.for_job(77).unwrap().unwrap();
    let live = Arc::new(AtomicBool::new(true));
    let observed = live.clone();
    let guard = crate::hold_release::HoldReleaseGuard::prepare(
        &row,
        Some(&source),
        Some(Arc::new(move || observed.load(Ordering::SeqCst))),
    );
    let revision = row.control().unwrap().revision;
    // A checkpoint alone never establishes a currently live lease.
    assert!(
        !crate::hold_release::HoldReleaseGuard::prepare(&row, Some(&source), None).matches(
            &owner.artifacts,
            &row,
            false
        )
    );
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::BeginResourceProbe {
        job: row.id,
        revision: revision.clone(),
        by: RetryBy::Operator,
        guard: guard.clone(),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    live.store(false, Ordering::SeqCst);
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::ReleaseResourceHold {
        job: row.id,
        revision: revision.clone(),
        by: RetryBy::Operator,
        guard: guard.clone(),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(!result.await.unwrap());
    assert_eq!(
        owner
            .state
            .job(row.id)
            .unwrap()
            .control()
            .unwrap()
            .lifecycle,
        "held"
    );
    live.store(true, Ordering::SeqCst);
    owner.artifacts.set_keep_on_failure(77, true).unwrap();
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::ReleaseResourceHold {
        job: row.id,
        revision: revision.clone(),
        by: RetryBy::Operator,
        guard,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(
        !result.await.unwrap(),
        "source revision changed after probe"
    );
    let source = owner.artifacts.for_job(77).unwrap().unwrap();
    let guard = crate::hold_release::HoldReleaseGuard::prepare(
        &row,
        Some(&source),
        Some(Arc::new(|| true)),
    );
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::ReleaseResourceHold {
        job: row.id,
        revision,
        by: RetryBy::Operator,
        guard,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    assert_eq!(
        owner
            .state
            .job(row.id)
            .unwrap()
            .control()
            .unwrap()
            .lifecycle,
        "running"
    );
}

#[tokio::test]
async fn worker_retry_schedule_survives_regrant_without_overriding_new_leader_intent() {
    use nzbd_state::artifacts::{WorkerControlContext, WORKER_CONTROL_CONTEXT_PARAM};
    let (_tmp, mut owner, _adapter, mut messages) =
        control_test_owner_with_persistence(false, None);
    let mut original = pending_job(77);
    original.status = JobStatus::PostQueued;
    original.dir_name = "worker-private-77".into();
    mark_new_lifecycle_cohort(&mut original);
    let context = WorkerControlContext {
        lease_id: "lease-77".into(),
        job_incarnation: "incarnation-77".into(),
        job_revision: 1,
        control_revision: 1,
        epoch: 1,
    };
    original.params.push((
        WORKER_CONTROL_CONTEXT_PARAM.into(),
        serde_json::to_string(&context).unwrap(),
    ));
    let path = owner.dest_dir.join(&original.dir_name);
    owner
        .artifacts
        .allocate_for_delivery(77, &owner.dest_dir, &path, true, true)
        .unwrap();
    std::fs::write(path.join("episode.mkv"), b"retained payload").unwrap();
    owner
        .artifacts
        .finish(77, &path, &owner.dest_dir, "active")
        .unwrap();
    owner.artifacts.bind_worker_context(77, &context).unwrap();
    owner.state.jobs.push(original.clone());
    assert!(owner.hold_job(
        JobId(77),
        nzbd_types::HoldCause::parse("capacity"),
        "move",
        "destination full"
    ));
    let control = owner.state.jobs[0].control().unwrap();
    let deadline = unix_now() + 300;
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::SchedulePostRetry {
        job: JobId(77),
        revision: control.revision,
        retry_attempt: control.retry_attempt.saturating_add(1),
        next_retry: deadline,
        blocked_since: unix_now(),
        message: "retry scheduled".into(),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(
        result.await.unwrap(),
        "worker must commit to local inventory rather than a shared snapshot"
    );
    let durable = owner
        .artifacts
        .worker_control(77, &context)
        .unwrap()
        .unwrap();
    assert_eq!(durable.control.next_retry_at_unix, Some(deadline));
    owner.state.jobs.clear();
    owner.artifacts.close().unwrap();
    owner.artifacts = Arc::new(nzbd_state::artifacts::Inventory::open(&owner.state_dir).unwrap());
    assert!(import_job_for_test(&mut owner, &mut messages, original.clone()).await);
    assert_eq!(
        owner.state.jobs[0].control().unwrap().next_retry_at_unix,
        Some(deadline)
    );
    assert_eq!(owner.state.jobs[0].status, JobStatus::Paused);
    let mut newer = original.clone();
    let mut newer_context = context.clone();
    newer_context.control_revision += 1;
    newer
        .params
        .retain(|(key, _)| key != WORKER_CONTROL_CONTEXT_PARAM);
    newer.params.push((
        WORKER_CONTROL_CONTEXT_PARAM.into(),
        serde_json::to_string(&newer_context).unwrap(),
    ));
    assert!(import_job_for_test(&mut owner, &mut messages, newer).await);
    assert!(
        owner.state.jobs[0].control().is_none(),
        "new leader intent replaces the old local hold"
    );
    assert!(
        !import_job_for_test(&mut owner, &mut messages, original).await,
        "late old authority cannot overwrite the newer grant"
    );
}

#[tokio::test]
async fn replacement_commit_is_single_and_fencing_cannot_degrade_to_volatile_success() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let allowed = Arc::new(AtomicBool::new(true));
    let observed = allowed.clone();
    let (tmp, mut owner, _adapter, mut messages) = control_test_owner_with_persistence(
        true,
        Some(Arc::new(move || observed.load(Ordering::SeqCst))),
    );
    let original = pending_job(3);
    let file = original.files[0].id;
    owner.state.jobs.push(original.clone());
    owner.state.next_job_id = 3;
    owner.state.next_file_id = file.0;
    owner.file_sizes.insert(file, 42);
    owner.allocated_jobs.insert(JobId(3));
    owner
        .post_fetch_files
        .insert(JobId(3), HashSet::from([file]));
    persist_snapshot_for_test(&mut owner, &mut messages).await;
    let before = serde_json::to_value(owner.state.to_doc()).unwrap();
    allowed.store(false, Ordering::SeqCst);
    for (round, require_snapshot) in [(1, true), (2, false)] {
        let mut replacement = original.clone();
        replacement.priority = 99;
        replacement.files[0].id = FileId(999);
        let (reply, result) = oneshot::channel();
        owner.on_command(QueueCommand::ImportJobIfPresent {
            job: Box::new(replacement),
            require_snapshot,
            completion_proof: None,
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(
            !result.await.unwrap(),
            "fenced replacement round {round} must refuse"
        );
        assert_eq!(serde_json::to_value(owner.state.to_doc()).unwrap(), before);
        assert_eq!(owner.file_sizes.get(&file), Some(&42));
        assert!(owner.allocated_jobs.contains(&JobId(3)));
        assert!(owner.post_fetch_files[&JobId(3)].contains(&file));
        let persisted = SnapshotStore::open(&tmp.path().join("state"))
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert_eq!(serde_json::to_value(persisted).unwrap(), before);
    }
    // The remote import command must return the same definite failure.
    let mut replacement = original;
    replacement.priority = 101;
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::ImportJob {
        job: Box::new(replacement),
        preparation: None,
        authority: Default::default(),
        emit_finished: true,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap().is_err());
    assert_eq!(owner.state.jobs[0].priority, 0);
}

#[cfg(unix)]
#[test]
fn capacity_forecast_selects_finalized_and_repair_staging_paths() {
    use std::io::Write;
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let mut row = pending_job(76);
    let file = row.files[0].id;
    row.params
        .push((format!("*File:size:{}", file.0), (128 * 1024).to_string()));
    row.files[0].segments[0].state = SegmentState::Done {
        offset: 0,
        len: 65536,
        crc: crc32fast::hash(&[3; 65536]),
    };
    let legacy = root.join("payload.bin.part");
    let stable = root.join(format!(".runner-file-{}.part", file.0));
    let mut output = std::fs::File::create(&legacy).unwrap();
    output.write_all(&[3; 65536]).unwrap();
    output.sync_all().unwrap();
    let legacy_remaining = crate::download_capacity::remaining(&row, root);
    assert!(legacy_remaining < 128 * 1024);
    let output = std::fs::File::create(&stable).unwrap();
    output.set_len(1).unwrap();
    output.sync_all().unwrap();
    assert_eq!(
        crate::download_capacity::remaining(&row, root),
        128 * 1024,
        "truncated stable checkpoint cannot borrow verified legacy credit"
    );
    std::fs::remove_file(&stable).unwrap();
    std::fs::rename(&legacy, &stable).unwrap();
    assert_eq!(
        crate::download_capacity::remaining(&row, root),
        legacy_remaining
    );
    row.files[0].finalized = true;
    row.files[0].segments[0].state = SegmentState::Done {
        offset: 0,
        len: 128 * 1024,
        crc: crc32fast::hash(&vec![3; 128 * 1024]),
    };
    let mut final_file = std::fs::File::create(root.join("payload.bin")).unwrap();
    final_file.write_all(&vec![3; 128 * 1024]).unwrap();
    final_file.sync_all().unwrap();
    assert_eq!(
        crate::download_capacity::remaining(&row, root),
        0,
        "full finalized checkpoint is already allocated"
    );
    row.params
        .push((format!("*File:repair:{}", file.0), "true".into()));
    assert_eq!(
        crate::download_capacity::remaining(&row, root),
        128 * 1024,
        "repair uses its shorter staging checkpoint, not a different final file"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn partial_download_capacity_and_restarted_writer_reserve_only_unwritten_bytes() {
    use std::io::{Read, Seek, SeekFrom, Write};
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    let mut row = pending_job(78);
    mark_new_lifecycle_cohort(&mut row);
    let file = row.files[0].id;
    let mut next = row.files[0].segments[0].clone();
    next.number = 2;
    next.size = 65536;
    let mut last = next.clone();
    last.number = 3;
    row.files[0].segments.push(next);
    row.files[0].segments.push(last);
    owner.state.jobs.push(row);
    allocate_for_test(&mut owner, &mut messages, JobId(78), true).await;
    let source = owner.artifacts.for_job(78).unwrap().unwrap();
    let partial = source.path.join(format!(".runner-file-{}.part", file.0));
    let checkpoint = vec![7u8; 16 * 1024 * 1024];
    let legacy = source.path.join("payload.bin.part");
    let mut output = std::fs::File::create(&legacy).unwrap();
    output.write_all(&checkpoint).unwrap();
    output.sync_all().unwrap();
    drop(output);
    let reserve_margin = 1024 * 1024 * 1024;
    let _headroom = nzbd_state::capacity::inject_available_test_limit(
        &source.path,
        reserve_margin + 12 * 1024 * 1024,
    )
    .unwrap();
    let declared = 24 * 1024 * 1024;
    owner.on_msg(EngineMsg::SegmentWritten {
        job: JobId(78),
        file,
        seg_number: 1,
        offset: 0,
        len: checkpoint.len() as u32,
        crc: crc32fast::hash(&checkpoint),
        file_size: declared,
        server: ServerId(1),
    });
    assert!(owner.hold_job(
        JobId(78),
        nzbd_types::HoldCause::Capacity,
        "download_write",
        "disk full"
    ));
    pump(&mut owner, &mut messages).await;
    let row = owner.state.job(JobId(78)).unwrap().clone();
    let remaining = crate::download_capacity::remaining(&row, &source.path);
    assert!(remaining < declared);
    assert!(
        nzbd_state::capacity::reserve(&source.path, declared).is_err(),
        "full-size readmission would strand this partially completed job"
    );
    let reservation =
        nzbd_state::capacity::health(&source.path, remaining, "partial-download-capacity").unwrap();
    drop(reservation);
    let guard = crate::hold_release::HoldReleaseGuard::prepare(&row, Some(&source), None);
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::ReleaseResourceHold {
        job: JobId(78),
        revision: row.control().unwrap().revision,
        by: RetryBy::Operator,
        guard,
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    assert!(
        !partial.exists(),
        "health credited only the validated legacy staging path"
    );
    // Simulated headroom/claims belong only to this exact fixture root.
    let unrelated = owner.dest_dir.join("unrelated-capacity-context");
    std::fs::create_dir(&unrelated).unwrap();
    let independent = nzbd_state::capacity::reserve(&unrelated, declared).unwrap();
    drop(independent);
    // The injected durable range must first cross real CRC verification;
    // only its verified proof can admit the restarted writer's birth.
    let writer = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let candidate = owner.writer_for(JobId(78), file);
            if !candidate.is_closed() {
                break candidate;
            }
            owner.start_verifications();
            pump(&mut owner, &mut messages).await;
            tokio::select! {
                message = messages.recv() => owner.on_msg(message.unwrap()),
                _ = tokio::time::sleep(Duration::from_millis(1)) => {},
            }
        }
    })
    .await
    .expect("verified partial range must admit a real restarted writer");
    let resumed = vec![13u8; 65536];
    writer
        .send(WriteCmd::Segment {
            seg_number: 2,
            offset: checkpoint.len() as u64,
            data: resumed.clone(),
            crc: crc32fast::hash(&resumed),
            file_size: declared,
            server: ServerId(1),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = messages.recv().await.unwrap();
            let event = match &message {
                EngineMsg::ScopedOutcome { message, .. } => &**message,
                event => event,
            };
            if let EngineMsg::WriterError { error, .. } = event {
                panic!("resumed writer must use remaining-byte reservation: {error}");
            }
            owner.on_msg(message);
            let written = owner
                .state
                .job(JobId(78))
                .unwrap()
                .files
                .iter()
                .find(|entry| entry.id == file)
                .unwrap()
                .segments
                .iter()
                .any(|segment| {
                    segment.number == 2 && matches!(segment.state, SegmentState::Done { .. })
                });
            if written {
                break;
            }
        }
    })
    .await
    .unwrap();
    let mut input = std::fs::File::open(&partial).unwrap();
    input
        .seek(SeekFrom::Start(checkpoint.len() as u64))
        .unwrap();
    let mut actual = vec![0; 65536];
    input.read_exact(&mut actual).unwrap();
    assert_eq!(actual, resumed);
    owner.writers[&file].stop.cancel();
    let mut stopped = owner.writers[&file].stopped.clone();
    stopped.wait_for(|done| *done).await.unwrap();
}

#[tokio::test]
async fn storage_recovery_recreates_stopped_writer_and_writes_pending_segment() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    let job = pending_job(79);
    let file = job.files[0].id;
    owner.state.jobs.push(job);
    allocate_for_test(&mut owner, &mut messages, JobId(79), true).await;
    let _ = owner.writer_for(JobId(79), file);
    pump(&mut owner, &mut messages).await;
    let old_writer = owner.writer_for(JobId(79), file);
    let mut old_stopped = owner.writers[&file].stopped.clone();
    old_writer
        .send(WriteCmd::InjectFailure("write", libc::ENOSPC))
        .await
        .unwrap();
    old_writer
        .send(WriteCmd::Segment {
            seg_number: 1,
            offset: 0,
            data: b"recovered".to_vec(),
            crc: crc32fast::hash(b"recovered"),
            file_size: 9,
            server: ServerId(1),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !owner.state.jobs[0].held() {
            owner.on_msg(messages.recv().await.unwrap());
        }
        old_stopped.wait_for(|stopped| *stopped).await.unwrap();
    })
    .await
    .unwrap();
    assert!(old_writer.is_closed());
    pump(&mut owner, &mut messages).await;
    let revision = owner.state.jobs[0].control().unwrap().revision;
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::ReleaseResourceHold {
        job: JobId(79),
        revision,
        by: RetryBy::Operator,
        guard: Default::default(),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(result.await.unwrap());
    let _ = owner.writer_for(JobId(79), file);
    pump(&mut owner, &mut messages).await;
    let writer = owner.writer_for(JobId(79), file);
    writer
        .send(WriteCmd::Segment {
            seg_number: 1,
            offset: 0,
            data: b"recovered".to_vec(),
            crc: crc32fast::hash(b"recovered"),
            file_size: 9,
            server: ServerId(1),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let message = messages.recv().await.unwrap();
            owner.on_msg(message);
            let written = owner
                .state
                .job(JobId(79))
                .unwrap()
                .files
                .iter()
                .find(|entry| entry.id == file)
                .unwrap()
                .segments
                .iter()
                .any(|segment| {
                    segment.number == 1 && matches!(segment.state, SegmentState::Done { .. })
                });
            if written {
                break;
            }
        }
    })
    .await
    .unwrap();
    let source = owner.artifacts.for_job(79).unwrap().unwrap();
    assert_eq!(
        std::fs::read(source.path.join(format!(".runner-file-{}.part", file.0))).unwrap(),
        b"recovered"
    );
    owner.writers[&file].stop.cancel();
}

#[tokio::test]
async fn resource_probes_continue_after_eight_failures_and_explicit_retry_bypasses_deadline() {
    let (_tmp, mut owner, _adapter, mut messages) = control_test_owner();
    owner.state.jobs.push(pending_job(1));
    assert!(owner.hold_job(
        JobId(1),
        nzbd_types::HoldCause::parse("capacity"),
        "download_write",
        "disk full"
    ));
    pump(&mut owner, &mut messages).await;
    let revision = owner.state.jobs[0].control().unwrap().revision;
    for attempt in 1..=12 {
        owner.state.jobs[0]
            .params
            .retain(|(key, _)| key != "*Control:probe_after");
        let (reply, result) = oneshot::channel();
        owner.on_command(QueueCommand::BeginResourceProbe {
            job: JobId(1),
            revision: revision.clone(),
            by: RetryBy::Recovery,
            guard: Default::default(),
            reply,
        });
        pump(&mut owner, &mut messages).await;
        assert!(
            result.await.unwrap(),
            "resource retry {attempt} must remain admitted"
        );
    }
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::BeginResourceProbe {
        job: JobId(1),
        revision: revision.clone(),
        by: RetryBy::Recovery,
        guard: Default::default(),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(
        !result.await.unwrap(),
        "automatic retry honors persisted backoff"
    );
    let (reply, result) = oneshot::channel();
    owner.on_command(QueueCommand::BeginResourceProbe {
        job: JobId(1),
        revision,
        by: RetryBy::Operator,
        guard: Default::default(),
        reply,
    });
    pump(&mut owner, &mut messages).await;
    assert!(
        result.await.unwrap(),
        "explicit retry can probe immediately"
    );
    let count = owner.state.jobs[0]
        .params
        .iter()
        .find(|(key, _)| key == "*Control:attempts")
        .unwrap()
        .1
        .parse::<u32>()
        .unwrap();
    assert_eq!(count, 13);
}

#[test]
fn unpinned_failed_receipt_adopts_its_jobs_source_and_reports_disposal() {
    // Field report 2026-11-02: health failures retained before the receipt
    // pinned its source reported runner_owned forever, so Curator never saw
    // the source go and never replaced the release.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("processing");
    std::fs::create_dir(&root).unwrap();
    let inventory = nzbd_state::artifacts::Inventory::open(&tmp.path().join("state")).unwrap();
    let source = root.join("health-failed");
    inventory
        .allocate_for_delivery(2202, &root, &source, true, false)
        .unwrap();
    let artifact = inventory.finish(2202, &source, &root, "retained").unwrap();
    let mut receipt = UrlAdmissionReceipt {
        protocol_version: 1,
        request_token: "t-health".into(),
        submission_sha256: "a".repeat(64),
        job_id: 2202,
        generation: "admission".into(),
        runner_instance_id: inventory.installation.clone(),
        terminal_outcome: Some("FAILURE/HEALTH".into()),
        outcome: "failed".into(),
        failure_stage: Some("health".into()),
        failure_cause: Some("retained for recovery".into()),
        custody_state: "runner_owned".into(),
        keep_source_on_failure: false,
        source_cleanup_status: None,
        publication_path: None,
        source_artifact_id: None,
        source_generation: None,
        failure_reason: None,
        fetch_cause: None,
        blames_release: None,
        salvage: None,
    };
    let mut foreign = receipt.clone();
    foreign.runner_instance_id = "another-installation".into();

    assert!(refresh_url_admission_custody(&mut receipt, &inventory));
    assert_eq!(
        receipt.source_artifact_id.as_deref(),
        Some(artifact.id.as_str())
    );
    assert_eq!(receipt.custody_state, "runner_owned");
    assert!(!refresh_url_admission_custody(&mut foreign, &inventory));
    assert!(foreign.source_artifact_id.is_none());

    inventory.reconcile_empty_failed_sources().unwrap();
    inventory.run_due_deletes().unwrap();
    assert!(refresh_url_admission_custody(&mut receipt, &inventory));
    assert_eq!(receipt.custody_state, "deleted_by_runner");
    assert_eq!(receipt.source_cleanup_status.as_deref(), Some("removed"));
}

#[test]
fn failed_receipt_settles_when_its_source_is_gone() {
    // Review finding 2026-11-02: a retained failed source removed by hand is
    // marked source_gone, which still read as runner_owned and held Curator.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("processing");
    std::fs::create_dir(&root).unwrap();
    let inventory = nzbd_state::artifacts::Inventory::open(&tmp.path().join("state")).unwrap();
    let source = root.join("health-failed-removed");
    inventory
        .allocate_for_delivery(2203, &root, &source, true, false)
        .unwrap();
    std::fs::write(source.join("partial.mkv"), b"partial").unwrap();
    let artifact = inventory.finish(2203, &source, &root, "retained").unwrap();
    let mut receipt = UrlAdmissionReceipt {
        protocol_version: 1,
        request_token: "t-gone".into(),
        submission_sha256: "a".repeat(64),
        job_id: 2203,
        generation: "admission".into(),
        runner_instance_id: inventory.installation.clone(),
        terminal_outcome: Some("FAILURE/HEALTH".into()),
        outcome: "failed".into(),
        failure_stage: Some("health".into()),
        failure_cause: None,
        custody_state: "runner_owned".into(),
        keep_source_on_failure: false,
        source_cleanup_status: None,
        publication_path: None,
        source_artifact_id: Some(artifact.id.clone()),
        source_generation: Some(artifact.generation.clone()),
        failure_reason: None,
        fetch_cause: None,
        blames_release: None,
        salvage: None,
    };
    std::fs::remove_dir_all(&source).unwrap();
    inventory.reconcile_missing().unwrap();
    assert_eq!(inventory.get(&artifact.id).unwrap().state, "source_gone");
    assert!(refresh_url_admission_custody(&mut receipt, &inventory));
    assert_eq!(receipt.custody_state, "deleted_by_runner");
    assert_eq!(receipt.source_cleanup_status.as_deref(), Some("removed"));
}

#[tokio::test]
async fn naming_expectations_require_current_writer_and_durable_finalization() {
    use nzbd_state::artifacts::writer_naming::{verify_producer_read, NamingCandidate};
    for (current, durable, replace_pending) in [
        (false, true, false),
        (true, false, false),
        (true, true, true),
        (true, true, false),
    ] {
        let allowed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let gate = allowed.clone();
        let (_temp, mut owner, _adapter, mut messages) = control_test_owner_with_persistence(
            true,
            Some(Arc::new(move || {
                gate.load(std::sync::atomic::Ordering::Acquire)
            })),
        );
        pump(&mut owner, &mut messages).await;
        owner.state.jobs.push(pending_job(1));
        allocate_for_test(&mut owner, &mut messages, JobId(1), true).await;
        let allocation = owner.artifacts.for_job(1).unwrap().unwrap();
        let path = allocation.path.join("payload.bin");
        let bytes = b"CRC-validated producer bytes";
        std::fs::write(&path, bytes).unwrap();
        let reader = nzbd_state::fileops::open(&path).unwrap();
        let identity = nzbd_state::fileops::identity(&reader).unwrap();
        let naming = verify_producer_read(
            reader,
            bytes.len() as u64,
            crc32fast::hash(bytes),
            true,
            &|| Ok(()),
        )
        .unwrap()
        .unwrap()
        .published(&path, &identity)
        .unwrap();
        let record = owner.state.job_mut(JobId(1)).unwrap();
        record
            .params
            .push(("*File:generation:10".into(), "99".into()));
        record.files[0].segments[0].state = SegmentState::Done {
            offset: 0,
            len: bytes.len() as u32,
            crc: crc32fast::hash(bytes),
        };
        owner.writer_epochs.insert(FileId(10), 99);
        allowed.store(durable, std::sync::atomic::Ordering::Release);
        owner.on_msg(EngineMsg::ScopedWriter {
            job: JobId(1),
            file: FileId(10),
            generation: if current { 99 } else { 98 },
            fact: WriterFact::Finalized {
                ok: true,
                final_path: Some(path.clone()),
                combined_crc: Some(crc32fast::hash(bytes)),
                identity: Some(identity),
                naming: Some(Box::new(naming)),
            },
        });
        if replace_pending {
            // The completion was accepted, but its durable acknowledgement has
            // not been delivered to the owner. A replacement wins that race.
            assert!(!owner.persistence_callbacks.is_empty());
            owner.writer_epochs.insert(FileId(10), 100);
            owner.artifacts.invalidate_writer_naming_file(1, 10);
        }
        pump(&mut owner, &mut messages).await;
        let found = owner.artifacts.take_writer_naming(
            1,
            &[NamingCandidate {
                file: 10,
                writer_generation: 99,
                path,
            }],
        );
        assert_eq!(
            found.len(),
            usize::from(current && durable && !replace_pending),
            "only matching durable completion admits naming evidence"
        );
        if !current || !durable {
            assert!(!owner.state.job(JobId(1)).unwrap().files[0].finalized);
        }
        drop(found);
        owner.artifacts.prepare_writer_naming();
    }
}

/// No download-completion or terminal event was announced. `job_status_changed`
/// (contract §2.4) fires on every published transition and is not a
/// completion.
fn no_completion_event(events: &mut broadcast::Receiver<Event>) -> bool {
    loop {
        match events.try_recv() {
            Ok(Event::JobStatusChanged { .. }) => continue,
            Ok(_) => return false,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(_) => return true,
        }
    }
}
