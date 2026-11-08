use super::*;

fn owner_fixture(
    temp: &tempfile::TempDir,
) -> (
    Owner,
    crate::backend::BackendAdapterPort,
    mpsc::Receiver<EngineMsg>,
) {
    let (budget_tx, _) = watch::channel(crate::pool::BudgetEnvelope::default());
    let (events, _) = broadcast::channel(1);
    let (epoch_tx, _) = watch::channel(0);
    let (engine_tx, engine_messages) = mpsc::channel(1);
    let (backend, adapter) = crate::backend::backend_channel(1, 1);
    let owner = Owner::recover(
        &temp.path().join("state"),
        None,
        None,
        temp.path().join("dest"),
        Vec::new(),
        None,
        Arc::new(Vec::new()),
        Tuning::default(),
        true,
        true,
        "batch-test",
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
    (owner, adapter, engine_messages)
}

fn pending_job(id: u32, file: FileId) -> Job {
    let mut job = Job {
        id: JobId(id),
        kind: JobKind::Nzb,
        name: format!("batch-{id}"),
        dir_name: format!("batch-{id}"),
        name_provisional: false,
        delivery_lifecycle: false,
        queued_at_unix: 0,
        original_name: String::new(),
        category: None,
        priority: 0,
        dupe: Default::default(),
        params: vec![],
        files: vec![nzbd_types::FileEntry {
            id: file,
            subject: "payload.bin".into(),
            filename: "payload.bin".into(),
            filename_confirmed: true,
            is_par2: false,
            paused: false,
            groups: vec![],
            date: None,
            crc32: None,
            finalized: false,
            segments: vec![nzbd_types::Segment {
                message_id: "pending@example".into(),
                number: 1,
                size: 1,
                state: SegmentState::Pending,
            }],
        }],
        totals: Default::default(),
        status: JobStatus::Queued,
        torrent: None,
        stages: vec![],
    };
    recompute_job_totals(&mut job);
    job
}

async fn pump_idle(owner: &mut Owner, messages: &mut mpsc::Receiver<EngineMsg>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            while let Ok(message) = messages.try_recv() {
                owner.on_msg(message);
            }
            if owner.persistence.in_flight() == 0
                && owner.persistence_callbacks.is_empty()
                && owner.io_callbacks.is_empty()
            {
                tokio::task::yield_now().await;
                match messages.try_recv() {
                    Ok(message) => {
                        owner.on_msg(message);
                        continue;
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        panic!("fixture result channel disconnected before worker completion")
                    }
                }
            }
            tokio::select! {
                message = messages.recv() => owner.on_msg(message.expect("fixture worker result channel")),
                _ = tokio::time::sleep(Duration::from_millis(1)) => {},
            }
        }
    })
    .await
    .expect("fixture workers reach an idle acknowledgement boundary");
}

#[tokio::test]
async fn acknowledged_recovery_drains_same_root_backlog_without_owner_ticks() {
    let temp = tempfile::tempdir().unwrap();
    let (mut owner, _adapter, mut messages) = owner_fixture(&temp);
    pump_idle(&mut owner, &mut messages).await;
    let root = temp.path().join("payloads");
    std::fs::create_dir_all(&root).unwrap();
    for id in 1..=20 {
        let allocation = owner
            .artifacts
            .allocate(id, &root, &root.join(format!("backlog-{id}")))
            .unwrap();
        owner.allocation_cache.insert(JobId(id), allocation);
        owner.state.jobs.push(pending_job(id, FileId(id)));
        owner.queue_verification(JobId(id));
    }
    owner.start_verifications();
    // Only real joined worker results and persistence acknowledgements drive
    // this loop. No periodic tick or explicit next-worker call is supplied.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !owner.verification.is_empty() {
            assert!(owner.verification_roots.len() <= 1);
            assert!(
                owner
                    .verification
                    .values()
                    .filter(|state| state.running)
                    .count()
                    <= 1
            );
            owner.on_msg(messages.recv().await.expect("recovery result channel"));
        }
    })
    .await
    .expect("same-root backlog drains on durable recovery acknowledgements");
    assert_eq!(owner.state.jobs.len(), 20);
    assert!(owner.state.jobs.iter().all(|job| {
        job.files[0].segments[0].state == SegmentState::Pending && !job.files[0].finalized
    }));
    tokio::time::timeout(
        Duration::from_secs(5),
        owner.drain_shutdown_results(&mut messages),
    )
    .await
    .expect("all worker custody drains before removing fixture roots");
}

#[tokio::test]
async fn same_root_verification_yields_to_resumed_job_after_publishing_progress() {
    let temp = tempfile::tempdir().unwrap();
    let (mut owner, _adapter, mut messages) = owner_fixture(&temp);
    pump_idle(&mut owner, &mut messages).await;
    let payload_root = temp.path().join("payloads");
    std::fs::create_dir_all(&payload_root).unwrap();

    let paused_id = JobId(1);
    let paused_file = FileId(10);
    let mut paused = pending_job(paused_id.0, paused_file);
    paused.status = JobStatus::Paused;
    let paused_bytes: Vec<u8> = (0..65).map(|index| index as u8).collect();
    paused.files[0].segments = paused_bytes
        .iter()
        .enumerate()
        .map(|(index, byte)| nzbd_types::Segment {
            message_id: format!("paused-{index}").into(),
            number: index as u32 + 1,
            size: 1,
            state: SegmentState::Done {
                offset: index as u64,
                len: 1,
                crc: crc32fast::hash(&[*byte]),
            },
        })
        .collect();
    paused
        .params
        .push((format!("*File:size:{}", paused_file.0), "65".into()));
    let paused_allocation = owner
        .artifacts
        .allocate(paused_id.0, &payload_root, &payload_root.join("paused"))
        .unwrap();
    std::fs::write(
        paused_allocation
            .path
            .join(format!(".runner-file-{}.part", paused_file.0)),
        paused_bytes,
    )
    .unwrap();
    owner.allocation_cache.insert(paused_id, paused_allocation);
    owner.state.jobs.push(paused);
    owner.queue_verification(paused_id);

    owner.start_verifications();
    let first_batch = tokio::time::timeout(Duration::from_secs(5), messages.recv())
        .await
        .expect("paused job verification batch completes")
        .expect("verification worker result channel stays open");
    assert!(
        matches!(&first_batch, EngineMsg::VerificationFinished { job, .. } if *job == paused_id),
        "the first result is the paused job's verification batch"
    );
    let resumed_id = JobId(2);
    let resumed_file = FileId(20);
    let mut resumed = pending_job(resumed_id.0, resumed_file);
    resumed.status = JobStatus::Queued;
    resumed.priority = 100;
    let resumed_bytes = b"resume";
    resumed.files[0].segments[0].size = resumed_bytes.len() as u32;
    resumed.files[0].segments[0].state = SegmentState::Done {
        offset: 0,
        len: resumed_bytes.len() as u32,
        crc: crc32fast::hash(resumed_bytes),
    };
    resumed.params.push((
        format!("*File:size:{}", resumed_file.0),
        resumed_bytes.len().to_string(),
    ));
    let resumed_allocation = owner
        .artifacts
        .allocate(resumed_id.0, &payload_root, &payload_root.join("resumed"))
        .unwrap();
    std::fs::write(
        resumed_allocation
            .path
            .join(format!(".runner-file-{}.part", resumed_file.0)),
        resumed_bytes,
    )
    .unwrap();
    owner
        .allocation_cache
        .insert(resumed_id, resumed_allocation);
    owner.state.jobs.push(resumed);
    owner.queue_verification(resumed_id);

    // Make the newly eligible row visible before processing the completed
    // batch: the completion itself must fairly refill the freed root slot.
    owner.on_msg(first_batch);

    let paused_progress = owner
        .verification
        .get(&paused_id)
        .expect("paused verification remains scheduled after a partial batch");
    assert!(!paused_progress.running, "the batch releases the root slot");
    assert_eq!(paused_progress.phase, "pending");
    assert!(
        paused_progress.checked_bytes > 0,
        "intermediate progress is visible before the job completes"
    );
    assert!(
        (1..=64).contains(&paused_progress.checked_bytes),
        "the time or range budget bounds each progress batch"
    );

    assert!(
        owner.verification[&resumed_id].running,
        "the resumed higher-priority job is admitted on the freed root next"
    );
    assert!(
        !owner.verification[&paused_id].running,
        "the paused job's next batch waits behind newly eligible work"
    );
    assert!(
        owner.verification[&paused_id].checked_bytes > 0,
        "the yielded job retains its visible nonzero progress"
    );

    let resumed_batch = tokio::time::timeout(Duration::from_secs(5), messages.recv())
        .await
        .expect("resumed job verification completes")
        .expect("verification worker result channel stays open");
    assert!(
        matches!(&resumed_batch, EngineMsg::VerificationFinished { job, .. } if *job == resumed_id),
        "the next worker result belongs to the resumed job"
    );
    owner.on_msg(resumed_batch);
    tokio::time::timeout(
        Duration::from_secs(5),
        owner.drain_shutdown_results(&mut messages),
    )
    .await
    .expect("fixture owner drains its workers before the temporary payload root is removed");
}

#[tokio::test]
async fn partial_recovery_batches_continue_without_owner_ticks() {
    let temp = tempfile::tempdir().unwrap();
    let (mut owner, _adapter, mut messages) = owner_fixture(&temp);
    pump_idle(&mut owner, &mut messages).await;
    let root = temp.path().join("payloads");
    std::fs::create_dir(&root).unwrap();
    let allocation = owner
        .artifacts
        .allocate(1, &root, &root.join("partial-batches"))
        .unwrap();
    let mut job = pending_job(1, FileId(1));
    job.status = JobStatus::Paused;
    let bytes = vec![0x5a; 129];
    job.files[0].segments = bytes
        .iter()
        .enumerate()
        .map(|(index, byte)| nzbd_types::Segment {
            message_id: format!("range-{index}").into(),
            number: index as u32 + 1,
            size: 1,
            state: SegmentState::Done {
                offset: index as u64,
                len: 1,
                crc: crc32fast::hash(&[*byte]),
            },
        })
        .collect();
    job.params
        .push(("*File:size:1".into(), bytes.len().to_string()));
    recompute_job_totals(&mut job);
    std::fs::write(allocation.path.join(".runner-file-1.part"), &bytes).unwrap();
    owner.allocation_cache.insert(job.id, allocation);
    owner.state.jobs.push(job);
    owner.queue_verification(JobId(1));
    owner.start_verifications();
    let mut batches = 0;
    tokio::time::timeout(Duration::from_secs(5), async {
        while owner.verification.contains_key(&JobId(1)) {
            let message = messages.recv().await.unwrap();
            if matches!(message, EngineMsg::VerificationFinished { .. }) {
                batches += 1;
            }
            owner.on_msg(message);
            assert!(owner.verification_roots.len() <= 1);
        }
    })
    .await
    .expect("partial batches must refill on completion without any owner tick");
    assert!(
        batches >= 3,
        "the real 64-range batch boundary must be exercised"
    );
    assert_eq!(
        owner.state.job(JobId(1)).unwrap().totals.success_articles,
        129
    );
    owner.drain_shutdown_results(&mut messages).await;
}

#[tokio::test]
async fn pristine_recovery_backlog_is_bounded_and_drains_without_checksum_workers() {
    let temp = tempfile::tempdir().unwrap();
    let (mut owner, _adapter, mut messages) = owner_fixture(&temp);
    pump_idle(&mut owner, &mut messages).await;
    for id in 1..=160 {
        owner.state.jobs.push(pending_job(id, FileId(id)));
    }
    for id in 1..=160 {
        owner.queue_payload_recovery(JobId(id));
    }
    assert!(
        owner.allocating_jobs.len() <= 64,
        "metadata operations reserve bounded future payload capacity"
    );
    let drained = tokio::time::timeout(Duration::from_secs(60), async {
        while !owner.verification.is_empty() || !owner.allocating_jobs.is_empty() {
            assert!(
                owner.verification_roots.is_empty(),
                "zero-claim rows require custody checks, not CRC workers"
            );
            assert!(owner.payload_waiting.len() <= 64);
            assert!(owner.state.jobs.iter().all(|job| !job.held()));
            owner.on_msg(messages.recv().await.unwrap());
        }
    })
    .await;
    assert!(drained.is_ok(), "recovery stalled: allocated={} allocating={} queued={} active={} callbacks={} pending={:?}",
        owner.allocation_cache.len(), owner.allocating_jobs.len(), owner.payload_waiting.len(), owner.payload_active.len(), owner.io_callbacks.len(),
        owner.verification.iter().take(4).map(|(id,state)|(id,state.phase.clone(),state.reason.clone())).collect::<Vec<_>>());
    assert_eq!(owner.allocation_cache.len(), 160);
    assert!(owner
        .state
        .jobs
        .iter()
        .all(|job| job.totals.success_articles == 0 && job.status == JobStatus::Queued));
    owner.drain_shutdown_results(&mut messages).await;
}

#[tokio::test]
async fn payload_capacity_refusal_is_deferred_and_does_not_hold_a_download() {
    let temp = tempfile::tempdir().unwrap();
    let (mut owner, _adapter, mut messages) = owner_fixture(&temp);
    pump_idle(&mut owner, &mut messages).await;
    let root = temp.path().join("blocked-root");
    let (release, gate) = std::sync::mpsc::channel();
    let (entered, started) = oneshot::channel();
    owner.payload_then(
        root.clone(),
        move || {
            entered.send(()).unwrap();
            gate.recv().unwrap();
            Ok(())
        },
        |_, result| result.unwrap(),
    );
    started.await.unwrap();
    for _ in 0..64 {
        owner.payload_then(root.clone(), || Ok(()), |_, result| result.unwrap());
    }
    let (reply, refusal) = oneshot::channel();
    owner.payload_then(
        root,
        || -> Result<(), nzbd_state::StateError> { panic!("unadmitted work must not execute") },
        move |_, result| {
            reply.send(result).unwrap();
        },
    );
    let error = refusal.await.unwrap().unwrap_err();
    assert!(matches!(error, nzbd_state::StateError::Busy(_)));
    assert!(!error.to_string().contains("corrupt"));
    owner.state.jobs.push(pending_job(1, FileId(1)));
    assert!(owner.ensure_allocation(JobId(1), false).is_err());
    assert!(!owner.state.job(JobId(1)).unwrap().held());
    assert!(!owner.allocating_jobs.contains(&JobId(1)));
    owner.servers = Arc::new(vec![nzbd_types::ServerDef {
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
    assert!(owner.grant_work(ServerId(1), 1).is_empty());
    let scheduler_held_the_job = owner.state.job(JobId(1)).unwrap().held();
    release.send(()).unwrap();
    pump_idle(&mut owner, &mut messages).await;
    assert!(owner.payload_waiting.is_empty());
    owner.drain_shutdown_results(&mut messages).await;
    assert!(
        !scheduler_held_the_job,
        "grant_work must defer a full payload lane instead of holding the download"
    );
}

#[tokio::test]
async fn zero_display_totals_do_not_skip_real_done_range_validation() {
    let temp = tempfile::tempdir().unwrap();
    let (mut owner, _adapter, mut messages) = owner_fixture(&temp);
    pump_idle(&mut owner, &mut messages).await;
    let allocation = owner
        .artifacts
        .allocate(1, &owner.dest_dir, &owner.dest_dir.join("batch-1"))
        .unwrap();
    std::fs::write(allocation.path.join(".runner-file-1.part"), b"x").unwrap();
    let mut job = pending_job(1, FileId(1));
    job.status = JobStatus::Paused;
    job.files[0].segments[0].state = SegmentState::Done {
        offset: 0,
        len: 1,
        crc: crc32fast::hash(b"y"),
    };
    // Deliberately stale display counters cannot discharge a real byte claim.
    job.totals = Default::default();
    owner.state.jobs.push(job);
    owner.queue_payload_recovery(JobId(1));
    let mut validated = false;
    tokio::time::timeout(Duration::from_secs(5), async {
        while owner.verification.contains_key(&JobId(1)) {
            let message = messages.recv().await.unwrap();
            if matches!(message, EngineMsg::VerificationFinished { .. }) {
                validated = true;
            }
            owner.on_msg(message);
        }
    })
    .await
    .expect("a real Done range must pass through the existing verifier");
    assert!(validated);
    assert!(
        matches!(
            owner.state.job(JobId(1)).unwrap().files[0].segments[0].state,
            SegmentState::Pending
        ),
        "the bad legacy CRC must be rejected even with zero displayed totals"
    );
    owner.drain_shutdown_results(&mut messages).await;
}

#[tokio::test]
async fn saturated_journal_admission_checkpoints_latest_progress_without_false_holds() {
    let temp = tempfile::tempdir().unwrap();
    let (mut owner, _adapter, mut messages) = owner_fixture(&temp);
    pump_idle(&mut owner, &mut messages).await;
    let mut job = pending_job(1, FileId(1));
    job.status = JobStatus::Paused;
    job.files[0].segments = (1..=302)
        .map(|number| nzbd_types::Segment {
            message_id: format!("journal-{number}").into(),
            number,
            size: 1,
            state: SegmentState::Pending,
        })
        .collect();
    recompute_job_totals(&mut job);
    owner.state.jobs.push(job);
    let (release, gate) = std::sync::mpsc::channel();
    let (entered, started) = oneshot::channel();
    owner.io_then(
        move || {
            entered.send(()).unwrap();
            gate.recv().unwrap();
            Ok(())
        },
        |_, result| result.unwrap(),
    );
    started.await.unwrap();
    // Simulate already accepted writer facts while the real ordered lane is
    // blocked. More than its 256 slots must remain represented by owner state.
    for number in 1..=300 {
        owner.on_segment_written(
            JobId(1),
            FileId(1),
            number,
            u64::from(number - 1),
            1,
            42,
            302,
            ServerId(1),
        );
    }
    assert!(owner.journal_checkpoint.is_some());
    assert!(!owner.state.job(JobId(1)).unwrap().held());
    assert_eq!(
        owner.state.job(JobId(1)).unwrap().totals.success_articles,
        300
    );
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(60), async {
        while !owner.journal_checkpoint_inflight { owner.on_msg(messages.recv().await.unwrap()); }
        let captured = owner.journal_checkpoint;
        // Another in-flight segment finishes after the snapshot was captured.
        // Its newer generation must keep the backpressure fence in place.
        owner.on_segment_written(JobId(1), FileId(1), 301, 300, 1, 42, 302, ServerId(1));
        assert_ne!(owner.journal_checkpoint, captured);
        loop {
            let message = messages.recv().await.unwrap();
            let first_checkpoint = matches!(&message, EngineMsg::PersistenceFinished { result, .. } if result.snapshot);
            owner.on_msg(message);
            if first_checkpoint {
                assert!(owner.journal_checkpoint.is_some(), "an older checkpoint cannot release newer undurable progress");
                break;
            }
        }
        while owner.journal_checkpoint.is_some() { owner.on_msg(messages.recv().await.unwrap()); }
        pump_idle(&mut owner, &mut messages).await;
    }).await.expect("available persistence capacity must checkpoint retained facts without a timer");
    assert!(!owner.state.job(JobId(1)).unwrap().held());
    let saved = SnapshotStore::open(&owner.state_dir)
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    let done = saved.jobs[0].files[0]
        .segments
        .iter()
        .filter(|segment| matches!(segment.state, SegmentState::Done { .. }))
        .count();
    assert_eq!(
        done, 301,
        "the durable snapshot must cover all accepted facts, including refused append admission"
    );
    owner.drain_shutdown_results(&mut messages).await;
}

#[tokio::test]
async fn failed_journal_checkpoint_retains_fence_and_backs_off_until_storage_recovers() {
    let temp = tempfile::tempdir().unwrap();
    let (mut owner, _adapter, mut messages) = owner_fixture(&temp);
    pump_idle(&mut owner, &mut messages).await;
    owner.state.jobs.push(pending_job(1, FileId(1)));
    let blocked = owner.state_dir.join("queue.json.tmp");
    std::fs::create_dir(&blocked).unwrap();
    owner.journal_checkpoint = Some(fresh_epoch());
    owner.retry_journal_checkpoint();
    pump_idle(&mut owner, &mut messages).await;
    assert!(owner.journal_checkpoint.is_some());
    assert!(!owner.journal_checkpoint_inflight);
    assert!(owner.journal_checkpoint_retry_at > Instant::now());
    owner.retry_journal_checkpoint();
    assert!(
        owner.persistence_callbacks.is_empty(),
        "storage failure must not spin on new saves"
    );
    std::fs::remove_dir(blocked).unwrap();
    owner.journal_checkpoint_retry_at = Instant::now();
    owner.retry_journal_checkpoint();
    pump_idle(&mut owner, &mut messages).await;
    assert!(
        owner.journal_checkpoint.is_none(),
        "only the actual durable checkpoint releases the fence"
    );
    owner.drain_shutdown_results(&mut messages).await;
}
