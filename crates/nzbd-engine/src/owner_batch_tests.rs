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

    owner.start_verifications();

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
async fn recovery_checkpoint_backpressures_the_next_same_root_verification() {
    let temp = tempfile::tempdir().unwrap();
    let (mut owner, _adapter, mut messages) = owner_fixture(&temp);
    pump_idle(&mut owner, &mut messages).await;
    let root = temp.path().join("payloads");
    std::fs::create_dir_all(&root).unwrap();
    for id in 1..=20 {
        let allocation = owner
            .artifacts
            .allocate(id, &root, &root.join(format!("checkpoint-{id}")))
            .unwrap();
        owner.allocation_cache.insert(JobId(id), allocation);
        owner.state.jobs.push(pending_job(id, FileId(id)));
        owner.queue_verification(JobId(id));
    }
    let (started, started_rx) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    owner.io_then(
        move || {
            let _ = started.send(());
            wait.recv().expect("release checkpoint persistence barrier");
            Ok(())
        },
        |_, result| result.unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(5), started_rx)
        .await
        .unwrap()
        .unwrap();
    owner.start_verifications();
    let first = tokio::time::timeout(Duration::from_secs(5), messages.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        &first,
        EngineMsg::VerificationFinished { job: JobId(1), .. }
    ));
    owner.on_msg(first);
    assert_eq!(owner.verification[&JobId(1)].phase, "saving");
    assert!(
        owner.verification.contains_key(&JobId(1)),
        "uncommitted verification must remain fenced"
    );
    // A periodic owner tick must not create another full queue checkpoint
    // behind this storage root's still-unacknowledged recovery checkpoint.
    owner.start_verifications();
    let ran_ahead = owner.verification.values().any(|state| state.running);
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !owner.verification.is_empty() {
            owner.on_msg(messages.recv().await.expect("recovery completion"));
        }
    })
    .await
    .expect("acknowledgements resume the bounded recovery backlog");
    owner.drain_shutdown_results(&mut messages).await;
    assert!(
        !ran_ahead,
        "the next same-root verifier ran before the checkpoint was durable"
    );
}

#[tokio::test]
async fn recovery_checkpoint_preserves_other_root_progress_and_releases_failed_saves() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let temp = tempfile::tempdir().unwrap();
    let (mut owner, _adapter, mut messages) = owner_fixture(&temp);
    pump_idle(&mut owner, &mut messages).await;
    let allowed = Arc::new(AtomicBool::new(true));
    let guard = allowed.clone();
    owner.persist_guard = Some(Arc::new(move || guard.load(Ordering::SeqCst)));
    let first_root = temp.path().join("first");
    let second_root = temp.path().join("second");
    for root in [&first_root, &second_root] {
        std::fs::create_dir_all(root).unwrap();
    }
    for id in 1..=2 {
        let allocation = owner
            .artifacts
            .allocate(id, &first_root, &first_root.join(format!("job-{id}")))
            .unwrap();
        owner.allocation_cache.insert(JobId(id), allocation);
        owner.state.jobs.push(pending_job(id, FileId(id)));
        owner.queue_verification(JobId(id));
    }
    let (started, started_rx) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    owner.io_then(
        move || {
            let _ = started.send(());
            wait.recv().unwrap();
            Ok(())
        },
        |_, result| result.unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(5), started_rx)
        .await
        .unwrap()
        .unwrap();
    owner.start_verifications();
    let first = tokio::time::timeout(Duration::from_secs(5), messages.recv())
        .await
        .unwrap()
        .unwrap();
    owner.on_msg(first);
    assert_eq!(owner.verification_checkpoints.len(), 1);
    let allocation = owner
        .artifacts
        .allocate(3, &second_root, &second_root.join("job-3"))
        .unwrap();
    owner.allocation_cache.insert(JobId(3), allocation);
    owner.state.jobs.push(pending_job(3, FileId(3)));
    owner.queue_verification(JobId(3));
    owner.start_verifications();
    assert!(
        !owner.verification[&JobId(2)].running,
        "same-root verification waits for commit"
    );
    assert!(
        owner.verification[&JobId(3)].running,
        "another storage root remains independent"
    );
    let other = tokio::time::timeout(Duration::from_secs(5), messages.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        &other,
        EngineMsg::VerificationFinished { job: JobId(3), .. }
    ));
    owner.on_msg(other);
    assert_eq!(owner.verification_checkpoints.len(), 2);
    let third_root = temp.path().join("third");
    std::fs::create_dir_all(&third_root).unwrap();
    let allocation = owner
        .artifacts
        .allocate(4, &third_root, &third_root.join("job-4"))
        .unwrap();
    owner.allocation_cache.insert(JobId(4), allocation);
    owner.state.jobs.push(pending_job(4, FileId(4)));
    owner.queue_verification(JobId(4));
    owner.start_verifications();
    assert!(
        !owner.verification[&JobId(4)].running,
        "completed checkpoints retain the global recovery budget too"
    );
    allowed.store(false, Ordering::SeqCst);
    release.send(()).unwrap();
    pump_idle(&mut owner, &mut messages).await;
    assert!(
        owner.verification_checkpoints.is_empty(),
        "failed checkpoints release their exact root reservations"
    );
    assert_eq!(owner.verification[&JobId(1)].phase, "blocked");
    assert_eq!(owner.verification[&JobId(3)].phase, "blocked");
    assert!(
        owner.persistence_fenced,
        "a failed authority guard retains the existing persistence fence"
    );
    owner.start_verifications();
    assert!(
        owner.verification[&JobId(2)].running,
        "the failed checkpoint no longer reserves this root"
    );
    assert!(
        owner.verification[&JobId(4)].running,
        "released capacity also admits the third storage root"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while owner.verification.values().any(|state| state.running) {
            owner.on_msg(messages.recv().await.unwrap());
        }
    })
    .await
    .unwrap();
    assert_eq!(
        owner.verification[&JobId(2)].phase,
        "blocked",
        "released backpressure must not bypass a failed authority fence"
    );
    assert!(owner.verification_checkpoints.is_empty());
    owner.drain_shutdown_results(&mut messages).await;
}

#[tokio::test]
async fn stale_recovery_checkpoint_acknowledgement_releases_its_root() {
    let temp = tempfile::tempdir().unwrap();
    let (mut owner, _adapter, mut messages) = owner_fixture(&temp);
    pump_idle(&mut owner, &mut messages).await;
    let root = temp.path().join("payloads");
    std::fs::create_dir_all(&root).unwrap();
    let allocation = owner
        .artifacts
        .allocate(1, &root, &root.join("job-1"))
        .unwrap();
    owner.allocation_cache.insert(JobId(1), allocation);
    owner.state.jobs.push(pending_job(1, FileId(1)));
    owner.queue_verification(JobId(1));
    let (started, started_rx) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    owner.io_then(
        move || {
            let _ = started.send(());
            wait.recv().unwrap();
            Ok(())
        },
        |_, result| result.unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(5), started_rx)
        .await
        .unwrap()
        .unwrap();
    owner.start_verifications();
    let first = tokio::time::timeout(Duration::from_secs(5), messages.recv())
        .await
        .unwrap()
        .unwrap();
    owner.on_msg(first);
    let old_generation = owner.job_epochs[&JobId(1)];
    let new_generation = fresh_epoch();
    owner.job_epochs.insert(JobId(1), new_generation);
    owner.queue_verification(JobId(1));
    owner.start_verifications();
    assert!(
        !owner.verification[&JobId(1)].running,
        "replacement cannot run ahead of an older queued checkpoint"
    );
    release.send(()).unwrap();
    pump_idle(&mut owner, &mut messages).await;
    assert!(!owner
        .verification_checkpoints
        .contains_key(&(JobId(1), old_generation)));
    assert_eq!(owner.verification[&JobId(1)].generation, new_generation);
    owner.start_verifications();
    assert!(owner.verification[&JobId(1)].running);
    tokio::time::timeout(Duration::from_secs(5), async {
        while owner.verification.contains_key(&JobId(1)) {
            owner.on_msg(messages.recv().await.unwrap());
        }
    })
    .await
    .unwrap();
    assert!(owner.verification_checkpoints.is_empty());
    owner.drain_shutdown_results(&mut messages).await;
}
