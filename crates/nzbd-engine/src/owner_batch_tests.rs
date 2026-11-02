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
