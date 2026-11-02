//! Actual pending-worker import across real durable heartbeat renewals.
use super::*;
use axum::{extract::State, routing::post, Json, Router};
use nzbd_types::{FileEntry, FileId, Job, JobKind, JobTotals, Segment, SegmentState};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Condvar,
};
use std::time::Duration;

struct Gate {
    calls: AtomicUsize,
    outcome: AtomicUsize,
    entered: tokio::sync::Semaphore,
    release: (Mutex<bool>, Condvar),
}
static GATE: Mutex<Option<Arc<Gate>>> = Mutex::new(None);
pub(super) fn checkpoint(node: &str, job: JobId) {
    if node != "crc-renew-worker" || job.0 != 8801 {
        return;
    }
    let gate = GATE.lock().unwrap().clone();
    if let Some(gate) = gate {
        // prepare's initial authority check, range-open check, first chunk,
        // then second chunk. At least64KiB was actually CRC-read before this.
        if gate.calls.fetch_add(1, Ordering::SeqCst) == 3 {
            gate.entered.add_permits(1);
            let (lock, wake) = &gate.release;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
        }
    }
}
pub(super) fn record_result(node: &str, job: JobId, ok: bool) {
    if node == "crc-renew-worker" && job.0 == 8801 {
        if let Some(gate) = GATE.lock().unwrap().clone() {
            gate.outcome.store(if ok { 1 } else { 2 }, Ordering::SeqCst);
        }
    }
}
fn release(gate: &Gate) {
    *gate.release.0.lock().unwrap() = true;
    gate.release.1.notify_all();
}
struct ReleaseOnExit(Arc<Gate>);
impl Drop for ReleaseOnExit {
    fn drop(&mut self) {
        release(&self.0);
        *GATE.lock().unwrap() = None;
    }
}
#[derive(Clone)]
struct Leader {
    store: crate::control::ControlStore,
    grant: Arc<Mutex<Option<crate::proto::Grant>>>,
    polls: Arc<AtomicUsize>,
    renewals: Arc<AtomicUsize>,
    serial: Arc<tokio::sync::Mutex<()>>,
}
async fn poll(
    State(state): State<Leader>,
    Json(request): Json<crate::proto::PollRequest>,
) -> Json<PollResponse> {
    assert_eq!(request.node, "crc-renew-worker");
    Json(PollResponse {
        grants: if state.polls.fetch_add(1, Ordering::SeqCst) == 0 {
            vec![state.grant.lock().unwrap().take().expect("single grant")]
        } else {
            Vec::new()
        },
    })
}
async fn heartbeat(
    State(state): State<Leader>,
    Json(request): Json<crate::proto::HeartbeatRequest>,
) -> Json<crate::proto::HeartbeatResponse> {
    let _serial = state.serial.lock().await;
    let mut renewed = Vec::new();
    let mut cancel = Vec::new();
    let mut controls = HashMap::new();
    for progress in request.leases {
        match state
            .store
            .renew(&progress.token, Duration::from_secs(3))
            .await
            .unwrap()
        {
            Some(token) => {
                renewed.push(token);
                controls.insert(progress.lease_id, 1);
                state.renewals.fetch_add(1, Ordering::SeqCst);
            }
            None => cancel.push(progress.lease_id),
        }
    }
    Json(crate::proto::HeartbeatResponse {
        cancel,
        renewed,
        controls,
        server_budgets: None,
        budget_generation: 0,
        post_fetch_budgeted: true,
    })
}
async fn reject() -> Json<crate::proto::RejectResponse> {
    Json(crate::proto::RejectResponse { released: true })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn pending_worker_crc_accepts_real_renewal_and_refuses_expired_or_replaced_grants() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("nzbd_cluster=debug,nzbd_engine=info")
        .with_test_writer()
        .try_init();
    for disposition in ["renewed", "expired", "replaced", "legacy_no_proof"] {
        let temp = tempfile::tempdir().unwrap();
        let raft = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let api = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let cfg = ClusterConfig {
            cluster_id: "crc-test".into(),
            node_name: "crc-renew-worker".into(),
            shared_dir: temp.path().to_owned(),
            advertise_url: "http://127.0.0.1:1".into(),
            secret: "secret".into(),
            coordinator: false,
            priority: 0,
            download: true,
            max_download_jobs: 1,
            post_process: false,
            pp_slots: 0,
            lease_interval: Duration::from_millis(100),
            takeover_after: Duration::from_secs(1),
            worker_ttl: Duration::from_secs(3),
            control_dir: temp.path().join("control"),
            control_node_id: 1,
            control_raft_bind: raft.local_addr().unwrap().to_string(),
            control_api_bind: api.local_addr().unwrap().to_string(),
            control_peers: Vec::new(),
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: Vec::new(),
            torrent_payload_roots: Vec::new(),
        };
        drop(raft);
        drop(api);
        let store = crate::control::ControlStore::start(&cfg).await.unwrap();
        let state_dir = temp.path().join("state");
        let destination = temp.path().join("dest");
        std::fs::create_dir_all(&destination).unwrap();
        let owner =
            nzbd_state::artifacts::Inventory::open(&temp.path().join("owner-inventory")).unwrap();
        owner
            .allocate_for_delivery(
                8801,
                &destination,
                &destination.join("crc-job"),
                false,
                false,
            )
            .unwrap();
        let authority = owner
            .cluster_download_authority_source(8801)
            .unwrap()
            .unwrap();
        assert!(
            !owner
                .for_job(8801)
                .unwrap()
                .unwrap()
                .automatic_cleanup_eligible,
            "provenance cannot upgrade legacy cleanup eligibility"
        );
        let bytes = vec![0x51u8; 3 * 65536];
        let crc = crc32fast::hash(&bytes);
        std::fs::write(destination.join("crc-job/.runner-file-88001.part"), &bytes).unwrap();
        let job = Job {
            id: JobId(8801),
            kind: JobKind::Nzb,
            name: "crc-job".into(),
            dir_name: "crc-job".into(),
            name_provisional: false,
            delivery_lifecycle: false,
            queued_at_unix: 0,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: Default::default(),
            params: if disposition == "legacy_no_proof" {
                Vec::new()
            } else {
                vec![(
                    crate::delivery::AUTHORITY_COPY_PARAM.into(),
                    serde_json::to_string(&crate::delivery::AuthoritySourceCopy {
                        owner_node: "authority".into(),
                        copy: authority,
                    })
                    .unwrap(),
                )]
            },
            files: vec![FileEntry {
                id: FileId(88001),
                subject: "crc.mkv".into(),
                filename: "crc.mkv".into(),
                filename_confirmed: true,
                is_par2: false,
                paused: false,
                groups: Vec::new(),
                date: None,
                segments: vec![Segment {
                    message_id: "crc-message".into(),
                    number: 1,
                    size: bytes.len() as u32,
                    state: SegmentState::Pending,
                }],
                crc32: None,
                finalized: false,
            }],
            totals: JobTotals {
                size: bytes.len() as u64,
                total_articles: 1,
                ..Default::default()
            },
            status: JobStatus::Queued,
            torrent: None,
            stages: Vec::new(),
        };
        let mut journal = nzbd_state::JobJournals::open(&state_dir, "previous-worker").unwrap();
        journal
            .append(&nzbd_state::JournalRecord {
                job: job.id,
                file: FileId(88001),
                segment_number: 1,
                offset: 0,
                len: bytes.len() as u32,
                crc32: crc,
                file_size: bytes.len() as u64,
            })
            .unwrap();
        journal.sync().unwrap();
        drop(journal);
        let mut engine_cfg = nzbd_engine::EngineConfig::single_node(
            Vec::new(),
            state_dir,
            destination.clone(),
            nzbd_engine::Tuning {
                min_free_disk_bytes: 0,
                ..Default::default()
            },
            None,
        );
        engine_cfg.persist_queue = false;
        engine_cfg.download_enabled = false;
        let engine = nzbd_engine::Engine::spawn(engine_cfg).await.unwrap();
        store
            .seed_job(
                8801,
                "crc-job-incarnation",
                &serde_json::to_string(&job).unwrap(),
            )
            .await
            .unwrap();
        let crate::control::LeaseClaim::Acquired(token) = store
            .acquire(
                "download:8801",
                "crc-test",
                8801,
                "crc-job-incarnation",
                "crc-renew-worker",
                "worker-process",
                "download",
                "{}",
                1,
                Duration::from_secs(3),
            )
            .await
            .unwrap()
        else {
            panic!("grant not committed")
        };
        let grant = crate::proto::Grant {
            lifecycle_protocol: LIFECYCLE_PROTOCOL,
            lease_id: "crc-lease".into(),
            token: token.clone(),
            job_incarnation: "crc-job-incarnation".into(),
            job_revision: 1,
            control_revision: 1,
            scope: serde_json::json!({"whole_job":true,"_startOrdered":1,"_workerStorage":engine.artifacts().installation}),
            epoch: 1,
            kind: LeaseKind::Download,
            job,
            server_budgets: HashMap::new(),
            budget_generation: 0,
            post_fetch_budgeted: true,
        };
        let leader = Leader {
            store: store.clone(),
            grant: Arc::new(Mutex::new(Some(grant))),
            polls: Arc::new(AtomicUsize::new(0)),
            renewals: Arc::new(AtomicUsize::new(0)),
            serial: Arc::new(tokio::sync::Mutex::new(())),
        };
        let app = Router::new()
            .route("/cluster/v1/work/poll", post(poll))
            .route("/cluster/v1/work/heartbeat", post(heartbeat))
            .route("/cluster/v1/work/reject", post(reject))
            .with_state(leader.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let (_view_tx, view) = tokio::sync::watch::channel(crate::election::LeaderView {
            is_me: false,
            record: Some(crate::election::LeaderRecord {
                node: "authority".into(),
                api_url: url,
                epoch: 1,
                seq: 1,
            }),
        });
        let gate = Arc::new(Gate {
            calls: AtomicUsize::new(0),
            outcome: AtomicUsize::new(0),
            entered: tokio::sync::Semaphore::new(0),
            release: (Mutex::new(false), Condvar::new()),
        });
        *GATE.lock().unwrap() = Some(gate.clone());
        let _release_on_exit = ReleaseOnExit(gate.clone());
        let active: ActiveLeases = Default::default();
        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();
        spawn_worker(
            cfg.clone(),
            Vec::new(),
            engine.clone(),
            view,
            ClusterClient::new("secret".into()),
            active.clone(),
            None,
            destination.clone(),
            "worker-process".into(),
            cancel.clone(),
            &tracker,
        );
        tokio::time::timeout(Duration::from_secs(5), gate.entered.acquire())
            .await
            .expect("actual worker CRC gate not reached")
            .unwrap()
            .forget();
        assert!(
            engine.export_job(JobId(8801)).await.unwrap().is_none(),
            "pending CRC must not publish queue state"
        );
        tokio::time::timeout(
            Duration::from_millis(500),
            engine.set_speed_limit(Some(1234)),
        )
        .await
        .expect("CRC blocked queue controls")
        .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        while leader.renewals.load(Ordering::SeqCst) < 2 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "actual independent heartbeat lane did not renew during CRC"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let renewed = store.current_lease("download:8801").await.unwrap().unwrap();
        assert!(renewed.revision > token.revision);
        if disposition == "expired" {
            active
                .lock()
                .unwrap()
                .get_mut("crc-lease")
                .unwrap()
                .deadline = Instant::now() - Duration::from_secs(1);
        }
        if disposition == "replaced" {
            let _serial = leader.serial.lock().await;
            let renewed = store.current_lease("download:8801").await.unwrap().unwrap();
            assert!(store.release(&renewed).await.unwrap());
            let crate::control::LeaseClaim::Acquired(replacement) = store
                .acquire(
                    "download:8801",
                    "crc-test",
                    8801,
                    "crc-job-incarnation",
                    "crc-renew-worker",
                    "worker-process",
                    "download",
                    "{}",
                    1,
                    Duration::from_secs(3),
                )
                .await
                .unwrap()
            else {
                panic!("replacement not committed")
            };
            let mut state = active.lock().unwrap().remove("crc-lease").unwrap();
            state.token = replacement;
            active
                .lock()
                .unwrap()
                .insert("crc-replacement".into(), state);
        }
        release(&gate);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while gate.outcome.load(Ordering::SeqCst) == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "actual worker import did not return"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if matches!(disposition, "renewed" | "legacy_no_proof") {
            assert_eq!(
                gate.outcome.load(Ordering::SeqCst),
                1,
                "renewed same-operation import refused"
            );
            // The import ACK establishes the durable replacement. Startup
            // verification and stopped-writer joins finish independently, and
            // the public export correctly refuses that provisional interval.
            let job = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    match engine.export_job(JobId(8801)).await {
                        Ok(Some(job)) => break job,
                        Err(nzbd_engine::EngineError::Lifecycle(reason))
                            if reason == "job verification or writer retirement is still in progress; retry later" =>
                        {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        other => panic!("accepted worker import did not remain available: {other:?}"),
                    }
                }
            })
            .await
            .expect("accepted worker import did not finish verification and writer retirement");
            assert!(matches!(
                job.files[0].segments[0].state,
                SegmentState::Done { .. }
            ));
        } else {
            assert_eq!(gate.outcome.load(Ordering::SeqCst), 2);
            assert!(
                engine.export_job(JobId(8801)).await.unwrap().is_none(),
                "{disposition} grant imported after revocation"
            );
            if disposition == "replaced" {
                assert!(active.lock().unwrap().contains_key("crc-replacement"));
            }
        }
        cancel.cancel();
        tracker.close();
        tracker.wait().await;
        server.abort();
        engine.shutdown().await;
        store.shutdown().await.unwrap();
    }
}
