use super::*;
use nzbd_engine::Tuning;
use nzbd_state::artifacts::*;
use nzbd_types::*;
use sha2::{Digest, Sha256};
use std::time::Duration;
struct Node {
    runtime: crate::ClusterRuntime,
    server: tokio::task::JoinHandle<()>,
    url: String,
}
impl Node {
    async fn start(root: &std::path::Path, name: &str, coordinator: bool) -> Self {
        let api = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raft = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let control = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let cfg = crate::ClusterConfig {
            cluster_id: "exact-keep".into(),
            node_name: name.into(),
            shared_dir: root.into(),
            advertise_url: format!("http://{}", api.local_addr().unwrap()),
            secret: "keep-secret".into(),
            coordinator,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            pp_slots: 0,
            lease_interval: Duration::from_millis(100),
            takeover_after: Duration::from_secs(1),
            worker_ttl: Duration::from_secs(2),
            control_dir: root.join(format!("control-{name}")),
            control_node_id: 1,
            control_raft_bind: raft.local_addr().unwrap().to_string(),
            control_api_bind: control.local_addr().unwrap().to_string(),
            control_peers: vec![],
            download_weight: 1,
            pp_weight: 1,
            disk_guard_roots: vec![],
            torrent_payload_roots: vec![],
        };
        drop(raft);
        drop(control);
        let runtime = crate::ClusterRuntime::start(
            cfg,
            vec![],
            Tuning {
                min_free_disk_bytes: 0,
                ..Default::default()
            },
            root.join(format!("dest-{name}")),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let app = runtime.router("cancel-test", vec![]);
        let server = tokio::spawn(async move { axum::serve(api, app).await.unwrap() });
        Self {
            url: runtime.cfg.advertise_url.clone(),
            runtime,
            server,
        }
    }
    async fn stop(self) {
        self.runtime.shutdown().await;
        self.server.abort();
        let _ = self.server.await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_delete_waits_remote_join_before_owner_seal_and_physical_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let a = Node::start(temp.path(), "authority", true).await;
    let b = Node::start(temp.path(), "writer", false).await;
    let until = tokio::time::Instant::now() + Duration::from_secs(15);
    while !a.runtime.leader_shared.authority_ready()
        || b.runtime.leader_view().leader_url() != Some(a.url.as_str())
        || !crate::registry::read_nodes(&a.runtime.leader_shared.layout)
            .iter()
            .any(|n| n.name == "writer")
    {
        assert!(tokio::time::Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let inventory = a.runtime.engine.artifacts();
    let root = a.runtime.leader_shared.dest_dir.clone();
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("source");
    inventory
        .allocate_for_delivery(7, &root, &path, true, true)
        .unwrap();
    let file = path.join(".runner-file-1.part");
    std::fs::write(&file, b"partial usable download").unwrap();
    let copy = inventory
        .cluster_download_authority_source(7)
        .unwrap()
        .unwrap();
    let fact = serde_json::to_string(&crate::delivery::AuthoritySourceCopy {
        owner_node: "authority".into(),
        copy: copy.clone(),
    })
    .unwrap();
    let mut job = Job {
        id: JobId(7),
        kind: JobKind::Nzb,
        name: "cancelled".into(),
        dir_name: "source".into(),
        name_provisional: false,
        delivery_lifecycle: true,
        queued_at_unix: 1,
        original_name: String::new(),
        category: None,
        priority: 0,
        dupe: Default::default(),
        params: vec![(crate::delivery::AUTHORITY_COPY_PARAM.into(), fact.clone())],
        files: vec![nzbd_types::FileEntry {
            id: FileId(1),
            subject: "partial".into(),
            filename: "unconfirmed.bin".into(),
            filename_confirmed: false,
            is_par2: false,
            paused: true,
            groups: vec![],
            date: None,
            segments: vec![],
            crc32: None,
            finalized: false,
        }],
        totals: Default::default(),
        status: JobStatus::Paused,
        torrent: None,
        stages: vec![],
    };
    a.runtime
        .engine
        .import_job(job.clone(), false, false)
        .await
        .unwrap();
    // Main's import ACK precedes its bounded startup verification. The
    // deletion API intentionally refuses while that owner transition is in
    // flight; establish the fixture's ready source before granting B work.
    let imported_until = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        match a.runtime.engine.export_job(job.id).await {
            Ok(Some(_)) => break,
            Err(error)
                if error
                    .to_string()
                    .contains("job verification or writer retirement is still in progress") =>
            {
                assert!(
                    tokio::time::Instant::now() < imported_until,
                    "fixture import verification did not finish: {error}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            result => panic!("fixture imported source did not become readable: {result:?}"),
        }
    }
    let control = a.runtime.control.as_ref().unwrap();
    control
        .commit_job_delta(&[(7, Some(job.clone()))])
        .await
        .unwrap();
    let (incarnation, revision) = control.job_identity(7).await.unwrap().unwrap();
    let issuer = a.runtime.leader_shared.authority_identity();
    let scope=serde_json::json!({"_startOrdered":1,"_workerStorage":b.runtime.engine.artifacts().installation,"_authority":{"node":issuer.0,"incarnation":issuer.1,"fence":issuer.2}}).to_string();
    let crate::control::LeaseClaim::Acquired(token) = control
        .acquire(
            "work/job/7/download",
            "exact-keep",
            7,
            &incarnation,
            "writer",
            &b.runtime.leader_shared.owner_incarnation,
            "download",
            &scope,
            revision,
            Duration::from_secs(60),
        )
        .await
        .unwrap()
    else {
        panic!("grant")
    };
    let context = WorkerControlContext {
        lease_id: format!("{}@{}", token.resource, token.fence),
        job_incarnation: incarnation.clone(),
        job_revision: revision,
        control_revision: revision,
        epoch: issuer.2,
    };
    b.runtime
        .engine
        .artifacts()
        .begin_worker_grant(&WorkerStopRequest {
            job: 7,
            context: context.clone(),
            owner_incarnation: token.owner_incarnation.clone(),
            storage_instance_id: b.runtime.engine.artifacts().installation.clone(),
            removal_revision: None,
            authority_source_json: Some(fact),
            start_ordered: true,
        })
        .unwrap();
    let active = b
        .runtime
        .leader_shared
        .worker_leases
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    active.lock().unwrap().insert(
        context.lease_id.clone(),
        crate::worker::LeaseState {
            job: job.id,
            kind: crate::proto::LeaseKind::Download,
            token: token.clone(),
            job_incarnation: incarnation.clone(),
            job_revision: revision,
            control_revision: revision,
            deadline: tokio::time::Instant::now() + Duration::from_secs(60),
            cancel: cancel.clone(),
            pp_ready: false,
            ready_job: None,
        },
    );
    let tracker = tokio_util::task::TaskTracker::new();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let release = gate.clone();
    let (entered, writing) = tokio::sync::oneshot::channel();
    let actual_file = file.clone();
    active.spawn_operation(&tracker, context.lease_id.clone(), async move {
        let writer = std::fs::OpenOptions::new()
            .write(true)
            .open(actual_file)
            .unwrap();
        entered.send(()).unwrap();
        cancel.cancelled().await;
        release.acquire().await.unwrap().forget();
        writer.sync_all().unwrap();
        drop(writer);
    });
    writing.await.unwrap();
    a.runtime
        .engine
        .set_delegated(job.id, Some("writer".into()))
        .await
        .unwrap();
    let client = crate::http::ClusterClient::new("keep-secret".into());
    let request = hyper::Request::post("/api/v1/jobs/7/actions/delete-files")
        .body(axum::body::Body::empty())
        .unwrap();
    let response = client.forward(&a.url, request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "DeleteFiles response: {}",
        String::from_utf8_lossy(&body)
    );
    assert!(
        file.exists(),
        "owner A cannot unlink while delegated B still holds its writer"
    );
    job.params.push((
        WORKER_CONTROL_CONTEXT_PARAM.into(),
        serde_json::to_string(&context).unwrap(),
    ));
    job.status = JobStatus::Completed;
    let key = format!("{:x}", Sha256::digest(token.resource.as_bytes()));
    let sealed = a
        .runtime
        .cfg
        .shared_dir
        .join(".nzbd-cluster/generations/job-7")
        .join(format!("work-{}", &key[..16]))
        .join(format!("fence-{}", token.fence));
    std::fs::create_dir_all(sealed.join("files")).unwrap();
    let bytes = serde_json::to_vec(&job).unwrap();
    std::fs::write(sealed.join("job.json"), &bytes).unwrap();
    let mut manifest = crate::worker::GenerationManifest {
        job_id: 7,
        job_incarnation: incarnation.clone(),
        owner_node_id: "writer".into(),
        fence: token.fence,
        result_id: String::new(),
        job_sha256: format!("{:x}", Sha256::digest(&bytes)),
        files: vec![],
        total_bytes: 0,
        sealed_at_unix_ms: crate::control::unix_ms_public(),
        artifact_copy: None,
        directory_identity: Some(crate::worker::generation_directory_identity(&sealed).unwrap()),
        file_outcomes: vec![],
        identity_version: 3,
        source_disposition: None,
    };
    manifest.result_id = format!(
        "sha256:{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                manifest.job_id,
                &manifest.job_incarnation,
                &manifest.owner_node_id,
                manifest.fence,
                &manifest.job_sha256,
                &manifest.files,
                &manifest.artifact_copy,
                &manifest.directory_identity,
                &manifest.file_outcomes,
                &manifest.source_disposition
            ))
            .unwrap()
        )
    );
    std::fs::write(
        sealed.join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let request = CancellationRequest {
        lifecycle_protocol: 2,
        node: "writer".into(),
        job_id: 7,
        job_incarnation: incarnation.clone(),
        lease_id: context.lease_id.clone(),
        token: token.clone(),
        expected_job_revision: revision,
        result_id: manifest.result_id,
        result_ref: sealed.to_string_lossy().into_owned(),
        receipt_id: "cancel-actual-stop".into(),
    };
    let pending = tokio::spawn({
        let url = a.url.clone();
        let client = client.clone();
        async move {
            client
                .post_json::<_, CancellationDecision>(
                    &url,
                    "/cluster/v1/work/cancelled-completion",
                    &request,
                )
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        !pending.is_finished(),
        "stop response must await the actual remote operation join"
    );
    assert!(file.exists());
    gate.add_permits(1);
    let decision = pending.await.unwrap().unwrap();
    assert_eq!(decision.disposition, "cleanup_pending");
    let until = tokio::time::Instant::now() + Duration::from_secs(15);
    while path.exists() || sealed.exists() {
        assert!(
            tokio::time::Instant::now() < until,
            "physical copies did not drain: {:?}",
            control
                .cluster_delivery(&decision.cleanup_id)
                .await
                .unwrap()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        control
            .cluster_discovery_page(0, 10, true)
            .await
            .unwrap()
            .0
            .is_empty(),
        "cancellation cannot fabricate READY"
    );
    assert!(
        b.runtime
            .engine
            .artifacts()
            .worker_grant_is_stopped(7, &context)
            .unwrap()
    );
    assert!(
        b.runtime
            .engine
            .artifacts()
            .begin_worker_grant(&WorkerStopRequest {
                job: 7,
                context,
                owner_incarnation: token.owner_incarnation,
                storage_instance_id: b.runtime.engine.artifacts().installation.clone(),
                removal_revision: None,
                authority_source_json: job
                    .params
                    .iter()
                    .find(|(key, _)| key == crate::delivery::AUTHORITY_COPY_PARAM)
                    .map(|(_, v)| v.clone()),
                start_ordered: true
            })
            .is_err(),
        "late delivery of the removed grant must refuse before start"
    );
    tracker.close();
    tracker.wait().await;
    b.stop().await;
    a.stop().await;
}
