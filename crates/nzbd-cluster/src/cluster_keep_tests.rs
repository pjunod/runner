use super::*;
use axum::{middleware, routing::post, Router};
use http_body_util::BodyExt;
use nzbd_engine::Tuning;
use nzbd_state::artifacts::{Delivery, DeliveryFile, FinalizationProof};
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
            workspace_dir: None,
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
        if let Some(control) = &runtime.control {
            install_schema(control).await.unwrap();
        }
        // Use the production auth wrapper at the same boundary POST mounts.
        // The fixture intentionally has no user password; peer requests still
        // require the actual shared cluster secret before JSON extraction.
        async fn secret(
            State(shared): State<Arc<LeaderShared>>,
            request: Request,
            next: Next,
        ) -> Response {
            if crate::http::secret_matches(
                request
                    .headers()
                    .get(crate::proto::SECRET_HEADER)
                    .and_then(|value| value.to_str().ok()),
                &shared.cfg.secret,
            ) {
                next.run(request).await
            } else {
                axum::http::StatusCode::FORBIDDEN.into_response()
            }
        }
        let shared = runtime.leader_shared.clone();
        let peer = Router::new()
            .route(PEER_PATH, post(apply_owner).with_state(shared.clone()))
            .route_layer(middleware::from_fn_with_state(shared.clone(), secret));
        let user = nzbd_api::require_auth(
            Router::new()
                .fallback(|| async { axum::http::StatusCode::NOT_FOUND })
                .layer(middleware::from_fn_with_state(shared.clone(), serve)),
            Default::default(),
        );
        let app = Router::new().merge(peer).merge(user);
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
async fn request(
    node: &Node,
    method: &str,
    path: &str,
    body: serde_json::Value,
) -> (u16, serde_json::Value) {
    let request = hyper::Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap();
    let response = crate::http::ClusterClient::new("keep-secret".into())
        .forward(&node.url, request)
        .await
        .unwrap();
    let status = response.status().as_u16();
    let data = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&data).unwrap())
}
fn record(copy: ClusterArtifactCopy) -> crate::delivery::ClusterDelivery {
    let delivery:Delivery=serde_json::from_value(serde_json::json!({"protocolVersion":1,"sequence":0,"status":"ready","deliveryId":"kept-delivery","publicationGeneration":"publication-generation","artifactId":"publication","sourceArtifactId":copy.source.artifact,"sourceGeneration":copy.source.generation,"jobId":7,"runnerInstanceId":"keep-instance","requestToken":"keep-request","keepSourceOnFailure":false,"terminalOutcome":"UNPACK_FAILURE","publicationPath":"/fixture/publication","manifestDigest":"digest","files":[],"fileOutcomes":[],"readyAt":1,"acceptedAt":null,"consumerReceiptId":null,"sourceCleanup":{"status":"pending","error":null,"attempts":0,"nextRetryAt":null,"blockedSince":null,"warning":null}})).unwrap();
    crate::delivery::ClusterDelivery {
        delivery,
        receipt_id: "keep-receipt".into(),
        result_id: "keep-result".into(),
        copies: vec![CopyObligation {
            id: "raw-source".into(),
            owner_node: "source-owner".into(),
            artifact: Some(copy),
            local_delivery_id: None,
            result_ref: None,
            result_id: None,
            directory_identity: None,
            deletion_files: None,
            status: "pending".into(),
            error: None,
            claim_id: None,
            attempts: 0,
            next_retry_at: None,
        }],
        cleanup_only: false,
        source_disposition: Some("unusable".into()),
        revision: 0,
    }
}
async fn wait_authority(node: &Node) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !node.runtime.leader_shared.authority_ready() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
async fn wait_owner(leader: &Node, owner: &Node) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while owner.runtime.leader_shared.view.borrow().leader_url() != Some(leader.url.as_str())
        || !crate::registry::read_nodes(&leader.runtime.leader_shared.layout)
            .iter()
            .any(|node| node.name == owner.runtime.cfg.node_name && node.api_url == owner.url)
    {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retired_cross_node_keep_owner_restart_and_cleanup_race_are_truthful() {
    let temp = tempfile::tempdir().unwrap();
    let leader = Node::start(temp.path(), "authority", true).await;
    wait_authority(&leader).await;
    let mut owner = Node::start(temp.path(), "source-owner", false).await;
    wait_owner(&leader, &owner).await;
    let inventory = owner.runtime.engine.artifacts();
    let root = temp.path().join("source-files");
    std::fs::create_dir(&root).unwrap();
    let path = root.join("retired");
    inventory
        .allocate_for_delivery(7, &root, &path, true, true)
        .unwrap();
    std::fs::write(path.join("archive.bin"), b"source bytes").unwrap();
    inventory.finish(7, &path, &root, "completed").unwrap();
    let copy = inventory
        .cluster_download_authority_source(7)
        .unwrap()
        .unwrap();
    let control = leader.runtime.control.as_ref().unwrap();
    control
        .commit_delivery(&record(copy.clone()))
        .await
        .unwrap();
    assert!(
        control.current_job_intent(7).await.unwrap().is_none(),
        "fixture must actually be retired"
    );
    let (status, pending) = request(
        &leader,
        "PUT",
        "/api/v1/jobs/7/failure-keep",
        serde_json::json!({"keep":true}),
    )
    .await;
    assert_eq!(status, 202);
    assert_eq!(pending["applied"], false);
    assert_eq!(pending["keep"], true);
    assert!(
        !inventory
            .get(&copy.source.artifact)
            .unwrap()
            .keep_on_failure,
        "desired flag was falsely treated as applied"
    );
    reconcile_one(&leader.runtime.leader_shared).await.unwrap();
    let (status, confirmed) = request(
        &leader,
        "GET",
        "/api/v1/jobs/7/failure-keep",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 200, "{confirmed}");
    assert_eq!(confirmed["applied"], true);
    assert!(
        inventory
            .get(&copy.source.artifact)
            .unwrap()
            .keep_on_failure
    );
    assert!(!inventory.get(&copy.source.artifact).unwrap().keep);
    assert!(
        !control
            .cluster_delivery("kept-delivery")
            .await
            .unwrap()
            .unwrap()
            .delivery
            .keep_source_on_failure,
        "immutable admission Keep changed"
    );
    // Owner offline does not become a success. Pending desire survives owner restart.
    drop(inventory);
    owner.stop().await;
    let (_, pending) = request(
        &leader,
        "PUT",
        "/api/v1/jobs/7/failure-keep",
        serde_json::json!({"keep":false}),
    )
    .await;
    assert_eq!(pending["applied"], false);
    reconcile_one(&leader.runtime.leader_shared).await.unwrap();
    let (_, pending) = request(
        &leader,
        "GET",
        "/api/v1/jobs/7/failure-keep",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(pending["applied"], false);
    owner = Node::start(temp.path(), "source-owner", false).await;
    wait_owner(&leader, &owner).await;
    // Advance only this fixture's persistent backoff to exercise a due retry.
    let mut value = load(control, 7).await.unwrap().unwrap();
    for entry in &mut value.targets {
        entry.next_retry = 0;
    }
    save(
        control,
        &value,
        leader.runtime.leader_shared.authority_identity(),
    )
    .await
    .unwrap();
    reconcile_one(&leader.runtime.leader_shared).await.unwrap();
    let (_, confirmed) = request(
        &leader,
        "GET",
        "/api/v1/jobs/7/failure-keep",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(confirmed["applied"], true, "{confirmed}");
    assert_eq!(confirmed["keep"], false);
    let inventory = owner.runtime.engine.artifacts();
    assert!(
        !inventory
            .get(&copy.source.artifact)
            .unwrap()
            .keep_on_failure
    );
    // Cleanup already owns the exact physical guard before a fresh Keep asks.
    let obligation = record(copy.clone()).copies.remove(0);
    let guard = owner
        .runtime
        .leader_shared
        .copy_guards
        .lock_for(&obligation);
    let executing = guard.lock().await;
    assert!(
        check_copy_policy(&owner.runtime.leader_shared, &obligation, "UNPACK_FAILURE")
            .await
            .unwrap()
            .is_none()
    );
    let (_, pending) = request(
        &leader,
        "PUT",
        "/api/v1/jobs/7/failure-keep",
        serde_json::json!({"keep":true}),
    )
    .await;
    assert_eq!(pending["applied"], false);
    let pending_owner = tokio::spawn({
        let shared = leader.runtime.leader_shared.clone();
        async move { reconcile_one(&shared).await }
    });
    let waiting = tokio::time::Instant::now() + Duration::from_secs(3);
    while Arc::strong_count(&guard) < 2 {
        assert!(
            tokio::time::Instant::now() < waiting,
            "owner never reached exact copy guard"
        );
        tokio::task::yield_now().await;
    }
    let files = vec![DeliveryFile {
        path: "archive.bin".into(),
        size_bytes: 12,
        sha256: format!("{:x}", Sha256::digest(b"source bytes")),
    }];
    let cleanup = inventory
        .retire_cluster_artifact_copy_with_disposition(
            &copy,
            "UNPACK_FAILURE",
            FinalizationProof::Cluster {
                receipt: "keep-race-receipt".into(),
                accepted_at_ms: 1000,
            },
            true,
            Some(&files),
        )
        .unwrap();
    assert_eq!(cleanup.status, "removed");
    assert!(!path.exists());
    drop(executing);
    pending_owner.await.unwrap().unwrap();
    let (_, gone) = request(
        &leader,
        "GET",
        "/api/v1/jobs/7/failure-keep",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(gone["available"], false, "{gone}");
    assert_eq!(gone["keep"], false);
    assert!(gone["message"].as_str().unwrap().contains("cannot restore"));
    let queued = nzbd_types::Job {
        id: nzbd_types::JobId(9),
        kind: nzbd_types::JobKind::Nzb,
        name: "Keep queue intent".into(),
        dir_name: "queued-nine".into(),
        name_provisional: false,
        delivery_lifecycle: true,
        queued_at_unix: 1,
        original_name: String::new(),
        category: None,
        priority: 0,
        dupe: Default::default(),
        params: vec![],
        files: vec![],
        totals: Default::default(),
        status: nzbd_types::JobStatus::Paused,
        torrent: None,
        stages: vec![],
    };
    control
        .seed_job(
            9,
            "queued-incarnation",
            &serde_json::to_string(&queued).unwrap(),
        )
        .await
        .unwrap();
    let (status, queued_keep) = request(
        &leader,
        "PUT",
        "/api/v1/jobs/9/failure-keep",
        serde_json::json!({"keep":true}),
    )
    .await;
    assert_eq!(status, 200, "{queued_keep}");
    let (current, revision, _) = control.current_job_intent(9).await.unwrap().unwrap();
    assert_eq!(revision, 2);
    assert_eq!(current.name, queued.name);
    assert!(current
        .params
        .iter()
        .any(|(key, value)| key == "*Runner:KeepOnFailure" && value == "1"));
    request(
        &leader,
        "PUT",
        "/api/v1/jobs/9/failure-keep",
        serde_json::json!({"keep":true}),
    )
    .await;
    assert_eq!(
        control.current_job_intent(9).await.unwrap().unwrap().1,
        2,
        "retry duplicated the current job mutation"
    );
    owner.stop().await;
    leader.stop().await;
}

#[test]
fn copy_guard_uses_exact_physical_identity_and_drops_unused_entries() {
    let target = Target {
        id: "x".into(),
        owner: "worker".into(),
        job: 7,
        delivery: None,
        copy_id: "one".into(),
        artifact: None,
        local_delivery: None,
        result_ref: Some("/root/job-7/work/fence-1".into()),
        result_id: Some("result".into()),
        directory_identity: Some((1, 2)),
        initial_keep: false,
        removed: false,
    };
    let guards = CopyGuards::default();
    let a = guards.lock_for(&target.obligation());
    let mut duplicate = target.obligation();
    duplicate.id = "different-receipt-alias".into();
    let b = guards.lock_for(&duplicate);
    assert!(Arc::ptr_eq(&a, &b));
    duplicate.directory_identity = Some((1, 3));
    let c = guards.lock_for(&duplicate);
    assert!(!Arc::ptr_eq(&a, &c));
    drop(a);
    drop(b);
    drop(c);
    let _last = guards.lock_for(&target.obligation());
    assert_eq!(guards.0.lock().unwrap().len(), 1);
}
#[test]
fn pending_reply_never_claims_unconfirmed_application_or_restored_source() {
    let target = Target {
        id: "x".into(),
        owner: "worker".into(),
        job: 7,
        delivery: None,
        copy_id: "one".into(),
        artifact: None,
        local_delivery: Some("local".into()),
        result_ref: None,
        result_id: None,
        directory_identity: None,
        initial_keep: false,
        removed: false,
    };
    let mut preference = Preference {
        job: 7,
        desired: Some(true),
        desired_revision: 1,
        queue_keep: None,
        version: 1,
        cursor: 0,
        targets: vec![TargetState {
            target,
            ack_revision: None,
            available: None,
            keep: None,
            observed_at: 0,
            attempts: 0,
            next_retry: 0,
            blocked_since: None,
            error: Some("Owner offline".into()),
        }],
    };
    let pending = reply(&preference, 0);
    assert!(!pending.applied);
    assert_eq!(pending.pending_owners, vec!["worker"]);
    assert!(pending.keep);
    preference.targets[0].available = Some(false);
    preference.targets[0].ack_revision = Some(1);
    preference.targets[0].error = None;
    let gone = reply(&preference, 0);
    assert!(!gone.available && !gone.keep && gone.applied);
}
