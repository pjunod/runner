use super::*;
use crate::control::{LeaseClaim, LeaseToken};
use nzbd_state::artifacts::{
    ClusterArtifactCopy, DeliveryFile, FinalizationProof, Inventory, WorkerControlContext,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

struct Fixture {
    temp: tempfile::TempDir,
    control: ControlStore,
    cfg: crate::ClusterConfig,
    root: PathBuf,
    request: CancellationRequest,
    job: nzbd_types::Job,
    issuer: LeaseToken,
    owner: Arc<Inventory>,
    source: ClusterArtifactCopy,
    raw_files: Vec<DeliveryFile>,
}
impl Fixture {
    async fn new() -> Self {
        Self::new_with_delivery(true).await
    }
    async fn new_with_delivery(delivery: bool) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let raft = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let api = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let cfg = crate::ClusterConfig {
            cluster_id: "cancellation-test".into(),
            node_name: "authority".into(),
            shared_dir: temp.path().join("shared"),
            advertise_url: "http://127.0.0.1:1".into(),
            secret: "test-secret".into(),
            coordinator: true,
            priority: 0,
            download: false,
            max_download_jobs: 0,
            post_process: false,
            pp_slots: 0,
            lease_interval: Duration::from_millis(100),
            takeover_after: Duration::from_secs(1),
            worker_ttl: Duration::from_secs(2),
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
        let control = ControlStore::start(&cfg).await.unwrap();
        install_schema(&control).await.unwrap();
        control
            .client
            .txn(
                crate::grant_receipts::SCHEMA
                    .iter()
                    .map(|sql| ((*sql).to_owned(), params!()))
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        control
            .initialize_delivery_identity("cancel-instance", "authority")
            .await
            .unwrap();
        let LeaseClaim::Acquired(issuer) = control
            .acquire(
                "cluster/leader",
                "cancellation-test",
                1,
                "leader",
                "authority",
                "leader-boot",
                "leader",
                r#"{"lifecycleProtocol":2}"#,
                1,
                Duration::from_secs(180),
            )
            .await
            .unwrap()
        else {
            panic!("leader")
        };
        let owner = Arc::new(Inventory::open(&temp.path().join("owner-state")).unwrap());
        let raw_root = temp.path().join("raw");
        std::fs::create_dir(&raw_root).unwrap();
        let raw = raw_root.join("source");
        owner
            .allocate_for_delivery(7, &raw_root, &raw, true, delivery)
            .unwrap();
        std::fs::write(raw.join("archive.bin"), b"original independent raw bytes").unwrap();
        owner.finish(7, &raw, &raw_root, "completed").unwrap();
        let source = owner.cluster_download_authority_source(7).unwrap().unwrap();
        let raw_files = vec![DeliveryFile {
            path: "archive.bin".into(),
            size_bytes: 30,
            sha256: format!("{:x}", Sha256::digest(b"original independent raw bytes")),
        }];
        let context = WorkerControlContext {
            lease_id: "work/job/7/post@1".into(),
            job_incarnation: "cancel-incarnation".into(),
            job_revision: 1,
            control_revision: 1,
            epoch: 1,
        };
        let job = nzbd_types::Job {
            id: nzbd_types::JobId(7),
            kind: nzbd_types::JobKind::Nzb,
            name: "cancelled".into(),
            dir_name: "source".into(),
            name_provisional: false,
            delivery_lifecycle: delivery,
            queued_at_unix: 123,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: Default::default(),
            params: vec![
                (
                    nzbd_state::artifacts::WORKER_CONTROL_CONTEXT_PARAM.into(),
                    serde_json::to_string(&context).unwrap(),
                ),
                (
                    crate::delivery::AUTHORITY_COPY_PARAM.into(),
                    serde_json::to_string(&crate::delivery::AuthoritySourceCopy {
                        owner_node: "source-owner".into(),
                        copy: source.clone(),
                    })
                    .unwrap(),
                ),
                ("monarr-transfer".into(), "cancel-request".into()),
            ],
            files: Vec::new(),
            totals: Default::default(),
            status: nzbd_types::JobStatus::Completed,
            torrent: None,
            stages: Vec::new(),
        };
        control
            .seed_job(
                7,
                "cancel-incarnation",
                &serde_json::to_string(&job).unwrap(),
            )
            .await
            .unwrap();
        let LeaseClaim::Acquired(token) = control
            .acquire(
                "work/job/7/post",
                "cancellation-test",
                7,
                "cancel-incarnation",
                "worker",
                "worker-boot",
                "post",
                "{}",
                1,
                Duration::from_secs(180),
            )
            .await
            .unwrap()
        else {
            panic!("work")
        };
        let root = temp.path().join("shared/.nzbd-cluster/generations");
        let key = format!("{:x}", Sha256::digest(token.resource.as_bytes()));
        let path = root
            .join("job-7")
            .join(format!("work-{}", &key[..16]))
            .join("fence-1");
        std::fs::create_dir_all(path.join("files")).unwrap();
        std::fs::write(path.join("files/video.bin"), b"sealed output").unwrap();
        let job_bytes = serde_json::to_vec(&job).unwrap();
        std::fs::write(path.join("job.json"), &job_bytes).unwrap();
        let mut manifest = crate::worker::GenerationManifest {
            job_id: 7,
            job_incarnation: "cancel-incarnation".into(),
            owner_node_id: "worker".into(),
            fence: 1,
            result_id: String::new(),
            job_sha256: format!("{:x}", Sha256::digest(&job_bytes)),
            files: vec![crate::worker::GenerationFile {
                path: "video.bin".into(),
                bytes: 13,
                sha256: format!("{:x}", Sha256::digest(b"sealed output")),
            }],
            total_bytes: 13,
            sealed_at_unix_ms: crate::control::unix_ms_public(),
            artifact_copy: None,
            directory_identity: Some(crate::worker::generation_directory_identity(&path).unwrap()),
            file_outcomes: Vec::new(),
            identity_version: 3,
            source_disposition: None,
        };
        rehash(&mut manifest);
        std::fs::write(
            path.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        let request = CancellationRequest {
            lifecycle_protocol: 2,
            node: "worker".into(),
            job_id: 7,
            job_incarnation: "cancel-incarnation".into(),
            lease_id: context.lease_id,
            token,
            expected_job_revision: 1,
            result_id: manifest.result_id,
            result_ref: path.to_str().unwrap().into(),
            receipt_id: "cancel-result-receipt".into(),
        };
        Self {
            temp,
            control,
            cfg,
            root,
            request,
            job,
            issuer,
            owner,
            source,
            raw_files,
        }
    }
    fn issuer(&self) -> (&str, &str, u64) {
        (
            &self.issuer.owner_node_id,
            &self.issuer.owner_incarnation,
            self.issuer.fence,
        )
    }
    async fn remove(&self, mode: RemovalMode) {
        let envelope = serde_json::json!({"next_intent":serde_json::to_string(&self.job).unwrap(),"deleted":true});
        let mut writes=vec![crate::control::authority_statement(self.issuer()).unwrap(),("INSERT INTO command_receipts(command_id,job_id,expected_revision,applied_revision,command_json,applied_at_ms) VALUES('remove-test',7,1,2,?1,?2)".into(),params!(envelope.to_string(),crate::control::unix_ms_public()))];
        writes.extend(tombstone_statements(
            7,
            "cancel-incarnation",
            1,
            2,
            mode,
            crate::control::unix_ms_public(),
        ));
        self.control
            .client
            .txn(writes)
            .await
            .unwrap()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    }
    async fn decide(&self) -> Result<CancellationDecision, String> {
        decide(&self.control, &self.root, &self.request, self.issuer()).await
    }
    fn ack(&self, decision: &CancellationDecision) -> OwnerStopAck {
        OwnerStopAck {
            cleanup_id: decision.cleanup_id.clone(),
            copy_id: format!(
                "artifact:{}:{}:{}",
                self.source.storage_instance_id,
                self.source.publication.artifact,
                self.source.publication.generation
            ),
            job_id: 7,
            job_incarnation: "cancel-incarnation".into(),
            removal_applied_revision: 2,
            copy: self.source.clone(),
            files: self.raw_files.clone(),
        }
    }
    fn raw_path(&self) -> PathBuf {
        self.owner
            .get(&self.source.publication.artifact)
            .unwrap()
            .path
    }
    async fn close(self) {
        self.control.shutdown().await.unwrap();
    }
}
fn rehash(m: &mut crate::worker::GenerationManifest) {
    m.result_id = format!(
        "sha256:{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                m.job_id,
                &m.job_incarnation,
                &m.owner_node_id,
                m.fence,
                &m.job_sha256,
                &m.files,
                &m.artifact_copy,
                &m.directory_identity,
                &m.file_outcomes,
                &m.source_disposition
            ))
            .unwrap()
        )
    );
}

#[test]
fn only_explicit_byte_actions_select_delete_mode() {
    assert_eq!(
        RemovalMode::native_action("delete-files"),
        Some(RemovalMode::DeleteFiles)
    );
    assert_eq!(
        RemovalMode::compat_action("GroupFinalDelete"),
        Some(RemovalMode::DeleteFiles)
    );
    for action in ["GroupDelete", "GroupParkDelete", "GroupTrashDelete"] {
        assert_eq!(
            RemovalMode::compat_action(action),
            Some(RemovalMode::KeepFiles)
        );
    }
    assert_eq!(
        RemovalMode::native_action("delete"),
        Some(RemovalMode::KeepFiles)
    );
    assert_eq!(RemovalMode::compat_action("Unknown"), None);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_exact_result_gets_private_cleanup_and_raw_owner_must_ack() {
    let f = Fixture::new().await;
    f.remove(RemovalMode::DeleteFiles).await;
    let decision = f.decide().await.unwrap();
    assert_eq!(decision.disposition, "cleanup_pending");
    let record = f
        .control
        .cluster_delivery(&decision.cleanup_id)
        .await
        .unwrap()
        .unwrap();
    assert!(record.cleanup_only);
    assert!(record.delivery.files.is_empty());
    assert!(record.delivery.publication_path.is_empty());
    assert!(record.delivery.accepted_at.is_none() && record.delivery.consumer_receipt_id.is_none());
    let raw = record
        .copies
        .iter()
        .find(|copy| copy.artifact.is_some())
        .unwrap();
    assert_eq!(raw.status, "review_required");
    assert!(raw.deletion_files.is_none());
    let mut ack = f.ack(&decision);
    let mut executing = record.clone();
    let raw_index = executing
        .copies
        .iter()
        .position(|copy| copy.artifact.is_some())
        .unwrap();
    executing.copies[raw_index].status = "running".into();
    executing.copies[raw_index].claim_id = Some("live-claim".into());
    f.control.update_cluster_delivery(&executing).await.unwrap();
    assert!(
        acknowledge_owner_stop(&f.control, &ack, "source-owner", f.issuer())
            .await
            .unwrap_err()
            .contains("executing")
    );
    let mut settled = f
        .control
        .cluster_delivery(&decision.cleanup_id)
        .await
        .unwrap()
        .unwrap();
    settled.copies[raw_index].status = "review_required".into();
    settled.copies[raw_index].claim_id = None;
    f.control.update_cluster_delivery(&settled).await.unwrap();
    assert!(
        acknowledge_owner_stop(&f.control, &ack, "wrong-owner", f.issuer())
            .await
            .is_err()
    );
    ack.copy.source.generation.push_str("-changed");
    assert!(
        acknowledge_owner_stop(&f.control, &ack, "source-owner", f.issuer())
            .await
            .is_err()
    );
    ack = f.ack(&decision);
    assert!(
        acknowledge_owner_stop(&f.control, &ack, "source-owner", f.issuer())
            .await
            .unwrap()
    );
    let record = f
        .control
        .cluster_delivery(&decision.cleanup_id)
        .await
        .unwrap()
        .unwrap();
    let raw = record
        .copies
        .iter()
        .find(|copy| copy.artifact.is_some())
        .unwrap();
    assert_eq!(raw.status, "pending");
    assert_eq!(raw.deletion_files.as_ref().unwrap()[0].path, "archive.bin");
    let mut protected = record.clone();
    protected.copies[raw_index].status = "retained".into();
    protected.copies[raw_index].error = Some("recovery dependency remains".into());
    protected.copies[raw_index].claim_id = Some("finished-claim".into());
    protected.copies[raw_index].next_retry_at = Some(crate::control::unix_ms_public() / 1000 + 120);
    f.control.update_cluster_delivery(&protected).await.unwrap();
    assert!(
        !acknowledge_owner_stop(&f.control, &ack, "source-owner", f.issuer())
            .await
            .unwrap()
    );
    assert_eq!(
        f.control
            .cluster_delivery(&decision.cleanup_id)
            .await
            .unwrap()
            .unwrap()
            .copies,
        protected.copies
    );
    ack.files[0].sha256 = "0".repeat(64);
    assert!(
        acknowledge_owner_stop(&f.control, &ack, "source-owner", f.issuer())
            .await
            .is_err()
    );
    assert!(f.raw_path().exists());
    assert!(Path::new(&f.request.result_ref).exists());
    f.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_delete_drains_exact_sealed_and_owned_raw_copies_and_replay_never_reads_them() {
    for delivery in [true, false] {
        let mut f = Fixture::new_with_delivery(delivery).await;
        let source = f.owner.get(&f.source.publication.artifact).unwrap();
        f.owner
            .retention(&source.id, source.revision, true, None)
            .unwrap();
        f.owner.set_keep_on_failure(7, true).unwrap();
        let unrelated = f.raw_path().parent().unwrap().join("unrelated-kept");
        let other = f
            .owner
            .allocate_for_delivery(8, unrelated.parent().unwrap(), &unrelated, true, false)
            .unwrap();
        std::fs::write(unrelated.join("keep.bin"), b"unrelated kept bytes").unwrap();
        f.owner
            .finish(8, &unrelated, unrelated.parent().unwrap(), "completed")
            .unwrap();
        let other = f.owner.get(&other.id).unwrap();
        f.owner
            .retention(&other.id, other.revision, true, None)
            .unwrap();
        f.owner.set_keep_on_failure(8, true).unwrap();
        f.remove(RemovalMode::DeleteFiles).await;
        let decision = f.decide().await.unwrap();
        acknowledge_owner_stop(&f.control, &f.ack(&decision), "source-owner", f.issuer())
            .await
            .unwrap();
        let record = f
            .control
            .cluster_delivery(&decision.cleanup_id)
            .await
            .unwrap()
            .unwrap();
        let generation = record
            .copies
            .iter()
            .find(|copy| copy.result_ref.is_some())
            .unwrap();
        let path = Path::new(generation.result_ref.as_ref().unwrap());
        nzbd_state::artifacts::remove_cluster_generation_copy(
            &f.root,
            path.strip_prefix(&f.root).unwrap(),
            generation.directory_identity.unwrap(),
            generation.deletion_files.as_ref().unwrap(),
        )
        .unwrap();
        assert!(!path.exists());
        let raw = record
            .copies
            .iter()
            .find(|copy| copy.artifact.is_some())
            .unwrap();
        let result = f
            .owner
            .delete_cancelled_cluster_artifact_copy(
                raw.artifact.as_ref().unwrap(),
                FinalizationProof::Cluster {
                    receipt: record.receipt_id,
                    accepted_at_ms: decision.accepted_at_ms,
                },
                raw.deletion_files.as_deref().unwrap(),
            )
            .unwrap();
        assert_eq!(result.status, "removed");
        assert!(!f.raw_path().exists());
        assert_eq!(
            std::fs::read(unrelated.join("keep.bin")).unwrap(),
            b"unrelated kept bytes"
        );
        let other = f.owner.for_job(8).unwrap().unwrap();
        assert!(other.keep && other.keep_on_failure);
        f.control.shutdown().await.unwrap();
        f.control = ControlStore::start(&f.cfg).await.unwrap();
        assert_eq!(decision, f.decide().await.unwrap());
        let mut changed = f.request.clone();
        changed.result_id.push('x');
        assert!(decide(&f.control, &f.root, &changed, f.issuer())
            .await
            .is_err());
        f.close().await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remove_keep_and_completed_retirement_never_authorize_cleanup() {
    for mode in [RemovalMode::KeepFiles, RemovalMode::CompletedRetirement] {
        let f = Fixture::new().await;
        f.remove(mode).await;
        if mode == RemovalMode::CompletedRetirement {
            assert!(f
                .decide()
                .await
                .unwrap_err()
                .contains("no cancellation cleanup"));
        } else {
            let decision = f.decide().await.unwrap();
            assert_eq!(decision.disposition, "retained");
            let record = f
                .control
                .cluster_delivery(&decision.cleanup_id)
                .await
                .unwrap()
                .unwrap();
            assert!(record
                .copies
                .iter()
                .all(|copy| copy.status == "review_required"));
            assert!(acknowledge_owner_stop(
                &f.control,
                &f.ack(&decision),
                "source-owner",
                f.issuer()
            )
            .await
            .is_err());
            assert_eq!(decision, f.decide().await.unwrap());
        }
        assert!(Path::new(&f.request.result_ref).exists());
        assert!(f.raw_path().exists());
        f.close().await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_or_missing_historical_grant_is_retained_without_a_decision() {
    let f = Fixture::new().await;
    assert!(f
        .decide()
        .await
        .unwrap_err()
        .contains("durable user removal"));
    f.remove(RemovalMode::DeleteFiles).await;
    f.control
        .client
        .execute(
            "DELETE FROM work_grant_receipts WHERE resource=?1",
            params!(&f.request.token.resource),
        )
        .await
        .unwrap();
    assert!(f.decide().await.unwrap_err().contains("historical grant"));
    assert!(existing_decision(&f.control, &f.request)
        .await
        .unwrap()
        .is_none());
    assert!(Path::new(&f.request.result_ref).exists());
    assert!(f.raw_path().exists());
    f.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changed_seal_wrong_context_outside_path_and_stale_authority_refuse() {
    let f = Fixture::new().await;
    f.remove(RemovalMode::DeleteFiles).await;
    for field in ["path", "owner", "fence", "revision", "issuer"] {
        let mut req = f.request.clone();
        let mut issuer = f.issuer();
        match field {
            "path" => req.result_ref = f.temp.path().to_str().unwrap().into(),
            "owner" => {
                req.node = "other".into();
                req.token.owner_node_id = req.node.clone();
            }
            "fence" => req.token.fence += 1,
            "revision" => req.expected_job_revision += 1,
            _ => issuer.2 += 1,
        }
        assert!(
            decide(&f.control, &f.root, &req, issuer).await.is_err(),
            "accepted {field}"
        );
        assert!(existing_decision(&f.control, &f.request)
            .await
            .unwrap()
            .is_none());
    }
    std::fs::write(
        Path::new(&f.request.result_ref).join("job.json"),
        b"changed",
    )
    .unwrap();
    assert!(f.decide().await.unwrap_err().contains("digest"));
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn regrant_history_proves_old_seal_but_never_implies_other_writers_stopped() {
    let f = Fixture::new().await;
    assert!(f.control.release(&f.request.token).await.unwrap());
    let LeaseClaim::Acquired(next) = f
        .control
        .acquire(
            &f.request.token.resource,
            "cancellation-test",
            7,
            "cancel-incarnation",
            "other-worker",
            "other-boot",
            "post",
            "{}",
            1,
            Duration::from_secs(180),
        )
        .await
        .unwrap()
    else {
        panic!("regrant")
    };
    assert!(next.fence > f.request.token.fence);
    f.remove(RemovalMode::DeleteFiles).await;
    let decision = f.decide().await.unwrap();
    let record = f
        .control
        .cluster_delivery(&decision.cleanup_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record
            .copies
            .iter()
            .find(|copy| copy.artifact.is_some())
            .unwrap()
            .status,
        "review_required"
    );
    assert!(f.raw_path().exists());
    f.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn existing_consumer_publication_is_never_replaced_by_cancellation() {
    let f = Fixture::new().await;
    f.remove(RemovalMode::DeleteFiles).await;
    let proof = sealed_proof(&f.root, &f.request).unwrap();
    let removal = f
        .control
        .cancellation_removal(7, "cancel-incarnation")
        .await
        .unwrap()
        .unwrap();
    let d = CancellationDecision {
        receipt_id: f.request.receipt_id.clone(),
        cleanup_id: "existing-consumer-publication".into(),
        accepted_at_ms: crate::control::unix_ms_public(),
        disposition: "retained".into(),
        reason: "fixture publication".into(),
    };
    let mut record =
        cancellation_record(&f.request, &proof, &removal, &d, "cancel-instance".into()).unwrap();
    record.cleanup_only = false;
    record.delivery.status = "accepted".into();
    record.delivery.consumer_receipt_id = Some("actual-consumer-receipt".into());
    f.control.commit_delivery(&record).await.unwrap();
    assert!(f
        .decide()
        .await
        .unwrap_err()
        .contains("consumer publication custody"));
    assert!(existing_decision(&f.control, &f.request)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        f.control
            .cluster_delivery(&d.cleanup_id)
            .await
            .unwrap()
            .unwrap()
            .delivery
            .consumer_receipt_id
            .as_deref(),
        Some("actual-consumer-receipt")
    );
    assert!(f.raw_path().exists());
    f.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn immutable_remove_keep_cannot_be_upgraded_to_delete_files() {
    let f = Fixture::new().await;
    f.remove(RemovalMode::KeepFiles).await;
    let results = f
        .control
        .client
        .txn(tombstone_statements(
            7,
            "cancel-incarnation",
            1,
            2,
            RemovalMode::DeleteFiles,
            crate::control::unix_ms_public(),
        ))
        .await;
    assert!(results.is_err() || results.unwrap().into_iter().any(|result| result.is_err()));
    assert_eq!(
        f.control
            .cancellation_removal(7, "cancel-incarnation")
            .await
            .unwrap()
            .unwrap()
            .mode,
        RemovalMode::KeepFiles
    );
    assert_eq!(f.decide().await.unwrap().disposition, "retained");
    f.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn active_use_and_unknown_raw_members_still_block_explicit_cancellation() {
    for block in ["active", "unknown"] {
        let f = Fixture::new().await;
        f.remove(RemovalMode::DeleteFiles).await;
        let decision = f.decide().await.unwrap();
        acknowledge_owner_stop(&f.control, &f.ack(&decision), "source-owner", f.issuer())
            .await
            .unwrap();
        let guard = if block == "active" {
            f.owner.acquire_terminal_use(7, &|| Ok(())).unwrap()
        } else {
            None
        };
        if block == "unknown" {
            std::fs::write(f.raw_path().join("unrecorded.bin"), b"unowned addition").unwrap();
        }
        let result = f.owner.delete_cancelled_cluster_artifact_copy(
            &f.source,
            FinalizationProof::Cluster {
                receipt: f.request.receipt_id.clone(),
                accepted_at_ms: decision.accepted_at_ms,
            },
            &f.raw_files,
        );
        assert!(
            result.is_err() || result.unwrap().status != "removed",
            "bypassed {block}"
        );
        assert_eq!(
            std::fs::read(f.raw_path().join("archive.bin")).unwrap(),
            b"original independent raw bytes"
        );
        drop(guard);
        f.close().await;
    }
}
