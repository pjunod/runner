use super::*;
use std::sync::Arc;

fn fixture() -> (tempfile::TempDir, Inventory, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("processing");
    std::fs::create_dir(&root).unwrap();
    let inventory = Inventory::open(&tmp.path().join("state")).unwrap();
    (tmp, inventory, root)
}
fn parked(db: &Inventory, root: &Path) -> Artifact {
    let path = root.join("job");
    db.allocate(1, root, &path).unwrap();
    std::fs::write(path.join("episode.mkv"), b"original media").unwrap();
    db.finish(1, &path, root, "parked_failed").unwrap()
}

#[test]
fn remount_identity_requires_same_source_type_and_filesystem_root() {
    let identity = |device, mount_id: &str| Identity {
        device,
        inode: 17,
        mount_id: mount_id.to_owned(),
        bytes: 0,
        modified: String::new(),
        changed: String::new(),
        directory: true,
    };
    let original = identity(10, "linux\n4:ext4\n8:/dev/disk-a\n1:/\n8:/media/library");
    let remounted = identity(99, "linux\n4:ext4\n8:/dev/disk-a\n1:/\n8:/media/library");
    let reused_device = identity(10, "linux\n4:ext4\n8:/dev/disk-b\n1:/\n8:/media/library");
    assert!(original.same_object(&remounted));
    assert!(!original.same_object(&reused_device));
}

#[test]
fn legacy_mount_identity_requires_same_device() {
    let identity = |device| Identity {
        device,
        inode: 17,
        mount_id: String::new(),
        bytes: 0,
        modified: String::new(),
        changed: String::new(),
        directory: true,
    };
    assert!(identity(10).same_object(&identity(10)));
    assert!(!identity(10).same_object(&identity(99)));
}

#[test]
fn unavailable_root_becoming_available_again_recovers_same_artifact() {
    let (tmp, db, root) = fixture();
    let artifact = parked(&db, &root);
    let offline = tmp.path().join("downloads-offline");
    std::fs::rename(&root, &offline).unwrap();
    assert!(db.verify_root(&artifact).is_err());
    std::fs::rename(&offline, &root).unwrap();
    assert!(db.verify_root(&artifact).is_ok());
    assert!(db.verify(&artifact).is_ok());
}

#[test]
fn delivery_manifest_and_acceptance_survive_history_independently() {
    let (_tmp, db, root) = fixture();
    let path = root.join("published");
    db.allocate_for_delivery(44, &root, &path, true, true)
        .unwrap();
    std::fs::write(path.join("movie.mkv"), b"media bytes").unwrap();
    std::fs::write(path.join("notes.nfo"), b"metadata").unwrap();
    db.finish(44, &path, &root, "active").unwrap();

    let delivery = db.create_delivery(44, true, Some("t-44-test")).unwrap();
    let transferred = db.for_job(44).unwrap().unwrap();
    assert_eq!(transferred.state, "transferred");
    assert!(!transferred.owned);
    assert!(!transferred.keep);
    assert!(db
        .request_delete(&transferred.id, transferred.revision, "must-not-delete", 0)
        .is_err());
    assert_eq!(delivery.protocol_version, 1);
    assert_eq!(delivery.sequence, 1);
    assert_eq!(delivery.files.len(), 2);
    assert_eq!(delivery.files[0].path, "movie.mkv");
    assert_eq!(delivery.files[0].size_bytes, b"media bytes".len() as u64);
    assert_eq!(
        db.create_delivery(44, true, Some("t-44-test")).unwrap(),
        delivery
    );

    let (pending, cursor) = db.deliveries_after(0, 10).unwrap();
    assert_eq!(cursor, 1);
    assert_eq!(pending, vec![delivery.clone()]);
    let accepted = db
        .accept_delivery(
            &delivery.delivery_id,
            &delivery.publication_generation,
            &delivery.manifest_digest,
            "curator-receipt-44",
        )
        .unwrap();
    assert!(accepted.accepted_at.is_some());
    assert_eq!(
        db.accept_delivery(
            &delivery.delivery_id,
            &delivery.publication_generation,
            &delivery.manifest_digest,
            "curator-receipt-44",
        )
        .unwrap(),
        accepted
    );
    assert!(db
        .accept_delivery(
            &delivery.delivery_id,
            &delivery.publication_generation,
            &delivery.manifest_digest,
            "different-receipt",
        )
        .is_err());
    assert!(db.deliveries_after(0, 10).unwrap().0.is_empty());
    assert_eq!(db.delivery(&delivery.delivery_id).unwrap(), accepted);
}

#[test]
fn pre_upgrade_jobs_cannot_create_automatic_delivery_receipts() {
    let (_tmp, db, root) = fixture();
    let path = root.join("legacy");
    db.allocate(45, &root, &path).unwrap();
    std::fs::write(path.join("media.mkv"), b"legacy media").unwrap();
    db.finish(45, &path, &root, "active").unwrap();

    assert!(db.create_delivery(45, false, None).is_err());
    assert_eq!(db.deliveries_after(0, 10).unwrap(), (vec![], 0));
    assert!(path.join("media.mkv").exists());
}

#[test]
fn pre_upgrade_artifacts_cannot_run_queued_automatic_deletion() {
    let (_tmp, db, root) = fixture();
    let legacy = parked(&db, &root);
    let op = db
        .request_delete_authorized(&legacy.id, legacy.revision, "legacy-retention", 0, true)
        .unwrap();
    let result = db.execute_delete(&op.id).unwrap();
    assert_eq!(result.state, "review");
    assert!(legacy.path.exists());
    assert!(result
        .error
        .as_deref()
        .is_some_and(|message| message.contains("pre-existing artifact")));
}

#[test]
fn pre_upgrade_explicit_delete_is_frozen_but_new_delete_remains_available() {
    let (tmp, db, root) = fixture();
    let legacy = parked(&db, &root);
    let pending = db
        .request_delete(&legacy.id, legacy.revision, "old-explicit-delete", 0)
        .unwrap();
    // A pre-upgrade failed deletion is resumable after restart too; it is not
    // safe merely because its first execution already entered retry state.
    db.db
        .lock()
        .unwrap()
        .execute(
            "UPDATE operations SET state='retry',data=json_set(data,'$.state','retry') WHERE id=?1",
            [&pending.id],
        )
        .unwrap();
    // Simulate an inventory opened before the lifecycle migration marker.
    db.db
        .lock()
        .unwrap()
        .execute(
            "DELETE FROM meta WHERE key='delivery_backlog_migration'",
            [],
        )
        .unwrap();
    db.close().unwrap();

    let migrated = Inventory::open(&tmp.path().join("state")).unwrap();
    let old = migrated.execute_delete(&pending.id).unwrap();
    assert_eq!(old.state, "review");
    assert!(legacy.path.exists());
    let reviewed = migrated
        .request_delete(&legacy.id, legacy.revision, "new-reviewed-delete", 0)
        .unwrap();
    assert_eq!(
        migrated.execute_delete(&reviewed.id).unwrap().state,
        "succeeded"
    );
}

#[test]
fn interrupted_running_source_cleanup_is_retried_after_reopen() {
    let (tmp, db, root) = fixture();
    let path = root.join("published");
    db.allocate_for_delivery(46, &root, &path, true, true)
        .unwrap();
    std::fs::write(path.join("media.mkv"), b"payload").unwrap();
    db.finish(46, &path, &root, "active").unwrap();
    let delivery = db.create_delivery(46, true, Some("t-46-test")).unwrap();
    db.db
        .lock()
        .unwrap()
        .execute(
            "UPDATE deliveries SET cleanup_status='running',cleanup_attempts=1 WHERE id=?1",
            [&delivery.delivery_id],
        )
        .unwrap();
    db.db.lock().unwrap().execute(
        "INSERT INTO delivery_cleanup_attempts(delivery_id,attempt,started_at,wait_ms,outcome) VALUES(?1,1,?2,0,'running')",
        rusqlite::params![delivery.delivery_id, now_millis()],
    ).unwrap();
    db.close().unwrap();

    let restarted = Inventory::open(&tmp.path().join("state")).unwrap();
    restarted.retry_delivery_cleanup().unwrap();
    let recovered = restarted.delivery(&delivery.delivery_id).unwrap();
    assert_eq!(recovered.source_cleanup.status, "succeeded");
    assert_eq!(recovered.source_cleanup.attempts, 2);
    assert_eq!(recovered.source_cleanup.attempt_history.len(), 2);
    assert_eq!(
        recovered.source_cleanup.attempt_history[0].outcome,
        "interrupted"
    );
    assert!(recovered.source_cleanup.attempt_history[0]
        .work_ms
        .is_some());
    assert_eq!(
        recovered.source_cleanup.attempt_history[1].outcome,
        "succeeded"
    );
}

#[test]
fn delivery_source_copy_is_retired_after_durable_manifest() {
    let (_tmp, db, root) = fixture();
    let source = root.join("working");
    db.allocate_for_delivery(47, &root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("media.mkv"), b"payload").unwrap();
    db.finish(47, &source, &root, "active").unwrap();
    let destination_root = _tmp.path().join("completed");
    std::fs::create_dir(&destination_root).unwrap();
    let destination = destination_root.join("release");

    fs::RENAME_FAILURE.with(|failure| failure.set(Some(libc::EINVAL)));
    let moved = db
        .relocate_for_delivery_checked(47, &destination, &|| Ok(()))
        .unwrap();
    assert_eq!(moved.published_path, destination);
    assert!(source.exists(), "copy publication retains its source");
    let delivery = db.create_delivery(47, true, Some("t-47-test")).unwrap();
    let updated = db.retire_delivery_source(&delivery.delivery_id).unwrap();
    assert_eq!(updated.source_cleanup.status, "succeeded");
    assert!(updated.source_cleanup.started_at.is_some());
    assert!(updated.source_cleanup.last_attempt_at.is_some());
    assert!(updated.source_cleanup.completed_at.is_some());
    assert!(!source.exists(), "cleanup removes the actual source copy");
    assert!(destination.join("media.mkv").exists());
}

#[test]
fn selected_delivery_copies_only_payload_and_prunes_resolved_source_files() {
    let (tmp, db, root) = fixture();
    let source = root.join("working-selected");
    db.allocate_for_delivery(49, &root, &source, true, true)
        .unwrap();
    std::fs::create_dir(source.join("Season 1")).unwrap();
    std::fs::write(source.join("Season 1/ep01.mkv"), b"verified episode").unwrap();
    std::fs::write(source.join("readme.txt"), b"release notes").unwrap();
    std::fs::write(source.join("unresolved.rar"), b"archive bytes").unwrap();
    std::fs::write(source.join("consumed.rar"), b"transformed archive").unwrap();
    db.finish(49, &source, &root, "active").unwrap();
    let artifact = db.for_job(49).unwrap().unwrap();
    let selected = artifact
        .files
        .iter()
        .filter(|entry| entry.path == "Season 1/ep01.mkv")
        .cloned()
        .collect();
    let outcomes = vec![
        DeliveryFileOutcome {
            source_path: "Season 1/ep01.mkv".into(),
            publication_path: Some("Season 1/ep01.mkv".into()),
            classification: "video".into(),
            outcome: "published".into(),
            reason: None,
            source_cleanup_status: "pending".into(),
        },
        DeliveryFileOutcome {
            source_path: "readme.txt".into(),
            publication_path: None,
            classification: "incidental".into(),
            outcome: "excluded_source_cleanup".into(),
            reason: Some("known readme".into()),
            source_cleanup_status: "pending".into(),
        },
        DeliveryFileOutcome {
            source_path: "consumed.rar".into(),
            publication_path: None,
            classification: "archive_input".into(),
            outcome: "consumed".into(),
            reason: Some("verified extraction published".into()),
            source_cleanup_status: "pending".into(),
        },
        DeliveryFileOutcome {
            source_path: "unresolved.rar".into(),
            publication_path: None,
            classification: "archive_input".into(),
            outcome: "retained_processing".into(),
            reason: Some("no extraction record".into()),
            source_cleanup_status: "retained".into(),
        },
    ];
    let destination_root = tmp.path().join("completed");
    std::fs::create_dir(&destination_root).unwrap();
    let destination = destination_root.join("release");
    db.publish_selected_for_delivery_checked(49, &destination, selected, outcomes, &|| Ok(()))
        .unwrap();
    assert!(destination.join("Season 1/ep01.mkv").exists());
    assert!(!destination.join("readme.txt").exists());
    assert!(!destination.join("unresolved.rar").exists());
    assert!(!destination.join("consumed.rar").exists());

    db.close().unwrap();
    let db = Inventory::open(&tmp.path().join("state")).unwrap();
    let replayed = db
        .delivery_file_outcomes(
            &db.for_job(49).unwrap().unwrap().id,
            &db.for_job(49).unwrap().unwrap().generation,
        )
        .unwrap();
    assert_eq!(replayed.len(), 4);
    let delivery = db.create_delivery(49, true, Some("t-49-test")).unwrap();
    let cleanup = db.retire_delivery_source(&delivery.delivery_id).unwrap();
    assert_eq!(cleanup.source_cleanup.status, "blocked");
    assert!(!source.join("Season 1/ep01.mkv").exists());
    assert!(!source.join("readme.txt").exists());
    assert!(source.join("unresolved.rar").exists());
    let status = |path: &str| {
        cleanup
            .file_outcomes
            .iter()
            .find(|outcome| outcome.source_path == path)
            .unwrap()
            .source_cleanup_status
            .as_str()
    };
    assert_eq!(status("Season 1/ep01.mkv"), "removed");
    assert_eq!(status("readme.txt"), "removed");
    assert_eq!(status("consumed.rar"), "removed");
    assert_eq!(status("unresolved.rar"), "retained");
    assert!(!source.join("consumed.rar").exists());
}

#[test]
fn standalone_lifecycle_publication_uses_selected_tree_and_journaled_source_retirement() {
    let (tmp, db, root) = fixture();
    let source = root.join("working-standalone");
    db.allocate_for_delivery(51, &root, &source, true, false)
        .unwrap();
    std::fs::create_dir(source.join("Season 1")).unwrap();
    std::fs::write(source.join("Season 1/ep01.mkv"), b"verified episode").unwrap();
    std::fs::write(source.join("release.nzb"), b"working input").unwrap();
    std::fs::write(source.join("unknown.bin"), b"possibly useful bytes").unwrap();
    db.finish(51, &source, &root, "active").unwrap();
    let artifact = db.for_job(51).unwrap().unwrap();
    let selected = artifact
        .files
        .iter()
        .filter(|entry| entry.path == "Season 1/ep01.mkv")
        .cloned()
        .collect();
    let outcomes = vec![
        DeliveryFileOutcome {
            source_path: "Season 1/ep01.mkv".into(),
            publication_path: Some("Season 1/ep01.mkv".into()),
            classification: "video".into(),
            outcome: "published".into(),
            reason: None,
            source_cleanup_status: "pending".into(),
        },
        DeliveryFileOutcome {
            source_path: "release.nzb".into(),
            publication_path: None,
            classification: "incidental".into(),
            outcome: "excluded_source_cleanup".into(),
            reason: Some("download descriptor".into()),
            source_cleanup_status: "pending".into(),
        },
        DeliveryFileOutcome {
            source_path: "unknown.bin".into(),
            publication_path: None,
            classification: "unknown".into(),
            outcome: "retained_processing".into(),
            reason: Some("unrecognized potentially useful file".into()),
            source_cleanup_status: "retained".into(),
        },
    ];
    let target_root = tmp.path().join("completed-standalone");
    std::fs::create_dir(&target_root).unwrap();
    let target = target_root.join("release");
    db.publish_selected_checked(51, &target, selected, outcomes, &|| Ok(()))
        .unwrap();
    db.reconcile_relocations().unwrap();

    assert!(target.join("Season 1/ep01.mkv").exists());
    assert!(!target.join("release.nzb").exists());
    assert!(!target.join("unknown.bin").exists());
    assert!(!source.join("Season 1/ep01.mkv").exists());
    assert!(!source.join("release.nzb").exists());
    assert!(source.join("unknown.bin").exists());
    assert!(db
        .for_job(51)
        .unwrap()
        .is_some_and(|artifact| artifact.owned));
    assert!(db.selected_publication_for_job(51).unwrap().is_some());
    assert!(db.delivery_for_job(51).unwrap().is_none());
    let whole_delete_queued: bool = db.db.lock().unwrap().query_row(
        "SELECT EXISTS(SELECT 1 FROM operations WHERE artifact=?1 AND state IN ('queued','running') AND json_extract(data,'$.kind')='delete')",
        [&artifact.id],
        |row| row.get(0),
    ).unwrap();
    assert!(
        !whole_delete_queued,
        "selected publication must never queue whole-source deletion"
    );
}

#[test]
fn selected_publication_io_failure_keeps_source_for_same_attempt_retry() {
    let (tmp, db, root) = fixture();
    let source = root.join("working-retry");
    db.allocate_for_delivery(50, &root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("episode.mkv"), b"verified payload").unwrap();
    db.finish(50, &source, &root, "active").unwrap();
    let artifact = db.for_job(50).unwrap().unwrap();
    let selected = artifact
        .files
        .iter()
        .filter(|entry| entry.path == "episode.mkv")
        .cloned()
        .collect::<Vec<_>>();
    let outcomes = vec![DeliveryFileOutcome {
        source_path: "episode.mkv".into(),
        publication_path: Some("episode.mkv".into()),
        classification: "media".into(),
        outcome: "published".into(),
        reason: Some("verified complete media".into()),
        source_cleanup_status: "pending".into(),
    }];
    let target_parent = tmp.path().join("not-yet-mounted-completed");
    let target = target_parent.join("release-generation");
    relocation::FAULTS.with(|faults| {
        faults.borrow_mut().insert("copy_chunk", libc::EIO);
    });
    assert!(db
        .publish_selected_for_delivery_checked(
            50,
            &target,
            selected.clone(),
            outcomes.clone(),
            &|| Ok(()),
        )
        .is_err());
    let held = db.for_job(50).unwrap().unwrap();
    assert!(held.owned);
    assert_eq!(held.path, source);
    assert_eq!(
        std::fs::read(source.join("episode.mkv")).unwrap(),
        b"verified payload"
    );

    let relocation_id = db
        .pending_relocations(&artifact.id)
        .unwrap()
        .into_iter()
        .find(|operation| operation.kind == "relocate")
        .unwrap()
        .id;
    db.db
        .lock()
        .unwrap()
        .execute(
            "UPDATE operations SET data=json_set(data,'$.next_retry',0) WHERE id=?1",
            [&relocation_id],
        )
        .unwrap();
    drop(db);
    let db = Inventory::open(&tmp.path().join("state")).unwrap();
    db.reconcile_relocations().unwrap();
    let delivery = db.create_delivery(50, true, Some("retry-50")).unwrap();
    assert_eq!(delivery.files.len(), 1);
    assert!(target.join("episode.mkv").exists());
    assert!(source.join("episode.mkv").exists());
}

#[test]
fn keep_after_publication_targets_retained_source_generation() {
    let (tmp, db, root) = fixture();
    let source = root.join("keep-after-publication-source");
    db.allocate_for_delivery(56, &root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("episode.mkv"), b"verified payload").unwrap();
    db.finish(56, &source, &root, "active").unwrap();
    let original = db.for_job(56).unwrap().unwrap();
    let selected = original
        .files
        .iter()
        .filter(|entry| entry.path == "episode.mkv")
        .cloned()
        .collect::<Vec<_>>();
    let target = tmp.path().join("completed-keep").join("episode-generation");
    std::fs::create_dir(target.parent().unwrap()).unwrap();
    db.publish_selected_for_delivery_checked(
        56,
        &target,
        selected,
        vec![DeliveryFileOutcome {
            source_path: "episode.mkv".into(),
            publication_path: Some("episode.mkv".into()),
            classification: "media".into(),
            outcome: "published".into(),
            reason: Some("verified complete media".into()),
            source_cleanup_status: "pending".into(),
        }],
        &|| Ok(()),
    )
    .unwrap();
    let delivery = db
        .create_delivery(56, true, Some("keep-after-publication"))
        .unwrap();
    let source_id = delivery.source_artifact_id.clone();
    assert_ne!(source_id, delivery.artifact_id);
    assert!(!delivery.keep_source_on_failure);

    assert!(db.set_keep_on_failure(56, true).unwrap());
    assert!(db.get(&source_id).unwrap().keep_on_failure);
    // The durable admission choice is immutable historical provenance; the
    // mutable preference belongs to the retained Runner source row.
    assert!(
        !db.delivery(&delivery.delivery_id)
            .unwrap()
            .keep_source_on_failure
    );

    assert!(db.set_keep_on_failure(56, false).unwrap());
    assert!(!db.get(&source_id).unwrap().keep_on_failure);
}

#[test]
fn processing_successor_keeps_immutable_cleanup_cohort() {
    let (_tmp, db, root) = fixture();
    let source = root.join("processing");
    db.allocate_for_delivery(48, &root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("media.mkv"), b"payload").unwrap();
    db.finish(48, &source, &root, "active").unwrap();
    let successor = root.join("processing.private-next");
    let next = db
        .allocate_processing_successor(48, &root, &successor)
        .unwrap();
    assert!(next.automatic_cleanup_eligible);
    assert!(next.delivery_lifecycle);
    assert_eq!(next.origin_path, successor);
}

#[test]
fn processing_successor_preserves_live_keep_on_owned_predecessor() {
    let (_tmp, db, root) = fixture();
    let source = root.join("owned-keep-source");
    db.allocate_for_delivery(49, &root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("media.mkv"), b"payload").unwrap();
    db.finish(49, &source, &root, "active").unwrap();
    assert!(db.set_keep_on_failure(49, true).unwrap());

    // The queue snapshot is stale and says Keep=false. The current owned
    // artifact remains the authority when a worker creates its private copy.
    let successor = root.join("owned-keep-source.private-next");
    let next = db
        .allocate_processing_successor_with_policy(
            49,
            &root,
            &successor,
            Some((false, false, false)),
        )
        .unwrap();
    assert!(next.owned);
    assert!(next.automatic_cleanup_eligible);
    assert!(next.delivery_lifecycle);
    assert!(next.keep_on_failure);
}

#[test]
fn journaled_par_restore_allows_only_same_content_owned_rename_transition() {
    let (_tmp, db, root) = fixture();
    let source = root.join("restore-owned-rename");
    db.allocate_for_delivery(52, &root, &source, true, false)
        .unwrap();
    std::fs::write(
        source.join("renamed-file.mkv.obfuscated"),
        b"known complete bytes",
    )
    .unwrap();
    db.finish(52, &source, &root, "active").unwrap();
    let before =
        fs::identity_for_file(&File::open(source.join("renamed-file.mkv.obfuscated")).unwrap())
            .unwrap();

    db.restore_file(
        52,
        &source.join("renamed-file.mkv.obfuscated"),
        &source.join("renamed-file.mkv"),
    )
    .unwrap();

    let restored = File::open(source.join("renamed-file.mkv")).unwrap();
    let after = fs::identity_for_file(&restored).unwrap();
    assert!(before.same_object(&after));
    assert_eq!(
        std::fs::read(source.join("renamed-file.mkv")).unwrap(),
        b"known complete bytes"
    );
    assert!(!source.join("renamed-file.mkv.obfuscated").exists());
}

#[test]
fn legacy_four_field_restore_journal_is_fenced_without_startup_failure() {
    let (_tmp, db, root) = fixture();
    let source_path = root.join("legacy-restore");
    db.allocate(54, &root, &source_path).unwrap();
    std::fs::write(source_path.join("old.mkv"), b"content").unwrap();
    db.finish(54, &source_path, &root, "active").unwrap();
    let source = db.for_job(54).unwrap().unwrap();
    let identity = source
        .files
        .iter()
        .find(|entry| entry.path == "old.mkv")
        .unwrap()
        .identity
        .clone();
    let operation = Operation {
        id: "legacy-restore-four-tuple".into(),
        artifact: source.id.clone(),
        kind: "par_restore".into(),
        state: "running".into(),
        request: serde_json::to_string(&(
            source,
            PathBuf::from("old.mkv"),
            PathBuf::from("restored.mkv"),
            identity,
        ))
        .unwrap(),
        created_at: now(),
        not_before: 0,
        attempts: 1,
        next_retry: 0,
        error: None,
    };
    save_operation(&db.db.lock().unwrap(), &operation).unwrap();

    assert!(db.reconcile_transforms().is_ok());
    let fenced = db.operation("legacy-restore-four-tuple").unwrap();
    assert_eq!(fenced.state, "review");
    assert!(fenced
        .error
        .as_deref()
        .unwrap()
        .contains("lacks rename content proof"));
    assert!(source_path.join("old.mkv").exists());
    assert!(!source_path.join("restored.mkv").exists());
}

#[test]
fn live_failure_keep_blocks_runner_disposal_but_not_explicit_delete() {
    let (_tmp, inventory, root) = fixture();
    let db = Arc::new(inventory);
    let source = root.join("keep-at-disposal");
    db.allocate_for_delivery(53, &root, &source, true, false)
        .unwrap();
    std::fs::write(
        source.join("payload.mkv"),
        b"failed payload retained on request",
    )
    .unwrap();
    db.finish(53, &source, &root, "active").unwrap();
    let guard = db.acquire_attempt_use(53).unwrap().unwrap();
    assert!(db.set_keep_on_failure(53, true).unwrap());
    assert!(db.delete_failed_payload(&guard, 53, &|| Ok(())).is_err());
    assert!(source.join("payload.mkv").exists());
    drop(guard);

    // Explicit Delete is only available after the writer has quiesced and
    // the artifact has entered its retained terminal state.
    db.finish(53, &source, &root, "retained").unwrap();

    let artifact = db.for_job(53).unwrap().unwrap();
    let explicit = db
        .request_delete(
            &artifact.id,
            artifact.revision,
            "reviewed-explicit-delete",
            0,
        )
        .unwrap();
    assert_eq!(db.execute_delete(&explicit.id).unwrap().state, "succeeded");
    assert!(!source.exists());
}

#[test]
fn selected_publication_resumes_a_partial_private_copy_after_restart() {
    let (tmp, db, root) = fixture();
    let source = root.join("copy-resume");
    db.allocate_for_delivery(55, &root, &source, true, true)
        .unwrap();
    let payload = vec![0x5a; 256 * 1024];
    std::fs::write(source.join("episode.mkv"), &payload).unwrap();
    db.finish(55, &source, &root, "active").unwrap();
    let selected = db
        .for_job(55)
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .filter(|entry| entry.path == "episode.mkv")
        .collect::<Vec<_>>();
    let outcomes = vec![DeliveryFileOutcome {
        source_path: "episode.mkv".into(),
        publication_path: Some("episode.mkv".into()),
        classification: "video".into(),
        outcome: "published".into(),
        reason: None,
        source_cleanup_status: "pending".into(),
    }];
    let target_root = tmp.path().join("completed-resume");
    std::fs::create_dir(&target_root).unwrap();
    let target = target_root.join("private-target");
    relocation::FAULTS.with(|faults| {
        faults.borrow_mut().insert("copy_chunk", libc::EIO);
    });
    assert!(db
        .publish_selected_for_delivery_checked(55, &target, selected, outcomes, &|| Ok(()))
        .is_err());
    assert!(source.join("episode.mkv").exists());
    assert!(!target.exists());
    drop(db);

    let reopened = Inventory::open(&tmp.path().join("state")).unwrap();
    reopened.reconcile_startup(&[55]).unwrap();
    reopened
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE operations SET data=json_set(data,'$.next_retry',0) WHERE json_extract(data,'$.kind')='relocate' AND json_extract(data,'$.state')='retry'",
            [],
        )
        .unwrap();
    reopened.reconcile_relocations().unwrap();
    assert_eq!(std::fs::read(target.join("episode.mkv")).unwrap(), payload);
    assert!(source.join("episode.mkv").exists());
    assert!(reopened.selected_publication_for_job(55).unwrap().is_some());
}

#[test]
fn selected_publication_hardlink_fallback_keeps_original_source_identity() {
    let (tmp, db, root) = fixture();
    let source = root.join("hardlink-original");
    db.allocate_for_delivery(58, &root, &source, true, true)
        .unwrap();
    std::fs::create_dir(source.join("Season 1")).unwrap();
    std::fs::write(source.join("Season 1/episode.mkv"), b"verified payload").unwrap();
    db.finish(58, &source, &root, "active").unwrap();
    let artifact = db.for_job(58).unwrap().unwrap();
    let selected = artifact
        .files
        .iter()
        .filter(|entry| entry.path == "Season 1/episode.mkv")
        .cloned()
        .collect::<Vec<_>>();
    let completed = tmp.path().join("completed-hardlink");
    std::fs::create_dir(&completed).unwrap();
    let target = completed.join("release-generation");

    fs::RENAME_FAILURE.with(|fail| fail.set(Some(libc::EINVAL)));
    db.publish_selected_for_delivery_checked(
        58,
        &target,
        selected,
        vec![DeliveryFileOutcome {
            source_path: "Season 1/episode.mkv".into(),
            publication_path: Some("Season 1/episode.mkv".into()),
            classification: "video".into(),
            outcome: "published".into(),
            reason: Some("verified complete media".into()),
            source_cleanup_status: "pending".into(),
        }],
        &|| Ok(()),
    )
    .unwrap();
    let delivery = db.create_delivery(58, true, Some("hardlink-58")).unwrap();

    assert_eq!(
        std::fs::read(target.join("Season 1/episode.mkv")).unwrap(),
        b"verified payload"
    );
    assert!(source.join("Season 1/episode.mkv").exists());
    assert_eq!(delivery.source_generation, artifact.generation);
}

#[test]
fn selected_publication_retries_capacity_failure_before_scratch_exists() {
    let (tmp, db, root) = fixture();
    let source = root.join("capacity-retry");
    db.allocate_for_delivery(57, &root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("episode.mkv"), b"verified payload").unwrap();
    db.finish(57, &source, &root, "active").unwrap();
    let selected = db
        .for_job(57)
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .filter(|entry| entry.path == "episode.mkv")
        .collect::<Vec<_>>();
    let outcomes = vec![DeliveryFileOutcome {
        source_path: "episode.mkv".into(),
        publication_path: Some("episode.mkv".into()),
        classification: "video".into(),
        outcome: "published".into(),
        reason: None,
        source_cleanup_status: "pending".into(),
    }];
    let completed = tmp.path().join("capacity-completed");
    std::fs::create_dir(&completed).unwrap();
    let target = completed.join("release-generation");
    crate::capacity::FAIL_NEXT_RESERVATION.with(|fail| fail.set(true));
    assert!(db
        .publish_selected_for_delivery_checked(57, &target, selected, outcomes, &|| Ok(()))
        .is_err());
    assert!(source.join("episode.mkv").exists());
    assert!(!target.exists());

    let artifact_id = db.for_job(57).unwrap().unwrap().id;
    let pending_id: String = db
        .db
        .lock()
        .unwrap()
        .query_row(
            "SELECT id FROM operations WHERE artifact=?1 AND json_extract(data,'$.kind')='relocate'",
            [&artifact_id],
            |row| row.get(0),
        )
        .unwrap();
    db.db
        .lock()
        .unwrap()
        .execute(
            "UPDATE operations SET state='retry', data=json_set(data,'$.next_retry',0) WHERE id=?1",
            [&pending_id],
        )
        .unwrap();
    drop(db);
    let reopened = Inventory::open(&tmp.path().join("state")).unwrap();
    reopened.reconcile_relocations().unwrap();
    assert_eq!(
        std::fs::read(target.join("episode.mkv")).unwrap(),
        b"verified payload"
    );
    assert!(source.join("episode.mkv").exists());
}

#[test]
fn resumed_relocation_does_not_restore_artifact_after_concurrent_delete() {
    use std::sync::{Arc, Barrier};

    let (tmp, db, root) = fixture();
    let source = root.join("cancel-copy-resume");
    db.allocate_for_delivery(62, &root, &source, true, true)
        .unwrap();
    let payload = vec![0x41; 512 * 1024];
    std::fs::write(source.join("episode.mkv"), &payload).unwrap();
    db.finish(62, &source, &root, "active").unwrap();
    let source_artifact = db.for_job(62).unwrap().unwrap();
    let expected_source_identity = source_artifact.identity.clone().unwrap();
    let selected = source_artifact
        .files
        .into_iter()
        .filter(|entry| entry.path == "episode.mkv")
        .collect::<Vec<_>>();
    let completed = tmp.path().join("cancel-copy-completed");
    std::fs::create_dir(&completed).unwrap();
    let target = completed.join("generation");
    relocation::FAULTS.with(|faults| {
        faults.borrow_mut().insert("copy_chunk", libc::EIO);
    });
    assert!(db
        .publish_selected_for_delivery_checked(
            62,
            &target,
            selected,
            vec![DeliveryFileOutcome {
                source_path: "episode.mkv".into(),
                publication_path: Some("episode.mkv".into()),
                classification: "video".into(),
                outcome: "published".into(),
                reason: None,
                source_cleanup_status: "pending".into(),
            }],
            &|| Ok(()),
        )
        .is_err());
    let relocation_id: String = db
        .db
        .lock()
        .unwrap()
        .query_row(
            "SELECT id FROM operations WHERE artifact=?1 AND json_extract(data,'$.kind')='relocate'",
            [&source_artifact.id],
            |row| row.get(0),
        )
        .unwrap();
    db.db
        .lock()
        .unwrap()
        .execute(
            "UPDATE operations SET state='retry',data=json_set(data,'$.next_retry',0) WHERE id=?1",
            [&relocation_id],
        )
        .unwrap();
    drop(db);

    let reopened = Arc::new(Inventory::open(&tmp.path().join("state")).unwrap());
    let gate = Arc::new((Barrier::new(2), Barrier::new(2)));
    *relocation::COPY_GATE.lock().unwrap() = Some((expected_source_identity, gate.clone()));
    let worker_inventory = reopened.clone();
    let worker = std::thread::spawn(move || worker_inventory.reconcile_relocations());
    gate.0.wait();

    // Model the delete transaction winning while resumed copy work has
    // yielded the global mutation guard.
    {
        let _guard = reopened.mutation_guard().unwrap();
        let mut operation = reopened.operation(&relocation_id).unwrap();
        operation.state = "cancelled".into();
        operation.error = Some("deleted during resumed copy".into());
        let mut artifact = reopened.get(&source_artifact.id).unwrap();
        artifact.state = "deleted".into();
        artifact.hold = None;
        artifact.revision += 1;
        let mut connection = reopened.db.lock().unwrap();
        let tx = connection.transaction().unwrap();
        save_operation(&tx, &operation).unwrap();
        save_artifact(&tx, &artifact).unwrap();
        tx.commit().unwrap();
    }
    gate.1.wait();
    worker.join().unwrap().unwrap();
    *relocation::COPY_GATE.lock().unwrap() = None;

    let artifact = reopened.get(&source_artifact.id).unwrap();
    assert_eq!(artifact.state, "deleted");
    assert!(artifact.hold.is_none());
    assert_eq!(
        reopened.operation(&relocation_id).unwrap().state,
        "cancelled"
    );
    assert_eq!(std::fs::read(source.join("episode.mkv")).unwrap(), payload);
}

#[test]
fn unaccepted_older_delivery_remains_discoverable_after_newer_acceptance() {
    let (_tmp, db, root) = fixture();
    let make = |job: u32, name: &str| {
        let path = root.join(name);
        db.allocate_for_delivery(job, &root, &path, true, true)
            .unwrap();
        std::fs::write(path.join("media.mkv"), name.as_bytes()).unwrap();
        db.finish(job, &path, &root, "active").unwrap();
        db.create_delivery(job, true, Some(&format!("t-{job}-test")))
            .unwrap()
    };
    let old = make(50, "old");
    let newer = make(51, "newer");
    db.accept_delivery(
        &newer.delivery_id,
        &newer.publication_generation,
        &newer.manifest_digest,
        "receipt-newer",
    )
    .unwrap();

    let (pending, cursor) = db.deliveries_after(0, 10).unwrap();
    assert_eq!(cursor, 2);
    assert_eq!(pending, vec![old]);
}

#[test]
fn failed_inventory_lock_prevents_a_second_writer_and_offline_restore() {
    let (tmp, db, _) = fixture();
    assert!(Inventory::open(&tmp.path().join("state")).is_err());
    drop(db);
    assert!(Inventory::open(&tmp.path().join("state")).is_ok());
}
#[test]
fn history_is_not_required_for_ownership_or_removal() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let op = db
        .request_delete(&a.id, a.revision, "delete-one", 0)
        .unwrap();
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "succeeded");
    assert!(!a.path.exists());
    assert_eq!(db.get(&a.id).unwrap().state, "deleted");
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "succeeded");
}
#[test]
fn unknown_folders_cannot_be_deleted_and_adoption_starts_with_keep() {
    let (_tmp, db, root) = fixture();
    let path = root.join("old");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("media.mkv"), b"untouched").unwrap();
    let a = db.discover(&root, &path, false).unwrap();
    assert!(db.request_delete(&a.id, a.revision, "unknown", 0).is_err());
    assert!(
        db.adopt(&a.id, a.revision).is_err(),
        "inspection must precede adoption"
    );
    let a = db.inspect(&a.id).unwrap();
    let a = db.adopt(&a.id, a.revision).unwrap();
    assert!(a.keep && a.owned);
    assert!(db.request_delete(&a.id, a.revision, "kept", 0).is_err());
}
#[test]
fn keep_cancels_an_undo_window_without_accelerating_it() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let op = db.request_delete(&a.id, a.revision, "ui", 8).unwrap();
    let same = db.request_delete(&a.id, a.revision, "legacy", 0).unwrap();
    assert_eq!(same.id, op.id);
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "queued");
    db.retention(&a.id, a.revision, true, None).unwrap();
    assert_eq!(db.operation(&op.id).unwrap().state, "cancelled");
    assert!(a.path.exists());
}
#[test]
fn idempotency_keys_cannot_be_reused_with_different_intent() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    db.request_delete(&a.id, a.revision, "request", 8).unwrap();
    assert!(db.request_delete(&a.id, a.revision, "request", 0).is_err());
}
#[test]
fn new_files_and_replaced_directories_refuse_deletion() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    std::fs::write(a.path.join("unowned.mkv"), b"neighbour").unwrap();
    let op = db.request_delete(&a.id, a.revision, "new-file", 0).unwrap();
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "review");
    assert_eq!(
        std::fs::read(a.path.join("episode.mkv")).unwrap(),
        b"original media"
    );
    assert!(a.path.join("unowned.mkv").exists());
}
#[test]
fn unavailable_root_is_not_successful_deletion() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let op = db
        .request_delete(&a.id, a.revision, "lost-volume", 0)
        .unwrap();
    std::fs::rename(&root, root.with_extension("offline")).unwrap();
    let result = db.execute_delete(&op.id).unwrap();
    assert_eq!(result.state, "retry");
    assert_ne!(db.get(&a.id).unwrap().state, "deleted");
}
#[test]
fn directory_substitution_cannot_grant_ownership() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let op = db
        .request_delete(&a.id, a.revision, "replacement", 0)
        .unwrap();
    std::fs::rename(&a.path, root.join("original")).unwrap();
    std::fs::create_dir(&a.path).unwrap();
    std::fs::write(a.path.join("other.mkv"), b"other owner").unwrap();
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "review");
    assert!(a.path.join("other.mkv").exists());
}
#[test]
fn symlink_payload_entry_cannot_escape_the_owned_directory() {
    use std::os::unix::fs::symlink;
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let outside = tmp.path().join("library");
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("movie.mkv"), b"library").unwrap();
    symlink(&outside, a.path.join("escape")).unwrap();
    let op = db.request_delete(&a.id, a.revision, "link", 0).unwrap();
    assert_ne!(db.execute_delete(&op.id).unwrap().state, "succeeded");
    assert!(outside.join("movie.mkv").exists());
}
#[test]
fn wall_clock_alone_never_expires_a_payload_and_keep_resets_elapsed_time() {
    let (_tmp, db, root) = fixture();
    let mut a = parked(&db, &root);
    db.set_settings(&Settings {
        enabled: true,
        ..Settings::default()
    })
    .unwrap();
    a.deadline = Some(1);
    a.retention_seconds = 100;
    a.eligible_seconds = 0;
    save_artifact(&db.db.lock().unwrap(), &a).unwrap();
    db.tick().unwrap();
    assert!(a.path.exists());
    db.clocks.lock().unwrap().insert(
        a.id.clone(),
        (a.revision, Instant::now() - Duration::from_secs(50)),
    );
    db.tick().unwrap();
    let a = db.get(&a.id).unwrap();
    assert_eq!(a.eligible_seconds, 50);
    let kept = db.retention(&a.id, a.revision, true, None).unwrap();
    let released = db.retention(&a.id, kept.revision, false, None).unwrap();
    db.clocks.lock().unwrap().insert(
        a.id.clone(),
        (a.revision, Instant::now() - Duration::from_secs(500)),
    );
    db.tick().unwrap();
    assert_eq!(
        db.get(&a.id).unwrap().eligible_seconds,
        0,
        "old checkpoint cannot count held time"
    );
    assert!(released.deadline.unwrap() >= now() + 99);
}
#[test]
fn restore_quarantines_pending_deletion_and_retention() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    db.request_delete(&a.id, a.revision, "old-intent", 0)
        .unwrap();
    db.quarantine_restore().unwrap();
    assert_eq!(db.operation("old-intent").unwrap().state, "review");
    let restored = db.get(&a.id).unwrap();
    assert!(restored.keep);
    assert!(restored.hold.is_some());
    assert_eq!(restored.eligible_seconds, 0);
    assert!(a.path.exists());
}
#[test]
fn recovery_copy_is_independent_and_receipt_releases_only_a_complete_selection() {
    use std::os::unix::fs::MetadataExt;
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let r = db
        .stage_recovery(
            &a.id,
            a.revision,
            "copy-one",
            &["episode.mkv".into()],
            &tmp.path().join("recovery"),
        )
        .unwrap();
    assert_eq!(r.state, "published", "{:?}", r.error);
    let copy = r.published.join("payload/episode.mkv");
    assert_ne!(
        std::fs::metadata(&copy).unwrap().ino(),
        std::fs::metadata(a.path.join("episode.mkv")).unwrap().ino()
    );
    assert!(db.get(&a.id).unwrap().hold.is_some());
    db.claim_recovery(&r.id, "consumer-a", "import-a", &r.manifest_digest)
        .unwrap();
    assert!(db
        .claim_recovery(&r.id, "consumer-b", "import-b", &r.manifest_digest)
        .is_err());
    let receipt = Receipt {
        import_id: "import-a".into(),
        manifest_digest: r.manifest_digest.clone(),
        files: r
            .files
            .iter()
            .map(|f| ReceiptFile {
                id: f.id.clone(),
                bytes: f.bytes,
                sha256: f.sha256.clone(),
                result: "imported".into(),
            })
            .collect(),
    };
    assert!(db
        .recovery_receipt(&r.id, "wrong-consumer", receipt.clone())
        .is_err());
    assert_eq!(
        db.recovery_receipt(&r.id, "consumer-a", receipt.clone())
            .unwrap()
            .state,
        "imported"
    );
    assert_eq!(
        db.recovery_receipt(&r.id, "consumer-a", receipt)
            .unwrap()
            .state,
        "imported"
    );
    assert!(db.get(&a.id).unwrap().hold.is_none());
    let staged = db.get(&format!("recovery-{}", r.id)).unwrap();
    assert_eq!(staged.state, "recovery_imported");
    assert_eq!(staged.retention_seconds, 86400);
}
#[test]
fn claimed_recovery_never_releases_on_cancel_without_worker_acknowledgement() {
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let r = db
        .stage_recovery(
            &a.id,
            a.revision,
            "cancel-copy",
            &["episode.mkv".into()],
            &tmp.path().join("recovery"),
        )
        .unwrap();
    db.claim_recovery(&r.id, "curator", "job", &r.manifest_digest)
        .unwrap();
    assert_eq!(
        db.cancel_recovery(&r.id, None, false).unwrap().state,
        "cancel_pending"
    );
    assert!(db.cancel_recovery(&r.id, Some("other"), true).is_err());
    assert!(db.get(&a.id).unwrap().hold.is_some());
    assert_eq!(
        db.cancel_recovery(&r.id, Some("curator"), true)
            .unwrap()
            .state,
        "cancelled"
    );
    assert!(
        db.get(&a.id).unwrap().hold.is_some(),
        "cancelled staging requires review"
    );
}

#[test]
fn inspection_of_added_bytes_revokes_automatic_ownership() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    std::fs::write(a.path.join("another.mkv"), b"unrelated").unwrap();
    let observed = db.inspect(&a.id).unwrap();
    assert!(!observed.owned && observed.keep);
    assert!(db
        .request_delete(&a.id, observed.revision, "after-inspect", 0)
        .is_err());
}
#[test]
fn a_missing_sidecar_never_becomes_success_on_retry() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let op = db
        .request_delete(&a.id, a.revision, "marker-lost", 0)
        .unwrap();
    std::fs::remove_file(
        db.state_dir
            .join("artifact-identities")
            .join(&a.id)
            .join(format!("{}.json", a.generation)),
    )
    .unwrap();
    let mut result = db.execute_delete(&op.id).unwrap();
    assert_eq!(result.state, "retry");
    result.next_retry = 0;
    save_operation(&db.db.lock().unwrap(), &result).unwrap();
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "retry");
    assert!(a.path.exists());
}
#[test]
fn retention_preview_is_atomic_and_never_shortens_elapsed_eligibility() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let p = db.preview_retention(1).unwrap();
    assert_eq!(p.entries.len(), 1);
    let kept = db.retention(&a.id, a.revision, true, None).unwrap();
    assert!(db.apply_retention(&p.id).is_err());
    assert_eq!(db.get(&a.id).unwrap().retention_seconds, 7 * 86400);
    db.retention(&a.id, kept.revision, false, None).unwrap();
    let p = db.preview_retention(1).unwrap();
    db.apply_retention(&p.id).unwrap();
    let a = db.get(&a.id).unwrap();
    assert_eq!(a.retention_seconds, 86400);
    assert_eq!(a.eligible_seconds, 0);
    assert!(a.deadline.unwrap() >= now() + 86399);
}
#[test]
fn relocation_refuses_existing_destination_and_commits_identity_after_rename() {
    let (tmp, db, root) = fixture();
    let source = root.join("live");
    db.allocate(9, &root, &source).unwrap();
    std::fs::write(source.join("media.mkv"), b"whole media").unwrap();
    let target_root = tmp.path().join("failed");
    std::fs::create_dir(&target_root).unwrap();
    let target = target_root.join("live");
    std::fs::create_dir(&target).unwrap();
    assert!(db.relocate(9, &target).is_err());
    assert!(source.exists());
    std::fs::remove_dir(&target).unwrap();
    db.relocate(9, &target).unwrap();
    assert!(!source.exists());
    assert_eq!(
        std::fs::read(target.join("media.mkv")).unwrap(),
        b"whole media"
    );
    let a = db
        .finish(9, &target, &target_root, "parked_failed")
        .unwrap();
    assert_eq!(a.path, target);
    assert!(a.owned);
}
#[test]
fn published_recovery_reconciles_after_lost_acknowledgement() {
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let mut r = db
        .stage_recovery(
            &a.id,
            a.revision,
            "restart-stage",
            &["episode.mkv".into()],
            &tmp.path().join("recovery"),
        )
        .unwrap();
    r.state = "publishing".into();
    recovery::save(&db.db.lock().unwrap(), &r).unwrap();
    db.reconcile_recoveries().unwrap();
    assert_eq!(db.recovery(&r.id).unwrap().state, "published");
    assert!(db.get(&a.id).unwrap().hold.is_some());
}
#[test]
fn protected_role_change_refuses_an_already_queued_deletion() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let op = db
        .request_delete(&a.id, a.revision, "root-role", 0)
        .unwrap();
    db.protect_roots(std::slice::from_ref(&a.path)).unwrap();
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "review");
    assert!(a.path.exists());
}
#[test]
fn offline_backup_restores_only_with_explicit_quarantine() {
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root);
    db.request_delete(&a.id, a.revision, "pre-backup", 0)
        .unwrap();
    let backup = tmp.path().join("backup");
    db.backup(&backup).unwrap();
    assert!(backup.join("backup.json").exists());
    let restored = Inventory::open(&backup).unwrap();
    restored.quarantine_restore().unwrap();
    assert_eq!(restored.operation("pre-backup").unwrap().state, "review");
    assert!(restored.get(&a.id).unwrap().keep);
}

#[test]
fn receipt_cleanup_leaves_unselected_source_bytes_held() {
    let (tmp, db, root) = fixture();
    let path = root.join("job");
    db.allocate(1, &root, &path).unwrap();
    std::fs::write(path.join("selected.mkv"), b"selected").unwrap();
    std::fs::write(path.join("other.mkv"), b"other").unwrap();
    let a = db.finish(1, &path, &root, "parked_failed").unwrap();
    let r = db
        .stage_recovery(
            &a.id,
            a.revision,
            "selection",
            &["selected.mkv".into()],
            &tmp.path().join("recovery"),
        )
        .unwrap();
    db.claim_recovery(&r.id, "curator", "import", &r.manifest_digest)
        .unwrap();
    db.recovery_receipt(
        &r.id,
        "curator",
        Receipt {
            import_id: "import".into(),
            manifest_digest: r.manifest_digest.clone(),
            files: r
                .files
                .iter()
                .map(|f| ReceiptFile {
                    id: f.id.clone(),
                    bytes: f.bytes,
                    sha256: f.sha256.clone(),
                    result: "imported".into(),
                })
                .collect(),
        },
    )
    .unwrap();
    db.prune_receipted_source(&r.id).unwrap();
    assert!(!path.join("selected.mkv").exists());
    assert_eq!(std::fs::read(path.join("other.mkv")).unwrap(), b"other");
    assert!(db.get(&a.id).unwrap().hold.is_some());
    db.prune_receipted_source(&r.id).unwrap();
    assert!(path.join("other.mkv").exists());
}

#[test]
#[ignore = "release performance fixture; run once during final verification"]
fn lifecycle_scale_one_million_tombstones() {
    let (tmp, db, root) = fixture();
    let sample = parked(&db, &root);
    let started = Instant::now();
    {
        let mut connection = db.db.lock().unwrap();
        let tx = connection.transaction().unwrap();
        for i in 0..1_000_000 {
            let mut a = sample.clone();
            a.id = format!("benchmark-{i:08}");
            a.job = None;
            a.path = root.join(&a.id);
            a.state = "source_gone".into();
            a.files.clear();
            a.updated_at = 1;
            save_artifact(&tx, &a).unwrap();
        }
        tx.commit().unwrap();
    }
    let mut times = Vec::new();
    for _ in 0..100 {
        let start = Instant::now();
        assert_eq!(db.list(0, 100).unwrap().len(), 100);
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(f64::total_cmp);
    let bytes = std::fs::metadata(tmp.path().join("state/artifacts.sqlite"))
        .unwrap()
        .len();
    eprintln!("lifecycle rows=1000001 list_p95_ms={:.3} database_bytes={} preparation_seconds={:.1} os={} arch={}",times[94],bytes,started.elapsed().as_secs_f64(),std::env::consts::OS,std::env::consts::ARCH);
    assert!(
        times[94] < 200.0,
        "cached first-page latency exceeded design target"
    );
}

#[test]
fn queued_expiry_is_invalidated_by_a_fresh_policy_or_disabled_automation() {
    for disable in [false, true] {
        let (_tmp, db, root) = fixture();
        let path = root.join("job");
        db.allocate_for_delivery(1, &root, &path, true, false)
            .unwrap();
        std::fs::write(path.join("episode.mkv"), b"original media").unwrap();
        let mut a = db.finish(1, &path, &root, "parked_failed").unwrap();
        db.set_settings(&Settings {
            enabled: true,
            ..Default::default()
        })
        .unwrap();
        a.deadline = Some(now() - 1);
        a.eligible_seconds = a.retention_seconds;
        save_artifact(&db.db.lock().unwrap(), &a).unwrap();
        let op = db
            .request_delete_authorized(&a.id, a.revision, "expiry", 0, true)
            .unwrap();
        if disable {
            db.set_settings(&Settings::default()).unwrap();
        } else {
            db.retention(&a.id, a.revision, false, Some(86400 * 30))
                .unwrap();
        }
        assert_eq!(db.execute_delete(&op.id).unwrap().state, "cancelled");
        assert!(a.path.join("episode.mkv").exists());
    }
}
#[test]
fn startup_orphaned_active_allocation_is_reviewable_and_detached_from_job_id() {
    let (_tmp, db, root) = fixture();
    let a = db.allocate(42, &root, &root.join("orphan")).unwrap();
    std::fs::write(a.path.join("media.mkv"), b"keep").unwrap();
    db.reconcile_startup(&[]).unwrap();
    let orphan = db.get(&a.id).unwrap();
    assert_eq!(orphan.state, "retained");
    assert!(orphan.keep && orphan.hold.is_some());
    assert!(db.for_job(42).unwrap().is_none());
    assert!(db.inspect(&a.id).is_ok());
    assert!(db.allocate(42, &root, &root.join("new-job")).is_ok());
}
#[test]
fn enabled_discovery_coalesces_and_scans_explicit_nested_category_roots() {
    let (_tmp, db, root) = fixture();
    let category = root.join("tv");
    std::fs::create_dir(&category).unwrap();
    let orphan = category.join("old-media");
    std::fs::create_dir(&orphan).unwrap();
    db.configure_scan(
        serde_json::json!({"roots":[root,category],"excluded":[category],"active":[]}),
    )
    .unwrap();
    db.set_settings(&Settings {
        enabled: true,
        ..Default::default()
    })
    .unwrap();
    db.tick().unwrap();
    let scan = db.discovery_status().unwrap().unwrap();
    assert_eq!(scan.state, "succeeded");
    db.tick().unwrap();
    assert_eq!(db.discovery_status().unwrap().unwrap().id, scan.id);
    let rows = db.list(0, 100).unwrap();
    assert!(rows.iter().any(|a| a.path == orphan && a.keep));
}

#[test]
fn stopped_inventory_releases_lock_but_old_handles_cannot_mutate() {
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root);
    db.close().unwrap();
    let restarted = Inventory::open(&tmp.path().join("state")).unwrap();
    assert!(db
        .request_delete(&a.id, a.revision, "stale-handle", 0)
        .is_err());
    assert!(db.set_settings(&Settings::default()).is_err());
    assert_eq!(restarted.get(&a.id).unwrap().path, a.path);
}

// ---- Files tab regressions (field report 2026-09-28) ---------------------
// A scan used to record a directory and stop: the row said "0 entries · 0 B"
// for a folder nobody had walked, and the walk itself waited for the 30 s
// maintenance tick, five folders at a time.
#[test]
fn scan_measures_every_discovered_folder_in_one_run() {
    let (_tmp, db, root) = fixture();
    for (name, size) in [("alpha", 3usize), ("bravo", 5), ("charlie", 7)] {
        let dir = root.join(name);
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("episode.mkv"), vec![b'x'; size * 1000]).unwrap();
        std::fs::create_dir(dir.join("subs")).unwrap();
        std::fs::write(dir.join("subs/en.srt"), b"1\n").unwrap();
    }
    let op = db
        .submit_task(
            "scan",
            "installation",
            "",
            serde_json::json!({"roots":[root],"excluded":[],"active":[]}),
        )
        .unwrap();
    assert_eq!(op.state, "queued");
    db.run_tasks().unwrap();
    assert_eq!(db.operation(&op.id).unwrap().state, "succeeded");
    let page = db
        .list_page(&ListQuery {
            limit: 50,
            sort: ListSort::Size,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(page.total, 3);
    let names: Vec<_> = page
        .rows
        .iter()
        .map(|r| {
            r.artifact
                .path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(
        names,
        ["charlie", "bravo", "alpha"],
        "size sort, largest first"
    );
    for row in &page.rows {
        assert!(
            row.artifact.measured(),
            "{}: walked at discovery",
            row.artifact.path.display()
        );
        assert!(row.artifact.inspected_at.is_some());
        assert_eq!(
            row.files, 2,
            "regular files only; the subs directory is structure"
        );
        assert!(row.bytes >= 3000);
        assert_eq!(row.artifact.state, "unknown");
        assert!(
            row.artifact.files.is_empty(),
            "the list strips manifests in SQL"
        );
    }
    let full = db.get(&page.rows[0].artifact.id).unwrap();
    assert_eq!(
        full.files.len(),
        3,
        "the record itself keeps the whole manifest"
    );
}

#[cfg(unix)]
#[test]
fn scan_reports_a_folder_it_cannot_measure_without_failing_the_scan() {
    let (_tmp, db, root) = fixture();
    let good = root.join("good");
    std::fs::create_dir(&good).unwrap();
    std::fs::write(good.join("a.mkv"), b"media").unwrap();
    let odd = root.join("odd");
    std::fs::create_dir(&odd).unwrap();
    // A socket is a special file: the manifest walk refuses it.
    let _sock = std::os::unix::net::UnixListener::bind(odd.join("ctl.sock")).unwrap();
    let op = db
        .submit_task(
            "scan",
            "installation",
            "",
            serde_json::json!({"roots":[root],"excluded":[],"active":[]}),
        )
        .unwrap();
    db.run_tasks().unwrap();
    assert_eq!(db.operation(&op.id).unwrap().state, "succeeded");
    let odd_row = db.for_path(&odd).unwrap().unwrap();
    assert!(!odd_row.measured());
    // open(2) refuses a socket (ENXIO) before the walk can even classify it;
    // whichever layer objects, the reason lands on the record.
    assert!(
        !odd_row.error.as_deref().unwrap_or("").is_empty(),
        "{:?}",
        odd_row.error
    );
    let good_row = db.for_path(&good).unwrap().unwrap();
    assert!(good_row.measured() && good_row.error.is_none());
    let attention = db
        .list_page(&ListQuery {
            limit: 10,
            filter: ListFilter::Attention,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        attention.total, 2,
        "unknown folders need a person either way"
    );
}

#[test]
fn scan_skips_a_root_that_does_not_exist_yet() {
    let (_tmp, db, root) = fixture();
    std::fs::create_dir(root.join("present")).unwrap();
    let op = db
        .submit_task(
            "scan",
            "installation",
            "",
            serde_json::json!({"roots":[root.clone(), root.join("failed")],"excluded":[],"active":[]}),
        )
        .unwrap();
    db.run_tasks().unwrap();
    let op = db.operation(&op.id).unwrap();
    assert_eq!(op.state, "succeeded", "{:?}", op.error);
    assert!(db.for_path(&root.join("present")).unwrap().is_some());
}

#[test]
fn run_tasks_drains_the_whole_queue_not_five_per_tick() {
    let (_tmp, db, root) = fixture();
    let mut ops = Vec::new();
    for i in 0..12 {
        ops.push(
            db.submit_task(
                "scan",
                "installation",
                &format!("scan-{i}"),
                serde_json::json!({"roots":[root],"excluded":[],"active":[]}),
            )
            .unwrap(),
        );
    }
    db.run_tasks().unwrap();
    for op in ops {
        assert_eq!(
            db.operation(&op.id).unwrap().state,
            "succeeded",
            "{}",
            op.id
        );
    }
}

#[test]
fn list_page_filters_sorts_searches_and_counts() {
    let (_tmp, db, root) = fixture();
    let owned = parked(&db, &root); // parked_failed, owned
    for name in ["Archer.S01", "Bates.Motel.S05", "Fright.Night"] {
        let dir = root.join(name);
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("x.mkv"), vec![b'x'; name.len() * 100]).unwrap();
        let a = db.discover(&root, &dir, false).unwrap();
        db.inspect(&a.id).unwrap();
    }
    let gone = root.join("gone");
    std::fs::create_dir(&gone).unwrap();
    let gone_a = db.discover(&root, &gone, false).unwrap();
    std::fs::remove_dir(&gone).unwrap();
    db.reconcile_missing().unwrap();
    assert_eq!(db.get(&gone_a.id).unwrap().state, "source_gone");

    let live = db
        .list_page(&ListQuery {
            limit: 100,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(live.total, 4);
    assert_eq!(
        live.counts,
        ListCounts {
            all: 5,
            live: 4,
            attention: 3,
            owned: 1,
            cleared: 1,
            live_bytes: live.counts.live_bytes
        }
    );
    assert!(live.counts.live_bytes > 0);
    let by_name = db
        .list_page(&ListQuery {
            limit: 100,
            sort: ListSort::Name,
            ..Default::default()
        })
        .unwrap();
    let names: Vec<_> = by_name
        .rows
        .iter()
        .map(|r| {
            r.artifact
                .path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(
        names,
        ["Archer.S01", "Bates.Motel.S05", "Fright.Night", "job"]
    );
    let owned_only = db
        .list_page(&ListQuery {
            limit: 100,
            filter: ListFilter::Owned,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(owned_only.rows.len(), 1);
    assert_eq!(owned_only.rows[0].artifact.id, owned.id);
    let cleared = db
        .list_page(&ListQuery {
            limit: 100,
            filter: ListFilter::Cleared,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(cleared.rows[0].artifact.id, gone_a.id);
    let search = db
        .list_page(&ListQuery {
            limit: 100,
            filter: ListFilter::All,
            q: "bates".into(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(search.total, 1, "case-insensitive substring");
    let wild = db
        .list_page(&ListQuery {
            limit: 100,
            filter: ListFilter::All,
            q: "%".into(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(wild.total, 0, "LIKE metacharacters are literal");
    let paged = db
        .list_page(&ListQuery {
            limit: 2,
            offset: 2,
            sort: ListSort::Name,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(paged.total, 4);
    assert_eq!(paged.rows.len(), 2);
}

// Review finding: the list stripped the manifest in SQL and then asked the
// stripped record whether it had been measured — every folder Runner itself
// wrote (finalized, never "inspected") came back unmeasured.
#[test]
fn list_reports_runner_written_payloads_as_measured() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    assert!(
        db.get(&a.id).unwrap().inspected_at.is_some(),
        "finish measures the payload"
    );
    let page = db
        .list_page(&ListQuery {
            limit: 10,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(page.rows.len(), 1);
    assert!(page.rows[0].measured);
    assert_eq!(page.rows[0].files, 1);
    // An older record: manifest present, no inspected_at. Still measured.
    let mut old = db.get(&a.id).unwrap();
    old.inspected_at = None;
    save_artifact(&db.db.lock().unwrap(), &old).unwrap();
    let page = db
        .list_page(&ListQuery {
            limit: 10,
            ..Default::default()
        })
        .unwrap();
    assert!(
        page.rows[0].measured,
        "a manifest is measurement, whoever wrote it"
    );
}

#[test]
fn scan_does_not_rewalk_a_folder_whose_walk_already_failed() {
    let (_tmp, db, root) = fixture();
    let odd = root.join("odd");
    std::fs::create_dir(&odd).unwrap();
    let a = db.discover(&root, &odd, false).unwrap();
    db.note_error(&a.id, "special file: ctl.sock").unwrap();
    let before = db.get(&a.id).unwrap();
    db.submit_task(
        "scan",
        "installation",
        "again",
        serde_json::json!({"roots":[root],"excluded":[],"active":[]}),
    )
    .unwrap();
    db.run_tasks().unwrap();
    let after = db.get(&a.id).unwrap();
    assert_eq!(
        after.revision, before.revision,
        "the periodic scan leaves it to Inspect"
    );
    assert!(after.error.is_some());
    db.inspect(&a.id).unwrap();
    assert!(
        db.get(&a.id).unwrap().error.is_none(),
        "a manual inspect clears the error"
    );
}

#[test]
fn compact_keeps_the_size_of_a_cleared_payload() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let op = db.request_delete(&a.id, a.revision, "gone", 0).unwrap();
    db.execute_delete(&op.id).unwrap();
    let mut old = db.get(&a.id).unwrap();
    old.updated_at = now() - 100 * 86400;
    save_artifact(&db.db.lock().unwrap(), &old).unwrap();
    assert_eq!(db.compact().unwrap(), 1);
    let page = db
        .list_page(&ListQuery {
            limit: 10,
            filter: ListFilter::Cleared,
            ..Default::default()
        })
        .unwrap();
    assert!(page.rows[0].artifact.files.is_empty() || page.rows[0].files > 0);
    assert!(
        page.rows[0].bytes > 0,
        "the summary survives manifest retirement"
    );
    assert_eq!(
        db.get(&a.id).unwrap().files.len(),
        0,
        "…while the manifest is gone"
    );
}

#[test]
fn a_closed_inventory_leaves_queued_tasks_for_the_next_boot() {
    let (_tmp, db, root) = fixture();
    let op = db
        .submit_task(
            "scan",
            "installation",
            "later",
            serde_json::json!({"roots":[root],"excluded":[],"active":[]}),
        )
        .unwrap();
    db.close().unwrap();
    db.run_tasks().unwrap();
    assert_eq!(db.operation(&op.id).unwrap().state, "queued");
}

#[test]
fn summary_columns_backfill_for_an_inventory_written_before_they_existed() {
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let (files, bytes) = db.get(&a.id).unwrap().summary();
    assert!(files == 1 && bytes > 0);
    db.close().unwrap();
    drop(db);
    let path = tmp.path().join("state/artifacts.sqlite");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "DROP INDEX IF EXISTS artifacts_bytes;
             ALTER TABLE artifacts DROP COLUMN files;
             ALTER TABLE artifacts DROP COLUMN bytes;",
        )
        .unwrap();
    }
    let reopened = Inventory::open(&tmp.path().join("state")).unwrap();
    let page = reopened
        .list_page(&ListQuery {
            limit: 10,
            ..Default::default()
        })
        .unwrap();
    assert_eq!((page.rows[0].files, page.rows[0].bytes), (files, bytes));
}

// ---- recovery staging copy (field report 2026-09-28 #3) --------------------
// A 46.8 GiB staging copy across two network mounts ran 24 minutes and then
// recorded "filesystem: Invalid argument (os error 22)": std::io::copy had
// gone down copy_file_range and the error carried no operation and no path.
#[test]
fn staging_copies_hash_in_one_pass_and_publish_verifies_the_copy() {
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root);
    // Something bigger than one read buffer, so the loop actually loops.
    let big: Vec<u8> = (0..9_000_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(a.path.join("big.bin"), &big).unwrap();
    let a = db.inspect(&a.id).unwrap();
    let a = db.adopt(&a.id, a.revision).unwrap();
    let recovery_root = tmp.path().join("recovery");
    let r = db
        .stage_recovery(
            &a.id,
            a.revision,
            "stage-big",
            &["big.bin".into(), "episode.mkv".into()],
            &recovery_root,
        )
        .unwrap();
    assert_eq!(r.state, "published", "{:?}", r.error);
    let copied = std::fs::read(r.published.join("payload/big.bin")).unwrap();
    assert_eq!(copied, big, "the copy is byte-identical");
    let big_entry = r.files.iter().find(|f| f.path == "big.bin").unwrap();
    assert_eq!(big_entry.bytes, big.len() as u64);
    use sha2::Digest;
    assert_eq!(
        big_entry.sha256,
        format!("{:x}", sha2::Sha256::digest(&big)),
        "the manifest digest is the source's digest"
    );
    assert_eq!(
        db.get(&a.id).unwrap().hold.as_deref(),
        Some(format!("recovery:{}", r.id).as_str())
    );
    assert_eq!(r.source, a.path, "a handoff names the folder it came from");
    let listed = db.recoveries(0).unwrap();
    assert_eq!(listed[0].source, a.path);
}

#[test]
fn a_staging_failure_names_the_operation_and_the_path() {
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root); // owned parked_failed, stageable as is
                                // The recovery root is a FILE, so creating the staging tree under it fails.
    let recovery_root = tmp.path().join("recovery");
    std::fs::write(&recovery_root, b"not a directory").unwrap();
    let r = db
        .stage_recovery(
            &a.id,
            a.revision,
            "stage-bad",
            &["episode.mkv".into()],
            &recovery_root,
        )
        .unwrap();
    assert_eq!(r.state, "failed");
    let err = r.error.unwrap();
    assert!(
        err.contains(&recovery_root.display().to_string()),
        "the error names the path: {err}"
    );
    assert!(
        err.contains("measure free space on") && err.contains("Not a directory"),
        "…and the operation: {err}"
    );
}

#[test]
fn recovery_publication_falls_back_without_sharing_original_inodes() {
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root);
    fs::RENAME_FAILURE.with(|failure| failure.set(Some(libc::EINVAL)));
    let r = db
        .stage_recovery(
            &a.id,
            a.revision,
            "fallback-stage",
            &["episode.mkv".into()],
            &tmp.path().join("recovery"),
        )
        .unwrap();
    assert_eq!(r.state, "published", "{:?}", r.error);
    let input = fs::identity(&std::fs::metadata(a.path.join("episode.mkv")).unwrap());
    let output = fs::identity(&std::fs::metadata(r.published.join("payload/episode.mkv")).unwrap());
    assert!(!input.same_object(&output));
    assert!(!tmp.path().join("recovery/.staging").join(&r.id).exists());
    assert!(a.path.join("episode.mkv").exists());
    db.reconcile_recoveries().unwrap();
    assert_eq!(db.recovery(&r.id).unwrap().state, "published");
}

#[test]
fn recovery_cleanup_failure_keeps_publication_claimable_and_retries_after_restart() {
    for cancel in [false, true] {
        let (tmp, db, root) = fixture();
        let a = parked(&db, &root);
        fs::RENAME_FAILURE.with(|f| f.set(Some(libc::EINVAL)));
        super::publication::RETIRE_FAILURE.with(|f| f.set(true));
        let r = db
            .stage_recovery(
                &a.id,
                a.revision,
                "cleanup-fault",
                &["episode.mkv".into()],
                &tmp.path().join("recovery"),
            )
            .unwrap();
        assert_eq!(r.state, "published");
        let scratch = tmp.path().join("recovery/.staging").join(&r.id);
        assert!(scratch.exists());
        if cancel {
            db.cancel_recovery(&r.id, None, false).unwrap();
        } else {
            db.claim_recovery(&r.id, "consumer", "import", &r.manifest_digest)
                .unwrap();
        }
        drop(db);
        let db = Inventory::open(&tmp.path().join("state")).unwrap();
        db.reconcile_recoveries().unwrap();
        assert_eq!(
            db.recovery(&r.id).unwrap().state,
            if cancel { "cancelled" } else { "claimed" }
        );
        assert!(!scratch.exists());
        assert!(r.published.join("payload/episode.mkv").is_file());
    }
}
