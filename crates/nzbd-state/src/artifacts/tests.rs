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

fn expected_custody(artifact: &Artifact) -> ArtifactCustody {
    ArtifactCustody {
        generation: artifact.generation.clone(),
        state: artifact.state.clone(),
        owned: artifact.owned,
        keep: artifact.keep,
        keep_on_failure: artifact.keep_on_failure,
        gone_reason: artifact.gone_reason.clone(),
    }
}

#[test]
fn receipt_custody_matches_full_header_without_loading_large_manifest() {
    let (_tmp, db, root) = fixture();
    let mut artifact = db.allocate(1, &root, &root.join("large-manifest")).unwrap();
    artifact.state = "retained".into();
    artifact.keep = true;
    artifact.keep_on_failure = true;
    let mut identity = artifact.root_identity.clone();
    identity.directory = false;
    artifact.files = (0..5_000)
        .map(|index| FileEntry {
            path: format!("disc-{index}/episode-{index}.mkv"),
            identity: identity.clone(),
            digest: Some("ab".repeat(32)),
        })
        .collect();
    save_artifact(&db.db.lock().unwrap(), &artifact).unwrap();
    let full = db.get(&artifact.id).unwrap();
    assert_eq!(
        db.receipt_custody(&artifact.id).unwrap(),
        expected_custody(&full)
    );
    let raw: String = db
        .db
        .lock()
        .unwrap()
        .query_row(RECEIPT_CUSTODY_QUERY, [&artifact.id], |row| row.get(0))
        .unwrap();
    assert!(!serde_json::from_str::<serde_json::Value>(&raw)
        .unwrap()
        .as_object()
        .unwrap()
        .contains_key("files"));
    assert!(raw.len() * 100 < serde_json::to_string(&full).unwrap().len());
    // Report reproducible Rust decode/query costs without a timing assertion.
    let started = Instant::now();
    for _ in 0..100 {
        std::hint::black_box(db.get(&artifact.id).unwrap());
    }
    let full_elapsed = started.elapsed();
    let started = Instant::now();
    for _ in 0..100 {
        std::hint::black_box(db.receipt_custody(&artifact.id).unwrap());
    }
    eprintln!("custody large manifest: files={} full_bytes={} projected_bytes={} iterations=100 full_us={} custody_us={}",full.files.len(),serde_json::to_string(&full).unwrap().len(),raw.len(),full_elapsed.as_micros(),started.elapsed().as_micros());
}

fn assert_custody_covering_plan(inventory: &Inventory, query: &str, argument: &str) {
    let db = inventory.db.lock().unwrap();
    let plan: String = db
        .query_row(&format!("EXPLAIN QUERY PLAN {query}"), [argument], |row| {
            row.get(3)
        })
        .unwrap();
    assert!(plan.contains("artifacts_receipt_custody"), "{plan}");
    let table_root: i64 = db
        .query_row(
            "SELECT rootpage FROM sqlite_master WHERE type='table' AND name='artifacts'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let index_root: i64 = db.query_row("SELECT rootpage FROM sqlite_master WHERE type='index' AND name='artifacts_receipt_custody'", [], |row| row.get(0)).unwrap();
    let operations = db
        .prepare(&format!("EXPLAIN {query}"))
        .unwrap()
        .query_map([argument], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let table_cursors = operations
        .iter()
        .filter(|(op, _, root)| op == "OpenRead" && *root == table_root)
        .map(|(_, cursor, _)| *cursor)
        .collect::<std::collections::HashSet<_>>();
    assert!(operations
        .iter()
        .any(|(op, _, root)| op == "OpenRead" && *root == index_root));
    assert!(
        !operations.iter().any(
            |(op, cursor, _)| matches!(op.as_str(), "Column" | "RowData")
                && table_cursors.contains(cursor)
        ),
        "must not fetch the table's manifest blob: {operations:?}"
    );
}

#[test]
fn receipt_custody_reads_only_its_covering_index() {
    let (_tmp, inventory, root) = fixture();
    let artifact = inventory
        .allocate(1, &root, &root.join("index-plan"))
        .unwrap();
    assert_custody_covering_plan(&inventory, RECEIPT_CUSTODY_QUERY, &artifact.id);
}

#[test]
fn receipt_custodies_share_one_read_statement_and_covering_index() {
    let (_tmp, inventory, root) = fixture();
    let first = inventory.allocate(1, &root, &root.join("first")).unwrap();
    let second = inventory.allocate(2, &root, &root.join("second")).unwrap();
    let keys = vec![first.id, second.id, "missing".into()];
    assert_custody_covering_plan(
        &inventory,
        RECEIPT_CUSTODIES_QUERY,
        &serde_json::to_string(&keys).unwrap(),
    );
    let db = inventory.db.lock().unwrap();
    let transaction_count = db
        .prepare(&format!("EXPLAIN {RECEIPT_CUSTODIES_QUERY}"))
        .unwrap()
        .query_map([serde_json::to_string(&keys).unwrap()], |row| {
            row.get::<_, String>(1)
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
        .into_iter()
        .filter(|opcode| opcode == "Transaction")
        .count();
    assert_eq!(
        transaction_count, 1,
        "the batch must use one read transaction, not one statement per key"
    );
}

#[test]
fn receipt_custodies_isolate_bad_rows_missing_duplicates_and_fresh_mutations() {
    let (_tmp, db, root) = fixture();
    let mut first = db.allocate(1, &root, &root.join("batch-first")).unwrap();
    let mut second = db.allocate(2, &root, &root.join("batch-second")).unwrap();
    let broken_json = db.allocate(3, &root, &root.join("batch-json")).unwrap();
    let broken_header = db.allocate(4, &root, &root.join("batch-header")).unwrap();
    let broken_type = db.allocate(5, &root, &root.join("batch-blob")).unwrap();
    {
        let connection = db.db.lock().unwrap();
        connection
            .execute(
                "UPDATE artifacts SET data='{broken' WHERE id=?1",
                [&broken_json.id],
            )
            .unwrap();
        let mut malformed = serde_json::to_value(&broken_header).unwrap();
        malformed["owned"] = serde_json::json!("invalid");
        connection
            .execute(
                "UPDATE artifacts SET data=?1 WHERE id=?2",
                params![malformed.to_string(), broken_header.id],
            )
            .unwrap();
        // SQLite JSON functions accept some BLOB inputs; the projection must
        // not promote a value the original typed text read would reject.
        connection
            .execute(
                "UPDATE artifacts SET data=?1 WHERE id=?2",
                params![serde_json::to_vec(&broken_type).unwrap(), broken_type.id],
            )
            .unwrap();
    }
    let mut keys = vec![
        first.id.clone(),
        second.id.clone(),
        broken_json.id.clone(),
        broken_header.id.clone(),
        broken_type.id.clone(),
    ];
    keys.extend((0..2_000).map(|i| format!("missing-{i}")));
    keys.push(first.id.clone());
    let rows = db.receipt_custodies(&keys).unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[&first.id].as_ref().unwrap(), &expected_custody(&first));
    assert_eq!(
        rows[&second.id].as_ref().unwrap(),
        &expected_custody(&second)
    );
    assert!(matches!(rows[&broken_json.id], Err(Error::Json(_))));
    assert!(matches!(rows[&broken_header.id], Err(Error::Json(_))));
    assert!(matches!(rows[&broken_type.id], Err(Error::Sql(_))));
    assert!(matches!(db.get(&broken_type.id), Err(Error::Sql(_))));
    assert!(matches!(
        db.receipt_custody(&broken_type.id),
        Err(Error::Sql(_))
    ));
    assert!(!rows.contains_key("missing-0"));
    assert!(db.receipt_custodies(&[]).unwrap().is_empty());
    assert!(db.set_keep_on_failure(1, true).unwrap());
    first = db.get(&first.id).unwrap();
    second.generation = "new-generation".into();
    second.state = "deleted".into();
    second.owned = false;
    second.keep = true;
    save_artifact(&db.db.lock().unwrap(), &second).unwrap();
    let updated = db.receipt_custodies(&keys).unwrap();
    assert_eq!(
        updated[&first.id].as_ref().unwrap(),
        &expected_custody(&first)
    );
    assert_eq!(
        updated[&second.id].as_ref().unwrap(),
        &expected_custody(&second)
    );
}

#[test]
fn receipt_custody_is_fresh_after_keep_generation_and_state_changes() {
    let (_tmp, db, root) = fixture();
    let mut artifact = db.allocate(1, &root, &root.join("fresh-custody")).unwrap();
    assert_eq!(
        db.receipt_custody(&artifact.id).unwrap(),
        expected_custody(&artifact)
    );
    assert!(db.set_keep_on_failure(1, true).unwrap());
    artifact = db.get(&artifact.id).unwrap();
    assert!(db.receipt_custody(&artifact.id).unwrap().keep_on_failure);
    for (state, owned, keep) in [
        ("source_gone", true, true),
        ("deleted", false, false),
        ("active", true, false),
    ] {
        artifact.generation = format!("changed-{state}");
        artifact.state = state.into();
        artifact.owned = owned;
        artifact.keep = keep;
        artifact.keep_on_failure = false;
        save_artifact(&db.db.lock().unwrap(), &artifact).unwrap();
        assert_eq!(
            db.receipt_custody(&artifact.id).unwrap(),
            expected_custody(&db.get(&artifact.id).unwrap())
        );
    }
    assert!(matches!(db.receipt_custody("absent"), Err(Error::NotFound)));
}

#[test]
fn receipt_custody_preserves_header_defaults_and_malformed_field_errors() {
    let (_tmp, db, root) = fixture();
    let artifact = db.allocate(1, &root, &root.join("typed-header")).unwrap();
    let original = serde_json::to_value(&artifact).unwrap();
    let mut legacy = original.clone();
    legacy.as_object_mut().unwrap().remove("keep_on_failure");
    legacy.as_object_mut().unwrap().remove("files");
    db.db
        .lock()
        .unwrap()
        .execute(
            "UPDATE artifacts SET data=?1 WHERE id=?2",
            params![legacy.to_string(), artifact.id],
        )
        .unwrap();
    assert_eq!(
        db.receipt_custody(&artifact.id).unwrap(),
        expected_custody(&db.get(&artifact.id).unwrap())
    );
    for (key, value) in [
        ("owned", serde_json::json!("not-bool")),
        ("generation", serde_json::Value::Null),
        ("root_identity", serde_json::json!({"inode":"invalid"})),
        ("failure_assessment", serde_json::json!({"disposition":123})),
    ] {
        let mut malformed = original.clone();
        malformed[key] = value;
        db.db
            .lock()
            .unwrap()
            .execute(
                "UPDATE artifacts SET data=?1 WHERE id=?2",
                params![malformed.to_string(), artifact.id],
            )
            .unwrap();
        assert!(
            matches!(db.get(&artifact.id), Err(Error::Json(_))),
            "full {key}"
        );
        assert!(
            matches!(db.receipt_custody(&artifact.id), Err(Error::Json(_))),
            "header {key}"
        );
    }
}

#[test]
fn receipt_custody_index_migrates_on_reopen_without_promoting_invalid_json() {
    let (tmp, inventory, root) = fixture();
    let good = inventory.allocate(1, &root, &root.join("good")).unwrap();
    let bad = inventory.allocate(2, &root, &root.join("bad")).unwrap();
    {
        let db = inventory.db.lock().unwrap();
        db.execute("UPDATE artifacts SET data='{broken' WHERE id=?1", [&bad.id])
            .unwrap();
        db.execute_batch("DROP INDEX artifacts_receipt_custody")
            .unwrap();
    }
    inventory.close().unwrap();
    let reopened = Inventory::open(&tmp.path().join("state")).unwrap();
    assert_eq!(
        reopened.receipt_custody(&good.id).unwrap(),
        expected_custody(&good)
    );
    assert!(matches!(reopened.get(&bad.id), Err(Error::Json(_))));
    assert!(matches!(
        reopened.receipt_custody(&bad.id),
        Err(Error::Json(_))
    ));
    let rows = reopened
        .receipt_custodies(&[good.id.clone(), bad.id.clone(), "missing".into()])
        .unwrap();
    assert_eq!(rows[&good.id].as_ref().unwrap(), &expected_custody(&good));
    assert!(matches!(rows[&bad.id], Err(Error::Json(_))));
    assert!(!rows.contains_key("missing"));
    assert_eq!(reopened.db.lock().unwrap().query_row("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='artifacts_receipt_custody'",[],|row| row.get::<_,i64>(0)).unwrap(),1);
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
fn legacy_allocation_marker_normalizes_only_missing_identity_defaults() {
    let (_tmp, db, root) = fixture();
    let mut artifact = parked(&db, &root);
    let marker = db
        .state_dir
        .join("artifact-identities")
        .join(&artifact.id)
        .join(format!("{}.json", artifact.generation));
    artifact.identity.as_mut().unwrap().mount_id.clear();
    artifact.identity.as_mut().unwrap().changed.clear();
    db.sidecar(&artifact).unwrap();
    let mut data: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&marker).unwrap()).unwrap();
    data["identity"].as_object_mut().unwrap().remove("mount_id");
    data["identity"].as_object_mut().unwrap().remove("changed");
    std::fs::write(&marker, serde_json::to_vec(&data).unwrap()).unwrap();
    assert!(db.verify(&artifact).is_ok());

    // Normalization must not waive an explicitly different field, even one
    // ignored by same_object/PartialEq when the other side is legacy.
    for (field, value) in [
        ("mount_id", serde_json::json!("another-volume")),
        ("changed", serde_json::json!("another-change-time")),
        ("inode", serde_json::json!(0)),
        ("bytes", serde_json::json!(0)),
        ("inode", serde_json::json!("malformed")),
    ] {
        let mut tampered = data.clone();
        tampered["identity"][field] = value;
        std::fs::write(&marker, serde_json::to_vec(&tampered).unwrap()).unwrap();
        assert!(
            matches!(db.verify(&artifact), Err(Error::Conflict(_))),
            "{field}"
        );
    }
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
fn delivery_manifest_survives_history_independently() {
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
    // Contract §2.5: there is no acceptance; the row stays on the feed.
    assert_eq!(
        db.deliveries_after(0, 10).unwrap().0,
        vec![delivery.clone()]
    );
    assert_eq!(db.delivery(&delivery.delivery_id).unwrap(), delivery);
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
fn pre_upgrade_explicit_delete_resumes_under_closed_scope() {
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

    // No-wedge (closed scope): the migration still freezes it, and the
    // open's automatic exit resumes it — an owned payload is Runner's own.
    let migrated = Inventory::open(&tmp.path().join("state")).unwrap();
    assert_eq!(migrated.operation(&pending.id).unwrap().state, "retry");
    let old = migrated.execute_delete(&pending.id).unwrap();
    assert_eq!(old.state, "succeeded");
    assert!(!legacy.path.exists());
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
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        },
        DeliveryFileOutcome {
            source_path: "readme.txt".into(),
            publication_path: None,
            classification: "incidental".into(),
            outcome: "excluded_source_cleanup".into(),
            reason: Some("known readme".into()),
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        },
        DeliveryFileOutcome {
            source_path: "consumed.rar".into(),
            publication_path: None,
            classification: "archive_input".into(),
            outcome: "consumed".into(),
            reason: Some("verified extraction published".into()),
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        },
        DeliveryFileOutcome {
            source_path: "unresolved.rar".into(),
            publication_path: None,
            classification: "archive_input".into(),
            outcome: "retained_processing".into(),
            reason: Some("no extraction record".into()),
            repair_dependency: false,
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
    // Closed scope (W4): after a successful delivery nothing in the source
    // is retained for review; the tree goes whole.
    let cleanup = db.retire_delivery_source(&delivery.delivery_id).unwrap();
    assert_eq!(cleanup.source_cleanup.status, "succeeded");
    assert!(!source.exists());
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
    assert_eq!(status("unresolved.rar"), "removed");
}

#[test]
fn selected_delivery_retires_published_par_repair_dependency_after_success() {
    let (tmp, db, root) = fixture();
    let source = root.join("working-par-dependency");
    db.allocate_for_delivery(54, &root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("episode.mkv"), b"verified payload").unwrap();
    std::fs::write(source.join("set.par2"), b"validated recovery evidence").unwrap();
    db.finish(54, &source, &root, "active").unwrap();
    let artifact = db.for_job(54).unwrap().unwrap();
    let selected = artifact
        .files
        .iter()
        .filter(|entry| entry.path == "episode.mkv")
        .cloned()
        .collect();
    let outcomes = vec![
        DeliveryFileOutcome {
            source_path: "episode.mkv".into(),
            publication_path: Some("episode.mkv".into()),
            classification: "video".into(),
            outcome: "published".into(),
            reason: Some("complete CRC plus a measured repairable PAR set".into()),
            repair_dependency: true,
            source_cleanup_status: "pending".into(),
        },
        DeliveryFileOutcome {
            source_path: "set.par2".into(),
            publication_path: None,
            classification: "repair_input".into(),
            outcome: "retained_processing".into(),
            reason: Some("retry repair using this measured set".into()),
            repair_dependency: true,
            source_cleanup_status: "retained".into(),
        },
    ];
    let completed = tmp.path().join("completed-par");
    std::fs::create_dir(&completed).unwrap();
    let target = completed.join("release-generation");
    db.publish_selected_for_delivery_checked(54, &target, selected, outcomes, &|| Ok(()))
        .unwrap();
    let delivery = db
        .create_delivery(54, true, Some("repair-dependency-token"))
        .unwrap();
    // A published repair input is linked, not renamed, so the PAR set stays
    // repairable while the job can still need it. Once the delivery is a
    // success that need is over (closed scope, W4): the source goes whole
    // and the publication keeps its own name.
    let cleanup = db.retire_delivery_source(&delivery.delivery_id).unwrap();
    assert_eq!(cleanup.source_cleanup.status, "succeeded");
    assert!(!source.exists());
    assert!(target.join("episode.mkv").exists());
    let status = |path: &str| {
        cleanup
            .file_outcomes
            .iter()
            .find(|outcome| outcome.source_path == path)
            .unwrap()
            .source_cleanup_status
            .clone()
    };
    assert_eq!(status("episode.mkv"), "removed");
    assert_eq!(status("set.par2"), "removed");
    assert!(!db.get(&delivery.artifact_id).unwrap().keep_on_failure);
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
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        },
        DeliveryFileOutcome {
            source_path: "release.nzb".into(),
            publication_path: None,
            classification: "incidental".into(),
            outcome: "excluded_source_cleanup".into(),
            reason: Some("download descriptor".into()),
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        },
        DeliveryFileOutcome {
            source_path: "unknown.bin".into(),
            publication_path: None,
            classification: "unknown".into(),
            outcome: "retained_processing".into(),
            reason: Some("unrecognized potentially useful file".into()),
            repair_dependency: false,
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
        repair_dependency: false,
        source_cleanup_status: "pending".into(),
    }];
    let target_parent = tmp.path().join("not-yet-mounted-completed");
    let target = target_parent.join("release-generation");
    relocation::FAULTS.with(|faults| {
        faults.borrow_mut().insert("move_file", libc::EIO);
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
    // Contract §2.5 (M3): the selected file is moved, never in two places.
    assert!(!source.join("episode.mkv").exists());
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
            repair_dependency: false,
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
fn clearing_keep_only_schedules_delete_for_persisted_unusable_assessment() {
    let (_tmp, db, root) = fixture();
    let unusable = root.join("kept-unusable");
    db.allocate_for_delivery(58, &root, &unusable, true, false)
        .unwrap();
    std::fs::write(unusable.join("bad.bin"), b"terminally unusable").unwrap();
    let unusable_artifact = db.finish(58, &unusable, &root, "retained").unwrap();
    db.set_failure_assessment(
        &unusable_artifact.id,
        &unusable_artifact.generation,
        "unusable",
        "bounded integrity assessment found no verified payload or repair path",
    )
    .unwrap();
    assert!(db.set_keep_on_failure(58, true).unwrap());
    assert!(db.set_keep_on_failure(58, false).unwrap());
    let delete = db
        .operation(&format!("failed-delete-{}", unusable_artifact.id))
        .unwrap();
    assert_eq!(delete.state, "queued");
    assert!(
        unusable.exists(),
        "clearing Keep schedules durable deletion"
    );
    db.run_due_deletes().unwrap();
    assert!(!unusable.exists());

    let recoverable = root.join("kept-recoverable");
    db.allocate_for_delivery(59, &root, &recoverable, true, false)
        .unwrap();
    std::fs::write(recoverable.join("complete.rar"), b"recoverable input").unwrap();
    let recoverable_artifact = db.finish(59, &recoverable, &root, "retained").unwrap();
    db.set_failure_assessment(
        &recoverable_artifact.id,
        &recoverable_artifact.generation,
        "recoverable",
        "complete recognized archive; retry extraction with installed 7z",
    )
    .unwrap();
    assert!(db.set_keep_on_failure(59, true).unwrap());
    assert!(db.set_keep_on_failure(59, false).unwrap());
    assert!(db
        .operation(&format!("failed-delete-{}", recoverable_artifact.id))
        .is_err());
    assert!(recoverable.join("complete.rar").exists());

    let unknown = root.join("legacy-no-assessment");
    db.allocate_for_delivery(60, &root, &unknown, true, false)
        .unwrap();
    std::fs::write(unknown.join("old.bin"), b"legacy bytes").unwrap();
    let unknown_artifact = db.finish(60, &unknown, &root, "retained").unwrap();
    assert!(db.set_keep_on_failure(60, true).unwrap());
    assert!(db.set_keep_on_failure(60, false).unwrap());
    assert!(db
        .operation(&format!("failed-delete-{}", unknown_artifact.id))
        .is_err());
    assert!(unknown.join("old.bin").exists());
}

#[test]
fn keep_toggle_resumes_default_terminal_delete_journal_without_assessment() {
    let (tmp, db, root) = fixture();
    let db = Arc::new(db);
    let path = root.join("default-terminal-delete");
    db.allocate_for_delivery(65, &root, &path, true, false)
        .unwrap();
    std::fs::write(path.join("bad.bin"), b"terminally unusable bytes").unwrap();
    db.finish(65, &path, &root, "active").unwrap();
    let guard = db.acquire_attempt_use(65).unwrap().unwrap();
    let op_id = format!("failed-delete-{}", db.for_job(65).unwrap().unwrap().id);
    let offline = root
        .parent()
        .unwrap()
        .join("processing-default-delete-offline");
    let calls = std::cell::Cell::new(0);
    let operation = db
        .delete_failed_payload(&guard, 65, &|| {
            let next = calls.get() + 1;
            calls.set(next);
            if next == 5 {
                // The delete operation has been durably marked running by
                // this checkpoint. Hide its root so the real filesystem
                // verification returns a retryable NotFound I/O error.
                std::fs::rename(&root, &offline)?;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(operation.state, "retry");
    assert_eq!(operation.attempts, 1);
    assert!(db
        .get(&operation.artifact)
        .unwrap()
        .failure_assessment
        .is_none());

    assert!(db.set_keep_on_failure(65, true).unwrap());
    assert_eq!(db.operation(&op_id).unwrap().state, "cancelled");
    assert!(db.set_keep_on_failure(65, false).unwrap());
    let resumed = db.operation(&op_id).unwrap();
    assert_eq!(resumed.state, "queued");
    assert_eq!(resumed.attempts, 1);
    assert_eq!(resumed.request, operation.request);
    drop(guard);
    db.close().unwrap();

    std::fs::rename(&offline, &root).unwrap();
    let restarted = Inventory::open(&tmp.path().join("state")).unwrap();
    restarted.run_due_deletes().unwrap();
    let completed = restarted.operation(&op_id).unwrap();
    assert_eq!(completed.state, "succeeded");
    assert_eq!(completed.attempts, 2);
    assert!(!path.exists());
}

#[test]
fn keep_off_resumes_retryable_failed_delete_for_the_same_generation_after_restart() {
    let (tmp, db, root) = fixture();
    let path = root.join("retryable-unusable");
    db.allocate_for_delivery(62, &root, &path, true, false)
        .unwrap();
    std::fs::write(path.join("bad.bin"), b"unusable payload").unwrap();
    let artifact = db.finish(62, &path, &root, "retained").unwrap();
    db.set_failure_assessment(
        &artifact.id,
        &artifact.generation,
        "unusable",
        "bounded terminal corruption evidence",
    )
    .unwrap();
    assert!(db.set_keep_on_failure(62, true).unwrap());
    assert!(db.set_keep_on_failure(62, false).unwrap());
    let op_id = format!("failed-delete-{}", artifact.id);
    let authorized = db.operation(&op_id).unwrap();

    // A missing volume is a retryable I/O failure, not a new deletion
    // authorization or a review hold.
    let offline = root.parent().unwrap().join("processing-offline");
    std::fs::rename(&root, &offline).unwrap();
    let failed = db.execute_delete(&op_id).unwrap();
    assert_eq!(failed.state, "retry");
    assert_eq!(failed.attempts, 1);
    assert_eq!(db.get(&artifact.id).unwrap().state, "delete_failed");

    assert!(db.set_keep_on_failure(62, true).unwrap());
    assert_eq!(db.operation(&op_id).unwrap().state, "cancelled");
    assert!(db.set_keep_on_failure(62, false).unwrap());
    let resumed = db.operation(&op_id).unwrap();
    assert_eq!(resumed.state, "queued");
    assert_eq!(resumed.attempts, 1, "Keep toggles preserve delete progress");
    assert_eq!(resumed.request, authorized.request);
    db.close().unwrap();

    std::fs::rename(&offline, &root).unwrap();
    let restarted = Inventory::open(&tmp.path().join("state")).unwrap();
    restarted.run_due_deletes().unwrap();
    let succeeded = restarted.operation(&op_id).unwrap();
    assert_eq!(succeeded.state, "succeeded");
    assert_eq!(succeeded.attempts, 2);
    assert!(!path.exists());
    assert_eq!(restarted.get(&artifact.id).unwrap().state, "deleted");
}

#[test]
fn restart_resumes_running_delete_of_partially_removed_generation() {
    let (tmp, db, root) = fixture();
    let path = root.join("interrupted-unusable");
    db.allocate_for_delivery(63, &root, &path, true, false)
        .unwrap();
    std::fs::write(path.join("one.bin"), b"first").unwrap();
    std::fs::write(path.join("two.bin"), b"second").unwrap();
    let artifact = db.finish(63, &path, &root, "retained").unwrap();
    db.set_failure_assessment(
        &artifact.id,
        &artifact.generation,
        "unusable",
        "bounded terminal corruption evidence",
    )
    .unwrap();
    assert!(db.set_keep_on_failure(63, true).unwrap());
    assert!(db.set_keep_on_failure(63, false).unwrap());
    let op_id = format!("failed-delete-{}", artifact.id);
    let mut operation = db.operation(&op_id).unwrap();
    operation.state = "running".into();
    operation.attempts = 1;
    let mut deleting = db.get(&artifact.id).unwrap();
    deleting.state = "deleting".into();
    {
        let mut connection = db.db.lock().unwrap();
        let tx = connection.transaction().unwrap();
        save_operation(&tx, &operation).unwrap();
        save_artifact(&tx, &deleting).unwrap();
        tx.commit().unwrap();
    }
    // Simulate a process exiting after removing one file but before the
    // operation's completion record was committed.
    std::fs::remove_file(path.join("one.bin")).unwrap();
    db.close().unwrap();

    let restarted = Inventory::open(&tmp.path().join("state")).unwrap();
    let interrupted = restarted.operation(&op_id).unwrap();
    assert_eq!(interrupted.state, "running");
    assert_eq!(interrupted.attempts, 1);
    assert!(restarted.set_keep_on_failure(63, true).unwrap());
    assert_eq!(restarted.operation(&op_id).unwrap().state, "cancelled");
    assert!(restarted.set_keep_on_failure(63, false).unwrap());
    let resumed = restarted.operation(&op_id).unwrap();
    assert_eq!(resumed.state, "queued");
    assert_eq!(resumed.attempts, 1);
    assert_eq!(resumed.request, operation.request);
    restarted.run_due_deletes().unwrap();
    let completed = restarted.operation(&op_id).unwrap();
    assert_eq!(completed.state, "succeeded");
    assert_eq!(completed.attempts, 2);
    assert!(!path.exists());
    assert_eq!(restarted.get(&artifact.id).unwrap().state, "deleted");
}

#[test]
fn keep_off_does_not_requeue_conflicted_failed_delete() {
    let (_tmp, db, root) = fixture();
    let path = root.join("conflicted-unusable");
    db.allocate_for_delivery(64, &root, &path, true, false)
        .unwrap();
    std::fs::write(path.join("bad.bin"), b"original bytes").unwrap();
    let artifact = db.finish(64, &path, &root, "retained").unwrap();
    db.set_failure_assessment(
        &artifact.id,
        &artifact.generation,
        "unusable",
        "bounded terminal corruption evidence",
    )
    .unwrap();
    assert!(db.set_keep_on_failure(64, true).unwrap());
    assert!(db.set_keep_on_failure(64, false).unwrap());
    let op_id = format!("failed-delete-{}", artifact.id);
    std::fs::write(path.join("bad.bin"), b"external replacement").unwrap();
    assert_eq!(db.execute_delete(&op_id).unwrap().state, "review");
    assert_eq!(
        db.get(&artifact.id).unwrap().hold.as_deref(),
        Some("review")
    );

    assert!(db.set_keep_on_failure(64, true).unwrap());
    assert!(db.set_keep_on_failure(64, false).unwrap());
    assert_eq!(db.operation(&op_id).unwrap().state, "review");
    assert_eq!(
        db.get(&artifact.id).unwrap().hold.as_deref(),
        Some("review")
    );
    assert!(path.join("bad.bin").exists());
}

#[test]
fn clearing_keep_retries_partial_delivery_cleanup_as_a_closed_scope() {
    let (tmp, db, root) = fixture();
    let source = root.join("keep-partial-source");
    db.allocate_for_delivery(61, &root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("episode.mkv"), b"verified payload").unwrap();
    std::fs::write(source.join("sample.txt"), b"disposable sample").unwrap();
    std::fs::write(source.join("complete.rar"), b"recoverable archive").unwrap();
    db.finish(61, &source, &root, "active").unwrap();
    let artifact = db.for_job(61).unwrap().unwrap();
    let selected = artifact
        .files
        .iter()
        .filter(|entry| entry.path == "episode.mkv")
        .cloned()
        .collect();
    let outcomes = vec![
        DeliveryFileOutcome {
            source_path: "episode.mkv".into(),
            publication_path: Some("episode.mkv".into()),
            classification: "video".into(),
            outcome: "published".into(),
            reason: None,
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        },
        DeliveryFileOutcome {
            source_path: "sample.txt".into(),
            publication_path: None,
            classification: "incidental".into(),
            outcome: "excluded_source_cleanup".into(),
            reason: Some("known disposable sample".into()),
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        },
        DeliveryFileOutcome {
            source_path: "complete.rar".into(),
            publication_path: None,
            classification: "archive_input".into(),
            outcome: "archive_review".into(),
            reason: Some("complete recognized archive; retry extraction with 7z".into()),
            repair_dependency: false,
            source_cleanup_status: "review_required".into(),
        },
    ];
    let completed = tmp.path().join("completed-keep-partial");
    std::fs::create_dir(&completed).unwrap();
    let target = completed.join("release-generation");
    db.publish_selected_for_delivery_checked(61, &target, selected, outcomes, &|| Ok(()))
        .unwrap();
    let delivery = db
        .create_delivery_with_outcome(
            61,
            true,
            Some("keep-partial-token"),
            "PARTIAL",
            Some("assessment"),
            Some("archive_review"),
        )
        .unwrap();
    assert!(db.set_keep_on_failure(61, true).unwrap());
    let kept = db.retire_delivery_source(&delivery.delivery_id).unwrap();
    assert_eq!(kept.source_cleanup.status, "blocked");
    assert!(source.join("sample.txt").exists());
    assert!(source.join("complete.rar").exists());

    // The immutable serialized receipt is historical evidence. Make its
    // cleanup field stale to ensure Keep-off consults the mutable SQL column,
    // not that old JSON snapshot.
    {
        let db = db.db.lock().unwrap();
        db.execute(
            "UPDATE deliveries SET data=json_set(data,'$.sourceCleanup.status','succeeded') WHERE id=?1",
            [&delivery.delivery_id],
        )
        .unwrap();
    }

    assert!(db.set_keep_on_failure(61, false).unwrap());
    let scheduled = db.delivery(&delivery.delivery_id).unwrap();
    assert_eq!(scheduled.source_cleanup.status, "retry");
    // Without Keep the failure follows the closed-scope rule: what the
    // publication did not take is removed with the source.
    let refreshed = db.retire_delivery_source(&delivery.delivery_id).unwrap();
    assert_eq!(refreshed.source_cleanup.status, "succeeded");
    assert!(!source.exists());
    assert!(target.join("episode.mkv").exists());
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

    for root in db.startup_reconciliation_roots().unwrap() {
        assert!(db.reconcile_transforms_root_snapshot(&root, None).is_ok());
    }
    let fenced = db.operation("legacy-restore-four-tuple").unwrap();
    // W9: abandoned without replay; post-processing re-runs the transform.
    assert_eq!(fenced.state, "cancelled");
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
fn legacy_whole_source_relocation_replays_only_owned_private_staging() {
    for phase in ["before-staging", "owned-partial", "unknown-nonempty"] {
        let (tmp, db, root) = fixture();
        let source = root.join("legacy-source");
        db.allocate_for_delivery(95, &root, &source, true, true)
            .unwrap();
        let bytes = vec![0x35; 256 * 1024];
        std::fs::write(source.join("episode.mkv"), &bytes).unwrap();
        db.finish(95, &source, &root, "active").unwrap();
        let target_root = tmp.path().join("completed-legacy");
        std::fs::create_dir(&target_root).unwrap();
        let target = target_root.join("release");
        relocation::FAULTS.with(|faults| {
            faults.borrow_mut().insert(
                if phase == "owned-partial" {
                    "copy_chunk"
                } else {
                    "before_move"
                },
                libc::EIO,
            );
        });
        assert!(db
            .relocate_for_delivery_checked(95, &target, &|| Ok(()))
            .is_err());
        let artifact = db.for_job(95).unwrap().unwrap();
        let op = db.pending_relocations(&artifact.id).unwrap().remove(0);
        let request: serde_json::Value = serde_json::from_str(&op.request).unwrap();
        assert!(request["publication_files"].is_null());
        assert_eq!(request["scratch"].is_null(), phase != "owned-partial");
        let scratch = target_root.join(format!(".runner-{}", op.id));
        if phase == "unknown-nonempty" {
            std::fs::create_dir(&scratch).unwrap();
            std::fs::write(scratch.join("unknown.bin"), b"unrelated").unwrap();
        }
        drop(db);
        let reopened = Inventory::open(&tmp.path().join("state")).unwrap();
        reopened.reconcile_startup(&[95]).unwrap();
        reopened
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE operations SET data=json_set(data,'$.next_retry',0) WHERE id=?1",
                [&op.id],
            )
            .unwrap();
        reopened.reconcile_relocations().unwrap();
        if phase == "unknown-nonempty" {
            assert_eq!(reopened.operation(&op.id).unwrap().state, "review");
            assert_eq!(
                std::fs::read(scratch.join("unknown.bin")).unwrap(),
                b"unrelated"
            );
            assert!(!target.exists());
        } else {
            assert_eq!(reopened.operation(&op.id).unwrap().state, "succeeded");
            assert_eq!(std::fs::read(target.join("episode.mkv")).unwrap(), bytes);
            let delivery = reopened
                .create_delivery(95, true, Some("legacy-95"))
                .unwrap();
            assert_eq!(delivery.files.len(), 1);
        }
        assert_eq!(std::fs::read(source.join("episode.mkv")).unwrap(), bytes);
    }
}

#[test]
fn selected_publication_resumes_an_interrupted_move_after_restart() {
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
        repair_dependency: false,
        source_cleanup_status: "pending".into(),
    }];
    let target_root = tmp.path().join("completed-resume");
    std::fs::create_dir(&target_root).unwrap();
    let target = target_root.join("private-target");
    relocation::FAULTS.with(|faults| {
        faults.borrow_mut().insert("move_file", libc::EIO);
    });
    assert!(db
        .publish_selected_for_delivery_checked(55, &target, selected, outcomes, &|| Ok(()))
        .is_err());
    assert!(source.join("episode.mkv").exists());
    assert!(!target.join("episode.mkv").exists());
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
    assert!(!source.join("episode.mkv").exists(), "moved, not copied");
    assert!(reopened.selected_publication_for_job(55).unwrap().is_some());
}

#[test]
fn selected_publication_link_fallback_moves_and_keeps_source_generation() {
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
            repair_dependency: false,
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
    // The EINVAL rename took the link + unlink fallback: one name, in the
    // publication (contract §2.5, PR #247 chain).
    assert!(!source.join("Season 1/episode.mkv").exists());
    assert_eq!(delivery.source_generation, artifact.generation);
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
        faults.borrow_mut().insert("move_file", libc::EIO);
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
                repair_dependency: false,
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
    relocation::COPY_GATE
        .lock()
        .unwrap()
        .push((expected_source_identity.clone(), gate.clone()));
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
    relocation::COPY_GATE
        .lock()
        .unwrap()
        .retain(|(identity, _)| !identity.same_object(&expected_source_identity));

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
fn every_delivery_stays_on_the_reconciliation_feed() {
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
    let (feed, cursor) = db.deliveries_after(0, 10).unwrap();
    assert_eq!(cursor, 2);
    assert_eq!(feed, vec![old, newer]);
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
fn parked_failure_expires_on_its_wall_clock_deadline_across_restarts_without_opt_in() {
    // R4 / W3: expiry is the deadline alone — no uptime clock, no
    // automation toggle — and it holds across a restart.
    let (tmp, db, root) = fixture();
    let path = root.join("job");
    db.allocate_for_delivery(1, &root, &path, true, false)
        .unwrap();
    std::fs::write(path.join("episode.mkv"), b"original media").unwrap();
    let mut a = db.finish(1, &path, &root, "parked_failed").unwrap();
    assert!(!db.settings().unwrap().enabled);
    a.retention_seconds = 100;
    a.deadline = Some(now() + 3600);
    save_artifact(&db.db.lock().unwrap(), &a).unwrap();
    db.tick().unwrap();
    db.run_due_deletes().unwrap();
    assert!(a.path.exists(), "not yet due");
    a.deadline = Some(now() - 1);
    save_artifact(&db.db.lock().unwrap(), &a).unwrap();
    db.close().unwrap();
    drop(db);
    let db = Inventory::open(&tmp.path().join("state")).unwrap();
    let kept = db.retention(&a.id, a.revision, true, None).unwrap();
    db.tick().unwrap();
    assert!(a.path.exists(), "Keep holds an expired payload");
    let release_started = now();
    let released = db.retention(&a.id, kept.revision, false, None).unwrap();
    let release_finished = now();
    assert!(
        (release_started + 100..=release_finished + 100).contains(&released.deadline.unwrap()),
        "releasing Keep starts a fresh period"
    );
    let mut due = db.get(&a.id).unwrap();
    due.deadline = Some(now() - 1);
    save_artifact(&db.db.lock().unwrap(), &due).unwrap();
    db.tick().unwrap();
    db.run_due_deletes().unwrap();
    assert!(!a.path.exists(), "expired at its wall-clock deadline");
}
/// W3 upgrade: a curator-admitted failure an older build parked under
/// park-always (2713, 2770, …) follows `failure_action = delete` once its
/// salvage has settled — due now, deleted by the ordinary retention path.
/// A kept failure and a job curator did not admit stay parked.
#[test]
fn parked_curator_failures_expire_under_delete_policy() {
    use super::terminals::tests::failed;
    let (_tmp, db, root) = fixture();
    let mut paths = Vec::new();
    for (job, transfer, keep) in [(1, true, false), (2, true, true), (3, false, false)] {
        let path = root.join(format!("job-{job}"));
        db.allocate_for_delivery(job, &root, &path, true, false)
            .unwrap();
        std::fs::write(path.join("episode.mkv"), b"original media").unwrap();
        db.finish(job, &path, &root, "parked_failed").unwrap();
        let mut terminal = failed(job, SalvageState::None);
        terminal.keep_on_failure = keep;
        if !transfer {
            terminal.transfer = None;
        }
        db.record_terminal(&terminal).unwrap();
        paths.push(path);
    }
    assert_eq!(db.expire_parked_curator_failures().unwrap(), 1);
    assert_eq!(
        db.expire_parked_curator_failures().unwrap(),
        0,
        "idempotent"
    );
    db.tick().unwrap();
    db.run_due_deletes().unwrap();
    assert!(!paths[0].exists(), "curator-admitted failure deleted");
    assert!(paths[1].exists(), "keep_on_failure stays");
    assert!(
        paths[2].exists(),
        "a job curator did not admit keeps its retention"
    );
}
/// W9 upgrade: a transform an older build parked in `review`, with the
/// "review: interrupted transform" Keep hold on its source, is abandoned
/// at open so post-processing re-runs it; idempotent.
#[test]
fn reviewed_transforms_are_abandoned_on_open() {
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let mut held = db.get(&a.id).unwrap();
    held.hold = Some("review: interrupted transform".into());
    held.keep = true;
    save_artifact(&db.db.lock().unwrap(), &held).unwrap();
    let op = Operation {
        id: "par-restore-reviewed".into(),
        artifact: a.id.clone(),
        kind: "par_restore".into(),
        state: "review".into(),
        request: "[]".into(),
        created_at: now(),
        not_before: 0,
        attempts: 1,
        next_retry: 0,
        error: Some("interrupted restore has ambiguous identities".into()),
    };
    save_operation(&db.db.lock().unwrap(), &op).unwrap();
    db.close().unwrap();
    drop(db);
    for _ in 0..2 {
        let db = Inventory::open(&tmp.path().join("state")).unwrap();
        let after = db.operation(&op.id).unwrap();
        assert_eq!(after.state, "cancelled");
        assert!(after.error.unwrap().contains("ambiguous identities"));
        let source = db.get(&a.id).unwrap();
        assert!(source.hold.is_none() && !source.keep);
        db.close().unwrap();
    }
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
    let apply_started = now();
    db.apply_retention(&p.id).unwrap();
    let apply_finished = now();
    let a = db.get(&a.id).unwrap();
    assert_eq!(a.retention_seconds, 86400);
    assert!((apply_started + 86400..=apply_finished + 86400).contains(&a.deadline.unwrap()));
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
fn succeeded_relocation_proof_requires_exact_owned_current_generation_and_paths() {
    let (tmp, db, root) = fixture();
    let source = root.join("proof-source");
    let artifact = db.allocate(91, &root, &source).unwrap();
    std::fs::write(source.join("media.mkv"), b"whole media").unwrap();
    let target_root = tmp.path().join("proof-target");
    std::fs::create_dir(&target_root).unwrap();
    let target = target_root.join("published");
    let generation = SourceGeneration {
        artifact: artifact.id.clone(),
        generation: artifact.generation.clone(),
    };
    assert!(!db
        .has_succeeded_relocation(&generation, &source, &target)
        .unwrap());
    db.relocate(91, &target).unwrap();
    assert!(db
        .has_succeeded_relocation(&generation, &source, &target)
        .unwrap());
    assert!(!db
        .has_succeeded_relocation(&generation, &target, &source)
        .unwrap());
    assert!(!db
        .has_succeeded_relocation(&generation, &root.join("other"), &target)
        .unwrap());
    let mut stale = generation.clone();
    stale.generation.push_str("-stale");
    assert!(!db
        .has_succeeded_relocation(&stale, &source, &target)
        .unwrap());
    let mut current = db.get(&artifact.id).unwrap();
    current.owned = false;
    save_artifact(&db.db.lock().unwrap(), &current).unwrap();
    assert!(!db
        .has_succeeded_relocation(&generation, &source, &target)
        .unwrap());
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
    eprintln!(
        "lifecycle rows=1000001 list_p95_ms={:.3} database_bytes={} preparation_seconds={:.1} os={} arch={}",
        times[94],
        bytes,
        started.elapsed().as_secs_f64(),
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    assert!(
        times[94] < 200.0,
        "cached first-page latency exceeded design target"
    );
}

#[test]
fn queued_expiry_is_invalidated_by_a_fresh_policy_not_by_the_automation_toggle() {
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
        save_artifact(&db.db.lock().unwrap(), &a).unwrap();
        let op = db
            .request_delete_authorized(&a.id, a.revision, "expiry", 0, true)
            .unwrap();
        if disable {
            // W3: the automation toggle is not expiry authority; a due
            // expiry proceeds with it off.
            db.set_settings(&Settings::default()).unwrap();
            assert_eq!(db.execute_delete(&op.id).unwrap().state, "succeeded");
            assert!(!a.path.join("episode.mkv").exists());
            continue;
        }
        db.retention(&a.id, a.revision, false, Some(86400 * 30))
            .unwrap();
        assert_eq!(db.execute_delete(&op.id).unwrap().state, "cancelled");
        assert!(a.path.join("episode.mkv").exists());
    }
}
#[test]
fn startup_orphaned_active_allocation_preserves_binding_until_explicit_successor() {
    let (_tmp, db, root) = fixture();
    let a = db.allocate(42, &root, &root.join("orphan")).unwrap();
    std::fs::write(a.path.join("media.mkv"), b"keep").unwrap();
    db.reconcile_startup(&[]).unwrap();
    // No-wedge (closed scope): an owned orphan parks with the failed TTL
    // whenever it was allocated; the binding survives for a successor.
    let orphan = db.get(&a.id).unwrap();
    assert_eq!(orphan.state, "parked_failed");
    assert!(!orphan.keep && orphan.hold.is_none());
    assert!(orphan.earliest_expiry().is_some());
    assert_eq!(db.for_job(42).unwrap().unwrap().id, a.id);
    assert!(db.inspect(&a.id).is_ok());
    assert!(db
        .allocate_processing_successor(42, &root, &root.join("new-job"))
        .is_ok());
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
    // A FIFO is a special file: the manifest walk refuses it. Unlike a Unix
    // socket, its pathname also works beneath long managed workspace roots.
    use std::os::unix::ffi::OsStrExt;
    let fifo_path = odd.join("ctl.fifo");
    let fifo_name = std::ffi::CString::new(fifo_path.as_os_str().as_bytes()).unwrap();
    // SAFETY: fifo_name is a live NUL-terminated pathname; mkfifo retains no
    // pointer after returning. The fixture owns its containing directory.
    let created = unsafe { libc::mkfifo(fifo_name.as_ptr(), 0o600) };
    assert_eq!(
        created,
        0,
        "{}: {}",
        fifo_path.display(),
        std::io::Error::last_os_error()
    );
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
    // The descriptor opens without blocking, then the manifest rejects its
    // nonregular file type. The actionable reason lands on the record.
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
            filter: ListFilter::Foreign,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        attention.total, 2,
        "folders Runner did not create are listed apart, as not Runner's"
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
            attention: 0,
            foreign: 3,
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
    let (tmp, db, root) = fixture();
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
    drop(db);
    let next = Inventory::open(&tmp.path().join("state")).unwrap();
    assert_eq!(next.operation(&op.id).unwrap().state, "queued");
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

#[test]
fn selected_publication_io_failure_resumes_in_process_on_tick() {
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
        repair_dependency: false,
        source_cleanup_status: "pending".into(),
    }];
    let target_parent = tmp.path().join("not-yet-mounted-completed");
    let target = target_parent.join("release-generation");
    relocation::FAULTS.with(|faults| {
        faults.borrow_mut().insert("move_file", libc::EIO);
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
    relocation::FAULTS.with(|faults| {
        faults.borrow_mut().insert("move_file", libc::ENOSPC);
    });
    db.tick().unwrap();
    let retry = db.operation(&relocation_id).unwrap();
    assert_eq!(retry.state, "retry");
    assert!(
        db.for_job(50).unwrap().unwrap().hold.is_none(),
        "repeat I/O must not create a permanent review hold"
    );
    let mut previously_stuck = db.for_job(50).unwrap().unwrap();
    previously_stuck.hold = Some("review: interrupted move".into());
    save_artifact(&db.db.lock().unwrap(), &previously_stuck).unwrap();
    db.db
        .lock()
        .unwrap()
        .execute(
            "UPDATE operations SET data=json_set(data,'$.next_retry',0) WHERE id=?1",
            [&relocation_id],
        )
        .unwrap();
    db.tick().unwrap();
    db.publish_selected_for_delivery_checked(50, &target, selected, outcomes, &|| Ok(()))
        .unwrap();
    let attempts: i64 = db.db.lock().unwrap().query_row(
        "SELECT count(*) FROM operations WHERE artifact=?1 AND json_extract(data,'$.kind')='relocate'",
        [&artifact.id], |row| row.get(0)).unwrap();
    assert_eq!(attempts, 1, "live retries reuse the original attempt");
    let delivery = db.create_delivery(50, true, Some("retry-50")).unwrap();
    assert_eq!(delivery.files.len(), 1);
    assert!(target.join("episode.mkv").exists());
    assert!(!source.join("episode.mkv").exists(), "moved, not copied");
}

#[test]
fn successful_publication_ignores_failure_keep_for_incidental_cleanup() {
    let (tmp, db, root) = fixture();
    let source = root.join("keep-success");
    db.allocate_for_delivery(72, &root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("episode.mkv"), b"verified payload").unwrap();
    std::fs::write(source.join("release.nzb"), b"descriptor").unwrap();
    db.finish(72, &source, &root, "active").unwrap();
    let selected = db
        .for_job(72)
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .filter(|entry| entry.path == "episode.mkv")
        .collect();
    let outcomes = vec![
        DeliveryFileOutcome {
            source_path: "episode.mkv".into(),
            publication_path: Some("episode.mkv".into()),
            classification: "video".into(),
            outcome: "published".into(),
            reason: None,
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        },
        DeliveryFileOutcome {
            source_path: "release.nzb".into(),
            publication_path: None,
            classification: "incidental".into(),
            outcome: "excluded_source_cleanup".into(),
            reason: None,
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        },
    ];
    let target = tmp.path().join("completed/release");
    db.publish_selected_for_delivery_checked(72, &target, selected, outcomes, &|| Ok(()))
        .unwrap();
    let delivery = db
        .create_delivery(72, true, Some("keep-success-72"))
        .unwrap();
    assert!(db.set_keep_on_failure(72, true).unwrap());
    let cleanup = db.retire_delivery_source(&delivery.delivery_id).unwrap();
    assert_eq!(
        cleanup.source_cleanup.status, "succeeded",
        "{:?}",
        cleanup.source_cleanup.error
    );
    assert!(!source.exists());
    assert!(target.join("episode.mkv").exists());
}

#[test]
fn keep_enabled_between_source_lookup_and_unlink_retains_failed_incidental_bytes() {
    let (tmp, db, root) = fixture();
    let db = Arc::new(db);
    let source = root.join("keep-success");
    db.allocate_for_delivery(72, &root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("episode.mkv"), b"verified payload").unwrap();
    std::fs::write(source.join("release.nzb"), b"descriptor").unwrap();
    db.finish(72, &source, &root, "active").unwrap();
    let selected = db
        .for_job(72)
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .filter(|entry| entry.path == "episode.mkv")
        .collect();
    let outcomes = vec![
        DeliveryFileOutcome {
            source_path: "episode.mkv".into(),
            publication_path: Some("episode.mkv".into()),
            classification: "video".into(),
            outcome: "published".into(),
            reason: None,
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        },
        DeliveryFileOutcome {
            source_path: "release.nzb".into(),
            publication_path: None,
            classification: "incidental".into(),
            outcome: "excluded_source_cleanup".into(),
            reason: None,
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        },
    ];
    let target = tmp.path().join("completed/release");
    db.publish_selected_for_delivery_checked(72, &target, selected, outcomes, &|| Ok(()))
        .unwrap();
    let delivery = db
        .create_delivery_with_outcome(
            72,
            true,
            Some("keep-race-72"),
            "PARTIAL",
            Some("unpack"),
            Some("io"),
        )
        .unwrap();
    let identity = db
        .get(&delivery.source_artifact_id)
        .unwrap()
        .identity
        .unwrap();
    let gate = Arc::new((std::sync::Barrier::new(2), std::sync::Barrier::new(2)));
    relocation::COPY_GATE
        .lock()
        .unwrap()
        .push((identity, gate.clone()));
    let worker_db = db.clone();
    let worker =
        std::thread::spawn(move || worker_db.retire_delivery_source(&delivery.delivery_id));
    gate.0.wait();
    assert!(db.set_keep_on_failure(72, true).unwrap());
    gate.1.wait();
    let cleanup = worker.join().unwrap().unwrap();
    assert_eq!(cleanup.source_cleanup.status, "blocked");
    assert!(source.join("release.nzb").exists());
    assert!(target.join("episode.mkv").exists());
}

#[test]
fn selected_publication_reserves_only_selected_bytes() {
    let (tmp, db, root) = fixture();
    let source = root.join("capacity-selection");
    db.allocate_for_delivery(73, &root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("episode.mkv"), b"verified payload").unwrap();
    let completed = tmp.path().join("selected-completed");
    std::fs::create_dir(&completed).unwrap();
    // A sparse unresolved sibling exceeds all available destination capacity
    // without consuming that capacity. Only the small selected file is copied.
    let sibling = std::fs::File::create(source.join("unresolved.bin")).unwrap();
    sibling
        .set_len(
            crate::capacity::available(&completed)
                .unwrap()
                .saturating_add(1),
        )
        .unwrap();
    db.finish(73, &source, &root, "active").unwrap();
    let selected = db
        .for_job(73)
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .filter(|entry| entry.path == "episode.mkv")
        .collect();
    let outcomes = vec![DeliveryFileOutcome {
        source_path: "episode.mkv".into(),
        publication_path: Some("episode.mkv".into()),
        classification: "video".into(),
        outcome: "published".into(),
        reason: None,
        repair_dependency: false,
        source_cleanup_status: "pending".into(),
    }];
    let target = completed.join("release");
    db.publish_selected_for_delivery_checked(73, &target, selected, outcomes, &|| Ok(()))
        .unwrap();
    assert_eq!(
        std::fs::read(target.join("episode.mkv")).unwrap(),
        b"verified payload"
    );
    assert!(source.join("unresolved.bin").exists());
    assert!(!target.join("unresolved.bin").exists());
}

#[test]
fn delivery_stage_snapshot_is_bounded_optional_and_immutable_on_replay() {
    let (tmp, db, root) = fixture();
    let source = root.join("stage-snapshot");
    db.allocate_for_delivery(74, &root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("episode.mkv"), b"verified payload").unwrap();
    db.finish(74, &source, &root, "active").unwrap();
    let snapshot = DeliveryRunnerStages {
        node_id: "worker-one".into(),
        clock_domain: "worker-one:boot-one".into(),
        spans: vec![nzbd_types::StageSpan {
            stage: nzbd_types::PostStage::Move,
            started_at_unix: 123,
            ms: None,
        }],
    };
    let timing = nzbd_types::LifecycleTiming {
        version: 1,
        intervals: vec![nzbd_types::LifecycleInterval {
            stage: nzbd_types::LifecycleStage::Download,
            node_id: "worker-one".into(),
            clock_domain: "worker-one:boot-one".into(),
            start_unix_ms: 1000,
            end_unix_ms: Some(900),
            start_monotonic_ms: Some(0),
            end_monotonic_ms: Some(10),
            duration_ms: Some(10),
            state: nzbd_types::LifecycleIntervalState::Completed,
            observations: 1,
        }],
        omitted_intervals: std::collections::BTreeMap::from([(
            nzbd_types::LifecycleStage::Queue,
            2,
        )]),
    };
    let delivery = db
        .create_delivery_with_lifecycle_snapshot(
            74,
            true,
            Some("stage-74"),
            "SUCCESS",
            None,
            None,
            false,
            Some(snapshot.clone()),
            Some(timing.clone()),
        )
        .unwrap();
    assert_eq!(delivery.runner_stages, Some(snapshot.clone()));
    assert_eq!(delivery.lifecycle_timing, Some(timing.clone()));
    drop(db);
    let db = Inventory::open(&tmp.path().join("state")).unwrap();
    assert_eq!(
        db.delivery(&delivery.delivery_id).unwrap().lifecycle_timing,
        Some(timing.clone())
    );
    let mut later = snapshot.clone();
    later.spans[0].ms = Some(99);
    let replay = db
        .create_delivery_with_outcome_keep_and_stages(
            74,
            true,
            Some("stage-74"),
            "SUCCESS",
            None,
            None,
            false,
            Some(later),
        )
        .unwrap();
    assert_eq!(replay.lifecycle_timing, Some(timing));
    assert_eq!(
        replay.runner_stages,
        Some(snapshot.clone()),
        "READY freezes the original clock evidence, including unknown duration"
    );
    let mut excessive = snapshot;
    excessive.spans = vec![excessive.spans[0]; 129];
    assert!(db
        .create_delivery_with_outcome_keep_and_stages(
            74,
            true,
            Some("stage-74"),
            "SUCCESS",
            None,
            None,
            false,
            Some(excessive)
        )
        .is_err());
}

#[test]
fn delivery_discovery_skips_malformed_row_with_trusted_cursor() {
    let (_tmp, db, root) = fixture();
    let mut deliveries = Vec::new();
    for job in [75, 76] {
        let source = root.join(format!("discovery-{job}"));
        db.allocate_for_delivery(job, &root, &source, true, true)
            .unwrap();
        std::fs::write(source.join("episode.mkv"), b"payload").unwrap();
        db.finish(job, &source, &root, "active").unwrap();
        deliveries.push(
            db.create_delivery(job, true, Some(&format!("request-{job}")))
                .unwrap(),
        );
    }
    db.db
        .lock()
        .unwrap()
        .execute(
            "UPDATE deliveries SET data='{' WHERE id=?1",
            [&deliveries[0].delivery_id],
        )
        .unwrap();
    let (page, cursor) = db.deliveries_discovery_page(0, 100).unwrap();
    assert_eq!(page.len(), 2);
    assert_eq!(page[0]["deliveryId"], deliveries[0].delivery_id);
    assert_eq!(page[0]["sequence"], deliveries[0].sequence);
    assert!(page[0]["discoveryError"]
        .as_str()
        .unwrap()
        .contains("invalid"));
    assert_eq!(page[1]["deliveryId"], deliveries[1].delivery_id);
    assert_eq!(cursor, deliveries[1].sequence);
}

#[test]
fn blocked_startup_root_does_not_block_other_root_or_durable_metadata() {
    use std::sync::{mpsc, Condvar};
    let (_tmp, inventory, root_a) = fixture();
    let root_b = root_a.parent().unwrap().join("other-root");
    std::fs::create_dir(&root_b).unwrap();
    inventory
        .allocate(81, &root_a, &root_a.join("retiring"))
        .unwrap();
    inventory.prepare_forget(81).unwrap();
    let inventory = Arc::new(inventory);
    let (arrive, arrival) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    *startup::ROOT_OPEN_GATE.lock().unwrap() = Some((root_a.clone(), arrive, release.clone()));
    let recovering = inventory.clone();
    let recovery = std::thread::spawn(move || recovering.reconcile_startup_root(&root_a, &[81]));
    if arrival.recv_timeout(Duration::from_secs(2)).is_err() {
        *release.0.lock().unwrap() = true;
        release.1.notify_all();
        *startup::ROOT_OPEN_GATE.lock().unwrap() = None;
        panic!("root recovery did not reach the payload-open boundary");
    }
    let healthy = inventory.clone();
    let (send, receive) = mpsc::channel();
    let admission = std::thread::spawn(move || {
        let result = (|| {
            let mut settings = healthy.settings()?;
            settings.failed_retention_days = 3;
            healthy.set_settings(&settings)?;
            healthy.allocate(82, &root_b, &root_b.join("new-job"))?;
            Ok::<_, Error>(())
        })();
        send.send(result).unwrap();
    });
    let result = receive.recv_timeout(Duration::from_secs(2));
    // Always release the worker before asserting, even on regression.
    *release.0.lock().unwrap() = true;
    release.1.notify_all();
    recovery.join().unwrap().unwrap();
    admission.join().unwrap();
    result
        .expect("healthy root and durable metadata waited for the blocked payload root")
        .unwrap();
    assert_eq!(inventory.settings().unwrap().failed_retention_days, 3);
    assert!(inventory.for_job(82).unwrap().is_some());
}

#[test]
fn adopted_startup_orphan_restores_only_automatic_keep_and_hold() {
    let (_tmp, inventory, root) = fixture();
    let artifact = inventory
        .allocate(83, &root, &root.join("adopted"))
        .unwrap();
    inventory.reconcile_startup_root(&root, &[]).unwrap();
    let recovered = inventory.reclaim_recovered_orphan(83).unwrap().unwrap();
    assert_eq!(recovered.id, artifact.id);
    assert_eq!(recovered.state, "active");
    assert_eq!(recovered.keep, artifact.keep);
    assert_eq!(recovered.hold, artifact.hold);
    let kept = inventory
        .retention(&recovered.id, recovered.revision, true, None)
        .unwrap();
    inventory.reconcile_startup_root(&root, &[]).unwrap();
    let recovered = inventory.reclaim_recovered_orphan(83).unwrap().unwrap();
    assert!(recovered.keep && kept.keep);
}

#[test]
fn legacy_worker_enrollment_allows_operational_custody_without_cleanup_ownership() {
    let (_tmp, inventory, root) = fixture();
    let inventory = Arc::new(inventory);
    let path = root.join("shared-worker-input");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("payload.bin"), b"shared bytes").unwrap();
    inventory.register_legacy_active(801, &root, &path).unwrap();
    let admitted = inventory.for_job(801).unwrap().unwrap();
    assert_eq!(admitted.state, "active");
    assert!(!admitted.owned);
    assert!(admitted.keep);
    assert!(admitted.hold.is_none());
    assert_eq!(
        inventory.allocate(801, &root, &path).unwrap().id,
        admitted.id
    );
    let custody = inventory
        .acquire_terminal_use(801, &|| Ok(()))
        .unwrap()
        .unwrap();
    assert_eq!(custody.source().artifact, admitted.id);
    assert!(inventory
        .request_delete(&admitted.id, admitted.revision, "must-not-delete", 0)
        .is_err());
    drop(custody);

    let mut held = inventory.for_job(801).unwrap().unwrap();
    held.hold = Some("review: operator blocked this source".into());
    held.revision += 1;
    save_artifact(&inventory.db.lock().unwrap(), &held).unwrap();
    inventory.register_legacy_active(801, &root, &path).unwrap();
    let preserved = inventory.for_job(801).unwrap().unwrap();
    assert_eq!(preserved.hold, held.hold);
    assert_eq!(preserved.revision, held.revision);
}

#[test]
fn unavailable_legacy_enrollment_preserves_existing_association_and_controls() {
    let (temp, inventory, root) = fixture();
    let path = root.join("shared-worker-input");
    std::fs::create_dir(&path).unwrap();
    inventory.register_legacy_active(802, &root, &path).unwrap();
    let before = inventory.for_job(802).unwrap().unwrap();
    std::fs::rename(&root, temp.path().join("offline-root")).unwrap();
    assert!(inventory.register_legacy_active(802, &root, &path).is_err());
    let after = inventory.for_job(802).unwrap().unwrap();
    assert_eq!(after.id, before.id);
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.path, before.path);
    assert_eq!(after.hold, before.hold);
    assert_eq!(after.job, Some(802));
}

#[test]
fn completed_relocation_retirement_requires_released_exact_generation() {
    for with_delivery in [false, true] {
        let (_temporary, inventory, root) = fixture();
        let inventory = Arc::new(inventory);
        let source_path = root.join("generation-retirement-source");
        inventory
            .allocate_for_delivery(84, &root, &source_path, true, with_delivery)
            .unwrap();
        std::fs::write(source_path.join("episode.mkv"), b"verified publication").unwrap();
        let source = inventory.finish(84, &source_path, &root, "active").unwrap();
        let use_guard = inventory.acquire_attempt_use(84).unwrap().unwrap();
        let generation = use_guard.source().clone();
        let published_root = root
            .parent()
            .unwrap()
            .join("completed-generation-retirement");
        std::fs::create_dir(&published_root).unwrap();
        let published_path = published_root.join("published");
        let publish = if with_delivery {
            Inventory::publish_selected_for_delivery_checked
        } else {
            Inventory::publish_selected_checked
        };
        let publication = publish(
            &inventory,
            84,
            &published_path,
            source.files.clone(),
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
        let retired_source_id = format!("source-{}", publication.operation_id);
        assert!(inventory.retire_completed_relocations(&generation).is_err());
        assert!(
            source_path.exists(),
            "the source directory waits for release"
        );
        assert!(published_path.join("episode.mkv").exists());

        let delivery = with_delivery.then(|| {
            inventory
                .create_delivery(84, true, Some("release-custody"))
                .unwrap()
        });
        if let Some(delivery) = &delivery {
            let pending = inventory
                .retire_delivery_source(&delivery.delivery_id)
                .unwrap();
            assert_eq!(pending.source_cleanup.status, "retry");
            assert!(pending.source_cleanup.next_retry_at.unwrap() > now());
        }
        drop(use_guard);
        inventory
            .retire_completed_relocations(&SourceGeneration {
                artifact: generation.artifact.clone(),
                generation: "another-generation".into(),
            })
            .unwrap();
        assert!(source_path.exists());
        assert_ne!(inventory.get(&retired_source_id).unwrap().state, "deleted");

        inventory.retire_completed_relocations(&generation).unwrap();
        assert!(!source_path.exists());
        if let Some(delivery) = &delivery {
            assert_eq!(
                inventory
                    .delivery(&delivery.delivery_id)
                    .unwrap()
                    .source_cleanup
                    .status,
                "succeeded"
            );
        }
        assert_eq!(inventory.get(&retired_source_id).unwrap().state, "deleted");
        let published = inventory.for_job(84).unwrap().unwrap();
        let descriptor = inventory.verify(&published).unwrap();
        assert_eq!(fs::manifest(&descriptor, 100_000).unwrap(), published.files);
        assert_eq!(
            std::fs::read(published_path.join("episode.mkv")).unwrap(),
            b"verified publication"
        );
    }
}
#[test]
fn empty_retained_failed_source_is_disposed_but_files_and_keep_are_not() {
    // Field report 2026-11-02: health failures retained empty directories
    // "for recovery" and froze Curator's replacement behind nothing.
    let (_tmp, db, root) = fixture();
    let empty = root.join("health-failed-empty");
    db.allocate_for_delivery(71, &root, &empty, true, false)
        .unwrap();
    let empty_artifact = db.finish(71, &empty, &root, "retained").unwrap();

    let populated = root.join("health-failed-with-bytes");
    db.allocate_for_delivery(72, &root, &populated, true, false)
        .unwrap();
    std::fs::write(populated.join("partial.mkv"), b"bytes a person may inspect").unwrap();
    let populated_artifact = db.finish(72, &populated, &root, "retained").unwrap();

    let kept = root.join("health-failed-kept");
    db.allocate_for_delivery(73, &root, &kept, true, false)
        .unwrap();
    let kept_artifact = db.finish(73, &kept, &root, "retained").unwrap();
    assert!(db.set_keep_on_failure(73, true).unwrap());

    db.reconcile_empty_failed_sources().unwrap();
    db.run_due_deletes().unwrap();

    assert!(
        !empty.exists(),
        "an empty retained source has nothing to keep"
    );
    assert_eq!(db.get(&empty_artifact.id).unwrap().state, "deleted");
    assert!(populated.join("partial.mkv").exists());
    assert_eq!(db.get(&populated_artifact.id).unwrap().state, "retained");
    assert!(kept.exists(), "an explicit Keep is never overridden");
    assert_eq!(db.get(&kept_artifact.id).unwrap().state, "retained");
}

#[test]
fn external_output_observation_never_adopts_owned_associated_or_replaced_paths() {
    let (_tmp, inventory, root) = fixture();
    let source = root.join("owned-source");
    let owned = inventory
        .allocate_for_delivery(810, &root, &source, true, false)
        .unwrap();
    assert!(inventory.observe_external_output(&root, &source).is_err());
    assert!(inventory.get(&owned.id).unwrap().owned);

    let result = root.join("external-output");
    std::fs::create_dir(&result).unwrap();
    let observed = inventory.observe_external_output(&root, &result).unwrap();
    assert!(!observed.owned);
    assert_eq!(observed.job, None);
    inventory
        .register_legacy_active(811, &root, &result)
        .unwrap();
    assert!(inventory.observe_external_output(&root, &result).is_err());
    assert_eq!(inventory.for_job(811).unwrap().unwrap().id, observed.id);

    let replaced = root.join("replaced-output");
    std::fs::create_dir(&replaced).unwrap();
    let captured = inventory.observe_external_output(&root, &replaced).unwrap();
    std::fs::rename(&replaced, root.join("original-output")).unwrap();
    std::fs::create_dir(&replaced).unwrap();
    std::fs::write(replaced.join("foreign.bin"), b"foreign bytes").unwrap();
    assert!(inventory.observe_external_output(&root, &replaced).is_err());
    let unchanged = inventory.get(&captured.id).unwrap();
    assert_eq!(unchanged.identity, captured.identity);
    assert!(!unchanged.owned);
    assert_eq!(
        std::fs::read(replaced.join("foreign.bin")).unwrap(),
        b"foreign bytes"
    );
}

/// Contract §2.6 (orchestrator addition to M3): a park whose destination
/// root cannot be created is journaled as the move's first step — an
/// operation on the retry ladder, visible with the artifact (Attention) and
/// to the disposal gate's `disposal_retry_at` — and the same request
/// succeeds once the root can be made.
#[test]
fn park_root_creation_failure_lands_on_the_operation_ladder() {
    let (tmp, db, root) = fixture();
    let source = root.join("failed-source");
    db.allocate_for_delivery(88, &root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("episode.mkv"), b"failed bytes").unwrap();
    db.finish(88, &source, &root, "active").unwrap();
    // The park root's parent is a regular file: create_dir_all fails.
    let blocker = tmp.path().join("parked");
    std::fs::write(&blocker, b"not a directory").unwrap();
    let target = blocker.join("failed").join("failed-source");
    let error = db.relocate(88, &target).unwrap_err();
    assert!(matches!(error, Error::Io(_)), "{error}");
    let artifact = db.for_job(88).unwrap().unwrap();
    let operations = db.pending_relocations(&artifact.id).unwrap();
    assert_eq!(operations.len(), 1, "the failure is journaled");
    let operation = &operations[0];
    assert_eq!(operation.state, "retry");
    assert_eq!(operation.attempts, 1);
    assert!(operation.next_retry > now(), "on the ladder");
    assert!(operation
        .error
        .as_deref()
        .is_some_and(|error| error.contains("could not be created")));
    assert_eq!(
        db.disposal_retry_at(88).unwrap(),
        Some(operation.next_retry)
    );
    assert!(artifact
        .error
        .as_deref()
        .is_some_and(|error| error.starts_with("destination root ")));
    let attention = db
        .list_page(&ListQuery {
            limit: 50,
            filter: ListFilter::Attention,
            ..ListQuery::default()
        })
        .unwrap();
    assert!(
        attention
            .rows
            .iter()
            .any(|row| row.artifact.id == artifact.id),
        "visible under Attention while its error stands"
    );
    assert!(source.join("episode.mkv").exists(), "nothing moved");
    // A second failure climbs the ladder.
    assert!(db.relocate(88, &target).is_err());
    let again = db.pending_relocations(&artifact.id).unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].attempts, 2);
    // Once the root can be created, the same request parks the source.
    std::fs::remove_file(&blocker).unwrap();
    let moved = db.relocate(88, &target).unwrap();
    assert_eq!(moved.published_path, target);
    assert!(target.join("episode.mkv").exists());
    assert!(db.pending_relocations(&artifact.id).unwrap().is_empty());
    assert!(db.for_job(88).unwrap().unwrap().error.is_none());
}

/// M6b: the v1 acknowledgement columns an M1a-or-older build wrote are
/// dropped on open, and the rows they sat beside survive.
#[test]
fn v1_acceptance_columns_are_dropped_on_open() {
    let (tmp, db, _root) = fixture();
    db.close().unwrap();
    drop(db);
    let path = tmp.path().join("state/artifacts.sqlite");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "ALTER TABLE deliveries ADD COLUMN accepted_at INTEGER;
             ALTER TABLE deliveries ADD COLUMN consumer_receipt_id TEXT;
             INSERT INTO deliveries(id,artifact,generation,data,accepted_at,consumer_receipt_id)
               VALUES('d1','a1','g1','{}',1794076138,'receipt');",
        )
        .unwrap();
    }
    let reopened = Inventory::open(&tmp.path().join("state")).unwrap();
    reopened.close().unwrap();
    drop(reopened);
    let conn = rusqlite::Connection::open(&path).unwrap();
    let columns: Vec<String> = conn
        .prepare("SELECT name FROM pragma_table_info('deliveries')")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    assert!(
        !columns
            .iter()
            .any(|c| c == "accepted_at" || c == "consumer_receipt_id"),
        "{columns:?}"
    );
    let kept: i64 = conn
        .query_row("SELECT COUNT(*) FROM deliveries WHERE id='d1'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(kept, 1);
}

// No-wedge task 2: every review/retained hold takes its registered exit.

fn held(db: &Inventory, root: &Path, job: u32, hold: &str) -> Artifact {
    let path = root.join(format!("held-{job}"));
    db.allocate(job, root, &path).unwrap();
    std::fs::write(path.join("payload.bin"), b"payload bytes").unwrap();
    let mut a = db.finish(job, &path, root, "retained").unwrap();
    a.hold = Some(hold.into());
    a.updated_at = now() - 2 * exits::HOLD_EXIT_SECONDS;
    save_artifact(&db.db.lock().unwrap(), &a).unwrap();
    a
}

#[test]
fn cancelled_recovery_holds_exit_and_only_the_copy_is_disposed() {
    let (_tmp, db, root) = fixture();
    let source = held(&db, &root, 1, "review: cancelled recovery");
    let staging = held(&db, &root, 2, "review: cancelled recovery staging");
    let report = db.exit_wedged_states(0, false).unwrap();
    assert_eq!((report.released, report.parked), (1, 1));
    let source = db.get(&source.id).unwrap();
    assert!(source.hold.is_none());
    assert_eq!(source.state, "retained", "the original keeps its lifecycle");
    let copy = db.get(&staging.id).unwrap();
    assert_eq!(copy.state, "parked_failed");
    assert!(copy.earliest_expiry().is_some_and(|due| due <= now()));
    db.tick().unwrap();
    db.run_due_deletes().unwrap();
    assert!(!staging.path.exists(), "the copy is disposed");
    assert!(source.path.exists(), "the original is never removed");
    // Idempotent: nothing left to convert.
    assert_eq!(db.exit_wedged_states(0, true).unwrap().total(), 0);
}

#[test]
fn unowned_content_is_never_touched_and_its_delete_is_cancelled() {
    let (_tmp, db, root) = fixture();
    let adopted = held(&db, &root, 1, "review: files changed; adoption required");
    let discovered = held(&db, &root, 2, "review");
    let mut unheld = db.get(&discovered.id).unwrap();
    unheld.hold = None;
    save_artifact(&db.db.lock().unwrap(), &unheld).unwrap();
    let op = db
        .request_delete(&discovered.id, unheld.revision, "old-delete", 0)
        .unwrap();
    for (a, hold) in [
        (&adopted, "review: files changed; adoption required"),
        (&discovered, "review"),
    ] {
        let mut a = db.get(&a.id).unwrap();
        a.owned = false;
        a.hold = Some(hold.into());
        save_artifact(&db.db.lock().unwrap(), &a).unwrap();
    }
    {
        let db = db.db.lock().unwrap();
        db.execute(
            "UPDATE operations SET state='review',data=json_set(data,'$.state','review','$.error','deletion generation changed') WHERE id=?1",
            [&op.id],
        )
        .unwrap();
    }
    db.exit_wedged_states(0, true).unwrap();
    for a in [&adopted, &discovered] {
        let after = db.get(&a.id).unwrap();
        assert!(after.hold.is_some() && !after.owned && after.state == "retained");
        assert!(a.path.exists());
    }
    assert_eq!(db.operation(&op.id).unwrap().state, "cancelled");
}

#[test]
fn refused_disposal_resnapshots_the_closed_scope_and_retries() {
    let (_tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let op = db.request_delete(&a.id, a.revision, "dispose", 0).unwrap();
    // Something inside the job's own directory that is not in the manifest.
    std::fs::write(a.path.join("late.part"), b"written after the snapshot").unwrap();
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "review");
    let mut refused = db.get(&a.id).unwrap();
    assert_eq!(refused.hold.as_deref(), Some("review"));
    refused.updated_at = now() - 2 * exits::HOLD_EXIT_SECONDS;
    save_artifact(&db.db.lock().unwrap(), &refused).unwrap();
    // The tick's grace has passed: re-snapshot and retry.
    let report = db
        .exit_wedged_states(exits::HOLD_EXIT_SECONDS, true)
        .unwrap();
    assert_eq!(report.retried, 1);
    assert_eq!(db.operation(&op.id).unwrap().state, "retry");
    assert!(db.get(&a.id).unwrap().hold.is_none());
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "succeeded");
    assert!(!a.path.exists());
}

/// Finding 4: names the manifest refuses (a symlink out of the tree, a fifo)
/// no longer block the re-snapshot: the closed scope is emptied by raw name
/// (the link's target survives) and the disposal completes.
#[cfg(unix)]
#[test]
fn refused_names_in_a_closed_scope_do_not_block_its_disposal() {
    let (tmp, db, root) = fixture();
    let a = parked(&db, &root);
    let op = db.request_delete(&a.id, a.revision, "dispose", 0).unwrap();
    // A regular late file first causes a real, authorized review state.
    // Add refused names only after that, before the automatic re-snapshot.
    std::fs::write(a.path.join("late.part"), b"after the snapshot").unwrap();
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "review");
    let outside = tmp.path().join("outside.bin");
    std::fs::write(&outside, b"not runner's").unwrap();
    std::os::unix::fs::symlink(&outside, a.path.join("link")).unwrap();
    let fifo = std::ffi::CString::new(std::os::unix::ffi::OsStrExt::as_bytes(
        a.path.join("pipe").as_os_str(),
    ))
    .unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    let mut refused = db.get(&a.id).unwrap();
    assert_eq!(refused.hold.as_deref(), Some("review"));
    refused.updated_at = now() - 2 * exits::HOLD_EXIT_SECONDS;
    save_artifact(&db.db.lock().unwrap(), &refused).unwrap();
    let report = db
        .exit_wedged_states(exits::HOLD_EXIT_SECONDS, true)
        .unwrap();
    assert_eq!(report.retried, 1);
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "succeeded");
    assert!(!a.path.exists());
    assert_eq!(std::fs::read(&outside).unwrap(), b"not runner's");
}

#[test]
fn a_fresh_hold_waits_for_the_grace_and_a_restart_releases_quiescence() {
    let (tmp, db, root) = fixture();
    let a = held(&db, &root, 1, "review: subprocess quiescence unconfirmed");
    let mut fresh = db.get(&a.id).unwrap();
    fresh.updated_at = now();
    save_artifact(&db.db.lock().unwrap(), &fresh).unwrap();
    assert_eq!(
        db.exit_wedged_states(exits::HOLD_EXIT_SECONDS, true)
            .unwrap()
            .total(),
        0,
        "the flow that set it gets HOLD_EXIT_SECONDS first"
    );
    db.close().unwrap();
    drop(db);
    // No subprocess outlives the process: the open releases it.
    let db = Inventory::open(&tmp.path().join("state")).unwrap();
    assert!(db.get(&a.id).unwrap().hold.is_none());
}

#[test]
fn an_interrupted_writer_retirement_is_reclassified() {
    let (_tmp, db, root) = fixture();
    let gone = held(&db, &root, 1, "review: interrupted writer retirement");
    let replaced = held(&db, &root, 2, "review: interrupted writer retirement");
    let intact = held(&db, &root, 3, "review: interrupted writer retirement");
    std::fs::remove_dir_all(&gone.path).unwrap();
    // Keep the old inode allocated: ext4 can immediately reuse it after
    // unlink, which would make this a same-object fixture by accident.
    let original_dir = fs::open_dir(&replaced.path).unwrap();
    std::fs::remove_dir_all(&replaced.path).unwrap();
    std::fs::create_dir(&replaced.path).unwrap();
    std::fs::write(replaced.path.join("someone-else"), b"x").unwrap();
    let replacement = fs::open_dir(&replaced.path).unwrap();
    assert!(!fs::identity_for_file(&original_dir)
        .unwrap()
        .same_object(&fs::identity_for_file(&replacement).unwrap()));
    db.exit_wedged_states(0, true).unwrap();
    assert_eq!(db.get(&gone.id).unwrap().state, "source_gone");
    let other = db.get(&replaced.id).unwrap();
    assert!(!other.owned && other.hold.as_deref() == Some("review"));
    assert!(replaced.path.join("someone-else").exists(), "never deleted");
    let parked = db.get(&intact.id).unwrap();
    assert_eq!(parked.state, "parked_failed");
    assert!(parked.hold.is_none() && parked.earliest_expiry().is_some());
}

#[test]
fn materialization_predecessors_and_orphans_park_with_the_failed_ttl() {
    let (_tmp, db, root) = fixture();
    let mut rows = Vec::new();
    for (job, hold) in [
        (1, "review: superseded by accepted generation"),
        (2, "review: allocation has no recovered job"),
        (3, "review: previous external job generation"),
        (4, "review: superseded private processing source"),
    ] {
        let a = held(&db, &root, job, hold);
        let mut pinned = db.get(&a.id).unwrap();
        pinned.keep = true; // the pin the flow set with the hold
        save_artifact(&db.db.lock().unwrap(), &pinned).unwrap();
        rows.push(a);
    }
    let report = db.exit_wedged_states(0, false).unwrap();
    assert_eq!(report.parked, 4);
    for a in rows {
        let after = db.get(&a.id).unwrap();
        assert_eq!(after.state, "parked_failed", "{:?}", a.hold);
        assert!(!after.keep && after.hold.is_none());
        assert!(
            after.deadline.is_some_and(|due| due > now()),
            "TTL, not now"
        );
        assert!(a.path.exists());
    }
}

#[test]
fn pre_upgrade_automatic_delete_in_review_resumes_on_the_tick() {
    let (_tmp, db, root) = fixture();
    let legacy = parked(&db, &root);
    let op = db
        .request_delete_authorized(&legacy.id, legacy.revision, "legacy-retention", 0, true)
        .unwrap();
    assert_eq!(db.execute_delete(&op.id).unwrap().state, "review");
    let report = db.exit_wedged_states(0, false).unwrap();
    assert_eq!(report.retried, 1);
    assert_eq!(db.operation(&op.id).unwrap().state, "retry");
    assert!(db.get(&legacy.id).unwrap().automatic_cleanup_eligible);
}

/// Finding 8: releasing a restore quarantine gives each row back the Keep it
/// had — the forced Keep does not block delivery cleanup forever — while an
/// operator's Keep survives; an already-quarantined (upgrade) row loses the
/// forced Keep unless an explicit operator Keep event exists.
#[test]
fn releasing_a_restore_quarantine_restores_the_previous_keep() {
    let (_tmp, db, root) = fixture();
    let plain = held(&db, &root, 1, "x");
    let kept = held(&db, &root, 2, "x");
    let upgraded = held(&db, &root, 3, "x");
    let upgraded_kept = held(&db, &root, 4, "x");
    for a in [&plain, &kept, &upgraded, &upgraded_kept] {
        let mut row = db.get(&a.id).unwrap();
        row.hold = None;
        save_artifact(&db.db.lock().unwrap(), &row).unwrap();
    }
    let row = db.get(&kept.id).unwrap();
    db.retention(&kept.id, row.revision, true, None).unwrap();
    let row = db.get(&upgraded_kept.id).unwrap();
    db.retention(&upgraded_kept.id, row.revision, true, None)
        .unwrap();
    db.quarantine_restore().unwrap();
    // Rows quarantined by a build that recorded nothing.
    db.db
        .lock()
        .unwrap()
        .execute(
            "DELETE FROM events WHERE kind='restore_quarantine' AND artifact IN (?1,?2)",
            rusqlite::params![upgraded.id, upgraded_kept.id],
        )
        .unwrap();
    for a in [&plain, &kept, &upgraded, &upgraded_kept] {
        let mut row = db.get(&a.id).unwrap();
        assert!(row.keep);
        row.updated_at = now() - 2 * exits::HOLD_EXIT_SECONDS;
        save_artifact(&db.db.lock().unwrap(), &row).unwrap();
    }
    db.exit_wedged_states(exits::HOLD_EXIT_SECONDS, true)
        .unwrap();
    for (a, keep) in [
        (&plain, false),
        (&kept, true),
        (&upgraded, false),
        (&upgraded_kept, true),
    ] {
        let row = db.get(&a.id).unwrap();
        assert!(row.hold.is_none(), "{}", a.id);
        assert_eq!(row.keep, keep, "{}", a.id);
    }
}

/// Finding 10: parking a pinned hold clears only the pin's Keep; an
/// operator's explicit Keep stays.
#[test]
fn parking_a_pinned_hold_keeps_an_operator_keep() {
    let (_tmp, db, root) = fixture();
    let pinned = held(
        &db,
        &root,
        1,
        "review: superseded private processing source",
    );
    let operator = held(&db, &root, 2, "x");
    let mut row = db.get(&operator.id).unwrap();
    row.hold = None;
    save_artifact(&db.db.lock().unwrap(), &row).unwrap();
    let row = db.get(&operator.id).unwrap();
    db.retention(&operator.id, row.revision, true, None)
        .unwrap();
    for (a, hold) in [
        (&pinned, "review: superseded private processing source"),
        (&operator, "review: superseded private processing source"),
    ] {
        let mut row = db.get(&a.id).unwrap();
        row.hold = Some(hold.into());
        row.keep = true;
        row.updated_at = now() - 2 * exits::HOLD_EXIT_SECONDS;
        save_artifact(&db.db.lock().unwrap(), &row).unwrap();
    }
    db.exit_wedged_states(exits::HOLD_EXIT_SECONDS, true)
        .unwrap();
    let pinned = db.get(&pinned.id).unwrap();
    assert_eq!(pinned.state, "parked_failed");
    assert!(!pinned.keep, "the pin's Keep is cleared");
    let operator = db.get(&operator.id).unwrap();
    assert_eq!(operator.state, "parked_failed");
    assert!(operator.keep, "the operator's Keep stays");
}

/// Finding 12: a recovery handoff waiting on a consumer that never claims
/// (`published`) or stops heartbeating its claim (`claimed`) for
/// failed_retention_days is cancelled through the existing path; a fresh
/// one is untouched.
#[test]
fn silent_recovery_handoffs_are_cancelled_after_the_failed_ttl() {
    let (tmp, db, root) = fixture();
    let mut ids = Vec::new();
    for job in 1..=3u32 {
        let path = root.join(format!("job-{job}"));
        db.allocate(job, &root, &path).unwrap();
        std::fs::write(path.join("episode.mkv"), b"media").unwrap();
        let a = db.finish(job, &path, &root, "parked_failed").unwrap();
        let r = db
            .stage_recovery(
                &a.id,
                a.revision,
                &format!("handoff-{job}"),
                &["episode.mkv".into()],
                &tmp.path().join(format!("recovery-{job}")),
            )
            .unwrap();
        ids.push(r);
    }
    db.claim_recovery(&ids[1].id, "curator", "import", &ids[1].manifest_digest)
        .unwrap();
    let ttl = i64::from(db.settings().unwrap().failed_retention_days) * 86400;
    for r in &ids[..2] {
        let mut stale = db.recovery(&r.id).unwrap();
        stale.updated_at = now() - ttl - 60;
        super::recovery::save(&db.db.lock().unwrap(), &stale).unwrap();
    }
    let report = db.exit_wedged_states(0, true).unwrap();
    assert_eq!(report.recoveries, 2, "{report:?}");
    assert_eq!(db.recovery(&ids[0].id).unwrap().state, "cancelled");
    assert_eq!(db.recovery(&ids[1].id).unwrap().state, "cancelled");
    assert_ne!(db.recovery(&ids[2].id).unwrap().state, "cancelled");
    // A late claim from the silent consumer is refused.
    assert!(db
        .claim_recovery(&ids[1].id, "curator", "import", &ids[1].manifest_digest)
        .is_err());
}

// ---------------------------------------------------------------------------
// Post-move fixes (2026-11-11): failed publications, failed transforms,
// rediscovered publications, the attention rule.
// ---------------------------------------------------------------------------

fn fail_terminal(db: &Inventory, job: u32, keep_on_failure: bool) {
    let mut terminal = terminals::tests::failed(job, SalvageState::SettledNone);
    terminal.keep_on_failure = keep_on_failure;
    db.record_terminal(&terminal).unwrap();
    db.mark_terminal_step(job, &TerminalStep::Emitted).unwrap();
}

/// A copy publication of `job` from `root/job-<job>` (media + a PAR volume
/// + an archive) to `<tmp>/completed/release`; its source stays behind.
fn copy_published(tmp: &tempfile::TempDir, db: &Inventory, root: &Path, job: u32) -> PathBuf {
    let source = root.join(format!("job-{job}"));
    db.allocate_for_delivery(job, root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("media.mkv"), b"payload").unwrap();
    std::fs::write(source.join("media.vol00+01.par2"), b"recovery volume").unwrap();
    std::fs::write(source.join("media.rar"), b"unextracted archive").unwrap();
    db.finish(job, &source, root, "active").unwrap();
    let completed = tmp.path().join("completed");
    std::fs::create_dir_all(&completed).unwrap();
    fs::RENAME_FAILURE.with(|failure| failure.set(Some(libc::EINVAL)));
    db.relocate_for_delivery_checked(job, &completed.join(format!("release-{job}")), &|| Ok(()))
        .unwrap();
    assert!(source.exists(), "copy publication retains its source");
    source
}

/// Park disposition of a failed copy publication: the remainder the
/// publication did not take (PAR volumes, archives — recovery input) is
/// never wiped. It is parked in place with the failed TTL, so retention is
/// its exit; closed scope is not granted; a second pass is a no-op.
#[test]
fn a_parked_failed_publications_remainder_is_parked_not_wiped() {
    let (tmp, db, root) = fixture();
    let source = copy_published(&tmp, &db, &root, 47);
    fail_terminal(&db, 47, false);
    assert!(
        db.park_superseded_publication_sources(47)
            .unwrap()
            .is_empty(),
        "the publication is still at its destination: nothing supersedes it"
    );
    let failed = tmp.path().join("failed");
    std::fs::create_dir(&failed).unwrap();
    db.relocate(47, &failed.join("release")).unwrap();
    assert!(
        db.retire_superseded_publication_sources(47)
            .unwrap()
            .is_empty(),
        "a park never grants closed scope"
    );
    let parked = db.park_superseded_publication_sources(47).unwrap();
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].path, source);
    assert!(matches!(parked[0].outcome, SupersededOutcome::Parked(_)));
    assert!(source.join("media.vol00+01.par2").exists());
    assert!(source.join("media.rar").exists());
    let remainder = db
        .for_path(&source)
        .unwrap()
        .filter(|row| row.id.starts_with("source-"))
        .unwrap();
    assert_eq!(remainder.state, "parked_failed");
    assert!(remainder.owned && remainder.deadline.is_some_and(|due| due > now()));
    assert_eq!(
        db.park_superseded_publication_sources(47).unwrap()[0].outcome,
        SupersededOutcome::Parked("parked in place with the failed TTL".into()),
        "idempotent"
    );
}

/// Delete disposition: once the published generation itself is deleted (a
/// succeeded delete operation), the pre-publication source retires by
/// closed scope, as after a delivery.
#[test]
fn a_deleted_failed_publications_source_retires_by_closed_scope() {
    let (tmp, db, root) = fixture();
    let db = Arc::new(db);
    let source = copy_published(&tmp, &db, &root, 49);
    fail_terminal(&db, 49, false);
    let guard = db.acquire_terminal_use(49, &|| Ok(())).unwrap().unwrap();
    let op = db.delete_failed_payload(&guard, 49, &|| Ok(())).unwrap();
    assert_eq!(op.state, "succeeded");
    drop(guard);
    let retired = db.retire_superseded_publication_sources(49).unwrap();
    assert_eq!(
        retired,
        vec![SupersededSource {
            path: source.clone(),
            outcome: SupersededOutcome::Retired,
        }]
    );
    assert!(!source.exists());
    assert!(db
        .retire_superseded_publication_sources(49)
        .unwrap()
        .is_empty());
}

/// A terminal row that was not deleted (`source_gone`) is no proof of a
/// Delete disposition: no closed scope.
#[test]
fn a_vanished_publication_grants_no_closed_scope() {
    let (tmp, db, root) = fixture();
    let source = copy_published(&tmp, &db, &root, 50);
    fail_terminal(&db, 50, false);
    let mut published = db.for_job(50).unwrap().unwrap();
    published.state = "source_gone".into();
    save_artifact(&db.db.lock().unwrap(), &published).unwrap();
    assert!(db
        .retire_superseded_publication_sources(50)
        .unwrap()
        .is_empty());
    assert!(source.join("media.rar").exists());
}

/// Keep-on-failure content is never deleted nor parked away.
#[test]
fn a_kept_failure_keeps_its_superseded_publication_source() {
    let (tmp, db, root) = fixture();
    let source = copy_published(&tmp, &db, &root, 48);
    fail_terminal(&db, 48, true);
    let failed = tmp.path().join("failed");
    std::fs::create_dir(&failed).unwrap();
    db.relocate(48, &failed.join("release")).unwrap();
    assert!(db
        .retire_superseded_publication_sources(48)
        .unwrap()
        .is_empty());
    assert!(db
        .park_superseded_publication_sources(48)
        .unwrap()
        .is_empty());
    assert!(source.join("media.mkv").exists());
}

/// An exhausted disposal ladder settles the obligation and gives the
/// payload a timed exit in the same transaction: live rows are parked with
/// the failed TTL; a kept row stays retained.
#[test]
fn an_exhausted_disposal_parks_what_remains() {
    let (_tmp, db, root) = fixture();
    let path = root.join("job-70");
    db.allocate(70, &root, &path).unwrap();
    std::fs::write(path.join("payload.bin"), b"bytes").unwrap();
    let mut row = db.finish(70, &path, &root, "active").unwrap();
    row.state = "transitioning".into();
    save_artifact(&db.db.lock().unwrap(), &row).unwrap();
    fail_terminal(&db, 70, false);
    assert_eq!(db.settle_exhausted_disposal(70, "settled").unwrap(), 1);
    let after = db.get(&row.id).unwrap();
    assert_eq!(after.state, "parked_failed");
    assert!(after.deadline.is_some_and(|due| due > now()));
    assert_eq!(
        db.terminal(70).unwrap().unwrap().disposal.as_deref(),
        Some("settled")
    );
    assert!(path.join("payload.bin").exists(), "nothing deleted now");
}

/// A transform journal the failed attempt left `running` with its workspace
/// gone pinned the source `transitioning`, and every disposal was refused
/// as "not quiescent". It ends with the attempt; the source becomes
/// quiescent and the disposition moves it.
#[test]
fn a_failed_jobs_unfinished_transform_ends_and_the_source_can_be_disposed() {
    let (tmp, db, root) = fixture();
    let source = root.join("job-5");
    db.allocate_for_delivery(5, &root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("a.rar"), b"archive").unwrap();
    let workspace = db.workspace(5, "extract", "abc").unwrap();
    assert_eq!(db.for_job(5).unwrap().unwrap().state, "transitioning");
    std::fs::remove_dir_all(&workspace.scratch.path).unwrap();
    let failed = tmp.path().join("failed");
    std::fs::create_dir(&failed).unwrap();
    let refused = db.relocate(5, &failed.join("job-5")).unwrap_err();
    assert!(refused.to_string().contains("not quiescent"), "{refused}");

    assert_eq!(
        db.end_failed_job_transforms(5).unwrap(),
        0,
        "no terminal yet: the attempt may still own it"
    );
    fail_terminal(&db, 5, false);
    assert_eq!(db.end_failed_job_transforms(5).unwrap(), 1);
    assert_eq!(
        db.operation(&workspace.operation_id).unwrap().state,
        "failed"
    );
    assert_eq!(db.get(&workspace.scratch.id).unwrap().state, "source_gone");
    assert_eq!(db.for_job(5).unwrap().unwrap().state, "active");
    assert_eq!(db.end_failed_job_transforms(5).unwrap(), 0, "idempotent");
    db.relocate(5, &failed.join("job-5")).unwrap();
    assert_eq!(
        std::fs::read(failed.join("job-5/a.rar")).unwrap(),
        b"archive",
        "nothing was deleted; the disposition moved it"
    );
}

/// A scan that walked a publication's destination before it committed
/// recorded Runner's own publication as unknown/Keep/review. Such a
/// duplicate (same path and directory object) is closed with a pointer to
/// Runner's row, keeping its record; a different object at the path is not
/// folded. An unregistered directory whose verifying manifest names one of
/// this installation's generations becomes `transferred`; one naming a
/// generation this installation never recorded, or none, stays foreign.
/// Idempotent, and manifests are not re-read on every pass.
#[test]
fn rediscovered_runner_publications_fold_back_into_custody() {
    let (_tmp, db, root) = fixture();
    let ours = root.join("published");
    db.allocate(61, &root, &ours).unwrap();
    std::fs::write(ours.join("ep.mkv"), b"episode").unwrap();
    let runner_row = db.finish(61, &ours, &root, "active").unwrap();
    let duplicate = {
        let stray = root.join("stray");
        std::fs::create_dir(&stray).unwrap();
        let mut duplicate = db.discover(&root, &stray, false).unwrap();
        std::fs::remove_dir(&stray).unwrap();
        duplicate.path = ours.clone();
        duplicate.identity = runner_row.identity.clone();
        save_artifact(&db.db.lock().unwrap(), &duplicate).unwrap();
        duplicate
    };
    let impostor = {
        let other = root.join("other");
        std::fs::create_dir(&other).unwrap();
        let mut impostor = db.discover(&root, &other, false).unwrap();
        impostor.path = ours.clone();
        save_artifact(&db.db.lock().unwrap(), &impostor).unwrap();
        impostor
    };

    let manifest_dir = |name: &str, generation: &str| {
        let dir = root.join(name);
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("ep.mkv"), b"payload!").unwrap();
        let manifest = intents::RunnerManifest::new(
            generation.into(),
            62,
            Some("t-62".into()),
            0,
            false,
            vec![intents::ManifestFile {
                path: "ep.mkv".into(),
                bytes: 8,
                sha256: "00".into(),
            }],
        );
        std::fs::write(
            dir.join(intents::RUNNER_MANIFEST_NAME),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        db.discover(&root, &dir, false).unwrap()
    };
    let orphan_row = manifest_dir("orphan.job-62.runner-a", &runner_row.generation);
    let stranger_row = manifest_dir("stranger", "0123456789abcdef");
    let foreign = root.join("hermes-cinema");
    std::fs::create_dir(&foreign).unwrap();
    std::fs::write(foreign.join("movie.mkv"), b"not ours").unwrap();
    let foreign_row = db.discover(&root, &foreign, false).unwrap();

    assert_eq!(db.fold_rediscovered_publications().unwrap(), 2);
    let closed = db.get(&duplicate.id).unwrap();
    assert_eq!(closed.state, "deleted", "closed, not erased");
    assert_eq!(closed.created_at, duplicate.created_at, "first seen kept");
    assert!(ours.join("ep.mkv").exists(), "no bytes touched");
    assert_eq!(db.get(&impostor.id).unwrap().state, "unknown");
    let transferred = db.get(&orphan_row.id).unwrap();
    assert_eq!(transferred.state, "transferred");
    assert!(!transferred.owned && !transferred.keep && transferred.hold.is_none());
    assert_eq!(db.get(&stranger_row.id).unwrap().state, "unknown");
    assert_eq!(db.get(&foreign_row.id).unwrap().state, "unknown");
    assert!(foreign.join("movie.mkv").exists(), "never deleted");
    assert_eq!(
        db.fold_rediscovered_publications().unwrap(),
        0,
        "idempotent"
    );
    let checked: i64 = db
        .db
        .lock()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM meta WHERE key LIKE 'publication-fold-checked:%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(checked >= 2, "foreign manifests are read once per revision");

    let page = |filter| {
        db.list_page(&ListQuery {
            limit: 50,
            filter,
            ..Default::default()
        })
        .unwrap()
    };
    assert_eq!(page(ListFilter::Attention).total, 0);
    assert_eq!(page(ListFilter::Foreign).total, 3);
}

/// A scan never records a publication still landing — an unresolved
/// relocation's destination, a directory containing one (a category), or
/// Runner's private `.runner-*` staging — as discovered content.
#[test]
fn a_scan_skips_a_publication_still_in_flight() {
    let (_tmp, db, root) = fixture();
    let landing = root.join("landing");
    let category = root.join("tv");
    let staging = root.join(".runner-move-abc");
    for dir in [&landing, &category, &staging] {
        std::fs::create_dir(dir).unwrap();
    }
    for (id, destination) in [
        ("move-landing", landing.clone()),
        ("move-tv", category.join("Show.S01E01")),
    ] {
        save_operation(
            &db.db.lock().unwrap(),
            &Operation {
                id: id.into(),
                artifact: "elsewhere".into(),
                kind: "relocate".into(),
                state: "retry".into(),
                request: serde_json::json!({ "destination": destination }).to_string(),
                created_at: now(),
                not_before: 0,
                attempts: 1,
                next_retry: now() + 60,
                error: None,
            },
        )
        .unwrap();
    }
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
    for dir in [&landing, &category, &staging] {
        assert!(db.for_path(dir).unwrap().is_none(), "{}", dir.display());
    }
}

/// Owner's rule (Paul, 2026-11-11): nothing Runner created may sit in
/// "Needs attention" while its exit is running. Every artifact state ×
/// every hold Runner writes, on a fresh Runner row with no error, stays out
/// of it; content Runner did not create is listed apart as foreign. What
/// must show does: an error, a hold with no registered exit, and any row
/// that has outlived its registered exit (an expired timed hold, a
/// retention deadline long past, an in-progress state no worker finished).
#[test]
fn attention_holds_only_what_needs_a_person_or_outlived_its_exit() {
    let (_tmp, db, root) = fixture();
    let base = root.join("base");
    db.allocate(1, &root, &base).unwrap();
    let template = db.finish(1, &base, &root, "active").unwrap();
    let holds = std::iter::once(None)
        .chain(
            exits::HOLD_EXITS
                .iter()
                .filter(|rule| !matches!(rule.exit, exits::Exit::Operator(_)))
                .map(|rule| {
                    Some(if rule.prefix {
                        format!("{}x", rule.hold)
                    } else {
                        rule.hold.to_string()
                    })
                }),
        )
        .collect::<Vec<_>>();
    let mut states = exits::STATE_EXITS
        .iter()
        .filter(|state| state.scope == "artifact")
        .map(|state| state.state)
        .collect::<Vec<_>>();
    states.push("transferred");
    let save = |id: &str, edit: &dyn Fn(&mut Artifact)| {
        let mut a = template.clone();
        a.id = id.into();
        edit(&mut a);
        save_artifact(&db.db.lock().unwrap(), &a).unwrap();
    };
    let mut rows = 0;
    for state in &states {
        for hold in &holds {
            rows += 1;
            save(&format!("runner-{rows}"), &|a| {
                a.state = (*state).into();
                // Custody handed to the consumer is the one unowned state
                // Runner creates.
                a.owned = *state != "transferred";
                a.keep = true;
                a.hold = hold.clone();
                a.updated_at = now();
            });
        }
    }
    let attention = || {
        db.list_page(&ListQuery {
            limit: 200,
            filter: ListFilter::Attention,
            ..Default::default()
        })
        .unwrap()
    };
    assert_eq!(attention().total, 0, "{:?}", attention().rows.first());
    for rule in exits::HOLD_EXITS {
        if matches!(rule.exit, exits::Exit::Operator(_)) {
            assert_eq!(
                rule.action,
                exits::HoldAction::Adoption,
                "{}: an operator exit is for content Runner did not create",
                rule.hold
            );
        }
    }
    let late = now() - exits::STUCK_PROGRESS_SECONDS - 2 * exits::OVERDUE_GRACE_SECONDS;
    save("errored", &|a| {
        a.error = Some("No space left on device".into())
    });
    save("missing-exit", &|a| {
        a.hold = Some("a hold nothing registered".into())
    });
    save("overdue-hold", &|a| {
        a.state = "retained".into();
        a.hold = Some("review: cancelled recovery".into());
        a.updated_at = late;
    });
    save("expired", &|a| {
        a.state = "parked_failed".into();
        a.keep = false;
        a.deadline = Some(late);
    });
    save("stuck", &|a| {
        a.state = "transitioning".into();
        a.updated_at = late;
    });
    // In flight for a day is not overdue: its own event ends it.
    save("long-transform", &|a| {
        a.state = "retained".into();
        a.hold = Some("transform workspace".into());
        a.updated_at = late;
    });
    let mut flagged = attention()
        .rows
        .into_iter()
        .map(|row| row.artifact.id)
        .collect::<Vec<_>>();
    flagged.sort();
    assert_eq!(
        flagged,
        [
            "errored",
            "expired",
            "missing-exit",
            "overdue-hold",
            "stuck"
        ]
    );
}

/// The interrupted-writer-retirement hold's exit ("gone → source_gone")
/// is reached at startup when the path is already gone, instead of being
/// re-armed (and its timer restarted) at every boot.
#[test]
fn startup_settles_a_retirement_whose_directory_is_gone() {
    let (_tmp, db, root) = fixture();
    let path = root.join("job-2595");
    db.allocate(2595, &root, &path).unwrap();
    let mut a = db.finish(2595, &path, &root, "active").unwrap();
    a.keep = true;
    save_artifact(&db.db.lock().unwrap(), &a).unwrap();
    std::fs::remove_dir_all(&path).unwrap();
    db.reconcile_startup_root(&root, &[]).unwrap();
    let after = db.get(&a.id).unwrap();
    assert_eq!(after.state, "source_gone");
    assert!(after.hold.is_none());
    assert!(db
        .operation(&format!("startup-orphan-{}-{}", a.id, a.generation))
        .is_err());
}

/// An interrupted allocation (identity never committed) at a name only
/// Runner gives stays Runner's: empty and orphaned it is removed (rmdir,
/// never recursive) and closed; with bytes and orphaned it is parked with
/// the failed TTL; for a live job it becomes its active allocation. A
/// directory at any other name is not claimed.
#[test]
fn interrupted_runner_allocations_end_instead_of_turning_foreign() {
    let (_tmp, db, root) = fixture();
    let allocating = |id: &str, job: Option<u32>, name: &str, bytes: bool| {
        let path = root.join(name);
        std::fs::create_dir(&path).unwrap();
        if bytes {
            std::fs::write(path.join("part.bin"), b"partial").unwrap();
        }
        let mut a = db.discover(&root, &path, false).unwrap();
        a.id = id.into();
        a.job = job;
        a.state = "allocating".into();
        a.owned = true;
        a.keep = false;
        a.hold = None;
        a.identity = None;
        save_artifact(&db.db.lock().unwrap(), &a).unwrap();
        db.db
            .lock()
            .unwrap()
            .execute(
                "DELETE FROM artifacts WHERE path=?1 AND id<>?2",
                params![path.to_string_lossy(), id],
            )
            .unwrap();
        path
    };
    let empty = allocating("scratch-extract-k1", None, "extract-k1", false);
    let full = allocating("scratch-extract-k2", None, "extract-k2", true);
    let live = allocating("live", Some(77), "Show.job-77-url-abc", true);
    let stranger = allocating("stranger", Some(78), "someone-elses-dir", true);
    db.reconcile_startup_root(&root, &[77, 78]).unwrap();

    assert_eq!(db.get("scratch-extract-k1").unwrap().state, "deleted");
    assert!(!empty.exists());
    let parked = db.get("scratch-extract-k2").unwrap();
    assert_eq!(parked.state, "parked_failed");
    assert!(parked.owned && parked.deadline.is_some());
    assert!(full.join("part.bin").exists());
    let active = db.get("live").unwrap();
    assert_eq!(active.state, "active");
    assert!(active.owned && active.identity.is_some() && active.hold.is_none());
    assert!(live.exists());
    let other = db.get("stranger").unwrap();
    assert!(!other.owned, "not provably Runner's");
    assert!(stranger.join("part.bin").exists());
}

/// Force a relocation's source cleanup into the pre-0.9.1 `blocked` state
/// (no `blocked_at`), with its delivery blocked too.
fn block_cleanup(db: &Inventory, operation: &str, reason: &str) {
    let mut op = db.operation(operation).unwrap();
    let mut movement: serde_json::Value = serde_json::from_str(&op.request).unwrap();
    movement["cleanup"]["state"] = "blocked".into();
    movement["cleanup"]["error"] = reason.into();
    movement["cleanup"]["next_retry"] = 0.into();
    if let Some(object) = movement["cleanup"].as_object_mut() {
        object.remove("blocked_at");
    }
    op.request = serde_json::to_string(&movement).unwrap();
    let db_lock = db.db.lock().unwrap();
    save_operation(&db_lock, &op).unwrap();
    db_lock
        .execute(
            "UPDATE deliveries SET cleanup_status='blocked',cleanup_error=?2 WHERE artifact=?1",
            params![op.artifact, reason],
        )
        .unwrap();
}

fn cleanup_of(db: &Inventory, operation: &str) -> serde_json::Value {
    serde_json::from_str(&db.operation(operation).unwrap().request).unwrap()
}

/// A delivered copy publication of job `job`: (source dir, move op id,
/// delivery id).
fn delivered_copy(
    tmp: &tempfile::TempDir,
    db: &Inventory,
    root: &Path,
    job: u32,
) -> (PathBuf, String, String) {
    let source = root.join(format!("delivered-{job}"));
    db.allocate_for_delivery(job, root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("media.mkv"), b"payload").unwrap();
    db.finish(job, &source, root, "active").unwrap();
    let completed = tmp.path().join("completed");
    std::fs::create_dir_all(&completed).unwrap();
    fs::RENAME_FAILURE.with(|failure| failure.set(Some(libc::EINVAL)));
    let moved = db
        .relocate_for_delivery_checked(job, &completed.join(format!("release-{job}")), &|| Ok(()))
        .unwrap();
    let delivery = db.create_delivery(job, true, Some("t-test")).unwrap();
    (source, moved.operation_id, delivery.delivery_id)
}

/// nuc4, 26 rows: "configured root identity changed or volume is
/// unavailable". The NFS mount source was renamed by a remount
/// (`qnap:/working` → `qnap-storage:/working`); type, root, mount point and
/// inode are unchanged. Identities recorded under the old source rebind,
/// and the cleanup blocked before 0.9.1 is re-verified on its timed exit
/// and completes with no operator action.
#[cfg(target_os = "linux")] // Exercises the Linux mountinfo source-alias contract.
#[test]
fn a_cleanup_blocked_by_a_renamed_mount_source_rebinds_and_completes() {
    let (tmp, db, root) = fixture();
    let (source, operation, delivery) = delivered_copy(&tmp, &db, &root, 80);
    // Record everything as it was under the old mount source. The old
    // identity is rebuilt structurally — only the source field changes —
    // and swapped as a whole mount_id at each JSON nesting level. (A text
    // replace of the `<len>:<source>` field alone also rewrote the
    // filesystem-type field wherever the two read the same — `overlay` on
    // `overlay` on the CI runner — which is a different filesystem, rightly
    // refused.)
    let rebuilt = |live: &str| {
        let (platform, rest) = live.split_once('\n').unwrap();
        let fields = rest
            .split('\n')
            .map(|field| field.split_once(':').unwrap().1.to_owned())
            .collect::<Vec<_>>();
        assert_eq!(fields.len(), 4, "{live:?}");
        let mut old = vec![platform.to_owned()];
        for (index, value) in fields.iter().enumerate() {
            let value = if index == 1 {
                format!("old-alias-{value}")
            } else {
                value.clone()
            };
            old.push(format!("{}:{value}", value.len()));
        }
        old.join("\n")
    };
    let escape = |text: &str| {
        let quoted = serde_json::to_string(text).unwrap();
        quoted[1..quoted.len() - 1].to_owned()
    };
    let mut swaps = Vec::new();
    for dir in [root.clone(), tmp.path().join("completed")] {
        let live = fs::mount_id(&fs::open_dir(&dir).unwrap()).unwrap();
        let old = rebuilt(&live);
        assert!(!live.contains('\\') && !old.contains('\\'));
        let identity = |mount_id: &str| Identity {
            device: 1,
            inode: 2,
            mount_id: mount_id.to_owned(),
            bytes: 0,
            modified: String::new(),
            changed: String::new(),
            directory: true,
        };
        let (recorded, current) = (identity(&old), identity(&live));
        assert!(recorded.same_object(&current), "only the source differs");
        // Nested JSON first (a request inside a row), then one level.
        swaps.push((escape(&escape(&live)), escape(&escape(&old))));
        swaps.push((escape(&live), escape(&old)));
    }
    swaps.sort();
    swaps.dedup();
    for (from, to) in &swaps {
        for table in ["artifacts", "operations"] {
            db.db
                .lock()
                .unwrap()
                .execute(
                    &format!("UPDATE {table} SET data=replace(data,?1,?2)"),
                    params![from, to],
                )
                .unwrap();
        }
        for entry in walk(&tmp.path().join("state/artifact-identities")) {
            let text = std::fs::read_to_string(&entry).unwrap();
            std::fs::write(&entry, text.replace(from.as_str(), to)).unwrap();
        }
    }
    let recorded = cleanup_of(&db, &operation);
    let recorded = recorded["source"]["root_identity"]["mount_id"]
        .as_str()
        .unwrap();
    assert!(recorded.contains("old-alias-"), "{recorded:?}");
    assert_eq!(
        recorded,
        rebuilt(&fs::mount_id(&fs::open_dir(&root).unwrap()).unwrap()),
        "only the source field was rewritten"
    );
    block_cleanup(
        &db,
        &operation,
        "configured root identity changed or volume is unavailable",
    );

    db.exit_wedged_states(0, true).unwrap();
    assert_eq!(cleanup_of(&db, &operation)["cleanup"]["state"], "retry");
    db.retire_delivery_source(&delivery).unwrap();
    assert_eq!(cleanup_of(&db, &operation)["cleanup_done"], true);
    assert!(
        !source.exists(),
        "closed scope removed the delivered source"
    );
}

#[cfg(target_os = "linux")]
fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(walk(&path));
        } else {
            files.push(path);
        }
    }
    files
}

/// Any cause that keeps blocking: after its rounds of timed re-verification
/// the retained source is parked for retention (immediately for a delivered
/// success), retention removes it, and the cleanup closes — no operator.
#[test]
fn a_cleanup_that_stays_blocked_ends_through_retention() {
    let (tmp, db, root) = fixture();
    let (source, operation, delivery) = delivered_copy(&tmp, &db, &root, 81);
    let source_row = format!("source-{operation}");
    let mut row = db.get(&source_row).unwrap();
    row.hold = Some("a hold that never lifts".into());
    save_artifact(&db.db.lock().unwrap(), &row).unwrap();
    block_cleanup(
        &db,
        &operation,
        "source retirement is blocked by ownership/hold state (retained)",
    );
    for _ in 0..super::relocation::BLOCKED_CLEANUP_ROUNDS {
        db.exit_wedged_states(0, true).unwrap();
        // The normal driver retries and is refused again.
        let mut op = db.operation(&operation).unwrap();
        let mut movement: serde_json::Value = serde_json::from_str(&op.request).unwrap();
        movement["cleanup"]["next_retry"] = 0.into();
        op.request = serde_json::to_string(&movement).unwrap();
        save_operation(&db.db.lock().unwrap(), &op).unwrap();
        let _ = db.retire_delivery_source(&delivery);
        assert_eq!(cleanup_of(&db, &operation)["cleanup"]["state"], "blocked");
        let mut op = db.operation(&operation).unwrap();
        let mut movement: serde_json::Value = serde_json::from_str(&op.request).unwrap();
        movement["cleanup"]["blocked_at"] = 0.into();
        op.request = serde_json::to_string(&movement).unwrap();
        save_operation(&db.db.lock().unwrap(), &op).unwrap();
    }
    db.exit_wedged_states(0, true).unwrap();
    let parked = db.get(&source_row).unwrap();
    assert_eq!(parked.state, "parked_failed");
    assert!(parked.hold.is_none());
    assert!(
        parked.deadline.is_some_and(|due| due <= now()),
        "delivered: immediate"
    );
    db.tick().unwrap();
    db.run_due_deletes().unwrap();
    assert!(
        db.get(&source_row).unwrap().terminal(),
        "retention removes a long-id derived row too"
    );
    assert!(!source.exists());
    // Gone: the next sweep closes the cleanup.
    block_cleanup(&db, &operation, "anything");
    db.exit_wedged_states(0, true).unwrap();
    assert_eq!(cleanup_of(&db, &operation)["cleanup_done"], true);
}

/// Rebinding tolerates only a renamed mount source: a different filesystem
/// type, root or mount point is still a different object.
#[test]
fn a_mount_rebind_needs_type_root_and_mount_point_to_match() {
    let id = |kind: &str, source: &str, root: &str, point: &str| Identity {
        device: 1,
        inode: 39714832,
        mount_id: format!(
            "linux\n{}:{kind}\n{}:{source}\n{}:{root}\n{}:{point}",
            kind.len(),
            source.len(),
            root.len(),
            point.len()
        ),
        bytes: 0,
        modified: String::new(),
        changed: String::new(),
        directory: true,
    };
    let recorded = id("nfs4", "qnap:/working", "/", "/working");
    assert!(recorded.same_object(&id("nfs4", "qnap-storage:/working", "/", "/working")));
    assert!(!recorded.same_object(&id("nfs", "qnap-storage:/working", "/", "/working")));
    assert!(!recorded.same_object(&id("nfs4", "qnap-storage:/working", "/sub", "/working")));
    assert!(!recorded.same_object(&id("nfs4", "qnap-storage:/working", "/", "/elsewhere")));
    // A source spelled like its type (`overlay` on `overlay`, a container's
    // root filesystem) parses field by field.
    let overlay = id("overlay", "overlay", "/", "/");
    assert!(overlay.same_object(&id("overlay", "overlay-renamed", "/", "/")));
    assert!(!overlay.same_object(&id("tmpfs", "overlay", "/", "/")));
}

/// nuc4, 0.9.1 attention (44 rows), built from the live shapes: transform
/// workspaces of long-finished jobs failing retirement with
/// `scratch_identity_changed, terminal_job_evidence_missing` (sometimes
/// `source_not_finalized`, `transform_not_terminal`,
/// `source_identity_changed`), or `source_operator_keep, ownership_unproven,
/// …, source_disappearance_is_not_completion`; a cancelled job's `running`
/// extract pinning its source `transitioning`; delivered rows carrying a
/// stale ENOSPC / lost-lease error; an old `transitioning` row with no
/// transform left. Every one ends with no operator action; a kept failure
/// and anything outside Runner's private workspace root are left alone.
#[test]
fn finished_jobs_workspaces_and_stale_rows_end_without_an_operator() {
    let (tmp, db, root) = fixture();
    let terminal = |job: u32, outcome: nzbd_types::TerminalOutcome, keep: bool| {
        let mut terminal = terminals::tests::failed(job, SalvageState::SettledNone);
        terminal.outcome = outcome;
        terminal.keep_on_failure = keep;
        db.record_terminal(&terminal).unwrap();
        db.mark_terminal_step(job, &TerminalStep::Emitted).unwrap();
    };
    // One job directory with a real workspace under the private root.
    let job_with_workspace = |job: u32, kind: &str| {
        let source = root.join(format!("Release.job-{job}-url-abc"));
        db.allocate_for_delivery(job, &root, &source, true, true)
            .unwrap();
        std::fs::write(source.join("media.rar"), b"archive").unwrap();
        let workspace = db.workspace(job, kind, "abc").unwrap();
        std::fs::write(workspace.scratch.path.join("partial.bin"), b"scratch").unwrap();
        workspace
    };
    let set_op = |workspace: &Workspace, state: &str| {
        let mut op = db.operation(&workspace.operation_id).unwrap();
        op.state = state.into();
        if state == "succeeded" {
            // A finished transform records what it published (live shape).
            let mut journal: Workspace = serde_json::from_str(&op.request).unwrap();
            journal.published_files = Some(Vec::new());
            op.request = serde_json::to_string(&journal).unwrap();
        }
        save_operation(&db.db.lock().unwrap(), &op).unwrap();
    };
    let edit = |id: &str, change: &dyn Fn(&mut Artifact)| {
        let mut row = db.get(id).unwrap();
        change(&mut row);
        save_artifact(&db.db.lock().unwrap(), &row).unwrap();
    };
    let retained_scratch = |workspace: &Workspace, drift: bool| {
        edit(&workspace.scratch.id, &|row| {
            row.state = "retained".into();
            if drift {
                // `scratch_identity_changed`: the root moved, inodes changed.
                if let Some(identity) = row.identity.as_mut() {
                    identity.inode += 1;
                }
            }
        });
    };

    // Delivered job, succeeded extract, drifted scratch, no finalization.
    let delivered = job_with_workspace(2835, "extract");
    set_op(&delivered, "succeeded");
    retained_scratch(&delivered, true);
    edit(&delivered.source.id, &|row| {
        row.state = "transferred".into();
        row.owned = false;
    });
    terminal(2835, nzbd_types::TerminalOutcome::Completed, false);
    // Failed par_repair whose source an older startup path marked
    // `source_gone` with Keep and no ownership.
    let orphaned = job_with_workspace(2400, "par_repair");
    set_op(&orphaned, "failed");
    retained_scratch(&orphaned, true);
    edit(&orphaned.source.id, &|row| {
        row.state = "source_gone".into();
        row.keep = true;
        row.owned = false;
    });
    terminal(2400, nzbd_types::TerminalOutcome::Failed, false);
    // Parked failure, succeeded extract.
    let parked = job_with_workspace(2856, "extract");
    set_op(&parked, "succeeded");
    retained_scratch(&parked, false);
    edit(&parked.source.id, &|row| {
        row.state = "parked_failed".into();
        row.deadline = Some(now() + 3600);
        row.retention_seconds = 3600;
    });
    terminal(2856, nzbd_types::TerminalOutcome::Failed, false);
    // Cancelled job whose extract is still `running` (ENOENT) and whose
    // source sits `transitioning`.
    let cancelled = job_with_workspace(2771, "extract");
    assert_eq!(db.for_job(2771).unwrap().unwrap().state, "transitioning");
    terminal(2771, nzbd_types::TerminalOutcome::Cancelled, false);
    // A kept failure keeps its workspace.
    let kept = job_with_workspace(2900, "extract");
    set_op(&kept, "failed");
    retained_scratch(&kept, false);
    terminal(2900, nzbd_types::TerminalOutcome::Failed, true);

    // Delivered rows with a stale ENOSPC / lost-lease error.
    let stale = {
        let path = root.join("delivered.job-2755-url-x");
        db.allocate(2755, &root, &path).unwrap();
        let mut row = db.finish(2755, &path, &root, "active").unwrap();
        row.state = "transferred".into();
        row.owned = false;
        row.error = Some("filesystem: No space left on device (os error 28)".into());
        save_artifact(&db.db.lock().unwrap(), &row).unwrap();
        row
    };
    // An old `transitioning` row of a job long gone, nothing holding it.
    let leftover = {
        let path = root.join("Old.Release-xpost");
        db.allocate(1704, &root, &path).unwrap();
        std::fs::write(path.join("media.mkv"), b"old").unwrap();
        let mut row = db.finish(1704, &path, &root, "active").unwrap();
        row.state = "transitioning".into();
        save_artifact(&db.db.lock().unwrap(), &row).unwrap();
        row
    };

    for _ in 0..3 {
        db.reconcile_workspace_retirements().unwrap();
    }
    for workspace in [&delivered, &orphaned, &parked, &cancelled] {
        let scratch = db.get(&workspace.scratch.id).unwrap();
        assert_eq!(scratch.state, "deleted", "{}", scratch.id);
        assert!(!workspace.scratch.path.exists(), "{}", scratch.id);
    }
    assert_eq!(
        db.operation(&cancelled.operation_id).unwrap().state,
        "failed"
    );
    let cancelled_source = db.get(&cancelled.source.id).unwrap();
    assert_eq!(cancelled_source.state, "parked_failed");
    assert!(cancelled_source.deadline.is_some_and(|due| due > now()));
    assert!(
        cancelled_source.path.join("media.rar").exists(),
        "TTL, not now"
    );
    assert_eq!(db.get(&parked.source.id).unwrap().state, "parked_failed");
    assert!(
        kept.scratch.path.join("partial.bin").exists(),
        "a kept failure's workspace stays"
    );
    assert_eq!(db.get(&kept.scratch.id).unwrap().state, "retained");

    // Restart: stale errors clear on the later evidence; the leftover
    // `transitioning` row is classified like any orphaned allocation.
    db.close().unwrap();
    drop(db);
    let db = Inventory::open(&tmp.path().join("state")).unwrap();
    assert!(db.get(&stale.id).unwrap().error.is_none());
    db.reconcile_startup_root(&root, &[]).unwrap();
    let leftover = db.get(&leftover.id).unwrap();
    assert_eq!(leftover.state, "parked_failed");
    assert!(leftover.owned && leftover.deadline.is_some());

    let attention = db
        .list_page(&ListQuery {
            limit: 200,
            filter: ListFilter::Attention,
            ..Default::default()
        })
        .unwrap();
    let flagged = attention
        .rows
        .iter()
        .map(|row| (row.artifact.id.clone(), row.artifact.error.clone()))
        .collect::<Vec<_>>();
    // The kept failure's workspace is the one row a person may still look at
    // (its retirement reasons show); nothing else remains.
    assert!(
        flagged.iter().all(|(id, _)| id == &kept.scratch.id),
        "{flagged:?}"
    );
}

/// Content outside Runner's private workspace root is never removed by
/// the finished-job discard, even when a transform journal names it.
#[test]
fn a_finished_jobs_workspace_outside_the_private_root_is_not_removed() {
    let (tmp, db, root) = fixture();
    let source = root.join("Release.job-3000-url-abc");
    db.allocate_for_delivery(3000, &root, &source, true, true)
        .unwrap();
    let workspace = db.workspace(3000, "extract", "abc").unwrap();
    let foreign = tmp.path().join("hermes-cinema");
    std::fs::create_dir(&foreign).unwrap();
    std::fs::write(foreign.join("movie.mkv"), b"not ours").unwrap();
    let mut scratch = db.get(&workspace.scratch.id).unwrap();
    scratch.path = foreign.clone();
    scratch.root = tmp.path().to_path_buf();
    save_artifact(&db.db.lock().unwrap(), &scratch).unwrap();
    let mut terminal = terminals::tests::failed(3000, SalvageState::SettledNone);
    terminal.outcome = nzbd_types::TerminalOutcome::Completed;
    db.record_terminal(&terminal).unwrap();
    db.mark_terminal_step(3000, &TerminalStep::Emitted).unwrap();
    db.reconcile_workspace_retirements().unwrap();
    assert!(foreign.join("movie.mkv").exists());
    assert_ne!(db.get(&workspace.scratch.id).unwrap().state, "deleted");
}

/// Shared shape for the finished-job discard guards: a failed job 4000
/// (or `outcome`) with a real workspace under the private root, its
/// transform `op_state`, scratch retained.
fn finished_workspace(
    db: &Inventory,
    root: &Path,
    job: u32,
    outcome: nzbd_types::TerminalOutcome,
    op_state: &str,
) -> Workspace {
    let source = root.join(format!("Release.job-{job}-url-abc"));
    db.allocate_for_delivery(job, root, &source, true, true)
        .unwrap();
    let workspace = db.workspace(job, "extract", "abc").unwrap();
    std::fs::write(workspace.scratch.path.join("out.mkv"), b"output").unwrap();
    let mut op = db.operation(&workspace.operation_id).unwrap();
    op.state = op_state.into();
    save_operation(&db.db.lock().unwrap(), &op).unwrap();
    let mut scratch = db.get(&workspace.scratch.id).unwrap();
    scratch.state = "retained".into();
    save_artifact(&db.db.lock().unwrap(), &scratch).unwrap();
    let mut terminal = terminals::tests::failed(job, SalvageState::SettledNone);
    terminal.outcome = outcome;
    db.record_terminal(&terminal).unwrap();
    db.mark_terminal_step(job, &TerminalStep::Emitted).unwrap();
    workspace
}

/// Review fix 1: a workspace something the job handed on names — the
/// terminal's publication, a delivery row — is never discarded, and a
/// completed job's workspace goes only when its transform failed or
/// published its output.
#[test]
fn a_finished_jobs_workspace_that_was_handed_on_is_kept() {
    use nzbd_types::TerminalOutcome::{Completed, Failed};
    let (_tmp, db, root) = fixture();
    // Completed, transform `succeeded` but no published output recorded.
    let unpublished = finished_workspace(&db, &root, 4001, Completed, "succeeded");
    // A delivery row naming the scratch path.
    let delivered = finished_workspace(&db, &root, 4002, Failed, "failed");
    db.db
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO deliveries(sequence,id,artifact,generation,data) VALUES(999,'d-4002','other','g',?1)",
            [serde_json::json!({ "publicationPath": delivered.scratch.path }).to_string()],
        )
        .unwrap();
    // The terminal's publication lives inside the scratch.
    let published = finished_workspace(&db, &root, 4003, Failed, "failed");
    db.db
        .lock()
        .unwrap()
        .execute(
            "UPDATE job_terminals SET publication=?2 WHERE job=?1",
            params![
                4003,
                serde_json::to_string(&nzbd_types::Publication {
                    path: published
                        .scratch
                        .path
                        .join("out")
                        .to_string_lossy()
                        .into_owned(),
                    generation: "g".into(),
                    manifest_digest: String::new(),
                    files: 1,
                    cross_volume: false,
                })
                .unwrap()
            ],
        )
        .unwrap();
    // Control: a failed job's workspace with nothing naming it goes.
    let garbage = finished_workspace(&db, &root, 4004, Failed, "failed");
    for _ in 0..3 {
        db.reconcile_workspace_retirements().unwrap();
    }
    for kept in [&unpublished, &delivered, &published] {
        assert!(
            kept.scratch.path.join("out.mkv").exists(),
            "{}",
            kept.scratch.id
        );
        assert_ne!(db.get(&kept.scratch.id).unwrap().state, "deleted");
    }
    assert_eq!(db.get(&garbage.scratch.id).unwrap().state, "deleted");
    assert!(!garbage.scratch.path.exists());
}

/// Review fix 2: when the workspace root does not verify (an unmounted or
/// replaced volume), nothing is removed and no row is marked deleted — not
/// even when the directory looks absent.
#[test]
fn a_finished_jobs_workspace_on_an_unverified_root_is_left_alone() {
    let (_tmp, db, root) = fixture();
    let workspace = finished_workspace(
        &db,
        &root,
        4010,
        nzbd_types::TerminalOutcome::Failed,
        "failed",
    );
    let mut scratch = db.get(&workspace.scratch.id).unwrap();
    scratch.root_identity.inode += 1; // not the live root object
    save_artifact(&db.db.lock().unwrap(), &scratch).unwrap();
    db.reconcile_workspace_retirements().unwrap();
    assert!(workspace.scratch.path.join("out.mkv").exists());
    std::fs::remove_dir_all(&workspace.scratch.path).unwrap(); // looks gone
    db.reconcile_workspace_retirements().unwrap();
    assert_ne!(db.get(&workspace.scratch.id).unwrap().state, "deleted");
}

/// Review fix 4: the job is the one the workspace journal names; a source
/// row that names another job (whose terminal is final) does not make the
/// workspace garbage, and the cancelled-source parking follows the
/// journal's job.
#[test]
fn a_workspace_follows_the_job_its_journal_names() {
    let (_tmp, db, root) = fixture();
    let workspace = finished_workspace(
        &db,
        &root,
        4020,
        nzbd_types::TerminalOutcome::Failed,
        "failed",
    );
    // Its own job is still running: drop that terminal.
    db.db
        .lock()
        .unwrap()
        .execute("DELETE FROM job_terminals WHERE job=4020", [])
        .unwrap();
    // The source row names a different, cancelled job.
    let mut parent = db.get(&workspace.source.id).unwrap();
    parent.job = Some(4021);
    save_artifact(&db.db.lock().unwrap(), &parent).unwrap();
    let mut terminal = terminals::tests::failed(4021, SalvageState::SettledNone);
    terminal.outcome = nzbd_types::TerminalOutcome::Cancelled;
    db.record_terminal(&terminal).unwrap();
    db.mark_terminal_step(4021, &TerminalStep::Emitted).unwrap();
    db.reconcile_workspace_retirements().unwrap();
    assert!(workspace.scratch.path.join("out.mkv").exists());
    assert_ne!(db.get(&workspace.scratch.id).unwrap().state, "deleted");
    assert_ne!(db.get(&workspace.source.id).unwrap().state, "parked_failed");
}

/// Review fix 6: an orphan `transitioning` row whose directory no longer
/// verifies is parked with the failed TTL, not given a hold whose exit
/// would release it back into `transitioning`.
#[test]
fn an_unverifiable_leftover_transitioning_row_is_parked() {
    let (_tmp, db, root) = fixture();
    let path = root.join("Old.Release.job-1705-url-x");
    db.allocate(1705, &root, &path).unwrap();
    std::fs::write(path.join("media.mkv"), b"old").unwrap();
    let mut row = db.finish(1705, &path, &root, "active").unwrap();
    row.state = "transitioning".into();
    if let Some(identity) = row.identity.as_mut() {
        identity.inode += 1;
    }
    save_artifact(&db.db.lock().unwrap(), &row).unwrap();
    db.reconcile_startup_root(&root, &[]).unwrap();
    let after = db.get(&row.id).unwrap();
    assert_eq!(after.state, "parked_failed");
    assert!(after.hold.is_none() && after.deadline.is_some());
    assert!(path.join("media.mkv").exists(), "TTL, not now");
}

/// A legacy-identity row (empty `mount_id`, device changed by a remount)
/// in the shape of nuc4's job 1704: `transitioning`, owned, no hold, no
/// error, not cleanup-eligible, no delivery lifecycle, deadline long past,
/// a failed extract its only transform.
fn legacy_row(db: &Inventory, root: &Path, job: u32, name: &str) -> Artifact {
    let path = root.join(name);
    db.allocate(job, root, &path).unwrap();
    std::fs::write(path.join("media.mkv"), b"old").unwrap();
    let mut row = db.finish(job, &path, root, "active").unwrap();
    row.state = "transitioning".into();
    row.owned = true;
    row.hold = None;
    row.error = None;
    row.automatic_cleanup_eligible = false;
    row.delivery_lifecycle = false;
    row.deadline = Some(1791324948);
    let legacy = |identity: &mut Identity| {
        identity.mount_id.clear();
        identity.changed.clear();
        identity.device += 10; // the remount gave the volume a new device
    };
    legacy(&mut row.root_identity);
    if let Some(identity) = row.identity.as_mut() {
        legacy(identity);
    }
    for file in &mut row.files {
        legacy(&mut file.identity);
    }
    save_artifact(&db.db.lock().unwrap(), &row).unwrap();
    save_operation(
        &db.db.lock().unwrap(),
        &Operation {
            id: format!("extract-{}-old", row.id),
            artifact: row.id.clone(),
            kind: "extract".into(),
            state: "failed".into(),
            request: "{}".into(),
            created_at: 0,
            not_before: 0,
            attempts: 1,
            next_retry: 0,
            error: Some("Operator approved discard of abandoned scratch".into()),
        },
    )
    .unwrap();
    row
}

/// A mount table whose only real mount covers `point`, with `point`'s
/// actual device, plus the root filesystem.
fn mount_table(point: &Path) -> String {
    use std::os::unix::fs::MetadataExt;
    let device = std::fs::metadata(point).unwrap().dev();
    #[cfg(target_os = "macos")]
    let device = device as libc::dev_t;
    let canonical = std::fs::canonicalize(point).unwrap();
    format!(
        "1 0 253:0 / / rw - ext4 /dev/root rw\n100 1 {}:{} / {} rw - nfs4 qnap-storage:/working rw\n",
        libc::major(device),
        libc::minor(device),
        canonical.display()
    )
}

/// nuc4 job 1704, with positive evidence the root is the live volume (its
/// device is the current mount's): a legacy row whose path is gone settles
/// `source_gone`, marked `legacy_root_rebind` (gone, not removed); one whose
/// path is present is rebound to the live device and proceeds normally (an
/// orphan: parked with the failed TTL); a root that is a different directory
/// leaves its row alone.
#[test]
fn a_legacy_identity_orphan_rebinds_on_a_live_root() {
    let (_tmp, db, root) = fixture();
    std::fs::write(root.join("someone-else.txt"), b"the root is not empty").unwrap();
    let gone = legacy_row(
        &db,
        &root,
        1704,
        "Heated.Rivalry.S01E05.MULTI.2160p.WEB.H265-BAWLS-xpost",
    );
    std::fs::remove_dir_all(&gone.path).unwrap();
    let present = legacy_row(&db, &root, 1706, "Still.Here-xpost");
    let elsewhere = legacy_row(&db, &root, 1707, "Other.Root-xpost");
    let mut moved = db.get(&elsewhere.id).unwrap();
    moved.root_identity.inode += 1; // the root is a different directory now
    save_artifact(&db.db.lock().unwrap(), &moved).unwrap();

    super::startup::MOUNTINFO.with(|value| *value.borrow_mut() = Some(mount_table(&root)));
    db.reconcile_startup_root(&root, &[]).unwrap();
    super::startup::MOUNTINFO.with(|value| *value.borrow_mut() = None);

    let settled = db.get(&gone.id).unwrap();
    assert_eq!(settled.state, "source_gone");
    assert_eq!(settled.gone_reason.as_deref(), Some("legacy_root_rebind"));
    let rebound = db.get(&present.id).unwrap();
    assert_eq!(rebound.state, "parked_failed", "proceeds like any orphan");
    assert_ne!(rebound.root_identity.device, present.root_identity.device);
    assert!(present.path.join("media.mkv").exists(), "TTL, not now");
    assert_eq!(db.get(&elsewhere.id).unwrap().state, "transitioning");
    let attention = db
        .list_page(&ListQuery {
            limit: 50,
            filter: ListFilter::Attention,
            ..Default::default()
        })
        .unwrap();
    assert!(!attention.rows.iter().any(|row| row.artifact.id == gone.id));
}

/// Without positive evidence nothing settles: a root that only the parent
/// (root) filesystem covers — an unmounted volume's mountpoint — or an
/// empty root directory leaves legacy rows exactly as they were.
#[test]
fn a_legacy_identity_orphan_waits_without_a_live_root() {
    let (tmp, db, root) = fixture();
    std::fs::write(root.join("someone-else.txt"), b"not empty").unwrap();
    let unmounted = legacy_row(&db, &root, 1708, "Unmounted-xpost");
    std::fs::remove_dir_all(&unmounted.path).unwrap();
    // Only the root filesystem covers the root: the volume is not mounted.
    let only_rootfs = {
        use std::os::unix::fs::MetadataExt;
        let device = std::fs::metadata(&root).unwrap().dev();
        #[cfg(target_os = "macos")]
        let device = device as libc::dev_t;
        format!(
            "1 0 {}:{} / / rw - ext4 /dev/root rw\n",
            libc::major(device),
            libc::minor(device)
        )
    };
    super::startup::MOUNTINFO.with(|value| *value.borrow_mut() = Some(only_rootfs));
    db.reconcile_startup_root(&root, &[]).unwrap();
    assert_eq!(db.get(&unmounted.id).unwrap().state, "transitioning");

    // An empty mountpoint, even one a mount line claims.
    let empty_root = tmp.path().join("empty-mountpoint");
    std::fs::create_dir(&empty_root).unwrap();
    std::fs::write(empty_root.join("placeholder"), b"x").unwrap();
    let empty = legacy_row(&db, &empty_root, 1709, "Was.Here-xpost");
    std::fs::remove_dir_all(&empty.path).unwrap();
    std::fs::remove_file(empty_root.join("placeholder")).unwrap();
    super::startup::MOUNTINFO.with(|value| *value.borrow_mut() = Some(mount_table(&empty_root)));
    db.reconcile_startup_root(&empty_root, &[]).unwrap();
    super::startup::MOUNTINFO.with(|value| *value.borrow_mut() = None);
    assert_eq!(db.get(&empty.id).unwrap().state, "transitioning");
}

/// nuc4 job 2710's shape: a failed copy publication parked to the failed
/// root (the move committed; the row stayed `active` because an older
/// build never recorded the disposal), whose parked directory is then
/// removed outside Runner. Returns (original download, parked path).
fn vanished_shape(
    tmp: &tempfile::TempDir,
    db: &Inventory,
    root: &Path,
    job: u32,
) -> (PathBuf, PathBuf) {
    let source = copy_published(tmp, db, root, job);
    fail_terminal(db, job, false);
    let failed = tmp.path().join("failed");
    std::fs::create_dir_all(&failed).unwrap();
    let parked = failed.join(format!(
        "Strangers with Candy (1999)-xpost.job-{job}-url-a4373393948b"
    ));
    db.relocate(job, &parked).unwrap();
    assert_eq!(db.for_job(job).unwrap().unwrap().state, "active");
    (source, parked)
}

/// Move the first-missing stamp back past the confirmation grace.
fn age_missing(db: &Inventory, job: u32) {
    let row = db.for_job(job).unwrap().unwrap();
    let changed = db
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE meta SET value=?2 WHERE key=?1",
            params![
                format!("vanished:{}:{}", row.id, row.generation),
                (now() - super::relocation::VANISHED_CONFIRM_SECONDS - 1).to_string()
            ],
        )
        .unwrap();
    assert_eq!(changed, 1, "the first pass recorded first_missing_at");
}

/// nuc4 job 2710: every disposal re-ran the park against the vanished
/// directory and failed ENOENT; `reconcile_missing` never looks at `active`
/// rows. Absence confirmed on two passes ≥ 10 min apart settles the row
/// `source_gone` (inventory kept; gone, not removed by Runner) and parks the
/// job's original download in place with the failed TTL, in one step.
#[test]
fn a_failed_jobs_vanished_parked_copy_settles_gone_after_two_passes() {
    let (tmp, db, root) = fixture();
    let (source, parked) = vanished_shape(&tmp, &db, &root, 2710);
    assert_eq!(
        db.settle_vanished_failed_source(2710).unwrap(),
        None,
        "present"
    );
    std::fs::remove_dir_all(&parked).unwrap(); // removed outside Runner
    let refused = db.relocate(2710, &parked).unwrap_err();
    assert!(refused.to_string().contains("No such file"), "{refused}");

    assert_eq!(
        db.settle_vanished_failed_source(2710).unwrap(),
        None,
        "first pass only records"
    );
    assert_eq!(
        db.settle_vanished_failed_source(2710).unwrap(),
        None,
        "within the grace"
    );
    age_missing(&db, 2710);
    let note = db.settle_vanished_failed_source(2710).unwrap().unwrap();
    assert!(note.contains("not removed by Runner"), "{note}");
    let gone = db.for_job(2710).unwrap().unwrap();
    assert_eq!(gone.state, "source_gone");
    assert_eq!(
        gone.gone_reason.as_deref(),
        Some("missing from its verified root")
    );
    assert!(!gone.files.is_empty(), "the inventory is kept");
    let remainder = db
        .for_path(&source)
        .unwrap()
        .filter(|row| row.id.starts_with("source-"))
        .unwrap();
    assert_eq!(remainder.state, "parked_failed");
    assert!(remainder.deadline.is_some_and(|due| due > now()));
    assert!(source.join("media.rar").exists(), "TTL, not now");
    // Idempotent: a repeat (the disposal record lost to a crash) reports
    // the same settlement and changes nothing.
    let again = db.settle_vanished_failed_source(2710).unwrap().unwrap();
    assert!(!again.contains("original download parked"), "{again}");
    assert_eq!(
        db.get(&remainder.id).unwrap().revision,
        remainder.revision,
        "nothing re-parked"
    );
}

/// A transient ENOENT (the directory comes back) clears the stamp; it never
/// settles anything.
#[test]
fn a_transient_absence_recovers_without_settling() {
    let (tmp, db, root) = fixture();
    let (_source, parked) = vanished_shape(&tmp, &db, &root, 2712);
    let moved_aside = parked.with_extension("aside");
    std::fs::rename(&parked, &moved_aside).unwrap();
    assert_eq!(db.settle_vanished_failed_source(2712).unwrap(), None);
    std::fs::rename(&moved_aside, &parked).unwrap(); // back
    assert_eq!(db.settle_vanished_failed_source(2712).unwrap(), None);
    let row = db.for_job(2712).unwrap().unwrap();
    let stamp: Option<String> = db
        .db
        .lock()
        .unwrap()
        .query_row(
            "SELECT value FROM meta WHERE key=?1",
            [format!("vanished:{}:{}", row.id, row.generation)],
            |r| r.get(0),
        )
        .optional()
        .unwrap();
    assert!(stamp.is_none(), "a reappearance clears first_missing_at");
    assert_eq!(row.state, "active");
}

/// Any queued or running operation on the artifact, of any kind, blocks it.
#[test]
fn a_vanished_source_with_an_operation_in_flight_waits() {
    let (tmp, db, root) = fixture();
    let (_source, parked) = vanished_shape(&tmp, &db, &root, 2713);
    std::fs::remove_dir_all(&parked).unwrap();
    assert_eq!(db.settle_vanished_failed_source(2713).unwrap(), None);
    age_missing(&db, 2713);
    let row = db.for_job(2713).unwrap().unwrap();
    save_operation(
        &db.db.lock().unwrap(),
        &Operation {
            id: "inspect-in-flight".into(),
            artifact: row.id.clone(),
            kind: "inspect".into(),
            state: "queued".into(),
            request: "{}".into(),
            created_at: now(),
            not_before: 0,
            attempts: 0,
            next_retry: 0,
            error: None,
        },
    )
    .unwrap();
    assert_eq!(db.settle_vanished_failed_source(2713).unwrap(), None);
    assert_eq!(db.for_job(2713).unwrap().unwrap().state, "active");
}

/// A crash after the row was settled but before its original download was
/// parked resumes: the next pass parks it and reports the settlement.
#[test]
fn a_vanished_settlement_interrupted_before_parking_resumes() {
    let (tmp, db, root) = fixture();
    let (source, parked) = vanished_shape(&tmp, &db, &root, 2714);
    std::fs::remove_dir_all(&parked).unwrap();
    let mut row = db.for_job(2714).unwrap().unwrap();
    row.state = "source_gone".into();
    row.gone_reason = Some("missing from its verified root".into());
    row.updated_at = now();
    save_artifact(&db.db.lock().unwrap(), &row).unwrap();
    let note = db.settle_vanished_failed_source(2714).unwrap().unwrap();
    assert!(note.contains("original download parked"), "{note}");
    let remainder = db
        .for_path(&source)
        .unwrap()
        .filter(|row| row.id.starts_with("source-"))
        .unwrap();
    assert_eq!(remainder.state, "parked_failed");
}

/// The settle needs a durable failure: a job without a terminal is left to
/// its own lifecycle even when its directory is gone.
#[test]
fn a_vanished_source_of_a_live_job_is_not_settled() {
    let (tmp, db, root) = fixture();
    let _source = copy_published(&tmp, &db, &root, 2711);
    let published = db.for_job(2711).unwrap().unwrap();
    std::fs::remove_dir_all(&published.path).unwrap();
    assert_eq!(db.settle_vanished_failed_source(2711).unwrap(), None);
    assert_eq!(db.for_job(2711).unwrap().unwrap().state, "active");
}

#[test]
fn salvage_retry_is_durable_and_pending_never_grants_disposal() {
    let (tmp, db, root) = fixture();
    let path = root.join("salvage");
    db.allocate(801, &root, &path).unwrap();
    let terminal = terminals::tests::failed(801, SalvageState::Pending);
    db.record_terminal(&terminal).unwrap();
    db.mark_terminal_step(801, &TerminalStep::Emitted).unwrap();
    assert!(db.begin_salvage_attempt(801).unwrap());
    db.note_salvage_error(801, "capacity: extractor volume full")
        .unwrap();
    assert!(!db.salvage_retry_due(801).unwrap());
    assert!(!db.begin_salvage_attempt(801).unwrap());
    assert!(db.disposal_allowed(801).unwrap().is_none());
    let op = vec![db.operation("salvage:801").unwrap()];
    assert!(op
        .iter()
        .any(|op| op.kind == "salvage" && op.attempts == 1 && op.next_retry > now()));
    drop(db);
    let db = Inventory::open(&tmp.path().join("state")).unwrap();
    assert!(
        !db.begin_salvage_attempt(801).unwrap(),
        "restart respects clock"
    );
    db.db
        .lock()
        .unwrap()
        .execute(
            "UPDATE operations SET data=json_set(data,'$.next_retry',0) WHERE id='salvage:801'",
            [],
        )
        .unwrap();
    assert!(db.begin_salvage_attempt(801).unwrap());
    let before = db.terminal(801).unwrap().unwrap();
    db.settle_salvage(801, &SalvageOutcome::None { error: None })
        .unwrap();
    assert!(!db.begin_salvage_attempt(801).unwrap());
    let after = db.terminal(801).unwrap().unwrap();
    assert_eq!(before.reason, after.reason);
    assert_eq!(before.entry.status, after.entry.status);
    assert!(path.exists(), "no deletion or custody transfer");
}

#[test]
fn transferred_inventory_requires_two_verified_absences_and_keeps_audit() {
    let (_tmp, db, root) = fixture();
    let path = root.join("transferred");
    db.allocate_for_delivery(802, &root, &path, true, true)
        .unwrap();
    std::fs::write(path.join("movie.mkv"), b"movie").unwrap();
    db.finish(802, &path, &root, "completed").unwrap();
    let delivery = db.create_delivery(802, true, None).unwrap();
    let original = db.get(&delivery.artifact_id).unwrap();
    db.reconcile_missing().unwrap();
    assert_eq!(
        db.get(&original.id).unwrap().state,
        "transferred",
        "present remains live"
    );
    std::fs::remove_dir_all(&path).unwrap();
    db.reconcile_missing().unwrap(); // cursor wrap
    db.reconcile_missing().unwrap();
    assert_eq!(
        db.get(&original.id).unwrap().state,
        "transferred",
        "first absence is not disposition"
    );
    let key = format!(
        "transferred-missing:{}:{}:{}",
        original.id, original.generation, original.revision
    );
    db.db
        .lock()
        .unwrap()
        .execute("UPDATE meta SET value='0' WHERE key=?1", [&key])
        .unwrap();
    db.reconcile_missing().unwrap();
    db.reconcile_missing().unwrap();
    let gone = db.get(&original.id).unwrap();
    assert_eq!(gone.state, "source_gone");
    assert_eq!(gone.files, original.files);
    assert_eq!(
        serde_json::to_value(db.delivery(&delivery.delivery_id).unwrap()).unwrap(),
        serde_json::to_value(delivery).unwrap()
    );
    assert_eq!(db.list_page(&ListQuery::default()).unwrap().counts.live, 0);
    assert_eq!(
        db.list_page(&ListQuery {
            filter: ListFilter::All,
            ..Default::default()
        })
        .unwrap()
        .counts
        .all,
        1
    );
}

#[test]
fn transferred_inventory_preserves_reappearing_unverified_and_busy_custody() {
    let (tmp, db, root) = fixture();
    let path = root.join("live-transfer");
    db.allocate_for_delivery(803, &root, &path, true, true)
        .unwrap();
    std::fs::write(path.join("media.mkv"), b"payload").unwrap();
    db.finish(803, &path, &root, "completed").unwrap();
    let delivery = db.create_delivery(803, true, None).unwrap();
    let original = db.get(&delivery.artifact_id).unwrap();
    let aside = tmp.path().join("aside");
    std::fs::rename(&path, &aside).unwrap();
    db.reconcile_missing().unwrap();
    let key = format!(
        "transferred-missing:{}:{}:{}",
        original.id, original.generation, original.revision
    );
    db.db
        .lock()
        .unwrap()
        .execute("UPDATE meta SET value='0' WHERE key=?1", [&key])
        .unwrap();
    std::fs::rename(&aside, &path).unwrap();
    db.reconcile_missing().unwrap();
    db.reconcile_missing().unwrap();
    assert_eq!(db.get(&original.id).unwrap().state, "transferred");
    let stamp: bool = db
        .db
        .lock()
        .unwrap()
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM meta WHERE key=?1)",
            [&key],
            |r| r.get(0),
        )
        .unwrap();
    assert!(!stamp, "reappearance invalidates absence");
    std::fs::rename(&root, tmp.path().join("old-root")).unwrap();
    std::fs::create_dir(&root).unwrap();
    db.reconcile_missing().unwrap();
    db.reconcile_missing().unwrap();
    assert_eq!(
        db.get(&original.id).unwrap().state,
        "transferred",
        "a remounted/replaced root is not absence proof"
    );
    assert_eq!(db.get(&original.id).unwrap().files, original.files);
    assert!(tmp.path().join("old-root/live-transfer/media.mkv").exists());
}

#[test]
fn salvage_retry_rejects_changed_generation_and_keeps_the_old_error_audit() {
    let (_tmp, db, root) = fixture();
    let path = root.join("salvage-replacement");
    db.allocate(804, &root, &path).unwrap();
    db.record_terminal(&terminals::tests::failed(804, SalvageState::Pending))
        .unwrap();
    db.mark_terminal_step(804, &TerminalStep::Emitted).unwrap();
    assert!(db.begin_salvage_attempt(804).unwrap());
    db.note_salvage_error(804, "capacity exhausted").unwrap();
    let mut source = db.for_job(804).unwrap().unwrap();
    source.generation = "replacement".into();
    save_artifact(&db.db.lock().unwrap(), &source).unwrap();
    db.db
        .lock()
        .unwrap()
        .execute(
            "UPDATE operations SET data=json_set(data,'$.next_retry',0) WHERE id='salvage:804'",
            [],
        )
        .unwrap();
    assert!(db.begin_salvage_attempt(804).is_err());
    assert_eq!(
        db.operation("salvage:804").unwrap().error.as_deref(),
        Some("capacity exhausted")
    );
    assert!(db.disposal_allowed(804).unwrap().is_none());
}

#[test]
fn expired_worker_parking_preserves_unknown_bytes_keep_and_legacy_enrollment() {
    for (owned, enrolled, keep) in [
        (true, true, false),
        (true, true, true),
        (true, false, false),
        (false, false, false),
    ] {
        let (_tmp, db, root) = fixture();
        let path = root.join("stopped");
        let mut artifact = if owned {
            db.allocate_for_delivery(905, &root, &path, true, enrolled)
                .unwrap()
        } else {
            std::fs::create_dir(&path).unwrap();
            db.register_legacy_active(905, &root, &path).unwrap();
            db.for_job(905).unwrap().unwrap()
        };
        std::fs::write(path.join("partial.rar"), b"uncertain recovery input").unwrap();
        artifact.keep = keep;
        artifact.automatic_cleanup_eligible = enrolled;
        save_artifact(&db.db.lock().unwrap(), &artifact).unwrap();
        let context = WorkerControlContext {
            lease_id: "exact-grant".into(),
            job_incarnation: "905".into(),
            job_revision: 1,
            control_revision: 1,
            epoch: 1,
        };
        let request = WorkerStopRequest {
            job: 905,
            context,
            owner_incarnation: "worker-boot".into(),
            storage_instance_id: db.installation.clone(),
            removal_revision: None,
            authority_source_json: None,
            start_ordered: true,
        };
        db.begin_worker_grant(&request).unwrap();
        let ticket = db.prepare_worker_stop(&request).unwrap();
        let receipt = db
            .commit_worker_stopped(
                &ticket,
                &[SourceGeneration {
                    artifact: artifact.id.clone(),
                    generation: artifact.generation.clone(),
                }],
            )
            .unwrap();
        db.park_expired_worker_sources(&receipt).unwrap();
        let parked = db.get(&artifact.id).unwrap();
        assert_eq!(parked.owned, owned);
        assert_eq!(parked.automatic_cleanup_eligible, enrolled);
        assert_eq!(parked.keep, keep);
        assert_eq!(
            parked.failure_assessment.as_ref().unwrap().disposition,
            "unknown"
        );
        assert_eq!(parked.failure_reason.as_deref(), Some("hold_expired"));
        assert!(parked
            .error
            .as_deref()
            .unwrap()
            .contains("not proven unusable"));
        if owned && enrolled {
            assert_eq!(parked.state, "parked_failed");
            assert_eq!(parked.deadline.is_some(), !keep);
        } else {
            assert_eq!(parked.state, artifact.state);
            assert_eq!(parked.deadline, artifact.deadline);
        }
        assert_eq!(
            std::fs::read(path.join("partial.rar")).unwrap(),
            b"uncertain recovery input"
        );
        let mut wrong = receipt.clone();
        wrong.sources[0].generation = "replacement".into();
        assert!(db.park_expired_worker_sources(&wrong).is_err());
    }
}

#[test]
fn expiry_checkpoint_and_outbox_are_atomic_and_empty_reboot_stop_is_rejected() {
    let (tmp, db, root) = fixture();
    let path = root.join("expiry");
    let source = db
        .allocate_for_delivery(906, &root, &path, true, true)
        .unwrap();
    std::fs::write(path.join("payload.part"), b"unknown bytes").unwrap();
    let context = WorkerControlContext {
        lease_id: "work/job/906/post@1".into(),
        job_incarnation: "906".into(),
        job_revision: 1,
        control_revision: 1,
        epoch: 1,
    };
    let mut job = nzbd_types::Job {
        id: nzbd_types::JobId(906),
        kind: nzbd_types::JobKind::Nzb,
        name: "expiry".into(),
        dir_name: "expiry".into(),
        name_provisional: false,
        delivery_lifecycle: true,
        queued_at_unix: 0,
        original_name: String::new(),
        category: None,
        priority: 0,
        dupe: Default::default(),
        params: vec![(
            WORKER_CONTROL_CONTEXT_PARAM.into(),
            serde_json::to_string(&context).unwrap(),
        )],
        files: vec![],
        totals: Default::default(),
        status: nzbd_types::JobStatus::Held,
        torrent: None,
        stages: vec![],
    };
    let mut control:nzbd_types::JobControl=serde_json::from_value(serde_json::json!({"version":1,"revision":"3","lifecycle":"held","cause":"capacity","stage":"unpack","retry_policy":"scheduled_retry","message":"full","instance":"episode","deadline_unix":0})).unwrap();
    job.set_control(&control);
    db.bind_worker_context(906, &context).unwrap();
    db.checkpoint_worker_control(&job, "0").unwrap();
    let stop = WorkerStopRequest {
        job: 906,
        context: context.clone(),
        owner_incarnation: "worker-boot".into(),
        storage_instance_id: db.installation.clone(),
        removal_revision: None,
        authority_source_json: None,
        start_ordered: true,
    };
    db.begin_worker_grant(&stop).unwrap();
    control.revision = "4".into();
    control.expiry_pending = true;
    job.set_control(&control);
    let notice = serde_json::json!({"node":"worker","job":906,"context":context,"token":{"resource":"work/job/906/post","owner_node_id":"worker","owner_incarnation":"worker-boot","fence":1,"revision":1,"expires_at_unix_ms":100},"control":control,"source":{"artifact":source.id,"generation":source.generation},"fetched":{"success_articles":0,"total_articles":0,"bytes":0},"stop_request":stop,"stopped":null});
    let mut bad = notice.clone();
    bad["source"]["generation"] = "replacement".into();
    assert!(db
        .checkpoint_worker_hold_expiry(&job, "3", &bad.to_string())
        .is_err());
    assert!(
        !db.worker_control_metadata(906, &context)
            .unwrap()
            .unwrap()
            .control
            .expiry_pending
    );
    assert!(db.worker_expiry_outbox("").unwrap().is_empty());
    db.checkpoint_worker_hold_expiry(&job, "3", &notice.to_string())
        .unwrap();
    assert!(
        db.worker_control_metadata(906, &context)
            .unwrap()
            .unwrap()
            .control
            .expiry_pending
    );
    assert_eq!(db.worker_expiry_outbox("").unwrap().len(), 1);
    drop(db);
    let db = Inventory::open(&tmp.path().join("state")).unwrap();
    let ticket = db.prepare_worker_stop(&stop).unwrap();
    assert!(ticket.recovered_session);
    assert!(
        db.commit_worker_stopped(&ticket, &[]).is_err(),
        "reboot is not proof of the selected source's writer/process join"
    );
    assert!(
        db.prepare_worker_stop(&stop).unwrap().receipt.is_none(),
        "no irreversible empty receipt committed"
    );
    let source_ref = SourceGeneration {
        artifact: source.id.clone(),
        generation: source.generation.clone(),
    };
    db.note_worker_expiry_error(
        &source_ref,
        "exact joined source proof unavailable after restart",
    )
    .unwrap();
    assert_eq!(
        db.get(&source.id).unwrap().salvage.as_deref(),
        Some("pending")
    );
    assert_eq!(db.worker_expiry_outbox("").unwrap().len(), 1);
    // Once the actual actor join supplies the exact source, the receipt can
    // advance without deleting or replacing the unresolved audit.
    let receipt = db.commit_worker_stopped(&ticket, &[source_ref]).unwrap();
    db.park_expired_worker_sources(&receipt).unwrap();
    let before = db.get(&source.id).unwrap();
    db.park_expired_worker_sources(&receipt).unwrap();
    assert_eq!(db.get(&source.id).unwrap().revision, before.revision);
    let deadline = before.deadline;
    db.note_worker_expiry_error(&receipt.sources[0], "second RPC reply lost")
        .unwrap();
    assert_eq!(
        db.get(&source.id).unwrap().salvage.as_deref(),
        Some("settled_none")
    );
    db.park_expired_worker_sources(&receipt).unwrap();
    let acknowledged = db.get(&source.id).unwrap();
    assert_eq!(acknowledged.deadline, deadline);
    assert_eq!(acknowledged.salvage.as_deref(), Some("settled_none"));
    assert!(!acknowledged.error.unwrap().contains("pending"));

    let (key, raw) = db.worker_expiry_outbox("").unwrap().pop().unwrap();
    db.finish_worker_expiry_outbox(&key, "joined; sources preserved")
        .unwrap();
    assert!(db.worker_expiry_outbox("").unwrap().is_empty());
    let saved: String = db
        .db
        .lock()
        .unwrap()
        .query_row(
            "SELECT value FROM meta WHERE key=?1",
            [key.replacen("expiry-outbox:", "expiry-audit:", 1)],
            |row| row.get(0),
        )
        .unwrap();
    assert!(saved.contains("joined; sources preserved"));
    assert!(saved.contains(
        &serde_json::from_str::<serde_json::Value>(&raw).unwrap()["source"]["generation"]
            .as_str()
            .unwrap()
            .to_string()
    ));
    assert!(path.join("payload.part").exists());
}

#[test]
fn hold_expired_blocks_replay_of_old_failed_delete_without_deleting_bytes() {
    let (_tmp, db, root) = fixture();
    let path = root.join("expired-delete");
    let mut source = db
        .allocate_for_delivery(908, &root, &path, true, true)
        .unwrap();
    std::fs::write(path.join("payload.bin"), b"uncertain bytes").unwrap();
    source.failure_reason = Some("hold_expired".into());
    save_artifact(&db.db.lock().unwrap(), &source).unwrap();
    let op = Operation {
        id: "failed-delete-908".into(),
        artifact: source.id.clone(),
        kind: "delete".into(),
        state: "queued".into(),
        request: serde_json::to_string(&DeleteRequest::Scoped((
            source.id.clone(),
            source.revision,
            0,
            true,
            source.generation.clone(),
        )))
        .unwrap(),
        created_at: now(),
        not_before: 0,
        attempts: 0,
        next_retry: 0,
        error: None,
    };
    save_operation(&db.db.lock().unwrap(), &op).unwrap();
    let cancelled = db.execute_delete(&op.id).unwrap();
    assert_eq!(cancelled.state, "cancelled");
    assert!(cancelled.error.unwrap().contains("hold-expired"));
    assert_eq!(
        std::fs::read(path.join("payload.bin")).unwrap(),
        b"uncertain bytes"
    );
}

#[test]
fn hold_expired_curator_failure_keeps_failed_ttl_through_rescan_and_tick() {
    let (_tmp, db, root) = fixture();
    let path = root.join("expired-ttl");
    db.allocate_for_delivery(909, &root, &path, true, true)
        .unwrap();
    std::fs::write(path.join("uncertain.rar"), b"still recoverable").unwrap();
    let mut source = db.finish(909, &path, &root, "parked_failed").unwrap();
    let future = now() + 86400;
    source.deadline = Some(future);
    source.failure_reason = Some("hold_expired".into());
    source.failure_assessment = Some(FailureAssessment {
        disposition: "unknown".into(),
        reason: "deadline-abandoned assessment".into(),
        assessed_at: now(),
    });
    save_artifact(&db.db.lock().unwrap(), &source).unwrap();
    let mut terminal = terminals::tests::failed(909, SalvageState::SettledNone);
    terminal.reason = Some(nzbd_types::FailureReason::HoldExpired);
    terminal.transfer = Some("curator-admitted".into());
    terminal.salvage_error =
        Some("deadline-abandoned assessment; bytes not proven unusable".into());
    db.record_terminal(&terminal).unwrap();
    db.mark_terminal_step(909, &TerminalStep::Emitted).unwrap();
    assert_eq!(db.expire_parked_curator_failures().unwrap(), 0);
    assert_eq!(db.get(&source.id).unwrap().deadline, Some(future));
    db.tick().unwrap();
    assert_eq!(db.get(&source.id).unwrap().deadline, Some(future));
    assert!(path.join("uncertain.rar").exists());
    assert!(db
        .terminal(909)
        .unwrap()
        .unwrap()
        .salvage_error
        .unwrap()
        .contains("not proven unusable"));
}
