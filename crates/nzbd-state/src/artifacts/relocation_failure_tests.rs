use super::relocation::FAULTS;
use super::*;
fn fixture() -> (tempfile::TempDir, Inventory, PathBuf, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("downloads");
    let ordinary = temp.path().join("ordinary");
    std::fs::create_dir(&source).unwrap();
    std::fs::create_dir(&ordinary).unwrap();
    let inventory = Inventory::open(&temp.path().join("state")).unwrap();
    let path = source.join("job");
    inventory.allocate(91, &source, &path).unwrap();
    std::fs::write(path.join("media.mkv"), b"PAR_FAILURE retained input").unwrap();
    (temp, inventory, path, ordinary.join("job"))
}
#[test]
fn unsupported_publication_copies_zero_payload_and_repeated_review_is_one_claim() {
    for code in [libc::EINVAL, libc::ENOSYS, libc::EOPNOTSUPP] {
        let (temp, inventory, source, target) = fixture();
        FAULTS.with(|f| f.borrow_mut().insert("before_move", code));
        assert!(inventory.relocate(91, &target).is_err());
        assert!(source.join("media.mkv").exists());
        assert!(!target.exists());
        for _ in 0..3 {
            assert!(inventory.relocate(91, &target).is_err());
        }
        let count: i64 = inventory
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT count(*) FROM operations WHERE json_extract(data,'$.kind')='relocate'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
        drop(inventory);
        let reopened = Inventory::open(&temp.path().join("state")).unwrap();
        reopened.reconcile_startup(&[91]).unwrap();
        assert!(reopened.relocate(91, &target).is_err());
        assert!(source.exists());
        assert_eq!(
            std::fs::read_dir(target.parent().unwrap()).unwrap().count(),
            0
        );
    }
}
#[test]
fn unsupported_flagged_rename_publishes_without_a_registry_or_feature_gate() {
    let (_temp, inventory, source, target) = fixture();
    fs::RENAME_FAILURE.with(|f| f.set(Some(libc::EINVAL)));
    let result = inventory.relocate(91, &target).unwrap();
    assert_eq!(result.published_path, target);
    assert_eq!(
        std::fs::read(target.join("media.mkv")).unwrap(),
        b"PAR_FAILURE retained input"
    );
    assert!(!source.exists(), "verified duplicate source is retired");
    let repeated = inventory.relocate(91, &target).unwrap();
    assert_eq!(result.operation_id, repeated.operation_id);
    assert_eq!(result.generation, repeated.generation);
}
#[test]
fn lost_commit_acknowledgement_reconciles_same_publication() {
    let (_temp, inventory, _source, target) = fixture();
    FAULTS.with(|f| f.borrow_mut().insert("registry_commit", libc::EIO));
    assert!(inventory.relocate(91, &target).is_err());
    assert!(target.join("media.mkv").exists());
    inventory.reconcile_relocations().unwrap();
    assert_eq!(
        inventory.relocate(91, &target).unwrap().published_path,
        target
    );
}

#[test]
fn deleted_move_source_stays_deleted_when_its_path_is_reused() {
    let (temp, inventory, source, target) = fixture();
    // Keep the old inode allocated so reuse of the pathname cannot disguise
    // the generation change on filesystems that recycle inodes immediately.
    let _old_directory = File::open(&source).unwrap();
    FAULTS.with(|f| f.borrow_mut().insert("before_move", libc::EINVAL));
    assert!(inventory.relocate(91, &target).is_err());
    let old = inventory
        .finish(91, &source, source.parent().unwrap(), "retained")
        .unwrap();
    let deletion = inventory
        .request_delete(&old.id, old.revision, "delete-failed-source", 0)
        .unwrap();
    assert_eq!(
        inventory.execute_delete(&deletion.id).unwrap().state,
        "succeeded"
    );
    let replacement = inventory
        .allocate(92, source.parent().unwrap(), &source)
        .unwrap();
    std::fs::write(source.join("replacement.mkv"), b"new generation").unwrap();
    drop(inventory);

    let reopened = Inventory::open(&temp.path().join("state")).unwrap();
    reopened.reconcile_startup(&[92]).unwrap();
    assert_eq!(reopened.get(&old.id).unwrap().state, "deleted");
    assert_eq!(
        serde_json::to_value(reopened.get(&replacement.id).unwrap()).unwrap(),
        serde_json::to_value(&replacement).unwrap()
    );
    assert_eq!(
        std::fs::read(source.join("replacement.mkv")).unwrap(),
        b"new generation"
    );
    assert!(reopened
        .release_review(&old.id, reopened.get(&old.id).unwrap().revision)
        .is_err());
}

#[test]
fn startup_restores_a_deletion_over_a_legacy_resurrected_move_source() {
    let (temp, inventory, source, target) = fixture();
    FAULTS.with(|f| f.borrow_mut().insert("before_move", libc::EINVAL));
    assert!(inventory.relocate(91, &target).is_err());
    let old = inventory
        .finish(91, &source, source.parent().unwrap(), "retained")
        .unwrap();
    let deletion = inventory
        .request_delete(&old.id, old.revision, "legacy-deletion", 0)
        .unwrap();
    let stale_move: Operation = {
        let db = inventory.db.lock().unwrap();
        let raw: String = db.query_row(
            "SELECT data FROM operations WHERE artifact=?1 AND json_extract(data,'$.kind')='relocate'",
            [&old.id], |r| r.get(0),
        ).unwrap();
        serde_json::from_str(&raw).unwrap()
    };
    inventory.execute_delete(&deletion.id).unwrap();
    // Reproduce a real legacy journal, including independently ordered dates.
    let mut legacy = inventory.operation(&deletion.id).unwrap();
    legacy.request = serde_json::to_string(&(&old.id, old.revision, 0u64, false)).unwrap();
    legacy.created_at = stale_move.created_at + 1;
    save_operation(&inventory.db.lock().unwrap(), &legacy).unwrap();
    // Persist exactly the inconsistent state produced by the old reconciler.
    let mut zombie = inventory.get(&old.id).unwrap();
    zombie.state = "retained".into();
    zombie.hold = Some("review: interrupted move".into());
    zombie.revision += 1;
    save_artifact(&inventory.db.lock().unwrap(), &zombie).unwrap();
    save_operation(&inventory.db.lock().unwrap(), &stale_move).unwrap();
    let replacement = inventory
        .allocate(92, source.parent().unwrap(), &source)
        .unwrap();
    std::fs::write(source.join("replacement.mkv"), b"keep me").unwrap();
    drop(inventory);

    let reopened = Inventory::open(&temp.path().join("state")).unwrap();
    reopened.reconcile_startup(&[92]).unwrap();
    let restored = reopened.get(&old.id).unwrap();
    assert_eq!(restored.state, "deleted");
    assert!(restored.hold.is_none());
    assert_eq!(
        reopened.operation(&stale_move.id).unwrap().state,
        "cancelled"
    );
    assert_eq!(
        serde_json::to_value(reopened.get(&replacement.id).unwrap()).unwrap(),
        serde_json::to_value(&replacement).unwrap()
    );
    reopened.reconcile_startup(&[92]).unwrap();
    assert_eq!(
        serde_json::to_value(reopened.get(&old.id).unwrap()).unwrap(),
        serde_json::to_value(restored).unwrap(),
        "recovery is idempotent"
    );
    assert_eq!(
        std::fs::read(source.join("replacement.mkv")).unwrap(),
        b"keep me"
    );
}

#[test]
fn relocation_cannot_commit_after_source_authority_has_ended() {
    for state in ["deleted", "source_gone", "deleting", "delete_failed"] {
        let (_temp, inventory, _source, target) = fixture();
        FAULTS.with(|f| f.borrow_mut().insert("registry_commit", libc::EIO));
        assert!(inventory.relocate(91, &target).is_err());
        // Publication is valid, so only the source lifecycle check prevents
        // the old move from winning over a newer terminal/deletion transition.
        assert!(target.join("media.mkv").exists());
        let mut current = inventory.for_job(91).unwrap().unwrap();
        current.state = state.into();
        current.revision += 1;
        save_artifact(&inventory.db.lock().unwrap(), &current).unwrap();
        inventory.reconcile_relocations().unwrap();
        assert_eq!(
            serde_json::to_value(inventory.get(&current.id).unwrap()).unwrap(),
            serde_json::to_value(&current).unwrap(),
        );
        assert_eq!(
            std::fs::read(target.join("media.mkv")).unwrap(),
            b"PAR_FAILURE retained input"
        );
    }
}

#[test]
fn pending_deletion_survives_relocation_reconciliation_without_losing_authorization() {
    for cancel in [false, true] {
        let (_temp, inventory, source, target) = fixture();
        FAULTS.with(|f| f.borrow_mut().insert("before_move", libc::EINVAL));
        assert!(inventory.relocate(91, &target).is_err());
        let old = inventory
            .finish(91, &source, source.parent().unwrap(), "retained")
            .unwrap();
        let deletion = inventory
            .request_delete(&old.id, old.revision, "pending-delete", 0)
            .unwrap();
        inventory.reconcile_relocations().unwrap();
        assert_eq!(inventory.get(&old.id).unwrap().revision, old.revision);
        if cancel {
            inventory.cancel_delete(&deletion.id).unwrap();
            inventory.reconcile_relocations().unwrap();
            assert_eq!(
                inventory.get(&old.id).unwrap().hold.as_deref(),
                Some("review: interrupted move")
            );
            assert!(source.join("media.mkv").exists());
        } else {
            assert_eq!(
                inventory.execute_delete(&deletion.id).unwrap().state,
                "succeeded"
            );
            inventory.reconcile_relocations().unwrap();
            assert_eq!(inventory.get(&old.id).unwrap().state, "deleted");
        }
    }
}

#[test]
fn successful_delete_never_tombstones_a_recreated_artifact_id() {
    let (_temp, inventory, source, _target) = fixture();
    let old = inventory
        .finish(91, &source, source.parent().unwrap(), "retained")
        .unwrap();
    let op = inventory
        .request_delete(&old.id, old.revision, "old-generation", 0)
        .unwrap();
    inventory.execute_delete(&op.id).unwrap();
    let mut next = inventory
        .allocate(92, source.parent().unwrap(), &source)
        .unwrap();
    // Workspace/scratch IDs can be deterministic. Simulate their legitimate
    // recreation with a fresh generation, without rewriting the old journal.
    next.id = old.id.clone();
    next.job = None;
    next.state = "retained".into();
    inventory.sidecar(&next).unwrap();
    save_artifact(&inventory.db.lock().unwrap(), &next).unwrap();
    inventory.reconcile_deleted_artifacts().unwrap();
    assert_eq!(
        serde_json::to_value(inventory.get(&next.id).unwrap()).unwrap(),
        serde_json::to_value(&next).unwrap()
    );
    assert!(source.exists());
}

#[test]
fn pending_delete_cannot_cross_generations_and_legacy_success_cannot_repair_them() {
    let (_temp, inventory, source, _target) = fixture();
    let old = inventory
        .finish(91, &source, source.parent().unwrap(), "retained")
        .unwrap();
    let op = inventory
        .request_delete(&old.id, old.revision, "generation-check", 0)
        .unwrap();
    let mut changed = old.clone();
    changed.generation = "new-generation".into();
    save_artifact(&inventory.db.lock().unwrap(), &changed).unwrap();
    assert_eq!(inventory.execute_delete(&op.id).unwrap().state, "review");
    assert!(source.join("media.mkv").exists());
    let mut legacy = op;
    legacy.state = "succeeded".into();
    legacy.request = serde_json::to_string(&(&old.id, old.revision, 0u64, false)).unwrap();
    save_operation(&inventory.db.lock().unwrap(), &legacy).unwrap();
    inventory.reconcile_deleted_artifacts().unwrap();
    assert_eq!(
        inventory.get(&old.id).unwrap().generation,
        changed.generation
    );
    assert_eq!(inventory.get(&old.id).unwrap().state, "retained");
}
