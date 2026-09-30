use super::relocation::{RegistryPolicy, FAULTS};
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
        FAULTS.with(|f| f.borrow_mut().insert("capability", code));
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
fn registry_generation_is_stable_retains_source_and_failed_media_status() {
    let (temp, inventory, source, target) = fixture();
    let managed = temp.path().join("registry");
    std::fs::create_dir(&managed).unwrap();
    let policy = RegistryPolicy {
        managed_root: managed,
        consumers_isolated: true,
    };
    FAULTS.with(|f| f.borrow_mut().insert("capability", libc::EINVAL));
    let result = inventory
        .relocate_with_registry(91, &target, Some(&policy))
        .unwrap();
    assert!(result.published_path.join("media.mkv").exists());
    assert!(source.join("media.mkv").exists());
    assert!(!target.exists());
    let repeated = inventory
        .relocate_with_registry(91, &target, Some(&policy))
        .unwrap();
    assert_eq!(result.operation_id, repeated.operation_id);
    assert_eq!(result.published_path, repeated.published_path);
    let parked = inventory
        .finish(
            91,
            &result.published_path,
            result.published_path.parent().unwrap(),
            "parked_failed",
        )
        .unwrap();
    assert_eq!(parked.state, "parked_failed");
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
