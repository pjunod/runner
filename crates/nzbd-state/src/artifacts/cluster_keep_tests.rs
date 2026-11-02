use super::*;
use std::sync::Arc;

fn fixture() -> (tempfile::TempDir, Arc<Inventory>, ClusterArtifactCopy) {
    let temp = tempfile::tempdir().unwrap();
    let inventory = Arc::new(Inventory::open(&temp.path().join("state")).unwrap());
    let root = temp.path().join("payloads");
    std::fs::create_dir(&root).unwrap();
    let source = root.join("source");
    inventory
        .allocate_for_delivery(7, &root, &source, true, true)
        .unwrap();
    std::fs::write(source.join("archive.bin"), b"independent source bytes").unwrap();
    inventory.finish(7, &source, &root, "completed").unwrap();
    let copy = inventory
        .cluster_download_authority_source(7)
        .unwrap()
        .unwrap();
    (temp, inventory, copy)
}
#[test]
fn exact_failure_keep_is_durable_and_does_not_change_generic_or_unrelated_keep() {
    let (temp, inventory, copy) = fixture();
    let root = temp.path().join("other");
    std::fs::create_dir(&root).unwrap();
    let other = inventory
        .allocate_for_delivery(8, &root, &root.join("other"), true, true)
        .unwrap();
    inventory.set_keep_on_failure(8, true).unwrap();
    let target = ClusterFailureKeepTarget::Artifact { copy: copy.clone() };
    assert!(
        inventory
            .cluster_copy_failure_keep(&target, Some((1, true)))
            .unwrap()
            .keep
    );
    assert!(!inventory.get(&copy.source.artifact).unwrap().keep);
    assert!(inventory.get(&other.id).unwrap().keep_on_failure);
    drop(inventory);
    let inventory = Inventory::open(&temp.path().join("state")).unwrap();
    assert!(
        inventory
            .cluster_copy_failure_keep(&target, None)
            .unwrap()
            .keep
    );
    assert!(
        !inventory
            .cluster_copy_failure_keep(&target, Some((2, false)))
            .unwrap()
            .keep
    );
    assert!(
        inventory
            .cluster_copy_failure_keep(&target, Some((1, true)))
            .is_err()
    );
    assert!(
        !inventory
            .get(&copy.source.artifact)
            .unwrap()
            .keep_on_failure
    );
    assert!(inventory.get(&other.id).unwrap().keep_on_failure);
}
#[test]
fn exact_failure_keep_refuses_replaced_directory_and_generation() {
    let (_temp, inventory, copy) = fixture();
    let mut wrong = copy.clone();
    wrong.source.generation = "other".into();
    assert!(
        inventory
            .cluster_copy_failure_keep(
                &ClusterFailureKeepTarget::Artifact { copy: wrong },
                Some((1, true))
            )
            .is_err()
    );
    let artifact = inventory.get(&copy.source.artifact).unwrap();
    let retained = artifact.root.join("retained-original");
    std::fs::rename(&artifact.path, &retained).unwrap();
    std::fs::create_dir(&artifact.path).unwrap();
    std::fs::write(artifact.path.join("unknown"), b"unknown").unwrap();
    assert!(
        inventory
            .cluster_copy_failure_keep(
                &ClusterFailureKeepTarget::Artifact { copy },
                Some((1, true))
            )
            .is_err()
    );
    assert_eq!(
        std::fs::read(retained.join("archive.bin")).unwrap(),
        b"independent source bytes"
    );
    assert_eq!(
        std::fs::read(artifact.path.join("unknown")).unwrap(),
        b"unknown"
    );
}
#[test]
fn exact_keep_clear_requeues_only_an_existing_eligible_failure_operation() {
    let (_temp, inventory, copy) = fixture();
    let target = ClusterFailureKeepTarget::Artifact { copy: copy.clone() };
    inventory
        .cluster_copy_failure_keep(&target, Some((1, true)))
        .unwrap();
    inventory
        .set_failure_assessment(
            &copy.source.artifact,
            &copy.source.generation,
            "unusable",
            "fixture terminal archive failure",
        )
        .unwrap();
    inventory
        .cluster_copy_failure_keep(&target, Some((2, false)))
        .unwrap();
    let id = format!("failed-delete-{}", copy.source.artifact);
    assert!(
        matches!(inventory.operation(&id), Err(Error::NotFound)),
        "clearing replicated Keep created deletion authority"
    );
    // Existing public policy creates its ordinary positively-assessed operation.
    inventory.set_keep_on_failure(7, false).unwrap();
    assert_eq!(inventory.operation(&id).unwrap().state, "queued");
    inventory
        .cluster_copy_failure_keep(&target, Some((3, true)))
        .unwrap();
    assert_eq!(inventory.operation(&id).unwrap().state, "cancelled");
    inventory
        .cluster_copy_failure_keep(&target, Some((4, false)))
        .unwrap();
    assert_eq!(inventory.operation(&id).unwrap().state, "queued");
    assert!(
        inventory
            .get(&copy.source.artifact)
            .unwrap()
            .path
            .join("archive.bin")
            .exists()
    );
}
#[test]
fn sealed_failure_keep_survives_restart_and_never_adopts_replaced_or_missing_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let inventory = Inventory::open(&state).unwrap();
    let root = temp.path().join("generations");
    let path = root.join("job-7/work-test/fence-1");
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(path.join("video.bin"), b"sealed bytes").unwrap();
    let observed = fs::identity_for_file(&fs::open_dir(&path).unwrap()).unwrap();
    let target = ClusterFailureKeepTarget::Sealed {
        root: root.clone(),
        path: path.clone(),
        identity: (observed.device, observed.inode),
        result_id: "result-1".into(),
        initial_keep: false,
    };
    assert!(
        !inventory
            .cluster_copy_failure_keep(&target, None)
            .unwrap()
            .keep
    );
    assert!(
        inventory
            .cluster_copy_failure_keep(&target, Some((1, true)))
            .unwrap()
            .keep
    );
    drop(inventory);
    let inventory = Inventory::open(&state).unwrap();
    assert!(
        inventory
            .cluster_copy_failure_keep(&target, None)
            .unwrap()
            .keep
    );
    let kept = root.join("old-copy");
    std::fs::rename(&path, &kept).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(
        inventory
            .cluster_copy_failure_keep(&target, Some((2, false)))
            .is_err()
    );
    assert_eq!(
        std::fs::read(kept.join("video.bin")).unwrap(),
        b"sealed bytes"
    );
    std::fs::remove_dir(&path).unwrap();
    let gone = inventory
        .cluster_copy_failure_keep(&target, Some((2, true)))
        .unwrap();
    assert!(
        !gone.available && !gone.keep,
        "Keep claimed it restored an absent exact source"
    );
}
