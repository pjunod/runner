use super::*;

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
        let mut a = parked(&db, &root);
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
