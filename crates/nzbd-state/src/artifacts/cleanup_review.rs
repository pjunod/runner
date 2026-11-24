//! Explicit reauthorization of a failed deletion. Ordinary verification is unchanged.
use super::*;
use sha2::Digest;
use std::io::Read;

#[cfg(test)]
type CaptureHook = Box<dyn FnOnce()>;
#[cfg(test)]
thread_local! { static AFTER_CAPTURE: std::cell::RefCell<Option<CaptureHook>> = const { std::cell::RefCell::new(None) }; }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CleanupReview {
    pub artifact: String,
    pub generation: String,
    pub revision: u64,
    pub root: PathBuf,
    pub path: PathBuf,
    pub previous_root: Identity,
    pub observed_root: Identity,
    pub previous_identity: Option<Identity>,
    pub observed_identity: Option<Identity>,
    pub files: Vec<FileEntry>,
    pub fingerprint: String,
}

impl Inventory {
    fn cleanup_review_candidate(&self, key: &str) -> Result<Artifact> {
        let a = self.get(key)?;
        if !cfg!(unix)
            || !a.owned
            || a.state != "delete_failed"
            || a.hold.as_deref() != Some("review")
        {
            return Err(Error::Conflict(
                "only an owned failed deletion held for review can be reviewed".into(),
            ));
        }
        if self.open_intent_for_artifact(key)? {
            return Err(Error::Conflict(
                "an open publication move owns this source".into(),
            ));
        }
        let db = self.db.lock().unwrap();
        let active: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM recoveries WHERE artifact=?1 AND state NOT IN ('imported','cancelled','partial')) OR EXISTS(SELECT 1 FROM operations WHERE artifact=?1 AND state IN ('queued','running','retry'))",
            [key], |r| r.get(0))?;
        let reviewed: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM operations WHERE artifact=?1 AND state='review' AND json_extract(data,'$.kind')='delete')", [key], |r| r.get(0))?;
        if active || !reviewed {
            return Err(Error::Conflict(
                "active or ambiguous custody prevents cleanup review".into(),
            ));
        }
        drop(db);
        // The old allocation marker remains authority even when the volume has
        // been remounted. Never manufacture ownership from a path or inode.
        let marker = self
            .state_dir
            .join("artifact-identities")
            .join(&a.id)
            .join(format!("{}.json", a.generation));
        let data: serde_json::Value = serde_json::from_slice(&std::fs::read(marker)?)?;
        let identity: Option<Identity> = serde_json::from_value(data["identity"].clone())?;
        let root: Identity = serde_json::from_value(data["root_identity"].clone())?;
        if data["installation"] != self.installation
            || data["artifact"] != a.id
            || data["generation"] != a.generation
            || data["path"] != a.path.to_string_lossy().as_ref()
            || data["root"] != a.root.to_string_lossy().as_ref()
            || serde_json::to_value(identity)? != serde_json::to_value(&a.identity)?
            || serde_json::to_value(root)? != serde_json::to_value(&a.root_identity)?
        {
            return Err(Error::Conflict(
                "allocation marker does not match the failed generation".into(),
            ));
        }
        Ok(a)
    }

    fn capture_cleanup_review(&self, a: &Artifact) -> Result<CleanupReview> {
        if a.path.parent() != Some(a.root.as_path()) {
            return Err(Error::Conflict(
                "payload is not an immediate child of its root".into(),
            ));
        }
        let root = fs::open_dir(&a.root)?;
        let observed_root = fs::identity_for_file(&root)?;
        let legacy_remount = self.live_legacy_root_device(a) == Some(observed_root.device)
            && a.root_identity.mount_id.is_empty()
            && !observed_root.mount_id.is_empty()
            && a.root_identity.inode == observed_root.inode
            && a.root_identity.directory
            && observed_root.directory;
        if !a.root_identity.same_object(&observed_root) && !legacy_remount {
            return Err(Error::Conflict(
                "root object changed or stable mount identity is unavailable".into(),
            ));
        }
        // Reuse protected-role checks with the explicitly observed root. No
        // stored evidence is changed by previewing this prospective binding.
        let mut candidate = a.clone();
        candidate.root_identity = observed_root.clone();
        self.verify_root(&candidate)?;
        let dir = match fs::open_at(&root, Path::new(a.path.file_name().unwrap()), true) {
            Ok(dir) => Some(dir),
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };
        let mut files = Vec::new();
        let mut observed_identity = None;
        if let Some(dir) = dir {
            let observed = fs::identity_for_file(&dir)?;
            let old = a
                .identity
                .as_ref()
                .ok_or_else(|| Error::Conflict("allocation identity is unavailable".into()))?;
            if !old.same_object(&observed)
                && !(legacy_remount
                    && old.inode == observed.inode
                    && old.directory
                    && observed.directory
                    && observed.mount_id == observed_root.mount_id)
            {
                return Err(Error::Conflict(
                    "payload object changed; cleanup review cannot adopt a replacement".into(),
                ));
            }
            files = fs::manifest(&dir, 100_000)?;
            files.sort_by(|a, b| a.path.cmp(&b.path));
            for entry in &mut files {
                if entry.identity.directory {
                    continue;
                }
                let mut file = fs::open_relative(&dir, &entry.path)?;
                if fs::identity_for_file(&file)? != entry.identity {
                    return Err(Error::Conflict("file changed during cleanup review".into()));
                }
                let mut digest = sha2::Sha256::new();
                let mut buffer = [0; 65536];
                loop {
                    let n = file.read(&mut buffer)?;
                    if n == 0 {
                        break;
                    }
                    digest.update(&buffer[..n]);
                }
                if fs::identity_for_file(&file)? != entry.identity {
                    return Err(Error::Conflict(
                        "file changed while hashing cleanup review".into(),
                    ));
                }
                entry.digest = Some(format!("{:x}", digest.finalize()));
            }
            let mut after = fs::manifest(&dir, 100_000)?;
            after.sort_by(|a, b| a.path.cmp(&b.path));
            let without_digests: Vec<_> = files
                .iter()
                .cloned()
                .map(|mut f| {
                    f.digest = None;
                    f
                })
                .collect();
            if after != without_digests {
                return Err(Error::Conflict(
                    "manifest changed during cleanup review".into(),
                ));
            }
            observed_identity = Some(observed);
        }
        #[cfg(test)]
        AFTER_CAPTURE.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        // Hashes describe the opened objects. Recheck their names too so an
        // external rename during a long read cannot accept an already stale
        // allocation (even though ordinary deletion would still refuse it).
        let named_root = self.verify_root(&candidate)?;
        let named = fs::open_at(&named_root, Path::new(a.path.file_name().unwrap()), true);
        match (&observed_identity, named) {
            (Some(expected), Ok(file)) if fs::identity_for_file(&file)? == *expected => {}
            (None, Err(Error::Io(e))) if e.kind() == std::io::ErrorKind::NotFound => {}
            _ => {
                return Err(Error::Conflict(
                    "allocation name changed during cleanup review".into(),
                ))
            }
        }
        let mut review = CleanupReview {
            artifact: a.id.clone(),
            generation: a.generation.clone(),
            revision: a.revision,
            root: a.root.clone(),
            path: a.path.clone(),
            previous_root: a.root_identity.clone(),
            observed_root,
            previous_identity: a.identity.clone(),
            observed_identity,
            files,
            fingerprint: String::new(),
        };
        review.fingerprint = format!("{:x}", sha2::Sha256::digest(serde_json::to_vec(&review)?));
        Ok(review)
    }

    pub fn preview_cleanup_review(&self, key: &str) -> Result<CleanupReview> {
        let a = self.cleanup_review_candidate(key)?;
        let Some(_lease) = self.reserve_reconciliation(std::slice::from_ref(&a))? else {
            return Err(Error::Conflict("artifact changed or is in use".into()));
        };
        let review = self.capture_cleanup_review(&a)?;
        self.check_payload_capture(&a)?;
        Ok(review)
    }

    pub fn accept_cleanup_review(
        &self,
        key: &str,
        revision: u64,
        generation: &str,
        fingerprint: &str,
        acknowledged: bool,
    ) -> Result<Artifact> {
        if !acknowledged || fingerprint.len() != 64 {
            return Err(Error::Conflict(
                "explicit acknowledgement of the current root and bytes is required".into(),
            ));
        }
        let original = self.cleanup_review_candidate(key)?;
        if original.revision != revision || original.generation != generation {
            return Err(Error::Conflict(
                "cleanup review generation or revision changed".into(),
            ));
        }
        let Some(_lease) = self.reserve_reconciliation(std::slice::from_ref(&original))? else {
            return Err(Error::Conflict("artifact changed or is in use".into()));
        };
        let review = self.capture_cleanup_review(&original)?;
        if review.fingerprint != fingerprint {
            return Err(Error::Conflict(
                "cleanup review evidence changed; preview again".into(),
            ));
        }
        let _guard = self.mutation_guard()?;
        self.check_payload_capture(&original)?;
        let mut a = original.clone();
        a.root_identity = review.observed_root;
        a.identity = review.observed_identity;
        a.files = review.files;
        a.keep = true;
        a.automatic_cleanup_eligible = false;
        a.hold = None;
        a.error = None;
        a.revision += 1;
        a.updated_at = now();
        a.inspected_at = Some(now());
        if a.identity.is_some() {
            // A new evidence generation gets a new immutable sidecar path.
            // A DB failure/crash cannot overwrite the old allocation authority.
            a.generation = id(&self.db.lock().unwrap())?;
            a.state = "retained".into();
            self.sidecar(&a)?;
        } else {
            a.state = "source_gone".into();
        }
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        // Old approvals never execute against the reviewed replacement evidence.
        let raws = {
            let mut stmt =
                tx.prepare("SELECT data FROM operations WHERE artifact=?1 AND state='review'")?;
            let rows = stmt.query_map([key], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in raws {
            let mut op: Operation = serde_json::from_str(&raw)?;
            if op.kind == "delete" {
                op.state = "cancelled".into();
                save_operation(&tx, &op)?;
            }
        }
        save_artifact(&tx, &a)?;
        event(
            &tx,
            key,
            "cleanup_review_accepted",
            &serde_json::to_string(&serde_json::json!({
                "previous_generation":original.generation,"generation":a.generation,"fingerprint":fingerprint,
                "previous_root":original.root_identity,"observed_root":a.root_identity,"state":a.state
            }))?,
        )?;
        tx.commit()?;
        Ok(a)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failed() -> (tempfile::TempDir, Inventory, PathBuf, Artifact, Operation) {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("processing");
        std::fs::create_dir(&root).unwrap();
        let db = Inventory::open(&tmp.path().join("state")).unwrap();
        let path = root.join("payload");
        db.allocate(7, &root, &path).unwrap();
        std::fs::write(path.join("media.mkv"), b"before").unwrap();
        let a = db.finish(7, &path, &root, "parked_failed").unwrap();
        let op = db
            .request_delete(&a.id, a.revision, "original-delete", 0)
            .unwrap();
        std::fs::write(path.join("media.mkv"), b"changed current bytes").unwrap();
        let op = db.execute_delete(&op.id).unwrap();
        assert_eq!(op.state, "review");
        let a = db.get(&a.id).unwrap();
        assert_eq!(a.state, "delete_failed");
        (tmp, db, root, a, op)
    }

    #[test]
    fn cleanup_review_requires_fresh_explicit_approval_and_keeps_new_generation() {
        let (_tmp, db, _root, a, op) = failed();
        let review = db.preview_cleanup_review(&a.id).unwrap();
        assert!(review.files[0].digest.is_some());
        assert!(db
            .accept_cleanup_review(&a.id, a.revision, &a.generation, &review.fingerprint, false)
            .is_err());
        std::fs::write(a.path.join("media.mkv"), b"different current bytes").unwrap();
        assert!(db
            .accept_cleanup_review(&a.id, a.revision, &a.generation, &review.fingerprint, true)
            .is_err());
        let review = db.preview_cleanup_review(&a.id).unwrap();
        let accepted = db
            .accept_cleanup_review(&a.id, a.revision, &a.generation, &review.fingerprint, true)
            .unwrap();
        assert_ne!(accepted.generation, a.generation);
        assert_eq!(accepted.state, "retained");
        assert!(accepted.keep);
        assert!(!accepted.automatic_cleanup_eligible);
        assert_eq!(db.operation(&op.id).unwrap().state, "cancelled");
        assert!(db
            .request_delete(&accepted.id, accepted.revision, "no-keep-release", 0)
            .is_err());
        let accepted = db
            .retention(&accepted.id, accepted.revision, false, None)
            .unwrap();
        let next = db
            .request_delete(&accepted.id, accepted.revision, "new-delete", 0)
            .unwrap();
        assert_eq!(db.execute_delete(&next.id).unwrap().state, "succeeded");
        assert!(!accepted.path.exists());
    }

    #[test]
    fn cleanup_review_refuses_replaced_directory_marker_and_stale_revision() {
        let (_tmp, db, root, a, _) = failed();
        let review = db.preview_cleanup_review(&a.id).unwrap();
        assert!(db
            .accept_cleanup_review(
                &a.id,
                a.revision + 1,
                &a.generation,
                &review.fingerprint,
                true
            )
            .is_err());
        std::fs::rename(&a.path, root.join("old")).unwrap();
        std::fs::create_dir(&a.path).unwrap();
        std::fs::write(a.path.join("media.mkv"), b"replacement").unwrap();
        assert!(db.preview_cleanup_review(&a.id).is_err());
        assert_eq!(
            std::fs::read(a.path.join("media.mkv")).unwrap(),
            b"replacement"
        );
        std::fs::remove_dir_all(&a.path).unwrap();
        std::fs::rename(root.join("old"), &a.path).unwrap();
        let marker = db
            .state_dir
            .join("artifact-identities")
            .join(&a.id)
            .join(format!("{}.json", a.generation));
        std::fs::write(marker, b"{}").unwrap();
        assert!(db.preview_cleanup_review(&a.id).is_err());
    }

    #[test]
    fn cleanup_review_refuses_active_custody_and_extra_files_after_preview() {
        let (_tmp, db, _root, a, _) = failed();
        let review = db.preview_cleanup_review(&a.id).unwrap();
        std::fs::write(a.path.join("new.mkv"), b"new").unwrap();
        assert!(db
            .accept_cleanup_review(&a.id, a.revision, &a.generation, &review.fingerprint, true)
            .is_err());
        let lease = db
            .reserve_reconciliation(std::slice::from_ref(&a))
            .unwrap()
            .unwrap();
        assert!(db.preview_cleanup_review(&a.id).is_err());
        drop(lease);
        assert!(db.preview_cleanup_review(&a.id).is_ok());
    }

    #[test]
    fn cleanup_review_missing_payload_settles_only_the_reviewed_absence() {
        let (_tmp, db, root, a, op) = failed();
        std::fs::remove_dir_all(&a.path).unwrap();
        std::fs::write(root.join("unrelated"), b"keep").unwrap();
        let review = db.preview_cleanup_review(&a.id).unwrap();
        assert!(review.observed_identity.is_none());
        let accepted = db
            .accept_cleanup_review(&a.id, a.revision, &a.generation, &review.fingerprint, true)
            .unwrap();
        assert_eq!(accepted.state, "source_gone");
        assert_eq!(db.operation(&op.id).unwrap().state, "cancelled");
        assert_eq!(std::fs::read(root.join("unrelated")).unwrap(), b"keep");
    }

    #[test]
    fn cleanup_review_failed_db_commit_does_not_overwrite_old_allocation_marker() {
        let (_tmp, db, _root, a, _) = failed();
        let marker = db
            .state_dir
            .join("artifact-identities")
            .join(&a.id)
            .join(format!("{}.json", a.generation));
        let before = std::fs::read(&marker).unwrap();
        let review = db.preview_cleanup_review(&a.id).unwrap();
        db.db.lock().unwrap().execute_batch("CREATE TRIGGER reject_review BEFORE UPDATE ON artifacts BEGIN SELECT RAISE(ABORT,'injected review failure'); END;").unwrap();
        assert!(db
            .accept_cleanup_review(&a.id, a.revision, &a.generation, &review.fingerprint, true)
            .is_err());
        assert_eq!(std::fs::read(marker).unwrap(), before);
        assert_eq!(db.get(&a.id).unwrap().revision, a.revision);
        assert!(db.preview_cleanup_review(&a.id).is_ok());
    }

    #[test]
    fn cleanup_review_refuses_name_replacement_during_capture() {
        let (_tmp, db, root, a, _) = failed();
        let path = a.path.clone();
        AFTER_CAPTURE.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                std::fs::rename(&path, root.join("old-allocation")).unwrap();
                std::fs::create_dir(&path).unwrap();
                std::fs::write(path.join("media.mkv"), b"replacement").unwrap();
            }))
        });
        assert!(db.preview_cleanup_review(&a.id).is_err());
        assert_eq!(db.get(&a.id).unwrap().revision, a.revision);
        assert_eq!(
            std::fs::read(a.path.join("media.mkv")).unwrap(),
            b"replacement"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cleanup_review_legacy_remount_requires_matching_root_and_payload_objects() {
        use std::os::unix::fs::MetadataExt;
        let (_tmp, db, root, a, _) = failed();
        let mut legacy = a.clone();
        legacy.root_identity.mount_id.clear();
        legacy.root_identity.device += 100;
        let identity = legacy.identity.as_mut().unwrap();
        identity.mount_id.clear();
        identity.device += 100;
        db.sidecar(&legacy).unwrap();
        save_artifact(&db.db.lock().unwrap(), &legacy).unwrap();
        assert!(db.verify_root(&legacy).is_err());
        // A fallback on the parent filesystem is not remount evidence.
        startup::MOUNTINFO.with(|v| {
            *v.borrow_mut() = Some(format!(
                "1 0 {}:{} / / rw - ext4 /dev/root rw\n",
                libc::major(std::fs::metadata(&root).unwrap().dev()),
                libc::minor(std::fs::metadata(&root).unwrap().dev())
            ))
        });
        assert!(db.preview_cleanup_review(&legacy.id).is_err());
        let device = std::fs::metadata(&root).unwrap().dev();
        startup::MOUNTINFO.with(|v| *v.borrow_mut() = Some(format!("1 0 253:0 / / rw - ext4 /dev/root rw\n100 1 {}:{} / {} rw - nfs4 qnap:/processing rw\n", libc::major(device), libc::minor(device), root.display())));
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                startup::MOUNTINFO.with(|v| *v.borrow_mut() = None);
            }
        }
        let _reset = Reset;
        let review = db.preview_cleanup_review(&legacy.id).unwrap();
        assert!(!review.observed_root.mount_id.is_empty());
        let accepted = db
            .accept_cleanup_review(
                &legacy.id,
                legacy.revision,
                &legacy.generation,
                &review.fingerprint,
                true,
            )
            .unwrap();
        assert!(accepted.keep);
        assert!(db.verify(&accepted).is_ok());
        let mut wrong = db.get(&accepted.id).unwrap();
        wrong.state = "delete_failed".into();
        wrong.hold = Some("review".into());
        wrong.root_identity.inode += 1;
        db.sidecar(&wrong).unwrap();
        save_artifact(&db.db.lock().unwrap(), &wrong).unwrap();
        // A root replacement cannot be reauthorized just by acknowledging it.
        let pending = Operation {
            id: "review-again".into(),
            artifact: wrong.id.clone(),
            kind: "delete".into(),
            state: "review".into(),
            request: String::new(),
            created_at: now(),
            not_before: 0,
            attempts: 1,
            next_retry: 0,
            error: None,
        };
        save_operation(&db.db.lock().unwrap(), &pending).unwrap();
        assert!(db.preview_cleanup_review(&wrong.id).is_err());
        assert!(root.exists());
    }
}
