use super::*;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecoveryFile {
    pub id: String,
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Recovery {
    pub id: String,
    pub installation: String,
    pub artifact: String,
    pub generation: String,
    pub request_id: String,
    pub request: String,
    pub state: String,
    pub manifest_digest: String,
    pub files: Vec<RecoveryFile>,
    pub published: PathBuf,
    pub consumer: Option<String>,
    pub import_id: Option<String>,
    pub receipt: Option<Receipt>,
    pub error: Option<String>,
    #[serde(default)]
    pub scratch_identity: Option<Identity>,
    #[serde(default)]
    pub publication_identity: Option<Identity>,
    /// The folder the copy was taken from — what a person calls this handoff.
    /// Rows staged before this field existed carry an empty path.
    #[serde(default)]
    pub source: PathBuf,
    pub created_at: i64,
    pub updated_at: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiptFile {
    pub id: String,
    pub bytes: u64,
    pub sha256: String,
    pub result: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Receipt {
    pub import_id: String,
    pub manifest_digest: String,
    pub files: Vec<ReceiptFile>,
}
pub(super) fn save(db: &Connection, r: &Recovery) -> Result<()> {
    db.execute("INSERT INTO recoveries(id,artifact,state,data) VALUES(?1,?2,?3,?4) ON CONFLICT(id) DO UPDATE SET state=CASE WHEN recoveries.state='cancel_pending' AND excluded.state IN ('staging','publishing') THEN recoveries.state ELSE excluded.state END,data=CASE WHEN recoveries.state='cancel_pending' AND excluded.state IN ('staging','publishing') THEN json_set(excluded.data,'$.state','cancel_pending') ELSE excluded.data END",params![r.id,r.artifact,r.state,serde_json::to_string(r)?])?;
    Ok(())
}
fn digest(file: &mut File) -> Result<String> {
    let mut hash = Sha256::new();
    let mut buf = [0u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

/// A filesystem error that says what was being done to what. Field report
/// 2026-09-28: a 46.8 GiB staging copy across two network mounts ran for 24
/// minutes and then recorded "filesystem: Invalid argument (os error 22)" —
/// no operation, no path, nothing to act on.
fn at<T>(r: std::io::Result<T>, op: &str, path: &Path) -> Result<T> {
    r.map_err(|e| {
        Error::Io(std::io::Error::new(
            e.kind(),
            format!("{op} {}: {e}", path.display()),
        ))
    })
}

/// Same, for a step that already returns this module's `Result`.
fn at_dir<T>(r: Result<T>, op: &str, path: &Path) -> Result<T> {
    r.map_err(|e| match e {
        Error::Io(io) => Error::Io(std::io::Error::new(
            io.kind(),
            format!("{op} {}: {io}", path.display()),
        )),
        other => other,
    })
}

/// Copy `input` to a new file at `target` with a plain read/write loop,
/// hashing the source bytes as they pass. `std::io::copy` reaches for
/// `copy_file_range`, which across two different (network) filesystems can
/// fail with EINVAL after partial progress, at which point std surfaces the
/// error instead of falling back — the 24-minute failure above. A user-space
/// loop is the same speed on a network mount and cannot take that path.
/// Returns (bytes written, sha256 of the bytes read from the source).
fn copy_hashing(input: &mut File, target: &Path) -> Result<(u64, String)> {
    let mut out = at(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(target),
        "create",
        target,
    )?;
    let mut hash = Sha256::new();
    let mut buf = vec![0u8; 4 * 1024 * 1024];
    let mut written = 0u64;
    loop {
        let n = at(input.read(&mut buf), "read source for", target)?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
        at(out.write_all(&buf[..n]), "write", target)?;
        written += n as u64;
    }
    at(out.sync_all(), "fsync", target)?;
    Ok((written, format!("{:x}", hash.finalize())))
}

/// fsync a directory where the filesystem allows it. CIFS, some FUSE
/// filesystems and a few others answer EINVAL / ENOTSUP for a directory
/// handle; the regular files inside were already flushed, and refusing to
/// publish over that would leave a complete copy unusable.
fn sync_dir_lenient(path: &Path) -> Result<()> {
    let dir = at(File::open(path), "open directory", path)?;
    match dir.sync_all() {
        Ok(()) => Ok(()),
        // EINVAL → InvalidInput; ENOTSUP / EOPNOTSUPP / ENOSYS → Unsupported.
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::InvalidInput | std::io::ErrorKind::Unsupported
            ) =>
        {
            tracing::debug!(path=%path.display(), error=%e, "directory fsync unsupported here; continuing");
            Ok(())
        }
        Err(e) => at(Err(e), "fsync directory", path),
    }
}

impl Inventory {
    pub fn recovery(&self, key: &str) -> Result<Recovery> {
        read(&self.db.lock().unwrap(), "recoveries", key)
    }
    pub fn recoveries(&self, offset: usize) -> Result<Vec<Recovery>> {
        let db = self.db.lock().unwrap();
        let mut stmt = db.prepare("SELECT data FROM recoveries ORDER BY id LIMIT 100 OFFSET ?1")?;
        let rows = stmt.query_map([offset as i64], |r| r.get::<_, String>(0))?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }
    pub fn recoveries_visible(
        &self,
        offset: usize,
        include_terminal: bool,
    ) -> Result<Vec<Recovery>> {
        if include_terminal {
            return self.recoveries(offset);
        }
        let db = self.db.lock().unwrap();
        let mut stmt = db.prepare("SELECT data FROM recoveries WHERE state NOT IN ('imported','cancelled') ORDER BY id LIMIT 100 OFFSET ?1")?;
        let rows = stmt.query_map([offset as i64], |r| r.get::<_, String>(0))?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }
    /// Copies are independent inodes. Originals are held before scratch is
    /// allocated, and remain held through ambiguous claims or partial receipts.
    pub fn preview_recovery(
        &self,
        key: &str,
        revision: u64,
        paths: &[String],
        root: &Path,
    ) -> Result<serde_json::Value> {
        let a = self.get(key)?;
        if a.revision != revision
            || !a.owned
            || a.hold.is_some()
            || !matches!(a.state.as_str(), "parked_failed" | "retained" | "completed")
        {
            return Err(Error::Conflict(
                "stale selection, unowned or held source".into(),
            ));
        }
        self.verify(&a)?;
        if paths.is_empty() || paths.len() > 1000 {
            return Err(Error::Conflict("select 1–1000 regular files".into()));
        }
        let mut seen = std::collections::HashSet::new();
        let mut bytes = 0u64;
        for path in paths {
            if !seen.insert(path) {
                return Err(Error::Conflict("duplicate selection".into()));
            }
            let f = a
                .files
                .iter()
                .find(|f| &f.path == path && !f.identity.directory)
                .ok_or_else(|| Error::Conflict("selection changed".into()))?;
            bytes = bytes.saturating_add(f.identity.bytes);
        }
        fs::absolute(root)?;
        let mut volume = root;
        while !volume.try_exists()? {
            volume = volume
                .parent()
                .ok_or_else(|| Error::Conflict("recovery volume unavailable".into()))?;
        }
        let available = fs::available_bytes(&fs::open_dir(volume)?)?;
        Ok(
            serde_json::json!({"revision":revision,"selected_files":paths.len(),"source_bytes":bytes,"additional_staging_bytes":bytes,"additional_library_bytes":bytes,"reserve_bytes":1073741824u64,"staging_available_bytes":available,"staging_capacity_met":available>=bytes.saturating_add(1073741824),"recovery_root":root,"library_capacity":"Curator checks its own library volume before import","verification":"Copy digests verify identical bytes; media completeness is assessed separately in Curator"}),
        )
    }
    pub fn stage_recovery(
        &self,
        key: &str,
        revision: u64,
        request_id: &str,
        paths: &[String],
        root: &Path,
    ) -> Result<Recovery> {
        let _guard = self.mutation_guard()?;
        if paths.is_empty() || paths.len() > 1000 || request_id.is_empty() || request_id.len() > 128
        {
            return Err(Error::Conflict(
                "select 1–1000 regular files and provide an idempotency key".into(),
            ));
        }
        let request = serde_json::to_string(&(key, revision, paths, root))?;
        {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare(
                "SELECT data FROM recoveries WHERE json_extract(data,'$.request_id')=?1",
            )?;
            let raw: Option<String> = stmt.query_row([request_id], |r| r.get(0)).optional()?;
            if let Some(raw) = raw {
                let existing: Recovery = serde_json::from_str(&raw)?;
                drop(stmt);
                drop(db);
                return if existing.request == request {
                    self.resume_publication(existing)
                } else {
                    Err(Error::Conflict("idempotency key reused".into()))
                };
            }
        }
        let mut a = self.get(key)?;
        if a.revision != revision
            || !a.owned
            || a.hold.is_some()
            || !matches!(a.state.as_str(), "parked_failed" | "retained" | "completed")
        {
            return Err(Error::Conflict(
                "stale revision, unowned files, or active operation".into(),
            ));
        }
        fs::absolute(root)?;
        if root.starts_with(&a.path)
            || a.path.starts_with(root)
            || root.starts_with(&self.state_dir)
            || self.state_dir.starts_with(root)
        {
            return Err(Error::Conflict(
                "recovery root overlaps source or state".into(),
            ));
        }
        let source = self.verify(&a)?;
        let observed = fs::manifest(&source, 100_000)?;
        if observed != a.files {
            return Err(Error::Conflict("source changed; inspect again".into()));
        }
        let mut selected = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for path in paths {
            if !seen.insert(path) {
                return Err(Error::Conflict("duplicate selection".into()));
            }
            let f = a
                .files
                .iter()
                .find(|f| f.path == *path && !f.identity.directory)
                .ok_or_else(|| {
                    Error::Conflict("select a regular file from the current inventory".into())
                })?;
            let name = path.to_ascii_lowercase();
            if name.ends_with(".tmp")
                || name.ends_with(".part")
                || name.ends_with(".par2")
                || name.ends_with(".rar")
                || name.ends_with(".7z")
            {
                return Err(Error::Conflict(
                    "temporary, archive and parity files cannot be staged as media".into(),
                ));
            }
            selected.push(f.clone());
        }
        // Persist both hold and intention atomically before making scratch.
        let mut r = {
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            let rid = id(&tx)?;
            let r = Recovery {
                id: rid.clone(),
                installation: self.installation.clone(),
                artifact: key.into(),
                generation: id(&tx)?,
                request_id: request_id.into(),
                request,
                state: "staging".into(),
                manifest_digest: String::new(),
                files: Vec::new(),
                published: root.join("published").join(&rid),
                consumer: None,
                import_id: None,
                receipt: None,
                error: None,
                scratch_identity: None,
                publication_identity: None,
                source: a.path.clone(),
                created_at: now(),
                updated_at: now(),
            };
            a.hold = Some(format!("recovery:{}", r.id));
            a.revision += 1;
            save_artifact(&tx, &a)?;
            save(&tx, &r)?;
            event(&tx, key, "recovery_staging", &r.id)?;
            tx.commit()?;
            r
        };
        drop(_guard); // The durable hold protects this source while other jobs keep admitting.
        let result = (|| {
            let required: u64 = selected.iter().map(|f| f.identity.bytes).sum();
            let mut volume = root;
            while !volume.exists() {
                volume = volume
                    .parent()
                    .ok_or_else(|| Error::Conflict("recovery volume unavailable".into()))?;
            }
            let available = at_dir(
                fs::open_dir(volume).and_then(|d| fs::available_bytes(&d)),
                "measure free space on",
                volume,
            )?;
            if available < required.saturating_add(1024 * 1024 * 1024) {
                return Err(Error::Conflict(format!("recovery staging needs {required} bytes plus 1 GiB reserve; {available} available")));
            }
            at(std::fs::create_dir_all(root), "create recovery root", root)?;
            at_dir(fs::open_dir(root), "open recovery root", root)?;
            let staging = root.join(".staging");
            let published = root.join("published");
            at(std::fs::create_dir_all(&staging), "create", &staging)?;
            at(std::fs::create_dir_all(&published), "create", &published)?;
            let staging_dir = at_dir(fs::open_dir(&staging), "open", &staging)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                // Owner-only scratch. A mount that cannot carry POSIX modes
                // (CIFS without unix extensions) answers EPERM/ENOTSUP; the
                // copy is still private to the daemon's user there.
                if let Err(e) = staging_dir.set_permissions(std::fs::Permissions::from_mode(0o700))
                {
                    tracing::debug!(path=%staging.display(), error=%e, "staging directory mode not applied");
                }
            }
            sync_dir_lenient(&staging)?;
            at_dir(fs::open_dir(&published), "open", &published)?;
            let scratch = staging.join(&r.id);
            at(std::fs::create_dir(&scratch), "create", &scratch)?;
            r.scratch_identity = Some(fs::identity(&at(
                at_dir(fs::open_dir(&scratch), "open", &scratch)?.metadata(),
                "stat",
                &scratch,
            )?));
            sync_dir_lenient(&staging)?;
            save(&self.db.lock().unwrap(), &r)?;
            let payload = scratch.join("payload");
            at(std::fs::create_dir(&payload), "create", &payload)?;
            for f in selected {
                if self.recovery(&r.id)?.state == "cancel_pending" {
                    return Err(Error::Conflict("staging cancelled".into()));
                }
                let mut input = at_dir(
                    fs::open_relative(&source, &f.path),
                    "open source file",
                    Path::new(&f.path),
                )?;
                if fs::identity(&at(
                    input.metadata(),
                    "stat source file",
                    Path::new(&f.path),
                )?) != f.identity
                {
                    return Err(Error::Conflict("source identity changed".into()));
                }
                let target = payload.join(&f.path);
                if let Some(p) = target.parent() {
                    at(std::fs::create_dir_all(p), "create", p)?;
                }
                // One pass over the source (copy + hash), one over the copy
                // (verify what landed). The old path read the source twice
                // and the copy once: three passes over a 46.8 GiB file.
                let (bytes, source_hash) = copy_hashing(&mut input, &target)?;
                let sha256 = digest(&mut at(File::open(&target), "open copy", &target)?)?;
                if bytes != f.identity.bytes
                    || sha256 != source_hash
                    || fs::identity(&input.metadata()?) != f.identity
                {
                    return Err(Error::Conflict(
                        "source changed while copying; original remains held".into(),
                    ));
                }
                let file_id = format!("{:x}", Sha256::digest(f.path.as_bytes()));
                r.files.push(RecoveryFile {
                    id: file_id,
                    path: f.path,
                    bytes,
                    sha256,
                });
                sync_dir_lenient(target.parent().unwrap())?;
            }
            r.manifest_digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&r.files)?));
            let manifest_path = scratch.join("manifest.json");
            let mut mf = at(
                std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&manifest_path),
                "create",
                &manifest_path,
            )?;
            at(
                mf.write_all(&serde_json::to_vec(&r)?),
                "write",
                &manifest_path,
            )?;
            at(mf.sync_all(), "fsync", &manifest_path)?;
            sync_dir_lenient(&payload)?;
            sync_dir_lenient(&scratch)?;
            sync_dir_lenient(&staging)?;
            // Journal the complete manifest before publication. Crash recovery
            // may validate it and finish the acknowledgement without recopying.
            let _publication_guard = self.mutation_guard()?;
            if self.recovery(&r.id)?.state == "cancel_pending" {
                return Err(Error::Conflict("staging cancelled".into()));
            }
            r.state = "publishing".into();
            save(&self.db.lock().unwrap(), &r)?;
            let publication_key = format!("recovery-{}", r.id);
            let published_tree = self.publish_directory_unlocked(
                &scratch,
                &r.published,
                &r.artifact,
                &publication_key,
            )?;
            r.publication_identity = Some(published_tree.identity);
            save(&self.db.lock().unwrap(), &r)?;
            sync_dir_lenient(&published)?;
            sync_dir_lenient(&staging)?;
            self.register_publication(&r)?;
            if self
                .operation(&format!("publish-recovery-{}", r.id))
                .is_ok()
            {
                self.retire_publication_source_unlocked(&format!("recovery-{}", r.id))?;
            }
            Ok(())
        })();
        let _guard = self.mutation_guard()?;
        let cancelled = self.recovery(&r.id)?.state == "cancel_pending";
        match result {
            Ok(()) => r.state = "published".into(),
            Err(e) => {
                r.state = "failed".into();
                r.error = Some(e.to_string());
                let scratch = root.join(".staging").join(&r.id);
                if scratch.try_exists().unwrap_or(false) {
                    self.discover_unlocked(scratch.parent().unwrap(), &scratch)?;
                }
            }
        }
        if cancelled {
            r.state = "cancelled".into();
            let mut source = self.get(&r.artifact)?;
            source.hold = Some("review: cancelled recovery".into());
            source.revision += 1;
            save_artifact(&self.db.lock().unwrap(), &source)?;
            if let Ok(mut staged) = self.get(&format!("recovery-{}", r.id)) {
                staged.hold = Some("review: cancelled recovery staging".into());
                staged.revision += 1;
                save_artifact(&self.db.lock().unwrap(), &staged)?;
            }
        }
        r.updated_at = now();
        save(&self.db.lock().unwrap(), &r)?;
        Ok(r)
    }
    /// Resolve publication from its durable manifest. Incomplete copying is
    /// retained for explicit review; it is never recursively erased on restart.
    fn resume_publication(&self, mut r: Recovery) -> Result<Recovery> {
        if r.state == "cancel_pending" && r.consumer.is_none() {
            r.state = "cancelled".into();
            r.updated_at = now();
            let mut source = self.get(&r.artifact)?;
            source.hold = Some("review: cancelled recovery".into());
            source.revision += 1;
            save_artifact(&self.db.lock().unwrap(), &source)?;
            save(&self.db.lock().unwrap(), &r)?;
            if let Some(root) = r.published.parent().and_then(Path::parent) {
                let scratch = root.join(".staging").join(&r.id);
                if scratch.try_exists().unwrap_or(false) {
                    self.discover_unlocked(scratch.parent().unwrap(), &scratch)?;
                }
            }
            return Ok(r);
        }
        if !matches!(r.state.as_str(), "staging" | "publishing") {
            return Ok(r);
        }
        let root = r
            .published
            .parent()
            .and_then(Path::parent)
            .ok_or_else(|| Error::Conflict("invalid recovery root".into()))?;
        let scratch = root.join(".staging").join(&r.id);
        let result = (|| {
            if r.state != "publishing" {
                return Err(Error::Conflict("copy interrupted before its complete manifest was durable; inspect retained staging".into()));
            }
            let publication_key = format!("recovery-{}", r.id);
            if self
                .operation(&format!("publish-{publication_key}"))
                .is_ok()
                || !r.published.try_exists()?
            {
                let tree = self.publish_directory_unlocked(
                    &scratch,
                    &r.published,
                    &r.artifact,
                    &publication_key,
                )?;
                r.publication_identity = Some(tree.identity);
                save(&self.db.lock().unwrap(), &r)?;
            }
            let location = if r.published.try_exists()? {
                &r.published
            } else {
                &scratch
            };
            let dir = fs::open_dir(location)?;
            let observed = fs::identity(&dir.metadata()?);
            if !r
                .publication_identity
                .as_ref()
                .or(r.scratch_identity.as_ref())
                .is_some_and(|i| i.same_object(&observed))
            {
                return Err(Error::Conflict(
                    "publication directory identity changed".into(),
                ));
            }
            let raw = fs::open_relative(&dir, "manifest.json")?;
            let manifest: Recovery = serde_json::from_reader(raw)?;
            if manifest.id != r.id
                || manifest.generation != r.generation
                || manifest.manifest_digest != r.manifest_digest
            {
                return Err(Error::Conflict("publication manifest changed".into()));
            }
            let entries = fs::manifest(&dir, 100_000)?;
            if entries.iter().filter(|f| !f.identity.directory).count() != r.files.len() + 1 {
                return Err(Error::Conflict("unexpected files in publication".into()));
            }
            for f in &r.files {
                let mut input = fs::open_relative(&dir, &format!("payload/{}", f.path))?;
                if input.metadata()?.len() != f.bytes || digest(&mut input)? != f.sha256 {
                    return Err(Error::Conflict("publication digest changed".into()));
                }
            }
            if location == &scratch {
                return Err(Error::Conflict(
                    "recovery publication did not reach its destination".into(),
                ));
            }
            self.register_publication(&r)?;
            if self
                .operation(&format!("publish-recovery-{}", r.id))
                .is_ok()
            {
                self.retire_publication_source_unlocked(&format!("recovery-{}", r.id))?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                r.state = "published".into();
                r.error = None;
            }
            Err(e) => {
                r.state = "failed".into();
                r.error = Some(e.to_string());
                // The inventory owns the diagnosis even when copying never
                // reached publication. Adoption remains an explicit action.
                if scratch.try_exists().unwrap_or(false) {
                    self.discover_unlocked(scratch.parent().unwrap(), &scratch)?;
                }
            }
        }
        r.updated_at = now();
        save(&self.db.lock().unwrap(), &r)?;
        Ok(r)
    }
    pub fn reconcile_recoveries(&self) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let pending = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare(
                "SELECT data FROM recoveries WHERE state IN ('staging','publishing') OR (state='cancel_pending' AND json_extract(data,'$.consumer') IS NULL) LIMIT 10",
            )?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in pending {
            self.resume_publication(serde_json::from_str(&raw)?)?;
        }
        Ok(())
    }
    pub fn claim_recovery(
        &self,
        key: &str,
        consumer: &str,
        import_id: &str,
        manifest: &str,
    ) -> Result<Recovery> {
        let _guard = self.mutation_guard()?;
        let mut r = self.recovery(key)?;
        if r.manifest_digest != manifest || import_id.is_empty() {
            return Err(Error::Conflict(
                "manifest changed or missing import id".into(),
            ));
        }
        if r.consumer.as_deref() == Some(consumer)
            && r.import_id.as_deref() == Some(import_id)
            && matches!(r.state.as_str(), "claimed" | "imported")
        {
            r.updated_at = now();
            save(&self.db.lock().unwrap(), &r)?;
            return Ok(r);
        }
        if r.state != "published" || r.consumer.is_some() {
            return Err(Error::Conflict(
                "recovery already claimed or unavailable".into(),
            ));
        }
        r.consumer = Some(consumer.into());
        r.import_id = Some(import_id.into());
        r.state = "claimed".into();
        r.updated_at = now();
        save(&self.db.lock().unwrap(), &r)?;
        Ok(r)
    }
    pub fn recovery_receipt(
        &self,
        key: &str,
        consumer: &str,
        receipt: Receipt,
    ) -> Result<Recovery> {
        let _guard = self.mutation_guard()?;
        let mut r = self.recovery(key)?;
        if r.consumer.as_deref() != Some(consumer)
            || r.import_id.as_deref() != Some(&receipt.import_id)
            || r.manifest_digest != receipt.manifest_digest
        {
            return Err(Error::Conflict(
                "receipt does not belong to this claim".into(),
            ));
        }
        if let Some(previous) = &r.receipt {
            if previous == &receipt {
                return Ok(r);
            }
            return Err(Error::Conflict("conflicting receipt".into()));
        }
        if r.state != "claimed" {
            return Err(Error::Conflict("recovery is not claimed".into()));
        }
        let mut seen = std::collections::HashSet::new();
        for f in &receipt.files {
            let expected = r
                .files
                .iter()
                .find(|x| x.id == f.id)
                .ok_or_else(|| Error::Conflict("receipt includes an unselected file".into()))?;
            if !seen.insert(&f.id)
                || f.bytes != expected.bytes
                || f.sha256 != expected.sha256
                || !matches!(
                    f.result.as_str(),
                    "imported" | "already_present" | "failed" | "skipped"
                )
            {
                return Err(Error::Conflict("invalid per-file receipt".into()));
            }
        }
        let complete = receipt.files.len() == r.files.len()
            && receipt
                .files
                .iter()
                .all(|f| matches!(f.result.as_str(), "imported" | "already_present"));
        r.state = if complete { "imported" } else { "partial" }.into();
        r.receipt = Some(receipt);
        r.updated_at = now();
        let mut a = self.get(&r.artifact)?;
        if complete {
            a.hold = None;
            a.eligible_seconds = 0;
            a.deadline = Some(now().saturating_add(a.retention_seconds as i64));
            // Unselected media is never implicitly authorized for expiry.
            if a.files.iter().filter(|f| !f.identity.directory).count() != r.files.len() {
                a.hold = Some("review: unselected source files remain".into());
            }
            a.revision += 1;
        }
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        if complete {
            let mut staged: Artifact = read(&tx, "artifacts", &format!("recovery-{}", r.id))?;
            staged.state = "recovery_imported".into();
            staged.hold = None;
            staged.deadline = Some(now() + 86400);
            staged.eligible_seconds = 0;
            staged.revision += 1;
            save_artifact(&tx, &staged)?;
        }
        save(&tx, &r)?;
        save_artifact(&tx, &a)?;
        event(&tx, &a.id, "recovery_receipt", &r.state)?;
        tx.commit()?;
        Ok(r)
    }
    pub fn cancel_recovery(
        &self,
        key: &str,
        consumer: Option<&str>,
        ack: bool,
    ) -> Result<Recovery> {
        let _guard = self.mutation_guard()?;
        let mut r = self.recovery(key)?;
        if matches!(r.state.as_str(), "staging" | "publishing")
            || (r.state == "cancel_pending" && r.consumer.is_none())
        {
            r.state = "cancel_pending".into();
            r.updated_at = now();
            save(&self.db.lock().unwrap(), &r)?;
            return Ok(r);
        }
        if r.state == "imported" {
            return Err(Error::Conflict("import already committed".into()));
        }
        let final_partial = r.state == "partial" && r.receipt.is_some();
        if r.consumer.is_some() && !ack && !final_partial {
            r.state = "cancel_pending".into();
        } else {
            if r.consumer.is_some()
                && !final_partial
                && (consumer != r.consumer.as_deref() || r.state != "cancel_pending")
            {
                return Err(Error::Conflict(
                    "claimed cancellation requires the worker's quiescence acknowledgement".into(),
                ));
            }
            r.state = "cancelled".into();
            let mut a = self.get(&r.artifact)?;
            if let Ok(mut staged) = self.get(&format!("recovery-{}", r.id)) {
                staged.hold = Some("review: cancelled recovery staging".into());
                staged.revision += 1;
                save_artifact(&self.db.lock().unwrap(), &staged)?;
            }
            a.hold = Some("review: cancelled recovery".into());
            a.revision += 1;
            save_artifact(&self.db.lock().unwrap(), &a)?;
        }
        r.updated_at = now();
        save(&self.db.lock().unwrap(), &r)?;
        Ok(r)
    }
}

impl Inventory {
    /// Explicit, receipt-scoped source cleanup. Other source files and partial
    /// failures remain held; a receipt never authorizes deleting a whole tree.
    pub fn prune_receipted_source(&self, key: &str) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let r = self.recovery(key)?;
        if !matches!(r.state.as_str(), "imported" | "partial") {
            return Err(Error::Conflict(
                "a final per-file receipt is required".into(),
            ));
        }
        let receipt = r
            .receipt
            .as_ref()
            .ok_or_else(|| Error::Conflict("no final receipt".into()))?;
        let mut a = self.get(&r.artifact)?;
        if a.terminal() {
            return Ok(());
        }
        if !a.owned || a.keep {
            return Err(Error::Conflict("source is unowned or kept".into()));
        }
        if a.hold.as_deref().is_some_and(|hold| {
            hold != format!("recovery:{}", r.id)
                && hold != "review: unselected source files remain"
                && hold != "review: unselected source files remain after receipt cleanup"
        }) {
            return Err(Error::Conflict("source has an unrelated hold".into()));
        }
        self.verify_root(&a)?;
        if !a.path.try_exists()? {
            a.state = "source_gone".into();
            a.hold = None;
            a.updated_at = now();
            a.revision += 1;
            save_artifact(&self.db.lock().unwrap(), &a)?;
            return Ok(());
        }
        let dir = self.verify(&a)?;
        let mut removals = Vec::new();
        for imported in receipt
            .files
            .iter()
            .filter(|f| matches!(f.result.as_str(), "imported" | "already_present"))
        {
            let selected = r
                .files
                .iter()
                .find(|f| f.id == imported.id)
                .ok_or_else(|| Error::Conflict("receipt file missing from manifest".into()))?;
            let Some(entry) = a
                .files
                .iter()
                .find(|f| f.path == selected.path && !f.identity.directory)
            else {
                continue;
            };
            let mut file = match fs::open_relative(&dir, &entry.path) {
                Ok(file) => file,
                Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            if fs::identity(&file.metadata()?) != entry.identity
                || digest(&mut file)? != selected.sha256
            {
                return Err(Error::Conflict(
                    "source file changed after recovery staging".into(),
                ));
            }
            removals.push(entry.clone());
        }
        // Verify every selected file before removing any of them.
        for entry in &removals {
            fs::remove_entry(&dir, entry)?;
        }
        let mut directories = a
            .files
            .iter()
            .filter(|e| e.identity.directory)
            .cloned()
            .collect::<Vec<_>>();
        directories.sort_by_key(|e| std::cmp::Reverse(e.path.matches('/').count()));
        for entry in directories {
            match fs::open_relative(&dir, &entry.path) {
                Ok(child) if fs::names(&child)?.is_empty() => {
                    fs::remove_entry(&dir, &entry)?;
                }
                Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => (),
                Err(e) => return Err(e),
                _ => (),
            }
        }
        let remaining = fs::manifest(&dir, 100_000)?;
        if remaining.is_empty() {
            let root = self.verify_root(&a)?;
            let current = fs::open_at(&root, a.path.file_name().unwrap().as_ref(), true)?;
            let observed = fs::identity(&current.metadata()?);
            if !a
                .identity
                .as_ref()
                .is_some_and(|i| i.same_object(&observed))
            {
                return Err(Error::Conflict("source directory changed".into()));
            }
            fs::unlink(&root, a.path.file_name().unwrap().as_ref(), true)?;
            a.state = "deleted".into();
            a.hold = None;
        } else {
            // Directory mtimes change as selected children are removed, but
            // additions or substituted objects revoke remaining ownership.
            if remaining.iter().any(|f| {
                !a.files.iter().any(|old| {
                    old.path == f.path
                        && old.identity.same_object(&f.identity)
                        && (f.identity.directory || old.identity == f.identity)
                })
            }) {
                a.owned = false;
                a.keep = true;
            }
            a.hold = Some("review: unselected source files remain after receipt cleanup".into());
        }
        a.files = remaining;
        a.revision += 1;
        a.updated_at = now();
        save_artifact(&self.db.lock().unwrap(), &a)?;
        event(
            &self.db.lock().unwrap(),
            &a.id,
            "receipt_source_cleanup",
            key,
        )?;
        Ok(())
    }
}
