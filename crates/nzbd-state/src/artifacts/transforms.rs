//! Operation-owned PAR/extraction workspaces, in the existing custody journal.
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Workspace {
    pub operation_id: String,
    pub source: Artifact,
    pub scratch: Artifact,
    #[serde(default)]
    pub retained: std::collections::HashMap<String, String>,
}
impl Inventory {
    pub fn workspace(&self, job: u32, kind: &str, token: &str) -> Result<Workspace> {
        let _guard = self.mutation_guard()?;
        if !matches!(kind, "extract" | "par_repair")
            || token.len() > 128
            || !token.bytes().all(|c| c.is_ascii_hexdigit())
        {
            return Err(Error::Conflict("invalid transform identity".into()));
        }
        let mut source = self.for_job(job)?.ok_or(Error::NotFound)?;
        if source.hold.is_some() || !matches!(source.state.as_str(), "active" | "transitioning") {
            return Err(Error::Conflict("transform source is held".into()));
        }
        let key = format!("{kind}-{}-{}-{token}", source.id, source.generation);
        if let Ok(op) = self.operation(&key) {
            let workspace: Workspace = serde_json::from_str(&op.request)?;
            self.verify(&workspace.scratch)?;
            // The operation identity, not a directory name, grants reuse.
            return Ok(workspace);
        }
        let dir = self.verify(&source)?;
        source.files = fs::manifest(&dir, 100_000)?;
        let workspace_root = self.state_dir.join("transform-workspaces");
        std::fs::create_dir_all(&workspace_root)?;
        let workspace_dir = fs::open_dir(&workspace_root)?;
        let path = workspace_root.join(&key);
        let mut scratch = source.clone();
        scratch.id = format!("scratch-{key}");
        scratch.job = None;
        scratch.generation = id(&self.db.lock().unwrap())?;
        scratch.root = workspace_root;
        scratch.root_identity = fs::identity(&workspace_dir.metadata()?);
        scratch.path = path.clone();
        scratch.identity = None;
        scratch.files.clear();
        scratch.state = "allocating".into();
        scratch.keep = true;
        scratch.owned = true;
        scratch.hold = Some("transform workspace".into());
        let mut workspace = Workspace {
            operation_id: key.clone(),
            source: source.clone(),
            scratch,
            retained: Default::default(),
        };
        let mut op = Operation {
            id: key,
            artifact: source.id.clone(),
            kind: kind.into(),
            state: "running".into(),
            request: serde_json::to_string(&workspace)?,
            created_at: now(),
            not_before: 0,
            attempts: 1,
            next_retry: 0,
            error: None,
        };
        source.state = "transitioning".into();
        source.revision += 1;
        {
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            save_operation(&tx, &op)?;
            save_artifact(&tx, &source)?;
            tx.commit()?;
        }
        // A crash before identity commit leaves an unowned allocation for review.
        std::fs::create_dir(&path)?;
        fs::sync_directory(&workspace_dir)?;
        let scratch_dir = fs::open_dir(&path)?;
        workspace.scratch.identity = Some(fs::identity(&scratch_dir.metadata()?));
        workspace.scratch.state = "active".into();
        self.sidecar(&workspace.scratch)?;
        op.request = serde_json::to_string(&workspace)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &workspace.scratch)?;
        save_operation(&tx, &op)?;
        tx.commit()?;
        Ok(workspace)
    }

    pub fn retain_transform_original(
        &self,
        workspace: &mut Workspace,
        relative: &str,
    ) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let entry = workspace
            .source
            .files
            .iter()
            .find(|f| f.path == relative)
            .ok_or_else(|| Error::Conflict("original is outside captured manifest".into()))?;
        let parent = Path::new(relative).parent().unwrap_or(Path::new(""));
        use sha2::{Digest, Sha256};
        let backup = parent.join(format!(
            ".runner-original-{:x}",
            Sha256::digest(format!("{}:{relative}", workspace.operation_id).as_bytes())
        ));
        let backup = backup.to_string_lossy().into_owned();
        workspace.retained.insert(relative.into(), backup.clone());
        let mut op = self.operation(&workspace.operation_id)?;
        op.request = serde_json::to_string(workspace)?;
        save_operation(&self.db.lock().unwrap(), &op)?;
        let root = fs::open_dir(&workspace.source.path)?;
        if let Ok(old) = fs::open_relative(&root, &backup) {
            if fs::identity(&old.metadata()?) == entry.identity {
                return Ok(());
            }
            return Err(Error::Conflict("retained original identity changed".into()));
        }
        let original = fs::open_relative(&root, relative)?;
        if fs::identity(&original.metadata()?) != entry.identity {
            return Err(Error::Conflict("original identity changed".into()));
        }
        fs::rename_exclusive(
            &workspace.source.path.join(relative),
            &workspace.source.path.join(&backup),
        )?;
        Ok(())
    }

    pub fn finish_workspace(&self, workspace: &Workspace) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let mut source = self.get(&workspace.source.id)?;
        let dir = self.verify(&source)?;
        // Each original file remains unchanged; new output does not authorize
        // removal of any original archive or unrelated sibling.
        for entry in workspace
            .source
            .files
            .iter()
            .filter(|f| !f.identity.directory)
        {
            let path = workspace.retained.get(&entry.path).unwrap_or(&entry.path);
            let observed = fs::open_relative(&dir, path)?;
            if fs::identity(&observed.metadata()?) != entry.identity {
                return Err(Error::Conflict("transform input changed".into()));
            }
        }
        let mut scratch = self.get(&workspace.scratch.id)?;
        let stage = self.verify(&scratch)?;
        scratch.files = fs::manifest(&stage, 100_000)?;
        scratch.state = "retained".into();
        source.state = "active".into();
        source.revision += 1;
        source.files = fs::manifest(&dir, 100_000)?;
        let mut op = self.operation(&workspace.operation_id)?;
        op.state = "succeeded".into();
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &source)?;
        save_artifact(&tx, &scratch)?;
        save_operation(&tx, &op)?;
        event(
            &tx,
            &source.id,
            "transform_published",
            &workspace.operation_id,
        )?;
        tx.commit()?;
        Ok(())
    }
}

impl Inventory {
    /// Journal an intact file's identity before restoring a catalog path.
    pub fn restore_file(&self, job: u32, from: &Path, to: &Path) -> Result<()> {
        use sha2::{Digest, Sha256};
        let _guard = self.mutation_guard()?;
        let mut source = self.for_job(job)?.ok_or(Error::NotFound)?;
        let root = self.verify(&source)?;
        let old = from
            .strip_prefix(&source.path)
            .map_err(|_| Error::Conflict("restore source outside payload".into()))?;
        let new = to
            .strip_prefix(&source.path)
            .map_err(|_| Error::Conflict("restore target outside payload".into()))?;
        if new
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
        {
            return Err(Error::Conflict("unsafe restore target".into()));
        }
        let file = fs::open_relative(&root, &old.to_string_lossy())?;
        let identity = fs::identity(&file.metadata()?);
        let key = format!(
            "restore-{}-{:x}",
            source.generation,
            Sha256::digest(
                [
                    old.as_os_str().as_encoded_bytes(),
                    new.as_os_str().as_encoded_bytes()
                ]
                .concat()
            )
        );
        let mut op = Operation {
            id: key,
            artifact: source.id.clone(),
            kind: "par_restore".into(),
            state: "running".into(),
            request: serde_json::to_string(&(source.clone(), old, new, &identity))?,
            created_at: now(),
            not_before: 0,
            attempts: 1,
            next_retry: 0,
            error: None,
        };
        save_operation(&self.db.lock().unwrap(), &op)?;
        if let Some(parent) = new.parent() {
            crate::fileops::parents(&source.path, parent)?;
        }
        fs::rename_exclusive(from, to)?;
        let observed = fs::open_relative(&root, &new.to_string_lossy())?;
        if fs::identity(&observed.metadata()?) != identity {
            return Err(Error::Conflict("restore identity changed".into()));
        }
        source.files = fs::manifest(&root, 100_000)?;
        source.revision += 1;
        op.state = "succeeded".into();
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &source)?;
        save_operation(&tx, &op)?;
        tx.commit()?;
        Ok(())
    }
}

impl Inventory {
    /// Reconcile journaled mappings before new queue or PP operations start.
    pub(super) fn reconcile_transforms(&self) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let raws = {
            let db = self.db.lock().unwrap();
            let mut statement=db.prepare("SELECT data FROM operations WHERE state='running' AND json_extract(data,'$.kind') IN ('par_restore','par_repair','extract')")?;
            let rows = statement.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in raws {
            let mut op: Operation = serde_json::from_str(&raw)?;
            let result = (|| -> Result<()> {
                if op.kind == "par_restore" {
                    let (mut source, old, new, identity): (Artifact, PathBuf, PathBuf, Identity) =
                        serde_json::from_str(&op.request)?;
                    let root = self.verify(&source)?;
                    let before = fs::open_relative(&root, &old.to_string_lossy());
                    let after = fs::open_relative(&root, &new.to_string_lossy());
                    match (before, after) {
                        (Ok(file), Err(Error::Io(e)))
                            if e.kind() == std::io::ErrorKind::NotFound
                                && fs::identity(&file.metadata()?) == identity =>
                        {
                            if let Some(parent) = new.parent() {
                                crate::fileops::parents(&source.path, parent)?;
                            }
                            fs::rename_exclusive(&source.path.join(&old), &source.path.join(&new))?;
                        }
                        (Err(Error::Io(e)), Ok(file))
                            if e.kind() == std::io::ErrorKind::NotFound
                                && fs::identity(&file.metadata()?) == identity => {}
                        _ => {
                            return Err(Error::Conflict(
                                "interrupted restore has ambiguous identities".into(),
                            ))
                        }
                    }
                    source.files = fs::manifest(&root, 100_000)?;
                    source.revision += 1;
                    op.state = "succeeded".into();
                    let mut db = self.db.lock().unwrap();
                    let tx = db.transaction()?;
                    save_artifact(&tx, &source)?;
                    save_operation(&tx, &op)?;
                    tx.commit()?;
                } else {
                    let workspace: Workspace = serde_json::from_str(&op.request)?;
                    self.verify(&workspace.scratch)?;
                    let root = self.verify(&workspace.source)?;
                    for entry in workspace
                        .source
                        .files
                        .iter()
                        .filter(|e| !e.identity.directory)
                    {
                        let path = workspace.retained.get(&entry.path).unwrap_or(&entry.path);
                        if fs::identity(&fs::open_relative(&root, path)?.metadata()?)
                            != entry.identity
                        {
                            return Err(Error::Conflict(
                                "interrupted transform input changed".into(),
                            ));
                        }
                    }
                }
                Ok(())
            })();
            if let Err(error) = result {
                op.state = "review".into();
                op.error = Some(error.to_string());
                let mut source = self.get(&op.artifact)?;
                source.hold = Some("review: interrupted transform".into());
                source.keep = true;
                let mut db = self.db.lock().unwrap();
                let tx = db.transaction()?;
                save_artifact(&tx, &source)?;
                save_operation(&tx, &op)?;
                tx.commit()?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn transforms_reuse_owned_workspace_and_retain_originals() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("downloads");
        std::fs::create_dir(&root).unwrap();
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        let source = root.join("job");
        inventory.allocate(5, &root, &source).unwrap();
        std::fs::write(source.join("flat.bin"), b"original").unwrap();
        let mut workspace = inventory.workspace(5, "par_repair", "abc").unwrap();
        std::fs::write(
            workspace.scratch.path.join("repaired.bin"),
            b"verified output",
        )
        .unwrap();
        inventory
            .retain_transform_original(&mut workspace, "flat.bin")
            .unwrap();
        drop(inventory);
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        inventory.reconcile_startup(&[5]).unwrap();
        let reused = inventory.workspace(5, "par_repair", "abc").unwrap();
        assert_eq!(workspace.operation_id, reused.operation_id);
        inventory.finish_workspace(&reused).unwrap();
        let backup = reused.retained.get("flat.bin").unwrap();
        assert_eq!(std::fs::read(source.join(backup)).unwrap(), b"original");
        assert_eq!(
            std::fs::read(reused.scratch.path.join("repaired.bin")).unwrap(),
            b"verified output"
        );
    }
}
