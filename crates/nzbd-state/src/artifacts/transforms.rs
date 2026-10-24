//! Operation-owned PAR/extraction workspaces, in the existing custody journal.
use super::*;

fn digest_open(file: &mut File) -> Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::{Read, Seek};
    let before = fs::identity_for_file(file)?;
    file.rewind()?;
    let mut digest = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        digest.update(&buffer[..n]);
    }
    file.rewind()?;
    if fs::identity_for_file(file)? != before {
        return Err(Error::Conflict(
            "file changed while verifying owned rename".into(),
        ));
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn rename_transition_matches(before: &Identity, after: &Identity) -> bool {
    before.same_object(after) && before.bytes == after.bytes && before.modified == after.modified
}

fn digest_file(root: &File, path: &str, expected: &Identity) -> Result<String> {
    digest_file_with_checkpoint(root, path, expected, || Ok(()))
}

fn digest_file_with_checkpoint(
    root: &File,
    path: &str,
    expected: &Identity,
    mut checkpoint: impl FnMut() -> std::io::Result<()>,
) -> Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let mut file = fs::open_relative(root, path)?;
    if fs::identity_for_file(&file)? != *expected {
        return Err(Error::Conflict(format!(
            "transform output identity changed before digest: {path}"
        )));
    }
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        checkpoint()?;
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    if fs::identity_for_file(&file)? != *expected {
        return Err(Error::Conflict(format!(
            "transform output identity changed while hashing: {path}"
        )));
    }
    Ok(format!("{:x}", digest.finalize()))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Workspace {
    #[serde(default)]
    pub keep_provenance: bool,
    #[serde(default)]
    pub logical_key: Option<String>,
    pub operation_id: String,
    #[serde(default)]
    pub input_path: Option<String>,
    #[serde(default)]
    pub input_paths: Vec<String>,
    pub source: Artifact,
    pub scratch: Artifact,
    #[serde(default)]
    pub retained: std::collections::HashMap<String, String>,
    /// Content proof for operation-owned original-file renames. Rename changes
    /// inode ctime on common filesystems; this proof permits only that journaled
    /// transition while keeping ordinary identity comparisons strict.
    #[serde(default)]
    pub retained_digests: std::collections::HashMap<String, String>,
    #[serde(default)]
    pub attempts: Vec<WorkspaceAttempt>,
    #[serde(default)]
    pub published_files: Option<Vec<FileEntry>>,
    /// Source-relative outputs that a verified PAR repair wrote back to the
    /// processing generation. These are captured only after validating the
    /// PAR catalog digest and copying the output into the source tree.
    #[serde(default)]
    pub verified_output_paths: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkspaceAttempt {
    pub directory: String,
    pub identity: Option<Identity>,
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
        let logical_key = format!("{kind}-{}-{}-{token}", source.id, source.generation);
        let raw: Option<String> = self.db.lock().unwrap().query_row(
            "SELECT data FROM operations WHERE (id=?1 OR json_extract(json_extract(data,'$.request'),'$.logical_key')=?1) ORDER BY rowid DESC LIMIT 1",
            [&logical_key], |row| row.get(0),
        ).optional()?;
        let mut key = logical_key.clone();
        if let Some(raw) = raw {
            let op: Operation = serde_json::from_str(&raw)?;
            let workspace: Workspace = serde_json::from_str(&op.request)?;
            let scratch = self.get(&workspace.scratch.id)?;
            let pending: bool = self.db.lock().unwrap().query_row(
                "SELECT EXISTS(SELECT 1 FROM operations WHERE artifact=?1 AND state IN ('queued','running','retry','succeeded') AND json_extract(data,'$.kind') IN ('delete','retire_workspace'))",
                [&scratch.id], |row| row.get(0),
            )?;
            if scratch.terminal()
                || matches!(scratch.state.as_str(), "deleting" | "delete_failed")
                || pending
                || op.state == "failed"
            {
                key = format!("{logical_key}-{}", id(&self.db.lock().unwrap())?);
            } else {
                if op.state == "review"
                    || scratch
                        .hold
                        .as_deref()
                        .is_some_and(|h| h != "transform workspace")
                {
                    return Err(Error::Conflict("workspace requires review".into()));
                }
                self.verify(&scratch)?;
                self.verify_workspace_attempts(&workspace)?;
                return Ok(workspace);
            }
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
        scratch.root_identity = fs::identity_for_file(&workspace_dir)?;
        scratch.path = path.clone();
        scratch.identity = None;
        scratch.files.clear();
        scratch.state = "allocating".into();
        scratch.keep = false;
        scratch.retention_seconds = 0;
        scratch.eligible_seconds = 0;
        scratch.deadline = None;
        scratch.owned = true;
        scratch.hold = Some("transform workspace".into());
        let mut workspace = Workspace {
            keep_provenance: true,
            logical_key: Some(logical_key),
            operation_id: key.clone(),
            input_path: None,
            input_paths: Vec::new(),
            source: source.clone(),
            scratch,
            retained: Default::default(),
            retained_digests: Default::default(),
            attempts: Vec::new(),
            published_files: None,
            verified_output_paths: Vec::new(),
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
        workspace.scratch.identity = Some(fs::identity_for_file(&scratch_dir)?);
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

    /// Bind the exact source-relative archive name to this transform journal.
    /// This is evidence only; it becomes a consumed input only when the same
    /// operation later records a successfully published extraction.
    pub fn bind_extraction_input(&self, workspace: &mut Workspace, input: &Path) -> Result<()> {
        self.bind_extraction_inputs(workspace, &[input.to_path_buf()])
    }

    /// Bind all files consumed by a successful extraction, including every
    /// volume of a multi-part archive. The set is immutable for a transform
    /// operation and persisted before extraction starts.
    pub fn bind_extraction_inputs(
        &self,
        workspace: &mut Workspace,
        inputs: &[PathBuf],
    ) -> Result<()> {
        if inputs.is_empty() {
            return Err(Error::Conflict("extraction input set is empty".into()));
        }
        let mut relatives = inputs
            .iter()
            .map(|input| {
                let relative = input.strip_prefix(&workspace.source.path).map_err(|_| {
                    Error::Conflict("extraction input is outside its source generation".into())
                })?;
                relative
                    .components()
                    .map(|part| match part {
                        std::path::Component::Normal(s) => s.to_str().map(str::to_owned),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()
                    .filter(|parts| !parts.is_empty())
                    .map(|parts| parts.join("/"))
                    .ok_or_else(|| Error::Conflict("unsupported extraction input path".into()))
            })
            .collect::<Result<Vec<_>>>()?;
        relatives.sort();
        relatives.dedup();
        let relative = relatives[0].clone();
        let _guard = self.mutation_guard()?;
        let mut op = self.operation(&workspace.operation_id)?;
        if workspace.input_paths == relatives && workspace.input_path.as_deref() == Some(&relative)
        {
            return Ok(());
        }
        if op.kind != "extract" || op.state != "running" {
            return Err(Error::Conflict("extraction journal is not active".into()));
        }
        let source = self.get(&workspace.source.id)?;
        if source.generation != workspace.source.generation
            || relatives.iter().any(|path| {
                !source
                    .files
                    .iter()
                    .any(|entry| entry.path == *path && !entry.identity.directory)
            })
        {
            return Err(Error::Conflict(
                "extraction input is absent from the source manifest".into(),
            ));
        }
        if !workspace.input_paths.is_empty() && workspace.input_paths != relatives {
            return Err(Error::Conflict(
                "extraction journal input set changed".into(),
            ));
        }
        if workspace
            .input_path
            .as_deref()
            .is_some_and(|existing| existing != relative)
        {
            return Err(Error::Conflict("extraction journal input changed".into()));
        }
        workspace.input_path = Some(relative);
        workspace.input_paths = relatives;
        op.request = serde_json::to_string(workspace)?;
        save_operation(&self.db.lock().unwrap(), &op)
    }

    /// Internal scratch is owned by a transform, not by a queue job. Startup
    /// must establish that association before applying orphan-job recovery.
    pub(super) fn is_transform_scratch(&self, artifact: &Artifact) -> Result<bool> {
        if artifact.job.is_some() {
            return Ok(false);
        }
        let raw: Option<String> = self.db.lock().unwrap().query_row(
            "SELECT data FROM operations WHERE json_extract(data,'$.kind') IN ('extract','par_repair') AND json_extract(json_extract(data,'$.request'),'$.scratch.id')=?1 AND json_extract(json_extract(data,'$.request'),'$.scratch.generation')=?2 LIMIT 1",
            params![artifact.id, artifact.generation], |row| row.get(0),
        ).optional()?;
        let Some(raw) = raw else {
            return Ok(false);
        };
        let operation: Operation = serde_json::from_str(&raw)?;
        let workspace: Workspace = serde_json::from_str(&operation.request)?;
        Ok(workspace.scratch.path == artifact.path
            && workspace.scratch.root == artifact.root
            && workspace.scratch.identity == artifact.identity
            && workspace
                .scratch
                .root_identity
                .same_object(&artifact.root_identity))
    }

    fn verify_workspace_attempts(&self, workspace: &Workspace) -> Result<()> {
        let root = self.verify(&workspace.scratch)?;
        for attempt in &workspace.attempts {
            let expected = attempt.identity.as_ref().ok_or_else(|| {
                Error::Conflict("interrupted extraction attempt allocation requires review".into())
            })?;
            let observed = fs::open_relative(&root, &attempt.directory)?;
            if !expected.same_object(&fs::identity_for_file(&observed)?) {
                return Err(Error::Conflict(
                    "extraction attempt identity changed".into(),
                ));
            }
        }
        Ok(())
    }

    /// Journal a new extraction generation; prior bytes are never reused or deleted.
    pub fn begin_workspace_attempt(&self, workspace: &mut Workspace) -> Result<PathBuf> {
        let _guard = self.mutation_guard()?;
        let mut op = self.operation(&workspace.operation_id)?;
        let mut current: Workspace = serde_json::from_str(&op.request)?;
        self.verify_workspace_attempts(&current)?;
        if !matches!(op.kind.as_str(), "extract" | "par_repair")
            || !(op.state == "running"
                || (op.state == "succeeded" && current.published_files.is_none()))
            || current.attempts.len() >= 64
        {
            return Err(Error::Conflict("extraction attempt requires review".into()));
        }
        op.state = "running".into();
        let directory = format!("attempt-{}", current.attempts.len() + 1);
        current.attempts.push(WorkspaceAttempt {
            directory: directory.clone(),
            identity: None,
        });
        op.request = serde_json::to_string(&current)?;
        save_operation(&self.db.lock().unwrap(), &op)?;
        let path = current.scratch.path.join(&directory);
        std::fs::create_dir(&path)?;
        fs::sync_directory(&self.verify(&current.scratch)?)?;
        current.attempts.last_mut().unwrap().identity =
            Some(fs::identity_for_file(&fs::open_dir(&path)?)?);
        op.request = serde_json::to_string(&current)?;
        op.attempts = current.attempts.len() as u32;
        save_operation(&self.db.lock().unwrap(), &op)?;
        *workspace = current;
        Ok(path)
    }

    /// Revalidate the captured source generation and root before transform work.
    pub fn validate_workspace_source(&self, workspace: &Workspace) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let source = self.get(&workspace.source.id)?;
        if source.generation != workspace.source.generation
            || source.path != workspace.source.path
            || source.hold.is_some()
            || !matches!(source.state.as_str(), "active" | "transitioning")
        {
            return Err(Error::Conflict(
                "transform source generation or authority changed".into(),
            ));
        }
        self.verify(&source)?;
        Ok(())
    }

    pub fn retain_transform_original(
        &self,
        workspace: &mut Workspace,
        relative: &str,
    ) -> Result<()> {
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
        let root = fs::open_dir(&workspace.source.path)?;
        let original = fs::open_relative(&root, relative)?;
        let original_identity = fs::identity_for_file(&original)?;
        if original_identity != entry.identity {
            return Err(Error::Conflict("original identity changed".into()));
        }
        // Hash through a stable descriptor before taking the global mutation
        // guard. Large media files must not block unrelated custody controls.
        let digest = digest_open(&mut original.try_clone()?)?;
        let _guard = self.mutation_guard()?;
        let current = self.get(&workspace.source.id)?;
        if current.generation != workspace.source.generation
            || current.path != workspace.source.path
            || current.terminal()
        {
            return Err(Error::Conflict(
                "transform source generation changed".into(),
            ));
        }
        let mut op = self.operation(&workspace.operation_id)?;
        let latest_workspace: Workspace = serde_json::from_str(&op.request)?;
        if latest_workspace.source.generation != workspace.source.generation
            || latest_workspace.operation_id != workspace.operation_id
        {
            return Err(Error::Conflict("transform operation changed".into()));
        }
        if fs::identity_for_file(&original)? != original_identity {
            return Err(Error::Conflict("original changed while hashing".into()));
        }
        if let Ok(old) = fs::open_relative(&root, &backup) {
            let observed = fs::identity_for_file(&old)?;
            let expected_digest = workspace
                .retained_digests
                .get(relative)
                .ok_or_else(|| Error::Conflict("retained original proof missing".into()))?;
            if rename_transition_matches(&entry.identity, &observed)
                && digest_open(&mut old.try_clone()?)? == *expected_digest
            {
                if let Some(captured) = workspace
                    .source
                    .files
                    .iter_mut()
                    .find(|f| f.path == relative)
                {
                    captured.identity = observed;
                }
                workspace.retained.insert(relative.into(), backup);
                op.request = serde_json::to_string(workspace)?;
                save_operation(&self.db.lock().unwrap(), &op)?;
                return Ok(());
            }
            return Err(Error::Conflict("retained original identity changed".into()));
        }
        workspace.retained.insert(relative.into(), backup.clone());
        workspace.retained_digests.insert(relative.into(), digest);
        op.request = serde_json::to_string(workspace)?;
        save_operation(&self.db.lock().unwrap(), &op)?;
        fs::rename_exclusive(
            &workspace.source.path.join(relative),
            &workspace.source.path.join(&backup),
        )?;
        let renamed = fs::open_relative(&root, &backup)?;
        let observed = fs::identity_for_file(&renamed)?;
        if !rename_transition_matches(&entry.identity, &observed) {
            return Err(Error::Conflict(
                "retained original changed during rename".into(),
            ));
        }
        if let Some(captured) = workspace
            .source
            .files
            .iter_mut()
            .find(|f| f.path == relative)
        {
            captured.identity = observed;
        }
        op.request = serde_json::to_string(workspace)?;
        save_operation(&self.db.lock().unwrap(), &op)?;
        Ok(())
    }

    pub fn finish_workspace(&self, workspace: &Workspace) -> Result<()> {
        self.finish_workspace_output(workspace, None, true)
    }

    /// Finish a verified PAR repair while durably recording the exact
    /// source-relative output files whose repaired bytes were validated
    /// against the PAR catalog. This proof is generation-bound and is
    /// rechecked by `verified_transform_evidence` before publication.
    pub fn finish_repair_workspace(
        &self,
        workspace: &Workspace,
        verified_output_paths: &[String],
    ) -> Result<()> {
        if verified_output_paths.is_empty() {
            return Err(Error::Conflict("verified repair has no outputs".into()));
        }
        let mut workspace = workspace.clone();
        workspace.verified_output_paths = verified_output_paths.to_vec();
        // Hash large repaired media before entering the inventory mutation
        // lock. The subsequent journal commit rechecks each captured identity
        // while holding that lock.
        let root = fs::open_dir(&workspace.source.path)?;
        let mut proofs = Vec::with_capacity(verified_output_paths.len());
        for path in verified_output_paths {
            let relative = Path::new(path);
            if relative.is_absolute()
                || relative
                    .components()
                    .any(|part| !matches!(part, std::path::Component::Normal(_)))
            {
                return Err(Error::Conflict(
                    "invalid verified repair output path".into(),
                ));
            }
            let file = fs::open_relative(&root, path)?;
            let identity = fs::identity_for_file(&file)?;
            if identity.directory {
                return Err(Error::Conflict("repair output is not a file".into()));
            }
            let digest = digest_file(&root, path, &identity)?;
            proofs.push(FileEntry {
                path: path.clone(),
                identity,
                digest: Some(digest),
            });
        }
        workspace.published_files = Some(proofs);
        self.finish_workspace_output(&workspace, None, true)
    }

    pub fn finish_extraction(&self, workspace: &Workspace, output: &Path) -> Result<()> {
        self.finish_workspace_output(workspace, Some(output), true)
    }

    /// Release an exhausted repair attempt without publishing scratch output.
    /// Original identities are checked before normal failure disposition resumes.
    pub fn abandon_workspace(&self, workspace: &Workspace) -> Result<()> {
        self.finish_workspace_output(workspace, None, false)
    }

    pub fn abandon_repair_workspace(&self, workspace: &Workspace) -> Result<()> {
        let op = self.operation(&workspace.operation_id)?;
        if op.kind != "par_repair"
            || !workspace.retained.is_empty()
            || workspace.published_files.is_some()
            || op.state == "succeeded"
        {
            return Err(Error::Conflict(
                "repair has already published output".into(),
            ));
        }
        self.finish_workspace_output(workspace, None, false)
    }

    /// A successful extraction can be reused after a later stage fails. Names
    /// may have been journal-renamed, but every output must retain its complete
    /// identity in the current owned manifest AND on disk.
    pub fn extraction_published(&self, workspace: &Workspace) -> Result<bool> {
        let _guard = self.mutation_guard()?;
        let op = self.operation(&workspace.operation_id)?;
        if op.kind != "extract" || op.state != "succeeded" {
            return Ok(false);
        }
        let Some(outputs) = &workspace.published_files else {
            return Ok(false);
        };
        let source = self.get(&workspace.source.id)?;
        if source.generation != workspace.source.generation
            || source.terminal()
            || source.hold.is_some()
        {
            return Err(Error::Conflict(
                "extraction no longer owns this generation".into(),
            ));
        }
        let root = self.verify(&source)?;
        for expected in outputs {
            let entry = source
                .files
                .iter()
                .find(|f| f.identity == expected.identity)
                .ok_or_else(|| Error::Conflict("published extraction output changed".into()))?;
            if fs::identity_for_file(&fs::open_relative(&root, &entry.path)?)? != expected.identity
            {
                return Err(Error::Conflict(
                    "published extraction output changed".into(),
                ));
            }
        }
        Ok(true)
    }

    /// Return only extraction outputs whose successful transform journal and
    /// current source identities still prove them. This is used by delivery
    /// salvage after a later archive or script failed; an extension alone is
    /// never integrity evidence.
    pub fn verified_transform_evidence(
        &self,
        job: u32,
    ) -> Result<(
        std::collections::HashSet<String>,
        std::collections::HashSet<String>,
    )> {
        let Some(source) = self.for_job(job)? else {
            return Ok(Default::default());
        };
        let raws = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare(
                "SELECT data FROM operations WHERE artifact=?1 AND state='succeeded' AND json_extract(data,'$.kind') IN ('extract','par_repair')",
            )?;
            let rows = statement.query_map([&source.id], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let mut inputs = std::collections::HashSet::new();
        let mut outputs = std::collections::HashSet::new();
        let root = self.verify(&source)?;
        let observed = fs::manifest(&root, 100_000)?;
        for raw in raws {
            let operation: Operation = serde_json::from_str(&raw)?;
            let workspace: Workspace = serde_json::from_str(&operation.request)?;
            if workspace.source.generation != source.generation
                || operation.state != "succeeded"
                || !matches!(operation.kind.as_str(), "extract" | "par_repair")
            {
                continue;
            }
            if operation.kind == "extract" {
                inputs.extend(workspace.input_paths.iter().cloned());
                if let Some(path) = &workspace.input_path {
                    inputs.insert(path.clone());
                }
            }
            for output in workspace.published_files.as_deref().unwrap_or_default() {
                if let Some(current) = observed
                    .iter()
                    .find(|entry| entry.identity == output.identity && !entry.identity.directory)
                {
                    if fs::open_relative(&root, &current.path)
                        .and_then(|file| fs::identity_for_file(&file))
                        .is_ok_and(|identity| identity == output.identity)
                    {
                        outputs.insert(current.path.clone());
                    }
                }
            }
            // A successful repair may rewrite the damaged file in place.
            // Such outputs cannot match the pre-repair manifest identity, so
            // their exact paths were recorded only after PAR digest validation
            // and are checked against the current generation here.
            if operation.kind == "par_repair" {
                let Some(proofs) = workspace.published_files.as_ref() else {
                    continue;
                };
                for path in workspace.verified_output_paths {
                    let Some(proof) = proofs
                        .iter()
                        .find(|entry| entry.path == path && !entry.identity.directory)
                    else {
                        continue;
                    };
                    let matches_identity = observed
                        .iter()
                        .find(|entry| entry.path == path && !entry.identity.directory)
                        .is_some_and(|current| current.identity == proof.identity);
                    let matches_digest = proof.digest.as_deref().is_some_and(|expected| {
                        digest_file(&root, &path, &proof.identity)
                            .is_ok_and(|actual| actual == expected)
                    });
                    if matches_identity && matches_digest {
                        outputs.insert(path.clone());
                    }
                }
            }
        }
        Ok((inputs, outputs))
    }

    /// Return journaled filename restorations that still match the same
    /// source generation, controlled rename identity transition, and exact
    /// post-rename digest. This proves provenance for a renamed path only; a
    /// caller must still establish payload integrity (for example, by
    /// matching the finalized NZB whole-file CRC) before publishing it.
    pub fn verified_restore_paths(
        &self,
        job: u32,
    ) -> Result<std::collections::HashMap<String, String>> {
        self.verified_restore_paths_with_checkpoint(job, || Ok(()))
    }

    pub fn verified_restore_paths_with_checkpoint(
        &self,
        job: u32,
        mut checkpoint: impl FnMut() -> std::io::Result<()>,
    ) -> Result<std::collections::HashMap<String, String>> {
        let Some(source) = self.for_job(job)? else {
            return Ok(Default::default());
        };
        let raws = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare(
                "SELECT data FROM operations WHERE artifact=?1 AND state='succeeded' AND json_extract(data,'$.kind')='par_restore'",
            )?;
            let rows = statement.query_map([&source.id], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let root = self.verify(&source)?;
        let mut proofs = Vec::new();
        for raw in raws {
            let operation: Operation = serde_json::from_str(&raw)?;
            if operation.kind != "par_restore" || operation.state != "succeeded" {
                continue;
            }
            let raw_request: serde_json::Value = serde_json::from_str(&operation.request)?;
            if raw_request
                .as_array()
                .is_some_and(|fields| fields.len() == 4)
            {
                // Pre-proof restore journals cannot establish either a
                // controlled ctime transition or the bytes' original content.
                continue;
            }
            let (recorded_source, old, new, original, expected_digest): (
                Artifact,
                PathBuf,
                PathBuf,
                Identity,
                String,
            ) = serde_json::from_str(&operation.request)?;
            if recorded_source.id != source.id
                || recorded_source.generation != source.generation
                || recorded_source.path != source.path
            {
                continue;
            }
            let (Some(old), Some(new)) = (old.to_str(), new.to_str()) else {
                continue;
            };
            let safe_relative = |path: &str| {
                let path = Path::new(path);
                !path.as_os_str().is_empty()
                    && !path.is_absolute()
                    && path
                        .components()
                        .all(|part| matches!(part, std::path::Component::Normal(_)))
            };
            if !safe_relative(old) || !safe_relative(new) {
                continue;
            }
            proofs.push((old.to_string(), new.to_string(), original, expected_digest));
        }
        let mut restored = std::collections::HashMap::new();
        for (old, new, original, expected_digest) in &proofs {
            let Some(current) = source.files.iter().find(|entry| entry.path == *new) else {
                continue;
            };
            if current.identity.directory
                || !rename_transition_matches(original, &current.identity)
                || digest_file_with_checkpoint(&root, new, &current.identity, &mut checkpoint)?
                    != *expected_digest
            {
                continue;
            }
            // A second owned rename may move an already restored name. Follow
            // only journal edges whose bytes and file identity chain remain
            // the same; intermediate path absence is expected after rename.
            let mut oldest = old.as_str();
            let mut child_identity = original;
            let mut seen = std::collections::HashSet::new();
            loop {
                restored.insert(oldest.to_string(), new.clone());
                let predecessor =
                    proofs
                        .iter()
                        .find(|(_, candidate_new, candidate_identity, digest)| {
                            candidate_new == oldest
                                && digest == expected_digest
                                && rename_transition_matches(candidate_identity, child_identity)
                        });
                let Some((predecessor_old, _, predecessor_identity, _)) = predecessor else {
                    break;
                };
                if predecessor_old == oldest || !seen.insert(oldest.to_string()) {
                    break;
                }
                oldest = predecessor_old;
                child_identity = predecessor_identity;
            }
        }
        Ok(restored)
    }

    fn finish_workspace_output(
        &self,
        workspace: &Workspace,
        output: Option<&Path>,
        succeeded: bool,
    ) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let mut source = self.get(&workspace.source.id)?;
        if source.generation != workspace.source.generation {
            return Err(Error::Conflict(
                "transform source generation changed".into(),
            ));
        }
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
            if fs::identity_for_file(&observed)? != entry.identity {
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
        if let Some(output) = output {
            let output_root = fs::open_dir(output)?;
            let mut finished = workspace.clone();
            finished.published_files = Some(
                fs::manifest(&output_root, 100_000)?
                    .iter()
                    .filter(|e| !e.identity.directory)
                    .map(|e| {
                        let identity = fs::identity_for_file(&fs::open_relative(&dir, &e.path)?)?;
                        Ok(FileEntry {
                            path: e.path.clone(),
                            identity,
                            digest: None,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
            op.request = serde_json::to_string(&finished)?;
        } else if succeeded && !workspace.verified_output_paths.is_empty() {
            let observed = fs::manifest(&dir, 100_000)?;
            let finished = workspace.clone();
            let proofs = finished.published_files.as_ref().ok_or_else(|| {
                Error::Conflict("verified repair output has no durable byte proof".into())
            })?;
            if proofs.len() != finished.verified_output_paths.len()
                || proofs.iter().any(|proof| {
                    !finished.verified_output_paths.contains(&proof.path)
                        || proof.digest.as_ref().is_none_or(String::is_empty)
                        || !observed.iter().any(|entry| {
                            entry.path == proof.path && entry.identity == proof.identity
                        })
                })
            {
                return Err(Error::Conflict(
                    "verified repair output changed before journal commit".into(),
                ));
            }
            op.request = serde_json::to_string(&finished)?;
        }
        op.state = if succeeded { "succeeded" } else { "failed" }.into();
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &source)?;
        save_artifact(&tx, &scratch)?;
        save_operation(&tx, &op)?;
        event(
            &tx,
            &source.id,
            if succeeded {
                "transform_published"
            } else {
                "transform_exhausted"
            },
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
        let mut source = self.for_job(job)?.ok_or(Error::NotFound)?;
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
        let root = self.verify(&source)?;
        let file = fs::open_relative(&root, &old.to_string_lossy())?;
        let identity = fs::identity_for_file(&file)?;
        // Keep the potentially large content pass outside the global mutation
        // lock, then revalidate the exact source generation and identity.
        let digest = digest_open(&mut file.try_clone()?)?;
        let _guard = self.mutation_guard()?;
        let current = self.for_job(job)?.ok_or(Error::NotFound)?;
        if current.id != source.id
            || current.generation != source.generation
            || current.path != source.path
        {
            return Err(Error::Conflict("restore source generation changed".into()));
        }
        source = current;
        let root = self.verify(&source)?;
        let current_file = fs::open_relative(&root, &old.to_string_lossy())?;
        if fs::identity_for_file(&current_file)? != identity {
            return Err(Error::Conflict(
                "restore source changed while hashing".into(),
            ));
        }
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
            request: serde_json::to_string(&(source.clone(), old, new, &identity, &digest))?,
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
        let observed_identity = fs::identity_for_file(&observed)?;
        if !rename_transition_matches(&identity, &observed_identity) {
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
        let mut guard = Some(self.mutation_guard()?);
        let raws = {
            let db = self.db.lock().unwrap();
            let mut statement=db.prepare("SELECT data FROM operations WHERE (state='running' OR (state='retry' AND COALESCE(json_extract(data,'$.next_retry'),0)<=?1)) AND json_extract(data,'$.kind') IN ('par_restore','par_repair','extract')")?;
            let rows = statement.query_map([now()], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in raws {
            let mut op: Operation = serde_json::from_str(&raw)?;
            if op.state == "retry" {
                op.state = "running".into();
                op.next_retry = 0;
                save_operation(&self.db.lock().unwrap(), &op)?;
            }
            let current = self.get(&op.artifact)?;
            let source_generation = if op.kind == "par_restore" {
                let tuple: serde_json::Value = serde_json::from_str(&op.request)?;
                if tuple.as_array().is_some_and(|fields| fields.len() == 4) {
                    // Old journals did not carry content proof for the
                    // ctime-changing rename transition. Preserve the files
                    // and require explicit review instead of failing startup
                    // or inventing authority from today's bytes.
                    op.state = "review".into();
                    op.error = Some("legacy restore journal lacks rename content proof".into());
                    save_operation(&self.db.lock().unwrap(), &op)?;
                    continue;
                }
                let (source, _, _, _, _): (Artifact, PathBuf, PathBuf, Identity, String) =
                    serde_json::from_value(tuple)?;
                source.generation
            } else {
                let workspace: Workspace = serde_json::from_str(&op.request)?;
                workspace.source.generation
            };
            if current.terminal()
                || current.state == "deleting"
                || current.generation != source_generation
            {
                op.state = "cancelled".into();
                op.error = Some("transform no longer owns the source generation".into());
                save_operation(&self.db.lock().unwrap(), &op)?;
                continue;
            }
            let result = (|| -> Result<()> {
                if op.kind == "par_restore" {
                    let (mut source, old, new, identity, digest): (
                        Artifact,
                        PathBuf,
                        PathBuf,
                        Identity,
                        String,
                    ) = serde_json::from_str(&op.request)?;
                    let root = self.verify(&source)?;
                    let before = fs::open_relative(&root, &old.to_string_lossy());
                    let after = fs::open_relative(&root, &new.to_string_lossy());
                    let before_identity = match &before {
                        Ok(file) => Some(fs::identity_for_file(file)?),
                        Err(_) => None,
                    };
                    let after_identity = match &after {
                        Ok(file) => Some(fs::identity_for_file(file)?),
                        Err(_) => None,
                    };
                    let needs_hash = matches!(
                        (&before, &after),
                        (Ok(_), Ok(_)) | (Ok(_), Err(_)) | (Err(_), Ok(_))
                    );
                    let (before_digest, after_digest) = if needs_hash {
                        drop(guard.take());
                        let before_digest = before
                            .as_ref()
                            .ok()
                            .map(|file| digest_open(&mut file.try_clone()?))
                            .transpose()?;
                        let after_digest = after
                            .as_ref()
                            .ok()
                            .map(|file| digest_open(&mut file.try_clone()?))
                            .transpose()?;
                        guard = Some(self.mutation_guard()?);
                        let current_op = self.operation(&op.id)?;
                        let current_source = self.get(&op.artifact)?;
                        let before_rechecked = match &before {
                            Ok(file) => Some(fs::identity_for_file(file)?),
                            Err(_) => None,
                        };
                        let after_rechecked = match &after {
                            Ok(file) => Some(fs::identity_for_file(file)?),
                            Err(_) => None,
                        };
                        if current_op.state != "running"
                            || current_source.generation != source.generation
                            || current_source.path != source.path
                            || before_rechecked != before_identity
                            || after_rechecked != after_identity
                        {
                            return Err(Error::Conflict(
                                "restore authority changed while verifying content".into(),
                            ));
                        }
                        (before_digest, after_digest)
                    } else {
                        (None, None)
                    };
                    match (before, after) {
                        (Ok(_), Ok(_))
                            if before_identity.as_ref() == Some(&identity)
                                && after_identity.as_ref().is_some_and(|observed| {
                                    rename_transition_matches(&identity, observed)
                                })
                                && after_digest.as_deref() == Some(digest.as_str()) =>
                        {
                            fs::rename_exclusive(&source.path.join(&old), &source.path.join(&new))?;
                        }
                        (Ok(_file), Err(Error::Io(e)))
                            if e.kind() == std::io::ErrorKind::NotFound
                                && before_identity.as_ref() == Some(&identity)
                                && before_digest.as_deref() == Some(digest.as_str()) =>
                        {
                            if let Some(parent) = new.parent() {
                                crate::fileops::parents(&source.path, parent)?;
                            }
                            fs::rename_exclusive(&source.path.join(&old), &source.path.join(&new))?;
                        }
                        (Err(Error::Io(e)), Ok(_file))
                            if e.kind() == std::io::ErrorKind::NotFound
                                && after_identity.as_ref().is_some_and(|observed| {
                                    rename_transition_matches(&identity, observed)
                                })
                                && after_digest.as_deref() == Some(digest.as_str()) => {}
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
                    let mut workspace: Workspace = serde_json::from_str(&op.request)?;
                    self.verify(&workspace.scratch)?;
                    self.verify_workspace_attempts(&workspace)?;
                    let root = self.verify(&workspace.source)?;
                    for entry in workspace
                        .source
                        .files
                        .iter_mut()
                        .filter(|e| !e.identity.directory)
                    {
                        let path = workspace
                            .retained
                            .get(&entry.path)
                            .unwrap_or(&entry.path)
                            .clone();
                        let file = fs::open_relative(&root, &path)?;
                        let observed = fs::identity_for_file(&file)?;
                        if observed != entry.identity {
                            let proof = workspace.retained_digests.get(&entry.path);
                            let digest_matches = if let Some(digest) = proof {
                                drop(guard.take());
                                let actual = digest_open(&mut file.try_clone()?)?;
                                guard = Some(self.mutation_guard()?);
                                let current_op = self.operation(&op.id)?;
                                let current_source = self.get(&op.artifact)?;
                                if current_op.state != "running"
                                    || current_source.generation != workspace.source.generation
                                    || fs::identity_for_file(&file)? != observed
                                {
                                    return Err(Error::Conflict(
                                        "transform authority changed while verifying content"
                                            .into(),
                                    ));
                                }
                                actual == *digest
                            } else {
                                false
                            };
                            if !workspace.retained.contains_key(&entry.path)
                                || !rename_transition_matches(&entry.identity, &observed)
                                || !digest_matches
                            {
                                return Err(Error::Conflict(
                                    "interrupted transform input changed".into(),
                                ));
                            }
                            entry.identity = observed;
                        }
                    }
                    op.request = serde_json::to_string(&workspace)?;
                    save_operation(&self.db.lock().unwrap(), &op)?;
                }
                Ok(())
            })();
            if let Err(error) = result {
                if guard.is_none() {
                    guard = Some(self.mutation_guard()?);
                }
                op.error = Some(error.to_string());
                if matches!(error, Error::Io(_)) {
                    op.attempts = op.attempts.saturating_add(1);
                    let delay = [30, 60, 120, 300][op.attempts.saturating_sub(1).min(3) as usize];
                    op.state = "retry".into();
                    op.next_retry = now().saturating_add(delay);
                    save_operation(&self.db.lock().unwrap(), &op)?;
                } else {
                    op.state = "review".into();
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
        }
        Ok(())
    }
}

impl Inventory {
    /// Validate custody before an operator retries a held PP job. This does not
    /// release an identity or relocation hold, adopt bytes, or delete an attempt.
    pub fn validate_post_retry(&self, job: u32) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let source = self.for_job(job)?.ok_or(Error::NotFound)?;
        if self.source_in_use(&SourceGeneration {
            artifact: source.id.clone(),
            generation: source.generation.clone(),
        }) {
            return Err(Error::Conflict(
                "previous processing workers have not quiesced".into(),
            ));
        }
        if !source.owned
            || source.hold.is_some()
            || !matches!(
                source.state.as_str(),
                "active" | "transitioning" | "retained"
            )
        {
            return Err(Error::Conflict(
                "payload custody must be resolved before retrying post-processing".into(),
            ));
        }
        let root = self.verify(&source)?;
        for entry in source.files.iter().filter(|e| !e.identity.directory) {
            let file = fs::open_relative(&root, &entry.path)?;
            if fs::identity_for_file(&file)? != entry.identity {
                return Err(Error::Conflict(
                    "post-processing input changed; review required".into(),
                ));
            }
        }
        let pending: i64 = self.db.lock().unwrap().query_row(
            "SELECT COUNT(*) FROM operations WHERE artifact=?1 AND state IN ('running','review','queued','retry') AND json_extract(data,'$.kind') IN ('relocate','delete')",
            [&source.id], |r| r.get(0),
        )?;
        if pending != 0 {
            return Err(Error::Conflict(
                "resolve pending relocation or deletion before retrying post-processing".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn startup_preserves_transform_scratch_custody_and_protections() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("processing");
        std::fs::create_dir(&root).unwrap();
        let state = temp.path().join("state");
        let inventory = Inventory::open(&state).unwrap();
        let source = root.join("job");
        inventory.allocate(5, &root, &source).unwrap();
        std::fs::write(source.join("archive.rar"), b"original").unwrap();
        let workspace = inventory.workspace(5, "extract", "abc").unwrap();
        std::fs::write(workspace.scratch.path.join("partial"), b"scratch").unwrap();
        let scratch = inventory.get(&workspace.scratch.id).unwrap();
        assert!(inventory.is_transform_scratch(&scratch).unwrap());
        let mut changed = scratch.clone();
        changed.generation = "unrelated-generation".into();
        assert!(!inventory.is_transform_scratch(&changed).unwrap());
        changed = scratch.clone();
        changed.path = root.join("same-name-is-not-custody");
        assert!(!inventory.is_transform_scratch(&changed).unwrap());
        drop(inventory);

        let inventory = Inventory::open(&state).unwrap();
        inventory.reconcile_startup(&[5]).unwrap();
        let recovered = inventory.get(&scratch.id).unwrap();
        assert_eq!(recovered.state, "active");
        assert_eq!(recovered.keep, scratch.keep);
        assert_eq!(recovered.hold, scratch.hold);
        assert_eq!(recovered.generation, scratch.generation);
        assert_eq!(
            std::fs::read(recovered.path.join("partial")).unwrap(),
            b"scratch"
        );
        assert_eq!(
            std::fs::read(source.join("archive.rar")).unwrap(),
            b"original"
        );
    }

    #[test]
    fn extraction_retry_generations_survive_restart_and_reject_changed_identity() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("downloads");
        std::fs::create_dir(&root).unwrap();
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        let source = root.join("job");
        inventory.allocate(5, &root, &source).unwrap();
        std::fs::write(source.join("archive.rar"), b"original").unwrap();
        let mut workspace = inventory.workspace(5, "extract", "abc").unwrap();
        let first = inventory.begin_workspace_attempt(&mut workspace).unwrap();
        std::fs::write(first.join("partial.bin"), b"retained partial").unwrap();
        drop(inventory);
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        inventory.reconcile_startup(&[5]).unwrap();
        let mut workspace = inventory.workspace(5, "extract", "abc").unwrap();
        let second = inventory.begin_workspace_attempt(&mut workspace).unwrap();
        assert_ne!(first, second);
        assert_eq!(
            std::fs::read(first.join("partial.bin")).unwrap(),
            b"retained partial"
        );
        assert_eq!(workspace.attempts.len(), 2);
        std::fs::rename(&second, second.with_extension("retained")).unwrap();
        std::fs::create_dir(&second).unwrap();
        assert!(inventory.begin_workspace_attempt(&mut workspace).is_err());
    }
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

    #[test]
    fn verified_par_repair_output_survives_restart_as_generation_bound_evidence() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("downloads");
        std::fs::create_dir(&root).unwrap();
        let state = temp.path().join("state");
        let inventory = Inventory::open(&state).unwrap();
        let source = root.join("job");
        inventory
            .allocate_for_delivery(5, &root, &source, true, true)
            .unwrap();
        std::fs::write(source.join("episode.mkv"), b"damaged direct media").unwrap();

        let mut workspace = inventory.workspace(5, "par_repair", "abc").unwrap();
        inventory
            .retain_transform_original(&mut workspace, "episode.mkv")
            .unwrap();
        // The repair layer calls this only after PAR full-digest validation
        // and checked publication into the source generation.
        let repaired = b"repaired PAR-verified bytes";
        std::fs::write(source.join("episode.mkv"), repaired).unwrap();
        inventory
            .finish_repair_workspace(&workspace, &["episode.mkv".into()])
            .unwrap();
        drop(inventory);

        let inventory = Inventory::open(&state).unwrap();
        let (_, verified) = inventory.verified_transform_evidence(5).unwrap();
        assert!(verified.contains("episode.mkv"));
        assert_eq!(
            std::fs::read(source.join("episode.mkv")).unwrap(),
            b"repaired PAR-verified bytes"
        );
        assert_eq!(
            std::fs::read(source.join(workspace.retained.get("episode.mkv").unwrap())).unwrap(),
            b"damaged direct media"
        );
        let source_artifact = inventory.for_job(5).unwrap().unwrap();
        let selected = source_artifact
            .files
            .iter()
            .find(|entry| entry.path == "episode.mkv")
            .unwrap()
            .clone();
        let completed = temp.path().join("completed");
        std::fs::create_dir(&completed).unwrap();
        let outcome = DeliveryFileOutcome {
            source_path: "episode.mkv".into(),
            publication_path: Some("episode.mkv".into()),
            classification: "media".into(),
            outcome: "published".into(),
            reason: Some("PAR digest-validated repair output".into()),
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        };
        let publication = inventory
            .publish_selected_for_delivery_checked(
                5,
                &completed.join("release.runner-generation"),
                vec![selected],
                vec![outcome],
                &|| Ok(()),
            )
            .unwrap();
        assert_eq!(
            std::fs::read(publication.published_path.join("episode.mkv")).unwrap(),
            repaired
        );
    }

    #[test]
    fn changed_par_repair_output_is_not_retrusted_after_restart() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("downloads");
        std::fs::create_dir(&root).unwrap();
        let state = temp.path().join("state");
        let inventory = Inventory::open(&state).unwrap();
        let source = root.join("job");
        inventory.allocate(5, &root, &source).unwrap();
        std::fs::write(source.join("episode.mkv"), b"damaged direct media").unwrap();

        let mut workspace = inventory.workspace(5, "par_repair", "abc").unwrap();
        inventory
            .retain_transform_original(&mut workspace, "episode.mkv")
            .unwrap();
        let repaired = b"repaired PAR-verified bytes";
        std::fs::write(source.join("episode.mkv"), repaired).unwrap();
        inventory
            .finish_repair_workspace(&workspace, &["episode.mkv".into()])
            .unwrap();
        drop(inventory);

        std::fs::write(source.join("episode.mkv"), vec![b'x'; repaired.len()]).unwrap();
        let inventory = Inventory::open(&state).unwrap();
        let (_, verified) = inventory.verified_transform_evidence(5).unwrap();
        assert!(!verified.contains("episode.mkv"));
    }
}
