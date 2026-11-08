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
        crate::fileops::payload_read(n);
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

fn digest_open_observed(
    file: &mut File,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
    scanned: &dyn Fn(u64),
) -> Result<(String, u64)> {
    use sha2::{Digest, Sha256};
    use std::io::{Read, Seek};
    checkpoint()?;
    let before = fs::identity_for_file(file)?;
    file.rewind()?;
    let mut digest = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0; 65536];
    loop {
        checkpoint()?;
        let n = file.read(&mut buffer)?;
        crate::fileops::payload_read(n);
        scanned(n as u64);
        if n == 0 {
            break;
        }
        bytes += n as u64;
        digest.update(&buffer[..n]);
    }
    checkpoint()?;
    if fs::identity_for_file(file)? != before || bytes != before.bytes {
        return Err(Error::Conflict(
            "file changed while verifying owned rename".into(),
        ));
    }
    Ok((format!("{:x}", digest.finalize()), bytes))
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
        crate::fileops::payload_read(count);
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

// Only one validation call owns this cache. Reopening and checking the full
// identity on every hit preserves changed-file detection between proof paths.
type ProofDigests = std::collections::HashMap<String, (Identity, String)>;

fn digest_file_once(
    root: &File,
    path: &str,
    expected: &Identity,
    digests: &mut ProofDigests,
    checkpoint: &mut dyn FnMut() -> std::io::Result<()>,
) -> Result<String> {
    checkpoint()?;
    let current = fs::open_relative(root, path)?;
    if fs::identity_for_file(&current)? != *expected {
        return Err(Error::Conflict(format!(
            "transform output identity changed before digest: {path}"
        )));
    }
    if let Some((identity, digest)) = digests.get(path) {
        if identity == expected {
            return Ok(digest.clone());
        }
    }
    let digest = digest_file_with_checkpoint(root, path, expected, checkpoint)?;
    if fs::identity_for_file(&fs::open_relative(root, path)?)? != *expected {
        return Err(Error::Conflict(format!(
            "transform output identity changed after digest: {path}"
        )));
    }
    digests.insert(path.to_owned(), (expected.clone(), digest.clone()));
    Ok(digest)
}

/// See [`Inventory::verified_custody_evidence`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CustodyEvidence {
    /// Former source-relative path -> current path, for every owned rename
    /// whose current bytes still match the rename journal's digest.
    pub restored: std::collections::HashMap<String, String>,
    /// Archive inputs a successful extraction consumed, at former and current
    /// paths.
    pub consumed_inputs: std::collections::HashSet<String>,
    /// Extraction and PAR-repair outputs proven intact, at current paths.
    pub verified_outputs: std::collections::HashSet<String>,
}

/// One former path of a renamed file, with the file's complete identity at
/// the moment it left that path.
struct VerifiedRename {
    current: String,
    before: Identity,
    digest: String,
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
    fn runtime_workspace_captures(&self, workspace: &Workspace) -> Result<[Artifact; 2]> {
        let source = self.get(&workspace.source.id)?;
        let scratch = self.get(&workspace.scratch.id)?;
        if source.generation != workspace.source.generation
            || source.path != workspace.source.path
            || source.root != workspace.source.root
            || source.terminal()
            || source.hold.is_some()
            || scratch.generation != workspace.scratch.generation
            || scratch.path != workspace.scratch.path
            || scratch.root != workspace.scratch.root
            || scratch.terminal()
        {
            return Err(Error::Conflict(
                "transform participant generation or custody changed".into(),
            ));
        }
        Ok([source, scratch])
    }

    fn save_transform_progress(
        &self,
        operation: &Operation,
        previous: &Operation,
        captures: &[Artifact],
    ) -> Result<()> {
        let _coordinator = self.mutation_guard()?;
        for capture in captures {
            self.check_payload_capture(capture)?;
        }
        let current = self.operation(&previous.id)?;
        if current.request != previous.request
            || current.state != previous.state
            || current.kind != previous.kind
            || current.artifact != previous.artifact
        {
            return Err(Error::Conflict(
                "transform journal changed during payload work".into(),
            ));
        }
        save_operation(&self.db.lock().unwrap(), operation)
    }

    pub fn workspace(&self, job: u32, kind: &str, token: &str) -> Result<Workspace> {
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
        let mut lease = self
            .reserve_processing_mutation(std::slice::from_ref(&source))?
            .ok_or_else(|| Error::Conflict("transform namespace remains in use".into()))?;
        let source_before = source.clone();
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
                // A completed repair is immutable publication evidence. A
                // restarted pipeline may need to verify/repair this set again,
                // but cannot append attempts to that successful operation.
                || (op.kind == "par_repair"
                    && op.state == "succeeded"
                    && workspace.published_files.is_some())
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
                {
                    let coordinator = self.mutation_guard()?;
                    self.check_payload_capture(&source_before)?;
                    lease.extend_locked(&scratch, &coordinator)?;
                }
                self.verify(&scratch)?;
                self.verify_workspace_attempts(&workspace)?;
                let _coordinator = self.mutation_guard()?;
                self.check_payload_capture(&source_before)?;
                self.check_payload_capture(&scratch)?;
                if self.operation(&op.id)?.request != op.request {
                    return Err(Error::Conflict(
                        "transform journal changed while reopening workspace".into(),
                    ));
                }
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
            let coordinator = self.mutation_guard()?;
            self.check_payload_capture(&source_before)?;
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            save_operation(&tx, &op)?;
            save_artifact(&tx, &source)?;
            save_artifact(&tx, &workspace.scratch)?;
            tx.commit()?;
            drop(db);
            lease.extend_locked(&workspace.scratch, &coordinator)?;
        }
        let pending_scratch = workspace.scratch.clone();
        let pending_operation = op.clone();
        // A crash before identity commit leaves an unowned allocation for review.
        std::fs::create_dir(&path)?;
        fs::sync_directory(&workspace_dir)?;
        let scratch_dir = fs::open_dir(&path)?;
        workspace.scratch.identity = Some(fs::identity_for_file(&scratch_dir)?);
        workspace.scratch.state = "active".into();
        self.sidecar(&workspace.scratch)?;
        op.request = serde_json::to_string(&workspace)?;
        let _coordinator = self.mutation_guard()?;
        self.check_payload_capture(&source)?;
        self.check_payload_capture(&pending_scratch)?;
        if self.operation(&op.id)?.request != pending_operation.request {
            return Err(Error::Conflict(
                "transform allocation journal changed".into(),
            ));
        }
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
        let [current_source, current_scratch] = self.runtime_workspace_captures(workspace)?;
        if self.reconciliation_in_use(&current_source)
            || self.reconciliation_in_use(&current_scratch)
            || op.request != serde_json::to_string(workspace)?
        {
            return Err(Error::Conflict(
                "extraction journal is busy or changed".into(),
            ));
        }
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
        let raw: Option<String> = self
            .db
            .lock()
            .unwrap()
            .query_row(
                operation_queries::TRANSFORM_SCRATCH_GENERATION,
                params![artifact.id, artifact.generation],
                |row| row.get(0),
            )
            .optional()?;
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
        let mut op = self.operation(&workspace.operation_id)?;
        let mut current: Workspace = serde_json::from_str(&op.request)?;
        let captures = self.runtime_workspace_captures(&current)?;
        let _lease = self
            .reserve_processing_mutation(&captures)?
            .ok_or_else(|| Error::Conflict("workspace attempt remains in use".into()))?;
        let mut previous = op.clone();
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
        self.save_transform_progress(&op, &previous, &captures)?;
        previous = op.clone();
        let path = current.scratch.path.join(&directory);
        std::fs::create_dir(&path)?;
        fs::sync_directory(&self.verify(&current.scratch)?)?;
        current.attempts.last_mut().unwrap().identity =
            Some(fs::identity_for_file(&fs::open_dir(&path)?)?);
        op.request = serde_json::to_string(&current)?;
        op.attempts = current.attempts.len() as u32;
        self.save_transform_progress(&op, &previous, &captures)?;
        *workspace = current;
        Ok(path)
    }

    /// Revalidate the captured source generation and root before transform work.
    pub fn validate_workspace_source(&self, workspace: &Workspace) -> Result<()> {
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
        let _lease = self
            .reserve_processing_mutation(std::slice::from_ref(&source))?
            .ok_or_else(|| Error::Conflict("transform source is busy".into()))?;
        self.verify(&source)?;
        let _coordinator = self.mutation_guard()?;
        self.check_payload_capture(&source)?;
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
            .ok_or_else(|| Error::Conflict("original is outside captured manifest".into()))?
            .clone();
        let captures = self.runtime_workspace_captures(workspace)?;
        let _lease = self
            .reserve_processing_mutation(&captures)?
            .ok_or_else(|| Error::Conflict("transform original remains in use".into()))?;
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
        let replay_digest = workspace.retained_digests.get(relative);
        if original_identity != entry.identity
            && !(replay_digest.is_some()
                && rename_transition_matches(&entry.identity, &original_identity))
        {
            return Err(Error::Conflict("original identity changed".into()));
        }
        // Hash through a stable descriptor before taking the global mutation
        // guard. Large media files must not block unrelated custody controls.
        let digest = digest_open(&mut original.try_clone()?)?;
        if replay_digest.is_some_and(|expected| *expected != digest) {
            return Err(Error::Conflict("retained original content changed".into()));
        }
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
        let mut previous = op.clone();
        if op.request != serde_json::to_string(workspace)? {
            return Err(Error::Conflict(
                "transform journal changed before retaining original".into(),
            ));
        }
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
                && observed.same_object(&original_identity)
                && digest == *expected_digest
            {
                fs::rename_exclusive(
                    &workspace.source.path.join(relative),
                    &workspace.source.path.join(&backup),
                )?;
                let final_file = fs::open_relative(&root, &backup)?;
                let final_identity = fs::identity_for_file(&final_file)?;
                if !rename_transition_matches(&entry.identity, &final_identity)
                    || digest_open(&mut final_file.try_clone()?)? != *expected_digest
                    || fs::identity_for_file(&final_file)? != final_identity
                    || crate::fileops::identity(&crate::fileops::open(
                        &workspace.source.path.join(&backup),
                    )?)? != final_identity
                {
                    return Err(Error::Conflict("retained replay content changed".into()));
                }
                if let Some(captured) = workspace
                    .source
                    .files
                    .iter_mut()
                    .find(|f| f.path == relative)
                {
                    captured.identity = final_identity;
                }
                workspace.retained.insert(relative.into(), backup);
                op.request = serde_json::to_string(workspace)?;
                self.save_transform_progress(&op, &previous, &captures)?;
                return Ok(());
            }
            return Err(Error::Conflict("retained original identity changed".into()));
        }
        workspace.retained.insert(relative.into(), backup.clone());
        workspace
            .retained_digests
            .insert(relative.into(), digest.clone());
        op.request = serde_json::to_string(workspace)?;
        self.save_transform_progress(&op, &previous, &captures)?;
        previous = op.clone();
        fs::rename_exclusive(
            &workspace.source.path.join(relative),
            &workspace.source.path.join(&backup),
        )?;
        let renamed = fs::open_relative(&root, &backup)?;
        let observed = fs::identity_for_file(&renamed)?;
        if !rename_transition_matches(&entry.identity, &observed)
            || digest_open(&mut renamed.try_clone()?)? != digest
            || fs::identity_for_file(&renamed)? != observed
            || crate::fileops::identity(&crate::fileops::open(
                &workspace.source.path.join(&backup),
            )?)? != observed
        {
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
        self.save_transform_progress(&op, &previous, &captures)?;
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

    /// Reuse only the completed repair for this exact catalog and source
    /// generation, with every expected output freshly proven at its current
    /// path. Scratch retirement does not invalidate a published byte proof.
    pub fn repair_published(
        &self,
        job: u32,
        token: &str,
        expected_paths: &[String],
        mut checkpoint: impl FnMut() -> std::io::Result<()>,
        mut verified: impl FnMut(u64),
    ) -> Result<bool> {
        checkpoint()?;
        if expected_paths.is_empty()
            || token.len() > 128
            || !token.bytes().all(|c| c.is_ascii_hexdigit())
        {
            return Ok(false);
        }
        let source = self.for_job(job)?.ok_or(Error::NotFound)?;
        if source.hold.is_some() || !matches!(source.state.as_str(), "active" | "transitioning") {
            return Ok(false);
        }
        let key = format!("par_repair-{}-{}-{token}", source.id, source.generation);
        let raw: Option<String> = self.db.lock().unwrap().query_row(
            "SELECT data FROM operations WHERE artifact=?1 AND state='succeeded' AND json_extract(data,'$.kind')='par_repair' AND (id=?2 OR json_extract(json_extract(data,'$.request'),'$.logical_key')=?2) ORDER BY rowid DESC LIMIT 1",
            params![source.id, key], |row| row.get(0),
        ).optional()?;
        let Some(raw) = raw else { return Ok(false) };
        let operation: Operation = serde_json::from_str(&raw)?;
        let workspace: Workspace = serde_json::from_str(&operation.request)?;
        let paths: std::collections::HashSet<_> = expected_paths.iter().collect();
        let Some(proofs) = workspace.published_files.as_ref() else {
            return Ok(false);
        };
        if workspace.source.generation != source.generation
            || workspace.source.id != source.id
            || workspace.source.path != source.path
            || workspace.source.root != source.root
            || paths.len() != expected_paths.len()
            || workspace.verified_output_paths.len() != paths.len()
            || proofs.len() != paths.len()
            || !paths
                .iter()
                .all(|path| workspace.verified_output_paths.contains(path))
        {
            return Ok(false);
        }
        let _lease = self
            .reserve_processing_mutation(std::slice::from_ref(&source))?
            .ok_or_else(|| Error::Conflict("repair proof source remains in use".into()))?;
        let root = self.verify(&source)?;
        let mut digests = ProofDigests::new();
        let mut renamed = None;
        for path in expected_paths {
            checkpoint()?;
            let Some(proof) = proofs
                .iter()
                .find(|entry| entry.path == *path && !entry.identity.directory)
            else {
                return Ok(false);
            };
            let Some(expected) = proof.digest.as_deref().filter(|digest| !digest.is_empty()) else {
                return Ok(false);
            };
            let current = source.files.iter().find(|entry| entry.path == *path);
            let intact = if current.is_some_and(|entry| entry.identity == proof.identity) {
                digest_file_once(&root, path, &proof.identity, &mut digests, &mut checkpoint)?
                    == expected
            } else {
                if renamed.is_none() {
                    renamed = Some(self.verified_renames(
                        &source,
                        &root,
                        &mut digests,
                        &mut checkpoint,
                    )?);
                }
                renamed.as_ref().unwrap().get(path).is_some_and(|rename| {
                    rename.before == proof.identity && rename.digest == expected
                })
            };
            if !intact {
                return Ok(false);
            }
            verified(proof.identity.bytes);
        }
        checkpoint()?;
        let _coordinator = self.mutation_guard()?;
        self.check_payload_capture(&source)?;
        let current = self.operation(&operation.id)?;
        if current.state != "succeeded" || current.request != operation.request {
            return Err(Error::Conflict(
                "completed repair journal changed during verification".into(),
            ));
        }
        drop(_coordinator);
        drop(_lease);
        self.abandon_superseded_repairs(&source, &key, &mut checkpoint)?;
        Ok(true)
    }

    /// A cancelled retry may leave an unpublished private attempt behind.
    /// Reusing a proven repair must close that attempt before publication,
    /// without claiming its scratch bytes as output or deleting them.
    fn abandon_superseded_repairs(
        &self,
        source: &Artifact,
        logical_key: &str,
        mut checkpoint: impl FnMut() -> std::io::Result<()>,
    ) -> Result<()> {
        let raws = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare(
                "SELECT data FROM operations WHERE artifact=?1 AND state IN ('running','queued','retry','review') ORDER BY rowid",
            )?;
            let rows = statement.query_map([&source.id], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let mut superseded = Vec::new();
        for raw in raws {
            checkpoint()?;
            let operation: Operation = serde_json::from_str(&raw)?;
            if operation.kind != "par_repair" || operation.state != "running" {
                return Err(Error::Conflict(
                    "another pending operation blocks repair reuse".into(),
                ));
            }
            let workspace: Workspace = serde_json::from_str(&operation.request)?;
            if workspace.logical_key.as_deref().unwrap_or(&operation.id) != logical_key
                || workspace.source.id != source.id
                || workspace.source.generation != source.generation
                || workspace.source.path != source.path
                || workspace.source.root != source.root
                || workspace.published_files.is_some()
                || !workspace.verified_output_paths.is_empty()
                || !workspace.retained.is_empty()
            {
                return Err(Error::Conflict(
                    "another repair generation blocks reuse".into(),
                ));
            }
            superseded.push(workspace);
        }
        if superseded.is_empty() {
            return if source.state == "active" {
                Ok(())
            } else {
                Err(Error::Conflict(
                    "source transition has no superseded repair attempt".into(),
                ))
            };
        }
        for workspace in superseded {
            checkpoint()?;
            self.abandon_repair_workspace(&workspace)?;
        }
        Ok(())
    }

    /// A successful extraction can be reused after a later stage fails. Names
    /// may have been journal-renamed, but every output must retain its complete
    /// identity in the current owned manifest AND on disk.
    pub fn extraction_published(&self, workspace: &Workspace) -> Result<bool> {
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
        let _lease = self
            .reserve_processing_mutation(std::slice::from_ref(&source))?
            .ok_or_else(|| Error::Conflict("extraction source remains in use".into()))?;
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
        let _coordinator = self.mutation_guard()?;
        self.check_payload_capture(&source)?;
        if self.operation(&op.id)?.request != op.request {
            return Err(Error::Conflict(
                "extraction journal changed during validation".into(),
            ));
        }
        Ok(true)
    }

    /// Return only extraction outputs whose successful transform journal and
    /// current source identities still prove them. This is used by delivery
    /// salvage after a later archive or script failed; an extension alone is
    /// never integrity evidence. Owned renames journaled after the transform
    /// are followed; see [`Inventory::verified_custody_evidence`].
    pub fn verified_transform_evidence(
        &self,
        job: u32,
    ) -> Result<(
        std::collections::HashSet<String>,
        std::collections::HashSet<String>,
    )> {
        let evidence = self.verified_custody_evidence(job, || Ok(()))?;
        Ok((evidence.consumed_inputs, evidence.verified_outputs))
    }

    /// Integrity evidence for one source generation, from all three custody
    /// journals in a single hashing pass: owned renames (`par_restore`),
    /// successful extractions, and PAR-validated repairs.
    ///
    /// The journals are judged together because they compose. Post-processing
    /// extracts first and renames afterwards (the final deobfuscation pass
    /// gives an obfuscated `<hash>.mkv` and its `<hash>.partNN.rar` volumes
    /// the release name). A rename changes inode ctime, so an extraction
    /// output no longer matches the complete identity its journal recorded.
    /// The rename journal is what proves that transition: it recorded the
    /// file's complete identity immediately before the rename and a digest of
    /// its bytes, and the current file must still be the same object with the
    /// same bytes. An output is followed through a rename only when the
    /// journaled pre-rename identity is exactly the identity the transform
    /// recorded, so the chain transform → rename → current file is unbroken.
    pub fn verified_custody_evidence(
        &self,
        job: u32,
        mut checkpoint: impl FnMut() -> std::io::Result<()>,
    ) -> Result<CustodyEvidence> {
        let Some(source) = self.for_job(job)? else {
            return Ok(Default::default());
        };
        let _lease = self
            .reserve_processing_mutation(std::slice::from_ref(&source))?
            .ok_or_else(|| Error::Conflict("custody evidence source remains in use".into()))?;
        let root = self.verify(&source)?;
        let mut digests = ProofDigests::new();
        let renamed = self.verified_renames(&source, &root, &mut digests, &mut checkpoint)?;
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
                let consumed = workspace
                    .input_paths
                    .iter()
                    .chain(workspace.input_path.as_ref())
                    .cloned()
                    .collect::<Vec<_>>();
                for path in consumed {
                    // "Consumed" authorizes source cleanup to delete the file,
                    // so it needs the same proof as a published output: the
                    // exact identity the successful extraction bound (and
                    // rechecked when it finished), either still at its bound
                    // path or carried to a new name by the rename journal.
                    let Some(bound) = workspace
                        .source
                        .files
                        .iter()
                        .find(|entry| entry.path == path && !entry.identity.directory)
                    else {
                        continue;
                    };
                    if observed
                        .iter()
                        .any(|entry| entry.path == path && entry.identity == bound.identity)
                    {
                        inputs.insert(path.clone());
                    }
                    if let Some(rename) = renamed.get(&path) {
                        if rename.before == bound.identity {
                            inputs.insert(rename.current.clone());
                        }
                    }
                }
            }
            for output in workspace.published_files.as_deref().unwrap_or_default() {
                if output.identity.directory {
                    continue;
                }
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
                    continue;
                }
                // Repair outputs are proven by path and digest below.
                if operation.kind == "extract" {
                    if let Some(rename) = renamed.get(&output.path) {
                        if rename.before == output.identity {
                            outputs.insert(rename.current.clone());
                        }
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
                    if matches_identity {
                        checkpoint()?;
                        let matches_digest = proof.digest.as_deref().is_some_and(|expected| {
                            digest_file_once(
                                &root,
                                &path,
                                &proof.identity,
                                &mut digests,
                                &mut checkpoint,
                            )
                            .is_ok_and(|actual| actual == expected)
                        });
                        if matches_digest {
                            outputs.insert(path.clone());
                        }
                        continue;
                    }
                    // Renamed after repair: the rename journal hashed the
                    // current bytes already, so the same digest and the exact
                    // pre-rename identity complete the proof.
                    if let Some(rename) = renamed.get(&path) {
                        if rename.before == proof.identity
                            && proof.digest.as_deref() == Some(rename.digest.as_str())
                        {
                            outputs.insert(rename.current.clone());
                        }
                    }
                }
            }
        }
        // Native acquisitions have no NZB segments/CRC. Their completed
        // piece-verified copy journal carries independent content proof, with
        // the same fresh identity/digest and rename checks as repaired output.
        for proof in self.torrent_copy_integrity_files(job, &source)? {
            let Some(expected_digest) = proof.digest.as_deref() else {
                continue;
            };
            if observed
                .iter()
                .any(|entry| entry.path == proof.path && entry.identity == proof.identity)
            {
                checkpoint()?;
                if digest_file_once(
                    &root,
                    &proof.path,
                    &proof.identity,
                    &mut digests,
                    &mut checkpoint,
                )? == expected_digest
                {
                    outputs.insert(proof.path.clone());
                }
            } else if let Some(rename) = renamed.get(&proof.path) {
                if rename.before == proof.identity && rename.digest == expected_digest {
                    outputs.insert(rename.current.clone());
                }
            }
        }
        let _coordinator = self.mutation_guard()?;
        self.check_payload_capture(&source)?;
        Ok(CustodyEvidence {
            restored: renamed
                .into_iter()
                .map(|(old, rename)| (old, rename.current))
                .collect(),
            consumed_inputs: inputs,
            verified_outputs: outputs,
        })
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
        let _lease = self
            .reserve_processing_mutation(std::slice::from_ref(&source))?
            .ok_or_else(|| Error::Conflict("custody evidence source remains in use".into()))?;
        let root = self.verify(&source)?;
        let mut digests = ProofDigests::new();
        let result = self
            .verified_renames(&source, &root, &mut digests, &mut checkpoint)?
            .into_iter()
            .map(|(old, rename)| (old, rename.current))
            .collect();
        let _coordinator = self.mutation_guard()?;
        self.check_payload_capture(&source)?;
        Ok(result)
    }

    /// Every former path of a file whose owned-rename chain still verifies,
    /// keyed by that former path.
    fn verified_renames(
        &self,
        source: &Artifact,
        root: &File,
        digests: &mut ProofDigests,
        mut checkpoint: impl FnMut() -> std::io::Result<()>,
    ) -> Result<std::collections::HashMap<String, VerifiedRename>> {
        let raws = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare(
                "SELECT data FROM operations WHERE artifact=?1 AND state='succeeded' AND json_extract(data,'$.kind')='par_restore'",
            )?;
            let rows = statement.query_map([&source.id], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
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
                || digest_file_once(root, new, &current.identity, digests, &mut checkpoint)?
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
                restored.insert(
                    oldest.to_string(),
                    VerifiedRename {
                        current: new.clone(),
                        before: child_identity.clone(),
                        digest: expected_digest.clone(),
                    },
                );
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
        let captures = self.runtime_workspace_captures(workspace)?;
        let mut source = captures[0].clone();
        let mut scratch = captures[1].clone();
        let _lease = self
            .reserve_processing_mutation(&captures)?
            .ok_or_else(|| Error::Conflict("transform output remains in use".into()))?;
        let previous = self.operation(&workspace.operation_id)?;
        let mut journal_workspace: Workspace = serde_json::from_str(&previous.request)?;
        journal_workspace.published_files = workspace.published_files.clone();
        journal_workspace.verified_output_paths = workspace.verified_output_paths.clone();
        if serde_json::to_string(&journal_workspace)? != serde_json::to_string(workspace)? {
            return Err(Error::Conflict(
                "transform completion uses a stale workspace journal".into(),
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
        let stage = self.verify(&scratch)?;
        scratch.files = fs::manifest(&stage, 100_000)?;
        scratch.state = "retained".into();
        scratch.revision += 1;
        source.state = "active".into();
        source.revision += 1;
        source.files = fs::manifest(&dir, 100_000)?;
        let mut op = previous.clone();
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
        let _coordinator = self.mutation_guard()?;
        for capture in &captures {
            self.check_payload_capture(capture)?;
        }
        let current = self.operation(&previous.id)?;
        if current.request != previous.request || current.state != previous.state {
            return Err(Error::Conflict(
                "transform completion journal changed".into(),
            ));
        }
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

enum RestoreContent<'a> {
    Source(Option<&'a crate::fileops::ContentProof>),
    Expected(&'a writer_naming::NamingEvidence),
}

impl Inventory {
    /// Journal an intact file's identity before restoring a catalog path.
    pub fn restore_file(&self, job: u32, from: &Path, to: &Path) -> Result<()> {
        self.restore_file_inner(
            job,
            from,
            to,
            RestoreContent::Source(None),
            &|| Ok(()),
            &|_| {},
        )
        .map(|_| ())
    }
    pub fn restore_file_with_proof(
        &self,
        job: u32,
        from: &Path,
        to: &Path,
        proof: &crate::fileops::ContentProof,
    ) -> Result<()> {
        self.restore_file_with_proof_observed(job, from, to, proof, &|| Ok(()), &|_| {})
            .map(|_| ())
    }
    pub fn restore_file_with_proof_observed(
        &self,
        job: u32,
        from: &Path,
        to: &Path,
        proof: &crate::fileops::ContentProof,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
        scanned: &dyn Fn(u64),
    ) -> Result<writer_naming::RestoreMetrics> {
        self.restore_file_inner(
            job,
            from,
            to,
            RestoreContent::Source(Some(proof)),
            checkpoint,
            scanned,
        )
    }
    pub fn restore_file_expected(
        &self,
        job: u32,
        from: &Path,
        to: &Path,
        expected: &writer_naming::NamingEvidence,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
        scanned: &dyn Fn(u64),
    ) -> Result<writer_naming::RestoreMetrics> {
        self.restore_file_inner(
            job,
            from,
            to,
            RestoreContent::Expected(expected),
            checkpoint,
            scanned,
        )
    }
    fn restore_file_inner(
        &self,
        job: u32,
        from: &Path,
        to: &Path,
        content: RestoreContent<'_>,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
        scanned: &dyn Fn(u64),
    ) -> Result<writer_naming::RestoreMetrics> {
        use sha2::{Digest, Sha256};
        let expected_token = match &content {
            RestoreContent::Expected(token) => Some(*token),
            _ => None,
        };
        let check = || -> std::io::Result<()> {
            checkpoint()?;
            if expected_token.is_some_and(|token| !token.live()) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "producer naming expectation was revoked",
                ));
            }
            Ok(())
        };
        check()?;
        let mut metrics = writer_naming::RestoreMetrics::default();
        let mut source = self.for_job(job)?.ok_or(Error::NotFound)?;
        let original = source.clone();
        let _lease = self
            .reserve_processing_mutation(std::slice::from_ref(&source))?
            .ok_or_else(|| Error::Conflict("restore namespace remains in use".into()))?;
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
        let digest = match content {
            RestoreContent::Expected(expected) => {
                if expected.job() != job || !self.naming_candidate_matches(expected, from) {
                    return Err(Error::Conflict(
                        "producer naming expectation no longer matches source".into(),
                    ));
                }
                expected.expected_sha().to_owned()
            }
            RestoreContent::Source(proof) => match proof {
                Some(proof) if proof.identity == identity => proof.sha256.clone(),
                Some(_) => {
                    return Err(Error::Conflict(
                        "restore fingerprint identity changed".into(),
                    ))
                }
                None => digest_open(&mut file.try_clone()?)?,
            },
        };
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
        check()?;
        let journal_started = Instant::now();
        {
            let _coordinator = self.mutation_guard()?;
            self.check_payload_capture(&original)?;
            match self.operation(&op.id) {
                Err(Error::NotFound) => (),
                Ok(previous) if previous.request == op.request && previous.state == "running" => (),
                Ok(_) => {
                    return Err(Error::Conflict(
                        "restore journal already belongs to another transition".into(),
                    ));
                }
                Err(error) => return Err(error),
            }
            save_operation(&self.db.lock().unwrap(), &op)?;
        }
        metrics.journal_ms += journal_started.elapsed().as_millis() as u64;
        let previous = op.clone();
        check()?;
        let namespace_started = Instant::now();
        if let Some(parent) = new.parent() {
            crate::fileops::parents(&source.path, parent)?;
        }
        fs::rename_exclusive(from, to)?;
        let observed = fs::open_relative(&root, &new.to_string_lossy())?;
        let observed_identity = fs::identity_for_file(&observed)?;
        metrics.namespace_ms = namespace_started.elapsed().as_millis() as u64;
        let readback_started = Instant::now();
        let (observed_digest, bytes) =
            digest_open_observed(&mut observed.try_clone()?, &check, scanned)?;
        metrics.readback_ms = readback_started.elapsed().as_millis() as u64;
        metrics.readback_bytes = bytes;
        if !rename_transition_matches(&identity, &observed_identity)
            || observed_digest != digest
            || fs::identity_for_file(&observed)? != observed_identity
            || crate::fileops::identity(&crate::fileops::open(to)?)? != observed_identity
        {
            return Err(Error::Conflict("restore identity changed".into()));
        }
        check()?;
        let manifest_started = Instant::now();
        source.files = fs::manifest(&root, 100_000)?;
        metrics.manifest_ms = manifest_started.elapsed().as_millis() as u64;
        source.revision += 1;
        op.state = "succeeded".into();
        check()?;
        let journal_started = Instant::now();
        let _coordinator = self.mutation_guard()?;
        self.check_payload_capture(&original)?;
        let current = self.operation(&previous.id)?;
        if current.request != previous.request || current.state != previous.state {
            return Err(Error::Conflict(
                "restore journal changed during rename".into(),
            ));
        }
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &source)?;
        save_operation(&tx, &op)?;
        check()?;
        tx.commit()?;
        metrics.journal_ms += journal_started.elapsed().as_millis() as u64;
        Ok(metrics)
    }
}

impl Inventory {
    /// Reconcile journaled mappings before new queue or PP operations start.
    #[cfg(test)]
    pub(super) fn reconcile_transforms(&self) -> Result<()> {
        for root in self.startup_reconciliation_roots()? {
            self.reconcile_transforms_root(&root)?;
        }
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn reconcile_transforms_root(&self, root: &Path) -> Result<()> {
        self.reconcile_transforms_root_snapshot(root, None)
    }
    pub(super) fn reconcile_transforms_root_snapshot(
        &self,
        root: &Path,
        snapshot: Option<&StartupReconciliation>,
    ) -> Result<()> {
        let raws = {
            let db = self.db.lock().unwrap();
            let mut statement=db.prepare("SELECT o.data FROM operations o JOIN artifacts a ON a.id=o.artifact WHERE json_extract(a.data,'$.root')=?2 AND (o.state='running' OR (o.state='retry' AND COALESCE(json_extract(o.data,'$.next_retry'),0)<=?1)) AND json_extract(o.data,'$.kind') IN ('par_restore','par_repair','extract')")?;
            let rows = statement
                .query_map(params![now(), root.to_string_lossy().as_ref()], |r| {
                    r.get::<_, String>(0)
                })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in raws {
            let mut op: Operation = serde_json::from_str(&raw)?;
            let source = self.get(&op.artifact)?;
            if let Some(snapshot) = snapshot {
                if !snapshot.contains_operation(&op)? || !snapshot.allows_artifact(self, &source)? {
                    continue;
                }
            }
            let mut participants = vec![source.clone()];
            let mut obsolete = source.terminal() || source.state == "deleting";
            if op.kind != "par_restore" {
                let workspace: Workspace = serde_json::from_str(&op.request)?;
                obsolete |= source.generation != workspace.source.generation;
                if !obsolete {
                    participants.push(self.get(&workspace.scratch.id)?);
                }
            }
            if obsolete {
                let _coordinator = self.mutation_guard()?;
                self.check_reconciliation_source(&op, &source)?;
                op.state = "cancelled".into();
                op.error = Some("transform no longer owns the source generation".into());
                save_operation(&self.db.lock().unwrap(), &op)?;
                continue;
            }
            let Some(_lease) = self.reserve_reconciliation(&participants)? else {
                continue;
            };
            if let Some(snapshot) = snapshot {
                let mut allowed = true;
                for participant in &participants {
                    allowed &= snapshot.allows_artifact(self, participant)?;
                }
                if !allowed {
                    continue;
                }
            }
            let baseline = self.operation(&op.id)?;
            if baseline.request != op.request || baseline.state != op.state {
                continue;
            }
            if op.state == "retry" {
                op.state = "running".into();
                op.next_retry = 0;
                let _coordinator = self.mutation_guard()?;
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
                    let _coordinator = self.mutation_guard()?;
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
                let _coordinator = self.mutation_guard()?;
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
                            if before_identity.as_ref().is_some_and(|observed| {
                                rename_transition_matches(&identity, observed)
                            }) && after_identity.as_ref().is_some_and(|observed| {
                                rename_transition_matches(&identity, observed)
                            }) && before_identity
                                .as_ref()
                                .zip(after_identity.as_ref())
                                .is_some_and(|(a, b)| a.same_object(b))
                                && before_digest.as_deref() == Some(digest.as_str())
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
                            ));
                        }
                    }
                    let final_file = fs::open_relative(&root, &new.to_string_lossy())?;
                    let final_identity = fs::identity_for_file(&final_file)?;
                    if !rename_transition_matches(&identity, &final_identity)
                        || digest_open(&mut final_file.try_clone()?)? != digest
                        || fs::identity_for_file(&final_file)? != final_identity
                        || crate::fileops::identity(&crate::fileops::open(
                            &source.path.join(&new),
                        )?)? != final_identity
                    {
                        return Err(Error::Conflict("restore replay content changed".into()));
                    }
                    let files = fs::manifest(&root, 100_000)?;
                    let _coordinator = self.mutation_guard()?;
                    self.check_reconciliation_source(&op, &current)?;
                    source = self.get(&source.id)?;
                    source.files = files;
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
                                let actual = digest_open(&mut file.try_clone()?)?;
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
                    let _coordinator = self.mutation_guard()?;
                    self.check_reconciliation_source(&op, &current)?;
                    op.request = serde_json::to_string(&workspace)?;
                    save_operation(&self.db.lock().unwrap(), &op)?;
                }
                Ok(())
            })();
            if let Err(error) = result {
                let _coordinator = self.mutation_guard()?;
                // A concurrent metadata transition wins over this worker.
                if self.check_reconciliation_source(&op, &current).is_err() {
                    continue;
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
    /// Whether post-processing may be retried from the beginning. The
    /// filesystem proof (root identity, every payload file's identity) runs
    /// WITHOUT the mutation guard: it is called against volumes that just
    /// failed, and a wedged NFS/FUSE mount must stall only this caller — not
    /// every allocation, attempt lease and inventory write queued behind the
    /// guard. The guarded re-check afterwards proves the row it verified is
    /// still the row (same id, generation and revision).
    pub fn validate_post_retry(&self, job: u32) -> Result<()> {
        self.prepare_post_retry(job).map(|_| ())
    }

    /// Return the exact version whose physical custody was checked. A caller
    /// can carry this version to a short actor CAS without rereading paths.
    pub fn prepare_post_retry(&self, job: u32) -> Result<Artifact> {
        self.prepare_post_retry_inner(job, &|| Ok(()), true)
    }

    pub fn prepare_post_retry_checked(
        &self,
        job: u32,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Artifact> {
        self.prepare_post_retry_inner(job, checkpoint, false)
    }

    fn prepare_post_retry_inner(
        &self,
        job: u32,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
        retire_sources: bool,
    ) -> Result<Artifact> {
        checkpoint()?;
        // A journaled rename may already have moved the old source path.
        // Resume only its exact retryable operation before checking the new
        // committed source; review and deletion operations remain fenced.
        self.resume_post_relocation_checked(job, checkpoint, retire_sources)?;
        let source = {
            let _guard = self.mutation_guard()?;
            let source = self.for_job(job)?.ok_or(Error::NotFound)?;
            self.post_retry_custody(&source)?;
            source
        };
        let root = self.verify(&source)?;
        for entry in source.files.iter().filter(|e| !e.identity.directory) {
            checkpoint()?;
            let file = fs::open_relative(&root, &entry.path)?;
            if fs::identity_for_file(&file)? != entry.identity {
                return Err(Error::Conflict(
                    "post-processing input changed; review required".into(),
                ));
            }
        }
        checkpoint()?;
        let _guard = self.mutation_guard()?;
        let current = self.for_job(job)?.ok_or(Error::NotFound)?;
        if (&current.id, &current.generation, current.revision)
            != (&source.id, &source.generation, source.revision)
        {
            return Err(Error::Conflict(
                "payload custody changed during the retry check; check again".into(),
            ));
        }
        self.post_retry_custody(&current)?;
        let pending: i64 = self.db.lock().unwrap().query_row(
            "SELECT COUNT(*) FROM operations WHERE artifact=?1 AND state IN ('running','review','queued','retry') AND (json_extract(data,'$.kind')='delete' OR (json_extract(data,'$.kind')='relocate' AND state!='retry'))",
            [&current.id], |r| r.get(0),
        )?;
        let retry_relocations:bool=self.db.lock().unwrap().query_row(
            "SELECT EXISTS(SELECT 1 FROM operations WHERE artifact=?1 AND state='retry' AND json_extract(data,'$.kind')='relocate')",[&current.id],|row|row.get(0))?;
        if pending != 0 || (retry_relocations && !self.recoverable_post_relocation(&current)?) {
            return Err(Error::Conflict(
                "resolve pending relocation or deletion before retrying post-processing".into(),
            ));
        }
        Ok(current)
    }

    /// The custody facts a post retry needs, read from the row alone.
    fn post_retry_custody(&self, source: &Artifact) -> Result<()> {
        if self.source_in_use(&SourceGeneration {
            artifact: source.id.clone(),
            generation: source.generation.clone(),
        }) {
            return Err(Error::Conflict(
                "previous processing workers have not quiesced".into(),
            ));
        }
        if !source.owned
            || (source.hold.is_some()
                && !(source.hold.as_deref() == Some("review: interrupted move")
                    && self.recoverable_post_relocation(source)?))
            || !matches!(
                source.state.as_str(),
                "active" | "transitioning" | "retained"
            )
        {
            return Err(Error::Conflict(
                "payload custody must be resolved before retrying post-processing".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_reuses_captured_proof_and_verifies_final_inode_once() {
        use sha2::{Digest, Sha256};
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("processing");
        std::fs::create_dir(&root).unwrap();
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        let source = root.join("job");
        inventory.allocate(5, &root, &source).unwrap();
        let bytes = vec![11; 131073];
        let from = source.join("obfuscated");
        let to = source.join("episode.mkv");
        std::fs::write(&from, &bytes).unwrap();
        let input = crate::fileops::open(&from).unwrap();
        let proof = crate::fileops::ContentProof {
            identity: fs::identity_for_file(&input).unwrap(),
            sha256: format!("{:x}", Sha256::digest(&bytes)),
        };
        crate::fileops::PAYLOAD_READ_BYTES.with(|n| n.set(0));
        fs::RENAME_FAILURE.with(|failure| failure.set(Some(libc::EINVAL)));
        inventory
            .restore_file_with_proof(5, &from, &to, &proof)
            .unwrap();
        assert_eq!(
            crate::fileops::PAYLOAD_READ_BYTES.with(|n| n.get()),
            bytes.len() as u64
        );
        assert!(!from.exists());
        assert_eq!(std::fs::read(to).unwrap(), bytes);
    }

    #[test]
    fn restore_replay_accepts_journaled_two_name_ctime_transition() {
        use sha2::{Digest, Sha256};
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("processing");
        std::fs::create_dir(&root).unwrap();
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        let path = root.join("job");
        inventory
            .allocate_for_delivery(5, &root, &path, true, false)
            .unwrap();
        std::fs::write(path.join("old"), b"original").unwrap();
        let source = inventory.refresh_for_delivery(5).unwrap();
        let identity = source
            .files
            .iter()
            .find(|f| f.path == "old")
            .unwrap()
            .identity
            .clone();
        let op = Operation {
            id: "interrupted-restore".into(),
            artifact: source.id.clone(),
            kind: "par_restore".into(),
            state: "running".into(),
            request: serde_json::to_string(&(
                source.clone(),
                PathBuf::from("old"),
                PathBuf::from("new"),
                identity,
                format!("{:x}", Sha256::digest(b"original")),
            ))
            .unwrap(),
            created_at: now(),
            not_before: 0,
            attempts: 1,
            next_retry: 0,
            error: None,
        };
        save_operation(&inventory.db.lock().unwrap(), &op).unwrap();
        std::fs::hard_link(path.join("old"), path.join("new")).unwrap();
        inventory.reconcile_transforms().unwrap();
        assert_eq!(inventory.operation(&op.id).unwrap().state, "succeeded");
        assert!(!path.join("old").exists());
        assert_eq!(std::fs::read(path.join("new")).unwrap(), b"original");
    }
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
    fn completed_repair_reuse_closes_only_unpublished_retries_and_keeps_their_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("downloads");
        std::fs::create_dir(&root).unwrap();
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        let source = root.join("job");
        inventory
            .allocate_for_delivery(5, &root, &source, true, false)
            .unwrap();
        std::fs::write(source.join("media.bin"), b"verified media").unwrap();
        let completed = inventory.workspace(5, "par_repair", "abc").unwrap();
        inventory
            .finish_repair_workspace(&completed, &["media.bin".into()])
            .unwrap();
        let mut interrupted = inventory.workspace(5, "par_repair", "abc").unwrap();
        let attempt = inventory.begin_workspace_attempt(&mut interrupted).unwrap();
        std::fs::write(attempt.join("partial"), b"private partial").unwrap();
        assert_eq!(
            inventory.for_job(5).unwrap().unwrap().state,
            "transitioning"
        );
        assert!(inventory.refresh_for_delivery(5).is_err());
        assert!(inventory
            .repair_published(5, "abc", &["media.bin".into()], || Ok(()), |_| {})
            .unwrap());
        assert_eq!(
            inventory
                .operation(&interrupted.operation_id)
                .unwrap()
                .state,
            "failed"
        );
        assert_eq!(
            inventory.operation(&completed.operation_id).unwrap().state,
            "succeeded"
        );
        assert_eq!(
            inventory.get(&interrupted.scratch.id).unwrap().state,
            "retained"
        );
        assert_eq!(
            std::fs::read(attempt.join("partial")).unwrap(),
            b"private partial"
        );
        assert_eq!(inventory.refresh_for_delivery(5).unwrap().state, "active");

        let matching = inventory.workspace(5, "par_repair", "abc").unwrap();
        let unrelated = inventory.workspace(5, "par_repair", "def").unwrap();
        assert!(inventory
            .repair_published(5, "abc", &["media.bin".into()], || Ok(()), |_| {})
            .is_err());
        assert_eq!(
            inventory.operation(&unrelated.operation_id).unwrap().state,
            "running"
        );
        assert_eq!(
            inventory.operation(&matching.operation_id).unwrap().state,
            "running"
        );
        assert_eq!(
            inventory.for_job(5).unwrap().unwrap().state,
            "transitioning"
        );
    }

    #[test]
    fn completed_repair_reuse_requires_the_exact_catalog_generation_and_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("downloads");
        std::fs::create_dir(&root).unwrap();
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        let source = root.join("job");
        inventory.allocate(5, &root, &source).unwrap();
        let bytes = vec![19; 131073];
        std::fs::write(source.join("media.bin"), &bytes).unwrap();
        let workspace = inventory.workspace(5, "par_repair", "abc").unwrap();
        inventory
            .finish_repair_workspace(&workspace, &["media.bin".into()])
            .unwrap();
        let paths = vec!["media.bin".into()];
        let mut observed = 0;
        assert!(inventory
            .repair_published(5, "abc", &paths, || Ok(()), |n| observed += n)
            .unwrap());
        assert_eq!(observed, bytes.len() as u64);
        assert!(!inventory
            .repair_published(5, "def", &paths, || Ok(()), |_| {})
            .unwrap());
        assert!(!inventory
            .repair_published(5, "abc", &["other.bin".into()], || Ok(()), |_| {})
            .unwrap());
        assert!(!inventory
            .repair_published(
                5,
                "abc",
                &["media.bin".into(), "media.bin".into()],
                || Ok(()),
                |_| {}
            )
            .unwrap());
        let mut original = inventory.for_job(5).unwrap().unwrap();
        let saved = original.clone();
        original.generation = "another-generation".into();
        save_artifact(&inventory.db.lock().unwrap(), &original).unwrap();
        assert!(!inventory
            .repair_published(5, "abc", &paths, || Ok(()), |_| {})
            .unwrap());
        save_artifact(&inventory.db.lock().unwrap(), &saved).unwrap();
        let operation = inventory.operation(&workspace.operation_id).unwrap();
        let mut changed = operation.clone();
        let mut proof: Workspace = serde_json::from_str(&changed.request).unwrap();
        proof.published_files.as_mut().unwrap()[0].digest = Some("0".repeat(64));
        changed.request = serde_json::to_string(&proof).unwrap();
        save_operation(&inventory.db.lock().unwrap(), &changed).unwrap();
        assert!(!inventory
            .repair_published(5, "abc", &paths, || Ok(()), |_| {})
            .unwrap());
        save_operation(&inventory.db.lock().unwrap(), &operation).unwrap();
        inventory
            .restore_file(5, &source.join("media.bin"), &source.join("release.mkv"))
            .unwrap();
        assert!(inventory
            .repair_published(5, "abc", &paths, || Ok(()), |_| {})
            .unwrap());
        std::fs::write(source.join("release.mkv"), vec![20; bytes.len()]).unwrap();
        assert!(!matches!(
            inventory.repair_published(5, "abc", &paths, || Ok(()), |_| {}),
            Ok(true)
        ));
    }

    #[test]
    fn cached_digest_rechecks_identity_and_cancellation_before_reuse() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("payload");
        std::fs::write(&path, b"original bytes").unwrap();
        let root = fs::open_dir(temp.path()).unwrap();
        let identity =
            fs::identity_for_file(&fs::open_relative(&root, "payload").unwrap()).unwrap();
        let mut cache = ProofDigests::new();
        digest_file_once(&root, "payload", &identity, &mut cache, &mut || Ok(())).unwrap();
        assert!(matches!(
            digest_file_once(&root, "payload", &identity, &mut cache,
                &mut || Err(std::io::ErrorKind::Interrupted.into())),
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted
        ));
        std::fs::rename(&path, temp.path().join("retained")).unwrap();
        std::fs::write(&path, b"original bytes").unwrap();
        assert!(digest_file_once(&root, "payload", &identity, &mut cache, &mut || Ok(())).is_err());
        assert_eq!(
            std::fs::read(temp.path().join("retained")).unwrap(),
            b"original bytes"
        );
        let replacement =
            fs::identity_for_file(&fs::open_relative(&root, "payload").unwrap()).unwrap();
        digest_file_once(&root, "payload", &replacement, &mut cache, &mut || Ok(())).unwrap();
        std::fs::write(&path, b"modified bytes").unwrap();
        assert!(
            digest_file_once(&root, "payload", &replacement, &mut cache, &mut || Ok(())).is_err()
        );
    }

    #[test]
    fn custody_evidence_hashes_each_current_file_once_across_rename_and_repair_proofs() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("downloads");
        std::fs::create_dir(&root).unwrap();
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        let source = root.join("job");
        inventory.allocate(5, &root, &source).unwrap();
        let bytes = vec![23; 131073];
        std::fs::write(source.join("original.mkv"), &bytes).unwrap();
        // Two successful rename journals end at the current path. Both
        // must keep their provenance without rereading the same object.
        for (old, new) in [
            ("original.mkv", "release.mkv"),
            ("release.mkv", "intermediate.mkv"),
            ("intermediate.mkv", "release.mkv"),
        ] {
            inventory
                .restore_file(5, &source.join(old), &source.join(new))
                .unwrap();
        }
        let workspace = inventory.workspace(5, "par_repair", "abc").unwrap();
        inventory
            .finish_repair_workspace(&workspace, &["release.mkv".into()])
            .unwrap();
        crate::fileops::PAYLOAD_READ_BYTES.with(|n| n.set(0));
        let evidence = inventory.verified_custody_evidence(5, || Ok(())).unwrap();
        assert_eq!(
            evidence.restored.get("original.mkv").map(String::as_str),
            Some("release.mkv")
        );
        assert!(evidence.verified_outputs.contains("release.mkv"));
        assert_eq!(
            crate::fileops::PAYLOAD_READ_BYTES.with(|n| n.get()),
            bytes.len() as u64
        );
        // A new validation starts a new proof boundary and must read again.
        inventory.verified_custody_evidence(5, || Ok(())).unwrap();
        assert_eq!(
            crate::fileops::PAYLOAD_READ_BYTES.with(|n| n.get()),
            2 * bytes.len() as u64
        );
    }

    #[test]
    fn restarting_completed_repair_preserves_proof_and_allocates_fresh_workspace() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("downloads");
        std::fs::create_dir(&root).unwrap();
        let state = temp.path().join("state");
        let inventory = Inventory::open(&state).unwrap();
        let source = root.join("job");
        inventory.allocate(5, &root, &source).unwrap();
        std::fs::write(source.join("episode.mkv"), b"verified repaired media").unwrap();
        let mut first = inventory.workspace(5, "par_repair", "abc").unwrap();
        let attempt = inventory.begin_workspace_attempt(&mut first).unwrap();
        std::fs::write(attempt.join("proof-input"), b"retained scratch").unwrap();
        inventory
            .finish_repair_workspace(&first, &["episode.mkv".into()])
            .unwrap();
        let completed = inventory.operation(&first.operation_id).unwrap();
        drop(inventory);

        let inventory = Inventory::open(&state).unwrap();
        inventory.reconcile_startup(&[5]).unwrap();
        inventory.validate_post_retry(5).unwrap();
        let mut retry = inventory.workspace(5, "par_repair", "abc").unwrap();
        let retry_attempt = inventory.begin_workspace_attempt(&mut retry).unwrap();
        assert_ne!(first.operation_id, retry.operation_id);
        assert_ne!(attempt, retry_attempt);
        assert_eq!(
            std::fs::read(attempt.join("proof-input")).unwrap(),
            b"retained scratch"
        );
        assert_eq!(
            inventory.operation(&first.operation_id).unwrap().request,
            completed.request
        );
        assert_eq!(
            inventory.operation(&first.operation_id).unwrap().state,
            "succeeded"
        );
        let (_, verified) = inventory.verified_transform_evidence(5).unwrap();
        assert!(verified.contains("episode.mkv"));
        assert_eq!(
            inventory
                .workspace(5, "par_repair", "abc")
                .unwrap()
                .operation_id,
            retry.operation_id
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

    /// Rewrite `path` in place with same-length bytes and its original mtime,
    /// so only ctime and content differ. Waits for ctime to advance so a
    /// coarse-timestamp filesystem cannot make the change invisible.
    fn rewrite_preserving_mtime(path: &Path, bytes: &[u8]) {
        let before = std::fs::metadata(path).unwrap();
        assert_eq!(before.len(), bytes.len() as u64);
        let modified = before.modified().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            std::thread::sleep(std::time::Duration::from_millis(5));
            std::fs::write(path, bytes).unwrap();
            std::fs::File::options()
                .write(true)
                .open(path)
                .unwrap()
                .set_modified(modified)
                .unwrap();
            let after = std::fs::metadata(path).unwrap();
            if fs::identity(&after) != fs::identity(&before) {
                assert_eq!(after.modified().unwrap(), modified);
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "filesystem never advanced ctime"
            );
        }
    }

    /// Obfuscated release shape: `<hash>.partNN.rar` holding `<hash>.mkv`.
    /// Extraction publishes the hash-named media, then the final
    /// deobfuscation pass renames media and volumes to the release name.
    fn extracted_then_renamed(
        tamper_before_rename: Option<&str>,
    ) -> (tempfile::TempDir, Inventory, PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("downloads");
        std::fs::create_dir(&root).unwrap();
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        let source = root.join("job");
        inventory
            .allocate_for_delivery(5, &root, &source, true, true)
            .unwrap();
        for part in ["part1", "part2"] {
            std::fs::write(
                source.join(format!("9f8e7d6c5b4a.{part}.rar")),
                format!("volume {part}"),
            )
            .unwrap();
        }
        inventory.finish(5, &source, &root, "active").unwrap();
        let mut workspace = inventory.workspace(5, "extract", "5e7").unwrap();
        inventory
            .bind_extraction_inputs(
                &mut workspace,
                &[
                    source.join("9f8e7d6c5b4a.part1.rar"),
                    source.join("9f8e7d6c5b4a.part2.rar"),
                ],
            )
            .unwrap();
        let attempt = inventory.begin_workspace_attempt(&mut workspace).unwrap();
        let output = attempt.join("output");
        std::fs::create_dir_all(&output).unwrap();
        std::fs::write(output.join("9f8e7d6c5b4a.mkv"), b"extracted episode").unwrap();
        crate::fileops::copy_publish(
            &output.join("9f8e7d6c5b4a.mkv"),
            &source.join("9f8e7d6c5b4a.mkv"),
        )
        .unwrap();
        inventory.finish_extraction(&workspace, &output).unwrap();
        if let Some(name) = tamper_before_rename {
            let path = source.join(name);
            let mut bytes = std::fs::read(&path).unwrap();
            bytes.make_ascii_uppercase();
            rewrite_preserving_mtime(&path, &bytes);
        }
        for (from, to) in [
            ("9f8e7d6c5b4a.mkv", "Show.S01E01.mkv"),
            ("9f8e7d6c5b4a.part1.rar", "Show.S01E01.part1.rar"),
            ("9f8e7d6c5b4a.part2.rar", "Show.S01E01.part2.rar"),
        ] {
            inventory
                .restore_file(5, &source.join(from), &source.join(to))
                .unwrap();
        }
        (temp, inventory, source)
    }

    #[test]
    fn extraction_proof_follows_owned_rename_of_outputs_and_inputs() {
        let (_temp, inventory, _source) = extracted_then_renamed(None);
        let evidence = inventory.verified_custody_evidence(5, || Ok(())).unwrap();
        assert!(
            evidence.verified_outputs.contains("Show.S01E01.mkv"),
            "renamed extraction output keeps its proof: {evidence:?}"
        );
        assert!(evidence.consumed_inputs.contains("Show.S01E01.part1.rar"));
        assert!(evidence.consumed_inputs.contains("Show.S01E01.part2.rar"));
        assert_eq!(
            evidence
                .restored
                .get("9f8e7d6c5b4a.mkv")
                .map(String::as_str),
            Some("Show.S01E01.mkv")
        );
        // The pair API used by older callers sees the same proof.
        let (consumed, verified) = inventory.verified_transform_evidence(5).unwrap();
        assert_eq!(consumed, evidence.consumed_inputs);
        assert_eq!(verified, evidence.verified_outputs);
    }

    #[test]
    fn renamed_output_rewritten_in_place_after_rename_is_not_trusted() {
        let (_temp, inventory, source) = extracted_then_renamed(None);
        // Same inode, size and mtime: only the bytes can tell.
        rewrite_preserving_mtime(&source.join("Show.S01E01.mkv"), b"EXTRACTED EPISODE");
        inventory.refresh_for_delivery(5).unwrap();
        let evidence = inventory.verified_custody_evidence(5, || Ok(())).unwrap();
        assert!(!evidence.verified_outputs.contains("Show.S01E01.mkv"));
    }

    #[test]
    fn output_changed_between_extraction_and_rename_is_not_trusted() {
        let (_temp, inventory, _source) = extracted_then_renamed(Some("9f8e7d6c5b4a.mkv"));
        let evidence = inventory.verified_custody_evidence(5, || Ok(())).unwrap();
        assert!(
            !evidence.verified_outputs.contains("Show.S01E01.mkv"),
            "the rename journal proves only bytes at rename time"
        );
        // The untouched volumes are still the consumed inputs.
        assert!(evidence.consumed_inputs.contains("Show.S01E01.part1.rar"));
    }

    #[test]
    fn input_changed_between_extraction_and_rename_is_not_consumed() {
        let (_temp, inventory, _source) = extracted_then_renamed(Some("9f8e7d6c5b4a.part1.rar"));
        let evidence = inventory.verified_custody_evidence(5, || Ok(())).unwrap();
        assert!(
            !evidence.consumed_inputs.contains("Show.S01E01.part1.rar"),
            "consumed authorizes deletion; it needs the exact bound identity"
        );
        assert!(!evidence.consumed_inputs.contains("9f8e7d6c5b4a.part1.rar"));
        assert!(evidence.consumed_inputs.contains("Show.S01E01.part2.rar"));
        assert!(evidence.verified_outputs.contains("Show.S01E01.mkv"));
    }

    #[test]
    fn a_new_file_at_a_consumed_inputs_former_path_is_not_consumed() {
        let (_temp, inventory, source) = extracted_then_renamed(None);
        std::fs::write(source.join("9f8e7d6c5b4a.part1.rar"), b"volume part1").unwrap();
        inventory.refresh_for_delivery(5).unwrap();
        let evidence = inventory.verified_custody_evidence(5, || Ok(())).unwrap();
        assert!(!evidence.consumed_inputs.contains("9f8e7d6c5b4a.part1.rar"));
        assert!(evidence.consumed_inputs.contains("Show.S01E01.part1.rar"));
    }
}
