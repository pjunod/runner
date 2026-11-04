//! Journaled independent torrent processing copies. Seed bytes remain read-only.
use super::*;
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, Write};
use std::sync::Arc;

/// FD-anchored read-only snapshot used by the native metainfo piece verifier.
/// Selected identities are frozen before verification and reused by copying.
pub struct TorrentSourceReader<'a> {
    root: &'a File,
    seen: std::sync::Mutex<std::collections::HashMap<String, Identity>>,
}
impl TorrentSourceReader<'_> {
    pub fn read_exact_at(
        &self,
        path: &Path,
        offset: u64,
        buffer: &mut [u8],
    ) -> std::io::Result<()> {
        let mut run = || -> Result<()> {
            let name = path
                .to_str()
                .filter(|name| !name.is_empty() && name.len() <= 4096)
                .ok_or_else(|| Error::Conflict("invalid verification source path".into()))?;
            if !path
                .components()
                .all(|part| matches!(part, std::path::Component::Normal(_)))
            {
                return Err(Error::Conflict("unsafe verification source path".into()));
            }
            let mut input = fs::open_relative(self.root, name)?;
            let observed = fs::identity_for_file(&input)?;
            if !input.metadata()?.is_file() {
                return Err(Error::Conflict(
                    "verification source is not a regular file".into(),
                ));
            }
            {
                let mut seen = self.seen.lock().unwrap();
                if seen.get(name).is_some_and(|expected| expected != &observed) {
                    return Err(Error::Conflict(
                        "torrent source changed before piece verification".into(),
                    ));
                }
                seen.entry(name.into()).or_insert_with(|| observed.clone());
            }
            input.seek(std::io::SeekFrom::Start(offset))?;
            input.read_exact(buffer)?;
            if fs::identity_for_file(&input)? != observed {
                return Err(Error::Conflict(
                    "torrent source changed during piece verification".into(),
                ));
            }
            Ok(())
        };
        run().map_err(|error| match error {
            Error::Io(error) => error,
            other => std::io::Error::new(std::io::ErrorKind::InvalidData, other.to_string()),
        })
    }
}
type SourceVerifier<'a> = Option<&'a dyn Fn(&TorrentSourceReader<'_>) -> Result<()>>;
fn verify_copy_files(
    source: &File,
    files: &[CopyFile],
    verifier: SourceVerifier<'_>,
) -> Result<()> {
    if let Some(verify) = verifier {
        let reader = TorrentSourceReader {
            root: source,
            seen: std::sync::Mutex::new(
                files
                    .iter()
                    .map(|file| (file.path.clone(), file.source.clone()))
                    .collect(),
            ),
        };
        verify(&reader)?;
        for entry in files {
            checked_source(source, entry)?;
        }
    }
    Ok(())
}

const MAX_PLAN: usize = 16 * 1024 * 1024;
const MAX_ORPHANS: usize = 16;
const MAX_PROGRESS_ROW: usize = 16 * 1024;
const MAX_PROGRESS_TOTAL: usize = 128 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Stage {
    name: String,
    identity: Option<Identity>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct CopyFile {
    path: String,
    source: Identity,
    stage: Option<Stage>,
    target: Option<Identity>,
    digest: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct CopyDirectory {
    path: String,
    identity: Option<Identity>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct UncommittedStage {
    name: String,
    observed: Option<Identity>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct TorrentCopy {
    job: u32,
    fingerprint: String,
    generation: String,
    donor: u32,
    info_hash: String,
    source_root: PathBuf,
    source_identity: Identity,
    target_root_identity: Identity,
    destination: PathBuf,
    #[serde(skip)]
    files: Vec<CopyFile>,
    #[serde(skip)]
    directories: Vec<CopyDirectory>,
    file_count: usize,
    directory_count: usize,
    selection_digest: String,
    #[serde(default)]
    integrity_verified: bool,
    // These names have no ownership receipt. Never adopt, move or delete them.
    // They live outside the final payload; bounded retention need not strand it.
    uncommitted_stages: Vec<UncommittedStage>,
    compacted_delivery: Option<String>,
    compacted_source: Option<SourceGeneration>,
    compacted_delete: Option<String>,
    compacted_stage_cleanup: Option<String>,
    stage_cleanup_complete: bool,
}

#[derive(Serialize, Deserialize)]
struct SelectionManifest {
    files: Vec<CopyFile>,
    directories: Vec<CopyDirectory>,
}
#[derive(Serialize, Deserialize)]
struct FileProgress {
    binding: String,
    stage: Option<Stage>,
    target: Option<Identity>,
    digest: Option<String>,
}
#[derive(Serialize, Deserialize)]
struct DirectoryProgress {
    binding: String,
    identity: Option<Identity>,
}
fn file_binding(file: &CopyFile) -> Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&(&file.path, &file.source))?)
    ))
}
fn directory_binding(directory: &CopyDirectory) -> String {
    format!("{:x}", Sha256::digest(directory.path.as_bytes()))
}
fn strip_mount(identity: &mut Option<Identity>, root: &Identity) -> Result<()> {
    if let Some(identity) = identity {
        if identity.mount_id != root.mount_id {
            return Err(Error::Conflict(
                "torrent checkpoint moved outside its recorded destination mount".into(),
            ));
        }
        identity.mount_id.clear();
    }
    Ok(())
}
fn restore_mount(identity: &mut Option<Identity>, root: &Identity) -> Result<()> {
    if let Some(identity) = identity {
        if !identity.mount_id.is_empty() {
            return Err(Error::Conflict(
                "torrent checkpoint mount binding changed".into(),
            ));
        }
        identity.mount_id = root.mount_id.clone();
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct StageCleanupStatus {
    job: u32,
    source: SourceGeneration,
    blocked_since: Option<i64>,
    warning: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StageCleanupFile {
    entry: FileEntry,
    outcome: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct StageCleanup {
    source: SourceGeneration,
    deletion: String,
    files: Vec<StageCleanupFile>,
}
fn flat_stage(name: &str) -> bool {
    !name.is_empty()
        && Path::new(name).components().count() == 1
        && Path::new(name)
            .components()
            .all(|part| matches!(part, std::path::Component::Normal(_)))
}
fn retain_unknown(plan: &mut TorrentCopy, name: String, observed: Option<Identity>) -> Result<()> {
    if plan
        .uncommitted_stages
        .iter()
        .any(|entry| entry.name == name)
    {
        return Ok(());
    }
    if plan.uncommitted_stages.len() >= MAX_ORPHANS {
        return Err(Error::Conflict(format!(
            "too many uncommitted torrent staging objects; additional unowned path retained for review: {}",
            plan.destination
                .parent()
                .unwrap_or(Path::new("/"))
                .join(&name)
                .display()
        )));
    }
    plan.uncommitted_stages
        .push(UncommittedStage { name, observed });
    Ok(())
}

#[cfg(test)]
thread_local! {
    static FAULT: std::cell::RefCell<Option<(&'static str, usize)>> = const { std::cell::RefCell::new(None) };
    static HEADROOM: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}
fn boundary(stage: &str, index: usize) -> Result<()> {
    #[cfg(test)]
    if FAULT.with(|fault| {
        let matches = fault
            .borrow()
            .is_some_and(|(name, file)| name == stage && file == index);
        if matches {
            fault.borrow_mut().take();
        }
        matches
    }) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            format!("injected {stage}"),
        )
        .into());
    }
    let _ = (stage, index);
    Ok(())
}
fn encode_receipt(op: &mut Operation, plan: &TorrentCopy) -> Result<()> {
    op.request = serde_json::to_string(plan)?;
    if op.request.len() > 64 * 1024 {
        return Err(Error::Conflict(
            "torrent copy receipt exceeds bounded size; retained for review".into(),
        ));
    }
    Ok(())
}
fn persist(inventory: &Inventory, op: &mut Operation, plan: &TorrentCopy) -> Result<()> {
    encode_receipt(op, plan)?;
    let mut db = inventory.db.lock().unwrap();
    let tx = db.transaction()?;
    let previous: Option<String> = tx
        .query_row(
            "SELECT json_extract(data,'$.request') FROM operations WHERE id=?1",
            [&op.id],
            |row| row.get(0),
        )
        .optional()?;
    let previous_count = previous
        .and_then(|raw| serde_json::from_str::<TorrentCopy>(&raw).ok())
        .map_or(0, |plan| plan.uncommitted_stages.len());
    if plan.uncommitted_stages.len() > previous_count {
        event(
            &tx,
            &op.artifact,
            "torrent_stage_review",
            &unknown_stage_warning(plan).unwrap(),
        )?;
    }
    save_operation(&tx, op)?;
    tx.commit()?;
    Ok(())
}
fn unknown_stage_warning(plan: &TorrentCopy) -> Option<String> {
    if plan.uncommitted_stages.is_empty() {
        return None;
    }
    let root = plan.destination.parent().unwrap_or(Path::new("/"));
    Some(format!(
        "Uncommitted staging objects retained for review because their creation had no durable inode receipt. Ownership and deletion are not authorized. Exact paths: {}",
        plan.uncommitted_stages
            .iter()
            .map(|stage| root.join(&stage.name).display().to_string())
            .collect::<Vec<_>>()
            .join("; ")
    ))
}
fn ensure_tables(inventory: &Inventory) -> Result<()> {
    inventory.db.lock().unwrap().execute_batch("CREATE TABLE IF NOT EXISTS torrent_copy_manifests(operation_id TEXT PRIMARY KEY REFERENCES operations(id) ON DELETE CASCADE,generation TEXT NOT NULL,data TEXT NOT NULL,progress_bytes INTEGER NOT NULL DEFAULT 0,progress_rows INTEGER NOT NULL DEFAULT 0) STRICT; CREATE TABLE IF NOT EXISTS torrent_copy_progress(operation_id TEXT NOT NULL REFERENCES operations(id) ON DELETE CASCADE,generation TEXT NOT NULL,kind TEXT NOT NULL,ordinal INTEGER NOT NULL,data TEXT NOT NULL,PRIMARY KEY(operation_id,generation,kind,ordinal)) STRICT;")?;
    Ok(())
}
fn persist_initial(
    inventory: &Inventory,
    op: &mut Operation,
    plan: &mut TorrentCopy,
) -> Result<()> {
    ensure_tables(inventory)?;
    let manifest = serde_json::to_string(&SelectionManifest {
        files: plan.files.clone(),
        directories: plan.directories.clone(),
    })?;
    if manifest.len() > MAX_PLAN || plan.files.len() + plan.directories.len() > 100_000 {
        return Err(Error::Conflict(
            "torrent selection manifest exceeds supported size; retained for review".into(),
        ));
    }
    plan.file_count = plan.files.len();
    plan.directory_count = plan.directories.len();
    plan.selection_digest = format!("{:x}", Sha256::digest(manifest.as_bytes()));
    encode_receipt(op, plan)?;
    let mut db = inventory.db.lock().unwrap();
    let tx = db.transaction()?;
    save_operation(&tx, op)?;
    tx.execute(
        "INSERT INTO torrent_copy_manifests(operation_id,generation,data) VALUES(?1,?2,?3)",
        params![op.id, plan.generation, manifest],
    )?;
    tx.commit()?;
    Ok(())
}
fn persist_progress(
    inventory: &Inventory,
    op: &Operation,
    plan: &TorrentCopy,
    index: usize,
    directory: bool,
) -> Result<()> {
    let (kind, data) = if directory {
        let entry = &plan.directories[index];
        let mut identity = entry.identity.clone();
        strip_mount(&mut identity, &plan.target_root_identity)?;
        (
            "directory",
            serde_json::to_string(&DirectoryProgress {
                binding: directory_binding(entry),
                identity,
            })?,
        )
    } else {
        let entry = &plan.files[index];
        let mut stage = entry.stage.clone();
        let mut target = entry.target.clone();
        if let Some(stage) = &mut stage {
            strip_mount(&mut stage.identity, &plan.target_root_identity)?;
        }
        strip_mount(&mut target, &plan.target_root_identity)?;
        (
            "file",
            serde_json::to_string(&FileProgress {
                binding: file_binding(entry)?,
                stage,
                target,
                digest: entry.digest.clone(),
            })?,
        )
    };
    if data.len() > MAX_PROGRESS_ROW {
        return Err(Error::Conflict(
            "torrent file checkpoint exceeds bounded size".into(),
        ));
    }
    let mut db = inventory.db.lock().unwrap();
    let tx = db.transaction()?;
    let (total,rows):(i64,i64)=tx.query_row("SELECT progress_bytes,progress_rows FROM torrent_copy_manifests WHERE operation_id=?1 AND generation=?2",params![op.id,plan.generation],|row|Ok((row.get(0)?,row.get(1)?)))?;
    let prior:Option<i64>=tx.query_row("SELECT length(CAST(data AS BLOB)) FROM torrent_copy_progress WHERE operation_id=?1 AND generation=?2 AND kind=?3 AND ordinal=?4",params![op.id,plan.generation,kind,index as i64],|row|row.get(0)).optional()?;
    let next = total
        .checked_sub(prior.unwrap_or(0))
        .and_then(|bytes| bytes.checked_add(data.len() as i64))
        .ok_or_else(|| Error::Conflict("torrent checkpoint byte accounting changed".into()))?;
    let next_rows = rows
        .checked_add(i64::from(prior.is_none()))
        .ok_or_else(|| Error::Conflict("torrent checkpoint row accounting changed".into()))?;
    if next < 0
        || next_rows < 0
        || next > MAX_PROGRESS_TOTAL as i64
        || next_rows > (plan.file_count + plan.directory_count) as i64
    {
        return Err(Error::Conflict(
            "torrent checkpoint aggregate exceeds supported size; retained for review".into(),
        ));
    }
    tx.execute("INSERT INTO torrent_copy_progress(operation_id,generation,kind,ordinal,data) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(operation_id,generation,kind,ordinal) DO UPDATE SET data=excluded.data",params![op.id,plan.generation,kind,index as i64,data])?;
    tx.execute("UPDATE torrent_copy_manifests SET progress_bytes=?3,progress_rows=?4 WHERE operation_id=?1 AND generation=?2",params![op.id,plan.generation,next,next_rows])?;
    tx.commit()?;
    Ok(())
}
fn load_plan(inventory: &Inventory, op: &Operation) -> Result<TorrentCopy> {
    let mut plan: TorrentCopy = serde_json::from_str(&op.request)?;
    if plan.compacted_delivery.is_some() || plan.compacted_delete.is_some() {
        return Ok(plan);
    }
    ensure_tables(inventory)?;
    let expected_count = plan
        .file_count
        .checked_add(plan.directory_count)
        .filter(|count| *count <= 100_000)
        .ok_or_else(|| {
            Error::Conflict("torrent selection count exceeds supported bounds".into())
        })?;
    let (manifest, progress) = {
        let db = inventory.db.lock().unwrap();
        let bounds:Option<(i64,i64,i64)>=db.query_row("SELECT length(CAST(data AS BLOB)),progress_bytes,progress_rows FROM torrent_copy_manifests WHERE operation_id=?1 AND generation=?2",params![op.id,plan.generation],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).optional()?;
        let (manifest_bytes, stored_bytes, stored_rows) = bounds.ok_or_else(|| {
            Error::Conflict("torrent immutable selection manifest is missing".into())
        })?;
        if manifest_bytes < 0
            || stored_bytes < 0
            || stored_rows < 0
            || manifest_bytes > MAX_PLAN as i64
            || stored_bytes > MAX_PROGRESS_TOTAL as i64
            || stored_rows > expected_count as i64
        {
            return Err(Error::Conflict("torrent selection/progress metadata exceeds supported aggregate bounds; retained for review".into()));
        }
        let (rows,largest,total):(i64,i64,i64)=db.query_row("SELECT count(*),COALESCE(max(length(CAST(data AS BLOB))),0),COALESCE(sum(length(CAST(data AS BLOB))),0) FROM torrent_copy_progress WHERE operation_id=?1 AND generation=?2",params![op.id,plan.generation],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?;
        if rows < 0
            || largest < 0
            || total < 0
            || rows > expected_count as i64
            || largest > MAX_PROGRESS_ROW as i64
            || total > MAX_PROGRESS_TOTAL as i64
            || total != stored_bytes
            || rows != stored_rows
        {
            return Err(Error::Conflict("torrent progress metadata exceeds supported row/aggregate bounds; retained for review".into()));
        }
        let manifest: String = db.query_row(
            "SELECT data FROM torrent_copy_manifests WHERE operation_id=?1 AND generation=?2",
            params![op.id, plan.generation],
            |row| row.get(0),
        )?;
        let mut query=db.prepare("SELECT kind,ordinal,data FROM torrent_copy_progress WHERE operation_id=?1 AND generation=?2 ORDER BY kind,ordinal LIMIT 100001")?;
        let rows = query.query_map(params![op.id, plan.generation], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let progress = rows.collect::<std::result::Result<Vec<_>, _>>()?;
        (manifest, progress)
    };
    // Parsing/hashing the bounded bulk state does not hold the actor's DB
    // mutex. The actor completion path reads only the small main receipt.
    if format!("{:x}", Sha256::digest(manifest.as_bytes())) != plan.selection_digest {
        return Err(Error::Conflict(
            "torrent immutable selection manifest changed".into(),
        ));
    }
    let selection: SelectionManifest = serde_json::from_str(&manifest)?;
    if selection.files.len() != plan.file_count
        || selection.directories.len() != plan.directory_count
    {
        return Err(Error::Conflict(
            "torrent immutable selection count changed".into(),
        ));
    }
    plan.files = selection.files;
    plan.directories = selection.directories;
    for (kind, index, raw) in progress {
        if index < 0 {
            return Err(Error::Conflict(
                "torrent checkpoint index is negative".into(),
            ));
        }
        let index = index as usize;
        match kind.as_str() {
            "file" => {
                let mut progress: FileProgress = serde_json::from_str(&raw)?;
                let expected = plan.files.get_mut(index).ok_or_else(|| {
                    Error::Conflict("torrent file checkpoint index changed".into())
                })?;
                if progress.binding != file_binding(expected)? {
                    return Err(Error::Conflict(
                        "torrent file checkpoint differs from immutable selection".into(),
                    ));
                }
                if let Some(stage) = &mut progress.stage {
                    restore_mount(&mut stage.identity, &plan.target_root_identity)?;
                }
                restore_mount(&mut progress.target, &plan.target_root_identity)?;
                expected.stage = progress.stage;
                expected.target = progress.target;
                expected.digest = progress.digest;
            }
            "directory" => {
                let mut progress: DirectoryProgress = serde_json::from_str(&raw)?;
                let expected = plan.directories.get_mut(index).ok_or_else(|| {
                    Error::Conflict("torrent directory checkpoint index changed".into())
                })?;
                if progress.binding != directory_binding(expected) {
                    return Err(Error::Conflict(
                        "torrent directory checkpoint differs from immutable selection".into(),
                    ));
                }
                restore_mount(&mut progress.identity, &plan.target_root_identity)?;
                expected.identity = progress.identity;
            }
            _ => return Err(Error::Conflict("unknown torrent checkpoint kind".into())),
        }
    }
    Ok(plan)
}
fn safe_selection(selected: &[(PathBuf, u64)]) -> Result<()> {
    if selected.is_empty() || selected.len() > 100_000 {
        return Err(Error::Conflict(
            "torrent selection is empty or exceeds supported file count".into(),
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut bytes = 0usize;
    for (path, _) in selected {
        let path = path
            .to_str()
            .ok_or_else(|| Error::Conflict("non-UTF8 torrent filename requires review".into()))?;
        bytes = bytes.saturating_add(path.len());
        if path.is_empty()
            || bytes > MAX_PLAN / 2
            || !Path::new(path)
                .components()
                .all(|part| matches!(part, std::path::Component::Normal(_)))
            || !seen.insert(path)
        {
            return Err(Error::Conflict(
                "unsafe, duplicate or oversized torrent selection path".into(),
            ));
        }
    }
    for (path, _) in selected {
        let mut parent = path.parent();
        while let Some(path) = parent.filter(|path| !path.as_os_str().is_empty()) {
            if seen.contains(path.to_str().unwrap()) {
                return Err(Error::Conflict(
                    "torrent selected file overlaps another selected path".into(),
                ));
            }
            parent = path.parent();
        }
    }
    Ok(())
}
fn fingerprint(
    job: u32,
    donor: u32,
    hash: &str,
    source: &Path,
    selected: &[(PathBuf, u64)],
    root: &Path,
    target: &Path,
) -> Result<String> {
    let raw = serde_json::to_vec(&(job, donor, hash, source, selected, root, target))?;
    if raw.len() > MAX_PLAN {
        return Err(Error::Conflict(
            "torrent selection metadata exceeds supported size".into(),
        ));
    }
    Ok(format!("{:x}", Sha256::digest(raw)))
}
fn transient_use(error: Error) -> Error {
    if matches!(&error,Error::Conflict(reason) if matches!(reason.as_str(), "previous processing workers have not quiesced" | "previous processing or allocation workers have not quiesced"))
    {
        std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "torrent copy source is in use; retry this acquisition",
        )
        .into()
    } else {
        error
    }
}
fn checked_source(root: &File, entry: &CopyFile) -> Result<File> {
    let input = fs::open_relative(root, &entry.path)?;
    if !input.metadata()?.is_file() || fs::identity_for_file(&input)? != entry.source {
        return Err(Error::Conflict(
            "verified torrent source changed; retained for review".into(),
        ));
    }
    Ok(input)
}
// Verify every existing byte against the immutable donor before forecasting
// remaining writes or appending. Partial crash output never authorizes overwrite.
fn verified_prefix(
    input: &mut File,
    existing: &mut File,
    length: u64,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<String> {
    input.rewind()?;
    existing.rewind()?;
    let mut digest = Sha256::new();
    let mut left = length;
    let mut a = [0; 65536];
    let mut b = [0; 65536];
    while left > 0 {
        checkpoint()?;
        let count = left.min(a.len() as u64) as usize;
        input.read_exact(&mut a[..count])?;
        existing.read_exact(&mut b[..count])?;
        if a[..count] != b[..count] {
            return Err(Error::Conflict(
                "torrent private copy bytes changed; retained for review".into(),
            ));
        }
        digest.update(&a[..count]);
        left -= count as u64;
    }
    Ok(format!("{:x}", digest.finalize()))
}

impl Inventory {
    pub fn torrent_copy_ready(&self, job: u32, source: &SourceGeneration) -> Result<bool> {
        let raw:Option<String>=self.db.lock().unwrap().query_row("SELECT data FROM operations WHERE id=?1 AND state='succeeded' AND artifact=?2 AND EXISTS(SELECT 1 FROM artifacts a WHERE a.id=operations.artifact AND a.state NOT IN ('deleted','source_gone')) AND length(CAST(data AS BLOB))<=262144",params![format!("torrent-copy-job-{job}"),source.artifact],|row|row.get(0)).optional()?;
        let Some(raw) = raw else { return Ok(false) };
        let operation: Operation = serde_json::from_str(&raw)?;
        if operation.kind != "torrent-copy" {
            return Ok(false);
        }
        let receipt: TorrentCopy = serde_json::from_str(&operation.request)?;
        Ok(receipt.job == job
            && receipt.generation == source.generation
            && receipt.compacted_delete.is_none())
    }

    /// Only a completed piece-verified private copy grants content evidence.
    /// The caller must freshly validate these identities/digests or a verified
    /// rename before publishing; this DB projection alone grants no authority.
    pub(super) fn torrent_copy_integrity_files(
        &self,
        job: u32,
        source: &Artifact,
    ) -> Result<Vec<FileEntry>> {
        let op = match self.operation(&format!("torrent-copy-job-{job}")) {
            Ok(op) => op,
            Err(Error::NotFound) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        if op.kind != "torrent-copy"
            || op.state != "succeeded"
            || op.artifact != source.id
            || !source.owned
        {
            return Ok(Vec::new());
        }
        let receipt: TorrentCopy = serde_json::from_str(&op.request)?;
        if !receipt.integrity_verified
            || receipt.job != job
            || receipt.generation != source.generation
            || receipt.destination != source.path
            || receipt.compacted_delete.is_some()
            || receipt.compacted_delivery.is_some()
        {
            return Ok(Vec::new());
        }
        let plan = load_plan(self, &op)?;
        plan.files
            .into_iter()
            .map(|file| {
                Ok(FileEntry {
                    path: file.path,
                    identity: file.target.ok_or_else(|| {
                        Error::Conflict("verified torrent copy lacks target identity".into())
                    })?,
                    digest: Some(
                        file.digest
                            .filter(|digest| {
                                digest.len() == 64
                                    && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
                            })
                            .ok_or_else(|| {
                                Error::Conflict("verified torrent copy lacks durable digest".into())
                            })?,
                    ),
                })
            })
            .collect()
    }

    /// Reading a legacy/unowned donor creates no deletion authority. A known
    /// owned donor is pinned by the existing exact-generation use registry.
    fn pin_torrent_donor(
        self: &Arc<Self>,
        donor: u32,
        source_root: &Path,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Option<AttemptUse>> {
        let initial = self.for_job(donor)?;
        if let Some(source) = initial.filter(|artifact| artifact.owned) {
            if source.path != source_root {
                return Err(Error::Conflict(
                    "owned torrent donor path differs from its current source generation".into(),
                ));
            }
            let guard = self
                .acquire_terminal_use(donor, checkpoint)
                .map_err(transient_use)?
                .ok_or(Error::NotFound)?;
            let current = self.for_job(donor)?.ok_or(Error::NotFound)?;
            if guard.source().artifact != source.id
                || guard.source().generation != source.generation
                || current.id != source.id
                || current.generation != source.generation
                || current.path != source_root
            {
                return Err(Error::Conflict(
                    "owned torrent donor generation changed".into(),
                ));
            }
            self.verify(&current)?;
            return Ok(Some(guard));
        }
        if self
            .for_path(source_root)?
            .is_some_and(|source| source.owned)
        {
            return Err(Error::Conflict(
                "torrent donor belongs to another owned source; review required".into(),
            ));
        }
        Ok(None)
    }

    #[allow(clippy::too_many_arguments)] // Explicit acquisition/custody boundary shared with native admission.
    pub fn copy_torrent_acquisition(
        self: &Arc<Self>,
        job: u32,
        donor: u32,
        info_hash: &str,
        source_root: &Path,
        selected: &[(PathBuf, u64)],
        destination_root: &Path,
        destination: &Path,
        keep_on_failure: bool,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Artifact> {
        self.copy_torrent_acquisition_with_verifier(
            job,
            donor,
            info_hash,
            source_root,
            selected,
            destination_root,
            destination,
            keep_on_failure,
            None,
            checkpoint,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub fn copy_torrent_acquisition_verified(
        self: &Arc<Self>,
        job: u32,
        donor: u32,
        info_hash: &str,
        source_root: &Path,
        selected: &[(PathBuf, u64)],
        destination_root: &Path,
        destination: &Path,
        keep_on_failure: bool,
        verifier: &dyn Fn(&TorrentSourceReader<'_>) -> Result<()>,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Artifact> {
        self.copy_torrent_acquisition_with_verifier(
            job,
            donor,
            info_hash,
            source_root,
            selected,
            destination_root,
            destination,
            keep_on_failure,
            Some(verifier),
            checkpoint,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn copy_torrent_acquisition_with_verifier(
        self: &Arc<Self>,
        job: u32,
        donor: u32,
        info_hash: &str,
        source_root: &Path,
        selected: &[(PathBuf, u64)],
        destination_root: &Path,
        destination: &Path,
        keep_on_failure: bool,
        verifier: SourceVerifier<'_>,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Artifact> {
        checkpoint()?;
        safe_selection(selected)?;
        fs::absolute(source_root)?;
        fs::absolute(destination_root)?;
        fs::absolute(destination)?;
        if info_hash.len() != 40
            || !info_hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            || job == donor
            || destination.starts_with(source_root)
            || source_root.starts_with(destination)
        {
            return Err(Error::Conflict(
                "invalid or overlapping torrent copy identity".into(),
            ));
        }
        let fingerprint = fingerprint(
            job,
            donor,
            info_hash,
            source_root,
            selected,
            destination_root,
            destination,
        )?;
        let key = format!("torrent-copy-job-{job}");
        let existing = match self.operation(&key) {
            Ok(op) => Some(op),
            Err(Error::NotFound) => None,
            Err(error) => return Err(error),
        };
        if let Some(op) = &existing {
            let plan: TorrentCopy = serde_json::from_str(&op.request)?;
            if plan.fingerprint != fingerprint || plan.job != job {
                return Err(Error::Conflict("torrent copy replay inputs changed".into()));
            }
            if plan.compacted_delete.is_some() {
                return Err(Error::Conflict(
                    "torrent acquisition was deleted; use a fresh request".into(),
                ));
            }
            if let Some(delivery_id) = &plan.compacted_delivery {
                let delivery = self.delivery(delivery_id)?;
                if op.state != "succeeded"
                    || delivery.job_id != job
                    || plan.compacted_source.as_ref()
                        != Some(&SourceGeneration {
                            artifact: delivery.source_artifact_id.clone(),
                            generation: delivery.source_generation.clone(),
                        })
                {
                    return Err(Error::Conflict(
                        "torrent terminal replay proof is inconsistent".into(),
                    ));
                }
                // This is only a terminal no-op receipt. It allocates no path,
                // rehashes no retired donor and grants no cleanup authority.
                let artifact = self.get(&op.artifact)?;
                if artifact.generation != plan.generation {
                    return Err(Error::Conflict(
                        "torrent compact replay generation changed".into(),
                    ));
                }
                return Ok(artifact);
            }
            if op.state == "succeeded" {
                let artifact = self.get(&op.artifact)?;
                if artifact.generation != plan.generation || artifact.terminal() {
                    return Err(Error::Conflict(
                        "completed torrent copy source generation is unavailable".into(),
                    ));
                }
                return Ok(artifact);
            }
        }
        let _donor_use = self.pin_torrent_donor(donor, source_root, checkpoint)?;
        let source = fs::open_dir(source_root)?;
        let source_identity = fs::identity_for_file(&source)?;
        if let Some(guard) = &_donor_use {
            let owned = self.get(&guard.source().artifact)?;
            if owned.generation != guard.source().generation
                || !owned
                    .identity
                    .as_ref()
                    .is_some_and(|identity| identity.same_object(&source_identity))
            {
                return Err(Error::Conflict(
                    "owned torrent donor directory identity changed".into(),
                ));
            }
        }
        let artifact = self.allocate_for_delivery_with_keep(
            job,
            destination_root,
            destination,
            true,
            true,
            keep_on_failure,
        )?;
        if !artifact.owned || artifact.path != destination || artifact.state != "active" {
            return Err(Error::Conflict(
                "torrent processing copy has no current owned allocation".into(),
            ));
        }
        let _target_use = self
            .acquire_attempt_use_checked(job, checkpoint)
            .map_err(transient_use)?
            .ok_or(Error::NotFound)?;
        let target = self.verify(&artifact)?;
        let root = self.verify_root(&artifact)?;
        let replaying = existing.is_some();
        let mut op = if let Some(op) = existing {
            op
        } else {
            let mut files = Vec::new();
            let mut directories = std::collections::BTreeSet::new();
            for (path, length) in selected {
                let input = fs::open_relative(&source, path.to_str().unwrap())?;
                let identity = fs::identity_for_file(&input)?;
                if !input.metadata()?.is_file() || identity.directory || identity.bytes != *length {
                    return Err(Error::Conflict(
                        "verified torrent file identity or length changed".into(),
                    ));
                }
                let mut parent = path.parent();
                while let Some(path) = parent.filter(|path| !path.as_os_str().is_empty()) {
                    directories.insert(path.to_str().unwrap().to_string());
                    parent = path.parent();
                }
                files.push(CopyFile {
                    path: path.to_str().unwrap().into(),
                    source: identity,
                    stage: None,
                    target: None,
                    digest: None,
                });
            }
            verify_copy_files(&source, &files, verifier)?;
            let mut directories: Vec<_> = directories
                .into_iter()
                .map(|path| CopyDirectory {
                    path,
                    identity: None,
                })
                .collect();
            directories.sort_by_key(|dir| Path::new(&dir.path).components().count());
            let mut plan = TorrentCopy {
                job,
                fingerprint: fingerprint.clone(),
                generation: artifact.generation.clone(),
                donor,
                info_hash: info_hash.into(),
                source_root: source_root.into(),
                source_identity: source_identity.clone(),
                target_root_identity: artifact.root_identity.clone(),
                destination: destination.into(),
                files,
                directories,
                file_count: 0,
                directory_count: 0,
                selection_digest: String::new(),
                integrity_verified: verifier.is_some(),
                uncommitted_stages: Vec::new(),
                compacted_delivery: None,
                compacted_source: None,
                compacted_delete: None,
                compacted_stage_cleanup: None,
                stage_cleanup_complete: false,
            };
            let mut op = Operation {
                id: key,
                artifact: artifact.id.clone(),
                kind: "torrent-copy".into(),
                state: "running".into(),
                request: String::new(),
                created_at: now(),
                not_before: now(),
                attempts: 0,
                next_retry: 0,
                error: None,
            };
            persist_initial(self, &mut op, &mut plan)?;
            op
        };
        let mut plan = load_plan(self, &op)?;
        if replaying {
            verify_copy_files(&source, &plan.files, verifier)?;
            if verifier.is_some() && !plan.integrity_verified {
                plan.integrity_verified = true;
                persist(self, &mut op, &plan)?;
            }
        }
        if plan.generation != artifact.generation
            || op.artifact != artifact.id
            || !plan.source_identity.same_object(&source_identity)
            || plan.files.len() != selected.len()
            || plan
                .files
                .iter()
                .zip(selected)
                .any(|(file, (path, length))| {
                    Path::new(&file.path) != path || file.source.bytes != *length
                })
        {
            return Err(Error::Conflict(
                "torrent copy source, generation or selection changed".into(),
            ));
        }
        if plan
            .files
            .iter()
            .filter_map(|file| file.stage.as_ref())
            .any(|stage| !flat_stage(&stage.name))
            || plan
                .uncommitted_stages
                .iter()
                .any(|stage| !flat_stage(&stage.name))
        {
            return Err(Error::Conflict(
                "unsafe torrent staging journal path".into(),
            ));
        }
        let result = self.resume_torrent_copy(
            &artifact, &root, &target, &source, &mut op, &mut plan, checkpoint,
        );
        if let Err(error) = &result {
            op.state = if matches!(error, Error::Conflict(_)) {
                "review"
            } else {
                "retry"
            }
            .into();
            op.error = Some(error.to_string());
            persist(self, &mut op, &plan)?;
        }
        result
    }

    #[allow(clippy::too_many_arguments)] // Handles and journal travel together through one copy attempt.
    fn resume_torrent_copy(
        &self,
        artifact: &Artifact,
        root: &File,
        target: &File,
        source: &File,
        op: &mut Operation,
        plan: &mut TorrentCopy,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Artifact> {
        // An interrupted create with no inode receipt is unowned. Record it
        // outside the final payload and choose a fresh name; never adopt it.
        for index in 0..plan.files.len() {
            let entry = plan.files[index].clone();
            if let Some(stage) = entry.stage.filter(|stage| stage.identity.is_none()) {
                match fs::open_at(root, Path::new(&stage.name), false) {
                    Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
                    observed => {
                        retain_unknown(
                            plan,
                            stage.name,
                            observed
                                .ok()
                                .and_then(|file| fs::identity_for_file(&file).ok()),
                        )?;
                        // Retain the exact unknown path durably before clearing
                        // its per-file pointer. Either crash order keeps a
                        // visible review route; replay never adopts the inode.
                        persist(self, op, plan)?;
                        boundary("unknown_retained", index)?;
                        plan.files[index].stage = None;
                        persist_progress(self, op, plan, index, false)?;
                    }
                }
            }
        }
        let mut remaining = 0u64;
        for entry in &plan.files {
            checkpoint()?;
            let mut input = checked_source(source, entry)?;
            if entry.target.is_none() {
                match fs::open_relative(target, &entry.path) {
                    Ok(current) => {
                        let identity = fs::identity_for_file(&current)?;
                        if entry.digest.is_none()
                            || !entry
                                .stage
                                .as_ref()
                                .and_then(|stage| stage.identity.as_ref())
                                .is_some_and(|expected| expected.same_object(&identity))
                        {
                            return Err(Error::Conflict(
                                "unknown torrent final target requires review".into(),
                            ));
                        }
                    }
                    Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => {
                        return Err(Error::Conflict(
                            "unsafe torrent final target requires review".into(),
                        ));
                    }
                }
            }
            let progress = if let Some(expected) = &entry.target {
                let mut output = fs::open_relative(target, &entry.path)?;
                if fs::identity_for_file(&output)? != *expected || entry.digest.is_none() {
                    return Err(Error::Conflict(
                        "published torrent copy identity changed".into(),
                    ));
                }
                let digest =
                    verified_prefix(&mut input, &mut output, entry.source.bytes, checkpoint)?;
                if entry.digest.as_ref() != Some(&digest) {
                    return Err(Error::Conflict(
                        "published torrent copy digest changed".into(),
                    ));
                }
                entry.source.bytes
            } else if let Some(stage) = entry
                .stage
                .as_ref()
                .filter(|stage| stage.identity.is_some())
            {
                let existing = fs::open_at(root, Path::new(&stage.name), false);
                // Exclusive rename may have committed before the journal did.
                let mut output = match existing {
                    Ok(file) => file,
                    Err(Error::Io(error))
                        if error.kind() == std::io::ErrorKind::NotFound
                            && entry.digest.is_some() =>
                    {
                        fs::open_relative(target, &entry.path)?
                    }
                    Err(Error::Io(error))
                        if error.kind() == std::io::ErrorKind::NotFound
                            || error.raw_os_error() == Some(libc::ELOOP) =>
                    {
                        return Err(Error::Conflict(format!(
                            "journaled torrent staging path changed: {error}"
                        )));
                    }
                    Err(error) => return Err(error),
                };
                let identity = fs::identity_for_file(&output)?;
                if !stage.identity.as_ref().unwrap().same_object(&identity)
                    || identity.bytes > entry.source.bytes
                {
                    return Err(Error::Conflict(
                        "torrent staging identity changed; retained for review".into(),
                    ));
                }
                match verified_prefix(&mut input, &mut output, identity.bytes, checkpoint) {
                    Ok(digest) => {
                        if entry.digest.as_ref().is_some_and(|saved| saved != &digest) {
                            return Err(Error::Conflict(
                                "finished torrent staging bytes changed".into(),
                            ));
                        }
                        identity.bytes
                    }
                    Err(Error::Conflict(_)) if entry.digest.is_none() => {
                        // No completed-byte proof exists. An exact journal-owned
                        // partial inode can contain a torn write after a crash.
                        // Restart only this inode, under the exclusive copy lease.
                        if fs::identity_for_file(&input)? != entry.source
                            || fs::identity_for_file(&output)? != identity
                        {
                            return Err(Error::Conflict(
                                "torrent donor or partial stage changed during recovery".into(),
                            ));
                        }
                        checkpoint()?;
                        let writable =
                            crate::fileops::writer(&artifact.root.join(&stage.name), false)?;
                        if fs::identity_for_file(&writable)? != identity {
                            return Err(Error::Conflict(
                                "torrent partial stage writer changed".into(),
                            ));
                        }
                        writable.set_len(0)?;
                        writable.sync_all()?;
                        0
                    }
                    Err(error) => return Err(error),
                }
            } else {
                0
            };
            if fs::identity_for_file(&input)? != entry.source {
                return Err(Error::Conflict(
                    "torrent donor changed while verifying resume".into(),
                ));
            }
            remaining = remaining
                .checked_add(entry.source.bytes - progress)
                .ok_or_else(|| Error::Conflict("torrent size overflow".into()))?;
        }
        #[cfg(test)]
        if HEADROOM.with(|limit| limit.get().is_some_and(|bytes| remaining > bytes)) {
            return Err(std::io::Error::from_raw_os_error(libc::ENOSPC).into());
        }
        // Completed digest-verified files need no new reservation, including
        // a fully written stage awaiting only its metadata/rename commit.
        let _capacity = if remaining == 0 {
            None
        } else {
            Some(crate::capacity::reserve(&artifact.root, remaining)?)
        };
        for index in 0..plan.directories.len() {
            checkpoint()?;
            let entry = plan.directories[index].clone();
            if let Some(expected) = entry.identity {
                let directory = fs::open_relative(target, &entry.path)?;
                if !expected.same_object(&fs::identity_for_file(&directory)?) {
                    return Err(Error::Conflict("torrent target directory changed".into()));
                }
            } else {
                if fs::open_relative(target, &entry.path).is_ok() {
                    return Err(Error::Conflict(
                        "unknown torrent target directory requires review".into(),
                    ));
                }
                crate::fileops::parents(&artifact.path, Path::new(&entry.path))?;
                let directory = fs::open_relative(target, &entry.path)?;
                plan.directories[index].identity = Some(fs::identity_for_file(&directory)?);
                persist_progress(self, op, plan, index, true)?;
            }
        }
        for index in 0..plan.files.len() {
            checkpoint()?;
            if plan.files[index].target.is_some() {
                continue;
            }
            if plan.files[index].stage.is_none() {
                let nonce = id(&self.db.lock().unwrap())?;
                plan.files[index].stage = Some(Stage {
                    name: format!(".torrent-copy-{}-{nonce}.part", artifact.generation),
                    identity: None,
                });
                persist_progress(self, op, plan, index, false)?;
                boundary("stage_intent", index)?;
            }
            if plan.files[index].stage.as_ref().unwrap().identity.is_none() {
                let stage = plan.files[index].stage.as_ref().unwrap();
                let output = fs::create_file_exclusive_at(root, Path::new(&stage.name))?;
                output.sync_all()?;
                fs::sync_directory(root)?;
                boundary("stage_created", index)?;
                plan.files[index].stage.as_mut().unwrap().identity =
                    Some(fs::identity_for_file(&output)?);
                persist_progress(self, op, plan, index, false)?;
                boundary("stage_identity", index)?;
            }
            let entry = plan.files[index].clone();
            let stage = entry.stage.as_ref().unwrap();
            let stage_path = artifact.root.join(&stage.name);
            let stage_open = fs::open_at(root, Path::new(&stage.name), false);
            if let Ok(mut staged) = stage_open {
                let identity = fs::identity_for_file(&staged)?;
                if !stage.identity.as_ref().unwrap().same_object(&identity) {
                    return Err(Error::Conflict(
                        "torrent staging object was replaced".into(),
                    ));
                }
                let mut input = checked_source(source, &entry)?;
                verified_prefix(&mut input, &mut staged, identity.bytes, checkpoint)?;
                let mut output = crate::fileops::writer(&stage_path, false)?;
                if !identity.same_object(&fs::identity_for_file(&output)?) {
                    return Err(Error::Conflict(
                        "torrent staging writer identity changed".into(),
                    ));
                }
                output.seek(std::io::SeekFrom::Start(identity.bytes))?;
                let mut buffer = [0; 65536];
                loop {
                    checkpoint()?;
                    let count = input.read(&mut buffer)?;
                    if count == 0 {
                        break;
                    }
                    output.write_all(&buffer[..count])?;
                    boundary("copy_chunk", index)?;
                }
                output.sync_all()?;
                let mut copied = fs::open_at(root, Path::new(&stage.name), false)?;
                let current = fs::identity_for_file(&copied)?;
                if !identity.same_object(&current)
                    || current.bytes != entry.source.bytes
                    || fs::identity_for_file(&input)? != entry.source
                {
                    return Err(Error::Conflict("torrent copy changed while writing".into()));
                }
                let digest =
                    verified_prefix(&mut input, &mut copied, entry.source.bytes, checkpoint)?;
                if fs::identity_for_file(&copied)? != current
                    || fs::identity_for_file(&input)? != entry.source
                {
                    return Err(Error::Conflict("torrent copy changed while hashing".into()));
                }
                plan.files[index].digest = Some(digest);
                plan.files[index].stage.as_mut().unwrap().identity = Some(current);
                persist_progress(self, op, plan, index, false)?;
                boundary("stage_complete", index)?;
                checkpoint()?;
                crate::fileops::rename_exclusive(&stage_path, &artifact.path.join(&entry.path))?;
                boundary("published", index)?;
            } else if !matches!(stage_open,Err(Error::Io(ref error)) if error.kind()==std::io::ErrorKind::NotFound)
                || entry.digest.is_none()
            {
                return Err(Error::Conflict(
                    "torrent staging object is unavailable".into(),
                ));
            }
            let mut published = fs::open_relative(target, &entry.path)?;
            let current = fs::identity_for_file(&published)?;
            let expected = plan.files[index]
                .stage
                .as_ref()
                .unwrap()
                .identity
                .as_ref()
                .unwrap();
            if !expected.same_object(&current) || current.bytes != entry.source.bytes {
                return Err(Error::Conflict(
                    "torrent final target is not the journaled private copy".into(),
                ));
            }
            let mut input = checked_source(source, &entry)?;
            let digest =
                verified_prefix(&mut input, &mut published, entry.source.bytes, checkpoint)?;
            if plan.files[index].digest.as_ref() != Some(&digest)
                || fs::identity_for_file(&published)? != current
                || fs::identity_for_file(&input)? != entry.source
            {
                return Err(Error::Conflict("torrent final target changed".into()));
            }
            plan.files[index].target = Some(current);
            persist_progress(self, op, plan, index, false)?;
            boundary("target_identity", index)?;
        }
        let manifest = fs::manifest(target, 100_000)?;
        let expected_files: std::collections::BTreeMap<_, _> = plan
            .files
            .iter()
            .map(|file| (file.path.as_str(), file.target.as_ref()))
            .collect();
        let expected_directories: std::collections::BTreeMap<_, _> = plan
            .directories
            .iter()
            .map(|directory| (directory.path.as_str(), directory.identity.as_ref()))
            .collect();
        if manifest.len() != expected_files.len() + expected_directories.len()
            || manifest.iter().any(|file| {
                if file.identity.directory {
                    !expected_directories
                        .get(file.path.as_str())
                        .and_then(|identity| *identity)
                        .is_some_and(|expected| expected.same_object(&file.identity))
                } else {
                    expected_files
                        .get(file.path.as_str())
                        .and_then(|identity| *identity)
                        != Some(&file.identity)
                }
            })
        {
            return Err(Error::Conflict(
                "unknown or missing path in torrent processing copy; retained for review".into(),
            ));
        }
        checkpoint()?;
        let observed_source = fs::open_dir(&plan.source_root)?;
        if !plan
            .source_identity
            .same_object(&fs::identity_for_file(&observed_source)?)
        {
            return Err(Error::Conflict(
                "torrent donor path changed during copy".into(),
            ));
        }
        let finished = self.finish(plan.job, &artifact.path, &artifact.root, "active")?;
        op.state = "succeeded".into();
        op.error = unknown_stage_warning(plan);
        persist(self, op, plan)?;
        Ok(finished)
    }

    /// Clean only journal-owned staging objects after an exact completed
    /// inventory Delete. Queue absence, missing payload paths and timeouts are
    /// never cleanup authority. Explicit delete-files overrides Keep; automatic
    /// failure deletion remains subject to the current retention controls.
    pub fn cleanup_torrent_copy_stages(
        &self,
        job: u32,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Operation> {
        let result = self.cleanup_torrent_copy_stages_inner(job, checkpoint);
        // A failed/live copy is never rewritten to record cleanup status. In
        // particular WouldBlock cannot roll back a newer per-file checkpoint.
        let _guard = self.mutation_guard()?;
        if let Ok(copy) = self.operation(&format!("torrent-copy-job-{job}")) {
            let receipt: TorrentCopy = serde_json::from_str(&copy.request)?;
            let id = format!("torrent-stage-status-job-{job}");
            let mut status = match self.operation(&id) {
                Ok(op) => op,
                Err(Error::NotFound) => Operation {
                    id,
                    artifact: copy.artifact.clone(),
                    kind: "torrent-stage-cleanup-status".into(),
                    state: "pending".into(),
                    request: serde_json::to_string(&StageCleanupStatus {
                        job,
                        source: SourceGeneration {
                            artifact: copy.artifact.clone(),
                            generation: receipt.generation.clone(),
                        },
                        blocked_since: None,
                        warning: None,
                    })?,
                    created_at: now(),
                    not_before: now(),
                    attempts: 0,
                    next_retry: 0,
                    error: None,
                },
                Err(error) => return Err(error),
            };
            let mut details: StageCleanupStatus = serde_json::from_str(&status.request)?;
            if details.job != job
                || details.source.artifact != copy.artifact
                || details.source.generation != receipt.generation
            {
                return Err(Error::Conflict(
                    "torrent staging status generation changed".into(),
                ));
            }
            match &result {
                Ok(op) => {
                    status.state = op.state.clone();
                    status.error = op.error.clone();
                    status.next_retry = 0;
                    if op.state == "succeeded" {
                        details.blocked_since = None;
                        details.warning = None;
                    }
                }
                Err(error) => {
                    status.attempts = status.attempts.saturating_add(1);
                    let blocked = *details.blocked_since.get_or_insert_with(now);
                    status.state = if matches!(error, Error::Conflict(_)) {
                        "review"
                    } else {
                        "retry"
                    }
                    .into();
                    status.error = Some(format!("Staging cleanup pending: {error}"));
                    let jitter_key = format!("{:x}", Sha256::digest(status.id.as_bytes()));
                    status.next_retry = if status.state == "review" {
                        0
                    } else {
                        url_storage_retry_deadline(
                            &jitter_key,
                            status.attempts.saturating_sub(1),
                            now(),
                            status.next_retry,
                            None,
                        )
                    };
                    details.warning=(now().saturating_sub(blocked)>=30*60).then(||"Staging cleanup has been blocked for at least 30 minutes; review the recorded cause.".into());
                }
            }
            status.request = serde_json::to_string(&details)?;
            save_operation(&self.db.lock().unwrap(), &status)?;
        }
        result
    }
    fn cleanup_torrent_copy_stages_inner(
        &self,
        job: u32,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Operation> {
        let _guard = self.mutation_guard()?;
        checkpoint()?;
        let mut copy = self.operation(&format!("torrent-copy-job-{job}"))?;
        let mut plan: TorrentCopy = serde_json::from_str(&copy.request)?;
        if let Some(id) = &plan.compacted_stage_cleanup {
            return self.operation(id);
        }
        let source = self.get(&copy.artifact)?;
        if source.generation != plan.generation || !source.owned {
            return Err(Error::Conflict(
                "torrent staging source generation changed".into(),
            ));
        }
        if self.artifact_in_use(&source)? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "torrent copy workers have not quiesced",
            )
            .into());
        }
        if source.state != "deleted" || source.hold.is_some() {
            return Err(Error::Conflict(
                "torrent stage cleanup requires an exact deleted owned source generation".into(),
            ));
        }
        let candidates = {
            let db = self.db.lock().unwrap();
            let mut query=db.prepare("SELECT data FROM operations WHERE artifact=?1 AND state='succeeded' AND json_extract(data,'$.kind')='delete' ORDER BY rowid DESC LIMIT 2")?;
            let rows = query.query_map([&source.id], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let mut authorized = Vec::new();
        for raw in candidates {
            let deletion: Operation = serde_json::from_str(&raw)?;
            let request: DeleteRequest = serde_json::from_str(&deletion.request)?;
            let (artifact, _, _, automatic, generation) = request.fields();
            if artifact == source.id && generation == Some(plan.generation.as_str()) {
                authorized.push((deletion.id, automatic));
            }
        }
        if authorized.len() != 1 {
            return Err(Error::Conflict(
                "torrent stages have no unique exact completed Delete receipt".into(),
            ));
        }
        let (deletion, automatic) = &authorized[0];
        if *automatic && (source.keep || source.keep_on_failure) {
            return Err(Error::Conflict(
                "current Keep protects automatic torrent staging cleanup".into(),
            ));
        }
        plan = load_plan(self, &copy)?;
        let root = self.verify_root(&source)?;
        let cleanup_id = format!("torrent-stage-cleanup-{}-{}", source.id, source.generation);
        let mut op = match self.operation(&cleanup_id) {
            Ok(op) => op,
            Err(Error::NotFound) => {
                let mut files = Vec::new();
                for file in plan.files.clone() {
                    let Some(stage) = &file.stage else {
                        continue;
                    };
                    if !flat_stage(&stage.name) {
                        return Err(Error::Conflict(
                            "unsafe torrent staging cleanup path".into(),
                        ));
                    }
                    let observed = fs::open_at(&root, Path::new(&stage.name), false);
                    if file.target.is_some() || stage.identity.is_none() {
                        if !matches!(&observed,Err(Error::Io(error)) if error.kind()==std::io::ErrorKind::NotFound)
                        {
                            retain_unknown(
                                &mut plan,
                                stage.name.clone(),
                                observed
                                    .ok()
                                    .and_then(|file| fs::identity_for_file(&file).ok()),
                            )?;
                        }
                        continue;
                    }
                    let expected = stage.identity.as_ref().unwrap();
                    match observed {
                        Ok(current) => {
                            let identity = fs::identity_for_file(&current)?;
                            if !expected.same_object(&identity)
                                || identity.bytes > file.source.bytes
                                || file.digest.is_some() && identity != *expected
                            {
                                return Err(Error::Conflict(
                                    "torrent staging cleanup identity changed; retained for review"
                                        .into(),
                                ));
                            }
                            files.push(StageCleanupFile {
                                entry: FileEntry {
                                    path: stage.name.clone(),
                                    identity,
                                    digest: file.digest.clone(),
                                },
                                outcome: None,
                            });
                        }
                        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error),
                    }
                }
                // Persist unknown-object retention even when no owned stage
                // remains. This is review evidence, never adoption authority.
                persist(self, &mut copy, &plan)?;
                let data = StageCleanup {
                    source: SourceGeneration {
                        artifact: source.id.clone(),
                        generation: source.generation.clone(),
                    },
                    deletion: deletion.clone(),
                    files,
                };
                let op = Operation {
                    id: cleanup_id,
                    artifact: source.id.clone(),
                    kind: "torrent-stage-cleanup".into(),
                    state: "running".into(),
                    request: serde_json::to_string(&data)?,
                    created_at: now(),
                    not_before: now(),
                    attempts: 0,
                    next_retry: 0,
                    error: None,
                };
                save_operation(&self.db.lock().unwrap(), &op)?;
                op
            }
            Err(error) => return Err(error),
        };
        let mut cleanup: StageCleanup = serde_json::from_str(&op.request)?;
        if cleanup.source.artifact != source.id
            || cleanup.source.generation != source.generation
            || cleanup.deletion != *deletion
        {
            return Err(Error::Conflict(
                "torrent staging cleanup receipt changed".into(),
            ));
        }
        for index in 0..cleanup.files.len() {
            checkpoint()?;
            if cleanup.files[index].outcome.is_some() {
                continue;
            }
            let entry = &cleanup.files[index].entry;
            if !flat_stage(&entry.path)
                || !plan.files.iter().any(|file| {
                    file.target.is_none()
                        && file.stage.as_ref().is_some_and(|stage| {
                            stage.name == entry.path
                                && stage
                                    .identity
                                    .as_ref()
                                    .is_some_and(|identity| identity.same_object(&entry.identity))
                        })
                })
            {
                return Err(Error::Conflict(
                    "torrent stage cleanup no longer matches copy ownership".into(),
                ));
            }
            let outcome = match fs::open_at(&root, Path::new(&entry.path), false) {
                Ok(_) => {
                    fs::remove_entry(&root, entry)?;
                    fs::sync_directory(&root)?;
                    boundary("stage_unlinked", index)?;
                    "removed"
                }
                Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    "absence_confirmed"
                }
                Err(error) => return Err(error),
            };
            cleanup.files[index].outcome = Some(outcome.into());
            op.request = serde_json::to_string(&cleanup)?;
            save_operation(&self.db.lock().unwrap(), &op)?;
        }
        op.state = if plan.uncommitted_stages.is_empty() {
            "succeeded"
        } else {
            "review"
        }
        .into();
        op.error = unknown_stage_warning(&plan);
        plan.stage_cleanup_complete = true;
        plan.compacted_delete = Some(deletion.clone());
        plan.compacted_stage_cleanup = Some(op.id.clone());
        plan.files.clear();
        plan.directories.clear();
        copy.state = "cancelled".into();
        copy.next_retry = 0;
        if op.error.is_some() {
            copy.error = op.error.clone();
        }
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        encode_receipt(&mut copy, &plan)?;
        tx.execute(
            "DELETE FROM torrent_copy_progress WHERE operation_id=?1 AND generation=?2",
            params![copy.id, plan.generation],
        )?;
        tx.execute(
            "DELETE FROM torrent_copy_manifests WHERE operation_id=?1 AND generation=?2",
            params![copy.id, plan.generation],
        )?;
        save_operation(&tx, &copy)?;
        save_operation(&tx, &op)?;
        tx.commit()?;
        Ok(op)
    }

    /// Independent maintenance/restart discovery closes the crash gap between
    /// committed child deletion and its optional immediate cleanup follow-up.
    pub fn reconcile_torrent_stage_cleanups(
        &self,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<usize> {
        let cursor_key = "torrent_stage_cleanup_cursor";
        let jobs = {
            let db = self.db.lock().unwrap();
            let cursor: Option<String> = db
                .query_row("SELECT value FROM meta WHERE key=?1", [cursor_key], |row| {
                    row.get(0)
                })
                .optional()?;
            let cursor = cursor
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0);
            let query_page = |after: i64| -> Result<Vec<(i64, u32)>> {
                let mut query=db.prepare("SELECT o.rowid,json_extract(json_extract(o.data,'$.request'),'$.job') FROM operations o JOIN artifacts a ON a.id=o.artifact LEFT JOIN operations s ON s.id='torrent-stage-status-job-'||json_extract(json_extract(o.data,'$.request'),'$.job') WHERE o.rowid>?1 AND json_extract(o.data,'$.kind')='torrent-copy' AND a.state='deleted' AND COALESCE(json_extract(json_extract(o.data,'$.request'),'$.stage_cleanup_complete'),0)=0 AND (s.id IS NULL OR (s.state!='review' AND COALESCE(json_extract(s.data,'$.next_retry'),0)<=?2)) ORDER BY o.rowid LIMIT 64")?;
                let rows =
                    query.query_map(params![after, now()], |row| Ok((row.get(0)?, row.get(1)?)))?;
                Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
            };
            let page = query_page(cursor)?;
            if page.is_empty() && cursor > 0 {
                query_page(0)?
            } else {
                page
            }
        };
        let mut completed = 0;
        for (row, job) in jobs {
            checkpoint()?;
            match self.cleanup_torrent_copy_stages(job, checkpoint) {
                Ok(_) => completed += 1,
                Err(error) => tracing::warn!(job,%error,"torrent staging cleanup remains pending"),
            }
            self.db.lock().unwrap().execute("INSERT INTO meta(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",params![cursor_key,row.to_string()])?;
        }
        Ok(completed)
    }

    /// Inventory maintenance finds durable publications even after the child
    /// has left queue/history or the native torrent service has been disabled.
    pub fn reconcile_torrent_copy_compaction(
        &self,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<usize> {
        let key = "torrent_copy_compaction_cursor";
        let rows = {
            let db = self.db.lock().unwrap();
            let cursor: Option<String> = db
                .query_row("SELECT value FROM meta WHERE key=?1", [key], |row| {
                    row.get(0)
                })
                .optional()?;
            let cursor = cursor
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0);
            let page = |after: i64| -> Result<Vec<(i64, String, String)>> {
                let mut query=db.prepare("SELECT rowid,id,artifact FROM operations WHERE rowid>?1 AND state='succeeded' AND json_extract(data,'$.kind')='torrent-copy' AND json_extract(json_extract(data,'$.request'),'$.compacted_delivery') IS NULL ORDER BY rowid LIMIT 64")?;
                let rows =
                    query.query_map([after], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
                Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
            };
            let rows = page(cursor)?;
            if rows.is_empty() && cursor > 0 {
                page(0)?
            } else {
                rows
            }
        };
        let mut compacted = 0;
        for (row, id, artifact) in rows {
            checkpoint()?;
            let op = self.operation(&id)?;
            let plan: TorrentCopy = serde_json::from_str(&op.request)?;
            let deliveries = {
                let db = self.db.lock().unwrap();
                // Both direct publication and PP relocation preserve the
                // indexed original artifact/generation. A PP scratch has an
                // exact succeeded transformation linking its source generation.
                let mut query=db.prepare("SELECT id FROM deliveries WHERE artifact=?1 AND generation=?2 UNION SELECT d.id FROM operations t JOIN deliveries d ON d.artifact=json_extract(json_extract(t.data,'$.request'),'$.scratch.id') AND d.generation=json_extract(json_extract(t.data,'$.request'),'$.scratch.generation') WHERE t.artifact=?1 AND t.state='succeeded' AND json_extract(t.data,'$.kind') IN ('extract','par_repair') AND json_extract(json_extract(t.data,'$.request'),'$.source.generation')=?2 LIMIT 2")?;
                let rows = query.query_map(params![artifact, plan.generation], |row| {
                    row.get::<_, String>(0)
                })?;
                rows.collect::<std::result::Result<Vec<_>, _>>()?
            };
            if deliveries.len() == 1 {
                match self.compact_torrent_copy_for_delivery(&deliveries[0]) {
                    Ok(true) => compacted += 1,
                    Ok(false) => {}
                    Err(error) => {
                        // A retained/review row cannot starve subsequent jobs.
                        // Reload the small receipt under serialization; never
                        // overwrite bulk progress or a concurrently compacted proof.
                        let _guard = self.mutation_guard()?;
                        let mut current = self.operation(&id)?;
                        let receipt: TorrentCopy = serde_json::from_str(&current.request)?;
                        if current.state == "succeeded"
                            && receipt.compacted_delivery.is_none()
                            && receipt.compacted_delete.is_none()
                        {
                            current.error = Some(format!(
                                "Torrent copy metadata compaction requires review: {error}"
                            ));
                            save_operation(&self.db.lock().unwrap(), &current)?;
                        }
                    }
                }
            }
            self.db.lock().unwrap().execute("INSERT INTO meta(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",params![key,row.to_string()])?;
        }
        Ok(compacted)
    }

    /// Compact only after a real immutable Delivery has durably sealed this
    /// exact copy (possibly through a recorded PP scratch/relocation). No
    /// caller-supplied acceptance flag can authorize it.
    pub fn compact_torrent_copy_for_delivery(&self, delivery_id: &str) -> Result<bool> {
        let _guard = self.mutation_guard()?;
        let delivery = self.delivery(delivery_id)?;
        let key = format!("torrent-copy-job-{}", delivery.job_id);
        let mut op = match self.operation(&key) {
            Ok(op) => op,
            Err(Error::NotFound) => return Ok(false),
            Err(error) => return Err(error),
        };
        if op.kind != "torrent-copy" || op.state != "succeeded" {
            return Ok(false);
        }
        let mut plan = load_plan(self, &op)?;
        if plan.job != delivery.job_id {
            return Err(Error::Conflict(
                "torrent delivery job differs from copy receipt".into(),
            ));
        }
        if let Some(id) = &plan.compacted_delivery {
            return Ok(id == delivery_id);
        }
        let direct = delivery.source_artifact_id == op.artifact
            && delivery.source_generation == plan.generation;
        let relocated = delivery.artifact_id == op.artifact
            && delivery.publication_generation == plan.generation
            && self.delivery_source_ref(
                &delivery.artifact_id,
                &delivery.publication_generation,
                &delivery.publication_path,
            )? == (
                delivery.source_artifact_id.clone(),
                delivery.source_generation.clone(),
            );
        let scratch = if direct || relocated {
            false
        } else {
            let raw:Option<String>=self.db.lock().unwrap().query_row("SELECT data FROM operations WHERE artifact=?1 AND state='succeeded' AND json_extract(data,'$.kind') IN ('extract','par_repair') AND json_extract(json_extract(data,'$.request'),'$.scratch.id')=?2 LIMIT 1",params![op.artifact,delivery.artifact_id],|row|row.get(0)).optional()?;
            if let Some(raw) = raw {
                let transform: Operation = serde_json::from_str(&raw)?;
                let workspace: Workspace = serde_json::from_str(&transform.request)?;
                workspace.source.id == op.artifact
                    && workspace.source.generation == plan.generation
                    && workspace.source.job == Some(plan.job)
                    && workspace.scratch.id == delivery.artifact_id
                    && workspace.scratch.generation == delivery.publication_generation
                    && workspace
                        .published_files
                        .as_ref()
                        .is_some_and(|files| !files.is_empty())
                    && self.delivery_source_ref(
                        &delivery.artifact_id,
                        &delivery.publication_generation,
                        &delivery.publication_path,
                    )? == (
                        delivery.source_artifact_id.clone(),
                        delivery.source_generation.clone(),
                    )
            } else {
                false
            }
        };
        if !direct && !relocated && !scratch {
            return Ok(false);
        }
        if plan
            .files
            .iter()
            .any(|file| file.target.is_none() || file.digest.is_none())
        {
            return Err(Error::Conflict(
                "torrent copy is missing completed per-file proof".into(),
            ));
        }
        let artifact = self.get(&op.artifact)?;
        let root = self.verify_root(&artifact)?;
        for file in plan.files.clone() {
            if let Some(stage) = file.stage {
                let observed = fs::open_at(&root, Path::new(&stage.name), false);
                if !matches!(&observed,Err(Error::Io(error)) if error.kind()==std::io::ErrorKind::NotFound)
                {
                    retain_unknown(
                        &mut plan,
                        stage.name,
                        observed
                            .ok()
                            .and_then(|file| fs::identity_for_file(&file).ok()),
                    )?;
                }
            }
        }
        plan.stage_cleanup_complete = true;
        plan.files.clear();
        plan.directories.clear();
        plan.compacted_delivery = Some(delivery_id.into());
        op.error = unknown_stage_warning(&plan);
        plan.compacted_source = Some(SourceGeneration {
            artifact: delivery.source_artifact_id.clone(),
            generation: delivery.source_generation.clone(),
        });
        if serde_json::to_vec(&plan)?.len() > 64 * 1024 {
            return Err(Error::Conflict(
                "torrent terminal proof exceeds bounded size; retained for review".into(),
            ));
        }
        encode_receipt(&mut op, &plan)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        tx.execute(
            "DELETE FROM torrent_copy_progress WHERE operation_id=?1 AND generation=?2",
            params![op.id, plan.generation],
        )?;
        tx.execute(
            "DELETE FROM torrent_copy_manifests WHERE operation_id=?1 AND generation=?2",
            params![op.id, plan.generation],
        )?;
        save_operation(&tx, &op)?;
        tx.commit()?;
        Ok(true)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::cell::Cell;

    const HASH: &str = "0123456789012345678901234567890123456789";
    struct Fixture {
        temp: tempfile::TempDir,
        inventory: Arc<Inventory>,
        donor: PathBuf,
        root: PathBuf,
        target: PathBuf,
        selected: Vec<(PathBuf, u64)>,
    }
    impl Fixture {
        fn new(owned: bool, files: usize) -> Self {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("processing");
            std::fs::create_dir(&root).unwrap();
            let seeds = temp.path().join("seeds");
            std::fs::create_dir(&seeds).unwrap();
            let donor = seeds.join("donor");
            let inventory = Arc::new(Inventory::open(&temp.path().join("state")).unwrap());
            if owned {
                inventory
                    .allocate_for_delivery(1, &seeds, &donor, true, false)
                    .unwrap();
            } else {
                std::fs::create_dir(&donor).unwrap();
            }
            let mut selected = Vec::new();
            for index in 0..files {
                let name = PathBuf::from(format!("media-{index}.mkv"));
                let data = if files > 8 {
                    vec![index as u8; 32]
                } else if index == 0 {
                    vec![42; 131_073]
                } else {
                    vec![71]
                };
                std::fs::write(donor.join(&name), &data).unwrap();
                selected.push((name, data.len() as u64));
            }
            if owned {
                inventory.finish(1, &donor, &seeds, "completed").unwrap();
            }
            let target = root.join("child");
            Self {
                temp,
                inventory,
                donor,
                root,
                target,
                selected,
            }
        }
        fn run(&self) -> Result<Artifact> {
            self.inventory.copy_torrent_acquisition(
                2,
                1,
                HASH,
                &self.donor,
                &self.selected,
                &self.root,
                &self.target,
                false,
                &|| Ok(()),
            )
        }
        fn restart(self) -> Self {
            let Self {
                temp,
                inventory,
                donor,
                root,
                target,
                selected,
            } = self;
            drop(inventory);
            let inventory = Arc::new(Inventory::open(&temp.path().join("state")).unwrap());
            Self {
                temp,
                inventory,
                donor,
                root,
                target,
                selected,
            }
        }
        fn plan(&self) -> TorrentCopy {
            load_plan(
                &self.inventory,
                &self.inventory.operation("torrent-copy-job-2").unwrap(),
            )
            .unwrap()
        }
        fn assert_bytes(&self) {
            for (path, _) in &self.selected {
                assert_eq!(
                    std::fs::read(self.target.join(path)).unwrap(),
                    std::fs::read(self.donor.join(path)).unwrap()
                );
                let a =
                    fs::identity_for_file(&crate::fileops::open(&self.target.join(path)).unwrap())
                        .unwrap();
                let b =
                    fs::identity_for_file(&crate::fileops::open(&self.donor.join(path)).unwrap())
                        .unwrap();
                assert!(
                    !a.same_object(&b),
                    "processing copy must not hardlink seed bytes"
                );
            }
        }
    }
    #[test]
    fn torn_owned_partial_restarts_but_completed_digest_mismatch_is_reviewed() {
        for completed in [false, true] {
            let mut f = Fixture::new(true, 1);
            FAULT.with(|fault| {
                *fault.borrow_mut() = Some((
                    if completed {
                        "stage_complete"
                    } else {
                        "copy_chunk"
                    },
                    0,
                ))
            });
            assert!(f.run().is_err());
            let stage = f.root.join(&f.plan().files[0].stage.as_ref().unwrap().name);
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&stage)
                .unwrap();
            file.seek(std::io::SeekFrom::Start(17)).unwrap();
            file.write_all(b"torn suffix").unwrap();
            file.sync_all().unwrap();
            f = f.restart();
            if completed {
                assert!(f.run().is_err());
                assert!(stage.exists());
            } else {
                f.run().unwrap();
                f.assert_bytes();
            }
            assert_eq!(
                std::fs::read(f.donor.join(&f.selected[0].0)).unwrap(),
                vec![42; 131073]
            );
        }
    }
    #[test]
    fn checkpoint_rows_are_small_and_immutable_selection_is_not_rewritten() {
        let f = Fixture::new(true, 100);
        FAULT.with(|fault| *fault.borrow_mut() = Some(("stage_complete", 0)));
        assert!(f.run().is_err());
        let manifest: String = f
            .inventory
            .db
            .lock()
            .unwrap()
            .query_row("SELECT data FROM torrent_copy_manifests", [], |row| {
                row.get(0)
            })
            .unwrap();
        f.run().unwrap();
        let db = f.inventory.db.lock().unwrap();
        assert_eq!(
            manifest,
            db.query_row::<String, _, _>("SELECT data FROM torrent_copy_manifests", [], |row| row
                .get(0))
                .unwrap()
        );
        let (count, max, total): (i64, i64, i64) = db
            .query_row(
                "SELECT count(*),max(length(data)),sum(length(data)) FROM torrent_copy_progress",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(count, 100);
        assert!(max < 2048);
        assert!(total < 200000);
        drop(db);
        assert!(
            f.inventory
                .operation("torrent-copy-job-2")
                .unwrap()
                .request
                .len()
                < 4096
        );
    }
    #[test]
    fn oversized_progress_metadata_refuses_before_json_materialization() {
        for corruption in ["row", "counter", "manifest"] {
            let f = Fixture::new(true, 1);
            FAULT.with(|fault| *fault.borrow_mut() = Some(("stage_identity", 0)));
            assert!(f.run().is_err());
            let db = f.inventory.db.lock().unwrap();
            match corruption {
                "row" => {
                    db.execute(
                        "UPDATE torrent_copy_progress SET data=CAST(zeroblob(?1) AS TEXT)",
                        [(MAX_PROGRESS_ROW + 1) as i64],
                    )
                    .unwrap();
                }
                "counter" => {
                    db.execute(
                        "UPDATE torrent_copy_manifests SET progress_bytes=?1",
                        [(MAX_PROGRESS_TOTAL + 1) as i64],
                    )
                    .unwrap();
                }
                _ => {
                    db.execute(
                        "UPDATE torrent_copy_manifests SET data=CAST(zeroblob(?1) AS TEXT)",
                        [(MAX_PLAN + 1) as i64],
                    )
                    .unwrap();
                }
            }
            drop(db);
            assert!(matches!(
                load_plan(
                    &f.inventory,
                    &f.inventory.operation("torrent-copy-job-2").unwrap()
                ),
                Err(Error::Conflict(_))
            ));
        }
    }
    #[test]
    fn cleanup_during_copy_records_separate_retry_without_overwriting_progress() {
        let f = Fixture::new(true, 1);
        let checked = Cell::new(false);
        let checkpoint = || {
            if !checked.get() && f.inventory.operation("torrent-copy-job-2").is_ok() {
                checked.set(true);
                let before = f.inventory.operation("torrent-copy-job-2").unwrap().request;
                let retry_started = now();
                assert!(
                    matches!(f.inventory.cleanup_torrent_copy_stages(2,&||Ok(())),Err(Error::Io(ref error)) if error.kind()==std::io::ErrorKind::WouldBlock)
                );
                assert_eq!(
                    before,
                    f.inventory.operation("torrent-copy-job-2").unwrap().request
                );
                let status = f.inventory.operation("torrent-stage-status-job-2").unwrap();
                assert_eq!(status.state, "retry");
                assert_eq!(status.attempts, 1);
                assert!((retry_started + 30..=now() + 35).contains(&status.next_retry));
                let mut status = status;
                let mut details: StageCleanupStatus =
                    serde_json::from_str(&status.request).unwrap();
                let first_blocked = now() - 1801;
                details.blocked_since = Some(first_blocked);
                status.request = serde_json::to_string(&details).unwrap();
                save_operation(&f.inventory.db.lock().unwrap(), &status).unwrap();
                for base in [60, 120, 300] {
                    let retry_started = now();
                    assert!(f
                        .inventory
                        .cleanup_torrent_copy_stages(2, &|| Ok(()))
                        .is_err());
                    let status = f.inventory.operation("torrent-stage-status-job-2").unwrap();
                    assert!((retry_started + base..=now() + base + base / 10 + 1)
                        .contains(&status.next_retry));
                    let details: StageCleanupStatus =
                        serde_json::from_str(&status.request).unwrap();
                    assert_eq!(details.blocked_since, Some(first_blocked));
                    assert!(details.warning.is_some());
                    assert_eq!(
                        before,
                        f.inventory.operation("torrent-copy-job-2").unwrap().request
                    );
                }
            }
            Ok(())
        };
        f.inventory
            .copy_torrent_acquisition(
                2,
                1,
                HASH,
                &f.donor,
                &f.selected,
                &f.root,
                &f.target,
                false,
                &checkpoint,
            )
            .unwrap();
        assert!(checked.get());
        f.assert_bytes();
    }
    #[test]
    fn deleted_child_compacts_bulk_and_refuses_reconstruction_without_delivery() {
        let mut f = Fixture::new(true, 100);
        FAULT.with(|fault| *fault.borrow_mut() = Some(("copy_chunk", 50)));
        assert!(f.run().is_err());
        delete_child(&f, false);
        f.inventory
            .cleanup_torrent_copy_stages(2, &|| Ok(()))
            .unwrap();
        let receipt = f.inventory.operation("torrent-copy-job-2").unwrap().request;
        assert!(receipt.len() < 4096);
        let db = f.inventory.db.lock().unwrap();
        assert_eq!(
            db.query_row::<i64, _, _>("SELECT count(*) FROM torrent_copy_progress", [], |row| row
                .get(0))
                .unwrap(),
            0
        );
        assert_eq!(
            db.query_row::<i64, _, _>("SELECT count(*) FROM torrent_copy_manifests", [], |row| row
                .get(0))
                .unwrap(),
            0
        );
        drop(db);
        f = f.restart();
        assert!(f.run().is_err());
        assert_eq!(
            receipt,
            f.inventory.operation("torrent-copy-job-2").unwrap().request
        );
    }
    #[test]
    fn auxiliary_rows_cascade_with_parent_and_ready_check_uses_only_bounded_receipt() {
        let f = Fixture::new(true, 2);
        let artifact = f.run().unwrap();
        let source = SourceGeneration {
            artifact: artifact.id,
            generation: artifact.generation,
        };
        assert!(f.inventory.torrent_copy_ready(2, &source).unwrap());
        let db = f.inventory.db.lock().unwrap();
        db.execute("UPDATE torrent_copy_manifests SET data='corrupt'", [])
            .unwrap();
        drop(db);
        assert!(f.inventory.torrent_copy_ready(2, &source).unwrap());
        let db = f.inventory.db.lock().unwrap();
        db.execute("DELETE FROM operations WHERE id='torrent-copy-job-2'", [])
            .unwrap();
        assert_eq!(
            db.query_row::<i64, _, _>("SELECT count(*) FROM torrent_copy_progress", [], |row| row
                .get(0))
                .unwrap(),
            0
        );
        assert_eq!(
            db.query_row::<i64, _, _>("SELECT count(*) FROM torrent_copy_manifests", [], |row| row
                .get(0))
                .unwrap(),
            0
        );
    }
    #[test]
    fn verified_torrent_copy_enters_custody_evidence_and_preserves_rename_binding() {
        let f = Fixture::new(true, 1);
        f.inventory
            .copy_torrent_acquisition_verified(
                2,
                1,
                HASH,
                &f.donor,
                &f.selected,
                &f.root,
                &f.target,
                false,
                &|reader| {
                    let mut bytes = [0u8; 1];
                    reader.read_exact_at(&f.selected[0].0, 0, &mut bytes)?;
                    assert_eq!(bytes, [42]);
                    Ok(())
                },
                &|| Ok(()),
            )
            .unwrap();
        let evidence = f.inventory.verified_custody_evidence(2, || Ok(())).unwrap();
        assert!(evidence.verified_outputs.contains("media-0.mkv"));
        f.inventory
            .restore_file(
                2,
                &f.target.join("media-0.mkv"),
                &f.target.join("renamed.mkv"),
            )
            .unwrap();
        let evidence = f.inventory.verified_custody_evidence(2, || Ok(())).unwrap();
        assert!(evidence.verified_outputs.contains("renamed.mkv"));
        std::fs::write(f.target.join("renamed.mkv"), b"changed").unwrap();
        let evidence = f.inventory.verified_custody_evidence(2, || Ok(()));
        assert!(evidence.is_err() || !evidence.unwrap().verified_outputs.contains("renamed.mkv"));
        let unverified = Fixture::new(true, 1);
        unverified.run().unwrap();
        assert!(unverified
            .inventory
            .verified_custody_evidence(2, || Ok(()))
            .unwrap()
            .verified_outputs
            .is_empty());
    }

    #[test]
    fn inventory_maintenance_compacts_delivery_without_live_job_or_service() {
        let f = Fixture::new(true, 2);
        f.run().unwrap();
        let delivery = f
            .inventory
            .create_delivery_with_outcome_and_keep(
                2,
                true,
                Some("retired-child"),
                "SUCCESS",
                None,
                None,
                false,
            )
            .unwrap();
        assert_eq!(
            f.inventory
                .reconcile_torrent_copy_compaction(&|| Ok(()))
                .unwrap(),
            1
        );
        assert_eq!(
            f.plan().compacted_delivery.as_deref(),
            Some(delivery.delivery_id.as_str())
        );
        assert_eq!(
            f.inventory
                .reconcile_torrent_copy_compaction(&|| Ok(()))
                .unwrap(),
            0
        );
    }
    #[test]
    fn compaction_review_does_not_starve_a_later_durable_delivery() {
        let f = Fixture::new(true, 2);
        f.run().unwrap();
        f.inventory
            .create_delivery_with_outcome_and_keep(
                2,
                true,
                Some("review-child"),
                "SUCCESS",
                None,
                None,
                false,
            )
            .unwrap();
        let second = f.root.join("second-child");
        f.inventory
            .copy_torrent_acquisition(
                3,
                1,
                HASH,
                &f.donor,
                &f.selected,
                &f.root,
                &second,
                false,
                &|| Ok(()),
            )
            .unwrap();
        let receipt = f
            .inventory
            .create_delivery_with_outcome_and_keep(
                3,
                true,
                Some("later-child"),
                "SUCCESS",
                None,
                None,
                false,
            )
            .unwrap();
        f.inventory.db.lock().unwrap().execute("UPDATE torrent_copy_manifests SET data='corrupt' WHERE operation_id='torrent-copy-job-2'",[]).unwrap();
        assert_eq!(
            f.inventory
                .reconcile_torrent_copy_compaction(&|| Ok(()))
                .unwrap(),
            1
        );
        assert!(f
            .inventory
            .operation("torrent-copy-job-2")
            .unwrap()
            .error
            .unwrap()
            .contains("compaction requires review"));
        let plan: TorrentCopy =
            serde_json::from_str(&f.inventory.operation("torrent-copy-job-3").unwrap().request)
                .unwrap();
        assert_eq!(
            plan.compacted_delivery.as_deref(),
            Some(receipt.delivery_id.as_str())
        );
    }
    #[test]
    fn unknown_path_retention_commits_before_per_file_pointer_is_cleared() {
        let mut f = Fixture::new(true, 1);
        FAULT.with(|fault| *fault.borrow_mut() = Some(("stage_created", 0)));
        assert!(f.run().is_err());
        let path = f.root.join(&f.plan().files[0].stage.as_ref().unwrap().name);
        FAULT.with(|fault| *fault.borrow_mut() = Some(("unknown_retained", 0)));
        assert!(f.run().is_err());
        f = f.restart();
        assert_eq!(f.plan().uncommitted_stages.len(), 1);
        f.run().unwrap();
        f.assert_bytes();
        assert!(path.exists());
        let events = serde_json::to_string(
            &f.inventory
                .events(&f.inventory.for_job(2).unwrap().unwrap().id, 0)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(events.matches("torrent_stage_review").count(), 1);
        assert!(events.contains(path.to_str().unwrap()));
    }
    #[test]
    fn unknown_staging_paths_and_reason_are_in_existing_artifact_events_and_receipt() {
        let f = Fixture::new(true, 1);
        FAULT.with(|fault| *fault.borrow_mut() = Some(("stage_created", 0)));
        assert!(f.run().is_err());
        let path = f.root.join(&f.plan().files[0].stage.as_ref().unwrap().name);
        f.run().unwrap();
        let op = f.inventory.operation("torrent-copy-job-2").unwrap();
        assert!(op.error.unwrap().contains(path.to_str().unwrap()));
        let events = f.inventory.events(&op.artifact, 0).unwrap();
        let events = serde_json::to_string(&events).unwrap();
        assert!(events.contains(path.to_str().unwrap()));
        assert!(events.contains("no durable inode receipt"));
    }
    #[test]
    fn owned_donor_copy_is_independent_and_preserves_seed_policy() {
        let f = Fixture::new(true, 2);
        let before = f.inventory.for_job(1).unwrap().unwrap();
        let copied = f.run().unwrap();
        f.assert_bytes();
        assert!(copied.owned && copied.delivery_lifecycle && copied.automatic_cleanup_eligible);
        let after = f.inventory.for_job(1).unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(before).unwrap(),
            serde_json::to_value(after).unwrap()
        );
        std::fs::write(
            f.target.join(&f.selected[0].0),
            b"child modified by processing",
        )
        .unwrap();
        assert_eq!(
            std::fs::read(f.donor.join(&f.selected[0].0)).unwrap(),
            vec![42; 131_073]
        );
    }
    #[test]
    fn legacy_donor_is_readable_without_adoption_or_cleanup_authority() {
        let f = Fixture::new(false, 1);
        f.run().unwrap();
        f.assert_bytes();
        assert!(f.inventory.for_job(1).unwrap().is_none());
        assert!(f.inventory.for_path(&f.donor).unwrap().is_none());
    }
    #[test]
    fn journaled_creation_copy_and_publication_boundaries_resume_after_restart() {
        for stage in [
            "stage_intent",
            "stage_created",
            "stage_identity",
            "copy_chunk",
            "stage_complete",
            "published",
            "target_identity",
        ] {
            let mut f = Fixture::new(true, 1);
            FAULT.with(|fault| *fault.borrow_mut() = Some((stage, 0)));
            let error = f.run().unwrap_err();
            assert!(
                error.to_string().contains(stage),
                "fault not reached: {stage}: {error}"
            );
            f = f.restart();
            f.run().unwrap();
            f.assert_bytes();
            assert_eq!(
                f.inventory.operation("torrent-copy-job-2").unwrap().state,
                "succeeded"
            );
            if stage == "stage_created" {
                let retained = f.plan().uncommitted_stages;
                assert_eq!(retained.len(), 1);
                assert!(f.root.join(&retained[0].name).exists());
            }
        }
    }
    #[test]
    fn unreceipted_stage_is_retained_unadopted_and_does_not_strand_usable_copy() {
        let mut f = Fixture::new(true, 1);
        FAULT.with(|fault| *fault.borrow_mut() = Some(("stage_created", 0)));
        assert!(f.run().is_err());
        let unknown = f.root.join(&f.plan().files[0].stage.as_ref().unwrap().name);
        std::fs::write(&unknown, b"uncommitted external bytes").unwrap();
        f = f.restart();
        f.run().unwrap();
        f.assert_bytes();
        assert_eq!(
            std::fs::read(&unknown).unwrap(),
            b"uncommitted external bytes"
        );
        let plan = f.plan();
        assert_eq!(plan.uncommitted_stages.len(), 1);
        assert_ne!(
            plan.files[0].stage.as_ref().unwrap().name,
            plan.uncommitted_stages[0].name
        );
    }
    #[test]
    fn unknown_or_replaced_final_and_stage_objects_remain_for_review() {
        for mutation in [
            "empty-final",
            "nonempty-final",
            "replaced-stage",
            "symlink-stage",
        ] {
            let mut f = Fixture::new(true, 1);
            FAULT.with(|fault| *fault.borrow_mut() = Some(("stage_identity", 0)));
            assert!(f.run().is_err());
            let stage = f.root.join(&f.plan().files[0].stage.as_ref().unwrap().name);
            let final_path = f.target.join(&f.selected[0].0);
            let protected = if mutation.ends_with("final") {
                std::fs::write(
                    &final_path,
                    if mutation == "empty-final" {
                        &b""[..]
                    } else {
                        &b"operator final bytes"[..]
                    },
                )
                .unwrap();
                final_path.clone()
            } else {
                let saved = f.root.join("retained-original-stage");
                std::fs::rename(&stage, &saved).unwrap();
                if mutation == "symlink-stage" {
                    std::os::unix::fs::symlink(f.donor.join(&f.selected[0].0), &stage).unwrap();
                    saved
                } else {
                    std::fs::write(&stage, b"operator staging bytes").unwrap();
                    stage.clone()
                }
            };
            let bytes = std::fs::read(&protected).unwrap();
            f = f.restart();
            assert!(f.run().is_err(), "unknown object accepted: {mutation}");
            assert_eq!(std::fs::read(&protected).unwrap(), bytes);
            assert_eq!(
                f.inventory.operation("torrent-copy-job-2").unwrap().state,
                "review"
            );
        }
    }
    #[test]
    fn changed_donor_and_changed_completed_target_are_not_overwritten() {
        for donor_changed in [true, false] {
            let mut f = Fixture::new(true, 1);
            FAULT.with(|fault| *fault.borrow_mut() = Some(("target_identity", 0)));
            assert!(f.run().is_err());
            let changed = if donor_changed {
                f.donor.join(&f.selected[0].0)
            } else {
                f.target.join(&f.selected[0].0)
            };
            std::fs::write(&changed, b"changed retained bytes").unwrap();
            f = f.restart();
            assert!(f.run().is_err());
            assert_eq!(std::fs::read(changed).unwrap(), b"changed retained bytes");
        }
    }
    #[test]
    fn resume_reserves_only_verified_missing_bytes_under_tight_headroom() {
        let mut f = Fixture::new(true, 2);
        FAULT.with(|fault| *fault.borrow_mut() = Some(("target_identity", 0)));
        assert!(f.run().is_err());
        f = f.restart();
        HEADROOM.with(|limit| limit.set(Some(1)));
        let resumed = f.run();
        HEADROOM.with(|limit| limit.set(None));
        resumed.unwrap();
        f.assert_bytes();
        // Both complete files require no allocation forecast at all.
        HEADROOM.with(|limit| limit.set(Some(0)));
        let replay = f.run();
        HEADROOM.with(|limit| limit.set(None));
        replay.unwrap();
    }
    #[test]
    fn partial_stage_prefix_is_verified_and_only_remaining_bytes_are_reserved() {
        let mut f = Fixture::new(true, 1);
        FAULT.with(|fault| *fault.borrow_mut() = Some(("copy_chunk", 0)));
        assert!(f.run().is_err());
        let stage = f.root.join(&f.plan().files[0].stage.as_ref().unwrap().name);
        let written = std::fs::metadata(&stage).unwrap().len();
        assert_eq!(written, 65536);
        f = f.restart();
        HEADROOM.with(|limit| limit.set(Some(131_073 - written)));
        let resumed = f.run();
        HEADROOM.with(|limit| limit.set(None));
        resumed.unwrap();
        f.assert_bytes();
    }
    #[test]
    fn owned_donor_read_use_blocks_delete_without_mutating_source_state() {
        let f = Fixture::new(true, 1);
        let source = f.inventory.for_job(1).unwrap().unwrap();
        let observed = Cell::new(false);
        let checkpoint = || {
            let child = f.inventory.for_job(2).unwrap();
            if !observed.get()
                && child.as_ref().is_some_and(|child| {
                    f.inventory.source_in_use(&SourceGeneration {
                        artifact: child.id.clone(),
                        generation: child.generation.clone(),
                    })
                })
            {
                observed.set(true);
                assert!(f.inventory.source_in_use(&SourceGeneration {
                    artifact: source.id.clone(),
                    generation: source.generation.clone()
                }));
                assert!(f
                    .inventory
                    .request_delete(&source.id, source.revision, "during-copy", 0)
                    .is_err());
                let current = f.inventory.get(&source.id).unwrap();
                assert_eq!(current.state, "completed");
                assert_eq!(current.revision, source.revision);
            }
            Ok(())
        };
        f.inventory
            .copy_torrent_acquisition(
                2,
                1,
                HASH,
                &f.donor,
                &f.selected,
                &f.root,
                &f.target,
                false,
                &checkpoint,
            )
            .unwrap();
        assert!(observed.get());
        assert!(!f.inventory.source_in_use(&SourceGeneration {
            artifact: source.id.clone(),
            generation: source.generation.clone()
        }));
        let deletion = f
            .inventory
            .request_delete(&source.id, source.revision, "after-copy", 60)
            .unwrap();
        f.inventory.cancel_delete(&deletion.id).unwrap();
        assert!(f.donor.exists());
    }
    #[test]
    fn changed_control_checkpoint_stops_writes_and_preserves_resume_proof() {
        let f = Fixture::new(true, 1);
        let interrupted = Cell::new(false);
        let checkpoint = || {
            if f.inventory
                .operation("torrent-copy-job-2")
                .ok()
                .is_some_and(|op| {
                    load_plan(&f.inventory, &op).unwrap().files[0]
                        .stage
                        .as_ref()
                        .is_some_and(|stage| stage.identity.is_some())
                })
            {
                interrupted.set(true);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "child paused",
                ));
            }
            Ok(())
        };
        assert!(f
            .inventory
            .copy_torrent_acquisition(
                2,
                1,
                HASH,
                &f.donor,
                &f.selected,
                &f.root,
                &f.target,
                false,
                &checkpoint
            )
            .is_err());
        assert!(interrupted.get());
        assert!(!f.target.join(&f.selected[0].0).exists());
        f.run().unwrap();
        f.assert_bytes();
    }
    #[test]
    fn immutable_delivery_compacts_large_plan_and_replay_never_regrows_or_reads_retired_donor() {
        let mut f = Fixture::new(true, 100);
        let copied = f.run().unwrap();
        let before:i64=f.inventory.db.lock().unwrap().query_row("SELECT length(data) FROM torrent_copy_manifests WHERE operation_id='torrent-copy-job-2'",[],|row|row.get(0)).unwrap();
        assert!(before > 16 * 1024);
        assert!(f
            .inventory
            .compact_torrent_copy_for_delivery("missing")
            .is_err());
        let receipt = f
            .inventory
            .create_delivery_with_outcome_and_keep(
                2,
                true,
                Some("native-request"),
                "SUCCESS",
                None,
                None,
                false,
            )
            .unwrap();
        assert!(f
            .inventory
            .compact_torrent_copy_for_delivery(&receipt.delivery_id)
            .unwrap());
        let compact = f.inventory.operation("torrent-copy-job-2").unwrap().request;
        assert!(
            compact.len() < 4096,
            "terminal proof was not compact: {}",
            compact.len()
        );
        assert!(f.plan().files.is_empty());
        // These are exclusively fixture-owned paths. A terminal retry must not
        // recreate either already retired tree or read their absent bytes.
        std::fs::remove_dir_all(&f.donor).unwrap();
        std::fs::remove_dir_all(&f.target).unwrap();
        f = f.restart();
        assert_eq!(f.run().unwrap().id, copied.id);
        assert!(!f.target.exists() && !f.donor.exists());
        assert_eq!(
            f.inventory.operation("torrent-copy-job-2").unwrap().request,
            compact
        );
        f.selected[0].1 += 1;
        assert!(f.run().is_err());
        assert_eq!(
            f.inventory.operation("torrent-copy-job-2").unwrap().request,
            compact
        );
    }
    fn delete_child(f: &Fixture, automatic: bool) {
        let mut artifact = f
            .inventory
            .finish(2, &f.target, &f.root, "retained")
            .unwrap();
        if automatic {
            let mut settings = f.inventory.settings().unwrap();
            settings.enabled = true;
            f.inventory.set_settings(&settings).unwrap();
            artifact.state = "parked_failed".into();
            artifact.deadline = Some(now() - 1);
            artifact.retention_seconds = 1;
            artifact.eligible_seconds = 1;
            save_artifact(&f.inventory.db.lock().unwrap(), &artifact).unwrap();
        }
        let operation = f
            .inventory
            .request_delete_authorized(
                &artifact.id,
                artifact.revision,
                "child-delete",
                0,
                automatic,
            )
            .unwrap();
        assert_eq!(
            f.inventory.execute_delete(&operation.id).unwrap().state,
            "succeeded"
        );
        assert!(!f.target.exists());
    }
    #[test]
    fn mutable_keep_change_between_failure_and_retry_does_not_change_copy_identity() {
        let mut f = Fixture::new(true, 1);
        FAULT.with(|fault| *fault.borrow_mut() = Some(("stage_identity", 0)));
        assert!(f.run().is_err());
        assert!(f.inventory.set_keep_on_failure(2, true).unwrap());
        let before = f.plan().fingerprint;
        f = f.restart();
        let copied = f
            .inventory
            .copy_torrent_acquisition(
                2,
                1,
                HASH,
                &f.donor,
                &f.selected,
                &f.root,
                &f.target,
                true,
                &|| Ok(()),
            )
            .unwrap();
        assert!(copied.keep_on_failure);
        assert_eq!(f.plan().fingerprint, before);
        f.assert_bytes();
    }
    #[test]
    fn concurrent_independent_children_treat_owned_donor_contention_as_transient() {
        let f = Fixture::new(true, 1);
        let reached = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let inv = f.inventory.clone();
        let donor = f.donor.clone();
        let root = f.root.clone();
        let target = f.target.clone();
        let selected = f.selected.clone();
        let ready = reached.clone();
        let resume = release.clone();
        let first = std::thread::spawn(move || {
            let stopped = Cell::new(false);
            inv.copy_torrent_acquisition(
                2,
                1,
                HASH,
                &donor,
                &selected,
                &root,
                &target,
                false,
                &|| {
                    if !stopped.get()
                        && inv.for_job(2).unwrap().is_some_and(|child| {
                            inv.source_in_use(&SourceGeneration {
                                artifact: child.id,
                                generation: child.generation,
                            })
                        })
                    {
                        stopped.set(true);
                        ready.wait();
                        resume.wait();
                    }
                    Ok(())
                },
            )
        });
        reached.wait();
        let second_target = f.root.join("second-child");
        let busy = f
            .inventory
            .copy_torrent_acquisition(
                3,
                1,
                HASH,
                &f.donor,
                &f.selected,
                &f.root,
                &second_target,
                false,
                &|| Ok(()),
            )
            .unwrap_err();
        assert!(
            matches!(busy,Error::Io(ref error) if error.kind()==std::io::ErrorKind::WouldBlock)
        );
        assert!(!second_target.exists());
        release.wait();
        first.join().unwrap().unwrap();
        f.inventory
            .copy_torrent_acquisition(
                3,
                1,
                HASH,
                &f.donor,
                &f.selected,
                &f.root,
                &second_target,
                false,
                &|| Ok(()),
            )
            .unwrap();
        let a = crate::fileops::identity(
            &crate::fileops::open(&f.target.join(&f.selected[0].0)).unwrap(),
        )
        .unwrap();
        let b = crate::fileops::identity(
            &crate::fileops::open(&second_target.join(&f.selected[0].0)).unwrap(),
        )
        .unwrap();
        assert!(!a.same_object(&b));
        f.assert_bytes();
    }
    #[test]
    fn restart_discovers_owned_stages_after_committed_child_delete() {
        let mut f = Fixture::new(true, 1);
        FAULT.with(|fault| *fault.borrow_mut() = Some(("copy_chunk", 0)));
        assert!(f.run().is_err());
        let stage = f.root.join(&f.plan().files[0].stage.as_ref().unwrap().name);
        assert!(stage.exists());
        delete_child(&f, false);
        f = f.restart();
        assert_eq!(
            f.inventory
                .reconcile_torrent_stage_cleanups(&|| Ok(()))
                .unwrap(),
            1
        );
        assert!(!stage.exists());
        assert!(f.plan().stage_cleanup_complete);
        assert_eq!(
            f.inventory
                .reconcile_torrent_stage_cleanups(&|| Ok(()))
                .unwrap(),
            0
        );
        assert_eq!(
            f.inventory
                .cleanup_torrent_copy_stages(2, &|| Ok(()))
                .unwrap()
                .state,
            "succeeded"
        );
    }
    #[test]
    fn stage_unlink_crash_replay_records_absence_without_adopting_replacement() {
        for replaced in [false, true] {
            let mut f = Fixture::new(true, 1);
            FAULT.with(|fault| *fault.borrow_mut() = Some(("copy_chunk", 0)));
            assert!(f.run().is_err());
            let stage = f.root.join(&f.plan().files[0].stage.as_ref().unwrap().name);
            delete_child(&f, false);
            FAULT.with(|fault| *fault.borrow_mut() = Some(("stage_unlinked", 0)));
            assert!(f
                .inventory
                .cleanup_torrent_copy_stages(2, &|| Ok(()))
                .is_err());
            assert!(!stage.exists());
            if replaced {
                std::fs::write(&stage, b"new unrelated file").unwrap();
            }
            f = f.restart();
            let result = f.inventory.cleanup_torrent_copy_stages(2, &|| Ok(()));
            if replaced {
                assert!(result.is_err());
                assert_eq!(std::fs::read(&stage).unwrap(), b"new unrelated file");
            } else {
                let operation = result.unwrap();
                let proof: StageCleanup = serde_json::from_str(&operation.request).unwrap();
                assert_eq!(proof.files[0].outcome.as_deref(), Some("absence_confirmed"));
            }
        }
    }
    #[test]
    fn automatic_cleanup_honors_current_keep_and_explicit_delete_receipt_overrides_it() {
        for automatic in [false, true] {
            let f = Fixture::new(true, 1);
            FAULT.with(|fault| *fault.borrow_mut() = Some(("copy_chunk", 0)));
            assert!(f.run().is_err());
            let stage = f.root.join(&f.plan().files[0].stage.as_ref().unwrap().name);
            delete_child(&f, automatic);
            // Model a later retention control reaching this owner after the
            // payload delete committed but before separate stage cleanup.
            let mut source = f.inventory.for_job(2).unwrap().unwrap();
            source.keep = true;
            source.keep_on_failure = true;
            save_artifact(&f.inventory.db.lock().unwrap(), &source).unwrap();
            let result = f.inventory.cleanup_torrent_copy_stages(2, &|| Ok(()));
            if automatic {
                assert!(result.is_err());
                assert!(stage.exists());
            } else {
                assert_eq!(result.unwrap().state, "succeeded");
                assert!(!stage.exists());
            }
        }
    }
    #[test]
    fn keep_files_and_uncommitted_stages_remain_visible_without_cleanup_authority() {
        let f = Fixture::new(true, 1);
        FAULT.with(|fault| *fault.borrow_mut() = Some(("stage_created", 0)));
        assert!(f.run().is_err());
        let unknown = f.root.join(&f.plan().files[0].stage.as_ref().unwrap().name);
        f.inventory
            .finish(2, &f.target, &f.root, "retained")
            .unwrap();
        assert!(f
            .inventory
            .cleanup_torrent_copy_stages(2, &|| Ok(()))
            .is_err());
        assert!(unknown.exists());
        delete_child(&f, false);
        let result = f
            .inventory
            .cleanup_torrent_copy_stages(2, &|| Ok(()))
            .unwrap();
        assert_eq!(result.state, "review");
        assert!(result.error.unwrap().contains("no durable inode receipt"));
        assert!(unknown.exists());
    }
    #[test]
    fn cleanup_scan_advances_past_first_page_review_rows_across_restart() {
        let mut f = Fixture::new(true, 1);
        let template = f.inventory.for_job(1).unwrap().unwrap();
        for index in 0..64u32 {
            let mut artifact = template.clone();
            artifact.id = format!("review-{index}");
            artifact.generation = format!("generation-{index}");
            artifact.job = Some(1000 + index);
            artifact.state = "deleted".into();
            artifact.hold = Some("review retained source".into());
            save_artifact(&f.inventory.db.lock().unwrap(), &artifact).unwrap();
            let plan = TorrentCopy {
                job: 1000 + index,
                fingerprint: "fixture".into(),
                generation: artifact.generation.clone(),
                donor: 1,
                info_hash: HASH.into(),
                source_root: f.donor.clone(),
                source_identity: template.identity.clone().unwrap(),
                target_root_identity: template.root_identity.clone(),
                destination: f.root.join(format!("review-{index}")),
                files: Vec::new(),
                directories: Vec::new(),
                file_count: 0,
                directory_count: 0,
                selection_digest: String::new(),
                integrity_verified: false,
                uncommitted_stages: Vec::new(),
                compacted_delivery: None,
                compacted_source: None,
                compacted_delete: None,
                compacted_stage_cleanup: None,
                stage_cleanup_complete: false,
            };
            let op = Operation {
                id: format!("torrent-copy-job-{}", 1000 + index),
                artifact: artifact.id,
                kind: "torrent-copy".into(),
                state: "retry".into(),
                request: serde_json::to_string(&plan).unwrap(),
                created_at: now(),
                not_before: now(),
                attempts: 0,
                next_retry: 0,
                error: None,
            };
            save_operation(&f.inventory.db.lock().unwrap(), &op).unwrap();
        }
        FAULT.with(|fault| *fault.borrow_mut() = Some(("copy_chunk", 0)));
        assert!(f.run().is_err());
        let stage = f.root.join(&f.plan().files[0].stage.as_ref().unwrap().name);
        delete_child(&f, false);
        assert_eq!(
            f.inventory
                .reconcile_torrent_stage_cleanups(&|| Ok(()))
                .unwrap(),
            0
        );
        assert!(stage.exists());
        let review = f
            .inventory
            .operation("torrent-stage-status-job-1000")
            .unwrap();
        assert!(review.error.unwrap().contains("Staging cleanup pending"));
        assert_eq!(review.state, "review");
        assert_eq!(review.next_retry, 0);
        f = f.restart();
        assert_eq!(
            f.inventory
                .reconcile_torrent_stage_cleanups(&|| Ok(()))
                .unwrap(),
            1
        );
        assert!(!stage.exists());
    }
}
