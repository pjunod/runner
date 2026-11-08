//! Journaled per-file publication (pipeline contract v2 §1, §2.5).
//!
//! A selected publication is no longer staged privately and copied as a
//! tree. The move intent — generation, target directory, every selected
//! file's identity and destination — is written to `relocation_intents` in
//! the same transaction as the relocation `Operation`, **before the first
//! rename**. Each file then moves on its own (`renameat2(RENAME_NOREPLACE)`
//! with the PR #247 fallback chain on the same volume; one streaming,
//! hashing copy across volumes) and is marked moved. `.runner-manifest.json`
//! is written last; it is the commit marker, and a directory without it is
//! not a publication. On restart every intent still open is either
//! **finished** (files verified where the journal says they are, the rest
//! moved, the manifest written) or **unwound** (moved files put back by
//! identity, the target removed) — decided by whether every selected file is
//! still accounted for, unchanged, on one side or the other.

use super::relocation::Relocation;
use super::*;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};

/// The commit marker of a publication directory (contract §1).
pub const RUNNER_MANIFEST_NAME: &str = ".runner-manifest.json";
const RUNNER_MANIFEST_PART: &str = ".runner-manifest.json.part";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentState {
    /// Journaled; the move is in progress (or was, before a crash).
    Open,
    /// Restart reconciliation decided to finish the move.
    Finishing,
    /// Restart reconciliation decided to put the moved files back.
    Unwinding,
    /// The manifest exists and the inventory records the publication.
    Committed,
    /// Every moved file is back in the source; the target is gone.
    Unwound,
}

impl IntentState {
    pub fn as_str(self) -> &'static str {
        match self {
            IntentState::Open => "open",
            IntentState::Finishing => "finishing",
            IntentState::Unwinding => "unwinding",
            IntentState::Committed => "committed",
            IntentState::Unwound => "unwound",
        }
    }
    fn parse(value: &str) -> Result<Self> {
        Ok(match value {
            "open" => IntentState::Open,
            "finishing" => IntentState::Finishing,
            "unwinding" => IntentState::Unwinding,
            "committed" => IntentState::Committed,
            "unwound" => IntentState::Unwound,
            other => {
                return Err(Error::Conflict(format!(
                    "unknown relocation intent state {other}"
                )))
            }
        })
    }
    /// Disposal may not claim either side while this is true.
    pub fn is_open(self) -> bool {
        matches!(
            self,
            IntentState::Open | IntentState::Finishing | IntentState::Unwinding
        )
    }
}

const OPEN_STATES: &str = "('open','finishing','unwinding')";

/// One selected file of a journaled move.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IntentFile {
    /// Path relative to the source (processing) directory.
    pub src: String,
    /// Path relative to the publication directory.
    pub dst: String,
    pub bytes: u64,
    pub inode: u64,
    pub dev: u64,
    /// The source file's full identity when the intent was journaled.
    pub identity: Identity,
    /// The journaled digest, if one was known, else the one computed by the
    /// single read pass (same volume) or the streaming copy (cross volume).
    pub sha256: Option<String>,
    /// The file's identity in the publication once moved.
    #[serde(default)]
    pub dst_identity: Option<Identity>,
    pub moved: bool,
}

/// One row of `relocation_intents`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelocationIntent {
    /// The publication generation (the source artifact's generation).
    pub generation: String,
    pub job: u32,
    pub transfer: Option<String>,
    pub artifact: String,
    /// The relocation `Operation` this intent journals.
    pub operation: String,
    pub source_dir: PathBuf,
    pub target_dir: PathBuf,
    pub cross_volume: bool,
    pub files: Vec<IntentFile>,
    pub state: IntentState,
    /// Why reconciliation finished or unwound, with the per-file evidence.
    pub evidence: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// One file of `.runner-manifest.json`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestFile {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

/// `.runner-manifest.json` (contract §2.5, IMPL_RUNNER §3.6). `digest` is
/// the deployed `runner-manifest-v1` algorithm over `files`, bare hex.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunnerManifest {
    pub generation: String,
    pub job: u32,
    pub transfer: Option<String>,
    pub created_at: i64,
    pub cross_volume: bool,
    pub files: Vec<ManifestFile>,
    pub digest: String,
}

impl RunnerManifest {
    /// Files are put in bytewise path order and the digest computed over them.
    pub fn new(
        generation: String,
        job: u32,
        transfer: Option<String>,
        created_at: i64,
        cross_volume: bool,
        mut files: Vec<ManifestFile>,
    ) -> Self {
        files.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
        let digest = Self::digest_of(&files);
        RunnerManifest {
            generation,
            job,
            transfer,
            created_at,
            cross_volume,
            files,
            digest,
        }
    }
    /// The `runner-manifest-v1` digest of `files` (sorted here).
    pub fn digest_of(files: &[ManifestFile]) -> String {
        let mut ordered = files.iter().collect::<Vec<_>>();
        ordered.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
        super::delivery::manifest_digest_of(
            ordered
                .into_iter()
                .map(|file| (file.path.as_str(), file.bytes, file.sha256.as_str())),
        )
    }
    /// The manifest's `digest` matches its `files`.
    pub fn verifies(&self) -> bool {
        !self.files.is_empty() && self.digest == Self::digest_of(&self.files)
    }
}

/// Where restart reconciliation found one selected file.
enum Found {
    Target(Identity, String),
    Source,
    Neither(String),
}

#[cfg(any(test, feature = "test-support"))]
thread_local! {
    static INJECTED_CRASH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A crash-injection point (IMPL §7.6). A fault injected here stops the
/// publication exactly as a process death would: nothing after it runs and
/// the error path leaves the journal as it was.
pub(super) fn crash_point(stage: &'static str, scope: Option<&Identity>) -> Result<()> {
    let result = super::relocation::boundary(stage, scope);
    #[cfg(any(test, feature = "test-support"))]
    if result.is_err() {
        INJECTED_CRASH.with(|crashed| crashed.set(true));
    }
    result
}

/// True once, right after an injected crash point fired on this thread.
pub(super) fn take_injected_crash() -> bool {
    #[cfg(any(test, feature = "test-support"))]
    {
        INJECTED_CRASH.with(|crashed| crashed.replace(false))
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        false
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parent_of(path: &str) -> Option<&Path> {
    Path::new(path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
}

fn relative_dir(root: &File, path: Option<&Path>) -> Result<File> {
    match path {
        None => Ok(root.try_clone()?),
        Some(path) => fs::open_relative(
            root,
            path.to_str()
                .ok_or_else(|| Error::Conflict("publication path is not UTF-8".into()))?,
        ),
    }
}

fn file_name(path: &str) -> Result<&std::ffi::OsStr> {
    Path::new(path)
        .file_name()
        .ok_or_else(|| Error::Conflict(format!("publication file has no name: {path}")))
}

/// Same object, same size, same modification time: what survives a rename.
fn moved_identity_matches(recorded: &Identity, observed: &Identity) -> bool {
    recorded.same_object(observed)
        && recorded.bytes == observed.bytes
        && recorded.modified == observed.modified
}

fn open_optional(root: &File, path: &str) -> Result<Option<File>> {
    match fs::open_relative(root, path) {
        Ok(file) => Ok(Some(file)),
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn decode_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawIntent> {
    Ok(RawIntent {
        generation: row.get(0)?,
        job: row.get(1)?,
        transfer: row.get(2)?,
        artifact: row.get(3)?,
        operation: row.get(4)?,
        source_dir: row.get(5)?,
        target_dir: row.get(6)?,
        cross_volume: row.get(7)?,
        files: row.get(8)?,
        state: row.get(9)?,
        evidence: row.get(10)?,
        created_at: row.get(11)?,
        updated_at: row.get(12)?,
    })
}

struct RawIntent {
    generation: String,
    job: u32,
    transfer: Option<String>,
    artifact: String,
    operation: String,
    source_dir: String,
    target_dir: String,
    cross_volume: bool,
    files: String,
    state: String,
    evidence: Option<String>,
    created_at: i64,
    updated_at: i64,
}

impl RawIntent {
    fn decode(self) -> Result<RelocationIntent> {
        Ok(RelocationIntent {
            generation: self.generation,
            job: self.job,
            transfer: self.transfer,
            artifact: self.artifact,
            operation: self.operation,
            source_dir: PathBuf::from(self.source_dir),
            target_dir: PathBuf::from(self.target_dir),
            cross_volume: self.cross_volume,
            files: serde_json::from_str(&self.files)?,
            state: IntentState::parse(&self.state)?,
            evidence: self.evidence,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

const COLUMNS: &str = "generation,job,transfer,artifact,operation,source_dir,target_dir,cross_volume,files,state,evidence,created_at,updated_at";

pub(super) fn insert_intent(db: &Connection, intent: &RelocationIntent) -> Result<()> {
    db.execute(
        &format!("INSERT INTO relocation_intents({COLUMNS}) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)"),
        params![
            intent.generation,
            intent.job,
            intent.transfer,
            intent.artifact,
            intent.operation,
            intent.source_dir.to_string_lossy(),
            intent.target_dir.to_string_lossy(),
            intent.cross_volume,
            serde_json::to_string(&intent.files)?,
            intent.state.as_str(),
            intent.evidence,
            intent.created_at,
            intent.updated_at,
        ],
    )?;
    Ok(())
}

fn update_intent(db: &Connection, intent: &RelocationIntent) -> Result<()> {
    db.execute(
        "UPDATE relocation_intents SET files=?2,state=?3,evidence=?4,updated_at=?5 WHERE generation=?1",
        params![
            intent.generation,
            serde_json::to_string(&intent.files)?,
            intent.state.as_str(),
            intent.evidence,
            now(),
        ],
    )?;
    Ok(())
}

impl Inventory {
    /// The move intent for a selected publication, before anything moves.
    pub(super) fn new_relocation_intent(
        &self,
        operation: &str,
        source: &Artifact,
        selected: &[FileEntry],
        target_dir: &Path,
        destination_root: &Identity,
        transfer: Option<&str>,
    ) -> Result<RelocationIntent> {
        let job = source
            .job
            .ok_or_else(|| Error::Conflict("selected publication source has no job".into()))?;
        let cross_volume = source
            .identity
            .as_ref()
            .is_some_and(|identity| identity.device != destination_root.device);
        let mut files = selected
            .iter()
            .filter(|entry| !entry.identity.directory)
            .map(|entry| IntentFile {
                src: entry.path.clone(),
                dst: entry.path.clone(),
                bytes: entry.identity.bytes,
                inode: entry.identity.inode,
                dev: entry.identity.device,
                identity: entry.identity.clone(),
                sha256: entry.digest.clone().filter(|digest| {
                    digest.len() == 64
                        && digest
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                }),
                dst_identity: None,
                moved: false,
            })
            .collect::<Vec<_>>();
        if files.is_empty() {
            return Err(Error::Conflict(
                "selected publication has no payload files".into(),
            ));
        }
        files.sort_by(|a, b| a.src.as_bytes().cmp(b.src.as_bytes()));
        if files.iter().any(|file| file.dst == RUNNER_MANIFEST_NAME) {
            return Err(Error::Conflict(
                "a payload file is named like the publication manifest".into(),
            ));
        }
        let at = now();
        Ok(RelocationIntent {
            generation: source.generation.clone(),
            job,
            transfer: transfer.map(str::to_owned),
            artifact: source.id.clone(),
            operation: operation.into(),
            source_dir: source.path.clone(),
            target_dir: target_dir.into(),
            cross_volume,
            files,
            state: IntentState::Open,
            evidence: None,
            created_at: at,
            updated_at: at,
        })
    }

    pub fn relocation_intent(&self, generation: &str) -> Result<Option<RelocationIntent>> {
        let db = self.db.lock().unwrap();
        let raw = db
            .query_row(
                &format!("SELECT {COLUMNS} FROM relocation_intents WHERE generation=?1"),
                [generation],
                decode_row,
            )
            .optional()?;
        raw.map(RawIntent::decode).transpose()
    }

    fn intents_where(&self, predicate: &str) -> Result<Vec<RelocationIntent>> {
        let db = self.db.lock().unwrap();
        let mut statement = db.prepare(&format!(
            "SELECT {COLUMNS} FROM relocation_intents WHERE {predicate} ORDER BY created_at,generation"
        ))?;
        let rows = statement.query_map([], decode_row)?;
        let raw = rows.collect::<std::result::Result<Vec<_>, _>>()?;
        raw.into_iter().map(RawIntent::decode).collect()
    }

    /// Every intent restart reconciliation has not yet finished or unwound —
    /// the "publication directories without a manifest" runner still owes a
    /// decision on. Post-deploy check: empty after the first restart.
    pub fn open_relocation_intents(&self) -> Result<Vec<RelocationIntent>> {
        self.intents_where(&format!("state IN {OPEN_STATES}"))
    }

    /// Finish or unwind any open intent of this job's publication now — the
    /// post-processing restart path calls this before it decides whether
    /// the publication already committed.
    pub fn reconcile_job_intents(&self, job: u32) -> Result<()> {
        let artifacts = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare(&format!(
                "SELECT DISTINCT artifact FROM relocation_intents WHERE job=?1 AND state IN {OPEN_STATES}"
            ))?;
            let rows = statement.query_map([job], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for artifact in artifacts {
            self.reconcile_open_intents(None, Some(&artifact), None, &|| Ok(()))?;
        }
        Ok(())
    }

    /// The job's committed per-file publication, if its current artifact is
    /// one: what a restarted salvage completes instead of selecting again.
    pub fn committed_intent_publication(
        &self,
        job: u32,
    ) -> Result<Option<super::relocation::RelocationResult>> {
        let Some(artifact) = self.for_job(job)? else {
            return Ok(None);
        };
        let Some(intent) = self.relocation_intent(&artifact.generation)? else {
            return Ok(None);
        };
        if intent.artifact != artifact.id
            || intent.state != IntentState::Committed
            || intent.target_dir != artifact.path
        {
            return Ok(None);
        }
        Ok(Some(super::relocation::RelocationResult {
            operation_id: intent.operation,
            artifact_id: artifact.id,
            generation: artifact.generation,
            published_path: artifact.path,
        }))
    }

    /// Contract §2.5: disposal may claim neither side of an open move.
    pub fn open_intent_for_job(&self, job: u32) -> Result<bool> {
        Ok(self.db.lock().unwrap().query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM relocation_intents WHERE job=?1 AND state IN {OPEN_STATES})"),
            [job],
            |row| row.get(0),
        )?)
    }

    pub(super) fn open_intent_for_artifact(&self, artifact: &str) -> Result<bool> {
        Ok(self.db.lock().unwrap().query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM relocation_intents WHERE artifact=?1 AND state IN {OPEN_STATES})"),
            [artifact],
            |row| row.get(0),
        )?)
    }

    /// Per-file digests of a committed publication, keyed by publication
    /// path, with the identity each file had when it landed. A digest is
    /// only reused for a file still bearing that identity.
    pub(super) fn committed_intent_digests(
        &self,
        artifact: &str,
        generation: &str,
    ) -> Result<std::collections::HashMap<String, (Identity, String)>> {
        let Some(intent) = self.relocation_intent(generation)? else {
            return Ok(Default::default());
        };
        if intent.artifact != artifact || intent.state != IntentState::Committed {
            return Ok(Default::default());
        }
        Ok(intent
            .files
            .into_iter()
            .filter_map(|file| Some((file.dst, (file.dst_identity?, file.sha256?))))
            .collect())
    }

    /// The selected-publication snapshot a cluster worker seals, from the
    /// committed intent.
    pub(super) fn intent_delivery_files(&self, generation: &str) -> Result<Vec<DeliveryFile>> {
        let intent = self
            .relocation_intent(generation)?
            .filter(|intent| intent.state == IntentState::Committed)
            .ok_or_else(|| {
                Error::Conflict("selected publication journal is not committed".into())
            })?;
        let mut files = intent
            .files
            .into_iter()
            .map(|file| {
                Ok(DeliveryFile {
                    sha256: file.sha256.ok_or_else(|| {
                        Error::Conflict(format!(
                            "selected publication lacks a durable digest: {}",
                            file.dst
                        ))
                    })?,
                    path: file.dst,
                    size_bytes: file.bytes,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        files.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
        Ok(files)
    }

    /// A same-volume selected file that left the source by rename.
    pub(super) fn intent_moved_away(&self, generation: &str, source_path: &str) -> Result<bool> {
        Ok(self.relocation_intent(generation)?.is_some_and(|intent| {
            intent.state == IntentState::Committed
                && !intent.cross_volume
                && intent
                    .files
                    .iter()
                    .any(|file| file.src == source_path && file.moved)
        }))
    }

    /// A publication of this source generation that restart reconciliation
    /// unwound because the source was no longer complete. Publishing it
    /// again is refused (`failed/io`, contract §2.5).
    pub(super) fn unwound_intent(
        &self,
        artifact: &str,
        generation: &str,
    ) -> Result<Option<RelocationIntent>> {
        Ok(self
            .relocation_intent(generation)?
            .filter(|intent| intent.artifact == artifact && intent.state == IntentState::Unwound))
    }

    /// Hash one already-open file: the single read pass a same-volume file
    /// without a journaled digest gets.
    fn hash_open_file(
        &self,
        file: &File,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<String> {
        let mut input = file.try_clone()?;
        std::io::Seek::rewind(&mut input)?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0u8; 256 * 1024];
        loop {
            checkpoint()?;
            let count = input.read(&mut buffer)?;
            crate::fileops::payload_read(count);
            fs::count_op(&self.installation, "hash_bytes", count as u64);
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }
        Ok(hex(&hasher.finalize()))
    }

    /// Move one selected file into the publication and return its updated
    /// journal entry. Nothing is journaled here; the caller persists it.
    fn move_intent_file(
        &self,
        intent: &RelocationIntent,
        file: &IntentFile,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
        scope: Option<&Identity>,
    ) -> Result<IntentFile> {
        let source_root = fs::open_dir(&intent.source_dir)?;
        let target_root = fs::open_dir(&intent.target_dir)?;
        let input = fs::open_relative(&source_root, &file.src)?;
        if fs::identity_for_file(&input)? != file.identity {
            return Err(Error::Conflict(format!(
                "source changed before its publication move: {}",
                file.src
            )));
        }
        if let Some(parent) = parent_of(&file.dst) {
            crate::fileops::parents(&intent.target_dir, parent)?;
        }
        let parent = relative_dir(&target_root, parent_of(&file.dst))?;
        let from = intent.source_dir.join(&file.src);
        let to = intent.target_dir.join(&file.dst);
        let mut moved = file.clone();
        if !intent.cross_volume {
            if moved.sha256.is_none() {
                moved.sha256 = Some(self.hash_open_file(&input, checkpoint)?);
                if fs::identity_for_file(&input)? != file.identity {
                    return Err(Error::Conflict(format!(
                        "source changed while hashing: {}",
                        file.src
                    )));
                }
            }
            checkpoint()?;
            fs::rename_exclusive(&from, &to)?;
            fs::count_op(&self.installation, "rename", 1);
            let landed = fs::open_at(&parent, Path::new(file_name(&file.dst)?), false)?;
            let landed_identity = fs::identity_for_file(&landed)?;
            if !moved_identity_matches(&file.identity, &landed_identity) {
                return Err(Error::Conflict(format!(
                    "publication file is not the journaled source after rename: {}",
                    file.dst
                )));
            }
            moved.dst_identity = Some(landed_identity);
        } else {
            let part = format!("{}.part", file_name(&file.dst)?.to_string_lossy());
            if fs::open_at(&parent, Path::new(&part), false).is_ok() {
                // Our own interrupted copy, inside a directory that is not
                // yet a publication: never authority, always redone.
                fs::unlink(&parent, Path::new(&part), false)?;
            }
            let mut output = fs::create_file_exclusive_at(&parent, Path::new(&part))?;
            let mut reader = input.try_clone()?;
            let mut hasher = Sha256::new();
            let mut buffer = vec![0u8; 256 * 1024];
            let mut copied = 0u64;
            loop {
                checkpoint()?;
                let count = reader.read(&mut buffer)?;
                crate::fileops::payload_read(count);
                if count == 0 {
                    break;
                }
                output.write_all(&buffer[..count])?;
                hasher.update(&buffer[..count]);
                fs::count_op(&self.installation, "copy_bytes", count as u64);
                copied += count as u64;
                super::relocation::boundary("copy_chunk", scope)?;
            }
            output.set_permissions(input.metadata()?.permissions())?;
            output.sync_all()?;
            drop(output);
            let sha256 = hex(&hasher.finalize());
            if copied != file.bytes || fs::identity_for_file(&input)? != file.identity {
                return Err(Error::Conflict(format!(
                    "source changed during its publication copy: {}",
                    file.src
                )));
            }
            if moved
                .sha256
                .as_ref()
                .is_some_and(|journaled| journaled != &sha256)
            {
                return Err(Error::Conflict(format!(
                    "source no longer matches its journaled digest: {}",
                    file.src
                )));
            }
            moved.sha256 = Some(sha256);
            checkpoint()?;
            let staged = to.with_file_name(&part);
            fs::rename_exclusive(&staged, &to)?;
            fs::count_op(&self.installation, "rename", 1);
            let landed = fs::open_at(&parent, Path::new(file_name(&file.dst)?), false)?;
            let landed_identity = fs::identity_for_file(&landed)?;
            if landed_identity.bytes != file.bytes {
                return Err(Error::Conflict(format!(
                    "publication copy changed after it landed: {}",
                    file.dst
                )));
            }
            moved.dst_identity = Some(landed_identity);
        }
        moved.moved = true;
        Ok(moved)
    }

    /// Move every file not yet moved, write the manifest, commit. Shared by
    /// the live publication and restart reconciliation's finish.
    pub(super) fn finish_intent(
        &self,
        mut intent: RelocationIntent,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
        scope: Option<&Identity>,
    ) -> Result<RunnerManifest> {
        match fs::open_dir(&intent.target_dir) {
            Ok(_) => {}
            Err(Error::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound
                    && intent.files.iter().all(|file| !file.moved) =>
            {
                // The journal committed and the process stopped before mkdir.
                let parent = intent
                    .target_dir
                    .parent()
                    .ok_or_else(|| Error::Conflict("publication has no parent".into()))?;
                std::fs::create_dir(&intent.target_dir)?;
                fs::sync_directory(&fs::open_dir(parent)?)?;
            }
            Err(error) => return Err(error),
        }
        let mut renamed = 0usize;
        for index in 0..intent.files.len() {
            if intent.files[index].moved {
                continue;
            }
            checkpoint()?;
            super::relocation::boundary("move_file", scope)?;
            {
                // Authority is re-read before every move: a deletion or a
                // newer generation that won meanwhile stops the move here.
                let _guard = self.mutation_guard()?;
                let op = self.operation(&intent.operation)?;
                let movement: Relocation = serde_json::from_str(&op.request)?;
                if self.relocation_source(&op, &movement)?.is_none() {
                    return Err(Error::Conflict(
                        "relocation authority revoked during the publication move".into(),
                    ));
                }
            }
            let moved = self.move_intent_file(&intent, &intent.files[index], checkpoint, scope)?;
            intent.files[index] = moved;
            {
                let _guard = self.mutation_guard()?;
                update_intent(&self.db.lock().unwrap(), &intent)?;
            }
            renamed += 1;
            if renamed == 1 {
                crash_point("after_first_rename", scope)?;
            }
        }
        crash_point("after_last_rename", scope)?;
        let manifest = self.write_manifest(&intent)?;
        crash_point("after_manifest", scope)?;
        self.commit_intent(&intent, &manifest)?;
        Ok(manifest)
    }

    /// Write `.runner-manifest.json` last: `.part`, fsync, exclusive rename.
    /// An existing manifest is accepted only if it is this intent's.
    fn write_manifest(&self, intent: &RelocationIntent) -> Result<RunnerManifest> {
        let files = intent
            .files
            .iter()
            .map(|file| {
                Ok(ManifestFile {
                    path: file.dst.clone(),
                    bytes: file.bytes,
                    sha256: file.sha256.clone().ok_or_else(|| {
                        Error::Conflict(format!("moved file has no digest: {}", file.dst))
                    })?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let target_root = fs::open_dir(&intent.target_dir)?;
        if let Some(existing) = read_manifest(&target_root)? {
            let expected = RunnerManifest::new(
                intent.generation.clone(),
                intent.job,
                intent.transfer.clone(),
                existing.created_at,
                intent.cross_volume,
                files,
            );
            if existing != expected {
                return Err(Error::Conflict(
                    "publication manifest does not match its journaled intent".into(),
                ));
            }
            return Ok(existing);
        }
        let manifest = RunnerManifest::new(
            intent.generation.clone(),
            intent.job,
            intent.transfer.clone(),
            now(),
            intent.cross_volume,
            files,
        );
        if fs::open_at(&target_root, Path::new(RUNNER_MANIFEST_PART), false).is_ok() {
            fs::unlink(&target_root, Path::new(RUNNER_MANIFEST_PART), false)?;
        }
        let mut output =
            fs::create_file_exclusive_at(&target_root, Path::new(RUNNER_MANIFEST_PART))?;
        output.write_all(&serde_json::to_vec_pretty(&manifest)?)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            output.set_permissions(std::fs::Permissions::from_mode(0o644))?;
        }
        output.sync_all()?;
        drop(output);
        fs::rename_exclusive(
            &intent.target_dir.join(RUNNER_MANIFEST_PART),
            &intent.target_dir.join(RUNNER_MANIFEST_NAME),
        )?;
        fs::count_op(&self.installation, "manifest_write", 1);
        fs::sync_directory(&target_root)?;
        Ok(manifest)
    }

    /// Record the publication: the source artifact now names the publication
    /// directory; what is left of the processing directory (unselected
    /// siblings) is a retained `source-<op>` record runner disposes of; the
    /// operation succeeds and the intent commits — one transaction.
    fn commit_intent(&self, intent: &RelocationIntent, manifest: &RunnerManifest) -> Result<()> {
        if !manifest.verifies() {
            return Err(Error::Conflict(
                "publication manifest digest is invalid".into(),
            ));
        }
        let _guard = self.mutation_guard()?;
        let mut op = self.operation(&intent.operation)?;
        let mut movement: Relocation = serde_json::from_str(&op.request)?;
        if op.state == "succeeded" {
            let mut committed = intent.clone();
            committed.state = IntentState::Committed;
            update_intent(&self.db.lock().unwrap(), &committed)?;
            return Ok(());
        }
        let mut current = self.relocation_source(&op, &movement)?.ok_or_else(|| {
            Error::Conflict("relocation no longer owns the source generation".into())
        })?;
        let target_root = fs::open_dir(&intent.target_dir)?;
        let target_identity = fs::identity_for_file(&target_root)?;
        let files = fs::manifest(&target_root, 100_000)?;
        let mut expected = std::collections::BTreeSet::new();
        expected.insert(RUNNER_MANIFEST_NAME.to_string());
        for file in &intent.files {
            expected.insert(file.dst.clone());
            let mut parent = parent_of(&file.dst);
            while let Some(path) = parent {
                expected.insert(path.to_string_lossy().into_owned());
                parent = parent_of(path.to_str().unwrap_or_default());
            }
        }
        let observed = files
            .iter()
            .map(|entry| entry.path.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if observed != expected {
            return Err(Error::Conflict(
                "publication directory differs from its journaled intent".into(),
            ));
        }
        for file in &intent.files {
            let entry = files.iter().find(|entry| entry.path == file.dst).unwrap();
            if !file
                .dst_identity
                .as_ref()
                .is_some_and(|landed| landed == &entry.identity)
            {
                return Err(Error::Conflict(format!(
                    "publication file changed before commit: {}",
                    file.dst
                )));
            }
        }
        let source_root = fs::open_dir(&intent.source_dir)?;
        let remaining = fs::manifest(&source_root, 100_000)?;
        let root = intent
            .target_dir
            .parent()
            .ok_or_else(|| Error::Conflict("publication has no parent".into()))?;
        if current.hold.as_deref() == Some("review: interrupted move") {
            // The operation-generated hold older releases persisted on an
            // interrupted move; explicit holds and Keep stay authoritative.
            current.hold = None;
        }
        current.path = intent.target_dir.clone();
        current.root = root.into();
        current.root_identity = movement.destination_root.clone();
        current.identity = Some(target_identity);
        current.files = files;
        current.state = "active".into();
        current.error = None;
        current.revision += 2;
        current.updated_at = now();
        self.sidecar(&current)?;
        let mut old = movement.source.clone();
        old.id = format!("source-{}", op.id);
        old.job = None;
        old.state = "retained".into();
        old.hold = None;
        old.keep = false;
        old.retention_seconds = 0;
        old.deadline = None;
        old.files = remaining;
        old.updated_at = now();
        self.sidecar(&old)?;
        let mut committed = intent.clone();
        committed.state = IntentState::Committed;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &old)?;
        save_artifact(&tx, &current)?;
        movement.cleanup_done = false;
        op.state = "succeeded".into();
        op.error = None;
        op.request = serde_json::to_string(&movement)?;
        save_operation(&tx, &op)?;
        update_intent(&tx, &committed)?;
        event(
            &tx,
            &current.id,
            "relocated",
            &current.path.to_string_lossy(),
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Restart reconciliation (contract §2.5, IMPL §7.2): every open intent
    /// is finished or unwound — exactly one of them — before any disposal of
    /// either side is eligible. `root`/`artifact` narrow the sweep the way
    /// the relocation reconciler's do; `snapshot` limits startup to the
    /// operations that existed at the boot boundary.
    pub(super) fn reconcile_open_intents(
        &self,
        root: Option<&Path>,
        artifact: Option<&str>,
        snapshot: Option<&StartupReconciliation>,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<()> {
        for intent in self.open_relocation_intents()? {
            if artifact.is_some_and(|artifact| artifact != intent.artifact) {
                continue;
            }
            let source = match self.get(&intent.artifact) {
                Ok(source) => source,
                Err(Error::NotFound) => continue,
                Err(error) => return Err(error),
            };
            if root.is_some_and(|root| root != source.root) {
                continue;
            }
            let op = match self.operation(&intent.operation) {
                Ok(op) => op,
                Err(Error::NotFound) => continue,
                Err(error) => return Err(error),
            };
            if let Some(snapshot) = snapshot {
                if !snapshot.contains_operation(&op)? {
                    continue;
                }
            }
            // An explicit (per-artifact) reconcile is a retry request; the
            // sweep honours the operation's backoff.
            if artifact.is_none() && op.state == "retry" && op.next_retry > now() {
                continue;
            }
            // An admitted deletion keeps its undo window; it is refused
            // while the intent is open, so this is only a pre-M3 admission.
            let deletion_pending: bool = self.db.lock().unwrap().query_row(
                "SELECT EXISTS(SELECT 1 FROM operations WHERE artifact=?1 AND state='queued' AND json_extract(data,'$.kind')='delete')",
                [&intent.artifact],
                |row| row.get(0),
            )?;
            if deletion_pending {
                continue;
            }
            if !self
                .relocation_active
                .lock()
                .unwrap()
                .insert(intent.operation.clone())
            {
                continue;
            }
            let _flight = super::relocation::RelocationLease {
                inventory: self,
                id: intent.operation.clone(),
            };
            let Some(_lease) = self.reserve_reconciliation(std::slice::from_ref(&source))? else {
                continue;
            };
            // Re-read under the single flight: the live attempt may have
            // committed between the listing and the claim.
            let Some(intent) = self
                .relocation_intent(&intent.generation)?
                .filter(|intent| intent.state.is_open())
            else {
                continue;
            };
            if let Err(error) = self.resolve_intent(intent, checkpoint, source.identity.as_ref()) {
                if take_injected_crash() {
                    return Err(error);
                }
                tracing::warn!(operation = %op.id, %error, "publication intent not yet resolved; retrying");
                let _guard = self.mutation_guard()?;
                let mut op = self.operation(&op.id)?;
                if matches!(op.state.as_str(), "running" | "retry" | "review") {
                    if matches!(error, Error::Io(_) | Error::Sql(_)) {
                        op.attempts = op.attempts.saturating_add(1);
                        let delay =
                            [30, 60, 120, 300][op.attempts.saturating_sub(1).min(3) as usize];
                        op.state = "retry".into();
                        op.next_retry = now().saturating_add(delay);
                    } else {
                        op.state = "review".into();
                    }
                    op.error = Some(format!("publication intent: {error}"));
                    save_operation(&self.db.lock().unwrap(), &op)?;
                }
            }
        }
        Ok(())
    }

    /// Decide and perform finish or unwind for one open intent.
    fn resolve_intent(
        &self,
        mut intent: RelocationIntent,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
        scope: Option<&Identity>,
    ) -> Result<()> {
        // A manifest is the commit marker: the crash was after step 4.
        if intent.state != IntentState::Unwinding {
            if let Ok(target_root) = fs::open_dir(&intent.target_dir) {
                if let Some(manifest) = read_manifest(&target_root)? {
                    tracing::info!(generation = %intent.generation, "publication manifest present; committing");
                    return self.commit_intent(&intent, &manifest);
                }
            }
        }
        if intent.state == IntentState::Open {
            let mut evidence = Vec::new();
            let mut complete = true;
            for index in 0..intent.files.len() {
                checkpoint()?;
                match self.locate_intent_file(&intent, &intent.files[index], checkpoint)? {
                    Found::Target(landed, sha256) => {
                        evidence.push(format!("{}: in publication", intent.files[index].dst));
                        let file = &mut intent.files[index];
                        file.moved = true;
                        file.dst_identity = Some(landed);
                        file.sha256 = Some(sha256);
                    }
                    Found::Source => {
                        evidence.push(format!("{}: intact in source", intent.files[index].src));
                        intent.files[index].moved = false;
                    }
                    Found::Neither(why) => {
                        complete = false;
                        evidence.push(format!("{}: {why}", intent.files[index].src));
                    }
                }
            }
            intent.state = if complete {
                IntentState::Finishing
            } else {
                IntentState::Unwinding
            };
            intent.evidence = Some(evidence.join("; "));
            tracing::warn!(
                generation = %intent.generation,
                decision = intent.state.as_str(),
                evidence = intent.evidence.as_deref().unwrap_or_default(),
                "restart reconciliation of an open publication intent"
            );
            let _guard = self.mutation_guard()?;
            update_intent(&self.db.lock().unwrap(), &intent)?;
        }
        match intent.state {
            IntentState::Finishing => self.finish_intent(intent, checkpoint, scope).map(|_| ()),
            IntentState::Unwinding => self.unwind_intent(intent),
            _ => Ok(()),
        }
    }

    /// Where one selected file is, verified by identity (same volume) or by
    /// size and digest (cross volume).
    fn locate_intent_file(
        &self,
        intent: &RelocationIntent,
        file: &IntentFile,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Found> {
        let target = match fs::open_dir(&intent.target_dir) {
            Ok(root) => open_optional(&root, &file.dst)?,
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let source_root = fs::open_dir(&intent.source_dir)?;
        let source = open_optional(&source_root, &file.src)?;
        if let Some(landed) = target {
            let landed_identity = fs::identity_for_file(&landed)?;
            if !intent.cross_volume {
                if moved_identity_matches(&file.identity, &landed_identity) {
                    let sha256 = match &file.sha256 {
                        Some(sha256) => sha256.clone(),
                        None => self.hash_open_file(&landed, checkpoint)?,
                    };
                    return Ok(Found::Target(landed_identity, sha256));
                }
                return Ok(Found::Neither(format!(
                    "publication holds a different file at {}",
                    file.dst
                )));
            }
            if landed_identity.bytes == file.bytes {
                let landed_sha = self.hash_open_file(&landed, checkpoint)?;
                let expected = match &file.sha256 {
                    Some(sha256) => Some(sha256.clone()),
                    None => match &source {
                        Some(input) if fs::identity_for_file(input)? == file.identity => {
                            Some(self.hash_open_file(input, checkpoint)?)
                        }
                        _ => None,
                    },
                };
                if expected.as_deref() == Some(landed_sha.as_str()) {
                    return Ok(Found::Target(landed_identity, landed_sha));
                }
            }
            // A partial or unverifiable copy is ours to redo, if the source
            // still holds the original.
        }
        match source {
            Some(input) if fs::identity_for_file(&input)? == file.identity => {
                if let Some(journaled) = &file.sha256 {
                    if &self.hash_open_file(&input, checkpoint)? != journaled {
                        return Ok(Found::Neither(
                            "source content no longer matches its journaled digest".into(),
                        ));
                    }
                }
                Ok(Found::Source)
            }
            Some(_) => Ok(Found::Neither(
                "source file changed since the move was journaled".into(),
            )),
            None => Ok(Found::Neither(
                "missing from both the source and the publication".into(),
            )),
        }
    }

    /// Put every moved file back by identity, remove the target, and record
    /// the source as runner's again. The job fails `io` on its next
    /// publication attempt (`unwound_intent`).
    fn unwind_intent(&self, mut intent: RelocationIntent) -> Result<()> {
        if intent.state != IntentState::Unwinding {
            intent.state = IntentState::Unwinding;
            let _guard = self.mutation_guard()?;
            update_intent(&self.db.lock().unwrap(), &intent)?;
        }
        let source_root = fs::open_dir(&intent.source_dir)?;
        if let Ok(target_root) = fs::open_dir(&intent.target_dir) {
            for file in &intent.files {
                let parent = match relative_dir(&target_root, parent_of(&file.dst)) {
                    Ok(parent) => parent,
                    Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                        continue
                    }
                    Err(error) => return Err(error),
                };
                let name = file_name(&file.dst)?;
                let part = format!("{}.part", name.to_string_lossy());
                if fs::open_at(&parent, Path::new(&part), false).is_ok() {
                    fs::unlink(&parent, Path::new(&part), false)?;
                }
                let Some(landed) = open_optional(&target_root, &file.dst)? else {
                    continue;
                };
                let landed_identity = fs::identity_for_file(&landed)?;
                if intent.cross_volume {
                    // A copy: the original never left the source.
                    if file
                        .dst_identity
                        .as_ref()
                        .is_some_and(|recorded| recorded == &landed_identity)
                        || open_optional(&source_root, &file.src)?.is_some()
                    {
                        fs::unlink(&parent, Path::new(name), false)?;
                        fs::sync_directory(&parent)?;
                        continue;
                    }
                    return Err(Error::Conflict(format!(
                        "cannot unwind {}: the source copy is gone; retained for review",
                        file.dst
                    )));
                }
                if !moved_identity_matches(&file.identity, &landed_identity) {
                    return Err(Error::Conflict(format!(
                        "cannot unwind {}: it is not the journaled file; retained for review",
                        file.dst
                    )));
                }
                if let Some(source_parent) = parent_of(&file.src) {
                    crate::fileops::parents(&intent.source_dir, source_parent)?;
                }
                fs::rename_exclusive(
                    &intent.target_dir.join(&file.dst),
                    &intent.source_dir.join(&file.src),
                )?;
                let back = fs::open_relative(&source_root, &file.src)?;
                if !moved_identity_matches(&file.identity, &fs::identity_for_file(&back)?) {
                    return Err(Error::Conflict(format!(
                        "unwound file is not the journaled source: {}",
                        file.src
                    )));
                }
            }
            if fs::open_at(&target_root, Path::new(RUNNER_MANIFEST_PART), false).is_ok() {
                fs::unlink(&target_root, Path::new(RUNNER_MANIFEST_PART), false)?;
            }
            let mut left = fs::manifest(&target_root, 100_000)?;
            left.sort_by_key(|entry| std::cmp::Reverse(entry.path.matches('/').count()));
            for entry in &left {
                if !entry.identity.directory {
                    return Err(Error::Conflict(format!(
                        "unwound publication directory holds an unexpected file: {}",
                        entry.path
                    )));
                }
                fs::remove_entry(&target_root, entry)?;
            }
            let parent = fs::open_dir(
                intent
                    .target_dir
                    .parent()
                    .ok_or_else(|| Error::Conflict("publication has no parent".into()))?,
            )?;
            fs::unlink(
                &parent,
                Path::new(
                    intent
                        .target_dir
                        .file_name()
                        .ok_or_else(|| Error::Conflict("publication has no name".into()))?,
                ),
                true,
            )?;
            fs::sync_directory(&parent)?;
        }
        let evidence = intent
            .evidence
            .clone()
            .unwrap_or_else(|| "source incomplete".into());
        intent.state = IntentState::Unwound;
        let _guard = self.mutation_guard()?;
        let mut op = self.operation(&intent.operation)?;
        let movement: Relocation = serde_json::from_str(&op.request)?;
        let current = self.relocation_source(&op, &movement)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        if let Some(mut current) = current {
            current.state = "active".into();
            current.error = Some(format!("publication unwound: {evidence}"));
            current.files = fs::manifest(&source_root, 100_000)?;
            current.revision += 1;
            current.updated_at = now();
            save_artifact(&tx, &current)?;
        }
        if !matches!(op.state.as_str(), "succeeded" | "cancelled") {
            op.state = "cancelled".into();
            op.error = Some(format!("publication unwound: {evidence}"));
            save_operation(&tx, &op)?;
        }
        update_intent(&tx, &intent)?;
        event(&tx, &intent.artifact, "publication_unwound", &evidence)?;
        tx.commit()?;
        Ok(())
    }
}

/// Read and check a directory's `.runner-manifest.json`, if it has one.
pub(super) fn read_manifest(dir: &File) -> Result<Option<RunnerManifest>> {
    let Some(mut file) = (match fs::open_at(dir, Path::new(RUNNER_MANIFEST_NAME), false) {
        Ok(file) => Some(file),
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    }) else {
        return Ok(None);
    };
    let mut raw = Vec::new();
    Read::by_ref(&mut file)
        .take(64 * 1024 * 1024)
        .read_to_end(&mut raw)?;
    let manifest: RunnerManifest = serde_json::from_slice(&raw)?;
    if !manifest.verifies() {
        return Err(Error::Conflict(
            "publication manifest digest does not match its files".into(),
        ));
    }
    Ok(Some(manifest))
}

#[cfg(any(test, feature = "test-support"))]
impl Inventory {
    /// IMPL §7.6: this inventory's payload operation counts (see
    /// `fs::count_op`).
    pub fn fs_ops(&self) -> std::collections::HashMap<&'static str, u64> {
        fs::fs_ops_snapshot(&self.installation)
    }
    pub fn reset_fs_ops(&self) {
        fs::fs_ops_reset(&self.installation)
    }
}

#[cfg(test)]
mod tests {
    //! Contract §2.5 / plan M3 at the inventory: per-file moves under a
    //! journaled intent, manifest last, finish-or-unwind on restart.
    use super::*;

    fn fixture_in(base: &Path) -> (Inventory, PathBuf) {
        let root = base.join("processing");
        std::fs::create_dir_all(&root).unwrap();
        (Inventory::open(&base.join("state")).unwrap(), root)
    }

    fn outcome(path: &str, published: bool) -> DeliveryFileOutcome {
        DeliveryFileOutcome {
            source_path: path.into(),
            publication_path: published.then(|| path.into()),
            classification: if published { "video" } else { "repair_input" }.into(),
            outcome: if published {
                "published"
            } else {
                "excluded_source_cleanup"
            }
            .into(),
            reason: None,
            repair_dependency: false,
            source_cleanup_status: "pending".into(),
        }
    }

    /// A delivery-lifecycle job whose source holds `selected` payload and
    /// `siblings` that stay behind. Returns (selected entries, outcomes).
    fn job(
        db: &Inventory,
        root: &Path,
        job: u32,
        selected: &[(&str, &[u8])],
        siblings: &[(&str, &[u8])],
    ) -> (PathBuf, Vec<FileEntry>, Vec<DeliveryFileOutcome>) {
        let source = root.join(format!("release-{job}"));
        db.allocate_for_delivery(job, root, &source, true, true)
            .unwrap();
        for (path, bytes) in selected.iter().chain(siblings) {
            let path = source.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bytes).unwrap();
        }
        db.finish(job, &source, root, "active").unwrap();
        let artifact = db.for_job(job).unwrap().unwrap();
        let chosen = artifact
            .files
            .iter()
            .filter(|entry| selected.iter().any(|(path, _)| *path == entry.path))
            .cloned()
            .collect();
        let outcomes = selected
            .iter()
            .map(|(path, _)| outcome(path, true))
            .chain(siblings.iter().map(|(path, _)| outcome(path, false)))
            .collect();
        (source, chosen, outcomes)
    }

    const SELECTED: &[(&str, &[u8])] = &[
        ("Show.S01E01.mkv", b"episode one bytes"),
        ("Show.S01E02.mkv", b"episode two, a little longer"),
        ("Extras/featurette.mkv", b"extra"),
    ];
    const SIBLINGS: &[(&str, &[u8])] = &[
        ("Show.par2", b"recovery set"),
        ("Show.vol00+01.par2", b"recovery volume"),
    ];

    fn selected_bytes() -> u64 {
        SELECTED.iter().map(|(_, bytes)| bytes.len() as u64).sum()
    }

    /// Every selected file in exactly one place, and that place is the
    /// committed publication; the siblings remain in the source.
    fn assert_one_publication(target: &Path, source: &Path) {
        let manifest: RunnerManifest =
            serde_json::from_slice(&std::fs::read(target.join(RUNNER_MANIFEST_NAME)).unwrap())
                .unwrap();
        assert!(manifest.verifies());
        assert_eq!(manifest.files.len(), SELECTED.len());
        for (path, bytes) in SELECTED {
            assert_eq!(std::fs::read(target.join(path)).unwrap(), *bytes, "{path}");
            assert!(!source.join(path).exists(), "{path} is in two places");
        }
        for (path, _) in SIBLINGS {
            assert!(source.join(path).exists(), "unselected {path} stays");
            assert!(
                !target.join(path).exists(),
                "unselected {path} never published"
            );
        }
        let siblings_of_target = std::fs::read_dir(target.parent().unwrap())
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains(".runner-")
            })
            .count();
        assert_eq!(siblings_of_target, 1, "exactly one publication directory");
    }

    /// Plan M3 (a) at the inventory: N renames for N selected files, one
    /// read pass per file (and none in the delivery that follows), no copy;
    /// unselected siblings stay in the processing directory and retire with
    /// the delivery.
    #[test]
    fn same_volume_publication_renames_selected_only() {
        let base = tempfile::tempdir_in(
            Path::new("/dev/shm")
                .is_dir()
                .then_some("/dev/shm")
                .unwrap_or("/tmp"),
        )
        .unwrap();
        let (db, root) = fixture_in(base.path());
        let (source, selected, outcomes) = job(&db, &root, 61, SELECTED, SIBLINGS);
        let completed = base.path().join("completed");
        std::fs::create_dir(&completed).unwrap();
        let target = completed.join("release.job-61.runner-g");
        db.reset_fs_ops();
        crate::fileops::PAYLOAD_READ_BYTES.with(|total| total.set(0));
        db.publish_selected_with_transfer(
            61,
            &target,
            selected,
            outcomes,
            Some("t-61-token"),
            true,
            &|| Ok(()),
        )
        .unwrap();
        let ops = db.fs_ops();
        assert_eq!(ops.get("rename").copied(), Some(SELECTED.len() as u64));
        assert_eq!(ops.get("copy_bytes").copied().unwrap_or(0), 0);
        assert_eq!(ops.get("hash_bytes").copied(), Some(selected_bytes()));
        assert_eq!(ops.get("manifest_write").copied(), Some(1));
        assert_one_publication(&target, &source);
        let intent = db
            .relocation_intent(&db.for_job(61).unwrap().unwrap().generation)
            .unwrap()
            .unwrap();
        assert_eq!(intent.state, IntentState::Committed);
        assert!(!intent.cross_volume);

        let delivery = db.create_delivery(61, true, Some("t-61-token")).unwrap();
        assert_eq!(
            db.fs_ops().get("hash_bytes").copied(),
            Some(selected_bytes()),
            "the delivery reuses the journaled digests: one hash pass in all"
        );
        assert_eq!(
            crate::fileops::PAYLOAD_READ_BYTES.with(|total| total.get()),
            selected_bytes(),
            "no other payload read anywhere in publication + delivery"
        );
        let manifest = delivery.manifest.clone().expect("manifest on the feed row");
        assert_eq!(manifest.digest, delivery.manifest_digest);
        assert_eq!(manifest.transfer.as_deref(), Some("t-61-token"));
        assert_eq!(manifest.job, 61);
        assert_eq!(
            delivery.files.len(),
            SELECTED.len(),
            "the marker is not a payload file"
        );
        assert_eq!(manifest.generation, delivery.publication_generation);

        let cleanup = db.retire_delivery_source(&delivery.delivery_id).unwrap();
        assert_eq!(cleanup.source_cleanup.status, "succeeded");
        assert!(!source.exists(), "the processing directory is disposed");
        assert_one_publication_after_retire(&target);
    }

    fn assert_one_publication_after_retire(target: &Path) {
        for (path, bytes) in SELECTED {
            assert_eq!(std::fs::read(target.join(path)).unwrap(), *bytes, "{path}");
        }
    }

    /// Plan M3 (c) at the inventory: a crash after the first rename, after
    /// the last rename (before the manifest), and after the manifest (before
    /// the inventory commit) each leave, after a restart, exactly one usable
    /// publication, no file in two places, and disposal refused until then.
    #[test]
    fn each_crash_point_restarts_to_one_publication() {
        for stage in ["after_first_rename", "after_last_rename", "after_manifest"] {
            let base = tempfile::tempdir().unwrap();
            let (db, root) = fixture_in(base.path());
            let (source, selected, outcomes) = job(&db, &root, 62, SELECTED, SIBLINGS);
            let completed = base.path().join("completed");
            std::fs::create_dir(&completed).unwrap();
            let target = completed.join("release.job-62.runner-g");
            super::super::relocation::FAULTS
                .with(|faults| faults.borrow_mut().insert(stage, libc::EIO));
            assert!(db
                .publish_selected_with_transfer(
                    62,
                    &target,
                    selected,
                    outcomes,
                    Some("t-62"),
                    true,
                    &|| Ok(())
                )
                .is_err());
            let artifact = db.for_job(62).unwrap().unwrap();
            assert!(db.open_intent_for_job(62).unwrap(), "{stage}: intent open");
            assert!(
                db.request_delete(&artifact.id, artifact.revision, "dispose-early", 0)
                    .is_err(),
                "{stage}: disposal refused while the move is open"
            );
            db.close().unwrap();
            // Restart.
            let db = Inventory::open(&base.path().join("state")).unwrap();
            db.reconcile_relocations().unwrap();
            assert!(!db.open_intent_for_job(62).unwrap(), "{stage}: resolved");
            assert!(db.open_relocation_intents().unwrap().is_empty());
            let intent = db.relocation_intent(&artifact.generation).unwrap().unwrap();
            assert_eq!(intent.state, IntentState::Committed, "{stage}: finished");
            assert_one_publication(&target, &source);
            let delivery = db.create_delivery(62, true, Some("t-62")).unwrap();
            assert_eq!(
                delivery.manifest.as_ref().unwrap().digest,
                delivery.manifest_digest,
                "{stage}"
            );
            db.retire_delivery_source(&delivery.delivery_id).unwrap();
            assert!(!source.exists(), "{stage}: disposed after commit");
        }
    }

    /// Plan M3 (e): with the source half-moved and a remaining source file
    /// corrupted, restart unwinds rather than finishes; the next publication
    /// attempt is an I/O failure.
    #[test]
    fn crash_with_corrupted_source_unwinds() {
        let base = tempfile::tempdir().unwrap();
        let (db, root) = fixture_in(base.path());
        let (source, selected, outcomes) = job(&db, &root, 63, SELECTED, SIBLINGS);
        let completed = base.path().join("completed");
        std::fs::create_dir(&completed).unwrap();
        let target = completed.join("release.job-63.runner-g");
        super::super::relocation::FAULTS
            .with(|faults| faults.borrow_mut().insert("after_first_rename", libc::EIO));
        assert!(db
            .publish_selected_with_transfer(
                63,
                &target,
                selected.clone(),
                outcomes.clone(),
                None,
                true,
                &|| Ok(())
            )
            .is_err());
        let generation = db.for_job(63).unwrap().unwrap().generation;
        let intent = db.relocation_intent(&generation).unwrap().unwrap();
        let moved = intent.files.iter().filter(|file| file.moved).count();
        assert_eq!(moved, 1, "half-moved");
        let remaining = intent.files.iter().find(|file| !file.moved).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(source.join(&remaining.src), b"corrupted bytes!!").unwrap();
        db.close().unwrap();
        let db = Inventory::open(&base.path().join("state")).unwrap();
        db.reconcile_relocations().unwrap();
        let intent = db.relocation_intent(&generation).unwrap().unwrap();
        assert_eq!(intent.state, IntentState::Unwound);
        assert!(intent
            .evidence
            .as_deref()
            .unwrap()
            .contains("changed since the move was journaled"));
        assert!(!target.exists(), "the target is removed");
        for (path, bytes) in SELECTED {
            if *path == remaining.src {
                continue;
            }
            assert_eq!(
                std::fs::read(source.join(path)).unwrap(),
                *bytes,
                "{path} put back"
            );
        }
        assert!(!db.open_intent_for_job(63).unwrap());
        let error = db
            .publish_selected_with_transfer(63, &target, selected, outcomes, None, true, &|| Ok(()))
            .unwrap_err();
        assert!(
            matches!(&error, Error::Io(io) if io.to_string().contains("publication unwound")),
            "{error}"
        );
    }

    /// Plan M3 (d): no NFS fixture exists in CI, so the PR #247 fallback
    /// chain is driven by the rename seam: `renameat2` answers EINVAL and the
    /// file still publishes, through `linkat` + `unlink`.
    #[test]
    fn nfs_fixture_fallback_chain() {
        let base = tempfile::tempdir().unwrap();
        let (db, root) = fixture_in(base.path());
        let (source, selected, outcomes) = job(&db, &root, 64, SELECTED, SIBLINGS);
        let completed = base.path().join("completed");
        std::fs::create_dir(&completed).unwrap();
        let target = completed.join("release.job-64.runner-g");
        super::super::fs::RENAME_FAILURE.with(|failure| failure.set(Some(libc::EINVAL)));
        db.publish_selected_with_transfer(64, &target, selected, outcomes, None, true, &|| Ok(()))
            .unwrap();
        assert!(
            super::super::fs::RENAME_FAILURE
                .with(|failure| failure.get())
                .is_none(),
            "the first rename took the fallback"
        );
        assert_one_publication(&target, &source);
    }

    /// Cross volume: one streaming copy per file that produces the digest
    /// as it writes — no separate hash pass; the originals stay in the
    /// source until the delivery retires them.
    #[test]
    fn cross_volume_publication_copies_once() {
        let shm = Path::new("/dev/shm");
        let base = tempfile::tempdir().unwrap();
        let target_base =
            tempfile::tempdir_in(if shm.is_dir() { shm } else { base.path() }).unwrap();
        let (db, root) = fixture_in(base.path());
        let (source, selected, outcomes) = job(&db, &root, 65, SELECTED, SIBLINGS);
        let completed = target_base.path().join("completed");
        std::fs::create_dir(&completed).unwrap();
        use std::os::unix::fs::MetadataExt;
        let cross =
            std::fs::metadata(&root).unwrap().dev() != std::fs::metadata(&completed).unwrap().dev();
        let target = completed.join("release.job-65.runner-g");
        db.reset_fs_ops();
        db.publish_selected_with_transfer(65, &target, selected, outcomes, None, true, &|| Ok(()))
            .unwrap();
        let ops = db.fs_ops();
        assert_eq!(ops.get("rename").copied(), Some(SELECTED.len() as u64));
        if !cross {
            eprintln!("cross_volume_publication_copies_once: /dev/shm shares a device with the temp dir; asserted same-volume shape only");
            return;
        }
        assert_eq!(ops.get("copy_bytes").copied(), Some(selected_bytes()));
        assert_eq!(ops.get("hash_bytes").copied().unwrap_or(0), 0);
        for (path, bytes) in SELECTED {
            assert_eq!(std::fs::read(target.join(path)).unwrap(), *bytes);
            assert!(source.join(path).exists(), "original kept until retirement");
        }
        let delivery = db.create_delivery(65, true, None).unwrap();
        assert!(delivery.manifest.as_ref().unwrap().cross_volume);
        assert_eq!(db.fs_ops().get("hash_bytes").copied().unwrap_or(0), 0);
        db.retire_delivery_source(&delivery.delivery_id).unwrap();
        assert!(!source.exists());
        assert_one_publication_after_retire(&target);
    }

    /// A cross-volume move reserves its bytes before anything is journaled:
    /// a refused reservation leaves no intent and the source untouched.
    #[test]
    fn cross_volume_capacity_refusal_journals_nothing() {
        let shm = Path::new("/dev/shm");
        let base = tempfile::tempdir().unwrap();
        let target_base =
            tempfile::tempdir_in(if shm.is_dir() { shm } else { base.path() }).unwrap();
        let (db, root) = fixture_in(base.path());
        let (source, selected, outcomes) = job(&db, &root, 66, SELECTED, SIBLINGS);
        let completed = target_base.path().join("completed");
        std::fs::create_dir(&completed).unwrap();
        use std::os::unix::fs::MetadataExt;
        if std::fs::metadata(&root).unwrap().dev() == std::fs::metadata(&completed).unwrap().dev() {
            eprintln!("cross_volume_capacity_refusal_journals_nothing: no second device; nothing to assert");
            return;
        }
        crate::capacity::FAIL_NEXT_RESERVATION.with(|fail| fail.set(true));
        let target = completed.join("release.job-66.runner-g");
        assert!(db
            .publish_selected_with_transfer(66, &target, selected, outcomes, None, true, &|| Ok(()))
            .is_err());
        assert!(db.open_relocation_intents().unwrap().is_empty());
        assert!(!target.exists());
        for (path, bytes) in SELECTED {
            assert_eq!(std::fs::read(source.join(path)).unwrap(), *bytes);
        }
    }

    /// The golden vector of the deployed `runner-manifest-v1` digest, now
    /// carried by `.runner-manifest.json` (M1a deferred this to M3).
    #[test]
    fn manifest_digest_golden_vector_unchanged() {
        let sha = "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881";
        let manifest = RunnerManifest::new(
            "0123456789abcdef0123456789abcdef".into(),
            2333,
            Some("t-41-00112233445566778899aabbccddeeff".into()),
            1_794_076_138,
            false,
            vec![
                ManifestFile {
                    path: "z.mkv".into(),
                    bytes: 1,
                    sha256: sha.into(),
                },
                ManifestFile {
                    path: "é<&.mkv".into(),
                    bytes: 1,
                    sha256: sha.into(),
                },
            ],
        );
        assert_eq!(
            manifest.digest,
            "a0acee1d6757529c7deca3dc2ac2fa2f0b6e96db781d83b8d85d732cedd6459f"
        );
        assert_eq!(manifest.files[0].path, "z.mkv", "bytewise path order");
        let json = serde_json::to_value(&manifest).unwrap();
        for key in [
            "generation",
            "job",
            "transfer",
            "created_at",
            "cross_volume",
            "files",
            "digest",
        ] {
            assert!(json.get(key).is_some(), "manifest field {key}");
        }
        assert_eq!(json["files"][0]["bytes"], 1);
        let parsed: RunnerManifest = serde_json::from_value(json).unwrap();
        assert!(parsed.verifies());
    }

    /// Plan M3 (b): the compat fixture's digest, produced by curator's own
    /// `ports.ManifestDigest`, equals runner's recomputation over the same
    /// manifest file.
    #[test]
    fn manifest_digest_matches_curator_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/runner_manifest_v1.json"))
                .unwrap();
        let manifest: RunnerManifest = serde_json::from_value(fixture["manifest"].clone()).unwrap();
        let curator = fixture["curatorDigest"].as_str().unwrap();
        assert_eq!(RunnerManifest::digest_of(&manifest.files), curator);
        assert_eq!(manifest.digest, curator);
        assert!(manifest.verifies());
        let rebuilt = RunnerManifest::new(
            manifest.generation.clone(),
            manifest.job,
            manifest.transfer.clone(),
            manifest.created_at,
            manifest.cross_volume,
            manifest.files.iter().rev().cloned().collect(),
        );
        assert_eq!(
            rebuilt, manifest,
            "runner writes the files in bytewise order"
        );
    }
}
