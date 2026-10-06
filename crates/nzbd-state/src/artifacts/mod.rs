//! Authoritative payload inventory, deliberately independent of History.
//!
//! SQLite owns intent and policy; external sidecars own allocation identity.
//! Nothing in a media payload is a marker or grants permission to remove it.
pub(crate) mod fs;
mod policy;
mod publication;
pub use policy::{RetentionChange, RetentionPreview};
mod recovery;
mod relocation;
pub use relocation::RelocationResult;
mod tasks;
mod transforms;
mod workspace_lifecycle;
pub use recovery::{Receipt, ReceiptFile, Recovery, RecoveryFile};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
pub use transforms::Workspace;
pub use workspace_lifecycle::{
    AttemptUse, FinalizationProof, SourceGeneration, StagedCompletion, WorkspaceAssessment,
};

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("inventory database: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("filesystem: {0}")]
    Io(#[from] std::io::Error),
    #[error("inventory data: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Conflict(String),
    #[error("artifact not found")]
    NotFound,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Identity {
    pub device: u64,
    pub inode: u64,
    pub bytes: u64,
    pub modified: String,
    pub directory: bool,
}
impl Identity {
    pub fn same_object(&self, other: &Self) -> bool {
        self.device == other.device
            && self.inode == other.inode
            && self.directory == other.directory
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileEntry {
    pub path: String,
    pub identity: Identity,
    pub digest: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Artifact {
    pub id: String,
    pub generation: String,
    pub revision: u64,
    pub job: Option<u32>,
    pub path: PathBuf,
    pub root: PathBuf,
    pub root_identity: Identity,
    pub identity: Option<Identity>,
    pub state: String,
    pub owned: bool,
    pub keep: bool,
    pub hold: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub retention_seconds: u64,
    pub deadline: Option<i64>,
    pub eligible_seconds: u64,
    #[serde(default)]
    pub files: Vec<FileEntry>,
    pub error: Option<String>,
    /// When a walk last measured `files`. Discovery records a directory
    /// without walking it, so an unknown folder with no entries is
    /// "not measured yet" until this is set — never "empty".
    #[serde(default)]
    pub inspected_at: Option<i64>,
}
impl Artifact {
    pub fn terminal(&self) -> bool {
        matches!(self.state.as_str(), "deleted" | "source_gone")
    }
    /// Regular files and their bytes; directories are structure, not payload.
    pub fn summary(&self) -> (u64, u64) {
        let files = self.files.iter().filter(|f| !f.identity.directory);
        (
            files.clone().count() as u64,
            files.map(|f| f.identity.bytes).sum(),
        )
    }
    /// True once a walk has populated `files` (a finalized job, a restore,
    /// or an inspection). A discovered-but-unmeasured folder reports false.
    pub fn measured(&self) -> bool {
        self.inspected_at.is_some() || !self.files.is_empty()
    }
    fn eligible(&self) -> bool {
        self.owned
            && matches!(self.state.as_str(), "parked_failed" | "recovery_imported")
            && !self.keep
            && self.hold.is_none()
            && self.retention_seconds > 0
    }
    pub fn earliest_expiry(&self, now: i64) -> Option<i64> {
        if !self.eligible() {
            return None;
        }
        Some(self.deadline?.max(
            now.saturating_add(self.retention_seconds.saturating_sub(self.eligible_seconds) as i64),
        ))
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Operation {
    pub id: String,
    pub artifact: String,
    pub kind: String,
    pub state: String,
    pub request: String,
    pub created_at: i64,
    pub not_before: i64,
    pub attempts: u32,
    pub next_retry: i64,
    pub error: Option<String>,
}

/// New deletes bind authorization to an allocation, never just its reusable
/// row ID. Old journals remain readable but cannot authorize startup repair
/// without generation evidence.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum DeleteRequest {
    Scoped((String, u64, u64, bool, String)),
    Legacy((String, u64, u64, bool)),
}
impl DeleteRequest {
    fn fields(&self) -> (&str, u64, u64, bool, Option<&str>) {
        match self {
            Self::Scoped((id, rev, undo, auto, generation)) => {
                (id, *rev, *undo, *auto, Some(generation))
            }
            Self::Legacy((id, rev, undo, auto)) => (id, *rev, *undo, *auto, None),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Settings {
    pub enabled: bool,
    pub failed_retention_days: u32,
    pub recovery_root: PathBuf,
    pub consumer_token: String,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            enabled: false,
            failed_retention_days: 7,
            recovery_root: PathBuf::new(),
            consumer_token: String::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ListFilter {
    /// Every record, cleared ones included.
    All,
    /// Everything still on disk (the default view).
    #[default]
    Live,
    /// Live records that need a person: not owned, on hold, or in error.
    Attention,
    /// Live records Runner owns outright.
    Owned,
    /// Deleted / source-gone records kept for the audit trail.
    Cleared,
}
impl ListFilter {
    fn sql(self) -> &'static str {
        match self {
            Self::All => "1=1",
            Self::Live => "state NOT IN ('deleted','source_gone')",
            Self::Attention => "state NOT IN ('deleted','source_gone') AND (json_extract(data,'$.owned')=0 OR json_extract(data,'$.hold') IS NOT NULL OR json_extract(data,'$.error') IS NOT NULL)",
            Self::Owned => "state NOT IN ('deleted','source_gone') AND json_extract(data,'$.owned')=1",
            Self::Cleared => "state IN ('deleted','source_gone')",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ListSort {
    #[default]
    Updated,
    Size,
    Files,
    Name,
}
#[derive(Clone, Debug, Default)]
pub struct ListQuery {
    pub offset: usize,
    pub limit: usize,
    pub filter: ListFilter,
    pub sort: ListSort,
    /// Case-insensitive substring of the path.
    pub q: String,
}
#[derive(Clone, Debug, Serialize)]
pub struct ListRow {
    /// The record without its manifest; ask `measured` here, not
    /// `artifact.measured()`, which cannot see the stripped file list.
    pub artifact: Artifact,
    pub files: u64,
    pub bytes: u64,
    pub measured: bool,
}
#[derive(Clone, Debug, Default, Serialize, PartialEq, Eq)]
pub struct ListCounts {
    pub all: u64,
    pub live: u64,
    pub attention: u64,
    pub owned: u64,
    pub cleared: u64,
    pub live_bytes: u64,
}
#[derive(Clone, Debug, Serialize)]
pub struct ListPage {
    pub rows: Vec<ListRow>,
    pub total: u64,
    pub counts: ListCounts,
}

pub struct Inventory {
    db: Mutex<Connection>,
    workspace_uses: Mutex<std::collections::HashMap<(String, String), usize>>,
    workspace_cursor: Mutex<String>,
    _process_lock: File,
    closed: std::sync::atomic::AtomicBool,
    state_dir: PathBuf,
    pub installation: String,
    // Serialized mutation coordinator. Reads never wait for filesystem work.
    mutation: Mutex<()>,
    clocks: Mutex<std::collections::HashMap<String, (u64, Instant)>>,
    reconcile_cursor: Mutex<String>,
    // One task runner at a time: the maintenance tick and an API kick can
    // both drain the queue, and a task must never execute twice.
    tasks: Mutex<()>,
}

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
fn id(db: &Connection) -> Result<String> {
    Ok(db.query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0))?)
}
fn read<T: serde::de::DeserializeOwned>(db: &Connection, table: &str, key: &str) -> Result<T> {
    let value: Option<String> = db
        .query_row(
            &format!("SELECT data FROM {table} WHERE id=?1"),
            [key],
            |r| r.get(0),
        )
        .optional()?;
    serde_json::from_str(&value.ok_or(Error::NotFound)?).map_err(Into::into)
}
fn save_artifact(db: &Connection, a: &Artifact) -> Result<()> {
    let (files, bytes) = a.summary();
    db.execute("INSERT INTO artifacts(id,job,path,state,updated_at,data,files,bytes) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
        ON CONFLICT(id) DO UPDATE SET job=excluded.job,path=excluded.path,state=excluded.state,updated_at=excluded.updated_at,data=excluded.data,files=excluded.files,bytes=excluded.bytes",
        params![a.id, a.job, a.path.to_string_lossy(), a.state, a.updated_at, serde_json::to_string(a)?, files as i64, bytes as i64])?;
    Ok(())
}
fn save_operation(db: &Connection, op: &Operation) -> Result<()> {
    db.execute(
        "INSERT INTO operations(id,artifact,state,data) VALUES(?1,?2,?3,?4)
       ON CONFLICT(id) DO UPDATE SET state=excluded.state,data=excluded.data",
        params![op.id, op.artifact, op.state, serde_json::to_string(op)?],
    )?;
    Ok(())
}
fn event(db: &Connection, artifact: &str, kind: &str, detail: &str) -> Result<()> {
    db.execute(
        "INSERT INTO events(artifact,at,kind,detail) VALUES(?1,?2,?3,?4)",
        params![artifact, now(), kind, detail],
    )?;
    Ok(())
}

impl Inventory {
    pub fn open(state_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(state_dir)?;
        let process_lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(state_dir.join("artifacts.lock"))?;
        process_lock.try_lock().map_err(|e| {
            Error::Conflict(format!(
                "inventory is already open; stop the daemon before restore: {e}"
            ))
        })?;
        let db = Connection::open(state_dir.join("artifacts.sqlite"))?;
        db.busy_timeout(Duration::from_secs(5))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
          CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS artifacts(id TEXT PRIMARY KEY,job INTEGER,path TEXT NOT NULL,state TEXT NOT NULL,updated_at INTEGER NOT NULL,data TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS artifacts_recent ON artifacts(updated_at DESC,id);
          CREATE INDEX IF NOT EXISTS artifacts_live_recent ON artifacts(updated_at DESC,id) WHERE state NOT IN ('deleted','source_gone');
          CREATE INDEX IF NOT EXISTS artifacts_job ON artifacts(job);
          CREATE INDEX IF NOT EXISTS artifacts_path ON artifacts(path);
          CREATE INDEX IF NOT EXISTS artifacts_state ON artifacts(state,updated_at);
          CREATE TABLE IF NOT EXISTS operations(id TEXT PRIMARY KEY,artifact TEXT NOT NULL,state TEXT NOT NULL,data TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS operations_state ON operations(state);
          CREATE TABLE IF NOT EXISTS recoveries(id TEXT PRIMARY KEY,artifact TEXT NOT NULL,state TEXT NOT NULL,data TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS workspace_finalizations(source TEXT NOT NULL,generation TEXT NOT NULL,outcome TEXT NOT NULL,proof TEXT NOT NULL,job INTEGER,activated INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(source,generation));
          CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT,artifact TEXT NOT NULL,at INTEGER NOT NULL,kind TEXT NOT NULL,detail TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS events_artifact ON events(artifact,seq);
          INSERT OR IGNORE INTO meta VALUES('schema','1');
          INSERT OR IGNORE INTO meta VALUES('installation',lower(hex(randomblob(16))));")?;
        let schema: String =
            db.query_row("SELECT value FROM meta WHERE key='schema'", [], |r| {
                r.get(0)
            })?;
        if schema != "1" {
            return Err(Error::Conflict(format!(
                "unsupported inventory schema {schema}"
            )));
        }
        let installation =
            db.query_row("SELECT value FROM meta WHERE key='installation'", [], |r| {
                r.get(0)
            })?;
        // Summary columns (schema 1, additive): the list sorts and counts by
        // size without deserializing every manifest. Backfilled once from
        // the JSON so an inventory written before the columns existed is
        // sortable the moment it opens.
        let has_summary: bool = db.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('artifacts') WHERE name='bytes'",
            [],
            |r| r.get::<_, i64>(0).map(|n| n > 0),
        )?;
        if !has_summary {
            db.execute_batch(
                "BEGIN;
                 ALTER TABLE artifacts ADD COLUMN files INTEGER NOT NULL DEFAULT 0;
                 ALTER TABLE artifacts ADD COLUMN bytes INTEGER NOT NULL DEFAULT 0;
                 UPDATE artifacts SET
                   files=(SELECT COUNT(*) FROM json_each(json_extract(artifacts.data,'$.files')) f
                          WHERE json_extract(f.value,'$.identity.directory')=0),
                   bytes=(SELECT COALESCE(SUM(json_extract(f.value,'$.identity.bytes')),0)
                          FROM json_each(json_extract(artifacts.data,'$.files')) f
                          WHERE json_extract(f.value,'$.identity.directory')=0);
                 CREATE INDEX IF NOT EXISTS artifacts_bytes ON artifacts(bytes DESC,updated_at DESC,id);
                 COMMIT;",
            )?;
        }
        fs::sync_directory(&fs::open_dir(state_dir)?)?;
        Ok(Self {
            db: Mutex::new(db),
            workspace_uses: Mutex::new(std::collections::HashMap::new()),
            workspace_cursor: Mutex::new(String::new()),
            _process_lock: process_lock,
            closed: std::sync::atomic::AtomicBool::new(false),
            state_dir: state_dir.into(),
            installation,
            mutation: Mutex::new(()),
            clocks: Mutex::new(std::collections::HashMap::new()),
            reconcile_cursor: Mutex::new(String::new()),
            tasks: Mutex::new(()),
        })
    }
    fn mutation_guard(&self) -> Result<std::sync::MutexGuard<'_, ()>> {
        let guard = self.mutation.lock().unwrap();
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::Conflict("inventory has stopped".into()));
        }
        Ok(guard)
    }
    /// Called only after the engine's writers and maintenance tasks finish.
    /// Old API handles retain read access but cannot mutate a restarted daemon.
    pub fn close(&self) -> Result<()> {
        let _guard = self.mutation.lock().unwrap();
        if !self.closed.swap(true, std::sync::atomic::Ordering::AcqRel) {
            self._process_lock.unlock()?;
        }
        Ok(())
    }
    pub fn settings(&self) -> Result<Settings> {
        let db = self.db.lock().unwrap();
        let raw: Option<String> = db
            .query_row("SELECT value FROM meta WHERE key='settings'", [], |r| {
                r.get(0)
            })
            .optional()?;
        Ok(raw
            .map(|s| serde_json::from_str(&s))
            .transpose()?
            .unwrap_or_default())
    }
    /// Advisory prerequisites never prevent accepting the enable switch.
    pub fn set_settings(&self, s: &Settings) -> Result<()> {
        let _guard = self.mutation_guard()?;
        if s.failed_retention_days > 3650 {
            return Err(Error::Conflict(
                "retention must be at most 3650 days".into(),
            ));
        }
        self.db.lock().unwrap().execute("INSERT INTO meta VALUES('settings',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [serde_json::to_string(s)?])?;
        Ok(())
    }
    pub fn get(&self, key: &str) -> Result<Artifact> {
        read(&self.db.lock().unwrap(), "artifacts", key)
    }
    pub fn for_job(&self, job: u32) -> Result<Option<Artifact>> {
        let db = self.db.lock().unwrap();
        let raw: Option<String> = db
            .query_row(
                "SELECT data FROM artifacts WHERE job=?1 ORDER BY updated_at DESC LIMIT 1",
                [job],
                |r| r.get(0),
            )
            .optional()?;
        Ok(raw.map(|s| serde_json::from_str(&s)).transpose()?)
    }
    pub fn for_path(&self, path: &Path) -> Result<Option<Artifact>> {
        let db = self.db.lock().unwrap();
        let raw: Option<String> = db
            .query_row(
                "SELECT data FROM artifacts WHERE path=?1 ORDER BY updated_at DESC LIMIT 1",
                [path.to_string_lossy()],
                |r| r.get(0),
            )
            .optional()?;
        Ok(raw.map(|s| serde_json::from_str(&s)).transpose()?)
    }
    pub fn list(&self, offset: usize, limit: usize) -> Result<Vec<Artifact>> {
        self.list_visible(offset, limit, true)
    }
    pub fn list_visible(
        &self,
        offset: usize,
        limit: usize,
        include_terminal: bool,
    ) -> Result<Vec<Artifact>> {
        let db = self.db.lock().unwrap();
        let query = if include_terminal {
            "SELECT data FROM artifacts ORDER BY updated_at DESC,id LIMIT ?1 OFFSET ?2"
        } else {
            "SELECT data FROM artifacts WHERE state NOT IN ('deleted','source_gone') ORDER BY updated_at DESC,id LIMIT ?1 OFFSET ?2"
        };
        let mut stmt = db.prepare(query)?;
        let rows = stmt.query_map(params![limit.min(200) as i64, offset as i64], |r| {
            r.get::<_, String>(0)
        })?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }
    /// One page of the inventory, shaped for a list: manifests are stripped
    /// in SQL (`json_remove`) so a 100k-entry folder costs the same as an
    /// empty one, and every filter has a count so the UI can say how many
    /// rows sit behind each view instead of making the operator page to find out.
    pub fn list_page(&self, q: &ListQuery) -> Result<ListPage> {
        let db = self.db.lock().unwrap();
        let mut sql_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        let mut where_sql = String::from(q.filter.sql());
        let needle = q.q.trim();
        if !needle.is_empty() {
            let escaped = needle
                .replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_");
            where_sql.push_str(" AND path LIKE ?1 ESCAPE '\\'");
            sql_params.push(Box::new(format!("%{escaped}%")));
        }
        let order = match q.sort {
            ListSort::Updated => "updated_at DESC,id",
            ListSort::Size => "bytes DESC,updated_at DESC,id",
            ListSort::Files => "files DESC,updated_at DESC,id",
            ListSort::Name => "path COLLATE NOCASE ASC,id",
        };
        let limit = q.limit.clamp(1, 200) as i64;
        let total: i64 = db.query_row(
            &format!("SELECT COUNT(*) FROM artifacts WHERE {where_sql}"),
            rusqlite::params_from_iter(sql_params.iter().map(|p| p.as_ref())),
            |r| r.get(0),
        )?;
        let mut stmt = db.prepare(&format!(
            "SELECT json_remove(data,'$.files'),files,bytes,json_array_length(json_extract(data,'$.files')) FROM artifacts WHERE {where_sql} ORDER BY {order} LIMIT {limit} OFFSET {}",
            q.offset as i64
        ))?;
        let rows = stmt
            .query_map(
                rusqlite::params_from_iter(sql_params.iter().map(|p| p.as_ref())),
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, Option<i64>>(3)?.unwrap_or(0),
                    ))
                },
            )?
            .map(|r| {
                let (raw, files, bytes, entries) = r?;
                let artifact: Artifact = serde_json::from_str(&raw)?;
                Ok(ListRow {
                    measured: artifact.inspected_at.is_some() || entries > 0,
                    artifact,
                    files: files.max(0) as u64,
                    bytes: bytes.max(0) as u64,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut counts = ListCounts::default();
        for (filter, slot) in [
            (ListFilter::All, &mut counts.all),
            (ListFilter::Live, &mut counts.live),
            (ListFilter::Attention, &mut counts.attention),
            (ListFilter::Owned, &mut counts.owned),
            (ListFilter::Cleared, &mut counts.cleared),
        ] {
            *slot = db.query_row(
                &format!("SELECT COUNT(*) FROM artifacts WHERE {}", filter.sql()),
                [],
                |r| r.get::<_, i64>(0),
            )? as u64;
        }
        counts.live_bytes = db.query_row(
            &format!(
                "SELECT COALESCE(SUM(bytes),0) FROM artifacts WHERE {}",
                ListFilter::Live.sql()
            ),
            [],
            |r| r.get::<_, i64>(0),
        )? as u64;
        Ok(ListPage {
            rows,
            total: total as u64,
            counts,
        })
    }
    pub fn events(&self, key: &str, after: i64) -> Result<Vec<serde_json::Value>> {
        let db = self.db.lock().unwrap();
        let mut stmt = db.prepare("SELECT seq,at,kind,detail FROM events WHERE artifact=?1 AND seq>?2 ORDER BY seq LIMIT 100")?;
        let rows = stmt.query_map(params![key,after], |r| Ok(serde_json::json!({"seq":r.get::<_,i64>(0)?,"at":r.get::<_,i64>(1)?,"kind":r.get::<_,String>(2)?,"detail":r.get::<_,String>(3)?})))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }
    fn sidecar(&self, a: &Artifact) -> Result<()> {
        let dir = self.state_dir.join("artifact-identities").join(&a.id);
        std::fs::create_dir_all(&dir)?;
        let target = dir.join(format!("{}.json", a.generation));
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let tmp = dir.join(format!(
            "{}.{}.{nonce}.tmp",
            a.generation,
            std::process::id()
        ));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        let result = (|| {
            file.write_all(&serde_json::to_vec(&serde_json::json!({"installation":self.installation,"artifact":a.id,"generation":a.generation,"path":a.path,"root":a.root,"root_identity":a.root_identity,"identity":a.identity}))?)?;
            file.sync_all()?;
            std::fs::rename(&tmp, &target)?;
            fs::sync_directory(&fs::open_dir(&dir)?)?;
            fs::sync_directory(&fs::open_dir(dir.parent().unwrap())?)?;
            fs::sync_directory(&fs::open_dir(&self.state_dir)?)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(tmp);
        }
        result
    }
    /// Commit allocation intent before the directory exists or a writer opens
    /// a file. A directory already on disk must be explicitly adopted instead.
    pub fn allocate(&self, job: u32, root: &Path, path: &Path) -> Result<Artifact> {
        let _guard = self.mutation_guard()?;
        if let Some(a) = self.for_job(job)? {
            if a.path == path && a.state == "active" {
                self.verify(&a)?;
                return Ok(a);
            }
            return Err(Error::Conflict(
                "job already owns a different or terminal allocation".into(),
            ));
        }
        fs::absolute(path)?;
        if path.parent() != Some(root) {
            return Err(Error::Conflict(
                "allocation must be an immediate child of its root".into(),
            ));
        }
        let root_file = fs::open_dir(root)?;
        let root_identity = fs::identity(&root_file.metadata()?);
        if std::fs::symlink_metadata(path).is_ok() {
            return Err(Error::Conflict(
                "existing payload needs explicit ownership review".into(),
            ));
        }
        let db = self.db.lock().unwrap();
        let mut a = Artifact {
            id: id(&db)?,
            generation: id(&db)?,
            revision: 1,
            job: Some(job),
            path: path.into(),
            root: root.into(),
            root_identity,
            identity: None,
            state: "allocating".into(),
            owned: cfg!(unix),
            keep: !cfg!(unix),
            hold: None,
            created_at: now(),
            updated_at: now(),
            retention_seconds: 0,
            deadline: None,
            eligible_seconds: 0,
            files: Vec::new(),
            error: None,
            inspected_at: None,
        };
        save_artifact(&db, &a)?;
        drop(db);
        std::fs::create_dir(path)?;
        fs::sync_directory(&root_file)?;
        a.identity = Some(fs::identity(&fs::open_dir(path)?.metadata()?));
        self.sidecar(&a)?;
        a.state = "active".into();
        save_artifact(&self.db.lock().unwrap(), &a)?;
        Ok(a)
    }
    /// Upgrade a live queue record without claiming its pre-existing bytes.
    /// It may resume writing; destructive actions still need operator adoption.
    pub fn register_legacy_active(&self, job: u32, root: &Path, path: &Path) -> Result<()> {
        if !path.exists() {
            return Ok(());
        }
        if let Some(mut previous) = self.for_job(job)? {
            if previous.path == path && self.verify(&previous).is_ok() {
                return Ok(());
            }
            let _guard = self.mutation_guard()?;
            previous.job = None;
            previous.state = if previous.path == path {
                "source_gone"
            } else {
                "retained"
            }
            .into();
            previous.keep = true;
            previous.hold = Some("review: previous external job generation".into());
            previous.revision += 1;
            save_artifact(&self.db.lock().unwrap(), &previous)?;
        }
        let mut a = self.discover(root, path, true)?;
        let _guard = self.mutation_guard()?;
        a.job = Some(job);
        a.state = "active".into();
        save_artifact(&self.db.lock().unwrap(), &a)
    }

    /// Called once before queue writers start, never from the periodic worker.
    pub fn reconcile_startup(&self, live_jobs: &[u32]) -> Result<()> {
        self.reconcile_deleted_artifacts()?;
        self.reconcile_relocations()?;
        self.reconcile_transforms()?;
        let _guard = self.mutation_guard()?;
        let rows = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare(
                "SELECT data FROM artifacts WHERE state IN ('allocating','retiring','active')",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in rows {
            let mut a: Artifact = serde_json::from_str(&raw)?;
            // A scratch artifact intentionally has no job ID. Its transform
            // operation supplies custody; it is not an orphaned download.
            if self.is_transform_scratch(&a)? {
                continue;
            }
            let live = a.job.is_some_and(|job| live_jobs.contains(&job));
            if a.state == "active" && live {
                continue;
            }
            if self.verify_root(&a).is_err() {
                continue;
            }
            if a.state == "allocating" {
                if !a.path.try_exists()? && live {
                    std::fs::create_dir(&a.path)?;
                    fs::sync_directory(&fs::open_dir(&a.root)?)?;
                }
                if !a.path.try_exists()? {
                    a.state = "source_gone".into();
                } else {
                    // A crash between mkdir and sidecar cannot prove which
                    // actor created the directory. Resume without deletion authority.
                    a.identity = Some(fs::identity(&fs::open_dir(&a.path)?.metadata()?));
                    a.owned = false;
                    a.keep = true;
                    a.hold = Some("review".into());
                    a.state = if live { "active" } else { "retained" }.into();
                }
            } else if let Ok(dir) = self.verify(&a) {
                a.files = fs::manifest(&dir, 100_000)?;
                a.inspected_at = Some(now());
                a.state = "retained".into();
                a.hold = if a.owned { None } else { Some("review".into()) };
            } else {
                a.hold = Some("review: interrupted writer retirement".into());
            }
            if !live {
                a.job = None;
                a.keep = true;
                a.hold = Some("review: allocation has no recovered job".into());
            }
            a.revision += 1;
            a.updated_at = now();
            save_artifact(&self.db.lock().unwrap(), &a)?;
        }
        Ok(())
    }

    pub fn prepare_forget(&self, job: u32) -> Result<Option<Artifact>> {
        let _guard = self.mutation_guard()?;
        let Some(mut a) = self.for_job(job)? else {
            return Ok(None);
        };
        if a.state == "retiring" {
            return Ok(Some(a));
        }
        if a.state != "active" {
            return Ok(None);
        }
        a.state = "retiring".into();
        a.hold = Some("waiting for writer stop".into());
        a.revision += 1;
        a.updated_at = now();
        save_artifact(&self.db.lock().unwrap(), &a)?;
        Ok(Some(a))
    }

    pub fn reconcile_missing(&self) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let raws = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare("SELECT data FROM artifacts WHERE state IN ('completed','retained','unknown','retiring') AND id>?1 ORDER BY id LIMIT 1000")?;
            let cursor = self.reconcile_cursor.lock().unwrap().clone();
            let rows = stmt.query_map([cursor], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        if raws.is_empty() {
            self.reconcile_cursor.lock().unwrap().clear();
        }
        for raw in raws {
            let mut a: Artifact = serde_json::from_str(&raw)?;
            *self.reconcile_cursor.lock().unwrap() = a.id.clone();
            if self.verify_root(&a).is_err() {
                continue;
            }
            match std::fs::symlink_metadata(&a.path) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    a.state = "source_gone".into();
                    a.updated_at = now();
                    a.revision += 1;
                    save_artifact(&self.db.lock().unwrap(), &a)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn protect_roots(&self, roots: &[PathBuf]) -> Result<()> {
        let _guard = self.mutation_guard()?;
        self.db.lock().unwrap().execute("INSERT INTO meta VALUES('protected_roots',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",[serde_json::to_string(roots)?])?;
        Ok(())
    }
    fn verify_root(&self, a: &Artifact) -> Result<File> {
        let raw: Option<String> = self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT value FROM meta WHERE key='protected_roots'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(raw) = raw {
            let roots: Vec<PathBuf> = serde_json::from_str(&raw)?;
            if roots
                .iter()
                .any(|root| root == &a.path || root.starts_with(&a.path))
            {
                return Err(Error::Conflict(
                    "payload overlaps a configured directory role".into(),
                ));
            }
        }
        let root = fs::open_dir(&a.root)?;
        if !a
            .root_identity
            .same_object(&fs::identity(&root.metadata()?))
        {
            return Err(Error::Conflict(
                "configured root identity changed or volume is unavailable".into(),
            ));
        }
        Ok(root)
    }
    fn verify(&self, a: &Artifact) -> Result<File> {
        self.verify_root(a)?;
        let file = fs::open_dir(&a.path)?;
        let observed = fs::identity(&file.metadata()?);
        if !a
            .identity
            .as_ref()
            .is_some_and(|i| i.same_object(&observed))
        {
            return Err(Error::Conflict(
                "payload identity changed; review required".into(),
            ));
        }
        if a.owned {
            let marker = self
                .state_dir
                .join("artifact-identities")
                .join(&a.id)
                .join(format!("{}.json", a.generation));
            let data: serde_json::Value = serde_json::from_slice(&std::fs::read(marker)?)?;
            if data["installation"] != self.installation
                || data["artifact"] != a.id
                || data["generation"] != a.generation
                || data["path"] != a.path.to_string_lossy().as_ref()
                || data["identity"] != serde_json::to_value(&a.identity)?
            {
                return Err(Error::Conflict(
                    "external allocation identity does not match inventory".into(),
                ));
            }
        }
        Ok(file)
    }
    /// Called at a writer/PP quiescence boundary, before publishing History.
    pub fn finish(&self, job: u32, path: &Path, root: &Path, state: &str) -> Result<Artifact> {
        let _guard = self.mutation_guard()?;
        let a = self.for_job(job)?.ok_or(Error::NotFound)?;
        self.finish_artifact_unlocked(a, path, root, state)
    }
    pub(super) fn finish_artifact_unlocked(
        &self,
        mut a: Artifact,
        path: &Path,
        root: &Path,
        state: &str,
    ) -> Result<Artifact> {
        // History may record a bounded PP failure while its independent
        // deletion journal keeps retrying. Do not erase that pending authority.
        if matches!(a.state.as_str(), "deleting" | "delete_failed")
            && state == "retained"
            && a.path == path
            && a.root == root
        {
            return Ok(a);
        }
        if a.path == path && a.state == state {
            self.verify(&a)?;
            return Ok(a);
        }
        if !matches!(
            a.state.as_str(),
            "active" | "transitioning" | "retiring" | "retained"
        ) {
            return Err(Error::Conflict("allocation is not active".into()));
        }
        if a.path != path || a.root != root {
            return Err(Error::Conflict(
                "finalization requires a committed relocation".into(),
            ));
        }
        let dir = self.verify(&a)?;
        let root_file = fs::open_dir(root)?;
        a.path = path.into();
        a.root = root.into();
        a.root_identity = fs::identity(&root_file.metadata()?);
        a.identity = Some(fs::identity(&dir.metadata()?));
        a.files = fs::manifest(&dir, 100_000)?;
        a.inspected_at = Some(now());
        if a.state == "retiring" {
            a.hold = if a.owned { None } else { Some("review".into()) };
        }
        a.state = state.into();
        a.revision += 1;
        a.updated_at = now();
        if state == "parked_failed" && a.owned {
            a.retention_seconds = u64::from(self.settings()?.failed_retention_days) * 86400;
            a.deadline = Some(now().saturating_add(a.retention_seconds as i64));
            a.eligible_seconds = 0;
        }
        self.sidecar(&a)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &a)?;
        event(&tx, &a.id, "finalized", state)?;
        tx.commit()?;
        Ok(a)
    }
    /// Capture an unknown directory without granting deletion authority.
    pub fn discover(&self, root: &Path, path: &Path, active: bool) -> Result<Artifact> {
        let _guard = self.mutation_guard()?;
        self.discover_record(root, path, active)
    }
    fn discover_unlocked(&self, root: &Path, path: &Path) -> Result<Artifact> {
        self.discover_record(root, path, false)
    }
    fn discover_record(&self, root: &Path, path: &Path, active: bool) -> Result<Artifact> {
        fs::absolute(path)?;
        if path.parent() != Some(root) {
            return Err(Error::Conflict(
                "discovery must be an immediate child".into(),
            ));
        }
        let db = self.db.lock().unwrap();
        let existing: Option<String> = db.query_row("SELECT data FROM artifacts WHERE path=?1 AND state NOT IN ('deleted','source_gone') LIMIT 1", [path.to_string_lossy()], |r| r.get(0)).optional()?;
        if let Some(raw) = existing {
            return Ok(serde_json::from_str(&raw)?);
        }
        let dir = fs::open_dir(path)?;
        let root_file = fs::open_dir(root)?;
        let a = Artifact {
            id: id(&db)?,
            generation: id(&db)?,
            revision: 1,
            job: None,
            path: path.into(),
            root: root.into(),
            root_identity: fs::identity(&root_file.metadata()?),
            identity: Some(fs::identity(&dir.metadata()?)),
            state: if active {
                "active_unverified"
            } else {
                "unknown"
            }
            .into(),
            owned: false,
            keep: true,
            hold: Some("review".into()),
            created_at: now(),
            updated_at: now(),
            retention_seconds: 0,
            deadline: None,
            eligible_seconds: 0,
            files: Vec::new(),
            error: None,
            inspected_at: None,
        };
        save_artifact(&db, &a)?;
        Ok(a)
    }
    pub fn inspect(&self, key: &str) -> Result<Artifact> {
        let _guard = self.mutation_guard()?;
        let mut a = self.get(key)?;
        let dir = self.verify(&a)?;
        if matches!(
            a.state.as_str(),
            "active" | "active_unverified" | "transitioning" | "retiring" | "deleting"
        ) {
            return Err(Error::Conflict(
                "writers must finish before inspection".into(),
            ));
        }
        let files = fs::manifest(&dir, 100_000)?;
        if a.owned && files != a.files {
            a.owned = false;
            a.keep = true;
            a.hold = Some("review: files changed; adoption required".into());
        }
        a.files = files;
        a.inspected_at = Some(now());
        a.error = None;
        a.updated_at = now();
        a.revision += 1;
        save_artifact(&self.db.lock().unwrap(), &a)?;
        Ok(a)
    }
    /// Record why an automatic walk could not measure a folder. The record
    /// stays; the operator sees the reason on the row instead of a zero.
    pub fn note_error(&self, key: &str, message: &str) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let mut a = self.get(key)?;
        a.error = Some(message.into());
        a.updated_at = now();
        save_artifact(&self.db.lock().unwrap(), &a)
    }
    pub fn adopt(&self, key: &str, revision: u64) -> Result<Artifact> {
        if !cfg!(unix) {
            return Err(Error::Conflict(
                "checked destructive ownership requires descriptor-relative filesystem support"
                    .into(),
            ));
        }
        let _guard = self.mutation_guard()?;
        let mut a = self.get(key)?;
        if a.revision != revision
            || a.owned
            || !matches!(
                a.state.as_str(),
                "unknown" | "retained" | "parked_failed" | "completed"
            )
        {
            return Err(Error::Conflict(
                "stale preview or artifact is not unknown".into(),
            ));
        }
        let dir = self.verify(&a)?;
        let files = fs::manifest(&dir, 100_000)?;
        if files != a.files {
            return Err(Error::Conflict(
                "files changed; inspect before adopting".into(),
            ));
        }
        a.owned = true;
        a.keep = true;
        a.hold = None;
        a.state = "retained".into();
        a.revision += 1;
        self.sidecar(&a)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &a)?;
        event(&tx, key, "adopted", "kept indefinitely")?;
        tx.commit()?;
        Ok(a)
    }
    pub fn retention(
        &self,
        key: &str,
        revision: u64,
        keep: bool,
        seconds: Option<u64>,
    ) -> Result<Artifact> {
        let _guard = self.mutation_guard()?;
        let mut a = self.get(key)?;
        if a.revision != revision || matches!(a.state.as_str(), "deleting" | "deleted") {
            return Err(Error::Conflict(
                "stale revision or deletion already started".into(),
            ));
        }
        a.keep = keep;
        if let Some(seconds) = seconds {
            if seconds > 315_360_000 {
                return Err(Error::Conflict("retention exceeds 3650 days".into()));
            }
            a.retention_seconds = seconds;
        }
        a.eligible_seconds = 0;
        a.deadline = Some(now().saturating_add(a.retention_seconds as i64));
        a.revision += 1;
        a.updated_at = now();
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        if keep {
            let mut stmt =
                tx.prepare("SELECT data FROM operations WHERE artifact=?1 AND state='queued'")?;
            let ops = stmt
                .query_map([key], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            drop(stmt);
            for raw in ops {
                let mut op: Operation = serde_json::from_str(&raw)?;
                op.state = "cancelled".into();
                save_operation(&tx, &op)?;
            }
        }
        save_artifact(&tx, &a)?;
        event(
            &tx,
            key,
            "retention",
            if keep {
                "Keep"
            } else {
                "fresh retention period"
            },
        )?;
        tx.commit()?;
        Ok(a)
    }
    pub fn release_review(&self, key: &str, revision: u64) -> Result<Artifact> {
        let _guard = self.mutation_guard()?;
        let mut a = self.get(key)?;
        if a.revision != revision
            || !a
                .hold
                .as_deref()
                .is_some_and(|h| h.starts_with("review") || h.starts_with("restored backup"))
        {
            return Err(Error::Conflict(
                "stale revision or hold is owned by an active operation".into(),
            ));
        }
        let dir = self.verify(&a)?;
        if fs::manifest(&dir, 100_000)? != a.files {
            return Err(Error::Conflict(
                "files changed; inspect before releasing review hold".into(),
            ));
        }
        let pending:bool=self.db.lock().unwrap().query_row("SELECT EXISTS(SELECT 1 FROM recoveries WHERE artifact=?1 AND state NOT IN ('imported','cancelled','partial'))",[key],|r|r.get(0))?;
        if pending {
            return Err(Error::Conflict(
                "recovery is still active or ambiguous".into(),
            ));
        }
        a.hold = None;
        a.eligible_seconds = 0;
        a.deadline = Some(now().saturating_add(a.retention_seconds as i64));
        a.revision += 1;
        save_artifact(&self.db.lock().unwrap(), &a)?;
        Ok(a)
    }

    pub fn operation(&self, key: &str) -> Result<Operation> {
        read(&self.db.lock().unwrap(), "operations", key)
    }
    pub fn request_delete(
        &self,
        key: &str,
        revision: u64,
        request_id: &str,
        undo_seconds: u64,
    ) -> Result<Operation> {
        self.request_delete_authorized(key, revision, request_id, undo_seconds, false)
    }
    fn request_delete_authorized(
        &self,
        key: &str,
        revision: u64,
        request_id: &str,
        undo_seconds: u64,
        automatic: bool,
    ) -> Result<Operation> {
        self.request_delete_with_use(key, revision, request_id, undo_seconds, automatic, None)
    }
    fn request_delete_with_use(
        &self,
        key: &str,
        revision: u64,
        request_id: &str,
        undo_seconds: u64,
        automatic: bool,
        authority: Option<(&AttemptUse, &dyn Fn() -> std::io::Result<()>)>,
    ) -> Result<Operation> {
        let _guard = self.mutation_guard()?;
        if let Some((_, checkpoint)) = authority {
            checkpoint()?;
        }
        let use_guard = authority.map(|(guard, _)| guard);
        if request_id.is_empty() || request_id.len() > 128 || undo_seconds > 60 {
            return Err(Error::Conflict(
                "valid idempotency key and undo of 0–60 seconds required".into(),
            ));
        }
        let a = self.get(key)?;
        let request =
            serde_json::to_string(&(key, revision, undo_seconds, automatic, &a.generation))?;
        match self.operation(request_id) {
            Ok(op) => {
                let previous: DeleteRequest = serde_json::from_str(&op.request)?;
                let (id, rev, undo, auto, generation) = previous.fields();
                return if (id, rev, undo, auto) == (key, revision, undo_seconds, automatic)
                    && generation.is_none_or(|g| g == a.generation)
                {
                    Ok(op)
                } else {
                    Err(Error::Conflict(
                        "idempotency key reused with different request".into(),
                    ))
                };
            }
            Err(Error::NotFound) => {}
            Err(e) => return Err(e),
        }
        if a.revision != revision
            || (self.artifact_in_use(&a)? && !self.authorized_processing_use(&a, use_guard))
            || !a.owned
            || a.keep
            || a.hold.is_some()
            || !matches!(
                a.state.as_str(),
                "parked_failed"
                    | "retained"
                    | "completed"
                    | "recovery_imported"
                    | "recovery_staged"
            )
        {
            return Err(Error::Conflict(
                "stale revision, active payload, Keep or recovery/review hold".into(),
            ));
        }
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let pending:Option<String>=tx.query_row("SELECT data FROM operations WHERE artifact=?1 AND state IN ('queued','running','retry') AND json_extract(data,'$.kind')='delete' LIMIT 1",[key],|r|r.get(0)).optional()?;
        if let Some(raw) = pending {
            return Ok(serde_json::from_str(&raw)?);
        }
        let op = Operation {
            id: request_id.into(),
            artifact: key.into(),
            kind: "delete".into(),
            state: "queued".into(),
            request,
            created_at: now(),
            not_before: now() + undo_seconds as i64,
            attempts: 0,
            next_retry: 0,
            error: None,
        };
        save_operation(&tx, &op)?;
        event(&tx, key, "delete_requested", request_id)?;
        tx.commit()?;
        Ok(op)
    }
    pub fn cancel_delete(&self, key: &str) -> Result<Operation> {
        let _guard = self.mutation_guard()?;
        let mut op = self.operation(key)?;
        if op.state != "queued" || !matches!(op.kind.as_str(), "delete" | "prune") {
            return Err(Error::Conflict(
                "deletion already started or finished".into(),
            ));
        }
        op.state = "cancelled".into();
        save_operation(&self.db.lock().unwrap(), &op)?;
        Ok(op)
    }
    pub fn execute_delete(&self, key: &str) -> Result<Operation> {
        self.execute_delete_with_use(key, None)
    }
    fn execute_delete_with_use(
        &self,
        key: &str,
        authority: Option<(&AttemptUse, &dyn Fn() -> std::io::Result<()>)>,
    ) -> Result<Operation> {
        let _guard = self.mutation_guard()?;
        if let Some((_, checkpoint)) = authority {
            checkpoint()?;
        }
        let use_guard = authority.map(|(guard, _)| guard);
        let mut op = self.operation(key)?;
        if op.kind != "delete" {
            return Err(Error::Conflict("not a deletion operation".into()));
        }
        if matches!(op.state.as_str(), "succeeded" | "cancelled" | "review")
            || op.not_before > now()
            || op.next_retry > now()
        {
            return Ok(op);
        }
        let mut a = self.get(&op.artifact)?;
        let request: DeleteRequest = serde_json::from_str(&op.request)?;
        let (_, revision, _, automatic, generation) = request.fields();
        if generation != Some(a.generation.as_str()) {
            op.state = "review".into();
            op.error = Some("deletion generation changed or legacy authorization lacks generation; request deletion again after review".into());
            save_operation(&self.db.lock().unwrap(), &op)?;
            return Ok(op);
        }
        if op.attempts == 0
            && (a.revision != revision
                || (automatic
                    && (!self.settings()?.enabled
                        || !a.eligible()
                        || a.eligible_seconds < a.retention_seconds
                        || !a.deadline.is_some_and(|d| d <= now()))))
        {
            op.state = "cancelled".into();
            op.error = Some("deletion authorization changed; a fresh request is required".into());
            save_operation(&self.db.lock().unwrap(), &op)?;
            return Ok(op);
        }
        if (self.artifact_in_use(&a)? && !self.authorized_processing_use(&a, use_guard))
            || a.keep
            || a.hold.is_some()
            || !a.owned
        {
            return Err(Error::Conflict("payload acquired a hold".into()));
        }
        op.state = "running".into();
        op.attempts += 1;
        a.state = "deleting".into();
        {
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            // Deletion now owns this generation. A failed or interrupted move
            // cannot regain authority on restart, even if deletion needs retry.
            Self::cancel_relocations(&tx, &a.id, &a.generation, "source deletion started")?;
            save_operation(&tx, &op)?;
            save_artifact(&tx, &a)?;
            tx.commit()?;
        }
        let result = authority
            .map_or(Ok(()), |(_, checkpoint)| checkpoint().map_err(Error::from))
            .and_then(|()| self.delete_manifest_checked(&a, op.attempts));
        match result {
            Ok(()) => {
                op.state = "succeeded".into();
                op.error = None;
                a.state = "deleted".into();
                a.error = None;
            }
            Err(e) => {
                let conflict = matches!(e, Error::Conflict(_));
                op.error = Some(e.to_string());
                a.error = op.error.clone();
                op.state = if conflict { "review" } else { "retry" }.into();
                a.state = "delete_failed".into();
                if conflict {
                    a.hold = Some("review".into());
                }
                op.next_retry =
                    now() + [60, 300, 1800, 21600][(op.attempts.saturating_sub(1) as usize).min(3)];
            }
        }
        a.updated_at = now();
        a.revision += 1;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_operation(&tx, &op)?;
        save_artifact(&tx, &a)?;
        event(
            &tx,
            &a.id,
            &op.state,
            op.error.as_deref().unwrap_or("deletion confirmed"),
        )?;
        tx.commit()?;
        Ok(op)
    }
    /// Shared identity/manifest-checked deletion. Call only while holding the
    /// mutation coordinator and after generation-specific authorization.
    fn delete_manifest_checked(&self, a: &Artifact, attempts: u32) -> Result<()> {
        let root = self.verify_root(a)?;
        if attempts > 1 {
            match std::fs::symlink_metadata(&a.path) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(e) => return Err(e.into()),
                Ok(_) => (),
            }
        }
        let dir = self.verify(a)?;
        let observed = fs::manifest(&dir, 100_000)?;
        // A restarted partial deletion may have fewer entries, never more
        // or different ones. Every surviving regular file is revalidated.
        for f in &observed {
            if !a.files.iter().any(|owned| {
                owned.path == f.path
                    && owned.identity.same_object(&f.identity)
                    && (f.identity.directory || owned.identity == f.identity)
            }) {
                return Err(Error::Conflict(format!(
                    "unowned or changed entry: {}",
                    f.path
                )));
            }
        }
        let mut files = observed;
        files.sort_by_key(|f| std::cmp::Reverse(f.path.matches('/').count()));
        for f in &files {
            if !f.identity.directory {
                fs::remove_entry(&dir, f)?;
            }
        }
        for f in &files {
            if f.identity.directory {
                fs::remove_entry(&dir, f)?;
            }
        }
        if !fs::names(&dir)?.is_empty() {
            return Err(Error::Conflict("new files appeared during deletion".into()));
        }
        if a.path.parent() != Some(a.root.as_path()) {
            return Err(Error::Conflict(
                "payload no longer an immediate child of root".into(),
            ));
        }
        // Recheck the entry itself; unlinkat cannot follow a replacement.
        let current = fs::open_at(&root, Path::new(a.path.file_name().unwrap()), true)?;
        if !fs::identity(&current.metadata()?).same_object(a.identity.as_ref().unwrap()) {
            return Err(Error::Conflict("payload directory replaced".into()));
        }
        fs::unlink(&root, Path::new(a.path.file_name().unwrap()), true)?;
        Ok(())
    }

    /// Count only daemon monotonic uptime observed while eligible. Reset the
    /// checkpoint before the transaction: a failed commit loses time safely.
    pub fn tick(&self) -> Result<()> {
        let enabled = self.settings()?.enabled;
        let mut due = Vec::new();
        {
            let _guard = self.mutation_guard()?;
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            let raws = {
                let mut stmt =
                    tx.prepare("SELECT data FROM artifacts WHERE state IN ('parked_failed','recovery_imported')")?;
                let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
                rows.collect::<std::result::Result<Vec<_>, _>>()?
            };
            let mut active_clocks = std::collections::HashSet::new();
            for raw in raws {
                let mut a: Artifact = serde_json::from_str(&raw)?;
                active_clocks.insert(a.id.clone());
                if enabled && a.eligible() {
                    let elapsed = {
                        let mut clocks = self.clocks.lock().unwrap();
                        let point = clocks
                            .entry(a.id.clone())
                            .or_insert((a.revision, Instant::now()));
                        let elapsed = if point.0 == a.revision {
                            point.1.elapsed().as_secs()
                        } else {
                            0
                        };
                        *point = (a.revision, Instant::now());
                        elapsed
                    };
                    a.eligible_seconds = a
                        .eligible_seconds
                        .saturating_add(elapsed)
                        .min(a.retention_seconds);
                    save_artifact(&tx, &a)?;
                    if a.eligible_seconds >= a.retention_seconds
                        && a.deadline.is_some_and(|d| d <= now())
                    {
                        due.push((a.id.clone(), a.revision));
                    }
                } else {
                    self.clocks.lock().unwrap().remove(&a.id);
                }
            }
            self.clocks
                .lock()
                .unwrap()
                .retain(|key, _| active_clocks.contains(key));
            tx.commit()?;
        }
        for (key, revision) in due {
            if let Err(e) = self.request_delete_authorized(
                &key,
                revision,
                &format!("retention-{key}-{revision}"),
                0,
                true,
            ) {
                tracing::warn!(artifact=%key,error=%e,"expiry changed before admission");
            }
        }
        self.run_due_deletes()?;
        self.reconcile_workspace_retirements()?;
        self.run_workspace_retirements()?;
        self.reconcile_recoveries()?;
        self.schedule_discovery()?;
        self.run_tasks()?;
        self.reconcile_missing()?;
        self.compact()?;
        Ok(())
    }
    /// Execute every delete whose undo window has closed. Part of the
    /// maintenance tick, and kicked by the API once a requested delete's
    /// window expires so the files go when the toast said they would.
    pub fn run_due_deletes(&self) -> Result<()> {
        // No runner lock: execute_delete is serialized by the mutation guard
        // and idempotent, and a delete must not queue behind a scan drain.
        let pending = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare(
                "SELECT id FROM operations WHERE state IN ('queued','running','retry') AND json_extract(data,'$.kind')='delete' AND json_extract(data,'$.not_before')<=unixepoch() AND json_extract(data,'$.next_retry')<=unixepoch() ORDER BY json_extract(data,'$.next_retry'),json_extract(data,'$.created_at') LIMIT 25",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for key in pending {
            if let Err(e) = self.execute_delete(&key) {
                tracing::warn!(operation=%key,error=%e,"artifact deletion remains pending");
            }
        }
        Ok(())
    }
    pub fn compact(&self) -> Result<usize> {
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let rows = {
            let mut stmt = tx.prepare("SELECT data FROM artifacts WHERE state IN ('deleted','source_gone') AND updated_at<?1 AND json_array_length(json_extract(data,'$.files'))>0 LIMIT 100")?;
            let rows = stmt.query_map([now() - 90 * 86400], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let count = rows.len();
        for raw in rows {
            let mut a: Artifact = serde_json::from_str(&raw)?;
            // No live operation consults sidecars for terminal payloads. Keep
            // the small SQL identity/idempotency record, retire bulky manifests
            // and internal sidecar scratch after the diagnostic window.
            let identities = self.state_dir.join("artifact-identities");
            let directory = identities.join(&a.id);
            if directory.try_exists()? {
                let dir = fs::open_dir(&directory)?;
                for entry in fs::manifest(&dir, 100)? {
                    if entry.identity.directory
                        || !(entry.path.ends_with(".json") || entry.path.ends_with(".tmp"))
                    {
                        return Err(Error::Conflict(
                            "unexpected allocation sidecar entry".into(),
                        ));
                    }
                    fs::remove_entry(&dir, &entry)?;
                }
                fs::unlink(&fs::open_dir(&identities)?, Path::new(&a.id), true)?;
            }
            a.files.clear();
            // The manifest goes; the summary stays so the Cleared view still
            // knows how big the payload was.
            tx.execute(
                "UPDATE artifacts SET data=?2, updated_at=?3 WHERE id=?1",
                params![a.id, serde_json::to_string(&a)?, a.updated_at],
            )?;
        }
        tx.execute(
            "DELETE FROM events WHERE seq IN (SELECT seq FROM events WHERE at<?1 LIMIT 1000)",
            [now() - 90 * 86400],
        )?;
        tx.commit()?;
        Ok(count)
    }

    fn register_publication(&self, recovery: &Recovery) -> Result<()> {
        if let Ok(existing) = self.get(&format!("recovery-{}", recovery.id)) {
            self.verify(&existing)?;
            return Ok(());
        }
        let root = recovery
            .published
            .parent()
            .ok_or_else(|| Error::Conflict("invalid publication root".into()))?;
        let dir = fs::open_dir(&recovery.published)?;
        let observed = fs::identity(&dir.metadata()?);
        if !recovery
            .publication_identity
            .as_ref()
            .or(recovery.scratch_identity.as_ref())
            .is_some_and(|expected| expected.same_object(&observed))
        {
            return Err(Error::Conflict(
                "recovery publication identity changed".into(),
            ));
        }
        let root_dir = fs::open_dir(root)?;
        let a = Artifact {
            id: format!("recovery-{}", recovery.id),
            generation: recovery.generation.clone(),
            revision: 1,
            job: None,
            path: recovery.published.clone(),
            root: root.into(),
            root_identity: fs::identity(&root_dir.metadata()?),
            identity: Some(fs::identity(&dir.metadata()?)),
            state: "recovery_staged".into(),
            owned: true,
            keep: false,
            hold: Some("awaiting complete import receipt".into()),
            created_at: now(),
            updated_at: now(),
            retention_seconds: 86400,
            deadline: None,
            eligible_seconds: 0,
            files: fs::manifest(&dir, 100_000)?,
            error: None,
            inspected_at: Some(now()),
        };
        self.sidecar(&a)?;
        save_artifact(&self.db.lock().unwrap(), &a)
    }

    /// Explicit restore quarantine; run before serving requests from a restored
    /// backup. Old retention and deletion intents cannot become active again.
    pub fn quarantine_restore(&self) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let raws = {
            let mut stmt = tx.prepare(
                "SELECT data FROM artifacts WHERE state NOT IN ('deleted','source_gone')",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in raws {
            let mut a: Artifact = serde_json::from_str(&raw)?;
            a.hold = Some("restored backup: review required".into());
            a.keep = true;
            a.eligible_seconds = 0;
            a.deadline = None;
            a.revision += 1;
            save_artifact(&tx, &a)?;
        }
        tx.execute("UPDATE recoveries SET state='review', data=json_set(data,'$.state','review','$.error','Restored backup: reconcile consumer and create a fresh handoff') WHERE state NOT IN ('imported','cancelled')", [])?;
        let ops = {
            let mut stmt = tx.prepare(
                "SELECT data FROM operations WHERE state IN ('queued','running','retry')",
            )?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in ops {
            let mut op: Operation = serde_json::from_str(&raw)?;
            op.state = "review".into();
            op.error = Some("restored backup; old deletion invalidated".into());
            save_operation(&tx, &op)?;
        }
        event(
            &tx,
            "installation",
            "restore",
            "all outstanding operations quarantined",
        )?;
        tx.commit()?;
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests;

#[cfg(test)]
mod relocation_failure_tests;
