//! Reconciliation is owned background work; HTTP reads use the local WAL view.
use super::*;

mod incremental;
use std::collections::HashSet;
#[cfg(test)]
use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, MutexGuard, Weak};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryMode {
    LocalOnly,
    Shared,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct HistorySyncStatus {
    pub mode: HistoryMode,
    pub state: &'static str,
    pub paused: bool,
    pub repair_pending: bool,
    pub last_duration_ms: u64,
    pub last_bytes_read: u64,
    pub last_success_age_secs: Option<u64>,
    pub last_error: Option<String>,
    pub passes: u64,
    pub skipped_unchanged: u64,
    pub max_batch_ms: u64,
    pub last_scan: &'static str,
    pub last_entries_reconciled: u64,
    pub last_files_rebuilt: u64,
    pub last_incomplete_tails: u64,
    pub last_malformed_lines: u64,
    pub index_path: PathBuf,
    pub placement: &'static str,
}

pub(super) struct Seen {
    pub first: i64,
    pub last: i64,
    pub count: i64,
    pub client: Option<String>,
}

struct Progress {
    last_success: Option<Instant>,
    last_attempt: Option<Instant>,
    last_full: Option<Instant>,
    last_duration_ms: u64,
    last_bytes_read: u64,
    last_error: Option<String>,
    passes: u64,
    skipped_unchanged: u64,
    fingerprints: Vec<Fingerprint>,
}

pub(super) struct SyncControl {
    mode: HistoryMode,
    gate: Mutex<()>,
    pause_control: Mutex<()>,
    pub(super) mutation: Mutex<()>,
    pub(super) generation: AtomicU64,
    dirty: AtomicBool,
    paused: AtomicBool,
    stopping: AtomicBool,
    running: AtomicBool,
    worker_started: AtomicBool,
    pub(super) wake: Mutex<Option<std::thread::Thread>>,
    bytes: AtomicU64,
    max_batch_ms: AtomicU64,
    pub(super) pending_seen: Mutex<std::collections::BTreeMap<u32, Seen>>,
    progress: Mutex<Progress>,
    incremental: Mutex<incremental::Cache>,
    last_scan: Mutex<incremental::ScanStats>,
    index_path: PathBuf,
    placement: &'static str,
}

impl SyncControl {
    pub(super) fn new(mode: HistoryMode, paused: bool, path: &Path) -> Self {
        Self {
            mode,
            gate: Mutex::new(()),
            pause_control: Mutex::new(()),
            mutation: Mutex::new(()),
            generation: AtomicU64::new(0),
            dirty: AtomicBool::new(false),
            paused: AtomicBool::new(paused),
            stopping: AtomicBool::new(false),
            running: AtomicBool::new(false),
            worker_started: AtomicBool::new(false),
            wake: Mutex::new(None),
            bytes: AtomicU64::new(0),
            max_batch_ms: AtomicU64::new(0),
            pending_seen: Mutex::new(Default::default()),
            progress: Mutex::new(Progress {
                last_success: None,
                last_attempt: None,
                last_full: None,
                last_duration_ms: 0,
                last_bytes_read: 0,
                last_error: None,
                passes: 0,
                skipped_unchanged: 0,
                fingerprints: vec![],
            }),
            incremental: Mutex::new(Default::default()),
            last_scan: Mutex::new(Default::default()),
            index_path: path.to_owned(),
            placement: placement(path),
        }
    }

    pub(super) fn mark_opened(&self) {
        let mut p = self.progress.lock().unwrap();
        p.last_success = Some(Instant::now());
        p.last_attempt = p.last_success;
        p.last_bytes_read = self.bytes.swap(0, Ordering::Relaxed);
    }
}

/// Fence both edges of a mutation. A scanner can start while a writer is
/// already inside its critical section; that scanner must fail after the
/// writer finishes, even if it captured the writer's starting generation.
pub(super) struct ReplayFence<'a> {
    db: &'a HistoryDb,
    _lock: MutexGuard<'a, ()>,
}
impl<'a> ReplayFence<'a> {
    pub(super) fn new(db: &'a HistoryDb) -> Self {
        let lock = db.sync.mutation.lock().unwrap();
        db.sync.generation.fetch_add(1, Ordering::SeqCst);
        Self { db, _lock: lock }
    }
}
impl Drop for ReplayFence<'_> {
    fn drop(&mut self) {
        self.db.sync.generation.fetch_add(1, Ordering::SeqCst);
    }
}

/// Every local publication starts dirty. Only complete success clears that
/// operation's repair requirement; an earlier failure remains dirty.
pub(super) struct MutationGuard<'a> {
    db: &'a HistoryDb,
    previous_dirty: bool,
    _fence: ReplayFence<'a>,
}
impl<'a> MutationGuard<'a> {
    pub(super) fn new(db: &'a HistoryDb) -> Self {
        let fence = ReplayFence::new(db);
        let previous_dirty = db.sync.dirty.swap(true, Ordering::SeqCst);
        Self {
            db,
            previous_dirty,
            _fence: fence,
        }
    }
    pub(super) fn commit(self) {
        self.db
            .sync
            .dirty
            .store(self.previous_dirty, Ordering::SeqCst);
    }
}

/// Dropping the runtime-owned guard requests a cooperative stop. A stalled
/// filesystem syscall never makes Tokio runtime teardown wait for this thread.
pub struct HistoryWorker {
    db: Weak<HistoryDb>,
    thread: std::thread::Thread,
}
impl HistoryWorker {
    pub fn stop(&self) {
        if let Some(db) = self.db.upgrade() {
            db.sync.stopping.store(true, Ordering::SeqCst);
        }
        self.thread.unpark();
    }
}
impl Drop for HistoryWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

impl HistoryDb {
    pub fn start_worker(self: &Arc<Self>) -> Result<HistoryWorker, StateError> {
        if self.sync.worker_started.swap(true, Ordering::SeqCst) {
            return Err(StateError::Corrupt("history worker already started".into()));
        }
        let weak = Arc::downgrade(self);
        let worker_db = weak.clone();
        let handle = std::thread::Builder::new().name("history-sync".into()).spawn(move || {
            loop {
                std::thread::park_timeout(Duration::from_secs(5));
                let Some(db) = worker_db.upgrade() else { break };
                if db.sync.stopping.load(Ordering::SeqCst) { break; }
                if let Err(e) = db.flush_seen() { tracing::warn!(error = %e, "history observations await retry"); }
                if let Err(e) = db.refresh() {
                    if !db.sync.paused.load(Ordering::SeqCst) && !db.sync.stopping.load(Ordering::SeqCst) {
                        tracing::warn!(error = %e, "history synchronization failed; retaining indexed view");
                    }
                }
            }
        }).map_err(|e| {
            self.sync.worker_started.store(false, Ordering::SeqCst);
            StateError::Corrupt(format!("start history worker: {e}"))
        })?;
        *self.sync.wake.lock().unwrap() = Some(handle.thread().clone());
        handle.thread().unpark();
        Ok(HistoryWorker {
            db: weak,
            thread: handle.thread().clone(),
        })
    }

    /// Admission can wait here; listings never call this. The gate covers the
    /// entire pass, and the interval starts when the preceding attempt ends.
    pub fn refresh(&self) -> Result<(), StateError> {
        let _gate = self.sync.gate.lock().unwrap();
        if self.sync.paused.load(Ordering::SeqCst) || self.sync.stopping.load(Ordering::SeqCst) {
            return Err(StateError::Corrupt(
                "history synchronization is paused or stopping".into(),
            ));
        }
        if self.sync.mode == HistoryMode::LocalOnly && !self.sync.dirty.load(Ordering::SeqCst) {
            // A successful local writer can clear the transient dirty flag after
            // interrupting replay. No repair remains, so retire the old failure
            // without claiming that another reconciliation actually ran.
            self.sync.progress.lock().unwrap().last_error = None;
            return Ok(());
        }
        if self
            .sync
            .progress
            .lock()
            .unwrap()
            .last_attempt
            .is_some_and(|t| t.elapsed() < Duration::from_secs(5))
        {
            return Ok(());
        }
        self.sync.running.store(true, Ordering::SeqCst);
        self.sync.bytes.store(0, Ordering::Relaxed);
        let start = Instant::now();
        let result = self.replay_logs(true);
        let mut progress = self.sync.progress.lock().unwrap();
        progress.last_attempt = Some(Instant::now());
        progress.last_duration_ms = start.elapsed().as_millis() as u64;
        progress.last_bytes_read = self.sync.bytes.load(Ordering::Relaxed);
        progress.last_error = result.as_ref().err().map(ToString::to_string);
        if result.is_ok() {
            progress.last_success = progress.last_attempt;
        }
        self.sync.running.store(false, Ordering::SeqCst);
        result
    }

    pub fn sync_status(&self) -> HistorySyncStatus {
        let p = self.sync.progress.lock().unwrap();
        let scan = self.sync.last_scan.lock().unwrap();
        let paused = self.sync.paused.load(Ordering::SeqCst);
        let running = self.sync.running.load(Ordering::SeqCst);
        let stopping = self.sync.stopping.load(Ordering::SeqCst);
        HistorySyncStatus {
            mode: self.sync.mode,
            state: if running && (paused || stopping) {
                "stopping"
            } else if running {
                "running"
            } else if paused {
                "paused"
            } else if stopping {
                "stopped"
            } else if p.last_error.is_some() {
                "failed"
            } else {
                "idle"
            },
            paused,
            repair_pending: self.sync.dirty.load(Ordering::SeqCst),
            last_duration_ms: p.last_duration_ms,
            last_bytes_read: p.last_bytes_read,
            last_success_age_secs: p.last_success.map(|t| t.elapsed().as_secs()),
            last_error: p.last_error.clone(),
            passes: p.passes,
            skipped_unchanged: p.skipped_unchanged,
            max_batch_ms: self.sync.max_batch_ms.load(Ordering::Relaxed),
            last_scan: scan.kind,
            last_entries_reconciled: scan.entries,
            last_files_rebuilt: scan.rebuilt,
            last_incomplete_tails: scan.incomplete,
            last_malformed_lines: scan.malformed,
            index_path: self.sync.index_path.clone(),
            placement: self.sync.placement,
        }
    }

    pub fn set_sync_paused(&self, paused: bool) -> Result<(), StateError> {
        let _control = self.sync.pause_control.lock().unwrap();
        // Persist before acknowledging. Set the atomic first for cooperative
        // cancellation while waiting for a replay batch's writer connection.
        let previous = self.sync.paused.swap(true, Ordering::SeqCst);
        let result = self.conn.lock().unwrap().execute(
            "INSERT INTO meta(key,value) VALUES('history_sync_paused',?1)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [i64::from(paused)],
        );
        match result {
            Ok(_) => {
                self.sync.paused.store(paused, Ordering::SeqCst);
                if !paused {
                    self.sync.progress.lock().unwrap().last_attempt = None;
                }
                Ok(())
            }
            Err(e) => {
                self.sync.paused.store(previous, Ordering::SeqCst);
                Err(StateError::Corrupt(format!("history pause: {e}")))
            }
        }
    }

    fn replay_checkpoint(&self, quiet: bool, generation: u64) -> Result<(), StateError> {
        if quiet
            && (self.sync.paused.load(Ordering::SeqCst)
                || self.sync.stopping.load(Ordering::SeqCst))
        {
            return Err(StateError::Corrupt(
                "history synchronization interrupted".into(),
            ));
        }
        if self.sync.generation.load(Ordering::SeqCst) != generation {
            return Err(StateError::Corrupt(
                "history changed during reconciliation; retrying".into(),
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    fn full_replay_logs(&self, quiet: bool) -> Result<(), StateError> {
        let Some(dir) = self.jsonl.as_ref().and_then(|p| p.parent()) else {
            return Ok(());
        };
        let generation = self.sync.generation.load(Ordering::SeqCst);
        let paths = log_paths(dir)?;
        let mut snapshots = Vec::new();
        for path in paths {
            self.replay_checkpoint(quiet, generation)?;
            let file = fsx::open(&path)?;
            let meta = fsx::ctx(file.metadata(), "inspect history log", &path)?;
            snapshots.push((Fingerprint::new(path, &meta), file));
        }
        let fingerprints: Vec<_> = snapshots.iter().map(|(f, _)| f.clone()).collect();
        {
            let mut p = self.sync.progress.lock().unwrap();
            if quiet
                && !self.sync.dirty.load(Ordering::SeqCst)
                && p.fingerprints == fingerprints
                && p.last_full
                    .is_some_and(|t| t.elapsed() < Duration::from_secs(60))
            {
                p.skipped_unchanged += 1;
                return Ok(());
            }
            p.passes += 1;
        }
        let floor = self.ingest_floor()?;
        let mut tombstones = HashSet::new();
        for (fp, file) in &mut snapshots {
            for line in BufReader::new(file.take(fp.len)).split(b'\n') {
                let line = fsx::ctx(line, "read", &fp.path)?;
                self.sync
                    .bytes
                    .fetch_add(line.len() as u64 + 1, Ordering::Relaxed);
                self.replay_checkpoint(quiet, generation)?;
                if serde_json::from_slice::<HistoryMutationProbe<'_>>(&line)
                    .ok()
                    .and_then(|p| p.op)
                    == Some("tombstone")
                {
                    if let Ok(HistoryMutation::Tombstone {
                        job,
                        completed_at_unix,
                    }) = serde_json::from_slice(&line)
                    {
                        tombstones.insert((job.0, completed_at_unix));
                    }
                }
            }
        }
        self.replay_tombstones(&tombstones, quiet, generation)?;
        let mut batch = Vec::new();
        let mut batch_bytes = 0;
        for (fp, file) in &mut snapshots {
            fsx::ctx(file.seek(SeekFrom::Start(0)), "seek history log", &fp.path)?;
            for line in BufReader::new(file.take(fp.len)).split(b'\n') {
                let line = fsx::ctx(line, "read", &fp.path)?;
                self.sync
                    .bytes
                    .fetch_add(line.len() as u64 + 1, Ordering::Relaxed);
                self.replay_checkpoint(quiet, generation)?;
                if let Ok(entry) = serde_json::from_slice::<HistoryEntry>(&line) {
                    if entry.completed_at_unix >= floor
                        && !tombstones.contains(&(entry.job.0, entry.completed_at_unix))
                    {
                        batch_bytes += line.len();
                        batch.push(entry);
                    }
                }
                if batch.len() >= 16 || batch_bytes >= 128 * 1024 {
                    self.replay_batch(&batch, quiet, generation)?;
                    batch.clear();
                    batch_bytes = 0;
                }
            }
        }
        self.replay_batch(&batch, quiet, generation)?;
        let _mutation = self.sync.mutation.lock().unwrap();
        self.replay_checkpoint(quiet, generation)?;
        // Cache only the snapshots read, not a newer pathname's metadata.
        let mut p = self.sync.progress.lock().unwrap();
        p.fingerprints = fingerprints;
        p.last_full = Some(Instant::now());
        self.sync.dirty.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn replay_tombstones(
        &self,
        tombstones: &HashSet<HistoryKey>,
        quiet: bool,
        generation: u64,
    ) -> Result<(), StateError> {
        if tombstones.is_empty() {
            return Ok(());
        }
        // Probe only incoming keys in bounded writer transactions. Reading the
        // lifetime tombstone table here would make one appended deletion O(all
        // prior deletions) and contend with history pages' reader connection.
        let pending: Vec<_> = tombstones.iter().copied().collect();
        for batch in pending.chunks(16) {
            self.replay_tombstone_batch(batch, quiet, generation)?;
        }
        Ok(())
    }

    fn replay_tombstone_batch(
        &self,
        keys: &[HistoryKey],
        quiet: bool,
        generation: u64,
    ) -> Result<(), StateError> {
        let _mutation = self.sync.mutation.lock().unwrap();
        self.replay_checkpoint(quiet, generation)?;
        let mut conn = self.conn.lock().unwrap();
        let started = Instant::now();
        let tx = conn.transaction().map_err(sql_error)?;
        for (job, at) in keys {
            self.replay_checkpoint(quiet, generation)?;
            let inserted = tx
                .prepare_cached(
                    "INSERT OR IGNORE INTO history_tombstones(job_id,completed_at) VALUES(?1,?2)",
                )
                .map_err(sql_error)?
                .execute(rusqlite::params![job, at])
                .map_err(sql_error)?;
            if inserted != 0 {
                tx.prepare_cached("DELETE FROM history WHERE job_id=?1 AND completed_at=?2")
                    .map_err(sql_error)?
                    .execute(rusqlite::params![job, at])
                    .map_err(sql_error)?;
            }
        }
        tx.commit().map_err(sql_error)?;
        self.sync
            .max_batch_ms
            .fetch_max(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        Ok(())
    }

    fn replay_batch(
        &self,
        entries: &[HistoryEntry],
        quiet: bool,
        generation: u64,
    ) -> Result<(), StateError> {
        if entries.is_empty() {
            return Ok(());
        }
        let _mutation = self.sync.mutation.lock().unwrap();
        self.replay_checkpoint(quiet, generation)?;
        let mut conn = self.conn.lock().unwrap();
        let started = Instant::now();
        let tx = conn.transaction().map_err(sql_error)?;
        for entry in entries {
            self.replay_checkpoint(quiet, generation)?;
            replay_entry(&tx, entry)?;
        }
        tx.commit().map_err(sql_error)?;
        self.sync
            .max_batch_ms
            .fetch_max(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        Ok(())
    }
}

fn sql_error(e: rusqlite::Error) -> StateError {
    StateError::Corrupt(format!("history replay: {e}"))
}

/// Avoid attempted INSERTs on known keys: even a skipped UPSERT update can
/// advance AUTOINCREMENT. Existing rows retain their IDs and observation data.
fn replay_entry(conn: &Connection, e: &HistoryEntry) -> Result<(), StateError> {
    let record = e.record.as_ref().map(serde_json::to_string).transpose()?;
    let exists = conn
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM history WHERE job_id=?1 AND completed_at=?2)")
        .map_err(sql_error)?
        .query_row(rusqlite::params![e.job.0, e.completed_at_unix], |r| {
            r.get::<_, bool>(0)
        })
        .map_err(sql_error)?;
    if exists {
        conn.prepare_cached("UPDATE history SET hidden=?3,
            removed_at=COALESCE(?4,removed_at), picked_up_by=COALESCE(?5,picked_up_by), record=COALESCE(?6,record)
            WHERE job_id=?1 AND completed_at=?2 AND
            (hidden IS NOT ?3 OR removed_at IS NOT COALESCE(?4,removed_at)
             OR picked_up_by IS NOT COALESCE(?5,picked_up_by) OR record IS NOT COALESCE(?6,record))")
            .map_err(sql_error)?.execute(rusqlite::params![e.job.0,e.completed_at_unix,e.hidden,e.removed_at_unix,e.picked_up_by,record]).map_err(sql_error)?;
    } else {
        conn.prepare_cached("INSERT INTO history(job_id,name,category,final_dir,status,size,health,params,dupe_key,dupe_score,completed_at,hidden,removed_at,picked_up_by,stages,record)
            SELECT ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16
            WHERE NOT EXISTS(SELECT 1 FROM history_tombstones WHERE job_id=?1 AND completed_at=?11)")
            .map_err(sql_error)?.execute(rusqlite::params![e.job.0,e.name,e.category,e.final_dir,e.status,e.size as i64,e.health,
                serde_json::to_string(&e.params)?,e.dupe_key,e.dupe_score,e.completed_at_unix,e.hidden,e.removed_at_unix,e.picked_up_by,
                serde_json::to_string(&e.stages)?,record]).map_err(sql_error)?;
    }
    Ok(())
}

#[derive(Clone, PartialEq, Eq)]
struct Fingerprint {
    path: PathBuf,
    len: u64,
    modified: Option<std::time::SystemTime>,
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
}
impl Fingerprint {
    fn new(path: PathBuf, m: &std::fs::Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Self {
            path,
            len: m.len(),
            modified: m.modified().ok(),
            #[cfg(unix)]
            dev: m.dev(),
            #[cfg(unix)]
            ino: m.ino(),
        }
    }
}
fn log_paths(dir: &Path) -> Result<Vec<PathBuf>, StateError> {
    let entries = match fsx::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.is_not_found() => return Ok(vec![]),
        Err(e) => return Err(e),
    };
    let mut paths = vec![];
    for e in entries {
        let path = fsx::ctx(e, "list history", dir)?.path();
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        if name.starts_with("history") && name.ends_with(".jsonl") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

pub(super) fn writer_lock(
    dir: Option<&Path>,
    tag: Option<&str>,
    mode: HistoryMode,
) -> Result<Vec<std::fs::File>, StateError> {
    if dir.is_none() && mode == HistoryMode::Shared && tag.is_none() {
        return Ok(vec![]);
    }
    if mode == HistoryMode::LocalOnly && tag.is_some() {
        return Err(StateError::Corrupt(
            "local-only history cannot have a peer writer tag".into(),
        ));
    }
    let dir = dir.ok_or_else(|| {
        StateError::Corrupt("local-only history requires its portable directory".into())
    })?;
    fsx::create_dir_all(dir)?;
    // The common authority lock is held before observing the directory, so
    // even a peer that has not appended yet excludes the local-only fast path.
    let authority_path = dir.join(".history-authority.lock");
    let authority = fsx::open_append(&authority_path)?;
    let result = if mode == HistoryMode::LocalOnly {
        authority.try_lock()
    } else {
        authority.try_lock_shared()
    };
    result.map_err(|e| {
        StateError::Corrupt(format!(
            "history directory {} has incompatible ownership: {e}",
            dir.display()
        ))
    })?;
    let mut locks = vec![authority];
    if mode == HistoryMode::Shared && tag.is_none() {
        return Ok(locks);
    }
    if mode == HistoryMode::LocalOnly
        && log_paths(dir)?
            .iter()
            .any(|p| p.file_name().is_some_and(|n| n != "history.jsonl"))
    {
        return Err(StateError::Corrupt(
            "local-only history contains peer logs; use shared history mode".into(),
        ));
    }
    let path = dir.join(
        tag.map(|t| format!(".history-{t}-writer.lock"))
            .unwrap_or_else(|| ".history-writer.lock".into()),
    );
    let file = fsx::open_append(&path)?;
    file.try_lock().map_err(|e| {
        StateError::Corrupt(format!(
            "history writer already owns {}: {e}",
            path.display()
        ))
    })?;
    locks.push(file);
    Ok(locks)
}

fn placement(path: &Path) -> &'static str {
    #[cfg(target_os = "linux")]
    {
        let Ok(path) = fsx::canonicalize(path) else {
            return "unknown";
        };
        let Ok(table) = fsx::read_to_string(Path::new("/proc/self/mountinfo")) else {
            return "unknown";
        };
        let mut best = (0, "unknown");
        for line in table.lines() {
            let Some((left, right)) = line.split_once(" - ") else {
                continue;
            };
            let Some(mount) = left.split_whitespace().nth(4) else {
                continue;
            };
            let mount = mount
                .replace("\\040", " ")
                .replace("\\011", "\t")
                .replace("\\134", "\\");
            if path.starts_with(&mount) && mount.len() >= best.0 {
                let kind = right.split_whitespace().next().unwrap_or("");
                best = (
                    mount.len(),
                    match kind {
                        "nfs" | "nfs4" | "cifs" | "smb3" | "ceph" => "network",
                        k if k.starts_with("fuse") => "network_or_fuse",
                        "overlay" | "tmpfs" => "ephemeral_or_overlay",
                        "ext4" | "xfs" | "btrfs" | "zfs" => "local",
                        _ => "unknown",
                    },
                );
            }
        }
        best.1
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        "unknown"
    }
}

fn index_marker(dir: &Path, tag: Option<&str>) -> PathBuf {
    dir.join(
        tag.map(|t| format!(".history-{t}-index-path"))
            .unwrap_or_else(|| ".history-index-path".into()),
    )
}

/// Startup holds the portable writer lock, so a new location cannot race an
/// already running daemon using the same history authority. VACUUM INTO reads
/// committed WAL state and preserves rowids and sqlite_sequence.
pub(super) fn prepare_index(
    target: &Path,
    logs: Option<&Path>,
    tag: Option<&str>,
    spool: Option<&Path>,
) -> Result<(), StateError> {
    let (Some(logs), Some(state)) = (logs, spool.and_then(Path::parent)) else {
        return Ok(());
    };
    let marker = index_marker(logs, tag);
    let source = match fsx::read_to_string(&marker) {
        Ok(path) => PathBuf::from(path),
        Err(e) if e.is_not_found() => state.join("history.sqlite"),
        Err(e) => return Err(e),
    };
    if fsx::exists(&marker)? && !fsx::exists(&source)? {
        return Err(StateError::Corrupt(format!(
            "active history index {} is missing; explicit recovery required",
            source.display()
        )));
    }
    if source == target {
        return Ok(());
    }
    if fsx::exists(target)? {
        if fsx::exists(&source)? && fsx::canonicalize(target)? == fsx::canonicalize(&source)? {
            return Ok(());
        }
        return Err(StateError::Corrupt(format!(
            "refusing to replace existing history index {}; active index is {}",
            target.display(),
            source.display()
        )));
    }
    if !fsx::exists(&source)? {
        if fsx::exists(&marker)? {
            return Err(StateError::Corrupt(format!(
                "active history index {} is missing; explicit recovery required",
                source.display()
            )));
        }
        return Ok(());
    }
    let tmp = target.with_extension("sqlite.migrating");
    if fsx::exists(&tmp)? {
        fsx::remove_file(&tmp)?;
    }
    let source_conn =
        Connection::open_with_flags(&source, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(sql_error)?;
    source_conn
        .execute("VACUUM INTO ?1", [tmp.to_string_lossy().as_ref()])
        .map_err(sql_error)?;
    let file = fsx::open(&tmp)?;
    fsx::sync_data(&file, &tmp)?;
    fsx::rename(&tmp, target)?;
    fsx::sync_parent(target)?;
    tracing::info!(from = %source.display(), to = %target.display(), "history index copied with committed WAL and cursor state");
    Ok(())
}

pub(super) fn record_index_path(
    target: &Path,
    logs: Option<&Path>,
    tag: Option<&str>,
    spool: Option<&Path>,
) -> Result<(), StateError> {
    let Some(logs) = logs.filter(|_| spool.is_some()) else {
        return Ok(());
    };
    let path = index_marker(logs, tag);
    let target = fsx::canonicalize(target)?;
    let tmp = path.with_extension("tmp");
    let mut f = fsx::create(&tmp)?;
    fsx::write_whole(&mut f, target.to_string_lossy().as_bytes(), &tmp)?;
    fsx::sync_data(&f, &tmp)?;
    fsx::rename(&tmp, &path)?;
    fsx::sync_parent(&path)
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) fn entry(job: u32, at: i64) -> HistoryEntry {
        HistoryEntry {
            job: crate::JobId(job),
            name: format!("job-{job}"),
            category: Some("tv".into()),
            final_dir: Some("/dest/x".into()),
            status: "SUCCESS".into(),
            size: 1000,
            health: 1000,
            params: vec![("drone".into(), "abc123".into())],
            dupe_key: String::new(),
            dupe_score: 0,
            completed_at_unix: at,
            hidden: false,
            first_seen_at_unix: None,
            last_seen_at_unix: None,
            seen_count: 0,
            removed_at_unix: None,
            picked_up_by: None,
            record: None,
            stages: Vec::new(),
            seq: 0,
        }
    }

    fn due(db: &HistoryDb) {
        db.sync.progress.lock().unwrap().last_attempt = None;
    }
    fn local(root: &Path, index: &Path) -> HistoryDb {
        HistoryDb::open_configured(
            &index.join("history.sqlite"),
            Some(&root.join("history")),
            None,
            HistoryMode::LocalOnly,
            Some(&root.join("nzbs")),
        )
        .unwrap()
    }

    #[test]
    fn local_only_skips_replay_but_repairs_failed_index_publication() {
        let t = tempfile::tempdir().unwrap();
        let db = local(t.path(), t.path());
        db.record(&entry(1, 100)).unwrap();
        due(&db);
        db.refresh().unwrap();
        assert_eq!(db.sync_status().passes, 1, "only startup scanned logs");
        db.conn.lock().unwrap().execute_batch("CREATE TRIGGER fail_insert BEFORE INSERT ON history BEGIN SELECT RAISE(FAIL,'injected'); END;").unwrap();
        assert!(db.record_seq_durable(&entry(2, 200)).is_err());
        assert!(db.sync_status().repair_pending);
        due(&db);
        assert!(db.refresh().is_err());
        assert_eq!(db.sync_status().state, "failed");
        assert!(db.sync_status().last_error.is_some());
        assert!(db.sync_status().repair_pending);
        db.conn
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER fail_insert")
            .unwrap();
        due(&db);
        db.refresh().unwrap();
        assert_eq!(db.count_filtered(true).unwrap(), 2);
        assert!(!db.sync_status().repair_pending);
        due(&db);
        db.refresh().unwrap();
        assert_eq!(
            db.sync_status().passes,
            3,
            "startup, failed repair, successful repair"
        );
    }

    #[test]
    fn clean_local_history_retires_interrupted_replay_without_claiming_a_scan() {
        let t = tempfile::tempdir().unwrap();
        let db = local(t.path(), t.path());
        let last_success = db.sync.progress.lock().unwrap().last_success;
        let passes = db.sync_status().passes;
        let generation = db.sync.generation.load(Ordering::SeqCst);
        db.record(&entry(1, 100)).unwrap();
        let conflict = db.replay_checkpoint(true, generation).unwrap_err();
        // The worker stores this result after the successful local writer has
        // already cleared its transient dirty flag.
        db.sync.progress.lock().unwrap().last_error = Some(conflict.to_string());
        assert_eq!(db.sync_status().state, "failed");
        assert!(!db.sync_status().repair_pending);

        db.refresh().unwrap();

        let status = db.sync_status();
        assert_eq!(status.state, "idle");
        assert!(status.last_error.is_none());
        assert_eq!(status.passes, passes);
        assert_eq!(db.sync.progress.lock().unwrap().last_success, last_success);
        assert_eq!(db.count_filtered(true).unwrap(), 1);
    }

    #[test]
    fn directory_authority_excludes_cross_mode_writers_in_both_orders() {
        let t = tempfile::tempdir().unwrap();
        let logs = t.path().join("history");
        let local_db = local(t.path(), t.path());
        assert!(
            HistoryDb::open_tagged(&t.path().join("peer.sqlite"), Some(&logs), Some("peer"))
                .is_err()
        );
        assert!(HistoryDb::open(&t.path().join("generic.sqlite"), Some(&logs)).is_err());
        drop(local_db);
        let peer = HistoryDb::open_tagged(&t.path().join("peer.sqlite"), Some(&logs), Some("peer"))
            .unwrap();
        // No peer log exists yet, but its authority must already exclude local.
        assert!(!logs.join("history.peer.jsonl").exists());
        assert!(HistoryDb::open_configured(
            &t.path().join("local2.sqlite"),
            Some(&logs),
            None,
            HistoryMode::LocalOnly,
            None
        )
        .is_err());
        let other =
            HistoryDb::open_tagged(&t.path().join("other.sqlite"), Some(&logs), Some("other"))
                .unwrap();
        assert!(HistoryDb::open_tagged(
            &t.path().join("duplicate.sqlite"),
            Some(&logs),
            Some("peer")
        )
        .is_err());
        drop((peer, other));
        assert!(HistoryDb::open_configured(
            &t.path().join("local2.sqlite"),
            Some(&logs),
            None,
            HistoryMode::LocalOnly,
            None
        )
        .is_ok());
    }

    #[test]
    fn tombstone_catchup_is_resumable_between_bounded_batches() {
        let t = tempfile::tempdir().unwrap();
        let db = local(t.path(), t.path());
        for id in 1..=50 {
            db.record(&entry(id, 100)).unwrap();
        }
        let keys: Vec<_> = (1..=49).map(|id| (id, 100)).collect();
        let generation = db.sync.generation.load(Ordering::SeqCst);
        db.replay_tombstone_batch(&keys[..16], true, generation)
            .unwrap();
        db.set_sync_paused(true).unwrap();
        assert!(db
            .replay_tombstone_batch(&keys[16..32], true, generation)
            .is_err());
        assert_eq!(
            db.count_filtered(true).unwrap(),
            34,
            "pause preserves only committed batches"
        );
        db.set_sync_paused(false).unwrap();
        db.replay_tombstones(&keys.into_iter().collect(), true, generation)
            .unwrap();
        assert_eq!(
            db.list_filtered(100, true).unwrap()[0].job,
            crate::JobId(50)
        );
        assert_eq!(db.count_filtered(true).unwrap(), 1);
        db.replay_logs(false).unwrap();
        assert_eq!(
            db.count_filtered(true).unwrap(),
            1,
            "old portable entries cannot undo committed tombstones"
        );
    }

    #[test]
    fn local_ownership_is_exclusive_and_untagged_shared_stores_still_ingest() {
        let t = tempfile::tempdir().unwrap();
        let db = local(t.path(), t.path());
        assert!(HistoryDb::open_configured(
            &t.path().join("other.sqlite"),
            Some(&t.path().join("history")),
            None,
            HistoryMode::LocalOnly,
            None
        )
        .is_err());
        drop(db);
        let shared = HistoryDb::open(
            &t.path().join("shared.sqlite"),
            Some(&t.path().join("history")),
        )
        .unwrap();
        std::fs::write(
            t.path().join("history/history.peer.jsonl"),
            format!("{}\n", serde_json::to_string(&entry(3, 300)).unwrap()),
        )
        .unwrap();
        due(&shared);
        shared.refresh().unwrap();
        assert_eq!(shared.count_filtered(true).unwrap(), 1);
        assert!(HistoryDb::open_configured(
            &t.path().join("local.sqlite"),
            Some(&t.path().join("history")),
            None,
            HistoryMode::LocalOnly,
            None
        )
        .is_err());
    }

    #[test]
    fn unchanged_refresh_does_no_content_io_or_sequence_writes() {
        let t = tempfile::tempdir().unwrap();
        let db = HistoryDb::open(&t.path().join("h.sqlite"), Some(&t.path().join("logs"))).unwrap();
        db.record(&entry(1, 100)).unwrap();
        due(&db);
        db.refresh().unwrap();
        let seq = || {
            db.conn
                .lock()
                .unwrap()
                .query_row(
                    "SELECT seq FROM sqlite_sequence WHERE name='history'",
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap()
        };
        let before = seq();
        due(&db);
        db.refresh().unwrap();
        assert_eq!(db.sync_status().last_bytes_read, 0);
        assert_eq!(db.sync_status().skipped_unchanged, 1);
        // A forced actual replay also must not advance AUTOINCREMENT.
        db.replay_logs(false).unwrap();
        assert_eq!(seq(), before);
    }

    #[test]
    fn pause_persists_but_mandatory_startup_recovery_still_runs() {
        let t = tempfile::tempdir().unwrap();
        {
            let db = local(t.path(), t.path());
            db.record(&entry(1, 100)).unwrap();
            db.set_sync_paused(true).unwrap();
        }
        let db = local(t.path(), t.path());
        assert!(db.sync_status().paused);
        assert_eq!(db.count_filtered(true).unwrap(), 1);
        assert!(db.refresh().is_err());
        db.set_sync_paused(false).unwrap();
        assert!(!db.sync_status().paused);
        db.refresh().unwrap();
    }

    #[test]
    fn readers_do_not_wait_for_the_reconciliation_gate_or_writer_connection() {
        let t = tempfile::tempdir().unwrap();
        let db = Arc::new(local(t.path(), t.path()));
        db.record(&entry(1, 100)).unwrap();
        let gate = db.sync.gate.lock().unwrap();
        let writer = db.conn.lock().unwrap();
        let (send, recv) = std::sync::mpsc::channel();
        let other = db.clone();
        let task =
            std::thread::spawn(move || send.send(other.read_page(20, 0, None).unwrap().1).unwrap());
        let result = recv.recv_timeout(Duration::from_secs(2));
        drop(writer);
        drop(gate);
        task.join().unwrap();
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn replay_cannot_publish_a_snapshot_after_an_acknowledged_local_mutation() {
        let t = tempfile::tempdir().unwrap();
        let db = local(t.path(), t.path());
        db.record(&entry(1, 100)).unwrap();
        let generation = db.sync.generation.load(Ordering::SeqCst);
        db.hide(crate::JobId(1), Some("client"), 200).unwrap();
        assert!(db.replay_batch(&[entry(1, 100)], true, generation).is_err());
        assert!(db.list_filtered(1, true).unwrap()[0].hidden);
    }

    #[test]
    fn index_relocation_keeps_cursor_observations_pause_and_spool() {
        let t = tempfile::tempdir().unwrap();
        let legacy = t.path().join("state");
        let target = t.path().join("local-index");
        let seq;
        {
            let db = local(&legacy, &legacy);
            seq = db.record_seq(&entry(7, 700)).unwrap();
            db.mark_seen(&[crate::JobId(7)], Some("consumer"), 800)
                .unwrap();
            db.flush_seen().unwrap();
            db.spool_nzb(crate::JobId(7), b"<nzb/>").unwrap();
            db.set_sync_paused(true).unwrap();
        }
        {
            let db = local(&legacy, &target);
            let row = db.list_filtered(1, true).unwrap().remove(0);
            assert_eq!(row.seq, seq);
            assert_eq!(row.seen_count, 1);
            assert!(db.sync_status().paused);
            assert!(db.has_spool(crate::JobId(7)));
            db.record(&entry(8, 800)).unwrap();
        }
        assert_eq!(local(&legacy, &target).count_filtered(true).unwrap(), 2);
        // Switching back to an abandoned existing file must not lose new rows.
        assert!(HistoryDb::open_configured(
            &legacy.join("history.sqlite"),
            Some(&legacy.join("history")),
            None,
            HistoryMode::LocalOnly,
            Some(&legacy.join("nzbs"))
        )
        .is_err());
    }

    #[test]
    fn failed_shared_scan_does_not_advance_success_or_cache() {
        let t = tempfile::tempdir().unwrap();
        let db = HistoryDb::open(&t.path().join("h.sqlite"), Some(&t.path().join("logs"))).unwrap();
        std::fs::create_dir_all(t.path().join("logs/history.bad.jsonl")).unwrap();
        let before = db.sync.progress.lock().unwrap().last_success;
        due(&db);
        assert!(db.refresh().is_err());
        assert_eq!(db.sync.progress.lock().unwrap().last_success, before);
        std::fs::remove_dir(t.path().join("logs/history.bad.jsonl")).unwrap();
        std::fs::write(
            t.path().join("logs/history.peer.jsonl"),
            format!("{}\n", serde_json::to_string(&entry(1, 100)).unwrap()),
        )
        .unwrap();
        due(&db);
        db.refresh().unwrap();
        assert_eq!(db.count_filtered(true).unwrap(), 1);
    }

    #[test]
    fn observation_is_deferred_when_replay_owns_writer_and_retried_without_loss() {
        let t = tempfile::tempdir().unwrap();
        let db = local(t.path(), t.path());
        db.record(&entry(1, 100)).unwrap();
        let writer = db.conn.lock().unwrap();
        db.mark_seen(&[crate::JobId(1)], Some("consumer"), 200)
            .unwrap();
        db.mark_seen(&[crate::JobId(1)], Some("consumer"), 201)
            .unwrap();
        assert_eq!(db.list_filtered(1, true).unwrap()[0].seen_count, 0);
        drop(writer);
        db.flush_seen().unwrap();
        let row = db.list_filtered(1, true).unwrap().remove(0);
        assert_eq!(row.seen_count, 2);
        assert_eq!(row.first_seen_at_unix, Some(200));
        assert_eq!(row.last_seen_at_unix, Some(201));
    }

    #[test]
    fn simultaneous_due_callers_share_one_completed_pass() {
        let t = tempfile::tempdir().unwrap();
        let db = Arc::new(
            HistoryDb::open(&t.path().join("h.sqlite"), Some(&t.path().join("logs"))).unwrap(),
        );
        db.record(&entry(1, 100)).unwrap();
        due(&db);
        let gate = db.sync.gate.lock().unwrap();
        let start = Arc::new(std::sync::Barrier::new(9));
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let db = db.clone();
                let start = start.clone();
                std::thread::spawn(move || {
                    start.wait();
                    db.refresh().unwrap();
                })
            })
            .collect();
        start.wait();
        drop(gate);
        for task in tasks {
            task.join().unwrap();
        }
        assert_eq!(
            db.sync_status().passes,
            2,
            "startup plus one shared refresh"
        );
    }

    #[test]
    fn migration_reads_committed_wal_instead_of_copying_only_main_file() {
        let t = tempfile::tempdir().unwrap();
        let state = t.path().join("state");
        std::fs::create_dir_all(&state).unwrap();
        let db =
            HistoryDb::open(&state.join("history.sqlite"), Some(&state.join("history"))).unwrap();
        db.record(&entry(1, 100)).unwrap();
        let pinned = Connection::open(state.join("history.sqlite")).unwrap();
        pinned
            .execute_batch("BEGIN; SELECT * FROM history;")
            .unwrap();
        let seq = db.record_seq(&entry(2, 200)).unwrap();
        drop(db);
        assert!(
            std::fs::metadata(state.join("history.sqlite-wal"))
                .unwrap()
                .len()
                > 0
        );
        let migrated = local(&state, &t.path().join("local"));
        assert_eq!(migrated.count_filtered(true).unwrap(), 2);
        assert_eq!(migrated.list_filtered(1, true).unwrap()[0].seq, seq);
        drop(pinned);
    }

    #[test]
    fn paused_replay_cannot_commit_and_worker_ownership_is_unique() {
        let t = tempfile::tempdir().unwrap();
        let db = Arc::new(local(t.path(), t.path()));
        db.set_sync_paused(true).unwrap();
        assert!(db.replay_batch(&[entry(1, 100)], true, 0).is_err());
        assert_eq!(db.count_filtered(true).unwrap(), 0);
        let worker = db.start_worker().unwrap();
        assert!(db.start_worker().is_err());
        worker.stop();
        assert!(db.refresh().is_err());
    }

    #[test]
    fn missing_registered_index_requires_explicit_recovery() {
        let t = tempfile::tempdir().unwrap();
        drop(local(t.path(), t.path()));
        std::fs::remove_file(t.path().join("history.sqlite")).unwrap();
        assert!(HistoryDb::open_configured(
            &t.path().join("history.sqlite"),
            Some(&t.path().join("history")),
            None,
            HistoryMode::LocalOnly,
            Some(&t.path().join("nzbs"))
        )
        .is_err());
    }

    #[test]
    #[ignore = "repeatable synthetic performance probe; run explicitly with --nocapture"]
    fn history_loading_probe() {
        use std::io::Write;
        let t = tempfile::tempdir().unwrap();
        let logs = t.path().join("logs");
        std::fs::create_dir_all(&logs).unwrap();
        let rows = 1000;
        let mut f =
            std::io::BufWriter::new(std::fs::File::create(logs.join("history.jsonl")).unwrap());
        for n in 0..rows {
            let mut e = entry(n, 1000 + n as i64);
            e.record = Some(crate::JobRecord {
                original_name: Some("x".repeat(16 * 1024)),
                ..Default::default()
            });
            serde_json::to_writer(&mut f, &e).unwrap();
            f.write_all(b"\n").unwrap();
        }
        f.flush().unwrap();
        let start = Instant::now();
        let db = Arc::new(HistoryDb::open(&t.path().join("h.sqlite"), Some(&logs)).unwrap());
        let startup = start.elapsed();
        let replay_db = db.clone();
        let replay = std::thread::spawn(move || {
            let start = Instant::now();
            for _ in 0..3 {
                replay_db.replay_logs(false).unwrap();
            }
            start.elapsed()
        });
        let mut times = vec![];
        for i in 0..100 {
            let start = Instant::now();
            let (page, total) = db.read_page(20, [0, 500, 980][i % 3], None).unwrap();
            assert_eq!(total, rows as u64);
            assert_eq!(page.len(), 20);
            times.push(start.elapsed().as_micros());
        }
        let background = replay.join().unwrap();
        times.sort_unstable();
        println!("rows={rows} synthetic_record_bytes=16384 startup_ms={} replay_three_ms={} read_p50_us={} read_p95_us={} read_p99_us={} max_batch_ms={}",startup.as_millis(),background.as_millis(),times[49],times[94],times[98],db.sync_status().max_batch_ms);
    }

    #[test]
    fn query_plan_uses_history_ordering_index() {
        let t = tempfile::tempdir().unwrap();
        let db = local(t.path(), t.path());
        let c = db.reader.lock().unwrap();
        let mut q=c.prepare("EXPLAIN QUERY PLAN SELECT * FROM history ORDER BY completed_at DESC,id DESC LIMIT 20 OFFSET 500").unwrap();
        let plan = q
            .query_map([], |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join(" ");
        assert!(plan.contains("history_completed_at_id"), "{plan}");
        assert!(!plan.contains("TEMP B-TREE"), "{plan}");
    }
}
