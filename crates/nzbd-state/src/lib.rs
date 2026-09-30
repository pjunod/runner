//! Persistence for the download queue. See ARCHITECTURE.md §8.6.
//!
//! Two artifacts in the state directory, plus a marker:
//!
//! 1. **Queue snapshot** (`queue.json`): all jobs/files/segments sans
//!    transient lease state. Written atomically (tmp + rename, fsync'd),
//!    debounced on structural change.
//! 2. **Segment journal** (`segments.journal`): append-only records of
//!    completed segments — one JSON line each, fsync'd on a short interval
//!    by the engine tick, compacted (truncated) whenever a fresh snapshot
//!    has folded them in. Recovery = load snapshot, replay journal,
//!    re-lease everything else.
//! 3. **`unclean` marker**: present while the daemon runs; removed on
//!    graceful shutdown. Its presence at startup signals a crash.
//!
//! Everything here is deliberately synchronous std I/O: the owner task calls
//! appends (page-cache writes, microseconds) inline and defers fsync to its
//! 1 Hz tick — same policy as NZBGet's DiskState, minus the bespoke format.
//!
//! **History** in SQLite arrives in phase 2 (the trait is defined below).

pub mod artifacts;
pub mod capacity;
pub mod fileops;
mod fsx;
pub mod history;
pub mod torrent_sources;
#[cfg(windows)]
mod windows_secret;

use nzbd_types::{FileId, Job, JobId, TorrentSource};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// A filesystem call failed. `op` is the human verb ("create
    /// directory", "fsync", …) and `path` is what it was applied to.
    ///
    /// A bare `std::io::Error` carries an errno and nothing else, so
    /// "Permission denied (os error 13)" leaves an operator with no idea
    /// which file to chmod. Every fs call in this crate goes through
    /// [`fsx`] so the path is always in the message.
    #[error("{op} {}: {source}", .path.display())]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("serialize: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("corrupt state: {0}")]
    Corrupt(String),
}

impl StateError {
    /// The path this error is about, when it is a filesystem error.
    pub fn path(&self) -> Option<&Path> {
        match self {
            StateError::Io { path, .. } => Some(path),
            _ => None,
        }
    }

    /// The underlying errno kind, when this is a filesystem error.
    pub fn io_kind(&self) -> Option<std::io::ErrorKind> {
        match self {
            StateError::Io { source, .. } => Some(source.kind()),
            _ => None,
        }
    }

    pub fn is_not_found(&self) -> bool {
        self.io_kind() == Some(std::io::ErrorKind::NotFound)
    }

    /// True for EACCES/EPERM — the daemon lacks rights on [`Self::path`].
    /// The startup path uses this to print an actionable hint.
    pub fn is_permission_denied(&self) -> bool {
        matches!(self.io_kind(), Some(std::io::ErrorKind::PermissionDenied))
    }
}

// ---------------------------------------------------------------------------
// Segment journal
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalRecord {
    pub job: JobId,
    pub file: FileId,
    pub segment_number: u32,
    /// Position of the decoded part in the output file.
    pub offset: u64,
    pub len: u32,
    pub crc32: u32,
    /// Total output-file size from the yEnc header — lets recovery finalize
    /// files whose remaining segments all failed before the crash.
    pub file_size: u64,
}

/// Append-only segment journal backed by a single file of JSON lines.
/// Replay tolerates a torn trailing line (crash mid-append).
pub struct FsJournal {
    path: PathBuf,
    file: File,
    dirty: bool,
}

impl FsJournal {
    pub fn open(dir: &Path) -> Result<FsJournal, StateError> {
        fsx::create_dir_all(dir)?;
        let path = dir.join("segments.journal");
        let file = fsx::open_append(&path)?;
        Ok(FsJournal {
            path,
            file,
            dirty: false,
        })
    }

    pub fn append(&mut self, rec: &JournalRecord) -> Result<(), StateError> {
        let mut line = serde_json::to_vec(rec)?;
        line.push(b'\n');
        fsx::write_all(&mut self.file, &line, &self.path)?;
        self.dirty = true;
        Ok(())
    }

    /// fsync if anything was appended since the last sync.
    pub fn sync(&mut self) -> Result<(), StateError> {
        if self.dirty {
            fsx::sync_data(&self.file, &self.path)?;
            self.dirty = false;
        }
        Ok(())
    }

    /// Read every intact record. Stops (without erroring) at the first
    /// corrupt or torn line — everything after it is unusable anyway.
    pub fn replay(&self) -> Result<Vec<JournalRecord>, StateError> {
        let file = match fsx::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.is_not_found() => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut out = Vec::new();
        for line in BufReader::new(file).split(b'\n') {
            let line = fsx::ctx(line, "read", &self.path)?;
            if line.is_empty() {
                continue;
            }
            match serde_json::from_slice::<JournalRecord>(&line) {
                Ok(rec) => out.push(rec),
                Err(_) => break, // torn tail
            }
        }
        Ok(out)
    }

    /// Truncate after the records have been folded into a snapshot.
    pub fn compact(&mut self) -> Result<(), StateError> {
        fsx::truncate(&self.path)?;
        self.dirty = false;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Per-job fenced journals (CLUSTERING.md §6.4 / ADR-16)
//
// One journal file per (job, writer-lease): `jobs/<id>/journal.<suffix>`.
// Replay UNIONS the Done records across every journal file of a job —
// records are idempotent (immutable article content ⇒ same offset/len/crc),
// so an expired lease holder appending late is harmless. This is what makes
// cross-node reclaim safe without any shared-file locking.
// ---------------------------------------------------------------------------

/// An open journal file, paired with the path it was opened from so
/// write/fsync failures can name it.
struct OpenJournal {
    path: PathBuf,
    file: File,
}

pub struct JobJournals {
    jobs_dir: PathBuf,
    /// Fencing suffix — the work-lease id, or "local" for single-node.
    suffix: String,
    open: std::collections::HashMap<u32, OpenJournal>,
    dirty: bool,
}

impl JobJournals {
    pub fn open(state_dir: &Path, suffix: &str) -> Result<JobJournals, StateError> {
        let jobs_dir = state_dir.join("jobs");
        fsx::create_dir_all(&jobs_dir)?;
        Ok(JobJournals {
            jobs_dir,
            suffix: sanitize_suffix(suffix),
            open: Default::default(),
            dirty: false,
        })
    }

    pub fn jobs_dir(&self) -> &Path {
        &self.jobs_dir
    }

    fn file_for(&mut self, job: JobId) -> Result<&mut OpenJournal, StateError> {
        if !self.open.contains_key(&job.0) {
            let dir = self.jobs_dir.join(job.0.to_string());
            fsx::create_dir_all(&dir)?;
            let path = dir.join(format!("journal.{}", self.suffix));
            let file = fsx::open_append(&path)?;
            self.open.insert(job.0, OpenJournal { path, file });
        }
        Ok(self.open.get_mut(&job.0).unwrap())
    }

    pub fn append(&mut self, rec: &JournalRecord) -> Result<(), StateError> {
        let mut line = serde_json::to_vec(rec)?;
        line.push(b'\n');
        let j = self.file_for(rec.job)?;
        fsx::write_all(&mut j.file, &line, &j.path)?;
        self.dirty = true;
        Ok(())
    }

    /// fsync every journal touched since the last sync.
    pub fn sync(&mut self) -> Result<(), StateError> {
        if self.dirty {
            for j in self.open.values() {
                fsx::sync_data(&j.file, &j.path)?;
            }
            self.dirty = false;
        }
        Ok(())
    }

    /// Union replay of ALL journal files (any suffix) for one job.
    pub fn replay_job(state_dir: &Path, job: JobId) -> Result<Vec<JournalRecord>, StateError> {
        let dir = state_dir.join("jobs").join(job.0.to_string());
        replay_dir(&dir)
    }

    /// Union replay across every job directory.
    pub fn replay_all(state_dir: &Path) -> Result<Vec<JournalRecord>, StateError> {
        let jobs_dir = state_dir.join("jobs");
        let mut out = Vec::new();
        let entries = match fsx::read_dir(&jobs_dir) {
            Ok(e) => e,
            Err(e) if e.is_not_found() => return Ok(out),
            Err(e) => return Err(e),
        };
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                out.extend(replay_dir(&entry.path())?);
            }
        }
        Ok(out)
    }

    /// Remove every journal file of a job — call ONLY as the queue
    /// authority, immediately after a snapshot that folds the job's state,
    /// and never while the job is delegated (a live foreign writer).
    pub fn remove_job(&mut self, job: JobId) -> Result<(), StateError> {
        self.open.remove(&job.0);
        let dir = self.jobs_dir.join(job.0.to_string());
        match fsx::remove_dir_all(&dir) {
            Err(e) if e.is_not_found() => Ok(()),
            other => other,
        }
    }
}

fn replay_dir(dir: &Path) -> Result<Vec<JournalRecord>, StateError> {
    let mut out = Vec::new();
    let entries = match fsx::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.is_not_found() => return Ok(out),
        Err(e) => return Err(e),
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with("journal.") {
            continue;
        }
        let path = entry.path();
        let file = match fsx::open(&path) {
            Ok(f) => f,
            Err(_) => continue,
        };
        for line in BufReader::new(file).split(b'\n') {
            let line = fsx::ctx(line, "read", &path)?;
            if line.is_empty() {
                continue;
            }
            match serde_json::from_slice::<JournalRecord>(&line) {
                Ok(rec) => out.push(rec),
                Err(_) => break, // torn tail of this file; others still count
            }
        }
    }
    Ok(out)
}

fn sanitize_suffix(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() {
        "local".into()
    } else {
        cleaned
    }
}

// ---------------------------------------------------------------------------
// Queue snapshot
// ---------------------------------------------------------------------------

/// The queue document written by this binary.
///
/// Version 1 is the unversioned shape written before BitTorrent groundwork;
/// missing `schema_version` therefore means 1 on read. Version 2 adds the
/// explicit envelope. Version 3 reserves the torrent job variant and its
/// defaulted durable transfer record. Older schema-2 documents continue to
/// decode as their original Usenet jobs and are rewritten as version 3 on the
/// next owner snapshot. Version 4 adds protected pending torrent admissions.
pub const QUEUE_SCHEMA_VERSION: u32 = 4;
const OLDEST_QUEUE_SCHEMA_VERSION: u32 = 1;

const fn legacy_queue_schema_version() -> u32 {
    OLDEST_QUEUE_SCHEMA_VERSION
}

/// Everything needed to reconstruct the queue after a restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueSnapshotDoc {
    #[serde(default = "legacy_queue_schema_version")]
    pub schema_version: u32,
    /// Sources accepted before their descriptor is known. The source itself
    /// is a mode-0600 sidecar; this record is deliberately non-secret.
    #[serde(default)]
    pub pending_admissions: Vec<PendingAdmission>,
    pub jobs: Vec<Job>,
    pub next_job_id: u32,
    pub next_file_id: u32,
    pub download_paused: bool,
    pub speed_limit_bps: Option<u64>,
    /// How many jobs may download at once. Absent in snapshots written
    /// before the setting existed; `QueueState::from_doc` clamps the
    /// resulting 0 up to 1 rather than treating it as "none may".
    #[serde(default)]
    pub max_active_downloads: u32,
}

impl Default for QueueSnapshotDoc {
    fn default() -> Self {
        Self {
            schema_version: QUEUE_SCHEMA_VERSION,
            pending_admissions: Vec::new(),
            jobs: Vec::new(),
            next_job_id: 0,
            next_file_id: 0,
            download_paused: false,
            speed_limit_bps: None,
            max_active_downloads: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingAdmission {
    pub job_id: JobId,
    pub source: TorrentSource,
    pub secret_ref: PathBuf,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub paused: bool,
    #[serde(default)]
    pub seed_ratio_limit: Option<f64>,
    #[serde(default)]
    pub seed_time_limit_secs: Option<u64>,
    #[serde(default)]
    pub stop_seeding_on_complete: Option<bool>,
    #[serde(default)]
    pub params: Vec<(String, String)>,
    #[serde(default)]
    pub client: Option<String>,
}

/// Read only when the typed queue document fails. Unknown fields — including
/// a future job representation — are skipped, so a newer schema still
/// produces an actionable version error without making every successful boot
/// parse a production-sized queue twice.
#[derive(Deserialize)]
struct QueueSnapshotHeader {
    #[serde(default = "legacy_queue_schema_version")]
    schema_version: u32,
}

pub struct SnapshotStore {
    path: PathBuf,
    tmp: PathBuf,
    dir: PathBuf,
}

impl SnapshotStore {
    pub fn open(dir: &Path) -> Result<SnapshotStore, StateError> {
        fsx::create_dir_all_durable(dir)?;
        Ok(SnapshotStore {
            path: dir.join("queue.json"),
            tmp: dir.join("queue.json.tmp"),
            dir: dir.to_path_buf(),
        })
    }

    /// Atomic write: encode to memory, ONE `write_all`, fsync + rename +
    /// fsync(dir). Returns the snapshot's size in bytes.
    pub fn save(&self, doc: &QueueSnapshotDoc) -> Result<u64, StateError> {
        self.save_guarded(doc, &|| true)
    }

    /// Atomic write with a fencing guard checked immediately before the
    /// commit rename (CLUSTERING.md §6.4: a deposed leader must not clobber
    /// its successor's snapshot). Returns `Corrupt("fenced")` when the
    /// guard rejects; the snapshot's size in bytes otherwise.
    ///
    /// The doc is serialized to memory first and hits the file as a single
    /// `write_all`. This is load-bearing, not style: `serde_json::to_writer`
    /// straight into a `File` issues one tiny write syscall per JSON token,
    /// and a queue of a few hundred thousand segments on a FUSE/network
    /// state volume turned every save into ~65 s of syscalls while the
    /// engine sat blocked behind it (field report 2026-07-26 — the volume's
    /// fsync itself took 20 ms). One buffer, one write: the volume only
    /// pays for the fsync it can't be spared.
    pub fn save_guarded(
        &self,
        doc: &QueueSnapshotDoc,
        guard: &dyn Fn() -> bool,
    ) -> Result<u64, StateError> {
        if doc.schema_version != QUEUE_SCHEMA_VERSION {
            return Err(StateError::Corrupt(format!(
                "refusing to write queue schema version {}; this nzbd writes {}",
                doc.schema_version, QUEUE_SCHEMA_VERSION
            )));
        }
        let bytes = serde_json::to_vec(doc)?;
        let mut f = fsx::create(&self.tmp)?;
        fsx::write_whole(&mut f, &bytes, &self.tmp)?;
        fsx::sync_data(&f, &self.tmp)?;
        drop(f);
        if !guard() {
            return Err(StateError::Corrupt(
                "fenced: no longer the authority".into(),
            ));
        }
        fsx::rename(&self.tmp, &self.path)?;
        fsx::sync_dir(&self.dir)?;
        Ok(bytes.len() as u64)
    }

    /// `None` if no snapshot exists yet. A corrupt snapshot is an error —
    /// the operator should decide, not silently lose a queue.
    pub fn load(&self) -> Result<Option<QueueSnapshotDoc>, StateError> {
        let bytes = match fsx::read(&self.path) {
            Ok(b) => b,
            Err(e) if e.is_not_found() => return Ok(None),
            Err(e) => return Err(e),
        };
        match serde_json::from_slice::<QueueSnapshotDoc>(&bytes) {
            Ok(doc) => {
                validate_queue_schema_version(doc.schema_version)?;
                Ok(Some(doc))
            }
            Err(typed_error) => {
                if let Ok(header) = serde_json::from_slice::<QueueSnapshotHeader>(&bytes) {
                    validate_queue_schema_version(header.schema_version)?;
                }
                Err(StateError::Corrupt(format!("queue.json: {typed_error}")))
            }
        }
    }
}

fn validate_queue_schema_version(schema_version: u32) -> Result<(), StateError> {
    if schema_version > QUEUE_SCHEMA_VERSION {
        return Err(StateError::Corrupt(format!(
            "queue.json schema version {} is newer than this nzbd supports ({}); upgrade nzbd or restore a compatible queue snapshot",
            schema_version, QUEUE_SCHEMA_VERSION
        )));
    }
    if schema_version < OLDEST_QUEUE_SCHEMA_VERSION {
        return Err(StateError::Corrupt(format!(
            "queue.json schema version {} is older than this nzbd supports ({})",
            schema_version, OLDEST_QUEUE_SCHEMA_VERSION
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Unclean-shutdown marker
// ---------------------------------------------------------------------------

pub struct UncleanMarker {
    path: PathBuf,
}

impl UncleanMarker {
    /// `tag` scopes the marker: in cluster mode every node shares one state
    /// directory, so each arms its own `unclean.<node>` file.
    pub fn new(dir: &Path, tag: &str) -> UncleanMarker {
        UncleanMarker {
            path: dir.join(format!("unclean.{}", sanitize_suffix(tag))),
        }
    }

    /// Returns whether the previous run ended uncleanly, then (re)arms the
    /// marker for this run.
    pub fn check_and_arm(&self) -> Result<bool, StateError> {
        let was_unclean = self.path.exists();
        if let Some(parent) = self.path.parent() {
            fsx::create_dir_all(parent)?;
        }
        fsx::write(&self.path, b"")?;
        Ok(was_unclean)
    }

    /// Graceful shutdown: state on disk is consistent.
    pub fn disarm(&self) -> Result<(), StateError> {
        match fsx::remove_file(&self.path) {
            Err(e) if e.is_not_found() => Ok(()),
            other => other,
        }
    }
}

// ---------------------------------------------------------------------------
// History (phase 2: SQLite implementation)
// ---------------------------------------------------------------------------

/// Terminal record of a job for the history store.
/// One file, as it stood when the job left the queue.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HistoryFile {
    pub name: String,
    pub size: u64,
    pub segments_total: u32,
    pub segments_done: u32,
    pub segments_failed: u32,
    pub par2: bool,
}

/// **The job as it was, kept with the job.**
///
/// History used to hold a summary — name, status, size, health — and drop
/// everything the queue knew: which files, how many articles, how many had
/// to be retried, which indexer it came from, who asked for it. Those are
/// exactly the questions asked *afterwards*: "which file was short?", "was
/// this the release I think it was?", "where did I get this?". A record
/// that answers them only while the job is still in the queue answers them
/// at the one time nobody is asking (field report 2026-07-29).
///
/// Optional because entries written before this existed do not have one,
/// and because a job that failed before it had files has nothing to say.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct JobRecord {
    /// Additive protocol discriminator. Old history entries deserialize as
    /// NZB so existing consumers retain their historical interpretation.
    #[serde(default = "default_history_job_kind")]
    pub kind: nzbd_types::JobKind,
    pub files: Vec<HistoryFile>,
    pub total_articles: u32,
    pub success_articles: u32,
    pub failed_articles: u32,
    /// Bytes that had to be fetched more than once — wasted bandwidth,
    /// invisible in the size totals.
    #[serde(default)]
    pub retried_articles: u32,
    pub par_size: u64,
    /// Where it came from, and who asked for it.
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub client: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
    /// When the job entered the queue. With `completed_at_unix` this is
    /// how long the whole thing took, which no other field records.
    #[serde(default)]
    pub queued_at_unix: Option<i64>,
    /// The name the job was admitted under, when it later renamed itself
    /// from its own metadata. Keeps the *arr's original reference findable.
    #[serde(default)]
    pub original_name: Option<String>,
    /// The directory the files were written to, relative to the download
    /// destination.
    #[serde(default)]
    pub dir_name: Option<String>,
    /// Stable torrent facts retained after the live backend handle is gone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub torrent: Option<TorrentHistoryRecord>,
}

fn default_history_job_kind() -> nzbd_types::JobKind {
    nzbd_types::JobKind::Nzb
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TorrentHistoryRecord {
    pub info_hash_v1: String,
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    pub seeding_seconds: u64,
    pub ready_at_unix: Option<i64>,
    pub terminal_reason: String,
    pub payload: nzbd_types::TorrentPayloadDisposition,
}

impl JobRecord {
    /// Capture everything the queue knew, as the job leaves it.
    pub fn from_job(job: &nzbd_types::Job) -> JobRecord {
        let param = |k: &str| {
            job.params
                .iter()
                .find(|(pk, _)| pk == k)
                .map(|(_, v)| v.clone())
        };
        JobRecord {
            kind: job.kind,
            files: job
                .files
                .iter()
                .map(|f| HistoryFile {
                    name: f.filename.clone(),
                    size: f.segments.iter().map(|s| s.size as u64).sum(),
                    segments_total: f.segments.len() as u32,
                    segments_done: f
                        .segments
                        .iter()
                        .filter(|s| matches!(s.state, nzbd_types::SegmentState::Done { .. }))
                        .count() as u32,
                    segments_failed: f
                        .segments
                        .iter()
                        .filter(|s| matches!(s.state, nzbd_types::SegmentState::Failed))
                        .count() as u32,
                    par2: f.is_par2,
                })
                .collect(),
            total_articles: job.totals.total_articles,
            success_articles: job.totals.success_articles,
            failed_articles: job.totals.failed_articles,
            // Not on JobTotals — the engine tracks it per job in the live
            // snapshot only, so history records 0 rather than a wrong number.
            retried_articles: 0,
            par_size: job.totals.par_size,
            url: param("*URL"),
            client: param("*Client"),
            category: job.category.clone(),
            queued_at_unix: (job.queued_at_unix > 0).then_some(job.queued_at_unix),
            original_name: (!job.original_name.is_empty()).then(|| job.original_name.clone()),
            dir_name: (!job.dir_name.is_empty()).then(|| job.dir_name.clone()),
            torrent: job.torrent.as_ref().and_then(|torrent| {
                torrent.removal_outcome.map(|payload| TorrentHistoryRecord {
                    info_hash_v1: torrent.info_hash_v1.clone(),
                    downloaded_bytes: torrent.downloaded_bytes,
                    uploaded_bytes: torrent.uploaded_bytes,
                    seeding_seconds: torrent.seeding_seconds,
                    ready_at_unix: torrent.ready_at_unix,
                    terminal_reason: if job.status == nzbd_types::JobStatus::Failed {
                        "failed".to_owned()
                    } else {
                        "removed".to_owned()
                    },
                    payload,
                })
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub job: JobId,
    pub name: String,
    pub category: Option<String>,
    pub final_dir: Option<String>,
    pub status: String,
    pub size: u64,
    #[serde(default = "default_health")]
    pub health: u16,
    /// Job parameters at completion (drone ids, script settings, …).
    #[serde(default)]
    pub params: Vec<(String, String)>,
    /// Duplicate-detection metadata (empty key = none).
    #[serde(default)]
    pub dupe_key: String,
    #[serde(default)]
    pub dupe_score: i32,
    pub completed_at_unix: i64,
    // ---- handoff tracking (ARCH: the *arr pickup is a PULL; these record
    // what we observed of it) ------------------------------------------
    /// Hidden from compat `history` responses — set when a client deletes
    /// the entry after import (NZBGet HistoryDelete semantics) or the user
    /// hides it. Restorable.
    #[serde(default)]
    pub hidden: bool,
    /// First/last time a compat client's history poll listed this entry.
    #[serde(default)]
    pub first_seen_at_unix: Option<i64>,
    #[serde(default)]
    pub last_seen_at_unix: Option<i64>,
    #[serde(default)]
    pub seen_count: u32,
    /// When a client deleted (hid) the entry — the "imported" signal.
    #[serde(default)]
    pub removed_at_unix: Option<i64>,
    /// User-Agent of the polling/deleting client (Sonarr/x, Radarr/x…).
    #[serde(default)]
    pub picked_up_by: Option<String>,
    /// Everything the queue knew about this job, captured as it left.
    /// `None` for entries written before this existed.
    #[serde(default)]
    pub record: Option<JobRecord>,
    /// Where the post-processing time went, stage by stage, copied off the
    /// job as it finished.
    ///
    /// History is where that question actually gets asked — "why did that
    /// one take forty minutes" is something you wonder the next morning,
    /// about a job that left the queue hours ago. Empty for entries
    /// written before this column existed, and for jobs that failed before
    /// post-processing began.
    #[serde(default)]
    pub stages: Vec<nzbd_types::StageSpan>,
    /// Read-side cursor: this node's SQLite rowid, ascending and monotone.
    /// Populated on read, ignored on write, and deliberately **not**
    /// persisted to the JSONL — the JSONL is the portable source of truth
    /// and a rowid is an artifact of one node's index, so a rebuilt index
    /// legitimately assigns different ones. Consumers treat it as an
    /// opaque "everything newer than this" token against the node they are
    /// talking to (`GET /api/v1/history?since_seq=`).
    #[serde(default)]
    pub seq: i64,
}

fn default_health() -> u16 {
    1000
}

pub trait HistoryStore {
    fn record(&mut self, entry: &HistoryEntry) -> Result<(), StateError>;
    fn list(&self, limit: usize) -> Result<Vec<HistoryEntry>, StateError>;
    fn delete(&mut self, job: JobId) -> Result<bool, StateError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Write as _;

    /// `unwrap_err` needs `T: Debug`; the store handles deliberately
    /// aren't printable, so pull the error out by hand.
    fn err_of<T>(r: Result<T, StateError>) -> StateError {
        match r {
            Ok(_) => panic!("expected a StateError"),
            Err(e) => e,
        }
    }

    fn rec(seg: u32) -> JournalRecord {
        JournalRecord {
            job: JobId(1),
            file: FileId(2),
            segment_number: seg,
            offset: seg as u64 * 1000,
            len: 1000,
            crc32: 0xDEAD_0000 + seg,
            file_size: 5000,
        }
    }

    fn minimal_job() -> Job {
        Job {
            id: JobId(7),
            kind: nzbd_types::JobKind::Nzb,
            name: "legacy".into(),
            dir_name: "legacy".into(),
            name_provisional: false,
            queued_at_unix: 0,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: nzbd_types::DupeInfo::default(),
            params: Vec::new(),
            files: Vec::new(),
            totals: nzbd_types::JobTotals::default(),
            status: nzbd_types::JobStatus::Paused,
            torrent: None,
            stages: Vec::new(),
        }
    }

    #[test]
    fn journal_roundtrip_and_compact() {
        let dir = tempfile::tempdir().unwrap();
        let mut j = FsJournal::open(dir.path()).unwrap();
        for i in 0..5 {
            j.append(&rec(i)).unwrap();
        }
        j.sync().unwrap();
        assert_eq!(j.replay().unwrap().len(), 5);
        assert_eq!(j.replay().unwrap()[3], rec(3));

        j.compact().unwrap();
        assert!(j.replay().unwrap().is_empty());

        // append still works post-compact
        j.append(&rec(9)).unwrap();
        j.sync().unwrap();
        assert_eq!(j.replay().unwrap(), vec![rec(9)]);
    }

    #[cfg(windows)]
    #[test]
    fn journal_truncation_reopens_an_append_only_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("append-only.journal");
        let mut append_only = fsx::open_append(&path).unwrap();
        fsx::write_all(&mut append_only, b"before\n", &path).unwrap();
        fsx::sync_data(&append_only, &path).unwrap();

        fsx::truncate(&path).unwrap();

        fsx::write_all(&mut append_only, b"after\n", &path).unwrap();
        fsx::sync_data(&append_only, &path).unwrap();
        assert_eq!(fsx::read(&path).unwrap(), b"after\n");
    }

    #[test]
    fn journal_tolerates_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut j = FsJournal::open(dir.path()).unwrap();
        j.append(&rec(0)).unwrap();
        j.append(&rec(1)).unwrap();
        j.sync().unwrap();
        // simulate a crash mid-append
        let mut f = OpenOptions::new()
            .append(true)
            .open(dir.path().join("segments.journal"))
            .unwrap();
        f.write_all(b"{\"job\":1,\"file\":2,\"segment_nu").unwrap();
        drop(f);

        let j2 = FsJournal::open(dir.path()).unwrap();
        let recs = j2.replay().unwrap();
        assert_eq!(recs.len(), 2, "torn line must be dropped, prior kept");
    }

    #[test]
    fn snapshot_save_load() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        assert!(store.load().unwrap().is_none());

        let doc = QueueSnapshotDoc {
            schema_version: QUEUE_SCHEMA_VERSION,
            pending_admissions: Vec::new(),
            jobs: vec![],
            next_job_id: 7,
            next_file_id: 42,
            download_paused: true,
            speed_limit_bps: Some(1_000_000),
            max_active_downloads: 3,
        };
        let bytes = store.save(&doc).unwrap();
        assert_eq!(
            bytes,
            std::fs::metadata(dir.path().join("queue.json"))
                .unwrap()
                .len(),
            "save reports exactly what landed on disk"
        );
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.schema_version, QUEUE_SCHEMA_VERSION);
        assert_eq!(loaded.next_job_id, 7);
        assert_eq!(loaded.next_file_id, 42);
        assert!(loaded.download_paused);
        assert_eq!(loaded.speed_limit_bps, Some(1_000_000));
        assert!(!dir.path().join("queue.json.tmp").exists());
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_commit_reports_directory_sync_failure() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        let doc = QueueSnapshotDoc {
            schema_version: QUEUE_SCHEMA_VERSION,
            pending_admissions: Vec::new(),
            jobs: vec![],
            next_job_id: 1,
            next_file_id: 1,
            download_paused: false,
            speed_limit_bps: None,
            max_active_downloads: 1,
        };
        crate::fsx::fail_next_dir_sync();
        let error = store.save(&doc).unwrap_err();
        assert!(error.to_string().contains("fsync directory"));
    }

    #[test]
    fn snapshot_loads_an_unversioned_document_as_v1() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        std::fs::write(
            dir.path().join("queue.json"),
            br#"{"jobs":[],"next_job_id":7,"next_file_id":9,"download_paused":false,"speed_limit_bps":null}"#,
        )
        .unwrap();

        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.schema_version, 1);
        assert_eq!(loaded.next_job_id, 7);
        assert_eq!(loaded.next_file_id, 9);
        assert_eq!(loaded.max_active_downloads, 0);
    }

    #[test]
    fn snapshot_loads_schema_2_jobs_without_changing_usenet_meaning() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        let mut value = serde_json::to_value(QueueSnapshotDoc {
            schema_version: 2,
            pending_admissions: Vec::new(),
            jobs: vec![minimal_job()],
            next_job_id: 7,
            next_file_id: 0,
            download_paused: false,
            speed_limit_bps: None,
            max_active_downloads: 1,
        })
        .unwrap();
        value["jobs"][0].as_object_mut().unwrap().remove("torrent");
        std::fs::write(
            dir.path().join("queue.json"),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();

        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.schema_version, 2);
        assert_eq!(loaded.jobs.len(), 1);
        assert_eq!(loaded.jobs[0].kind, nzbd_types::JobKind::Nzb);
        assert!(loaded.jobs[0].torrent.is_none());
        assert_eq!(loaded.jobs[0].status, nzbd_types::JobStatus::Paused);
    }

    #[test]
    fn schema_3_migrates_with_no_pending_admissions() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        std::fs::write(dir.path().join("queue.json"), br#"{"schema_version":3,"jobs":[],"next_job_id":2,"next_file_id":0,"download_paused":false,"speed_limit_bps":null,"max_active_downloads":1}"#).unwrap();
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.schema_version, 3);
        assert!(loaded.pending_admissions.is_empty());
    }

    #[test]
    fn snapshot_roundtrips_a_non_empty_schema_4_pending_admission() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        let pending = PendingAdmission {
            job_id: JobId(7),
            source: nzbd_types::TorrentSource::Magnet,
            secret_ref: "torrents/pending/7.source".into(),
            category: Some("tv".into()),
            priority: 10,
            paused: true,
            seed_ratio_limit: Some(1.5),
            seed_time_limit_secs: Some(3_600),
            stop_seeding_on_complete: Some(true),
            params: vec![("source".into(), "test".into())],
            client: Some("Sonarr".into()),
        };
        store
            .save(&QueueSnapshotDoc {
                pending_admissions: vec![pending.clone()],
                ..QueueSnapshotDoc::default()
            })
            .unwrap();

        let raw: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("queue.json")).unwrap()).unwrap();
        assert_eq!(raw["schema_version"], QUEUE_SCHEMA_VERSION);
        assert_eq!(raw["pending_admissions"].as_array().unwrap().len(), 1);
        assert_eq!(
            store.load().unwrap().unwrap().pending_admissions,
            vec![pending]
        );
    }

    #[test]
    fn snapshot_roundtrips_schema_4_torrent_control_state() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        let mut job = minimal_job();
        job.kind = nzbd_types::JobKind::Torrent;
        job.name = "torrent".into();
        job.torrent = Some(nzbd_types::TorrentRecord {
            info_hash_v1: "0123456789abcdef0123456789abcdef01234567".into(),
            source: nzbd_types::TorrentSource::Metainfo,
            metadata_file: "meta/0123.torrent".into(),
            payload_root: PathBuf::new(),
            phase: nzbd_types::TorrentPhase::PausedDownload,
            control_intent: nzbd_types::TorrentControlIntent::Paused,
            removal_intent: None,
            removal_outcome: None,
            removal_confirmed_at_unix: None,
            stop_reason: None,
            files: vec![nzbd_types::TorrentFileRecord {
                path: "payload.bin".into(),
                length: 42,
                selected: true,
                downloaded_bytes: 0,
            }],
            total_bytes: 42,
            selected_bytes: 42,
            downloaded_bytes: 21,
            uploaded_bytes: 5,
            seeding_seconds: 0,
            ready_at_unix: None,
            content_path: None,
            seed_policy: nzbd_types::SeedPolicy::default(),
            last_activity_unix: Some(1_800_000_000),
            last_error: None,
        });
        store
            .save(&QueueSnapshotDoc {
                jobs: vec![job],
                next_job_id: 7,
                max_active_downloads: 1,
                ..Default::default()
            })
            .unwrap();

        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.schema_version, 4);
        let torrent = loaded.jobs[0].torrent.as_ref().unwrap();
        assert_eq!(torrent.downloaded_bytes, 21);
        assert_eq!(torrent.files[0].path, PathBuf::from("payload.bin"));

        // This envelope is the downgrade guard: a schema-2 binary rejects
        // the document before it can misread or skip the torrent variant.
        let raw: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("queue.json")).unwrap()).unwrap();
        assert_eq!(raw["schema_version"], 4);
    }

    #[test]
    fn snapshot_reports_future_schema_instead_of_nested_job_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        std::fs::write(
            dir.path().join("queue.json"),
            br#"{"schema_version":5,"jobs":[{"kind":"future_transfer"}]}"#,
        )
        .unwrap();

        let err = store.load().unwrap_err().to_string();
        assert!(
            err.contains("queue.json schema version 5 is newer than this nzbd supports (4)"),
            "unexpected error: {err}"
        );
        assert!(
            !err.contains("unknown variant"),
            "version fallback must win over a nested enum error: {err}"
        );
    }

    #[test]
    fn snapshot_refuses_to_write_a_noncurrent_schema() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        let doc = QueueSnapshotDoc {
            schema_version: 1,
            ..Default::default()
        };

        let err = store.save(&doc).unwrap_err().to_string();
        assert!(
            err.contains("refusing to write queue schema version 1; this nzbd writes 4"),
            "unexpected error: {err}"
        );
        assert!(!dir.path().join("queue.json").exists());
        assert!(!dir.path().join("queue.json.tmp").exists());
    }

    /// A production-shaped queue (hundreds of thousands of segments) must
    /// round-trip through the snapshot store. The size this doc reaches is
    /// why the save path pre-encodes and writes ONCE — the field failure
    /// this pins was ~30 MiB of JSON emitted as millions of token-sized
    /// write syscalls onto a FUSE volume, 65 s per save.
    #[test]
    fn snapshot_survives_a_production_sized_queue() {
        use nzbd_types::{DupeInfo, FileEntry, Job, JobKind, JobStatus, JobTotals, SegmentState};
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        let seg = |n: u32| nzbd_types::Segment {
            message_id: format!("part{n}of9999.deadbeefcafebabe{n:08x}@news.example.org").into(),
            number: n,
            size: 750_000,
            state: if n.is_multiple_of(3) {
                SegmentState::Done {
                    offset: n as u64 * 750_000,
                    len: 750_000,
                    crc: 0xC0FFEE ^ n,
                }
            } else {
                SegmentState::Pending
            },
        };
        let file = |fid: u32| FileEntry {
            id: FileId(fid),
            subject: format!("a file {fid}"),
            filename: format!("linux-{fid:04}.r{:02}", fid % 100),
            filename_confirmed: true,
            is_par2: fid.is_multiple_of(7),
            paused: false,
            groups: vec!["alt.binaries.example".into()],
            date: Some(1_753_000_000),
            segments: (0..500).map(seg).collect(),
            crc32: None,
            finalized: false,
        };
        let doc = QueueSnapshotDoc {
            schema_version: QUEUE_SCHEMA_VERSION,
            pending_admissions: Vec::new(),
            jobs: (0..4u32)
                .map(|j| Job {
                    id: JobId(j),
                    kind: JobKind::Nzb,
                    name: format!("job {j}"),
                    dir_name: String::new(),
                    name_provisional: false,
                    queued_at_unix: 0,
                    original_name: String::new(),
                    category: Some("tv".into()),
                    priority: 0,
                    dupe: DupeInfo::default(),
                    params: vec![("drone".into(), format!("id-{j}"))],
                    files: (j * 100..j * 100 + 100).map(file).collect(),
                    totals: JobTotals::default(),
                    status: JobStatus::Downloading,
                    torrent: None,
                    stages: Vec::new(),
                })
                .collect(),
            next_job_id: 5,
            next_file_id: 400,
            download_paused: false,
            speed_limit_bps: None,
            max_active_downloads: 1,
        };
        // 4 jobs × 100 files × 500 segments = 200k segments.
        let bytes = store.save(&doc).unwrap();
        assert!(bytes > 10 << 20, "doc is production-sized ({bytes} B)");
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.jobs.len(), 4);
        assert_eq!(loaded.jobs[3].files.len(), 100);
        assert_eq!(loaded.jobs[3].files[99].segments.len(), 500);
        assert_eq!(
            loaded.jobs[1].files[0].segments[0].message_id,
            doc.jobs[1].files[0].segments[0].message_id
        );
    }

    #[test]
    fn job_journals_union_across_lease_files() {
        let dir = tempfile::tempdir().unwrap();

        // Two writers (two lease suffixes) journal different segments of
        // the same job — e.g. a worker died and the job was reclaimed.
        let mut a = JobJournals::open(dir.path(), "L1-1").unwrap();
        a.append(&rec(0)).unwrap();
        a.append(&rec(1)).unwrap();
        a.sync().unwrap();
        let mut b = JobJournals::open(dir.path(), "L2-9").unwrap();
        b.append(&rec(2)).unwrap();
        b.append(&rec(1)).unwrap(); // duplicate: union is idempotent upstream
        b.sync().unwrap();

        let recs = JobJournals::replay_job(dir.path(), JobId(1)).unwrap();
        assert_eq!(recs.len(), 4, "union of both lease files");
        let segs: std::collections::BTreeSet<u32> = recs.iter().map(|r| r.segment_number).collect();
        assert_eq!(segs.into_iter().collect::<Vec<_>>(), vec![0, 1, 2]);

        assert_eq!(JobJournals::replay_all(dir.path()).unwrap().len(), 4);

        // Authority folds + removes.
        a.remove_job(JobId(1)).unwrap();
        assert!(JobJournals::replay_job(dir.path(), JobId(1))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn snapshot_guard_fences_the_commit() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        store
            .save(&QueueSnapshotDoc {
                next_job_id: 1,
                ..Default::default()
            })
            .unwrap();

        let err = store
            .save_guarded(
                &QueueSnapshotDoc {
                    next_job_id: 99,
                    ..Default::default()
                },
                &|| false, // deposed between write and commit
            )
            .unwrap_err();
        assert!(matches!(err, StateError::Corrupt(_)));
        // The old snapshot survives untouched.
        assert_eq!(store.load().unwrap().unwrap().next_job_id, 1);
    }

    #[test]
    fn unclean_marker_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let m = UncleanMarker::new(dir.path(), "local");
        assert!(!m.check_and_arm().unwrap(), "first run is clean");
        assert!(
            m.check_and_arm().unwrap(),
            "second arm without disarm = unclean"
        );
        m.disarm().unwrap();
        assert!(!m.check_and_arm().unwrap(), "after disarm = clean");
        m.disarm().unwrap();
        m.disarm().unwrap(); // idempotent
    }

    // -----------------------------------------------------------------
    // Error reporting. A state error that names only an errno costs an
    // operator an afternoon; these lock the path into the message.
    // -----------------------------------------------------------------

    #[test]
    fn error_display_names_op_path_and_cause() {
        let e = StateError::Io {
            op: "create directory",
            path: PathBuf::from("/data/usenet/queue"),
            source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        };
        assert!(
            e.to_string()
                .starts_with("create directory /data/usenet/queue: "),
            "{e}"
        );
        assert_eq!(e.path(), Some(Path::new("/data/usenet/queue")));
        assert!(e.is_permission_denied());
        assert!(!e.is_not_found());
        // Non-io variants have no path and must not claim one.
        assert_eq!(StateError::Corrupt("x".into()).path(), None);
        assert!(!StateError::Corrupt("x".into()).is_permission_denied());
    }

    /// Every startup entry point must name the directory it choked on.
    /// A regular file standing where the state dir belongs fails for real
    /// on every platform, privileged or not.
    #[test]
    fn startup_paths_report_the_directory_they_failed_on() {
        let dir = tempfile::tempdir().unwrap();
        let blocked = dir.path().join("queue");
        std::fs::write(&blocked, b"a file, not a directory").unwrap();
        let shown = blocked.display().to_string();

        let err = err_of(UncleanMarker::new(&blocked, "local").check_and_arm());
        assert!(err.to_string().contains(&shown), "unclean marker: {err}");

        let err = err_of(SnapshotStore::open(&blocked));
        assert!(err.to_string().contains(&shown), "snapshot store: {err}");
        assert_eq!(err.path(), Some(blocked.as_path()));

        let err = err_of(JobJournals::open(&blocked, "local"));
        assert!(err.to_string().contains(&shown), "job journals: {err}");

        let err = err_of(FsJournal::open(&blocked));
        assert!(err.to_string().contains(&shown), "segment journal: {err}");
    }

    /// The reported bug, reproduced: a state dir the daemon may read but
    /// not write. Every layer must say which path, not just EACCES.
    #[cfg(unix)]
    #[test]
    fn permission_denied_names_the_unwritable_path() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("queue");
        std::fs::create_dir(&state).unwrap();
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o500)).unwrap();

        // root ignores mode bits — skip rather than report a false pass.
        if std::fs::write(state.join(".probe"), b"").is_ok() {
            eprintln!("skipping permission test: running privileged");
            return;
        }

        let marker_path = state.join("unclean.local");
        let err = err_of(UncleanMarker::new(&state, "local").check_and_arm());
        assert!(err.is_permission_denied(), "{err}");
        assert_eq!(err.path(), Some(marker_path.as_path()));
        assert!(
            err.to_string().contains(&state.display().to_string()),
            "operator must be able to see what to chmod: {err}"
        );

        // Restore so tempdir cleanup can remove it.
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    /// Guard rail for the invariant above: a future `std::fs` call added
    /// straight into this crate would silently drop the path again.
    #[test]
    fn all_filesystem_calls_go_through_fsx() {
        for (name, src) in [
            ("lib.rs", include_str!("lib.rs")),
            ("history.rs", include_str!("history.rs")),
        ] {
            // Test fixtures may use std::fs freely.
            let code = src.split("#[cfg(test)]").next().unwrap();
            for (i, line) in code.lines().enumerate() {
                let trimmed = line.trim_start();
                // Comments and imports are fine; call sites are not.
                if trimmed.starts_with("//") || trimmed.starts_with("use ") {
                    continue;
                }
                for banned in ["std::fs::", "OpenOptions"] {
                    assert!(
                        !line.contains(banned),
                        "{name}:{} calls the filesystem directly ({banned}) — route it \
                         through fsx so failures name the path:\n  {line}",
                        i + 1
                    );
                }
            }
        }
    }
}
