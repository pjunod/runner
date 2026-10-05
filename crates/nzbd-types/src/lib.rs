//! Domain model for nzbd. No I/O lives here.
//!
//! Naming maps to the NZBGet reference implementation where behavior is
//! carried over (see docs/ARCHITECTURE.md §3): `Job` ≈ NzbInfo,
//! `FileEntry` ≈ FileInfo, `Segment` ≈ ArticleInfo.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub mod metrics;

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FileId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ServerId(pub u32);

// ---------------------------------------------------------------------------
// Priorities (NZBGet-compatible scale; 900 == Force, ignores pause/quota)
// ---------------------------------------------------------------------------

pub const PRIORITY_FORCE: i32 = 900;

// ---------------------------------------------------------------------------
// Jobs, files, segments
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    #[default]
    Nzb,
    Url,
    Torrent,
}

/// Durable origin of a BitTorrent job. Secrets such as source URLs and
/// tracker passkeys do not belong here; they are stored through the secret
/// boundary and only a non-secret source class reaches the queue snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentSource {
    Metainfo,
    Magnet,
    Url,
}

/// Durable BitTorrent lifecycle state. Volatile peer/tracker counters stay in
/// the backend and its detail projection rather than turning queue snapshots
/// into a high-frequency engine database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentPhase {
    FetchingSource,
    FetchingMetadata,
    Queued,
    Checking,
    Downloading,
    Seeding,
    PausedDownload,
    PausedSeed,
    MissingFiles,
    Failed,
}

/// The last pause/resume request durably accepted by the queue owner.
///
/// This is deliberately separate from [`TorrentPhase`]: phases describe what
/// the backend has observed, while this is the queue's authoritative request
/// that survives a restart before the backend has caught up.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentControlIntent {
    #[default]
    Running,
    Paused,
}

/// The queue's authoritative request to remove a torrent after the backend
/// has stopped its engine handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TorrentRemovalIntent {
    pub delete_data: bool,
}

/// Confirmed disposition of a torrent payload after its engine handle was
/// removed. Persisting this before terminal history is written makes the
/// final transition retryable without repeating filesystem deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentPayloadDisposition {
    Retained,
    Deleted,
}

impl TorrentPhase {
    /// Whether this phase may compete for a shared active-download slot.
    pub fn wants_download_slot(self) -> bool {
        matches!(
            self,
            Self::FetchingSource
                | Self::FetchingMetadata
                | Self::Queued
                | Self::Checking
                | Self::Downloading
        )
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct SeedPolicy {
    /// Stop once the selected payload is verified. Zero numeric limits still
    /// mean unlimited, so this intent must be represented independently.
    #[serde(default)]
    pub stop_on_complete: bool,
    pub ratio_limit: Option<f64>,
    pub time_limit_secs: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentStopReason {
    Manual,
    DownloadComplete,
    RatioLimit,
    TimeLimit,
    StorageFull,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TorrentFileRecord {
    pub path: PathBuf,
    pub length: u64,
    pub selected: bool,
    /// Hash-verified bytes retained as an advisory per-file checkpoint.
    #[serde(default)]
    pub downloaded_bytes: u64,
}

/// Queue-owned BitTorrent control state. Engine resume data (piece maps,
/// peers, tracker timers) remains an accelerator below this record and may
/// never admit a job on its own.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TorrentRecord {
    /// Lowercase 40-character v1 info hash.
    pub info_hash_v1: String,
    pub source: TorrentSource,
    /// Relative to the configured torrent state root.
    pub metadata_file: PathBuf,
    /// Canonical configured root selected for this job at admission.
    #[serde(default)]
    pub payload_root: PathBuf,
    pub phase: TorrentPhase,
    /// Defaulted so records written before owner-side control routing retain
    /// their historical (running) meaning.
    #[serde(default)]
    pub control_intent: TorrentControlIntent,
    /// Defaulted so rows written before durable removal routing remain valid.
    #[serde(default)]
    pub removal_intent: Option<TorrentRemovalIntent>,
    /// Set only after the backend has confirmed removal. A queue record with
    /// this value is waiting solely for its durable terminal history write.
    #[serde(default)]
    pub removal_outcome: Option<TorrentPayloadDisposition>,
    /// Stable terminal-history key captured with `removal_outcome`.
    #[serde(default)]
    pub removal_confirmed_at_unix: Option<i64>,
    pub files: Vec<TorrentFileRecord>,
    pub total_bytes: u64,
    pub selected_bytes: u64,
    /// Advisory checkpoint; piece verification remains authoritative.
    pub downloaded_bytes: u64,
    /// Cumulative across restarts.
    pub uploaded_bytes: u64,
    /// Cumulative across restarts.
    pub seeding_seconds: u64,
    pub ready_at_unix: Option<i64>,
    /// Canonical payload path after verification.
    pub content_path: Option<PathBuf>,
    pub seed_policy: SeedPolicy,
    #[serde(default)]
    pub stop_reason: Option<TorrentStopReason>,
    pub last_activity_unix: Option<i64>,
    /// Redacted, display-safe, and bounded by the backend before persistence.
    pub last_error: Option<String>,
}

#[cfg(test)]
mod torrent_control_tests {
    use super::*;

    #[test]
    fn absent_control_intent_reads_as_running() {
        let record: TorrentRecord = serde_json::from_str(
            r#"{
                "info_hash_v1":"0123456789abcdef0123456789abcdef01234567",
                "source":"metainfo",
                "metadata_file":"meta/example.torrent",
                "phase":"paused_download",
                "files":[],
                "total_bytes":1,
                "selected_bytes":1,
                "downloaded_bytes":0,
                "uploaded_bytes":0,
                "seeding_seconds":0,
                "ready_at_unix":null,
                "content_path":null,
                "seed_policy":{"ratio_limit":null,"time_limit_secs":null},
                "last_activity_unix":null,
                "last_error":null
            }"#,
        )
        .unwrap();
        assert_eq!(record.control_intent, TorrentControlIntent::Running);
        assert_eq!(record.removal_intent, None);
        assert!(!record.seed_policy.stop_on_complete);
        assert_eq!(record.stop_reason, None);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DupeMode {
    Score,
    All,
    Force,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DupeInfo {
    pub key: String,
    pub score: i32,
    pub mode: Option<DupeMode>,
}

/// Byte/article accounting for a job. All sizes in bytes.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct JobTotals {
    pub size: u64,
    pub par_size: u64,
    pub failed_size: u64,
    pub failed_par_size: u64,
    pub success_size: u64,
    pub total_articles: u32,
    pub success_articles: u32,
    pub failed_articles: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentState {
    Pending,
    Leased {
        server: ServerId,
    },
    /// Decoded and written. `offset`/`len` are positions in the output file
    /// (yEnc `begin - 1` / part length); `crc` is the decoded part CRC32.
    Done {
        offset: u64,
        len: u32,
        crc: u32,
    },
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub message_id: Box<str>,
    pub number: u32,
    /// Size from the NZB `<segment bytes=..>` attribute (encoded size, advisory).
    pub size: u32,
    pub state: SegmentState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub id: FileId,
    pub subject: String,
    pub filename: String,
    pub filename_confirmed: bool,
    pub is_par2: bool,
    pub paused: bool,
    pub groups: Vec<String>,
    /// Post date (unix) from the NZB — drives retention pre-fail and
    /// `PropagationDelay`.
    #[serde(default)]
    pub date: Option<i64>,
    pub segments: Vec<Segment>,
    /// Combined CRC32 of the decoded file, available once all segments are done.
    pub crc32: Option<u32>,
    /// Download writer finished: published intact, or privately sealed for repair
    /// when the job carries a `*File:repair:<id>` parameter.
    #[serde(default)]
    pub finalized: bool,
}

impl FileEntry {
    /// All segments in a terminal state (done or failed).
    pub fn is_terminal(&self) -> bool {
        self.segments
            .iter()
            .all(|s| matches!(s.state, SegmentState::Done { .. } | SegmentState::Failed))
    }

    pub fn done_segments(&self) -> usize {
        self.segments
            .iter()
            .filter(|s| matches!(s.state, SegmentState::Done { .. }))
            .count()
    }

    pub fn has_any_done(&self) -> bool {
        self.segments
            .iter()
            .any(|s| matches!(s.state, SegmentState::Done { .. }))
    }
}

// ---------------------------------------------------------------------------
// Servers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsMode {
    None,
    Tls,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertLevel {
    None,
    Minimal,
    Strict,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerDef {
    pub id: ServerId,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub tls: TlsMode,
    pub username: Option<String>,
    pub password: Option<String>,
    pub active: bool,
    /// Normalized failover tier (NZBGet "Level"): 0 = main, 1 = first backup…
    pub tier: u8,
    /// Servers with the same (tier, group>0) are interchangeable: a
    /// per-article failure on one skips the whole group.
    pub group: u8,
    /// Fill server (NZBGet "Optional"): when blocked, never stalls progress —
    /// selection falls through to the next tier instead of waiting.
    pub fill: bool,
    pub max_connections: u16,
    /// NNTP command pipelining depth (first-class, unlike NZBGet). 1 = off.
    pub pipeline_depth: u8,
    /// 0 = unlimited. Articles older than this are pre-failed on this server.
    pub retention_days: u32,
    pub cert_verification: CertLevel,
}

// ---------------------------------------------------------------------------
// Health (per-mille), formulas carried exactly from NZBGet
// (daemon/queue/DownloadInfo.cpp — CalcHealth / CalcCriticalHealth)
// ---------------------------------------------------------------------------

/// Health of a download in per-mille (0..=1000). 1000 = 100.0%.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Health(pub u16);

impl Health {
    pub const PERFECT: Health = Health(1000);

    /// Fraction of *non-par* data successfully downloaded, per-mille.
    ///
    /// `health = (size − parSize − (failed − parFailed)) × 1000 / (size − parSize)`,
    /// clamped to 999 if any non-par bytes failed, 1000 iff nothing failed.
    pub fn calc(t: &JobTotals) -> Health {
        let non_par_failed = t.failed_size.saturating_sub(t.failed_par_size);
        if non_par_failed == 0 {
            return Health::PERFECT;
        }
        let denom = t.size.saturating_sub(t.par_size);
        if denom == 0 {
            return Health(0);
        }
        let raw = denom.saturating_sub(non_par_failed).saturating_mul(1000) / denom;
        Health(raw.min(999) as u16)
    }

    /// The health threshold below which par-repair is hopeless.
    ///
    /// `goodPar = parSize − parFailed`;
    /// 0 when par data ≥ half of total (repair always feasible);
    /// else `(size − 2·goodPar) × 1000 / (size − goodPar)`;
    /// 850 as an empirical fallback when the result is 1000 and estimation is
    /// allowed (guards against renamed/undetected par files).
    pub fn calc_critical(t: &JobTotals, allow_estimation: bool) -> Health {
        let good_par = t.par_size.saturating_sub(t.failed_par_size);
        if t.size == 0 || good_par.saturating_mul(2) >= t.size {
            return Health(0);
        }
        let denom = t.size - good_par; // > 0 because good_par*2 < size
        let raw = (t.size - 2 * good_par).saturating_mul(1000) / denom;
        let raw = raw.min(1000) as u16;
        if raw == 1000 && allow_estimation {
            Health(850)
        } else {
            Health(raw)
        }
    }
}

// ---------------------------------------------------------------------------
// Job status (native vocabulary; compat strings live in nzbd-compat)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostStage {
    ParRename,
    ParVerify,
    ParRepair,
    RarRename,
    Unpack,
    Cleanup,
    Move,
    PostUnpackRename,
    Script,
}

impl PostStage {
    /// The wire name — the same snake_case spelling serde produces, but as
    /// a `&'static str` so event payloads and metric labels cannot drift
    /// from the serialized form.
    pub fn as_str(&self) -> &'static str {
        match self {
            PostStage::ParRename => "par_rename",
            PostStage::ParVerify => "par_verify",
            PostStage::ParRepair => "par_repair",
            PostStage::RarRename => "rar_rename",
            PostStage::Unpack => "unpack",
            PostStage::Cleanup => "cleanup",
            PostStage::Move => "move",
            PostStage::PostUnpackRename => "post_unpack_rename",
            PostStage::Script => "script",
        }
    }

    /// Every variant, in pipeline order. Metrics exposition walks this, so
    /// a stage added above appears on `/metrics` without a second edit.
    pub const ALL: [PostStage; 9] = [
        PostStage::ParRename,
        PostStage::ParVerify,
        PostStage::ParRepair,
        PostStage::RarRename,
        PostStage::Unpack,
        PostStage::Cleanup,
        PostStage::Move,
        PostStage::PostUnpackRename,
        PostStage::Script,
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Downloading,
    Paused,
    Fetching, // URL fetch
    PostQueued,
    Post { stage: PostStage },
    Completed,
    Failed,
    Deleted,
}

/// One stage a job passed through, and how long it stayed there.
///
/// The post manager has always measured this — `Stages::enter` stamps an
/// `Instant` on every transition — and then banked it only into the
/// process-wide histogram, so the per-job number was computed and thrown
/// away at the same seam. "Why did THIS one take forty minutes" was
/// unanswerable from a number that had already been averaged with every
/// other job. Recording the span on the job is what makes the queue row's
/// stage timer, the detail pipeline and the history breakdown possible;
/// all three read this one list.
///
/// `ms` is `None` while the stage is still running: the UI shows a live
/// timer from `started_at_unix` and switches to the banked figure when the
/// stage closes, so a restart mid-repair does not invent a duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageSpan {
    pub stage: PostStage,
    pub started_at_unix: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ms: Option<u64>,
}

/// Job param stamped by post-processing when it finishes (value = final
/// PP status). Its presence means "never post-process this job again" —
/// across restarts, leader failovers and lease reclaims.
pub const PP_DONE_PARAM: &str = "*PP:done";

/// Versioned nonterminal control. Revisions are decimal strings on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobControl {
    pub version: u32,
    pub revision: String,
    pub lifecycle: String,
    pub cause: String,
    pub stage: String,
    pub retry_policy: String,
    pub message: String,
    pub instance: String,
    #[serde(default)]
    pub previous_status: Option<JobStatus>,
    #[serde(default)]
    pub manual_pause: bool,
}
pub const CONTROL_PARAM: &str = "*Control:v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    pub kind: JobKind,
    /// The **display** name. Free to change after admission: an obfuscated
    /// post names itself from its own par2 metadata the moment that lands,
    /// so this is not a stable identifier and nothing on disk may be keyed
    /// to it. Use [`Job::dir_name`] for storage.
    pub name: String,
    /// The **storage** name: the directory this job's files live under,
    /// fixed at admission and never changed.
    ///
    /// Split from `name` because renaming a job mid-download used to move
    /// its destination out from under the writers — the directory is
    /// recomputed from the name every time a writer spawns, so half the
    /// files would land in the old directory and half in the new one. A
    /// display name that can improve and a path that must not are two
    /// different things.
    ///
    /// Empty on a snapshot written before this existed; read it through
    /// `queue::job_dir_name`, which falls back to sanitizing `name` — the
    /// exact behaviour those jobs already had.
    #[serde(default)]
    pub dir_name: String,
    /// True while `name` is a stand-in rather than something the job's own
    /// documents said.
    ///
    /// Tracked as a flag instead of re-derived from the string, because a
    /// good placeholder is indistinguishable from a real title by
    /// inspection — `monarr · drunkenslug · cc310b99` reads as informative,
    /// which is the point of it, and a gate that asked "does this look
    /// like junk?" would refuse the real name when it finally arrived.
    #[serde(default)]
    pub name_provisional: bool,
    /// When this job entered the queue. With the history entry's
    /// completion time this is how long the whole thing took — the one
    /// duration nothing else records. `0` on snapshots written before it
    /// existed.
    #[serde(default)]
    pub queued_at_unix: i64,
    /// The name the job was admitted under, kept when it later renames
    /// itself from its own metadata so the *arr's original reference stays
    /// findable. Empty when the job never renamed.
    #[serde(default)]
    pub original_name: String,
    pub category: Option<String>,
    pub priority: i32,
    pub dupe: DupeInfo,
    /// Post-processing parameters, including e.g. Sonarr's `drone` tracking id.
    pub params: Vec<(String, String)>,
    pub files: Vec<FileEntry>,
    pub totals: JobTotals,
    pub status: JobStatus,
    /// Present only for [`JobKind::Torrent`]. Defaulted so version-1/2 queue
    /// documents retain their exact Usenet meaning when read by schema 3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub torrent: Option<TorrentRecord>,
    /// Post-processing stages this job has entered, in order. Appended on
    /// every transition and persisted with the snapshot, so a daemon
    /// restart mid-pipeline does not erase where the time went. Defaulted
    /// on read: a queue.json written before this existed loads with an
    /// empty timeline rather than failing to parse.
    #[serde(default)]
    pub stages: Vec<StageSpan>,
}

impl Job {
    pub fn control(&self) -> Option<JobControl> {
        self.params
            .iter()
            .find(|(k, _)| k == CONTROL_PARAM)
            .and_then(|(_, v)| serde_json::from_str(v).ok())
    }
    pub fn held(&self) -> bool {
        self.control()
            .is_some_and(|c| c.lifecycle == "held" || c.version != 1)
    }
    pub fn set_control(&mut self, control: &JobControl) {
        self.params.retain(|(k, _)| k != CONTROL_PARAM);
        self.params.push((
            CONTROL_PARAM.into(),
            serde_json::to_string(control).expect("control serializes"),
        ));
    }

    pub fn force_priority(&self) -> bool {
        self.priority >= PRIORITY_FORCE
    }

    /// A protocol-neutral successful-payload readiness fact for native
    /// consumers. Usenet jobs become ready only after a successful durable
    /// post-processing stamp; torrents become ready only after their selected
    /// payload passes piece checks.
    pub fn ready_at_unix(&self) -> Option<i64> {
        self.torrent
            .as_ref()
            .and_then(|torrent| torrent.ready_at_unix)
    }

    /// A terminal download checkpoint retained privately for PAR verification.
    pub fn file_needs_repair(&self, file: FileId) -> bool {
        self.params
            .iter()
            .any(|(key, _)| key == &format!("*File:repair:{}", file.0))
    }

    pub fn ready(&self) -> bool {
        if self.held() {
            return false;
        }
        match self.kind {
            JobKind::Torrent => self.ready_at_unix().is_some(),
            JobKind::Nzb | JobKind::Url => self
                .params
                .iter()
                .any(|(key, value)| key == PP_DONE_PARAM && value == "SUCCESS"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn totals(size: u64, par: u64, failed: u64, par_failed: u64) -> JobTotals {
        JobTotals {
            size,
            par_size: par,
            failed_size: failed,
            failed_par_size: par_failed,
            ..Default::default()
        }
    }

    #[test]
    fn health_perfect_when_nothing_failed() {
        assert_eq!(Health::calc(&totals(1000, 200, 0, 0)), Health(1000));
        // par failures alone don't reduce health (non-par data is intact)
        assert_eq!(Health::calc(&totals(1000, 200, 50, 50)), Health(1000));
    }

    #[test]
    fn health_clamps_to_999_on_any_nonpar_failure() {
        // 1 byte of non-par failure out of 800 non-par bytes: ratio rounds to
        // 998 (floor division), and must never report 1000.
        let h = Health::calc(&totals(1000, 200, 1, 0));
        assert!(h < Health(1000) && h >= Health(998), "got {h:?}");
    }

    #[test]
    fn health_proportional() {
        // non-par = 800, non-par failed = 400 -> 500 per-mille
        assert_eq!(Health::calc(&totals(1000, 200, 400, 0)), Health(500));
        // everything non-par failed -> 0
        assert_eq!(Health::calc(&totals(1000, 200, 800, 0)), Health(0));
    }

    #[test]
    fn critical_health_zero_when_par_covers_half() {
        // good par (500) * 2 >= size (1000): repair always feasible
        assert_eq!(
            Health::calc_critical(&totals(1000, 500, 0, 0), false),
            Health(0)
        );
    }

    #[test]
    fn critical_health_formula() {
        // size=1000, goodPar=200 -> (1000-400)*1000/(1000-200) = 750
        assert_eq!(
            Health::calc_critical(&totals(1000, 200, 0, 0), false),
            Health(750)
        );
        // failed par shrinks goodPar: par=200 with 100 failed -> goodPar=100
        // (1000-200)*1000/(1000-100) = 888
        assert_eq!(
            Health::calc_critical(&totals(1000, 200, 100, 100), false),
            Health(888)
        );
    }

    #[test]
    fn critical_health_estimation_fallback() {
        // No par at all -> raw = 1000; with estimation allowed -> 850
        assert_eq!(
            Health::calc_critical(&totals(1000, 0, 0, 0), true),
            Health(850)
        );
        assert_eq!(
            Health::calc_critical(&totals(1000, 0, 0, 0), false),
            Health(1000)
        );
    }

    #[test]
    fn force_priority_threshold() {
        let mut job = Job {
            id: JobId(1),
            kind: JobKind::Nzb,
            name: "x".into(),
            dir_name: String::new(),
            name_provisional: false,
            queued_at_unix: 0,
            original_name: String::new(),
            category: None,
            priority: 100,
            dupe: DupeInfo::default(),
            params: vec![],
            files: vec![],
            totals: JobTotals::default(),
            status: JobStatus::Queued,
            torrent: None,
            stages: Vec::new(),
        };
        assert!(!job.force_priority());
        job.priority = PRIORITY_FORCE;
        assert!(job.force_priority());
    }

    #[test]
    fn usenet_ready_requires_successful_post_processing() {
        let mut job = Job {
            id: JobId(1),
            kind: JobKind::Nzb,
            name: "x".into(),
            dir_name: String::new(),
            name_provisional: false,
            queued_at_unix: 0,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: DupeInfo::default(),
            params: vec![],
            files: vec![],
            totals: JobTotals::default(),
            status: JobStatus::Completed,
            torrent: None,
            stages: Vec::new(),
        };
        for failure in ["PAR_FAILURE", "UNPACK_FAILURE", "FAILURE/HEALTH"] {
            job.params = vec![(PP_DONE_PARAM.into(), failure.into())];
            assert!(!job.ready(), "{failure} is terminal, not ready");
        }
        job.params = vec![(PP_DONE_PARAM.into(), "SUCCESS_FAILURE".into())];
        assert!(
            !job.ready(),
            "a success-prefixed corrupt stamp must fail closed"
        );
        job.params = vec![(PP_DONE_PARAM.into(), "SUCCESS".into())];
        assert!(job.ready());
    }
    #[test]
    fn control_v1_golden_keeps_decimal_revision_and_ignores_future_fields() {
        let value: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/job-control-v1.json")).unwrap();
        let control: JobControl = serde_json::from_value(value.clone()).unwrap();
        let wire = serde_json::to_value(control).unwrap();
        for key in [
            "version",
            "revision",
            "lifecycle",
            "cause",
            "stage",
            "retry_policy",
            "message",
            "instance",
        ] {
            assert_eq!(wire[key], value[key]);
        }
    }
}
