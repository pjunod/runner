//! The lock-free read model. The queue-owner task publishes an immutable
//! [`QueueSnapshot`] via `arc-swap` (debounced to its 1 Hz tick plus
//! structural changes); API handlers load it without ever blocking the
//! engine (ARCHITECTURE.md §8.1).

use arc_swap::ArcSwap;
use nzbd_types::{JobId, JobKind, JobStatus, StageSpan};
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairPhase {
    Matching,
    Preparing,
    Verifying,
    FetchingRecovery,
    Reconstructing,
    Validating,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RepairProgress {
    pub attempt_id: String,
    pub phase: RepairPhase,
    pub files_done: u32,
    pub files_total: u32,
    pub bytes_scanned: u64,
    pub round: u32,
    pub recovery_blocks_available: u32,
    pub additional_blocks_needed: Option<u32>,
    pub last_progress_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repair_progress: Option<RepairProgress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub control: Option<nzbd_types::JobControl>,
    pub id: JobId,
    pub kind: JobKind,
    pub name: String,
    /// Coarse transfer lifecycle. `Completed` says the article download is
    /// complete; it does not by itself say post-processing is complete. Use
    /// `ready` for that integration boundary.
    pub status: JobStatus,
    pub category: Option<String>,
    pub priority: i32,
    pub size_bytes: u64,
    pub downloaded_bytes: u64,
    pub failed_bytes: u64,
    pub remaining_bytes: u64,
    pub total_articles: u32,
    pub done_articles: u32,
    pub failed_articles: u32,
    pub files_total: u32,
    pub files_done: u32,
    /// Per-mille (NZBGet scale: 1000 = 100.0%).
    pub health: u16,
    pub critical_health: u16,
    /// True when the threshold is a fallback because no usable PAR bytes are known.
    pub critical_health_estimated: bool,
    /// This job's current download rate (EMA, bytes/sec; 0 unless
    /// actively downloading). For local jobs this is WIRE bytes — the
    /// same measurement as the queue-wide rate, so the two never
    /// structurally disagree.
    pub rate_bps: u64,
    /// Article download attempts that failed and were retried — the gap
    /// between wire throughput and completed bytes, made visible.
    pub retried_articles: u32,
    /// Cluster: node currently executing this job remotely (None = local).
    pub assigned_node: Option<String>,
    /// Post-processing already finished (the `*PP:done` stamp is present).
    pub pp_done: bool,
    /// Protocol-neutral successful-payload readiness. For Usenet this is a
    /// successful durable post-processing completion stamp; for torrents it
    /// is set only after selected payload bytes pass piece verification.
    pub ready: bool,
    pub ready_at_unix: Option<i64>,
    /// Torrent-only transfer facts. Zero for Usenet jobs, preserving the
    /// existing mixed queue shape without a parallel endpoint.
    pub uploaded_bytes: u64,
    pub upload_rate_bps: u64,
    pub ratio: f64,
    pub seeding_seconds: u64,
    pub useful_peers: u32,
    pub torrent_phase: Option<nzbd_types::TorrentPhase>,
    pub torrent_control_intent: Option<nzbd_types::TorrentControlIntent>,
    pub seed_policy: Option<nzbd_types::SeedPolicy>,
    pub seed_stop_reason: Option<nzbd_types::TorrentStopReason>,
    pub torrent_error: Option<String>,
    /// Duplicate-detection metadata (empty key = no dupe tracking).
    pub dupe_key: String,
    pub dupe_score: i32,
    /// The job's non-internal parameters — a consumer's own tracking id
    /// (`drone`, `monarr-transfer`) among them. Carried on the snapshot so
    /// the queue UI and `GET /api/v1/jobs/{id}` can show it without an
    /// export round-trip: the whole value of a transfer id is that you can
    /// SEE it on the job it belongs to. `*`-internal params stay out.
    pub params: Vec<(String, String)>,
    /// Post-processing stages this job has passed through, in order, with
    /// the wall time each took (`ms` absent = the stage is still running).
    /// Rides the existing 1 Hz tick, so the queue row's stage timer and
    /// the detail pipeline cost no extra request.
    pub stages: Vec<StageSpan>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerVolume {
    pub server: u32,
    /// Configured display name, so the UI's per-provider chips can say
    /// "eweka" rather than "#0".
    pub name: String,
    pub total_bytes: u64,
    pub day_bytes: u64,
    pub month_bytes: u64,
    /// This server's current share of the wire rate (EMA, bytes/sec),
    /// computed from the SAME counters as `download_rate_bps` — so the
    /// per-server rates sum to the header rate instead of to some other
    /// number that also calls itself throughput.
    pub rate_bps: u64,
}

/// The enforcing disk guard's cached evidence for one filesystem.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StorageVolumeSnapshot {
    pub label: String,
    pub path: String,
    pub available_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    /// False when the current cycle could not measure this row. Non-None
    /// capacity is conservative last-known data; None means no successful
    /// reading has ever been observed.
    pub current: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct QueueSnapshot {
    pub up_since_unix: i64,
    pub download_paused: bool,
    /// Daily/monthly quota exhausted (force-priority jobs still run).
    pub quota_reached: bool,
    /// Intake is held by a below-floor reading, an observed write failure, an
    /// initial fail-safe measurement, or incomplete evidence after a prior
    /// hold.
    pub disk_low: bool,
    /// Lowest measured free space across every configured write root. None
    /// means the enforcing probe has no usable reading or is disabled.
    #[serde(default)]
    pub disk_guard_free_bytes: Option<u64>,
    /// Operator-facing role of the limiting configured root.
    #[serde(default)]
    pub disk_guard_label: Option<String>,
    /// Configured path whose containing filesystem currently limits intake.
    #[serde(default)]
    pub disk_guard_path: Option<String>,
    /// The current hold was latched by an observed ENOSPC/EDQUOT write,
    /// rather than solely by the cached capacity forecast.
    #[serde(default)]
    pub disk_guard_write_latched: bool,
    /// True only when the enforcing cycle measured every configured root.
    /// False with `disk_low` can mean a prior hold is being retained because
    /// incomplete evidence is not recovery proof.
    #[serde(default)]
    pub disk_guard_all_roots_known: bool,
    /// The same per-filesystem evidence used by the enforcing guard. The API
    /// renders this cache directly; it never launches an independent probe.
    #[serde(default)]
    pub storage_volumes: Vec<StorageVolumeSnapshot>,
    /// Cumulative out-of-space errors (ENOSPC/EDQUOT) reported by write
    /// paths since start. Use `disk_guard_write_latched`, not this historical
    /// count, to identify the cause of the current hold.
    #[serde(default)]
    pub enospc_observed: u64,
    /// What the write path was doing when it last ran out of space
    /// (operation and path, as the fsx layer stamped it).
    #[serde(default)]
    pub enospc_where: Option<String>,
    /// Per-server session/day/month volume counters (this node).
    pub server_volumes: Vec<ServerVolume>,
    /// Servers currently blocked after connect failures (retrying on a
    /// timer). Surfaced so the UI can explain a stalled queue.
    pub blocked_servers: Vec<u32>,
    /// Whether critical-health abort is armed (`[post] health_action` is
    /// park/delete). Surfaced so the UI can tell the user whether a doomed
    /// download will be cut off early or just run to completion.
    pub health_abort: bool,
    pub speed_limit_bps: Option<u64>,
    /// How many jobs may download at once.
    pub max_active_downloads: u32,
    pub download_rate_bps: u64,
    pub session_downloaded_bytes: u64,
    /// Bytes still to fetch across active jobs (non-paused files).
    pub remaining_bytes: u64,
    pub jobs: Vec<JobSummary>,
}

pub type SharedSnapshot = Arc<ArcSwap<QueueSnapshot>>;

pub fn new_shared_snapshot() -> SharedSnapshot {
    Arc::new(ArcSwap::from_pointee(QueueSnapshot::default()))
}

#[cfg(test)]
mod mobile_queue_contract_tests {
    use super::JobSummary;

    #[test]
    fn torrent_mobile_queue_fixture_matches_summary_serialization() {
        let fixtures: serde_json::Value = serde_json::from_str(include_str!(
            "../../nzbd-types/fixtures/mobile-queue-parity.json"
        ))
        .unwrap();
        for fixture in fixtures.as_array().unwrap() {
            if fixture["wire"] == false {
                continue;
            }
            let input = &fixture["job"];
            // Construct the actual engine DTO, rather than adding Deserialize to it.
            let summary = JobSummary {
                repair_progress: None,
                control: None,
                id: serde_json::from_value(input["id"].clone()).unwrap(),
                kind: serde_json::from_value(input["kind"].clone()).unwrap(),
                name: serde_json::from_value(input["name"].clone()).unwrap(),
                status: serde_json::from_value(input["status"].clone()).unwrap(),
                category: serde_json::from_value(input["category"].clone()).unwrap(),
                priority: serde_json::from_value(input["priority"].clone()).unwrap(),
                size_bytes: serde_json::from_value(input["size_bytes"].clone()).unwrap(),
                downloaded_bytes: serde_json::from_value(input["downloaded_bytes"].clone())
                    .unwrap(),
                failed_bytes: serde_json::from_value(input["failed_bytes"].clone()).unwrap(),
                remaining_bytes: serde_json::from_value(input["remaining_bytes"].clone()).unwrap(),
                total_articles: serde_json::from_value(input["total_articles"].clone()).unwrap(),
                done_articles: serde_json::from_value(input["done_articles"].clone()).unwrap(),
                failed_articles: serde_json::from_value(input["failed_articles"].clone()).unwrap(),
                files_total: serde_json::from_value(input["files_total"].clone()).unwrap(),
                files_done: serde_json::from_value(input["files_done"].clone()).unwrap(),
                health: serde_json::from_value(input["health"].clone()).unwrap(),
                critical_health: serde_json::from_value(input["critical_health"].clone()).unwrap(),
                critical_health_estimated: input["critical_health_estimated"].as_bool().unwrap(),
                rate_bps: serde_json::from_value(input["rate_bps"].clone()).unwrap(),
                retried_articles: serde_json::from_value(input["retried_articles"].clone())
                    .unwrap(),
                assigned_node: serde_json::from_value(input["assigned_node"].clone()).unwrap(),
                pp_done: serde_json::from_value(input["pp_done"].clone()).unwrap(),
                ready: serde_json::from_value(input["ready"].clone()).unwrap(),
                ready_at_unix: serde_json::from_value(input["ready_at_unix"].clone()).unwrap(),
                uploaded_bytes: serde_json::from_value(input["uploaded_bytes"].clone()).unwrap(),
                upload_rate_bps: serde_json::from_value(input["upload_rate_bps"].clone()).unwrap(),
                ratio: serde_json::from_value(input["ratio"].clone()).unwrap(),
                seeding_seconds: serde_json::from_value(input["seeding_seconds"].clone()).unwrap(),
                useful_peers: serde_json::from_value(input["useful_peers"].clone()).unwrap(),
                torrent_phase: serde_json::from_value(input["torrent_phase"].clone()).unwrap(),
                torrent_control_intent: serde_json::from_value(
                    input["torrent_control_intent"].clone(),
                )
                .unwrap(),
                seed_policy: serde_json::from_value(input["seed_policy"].clone()).unwrap(),
                seed_stop_reason: serde_json::from_value(input["seed_stop_reason"].clone())
                    .unwrap(),
                torrent_error: serde_json::from_value(input["torrent_error"].clone()).unwrap(),
                dupe_key: serde_json::from_value(input["dupe_key"].clone()).unwrap(),
                dupe_score: serde_json::from_value(input["dupe_score"].clone()).unwrap(),
                params: serde_json::from_value(input["params"].clone()).unwrap(),
                stages: serde_json::from_value(input["stages"].clone()).unwrap(),
            };
            assert_eq!(
                serde_json::to_value(summary).unwrap(),
                *input,
                "{}",
                fixture["name"]
            );
        }
    }
}
