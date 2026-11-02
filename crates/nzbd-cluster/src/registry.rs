//! Node presence registry (CLUSTERING.md §5): every node renews
//! `nodes/<name>.json` each lease interval with its capabilities and load;
//! liveness is judged by observed seq progression, never wall clocks.

use crate::layout::SharedLayout;
use crate::proto::NodeRecord;
use crate::ClusterConfig;
use nzbd_engine::EngineHandle;
use nzbd_types::JobStatus;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub fn spawn_registry(
    layout: SharedLayout,
    cfg: ClusterConfig,
    engine: EngineHandle,
    cancel: CancellationToken,
    tracker: &TaskTracker,
) {
    tracker.spawn(async move {
        let mut seq = 0u64;
        let path = layout.node_file(&cfg.node_name);
        loop {
            if cancel.is_cancelled() {
                break;
            }
            seq += 1;
            let snap = engine.snapshot();
            let active = snap
                .jobs
                .iter()
                .filter(|j| j.assigned_node.is_none() && matches!(j.status, JobStatus::Downloading))
                .count() as u32;
            let rec = NodeRecord {
                lifecycle_protocol: crate::proto::LIFECYCLE_PROTOCOL,
                name: cfg.node_name.clone(),
                api_url: cfg.advertise_url.clone(),
                download: cfg.download,
                post_process: cfg.post_process,
                max_download_jobs: cfg.max_download_jobs,
                download_weight: cfg.download_weight,
                pp_weight: cfg.pp_weight,
                active_download_jobs: active,
                disk_guard_capable: true,
                disk_low: snap.disk_low,
                disk_guard_free_bytes: snap.disk_guard_free_bytes,
                disk_guard_label: snap.disk_guard_label.clone(),
                disk_guard_path: snap.disk_guard_path.clone(),
                disk_guard_write_latched: snap.disk_guard_write_latched,
                disk_guard_all_roots_known: snap.disk_guard_all_roots_known,
                pp_slots: if cfg.post_process { cfg.pp_slots } else { 0 },
                rate_bps: snap.download_rate_bps,
                seq,
            };
            if let Err(e) = layout.write_json(&path, &rec) {
                tracing::warn!(error = %e, "node registry write failed");
            }
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(cfg.lease_interval) => {}
            }
        }
    });
}

/// Read every node record (freshness judged by the caller via seq
/// progression tracking).
pub fn read_nodes(layout: &SharedLayout) -> Vec<NodeRecord> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(layout.nodes_dir()) else {
        return out;
    };
    for entry in entries.flatten() {
        if entry.file_name().to_string_lossy().ends_with(".json") {
            if let Some(rec) = SharedLayout::read_json::<NodeRecord>(&entry.path()) {
                out.push(rec);
            }
        }
    }
    out
}
