# Runner documentation

Every file under `docs/` is listed here; `crates/nzbd/tests/docs_index.rs`
fails when a file is missing from this index or any relative link under
`docs/` (or in the top-level `README.md`) does not resolve. The project's
status page is [`STATUS.md`](../STATUS.md) at the repository root.

## Using runner

| Document | What it is |
|---|---|
| [INSTALL.md](INSTALL.md) | Installing runner from packages, containers or source |
| [DEPLOY.md](DEPLOY.md) | Deploying and upgrading runner, including the dev Compose stack |
| [CONFIGURATION.md](CONFIGURATION.md) | Every `nzbd.toml` key |
| [USAGE.md](USAGE.md) | The web UI, the NZBGet-compatible shim and day-to-day operation |
| [MOBILE.md](MOBILE.md) | The iPhone, iPad and Android remote |
| [FILE_LIFECYCLE_OPERATIONS.md](FILE_LIFECYCLE_OPERATIONS.md) | Retention, parking, recovery mounts, cancellation, backup and restore of payload files |

## The seam with curator and cinema

| Document | What it is |
|---|---|
| [INTEGRATION.md](INTEGRATION.md) | The wire reference: job API, events, history cursor, publications, disposal |
| [PIPELINE_CONTRACT.md](PIPELINE_CONTRACT.md) | Pipeline contract v2 — the design and its rationale |
| [adr/0020-pipeline-contract-v2.md](adr/0020-pipeline-contract-v2.md) | ADR-20: contract v2 and what it replaced |

## Design

| Document | What it is |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Architecture and the ADR summary (§15) |
| [CLUSTERING.md](CLUSTERING.md) | Multi-node clustering over a shared work volume |
| [CLUSTERING_COMPLETION_PLAN.md](CLUSTERING_COMPLETION_PLAN.md) | Proposed completion of cluster phase C3 |
| [CLUSTERING_STATUS.md](CLUSTERING_STATUS.md) | Cluster completion progress and remaining work |
| [BITTORRENT_PROPOSAL.md](BITTORRENT_PROPOSAL.md) | BitTorrent support (ADR-19) |
| [BITTORRENT_M0_REPORT.md](BITTORRENT_M0_REPORT.md) | BitTorrent M0: the engine boundary (read by `scripts/check-bittorrent-release-review.sh`) |
| [BITTORRENT_M1B_REPORT.md](BITTORRENT_M1B_REPORT.md) | BitTorrent M1b: one queue contract |
| [BITTORRENT_GATE9_REVIEW.md](BITTORRENT_GATE9_REVIEW.md) | BitTorrent gate 9 dependency review (read by the release-review script) |
| [BITTORRENT_RELEASE_REVIEW.md](BITTORRENT_RELEASE_REVIEW.md) | BitTorrent single-node release review (read by the release-review script) |
| [BITTORRENT_BUILD_STATUS.md](BITTORRENT_BUILD_STATUS.md) | BitTorrent build record |
| [BITTORRENT_MAGNET_DHT_STATUS.md](BITTORRENT_MAGNET_DHT_STATUS.md) | Magnet DHT build record |
| [HISTORY_INCREMENTAL.md](HISTORY_INCREMENTAL.md) | Incremental history ingestion |
| [HISTORY_LOADING_PLAN.md](HISTORY_LOADING_PLAN.md) | History loading performance |
| [STARTUP_RECOVERY_BACKPRESSURE.md](STARTUP_RECOVERY_BACKPRESSURE.md) | Startup recovery pacing and durable progress |
| [STARTUP_VERIFICATION_PLAN.md](STARTUP_VERIFICATION_PLAN.md) | Startup availability and runtime verification |
| [URL_FETCH_RECOVERY.md](URL_FETCH_RECOVERY.md) | "Fetching NZB" stalls and the URL recovery contract |
| [PAR_REPAIR_PROGRESS_PLAN.md](PAR_REPAIR_PROGRESS_PLAN.md) | Bounded PAR scanning and useful recovery retries |
| [RENAME_PERFORMANCE_PLAN.md](RENAME_PERFORMANCE_PLAN.md) | Reusing producer reads in rename and verified commits |
| [DOWNLOAD_SPEED_ROOT_CAUSE_PLAN.md](DOWNLOAD_SPEED_ROOT_CAUSE_PLAN.md) | Download speed regression and its correction |
| [IO_EFFICIENCY_AUDIT.md](IO_EFFICIENCY_AUDIT.md) | Redundant I/O audit (follow-ups open) |
| [UI_V2_PLAN.md](UI_V2_PLAN.md) | Web UI v2 and the rendering laws its DOM test enforces |
| [MOBILE_QUEUE_PARITY_PLAN.md](MOBILE_QUEUE_PARITY_PLAN.md) | Torrent grouping in the mobile queue |
| [MOBILE_REVIEW.md](MOBILE_REVIEW.md) | Mobile app review and its open punch list |

## Archive

Superseded plans and finished status or review records, kept as history.
Each opens with a pointer to what replaced it.

| Document | Replaced by |
|---|---|
| [archive/CURATOR_DELIVERY.md](archive/CURATOR_DELIVERY.md) | contract v2 §2.5 — there is no acknowledgement |
| [archive/HELD_JOBS_PLAN.md](archive/HELD_JOBS_PLAN.md) | contract v2 §2.3 — environmental holds with a deadline |
| [archive/INTEGRATION_PLAN.md](archive/INTEGRATION_PLAN.md) | [INTEGRATION.md](INTEGRATION.md) |
| [archive/INTEGRATION_RECOVERY_STATUS.md](archive/INTEGRATION_RECOVERY_STATUS.md) | contract v2 — custody and acknowledgement are gone |
| [archive/LIFECYCLE_REMEDIATION_STATUS.md](archive/LIFECYCLE_REMEDIATION_STATUS.md) | contract v2 and [FILE_LIFECYCLE_OPERATIONS.md](FILE_LIFECYCLE_OPERATIONS.md) |
| [archive/REGRAB_LOOP_PLAN.md](archive/REGRAB_LOOP_PLAN.md) | contract v2 §2.2 — typed reasons and `blames_release` |
| [archive/PUBLICATION_PERFORMANCE_PLAN.md](archive/PUBLICATION_PERFORMANCE_PLAN.md) | per-file journaled publication (INTEGRATION.md §4) |
| [archive/FILE_LIFECYCLE_PLAN.md](archive/FILE_LIFECYCLE_PLAN.md) | [FILE_LIFECYCLE_OPERATIONS.md](FILE_LIFECYCLE_OPERATIONS.md) |
| [archive/FILE_LIFECYCLE_REVIEW.md](archive/FILE_LIFECYCLE_REVIEW.md) | [FILE_LIFECYCLE_OPERATIONS.md](FILE_LIFECYCLE_OPERATIONS.md) |
| [archive/FILE_LIFECYCLE_STATUS.md](archive/FILE_LIFECYCLE_STATUS.md) | [FILE_LIFECYCLE_OPERATIONS.md](FILE_LIFECYCLE_OPERATIONS.md) |
| [archive/DOWNLOAD_INCIDENT_REVIEW.md](archive/DOWNLOAD_INCIDENT_REVIEW.md) | the "Download incident recovery" entry in `STATUS.md` |
| [archive/DOWNLOAD_INCIDENT_STATUS.md](archive/DOWNLOAD_INCIDENT_STATUS.md) | the same |
| [archive/DOWNLOAD_INCIDENT_OPUS_REVIEW.md](archive/DOWNLOAD_INCIDENT_OPUS_REVIEW.md) | the same |
| [archive/DOWNLOAD_INCIDENT_OPUS_REVIEW_V2.md](archive/DOWNLOAD_INCIDENT_OPUS_REVIEW_V2.md) | the same |
| [archive/DOWNLOAD_INCIDENT_INVESTIGATION_ARCHIVE.md](archive/DOWNLOAD_INCIDENT_INVESTIGATION_ARCHIVE.md) | the same |
| [archive/DEFECT_HISTORY_DELETE.md](archive/DEFECT_HISTORY_DELETE.md) | [HISTORY_INCREMENTAL.md](HISTORY_INCREMENTAL.md) |
| [archive/DEV_COMPOSE_BUILD_STATUS.md](archive/DEV_COMPOSE_BUILD_STATUS.md) | [DEPLOY.md](DEPLOY.md) |
| [archive/HISTORY_LOADING_STATUS.md](archive/HISTORY_LOADING_STATUS.md) | [HISTORY_LOADING_PLAN.md](HISTORY_LOADING_PLAN.md) |
| [archive/MOBILE_QUEUE_PARITY_STATUS.md](archive/MOBILE_QUEUE_PARITY_STATUS.md) | [MOBILE.md](MOBILE.md) |
| [archive/MOBILE_QUEUE_PARITY_REVIEW.md](archive/MOBILE_QUEUE_PARITY_REVIEW.md) | [MOBILE_QUEUE_PARITY_PLAN.md](MOBILE_QUEUE_PARITY_PLAN.md) |
| [archive/PAR_REPAIR_PROGRESS_STATUS.md](archive/PAR_REPAIR_PROGRESS_STATUS.md) | [PAR_REPAIR_PROGRESS_PLAN.md](PAR_REPAIR_PROGRESS_PLAN.md) |
| [archive/STARTUP_VERIFICATION_STATUS.md](archive/STARTUP_VERIFICATION_STATUS.md) | [STARTUP_VERIFICATION_PLAN.md](STARTUP_VERIFICATION_PLAN.md) |
| [archive/STARTUP_VERIFICATION_REVIEW.md](archive/STARTUP_VERIFICATION_REVIEW.md) | the same |
| [archive/STARTUP_VERIFICATION_OPUS_REVIEW.md](archive/STARTUP_VERIFICATION_OPUS_REVIEW.md) | the same |
| [archive/STARTUP_VERIFICATION_OPUS_REREVIEW.md](archive/STARTUP_VERIFICATION_OPUS_REREVIEW.md) | the same |
| [archive/STARTUP_VERIFICATION_OPUS_THIRD_REVIEW.md](archive/STARTUP_VERIFICATION_OPUS_THIRD_REVIEW.md) | the same |
| [archive/STARTUP_IMPLEMENTATION_REVIEW.md](archive/STARTUP_IMPLEMENTATION_REVIEW.md) | the same |
| [archive/STARTUP_RECOVERY_REVIEW.md](archive/STARTUP_RECOVERY_REVIEW.md) | [STARTUP_RECOVERY_BACKPRESSURE.md](STARTUP_RECOVERY_BACKPRESSURE.md) |
| [archive/TORRENT_QUEUE_STATUS.md](archive/TORRENT_QUEUE_STATUS.md) | [BITTORRENT_PROPOSAL.md](BITTORRENT_PROPOSAL.md) |
