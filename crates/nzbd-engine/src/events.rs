//! Engine broadcast events (mirrored to the API's SSE stream in phase 3).

use nzbd_types::{FileId, JobId, JobStatus, PostStage, ServerId};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    JobControlChanged {
        job: JobId,
        control: nzbd_types::JobControl,
    },
    JobAdded {
        job: JobId,
        name: String,
    },
    /// The article-download phase ended. This is not payload readiness:
    /// Usenet jobs may still verify, repair, unpack, move and run scripts.
    /// Native consumers finish the handoff on [`Event::JobPpFinished`].
    JobFinished {
        job: JobId,
        name: String,
        status: JobStatus,
        health: u16,
    },
    JobDeleted {
        job: JobId,
    },
    /// A URL admission resolved from its durable `Fetching` placeholder.
    /// Cluster control uses this narrow event to commit the exact queued or
    /// failed replacement before it is eligible for scheduling.
    UrlFetchResolved {
        job: JobId,
    },
    FileFinished {
        job: JobId,
        file: FileId,
        filename: String,
        ok: bool,
    },
    /// A segment failed on every server at every tier.
    SegmentExhausted {
        job: JobId,
        file: FileId,
        segment: u32,
    },
    ServerBlocked {
        server: ServerId,
        seconds: u64,
    },
    QueuePauseChanged {
        paused: bool,
        /// Who asked (client UA / "web-ui" / "cli") — surfaced in the UI
        /// and logs so a queue that keeps (un)pausing itself is a
        /// one-glance diagnosis instead of whack-a-mole: SOME client sent
        /// this; the engine never flips the flag on its own.
        source: String,
    },
    SpeedLimitChanged {
        bytes_per_sec: Option<u64>,
    },
    MaxActiveDownloadsChanged {
        n: u32,
    },
    /// Cluster: job (un)delegated to a node.
    JobAssigned {
        job: JobId,
        node: Option<String>,
    },
    /// Post-processing entered a stage. `JobFinished` fires when the
    /// *download* completes; everything after it — verify, repair, unpack,
    /// move, scripts — used to be visible only in the log. A consumer
    /// showing "stuck on something" instead of "repairing since 4 min" is
    /// the black box this event closes.
    JobPpStage {
        job: JobId,
        name: String,
        stage: PostStage,
    },
    /// Post-processing finished AND the history row is durably written.
    ///
    /// Ordering is normative (docs/INTEGRATION_PLAN.md §1): this is
    /// emitted only after `HistoryDb::record` returns, so a consumer may
    /// react to it and immediately read
    /// `GET /api/v1/history?since_seq=<history_seq - 1>` and be certain the
    /// row is there. Emitting before the write would hand every consumer a
    /// race it cannot see.
    JobPpFinished {
        job: JobId,
        name: String,
        category: Option<String>,
        /// The history status string verbatim — `SUCCESS`, `PAR_FAILURE`,
        /// `UNPACK_FAILURE`, `SCRIPT_FAILURE` for jobs that reached PP, and
        /// the pre-PP terminal strings `FAILURE/HEALTH` / `FAILURE/FETCH`
        /// for jobs that never did. Consumers match on the `SUCCESS` /
        /// `FAILURE` prefix; no value here is invented for the event.
        pp_status: String,
        /// Where the files actually are — absent on the failure paths that
        /// never produced a directory.
        final_dir: Option<String>,
        size_bytes: u64,
        health: u16,
        /// The job's non-`*` params (the `monarr-transfer` id rides here).
        params: Vec<(String, String)>,
        /// Rowid of the history entry just written: the cursor value a
        /// consumer passes to `?since_seq=`. 0 when the write failed —
        /// the event still fires, because "PP ended and history did not
        /// record it" is precisely the moment a consumer must not miss.
        history_seq: i64,
    },
}
