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
    /// The job's one terminal fact (pipeline contract v2 §2.1).
    ///
    /// Emitted exactly once per job, from the job's `job_terminals` record,
    /// after that record and the history row are durable and before any
    /// salvage, disposal or retention touches the bytes. Constructed in one
    /// place: the emit step of `terminal::record_terminal`'s replay.
    /// `history_seq` is never 0: a failed history write holds the job
    /// `storage_unavailable` on stage `persistence` and the event is sent
    /// when the write lands.
    JobPpFinished {
        job: JobId,
        name: String,
        category: Option<String>,
        /// `params["monarr-transfer"]`; absent only for jobs curator did not add.
        transfer: Option<String>,
        outcome: nzbd_types::TerminalOutcome,
        /// `failed` only: the closed reason, typed by the producer.
        reason: Option<nzbd_types::FailureReason>,
        /// `fetch` only: `gone` or `indexer_unavailable`.
        fetch_cause: Option<nzbd_types::FetchCause>,
        /// `failed` only: a `PostStage` name or `download` | `health` | `fetch`.
        stage: Option<String>,
        /// `failed` only: human text, at most 2048 characters.
        message: Option<String>,
        blames_release: bool,
        /// `pending` when runner will assess and publish usable partial
        /// output (announced later by [`Event::JobPublicationReady`]).
        salvage: nzbd_types::Salvage,
        keep_on_failure: bool,
        fetched: nzbd_types::Fetched,
        /// `completed` only.
        publication: Option<nzbd_types::Publication>,
        size_bytes: u64,
        health: u16,
        /// Consumer params plus `*Control:v1`.
        params: Vec<(String, String)>,
        /// Rowid of the durable history entry; the `since_seq` cursor value.
        history_seq: i64,
        /// The history index's identity: the cursor is `(incarnation, seq)`.
        history_incarnation: String,
    },
    /// Every `JobStatus` transition, including into and out of `held`
    /// (contract §2.4). `status` is the projected native status.
    JobStatusChanged {
        job: JobId,
        transfer: Option<String>,
        status: String,
        stage: Option<String>,
        control: Option<nzbd_types::JobControl>,
    },
    /// A salvage publication of a failed job is ready (contract §2.2). Not a
    /// terminal: the job stays `failed`.
    JobPublicationReady {
        job: JobId,
        transfer: Option<String>,
        publication: nzbd_types::Publication,
    },
}
