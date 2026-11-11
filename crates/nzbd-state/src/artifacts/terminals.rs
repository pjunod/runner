//! The authoritative terminal record (pipeline contract v2 §2.1, §2.6).
//!
//! A job's terminal is first written as one `job_terminals` row, in the same
//! SQLite transaction as the salvage obligation on the job's current
//! artifact. The history append, the queue-row markers and the
//! `job_pp_finished` emit are then performed *from* this row and each is
//! marked done on it, so a crash between any two of those writes is replayed
//! from the row on the next boot. Disposal consults this row — never the
//! history row or the queue row — and is not eligible while salvage is
//! still `pending`.

use super::*;
use nzbd_types::{FailureReason, FetchCause, Fetched, Publication, TerminalOutcome};

/// The salvage obligation as persisted. The event's `salvage` field is
/// `pending` for every state but `None` (the obligation existed when the
/// terminal was recorded).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SalvageState {
    /// No salvage applies (fetch/identity, completed, cancelled, nothing fetched).
    None,
    /// Runner owes an assessment; disposal waits.
    Pending,
    /// A salvage publication was announced with `job_publication_ready`.
    Published,
    /// Assessed; nothing usable.
    SettledNone,
}

impl SalvageState {
    pub fn as_str(self) -> &'static str {
        match self {
            SalvageState::None => "none",
            SalvageState::Pending => "pending",
            SalvageState::Published => "published",
            SalvageState::SettledNone => "settled_none",
        }
    }
    pub fn parse(s: &str) -> Option<SalvageState> {
        match s {
            "none" => Some(SalvageState::None),
            "pending" => Some(SalvageState::Pending),
            "published" => Some(SalvageState::Published),
            "settled_none" => Some(SalvageState::SettledNone),
            _ => None,
        }
    }
    /// The wire value of `job_pp_finished.salvage`.
    pub fn wire(self) -> nzbd_types::Salvage {
        if self == SalvageState::None {
            nzbd_types::Salvage::None
        } else {
            nzbd_types::Salvage::Pending
        }
    }
}

/// How a salvage obligation settles.
#[derive(Clone, Debug)]
pub enum SalvageOutcome {
    Published(Publication),
    None { error: Option<String> },
}

/// The steps performed from a terminal row, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TerminalStep {
    /// The row (and the salvage obligation) committed.
    Recorded,
    HistoryWritten {
        seq: i64,
        incarnation: String,
    },
    MarkersStamped,
    Emitted,
}

impl TerminalStep {
    fn key(&self) -> u8 {
        match self {
            TerminalStep::Recorded => 0,
            TerminalStep::HistoryWritten { .. } => 1,
            TerminalStep::MarkersStamped => 2,
            TerminalStep::Emitted => 3,
        }
    }
}

/// One job's terminal, mirroring the `job_terminals` row.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobTerminal {
    pub job: u32,
    pub transfer: Option<String>,
    pub outcome: TerminalOutcome,
    pub reason: Option<FailureReason>,
    pub fetch_cause: Option<FetchCause>,
    pub stage: Option<String>,
    pub message: Option<String>,
    pub blames_release: bool,
    pub salvage: SalvageState,
    pub keep_on_failure: bool,
    pub fetched: Fetched,
    pub publication: Option<Publication>,
    /// Params to stamp on the queue row (`*PP:done`, `*PP:failure-at`,
    /// `Failure:*`, `*PP:history-seq` is added from the history step).
    pub markers: Vec<(String, String)>,
    /// The history entry to append, prepared by the producer (seq 0).
    pub entry: crate::HistoryEntry,
    pub history_seq: Option<i64>,
    pub history_incarnation: Option<String>,
    pub markers_stamped: bool,
    pub emitted: bool,
    /// The disposal note once the failed source has been disposed.
    pub disposal: Option<String>,
    pub salvage_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl JobTerminal {
    /// Every step done: nothing to replay.
    pub fn finished(&self) -> bool {
        self.history_seq.is_some() && self.markers_stamped && self.emitted
    }
    /// The legacy history status string (`SUCCESS`, `UNPACK_FAILURE`, …).
    pub fn status(&self) -> &str {
        &self.entry.status
    }
}

const COLUMNS: &str = "job,transfer,outcome,reason,fetch_cause,stage,message,blames_release,salvage,keep_on_failure,fetched,publication,markers,entry,history_seq,history_incarnation,markers_stamped,emitted,disposal,salvage_error,created_at,updated_at";

fn read_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawTerminal> {
    Ok(RawTerminal {
        job: row.get(0)?,
        transfer: row.get(1)?,
        outcome: row.get(2)?,
        reason: row.get(3)?,
        fetch_cause: row.get(4)?,
        stage: row.get(5)?,
        message: row.get(6)?,
        blames_release: row.get::<_, i64>(7)? != 0,
        salvage: row.get(8)?,
        keep_on_failure: row.get::<_, i64>(9)? != 0,
        fetched: row.get(10)?,
        publication: row.get(11)?,
        markers: row.get(12)?,
        entry: row.get(13)?,
        history_seq: row.get(14)?,
        history_incarnation: row.get(15)?,
        markers_stamped: row.get::<_, i64>(16)? != 0,
        emitted: row.get::<_, i64>(17)? != 0,
        disposal: row.get(18)?,
        salvage_error: row.get(19)?,
        created_at: row.get(20)?,
        updated_at: row.get(21)?,
    })
}

struct RawTerminal {
    job: u32,
    transfer: Option<String>,
    outcome: String,
    reason: Option<String>,
    fetch_cause: Option<String>,
    stage: Option<String>,
    message: Option<String>,
    blames_release: bool,
    salvage: String,
    keep_on_failure: bool,
    fetched: String,
    publication: Option<String>,
    markers: String,
    entry: String,
    history_seq: Option<i64>,
    history_incarnation: Option<String>,
    markers_stamped: bool,
    emitted: bool,
    disposal: Option<String>,
    salvage_error: Option<String>,
    created_at: i64,
    updated_at: i64,
}

impl RawTerminal {
    fn decode(self) -> Result<JobTerminal> {
        let corrupt = |what: &str| Error::Conflict(format!("job_terminals {what} is invalid"));
        Ok(JobTerminal {
            job: self.job,
            transfer: self.transfer,
            outcome: TerminalOutcome::parse(&self.outcome).ok_or_else(|| corrupt("outcome"))?,
            reason: self
                .reason
                .map(|r| FailureReason::parse(&r).ok_or_else(|| corrupt("reason")))
                .transpose()?,
            fetch_cause: self
                .fetch_cause
                .map(|c| FetchCause::parse(&c).ok_or_else(|| corrupt("fetch_cause")))
                .transpose()?,
            stage: self.stage,
            message: self.message,
            blames_release: self.blames_release,
            salvage: SalvageState::parse(&self.salvage).ok_or_else(|| corrupt("salvage"))?,
            keep_on_failure: self.keep_on_failure,
            fetched: serde_json::from_str(&self.fetched)?,
            publication: self
                .publication
                .map(|p| serde_json::from_str(&p))
                .transpose()?,
            markers: serde_json::from_str(&self.markers)?,
            entry: serde_json::from_str(&self.entry)?,
            history_seq: self.history_seq,
            history_incarnation: self.history_incarnation,
            markers_stamped: self.markers_stamped,
            emitted: self.emitted,
            disposal: self.disposal,
            salvage_error: self.salvage_error,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

fn set_artifact_salvage(tx: &Connection, job: u32, salvage: SalvageState) -> Result<usize> {
    // The obligation is on the job's *source*, chosen by role, not recency
    // (review of #282). Once salvage publishes, the job's own artifact row
    // is the publication Curator owns (`transferred`, a delivery's
    // artifact) and the retained source is the delivery's
    // `sourceArtifactId`, a row with no job column; before that, the source
    // is the job's row that is not a publication.
    Ok(tx.execute(
        "UPDATE artifacts SET data=json_set(data,'$.salvage',?2) WHERE id=COALESCE(
            (SELECT json_extract(d.data,'$.sourceArtifactId') FROM deliveries d JOIN artifacts p ON p.id=d.artifact WHERE p.job=?1 ORDER BY d.sequence DESC LIMIT 1),
            (SELECT a.id FROM artifacts a WHERE a.job=?1 AND json_extract(a.data,'$.state')<>'transferred' AND a.id NOT IN (SELECT artifact FROM deliveries) ORDER BY a.updated_at DESC LIMIT 1))",
        rusqlite::params![job, salvage.as_str()],
    )
    .and_then(|updated| {
        // A relocation keeps the job's artifact id on the publication, so
        // the obligation written before it moved travels along; the
        // publication owes nothing.
        tx.execute(
            "UPDATE artifacts SET data=json_remove(data,'$.salvage') WHERE job=?1 AND id IN (SELECT artifact FROM deliveries)",
            [job],
        )?;
        Ok(updated)
    })?)
}

/// Mirror the failure reason onto the job's **source** artifact (display
/// only; `job_terminals` stays the authority). The source is picked by role,
/// not recency: a publication (state `transferred`, or any artifact a delivery
/// row names as its published one) belongs to Curator and is never marked.
/// W2: a failed terminal whose salvage settled `none` while a publication
/// of the job was still unresolved, and whose move later committed, owes
/// that publication as salvage. Reopen the obligation so the salvage path
/// finishes it from the journal. Runs in the intent-commit transaction and,
/// for rows an older build left, once per open. Idempotent: once the
/// delivery row exists (or the publication is gone) nothing matches.
pub(super) fn reopen_salvage_for_committed_publications(
    tx: &Connection,
    job: Option<u32>,
) -> Result<usize> {
    let jobs = {
        let mut statement = tx.prepare(
            "SELECT t.job FROM job_terminals t WHERE t.outcome='failed' AND t.salvage='settled_none' AND (?1 IS NULL OR t.job=?1) AND EXISTS (SELECT 1 FROM relocation_intents i JOIN artifacts a ON a.id=i.artifact WHERE i.job=t.job AND i.state='committed' AND json_extract(a.data,'$.generation')=i.generation AND a.path=i.target_dir AND a.state IN ('active','completed') AND json_extract(a.data,'$.owned')=1 AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.artifact=i.artifact AND d.generation=i.generation))",
        )?;
        let rows = statement.query_map([job], |row| row.get::<_, u32>(0))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()?
    };
    for job in &jobs {
        tx.execute(
            "UPDATE job_terminals SET salvage='pending',salvage_error=NULL,updated_at=?2 WHERE job=?1",
            rusqlite::params![job, now()],
        )?;
        set_artifact_salvage(tx, *job, SalvageState::Pending)?;
        tracing::info!(
            job,
            "committed publication reopens a settled salvage obligation"
        );
    }
    Ok(jobs.len())
}

fn set_artifact_failure_reason(tx: &Connection, job: u32, reason: FailureReason) -> Result<usize> {
    Ok(tx.execute(
        "UPDATE artifacts SET data=json_set(data,'$.failure_reason',?2) WHERE id=(SELECT id FROM artifacts WHERE job=?1 AND json_extract(data,'$.state')<>'transferred' AND id NOT IN (SELECT artifact FROM deliveries) ORDER BY updated_at DESC LIMIT 1)",
        rusqlite::params![job, reason.as_str()],
    )?)
}

impl Inventory {
    /// Contract §2.1 step one: insert the terminal row and the artifact's
    /// salvage obligation in one transaction. Idempotent: when a row for the
    /// job already exists it is returned unchanged (replay continues from
    /// its step flags). A `pending` obligation on a job with no artifact
    /// (nothing was ever allocated) settles to `none`: there is nothing to
    /// salvage.
    pub fn record_terminal(&self, terminal: &JobTerminal) -> Result<JobTerminal> {
        #[cfg(feature = "test-support")]
        if TERMINAL_INSERT_FAILURES
            .lock()
            .unwrap()
            .remove(&terminal.job)
        {
            return Err(Error::Io(std::io::Error::other(
                "injected job_terminals insert failure (os error 5)",
            )));
        }
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let existing = tx
            .query_row(
                &format!("SELECT {COLUMNS} FROM job_terminals WHERE job=?1"),
                [terminal.job],
                read_row,
            )
            .optional()?;
        if let Some(existing) = existing {
            tx.commit()?;
            return existing.decode();
        }
        let mut row = terminal.clone();
        let at = now();
        row.created_at = at;
        row.updated_at = at;
        if let (TerminalOutcome::Failed, Some(reason)) = (row.outcome, row.reason) {
            set_artifact_failure_reason(&tx, row.job, reason)?;
        }
        if row.salvage == SalvageState::Pending {
            let has_artifact: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM artifacts WHERE job=?1)",
                [row.job],
                |r| r.get(0),
            )?;
            if has_artifact {
                set_artifact_salvage(&tx, row.job, SalvageState::Pending)?;
            } else {
                row.salvage = SalvageState::None;
            }
        }
        tx.execute(
            &format!("INSERT INTO job_terminals({COLUMNS}) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,NULL,NULL,0,0,NULL,NULL,?15,?15)"),
            rusqlite::params![
                row.job,
                row.transfer,
                row.outcome.as_str(),
                row.reason.map(FailureReason::as_str),
                row.fetch_cause.map(FetchCause::as_str),
                row.stage,
                row.message,
                row.blames_release as i64,
                row.salvage.as_str(),
                row.keep_on_failure as i64,
                serde_json::to_string(&row.fetched)?,
                row.publication.as_ref().map(serde_json::to_string).transpose()?,
                serde_json::to_string(&row.markers)?,
                serde_json::to_string(&row.entry)?,
                at,
            ],
        )?;
        tx.commit()?;
        Ok(row)
    }

    pub fn terminal(&self, job: u32) -> Result<Option<JobTerminal>> {
        let db = self.db.lock().unwrap();
        db.query_row(
            &format!("SELECT {COLUMNS} FROM job_terminals WHERE job=?1"),
            [job],
            read_row,
        )
        .optional()?
        .map(RawTerminal::decode)
        .transpose()
    }

    /// Mark one replayed step done on the row.
    pub fn mark_terminal_step(&self, job: u32, step: &TerminalStep) -> Result<()> {
        let db = self.db.lock().unwrap();
        let changed = match step {
            TerminalStep::Recorded => return Ok(()),
            TerminalStep::HistoryWritten { seq, incarnation } => db.execute(
                "UPDATE job_terminals SET history_seq=?2,history_incarnation=?3,updated_at=?4 WHERE job=?1 AND history_seq IS NULL",
                rusqlite::params![job, seq, incarnation, now()],
            )?,
            TerminalStep::MarkersStamped => db.execute(
                "UPDATE job_terminals SET markers_stamped=1,updated_at=?2 WHERE job=?1",
                rusqlite::params![job, now()],
            )?,
            TerminalStep::Emitted => db.execute(
                "UPDATE job_terminals SET emitted=1,updated_at=?2 WHERE job=?1",
                rusqlite::params![job, now()],
            )?,
        };
        if changed == 0 && !matches!(step, TerminalStep::HistoryWritten { .. }) {
            return Err(Error::NotFound);
        }
        Ok(())
    }

    /// Boot replay: every row with a step still to perform.
    pub fn unfinished_terminals(&self) -> Result<Vec<JobTerminal>> {
        self.terminals_where("history_seq IS NULL OR markers_stamped=0 OR emitted=0")
    }

    /// Terminals whose salvage obligation is still owed and whose event has
    /// gone out (salvage never precedes the terminal fact).
    pub fn pending_salvage(&self) -> Result<Vec<JobTerminal>> {
        self.terminals_where("salvage='pending' AND emitted=1")
    }

    /// Failed terminals whose salvage has settled and whose source has not
    /// yet been disposed.
    pub fn pending_disposal(&self) -> Result<Vec<JobTerminal>> {
        self.terminals_where(
            "outcome='failed' AND salvage<>'pending' AND emitted=1 AND disposal IS NULL",
        )
    }

    fn terminals_where(&self, predicate: &str) -> Result<Vec<JobTerminal>> {
        let db = self.db.lock().unwrap();
        let mut statement = db.prepare(&format!(
            "SELECT {COLUMNS} FROM job_terminals WHERE {predicate} ORDER BY job"
        ))?;
        let rows = statement.query_map([], read_row)?;
        let raw = rows.collect::<std::result::Result<Vec<_>, _>>()?;
        raw.into_iter().map(RawTerminal::decode).collect()
    }

    /// Settle the salvage obligation (contract §2.2): `published` once the
    /// salvage publication is announced, `settled_none` when nothing was
    /// usable. Settling an obligation that is not pending is a no-op.
    ///
    /// `settled_none` is refused while a publication move of the job is
    /// open: reconciliation may still commit it, and a committed
    /// publication is a salvage publication (W2).
    pub fn settle_salvage(&self, job: u32, outcome: &SalvageOutcome) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        if matches!(outcome, SalvageOutcome::None { .. }) {
            let open: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM relocation_intents WHERE job=?1 AND state IN ('open','finishing','unwinding'))",
                [job],
                |row| row.get(0),
            )?;
            if open {
                return Err(Error::Conflict(
                    "a publication move of this job is still open; salvage settles once it resolves"
                        .into(),
                ));
            }
        }
        let (state, error) = match outcome {
            SalvageOutcome::Published(_) => (SalvageState::Published, None),
            SalvageOutcome::None { error } => (SalvageState::SettledNone, error.clone()),
        };
        let changed = tx.execute(
            "UPDATE job_terminals SET salvage=?2,salvage_error=?3,updated_at=?4 WHERE job=?1 AND salvage='pending'",
            rusqlite::params![job, state.as_str(), error, now()],
        )?;
        if changed > 0 {
            set_artifact_salvage(&tx, job, state)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Complete a failed job's salvage from a committed publication journal,
    /// without the queue row (it may already be retired) and without reading
    /// the publication: the delivery row is built from the journal and the
    /// terminal. `None` when the job owes no such salvage.
    pub fn finish_committed_salvage(&self, job: u32) -> Result<Option<super::Delivery>> {
        let Some(terminal) = self.terminal(job)? else {
            return Ok(None);
        };
        if terminal.outcome != TerminalOutcome::Failed || terminal.salvage != SalvageState::Pending
        {
            return Ok(None);
        }
        let Some(moved) = self.committed_intent_publication(job)? else {
            return Ok(None);
        };
        if let Some(delivery) = self
            .delivery_for_job(job)?
            .filter(|delivery| delivery.publication_generation == moved.generation)
        {
            return Ok(Some(delivery));
        }
        let artifact = self.get(&moved.artifact_id)?;
        if !artifact.delivery_lifecycle {
            return Ok(None);
        }
        let root = moved
            .published_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| artifact.root.clone());
        self.finish(job, &moved.published_path, &root, "completed")?;
        let prepared = self.prepare_delivery(job, true)?;
        self.commit_prepared_delivery(
            prepared,
            terminal.transfer.as_deref(),
            terminal.status(),
            Some(terminal.stage.as_deref().unwrap_or_default()),
            Some(terminal.message.as_deref().unwrap_or_default()),
            terminal.keep_on_failure,
            None,
            None,
        )
        .map(Some)
    }

    /// W3 (Paul, 2026-11-11): a curator-admitted failure the job does not
    /// keep follows `failure_action` once its salvage settles; under
    /// `delete` it is never parked. Sources an older build parked under the
    /// replaced park-always rule are due now, through the ordinary retention
    /// path (`tick`). Returns how many became due.
    pub fn expire_parked_curator_failures(&self) -> Result<usize> {
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let raws = {
            let mut statement = tx.prepare(
                "SELECT a.data FROM artifacts a JOIN job_terminals t ON t.job=a.job WHERE a.state='parked_failed' AND t.outcome='failed' AND t.keep_on_failure=0 AND t.transfer IS NOT NULL AND t.salvage<>'pending'",
            )?;
            let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let at = now();
        let mut due = 0;
        for raw in raws {
            let mut artifact: Artifact = serde_json::from_str(&raw)?;
            if artifact.keep
                || artifact.keep_on_failure
                || artifact.deadline.is_some_and(|deadline| deadline <= at)
            {
                continue;
            }
            artifact.deadline = Some(at);
            artifact.revision += 1;
            artifact.updated_at = at;
            save_artifact(&tx, &artifact)?;
            event(
                &tx,
                &artifact.id,
                "retention",
                "curator-admitted failure follows failure_action",
            )?;
            due += 1;
        }
        tx.commit()?;
        Ok(due)
    }

    /// W8: re-snapshot a failed source's file inventory once, for an
    /// advisory assessment that found it drifted (a PAR repair rewrote a
    /// part, a file vanished). Allowed for the owned, live source only.
    pub fn refresh_failed_source(&self, job: u32) -> Result<Artifact> {
        let Some(mut source) = self.job_source(job)? else {
            return Err(Error::NotFound);
        };
        if !source.owned || source.terminal() || source.hold.is_some() {
            return Err(Error::Conflict(
                "failed source is not an owned live generation".into(),
            ));
        }
        let original = source.clone();
        let dir = self.verify(&source)?;
        source.files = fs::manifest(&dir, 100_000)?;
        source.inspected_at = Some(now());
        source.revision += 1;
        source.updated_at = now();
        let _coordinator = self.mutation_guard()?;
        self.check_payload_capture(&original)?;
        save_artifact(&self.db.lock().unwrap(), &source)?;
        Ok(source)
    }

    /// W8: a kept failure's bytes stay where they are. Recording that is a
    /// state change, not a re-capture of the directory: a drifted or
    /// partly missing source must not keep the follow-up looping.
    pub fn retain_kept_failure(&self, job: u32, path: &Path) -> Result<bool> {
        let _guard = self.mutation_guard()?;
        let Some(mut artifact) = self.for_job(job)? else {
            return Ok(false);
        };
        if artifact.path != path || !artifact.owned {
            return Ok(false);
        }
        if artifact.state == "retained" {
            return Ok(true);
        }
        if !matches!(
            artifact.state.as_str(),
            "active" | "transitioning" | "retiring"
        ) {
            return Ok(false);
        }
        artifact.state = "retained".into();
        artifact.revision += 1;
        artifact.updated_at = now();
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &artifact)?;
        event(&tx, &artifact.id, "finalized", "retained")?;
        tx.commit()?;
        Ok(true)
    }

    /// Record the disposal of a failed source; disposal is then done.
    pub fn record_disposal(&self, job: u32, note: &str) -> Result<()> {
        let db = self.db.lock().unwrap();
        db.execute(
            "UPDATE job_terminals SET disposal=?2,updated_at=?3 WHERE job=?1",
            rusqlite::params![job, note, now()],
        )?;
        Ok(())
    }

    /// The disposal gate (contract §2.5–2.6): `Some(terminal)` only when the
    /// job has a terminal record, its event has been emitted, its salvage
    /// obligation is no longer pending and no publication move of the job is
    /// still open (reconciliation has not yet finished or unwound it).
    pub fn disposal_allowed(&self, job: u32) -> Result<Option<JobTerminal>> {
        if self.open_intent_for_job(job)? {
            return Ok(None);
        }
        Ok(self
            .terminal(job)?
            .filter(|t| t.emitted && t.salvage != SalvageState::Pending))
    }

    /// Test support: the next `record_terminal` replay for `job` stops right
    /// after `after`, as a crash there would.
    #[cfg(feature = "test-support")]
    pub fn inject_terminal_crash(&self, job: u32, after: TerminalStep) {
        TERMINAL_CRASHES.lock().unwrap().insert(job, after.key());
    }

    /// The job's source artifact by role (see `set_artifact_salvage`): a
    /// salvage delivery's `sourceArtifactId`, else the job's row that is not
    /// a publication.
    pub fn job_source(&self, job: u32) -> Result<Option<super::Artifact>> {
        let raw: Option<String> = self.db.lock().unwrap().query_row(
            "SELECT data FROM artifacts WHERE id=COALESCE(
                (SELECT json_extract(d.data,'$.sourceArtifactId') FROM deliveries d JOIN artifacts p ON p.id=d.artifact WHERE p.job=?1 ORDER BY d.sequence DESC LIMIT 1),
                (SELECT a.id FROM artifacts a WHERE a.job=?1 AND json_extract(a.data,'$.state')<>'transferred' AND a.id NOT IN (SELECT artifact FROM deliveries) ORDER BY a.updated_at DESC LIMIT 1))",
            [job],
            |row| row.get(0),
        ).optional()?;
        Ok(raw.map(|raw| serde_json::from_str(&raw)).transpose()?)
    }

    /// Review #280-7: when the job's failed-source disposal may next run.
    /// Disposal is an inventory `delete` (or a park `relocate`) operation
    /// whose own attempts / `next_retry` ladder is durable and listed with
    /// the artifact (`/api/v1/artifacts`); this is the latest such retry
    /// still pending for any of the job's artifacts, if one is.
    pub fn disposal_retry_at(&self, job: u32) -> Result<Option<i64>> {
        let db = self.db.lock().unwrap();
        Ok(db.query_row(
            "SELECT MAX(json_extract(o.data,'$.next_retry')) FROM operations o JOIN artifacts a ON a.id=o.artifact WHERE a.job=?1 AND o.state NOT IN ('succeeded','cancelled') AND json_extract(o.data,'$.kind') IN ('delete','relocate') AND json_extract(o.data,'$.next_retry')>0",
            [job],
            |row| row.get::<_, Option<i64>>(0),
        )?)
    }

    /// Test support: the next failed-source disposal for `job` loses its
    /// post-processing admission right after the move/delete, before the
    /// outcome is recorded (as an owner fence or shutdown would).
    #[cfg(feature = "test-support")]
    pub fn inject_disposal_admission_loss(&self, job: u32) {
        DISPOSAL_ADMISSION_LOSSES.lock().unwrap().insert(job);
    }

    /// Consulted by disposal after the move. Always false outside the
    /// `test-support` feature.
    pub fn take_injected_disposal_admission_loss(&self, job: u32) -> bool {
        #[cfg(feature = "test-support")]
        {
            return DISPOSAL_ADMISSION_LOSSES.lock().unwrap().remove(&job);
        }
        #[allow(unreachable_code)]
        {
            let _ = job;
            false
        }
    }

    /// Whether a failed-source disposal operation (`delete`, or a park
    /// `relocate`) for any of the job's artifacts is still unresolved. Its
    /// payload is then held by that operation — possibly already moved but
    /// not committed — not absent.
    pub fn disposal_in_flight(&self, job: u32) -> Result<bool> {
        let db = self.db.lock().unwrap();
        Ok(db.query_row(
            "SELECT EXISTS(SELECT 1 FROM operations o JOIN artifacts a ON a.id=o.artifact WHERE a.job=?1 AND o.state NOT IN ('succeeded','cancelled') AND json_extract(o.data,'$.kind') IN ('delete','relocate'))",
            [job],
            |row| row.get(0),
        )?)
    }

    /// Operator Retry now for a failed source's disposal: the job's pending
    /// park relocation becomes due on its own ladder and is replayed now
    /// (the same single-operation resume a post-processing retry uses). A
    /// move that ran but did not commit is committed where its bytes are;
    /// anything the relocation cannot resume is left on its ladder.
    pub fn retry_disposal_now(&self, job: u32) -> Result<()> {
        self.resume_post_relocation_checked(job, &|| Ok(()), true)
    }

    /// Test support: the next `record_terminal` for `job` fails before the
    /// row is written, as a refused write would.
    #[cfg(feature = "test-support")]
    pub fn inject_terminal_insert_failure(&self, job: u32) {
        TERMINAL_INSERT_FAILURES.lock().unwrap().insert(job);
    }

    /// Consulted by the terminal orchestration between steps. Always false
    /// outside the `test-support` feature.
    pub fn take_injected_terminal_crash(&self, job: u32, after: &TerminalStep) -> bool {
        #[cfg(feature = "test-support")]
        {
            let mut crashes = TERMINAL_CRASHES.lock().unwrap();
            if crashes.get(&job) == Some(&after.key()) {
                crashes.remove(&job);
                return true;
            }
        }
        let _ = (job, after.key());
        false
    }
}

#[cfg(feature = "test-support")]
static TERMINAL_CRASHES: std::sync::LazyLock<Mutex<std::collections::HashMap<u32, u8>>> =
    std::sync::LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));
#[cfg(feature = "test-support")]
static DISPOSAL_ADMISSION_LOSSES: std::sync::LazyLock<Mutex<std::collections::HashSet<u32>>> =
    std::sync::LazyLock::new(|| Mutex::new(std::collections::HashSet::new()));
#[cfg(feature = "test-support")]
static TERMINAL_INSERT_FAILURES: std::sync::LazyLock<Mutex<std::collections::HashSet<u32>>> =
    std::sync::LazyLock::new(|| Mutex::new(std::collections::HashSet::new()));

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn entry(job: u32) -> crate::HistoryEntry {
        crate::HistoryEntry {
            job: nzbd_types::JobId(job),
            name: "x".into(),
            category: None,
            final_dir: None,
            status: "UNPACK_FAILURE".into(),
            size: 0,
            health: 1000,
            params: vec![],
            dupe_key: String::new(),
            dupe_score: 0,
            completed_at_unix: 1,
            hidden: false,
            first_seen_at_unix: None,
            last_seen_at_unix: None,
            seen_count: 0,
            removed_at_unix: None,
            picked_up_by: None,
            record: None,
            stages: vec![],
            seq: 0,
        }
    }

    pub(crate) fn failed(job: u32, salvage: SalvageState) -> JobTerminal {
        JobTerminal {
            job,
            transfer: Some("t-1".into()),
            outcome: TerminalOutcome::Failed,
            reason: Some(FailureReason::Unpack),
            fetch_cause: None,
            stage: Some("unpack".into()),
            message: Some("bad archive".into()),
            blames_release: true,
            salvage,
            keep_on_failure: false,
            fetched: Fetched::default(),
            publication: None,
            markers: vec![("*PP:done".into(), "UNPACK_FAILURE".into())],
            entry: entry(job),
            history_seq: None,
            history_incarnation: None,
            markers_stamped: false,
            emitted: false,
            disposal: None,
            salvage_error: None,
            created_at: 0,
            updated_at: 0,
        }
    }

    #[test]
    fn terminal_row_is_written_once_and_replays_from_its_flags() {
        let dir = tempfile::tempdir().unwrap();
        let inventory = Inventory::open(dir.path()).unwrap();
        // No artifact: a pending obligation settles to none at record time.
        let row = inventory
            .record_terminal(&failed(7, SalvageState::Pending))
            .unwrap();
        assert_eq!(row.salvage, SalvageState::None);
        assert_eq!(inventory.unfinished_terminals().unwrap().len(), 1);
        // A second producer for the same job gets the stored row back.
        let mut other = failed(7, SalvageState::None);
        other.reason = Some(FailureReason::Health);
        let again = inventory.record_terminal(&other).unwrap();
        assert_eq!(again.reason, Some(FailureReason::Unpack));
        inventory
            .mark_terminal_step(
                7,
                &TerminalStep::HistoryWritten {
                    seq: 41,
                    incarnation: "abc".into(),
                },
            )
            .unwrap();
        inventory
            .mark_terminal_step(7, &TerminalStep::MarkersStamped)
            .unwrap();
        assert!(inventory.disposal_allowed(7).unwrap().is_none());
        inventory
            .mark_terminal_step(7, &TerminalStep::Emitted)
            .unwrap();
        assert!(inventory.unfinished_terminals().unwrap().is_empty());
        let row = inventory.disposal_allowed(7).unwrap().unwrap();
        assert_eq!(row.history_seq, Some(41));
        assert_eq!(inventory.pending_disposal().unwrap().len(), 1);
        inventory.record_disposal(7, "parked at /x").unwrap();
        assert!(inventory.pending_disposal().unwrap().is_empty());
    }

    /// Contract §2.5: with the terminal emitted and salvage settled, an open
    /// publication intent alone still refuses disposal; once reconciliation
    /// resolves it, disposal is allowed.
    #[test]
    fn open_intent_alone_blocks_disposal() {
        let dir = tempfile::tempdir().unwrap();
        let inventory = Inventory::open(dir.path()).unwrap();
        inventory
            .record_terminal(&failed(8, SalvageState::None))
            .unwrap();
        for step in [
            TerminalStep::HistoryWritten {
                seq: 3,
                incarnation: "abc".into(),
            },
            TerminalStep::MarkersStamped,
            TerminalStep::Emitted,
        ] {
            inventory.mark_terminal_step(8, &step).unwrap();
        }
        assert!(inventory.disposal_allowed(8).unwrap().is_some());
        let intent = super::super::RelocationIntent {
            generation: "g8".into(),
            job: 8,
            transfer: None,
            artifact: "a8".into(),
            operation: "op8".into(),
            source_dir: dir.path().join("src"),
            target_dir: dir.path().join("dst"),
            cross_volume: false,
            files: Vec::new(),
            state: super::super::IntentState::Open,
            evidence: None,
            created_at: 0,
            updated_at: 0,
        };
        super::super::intents::insert_intent(&inventory.db.lock().unwrap(), &intent).unwrap();
        assert!(inventory.disposal_allowed(8).unwrap().is_none());
        inventory
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE relocation_intents SET state='unwound' WHERE generation='g8'",
                [],
            )
            .unwrap();
        assert!(inventory.disposal_allowed(8).unwrap().is_some());
    }

    #[test]
    fn failure_reason_is_mirrored_onto_the_source_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("processing");
        let path = root.join("job-9");
        std::fs::create_dir_all(&root).unwrap();
        let inventory = Inventory::open(dir.path()).unwrap();
        let artifact = inventory.allocate(9, &root, &path).unwrap();
        assert_eq!(artifact.failure_reason, None);
        inventory
            .record_terminal(&failed(9, SalvageState::Pending))
            .unwrap();
        let after = inventory.get(&artifact.id).unwrap();
        assert_eq!(after.failure_reason.as_deref(), Some("unpack"));
        assert_eq!(after.salvage.as_deref(), Some("pending"));
    }

    #[test]
    fn failure_reason_skips_a_publication_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("processing");
        std::fs::create_dir_all(&root).unwrap();
        let inventory = Inventory::open(dir.path()).unwrap();
        let source = inventory.allocate(11, &root, &root.join("job-11")).unwrap();
        // A newer artifact for the same job that is a publication.
        let publication = inventory
            .allocate(12, &root, &root.join("job-11-pub"))
            .unwrap();
        {
            let db = inventory.db.lock().unwrap();
            db.execute(
                "UPDATE artifacts SET job=11,data=json_set(data,'$.job',11,'$.state','transferred','$.owned',json('false')),updated_at=?2 WHERE id=?1",
                rusqlite::params![publication.id, now() + 100],
            )
            .unwrap();
        }
        inventory
            .record_terminal(&failed(11, SalvageState::None))
            .unwrap();
        assert_eq!(
            inventory.get(&source.id).unwrap().failure_reason.as_deref(),
            Some("unpack")
        );
        assert_eq!(inventory.get(&publication.id).unwrap().failure_reason, None);
    }
}
