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
    pub fn settle_salvage(&self, job: u32, outcome: &SalvageOutcome) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
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

    /// Record the disposal of a failed source; disposal is then done.
    pub fn record_disposal(&self, job: u32, note: &str) -> Result<()> {
        let db = self.db.lock().unwrap();
        db.execute(
            "UPDATE job_terminals SET disposal=?2,updated_at=?3 WHERE job=?1",
            rusqlite::params![job, note, now()],
        )?;
        Ok(())
    }

    /// The disposal gate (contract §2.6): `Some(terminal)` only when the
    /// job has a terminal record, its event has been emitted and its salvage
    /// obligation is no longer pending.
    pub fn disposal_allowed(&self, job: u32) -> Result<Option<JobTerminal>> {
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
mod tests {
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
}
