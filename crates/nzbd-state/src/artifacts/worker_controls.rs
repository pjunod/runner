//! Node-local durable controls for a leased worker's exact source generation.
use super::*;

/// Stable authority tuple. Lease renewal expiry is deliberately absent: a
/// renewal retains this work, while new leader intent or reassignment does not.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkerControlContext {
    pub lease_id: String,
    pub job_incarnation: String,
    pub job_revision: u64,
    pub control_revision: u64,
    pub epoch: u64,
}

pub const WORKER_CONTROL_CONTEXT_PARAM: &str = "*Cluster:ControlContext";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerControlCheckpoint {
    pub context: WorkerControlContext,
    pub source: SourceGeneration,
    pub control: nzbd_types::JobControl,
    pub probe_params: Vec<(String, String)>,
}

impl WorkerControlContext {
    pub fn for_job(job: &nzbd_types::Job) -> Option<Self> {
        let values = job
            .params
            .iter()
            .filter(|(key, _)| key == WORKER_CONTROL_CONTEXT_PARAM)
            .collect::<Vec<_>>();
        if values.len() != 1 || values[0].1.len() > 4096 {
            return None;
        }
        let context: Self = serde_json::from_str(&values[0].1).ok()?;
        if context.lease_id.is_empty()
            || context.lease_id.len() > 256
            || context.job_incarnation.is_empty()
            || context.job_incarnation.len() > 256
        {
            return None;
        }
        Some(context)
    }
}

impl Inventory {
    /// Bind an externally validated grant before publishing a volatile worker
    /// queue row. This prevents a later restart replaying an older checkpoint.
    pub fn bind_worker_context(&self, job: u32, context: &WorkerControlContext) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let db = self.db.lock().unwrap();
        let raw: Option<String> = db
            .query_row(
                "SELECT data FROM worker_authority WHERE job=?1",
                [job],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(raw) = raw {
            let previous: WorkerControlContext = serde_json::from_str(&raw)?;
            if previous.job_incarnation == context.job_incarnation
                && (context.epoch < previous.epoch
                    || context.job_revision < previous.job_revision
                    || context.control_revision < previous.control_revision)
            {
                return Err(Error::Conflict("stale worker grant authority".into()));
            }
        }
        db.execute("INSERT INTO worker_authority(job,data) VALUES(?1,?2) ON CONFLICT(job) DO UPDATE SET data=excluded.data", params![job, serde_json::to_string(context)?])?;
        Ok(())
    }

    pub fn checkpoint_worker_control(
        &self,
        job: &nzbd_types::Job,
        expected_revision: &str,
    ) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let context = WorkerControlContext::for_job(job)
            .ok_or_else(|| Error::Conflict("worker control has no lease authority".into()))?;
        let source = self.for_job(job.id.0)?.ok_or(Error::NotFound)?;
        if !source.owned || source.terminal() {
            return Err(Error::Conflict(
                "worker control requires an owned source generation".into(),
            ));
        }
        self.verify(&source)?;
        let control = job
            .control()
            .ok_or_else(|| Error::Conflict("worker control missing".into()))?;
        let source_generation = SourceGeneration {
            artifact: source.id,
            generation: source.generation,
        };
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let authority: String = tx.query_row(
            "SELECT data FROM worker_authority WHERE job=?1",
            [job.id.0],
            |row| row.get(0),
        )?;
        if serde_json::from_str::<WorkerControlContext>(&authority)? != context {
            return Err(Error::Conflict("worker grant authority changed".into()));
        }
        let raw: Option<String> = tx
            .query_row(
                "SELECT data FROM worker_controls WHERE job=?1",
                [job.id.0],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(raw) = raw {
            let previous: WorkerControlCheckpoint = serde_json::from_str(&raw)?;
            if previous.context == context
                && previous.source == source_generation
                && (previous.control.instance != control.instance
                    || previous.control.revision != expected_revision)
            {
                return Err(Error::Conflict("worker control revision changed".into()));
            }
        }
        let checkpoint = WorkerControlCheckpoint {
            context,
            source: source_generation,
            control,
            probe_params: job
                .params
                .iter()
                .filter(|(key, _)| {
                    matches!(key.as_str(), "*Control:attempts" | "*Control:probe_after")
                })
                .cloned()
                .collect(),
        };
        tx.execute("INSERT INTO worker_controls(job,data) VALUES(?1,?2) ON CONFLICT(job) DO UPDATE SET data=excluded.data", params![job.id.0, serde_json::to_string(&checkpoint)?])?;
        tx.commit()?;
        Ok(())
    }

    pub fn worker_control(
        &self,
        job: u32,
        context: &WorkerControlContext,
    ) -> Result<Option<WorkerControlCheckpoint>> {
        let raw: Option<String> = self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT data FROM worker_controls WHERE job=?1",
                [job],
                |row| row.get(0),
            )
            .optional()?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let checkpoint: WorkerControlCheckpoint = serde_json::from_str(&raw)?;
        if checkpoint.context != *context {
            return Ok(None);
        }
        let authority: Option<String> = self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT data FROM worker_authority WHERE job=?1",
                [job],
                |row| row.get(0),
            )
            .optional()?;
        if authority
            .as_deref()
            .map(serde_json::from_str::<WorkerControlContext>)
            .transpose()?
            .as_ref()
            != Some(context)
        {
            return Ok(None);
        }
        let Some(source) = self.for_job(job)? else {
            return Ok(None);
        };
        if source.id != checkpoint.source.artifact
            || source.generation != checkpoint.source.generation
            || !source.owned
            || source.terminal()
        {
            return Ok(None);
        }
        self.verify(&source)?;
        Ok(Some(checkpoint))
    }
}
