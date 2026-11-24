//! A pending terminal obligation has a durable operation clock. Scans and
//! duplicate terminal events observe this clock; they do not start a retry.
use super::*;

fn operation_id(job: u32) -> String {
    format!("salvage:{job}")
}

impl Inventory {
    /// Database errors propagate: inability to read a clock is not authority
    /// to attempt work. Explicit Retry now does not bypass a salvage clock.
    pub fn salvage_retry_due(&self, job: u32) -> Result<bool> {
        let db = self.db.lock().unwrap();
        let retry: Option<i64> = db.query_row(
            "SELECT json_extract(data,'$.next_retry') FROM operations WHERE id=?1 AND state='retry'",
            [operation_id(job)], |row| row.get(0)).optional()?;
        Ok(retry.is_none_or(|at| at <= now()))
    }

    /// Reserve the next attempt before touching storage. A crash or a failed
    /// error write still leaves a durable backoff, never a rescan busy loop.
    pub fn begin_salvage_attempt(&self, job: u32) -> Result<bool> {
        let _guard = self.mutation_guard()?;
        let Some(terminal) = self.terminal(job)? else {
            return Ok(false);
        };
        if terminal.salvage != SalvageState::Pending || !terminal.emitted {
            return Ok(false);
        }
        let source = self.job_source(job)?.ok_or(Error::NotFound)?;
        let request =
            serde_json::to_string(&(terminal.created_at, &source.id, &source.generation))?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let raw: Option<String> = tx
            .query_row(
                "SELECT data FROM operations WHERE id=?1",
                [operation_id(job)],
                |r| r.get(0),
            )
            .optional()?;
        let mut op = if let Some(raw) = raw {
            let op: Operation = serde_json::from_str(&raw)?;
            if op.request != request {
                return Err(Error::Conflict(
                    "salvage source generation changed; obligation retained".into(),
                ));
            }
            if op.state != "retry" || op.next_retry > now() {
                return Ok(false);
            }
            op
        } else {
            Operation {
                id: operation_id(job),
                artifact: source.id,
                kind: "salvage".into(),
                state: "retry".into(),
                request,
                created_at: now(),
                not_before: 0,
                attempts: 0,
                next_retry: 0,
                error: None,
            }
        };
        op.attempts = op.attempts.saturating_add(1);
        op.next_retry = now().saturating_add(retry_delay(op.attempts));
        op.error = Some("salvage attempt reserved; obligation remains pending until publication or assessment commits".into());
        save_operation(&tx, &op)?;
        event(
            &tx,
            &op.artifact,
            "salvage_retry",
            &format!("attempt {}; next retry at {}", op.attempts, op.next_retry),
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub fn note_salvage_error(&self, job: u32, error: &str) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let db = self.db.lock().unwrap();
        let raw: Option<String> = db
            .query_row(
                "SELECT data FROM operations WHERE id=?1 AND state='retry'",
                [operation_id(job)],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(raw) = raw {
            let mut op: Operation = serde_json::from_str(&raw)?;
            op.error = Some(error.to_owned());
            save_operation(&db, &op)?;
        }
        Ok(())
    }
}
