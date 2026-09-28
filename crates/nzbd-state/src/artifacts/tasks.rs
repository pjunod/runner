use super::*;

impl Inventory {
    pub fn configure_scan(&self, request: serde_json::Value) -> Result<()> {
        let _guard = self.mutation_guard()?;
        self.db.lock().unwrap().execute("INSERT INTO meta VALUES('scan_request',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [serde_json::to_string(&request)?])?;
        Ok(())
    }
    pub fn discovery_status(&self) -> Result<Option<Operation>> {
        let raw: Option<String> = self.db.lock().unwrap().query_row("SELECT data FROM operations WHERE json_extract(data,'$.kind')='scan' ORDER BY json_extract(data,'$.created_at') DESC LIMIT 1", [], |r| r.get(0)).optional()?;
        raw.map(|r| serde_json::from_str(&r).map_err(Error::from))
            .transpose()
    }
    pub(super) fn schedule_discovery(&self) -> Result<()> {
        if !self.settings()?.enabled {
            return Ok(());
        }
        if self.discovery_status()?.is_some_and(|o| {
            matches!(o.state.as_str(), "queued" | "running") || o.created_at > now() - 900
        }) {
            return Ok(());
        }
        let request: Option<String> = self
            .db
            .lock()
            .unwrap()
            .query_row("SELECT value FROM meta WHERE key='scan_request'", [], |r| {
                r.get(0)
            })
            .optional()?;
        if let Some(raw) = request {
            self.submit_task(
                "scan",
                "installation",
                &format!("periodic-scan-{}", now() / 900),
                serde_json::from_str(&raw)?,
            )?;
        }
        Ok(())
    }

    /// Durable admission for filesystem inspection and recovery copying. A
    /// 202 response always names a persisted task, including before a restart.
    pub fn submit_task(
        &self,
        kind: &str,
        artifact: &str,
        request_id: &str,
        request: serde_json::Value,
    ) -> Result<Operation> {
        if !matches!(kind, "inspect" | "scan" | "stage" | "prune") {
            return Err(Error::Conflict("unknown task kind".into()));
        }
        let _guard = self.mutation_guard()?;
        let db = self.db.lock().unwrap();
        let key = if request_id.is_empty() {
            id(&db)?
        } else {
            request_id.into()
        };
        let request = serde_json::to_string(&request)?;
        match read::<Operation>(&db, "operations", &key) {
            Ok(op) => {
                return if op.request == request && op.kind == kind && op.artifact == artifact {
                    Ok(op)
                } else {
                    Err(Error::Conflict("idempotency key reused".into()))
                }
            }
            Err(Error::NotFound) => {}
            Err(e) => return Err(e),
        }
        let op = Operation {
            id: key,
            artifact: artifact.into(),
            kind: kind.into(),
            state: "queued".into(),
            request,
            created_at: now(),
            not_before: now() + if kind == "prune" { 8 } else { 0 },
            attempts: 0,
            next_retry: 0,
            error: None,
        };
        save_operation(&db, &op)?;
        Ok(op)
    }
    pub fn execute_task(&self, key: &str) -> Result<Operation> {
        let mut op = self.operation(key)?;
        if !matches!(op.state.as_str(), "queued" | "running") || op.not_before > now() {
            return Ok(op);
        }
        op.state = "running".into();
        op.attempts += 1;
        save_operation(&self.db.lock().unwrap(), &op)?;
        let request: serde_json::Value = serde_json::from_str(&op.request)?;
        let result = match op.kind.as_str() {
            "prune" => self.prune_receipted_source(
                request["recovery"]
                    .as_str()
                    .ok_or_else(|| Error::Conflict("missing recovery".into()))?,
            ),
            "inspect" => self.inspect(&op.artifact).map(|_| ()),
            "stage" => (|| {
                let revision = request["revision"]
                    .as_u64()
                    .ok_or_else(|| Error::Conflict("missing revision".into()))?;
                let files: Vec<String> = serde_json::from_value(request["files"].clone())?;
                let root: PathBuf = serde_json::from_value(request["root"].clone())?;
                let recovery =
                    self.stage_recovery(&op.artifact, revision, &op.id, &files, &root)?;
                if recovery.state != "published" {
                    return Err(Error::Conflict(
                        recovery
                            .error
                            .unwrap_or_else(|| format!("recovery {}", recovery.state)),
                    ));
                }
                Ok(())
            })(),
            "scan" => (|| {
                let roots: Vec<PathBuf> = serde_json::from_value(request["roots"].clone())?;
                let excluded: Vec<PathBuf> = serde_json::from_value(request["excluded"].clone())?;
                let active: Vec<PathBuf> = serde_json::from_value(request["active"].clone())?;
                let mut count = 0;
                let mut failures = Vec::new();
                for root in roots {
                    match fs::open_dir(&root) {
                        Ok(_) => {}
                        // A root that does not exist yet (the failed
                        // directory before the first parked failure) has
                        // nothing to find; it is not a scan failure.
                        Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(e) => {
                            failures.push(format!("{}: {e}", root.display()));
                            continue;
                        }
                    }
                    for entry in std::fs::read_dir(&root)? {
                        if count >= 2000 {
                            return Err(Error::Conflict(
                                "scan reached 2000 directories; results are incomplete".into(),
                            ));
                        }
                        let entry = entry?;
                        let path = entry.path();
                        if excluded.iter().any(|p| path == *p || p.starts_with(&path)) {
                            continue;
                        }
                        if !entry.file_type()?.is_dir() {
                            continue;
                        }
                        count += 1;
                        let a = self.discover(&root, &path, active.contains(&path))?;
                        // A row that says "0 files · 0 B" for a folder nobody
                        // has walked is a lie. Measure what discovery found,
                        // and carry a walk failure on the record rather than
                        // failing the whole scan for one bad folder.
                        if a.state == "unknown" && !a.measured() {
                            if let Err(e) = self.inspect(&a.id) {
                                tracing::warn!(path=%path.display(), error=%e, "discovered folder could not be measured");
                                let _ = self.note_error(&a.id, &e.to_string());
                            }
                        }
                    }
                }
                if !failures.is_empty() {
                    return Err(Error::Conflict(failures.join("; ")));
                }
                Ok(())
            })(),
            _ => Err(Error::Conflict("unknown task kind".into())),
        };
        match result {
            Ok(()) => {
                op.state = "succeeded".into();
                op.error = None;
            }
            Err(e) => {
                op.state = "failed".into();
                op.error = Some(e.to_string());
            }
        }
        save_operation(&self.db.lock().unwrap(), &op)?;
        Ok(op)
    }
    /// Drain every runnable task. Called from the 30 s maintenance tick and
    /// kicked directly by the API after admission, so a click does not wait
    /// for the tick; the runner lock keeps the two from executing the same
    /// task twice. Bounded so a task that re-queues itself cannot spin.
    pub fn run_tasks(&self) -> Result<()> {
        let _runner = self.tasks.lock().unwrap();
        for _ in 0..400 {
            let keys = {
                let db = self.db.lock().unwrap();
                let mut stmt=db.prepare("SELECT id FROM operations WHERE state IN ('queued','running') AND json_extract(data,'$.kind') IN ('inspect','scan','stage','prune') AND json_extract(data,'$.not_before')<=unixepoch() ORDER BY json_extract(data,'$.created_at') LIMIT 5")?;
                let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
                rows.collect::<std::result::Result<Vec<_>, _>>()?
            };
            if keys.is_empty() {
                return Ok(());
            }
            for key in keys {
                self.execute_task(&key)?;
            }
        }
        Ok(())
    }
}
