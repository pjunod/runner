use super::*;

#[derive(Serialize, Deserialize)]
pub struct RetentionPreview {
    pub id: String,
    pub days: u32,
    pub entries: Vec<RetentionChange>,
    pub limit: usize,
}
#[derive(Serialize, Deserialize)]
pub struct RetentionChange {
    pub id: String,
    pub revision: u64,
    pub path: PathBuf,
    pub previous_seconds: u64,
}
impl Inventory {
    pub fn preview_retention(&self, days: u32) -> Result<RetentionPreview> {
        if days > 3650 {
            return Err(Error::Conflict("retention exceeds 3650 days".into()));
        }
        let _guard = self.mutation_guard()?;
        let db = self.db.lock().unwrap();
        let mut stmt = db.prepare("SELECT data FROM artifacts WHERE state='parked_failed' AND json_extract(data,'$.owned')=1 AND json_extract(data,'$.keep')=0 AND json_extract(data,'$.hold') IS NULL AND json_extract(data,'$.retention_seconds')<>?1 ORDER BY updated_at,id LIMIT 1000")?;
        let rows = stmt.query_map([i64::from(days) * 86400], |r| r.get::<_, String>(0))?;
        let mut entries = Vec::new();
        for raw in rows {
            let a: Artifact = serde_json::from_str(&raw?)?;
            entries.push(RetentionChange {
                id: a.id,
                revision: a.revision,
                path: a.path,
                previous_seconds: a.retention_seconds,
            });
        }
        let preview = RetentionPreview {
            id: id(&db)?,
            days,
            entries,
            limit: 1000,
        };
        save_operation(
            &db,
            &Operation {
                id: preview.id.clone(),
                artifact: "policy".into(),
                kind: "retention_preview".into(),
                state: "preview".into(),
                request: serde_json::to_string(&preview)?,
                created_at: now(),
                not_before: 0,
                attempts: 0,
                next_retry: 0,
                error: None,
            },
        )?;
        Ok(preview)
    }
    pub fn apply_retention(&self, key: &str) -> Result<Operation> {
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let mut op: Operation = read(&tx, "operations", key)?;
        if op.kind != "retention_preview" {
            return Err(Error::Conflict("not a retention preview".into()));
        }
        if op.state == "succeeded" {
            return Ok(op);
        }
        if op.state != "preview" {
            return Err(Error::Conflict("preview is no longer applicable".into()));
        }
        let preview: RetentionPreview = serde_json::from_str(&op.request)?;
        for change in preview.entries {
            let mut a: Artifact = read(&tx, "artifacts", &change.id)?;
            if a.revision != change.revision
                || a.keep
                || a.hold.is_some()
                || !a.owned
                || a.state != "parked_failed"
            {
                return Err(Error::Conflict(
                    "inventory changed; create a new retention preview".into(),
                ));
            }
            a.retention_seconds = u64::from(preview.days) * 86400;
            a.deadline = Some(now().saturating_add(a.retention_seconds as i64));
            a.revision += 1;
            a.updated_at = now();
            save_artifact(&tx, &a)?;
            event(&tx, &a.id, "retention_policy_applied", key)?;
        }
        op.state = "succeeded".into();
        save_operation(&tx, &op)?;
        tx.commit()?;
        Ok(op)
    }
    pub fn metrics(&self) -> Result<String> {
        use std::fmt::Write;
        let db = self.db.lock().unwrap();
        let mut text = String::new();
        for (table, name) in [
            ("artifacts", "nzbd_artifacts"),
            ("operations", "nzbd_artifact_operations"),
            ("recoveries", "nzbd_recoveries"),
        ] {
            writeln!(&mut text, "# TYPE {name} gauge").unwrap();
            let mut stmt = db.prepare(&format!(
                "SELECT state,count(*) FROM {table} GROUP BY state"
            ))?;
            for row in stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))? {
                let (state, count) = row?;
                if state.bytes().all(|b| b.is_ascii_lowercase() || b == b'_') {
                    writeln!(&mut text, "{name}{{state=\"{state}\"}} {count}").unwrap();
                }
            }
        }
        Ok(text)
    }
    /// Offline snapshot: the inventory process lock excludes the daemon and
    /// VACUUM INTO snapshots WAL contents without copying live SQLite files.
    pub fn backup(&self, output: &Path) -> Result<()> {
        let _guard = self.mutation_guard()?;
        fs::absolute(output)?;
        if output.starts_with(&self.state_dir) || self.state_dir.starts_with(output) {
            return Err(Error::Conflict(
                "backup must be outside the state directory".into(),
            ));
        }
        std::fs::create_dir(output)?;
        let result = (|| {
            let target = output.join("artifacts.sqlite");
            self.db
                .lock()
                .unwrap()
                .execute("VACUUM INTO ?1", [target.to_string_lossy()])?;
            File::open(&target)?.sync_all()?;
            let identities = self.state_dir.join("artifact-identities");
            if identities.try_exists()? {
                let root = fs::open_dir(&identities)?;
                let dest = output.join("artifact-identities");
                std::fs::create_dir(&dest)?;
                let entries = fs::manifest(&root, 1_000_000)?;
                for entry in &entries {
                    let path = dest.join(&entry.path);
                    if entry.identity.directory {
                        std::fs::create_dir(&path)?;
                    } else {
                        let mut input = fs::open_relative(&root, &entry.path)?;
                        let mut out = std::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(&path)?;
                        std::io::copy(&mut input, &mut out)?;
                        out.sync_all()?;
                    }
                }
                for entry in entries.iter().rev().filter(|e| e.identity.directory) {
                    fs::sync_directory(&fs::open_dir(&dest.join(&entry.path))?)?;
                }
                fs::sync_directory(&fs::open_dir(&dest)?)?;
            }
            let mut manifest = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(output.join("backup.json"))?;
            manifest.write_all(&serde_json::to_vec_pretty(&serde_json::json!({"installation":self.installation,"schema":1,"created_at":now(),"restore_requires_quarantine":true,"scope":"inventory and external allocation identities; payload volumes and queue/history are separate backups"}))?)?;
            manifest.sync_all()?;
            fs::sync_directory(&fs::open_dir(output)?)?;
            fs::sync_directory(&fs::open_dir(output.parent().unwrap())?)?;
            Ok(())
        })();
        // Never erase a partial backup recursively. Absence of backup.json
        // distinguishes incomplete snapshots for operator review.
        result
    }
}
