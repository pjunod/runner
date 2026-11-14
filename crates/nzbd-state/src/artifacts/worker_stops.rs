//! Small node-local start/stop receipts. They confer no byte ownership.
use super::*;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerStopRequest {
    pub job: u32,
    pub context: WorkerControlContext,
    pub owner_incarnation: String,
    pub storage_instance_id: String,
    pub removal_revision: Option<u64>,
    pub authority_source_json: Option<String>,
    pub start_ordered: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerStoppedReceipt {
    pub id: String,
    pub job: u32,
    pub context: WorkerControlContext,
    pub owner_incarnation: String,
    pub storage_instance_id: String,
    pub authority_source_json: Option<String>,
    pub sources: Vec<SourceGeneration>,
    pub stopped_at_ms: i64,
    pub never_started: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GrantJournal {
    request: WorkerStopRequest,
    session: String,
    phase: String,
    #[serde(default)]
    never_started: bool,
    #[serde(default)]
    receipt: Option<WorkerStoppedReceipt>,
}
#[derive(Clone, Debug)]
pub struct WorkerStopTicket {
    pub request: WorkerStopRequest,
    pub never_started: bool,
    pub recovered_session: bool,
    pub receipt: Option<WorkerStoppedReceipt>,
}
pub fn same_worker_operation(left: &WorkerControlContext, right: &WorkerControlContext) -> bool {
    left.lease_id == right.lease_id
        && left.job_incarnation == right.job_incarnation
        && left.job_revision == right.job_revision
        && left.control_revision == right.control_revision
}
fn key(job: u32, context: &WorkerControlContext) -> String {
    format!(
        "worker-stop:{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                job,
                &context.lease_id,
                &context.job_incarnation,
                context.job_revision,
                context.control_revision
            ))
            .expect("bounded grant identity serializes")
        )
    )
}
fn validate(request: &WorkerStopRequest, installation: &str) -> Result<()> {
    if request.job == 0
        || request.storage_instance_id != installation
        || request.owner_incarnation.is_empty()
        || request.owner_incarnation.len() > 128
        || request.context.lease_id.is_empty()
        || request.context.lease_id.len() > 256
        || request.context.job_incarnation.is_empty()
        || request.context.job_incarnation.len() > 256
        || request.context.job_revision == 0
        || request.context.control_revision == 0
        || request.context.job_revision > i64::MAX as u64
        || request.context.control_revision > i64::MAX as u64
        || request.removal_revision.is_some_and(|revision| {
            revision <= request.context.job_revision || revision > i64::MAX as u64
        })
        || request.authority_source_json.as_ref().is_some_and(|value| {
            value.len() > 8192 || serde_json::from_str::<serde_json::Value>(value).is_err()
        })
    {
        return Err(Error::Conflict(
            "worker stop has unsupported or mismatched grant/storage identity".into(),
        ));
    }
    Ok(())
}
fn load(db: &Connection, id: &str) -> Result<Option<GrantJournal>> {
    let raw: Option<String> = db
        .query_row(
            "SELECT data FROM worker_grant_stops WHERE id=?1",
            [id],
            |row| row.get(0),
        )
        .optional()?;
    raw.map(|raw| serde_json::from_str(&raw).map_err(Into::into))
        .transpose()
}
fn save(db: &Connection, id: &str, record: &GrantJournal) -> Result<()> {
    let data = serde_json::to_string(record)?;
    if data.len() > 16384 {
        return Err(Error::Conflict(
            "worker stop receipt exceeds16KiB bound".into(),
        ));
    }
    db.execute("INSERT INTO worker_grant_stops(id,job,incarnation,lease,revision,control_revision,phase,data) VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(id) DO UPDATE SET phase=excluded.phase,data=excluded.data",params![id,record.request.job,&record.request.context.job_incarnation,&record.request.context.lease_id,record.request.context.job_revision as i64,record.request.context.control_revision as i64,&record.phase,data])?;
    Ok(())
}
impl Inventory {
    /// Capture only the recorded owned manifest or the exact writer names of
    /// the removed job, after the peer caller has joined every grant writer.
    /// Unknown members are retained; this does not discover/adopt arbitrary files.
    pub fn cluster_cancellation_file_seal(
        &self,
        copy: &ClusterArtifactCopy,
        writer_paths: &[String],
    ) -> Result<Vec<DeliveryFile>> {
        self.cluster_owned_copy_file_seal(copy, writer_paths)
    }

    /// Seal existing owned members after the caller has joined the generation's
    /// writers. This never adopts unknown files or grants deletion authority.
    pub fn cluster_owned_copy_file_seal(
        &self,
        copy: &ClusterArtifactCopy,
        writer_paths: &[String],
    ) -> Result<Vec<DeliveryFile>> {
        use std::io::Read;
        let _guard = self.mutation_guard()?;
        let artifact = self.get(&copy.publication.artifact)?;
        if copy.storage_instance_id != self.installation
            || artifact.generation != copy.publication.generation
            || artifact.job != Some(copy.job_id)
            || !artifact.owned
            || !artifact.automatic_cleanup_eligible
            || self.artifact_in_use(&artifact)?
        {
            return Err(Error::Conflict(
                "owned copy identity/use changed; retain for review".into(),
            ));
        }
        let directory = self.verify(&artifact)?;
        let observed = fs::manifest(&directory, 100_000)?;
        let recorded: std::collections::HashMap<_, _> = artifact
            .files
            .iter()
            .map(|entry| (entry.path.as_str(), entry))
            .collect();
        let paths: std::collections::HashSet<&str> =
            writer_paths.iter().map(String::as_str).collect();
        let allowed: std::collections::HashSet<&str> =
            if artifact.files.is_empty() && artifact.state == "active" {
                paths
            } else {
                recorded.keys().copied().collect()
            };
        let mut directories = std::collections::HashSet::new();
        for path in &allowed {
            let mut parent = Path::new(path).parent();
            while let Some(path) = parent.filter(|p| !p.as_os_str().is_empty()) {
                directories.insert(path.to_string_lossy().into_owned());
                parent = path.parent();
            }
        }
        let mut files = Vec::new();
        for entry in observed {
            if entry.identity.directory {
                if !directories.contains(&entry.path) {
                    return Err(Error::Conflict(format!(
                        "unrecorded directory {}; source retained",
                        entry.path
                    )));
                }
                continue;
            }
            if !allowed.contains(entry.path.as_str())
                || recorded
                    .get(entry.path.as_str())
                    .is_some_and(|known| known.identity != entry.identity)
            {
                return Err(Error::Conflict(format!(
                    "unrecorded/changed file {}; source retained",
                    entry.path
                )));
            }
            let mut input = fs::open_relative(&directory, &entry.path)?;
            let mut hash = Sha256::new();
            let mut buffer = [0u8; 65536];
            loop {
                let count = input.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                hash.update(&buffer[..count]);
            }
            if fs::identity_for_file(&input)? != entry.identity {
                return Err(Error::Conflict(
                    "file changed while sealing stopped source".into(),
                ));
            }
            files.push(DeliveryFile {
                path: entry.path,
                size_bytes: entry.identity.bytes,
                sha256: format!("{:x}", hash.finalize()),
            });
        }
        Ok(files)
    }
    /// Resolve a previously journaled exact lease without reconstructing its
    /// renewed epoch from a caller. Missing means never-started only when the
    /// immutable authority grant advertised start ordering.
    pub fn worker_grant_request_by_lease(
        &self,
        job: u32,
        incarnation: &str,
        lease: &str,
    ) -> Result<Option<WorkerStopRequest>> {
        let db = self.db.lock().unwrap();
        let raw: Option<String> = db
            .query_row(
                "SELECT data FROM worker_grant_stops WHERE job=?1 AND incarnation=?2 AND lease=?3",
                params![job, incarnation, lease],
                |row| row.get(0),
            )
            .optional()?;
        raw.map(|raw| {
            serde_json::from_str::<GrantJournal>(&raw)
                .map(|record| record.request)
                .map_err(Into::into)
        })
        .transpose()
    }
    /// This commit precedes private allocation/copy/import/writer creation.
    /// A removal fence and a late grant start serialize on the same guard.
    pub fn begin_worker_grant(&self, request: &WorkerStopRequest) -> Result<()> {
        validate(request, &self.installation)?;
        if request.removal_revision.is_some() || !request.start_ordered {
            return Err(Error::Conflict(
                "grant start requires current start-ordering protocol".into(),
            ));
        }
        let _guard = self.mutation_guard()?;
        let db = self.db.lock().unwrap();
        if self.worker_grant_is_stopped_unlocked(&db, request.job, &request.context)? {
            return Err(Error::Conflict(
                "this work was durably stopped; refuse the late grant".into(),
            ));
        }
        let id = key(request.job, &request.context);
        if let Some(previous) = load(&db, &id)? {
            if previous.request.owner_incarnation != request.owner_incarnation
                || previous.request.authority_source_json != request.authority_source_json
            {
                return Err(Error::Conflict("grant start identity changed".into()));
            }
            return Err(Error::Conflict("grant start already recorded; replay its established work rather than starting it twice".into()));
        }
        save(
            &db,
            &id,
            &GrantJournal {
                request: request.clone(),
                session: self.worker_session.clone(),
                phase: "started".into(),
                never_started: false,
                receipt: None,
            },
        )
    }
    pub(super) fn worker_grant_is_stopped_unlocked(
        &self,
        db: &Connection,
        job: u32,
        context: &WorkerControlContext,
    ) -> Result<bool> {
        Ok(db.query_row("SELECT EXISTS(SELECT 1 FROM worker_removal_fences WHERE job=?1 AND incarnation=?2 AND revision>=?3) OR EXISTS(SELECT 1 FROM worker_grant_stops WHERE job=?1 AND incarnation=?2 AND lease=?4 AND revision=?3 AND control_revision=?5 AND phase IN('stopping','stopped'))",params![job,&context.job_incarnation,context.job_revision as i64,&context.lease_id,context.control_revision as i64],|row|row.get(0))?)
    }
    pub fn worker_grant_is_stopped(
        &self,
        job: u32,
        context: &WorkerControlContext,
    ) -> Result<bool> {
        self.worker_grant_is_stopped_unlocked(&self.db.lock().unwrap(), job, context)
    }
    pub fn worker_grant_request(
        &self,
        job: u32,
        context: &WorkerControlContext,
    ) -> Result<Option<WorkerStopRequest>> {
        Ok(load(&self.db.lock().unwrap(), &key(job, context))?.map(|record| record.request))
    }
    /// Establish the late-start fence before joining any writer. Absent start
    /// history proves nothing unless the immutable grant promised ordering.
    pub fn prepare_worker_stop(&self, request: &WorkerStopRequest) -> Result<WorkerStopTicket> {
        validate(request, &self.installation)?;
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let id = key(request.job, &request.context);
        let previous = load(&tx, &id)?;
        let never_started = previous.as_ref().is_none_or(|record| record.never_started);
        if never_started && (!request.start_ordered || request.removal_revision.is_none()) {
            return Err(Error::Conflict(
                "legacy/unproven grant has no safe start ordering; retain for review".into(),
            ));
        }
        let mut record = previous.unwrap_or(GrantJournal {
            request: request.clone(),
            session: self.worker_session.clone(),
            phase: "stopping".into(),
            never_started: true,
            receipt: None,
        });
        if record.request.owner_incarnation != request.owner_incarnation
            || record.request.authority_source_json != request.authority_source_json
            || !same_worker_operation(&record.request.context, &request.context)
        {
            return Err(Error::Conflict(
                "worker stop differs from its recorded grant".into(),
            ));
        }
        if let Some(revision) = request.removal_revision {
            tx.execute("INSERT INTO worker_removal_fences(job,incarnation,revision) VALUES(?1,?2,?3) ON CONFLICT(job,incarnation) DO UPDATE SET revision=max(revision,excluded.revision)",params![request.job,&request.context.job_incarnation,revision as i64])?;
        }
        let receipt = record.receipt.clone();
        let recovered_session = record.session != self.worker_session;
        if receipt.is_none() {
            record.phase = "stopping".into();
            save(&tx, &id, &record)?;
        }
        tx.commit()?;
        Ok(WorkerStopTicket {
            request: request.clone(),
            never_started,
            recovered_session,
            receipt,
        })
    }
    /// Caller supplies an actor-confirmed, actually joined watch set. This
    /// small receipt replaces the in-memory watches; it authorizes no files.
    pub fn commit_worker_stopped(
        &self,
        ticket: &WorkerStopTicket,
        sources: &[SourceGeneration],
    ) -> Result<WorkerStoppedReceipt> {
        validate(&ticket.request, &self.installation)?;
        if sources.len() > 32 {
            return Err(Error::Conflict(
                "worker stop source set exceeds32 generations; retain for review".into(),
            ));
        }
        let _guard = self.mutation_guard()?;
        let db = self.db.lock().unwrap();
        let id = key(ticket.request.job, &ticket.request.context);
        let mut record = load(&db, &id)?.ok_or(Error::NotFound)?;
        if let Some(receipt) = record.receipt {
            return Ok(receipt);
        }
        if record.phase != "stopping"
            || record.request.owner_incarnation != ticket.request.owner_incarnation
            || record.request.authority_source_json != ticket.request.authority_source_json
        {
            return Err(Error::Conflict(
                "worker stop changed before its joined receipt".into(),
            ));
        }
        let receipt = WorkerStoppedReceipt {
            id: id.clone(),
            job: ticket.request.job,
            context: ticket.request.context.clone(),
            owner_incarnation: ticket.request.owner_incarnation.clone(),
            storage_instance_id: self.installation.clone(),
            authority_source_json: ticket.request.authority_source_json.clone(),
            sources: sources.to_vec(),
            stopped_at_ms: now_millis(),
            never_started: ticket.never_started,
        };
        record.phase = "stopped".into();
        record.receipt = Some(receipt.clone());
        save(&db, &id, &record)?;
        Ok(receipt)
    }
}
