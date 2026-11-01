//! Foreign-journal preparation runs off the queue actor; apply is revision fenced.
use crate::{
    queue::{job_dir_name, recompute_job_totals},
    EngineError,
};
use nzbd_state::{
    artifacts::{Artifact, AttemptUse, Identity, Inventory},
    JobJournals,
};
use nzbd_types::{FileId, Job, JobId, SegmentState};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone)]
pub(crate) struct ImportAuthority(pub Arc<dyn Fn() -> bool + Send + Sync>);
impl Default for ImportAuthority {
    fn default() -> Self {
        Self(Arc::new(|| true))
    }
}
impl std::fmt::Debug for ImportAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ImportAuthority")
    }
}
impl ImportAuthority {
    pub(crate) fn valid(&self) -> bool {
        (self.0)()
    }
}

#[derive(Clone, PartialEq, Eq)]
struct SourceVersion {
    artifact: String,
    generation: String,
    revision: u64,
    path: PathBuf,
}
impl From<&Artifact> for SourceVersion {
    fn from(artifact: &Artifact) -> Self {
        Self {
            artifact: artifact.id.clone(),
            generation: artifact.generation.clone(),
            revision: artifact.revision,
            path: artifact.path.clone(),
        }
    }
}

pub(crate) struct PreparedImport {
    pub expected_intent: Option<String>,
    authority: ImportAuthority,
    source: Option<SourceVersion>,
    root: PathBuf,
    identity: Option<Identity>,
    _read_lease: Option<AttemptUse>,
    pub file_sizes: HashMap<FileId, u64>,
    pub imported_files: HashSet<FileId>,
}

impl std::fmt::Debug for PreparedImport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedImport")
            .field("root", &self.root)
            .field("file_count", &self.imported_files.len())
            .finish_non_exhaustive()
    }
}

pub(crate) fn intent(job: Option<&Job>) -> Option<String> {
    job.map(|job| {
        format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    job.id,
                    job.kind,
                    job.status,
                    &job.name,
                    &job.dir_name,
                    job.name_provisional,
                    job.queued_at_unix,
                    &job.category,
                    job.priority,
                    job.delivery_lifecycle,
                    &job.params,
                ))
                .expect("job intent fields are JSON encodable")
            )
        )
    })
}

#[cfg(test)]
pub(crate) static PREPARATION_GATE: std::sync::Mutex<
    Option<(PathBuf, Arc<(std::sync::Barrier, std::sync::Barrier)>)>,
> = std::sync::Mutex::new(None);
fn checkpoint(root: &Path) {
    #[cfg(test)]
    {
        let gate = {
            let mut configured = PREPARATION_GATE.lock().unwrap();
            if configured
                .as_ref()
                .is_some_and(|(expected, _)| expected == root)
            {
                configured.take().map(|(_, gate)| gate)
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.0.wait();
            gate.1.wait();
        }
    }
    let _ = root;
}

pub(crate) fn prepare(
    mut job: Job,
    expected_intent: Option<String>,
    state_dir: &Path,
    destination: &Path,
    inventory: Arc<Inventory>,
    authority: ImportAuthority,
) -> Result<(Job, PreparedImport), EngineError> {
    if !authority.valid() {
        return Err(EngineError::StaleImport);
    }
    if job.torrent.is_some() {
        return Err(EngineError::Lifecycle(
            "torrent imports do not use Usenet journals".into(),
        ));
    }
    let relative = PathBuf::from(job_dir_name(&job));
    if relative
        .components()
        .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(EngineError::Lifecycle(
            "unsafe imported journal root".into(),
        ));
    }
    let root = destination.join(relative);
    let records = JobJournals::replay_job(state_dir, job.id)?;
    let identity = match nzbd_state::fileops::open_directory(&root) {
        Ok(root) => Some(nzbd_state::fileops::identity(&root).map_err(lifecycle)?),
        Err(nzbd_state::artifacts::Error::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound && records.is_empty() =>
        {
            None
        }
        Err(error) => return Err(lifecycle(error)),
    };
    let source = inventory.for_job(job.id.0).map_err(lifecycle)?;
    let read_lease = if source
        .as_ref()
        .is_some_and(|source| source.path == root && identity.is_some())
    {
        inventory
            .acquire_terminal_use(job.id.0, &|| Ok(()))
            .map_err(lifecycle)?
    } else {
        None
    };
    let source = inventory
        .for_job(job.id.0)
        .map_err(lifecycle)?
        .as_ref()
        .map(SourceVersion::from);
    checkpoint(&root);
    let mut file_sizes = HashMap::new();
    let mut imported_files = HashSet::new();
    for record in records.into_iter().filter(|record| record.job == job.id) {
        let Some(file) = job.files.iter_mut().find(|file| file.id == record.file) else {
            continue;
        };
        if Path::new(&file.filename)
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            return Err(EngineError::Lifecycle(
                "unsafe imported journal file".into(),
            ));
        }
        let stable = root.join(format!(".runner-file-{}.part", file.id.0));
        let path = if stable.exists() {
            stable
        } else {
            root.join(&file.filename)
        };
        if !crate::writer::validate_range_checked(
            &path,
            record.offset,
            record.len as u64,
            record.crc32,
            &|| authority.valid(),
        ) {
            if !authority.valid() {
                return Err(EngineError::StaleImport);
            }
            return Err(EngineError::Lifecycle(
                "foreign journal bytes require revalidation; original queue and source retained"
                    .into(),
            ));
        }
        if record.file_size > 0 {
            file_sizes.insert(record.file, record.file_size);
        }
        if let Some(segment) = file
            .segments
            .iter_mut()
            .find(|segment| segment.number == record.segment_number)
        {
            segment.state = SegmentState::Done {
                offset: record.offset,
                len: record.len,
                crc: record.crc32,
            };
            imported_files.insert(file.id);
        }
    }
    if let Some(identity) = identity.as_ref() {
        let current = nzbd_state::fileops::open_directory(&root).map_err(lifecycle)?;
        if !identity.same_object(&nzbd_state::fileops::identity(&current).map_err(lifecycle)?) {
            return Err(EngineError::StaleImport);
        }
    } else if !matches!(std::fs::symlink_metadata(&root),Err(error) if error.kind()==std::io::ErrorKind::NotFound)
    {
        return Err(EngineError::StaleImport);
    }
    for (file, size) in &file_sizes {
        let key = format!("*File:size:{}", file.0);
        if let Some((_, value)) = job.params.iter_mut().find(|(name, _)| name == &key) {
            *value = size.to_string();
        } else {
            job.params.push((key, size.to_string()));
        }
    }
    recompute_job_totals(&mut job);
    Ok((
        job,
        PreparedImport {
            expected_intent,
            authority,
            source,
            root,
            identity,
            _read_lease: read_lease,
            file_sizes,
            imported_files,
        },
    ))
}
fn lifecycle(error: nzbd_state::artifacts::Error) -> EngineError {
    EngineError::Lifecycle(error.to_string())
}

impl PreparedImport {
    pub(crate) fn merge_current_progress(&self, incoming: &mut Job, current: Option<&Job>) {
        if !self
            .source
            .as_ref()
            .is_some_and(|source| source.path == self.root)
        {
            return;
        }
        let Some(current) = current else {
            return;
        };
        for file in &mut incoming.files {
            let Some(existing) = current
                .files
                .iter()
                .find(|existing| existing.id == file.id && existing.subject == file.subject)
            else {
                continue;
            };
            for segment in &mut file.segments {
                if let Some(existing) = existing.segments.iter().find(|existing| {
                    existing.number == segment.number && existing.message_id == segment.message_id
                }) {
                    if matches!(existing.state, SegmentState::Done { .. })
                        && !matches!(segment.state, SegmentState::Done { .. })
                    {
                        segment.state = existing.state;
                    }
                }
            }
        }
        recompute_job_totals(incoming);
    }

    pub(crate) fn valid(&self, job: JobId, current: Option<&Job>, inventory: &Inventory) -> bool {
        if !self.authority.valid() || self.expected_intent != intent(current) {
            return false;
        }
        let observed = match inventory.for_job(job.0) {
            Ok(source) => source,
            Err(_) => return false,
        };
        if observed.as_ref().map(SourceVersion::from) != self.source {
            return false;
        }
        match self.identity.as_ref() {
            Some(identity) => nzbd_state::fileops::open_directory(&self.root)
                .and_then(|root| nzbd_state::fileops::identity(&root))
                .is_ok_and(|observed| identity.same_object(&observed)),
            None => {
                matches!(std::fs::symlink_metadata(&self.root),Err(error) if error.kind()==std::io::ErrorKind::NotFound)
            }
        }
    }
}
