//! Per-file serial disk writers. Acknowledgements follow segment data sync;
//! every generation reserves its extents before any seek or mutation.
use crate::owner::EngineMsg;
use crate::recovery::{self, ProvenRange, ValidationOutcome};
use nzbd_state::artifacts::Identity;
use nzbd_types::{FileId, JobId, ServerId};
use std::{
    io::SeekFrom,
    path::{Path, PathBuf},
};
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;

#[derive(Debug)]
pub enum WriteCmd {
    PublicationName(String),
    SealPartial {
        file_size: u64,
        ranges: Vec<(u64, u32, u32)>,
    },
    #[cfg(test)]
    InjectFailure(&'static str, i32),
    Segment {
        seg_number: u32,
        offset: u64,
        data: Vec<u8>,
        crc: u32,
        file_size: u64,
        server: ServerId,
    },
    Finalize {
        file_size: u64,
        combined_crc: Option<u32>,
    },
}

#[derive(Clone, Debug)]
pub struct WriterHandle {
    pub tx: mpsc::Sender<WriteCmd>,
    pub stop: tokio_util::sync::CancellationToken,
    pub stopped: tokio::sync::watch::Receiver<bool>,
    pub generation: u64,
}

#[derive(Clone, Default)]
pub(crate) struct WriterScope {
    pub generation: u64,
    pub proven: Vec<ProvenRange>,
    pub expected_identity: Option<Identity>,
    pub collect_naming: bool,
    pub naming_inventory: Option<std::sync::Arc<nzbd_state::artifacts::Inventory>>,
    #[cfg(test)]
    pub naming_test_hook: Option<std::sync::Arc<dyn Fn(&'static str) + Send + Sync>>,
    #[cfg(test)]
    pub sync_test_hook: Option<std::sync::Arc<dyn Fn(&'static str) + Send + Sync>>,
}

#[derive(Debug)]
pub(crate) enum WriterFact {
    Written {
        seg_number: u32,
        offset: u64,
        len: u32,
        crc: u32,
        file_size: u64,
        server: ServerId,
        identity: Identity,
    },
    Finalized {
        ok: bool,
        final_path: Option<PathBuf>,
        combined_crc: Option<u32>,
        identity: Option<Identity>,
        naming: Option<Box<nzbd_state::artifacts::writer_naming::PublishedProducerRead>>,
    },
    Error {
        error: String,
        /// The hold the failing `io::Error` named by errno; `None` for a
        /// refusal with no errno (an identity conflict, a bad range).
        cause: Option<nzbd_types::HoldCause>,
    },
    Rejected {
        seg_number: u32,
        server: ServerId,
        reason: String,
    },
}

const WRITER_QUEUE: usize = 8;

/// Test support: an unscoped writer that reports plain `EngineMsg` facts.
/// Every production writer is scoped ([`spawn_writer_scoped`]).
#[cfg(test)]
pub fn spawn_writer(
    tracker: &TaskTracker,
    job: JobId,
    file: FileId,
    dir: PathBuf,
    final_name: String,
    engine_tx: mpsc::Sender<EngineMsg>,
) -> WriterHandle {
    spawn(
        tracker,
        job,
        file,
        dir,
        final_name,
        engine_tx,
        WriterScope::default(),
        false,
    )
}
pub(crate) fn spawn_writer_scoped(
    tracker: &TaskTracker,
    job: JobId,
    file: FileId,
    dir: PathBuf,
    final_name: String,
    engine_tx: mpsc::Sender<EngineMsg>,
    scope: WriterScope,
) -> WriterHandle {
    spawn(tracker, job, file, dir, final_name, engine_tx, scope, true)
}
// This boundary keeps the writer's actor identity, channels, and admission
// evidence explicit instead of hiding lifecycle state in a new container.
#[allow(clippy::too_many_arguments)]
fn spawn(
    tracker: &TaskTracker,
    job: JobId,
    file: FileId,
    dir: PathBuf,
    final_name: String,
    engine_tx: mpsc::Sender<EngineMsg>,
    scope: WriterScope,
    scoped: bool,
) -> WriterHandle {
    let generation = scope.generation;
    let (tx, rx) = mpsc::channel(WRITER_QUEUE);
    let stop = tokio_util::sync::CancellationToken::new();
    let cancellation = stop.clone();
    let (done, stopped) = tokio::sync::watch::channel(false);
    tracker.spawn(async move {
        writer_task(
            job,
            file,
            dir,
            final_name,
            rx,
            engine_tx,
            cancellation,
            scope,
            scoped,
        )
        .await;
        // Cancellation requests are not quiescence. Publish only after actual return.
        let _ = done.send(true);
    });
    WriterHandle {
        tx,
        stop,
        stopped,
        generation,
    }
}

struct Reporter {
    job: JobId,
    file: FileId,
    generation: u64,
    scoped: bool,
    tx: mpsc::Sender<EngineMsg>,
}
impl Reporter {
    async fn send(&self, fact: WriterFact) -> bool {
        let message = if self.scoped {
            EngineMsg::ScopedWriter {
                job: self.job,
                file: self.file,
                generation: self.generation,
                fact,
            }
        } else {
            match fact {
                WriterFact::Written {
                    seg_number,
                    offset,
                    len,
                    crc,
                    file_size,
                    server,
                    ..
                } => EngineMsg::SegmentWritten {
                    job: self.job,
                    file: self.file,
                    seg_number,
                    offset,
                    len,
                    crc,
                    file_size,
                    server,
                },
                WriterFact::Finalized {
                    ok,
                    final_path,
                    combined_crc,
                    naming,
                    ..
                } => {
                    if naming.is_some() {
                        let _ = tokio::task::spawn_blocking(move || drop(naming)).await;
                    }
                    EngineMsg::WriterFinalized {
                        job: self.job,
                        file: self.file,
                        ok,
                        final_path,
                        combined_crc,
                    }
                }
                WriterFact::Error { error, cause } => EngineMsg::WriterError {
                    job: self.job,
                    file: self.file,
                    error,
                    cause,
                },
                WriterFact::Rejected { reason, .. } => EngineMsg::WriterError {
                    job: self.job,
                    file: self.file,
                    error: reason,
                    cause: None,
                },
            }
        };
        match self.tx.send(message).await {
            Ok(()) => true,
            Err(error) => {
                if let EngineMsg::ScopedWriter {
                    fact:
                        WriterFact::Finalized {
                            naming: Some(naming),
                            ..
                        },
                    ..
                } = error.0
                {
                    let _ = tokio::task::spawn_blocking(move || drop(naming)).await;
                }
                false
            }
        }
    }
    /// A refusal with no errno behind it: the job's terminal `failed/io`.
    async fn error(&self, error: impl ToString) {
        self.send(WriterFact::Error {
            error: error.to_string(),
            cause: None,
        })
        .await;
    }
    /// A failed filesystem call: the errno along its error chain decides
    /// the hold ([`crate::error_hold_cause`]).
    async fn io_error(
        &self,
        context: &str,
        error: &(dyn std::error::Error + Send + Sync + 'static),
    ) {
        let fact = WriterFact::Error {
            error: format!("{context}: {error}"),
            cause: crate::error_hold_cause(error),
        };
        self.send(fact).await;
    }
}

async fn file_identity(file: &File) -> std::io::Result<Identity> {
    let descriptor = file.try_clone().await?.into_std().await;
    recovery::capture_identity(&descriptor).map_err(std::io::Error::other)
}
fn identity_error() -> std::io::Error {
    std::io::Error::other("writer identity conflict")
}

async fn ensure_open(
    dir: &Path,
    part: &Path,
    out: &mut Option<File>,
    expected: &Option<Identity>,
) -> std::io::Result<()> {
    if out.is_none() {
        if expected.is_none() {
            tokio::fs::create_dir_all(dir).await?;
        }
        let descriptor =
            nzbd_state::fileops::writer(part, expected.is_none()).map_err(std::io::Error::other)?;
        let identity = recovery::capture_identity(&descriptor).map_err(std::io::Error::other)?;
        if expected.as_ref().is_some_and(|saved| *saved != identity) {
            return Err(identity_error());
        }
        *out = Some(File::from_std(descriptor));
    }
    if let Some(expected) = expected {
        if file_identity(out.as_ref().unwrap()).await? != *expected {
            return Err(identity_error());
        }
    }
    Ok(())
}

// Pending facts carry no payload buffers. The existing eight-command input
// queue bounds batching; no timer or additional payload queue is introduced.
struct PendingWrite {
    seg_number: u32,
    offset: u64,
    len: u32,
    crc: u32,
    file_size: u64,
    server: ServerId,
}

#[derive(Default)]
struct PendingWrites {
    facts: Vec<PendingWrite>,
    dirty: bool,
    #[cfg(test)]
    sync_fault: Option<i32>,
}

impl PendingWrites {
    async fn flush(
        &mut self,
        out: &Option<File>,
        expected: &mut Option<Identity>,
        report: &Reporter,
        part_path: &Path,
        #[cfg(test)] hook: Option<&std::sync::Arc<dyn Fn(&'static str) + Send + Sync>>,
    ) -> bool {
        if self.facts.is_empty() {
            return true;
        }
        let file = out.as_ref().expect("pending writes have an open file");
        if self.dirty {
            #[cfg(test)]
            if let Some(hook) = hook {
                hook("before_sync");
            }
            let result = file.sync_data().await;
            #[cfg(test)]
            let result = if let Some(code) = self.sync_fault.take() {
                Err(std::io::Error::from_raw_os_error(code))
            } else {
                result
            };
            if let Err(error) = result {
                report
                    .io_error(&format!("write {}", part_path.display()), &error)
                    .await;
                return false;
            }
            #[cfg(test)]
            if let Some(hook) = hook {
                hook("after_sync");
            }
        }
        let identity = match file_identity(file).await {
            Ok(identity) => identity,
            Err(error) => {
                report
                    .io_error(&format!("identity {}", part_path.display()), &error)
                    .await;
                return false;
            }
        };
        if expected.as_ref().is_some_and(|prior| *prior != identity) {
            report.error(identity_error()).await;
            return false;
        }
        *expected = Some(identity.clone());
        // Every fact names the same post-sync identity. No member of a new
        // batch is acknowledged before the shared durability boundary succeeds.
        for fact in self.facts.drain(..) {
            if !report
                .send(WriterFact::Written {
                    seg_number: fact.seg_number,
                    offset: fact.offset,
                    len: fact.len,
                    crc: fact.crc,
                    file_size: fact.file_size,
                    server: fact.server,
                    identity: identity.clone(),
                })
                .await
            {
                return false;
            }
        }
        self.dirty = false;
        true
    }
}

// The task owns one writer actor; keep its bounded plumbing explicit here.
#[allow(clippy::too_many_arguments)]
async fn writer_task(
    job: JobId,
    file_id: FileId,
    dir: PathBuf,
    final_name: String,
    mut rx: mpsc::Receiver<WriteCmd>,
    engine_tx: mpsc::Sender<EngineMsg>,
    stop: tokio_util::sync::CancellationToken,
    scope: WriterScope,
    scoped: bool,
) {
    let report = Reporter {
        job,
        file: file_id,
        generation: scope.generation,
        scoped,
        tx: engine_tx,
    };
    let part_path = dir.join(format!(".runner-file-{}.part", file_id.0));
    let mut final_path = dir.join(&final_name);
    let resolved = match recovery::resolve_payload(&dir, file_id, &final_name) {
        Ok(payload) => payload,
        Err(error) => {
            match error {
                recovery::ResolveError::Unavailable { message, cause } => {
                    report
                        .send(WriterFact::Error {
                            error: format!("write admission / payload resolution: {message}"),
                            cause,
                        })
                        .await;
                }
                recovery::ResolveError::IdentityConflict => {
                    report
                        .error("write admission / payload resolution: identity conflict")
                        .await;
                }
            }
            return;
        }
    };
    if let Some(payload) = &resolved {
        if scope
            .expected_identity
            .as_ref()
            .is_some_and(|saved| *saved != payload.identity)
        {
            report.error("writer identity conflict at admission").await;
            return;
        }
        if !payload.published && payload.path != part_path {
            if let Err(error) = nzbd_state::fileops::link(&payload.path, &part_path) {
                report.io_error("legacy partial migration", &error).await;
                return;
            }
        }
    }
    if !scope.proven.is_empty()
        && (scope.expected_identity.is_none()
            || !recovery::coherent_ranges(
                &scope.proven,
                scope.expected_identity.as_ref().unwrap().bytes,
            ))
    {
        report.error("writer proven range identity conflict").await;
        return;
    }
    // Linking a legacy alias updates ctime. Refresh only the object just established.
    let mut expected = scope.expected_identity;
    if expected.is_some() {
        if let Some(payload) = &resolved {
            let opened = match nzbd_state::fileops::open(&payload.path) {
                Ok(opened) => opened,
                Err(error) => {
                    report.io_error("open partial", &error).await;
                    return;
                }
            };
            expected = match nzbd_state::fileops::identity(&opened) {
                Ok(identity) => Some(identity),
                Err(error) => {
                    report.io_error("partial identity", &error).await;
                    return;
                }
            };
        }
    }
    let published_only = resolved.as_ref().is_some_and(|payload| payload.published);
    let mut ranges = scope.proven;
    let mut out = None;
    let mut reservation = None;
    #[cfg(test)]
    let mut fault = None;
    let mut pending = PendingWrites::default();
    let mut deferred = None;
    loop {
        // Drain only an already-queued contiguous segment prefix. A control
        // command, an empty queue, the batch bound, or cancellation flushes the
        // accepted prefix before another command can run.
        let cmd = if !pending.facts.is_empty() {
            let next = if !stop.is_cancelled() && pending.facts.len() < WRITER_QUEUE {
                rx.try_recv().ok()
            } else {
                None
            };
            match next {
                Some(command @ WriteCmd::Segment { .. }) => command,
                other => {
                    deferred = other;
                    if !pending
                        .flush(
                            &out,
                            &mut expected,
                            &report,
                            &part_path,
                            #[cfg(test)]
                            scope.sync_test_hook.as_ref(),
                        )
                        .await
                    {
                        return;
                    }
                    if stop.is_cancelled() {
                        break;
                    }
                    continue;
                }
            }
        } else if let Some(command) = deferred.take() {
            if stop.is_cancelled() {
                break;
            }
            command
        } else {
            // Never cancel an in-flight seek/write/sync. Only the next receive
            // is cancellable, after any completed writes have been drained.
            tokio::select! { biased; _ = stop.cancelled() => break,
            command = rx.recv() => match command { Some(command)=>command,None=>break } }
        };
        match cmd {
            #[cfg(test)]
            WriteCmd::InjectFailure(stage, code) => {
                fault = Some((stage, code));
            }
            WriteCmd::PublicationName(name) => {
                if !recovery::safe_name(&name) {
                    report.error("invalid publication name").await;
                    return;
                }
                final_path = dir.join(name);
            }
            WriteCmd::Segment {
                seg_number,
                offset,
                data,
                crc,
                file_size,
                server,
            } => {
                let range = ProvenRange {
                    seg_number,
                    offset,
                    len: u32::try_from(data.len()).unwrap_or_default(),
                    crc,
                };
                let invalid = published_only
                    || range.len == 0
                    || range.end().is_none()
                    || (file_size > 0 && range.end().is_some_and(|end| end > file_size));
                let duplicate = ranges.contains(&range);
                let conflict = ranges.iter().any(|prior| {
                    (prior.seg_number == seg_number || prior.overlaps(&range)) && *prior != range
                });
                if invalid || conflict {
                    if !pending
                        .flush(
                            &out,
                            &mut expected,
                            &report,
                            &part_path,
                            #[cfg(test)]
                            scope.sync_test_hook.as_ref(),
                        )
                        .await
                    {
                        return;
                    }
                    if !report
                        .send(WriterFact::Rejected {
                            seg_number,
                            server,
                            reason: "segment extent/identity conflicts with reserved ranges".into(),
                        })
                        .await
                    {
                        return;
                    }
                    continue;
                }
                if let Err(error) = ensure_open(&dir, &part_path, &mut out, &expected).await {
                    report
                        .io_error(&format!("write {}", part_path.display()), &error)
                        .await;
                    return;
                }
                if !duplicate {
                    if reservation.is_none() && file_size > 0 {
                        let done = ranges
                            .iter()
                            .map(|range| u64::from(range.len))
                            .fold(0u64, u64::saturating_add)
                            .min(file_size);
                        let end = ranges
                            .iter()
                            .filter_map(ProvenRange::end)
                            .max()
                            .unwrap_or(0);
                        let credit =
                            crate::download_capacity::persisted_credit(&part_path, done, end);
                        match nzbd_state::capacity::reserve(&dir, file_size.saturating_sub(credit))
                        {
                            Ok(claim) => reservation = Some(claim),
                            Err(error) => {
                                report
                                    .io_error(&format!("write admission {}", dir.display()), &error)
                                    .await;
                                return;
                            }
                        }
                    }
                    // Reserve before any mutation, including preallocation. A failed
                    // or uncertain write terminates this generation rather than releasing it.
                    ranges.push(range.clone());
                    #[cfg(test)]
                    let injected = fault.take();
                    #[cfg(test)]
                    if let Some(("write", code)) = injected {
                        report
                            .io_error("write", &std::io::Error::from_raw_os_error(code))
                            .await;
                        return;
                    }
                    let f = out.as_mut().unwrap();
                    let result = async {
                        if file_size > 0 && f.metadata().await?.len() < file_size {
                            f.set_len(file_size).await?;
                        }
                        f.seek(SeekFrom::Start(offset)).await?;
                        f.write_all(&data).await?;
                        // Drain Tokio's buffered write before checking identity or
                        // seeking for another member. Durability is shared below.
                        f.flush().await?;
                        Ok::<_, std::io::Error>(())
                    }
                    .await;
                    #[cfg(test)]
                    if let Some(("sync", code)) = injected {
                        pending.sync_fault = Some(code);
                    }
                    if let Err(error) = result {
                        report
                            .io_error(&format!("write {}", part_path.display()), &error)
                            .await;
                        return;
                    }
                    pending.dirty = true;
                }
                let identity = match file_identity(out.as_ref().unwrap()).await {
                    Ok(identity) => identity,
                    Err(error) => {
                        report.io_error("write identity", &error).await;
                        return;
                    }
                };
                expected = Some(identity);
                pending.facts.push(PendingWrite {
                    seg_number,
                    offset,
                    len: range.len,
                    crc,
                    file_size,
                    server,
                });
            }
            WriteCmd::SealPartial {
                file_size,
                ranges: checks,
            } => {
                let result = async {
                    if file_size == 0
                        || ranges
                            .iter()
                            .any(|range| range.end().is_none_or(|end| end > file_size))
                    {
                        return Err(identity_error());
                    }
                    ensure_open(&dir, &part_path, &mut out, &expected).await?;
                    let f = out.as_ref().unwrap();
                    f.sync_data().await?;
                    let identity = file_identity(f).await?;
                    if identity.bytes != file_size {
                        return Err(identity_error());
                    }
                    for (offset, len, crc) in checks {
                        require_valid(&part_path, offset, u64::from(len), crc, Some(&identity))?;
                    }
                    Ok::<_, std::io::Error>(identity)
                }
                .await;
                match result {
                    Ok(identity) => {
                        report
                            .send(WriterFact::Finalized {
                                ok: true,
                                final_path: Some(part_path.clone()),
                                combined_crc: None,
                                identity: Some(identity),
                                naming: None,
                            })
                            .await;
                    }
                    Err(error) => report.io_error("seal partial", &error).await,
                }
                return;
            }
            WriteCmd::Finalize {
                file_size,
                combined_crc,
            } => {
                #[cfg(test)]
                if let Some(("finalize", code)) = fault.take() {
                    report
                        .io_error("finalize", &std::io::Error::from_raw_os_error(code))
                        .await;
                    return;
                }
                if ranges.iter().any(|range| {
                    range
                        .end()
                        .is_none_or(|end| file_size > 0 && end > file_size)
                }) {
                    report
                        .error("finalize identity conflict: would trim a reserved range")
                        .await;
                    return;
                }
                let result = finalize(
                    &dir,
                    &part_path,
                    &final_path,
                    &mut out,
                    &expected,
                    FinalizeOptions {
                        file_size,
                        combined_crc,
                        collect_naming: scope.collect_naming,
                        naming_inventory: scope.naming_inventory.clone(),
                        cancellation: stop.clone(),
                        #[cfg(test)]
                        naming_test_hook: scope.naming_test_hook.clone(),
                    },
                )
                .await;
                if stop.is_cancelled() {
                    if let Ok((_, Some(naming))) = result {
                        if let Some(inventory) = scope.naming_inventory.as_ref() {
                            inventory.discard_writer_naming(naming);
                        } else {
                            let _ = tokio::task::spawn_blocking(move || drop(naming)).await;
                        }
                    }
                    return;
                }
                match result {
                    Ok((identity, naming)) => {
                        report
                            .send(WriterFact::Finalized {
                                ok: true,
                                final_path: Some(final_path.clone()),
                                combined_crc,
                                identity: Some(identity),
                                naming: naming.map(Box::new),
                            })
                            .await;
                    }
                    Err(error) => {
                        report
                            .io_error("finalize identity/publication", &error)
                            .await
                    }
                }
                return;
            }
        }
    }
}

fn require_valid(
    path: &Path,
    offset: u64,
    len: u64,
    crc: u32,
    expected: Option<&Identity>,
) -> std::io::Result<Identity> {
    match recovery::validate_range(path, offset, len, crc, expected) {
        ValidationOutcome::Valid(identity) => Ok(identity),
        other => Err(std::io::Error::other(format!(
            "checkpoint validation: {other:?}"
        ))),
    }
}

struct FinalizeOptions {
    file_size: u64,
    combined_crc: Option<u32>,
    collect_naming: bool,
    naming_inventory: Option<std::sync::Arc<nzbd_state::artifacts::Inventory>>,
    cancellation: tokio_util::sync::CancellationToken,
    #[cfg(test)]
    naming_test_hook: Option<std::sync::Arc<dyn Fn(&'static str) + Send + Sync>>,
}

/// The writer owns and joins this worker before publishing its stopped fact.
/// Naming digests share the mandatory CRC pass; publication still follows CRC.
async fn verify_complete(
    path: &Path,
    identity: &Identity,
    size: u64,
    crc: u32,
    options: &FinalizeOptions,
) -> std::io::Result<Option<nzbd_state::artifacts::writer_naming::ProducerRead>> {
    let path = path.to_owned();
    let expected = identity.clone();
    let cancel = options.cancellation.clone();
    let collect = options.collect_naming;
    let inventory = options.naming_inventory.clone();
    #[cfg(test)]
    let hook = options.naming_test_hook.clone();
    tokio::task::spawn_blocking(move || {
        if let Some(inventory) = inventory {
            inventory.prepare_writer_naming();
        }
        let file = nzbd_state::fileops::open(&path).map_err(std::io::Error::other)?;
        if recovery::capture_identity(&file).map_err(std::io::Error::other)? != expected {
            return Err(identity_error());
        }
        if size == 0 {
            require_valid(&path, 0, 0, crc, Some(&expected))?;
            return Ok(None);
        }
        let result = nzbd_state::artifacts::writer_naming::verify_producer_read(
            file,
            size,
            crc,
            collect,
            &|| {
                #[cfg(test)]
                if let Some(hook) = &hook {
                    hook("scan");
                }
                if cancel.is_cancelled() {
                    Err(std::io::ErrorKind::Interrupted.into())
                } else {
                    Ok(())
                }
            },
        );
        #[cfg(test)]
        if result.as_ref().is_ok_and(|naming| naming.is_some()) {
            if let Some(hook) = &hook {
                hook("after_crc");
            }
        }
        result
    })
    .await
    .map_err(std::io::Error::other)?
}

async fn publish_naming(
    naming: Option<nzbd_state::artifacts::writer_naming::ProducerRead>,
    path: &Path,
    identity: &Identity,
) -> Option<nzbd_state::artifacts::writer_naming::PublishedProducerRead> {
    let naming = naming?;
    let path = path.to_owned();
    let identity = identity.clone();
    // Optional locator checks and rejected descriptor closure stay off Tokio.
    tokio::task::spawn_blocking(move || naming.published(&path, &identity))
        .await
        .ok()
        .flatten()
}

async fn finalize(
    dir: &Path,
    part: &Path,
    final_path: &Path,
    out: &mut Option<File>,
    expected: &Option<Identity>,
    options: FinalizeOptions,
) -> std::io::Result<(
    Identity,
    Option<nzbd_state::artifacts::writer_naming::PublishedProducerRead>,
)> {
    let file_size = options.file_size;
    let combined_crc = options.combined_crc;
    if out.is_none() {
        match nzbd_state::fileops::writer(part, false) {
            Ok(file) => *out = Some(File::from_std(file)),
            Err(nzbd_state::artifacts::Error::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                let opened =
                    nzbd_state::fileops::open(final_path).map_err(std::io::Error::other)?;
                let identity =
                    recovery::capture_identity(&opened).map_err(std::io::Error::other)?;
                if file_size == 0
                    || identity.bytes != file_size
                    || expected.as_ref().is_some_and(|saved| *saved != identity)
                {
                    return Err(identity_error());
                }
                if let Some(crc) = combined_crc {
                    let naming =
                        verify_complete(final_path, &identity, file_size, crc, &options).await?;
                    let naming = publish_naming(naming, final_path, &identity).await;
                    return Ok((identity, naming));
                }
                if expected.is_some() {
                    return Ok((identity, None));
                }
                return Err(identity_error());
            }
            Err(error) => return Err(std::io::Error::other(error)),
        }
    }
    ensure_open(dir, part, out, expected).await?;
    let f = out.as_ref().unwrap();
    if file_size > 0 {
        f.set_len(file_size).await?;
    }
    // Segment acknowledgements already synced. Sync again for completion metadata.
    f.sync_data().await?;
    let identity = file_identity(f).await?;
    if file_size > 0 && identity.bytes != file_size {
        return Err(identity_error());
    }
    let naming = if let Some(crc) = combined_crc {
        verify_complete(part, &identity, file_size, crc, &options).await?
    } else {
        None
    };
    let publication = async {
        if options.cancellation.is_cancelled() {
            return Err(std::io::ErrorKind::Interrupted.into());
        }
        // Keep the writer descriptor alive while checking path and publishing.
        let source = nzbd_state::fileops::open(part).map_err(std::io::Error::other)?;
        if recovery::capture_identity(&source).map_err(std::io::Error::other)? != identity {
            return Err(identity_error());
        }
        match nzbd_state::fileops::link(part, final_path) {
            Ok(()) => {}
            Err(nzbd_state::artifacts::Error::Io(error))
                if error.kind() == std::io::ErrorKind::AlreadyExists =>
            {
                let target =
                    nzbd_state::fileops::open(final_path).map_err(std::io::Error::other)?;
                let target_identity =
                    recovery::capture_identity(&target).map_err(std::io::Error::other)?;
                if !target_identity.same_object(&identity)
                    || target_identity.bytes != identity.bytes
                    || target_identity.modified != identity.modified
                {
                    return Err(identity_error());
                }
            }
            Err(error) => return Err(std::io::Error::other(error)),
        }
        if let Some(parent) = final_path.parent() {
            File::open(parent).await?.sync_all().await?;
        }
        tokio::fs::remove_file(part).await?;
        if let Some(parent) = part.parent() {
            File::open(parent).await?.sync_all().await?;
        }
        let identity = file_identity(out.as_ref().unwrap()).await?;
        Ok::<_, std::io::Error>(identity)
    }
    .await;
    let identity = match publication {
        Ok(identity) => identity,
        Err(error) => {
            // Even an optional read handle can block in close on the source
            // filesystem. Join its disposal before reporting writer quiescence.
            tokio::task::spawn_blocking(move || {
                #[cfg(test)]
                if let Some(hook) = &options.naming_test_hook {
                    hook("dispose");
                }
                drop(naming);
            })
            .await
            .map_err(std::io::Error::other)?;
            return Err(error);
        }
    };
    let naming = publish_naming(naming, final_path, &identity).await;
    drop(out.take());
    Ok((identity, naming))
}

#[cfg(test)]
pub(crate) fn validate_range(path: &std::path::Path, offset: u64, len: u64, crc: u32) -> bool {
    validate_range_checked(path, offset, len, crc, &|| true)
}
pub(crate) fn validate_range_checked(
    path: &std::path::Path,
    offset: u64,
    len: u64,
    crc: u32,
    checkpoint: &dyn Fn() -> bool,
) -> bool {
    use std::io::{Read, Seek};
    if !checkpoint() {
        return false;
    }
    let Ok(meta) = std::fs::symlink_metadata(path) else {
        return false;
    };
    if !meta.is_file()
        || meta.file_type().is_symlink()
        || offset.checked_add(len).is_none_or(|end| end > meta.len())
    {
        return false;
    }
    let Ok(mut file) = nzbd_state::fileops::open(path) else {
        return false;
    };
    if file.seek(SeekFrom::Start(offset)).is_err() {
        return false;
    }
    let mut digest = crc32fast::Hasher::new();
    let mut remaining = len;
    let mut data = [0; 65536];
    while remaining > 0 {
        if !checkpoint() {
            return false;
        }
        let amount = remaining.min(data.len() as u64) as usize;
        if file.read_exact(&mut data[..amount]).is_err() {
            return false;
        }
        digest.update(&data[..amount]);
        remaining -= amount as u64;
    }
    digest.finalize() == crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::channel;

    async fn naming_writer_event(events: &mut mpsc::Receiver<EngineMsg>) -> Option<EngineMsg> {
        tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
            .await
            .expect("writer must report within the fixture deadline")
    }

    #[tokio::test]
    async fn cancellation_and_publication_failure_join_naming_workers_before_stopped() {
        for boundary in ["scan", "after_crc", "collision"] {
            let tmp = tempfile::tempdir().unwrap();
            let tracker = TaskTracker::new();
            let (tx, mut events) = channel(8);
            let (entered_tx, mut entered) = tokio::sync::mpsc::unbounded_channel();
            let (release, blocked) = std::sync::mpsc::channel();
            let blocked = std::sync::Mutex::new(blocked);
            let hook = std::sync::Arc::new(move |stage| {
                let wait = match boundary {
                    "scan" => stage == "scan",
                    "after_crc" => matches!(stage, "after_crc" | "dispose"),
                    _ => stage == "dispose",
                };
                if wait {
                    entered_tx.send(stage).unwrap();
                    blocked.lock().unwrap().recv().unwrap();
                }
            });
            let mut writer = spawn_writer_scoped(
                &tracker,
                JobId(1),
                FileId(2),
                tmp.path().into(),
                "done".into(),
                tx,
                WriterScope {
                    collect_naming: true,
                    naming_test_hook: Some(hook),
                    ..Default::default()
                },
            );
            let bytes = b"mandatory CRC and optional naming";
            writer
                .tx
                .send(WriteCmd::Segment {
                    seg_number: 1,
                    offset: 0,
                    data: bytes.to_vec(),
                    crc: crc32fast::hash(bytes),
                    file_size: bytes.len() as u64,
                    server: ServerId(1),
                })
                .await
                .unwrap();
            assert!(matches!(
                naming_writer_event(&mut events).await,
                Some(EngineMsg::ScopedWriter {
                    fact: WriterFact::Written { .. },
                    ..
                })
            ));
            if boundary == "collision" {
                std::fs::write(tmp.path().join("done"), b"unrelated existing destination").unwrap();
            }
            writer
                .tx
                .send(WriteCmd::Finalize {
                    file_size: bytes.len() as u64,
                    combined_crc: Some(crc32fast::hash(bytes)),
                })
                .await
                .unwrap();
            let stage = tokio::time::timeout(std::time::Duration::from_secs(5), entered.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                stage,
                if boundary == "collision" {
                    "dispose"
                } else {
                    boundary
                }
            );
            if boundary != "collision" {
                writer.stop.cancel();
            }
            // This current-thread runtime can still run while the worker is
            // blocked. Cancellation alone cannot announce writer quiescence.
            tokio::task::yield_now().await;
            assert!(!*writer.stopped.borrow());
            release.send(()).unwrap();
            if boundary == "after_crc" {
                assert_eq!(
                    tokio::time::timeout(std::time::Duration::from_secs(5), entered.recv())
                        .await
                        .unwrap(),
                    Some("dispose")
                );
                assert!(!*writer.stopped.borrow());
                release.send(()).unwrap();
            }
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                writer.stopped.wait_for(|done| *done),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                std::fs::read(tmp.path().join(".runner-file-2.part")).unwrap(),
                bytes
            );
            if boundary == "collision" {
                assert_eq!(
                    std::fs::read(tmp.path().join("done")).unwrap(),
                    b"unrelated existing destination"
                );
            } else {
                assert!(!tmp.path().join("done").exists());
            }
        }
    }

    #[tokio::test]
    async fn writer_without_combined_crc_publishes_without_naming_evidence() {
        let tmp = tempfile::tempdir().unwrap();
        let tracker = TaskTracker::new();
        let (tx, mut events) = channel(8);
        let writer = spawn_writer_scoped(
            &tracker,
            JobId(1),
            FileId(2),
            tmp.path().into(),
            "done".into(),
            tx,
            WriterScope {
                collect_naming: true,
                ..Default::default()
            },
        );
        writer
            .tx
            .send(WriteCmd::Segment {
                seg_number: 1,
                offset: 0,
                data: b"data".to_vec(),
                crc: crc32fast::hash(b"data"),
                file_size: 4,
                server: ServerId(1),
            })
            .await
            .unwrap();
        naming_writer_event(&mut events).await.unwrap();
        writer
            .tx
            .send(WriteCmd::Finalize {
                file_size: 4,
                combined_crc: None,
            })
            .await
            .unwrap();
        assert!(matches!(
            naming_writer_event(&mut events).await,
            Some(EngineMsg::ScopedWriter {
                fact: WriterFact::Finalized {
                    ok: true,
                    naming: None,
                    combined_crc: None,
                    ..
                },
                ..
            })
        ));
        assert_eq!(std::fs::read(tmp.path().join("done")).unwrap(), b"data");
    }

    /// Writes segments out of order with a gap, finalizes, and checks the
    /// assembled bytes (gap zero-filled, exact final size, `.part` renamed).
    #[tokio::test]
    async fn assembles_out_of_order_with_gap() {
        let tmp = tempfile::tempdir().unwrap();
        let tracker = TaskTracker::new();
        let (etx, mut erx) = channel(64);
        let h = spawn_writer(
            &tracker,
            JobId(1),
            FileId(1),
            tmp.path().to_path_buf(),
            "out.bin".into(),
            etx,
        );

        let file_size = 10u64;
        // segment 2 first: bytes 5..8, then segment 1: bytes 0..3. Gap at 3..5 and 8..10.
        h.tx.send(WriteCmd::Segment {
            seg_number: 2,
            offset: 5,
            data: vec![0xBB; 3],
            crc: 0,
            file_size,
            server: ServerId(1),
        })
        .await
        .unwrap();
        h.tx.send(WriteCmd::Segment {
            seg_number: 1,
            offset: 0,
            data: vec![0xAA; 3],
            crc: 0,
            file_size,
            server: ServerId(1),
        })
        .await
        .unwrap();
        h.tx.send(WriteCmd::Finalize {
            file_size,
            combined_crc: None,
        })
        .await
        .unwrap();

        let mut written = 0;
        let mut finalized = false;
        while let Some(msg) = erx.recv().await {
            match msg {
                EngineMsg::SegmentWritten { .. } => written += 1,
                EngineMsg::WriterFinalized { ok, .. } => {
                    assert!(ok);
                    finalized = true;
                    break;
                }
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(written, 2);
        assert!(finalized);

        let bytes = std::fs::read(tmp.path().join("out.bin")).unwrap();
        let mut expected = vec![0u8; 10];
        expected[0..3].fill(0xAA);
        expected[5..8].fill(0xBB);
        assert_eq!(bytes, expected);
        assert!(!tmp.path().join("out.bin.part").exists());
    }

    /// A finalize that cannot complete must report failure, not success.
    ///
    /// Regression test for the worst bug this engine has had: two 40-60 GB
    /// remuxes reported SUCCESS with 500 MiB on disk. Segment writes already
    /// sync before acknowledgement; the completion sync must also propagate
    /// any later metadata or deferred writeback failure.
    ///
    /// The failure is simulated the only way it can be from a unit test:
    /// the `.part` file is removed while the destination directory is gone
    /// too, so reopening it cannot succeed and the rename has nothing to
    /// move. What matters is the plumbing — an error here becomes
    /// `ok: false`, and the owner turns `ok: false` into a failed job rather
    /// than a healthy one.
    #[tokio::test]
    async fn a_finalize_that_fails_reports_not_ok() {
        let tmp = tempfile::tempdir().unwrap();
        let tracker = TaskTracker::new();
        let (etx, mut erx) = channel(64);
        let dir = tmp.path().join("gone");
        std::fs::create_dir_all(&dir).unwrap();
        let h = spawn_writer(
            &tracker,
            JobId(1),
            FileId(1),
            dir.clone(),
            "x.bin".into(),
            etx,
        );

        h.tx.send(WriteCmd::Segment {
            seg_number: 1,
            offset: 0,
            data: vec![0xAA; 16],
            crc: 0,
            file_size: 16,
            server: ServerId(1),
        })
        .await
        .unwrap();
        match erx.recv().await {
            Some(EngineMsg::SegmentWritten { .. }) => {}
            other => panic!("unexpected {other:?}"),
        }

        // The destination disappears out from under the writer, which is what
        // a filesystem going away looks like from in here.
        std::fs::remove_dir_all(&dir).unwrap();

        h.tx.send(WriteCmd::Finalize {
            file_size: 16,
            combined_crc: None,
        })
        .await
        .unwrap();
        match erx.recv().await {
            Some(EngineMsg::WriterFinalized { ok, .. }) => assert!(
                !ok,
                "a finalize that could not place the file must report ok:false — \
                 reporting success here is what called 500 MiB of a 48 GiB remux \
                 a completed download"
            ),
            Some(EngineMsg::WriterError { .. }) => {}
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn resume_reopen_preserves_existing_data() {
        let tmp = tempfile::tempdir().unwrap();
        let tracker = TaskTracker::new();

        // First writer: one segment.
        let (etx, mut erx) = channel(64);
        let h = spawn_writer(
            &tracker,
            JobId(1),
            FileId(1),
            tmp.path().to_path_buf(),
            "r.bin".into(),
            etx,
        );
        h.tx.send(WriteCmd::Segment {
            seg_number: 1,
            offset: 0,
            data: vec![1, 2, 3, 4],
            crc: 0,
            file_size: 8,
            server: ServerId(1),
        })
        .await
        .unwrap();
        erx.recv().await.unwrap();
        drop(h); // "crash": writer exits on channel close, no finalize

        // Second writer (recovery): remaining segment + finalize.
        let (etx2, mut erx2) = channel(64);
        let h2 = spawn_writer(
            &tracker,
            JobId(1),
            FileId(1),
            tmp.path().to_path_buf(),
            "r.bin".into(),
            etx2,
        );
        h2.tx
            .send(WriteCmd::Segment {
                seg_number: 2,
                offset: 4,
                data: vec![5, 6, 7, 8],
                crc: 0,
                file_size: 8,
                server: ServerId(1),
            })
            .await
            .unwrap();
        h2.tx
            .send(WriteCmd::Finalize {
                file_size: 8,
                combined_crc: None,
            })
            .await
            .unwrap();
        loop {
            match erx2.recv().await.unwrap() {
                EngineMsg::WriterFinalized { ok, .. } => {
                    assert!(ok);
                    break;
                }
                EngineMsg::SegmentWritten { .. } => {}
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(
            std::fs::read(tmp.path().join("r.bin")).unwrap(),
            vec![1, 2, 3, 4, 5, 6, 7, 8]
        );
    }

    #[tokio::test]
    async fn recovery_finalize_is_idempotent_when_final_file_already_exists() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("done.bin"), b"durable bytes").unwrap();
        let tracker = TaskTracker::new();
        let (engine_tx, mut engine_rx) = channel(4);
        let writer = spawn_writer(
            &tracker,
            JobId(7),
            FileId(8),
            tmp.path().to_path_buf(),
            "done.bin".into(),
            engine_tx,
        );

        writer
            .tx
            .send(WriteCmd::Finalize {
                file_size: 13,
                combined_crc: Some(crc32fast::hash(b"durable bytes")),
            })
            .await
            .unwrap();
        match engine_rx.recv().await {
            Some(EngineMsg::WriterFinalized {
                ok,
                final_path,
                combined_crc,
                ..
            }) => {
                assert!(ok);
                assert_eq!(
                    final_path.as_deref(),
                    Some(tmp.path().join("done.bin").as_path())
                );
                assert_eq!(combined_crc, Some(crc32fast::hash(b"durable bytes")));
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(
            std::fs::read(tmp.path().join("done.bin")).unwrap(),
            b"durable bytes"
        );
    }

    #[tokio::test]
    async fn segment_write_errors_are_reported_and_closed_engine_stops_writer() {
        let tmp = tempfile::tempdir().unwrap();
        let blocked_dir = tmp.path().join("not-a-directory");
        std::fs::write(&blocked_dir, b"file").unwrap();
        let tracker = TaskTracker::new();
        let (engine_tx, mut engine_rx) = channel(4);
        let writer = spawn_writer(
            &tracker,
            JobId(1),
            FileId(2),
            blocked_dir,
            "x.bin".into(),
            engine_tx,
        );
        writer
            .tx
            .send(WriteCmd::Segment {
                seg_number: 3,
                offset: 0,
                data: vec![1, 2, 3],
                crc: 4,
                file_size: 3,
                server: ServerId(5),
            })
            .await
            .unwrap();
        match engine_rx.recv().await {
            Some(EngineMsg::WriterError {
                job, file, error, ..
            }) => {
                assert_eq!(job, JobId(1));
                assert_eq!(file, FileId(2));
                assert!(error.contains("write admission"));
            }
            other => panic!("unexpected {other:?}"),
        }

        let tracker = TaskTracker::new();
        let (engine_tx, engine_rx) = channel(1);
        drop(engine_rx);
        let writer = spawn_writer(
            &tracker,
            JobId(9),
            FileId(10),
            tmp.path().join("writable"),
            "orphan.bin".into(),
            engine_tx,
        );
        writer
            .tx
            .send(WriteCmd::Segment {
                seg_number: 1,
                offset: 0,
                data: vec![9],
                crc: 0,
                file_size: 1,
                server: ServerId(1),
            })
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), writer.tx.closed())
            .await
            .expect("writer must stop when the engine receiver closes");
    }

    #[tokio::test]
    async fn recovery_finalize_rejects_a_non_file_part_path() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir(tmp.path().join(".runner-file-1.part")).unwrap();
        let tracker = TaskTracker::new();
        let (engine_tx, mut engine_rx) = channel(4);
        let writer = spawn_writer(
            &tracker,
            JobId(1),
            FileId(1),
            tmp.path().to_path_buf(),
            "bad.bin".into(),
            engine_tx,
        );
        writer
            .tx
            .send(WriteCmd::Finalize {
                file_size: 10,
                combined_crc: None,
            })
            .await
            .unwrap();
        assert!(matches!(
            engine_rx.recv().await,
            Some(EngineMsg::WriterFinalized { ok: false, .. } | EngineMsg::WriterError { .. })
        ));
    }
    #[tokio::test]
    async fn write_sync_and_publication_exhaustion_preserve_same_file_for_resume() {
        for stage in ["write", "sync", "finalize"] {
            for code in [libc::ENOSPC, libc::EDQUOT] {
                let tmp = tempfile::tempdir().unwrap();
                let tracker = TaskTracker::new();
                let (tx, mut events) = channel(8);
                let writer = spawn_writer(
                    &tracker,
                    JobId(1),
                    FileId(9),
                    tmp.path().into(),
                    "same.bin".into(),
                    tx.clone(),
                );
                if stage != "finalize" {
                    writer
                        .tx
                        .send(WriteCmd::InjectFailure(stage, code))
                        .await
                        .unwrap();
                }
                let segment = || WriteCmd::Segment {
                    seg_number: 1,
                    offset: 0,
                    data: b"verified".to_vec(),
                    crc: crc32fast::hash(b"verified"),
                    file_size: 8,
                    server: ServerId(1),
                };
                writer.tx.send(segment()).await.unwrap();
                if stage == "finalize" {
                    assert!(matches!(
                        events.recv().await,
                        Some(EngineMsg::SegmentWritten { .. })
                    ));
                    writer
                        .tx
                        .send(WriteCmd::InjectFailure(stage, code))
                        .await
                        .unwrap();
                    writer
                        .tx
                        .send(WriteCmd::Finalize {
                            file_size: 8,
                            combined_crc: Some(crc32fast::hash(b"verified")),
                        })
                        .await
                        .unwrap();
                }
                assert!(
                    matches!(events.recv().await, Some(EngineMsg::WriterError { .. })),
                    "{stage}"
                );
                assert!(!tmp.path().join("same.bin").exists());
                if stage != "write" {
                    assert_eq!(
                        std::fs::read(tmp.path().join(".runner-file-9.part")).unwrap(),
                        b"verified"
                    );
                }
                let resumed = spawn_writer(
                    &tracker,
                    JobId(1),
                    FileId(9),
                    tmp.path().into(),
                    "same.bin".into(),
                    tx,
                );
                resumed.tx.send(segment()).await.unwrap();
                assert!(matches!(
                    events.recv().await,
                    Some(EngineMsg::SegmentWritten { .. })
                ));
                resumed
                    .tx
                    .send(WriteCmd::Finalize {
                        file_size: 8,
                        combined_crc: Some(crc32fast::hash(b"verified")),
                    })
                    .await
                    .unwrap();
                assert!(matches!(
                    events.recv().await,
                    Some(EngineMsg::WriterFinalized { ok: true, .. })
                ));
                assert_eq!(
                    std::fs::read(tmp.path().join("same.bin")).unwrap(),
                    b"verified"
                );
            }
        }
    }
    #[tokio::test]
    async fn scoped_writer_reserves_before_owner_ack_and_duplicate_does_not_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let tracker = TaskTracker::new();
        // Capacity one keeps the first acknowledgement outstanding while queued
        // conflicting and replay commands reach the serial writer.
        let (tx, mut rx) = channel(1);
        let writer = spawn_writer_scoped(
            &tracker,
            JobId(1),
            FileId(2),
            dir.path().to_path_buf(),
            "out".into(),
            tx,
            WriterScope {
                generation: 9,
                ..WriterScope::default()
            },
        );
        for (seg_number, offset, data, crc) in [
            (1, 0, vec![1, 2, 3], 42),
            (2, 2, vec![9, 9, 9], 43),
            (1, 0, vec![8, 8, 8], 42),
        ] {
            writer
                .tx
                .send(WriteCmd::Segment {
                    seg_number,
                    offset,
                    data,
                    crc,
                    file_size: 5,
                    server: ServerId(1),
                })
                .await
                .unwrap();
        }
        assert!(matches!(
            rx.recv().await,
            Some(EngineMsg::ScopedWriter {
                generation: 9,
                fact: WriterFact::Written { seg_number: 1, .. },
                ..
            })
        ));
        assert!(matches!(
            rx.recv().await,
            Some(EngineMsg::ScopedWriter {
                fact: WriterFact::Rejected { seg_number: 2, .. },
                ..
            })
        ));
        assert!(matches!(
            rx.recv().await,
            Some(EngineMsg::ScopedWriter {
                fact: WriterFact::Written { seg_number: 1, .. },
                ..
            })
        ));
        assert_eq!(
            std::fs::read(dir.path().join(".runner-file-2.part")).unwrap(),
            [1, 2, 3, 0, 0]
        );
        writer.stop.cancel();
        let mut stopped = writer.stopped.clone();
        stopped.wait_for(|done| *done).await.unwrap();
    }

    #[tokio::test]
    async fn seeded_writer_rejects_overlap_different_crc_and_trimming() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".runner-file-2.part");
        std::fs::write(&path, b"abcd").unwrap();
        let opened = nzbd_state::fileops::open(&path).unwrap();
        let identity = recovery::capture_identity(&opened).unwrap();
        let tracker = TaskTracker::new();
        let (tx, mut rx) = channel(8);
        let writer = spawn_writer_scoped(
            &tracker,
            JobId(1),
            FileId(2),
            dir.path().to_path_buf(),
            "out".into(),
            tx,
            WriterScope {
                generation: 4,
                proven: vec![ProvenRange {
                    seg_number: 1,
                    offset: 0,
                    len: 4,
                    crc: 42,
                }],
                expected_identity: Some(identity),
                ..WriterScope::default()
            },
        );
        for (seg_number, crc) in [(2, 42), (1, 43)] {
            writer
                .tx
                .send(WriteCmd::Segment {
                    seg_number,
                    offset: 0,
                    data: b"zzzz".to_vec(),
                    crc,
                    file_size: 4,
                    server: ServerId(1),
                })
                .await
                .unwrap();
            assert!(matches!(
                rx.recv().await,
                Some(EngineMsg::ScopedWriter {
                    fact: WriterFact::Rejected { .. },
                    ..
                })
            ));
        }
        writer
            .tx
            .send(WriteCmd::Finalize {
                file_size: 3,
                combined_crc: None,
            })
            .await
            .unwrap();
        assert!(matches!(
            rx.recv().await,
            Some(EngineMsg::ScopedWriter {
                fact: WriterFact::Error { .. },
                ..
            })
        ));
        assert_eq!(std::fs::read(path).unwrap(), b"abcd");
    }

    fn batch_segment(number: u32, offset: u64, bytes: &[u8], size: u64) -> WriteCmd {
        WriteCmd::Segment {
            seg_number: number,
            offset,
            data: bytes.to_vec(),
            crc: crc32fast::hash(bytes),
            file_size: size,
            server: ServerId(1),
        }
    }

    fn queued_writer(
        dir: &Path,
        commands: Vec<WriteCmd>,
        scope: WriterScope,
    ) -> (
        mpsc::Receiver<EngineMsg>,
        tokio_util::sync::CancellationToken,
        tokio::task::JoinHandle<()>,
    ) {
        let (tx, rx) = channel(WRITER_QUEUE);
        for command in commands {
            tx.try_send(command).unwrap();
        }
        drop(tx);
        let (events, received) = channel(32);
        let stop = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(writer_task(
            JobId(1),
            FileId(2),
            dir.into(),
            "out".into(),
            rx,
            events,
            stop.clone(),
            scope,
            true,
        ));
        (received, stop, task)
    }

    async fn batch_facts(events: &mut mpsc::Receiver<EngineMsg>) -> Vec<WriterFact> {
        let mut facts = Vec::new();
        while let Some(message) = naming_writer_event(events).await {
            match message {
                EngineMsg::ScopedWriter { fact, .. } => facts.push(fact),
                _ => panic!("scoped writer must retain its generation envelope"),
            }
        }
        facts
    }

    #[tokio::test]
    async fn queued_segments_share_one_sync_and_post_sync_identity() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let temp = tempfile::tempdir().unwrap();
        let syncs = Arc::new(AtomicUsize::new(0));
        let observed = syncs.clone();
        let commands = (1..=WRITER_QUEUE)
            .map(|n| batch_segment(n as u32, (n - 1) as u64, &[n as u8], WRITER_QUEUE as u64))
            .collect();
        let scope = WriterScope {
            sync_test_hook: Some(Arc::new(move |stage| {
                if stage == "before_sync" {
                    observed.fetch_add(1, Ordering::SeqCst);
                }
            })),
            ..Default::default()
        };
        let (mut events, _, task) = queued_writer(temp.path(), commands, scope);
        let facts = batch_facts(&mut events).await;
        task.await.unwrap();
        assert_eq!(facts.len(), WRITER_QUEUE);
        assert_eq!(
            syncs.load(Ordering::SeqCst),
            1,
            "an already queued prefix shares one durable flush"
        );
        let mut first = None;
        for (index, fact) in facts.iter().enumerate() {
            let WriterFact::Written {
                seg_number,
                identity,
                ..
            } = fact
            else {
                panic!("every valid segment is acknowledged");
            };
            assert_eq!(*seg_number, index as u32 + 1);
            if let Some(expected) = &first {
                assert_eq!(identity, expected);
            } else {
                first = Some(identity.clone());
            }
        }
        assert_eq!(
            std::fs::read(temp.path().join(".runner-file-2.part")).unwrap(),
            (1..=WRITER_QUEUE as u8).collect::<Vec<_>>()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_drains_written_batch_only_after_shared_sync() {
        use std::sync::{Arc, Mutex};
        let temp = tempfile::tempdir().unwrap();
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let entered = Mutex::new(Some(entered));
        let (release, gate) = std::sync::mpsc::channel();
        let gate = Mutex::new(gate);
        let scope = WriterScope {
            sync_test_hook: Some(Arc::new(move |stage| {
                if stage == "before_sync" {
                    if let Some(signal) = entered.lock().unwrap().take() {
                        let _ = signal.send(());
                    }
                    let _ = gate.lock().unwrap().recv();
                }
            })),
            ..Default::default()
        };
        let mut commands = (1..=4)
            .map(|n| batch_segment(n, u64::from(n - 1), &[n as u8], 4))
            .collect::<Vec<_>>();
        commands.push(WriteCmd::Finalize {
            file_size: 4,
            combined_crc: Some(crc32fast::hash(&[1, 2, 3, 4])),
        });
        let (mut events, stop, task) = queued_writer(temp.path(), commands, scope);
        tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap();
        let early = events.try_recv().is_ok();
        stop.cancel();
        release.send(()).unwrap();
        let facts = batch_facts(&mut events).await;
        task.await.unwrap();
        assert!(
            !early,
            "no batch acknowledgement may precede the shared durable flush"
        );
        assert_eq!(facts.len(), 4);
        assert!(facts
            .iter()
            .all(|fact| matches!(fact, WriterFact::Written { .. })));
        assert!(
            !temp.path().join("out").exists(),
            "cancelled queued Finalize must not publish"
        );
        assert_eq!(
            std::fs::read(temp.path().join(".runner-file-2.part")).unwrap(),
            [1, 2, 3, 4]
        );
    }

    #[tokio::test]
    async fn failed_shared_sync_acknowledges_none_and_retains_all_written_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let mut commands = vec![WriteCmd::InjectFailure("sync", 28)];
        commands.extend((1..=4).map(|n| batch_segment(n, u64::from(n - 1), &[n as u8], 4)));
        let (mut events, _, task) = queued_writer(temp.path(), commands, WriterScope::default());
        let facts = batch_facts(&mut events).await;
        task.await.unwrap();
        assert_eq!(facts.len(), 1);
        assert!(
            matches!(&facts[0], WriterFact::Error { error, cause: Some(nzbd_types::HoldCause::Capacity) } if error.contains("os error 28")),
            "a failed shared sync on a full volume holds for capacity, not failed/io: {facts:?}"
        );
        assert_eq!(
            std::fs::read(temp.path().join(".runner-file-2.part")).unwrap(),
            [1, 2, 3, 4]
        );
        assert!(!temp.path().join("out").exists());
    }

    #[tokio::test]
    async fn publication_name_and_finalize_do_not_overtake_a_pending_batch() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let temp = tempfile::tempdir().unwrap();
        let syncs = Arc::new(AtomicUsize::new(0));
        let observed = syncs.clone();
        let scope = WriterScope {
            sync_test_hook: Some(Arc::new(move |stage| {
                if stage == "before_sync" {
                    observed.fetch_add(1, Ordering::SeqCst);
                }
            })),
            ..Default::default()
        };
        let commands = vec![
            batch_segment(1, 0, b"a", 2),
            WriteCmd::PublicationName("published.bin".into()),
            batch_segment(2, 1, b"b", 2),
            WriteCmd::Finalize {
                file_size: 2,
                combined_crc: Some(crc32fast::hash(b"ab")),
            },
        ];
        let (mut events, _, task) = queued_writer(temp.path(), commands, scope);
        let facts = batch_facts(&mut events).await;
        task.await.unwrap();
        assert_eq!(
            syncs.load(Ordering::SeqCst),
            2,
            "a publication command separates durable prefixes"
        );
        assert!(matches!(
            &facts[..],
            [
                WriterFact::Written { seg_number: 1, .. },
                WriterFact::Written { seg_number: 2, .. },
                WriterFact::Finalized { ok: true, .. }
            ]
        ));
        assert_eq!(
            std::fs::read(temp.path().join("published.bin")).unwrap(),
            b"ab"
        );
    }

    #[tokio::test]
    async fn duplicate_and_rejected_overlap_preserve_batch_acknowledgement_order() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let temp = tempfile::tempdir().unwrap();
        let syncs = Arc::new(AtomicUsize::new(0));
        let observed = syncs.clone();
        let scope = WriterScope {
            sync_test_hook: Some(Arc::new(move |stage| {
                if stage == "before_sync" {
                    observed.fetch_add(1, Ordering::SeqCst);
                }
            })),
            ..Default::default()
        };
        let commands = vec![
            batch_segment(1, 0, b"ab", 4),
            batch_segment(1, 0, b"ab", 4),
            batch_segment(9, 1, b"xx", 4),
            batch_segment(2, 2, b"cd", 4),
        ];
        let (mut events, _, task) = queued_writer(temp.path(), commands, scope);
        let facts = batch_facts(&mut events).await;
        task.await.unwrap();
        assert_eq!(syncs.load(Ordering::SeqCst), 2);
        assert!(matches!(
            &facts[..],
            [
                WriterFact::Written { seg_number: 1, .. },
                WriterFact::Written { seg_number: 1, .. },
                WriterFact::Rejected { seg_number: 9, .. },
                WriterFact::Written { seg_number: 2, .. }
            ]
        ));
        assert_eq!(
            std::fs::read(temp.path().join(".runner-file-2.part")).unwrap(),
            b"abcd"
        );
    }

    #[tokio::test]
    async fn external_mutation_during_shared_sync_is_not_acknowledged() {
        use std::sync::Arc;
        let temp = tempfile::tempdir().unwrap();
        let part = temp.path().join(".runner-file-2.part");
        let changed = part.clone();
        let scope = WriterScope {
            sync_test_hook: Some(Arc::new(move |stage| {
                if stage == "before_sync" {
                    std::fs::write(&changed, b"foreign mutation").unwrap();
                }
            })),
            ..Default::default()
        };
        let commands = vec![batch_segment(1, 0, b"ab", 4), batch_segment(2, 2, b"cd", 4)];
        let (mut events, _, task) = queued_writer(temp.path(), commands, scope);
        let facts = batch_facts(&mut events).await;
        task.await.unwrap();
        assert!(
            matches!(&facts[..], [WriterFact::Error { error, cause: None }] if error.contains("identity"))
        );
        assert_eq!(std::fs::read(part).unwrap(), b"foreign mutation");
        assert!(!temp.path().join("out").exists());
    }
}
