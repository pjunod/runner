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

#[derive(Clone, Debug, Default)]
pub(crate) struct WriterScope {
    pub generation: u64,
    pub proven: Vec<ProvenRange>,
    pub expected_identity: Option<Identity>,
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
    },
    Error {
        error: String,
    },
    Rejected {
        seg_number: u32,
        server: ServerId,
        reason: String,
    },
}

const WRITER_QUEUE: usize = 8;

/// Compatibility entry point for direct callers without durable scoped evidence.
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
                    ..
                } => EngineMsg::WriterFinalized {
                    job: self.job,
                    file: self.file,
                    ok,
                    final_path,
                    combined_crc,
                },
                WriterFact::Error { error } => EngineMsg::WriterError {
                    job: self.job,
                    file: self.file,
                    error,
                },
                WriterFact::Rejected { reason, .. } => EngineMsg::WriterError {
                    job: self.job,
                    file: self.file,
                    error: reason,
                },
            }
        };
        self.tx.send(message).await.is_ok()
    }
    async fn error(&self, error: impl ToString) {
        self.send(WriterFact::Error {
            error: error.to_string(),
        })
        .await;
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
            report
                .error(format!("write admission / payload resolution: {error:?}"))
                .await;
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
                report
                    .error(format!("legacy partial migration: {error}"))
                    .await;
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
                    report.error(error).await;
                    return;
                }
            };
            expected = match recovery::capture_identity(&opened) {
                Ok(identity) => Some(identity),
                Err(error) => {
                    report.error(error).await;
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
    loop {
        // Never cancel an in-flight seek/write/sync. Stop before the next command.
        let cmd = tokio::select! { biased; _ = stop.cancelled() => break,
        command = rx.recv() => match command { Some(command)=>command,None=>break } };
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
                        .error(format!("write {}: {error}", part_path.display()))
                        .await;
                    return;
                }
                if !duplicate {
                    if reservation.is_none() && file_size > 0 {
                        match nzbd_state::capacity::reserve(&dir, file_size) {
                            Ok(claim) => reservation = Some(claim),
                            Err(error) => {
                                report
                                    .error(format!("write admission {}: {error}", dir.display()))
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
                            .error(format!(
                                "write: {}",
                                std::io::Error::from_raw_os_error(code)
                            ))
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
                        // Segment sync is the durability boundary for every acknowledgement.
                        f.sync_data().await?;
                        Ok::<_, std::io::Error>(())
                    }
                    .await;
                    #[cfg(test)]
                    let result = if let Some(("sync", code)) = injected {
                        Err(std::io::Error::from_raw_os_error(code))
                    } else {
                        result
                    };
                    if let Err(error) = result {
                        report
                            .error(format!("write {}: {error}", part_path.display()))
                            .await;
                        return;
                    }
                }
                let identity = match file_identity(out.as_ref().unwrap()).await {
                    Ok(identity) => identity,
                    Err(error) => {
                        report.error(error).await;
                        return;
                    }
                };
                expected = Some(identity.clone());
                if !report
                    .send(WriterFact::Written {
                        seg_number,
                        offset,
                        len: range.len,
                        crc,
                        file_size,
                        server,
                        identity,
                    })
                    .await
                {
                    return;
                }
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
                            })
                            .await;
                    }
                    Err(error) => report.error(format!("seal partial: {error}")).await,
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
                        .error(format!(
                            "finalize: {}",
                            std::io::Error::from_raw_os_error(code)
                        ))
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
                    file_size,
                    combined_crc,
                )
                .await;
                match result {
                    Ok(identity) => {
                        report
                            .send(WriterFact::Finalized {
                                ok: true,
                                final_path: Some(final_path.clone()),
                                combined_crc,
                                identity: Some(identity),
                            })
                            .await;
                    }
                    Err(error) => {
                        report
                            .error(format!("finalize identity/publication: {error}"))
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

async fn finalize(
    dir: &Path,
    part: &Path,
    final_path: &Path,
    out: &mut Option<File>,
    expected: &Option<Identity>,
    file_size: u64,
    combined_crc: Option<u32>,
) -> std::io::Result<Identity> {
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
                    return require_valid(final_path, 0, file_size, crc, Some(&identity));
                }
                if expected.is_some() {
                    return Ok(identity);
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
    if let Some(crc) = combined_crc {
        require_valid(part, 0, file_size, crc, Some(&identity))?;
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
            let target = nzbd_state::fileops::open(final_path).map_err(std::io::Error::other)?;
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
    drop(out.take());
    Ok(identity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::channel;

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
            Some(EngineMsg::WriterError { job, file, error }) => {
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
}
