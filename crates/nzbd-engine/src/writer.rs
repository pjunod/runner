//! Per-file disk writer tasks (ARCHITECTURE.md §8.4).
//!
//! One task owns each output file; decoded segments arrive over a bounded
//! channel (backpressure) from whichever connection task decoded them.
//! DirectWrite semantics: the file is preallocated sparse to its full yEnc
//! size on first write, each part is written at its yEnc offset
//! (`begin − 1`), gaps stay zero-filled, and completion is an atomic rename
//! from `<name>.part` to `<name>`. Resume reopens the `.part` file without
//! truncation.

use crate::owner::EngineMsg;
use nzbd_types::{FileId, JobId, ServerId};
use std::io::SeekFrom;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;

#[derive(Debug)]
pub enum WriteCmd {
    PublicationName(String),
    /// Flush and validate received ranges, retaining the private partial path.
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
        /// Total output-file size from the yEnc header (0 = unknown).
        file_size: u64,
        server: ServerId,
    },
    /// All segments accounted for: extend/trim to `file_size`, fsync,
    /// rename into place. `combined_crc` is the whole-file CRC when every
    /// segment succeeded contiguously.
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
}

/// Bounded queue per file: decoded segments are large, keep few in flight.
const WRITER_QUEUE: usize = 8;

pub fn spawn_writer(
    tracker: &TaskTracker,
    job: JobId,
    file: FileId,
    dir: PathBuf,
    final_name: String,
    engine_tx: mpsc::Sender<EngineMsg>,
) -> WriterHandle {
    let (tx, rx) = mpsc::channel(WRITER_QUEUE);
    let stop = tokio_util::sync::CancellationToken::new();
    let cancellation = stop.clone();
    let (done, stopped) = tokio::sync::watch::channel(false);
    tracker.spawn(async move {
        writer_task(job, file, dir, final_name, rx, engine_tx, cancellation).await;
        let _ = done.send(true);
    });
    WriterHandle { tx, stop, stopped }
}

async fn writer_task(
    job: JobId,
    file_id: FileId,
    dir: PathBuf,
    final_name: String,
    mut rx: mpsc::Receiver<WriteCmd>,
    engine_tx: mpsc::Sender<EngineMsg>,
    stop: tokio_util::sync::CancellationToken,
) {
    let part_path = dir.join(format!(".runner-file-{}.part", file_id.0));
    let legacy = dir.join(format!("{final_name}.part"));
    if !part_path.exists() && legacy.exists() {
        // Exclusive alias migration retains the legacy entry as evidence.
        if let Err(e) = nzbd_state::fileops::link(&legacy, &part_path) {
            let _ = engine_tx
                .send(EngineMsg::WriterError {
                    job,
                    file: file_id,
                    error: format!("legacy partial migration: {e}"),
                })
                .await;
            return;
        }
    }
    let mut final_path = dir.join(&final_name);
    let mut out: Option<File> = None;
    let mut preallocated = false;
    let mut reservation = None;
    #[cfg(test)]
    let mut fault = None;

    loop {
        let cmd = tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            cmd = rx.recv() => match cmd { Some(cmd) => cmd, None => break },
        };
        match cmd {
            #[cfg(test)]
            WriteCmd::InjectFailure(stage, code) => {
                fault = Some((stage, code));
            }
            WriteCmd::SealPartial { file_size, ranges } => {
                let result = async {
                    if let Some(f) = out.as_mut() {
                        f.sync_data().await?;
                    }
                    drop(out.take());
                    let metadata = std::fs::symlink_metadata(&part_path)?;
                    if file_size == 0
                        || !metadata.is_file()
                        || metadata.len() != file_size
                        || ranges.iter().any(|(offset, len, crc)| {
                            !validate_range(&part_path, *offset, u64::from(*len), *crc)
                        })
                    {
                        return Err(std::io::Error::other("partial checkpoint identity differs"));
                    }
                    Ok::<_, std::io::Error>(())
                }
                .await;
                let message = match result {
                    Ok(()) => EngineMsg::WriterFinalized {
                        job,
                        file: file_id,
                        ok: true,
                        final_path: Some(part_path.clone()),
                        combined_crc: None,
                    },
                    Err(error) => EngineMsg::WriterError {
                        job,
                        file: file_id,
                        error: format!("seal partial: {error}"),
                    },
                };
                let _ = engine_tx.send(message).await;
                return;
            }
            WriteCmd::PublicationName(name) => {
                if name.is_empty()
                    || name.contains(['/', '\\', ':', '\0'])
                    || name == "."
                    || name == ".."
                {
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
                if reservation.is_none() && file_size > 0 {
                    let _ = tokio::fs::create_dir_all(&dir).await;
                    match nzbd_state::capacity::reserve(&dir, file_size) {
                        Ok(claim) => reservation = Some(claim),
                        Err(e) => {
                            let _ = engine_tx
                                .send(EngineMsg::WriterError {
                                    job,
                                    file: file_id,
                                    error: format!("write admission {}: {e}", dir.display()),
                                })
                                .await;
                            return;
                        }
                    }
                }
                #[cfg(test)]
                let injected = fault.take();
                #[cfg(test)]
                if let Some(("write", code)) = injected {
                    let _ = engine_tx
                        .send(EngineMsg::WriterError {
                            job,
                            file: file_id,
                            error: format!("write: {}", std::io::Error::from_raw_os_error(code)),
                        })
                        .await;
                    return;
                }
                let result = write_segment(
                    &dir,
                    &part_path,
                    &mut out,
                    &mut preallocated,
                    offset,
                    &data,
                    file_size,
                )
                .await;
                #[cfg(test)]
                let result = if let Some(("sync", code)) = injected {
                    Err(std::io::Error::from_raw_os_error(code))
                } else {
                    result
                };
                let msg = match result {
                    Ok(()) => EngineMsg::SegmentWritten {
                        job,
                        file: file_id,
                        seg_number,
                        offset,
                        len: data.len() as u32,
                        crc,
                        file_size,
                        server,
                    },
                    Err(e) => {
                        let _ = engine_tx
                            .send(EngineMsg::WriterError {
                                job,
                                file: file_id,
                                error: format!("write {}: {e}", part_path.display()),
                            })
                            .await;
                        return; // drop/drain the channel; no more writes after failure
                    }
                };
                if engine_tx.send(msg).await.is_err() {
                    break; // engine gone
                }
            }
            WriteCmd::Finalize {
                file_size,
                combined_crc,
            } => {
                #[cfg(test)]
                if let Some(("finalize", code)) = fault.take() {
                    let _ = engine_tx
                        .send(EngineMsg::WriterError {
                            job,
                            file: file_id,
                            error: format!("finalize: {}", std::io::Error::from_raw_os_error(code)),
                        })
                        .await;
                    return;
                }
                let result =
                    finalize(&part_path, &final_path, &mut out, file_size, combined_crc).await;
                let msg = match result {
                    Ok(()) => EngineMsg::WriterFinalized {
                        job,
                        file: file_id,
                        ok: true,
                        final_path: Some(final_path.clone()),
                        combined_crc,
                    },
                    Err(e) => {
                        tracing::warn!(job = job.0, file = file_id.0, error = %e, "finalize failed");
                        // Ground truth about the volume, from the one
                        // place that actually tried to use it.
                        if crate::is_out_of_space(&e.to_string()) {
                            let _ = engine_tx
                                .send(EngineMsg::WriterError {
                                    job,
                                    file: file_id,
                                    error: format!("finalize {}: {e}", final_path.display()),
                                })
                                .await;
                            return;
                        }
                        EngineMsg::WriterError {
                            job,
                            file: file_id,
                            error: format!("finalize identity/publication: {e}"),
                        }
                    }
                };
                let _ = engine_tx.send(msg).await;
                return;
            }
        }
    }
    // Channel closed without Finalize: job deleted or engine stopping.
    // Leave the `.part` file for resume / directory cleanup.
}

async fn write_segment(
    dir: &PathBuf,
    part_path: &std::path::Path,
    out: &mut Option<File>,
    preallocated: &mut bool,
    offset: u64,
    data: &[u8],
    file_size: u64,
) -> std::io::Result<()> {
    if out.is_none() {
        tokio::fs::create_dir_all(dir).await?;
        // No truncate: resume must keep already-written parts.
        let f = File::from_std(
            nzbd_state::fileops::writer(part_path, true).map_err(std::io::Error::other)?,
        );
        *out = Some(f);
    }
    let f = out.as_mut().unwrap();
    if !*preallocated && file_size > 0 {
        let current = f.metadata().await?.len();
        if current < file_size {
            // Sparse preallocation (POSIX truncate-up), NZBGet DirectWrite.
            f.set_len(file_size).await?;
        }
        *preallocated = true;
    }
    f.seek(SeekFrom::Start(offset)).await?;
    if file_size > 0
        && offset
            .checked_add(data.len() as u64)
            .is_none_or(|end| end > file_size)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "segment exceeds declared size",
        ));
    }
    f.write_all(data).await?;
    // Checkpoint boundary: one segment. Durability precedes journal/snapshot proof.
    f.sync_data().await?;
    Ok(())
}

async fn finalize(
    part_path: &std::path::Path,
    final_path: &std::path::Path,
    out: &mut Option<File>,
    file_size: u64,
    combined_crc: Option<u32>,
) -> std::io::Result<()> {
    if out.is_none() {
        match nzbd_state::fileops::writer(part_path, false)
            .map(File::from_std)
            .map_err(|e| match e {
                nzbd_state::artifacts::Error::Io(io) => io,
                other => std::io::Error::other(other.to_string()),
            }) {
            Ok(f) => *out = Some(f),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if file_size > 0
                    && combined_crc.is_some_and(|crc| validate_range(final_path, 0, file_size, crc))
                {
                    return Ok(()); // size and complete checkpoint digest establish identity
                }
                return Err(e);
            }
            Err(e) => return Err(e),
        }
    }
    let f = out.as_mut().unwrap();
    if file_size > 0 {
        // Zero-fill trailing gap / trim over-preallocation.
        f.set_len(file_size).await?;
    }
    // Data durability at the completion boundary; per-segment writes stay
    // relaxed (page cache), matching the configured-default policy.
    //
    // This is also the only point at which a deferred writeback failure can
    // reach us. Per-segment writes land in the page cache and return Ok; on a
    // network filesystem the ENOSPC/EDQUOT/EIO that killed them surfaces here
    // or nowhere at all.
    f.sync_data().await?;
    drop(out.take()); // close before rename

    // Then check the obvious thing, because the obvious thing turned out not
    // to be true: is the file the length it is supposed to be?
    //
    // Two 40-60 GB remuxes finished as SUCCESS with 500 MiB on disk — stopped
    // dead on a 500 MiB boundary by a limit outside this process, and reported
    // complete because nothing ever compared what was written against what was
    // promised. Every other check in the engine is about the article set:
    // whether the bytes arrived off the wire. None of them look at the file. A
    // downloader may report that it failed; it may not report success for a
    // file it did not write.
    if file_size > 0 {
        let on_disk = tokio::fs::metadata(part_path).await?.len();
        if on_disk != file_size {
            return Err(std::io::Error::other(format!(
                "{} is {on_disk} bytes on disk but should be {file_size}: \
                 {} bytes never reached the filesystem",
                part_path.display(),
                file_size.saturating_sub(on_disk),
            )));
        }
    }

    if let Some(crc) = combined_crc {
        if !validate_range(part_path, 0, file_size, crc) {
            return Err(std::io::Error::other("complete checkpoint digest differs"));
        }
    }
    nzbd_state::fileops::link(part_path, final_path).map_err(std::io::Error::other)?;
    if let Some(parent) = final_path.parent() {
        File::open(parent).await?.sync_all().await?;
    }
    tokio::fs::remove_file(part_path).await?;
    Ok(())
}

pub(crate) fn validate_range(path: &std::path::Path, offset: u64, len: u64, crc: u32) -> bool {
    use std::io::{Read, Seek};
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
    /// remuxes reported SUCCESS with 500 MiB on disk. `sync_data` is the only
    /// point at which a deferred writeback error can reach us — per-segment
    /// writes land in the page cache and return Ok, so on a network
    /// filesystem the ENOSPC/EDQUOT/EIO that killed them surfaces here or
    /// nowhere. Losing it means calling a download that never reached the
    /// disk finished.
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
}
