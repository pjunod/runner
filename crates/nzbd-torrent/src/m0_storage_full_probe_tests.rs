use super::{
    TorrentAddConfig, TorrentError, TorrentHandle, TorrentPhase, TorrentSession, TorrentStats,
    DISPLAY_SAFE_ERROR_MAX_BYTES,
};
use anyhow::Result;
use librqbit::storage::filesystem::FilesystemStorageFactory;
use librqbit::storage::{BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage};
use librqbit::{ManagedTorrentShared, TorrentMetadata};
use sha1::{Digest, Sha1};
use std::error::Error;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const FAULT_FILE_NAME: &str = "storage-full.bin";
const SIZING_FILE_NAME: &str = "storage-sizing-full.bin";
const CONTROL_FILE_NAME: &str = "storage-control.bin";
const PIECE_LENGTH: usize = 16 * 1024;
const FAULT_PAYLOAD_BYTES: usize = 256 * 1024;
const CONTROL_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const CONTROL_UPLOAD_LIMIT_BPS: u32 = 8 * 1024 * 1024;
const TEST_LISTEN_PORT_START: u16 = 49_152;
const FAULT_DEADLINE: Duration = Duration::from_secs(20);
const CONTROL_DEADLINE: Duration = Duration::from_secs(30);
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(10);
const STATS_DEADLINE: Duration = Duration::from_secs(1);
const INJECTED_ERROR: &str = "injected M0 storage-full fault";
const INJECTED_SIZING_ERROR: &str = "injected M0 storage sizing-full fault";

#[derive(Clone, Copy)]
enum FaultPoint {
    WriteAfter { successful_writes: usize },
    EnsureFileLength,
}

#[derive(Clone)]
struct StorageFullFactory {
    fault_point: FaultPoint,
    write_attempts: Arc<AtomicUsize>,
    successful_bytes: Arc<AtomicUsize>,
    ensure_length_attempts: Arc<AtomicUsize>,
    requested_length: Arc<AtomicUsize>,
}

impl StorageFactory for StorageFullFactory {
    type Storage = StorageFullStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> Result<Self::Storage> {
        Ok(StorageFullStorage {
            inner: Box::new(FilesystemStorageFactory::default().create(shared, metadata)?),
            fault_point: self.fault_point,
            write_attempts: self.write_attempts.clone(),
            successful_bytes: self.successful_bytes.clone(),
            ensure_length_attempts: self.ensure_length_attempts.clone(),
            requested_length: self.requested_length.clone(),
        })
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

struct StorageFullStorage {
    inner: Box<dyn TorrentStorage>,
    fault_point: FaultPoint,
    write_attempts: Arc<AtomicUsize>,
    successful_bytes: Arc<AtomicUsize>,
    ensure_length_attempts: Arc<AtomicUsize>,
    requested_length: Arc<AtomicUsize>,
}

impl TorrentStorage for StorageFullStorage {
    fn init(&mut self, shared: &ManagedTorrentShared, metadata: &TorrentMetadata) -> Result<()> {
        self.inner.init(shared, metadata)
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buffer: &mut [u8]) -> Result<()> {
        self.inner.pread_exact(file_id, offset, buffer)
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buffer: &[u8]) -> Result<()> {
        let attempt = self.write_attempts.fetch_add(1, Ordering::SeqCst);
        if matches!(
            self.fault_point,
            FaultPoint::WriteAfter { successful_writes } if attempt >= successful_writes
        ) {
            return Err(io::Error::new(io::ErrorKind::StorageFull, INJECTED_ERROR).into());
        }
        self.inner.pwrite_all(file_id, offset, buffer)?;
        self.successful_bytes
            .fetch_add(buffer.len(), Ordering::SeqCst);
        Ok(())
    }

    fn remove_file(&self, file_id: usize, filename: &Path) -> Result<()> {
        self.inner.remove_file(file_id, filename)
    }

    fn remove_directory_if_empty(&self, path: &Path) -> Result<()> {
        self.inner.remove_directory_if_empty(path)
    }

    fn ensure_file_length(&self, file_id: usize, length: u64) -> Result<()> {
        self.ensure_length_attempts.fetch_add(1, Ordering::SeqCst);
        self.requested_length
            .store(length.try_into()?, Ordering::SeqCst);
        if matches!(self.fault_point, FaultPoint::EnsureFileLength) {
            return Err(io::Error::new(io::ErrorKind::StorageFull, INJECTED_SIZING_ERROR).into());
        }
        self.inner.ensure_file_length(file_id, length)
    }

    fn take(&self) -> Result<Box<dyn TorrentStorage>> {
        Ok(Box::new(Self {
            inner: self.inner.take()?,
            fault_point: self.fault_point,
            write_attempts: self.write_attempts.clone(),
            successful_bytes: self.successful_bytes.clone(),
            ensure_length_attempts: self.ensure_length_attempts.clone(),
            requested_length: self.requested_length.clone(),
        }))
    }
}

fn payload(size: usize, salt: usize) -> Vec<u8> {
    (0..size)
        .map(|index| ((index * 31 + index / 7 + salt) % 251) as u8)
        .collect()
}

fn bencode_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(bytes.len().to_string().as_bytes());
    output.push(b':');
    output.extend_from_slice(bytes);
}

fn metainfo(payload: &[u8], filename: &str) -> Vec<u8> {
    let mut pieces = Vec::new();
    for piece in payload.chunks(PIECE_LENGTH) {
        pieces.extend_from_slice(&Sha1::digest(piece));
    }

    let mut info = Vec::new();
    info.push(b'd');
    bencode_bytes(&mut info, b"length");
    info.extend_from_slice(format!("i{}e", payload.len()).as_bytes());
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, filename.as_bytes());
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(format!("i{PIECE_LENGTH}e").as_bytes());
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &pieces);
    info.push(b'e');

    let mut torrent = b"d4:info".to_vec();
    torrent.extend_from_slice(&info);
    torrent.push(b'e');
    torrent
}

async fn stop_with_deadline(session: TorrentSession, name: &str) -> Result<(), Box<dyn Error>> {
    tokio::time::timeout(SHUTDOWN_DEADLINE, session.stop())
        .await
        .map_err(|_| io::Error::other(format!("{name} shutdown exceeded 10 seconds")))?;
    Ok(())
}

fn stats_with_deadline(
    handle: &TorrentHandle,
    name: &str,
) -> Result<(TorrentStats, Duration), Box<dyn Error>> {
    let (sender, receiver) = mpsc::sync_channel(1);
    let handle = handle.clone();
    let started = Instant::now();
    thread::spawn(move || {
        let _ = sender.send(handle.stats());
    });
    let stats = receiver.recv_timeout(STATS_DEADLINE).map_err(|error| {
        io::Error::other(format!(
            "{name} stats did not return within one second: {error}"
        ))
    })?;
    Ok((stats, started.elapsed()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "native M0 storage sizing-fault admission probe; run explicitly"]
async fn storage_full_during_file_sizing_is_fail_closed() -> Result<(), Box<dyn Error>> {
    let sizing_payload = payload(FAULT_PAYLOAD_BYTES, 13);
    let sizing_metainfo = metainfo(&sizing_payload, SIZING_FILE_NAME);
    let download_root = tempfile::tempdir()?;
    let downloader =
        TorrentSession::start(download_root.path().to_path_buf(), Default::default()).await?;
    let write_attempts = Arc::new(AtomicUsize::new(0));
    let successful_bytes = Arc::new(AtomicUsize::new(0));
    let ensure_length_attempts = Arc::new(AtomicUsize::new(0));
    let requested_length = Arc::new(AtomicUsize::new(0));

    let admission_started = Instant::now();
    let admission_result = tokio::time::timeout(
        FAULT_DEADLINE,
        downloader.add_metainfo_with_storage_for_m0(
            sizing_metainfo,
            TorrentAddConfig {
                paused: true,
                overwrite: true,
                ..Default::default()
            },
            StorageFullFactory {
                fault_point: FaultPoint::EnsureFileLength,
                write_attempts: write_attempts.clone(),
                successful_bytes: successful_bytes.clone(),
                ensure_length_attempts: ensure_length_attempts.clone(),
                requested_length: requested_length.clone(),
            }
            .boxed(),
        ),
    )
    .await
    .map_err(|_| io::Error::other("sizing-fault admission exceeded 20 seconds"))?;
    let admission_elapsed = admission_started.elapsed();
    let (control_plane, stats_elapsed) = match admission_result {
        Err(TorrentError::Engine(error_text)) => {
            validate_sizing_error(&error_text)?;
            ("admission_error", Duration::ZERO)
        }
        Err(error) => {
            return Err(io::Error::other(format!(
                "sizing fault returned the wrong adapter error: {error}"
            ))
            .into());
        }
        Ok(handle) => {
            let initialization_error =
                tokio::time::timeout(FAULT_DEADLINE, handle.wait_until_initialized())
                    .await
                    .map_err(|_| {
                        io::Error::other("sizing-fault initialization exceeded 20 seconds")
                    })?
                    .expect_err("sizing fault must fail initialization");
            let TorrentError::Engine(initialization_error) = initialization_error else {
                return Err(io::Error::other(format!(
                    "sizing fault returned the wrong initialization error: {initialization_error}"
                ))
                .into());
            };
            validate_sizing_error(&initialization_error)?;

            let (stats, stats_elapsed) = stats_with_deadline(&handle, "sizing-fault torrent")?;
            let stats_error = stats.error.as_deref().ok_or_else(|| {
                io::Error::other(format!(
                    "sizing fault reached stats without an error: {stats:?}"
                ))
            })?;
            validate_sizing_error(stats_error)?;
            if stats.phase != TorrentPhase::Error || stats.finished || stats.progress_bytes != 0 {
                return Err(io::Error::other(format!(
                    "sizing fault did not remain in the error state: {stats:?}"
                ))
                .into());
            }
            ("torrent_error", stats_elapsed)
        }
    };

    let payload_path = download_root.path().join(SIZING_FILE_NAME);
    let observed_file_bytes = std::fs::metadata(&payload_path)?.len();
    let admitted_torrents = downloader.inner.with_torrents(|torrents| torrents.count());
    if ensure_length_attempts.load(Ordering::SeqCst) != 1
        || requested_length.load(Ordering::SeqCst) != FAULT_PAYLOAD_BYTES
        || write_attempts.load(Ordering::SeqCst) != 0
        || successful_bytes.load(Ordering::SeqCst) != 0
        || observed_file_bytes != 0
    {
        return Err(io::Error::other(format!(
            "sizing fault did not fail closed: control_plane={control_plane}, admitted_torrents={admitted_torrents}, ensure_length_attempts={}, requested_length={}, write_attempts={}, successful_bytes={}, observed_file_bytes={observed_file_bytes}",
            ensure_length_attempts.load(Ordering::SeqCst),
            requested_length.load(Ordering::SeqCst),
            write_attempts.load(Ordering::SeqCst),
            successful_bytes.load(Ordering::SeqCst)
        ))
        .into());
    }

    stop_with_deadline(downloader, "sizing-fault downloader").await?;
    println!(
        "bittorrent_storage_sizing_full injected_fault_kind=StorageFull ensure_length_attempts={} requested_bytes={} observed_file_bytes={} control_plane={} admitted_torrents={} write_attempts={} admission_ms={} stats_ms={}",
        ensure_length_attempts.load(Ordering::SeqCst),
        requested_length.load(Ordering::SeqCst),
        observed_file_bytes,
        control_plane,
        admitted_torrents,
        write_attempts.load(Ordering::SeqCst),
        admission_elapsed.as_millis(),
        stats_elapsed.as_millis()
    );
    Ok(())
}

fn validate_sizing_error(error: &str) -> Result<(), Box<dyn Error>> {
    if error.len() > DISPLAY_SAFE_ERROR_MAX_BYTES
        || !error.contains(SIZING_FILE_NAME)
        || !error.contains(&FAULT_PAYLOAD_BYTES.to_string())
        || !error.contains(INJECTED_SIZING_ERROR)
    {
        return Err(io::Error::other(format!(
            "sizing fault lost its bounded file, length, or storage cause: {error}"
        ))
        .into());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "native M0 storage-fault probe; run explicitly"]
async fn storage_full_write_is_torrent_scoped() -> Result<(), Box<dyn Error>> {
    let fault_payload = payload(FAULT_PAYLOAD_BYTES, 7);
    let control_payload = payload(CONTROL_PAYLOAD_BYTES, 19);
    let fault_metainfo = metainfo(&fault_payload, FAULT_FILE_NAME);
    let control_metainfo = metainfo(&control_payload, CONTROL_FILE_NAME);

    let seed_root = tempfile::tempdir()?;
    std::fs::write(seed_root.path().join(FAULT_FILE_NAME), &fault_payload)?;
    std::fs::write(seed_root.path().join(CONTROL_FILE_NAME), &control_payload)?;
    let seeder = TorrentSession::start_with_listen_range_for_m0(
        seed_root.path().to_path_buf(),
        TEST_LISTEN_PORT_START..u16::MAX,
    )
    .await?;
    let seeder_port = seeder
        .tcp_listen_port()
        .ok_or_else(|| io::Error::other("M0 seeder did not expose its selected listen port"))?;
    seeder.set_upload_limit_bps(NonZeroU32::new(CONTROL_UPLOAD_LIMIT_BPS));
    let fault_seed = seeder
        .add_metainfo(
            fault_metainfo.clone(),
            TorrentAddConfig {
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
    let control_seed = seeder
        .add_metainfo(
            control_metainfo.clone(),
            TorrentAddConfig {
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
    tokio::time::timeout(FAULT_DEADLINE, fault_seed.wait_until_completed())
        .await
        .map_err(|_| io::Error::other("fault seeder initialization exceeded 20 seconds"))??;
    tokio::time::timeout(FAULT_DEADLINE, control_seed.wait_until_completed())
        .await
        .map_err(|_| io::Error::other("control seeder initialization exceeded 20 seconds"))??;

    let peer = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, seeder_port));
    let download_root = tempfile::tempdir()?;
    let downloader =
        TorrentSession::start(download_root.path().to_path_buf(), Default::default()).await?;

    let control_started = Instant::now();
    let control = downloader
        .add_metainfo(
            control_metainfo,
            TorrentAddConfig {
                overwrite: true,
                initial_peers: vec![peer],
                ..Default::default()
            },
        )
        .await?;
    let control_before_fault = tokio::time::timeout(CONTROL_DEADLINE, async {
        loop {
            let (stats, _) = stats_with_deadline(&control, "control torrent")?;
            if stats.phase == TorrentPhase::Live
                && stats.progress_bytes > 0
                && stats.progress_bytes < stats.total_bytes
            {
                break Ok::<_, Box<dyn Error>>(stats);
            }
            if stats.finished || stats.phase == TorrentPhase::Error {
                break Err(io::Error::other(format!(
                    "control torrent did not expose an in-flight state: {stats:?}"
                ))
                .into());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| io::Error::other("control torrent was not in flight within 30 seconds"))??;

    let write_attempts = Arc::new(AtomicUsize::new(0));
    let successful_bytes = Arc::new(AtomicUsize::new(0));
    let ensure_length_attempts = Arc::new(AtomicUsize::new(0));
    let requested_length = Arc::new(AtomicUsize::new(0));
    let fault = downloader
        .add_metainfo_with_storage_for_m0(
            fault_metainfo,
            TorrentAddConfig {
                overwrite: true,
                initial_peers: vec![peer],
                ..Default::default()
            },
            StorageFullFactory {
                fault_point: FaultPoint::WriteAfter {
                    successful_writes: 1,
                },
                write_attempts: write_attempts.clone(),
                successful_bytes: successful_bytes.clone(),
                ensure_length_attempts: ensure_length_attempts.clone(),
                requested_length: requested_length.clone(),
            }
            .boxed(),
        )
        .await?;

    let fault_started = Instant::now();
    let (fault_stats, stats_elapsed) = tokio::time::timeout(FAULT_DEADLINE, async {
        loop {
            if write_attempts.load(Ordering::SeqCst) >= 2 {
                let (stats, stats_elapsed) = stats_with_deadline(&fault, "faulted torrent")?;
                if stats.phase == TorrentPhase::Error {
                    break Ok::<_, Box<dyn Error>>((stats, stats_elapsed));
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| {
        io::Error::other("injected storage-full fault was not visible within 20 seconds")
    })??;
    let fault_elapsed = fault_started.elapsed();
    if fault_stats.finished
        || fault_stats.progress_bytes >= fault_stats.total_bytes
        || fault_stats
            .error
            .as_deref()
            .is_none_or(|error| !error.contains(INJECTED_ERROR))
        || write_attempts.load(Ordering::SeqCst) != 2
        || successful_bytes.load(Ordering::SeqCst) != PIECE_LENGTH
    {
        return Err(io::Error::other(format!(
            "storage-full state did not match the contract: stats={fault_stats:?}, write_attempts={}, successful_bytes={}",
            write_attempts.load(Ordering::SeqCst),
            successful_bytes.load(Ordering::SeqCst)
        ))
        .into());
    }
    if std::fs::read(download_root.path().join(FAULT_FILE_NAME))? == fault_payload {
        return Err(io::Error::other("faulted payload was incorrectly complete").into());
    }

    let (control_at_fault, _) = stats_with_deadline(&control, "control torrent at fault")?;
    if control_at_fault.phase != TorrentPhase::Live
        || control_at_fault.finished
        || control_at_fault.progress_bytes == 0
        || control_at_fault.progress_bytes >= control_at_fault.total_bytes
    {
        return Err(io::Error::other(format!(
            "control torrent was not still in flight at the fault boundary: before={control_before_fault:?}, at_fault={control_at_fault:?}"
        ))
        .into());
    }

    tokio::time::timeout(CONTROL_DEADLINE, control.wait_until_completed())
        .await
        .map_err(|_| io::Error::other("same-session control download exceeded 30 seconds"))??;
    let control_elapsed = control_started.elapsed();
    if std::fs::read(download_root.path().join(CONTROL_FILE_NAME))? != control_payload {
        return Err(io::Error::other("same-session control payload did not match").into());
    }

    let (fault_after_control, _) = stats_with_deadline(&fault, "faulted torrent after control")?;
    if fault_after_control.phase != TorrentPhase::Error
        || fault_after_control.progress_bytes != fault_stats.progress_bytes
        || fault_after_control.error != fault_stats.error
        || write_attempts.load(Ordering::SeqCst) != 2
        || successful_bytes.load(Ordering::SeqCst) != PIECE_LENGTH
    {
        return Err(io::Error::other(format!(
            "faulted torrent changed while the control completed: before={fault_stats:?}, after={fault_after_control:?}, write_attempts={}, successful_bytes={}",
            write_attempts.load(Ordering::SeqCst),
            successful_bytes.load(Ordering::SeqCst)
        ))
        .into());
    }

    stop_with_deadline(downloader, "downloader").await?;
    stop_with_deadline(seeder, "seeder").await?;
    println!(
        "bittorrent_storage_full injected_fault_kind=StorageFull successful_writes=1 filesystem_accepted_bytes={} write_attempts={} fault_phase={:?} fault_progress_bytes={} fault_total_bytes={} fault_ms={} stats_ms={} control_progress_at_fault={} control_bytes={} control_ms={}",
        successful_bytes.load(Ordering::SeqCst),
        write_attempts.load(Ordering::SeqCst),
        fault_stats.phase,
        fault_stats.progress_bytes,
        fault_stats.total_bytes,
        fault_elapsed.as_millis(),
        stats_elapsed.as_millis(),
        control_at_fault.progress_bytes,
        control_payload.len(),
        control_elapsed.as_millis()
    );
    Ok(())
}
