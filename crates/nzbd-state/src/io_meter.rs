//! Bytes Runner itself reads and writes, per filesystem.
//!
//! The dashboard graphs each storage filesystem's throughput from the
//! kernel's counters where they exist (block devices, NFS). A FUSE mount
//! such as GlusterFS, an SMB share, or an overlay with no backing device
//! has none, so for those the graph falls back to what Runner moved there
//! itself. That is most of the traffic on a processing volume, and it is
//! labelled as Runner's own so nobody reads it as the whole device.
//!
//! Every caller that moves payload bytes reports them here against the
//! filesystem's `st_dev`, which is the key the capacity probe and the I/O
//! sampler already use:
//!
//! * the article writer (decoded segments into the job directory),
//! * external tools (par2, 7z, unrar), from their `/proc/<pid>/io` while
//!   they run, against the directory they run in,
//! * the copy that publishes a finished job across filesystems,
//! * the PAR fingerprint pass, and
//! * BitTorrent payload transfer (downloaded bytes are written, uploaded
//!   bytes are read).
//!
//! Counting is a mutex-guarded map update: a few hundred calls a second at
//! full download speed, far below where the lock would matter.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

/// Cumulative bytes Runner has read from and written to one filesystem.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Totals {
    pub read_bytes: u64,
    pub write_bytes: u64,
}

static METER: LazyLock<Mutex<HashMap<u64, Totals>>> = LazyLock::new(Default::default);

fn add(device: u64, read: u64, write: u64) {
    if read == 0 && write == 0 {
        return;
    }
    let mut meter = METER
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let totals = meter.entry(device).or_default();
    totals.read_bytes = totals.read_bytes.wrapping_add(read);
    totals.write_bytes = totals.write_bytes.wrapping_add(write);
}

/// Record bytes Runner read from the filesystem `device`.
pub fn read(device: Option<u64>, bytes: u64) {
    if let Some(device) = device {
        add(device, bytes, 0);
    }
}

/// Record bytes Runner wrote to the filesystem `device`.
pub fn write(device: Option<u64>, bytes: u64) {
    if let Some(device) = device {
        add(device, 0, bytes);
    }
}

/// Cumulative totals for `device`; zero when Runner has moved nothing there.
pub fn totals(device: u64) -> Totals {
    METER
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&device)
        .copied()
        .unwrap_or_default()
}

/// The filesystem identity of already-fetched metadata.
#[cfg(unix)]
pub fn device_of_metadata(metadata: &std::fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(metadata.dev())
}

#[cfg(not(unix))]
pub fn device_of_metadata(_metadata: &std::fs::Metadata) -> Option<u64> {
    None
}

/// The filesystem holding an open file.
pub fn device_of_file(file: &std::fs::File) -> Option<u64> {
    file.metadata().ok().as_ref().and_then(device_of_metadata)
}

/// The filesystem holding `path`. This stats the path, so callers on a hot
/// loop look it up once and keep it.
pub fn device_of(path: &std::path::Path) -> Option<u64> {
    std::fs::metadata(path)
        .ok()
        .as_ref()
        .and_then(device_of_metadata)
}

/// A file whose reads are charged to its filesystem. Wrap the handle of a
/// pass that reads payload (hashing, CRC checks, readbacks); it derefs to
/// the `File` for metadata and the like, and every `read` (including
/// `read_exact` and `read_to_end`) is counted.
pub struct Metered {
    file: std::fs::File,
    device: Option<u64>,
}

impl Metered {
    pub fn new(file: std::fs::File) -> Self {
        let device = device_of_file(&file);
        Metered { file, device }
    }

    pub fn into_inner(self) -> std::fs::File {
        self.file
    }
}

impl std::io::Read for Metered {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.file.read(buf)?;
        read(self.device, n as u64);
        Ok(n)
    }
}

impl std::io::Seek for Metered {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

impl std::ops::Deref for Metered {
    type Target = std::fs::File;
    fn deref(&self) -> &std::fs::File {
        &self.file
    }
}

impl std::ops::DerefMut for Metered {
    fn deref_mut(&mut self) -> &mut std::fs::File {
        &mut self.file
    }
}

/// One tool's cumulative I/O as `/proc/<pid>/io` counts it: `rchar` and
/// `wchar`, every byte through read/write syscalls. These are used rather
/// than `read_bytes`/`write_bytes` because those count only block-layer
/// traffic and stay at zero on FUSE and network filesystems, which is the
/// only place this meter is shown.
#[cfg(target_os = "linux")]
pub fn process_io(pid: u32) -> Option<Totals> {
    let text = std::fs::read_to_string(format!("/proc/{pid}/io")).ok()?;
    parse_proc_io(&text)
}

#[cfg(not(target_os = "linux"))]
pub fn process_io(_pid: u32) -> Option<Totals> {
    None
}

/// Whether our child `pid` has exited, observed without reaping it so its
/// `/proc/<pid>/io` can still be read for a final sample. An error means
/// this cannot be observed here; the caller must fall back to an ordinary
/// wait rather than assume the child is gone (that would stop sampling a
/// tool that is still running).
#[cfg(target_os = "linux")]
pub fn exited_unreaped(pid: u32) -> std::io::Result<bool> {
    loop {
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let rc = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOWAIT | libc::WNOHANG,
            )
        };
        if rc == 0 {
            return Ok(unsafe { info.si_pid() } != 0);
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

pub fn parse_proc_io(text: &str) -> Option<Totals> {
    let field = |name: &str| {
        text.lines()
            .find_map(|line| line.strip_prefix(name)?.trim().parse::<u64>().ok())
    };
    Some(Totals {
        read_bytes: field("rchar:")?,
        write_bytes: field("wchar:")?,
    })
}

/// Follows one child process and charges its I/O to `device` as it runs.
/// Call [`ProcessMeter::sample`] periodically; each call charges only what
/// is new since the last. An exited child's counters stay readable until it
/// is reaped, so a caller that can sample between exit and reap loses
/// nothing; one that cannot loses at most one sampling interval.
pub struct ProcessMeter {
    pid: u32,
    device: Option<u64>,
    seen: Totals,
}

impl ProcessMeter {
    pub fn new(pid: u32, device: Option<u64>) -> Self {
        ProcessMeter {
            pid,
            device,
            seen: Totals::default(),
        }
    }

    pub fn sample(&mut self) {
        let Some(now) = process_io(self.pid) else {
            return;
        };
        let read = now.read_bytes.saturating_sub(self.seen.read_bytes);
        let write = now.write_bytes.saturating_sub(self.seen.write_bytes);
        self.seen = now;
        if let Some(device) = self.device {
            add(device, read, write);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn totals_accumulate_per_device() {
        // A device number no real filesystem in the test run uses.
        let device = u64::MAX - 7;
        let before = totals(device);
        read(Some(device), 100);
        write(Some(device), 40);
        write(None, 999);
        read(Some(device), 0);
        let after = totals(device);
        assert_eq!(after.read_bytes - before.read_bytes, 100);
        assert_eq!(after.write_bytes - before.write_bytes, 40);
        assert_eq!(totals(u64::MAX - 8), Totals::default());
    }

    #[test]
    fn a_metered_file_charges_every_read_to_its_filesystem() {
        use std::io::{Read, Seek, SeekFrom};
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("payload");
        std::fs::write(&path, vec![1u8; 10_000]).unwrap();
        let device = device_of(&path).unwrap();
        let before = totals(device).read_bytes;
        let mut file = Metered::new(std::fs::File::open(&path).unwrap());
        assert_eq!(file.metadata().unwrap().len(), 10_000, "derefs to the File");
        let mut head = [0u8; 100];
        file.read_exact(&mut head).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut all = Vec::new();
        file.read_to_end(&mut all).unwrap();
        // Other tests may read from the same filesystem concurrently.
        assert!(totals(device).read_bytes - before >= 10_100);
    }

    #[test]
    fn proc_io_reads_the_syscall_counters() {
        let text = "rchar: 4096\nwchar: 512\nsyscr: 3\nsyscw: 2\nread_bytes: 0\nwrite_bytes: 0\n";
        assert_eq!(
            parse_proc_io(text),
            Some(Totals {
                read_bytes: 4096,
                write_bytes: 512
            })
        );
        assert_eq!(parse_proc_io("syscr: 3\n"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_running_child_is_charged_as_it_works() {
        use std::io::Write;
        let tmp = tempfile::tempdir().unwrap();
        let device = u64::MAX - 9;
        let before = totals(device);
        // tee copies its stdin to out.bin and stays alive until stdin
        // closes, so the samples below never race its exit.
        let mut child = std::process::Command::new("tee")
            .arg("out.bin")
            .current_dir(tmp.path())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let pid = child.id();
        let mut meter = ProcessMeter::new(pid, Some(device));
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(&vec![5u8; 256 * 1024]).unwrap();
        stdin.flush().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            meter.sample();
            let seen = totals(device);
            if seen.read_bytes - before.read_bytes >= 256 * 1024
                && seen.write_bytes - before.write_bytes >= 256 * 1024
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "tee's I/O never reached the meter: {:?} (proc io {:?})",
                seen,
                process_io(pid)
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let charged = totals(device);
        meter.sample();
        assert_eq!(
            totals(device),
            charged,
            "a repeat sample charges nothing new"
        );
        drop(stdin);
        child.wait().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn an_exited_child_is_seen_before_it_is_reaped() {
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            match exited_unreaped(pid) {
                Ok(true) => break,
                Ok(false) => {}
                Err(error) => panic!("waitid(WNOWAIT) is unavailable here: {error}"),
            }
            assert!(std::time::Instant::now() < deadline, "true did not exit");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(
            process_io(pid).is_some(),
            "an exited, unreaped child's counters stay readable"
        );
        child.wait().unwrap();
        assert!(process_io(pid).is_none() || pid == std::process::id());
    }
}
