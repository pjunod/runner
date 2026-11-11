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
/// Call [`ProcessMeter::sample`] periodically and once more just before the
/// child is reaped; each call charges only what is new since the last.
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
    fn a_child_process_is_charged_to_its_directory() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("in.bin"), vec![7u8; 256 * 1024]).unwrap();
        let device = u64::MAX - 9;
        let before = totals(device);
        let mut child = std::process::Command::new("dd")
            .args(["if=in.bin", "of=out.bin", "bs=65536", "status=none"])
            .current_dir(tmp.path())
            .spawn()
            .unwrap();
        let mut meter = ProcessMeter::new(child.id(), Some(device));
        // An exited child that has not been reaped still has its counters.
        std::thread::sleep(std::time::Duration::from_millis(300));
        meter.sample();
        meter.sample();
        child.wait().unwrap();
        meter.sample();
        let after = totals(device);
        assert!(
            after.read_bytes - before.read_bytes >= 256 * 1024,
            "{after:?}"
        );
        assert!(
            after.write_bytes - before.write_bytes >= 256 * 1024,
            "{after:?}"
        );
        assert!(
            after.read_bytes - before.read_bytes < 512 * 1024,
            "repeat samples charge only what is new: {after:?}"
        );
    }
}
