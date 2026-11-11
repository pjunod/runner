//! Read/write throughput for each storage filesystem, for the dashboard.
//!
//! The capacity probe (`volumes`) already groups the configured paths onto
//! filesystems and records each one's `st_dev`. This module turns that
//! device number into a pair of byte counters once a second and reports the
//! rate between samples:
//!
//! * a block device (ext4, xfs, a bind mount of one in a container) is
//!   found in `/proc/diskstats` by major:minor, falling back to the mount
//!   source's device name for filesystems such as btrfs whose `st_dev` is
//!   an anonymous number. These counters are device-wide: they include
//!   every program using that disk, not only Runner.
//! * an NFS mount has no diskstats row. `/proc/self/mountstats` carries the
//!   bytes this machine actually sent to and received from the server for
//!   that mount, which is the traffic Runner's host puts on the NAS. Other
//!   clients of the same NAS are invisible from here.
//! * anything else (GlusterFS and other FUSE mounts, SMB/CIFS, overlay
//!   without a backing device, or a platform without procfs) has no kernel
//!   counters, so it reports what Runner itself moved there, from
//!   `nzbd_state::io_meter`: article writes, par2 and unpack tools, the
//!   copy to the completed folder, and torrent transfer. Other programs
//!   using that filesystem are not visible, and the source says so.
//!
//! Only procfs is read. Nothing here touches the storage itself, so a wedged
//! network mount cannot stall a sample; the sampler still runs on its own
//! thread so that the owner loop and the API never wait on it.

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;

use crate::volumes::DiskGuardReading;

/// How often the sampler reads the counters.
pub const SAMPLE_EVERY: Duration = Duration::from_secs(1);

/// A sample older than this is not shown as a current rate. A sampler that
/// stops publishing is reported as such instead of freezing the graph.
pub const STALE_AFTER: Duration = Duration::from_secs(5);

/// Where a filesystem's counters come from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IoSource {
    /// `/proc/diskstats`; `device` is the kernel name, e.g. `nvme0n1p2`.
    Block { device: String },
    /// `/proc/self/mountstats`; `export` is the mount source, e.g.
    /// `nas:/share`.
    Nfs { export: String },
    /// The kernel keeps no counters for this filesystem; the numbers are
    /// Runner's own I/O there. `fstype` names the filesystem type (or the
    /// reason, e.g. a platform without procfs).
    Runner { fstype: String },
}

impl IoSource {
    /// Stable machine name for the API.
    pub fn kind(&self) -> &'static str {
        match self {
            IoSource::Block { .. } => "block",
            IoSource::Nfs { .. } => "nfs",
            IoSource::Runner { .. } => "runner",
        }
    }

    pub fn detail(&self) -> &str {
        match self {
            IoSource::Block { device } => device,
            IoSource::Nfs { export } => export,
            IoSource::Runner { fstype } => fstype,
        }
    }
}

/// Cumulative bytes moved, as the kernel counts them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoCounters {
    pub read_bytes: u64,
    pub write_bytes: u64,
}

/// One filesystem's latest throughput.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeIo {
    pub device_id: u64,
    pub source: IoSource,
    /// Bytes per second since the previous sample. `None` on the first
    /// sample for a device, after a counter reset, or when its source changed.
    pub read_bps: Option<u64>,
    pub write_bps: Option<u64>,
}

/// Everything one sampler pass measured.
#[derive(Debug, Clone)]
pub struct DiskIoReading {
    pub at: Instant,
    pub volumes: Vec<VolumeIo>,
}

impl Default for DiskIoReading {
    fn default() -> Self {
        DiskIoReading {
            at: Instant::now(),
            volumes: Vec::new(),
        }
    }
}

impl DiskIoReading {
    pub fn for_device(&self, device_id: Option<u64>) -> Option<&VolumeIo> {
        let device_id = device_id?;
        self.volumes.iter().find(|io| io.device_id == device_id)
    }

    pub fn is_stale(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.at) > STALE_AFTER
    }
}

/// The procfs text one pass works from. Tests feed fixtures through it.
#[derive(Debug, Default, Clone)]
pub struct ProcText {
    pub mountinfo: String,
    pub diskstats: String,
    pub mountstats: String,
}

impl ProcText {
    #[cfg(target_os = "linux")]
    fn read() -> Option<ProcText> {
        let read = |path: &str| std::fs::read_to_string(path).ok();
        Some(ProcText {
            mountinfo: read("/proc/self/mountinfo")?,
            diskstats: read("/proc/diskstats").unwrap_or_default(),
            mountstats: read("/proc/self/mountstats").unwrap_or_default(),
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn read() -> Option<ProcText> {
        None
    }
}

/// Holds the previous counters so each pass can report a rate.
#[derive(Default)]
pub struct DiskIoSampler {
    previous: HashMap<u64, (Instant, IoSource, IoCounters)>,
}

impl DiskIoSampler {
    /// Sample the live system.
    pub fn sample(&mut self, devices: &[u64], now: Instant) -> DiskIoReading {
        let text = ProcText::read().unwrap_or_else(|| ProcText {
            mountinfo: String::new(),
            diskstats: String::new(),
            mountstats: String::new(),
        });
        self.sample_from(devices, now, &text, &canonical_device_name, &runner_totals)
    }

    /// Sample from the given procfs text. `device_name` resolves a mount
    /// source such as `/dev/mapper/vg-data` to its kernel name (`dm-0`).
    pub fn sample_from(
        &mut self,
        devices: &[u64],
        now: Instant,
        text: &ProcText,
        device_name: &dyn Fn(&str) -> Option<String>,
        runner: &dyn Fn(u64) -> IoCounters,
    ) -> DiskIoReading {
        let mounts = parse_mountinfo(&text.mountinfo);
        let disks = parse_diskstats(&text.diskstats);
        let nfs = parse_mountstats(&text.mountstats);
        let mut volumes = Vec::with_capacity(devices.len());
        for &device_id in devices {
            if volumes
                .iter()
                .any(|io: &VolumeIo| io.device_id == device_id)
            {
                continue;
            }
            // No kernel counters: fall back to what Runner moved there.
            let (source, counters) = match resolve(device_id, &mounts, &disks, &nfs, device_name) {
                (source, Some(counters)) => (source, counters),
                (source, None) => (source, runner(device_id)),
            };
            let rate = self
                .previous
                .get(&device_id)
                .and_then(|(at, prev_source, prev)| {
                    (prev_source == &source).then(|| rate_between(*at, *prev, now, counters))
                })
                .flatten();
            self.previous
                .insert(device_id, (now, source.clone(), counters));
            let (read_bps, write_bps) = rate.map_or((None, None), |(r, w)| (Some(r), Some(w)));
            volumes.push(VolumeIo {
                device_id,
                source,
                read_bps,
                write_bps,
            });
        }
        self.previous.retain(|id, _| devices.contains(id));
        DiskIoReading { at: now, volumes }
    }
}

fn runner_totals(device: u64) -> IoCounters {
    let totals = nzbd_state::io_meter::totals(device);
    IoCounters {
        read_bytes: totals.read_bytes,
        write_bytes: totals.write_bytes,
    }
}

fn no_kernel_counters() -> &'static str {
    if cfg!(target_os = "linux") {
        "no matching mount"
    } else {
        "no kernel counters on this platform"
    }
}

fn rate_between(
    then: Instant,
    before: IoCounters,
    now: Instant,
    after: IoCounters,
) -> Option<(u64, u64)> {
    let secs = now.saturating_duration_since(then).as_secs_f64();
    if secs < 0.2 {
        return None;
    }
    // A counter that went backwards means the device was replaced or the
    // filesystem remounted; there is no honest rate across that.
    let read = after.read_bytes.checked_sub(before.read_bytes)?;
    let write = after.write_bytes.checked_sub(before.write_bytes)?;
    Some(((read as f64 / secs) as u64, (write as f64 / secs) as u64))
}

/// Linux `dev_t` to (major, minor), matching glibc's `gnu_dev_major/minor`.
pub fn split_dev(dev: u64) -> (u64, u64) {
    let major = ((dev >> 8) & 0xfff) | ((dev >> 32) & 0xffff_f000);
    let minor = (dev & 0xff) | ((dev >> 12) & 0xffff_ff00);
    (major, minor)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Mount {
    mount_point: String,
    fstype: String,
    source: String,
}

/// `(major, minor)` → every mount of that device, in mountinfo order.
fn parse_mountinfo(text: &str) -> HashMap<(u64, u64), Vec<Mount>> {
    let mut mounts: HashMap<(u64, u64), Vec<Mount>> = HashMap::new();
    for line in text.lines() {
        let Some((before, after)) = line.split_once(" - ") else {
            continue;
        };
        let fields: Vec<&str> = before.split(' ').collect();
        let tail: Vec<&str> = after.split(' ').collect();
        if fields.len() < 5 || tail.len() < 2 {
            continue;
        }
        let Some((major, minor)) = fields[2].split_once(':') else {
            continue;
        };
        let (Ok(major), Ok(minor)) = (major.parse(), minor.parse()) else {
            continue;
        };
        mounts.entry((major, minor)).or_default().push(Mount {
            mount_point: fields[4].to_owned(),
            fstype: tail[0].to_owned(),
            source: tail[1].to_owned(),
        });
    }
    mounts
}

#[derive(Debug, Default)]
struct DiskStats {
    by_number: HashMap<(u64, u64), (String, IoCounters)>,
    by_name: HashMap<String, IoCounters>,
}

/// `/proc/diskstats`: `major minor name reads merged sectors_read ms
/// writes merged sectors_written ...`. Sectors are always 512 bytes here,
/// whatever the device's own sector size.
fn parse_diskstats(text: &str) -> DiskStats {
    let mut stats = DiskStats::default();
    for line in text.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 10 {
            continue;
        }
        let parse = |i: usize| fields[i].parse::<u64>().ok();
        let (Some(major), Some(minor), Some(read), Some(written)) =
            (parse(0), parse(1), parse(5), parse(9))
        else {
            continue;
        };
        let counters = IoCounters {
            read_bytes: read.saturating_mul(512),
            write_bytes: written.saturating_mul(512),
        };
        let name = fields[2].to_owned();
        stats.by_name.insert(name.clone(), counters);
        stats.by_number.insert((major, minor), (name, counters));
    }
    stats
}

/// `/proc/self/mountstats` NFS stanzas: mount point → (export, counters).
/// The `bytes:` line is `normal_read normal_write direct_read direct_write
/// server_read server_write read_pages write_pages`; the server columns
/// are what crossed the wire, cache hits and direct I/O accounted for.
fn parse_mountstats(text: &str) -> HashMap<String, (String, IoCounters)> {
    let mut out = HashMap::new();
    let mut current: Option<(String, String)> = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("device ") {
            current = None;
            let Some((export, rest)) = rest.split_once(" mounted on ") else {
                continue;
            };
            let Some((mount_point, rest)) = rest.split_once(" with fstype ") else {
                continue;
            };
            if rest.starts_with("nfs") {
                current = Some((mount_point.to_owned(), export.to_owned()));
            }
            continue;
        }
        let Some((mount_point, export)) = &current else {
            continue;
        };
        let trimmed = line.trim_start();
        let Some(numbers) = trimmed.strip_prefix("bytes:") else {
            continue;
        };
        let values: Vec<u64> = numbers
            .split_whitespace()
            .filter_map(|value| value.parse().ok())
            .collect();
        if values.len() >= 6 {
            out.insert(
                mount_point.clone(),
                (
                    export.clone(),
                    IoCounters {
                        read_bytes: values[4],
                        write_bytes: values[5],
                    },
                ),
            );
        }
    }
    out
}

fn resolve(
    device_id: u64,
    mounts: &HashMap<(u64, u64), Vec<Mount>>,
    disks: &DiskStats,
    nfs: &HashMap<String, (String, IoCounters)>,
    device_name: &dyn Fn(&str) -> Option<String>,
) -> (IoSource, Option<IoCounters>) {
    let number = split_dev(device_id);
    let mounts = mounts.get(&number).map(Vec::as_slice).unwrap_or(&[]);
    if let Some(mount) = mounts.iter().find(|mount| mount.fstype.starts_with("nfs")) {
        if let Some((export, counters)) =
            mounts.iter().find_map(|mount| nfs.get(&mount.mount_point))
        {
            return (
                IoSource::Nfs {
                    export: export.clone(),
                },
                Some(*counters),
            );
        }
        return (
            IoSource::Runner {
                fstype: format!("{} without mountstats", mount.fstype),
            },
            None,
        );
    }
    if let Some((name, counters)) = disks.by_number.get(&number) {
        return (
            IoSource::Block {
                device: name.clone(),
            },
            Some(*counters),
        );
    }
    // btrfs, and anything else that reports an anonymous st_dev, still
    // names its backing device as the mount source.
    for mount in mounts {
        if !mount.source.starts_with("/dev/") {
            continue;
        }
        if let Some(name) = device_name(&mount.source) {
            if let Some(counters) = disks.by_name.get(&name) {
                return (IoSource::Block { device: name }, Some(*counters));
            }
        }
    }
    let fstype = mounts
        .first()
        .map(|mount| mount.fstype.clone())
        .unwrap_or_else(|| no_kernel_counters().to_owned());
    (IoSource::Runner { fstype }, None)
}

/// `/dev/mapper/vg-data` → `dm-0`; `/dev/sdb1` → `sdb1`.
fn canonical_device_name(source: &str) -> Option<String> {
    let path = std::fs::canonicalize(source).unwrap_or_else(|_| source.into());
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
}

/// Run the sampler on its own thread until `cancel` fires or the owner that
/// holds `out` is gone. The device list follows the capacity probe, so a
/// config reload that changes the roots is picked up on the next pass.
pub fn spawn_sampler(
    guard: Arc<ArcSwap<DiskGuardReading>>,
    out: &Arc<ArcSwap<DiskIoReading>>,
    cancel: tokio_util::sync::CancellationToken,
) {
    let out: Weak<ArcSwap<DiskIoReading>> = Arc::downgrade(out);
    let spawned = std::thread::Builder::new()
        .name("nzbd-disk-io".into())
        .spawn(move || {
            let mut sampler = DiskIoSampler::default();
            while !cancel.is_cancelled() {
                let Some(out) = out.upgrade() else {
                    break;
                };
                let mut devices: Vec<u64> = guard
                    .load()
                    .volumes
                    .iter()
                    .filter_map(|volume| volume.device_id)
                    .collect();
                devices.dedup();
                out.store(Arc::new(sampler.sample(&devices, Instant::now())));
                drop(out);
                std::thread::sleep(SAMPLE_EVERY);
            }
        });
    if let Err(error) = spawned {
        tracing::warn!(%error, "could not start the storage I/O sampler; the dashboard will show no I/O");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MOUNTINFO: &str = "\
22 1 259:2 / / rw,relatime shared:1 - ext4 /dev/nvme0n1p2 rw
40 22 0:52 /export /data/downloads rw,relatime shared:30 - nfs4 nas:/share rw,vers=4.2
41 22 0:52 /export /mnt/alias rw,relatime shared:30 - nfs4 nas:/share rw,vers=4.2
42 22 0:60 / /srv/btrfs rw,relatime shared:31 - btrfs /dev/mapper/vg-data rw
43 22 0:61 / /mnt/smb rw,relatime shared:32 - cifs //nas/media rw
44 22 0:62 / /mnt/nfs-nostats rw,relatime shared:33 - nfs 10.0.0.2:/x rw
";

    fn diskstats(nvme_read: u64, nvme_written: u64) -> String {
        format!(
            "259       0 nvme0n1 100 0 9000 10 200 0 9000 10 0 0 0\n\
             259       2 nvme0n1p2 100 0 {nvme_read} 10 200 0 {nvme_written} 10 0 0 0\n\
             253       0 dm-0 1 0 2048 1 1 0 4096 1 0 0 0\n"
        )
    }

    fn mountstats(read: u64, write: u64) -> String {
        format!(
            "device /dev/nvme0n1p2 mounted on / with fstype ext4\n\
             device nas:/share mounted on /mnt/alias with fstype nfs4 statvers=1.1\n\
             \topts:\trw,vers=4.2\n\
             \tbytes:\t111 222 0 0 {read} {write} 9 9\n\
             device //nas/media mounted on /mnt/smb with fstype cifs\n"
        )
    }

    fn text(nvme: (u64, u64), nfs: (u64, u64)) -> ProcText {
        ProcText {
            mountinfo: MOUNTINFO.to_owned(),
            diskstats: diskstats(nvme.0, nvme.1),
            mountstats: mountstats(nfs.0, nfs.1),
        }
    }

    fn dev(major: u64, minor: u64) -> u64 {
        // Inverse of split_dev (glibc makedev).
        ((major & 0xfff) << 8)
            | ((major & 0xffff_f000) << 32)
            | (minor & 0xff)
            | ((minor & 0xffff_ff00) << 12)
    }

    fn no_runner(_: u64) -> IoCounters {
        IoCounters::default()
    }

    fn names(source: &str) -> Option<String> {
        match source {
            "/dev/mapper/vg-data" => Some("dm-0".to_owned()),
            other => other.rsplit('/').next().map(str::to_owned),
        }
    }

    #[test]
    fn dev_numbers_split_like_glibc() {
        assert_eq!(split_dev(dev(259, 2)), (259, 2));
        assert_eq!(split_dev(dev(8, 1)), (8, 1));
        assert_eq!(split_dev(dev(0, 52)), (0, 52));
        assert_eq!(split_dev(dev(4100, 70_000)), (4100, 70_000));
    }

    #[test]
    fn first_sample_names_the_source_and_reports_no_rate_yet() {
        let mut sampler = DiskIoSampler::default();
        let now = Instant::now();
        let reading = sampler.sample_from(
            &[dev(259, 2)],
            now,
            &text((0, 0), (0, 0)),
            &names,
            &no_runner,
        );
        let io = reading.for_device(Some(dev(259, 2))).unwrap();
        assert_eq!(
            io.source,
            IoSource::Block {
                device: "nvme0n1p2".into()
            }
        );
        assert_eq!((io.read_bps, io.write_bps), (None, None));
    }

    #[test]
    fn block_rates_are_sector_deltas_over_elapsed_time() {
        let mut sampler = DiskIoSampler::default();
        let t0 = Instant::now();
        let id = dev(259, 2);
        sampler.sample_from(&[id], t0, &text((1_000, 2_000), (0, 0)), &names, &no_runner);
        // 2048 sectors read and 4096 written over two seconds.
        let reading = sampler.sample_from(
            &[id],
            t0 + Duration::from_secs(2),
            &text((3_048, 6_096), (0, 0)),
            &names,
            &no_runner,
        );
        let io = reading.for_device(Some(id)).unwrap();
        assert_eq!(io.read_bps, Some(2048 * 512 / 2));
        assert_eq!(io.write_bps, Some(4096 * 512 / 2));
    }

    #[test]
    fn nfs_uses_the_server_byte_columns_through_any_bind_of_the_mount() {
        let mut sampler = DiskIoSampler::default();
        let t0 = Instant::now();
        let id = dev(0, 52);
        sampler.sample_from(
            &[id],
            t0,
            &text((0, 0), (10_000, 20_000)),
            &names,
            &no_runner,
        );
        let reading = sampler.sample_from(
            &[id],
            t0 + Duration::from_secs(1),
            &text((0, 0), (10_500, 21_000)),
            &names,
            &no_runner,
        );
        let io = reading.for_device(Some(id)).unwrap();
        assert_eq!(
            io.source,
            IoSource::Nfs {
                export: "nas:/share".into()
            }
        );
        assert_eq!((io.read_bps, io.write_bps), (Some(500), Some(1_000)));
    }

    #[test]
    fn an_anonymous_device_falls_back_to_the_mount_source() {
        let mut sampler = DiskIoSampler::default();
        let reading = sampler.sample_from(
            &[dev(0, 60)],
            Instant::now(),
            &text((0, 0), (0, 0)),
            &names,
            &no_runner,
        );
        assert_eq!(
            reading.volumes[0].source,
            IoSource::Block {
                device: "dm-0".into()
            }
        );
    }

    #[test]
    fn filesystems_without_kernel_counters_report_runners_own_io() {
        let mut sampler = DiskIoSampler::default();
        let t0 = Instant::now();
        let ids = [dev(0, 61), dev(0, 62), dev(0, 99)];
        let first = |_: u64| IoCounters {
            read_bytes: 1_000,
            write_bytes: 5_000,
        };
        let reading = sampler.sample_from(&ids, t0, &text((0, 0), (0, 0)), &names, &first);
        let fstypes: Vec<&str> = reading
            .volumes
            .iter()
            .map(|io| io.source.detail())
            .collect();
        assert_eq!(
            fstypes,
            ["cifs", "nfs without mountstats", "no matching mount"]
        );
        assert!(reading
            .volumes
            .iter()
            .all(|io| io.source.kind() == "runner" && io.read_bps.is_none()));
        let later = |_: u64| IoCounters {
            read_bytes: 3_000,
            write_bytes: 9_000,
        };
        let reading = sampler.sample_from(
            &ids,
            t0 + Duration::from_secs(2),
            &text((0, 0), (0, 0)),
            &names,
            &later,
        );
        assert!(reading
            .volumes
            .iter()
            .all(|io| io.read_bps == Some(1_000) && io.write_bps == Some(2_000)));
    }

    #[test]
    fn a_counter_that_goes_backwards_reports_no_rate() {
        let mut sampler = DiskIoSampler::default();
        let t0 = Instant::now();
        let id = dev(259, 2);
        sampler.sample_from(&[id], t0, &text((5_000, 5_000), (0, 0)), &names, &no_runner);
        let reset = sampler.sample_from(
            &[id],
            t0 + Duration::from_secs(1),
            &text((10, 10), (0, 0)),
            &names,
            &no_runner,
        );
        assert_eq!(reset.volumes[0].read_bps, None);
        // The next sample measures from the reset value.
        let after = sampler.sample_from(
            &[id],
            t0 + Duration::from_secs(2),
            &text((12, 14), (0, 0)),
            &names,
            &no_runner,
        );
        assert_eq!(after.volumes[0].read_bps, Some(1024));
        assert_eq!(after.volumes[0].write_bps, Some(2048));
    }

    #[test]
    fn devices_that_leave_the_config_are_forgotten() {
        let mut sampler = DiskIoSampler::default();
        let t0 = Instant::now();
        let id = dev(259, 2);
        sampler.sample_from(&[id], t0, &text((0, 0), (0, 0)), &names, &no_runner);
        sampler.sample_from(
            &[],
            t0 + Duration::from_secs(1),
            &text((0, 0), (0, 0)),
            &names,
            &no_runner,
        );
        let back = sampler.sample_from(
            &[id],
            t0 + Duration::from_secs(2),
            &text((100, 100), (0, 0)),
            &names,
            &no_runner,
        );
        assert_eq!(
            back.volumes[0].read_bps, None,
            "no rate across a gap in the config"
        );
    }

    #[test]
    fn staleness_is_measured_from_the_sample_time() {
        let t0 = Instant::now();
        let reading = DiskIoReading {
            at: t0,
            volumes: Vec::new(),
        };
        assert!(!reading.is_stale(t0 + Duration::from_secs(4)));
        assert!(reading.is_stale(t0 + Duration::from_secs(6)));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn the_live_root_filesystem_resolves_to_something() {
        use std::os::unix::fs::MetadataExt;
        let id = std::fs::metadata("/").unwrap().dev();
        let mut sampler = DiskIoSampler::default();
        let reading = sampler.sample(&[id], Instant::now());
        assert_eq!(reading.volumes.len(), 1);
        assert!(!reading.volumes[0].source.detail().is_empty());
    }
}
