//! Per-server download volume accounting + quotas (NZBGet `DailyQuota` /
//! `MonthlyQuota` / `QuotaStartDay`, per-server volume counters for the
//! `servervolumes` RPC).
//!
//! Windows roll on UTC civil dates; the monthly window starts on
//! `quota_start_day` of each month. Counters persist per node
//! (`volumes.<suffix>.json`) so cluster peers on the shared volume can be
//! summed for an account-wide quota decision without write contention.

use nzbd_types::ServerId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Synthetic source row for verified incoming BitTorrent payload. It shares
/// the quota book without pretending a peer swarm is an NNTP provider.
pub const TORRENT_SOURCE_ID: ServerId = ServerId(u32::MAX);

/// Days → (year, month, day) — Howard Hinnant's civil-from-days.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// The monthly-quota period key for a unix timestamp: periods begin on
/// `start_day` of each month (clamped to 28 for short months).
fn month_key(unix: i64, start_day: u32) -> i64 {
    let days = unix.div_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let start = start_day.clamp(1, 28);
    let (py, pm) = if d >= start {
        (y, m)
    } else if m == 1 {
        (y - 1, 12)
    } else {
        (y, m - 1)
    };
    py * 12 + pm as i64
}

fn day_key(unix: i64) -> i64 {
    unix.div_euclid(86_400)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VolumeWindow {
    pub total_bytes: u64,
    pub day_key: i64,
    pub day_bytes: u64,
    pub month_key: i64,
    pub month_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VolumeDoc {
    /// server id → window
    pub servers: HashMap<u32, VolumeWindow>,
}

impl VolumeDoc {
    pub fn day_total(&self, now_unix: i64) -> u64 {
        let key = day_key(now_unix);
        self.servers
            .values()
            .filter(|w| w.day_key == key)
            .map(|w| w.day_bytes)
            .sum()
    }

    pub fn month_total(&self, now_unix: i64, start_day: u32) -> u64 {
        let key = month_key(now_unix, start_day);
        self.servers
            .values()
            .filter(|w| w.month_key == key)
            .map(|w| w.month_bytes)
            .sum()
    }
}

/// This node's live counter book.
#[derive(Clone)]
pub struct VolumeBook {
    doc: VolumeDoc,
    path: PathBuf,
    dir: PathBuf,
    dirty: bool,
    revision: u64,
}

impl VolumeBook {
    pub fn load(state_dir: &Path, suffix: &str) -> VolumeBook {
        let dir = state_dir.to_path_buf();
        let path = dir.join(format!("volumes.{suffix}.json"));
        let doc = std::fs::read(&path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        VolumeBook {
            doc,
            path,
            dir,
            dirty: false,
            revision: 0,
        }
    }

    pub fn add(&mut self, server: ServerId, bytes: u64, now_unix: i64, start_day: u32) {
        let w = self.doc.servers.entry(server.0).or_default();
        let dk = day_key(now_unix);
        let mk = month_key(now_unix, start_day);
        if w.day_key != dk {
            w.day_key = dk;
            w.day_bytes = 0;
        }
        if w.month_key != mk {
            w.month_key = mk;
            w.month_bytes = 0;
        }
        w.total_bytes += bytes;
        w.day_bytes += bytes;
        w.month_bytes += bytes;
        self.dirty = true;
        self.revision = self.revision.wrapping_add(1);
    }

    pub fn doc(&self) -> &VolumeDoc {
        &self.doc
    }

    /// Cluster-aware totals: this node's counters plus every peer's
    /// `volumes.*.json` in the same state dir.
    pub fn cluster_totals(&self, now_unix: i64, start_day: u32) -> (u64, u64) {
        let (day, month) = self.peer_totals(now_unix, start_day);
        (
            day.saturating_add(self.doc.day_total(now_unix)),
            month.saturating_add(self.doc.month_total(now_unix, start_day)),
        )
    }

    pub fn peer_totals(&self, now_unix: i64, start_day: u32) -> (u64, u64) {
        let mut day = 0u64;
        let mut month = 0u64;
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p == self.path {
                    continue;
                }
                let name = p.file_name().unwrap_or_default().to_string_lossy();
                if name.starts_with("volumes.") && name.ends_with(".json") {
                    if let Some(doc) = std::fs::read(&p)
                        .ok()
                        .and_then(|b| serde_json::from_slice::<VolumeDoc>(&b).ok())
                    {
                        day += doc.day_total(now_unix);
                        month += doc.month_total(now_unix, start_day);
                    }
                }
            }
        }
        (day, month)
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }
    pub(crate) fn acknowledge_save(&mut self, revision: u64) {
        if self.revision == revision {
            self.dirty = false;
        }
    }
    pub(crate) fn save_result(&self) -> Result<(), nzbd_state::StateError> {
        if !self.dirty {
            return Ok(());
        }
        let bytes = serde_json::to_vec(&self.doc)?;
        let tmp = self.path.with_extension("tmp");
        let write = (|| {
            use std::io::Write;
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(&tmp, &self.path)?;
            #[cfg(unix)]
            std::fs::File::open(&self.dir)?.sync_all()?;
            Ok::<(), std::io::Error>(())
        })();
        write.map_err(|source| nzbd_state::StateError::Io {
            op: "save volume counters",
            path: self.path.clone(),
            source,
        })
    }

    pub fn save_if_dirty(&mut self) {
        if !self.dirty {
            return;
        }
        if let Ok(bytes) = serde_json::to_vec(&self.doc) {
            let tmp = self.path.with_extension("tmp");
            if std::fs::write(&tmp, &bytes).is_ok() && std::fs::rename(&tmp, &self.path).is_ok() {
                self.dirty = false;
            }
        }
    }
}

/// Capacity visible to the daemon on the filesystem holding a path.
///
/// `available` uses `f_bavail`, not `f_bfree`: reserved blocks are not
/// writable by the unprivileged daemon and therefore are not honest free
/// space for either the queue guard or the dashboard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskSpace {
    pub available: u64,
    pub total: u64,
}

/// One configured write root covered by the enforcing disk guard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskGuardRoot {
    pub label: String,
    pub path: PathBuf,
}

/// The lowest usable free-space reading across all configured write roots.
///
/// `available_bytes = None` means no root could be measured. That state never
/// false-trips the guard, matching the historical one-root behavior, while the
/// absent path makes the lack of evidence visible to callers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiskGuardReading {
    pub available_bytes: Option<u64>,
    pub limiting_label: Option<String>,
    pub limiting_path: Option<PathBuf>,
    /// True only when this probe cycle measured every configured root.
    /// A stale/unknown member may keep an existing hold active, but can
    /// never be used as evidence that a constrained volume recovered.
    pub all_roots_known: bool,
    /// Per-filesystem readings retained so the API and cluster diagnostics
    /// can expose the same facts the enforcing guard used.
    pub volumes: Vec<StorageVolumeReading>,
}

/// One measured filesystem used by one or more configured write roots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageVolumeReading {
    pub label: String,
    pub path: PathBuf,
    pub available_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    /// Filesystem identity where the platform exposes one. Not serialized;
    /// it is used only to collapse configured paths onto failure domains.
    pub device_id: Option<u64>,
    /// Whether this cycle measured the volume. False means the values, when
    /// present, are conservative last-known data retained after a timeout or
    /// inaccessible path.
    pub current: bool,
}

struct StorageMember<'a> {
    root: &'a DiskGuardRoot,
    measured: PathBuf,
}

struct StorageGroup<'a> {
    device: Option<u64>,
    members: Vec<StorageMember<'a>>,
}

/// Stateful, bounded multi-root probe. Each configured root gets an
/// independent deadline, while a process-wide permit pool caps live OS probe
/// threads across hot reloads. One wedged FUSE/Gluster mount cannot delay the
/// daemon or suppress readings from later roots while capacity remains.
#[derive(Default)]
pub struct DiskGuardProbe {
    last_known: HashMap<PathBuf, StorageVolumeReading>,
    in_flight: HashMap<PathBuf, std::sync::Arc<std::sync::atomic::AtomicBool>>,
}

impl DiskGuardProbe {
    pub async fn probe(
        &mut self,
        roots: &[DiskGuardRoot],
        deadline: std::time::Duration,
    ) -> DiskGuardReading {
        self.probe_with(roots, deadline, |root| {
            storage_volume_readings(std::slice::from_ref(&root))
                .into_iter()
                .next()
        })
        .await
    }

    pub(crate) async fn probe_until_cancelled(
        &mut self,
        roots: &[DiskGuardRoot],
        deadline: std::time::Duration,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Option<DiskGuardReading> {
        self.probe_with_until_cancelled(roots, deadline, cancel, |root| {
            storage_volume_readings(std::slice::from_ref(&root))
                .into_iter()
                .next()
        })
        .await
    }

    async fn probe_with_until_cancelled<F>(
        &mut self,
        roots: &[DiskGuardRoot],
        deadline: std::time::Duration,
        cancel: &tokio_util::sync::CancellationToken,
        measure: F,
    ) -> Option<DiskGuardReading>
    where
        F: Fn(DiskGuardRoot) -> Option<StorageVolumeReading> + Send + Sync + 'static,
    {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => None,
            reading = self.probe_with(roots, deadline, measure) => Some(reading),
        }
    }

    async fn probe_with<F>(
        &mut self,
        roots: &[DiskGuardRoot],
        deadline: std::time::Duration,
        measure: F,
    ) -> DiskGuardReading
    where
        F: Fn(DiskGuardRoot) -> Option<StorageVolumeReading> + Send + Sync + 'static,
    {
        use std::sync::atomic::Ordering;

        let mut queued = Vec::with_capacity(roots.len());
        let mut measured = Vec::with_capacity(roots.len());
        let mut flags = HashMap::with_capacity(roots.len());
        for root in roots {
            let flag = self
                .in_flight
                .entry(root.path.clone())
                .or_insert_with(|| shared_in_flight_flag(&root.path))
                .clone();
            if flag.swap(true, Ordering::AcqRel) {
                measured.push((
                    root.path.clone(),
                    unknown_storage_volume(root.label.clone(), root.path.clone()),
                ));
            } else {
                flags.insert(root.path.clone(), flag);
                queued.push(root.clone());
            }
        }

        let flags = std::sync::Arc::new(flags);
        let measure = std::sync::Arc::new(measure);
        measured.extend(
            bounded_root_readings_with(
                &queued,
                deadline,
                flags,
                process_probe_slots(),
                default_probe_spawner(),
                move |root| measure(root),
            )
            .await,
        );
        let mut readings = Vec::with_capacity(roots.len());
        for (path, mut reading) in measured {
            if reading.current {
                self.last_known.insert(path, reading.clone());
            } else if let Some(previous) = self.last_known.get(&path) {
                reading = previous.clone();
                reading.current = false;
            }
            readings.push(reading);
        }
        select_limiting_volume(coalesce_storage_volumes(readings))
    }
}

fn shared_in_flight_flag(path: &Path) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
    static FLAGS: std::sync::OnceLock<
        std::sync::Mutex<HashMap<PathBuf, std::sync::Weak<std::sync::atomic::AtomicBool>>>,
    > = std::sync::OnceLock::new();
    let mut flags = FLAGS
        .get_or_init(|| std::sync::Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    prune_dead_in_flight_flags(&mut flags);
    if let Some(flag) = flags.get(path).and_then(std::sync::Weak::upgrade) {
        return flag;
    }
    let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    flags.insert(path.to_path_buf(), std::sync::Arc::downgrade(&flag));
    flag
}

fn prune_dead_in_flight_flags(
    flags: &mut HashMap<PathBuf, std::sync::Weak<std::sync::atomic::AtomicBool>>,
) {
    flags.retain(|_, flag| flag.strong_count() > 0);
}

struct InFlightReset(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl Drop for InFlightReset {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

/// Process-wide ceiling for detached filesystem syscalls. The config also
/// caps one generation at 64 roots, but hot reload can rotate path names
/// while old kernel calls remain wedged. A permit lives inside the OS thread
/// until that syscall really returns, so reloads cannot grow the detached
/// thread population without bound.
const MAX_PROCESS_PROBE_THREADS: usize = 64;

struct ProbeSlots {
    semaphore: std::sync::Arc<tokio::sync::Semaphore>,
    live: std::sync::atomic::AtomicUsize,
}

impl ProbeSlots {
    fn new(limit: usize) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(limit)),
            live: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    #[cfg(test)]
    fn try_acquire(self: &std::sync::Arc<Self>) -> Option<ProbePermit> {
        use std::sync::atomic::Ordering;
        let permit = self.semaphore.clone().try_acquire_owned().ok()?;
        self.live.fetch_add(1, Ordering::AcqRel);
        Some(ProbePermit {
            permit: Some(permit),
            slots: self.clone(),
        })
    }

    async fn acquire_until(
        self: &std::sync::Arc<Self>,
        deadline: tokio::time::Instant,
    ) -> Option<ProbePermit> {
        use std::sync::atomic::Ordering;
        let permit = tokio::time::timeout_at(deadline, self.semaphore.clone().acquire_owned())
            .await
            .ok()?
            .ok()?;
        self.live.fetch_add(1, Ordering::AcqRel);
        Some(ProbePermit {
            permit: Some(permit),
            slots: self.clone(),
        })
    }
}

struct ProbePermit {
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    slots: std::sync::Arc<ProbeSlots>,
}

impl Drop for ProbePermit {
    fn drop(&mut self) {
        // Release the fair semaphore first, then publish the diagnostic live
        // count. Tests and reload accounting never observe live=0 while the
        // last permit is still unavailable.
        drop(self.permit.take());
        self.slots
            .live
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

fn process_probe_slots() -> std::sync::Arc<ProbeSlots> {
    static SLOTS: std::sync::OnceLock<std::sync::Arc<ProbeSlots>> = std::sync::OnceLock::new();
    SLOTS
        .get_or_init(|| ProbeSlots::new(MAX_PROCESS_PROBE_THREADS))
        .clone()
}

type ProbeWork = Box<dyn FnOnce() + Send + 'static>;
type ProbeSpawner =
    std::sync::Arc<dyn Fn(ProbeWork) -> std::io::Result<()> + Send + Sync + 'static>;

fn default_probe_spawner() -> ProbeSpawner {
    std::sync::Arc::new(|work| {
        std::thread::Builder::new()
            .name("nzbd-disk-probe".into())
            .spawn(work)
            .map(|_| ())
    })
}

fn unknown_storage_volume(label: String, path: PathBuf) -> StorageVolumeReading {
    StorageVolumeReading {
        label,
        path,
        available_bytes: None,
        total_bytes: None,
        device_id: None,
        current: false,
    }
}

#[cfg(test)]
async fn bounded_root_readings<F>(
    roots: &[DiskGuardRoot],
    deadline: std::time::Duration,
    measure: F,
) -> Vec<(PathBuf, StorageVolumeReading)>
where
    F: Fn(DiskGuardRoot) -> Option<StorageVolumeReading> + Send + Sync + 'static,
{
    bounded_root_readings_with(
        roots,
        deadline,
        std::sync::Arc::new(HashMap::new()),
        process_probe_slots(),
        default_probe_spawner(),
        measure,
    )
    .await
}

async fn bounded_root_readings_with<F>(
    roots: &[DiskGuardRoot],
    deadline: std::time::Duration,
    in_flight: std::sync::Arc<HashMap<PathBuf, std::sync::Arc<std::sync::atomic::AtomicBool>>>,
    slots: std::sync::Arc<ProbeSlots>,
    spawn: ProbeSpawner,
    measure: F,
) -> Vec<(PathBuf, StorageVolumeReading)>
where
    F: Fn(DiskGuardRoot) -> Option<StorageVolumeReading> + Send + Sync + 'static,
{
    let measure = std::sync::Arc::new(measure);
    // When earlier hot-reload generations retain permits, the current roots
    // share the capacity that remains. Bound that queue by one response
    // window per configured root, then give each syscall its own full
    // response deadline after admission. This avoids a permanent incomplete
    // cycle merely because responsive roots had to serialize.
    let queue_window = deadline.saturating_mul(u32::try_from(roots.len()).unwrap_or(u32::MAX));
    let slot_deadline = tokio::time::Instant::now() + queue_window;
    let mut tasks = tokio::task::JoinSet::new();
    for root in roots.iter().cloned() {
        let measure = measure.clone();
        let slots = slots.clone();
        let spawn = spawn.clone();
        let in_flight = in_flight.clone();
        tasks.spawn(async move {
            let path = root.path.clone();
            let label = root.label.clone();
            // An OS thread, rather than Tokio's non-abortable blocking pool,
            // keeps runtime teardown bounded when a kernel/filesystem call is
            // permanently wedged. The thread is detached on timeout; the
            // shared in-flight flag prevents another probe of this path and a
            // process-wide permit caps path churn across config reloads until
            // calls actually return.
            let reset = in_flight.get(&path).cloned().map(InFlightReset);
            // A permit can be held by a syscall from an earlier config
            // generation. Wait within this cycle's deadline so responsive
            // roots are serialized through the remaining permits instead of
            // one root being marked unknown forever on every cycle.
            let Some(permit) = slots.acquire_until(slot_deadline).await else {
                drop(reset);
                return (path.clone(), unknown_storage_volume(label, path.clone()));
            };
            let (tx, rx) = tokio::sync::oneshot::channel();
            let work: ProbeWork = Box::new(move || {
                let _permit = permit;
                let _reset = reset;
                let _ = tx.send(measure(root));
            });
            let reading = if spawn(work).is_err() {
                unknown_storage_volume(label, path.clone())
            } else {
                match tokio::time::timeout(deadline, rx).await {
                    Ok(Ok(Some(reading))) => reading,
                    Ok(Ok(None)) | Ok(Err(_)) | Err(_) => {
                        unknown_storage_volume(label, path.clone())
                    }
                }
            };
            (path, reading)
        });
    }

    let mut readings = Vec::with_capacity(roots.len());
    while let Some(joined) = tasks.join_next().await {
        let Ok((path, reading)) = joined else {
            continue;
        };
        readings.push((path, reading));
    }
    readings
}

#[cfg(unix)]
pub fn disk_space(path: &Path) -> Option<DiskSpace> {
    use std::os::unix::ffi::OsStrExt;
    let Ok(cstr) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return None;
    };
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(cstr.as_ptr(), &mut st) };
    if rc != 0 {
        return None;
    }
    Some(DiskSpace {
        available: (st.f_bavail as u64).saturating_mul(st.f_frsize as u64),
        total: (st.f_blocks as u64).saturating_mul(st.f_frsize as u64),
    })
}

#[cfg(windows)]
pub fn disk_space(path: &Path) -> Option<DiskSpace> {
    use std::os::windows::ffi::OsStrExt;

    #[link(name = "kernel32")]
    extern "system" {
        fn GetDiskFreeSpaceExW(
            directory: *const u16,
            available: *mut u64,
            total: *mut u64,
            free: *mut u64,
        ) -> i32;
    }

    let wide: Vec<u16> = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let mut available = 0u64;
    let mut total = 0u64;
    let mut free = 0u64;
    let ok = unsafe { GetDiskFreeSpaceExW(wide.as_ptr(), &mut available, &mut total, &mut free) };
    (ok != 0).then_some(DiskSpace { available, total })
}

#[cfg(not(any(unix, windows)))]
pub fn disk_space(_path: &Path) -> Option<DiskSpace> {
    None
}

/// Measure configured roots once per containing filesystem.
///
/// A root that does not exist yet is measured through its nearest existing
/// ancestor, because that is the filesystem it will consume when the daemon
/// creates it. Multiple roles on one mounted volume are grouped so neither the
/// dashboard nor the guard pretends they are independent failure domains.
pub fn storage_volume_readings(roots: &[DiskGuardRoot]) -> Vec<StorageVolumeReading> {
    let mut groups = Vec::<StorageGroup<'_>>::new();
    for root in roots {
        let measured = nearest_existing_ancestor(&root.path);
        let device = measured
            .as_ref()
            .and_then(|path| filesystem_device_id(path));
        let existing =
            device.and_then(|id| groups.iter().position(|group| group.device == Some(id)));
        let member = StorageMember {
            root,
            measured: measured.unwrap_or_else(|| root.path.clone()),
        };
        if let Some(index) = existing {
            groups[index].members.push(member);
        } else {
            groups.push(StorageGroup {
                device,
                members: vec![member],
            });
        }
    }
    groups
        .into_iter()
        .map(|group| {
            let space = group
                .members
                .iter()
                .find_map(|member| disk_space(&member.measured));
            StorageVolumeReading {
                label: group
                    .members
                    .iter()
                    .map(|member| member.root.label.as_str())
                    .collect::<Vec<_>>()
                    .join(" · "),
                path: common_storage_path(&group.members),
                available_bytes: space.map(|reading| reading.available),
                total_bytes: space.map(|reading| reading.total),
                device_id: group.device,
                current: space.is_some(),
            }
        })
        .collect()
}

#[cfg(unix)]
fn filesystem_device_id(path: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|metadata| metadata.dev())
}

// Windows capacity measurement remains fully supported. The standard
// metadata API does not expose a stable filesystem/volume identity across
// every supported Rust version, so distinct configured paths stay as
// conservative independent rows there instead of being falsely coalesced.
#[cfg(not(unix))]
fn filesystem_device_id(_path: &Path) -> Option<u64> {
    None
}

/// Return the measured filesystem with the least available space.
pub fn disk_guard_reading(roots: &[DiskGuardRoot]) -> DiskGuardReading {
    select_limiting_volume(storage_volume_readings(roots))
}

fn nearest_existing_ancestor(path: &Path) -> Option<PathBuf> {
    let mut measured = path.to_path_buf();
    loop {
        match std::fs::metadata(&measured) {
            Ok(_) => return Some(measured),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let parent = measured.parent()?.to_path_buf();
                if parent == measured {
                    return None;
                }
                measured = parent;
            }
            Err(_) => return None,
        }
    }
}

fn common_storage_path(members: &[StorageMember<'_>]) -> PathBuf {
    let mut common = members[0].root.path.clone();
    while !members
        .iter()
        .all(|member| member.root.path.starts_with(&common))
    {
        if !common.pop() {
            break;
        }
    }
    if common.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        common
    }
}

fn select_limiting_volume(
    readings: impl IntoIterator<Item = StorageVolumeReading>,
) -> DiskGuardReading {
    let volumes = readings.into_iter().collect::<Vec<_>>();
    let all_roots_known = !volumes.is_empty() && volumes.iter().all(|reading| reading.current);
    let Some(root) = volumes
        .iter()
        .filter(|reading| reading.available_bytes.is_some())
        .min_by_key(|reading| reading.available_bytes)
    else {
        return DiskGuardReading {
            all_roots_known,
            volumes,
            ..Default::default()
        };
    };
    DiskGuardReading {
        available_bytes: root.available_bytes,
        limiting_label: Some(root.label.clone()),
        limiting_path: Some(root.path.clone()),
        all_roots_known,
        volumes,
    }
}

fn coalesce_storage_volumes(
    readings: impl IntoIterator<Item = StorageVolumeReading>,
) -> Vec<StorageVolumeReading> {
    let mut groups = Vec::<StorageVolumeReading>::new();
    for reading in readings {
        let existing = reading.device_id.and_then(|device| {
            groups
                .iter()
                .position(|group| group.device_id == Some(device))
        });
        if let Some(index) = existing {
            let group = &mut groups[index];
            group.label.push_str(" · ");
            group.label.push_str(&reading.label);
            group.path = common_path_pair(&group.path, &reading.path);
            group.current &= reading.current;
            if let Some(available) = reading.available_bytes {
                let should_replace = group
                    .available_bytes
                    .is_none_or(|group_available| available < group_available);
                if should_replace {
                    group.available_bytes = Some(available);
                    group.total_bytes = reading.total_bytes;
                }
            }
        } else {
            groups.push(reading);
        }
    }
    groups
}

fn common_path_pair(left: &Path, right: &Path) -> PathBuf {
    let mut common = left.to_path_buf();
    while !right.starts_with(&common) {
        if !common.pop() {
            return PathBuf::from(".");
        }
    }
    common
}

/// Free bytes on the filesystem holding `path` (`u64::MAX` when it cannot
/// be measured, so an unavailable probe never false-trips the disk guard).
pub fn free_space(path: &Path) -> u64 {
    disk_space(path).map(|s| s.available).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ReleaseProbeGateOnDrop(std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);

    impl Drop for ReleaseProbeGateOnDrop {
        fn drop(&mut self) {
            let (lock, wake) = &*self.0;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
    }

    #[test]
    fn civil_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_723), (2024, 1, 1)); // leap-adjacent
        assert_eq!(civil_from_days(20_651), (2026, 7, 17));
    }

    #[test]
    fn month_key_honors_start_day() {
        // 2026-07-17, start day 1 → July period.
        let jul17 = 20_651 * 86_400;
        assert_eq!(month_key(jul17, 1), 2026 * 12 + 7);
        // Start day 20: the 17th belongs to the JUNE period.
        assert_eq!(month_key(jul17, 20), 2026 * 12 + 6);
        // Start day 20 on the 20th → July period begins.
        let jul20 = (20_651 + 3) * 86_400;
        assert_eq!(month_key(jul20, 20), 2026 * 12 + 7);
    }

    #[test]
    fn windows_roll_and_persist() {
        let tmp = tempfile::tempdir().unwrap();
        let mut book = VolumeBook::load(tmp.path(), "a");
        let day1 = 20_651 * 86_400;
        book.add(ServerId(1), 100, day1, 1);
        book.add(ServerId(1), 50, day1, 1);
        book.add(ServerId(2), 10, day1, 1);
        assert_eq!(book.doc().day_total(day1), 160);
        assert_eq!(book.doc().month_total(day1, 1), 160);

        // Next day: daily rolls, monthly accumulates.
        let day2 = day1 + 86_400;
        book.add(ServerId(1), 5, day2, 1);
        assert_eq!(book.doc().day_total(day2), 5);
        assert_eq!(book.doc().month_total(day2, 1), 165);
        assert_eq!(book.doc().servers[&1].total_bytes, 155);

        // Persist + reload.
        book.save_if_dirty();
        let book2 = VolumeBook::load(tmp.path(), "a");
        assert_eq!(book2.doc().month_total(day2, 1), 165);

        // A peer file is summed into cluster totals.
        let peer = VolumeDoc {
            servers: HashMap::from([(
                1,
                VolumeWindow {
                    total_bytes: 40,
                    day_key: day_key(day2),
                    day_bytes: 40,
                    month_key: month_key(day2, 1),
                    month_bytes: 40,
                },
            )]),
        };
        std::fs::write(
            tmp.path().join("volumes.b.json"),
            serde_json::to_vec(&peer).unwrap(),
        )
        .unwrap();
        let (day, month) = book2.cluster_totals(day2, 1);
        assert_eq!(day, 45);
        assert_eq!(month, 205);
    }

    #[test]
    fn free_space_measures_something() {
        let free = free_space(Path::new("/"));
        assert!(free > 0);
    }

    #[test]
    fn disk_space_reports_available_within_total() {
        let space = disk_space(Path::new("/")).expect("root filesystem is measurable");
        assert!(space.total > 0);
        assert!(space.available <= space.total);
    }

    #[test]
    fn disk_guard_selects_the_lowest_volume_and_keeps_its_identity() {
        let reading = select_limiting_volume([
            StorageVolumeReading {
                label: "state".into(),
                path: PathBuf::from("/state"),
                available_bytes: Some(900),
                total_bytes: Some(1000),
                device_id: Some(1),
                current: true,
            },
            StorageVolumeReading {
                label: "category: tv".into(),
                path: PathBuf::from("/library/tv"),
                available_bytes: Some(100),
                total_bytes: Some(1000),
                device_id: Some(2),
                current: true,
            },
            StorageVolumeReading {
                label: "temporary".into(),
                path: PathBuf::from("/scratch"),
                available_bytes: Some(500),
                total_bytes: Some(1000),
                device_id: Some(3),
                current: true,
            },
        ]);
        assert_eq!(reading.available_bytes, Some(100));
        assert_eq!(reading.limiting_label.as_deref(), Some("category: tv"));
        assert_eq!(
            reading.limiting_path.as_deref(),
            Some(Path::new("/library/tv"))
        );
    }

    #[test]
    fn disk_guard_without_a_measurement_is_explicitly_unknown() {
        let reading = select_limiting_volume([StorageVolumeReading {
            label: "unavailable".into(),
            path: PathBuf::from("/missing"),
            available_bytes: None,
            total_bytes: None,
            device_id: None,
            current: false,
        }]);
        assert_eq!(reading.available_bytes, None);
        assert!(!reading.all_roots_known);
        assert_eq!(reading.volumes.len(), 1);
    }

    #[test]
    fn completed_in_flight_paths_are_pruned_from_the_cross_reload_registry() {
        let live = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dead = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut flags = HashMap::from([
            (PathBuf::from("/live"), std::sync::Arc::downgrade(&live)),
            (PathBuf::from("/dead"), std::sync::Arc::downgrade(&dead)),
        ]);
        drop(dead);
        prune_dead_in_flight_flags(&mut flags);
        assert_eq!(flags.len(), 1);
        assert!(flags.contains_key(Path::new("/live")));
    }

    #[test]
    fn stale_low_reading_is_retained_but_never_counts_as_recovery_proof() {
        let reading = select_limiting_volume([
            StorageVolumeReading {
                label: "stale".into(),
                path: PathBuf::from("/stale"),
                available_bytes: Some(10),
                total_bytes: Some(1000),
                device_id: Some(1),
                current: false,
            },
            StorageVolumeReading {
                label: "healthy".into(),
                path: PathBuf::from("/healthy"),
                available_bytes: Some(900),
                total_bytes: Some(1000),
                device_id: Some(2),
                current: true,
            },
        ]);
        assert_eq!(reading.available_bytes, Some(10));
        assert_eq!(reading.limiting_label.as_deref(), Some("stale"));
        assert!(!reading.all_roots_known);
    }

    #[test]
    fn fresh_low_root_on_a_shared_filesystem_beats_stale_high_evidence() {
        let reading = select_limiting_volume(coalesce_storage_volumes([
            StorageVolumeReading {
                label: "stale role".into(),
                path: PathBuf::from("/shared/stale"),
                available_bytes: Some(900),
                total_bytes: Some(1000),
                device_id: Some(7),
                current: false,
            },
            StorageVolumeReading {
                label: "fresh role".into(),
                path: PathBuf::from("/shared/fresh"),
                available_bytes: Some(10),
                total_bytes: Some(1000),
                device_id: Some(7),
                current: true,
            },
        ]));

        assert_eq!(reading.available_bytes, Some(10));
        assert_eq!(reading.limiting_path.as_deref(), Some(Path::new("/shared")));
        assert!(!reading.all_roots_known);
    }

    #[test]
    fn unknown_shared_filesystem_member_never_erases_a_known_capacity() {
        let known = StorageVolumeReading {
            label: "known".into(),
            path: PathBuf::from("/shared/known"),
            available_bytes: Some(10),
            total_bytes: Some(1000),
            device_id: Some(7),
            current: true,
        };
        let unknown = StorageVolumeReading {
            label: "unknown".into(),
            path: PathBuf::from("/shared/unknown"),
            available_bytes: None,
            total_bytes: None,
            device_id: Some(7),
            current: false,
        };
        for readings in [
            [known.clone(), unknown.clone()],
            [unknown.clone(), known.clone()],
        ] {
            let reading = select_limiting_volume(coalesce_storage_volumes(readings));
            assert_eq!(reading.available_bytes, Some(10));
            assert!(!reading.all_roots_known);
        }
    }

    #[tokio::test]
    async fn slow_root_is_bounded_without_suppressing_fast_root() {
        let roots = [
            DiskGuardRoot {
                label: "slow".into(),
                path: PathBuf::from("/slow"),
            },
            DiskGuardRoot {
                label: "fast".into(),
                path: PathBuf::from("/fast"),
            },
        ];
        let started = std::time::Instant::now();
        let readings =
            bounded_root_readings(&roots, std::time::Duration::from_millis(500), |root| {
                if root.label == "slow" {
                    std::thread::sleep(std::time::Duration::from_secs(2));
                }
                Some(StorageVolumeReading {
                    label: root.label,
                    path: root.path,
                    available_bytes: Some(500),
                    total_bytes: Some(1000),
                    device_id: None,
                    current: true,
                })
            })
            .await;
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        assert_eq!(readings.len(), 2);
        let slow = readings
            .iter()
            .find(|(_, reading)| reading.label == "slow")
            .unwrap();
        let fast = readings
            .iter()
            .find(|(_, reading)| reading.label == "fast")
            .unwrap();
        assert!(!slow.1.current);
        assert!(fast.1.current);
        assert_eq!(fast.1.available_bytes, Some(500));
    }

    #[tokio::test]
    async fn spawn_failure_releases_exact_path_flag_and_process_permit() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let path = PathBuf::from("/spawn-failure");
        let flag = std::sync::Arc::new(AtomicBool::new(true));
        let flags = std::sync::Arc::new(HashMap::from([(path.clone(), flag.clone())]));
        let slots = ProbeSlots::new(1);
        let readings = bounded_root_readings_with(
            &[DiskGuardRoot {
                label: "spawn-failure".into(),
                path: path.clone(),
            }],
            std::time::Duration::from_millis(20),
            flags,
            slots.clone(),
            std::sync::Arc::new(|work| {
                drop(work);
                Err(std::io::Error::other("injected thread creation failure"))
            }),
            |_| panic!("failed thread creation must not run the measurement"),
        )
        .await;

        assert_eq!(readings.len(), 1);
        assert!(!readings[0].1.current);
        assert!(!flag.load(Ordering::Acquire));
        assert_eq!(slots.live.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn process_slots_bound_wedged_calls_across_rotating_probe_instances() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let slots = ProbeSlots::new(2);
        let gate = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let _release_on_failure = ReleaseProbeGateOnDrop(gate.clone());
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let roots: Vec<_> = (0..3)
            .map(|n| DiskGuardRoot {
                label: format!("generation-one-{n}"),
                path: PathBuf::from(format!("/wedged-generation-one-{n}")),
            })
            .collect();
        let gate_for_measure = gate.clone();
        let calls_for_measure = calls.clone();
        let first = bounded_root_readings_with(
            &roots,
            std::time::Duration::from_millis(30),
            std::sync::Arc::new(HashMap::new()),
            slots.clone(),
            default_probe_spawner(),
            move |_| {
                calls_for_measure.fetch_add(1, Ordering::SeqCst);
                let (lock, wake) = &*gate_for_measure;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = wake.wait(released).unwrap();
                }
                None
            },
        )
        .await;
        assert_eq!(first.len(), 3);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(slots.live.load(Ordering::Acquire), 2);

        // A hot-reload-equivalent probe with a brand-new path cannot create
        // another detached thread while the process-wide permits are held.
        let calls_for_rotated = calls.clone();
        let rotated = bounded_root_readings_with(
            &[DiskGuardRoot {
                label: "generation-two".into(),
                path: PathBuf::from("/wedged-generation-two"),
            }],
            std::time::Duration::from_millis(30),
            std::sync::Arc::new(HashMap::new()),
            slots.clone(),
            default_probe_spawner(),
            move |_| {
                calls_for_rotated.fetch_add(1, Ordering::SeqCst);
                None
            },
        )
        .await;
        assert!(!rotated[0].1.current);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let (lock, wake) = &*gate;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while slots.live.load(Ordering::Acquire) != 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "probe permits did not release"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let recovered = bounded_root_readings_with(
            &[DiskGuardRoot {
                label: "generation-three".into(),
                path: PathBuf::from("/generation-three"),
            }],
            std::time::Duration::from_millis(200),
            std::sync::Arc::new(HashMap::new()),
            slots,
            default_probe_spawner(),
            |root| {
                Some(StorageVolumeReading {
                    label: root.label,
                    path: root.path,
                    available_bytes: Some(1),
                    total_bytes: Some(2),
                    device_id: None,
                    current: true,
                })
            },
        )
        .await;
        assert!(recovered[0].1.current);
    }

    #[tokio::test]
    async fn healthy_generation_serializes_through_capacity_left_by_a_wedged_old_root() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Scale the process-wide 64-slot policy down to two slots: one is
        // permanently retained by a removed path, while a new generation
        // legitimately contains the full two responsive roots. Both new
        // roots must finish in one cycle by sharing the remaining slot.
        let slots = ProbeSlots::new(2);
        let old_wedged = slots.try_acquire().expect("old generation owns a slot");
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let calls_for_measure = calls.clone();
        let roots = [
            DiskGuardRoot {
                label: "new-one".into(),
                path: PathBuf::from("/new-one"),
            },
            DiskGuardRoot {
                label: "new-two".into(),
                path: PathBuf::from("/new-two"),
            },
        ];

        let readings = bounded_root_readings_with(
            &roots,
            std::time::Duration::from_millis(300),
            std::sync::Arc::new(HashMap::new()),
            slots.clone(),
            default_probe_spawner(),
            move |root| {
                calls_for_measure.fetch_add(1, Ordering::SeqCst);
                // Aggregate service exceeds one root's deadline, but each
                // individual call remains inside its own response window.
                std::thread::sleep(std::time::Duration::from_millis(200));
                Some(StorageVolumeReading {
                    label: root.label,
                    path: root.path,
                    available_bytes: Some(100),
                    total_bytes: Some(200),
                    device_id: None,
                    current: true,
                })
            },
        )
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), roots.len());
        assert_eq!(readings.len(), roots.len());
        assert!(readings.iter().all(|(_, reading)| reading.current));
        assert_eq!(slots.live.load(Ordering::Acquire), 1);
        drop(old_wedged);
        assert_eq!(slots.live.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn cancellation_aborts_queued_probe_work_without_waiting_for_slot_deadline() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let slots = ProbeSlots::new(1);
        let old_wedged = slots.try_acquire().expect("old syscall owns the only slot");
        let roots = [DiskGuardRoot {
            label: "queued".into(),
            path: PathBuf::from("/queued"),
        }];
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let calls_for_measure = calls.clone();
        let cancel = tokio_util::sync::CancellationToken::new();
        let cancel_for_probe = cancel.clone();
        let started = std::time::Instant::now();
        let probe = tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = cancel_for_probe.cancelled() => None,
                readings = bounded_root_readings_with(
                    &roots,
                    std::time::Duration::from_secs(5),
                    std::sync::Arc::new(HashMap::new()),
                    slots,
                    default_probe_spawner(),
                    move |_| {
                        calls_for_measure.fetch_add(1, Ordering::SeqCst);
                        None
                    },
                ) => Some(readings),
            }
        });
        tokio::task::yield_now().await;
        cancel.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_millis(200), probe)
            .await
            .expect("cancelled probe outlived the shutdown bound")
            .unwrap();
        assert!(result.is_none());
        assert!(started.elapsed() < std::time::Duration::from_millis(200));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(old_wedged);
    }

    #[tokio::test]
    async fn cancelling_probe_detaches_an_already_started_filesystem_call() {
        let gate = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let _release_on_failure = ReleaseProbeGateOnDrop(gate.clone());
        let gate_for_measure = gate.clone();
        let cancel = tokio_util::sync::CancellationToken::new();
        let cancel_for_probe = cancel.clone();
        let started = std::time::Instant::now();
        let run = tokio::spawn(async move {
            let mut probe = DiskGuardProbe::default();
            probe
                .probe_with_until_cancelled(
                    &[DiskGuardRoot {
                        label: "started".into(),
                        path: PathBuf::from("/started"),
                    }],
                    std::time::Duration::from_secs(5),
                    &cancel_for_probe,
                    move |_| {
                        let (lock, wake) = &*gate_for_measure;
                        let mut released = lock.lock().unwrap();
                        while !*released {
                            released = wake.wait(released).unwrap();
                        }
                        None
                    },
                )
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        cancel.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_millis(200), run)
            .await
            .expect("cancelled response await blocked shutdown")
            .unwrap();
        assert!(result.is_none());
        assert!(started.elapsed() < std::time::Duration::from_millis(200));
        let (lock, wake) = &*gate;
        *lock.lock().unwrap() = true;
        wake.notify_all();
    }

    #[tokio::test]
    async fn timed_out_root_is_not_rescheduled_while_its_syscall_is_still_running() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let roots = [DiskGuardRoot {
            label: "wedged".into(),
            path: PathBuf::from("/wedged"),
        }];
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let calls_for_probe = calls.clone();
        let mut probe = DiskGuardProbe::default();
        let measure = move |root: DiskGuardRoot| {
            calls_for_probe.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(120));
            Some(StorageVolumeReading {
                label: root.label,
                path: root.path,
                available_bytes: Some(500),
                total_bytes: Some(1000),
                device_id: None,
                current: true,
            })
        };

        let first = probe
            .probe_with(&roots, std::time::Duration::from_millis(20), measure)
            .await;
        assert!(!first.all_roots_known);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let second = probe
            .probe_with(&roots, std::time::Duration::from_millis(20), |_| {
                panic!("an in-flight root must not launch another blocking call")
            })
            .await;
        assert!(!second.all_roots_known);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        let third = probe
            .probe_with(&roots, std::time::Duration::from_millis(20), |root| {
                Some(StorageVolumeReading {
                    label: root.label,
                    path: root.path,
                    available_bytes: Some(600),
                    total_bytes: Some(1000),
                    device_id: None,
                    current: true,
                })
            })
            .await;
        assert!(third.all_roots_known);
        assert_eq!(third.available_bytes, Some(600));
    }

    #[test]
    fn a_permanently_blocked_probe_does_not_delay_tokio_runtime_shutdown() {
        let gate = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let _release_on_failure = ReleaseProbeGateOnDrop(gate.clone());
        let gate_for_probe = gate.clone();
        let root = DiskGuardRoot {
            label: "wedged".into(),
            path: PathBuf::from("/runtime-shutdown-wedged"),
        };
        let started = std::time::Instant::now();
        {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            let mut probe = DiskGuardProbe::default();
            runtime.block_on(probe.probe_with(
                &[root],
                std::time::Duration::from_millis(20),
                move |_| {
                    let (lock, wake) = &*gate_for_probe;
                    let mut released = lock.lock().unwrap();
                    while !*released {
                        released = wake.wait(released).unwrap();
                    }
                    None
                },
            ));
            // Dropping this runtime is the proof boundary. Tokio would wait
            // forever here if the probe used its non-abortable blocking pool.
        }
        assert!(started.elapsed() < std::time::Duration::from_millis(200));
        let (lock, wake) = &*gate;
        *lock.lock().unwrap() = true;
        wake.notify_all();
    }

    #[cfg(unix)]
    #[test]
    fn inaccessible_path_is_unknown_instead_of_borrowing_ancestor_capacity() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let blocked = tmp.path().join("blocked");
        std::fs::create_dir(&blocked).unwrap();
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o0)).unwrap();
        let target = DiskGuardRoot {
            label: "blocked".into(),
            path: blocked.join("future"),
        };
        let reading = storage_volume_readings(&[target]);
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o700)).unwrap();

        assert_eq!(reading.len(), 1);
        assert_eq!(reading[0].available_bytes, None);
        assert!(!reading[0].current);
    }

    #[cfg(unix)]
    #[test]
    fn configured_roots_on_one_filesystem_are_one_failure_domain() {
        let tmp = tempfile::tempdir().unwrap();
        let roots = [
            DiskGuardRoot {
                label: "state".into(),
                path: tmp.path().join("state"),
            },
            DiskGuardRoot {
                label: "downloads".into(),
                path: tmp.path().join("downloads"),
            },
        ];
        let readings = storage_volume_readings(&roots);
        assert_eq!(readings.len(), 1);
        assert_eq!(readings[0].label, "state · downloads");
        assert_eq!(readings[0].path, tmp.path());
        assert!(readings[0].available_bytes.is_some());
    }

    #[cfg(windows)]
    #[test]
    fn windows_keeps_distinct_paths_as_conservative_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let readings = storage_volume_readings(&[
            DiskGuardRoot {
                label: "state".into(),
                path: tmp.path().join("state"),
            },
            DiskGuardRoot {
                label: "downloads".into(),
                path: tmp.path().join("downloads"),
            },
        ]);
        assert_eq!(readings.len(), 2);
        assert!(readings.iter().all(|reading| reading.current));
    }
}
