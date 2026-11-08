//! Ephemeral expectations from a CRC-validated producer read. These tokens do
//! not prove current content; only destination readback can commit a rename.
use super::{fs, Artifact, Identity, Inventory};
use md5::{Digest, Md5};
use std::fs::File;
use std::io::{self, Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, OnceLock, Weak,
};

#[derive(Debug)]
struct DescriptorPermit;
static HELD: AtomicUsize = AtomicUsize::new(0);
static LIMIT: OnceLock<usize> = OnceLock::new();
#[cfg(any(test, feature = "test-support"))]
thread_local! { static HIDE_LOCATOR_CHANGES: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
/// Simulate a remote filesystem reporting unchanged locator metadata. The
/// destination digest is deliberately unaffected. Never enabled in production.
#[cfg(any(test, feature = "test-support"))]
pub fn with_hidden_locator_for_test<T>(run: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            HIDE_LOCATOR_CHANGES.with(|hidden| hidden.set(self.0));
        }
    }
    let _reset = Reset(HIDE_LOCATOR_CHANGES.with(|hidden| hidden.replace(true)));
    run()
}
fn descriptor_limit() -> usize {
    *LIMIT.get_or_init(|| {
        #[cfg(unix)]
        {
            let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
            if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0 {
                return (limit.rlim_cur / 4).min(256) as usize;
            }
        }
        64
    })
}
impl DescriptorPermit {
    fn acquire() -> Option<Self> {
        HELD.fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
            (held < descriptor_limit()).then_some(held + 1)
        })
        .ok()
        .map(|_| Self)
    }
}
impl Drop for DescriptorPermit {
    fn drop(&mut self) {
        HELD.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The immutable original read identity remains distinct from publication.
#[derive(Debug)]
pub struct ProducerRead {
    file: File,
    original: Identity,
    md5_full: [u8; 16],
    md5_prefix: [u8; 16],
    sha256: String,
    _permit: DescriptorPermit,
}
#[derive(Debug)]
pub struct PublishedProducerRead {
    read: ProducerRead,
    path: PathBuf,
    locator: Identity,
}
#[derive(Debug)]
pub struct NamingEvidence {
    valid: Arc<AtomicBool>,
    published: PublishedProducerRead,
    inventory: String,
    installation: String,
    job: u32,
    file: u32,
    writer_generation: u64,
    artifact: String,
    allocation_generation: String,
    root: PathBuf,
    root_identity: Identity,
    source_path: PathBuf,
}
pub(super) struct NamingLease {
    valid: Weak<AtomicBool>,
    artifact: String,
    generation: String,
}
pub struct NamingCandidate {
    pub file: u32,
    pub writer_generation: u64,
    pub path: PathBuf,
}
#[derive(Debug, Default)]
pub struct RestoreMetrics {
    pub readback_bytes: u64,
    pub readback_ms: u64,
    pub namespace_ms: u64,
    pub manifest_ms: u64,
    pub journal_ms: u64,
}

fn complete(identity: &Identity) -> bool {
    !identity.directory
        && !identity.mount_id.is_empty()
        && !identity.modified.is_empty()
        && !identity.changed.is_empty()
}
pub(super) fn strict_identity(a: &Identity, b: &Identity) -> bool {
    complete(a)
        && complete(b)
        && a.device == b.device
        && a.inode == b.inode
        && a.mount_id == b.mount_id
        && a.bytes == b.bytes
        && a.modified == b.modified
        && a.changed == b.changed
}
fn namespace_continuity(a: &Identity, b: &Identity) -> bool {
    complete(a)
        && complete(b)
        && a.device == b.device
        && a.inode == b.inode
        && a.mount_id == b.mount_id
        && a.bytes == b.bytes
        && a.modified == b.modified
}

/// Mandatory CRC validation, optional naming hashes, and one sequential pass.
/// Call on an owned joined blocking worker. No token is returned under pressure.
pub fn verify_producer_read(
    mut file: File,
    expected_size: u64,
    expected_crc: u32,
    collect_naming: bool,
    checkpoint: &dyn Fn() -> io::Result<()>,
) -> io::Result<Option<ProducerRead>> {
    checkpoint()?;
    let original = fs::identity_for_file(&file).map_err(io::Error::other)?;
    if original.directory || original.bytes != expected_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "producer size changed",
        ));
    }
    let permit = (collect_naming && expected_size > 0 && complete(&original))
        .then(DescriptorPermit::acquire)
        .flatten();
    file.rewind()?;
    let mut crc = crc32fast::Hasher::new();
    let mut md5 = Md5::new();
    let mut prefix = Md5::new();
    let mut sha = sha2::Sha256::new();
    let mut read = 0u64;
    let mut buffer = [0; 65536];
    loop {
        checkpoint()?;
        let n = file.read(&mut buffer)?;
        crate::fileops::payload_read(n);
        if n == 0 {
            break;
        }
        crc.update(&buffer[..n]);
        if permit.is_some() {
            md5.update(&buffer[..n]);
            sha.update(&buffer[..n]);
            let remaining = 16384u64.saturating_sub(read) as usize;
            prefix.update(&buffer[..n.min(remaining)]);
        }
        read += n as u64;
    }
    checkpoint()?;
    let after = fs::identity_for_file(&file).map_err(io::Error::other)?;
    // CRC-only scans still require exact before/after descriptor identity.
    let stable = original.device == after.device
        && original.inode == after.inode
        && original.mount_id == after.mount_id
        && original.bytes == after.bytes
        && original.modified == after.modified
        && original.changed == after.changed
        && original.directory == after.directory;
    if read != expected_size || !stable || crc.finalize() != expected_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "producer CRC or identity changed",
        ));
    }
    Ok(permit.map(|permit| ProducerRead {
        file,
        original,
        md5_full: md5.finalize().into(),
        md5_prefix: prefix.finalize().into(),
        sha256: format!("{:x}", sha.finalize()),
        _permit: permit,
    }))
}

impl ProducerRead {
    pub fn published(self, path: &Path, post_identity: &Identity) -> Option<PublishedProducerRead> {
        let held = fs::identity_for_file(&self.file).ok()?;
        let located = crate::fileops::identity(&crate::fileops::open(path).ok()?).ok()?;
        if !namespace_continuity(&self.original, &held)
            || !strict_identity(&held, post_identity)
            || !strict_identity(&held, &located)
        {
            return None;
        }
        Some(PublishedProducerRead {
            read: self,
            path: path.into(),
            locator: held,
        })
    }
}
impl NamingEvidence {
    pub(super) fn live(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }
    pub fn path(&self) -> &Path {
        &self.published.path
    }
    pub(super) fn job(&self) -> u32 {
        self.job
    }
    pub fn md5_full(&self) -> [u8; 16] {
        self.published.read.md5_full
    }
    pub fn md5_prefix(&self) -> [u8; 16] {
        self.published.read.md5_prefix
    }
    pub fn bytes(&self) -> u64 {
        self.published.read.original.bytes
    }
    pub(super) fn expected_sha(&self) -> &str {
        &self.published.read.sha256
    }
}
impl Inventory {
    pub fn invalidate_all_writer_naming(&self) {
        for (_, lease) in self.naming_leases.lock().unwrap().drain() {
            if let Some(valid) = lease.valid.upgrade() {
                valid.store(false, Ordering::Release);
            }
        }
        self.retire_naming_if(|_| true);
    }
    pub(super) fn invalidate_source_writer_naming(&self, artifact: &str, generation: &str) {
        self.naming_leases.lock().unwrap().retain(|_, lease| {
            if lease.artifact == artifact && lease.generation == generation {
                if let Some(valid) = lease.valid.upgrade() {
                    valid.store(false, Ordering::Release);
                }
                false
            } else {
                lease.valid.strong_count() > 0
            }
        });
        self.retire_naming_if(|entry| {
            entry.artifact == artifact && entry.allocation_generation == generation
        });
    }
    /// Make optional room before reserving another process-wide descriptor.
    /// Transferred active-pass tokens are never evicted or stripped of permits.
    pub fn prepare_writer_naming(&self) {
        self.drain_writer_naming();
        while HELD.load(Ordering::Acquire) >= descriptor_limit() {
            let old = self.writer_naming.lock().unwrap().pop_front();
            if old.is_none() {
                break;
            }
            drop(old); // Blocking caller, outside the cache lock.
        }
    }
    /// Run only on the existing joined filesystem/maintenance lane.
    pub fn drain_writer_naming(&self) {
        let retired = std::mem::take(&mut *self.retired_naming.lock().unwrap());
        drop(retired);
    }
    pub fn discard_writer_naming(&self, evidence: PublishedProducerRead) {
        self.retired_naming.lock().unwrap().push(evidence.read);
    }
    fn retire_naming_if(&self, mut matches: impl FnMut(&NamingEvidence) -> bool) {
        let retired = {
            let mut cache = self.writer_naming.lock().unwrap();
            let mut kept = std::collections::VecDeque::new();
            let mut retired = Vec::new();
            while let Some(entry) = cache.pop_front() {
                if matches(&entry) {
                    retired.push(entry.published.read);
                } else {
                    kept.push_back(entry);
                }
            }
            *cache = kept;
            retired
        };
        self.retired_naming.lock().unwrap().extend(retired);
    }
    pub fn invalidate_writer_naming(&self, job: u32) {
        self.naming_leases.lock().unwrap().retain(|(id, _), lease| {
            if *id == job {
                if let Some(valid) = lease.valid.upgrade() {
                    valid.store(false, Ordering::Release);
                }
                false
            } else {
                lease.valid.strong_count() > 0
            }
        });
        self.retire_naming_if(|entry| entry.job == job);
    }
    pub fn invalidate_writer_naming_file(&self, job: u32, file: u32) {
        if let Some(lease) = self.naming_leases.lock().unwrap().remove(&(job, file)) {
            if let Some(valid) = lease.valid.upgrade() {
                valid.store(false, Ordering::Release);
            }
        }
        self.retire_naming_if(|entry| entry.job == job && entry.file == file);
    }
    pub fn remember_writer_naming(
        &self,
        job: u32,
        file: u32,
        writer_generation: u64,
        allocation: &Artifact,
        evidence: PublishedProducerRead,
    ) {
        self.invalidate_writer_naming_file(job, file);
        if self.closed.load(Ordering::Acquire)
            || self.installation.is_empty()
            || allocation.generation.is_empty()
            || writer_generation == 0
        {
            self.discard_writer_naming(evidence);
            return;
        }
        let source = allocation;
        if source.job != Some(job)
            || !source.owned
            || source.terminal()
            || !evidence
                .path
                .strip_prefix(&source.path)
                .is_ok_and(|relative| {
                    !relative.as_os_str().is_empty()
                        && relative
                            .components()
                            .all(|part| matches!(part, std::path::Component::Normal(_)))
                })
        {
            self.discard_writer_naming(evidence);
            return;
        }
        let entry = NamingEvidence {
            valid: Arc::new(AtomicBool::new(true)),
            published: evidence,
            inventory: self.worker_session.clone(),
            installation: self.installation.clone(),
            job,
            file,
            writer_generation,
            artifact: source.id.clone(),
            allocation_generation: source.generation.clone(),
            root: source.root.clone(),
            root_identity: source.root_identity.clone(),
            source_path: source.path.clone(),
        };
        let mut leases = self.naming_leases.lock().unwrap();
        leases.retain(|_, lease| lease.valid.strong_count() > 0);
        leases.insert(
            (job, file),
            NamingLease {
                valid: Arc::downgrade(&entry.valid),
                artifact: source.id.clone(),
                generation: source.generation.clone(),
            },
        );
        drop(leases);
        let mut cache = self.writer_naming.lock().unwrap();
        let mut retired = Vec::new();
        while cache.len() >= descriptor_limit() {
            let Some(old) = cache.pop_front() else {
                self.retired_naming
                    .lock()
                    .unwrap()
                    .push(entry.published.read);
                return;
            };
            retired.push(old.published.read);
        }
        cache.push_back(entry);
        drop(cache);
        self.retired_naming.lock().unwrap().extend(retired);
    }
    pub fn take_writer_naming(&self, job: u32, current: &[NamingCandidate]) -> Vec<NamingEvidence> {
        let entries = {
            let mut cache = self.writer_naming.lock().unwrap();
            let mut taken = Vec::new();
            let mut kept = std::collections::VecDeque::new();
            while let Some(entry) = cache.pop_front() {
                if entry.job == job {
                    taken.push(entry);
                } else {
                    kept.push_back(entry);
                }
            }
            *cache = kept;
            taken
        };
        if entries.is_empty() {
            return Vec::new();
        }
        let Ok(Some(source)) = self.for_job(job) else {
            return Vec::new();
        };
        let current: std::collections::HashSet<_> = current
            .iter()
            .map(|candidate| {
                (
                    candidate.file,
                    candidate.writer_generation,
                    candidate.path.as_path(),
                )
            })
            .collect();
        entries
            .into_iter()
            .filter(|entry| {
                current.contains(&(
                    entry.file,
                    entry.writer_generation,
                    entry.published.path.as_path(),
                )) && self.naming_bound_to_source(entry, &entry.published.path, &source)
            })
            .collect()
    }

    pub fn naming_candidate_matches(&self, entry: &NamingEvidence, from: &Path) -> bool {
        self.for_job(entry.job)
            .ok()
            .flatten()
            .is_some_and(|source| self.naming_matches_source(entry, from, &source))
    }
    fn naming_bound_to_source(
        &self,
        entry: &NamingEvidence,
        from: &Path,
        source: &Artifact,
    ) -> bool {
        if !entry.live()
            || self.closed.load(Ordering::Acquire)
            || entry.inventory != self.worker_session
            || self.installation.is_empty()
            || entry.installation != self.installation
            || source.job != Some(entry.job)
            || !source.owned
            || source.hold.is_some()
            || !matches!(
                source.state.as_str(),
                "active" | "transitioning" | "retained" | "completed"
            )
            || source.terminal()
            || source.id != entry.artifact
            || source.generation != entry.allocation_generation
            || source.path != entry.source_path
            || source.root != entry.root
            || source.root_identity.mount_id.is_empty()
            || entry.root_identity.mount_id.is_empty()
            || source.root_identity.modified.is_empty()
            || source.root_identity.changed.is_empty()
            || entry.root_identity.modified.is_empty()
            || entry.root_identity.changed.is_empty()
            || source.root_identity.device != entry.root_identity.device
            || !source.root_identity.same_object(&entry.root_identity)
            || source.root_identity.mount_id != entry.root_identity.mount_id
            || from != entry.published.path
        {
            return false;
        }
        let Ok(relative) = from.strip_prefix(&source.path) else {
            return false;
        };
        if relative
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
        {
            return false;
        }
        !relative.as_os_str().is_empty()
    }
    fn naming_matches_source(
        &self,
        entry: &NamingEvidence,
        from: &Path,
        source: &Artifact,
    ) -> bool {
        if !self.naming_bound_to_source(entry, from, source) {
            return false;
        }
        let Ok(relative) = from.strip_prefix(&source.path) else {
            return false;
        };
        let Ok(root) = self.verify(source) else {
            return false;
        };
        let Ok(file) = fs::open_relative(&root, &relative.to_string_lossy()) else {
            return false;
        };
        let Ok(observed) = fs::identity_for_file(&file) else {
            return false;
        };
        let Ok(held) = fs::identity_for_file(&entry.published.read.file) else {
            return false;
        };
        #[cfg(any(test, feature = "test-support"))]
        let (observed, held) = if HIDE_LOCATOR_CHANGES.with(|hidden| hidden.get())
            && observed.same_object(&entry.published.locator)
            && held.same_object(&entry.published.locator)
            && observed.bytes == entry.published.locator.bytes
            && held.bytes == entry.published.locator.bytes
        {
            (
                entry.published.locator.clone(),
                entry.published.locator.clone(),
            )
        } else {
            (observed, held)
        };
        strict_identity(&held, &entry.published.locator)
            && strict_identity(&observed, &held)
            && namespace_continuity(&entry.published.read.original, &held)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    static SERIAL: Mutex<()> = Mutex::new(());
    fn fixture() -> (tempfile::TempDir, Inventory, PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("payload");
        std::fs::create_dir(&root).unwrap();
        let source = root.join("job");
        let inventory = Inventory::open(&temp.path().join("state")).unwrap();
        inventory
            .allocate_for_delivery(1, &root, &source, true, false)
            .unwrap();
        (temp, inventory, source)
    }
    fn published(path: &Path, bytes: &[u8]) -> PublishedProducerRead {
        verify_producer_read(
            crate::fileops::open(path).unwrap(),
            bytes.len() as u64,
            crc32fast::hash(bytes),
            true,
            &|| Ok(()),
        )
        .unwrap()
        .unwrap()
        .published(
            path,
            &crate::fileops::identity(&crate::fileops::open(path).unwrap()).unwrap(),
        )
        .unwrap()
    }
    fn install(inventory: &Inventory, path: &Path, file: u32, bytes: &[u8]) {
        let source = inventory.for_job(1).unwrap().unwrap();
        inventory.remember_writer_naming(1, file, 10, &source, published(path, bytes));
    }
    fn candidate(path: &Path, file: u32) -> NamingCandidate {
        NamingCandidate {
            path: path.into(),
            file,
            writer_generation: 10,
        }
    }
    #[test]
    fn producer_crc_is_required_and_scan_is_single_pass_without_optional_evidence() {
        let _serial = SERIAL.lock().unwrap();
        let (_temp, _inventory, source) = fixture();
        let path = source.join("data");
        let bytes = vec![7u8; 100_000];
        std::fs::write(&path, &bytes).unwrap();
        crate::fileops::PAYLOAD_READ_BYTES.with(|count| count.set(0));
        assert!(verify_producer_read(
            File::open(&path).unwrap(),
            bytes.len() as u64,
            crc32fast::hash(&bytes),
            false,
            &|| Ok(())
        )
        .unwrap()
        .is_none());
        assert_eq!(
            crate::fileops::PAYLOAD_READ_BYTES.with(|count| count.get()),
            bytes.len() as u64
        );
        assert!(verify_producer_read(
            File::open(&path).unwrap(),
            bytes.len() as u64,
            0,
            true,
            &|| Ok(())
        )
        .is_err());
        assert!(verify_producer_read(
            File::open(&path).unwrap(),
            1,
            crc32fast::hash(&bytes),
            true,
            &|| Ok(())
        )
        .is_err());
        let checks = std::cell::Cell::new(0);
        assert!(verify_producer_read(
            File::open(&path).unwrap(),
            bytes.len() as u64,
            crc32fast::hash(&bytes),
            true,
            &|| {
                checks.set(checks.get() + 1);
                if checks.get() >= 3 {
                    Err(io::ErrorKind::Interrupted.into())
                } else {
                    Ok(())
                }
            }
        )
        .is_err());
    }
    #[test]
    fn sibling_renames_keep_tokens_and_each_commit_reads_every_destination_byte() {
        let _serial = SERIAL.lock().unwrap();
        let (_temp, inventory, source) = fixture();
        let bytes = vec![9u8; 80_000];
        let a = source.join("a");
        let b = source.join("b");
        std::fs::write(&a, &bytes).unwrap();
        std::fs::write(&b, &bytes).unwrap();
        inventory.refresh_for_delivery(1).unwrap();
        install(&inventory, &a, 1, &bytes);
        install(&inventory, &b, 2, &bytes);
        let entries = inventory.take_writer_naming(1, &[candidate(&a, 1), candidate(&b, 2)]);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].md5_full(), <[u8; 16]>::from(Md5::digest(&bytes)));
        assert_eq!(
            entries[0].md5_prefix(),
            <[u8; 16]>::from(Md5::digest(&bytes[..16384]))
        );
        crate::fileops::PAYLOAD_READ_BYTES.with(|count| count.set(0));
        for (entry, path, target) in [
            (&entries[0], &a, source.join("aa")),
            (&entries[1], &b, source.join("bb")),
        ] {
            assert!(inventory.naming_candidate_matches(entry, path));
            let scanned = std::cell::Cell::new(0);
            let metrics = inventory
                .restore_file_expected(1, path, &target, entry, &|| Ok(()), &|n| {
                    scanned.set(scanned.get() + n)
                })
                .unwrap();
            assert_eq!(metrics.readback_bytes, bytes.len() as u64);
            assert_eq!(scanned.get(), bytes.len() as u64);
            assert_eq!(std::fs::read(target).unwrap(), bytes);
        }
        assert_eq!(
            crate::fileops::PAYLOAD_READ_BYTES.with(|count| count.get()),
            2 * bytes.len() as u64
        );
    }
    #[test]
    fn stale_generation_instance_path_and_legacy_identity_cannot_reuse_evidence() {
        let _serial = SERIAL.lock().unwrap();
        let (temp, inventory, source) = fixture();
        let path = source.join("a");
        let bytes = b"expected";
        std::fs::write(&path, bytes).unwrap();
        install(&inventory, &path, 1, bytes);
        let wrong = NamingCandidate {
            writer_generation: 11,
            ..candidate(&path, 1)
        };
        assert!(inventory.take_writer_naming(1, &[wrong]).is_empty());
        install(&inventory, &path, 1, bytes);
        let mut entry = inventory
            .take_writer_naming(1, &[candidate(&path, 1)])
            .pop()
            .unwrap();
        let other = Inventory::open(&temp.path().join("other-state")).unwrap();
        let artifact = inventory.for_job(1).unwrap().unwrap();
        super::super::save_artifact(&other.db.lock().unwrap(), &artifact).unwrap();
        assert!(!other.naming_candidate_matches(&entry, &path));
        assert!(!inventory.naming_candidate_matches(&entry, &source.join("other")));
        entry.allocation_generation.push('x');
        assert!(!inventory.naming_candidate_matches(&entry, &path));
        entry.allocation_generation.pop();
        entry.published.locator.changed.clear();
        assert!(!inventory.naming_candidate_matches(&entry, &path));
    }
    #[test]
    fn bounded_descriptors_survive_transfer_and_close_on_eviction_and_session_shutdown() {
        let _serial = SERIAL.lock().unwrap();
        let (_temp, inventory, source) = fixture();
        let path = source.join("a");
        let bytes = b"data";
        std::fs::write(&path, bytes).unwrap();
        assert!(descriptor_limit() <= 256);
        for file in 0..descriptor_limit() {
            install(&inventory, &path, file as u32, bytes);
        }
        assert_eq!(HELD.load(Ordering::Acquire), descriptor_limit());
        assert!(verify_producer_read(
            File::open(&path).unwrap(),
            4,
            crc32fast::hash(bytes),
            true,
            &|| Ok(())
        )
        .unwrap()
        .is_none());
        inventory.prepare_writer_naming();
        assert_eq!(HELD.load(Ordering::Acquire), descriptor_limit() - 1);
        install(&inventory, &path, 1000, bytes);
        let candidates: Vec<_> = (0..descriptor_limit())
            .map(|file| candidate(&path, file as u32))
            .chain(std::iter::once(candidate(&path, 1000)))
            .collect();
        let active = inventory.take_writer_naming(1, &candidates);
        assert_eq!(active.len(), descriptor_limit());
        inventory.close().unwrap();
        assert_eq!(
            HELD.load(Ordering::Acquire),
            descriptor_limit(),
            "active tokens retain real permits"
        );
        drop(active);
        assert_eq!(HELD.load(Ordering::Acquire), 0);
    }
    #[test]
    fn hidden_same_size_mutation_fails_final_sha_and_keeps_original_expected_journal() {
        let _serial = SERIAL.lock().unwrap();
        let (_temp, inventory, source) = fixture();
        let path = source.join("a");
        let target = source.join("b");
        let bytes = b"good";
        std::fs::write(&path, bytes).unwrap();
        inventory.refresh_for_delivery(1).unwrap();
        install(&inventory, &path, 1, bytes);
        let entry = inventory
            .take_writer_naming(1, &[candidate(&path, 1)])
            .pop()
            .unwrap();
        let expected = entry.expected_sha().to_owned();
        std::fs::write(&path, b"evil").unwrap();
        assert!(!inventory.naming_candidate_matches(&entry, &path));
        let result = with_hidden_locator_for_test(|| {
            assert!(inventory.naming_candidate_matches(&entry, &path));
            inventory.restore_file_expected(1, &path, &target, &entry, &|| Ok(()), &|_| {})
        });
        assert!(result.is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"evil");
        let raw: String = inventory
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT data FROM operations WHERE json_extract(data,'$.kind')='par_restore'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let op: super::super::Operation = serde_json::from_str(&raw).unwrap();
        assert_ne!(op.state, "succeeded");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&op.request).unwrap()[4],
            expected
        );
        assert!(inventory.validate_post_retry(1).is_err());
        for root in inventory.startup_reconciliation_roots().unwrap() {
            inventory
                .reconcile_transforms_root_snapshot(&root, None)
                .unwrap();
        }
        assert_eq!(inventory.operation(&op.id).unwrap().state, "review");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                &inventory.operation(&op.id).unwrap().request
            )
            .unwrap()[4],
            expected
        );
        assert_eq!(std::fs::read(&target).unwrap(), b"evil");
    }
    #[test]
    fn cancellation_during_destination_read_retains_unfinished_journal_and_data() {
        let _serial = SERIAL.lock().unwrap();
        let (_temp, inventory, source) = fixture();
        let path = source.join("a");
        let target = source.join("b");
        let bytes = vec![1u8; 100_000];
        std::fs::write(&path, &bytes).unwrap();
        inventory.refresh_for_delivery(1).unwrap();
        install(&inventory, &path, 1, &bytes);
        let entry = inventory
            .take_writer_naming(1, &[candidate(&path, 1)])
            .pop()
            .unwrap();
        let scanned = std::cell::Cell::new(0);
        assert!(inventory
            .restore_file_expected(
                1,
                &path,
                &target,
                &entry,
                &|| {
                    if scanned.get() > 0 {
                        Err(io::ErrorKind::Interrupted.into())
                    } else {
                        Ok(())
                    }
                },
                &|n| scanned.set(scanned.get() + n)
            )
            .is_err());
        assert_eq!(scanned.get(), 65536);
        assert_eq!(std::fs::read(&target).unwrap(), bytes);
        let state: String = inventory
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT state FROM operations WHERE json_extract(data,'$.kind')='par_restore'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "running");
    }
    #[test]
    fn active_transfer_is_revoked_without_closing_its_descriptor_on_the_invalidating_lane() {
        let _serial = SERIAL.lock().unwrap();
        let (_temp, inventory, source) = fixture();
        let path = source.join("a");
        std::fs::write(&path, b"data").unwrap();
        install(&inventory, &path, 1, b"data");
        let entry = inventory
            .take_writer_naming(1, &[candidate(&path, 1)])
            .pop()
            .unwrap();
        inventory.invalidate_writer_naming_file(1, 1);
        assert!(!inventory.naming_candidate_matches(&entry, &path));
        assert_eq!(HELD.load(Ordering::Acquire), 1);
        assert!(inventory
            .restore_file_expected(1, &path, &source.join("b"), &entry, &|| Ok(()), &|_| {})
            .is_err());
        assert!(path.exists());
        drop(entry);
        assert_eq!(HELD.load(Ordering::Acquire), 0);
        install(&inventory, &path, 1, b"data");
        inventory.invalidate_writer_naming(1);
        assert_eq!(
            HELD.load(Ordering::Acquire),
            1,
            "owner invalidation defers FUSE close"
        );
        inventory.drain_writer_naming();
        assert_eq!(HELD.load(Ordering::Acquire), 0);
    }
    #[test]
    fn changed_journal_cannot_commit_and_restart_replays_only_original_expected_bytes() {
        let _serial = SERIAL.lock().unwrap();
        let (_temp, inventory, source) = fixture();
        let path = source.join("a");
        let target = source.join("b");
        std::fs::write(&path, b"data").unwrap();
        inventory.refresh_for_delivery(1).unwrap();
        install(&inventory, &path, 1, b"data");
        let entry = inventory
            .take_writer_naming(1, &[candidate(&path, 1)])
            .pop()
            .unwrap();
        let changed = std::cell::Cell::new(false);
        assert!(inventory.restore_file_expected(1,&path,&target,&entry,&||Ok(()),&|n| {
            if n>0 && !changed.replace(true) {
                let db=inventory.db.lock().unwrap();
                let raw:String=db.query_row("SELECT data FROM operations WHERE json_extract(data,'$.kind')='par_restore'",[],|row|row.get(0)).unwrap();
                let mut op:super::super::Operation=serde_json::from_str(&raw).unwrap();
                let mut tuple:serde_json::Value=serde_json::from_str(&op.request).unwrap();
                tuple[4]="changed-expected-journal".into(); op.request=serde_json::to_string(&tuple).unwrap();
                super::super::save_operation(&db,&op).unwrap();
            }
        }).is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"data");
        for root in inventory.startup_reconciliation_roots().unwrap() {
            inventory
                .reconcile_transforms_root_snapshot(&root, None)
                .unwrap();
        }
        let state: String = inventory
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT state FROM operations WHERE json_extract(data,'$.kind')='par_restore'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "review");
    }
    #[test]
    fn restart_after_tentative_rename_uses_legacy_strict_digest_replay_not_cached_expectation() {
        let _serial = SERIAL.lock().unwrap();
        let (temp, inventory, source) = fixture();
        let path = source.join("a");
        let target = source.join("b");
        let bytes = vec![3u8; 100_000];
        std::fs::write(&path, &bytes).unwrap();
        inventory.refresh_for_delivery(1).unwrap();
        install(&inventory, &path, 1, &bytes);
        let entry = inventory
            .take_writer_naming(1, &[candidate(&path, 1)])
            .pop()
            .unwrap();
        let scanned = std::cell::Cell::new(0);
        assert!(inventory
            .restore_file_expected(
                1,
                &path,
                &target,
                &entry,
                &|| {
                    if scanned.get() > 0 {
                        Err(io::ErrorKind::Interrupted.into())
                    } else {
                        Ok(())
                    }
                },
                &|n| scanned.set(scanned.get() + n)
            )
            .is_err());
        inventory.close().unwrap();
        let reopened = Inventory::open(&temp.path().join("state")).unwrap();
        assert!(!reopened.naming_candidate_matches(&entry, &target));
        assert!(reopened
            .take_writer_naming(1, &[candidate(&target, 1)])
            .is_empty());
        for root in reopened.startup_reconciliation_roots().unwrap() {
            reopened
                .reconcile_transforms_root_snapshot(&root, None)
                .unwrap();
        }
        let state: String = reopened
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT state FROM operations WHERE json_extract(data,'$.kind')='par_restore'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "succeeded");
        assert_eq!(std::fs::read(&target).unwrap(), bytes);
        assert!(!path.exists());
    }
}
