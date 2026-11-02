//! Shared per-filesystem admission for writers, extraction and relocation.
//! Reservations are forecasts; native runtime failures remain authoritative.
#[cfg(unix)]
mod native {
    use std::collections::HashMap;
    use std::fs::{File, OpenOptions};
    use std::io::{self, Write};
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};

    const RESERVE: u64 = 1024 * 1024 * 1024;
    #[cfg(test)]
    thread_local! {
        pub(crate) static FAIL_NEXT_RESERVATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    // Test-support may isolate one exact directory's simulated headroom.
    // Production always uses (device, None), preserving per-device admission.
    #[cfg(feature = "test-support")]
    type ClaimKey = (u64, Option<u64>);
    #[cfg(not(feature = "test-support"))]
    type ClaimKey = u64;
    static CLAIMS: OnceLock<Mutex<HashMap<ClaimKey, u64>>> = OnceLock::new();
    #[cfg(feature = "test-support")]
    static TEST_AVAILABLE: OnceLock<Mutex<HashMap<(u64, u64), u64>>> = OnceLock::new();
    #[cfg(feature = "test-support")]
    pub struct CapacityTestLimit {
        identity: (u64, u64),
    }
    #[cfg(feature = "test-support")]
    impl Drop for CapacityTestLimit {
        fn drop(&mut self) {
            TEST_AVAILABLE
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .remove(&self.identity);
        }
    }
    #[cfg(feature = "test-support")]
    pub fn inject_available_test_limit(root: &Path, free: u64) -> io::Result<CapacityTestLimit> {
        let meta = std::fs::symlink_metadata(root)?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            return Err(io::Error::other("unsafe test capacity context"));
        }
        let identity = (meta.dev(), meta.ino());
        let mut limits = TEST_AVAILABLE.get_or_init(Default::default).lock().unwrap();
        if limits.contains_key(&identity) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "capacity test limit already active",
            ));
        }
        limits.insert(identity, free);
        Ok(CapacityTestLimit { identity })
    }
    pub struct Reservation {
        key: ClaimKey,
        bytes: u64,
    }
    impl Drop for Reservation {
        fn drop(&mut self) {
            let mut claims = CLAIMS.get_or_init(Default::default).lock().unwrap();
            let amount = claims.entry(self.key).or_default();
            *amount = amount.saturating_sub(self.bytes);
        }
    }

    pub fn reserve(root: &Path, bytes: u64) -> io::Result<Reservation> {
        #[cfg(test)]
        if FAIL_NEXT_RESERVATION.with(|fail| fail.replace(false)) {
            return Err(io::Error::from_raw_os_error(libc::ENOSPC));
        }
        let meta = std::fs::symlink_metadata(root)?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            return Err(io::Error::other("unsafe capacity context"));
        }
        let device = meta.dev();
        #[cfg(feature = "test-support")]
        let simulated = TEST_AVAILABLE
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .get(&(device, meta.ino()))
            .copied();
        #[cfg(not(feature = "test-support"))]
        let simulated: Option<u64> = None;
        #[cfg(feature = "test-support")]
        let key = (device, simulated.map(|_| meta.ino()));
        #[cfg(not(feature = "test-support"))]
        let key = device;
        let mut claims = CLAIMS.get_or_init(Default::default).lock().unwrap();
        let reserved = claims.entry(key).or_default();
        let free = match simulated {
            Some(free) => free,
            None => available(root)?,
        };
        if free < bytes.saturating_add(*reserved).saturating_add(RESERVE) {
            return Err(io::Error::from_raw_os_error(libc::ENOSPC));
        }
        *reserved = reserved.saturating_add(bytes);
        Ok(Reservation { key, bytes })
    }

    #[allow(clippy::unnecessary_cast)] // statvfs field widths vary across supported Unix targets
    pub fn available(root: &Path) -> io::Result<u64> {
        use std::os::unix::ffi::OsStrExt;
        let path = std::ffi::CString::new(root.as_os_str().as_bytes()).map_err(io::Error::other)?;
        let mut result = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        // SAFETY: valid NUL-terminated path and writable statvfs storage.
        if unsafe { libc::statvfs(path.as_ptr(), result.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let result = unsafe { result.assume_init() };
        Ok((result.f_bavail as u64).saturating_mul(result.f_frsize as u64))
    }

    /// Operator-triggered bounded write/flush probe in the destination context.
    /// It does not claim authoritative quota headroom; quota release is explicit.
    pub fn health(root: &Path, bytes: u64, token: &str) -> io::Result<Reservation> {
        let reservation = reserve(root, bytes)?;
        let before = std::fs::symlink_metadata(root)?;
        let path = root.join(format!(".runner-health-{token}"));
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let result = (|| {
            output.write_all(&[0; 65536])?;
            output.sync_all()?;
            File::open(root)?.sync_all()?;
            let after = std::fs::symlink_metadata(root)?;
            if before.dev() != after.dev() || before.ino() != after.ino() {
                return Err(io::Error::other("health context changed"));
            }
            Ok(())
        })();
        drop(output);
        let cleanup = std::fs::remove_file(&path);
        result?;
        cleanup?;
        Ok(reservation)
    }
    #[cfg(all(test, feature = "test-support"))]
    #[test]
    fn exact_test_headroom_and_claims_are_isolated_between_roots() {
        let temporary = tempfile::tempdir().unwrap();
        let a = temporary.path().join("a");
        let b = temporary.path().join("b");
        std::fs::create_dir(&a).unwrap();
        std::fs::create_dir(&b).unwrap();
        assert_eq!(
            std::fs::metadata(&a).unwrap().dev(),
            std::fs::metadata(&b).unwrap().dev()
        );
        let _a_limit = inject_available_test_limit(&a, RESERVE + 2 * 1024 * 1024).unwrap();
        let _b_limit = inject_available_test_limit(&b, RESERVE + 2 * 1024 * 1024).unwrap();
        let _a_claim = reserve(&a, 2 * 1024 * 1024).unwrap();
        let _b_claim = reserve(&b, 2 * 1024 * 1024).unwrap();
        assert!(reserve(&a, 1).is_err());
        assert!(reserve(&b, 1).is_err());
    }
}
#[cfg(all(unix, test))]
pub(crate) use native::FAIL_NEXT_RESERVATION;
#[cfg(unix)]
pub use native::{available, health, reserve, Reservation};
#[cfg(all(unix, feature = "test-support"))]
pub use native::{inject_available_test_limit, CapacityTestLimit};
#[cfg(not(unix))]
pub struct Reservation;
#[cfg(not(unix))]
pub fn reserve(_: &std::path::Path, _: u64) -> std::io::Result<Reservation> {
    Err(std::io::Error::other(
        "capacity and quota context unavailable; operator review required",
    ))
}
#[cfg(not(unix))]
pub fn available(_: &std::path::Path) -> std::io::Result<u64> {
    Err(std::io::Error::other("capacity context unavailable"))
}
#[cfg(not(unix))]
pub fn health(root: &std::path::Path, bytes: u64, _: &str) -> std::io::Result<Reservation> {
    reserve(root, bytes)
}
