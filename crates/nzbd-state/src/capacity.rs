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
    static CLAIMS: OnceLock<Mutex<HashMap<u64, u64>>> = OnceLock::new();
    pub struct Reservation {
        device: u64,
        bytes: u64,
    }
    impl Drop for Reservation {
        fn drop(&mut self) {
            let mut claims = CLAIMS.get_or_init(Default::default).lock().unwrap();
            let amount = claims.entry(self.device).or_default();
            *amount = amount.saturating_sub(self.bytes);
        }
    }

    pub fn reserve(root: &Path, bytes: u64) -> io::Result<Reservation> {
        let meta = std::fs::symlink_metadata(root)?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            return Err(io::Error::other("unsafe capacity context"));
        }
        let device = meta.dev();
        let mut claims = CLAIMS.get_or_init(Default::default).lock().unwrap();
        let reserved = claims.entry(device).or_default();
        let free = available(root)?;
        if free < bytes.saturating_add(*reserved).saturating_add(RESERVE) {
            return Err(io::Error::from_raw_os_error(libc::ENOSPC));
        }
        *reserved = reserved.saturating_add(bytes);
        Ok(Reservation { device, bytes })
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
}
#[cfg(unix)]
pub use native::{available, health, reserve, Reservation};
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
