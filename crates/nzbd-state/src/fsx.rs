//! `std::fs` wrappers that attach the path to every error.
//!
//! Nothing in this crate calls `std::fs` directly: an un-annotated
//! `io::Error` escaping to the daemon log is the bug this module exists
//! to prevent.
use super::StateError;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::Path;

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_DIR_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn fail_next_dir_sync() {
    FAIL_NEXT_DIR_SYNC.with(|flag| flag.set(true));
}

#[cfg(test)]
fn injected_dir_sync_failure(p: &Path) -> Result<(), StateError> {
    let fail = FAIL_NEXT_DIR_SYNC.with(|flag| flag.replace(false));
    if fail {
        return Err(StateError::Io {
            op: "fsync directory",
            path: p.to_path_buf(),
            source: std::io::Error::from_raw_os_error(5),
        });
    }
    Ok(())
}

pub(super) fn ctx<T>(
    r: std::io::Result<T>,
    op: &'static str,
    path: &Path,
) -> Result<T, StateError> {
    r.map_err(|source| StateError::Io {
        op,
        path: path.to_path_buf(),
        source,
    })
}

pub(super) fn create_dir_all(p: &Path) -> Result<(), StateError> {
    ctx(std::fs::create_dir_all(p), "create directory", p)
}

/// Create a directory tree and durably commit every newly-created directory
/// entry on POSIX. The missing paths are recorded before creation, then each
/// parent is synchronized from the leaf upward (`a/b/c`: sync `b`, then `a`).
pub(super) fn create_dir_all_durable(p: &Path) -> Result<(), StateError> {
    let mut missing = Vec::new();
    let mut cursor = Some(p);
    while let Some(path) = cursor {
        match path.try_exists() {
            Ok(true) => break,
            Ok(false) => missing.push(path.to_path_buf()),
            Err(error) => return ctx(Err(error), "inspect directory", path),
        }
        cursor = path.parent();
    }
    create_dir_all(p)?;
    for created in missing {
        sync_parent(&created)?;
    }
    Ok(())
}

pub(super) fn open(p: &Path) -> Result<File, StateError> {
    ctx(File::open(p), "open", p)
}

pub(super) fn create(p: &Path) -> Result<File, StateError> {
    ctx(File::create(p), "create", p)
}

pub(super) fn open_append(p: &Path) -> Result<File, StateError> {
    ctx(
        OpenOptions::new().create(true).append(true).open(p),
        "open for append",
        p,
    )
}

/// Create this incarnation's journal without opening another writer's inode.
pub(super) fn create_append_exclusive(p: &Path) -> Result<File, StateError> {
    ctx(
        OpenOptions::new().create_new(true).append(true).open(p),
        "create incarnation journal",
        p,
    )
}

pub(super) fn read(p: &Path) -> Result<Vec<u8>, StateError> {
    ctx(std::fs::read(p), "read", p)
}

pub(super) fn write(p: &Path, data: &[u8]) -> Result<(), StateError> {
    ctx(std::fs::write(p, data), "write", p)
}

pub(super) fn read_dir(p: &Path) -> Result<std::fs::ReadDir, StateError> {
    ctx(std::fs::read_dir(p), "read directory", p)
}

pub(super) fn rename(from: &Path, to: &Path) -> Result<(), StateError> {
    ctx(std::fs::rename(from, to), "rename onto", to)
}

pub(super) fn remove_file(p: &Path) -> Result<(), StateError> {
    ctx(std::fs::remove_file(p), "remove", p)
}

pub(super) fn remove_dir_all(p: &Path) -> Result<(), StateError> {
    ctx(std::fs::remove_dir_all(p), "remove directory", p)
}

// Handle operations: the caller passes the path it opened, since a
// `File` does not remember where it came from.

pub(super) fn write_all(f: &mut File, buf: &[u8], p: &Path) -> Result<(), StateError> {
    ctx(f.write_all(buf), "append to", p)
}

/// Like [`write_all`] but for writing a fresh file's whole content — the
/// error op says "write", not "append to", so a snapshot failure reads
/// correctly in the log.
pub(super) fn write_whole(f: &mut File, buf: &[u8], p: &Path) -> Result<(), StateError> {
    ctx(f.write_all(buf), "write", p)
}

pub(super) fn sync_data(f: &File, p: &Path) -> Result<(), StateError> {
    ctx(f.sync_data(), "fsync", p)
}

/// Commit a newly-created directory entry after its file contents are
/// durable. POSIX requires an explicit parent-directory fsync; Windows'
/// `FlushFileBuffers` on the created file already commits its metadata and
/// opening directories as regular `File`s is not portable there.
pub(super) fn sync_parent(p: &Path) -> Result<(), StateError> {
    #[cfg(unix)]
    if let Some(parent) = p.parent() {
        #[cfg(test)]
        injected_dir_sync_failure(parent)?;
        let dir = ctx(File::open(parent), "open directory for fsync", parent)?;
        ctx(dir.sync_all(), "fsync directory", parent)?;
    }
    Ok(())
}

pub(super) fn sync_dir(p: &Path) -> Result<(), StateError> {
    #[cfg(unix)]
    {
        #[cfg(test)]
        injected_dir_sync_failure(p)?;
        let dir = ctx(File::open(p), "open directory for fsync", p)?;
        ctx(dir.sync_all(), "fsync directory", p)?;
    }
    Ok(())
}

/// Truncate a file through a fresh write-capable handle, then make the
/// truncation durable. This must not use an append-only handle: on Windows
/// `FILE_APPEND_DATA` does not grant the `FILE_WRITE_DATA` permission required
/// by `SetEndOfFile`.
pub(super) fn truncate(p: &Path) -> Result<(), StateError> {
    let file = ctx(
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(p),
        "truncate",
        p,
    )?;
    sync_data(&file, p)
}

pub(super) fn canonicalize(path: &Path) -> Result<std::path::PathBuf, StateError> {
    ctx(std::fs::canonicalize(path), "resolve path", path)
}

pub(super) fn exists(path: &Path) -> Result<bool, StateError> {
    ctx(path.try_exists(), "inspect path", path)
}

pub(super) fn read_to_string(path: &Path) -> Result<String, StateError> {
    ctx(std::fs::read_to_string(path), "read", path)
}
