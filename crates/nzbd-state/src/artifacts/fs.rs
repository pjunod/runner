//! Descriptor-relative filesystem operations. Payload paths never act as authority.
use super::{Error, FileEntry, Identity, Result};
use std::fs::{self, File};
use std::path::{Component, Path};

#[cfg(test)]
thread_local! {
    pub(super) static RENAME_FAILURE: std::cell::Cell<Option<i32>> = const { std::cell::Cell::new(None) };
}

pub fn identity(meta: &fs::Metadata) -> Identity {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Identity {
            device: meta.dev(),
            inode: meta.ino(),
            bytes: meta.len(),
            modified: format!("{}:{}", meta.mtime(), meta.mtime_nsec()),
            directory: meta.is_dir(),
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        Identity {
            device: 0,
            inode: meta.creation_time(),
            bytes: meta.len(),
            modified: meta.last_write_time().to_string(),
            directory: meta.is_dir(),
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        Identity {
            device: 0,
            inode: 0,
            bytes: meta.len(),
            modified: format!("{:?}", meta.modified()),
            directory: meta.is_dir(),
        }
    }
}

pub fn absolute(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|p| matches!(p, Component::ParentDir | Component::CurDir))
    {
        return Err(Error::Conflict(
            "an absolute path without dot components is required".into(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
mod unix {
    use super::*;
    use std::ffi::{CStr, CString};
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;

    fn name(path: &Path) -> Result<CString> {
        CString::new(path.as_os_str().as_bytes()).map_err(|_| Error::Conflict("NUL in path".into()))
    }
    pub fn open_at(parent: &File, path: &Path, directory: bool) -> Result<File> {
        let n = name(path)?;
        let flags = libc::O_RDONLY
            | libc::O_NOFOLLOW
            | libc::O_CLOEXEC
            | if directory {
                libc::O_DIRECTORY
            } else {
                libc::O_NONBLOCK
            };
        // SAFETY: n is NUL terminated; parent owns its live descriptor. The
        // returned descriptor is transferred exactly once to File.
        let fd = unsafe { libc::openat(parent.as_raw_fd(), n.as_ptr(), flags) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }
    pub fn open_dir(path: &Path) -> Result<File> {
        absolute(path)?;
        // macOS system aliases are not payload links. Normalize these fixed
        // OS roots, then require no-follow for every operator-owned component.
        #[cfg(target_os = "macos")]
        let normalized = if path.starts_with("/var") {
            Path::new("/private/var").join(path.strip_prefix("/var").unwrap())
        } else if path.starts_with("/tmp") {
            Path::new("/private/tmp").join(path.strip_prefix("/tmp").unwrap())
        } else {
            path.to_path_buf()
        };
        #[cfg(target_os = "macos")]
        let path = normalized.as_path();
        let mut dir = File::open("/")?;
        for c in path.components() {
            if let Component::Normal(n) = c {
                dir = open_at(&dir, Path::new(n), true)?;
            }
        }
        Ok(dir)
    }
    pub fn names(dir: &File) -> Result<Vec<std::ffi::OsString>> {
        // fdopendir owns the duplicate, never the caller's descriptor.
        let fd = unsafe { libc::dup(dir.as_raw_fd()) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let stream = unsafe { libc::fdopendir(fd) };
        if stream.is_null() {
            unsafe {
                libc::close(fd);
            }
            return Err(std::io::Error::last_os_error().into());
        }
        // rewind because dup shares the underlying directory offset.
        unsafe {
            libc::rewinddir(stream);
        }
        let mut out = Vec::new();
        let result = loop {
            #[cfg(target_os = "linux")]
            unsafe {
                *libc::__errno_location() = 0;
            }
            #[cfg(any(target_os = "macos", target_os = "freebsd"))]
            unsafe {
                *libc::__error() = 0;
            }
            let entry = unsafe { libc::readdir(stream) };
            if entry.is_null() {
                let err = std::io::Error::last_os_error();
                break if err.raw_os_error() == Some(0) {
                    Ok(out)
                } else {
                    Err(err.into())
                };
            }
            let bytes = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
            if bytes != b"." && bytes != b".." {
                if out.len() >= 100_001 {
                    break Err(Error::Conflict(
                        "directory enumeration limit reached".into(),
                    ));
                }
                out.push(std::ffi::OsStr::from_bytes(bytes).to_os_string());
            }
        };
        unsafe {
            libc::closedir(stream);
        }
        result
    }
    pub fn unlink(parent: &File, path: &Path, directory: bool) -> Result<()> {
        let n = name(path)?;
        let rc = unsafe {
            libc::unlinkat(
                parent.as_raw_fd(),
                n.as_ptr(),
                if directory { libc::AT_REMOVEDIR } else { 0 },
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        parent.sync_all()?;
        Ok(())
    }
}

#[cfg(unix)]
pub use unix::{names, open_at, open_dir, unlink};

// Windows retains ordinary queue/PP operation and cached inspection. Checked
// destructive ownership remains unavailable until a handle-relative unlink
// implementation exists; admission on this platform creates unowned records.
#[cfg(windows)]
mod windows {
    use super::*;
    use std::os::windows::{
        ffi::OsStringExt,
        fs::{MetadataExt, OpenOptionsExt},
        io::AsRawHandle,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        GetFinalPathNameByHandleW, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_FLAG_OPEN_REPARSE_POINT,
    };
    fn handle_path(dir: &File) -> Result<std::path::PathBuf> {
        let mut buf = vec![0u16; 32768];
        let n = unsafe {
            GetFinalPathNameByHandleW(dir.as_raw_handle(), buf.as_mut_ptr(), buf.len() as u32, 0)
        };
        if n == 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if n as usize >= buf.len() {
            return Err(Error::Conflict("handle path too long".into()));
        }
        Ok(std::ffi::OsString::from_wide(&buf[..n as usize]).into())
    }
    fn open(path: &Path) -> Result<File> {
        let file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)?;
        if file.metadata()?.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(Error::Conflict("reparse point requires review".into()));
        }
        Ok(file)
    }
    pub fn open_dir(path: &Path) -> Result<File> {
        absolute(path)?;
        let mut current = std::path::PathBuf::new();
        for part in path.components() {
            current.push(part.as_os_str());
            if matches!(part, Component::Normal(_)) {
                let file = open(&current)?;
                if !file.metadata()?.is_dir() {
                    return Err(Error::Conflict("not a directory".into()));
                }
            }
        }
        open(path)
    }
    pub fn open_at(dir: &File, path: &Path, directory: bool) -> Result<File> {
        if path.components().count() != 1
            || !matches!(path.components().next(), Some(Component::Normal(_)))
        {
            return Err(Error::Conflict("invalid child name".into()));
        }
        let file = open(&handle_path(dir)?.join(path))?;
        if directory && !file.metadata()?.is_dir() {
            return Err(Error::Conflict("not a directory".into()));
        }
        Ok(file)
    }
    pub fn names(dir: &File) -> Result<Vec<std::ffi::OsString>> {
        let entries = fs::read_dir(handle_path(dir)?)?
            .take(100002)
            .map(|entry| entry.map(|e| e.file_name()))
            .collect::<std::io::Result<Vec<_>>>()?;
        if entries.len() > 100001 {
            return Err(Error::Conflict(
                "directory enumeration limit reached".into(),
            ));
        }
        Ok(entries)
    }
}
#[cfg(windows)]
pub use windows::{names, open_at, open_dir};
#[cfg(not(any(unix, windows)))]
pub fn open_dir(_: &Path) -> Result<File> {
    Err(Error::Conflict("filesystem access unsupported".into()))
}
#[cfg(not(any(unix, windows)))]
pub fn open_at(_: &File, _: &Path, _: bool) -> Result<File> {
    open_dir(Path::new("/"))
}
#[cfg(not(any(unix, windows)))]
pub fn names(_: &File) -> Result<Vec<std::ffi::OsString>> {
    Err(Error::Conflict("enumeration unsupported".into()))
}
#[cfg(not(unix))]
pub fn unlink(_: &File, _: &Path, _: bool) -> Result<()> {
    Err(Error::Conflict("descriptor deletion unavailable".into()))
}

pub fn sync_directory(dir: &File) -> Result<()> {
    #[cfg(unix)]
    dir.sync_all()?;
    // Windows cannot flush a read-only directory handle. No destructive
    // ownership is granted there; each written regular file is still flushed.
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

pub fn manifest(dir: &File, limit: usize) -> Result<Vec<FileEntry>> {
    fn walk(
        dir: &File,
        prefix: &Path,
        out: &mut Vec<FileEntry>,
        limit: usize,
        depth: usize,
    ) -> Result<()> {
        if depth > 64 {
            return Err(Error::Conflict(
                "directory depth exceeds 64; inspect manually".into(),
            ));
        }
        for n in names(dir)? {
            if out.len() >= limit {
                return Err(Error::Conflict(
                    "file inventory limit reached; no changes made".into(),
                ));
            }
            let path = prefix.join(&n);
            let display = path
                .to_str()
                .ok_or_else(|| Error::Conflict("non-UTF8 filename requires manual review".into()))?
                .to_owned();
            // Opening a directory as a regular descriptor is allowed. Symlinks
            // are refused by O_NOFOLLOW, special files by the metadata check.
            let child = open_at(dir, Path::new(&n), false)?;
            let meta = child.metadata()?;
            if !meta.is_file() && !meta.is_dir() {
                return Err(Error::Conflict(format!("special file: {display}")));
            }
            out.push(FileEntry {
                path: display,
                identity: identity(&meta),
                digest: None,
            });
            if meta.is_dir() {
                walk(&child, &path, out, limit, depth + 1)?;
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    walk(dir, Path::new(""), &mut out, limit, 0)?;
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

pub fn open_relative(root: &File, path: &str) -> Result<File> {
    let path = Path::new(path);
    if path.is_absolute()
        || path
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err(Error::Conflict("invalid relative file path".into()));
    }
    let mut dir = root.try_clone()?;
    let mut parts = path.components().peekable();
    while let Some(c) = parts.next() {
        dir = open_at(&dir, Path::new(c.as_os_str()), parts.peek().is_some())?;
    }
    Ok(dir)
}

pub fn remove_entry(root: &File, entry: &FileEntry) -> Result<()> {
    let path = Path::new(&entry.path);
    let parent_path = path.parent().unwrap_or(Path::new(""));
    let parent = if parent_path.as_os_str().is_empty() {
        root.try_clone()?
    } else {
        open_relative(root, parent_path.to_str().unwrap())?
    };
    let n = path
        .file_name()
        .ok_or_else(|| Error::Conflict("empty file path".into()))?;
    let child = open_at(&parent, Path::new(n), entry.identity.directory)?;
    let observed = identity(&child.metadata()?);
    if !entry.identity.same_object(&observed) || (!observed.directory && observed != entry.identity)
    {
        return Err(Error::Conflict(format!("identity changed: {}", entry.path)));
    }
    unlink(&parent, Path::new(n), entry.identity.directory)
}

#[cfg(unix)]
#[allow(clippy::unnecessary_cast)] // statvfs field widths differ across Unix targets.
pub fn available_bytes(dir: &File) -> Result<u64> {
    use std::os::fd::AsRawFd;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::fstatvfs(dir.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let stat = unsafe { stat.assume_init() };
    Ok((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}
#[cfg(not(unix))]
pub fn available_bytes(_: &File) -> Result<u64> {
    Err(Error::Conflict("free space is unavailable".into()))
}

/// Atomic publication must never replace an unrelated empty directory.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn rename_exclusive(from: &Path, to: &Path) -> Result<()> {
    use std::ffi::CString;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;
    let source = open_dir(
        from.parent()
            .ok_or_else(|| Error::Conflict("missing source parent".into()))?,
    )?;
    let target = open_dir(
        to.parent()
            .ok_or_else(|| Error::Conflict("missing target parent".into()))?,
    )?;
    let a = CString::new(
        from.file_name()
            .ok_or_else(|| Error::Conflict("missing source name".into()))?
            .as_bytes(),
    )
    .map_err(|_| Error::Conflict("invalid source".into()))?;
    let b = CString::new(
        to.file_name()
            .ok_or_else(|| Error::Conflict("missing target name".into()))?
            .as_bytes(),
    )
    .map_err(|_| Error::Conflict("invalid target".into()))?;
    if a == b {
        let src = identity(&source.metadata()?);
        let dst = identity(&target.metadata()?);
        if src.device == dst.device && src.inode == dst.inode {
            return Ok(());
        }
    }
    #[cfg(test)]
    let injected = RENAME_FAILURE.with(|e| e.take());
    #[cfg(not(test))]
    let injected: Option<i32> = None;
    #[cfg(target_os = "linux")]
    let rc = if injected.is_some() {
        -1
    } else {
        unsafe {
            libc::renameat2(
                source.as_raw_fd(),
                a.as_ptr(),
                target.as_raw_fd(),
                b.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        }
    };
    #[cfg(target_os = "macos")]
    let rc = if injected.is_some() {
        -1
    } else {
        unsafe {
            libc::renameatx_np(
                source.as_raw_fd(),
                a.as_ptr(),
                target.as_raw_fd(),
                b.as_ptr(),
                libc::RENAME_EXCL,
            )
        }
    };
    let error = injected
        .map(std::io::Error::from_raw_os_error)
        .or_else(|| (rc != 0).then(std::io::Error::last_os_error));
    if let Some(error) = error {
        if !matches!(
            error.raw_os_error(),
            Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP | libc::EEXIST)
        ) {
            return Err(error.into());
        }
        // Hard-link creation has atomic no-replacement semantics on NFS and
        // FUSE too. Restrict the fallback to regular files: POSIX offers no
        // equivalent directory primitive, and check-then-rename is racy.
        let name = Path::new(from.file_name().unwrap());
        let input = open_at(&source, name, false)?;
        let original = identity(&input.metadata()?);
        if !input.metadata()?.is_file() {
            return Err(error.into());
        }
        input.sync_all()?;
        if unsafe {
            libc::linkat(
                source.as_raw_fd(),
                a.as_ptr(),
                target.as_raw_fd(),
                b.as_ptr(),
                0,
            )
        } != 0
        {
            let link_error = std::io::Error::last_os_error();
            if link_error.raw_os_error() != Some(libc::EEXIST) {
                return Err(link_error.into());
            }
            // Resume only the two-name state left by link-before-unlink.
            // Equal bytes in a different inode do not authorize deletion.
            let existing = open_at(&target, Path::new(to.file_name().unwrap()), false)?;
            if identity(&existing.metadata()?) != original {
                return Err(link_error.into());
            }
        }
        target.sync_all()?;
        let linked = open_at(&target, Path::new(to.file_name().unwrap()), false)?;
        let current = open_at(&source, name, false)?;
        if identity(&linked.metadata()?) != original || identity(&current.metadata()?) != original {
            return Err(Error::Conflict(
                "source changed during exclusive file rename; both names retained for review"
                    .into(),
            ));
        }
        unlink(&source, name, false)?;
    }
    target.sync_all()?;
    source.sync_all()?;
    Ok(())
}
#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub fn rename_exclusive(_: &Path, _: &Path) -> Result<()> {
    Err(Error::Conflict(
        "exclusive directory publication unavailable on this platform".into(),
    ))
}

#[cfg(windows)]
pub fn rename_exclusive(from: &Path, to: &Path) -> Result<()> {
    // Windows rename fails if a destination directory already exists.
    std::fs::rename(from, to)?;
    Ok(())
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod exclusive_tests {
    use super::*;

    #[test]
    fn unsupported_file_rename_and_crash_replay_preserve_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let from = tmp.path().join("source");
        let to = tmp.path().join("target");
        for code in [libc::EINVAL, libc::ENOSYS, libc::EOPNOTSUPP] {
            fs::write(&from, b"payload").unwrap();
            let before = identity(&fs::metadata(&from).unwrap());
            RENAME_FAILURE.with(|v| v.set(Some(code)));
            rename_exclusive(&from, &to).unwrap();
            assert!(!from.exists());
            assert_eq!(identity(&fs::metadata(&to).unwrap()), before);
            fs::hard_link(&to, &from).unwrap();
            rename_exclusive(&from, &to).unwrap();
            assert!(!from.exists());
            assert_eq!(fs::read(&to).unwrap(), b"payload");
            rename_exclusive(&to, &to).unwrap();
            assert!(to.exists());
            fs::remove_file(&to).unwrap();
        }
    }

    #[test]
    fn exclusive_fallback_never_replaces_conflicts_or_moves_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let from = tmp.path().join("source");
        let to = tmp.path().join("target");
        fs::write(&from, b"same bytes different inode").unwrap();
        fs::write(&to, b"same bytes different inode").unwrap();
        for code in [libc::EINVAL, libc::EEXIST, libc::EIO] {
            RENAME_FAILURE.with(|v| v.set(Some(code)));
            assert!(rename_exclusive(&from, &to).is_err());
            assert_eq!(fs::read(&from).unwrap(), fs::read(&to).unwrap());
        }
        fs::remove_file(&from).unwrap();
        fs::remove_file(&to).unwrap();
        fs::create_dir(&from).unwrap();
        RENAME_FAILURE.with(|v| v.set(Some(libc::EINVAL)));
        assert!(rename_exclusive(&from, &to).is_err());
        assert!(from.is_dir());
        assert!(!to.exists());
    }
}
