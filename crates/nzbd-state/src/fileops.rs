//! Descriptor-anchored exclusive publication shared by PP and writers.
use crate::artifacts::{fs, Error, Result};
use std::fs::File;
#[cfg(unix)]
use std::os::{fd::AsRawFd, unix::ffi::OsStrExt};
use std::path::{Component, Path};

pub fn open(path: &Path) -> Result<File> {
    let parent = fs::open_dir(
        path.parent()
            .ok_or_else(|| Error::Conflict("missing parent".into()))?,
    )?;
    fs::open_at(
        &parent,
        Path::new(
            path.file_name()
                .ok_or_else(|| Error::Conflict("missing name".into()))?,
        ),
        false,
    )
}

pub fn parents(root: &Path, relative: &Path) -> Result<()> {
    let mut parent = fs::open_dir(root)?;
    for part in relative.components() {
        let Component::Normal(name) = part else {
            return Err(Error::Conflict("unsafe relative parent".into()));
        };
        #[cfg(unix)]
        {
            let name_c = std::ffi::CString::new(name.as_bytes())
                .map_err(|_| Error::Conflict("invalid parent".into()))?;
            if unsafe { libc::mkdirat(parent.as_raw_fd(), name_c.as_ptr(), 0o755) } != 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() != std::io::ErrorKind::AlreadyExists {
                    return Err(err.into());
                }
            }
            fs::sync_directory(&parent)?;
        }
        #[cfg(not(unix))]
        {
            return Err(Error::Conflict(
                "anchored parent publication unavailable".into(),
            ));
        }
        parent = fs::open_at(&parent, Path::new(name), true)?;
    }
    Ok(())
}

pub fn link(source: &Path, target: &Path) -> Result<()> {
    let source_dir = fs::open_dir(
        source
            .parent()
            .ok_or_else(|| Error::Conflict("missing source parent".into()))?,
    )?;
    let target_dir = fs::open_dir(
        target
            .parent()
            .ok_or_else(|| Error::Conflict("missing target parent".into()))?,
    )?;
    #[cfg(unix)]
    {
        let a = std::ffi::CString::new(source.file_name().unwrap().as_bytes())
            .map_err(|_| Error::Conflict("invalid source".into()))?;
        let b = std::ffi::CString::new(target.file_name().unwrap().as_bytes())
            .map_err(|_| Error::Conflict("invalid target".into()))?;
        let before = fs::open_at(&source_dir, Path::new(source.file_name().unwrap()), false)?;
        if !before.metadata()?.is_file() {
            return Err(Error::Conflict("source is not regular".into()));
        }
        before.sync_all()?;
        if unsafe {
            libc::linkat(
                source_dir.as_raw_fd(),
                a.as_ptr(),
                target_dir.as_raw_fd(),
                b.as_ptr(),
                0,
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        let after = fs::open_at(&target_dir, Path::new(target.file_name().unwrap()), false)?;
        if fs::identity(&before.metadata()?) != fs::identity(&after.metadata()?) {
            return Err(Error::Conflict(
                "source identity changed during publication".into(),
            ));
        }
        fs::sync_directory(&target_dir)?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        Err(Error::Conflict(
            "exclusive anchored publication unavailable".into(),
        ))
    }
}

pub fn copy_publish(source: &Path, target: &Path) -> Result<()> {
    use sha2::{Digest, Sha256};
    use std::io::{Read, Seek};
    let mut input = open(source)?;
    let identity = fs::identity(&input.metadata()?);
    let hash = |file: &mut File| -> Result<Vec<u8>> {
        let mut h = Sha256::new();
        let mut buf = [0; 65536];
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            h.update(&buf[..n]);
        }
        file.rewind()?;
        Ok(h.finalize().to_vec())
    };
    let expected = hash(&mut input)?;
    let parent = fs::open_dir(
        target
            .parent()
            .ok_or_else(|| Error::Conflict("missing publication parent".into()))?,
    )?;
    let basename = Path::new(
        target
            .file_name()
            .ok_or_else(|| Error::Conflict("missing publication name".into()))?,
    );
    if let Ok(mut existing) = fs::open_at(&parent, basename, false) {
        if hash(&mut existing)? == expected {
            return Ok(());
        }
        return Err(Error::Conflict(
            "publication collision requires review".into(),
        ));
    }
    #[cfg(unix)]
    {
        let key = format!(
            "{:x}",
            Sha256::digest([target.as_os_str().as_bytes(), &expected].concat())
        );
        let tmp = std::ffi::CString::new(format!(".runner-publish-{key}")).unwrap();
        let final_name = std::ffi::CString::new(basename.as_os_str().as_bytes())
            .map_err(|_| Error::Conflict("invalid publication name".into()))?;
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                tmp.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600,
            )
        };
        use std::os::fd::FromRawFd;
        let mut output = if fd < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(e.into());
            }
            // A previous owned operation may have finished its deterministic
            // temp. An incomplete one requires review, never another copy.
            let mut previous = fs::open_at(&parent, Path::new(tmp.to_str().unwrap()), false)?;
            if hash(&mut previous)? != expected {
                return Err(Error::Conflict(
                    "incomplete publication checkpoint requires review".into(),
                ));
            }
            previous
        } else {
            let mut output = unsafe { File::from_raw_fd(fd) };
            std::io::copy(&mut input, &mut output)?;
            output.sync_all()?;
            output.rewind()?;
            output
        };
        if hash(&mut output)? != expected || fs::identity(&input.metadata()?) != identity {
            return Err(Error::Conflict("publication source or copy changed".into()));
        }
        if unsafe {
            libc::linkat(
                parent.as_raw_fd(),
                tmp.as_ptr(),
                parent.as_raw_fd(),
                final_name.as_ptr(),
                0,
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        fs::sync_directory(&parent)?;
        let published = fs::open_at(&parent, basename, false)?;
        let temporary = fs::open_at(&parent, Path::new(tmp.to_str().unwrap()), false)?;
        if fs::identity(&published.metadata()?) == fs::identity(&output.metadata()?)
            && fs::identity(&temporary.metadata()?) == fs::identity(&output.metadata()?)
        {
            fs::unlink(&parent, Path::new(tmp.to_str().unwrap()), false)?;
            fs::sync_directory(&parent)?;
        }
        // Original workspace and deterministic temp remain until its custody
        // journal acknowledges publication. No name-only cleanup is permitted.
        Ok(())
    }
    #[cfg(not(unix))]
    {
        Err(Error::Conflict("anchored publication unavailable".into()))
    }
}

pub fn rename_exclusive(source: &Path, target: &Path) -> Result<()> {
    fs::rename_exclusive(source, target)
}

/// Open/create a stable writer leaf beneath a no-follow parent descriptor.
pub fn writer(path: &Path, create: bool) -> Result<File> {
    let parent = fs::open_dir(
        path.parent()
            .ok_or_else(|| Error::Conflict("missing writer parent".into()))?,
    )?;
    #[cfg(unix)]
    {
        use std::os::fd::FromRawFd;
        let name = std::ffi::CString::new(
            path.file_name()
                .ok_or_else(|| Error::Conflict("missing writer name".into()))?
                .as_bytes(),
        )
        .map_err(|_| Error::Conflict("invalid writer name".into()))?;
        let flags = libc::O_RDWR
            | libc::O_NOFOLLOW
            | libc::O_CLOEXEC
            | if create { libc::O_CREAT } else { 0 };
        let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags, 0o600) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let file = unsafe { File::from_raw_fd(fd) };
        if !file.metadata()?.is_file() {
            return Err(Error::Conflict("writer leaf is not regular".into()));
        }
        fs::sync_directory(&parent)?;
        Ok(file)
    }
    #[cfg(not(unix))]
    {
        let _ = (parent, create);
        Err(Error::Conflict("anchored writer unavailable".into()))
    }
}
