//! Descriptor-anchored exclusive publication shared by PP and writers.
use crate::artifacts::{fs, Error, Result};
use std::fs::File;
#[cfg(unix)]
use std::os::{fd::AsRawFd, unix::ffi::OsStrExt};
use std::path::{Component, Path};

#[cfg(test)]
thread_local! { pub(crate) static PAYLOAD_READ_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) }; }
#[inline]
pub(crate) fn payload_read(bytes: usize) {
    #[cfg(test)]
    PAYLOAD_READ_BYTES.with(|total| total.set(total.get().saturating_add(bytes as u64)));
    #[cfg(not(test))]
    let _ = bytes;
}

pub fn open_directory(path: &Path) -> Result<File> {
    fs::open_dir(path)
}

/// Object identity captured from the already-open descriptor, including mount identity.
pub fn identity(file: &File) -> Result<crate::artifacts::Identity> {
    fs::identity_for_file(file)
}

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
    copy_publish_checked(source, target, &|| Ok(()))
}

/// Cooperative copy: the caller must await the worker before retiring custody.
pub fn copy_publish_checked(
    source: &Path,
    target: &Path,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<()> {
    use sha2::{Digest, Sha256};
    use std::io::{Read, Seek, Write};
    let mut input = open(source)?;
    let identity = fs::identity_for_file(&input)?;
    let hash = |file: &mut File| -> Result<Vec<u8>> {
        let before = fs::identity_for_file(file)?;
        file.rewind()?;
        let mut h = Sha256::new();
        let mut buf = [0; 65536];
        loop {
            checkpoint()?;
            let n = file.read(&mut buf)?;
            payload_read(n);
            if n == 0 {
                break;
            }
            h.update(&buf[..n]);
        }
        if fs::identity_for_file(file)? != before {
            return Err(Error::Conflict("copy input changed while hashing".into()));
        }
        Ok(h.finalize().to_vec())
    };
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
    match fs::open_at(&parent, basename, false) {
        Ok(mut existing) => {
            let expected = hash(&mut input)?;
            if hash(&mut existing)? == expected && fs::identity_for_file(&input)? == identity {
                return Ok(());
            }
            return Err(Error::Conflict(
                "publication collision requires review".into(),
            ));
        }
        Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(e) => return Err(e),
    }
    #[cfg(unix)]
    {
        // Key the private attempt by its exact source identity, rather than
        // prehashing the entire source just to name a temporary file.
        let key = format!(
            "{:x}",
            Sha256::digest(
                [
                    target.as_os_str().as_bytes(),
                    &serde_json::to_vec(&identity)?
                ]
                .concat()
            )
        );
        let tmp_name = format!(".runner-publish-{key}");
        let tmp = std::ffi::CString::new(tmp_name.as_str()).unwrap();
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
        let (mut output, expected) = if fd < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(e.into());
            }
            // Restart is conservative: independently validate a previous temp.
            let mut previous = fs::open_at(&parent, Path::new(&tmp_name), false)?;
            let expected = hash(&mut input)?;
            if hash(&mut previous)? != expected {
                return Err(Error::Conflict(
                    "incomplete publication checkpoint requires review".into(),
                ));
            }
            (previous, expected)
        } else {
            let mut output = unsafe { File::from_raw_fd(fd) };
            let mut digest = Sha256::new();
            let mut count = 0u64;
            let mut buffer = [0; 65536];
            loop {
                checkpoint()?;
                let n = input.read(&mut buffer)?;
                payload_read(n);
                if n == 0 {
                    break;
                }
                output.write_all(&buffer[..n])?;
                digest.update(&buffer[..n]);
                count += n as u64;
            }
            if count != identity.bytes || fs::identity_for_file(&input)? != identity {
                return Err(Error::Conflict(
                    "publication source changed during copy".into(),
                ));
            }
            output.sync_all()?;
            (output, digest.finalize().to_vec())
        };
        checkpoint()?;
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
        let mut published = fs::open_at(&parent, basename, false)?;
        let temporary = fs::open_at(&parent, Path::new(&tmp_name), false)?;
        let object = fs::identity_for_file(&output)?;
        if !object.same_object(&fs::identity_for_file(&published)?)
            || !object.same_object(&fs::identity_for_file(&temporary)?)
        {
            return Err(Error::Conflict("copy publication identity changed".into()));
        }
        // The source is retained. Verify the final name after our last
        // namespace mutation so link/unlink ctime changes do not cause scans.
        fs::unlink(&parent, Path::new(&tmp_name), false)?;
        fs::sync_directory(&parent)?;
        if hash(&mut published)? != expected || fs::identity_for_file(&input)? != identity {
            return Err(Error::Conflict("publication source or copy changed".into()));
        }
        output.flush()?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        Err(Error::Conflict("anchored publication unavailable".into()))
    }
}

pub fn rename_exclusive(source: &Path, target: &Path) -> Result<()> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        fs::rename_exclusive_verified(source, target)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        fs::rename_exclusive(source, target)
    }
}

/// Content evidence belongs to the descriptor captured by its producer, never
/// to a pathname recaptured after hashing.
#[derive(Clone, Debug)]
pub struct ContentProof {
    pub identity: crate::artifacts::Identity,
    pub sha256: String,
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

#[cfg(test)]
mod copy_budget_tests {
    use super::*;

    #[test]
    fn fresh_copy_reads_each_source_and_destination_byte_once() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let target = tmp.path().join("target");
        let bytes = vec![19; 3 * 65536 + 17];
        std::fs::write(&source, &bytes).unwrap();
        PAYLOAD_READ_BYTES.with(|total| total.set(0));
        copy_publish_checked(&source, &target, &|| Ok(())).unwrap();
        assert_eq!(
            PAYLOAD_READ_BYTES.with(|total| total.get()),
            bytes.len() as u64 * 2
        );
        assert_eq!(std::fs::read(&target).unwrap(), bytes);
        assert!(source.exists());
        copy_publish_checked(&source, &target, &|| Ok(())).unwrap();
        std::fs::write(&target, vec![7; bytes.len()]).unwrap();
        assert!(copy_publish_checked(&source, &target, &|| Ok(())).is_err());
        assert!(source.exists());
    }

    #[test]
    fn final_copy_readback_rejects_same_length_destination_mutation() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let target = tmp.path().join("target");
        std::fs::write(&source, vec![19; 65536]).unwrap();
        let changed = std::cell::Cell::new(false);
        let result = copy_publish_checked(&source, &target, &|| {
            if target.exists() && !changed.replace(true) {
                std::fs::write(&target, vec![7; 65536])?;
            }
            Ok(())
        });
        assert!(result.is_err());
        assert!(source.exists());
    }
}
