//! Stable descriptor fingerprints; payload bytes are never retained.
use crate::PostError;
use md5::{Digest, Md5};
use std::{fs::Metadata, io::Read, path::Path, time::SystemTime};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStamp {
    pub length: u64,
    modified: Option<SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    changed: (i64, i64),
}
impl FileStamp {
    pub fn of(metadata: &Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Self {
            length: metadata.len(),
            modified: metadata.modified().ok(),
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(unix)]
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        }
    }
}
pub(crate) fn stamp(path: &Path) -> Result<FileStamp, PostError> {
    let input = nzbd_state::fileops::open(path)
        .map_err(|e| PostError::Subprocess(format!("input identity {}: {e}", path.display())))?;
    Ok(FileStamp::of(&input.metadata()?))
}
#[derive(Debug, Clone)]
pub(crate) struct Fingerprint {
    pub stamp: FileStamp,
    pub md5: [u8; 16],
    pub crcs: Vec<u32>,
}
pub(crate) fn scan(path: &Path, slice: u64) -> Result<Fingerprint, PostError> {
    scan_checked(
        path,
        slice,
        &crate::attempt::checkpoint,
        &crate::attempt::scanned,
    )
}
fn scan_checked(
    path: &Path,
    slice: u64,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
    scanned: &dyn Fn(u64),
) -> Result<Fingerprint, PostError> {
    if slice == 0 {
        return Err(PostError::Subprocess("PAR slice size is zero".into()));
    }
    let mut input =
        nzbd_state::fileops::open(path).map_err(|e| PostError::Subprocess(e.to_string()))?;
    let before = FileStamp::of(&input.metadata()?);
    let blocks = before.length.div_ceil(slice);
    if blocks.saturating_mul(4) > 64 * 1024 * 1024 {
        return Err(PostError::Subprocess(
            "PAR fingerprint metadata exceeds 64 MiB; reduce the recovery set size".into(),
        ));
    }
    let mut crcs = Vec::with_capacity(blocks as usize);
    let mut md5 = Md5::new();
    let mut crc = crc32fast::Hasher::new();
    let mut in_slice = 0u64;
    let mut read_bytes = 0u64;
    let mut buffer = [0; 65536];
    loop {
        checkpoint()?;
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        read_bytes += n as u64;
        scanned(n as u64);
        md5.update(&buffer[..n]);
        let mut offset = 0;
        while offset < n {
            let used = (slice - in_slice).min((n - offset) as u64) as usize;
            crc.update(&buffer[offset..offset + used]);
            offset += used;
            in_slice += used as u64;
            if in_slice == slice {
                crcs.push(std::mem::replace(&mut crc, crc32fast::Hasher::new()).finalize());
                in_slice = 0;
            }
        }
    }
    if in_slice > 0 {
        let zeros = [0; 65536];
        while in_slice < slice {
            checkpoint()?;
            let n = (slice - in_slice).min(zeros.len() as u64) as usize;
            crc.update(&zeros[..n]);
            in_slice += n as u64;
        }
        crcs.push(crc.finalize());
    }
    if read_bytes != before.length
        || before != FileStamp::of(&input.metadata()?)
        || stamp(path)? != before
    {
        return Err(PostError::Subprocess(format!(
            "input identity changed while scanning {}",
            path.display()
        )));
    }
    Ok(Fingerprint {
        stamp: before,
        md5: md5.finalize().into(),
        crcs,
    })
}
pub(crate) struct ProgressReader<R>(pub R);
impl<R: Read> Read for ProgressReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let n = self.0.read(buffer)?;
        crate::attempt::scanned(n as u64);
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn single_pass_fingerprint_covers_full_digest_and_padded_final_slice() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("input");
        let bytes: Vec<_> = (0..65539).map(|n| (n % 251) as u8).collect();
        std::fs::write(&path, &bytes).unwrap();
        let fingerprint = scan(&path, 8192).unwrap();
        assert_eq!(fingerprint.md5, <[u8; 16]>::from(Md5::digest(&bytes)));
        let expected: Vec<_> = bytes
            .chunks(8192)
            .map(|chunk| {
                let mut padded = vec![0; 8192];
                padded[..chunk.len()].copy_from_slice(chunk);
                crc32fast::hash(&padded)
            })
            .collect();
        assert_eq!(fingerprint.crcs, expected);
        std::fs::rename(&path, path.with_extension("old")).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        assert_ne!(
            stamp(&path).unwrap(),
            fingerprint.stamp,
            "same bytes do not establish unchanged custody"
        );
    }
    #[test]
    fn cancellation_and_path_replacement_are_detected_between_bounded_chunks() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("input");
        std::fs::write(&path, vec![7; 200000]).unwrap();
        let checks = std::cell::Cell::new(0);
        let bytes = std::cell::Cell::new(0);
        let result = scan_checked(
            &path,
            8192,
            &|| {
                checks.set(checks.get() + 1);
                if checks.get() > 2 {
                    Err(std::io::ErrorKind::Interrupted.into())
                } else {
                    Ok(())
                }
            },
            &|n| bytes.set(bytes.get() + n),
        );
        assert!(result.is_err());
        assert_eq!(bytes.get(), 131072);
        let checks = std::cell::Cell::new(0);
        let result = scan_checked(
            &path,
            8192,
            &|| {
                checks.set(checks.get() + 1);
                if checks.get() == 2 {
                    std::fs::rename(&path, path.with_extension("old"))?;
                    std::fs::write(&path, vec![7; 200000])?;
                }
                Ok(())
            },
            &|_| {},
        );
        assert!(result.unwrap_err().to_string().contains("identity changed"));
    }
}
