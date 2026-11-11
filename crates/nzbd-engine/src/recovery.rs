//! Recovery evidence: missing bytes and unavailable storage are different facts.
use nzbd_state::artifacts::Identity;
use nzbd_types::FileId;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

pub(crate) const WRITER_PROTOCOL: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct ProvenRange {
    pub seg_number: u32,
    pub offset: u64,
    pub len: u32,
    pub crc: u32,
}
impl ProvenRange {
    pub fn end(&self) -> Option<u64> {
        self.offset.checked_add(u64::from(self.len))
    }
    pub fn overlaps(&self, other: &Self) -> bool {
        match (self.end(), other.end()) {
            (Some(a), Some(b)) => self.offset < b && other.offset < a,
            _ => true,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DurableFileProof {
    pub protocol: u32,
    pub generation: String,
    pub allocation_generation: String,
    pub installation: String,
    pub identity: Identity,
    pub ranges: Vec<ProvenRange>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_name: Option<String>,
}
impl DurableFileProof {
    /// All claimed evidence must belong to this generation and file object.
    /// Immutable extents and generation retirement preserve the range proof;
    /// disjoint writes and publication can legitimately change size/timestamps.
    /// The caller separately establishes installation/root enrollment and ownership.
    pub fn covers(
        &self,
        generation: &str,
        allocation: &str,
        installation: &str,
        identity: &Identity,
        ranges: &[ProvenRange],
    ) -> bool {
        self.protocol == WRITER_PROTOCOL
            && !generation.is_empty()
            && !allocation.is_empty()
            && !installation.is_empty()
            && self.generation == generation
            && self.allocation_generation == allocation
            && self.installation == installation
            && !identity.directory
            && !self.identity.directory
            && self.identity.same_object(identity)
            && coherent_ranges(&self.ranges, identity.bytes)
            && coherent_ranges(ranges, identity.bytes)
            && {
                let proven: std::collections::HashSet<_> = self.ranges.iter().cloned().collect();
                ranges.iter().all(|range| proven.contains(range))
            }
    }
}

/// A writer proof is recorded only after the range's write, `sync_data` and
/// `fstat` succeeded (writer.rs `PendingWrites::flush`), so a proven range
/// needs no re-read on any filesystem — Gluster/FUSE and NFS included: a
/// successful sync is the same durability claim there. What the proof still
/// requires is a mount identity this build can parse (platform + filesystem
/// kind), so the object it names is the object found (Paul, 2026-11-11).
pub(crate) fn metadata_trustworthy(identity: &Identity) -> bool {
    let mut parts = identity.mount_id.lines();
    if !matches!(parts.next(), Some("linux" | "macos")) {
        return false;
    }
    let Some((length, kind)) = parts.next().and_then(|part| part.split_once(':')) else {
        return false;
    };
    !kind.is_empty() && length.parse::<usize>().ok() == Some(kind.len())
}

pub(crate) fn coherent_ranges(ranges: &[ProvenRange], bytes: u64) -> bool {
    let mut segments = std::collections::HashSet::with_capacity(ranges.len());
    if ranges.iter().any(|range| {
        range.len == 0
            || !range.end().is_some_and(|end| end <= bytes)
            || !segments.insert(range.seg_number)
    }) {
        return false;
    }
    let mut ordered: Vec<_> = ranges.iter().collect();
    ordered.sort_unstable_by_key(|range| range.offset);
    ordered
        .windows(2)
        .all(|pair| pair[0].end().is_some_and(|end| end <= pair[1].offset))
}

#[derive(Debug)]
pub(crate) enum ValidationOutcome {
    Valid(Identity),
    Mismatch,
    /// Error text is preserved; it is never evidence that bytes are absent.
    Unavailable(String),
    IdentityConflict,
    /// Only the caller can promote this to proven absence after root enrollment.
    Missing,
}

pub(crate) fn capture_identity(file: &File) -> Result<Identity, String> {
    nzbd_state::fileops::identity(file).map_err(|error| error.to_string())
}

fn open_outcome(error: nzbd_state::artifacts::Error) -> ValidationOutcome {
    match error {
        nzbd_state::artifacts::Error::Io(error) if error.kind() == std::io::ErrorKind::NotFound => {
            ValidationOutcome::Missing
        }
        nzbd_state::artifacts::Error::Conflict(_) => ValidationOutcome::IdentityConflict,
        error => ValidationOutcome::Unavailable(error.to_string()),
    }
}

pub(crate) fn validate_range(
    path: &Path,
    offset: u64,
    len: u64,
    crc: u32,
    expected: Option<&Identity>,
) -> ValidationOutcome {
    let mut file = match nzbd_state::fileops::open(path) {
        Ok(file) => file,
        Err(error) => return open_outcome(error),
    };
    let before = match capture_identity(&file) {
        Ok(identity) => identity,
        Err(error) => return ValidationOutcome::Unavailable(error),
    };
    if before.directory || expected.is_some_and(|identity| *identity != before) {
        return ValidationOutcome::IdentityConflict;
    }
    let Some(end) = offset.checked_add(len) else {
        return ValidationOutcome::Mismatch;
    };
    if end > before.bytes {
        return ValidationOutcome::Mismatch;
    }
    if let Err(error) = file.seek(SeekFrom::Start(offset)) {
        return ValidationOutcome::Unavailable(error.to_string());
    }
    let mut digest = crc32fast::Hasher::new();
    let mut remaining = len;
    let mut data = [0; 65536];
    while remaining > 0 {
        let amount = remaining.min(data.len() as u64) as usize;
        if let Err(error) = file.read_exact(&mut data[..amount]) {
            return ValidationOutcome::Unavailable(error.to_string());
        }
        digest.update(&data[..amount]);
        remaining -= amount as u64;
    }
    let after = match capture_identity(&file) {
        Ok(identity) => identity,
        Err(error) => return ValidationOutcome::Unavailable(error),
    };
    if before != after {
        return ValidationOutcome::IdentityConflict;
    }
    if digest.finalize() == crc {
        ValidationOutcome::Valid(after)
    } else {
        ValidationOutcome::Mismatch
    }
}

#[derive(Debug)]
pub(crate) struct ResolvedPayload {
    pub path: PathBuf,
    pub identity: Identity,
    pub published: bool,
}
#[derive(Debug)]
pub(crate) enum ResolveError {
    /// The payload could not be inspected; `cause` is the hold its errno
    /// names (`None`: a failure retrying cannot fix).
    Unavailable {
        message: String,
        cause: Option<nzbd_types::HoldCause>,
    },
    IdentityConflict,
}

impl ResolveError {
    fn unavailable(error: &(dyn std::error::Error + 'static)) -> Self {
        ResolveError::Unavailable {
            message: error.to_string(),
            cause: crate::error_hold_cause(error),
        }
    }
}

/// Inspect every alias, including the expected publication name. Never fall
/// through from a failed stat to another path, or silently choose conflicting bytes.
pub(crate) fn resolve_payload(
    dir: &Path,
    file: FileId,
    publication_name: &str,
) -> Result<Option<ResolvedPayload>, ResolveError> {
    if !safe_name(publication_name) {
        return Err(ResolveError::IdentityConflict);
    }
    let paths = [
        dir.join(format!(".runner-file-{}.part", file.0)),
        dir.join(format!("{publication_name}.part")),
        dir.join(publication_name),
    ];
    let mut found: Option<ResolvedPayload> = None;
    for (i, path) in paths.into_iter().enumerate() {
        match std::fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(ResolveError::unavailable(&error)),
            Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
                return Err(ResolveError::IdentityConflict);
            }
            Ok(_) => {}
        }
        let opened =
            nzbd_state::fileops::open(&path).map_err(|error| ResolveError::unavailable(&error))?;
        let identity = nzbd_state::fileops::identity(&opened)
            .map_err(|error| ResolveError::unavailable(&error))?;
        if let Some(previous) = &found {
            if previous.identity != identity {
                return Err(ResolveError::IdentityConflict);
            }
        } else {
            found = Some(ResolvedPayload {
                path,
                identity,
                published: i == 2,
            });
        }
    }
    Ok(found)
}

pub(crate) fn safe_name(name: &str) -> bool {
    !name.is_empty() && !name.contains(['/', '\\', ':', '\0']) && name != "." && name != ".."
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_and_mismatch_are_distinct() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("payload");
        assert!(matches!(
            validate_range(&path, 0, 1, 0, None),
            ValidationOutcome::Missing
        ));
        std::fs::write(&path, b"a").unwrap();
        assert!(matches!(
            validate_range(&path, 0, 2, 0, None),
            ValidationOutcome::Mismatch
        ));
        assert!(matches!(
            validate_range(&path, 0, 1, 0, None),
            ValidationOutcome::Mismatch
        ));
    }
    #[test]
    fn resolver_preserves_final_only_and_rejects_conflicting_aliases() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("done"), b"abc").unwrap();
        assert!(
            resolve_payload(dir.path(), FileId(1), "done")
                .unwrap()
                .unwrap()
                .published
        );
        std::fs::write(dir.path().join(".runner-file-1.part"), b"abc").unwrap();
        assert!(matches!(
            resolve_payload(dir.path(), FileId(1), "done"),
            Err(ResolveError::IdentityConflict)
        ));
    }
    #[test]
    fn coherent_proof_rejects_distinct_ids_claiming_same_extent() {
        let first = ProvenRange {
            seg_number: 1,
            offset: 0,
            len: 3,
            crc: 42,
        };
        let second = ProvenRange {
            seg_number: 2,
            ..first.clone()
        };
        assert!(!coherent_ranges(&[first, second], 3));
    }
    #[test]
    fn proof_survives_disjoint_addition_and_publication_metadata_changes() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial");
        let mut file = nzbd_state::fileops::writer(&path, true).unwrap();
        file.write_all(b"abc").unwrap();
        file.sync_data().unwrap();
        let range = ProvenRange {
            seg_number: 1,
            offset: 0,
            len: 3,
            crc: crc32fast::hash(b"abc"),
        };
        let proof = DurableFileProof {
            protocol: WRITER_PROTOCOL,
            generation: "writer1".into(),
            allocation_generation: "allocation1".into(),
            installation: "install1".into(),
            identity: capture_identity(&file).unwrap(),
            ranges: vec![range.clone()],
            publication_name: Some("final".into()),
        };
        file.write_all(b"def").unwrap();
        file.sync_data().unwrap();
        nzbd_state::fileops::link(&path, &dir.path().join("final")).unwrap();
        let current = capture_identity(&file).unwrap();
        assert!(proof.covers(
            "writer1",
            "allocation1",
            "install1",
            &current,
            std::slice::from_ref(&range)
        ));
        assert!(!proof.covers("writer2", "allocation1", "install1", &current, &[range]));
    }
}

#[cfg(test)]
mod metadata_admission_tests {
    use super::*;
    #[test]
    fn any_parsed_mount_identity_is_trusted_unknown_identity_is_not() {
        let identity = |mount_id: &str| Identity {
            device: 1,
            inode: 2,
            mount_id: mount_id.into(),
            bytes: 4,
            modified: "m".into(),
            changed: "c".into(),
            directory: false,
        };
        for mount in ["", "linux\n3:ext4", "unknown\n4:ext4", "linux\n0:"] {
            assert!(!metadata_trustworthy(&identity(mount)), "{mount}");
        }
        for mount in ["linux\n14:fuse.glusterfs\n4:/dev", "linux\n4:nfs4\n3:nas"] {
            assert!(metadata_trustworthy(&identity(mount)), "{mount}");
        }
        assert!(metadata_trustworthy(&identity(
            "linux\n4:ext4\n9:/dev/data"
        )));
        assert!(metadata_trustworthy(&identity(
            "macos\n4:apfs\n9:/dev/data"
        )));
    }
}
