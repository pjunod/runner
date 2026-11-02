//! Forecast only unwritten payload bytes. Done ranges are fsync acknowledgements
//! and are CRC-revalidated on recovery/foreign-journal import. File length alone
//! never earns credit: sparse preallocation and pending ranges are not progress.
use nzbd_types::{FileEntry, Job, SegmentState};
use std::path::{Path, PathBuf};

pub(crate) fn done_coverage(file: &FileEntry, size: u64) -> (u64, u64) {
    let mut ranges = file
        .segments
        .iter()
        .filter_map(|segment| match segment.state {
            SegmentState::Done { offset, len, .. } if len > 0 => offset
                .checked_add(u64::from(len))
                .filter(|end| *end <= size)
                .map(|end| (offset, end)),
            _ => None,
        })
        .collect::<Vec<_>>();
    ranges.sort_unstable();
    let mut covered = 0u64;
    let mut end = 0u64;
    for (start, next) in ranges {
        covered = covered.saturating_add(next.saturating_sub(start.max(end)));
        end = end.max(next);
    }
    (covered, end)
}

pub(crate) fn persisted_credit(path: &Path, done: u64, checkpoint_end: u64) -> u64 {
    if done == 0 {
        return 0;
    }
    let Ok(file) = nzbd_state::fileops::open(path) else {
        return 0;
    };
    let Ok(metadata) = file.metadata() else {
        return 0;
    };
    if !metadata.is_file() || metadata.len() < checkpoint_end {
        return 0;
    }
    // Done carries fsync/CRC proof from the owner or validated recovery.
    // Sparse zeros and compression still represent those bytes completely;
    // their physical block count is not remaining write demand.
    done
}

fn checkpoint_path(job: &Job, file: &FileEntry, root: &Path) -> Option<PathBuf> {
    let name = Path::new(&file.filename);
    if !name
        .components()
        .all(|part| matches!(part, std::path::Component::Normal(_)))
    {
        return None;
    }
    let repair = job
        .params
        .iter()
        .any(|(key, _)| key == &format!("*File:repair:{}", file.id.0));
    if file.finalized && !repair {
        return Some(root.join(name));
    }
    let stable = root.join(format!(".runner-file-{}.part", file.id.0));
    match std::fs::symlink_metadata(&stable) {
        Ok(_) => Some(stable),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Some(root.join(format!("{}.part", file.filename)))
        }
        Err(_) => None,
    }
}

/// Called in the blocking probe lane after source identity validation.
pub(crate) fn remaining(job: &Job, root: &Path) -> u64 {
    job.files
        .iter()
        .map(|file| {
            let key = format!("*File:size:{}", file.id.0);
            let size = job
                .params
                .iter()
                .find(|(name, _)| name == &key)
                .and_then(|(_, value)| value.parse::<u64>().ok())
                .filter(|size| *size > 0)
                .unwrap_or_else(|| {
                    file.segments.iter().fold(0u64, |sum, segment| {
                        sum.saturating_add(u64::from(segment.size))
                    })
                });
            let (done, end) = done_coverage(file, size);
            let credit = checkpoint_path(job, file, root)
                .map_or(0, |path| persisted_credit(&path, done, end));
            size.saturating_sub(credit)
        })
        .fold(0u64, u64::saturating_add)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nzbd_types::{FileId, Segment};
    fn file(ranges: &[(u64, u32)]) -> FileEntry {
        FileEntry {
            id: FileId(1),
            subject: "payload".into(),
            filename: "payload.bin".into(),
            filename_confirmed: true,
            is_par2: false,
            paused: false,
            groups: Vec::new(),
            date: None,
            segments: ranges
                .iter()
                .enumerate()
                .map(|(index, (offset, len))| Segment {
                    message_id: "part@test".into(),
                    number: index as u32 + 1,
                    size: *len,
                    state: SegmentState::Done {
                        offset: *offset,
                        len: *len,
                        crc: 0,
                    },
                })
                .collect(),
            crc32: None,
            finalized: false,
        }
    }
    #[test]
    fn verified_ranges_union_without_overflow_or_duplicate_credit() {
        assert_eq!(
            done_coverage(
                &file(&[(0, 100), (50, 100), (200, 0), (u64::MAX - 3, 10), (195, 10)]),
                200
            ),
            (150, 150)
        );
        let mut record = file(&[(0, 100), (100, 100)]);
        record.segments[0].state = SegmentState::Pending;
        record.segments[1].state = SegmentState::Failed;
        assert_eq!(done_coverage(&record, 200), (0, 0));
    }
    #[test]
    fn sparse_length_and_unverified_placeholders_do_not_earn_capacity_credit() {
        use std::io::{Seek, Write};
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("partial");
        let mut output = std::fs::File::create(&path).unwrap();
        output.set_len(128 * 1024 * 1024).unwrap();
        output.sync_all().unwrap();
        assert_eq!(persisted_credit(&path, 0, 0), 0);
        assert!(crate::writer::validate_range(
            &path,
            0,
            65536,
            crc32fast::hash(&[0; 65536])
        ));
        assert_eq!(
            persisted_credit(&path, 65536, 65536),
            65536,
            "CRC-verified sparse-zero Done ranges need no further writes"
        );
        output.rewind().unwrap();
        output.write_all(&[11; 65536]).unwrap();
        output.sync_all().unwrap();
        assert_eq!(persisted_credit(&path, 65536, 65536), 65536);
        assert_eq!(
            persisted_credit(&path, 0, 0),
            0,
            "allocated but unverified bytes are not progress"
        );
        output.set_len(1).unwrap();
        output.sync_all().unwrap();
        assert_eq!(
            persisted_credit(&path, 65536, 65536),
            0,
            "truncated checkpoints cannot earn stale credit"
        );
        #[cfg(unix)]
        {
            let alias = temporary.path().join("alias");
            std::os::unix::fs::symlink(&path, &alias).unwrap();
            assert_eq!(
                persisted_credit(&alias, 1, 1),
                0,
                "symlinks never supply checkpoint evidence"
            );
        }
    }
}
