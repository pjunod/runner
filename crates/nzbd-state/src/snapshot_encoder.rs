//! Bounded memoization of serialized jobs. Every result is a complete snapshot;
//! neither decoding nor replay depends on this cache or on an earlier file.
use crate::{snapshot_codec, Job, JobId, QueueSnapshotDoc, StateError, QUEUE_SCHEMA_VERSION};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::mem::size_of;

const MAX_JOBS: usize = 4096;
const MAX_BUDGET: usize = 768 * 1024 * 1024;
// Covers both hash tables, entry headers and capacity slack at MAX_JOBS.
const MAP_RESERVE: usize = 16 * 1024 * 1024;
const MAX_STAGED_FRAMES: usize = 256 * 1024 * 1024;
const MAX_JOB_FRAGMENT: usize = 128 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct SnapshotEncoderStats {
    pub reused_jobs: usize,
    pub encoded_jobs: usize,
    pub cached_jobs: usize,
    pub estimated_cache_bytes: usize,
    pub peak_estimated_cache_bytes: usize,
}

struct Frame {
    bytes: Vec<u8>,
    raw_len: u64,
    crc: crc32fast::Hasher,
    comma: bool,
}
impl Frame {
    fn new(raw: &[u8], comma: bool) -> Result<Self, StateError> {
        let mut crc = crc32fast::Hasher::new();
        crc.update(raw);
        Ok(Self {
            bytes: snapshot_codec::encode_frame(raw)?,
            raw_len: raw.len() as u64,
            crc,
            comma,
        })
    }
}
struct Entry {
    job: Job,
    frame: Frame,
    estimate: usize,
}

/// Retains at most one current copy of each eligible job, with an explicit
/// conservative allocated-capacity budget. It conveys no durability authority.
pub struct SnapshotEncoder {
    entries: HashMap<JobId, Entry>,
    budget: usize,
    stats: SnapshotEncoderStats,
}
impl Default for SnapshotEncoder {
    fn default() -> Self {
        Self::with_budget(MAX_BUDGET)
    }
}
impl SnapshotEncoder {
    pub fn with_budget(bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            budget: bytes.min(MAX_BUDGET),
            stats: Default::default(),
        }
    }
    pub fn stats(&self) -> SnapshotEncoderStats {
        self.stats
    }

    /// Consume an already-owned document; cache retention does not clone jobs.
    pub fn encode(&mut self, doc: QueueSnapshotDoc) -> Result<Vec<u8>, StateError> {
        let result = self.encode_inner(doc);
        if result.is_err() {
            self.entries.clear();
            self.stats = Default::default();
        }
        result
    }

    fn encode_inner(&mut self, doc: QueueSnapshotDoc) -> Result<Vec<u8>, StateError> {
        self.stats = Default::default();
        if doc.schema_version != QUEUE_SCHEMA_VERSION {
            return Err(StateError::Corrupt(format!(
                "refusing to encode queue schema version {}",
                doc.schema_version
            )));
        }
        if self.budget < MAP_RESERVE || doc.jobs.len() > MAX_JOBS {
            self.entries.clear();
            return snapshot_codec::encode(serde_json::to_vec(&doc)?);
        }
        let (prefix, tail) = root_fragments(&doc)?;
        if prefix.len().saturating_add(tail.len()) > snapshot_codec::MAX_BYTES {
            drop(prefix);
            drop(tail);
            self.entries.clear();
            return snapshot_codec::encode(serde_json::to_vec(&doc)?);
        }
        let first = Frame::new(&prefix, false)?;
        let last = Frame::new(&tail, false)?;
        let mut seen = HashSet::new();
        let mut duplicates = HashSet::new();
        for job in &doc.jobs {
            if !seen.insert(job.id) {
                duplicates.insert(job.id);
            }
        }
        let mut old = std::mem::take(&mut self.entries);
        old.retain(|id, _| seen.contains(id) && !duplicates.contains(id));
        let mut old_bytes = old
            .values()
            .fold(0usize, |n, e| n.saturating_add(e.estimate));
        let mut staged_bytes = 0usize;
        self.stats.peak_estimated_cache_bytes = if old.is_empty() {
            0
        } else {
            old_bytes.saturating_add(MAP_RESERVE)
        };
        let mut frames = Vec::with_capacity(doc.jobs.len());
        let mut retain = Vec::with_capacity(doc.jobs.len());
        let mut raw_len = first
            .raw_len
            .checked_add(last.raw_len)
            .ok_or_else(overflow)?;
        let mut encoded_len = 24usize
            .checked_add(first.bytes.len())
            .and_then(|n| n.checked_add(last.bytes.len()))
            .ok_or_else(overflow)?;
        let mut fallback = false;
        for (index, job) in doc.jobs.iter().enumerate() {
            let prior = old.remove(&job.id);
            if let Some(entry) = &prior {
                old_bytes = old_bytes.saturating_sub(entry.estimate);
            }
            let eligible = job.torrent.is_none() && !duplicates.contains(&job.id);
            let frame = match prior {
                Some(entry)
                    if eligible
                        && entry.frame.comma == (index != 0)
                        && same_job(&entry.job, job) =>
                {
                    self.stats.reused_jobs += 1;
                    entry.frame
                }
                _ => {
                    self.stats.encoded_jobs += 1;
                    let mut raw = Vec::new();
                    if index != 0 {
                        raw.push(b',');
                    }
                    serde_json::to_writer(&mut raw, job)?;
                    if raw.len() > MAX_JOB_FRAGMENT
                        || raw_len.saturating_add(raw.len() as u64)
                            > snapshot_codec::MAX_BYTES as u64
                    {
                        fallback = true;
                        break;
                    }
                    Frame::new(&raw, index != 0)?
                }
            };
            raw_len = raw_len.checked_add(frame.raw_len).ok_or_else(overflow)?;
            encoded_len = encoded_len
                .checked_add(frame.bytes.len())
                .ok_or_else(overflow)?;
            if raw_len > snapshot_codec::MAX_BYTES as u64 || encoded_len > MAX_STAGED_FRAMES {
                fallback = true;
                break;
            }
            let estimate = estimate_job(job).saturating_add(allocation(frame.bytes.capacity()));
            let within_budget = eligible
                && MAP_RESERVE
                    .saturating_add(old_bytes)
                    .saturating_add(staged_bytes)
                    .saturating_add(estimate)
                    <= self.budget;
            if within_budget {
                staged_bytes = staged_bytes.saturating_add(estimate);
            }
            self.stats.peak_estimated_cache_bytes = self.stats.peak_estimated_cache_bytes.max(
                MAP_RESERVE
                    .saturating_add(old_bytes)
                    .saturating_add(staged_bytes),
            );
            retain.push(within_budget.then_some(estimate));
            frames.push(frame);
        }
        drop(old);
        if fallback
            || raw_len < snapshot_codec::MIN_BYTES as u64
            || raw_len > snapshot_codec::MAX_BYTES as u64
            || encoded_len as u64 >= raw_len
        {
            // Release staged compressed fragments before allocating the full
            // legacy fallback buffer. Oversized queues remain supported.
            drop(frames);
            drop(retain);
            drop(first);
            drop(last);
            self.stats = Default::default();
            return snapshot_codec::encode(serde_json::to_vec(&doc)?);
        }
        let mut checksum = first.crc.clone();
        for frame in &frames {
            checksum.combine(&frame.crc);
        }
        checksum.combine(&last.crc);
        let count = u32::try_from(frames.len().checked_add(2).ok_or_else(overflow)?)
            .map_err(|_| overflow())?;
        let mut output = Vec::with_capacity(encoded_len);
        output.extend_from_slice(snapshot_codec::MAGIC_V2);
        output.extend_from_slice(&raw_len.to_le_bytes());
        output.extend_from_slice(&checksum.finalize().to_le_bytes());
        output.extend_from_slice(&count.to_le_bytes());
        output.extend_from_slice(&first.bytes);
        for frame in &frames {
            output.extend_from_slice(&frame.bytes);
        }
        output.extend_from_slice(&last.bytes);
        for ((job, frame), estimate) in doc.jobs.into_iter().zip(frames).zip(retain) {
            if let Some(estimate) = estimate {
                self.entries.insert(
                    job.id,
                    Entry {
                        job,
                        frame,
                        estimate,
                    },
                );
            }
        }
        self.stats.cached_jobs = self.entries.len();
        self.stats.estimated_cache_bytes = if self.entries.is_empty() {
            0
        } else {
            staged_bytes.saturating_add(MAP_RESERVE)
        };
        Ok(output)
    }
}
fn overflow() -> StateError {
    StateError::Corrupt("snapshot encoding length overflow".into())
}

fn same_job(left: &Job, right: &Job) -> bool {
    // Exhaustive on purpose: additions to Job require a serialization-equivalence
    // audit here. Torrent records contain floats (-0.0 == 0.0 is not byte-equal),
    // so they are always freshly encoded. All remaining nested types derive Eq.
    let Job {
        id: _,
        kind: _,
        name: _,
        dir_name: _,
        name_provisional: _,
        delivery_lifecycle: _,
        queued_at_unix: _,
        original_name: _,
        category: _,
        priority: _,
        dupe: _,
        params: _,
        files: _,
        totals: _,
        status: _,
        torrent: _,
        stages: _,
    } = left;
    left.torrent.is_none() && right.torrent.is_none() && left == right
}

// Count capacities, not serialized lengths, including containers and every
// owned string/Box allocation. Double requested capacity plus 64 bytes per
// allocation supplies allocator/slack headroom; the real-process benchmark
// reports RSS separately rather than claiming this estimate is an RSS limit.
fn allocation(capacity: usize) -> usize {
    if capacity == 0 {
        0
    } else {
        capacity.saturating_mul(2).saturating_add(64)
    }
}
fn vec_heap<T>(values: &Vec<T>) -> usize {
    allocation(values.capacity().saturating_mul(size_of::<T>()))
}
fn string_heap(value: &String) -> usize {
    allocation(value.capacity())
}
fn estimate_job(job: &Job) -> usize {
    if job.torrent.is_some() {
        return usize::MAX;
    }
    let mut n = size_of::<Job>().saturating_mul(2).saturating_add(64);
    for value in [&job.name, &job.dir_name, &job.original_name, &job.dupe.key] {
        n = n.saturating_add(string_heap(value));
    }
    if let Some(value) = &job.category {
        n = n.saturating_add(string_heap(value));
    }
    n = n.saturating_add(vec_heap(&job.params));
    for (key, value) in &job.params {
        n = n
            .saturating_add(string_heap(key))
            .saturating_add(string_heap(value));
    }
    n = n
        .saturating_add(vec_heap(&job.files))
        .saturating_add(vec_heap(&job.stages));
    let nzbd_types::DupeInfo {
        key: _,
        score: _,
        mode: _,
    } = &job.dupe;
    for file in &job.files {
        let nzbd_types::FileEntry {
            id: _,
            subject: _,
            filename: _,
            filename_confirmed: _,
            is_par2: _,
            paused: _,
            groups: _,
            date: _,
            segments: _,
            crc32: _,
            finalized: _,
        } = file;
        n = n
            .saturating_add(string_heap(&file.subject))
            .saturating_add(string_heap(&file.filename))
            .saturating_add(vec_heap(&file.groups))
            .saturating_add(vec_heap(&file.segments));
        for group in &file.groups {
            n = n.saturating_add(string_heap(group));
        }
        for segment in &file.segments {
            let nzbd_types::Segment {
                message_id: _,
                number: _,
                size: _,
                state: _,
            } = segment;
            n = n.saturating_add(allocation(segment.message_id.len()));
        }
    }
    n
}

fn root_fragments(doc: &QueueSnapshotDoc) -> Result<(Vec<u8>, Vec<u8>), StateError> {
    // No '..': adding a persisted root field must update this encoder.
    let QueueSnapshotDoc {
        schema_version,
        pending_admissions,
        url_admission_receipts,
        jobs: _,
        next_job_id,
        next_file_id,
        download_paused,
        speed_limit_bps,
        max_active_downloads,
    } = doc;
    #[derive(Serialize)]
    struct Prefix<'a> {
        schema_version: u32,
        pending_admissions: &'a [crate::PendingAdmission],
        url_admission_receipts: &'a [crate::UrlAdmissionReceipt],
    }
    #[derive(Serialize)]
    struct Tail {
        next_job_id: u32,
        next_file_id: u32,
        download_paused: bool,
        speed_limit_bps: Option<u64>,
        max_active_downloads: u32,
    }
    let mut prefix = serde_json::to_vec(&Prefix {
        schema_version: *schema_version,
        pending_admissions,
        url_admission_receipts,
    })?;
    prefix.pop();
    prefix.extend_from_slice(b",\"jobs\":[");
    let mut tail = serde_json::to_vec(&Tail {
        next_job_id: *next_job_id,
        next_file_id: *next_file_id,
        download_paused: *download_paused,
        speed_limit_bps: *speed_limit_bps,
        max_active_downloads: *max_active_downloads,
    })?;
    tail[0] = b',';
    tail.insert(0, b']');
    Ok((prefix, tail))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nzbd_types::*;
    fn job(id: u32) -> Job {
        Job {
            id: JobId(id),
            kind: JobKind::Nzb,
            name: format!("job-{id}"),
            dir_name: format!("dir-{id}"),
            name_provisional: false,
            delivery_lifecycle: true,
            queued_at_unix: 11,
            original_name: "original".into(),
            category: Some("tv".into()),
            priority: 1,
            dupe: DupeInfo {
                key: "dupe".into(),
                score: 2,
                mode: Some(DupeMode::Score),
            },
            params: vec![
                ("*Runner:KeepOnFailure".into(), "1".into()),
                ("*File:generation:1".into(), "generation".into()),
                ("*File:proof:1".into(), "proof".into()),
            ],
            files: vec![FileEntry {
                id: FileId(id),
                subject: "subject".into(),
                filename: "payload.mkv".into(),
                filename_confirmed: true,
                is_par2: false,
                paused: false,
                groups: vec!["group".into()],
                date: Some(12),
                segments: vec![Segment {
                    message_id: "message".into(),
                    number: 1,
                    size: 7,
                    state: SegmentState::Done {
                        offset: 0,
                        len: 5,
                        crc: 123,
                    },
                }],
                crc32: Some(123),
                finalized: false,
            }],
            totals: JobTotals {
                size: 7,
                par_size: 1,
                failed_size: 2,
                failed_par_size: 1,
                success_size: 5,
                total_articles: 3,
                success_articles: 1,
                failed_articles: 1,
            },
            status: JobStatus::Paused,
            torrent: None,
            stages: vec![StageSpan {
                stage: PostStage::Unpack,
                started_at_unix: 13,
                ms: Some(15),
            }],
        }
    }
    fn doc() -> QueueSnapshotDoc {
        let mut first = job(1);
        first
            .params
            .push(("opaque".into(), "metadata".repeat(160_000)));
        QueueSnapshotDoc {
            jobs: vec![first, job(2), job(3)],
            next_job_id: 4,
            next_file_id: 5,
            download_paused: true,
            speed_limit_bps: Some(12345),
            max_active_downloads: 2,
            ..Default::default()
        }
    }
    fn exact(encoder: &mut SnapshotEncoder, doc: QueueSnapshotDoc) -> Vec<u8> {
        let expected = serde_json::to_vec(&doc).unwrap();
        let bytes = encoder.encode(doc).unwrap();
        assert_eq!(snapshot_codec::decode(&bytes).unwrap().as_ref(), expected);
        assert!(encoder.stats().estimated_cache_bytes <= encoder.budget);
        assert!(encoder.stats().peak_estimated_cache_bytes <= encoder.budget);
        bytes
    }
    #[test]
    fn unchanged_jobs_are_reused_but_every_snapshot_is_independently_complete() {
        let mut encoder = SnapshotEncoder::default();
        let base = doc();
        let first = exact(&mut encoder, base.clone());
        assert!(first.starts_with(snapshot_codec::MAGIC_V2));
        assert_eq!(encoder.stats().encoded_jobs, 3);
        let second = exact(&mut encoder, base.clone());
        assert_eq!(encoder.stats().reused_jobs, 3);
        assert_eq!(first, second);
        drop(encoder);
        // No encoder, cache, old snapshot or external dictionary is needed.
        assert_eq!(
            snapshot_codec::decode(&second).unwrap().as_ref(),
            serde_json::to_vec(&base).unwrap()
        );
    }
    #[test]
    fn reorder_delete_replacement_duplicate_ids_and_root_updates_use_current_document() {
        let mut encoder = SnapshotEncoder::default();
        let base = doc();
        exact(&mut encoder, base.clone());
        let mut reordered = base.clone();
        reordered.jobs.swap(0, 2);
        exact(&mut encoder, reordered.clone());
        assert_eq!(
            encoder.stats().reused_jobs,
            1,
            "only the middle job retains its comma role"
        );
        reordered.jobs.remove(1);
        exact(&mut encoder, reordered.clone());
        reordered.jobs.push(job(4));
        exact(&mut encoder, reordered.clone());
        reordered.jobs[1]
            .params
            .push(("*File:generation:1".into(), "replacement".into()));
        exact(&mut encoder, reordered.clone());
        reordered.jobs.push(reordered.jobs[0].clone());
        reordered.download_paused = false;
        reordered.speed_limit_bps = None;
        reordered.next_job_id += 10;
        reordered.max_active_downloads = 1;
        exact(&mut encoder, reordered);
    }
    fn scalar_paths(value: &serde_json::Value, path: String, output: &mut Vec<String>) {
        match value {
            serde_json::Value::Array(values) => {
                for (i, v) in values.iter().enumerate() {
                    scalar_paths(v, format!("{path}/{i}"), output);
                }
            }
            serde_json::Value::Object(values) => {
                for (k, v) in values {
                    scalar_paths(
                        v,
                        format!("{path}/{}", k.replace('~', "~0").replace('/', "~1")),
                        output,
                    );
                }
            }
            _ => output.push(path),
        }
    }
    #[test]
    fn differential_encoding_covers_every_present_persisted_scalar() {
        let mut base = doc();
        base.pending_admissions.push(crate::PendingAdmission {
            job_id: JobId(90),
            source: TorrentSource::Magnet,
            secret_ref: "pending/90.source".into(),
            category: Some("tv".into()),
            priority: 1,
            paused: true,
            seed_ratio_limit: Some(1.5),
            seed_time_limit_secs: Some(100),
            stop_seeding_on_complete: Some(true),
            params: vec![("key".into(), "value".into())],
            client: Some("client".into()),
            torrent_tags: vec!["tag".into()],
        });
        base.url_admission_receipts.push(serde_json::from_value(serde_json::json!({"protocolVersion":1,"requestToken":"token","submissionSha256":"submission","jobId":1,"generation":"generation","runnerInstanceId":"instance","outcome":"failed","custodyState":"runner","terminalOutcome":"FAILURE","failureStage":"unpack","failureCause":"io","keepSourceOnFailure":true})).unwrap());
        let value = serde_json::to_value(&base).unwrap();
        let mut paths = Vec::new();
        scalar_paths(&value, String::new(), &mut paths);
        let mut encoder = SnapshotEncoder::default();
        let mut checked = 0;
        for path in paths {
            if path == "/schema_version" {
                continue;
            }
            let original = value.pointer(&path).unwrap();
            let candidates = match original {
                serde_json::Value::Bool(v) => vec![serde_json::json!(!v)],
                serde_json::Value::Number(n) if n.as_u64().is_some() => {
                    vec![serde_json::json!(n.as_u64().unwrap() + 1)]
                }
                serde_json::Value::Number(n) => vec![serde_json::json!(n.as_f64().unwrap() + 1.0)],
                serde_json::Value::String(s) => [
                    format!("{s}x"),
                    "queued".into(),
                    "failed".into(),
                    "completed".into(),
                    "torrent".into(),
                    "all".into(),
                    "par_check".into(),
                    "unpack".into(),
                    "move".into(),
                ]
                .into_iter()
                .map(serde_json::Value::String)
                .collect(),
                serde_json::Value::Null => vec![
                    serde_json::json!(1),
                    serde_json::json!("value"),
                    serde_json::json!(true),
                ],
                _ => unreachable!(),
            };
            for replacement in candidates {
                let mut changed = value.clone();
                *changed.pointer_mut(&path).unwrap() = replacement;
                let Ok(changed) = serde_json::from_value::<QueueSnapshotDoc>(changed) else {
                    continue;
                };
                if serde_json::to_vec(&changed).unwrap() == serde_json::to_vec(&base).unwrap() {
                    continue;
                }
                exact(&mut encoder, base.clone());
                exact(&mut encoder, changed);
                checked += 1;
                break;
            }
        }
        assert!(
            checked >= 100,
            "covered {checked} serialized scalar changes"
        );
    }
    #[test]
    fn torrent_float_records_are_fresh_even_when_numeric_equality_ignores_zero_sign() {
        let mut base = doc();
        base.jobs[2].kind = JobKind::Torrent;
        base.jobs[2].torrent = Some(serde_json::from_value(serde_json::json!({
            "info_hash_v1":"0123456789abcdef0123456789abcdef01234567", "source":"metainfo",
            "metadata_file":"meta/example.torrent", "phase":"paused_download", "files":[],
            "total_bytes":1,"selected_bytes":1,"downloaded_bytes":0,"uploaded_bytes":0,
            "seeding_seconds":0,"ready_at_unix":null,"content_path":null,
            "seed_policy":{"ratio_limit":-0.0,"time_limit_secs":null}, "last_activity_unix":null,"last_error":null
        })).unwrap());
        let mut changed = base.clone();
        changed.jobs[2]
            .torrent
            .as_mut()
            .unwrap()
            .seed_policy
            .ratio_limit = Some(0.0);
        assert_eq!(base.jobs[2], changed.jobs[2]);
        assert_ne!(
            serde_json::to_vec(&base).unwrap(),
            serde_json::to_vec(&changed).unwrap()
        );
        let mut encoder = SnapshotEncoder::default();
        exact(&mut encoder, base);
        exact(&mut encoder, changed);
        assert_eq!(encoder.stats().reused_jobs, 2);
        assert_eq!(encoder.stats().encoded_jobs, 1);
        assert!(!encoder.entries.contains_key(&JobId(3)));
    }

    #[test]
    fn a_cache_populated_by_failed_save_is_not_durability_authority() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::SnapshotStore::open(dir.path()).unwrap();
        store.save(&QueueSnapshotDoc::default()).unwrap();
        let before = std::fs::read(dir.path().join("queue.json")).unwrap();
        let mut encoder = SnapshotEncoder::default();
        let mut current = doc();
        assert!(store
            .save_cached_guarded(current.clone(), &mut encoder, &|| false)
            .is_err());
        assert_eq!(encoder.stats().cached_jobs, 3);
        assert_eq!(
            std::fs::read(dir.path().join("queue.json")).unwrap(),
            before
        );
        current.jobs[0].params[0].1 = "0".into();
        current.download_paused = false;
        let expected = serde_json::to_vec(&current).unwrap();
        store
            .save_cached_guarded(current, &mut encoder, &|| true)
            .unwrap();
        let committed = std::fs::read(dir.path().join("queue.json")).unwrap();
        assert!(committed.starts_with(snapshot_codec::MAGIC_V2));
        assert_eq!(
            snapshot_codec::decode(&committed).unwrap().as_ref(),
            expected
        );
        assert_eq!(encoder.stats().reused_jobs, 2);
    }

    #[test]
    fn capacity_budget_excludes_large_allocations_and_releases_old_entries() {
        let mut encoder = SnapshotEncoder::with_budget(MAP_RESERVE + 4 * 1024 * 1024);
        let mut base = doc();
        let mut oversized = String::with_capacity(8 * 1024 * 1024);
        oversized.push_str("short");
        base.jobs[2].name = oversized;
        // Moving preserves the deliberately large allocation capacity.
        let expected = serde_json::to_vec(&base).unwrap();
        let encoded = encoder.encode(base).unwrap();
        assert_eq!(snapshot_codec::decode(&encoded).unwrap().as_ref(), expected);
        assert_eq!(encoder.stats().cached_jobs, 2);
        assert!(!encoder.entries.contains_key(&JobId(3)));
        assert!(encoder.stats().estimated_cache_bytes <= encoder.budget);
        encoder.budget = 0;
        exact(&mut encoder, doc());
        assert!(encoder.entries.is_empty());
        assert_eq!(encoder.stats().estimated_cache_bytes, 0);
    }
    #[test]
    fn v2_frame_count_corruption_truncation_and_extra_frames_are_refused() {
        let bytes = exact(&mut SnapshotEncoder::default(), doc());
        for count in [0, 1, 4099, 4, 6] {
            let mut wrong = bytes.clone();
            wrong[20..24].copy_from_slice(&u32::to_le_bytes(count));
            assert!(snapshot_codec::decode(&wrong).is_err(), "count {count}");
        }
        for end in (6..bytes.len()).step_by(17).chain([bytes.len() - 1]) {
            assert!(
                snapshot_codec::decode(&bytes[..end]).is_err(),
                "truncation {end}"
            );
        }
        let mut wrong_crc = bytes.clone();
        wrong_crc[16] ^= 1;
        assert!(snapshot_codec::decode(&wrong_crc).is_err());
        let mut wrong_size = bytes.clone();
        wrong_size[8] ^= 1;
        assert!(snapshot_codec::decode(&wrong_size).is_err());
        let mut extra = bytes.clone();
        extra.extend_from_slice(&snapshot_codec::encode_frame(b"").unwrap());
        assert!(snapshot_codec::decode(&extra).is_err());
    }
}
