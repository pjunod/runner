//! Explicit article-range work and single-writer assembly.

use nzbd_types::{FileId, Job, JobId, JobStatus, SegmentState};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const TARGET_ARTICLES: usize = 32;
pub const RANGE_ASSIGNEE: &str = "__cluster_ranges__";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeScope {
    pub job_id: u32,
    pub file_id: u32,
    pub first_article: u32,
    pub last_article: u32,
    pub range_index: u32,
    pub range_count: u32,
}

pub fn scopes(job: &Job) -> Vec<RangeScope> {
    let Some(file) = job.files.iter().find(|file| !file.paused) else {
        return Vec::new();
    };
    if job.files.iter().filter(|file| !file.paused).count() != 1
        || file.segments.len() < TARGET_ARTICLES * 2
    {
        return Vec::new();
    }
    let chunks = file.segments.len().div_ceil(TARGET_ARTICLES);
    file.segments
        .chunks(TARGET_ARTICLES)
        .enumerate()
        .map(|(index, chunk)| RangeScope {
            job_id: job.id.0,
            file_id: file.id.0,
            first_article: chunk.first().unwrap().number,
            last_article: chunk.last().unwrap().number,
            range_index: index as u32,
            range_count: chunks as u32,
        })
        .collect()
}

pub fn resource(scope: &RangeScope) -> String {
    format!(
        "work/job/{}/file/{}/articles/{}-{}",
        scope.job_id, scope.file_id, scope.first_article, scope.last_article
    )
}

pub fn work_view(mut job: Job, scope: &RangeScope, fence: u64) -> Result<Job, String> {
    let file = job
        .files
        .iter()
        .find(|file| file.id == FileId(scope.file_id))
        .cloned()
        .ok_or_else(|| "range file disappeared".to_owned())?;
    let mut file = file;
    file.segments.retain(|segment| {
        segment.number >= scope.first_article && segment.number <= scope.last_article
    });
    if file.segments.is_empty() {
        return Err("range contains no articles".to_owned());
    }
    file.finalized = false;
    file.crc32 = None;
    job.files = vec![file];
    job.dir_name = format!(
        ".nzbd-cluster/range-work/job-{}/file-{}/{}-{}-fence-{fence}",
        scope.job_id, scope.file_id, scope.first_article, scope.last_article
    );
    job.params.retain(|(key, _)| key != "*Cluster:range");
    job.params.push((
        "*Cluster:range".into(),
        serde_json::to_string(scope).map_err(|e| e.to_string())?,
    ));
    job.status = JobStatus::Queued;
    nzbd_engine::queue::recompute_job_totals(&mut job);
    Ok(job)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcceptedRange {
    pub scope: RangeScope,
    pub result_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssembleScope {
    pub job_id: u32,
    pub file_id: u32,
    pub ranges: Vec<AcceptedRange>,
}

pub fn assemble(
    mut job: Job,
    scope: &AssembleScope,
    dest_dir: &Path,
    fence: u64,
) -> Result<Job, String> {
    if scope.job_id != job.id.0 {
        return Err("assembly scope names another job".into());
    }
    let expected_scopes = scopes(&job);
    if expected_scopes.is_empty() || expected_scopes.len() != scope.ranges.len() {
        return Err("assembly does not contain the exact expected range set".into());
    }
    let expected_resources: std::collections::HashSet<_> =
        expected_scopes.iter().map(resource).collect();
    let supplied_resources: std::collections::HashSet<_> = scope
        .ranges
        .iter()
        .map(|accepted| resource(&accepted.scope))
        .collect();
    if expected_resources != supplied_resources || supplied_resources.len() != scope.ranges.len() {
        return Err("assembly ranges overlap, duplicate, or leave a gap".into());
    }
    let original_dir = job.dir_name.clone();
    let file_index = job
        .files
        .iter()
        .position(|file| file.id == FileId(scope.file_id))
        .ok_or_else(|| "assembly file disappeared".to_owned())?;
    let filename = job.files[file_index].filename.clone();
    let advertised_crc = job.files[file_index].crc32;
    let private_dir = dest_dir.join(format!(
        ".nzbd-cluster/assembly/job-{}/file-{}/fence-{fence}",
        scope.job_id, scope.file_id
    ));
    std::fs::create_dir_all(&private_dir)
        .map_err(|error| format!("create assembly directory: {error}"))?;
    let building = private_dir.join(format!("{filename}.building"));
    let final_path = private_dir.join(&filename);
    let mut output = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&building)
        .map_err(|error| format!("create assembly output: {error}"))?;

    let mut terminal = std::collections::HashMap::new();
    let mut file_size = 0u64;
    let mut spans = Vec::new();
    for accepted in &scope.ranges {
        if accepted.scope.job_id != scope.job_id || accepted.scope.file_id != scope.file_id {
            return Err("accepted range identity does not match assembly".into());
        }
        let root = PathBuf::from(&accepted.result_ref);
        let partial: Job = serde_json::from_slice(
            &std::fs::read(root.join("job.json"))
                .map_err(|error| format!("read accepted range job: {error}"))?,
        )
        .map_err(|error| format!("decode accepted range job: {error}"))?;
        let partial_file = partial
            .files
            .iter()
            .find(|file| file.id == FileId(scope.file_id))
            .ok_or_else(|| "accepted range names another file".to_owned())?;
        if partial.id != job.id || partial.files.len() != 1 || partial_file.filename != filename {
            return Err("accepted range job identity mismatch".into());
        }
        let mut input = File::open(root.join("files").join(&partial_file.filename))
            .map_err(|error| format!("open accepted range payload: {error}"))?;
        for segment in &partial_file.segments {
            if segment.number < accepted.scope.first_article
                || segment.number > accepted.scope.last_article
            {
                return Err(format!(
                    "article {} escaped its accepted range",
                    segment.number
                ));
            }
            let SegmentState::Done { offset, len, crc } = segment.state else {
                return Err(format!(
                    "article {} is not durably complete",
                    segment.number
                ));
            };
            let mut data = vec![0u8; len as usize];
            input
                .seek(SeekFrom::Start(offset))
                .and_then(|_| input.read_exact(&mut data))
                .map_err(|error| format!("read article {}: {error}", segment.number))?;
            if crc32fast::hash(&data) != crc {
                return Err(format!("article {} CRC mismatch", segment.number));
            }
            output
                .seek(SeekFrom::Start(offset))
                .and_then(|_| output.write_all(&data))
                .map_err(|error| format!("write article {}: {error}", segment.number))?;
            file_size = file_size.max(offset.saturating_add(u64::from(len)));
            spans.push((
                offset,
                offset.saturating_add(u64::from(len)),
                segment.number,
            ));
            if terminal.insert(segment.number, segment.state).is_some() {
                return Err(format!(
                    "article {} was supplied more than once",
                    segment.number
                ));
            }
        }
    }
    let expected = &mut job.files[file_index];
    for segment in &mut expected.segments {
        segment.state = terminal
            .remove(&segment.number)
            .ok_or_else(|| format!("article {} has no accepted range", segment.number))?;
    }
    if !terminal.is_empty() {
        return Err("assembly supplied articles outside the authoritative file".into());
    }
    spans.sort_unstable_by_key(|(start, _, _)| *start);
    let mut cursor = 0u64;
    for (start, end, number) in &spans {
        if *start != cursor || end <= start {
            return Err(format!(
                "article {number} creates an overlap or byte gap at {cursor}"
            ));
        }
        cursor = *end;
    }
    if cursor != file_size {
        return Err("assembled byte coverage is incomplete".into());
    }
    output
        .set_len(file_size)
        .and_then(|_| output.sync_all())
        .map_err(|error| format!("flush assembly: {error}"))?;
    std::fs::rename(&building, &final_path)
        .map_err(|error| format!("publish private assembly: {error}"))?;
    std::fs::File::open(&private_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("flush private assembly directory: {error}"))?;
    let mut data = Vec::new();
    File::open(&final_path)
        .and_then(|mut file| file.read_to_end(&mut data))
        .map_err(|error| format!("read assembled output: {error}"))?;
    let whole_crc = crc32fast::hash(&data);
    if advertised_crc.is_some_and(|expected| expected != whole_crc) {
        return Err("assembled file CRC does not match the advertised whole-file CRC".into());
    }
    expected.crc32 = Some(whole_crc);
    expected.finalized = true;
    job.files.retain(|file| file.id == FileId(scope.file_id));
    job.dir_name = private_dir
        .strip_prefix(dest_dir)
        .map_err(|_| "assembly path escaped destination".to_owned())?
        .to_string_lossy()
        .into_owned();
    job.status = JobStatus::Completed;
    job.params
        .push(("*Cluster:original-dir".into(), original_dir));
    nzbd_engine::queue::recompute_job_totals(&mut job);
    Ok(job)
}

pub fn is_split_candidate(job: &Job) -> bool {
    !scopes(job).is_empty()
}

pub fn assembly_resource(job: JobId, file: FileId) -> String {
    format!("work/job/{}/file/{}/assemble", job.0, file.0)
}
