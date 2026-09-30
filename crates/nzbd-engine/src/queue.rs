//! Queue state: admission, priority selection, totals/health accounting.
//!
//! Selection order preserves NZBGet semantics (ARCHITECTURE.md §8.2):
//! highest-priority schedulable job → first incomplete non-paused file →
//! next pending segment; force priority (≥900) bypasses every pause;
//! `PropagationDelay` filters too-young files; the failover ladder decides
//! which servers may take the segment at its current tier.

use crate::backend::torrent_wants_download_slot;
use crate::failover::{Candidates, Ladder, SegmentAttempt};
use crate::AddOpts;
use nzbd_nzb::ParsedNzb;
use nzbd_state::{PendingAdmission, QueueSnapshotDoc, QUEUE_SCHEMA_VERSION};
use nzbd_types::{
    DupeInfo, FileEntry, FileId, Health, Job, JobId, JobKind, JobStatus, Segment, SegmentState,
    ServerDef, ServerId, TorrentRecord, TorrentSource,
};
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct QueueState {
    pub jobs: Vec<Job>,
    pub pending_admissions: Vec<PendingAdmission>,
    pub next_job_id: u32,
    pub next_file_id: u32,
    pub download_paused: bool,
    pub speed_limit_bps: Option<u64>,
    /// How many jobs may download at once. `1` is the historical
    /// behavior: one job takes every connection until it runs out of
    /// pending segments.
    ///
    /// Held here rather than on `Tuning` because it is adjustable while
    /// the daemon runs, like the speed limit — and for the same reason,
    /// it is the sort of thing you change *because* of what the queue is
    /// doing right now, which is exactly when a restart is unwelcome.
    pub max_active_downloads: u32,
}

#[derive(Debug)]
pub(crate) struct TorrentAdmissionCommit {
    pub name: String,
    pub category: Option<String>,
    pub priority: i32,
    pub paused: bool,
    pub params: Vec<(String, String)>,
    pub record: TorrentRecord,
}

/// The floor and ceiling for [`QueueState::max_active_downloads`]. Zero
/// would be indistinguishable from a paused queue while presenting itself
/// as a concurrency setting; the ceiling is high enough to be no real
/// limit and low enough that a typo cannot ask for a rotation over ten
/// thousand jobs on every lease.
pub const MIN_ACTIVE_DOWNLOADS: u32 = 1;
pub const MAX_ACTIVE_DOWNLOADS: u32 = 100;

pub fn clamp_active_downloads(n: u32) -> u32 {
    n.clamp(MIN_ACTIVE_DOWNLOADS, MAX_ACTIVE_DOWNLOADS)
}

impl Default for QueueState {
    /// Hand-written rather than derived because `max_active_downloads`
    /// must not default to zero: a derived `0` reads as "no job may
    /// download" and would silently stop a daemon built from a default
    /// state. The type has no safe zero, so it does not get one.
    fn default() -> QueueState {
        QueueState {
            jobs: Vec::new(),
            pending_admissions: Vec::new(),
            next_job_id: 0,
            next_file_id: 0,
            download_paused: false,
            speed_limit_bps: None,
            max_active_downloads: MIN_ACTIVE_DOWNLOADS,
        }
    }
}

/// Coordinates of one segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegRef {
    pub job: JobId,
    pub file: FileId,
    pub seg_number: u32,
}

impl QueueState {
    // -- persistence ---------------------------------------------------------

    /// Production recovery boundary for dormant torrent support. #163 owns
    /// constructing the torrent runtime; until then the generic owner must not
    /// adopt rows it cannot advance.
    pub(crate) fn from_runtime_doc(
        doc: QueueSnapshotDoc,
    ) -> Result<QueueState, nzbd_state::StateError> {
        if doc
            .jobs
            .iter()
            .any(|job| matches!(job.kind, JobKind::Torrent))
        {
            return Err(nzbd_state::StateError::Corrupt(
                "queue.json contains BitTorrent jobs, but this build has no production torrent backend; the queue was left unchanged"
                    .into(),
            ));
        }
        Ok(Self::from_doc(doc))
    }

    pub fn from_doc(doc: QueueSnapshotDoc) -> QueueState {
        let mut state = QueueState {
            jobs: doc.jobs,
            pending_admissions: doc.pending_admissions,
            next_job_id: doc.next_job_id,
            next_file_id: doc.next_file_id,
            download_paused: doc.download_paused,
            speed_limit_bps: doc.speed_limit_bps,
            // A snapshot written before this existed deserializes as 0,
            // which would read as "no job may download". Clamp on the way
            // in so an old queue.json cannot silently stop the daemon.
            max_active_downloads: clamp_active_downloads(doc.max_active_downloads),
        };
        // Leases are transient; anything in flight at the crash re-leases.
        for job in &mut state.jobs {
            for file in &mut job.files {
                for seg in &mut file.segments {
                    if matches!(seg.state, SegmentState::Leased { .. }) {
                        seg.state = SegmentState::Pending;
                    }
                }
            }
            if matches!(job.status, JobStatus::Downloading) {
                job.status = JobStatus::Queued;
            }
            // Backend handles are restored paused. A previous process's
            // activity timestamp must not make the scheduler reject the
            // first start needed to discover peers again.
            if let Some(torrent) = &mut job.torrent {
                if job.status == JobStatus::Queued
                    && torrent.control_intent == nzbd_types::TorrentControlIntent::Running
                    && torrent.ready_at_unix.is_none()
                    && torrent.removal_intent.is_none()
                    && matches!(
                        torrent.phase,
                        nzbd_types::TorrentPhase::Queued
                            | nzbd_types::TorrentPhase::Downloading
                            | nzbd_types::TorrentPhase::Checking
                            | nzbd_types::TorrentPhase::PausedDownload
                    )
                {
                    torrent.phase = nzbd_types::TorrentPhase::Queued;
                    torrent.last_activity_unix = Some(unix_now());
                }
            }
        }
        state.repair_names_and_directories();
        state
    }

    /// Boot repair for a queue admitted by a daemon that let two jobs
    /// share a name and a directory (field report 2026-07-29).
    ///
    /// Two things, both from the job's own stored file list, which is all
    /// the evidence the NZB ever had:
    ///
    /// 1. A job still carrying an open name gets the name its files
    ///    imply. The queue is full of jobs whose par2 set names them
    ///    perfectly and whose title says who asked for them instead.
    /// 2. A duplicate `dir_name` is broken apart — but **only for a job
    ///    that has not written anything yet**. Moving the directory of a
    ///    job with bytes on disk orphans them; those keep the folder they
    ///    have already used and only their display name improves.
    fn repair_names_and_directories(&mut self) {
        for i in 0..self.jobs.len() {
            if !name_is_open(&self.jobs[i]) {
                continue;
            }
            let names: Vec<String> = self.jobs[i]
                .files
                .iter()
                .map(|f| f.filename.clone())
                .collect();
            let Some(real) = name_from_files(&names) else {
                continue;
            };
            let untouched = self.jobs[i].totals.success_articles == 0;
            let was = self.jobs[i].name.clone();
            rename_job(&mut self.jobs[i], real, untouched, false);
            tracing::info!(job = self.jobs[i].id.0, from = %was, to = %self.jobs[i].name,
                "job renamed at boot from its own file list");
        }
        for i in 0..self.jobs.len() {
            let (id, want, untouched) = {
                let j = &self.jobs[i];
                (j.id, j.dir_name.clone(), j.totals.success_articles == 0)
            };
            if want.is_empty() || !untouched {
                continue;
            }
            let unique = self.unique_dir_name(id, want.clone());
            if unique != want {
                tracing::warn!(job = id.0, from = %want, to = %unique,
                    "two jobs shared one download directory; separating them");
                self.jobs[i].dir_name = unique;
            }
        }
    }

    pub fn to_doc(&self) -> QueueSnapshotDoc {
        QueueSnapshotDoc {
            schema_version: QUEUE_SCHEMA_VERSION,
            pending_admissions: self.pending_admissions.clone(),
            jobs: self.jobs.clone(),
            next_job_id: self.next_job_id,
            next_file_id: self.next_file_id,
            download_paused: self.download_paused,
            speed_limit_bps: self.speed_limit_bps,
            max_active_downloads: self.max_active_downloads,
        }
    }

    // -- lookup --------------------------------------------------------------

    pub fn job(&self, id: JobId) -> Option<&Job> {
        self.jobs.iter().find(|j| j.id == id)
    }

    pub fn job_mut(&mut self, id: JobId) -> Option<&mut Job> {
        self.jobs.iter_mut().find(|j| j.id == id)
    }

    pub fn file_mut(&mut self, job: JobId, file: FileId) -> Option<&mut FileEntry> {
        self.job_mut(job)?.files.iter_mut().find(|f| f.id == file)
    }

    pub fn segment_mut(&mut self, r: SegRef) -> Option<&mut Segment> {
        self.file_mut(r.job, r.file)?
            .segments
            .iter_mut()
            .find(|s| s.number == r.seg_number)
    }

    // -- admission -----------------------------------------------------------

    pub fn reserve_torrent_admission(&mut self, source: TorrentSource, opts: AddOpts) -> JobId {
        self.next_job_id += 1;
        let job_id = JobId(self.next_job_id);
        self.pending_admissions.push(PendingAdmission {
            job_id,
            source,
            secret_ref: nzbd_state::torrent_sources::PendingSourceStore::relative_ref(job_id),
            category: opts.category,
            priority: opts.priority,
            paused: opts.paused,
            seed_ratio_limit: opts.seed_ratio_limit,
            seed_time_limit_secs: opts.seed_time_limit_secs,
            stop_seeding_on_complete: opts.stop_seeding_on_complete,
            params: opts.params,
            client: opts.client,
        });
        job_id
    }

    pub fn cancel_torrent_admission(&mut self, id: JobId) -> bool {
        let before = self.pending_admissions.len();
        self.pending_admissions
            .retain(|pending| pending.job_id != id);
        before != self.pending_admissions.len()
    }

    pub(crate) fn commit_torrent_admission(
        &mut self,
        id: JobId,
        commit: TorrentAdmissionCommit,
    ) -> Option<Result<JobId, JobId>> {
        let TorrentAdmissionCommit {
            name,
            category,
            priority,
            paused,
            params,
            record,
        } = commit;
        self.pending_admissions
            .iter()
            .any(|p| p.job_id == id)
            .then_some(())?;
        if let Some(position) = self.jobs.iter().position(|job| {
            job.torrent
                .as_ref()
                .is_some_and(|torrent| torrent.info_hash_v1 == record.info_hash_v1)
                && !matches!(job.status, JobStatus::Failed | JobStatus::Deleted)
        }) {
            let existing_id = self.jobs[position].id;
            let existing = &mut self.jobs[position];
            for (key, value) in params {
                match existing.params.iter_mut().find(|(k, _)| *k == key) {
                    Some(slot) => slot.1 = value,
                    None => existing.params.push((key, value)),
                }
            }
            self.cancel_torrent_admission(id);
            return Some(Err(existing_id));
        }
        self.cancel_torrent_admission(id);
        let dir_name = self.unique_dir_name(id, sanitize_name(&name));
        let total = record.total_bytes;
        self.jobs.push(Job {
            id,
            kind: JobKind::Torrent,
            name,
            dir_name,
            name_provisional: false,
            queued_at_unix: unix_now(),
            original_name: String::new(),
            category,
            priority,
            dupe: DupeInfo::default(),
            params,
            files: Vec::new(),
            totals: nzbd_types::JobTotals {
                size: total,
                ..Default::default()
            },
            status: if paused {
                JobStatus::Paused
            } else {
                JobStatus::Queued
            },
            torrent: Some(record),
            stages: Vec::new(),
        });
        Some(Ok(id))
    }

    /// Add a parsed NZB as a job. `pause_extra_pars` queues `*.volNNN+MM.par2`
    /// files paused (delayed-par download, §3.2 — unpaused by the repair
    /// path in phase 2).
    pub fn admit_nzb(
        &mut self,
        name: String,
        parsed: &ParsedNzb,
        category: Option<String>,
        priority: i32,
        pause_extra_pars: bool,
    ) -> JobId {
        self.next_job_id += 1;
        let job_id = JobId(self.next_job_id);
        let category = category.or_else(|| parsed.meta.category.clone());
        let files = self.build_files(parsed, pause_extra_pars);

        let dir_name = self.unique_dir_name(job_id, sanitize_name(&name));
        let mut job = Job {
            id: job_id,
            kind: JobKind::Nzb,
            name,
            dir_name,
            name_provisional: false,
            queued_at_unix: unix_now(),
            original_name: String::new(),
            category,
            priority,
            dupe: DupeInfo::default(),
            params: Vec::new(),
            files,
            totals: Default::default(),
            status: JobStatus::Queued,
            torrent: None,
            stages: Vec::new(),
        };
        recompute_job_totals(&mut job);
        self.jobs.push(job);
        job_id
    }

    /// A storage directory belongs to exactly one job.
    ///
    /// Two jobs sharing a `dir_name` write into ONE directory: two
    /// releases interleaved in a folder, post-processing verifying and
    /// renaming them as though they were one release, and whichever
    /// finishes second moving the other's files into its category
    /// destination. It happened (field report 2026-07-29): sixteen jobs
    /// all landed in
    /// `/working/monarr/completed/monarr_0.11.0 · drunkenslug · monarr_0/`.
    ///
    /// Names cannot be relied on to be distinct — a provisional name is
    /// built from the client and the indexer, which are *identical* for
    /// every job a given \*arr adds, and two adds of the same release are
    /// legitimate anyway — so the directory is disambiguated by the one
    /// thing that is unique by construction.
    fn unique_dir_name(&self, id: JobId, want: String) -> String {
        if self.jobs.iter().any(|j| j.id != id && j.dir_name == want) {
            format!("{want}.{}", id.0)
        } else {
            want
        }
    }

    /// [`rename_job`] with the directory-uniqueness invariant applied.
    /// The owner loop renames through this, never the free function.
    pub fn set_job_name(&mut self, id: JobId, name: String, storage_too: bool, provisional: bool) {
        let Some(pos) = self.jobs.iter().position(|j| j.id == id) else {
            return;
        };
        rename_job(&mut self.jobs[pos], name, storage_too, provisional);
        if storage_too {
            let want = self.jobs[pos].dir_name.clone();
            self.jobs[pos].dir_name = self.unique_dir_name(id, want);
        }
    }

    /// Register a URL job: no files yet, `Fetching` until the NZB arrives
    /// (then [`QueueState::complete_url_fetch`] fills it in).
    pub fn admit_url(
        &mut self,
        name: String,
        url: &str,
        category: Option<String>,
        priority: i32,
    ) -> JobId {
        self.next_job_id += 1;
        let job_id = JobId(self.next_job_id);
        let dir_name = self.unique_dir_name(job_id, sanitize_name(&name));
        self.jobs.push(Job {
            id: job_id,
            kind: JobKind::Url,
            name,
            dir_name,
            name_provisional: false,
            queued_at_unix: unix_now(),
            original_name: String::new(),
            category,
            priority,
            dupe: DupeInfo::default(),
            params: vec![("*URL".into(), url.to_string())],
            files: Vec::new(),
            totals: Default::default(),
            status: JobStatus::Fetching,
            torrent: None,
            stages: Vec::new(),
        });
        job_id
    }

    /// The fetched NZB for a URL job: populate files and queue it.
    pub fn complete_url_fetch(
        &mut self,
        job_id: JobId,
        parsed: &ParsedNzb,
        pause_extra_pars: bool,
    ) -> bool {
        let files = self.build_files(parsed, pause_extra_pars);
        let meta_category = parsed.meta.category.clone();
        let Some(job) = self.job_mut(job_id) else {
            return false;
        };
        if !matches!(job.status, JobStatus::Fetching) {
            return false;
        }
        if job.category.is_none() {
            job.category = meta_category;
        }
        // URL jobs were named from the URL tail; re-clean now that the
        // NZB's own metadata (meta title, par2 set name) is available.
        //
        // A provisional name offers NO hint — ask the NZB alone.
        //
        // It reads as perfectly informative (that is its design: it is
        // built from the client and the indexer), so passing it as the
        // hint short-circuits `name_from_evidence`, which hands it
        // straight back and the NZB's own naming never gets a hearing.
        // That is how sixteen jobs with correct par2 sets sitting right
        // there ended up titled `monarr/0.11.0 · drunkenslug · monarr/0`
        // (field report 2026-07-29). Passing the client's ORIGINAL name
        // instead is no better: a placeholder only exists because that
        // name was already judged not good enough to keep.
        let hint = if job.name_provisional {
            String::new()
        } else {
            job.name.clone()
        };
        match name_from_evidence(&hint, parsed) {
            // A name out of the job's own documents settles it, and no
            // file has been written yet, so the directory follows.
            Some(real) => rename_job(job, real, true, false),
            // Nothing better in the NZB either: a provisional name stays
            // provisional (the owner loop re-offers one), and a job that
            // never had one keeps the old behaviour.
            None if !job.name_provisional => job.name = clean_job_name(&hint, parsed),
            None => {}
        }
        job.files = files;
        job.status = JobStatus::Queued;
        recompute_job_totals(job);
        true
    }

    fn build_files(&mut self, parsed: &ParsedNzb, pause_extra_pars: bool) -> Vec<FileEntry> {
        let mut files = Vec::with_capacity(parsed.files.len());
        let mut seen_names: HashMap<String, u32> = HashMap::new();
        for pf in &parsed.files {
            self.next_file_id += 1;
            let file_id = FileId(self.next_file_id);
            let mut filename = sanitize_name(&pf.filename_hint());
            // Disambiguate duplicate names inside one NZB (would clobber on disk).
            let n = seen_names.entry(filename.to_lowercase()).or_insert(0);
            *n += 1;
            if *n > 1 {
                filename = format!("{}.dup{}", filename, *n - 1);
            }
            let lower = filename.to_lowercase();
            let is_par2 = lower.ends_with(".par2");
            let is_extra_par = is_par2 && lower.contains(".vol");

            files.push(FileEntry {
                id: file_id,
                subject: pf.subject.clone(),
                filename,
                filename_confirmed: false,
                is_par2,
                paused: pause_extra_pars && is_extra_par,
                groups: pf.groups.clone(),
                date: pf.date,
                segments: pf
                    .segments
                    .iter()
                    .map(|s| Segment {
                        message_id: s.message_id.clone().into_boxed_str(),
                        number: s.number,
                        size: s.bytes.min(u32::MAX as u64) as u32,
                        state: SegmentState::Pending,
                    })
                    .collect(),
                crc32: None,
                finalized: false,
            });
        }
        files
    }

    // -- accounting ----------------------------------------------------------

    pub fn recompute_all_totals(&mut self) {
        for job in &mut self.jobs {
            recompute_job_totals(job);
        }
    }

    /// Bytes still to fetch (pending + leased, non-paused files, active jobs).
    pub fn remaining_bytes(&self) -> u64 {
        self.jobs
            .iter()
            .filter(|j| {
                matches!(
                    j.status,
                    JobStatus::Queued | JobStatus::Downloading | JobStatus::Paused
                )
            })
            .map(|j| {
                j.files
                    .iter()
                    .filter(|f| !f.paused)
                    .flat_map(|f| &f.segments)
                    .filter(|s| {
                        matches!(s.state, SegmentState::Pending | SegmentState::Leased { .. })
                    })
                    .map(|s| s.size as u64)
                    .sum::<u64>()
            })
            .sum()
    }
}

pub fn recompute_job_totals(job: &mut Job) {
    let mut t = nzbd_types::JobTotals::default();
    for f in &job.files {
        let par = f.is_par2;
        for s in &f.segments {
            let size = s.size as u64;
            t.size += size;
            t.total_articles += 1;
            if par {
                t.par_size += size;
            }
            match s.state {
                SegmentState::Done { .. } => {
                    t.success_size += size;
                    t.success_articles += 1;
                }
                SegmentState::Failed => {
                    t.failed_size += size;
                    t.failed_articles += 1;
                    if par {
                        t.failed_par_size += size;
                    }
                }
                _ => {}
            }
        }
    }
    job.totals = t;
}

/// Turn whatever an indexer/*arr handed us into a human job name.
///
/// The name a client hands us is a *hint*; the NZB itself is *evidence*.
/// The hint wins whenever it carries real information, but a hint that
/// carries none — a hash, a random token, a posting artifact like `yEnc` —
/// loses to what the document says about itself: its `<meta name>` title,
/// its par2 recovery-set base, then the common stem of its payload files.
pub fn clean_job_name(raw: &str, nzb: &nzbd_nzb::ParsedNzb) -> String {
    let cleaned = strip_name_junk(raw);
    match name_from_evidence(raw, nzb) {
        Some(name) => name,
        None if cleaned.is_empty() => "download".into(),
        None => cleaned,
    }
}

/// The best name the hint and the NZB *between them* can support, or
/// `None` when neither carries any information.
///
/// Separated from [`clean_job_name`] so a caller that has more context —
/// who asked for this job, and where it came from — can supply a better
/// last resort than the hash it was handed. See [`requestor_name`].
pub fn name_from_evidence(raw: &str, nzb: &nzbd_nzb::ParsedNzb) -> Option<String> {
    let cleaned = strip_name_junk(raw);
    if !cleaned.is_empty() && !is_uninformative(&cleaned) {
        return Some(cleaned);
    }
    let evidence = [
        nzb.meta.title.as_deref().map(str::to_string),
        par2_base_name(nzb),
        common_file_stem(nzb),
    ];
    evidence.into_iter().flatten().find_map(|candidate| {
        let c = strip_name_junk(&candidate);
        (!c.is_empty() && !is_uninformative(&c)).then_some(c)
    })
}

/// Is this name one a human learns nothing from? Public so the owner loop
/// can ask before overwriting a name with better evidence.
pub fn is_uninformative_name(name: &str) -> bool {
    let c = strip_name_junk(name);
    c.is_empty() || is_uninformative(&c)
}

/// A name for a job whose own documents gave nothing: say **who asked for
/// it and where it came from**.
///
/// Field report 2026-07-29, job #182: a 4.8 GiB download titled
/// `cc310b9901757996b0bdfd880c666e3812e6531d`, every payload file inside
/// it obfuscated too, so there was no evidence anywhere to name it by —
/// "that is not useful at all". It is not useful, and the honest reason is
/// that at admission nobody knows what it is yet. But somebody *asked* for
/// it, from *somewhere*, and both of those are facts we hold. `monarr ·
/// drunkenslug · cc310b99` answers "what is this and why is it here" well
/// enough to act on, which a bare hash never does.
///
/// This is a placeholder and is meant to be replaced: the job renames
/// itself the moment its par2 metadata arrives (see
/// `Owner::on_writer_finalized`). Returns `None` when there is no context
/// either — then the hash is genuinely all anyone has, and inventing
/// something would be worse.
pub fn requestor_name(job: &Job) -> Option<String> {
    let param = |k: &str| {
        job.params
            .iter()
            .find(|(pk, _)| pk == k)
            .map(|(_, v)| v.as_str())
    };
    let mut parts: Vec<String> = Vec::new();
    if let Some(client) = param(CLIENT_PARAM).filter(|c| !c.is_empty()) {
        parts.push(client.to_string());
    } else if let Some(cat) = job.category.as_deref().filter(|c| !c.is_empty()) {
        parts.push(cat.to_string());
    }
    if let Some(host) = param("*URL").and_then(url_host) {
        parts.push(host);
    }
    if parts.is_empty() {
        return None;
    }
    // A short id so two of these are still tellable apart, and so the row
    // can be matched back to the indexer link by eye.
    //
    // Read from what the CLIENT called this job, never from `job.name` —
    // which is the field this function's own result overwrites. Fed its
    // own output it converges: the first eight characters of
    // `monarr/0.11.0 · drunkenslug · e44dbc35` are `monarr/0`, so the
    // second call produces `monarr/0.11.0 · drunkenslug · monarr/0`, and
    // so does every call after it — for every job from that client and
    // indexer. Sixteen jobs with one name, and one directory between them
    // (field report 2026-07-29).
    let ident = strip_name_junk(if job.original_name.is_empty() {
        &job.name
    } else {
        &job.original_name
    });
    let short: String = ident.chars().take(8).collect();
    // The client's label may be missing, generic, or the same word we
    // already used. The job id is none of those.
    if short.is_empty() || parts.iter().any(|p| p.eq_ignore_ascii_case(&short)) {
        parts.push(format!("#{}", job.id.0));
    } else {
        parts.push(short);
    }
    Some(parts.join(" · "))
}

/// `https://drunkenslug.com/getnzb/…` → `drunkenslug`. The registrable
/// label, not the full host: `api.indexer.example.co.uk` reads as
/// `indexer`, which is the part an operator recognises.
///
/// `None` for an IP literal. `127.0.0.1` has no label that means anything
/// to a human, and picking one out of it yields `0` — which is how this
/// first went wrong.
fn url_host(url: &str) -> Option<String> {
    let rest = url.split("://").nth(1).unwrap_or(url);
    let host = rest.split(['/', '?', '#']).next()?;
    let host = host.rsplit('@').next()?;
    if host.starts_with('[') || host.matches(':').count() > 1 {
        return None; // IPv6 literal
    }
    let host = host.split(':').next()?;
    let labels: Vec<&str> = host
        .split('.')
        .filter(|l| !l.is_empty() && !l.eq_ignore_ascii_case("www"))
        .collect();
    if labels.is_empty() || labels.iter().all(|l| l.parse::<u32>().is_ok()) {
        return None; // empty, or an IPv4 literal
    }
    // Drop the TLD (and a two-part public suffix like `co.uk`).
    let drop = if labels.len() >= 3 && labels[labels.len() - 2].len() <= 3 {
        2
    } else {
        1
    };
    let idx = labels.len().saturating_sub(drop + 1);
    labels
        .get(idx)
        .filter(|l| l.parse::<u32>().is_err())
        .map(|s| s.to_string())
}

/// The job param carrying the client that asked for this job.
///
/// Reserved (`*`-prefixed) like `*URL`, so a client cannot set it on
/// itself through the public params surface.
pub const CLIENT_PARAM: &str = "*Client";

/// Adopt a better name.
///
/// `storage_too` moves the on-disk directory name with it, which is only
/// safe before any file has been written — at admission, or when a URL
/// job's NZB lands. Once writers exist the display name may still improve,
/// but the path it writes to must not move underneath them.
pub fn rename_job(job: &mut Job, new_name: String, storage_too: bool, provisional: bool) {
    if new_name.is_empty() || new_name == job.name {
        return;
    }
    // Remember what the client called it the FIRST time it changes. An
    // *arr that added `cc310b99…` and later reads history looking for it
    // must still be able to find the row.
    if job.original_name.is_empty() {
        job.original_name = job.name.clone();
    }
    job.name = new_name;
    job.name_provisional = provisional;
    if storage_too {
        job.dir_name = sanitize_name(&job.name);
    }
}

/// Is this job still waiting for a name it can call its own?
///
/// Two ways to be: nothing has replaced the junk it was admitted with, or
/// something has but only provisionally. Both are open to a better answer;
/// a real name from the job's own documents closes it for good.
pub fn name_is_open(job: &Job) -> bool {
    job.name_provisional || is_uninformative_name(&job.name)
}

/// The name a set of real filenames implies: their common stem, cut back
/// off the volume counter. `X.part01.rar` + `X.part02.rar` → `X`.
///
/// Shared by the NZB-time guess (payload subjects) and the par2-time
/// answer (FileDesc packets), because it is the same question asked of two
/// different sources.
pub fn common_stem(names: &[String]) -> Option<String> {
    let names: Vec<&String> = names
        .iter()
        .filter(|n| {
            let l = n.to_ascii_lowercase();
            !(l.ends_with(".par2") || l.ends_with(".nfo") || l.ends_with(".sfv"))
        })
        .collect();
    if names.is_empty() {
        return None;
    }
    if names.len() == 1 {
        // One payload file names the job by itself, minus its extension —
        // a single .mkv is the commonest shape there is.
        let n = names[0];
        let stem = n.rsplit_once('.').map_or(n.as_str(), |(base, ext)| {
            if ext.len() <= 4 && ext.chars().all(|c| c.is_ascii_alphanumeric()) {
                base
            } else {
                n.as_str()
            }
        });
        return (stem.chars().count() >= 4).then(|| stem.to_string());
    }
    let mut prefix: Vec<char> = names[0].chars().collect();
    for n in &names[1..] {
        let common = prefix
            .iter()
            .zip(n.chars())
            .take_while(|(a, b)| **a == *b)
            .count();
        prefix.truncate(common);
        if prefix.is_empty() {
            return None;
        }
    }
    // `Show.part0` → drop the counter, the volume word, then the separator.
    let s: String = prefix.into_iter().collect();
    let s = s.trim_end_matches(|c: char| c.is_ascii_digit());
    let s = s.trim_end_matches(['.', '-', '_', ' ']);
    let lower = s.to_ascii_lowercase();
    let s = [".part", ".vol", ".disc", ".cd", ".r", ".s"]
        .iter()
        .find(|w| lower.ends_with(*w))
        .map_or(s, |w| &s[..s.len() - w.len()]);
    let s = s.trim_end_matches(['.', '-', '_', ' ']);
    (s.chars().count() >= 4).then(|| s.to_string())
}

/// The name a par2 recovery set gives its own contents, if it is one a
/// human learns anything from.
pub fn name_from_par2(descs: &[nzbd_par2::FileDesc]) -> Option<String> {
    let names: Vec<String> = descs.iter().map(|d| d.name.clone()).collect();
    let candidate = common_stem(&names)?;
    let c = strip_name_junk(&candidate);
    (!c.is_empty() && !is_uninformative(&c)).then_some(c)
}

/// URL tail → name: cut glued query params, drop the extension, decode.
/// Also used at `add_url` time so even the `Fetching` placeholder job never
/// shows raw query junk (or the API key riding in it).
pub(crate) fn strip_name_junk(raw: &str) -> String {
    let mut name = raw.trim();
    // Full URL? Take the path's last segment.
    if name.starts_with("http://") || name.starts_with("https://") {
        let no_query = name.split(['?', '#']).next().unwrap_or(name);
        name = no_query.rsplit('/').next().unwrap_or(no_query);
    }
    // Query string glued onto a filename: cut at '?', or at '&' when the
    // remainder looks like k=v pairs (release names may contain a bare &).
    let mut cut = name.len();
    if let Some(q) = name.find('?') {
        cut = q;
    }
    let mut search = 0;
    while let Some(a) = name[search..cut].find('&') {
        let at = search + a;
        let rest = &name[at + 1..cut.min(name.len())];
        let param_like = rest
            .split('&')
            .next()
            .is_some_and(|p| p.contains('=') && !p.split('=').next().unwrap_or("").contains(' '));
        if param_like {
            cut = at;
            break;
        }
        search = at + 1;
    }
    let mut name = percent_decode(name[..cut].trim());
    for ext in [".nzb", ".NZB", ".Nzb"] {
        if let Some(stripped) = name.strip_suffix(ext) {
            name = stripped.to_string();
        }
    }
    name.trim().trim_matches('.').trim().to_string()
}

fn percent_decode(s: &str) -> String {
    if !s.contains('%') {
        return s.to_string();
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A name that tells a human nothing: a hash, a random token, or a Usenet
/// posting artifact. Any of these should lose to the NZB's own evidence.
fn is_uninformative(name: &str) -> bool {
    looks_obfuscated(name) || looks_like_token(name) || is_posting_artifact(name)
}

/// Hash-looking single token: long, one "word", overwhelmingly hex.
fn looks_obfuscated(name: &str) -> bool {
    let core: String = name.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
    if core.len() < 16 || name.contains(' ') {
        return false;
    }
    // Real release names carry dots/spaces and words; hashes don't.
    let hexish = core.chars().filter(|c| c.is_ascii_hexdigit()).count();
    let word_chars = name.chars().filter(|c| c.is_ascii_alphabetic()).count();
    hexish * 10 >= core.len() * 9 && word_chars * 3 < core.len() * 2
}

/// A long opaque token — base64/base62 ids like `UcsRDCyhGHPCP2TqBJWrnUg`
/// that `looks_obfuscated` misses because they are not hex. A scene name
/// always carries separators (`Show.S01E01.1080p`, `Some Movie 2024`); a
/// 16+ character run with none at all is an id, not a title.
fn looks_like_token(name: &str) -> bool {
    let n = name.trim();
    n.chars().count() >= 16
        && !n
            .chars()
            .any(|c| matches!(c, ' ' | '.' | '-' | '_' | '(' | ')' | '[' | ']'))
        && n.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '/' | '='))
}

/// Everything left after dropping Usenet posting furniture — the `yEnc`
/// marker and bare part counters (`(1/44)`, `[01/44]`) — is nothing.
///
/// Field report 2026-07-27: monarr handed us `yEnc` as the NZB filename and
/// eight jobs in a row were titled `yEnc` while their own file list read
/// `Bates.Motel.S01E07.720p.WEB-DL.DD5.1.H.264-KiNGS`.
fn is_posting_artifact(name: &str) -> bool {
    let informative: usize = name
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|t| {
            !t.is_empty()
                && !t.eq_ignore_ascii_case("yenc")
                && !t.chars().all(|c| c.is_ascii_digit())
        })
        .map(|t| t.len())
        .sum();
    informative < 3
}

/// Recovery-set base: the main `<base>.par2` (not `.volXX+YY`) filename.
fn par2_base_name(nzb: &nzbd_nzb::ParsedNzb) -> Option<String> {
    let names: Vec<String> = nzb.files.iter().map(|f| f.filename_hint()).collect();
    par2_base_of(&names)
}

/// The best name a set of *filenames* supports — the recovery-set base
/// first, then their common stem.
///
/// The same evidence pass [`name_from_evidence`] runs, asked of a job's
/// own stored file list rather than of a parsed NZB. That list survives a
/// restart, so a job admitted under a name it should never have kept can
/// still be told what it is (see `repair_names_and_directories`).
pub fn name_from_files(names: &[String]) -> Option<String> {
    [par2_base_of(names), common_stem(names)]
        .into_iter()
        .flatten()
        .find_map(|candidate| {
            let c = strip_name_junk(&candidate);
            (!c.is_empty() && !is_uninformative(&c)).then_some(c)
        })
}

fn par2_base_of(names: &[String]) -> Option<String> {
    let mut best: Option<String> = None;
    for hint in names {
        let lower = hint.to_ascii_lowercase();
        if !lower.ends_with(".par2") {
            continue;
        }
        let base = &hint[..hint.len() - 5];
        // Prefer the main par2 (no .volXX+YY suffix).
        if let Some(vol) = base.to_ascii_lowercase().rfind(".vol") {
            let candidate = base[..vol].to_string();
            best.get_or_insert(candidate);
        } else {
            return Some(base.to_string());
        }
    }
    best
}

/// Last resort when there is no par2 set and no meta title: the common
/// prefix of the payload filenames, cut back off the volume counter.
/// `X.part01.rar` + `X.part02.rar` → `X`.
fn common_file_stem(nzb: &nzbd_nzb::ParsedNzb) -> Option<String> {
    let names: Vec<String> = nzb.files.iter().map(|f| f.filename_hint()).collect();
    common_stem(&names)
}

/// The directory a job's files live under, relative to the destination.
///
/// Always go through this rather than `sanitize_name(&job.name)`. The two
/// agree for every job admitted since `dir_name` existed, and for jobs
/// restored from an older snapshot this falls back to exactly what they
/// already used — but only this one honours a job that has since renamed
/// itself, and a writer that disagrees with the rest of the daemon about
/// where a file goes is the bug the split exists to prevent.
pub fn job_dir_name(job: &Job) -> String {
    if job.dir_name.is_empty() {
        sanitize_name(&job.name)
    } else {
        job.dir_name.clone()
    }
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Filesystem-safe job/file names (path separators and control chars out).
pub fn sanitize_name(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\0' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();
    let trimmed = cleaned.trim().trim_matches('.').trim();
    let out = if trimmed.is_empty() {
        "unnamed"
    } else {
        trimmed
    };
    // Keep well under PATH_MAX with room for ".part"/dup suffixes.
    out.chars().take(200).collect()
}

// ---------------------------------------------------------------------------
// Selection
// ---------------------------------------------------------------------------

pub struct SelectionCtx<'a> {
    pub ladder: &'a Ladder<'a>,
    pub attempts: &'a mut HashMap<SegRef, SegmentAttempt>,
    pub is_blocked: &'a dyn Fn(ServerId) -> bool,
    /// Jobs executing on another node — invisible to local selection.
    pub delegated: &'a HashMap<JobId, String>,
    /// Delayed-PAR recovery jobs and the exact files the verifier authorized.
    /// Presence also permits a locally assigned PP job through delegation;
    /// the authority copy never has this executor-local entry.
    pub post_fetch_files: &'a HashMap<JobId, HashSet<FileId>>,
    /// False on post-processing-only cluster nodes. Those nodes can serve
    /// only jobs present in `post_fetch_files`.
    pub regular_downloads: bool,
    pub article_retries: u8,
    pub now_unix: i64,
    pub propagation_delay_secs: i64,
    /// Quota reached (or another soft hold): only force-priority jobs run.
    pub soft_hold: bool,
    /// Which job in the active set to begin the scan at. The owner
    /// advances this per granted lease, which is what turns a cap on
    /// concurrent jobs into actual concurrency: without it the first job
    /// in the set would still answer every request and the other slots
    /// would be occupied by jobs receiving nothing.
    pub rotate: usize,
}

pub struct SelectionResult {
    pub lease: Option<SegRef>,
    /// Segments discovered unrecoverable during the scan (all tiers
    /// exhausted) — the owner fails them through the common path.
    pub exhausted: Vec<SegRef>,
}

/// Find the next pending segment `server` may take, in queue priority order.
/// Does not mutate segment states (the owner applies the lease); does
/// escalate per-segment attempt tiers as a side effect of candidate
/// computation (that is the ladder's contract).
pub fn next_for_server(
    state: &QueueState,
    server: &ServerDef,
    ctx: &mut SelectionCtx<'_>,
) -> SelectionResult {
    let mut exhausted = Vec::new();

    let mut order: Vec<&Job> = state
        .jobs
        .iter()
        .filter(|j| !ctx.delegated.contains_key(&j.id) || ctx.post_fetch_files.contains_key(&j.id))
        .filter(|j| ctx.regular_downloads || ctx.post_fetch_files.contains_key(&j.id))
        .filter(|j| job_schedulable(j, state.download_paused || ctx.soft_hold))
        // A job whose every remaining segment is already leased has no
        // work to hand out. Letting it hold an active slot would spend
        // the slot on nothing, and would stall the pipe at the tail of
        // every job — which is the one place nzbd has always overlapped.
        .filter(|j| has_backend_work(j, ctx.now_unix, ctx.post_fetch_files.get(&j.id)))
        .collect();
    // Stable sort: equal priorities keep their queue-vec order, which is
    // user-controlled (move top/up/down/bottom) and persisted.
    order.sort_by_key(|j| scheduling_priority(j, ctx.now_unix));

    // The active set: the highest-priority jobs with work, at most
    // `max_active_downloads` of them. Priority decides WHO downloads;
    // within the set every job gets an equal share of the connections,
    // because "three at once" that silently gave one of them 95% of the
    // pipe would not be three at once.
    let cap = clamp_active_downloads(state.max_active_downloads) as usize;
    order.truncate(cap);

    // Rotate so successive grants start at successive jobs. With a cap of
    // 1 this is a no-op and selection is exactly the head-of-queue scan
    // it has always been.
    if order.len() > 1 {
        let by = ctx.rotate % order.len();
        order.rotate_left(by);
    }

    for job in order {
        let allowed_files = ctx.post_fetch_files.get(&job.id);
        for file in &job.files {
            if file.paused
                || file.is_terminal()
                || allowed_files.is_some_and(|allowed| !allowed.contains(&file.id))
            {
                continue;
            }
            if ctx.propagation_delay_secs > 0 {
                if let Some(date) = file.date {
                    if date + ctx.propagation_delay_secs > ctx.now_unix {
                        continue; // too young: not yet propagated everywhere
                    }
                }
            }
            let age_days = file
                .date
                .map(|d| ((ctx.now_unix - d).max(0) / 86_400) as u32);
            for seg in &file.segments {
                if !matches!(seg.state, SegmentState::Pending) {
                    continue;
                }
                let r = SegRef {
                    job: job.id,
                    file: file.id,
                    seg_number: seg.number,
                };
                let att = ctx
                    .attempts
                    .entry(r)
                    .or_insert_with(|| SegmentAttempt::new(ctx.article_retries));
                match ctx.ladder.current_candidates(att, ctx.is_blocked, age_days) {
                    Candidates::Servers(ids) if ids.contains(&server.id) => {
                        return SelectionResult {
                            lease: Some(r),
                            exhausted,
                        };
                    }
                    Candidates::Servers(_) | Candidates::WaitForBlocked => {}
                    Candidates::Exhausted => exhausted.push(r),
                }
            }
        }
    }
    SelectionResult {
        lease: None,
        exhausted,
    }
}

/// Does this job have at least one segment that could be handed out?
/// Short-circuits on the first hit, so the common "yes" case is cheap.
fn has_backend_work(job: &Job, now_unix: i64, allowed_files: Option<&HashSet<FileId>>) -> bool {
    match job.kind {
        JobKind::Torrent => job.torrent.as_ref().is_some_and(|torrent| {
            torrent_wants_download_slot(torrent, job.queued_at_unix, now_unix)
        }),
        JobKind::Nzb | JobKind::Url => job.files.iter().any(|f| {
            !f.paused
                && !f.is_terminal()
                && allowed_files.is_none_or(|allowed| allowed.contains(&f.id))
                && f.segments
                    .iter()
                    .any(|s| matches!(s.state, SegmentState::Pending))
        }),
    }
}

/// Is any segment of this job currently out with a connection? A job with
/// work in flight is downloading whatever its position says.
pub fn has_leased(job: &Job) -> bool {
    job.files.iter().any(|f| {
        f.segments
            .iter()
            .any(|s| matches!(s.state, SegmentState::Leased { .. }))
    })
}

/// An automatic retry of an idle torrent may use spare capacity, but cannot
/// preempt fresh work and renew its discovery grace indefinitely. Startup
/// and explicit user resume refresh the clock before entering this queue.
fn scheduling_priority(job: &Job, now_unix: i64) -> (bool, std::cmp::Reverse<i32>) {
    let stalled_retry = job.torrent.as_ref().is_some_and(|torrent| {
        torrent.phase == nzbd_types::TorrentPhase::Queued
            && torrent.last_activity_unix.is_some_and(|last| {
                now_unix.saturating_sub(last) >= crate::backend::STALLED_SLOT_YIELD_SECS
            })
    });
    (stalled_retry, std::cmp::Reverse(job.priority))
}

/// The jobs that may download right now, highest priority first. Shares
/// its filter and ordering with [`next_for_server`] so the set the
/// scheduler feeds and the set the status labels reflect cannot drift.
pub fn active_set(
    state: &QueueState,
    delegated: &HashMap<JobId, String>,
    soft_hold: bool,
    now_unix: i64,
) -> Vec<JobId> {
    active_set_with_recovery(state, delegated, &HashMap::new(), true, soft_hold, now_unix)
}

pub(crate) fn active_set_with_recovery(
    state: &QueueState,
    delegated: &HashMap<JobId, String>,
    post_fetch_files: &HashMap<JobId, HashSet<FileId>>,
    regular_downloads: bool,
    soft_hold: bool,
    now_unix: i64,
) -> Vec<JobId> {
    let mut order: Vec<&Job> = state
        .jobs
        .iter()
        .filter(|j| !delegated.contains_key(&j.id) || post_fetch_files.contains_key(&j.id))
        .filter(|j| regular_downloads || post_fetch_files.contains_key(&j.id))
        .filter(|j| job_schedulable(j, state.download_paused || soft_hold))
        .filter(|j| has_backend_work(j, now_unix, post_fetch_files.get(&j.id)))
        .collect();
    order.sort_by_key(|j| scheduling_priority(j, now_unix));
    order.truncate(clamp_active_downloads(state.max_active_downloads) as usize);
    order.iter().map(|j| j.id).collect()
}

/// Jobs claiming to download that are neither in the active set nor
/// holding a lease — the ones whose label has outlived the fact.
///
/// Pure and separate from the owner so the rule can be tested without
/// standing up an engine: it is a statement about queue state, not about
/// scheduling machinery.
pub fn jobs_to_requeue(
    state: &QueueState,
    delegated: &HashMap<JobId, String>,
    soft_hold: bool,
    now_unix: i64,
) -> Vec<JobId> {
    jobs_to_requeue_with_recovery(state, delegated, &HashMap::new(), true, soft_hold, now_unix)
}

pub(crate) fn jobs_to_requeue_with_recovery(
    state: &QueueState,
    delegated: &HashMap<JobId, String>,
    post_fetch_files: &HashMap<JobId, HashSet<FileId>>,
    regular_downloads: bool,
    soft_hold: bool,
    now_unix: i64,
) -> Vec<JobId> {
    let active: Vec<JobId> = active_set_with_recovery(
        state,
        delegated,
        post_fetch_files,
        regular_downloads,
        soft_hold,
        now_unix,
    );
    state
        .jobs
        .iter()
        .filter(|j| matches!(j.status, JobStatus::Downloading))
        .filter(|j| !active.contains(&j.id))
        .filter(|j| !has_leased(j))
        .map(|j| j.id)
        .collect()
}

fn job_schedulable(job: &Job, download_paused: bool) -> bool {
    match job.status {
        JobStatus::Queued | JobStatus::Downloading => job.force_priority() || !download_paused,
        JobStatus::Paused => job.force_priority(),
        _ => false,
    }
}

/// Parse the recovery-block count from a `*.volXX+NN.par2` filename, or
/// from the older `*.volAAA-BBB.par2` range form (inclusive, so `+1`).
pub fn vol_par_blocks(filename: &str) -> Option<u32> {
    let lower = filename.to_ascii_lowercase();
    let vol = lower.rfind(".vol")?;
    let rest = &lower[vol + 4..];
    let digits = |s: &str| -> Option<u32> {
        let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
        s[..end].parse().ok()
    };
    let sep = rest.find(['+', '-'])?;
    let count = digits(&rest[sep + 1..])?;
    if rest.as_bytes()[sep] == b'+' {
        return Some(count);
    }
    // Range form: the first number is the starting block.
    let start = digits(rest)?;
    count.checked_sub(start).map(|n| n + 1)
}

/// A paused par2 file of a job, priced in recovery blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParCandidate {
    pub id: FileId,
    /// Recovery blocks this file is believed to carry. Never 0 for a
    /// priced candidate; `None`-priced files are reported separately.
    pub blocks: u32,
    /// Bytes the NZB says this file is, from its segment sizes.
    pub bytes: u64,
    /// True when `blocks` is a size-derived estimate rather than a count
    /// read out of the filename.
    pub estimated: bool,
}

/// Price every paused par2 file of a job in recovery blocks.
///
/// The fast path is the `*.volXX+NN.par2` marker, which states the block
/// count outright. It is also useless on this indexer: an obfuscated post
/// names its recovery volumes `<32-hex>.par2` like everything else, so the
/// marker is absent on precisely the jobs that need repair — and a filter
/// that drops unpriceable files made `unpause_par_blocks` return 0 for
/// months, turning every repairable download into a PAR_FAILURE.
///
/// So the fallback: a recovery volume of `bytes` bytes carries about
/// `bytes / block_size` blocks (the packet overhead makes that an
/// over-estimate of a block or two, which only costs an extra round). When
/// even the block size is unknown the file is returned unpriced — the
/// caller's last resort is to unpause the smallest one and let the next
/// round escalate.
///
/// Returns (priced, unpriceable), both in job file order.
pub fn price_paused_pars(job: &Job, block_size: Option<u64>) -> (Vec<ParCandidate>, Vec<FileId>) {
    let mut priced = Vec::new();
    let mut unpriceable = Vec::new();
    for f in job.files.iter().filter(|f| f.paused && f.is_par2) {
        let bytes: u64 = f.segments.iter().map(|s| s.size as u64).sum();
        match vol_par_blocks(&f.filename) {
            Some(blocks) => priced.push(ParCandidate {
                id: f.id,
                blocks: blocks.max(1),
                bytes,
                estimated: false,
            }),
            None => match block_size.filter(|b| *b > 0) {
                Some(bs) => priced.push(ParCandidate {
                    id: f.id,
                    blocks: (bytes / bs).max(1).min(u32::MAX as u64) as u32,
                    bytes,
                    estimated: true,
                }),
                None => unpriceable.push(f.id),
            },
        }
    }
    (priced, unpriceable)
}

/// The paused par2 file with the fewest bytes — the cheapest probe when
/// nothing could be priced at all.
pub fn smallest_paused_par(job: &Job) -> Option<FileId> {
    job.files
        .iter()
        .filter(|f| f.paused && f.is_par2)
        .min_by_key(|f| f.segments.iter().map(|s| s.size as u64).sum::<u64>())
        .map(|f| f.id)
}

/// Choose the smallest set of paused par files covering `needed` recovery
/// blocks (NZBGet's delayed-par selection, simplified): prefer the smallest
/// single file that covers it; otherwise accumulate largest-first.
pub fn pick_par_files(candidates: &[(FileId, u32)], needed: u32) -> Vec<FileId> {
    let mut sorted: Vec<_> = candidates.to_vec();
    sorted.sort_by_key(|(_, blocks)| *blocks);
    if let Some((id, _)) = sorted.iter().find(|(_, b)| *b >= needed) {
        return vec![*id];
    }
    let mut out = Vec::new();
    let mut have = 0u32;
    for (id, blocks) in sorted.iter().rev() {
        if have >= needed {
            break;
        }
        out.push(*id);
        have += blocks;
    }
    out
}

/// Health verdict for a finished job: below critical health the download is
/// beyond repair (would be failed/parked by the health check; phase 2 adds
/// the par-aware paths).
pub fn final_status(job: &Job) -> (JobStatus, Health) {
    let health = Health::calc(&job.totals);
    let critical = Health::calc_critical(&job.totals, true);
    if health < critical {
        (JobStatus::Failed, health)
    } else {
        (JobStatus::Completed, health)
    }
}

/// Regenerate a job's NZB from queue state.
///
/// Lives here rather than in the API because two callers need it and they
/// are in different crates: the API's `GET /jobs/{id}/nzb` and delete-park,
/// and post-processing, which spools every finished job's NZB so history
/// can put it back. `nzbd-post` cannot reach `nzbd-api`.
///
/// Round-trips through the real parser (asserted in the API's tests): what
/// comes out is a document nzbd itself would accept.
pub fn job_to_nzb(job: &nzbd_types::Job) -> String {
    use std::fmt::Write as _;
    let esc = |s: &str| {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
    };
    let mut x = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<nzb xmlns=\"http://www.newzbin.com/DTD/2003/nzb\">\n",
    );
    let _ = writeln!(x, "  <head>");
    let _ = writeln!(x, "    <meta type=\"title\">{}</meta>", esc(&job.name));
    if let Some(c) = &job.category {
        let _ = writeln!(x, "    <meta type=\"category\">{}</meta>", esc(c));
    }
    let _ = writeln!(x, "  </head>");
    for f in &job.files {
        let subject = if f.subject.trim().is_empty() {
            &f.filename
        } else {
            &f.subject
        };
        let _ = writeln!(
            x,
            "  <file poster=\"nzbd\" date=\"{}\" subject=\"{}\">",
            f.date.unwrap_or(0),
            esc(subject)
        );
        let _ = writeln!(x, "    <groups>");
        for g in &f.groups {
            let _ = writeln!(x, "      <group>{}</group>", esc(g));
        }
        let _ = writeln!(x, "    </groups>");
        let _ = writeln!(x, "    <segments>");
        for s in &f.segments {
            let _ = writeln!(
                x,
                "      <segment bytes=\"{}\" number=\"{}\">{}</segment>",
                s.size,
                s.number,
                esc(&s.message_id)
            );
        }
        let _ = writeln!(x, "    </segments>");
        let _ = writeln!(x, "  </file>");
    }
    x.push_str("</nzb>\n");
    x
}

#[cfg(test)]
mod tests {
    use super::*;
    use nzbd_types::{CertLevel, PostStage, TlsMode};

    fn nzb_with(files: &[&str], title: Option<&str>) -> nzbd_nzb::ParsedNzb {
        let mut nzb = nzbd_nzb::ParsedNzb::default();
        nzb.meta.title = title.map(String::from);
        for f in files {
            nzb.files.push(nzbd_nzb::ParsedFile {
                subject: format!("desc \"{f}\" yEnc (1/3)"),
                ..Default::default()
            });
        }
        nzb
    }

    #[test]
    fn regenerated_nzb_is_parseable_and_escapes_queue_evidence() {
        let parsed = nzbd_nzb::ParsedNzb {
            meta: nzbd_nzb::NzbMeta {
                title: Some("ignored request title".into()),
                category: Some("ignored category".into()),
                password: None,
            },
            files: vec![nzbd_nzb::ParsedFile {
                subject: "subject \"payload & <one>.bin\" yEnc".into(),
                poster: Some("poster".into()),
                date: Some(1_700_000_000),
                groups: vec!["alt.test & binaries".into(), "alt.<backup>".into()],
                segments: vec![
                    nzbd_nzb::ParsedSegment {
                        message_id: "one&part@example".into(),
                        number: 1,
                        bytes: 123,
                    },
                    nzbd_nzb::ParsedSegment {
                        message_id: "two<part>@example".into(),
                        number: 2,
                        bytes: 456,
                    },
                ],
            }],
        };
        let mut state = QueueState::default();
        let id = state.admit_nzb(
            "Queue & <Title> \"quoted\"".into(),
            &parsed,
            Some("tv & movies".into()),
            0,
            false,
        );
        // Admission deliberately cleans request names; this test starts at
        // the regeneration boundary, whose input is the stored queue value.
        state.job_mut(id).unwrap().name = "Queue & <Title> \"quoted\"".into();

        let xml = job_to_nzb(state.job(id).unwrap());
        assert!(xml.contains("Queue &amp; &lt;Title&gt; &quot;quoted&quot;"));
        assert!(xml.contains("tv &amp; movies"));
        assert!(xml.contains("alt.test &amp; binaries"));
        assert!(xml.contains("alt.&lt;backup&gt;"));
        assert!(xml.contains("one&amp;part@example"));
        assert!(xml.contains("two&lt;part&gt;@example"));
        let reparsed = nzbd_nzb::parse(xml.as_bytes()).unwrap();
        assert_eq!(reparsed.files.len(), 1);
        assert_eq!(reparsed.files[0].date, Some(1_700_000_000));
        assert_eq!(reparsed.files[0].segments.len(), 2);
        assert_eq!(reparsed.files[0].segments[0].number, 1);
        assert_eq!(reparsed.files[0].segments[0].bytes, 123);
        assert_eq!(reparsed.files[0].segments[1].number, 2);
        assert_eq!(reparsed.files[0].segments[1].bytes, 456);
    }

    #[test]
    fn clean_name_strips_glued_indexer_query() {
        // The exact shape *arr + indexers produce.
        let nzb = nzb_with(&[], None);
        assert_eq!(
            clean_job_name(
                "a7709e1cd0b524e6cc3aef1999c99e7d1192c64a.nzb&i=136144&r=104c19dfb6da49e6daff4d158f6e39f7",
                &nzb
            ),
            // Still a hash after stripping, no better source -> cleaned raw.
            "a7709e1cd0b524e6cc3aef1999c99e7d1192c64a"
        );
    }

    #[test]
    fn clean_name_prefers_meta_title_for_obfuscated() {
        let nzb = nzb_with(&[], Some("Some.Show.S01E02.1080p.WEB.x264-GRP"));
        assert_eq!(
            clean_job_name(
                "7e1670882200f843144795b5c064a882811426d8.nzb&i=1&r=abc",
                &nzb
            ),
            "Some.Show.S01E02.1080p.WEB.x264-GRP"
        );
    }

    #[test]
    fn clean_name_falls_back_to_par2_set_name() {
        let nzb = nzb_with(
            &[
                "abadc0ffee123456789.part01.rar",
                "Cool.Movie.2024.1080p-GRP.par2",
                "Cool.Movie.2024.1080p-GRP.vol00+01.par2",
            ],
            None,
        );
        assert_eq!(
            clean_job_name("104c19dfb6da49e6daff4d158f6e39f7", &nzb),
            "Cool.Movie.2024.1080p-GRP"
        );
    }

    #[test]
    fn clean_name_keeps_real_release_names() {
        let nzb = nzb_with(&[], Some("should not be used"));
        assert_eq!(
            clean_job_name("Great.Doc.2023.2160p.WEB-DL.nzb", &nzb),
            "Great.Doc.2023.2160p.WEB-DL"
        );
        // Ampersand inside a real name survives (no k=v after it).
        assert_eq!(
            clean_job_name("Tom & Jerry Collection.nzb", &nzb),
            "Tom & Jerry Collection"
        );
        // Full URL: last path segment, query dropped, percent-decoded.
        assert_eq!(
            clean_job_name(
                "https://indexer.example/get/Nice%20Show%20S02.nzb?apikey=zzz",
                &nzb
            ),
            "Nice Show S02"
        );
    }

    /// Field report 2026-07-27 (screenshot): eight jobs in a row titled
    /// `yEnc`, every one of them from monarr, every one of them listing its
    /// files correctly underneath. The client's filename was posting
    /// furniture; the NZB knew its own name the whole time.
    #[test]
    fn clean_name_rejects_posting_furniture() {
        let nzb = nzb_with(
            &[
                "Bates.Motel.S01E07.720p.WEB-DL.DD5.1.H.264-KiNGS.par2",
                "Bates.Motel.S01E07.720p.WEB-DL.DD5.1.H.264-KiNGS.part01.rar",
            ],
            None,
        );
        let want = "Bates.Motel.S01E07.720p.WEB-DL.DD5.1.H.264-KiNGS";
        assert_eq!(clean_job_name("yEnc", &nzb), want);
        assert_eq!(clean_job_name("yEnc (1/44)", &nzb), want);
        assert_eq!(clean_job_name("[01/44] - yEnc", &nzb), want);
        // A counter on its own is just as empty.
        assert_eq!(clean_job_name("(1/2)", &nzb), want);
    }

    /// Same queue, job 120: a base64-ish id, which the hex test never saw.
    #[test]
    fn clean_name_rejects_opaque_token() {
        let nzb = nzb_with(&[], Some("Some.Movie.2025.2160p.WEB-DL-GRP"));
        assert_eq!(
            clean_job_name("UcsRDCyhGHPCP2TqBJWrnUg", &nzb),
            "Some.Movie.2025.2160p.WEB-DL-GRP"
        );
        // Separators mean it is a title, however terse.
        assert_eq!(
            clean_job_name("Him.2025.UHD.BluRay", &nzb),
            "Him.2025.UHD.BluRay"
        );
    }

    #[test]
    fn clean_name_falls_back_to_common_file_stem() {
        // No meta title, no par2 — the payload still agrees on a stem.
        let nzb = nzb_with(
            &[
                "The.Running.Man.1987.2160p.REMUX-CiNEPHiLES.part01.rar",
                "The.Running.Man.1987.2160p.REMUX-CiNEPHiLES.part02.rar",
                "The.Running.Man.1987.2160p.REMUX-CiNEPHiLES.part03.rar",
            ],
            None,
        );
        assert_eq!(
            clean_job_name("yEnc", &nzb),
            "The.Running.Man.1987.2160p.REMUX-CiNEPHiLES"
        );
        // A trailing year must survive the counter trim.
        let split = nzb_with(&["Blade.Runner.2049.001", "Blade.Runner.2049.002"], None);
        assert_eq!(clean_job_name("yEnc", &split), "Blade.Runner.2049");
    }

    /// Nothing to go on anywhere: keep the junk rather than invent a name,
    /// so the operator can still see what the client actually sent.
    #[test]
    fn clean_name_keeps_junk_when_there_is_no_evidence() {
        let nzb = nzb_with(&[], None);
        assert_eq!(clean_job_name("yEnc", &nzb), "yEnc");
        assert_eq!(clean_job_name("", &nzb), "download");
    }

    fn server(id: u32, tier: u8) -> ServerDef {
        ServerDef {
            id: ServerId(id),
            name: format!("s{id}"),
            host: "h".into(),
            port: 119,
            tls: TlsMode::None,
            username: None,
            password: None,
            active: true,
            tier,
            group: 0,
            fill: false,
            max_connections: 4,
            pipeline_depth: 1,
            retention_days: 0,
            cert_verification: CertLevel::Strict,
        }
    }

    fn sample_nzb(files: &[(&str, u32)]) -> ParsedNzb {
        let mut xml = String::from(r#"<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">"#);
        for (name, segs) in files {
            xml.push_str(&format!(
                r#"<file poster="p" date="1700000000" subject="&quot;{name}&quot; yEnc (1/{segs})"><groups><group>a.b</group></groups><segments>"#
            ));
            for n in 1..=*segs {
                xml.push_str(&format!(
                    r#"<segment bytes="1000" number="{n}">{name}.{n}@x</segment>"#
                ));
            }
            xml.push_str("</segments></file>");
        }
        xml.push_str("</nzb>");
        nzbd_nzb::parse(xml.as_bytes()).unwrap()
    }

    fn admit_fake_torrent(
        queue: &mut QueueState,
        priority: i32,
        phase: nzbd_types::TorrentPhase,
        last_activity_unix: Option<i64>,
    ) -> JobId {
        queue.next_job_id += 1;
        let id = JobId(queue.next_job_id);
        queue.jobs.push(Job {
            id,
            kind: JobKind::Torrent,
            name: "fake torrent".into(),
            dir_name: "fake-torrent".into(),
            name_provisional: false,
            queued_at_unix: 1_799_999_900,
            original_name: String::new(),
            category: None,
            priority,
            dupe: DupeInfo::default(),
            params: Vec::new(),
            files: Vec::new(),
            totals: Default::default(),
            status: JobStatus::Downloading,
            torrent: Some(nzbd_types::TorrentRecord {
                info_hash_v1: "0123456789abcdef0123456789abcdef01234567".into(),
                source: nzbd_types::TorrentSource::Magnet,
                metadata_file: "meta/fake.torrent".into(),
                payload_root: std::path::PathBuf::new(),
                phase,
                control_intent: nzbd_types::TorrentControlIntent::Running,
                removal_intent: None,
                removal_outcome: None,
                removal_confirmed_at_unix: None,
                stop_reason: None,
                files: Vec::new(),
                total_bytes: 100,
                selected_bytes: 100,
                downloaded_bytes: 0,
                uploaded_bytes: 0,
                seeding_seconds: 0,
                ready_at_unix: None,
                content_path: None,
                seed_policy: nzbd_types::SeedPolicy::default(),
                last_activity_unix,
                last_error: None,
            }),
            stages: Vec::new(),
        });
        id
    }

    /// The re-grab loop's first defect: every paused recovery volume of an
    /// obfuscated post is hash-named, `vol_par_blocks` prices none of them,
    /// and the old filter dropped what it could not price — so repair asked
    /// for one block, was told zero were available, and failed with 5 GB of
    /// recovery data sitting paused in the queue.
    #[test]
    fn hash_named_paused_vols_are_priced_by_size() {
        let mut q = QueueState::default();
        // 4 segments × 1000 bytes each; block size 1000 ⇒ 4 blocks apiece.
        let parsed = sample_nzb(&[
            ("data.rar", 3),
            ("aa11bb22.par2", 1),
            ("cc33dd44.par2", 4),
            ("ee55ff66.par2", 8),
        ]);
        let id = q.admit_nzb("job".into(), &parsed, None, 0, true);
        {
            // Admission cannot tell a hash-named volume from a hash-named
            // index, so stand in for what the field shows: the volumes are
            // paused and carry no `.volXX+NN` marker.
            let job = q.job_mut(id).unwrap();
            for f in job.files.iter_mut().skip(2) {
                f.paused = true;
            }
        }
        let job = q.job(id).unwrap();

        // No block size: nothing is priceable, and the caller is told so
        // rather than handed an empty list that reads as "all fetched".
        let (priced, unpriced) = price_paused_pars(job, None);
        assert!(priced.is_empty());
        assert_eq!(unpriced.len(), 2);
        assert_eq!(smallest_paused_par(job), Some(job.files[2].id));

        // With the block size from the job's own par2 index, they price.
        let (priced, unpriced) = price_paused_pars(job, Some(1000));
        assert!(unpriced.is_empty());
        assert_eq!(priced.len(), 2);
        assert_eq!(priced[0].blocks, 4);
        assert_eq!(priced[1].blocks, 8);
        assert!(priced.iter().all(|c| c.estimated));
        // …and pricing feeds selection: one block wanted, cheapest volume
        // that covers it, not the whole 5 GB.
        let pairs: Vec<_> = priced.iter().map(|c| (c.id, c.blocks)).collect();
        assert_eq!(pick_par_files(&pairs, 1), vec![job.files[2].id]);
    }

    /// A file whose name does state its block count keeps stating it — the
    /// estimate is a fallback, never an override.
    #[test]
    fn vol_marker_still_wins_over_the_size_estimate() {
        let mut q = QueueState::default();
        let parsed = sample_nzb(&[("data.rar", 3), ("data.vol00+07.par2", 2)]);
        let id = q.admit_nzb("job".into(), &parsed, None, 0, true);
        let job = q.job(id).unwrap();
        let (priced, unpriced) = price_paused_pars(job, Some(1000));
        assert!(unpriced.is_empty());
        assert_eq!(priced.len(), 1);
        assert_eq!(priced[0].blocks, 7, "the filename said 7, not 2");
        assert!(!priced[0].estimated);
    }

    #[test]
    fn admission_pauses_extra_pars_and_counts() {
        let mut q = QueueState::default();
        let parsed = sample_nzb(&[("data.rar", 3), ("data.par2", 1), ("data.vol00+01.par2", 2)]);
        let id = q.admit_nzb("job".into(), &parsed, None, 0, true);
        let job = q.job(id).unwrap();
        assert_eq!(job.files.len(), 3);
        assert!(!job.files[0].paused);
        assert!(!job.files[1].paused, "main par2 stays active");
        assert!(job.files[1].is_par2);
        assert!(job.files[2].paused, "vol par is delayed");
        assert_eq!(job.totals.total_articles, 6);
        assert_eq!(job.totals.size, 6000);
        assert_eq!(job.totals.par_size, 3000);
    }

    /// Helper: grant `n` leases the way the owner does — rotating the
    /// cursor per lease and marking each segment Leased — and report
    /// which job each went to.
    fn grant(q: &mut QueueState, servers: &[ServerDef], n: usize) -> Vec<JobId> {
        let ladder = Ladder::new(servers);
        let mut attempts = HashMap::new();
        let not_blocked = |_: ServerId| false;
        let no_delegation: HashMap<JobId, String> = HashMap::new();
        let no_recovery: HashMap<JobId, HashSet<FileId>> = HashMap::new();
        let mut out = Vec::new();
        for i in 0..n {
            let mut ctx = SelectionCtx {
                ladder: &ladder,
                attempts: &mut attempts,
                is_blocked: &not_blocked,
                delegated: &no_delegation,
                post_fetch_files: &no_recovery,
                regular_downloads: true,
                article_retries: 3,
                now_unix: 1_800_000_000,
                propagation_delay_secs: 0,
                soft_hold: false,
                rotate: i,
            };
            let Some(r) = next_for_server(q, &servers[0], &mut ctx).lease else {
                break;
            };
            if let Some(seg) = q.segment_mut(r) {
                seg.state = SegmentState::Leased {
                    server: servers[0].id,
                };
            }
            out.push(r.job);
        }
        out
    }

    /// The default is the behavior nzbd has always had: one job takes
    /// every connection until it has nothing left to hand out.
    #[test]
    fn one_active_download_is_head_of_queue() {
        let mut q = QueueState::default();
        assert_eq!(q.max_active_downloads, 1, "the default must not change");
        let a = q.admit_nzb("a".into(), &sample_nzb(&[("a.bin", 4)]), None, 0, true);
        q.admit_nzb("b".into(), &sample_nzb(&[("b.bin", 4)]), None, 0, true);

        let servers = vec![server(1, 0)];
        let got = grant(&mut q, &servers, 4);
        assert_eq!(got, vec![a, a, a, a], "every lease goes to the head job");
    }

    /// Raising the cap actually spreads the connections. A cap that only
    /// widened the eligible set would leave the head job answering every
    /// request while the other slots sat on jobs receiving nothing —
    /// three "active" downloads, one of them moving.
    #[test]
    fn raising_the_cap_spreads_leases_across_jobs() {
        let mut q = QueueState {
            max_active_downloads: 3,
            ..Default::default()
        };
        let a = q.admit_nzb("a".into(), &sample_nzb(&[("a.bin", 6)]), None, 0, true);
        let b = q.admit_nzb("b".into(), &sample_nzb(&[("b.bin", 6)]), None, 0, true);
        let c = q.admit_nzb("c".into(), &sample_nzb(&[("c.bin", 6)]), None, 0, true);
        let d = q.admit_nzb("d".into(), &sample_nzb(&[("d.bin", 6)]), None, 0, true);

        let got = grant(&mut q, &[server(1, 0)], 9);
        assert_eq!(got.len(), 9);
        let count = |j: JobId| got.iter().filter(|&&g| g == j).count();
        assert_eq!(count(a), 3, "an equal share, not a majority");
        assert_eq!(count(b), 3);
        assert_eq!(count(c), 3);
        assert_eq!(count(d), 0, "the fourth job is outside the cap");
    }

    /// Priority decides WHO is in the active set; within it everyone gets
    /// the same share.
    #[test]
    fn priority_picks_the_active_set_not_the_share() {
        let mut q = QueueState {
            max_active_downloads: 2,
            ..Default::default()
        };
        let low = q.admit_nzb("low".into(), &sample_nzb(&[("l.bin", 4)]), None, 0, true);
        let mid = q.admit_nzb("mid".into(), &sample_nzb(&[("m.bin", 4)]), None, 50, true);
        let high = q.admit_nzb("high".into(), &sample_nzb(&[("h.bin", 4)]), None, 100, true);

        let got = grant(&mut q, &[server(1, 0)], 4);
        let count = |j: JobId| got.iter().filter(|&&g| g == j).count();
        assert_eq!(count(high), 2);
        assert_eq!(count(mid), 2);
        assert_eq!(count(low), 0, "lowest priority waits its turn");
    }

    #[test]
    fn fake_torrent_and_usenet_share_priority_pause_and_active_slots() {
        const NOW: i64 = 1_800_000_000;
        let mut queue = QueueState::default();
        let torrent = admit_fake_torrent(
            &mut queue,
            100,
            nzbd_types::TorrentPhase::Downloading,
            Some(NOW),
        );
        let usenet = queue.admit_nzb("usenet".into(), &sample_nzb(&[("u.bin", 2)]), None, 0, true);
        let none = HashMap::new();

        assert_eq!(active_set(&queue, &none, false, NOW), vec![torrent]);
        assert!(
            grant(&mut queue, &[server(1, 0)], 1).is_empty(),
            "NNTP cannot take a slot currently owned by a higher-priority torrent"
        );

        queue.job_mut(torrent).unwrap().status = JobStatus::Paused;
        assert_eq!(active_set(&queue, &none, false, NOW), vec![usenet]);

        queue.job_mut(torrent).unwrap().status = JobStatus::Downloading;
        queue.max_active_downloads = 2;
        assert_eq!(active_set(&queue, &none, false, NOW), vec![torrent, usenet]);
        assert_eq!(grant(&mut queue, &[server(1, 0)], 1), vec![usenet]);
    }

    #[test]
    fn runtime_recovery_refuses_dormant_torrent_control_rows() {
        let mut queue = QueueState::default();
        admit_fake_torrent(
            &mut queue,
            0,
            nzbd_types::TorrentPhase::PausedDownload,
            None,
        );

        let error = QueueState::from_runtime_doc(queue.to_doc())
            .unwrap_err()
            .to_string();
        assert!(error.contains("no production torrent backend"));
        assert!(error.contains("queue was left unchanged"));
    }

    #[test]
    fn stalled_torrent_retries_cannot_preempt_fresh_work_after_scheduler_yield() {
        use crate::backend::{BackendFact, StopReason, STALLED_SLOT_YIELD_SECS};
        use nzbd_types::TorrentPhase;

        let mut now = 1_800_000_000;
        let mut queue = QueueState::default();
        let stalled = admit_fake_torrent(&mut queue, 100, TorrentPhase::Downloading, Some(now));
        let healthy = admit_fake_torrent(&mut queue, 0, TorrentPhase::Downloading, Some(now));
        let none = HashMap::new();

        for _ in 0..3 {
            now += STALLED_SLOT_YIELD_SECS;
            queue
                .job_mut(healthy)
                .unwrap()
                .torrent
                .as_mut()
                .unwrap()
                .last_activity_unix = Some(now);
            assert_eq!(active_set(&queue, &none, false, now), vec![healthy]);
            crate::torrent_runtime::reconcile_fact(
                queue.job_mut(stalled).unwrap(),
                BackendFact::Stopped {
                    job: stalled,
                    reason: StopReason::SchedulerYield,
                },
                None,
                now,
                std::path::Path::new("/unused"),
            );
            assert_eq!(active_set(&queue, &none, false, now + 1), vec![healthy]);

            // The retry is allowed when capacity becomes available.
            queue.job_mut(healthy).unwrap().status = JobStatus::Paused;
            assert_eq!(active_set(&queue, &none, false, now + 1), vec![stalled]);
            crate::torrent_runtime::reconcile_fact(
                queue.job_mut(stalled).unwrap(),
                BackendFact::Resumed { job: stalled },
                None,
                now,
                std::path::Path::new("/unused"),
            );
            queue.job_mut(healthy).unwrap().status = JobStatus::Downloading;
        }

        // NNTP leases must use the same ordering as torrent starts.
        now += STALLED_SLOT_YIELD_SECS;
        queue
            .job_mut(stalled)
            .unwrap()
            .torrent
            .as_mut()
            .unwrap()
            .phase = TorrentPhase::Queued;
        queue.job_mut(healthy).unwrap().status = JobStatus::Paused;
        let usenet = queue.admit_nzb("usenet".into(), &sample_nzb(&[("u.bin", 2)]), None, 0, true);
        assert_eq!(active_set(&queue, &none, false, now), vec![usenet]);
        // grant() uses a fixed earlier clock; keep the retry expired there too.
        queue
            .job_mut(stalled)
            .unwrap()
            .torrent
            .as_mut()
            .unwrap()
            .last_activity_unix = Some(1);
        assert_eq!(grant(&mut queue, &[server(1, 0)], 1), vec![usenet]);
    }

    #[test]
    fn stalled_fake_torrent_yields_then_reacquires_the_shared_slot() {
        const NOW: i64 = 1_800_000_000;
        let mut queue = QueueState::default();
        let torrent = admit_fake_torrent(
            &mut queue,
            100,
            nzbd_types::TorrentPhase::Downloading,
            Some(NOW - crate::backend::STALLED_SLOT_YIELD_SECS),
        );
        let usenet = queue.admit_nzb("usenet".into(), &sample_nzb(&[("u.bin", 2)]), None, 0, true);
        let none = HashMap::new();

        assert_eq!(
            active_set(&queue, &none, false, NOW),
            vec![usenet],
            "the stalled torrent yields without being removed"
        );
        assert_eq!(
            grant(&mut queue, &[server(1, 0)], 2),
            vec![usenet, usenet],
            "the following Usenet job can drain all of its download work"
        );
        assert!(
            active_set(&queue, &none, false, NOW).is_empty(),
            "fully leased Usenet work does not keep a slot"
        );
        queue
            .job_mut(torrent)
            .unwrap()
            .torrent
            .as_mut()
            .unwrap()
            .last_activity_unix = Some(NOW + 1);
        assert_eq!(
            active_set(&queue, &none, false, NOW + 1),
            vec![torrent],
            "new useful activity makes the torrent eligible again"
        );
    }

    /// A job whose every remaining segment is already out with a
    /// connection has nothing to hand out, so it must not hold a slot —
    /// otherwise the pipe stalls at the tail of every job, which is the
    /// one place nzbd has always overlapped.
    #[test]
    fn a_job_with_nothing_pending_does_not_hold_a_slot() {
        let mut q = QueueState {
            max_active_downloads: 1,
            ..Default::default()
        };
        let a = q.admit_nzb("a".into(), &sample_nzb(&[("a.bin", 2)]), None, 0, true);
        let b = q.admit_nzb("b".into(), &sample_nzb(&[("b.bin", 2)]), None, 0, true);

        let servers = vec![server(1, 0)];
        let got = grant(&mut q, &servers, 4);
        assert_eq!(
            got,
            vec![a, a, b, b],
            "once a is fully leased, b starts rather than the pipe idling"
        );
    }

    /// Out-of-range values cannot stop the queue or ask for a rotation
    /// over ten thousand jobs.
    #[test]
    fn the_cap_is_clamped_into_range() {
        assert_eq!(clamp_active_downloads(0), 1, "zero is not a pause button");
        assert_eq!(clamp_active_downloads(1), 1);
        assert_eq!(clamp_active_downloads(100), 100);
        assert_eq!(clamp_active_downloads(u32::MAX), 100);

        // A queue.json written before the setting existed deserializes
        // the field as 0; loading it must not stop the daemon.
        let doc = nzbd_state::QueueSnapshotDoc {
            schema_version: nzbd_state::QUEUE_SCHEMA_VERSION,
            pending_admissions: Vec::new(),
            jobs: vec![],
            next_job_id: 1,
            next_file_id: 1,
            download_paused: false,
            speed_limit_bps: None,
            max_active_downloads: 0,
        };
        assert_eq!(QueueState::from_doc(doc).max_active_downloads, 1);
    }

    /// `active_set` is what the status labels are settled against, so it
    /// must agree with what the scheduler actually feeds.
    #[test]
    fn the_active_set_matches_what_the_scheduler_serves() {
        let mut q = QueueState {
            max_active_downloads: 2,
            ..Default::default()
        };
        let a = q.admit_nzb("a".into(), &sample_nzb(&[("a.bin", 4)]), None, 0, true);
        let b = q.admit_nzb("b".into(), &sample_nzb(&[("b.bin", 4)]), None, 0, true);
        q.admit_nzb("c".into(), &sample_nzb(&[("c.bin", 4)]), None, 0, true);

        let no_delegation: HashMap<JobId, String> = HashMap::new();
        assert_eq!(active_set(&q, &no_delegation, false, 0), vec![a, b]);

        let got = grant(&mut q, &[server(1, 0)], 4);
        let served: std::collections::HashSet<JobId> = got.into_iter().collect();
        assert_eq!(served, [a, b].into_iter().collect());
    }

    /// A post-processing lease is storage work, not permission to open news
    /// server connections. In particular, resuming a file while unpack/move
    /// is active must not let the downloader mutate that directory under PP,
    /// and a PP-only cluster worker must not bypass provider budgets.
    #[test]
    fn post_processing_status_never_opens_the_downloader() {
        for stage in [PostStage::ParVerify, PostStage::Move] {
            let mut q = QueueState::default();
            let id = q.admit_nzb(
                "post".into(),
                &sample_nzb(&[("payload.bin", 2)]),
                None,
                0,
                false,
            );
            q.job_mut(id).unwrap().status = JobStatus::Post { stage };
            let none: HashMap<JobId, String> = HashMap::new();

            assert!(active_set(&q, &none, false, 0).is_empty());
            assert!(grant(&mut q, &[server(1, 0)], 1).is_empty());
            assert_eq!(q.job(id).unwrap().status, JobStatus::Post { stage });
        }
    }

    /// `Downloading` is set on a job's first lease and was never once
    /// set back, so a job that caught a single segment during the
    /// spill-over at the tail of another stayed labelled `Downloading`
    /// forever while receiving no work — a permanent claim on the
    /// strength of one article.
    #[test]
    fn a_job_that_stopped_getting_work_stops_claiming_to_download() {
        let mut q = QueueState {
            max_active_downloads: 1,
            ..Default::default()
        };
        let a = q.admit_nzb("a".into(), &sample_nzb(&[("a.bin", 4)]), None, 0, true);
        let b = q.admit_nzb("b".into(), &sample_nzb(&[("b.bin", 4)]), None, 0, true);
        let none: HashMap<JobId, String> = HashMap::new();

        // b caught one lease at a's tail and is now labelled Downloading.
        q.job_mut(a).unwrap().status = JobStatus::Downloading;
        q.job_mut(b).unwrap().status = JobStatus::Downloading;
        assert_eq!(active_set(&q, &none, false, 0), vec![a]);
        assert_eq!(
            jobs_to_requeue(&q, &none, false, 0),
            vec![b],
            "b is outside the set and holds nothing, so it is not downloading"
        );
        assert!(
            !jobs_to_requeue(&q, &none, false, 0).contains(&a),
            "the job actually being served keeps its label"
        );

        // While b still has a segment in flight it IS downloading,
        // whatever its position says.
        let r = {
            let f = &q.jobs.iter().find(|j| j.id == b).unwrap().files[0];
            SegRef {
                job: b,
                file: f.id,
                seg_number: f.segments[0].number,
            }
        };
        q.segment_mut(r).unwrap().state = SegmentState::Leased {
            server: ServerId(1),
        };
        assert!(
            jobs_to_requeue(&q, &none, false, 0).is_empty(),
            "a job with work in flight is never demoted"
        );
    }

    #[test]
    fn selection_respects_priority_pause_and_force() {
        let mut q = QueueState::default();
        let low = q.admit_nzb("low".into(), &sample_nzb(&[("a.bin", 2)]), None, 0, true);
        let high = q.admit_nzb("high".into(), &sample_nzb(&[("b.bin", 2)]), None, 100, true);

        let servers = vec![server(1, 0)];
        let ladder = Ladder::new(&servers);
        let mut attempts = HashMap::new();
        let not_blocked = |_: ServerId| false;
        let no_delegation: HashMap<JobId, String> = HashMap::new();
        let no_recovery: HashMap<JobId, HashSet<FileId>> = HashMap::new();
        let mut ctx = SelectionCtx {
            ladder: &ladder,
            attempts: &mut attempts,
            is_blocked: &not_blocked,
            delegated: &no_delegation,
            post_fetch_files: &no_recovery,
            regular_downloads: true,
            article_retries: 3,
            now_unix: 1_800_000_000,
            propagation_delay_secs: 0,

            soft_hold: false,
            rotate: 0,
        };

        let r = next_for_server(&q, &servers[0], &mut ctx);
        assert_eq!(r.lease.unwrap().job, high, "higher priority first");

        // Global pause blocks everything…
        q.download_paused = true;
        let mut ctx = SelectionCtx {
            ladder: &ladder,
            attempts: &mut attempts,
            is_blocked: &not_blocked,
            delegated: &no_delegation,
            post_fetch_files: &no_recovery,
            regular_downloads: true,
            article_retries: 3,
            now_unix: 1_800_000_000,
            propagation_delay_secs: 0,

            soft_hold: false,
            rotate: 0,
        };
        assert!(next_for_server(&q, &servers[0], &mut ctx).lease.is_none());

        // …except force priority.
        q.job_mut(low).unwrap().priority = nzbd_types::PRIORITY_FORCE;
        let mut ctx = SelectionCtx {
            ladder: &ladder,
            attempts: &mut attempts,
            is_blocked: &not_blocked,
            delegated: &no_delegation,
            post_fetch_files: &no_recovery,
            regular_downloads: true,
            article_retries: 3,
            now_unix: 1_800_000_000,
            propagation_delay_secs: 0,

            soft_hold: false,
            rotate: 0,
        };
        let r = next_for_server(&q, &servers[0], &mut ctx);
        assert_eq!(r.lease.unwrap().job, low, "force ignores global pause");
    }

    #[test]
    fn selection_skips_paused_files_and_finds_tiered_server() {
        let mut q = QueueState::default();
        let id = q.admit_nzb(
            "j".into(),
            &sample_nzb(&[("x.vol00+01.par2", 1), ("x.rar", 1)]),
            None,
            0,
            true,
        );
        let servers = vec![server(1, 0), server(2, 1)];
        let ladder = Ladder::new(&servers);
        let mut attempts = HashMap::new();
        let not_blocked = |_: ServerId| false;
        let no_delegation: HashMap<JobId, String> = HashMap::new();
        let no_recovery: HashMap<JobId, HashSet<FileId>> = HashMap::new();

        // Tier-1 server gets nothing while tier 0 is viable.
        let mut ctx = SelectionCtx {
            ladder: &ladder,
            attempts: &mut attempts,
            is_blocked: &not_blocked,
            delegated: &no_delegation,
            post_fetch_files: &no_recovery,
            regular_downloads: true,
            article_retries: 3,
            now_unix: 1_800_000_000,
            propagation_delay_secs: 0,

            soft_hold: false,
            rotate: 0,
        };
        assert!(next_for_server(&q, &servers[1], &mut ctx).lease.is_none());

        // Tier-0 server gets the rar (vol-par is paused).
        let mut ctx = SelectionCtx {
            ladder: &ladder,
            attempts: &mut attempts,
            is_blocked: &not_blocked,
            delegated: &no_delegation,
            post_fetch_files: &no_recovery,
            regular_downloads: true,
            article_retries: 3,
            now_unix: 1_800_000_000,
            propagation_delay_secs: 0,

            soft_hold: false,
            rotate: 0,
        };
        let r = next_for_server(&q, &servers[0], &mut ctx).lease.unwrap();
        assert_eq!(r.job, id);
        let file = q
            .job(id)
            .unwrap()
            .files
            .iter()
            .find(|f| f.id == r.file)
            .unwrap();
        assert_eq!(file.filename, "x.rar");
    }

    #[test]
    fn vol_block_parsing_and_selection() {
        assert_eq!(vol_par_blocks("x.vol00+01.par2"), Some(1));
        assert_eq!(vol_par_blocks("Show.S01.vol127+64.PAR2"), Some(64));
        assert_eq!(vol_par_blocks("x.par2"), None);
        assert_eq!(vol_par_blocks("x.vol7.par2"), None);
        // The range form is inclusive: 000-003 is four blocks.
        assert_eq!(vol_par_blocks("x.vol000-003.par2"), Some(4));
        assert_eq!(vol_par_blocks("x.vol004-004.par2"), Some(1));

        let c = [
            (FileId(1), 1),
            (FileId(2), 2),
            (FileId(3), 8),
            (FileId(4), 16),
        ];
        assert_eq!(
            pick_par_files(&c, 2),
            vec![FileId(2)],
            "smallest single cover"
        );
        assert_eq!(pick_par_files(&c, 5), vec![FileId(3)]);
        assert_eq!(
            pick_par_files(&c, 20),
            vec![FileId(4), FileId(3)],
            "accumulate largest-first"
        );
        assert_eq!(pick_par_files(&c, 100).len(), 4, "take everything if short");
    }

    #[test]
    fn sanitize_names() {
        assert_eq!(sanitize_name("a/b\\c:d"), "a_b_c_d");
        assert_eq!(sanitize_name("  .hidden.  "), "hidden");
        assert_eq!(sanitize_name(""), "unnamed");
    }

    /// Field report 2026-07-25: URL-added jobs showed the whole glued query
    /// string as their title ("af51ab….nzb&i=136144&r=<apikey>…"). The
    /// junk-stripper runs at add time now — these are the exact shapes.
    #[test]
    fn strip_name_junk_urls_and_glued_queries() {
        // dognzb-style: params glued straight onto the filename with '&'.
        assert_eq!(
            strip_name_junk(
                "af51ab64582e226f4bc8de91b7b757d8067ba8e6.nzb&i=136144&r=104c19dfb6da6d2a89b2"
            ),
            "af51ab64582e226f4bc8de91b7b757d8067ba8e6"
        );
        // Same, but as a full URL (the add_url fallback when no name given).
        assert_eq!(
            strip_name_junk(
                "https://api.indexer.example/getnzb/af51ab64582e226f4bc8de91b7b757d8067ba8e6.nzb&i=136144&r=104c19dfb6da6d2a89b2"
            ),
            "af51ab64582e226f4bc8de91b7b757d8067ba8e6"
        );
        // Regular '?' query, percent-encoded name.
        assert_eq!(
            strip_name_junk("https://x.example/dl/My%20Show%20S01E01.nzb?apikey=secret"),
            "My Show S01E01"
        );
        // A bare '&' inside a real release name survives.
        assert_eq!(
            strip_name_junk("Tom & Jerry (2021).nzb"),
            "Tom & Jerry (2021)"
        );
    }

    #[test]
    fn final_status_uses_health_gate() {
        let mut q = QueueState::default();
        let id = q.admit_nzb("j".into(), &sample_nzb(&[("a.bin", 10)]), None, 0, true);
        let job = q.job_mut(id).unwrap();
        // 9 done, 1 failed: health 900 ≥ critical 850 → completed
        for (i, s) in job.files[0].segments.iter_mut().enumerate() {
            s.state = if i == 0 {
                SegmentState::Failed
            } else {
                SegmentState::Done {
                    offset: 0,
                    len: 1000,
                    crc: 0,
                }
            };
        }
        recompute_job_totals(job);
        let (status, health) = final_status(job);
        assert_eq!(status, JobStatus::Completed);
        assert_eq!(health.0, 900);

        // 8 failed → health 200 < critical 850 → failed
        for s in job.files[0].segments.iter_mut().take(8) {
            s.state = SegmentState::Failed;
        }
        recompute_job_totals(job);
        let (status, health) = final_status(job);
        assert_eq!(status, JobStatus::Failed);
        assert_eq!(health.0, 200);
    }

    // ---- naming an obfuscated post ------------------------------------

    fn job_with(name: &str, params: &[(&str, &str)], category: Option<&str>) -> Job {
        Job {
            id: JobId(182),
            kind: JobKind::Url,
            name: name.into(),
            dir_name: String::new(),
            name_provisional: false,
            queued_at_unix: 0,
            original_name: String::new(),
            category: category.map(str::to_string),
            priority: 0,
            dupe: DupeInfo::default(),
            params: params
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            files: vec![],
            totals: Default::default(),
            status: JobStatus::Fetching,
            torrent: None,
            stages: vec![],
        }
    }

    /// Field report 2026-07-29, job #182: a 4.8 GiB download titled
    /// `cc310b99…`, everything inside it obfuscated too, so no evidence
    /// anywhere. "That is not useful at all." What we DO know is who asked
    /// and where from.
    #[test]
    fn a_job_with_no_evidence_is_named_by_who_asked_for_it() {
        let job = job_with(
            "cc310b9901757996b0bdfd880c666e3812e6531d",
            &[
                (CLIENT_PARAM, "monarr"),
                (
                    "*URL",
                    "https://drunkenslug.com/getnzb/cc310b99.nzb&i=1&r=KEY",
                ),
            ],
            Some("monarr"),
        );
        assert_eq!(
            requestor_name(&job).as_deref(),
            Some("monarr · drunkenslug · cc310b99")
        );
    }

    /// With no client the category still says something, and with neither
    /// there is nothing honest to say — inventing a name would be worse
    /// than the hash.
    #[test]
    fn the_requestor_name_says_only_what_is_actually_known() {
        let cat_only = job_with(
            "cc310b9901757996b0bdfd880c666e3812e6531d",
            &[("*URL", "https://api.nzbgeek.info/api?t=get&id=1")],
            Some("movies"),
        );
        assert_eq!(
            requestor_name(&cat_only).as_deref(),
            Some("movies · nzbgeek · cc310b99"),
            "the registrable label, not the api subdomain"
        );

        // A two-part public suffix resolves to the registered domain —
        // `example.co.uk`, so `example`, not the `indexer` subdomain.
        let couk = job_with(
            "cc310b9901757996b0bdfd880c666e3812e6531d",
            &[("*URL", "https://api.indexer.example.co.uk/get?id=1")],
            Some("movies"),
        );
        assert_eq!(
            requestor_name(&couk).as_deref(),
            Some("movies · example · cc310b99")
        );

        let nothing = job_with("cc310b9901757996b0bdfd880c666e3812e6531d", &[], None);
        assert_eq!(requestor_name(&nothing), None, "no context, no invention");

        // An IP literal has no label a human recognises. Picking one out
        // of it produced "0 · af51ab64" — worse than the hash it replaced.
        let ip = job_with(
            "af51ab64582e226f4bc8de91b7b757d8067ba8e6",
            &[("*URL", "http://127.0.0.1:8080/getnzb/af51ab64.nzb")],
            None,
        );
        assert_eq!(requestor_name(&ip), None);
        let ip6 = job_with(
            "af51ab64",
            &[("*URL", "http://[::1]:8080/getnzb/x.nzb")],
            Some("tv"),
        );
        assert_eq!(requestor_name(&ip6).as_deref(), Some("tv · af51ab64"));
    }

    /// The real answer, and the one worth waiting a minute for: par2
    /// FileDesc packets carry the true filenames even when every NZB
    /// subject is obfuscated.
    #[test]
    fn par2_metadata_names_the_job_its_nzb_could_not() {
        let descs = |names: &[&str]| -> Vec<nzbd_par2::FileDesc> {
            names
                .iter()
                .enumerate()
                .map(|(i, n)| nzbd_par2::FileDesc {
                    md5_full: [0; 16],
                    id: [i as u8; 16],
                    name: (*n).to_string(),
                    length: 100,
                    md5_16k: [0; 16],
                })
                .collect()
        };
        assert_eq!(
            name_from_par2(&descs(&[
                "Some.Movie.2024.1080p.WEB-DL.DDP5.1-GRP.part01.rar",
                "Some.Movie.2024.1080p.WEB-DL.DDP5.1-GRP.part02.rar",
                "Some.Movie.2024.1080p.WEB-DL.DDP5.1-GRP.par2",
            ]))
            .as_deref(),
            Some("Some.Movie.2024.1080p.WEB-DL.DDP5.1-GRP")
        );
        // A single payload file names the job by itself, minus extension.
        assert_eq!(
            name_from_par2(&descs(&["Show.S01E07.720p.WEB-DL-KiNGS.mkv"])).as_deref(),
            Some("Show.S01E07.720p.WEB-DL-KiNGS")
        );
        // A par2 set whose own contents are obfuscated names nothing, and
        // must say so rather than hand back another hash.
        assert_eq!(
            name_from_par2(&descs(&[
                "XyfmaV5wXwfrrrqbVHgvsqC8b2ztZK",
                "XyfmaV5wXwfrrrqbVHgvsqC8b2ztZL",
            ])),
            None
        );
        assert_eq!(name_from_par2(&[]), None);
    }

    /// A rename may move the storage directory only while nothing has been
    /// written. Getting this wrong splits a job's files across two
    /// directories, because the writer recomputes its target from the job
    /// every time one spawns.
    #[test]
    fn only_a_pre_download_rename_moves_the_directory() {
        let mut j = job_with("cc310b99", &[], None);
        j.dir_name = "cc310b99".into();

        rename_job(&mut j, "Real.Name.2024".into(), true, false);
        assert_eq!(j.name, "Real.Name.2024");
        assert_eq!(
            j.dir_name, "Real.Name.2024",
            "pre-download: the path follows"
        );

        rename_job(&mut j, "Even.Better.Name".into(), false, false);
        assert_eq!(j.name, "Even.Better.Name");
        assert_eq!(
            j.dir_name, "Real.Name.2024",
            "mid-download: the display name moves, the path must not"
        );

        // A no-op rename never touches anything.
        rename_job(&mut j, String::new(), true, false);
        assert_eq!(j.name, "Even.Better.Name");
    }

    /// The trap a good placeholder sets for itself: `monarr · drunkenslug ·
    /// cc310b99` is deliberately readable, so a gate that asks "does this
    /// look like junk?" says no — and refuses the real name when the par2
    /// index finally supplies it. The provisional flag is what keeps the
    /// question answerable.
    #[test]
    fn a_provisional_name_still_yields_to_the_real_one() {
        let mut j = job_with("cc310b9901757996b0bdfd880c666e3812e6531d", &[], None);
        assert!(name_is_open(&j), "raw junk is open to anything better");

        rename_job(&mut j, "monarr · drunkenslug · cc310b99".into(), true, true);
        assert!(
            !is_uninformative_name(&j.name),
            "the placeholder reads as a real name — that is the point of it"
        );
        assert!(name_is_open(&j), "…but it is still only a stand-in");

        rename_job(&mut j, "Some.Movie.2024.1080p-GRP".into(), false, false);
        assert!(
            !name_is_open(&j),
            "a name from the job's own documents is final"
        );

        // And nothing may overwrite it afterwards — a second par2 volume
        // carrying the same packets must not restart the churn.
        rename_job(&mut j, "Something.Else".into(), false, false);
        assert_eq!(j.name, "Something.Else", "rename_job itself does not gate");
        // The gate lives at the call sites, which consult name_is_open.
    }

    /// `job_dir_name` has to keep answering for jobs written before
    /// `dir_name` existed, or a restart relocates every in-flight download.
    #[test]
    fn a_pre_split_job_still_finds_its_own_directory() {
        let old = job_with("Some Movie: 2024/HDR", &[], None); // dir_name empty
        assert_eq!(job_dir_name(&old), sanitize_name("Some Movie: 2024/HDR"));
        let mut new = old.clone();
        new.dir_name = "explicit-dir".into();
        assert_eq!(job_dir_name(&new), "explicit-dir");
    }

    /// The gate every rename path consults.
    #[test]
    fn uninformative_names_are_the_ones_worth_replacing() {
        for junk in [
            "cc310b9901757996b0bdfd880c666e3812e6531d",
            "UcsRDCyhGHPCP2TqBJWrnUg",
            "yEnc",
            "",
            "   ",
        ] {
            assert!(
                is_uninformative_name(junk),
                "{junk:?} should be replaceable"
            );
        }
        for real in [
            "Some.Movie.2024.1080p.WEB-DL-GRP",
            "Jim Jefferies - Alcoholocaust (2010) 1080p",
            "Show.S01E07",
        ] {
            assert!(!is_uninformative_name(real), "{real:?} should be kept");
        }
    }

    // ---- the requestor placeholder must not eat its own tail ----------

    fn url_job(id: u32, client: &str, url: &str, name: &str) -> Job {
        let mut j = job_with(name, &[(CLIENT_PARAM, client), ("*URL", url)], None);
        j.id = JobId(id);
        j
    }

    /// Field report 2026-07-29: sixteen queued jobs, every one titled
    /// `monarr/0.11.0 · drunkenslug · monarr/0`, all writing into one
    /// directory.
    ///
    /// `requestor_name` took its discriminator from `job.name` — the field
    /// the rename it feeds then overwrites. Run twice (admission, then
    /// again when the URL fetch completes) it read its own output: the
    /// first eight characters of `monarr/0.11.0 · …` are `monarr/0`. That
    /// is a fixed point, and the same one for every job from that client.
    #[test]
    fn the_requestor_placeholder_is_stable_under_repetition() {
        let mut j = url_job(
            187,
            "monarr/0.11.0",
            "https://drunkenslug.com/getnzb/e44dbc357d05b55850a76028a6efec9f5ef893b0",
            "e44dbc357d05b55850a76028a6efec9f5ef893b0",
        );
        let first = requestor_name(&j).expect("client and indexer are both known");
        assert_eq!(first, "monarr/0.11.0 · drunkenslug · e44dbc35");

        rename_job(&mut j, first.clone(), true, true);
        let second = requestor_name(&j).expect("still nameable");
        assert_eq!(second, first, "a second pass must not consume the first");

        // …and a third, because the real daemon calls it once per add and
        // once per completed fetch, with restarts in between.
        rename_job(&mut j, second.clone(), true, true);
        assert_eq!(requestor_name(&j).as_deref(), Some(first.as_str()));
    }

    /// Two jobs from one client and one indexer are the normal case, not
    /// the exotic one. Their placeholders must still differ.
    #[test]
    fn two_jobs_from_one_requestor_are_still_tellable_apart() {
        let a = url_job(187, "monarr/0.11.0", "https://drunkenslug.com/x", "");
        let b = url_job(188, "monarr/0.11.0", "https://drunkenslug.com/x", "");
        let (na, nb) = (requestor_name(&a).unwrap(), requestor_name(&b).unwrap());
        assert_ne!(na, nb, "{na} and {nb} would share a directory");
        assert!(na.ends_with("#187") && nb.ends_with("#188"));

        // A client label that happens to repeat a part we already used is
        // no discriminator either.
        let mut c = url_job(190, "monarr", "https://drunkenslug.com/x", "monarr");
        c.name = "monarr".into();
        assert_eq!(
            requestor_name(&c).as_deref(),
            Some("monarr · drunkenslug · #190")
        );
    }

    /// A provisional name reads as informative — that is its whole design
    /// — so handing it back to the evidence pass as the *hint* short-
    /// circuits the pass and the NZB never gets asked. Sixteen jobs whose
    /// par2 sets named them perfectly were titled after their requestor.
    #[test]
    fn a_provisional_name_does_not_mask_the_nzb() {
        let mut q = QueueState::default();
        let id = q.admit_url(
            "e44dbc357d05b55850a76028a6efec9f5ef893b0".into(),
            "https://drunkenslug.com/getnzb/e44dbc35",
            Some("monarr".into()),
            0,
        );
        q.set_job_name(
            id,
            "monarr/0.11.0 · drunkenslug · e44dbc35".into(),
            true,
            true,
        );

        let nzb = nzb_with(
            &[
                "72.Hours.2026.2160p.NF.WEB-DL.DV.HDR.MULTi.mp4.vol000+01.par2",
                "72.Hours.2026.2160p.NF.WEB-DL.DV.HDR.MULTi.mp4.vol001+02.par2",
            ],
            None,
        );
        assert!(q.complete_url_fetch(id, &nzb, false));

        let j = q.job(id).unwrap();
        assert_eq!(j.name, "72.Hours.2026.2160p.NF.WEB-DL.DV.HDR.MULTi.mp4");
        assert!(!j.name_provisional, "the NZB's own naming is final");
        assert_eq!(
            j.dir_name, "72.Hours.2026.2160p.NF.WEB-DL.DV.HDR.MULTi.mp4",
            "nothing is on disk yet, so the directory follows"
        );
        assert_eq!(
            j.original_name, "e44dbc357d05b55850a76028a6efec9f5ef893b0",
            "what the client called it is still findable"
        );
    }

    /// A job whose NZB really has nothing keeps its placeholder — the
    /// fallback still works, it just no longer overwrites better answers.
    #[test]
    fn a_fetch_with_no_evidence_leaves_the_placeholder_alone() {
        let mut q = QueueState::default();
        let id = q.admit_url("cc310b99017579".into(), "https://x.example/a", None, 0);
        q.set_job_name(id, "monarr · x · cc310b99".into(), true, true);
        let nzb = nzb_with(&["XyfmaV5wXwfrrrqbVHgvsqC8b2ztZK"], None);
        assert!(q.complete_url_fetch(id, &nzb, false));
        let j = q.job(id).unwrap();
        assert_eq!(j.name, "monarr · x · cc310b99");
        assert!(j.name_provisional, "still open to a par2 answer later");
    }

    /// One directory, one job. Two releases interleaved in a folder is not
    /// a cosmetic defect: post-processing verifies and moves them as one.
    #[test]
    fn two_jobs_never_share_a_download_directory() {
        let mut q = QueueState::default();
        let nzb = nzb_with(&["Same.Release.2024-GRP.par2"], None);
        let a = q.admit_nzb("Same.Release.2024-GRP".into(), &nzb, None, 0, false);
        let b = q.admit_nzb("Same.Release.2024-GRP".into(), &nzb, None, 0, false);
        assert_ne!(
            q.job(a).unwrap().dir_name,
            q.job(b).unwrap().dir_name,
            "a duplicate add must not clobber the first one's files"
        );

        // And a rename that collides is separated the same way.
        q.set_job_name(b, "Same.Release.2024-GRP".into(), true, false);
        assert_ne!(q.job(a).unwrap().dir_name, q.job(b).unwrap().dir_name);
    }

    /// The queue that is already broken must not stay broken. On boot a
    /// job with an open name is told what it is by its own file list, and
    /// a shared directory is split — but only for a job that has not
    /// written anything, because moving the folder of one that has orphans
    /// the bytes already in it.
    #[test]
    fn boot_repairs_the_names_and_directories_it_finds() {
        let nzb = nzb_with(&["The.Dink.2026.2160p-PiRaTeS.mkv.vol000+01.par2"], None);
        let mut q = QueueState::default();
        let a = q.admit_nzb("x".into(), &nzb, None, 0, false);
        let b = q.admit_nzb("x".into(), &nzb, None, 0, false);
        for id in [a, b] {
            let j = q.job_mut(id).unwrap();
            j.name = "monarr/0.11.0 · drunkenslug · monarr/0".into();
            j.name_provisional = true;
            j.dir_name = "monarr_0.11.0 · drunkenslug · monarr_0".into();
        }
        // One of them has already written; its directory must not move.
        q.job_mut(b).unwrap().totals.success_articles = 3;

        let doc = q.to_doc();
        let q = QueueState::from_doc(doc);

        for id in [a, b] {
            assert_eq!(
                q.job(id).unwrap().name,
                "The.Dink.2026.2160p-PiRaTeS.mkv",
                "the file list knew all along"
            );
            assert!(!q.job(id).unwrap().name_provisional);
        }
        assert_eq!(
            q.job(b).unwrap().dir_name,
            "monarr_0.11.0 · drunkenslug · monarr_0",
            "bytes on disk: the folder stays where they are"
        );
        assert_ne!(
            q.job(a).unwrap().dir_name,
            q.job(b).unwrap().dir_name,
            "nothing written: this one moves out of the shared folder"
        );
    }

    #[test]
    fn a_file_list_names_a_job_the_same_way_an_nzb_does() {
        let names = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            name_from_files(&names(&["Cool.Movie.2024-GRP.par2", "abc.part01.rar"])).as_deref(),
            Some("Cool.Movie.2024-GRP")
        );
        assert_eq!(
            name_from_files(&names(&[
                "Show.S01E02.part01.rar",
                "Show.S01E02.part02.rar"
            ]))
            .as_deref(),
            Some("Show.S01E02")
        );
        // All obfuscated: say nothing rather than hand back a hash.
        assert_eq!(
            name_from_files(&names(&[
                "XyfmaV5wXwfrrrqbVHgvsqC8b2ztZK",
                "XyfmaV5wXwfrrrqbVHgvsqC8b2ztZL",
            ])),
            None
        );
        assert_eq!(name_from_files(&[]), None);
    }

    #[test]
    fn torrent_pending_intent_is_structurally_replaced_and_live_hash_deduplicates() {
        let mut queue = QueueState::default();
        let first = queue.reserve_torrent_admission(TorrentSource::Magnet, AddOpts::default());
        assert!(queue.jobs.is_empty());
        let record = TorrentRecord {
            info_hash_v1: "0123456789abcdef0123456789abcdef01234567".into(),
            source: TorrentSource::Magnet,
            metadata_file: "torrents/sources/hash.torrent".into(),
            payload_root: std::path::PathBuf::new(),
            phase: nzbd_types::TorrentPhase::Queued,
            control_intent: nzbd_types::TorrentControlIntent::Running,
            removal_intent: None,
            removal_outcome: None,
            removal_confirmed_at_unix: None,
            stop_reason: None,
            files: vec![],
            total_bytes: 42,
            selected_bytes: 42,
            downloaded_bytes: 0,
            uploaded_bytes: 0,
            seeding_seconds: 0,
            ready_at_unix: None,
            content_path: None,
            seed_policy: Default::default(),
            last_activity_unix: None,
            last_error: None,
        };
        assert_eq!(
            queue.commit_torrent_admission(
                first,
                TorrentAdmissionCommit {
                    name: "one".into(),
                    category: None,
                    priority: 0,
                    paused: false,
                    params: vec![],
                    record: record.clone()
                }
            ),
            Some(Ok(first))
        );
        assert!(queue.pending_admissions.is_empty());
        assert_eq!(queue.jobs[0].id, first);

        let retry = queue.reserve_torrent_admission(TorrentSource::Url, AddOpts::default());
        assert_eq!(
            queue.commit_torrent_admission(
                retry,
                TorrentAdmissionCommit {
                    name: "two".into(),
                    category: None,
                    priority: 0,
                    paused: false,
                    params: vec![],
                    record
                }
            ),
            Some(Err(first))
        );
        assert_eq!(queue.jobs.len(), 1);
        assert!(queue.pending_admissions.is_empty());
    }
}
