//! Measurements are observations, never import/delete authority. Actor-owned
//! cache avoids reparsing every persisted interval on each queue repaint.
use nzbd_types::{
    Job, JobId, JobStatus, LifecycleInterval, LifecycleIntervalState as State, LifecycleLedger,
    LifecycleObservation, LifecycleStage as Stage, LifecycleTiming, LIFECYCLE_TIMING_PARAM,
    PP_DONE_PARAM,
};
use std::collections::{BTreeSet, HashMap};
use std::hash::{BuildHasher, Hasher};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    OnceLock,
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
struct Clock {
    origin: Instant,
    nonce: String,
}
static CLOCK: OnceLock<Clock> = OnceLock::new();
static OBSERVATION: AtomicU64 = AtomicU64::new(1);
fn stamp(node: &str) -> (String, u64, u64) {
    let clock = CLOCK.get_or_init(|| {
        let mut random = std::collections::hash_map::RandomState::new().build_hasher();
        random.write_u32(std::process::id());
        random.write_u128(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        );
        Clock {
            origin: Instant::now(),
            nonce: format!("{:016x}", random.finish()),
        }
    });
    (
        format!("{node}:{}", clock.nonce),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64,
        clock.origin.elapsed().as_millis().min(u64::MAX as u128) as u64,
    )
}
fn desired(job: &Job, delegated: bool) -> BTreeSet<Stage> {
    if delegated || job.params.iter().any(|(k, _)| k == PP_DONE_PARAM) {
        return BTreeSet::new();
    }
    let mut stages = BTreeSet::new();
    let post = matches!(job.status, JobStatus::Post { .. })
        || job.stages.last().is_some_and(|s| s.ms.is_none());
    if post {
        stages.insert(Stage::PostProcessing);
    }
    match job.status {
        JobStatus::Fetching => {
            stages.insert(Stage::UrlFetch);
        }
        JobStatus::Downloading => {
            stages.insert(Stage::Download);
        }
        JobStatus::Queued | JobStatus::Paused | JobStatus::PostQueued => {
            stages.insert(Stage::Queue);
        }
        JobStatus::Completed if !post => {
            stages.insert(Stage::Queue);
        }
        _ => (),
    }
    stages
}
fn finish(interval: &mut LifecycleInterval, unix: u64, mono: u64) {
    if let Some(start) = interval.start_monotonic_ms.filter(|start| *start <= mono) {
        interval.end_monotonic_ms = Some(mono);
        interval.duration_ms = Some(mono - start);
        interval.end_unix_ms = Some(unix);
        interval.state = State::Completed;
    } else {
        interval.state = State::Unknown;
        interval.duration_ms = None;
    }
}
fn open(
    node: &str,
    domain: &str,
    job: JobId,
    stage: Stage,
    unix: u64,
    mono: Option<u64>,
    state: State,
) -> LifecycleObservation {
    LifecycleObservation {
        id: format!(
            "{}:{}:{}",
            domain,
            job.0,
            OBSERVATION.fetch_add(1, Ordering::Relaxed)
        ),
        interval: LifecycleInterval {
            stage,
            node_id: node.into(),
            clock_domain: domain.into(),
            start_unix_ms: unix,
            end_unix_ms: None,
            start_monotonic_ms: mono,
            end_monotonic_ms: None,
            duration_ms: None,
            state,
            observations: 1,
        },
    }
}
#[derive(Default)]
pub(crate) struct TimingCache {
    entries: HashMap<JobId, Cached>,
}
#[derive(Clone)]
struct Cached {
    token: String,
    last_wire: String,
    ledger: LifecycleLedger,
}
pub(crate) struct TimingCheckpoint(Option<Cached>);
impl TimingCache {
    pub fn checkpoint(&self, job: JobId) -> TimingCheckpoint {
        TimingCheckpoint(self.entries.get(&job).cloned())
    }
    pub fn restore(&mut self, job: JobId, checkpoint: TimingCheckpoint) {
        match checkpoint.0 {
            Some(cached) => {
                self.entries.insert(job, cached);
            }
            None => {
                self.entries.remove(&job);
            }
        }
    }
    pub fn sync(&mut self, job: &mut Job, node: &str, delegated: bool) -> bool {
        let point = stamp(node);
        self.sync_at(job, node, delegated, point)
    }
    fn sync_at(
        &mut self,
        job: &mut Job,
        node: &str,
        delegated: bool,
        point: (String, u64, u64),
    ) -> bool {
        let (domain, unix, mono) = point;
        let token = job
            .params
            .iter()
            .find(|(k, _)| k == "monarr-transfer")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        if self
            .entries
            .get(&job.id)
            .is_some_and(|cached| cached.token != token)
        {
            self.entries.remove(&job.id);
        }
        let raw = job
            .params
            .iter()
            .find(|(k, _)| k == LIFECYCLE_TIMING_PARAM)
            .map(|(_, v)| v.as_str())
            .unwrap_or("");
        let wanted = desired(job, delegated);
        let cached = self.entries.entry(job.id).or_insert_with(|| {
            let incoming = job.lifecycle_ledger();
            let legacy = incoming.is_none();
            let mut ledger = incoming.unwrap_or_default();
            for record in &mut ledger.records {
                if record.interval.state == State::Running {
                    record.interval.state = if record.interval.node_id == node {
                        State::Interrupted
                    } else {
                        State::Unknown
                    };
                    record.interval.duration_ms = None;
                    record.interval.end_monotonic_ms = None;
                    record.interval.end_unix_ms = None;
                }
            }
            if legacy {
                for stage in &wanted {
                    ledger.records.push(open(
                        node,
                        &format!("{node}:unrecorded"),
                        job.id,
                        *stage,
                        0,
                        None,
                        State::Unknown,
                    ));
                }
            }
            Cached {
                token,
                last_wire: String::new(),
                ledger,
            }
        });
        if cached.last_wire != raw {
            if let Some(incoming) = job.lifecycle_ledger() {
                cached.ledger.merge(&incoming);
            }
        }
        let needs_transition = cached.last_wire != raw
            || raw.is_empty()
            || cached.ledger.records.iter().any(|r| {
                r.interval.state == State::Running
                    && (r.interval.clock_domain != domain || !wanted.contains(&r.interval.stage))
            })
            || wanted.iter().any(|stage| {
                !cached.ledger.records.iter().any(|r| {
                    r.interval.stage == *stage
                        && r.interval.clock_domain == domain
                        && r.interval.state == State::Running
                })
            });
        if !needs_transition {
            return false;
        }
        let before = cached.ledger.clone();
        for record in &mut cached.ledger.records {
            let interval = &mut record.interval;
            if interval.state == State::Running
                && interval.clock_domain == domain
                && !wanted.contains(&interval.stage)
            {
                finish(interval, unix, mono);
            } else if interval.state == State::Running && interval.clock_domain != domain {
                interval.state = if interval.node_id == node {
                    State::Interrupted
                } else {
                    State::Unknown
                };
                interval.duration_ms = None;
                interval.end_monotonic_ms = None;
                interval.end_unix_ms = None;
            }
        }
        for stage in wanted {
            if !cached.ledger.records.iter().any(|r| {
                r.interval.stage == stage
                    && r.interval.clock_domain == domain
                    && r.interval.state == State::Running
            }) {
                cached.ledger.records.push(open(
                    node,
                    &domain,
                    job.id,
                    stage,
                    unix,
                    Some(mono),
                    State::Running,
                ));
            }
        }
        cached.ledger.bound();
        let changed = before != cached.ledger || raw.is_empty() || cached.last_wire != raw;
        if changed {
            job.set_lifecycle_ledger(&cached.ledger);
            cached.last_wire = job
                .params
                .iter()
                .find(|(k, _)| k == LIFECYCLE_TIMING_PARAM)
                .map(|(_, v)| v.clone())
                .unwrap_or_default();
        }
        changed
    }
    pub fn retain(&mut self, jobs: &BTreeSet<JobId>) {
        self.entries.retain(|id, _| jobs.contains(id));
    }
}
/// Freeze measured evidence at publication. Live queue intervals may continue,
/// but replay returns the already persisted immutable Delivery snapshot.
pub fn frozen_lifecycle_timing(job: &Job) -> Option<LifecycleTiming> {
    let mut ledger = job.lifecycle_ledger()?;
    for record in &mut ledger.records {
        let interval = &mut record.interval;
        if interval.state != State::Running {
            continue;
        }
        let (domain, unix, mono) = stamp(&interval.node_id);
        if interval.clock_domain == domain {
            finish(interval, unix, mono);
        } else {
            interval.state = State::Unknown;
            interval.duration_ms = None;
            interval.end_monotonic_ms = None;
            interval.end_unix_ms = None;
        }
    }
    Some(ledger.snapshot())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn job() -> Job {
        Job {
            id: JobId(1),
            kind: nzbd_types::JobKind::Nzb,
            name: "timing fixture".into(),
            dir_name: "fixture".into(),
            name_provisional: false,
            delivery_lifecycle: true,
            queued_at_unix: 1,
            original_name: String::new(),
            category: None,
            priority: 0,
            dupe: nzbd_types::DupeInfo::default(),
            params: vec![("monarr-transfer".into(), "same-request".into())],
            files: Vec::new(),
            totals: nzbd_types::JobTotals::default(),
            status: JobStatus::Queued,
            torrent: None,
            stages: Vec::new(),
        }
    }
    fn observe(
        cache: &mut TimingCache,
        job: &mut Job,
        node: &str,
        domain: &str,
        unix: u64,
        mono: u64,
    ) {
        cache.sync_at(job, node, false, (domain.into(), unix, mono));
    }
    #[test]
    fn major_measurements_follow_monotonic_time_across_wall_clock_changes() {
        let mut job = job();
        job.set_lifecycle_ledger(&LifecycleLedger::default());
        let mut cache = TimingCache::default();
        observe(&mut cache, &mut job, "node-a", "node-a:boot", 1000, 0);
        job.status = JobStatus::Downloading;
        observe(&mut cache, &mut job, "node-a", "node-a:boot", 10, 100);
        job.status = JobStatus::Post {
            stage: nzbd_types::PostStage::ParVerify,
        };
        observe(&mut cache, &mut job, "node-a", "node-a:boot", 50, 200);
        observe(&mut cache, &mut job, "node-a", "node-a:boot", 999999, 500);
        job.params.push((PP_DONE_PARAM.into(), "1".into()));
        observe(&mut cache, &mut job, "node-a", "node-a:boot", 70, 700);
        let timing = job.lifecycle_ledger().unwrap().snapshot();
        assert_eq!(
            timing
                .intervals
                .iter()
                .map(|i| (i.stage, i.duration_ms))
                .collect::<Vec<_>>(),
            vec![
                (Stage::Queue, Some(100)),
                (Stage::Download, Some(100)),
                (Stage::PostProcessing, Some(500))
            ]
        );
        assert!(timing.valid());
    }
    #[test]
    fn restart_retains_interrupted_evidence_without_inventing_elapsed_duration() {
        let mut job = job();
        job.set_lifecycle_ledger(&LifecycleLedger::default());
        let mut first = TimingCache::default();
        observe(&mut first, &mut job, "node-a", "node-a:old", 1000, 0);
        let mut restarted = TimingCache::default();
        observe(&mut restarted, &mut job, "node-a", "node-a:new", 1000000, 0);
        let timing = job.lifecycle_ledger().unwrap().snapshot();
        assert_eq!(timing.intervals.len(), 2);
        assert_eq!(timing.intervals[0].state, State::Interrupted);
        assert_eq!(timing.intervals[0].duration_ms, None);
        assert_eq!(timing.intervals[1].state, State::Running);
    }
    #[test]
    fn worker_import_and_stale_replay_preserve_both_clock_lanes_once() {
        let mut leader_job = job();
        leader_job.set_lifecycle_ledger(&LifecycleLedger::default());
        let mut leader = TimingCache::default();
        observe(
            &mut leader,
            &mut leader_job,
            "leader",
            "leader:boot",
            1000,
            0,
        );
        leader.sync_at(
            &mut leader_job,
            "leader",
            true,
            ("leader:boot".into(), 1010, 10),
        );
        let stale = leader_job.clone();
        let mut worker_job = leader_job.clone();
        let mut worker = TimingCache::default();
        worker_job.status = JobStatus::Downloading;
        observe(
            &mut worker,
            &mut worker_job,
            "worker",
            "worker:boot",
            900,
            0,
        );
        worker_job.status = JobStatus::Post {
            stage: nzbd_types::PostStage::ParVerify,
        };
        observe(
            &mut worker,
            &mut worker_job,
            "worker",
            "worker:boot",
            901,
            100,
        );
        worker_job.params.push((PP_DONE_PARAM.into(), "1".into()));
        observe(
            &mut worker,
            &mut worker_job,
            "worker",
            "worker:boot",
            902,
            300,
        );
        leader_job = worker_job;
        observe(
            &mut leader,
            &mut leader_job,
            "leader",
            "leader:boot",
            1020,
            20,
        );
        let expected = leader_job.lifecycle_ledger().unwrap();
        assert_eq!(expected.records.len(), 3);
        assert_eq!(
            expected
                .records
                .iter()
                .filter(|r| r.interval.node_id == "worker")
                .count(),
            2
        );
        leader_job.params = stale.params;
        leader_job.params.push((PP_DONE_PARAM.into(), "1".into()));
        observe(
            &mut leader,
            &mut leader_job,
            "leader",
            "leader:boot",
            1030,
            30,
        );
        assert_eq!(leader_job.lifecycle_ledger().unwrap(), expected);
        observe(
            &mut leader,
            &mut leader_job,
            "leader",
            "leader:boot",
            1040,
            40,
        );
        assert_eq!(leader_job.lifecycle_ledger().unwrap(), expected);
    }
    #[test]
    fn legacy_stage_is_explicitly_unknown_before_new_process_measurement() {
        let mut job = job();
        let mut cache = TimingCache::default();
        observe(&mut cache, &mut job, "node", "node:boot", 100000, 0);
        let timing = job.lifecycle_ledger().unwrap().snapshot();
        assert_eq!(timing.intervals[0].state, State::Unknown);
        assert_eq!(timing.intervals[0].start_unix_ms, 0);
        assert_eq!(timing.intervals[0].duration_ms, None);
    }
}
