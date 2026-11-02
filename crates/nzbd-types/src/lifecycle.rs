//! Bounded measured lifecycle evidence. Internal observation IDs survive replay;
//! public snapshots contain clock-scoped measurements without control authority.
use crate::Job;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
pub const LIFECYCLE_TIMING_PARAM: &str = "*Runner:LifecycleTiming";
pub const MAX_LIFECYCLE_INTERVALS: usize = 128;
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleStage {
    Queue,
    UrlFetch,
    Download,
    PostProcessing,
    /// Reserved for future measured handoff evidence; current publication
    /// snapshots keep consumer ACK in the separate mutable receipt lane.
    Handoff,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleIntervalState {
    Completed,
    Interrupted,
    Running,
    Unknown,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleInterval {
    pub stage: LifecycleStage,
    pub node_id: String,
    pub clock_domain: String,
    pub start_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_monotonic_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_monotonic_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    pub state: LifecycleIntervalState,
    #[serde(default = "one")]
    pub observations: u32,
}
fn one() -> u32 {
    1
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleTiming {
    pub version: u8,
    pub intervals: Vec<LifecycleInterval>,
    /// Conservative lower bounds; totals describe the recorded subset.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub omitted_intervals: BTreeMap<LifecycleStage, u32>,
}
impl LifecycleTiming {
    pub fn valid(&self) -> bool {
        self.version == 1 && self.intervals.len() <= MAX_LIFECYCLE_INTERVALS && self.omitted_intervals.len() <= 5
            && self.intervals.iter().all(|i| !i.node_id.is_empty() && i.node_id.len() <= 256 && !i.clock_domain.is_empty() && i.clock_domain.len() <= 256 && i.observations > 0
                && (i.state != LifecycleIntervalState::Completed || matches!((i.start_monotonic_ms, i.end_monotonic_ms, i.duration_ms), (Some(start), Some(end), Some(duration)) if end >= start && duration == end - start)))
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LifecycleObservation {
    pub id: String,
    #[serde(flatten)]
    pub interval: LifecycleInterval,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleLedger {
    pub version: u8,
    pub records: Vec<LifecycleObservation>,
    #[serde(default)]
    pub omitted_intervals: BTreeMap<LifecycleStage, u32>,
}
impl Default for LifecycleLedger {
    fn default() -> Self {
        Self {
            version: 1,
            records: Vec::new(),
            omitted_intervals: BTreeMap::new(),
        }
    }
}
impl LifecycleLedger {
    pub fn snapshot(&self) -> LifecycleTiming {
        LifecycleTiming {
            version: 1,
            intervals: self.records.iter().map(|r| r.interval.clone()).collect(),
            omitted_intervals: self.omitted_intervals.clone(),
        }
    }
    pub fn valid(&self) -> bool {
        self.version == 1
            && self.snapshot().valid()
            && self
                .records
                .iter()
                .all(|r| !r.id.is_empty() && r.id.len() <= 192)
            && self
                .records
                .iter()
                .map(|r| &r.id)
                .collect::<BTreeSet<_>>()
                .len()
                == self.records.len()
    }
    /// Replay unions stable observation identities. A completed worker lane
    /// refines a previously open observation; stale imports cannot reopen it.
    pub fn merge(&mut self, incoming: &Self) {
        if !incoming.valid() {
            return;
        }
        for (stage, count) in &incoming.omitted_intervals {
            self.omitted_intervals
                .entry(*stage)
                .and_modify(|v| *v = (*v).max(*count))
                .or_insert(*count);
        }
        for new in &incoming.records {
            if let Some(old) = self.records.iter_mut().find(|old| old.id == new.id) {
                if (old.interval.state != LifecycleIntervalState::Completed
                    && new.interval.state == LifecycleIntervalState::Completed)
                    || (old.interval.state == LifecycleIntervalState::Running
                        && new.interval.state != LifecycleIntervalState::Running)
                {
                    *old = new.clone();
                }
            } else {
                self.records.push(new.clone());
            }
        }
        self.bound();
    }
    pub fn bound(&mut self) {
        if self.records.len() <= MAX_LIFECYCLE_INTERVALS {
            return;
        }
        // Select deterministically, retaining an occurrence of every stage and
        // prioritising open observations. Never manufacture a cross-clock span.
        self.records.sort_by(|a, b| {
            (b.interval.state == LifecycleIntervalState::Running)
                .cmp(&(a.interval.state == LifecycleIntervalState::Running))
                .then_with(|| b.interval.start_unix_ms.cmp(&a.interval.start_unix_ms))
                .then_with(|| a.id.cmp(&b.id))
        });
        let mut stages = BTreeSet::new();
        let mut selected = BTreeSet::new();
        for record in &self.records {
            if stages.insert(record.interval.stage) {
                selected.insert(record.id.clone());
            }
        }
        for record in &self.records {
            if selected.len() == MAX_LIFECYCLE_INTERVALS {
                break;
            }
            selected.insert(record.id.clone());
        }
        let mut omitted = BTreeMap::<LifecycleStage, u32>::new();
        self.records.retain(|r| {
            if selected.contains(&r.id) {
                true
            } else {
                let count = omitted.entry(r.interval.stage).or_default();
                *count = count.saturating_add(r.interval.observations);
                false
            }
        });
        for (stage, count) in omitted {
            self.omitted_intervals
                .entry(stage)
                .and_modify(|v| *v = (*v).max(count))
                .or_insert(count);
        }
        self.records.sort_by(|a, b| {
            a.interval
                .start_unix_ms
                .cmp(&b.interval.start_unix_ms)
                .then_with(|| a.id.cmp(&b.id))
        });
    }
}
impl Job {
    pub fn lifecycle_ledger(&self) -> Option<LifecycleLedger> {
        let value = &self
            .params
            .iter()
            .find(|(key, _)| key == LIFECYCLE_TIMING_PARAM)?
            .1;
        if value.len() > 128 * 1024 {
            return None;
        }
        let ledger: LifecycleLedger = serde_json::from_str(value).ok()?;
        ledger.valid().then_some(ledger)
    }
    pub fn set_lifecycle_ledger(&mut self, ledger: &LifecycleLedger) {
        if !ledger.valid() {
            return;
        }
        if let Ok(value) = serde_json::to_string(ledger) {
            self.params.retain(|(key, _)| key != LIFECYCLE_TIMING_PARAM);
            self.params.push((LIFECYCLE_TIMING_PARAM.into(), value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_merge_exposes_omissions_and_retains_every_major_stage() {
        let stages = [
            LifecycleStage::Queue,
            LifecycleStage::UrlFetch,
            LifecycleStage::Download,
            LifecycleStage::PostProcessing,
        ];
        let mut ledger = LifecycleLedger::default();
        for i in 0..160 {
            ledger.records.push(LifecycleObservation {
                id: format!("observation-{i}"),
                interval: LifecycleInterval {
                    stage: stages[i % 4],
                    node_id: "node".into(),
                    clock_domain: format!("boot-{i}"),
                    start_unix_ms: i as u64,
                    end_unix_ms: Some(i as u64 + 1),
                    start_monotonic_ms: Some(0),
                    end_monotonic_ms: Some(1),
                    duration_ms: Some(1),
                    state: LifecycleIntervalState::Completed,
                    observations: 1,
                },
            });
        }
        ledger.bound();
        assert!(ledger.valid());
        assert_eq!(ledger.records.len(), 128);
        assert_eq!(ledger.omitted_intervals.len(), 4);
        assert_eq!(
            ledger
                .records
                .iter()
                .map(|r| r.interval.stage)
                .collect::<BTreeSet<_>>()
                .len(),
            4
        );
        let snapshot = ledger.clone();
        ledger.merge(&snapshot);
        assert_eq!(ledger, snapshot);
    }
    #[test]
    fn malformed_or_unbounded_internal_timing_cannot_be_read_as_evidence() {
        let ledger = LifecycleLedger {
            version: 2,
            ..Default::default()
        };
        assert!(!ledger.valid());
        let timing = LifecycleTiming {
            version: 1,
            intervals: vec![],
            omitted_intervals: BTreeMap::new(),
        };
        assert!(timing.valid());
        assert!(serde_json::from_str::<LifecycleTiming>(
            r#"{"version":1,"intervals":[],"omittedIntervals":{"invented_stage":1}}"#
        )
        .is_err());
    }
}
