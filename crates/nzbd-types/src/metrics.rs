//! Process-local counters that cross a crate boundary.
//!
//! Post-processing stage timings are measured in `nzbd-post` and exposed
//! on `/metrics` by `nzbd-api`, and neither crate depends on the other.
//! The shared counter lives here — in the crate both already depend on —
//! rather than becoming a new dependency edge or a global registry.

use crate::PostStage;
use std::sync::atomic::{AtomicU64, Ordering};

/// Per-stage duration accumulator, exported as a Prometheus summary
/// (`_count` + `_sum`). A summary, not a histogram: the useful question
/// is "is verify suddenly taking minutes", which mean-per-stage answers
/// at a fraction of the cardinality.
#[derive(Default)]
pub struct PpStageStats {
    /// Indexed by position in [`PostStage::ALL`].
    count: [AtomicU64; PostStage::ALL.len()],
    millis: [AtomicU64; PostStage::ALL.len()],
}

impl PpStageStats {
    pub fn new() -> PpStageStats {
        PpStageStats::default()
    }

    fn index(stage: PostStage) -> usize {
        PostStage::ALL
            .iter()
            .position(|s| *s == stage)
            .unwrap_or_default()
    }

    /// Record one completed run of `stage`.
    pub fn record(&self, stage: PostStage, elapsed: std::time::Duration) {
        let i = Self::index(stage);
        self.count[i].fetch_add(1, Ordering::Relaxed);
        self.millis[i].fetch_add(
            elapsed.as_millis().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    /// `(stage, runs, total seconds)` for every stage that has ever run.
    /// Stages that never ran are omitted: a metric that is always zero
    /// only teaches a dashboard to ignore it.
    pub fn snapshot(&self) -> Vec<(PostStage, u64, f64)> {
        PostStage::ALL
            .iter()
            .enumerate()
            .filter_map(|(i, stage)| {
                let n = self.count[i].load(Ordering::Relaxed);
                if n == 0 {
                    return None;
                }
                let secs = self.millis[i].load(Ordering::Relaxed) as f64 / 1000.0;
                Some((*stage, n, secs))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_per_stage_and_hides_stages_that_never_ran() {
        let s = PpStageStats::new();
        s.record(PostStage::Unpack, std::time::Duration::from_millis(1500));
        s.record(PostStage::Unpack, std::time::Duration::from_millis(500));
        s.record(PostStage::ParVerify, std::time::Duration::from_millis(250));

        let snap = s.snapshot();
        assert_eq!(snap.len(), 2, "only the stages that ran are reported");
        let unpack = snap
            .iter()
            .find(|(st, ..)| *st == PostStage::Unpack)
            .unwrap();
        assert_eq!(unpack.1, 2);
        assert!((unpack.2 - 2.0).abs() < f64::EPSILON);
    }
}
