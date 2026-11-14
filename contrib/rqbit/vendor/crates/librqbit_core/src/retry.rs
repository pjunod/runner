//! Owned peer/bootstrap retry policy. These fixed policies preserve rqbit 8.1.1's
//! backoff 0.4 timing: symmetric 50% jitter, a cap on the base (not jittered)
//! interval, and a wall-clock 24-hour budget including the proposed sleep.
//! Native std::time::Instant replaces the unmaintained instant abstraction.
//!
//! Nanosecond arithmetic adapted from backoff 0.4 (MIT), copyright 2015
//! András Ihrig. Permission is granted, free of charge, to any person obtaining
//! a copy of this software and associated documentation files, to deal in the
//! Software without restriction, including without limitation the rights to
//! use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
//! of the Software, and to permit persons to whom the Software is furnished to
//! do so, subject to the following conditions: the above copyright notice and
//! this permission notice shall be included in all copies or substantial
//! portions of the Software. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY
//! OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
//! OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
//! IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
//! DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
//! OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
//! USE OR OTHER DEALINGS IN THE SOFTWARE.

use std::time::{Duration, Instant};

const INITIAL: Duration = Duration::from_secs(10);
const BUDGET: Duration = Duration::from_secs(86400);

#[derive(Debug)]
pub struct RetryPolicy {
    interval: Duration,
    maximum: Duration,
    multiplier: f64,
    started: Instant,
}

impl RetryPolicy {
    pub fn peer() -> Self {
        Self::new(6.0, 3600)
    }
    pub fn bootstrap() -> Self {
        Self::new(1.5, 60)
    }

    fn new(multiplier: f64, maximum_secs: u64) -> Self {
        Self {
            interval: INITIAL,
            maximum: Duration::from_secs(maximum_secs),
            multiplier,
            started: Instant::now(),
        }
    }

    pub fn reset(&mut self) {
        self.interval = INITIAL;
        self.started = Instant::now();
    }

    pub fn next_backoff(&mut self) -> Option<Duration> {
        self.next_at(self.started.elapsed(), rand::random())
    }

    fn next_at(&mut self, elapsed: Duration, random: f64) -> Option<Duration> {
        if elapsed > BUDGET {
            return None;
        }
        let nanos =
            |d: Duration| d.as_secs() as f64 * 1_000_000_000.0 + f64::from(d.subsec_nanos());
        let duration = |n: f64| {
            Duration::new(
                (n / 1_000_000_000.0) as u64,
                (n as u64 % 1_000_000_000) as u32,
            )
        };
        let base = nanos(self.interval);
        // Preserve backoff 0.4's inclusive-nanosecond sampling and rounding.
        let delay = duration(base * 0.5 + random * (base + 1.0));
        self.interval = if base >= nanos(self.maximum) / self.multiplier {
            self.maximum
        } else {
            duration(base * self.multiplier)
        };
        (elapsed + delay <= BUDGET).then_some(delay)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_schedule_preserves_growth_jitter_and_base_cap() {
        let mut policy = RetryPolicy::peer();
        for seconds in [10, 60, 360, 2160, 3600, 3600] {
            assert_eq!(
                policy.next_at(Duration::ZERO, 0.0),
                Some(Duration::from_secs(seconds / 2))
            );
        }
        // A one-hour base cap still permits a jittered delay approaching 90m.
        assert_eq!(
            policy.next_at(Duration::ZERO, 0.75),
            Some(Duration::from_secs(4500))
        );
    }

    #[test]
    fn bootstrap_fractional_growth_matches_original_nanosecond_vectors() {
        let mut policy = RetryPolicy::bootstrap();
        for nanos in [
            5_000_000_000,
            7_500_000_000,
            11_250_000_000,
            16_875_000_000,
            25_312_500_000,
            30_000_000_000,
            30_000_000_000,
        ] {
            assert_eq!(
                policy.next_at(Duration::ZERO, 0.0),
                Some(Duration::from_nanos(nanos))
            );
        }
        let mut policy = RetryPolicy::bootstrap();
        assert_eq!(
            policy.next_at(Duration::ZERO, 0.25),
            Some(Duration::from_millis(7500))
        );
        assert_eq!(
            policy.next_at(Duration::ZERO, 0.75),
            Some(Duration::from_millis(18750))
        );
    }

    #[test]
    fn elapsed_budget_includes_work_and_sleep_and_reset_starts_over() {
        let mut policy = RetryPolicy::peer();
        assert_eq!(
            policy.next_at(BUDGET - Duration::from_secs(5), 0.0),
            Some(Duration::from_secs(5))
        );
        assert_eq!(policy.next_at(BUDGET - Duration::from_secs(5), 0.0), None);
        let interval = policy.interval;
        assert_eq!(policy.next_at(BUDGET + Duration::from_nanos(1), 0.0), None);
        assert_eq!(policy.interval, interval);
        policy.started = Instant::now() - BUDGET - Duration::from_secs(1);
        assert_eq!(policy.next_backoff(), None);
        policy.reset();
        assert!(policy.started.elapsed() < Duration::from_secs(1));
        assert_eq!(
            policy.next_at(Duration::ZERO, 0.0),
            Some(Duration::from_secs(5))
        );
    }
}
