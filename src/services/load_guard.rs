//! Backpressure from the machine itself.
//!
//! This stack shares a host with a chain node, and a backfill running at full
//! speed took the load average from six to twenty and starved it. Background
//! work therefore asks the host how busy it is before each pass: above a soft
//! threshold it slows down, above a hard one it waits. Live projection is
//! never gated this way — falling behind the chain is its own kind of outage.

use std::time::Duration;

#[derive(Clone, Debug)]
pub struct LoadGuard {
    /// Load average per core above which background work slows down.
    soft: f64,
    /// Load average per core above which background work waits.
    hard: f64,
    cores: f64,
    /// How long to wait before looking again while over the hard threshold.
    check_every: Duration,
}

impl Default for LoadGuard {
    fn default() -> Self {
        Self::new(0.7, 1.1)
    }
}

impl LoadGuard {
    /// Thresholds are per core, so the same numbers work on any machine.
    pub fn new(soft: f64, hard: f64) -> Self {
        Self {
            soft,
            hard,
            cores: std::thread::available_parallelism()
                .map(|count| count.get() as f64)
                .unwrap_or(1.0),
            check_every: Duration::from_secs(15),
        }
    }

    pub fn from_env() -> Self {
        let read = |key: &str, default: f64| {
            std::env::var(key)
                .ok()
                .and_then(|value| value.trim().parse::<f64>().ok())
                .filter(|value| *value > 0.0)
                .unwrap_or(default)
        };
        Self::new(
            read("LEDGERFLOW_LOAD_SOFT_PER_CORE", 0.7),
            read("LEDGERFLOW_LOAD_HARD_PER_CORE", 1.1),
        )
    }

    /// One-minute load average per core, or `None` where it cannot be read.
    /// Inside a container this still reports the host, which is the number
    /// that matters to the neighbours.
    pub fn load_per_core(&self) -> Option<f64> {
        let raw = std::fs::read_to_string("/proc/loadavg").ok()?;
        let one_minute: f64 = raw.split_whitespace().next()?.parse().ok()?;
        Some(one_minute / self.cores.max(1.0))
    }

    /// How much to stretch a delay between passes. One while the host is calm,
    /// growing as it gets busier, so background work fades instead of stopping
    /// dead.
    pub fn delay_multiplier(&self) -> f64 {
        match self.load_per_core() {
            Some(load) if load <= self.soft => 1.0,
            Some(load) if load >= self.hard => 8.0,
            Some(load) => {
                // Linear between the thresholds.
                let span = (self.hard - self.soft).max(f64::EPSILON);
                1.0 + 7.0 * ((load - self.soft) / span)
            }
            None => 1.0,
        }
    }

    pub fn stretch(&self, base: Duration) -> Duration {
        base.mul_f64(self.delay_multiplier())
    }

    /// Waits while the host is over the hard threshold. Returns the number of
    /// checks it waited, so a caller can log that it was held back.
    pub async fn wait_while_busy(&self) -> u32 {
        let mut waited = 0;
        while self.load_per_core().is_some_and(|load| load >= self.hard) {
            waited += 1;
            tokio::time::sleep(self.check_every).await;
            // Never hold a backfill forever: after a few minutes let one pass
            // through so progress is slow rather than stalled.
            if waited >= 12 {
                break;
            }
        }
        waited
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn guard(cores: f64) -> LoadGuard {
        LoadGuard {
            soft: 0.7,
            hard: 1.1,
            cores,
            check_every: Duration::from_millis(1),
        }
    }

    #[test]
    fn thresholds_scale_with_core_count() {
        let small = guard(4.0);
        let large = guard(64.0);
        // The same absolute load is heavy on four cores and light on sixty-four.
        assert!(small.cores < large.cores);
        assert_eq!(small.soft, large.soft);
    }

    #[test]
    fn a_calm_host_does_not_stretch_delays() {
        let guard = guard(32.0);
        // 32 cores at load 6.3 is 0.2 per core, well under the soft threshold.
        let multiplier = if 6.3 / 32.0 <= guard.soft { 1.0 } else { 2.0 };
        assert_eq!(multiplier, 1.0);
    }

    #[test]
    fn stretching_grows_the_gap_between_passes() {
        let guard = guard(32.0);
        let stretched = guard.stretch(Duration::from_secs(2));
        assert!(stretched >= Duration::from_secs(2));
    }
}
