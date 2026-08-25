//! Controllable time for deterministic tests and honest timestamps.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch.
pub type UnixMillis = u64;

/// Abstraction over wall-clock reads so tests can freeze or step time
/// instead of sleeping.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> UnixMillis;
}

/// Real system clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> UnixMillis {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            // Before the epoch is not representable in our unsigned type;
            // clamp to zero rather than panicking.
            .unwrap_or(0)
    }
}

/// Manually advanced clock for deterministic tests.
#[derive(Debug, Default)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    #[must_use]
    pub fn starting_at(ms: UnixMillis) -> Self {
        Self(AtomicU64::new(ms))
    }

    pub fn advance(&self, ms: u64) {
        let _ = self.0.fetch_add(ms, Ordering::SeqCst);
    }

    pub fn set(&self, ms: UnixMillis) {
        self.0.store(ms, Ordering::SeqCst);
    }

    /// Shared handle usable wherever `Arc<dyn Clock>` is expected.
    #[must_use]
    pub fn arc(self) -> std::sync::Arc<Self> {
        std::sync::Arc::new(self)
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> UnixMillis {
        self.0.load(Ordering::SeqCst)
    }
}

impl Clock for std::sync::Arc<dyn Clock> {
    fn now_ms(&self) -> UnixMillis {
        (**self).now_ms()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_is_controllable() {
        let clock = ManualClock::starting_at(1_000);
        assert_eq!(clock.now_ms(), 1_000);
        clock.advance(500);
        assert_eq!(clock.now_ms(), 1_500);
        clock.set(10);
        assert_eq!(clock.now_ms(), 10);
    }

    #[test]
    fn system_clock_is_plausible() {
        let now = SystemClock.now_ms();
        // 2020-01-01T00:00:00Z ..= 2100-01-01T00:00:00Z; catches gross breakage.
        assert!(
            (1_577_836_800_000..=4_102_444_800_000).contains(&now),
            "system clock returned implausible value {now}"
        );
    }
}
