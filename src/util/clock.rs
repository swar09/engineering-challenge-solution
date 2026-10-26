use chrono::{DateTime, Utc};
use quanta::{Clock, Instant};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicI64, Ordering};

/// High-resolution, TSC-backed clock that produces monotonic UTC timestamps
/// with minimal OS `clock_gettime` syscall overhead.
pub struct QuantaClock {
    clock: Clock,
    anchor_instant: Instant,
    anchor_utc_micros: AtomicI64,
}

impl QuantaClock {
    /// Creates a new `QuantaClock` anchored to the current UTC wall clock.
    pub fn new() -> Self {
        let clock = Clock::new();
        let anchor_utc = Utc::now();
        let anchor_instant = clock.now();
        Self {
            clock,
            anchor_instant,
            anchor_utc_micros: AtomicI64::new(anchor_utc.timestamp_micros()),
        }
    }

    /// Returns the current local UTC time using the CPU's invariant Time Stamp Counter.
    #[inline]
    pub fn now(&self) -> DateTime<Utc> {
        let elapsed = self.anchor_instant.elapsed();
        let micros = elapsed.as_micros() as i64;
        let base = self.anchor_utc_micros.load(Ordering::Relaxed);
        DateTime::from_timestamp_micros(base.saturating_add(micros)).unwrap_or_else(Utc::now)
    }

    /// Access the underlying `quanta::Clock` instance.
    #[inline]
    pub fn inner(&self) -> &Clock {
        &self.clock
    }
}

impl Default for QuantaClock {
    fn default() -> Self {
        Self::new()
    }
}

/// Global lazy-initialized singleton for low-latency TSC-backed timestamping.
pub static LOCAL_CLOCK: LazyLock<QuantaClock> = LazyLock::new(QuantaClock::new);

/// Convenience function returning the current local UTC timestamp from the invariant TSC.
#[inline]
pub fn local_now() -> DateTime<Utc> {
    LOCAL_CLOCK.now()
}
