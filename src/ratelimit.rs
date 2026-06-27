//! Per-provider rpm + rpd token buckets, to pool free-tier quotas without burning them.
//!
//! Ported from `telegram-happ-rust`'s `rate_limit.rs` (lazy proportional refill, no background
//! timer), extended to two buckets per provider (per-minute and per-day) and storing tokens as
//! `f64` so the fractional remainder of elapsed time is *not* discarded — the original truncated
//! `last_update = now` on every refill, which under-counts. Quotas matter more here than for a
//! chat bot, so we keep the fraction.
//!
//! `try_acquire` is the whole public surface: it consumes one token from *both* buckets atomically
//! (or neither) and returns whether the call is allowed. A blocked provider is skipped so the
//! worker fails over to the next one — we never sleep to wait for a refill.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Instant;

/// Drop idle entries once the map grows past this, to bound memory across many providers/models.
const CLEANUP_THRESHOLD: usize = 1024;

struct Bucket {
    tokens: f64,
    capacity: f64,
    window_secs: f64,
    last: Instant,
}

impl Bucket {
    fn new(capacity: u32, window_secs: f64, now: Instant) -> Self {
        let capacity = capacity as f64;
        Self {
            tokens: capacity,
            capacity,
            window_secs,
            last: now,
        }
    }

    /// Proportional refill: add `elapsed/window * capacity` tokens, clamped to capacity.
    fn refill(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.last).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens =
                (self.tokens + elapsed / self.window_secs * self.capacity).min(self.capacity);
            self.last = now;
        }
    }

    fn has_token(&self) -> bool {
        self.tokens >= 1.0
    }

    fn take(&mut self) {
        self.tokens -= 1.0;
    }
}

struct ProviderBuckets {
    rpm: Bucket,
    rpd: Bucket,
    last_touch: Instant,
}

static LIMITERS: LazyLock<Mutex<HashMap<String, ProviderBuckets>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Try to spend one request against `provider`'s rpm and rpd buckets.
/// Returns `true` if allowed (a token was taken from both); `false` if either is exhausted.
pub(crate) fn try_acquire(provider: &str, rpm: u32, rpd: u32) -> bool {
    let now = Instant::now();
    // Held only across pure compute — never across `.await` — so a std Mutex is correct here.
    let mut map = LIMITERS.lock().expect("rate limiter mutex poisoned");

    if map.len() > CLEANUP_THRESHOLD {
        // Evict providers idle for more than a full day (their buckets would be full anyway).
        map.retain(|_, b| now.duration_since(b.last_touch).as_secs_f64() < 86_400.0);
    }

    let b = map
        .entry(provider.to_string())
        .or_insert_with(|| ProviderBuckets {
            rpm: Bucket::new(rpm, 60.0, now),
            rpd: Bucket::new(rpd, 86_400.0, now),
            last_touch: now,
        });
    b.last_touch = now;
    b.rpm.refill(now);
    b.rpd.refill(now);

    if b.rpm.has_token() && b.rpd.has_token() {
        b.rpm.take();
        b.rpd.take();
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_up_to_capacity_then_blocks() {
        // Unique provider name so the process-global map can't collide with other tests.
        let p = "test-capacity";
        // rpd generous; rpm is the binding limit at 3.
        assert!(try_acquire(p, 3, 1000));
        assert!(try_acquire(p, 3, 1000));
        assert!(try_acquire(p, 3, 1000));
        // Refill within the same instant adds ~0 tokens, so the 4th is denied.
        assert!(!try_acquire(p, 3, 1000));
    }

    #[test]
    fn daily_bucket_can_be_the_binding_limit() {
        let p = "test-daily";
        assert!(try_acquire(p, 1000, 2));
        assert!(try_acquire(p, 1000, 2));
        assert!(!try_acquire(p, 1000, 2));
    }
}
