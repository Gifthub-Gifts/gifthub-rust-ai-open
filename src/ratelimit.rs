//! Per-provider rpm + rpd token buckets, to pool free-tier quotas without burning them.
//!
//! Ported from `telegram-happ-rust`'s `rate_limit.rs` (lazy proportional refill, no background
//! timer), extended to two buckets per provider (per-minute and per-day) and storing tokens as
//! `f64` so the fractional remainder of elapsed time is *not* discarded — the original truncated
//! `last_update = now` on every refill, which under-counts. Quotas matter more here than for a
//! chat bot, so we keep the fraction.
//!
//! [`try_acquire`] is the core: it consumes one token from *both* buckets atomically (only when both
//! are available) and tells the caller what to do. **Pacing policy (changed):** an rpm-limited call
//! returns [`Acquire::Wait`] with the time until the next token so the caller *waits and retries*
//! instead of dropping the job — jobs buffer in the Redis list and drain at the sustainable rate.
//! Only the daily ([`Acquire::DailyExhausted`]) cap is a hard stop (it won't refill in any useful
//! window, so the caller fails over / errors). [`budget_snapshot`] exposes the live pool so the
//! gateway can publish it for Django's dispatcher to pace against.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

/// Drop idle entries once the map grows past this, to bound memory across many providers/models.
const CLEANUP_THRESHOLD: usize = 1024;

/// Outcome of an acquire attempt.
#[derive(Debug)]
pub(crate) enum Acquire {
    /// A token was taken from both buckets — proceed with the call.
    Ready,
    /// Per-minute budget is momentarily empty; wait this long for the next token, then retry.
    Wait(Duration),
    /// Per-day budget is spent — a hard stop until the daily window rolls; fail over / error.
    DailyExhausted,
}

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

    /// How long until this bucket has a whole token, given its refill rate. Clamped to
    /// `[50ms, window]` so the caller always makes progress and never sleeps absurdly long.
    fn time_to_token(&self) -> Duration {
        let deficit = (1.0 - self.tokens).max(0.0);
        let rate = self.capacity / self.window_secs; // tokens per second
        let secs = if rate > 0.0 {
            deficit / rate
        } else {
            self.window_secs
        };
        Duration::from_secs_f64(secs.clamp(0.05, self.window_secs))
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
///
/// - [`Acquire::Ready`] — a token was taken from both buckets; proceed.
/// - [`Acquire::Wait`] — rpd has budget but rpm is momentarily empty; sleep the returned duration
///   and call again (this is the pacing that replaces the old "skip and error").
/// - [`Acquire::DailyExhausted`] — rpd is spent; a hard stop (fail over to another provider / error).
pub(crate) fn try_acquire(provider: &str, rpm: u32, rpd: u32) -> Acquire {
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

    // Daily cap is the hard stop: don't wait for it (it refills over a day), fail over instead.
    if !b.rpd.has_token() {
        return Acquire::DailyExhausted;
    }
    // Per-minute cap: pace, don't drop — tell the caller how long until the next token.
    if !b.rpm.has_token() {
        return Acquire::Wait(b.rpm.time_to_token());
    }
    b.rpm.take();
    b.rpd.take();
    Acquire::Ready
}

/// Snapshot the shared budget pool across `providers` (`(name, rpm, rpd)` each), for publishing to
/// Django: `(rpm_total, rpd_total, remaining_today)`. `remaining_today` is the summed whole tokens
/// left in the per-day buckets (a provider not yet seen counts as a full day's `rpd`). Reads/refreshes
/// the same `LIMITERS` the live calls spend from, so it reflects real remaining quota.
pub(crate) fn budget_snapshot(providers: &[(String, u32, u32)]) -> (u32, u32, u32) {
    let now = Instant::now();
    let mut map = LIMITERS.lock().expect("rate limiter mutex poisoned");
    let mut rpm_total = 0u32;
    let mut rpd_total = 0u32;
    let mut remaining = 0u32;
    for (name, rpm, rpd) in providers {
        rpm_total = rpm_total.saturating_add(*rpm);
        rpd_total = rpd_total.saturating_add(*rpd);
        let tokens = match map.get_mut(name) {
            Some(b) => {
                b.rpd.refill(now);
                b.rpd.tokens
            }
            None => *rpd as f64, // untouched provider still has its full daily budget
        };
        remaining = remaining.saturating_add(tokens.max(0.0).floor() as u32);
    }
    (rpm_total, rpd_total, remaining)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_up_to_capacity_then_paces() {
        // Unique provider name so the process-global map can't collide with other tests.
        let p = "test-capacity";
        // rpd generous; rpm is the binding limit at 3.
        assert!(matches!(try_acquire(p, 3, 1000), Acquire::Ready));
        assert!(matches!(try_acquire(p, 3, 1000), Acquire::Ready));
        assert!(matches!(try_acquire(p, 3, 1000), Acquire::Ready));
        // Refill within the same instant adds ~0 tokens, so the 4th waits (it does NOT error).
        match try_acquire(p, 3, 1000) {
            Acquire::Wait(d) => assert!(d > Duration::ZERO && d <= Duration::from_secs(60)),
            other => panic!("expected Wait, got {other:?}"),
        }
    }

    #[test]
    fn daily_exhaustion_is_a_hard_stop() {
        let p = "test-daily";
        assert!(matches!(try_acquire(p, 1000, 2), Acquire::Ready));
        assert!(matches!(try_acquire(p, 1000, 2), Acquire::Ready));
        // rpm has plenty, but the daily bucket is spent -> hard stop (not Wait).
        assert!(matches!(try_acquire(p, 1000, 2), Acquire::DailyExhausted));
    }

    #[test]
    fn budget_snapshot_sums_and_reflects_spend() {
        let providers = vec![
            ("snap_lite".to_string(), 3u32, 200u32),
            ("snap_flash".to_string(), 2u32, 200u32),
        ];
        // Untouched: full pool.
        let (rpm, rpd, remaining) = budget_snapshot(&providers);
        assert_eq!(rpm, 5);
        assert_eq!(rpd, 400);
        assert_eq!(remaining, 400);
        // Spend two from lite; remaining_today drops by two.
        assert!(matches!(try_acquire("snap_lite", 3, 200), Acquire::Ready));
        assert!(matches!(try_acquire("snap_lite", 3, 200), Acquire::Ready));
        let (_, _, remaining) = budget_snapshot(&providers);
        assert_eq!(remaining, 398);
    }
}
