//! Self-calibrating per-provider rate limits — the numbers come from **Google**, not from config.
//!
//! Google AI Studio doesn't expose your quota anywhere queryable (verified: no rate-limit headers on
//! any endpoint). The one place it tells you the truth is a `429`, whose body carries the real
//! `quotaValue`, whether it's a per-day or per-minute cap, and a `retryDelay` (see `quota.rs`). So
//! instead of hardcoding an RPD that's inevitably wrong, we **learn each limit the first time Google
//! rejects us** and pace off that thereafter:
//!
//! - **Per-day is purely reactive.** We call freely — there is *no* local daily cap to hold us back —
//!   and only a `PerDay` 429 stops the provider. Its `quotaValue` *becomes* the learned RPD, so a
//!   generous account is free to run up to its real ceiling (a config cap could never be exceeded, so
//!   it could never discover it was set too low). Recovery is a wall-clock daily rollover.
//! - **Per-minute is a proactive pacer** (a token bucket) so we don't burn quota bursting into 429s;
//!   its capacity is the *learned* RPM once a `PerMinute` 429 reveals it, seeded from config until then.
//!
//! [`budget_snapshot`] publishes the *learned* pool for Django to pace against; [`export_learned`] /
//! [`import_learned`] persist it to Redis so a redeploy doesn't re-learn (and re-burst) from scratch.
//!
//! Ported from `telegram-happ-rust`'s lazy proportional-refill bucket (no background timer, `f64`
//! tokens so the fractional remainder of elapsed time isn't discarded).

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::quota::{QuotaInfo, QuotaScope};

/// Drop idle entries once the map grows past this, to bound memory across many providers/models.
const CLEANUP_THRESHOLD: usize = 1024;

/// Seconds to shift the UTC day so the daily-usage counter rolls at ~midnight Pacific — where
/// Google's free-tier daily quotas reset. Approximate (ignores DST); the exact hour only affects
/// when `remaining_today` refills, not correctness.
const DAILY_RESET_OFFSET_SECS: u64 = 8 * 3600;

/// How long a `PerDay` 429 parks a provider when Google gives no `retryDelay`, and the bounds we
/// clamp its `retryDelay` to. The wall-clock day rollover is the real recovery; this just governs
/// re-probing when we haven't learned the exact limit yet.
const DAILY_BACKOFF: Duration = Duration::from_secs(300);
const DAILY_BACKOFF_MIN: Duration = Duration::from_secs(60);
const DAILY_BACKOFF_MAX: Duration = Duration::from_secs(3600);

/// Same, for a `PerMinute`/unclassified 429 — a short pace-and-retry.
const MINUTE_BACKOFF: Duration = Duration::from_secs(10);
const MINUTE_BACKOFF_MIN: Duration = Duration::from_secs(1);
const MINUTE_BACKOFF_MAX: Duration = Duration::from_secs(90);

/// rpm capacity for a persisted provider no longer present in the current config (should not happen).
const SEED_RPM_FALLBACK: u32 = 10;

/// Outcome of an acquire attempt.
#[derive(Debug)]
pub(crate) enum Acquire {
    /// A per-minute token was taken — proceed with the call.
    Ready,
    /// Per-minute budget is momentarily empty; wait this long for the next token, then retry.
    Wait(Duration),
    /// Per-day budget is spent — a hard stop until the daily window rolls; fail over / error.
    DailyExhausted,
}

/// The learned limits we persist across restarts (only the discovered `quotaValue`s — transient
/// usage/blocks are intentionally not persisted).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LearnedLimits {
    #[serde(skip_serializing_if = "Option::is_none")]
    rpm: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rpd: Option<u32>,
}

struct Bucket {
    tokens: f64,
    capacity: f64,
    window_secs: f64,
    last: Instant,
}

impl Bucket {
    fn new(capacity: u32, window_secs: f64, now: Instant) -> Self {
        let capacity = (capacity as f64).max(1.0);
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

    /// How long until this bucket has a whole token. Clamped to `[50ms, window]` so the caller
    /// always makes progress and never sleeps absurdly long.
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

struct ProviderState {
    /// Proactive per-minute pacer; capacity is the learned rpm once known, else the config seed.
    rpm: Bucket,
    /// Real limits learned from Google's 429 `quotaValue` (None until Google tells us).
    rpm_limit: Option<u32>,
    rpd_limit: Option<u32>,
    /// Requests counted against the current daily window (for `remaining_today`).
    used_today: u32,
    /// Which daily window `used_today` belongs to; a change rolls the counter over.
    day_stamp: u64,
    /// A `PerDay` 429 parks the provider until here (belt-and-suspenders alongside the learned cap).
    daily_blocked_until: Option<Instant>,
    /// A `PerMinute`/unclassified 429 backs the provider off until here.
    minute_blocked_until: Option<Instant>,
    last_touch: Instant,
}

impl ProviderState {
    fn new(rpm_capacity: u32, now: Instant) -> Self {
        Self {
            rpm: Bucket::new(rpm_capacity, 60.0, now),
            rpm_limit: None,
            rpd_limit: None,
            used_today: 0,
            day_stamp: current_day_stamp(),
            daily_blocked_until: None,
            minute_blocked_until: None,
            last_touch: now,
        }
    }

    /// Roll the daily counter over when the wall-clock window advances (this is the daily reset).
    fn roll_day(&mut self, day: u64) {
        if self.day_stamp != day {
            self.day_stamp = day;
            self.used_today = 0;
            self.daily_blocked_until = None;
        }
    }
}

static LIMITERS: LazyLock<Mutex<HashMap<String, ProviderState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The current daily window index, shifted so it rolls at ~Pacific midnight.
fn current_day_stamp() -> u64 {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    secs.saturating_sub(DAILY_RESET_OFFSET_SECS) / 86_400
}

/// Try to spend one request against `provider`. `seed_rpm`/`seed_rpd` are the config estimates used
/// only until Google teaches us the real numbers (and only `seed_rpm` sizes the initial pacer;
/// there is deliberately no local daily cap — see the module docs).
///
/// - [`Acquire::Ready`] — a per-minute token was taken; proceed, then call [`record_success`].
/// - [`Acquire::Wait`] — pace: sleep the returned duration and call again.
/// - [`Acquire::DailyExhausted`] — daily quota spent; fail over / error.
pub(crate) fn try_acquire(provider: &str, seed_rpm: u32, _seed_rpd: u32) -> Acquire {
    let now = Instant::now();
    let day = current_day_stamp();
    // Held only across pure compute — never across `.await` — so a std Mutex is correct here.
    let mut map = LIMITERS.lock().expect("rate limiter mutex poisoned");

    if map.len() > CLEANUP_THRESHOLD {
        // Evict providers idle for more than a full day (their pacers would be full anyway).
        map.retain(|_, b| now.duration_since(b.last_touch).as_secs_f64() < 86_400.0);
    }

    let b = map
        .entry(provider.to_string())
        .or_insert_with(|| ProviderState::new(seed_rpm, now));
    b.last_touch = now;
    b.roll_day(day);

    // Daily block from a 429 (parked until re-probe / day rollover).
    if let Some(until) = b.daily_blocked_until {
        if now < until {
            return Acquire::DailyExhausted;
        }
        b.daily_blocked_until = None;
    }
    // Once we've learned the real RPD and hit it, hold until the day rolls (don't re-trigger the 429).
    if let Some(rpd) = b.rpd_limit {
        if b.used_today >= rpd {
            return Acquire::DailyExhausted;
        }
    }
    // Per-minute block from a 429 (short pace-and-retry).
    if let Some(until) = b.minute_blocked_until {
        if now < until {
            return Acquire::Wait(until - now);
        }
        b.minute_blocked_until = None;
    }
    // Proactive per-minute pacer.
    b.rpm.refill(now);
    if !b.rpm.has_token() {
        return Acquire::Wait(b.rpm.time_to_token());
    }
    b.rpm.take();
    Acquire::Ready
}

/// Count a completed successful call toward today's usage (drives `remaining_today`). Called after
/// [`Acquire::Ready`] yields a real completion, so usage tracks successes, not mere attempts.
pub(crate) fn record_success(provider: &str) {
    let day = current_day_stamp();
    let mut map = LIMITERS.lock().expect("rate limiter mutex poisoned");
    if let Some(b) = map.get_mut(provider) {
        b.roll_day(day);
        b.used_today = b.used_today.saturating_add(1);
    }
}

/// Feed a 429's parsed quota facts back into the limiter so it self-calibrates: learn the real
/// `quotaValue`, and park the provider for the `retryDelay` Google gave (per-day → daily block that
/// holds until the daily rollover once the exact limit is known; per-minute → short backoff).
pub(crate) fn record_quota(provider: &str, info: &QuotaInfo) {
    let now = Instant::now();
    let day = current_day_stamp();
    let mut map = LIMITERS.lock().expect("rate limiter mutex poisoned");
    // The entry normally exists (try_acquire ran first); create defensively if not.
    let seed = info.limit.unwrap_or(SEED_RPM_FALLBACK);
    let b = map
        .entry(provider.to_string())
        .or_insert_with(|| ProviderState::new(seed, now));
    b.last_touch = now;
    b.roll_day(day);

    match info.scope {
        QuotaScope::PerDay => {
            if let Some(limit) = info.limit {
                b.rpd_limit = Some(limit);
                // Reflect exhaustion so the learned-cap check holds the provider until the day rolls.
                b.used_today = b.used_today.max(limit);
            }
            let d = info
                .retry_after
                .unwrap_or(DAILY_BACKOFF)
                .clamp(DAILY_BACKOFF_MIN, DAILY_BACKOFF_MAX);
            b.daily_blocked_until = Some(now + d);
        }
        QuotaScope::PerMinute => {
            if let Some(limit) = info.limit {
                b.rpm_limit = Some(limit);
                // Rebuild the pacer at the real capacity; empty, since we just got rate-limited.
                let mut bucket = Bucket::new(limit, 60.0, now);
                bucket.tokens = 0.0;
                b.rpm = bucket;
            }
            let d = info
                .retry_after
                .unwrap_or(MINUTE_BACKOFF)
                .clamp(MINUTE_BACKOFF_MIN, MINUTE_BACKOFF_MAX);
            b.minute_blocked_until = Some(now + d);
        }
        QuotaScope::Unknown => {
            // Ambiguous 429: brief backoff and retry (don't hard-stop on something we can't classify).
            let d = info
                .retry_after
                .unwrap_or(MINUTE_BACKOFF)
                .clamp(MINUTE_BACKOFF_MIN, MINUTE_BACKOFF_MAX);
            b.minute_blocked_until = Some(now + d);
        }
    }
}

/// Snapshot the shared budget pool across `providers` (`(name, seed_rpm, seed_rpd)` each), for
/// publishing to Django: `(rpm_total, rpd_total, remaining_today)`. Reports the **learned** limit
/// where known, falling back to the config seed until Google has spoken.
///
/// `remaining_today` is 0 for a provider currently daily-blocked. **Until the real daily limit is
/// learned we report the seed *flat* (not `seed − used_today`)** — decrementing an unproven seed would
/// make Django pause at the guess, so the gateway could never exceed it to trigger the `PerDay` 429
/// that reveals the true (often higher) ceiling. Once learned, we report the accurate headroom.
pub(crate) fn budget_snapshot(providers: &[(String, u32, u32)]) -> (u32, u32, u32) {
    let now = Instant::now();
    let day = current_day_stamp();
    let mut map = LIMITERS.lock().expect("rate limiter mutex poisoned");
    let mut rpm_total = 0u32;
    let mut rpd_total = 0u32;
    let mut remaining = 0u32;
    for (name, seed_rpm, seed_rpd) in providers {
        let (rpm, rpd, remain) = match map.get_mut(name) {
            Some(b) => {
                b.roll_day(day);
                let rpm = b.rpm_limit.unwrap_or(*seed_rpm);
                let rpd = b.rpd_limit.unwrap_or(*seed_rpd);
                let blocked = b.daily_blocked_until.is_some_and(|u| now < u);
                let remain = if blocked {
                    0
                } else {
                    match b.rpd_limit {
                        // Real limit known: accurate headroom.
                        Some(limit) => limit.saturating_sub(b.used_today),
                        // Unproven seed: report it flat so discovery isn't throttled to the guess.
                        None => *seed_rpd,
                    }
                };
                (rpm, rpd, remain)
            }
            // Untouched provider: full estimate from the seed.
            None => (*seed_rpm, *seed_rpd, *seed_rpd),
        };
        rpm_total = rpm_total.saturating_add(rpm);
        rpd_total = rpd_total.saturating_add(rpd);
        remaining = remaining.saturating_add(remain);
    }
    (rpm_total, rpd_total, remaining)
}

/// Serialize the learned limits (only providers Google has taught us about) for persistence.
pub(crate) fn export_learned() -> String {
    let map = LIMITERS.lock().expect("rate limiter mutex poisoned");
    let mut out: HashMap<String, LearnedLimits> = HashMap::new();
    for (name, b) in map.iter() {
        if b.rpm_limit.is_some() || b.rpd_limit.is_some() {
            out.insert(
                name.clone(),
                LearnedLimits {
                    rpm: b.rpm_limit,
                    rpd: b.rpd_limit,
                },
            );
        }
    }
    serde_json::to_string(&out).unwrap_or_else(|_| "{}".to_string())
}

/// Load persisted learned limits at startup so a redeploy paces off the last-known real numbers
/// instead of re-learning (re-bursting into 429s). `seeds` sizes each pacer for a `PerDay`-only
/// learned provider (where rpm wasn't yet discovered).
pub(crate) fn import_learned(json: &str, seeds: &[(String, u32, u32)]) {
    let parsed: HashMap<String, LearnedLimits> = match serde_json::from_str(json) {
        Ok(p) => p,
        Err(e) => {
            log::warn!("could not parse persisted learned limits: {e}");
            return;
        }
    };
    let now = Instant::now();
    let mut map = LIMITERS.lock().expect("rate limiter mutex poisoned");
    let mut loaded = 0usize;
    for (name, learned) in parsed {
        let seed_rpm = seeds
            .iter()
            .find(|(n, _, _)| *n == name)
            .map(|(_, r, _)| *r)
            .unwrap_or(SEED_RPM_FALLBACK);
        let mut state = ProviderState::new(learned.rpm.unwrap_or(seed_rpm), now);
        state.rpm_limit = learned.rpm;
        state.rpd_limit = learned.rpd;
        map.insert(name, state);
        loaded += 1;
    }
    if loaded > 0 {
        log::info!("loaded {loaded} persisted provider limit(s)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn per_day(limit: u32, retry_s: u64) -> QuotaInfo {
        QuotaInfo {
            scope: QuotaScope::PerDay,
            limit: Some(limit),
            retry_after: Some(Duration::from_secs(retry_s)),
        }
    }

    fn per_minute(limit: u32, retry_s: u64) -> QuotaInfo {
        QuotaInfo {
            scope: QuotaScope::PerMinute,
            limit: Some(limit),
            retry_after: Some(Duration::from_secs(retry_s)),
        }
    }

    #[test]
    fn paces_at_rpm_then_waits() {
        let p = "rl-capacity";
        assert!(matches!(try_acquire(p, 3, 1_000_000), Acquire::Ready));
        assert!(matches!(try_acquire(p, 3, 1_000_000), Acquire::Ready));
        assert!(matches!(try_acquire(p, 3, 1_000_000), Acquire::Ready));
        // 4th within the same instant: pacer empty -> Wait (never a hard error).
        match try_acquire(p, 3, 1_000_000) {
            Acquire::Wait(d) => assert!(d > Duration::ZERO && d <= Duration::from_secs(60)),
            other => panic!("expected Wait, got {other:?}"),
        }
    }

    #[test]
    fn no_local_daily_cap_until_google_says_so() {
        // A stingy seed_rpd must NOT stop us — only a real PerDay 429 does. This is what lets a
        // generous account discover its true ceiling instead of freezing at the config guess.
        let p = "rl-no-cap";
        for _ in 0..5 {
            assert!(matches!(try_acquire(p, 1000, 2), Acquire::Ready));
            record_success(p);
        }
        // Still serving past seed_rpd=2 because nothing has learned a daily limit.
        assert!(matches!(try_acquire(p, 1000, 2), Acquire::Ready));
    }

    #[test]
    fn learns_daily_limit_from_429_and_stops() {
        let p = "rl-learn-day";
        assert!(matches!(try_acquire(p, 1000, 1000), Acquire::Ready));
        // Google rejects with the real daily limit.
        record_quota(p, &per_day(20, 60));
        assert!(matches!(
            try_acquire(p, 1000, 1000),
            Acquire::DailyExhausted
        ));
        // Budget now reflects the learned 20 (not the 1000 seed) and 0 remaining.
        let (_, rpd, remaining) = budget_snapshot(&[(p.to_string(), 1000, 1000)]);
        assert_eq!(rpd, 20);
        assert_eq!(remaining, 0);
    }

    #[test]
    fn learns_per_minute_limit_and_backs_off() {
        let p = "rl-learn-min";
        assert!(matches!(try_acquire(p, 1000, 1000), Acquire::Ready));
        record_quota(p, &per_minute(15, 5));
        // Parked for the retryDelay -> Wait, not a hard stop.
        match try_acquire(p, 1000, 1000) {
            Acquire::Wait(d) => assert!(d > Duration::ZERO && d <= Duration::from_secs(5)),
            other => panic!("expected Wait, got {other:?}"),
        }
        // Budget reflects the learned rpm (15), not the 1000 seed.
        let (rpm, _, _) = budget_snapshot(&[(p.to_string(), 1000, 1000)]);
        assert_eq!(rpm, 15);
    }

    #[test]
    fn remaining_today_flat_until_learned_then_accurate() {
        let p = "rl-usage";
        let providers = [(p.to_string(), 1000, 100)];
        let (_, _, before) = budget_snapshot(&providers);
        assert_eq!(before, 100); // untouched -> full seed estimate
                                 // Spend a couple: remaining stays FLAT at the seed while the real daily limit is unproven —
                                 // decrementing here would throttle Django to the guess and block discovery of the true cap.
        assert!(matches!(try_acquire(p, 1000, 100), Acquire::Ready));
        record_success(p);
        assert!(matches!(try_acquire(p, 1000, 100), Acquire::Ready));
        record_success(p);
        let (_, _, flat) = budget_snapshot(&providers);
        assert_eq!(flat, 100);
        // Once Google reveals the real daily limit via a PerDay 429, remaining becomes accurate
        // (here: learned 50, and now daily-blocked -> 0 headroom).
        record_quota(p, &per_day(50, 60));
        let (_, rpd, after) = budget_snapshot(&providers);
        assert_eq!(rpd, 50);
        assert_eq!(after, 0);
    }

    #[test]
    fn export_import_round_trips_learned_limits() {
        let p = "rl-persist";
        try_acquire(p, 1000, 1000);
        record_quota(p, &per_day(137, 60));
        record_quota(p, &per_minute(9, 5));
        let json = export_learned();
        assert!(json.contains("rl-persist"));
        assert!(json.contains("137"));

        // Fresh provider name, import the persisted blob -> limits restored.
        let restored = json.replace("rl-persist", "rl-persist-2");
        import_learned(&restored, &[("rl-persist-2".to_string(), 1000, 1000)]);
        let (rpm, rpd, _) = budget_snapshot(&[("rl-persist-2".to_string(), 1000, 1000)]);
        assert_eq!(rpm, 9);
        assert_eq!(rpd, 137);
    }

    #[test]
    fn untouched_provider_reports_full_seed_pool() {
        let (rpm, rpd, remaining) = budget_snapshot(&[
            ("rl-fresh-a".to_string(), 3, 200),
            ("rl-fresh-b".to_string(), 2, 200),
        ]);
        assert_eq!(rpm, 5);
        assert_eq!(rpd, 400);
        assert_eq!(remaining, 400);
    }
}
