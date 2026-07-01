//! Parsing Google AI Studio's `429 RESOURCE_EXHAUSTED` body into the three facts we need to
//! self-calibrate rate limits **from Google itself** instead of hardcoding them: which quota was
//! hit (per-day vs per-minute), its real value (`quotaValue`), and how long to wait (`retryDelay`).
//!
//! This is the single place that knows Google's error shape. A real body looks like:
//! ```json
//! {"error":{"code":429,"status":"RESOURCE_EXHAUSTED","details":[
//!   {"@type":".../QuotaFailure","violations":[
//!     {"quotaId":"GenerateRequestsPerDayPerProjectPerModel-FreeTier","quotaValue":"20"}]},
//!   {"@type":".../RetryInfo","retryDelay":"22s"}]}}
//! ```
//! The OpenAI-compat endpoint we call wraps that object in a one-element array (`[{...}]`); the
//! native endpoint returns it bare. Both are handled. Anything unrecognized parses to `unknown()`,
//! so a malformed/absent body never panics and never fabricates a limit.

use std::time::Duration;

/// Which quota Google reported exhausted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaScope {
    /// `...RequestsPerDay...` — the daily cap; `quotaValue` is the real RPD.
    PerDay,
    /// `...RequestsPerMinute...` — the per-minute cap; `quotaValue` is the real RPM.
    PerMinute,
    /// A 429 we couldn't classify (no `QuotaFailure`, or an unfamiliar `quotaId`).
    Unknown,
}

/// The facts extracted from a 429 body.
#[derive(Debug, Clone)]
pub struct QuotaInfo {
    pub scope: QuotaScope,
    /// `quotaValue` — the real limit Google enforces, when present.
    pub limit: Option<u32>,
    /// `RetryInfo.retryDelay` — how long Google says to wait, when present.
    pub retry_after: Option<Duration>,
}

impl QuotaInfo {
    /// A 429 we couldn't extract anything useful from.
    pub fn unknown() -> Self {
        Self {
            scope: QuotaScope::Unknown,
            limit: None,
            retry_after: None,
        }
    }
}

impl std::fmt::Display for QuotaInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let scope = match self.scope {
            QuotaScope::PerDay => "per-day",
            QuotaScope::PerMinute => "per-minute",
            QuotaScope::Unknown => "unclassified",
        };
        write!(f, "{scope} quota")?;
        if let Some(l) = self.limit {
            write!(f, " (limit {l})")?;
        }
        if let Some(d) = self.retry_after {
            write!(f, ", retry in {:.0}s", d.as_secs_f64())?;
        }
        Ok(())
    }
}

/// Parse a Google 429 body. Never panics; unrecognized input → [`QuotaInfo::unknown`].
pub fn parse_429(body: &str) -> QuotaInfo {
    let v: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => return QuotaInfo::unknown(),
    };
    // OpenAI-compat wraps the error object in a top-level array; native returns it bare.
    let err = if v.is_array() {
        &v[0]["error"]
    } else {
        &v["error"]
    };
    let Some(details) = err["details"].as_array() else {
        return QuotaInfo::unknown();
    };

    let mut info = QuotaInfo::unknown();
    for d in details {
        let ty = d["@type"].as_str().unwrap_or("");
        if ty.ends_with("QuotaFailure") {
            if let Some(violation) = d["violations"].as_array().and_then(|a| a.first()) {
                let qid = violation["quotaId"].as_str().unwrap_or("");
                if qid.contains("PerDay") {
                    info.scope = QuotaScope::PerDay;
                } else if qid.contains("PerMinute") {
                    info.scope = QuotaScope::PerMinute;
                }
                // Only a *Requests* quota's `quotaValue` is a request count we can pace off. Gemini
                // also enforces token quotas (TPM/TPD) whose quotaId carries the same PerMinute/PerDay
                // window but whose `quotaValue` is a token count (often hundreds of thousands) —
                // learning that as an rpm/rpd would blow the request pacer wide open. So capture a
                // limit only for request quotas; a token 429 still parks the provider for its window
                // (via `scope`), just without a bogus number.
                if qid.contains("Request") {
                    // quotaValue arrives as a JSON string ("20"); tolerate a number too.
                    info.limit = violation["quotaValue"]
                        .as_str()
                        .and_then(|s| s.parse::<u32>().ok())
                        .or_else(|| violation["quotaValue"].as_u64().map(|n| n as u32));
                }
            }
        } else if ty.ends_with("RetryInfo") {
            info.retry_after = d["retryDelay"].as_str().and_then(parse_go_duration);
        }
    }
    info
}

/// Parse a protobuf `Duration` string like `"22s"` or `"1.5s"` into a [`Duration`]. Google emits
/// whole or fractional seconds with a trailing `s`.
fn parse_go_duration(s: &str) -> Option<Duration> {
    let secs: f64 = s.strip_suffix('s')?.trim().parse().ok()?;
    // Bound BEFORE constructing: `Duration::from_secs_f64` panics on NaN or a magnitude that
    // overflows (e.g. `"1e30s"` parses to a finite, positive f64 but still aborts the conversion) —
    // and with `panic = "abort"` in the release profile that would take the whole process down over
    // one malformed response body. A real `retryDelay` never legitimately exceeds a day; anything
    // outside `[0, 1 day]` is treated as absent (callers fall back to their default backoff).
    if (0.0..=86_400.0).contains(&secs) {
        Some(Duration::from_secs_f64(secs))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The exact body captured from Google (native endpoint) for gemini-2.5-flash.
    const NATIVE_429: &str = r#"{
      "error": {
        "code": 429,
        "message": "You exceeded your current quota ... Please retry in 22.7s.",
        "status": "RESOURCE_EXHAUSTED",
        "details": [
          {"@type": "type.googleapis.com/google.rpc.Help", "links": []},
          {"@type": "type.googleapis.com/google.rpc.QuotaFailure",
           "violations": [{
             "quotaMetric": "generativelanguage.googleapis.com/generate_content_free_tier_requests",
             "quotaId": "GenerateRequestsPerDayPerProjectPerModel-FreeTier",
             "quotaValue": "20"}]},
          {"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "22s"}
        ]
      }
    }"#;

    #[test]
    fn parses_native_per_day_body() {
        let q = parse_429(NATIVE_429);
        assert_eq!(q.scope, QuotaScope::PerDay);
        assert_eq!(q.limit, Some(20));
        assert_eq!(q.retry_after, Some(Duration::from_secs(22)));
    }

    #[test]
    fn parses_openai_compat_array_wrapped_body() {
        // The OpenAI-compat endpoint wraps the same error in a one-element array.
        let wrapped = format!("[{NATIVE_429}]");
        let q = parse_429(&wrapped);
        assert_eq!(q.scope, QuotaScope::PerDay);
        assert_eq!(q.limit, Some(20));
        assert_eq!(q.retry_after, Some(Duration::from_secs(22)));
    }

    #[test]
    fn classifies_per_minute() {
        let body = r#"{"error":{"details":[
            {"@type":"type.googleapis.com/google.rpc.QuotaFailure","violations":[
              {"quotaId":"GenerateRequestsPerMinutePerProjectPerModel-FreeTier","quotaValue":"15"}]},
            {"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"7.5s"}]}}"#;
        let q = parse_429(body);
        assert_eq!(q.scope, QuotaScope::PerMinute);
        assert_eq!(q.limit, Some(15));
        assert_eq!(q.retry_after, Some(Duration::from_secs_f64(7.5)));
    }

    #[test]
    fn token_quota_sets_window_but_not_a_request_limit() {
        // A TPM (tokens/minute) 429 shares the PerMinute window, but its quotaValue is a token count
        // (250k here). It must park the provider (scope recognized) yet learn NO rpm — otherwise the
        // request pacer would be reset to a token-scale number and stop throttling requests.
        let body = r#"{"error":{"details":[
            {"@type":"type.googleapis.com/google.rpc.QuotaFailure","violations":[
              {"quotaId":"GenerateContentInputTokensPerModelPerMinute-FreeTier","quotaValue":"250000"}]},
            {"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"30s"}]}}"#;
        let q = parse_429(body);
        assert_eq!(q.scope, QuotaScope::PerMinute); // window recognized -> we back off
        assert_eq!(q.limit, None); // but the token count is NOT learned as an rpm
        assert_eq!(q.retry_after, Some(Duration::from_secs(30)));
    }

    #[test]
    fn unrecognized_bodies_are_unknown_not_panics() {
        assert_eq!(parse_429("not json").scope, QuotaScope::Unknown);
        assert_eq!(parse_429("{}").scope, QuotaScope::Unknown);
        assert_eq!(
            parse_429(r#"{"error":{"code":429}}"#).scope,
            QuotaScope::Unknown
        );
        assert!(parse_429("{}").limit.is_none());
    }

    #[test]
    fn go_duration_parsing() {
        assert_eq!(parse_go_duration("22s"), Some(Duration::from_secs(22)));
        assert_eq!(
            parse_go_duration("1.5s"),
            Some(Duration::from_secs_f64(1.5))
        );
        assert_eq!(parse_go_duration("0s"), Some(Duration::ZERO));
        assert_eq!(parse_go_duration("22"), None); // no trailing 's'
        assert_eq!(parse_go_duration("abcs"), None);
        // Magnitude/overflow must NOT panic (panic=abort would kill the whole process) — treated as
        // absent, so the caller falls back to its default backoff.
        assert_eq!(parse_go_duration("1e30s"), None);
        assert_eq!(parse_go_duration("999999999s"), None); // > 1 day
        assert_eq!(parse_go_duration("-5s"), None);
        assert_eq!(parse_go_duration("infs"), None); // "inf" parses to a float -> rejected, no panic
    }
}
