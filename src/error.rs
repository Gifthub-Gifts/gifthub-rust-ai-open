//! Upstream-provider error type.
//!
//! House style is a hand-written enum with a manual `Display` (no `anyhow`/`thiserror`),
//! mirroring `vpn-watchdog`'s `ProviderError`. The variants map directly onto the failure
//! modes an OpenAI-shaped LLM endpoint produces, so the worker can decide per-variant
//! whether to retry the same provider or fail over to the next one.

#[derive(Debug)]
pub enum ProviderError {
    /// 401/403 — bad or missing credentials.
    Auth(String),
    /// 429 — upstream rate/quota limit, carrying what Google told us about it (per-day vs
    /// per-minute, the real `quotaValue`, and the suggested `retryDelay`). The caller feeds this
    /// to the limiter so it self-calibrates instead of pacing off hardcoded numbers.
    RateLimit(crate::quota::QuotaInfo),
    /// Any other non-2xx response.
    Api { status: u16, body: String },
    /// 2xx but the model returned no `content` (reasoning models do this) — treat as failure.
    Empty,
    /// Transport-level failure (connection refused, DNS, body read).
    Network(String),
    /// Request exceeded the client timeout.
    Timeout,
}

impl ProviderError {
    /// Whether retrying the *same* provider could plausibly help. Provider-level errors
    /// (auth/rate-limit/api/empty) should fail over to the next provider instead.
    pub(crate) fn is_transient(&self) -> bool {
        matches!(self, ProviderError::Network(_) | ProviderError::Timeout)
    }
}

impl std::fmt::Display for ProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProviderError::Auth(m) => write!(f, "auth error: {m}"),
            ProviderError::RateLimit(q) => write!(f, "rate limited: {q}"),
            ProviderError::Api { status, body } => write!(f, "api error {status}: {body}"),
            ProviderError::Empty => write!(f, "empty completion content"),
            ProviderError::Network(m) => write!(f, "network error: {m}"),
            ProviderError::Timeout => write!(f, "request timed out"),
        }
    }
}

impl std::error::Error for ProviderError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_classification() {
        assert!(ProviderError::Timeout.is_transient());
        assert!(ProviderError::Network("reset".into()).is_transient());
        assert!(!ProviderError::RateLimit(crate::quota::QuotaInfo::unknown()).is_transient());
        assert!(!ProviderError::Empty.is_transient());
        assert!(!ProviderError::Api {
            status: 500,
            body: "x".into()
        }
        .is_transient());
    }

    #[test]
    fn display_is_human_readable() {
        assert_eq!(ProviderError::Empty.to_string(), "empty completion content");
        assert_eq!(
            ProviderError::Api {
                status: 502,
                body: "bad gateway".into()
            }
            .to_string(),
            "api error 502: bad gateway"
        );
    }
}
