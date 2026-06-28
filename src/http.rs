//! Shared reqwest client builder.
//!
//! House rule (from `vpn-watchdog`): never `reqwest::Client::new()` directly — a bare client
//! has no request timeout and a single blackholed upstream can park a task forever. Always
//! build through here so every request carries connect + overall timeouts.

use std::time::Duration;

pub fn build_http_client(connect_timeout_secs: u64, request_timeout_secs: u64) -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(connect_timeout_secs))
        .timeout(Duration::from_secs(request_timeout_secs))
        .build()
        // Builder only fails on TLS backend init. Don't fall back to `Client::new()` — that client
        // has no timeouts (the very thing this fn exists to enforce), and a broken TLS backend means
        // no request would succeed anyway. Fail loudly at boot instead.
        .expect("failed to build HTTP client (TLS backend init error)")
}
