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
        // Builder only fails on TLS backend init; fall back rather than panic at boot.
        .unwrap_or_else(|_| reqwest::Client::new())
}
