//! Process configuration, loaded once from the environment.
//!
//! House idiom (both reference bots): a flat `Config` struct, a `from_env()` constructor, and a
//! `OnceLock` accessor returning `&'static Config`. Required vars `.expect("X must be set")` so a
//! misconfigured service fails loudly at boot; optional vars parse-or-default.
//!
//! Providers are fully config-driven. `AI_PROVIDERS` is an ordered, comma-separated list; for each
//! `NAME` the loader reads `NAME_BASE_URL` / `NAME_API_KEY` / `NAME_MODEL` (required) and
//! `NAME_RPM` / `NAME_RPD` (optional, falling back to `AI_DEFAULT_RPM`/`AI_DEFAULT_RPD`).

use std::collections::HashMap;
use std::env;
use std::sync::OnceLock;

use dotenv::dotenv;

use crate::provider::Provider;

static CONFIG: OnceLock<Config> = OnceLock::new();

/// Global accessor; initialises from the environment on first call.
pub fn get_config() -> &'static Config {
    CONFIG.get_or_init(Config::from_env)
}

pub struct Config {
    pub redis_url: String,
    pub jobs_key: String,
    pub result_prefix: String,
    pub result_ttl_secs: u64,
    pub pop_timeout_secs: u64,
    pub max_concurrency: usize,
    pub connect_timeout_secs: u64,
    pub request_timeout_secs: u64,
    pub max_attempts_per_provider: u32,

    pub providers: HashMap<String, Provider>,
    /// Declared order of `AI_PROVIDERS` (also the default failover chain).
    pub provider_order: Vec<String>,
    pub default_chain: Vec<String>,
    /// Per-task failover chains keyed by task name (e.g. `writer`, `judge`, `generator`,
    /// `translator`, `reviewer`), discovered generically from `AI_CHAIN_<TASK>` env vars.
    pub chains: HashMap<String, Vec<String>>,
}

impl Config {
    pub fn from_env() -> Self {
        dotenv().ok();

        let provider_order: Vec<String> = env::var("AI_PROVIDERS")
            .expect("AI_PROVIDERS must be set (comma-separated, e.g. openrouter,cloudflare,google)")
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect();
        assert!(
            !provider_order.is_empty(),
            "AI_PROVIDERS must list at least one provider"
        );

        let default_rpm = parse_or("AI_DEFAULT_RPM", 20u32);
        let default_rpd = parse_or("AI_DEFAULT_RPD", 1000u32);

        let mut providers = HashMap::new();
        for name in &provider_order {
            let up = name.to_uppercase();
            let base_url = env::var(format!("{up}_BASE_URL"))
                .unwrap_or_else(|_| panic!("{up}_BASE_URL must be set"));
            let api_key = env::var(format!("{up}_API_KEY"))
                .unwrap_or_else(|_| panic!("{up}_API_KEY must be set"));
            let model = env::var(format!("{up}_MODEL"))
                .unwrap_or_else(|_| panic!("{up}_MODEL must be set"));
            let rpm = parse_or(&format!("{up}_RPM"), default_rpm);
            let rpd = parse_or(&format!("{up}_RPD"), default_rpd);
            // Optional: only sent when set (Cloudflare needs it or it truncates at ~256 tokens).
            let max_tokens = env::var(format!("{up}_MAX_TOKENS"))
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok());
            providers.insert(
                name.clone(),
                Provider {
                    name: name.clone(),
                    base_url,
                    api_key,
                    model,
                    rpm,
                    rpd,
                    max_tokens,
                },
            );
        }

        let default_chain =
            parse_chain("AI_CHAIN_DEFAULT", &providers).unwrap_or_else(|| provider_order.clone());

        // Generic per-task chains: any `AI_CHAIN_<TASK>` env var becomes a chain keyed by the
        // lowercased task (writer, judge, generator, translator, reviewer, …). `AI_CHAIN_DEFAULT`
        // is the fallback (handled above) and is skipped here. A job's `purpose` selects the chain.
        let mut chains = HashMap::new();
        for (key, _) in env::vars() {
            if let Some(task) = chain_task_name(&key) {
                if let Some(c) = parse_chain(&key, &providers) {
                    chains.insert(task, c);
                }
            }
        }

        Config {
            redis_url: env::var("REDIS_URL").expect("REDIS_URL must be set"),
            jobs_key: env::var("AI_JOBS_KEY").unwrap_or_else(|_| "ai_jobs".to_string()),
            result_prefix: env::var("AI_RESULT_PREFIX")
                .unwrap_or_else(|_| "ai_result:".to_string()),
            result_ttl_secs: parse_or("AI_RESULT_TTL_SECS", 300u64),
            pop_timeout_secs: parse_or("POP_TIMEOUT_SECS", 5u64),
            max_concurrency: parse_or("MAX_CONCURRENCY", 4usize),
            connect_timeout_secs: parse_or("CONNECT_TIMEOUT_SECS", 15u64),
            request_timeout_secs: parse_or("REQUEST_TIMEOUT_SECS", 60u64),
            max_attempts_per_provider: parse_or("MAX_ATTEMPTS_PER_PROVIDER", 2u32),
            providers,
            provider_order,
            default_chain,
            chains,
        }
    }

    /// The failover chain for a job's `purpose`, falling back to the default chain.
    pub fn chain_for(&self, purpose: Option<&str>) -> &[String] {
        match purpose.and_then(|p| self.chains.get(p)) {
            Some(chain) => chain,
            None => &self.default_chain,
        }
    }

    pub fn provider_order_display(&self) -> String {
        self.provider_order.join(", ")
    }
}

/// Parse an env var into `T`, falling back to `default` when unset or unparseable.
fn parse_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// The task name from an `AI_CHAIN_<TASK>` env key, lowercased — or `None` for non-chain keys and
/// the special `AI_CHAIN_DEFAULT` (which is the fallback, not a task).
fn chain_task_name(key: &str) -> Option<String> {
    let task = key.strip_prefix("AI_CHAIN_")?;
    if task.is_empty() || task == "DEFAULT" {
        return None;
    }
    Some(task.to_lowercase())
}

/// Parse a comma-separated chain from `key`'s env value, dropping (with a warning) any name not in
/// `providers`. Returns `None` if the var is unset or resolves to no known providers.
fn parse_chain(key: &str, providers: &HashMap<String, Provider>) -> Option<Vec<String>> {
    parse_chain_str(key, &env::var(key).ok()?, providers)
}

/// Pure core of [`parse_chain`] (no env access), so it's unit-testable.
fn parse_chain_str(
    key: &str,
    raw: &str,
    providers: &HashMap<String, Provider>,
) -> Option<Vec<String>> {
    let chain: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| {
            if s.is_empty() {
                return false;
            }
            let known = providers.contains_key(s);
            if !known {
                log::warn!("{key}: unknown provider '{s}' ignored");
            }
            known
        })
        .collect();
    if chain.is_empty() {
        None
    } else {
        Some(chain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn providers(names: &[&str]) -> HashMap<String, Provider> {
        names
            .iter()
            .map(|n| {
                (
                    n.to_string(),
                    Provider {
                        name: n.to_string(),
                        base_url: String::new(),
                        api_key: String::new(),
                        model: String::new(),
                        rpm: 1,
                        rpd: 1,
                        max_tokens: None,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn chain_task_name_extracts_and_skips_default_and_nonchain() {
        assert_eq!(
            chain_task_name("AI_CHAIN_TRANSLATOR"),
            Some("translator".into())
        );
        assert_eq!(chain_task_name("AI_CHAIN_WRITER"), Some("writer".into()));
        assert_eq!(chain_task_name("AI_CHAIN_DEFAULT"), None);
        assert_eq!(chain_task_name("AI_CHAIN_"), None);
        assert_eq!(chain_task_name("REDIS_URL"), None);
    }

    #[test]
    fn parse_chain_str_orders_and_filters_unknown() {
        let p = providers(&["gemini_lite", "gemini_flash", "cloudflare"]);
        assert_eq!(
            parse_chain_str("k", "gemini_lite, gemini_flash", &p),
            Some(vec!["gemini_lite".into(), "gemini_flash".into()])
        );
        // unknown providers are dropped, known ones kept in order
        assert_eq!(
            parse_chain_str("k", "gemini_lite, nope, cloudflare", &p),
            Some(vec!["gemini_lite".into(), "cloudflare".into()])
        );
        // all-unknown and empty both resolve to None
        assert_eq!(parse_chain_str("k", "nope, nada", &p), None);
        assert_eq!(parse_chain_str("k", "", &p), None);
    }
}
