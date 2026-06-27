//! The Django <-> Rust job contract carried over Redis.
//!
//! Django (the producer) enqueues a [`Job`] as JSON onto the `ai_jobs` list; the gateway
//! pops it, runs the provider failover chain, and pushes a [`JobResult`] back onto
//! `ai_result:{job_id}`. Django owns every database write — the gateway only computes.

use serde::{Deserialize, Serialize};

/// One chat message, passed through verbatim to the OpenAI-shaped `messages` array.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: String,
}

/// A unit of work enqueued by Django.
#[derive(Debug, Deserialize)]
pub struct Job {
    pub job_id: String,
    /// Selects the failover chain (e.g. `"writer"` vs `"judge"`); falls back to the default chain.
    #[serde(default)]
    pub purpose: Option<String>,
    pub messages: Vec<Message>,
}

/// The gateway's reply. Exactly one of `content` / `error` is set.
#[derive(Debug, Serialize)]
pub struct JobResult {
    pub job_id: String,
    pub content: Option<String>,
    pub error: Option<String>,
    /// Which provider produced the content (None on failure).
    pub provider: Option<String>,
    /// Cloudflare neuron cost, when the provider reported it (`cf-ai-neurons` header).
    pub neurons: Option<f64>,
}

impl JobResult {
    pub(crate) fn ok(job_id: &str, content: String, provider: &str, neurons: Option<f64>) -> Self {
        Self {
            job_id: job_id.to_string(),
            content: Some(content),
            error: None,
            provider: Some(provider.to_string()),
            neurons,
        }
    }

    pub(crate) fn err(job_id: &str, error: String) -> Self {
        Self {
            job_id: job_id.to_string(),
            content: None,
            error: Some(error),
            provider: None,
            neurons: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_deserializes_with_optional_purpose() {
        let raw = r#"{"job_id":"abc","messages":[{"role":"user","content":"hi"}]}"#;
        let job: Job = serde_json::from_str(raw).unwrap();
        assert_eq!(job.job_id, "abc");
        assert!(job.purpose.is_none());
        assert_eq!(job.messages.len(), 1);
        assert_eq!(job.messages[0].role, "user");
    }

    #[test]
    fn result_serializes_success_and_error_shapes() {
        let ok = JobResult::ok("abc", "hello".into(), "openrouter", Some(58.7));
        let v: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&ok).unwrap()).unwrap();
        assert_eq!(v["content"], "hello");
        assert_eq!(v["provider"], "openrouter");
        assert!(v["error"].is_null());

        let err = JobResult::err("abc", "all providers failed".into());
        let v: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&err).unwrap()).unwrap();
        assert_eq!(v["error"], "all providers failed");
        assert!(v["content"].is_null());
    }
}
