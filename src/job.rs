//! The Django <-> Rust job contract.
//!
//! Django (the producer) enqueues a [`Job`] as JSON onto the `ai_jobs` list; the gateway pops it,
//! runs the provider failover chain, and **POSTs** a [`JobResult`] back to Django's callback URL
//! (HMAC-signed — see `callback.rs`). Django owns every database write — the gateway only computes.
//!
//! The `callback` field is an **opaque** routing blob (e.g. `{"content_id": 7, "step": "generator"}`):
//! the gateway never inspects or mutates it, it just echoes it back verbatim so Django can route the
//! result to the right row/step. Keeping it opaque is what keeps this service domain-agnostic.

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
    /// Selects the failover chain (e.g. `"generator"` vs `"reviewer"`); falls back to the default chain.
    #[serde(default)]
    pub purpose: Option<String>,
    pub messages: Vec<Message>,
    /// Opaque blob Django attaches for its own routing; echoed back verbatim in the callback.
    /// Defaults to `null` for older producers that don't send one.
    #[serde(default)]
    pub callback: serde_json::Value,
}

/// The gateway's reply, POSTed to Django. `ok` discriminates success from failure; on success
/// `content`/`provider`/`model` are set, on failure `error` is set. `callback` is echoed from the [`Job`].
#[derive(Debug, Serialize)]
pub struct JobResult {
    pub job_id: String,
    /// Echoed verbatim from the originating job so Django can route the result.
    pub callback: serde_json::Value,
    pub ok: bool,
    pub content: Option<String>,
    pub error: Option<String>,
    /// Which provider produced the content (None on failure).
    pub provider: Option<String>,
    /// The exact model that produced the content, e.g. `gemini-2.5-flash` (None on failure).
    /// Django records this so a draft shows the real model, not a static guess.
    pub model: Option<String>,
    /// Cloudflare neuron cost, when the provider reported it (`cf-ai-neurons` header).
    pub neurons: Option<f64>,
}

impl JobResult {
    pub(crate) fn ok(
        job_id: &str,
        callback: serde_json::Value,
        content: String,
        provider: &str,
        model: &str,
        neurons: Option<f64>,
    ) -> Self {
        Self {
            job_id: job_id.to_string(),
            callback,
            ok: true,
            content: Some(content),
            error: None,
            provider: Some(provider.to_string()),
            model: Some(model.to_string()),
            neurons,
        }
    }

    pub(crate) fn err(job_id: &str, callback: serde_json::Value, error: String) -> Self {
        Self {
            job_id: job_id.to_string(),
            callback,
            ok: false,
            content: None,
            error: Some(error),
            provider: None,
            model: None,
            neurons: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_deserializes_with_optional_purpose_and_callback() {
        let raw = r#"{"job_id":"abc","messages":[{"role":"user","content":"hi"}]}"#;
        let job: Job = serde_json::from_str(raw).unwrap();
        assert_eq!(job.job_id, "abc");
        assert!(job.purpose.is_none());
        assert!(job.callback.is_null()); // absent -> null, echoed back as-is
        assert_eq!(job.messages.len(), 1);
        assert_eq!(job.messages[0].role, "user");
    }

    #[test]
    fn job_preserves_opaque_callback() {
        let raw =
            r#"{"job_id":"abc","callback":{"content_id":7,"step":"generator"},"messages":[]}"#;
        let job: Job = serde_json::from_str(raw).unwrap();
        assert_eq!(job.callback["content_id"], 7);
        assert_eq!(job.callback["step"], "generator");
    }

    #[test]
    fn result_serializes_success_and_error_shapes() {
        let cb = serde_json::json!({"content_id": 7, "step": "generator"});
        let ok = JobResult::ok(
            "abc",
            cb.clone(),
            "hello".into(),
            "gemini_lite",
            "gemini-2.5-flash",
            Some(58.7),
        );
        let v: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&ok).unwrap()).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["content"], "hello");
        assert_eq!(v["provider"], "gemini_lite");
        assert_eq!(v["model"], "gemini-2.5-flash");
        assert_eq!(v["callback"]["content_id"], 7); // echoed verbatim
        assert!(v["error"].is_null());

        let err = JobResult::err("abc", cb, "all providers failed".into());
        let v: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&err).unwrap()).unwrap();
        assert_eq!(v["ok"], false);
        assert_eq!(v["error"], "all providers failed");
        assert!(v["content"].is_null());
        assert_eq!(v["callback"]["step"], "generator");
    }
}
