//! A single config-driven LLM provider speaking the OpenAI `chat/completions` shape.
//!
//! Every provider (OpenRouter, Cloudflare Workers AI, Google AI Studio) is identical wire-wise —
//! a `base_url` + bearer `api_key` + `model`. There is deliberately no per-provider code; adding a
//! provider is purely config. `call` maps HTTP status -> [`ProviderError`] before parsing the body
//! (mirrors the `vpn-watchdog` client pattern), and `call_with_retry` retries only *transient*
//! transport failures with the house `b = b*2 + 1` backoff.

use std::time::Duration;

use serde::Serialize;

use crate::error::ProviderError;
use crate::job::Message;

/// Max bytes of an error body we echo back (avoid logging/returning huge payloads).
const MAX_ERR_BODY: usize = 300;

pub struct Provider {
    pub name: String,
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub rpm: u32,
    pub rpd: u32,
    /// Optional output cap. Most providers complete fine without one, but Cloudflare Workers AI
    /// truncates at ~256 tokens unless this is sent — set `{NAME}_MAX_TOKENS` for those.
    pub max_tokens: Option<u32>,
}

/// A successful completion plus optional cost metadata.
pub struct Completion {
    pub content: String,
    pub neurons: Option<f64>,
}

#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: &'a [Message],
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
}

impl Provider {
    /// One attempt against this provider. `max_tokens` is sent only when configured
    /// (`{NAME}_MAX_TOKENS`): omit it for providers where a low cap makes reasoning models return
    /// empty `content`; set it for Cloudflare, which otherwise truncates at ~256 tokens.
    async fn call(
        &self,
        client: &reqwest::Client,
        messages: &[Message],
    ) -> Result<Completion, ProviderError> {
        let req = ChatRequest {
            model: &self.model,
            messages,
            max_tokens: self.max_tokens,
        };
        let resp = client
            .post(&self.base_url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&req)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    ProviderError::Timeout
                } else {
                    ProviderError::Network(e.to_string())
                }
            })?;

        let status = resp.status();
        // Cloudflare exposes `cf-ai-neurons` only on its *native* `/ai/run/{model}` endpoint, NOT the
        // OpenAI-compat `/ai/v1/chat/completions` one we call here — so this is usually `None`. We read
        // it opportunistically (harmless) for setups that proxy the native endpoint; real neuron
        // accounting would need a native-endpoint code path (future work).
        let neurons = resp
            .headers()
            .get("cf-ai-neurons")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<f64>().ok());

        if status.as_u16() == 401 || status.as_u16() == 403 {
            return Err(ProviderError::Auth(status.to_string()));
        }
        if status.as_u16() == 429 {
            return Err(ProviderError::RateLimit(status.to_string()));
        }

        let text = resp
            .text()
            .await
            .map_err(|e| ProviderError::Network(e.to_string()))?;
        if !status.is_success() {
            return Err(ProviderError::Api {
                status: status.as_u16(),
                body: text.chars().take(MAX_ERR_BODY).collect(),
            });
        }

        let v: serde_json::Value = serde_json::from_str(&text).map_err(|e| ProviderError::Api {
            status: status.as_u16(),
            body: format!("invalid JSON: {e}"),
        })?;
        let content = v["choices"][0]["message"]["content"]
            .as_str()
            .unwrap_or("")
            .trim()
            .to_string();
        if content.is_empty() {
            return Err(ProviderError::Empty);
        }
        Ok(Completion { content, neurons })
    }
}

/// Call `provider`, retrying only transient transport errors up to `max_attempts`
/// with growing backoff (2s, 5s, 11s, ...). Provider-level errors return immediately
/// so the caller fails over to the next provider.
pub(crate) async fn call_with_retry(
    provider: &Provider,
    client: &reqwest::Client,
    messages: &[Message],
    max_attempts: u32,
) -> Result<Completion, ProviderError> {
    let mut backoff = 2u64;
    let mut attempt = 1u32;
    loop {
        match provider.call(client, messages).await {
            Ok(c) => return Ok(c),
            Err(e) => {
                if !e.is_transient() || attempt >= max_attempts {
                    return Err(e);
                }
                log::warn!(
                    "[{}] transient error: {e} (retry {attempt}/{max_attempts} in {backoff}s)",
                    provider.name
                );
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                backoff = backoff.saturating_mul(2).saturating_add(1);
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chat_request_serializes_to_openai_shape() {
        let msgs = vec![Message {
            role: "user".into(),
            content: "hi".into(),
        }];
        let body = serde_json::to_value(ChatRequest {
            model: "m",
            messages: &msgs,
            max_tokens: None,
        })
        .unwrap();
        assert_eq!(body["model"], "m");
        assert_eq!(body["messages"][0]["role"], "user");
        assert_eq!(body["messages"][0]["content"], "hi");
        // None is omitted entirely (so providers that dislike a cap never see the field).
        assert!(body.get("max_tokens").is_none());
    }

    #[test]
    fn chat_request_includes_max_tokens_when_set() {
        let msgs = vec![Message {
            role: "user".into(),
            content: "hi".into(),
        }];
        let body = serde_json::to_value(ChatRequest {
            model: "m",
            messages: &msgs,
            max_tokens: Some(4096),
        })
        .unwrap();
        assert_eq!(body["max_tokens"], 4096);
    }
}
