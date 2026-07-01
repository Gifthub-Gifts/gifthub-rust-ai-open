# gifthub-rust-ai

A lean, multi-provider **LLM gateway** for GiftHub — the Phase 2 "beachhead" of the Rust offload
roadmap. It pools free-tier LLM quotas behind one OpenAI-shaped interface with failover, so the
Django app can draft/review product content without an always-on Python worker doing the I/O.

**Django stays the system-of-record.** This service never touches the database or Django's ORM — it
reads jobs from Redis, calls LLM providers, and **POSTs results back to Django over HTTP** (it also
publishes its live rate-limit budget to Redis so Django can pace how much it dispatches).

## How it fits

```
Django (Celery task)                          gifthub-rust-ai (this service)
  build prompt                                   BLPOP ai_jobs  ──────────────────────┐
  RPUSH ai_jobs {job_id,purpose,                 run failover chain (FIFO), pacing     │
              messages,callback}  ─────────────▶   on rpm: A → 429/empty/err → B → …   │
  (returns immediately — fire-and-forget)                                              │
  POST /internal/ai/callback/  ◀───────────────  POST result {job_id,callback,ok,…}  ──┘
  (HMAC-verified) → advance ProductContent       (X-AI-Signature: sha256=<HMAC>)
  reads ai_budget ◀──────────────────────────── SET ai_budget {rpm,rpd,remaining_today}
```

### Job contract

Producer (Django) → `ai_jobs` (Redis list), JSON:

```json
{ "job_id": "uuid", "purpose": "generator", "messages": [ { "role": "user", "content": "…" } ], "callback": { "content_id": 7, "step": "generator" } }
```

- `purpose` (optional) selects a failover chain (`generator` / `translator` / `reviewer`; legacy
  `writer` / `judge` still work); omit for the default chain.
- `callback` is an **opaque** blob the gateway echoes back verbatim — it never inspects or mutates it.

Result → **HTTP POST** to `DJANGO_CALLBACK_URL`, signed `X-AI-Signature: sha256=<hex HMAC-SHA256 of
the body, keyed by the shared secret>`, JSON:

```json
{ "job_id": "uuid", "callback": { "content_id": 7, "step": "generator" }, "ok": true, "content": "…", "error": null, "provider": "gemini_lite", "model": "gemini-2.5-flash", "neurons": null }
```

- `ok` discriminates success (`content`/`provider`/`model` set) from failure (`error` set).
- Any 2xx is "delivered"; on non-2xx / transport error the gateway retries with backoff a bounded
  number of times, then drops (Django's reaper re-dispatches the step).

### Budget contract

The gateway publishes the shared pool to the `ai_budget` Redis key (TTL'd, refreshed every few
seconds) so Django sizes its in-flight cap from the gateway's *own* limits and pauses if the key
disappears:

```json
{ "rpm": 5, "rpd": 400, "remaining_today": 137 }
```

## Design

- **Config-driven providers.** Adding a provider is pure config (`NAME_BASE_URL` / `NAME_API_KEY` /
  `NAME_MODEL` + optional `NAME_RPM` / `NAME_RPD`); no per-provider code. See `.env.example`.
- **Failover.** Walk the chain in order; on `429` / empty content / any error, move to the next
  provider. Transient transport errors are retried per-provider with `2s, 5s, 11s, …` backoff.
- **Per-provider budgets + pacing.** rpm + rpd token buckets (lazy proportional refill). An
  rpm-limited job **waits** for the next token and retries (jobs buffer in `ai_jobs` and drain at the
  sustainable rate) rather than being dropped; only the daily `rpd` cap is a hard stop (fail over /
  error). The shared pool is published as `ai_budget` for Django to pace against.
- **`max_tokens` is opt-in per provider** (`NAME_MAX_TOKENS`) — omitted by default (a low cap makes
  some reasoning models return empty content), but **required for Cloudflare**, which otherwise
  truncates at ~256 tokens.
- **Cloudflare neuron accounting** is captured from the `cf-ai-neurons` response header when present.

## Run

```bash
cp .env.example .env   # fill in REDIS_URL, DJANGO_CALLBACK_URL, AI_GATEWAY_WEBHOOK_SECRET + provider keys
cargo run              # dev
cargo build --release  # prod binary at target/release/gifthub-rust-ai
```

## Develop

```bash
cargo fmt --all
cargo test
cargo clippy --all-targets
```

CI runs `fmt --check`, `check`, `test`, release `build` (all `--locked`), and `clippy -D warnings`.

## Deploy

`.github/workflows/deploy.yml` (push to `main` or manual `workflow_dispatch` with a `DEPLOY`
confirm) builds the release binary, scp's it to the host, atomic-renames, restarts the
`gifthub-rust-ai.service` systemd unit, health-checks via journald (greps the
`🚀 gifthub-rust-ai started` ready marker), and rolls back to the previous binary on failure. The
binary is the whole artifact — there's no DB/migrations. Runtime config (`REDIS_URL` + provider API
keys) lives in a host `.env`, never in CI. Setup, the unit (`deploy/gifthub-rust-ai.service`), and
the required GitHub secrets are documented in **[DEPLOY.md](DEPLOY.md)**.

## Known limitations (this is a scaffold)

- **No integration test yet.** Unit tests cover pure logic only; the Redis/provider wiring has not
  been run end-to-end against a live Redis or real LLM endpoints. Next milestone: a smoke test
  against a local Redis + one provider.
- **At-least-once is not guaranteed.** A job is popped with `BLPOP` (removed from `ai_jobs`) before
  it's processed; if the gateway *crashes* between pop and callback — or the callback POST fails past
  its retries — that step is lost. Django's reaper reclaims any row whose callback never arrives and
  re-dispatches the missing step, so this self-heals; a graceful shutdown (SIGTERM) also drains
  in-flight jobs (and stops waiting for rpm slots). A fully reliable-queue pattern (`LMOVE` into a
  processing list + reclaim) is still deferred.
- **`panic = "abort"`** (size-optimized profile) means a panic in any task aborts the whole process
  — there is no per-task isolation. The hot path avoids panics; `systemd Restart=always` recovers.
- **Neuron accounting is effectively off** with the documented OpenAI-compat Cloudflare endpoint —
  `cf-ai-neurons` is only returned by the native `/ai/run` endpoint (see `provider.rs`).
