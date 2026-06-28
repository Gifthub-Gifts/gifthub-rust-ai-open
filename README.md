# gifthub-rust-ai

A lean, multi-provider **LLM gateway** for GiftHub — the Phase 2 "beachhead" of the Rust offload
roadmap. It pools free-tier LLM quotas behind one OpenAI-shaped interface with failover, so the
Django app can draft/review product content without an always-on Python worker doing the I/O.

**Django stays the system-of-record.** This service never touches the database or Django's ORM —
it only reads jobs from Redis, calls LLM providers, and writes results back to Redis.

## How it fits

```
Django (Celery task)                          gifthub-rust-ai (this service)
  build prompt                                   BLPOP ai_jobs  ───────────────┐
  RPUSH ai_jobs {job_id,purpose,messages} ─────▶ run failover chain (FIFO):    │
                                                   provider A → 429/empty/err → B → …
  BRPOP ai_result:{job_id}  ◀───────────────────  RPUSH ai_result:{job_id} {…} ┘
  write ProductContent DRAFT (Django owns DB)
```

### Redis job contract

Producer (Django) → `ai_jobs` (list), JSON:

```json
{ "job_id": "uuid", "purpose": "writer", "messages": [ { "role": "user", "content": "…" } ] }
```

- `purpose` (optional) selects a failover chain (`writer` / `judge`); omit for the default chain.

Consumer (Django) ← `ai_result:{job_id}` (list, TTL'd), JSON — exactly one of `content`/`error`:

```json
{ "job_id": "uuid", "content": "…", "error": null, "provider": "cloudflare", "neurons": 58.7 }
```

## Design

- **Config-driven providers.** Adding a provider is pure config (`NAME_BASE_URL` / `NAME_API_KEY` /
  `NAME_MODEL` + optional `NAME_RPM` / `NAME_RPD`); no per-provider code. See `.env.example`.
- **Failover.** Walk the chain in order; on `429` / empty content / any error, move to the next
  provider. Transient transport errors are retried per-provider with `2s, 5s, 11s, …` backoff.
- **Per-provider budgets.** rpm + rpd token buckets (lazy proportional refill) skip an exhausted
  provider rather than burning its upstream quota.
- **No `max_tokens`** is sent — capping it makes some reasoning models return empty content.
- **Cloudflare neuron accounting** is captured from the `cf-ai-neurons` response header when present.

## Run

```bash
cp .env.example .env   # fill in REDIS_URL + provider keys
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
  it's processed; if the gateway *crashes* between pop and publish, that job is lost and Django's
  `BRPOP ai_result:{job_id}` will time out (re-enqueue on the Django side). A graceful shutdown
  (SIGTERM) now drains in-flight jobs, so this only bites on a hard crash; a fully reliable-queue
  pattern (`LMOVE` into a processing list + reclaim) is still deferred.
- **`panic = "abort"`** (size-optimized profile) means a panic in any task aborts the whole process
  — there is no per-task isolation. The hot path avoids panics; `systemd Restart=always` recovers.
- **Neuron accounting is effectively off** with the documented OpenAI-compat Cloudflare endpoint —
  `cf-ai-neurons` is only returned by the native `/ai/run` endpoint (see `provider.rs`).
