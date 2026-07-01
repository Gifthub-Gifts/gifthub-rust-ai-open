//! gifthub-rust-ai — a multi-provider LLM gateway.
//!
//! Django enqueues `{job_id, purpose, messages, callback}` onto the `ai_jobs` Redis list; this service
//! pops each job, runs the per-purpose provider failover chain (with per-provider rpm/rpd budgets), and
//! **POSTs** `{job_id, callback, ok, content|error, provider, model, neurons}` back to Django's callback
//! URL (HMAC-signed). It also publishes its live budget to the `ai_budget` Redis key so Django can pace
//! how much work it dispatches. Django owns every database write — this binary only computes.
//!
//! Pacing policy: an rpm-limited job **waits** for the next token and retries (jobs buffer in `ai_jobs`
//! and drain at the sustainable rate); only the daily cap is a hard stop. The `max_concurrency`
//! semaphore bounds in-flight work, so unpicked jobs simply sit in the Redis list.

mod callback;
mod config;
mod error;
mod http;
mod job;
mod provider;
mod ratelimit;
mod redis_io;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::sync::Semaphore;

use config::Config;

#[tokio::main]
async fn main() {
    env_logger::init();
    let cfg = config::get_config();
    log::info!(
        "gifthub-rust-ai starting (providers: {})",
        cfg.provider_order_display()
    );

    // Two separate Redis connections, on purpose. BLPOP is a *blocking* command and holds its
    // underlying multiplexed connection until it returns (redis-rs multiplexed_connection.rs docs),
    // so it must not share a connection with the budget publisher. `consumer` does only the blocking
    // pop; `publisher` does the budget writes.
    let mut consumer = match redis_io::init_redis(&cfg.redis_url).await {
        Ok(c) => c,
        Err(e) => {
            log::error!("❌ Redis (consumer) connection failed: {e}");
            return;
        }
    };
    let publisher = match redis_io::init_redis(&cfg.redis_url).await {
        Ok(c) => {
            log::info!("✅ connected to Redis");
            c
        }
        Err(e) => {
            log::error!("❌ Redis (publisher) connection failed: {e}");
            return;
        }
    };

    let client = http::build_http_client(cfg.connect_timeout_secs, cfg.request_timeout_secs);
    let sem = Arc::new(Semaphore::new(cfg.max_concurrency));

    // Unified shutdown bus: one watch sender flipped by SIGINT or SIGTERM (systemctl stop / deploy).
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let mut sigterm =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    log::error!("failed to install SIGTERM handler: {e}");
                    return;
                }
            };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => log::info!("SIGINT received, shutting down…"),
            _ = sigterm.recv() => log::info!("SIGTERM received, shutting down…"),
        }
        shutdown_tx.send(true).ok();
    });

    // Budget publisher: republish the live pool `{rpm, rpd, remaining_today}` to the `ai_budget` key
    // on a TTL, so Django sizes its in-flight cap from the gateway's own limits — and *pauses* if we
    // die (the key expires). Publishes immediately, then every `budget_publish_secs`.
    {
        let mut conn = publisher.clone();
        let mut budget_shutdown = shutdown_rx.clone();
        let budgets = cfg.provider_budgets();
        tokio::spawn(async move {
            loop {
                let (rpm, rpd, remaining_today) = ratelimit::budget_snapshot(&budgets);
                let payload = serde_json::json!({
                    "rpm": rpm,
                    "rpd": rpd,
                    "remaining_today": remaining_today,
                })
                .to_string();
                if let Err(e) = redis_io::set_with_ttl(
                    &mut conn,
                    &cfg.budget_key,
                    &payload,
                    cfg.budget_ttl_secs,
                )
                .await
                {
                    log::warn!("failed to publish budget to '{}': {e}", cfg.budget_key);
                }
                tokio::select! {
                    _ = budget_shutdown.changed() => break,
                    _ = tokio::time::sleep(Duration::from_secs(cfg.budget_publish_secs)) => {}
                }
            }
        });
    }

    // Greppable ready marker (the deploy health check looks for this line in journald).
    log::info!(
        "🚀 gifthub-rust-ai started: consuming Redis list '{}'",
        cfg.jobs_key
    );

    loop {
        // Backpressure: take a permit BEFORE popping, so we never pop more jobs than we can run.
        // This bounds *in-flight tasks* (not just concurrent HTTP calls) and lets shutdown drain.
        let permit = tokio::select! {
            _ = shutdown_rx.changed() => break,
            p = Arc::clone(&sem).acquire_owned() => p.expect("semaphore should not be closed"),
        };

        let raw = tokio::select! {
            _ = shutdown_rx.changed() => break, // permit dropped here, returns to the pool
            res = redis_io::pop_job(&mut consumer, &cfg.jobs_key, cfg.pop_timeout_secs) => match res {
                Ok(Some(raw)) => raw,
                Ok(None) => continue, // BLPOP timed out — drop permit, re-check shutdown
                Err(e) => {
                    log::error!("BLPOP error: {e} (backing off 1s)");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            },
        };

        // Each task holds its permit until it finishes (POST included), so `max_concurrency` caps
        // concurrent jobs end-to-end. The shutdown receiver lets a job waiting for an rpm slot bail.
        let client = client.clone();
        let shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            handle_job(cfg, client, shutdown, raw).await;
        });
    }

    // Graceful drain: in-flight jobs each hold a permit, so acquiring them all means every job has
    // finished. Capped so a stuck upstream call can't hang shutdown forever (jobs also observe the
    // shutdown signal and stop waiting for rpm slots, so this usually returns promptly).
    log::info!("shutdown signal received; draining in-flight jobs…");
    let drain = Arc::clone(&sem).acquire_many_owned(cfg.max_concurrency as u32);
    match tokio::time::timeout(Duration::from_secs(cfg.request_timeout_secs + 5), drain).await {
        Ok(_) => log::info!("all in-flight jobs finished"),
        Err(_) => log::warn!("shutdown grace expired with jobs still in flight"),
    }

    log::info!("gifthub-rust-ai stopped");
}

/// Process one job: run its failover chain (pacing on rpm), then POST the result to Django.
async fn handle_job(
    cfg: &'static Config,
    client: reqwest::Client,
    shutdown: watch::Receiver<bool>,
    raw: String,
) {
    let job: job::Job = match serde_json::from_str(&raw) {
        Ok(j) => j,
        Err(e) => {
            // No job_id/callback to answer on — drop and move on.
            log::error!("dropping malformed job ({e}); raw {} bytes", raw.len());
            return;
        }
    };

    // Independent receiver for the callback phase: `run_chain` may consume the shutdown signal, so a
    // separate receiver (cloned while the signal is still false) still fires for `deliver`.
    let mut shutdown_cb = shutdown.clone();
    let result = run_chain(cfg, &client, shutdown, &job).await;
    callback::deliver(&client, cfg, &mut shutdown_cb, &result).await;
}

/// Walk the job's failover chain, returning the first success or a final error.
///
/// rpm-limited providers don't fail the job — the chain *waits* for the soonest token and retries,
/// bounded by `max_rpm_wait_secs` (and aborted early on shutdown). Only an all-providers
/// daily-exhausted / hard-failure state produces an error result.
async fn run_chain(
    cfg: &'static Config,
    client: &reqwest::Client,
    mut shutdown: watch::Receiver<bool>,
    job: &job::Job,
) -> job::JobResult {
    let chain = cfg.chain_for(job.purpose.as_deref());
    // Budget the time spent *waiting for rpm slots* only — NOT time spent inside provider HTTP calls
    // (a slow-failing chain must not masquerade as "rate-limited"). We accumulate actual sleep time.
    let max_wait = Duration::from_secs(cfg.max_rpm_wait_secs);
    let mut waited = Duration::ZERO;
    let mut last_err = String::from("no providers available");

    loop {
        // Soonest rpm refill across providers that are *only* per-minute limited this pass.
        let mut soonest: Option<Duration> = None;

        for name in chain {
            let Some(provider) = cfg.providers.get(name) else {
                continue;
            };
            match ratelimit::try_acquire(name, provider.rpm, provider.rpd) {
                ratelimit::Acquire::Ready => {
                    match provider::call_with_retry(
                        provider,
                        client,
                        &job.messages,
                        cfg.max_attempts_per_provider,
                    )
                    .await
                    {
                        Ok(out) => {
                            let neuron_note = out
                                .neurons
                                .map(|n| format!(", {n} neurons"))
                                .unwrap_or_default();
                            log::info!(
                                "[{name}] job {} ok ({} chars{neuron_note})",
                                job.job_id,
                                out.content.len()
                            );
                            return job::JobResult::ok(
                                &job.job_id,
                                job.callback.clone(),
                                out.content,
                                name,
                                &provider.model,
                                out.neurons,
                            );
                        }
                        Err(e) => {
                            log::warn!("[{name}] job {} failed: {e} (failing over)", job.job_id);
                            last_err = format!("{name}: {e}");
                        }
                    }
                }
                ratelimit::Acquire::Wait(d) => {
                    soonest = Some(soonest.map_or(d, |s| s.min(d)));
                }
                ratelimit::Acquire::DailyExhausted => {
                    log::warn!(
                        "[{name}] job {} skipped: daily quota (rpd={}) exhausted",
                        job.job_id,
                        provider.rpd
                    );
                    last_err = format!("{name}: daily quota exhausted");
                }
            }
        }

        // Walked the whole chain without a completed success.
        match soonest {
            // A provider just needs an rpm token: pace (wait) and retry — unless we've spent the wait
            // budget, or shutdown was requested.
            Some(d) if waited + d <= max_wait => {
                log::info!(
                    "job {} waiting {:.1}s for an rpm slot",
                    job.job_id,
                    d.as_secs_f64()
                );
                tokio::select! {
                    _ = shutdown.changed() => {
                        last_err = "gateway shutting down".to_string();
                        break;
                    }
                    _ = tokio::time::sleep(d) => {}
                }
                waited += d;
            }
            Some(_) => {
                last_err = format!("rpm wait budget ({}s) exceeded", cfg.max_rpm_wait_secs);
                break;
            }
            // No provider is merely rpm-limited (all daily-exhausted or hard-failed): stop.
            None => break,
        }
    }

    log::error!("job {} exhausted all providers: {last_err}", job.job_id);
    job::JobResult::err(&job.job_id, job.callback.clone(), last_err)
}
