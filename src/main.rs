//! gifthub-rust-ai — a multi-provider LLM gateway.
//!
//! Django enqueues `{job_id, purpose, messages}` onto the `ai_jobs` Redis list; this service pops
//! each job, runs the per-purpose provider failover chain (with per-provider rpm/rpd budgets), and
//! pushes `{job_id, content|error, provider, neurons}` back onto `ai_result:{job_id}`. Django owns
//! every database write — this binary only computes and answers over Redis.

mod config;
mod error;
mod http;
mod job;
mod provider;
mod ratelimit;
mod redis_io;

use std::sync::Arc;
use std::time::Duration;

use redis::aio::ConnectionManager;
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
    // so it must not share a connection with the result writers — otherwise every RPUSH would stall
    // behind an in-flight BLPOP. `consumer` does only the blocking pop; `publisher` (cloned into each
    // per-job task) does the writes.
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
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
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

        // Each task gets its own publisher clone (separate from the BLPOP connection) and holds the
        // permit until it finishes, so `max_concurrency` caps concurrent jobs end-to-end.
        let conn = publisher.clone();
        let client = client.clone();
        tokio::spawn(async move {
            let _permit = permit;
            handle_job(conn, cfg, client, raw).await;
        });
    }

    // Graceful drain: in-flight jobs each hold a permit, so acquiring them all means every job has
    // finished publishing. Capped so a stuck upstream call can't hang shutdown forever.
    log::info!("shutdown signal received; draining in-flight jobs…");
    let drain = Arc::clone(&sem).acquire_many_owned(cfg.max_concurrency as u32);
    match tokio::time::timeout(Duration::from_secs(cfg.request_timeout_secs + 5), drain).await {
        Ok(_) => log::info!("all in-flight jobs finished"),
        Err(_) => log::warn!("shutdown grace expired with jobs still in flight"),
    }

    log::info!("gifthub-rust-ai stopped");
}

/// Process one job: walk its failover chain, publish the first success or a final error.
async fn handle_job(
    mut conn: ConnectionManager,
    cfg: &'static Config,
    client: reqwest::Client,
    raw: String,
) {
    let job: job::Job = match serde_json::from_str(&raw) {
        Ok(j) => j,
        Err(e) => {
            // No job_id to answer on — drop and move on.
            log::error!("dropping malformed job ({e}); raw {} bytes", raw.len());
            return;
        }
    };

    let chain = cfg.chain_for(job.purpose.as_deref());
    let mut last_err = String::from("no providers available");

    for name in chain {
        let Some(provider) = cfg.providers.get(name) else {
            continue;
        };

        if !ratelimit::try_acquire(name, provider.rpm, provider.rpd) {
            log::warn!(
                "[{name}] job {} skipped: local budget exhausted (rpm={} rpd={})",
                job.job_id,
                provider.rpm,
                provider.rpd
            );
            last_err = format!("{name}: rate-limited");
            continue;
        }

        match provider::call_with_retry(
            provider,
            &client,
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
                let result = job::JobResult::ok(&job.job_id, out.content, name, out.neurons);
                publish(&mut conn, cfg, &job.job_id, &result).await;
                return;
            }
            Err(e) => {
                log::warn!("[{name}] job {} failed: {e} (failing over)", job.job_id);
                last_err = format!("{name}: {e}");
            }
        }
    }

    log::error!("job {} exhausted all providers: {last_err}", job.job_id);
    let result = job::JobResult::err(&job.job_id, last_err);
    publish(&mut conn, cfg, &job.job_id, &result).await;
}

/// Serialize and push a result; log (never panic) on failure so one bad publish can't kill a task.
async fn publish(
    conn: &mut ConnectionManager,
    cfg: &Config,
    job_id: &str,
    result: &job::JobResult,
) {
    let payload = match serde_json::to_string(result) {
        Ok(s) => s,
        Err(e) => {
            log::error!("failed to serialize result for job {job_id}: {e}");
            return;
        }
    };
    if let Err(e) = redis_io::publish_result(
        conn,
        &cfg.result_prefix,
        cfg.result_ttl_secs,
        job_id,
        &payload,
    )
    .await
    {
        log::error!("failed to publish result for job {job_id}: {e}");
    }
}
