//! Redis I/O: jobs in, budget out.
//!
//! Django enqueues jobs onto the `ai_jobs` list (consumed here via `BLPOP`); the gateway publishes its
//! live budget to the `ai_budget` key (`set_with_ttl`). Finished *results* no longer travel over Redis —
//! they're POSTed straight to Django (see `callback.rs`).
//!
//! Mirrors the `vpn-watchdog`/`telegram-happ-rust` pool idiom — build one cloneable connection
//! handle at boot and pass `&mut` clones into tasks; free functions take the connection first.
//! Commands are issued via explicit `redis::cmd(...)` so the code is robust across redis-rs
//! version churn in the typed-command API.

use redis::aio::ConnectionManager;

/// Build the shared, auto-reconnecting connection manager once at startup.
pub async fn init_redis(url: &str) -> redis::RedisResult<ConnectionManager> {
    let client = redis::Client::open(url)?;
    ConnectionManager::new(client).await
}

/// Block up to `timeout_secs` for the next job on `key`. Returns the raw JSON payload,
/// or `None` if the blocking pop timed out (so the caller can re-check shutdown).
///
/// Uses `BLPOP` (pop the head) while Django enqueues with `RPUSH` (push the tail) — opposite
/// ends, so the queue is FIFO (oldest job first). Popping the same end Django pushes would make
/// it LIFO and starve old jobs under backlog.
pub async fn pop_job(
    conn: &mut ConnectionManager,
    key: &str,
    timeout_secs: u64,
) -> redis::RedisResult<Option<String>> {
    // BLPOP returns [key, value] or nil on timeout.
    let res: Option<(String, String)> = redis::cmd("BLPOP")
        .arg(key)
        .arg(timeout_secs)
        .query_async(conn)
        .await?;
    Ok(res.map(|(_key, value)| value))
}

/// Publish a short-lived value at `key` with a TTL (`SET key value EX ttl`). Used to publish the live
/// budget snapshot so that if the gateway dies the key expires and Django's dispatcher pauses (rather
/// than pacing against a stale budget). Results are no longer written to Redis — they're POSTed to
/// Django directly (see `callback.rs`).
pub async fn set_with_ttl(
    conn: &mut ConnectionManager,
    key: &str,
    value: &str,
    ttl_secs: u64,
) -> redis::RedisResult<()> {
    redis::cmd("SET")
        .arg(key)
        .arg(value)
        .arg("EX")
        .arg(ttl_secs)
        .query_async::<()>(conn)
        .await
}

/// Read a string value at `key` (`GET key`), or `None` if it's unset. Used to load the persisted
/// learned provider limits at startup.
pub async fn get_value(
    conn: &mut ConnectionManager,
    key: &str,
) -> redis::RedisResult<Option<String>> {
    redis::cmd("GET").arg(key).query_async(conn).await
}

/// Write a value at `key` with **no** TTL (`SET key value`). Used to persist the learned provider
/// limits so they survive a restart (unlike the budget key, this must not expire).
pub async fn set_persistent(
    conn: &mut ConnectionManager,
    key: &str,
    value: &str,
) -> redis::RedisResult<()> {
    redis::cmd("SET")
        .arg(key)
        .arg(value)
        .query_async::<()>(conn)
        .await
}
