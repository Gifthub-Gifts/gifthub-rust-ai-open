//! Redis I/O: the only boundary between Django and this service.
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

/// Publish a result onto `{prefix}{job_id}` and set a TTL so unclaimed results self-expire.
/// Django consumes it with a matching `BRPOP {prefix}{job_id}`.
pub async fn publish_result(
    conn: &mut ConnectionManager,
    prefix: &str,
    ttl_secs: u64,
    job_id: &str,
    payload: &str,
) -> redis::RedisResult<()> {
    let key = format!("{prefix}{job_id}");
    // RPUSH + EXPIRE in one pipelined round-trip, so we can't crash/disconnect between them and
    // leave the result key without a TTL (which would leak it forever if Django never consumes it).
    redis::pipe()
        .cmd("RPUSH")
        .arg(&key)
        .arg(payload)
        .ignore()
        .cmd("EXPIRE")
        .arg(&key)
        .arg(ttl_secs)
        .ignore()
        .query_async::<()>(conn)
        .await?;
    Ok(())
}
