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
pub async fn brpop_job(
    conn: &mut ConnectionManager,
    key: &str,
    timeout_secs: u64,
) -> redis::RedisResult<Option<String>> {
    // BRPOP returns [key, value] or nil on timeout.
    let res: Option<(String, String)> = redis::cmd("BRPOP")
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
    redis::cmd("RPUSH")
        .arg(&key)
        .arg(payload)
        .query_async::<i64>(conn)
        .await?;
    redis::cmd("EXPIRE")
        .arg(&key)
        .arg(ttl_secs)
        .query_async::<i64>(conn)
        .await?;
    Ok(())
}
