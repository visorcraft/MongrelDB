//! Stage 1 gate qualification evidence (spec §10 "Stage 1 gate", §21
//! "Reference SLOs for qualification"): "1,000 concurrent client sessions
//! remain stable", "single-node server availability under load: no
//! crash/OOM", and "a point query over a warm local network has a documented
//! p95 baseline".
//!
//! The session scale test spins the daemon router on an ephemeral loopback
//! port over a tempdir `Database`, opens 1,000 concurrent sessions, has every
//! session issue light SQL (one durable `BEGIN`/`INSERT`/`COMMIT` write plus
//! `SELECT 1` reads), and asserts: every request succeeds, the session store
//! enforces its cap (the 1,001st open is rejected with 503), all sessions
//! close cleanly, and process RSS stays under a documented no-OOM tripwire.
//!
//! The loopback point-query test seeds a deterministic dataset over the HTTP
//! API, warms the read path, then issues 1,000 sequential
//! `SELECT id FROM items WHERE id = ?` point queries on one session and
//! reports client-observed p50/p95/p99 against a generous tripwire; the
//! numbers are harvested into BENCHMARKS.md "Stage 1 qualification".
//!
//! Scale knobs: `MONGRELDB_SCALE_SESSIONS` (default 1,000 — the gate count),
//! `MONGRELDB_SCALE_ROUNDS` (default 3 read rounds per session),
//! `MONGRELDB_SCALE_POINT_ROWS` (default 10,000 dataset rows),
//! `MONGRELDB_SCALE_POINT_QUERIES` (default 1,000 — the gate count).
//!
//! Run: `cargo test --test scale_test` from `crates/mongreldb-server`
//! (release mode for the documented evidence run).

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::Database;
use mongreldb_server::{build_app_with_sessions, SessionStore};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::{tempdir, TempDir};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// `/proc/self/status` field in bytes (Linux only; `None` elsewhere).
fn rss_bytes(field: &str) -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix(field))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()
        .map(|kib| kib * 1024)
}

fn items_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
            embedding_source: None,
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

async fn setup(sessions: usize) -> (TempDir, Arc<SessionStore>, std::net::SocketAddr) {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("items", items_schema()).unwrap();
    // The store cap equals the session count under test so the probe open
    // beyond the cap proves the bound is enforced (503, not an error or OOM).
    let store = Arc::new(SessionStore::new(sessions, Duration::from_secs(300)));
    let app = build_app_with_sessions(
        db,
        std::iter::empty::<Arc<dyn mongreldb_query::ExternalTableModule>>(),
        None,
        None,
        false,
        Arc::clone(&store),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (dir, store, addr)
}

/// Issue one SQL statement on a session; `Ok` only on HTTP 200.
async fn sql(
    client: &reqwest::Client,
    addr: &std::net::SocketAddr,
    token: &str,
    statement: &str,
) -> Result<(), String> {
    let response = client
        .post(format!("http://{addr}/sql"))
        .header("X-Session-ID", token)
        .json(&json!({ "sql": statement }))
        .send()
        .await
        .map_err(|error| format!("{statement:?}: transport error: {error}"))?;
    let status = response.status();
    if status != 200 {
        let body = response.text().await.unwrap_or_default();
        return Err(format!(
            "{statement:?}: HTTP {status}: {}",
            body.chars().take(200).collect::<String>()
        ));
    }
    Ok(())
}

/// Deterministic PRNG (Knuth MMIX LCG) — the measured query order must be
/// reproducible run to run (same construction as the core qualification
/// benchmark).
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }
}

/// Nearest-rank percentile (same convention as the core qualification
/// benchmark so BENCHMARKS.md figures are comparable).
fn percentile(sorted_or_unsorted: &mut [u128], fraction: f64) -> u128 {
    sorted_or_unsorted.sort_unstable();
    sorted_or_unsorted[((sorted_or_unsorted.len() - 1) as f64 * fraction).round() as usize]
}

async fn open_session(client: &reqwest::Client, addr: &std::net::SocketAddr) -> String {
    let response = client
        .post(format!("http://{addr}/sessions"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "open session must succeed");
    response
        .json::<Value>()
        .await
        .unwrap()
        .get("session_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .expect("open session: missing session_id")
}

async fn close_session(client: &reqwest::Client, addr: &std::net::SocketAddr, token: &str) {
    let response = client
        .delete(format!("http://{addr}/sessions/{token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "close session must succeed");
}

/// One timed loopback point query (`SELECT id FROM items WHERE id = {id}`).
/// The client-observed latency spans the full HTTP round trip including the
/// response body; `Ok` only on HTTP 200 with exactly the requested row back.
async fn point_query(
    client: &reqwest::Client,
    addr: &std::net::SocketAddr,
    token: &str,
    id: i64,
) -> Result<Duration, String> {
    let statement = format!("SELECT id FROM items WHERE id = {id}");
    let started = Instant::now();
    let response = client
        .post(format!("http://{addr}/sql"))
        .header("X-Session-ID", token)
        .json(&json!({ "sql": statement }))
        .send()
        .await
        .map_err(|error| format!("{statement:?}: transport error: {error}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|error| format!("{statement:?}: body error: {error}"))?;
    let elapsed = started.elapsed();
    if status != 200 {
        return Err(format!(
            "{statement:?}: HTTP {status}: {}",
            body.chars().take(200).collect::<String>()
        ));
    }
    let body: Value = serde_json::from_str(&body)
        .map_err(|error| format!("{statement:?}: bad JSON body: {error}"))?;
    // The buffered JSON format is a bare array of row objects.
    if body != json!([{ "id": id }]) {
        return Err(format!("{statement:?}: unexpected rows: {body}"));
    }
    Ok(elapsed)
}

#[tokio::test(flavor = "multi_thread")]
async fn one_thousand_concurrent_sessions_remain_stable() {
    let sessions = env_usize("MONGRELDB_SCALE_SESSIONS", 1_000);
    let rounds = env_usize("MONGRELDB_SCALE_ROUNDS", 3);
    // Bound in-flight requests, not sessions: sessions are logical (an
    // `X-Session-ID` token, not a pinned connection), so all 1,000 are open
    // concurrently while the client connection fan-out stays friendly to
    // CI machines with low file-descriptor limits.
    const MAX_IN_FLIGHT: usize = 256;
    // No-OOM tripwire (§21 "single-node server availability under load: no
    // crash/OOM"), not a tight limit: 1,000 in-memory sessions plus the
    // DataFusion/Arrow planning stack in one test process are expected well
    // under 1 GiB; 4 GiB catches a leak/runaway without flaking loaded CI.
    const RSS_TRIPWIRE_BYTES: u64 = 4 * 1024 * 1024 * 1024;

    let (_dir, store, addr) = setup(sessions).await;
    let client = reqwest::Client::new();
    let started = Instant::now();
    let rss_start = rss_bytes("VmRSS:");

    let in_flight = Arc::new(tokio::sync::Semaphore::new(MAX_IN_FLIGHT));
    let failures = Arc::new(AtomicUsize::new(0));
    let statements = Arc::new(AtomicU64::new(0));
    // Two barriers (sessions + 1 participants each) make the peak-membership
    // assertions deterministic: every session task waits after opening and
    // again before closing, so the main task observes exactly `sessions` live
    // sessions and can probe the store cap while none have closed yet.
    let open_barrier = Arc::new(tokio::sync::Barrier::new(sessions + 1));
    let close_barrier = Arc::new(tokio::sync::Barrier::new(sessions + 1));

    let mut tasks = Vec::with_capacity(sessions);
    for index in 0..sessions {
        let client = client.clone();
        let in_flight = Arc::clone(&in_flight);
        let failures = Arc::clone(&failures);
        let statements = Arc::clone(&statements);
        let open_barrier = Arc::clone(&open_barrier);
        let close_barrier = Arc::clone(&close_barrier);
        tasks.push(tokio::spawn(async move {
            let result: Result<(), String> = async {
                // Every statement runs under the in-flight bound, including
                // the session open itself.
                let _open_permit = in_flight.acquire().await.unwrap();
                let response = client
                    .post(format!("http://{addr}/sessions"))
                    .send()
                    .await
                    .map_err(|error| format!("open session: transport error: {error}"))?;
                if response.status() != 200 {
                    return Err(format!("open session: HTTP {}", response.status()));
                }
                let token = response
                    .json::<Value>()
                    .await
                    .map_err(|error| format!("open session: bad body: {error}"))?
                    .get("session_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .ok_or_else(|| "open session: missing session_id".to_string())?;
                drop(_open_permit);

                // Hold the session open until every session is live.
                open_barrier.wait().await;

                // One durable write per session (unique primary key), then
                // `rounds` light reads.
                for statement in [
                    "BEGIN".to_string(),
                    format!("INSERT INTO items (id) VALUES ({})", index + 1),
                    "COMMIT".to_string(),
                ] {
                    let _permit = in_flight.acquire().await.unwrap();
                    sql(&client, &addr, &token, &statement).await?;
                    statements.fetch_add(1, Ordering::Relaxed);
                }
                for _ in 0..rounds {
                    let _permit = in_flight.acquire().await.unwrap();
                    sql(&client, &addr, &token, "SELECT 1").await?;
                    statements.fetch_add(1, Ordering::Relaxed);
                }

                // Hold the session open until the cap probe has run.
                close_barrier.wait().await;

                let _permit = in_flight.acquire().await.unwrap();
                let response = client
                    .delete(format!("http://{addr}/sessions/{token}"))
                    .send()
                    .await
                    .map_err(|error| format!("close session: transport error: {error}"))?;
                if response.status() != 200 {
                    return Err(format!("close session: HTTP {}", response.status()));
                }
                Ok(())
            }
            .await;
            if let Err(error) = result {
                eprintln!("session {index}: {error}");
                failures.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // The whole workload is bounded; a hang (e.g. a failed task that never
    // reaches a barrier) is a failure, not a CI timeout.
    let live_at_peak = tokio::time::timeout(Duration::from_secs(240), async {
        // Released once every session is live: prove the peak membership is
        // exactly the gate count and that the store cap is enforced — one
        // more open must be rejected with 503, not crash or evict.
        open_barrier.wait().await;
        let live_at_peak = store.len();
        assert_eq!(
            live_at_peak, sessions,
            "all {sessions} sessions must be live at peak (store cap {sessions})"
        );
        let probe = client
            .post(format!("http://{addr}/sessions"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            probe.status(),
            503,
            "the session store must reject opens beyond its cap"
        );

        close_barrier.wait().await;
        for task in tasks {
            task.await.expect("session task panicked");
        }
        live_at_peak
    })
    .await
    .expect("scale workload did not finish within 240s");

    let elapsed = started.elapsed();
    let failures = failures.load(Ordering::Relaxed);
    let statements = statements.load(Ordering::Relaxed);
    let rss_end = rss_bytes("VmRSS:");
    let rss_peak = rss_bytes("VmHWM:");

    println!(
        "{}",
        json!({
            "test": "one_thousand_concurrent_sessions",
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "sessions": sessions,
            "read_rounds_per_session": rounds,
            "statements": statements,
            "failures": failures,
            "live_sessions_at_peak": live_at_peak,
            "sessions_after_close": store.len(),
            "elapsed_ms": elapsed.as_millis(),
            "rss_start_bytes": rss_start,
            "rss_end_bytes": rss_end,
            "peak_rss_bytes": rss_peak,
            "rss_tripwire_bytes": RSS_TRIPWIRE_BYTES,
        })
    );

    assert_eq!(failures, 0, "every session request must succeed");
    assert_eq!(
        statements,
        (sessions * (3 + rounds)) as u64,
        "every session must complete its write cycle and read rounds"
    );
    assert_eq!(store.len(), 0, "all sessions must close cleanly at the end");
    if let Some(peak) = rss_peak {
        assert!(
            peak <= RSS_TRIPWIRE_BYTES,
            "peak RSS {peak} bytes exceeds the {}-byte no-OOM tripwire",
            RSS_TRIPWIRE_BYTES
        );
    } else {
        eprintln!("peak RSS unavailable on this platform; RSS tripwire not asserted");
    }
}

/// Gate item: "a point query over a warm local network has a documented p95
/// baseline". Deterministic dataset seeded over the HTTP API, an untimed
/// warm pass, then 1,000 sequential point queries on one session measuring
/// client-observed latency (full HTTP round trip) per query. Numbers are
/// harvested into BENCHMARKS.md "Stage 1 qualification"; release mode only
/// for the documented figures.
#[tokio::test(flavor = "multi_thread")]
async fn loopback_point_query_p95_baseline() {
    let rows = env_usize("MONGRELDB_SCALE_POINT_ROWS", 10_000);
    let queries = env_usize("MONGRELDB_SCALE_POINT_QUERIES", 1_000);
    // Tripwire, not an SLO: catches stalls and pathological regressions
    // while loaded CI machines (and debug builds) cannot flake — release
    // loopback p95 is single-digit milliseconds on the reference machine.
    const P95_TRIPWIRE: Duration = Duration::from_millis(250);

    let (_dir, store, addr) = setup(8).await;
    // A hung request must fail the test, not stall CI until the job timeout.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let token = open_session(&client, &addr).await;
    let started = Instant::now();

    // Seed the deterministic dataset (ids 1..=rows) over the HTTP API in
    // 1,000-row single-statement transactions.
    const SEED_BATCH: i64 = 1_000;
    let mut next = 1i64;
    while next <= rows as i64 {
        let take = SEED_BATCH.min(rows as i64 - next + 1);
        let values = (next..next + take)
            .map(|id| format!("({id})"))
            .collect::<Vec<_>>()
            .join(", ");
        for statement in [
            "BEGIN".to_string(),
            format!("INSERT INTO items (id) VALUES {values}"),
            "COMMIT".to_string(),
        ] {
            sql(&client, &addr, &token, &statement).await.unwrap();
        }
        next += take;
    }

    // Warm pass: untimed point queries in deterministic pseudo-random order,
    // so the measured phase runs with a warm connection, planner, and cache.
    let mut lcg = Lcg(0x9E3779B97F4A7C15);
    for _ in 0..queries {
        let id = (lcg.next() % rows as u64) as i64 + 1;
        point_query(&client, &addr, &token, id).await.unwrap();
    }

    // Measured pass: sequential queries — each latency is one full loopback
    // round trip observed by the client, with no overlap between requests.
    let mut lcg = Lcg(0x2545F4914F6CDD1D);
    let mut samples_ns = Vec::with_capacity(queries);
    for _ in 0..queries {
        let id = (lcg.next() % rows as u64) as i64 + 1;
        let elapsed = point_query(&client, &addr, &token, id).await.unwrap();
        samples_ns.push(elapsed.as_nanos());
    }

    close_session(&client, &addr, &token).await;
    let elapsed = started.elapsed();

    let p50_ns = percentile(&mut samples_ns.clone(), 0.50);
    let p95_ns = percentile(&mut samples_ns.clone(), 0.95);
    let p99_ns = percentile(&mut samples_ns, 0.99);
    println!(
        "{}",
        json!({
            "test": "loopback_point_query",
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "rows": rows,
            "queries": queries,
            "point_query_latency": {"p50_us": p50_ns as f64 / 1e3, "p95_us": p95_ns as f64 / 1e3, "p99_us": p99_ns as f64 / 1e3},
            "p95_tripwire_ms": P95_TRIPWIRE.as_millis(),
            "sessions_after_close": store.len(),
            "elapsed_ms": elapsed.as_millis(),
        })
    );

    assert!(
        Duration::from_nanos(p95_ns as u64) <= P95_TRIPWIRE,
        "loopback point-query p95 {p95_ns}ns exceeds the {}ms tripwire",
        P95_TRIPWIRE.as_millis()
    );
}
