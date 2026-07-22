//! Stage 1 gate qualification evidence (spec §10 "Stage 1 gate", §21
//! "Reference SLOs for qualification"). Covers the two gate items not already
//! evidenced by the correctness suites (crash survival, isolation anomalies,
//! backup/PITR):
//!
//! - "One million-row overlapping read/write test has bounded RSS and commit
//!   p99" — N writers commit against one `Database` while M readers hold
//!   snapshots and issue point reads; the test bounds process peak RSS and the
//!   durable-commit p99 and requires zero errors. Default scale is 100,000
//!   rows for CI feasibility; the full gate scale is:
//!   `MONGRELDB_QUAL_ROWS=1000000 cargo test -p mongreldb-core --test qualification --release`
//! - "A point query ... has a documented p95 baseline" — a warm embedded
//!   point-query microbenchmark over a deterministic dataset reporting
//!   median/p95/p99 (harvested into BENCHMARKS.md "Stage 1 qualification").
//!
//! Run perf-relevant assertions in release mode:
//!   `cargo test -p mongreldb-core --test qualification --release`

use mongreldb_core::{schema::*, Database, MongrelError, RowId, Value};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// Perf assertions read process-wide RSS (`VmHWM` is monotonic for the whole
/// test binary), so the two tests in this binary must not run concurrently:
/// serialize them to keep RSS attribution honest.
static SERIAL: Mutex<()> = Mutex::new(());

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn pk_schema() -> Schema {
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

fn percentile(sorted_or_unsorted: &mut [u128], fraction: f64) -> u128 {
    sorted_or_unsorted.sort_unstable();
    sorted_or_unsorted[((sorted_or_unsorted.len() - 1) as f64 * fraction).round() as usize]
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

fn peak_rss_bytes() -> Option<u64> {
    rss_bytes("VmHWM:")
}

fn current_rss_bytes() -> Option<u64> {
    rss_bytes("VmRSS:")
}

/// Absolute peak-RSS bound for the overlapping read/write workload.
///
/// Calibration: the pre-existing one-million-row `read_generation` bench
/// measured a process peak RSS of 1,168,683,008 bytes (BENCHMARKS.md "Read
/// generations and paging"), i.e. ~1.1 KiB/row including all process overhead.
/// This test additionally runs concurrent writer threads staging 100-row
/// batches and reader threads pinning snapshots, and glibc allocator arenas
/// are not returned to the OS between the two tests in this binary, so the
/// bound allows 2 KiB/row (~1.8x the measured figure) plus a 512 MiB process
/// base (test harness, WAL/group-commit buffers, page-cache warm-up). At the
/// 1,000,000-row gate scale this yields ~2.5 GiB, over 2x the measured peak;
/// at the 100,000-row CI default ~712 MiB. The bound is absolute (not a
/// delta) because `VmHWM` is process-wide and monotonic.
fn rss_bound_bytes(rows: usize) -> u64 {
    const PROCESS_BASE: u64 = 512 * 1024 * 1024;
    const PER_ROW: u64 = 2 * 1024;
    PROCESS_BASE + rows as u64 * PER_ROW
}

/// Deterministic PRNG (Knuth MMIX LCG) — the benchmark datasets and read
/// orders must be reproducible run to run.
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

/// Seed `rows` rows (`id` = 0..rows) in 1,000-row transactions, returning the
/// allocated row ids in `id` order.
fn seed_rows(db: &Database, rows: usize) -> Vec<RowId> {
    const SEED_BATCH: usize = 1_000;
    let mut row_ids = Vec::with_capacity(rows);
    let mut next = 0i64;
    while (next as usize) < rows {
        let take = SEED_BATCH.min(rows - next as usize) as i64;
        let chunk_start = next;
        let (_, ids) = db
            .transaction_with_row_ids(|t| {
                for pk in chunk_start..chunk_start + take {
                    t.put("t", vec![(1, Value::Int64(pk))])?;
                }
                Ok(())
            })
            .expect("seed commit");
        assert_eq!(ids.len(), take as usize);
        row_ids.extend_from_slice(&ids);
        next += take;
    }
    row_ids
}

/// Gate item: "One million-row overlapping read/write test has bounded RSS
/// and commit p99." Env-scaled via `MONGRELDB_QUAL_ROWS` (default 100,000 for
/// CI; 1,000,000 for the full gate), `MONGRELDB_QUAL_WRITERS` (4),
/// `MONGRELDB_QUAL_READERS` (4), `MONGRELDB_QUAL_COMMIT_P99_MS` (500 release,
/// 1,000 debug, 3,000 Windows debug). Release mode owns the performance gate;
/// the debug default is a sanity bound for loaded cross-platform CI.
#[test]
fn overlapping_read_write_bounded_rss_and_commit_p99() {
    let _serial = SERIAL.lock().unwrap();

    let rows = env_usize("MONGRELDB_QUAL_ROWS", 100_000);
    let writers = env_usize("MONGRELDB_QUAL_WRITERS", 4);
    let readers = env_usize("MONGRELDB_QUAL_READERS", 4);
    let default_commit_p99_ms = if cfg!(all(windows, debug_assertions)) {
        3_000
    } else if cfg!(debug_assertions) {
        1_000
    } else {
        500
    };
    let commit_p99_max = Duration::from_millis(env_usize(
        "MONGRELDB_QUAL_COMMIT_P99_MS",
        default_commit_p99_ms,
    ) as u64);
    assert!(rows >= 1_000, "the workload needs a meaningful row count");
    assert!(writers > 0 && readers > 0);

    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("t", pk_schema()).unwrap();

    // Half the rows are seeded before the overlap phase; writers append the
    // other half while readers point-read the seeded half (never updated, so
    // every read must deterministically observe it).
    let seeded = rows / 2;
    let overlap = rows - seeded;
    let row_ids = Arc::new(seed_rows(&db, seeded));
    let rss_after_seed = current_rss_bytes();

    const COMMIT_BATCH: usize = 100;
    const READS_PER_SNAPSHOT: usize = 16;
    let next_pk = Arc::new(AtomicUsize::new(seeded));
    let writers_done = Arc::new(AtomicBool::new(false));
    let started = Instant::now();

    let mut writer_handles = Vec::with_capacity(writers);
    for _ in 0..writers {
        let db = Arc::clone(&db);
        let next_pk = Arc::clone(&next_pk);
        writer_handles.push(std::thread::spawn(move || {
            let mut commit_us = Vec::new();
            loop {
                let start = next_pk.fetch_add(COMMIT_BATCH, Ordering::Relaxed);
                if start >= seeded + overlap {
                    break;
                }
                let stop = (start + COMMIT_BATCH).min(seeded + overlap);
                let mut tx = db.begin();
                for pk in start..stop {
                    tx.put("t", vec![(1, Value::Int64(pk as i64))])?;
                }
                // Time the durable commit alone: WAL append + group-commit
                // fsync + publish (the gate's "commit p99").
                let commit_started = Instant::now();
                tx.commit()?;
                commit_us.push(commit_started.elapsed().as_micros());
            }
            Ok::<_, MongrelError>(commit_us)
        }));
    }

    let mut reader_handles = Vec::with_capacity(readers);
    for _ in 0..readers {
        let db = Arc::clone(&db);
        let row_ids = Arc::clone(&row_ids);
        let writers_done = Arc::clone(&writers_done);
        reader_handles.push(std::thread::spawn(move || {
            let mut lcg = Lcg(0x9E3779B97F4A7C15);
            let mut reads = 0u64;
            while !writers_done.load(Ordering::Acquire) {
                let mut tx = db.begin();
                for _ in 0..READS_PER_SNAPSHOT {
                    let i = (lcg.next() % seeded as u64) as usize;
                    let row = tx.get("t", row_ids[i])?;
                    let row = row.ok_or_else(|| {
                        MongrelError::Other(format!("seeded row {i} not visible"))
                    })?;
                    assert_eq!(
                        row.columns[0].1,
                        Value::Int64(i as i64),
                        "point read returned the wrong row"
                    );
                    reads += 1;
                }
                tx.rollback();
            }
            Ok::<_, MongrelError>(reads)
        }));
    }

    let mut commit_us: Vec<u128> = Vec::new();
    let mut write_errors = 0u64;
    for handle in writer_handles {
        match handle.join().expect("writer thread panicked") {
            Ok(mut samples) => commit_us.append(&mut samples),
            Err(error) => {
                eprintln!("writer error: {error}");
                write_errors += 1;
            }
        }
    }
    writers_done.store(true, Ordering::Release);

    let mut total_reads = 0u64;
    let mut read_errors = 0u64;
    for handle in reader_handles {
        match handle.join().expect("reader thread panicked") {
            Ok(reads) => total_reads += reads,
            Err(error) => {
                eprintln!("reader error: {error}");
                read_errors += 1;
            }
        }
    }

    let elapsed = started.elapsed();
    let peak_rss = peak_rss_bytes();
    let rss_bound = rss_bound_bytes(rows);
    let commit_p50 = percentile(&mut commit_us.clone(), 0.50);
    let commit_p99 = percentile(&mut commit_us, 0.99);
    let commits = commit_us.len();

    println!(
        "{}",
        serde_json::json!({
            "test": "overlapping_read_write",
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "rows": rows,
            "seeded_rows": seeded,
            "overlap_rows": overlap,
            "writers": writers,
            "readers": readers,
            "commits": commits,
            "point_reads": total_reads,
            "elapsed_ms": elapsed.as_millis(),
            "commit_latency": {"p50_us": commit_p50, "p99_us": commit_p99},
            "commit_p99_bound_ms": commit_p99_max.as_millis(),
            "rss_after_seed_bytes": rss_after_seed,
            "peak_rss_bytes": peak_rss,
            "peak_rss_bound_bytes": rss_bound,
            "write_errors": write_errors,
            "read_errors": read_errors,
        })
    );

    assert_eq!(write_errors, 0, "writers must commit without errors");
    assert_eq!(
        read_errors, 0,
        "readers must observe seeded rows without errors"
    );
    assert!(
        total_reads > 0,
        "readers must actually overlap the write phase"
    );
    assert_eq!(
        commits,
        overlap.div_ceil(COMMIT_BATCH),
        "every overlap batch must commit exactly once"
    );
    assert_eq!(
        db.table("t").unwrap().lock().count(),
        rows as u64,
        "all committed rows must be durable and visible"
    );
    // Perf bound is meaningful only in release (debug is multi-x slower and
    // CI machines are noisy). Debug still exercises correctness of the
    // overlapping workload and emits the JSON metrics line above.
    if !cfg!(debug_assertions) {
        assert!(
            Duration::from_micros(commit_p99 as u64) <= commit_p99_max,
            "commit p99 {commit_p99}us exceeds the {}ms bound",
            commit_p99_max.as_millis()
        );
    } else {
        eprintln!(
            "debug build: skipping commit p99 bound (p99={}us, bound={}ms)",
            commit_p99,
            commit_p99_max.as_millis()
        );
    }
    if let Some(peak) = peak_rss {
        assert!(
            peak <= rss_bound,
            "peak RSS {peak} bytes exceeds the bounded-RSS gate bound of {rss_bound} bytes"
        );
    } else {
        eprintln!("peak RSS unavailable on this platform; RSS bound not asserted");
    }
}

/// Gate item: a documented warm point-query p95 baseline. Deterministic
/// 100,000-row dataset; each measured query is a full embedded round trip
/// (begin transaction at a fresh snapshot, point get by row id, rollback)
/// over a warm cache. Numbers are harvested into BENCHMARKS.md "Stage 1
/// qualification"; release mode only for the documented figures.
#[test]
fn warm_point_query_p95_baseline() {
    let _serial = SERIAL.lock().unwrap();

    const ROWS: usize = 100_000;
    const QUERIES: usize = 10_000;
    // Sanity bound only: catches pathological regressions while staying far
    // above the expected microsecond-range latencies, so loaded CI machines
    // (and debug builds) cannot flake.
    const P95_SANITY_MAX: Duration = Duration::from_millis(20);

    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();
    let row_ids = seed_rows(&db, ROWS);

    // Warm pass: touch every row once so the measured phase reads a warm
    // page cache / hot index.
    {
        let mut tx = db.begin();
        for &row_id in &row_ids {
            std::hint::black_box(tx.get("t", row_id).unwrap());
        }
        tx.rollback();
    }

    let mut lcg = Lcg(0x2545F4914F6CDD1D);
    let mut samples_ns = Vec::with_capacity(QUERIES);
    for _ in 0..QUERIES {
        let i = (lcg.next() % ROWS as u64) as usize;
        let query_started = Instant::now();
        let mut tx = db.begin();
        let row = tx.get("t", row_ids[i]).unwrap();
        tx.rollback();
        samples_ns.push(query_started.elapsed().as_nanos());
        assert_eq!(
            row.map(|row| row.columns[0].1.clone()),
            Some(Value::Int64(i as i64)),
            "point query returned the wrong row"
        );
    }

    let p50_ns = percentile(&mut samples_ns.clone(), 0.50);
    let p95_ns = percentile(&mut samples_ns.clone(), 0.95);
    let p99_ns = percentile(&mut samples_ns, 0.99);
    println!(
        "{}",
        serde_json::json!({
            "test": "warm_point_query",
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "rows": ROWS,
            "queries": QUERIES,
            "point_query_latency": {"p50_us": p50_ns as f64 / 1e3, "p95_us": p95_ns as f64 / 1e3, "p99_us": p99_ns as f64 / 1e3},
            "peak_rss_bytes": peak_rss_bytes(),
        })
    );

    assert!(
        Duration::from_nanos(p95_ns as u64) <= P95_SANITY_MAX,
        "point-query p95 {p95_ns}ns exceeds the {}ms sanity bound",
        P95_SANITY_MAX.as_millis()
    );
}
