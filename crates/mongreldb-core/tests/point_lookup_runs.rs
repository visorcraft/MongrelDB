//! Point-query latency as a function of active immutable-run count.
//!
//! Per the index audit, point lookups probe each active immutable run, so the
//! underlying bound is roughly:
//!
//!   O(memtable seek + mutable-run seek + active immutable runs)
//!
//! This benchmark creates tables with controlled run counts (1, 4, 16, 64) by
//! inserting batches that exceed the mutable-run spill threshold (8 MiB by
//! default) and flushing after each. It then measures warm point-query
//! latency (p50/p95/p99) and prints the results as JSON for harvesting into
//! BENCHMARKS.md.
//!
//! Run in release mode for meaningful numbers:
//!   `cargo test -p mongreldb-core --test point_lookup_runs --release -- --nocapture`

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Database, RowId, Value};
use std::time::Instant;
use tempfile::tempdir;

/// Each batch puts ~rows_per_run rows; flush after each batch should spill
/// one immutable run. With ~120 B per row, ~70_000 rows ≈ 8.4 MiB.
const ROWS_PER_RUN: usize = 70_000;
const RUN_COUNTS: &[usize] = &[1, 4, 16];
const QUERIES: usize = 1_000;

/// Smaller batch size for the 64/256-run scaling fixtures so the test stays
/// tractable while still pushing the directory path past the 16-run
/// threshold the original file covers.
const ROWS_PER_RUN_FINE: usize = 5_000;
/// 10k warm samples per (run_count, layout) as required by TODO §1.5.
const QUERIES_FINE: usize = 10_000;

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

fn percentile(sorted: &mut [u128], fraction: f64) -> u128 {
    sorted.sort_unstable();
    sorted[((sorted.len() - 1) as f64 * fraction).round() as usize]
}

/// Build a table with approximately `target_runs` immutable runs by inserting
/// `target_runs` batches of `ROWS_PER_RUN` rows and flushing after each.
/// Returns the assigned row ids (one per inserted row) and the actual run
/// count observed after all flushes.
fn build_with_runs(target_runs: usize) -> (tempfile::TempDir, Vec<RowId>, usize) {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();
    let handle = db.table("t").unwrap();

    let mut row_ids = Vec::with_capacity(target_runs * ROWS_PER_RUN);
    let mut next_pk: i64 = 0;
    for _batch in 0..target_runs {
        let batch_start = next_pk;
        let batch_end = batch_start + ROWS_PER_RUN as i64;
        let (_, ids) = db
            .transaction_with_row_ids(|t| {
                for pk in batch_start..batch_end {
                    t.put("t", vec![(1, Value::Int64(pk))])?;
                }
                Ok(())
            })
            .expect("batch commit");
        assert_eq!(ids.len(), ROWS_PER_RUN);
        row_ids.extend_from_slice(&ids);
        next_pk = batch_end;
        // Force-flush each batch so the mutable-run tier spills to a fresh
        // immutable .sr per batch. Without this, all batches accumulate in
        // memtable and a single flush at the end produces one run.
        let mut table = handle.lock();
        table.force_flush().unwrap();
    }

    let run_count = handle.read().run_count();
    (dir, row_ids, run_count)
}

fn measure_point_query_latency(db: &Database, row_ids: &[RowId]) -> (u128, u128, u128) {
    // Warm pass — touches every row once so the measured phase reads a warm
    // page cache / hot index, isolating run-count cost from cold-start.
    {
        let mut tx = db.begin();
        for &rid in row_ids {
            std::hint::black_box(tx.get("t", rid).unwrap());
        }
        tx.rollback();
    }

    let mut lcg: u64 = 0x2545F4914F6CDD1D;
    let mut samples_ns: Vec<u128> = Vec::with_capacity(QUERIES);
    for _ in 0..QUERIES {
        lcg = lcg
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let i = (lcg >> 33) as usize % row_ids.len();
        let rid = row_ids[i];
        let started = Instant::now();
        let mut tx = db.begin();
        let row = tx.get("t", rid).unwrap();
        tx.rollback();
        samples_ns.push(started.elapsed().as_nanos());
        assert!(row.is_some(), "point query returned None for rid {}", rid.0);
    }

    let p50 = percentile(&mut samples_ns.clone(), 0.50);
    let p95 = percentile(&mut samples_ns.clone(), 0.95);
    let p99 = percentile(&mut samples_ns, 0.99);
    (p50, p95, p99)
}

#[test]
fn point_lookup_scaling_with_immutable_run_count() {
    let mut samples = Vec::new();
    let mut dirs: Vec<tempfile::TempDir> = Vec::new();
    for &runs in RUN_COUNTS {
        let (dir, row_ids, actual_runs) = build_with_runs(runs);
        // Reopen via the same path to ensure runs are on-disk before measuring.
        // Keep the dir alive until after the measurements.
        let db_path = dir.path().to_path_buf();
        let db = Database::open(&db_path).unwrap();
        let (p50_ns, p95_ns, p99_ns) = measure_point_query_latency(&db, &row_ids);
        samples.push(serde_json::json!({
            "target_runs": runs,
            "actual_runs": actual_runs,
            "rows": row_ids.len(),
            "queries": QUERIES,
            "point_query_latency": {
                "p50_us": p50_ns as f64 / 1e3,
                "p95_us": p95_ns as f64 / 1e3,
                "p99_us": p99_ns as f64 / 1e3,
            },
        }));
        dirs.push(dir);
    }
    println!(
        "{}",
        serde_json::json!({
            "test": "point_lookup_scaling_with_immutable_run_count",
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "samples": samples,
        })
    );
}

// ---------------------------------------------------------------------------
// 64 / 256 run fixtures covering the four TODO §1.5 layouts.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum Layout {
    /// Each run holds a non-overlapping slice of `RowId` space.
    Disjoint,
    /// Every run's min/max RowId range covers the queried rid, but only one
    /// run actually contains it (Kit-style update of the same PK across runs).
    Overlapping,
    /// The same logical PK has a version in every run (delete-then-put cycle).
    HotKeyHistory,
    /// No run contains the queried rid; min/max metadata is wide enough that
    /// the range directory cannot reject the miss by itself.
    WideMiss,
}

impl Layout {
    fn as_str(self) -> &'static str {
        match self {
            Layout::Disjoint => "disjoint",
            Layout::Overlapping => "overlapping",
            Layout::HotKeyHistory => "hot_key_history",
            Layout::WideMiss => "wide_miss",
        }
    }
}

const LAYOUTS: &[Layout] = &[
    Layout::Disjoint,
    Layout::Overlapping,
    Layout::HotKeyHistory,
    Layout::WideMiss,
];

/// Build a fixture for one (layout, run_count) pair. Each layout writes
/// `run_count` immutable runs and returns the row ids that exist at the
/// current snapshot plus the wide-miss target rid (when applicable).
fn build_layout_fixture(
    layout: Layout,
    run_count: usize,
) -> (tempfile::TempDir, Vec<RowId>, Option<RowId>) {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();
    let handle = db.table("t").unwrap();
    let mut live_rids: Vec<RowId> = Vec::new();
    let mut wide_miss_rid: Option<RowId> = None;

    match layout {
        Layout::Disjoint => {
            // Each batch holds a fresh, non-overlapping PK slice → disjoint
            // RowId ranges per run. The directory path should reject every
            // other run by min/max and open exactly one.
            let mut next_pk: i64 = 0;
            for _ in 0..run_count {
                let start = next_pk;
                let end = start + ROWS_PER_RUN_FINE as i64;
                let (_, ids) = db
                    .transaction_with_row_ids(|t| {
                        for pk in start..end {
                            t.put("t", vec![(1, Value::Int64(pk))])?;
                        }
                        Ok(())
                    })
                    .expect("batch commit");
                live_rids.extend_from_slice(&ids);
                next_pk = end;
                let mut table = handle.lock();
                table.force_flush().unwrap();
            }
        }
        Layout::Overlapping => {
            // Every run rewrites the same PK slice (Kit-style overwrite).
            // The min/max RowId range of every run thus covers the same span
            // (the first batch's rids), so any rid in that span falls inside
            // every run's metadata even though only the latest run owns a
            // live row at that rid.
            let pk_count = ROWS_PER_RUN_FINE;
            let mut anchor: Option<RowId> = None;
            for _ in 0..run_count {
                let (_, ids) = db
                    .transaction_with_row_ids(|t| {
                        for offset in 0..pk_count {
                            let pk = offset as i64;
                            let _ = t.put("t", vec![(1, Value::Int64(pk))])?;
                        }
                        Ok(())
                    })
                    .expect("batch commit");
                // First run's first rid sits inside every run's RowId span;
                // only the first run owns a live row at that rid (later runs
                // tombstone it via same-PK overwrite).
                if anchor.is_none() {
                    anchor = ids.first().copied();
                }
                // The latest batch owns the live PK set.
                live_rids = ids;
                let mut table = handle.lock();
                table.force_flush().unwrap();
            }
            wide_miss_rid = anchor;
        }
        Layout::HotKeyHistory => {
            // One hot PK (id=0) with Kit-style delete-then-put in every run,
            // plus a small padding set per run to keep min/max ranges wide.
            // `live_rids` collects one hot rid per run so each lookup
            // exercises the multi-run path (the older hot rids are still in
            // older runs as tombstones).
            let padding = 8;
            let mut hot_rid: Option<RowId> = None;
            for run_idx in 0..run_count {
                if let Some(prev) = hot_rid.take() {
                    let mut table = handle.lock();
                    table.delete(prev).unwrap();
                }
                let (_, ids) = db
                    .transaction_with_row_ids(|t| {
                        let _ = t.put("t", vec![(1, Value::Int64(0))])?;
                        for offset in 1..=padding {
                            let pk = (run_idx * padding + offset) as i64;
                            let _ = t.put("t", vec![(1, Value::Int64(pk))])?;
                        }
                        Ok(())
                    })
                    .expect("batch commit");
                hot_rid = Some(*ids.first().unwrap());
                // Every run contributes its hot rid so the lookup path
                // opens every run while serving a query.
                live_rids.push(*ids.first().unwrap());
                let mut table = handle.lock();
                table.force_flush().unwrap();
            }
            wide_miss_rid = Some(RowId(u64::MAX / 2));
        }
        Layout::WideMiss => {
            // Each run writes a small, dense slice of fresh PKs. The
            // wide-miss target sits outside every run's RowId range.
            let mut next_pk: i64 = 0;
            for _ in 0..run_count {
                let start = next_pk;
                let end = start + ROWS_PER_RUN_FINE as i64;
                let (_, ids) = db
                    .transaction_with_row_ids(|t| {
                        for pk in start..end {
                            t.put("t", vec![(1, Value::Int64(pk))])?;
                        }
                        Ok(())
                    })
                    .expect("batch commit");
                live_rids.extend_from_slice(&ids);
                next_pk = end;
                let mut table = handle.lock();
                table.force_flush().unwrap();
            }
            wide_miss_rid = Some(RowId(u64::MAX / 2));
        }
    }

    (dir, live_rids, wide_miss_rid)
}

/// Measure warm point-query latency for one (run_count, layout) fixture.
/// Returns p50/p95/p99 latency in nanoseconds plus the observed run count.
fn measure_layout_latency(
    db: &Database,
    row_ids: &[RowId],
    wide_miss: Option<RowId>,
    actual_runs: usize,
) -> serde_json::Value {
    // Warm pass.
    {
        let mut tx = db.begin();
        for &rid in row_ids {
            std::hint::black_box(tx.get("t", rid).unwrap());
        }
        if let Some(miss) = wide_miss {
            let _ = tx.get("t", miss);
        }
        tx.rollback();
    }

    let mut lcg: u64 = 0x2545F4914F6CDD1D;
    let mut samples_ns: Vec<u128> = Vec::with_capacity(QUERIES_FINE);
    for _ in 0..QUERIES_FINE {
        lcg = lcg
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        // Mix hit (random live rid) and wide-miss (~10%) queries.
        let is_miss = (lcg & 0x3F) == 0;
        lcg = lcg
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let rid = if is_miss {
            wide_miss.unwrap_or(RowId(u64::MAX / 2))
        } else {
            let i = (lcg >> 33) as usize % row_ids.len();
            row_ids[i]
        };
        let started = Instant::now();
        let mut tx = db.begin();
        let _ = tx.get("t", rid).unwrap();
        tx.rollback();
        samples_ns.push(started.elapsed().as_nanos());
    }

    let p50 = percentile(&mut samples_ns.clone(), 0.50);
    let p95 = percentile(&mut samples_ns.clone(), 0.95);
    let p99 = percentile(&mut samples_ns, 0.99);

    // Snapshot lookup metrics once for directory_candidates / run_refs
    // reporting. Per-Table metrics are exposed on the handle.
    let handle = db.table("t").unwrap();
    let t = handle.read();
    let metrics = t.lookup_metrics_snapshot();
    let _ = actual_runs;
    serde_json::json!({
        "p50_us": p50 as f64 / 1e3,
        "p95_us": p95 as f64 / 1e3,
        "p99_us": p99 as f64 / 1e3,
        "queries": QUERIES_FINE,
        "directory_lookup_hit": metrics.directory_lookup_hit,
        "directory_lookup_fallback": metrics.directory_lookup_fallback,
        "directory_incomplete": metrics.directory_incomplete,
        "directory_run_readers_opened": metrics.directory_run_readers_opened,
        "directory_early_stop_total": metrics.directory_early_stop_total,
    })
}

fn run_layout_suite(test_name: &'static str, run_count: usize) {
    let mut samples = Vec::new();
    let mut dirs: Vec<tempfile::TempDir> = Vec::new();
    for &layout in LAYOUTS {
        let (dir, row_ids, wide_miss) = build_layout_fixture(layout, run_count);
        let db_path = dir.path().to_path_buf();
        let db = Database::open(&db_path).unwrap();
        let actual_runs = db.table("t").unwrap().read().run_count();
        let latencies = measure_layout_latency(&db, &row_ids, wide_miss, actual_runs);
        samples.push(serde_json::json!({
            "layout": layout.as_str(),
            "target_runs": run_count,
            "actual_runs": actual_runs,
            "rows": row_ids.len(),
            "latencies": latencies,
        }));
        dirs.push(dir);
    }
    println!(
        "{}",
        serde_json::json!({
            "test": test_name,
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "samples": samples,
        })
    );
}

#[test]
fn point_lookup_layouts_64_runs() {
    run_layout_suite("point_lookup_layouts_64_runs", 64);
}

/// 256-run suite. Heavy fixture (>1M rows per layout); marked `#[ignore]` so
/// `cargo test -p mongreldb-core --test point_lookup_runs` stays fast.
/// Run on demand with `-- --ignored --nocapture`.
#[test]
#[ignore = "expensive: ~1.28M rows per layout; run with -- --ignored --nocapture"]
fn point_lookup_layouts_256_runs() {
    run_layout_suite("point_lookup_layouts_256_runs", 256);
}
