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
        lcg = lcg.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
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
