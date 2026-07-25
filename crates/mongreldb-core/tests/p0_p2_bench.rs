//! P0 / P2 split benchmark suite.
//!
//! Background: the original `put_no_fsync` regression turned out to be
//! dominated by fresh-table setup, not steady-state writes. The P0 fix
//! (deferred-root sync + finalization pass) improved table creation and the
//! first put, but did not establish that the steady-state put had ever
//! doubled. To make future regressions unambiguous, this suite splits the
//! write path into five micro-benchmarks and prints JSON suitable for
//! harvesting into BENCHMARKS.md:
//!
//! - `table_create_only` — fresh `Table::create`
//! - `first_put_after_create` — first `put` on a brand-new table
//! - `put_steady_state_on_reused_table` — average latency across many puts
//!   after the table has been warmed
//! - `put_batch_1000` — wall time to insert 1 000 rows in one transaction
//! - `commit_fsync` — average `commit()` wall time (includes the fsync)
//!
//! Gate separation:
//! - The default-standalone build (no `cluster`/`oidc`/`vault-kms` features)
//!   is the production target for P2; the numbers below match that path.
//! - The full-feature build (`--features cluster,oidc,vault-kms`) is a
//!   separate gate; it pulls in additional cold code paths and is run via:
//!   `cargo test -p mongreldb-core --test p0_p2_bench --release --features cluster,oidc,vault-kms`
//! - The two gates must both pass; never merge one gate's numbers into the
//!   other. Compare both against the documented SLOs in BENCHMARKS.md.
//!
//! Run in release mode for meaningful numbers:
//!   `cargo test -p mongreldb-core --test p0_p2_bench --release -- --nocapture`

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Table, Value};
use std::time::Instant;
use tempfile::tempdir;

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

fn measure<F: FnMut()>(iters: usize, mut f: F) -> (u128, u128) {
    // Warm-up: prime caches and lazy initializations.
    f();
    let mut samples_ns: Vec<u128> = Vec::with_capacity(iters);
    for _ in 0..iters {
        let started = Instant::now();
        f();
        samples_ns.push(started.elapsed().as_nanos());
    }
    samples_ns.sort_unstable();
    let p50 = samples_ns[iters / 2];
    let p95 = samples_ns[(iters * 95) / 100];
    (p50, p95)
}

#[test]
fn p0_p2_split_benchmarks() {
    let mut samples = serde_json::Map::new();

    // 1. table_create_only: time `Table::create` over many fresh tempdirs.
    let (p50_create_ns, p95_create_ns) = measure(20, || {
        let dir = tempdir().unwrap();
        let _t = Table::create(dir.path(), pk_schema(), 1).unwrap();
    });
    samples.insert(
        "table_create_only".into(),
        serde_json::json!({
            "iters": 20,
            "p50_us": p50_create_ns as f64 / 1e3,
            "p95_us": p95_create_ns as f64 / 1e3,
        }),
    );

    // 2. first_put_after_create: time the FIRST put on a fresh table. The
    //    P0 fix specifically targeted this path; numbers should be close to
    //    `put_steady_state_on_reused_table` after the deferred-root work.
    let (p50_first_ns, p95_first_ns) = measure(20, || {
        let dir = tempdir().unwrap();
        let mut t = Table::create(dir.path(), pk_schema(), 1).unwrap();
        t.put(vec![(1, Value::Int64(1))]).unwrap();
        t.commit().unwrap();
    });
    samples.insert(
        "first_put_after_create".into(),
        serde_json::json!({
            "iters": 20,
            "p50_us": p50_first_ns as f64 / 1e3,
            "p95_us": p95_first_ns as f64 / 1e3,
        }),
    );

    // 3. put_steady_state_on_reused_table: amortize across many puts so any
    //    one-time table-create cost drops out. This is the metric that was
    //    conflated with creation in the original regression.
    const STEADY_PUTS: usize = 1_000;
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    let steady_started = Instant::now();
    for i in 0..STEADY_PUTS {
        table.put(vec![(1, Value::Int64(i as i64))]).unwrap();
        if i % 100 == 99 {
            table.commit().unwrap();
        }
    }
    table.commit().unwrap();
    let steady_total_ns = steady_started.elapsed().as_nanos();
    let steady_avg_ns = steady_total_ns / STEADY_PUTS as u128;
    samples.insert(
        "put_steady_state_on_reused_table".into(),
        serde_json::json!({
            "iters": STEADY_PUTS,
            "avg_us_per_put": steady_avg_ns as f64 / 1e3,
            "total_ms": steady_total_ns as f64 / 1e6,
        }),
    );

    // 4. put_batch_1000: a single 1 000-row transaction. Different cost
    //    profile from steady-state single puts (one commit, one fsync).
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    let batch_started = Instant::now();
    let mut ids = Vec::with_capacity(1_000);
    for i in 0..1_000i64 {
        let rid = table.put(vec![(1, Value::Int64(i))]).unwrap();
        ids.push(rid);
    }
    table.commit().unwrap();
    let batch_total_ns = batch_started.elapsed().as_nanos();
    samples.insert(
        "put_batch_1000".into(),
        serde_json::json!({
            "iters": 1,
            "rows": 1_000,
            "total_ms": batch_total_ns as f64 / 1e6,
            "avg_us_per_put": (batch_total_ns / 1_000) as f64 / 1e3,
        }),
    );

    // 5. commit_fsync: time `commit()` in isolation. This is the
    //    fsync-bound path; measured on the same reused table to avoid
    //    double-counting the per-batch amortized commit cost.
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    for i in 0..200i64 {
        table.put(vec![(1, Value::Int64(i))]).unwrap();
    }
    let (p50_commit_ns, p95_commit_ns) = measure(50, || {
        table.put(vec![(1, Value::Int64(0))]).unwrap();
        table.commit().unwrap();
    });
    samples.insert(
        "commit_fsync".into(),
        serde_json::json!({
            "iters": 50,
            "p50_us": p50_commit_ns as f64 / 1e3,
            "p95_us": p95_commit_ns as f64 / 1e3,
        }),
    );

    println!(
        "{}",
        serde_json::json!({
            "test": "p0_p2_split_benchmarks",
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "samples": samples,
        })
    );
}
