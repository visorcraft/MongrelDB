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
//! - The full-feature build (`cluster`/`oidc`/`vault-kms`) is a separate
//!   gate. Those features live on `mongreldb-server`, not `mongreldb-core`,
//!   so the full-feature P2 gate runs the server loopback benchmark:
//!   `cargo test -p mongreldb-server --test scale_test --release --features cluster,oidc,vault-kms -- --nocapture loopback_point_query_p95_baseline`
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

/// Full latency statistics for one benchmark, in nanoseconds. REM-J
/// requires p50/p95/p99 plus min/max and the median absolute deviation so
/// evidence captures the distribution shape, not just two percentiles.
#[derive(Clone, Copy)]
struct Stats {
    iters: usize,
    p50_ns: u128,
    p95_ns: u128,
    p99_ns: u128,
    min_ns: u128,
    max_ns: u128,
    mad_ns: u128,
}

impl Stats {
    fn from_samples(mut samples_ns: Vec<u128>) -> Self {
        debug_assert!(!samples_ns.is_empty());
        samples_ns.sort_unstable();
        let iters = samples_ns.len();
        let percentile = |q: usize| samples_ns[(iters * q) / 100].min(samples_ns[iters - 1]);
        let p50_ns = percentile(50);
        // Median absolute deviation: median of |sample - p50|.
        let mut deviations: Vec<u128> = samples_ns
            .iter()
            .map(|sample| sample.abs_diff(p50_ns))
            .collect();
        deviations.sort_unstable();
        Stats {
            iters,
            p50_ns,
            p95_ns: percentile(95),
            p99_ns: percentile(99),
            min_ns: samples_ns[0],
            max_ns: samples_ns[iters - 1],
            mad_ns: deviations[iters / 2],
        }
    }

    fn to_json(self) -> serde_json::Value {
        serde_json::json!({
            "iters": self.iters,
            "p50_us": self.p50_ns as f64 / 1e3,
            "p95_us": self.p95_ns as f64 / 1e3,
            "p99_us": self.p99_ns as f64 / 1e3,
            "min_us": self.min_ns as f64 / 1e3,
            "max_us": self.max_ns as f64 / 1e3,
            "mad_us": self.mad_ns as f64 / 1e3,
        })
    }
}

fn measure<F: FnMut()>(iters: usize, mut f: F) -> Stats {
    // Warm-up: prime caches and lazy initializations.
    f();
    let mut samples_ns: Vec<u128> = Vec::with_capacity(iters);
    for _ in 0..iters {
        let started = Instant::now();
        f();
        samples_ns.push(started.elapsed().as_nanos());
    }
    Stats::from_samples(samples_ns)
}

#[test]
fn p0_p2_split_benchmarks() {
    let mut samples = serde_json::Map::new();

    // 1. table_create_only: time `Table::create` over many fresh tempdirs.
    let create_stats = measure(20, || {
        let dir = tempdir().unwrap();
        let _t = Table::create(dir.path(), pk_schema(), 1).unwrap();
    });
    samples.insert("table_create_only".into(), create_stats.to_json());

    // 2. first_put_after_create: time the FIRST put on a fresh table. The
    //    P0 fix specifically targeted this path; numbers should be close to
    //    `put_steady_state_on_reused_table` after the deferred-root work.
    let first_stats = measure(20, || {
        let dir = tempdir().unwrap();
        let mut t = Table::create(dir.path(), pk_schema(), 1).unwrap();
        t.put(vec![(1, Value::Int64(1))]).unwrap();
        t.commit().unwrap();
    });
    samples.insert("first_put_after_create".into(), first_stats.to_json());

    // 3. put_steady_state_on_reused_table: amortize across many puts so any
    //    one-time table-create cost drops out. This is the metric that was
    //    conflated with creation in the original regression. Per-put samples
    //    give the full REM-J distribution; the amortized average is kept for
    //    continuity with the historical SLO.
    const STEADY_PUTS: usize = 1_000;
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    let steady_started = Instant::now();
    let mut steady_samples_ns: Vec<u128> = Vec::with_capacity(STEADY_PUTS);
    for i in 0..STEADY_PUTS {
        let put_started = Instant::now();
        table.put(vec![(1, Value::Int64(i as i64))]).unwrap();
        steady_samples_ns.push(put_started.elapsed().as_nanos());
        if i % 100 == 99 {
            table.commit().unwrap();
        }
    }
    table.commit().unwrap();
    let steady_total_ns = steady_started.elapsed().as_nanos();
    let steady_avg_ns = steady_total_ns / STEADY_PUTS as u128;
    let mut steady_json = Stats::from_samples(steady_samples_ns).to_json();
    steady_json
        .as_object_mut()
        .unwrap()
        .insert("avg_us_per_put".into(), (steady_avg_ns as f64 / 1e3).into());
    steady_json
        .as_object_mut()
        .unwrap()
        .insert("total_ms".into(), (steady_total_ns as f64 / 1e6).into());
    samples.insert("put_steady_state_on_reused_table".into(), steady_json);

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
    let commit_stats = measure(50, || {
        table.put(vec![(1, Value::Int64(0))]).unwrap();
        table.commit().unwrap();
    });
    samples.insert("commit_fsync".into(), commit_stats.to_json());

    println!(
        "{}",
        serde_json::json!({
            "test": "p0_p2_split_benchmarks",
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "samples": samples,
        })
    );
}
