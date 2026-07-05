//! §5.9 — compaction as a query optimization: run-count cost threshold +
//! `maybe_compact` collapses accumulated runs back to one.

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Table, Value};
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "v".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

#[test]
fn maybe_compact_triggers_on_run_threshold_and_preserves_data() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    // Force a new sorted run per flush.
    db.set_mutable_run_spill_bytes(1);

    // Accumulate `AUTO_COMPACT_RUN_THRESHOLD + 2` runs, each holding one row.
    let n_runs = Table::AUTO_COMPACT_RUN_THRESHOLD + 2;
    for i in 0..n_runs as i64 {
        db.put(vec![(1, Value::Int64(i))]).unwrap();
        db.flush().unwrap();
    }
    assert!(
        db.run_count() >= n_runs,
        "expected >= {n_runs} runs, got {}",
        db.run_count()
    );
    assert!(
        db.should_compact(),
        "should_compact must be true at {} runs (threshold {})",
        db.run_count(),
        Table::AUTO_COMPACT_RUN_THRESHOLD
    );

    // maybe_compact runs compaction and reports it.
    let ran = db.maybe_compact().unwrap();
    assert!(ran, "maybe_compact should have run");

    // Collapsed to exactly one run; no longer over threshold.
    assert_eq!(db.run_count(), 1, "post-compaction run count");
    assert!(!db.should_compact());

    // Data integrity: all rows survived, in order.
    let snap = db.snapshot();
    let rows = db.visible_rows(snap).unwrap();
    let vals: Vec<i64> = rows
        .iter()
        .filter_map(|r| match r.columns.get(&1) {
            Some(Value::Int64(v)) => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(vals, (0..n_runs as i64).collect::<Vec<_>>());

    // maybe_compact is now a no-op.
    let ran2 = db.maybe_compact().unwrap();
    assert!(!ran2, "maybe_compact should be a no-op below threshold");
}

#[test]
fn maybe_compact_noop_under_threshold() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1);
    for i in 0..3i64 {
        db.put(vec![(1, Value::Int64(i))]).unwrap();
        db.flush().unwrap();
    }
    assert_eq!(db.run_count(), 3);
    assert!(!db.should_compact(), "3 runs is below the threshold");
    let ran = db.maybe_compact().unwrap();
    assert!(!ran);
    assert_eq!(db.run_count(), 3, "untouched");
}
