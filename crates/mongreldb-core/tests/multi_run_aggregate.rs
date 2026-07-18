//! Priority 7 (multi-run): native aggregates stream through the layout-aware
//! `scan_cursor`, so SUM/MIN/MAX/AVG/COUNT(col) work across multiple sorted runs
//! — not just a single run. Mirrors the single-run aggregate coverage.

use mongreldb_core::query::Condition;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{NativeAgg, NativeAggResult, Table, Value};
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "v".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 3,
                name: "f".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

#[test]
fn native_aggregate_spans_multiple_runs() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1); // each flush spills to its own sorted run

    for i in 0..50i64 {
        db.put(vec![
            (1, Value::Int64(i)),
            (2, Value::Int64(i)),
            (3, Value::Float64(i as f64)),
        ])
        .unwrap();
    }
    db.flush().unwrap();
    for i in 50..100i64 {
        db.put(vec![
            (1, Value::Int64(i)),
            (2, Value::Int64(i)),
            (3, Value::Float64(i as f64)),
        ])
        .unwrap();
    }
    db.flush().unwrap();
    assert!(
        db.run_count() >= 2,
        "expected multiple sorted runs, got {}",
        db.run_count()
    );

    let snap = db.snapshot();
    let agg = |col: u16, conds: &[Condition], a: NativeAgg| {
        db.aggregate_native(snap, Some(col), conds, a)
            .unwrap()
            .unwrap()
    };

    // Int64 column v = i for i in 0..100, spread across both runs.
    assert_eq!(
        agg(2, &[], NativeAgg::Sum),
        NativeAggResult::Int((0..100).sum())
    );
    assert_eq!(agg(2, &[], NativeAgg::Min), NativeAggResult::Int(0));
    assert_eq!(agg(2, &[], NativeAgg::Max), NativeAggResult::Int(99));
    assert_eq!(agg(2, &[], NativeAgg::Count), NativeAggResult::Count(100));
    match agg(2, &[], NativeAgg::Avg) {
        NativeAggResult::Float(a) => assert!((a - 49.5).abs() < 1e-9),
        other => panic!("expected Float avg, got {other:?}"),
    }

    // Float64 column.
    assert_eq!(agg(3, &[], NativeAgg::Max), NativeAggResult::Float(99.0));
    assert_eq!(agg(3, &[], NativeAgg::Min), NativeAggResult::Float(0.0));

    // Filtered aggregate spanning runs: v in [10, 19] ⇒ sum 10..=19 = 145.
    assert_eq!(
        agg(
            2,
            &[Condition::Range {
                column_id: 2,
                lo: 10,
                hi: 19,
            }],
            NativeAgg::Sum,
        ),
        NativeAggResult::Int((10..=19).sum()),
    );
    assert_eq!(
        agg(
            2,
            &[Condition::Range {
                column_id: 2,
                lo: 10,
                hi: 19,
            }],
            NativeAgg::Count,
        ),
        NativeAggResult::Count(10),
    );
}
