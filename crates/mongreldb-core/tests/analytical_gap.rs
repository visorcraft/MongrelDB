//! Phase 13: analytical SQL gap closure.
//!
//! 13.1 — resilient lazy page cursor: the fast page-aware cursor fires even
//! with unflushed rows in the memtable / mutable-run overlay. Rows in the
//! overlay are served as a final batch; stale versions in the run are excluded.

use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Condition, NativeAgg, Table, Value};
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
            },
            ColumnDef {
                id: 2,
                name: "cost".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "cost_bm".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
            predicate: None,
            options: Default::default(),
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

#[test]
fn page_cursor_serves_overlay_rows() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1); // force spill so run_refs.len() == 1
    for i in 0..1000i64 {
        db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(i * 10))])
            .unwrap();
    }
    db.flush().unwrap();

    // Pending writes in the memtable (committed but not flushed).
    db.put(vec![(1, Value::Int64(5000)), (2, Value::Int64(42))])
        .unwrap();
    db.put(vec![(1, Value::Int64(5001)), (2, Value::Int64(42))])
        .unwrap();
    db.commit().unwrap();

    let snap = db.snapshot();

    let cursor = db
        .native_page_cursor(
            snap,
            vec![(1, TypeId::Int64), (2, TypeId::Int64)],
            &[Condition::BitmapEq {
                column_id: 2,
                value: Value::Int64(42).encode_key(),
            }],
        )
        .unwrap()
        .expect("cursor should fire with overlay");

    assert_eq!(cursor.remaining_rows(), 2, "two overlay rows match cost=42");

    let mut all_ids: Vec<i64> = Vec::new();
    let mut c = cursor;
    while let Some(batch) = c.next_batch().unwrap() {
        if let Some(mongreldb_core::columnar::NativeColumn::Int64 { data, .. }) = batch.first() {
            all_ids.extend_from_slice(data);
        }
    }
    all_ids.sort_unstable();
    assert_eq!(all_ids, vec![5000, 5001]);
}

#[test]
fn page_cursor_overlay_shadows_run_version() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1);
    let run_rid = db
        .put(vec![(1, Value::Int64(1)), (2, Value::Int64(100))])
        .unwrap();
    db.flush().unwrap();
    assert_eq!(db.run_count(), 1, "one sorted run after spill");

    // Delete the run row + insert a new row with the same PK value but new cost.
    db.delete(run_rid).unwrap();
    db.put(vec![(1, Value::Int64(1)), (2, Value::Int64(200))])
        .unwrap();
    db.commit().unwrap();

    let snap = db.snapshot();

    let cursor = db
        .native_page_cursor(snap, vec![(2, TypeId::Int64)], &[])
        .unwrap()
        .expect("cursor should fire");

    let mut c = cursor;
    let mut costs: Vec<i64> = Vec::new();
    while let Some(batch) = c.next_batch().unwrap() {
        if let Some(mongreldb_core::columnar::NativeColumn::Int64 { data, .. }) = batch.first() {
            costs.extend_from_slice(data);
        }
    }
    // The deleted run row (cost=100) is shadowed by the tombstone; only the
    // new overlay row (cost=200) is visible.
    assert_eq!(costs, vec![200], "overlay version shadows stale run data");
}

#[test]
fn page_cursor_range_filter_with_overlay() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1);
    for i in 0..100i64 {
        db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(i))])
            .unwrap();
    }
    db.flush().unwrap();

    // Overlay rows: some match the range, some don't.
    db.put(vec![(1, Value::Int64(100)), (2, Value::Int64(5))])
        .unwrap();
    db.put(vec![(1, Value::Int64(101)), (2, Value::Int64(99))])
        .unwrap();
    db.commit().unwrap();

    let snap = db.snapshot();
    let cursor = db
        .native_page_cursor(
            snap,
            vec![(1, TypeId::Int64)],
            &[Condition::Range {
                column_id: 2,
                lo: 0,
                hi: 9,
            }],
        )
        .unwrap()
        .expect("cursor should fire with overlay + range");

    let mut c = cursor;
    let mut ids: Vec<i64> = Vec::new();
    while let Some(batch) = c.next_batch().unwrap() {
        if let Some(mongreldb_core::columnar::NativeColumn::Int64 { data, .. }) = batch.first() {
            ids.extend_from_slice(data);
        }
    }
    ids.sort_unstable();
    assert_eq!(
        ids.len(),
        11,
        "10 run rows (cost 0..9) + 1 overlay row (cost=5)"
    );
    assert!(ids.contains(&100), "overlay row matching range included");
    assert!(!ids.contains(&101), "overlay row outside range excluded");
}

#[test]
fn aggregate_count_with_overlay() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1);
    for i in 0..100i64 {
        db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(i))])
            .unwrap();
    }
    db.flush().unwrap();

    for i in 0..5i64 {
        db.put(vec![(1, Value::Int64(1000 + i)), (2, Value::Int64(i))])
            .unwrap();
    }
    db.commit().unwrap();

    let snap = db.snapshot();
    let result = db
        .aggregate_native(snap, None, &[], NativeAgg::Count)
        .unwrap()
        .expect("aggregate should fire with overlay");
    match result {
        mongreldb_core::NativeAggResult::Count(n) => {
            assert_eq!(n, 105, "100 run rows + 5 overlay rows");
        }
        _ => panic!("expected Count result"),
    }
}
