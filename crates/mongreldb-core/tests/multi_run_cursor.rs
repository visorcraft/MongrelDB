//! Phase 16.1: multi-run streaming cursor.
//!
//! `native_multi_run_cursor` does a k-way merge by `RowId` across N sorted
//! runs, resolving cross-run MVCC (newest visible version per `RowId`,
//! including tombstones) and the predicate up front, then lazily decoding only
//! the projected columns of surviving pages. These tests pin it against the
//! authoritative `Table::visible_rows` for multi-run layouts with overlapping
//! versions, deletes, and a live memtable overlay.

use mongreldb_core::columnar::NativeColumn;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Condition, Cursor, Table, Value};
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
                name: "cost".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
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

fn i64s(col: &NativeColumn) -> Vec<i64> {
    match col {
        NativeColumn::Int64 { data, .. } => data.clone(),
        _ => vec![],
    }
}

/// Drain a cursor projected as `(id, cost)` into a flat `(id, cost)` list.
fn drain_id_cost(
    db: &Table,
    snap: mongreldb_core::Snapshot,
    conditions: &[Condition],
) -> Vec<(i64, i64)> {
    let mut cur = db
        .native_multi_run_cursor(
            snap,
            vec![(1, TypeId::Int64), (2, TypeId::Int64)],
            conditions,
        )
        .unwrap()
        .expect("multi-run cursor should fire");
    let mut out = Vec::new();
    while let Some(batch) = cur.next_batch().unwrap() {
        let ids = i64s(&batch[0]);
        let costs = i64s(&batch[1]);
        for (a, b) in ids.iter().zip(costs.iter()) {
            out.push((*a, *b));
        }
    }
    out
}

#[test]
fn multi_run_cursor_equals_visible_rows() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1); // each flush spills a fresh run

    // Run 1: id 0..200.
    let mut run1_ids = Vec::new();
    for i in 0..200i64 {
        let rid = db
            .put(vec![(1, Value::Int64(i)), (2, Value::Int64(i * 10))])
            .unwrap();
        run1_ids.push(rid);
    }
    db.flush().unwrap();

    // Run 2: id 200..400 (disjoint row-ids).
    for i in 200..400i64 {
        db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(i * 10))])
            .unwrap();
    }
    db.flush().unwrap();

    // Run 3: tombstone for id 50 (cross-run delete — newest version is a tombstone).
    db.delete(run1_ids[50]).unwrap();
    db.flush().unwrap();

    // Overlay: unflushed inserts id 400..420 + delete a run-2 row by row-id.
    let run2_r250 = {
        // row-id for id==250 is the 51st insert of run 2 (200..400 ⇒ index 50).
        // Re-derive via a PK lookup so the test does not hard-code allocator state.
        db.put(vec![(1, Value::Int64(250)), (2, Value::Int64(2500))])
            .unwrap();
        let pk = db.lookup_pk(&Value::Int64(250).encode_key()).unwrap();
        db.delete(pk).unwrap();
        pk
    };
    for i in 400..420i64 {
        db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(i * 10))])
            .unwrap();
    }
    db.commit().unwrap();
    let _ = run2_r250;

    let snap = db.snapshot();
    assert!(db.run_count() >= 3, "expected a multi-run layout");

    // Authoritative reference.
    let mut expect: Vec<(i64, i64)> = db
        .visible_rows(snap)
        .unwrap()
        .into_iter()
        .map(|r| {
            let id = match r.columns.get(&1) {
                Some(Value::Int64(v)) => *v,
                _ => i64::MIN,
            };
            let cost = match r.columns.get(&2) {
                Some(Value::Int64(v)) => *v,
                _ => i64::MIN,
            };
            (id, cost)
        })
        .collect();
    expect.sort_unstable();

    let got = drain_id_cost(&db, snap, &[]);

    // The cursor streams in ascending RowId order; row-ids are allocated in
    // insertion order and ids are inserted ascending, so the raw stream must
    // already be ascending by id (this validates the k-way merge ordering).
    let mut ordered = got.clone();
    ordered.sort_unstable();
    assert_eq!(
        got, ordered,
        "cursor output must be in ascending RowId order"
    );

    assert_eq!(got, expect, "multi-run cursor must equal visible_rows");
    // id 50 (tombstoned across runs) and the deleted overlay row must be absent.
    assert!(
        got.iter().all(|(id, _)| *id != 50),
        "tombstoned id 50 must be dropped"
    );
}

#[test]
fn multi_run_cursor_applies_predicate() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1);

    for run in 0..3i64 {
        for i in 0..100i64 {
            let id = run * 1000 + i;
            db.put(vec![(1, Value::Int64(id)), (2, Value::Int64(id))])
                .unwrap();
        }
        db.flush().unwrap();
    }
    assert!(db.run_count() >= 3);

    let snap = db.snapshot();
    let cond = Condition::BitmapEq {
        column_id: 2,
        value: Value::Int64(1050).encode_key(), // unique ⇒ exactly id 1050 (run 1, i=50)
    };
    let got = drain_id_cost(&db, snap, std::slice::from_ref(&cond));
    assert_eq!(
        got,
        vec![(1050, 1050)],
        "predicate survivor in a multi-run layout"
    );

    // COUNT(*)-style exact row count via remaining_rows (no batch drain needed).
    let cur = db
        .native_multi_run_cursor(snap, vec![(1, TypeId::Int64)], &[cond])
        .unwrap()
        .unwrap();
    assert_eq!(cur.remaining_rows(), 1);
}

#[test]
fn multi_run_cursor_limits_short_circuits() {
    // A streaming cursor should not need to decode every page to satisfy a
    // small pull. We can't assert decode counts directly, but we can confirm
    // correctness when only the first batch is consumed and the rest dropped.
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1);
    for run in 0..4i64 {
        for i in 0..1000i64 {
            let id = run * 10_000 + i;
            db.put(vec![(1, Value::Int64(id)), (2, Value::Int64(id))])
                .unwrap();
        }
        db.flush().unwrap();
    }
    let snap = db.snapshot();
    let mut cur = db
        .native_multi_run_cursor(snap, vec![(1, TypeId::Int64)], &[])
        .unwrap()
        .unwrap();
    let first = cur.next_batch().unwrap().expect("at least one batch");
    let n = i64s(&first.into_iter().next().unwrap()).len();
    assert!(
        n > 0 && n <= 65_536,
        "first batch within the merge page bound: {n}"
    );
}
