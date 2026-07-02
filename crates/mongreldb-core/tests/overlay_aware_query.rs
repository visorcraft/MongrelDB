//! §5.1: overlay-aware querying. In-memory indexes (HOT, bitmap, FM) are
//! maintained on every `put` via `index_row`, so a query against a *dirty*
//! table (non-empty memtable overlay, no flush) must still resolve unflushed
//! rows through the bitmap/HOT fast paths — not just the flushed sorted runs.
//!
//! These tests pin that invariant: inserting rows that match a `BitmapEq`
//! condition without flushing, then querying, must return both the flushed and
//! the unflushed matching rows. They also guard the survivor-bounded overlay
//! materialization (only matching rows are touched, never the full overlay).

use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Condition, Query, Table, Value};
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
            },
            ColumnDef {
                id: 2,
                name: "cat".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![IndexDef {
            name: "cat_bm".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
        }],
        colocation: vec![],
        constraints: Default::default(),
    }
}

/// A bitmap equality query on a dirty table must include unflushed memtable
/// rows — the bitmap is updated on every `put`, not only at flush.
#[test]
fn bitmap_eq_includes_unflushed_overlay_rows() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();

    // Flushed rows: cat=7 for id 0..50.
    for i in 0..50i64 {
        db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(7))])
            .unwrap();
    }
    db.flush().unwrap();

    // Unflushed overlay: 30 more cat=7 rows (id 50..80) sitting in the memtable.
    for i in 50..80i64 {
        db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(7))])
            .unwrap();
    }
    db.commit().unwrap();

    // A different cat value, also unflushed, that must NOT match.
    db.put(vec![(1, Value::Int64(999)), (2, Value::Int64(9))])
        .unwrap();
    db.commit().unwrap();

    let snap = db.snapshot();
    let cond = Condition::BitmapEq {
        column_id: 2,
        value: Value::Int64(7).encode_key(),
    };
    let count = db
        .count_conditions(std::slice::from_ref(&cond), snap)
        .unwrap()
        .expect("bitmap condition is served");
    // 50 flushed + 30 unflushed = 80; the cat=9 row is excluded.
    assert_eq!(count, 80, "bitmap query must include unflushed overlay rows");

    // And the materialized query must return exactly those 80 ids (0..80).
    let mut rows = db.query(&Query::new().and(cond)).unwrap();
    rows.sort_by_key(|r| match r.columns.get(&1) {
        Some(Value::Int64(v)) => *v,
        _ => i64::MIN,
    });
    let ids: Vec<i64> = rows
        .iter()
        .map(|r| match r.columns.get(&1) {
            Some(Value::Int64(v)) => *v,
            _ => i64::MIN,
        })
        .collect();
    assert_eq!(ids, (0..80).collect::<Vec<_>>());
}

/// PK (HOT) point lookup must find an unflushed row.
#[test]
fn pk_lookup_finds_unflushed_overlay_row() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();

    db.put(vec![(1, Value::Int64(1)), (2, Value::Int64(7))])
        .unwrap();
    db.flush().unwrap();

    // Unflushed PK — never written to a sorted run.
    db.put(vec![(1, Value::Int64(42)), (2, Value::Int64(7))])
        .unwrap();
    db.commit().unwrap();

    let row = db
        .query(&Query::pk(Value::Int64(42).encode_key()))
        .unwrap();
    assert_eq!(row.len(), 1, "HOT must resolve an unflushed PK");
}

/// An unflushed *update* (same PK, new cat) must be reflected in a bitmap
/// query: the old cat value's bitmap entry is removed and the new one added.
#[test]
fn bitmap_reflects_unflushed_update() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();

    // id=10, cat=7 — flushed.
    db.put(vec![(1, Value::Int64(10)), (2, Value::Int64(7))])
        .unwrap();
    db.flush().unwrap();

    // Unflushed update: id=10 → cat=5 (upsert on PK displaces the old row).
    db.put(vec![(1, Value::Int64(10)), (2, Value::Int64(5))])
        .unwrap();
    db.commit().unwrap();

    let snap = db.snapshot();
    let cat7 = db
        .count_conditions(
            &[Condition::BitmapEq {
                column_id: 2,
                value: Value::Int64(7).encode_key(),
            }],
            snap,
        )
        .unwrap()
        .unwrap();
    assert_eq!(cat7, 0, "old cat value must be gone after unflushed update");

    let cat5 = db
        .count_conditions(
            &[Condition::BitmapEq {
                column_id: 2,
                value: Value::Int64(5).encode_key(),
            }],
            snap,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        cat5, 1,
        "new cat value must be visible after unflushed update"
    );
}

/// `BitmapIn` (OR-of-equalities) must also span flushed + overlay rows.
#[test]
fn bitmap_in_includes_unflushed_overlay_rows() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();

    for i in 0..10i64 {
        db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(1))])
            .unwrap();
    }
    db.flush().unwrap();
    // 5 unflushed cat=2 rows.
    for i in 10..15i64 {
        db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(2))])
            .unwrap();
    }
    db.commit().unwrap();

    let snap = db.snapshot();
    let count = db
        .count_conditions(
            &[Condition::BitmapIn {
                column_id: 2,
                values: vec![Value::Int64(1).encode_key(), Value::Int64(2).encode_key()],
            }],
            snap,
        )
        .unwrap()
        .unwrap();
    assert_eq!(count, 15, "BitmapIn must union flushed + overlay rows");
}
