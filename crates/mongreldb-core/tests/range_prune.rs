//! Phase 16.3: page-pruned range resolution on **unindexed** columns must stay
//! correct (and equal the brute-force visible set) under any layout — including
//! a non-empty memtable and multiple runs, the state that previously forced a
//! full-column decode. These columns have NO `LearnedRange` index, so resolution
//! goes through `Table::resolve_condition` → `range_scan_*` (now page-pruned).

use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Table, Value};
use tempfile::tempdir;

/// No LearnedRange index on `v`/`f` → resolution uses the (page-pruned) scan
/// path, not the PGM fast path. A bitmap index on a low-card tag column is
/// present to mirror a realistic schema without serving these ranges.
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
                name: "v".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 3,
                name: "f".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 4,
                name: "tag".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![IndexDef {
            name: "tag_bm".into(),
            column_id: 4,
            kind: IndexKind::Bitmap,
            predicate: None,
        }],
        colocation: vec![],
        constraints: Default::default(),
    }
}

fn row(i: i64) -> Vec<(u16, Value)> {
    vec![
        (1, Value::Int64(i)),
        (2, Value::Int64(i)),          // monotonic int
        (3, Value::Float64(i as f64)), // monotonic float
        (4, Value::Bytes(b"even".to_vec())),
    ]
}

fn brute_count_i64(db: &Table, lo: i64, hi: i64) -> usize {
    db.visible_rows(db.snapshot())
        .unwrap()
        .iter()
        .filter(|r| matches!(r.columns.get(&2), Some(Value::Int64(v)) if *v >= lo && *v <= hi))
        .count()
}

fn brute_count_f64(db: &Table, lo: f64, lo_inc: bool, hi: f64, hi_inc: bool) -> usize {
    db.visible_rows(db.snapshot())
        .unwrap()
        .iter()
        .filter(|r| match r.columns.get(&3) {
            Some(Value::Float64(v)) => {
                let ok_lo = if lo_inc { *v >= lo } else { *v > lo };
                let ok_hi = if hi_inc { *v <= hi } else { *v < hi };
                ok_lo && ok_hi
            }
            _ => false,
        })
        .count()
}

#[test]
fn range_scan_prunes_correctly_with_nonempty_memtable() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    // >1 PAX page (pages are 65 536 rows): 200_000 rows ⇒ 4 pages.
    let n = 200_000i64;
    db.bulk_load((0..n).map(row).collect::<Vec<_>>()).unwrap();

    // Dirty the memtable WITHOUT flushing: put + commit only. This is exactly
    // the state the benchmark reaches — `commit()` does not drain the memtable,
    // so the old "single clean run" gate was false and forced a full-column scan.
    for i in 0..7 {
        db.put(vec![
            (1, Value::Int64(n + i)),
            (2, Value::Int64(n + i)),
            (3, Value::Float64((n + i) as f64)),
            (4, Value::Bytes(b"odd".to_vec())),
        ])
        .unwrap();
        db.commit().unwrap();
    }

    // Selective range that lives entirely in page 0 (v in [10, 20] ⇒ 11 rows).
    // A correct page-pruner keeps only page 0; a full scan would decode all 4.
    let (lo, hi) = (10i64, 20);
    let got = db
        .query(&Query::new().and(Condition::Range {
            column_id: 2,
            lo,
            hi,
        }))
        .unwrap()
        .len();
    assert_eq!(got, brute_count_i64(&db, lo, hi));
    assert_eq!(got, 11);

    // Mid range spanning a page boundary.
    let (lo, hi) = (65_530i64, 65_545);
    let got = db
        .query(&Query::new().and(Condition::Range {
            column_id: 2,
            lo,
            hi,
        }))
        .unwrap()
        .len();
    assert_eq!(got, brute_count_i64(&db, lo, hi));

    // Empty range in the deep tail (nothing matches) — pruning skips every page.
    let got = db
        .query(&Query::new().and(Condition::Range {
            column_id: 2,
            lo: 500_000,
            hi: 600_000,
        }))
        .unwrap()
        .len();
    assert_eq!(got, 0);
}

#[test]
fn range_scan_f64_prunes_correctly_with_nonempty_memtable() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let n = 200_000i64;
    db.bulk_load((0..n).map(row).collect::<Vec<_>>()).unwrap();
    for i in 0..5 {
        db.put(vec![
            (1, Value::Int64(n + i)),
            (2, Value::Int64(n + i)),
            (3, Value::Float64((n + i) as f64)),
            (4, Value::Bytes(b"x".to_vec())),
        ])
        .unwrap();
        db.commit().unwrap();
    }

    let (lo, hi) = (5.0f64, 15.0);
    let got = db
        .query(&Query::new().and(Condition::RangeF64 {
            column_id: 3,
            lo,
            lo_inclusive: true,
            hi,
            hi_inclusive: false,
        }))
        .unwrap()
        .len();
    assert_eq!(got, brute_count_f64(&db, lo, true, hi, false));

    // Exclusive lo / inclusive hi.
    let (lo, hi) = (100.0f64, 200.0);
    let got = db
        .query(&Query::new().and(Condition::RangeF64 {
            column_id: 3,
            lo,
            lo_inclusive: false,
            hi,
            hi_inclusive: true,
        }))
        .unwrap()
        .len();
    assert_eq!(got, brute_count_f64(&db, lo, false, hi, true));
}

#[test]
fn range_scan_correct_after_flush_creates_second_run() {
    // Two runs (bulk_load + flush after extra puts) — the multi-run case.
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.bulk_load((0..70_000i64).map(row).collect::<Vec<_>>())
        .unwrap();
    db.put(row(70_000)).unwrap();
    db.flush().unwrap(); // spills a second run

    let (lo, hi) = (69_990i64, 70_000);
    let got = db
        .query(&Query::new().and(Condition::Range {
            column_id: 2,
            lo,
            hi,
        }))
        .unwrap()
        .len();
    assert_eq!(got, brute_count_i64(&db, lo, hi));
}

#[test]
fn range_scan_respects_deletes() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.bulk_load((0..70_000i64).map(row).collect::<Vec<_>>())
        .unwrap();
    // Delete rows 10..21 (the exact [10,20] survivors), commit (memtable dirty).
    for i in 10..21 {
        db.delete(mongreldb_core::RowId(i as u64)).unwrap();
    }
    db.commit().unwrap();

    let got = db
        .query(&Query::new().and(Condition::Range {
            column_id: 2,
            lo: 10,
            hi: 20,
        }))
        .unwrap()
        .len();
    assert_eq!(got, 0, "deleted rows must not reappear via the range path");
    assert_eq!(got, brute_count_i64(&db, 10, 20));
}
