//! PGM-served Range: an Int64 column declared `IndexKind::LearnedRange` is
//! served sub-linearly by a per-column learned index (not a column scan).

use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Table, Value};
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
                name: "v".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![IndexDef {
            name: "v_learned".into(),
            column_id: 2,
            kind: IndexKind::LearnedRange,
        }],
        colocation: vec![], constraints: Default::default(),
    }
}

#[test]
fn learned_range_serves_exact_results() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    // Values intentionally out of row-id order, spanning negatives.
    let vals = [100i64, -50, 0, -50, 1000, 7, -50, 250, 7, -1];
    db.bulk_load(
        vals.iter()
            .enumerate()
            .map(|(i, v)| vec![(1, Value::Int64(i as i64)), (2, Value::Int64(*v))])
            .collect::<Vec<_>>(),
    )
    .unwrap();

    // Range [-50, 7] ⇒ values -50(x3), -1, 0, 7(x2) = 7 rows.
    let q = Query::new().and(Condition::Range {
        column_id: 2,
        lo: -50,
        hi: 7,
    });
    let rows = db.query(&q).unwrap();
    assert_eq!(rows.len(), 7);

    // Negative-only range.
    let q = Query::new().and(Condition::Range {
        column_id: 2,
        lo: i64::MIN,
        hi: -1,
    });
    let rows = db.query(&q).unwrap();
    assert_eq!(rows.len(), 4); // -50, -50, -50, -1

    // Empty range.
    let q = Query::new().and(Condition::Range {
        column_id: 2,
        lo: 500,
        hi: 600,
    });
    assert!(db.query(&q).unwrap().is_empty());
}

#[test]
fn learned_range_matches_full_scan_answer() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.bulk_load(
        (0..5000i64)
            .map(|i| {
                vec![
                    (1, Value::Int64(i)),
                    // quasi-random values across the int range
                    (2, Value::Int64((i.wrapping_mul(2654435761) % 10000) - 5000)),
                ]
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();

    // Compare learned-served range against a brute-force count.
    let (lo, hi) = (-1000i64, 1000i64);
    let learned = db
        .query(&Query::new().and(Condition::Range {
            column_id: 2,
            lo,
            hi,
        }))
        .unwrap()
        .len();

    let snap = db.snapshot();
    let brute = db
        .visible_rows(snap)
        .unwrap()
        .iter()
        .filter(|r| matches!(r.columns.get(&2), Some(Value::Int64(v)) if *v >= lo && *v <= hi))
        .count();
    assert_eq!(learned, brute);
}
