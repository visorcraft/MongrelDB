//! Priority 2: selective overlay probing. A selective index query over a large
//! unflushed overlay must return exactly the matching rows — the engine bounds
//! overlay materialization to the index-resolved survivor set, which must not
//! change the result.

use mongreldb_core::query::Condition;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Snapshot, Table, Value};
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
                name: "cat".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
            ColumnDef {
                id: 3,
                name: "n".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "cat_bm".into(),
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

fn rows_for(snap: Snapshot, db: &mut Table, conds: Vec<Condition>) -> usize {
    let proj: [u16; 1] = [1];
    db.query_columns_native(&conds, Some(&proj), snap)
        .unwrap()
        .map(|cols| cols.first().map(|c| c.1.len()).unwrap_or(0))
        .unwrap_or(0)
}

#[test]
fn selective_index_query_over_large_dirty_overlay() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();

    // Flushed run: 100 rows, cat = "run".
    let run_rows: Vec<Vec<(u16, Value)>> = (0..100i64)
        .map(|i| {
            vec![
                (1, Value::Int64(i)),
                (2, Value::Bytes(b"run".to_vec())),
                (3, Value::Int64(i)),
            ]
        })
        .collect();
    db.bulk_load(run_rows).unwrap();

    // Large unflushed overlay: 500 rows, all cat = "bulk" except one "needle".
    for i in 100..600i64 {
        let cat: &[u8] = if i == 437 { b"needle" } else { b"bulk" };
        db.put(vec![
            (1, Value::Int64(i)),
            (2, Value::Bytes(cat.to_vec())),
            (3, Value::Int64(i)),
        ])
        .unwrap();
    }
    db.commit().unwrap(); // committed but unflushed ⇒ lives in the overlay
    let snap = db.snapshot();

    // Selective bitmap match: exactly one overlay row ("needle").
    assert_eq!(
        rows_for(
            snap,
            &mut db,
            vec![Condition::BitmapEq {
                column_id: 2,
                value: b"needle".to_vec(),
            }]
        ),
        1
    );

    // A value present only in the run (overlay has none) ⇒ all 100 run rows.
    assert_eq!(
        rows_for(
            snap,
            &mut db,
            vec![Condition::BitmapEq {
                column_id: 2,
                value: b"run".to_vec(),
            }]
        ),
        100
    );

    // A value present across the whole overlay ⇒ 499 overlay rows.
    assert_eq!(
        rows_for(
            snap,
            &mut db,
            vec![Condition::BitmapEq {
                column_id: 2,
                value: b"bulk".to_vec(),
            }]
        ),
        499
    );

    // Range residual (no overlay range index) forces full overlay materialization
    // and must still be correct: n in [430, 440] ⇒ ids 430..=440 = 11 rows.
    assert_eq!(
        rows_for(
            snap,
            &mut db,
            vec![Condition::Range {
                column_id: 3,
                lo: 430,
                hi: 440,
            }]
        ),
        11
    );

    // Mixed index + range: cat="needle" AND n in [430,440] ⇒ just id 437.
    assert_eq!(
        rows_for(
            snap,
            &mut db,
            vec![
                Condition::BitmapEq {
                    column_id: 2,
                    value: b"needle".to_vec(),
                },
                Condition::Range {
                    column_id: 3,
                    lo: 430,
                    hi: 440,
                },
            ]
        ),
        1
    );
}
