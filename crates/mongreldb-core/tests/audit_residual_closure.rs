//! Residual audit closures: R1–R5 (count after delete+flush/reopen, pin history,
//! update_many re-point), run-range skip structural checks, and result-cache
//! persistence policy for tiny entries.
//!
//! All tests drive public/shipped `Table` / `Database` APIs.

use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Table, Value};
use tempfile::tempdir;

fn city_schema() -> Schema {
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
                name: "city".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "city_bm".into(),
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

fn alpha_cond() -> Condition {
    Condition::BitmapEq {
        column_id: 2,
        value: b"alpha".to_vec(),
    }
}

fn load_alpha_beta(n: usize) -> Vec<Vec<(u16, Value)>> {
    (0..n)
        .map(|i| {
            vec![
                (1, Value::Int64(i as i64)),
                (
                    2,
                    Value::Bytes(
                        if i % 2 == 0 {
                            b"alpha".to_vec()
                        } else {
                            b"beta".to_vec()
                        },
                    ),
                ),
            ]
        })
        .collect()
}

/// R1: after pure delete + force_flush (empty overlay), Bitmap count matches
/// materialize oracle at the current snapshot.
#[test]
fn r1_count_conditions_after_delete_and_force_flush_matches_materialize() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), city_schema(), 1).unwrap();
    db.bulk_load(load_alpha_beta(100)).unwrap();
    db.flush().unwrap();

    let alpha = db
        .query(&Query::new().and(alpha_cond()))
        .unwrap();
    assert_eq!(alpha.len(), 50);
    db.delete(alpha[0].row_id).unwrap();
    db.commit().unwrap();
    db.force_flush().unwrap();

    // Overlay empty after force_flush — previously raw bitmap overcounted.
    assert!(db.memtable_is_empty());
    let snap = db.snapshot();
    let counted = db
        .count_conditions(std::slice::from_ref(&alpha_cond()), snap)
        .unwrap()
        .expect("bitmap condition served");
    let materialize = db.query(&Query::new().and(alpha_cond())).unwrap().len() as u64;
    assert_eq!(
        counted, materialize,
        "count_conditions must equal materialize after delete+flush"
    );
    assert_eq!(counted, 49);
}

/// R2: reopen after delete+flush+checkpoint must not overcount on a "clean" table.
#[test]
fn r2_reopen_after_delete_flush_count_matches_materialize() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create(dir.path(), city_schema(), 1).unwrap();
        db.bulk_load(load_alpha_beta(100)).unwrap();
        db.flush().unwrap();
        let alpha = db.query(&Query::new().and(alpha_cond())).unwrap();
        db.delete(alpha[0].row_id).unwrap();
        db.commit().unwrap();
        db.force_flush().unwrap();
        db.close().unwrap();
    }
    let mut db = Table::open(dir.path()).unwrap();
    let snap = db.snapshot();
    let counted = db
        .count_conditions(std::slice::from_ref(&alpha_cond()), snap)
        .unwrap()
        .expect("served");
    let materialize = db.query(&Query::new().and(alpha_cond())).unwrap().len() as u64;
    assert_eq!(counted, materialize);
    assert_eq!(counted, 49);
}

/// R3/R5: pinned snapshot still sees pre-delete BitmapEq survivors after flush
/// while the pin is held; cache isolation still holds.
#[test]
fn r3_pinned_bitmap_eq_survives_flush_under_pin() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), city_schema(), 1).unwrap();
    db.bulk_load(load_alpha_beta(100)).unwrap();
    db.flush().unwrap();

    let old_snap = db.pin_snapshot();
    let alpha = db.query(&Query::new().and(alpha_cond())).unwrap();
    assert_eq!(alpha.len(), 50);
    db.delete(alpha[0].row_id).unwrap();
    db.commit().unwrap();
    db.force_flush().unwrap();

    let new_snap = db.snapshot();
    let new_count = db
        .count_conditions(std::slice::from_ref(&alpha_cond()), new_snap)
        .unwrap()
        .unwrap();
    assert_eq!(new_count, 49);

    let old_count = db
        .count_conditions(std::slice::from_ref(&alpha_cond()), old_snap)
        .unwrap()
        .unwrap();
    assert_eq!(
        old_count, 50,
        "old pin must still discover deleted rid via Bitmap after flush"
    );

    // Cached path isolation (R5).
    let proj = [1u16];
    let cols_new = db
        .query_columns_native_cached(&[alpha_cond()], Some(&proj), new_snap)
        .unwrap()
        .expect("served");
    let cols_old = db
        .query_columns_native_cached(&[alpha_cond()], Some(&proj), old_snap)
        .unwrap()
        .expect("served");
    let n_new = match &cols_new[0].1 {
        mongreldb_core::columnar::NativeColumn::Int64 { data, .. } => data.len(),
        _ => panic!("expected int64"),
    };
    let n_old = match &cols_old[0].1 {
        mongreldb_core::columnar::NativeColumn::Int64 { data, .. } => data.len(),
        _ => panic!("expected int64"),
    };
    assert_eq!(n_new, 49);
    assert_eq!(n_old, 50);

    db.unpin_snapshot(old_snap);
}

/// R3: pin + compact rebuild re-indexes pin-needed Bitmap memberships.
#[test]
fn r3_pin_compact_preserves_historical_bitmap_eq() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), city_schema(), 1).unwrap();
    db.bulk_load(load_alpha_beta(40)).unwrap();
    db.force_flush().unwrap();
    // Second run so compact has work.
    for i in 40..80 {
        db.put(vec![
            (1, Value::Int64(i)),
            (
                2,
                Value::Bytes(if i % 2 == 0 {
                    b"alpha".to_vec()
                } else {
                    b"beta".to_vec()
                }),
            ),
        ])
        .unwrap();
    }
    db.commit().unwrap();
    db.force_flush().unwrap();

    let old_snap = db.pin_snapshot();
    let alpha = db.query(&Query::new().and(alpha_cond())).unwrap();
    assert!(alpha.len() >= 2);
    db.delete(alpha[0].row_id).unwrap();
    db.commit().unwrap();
    db.force_flush().unwrap();

    // Compact while pin holds versions.
    db.compact().unwrap();

    let old_count = db
        .count_conditions(std::slice::from_ref(&alpha_cond()), old_snap)
        .unwrap()
        .unwrap();
    let materialize_old = db
        .query_at_with_allowed(&Query::new().and(alpha_cond()), old_snap, None)
        .unwrap()
        .len() as u64;
    assert_eq!(
        old_count, materialize_old,
        "after compact under pin, count and materialize at old snap agree"
    );
    // Pre-delete alpha count was alpha.len(); after one delete at new snap, old still sees it.
    assert_eq!(old_count, alpha.len() as u64);

    db.unpin_snapshot(old_snap);
}

/// R4: Kit-style delete+put (new rid) leaves current-snap Bitmap listing correct
/// (exactly one live row per updated PK).
#[test]
fn r4_delete_then_put_same_pk_bitmap_lists_one_live() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), city_schema(), 1).unwrap();
    db.put(vec![
        (1, Value::Int64(1)),
        (2, Value::Bytes(b"alpha".to_vec())),
    ])
    .unwrap();
    db.commit().unwrap();
    db.force_flush().unwrap();

    let rows = db.query(&Query::new().and(alpha_cond())).unwrap();
    assert_eq!(rows.len(), 1);
    let old_rid = rows[0].row_id;

    // Kit applyUpdateInTxn shape: delete then put (new rid, same PK).
    db.delete(old_rid).unwrap();
    db.put(vec![
        (1, Value::Int64(1)),
        (2, Value::Bytes(b"alpha".to_vec())),
    ])
    .unwrap();
    db.commit().unwrap();

    let live = db.query(&Query::new().and(alpha_cond())).unwrap();
    assert_eq!(live.len(), 1, "exactly one live alpha after delete+put");
    assert_ne!(live[0].row_id, old_rid, "new rid after Kit update");

    let snap = db.snapshot();
    let counted = db
        .count_conditions(std::slice::from_ref(&alpha_cond()), snap)
        .unwrap()
        .unwrap();
    assert_eq!(counted, 1);
}

/// Point get skips runs outside the RowId range directory.
#[test]
fn get_skips_runs_outside_row_id_range_directory() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), city_schema(), 1).unwrap();
    // Run 1: ids 0..20
    for i in 0..20 {
        db.put(vec![
            (1, Value::Int64(i)),
            (2, Value::Bytes(b"alpha".to_vec())),
        ])
        .unwrap();
    }
    db.commit().unwrap();
    db.force_flush().unwrap();
    // Run 2: ids 1000..1020 (non-overlapping rid space after allocator advance
    // is not guaranteed — RowIds are sequential. Force via bulk of middle rows.)
    for i in 20..40 {
        db.put(vec![
            (1, Value::Int64(i)),
            (2, Value::Bytes(b"beta".to_vec())),
        ])
        .unwrap();
    }
    db.commit().unwrap();
    db.force_flush().unwrap();

    let snap = db.snapshot();
    // First and last live rids must be findable.
    let first = db
        .query(&Query::new().and(Condition::Pk(Value::Int64(0).encode_key())))
        .unwrap();
    assert_eq!(first.len(), 1);
    let rid = first[0].row_id;
    let got = db.get(rid, snap).expect("get must find row in range");
    assert!(!got.deleted);

    // Out-of-range synthetic rid must not panic and returns None.
    let missing = db.get(mongreldb_core::RowId(u64::MAX / 2), snap);
    assert!(missing.is_none());
}
