//! Regression: partial update of a non-indexed column must leave the row
//! findable by both primary key and secondary-index equality.
//!
//! Production signal (Roamarr): after segment updates, rows remained visible
//! by PK but vanished from a trip_id secondary-index list. This test drives
//! the shipped engine write paths (Database transaction put / update_many /
//! delete+put) against a minimal schema shaped like that failure mode.

use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Database, Value};
use tempfile::tempdir;

/// Segments-like: int PK, int FK with Bitmap secondary (Kit default for
/// declared indexes), non-indexed payload column.
fn segments_like_schema() -> Schema {
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
                name: "trip_id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 3,
                name: "title".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "segments_trip_idx".into(),
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

/// Bytes tag + Bitmap (classic secondary equality path).
fn tagged_schema() -> Schema {
    Schema {
        schema_id: 2,
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
                name: "tag".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 3,
                name: "payload".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "tag_bm".into(),
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

fn trip_index_query(trip_id: i64) -> Query {
    Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: Value::Int64(trip_id).encode_key(),
    })
}

fn tag_index_query(tag: &[u8]) -> Query {
    Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: tag.to_vec(),
    })
}

fn lookup_pk(db: &Database, table: &str, pk: i64) -> mongreldb_core::RowId {
    let handle = db.table(table).unwrap();
    let guard = handle.lock();
    guard
        .lookup_pk(&Value::Int64(pk).encode_key())
        .unwrap_or_else(|| panic!("PK {pk} missing"))
}

fn assert_row_by_pk(db: &Database, table: &str, pk: i64, expect_title: Option<&[u8]>) {
    let handle = db.table(table).unwrap();
    let guard = handle.lock();
    let rid = guard
        .lookup_pk(&Value::Int64(pk).encode_key())
        .unwrap_or_else(|| panic!("PK {pk} missing after update"));
    let row = guard
        .get(rid, guard.snapshot())
        .unwrap_or_else(|| panic!("row for PK {pk} not gettable"));
    assert_eq!(row.columns.get(&1), Some(&Value::Int64(pk)));
    if let Some(title) = expect_title {
        assert_eq!(
            row.columns.get(&3),
            Some(&Value::Bytes(title.to_vec())),
            "payload column mismatch for PK {pk}"
        );
    }
}

fn assert_listed_by_trip(db: &Database, table: &str, trip_id: i64, expect_pks: &[i64]) {
    let handle = db.table(table).unwrap();
    let mut guard = handle.lock();
    let rows = guard.query(&trip_index_query(trip_id)).unwrap();
    let mut pks: Vec<i64> = rows
        .iter()
        .map(|r| match r.columns.get(&1) {
            Some(Value::Int64(n)) => *n,
            other => panic!("expected Int64 pk, got {other:?}"),
        })
        .collect();
    pks.sort_unstable();
    let mut expected = expect_pks.to_vec();
    expected.sort_unstable();
    assert_eq!(
        pks, expected,
        "secondary-index trip_id={trip_id} listing mismatch"
    );
}

#[test]
fn update_many_partial_keeps_int64_bitmap_secondary() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("segments", segments_like_schema()).unwrap();

    db.transaction(|tx| {
        tx.put(
            "segments",
            vec![
                (1, Value::Int64(10)),
                (2, Value::Int64(42)),
                (3, Value::Bytes(b"Golden Jade".to_vec())),
            ],
        )?;
        tx.put(
            "segments",
            vec![
                (1, Value::Int64(11)),
                (2, Value::Int64(42)),
                (3, Value::Bytes(b"Outbound".to_vec())),
            ],
        )?;
        tx.put(
            "segments",
            vec![
                (1, Value::Int64(12)),
                (2, Value::Int64(99)),
                (3, Value::Bytes(b"Other trip".to_vec())),
            ],
        )?;
        Ok(())
    })
    .unwrap();

    assert_listed_by_trip(&db, "segments", 42, &[10, 11]);
    assert_listed_by_trip(&db, "segments", 99, &[12]);

    let rid10 = lookup_pk(&db, "segments", 10);

    // Product update path: only the non-indexed title column changes.
    db.transaction(|tx| {
        tx.update_many(
            "segments",
            vec![(
                rid10,
                vec![(3, Value::Bytes(b"Golden Jade Suvarnabhumi".to_vec()))],
            )],
        )?;
        Ok(())
    })
    .unwrap();

    assert_row_by_pk(
        &db,
        "segments",
        10,
        Some(b"Golden Jade Suvarnabhumi"),
    );
    assert_listed_by_trip(&db, "segments", 42, &[10, 11]);
    assert_listed_by_trip(&db, "segments", 99, &[12]);

    // Second amend on the same row (MCP re-auth / amend path).
    let rid10 = lookup_pk(&db, "segments", 10);
    db.transaction(|tx| {
        tx.update_many(
            "segments",
            vec![(
                rid10,
                vec![(3, Value::Bytes(b"Golden Jade Family Quadruple".to_vec()))],
            )],
        )?;
        Ok(())
    })
    .unwrap();

    assert_row_by_pk(
        &db,
        "segments",
        10,
        Some(b"Golden Jade Family Quadruple"),
    );
    assert_listed_by_trip(&db, "segments", 42, &[10, 11]);
}

#[test]
fn delete_then_put_full_row_keeps_int64_bitmap_secondary() {
    // Kit's applyUpdateInTxn is delete(rowId) + put(merged cells). Engine
    // update_many also normalizes to Delete+Put at commit. Exercise that shape
    // explicitly with a full-row put (all columns present, trip_id unchanged).
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("segments", segments_like_schema()).unwrap();

    db.transaction(|tx| {
        tx.put(
            "segments",
            vec![
                (1, Value::Int64(1)),
                (2, Value::Int64(7)),
                (3, Value::Bytes(b"hotel".to_vec())),
            ],
        )?;
        Ok(())
    })
    .unwrap();

    let rid = lookup_pk(&db, "segments", 1);

    db.transaction(|tx| {
        tx.delete("segments", rid)?;
        tx.put(
            "segments",
            vec![
                (1, Value::Int64(1)),
                (2, Value::Int64(7)),
                (3, Value::Bytes(b"hotel updated".to_vec())),
            ],
        )?;
        Ok(())
    })
    .unwrap();

    assert_row_by_pk(&db, "segments", 1, Some(b"hotel updated"));
    assert_listed_by_trip(&db, "segments", 7, &[1]);
}

#[test]
fn update_many_partial_keeps_bytes_bitmap_secondary() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", tagged_schema()).unwrap();

    db.transaction(|tx| {
        tx.put(
            "items",
            vec![
                (1, Value::Int64(1)),
                (2, Value::Bytes(b"alpha".to_vec())),
                (3, Value::Bytes(b"v1".to_vec())),
            ],
        )?;
        tx.put(
            "items",
            vec![
                (1, Value::Int64(2)),
                (2, Value::Bytes(b"beta".to_vec())),
                (3, Value::Bytes(b"v1".to_vec())),
            ],
        )?;
        Ok(())
    })
    .unwrap();

    let rid1 = lookup_pk(&db, "items", 1);
    db.transaction(|tx| {
        tx.update_many(
            "items",
            vec![(rid1, vec![(3, Value::Bytes(b"v2".to_vec()))])],
        )?;
        Ok(())
    })
    .unwrap();

    let handle = db.table("items").unwrap();
    let mut guard = handle.lock();
    let alpha = guard.query(&tag_index_query(b"alpha")).unwrap();
    assert_eq!(alpha.len(), 1);
    assert_eq!(alpha[0].columns.get(&1), Some(&Value::Int64(1)));
    assert_eq!(
        alpha[0].columns.get(&3),
        Some(&Value::Bytes(b"v2".to_vec()))
    );
    let beta = guard.query(&tag_index_query(b"beta")).unwrap();
    assert_eq!(beta.len(), 1);
}

#[test]
fn update_many_partial_survives_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("segments", segments_like_schema()).unwrap();
        db.transaction(|tx| {
            tx.put(
                "segments",
                vec![
                    (1, Value::Int64(5)),
                    (2, Value::Int64(100)),
                    (3, Value::Bytes(b"before".to_vec())),
                ],
            )?;
            Ok(())
        })
        .unwrap();
        let rid = lookup_pk(&db, "segments", 5);
        db.transaction(|tx| {
            tx.update_many(
                "segments",
                vec![(rid, vec![(3, Value::Bytes(b"after".to_vec()))])],
            )?;
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }

    let db = Database::open(dir.path()).unwrap();
    assert_row_by_pk(&db, "segments", 5, Some(b"after"));
    assert_listed_by_trip(&db, "segments", 100, &[5]);
}

#[test]
fn update_many_moving_trip_id_moves_bitmap_membership() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("segments", segments_like_schema()).unwrap();

    db.transaction(|tx| {
        tx.put(
            "segments",
            vec![
                (1, Value::Int64(1)),
                (2, Value::Int64(10)),
                (3, Value::Bytes(b"seg".to_vec())),
            ],
        )?;
        Ok(())
    })
    .unwrap();
    assert_listed_by_trip(&db, "segments", 10, &[1]);
    assert_listed_by_trip(&db, "segments", 20, &[]);

    let rid = lookup_pk(&db, "segments", 1);
    db.transaction(|tx| {
        // Change the indexed column itself — delta must Move, not only Repoint.
        tx.update_many(
            "segments",
            vec![(rid, vec![(2, Value::Int64(20))])],
        )?;
        Ok(())
    })
    .unwrap();

    assert_row_by_pk(&db, "segments", 1, Some(b"seg"));
    assert_listed_by_trip(&db, "segments", 10, &[]);
    assert_listed_by_trip(&db, "segments", 20, &[1]);
}

#[test]
fn title_only_update_does_not_leave_stale_bitmap_row_id() {
    // After PK-replace, the old row id must not remain in the trip_id bitmap
    // for the (unchanged) key — index-delta Repoint removes it.
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("segments", segments_like_schema()).unwrap();

    db.transaction(|tx| {
        tx.put(
            "segments",
            vec![
                (1, Value::Int64(1)),
                (2, Value::Int64(5)),
                (3, Value::Bytes(b"v1".to_vec())),
            ],
        )?;
        Ok(())
    })
    .unwrap();
    let old_rid = lookup_pk(&db, "segments", 1);

    db.transaction(|tx| {
        tx.update_many(
            "segments",
            vec![(old_rid, vec![(3, Value::Bytes(b"v2".to_vec()))])],
        )?;
        Ok(())
    })
    .unwrap();
    let new_rid = lookup_pk(&db, "segments", 1);
    assert_ne!(old_rid, new_rid, "update_many normalizes to delete+put (new rid)");

    {
        let handle = db.table("segments").unwrap();
        let mut guard = handle.lock();
        let rows = guard.query(&trip_index_query(5)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_id, new_rid);
        assert_eq!(
            rows[0].columns.get(&3),
            Some(&Value::Bytes(b"v2".to_vec()))
        );
    }
    // No duplicate / ghost rows for the same trip from the tombstoned rid.
    assert_listed_by_trip(&db, "segments", 5, &[1]);
}
