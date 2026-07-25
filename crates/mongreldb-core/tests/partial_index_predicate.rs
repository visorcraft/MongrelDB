//! Partial-index predicate-transition coverage.
//!
//! Predicate evaluator only supports the `IS NULL` / `IS NOT NULL` patterns
//! (engine.rs:12977). Other SQL WHERE clauses fall through to `true` (the
//! comment calls this "conservative — better to over-index than to miss rows").
//! These tests pin the IS NULL / IS NOT NULL transition behavior across
//! updates, flush, reopen, and `rebuild_indexes`.

use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Database, Value};
use tempfile::tempdir;

fn partial_schema() -> Schema {
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
                name: "tag".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 3,
                name: "deleted_at".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "live_tag_idx".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
            // Only supported predicates per `eval_partial_predicate`.
            predicate: Some("deleted_at IS NULL".into()),
            options: Default::default(),
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn encode_int64(v: i64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

/// Resolve a PK value to its row id via the HOT map. Use this instead of
/// `rids[0]` from `transaction_with_row_ids` because update_many reads
/// through the published read-generation, which may not yet include the
/// row id returned by the just-committed transaction in some paths.
fn lookup_pk(db: &Database, pk: i64) -> mongreldb_core::RowId {
    let handle = db.table("t").unwrap();
    let mut table = handle.lock();
    let rows = table
        .query(&Query::new().and(Condition::Pk(
            Value::Int64(pk).encode_key(),
        )))
        .unwrap();
    assert_eq!(rows.len(), 1, "PK {pk} should be present");
    rows[0].row_id
}

fn count_indexed(db: &Database, tag: i64) -> usize {
    let handle = db.table("t").unwrap();
    let mut table = handle.lock();
    table
        .query(&Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: encode_int64(tag),
        }))
        .unwrap()
        .len()
}

fn put_row(db: &Database, pk: i64, tag: i64, deleted_at: Value) -> mongreldb_core::RowId {
    let (_, rids) = db
        .transaction_with_row_ids(|t| {
            t.put(
                "t",
                vec![
                    (1, Value::Int64(pk)),
                    (2, Value::Int64(tag)),
                    (3, deleted_at),
                ],
            )?;
            Ok(())
        })
        .unwrap();
    rids[0]
}

#[test]
fn live_row_with_null_deleted_at_is_indexed() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", partial_schema()).unwrap();
    put_row(&db, 1, 100, Value::Null);
    assert_eq!(
        count_indexed(&db, 100),
        1,
        "row with deleted_at=NULL must be indexed"
    );
}

#[test]
fn soft_deleted_row_with_non_null_deleted_at_is_not_indexed() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", partial_schema()).unwrap();
    put_row(&db, 1, 100, Value::Int64(1_700_000_000));
    assert_eq!(
        count_indexed(&db, 100),
        0,
        "row with deleted_at set must not be indexed"
    );
}

#[test]
fn predicate_transition_drop_is_reflected_on_update() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", partial_schema()).unwrap();
    let rid = put_row(&db, 1, 200, Value::Null);
    assert_eq!(count_indexed(&db, 200), 1);

    db.transaction(|t| {
        t.update_many(
            "t",
            vec![(rid, vec![(3, Value::Int64(1_700_000_000))])],
        )?;
        Ok(())
    })
    .unwrap();
    assert_eq!(
        count_indexed(&db, 200),
        0,
        "soft-delete transition must drop row from partial index"
    );
}

#[test]
fn predicate_transition_restore_is_reflected_on_update() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", partial_schema()).unwrap();
    let rid = put_row(&db, 1, 300, Value::Int64(1_700_000_000));
    assert_eq!(count_indexed(&db, 300), 0);

    db.transaction(|t| {
        t.update_many("t", vec![(rid, vec![(3, Value::Null)])])?;
        Ok(())
    })
    .unwrap();
    assert_eq!(
        count_indexed(&db, 300),
        1,
        "restore transition must re-index the row"
    );
}

#[test]
fn changing_only_predicate_column_with_unchanged_indexed_value_works() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", partial_schema()).unwrap();
    let mut rid = put_row(&db, 1, 400, Value::Null);

    // update_many normalizes to delete+put, so each iteration produces a new
    // rid. Re-lookup via HOT after every transition.
    for new_value in [Value::Int64(100), Value::Null, Value::Int64(200)] {
        let expected = if matches!(new_value, Value::Null) { 1 } else { 0 };
        db.transaction(|t| {
            t.update_many("t", vec![(rid, vec![(3, new_value.clone())])])?;
            Ok(())
        })
        .unwrap();
        rid = lookup_pk(&db, 1);
        assert_eq!(
            count_indexed(&db, 400),
            expected,
            "after deleted_at={:?}, expected indexed count {}",
            new_value,
            expected
        );
    }
}

#[test]
fn predicate_transition_survives_flush_reopen_and_rebuild() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let db = Database::create(&path).unwrap();
    db.create_table("t", partial_schema()).unwrap();
    let rid = put_row(&db, 1, 500, Value::Null);

    db.transaction(|t| {
        t.update_many(
            "t",
            vec![(rid, vec![(3, Value::Int64(1_700_000_000))])],
        )?;
        Ok(())
    })
    .unwrap();

    db.compact_table("t").unwrap();
    drop(db);

    let db = Database::open(&path).unwrap();
    assert_eq!(
        count_indexed(&db, 500),
        0,
        "soft-delete must survive reopen"
    );
    db.rebuild_indexes("t").unwrap();
    assert_eq!(
        count_indexed(&db, 500),
        0,
        "soft-delete must survive rebuild_indexes"
    );

    drop(db);
    let db = Database::open(&path).unwrap();
    let rid_after = lookup_pk(&db, 1);
    db.transaction(|t| {
        t.update_many("t", vec![(rid_after, vec![(3, Value::Null)])])?;
        Ok(())
    })
    .unwrap();
    drop(db);

    let db = Database::open(&path).unwrap();
    db.rebuild_indexes("t").unwrap();
    assert_eq!(
        count_indexed(&db, 500),
        1,
        "restore must survive rebuild_indexes"
    );
}
