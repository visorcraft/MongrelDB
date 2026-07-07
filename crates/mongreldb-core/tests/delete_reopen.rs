//! Regression: a tombstone committed before reopen must still hide its row after
//! recovery. Previously the WAL replay stamped the in-memory tombstone with the
//! WAL record's monotonic `seq` (which outpaces the commit epoch), landing it in
//! the MVCC future and making deleted rows reappear after `Table::open`.

use mongreldb_core::{
    schema::{ColumnDef, ColumnFlags, Schema, TypeId},
    RowId, Table, Value,
};
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
                name: "v".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn put(db: &mut Table, id: i64, v: i64) -> RowId {
    db.put(vec![(1, Value::Int64(id)), (2, Value::Int64(v))])
        .unwrap()
}

/// Delete + commit + reopen: the row stays gone and count matches visible_rows.
#[test]
fn delete_survives_reopen_after_commit() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let a = put(&mut db, 1, 10);
    put(&mut db, 2, 20);
    db.commit().unwrap();
    db.delete(a).unwrap();
    db.commit().unwrap();
    assert_eq!(db.count(), 1);
    assert_eq!(db.visible_rows(db.snapshot()).unwrap().len(), 1);

    drop(db);
    let db = Table::open(dir.path()).unwrap();
    assert_eq!(db.count(), 1);
    let visible = db.visible_rows(db.snapshot()).unwrap();
    assert_eq!(visible.len(), 1, "deleted row reappeared after reopen");
    assert_eq!(visible[0].row_id, RowId(1));
    assert!(visible.iter().all(|r| r.row_id != a));
}

/// Delete + flush (spill to a sorted run) + reopen must also keep the row gone.
#[test]
fn delete_survives_reopen_after_flush() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let a = put(&mut db, 1, 10);
    put(&mut db, 2, 20);
    db.commit().unwrap();
    db.delete(a).unwrap();
    db.flush().unwrap();

    drop(db);
    let db = Table::open(dir.path()).unwrap();
    assert_eq!(db.count(), 1);
    let visible = db.visible_rows(db.snapshot()).unwrap();
    assert_eq!(
        visible.len(),
        1,
        "deleted row reappeared after flush + reopen"
    );
}

/// Upsert-by-replace (lookup_pk + delete + put) must not leave ghost duplicates
/// across reopen — the load-bearing pattern for a CRUD registry.
#[test]
fn upsert_by_replace_survives_reopen() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    put(&mut db, 1, 10);
    db.commit().unwrap();

    let new_rid = {
        if let Some(old) = db.lookup_pk(&1i64.to_be_bytes()) {
            db.delete(old).unwrap();
        }
        put(&mut db, 1, 90)
    };
    db.commit().unwrap();
    assert_eq!(db.count(), 1);
    assert_eq!(db.visible_rows(db.snapshot()).unwrap().len(), 1);

    drop(db);
    let db = Table::open(dir.path()).unwrap();
    assert_eq!(db.count(), 1, "ghost duplicate after reopen");
    let visible = db.visible_rows(db.snapshot()).unwrap();
    assert_eq!(visible.len(), 1, "more than one live row for the same PK");
    assert_eq!(visible[0].row_id, new_rid);
}
