//! P2.5 — atomic cross-table transactions on the shared WAL.

use mongreldb_core::{schema::*, Database, MongrelError, RowId, Value};
use tempfile::tempdir;

fn one_int_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "v".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

#[test]
fn cross_table_txn_is_all_or_nothing() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", one_int_schema()).unwrap();
    db.create_table("b", one_int_schema()).unwrap();

    db.transaction(|t| {
        t.put("a", vec![(1, Value::Int64(1))])?;
        t.put("b", vec![(1, Value::Int64(2))])?;
        Ok(())
    })
    .unwrap();

    assert_eq!(db.table("a").unwrap().lock().count(), 1);
    assert_eq!(db.table("b").unwrap().lock().count(), 1);

    // A rolled-back txn writes nothing.
    let _: Result<(), _> = db.transaction(|t| {
        t.put("a", vec![(1, Value::Int64(9))])?;
        Err(MongrelError::Other("boom".into()))
    });
    assert_eq!(db.table("a").unwrap().lock().count(), 1);
}

#[test]
fn txn_delete_and_put_in_one_commit() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", one_int_schema()).unwrap();
    db.transaction(|t| {
        t.put("a", vec![(1, Value::Int64(1))])?;
        t.put("a", vec![(1, Value::Int64(2))])?;
        Ok(())
    })
    .unwrap();
    assert_eq!(db.table("a").unwrap().lock().count(), 2);
    // delete the first row (RowId(0), the first allocated by a fresh table).
    db.transaction(|t| {
        t.delete("a", RowId(0))?;
        Ok(())
    })
    .unwrap();
    assert_eq!(db.table("a").unwrap().lock().count(), 1);
}
