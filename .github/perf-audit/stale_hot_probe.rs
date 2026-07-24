use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Database, Value};
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 77,
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
                name: "payload".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        ..Schema::default()
    }
}

fn lookup(db: &Database, pk: i64) -> Option<mongreldb_core::RowId> {
    let handle = db.table("items").unwrap();
    let guard = handle.lock();
    guard.lookup_pk(&Value::Int64(pk).encode_key())
}

#[test]
fn delete_after_flush_and_partial_update_clears_hot() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", schema()).unwrap();

    db.transaction(|tx| {
        tx.put("items", vec![(1, Value::Int64(3)), (2, Value::Int64(10))])?;
        Ok(())
    })
    .unwrap();
    db.table("items").unwrap().lock().flush().unwrap();

    let old = lookup(&db, 3).expect("inserted PK missing");
    db.transaction(|tx| {
        tx.update_many("items", vec![(old, vec![(2, Value::Int64(20))])])?;
        Ok(())
    })
    .unwrap();

    let replacement = lookup(&db, 3).expect("updated PK missing");
    assert_ne!(old, replacement, "normal-table update allocates a replacement row id");
    db.transaction(|tx| {
        tx.delete("items", replacement)?;
        Ok(())
    })
    .unwrap();

    assert_eq!(lookup(&db, 3), None, "deleted replacement left a stale HOT entry");
}

#[test]
fn simple_delete_after_flush_clears_hot() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    db.transaction(|tx| {
        tx.put("items", vec![(1, Value::Int64(3)), (2, Value::Int64(10))])?;
        Ok(())
    })
    .unwrap();
    db.table("items").unwrap().lock().flush().unwrap();
    let row_id = lookup(&db, 3).unwrap();
    db.transaction(|tx| {
        tx.delete("items", row_id)?;
        Ok(())
    })
    .unwrap();
    assert_eq!(lookup(&db, 3), None);
}
