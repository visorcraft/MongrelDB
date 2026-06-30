use mongreldb_core::{
    ColumnDef, ColumnFlags, Database, MongrelError, RowId, Schema, Table, TypeId, Value,
};
use tempfile::tempdir;

fn users_schema() -> Schema {
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
                name: "name".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![],
    }
}

fn row(id: i64, name: &[u8]) -> Vec<(u16, Value)> {
    vec![(1, Value::Int64(id)), (2, Value::Bytes(name.to_vec()))]
}

fn assert_conflict(err: MongrelError) {
    assert!(
        matches!(err, MongrelError::Conflict(_)),
        "expected conflict, got {err:?}"
    );
}

#[test]
fn transaction_truncate_survives_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("users", users_schema()).unwrap();
        db.transaction(|tx| {
            tx.put("users", row(1, b"alice"))?;
            tx.put("users", row(2, b"bob"))?;
            Ok(())
        })
        .unwrap();
        assert_eq!(db.table("users").unwrap().lock().count(), 2);

        db.transaction(|tx| tx.truncate("users")).unwrap();
        assert_eq!(db.table("users").unwrap().lock().count(), 0);
    }

    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.table("users").unwrap().lock().count(), 0);
}

#[test]
fn table_truncate_survives_reopen() {
    let dir = tempdir().unwrap();
    {
        let table_dir = dir.path().join("table");
        let mut table = Table::create(&table_dir, users_schema(), 1).unwrap();
        table.put(row(1, b"alice")).unwrap();
        table.put(row(2, b"bob")).unwrap();
        table.commit().unwrap();
        assert_eq!(table.count(), 2);

        table.truncate().unwrap();
        table.commit().unwrap();
        assert_eq!(table.count(), 0);
    }

    let table = Table::open(dir.path().join("table")).unwrap();
    assert_eq!(table.count(), 0);
}

#[test]
fn truncate_conflicts_with_concurrent_put_when_truncate_wins() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema()).unwrap();
    db.transaction(|tx| {
        tx.put("users", row(1, b"alice"))?;
        Ok(())
    })
    .unwrap();

    let mut truncate = db.begin();
    let mut put = db.begin();
    truncate.truncate("users").unwrap();
    put.put("users", row(2, b"bob")).unwrap();

    truncate.commit().unwrap();
    assert_conflict(put.commit().unwrap_err());
}

#[test]
fn truncate_conflicts_with_concurrent_put_when_put_wins() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema()).unwrap();
    db.transaction(|tx| {
        tx.put("users", row(1, b"alice"))?;
        Ok(())
    })
    .unwrap();

    let mut truncate = db.begin();
    let mut put = db.begin();
    truncate.truncate("users").unwrap();
    put.put("users", row(2, b"bob")).unwrap();

    put.commit().unwrap();
    assert_conflict(truncate.commit().unwrap_err());
}

#[test]
fn transaction_truncate_rejects_same_table_writes() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema()).unwrap();

    let mut tx = db.begin();
    tx.put("users", row(1, b"alice")).unwrap();
    assert!(tx.truncate("users").is_err());

    let mut tx = db.begin();
    tx.truncate("users").unwrap();
    assert!(tx.delete("users", RowId(1)).is_err());
}
