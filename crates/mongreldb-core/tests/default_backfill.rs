use mongreldb_core::{
    AlterColumn, ColumnDef, ColumnFlags, Database, DefaultExpr, Schema, TypeId, Value,
};
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 0,
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
                name: "score".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
            },
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

#[test]
fn set_not_null_backfills_declared_default_and_survives_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("items", schema()).unwrap();
        db.transaction(|transaction| {
            transaction.put("items", vec![(1, Value::Int64(1))])?;
            transaction.put("items", vec![(1, Value::Int64(2)), (2, Value::Null)])?;
            transaction.put("items", vec![(1, Value::Int64(3)), (2, Value::Int64(9))])?;
            Ok(())
        })
        .unwrap();
        db.alter_column(
            "items",
            "score",
            AlterColumn::set_default(DefaultExpr::Static(Value::Int64(7))),
        )
        .unwrap();
        let flags = db
            .table("items")
            .unwrap()
            .lock()
            .schema()
            .column("score")
            .unwrap()
            .flags
            .without(ColumnFlags::NULLABLE);
        db.alter_column("items", "score", AlterColumn::set_flags(flags))
            .unwrap();
    }

    let db = Database::open(dir.path()).unwrap();
    let mut values: Vec<i64> = db
        .table("items")
        .unwrap()
        .lock()
        .visible_rows(db.snapshot().0)
        .unwrap()
        .into_iter()
        .map(|row| match row.columns.get(&2) {
            Some(Value::Int64(value)) => *value,
            other => panic!("unexpected backfill value: {other:?}"),
        })
        .collect();
    values.sort_unstable();
    assert_eq!(values, vec![7, 7, 9]);
    assert!(!db
        .table("items")
        .unwrap()
        .lock()
        .schema()
        .column("score")
        .unwrap()
        .flags
        .contains(ColumnFlags::NULLABLE));
}

#[test]
fn set_not_null_without_default_still_rejects_existing_nulls() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    db.transaction(|transaction| {
        transaction.put("items", vec![(1, Value::Int64(1))])?;
        Ok(())
    })
    .unwrap();
    let flags = ColumnFlags::empty();
    assert!(db
        .alter_column("items", "score", AlterColumn::set_flags(flags))
        .is_err());
}
