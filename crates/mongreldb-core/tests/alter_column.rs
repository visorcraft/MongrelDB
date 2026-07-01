use mongreldb_core::schema::*;
use mongreldb_core::{AlterColumn, Database, MongrelError, Table, Value};
use tempfile::tempdir;

fn schema(nullable_v: bool) -> Schema {
    let mut v_flags = ColumnFlags::empty();
    if nullable_v {
        v_flags = v_flags.with(ColumnFlags::NULLABLE);
    }
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
                ty: TypeId::Bytes,
                flags: v_flags,
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
    }
}

#[test]
fn table_alter_column_renames_and_persists() {
    let dir = tempdir().unwrap();
    {
        let mut table = Table::create(dir.path(), schema(false), 1).unwrap();
        table
            .put(vec![(1, Value::Int64(1)), (2, Value::Bytes(b"a".to_vec()))])
            .unwrap();
        table.commit().unwrap();

        let altered = table
            .alter_column("v", AlterColumn::rename("value"))
            .unwrap();
        assert_eq!(altered.id, 2);
        assert_eq!(altered.name, "value");
        assert!(table.schema().column("v").is_none());
        assert_eq!(table.schema().column("value").unwrap().id, 2);
    }

    let reopened = Table::open(dir.path()).unwrap();
    assert_eq!(reopened.schema().column("value").unwrap().id, 2);
    assert_eq!(reopened.count(), 1);
}

#[test]
fn database_alter_column_updates_catalog_and_reopens() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("t", schema(false)).unwrap();
        db.transaction(|tx| {
            tx.put(
                "t",
                vec![(1, Value::Int64(1)), (2, Value::Bytes(b"a".to_vec()))],
            )
        })
        .unwrap();

        db.alter_column("t", "v", AlterColumn::rename("value"))
            .unwrap();
        assert_eq!(
            db.table("t")
                .unwrap()
                .lock()
                .schema()
                .column("value")
                .unwrap()
                .id,
            2
        );
        assert_eq!(
            db.catalog_snapshot().live("t").unwrap().schema.columns[1].name,
            "value"
        );
    }

    let reopened = Database::open(dir.path()).unwrap();
    let table = reopened.table("t").unwrap();
    let table = table.lock();
    assert_eq!(table.schema().column("value").unwrap().id, 2);
    assert_eq!(table.count(), 1);
}

#[test]
fn set_not_null_rejects_existing_nulls() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", schema(true)).unwrap();
    db.transaction(|tx| tx.put("t", vec![(1, Value::Int64(1))]))
        .unwrap();

    let flags = db
        .table("t")
        .unwrap()
        .lock()
        .schema()
        .column("v")
        .unwrap()
        .flags
        .without(ColumnFlags::NULLABLE);
    let err = db
        .alter_column("t", "v", AlterColumn::set_flags(flags))
        .unwrap_err();
    assert!(
        matches!(err, MongrelError::InvalidArgument(_)),
        "expected InvalidArgument, got {err:?}"
    );
}

#[test]
fn type_change_on_non_empty_incompatible_column_is_rejected() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", schema(false)).unwrap();
    db.transaction(|tx| {
        tx.put(
            "t",
            vec![(1, Value::Int64(1)), (2, Value::Bytes(b"a".to_vec()))],
        )
    })
    .unwrap();

    let err = db
        .alter_column("t", "v", AlterColumn::set_type(TypeId::Int64))
        .unwrap_err();
    assert!(
        matches!(err, MongrelError::Schema(_)),
        "expected Schema error, got {err:?}"
    );
}
