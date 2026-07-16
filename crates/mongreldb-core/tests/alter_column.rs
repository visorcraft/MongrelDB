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
                default_value: None,
            },
            ColumnDef {
                id: 2,
                name: "v".into(),
                ty: TypeId::Bytes,
                flags: v_flags,
                default_value: None,
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
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

#[test]
fn invalid_default_is_rejected_before_alter_wal_commit() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", schema(false)).unwrap();
    let initial_epoch = db.visible_epoch();
    let initial_schema_id = db.table("t").unwrap().lock().schema().schema_id;

    let error = db
        .alter_column(
            "t",
            "id",
            AlterColumn::set_default(DefaultExpr::Static(Value::Bool(true))),
        )
        .unwrap_err();
    assert!(matches!(error, MongrelError::Schema(_)));
    assert_eq!(db.visible_epoch(), initial_epoch);
    let table_handle = db.table("t").unwrap();
    let table = table_handle.lock();
    assert_eq!(table.schema().schema_id, initial_schema_id);
    assert!(table.schema().column("id").unwrap().default_value.is_none());
    drop(table);
    drop(table_handle);
    drop(db);

    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.visible_epoch(), initial_epoch);
    let table = reopened.table("t").unwrap();
    let table = table.lock();
    assert_eq!(table.schema().schema_id, initial_schema_id);
    assert!(table.schema().column("id").unwrap().default_value.is_none());
}

#[test]
fn standalone_schema_publish_failure_restores_live_schema() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(false), 1).unwrap();
    let schema_path = dir.path().join("schema.json");
    let saved_schema_path = dir.path().join("schema.json.saved");
    std::fs::rename(&schema_path, &saved_schema_path).unwrap();
    std::fs::create_dir(&schema_path).unwrap();

    let error = table
        .add_column("extra", TypeId::Int64, ColumnFlags::empty(), None)
        .unwrap_err();

    assert!(!matches!(
        error,
        MongrelError::DurableCommit { .. } | MongrelError::CommitOutcomeUnknown { .. }
    ));
    assert!(table.schema().column("extra").is_none());
    std::fs::remove_dir(&schema_path).unwrap();
    std::fs::rename(&saved_schema_path, &schema_path).unwrap();
    drop(table);

    let reopened = Table::open(dir.path()).unwrap();
    assert!(reopened.schema().column("extra").is_none());
}

#[test]
fn standalone_manifest_failure_reports_durable_schema_and_poison() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(false), 1).unwrap();
    let manifest_path = dir.path().join("_mf");
    let saved_manifest_path = dir.path().join("_mf.saved");
    std::fs::rename(&manifest_path, &saved_manifest_path).unwrap();
    std::fs::create_dir(&manifest_path).unwrap();

    let error = table
        .add_column("extra", TypeId::Int64, ColumnFlags::empty(), None)
        .unwrap_err();

    assert!(matches!(error, MongrelError::DurableCommit { .. }));
    assert!(table.schema().column("extra").is_some());
    assert!(table
        .put(vec![(1, Value::Int64(2)), (2, Value::Bytes(b"b".to_vec()))])
        .is_err());
    std::fs::remove_dir(&manifest_path).unwrap();
    std::fs::rename(&saved_manifest_path, &manifest_path).unwrap();
    drop(table);

    let reopened = Table::open(dir.path()).unwrap();
    assert!(reopened.schema().column("extra").is_some());
}

#[test]
fn alter_recovery_checkpoints_schema_and_manifest_together() {
    use mongreldb_core::{catalog, manifest};

    let dir = tempdir().unwrap();
    let (stale_catalog, table_id) = {
        let db = Database::create(dir.path()).unwrap();
        let table_id = db.create_table("t", schema(false)).unwrap();
        let stale_catalog = db.catalog_snapshot();
        db.alter_column("t", "v", AlterColumn::rename("value"))
            .unwrap();
        (stale_catalog, table_id)
    };

    catalog::write_atomic(dir.path(), &stale_catalog, None).unwrap();
    let table_dir = dir.path().join("tables").join(table_id.to_string());
    let stale_schema = stale_catalog.live("t").unwrap().schema.clone();
    std::fs::write(
        table_dir.join("schema.json"),
        serde_json::to_vec_pretty(&stale_schema).unwrap(),
    )
    .unwrap();
    let mut stale_manifest = manifest::read(&table_dir, None).unwrap();
    stale_manifest.schema_id = stale_schema.schema_id;
    manifest::write_atomic(&table_dir, &mut stale_manifest, None).unwrap();

    let db = Database::open(dir.path()).unwrap();
    let recovered_schema = db.table("t").unwrap().lock().schema().clone();
    assert!(recovered_schema.column("v").is_none());
    assert!(recovered_schema.column("value").is_some());
    let recovered_manifest = manifest::read(&table_dir, None).unwrap();
    assert_eq!(recovered_manifest.schema_id, recovered_schema.schema_id);
}
