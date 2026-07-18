use mongreldb_core::catalog;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Database, MaterializedViewEntry, MongrelError, Value};
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
            embedding_source: None,
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

#[test]
fn building_table_is_hidden_until_atomic_publish() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let build = "__mongreldb_ctas_build_query-1";
    db.create_building_table(build, "target", "query-1", schema())
        .unwrap();

    assert!(db.table_names().is_empty());
    assert!(db.table(build).is_err());
    assert!(db.table("target").is_err());

    let mut txn = db.begin();
    txn.put_building(build, vec![(1, Value::Int64(7))]).unwrap();
    txn.commit().unwrap();
    assert!(db.table_names().is_empty());

    let epoch = db.publish_building_table(build, "target").unwrap();
    assert!(epoch.0 > 0);
    assert_eq!(db.table_names(), vec!["target"]);
    assert_eq!(db.table("target").unwrap().lock().count(), 1);
}

#[test]
fn abandoned_build_is_reclaimed_on_reopen() {
    let dir = tempdir().unwrap();
    let build = "__mongreldb_ctas_build_query-2";
    let table_id = {
        let db = Database::create(dir.path()).unwrap();
        let table_id = db
            .create_building_table(build, "target", "query-2", schema())
            .unwrap();
        let mut txn = db.begin();
        txn.put_building(build, vec![(1, Value::Int64(7))]).unwrap();
        txn.commit().unwrap();
        table_id
    };
    let table_dir = dir.path().join("tables").join(table_id.to_string());
    assert!(table_dir.exists());

    let reopened = Database::open(dir.path()).unwrap();
    assert!(reopened.table_names().is_empty());
    assert!(reopened.table(build).is_err());
    assert!(reopened.table("target").is_err());
    assert!(!table_dir.exists());
}

#[test]
fn dropped_build_marker_retries_directory_cleanup_on_reopen() {
    let dir = tempdir().unwrap();
    let build = "__mongreldb_ctas_build_cleanup-retry";
    let table_id = {
        let db = Database::create(dir.path()).unwrap();
        db.create_building_table(build, "target", "cleanup-retry", schema())
            .unwrap()
    };
    let table_dir = dir.path().join("tables").join(table_id.to_string());
    let mut stored = catalog::read(dir.path(), None).unwrap().unwrap();
    let dropped_epoch = stored.db_epoch;
    stored
        .tables
        .iter_mut()
        .find(|entry| entry.table_id == table_id)
        .unwrap()
        .state = catalog::TableState::Dropped {
        at_epoch: dropped_epoch,
    };
    catalog::write_atomic(dir.path(), &stored, None).unwrap();

    Database::open(dir.path()).unwrap();
    assert!(!table_dir.exists());
}

#[test]
fn unreferenced_pre_ddl_table_directory_is_reclaimed_before_id_reuse() {
    let dir = tempdir().unwrap();
    {
        let _db = Database::create(dir.path()).unwrap();
    }
    let orphan = dir.path().join("tables").join("0");
    std::fs::create_dir_all(&orphan).unwrap();
    std::fs::write(orphan.join("partial"), b"crash before DDL WAL").unwrap();

    let reopened = Database::open(dir.path()).unwrap();
    assert!(!orphan.exists());
    assert_eq!(reopened.create_table("target", schema()).unwrap(), 0);
    assert!(reopened.table("target").is_ok());
}

#[test]
fn stale_next_table_id_is_repaired_above_live_catalog_ids() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        assert_eq!(db.create_table("first", schema()).unwrap(), 0);
    }
    let mut stored = catalog::read(dir.path(), None).unwrap().unwrap();
    stored.next_table_id = 0;
    catalog::write_atomic(dir.path(), &stored, None).unwrap();

    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.create_table("second", schema()).unwrap(), 1);
}

#[test]
fn exhausted_table_id_space_fails_without_filesystem_mutation() {
    let dir = tempdir().unwrap();
    {
        let _db = Database::create(dir.path()).unwrap();
    }
    let mut stored = catalog::read(dir.path(), None).unwrap().unwrap();
    stored.next_table_id = u64::MAX;
    catalog::write_atomic(dir.path(), &stored, None).unwrap();

    let reopened = Database::open(dir.path()).unwrap();
    let error = reopened.create_table("never", schema()).unwrap_err();
    assert!(error.to_string().contains("table id space exhausted"));
    assert!(!dir
        .path()
        .join("tables")
        .join(u64::MAX.to_string())
        .exists());
}

#[test]
fn published_build_survives_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        let build = "__mongreldb_ctas_build_query-3";
        db.create_building_table(build, "target", "query-3", schema())
            .unwrap();
        let mut txn = db.begin();
        txn.put_building(build, vec![(1, Value::Int64(7))]).unwrap();
        txn.commit().unwrap();
        db.publish_building_table(build, "target").unwrap();
    }

    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.table_names(), vec!["target"]);
    assert_eq!(reopened.table("target").unwrap().lock().count(), 1);
}

#[test]
fn building_table_reserves_its_intended_name() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let first = "__mongreldb_ctas_build_query-4";
    let second = "__mongreldb_ctas_build_query-5";
    db.create_building_table(first, "target", "query-4", schema())
        .unwrap();

    assert!(matches!(
        db.create_building_table(second, "target", "query-5", schema()),
        Err(MongrelError::InvalidArgument(_))
    ));
    assert!(matches!(
        db.create_table("target", schema()),
        Err(MongrelError::InvalidArgument(_))
    ));

    db.discard_building_table(first).unwrap();
    db.create_building_table(second, "target", "query-5", schema())
        .unwrap();
}

#[test]
fn building_table_rejects_duplicate_primary_keys() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let build = "__mongreldb_ctas_build_query-6";
    db.create_building_table(build, "target", "query-6", schema())
        .unwrap();

    let mut first = db.begin();
    first
        .put_building(build, vec![(1, Value::Int64(7))])
        .unwrap();
    assert!(matches!(
        first.put_building(build, vec![(1, Value::Int64(7))]),
        Err(MongrelError::InvalidArgument(_))
    ));
    first.commit().unwrap();

    let mut second = db.begin();
    assert!(matches!(
        second.put_building(build, vec![(1, Value::Int64(7))]),
        Err(MongrelError::InvalidArgument(_))
    ));
}

#[test]
fn controlled_publish_rejection_never_exposes_building_table() {
    let dir = tempdir().unwrap();
    let build = "__mongreldb_ctas_build_query-7";
    let db = Database::create(dir.path()).unwrap();
    db.create_building_table(build, "target", "query-7", schema())
        .unwrap();
    let mut txn = db.begin();
    txn.put_building(build, vec![(1, Value::Int64(7))]).unwrap();
    txn.commit().unwrap();
    let mut callbacks = 0;

    let error = db
        .publish_building_table_controlled(build, "target", || {
            callbacks += 1;
            Err(MongrelError::Other("cancelled before commit".into()))
        })
        .unwrap_err();
    assert_eq!(callbacks, 1);
    assert!(error.to_string().contains("cancelled before commit"));
    assert!(db.table_names().is_empty());
    assert!(db.table("target").is_err());

    drop(db);
    let reopened = Database::open(dir.path()).unwrap();
    assert!(reopened.table_names().is_empty());
    assert!(reopened.table("target").is_err());
}

#[test]
fn materialized_rebuild_publishes_table_and_definition_together() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("target", schema()).unwrap();
    db.transaction(|transaction| {
        transaction.put("target", vec![(1, Value::Int64(1))])?;
        Ok(())
    })
    .unwrap();
    db.set_materialized_view(MaterializedViewEntry {
        name: "target".into(),
        query: "SELECT 1".into(),
        last_refresh_epoch: 0,
        incremental: None,
    })
    .unwrap();

    let build = "__mongreldb_ctas_build_mv-rebuild";
    db.create_rebuilding_table(build, "target", "mv-rebuild", schema())
        .unwrap();
    let mut transaction = db.begin();
    transaction
        .put_building(build, vec![(1, Value::Int64(2))])
        .unwrap();
    transaction.commit().unwrap();
    assert_eq!(db.table("target").unwrap().lock().count(), 1);
    assert_eq!(db.materialized_view("target").unwrap().query, "SELECT 1");

    let replacement = MaterializedViewEntry {
        name: "target".into(),
        query: "SELECT 2".into(),
        last_refresh_epoch: 0,
        incremental: None,
    };
    let before_rejection = db.visible_epoch();
    let error = db
        .publish_materialized_rebuilding_table_controlled(
            build,
            "target",
            replacement.clone(),
            || Err(MongrelError::Cancelled),
        )
        .unwrap_err();
    assert!(matches!(error, MongrelError::Cancelled));
    assert_eq!(db.visible_epoch(), before_rejection);
    assert_eq!(db.materialized_view("target").unwrap().query, "SELECT 1");
    assert_eq!(db.table("target").unwrap().lock().count(), 1);

    let epoch = db
        .publish_materialized_rebuilding_table_controlled(build, "target", replacement, || Ok(()))
        .unwrap();
    let definition = db.materialized_view("target").unwrap();
    assert_eq!(definition.query, "SELECT 2");
    assert_eq!(definition.last_refresh_epoch, epoch.0);
    assert_eq!(db.table("target").unwrap().lock().count(), 1);

    drop(db);
    let reopened = Database::open(dir.path()).unwrap();
    let definition = reopened.materialized_view("target").unwrap();
    assert_eq!(definition.query, "SELECT 2");
    assert_eq!(definition.last_refresh_epoch, epoch.0);
    assert_eq!(reopened.table("target").unwrap().lock().count(), 1);
}
