use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Database, ExecutionControl, MongrelError, Table, Value};
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
            embedding_source: None,
        }],
        ..Schema::default()
    }
}

#[test]
fn standalone_put_is_committed_and_visible_by_flush() {
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    table.put(vec![(1, Value::Int64(7))]).unwrap();
    assert!(table.has_pending_writes());
    assert!(table.visible_rows(table.snapshot()).unwrap().is_empty());

    let (epoch, committed) = table.flush_with_outcome().unwrap();
    assert!(committed);
    assert_eq!(epoch, table.current_epoch());
    assert!(!table.has_pending_writes());
    let rows = table.visible_rows(table.snapshot()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(7)));
}

#[test]
fn standalone_noop_flush_preserves_epoch_and_generation() {
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    table.put(vec![(1, Value::Int64(7))]).unwrap();
    table.force_flush().unwrap();
    let epoch = table.current_epoch();
    let generation = table.data_generation();

    let (returned_epoch, committed) = table.flush_with_outcome().unwrap();
    assert!(!committed);
    assert_eq!(returned_epoch, epoch);
    assert_eq!(table.current_epoch(), epoch);
    assert_eq!(table.data_generation(), generation);
    assert_eq!(table.visible_rows(table.snapshot()).unwrap().len(), 1);
}

#[test]
fn standalone_manifest_failure_reports_durable_epoch_and_requires_reopen() {
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    table.put(vec![(1, Value::Int64(7))]).unwrap();
    let manifest = directory.path().join("_mf");
    let saved_manifest = directory.path().join("_mf.saved");
    std::fs::rename(&manifest, &saved_manifest).unwrap();
    std::fs::create_dir(&manifest).unwrap();

    let error = table.commit().unwrap_err();
    let epoch = match error {
        MongrelError::DurableCommit { epoch, .. } => epoch,
        other => panic!("expected durable commit error, got {other:?}"),
    };
    assert_eq!(table.current_epoch().0, epoch);
    assert_eq!(table.visible_rows(table.snapshot()).unwrap().len(), 1);
    assert!(table
        .put(vec![(1, Value::Int64(8))])
        .unwrap_err()
        .to_string()
        .contains("reopen required"));

    drop(table);
    std::fs::remove_dir(&manifest).unwrap();
    std::fs::rename(&saved_manifest, &manifest).unwrap();
    let reopened = Table::open(directory.path()).unwrap();
    assert_eq!(reopened.current_epoch().0, epoch);
    assert_eq!(reopened.visible_rows(reopened.snapshot()).unwrap().len(), 1);
}

#[test]
fn mounted_manifest_failure_reports_durable_epoch_and_poisons_database() {
    let directory = tempdir().unwrap();
    let db = Database::create(directory.path()).unwrap();
    let table_id = db.create_table("items", schema()).unwrap();
    let table = db.table("items").unwrap();
    table.lock().put(vec![(1, Value::Int64(7))]).unwrap();
    let manifest = directory
        .path()
        .join("tables")
        .join(table_id.to_string())
        .join("_mf");
    let saved_manifest = manifest.with_extension("saved");
    std::fs::rename(&manifest, &saved_manifest).unwrap();
    std::fs::create_dir(&manifest).unwrap();

    let error = table.lock().commit().unwrap_err();
    let epoch = match error {
        MongrelError::DurableCommit { epoch, .. } => epoch,
        other => panic!("expected durable commit error, got {other:?}"),
    };
    assert_eq!(db.visible_epoch().0, epoch);
    assert_eq!(table.lock().count(), 1);
    assert!(db
        .create_table("blocked", schema())
        .unwrap_err()
        .to_string()
        .contains("database poisoned"));

    drop(table);
    drop(db);
    std::fs::remove_dir(&manifest).unwrap();
    std::fs::rename(&saved_manifest, &manifest).unwrap();
    let reopened = Database::open(directory.path()).unwrap();
    assert_eq!(reopened.visible_epoch().0, epoch);
    assert_eq!(reopened.table("items").unwrap().lock().count(), 1);
}

#[test]
fn controlled_flush_rejection_leaves_no_durable_row() {
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    table.put(vec![(1, Value::Int64(7))]).unwrap();
    let control = ExecutionControl::new(None);
    let mut callbacks = 0;

    let error = table
        .flush_with_outcome_controlled(&control, || {
            callbacks += 1;
            Err(MongrelError::Other("cancelled before commit".into()))
        })
        .unwrap_err();
    assert_eq!(callbacks, 1);
    assert!(error.to_string().contains("cancelled before commit"));
    assert!(table.visible_rows(table.snapshot()).unwrap().is_empty());

    let (epoch, committed) = table.flush_with_outcome().unwrap();
    assert!(committed);
    assert_eq!(table.current_epoch(), epoch);
    assert_eq!(table.visible_rows(table.snapshot()).unwrap().len(), 1);

    drop(table);
    let reopened = Table::open(directory.path()).unwrap();
    assert_eq!(reopened.visible_rows(reopened.snapshot()).unwrap().len(), 1);
}

#[test]
fn standalone_reopen_at_max_txn_id_fails_before_wal_mutation() {
    use mongreldb_core::wal::{Op, Wal};
    use mongreldb_core::Epoch;

    let directory = tempdir().unwrap();
    drop(Table::create(directory.path(), schema(), 1).unwrap());
    let segment = directory.path().join("_wal/seg-000000.wal");
    std::fs::remove_file(&segment).unwrap();
    let mut wal = Wal::create(&segment, Epoch(0)).unwrap();
    wal.append_txn(
        u64::MAX,
        Op::TxnCommit {
            epoch: 1,
            added_runs: Vec::new(),
        },
    )
    .unwrap();
    wal.sync().unwrap();
    drop(wal);

    let mut table = Table::open(directory.path()).unwrap();
    let active = directory.path().join("_wal/seg-000001.wal");
    let before = std::fs::read(&active).unwrap();
    assert!(matches!(table.commit(), Err(MongrelError::Full(_))));
    assert_eq!(std::fs::read(active).unwrap(), before);
}
