use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{write_replica_epoch, Database, MongrelError, Snapshot, Value};

fn schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
        }],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn put(db: &Database, id: i64) {
    db.transaction(|txn| {
        txn.put("items", vec![(1, Value::Int64(id))])?;
        Ok(())
    })
    .unwrap();
}

#[test]
fn bootstrap_incremental_apply_and_read_only_enforcement() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");
    let leader = Database::create(&leader_path).unwrap();
    leader.create_table("items", schema()).unwrap();
    put(&leader, 1);

    let snapshot = leader.replication_snapshot().unwrap();
    snapshot.install(&follower_path).unwrap();
    assert_eq!(
        mongreldb_core::replica_epoch(&follower_path).unwrap(),
        snapshot.epoch()
    );

    let follower = Database::open(&follower_path).unwrap();
    assert!(follower.is_read_only_replica());
    assert!(matches!(
        follower.create_table("blocked", schema()),
        Err(MongrelError::ReadOnlyReplica)
    ));
    let table = follower.table("items").unwrap();
    assert!(matches!(
        table.lock().put(vec![(1, Value::Int64(9))]),
        Err(MongrelError::ReadOnlyReplica)
    ));
    drop(follower);

    put(&leader, 2);
    leader.create_table("extra", schema()).unwrap();
    leader
        .transaction(|txn| {
            txn.put("extra", vec![(1, Value::Int64(7))])?;
            Ok(())
        })
        .unwrap();
    let batch = leader.replication_batch_since(snapshot.epoch()).unwrap();
    assert!(!batch.requires_snapshot);
    assert!(!batch.records.is_empty());

    let follower = Database::open(&follower_path).unwrap();
    let applied_epoch = follower.append_replication_batch(&batch.records).unwrap();
    let durable_records = mongreldb_core::SharedWal::replay(&follower_path)
        .unwrap()
        .len();
    assert_eq!(
        follower.append_replication_batch(&batch.records).unwrap(),
        applied_epoch
    );
    assert_eq!(
        mongreldb_core::SharedWal::replay(&follower_path)
            .unwrap()
            .len(),
        durable_records,
        "retry must not duplicate a durable remote commit"
    );
    drop(follower);

    let follower = Database::open(&follower_path).unwrap();
    assert!(follower.visible_epoch().0 >= applied_epoch);
    let table = follower.table("items").unwrap();
    let rows = table
        .lock()
        .visible_rows(Snapshot::at(follower.visible_epoch()))
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(table.lock().count(), 2);
    assert_eq!(follower.table("extra").unwrap().lock().count(), 1);
    drop(follower);
    write_replica_epoch(&follower_path, applied_epoch).unwrap();
}

#[test]
fn retention_gap_and_spilled_run_require_bootstrap() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    let before = db.visible_epoch().0;
    db.set_spill_threshold(1);
    put(&db, 1);
    assert!(
        db.replication_batch_since(before)
            .unwrap()
            .requires_snapshot
    );

    db.checkpoint().unwrap();
    assert!(db.replication_batch_since(0).unwrap().requires_snapshot);
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_replica_bootstrap_and_incremental_apply() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("encrypted-leader");
    let follower_path = dir.path().join("encrypted-follower");
    let leader = Database::create_encrypted(&leader_path, "secret").unwrap();
    leader.create_table("items", schema()).unwrap();
    put(&leader, 1);
    let snapshot = leader.replication_snapshot().unwrap();
    snapshot.install(&follower_path).unwrap();

    put(&leader, 2);
    let batch = leader.replication_batch_since(snapshot.epoch()).unwrap();
    let follower = Database::open_encrypted(&follower_path, "secret").unwrap();
    let epoch = follower.append_replication_batch(&batch.records).unwrap();
    drop(follower);
    let follower = Database::open_encrypted(&follower_path, "secret").unwrap();
    assert!(follower.visible_epoch().0 >= epoch);
    assert_eq!(follower.table("items").unwrap().lock().count(), 2);
}

#[test]
fn authenticated_replica_reopens_with_copied_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("auth-leader");
    let follower_path = dir.path().join("auth-follower");
    let leader = Database::create_with_credentials(&leader_path, "admin", "pw").unwrap();
    leader.create_table("items", schema()).unwrap();
    put(&leader, 1);
    leader
        .replication_snapshot()
        .unwrap()
        .install(&follower_path)
        .unwrap();

    assert!(matches!(
        Database::open(&follower_path),
        Err(MongrelError::AuthRequired)
    ));
    let follower = Database::open_with_credentials(&follower_path, "admin", "pw").unwrap();
    assert_eq!(follower.table("items").unwrap().lock().count(), 1);
}
