use mongreldb_core::manifest;
use mongreldb_core::{ColumnDef, ColumnFlags, Database, Schema, Table, TypeId, Value};
use std::time::{SystemTime, UNIX_EPOCH};
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
                name: "created_at".into(),
                ty: TypeId::TimestampNanos,
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

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

#[test]
fn ttl_hides_rows_persists_and_compaction_reclaims() {
    let dir = tempdir().unwrap();
    let now = now_nanos();
    let (expired_id, live_id, null_id) = {
        let mut table = Table::create(dir.path(), schema(), 1).unwrap();
        table.set_mutable_run_spill_bytes(1);
        table.set_ttl("created_at", 1_000_000_000).unwrap();
        let expired = table
            .put(vec![
                (1, Value::Int64(1)),
                (2, Value::Int64(now - 2_000_000_000)),
            ])
            .unwrap();
        let live = table
            .put(vec![
                (1, Value::Int64(2)),
                (2, Value::Int64(now + 60_000_000_000)),
            ])
            .unwrap();
        let null = table
            .put(vec![(1, Value::Int64(3)), (2, Value::Null)])
            .unwrap();
        table.flush().unwrap();
        (expired, live, null)
    };

    let mut table = Table::open(dir.path()).unwrap();
    let policy = table.ttl().unwrap();
    assert_eq!(policy.column_id, 2);
    assert_eq!(policy.duration_nanos, 1_000_000_000);
    assert!(table.get(expired_id, table.snapshot()).is_none());
    assert!(table.get(live_id, table.snapshot()).is_some());
    assert!(table.get(null_id, table.snapshot()).is_some());
    assert_eq!(table.count(), 2);
    assert_eq!(table.visible_rows(table.snapshot()).unwrap().len(), 2);

    assert!(table.should_compact());
    table.compact().unwrap();
    assert_eq!(table.run_count(), 1);
    table.clear_ttl().unwrap();
    assert_eq!(table.count(), 2, "expired row was physically reclaimed");
    assert!(table.get(expired_id, table.snapshot()).is_none());
}

#[test]
fn database_ttl_ddl_recovers_from_wal() {
    let dir = tempdir().unwrap();
    let table_id = {
        let db = Database::create(dir.path()).unwrap();
        let table_id = db.create_table("events", schema()).unwrap();
        let policy = db
            .set_table_ttl("events", "created_at", 86_400_000_000_000)
            .unwrap();
        assert_eq!(policy.column_id, 2);
        table_id
    };

    // Simulate a crash after the DDL commit reached the shared WAL but before
    // its table manifest checkpoint was published.
    let table_dir = dir.path().join("tables").join(table_id.to_string());
    let mut stale = manifest::read(&table_dir, None).unwrap();
    stale.ttl = None;
    stale.current_epoch = 0;
    manifest::write_atomic(&table_dir, &mut stale, None).unwrap();

    let db = Database::open(dir.path()).unwrap();
    let policy = db.table("events").unwrap().lock().ttl().unwrap();
    assert_eq!(policy.column_id, 2);
    assert_eq!(policy.duration_nanos, 86_400_000_000_000);
}

#[test]
fn ttl_rejects_non_timestamp_columns_and_zero_duration() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    assert!(table.set_ttl("id", 1).is_err());
    assert!(table.set_ttl("created_at", 0).is_err());
}

#[test]
fn ttl_ddl_replicates_incrementally() {
    let dir = tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");
    let leader = Database::create(&leader_path).unwrap();
    leader.create_table("events", schema()).unwrap();

    let snapshot = leader.replication_snapshot().unwrap();
    snapshot.install(&follower_path).unwrap();
    leader
        .set_table_ttl("events", "created_at", 3_600_000_000_000)
        .unwrap();
    let batch = leader.replication_batch_since(snapshot.epoch()).unwrap();
    assert!(!batch.requires_snapshot);

    let follower = Database::open(&follower_path).unwrap();
    follower.append_replication_batch(&batch.records).unwrap();
    drop(follower);
    let follower = Database::open(&follower_path).unwrap();
    let policy = follower.table("events").unwrap().lock().ttl().unwrap();
    assert_eq!(policy.duration_nanos, 3_600_000_000_000);
}
