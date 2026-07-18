//! FND-006: injected durable-boundary faults leave the database openable and
//! consistent. The registry is process-global, so these tests serialize on
//! `TEST_LOCK` and disarm through `ScopedGuard`.

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Database, MongrelError, Value};
use mongreldb_fault::{Action, ScopedGuard};
use std::sync::Mutex;

static TEST_LOCK: Mutex<()> = Mutex::new(());

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
fn catalog_publish_before_failure_leaves_database_openable() {
    let _permit = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("db");
    let db = Database::create(&root).unwrap();
    db.create_table("items", schema()).unwrap();
    put(&db, 1);

    // Fail the atomic CATALOG checkpoint write. The DDL commit is already
    // durable in the WAL by the time the checkpoint runs, so the error
    // surfaces as a durable commit with an unknown post-commit step.
    let guard = ScopedGuard::new("catalog.publish.before", Action::Fail);
    let error = db.create_table("blocked", schema()).unwrap_err();
    assert!(
        matches!(error, MongrelError::DurableCommit { .. }),
        "expected DurableCommit from the failed catalog checkpoint, got {error:?}"
    );
    assert_eq!(mongreldb_fault::hits("catalog.publish.before"), 1);
    drop(guard);
    // The failed post-commit step poisons the live handle; reopen.
    drop(db);

    let reopened = Database::open(&root).unwrap();
    let items = reopened.table("items").unwrap();
    assert_eq!(items.lock().count(), 1);
    // Recovery rebuilds the catalog from the durable DDL in the WAL.
    assert!(
        reopened.table("blocked").is_ok(),
        "recovery replays the committed DDL into a consistent catalog"
    );
}

#[test]
fn snapshot_install_before_failure_is_survivable() {
    let _permit = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");
    let leader = Database::create(&leader_path).unwrap();
    leader.create_table("items", schema()).unwrap();
    put(&leader, 1);
    let snapshot = leader.replication_snapshot().unwrap();

    // Fail the install immediately before the staged snapshot is published.
    let guard = ScopedGuard::new("snapshot.install.before", Action::Fail);
    let error = snapshot.install(&follower_path).unwrap_err();
    assert!(
        matches!(&error, MongrelError::Other(message) if message.contains("snapshot.install.before")),
        "expected the injected fault, got {error:?}"
    );
    assert_eq!(mongreldb_fault::hits("snapshot.install.before"), 1);
    assert!(!follower_path.exists(), "the install never published");
    let stray: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(std::result::Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .contains("replica-stage")
        })
        .collect();
    assert!(stray.is_empty(), "staging tree was cleaned up: {stray:?}");
    drop(guard);

    // The destination was never touched, so the install is retriable.
    snapshot.install(&follower_path).unwrap();
    let follower = Database::open(&follower_path).unwrap();
    assert!(follower.is_read_only_replica());
    assert_eq!(follower.table("items").unwrap().lock().count(), 1);
    drop(follower);

    // The leader is unaffected by the failed install.
    put(&leader, 2);
    assert_eq!(leader.table("items").unwrap().lock().count(), 2);
}
