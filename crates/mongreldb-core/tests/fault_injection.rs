//! FND-006: injected durable-boundary faults leave the database openable and
//! consistent. The registry is process-global, so these tests serialize on
//! `TEST_LOCK` and disarm through `ScopedGuard`.

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::txn::{AbortReason, TransactionState};
use mongreldb_core::{Database, MongrelError, Value};
use mongreldb_fault::{Action, BarrierAction, ScopedGuard};
use std::sync::Mutex;
use std::time::Duration;

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
    let _permit = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
    let _permit = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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

/// FND-006: `txn.prepare.before` Fail aborts before Preparing/Committed.
#[test]
fn txn_prepare_before_fail_aborts_before_preparing() {
    let _permit = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    mongreldb_fault::clear();
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("db")).unwrap();
    db.create_table("items", schema()).unwrap();

    let mut txn = db.begin();
    txn.put("items", vec![(1, Value::Int64(1))]).unwrap();
    let handle = txn.state_handle();
    // Barrier-arm the later hooks so hits prove they never ran.
    mongreldb_fault::activate(
        "txn.prepare.after",
        Action::Barrier(BarrierAction::new("txn.prepare.after")),
    );
    mongreldb_fault::activate(
        "txn.decision.before",
        Action::Barrier(BarrierAction::new("txn.decision.before")),
    );
    let _guard = ScopedGuard::new("txn.prepare.before", Action::Fail);
    let error = txn.commit().unwrap_err();
    assert!(
        error.to_string().contains("txn.prepare.before"),
        "expected injected prepare fault, got {error:?}"
    );
    assert_eq!(mongreldb_fault::hits("txn.prepare.before"), 1);
    assert_eq!(mongreldb_fault::hits("txn.prepare.after"), 0);
    assert_eq!(mongreldb_fault::hits("txn.decision.before"), 0);
    match handle.state() {
        TransactionState::Aborted(AbortReason::Error(_)) => {}
        other => panic!("expected Aborted before Preparing/Committed, got {other:?}"),
    }
    // Nothing durable was published.
    assert_eq!(db.table("items").unwrap().lock().count(), 0);
}

/// FND-006: `txn.decision.before` Fail aborts before a durable commit receipt.
#[test]
fn txn_decision_before_fail_aborts_before_durable_receipt() {
    let _permit = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    mongreldb_fault::clear();
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("db")).unwrap();
    db.create_table("items", schema()).unwrap();

    let mut txn = db.begin();
    txn.put("items", vec![(1, Value::Int64(2))]).unwrap();
    let handle = txn.state_handle();
    // Count prepare hits (pass through); Fail only the decision fence.
    mongreldb_fault::activate(
        "txn.prepare.before",
        Action::Barrier(BarrierAction::new("txn.prepare.before")),
    );
    mongreldb_fault::activate(
        "txn.prepare.after",
        Action::Barrier(BarrierAction::new("txn.prepare.after")),
    );
    mongreldb_fault::activate(
        "txn.decision.after",
        Action::Barrier(BarrierAction::new("txn.decision.after")),
    );
    let _guard = ScopedGuard::new("txn.decision.before", Action::Fail);
    let error = txn.commit().unwrap_err();
    assert!(
        error.to_string().contains("txn.decision.before"),
        "expected injected decision fault, got {error:?}"
    );
    assert_eq!(mongreldb_fault::hits("txn.prepare.before"), 1);
    assert_eq!(mongreldb_fault::hits("txn.prepare.after"), 1);
    assert_eq!(mongreldb_fault::hits("txn.decision.before"), 1);
    assert_eq!(mongreldb_fault::hits("txn.decision.after"), 0);
    match handle.state() {
        TransactionState::Aborted(AbortReason::Error(_)) => {}
        // CommitCritical/Committed would mean the decision became public.
        other => panic!("expected Aborted before durable receipt, got {other:?}"),
    }
    assert_eq!(db.table("items").unwrap().lock().count(), 0);
}

/// FND-006: successful commit hits prepare/decision hooks once each.
/// Barriers record arrivals without sleeps; wait_barrier observes the wave
/// after the synchronous commit returns.
#[test]
fn txn_prepare_and_decision_hooks_hit_on_successful_commit() {
    let _permit = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    mongreldb_fault::clear();
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("db")).unwrap();
    db.create_table("items", schema()).unwrap();

    mongreldb_fault::activate(
        "txn.prepare.before",
        Action::Barrier(BarrierAction::new("txn.hooks")),
    );
    mongreldb_fault::activate(
        "txn.prepare.after",
        Action::Barrier(BarrierAction::new("txn.hooks")),
    );
    mongreldb_fault::activate(
        "txn.decision.before",
        Action::Barrier(BarrierAction::new("txn.hooks")),
    );
    mongreldb_fault::activate(
        "txn.decision.after",
        Action::Barrier(BarrierAction::new("txn.hooks")),
    );

    let mut txn = db.begin();
    txn.put("items", vec![(1, Value::Int64(3))]).unwrap();
    let handle = txn.state_handle();
    let epoch = txn.commit().unwrap();

    // Four prepare/decision boundary hits; no sleeps.
    mongreldb_fault::wait_barrier("txn.hooks", 4, Duration::from_secs(30)).unwrap();
    assert!(matches!(handle.state(), TransactionState::Committed(_)));
    assert!(epoch.0 > 0);
    assert_eq!(mongreldb_fault::hits("txn.prepare.before"), 1);
    assert_eq!(mongreldb_fault::hits("txn.prepare.after"), 1);
    assert_eq!(mongreldb_fault::hits("txn.decision.before"), 1);
    assert_eq!(mongreldb_fault::hits("txn.decision.after"), 1);
    assert_eq!(db.table("items").unwrap().lock().count(), 1);
    mongreldb_fault::clear();
}
