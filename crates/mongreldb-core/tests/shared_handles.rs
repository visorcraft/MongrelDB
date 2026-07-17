//! Stage 1A (spec §10.1): shared storage cores behind lightweight handles.
//!
//! Covers S1A-001 (one `DatabaseCore`, many `DatabaseHandle`s), S1A-002
//! (process-local `DatabaseManager` registry, exactly-once initialization,
//! last-drop closes), S1A-003 (stable file identity), and S1A-004
//! (lifecycle: drain-on-shutdown, operation guards).

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{
    Database, DatabaseManager, HandleIdentity, LifecycleState, MongrelError, OpenIdentity, Value,
};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

fn id_schema() -> Schema {
    Schema {
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
        }],
        ..Schema::default()
    }
}

/// Create a fresh database at a temporary root, then drop the exclusive
/// creator so `open_shared` performs the one shared initialization.
fn fresh_root() -> tempfile::TempDir {
    let dir = tempdir().unwrap();
    drop(Database::create(dir.path()).unwrap());
    dir
}

#[test]
fn open_shared_handles_reference_the_same_core() {
    let dir = fresh_root();
    let alice = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    let worker = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    assert!(
        Arc::ptr_eq(&alice.core(), &worker.core()),
        "both handles must reference the exact same process-local DatabaseCore"
    );
    assert_eq!(alice.lifecycle_state(), LifecycleState::Open);
}

#[test]
fn writes_through_one_handle_are_visible_to_the_other() {
    let dir = fresh_root();
    let writer = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    let reader = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();

    writer.create_table("items", id_schema()).unwrap();
    writer
        .transaction(|transaction| {
            transaction.put("items", vec![(1, Value::Int64(1))])?;
            transaction.put("items", vec![(1, Value::Int64(2))])?;
            Ok(())
        })
        .unwrap();

    let table = reader.table("items").unwrap();
    assert_eq!(table.lock().count(), 2);
}

#[test]
fn per_handle_identity_is_retained() {
    let dir = fresh_root();
    let anonymous = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    let service = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::ServicePrincipal {
                principal_id: [7; 16],
            },
        )
        .unwrap();
    assert_eq!(anonymous.identity(), &HandleIdentity::Credentialless);
    assert_eq!(
        service.identity(),
        &HandleIdentity::ServicePrincipal {
            principal_id: [7; 16]
        }
    );
    assert!(
        Arc::ptr_eq(&anonymous.core(), &service.core()),
        "distinct identities still share one core"
    );
}

#[test]
fn dropping_one_handle_keeps_storage_open_for_the_rest() {
    let dir = fresh_root();
    let first = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    let second = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    first.create_table("items", id_schema()).unwrap();
    drop(first);

    // Storage is unaffected by the drop: the surviving handle still writes.
    second
        .transaction(|transaction| {
            transaction.put("items", vec![(1, Value::Int64(9))])?;
            Ok(())
        })
        .unwrap();
    assert_eq!(second.table("items").unwrap().lock().count(), 1);
    assert_eq!(second.lifecycle_state(), LifecycleState::Open);

    // Last drop closes storage: an exclusive open succeeds afterwards.
    drop(second);
    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.table("items").unwrap().lock().count(), 1);
}

#[test]
fn exclusive_open_and_shared_core_reject_each_other() {
    let dir = fresh_root();

    // Shared core exists → exclusive open is rejected.
    let handle = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    assert!(matches!(
        Database::open(dir.path()),
        Err(MongrelError::DatabaseLocked { .. })
    ));
    drop(handle);

    // Exclusive owner exists → shared attach is rejected.
    let owner = Database::open(dir.path()).unwrap();
    assert!(matches!(
        DatabaseManager::global().open_shared(dir.path(), OpenIdentity::Credentialless),
        Err(MongrelError::DatabaseLocked { .. })
    ));
    drop(owner);

    // Both directions clear once the other closes.
    let handle = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    drop(handle);
    Database::open(dir.path()).unwrap();
}

#[test]
fn concurrent_open_shared_initializes_exactly_once() {
    let dir = fresh_root();
    let root = dir.path().to_path_buf();
    let mut threads = Vec::new();
    for _ in 0..8 {
        let root = root.clone();
        threads.push(std::thread::spawn(move || {
            DatabaseManager::global()
                .open_shared(root, OpenIdentity::Credentialless)
                .unwrap()
        }));
    }
    let handles: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    let first_core = handles[0].core();
    for handle in &handles[1..] {
        assert!(
            Arc::ptr_eq(&first_core, &handle.core()),
            "every racer must land on the one initialized core"
        );
    }
    // The shared core works for every racer.
    handles[0].create_table("items", id_schema()).unwrap();
    handles[3]
        .transaction(|transaction| {
            transaction.put("items", vec![(1, Value::Int64(4))])?;
            Ok(())
        })
        .unwrap();
    assert_eq!(handles[7].table("items").unwrap().lock().count(), 1);
}

#[test]
fn shutdown_drains_operations_and_closes_the_core() {
    let dir = fresh_root();
    let operator = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    let closer = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    operator.create_table("items", id_schema()).unwrap();
    operator
        .transaction(|transaction| {
            transaction.put("items", vec![(1, Value::Int64(1))])?;
            Ok(())
        })
        .unwrap();

    // An in-flight operation holds the drain open.
    let guard = operator.operation_guard().unwrap();
    let shutdown = std::thread::spawn(move || closer.shutdown(Duration::from_secs(30)));
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !shutdown.is_finished(),
        "shutdown must wait for in-flight operations"
    );
    assert_eq!(operator.lifecycle_state(), LifecycleState::Draining);
    // New operations are rejected while draining.
    assert!(operator.operation_guard().is_err());
    drop(guard);

    shutdown.join().unwrap().unwrap();
    assert_eq!(operator.lifecycle_state(), LifecycleState::Closed);
    assert!(operator.operation_guard().is_err());

    // A fresh attach re-initializes a new core; durable state survived.
    let reopened = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    assert_eq!(reopened.lifecycle_state(), LifecycleState::Open);
    assert_eq!(reopened.table("items").unwrap().lock().count(), 1);
    assert!(!Arc::ptr_eq(&operator.core(), &reopened.core()));
}

#[test]
fn exclusive_database_reports_identity_and_open_lifecycle() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path()).unwrap();
    assert_eq!(database.identity(), HandleIdentity::Credentialless);
    assert_eq!(database.lifecycle_state(), LifecycleState::Open);
    database.operation_guard().unwrap();
}

#[test]
fn shared_facades_reject_auth_mode_transitions() {
    let dir = fresh_root();
    let handle = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    // Fail closed: one shared handle must not flip the core's enforcement
    // mode out from under the other handles.
    assert!(matches!(
        handle.enable_auth("admin", "password"),
        Err(MongrelError::Conflict(_))
    ));
}
