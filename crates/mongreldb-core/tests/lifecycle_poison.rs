//! S1A-004: an fsync-level durability poison transitions the whole storage
//! core to `LifecycleState::Poisoned`, after which every guarded operation is
//! rejected at admission (and paths that already checked the write-path
//! poison flag keep their legacy error).
//!
//! Fault injection is process-global, so every test in this file serializes
//! against the others: a bystander commit inside the armed window would eat
//! the injected failure.

use mongreldb_core::schema::{ColumnDef, ColumnFlags, TypeId};
use mongreldb_core::{Database, LifecycleState, MongrelError, Schema, Value};
use tempfile::tempdir;

/// this binary so an armed hook cannot leak into a concurrently running test.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    let guard = SERIAL.lock().unwrap();
    mongreldb_fault::clear();
    guard
}

fn pk_schema() -> Schema {
    Schema {
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
fn injected_fsync_failure_poisons_the_whole_core() {
    let _serial = serial();
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();
    assert_eq!(db.lifecycle_state(), LifecycleState::Open);

    mongreldb_fault::activate("wal.fsync.before", mongreldb_fault::Action::Fail);
    let mut txn = db.begin();
    txn.put("t", vec![(1, Value::Int64(1))]).unwrap();
    let result = txn.commit();
    mongreldb_fault::clear();
    assert!(matches!(
        result,
        Err(MongrelError::CommitOutcomeUnknown { .. })
    ));

    // S1A-004: the fsync poison transitions the whole core lifecycle, not
    // just the write-path flag.
    assert_eq!(db.lifecycle_state(), LifecycleState::Poisoned);
    // Operation admission rejects with a typed error: guarded maintenance
    // gets the lifecycle Conflict...
    let error = db.gc().unwrap_err();
    assert!(
        matches!(error, MongrelError::Conflict(_)),
        "gc must reject on a poisoned core: {error:?}"
    );
    assert!(db.operation_guard().is_err());
    // ...and paths that already checked the write-path flag keep their
    // legacy poison error.
    let mut txn = db.begin();
    txn.put("t", vec![(1, Value::Int64(2))]).unwrap();
    assert!(txn
        .commit()
        .unwrap_err()
        .to_string()
        .contains("database poisoned"));
}
