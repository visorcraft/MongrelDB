//! S1B-005 transaction idempotency tests (spec §10.2): a repeated key with
//! an identical request replays the original commit receipt without
//! re-executing, a repeated key with a different request conflicts, records
//! survive restart, and expired records are swept.

use mongreldb_core::txn::{AbortReason, IdempotencyRequest, TransactionState};
use mongreldb_core::{schema::*, Database, MongrelError, Value};
use tempfile::tempdir;

fn pk_schema(schema_id: u64) -> Schema {
    Schema {
        schema_id,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn request(key: &str, fingerprint: u64) -> IdempotencyRequest {
    IdempotencyRequest {
        key: key.to_string(),
        owner: "alice".to_string(),
        fingerprint,
        ttl: None,
    }
}

fn row_count(db: &Database, table: &str) -> u64 {
    db.table(table).unwrap().lock().count()
}

#[test]
fn repeated_identical_request_replays_original_receipt() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();

    let mut first = db.begin();
    first.put("t", vec![(1, Value::Int64(1))]).unwrap();
    let first_epoch = first.commit_idempotent(request("k1", 7)).unwrap();
    assert_eq!(row_count(&db, "t"), 1);

    // Same key + fingerprint: even though this transaction staged a
    // different row, the replay must not re-execute it — the original
    // receipt (epoch) comes back and no second row appears.
    let mut second = db.begin();
    second.put("t", vec![(1, Value::Int64(2))]).unwrap();
    let second_handle = second.state_handle();
    let second_epoch = second.commit_idempotent(request("k1", 7)).unwrap();

    assert_eq!(
        second_epoch, first_epoch,
        "replay returns the original epoch"
    );
    assert_eq!(row_count(&db, "t"), 1, "replay must not re-execute writes");
    let TransactionState::Committed(receipt) = second_handle.state() else {
        panic!("expected Committed, got {:?}", second_handle.state());
    };
    assert_eq!(receipt.log_position.index, first_epoch.0);
}

#[test]
fn repeated_key_with_different_fingerprint_conflicts() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();

    let mut first = db.begin();
    first.put("t", vec![(1, Value::Int64(1))]).unwrap();
    first.commit_idempotent(request("k1", 7)).unwrap();

    let mut second = db.begin();
    second.put("t", vec![(1, Value::Int64(2))]).unwrap();
    let handle = second.state_handle();
    let error = second.commit_idempotent(request("k1", 8)).unwrap_err();
    assert!(
        matches!(error, MongrelError::Conflict(_)),
        "expected Conflict, got {error:?}"
    );
    assert!(matches!(
        handle.state(),
        TransactionState::Aborted(AbortReason::Conflict(_))
    ));
    assert_eq!(row_count(&db, "t"), 1);

    // A different owner reuses the key independently.
    let mut third = db.begin();
    third.put("t", vec![(1, Value::Int64(2))]).unwrap();
    let mut foreign = request("k1", 8);
    foreign.owner = "bob".to_string();
    third.commit_idempotent(foreign).unwrap();
    assert_eq!(row_count(&db, "t"), 2);
}

#[test]
fn idempotency_records_survive_restart() {
    let dir = tempdir().unwrap();
    let first_epoch = {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("t", pk_schema(1)).unwrap();
        let mut txn = db.begin();
        txn.put("t", vec![(1, Value::Int64(1))]).unwrap();
        txn.commit_idempotent(request("k-restart", 11)).unwrap()
    };

    let db = Database::open(dir.path()).unwrap();
    assert_eq!(row_count(&db, "t"), 1);
    let mut replay = db.begin();
    replay.put("t", vec![(1, Value::Int64(2))]).unwrap();
    let replay_epoch = replay.commit_idempotent(request("k-restart", 11)).unwrap();
    assert_eq!(
        replay_epoch, first_epoch,
        "after restart the original receipt is replayed"
    );
    assert_eq!(row_count(&db, "t"), 1, "no re-execution after restart");

    // A different fingerprint still conflicts after the restart.
    let mut other = db.begin();
    other.put("t", vec![(1, Value::Int64(3))]).unwrap();
    assert!(matches!(
        other.commit_idempotent(request("k-restart", 12)),
        Err(MongrelError::Conflict(_))
    ));
}

#[test]
fn expired_records_are_swept_and_keys_can_be_reused() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();

    let mut short_lived = request("k-ttl", 5);
    short_lived.ttl = Some(std::time::Duration::from_millis(30));
    let mut first = db.begin();
    first.put("t", vec![(1, Value::Int64(1))]).unwrap();
    let first_epoch = first.commit_idempotent(short_lived.clone()).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(80));

    // The record expired: the same key + fingerprint executes again instead
    // of replaying, producing a new epoch and the second row.
    let mut second = db.begin();
    second.put("t", vec![(1, Value::Int64(2))]).unwrap();
    let second_epoch = second.commit_idempotent(short_lived).unwrap();
    assert_ne!(second_epoch, first_epoch);
    assert_eq!(row_count(&db, "t"), 2);
}

#[test]
fn commit_without_key_is_unaffected_by_idempotency_ledger() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();

    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(1))])?;
        Ok(())
    })
    .unwrap();
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(2))])?;
        Ok(())
    })
    .unwrap();
    assert_eq!(row_count(&db, "t"), 2);
    assert!(
        !dir.path()
            .join(mongreldb_core::txn::IDEMPOTENCY_FILENAME)
            .exists(),
        "unkeyed commits must not create the ledger file"
    );
}
