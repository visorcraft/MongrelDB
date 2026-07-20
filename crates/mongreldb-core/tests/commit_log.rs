//! FND-004/FND-006 integration tests (spec §9.4, §9.6): the standalone
//! `CommitLog` adapter round-trips command envelopes through the shared WAL,
//! ordinary commits route through the commit log, and the named fault hooks
//! drive the same failure handling as real durability errors.

use mongreldb_core::{schema::*, Database, MongrelError, Query, Value};
use mongreldb_log::{CommandEnvelope, DurabilityLevel, ExecutionControl, LogPosition};
use mongreldb_types::hlc::HlcTimestamp;
use tempfile::tempdir;

/// Fault hooks live in a process-global registry; serialize every test in
/// this binary so an armed hook cannot leak into a concurrently running test.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    let guard = SERIAL.lock().unwrap();
    mongreldb_fault::clear();
    guard
}

fn pk_schema(schema_id: u64) -> Schema {
    Schema {
        schema_id,
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

fn row_count(db: &Database, table: &str) -> usize {
    db.table(table)
        .unwrap()
        .lock()
        .query(&Query::new())
        .unwrap()
        .len()
}

#[test]
fn propose_receipt_read_committed_round_trip() {
    let _serial = serial();
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let log = db.commit_log();

    let envelope = CommandEnvelope::new(7, [42u8; 16], b"stage-zero".to_vec());
    let receipt = log
        .propose(envelope.clone(), &ExecutionControl::default())
        .unwrap();
    assert_eq!(receipt.durability, DurabilityLevel::GroupCommit);
    assert_eq!(receipt.log_position.term, 0);
    assert!(receipt.log_position.index > 0);
    assert!(receipt.commit_ts > HlcTimestamp::ZERO);

    let entries = log.read_committed(LogPosition::ZERO, 16).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].position, receipt.log_position);
    assert_eq!(entries[0].envelope, envelope);
    assert!(entries[0].commit_ts > HlcTimestamp::ZERO);

    // `after` excludes the entry; the applied watermark covers the commit.
    assert!(log
        .read_committed(receipt.log_position, 16)
        .unwrap()
        .is_empty());
    assert!(log.applied_position().index >= receipt.log_position.index);

    // The committed envelope is durable: a reopen replays it from the WAL.
    drop(db);
    let db = Database::open(dir.path()).unwrap();
    let entries = db
        .commit_log()
        .read_committed(LogPosition::ZERO, 16)
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].envelope, envelope);
}

#[test]
fn normal_commits_route_through_the_commit_log() {
    let _serial = serial();
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();

    let mut txn = db.begin();
    txn.put("t", vec![(1, Value::Int64(1))]).unwrap();
    let epoch = txn.commit().unwrap();

    // Visibility was published after the commit log's receipt.
    assert!(db.snapshot().0.epoch >= epoch);
    assert_eq!(row_count(&db, "t"), 1);
    assert!(db.commit_log().applied_position().index >= epoch.0);
    // Ordinary transaction commits are not command envelopes: Stage 0 keeps
    // the v4 WAL record format (see commit_log.rs module docs), so
    // read_committed has nothing to replay for them.
    assert!(db
        .commit_log()
        .read_committed(LogPosition::ZERO, 16)
        .unwrap()
        .is_empty());
}

#[test]
fn injected_fsync_failure_fails_commit_without_visibility() {
    let _serial = serial();
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();

    mongreldb_fault::activate("wal.fsync.before", mongreldb_fault::Action::Fail);
    let mut txn = db.begin();
    txn.put("t", vec![(1, Value::Int64(1))]).unwrap();
    let result = txn.commit();
    mongreldb_fault::clear();

    let failed_epoch = match result {
        Err(MongrelError::CommitOutcomeUnknown { epoch, .. }) => epoch,
        other => panic!("expected CommitOutcomeUnknown, got {other:?}"),
    };
    // The commit reported failure and its data was never published.
    assert_eq!(row_count(&db, "t"), 0);
    // The assigned ticket was abandoned, so the watermark is not stuck.
    assert_eq!(db.visible_epoch().0, failed_epoch);
    // The handle poisons exactly as for a real fsync failure.
    let mut txn = db.begin();
    txn.put("t", vec![(1, Value::Int64(2))]).unwrap();
    assert!(txn
        .commit()
        .unwrap_err()
        .to_string()
        .contains("database poisoned"));
}

#[test]
fn injected_publish_failure_exercises_the_abort_path() {
    let _serial = serial();
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();

    mongreldb_fault::activate("commit.publish.before", mongreldb_fault::Action::Fail);
    let mut txn = db.begin();
    txn.put("t", vec![(1, Value::Int64(7))]).unwrap();
    let result = txn.commit();
    mongreldb_fault::clear();

    // The commit marker was already durable when publication failed, so this
    // surfaces exactly like every other post-durable failure: a structured
    // DurableCommit, never a rollback claim (once the commit log issued its
    // receipt the write is irrevocable).
    let failed_epoch = match result {
        Err(MongrelError::DurableCommit { epoch, .. }) => epoch,
        other => panic!("expected DurableCommit, got {other:?}"),
    };
    // Abort path consistency: the epoch ticket resolved (abandoned) instead of
    // stalling the watermark, and the handle poisons like any post-durable
    // failure.
    assert_eq!(db.visible_epoch().0, failed_epoch);
    let mut txn = db.begin();
    txn.put("t", vec![(1, Value::Int64(8))]).unwrap();
    assert!(txn
        .commit()
        .unwrap_err()
        .to_string()
        .contains("database poisoned"));
}

#[test]
fn snapshots_are_unsupported_in_stage_zero() {
    let _serial = serial();
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let log = db.commit_log();
    assert!(matches!(
        log.create_snapshot(),
        Err(mongreldb_log::LogError::Unsupported(_))
    ));
    let snapshot = mongreldb_log::LogSnapshot {
        position: LogPosition::ZERO,
        commit_ts: HlcTimestamp::ZERO,
        data: Vec::new(),
    };
    assert!(matches!(
        log.install_snapshot(snapshot),
        Err(mongreldb_log::LogError::Unsupported(_))
    ));
}
