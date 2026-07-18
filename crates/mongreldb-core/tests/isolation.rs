//! S1B-002 isolation-level tests (spec §10.2): formal transaction state,
//! ReadCommitted per-statement snapshots, RepeatableRead fixed snapshots
//! (including the deprecated `Snapshot` alias), and Serializable SSI-style
//! certification — write-skew and read-only dangerous structures must abort
//! with a serialization failure instead of committing a non-serializable
//! interleaving.

use mongreldb_core::txn::{AbortReason, IsolationLevel, TransactionState};
use mongreldb_core::{schema::*, Database, MongrelError, RowId, Value};
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
            embedding_source: None,
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

/// Insert one row per pk value and return their row ids in pk order.
fn seed_rows(db: &Database, table: &str, pks: &[i64]) -> Vec<RowId> {
    let (_, row_ids) = db
        .transaction_with_row_ids(|t| {
            for pk in pks {
                t.put(table, vec![(1, Value::Int64(*pk))])?;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(row_ids.len(), pks.len());
    row_ids
}

fn assert_serialization_failure(error: MongrelError) {
    match error {
        MongrelError::SerializationFailure { message } => assert!(
            message.contains("invalidated this transaction's reads"),
            "expected a serialization failure, got: {message}"
        ),
        other => panic!("expected a serialization failure, got {other:?}"),
    }
}

// ── Formal transaction state (S1B-001) ───────────────────────────────────

#[test]
fn commit_transitions_state_to_committed_with_receipt() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();

    let mut txn = db.begin();
    assert!(matches!(txn.state(), TransactionState::Active));
    let handle = txn.state_handle();
    let read_ts = txn.read_ts().expect("begin must capture a read timestamp");
    txn.put("t", vec![(1, Value::Int64(1))]).unwrap();
    let epoch = txn.commit().unwrap();

    let TransactionState::Committed(receipt) = handle.state() else {
        panic!("expected Committed, got {:?}", handle.state());
    };
    assert_eq!(receipt.log_position.term, 0);
    assert_eq!(receipt.log_position.index, epoch.0);
    assert_eq!(
        receipt.durability,
        mongreldb_log::DurabilityLevel::GroupCommit
    );
    // §8.2: the commit timestamp is strictly greater than the transaction's
    // read timestamp.
    assert!(
        receipt.commit_ts > read_ts,
        "commit ts {:?} must exceed read ts {:?}",
        receipt.commit_ts,
        read_ts
    );
}

#[test]
fn conflicted_commit_transitions_state_to_aborted() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();

    // t2 begins before t1 commits, so t2's write-write conflict surfaces as a
    // first-committer-wins abort at its own commit.
    let mut t1 = db.begin();
    let mut t2 = db.begin();
    let h2 = t2.state_handle();
    t1.put("t", vec![(1, Value::Int64(1))]).unwrap();
    t1.commit().unwrap();
    t2.put("t", vec![(1, Value::Int64(1))]).unwrap();
    let error = t2.commit().unwrap_err();
    assert!(matches!(error, MongrelError::Conflict(_)));
    assert!(matches!(
        h2.state(),
        TransactionState::Aborted(AbortReason::Conflict(_))
    ));
}

#[test]
fn rollback_and_drop_transition_state_to_aborted() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();

    let mut txn = db.begin();
    let handle = txn.state_handle();
    txn.put("t", vec![(1, Value::Int64(1))]).unwrap();
    txn.rollback();
    assert!(matches!(
        handle.state(),
        TransactionState::Aborted(AbortReason::RolledBack)
    ));

    let mut txn = db.begin();
    let handle = txn.state_handle();
    txn.put("t", vec![(1, Value::Int64(2))]).unwrap();
    drop(txn);
    assert!(matches!(
        handle.state(),
        TransactionState::Aborted(AbortReason::RolledBack)
    ));
}

// ── Read Committed (S1B-002) ─────────────────────────────────────────────

#[test]
fn read_committed_observes_commits_between_statements() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();

    let mut rc = db.begin_with_isolation(IsolationLevel::ReadCommitted);
    // The row is inserted and committed only after `rc` began.
    let (_, row_ids) = db
        .transaction_with_row_ids(|t| {
            t.put("t", vec![(1, Value::Int64(42))])?;
            Ok(())
        })
        .unwrap();
    // Read Committed re-pins per statement: this statement sees the row.
    let row = rc.get("t", row_ids[0]).unwrap();
    assert_eq!(
        row.map(|row| row.columns[0].1.clone()),
        Some(Value::Int64(42))
    );
    rc.rollback();
}

#[test]
fn repeatable_read_keeps_the_fixed_begin_snapshot() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();

    let mut rr = db.begin_with_isolation(IsolationLevel::RepeatableRead);
    // Insert + commit after `rr` began: invisible at the begin snapshot.
    let (_, row_ids) = db
        .transaction_with_row_ids(|t| {
            t.put("t", vec![(1, Value::Int64(42))])?;
            Ok(())
        })
        .unwrap();
    assert!(rr.get("t", row_ids[0]).unwrap().is_none());
    // A fresh transaction at the latest snapshot sees it.
    let mut latest = db.begin_with_isolation(IsolationLevel::ReadCommitted);
    assert!(latest.get("t", row_ids[0]).unwrap().is_some());
    latest.rollback();
    rr.rollback();
}

#[test]
fn repeatable_read_rereads_the_same_version_after_concurrent_update() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();
    let rows = seed_rows(&db, "t", &[1]);
    let row_id = rows[0];

    let mut rr = db.begin_with_isolation(IsolationLevel::RepeatableRead);
    let first = rr.get("t", row_id).unwrap().unwrap();
    db.transaction(|t| {
        t.update_many("t", vec![(row_id, vec![(1, Value::Int64(99))])])?;
        Ok(())
    })
    .unwrap();
    let second = rr.get("t", row_id).unwrap().unwrap();
    assert_eq!(
        first.columns, second.columns,
        "RepeatableRead must re-read the begin-snapshot version"
    );
    rr.rollback();
}

#[test]
fn deprecated_snapshot_alias_behaves_as_repeatable_read() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();

    #[allow(deprecated)]
    let level = IsolationLevel::Snapshot;
    assert_eq!(level.canonical(), IsolationLevel::RepeatableRead);
    let mut txn = db.begin_with_isolation(level);
    assert_eq!(txn.isolation(), level);
    let (_, row_ids) = db
        .transaction_with_row_ids(|t| {
            t.put("t", vec![(1, Value::Int64(42))])?;
            Ok(())
        })
        .unwrap();
    // Same fixed-at-begin semantics as RepeatableRead.
    assert!(txn.get("t", row_ids[0]).unwrap().is_none());
    txn.rollback();
}

// ── Serializable / SSI (S1B-002) ─────────────────────────────────────────

/// Classic write skew: both transactions read both rows, each writes a
/// different row. Repeatable Read (snapshot isolation) allows the anomaly;
/// Serializable must detect the rw-antidependency cycle and abort one side.
#[test]
fn serializable_write_skew_is_detected_and_aborted() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();
    let rows = seed_rows(&db, "t", &[1, 2]);
    let (r1, r2) = (rows[0], rows[1]);

    let mut t1 = db.begin_with_isolation(IsolationLevel::Serializable);
    let mut t2 = db.begin_with_isolation(IsolationLevel::Serializable);
    // Both read both rows.
    t1.get("t", r1).unwrap();
    t1.get("t", r2).unwrap();
    t2.get("t", r1).unwrap();
    t2.get("t", r2).unwrap();
    // Each writes a different row.
    t1.update_many("t", vec![(r1, vec![(1, Value::Int64(100))])])
        .unwrap();
    t2.update_many("t", vec![(r2, vec![(1, Value::Int64(200))])])
        .unwrap();

    t1.commit()
        .expect("first committer has no dangerous structure yet");
    let h2 = t2.state_handle();
    assert_serialization_failure(t2.commit().unwrap_err());
    assert!(matches!(
        h2.state(),
        TransactionState::Aborted(AbortReason::Conflict(_))
    ));
}

/// The same interleaving commits under Repeatable Read — documenting the
/// anomaly the Serializable level exists to prevent.
#[test]
fn repeatable_read_allows_the_write_skew_anomaly() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();
    let rows = seed_rows(&db, "t", &[1, 2]);
    let (r1, r2) = (rows[0], rows[1]);

    let mut t1 = db.begin_with_isolation(IsolationLevel::RepeatableRead);
    let mut t2 = db.begin_with_isolation(IsolationLevel::RepeatableRead);
    t1.get("t", r1).unwrap();
    t1.get("t", r2).unwrap();
    t2.get("t", r1).unwrap();
    t2.get("t", r2).unwrap();
    t1.update_many("t", vec![(r1, vec![(1, Value::Int64(100))])])
        .unwrap();
    t2.update_many("t", vec![(r2, vec![(1, Value::Int64(200))])])
        .unwrap();
    t1.commit().unwrap();
    t2.commit()
        .expect("snapshot isolation permits write skew (first-committer-wins only)");
}

/// A read-only serializable transaction whose reads were invalidated by a
/// concurrent commit must not silently commit a stale observation.
#[test]
fn serializable_read_only_invalidated_reads_abort() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();
    let rows = seed_rows(&db, "t", &[1]);
    let row_id = rows[0];

    let mut ro = db.begin_with_isolation(IsolationLevel::Serializable);
    ro.get("t", row_id).unwrap();
    // A concurrent writer updates the row after `ro` read it.
    db.transaction(|t| {
        t.update_many("t", vec![(row_id, vec![(1, Value::Int64(7))])])?;
        Ok(())
    })
    .unwrap();
    assert_serialization_failure(ro.commit().unwrap_err());
}

#[test]
fn serializable_read_only_without_invalidation_commits() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();
    let rows = seed_rows(&db, "t", &[1]);

    let mut ro = db.begin_with_isolation(IsolationLevel::Serializable);
    ro.get("t", rows[0]).unwrap();
    ro.commit()
        .expect("no concurrent write invalidated the read");
}

/// Predicate/range reads (tracked at table granularity) detect phantoms: a
/// concurrent insert on the read table invalidates the predicate.
#[test]
fn serializable_predicate_read_detects_phantom_insert() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();
    db.create_table("u", pk_schema(2)).unwrap();

    let mut t1 = db.begin_with_isolation(IsolationLevel::Serializable);
    t1.track_predicate_read("t").unwrap();
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(1))])?;
        Ok(())
    })
    .unwrap();
    assert_serialization_failure(t1.commit().unwrap_err());

    // A concurrent write on an unrelated table does not invalidate the read.
    let mut t2 = db.begin_with_isolation(IsolationLevel::Serializable);
    t2.track_predicate_read("t").unwrap();
    db.transaction(|t| {
        t.put("u", vec![(1, Value::Int64(1))])?;
        Ok(())
    })
    .unwrap();
    t2.commit()
        .expect("writes on another table are not phantoms");
}

/// Disjoint serializable writers with no tracked reads must not false-abort.
#[test]
fn serializable_disjoint_writers_commit() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();

    let mut t1 = db.begin_with_isolation(IsolationLevel::Serializable);
    let mut t2 = db.begin_with_isolation(IsolationLevel::Serializable);
    t1.put("t", vec![(1, Value::Int64(1))]).unwrap();
    t2.put("t", vec![(1, Value::Int64(2))]).unwrap();
    t1.commit().unwrap();
    t2.commit().unwrap();
    assert_eq!(db.table("t").unwrap().lock().count(), 2);
}

/// The tracked read set accumulates across the transaction's own read
/// operations (S1B-001 inspectability).
#[test]
fn serializable_tracks_read_and_predicate_sets() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema(1)).unwrap();
    let rows = seed_rows(&db, "t", &[1, 2]);

    let mut txn = db.begin_with_isolation(IsolationLevel::Serializable);
    assert_eq!(txn.isolation(), IsolationLevel::Serializable);
    assert!(txn.read_set().is_empty());
    txn.get("t", rows[0]).unwrap();
    txn.get("t", rows[1]).unwrap();
    txn.track_predicate_read("t").unwrap();
    assert_eq!(txn.read_set().len(), 2);
    assert_eq!(txn.predicate_set().len(), 1);
    txn.rollback();
}
