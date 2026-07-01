//! P2.4 — `SharedWal` multiplexes many tables' records onto one fd.
//! B1 — mounted tables route every write through that one shared WAL.

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Database, Epoch, Op, SharedWal, Value};
use tempfile::tempdir;

fn one_int_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "v".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
    }
}

#[test]
fn database_uses_one_wal_for_n_tables() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", one_int_schema()).unwrap();
    db.create_table("b", one_int_schema()).unwrap();
    // Mounted tables must NOT create their own `_wal/` dir (B1).
    for name in ["a", "b"] {
        let id = db.table_id(name).unwrap();
        let per_table_wal = dir.path().join("tables").join(id.to_string()).join("_wal");
        assert!(
            !per_table_wal.exists(),
            "mounted table {name} must not own a WAL"
        );
    }
    assert!(dir.path().join("_wal").exists(), "shared WAL exists");
}

#[test]
fn single_table_put_commit_recovers_from_shared_wal() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("t", one_int_schema()).unwrap();
        let t = db.table("t").unwrap();
        {
            let mut g = t.lock();
            g.put(vec![(1, Value::Int64(7))]).unwrap();
            g.commit().unwrap();
        }
        // Drop without an explicit clean-shutdown step: the row's durability
        // must come from the shared WAL alone.
    }
    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.table("t").unwrap().lock().count(), 1);
}

#[test]
fn shared_wal_interleaves_two_tables_one_fd() {
    let dir = tempdir().unwrap();
    let mut w = SharedWal::create(dir.path(), Epoch(0)).unwrap();
    w.append(
        1,
        10,
        Op::Put {
            table_id: 10,
            rows: vec![1],
        },
    )
    .unwrap();
    w.append(
        2,
        20,
        Op::Put {
            table_id: 20,
            rows: vec![2],
        },
    )
    .unwrap();
    w.append_commit(1, Epoch(1), &[]).unwrap();
    w.append_commit(2, Epoch(2), &[]).unwrap();
    let d = w.group_sync().unwrap();
    assert!(d >= 4);
    let recs = SharedWal::replay(dir.path()).unwrap();
    assert_eq!(
        recs.iter()
            .filter(|r| matches!(r.op, Op::Put { .. }))
            .count(),
        2
    );
}

#[test]
fn shared_wal_rotate_advances_segment_and_preserves_history() {
    let dir = tempdir().unwrap();
    let mut w = SharedWal::create(dir.path(), Epoch(0)).unwrap();
    w.append(
        1,
        1,
        Op::Put {
            table_id: 1,
            rows: vec![1],
        },
    )
    .unwrap();
    w.append_commit(1, Epoch(1), &[]).unwrap();
    w.group_sync().unwrap();
    w.rotate(1).unwrap();
    assert_eq!(w.active_segment_no(), 1);
    w.append(
        2,
        1,
        Op::Put {
            table_id: 1,
            rows: vec![2],
        },
    )
    .unwrap();
    w.append_commit(2, Epoch(2), &[]).unwrap();
    w.group_sync().unwrap();
    let recs = SharedWal::replay(dir.path()).unwrap();
    assert_eq!(
        recs.iter()
            .filter(|r| matches!(r.op, Op::Put { .. }))
            .count(),
        2
    );
}

#[test]
fn shared_wal_abort_marker_roundtrips() {
    let dir = tempdir().unwrap();
    let mut w = SharedWal::create(dir.path(), Epoch(0)).unwrap();
    w.append(
        7,
        1,
        Op::Put {
            table_id: 1,
            rows: vec![9],
        },
    )
    .unwrap();
    w.append_abort(7).unwrap();
    w.group_sync().unwrap();
    let recs = SharedWal::replay(dir.path()).unwrap();
    assert!(recs.iter().any(|r| matches!(r.op, Op::TxnAbort)));
}
