//! P2.4 — `SharedWal` multiplexes many tables' records onto one fd.

use mongreldb_core::{Epoch, Op, SharedWal};
use tempfile::tempdir;

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
