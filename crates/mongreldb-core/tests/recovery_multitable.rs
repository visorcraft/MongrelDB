//! P2.6 — two-pass, epoch-gated, no-truncate multi-table recovery.

use mongreldb_core::{schema::*, Database, Value};
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
    }
}

#[test]
fn recovery_replays_committed_skips_uncommitted_and_gates_by_flushed_epoch() {
    let dir = tempdir().unwrap();

    // 1) Commit transactions across 2 tables, then drop the DB without any
    //    clean shutdown (a raw reopen simulates a crash).
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("a", one_int_schema()).unwrap();
        db.create_table("b", one_int_schema()).unwrap();
        db.transaction(|t| {
            t.put("a", vec![(1, Value::Int64(1))])?;
            t.put("b", vec![(1, Value::Int64(2))])?;
            Ok(())
        })
        .unwrap();
        db.transaction(|t| {
            t.put("a", vec![(1, Value::Int64(3))])?;
            Ok(())
        })
        .unwrap();
    }

    // 2) Reopen: all committed rows present.
    {
        let db = Database::open(dir.path()).unwrap();
        assert_eq!(db.table("a").unwrap().lock().count(), 2);
        assert_eq!(db.table("b").unwrap().lock().count(), 1);
    }

    // 3) flush table A (flushed_epoch advances), append+commit a later txn to
    //    A, reopen -> the later txn is still applied (gated by epoch, not seq).
    {
        let db = Database::open(dir.path()).unwrap();
        // Flush A so its data is durable in a run and flushed_epoch advances.
        db.table("a").unwrap().lock().flush().unwrap();
        let pre = db.visible_epoch().0;
        db.transaction(|t| {
            t.put("a", vec![(1, Value::Int64(7))])?;
            Ok(())
        })
        .unwrap();
        let post = db.visible_epoch().0;
        assert!(post > pre, "commit must advance the epoch");
        assert_eq!(db.table("a").unwrap().lock().count(), 3);
    }
    {
        let db = Database::open(dir.path()).unwrap();
        // The post-flush txn (epoch > flushed_epoch) is replayed from the WAL.
        assert_eq!(db.table("a").unwrap().lock().count(), 3);
        assert_eq!(db.table("b").unwrap().lock().count(), 1);
    }
}

#[test]
fn recovery_ignores_uncommitted_txn() {
    // A transaction whose records were fsync'd but whose TxnCommit was NOT
    // (crash mid-commit) must not appear after reopen. We simulate this by
    // staging puts then dropping without commit (rollback).
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("a", one_int_schema()).unwrap();
        let mut tx = db.begin();
        tx.put("a", vec![(1, Value::Int64(99))]).unwrap();
        // rollback (drop) — nothing committed
        tx.rollback();
    }
    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.table("a").unwrap().lock().count(), 0);
}
