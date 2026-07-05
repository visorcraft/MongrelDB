//! P3.4 — unbounded transactions via quarantined uniform-epoch spill runs.

use mongreldb_core::query::{Condition, Query};
use mongreldb_core::{schema::*, Database, Value};
use tempfile::tempdir;

fn pk_schema() -> Schema {
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
        clustered: false,
    }
}

#[test]
fn transaction_larger_than_threshold_spills_and_commits_atomically() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();
    // Set a tiny threshold so even a single put triggers a spill.
    db.set_spill_threshold(1);

    let n: u64 = 100;
    db.transaction(|t| {
        for i in 0..n {
            t.put("t", vec![(1, Value::Int64(i as i64))])?;
        }
        Ok(())
    })
    .unwrap();

    // All rows visible after commit.
    assert_eq!(db.table("t").unwrap().lock().count(), n);

    // The pending run was moved into _runs/ — verify no _txn/ dirs remain.
    let tdir = dir
        .path()
        .join("tables")
        .join(db.table_id("t").unwrap().to_string());
    let txn_dir = tdir.join("_txn");
    assert!(
        !txn_dir.exists() || std::fs::read_dir(&txn_dir).unwrap().next().is_none(),
        "pending _txn/ dir should be empty after commit"
    );

    // Reopen — data is durable.
    drop(db);
    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.table("t").unwrap().lock().count(), n);
}

#[test]
fn stale_txn_dir_is_swept_on_reopen() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();
    let table_id = db.table_id("t").unwrap();
    let tdir = dir.path().join("tables").join(table_id.to_string());

    // Simulate a crash: create a stale _txn/ dir with a dummy file.
    let stale = tdir.join("_txn").join("99999");
    std::fs::create_dir_all(&stale).unwrap();
    std::fs::write(stale.join("r-1.sr"), b"stale").unwrap();

    // Reopen — the stale _txn/ dir is swept by the open path.
    drop(db);
    let db = Database::open(dir.path()).unwrap();

    // The stale dir is gone (or empty).
    assert!(!stale.exists(), "stale _txn/ dir must be swept on reopen");

    // Table still works.
    assert_eq!(db.table("t").unwrap().lock().count(), 0);
}

#[test]
fn huge_writeset_pre_validation_keeps_sequencer_bounded() {
    // A transaction with a large write set commits successfully. The two-phase
    // validation (pre-check outside the sequencer, delta re-check inside)
    // means the sequencer does O(1) work when no concurrent commits arrive.
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();

    // Commit a large write set (many distinct PKs → many conflict keys).
    let n: u64 = 500;
    db.transaction(|t| {
        for i in 0..n {
            t.put("t", vec![(1, Value::Int64(i as i64))])?;
        }
        Ok(())
    })
    .unwrap();

    // All rows visible.
    assert_eq!(db.table("t").unwrap().lock().count(), n);

    // A second large txn with distinct PKs also succeeds (no false conflicts).
    db.transaction(|t| {
        for i in n..(2 * n) {
            t.put("t", vec![(1, Value::Int64(i as i64))])?;
        }
        Ok(())
    })
    .unwrap();

    assert_eq!(db.table("t").unwrap().lock().count(), 2 * n);
}

#[test]
fn spilled_txn_does_not_materialize_rows_in_memtable() {
    // P3.4: a spilled large txn must keep peak memory bounded — the rows go to a
    // linked uniform-epoch run, NOT into the in-memory memtable. Reads still see
    // them (via the run + indexes), but the memtable stays empty.
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();
    db.set_spill_threshold(1);

    let n: u64 = 200;
    db.transaction(|t| {
        for i in 0..n {
            t.put("t", vec![(1, Value::Int64(i as i64))])?;
        }
        Ok(())
    })
    .unwrap();

    let tbl = db.table("t").unwrap();
    let mut g = tbl.lock();
    assert_eq!(g.count(), n, "all spilled rows must be visible");
    assert_eq!(
        g.memtable_len(),
        0,
        "spilled rows must not be materialized in the memtable"
    );
    // Index-served range query resolves entirely from the linked run.
    let q = Query::new().and(Condition::Range {
        column_id: 1,
        lo: 0,
        hi: (n as i64) - 1,
    });
    assert_eq!(g.query(&q).unwrap().len(), n as usize);
}

#[test]
fn spilled_run_respects_snapshot_isolation() {
    // A snapshot pinned before a spilled txn commits must NOT see its rows; a
    // current reader after commit must. Guards the uniform-epoch overlay (the run
    // is gated by RunRef.epoch_created, not its placeholder _epoch=0 column).
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();
    db.set_spill_threshold(1);

    let tbl = db.table("t").unwrap();
    let snap_before = tbl.lock().snapshot();

    db.transaction(|t| {
        for i in 0..50u64 {
            t.put("t", vec![(1, Value::Int64(i as i64))])?;
        }
        Ok(())
    })
    .unwrap();

    let g = tbl.lock();
    let before = g.visible_rows(snap_before).unwrap();
    assert_eq!(
        before.len(),
        0,
        "snapshot pinned before commit must not see spilled rows"
    );
    assert_eq!(g.count(), 50, "current readers see all committed rows");
}

#[test]
fn spilled_run_relink_is_idempotent_across_reopens() {
    // The shared-WAL recovery pass re-links spilled runs from the still-present
    // TxnCommit record. Re-opening repeatedly must NOT double-count or duplicate
    // the run: count stays stable and indexed lookups keep working.
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("t", pk_schema()).unwrap();
        db.set_spill_threshold(1);
        db.transaction(|t| {
            for i in 0..40i64 {
                t.put("t", vec![(1, Value::Int64(i))])?;
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(db.table("t").unwrap().lock().count(), 40);
    }

    for _ in 0..3 {
        let db = Database::open(dir.path()).unwrap();
        let tbl = db.table("t").unwrap();
        let mut g = tbl.lock();
        assert_eq!(g.count(), 40, "count must stay stable across reopens");
        let q = Query::new().and(Condition::Range {
            column_id: 1,
            lo: 0,
            hi: 39,
        });
        assert_eq!(g.query(&q).unwrap().len(), 40);
        drop(g);
        assert!(
            db.check().is_empty(),
            "check reports no integrity issues: {:?}",
            db.check()
        );
    }
}

#[test]
fn spilled_txn_recovers_run_from_wal_after_crash() {
    // A spilled transaction's data must survive a reopen even though the run
    // was linked in memory only (the manifest may not have persisted in time).
    // Recovery moves the run from _txn/ to _runs/ and links it.
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("t", pk_schema()).unwrap();
        db.set_spill_threshold(1);
        db.transaction(|t| {
            for i in 0..50i64 {
                t.put("t", vec![(1, Value::Int64(i))])?;
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(db.table("t").unwrap().lock().count(), 50);
    }

    // Reopen — the spilled run must be recovered.
    let db = Database::open(dir.path()).unwrap();
    assert_eq!(
        db.table("t").unwrap().lock().count(),
        50,
        "spilled run data must survive reopen"
    );
}
