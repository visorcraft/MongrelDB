//! P3.6 — retention-gated GC for dropped tables and pending runs.

use mongreldb_core::{schema::*, Database, ExecutionControl, MongrelError, Value};
use std::cell::Cell;
use std::sync::{Arc, Barrier};
use tempfile::tempdir;

fn pk_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "v".into(),
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

#[test]
fn dropped_table_dir_is_reclaimed_by_gc() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("doomed", pk_schema()).unwrap();
    db.transaction(|t| {
        t.put("doomed", vec![(1, Value::Int64(42))])?;
        Ok(())
    })
    .unwrap();
    assert_eq!(db.table("doomed").unwrap().lock().count(), 1);

    let table_id = db.table_id("doomed").unwrap();
    let tdir = dir.path().join("tables").join(table_id.to_string());
    assert!(tdir.exists());

    // Drop the table.
    db.drop_table("doomed").unwrap();
    // The table is gone from the live map but the dir still exists (retention).
    assert!(tdir.exists(), "dir retained until GC reclaims it");

    // No pinned snapshot → GC can reclaim immediately.
    let reclaimed = db.gc().unwrap();
    assert!(reclaimed >= 1, "GC should reclaim the dropped table dir");
    assert!(!tdir.exists(), "dir should be gone after GC");

    // Table is gone.
    assert!(db.table("doomed").is_err());
}

#[test]
fn controlled_gc_cancel_before_publish_preserves_candidates() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("doomed", pk_schema()).unwrap();
    let table_id = db.table_id("doomed").unwrap();
    let table_dir = dir.path().join("tables").join(table_id.to_string());
    db.drop_table("doomed").unwrap();

    let called = Cell::new(false);
    let control = ExecutionControl::new(None);
    let error = db
        .gc_controlled(&control, || {
            called.set(true);
            false
        })
        .unwrap_err();
    assert!(matches!(error, MongrelError::Cancelled));
    assert!(called.get());
    assert!(table_dir.exists());

    assert!(db.gc().unwrap() >= 1);
    assert!(!table_dir.exists());
}

#[test]
fn gc_receipt_is_scan_epoch_not_posthoc_visible_epoch() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("live", pk_schema()).unwrap();
    db.create_table("doomed", pk_schema()).unwrap();
    db.drop_table("doomed").unwrap();
    let scan_epoch = db.visible_epoch();

    let (reclaimed, receipt) = db
        .gc_controlled_with_receipt(&ExecutionControl::new(None), || {
            db.transaction(|transaction| {
                transaction.put("live", vec![(1, Value::Int64(7))])?;
                Ok(())
            })
            .unwrap();
            true
        })
        .unwrap();

    assert!(reclaimed >= 1);
    assert_eq!(receipt.unwrap().epoch, scan_epoch);
    assert!(db.visible_epoch() > scan_epoch);
}

#[test]
fn compaction_receipt_is_table_snapshot_not_posthoc_visible_epoch() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("compact", pk_schema()).unwrap();
    db.create_table("other", pk_schema()).unwrap();
    db.table("compact")
        .unwrap()
        .lock()
        .set_mutable_run_spill_bytes(1);
    for value in [1, 2] {
        db.transaction(|transaction| {
            transaction.put("compact", vec![(1, Value::Int64(value))])?;
            Ok(())
        })
        .unwrap();
        db.table("compact").unwrap().lock().flush().unwrap();
    }
    let table_epoch = db.visible_epoch();
    let handle = db.table("compact").unwrap();
    let (changed, receipt) = handle
        .lock()
        .compact_controlled_with_receipt(&ExecutionControl::new(None), || {
            db.transaction(|transaction| {
                transaction.put("other", vec![(1, Value::Int64(9))])?;
                Ok(())
            })
            .unwrap();
            true
        })
        .unwrap();

    assert!(changed);
    assert_eq!(receipt.unwrap().epoch, table_epoch);
    assert!(db.visible_epoch() > table_epoch);
}

#[test]
fn pinned_snapshot_blocks_drop_reclaim() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(1))])?;
        Ok(())
    })
    .unwrap();

    // Pin a snapshot at the current visible epoch (before the drop).
    let (_snap, guard) = db.snapshot();

    db.drop_table("t").unwrap();

    // GC should NOT reclaim the dir while a snapshot is pinned at an epoch
    // below the drop epoch.
    db.gc().unwrap();

    let table_id = {
        // Read the catalog directly to find the dropped table's id.
        let cat = db.catalog_snapshot();
        cat.tables
            .iter()
            .find(|t| t.name == "t")
            .map(|t| t.table_id)
            .unwrap()
    };
    let tdir = dir.path().join("tables").join(table_id.to_string());
    assert!(
        tdir.exists(),
        "dir must survive GC while a snapshot is pinned"
    );

    // Release the pin — now GC can reclaim.
    drop(guard);
    db.gc().unwrap();
    assert!(!tdir.exists(), "dir should be reclaimed after unpin");
}

#[test]
fn gc_does_not_delete_in_flight_txn_dir() {
    // A large txn spills into `_txn/<id>/`. While it is paused mid-commit (after
    // the spill write, before publish) a concurrent `gc()` must NOT delete its
    // pending dir — otherwise the commit loses data. After release the txn
    // commits and all rows are present.
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("t", pk_schema()).unwrap();
    db.set_spill_threshold(1);

    let reached = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    {
        let reached = reached.clone();
        let release = release.clone();
        db.__set_spill_hook(move || {
            reached.wait();
            release.wait();
        });
    }

    let writer = {
        let db = db.clone();
        std::thread::spawn(move || {
            db.transaction(|t| {
                for i in 0..80i64 {
                    t.put("t", vec![(1, Value::Int64(i))])?;
                }
                Ok(())
            })
            .unwrap();
        })
    };

    // Wait until the txn has written its spill run and is paused.
    reached.wait();

    // GC must not touch the in-flight txn's dir.
    db.gc().unwrap();
    let table_id = db.table_id("t").unwrap();
    let txn_dir = dir
        .path()
        .join("tables")
        .join(table_id.to_string())
        .join("_txn");
    let has_pending = txn_dir.exists()
        && std::fs::read_dir(&txn_dir)
            .map(|mut d| d.next().is_some())
            .unwrap_or(false);
    assert!(
        has_pending,
        "in-flight spill dir must survive a concurrent gc()"
    );

    // Release the txn; it commits successfully.
    release.wait();
    writer.join().unwrap();

    assert_eq!(db.table("t").unwrap().lock().count(), 80);
}

fn count_sr_files(runs_dir: &std::path::Path) -> usize {
    std::fs::read_dir(runs_dir)
        .map(|rd| {
            rd.flatten()
                .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sr"))
                .count()
        })
        .unwrap_or(0)
}

#[test]
fn superseded_runs_reaped_only_after_min_active_passes() {
    // Compaction supersedes its input runs but keeps the files on disk (the
    // `retiring` queue). `gc()` deletes them only once `min_active_snapshot`
    // passes the compaction epoch: a snapshot pinned below it keeps the files;
    // releasing it lets the next gc() reap them. The merged run always survives.
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();

    let tbl = db.table("t").unwrap();
    let table_id = db.table_id("t").unwrap();
    let runs_dir = dir
        .path()
        .join("tables")
        .join(table_id.to_string())
        .join("_runs");

    // Force each flush to spill to a sorted run (vs the mutable-run tier).
    tbl.lock().set_mutable_run_spill_bytes(1);

    // Two flushed runs.
    db.transaction(|t| t.put("t", vec![(1, Value::Int64(1))]).map(|_| ()))
        .unwrap();
    tbl.lock().flush().unwrap();
    db.transaction(|t| t.put("t", vec![(1, Value::Int64(2))]).map(|_| ()))
        .unwrap();
    tbl.lock().flush().unwrap();
    assert_eq!(tbl.lock().run_count(), 2);
    assert_eq!(count_sr_files(&runs_dir), 2);

    // Pin a snapshot, then advance the epoch past it with another flushed run so
    // the compaction epoch is strictly above the pinned epoch.
    let (_snap, guard) = db.snapshot();
    db.transaction(|t| t.put("t", vec![(1, Value::Int64(3))]).map(|_| ()))
        .unwrap();
    tbl.lock().flush().unwrap();
    assert_eq!(count_sr_files(&runs_dir), 3);

    // Compact: one merged run + three superseded runs kept on disk.
    tbl.lock().compact().unwrap();
    assert_eq!(tbl.lock().run_count(), 1);
    assert_eq!(
        count_sr_files(&runs_dir),
        4,
        "superseded run files retained after compaction"
    );

    // gc() while the snapshot is pinned below the compaction epoch keeps them.
    db.gc().unwrap();
    assert_eq!(
        count_sr_files(&runs_dir),
        4,
        "pinned snapshot must block reaping of superseded runs"
    );
    // check() must not flag the retained files as orphans.
    assert!(db.check().is_empty(), "check clean: {:?}", db.check());

    // Release the pin; now gc() reaps the superseded runs, merged run remains.
    drop(guard);
    db.gc().unwrap();
    assert_eq!(
        count_sr_files(&runs_dir),
        1,
        "superseded runs reaped after the pin is released"
    );
    assert_eq!(tbl.lock().count(), 3, "live count intact after reaping");
    assert!(
        db.check().is_empty(),
        "check clean after reap: {:?}",
        db.check()
    );
}

fn count_wal_segments(wal_dir: &std::path::Path) -> usize {
    std::fs::read_dir(wal_dir)
        .map(|rd| {
            rd.flatten()
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .map(|s| s.starts_with("seg-") && s.ends_with(".wal"))
                        .unwrap_or(false)
                })
                .count()
        })
        .unwrap_or(0)
}

#[test]
fn wal_segment_not_gcd_while_in_flight_txn_holds_it() {
    // While a large txn is mid-commit (paused after its spill write, before the
    // sequencer appends its TxnCommit) a concurrent gc() must NOT delete the WAL
    // segment it will write into; the txn then commits and recovers on reopen.
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("t", pk_schema()).unwrap();
    db.set_spill_threshold(1);
    let wal_dir = dir.path().join("_wal");

    let reached = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    {
        let reached = reached.clone();
        let release = release.clone();
        db.__set_spill_hook(move || {
            reached.wait();
            release.wait();
        });
    }
    let writer = {
        let db = db.clone();
        std::thread::spawn(move || {
            db.transaction(|t| {
                for i in 0..60i64 {
                    t.put("t", vec![(1, Value::Int64(i))])?;
                }
                Ok(())
            })
            .unwrap();
        })
    };

    reached.wait();
    let before = count_wal_segments(&wal_dir);
    db.gc().unwrap();
    assert_eq!(
        count_wal_segments(&wal_dir),
        before,
        "active WAL segment must survive gc() during an in-flight txn"
    );
    release.wait();
    writer.join().unwrap();

    assert_eq!(db.table("t").unwrap().lock().count(), 60);
    // Reopen — the committed txn is durable.
    drop(db);
    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.table("t").unwrap().lock().count(), 60);
}

#[test]
fn gc_reaps_accumulated_wal_segments_once_durable() {
    // `SharedWal::open` mints a fresh segment per reopen without truncating the
    // old ones. After the data is durable in runs, gc() reaps the rotated
    // (non-active) segments while keeping the active one — and the DB still
    // opens and reads correctly afterward.
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("_wal");
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("t", pk_schema()).unwrap();
        let tbl = db.table("t").unwrap();
        tbl.lock().set_mutable_run_spill_bytes(1);
        db.transaction(|t| t.put("t", vec![(1, Value::Int64(1))]).map(|_| ()))
            .unwrap();
        tbl.lock().flush().unwrap();
    }
    // A few reopens accumulate segments.
    for _ in 0..3 {
        let db = Database::open(dir.path()).unwrap();
        db.table("t").unwrap().lock().flush().unwrap();
        drop(db);
    }
    assert!(
        count_wal_segments(&wal_dir) > 1,
        "reopens should accumulate WAL segments"
    );

    let db = Database::open(dir.path()).unwrap();
    // Data is durable (flushed); gc reaps the rotated segments.
    db.gc().unwrap();
    assert_eq!(
        count_wal_segments(&wal_dir),
        1,
        "only the active segment remains after gc"
    );
    assert_eq!(db.table("t").unwrap().lock().count(), 1);

    // Still recoverable after reopening post-GC.
    drop(db);
    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.table("t").unwrap().lock().count(), 1);
}

#[test]
fn database_pinned_snapshot_survives_compaction() {
    // A reader pinned via `db.snapshot()` (the Database registry, not the
    // single-table pin set) must still see its version after a compaction on
    // that table — compaction's version-retention must consult the Database
    // registry, not only the table-local pins.
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();
    let tbl = db.table("t").unwrap();
    tbl.lock().set_mutable_run_spill_bytes(1);

    db.transaction(|t| t.put("t", vec![(1, Value::Int64(1))]).map(|_| ()))
        .unwrap();
    tbl.lock().flush().unwrap();
    let rid = {
        let g = tbl.lock();
        let snap = g.snapshot();
        g.visible_rows(snap).unwrap()[0].row_id
    };

    // Pin a snapshot that sees the live row, then delete + flush a tombstone.
    let (snap_pinned, guard) = db.snapshot();
    db.transaction(|t| t.delete("t", rid)).unwrap();
    tbl.lock().flush().unwrap();

    // Compact with the Database snapshot still pinned.
    tbl.lock().compact().unwrap();

    // The pinned snapshot must still see the row; the current view must not.
    {
        let g = tbl.lock();
        assert_eq!(
            g.visible_rows(snap_pinned).unwrap().len(),
            1,
            "Database-pinned snapshot must still see its version after compaction"
        );
        assert_eq!(g.visible_rows(g.snapshot()).unwrap().len(), 0);
    }

    drop(guard);
}

#[test]
fn gc_sweeps_stale_txn_dirs() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();
    let table_id = db.table_id("t").unwrap();
    let tdir = dir.path().join("tables").join(table_id.to_string());

    // Create a stale _txn/ dir (simulating a crashed large txn).
    let stale = tdir.join("_txn").join("12345");
    std::fs::create_dir_all(&stale).unwrap();
    std::fs::write(stale.join("r-1.sr"), b"stale").unwrap();

    // GC sweeps it.
    let reclaimed = db.gc().unwrap();
    assert!(reclaimed >= 1);
    assert!(!stale.exists());
}
