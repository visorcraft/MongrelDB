//! P3.6 — retention-gated GC for dropped tables and pending runs.

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
