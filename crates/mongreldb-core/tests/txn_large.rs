//! P3.4 — unbounded transactions via quarantined uniform-epoch spill runs.

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
