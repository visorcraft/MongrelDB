//! P7.1 — check/doctor for multi-table integrity.

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
fn check_reports_missing_run_file() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(1))])?;
        Ok(())
    })
    .unwrap();
    db.table("t").unwrap().lock().flush().unwrap();

    // Force a run spill so there's a .sr file to check.
    db.table("t").unwrap().lock().set_mutable_run_spill_bytes(1);
    for i in 0..100i64 {
        db.transaction(|t| {
            t.put("t", vec![(1, Value::Int64(i))])?;
            Ok(())
        })
        .unwrap();
    }
    db.table("t").unwrap().lock().flush().unwrap();

    let table_id = db.table_id("t").unwrap();
    let tdir = dir.path().join("tables").join(table_id.to_string());

    // Find and delete a run file.
    let runs_dir = tdir.join("_runs");
    if let Ok(entries) = std::fs::read_dir(&runs_dir) {
        let run_files: Vec<_> = entries.flatten().collect();
        if let Some(first) = run_files.first() {
            std::fs::remove_file(first.path()).unwrap();
        }
    }

    // check() should report the missing run.
    let issues = db.check();
    assert!(
        issues.iter().any(|i| i.description.contains("missing run")),
        "check should report missing run file, got: {:?}",
        issues
    );

    // doctor quarantines the bad table.
    let quarantined = db.doctor().unwrap();
    assert!(quarantined.contains(&table_id), "table quarantined");

    // The quarantine dir exists.
    assert!(
        dir.path()
            .join("_quarantine")
            .join(table_id.to_string())
            .exists(),
        "quarantined table dir exists"
    );
}

#[test]
fn check_passes_on_healthy_db() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(1))])?;
        Ok(())
    })
    .unwrap();

    let issues = db.check();
    assert!(
        issues.is_empty(),
        "healthy DB has no issues, got: {:?}",
        issues
    );
}

#[test]
fn doctor_quarantines_and_db_still_opens() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("good", pk_schema()).unwrap();
        db.create_table("bad", pk_schema()).unwrap();
        db.transaction(|t| {
            t.put("good", vec![(1, Value::Int64(1))])?;
            Ok(())
        })
        .unwrap();

        // Corrupt the "bad" table by removing its manifest.
        let bad_id = db.table_id("bad").unwrap();
        let manifest_path = dir
            .path()
            .join("tables")
            .join(bad_id.to_string())
            .join("_mf");
        if manifest_path.exists() {
            std::fs::write(&manifest_path, b"corrupted").unwrap();
        }

        let issues = db.check();
        assert!(
            issues.iter().any(|i| i.table_name == "bad"),
            "check reports bad table"
        );

        db.doctor().unwrap();
    }

    // Reopen — DB still works, "good" table is accessible, "bad" is gone.
    let db = Database::open(dir.path()).unwrap();
    assert!(db.table_names().iter().any(|n| n == "good"));
    assert!(!db.table_names().iter().any(|n| n == "bad"));
    assert_eq!(db.table("good").unwrap().lock().count(), 1);
}

#[test]
fn import_single_table_into_database() {
    use mongreldb_core::Table;

    // Create an "old" single-table directory with data.
    let old_dir = tempdir().unwrap();
    {
        let mut old = Table::create(old_dir.path(), pk_schema(), 1).unwrap();
        for i in 0..50i64 {
            old.put(vec![(1, Value::Int64(i))]).unwrap();
        }
        old.commit().unwrap();
        old.flush().unwrap();
    }

    // Import into a new Database.
    let new_dir = tempdir().unwrap();
    let db = Database::create(new_dir.path()).unwrap();
    db.create_table("imported", pk_schema()).unwrap();

    // Read all rows from the old table and insert them.
    {
        let old = Table::open(old_dir.path()).unwrap();
        let snap = old.snapshot();
        let rows = old.visible_rows(snap).unwrap();
        drop(old);

        for row in rows {
            let cells: Vec<(u16, Value)> = row
                .columns
                .iter()
                .map(|(&cid, v)| (cid, v.clone()))
                .collect();
            db.transaction(|t| {
                t.put("imported", cells)?;
                Ok(())
            })
            .unwrap();
        }
    }

    assert_eq!(db.table("imported").unwrap().lock().count(), 50);
}
