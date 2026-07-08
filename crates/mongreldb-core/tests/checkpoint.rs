//! Tests for `Database::checkpoint()` — deterministic-stable byte image.
//!
//! After checkpoint:
//!   - All data is durable in sorted runs (no memtable data)
//!   - Each table has exactly one sorted run (compacted)
//!   - At most one WAL segment exists (the fresh active segment)
//!   - The active WAL segment has 0 records (just the header)
//!   - Reopening yields the same data

use mongreldb_core::{schema::*, Database, Value};
use tempfile::tempdir;

fn make_schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
            },
            ColumnDef {
                id: 2,
                name: "name".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

#[test]
fn checkpoint_produces_stable_directory() {
    let dir = tempdir().unwrap();

    // Create + populate
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", make_schema()).unwrap();

    let handle = db.table("items").unwrap();
    {
        let mut t = handle.lock();
        t.put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"alice".to_vec())),
        ])
        .unwrap();
        t.put(vec![
            (1, Value::Int64(2)),
            (2, Value::Bytes(b"bob".to_vec())),
        ])
        .unwrap();
        t.put(vec![
            (1, Value::Int64(3)),
            (2, Value::Bytes(b"carol".to_vec())),
        ])
        .unwrap();
        t.commit().unwrap();
    }

    // Verify data is visible
    assert_eq!(handle.lock().count(), 3);

    // Snapshot
    db.checkpoint().unwrap();

    // Verify: no more than one WAL segment, and it should be header-only
    let wal_dir = dir.path().join("_wal");
    let segments: Vec<_> = std::fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
        .collect();
    assert!(
        segments.len() <= 1,
        "expected at most 1 WAL segment after checkpoint, found {}",
        segments.len()
    );
    if let Some(seg) = segments.first() {
        let size = seg.metadata().unwrap().len();
        assert!(
            size < 1024,
            "WAL segment should be header-only after checkpoint, got {} bytes",
            size
        );
    }

    // Verify: each table has exactly one sorted run
    let tables_dir = dir.path().join("tables");
    for entry in std::fs::read_dir(&tables_dir).unwrap() {
        let entry = entry.unwrap();
        let runs_dir = entry.path().join("_runs");
        if runs_dir.exists() {
            let runs: Vec<_> = std::fs::read_dir(&runs_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().is_some_and(|ext| ext == "sr"))
                .collect();
            assert_eq!(
                runs.len(),
                1,
                "expected exactly 1 sorted run per table after checkpoint"
            );
        }
    }
}

#[test]
fn checkpoint_preserves_data_after_reopen() {
    let dir = tempdir().unwrap();

    // Create + populate
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("data", make_schema()).unwrap();
        let handle = db.table("data").unwrap();
        {
            let mut t = handle.lock();
            t.put(vec![
                (1, Value::Int64(42)),
                (2, Value::Bytes(b"answer".to_vec())),
            ])
            .unwrap();
            t.commit().unwrap();
        }
        db.checkpoint().unwrap();
    }
    // Database handle dropped (lock released)

    // Reopen and verify data
    let db = Database::open(dir.path()).unwrap();
    let handle = db.table("data").unwrap();
    let mut g = handle.lock();

    assert_eq!(g.count(), 1, "count should include all rows");

    // Query PK = 42
    use mongreldb_core::query::{Condition, Query};
    let key = Value::Int64(42).encode_key();
    let mut q = Query::new();
    q = q.and(Condition::Pk(key));
    let rows = g.query(&q).unwrap();
    assert_eq!(rows.len(), 1, "PK query should find the row");
}

#[test]
fn snapshot_is_idempotent() {
    let dir = tempdir().unwrap();

    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", make_schema()).unwrap();
    let handle = db.table("t").unwrap();
    {
        let mut t = handle.lock();
        t.put(vec![(1, Value::Int64(1)), (2, Value::Bytes(b"x".to_vec()))])
            .unwrap();
        t.commit().unwrap();
    }

    // First checkpoint
    db.checkpoint().unwrap();

    // Snapshot the directory listing
    let listing1: Vec<String> = list_all_files(dir.path());

    // Second checkpoint (should be a no-op or produce the same files)
    db.checkpoint().unwrap();

    let listing2: Vec<String> = list_all_files(dir.path());

    // The set of files should be the same (segment numbers may differ
    // if rotation happened, but count should match).
    assert_eq!(
        listing1.len(),
        listing2.len(),
        "directory file count should be stable after repeated checkpoints"
    );
}

fn list_all_files(root: &std::path::Path) -> Vec<String> {
    let mut files = Vec::new();
    if root.is_dir() {
        for entry in std::fs::read_dir(root).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                files.extend(list_all_files(&path));
            } else {
                files.push(
                    path.strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    files.sort();
    files
}
