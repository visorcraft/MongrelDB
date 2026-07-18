//! Tests for `Database::checkpoint()` — deterministic-stable byte image.
//!
//! After checkpoint:
//!   - All data is durable in sorted runs (no memtable data)
//!   - Each table has exactly one sorted run (compacted)
//!   - At most one WAL segment exists (the fresh active segment)
//!   - The active WAL segment has 0 records (just the header)
//!   - Reopening yields the same data

use mongreldb_core::{schema::*, Database, Value};
use std::sync::{mpsc, Arc};
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
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "name".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
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
fn commit_after_checkpoint_uses_the_new_durable_wal() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("data", make_schema()).unwrap();
        db.transaction(|transaction| {
            transaction.put(
                "data",
                vec![(1, Value::Int64(1)), (2, Value::Bytes(b"before".to_vec()))],
            )
        })
        .unwrap();
        db.checkpoint().unwrap();
        db.transaction(|transaction| {
            transaction.put(
                "data",
                vec![(1, Value::Int64(2)), (2, Value::Bytes(b"after".to_vec()))],
            )
        })
        .unwrap();
    }

    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.table("data").unwrap().lock().count(), 2);
}

#[test]
fn checkpoint_blocks_commit_until_wal_reset_finishes() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("data", make_schema()).unwrap();
    db.transaction(|transaction| {
        transaction.put(
            "data",
            vec![(1, Value::Int64(1)), (2, Value::Bytes(b"before".to_vec()))],
        )
    })
    .unwrap();

    let (at_reset_tx, at_reset_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let checkpoint_db = Arc::clone(&db);
    let checkpoint = std::thread::spawn(move || {
        checkpoint_db.checkpoint_controlled(|| {
            at_reset_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok(())
        })
    });
    at_reset_rx.recv().unwrap();

    let (commit_started_tx, commit_started_rx) = mpsc::channel();
    let (commit_done_tx, commit_done_rx) = mpsc::channel();
    let commit_db = Arc::clone(&db);
    let commit = std::thread::spawn(move || {
        commit_started_tx.send(()).unwrap();
        let result = commit_db.transaction(|transaction| {
            transaction.put(
                "data",
                vec![(1, Value::Int64(2)), (2, Value::Bytes(b"after".to_vec()))],
            )
        });
        commit_done_tx.send(result).unwrap();
    });
    commit_started_rx.recv().unwrap();
    assert!(commit_done_rx
        .recv_timeout(std::time::Duration::from_millis(50))
        .is_err());

    release_tx.send(()).unwrap();
    checkpoint.join().unwrap().unwrap();
    commit_done_rx.recv().unwrap().unwrap();
    commit.join().unwrap();
    drop(db);

    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.table("data").unwrap().lock().count(), 2);
}

#[test]
fn failed_strict_flush_keeps_old_wal_for_recovery() {
    let dir = tempdir().unwrap();
    let table_id;
    {
        let db = Database::create(dir.path()).unwrap();
        table_id = db.create_table("data", make_schema()).unwrap();
        db.transaction(|transaction| {
            transaction.put(
                "data",
                vec![(1, Value::Int64(1)), (2, Value::Bytes(b"value".to_vec()))],
            )
        })
        .unwrap();
        let manifest = dir
            .path()
            .join("tables")
            .join(table_id.to_string())
            .join("_mf");
        let saved_manifest = manifest.with_extension("saved");
        std::fs::rename(&manifest, &saved_manifest).unwrap();
        std::fs::create_dir(&manifest).unwrap();
        assert!(db.checkpoint().is_err());
        let wal_files = std::fs::read_dir(dir.path().join("_wal"))
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "wal"))
            .count();
        assert!(wal_files >= 1);
        std::fs::remove_dir(&manifest).unwrap();
        std::fs::rename(&saved_manifest, &manifest).unwrap();
    }

    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.table("data").unwrap().lock().count(), 1);
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
