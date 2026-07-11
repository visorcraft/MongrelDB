use mongreldb_core::{verify_backup, ColumnDef, ColumnFlags, Database, Schema, TypeId, Value};
use std::sync::{Arc, Barrier};
use tempfile::tempdir;

fn schema() -> Schema {
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
                name: "value".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn insert(db: &Database, id: i64, value: &str) {
    db.transaction(|transaction| {
        transaction.put(
            "items",
            vec![
                (1, Value::Int64(id)),
                (2, Value::Bytes(value.as_bytes().to_vec())),
            ],
        )?;
        Ok(())
    })
    .unwrap();
}

fn row_count(db: &Database) -> usize {
    db.table("items")
        .unwrap()
        .lock()
        .visible_rows(db.snapshot().0)
        .unwrap()
        .len()
}

#[test]
fn hot_backup_is_directly_openable_and_checksummed() {
    let source = tempdir().unwrap();
    let destination_parent = tempdir().unwrap();
    let destination = destination_parent.path().join("backup");
    let db = Database::create(source.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    insert(&db, 1, "one");

    let report = db.hot_backup(&destination).unwrap();
    assert_eq!(report.destination, destination.canonicalize().unwrap());
    assert!(report.files > 0);
    let manifest = verify_backup(&destination).unwrap();
    assert_eq!(manifest.epoch, report.epoch);
    assert_eq!(manifest.total_bytes(), report.bytes);

    let restored = Database::open(&destination).unwrap();
    assert_eq!(row_count(&restored), 1);
    drop(restored);

    let schema_path = destination.join("tables/0/schema.json");
    std::fs::write(&schema_path, b"corrupt").unwrap();
    assert!(verify_backup(&destination).is_err());
}

#[test]
fn backup_run_pin_survives_concurrent_compaction_and_gc() {
    let source = tempdir().unwrap();
    let destination_parent = tempdir().unwrap();
    let destination = destination_parent.path().join("backup");
    let db = Arc::new(Database::create(source.path()).unwrap());
    db.create_table("items", schema()).unwrap();
    db.table("items")
        .unwrap()
        .lock()
        .set_mutable_run_spill_bytes(1);
    insert(&db, 1, "before");
    db.table("items").unwrap().lock().flush().unwrap();
    let old_run = source.path().join("tables/0/_runs/r-1.sr");
    assert!(old_run.is_file());

    let boundary = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    db.__set_backup_hook({
        let boundary = Arc::clone(&boundary);
        let resume = Arc::clone(&resume);
        move || {
            boundary.wait();
            resume.wait();
        }
    });

    let backup_db = Arc::clone(&db);
    let backup_destination = destination.clone();
    let backup = std::thread::spawn(move || backup_db.hot_backup(backup_destination));
    boundary.wait();

    insert(&db, 2, "after");
    {
        let handle = db.table("items").unwrap();
        let mut table = handle.lock();
        table.flush().unwrap();
        table.compact().unwrap();
    }
    db.gc().unwrap();
    assert!(old_run.is_file(), "active backup must pin retired run");
    resume.wait();

    backup.join().unwrap().unwrap();
    let restored = Database::open(&destination).unwrap();
    assert_eq!(row_count(&restored), 1, "backup is fixed at boundary");
    drop(restored);

    db.gc().unwrap();
    assert!(!old_run.exists(), "pin releases after backup install");
}

#[test]
fn backup_rejects_existing_or_nested_destination() {
    let source = tempdir().unwrap();
    let destination_parent = tempdir().unwrap();
    let db = Database::create(source.path()).unwrap();
    let existing = destination_parent.path().join("existing");
    std::fs::create_dir(&existing).unwrap();
    assert!(db.hot_backup(&existing).is_err());
    assert!(db.hot_backup(source.path().join("nested")).is_err());
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_hot_backup_reopens_with_same_passphrase() {
    let source = tempdir().unwrap();
    let destination_parent = tempdir().unwrap();
    let destination = destination_parent.path().join("backup");
    let db = Database::create_encrypted(source.path(), "correct horse").unwrap();
    db.create_table("items", schema()).unwrap();
    insert(&db, 1, "secret");
    db.hot_backup(&destination).unwrap();
    drop(db);

    assert!(Database::open_encrypted(&destination, "wrong").is_err());
    let restored = Database::open_encrypted(&destination, "correct horse").unwrap();
    assert_eq!(row_count(&restored), 1);
}
