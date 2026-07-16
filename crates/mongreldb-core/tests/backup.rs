use mongreldb_core::{
    verify_backup, ColumnDef, ColumnFlags, Database, ExecutionControl, MongrelError, Schema,
    TypeId, Value,
};
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
fn backup_rechecks_exact_admin_before_publication() {
    let source = tempdir().unwrap();
    let destination_parent = tempdir().unwrap();
    let destination = destination_parent.path().join("backup");
    let db =
        Arc::new(Database::create_with_credentials(source.path(), "admin", "admin-pw").unwrap());
    db.create_user("rescue", "rescue-password").unwrap();
    db.set_user_admin("rescue", true).unwrap();
    db.create_table("items", schema()).unwrap();
    insert(&db, 1, "one");
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

    let worker = {
        let db = Arc::clone(&db);
        let destination = destination.clone();
        std::thread::spawn(move || db.hot_backup(destination))
    };
    boundary.wait();
    db.drop_user("admin").unwrap();
    resume.wait();

    assert!(matches!(
        worker.join().unwrap(),
        Err(MongrelError::AuthRequired)
    ));
    assert!(!destination.exists());
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

#[test]
fn controlled_backup_cancel_before_publish_leaves_no_destination_or_stage() {
    let source = tempdir().unwrap();
    let destination_parent = tempdir().unwrap();
    let destination = destination_parent.path().join("backup");
    let db = Database::create(source.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    insert(&db, 1, "one");

    let error = db
        .hot_backup_controlled(&destination, &ExecutionControl::new(None), || false)
        .unwrap_err();
    assert!(matches!(error, MongrelError::Cancelled));
    assert!(!destination.exists());
    assert!(std::fs::read_dir(destination_parent.path())
        .unwrap()
        .all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("backup-stage")));
    assert_eq!(row_count(&db), 1);
}

#[cfg(unix)]
#[test]
fn backup_rejects_nested_stage_symlink_without_writing_outside() {
    use std::os::unix::fs::symlink;

    let source = tempdir().unwrap();
    let destination_parent = tempdir().unwrap();
    let outside = tempdir().unwrap();
    let destination = destination_parent.path().join("backup");
    let db = Database::create(source.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    db.table("items")
        .unwrap()
        .lock()
        .set_mutable_run_spill_bytes(1);
    insert(&db, 1, "one");
    db.table("items").unwrap().lock().flush().unwrap();
    db.__set_backup_hook({
        let parent = destination_parent.path().to_path_buf();
        let outside = outside.path().to_path_buf();
        move || {
            let stage = std::fs::read_dir(&parent)
                .unwrap()
                .flatten()
                .find(|entry| entry.file_name().to_string_lossy().contains("backup-stage"))
                .unwrap()
                .path();
            let runs = stage.join("tables/0/_runs");
            std::fs::remove_dir(&runs).unwrap();
            symlink(&outside, runs).unwrap();
        }
    });

    assert!(db.hot_backup(&destination).is_err());
    assert!(!outside.path().join("r-1.sr").exists());
    assert!(!destination.exists());
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
