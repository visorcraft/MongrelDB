//! Database file locking rejects independent handles in every process.

use mongreldb_core::Database;
use tempfile::tempdir;

#[test]
fn same_process_second_live_open_is_rejected() {
    let dir = tempdir().unwrap();
    let _db = Database::create(dir.path()).unwrap();
    let error = match Database::open(dir.path()) {
        Ok(_) => panic!("second live open unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("already open in this process"));
}

#[test]
fn open_after_drop_succeeds() {
    let dir = tempdir().unwrap();
    {
        let _db = Database::create(dir.path()).unwrap();
    } // db dropped → lock released
      // Now a fresh open should succeed.
    let _db2 = Database::open(dir.path()).unwrap();
}

#[cfg(unix)]
#[test]
fn open_pins_canonical_root_when_alias_is_replaced() {
    use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
    use std::os::unix::fs::symlink;

    let parent = tempdir().unwrap();
    let original = parent.path().join("original");
    let replacement = parent.path().join("replacement");
    drop(Database::create(&original).unwrap());
    drop(Database::create(&replacement).unwrap());
    let alias = parent.path().join("alias");
    symlink(&original, &alias).unwrap();

    let database = Database::open(&alias).unwrap();
    assert_eq!(database.root(), original.canonicalize().unwrap());
    std::fs::remove_file(&alias).unwrap();
    symlink(&replacement, &alias).unwrap();

    database
        .create_table(
            "pinned",
            Schema {
                columns: vec![ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                }],
                ..Schema::default()
            },
        )
        .unwrap();
    database.replication_snapshot().unwrap();
    let backup = parent.path().join("backup");
    database.hot_backup(&backup).unwrap();
    drop(database);

    assert!(Database::open(&original).unwrap().table("pinned").is_ok());
    assert!(Database::open(&replacement)
        .unwrap()
        .table("pinned")
        .is_err());
    mongreldb_core::backup::verify_backup(&backup).unwrap();
}

#[cfg(unix)]
#[test]
fn durable_extension_state_stays_on_pinned_root_after_root_swap() {
    use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
    use mongreldb_core::Value;

    let parent = tempdir().unwrap();
    let original = parent.path().join("database");
    let moved = parent.path().join("moved-database");
    let database = Database::create(&original).unwrap();
    let durable = database.durable_root();

    std::fs::rename(&original, &moved).unwrap();
    std::fs::create_dir(&original).unwrap();
    durable.create_directory_all("_server").unwrap();
    durable.write_new("_server/receipt", b"pinned").unwrap();
    database
        .create_table(
            "items",
            Schema {
                columns: vec![ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                }],
                ..Schema::default()
            },
        )
        .unwrap();
    database
        .transaction(|transaction| {
            transaction.put("items", vec![(1, Value::Int64(1))])?;
            Ok(())
        })
        .unwrap();
    database.replication_snapshot().unwrap();
    assert!(database.doctor().unwrap().is_empty());
    let backup = parent.path().join("backup");
    database.hot_backup(&backup).unwrap();

    assert_eq!(
        std::fs::read(moved.join("_server/receipt")).unwrap(),
        b"pinned"
    );
    assert!(!original.join("_server/receipt").exists());
    assert!(!original.join("tables").exists());
    mongreldb_core::backup::verify_backup(&backup).unwrap();
    drop(database);
    assert_eq!(
        Database::open(&moved)
            .unwrap()
            .table("items")
            .unwrap()
            .lock()
            .count(),
        1
    );
}
