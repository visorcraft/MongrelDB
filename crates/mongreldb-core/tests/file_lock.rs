//! Database file locking rejects independent handles in every process.

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Database, MongrelError, OpenOptions, Value};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use tempfile::tempdir;

fn id_schema() -> Schema {
    Schema {
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
            embedding_source: None,
        }],
        ..Schema::default()
    }
}

fn tree_bytes(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    fn visit(root: &Path, directory: &Path, files: &mut Vec<(PathBuf, Vec<u8>)>) {
        let mut entries = std::fs::read_dir(directory)
            .unwrap()
            .collect::<std::io::Result<Vec<_>>>()
            .unwrap();
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                visit(root, &path, files);
            } else {
                let relative = path.strip_prefix(root).unwrap();
                if relative == Path::new("_meta").join(".lock") {
                    continue;
                }
                files.push((relative.to_path_buf(), std::fs::read(path).unwrap()));
            }
        }
    }

    let mut files = Vec::new();
    visit(root, root, &mut files);
    files
}

fn assert_one_concurrent_open(
    open: Arc<dyn Fn() -> mongreldb_core::Result<Database> + Send + Sync>,
) {
    let start = Arc::new(Barrier::new(8));
    let release = Arc::new(Barrier::new(9));
    let (send, receive) = std::sync::mpsc::channel();
    let mut threads = Vec::new();
    for _ in 0..8 {
        let open = Arc::clone(&open);
        let start = Arc::clone(&start);
        let release = Arc::clone(&release);
        let send = send.clone();
        threads.push(std::thread::spawn(move || {
            start.wait();
            let database = open();
            send.send(database.is_ok()).unwrap();
            release.wait();
            database
        }));
    }
    drop(send);
    let winners = (0..8)
        .map(|_| receive.recv().unwrap())
        .filter(|winner| *winner)
        .count();
    assert_eq!(winners, 1);
    release.wait();
    for thread in threads {
        match thread.join().unwrap() {
            Ok(_) => {}
            Err(error) => assert!(matches!(error, MongrelError::DatabaseLocked { .. })),
        }
    }
}

#[test]
fn same_process_second_live_open_is_rejected() {
    let dir = tempdir().unwrap();
    let _db = Database::create(dir.path()).unwrap();
    let error = match Database::open(dir.path()) {
        Ok(_) => panic!("second live open unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(matches!(error, MongrelError::DatabaseLocked { .. }));
    assert!(error
        .to_string()
        .contains("reuse the existing Arc<Database>"));
}

#[test]
fn same_process_rejection_ignores_lock_timeout_and_mutates_nothing() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path()).unwrap();
    database.create_table("items", id_schema()).unwrap();
    database
        .transaction(|transaction| {
            transaction.put("items", vec![(1, Value::Int64(1))])?;
            Ok(())
        })
        .unwrap();
    let before = tree_bytes(dir.path());
    let metrics_before = Database::open_metrics();
    let started = std::time::Instant::now();
    let error = Database::open_with_options(
        dir.path(),
        OpenOptions::default().with_lock_timeout_ms(5_000),
    )
    .unwrap_err();
    assert!(matches!(error, MongrelError::DatabaseLocked { .. }));
    assert!(Database::open_metrics().failures > metrics_before.failures);
    assert!(started.elapsed() < std::time::Duration::from_millis(500));
    assert_eq!(tree_bytes(dir.path()), before);

    database
        .transaction(|transaction| {
            transaction.put("items", vec![(1, Value::Int64(2))])?;
            Ok(())
        })
        .unwrap();
    database.table("items").unwrap().lock().flush().unwrap();
    assert!(database.doctor().unwrap().is_empty());
    drop(database);

    let reopened = Database::open(dir.path()).unwrap();
    assert!(reopened.check().is_empty());
    assert!(reopened.doctor().unwrap().is_empty());
    assert_eq!(reopened.table("items").unwrap().lock().count(), 2);
}

#[test]
fn absolute_relative_and_dotted_aliases_are_rejected() {
    let current = std::env::current_dir().unwrap();
    let parent = tempfile::Builder::new()
        .prefix("mongreldb-open-alias-")
        .tempdir_in(&current)
        .unwrap();
    let root = parent.path().join("database");
    let child = parent.path().join("child");
    std::fs::create_dir(&child).unwrap();
    let database = Database::create(&root).unwrap();
    let relative = root.strip_prefix(&current).unwrap();
    assert!(matches!(
        Database::open(relative),
        Err(MongrelError::DatabaseLocked { .. })
    ));
    let dotted = child.join("..").join("database");
    assert!(matches!(
        Database::open(dotted),
        Err(MongrelError::DatabaseLocked { .. })
    ));
    drop(database);
}

#[test]
fn concurrent_same_process_open_has_one_winner() {
    let dir = tempdir().unwrap();
    drop(Database::create(dir.path()).unwrap());
    let root = dir.path().to_path_buf();
    let open_root = root.clone();
    assert_one_concurrent_open(Arc::new(move || Database::open(&open_root)));
    let reopened = Database::open(root).unwrap();
    assert!(reopened.check().is_empty());
}

#[test]
fn concurrent_credentialed_open_has_one_winner() {
    let dir = tempdir().unwrap();
    drop(Database::create_with_credentials(dir.path(), "admin", "secret").unwrap());
    let root = dir.path().to_path_buf();
    assert_one_concurrent_open(Arc::new(move || {
        Database::open_with_credentials(&root, "admin", "secret")
    }));
}

#[test]
fn concurrent_encrypted_open_has_one_winner() {
    let dir = tempdir().unwrap();
    drop(Database::create_encrypted(dir.path(), "secret").unwrap());
    let root = dir.path().to_path_buf();
    assert_one_concurrent_open(Arc::new(move || Database::open_encrypted(&root, "secret")));
}

#[test]
fn concurrent_create_has_one_winner() {
    let parent = tempdir().unwrap();
    let root = parent.path().join("database");
    let create_root = root.clone();
    assert_one_concurrent_open(Arc::new(move || Database::create(&create_root)));
    let reopened = Database::open(root).unwrap();
    assert!(reopened.check().is_empty());
}

#[test]
fn failed_create_releases_intended_path_reservation() {
    let dir = tempdir().unwrap();
    drop(Database::create(dir.path()).unwrap());
    assert!(matches!(
        Database::create(dir.path()),
        Err(MongrelError::InvalidArgument(_))
    ));
    Database::open(dir.path()).unwrap();
}

#[test]
fn create_racing_open_never_constructs_two_cores() {
    let parent = tempdir().unwrap();
    let root = parent.path().join("database");
    let start = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(3));
    let (send, receive) = std::sync::mpsc::channel();
    let mut threads = Vec::new();
    for create in [true, false] {
        let root = root.clone();
        let start = Arc::clone(&start);
        let release = Arc::clone(&release);
        let send = send.clone();
        threads.push(std::thread::spawn(move || {
            start.wait();
            let database = if create {
                Database::create(&root)
            } else {
                Database::open(&root)
            };
            send.send(database.is_ok()).unwrap();
            release.wait();
            database
        }));
    }
    drop(send);
    let successes = (0..2)
        .map(|_| receive.recv().unwrap())
        .filter(|success| *success)
        .count();
    assert_eq!(successes, 1);
    release.wait();
    for thread in threads {
        let _ = thread.join().unwrap();
    }
    Database::open(root).unwrap();
}

#[test]
fn arc_clones_share_one_core_and_shutdown_requires_final_owner() {
    let dir = tempdir().unwrap();
    let database = Arc::new(Database::create(dir.path()).unwrap());
    let worker = Arc::clone(&database);
    assert!(matches!(
        Arc::clone(&database).shutdown(),
        Err(MongrelError::DatabaseBusy { .. })
    ));
    let before_clone_drop = tree_bytes(dir.path());
    drop(worker);
    assert_eq!(tree_bytes(dir.path()), before_clone_drop);
    database.create_table("items", id_schema()).unwrap();
    let mut writers = Vec::new();
    for value in 1..=4 {
        let database = Arc::clone(&database);
        writers.push(std::thread::spawn(move || {
            database.transaction(|transaction| {
                transaction.put("items", vec![(1, Value::Int64(value))])?;
                Ok(())
            })
        }));
    }
    for writer in writers {
        writer.join().unwrap().unwrap();
    }
    database.shutdown().unwrap();

    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.table("items").unwrap().lock().count(), 4);
    assert!(reopened.doctor().unwrap().is_empty());
}

#[test]
fn failed_credentialed_open_releases_reservation() {
    let dir = tempdir().unwrap();
    drop(Database::create_with_credentials(dir.path(), "admin", "correct").unwrap());
    assert!(matches!(
        Database::open_with_credentials(dir.path(), "admin", "wrong"),
        Err(MongrelError::InvalidCredentials { .. })
    ));
    Database::open_with_credentials(dir.path(), "admin", "correct").unwrap();
}

#[test]
fn failed_encrypted_open_releases_reservation() {
    let dir = tempdir().unwrap();
    drop(Database::create_encrypted(dir.path(), "correct").unwrap());
    assert!(Database::open_encrypted(dir.path(), "wrong").is_err());
    Database::open_encrypted(dir.path(), "correct").unwrap();
}

#[test]
fn failed_raw_key_open_releases_reservation() {
    let dir = tempdir().unwrap();
    let correct = [7_u8; 32];
    drop(Database::create_with_key(dir.path(), &correct).unwrap());
    assert!(Database::open_with_key(dir.path(), &[9_u8; 32]).is_err());
    Database::open_with_key(dir.path(), &correct).unwrap();
}

#[test]
fn corrupt_catalog_failure_releases_reservation() {
    let dir = tempdir().unwrap();
    drop(Database::create(dir.path()).unwrap());
    let catalog = dir.path().join(mongreldb_core::catalog::CATALOG_FILENAME);
    let valid = std::fs::read(&catalog).unwrap();
    std::fs::write(&catalog, b"corrupt").unwrap();
    let first = Database::open(dir.path()).unwrap_err();
    let second = Database::open(dir.path()).unwrap_err();
    assert!(!matches!(first, MongrelError::DatabaseLocked { .. }));
    assert!(!matches!(second, MongrelError::DatabaseLocked { .. }));
    std::fs::write(catalog, valid).unwrap();
    Database::open(dir.path()).unwrap();
}

#[test]
fn table_mount_failure_releases_reservation() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path()).unwrap();
    let table_id = database.create_table("items", id_schema()).unwrap();
    drop(database);
    let table = dir.path().join("tables").join(table_id.to_string());
    let hidden = dir.path().join("hidden-table");
    std::fs::rename(&table, &hidden).unwrap();
    let first = Database::open(dir.path()).unwrap_err();
    let second = Database::open(dir.path()).unwrap_err();
    assert!(!matches!(first, MongrelError::DatabaseLocked { .. }));
    assert!(!matches!(second, MongrelError::DatabaseLocked { .. }));
    std::fs::rename(hidden, table).unwrap();
    Database::open(dir.path()).unwrap();
}

#[test]
fn missing_metadata_failure_releases_reservation() {
    let dir = tempdir().unwrap();
    assert!(matches!(
        Database::open(dir.path()),
        Err(MongrelError::NotFound(_))
    ));
    assert!(matches!(
        Database::open(dir.path()),
        Err(MongrelError::NotFound(_))
    ));
    Database::create(dir.path()).unwrap();
}

#[cfg(windows)]
#[test]
fn windows_case_alias_is_rejected_while_original_is_open() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path()).unwrap();
    let name = dir.path().file_name().unwrap().to_string_lossy();
    let alias_name = name
        .chars()
        .map(|character| {
            if character.is_ascii_lowercase() {
                character.to_ascii_uppercase()
            } else {
                character.to_ascii_lowercase()
            }
        })
        .collect::<String>();
    let alias = dir.path().parent().unwrap().join(alias_name);
    assert!(matches!(
        Database::open(alias),
        Err(MongrelError::DatabaseLocked { .. })
    ));
    drop(database);
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
                    embedding_source: None,
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
fn symlink_alias_is_rejected_while_original_is_open() {
    use std::os::unix::fs::symlink;

    let parent = tempdir().unwrap();
    let root = parent.path().join("database");
    let alias = parent.path().join("alias");
    let database = Database::create(&root).unwrap();
    symlink(&root, &alias).unwrap();
    assert!(matches!(
        Database::open(&alias),
        Err(MongrelError::DatabaseLocked { .. })
    ));
    drop(database);
    Database::open(alias).unwrap();
}

#[cfg(unix)]
#[test]
fn symlinked_parent_alias_is_rejected_while_original_is_open() {
    use std::os::unix::fs::symlink;

    let parent = tempdir().unwrap();
    let real_parent = parent.path().join("real");
    let alias_parent = parent.path().join("alias");
    std::fs::create_dir(&real_parent).unwrap();
    symlink(&real_parent, &alias_parent).unwrap();
    let root = real_parent.join("database");
    let database = Database::create(&root).unwrap();
    assert!(matches!(
        Database::open(alias_parent.join("database")),
        Err(MongrelError::DatabaseLocked { .. })
    ));
    drop(database);
    Database::open(alias_parent.join("database")).unwrap();
}

#[cfg(unix)]
#[test]
fn inherited_database_fails_closed_after_fork() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path()).unwrap();
    database.create_table("items", id_schema()).unwrap();

    let child = unsafe { libc::fork() };
    assert!(child >= 0, "fork failed");
    if child == 0 {
        let failed_closed = matches!(
            database.table("items"),
            Err(MongrelError::ForkedProcess { .. })
        );
        unsafe { libc::_exit(i32::from(!failed_closed)) };
    }

    let mut status = 0;
    assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);
    assert!(libc::WIFEXITED(status));
    assert_eq!(libc::WEXITSTATUS(status), 0);
    assert!(database.table("items").is_ok());
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
                    embedding_source: None,
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
