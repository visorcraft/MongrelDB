//! P1.4 — multi-table `Database` shares one epoch clock, caches, and snapshot
//! registry across tables; reopen sees every table.

use mongreldb_core::{schema::*, Database, Epoch, MongrelError, Value};
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

fn orders_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
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

fn items_schema() -> Schema {
    Schema {
        schema_id: 2,
        columns: vec![ColumnDef {
            id: 1,
            name: "sku".into(),
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

fn assert_existing_database_error(err: MongrelError) {
    match err {
        MongrelError::InvalidArgument(msg) => {
            assert!(msg.contains("database already exists"), "got: {msg}")
        }
        other => panic!("expected existing database error, got {other:?}"),
    }
}

#[test]
fn database_creates_tables_and_shares_one_clock() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let _o = db.create_table("orders", orders_schema()).unwrap();
    let _i = db.create_table("items", items_schema()).unwrap();
    assert_eq!(db.table_names().len(), 2);

    let e_pre = db.snapshot().0.epoch;
    db.table("orders")
        .unwrap()
        .lock()
        .put(vec![(1, Value::Int64(1))])
        .unwrap();
    let e_orders = db.table("orders").unwrap().lock().commit().unwrap();
    db.table("items")
        .unwrap()
        .lock()
        .put(vec![(1, Value::Int64(2))])
        .unwrap();
    let e_items = db.table("items").unwrap().lock().commit().unwrap();
    assert!(e_orders.0 > e_pre.0);
    assert!(
        e_items.0 > e_orders.0,
        "items epoch must exceed orders epoch"
    );

    drop(db);
    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.table_names().len(), 2);
    let _t = db.table("orders").unwrap();
    let _t = db.table("items").unwrap();
}

#[test]
fn close_flushes_and_reopens_committed_rows() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", items_schema()).unwrap();
    db.transaction(|transaction| transaction.put("items", vec![(1, Value::Int64(7))]))
        .unwrap();

    db.close().unwrap();
    drop(db);

    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.table("items").unwrap().lock().count(), 1);
}

#[test]
fn open_missing_database_returns_not_found() {
    let dir = tempdir().unwrap();

    assert!(matches!(
        Database::open(dir.path().join("missing")),
        Err(MongrelError::NotFound(_))
    ));
}

#[test]
fn create_refuses_existing_database_without_replacing_catalog() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("orders", orders_schema()).unwrap();
    }

    assert_existing_database_error(Database::create(dir.path()).unwrap_err());
    assert_existing_database_error(
        Database::create_with_credentials(dir.path(), "admin", "pw").unwrap_err(),
    );

    let db = Database::open(dir.path()).unwrap();
    assert!(db.table_names().iter().any(|name| name == "orders"));
    assert!(!db.require_auth_enabled());
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_create_refuses_existing_database_without_rewriting_keys() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create_encrypted(dir.path(), "right").unwrap();
        db.create_table("orders", orders_schema()).unwrap();
    }
    let key_path = dir.path().join("_meta").join("keys");
    let keys_before = std::fs::read(&key_path).unwrap();

    assert_existing_database_error(Database::create_encrypted(dir.path(), "wrong").unwrap_err());

    assert_eq!(std::fs::read(&key_path).unwrap(), keys_before);
    let db = Database::open_encrypted(dir.path(), "right").unwrap();
    assert!(db.table_names().iter().any(|name| name == "orders"));
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_large_schema_catalog_reopens() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create_encrypted(dir.path(), "right").unwrap();
        for index in 0..128 {
            db.create_table(
                &format!("table_{index:03}_{}", "catalog".repeat(12)),
                orders_schema(),
            )
            .unwrap();
        }
    }

    let reopened = Database::open_encrypted(dir.path(), "right").unwrap();
    assert_eq!(reopened.table_names().len(), 128);
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_credentialed_create_refuses_existing_database_without_rewriting_keys() {
    let dir = tempdir().unwrap();
    {
        let db =
            Database::create_encrypted_with_credentials(dir.path(), "right", "admin", "s3cret")
                .unwrap();
        db.create_table("orders", orders_schema()).unwrap();
    }
    let key_path = dir.path().join("_meta").join("keys");
    let keys_before = std::fs::read(&key_path).unwrap();

    assert_existing_database_error(
        Database::create_encrypted_with_credentials(dir.path(), "wrong", "admin", "pw")
            .unwrap_err(),
    );

    assert_eq!(std::fs::read(&key_path).unwrap(), keys_before);
    let db =
        Database::open_encrypted_with_credentials(dir.path(), "right", "admin", "s3cret").unwrap();
    assert!(db.table_names().iter().any(|name| name == "orders"));
}

#[cfg(feature = "encryption")]
#[test]
fn database_encrypted_opens_reject_malformed_salt_without_panicking() {
    for credentialed in [false, true] {
        for malformed in [
            Vec::new(),
            vec![0; mongreldb_core::encryption::SALT_LEN + 1],
        ] {
            let dir = tempdir().unwrap();
            if credentialed {
                drop(
                    Database::create_encrypted_with_credentials(
                        dir.path(),
                        "passphrase",
                        "admin",
                        "password",
                    )
                    .unwrap(),
                );
            } else {
                drop(Database::create_encrypted(dir.path(), "passphrase").unwrap());
            }
            std::fs::write(dir.path().join("_meta/keys"), malformed).unwrap();
            let error = if credentialed {
                Database::open_encrypted_with_credentials(
                    dir.path(),
                    "passphrase",
                    "admin",
                    "password",
                )
                .unwrap_err()
            } else {
                Database::open_encrypted(dir.path(), "passphrase").unwrap_err()
            };
            assert!(matches!(error, MongrelError::Encryption(_)));
        }
    }
}

#[test]
fn database_drop_table_removes_it() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", orders_schema()).unwrap();
    db.create_table("b", items_schema()).unwrap();
    assert_eq!(db.table_names().len(), 2);
    db.drop_table("a").unwrap();
    assert_eq!(db.table_names().len(), 1);
    assert!(db.table("a").is_err());
    // reopen still sees only b
    drop(db);
    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.table_names().len(), 1);
}

#[test]
fn database_snapshot_is_retained() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("orders", orders_schema()).unwrap();
    db.table("orders")
        .unwrap()
        .lock()
        .put(vec![(1, Value::Int64(1))])
        .unwrap();
    db.table("orders").unwrap().lock().commit().unwrap();
    let (snap, _guard) = db.snapshot();
    // create_table (DDL) advances the epoch to 1, then the put+commit to 2.
    assert_eq!(snap.epoch, Epoch(2));
}

#[test]
fn concurrent_cross_table_commits_publish_in_order() {
    // Two tables committing concurrently on the shared epoch authority must
    // never publish `visible` past an unpublished lower epoch: every committed
    // epoch is durable and visible before a higher one becomes visible.
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("a", orders_schema()).unwrap();
    db.create_table("b", items_schema()).unwrap();

    let n = 200usize;
    let mut handles = Vec::new();
    for name in ["a", "b"] {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 0..n {
                let h = db.table(name).unwrap();
                let mut t = h.lock();
                t.put(vec![(1, Value::Int64(i as i64))]).unwrap();
                let committed = t.commit().unwrap();
                let vis = db.visible_epoch();
                assert!(
                    vis.0 >= committed.0,
                    "visible {vis:?} < committed {committed:?} (unpublished)"
                );
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    // 2 create_table DDL commits + 2*n data commits advance the shared clock.
    assert_eq!(db.visible_epoch(), Epoch(2 + (2 * n) as u64));
}
