//! P1.3 — DB-wide catalog checkpoint (encrypted + authenticated, dir-fsync).

use mongreldb_core::{
    catalog::{self, Catalog, CatalogEntry, TableState},
    schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId},
    Database, MongrelError,
};
use tempfile::tempdir;

fn sample_schema() -> Schema {
    Schema {
        schema_id: 7,
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
                name: "secret".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "pk".into(),
            column_id: 1,
            kind: IndexKind::Bitmap,
            predicate: None,
            options: Default::default(),
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn sample_catalog() -> Catalog {
    Catalog {
        db_epoch: 7,
        next_table_id: 3,
        next_segment_no: 4,
        tables: vec![CatalogEntry {
            table_id: 1,
            name: "orders".into(),
            schema: sample_schema(),
            state: TableState::Live,
            created_epoch: 2,
        }],
        procedures: Vec::new(),
        triggers: Vec::new(),
        external_tables: Vec::new(),
        materialized_views: Vec::new(),
        security: Default::default(),
        security_version: 0,
        users: Vec::new(),
        roles: Vec::new(),
        next_user_id: 0,
        require_auth: false,
        user_version: None,
        application_id: None,
    }
}

fn block_catalog_publish(root: &std::path::Path) -> std::path::PathBuf {
    let catalog = root.join(catalog::CATALOG_FILENAME);
    let saved = root.join("CATALOG.saved");
    std::fs::rename(&catalog, &saved).unwrap();
    std::fs::create_dir(&catalog).unwrap();
    saved
}

fn restore_catalog(root: &std::path::Path, saved: &std::path::Path) {
    let catalog = root.join(catalog::CATALOG_FILENAME);
    std::fs::remove_dir(&catalog).unwrap();
    std::fs::rename(saved, catalog).unwrap();
}

#[test]
fn catalog_roundtrips_plaintext_and_dir_fsync() {
    let dir = tempdir().unwrap();
    let cat = sample_catalog();
    catalog::write_atomic(dir.path(), &cat, None).unwrap();
    let got = catalog::read(dir.path(), None).unwrap().unwrap();
    assert_eq!(got.db_epoch, 7);
    assert_eq!(got.next_table_id, 3);
    assert_eq!(got.next_segment_no, 4);
    assert_eq!(got.tables.len(), 1);
    assert_eq!(got.tables[0].name, "orders");
    assert_eq!(got.tables[0].table_id, 1);
    assert!(matches!(got.tables[0].state, TableState::Live));
    assert_eq!(got.tables[0].schema.columns.len(), 2);
}

#[test]
fn catalog_read_returns_none_when_missing() {
    let dir = tempdir().unwrap();
    assert!(catalog::read(dir.path(), None).unwrap().is_none());
}

#[test]
fn pragma_catalog_failure_keeps_durable_runtime_state_and_recovers() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let saved_catalog = block_catalog_publish(dir.path());

    let error = db
        .set_sql_pragma_i64_with_epoch("user_version", 55)
        .unwrap_err();
    let epoch = match error {
        MongrelError::DurableCommit { epoch, .. } => epoch,
        other => panic!("expected durable commit error, got {other:?}"),
    };
    assert_eq!(db.sql_pragma_i64("user_version").unwrap(), Some(55));
    assert_eq!(db.visible_epoch().0, epoch);
    assert!(db
        .set_sql_pragma_i64_with_epoch("application_id", 7)
        .unwrap_err()
        .to_string()
        .contains("database poisoned"));

    drop(db);
    restore_catalog(dir.path(), &saved_catalog);
    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.sql_pragma_i64("user_version").unwrap(), Some(55));
    assert_eq!(reopened.visible_epoch().0, epoch);
}

#[test]
fn rename_catalog_failure_publishes_new_name_and_recovers() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("before", sample_schema()).unwrap();
    let saved_catalog = block_catalog_publish(dir.path());

    let error = db.rename_table_with_epoch("before", "after").unwrap_err();
    let epoch = match error {
        MongrelError::DurableCommit { epoch, .. } => epoch,
        other => panic!("expected durable commit error, got {other:?}"),
    };
    assert!(db.table("before").is_err());
    assert!(db.table("after").is_ok());
    assert_eq!(db.visible_epoch().0, epoch);
    assert!(db
        .create_table("blocked", sample_schema())
        .unwrap_err()
        .to_string()
        .contains("database poisoned"));

    drop(db);
    restore_catalog(dir.path(), &saved_catalog);
    let reopened = Database::open(dir.path()).unwrap();
    assert!(reopened.table("before").is_err());
    assert!(reopened.table("after").is_ok());
    assert_eq!(reopened.visible_epoch().0, epoch);
}

#[test]
fn drop_catalog_failure_unmounts_table_and_recovers() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("doomed", sample_schema()).unwrap();
    let saved_catalog = block_catalog_publish(dir.path());

    let error = db.drop_table_with_epoch("doomed").unwrap_err();
    let epoch = match error {
        MongrelError::DurableCommit { epoch, .. } => epoch,
        other => panic!("expected durable commit error, got {other:?}"),
    };
    assert!(db.table("doomed").is_err());
    assert_eq!(db.visible_epoch().0, epoch);

    drop(db);
    restore_catalog(dir.path(), &saved_catalog);
    let reopened = Database::open(dir.path()).unwrap();
    assert!(reopened.table("doomed").is_err());
    assert_eq!(reopened.visible_epoch().0, epoch);
}

#[test]
fn controlled_pragma_rejection_prevents_wal_and_catalog_publication() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let mut callbacks = 0;

    let error = db
        .set_sql_pragma_i64_with_epoch_controlled("user_version", 55, || {
            callbacks += 1;
            Err(MongrelError::Other("cancelled before commit".into()))
        })
        .unwrap_err();
    assert_eq!(callbacks, 1);
    assert!(error.to_string().contains("cancelled before commit"));
    assert_eq!(db.sql_pragma_i64("user_version").unwrap(), None);

    drop(db);
    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.sql_pragma_i64("user_version").unwrap(), None);
}

#[test]
fn controlled_rename_rejection_keeps_original_table_after_reopen() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("before", sample_schema()).unwrap();
    let mut callbacks = 0;

    let error = db
        .rename_table_with_epoch_controlled("before", "after", || {
            callbacks += 1;
            Err(MongrelError::Other("cancelled before commit".into()))
        })
        .unwrap_err();
    assert_eq!(callbacks, 1);
    assert!(error.to_string().contains("cancelled before commit"));
    assert!(db.table("before").is_ok());
    assert!(db.table("after").is_err());

    drop(db);
    let reopened = Database::open(dir.path()).unwrap();
    assert!(reopened.table("before").is_ok());
    assert!(reopened.table("after").is_err());
}

#[cfg(feature = "encryption")]
#[test]
fn catalog_encrypted_is_authenticated() {
    let dir = tempdir().unwrap();
    let dek = [9u8; 32];
    let cat = sample_catalog();
    catalog::write_atomic(dir.path(), &cat, Some(&dek)).unwrap();
    // roundtrips under the right key
    let got = catalog::read(dir.path(), Some(&dek)).unwrap().unwrap();
    assert_eq!(got.db_epoch, 7);
    // tamper a byte of the file -> read must fail auth (None), not silently parse
    let p = dir.path().join("CATALOG");
    let mut b = std::fs::read(&p).unwrap();
    let n = b.len();
    b[n / 2] ^= 0xFF;
    std::fs::write(&p, b).unwrap();
    assert!(catalog::read(dir.path(), Some(&dek)).unwrap().is_none());
}

#[cfg(feature = "encryption")]
#[test]
fn catalog_encrypted_wrong_key_returns_none() {
    let dir = tempdir().unwrap();
    let dek = [9u8; 32];
    let cat = sample_catalog();
    catalog::write_atomic(dir.path(), &cat, Some(&dek)).unwrap();
    let wrong = [0u8; 32];
    assert!(catalog::read(dir.path(), Some(&wrong)).unwrap().is_none());
}
