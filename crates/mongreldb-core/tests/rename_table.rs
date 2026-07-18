//! Database::rename_table — name change, conflict prevention, handle validity,
//! and crash/reopen durability.

use mongreldb_core::schema::*;
use mongreldb_core::{Database, MongrelError, Value};
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 1,
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
                name: "v".into(),
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
fn rename_updates_catalog_and_keeps_handle_valid() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", schema()).unwrap();
    db.transaction(|t| {
        t.put(
            "a",
            vec![(1, Value::Int64(7)), (2, Value::Bytes(b"x".to_vec()))],
        )
    })
    .unwrap();

    // A handle acquired before the rename is keyed by table_id, so it must
    // remain valid afterwards (the table object does not move).
    let handle = db.table("a").unwrap();
    db.rename_table("a", "b").unwrap();

    assert_eq!(db.table_names(), vec!["b".to_string()]);
    assert!(db.table("a").is_err(), "old name should no longer resolve");
    assert_eq!(
        handle.lock().count(),
        1,
        "pre-rename handle still sees the row"
    );
    assert_eq!(
        db.table("b").unwrap().lock().count(),
        1,
        "new name resolves to the same table"
    );
}

#[test]
fn rename_rejects_conflicting_target_name() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", schema()).unwrap();
    db.create_table("b", schema()).unwrap();

    let err = db.rename_table("a", "b").unwrap_err();
    assert!(
        matches!(err, MongrelError::InvalidArgument(_)),
        "expected InvalidArgument, got {err:?}"
    );
    // Neither table was affected.
    let mut names = db.table_names();
    names.sort();
    assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn rename_rejects_missing_source() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let err = db.rename_table("ghost", "x").unwrap_err();
    assert!(matches!(err, MongrelError::NotFound(_)), "got {err:?}");
}

#[test]
fn rename_same_name_is_noop() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", schema()).unwrap();
    // Renaming to the same name must not trip the "target exists" check.
    db.rename_table("a", "a").unwrap();
    assert_eq!(db.table_names(), vec!["a".to_string()]);
}

#[test]
fn rename_rejects_empty_new_name() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", schema()).unwrap();
    let err = db.rename_table("a", "").unwrap_err();
    assert!(
        matches!(err, MongrelError::InvalidArgument(_)),
        "got {err:?}"
    );
}

#[test]
fn rename_survives_reopen_with_data_intact() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("a", schema()).unwrap();
        db.transaction(|t| {
            t.put(
                "a",
                vec![(1, Value::Int64(1)), (2, Value::Bytes(b"k".to_vec()))],
            )
        })
        .unwrap();
        db.rename_table("a", "b").unwrap();
        drop(db);
    }

    let reopened = Database::open(dir.path()).unwrap();
    // The rename was replayed from the WAL: new name present, old name gone.
    assert_eq!(reopened.table_names(), vec!["b".to_string()]);
    assert!(reopened.table("a").is_err());
    // The table_id, schema, and on-disk runs are unchanged, so the row survives.
    assert_eq!(reopened.table("b").unwrap().lock().count(), 1);
}

#[test]
fn rename_then_create_can_reuse_old_name() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", schema()).unwrap();
    db.rename_table("a", "b").unwrap();
    // The old name is now free for a distinct new table.
    db.create_table("a", schema()).unwrap();
    let mut names = db.table_names();
    names.sort();
    assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
}
