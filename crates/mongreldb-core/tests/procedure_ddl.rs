use mongreldb_core::procedure::{
    ProcedureBody, ProcedureMode, ProcedureParam, ProcedureStep, ProcedureValue, StoredProcedure,
};
use mongreldb_core::{
    ColumnDef, ColumnFlags, Database, IndexDef, IndexKind, MongrelError, Schema, TypeId, Value,
};
use std::sync::{mpsc, Arc};
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn database_creates_replaces_lists_and_drops_procedures() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema()).unwrap();

    let created = db
        .create_procedure(sample_procedure("active_users"))
        .unwrap();
    assert_eq!(created.version, 1);
    assert_eq!(db.procedures().len(), 1);
    assert!(db.procedure("active_users").is_some());

    let replaced = db
        .create_or_replace_procedure(sample_procedure("active_users"))
        .unwrap();
    assert_eq!(replaced.version, 2);
    assert_eq!(replaced.created_epoch, created.created_epoch);
    assert!(replaced.updated_epoch >= created.updated_epoch);

    db.drop_procedure("active_users").unwrap();
    assert!(db.procedure("active_users").is_none());
    assert!(db.procedures().is_empty());
}

#[test]
fn database_rejects_procedure_that_references_unknown_projection_column() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema()).unwrap();

    let mut proc = sample_procedure("bad_projection");
    if let ProcedureStep::NativeQuery { projection, .. } = &mut proc.body.steps[0] {
        *projection = Some(vec![99]);
    }

    let err = db.create_procedure(proc).unwrap_err().to_string();

    assert!(err.contains("unknown column id 99"));
}

#[test]
fn database_rejects_duplicate_procedure_names() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema()).unwrap();

    db.create_procedure(sample_procedure("active_users"))
        .unwrap();
    let err = db
        .create_procedure(sample_procedure("active_users"))
        .unwrap_err()
        .to_string();

    assert!(err.contains("already exists"));
}

#[test]
fn controlled_procedure_publish_aborts_without_live_or_disk_mutation() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema()).unwrap();
    let initial_epoch = db.visible_epoch();

    let error = db
        .create_procedure_controlled(sample_procedure("cancelled"), || {
            Err(MongrelError::Cancelled)
        })
        .unwrap_err();

    assert!(matches!(error, MongrelError::Cancelled));
    assert_eq!(db.catalog_snapshot().db_epoch, initial_epoch.0);
    assert!(db.procedure("cancelled").is_none());
    assert!(!dir.path().join(".CATALOG.tmp").exists());
    drop(db);
    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.visible_epoch(), initial_epoch);
    assert!(reopened.procedure("cancelled").is_none());
}

#[test]
fn procedure_catalog_write_failure_reports_durable_commit_and_recovers() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema()).unwrap();
    let initial_epoch = db.visible_epoch();
    let catalog = dir.path().join("CATALOG");
    let saved_catalog = dir.path().join("CATALOG.saved");
    std::fs::rename(&catalog, &saved_catalog).unwrap();
    std::fs::create_dir(&catalog).unwrap();

    let error = db
        .create_procedure(sample_procedure("durable_procedure"))
        .unwrap_err();

    assert!(matches!(error, MongrelError::DurableCommit { .. }));
    assert!(db.catalog_snapshot().db_epoch > initial_epoch.0);
    assert!(db.procedure("durable_procedure").is_some());
    assert!(db
        .create_procedure(sample_procedure("after_failure"))
        .is_err());
    std::fs::remove_dir(&catalog).unwrap();
    std::fs::rename(&saved_catalog, &catalog).unwrap();
    drop(db);
    let reopened = Database::open(dir.path()).unwrap();
    assert!(reopened.visible_epoch() > initial_epoch);
    assert!(reopened.procedure("durable_procedure").is_some());
    assert!(reopened.procedure("after_failure").is_none());
}

#[test]
fn concurrent_user_and_procedure_catalog_writes_preserve_both() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("users", users_schema()).unwrap();
    let (procedure_fenced_tx, procedure_fenced_rx) = mpsc::channel();
    let (release_procedure_tx, release_procedure_rx) = mpsc::channel();
    let procedure_db = Arc::clone(&db);
    let procedure_thread = std::thread::spawn(move || {
        procedure_db.create_procedure_controlled(sample_procedure("concurrent_proc"), || {
            procedure_fenced_tx.send(()).unwrap();
            release_procedure_rx.recv().unwrap();
            Ok(())
        })
    });
    procedure_fenced_rx.recv().unwrap();

    let (user_fenced_tx, user_fenced_rx) = mpsc::channel();
    let user_db = Arc::clone(&db);
    let user_thread = std::thread::spawn(move || {
        user_db.create_user_with_password_hash_controlled(
            "concurrent_user",
            "prepared-hash".into(),
            || {
                user_fenced_tx.send(()).unwrap();
                Ok(())
            },
        )
    });

    assert!(user_fenced_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err());
    release_procedure_tx.send(()).unwrap();
    procedure_thread.join().unwrap().unwrap();
    user_thread.join().unwrap().unwrap();

    assert!(db.procedure("concurrent_proc").is_some());
    assert!(db
        .users()
        .iter()
        .any(|user| user.username == "concurrent_user"));
    drop(db);
    let reopened = Database::open(dir.path()).unwrap();
    assert!(reopened.procedure("concurrent_proc").is_some());
    assert!(reopened
        .users()
        .iter()
        .any(|user| user.username == "concurrent_user"));
}

fn users_schema() -> Schema {
    Schema {
        schema_id: 1,
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
                name: "status".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "status_idx".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
            predicate: None,
            options: Default::default(),
        }],
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn sample_procedure(name: &str) -> StoredProcedure {
    StoredProcedure::new(
        name,
        ProcedureMode::ReadOnly,
        vec![ProcedureParam {
            name: "status".into(),
            ty: TypeId::Bytes,
            nullable: false,
            default: Some(Value::Bytes(b"active".to_vec())),
        }],
        ProcedureBody {
            steps: vec![ProcedureStep::NativeQuery {
                id: "read".into(),
                table: "users".into(),
                conditions: Vec::new(),
                projection: Some(vec![1, 2]),
                limit: Some(10),
            }],
            return_value: ProcedureValue::StepRows("read".into()),
        },
        0,
    )
    .unwrap()
}
