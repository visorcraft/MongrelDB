use mongreldb_core::procedure::{
    ProcedureBody, ProcedureMode, ProcedureParam, ProcedureStep, ProcedureValue, StoredProcedure,
};
use mongreldb_core::{
    ColumnDef, ColumnFlags, Database, IndexDef, IndexKind, Schema, TypeId, Value,
};
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

fn users_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "status".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![IndexDef {
            name: "status_idx".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
            predicate: None,
        }],
        colocation: Vec::new(),
        constraints: Default::default(),
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
