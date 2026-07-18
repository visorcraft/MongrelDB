use mongreldb_core::columnar::NativeColumn;
use mongreldb_core::procedure::{
    ProcedureBody, ProcedureCallOutput, ProcedureMode, ProcedureParam, ProcedureStep,
    ProcedureValue, StoredProcedure,
};
use mongreldb_core::{
    CancellationReason, ColumnDef, ColumnFlags, Database, ExecutionControl, IndexDef, IndexKind,
    MongrelError, Schema, TypeId, Value,
};
use std::collections::HashMap;
use tempfile::tempdir;

#[test]
fn read_only_procedure_returns_native_query_rows() {
    let (_dir, db) = seeded_db();
    db.create_procedure(read_users_procedure("read_users"))
        .unwrap();

    let result = db.call_procedure("read_users", HashMap::new()).unwrap();

    assert_eq!(result.epoch, None);
    let ProcedureCallOutput::Rows(rows) = result.output else {
        panic!("expected rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(1)));
    assert_eq!(
        rows[0].columns.get(&2),
        Some(&Value::Bytes(b"active".to_vec()))
    );
}

#[test]
fn read_write_procedure_commits_put_once() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema()).unwrap();
    db.create_procedure(insert_user_procedure("insert_user"))
        .unwrap();

    let mut args = HashMap::new();
    args.insert("id".into(), Value::Int64(7));
    args.insert("status".into(), Value::Bytes(b"active".to_vec()));
    let result = db.call_procedure("insert_user", args).unwrap();

    assert!(result.epoch.is_some());
    let ProcedureCallOutput::Row(row) = result.output else {
        panic!("expected row");
    };
    assert_eq!(row.columns.get(&1), Some(&Value::Int64(7)));
    assert_eq!(db.table("users").unwrap().lock().count(), 1);
}

#[test]
fn native_query_cancellation_rolls_back_procedure_and_database_remains_usable() {
    const ROWS: usize = 1_000_000;
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema()).unwrap();
    db.table("users")
        .unwrap()
        .lock()
        .bulk_load_columns(vec![
            (
                1,
                NativeColumn::Int64 {
                    data: (0..ROWS).map(|value| value as i64).collect(),
                    validity: vec![u8::MAX; ROWS.div_ceil(8)],
                },
            ),
            (
                2,
                NativeColumn::Bytes {
                    offsets: (0..=ROWS).map(|index| (index * 6) as u32).collect(),
                    values: b"active".repeat(ROWS),
                    validity: vec![u8::MAX; ROWS.div_ceil(8)],
                },
            ),
        ])
        .unwrap();
    db.create_procedure(scan_then_insert_procedure("scan_then_insert", ROWS as i64))
        .unwrap();

    let control = ExecutionControl::new(None);
    let cancel_control = control.clone();
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(5));
        cancel_control.cancel(CancellationReason::ClientRequest);
    });
    let error = db
        .call_procedure_as_controlled("scan_then_insert", HashMap::new(), None, &control, || true)
        .unwrap_err();
    canceller.join().unwrap();

    assert!(matches!(error, MongrelError::Cancelled));
    assert_eq!(db.table("users").unwrap().lock().count(), ROWS as u64);
    assert_eq!(
        db.query_for_current_principal(
            "users",
            &mongreldb_core::Query::new().with_limit(1),
            Some(&[1]),
        )
        .unwrap()
        .len(),
        1
    );
}

fn seeded_db() -> (tempfile::TempDir, Database) {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema()).unwrap();
    {
        let mut tx = db.begin();
        tx.put(
            "users",
            vec![(1, Value::Int64(1)), (2, Value::Bytes(b"active".to_vec()))],
        )
        .unwrap();
        tx.commit().unwrap();
    }
    (dir, db)
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
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "status".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![
            IndexDef {
                name: "pk".into(),
                column_id: 1,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "status_idx".into(),
                column_id: 2,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            },
        ],
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn read_users_procedure(name: &str) -> StoredProcedure {
    StoredProcedure::new(
        name,
        ProcedureMode::ReadOnly,
        Vec::new(),
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

fn insert_user_procedure(name: &str) -> StoredProcedure {
    StoredProcedure::new(
        name,
        ProcedureMode::ReadWrite,
        vec![
            ProcedureParam {
                name: "id".into(),
                ty: TypeId::Int64,
                nullable: false,
                default: None,
            },
            ProcedureParam {
                name: "status".into(),
                ty: TypeId::Bytes,
                nullable: false,
                default: None,
            },
        ],
        ProcedureBody {
            steps: vec![ProcedureStep::Put {
                id: "put".into(),
                table: "users".into(),
                cells: vec![
                    mongreldb_core::procedure::ProcedureCell {
                        column_id: 1,
                        value: ProcedureValue::Param("id".into()),
                    },
                    mongreldb_core::procedure::ProcedureCell {
                        column_id: 2,
                        value: ProcedureValue::Param("status".into()),
                    },
                ],
                returning: true,
            }],
            return_value: ProcedureValue::StepRow("put".into()),
        },
        0,
    )
    .unwrap()
}

fn scan_then_insert_procedure(name: &str, id: i64) -> StoredProcedure {
    StoredProcedure::new(
        name,
        ProcedureMode::ReadWrite,
        Vec::new(),
        ProcedureBody {
            steps: vec![
                ProcedureStep::NativeQuery {
                    id: "scan".into(),
                    table: "users".into(),
                    conditions: Vec::new(),
                    projection: Some(vec![1]),
                    limit: Some(1),
                },
                ProcedureStep::Put {
                    id: "put".into(),
                    table: "users".into(),
                    cells: vec![
                        mongreldb_core::procedure::ProcedureCell {
                            column_id: 1,
                            value: ProcedureValue::Literal(Value::Int64(id)),
                        },
                        mongreldb_core::procedure::ProcedureCell {
                            column_id: 2,
                            value: ProcedureValue::Literal(Value::Bytes(b"new".to_vec())),
                        },
                    ],
                    returning: false,
                },
            ],
            return_value: ProcedureValue::StepRows("scan".into()),
        },
        0,
    )
    .unwrap()
}
