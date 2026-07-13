use arrow::array::{Array, StringArray};
use mongreldb_core::{schema::*, Database, Value};
use mongreldb_query::MongrelSession;
use std::sync::Arc;
use tempfile::tempdir;

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

#[tokio::test]
async fn sql_creates_lists_calls_and_drops_procedure() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("users", users_schema()).unwrap();
    db.transaction(|tx| {
        tx.put(
            "users",
            vec![(1, Value::Int64(1)), (2, Value::Bytes(b"active".to_vec()))],
        )?;
        Ok(())
    })
    .unwrap();
    let session = MongrelSession::open(db).unwrap();

    let spec = serde_json::json!({
        "name": "read_users",
        "version": 1,
        "mode": "read_only",
        "params": [],
        "body": {
            "steps": [{
                "kind": "native_query",
                "id": "read",
                "table": "users",
                "conditions": [],
                "projection": [1, 2],
                "limit": 10
            }],
            "return_value": { "kind": "step_rows", "value": "read" }
        },
        "checksum": "",
        "created_epoch": 0,
        "updated_epoch": 0
    });
    session
        .run(&format!(
            "CREATE PROCEDURE read_users AS JSON '{}'",
            spec.to_string().replace('\'', "''")
        ))
        .await
        .unwrap();

    let listed = session.run("SHOW PROCEDURES").await.unwrap();
    let names = listed[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "read_users");

    let called = session.run("CALL read_users(JSON '{}')").await.unwrap();
    let json = called[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(json.value(0).contains("active"));

    session.run("DROP PROCEDURE read_users").await.unwrap();
    let listed = session.run("SHOW PROCEDURES").await.unwrap();
    assert_eq!(listed[0].num_rows(), 0);
}
