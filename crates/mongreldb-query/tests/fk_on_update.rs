use mongreldb_core::constraint::{FkAction, ForeignKey, TableConstraints};
use mongreldb_core::{ColumnDef, ColumnFlags, Database, Schema, TypeId, Value};
use mongreldb_query::MongrelSession;
use std::sync::Arc;
use tempfile::tempdir;

fn schema(name: &str, fk_action: Option<FkAction>) -> Schema {
    let id = if name == "users" { 1 } else { 10 };
    let mut columns = vec![ColumnDef {
        id,
        name: "id".into(),
        ty: TypeId::Int64,
        flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
        default_value: None,
    }];
    let mut constraints = TableConstraints::default();
    if let Some(on_update) = fk_action {
        columns.push(ColumnDef {
            id: 11,
            name: "user_id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty(),
            default_value: None,
        });
        constraints.foreign_keys.push(ForeignKey {
            id: 1,
            name: "orders_user_fk".into(),
            columns: vec![11],
            ref_table: "users".into(),
            ref_columns: vec![1],
            on_delete: FkAction::Restrict,
            on_update,
        });
    }
    Schema {
        schema_id: 0,
        columns,
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints,
        clustered: false,
    }
}

fn setup(action: FkAction) -> (tempfile::TempDir, Arc<Database>, MongrelSession) {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("users", schema("users", None)).unwrap();
    db.create_table("orders", schema("orders", Some(action)))
        .unwrap();
    db.transaction(|transaction| {
        transaction.put("users", vec![(1, Value::Int64(1))])?;
        transaction.put(
            "orders",
            vec![(10, Value::Int64(10)), (11, Value::Int64(1))],
        )?;
        Ok(())
    })
    .unwrap();
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    (dir, db, session)
}

#[tokio::test]
async fn sql_update_applies_fk_on_update_cascade() {
    let (_dir, db, session) = setup(FkAction::Cascade);
    session
        .run("UPDATE users SET id = 2 WHERE id = 1")
        .await
        .unwrap();
    let rows = db
        .table("orders")
        .unwrap()
        .lock()
        .visible_rows(db.snapshot().0)
        .unwrap();
    assert_eq!(rows[0].columns.get(&11), Some(&Value::Int64(2)));
}

#[tokio::test]
async fn sql_update_enforces_fk_on_update_restrict() {
    let (_dir, _db, session) = setup(FkAction::Restrict);
    let error = session
        .run("UPDATE users SET id = 2 WHERE id = 1")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("restricts update"));
}
