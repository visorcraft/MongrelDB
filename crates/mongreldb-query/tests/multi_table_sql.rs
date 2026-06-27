//! P4.1 — multi-table SQL over a Database.

use mongreldb_core::{schema::*, Database, Value};
use mongreldb_query::MongrelSession;
use std::sync::Arc;
use tempfile::tempdir;

fn orders_schema() -> Schema {
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
                name: "customer_id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![],
    }
}

fn customers_schema() -> Schema {
    Schema {
        schema_id: 2,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "name".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![],
    }
}

fn total_rows(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn cross_table_join_over_database() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());

    db.create_table("orders", orders_schema()).unwrap();
    db.create_table("customers", customers_schema()).unwrap();

    db.transaction(|t| {
        t.put(
            "customers",
            vec![(1, Value::Int64(1)), (2, Value::Bytes(b"Alice".to_vec()))],
        )?;
        t.put(
            "customers",
            vec![(1, Value::Int64(2)), (2, Value::Bytes(b"Bob".to_vec()))],
        )?;
        t.put("orders", vec![(1, Value::Int64(100)), (2, Value::Int64(1))])?;
        t.put("orders", vec![(1, Value::Int64(101)), (2, Value::Int64(2))])?;
        t.put("orders", vec![(1, Value::Int64(102)), (2, Value::Int64(1))])?;
        Ok(())
    })
    .unwrap();

    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    // Simple queries work.
    let batches = session.run("SELECT * FROM orders").await.unwrap();
    assert_eq!(total_rows(&batches), 3);

    let batches = session.run("SELECT * FROM customers").await.unwrap();
    assert_eq!(total_rows(&batches), 2);

    // Cross-table join.
    let batches = session
        .run("SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id")
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 3);

    // COUNT(*) is O(1).
    let batches = session.run("SELECT COUNT(*) FROM orders").await.unwrap();
    assert_eq!(total_rows(&batches), 1);
}

#[tokio::test]
async fn database_session_cache_invalidates_on_commit() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("t", orders_schema()).unwrap();

    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(1)), (2, Value::Int64(10))])?;
        Ok(())
    })
    .unwrap();

    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    // First query populates the cache.
    let batches = session.run("SELECT COUNT(*) FROM t").await.unwrap();
    assert_eq!(total_rows(&batches), 1);

    // Commit new data — cache must invalidate (epoch changes).
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(2)), (2, Value::Int64(20))])?;
        Ok(())
    })
    .unwrap();

    // Re-run — new result.
    let batches = session.run("SELECT * FROM t").await.unwrap();
    assert_eq!(total_rows(&batches), 2);
}
