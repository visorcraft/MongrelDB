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

#[tokio::test]
async fn create_and_drop_table_via_sql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());

    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    // CREATE TABLE via SQL.
    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();

    // Insert via the Database (SQL insert is not yet wired; use the native API).
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(1)), (2, Value::Int64(42))])?;
        Ok(())
    })
    .unwrap();

    // SELECT works.
    let batches = session.run("SELECT * FROM t").await.unwrap();
    assert_eq!(total_rows(&batches), 1);

    // DROP TABLE via SQL.
    session.run("DROP TABLE t").await.unwrap();

    // Table is gone — querying it should fail.
    let result = session.run("SELECT * FROM t").await;
    assert!(result.is_err(), "expected error after DROP TABLE, got Ok");
}

#[tokio::test]
async fn ddl_is_case_insensitive() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    // Mixed-case keywords.
    session
        .run("Create Table t (id BIGINT Primary Key, v BIGINT)")
        .await
        .unwrap();
    assert_eq!(db.table_names(), vec!["t".to_string()]);

    session.run("Drop Table t").await.unwrap();
    assert!(db.table_names().is_empty());
}

#[tokio::test]
async fn ddl_with_if_not_exists_and_if_exists() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    // CREATE TABLE IF NOT EXISTS
    session
        .run("CREATE TABLE IF NOT EXISTS t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    assert_eq!(db.table_names(), vec!["t".to_string()]);

    // DROP TABLE IF EXISTS on a live table succeeds.
    session.run("DROP TABLE IF EXISTS t").await.unwrap();
    assert!(db.table_names().is_empty());

    // DROP TABLE IF EXISTS on a non-existent table succeeds (no error).
    session.run("DROP TABLE IF EXISTS nonexist").await.unwrap();
}

#[tokio::test]
async fn schema_id_is_unique_per_table() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());

    let _ = db.create_table("a", orders_schema()).unwrap();
    let _ = db.create_table("b", customers_schema()).unwrap();

    let schema_a = db.table("a").unwrap().lock().schema().clone();
    let schema_b = db.table("b").unwrap().lock().schema().clone();
    assert_ne!(
        schema_a.schema_id, schema_b.schema_id,
        "schema_ids must be unique across tables"
    );
}

// --- AUTOINCREMENT via SQL DDL ------------------------------------------------

#[tokio::test]
async fn create_table_autoincrement_sets_flag_and_assigns_ids() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE counters (id BIGINT PRIMARY KEY AUTOINCREMENT, label TEXT)")
        .await
        .unwrap();

    // The AUTO_INCREMENT flag reached the engine schema from the SQL parser.
    let table = db.table("counters").unwrap();
    {
        let guard = table.lock();
        let id_col = guard
            .schema()
            .column("id")
            .expect("id column exists")
            .clone();
        assert!(id_col.flags.contains(ColumnFlags::AUTO_INCREMENT));
        assert!(id_col.flags.contains(ColumnFlags::PRIMARY_KEY));
    }

    // Omitting the PK triggers engine allocation (1-based, monotonic). The
    // returned assigned id is the direct proof; reading via the SQL session is
    // avoided because a single-table put does not advance the database-visible
    // epoch the session keys off.
    let assigned1 = table
        .lock()
        .put_returning(vec![(2, Value::Bytes(b"a".to_vec()))])
        .unwrap()
        .1;
    let assigned2 = table
        .lock()
        .put_returning(vec![(2, Value::Bytes(b"b".to_vec()))])
        .unwrap()
        .1;
    assert_eq!(assigned1, Some(1));
    assert_eq!(assigned2, Some(2));
}

#[tokio::test]
async fn autoincrement_keyword_is_case_insensitive() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("Create Table t (id Bigint Primary Key Autoincrement, v Bigint)")
        .await
        .unwrap();
    let flags = db
        .table("t")
        .unwrap()
        .lock()
        .schema()
        .column("id")
        .unwrap()
        .flags;
    assert!(flags.contains(ColumnFlags::AUTO_INCREMENT));
}

#[tokio::test]
async fn auto_increment_underscore_spelling_is_accepted() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY AUTO_INCREMENT, v BIGINT)")
        .await
        .unwrap();
    let flags = db
        .table("t")
        .unwrap()
        .lock()
        .schema()
        .column("id")
        .unwrap()
        .flags;
    assert!(flags.contains(ColumnFlags::AUTO_INCREMENT));
}

#[tokio::test]
async fn autoincrement_on_non_primary_key_is_rejected_with_no_dangling_wal_entry() {
    let dir = tempdir().unwrap();
    {
        let db = Arc::new(Database::create(dir.path()).unwrap());
        let session = MongrelSession::open(Arc::clone(&db)).unwrap();

        // AUTO_INCREMENT on a non-PK column violates the engine contract; the
        // schema must be rejected at creation.
        let result = session
            .run("CREATE TABLE bad (id BIGINT PRIMARY KEY, seq BIGINT AUTO_INCREMENT)")
            .await;
        assert!(
            result.is_err(),
            "AUTO_INCREMENT on a non-PK column must be rejected"
        );
        // In-process, the table was never published to the catalog.
        assert!(db.table_names().is_empty());

        // Drop the live handles; only the on-disk WAL remains for reopen.
        drop(session);
        drop(db);
    }

    // A rejected schema must leave NO durable trace. If the DDL had been
    // appended to the shared WAL before validation, `recover_ddl_from_wal`
    // would replay it (without re-validating) and resurrect "bad" in the
    // catalog — so an empty catalog after reopen proves the validation ran
    // before the WAL mutation.
    let reopened = Database::open(dir.path()).unwrap();
    assert!(
        reopened.table_names().is_empty(),
        "a rejected CREATE TABLE must not leave a table in the catalog after reopen"
    );
}
