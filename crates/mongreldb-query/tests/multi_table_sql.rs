//! P4.1 — multi-table SQL over a Database.

use datafusion::arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
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

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn i64_values(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn string_values(batches: &[RecordBatch], column: usize) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn bool_values(batches: &[RecordBatch], column: usize) -> Vec<bool> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row))
                .collect::<Vec<_>>()
        })
        .collect()
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

// --- ALTER TABLE ... RENAME TO ... -------------------------------------------

#[tokio::test]
async fn alter_table_rename_via_sql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    // Insert via the Database so the row is committed and visible to the
    // session's epoch before the rename.
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(1)), (2, Value::Int64(10))])?;
        t.put("t", vec![(1, Value::Int64(2)), (2, Value::Int64(20))])
    })
    .unwrap();

    session.run("ALTER TABLE t RENAME TO u").await.unwrap();

    // The old name is gone from the catalog and from DataFusion.
    assert!(!db.table_names().contains(&"t".to_string()));
    assert!(session.run("SELECT * FROM t").await.is_err());

    // The new name resolves and carries the data over.
    assert!(db.table_names().contains(&"u".to_string()));
    let batches = session.run("SELECT * FROM u").await.unwrap();
    assert_eq!(total_rows(&batches), 2);
}

#[tokio::test]
async fn alter_table_rename_rejects_conflict_via_sql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .run("CREATE TABLE a (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE b (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();

    let result = session.run("ALTER TABLE a RENAME TO b").await;
    assert!(
        result.is_err(),
        "renaming onto an existing table name must fail"
    );
    // Both original tables remain intact.
    let mut names = db.table_names();
    names.sort();
    assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
}

#[tokio::test]
async fn alter_table_rename_is_case_insensitive() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();

    session.run("Alter Table t Rename To u").await.unwrap();
    assert_eq!(db.table_names(), vec!["u".to_string()]);
}

#[tokio::test]
async fn alter_table_rename_column_via_sql_refreshes_schema() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(1)), (2, Value::Int64(10))])?;
        t.put("t", vec![(1, Value::Int64(2)), (2, Value::Int64(20))])
    })
    .unwrap();

    session
        .run("ALTER TABLE t RENAME COLUMN v TO amount")
        .await
        .unwrap();

    assert!(db
        .table("t")
        .unwrap()
        .lock()
        .schema()
        .column("amount")
        .is_some());
    assert!(session.run("SELECT v FROM t").await.is_err());
    let batches = session.run("SELECT amount FROM t").await.unwrap();
    assert_eq!(total_rows(&batches), 2);
    assert_eq!(batches[0].schema().field(0).name(), "amount");
}

#[tokio::test]
async fn alter_column_nullability_via_sql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();

    session
        .run("ALTER TABLE t ALTER COLUMN v DROP NOT NULL")
        .await
        .unwrap();
    assert!(db
        .table("t")
        .unwrap()
        .lock()
        .schema()
        .column("v")
        .unwrap()
        .flags
        .contains(ColumnFlags::NULLABLE));

    db.transaction(|t| t.put("t", vec![(1, Value::Int64(1))]))
        .unwrap();
    let result = session
        .run("ALTER TABLE t ALTER COLUMN v SET NOT NULL")
        .await;
    assert!(
        result.is_err(),
        "SET NOT NULL must reject existing NULL values"
    );
}

#[tokio::test]
async fn alter_column_type_via_sql_on_empty_table() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();

    session
        .run("ALTER TABLE t ALTER COLUMN v TYPE TEXT")
        .await
        .unwrap();

    assert_eq!(
        db.table("t")
            .unwrap()
            .lock()
            .schema()
            .column("v")
            .unwrap()
            .ty,
        TypeId::Bytes
    );
    let batches = session.run("SELECT v FROM t").await.unwrap();
    assert_eq!(total_rows(&batches), 0);
}

#[tokio::test]
async fn insert_update_delete_and_truncate_via_sql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, qty BIGINT)")
        .await
        .unwrap();
    session
        .run(
            "INSERT INTO items (id, name, qty) VALUES \
             (1, 'pencil', 5), (2, 'pen', 8), (3, 'eraser', 2)",
        )
        .await
        .unwrap();

    let batches = session
        .run("SELECT id, name, qty FROM items ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 2, 3]);
    assert_eq!(
        string_values(&batches, 1),
        vec![
            "pencil".to_string(),
            "pen".to_string(),
            "eraser".to_string()
        ]
    );
    assert_eq!(i64_values(&batches, 2), vec![5, 8, 2]);

    session
        .run("UPDATE items SET qty = 18 WHERE name = 'pen' OR id = 3")
        .await
        .unwrap();
    let batches = session
        .run("SELECT qty FROM items ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![5, 18, 18]);

    session
        .run("DELETE FROM items WHERE qty >= 18 AND name IN ('pen', 'eraser')")
        .await
        .unwrap();
    let batches = session
        .run("SELECT id FROM items ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1]);

    session.run("TRUNCATE TABLE items").await.unwrap();
    let batches = session.run("SELECT id FROM items").await.unwrap();
    assert_eq!(total_rows(&batches), 0);
}

#[tokio::test]
async fn insert_on_conflict_variants_via_sql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, qty BIGINT)")
        .await
        .unwrap();
    session
        .run("INSERT INTO items (id, name, qty) VALUES (1, 'old', 10)")
        .await
        .unwrap();
    session
        .run(
            "INSERT INTO items (id, name, qty) VALUES (1, 'ignored', 99) \
             ON CONFLICT (id) DO NOTHING",
        )
        .await
        .unwrap();

    let batches = session
        .run("SELECT name, qty FROM items WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["old".to_string()]);
    assert_eq!(i64_values(&batches, 1), vec![10]);

    session
        .run(
            "INSERT INTO items (id, name, qty) VALUES (1, 'new', 15) \
             ON CONFLICT (id) DO UPDATE SET name = excluded.name, qty = excluded.qty",
        )
        .await
        .unwrap();
    let batches = session
        .run("SELECT name, qty FROM items WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["new".to_string()]);
    assert_eq!(i64_values(&batches, 1), vec![15]);
}

#[tokio::test]
async fn create_and_drop_index_via_sql_preserves_rows() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE metrics (id BIGINT PRIMARY KEY, category TEXT, amount BIGINT)")
        .await
        .unwrap();
    session
        .run(
            "INSERT INTO metrics (id, category, amount) VALUES \
             (1, 'a', 10), (2, 'b', 20), (3, 'a', 30)",
        )
        .await
        .unwrap();

    session
        .run("CREATE INDEX idx_metrics_category ON metrics (category)")
        .await
        .unwrap();
    {
        let schema = db.table("metrics").unwrap().lock().schema().clone();
        assert_eq!(schema.indexes.len(), 1);
        assert_eq!(schema.indexes[0].name, "idx_metrics_category");
    }

    let batches = session
        .run("SELECT id FROM metrics WHERE category = 'a' ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 3]);

    session
        .run("DROP INDEX idx_metrics_category ON metrics")
        .await
        .unwrap();
    let schema = db.table("metrics").unwrap().lock().schema().clone();
    assert!(schema.indexes.is_empty());
    let batches = session
        .run("SELECT id FROM metrics ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 2, 3]);
}

#[tokio::test]
async fn create_and_drop_view_via_sql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, qty BIGINT)")
        .await
        .unwrap();
    session
        .run("INSERT INTO items (id, name, qty) VALUES (1, 'small', 1), (2, 'large', 20)")
        .await
        .unwrap();
    session
        .run("CREATE VIEW large_items AS SELECT name FROM items WHERE qty >= 10")
        .await
        .unwrap();

    let batches = session.run("SELECT name FROM large_items").await.unwrap();
    assert_eq!(string_values(&batches, 0), vec!["large".to_string()]);

    session.run("DROP VIEW large_items").await.unwrap();
    assert!(session.run("SELECT name FROM large_items").await.is_err());
}

#[tokio::test]
async fn introspection_and_admin_commands_via_sql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE alpha (id BIGINT PRIMARY KEY, note TEXT)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE beta (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();

    let batches = session.run("SHOW TABLES").await.unwrap();
    assert_eq!(
        string_values(&batches, 0),
        vec!["alpha".to_string(), "beta".to_string()]
    );

    let batches = session.run("DESCRIBE alpha").await.unwrap();
    assert_eq!(
        string_values(&batches, 0),
        vec!["id".to_string(), "note".to_string()]
    );
    assert_eq!(bool_values(&batches, 3), vec![true, false]);

    let batches = session.run("PRAGMA table_info(alpha)").await.unwrap();
    assert_eq!(
        string_values(&batches, 1),
        vec!["id".to_string(), "note".to_string()]
    );
    assert_eq!(bool_values(&batches, 4), vec![true, false]);

    let batches = session.run("CHECK").await.unwrap();
    assert_eq!(batches[0].schema().field(0).name(), "severity");
    session.run("VACUUM").await.unwrap();
}

#[tokio::test]
async fn explicit_transactions_stage_sql_dml() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE tx_items (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();

    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO tx_items (id, name) VALUES (1, 'one')")
        .await
        .unwrap();
    session
        .run("INSERT INTO tx_items (id, name) VALUES (2, 'two')")
        .await
        .unwrap();
    session.run("COMMIT").await.unwrap();

    let batches = session
        .run("SELECT id FROM tx_items ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 2]);

    session.run("BEGIN").await.unwrap();
    session
        .run("DELETE FROM tx_items WHERE id = 1")
        .await
        .unwrap();
    session.run("ROLLBACK").await.unwrap();
    let batches = session
        .run("SELECT id FROM tx_items ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 2]);
}

#[tokio::test]
async fn alter_table_add_and_drop_column_via_sql_preserves_rows() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session.run("INSERT INTO t (id) VALUES (1)").await.unwrap();

    session
        .run("ALTER TABLE t ADD COLUMN note TEXT")
        .await
        .unwrap();
    assert!(db
        .table("t")
        .unwrap()
        .lock()
        .schema()
        .column("note")
        .is_some());
    session
        .run("INSERT INTO t (id, note) VALUES (2, 'kept')")
        .await
        .unwrap();

    session.run("ALTER TABLE t DROP COLUMN note").await.unwrap();
    assert!(db
        .table("t")
        .unwrap()
        .lock()
        .schema()
        .column("note")
        .is_none());
    assert!(session.run("SELECT note FROM t").await.is_err());
    let batches = session.run("SELECT id FROM t ORDER BY id").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 2]);
}
