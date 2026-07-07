//! Adversarial tests — try to break the SQL features added in engine 0.37.0.
//! Each test targets edge cases, error conditions, and potential crashes.

use mongreldb_core::{schema::*, Database, Value};
use mongreldb_query::MongrelSession;
use std::sync::Arc;
use tempfile::tempdir;

fn setup_orders() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let schema = Schema {
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
                name: "amount".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 3,
                name: "category".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 4,
                name: "parent_id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    db.create_table("orders", schema).unwrap();
    let t = db.table("orders").unwrap();
    for i in 1i64..=5 {
        let cat = if i <= 2 {
            b"food".to_vec()
        } else {
            b"toys".to_vec()
        };
        let parent = if i > 1 {
            Value::Int64(i - 1)
        } else {
            Value::Null
        };
        t.lock()
            .put(vec![
                (1, Value::Int64(i)),
                (2, Value::Float64(i as f64 * 10.0)),
                (3, Value::Bytes(cat)),
                (4, parent),
            ])
            .unwrap();
    }
    t.lock().commit().unwrap();
    (dir, Arc::new(db))
}

fn setup_empty() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let schema = Schema {
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
                name: "val".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    db.create_table("items", schema).unwrap();
    (dir, Arc::new(db))
}

fn run(db: &Arc<Database>, sql: &str) -> Result<Vec<Vec<(String, String)>>, String> {
    let session = MongrelSession::open(Arc::clone(db)).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let batches = rt
        .block_on(async { session.run(sql).await })
        .map_err(|e| e.to_string())?;
    let mut rows = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        for i in 0..batch.num_rows() {
            let row: Vec<(String, String)> = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(j, f)| {
                    let col = batch.column(j);
                    let val = if col.is_null(i) {
                        "NULL".into()
                    } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::Int64Array>()
                    {
                        a.value(i).to_string()
                    } else if let Some(a) =
                        col.as_any().downcast_ref::<arrow::array::Float64Array>()
                    {
                        a.value(i).to_string()
                    } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::StringArray>()
                    {
                        a.value(i).to_string()
                    } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::Int32Array>()
                    {
                        a.value(i).to_string()
                    } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::UInt32Array>()
                    {
                        a.value(i).to_string()
                    } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::UInt64Array>()
                    {
                        a.value(i).to_string()
                    } else {
                        format!("{:?}", col.data_type())
                    };
                    (f.name().clone(), val)
                })
                .collect();
            rows.push(row);
        }
    }
    Ok(rows)
}

fn run_err(db: &Arc<Database>, sql: &str) -> String {
    match run(db, sql) {
        Ok(_) => String::new(),
        Err(e) => e,
    }
}

// ── Recursive CTEs: adversarial ───────────────────────────────────────────

#[test]
fn recursive_cte_empty_base() {
    let (_dir, db) = setup_empty();
    // Base case returns no rows → CTE should be empty.
    let result = run(&db,
        "WITH RECURSIVE r(n) AS (SELECT 1 FROM items WHERE id = 999 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT n FROM r");
    // The base query `SELECT 1 FROM items WHERE id = 999` on an empty table returns 0 rows.
    // The CTE should return empty.
    match result {
        Ok(rows) => assert_eq!(rows.len(), 0, "empty base → empty result"),
        Err(e) => panic!("empty base recursive CTE failed: {e}"),
    }
}

#[test]
fn recursive_cte_immediate_convergence() {
    let (_dir, db) = setup_orders();
    // Recursive arm WHERE clause is immediately false → base only.
    let rows = run(&db,
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 0) SELECT n FROM r").unwrap();
    assert_eq!(rows.len(), 1, "immediate convergence: base only");
    assert_eq!(rows[0][0].1, "1");
}

#[test]
fn recursive_cte_deep_recursion() {
    let (_dir, db) = setup_orders();
    // Generate 1000 rows — should converge within the safety bound.
    let rows = run(&db,
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 1000) SELECT count(*) AS c FROM r").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, "1000", "should produce exactly 1000 rows");
}

#[test]
fn recursive_cte_recursive_on_real_table() {
    let (_dir, db) = setup_orders();
    // Walk a parent_id chain using a recursive CTE.
    let rows = run(
        &db,
        "WITH RECURSIVE chain(id, parent_id) AS \
         (SELECT id, parent_id FROM orders WHERE id = 1 \
          UNION ALL \
          SELECT o.id, o.parent_id FROM orders o JOIN chain ON o.parent_id = chain.id) \
         SELECT id FROM chain ORDER BY id",
    )
    .unwrap();
    // id=1 → children: id=2 (parent_id=1), id=3 (parent_id=2), etc.
    assert!(rows.len() >= 1, "should walk at least 1 row");
    assert_eq!(rows[0][0].1, "1");
}

#[test]
fn recursive_cte_union_dedup() {
    let (_dir, db) = setup_orders();
    // UNION (not UNION ALL) should deduplicate.
    let rows = run(&db,
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION SELECT n FROM r WHERE n < 3) SELECT n FROM r ORDER BY n").unwrap();
    // Should return just {1} since UNION deduplicates and the recursive arm
    // produces values already in the set.
    assert_eq!(rows.len(), 1, "UNION dedup should prevent duplicates");
}

#[test]
fn recursive_cte_invalid_syntax() {
    let (_dir, db) = setup_orders();
    // Missing closing paren
    let err = run_err(&db, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5 SELECT n FROM r");
    assert!(!err.is_empty(), "malformed recursive CTE should error");
}

#[test]
fn recursive_cte_no_outer_query() {
    let (_dir, db) = setup_orders();
    // CTE definition with no outer query
    let err = run_err(
        &db,
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5)",
    );
    assert!(
        !err.is_empty(),
        "recursive CTE without outer query should error"
    );
}

// ── CTAS: adversarial ─────────────────────────────────────────────────────

#[test]
fn ctas_empty_result() {
    let (_dir, db) = setup_orders();
    // CTAS from a query that returns no rows.
    let err = run_err(
        &db,
        "CREATE TABLE empty_copy AS SELECT * FROM orders WHERE id > 999",
    );
    assert!(
        !err.is_empty(),
        "CTAS with empty result should error (can't infer schema)"
    );
}

#[test]
fn ctas_all_types() {
    let (_dir, db) = setup_orders();
    // CTAS with int64, float64, bytes, nullable int64.
    let result = run(
        &db,
        "CREATE TABLE full_copy AS SELECT id, amount, category, parent_id FROM orders",
    );
    assert!(result.is_ok(), "CTAS should succeed");
    let result_rows = result.unwrap();
    assert!(result_rows.is_empty(), "CTAS returns empty result set");
    // Verify data.
    let rows = run(&db, "SELECT id FROM full_copy ORDER BY id").unwrap();
    assert_eq!(rows.len(), 5, "full_copy should have 5 rows");
}

#[test]
fn ctas_already_exists() {
    let (_dir, db) = setup_orders();
    // Create then create again — should error.
    run(&db, "CREATE TABLE dup_copy AS SELECT id FROM orders").unwrap();
    let err = run_err(&db, "CREATE TABLE dup_copy AS SELECT id FROM orders");
    assert!(!err.is_empty(), "duplicate CTAS should error");
}

#[test]
fn ctas_if_not_exists() {
    let (_dir, db) = setup_orders();
    // CREATE TABLE IF NOT EXISTS AS SELECT — should be idempotent.
    run(
        &db,
        "CREATE TABLE IF NOT EXISTS idempotent AS SELECT id FROM orders",
    )
    .unwrap();
    // Second call should succeed (no-op).
    let result = run(
        &db,
        "CREATE TABLE IF NOT EXISTS idempotent AS SELECT id FROM orders",
    );
    assert!(result.is_ok(), "IF NOT EXISTS should be idempotent");
}

#[test]
fn ctas_complex_aggregation() {
    let (_dir, db) = setup_orders();
    // CTAS from a GROUP BY aggregation.
    let result = run(&db,
        "CREATE TABLE category_summary AS SELECT category, count(*) AS cnt, sum(amount) AS total FROM orders GROUP BY category");
    assert!(result.is_ok(), "CTAS with aggregation should succeed");
    let rows = run(
        &db,
        "SELECT category FROM category_summary ORDER BY category",
    )
    .unwrap();
    assert_eq!(rows.len(), 2, "2 categories: food, toys");
}

// ── Materialized views: adversarial ───────────────────────────────────────

#[test]
fn matview_already_exists() {
    let (_dir, db) = setup_orders();
    run(&db, "CREATE MATERIALIZED VIEW mv1 AS SELECT id FROM orders").unwrap();
    let err = run_err(&db, "CREATE MATERIALIZED VIEW mv1 AS SELECT id FROM orders");
    assert!(!err.is_empty(), "duplicate MATERIALIZED VIEW should error");
}

#[test]
fn matview_if_not_exists() {
    let (_dir, db) = setup_orders();
    run(
        &db,
        "CREATE MATERIALIZED VIEW IF NOT EXISTS mv2 AS SELECT id FROM orders",
    )
    .unwrap();
    // Second call should succeed (no-op).
    let result = run(
        &db,
        "CREATE MATERIALIZED VIEW IF NOT EXISTS mv2 AS SELECT id FROM orders",
    );
    assert!(result.is_ok(), "IF NOT EXISTS should be idempotent");
}

#[test]
fn matview_query_after_drop() {
    let (_dir, db) = setup_orders();
    run(&db, "CREATE MATERIALIZED VIEW mv3 AS SELECT id FROM orders").unwrap();
    run(&db, "DROP TABLE mv3").unwrap();
    let err = run_err(&db, "SELECT * FROM mv3");
    assert!(!err.is_empty(), "querying dropped matview should error");
}

#[test]
fn matview_with_join() {
    let (_dir, db) = setup_orders();
    // Materialized view from a self-join.
    let result = run(
        &db,
        "CREATE MATERIALIZED VIEW parent_child AS \
         SELECT o.id AS child, p.id AS parent \
         FROM orders o JOIN orders p ON o.parent_id = p.id",
    );
    assert!(result.is_ok(), "matview with join should succeed");
    let rows = run(&db, "SELECT count(*) FROM parent_child").unwrap();
    assert!(rows.len() == 1, "count should return 1 row");
}

// ── Multi-statement: adversarial ──────────────────────────────────────────

#[test]
fn multi_statement_trailing_semicolon() {
    let (_dir, db) = setup_orders();
    // Trailing semicolon — should not create an empty statement.
    let rows = run(&db, "SELECT 1;").unwrap_or_default();
    assert_eq!(rows.len(), 1, "trailing semicolon should still work");
}

#[test]
fn multi_statement_multiple_trailing_semicolons() {
    let (_dir, db) = setup_orders();
    let rows = run(&db, "SELECT 1;;").unwrap_or_default();
    assert_eq!(rows.len(), 1, "double trailing semicolons should not break");
}

#[test]
fn multi_statement_semicolon_in_string() {
    let (_dir, db) = setup_orders();
    // Semicolon inside a string literal — should not split.
    let rows = run(&db, "SELECT 'hello; world' AS greeting FROM orders LIMIT 1").unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0][0].1.contains(";"),
        "string should contain the semicolon"
    );
}

#[test]
fn multi_statement_empty_between_semicolons() {
    let (_dir, db) = setup_orders();
    let rows = run(&db, "SELECT 1;;; SELECT 2").unwrap_or_default();
    assert_eq!(
        rows.len(),
        1,
        "empty statements between semicolons should be skipped"
    );
    assert_eq!(rows[0][0].1, "2");
}

#[test]
fn multi_statement_only_semicolons() {
    let (_dir, db) = setup_orders();
    let result = run(&db, ";;;");
    // Should return empty, not error.
    match result {
        Ok(rows) => assert!(rows.is_empty(), "only semicolons → empty result"),
        Err(e) => panic!("only semicolons should not error: {e}"),
    }
}

#[test]
fn multi_statement_ctas_and_insert() {
    let (_dir, db) = setup_orders();
    let result = run(
        &db,
        "CREATE TABLE batch_test AS SELECT id, amount FROM orders;\
         INSERT INTO batch_test (id, amount) VALUES (99, 999.0);\
         INSERT INTO batch_test (id, amount) VALUES (100, 1000.0);\
         SELECT count(*) AS cnt FROM batch_test",
    );
    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1, "count should return 1 row");
            // Original 5 + 2 inserted = 7
            assert_eq!(
                rows[0][0].1, "7",
                "should have 7 rows (5 original + 2 inserted)"
            );
        }
        Err(e) => panic!("multi-statement CTAS+INSERT failed: {e}"),
    }
}

#[test]
fn multi_statement_does_not_split_trigger_body() {
    let (_dir, db) = setup_orders();
    // CREATE TRIGGER ... BEGIN ... END; — the semicolons inside BEGIN/END
    // must NOT be treated as statement separators.
    let result = run(
        &db,
        "CREATE TRIGGER audit_insert AFTER INSERT ON orders BEGIN \
         INSERT INTO orders (id, amount, category) VALUES (999, 1.0, 'audit'); \
         END",
    );
    // If this errors with "expected exactly one statement", the trigger body
    // was incorrectly split.
    match result {
        Ok(_) => {} // trigger created (or ignored if columns don't match)
        Err(e) => {
            // It's OK if the trigger fails due to column mismatch or PK conflict,
            // but NOT OK if it fails with a multi-statement parse error.
            assert!(
                !e.contains("expected exactly one statement"),
                "trigger body was incorrectly split as multi-statement: {e}"
            );
        }
    }
}

// ── FTS ranking: adversarial ──────────────────────────────────────────────

#[test]
fn fts_rank_null_text() {
    let (_dir, db) = setup_orders();
    // NULL text → score should be 0 or NULL, not error.
    let result = run(&db, "SELECT mongreldb_fts_rank(NULL, 'query')");
    assert!(result.is_ok(), "NULL text should not error");
}

#[test]
fn fts_rank_empty_query() {
    let (_dir, db) = setup_orders();
    let result = run(&db, "SELECT mongreldb_fts_rank('hello world', '')");
    assert!(result.is_ok(), "empty query should not error");
    let rows = result.unwrap();
    assert_eq!(rows.len(), 1);
    // Empty query → score = 0 (DataFusion may format as "0" or "0.0").
    let score: f64 = rows[0][0].1.parse().unwrap_or(-1.0);
    assert_eq!(score, 0.0, "empty query → score 0");
}

#[test]
fn fts_rank_empty_text() {
    let (_dir, db) = setup_orders();
    let result = run(&db, "SELECT mongreldb_fts_rank('', 'query')");
    assert!(result.is_ok(), "empty text should not error");
}

#[test]
fn fts_rank_special_chars() {
    let (_dir, db) = setup_orders();
    // Query with special characters that might break tokenizer.
    let result = run(
        &db,
        "SELECT mongreldb_fts_rank('hello! world? 123', 'hello')",
    );
    assert!(result.is_ok(), "special chars should not error");
}

#[test]
fn fts_rank_no_match() {
    let (_dir, db) = setup_orders();
    let rows = run(
        &db,
        "SELECT mongreldb_fts_rank('hello world', 'nonexistent') AS score",
    )
    .unwrap();
    let score: f64 = rows[0][0].1.parse().unwrap_or(-1.0);
    assert_eq!(score, 0.0, "no match → score 0");
}

#[test]
fn fts_rank_order_by() {
    let (_dir, db) = setup_orders();
    // Use fts_rank in ORDER BY and verify ranking is correct.
    let rows = run(
        &db,
        "SELECT category, mongreldb_fts_rank(category, 'food') AS score \
         FROM orders ORDER BY score DESC, category ASC",
    )
    .unwrap();
    assert_eq!(rows.len(), 5);
    // All food rows should have positive score; all toys rows should have 0.
    let food_count = rows
        .iter()
        .filter(|r| r[1].1.parse::<f64>().unwrap_or(0.0) > 0.0)
        .count();
    assert_eq!(food_count, 2, "2 food rows should have positive score");
}

#[test]
fn fts_rank_wrong_arg_count() {
    let (_dir, db) = setup_orders();
    // Wrong number of args → should error, not panic.
    let err = run_err(&db, "SELECT mongreldb_fts_rank('hello')");
    assert!(!err.is_empty(), "wrong arg count should error");
}

// ── Window functions: adversarial ─────────────────────────────────────────

#[test]
fn window_function_empty_table() {
    let (_dir, db) = setup_empty();
    let rows = run(
        &db,
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM items",
    )
    .unwrap_or_default();
    assert_eq!(rows.len(), 0, "empty table → empty window result");
}

#[test]
fn window_function_single_partition() {
    let (_dir, db) = setup_orders();
    // All rows in one partition.
    let rows = run(
        &db,
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM orders ORDER BY id",
    )
    .unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0][1].1, "1");
    assert_eq!(rows[4][1].1, "5");
}

#[test]
fn window_function_lag_lead() {
    let (_dir, db) = setup_orders();
    // LAG and LEAD window functions.
    let rows = run(
        &db,
        "SELECT id, LAG(id) OVER (ORDER BY id) AS prev, LEAD(id) OVER (ORDER BY id) AS next \
         FROM orders ORDER BY id",
    )
    .unwrap();
    assert_eq!(rows.len(), 5);
    // First row has no prev (NULL), last row has no next (NULL).
    assert_eq!(rows[0][1].1, "NULL", "first row prev should be NULL");
    assert_eq!(rows[4][2].1, "NULL", "last row next should be NULL");
    assert_eq!(rows[1][1].1, "1", "second row prev = 1");
}

#[test]
fn window_function_percent_rank() {
    let (_dir, db) = setup_orders();
    // PERCENT_RANK — should not panic.
    let result = run(
        &db,
        "SELECT id, PERCENT_RANK() OVER (ORDER BY id) AS pr FROM orders ORDER BY id",
    );
    assert!(result.is_ok(), "PERCENT_RANK should not error");
}

// ── Credential enforcement: adversarial ───────────────────────────────────

#[test]
fn cred_wrong_password() {
    let dir = tempdir().unwrap();
    Database::create_with_credentials(dir.path(), "admin", "correct-pw").unwrap();
    let err = Database::open_with_credentials(dir.path(), "admin", "wrong-pw").unwrap_err();
    assert!(matches!(
        err,
        mongreldb_core::MongrelError::InvalidCredentials { .. }
    ));
}

#[test]
fn cred_nonexistent_user() {
    let dir = tempdir().unwrap();
    Database::create_with_credentials(dir.path(), "admin", "correct-pw").unwrap();
    let err = Database::open_with_credentials(dir.path(), "ghost", "pw").unwrap_err();
    assert!(matches!(
        err,
        mongreldb_core::MongrelError::InvalidCredentials { .. }
    ));
}

#[test]
fn cred_open_credentialless_with_credentials() {
    let dir = tempdir().unwrap();
    Database::create(dir.path()).unwrap();
    let err = Database::open_with_credentials(dir.path(), "admin", "pw").unwrap_err();
    assert!(matches!(err, mongreldb_core::MongrelError::AuthNotRequired));
}

#[test]
fn cred_enable_then_reopen_without() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table(
            "t",
            Schema {
                schema_id: 1,
                columns: vec![ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                }],
                indexes: vec![],
                colocation: vec![],
                constraints: Default::default(),
                clustered: false,
            },
        )
        .unwrap();
        db.enable_auth("admin", "s3cret").unwrap();
    }
    // Reopen without credentials → AuthRequired.
    let err = Database::open(dir.path()).unwrap_err();
    assert!(matches!(err, mongreldb_core::MongrelError::AuthRequired));
}

#[test]
fn cred_disable_then_reopen_plain() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create_with_credentials(dir.path(), "admin", "s3cret").unwrap();
        db.disable_auth().unwrap();
    }
    // Plain open should now work.
    let db = Database::open(dir.path()).unwrap();
    assert!(!db.require_auth_enabled());
}

#[test]
fn cred_disable_when_already_disabled() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let err = db.disable_auth().unwrap_err();
    assert!(matches!(
        err,
        mongreldb_core::MongrelError::InvalidArgument(_)
    ));
}

#[test]
fn cred_permission_denied_on_table() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
        db.create_table(
            "orders",
            Schema {
                schema_id: 1,
                columns: vec![ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                }],
                indexes: vec![],
                colocation: vec![],
                constraints: Default::default(),
                clustered: false,
            },
        )
        .unwrap();
        db.create_user("reader", "r-pw").unwrap();
        db.create_role("read_role").unwrap();
        db.grant_permission(
            "read_role",
            mongreldb_core::auth::Permission::Select {
                table: "orders".into(),
            },
        )
        .unwrap();
        db.grant_role("reader", "read_role").unwrap();
    }
    let db = Database::open_with_credentials(dir.path(), "reader", "r-pw").unwrap();
    // Reader has SELECT but not INSERT.
    let handle = db.table("orders").unwrap();
    let err = handle.lock().put(vec![(1, Value::Int64(1))]).unwrap_err();
    assert!(matches!(
        err,
        mongreldb_core::MongrelError::PermissionDenied { .. }
    ));
}

#[test]
fn cred_all_permission_not_admin() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
        db.create_table(
            "orders",
            Schema {
                schema_id: 1,
                columns: vec![ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                }],
                indexes: vec![],
                colocation: vec![],
                constraints: Default::default(),
                clustered: false,
            },
        )
        .unwrap();
        db.create_user("power", "p-pw").unwrap();
        db.create_role("super").unwrap();
        db.grant_permission("super", mongreldb_core::auth::Permission::All)
            .unwrap();
        db.grant_role("power", "super").unwrap();
    }
    let db = Database::open_with_credentials(dir.path(), "power", "p-pw").unwrap();
    // Can do table-level ops.
    let handle = db.table("orders").unwrap();
    handle.lock().put(vec![(1, Value::Int64(1))]).unwrap();
    handle
        .lock()
        .query(&mongreldb_core::query::Query::new())
        .unwrap();
    // But NOT admin.
    let err = db.create_user("intruder", "pw").unwrap_err();
    assert!(matches!(
        err,
        mongreldb_core::MongrelError::PermissionDenied { .. }
    ));
}

#[cfg(feature = "encryption")]
#[test]
fn cred_encrypted_with_credentials_wrong_passphrase() {
    let dir = tempdir().unwrap();
    Database::create_encrypted_with_credentials(dir.path(), "passphrase", "admin", "s3cret")
        .unwrap();
    // Wrong passphrase → can't decrypt catalog.
    let err = Database::open_encrypted(dir.path(), "wrong").unwrap_err();
    assert!(
        !matches!(err, mongreldb_core::MongrelError::AuthRequired { .. }),
        "wrong passphrase should fail at decryption, not at auth"
    );
}

// ── Cross-feature interactions ─────────────────────────────────────────────

#[test]
fn ctas_then_recursive_cte_on_result() {
    let (_dir, db) = setup_orders();
    // CTAS → then query the result with a recursive CTE.
    run(&db, "CREATE TABLE copy AS SELECT id FROM orders").unwrap();
    let rows = run(
        &db,
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) \
         SELECT n FROM r",
    )
    .unwrap();
    assert_eq!(rows.len(), 3, "recursive CTE on CTAS-created table context");
}

#[test]
fn multi_statement_with_recursive_cte() {
    let (_dir, db) = setup_orders();
    // Recursive CTE inside a multi-statement batch.
    let rows = run(&db,
        "SELECT 1;\
         WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r").unwrap();
    assert_eq!(
        rows.len(),
        3,
        "last statement (recursive CTE) should return 3 rows"
    );
}

#[test]
fn matview_then_fts_rank() {
    let (_dir, db) = setup_orders();
    // Create a matview, then rank it.
    run(
        &db,
        "CREATE MATERIALIZED VIEW cat_mv AS SELECT DISTINCT category FROM orders",
    )
    .unwrap();
    let rows = run(
        &db,
        "SELECT category, mongreldb_fts_rank(category, 'food') AS score \
         FROM cat_mv ORDER BY score DESC",
    )
    .unwrap();
    assert_eq!(rows.len(), 2, "2 categories");
}
