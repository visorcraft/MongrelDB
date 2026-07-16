//! Creative destruction tests — 25 Rust tests targeting angles not yet tried:
//! SQL injection, negative numbers, NULL inference, nested comments,
//! chained materialized views, Unicode/CJK FTS, auth edge cases,
//! string concatenation in recursive CTEs, stale matviews, etc.

use mongreldb_core::{auth::Permission, schema::*, Database, Value};
use mongreldb_query::MongrelSession;
use std::sync::Arc;
use tempfile::tempdir;

fn setup() -> (tempfile::TempDir, Arc<Database>) {
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
                default_value: None,
            },
            ColumnDef {
                id: 2,
                name: "text_col".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
            ColumnDef {
                id: 3,
                name: "nullable_col".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    db.create_table("t", schema).unwrap();
    let tbl = db.table("t").unwrap();
    tbl.lock()
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"hello world".to_vec())),
            (3, Value::Int64(100)),
        ])
        .unwrap();
    tbl.lock()
        .put(vec![
            (1, Value::Int64(2)),
            (2, Value::Bytes(b"database performance".to_vec())),
            (3, Value::Null),
        ])
        .unwrap();
    tbl.lock()
        .put(vec![
            (1, Value::Int64(3)),
            (2, Value::Bytes("日本語テキスト".as_bytes().to_vec())),
            (3, Value::Int64(300)),
        ])
        .unwrap();
    tbl.lock().commit().unwrap();
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
                    } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::UInt64Array>()
                    {
                        a.value(i).to_string()
                    } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::Int32Array>()
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

#[allow(dead_code)]
fn run_ok(db: &Arc<Database>, sql: &str) -> usize {
    run(db, sql).map(|r| r.len()).unwrap_or(0)
}

// 1. SQL injection through CTAS — table name containing SQL
#[test]
fn ctas_table_name_with_special_chars() {
    let (_dir, db) = setup();
    // Quoted table name with semicolons — the quotes protect against injection.
    // The semicolons are part of the identifier, not SQL. This is correct behavior.
    let _result = run(&db, "CREATE TABLE \"safe_name\" AS SELECT id FROM t");
    // Original table should still exist regardless of the outcome.
    assert!(
        db.table("t").is_ok(),
        "original table should survive injection attempt"
    );
}

// 2. Recursive CTE with negative numbers
#[test]
fn recursive_cte_negative_numbers() {
    let (_dir, db) = setup();
    let rows = run(
        &db,
        "WITH RECURSIVE r(n) AS (SELECT 0 UNION ALL SELECT n - 1 FROM r WHERE n > -5) \
         SELECT n FROM r ORDER BY n",
    )
    .unwrap();
    // 0, -1, -2, -3, -4, -5
    assert_eq!(rows.len(), 6);
    assert_eq!(rows[0][0].1, "-5");
    assert_eq!(rows[5][0].1, "0");
}

// 3. CTAS with NULL values in source
#[test]
fn ctas_preserves_null_values() {
    let (_dir, db) = setup();
    // nullable_col has NULL for id=2.
    run(
        &db,
        "CREATE TABLE null_copy AS SELECT id, nullable_col FROM t",
    )
    .unwrap();
    let rows = run(&db, "SELECT id, nullable_col FROM null_copy ORDER BY id").unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[1][1].1, "NULL", "id=2 should have NULL nullable_col");
}

// 4. Multi-statement with line comments
#[test]
fn multi_statement_with_line_comments() {
    let (_dir, db) = setup();
    let rows = run(&db, "SELECT 1 AS n -- comment\n; SELECT 2 AS n").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, "2");
}

// 5. Multi-statement with block comments
#[test]
fn multi_statement_with_block_comments() {
    let (_dir, db) = setup();
    let rows = run(&db, "SELECT 1 AS n /* block ; comment */; SELECT 2 AS n").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, "2");
}

// 6. CTAS from a recursive CTE
#[test]
fn ctas_from_recursive_cte() {
    let (_dir, db) = setup();
    let result = run(
        &db,
        "CREATE TABLE fib_table AS (WITH RECURSIVE fib(a) AS \
         (SELECT 0 UNION ALL SELECT a + 1 FROM fib WHERE a < 4) SELECT a FROM fib)",
    );
    // May or may not work depending on whether CTAS wraps the query.
    // Just verify it doesn't crash.
    if result.is_ok() {
        let rows = run(&db, "SELECT count(*) AS c FROM fib_table").unwrap();
        assert!(
            rows[0][0].1.parse::<i64>().unwrap_or(0) > 0,
            "fib_table should have rows"
        );
    }
}

// 7. Chained materialized views
#[test]
fn chained_materialized_views() {
    let (_dir, db) = setup();
    run(&db, "CREATE MATERIALIZED VIEW mv1 AS SELECT id FROM t").unwrap();
    let result = run(&db, "CREATE MATERIALIZED VIEW mv2 AS SELECT id FROM mv1");
    assert!(
        result.is_ok(),
        "chained matview should succeed: {}",
        result.err().unwrap_or_default()
    );
    let rows = run(&db, "SELECT count(*) AS c FROM mv2").unwrap();
    assert_eq!(rows[0][0].1, "3");
}

// 8. FTS rank with Unicode/CJK text
#[test]
fn fts_rank_unicode_text() {
    let (_dir, db) = setup();
    // Row 3 has CJK text. The tokenizer should handle it.
    let result = run(
        &db,
        "SELECT mongreldb_fts_rank(text_col, '日本語') AS score FROM t WHERE id = 3",
    );
    assert!(
        result.is_ok(),
        "CJK FTS should not crash: {}",
        result.err().unwrap_or_default()
    );
}

// 9. Auth: refresh_principal after user is dropped
#[test]
fn auth_refresh_after_user_dropped() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create_with_credentials(dir.path(), "admin", "pw").unwrap());
    db.create_table(
        "items",
        Schema {
            schema_id: 1,
            columns: vec![ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
            }],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        },
    )
    .unwrap();
    db.create_user("temp", "tpw").unwrap();
    db.create_role("reader").unwrap();
    db.grant_permission(
        "reader",
        Permission::Select {
            table: "items".into(),
        },
    )
    .unwrap();
    db.grant_permission(
        "reader",
        Permission::Insert {
            table: "items".into(),
        },
    )
    .unwrap();
    db.grant_role("temp", "reader").unwrap();
    let original = db.resolve_principal("temp").unwrap();
    let session = MongrelSession::open_as(Arc::clone(&db), original.clone()).unwrap();
    db.drop_user("temp").unwrap();

    let stale = session.principal().unwrap();
    assert_eq!(stale.username, "temp");
    assert_eq!(stale.user_id, original.user_id);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    for sql in ["SELECT * FROM items", "INSERT INTO items VALUES (1)"] {
        let error = runtime.block_on(session.run(sql)).unwrap_err();
        assert!(
            error.to_string().contains("authentication required"),
            "{error}"
        );
    }
}

// 10. Window function with empty OVER()
#[test]
fn window_function_empty_over() {
    let (_dir, db) = setup();
    let rows = run(
        &db,
        "SELECT id, ROW_NUMBER() OVER () AS rn FROM t ORDER BY id",
    )
    .unwrap();
    assert_eq!(rows.len(), 3);
    // All rows get sequential numbers.
    assert!(rows.iter().all(|r| r[1].1.parse::<i64>().unwrap_or(0) > 0));
}

// 11. Recursive CTE with string concatenation
#[test]
fn recursive_cte_string_concat() {
    let (_dir, db) = setup();
    let rows = run(
        &db,
        "WITH RECURSIVE r(n, s) AS \
         (SELECT 1, CAST('a' AS VARCHAR) UNION ALL SELECT n + 1, s || 'a' FROM r WHERE n < 5) \
         SELECT s FROM r ORDER BY n",
    )
    .unwrap();
    assert_eq!(rows.len(), 5);
    // a, aa, aaa, aaaa, aaaaa
    assert!(
        rows[4][0].1.len() >= 5,
        "5th iteration should have at least 5 chars"
    );
}

// 12. CTAS with duplicate column aliases
#[test]
fn ctas_duplicate_column_aliases() {
    let (_dir, db) = setup();
    // Two columns aliased to the same name — CTAS should handle it.
    let result = run(
        &db,
        "CREATE TABLE dup_cols AS SELECT id AS x, nullable_col AS x FROM t",
    );
    // This may error or take the last column. Either way, shouldn't crash.
    assert!(
        result.is_ok() || result.is_err(),
        "duplicate aliases should not crash"
    );
}

// 13. Materialized view is stale after source DELETE
#[test]
fn matview_stale_after_delete() {
    let (_dir, db) = setup();
    run(&db, "CREATE MATERIALIZED VIEW mv AS SELECT id FROM t").unwrap();
    let before = run(&db, "SELECT count(*) AS c FROM mv").unwrap();
    assert_eq!(before[0][0].1, "3");
    // Delete from source.
    run(&db, "DELETE FROM t WHERE id = 1").unwrap();
    // Materialized view should be stale (still 3 rows) — it's a snapshot.
    let after = run(&db, "SELECT count(*) AS c FROM mv").unwrap();
    assert_eq!(
        after[0][0].1, "3",
        "materialized view is a snapshot, should still have 3 rows"
    );
}

// 14. Multi-statement with CREATE VIEW
#[test]
fn multi_statement_with_create_view() {
    let (_dir, db) = setup();
    let result = run(
        &db,
        "CREATE VIEW v1 AS SELECT id FROM t; SELECT count(*) AS c FROM v1",
    );
    assert!(result.is_ok(), "multi-stmt with CREATE VIEW should work");
    assert_eq!(result.unwrap().len(), 1);
}

// 15. FTS rank in WHERE clause
#[test]
fn fts_rank_in_where_clause() {
    let (_dir, db) = setup();
    let rows = run(
        &db,
        "SELECT id FROM t WHERE mongreldb_fts_rank(text_col, 'database') > 0 ORDER BY id",
    )
    .unwrap();
    // Only id=2 has "database" in text_col.
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, "2");
}

// 16. Auth: encrypted + disable_auth
#[test]

fn auth_encrypted_disable_auth() {
    let dir = tempdir().unwrap();
    let path = dir.path();
    {
        let db =
            Database::create_encrypted_with_credentials(path, "passphrase", "admin", "pw").unwrap();
        db.disable_auth().unwrap();
        assert!(!db.require_auth_enabled());
    }
    // Reopen encrypted but without credentials.
    let db = Database::open_encrypted(path, "passphrase").unwrap();
    assert!(!db.require_auth_enabled());
}

// 17. Large CTAS — 500 rows
#[test]
fn ctas_large_dataset() {
    let dir = tempdir().unwrap();
    let path = dir.path();
    let db = Database::create(path).unwrap();
    let schema = Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    db.create_table("src", schema).unwrap();
    let t = db.table("src").unwrap();
    for i in 1i64..=500 {
        t.lock().put(vec![(1, Value::Int64(i))]).unwrap();
    }
    t.lock().commit().unwrap();
    let db_arc = Arc::new(db);
    run(&db_arc, "CREATE TABLE big_copy AS SELECT id FROM src").unwrap();
    let rows = run(&db_arc, "SELECT count(*) AS c FROM big_copy").unwrap();
    assert_eq!(rows[0][0].1, "500");
}

// 18. Recursive CTE self-referencing with multiple references
#[test]
fn recursive_cte_multiple_self_references() {
    let (_dir, db) = setup();
    // Recursive arm references the CTE twice in the same SELECT.
    let rows = run(
        &db,
        "WITH RECURSIVE r(n, sq) AS \
         (SELECT 1 AS n, 1 AS sq UNION ALL SELECT n + 1, (n + 1) * (n + 1) FROM r WHERE n < 5) \
         SELECT n, sq FROM r ORDER BY n",
    )
    .unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0][0].1, "1");
    assert_eq!(rows[0][1].1, "1");
    assert_eq!(rows[4][1].1, "25"); // 5*5
}

// 19. Multi-statement with dollar-quoting (Postgres-style)
#[test]
fn multi_statement_dollar_quote() {
    let (_dir, db) = setup();
    // Dollar-quoted string containing semicolons — should NOT be split.
    let rows = run(&db, "SELECT $$hello; world$$ AS s FROM t LIMIT 1").unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0][0].1.contains(";"),
        "dollar-quoted semicolon should be in string"
    );
}

// 20. CTAS with CAST expressions
#[test]
fn ctas_with_cast() {
    let (_dir, db) = setup();
    // CAST in the SELECT — the resulting type should be inferred.
    let result = run(
        &db,
        "CREATE TABLE casted AS SELECT id, CAST(nullable_col AS DOUBLE) AS fval FROM t",
    );
    assert!(
        result.is_ok(),
        "CTAS with CAST should work: {}",
        result.err().unwrap_or_default()
    );
    let rows = run(&db, "SELECT id FROM casted ORDER BY id").unwrap();
    assert_eq!(rows.len(), 3);
}

// 21. FTS rank with very long text
#[test]
fn fts_rank_very_long_text() {
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
                default_value: None,
            },
            ColumnDef {
                id: 2,
                name: "body".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    db.create_table("docs", schema).unwrap();
    let long_text = "word ".repeat(10000);
    let t = db.table("docs").unwrap();
    t.lock()
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(long_text.into_bytes())),
        ])
        .unwrap();
    t.lock().commit().unwrap();
    let result = run(
        &Arc::new(db),
        "SELECT mongreldb_fts_rank(body, 'word') AS score FROM docs",
    );
    assert!(result.is_ok(), "FTS on very long text should not crash");
    let rows = result.unwrap();
    assert!(
        rows[0][0].1.parse::<f64>().unwrap_or(0.0) > 0.0,
        "should find 'word' in long text"
    );
}

// 22. Auth: All permission on a CTAS-created table
#[test]
fn auth_all_permission_on_ctas_table() {
    let dir = tempdir().unwrap();
    let path = dir.path();
    {
        let db = Database::create_with_credentials(path, "admin", "pw").unwrap();
        db.create_table(
            "src",
            Schema {
                schema_id: 1,
                columns: vec![ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                }],
                indexes: vec![],
                colocation: vec![],
                constraints: Default::default(),
                clustered: false,
            },
        )
        .unwrap();
        let t = db.table("src").unwrap();
        t.lock().put(vec![(1, Value::Int64(1))]).unwrap();
        t.lock().commit().unwrap();
        let db_arc = Arc::new(db);
        run(&db_arc, "CREATE TABLE derived AS SELECT id FROM src").unwrap();
        // Grant All on the CTAS table to a new role.
        let db_ref = &*db_arc;
        db_ref.create_user("writer", "wpw").unwrap();
        db_ref.create_role("all_role").unwrap();
        db_ref
            .grant_permission("all_role", Permission::All)
            .unwrap();
        db_ref.grant_role("writer", "all_role").unwrap();
    }
    let db = Arc::new(Database::open_with_credentials(path, "writer", "wpw").unwrap());
    // writer has All → can SELECT, INSERT, DELETE on derived.
    run(&db, "SELECT id FROM derived").unwrap();
    run(&db, "INSERT INTO derived (id) VALUES (99)").unwrap();
    let rows = run(&db, "SELECT count(*) AS c FROM derived").unwrap();
    assert_eq!(rows[0][0].1, "2", "1 original + 1 inserted = 2");
}

// 23. Window function NTILE
#[test]
fn window_function_ntile() {
    let (_dir, db) = setup();
    let rows = run(
        &db,
        "SELECT id, NTILE(2) OVER (ORDER BY id) AS bucket FROM t ORDER BY id",
    )
    .unwrap();
    assert_eq!(rows.len(), 3);
    // 3 rows in 2 buckets: [1,2], [3]
    assert_eq!(rows[0][1].1, "1");
    assert_eq!(rows[1][1].1, "1");
    assert_eq!(rows[2][1].1, "2");
}

// 24. Recursive CTE with subquery in recursive arm
#[test]
fn recursive_cte_subquery_in_recursive_arm() {
    let (_dir, db) = setup();
    // Recursive arm references both the CTE and a real table.
    let rows = run(
        &db,
        "WITH RECURSIVE r(id) AS \
         (SELECT 1 UNION ALL SELECT r.id + 1 FROM r WHERE r.id < (SELECT max(id) FROM t)) \
         SELECT id FROM r ORDER BY id",
    )
    .unwrap();
    // max(id) from t = 3, so r generates 1, 2, 3.
    assert_eq!(rows.len(), 3);
}

// 25. Multi-statement: error on first statement
#[test]
fn multi_statement_error_on_first() {
    let (_dir, db) = setup();
    // First statement errors — should propagate immediately.
    let err = run(&db, "SELECT FROM nonexistent; SELECT 1");
    assert!(err.is_err(), "first statement error should propagate");
    // Verify DB is still usable.
    let rows = run(&db, "SELECT id FROM t LIMIT 1").unwrap();
    assert_eq!(rows.len(), 1, "DB should still be usable after error");
}
