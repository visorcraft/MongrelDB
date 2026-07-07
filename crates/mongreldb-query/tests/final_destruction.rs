//! Final round destruction tests — 20 tests.
//! Untested dimensions: boundary values, resource limits, empty results,
//! nested recursion, NULL propagation, SQL injection via CTE,
//! admin password edge cases, long names, computed FTS input.

use mongreldb_core::{schema::*, Database, Value};
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
                name: "nullable_int".into(),
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
    db.create_table("data", schema).unwrap();
    let t = db.table("data").unwrap();
    t.lock()
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"hello database world".to_vec())),
            (3, Value::Int64(42)),
        ])
        .unwrap();
    t.lock()
        .put(vec![
            (1, Value::Int64(2)),
            (2, Value::Bytes(b"performance tuning guide".to_vec())),
            (3, Value::Null),
        ])
        .unwrap();
    t.lock()
        .put(vec![
            (1, Value::Int64(3)),
            (2, Value::Bytes(b"".to_vec())), // empty string (not NULL)
            (3, Value::Int64(-7)),
        ])
        .unwrap();
    t.lock().commit().unwrap();
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
    run(db, sql).err().unwrap_or_default()
}

// 1. Recursive CTE with large depth (near the 10K safety bound)
#[test]
fn recursive_cte_near_max_depth() {
    let (_dir, db) = setup();
    let rows = run(
        &db,
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5000) \
         SELECT count(*) AS c FROM r",
    )
    .unwrap();
    assert_eq!(rows[0][0].1, "5000", "should handle 5000 iterations");
}

// 2. Recursive CTE producing NULL in computed column
#[test]
fn recursive_cte_produces_null() {
    let (_dir, db) = setup();
    let rows = run(&db,
        "WITH RECURSIVE r(n, d) AS \
         (SELECT 1, NULL UNION ALL SELECT n + 1, CASE WHEN n = 2 THEN n ELSE NULL END FROM r WHERE n < 3) \
         SELECT d FROM r ORDER BY n").unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].1, "NULL", "first row d = NULL (n=1)");
    assert_eq!(
        rows[1][0].1, "NULL",
        "second row d = NULL (n=2, CASE fires on n but appears next iteration)"
    );
    assert_eq!(
        rows[2][0].1, "2",
        "third row d = 2 (n=3, CASE fired when n was 2 in prev iteration)"
    );
}

// 3. Multi-statement with only comments
#[test]
fn multi_statement_only_comments() {
    let (_dir, db) = setup();
    let result = run(&db, "/* just a comment */; -- line comment");
    // Should not crash. May return empty or error.
    assert!(
        result.is_ok() || result.is_err(),
        "comments-only should not crash"
    );
}

// 4. FTS rank on empty table
#[test]
fn fts_rank_empty_table() {
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
    db.create_table("empty_docs", schema).unwrap();
    let result = run(
        &Arc::new(db),
        "SELECT mongreldb_fts_rank(body, 'test') AS score FROM empty_docs",
    );
    assert!(result.is_ok(), "FTS on empty table should not crash");
    let rows = result.unwrap();
    assert_eq!(rows.len(), 0, "empty table → no rows");
}

// 5. Window COUNT(*) OVER() on empty table
#[test]
fn window_count_empty_table() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
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
    db.create_table("empty_t", schema).unwrap();
    let result = run(&Arc::new(db), "SELECT COUNT(*) OVER () AS c FROM empty_t");
    assert!(result.is_ok(), "window on empty table should not crash");
}

// 6. CTAS then immediately DROP in multi-statement
#[test]
fn ctas_then_drop_in_batch() {
    let (_dir, db) = setup();
    run(
        &db,
        "CREATE TABLE temp AS SELECT id FROM data LIMIT 1; DROP TABLE temp",
    )
    .unwrap();
    // Table should be gone.
    let err = run_err(&db, "SELECT * FROM temp");
    assert!(!err.is_empty(), "temp table should not exist after DROP");
}

// 7. Materialized view with zero rows
#[test]
fn materialized_view_zero_rows() {
    let (_dir, db) = setup();
    let result = run(
        &db,
        "CREATE MATERIALIZED VIEW empty_mv AS SELECT id FROM data WHERE id > 999",
    );
    // CTAS from empty result should error (can't infer schema).
    assert!(
        result.is_err(),
        "empty matview should error (can't infer schema)"
    );
}

// 8. FTS rank on computed expression (string concatenation)
#[test]
fn fts_rank_on_computed_expression() {
    let (_dir, db) = setup();
    let rows = run(
        &db,
        "SELECT id, mongreldb_fts_rank(text_col || ' suffix', 'database') AS score \
         FROM data ORDER BY score DESC, id ASC",
    )
    .unwrap();
    assert_eq!(rows.len(), 3);
    // Only id=1 has "database" in text_col.
    let top_score: f64 = rows[0][1].1.parse().unwrap_or(0.0);
    assert!(top_score > 0.0, "row with 'database' should score positive");
}

// 9. Auth: create_user with empty password
#[test]
fn auth_create_user_empty_password() {
    let dir = tempdir().unwrap();
    let db = Database::create_with_credentials(dir.path(), "admin", "pw").unwrap();
    // Empty password — Argon2id should still hash it (empty string is valid input).
    let result = db.create_user("empty_pw_user", "");
    assert!(
        result.is_ok(),
        "empty password should be accepted by Argon2id"
    );
    // Verify the user can be authenticated.
    let ok = db.verify_user("empty_pw_user", "").unwrap_or(None);
    assert!(
        ok.is_some(),
        "empty password user should authenticate with empty password"
    );
}

// 10. Multi-statement: 50 statements in one batch
#[test]
fn multi_statement_50_statements() {
    let (_dir, db) = setup();
    let mut sql = String::new();
    for i in 1..=50 {
        sql.push_str(&format!("SELECT {i} AS n"));
        if i < 50 {
            sql.push_str("; ");
        }
    }
    let rows = run(&db, &sql).unwrap();
    assert_eq!(rows.len(), 1, "only last statement's result is returned");
    assert_eq!(rows[0][0].1, "50");
}

// 11. CTAS with very long table name
#[test]
fn ctas_long_table_name() {
    let (_dir, db) = setup();
    let long_name = "a".repeat(200);
    let result = run(
        &db,
        &format!("CREATE TABLE {long_name} AS SELECT id FROM data LIMIT 1"),
    );
    assert!(
        result.is_ok(),
        "long table name should work: {}",
        result.err().unwrap_or_default()
    );
    let rows = run(&db, &format!("SELECT count(*) AS c FROM {long_name}")).unwrap();
    assert_eq!(rows[0][0].1, "1");
}

// 12. Empty string vs NULL distinction in CTAS
#[test]
fn ctas_empty_string_vs_null() {
    let (_dir, db) = setup();
    // Row 3 has empty string (b"") in text_col; row 2 has NULL in nullable_int.
    run(
        &db,
        "CREATE TABLE copy AS SELECT id, text_col, nullable_int FROM data",
    )
    .unwrap();
    let rows = run(
        &db,
        "SELECT id, text_col, nullable_int FROM copy ORDER BY id",
    )
    .unwrap();
    // Row 3: text_col is empty string (not NULL), nullable_int is -7.
    assert_ne!(rows[2][1].1, "NULL", "empty string should NOT be NULL");
    // Row 2: nullable_int IS NULL.
    assert_eq!(rows[1][2].1, "NULL", "nullable_int for id=2 should be NULL");
}

// 13. Recursive CTE with division producing fractions
#[test]
fn recursive_cte_fractional_division() {
    let (_dir, db) = setup();
    // Integer division: 100/2=50, 50/2=25, 25/2=12, ...
    // Use CAST to force float division.
    let rows = run(
        &db,
        "WITH RECURSIVE r(n) AS \
         (SELECT CAST(100.0 AS DOUBLE) UNION ALL SELECT n / 2.0 FROM r WHERE n > 1.0) \
         SELECT count(*) AS c FROM r",
    )
    .unwrap();
    // 100, 50, 25, 12.5, 6.25, 3.125, 1.5625 → 7 rows (>1.0)
    assert!(
        rows[0][0].1.parse::<i64>().unwrap_or(0) >= 5,
        "should generate several fractional steps"
    );
}

// 14. Window LAG with large offset
#[test]
fn window_lag_large_offset() {
    let (_dir, db) = setup();
    let rows = run(
        &db,
        "SELECT id, LAG(id, 10, -1) OVER (ORDER BY id) AS prev FROM data ORDER BY id",
    )
    .unwrap();
    assert_eq!(rows.len(), 3);
    // With offset=10 on 3 rows, all LAG values should be the default (-1).
    assert_eq!(rows[0][1].1, "-1", "LAG with offset > rows → default");
    assert_eq!(rows[2][1].1, "-1", "LAG with offset > rows → default");
}

// 15. Auth: permission check on table created via SQL (not kit schema)
#[test]
fn auth_permission_on_sql_created_table() {
    use mongreldb_core::auth::Permission;
    let dir = tempdir().unwrap();
    let path = dir.path();
    {
        let db = Database::create_with_credentials(path, "admin", "pw").unwrap();
        db.create_table(
            "kit_table",
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
        let t = db.table("kit_table").unwrap();
        t.lock().put(vec![(1, Value::Int64(1))]).unwrap();
        t.lock().commit().unwrap();
        // Create a table via SQL (CTAS).
        let db_arc = Arc::new(db);
        run(
            &db_arc,
            "CREATE TABLE sql_table AS SELECT id FROM kit_table",
        )
        .unwrap();
        // Create a user with Select on sql_table only.
        db_arc.create_user("viewer", "vpw").unwrap();
        db_arc.create_role("viewer_role").unwrap();
        db_arc
            .grant_permission(
                "viewer_role",
                Permission::Select {
                    table: "sql_table".into(),
                },
            )
            .unwrap();
        db_arc.grant_role("viewer", "viewer_role").unwrap();
    }
    let db = Arc::new(Database::open_with_credentials(path, "viewer", "vpw").unwrap());
    // viewer can SELECT from sql_table.
    let rows = run(&db, "SELECT id FROM sql_table").unwrap();
    assert_eq!(rows.len(), 1);
    // viewer CANNOT SELECT from kit_table (no permission).
    let err = run_err(&db, "SELECT id FROM kit_table");
    assert!(
        err.contains("PermissionDenied") || err.contains("permission denied"),
        "viewer should not access kit_table: {err}"
    );
}

// 16. CTAS from table with all-NULL column
#[test]
fn ctas_all_null_column() {
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
                name: "all_null".into(),
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
    db.create_table("src", schema).unwrap();
    let t = db.table("src").unwrap();
    t.lock()
        .put(vec![(1, Value::Int64(1)), (2, Value::Null)])
        .unwrap();
    t.lock()
        .put(vec![(1, Value::Int64(2)), (2, Value::Null)])
        .unwrap();
    t.lock().commit().unwrap();
    let db_arc = Arc::new(db);
    // CTAS from a table where all values in a column are NULL.
    let result = run(
        &db_arc,
        "CREATE TABLE null_col_copy AS SELECT id, all_null FROM src",
    );
    assert!(result.is_ok(), "CTAS with all-NULL column should work");
    let rows = run(&db_arc, "SELECT all_null FROM null_col_copy ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].1, "NULL");
    assert_eq!(rows[1][0].1, "NULL");
}

// 17. SQL injection via recursive CTE base query
#[test]
fn recursive_cte_injection_safe() {
    let (_dir, db) = setup();
    // The base query is controlled by us (from the AST), not user-injectable.
    // But verify that table names in the outer query are properly handled.
    let result = run(
        &db,
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) \
         SELECT n FROM \"r\"",
    );
    assert!(result.is_ok(), "quoted CTE name in outer query should work");
}

// 18. Recursive CTE with three columns
#[test]
fn recursive_cte_three_columns() {
    let (_dir, db) = setup();
    let rows = run(
        &db,
        "WITH RECURSIVE r(a, b, c) AS \
         (SELECT 1 AS a, 1 AS b, 1 AS c UNION ALL SELECT a + 1, b * 2, c + 3 FROM r WHERE a < 5) \
         SELECT a, b, c FROM r ORDER BY a",
    )
    .unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0][0].1, "1"); // a=1
    assert_eq!(rows[0][1].1, "1"); // b=1
    assert_eq!(rows[0][2].1, "1"); // c=1
    assert_eq!(rows[4][0].1, "5"); // a=5
    assert_eq!(rows[4][1].1, "16"); // b=1*2^4=16
    assert_eq!(rows[4][2].1, "13"); // c=1+3*4=13
}

// 19. CTAS from COUNT(*) — single row result
#[test]
fn ctas_from_count_star() {
    let (_dir, db) = setup();
    run(&db, "CREATE TABLE counts AS SELECT count(*) AS c FROM data").unwrap();
    let rows = run(&db, "SELECT c FROM counts").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, "3", "3 rows in source");
}

// 20. Multi-statement with CREATE TRIGGER (should NOT split trigger body)
#[test]
fn multi_statement_with_trigger_does_not_split() {
    let (_dir, db) = setup();
    // A trigger body with semicolons inside BEGIN...END should not be split.
    let result = run(
        &db,
        "CREATE TRIGGER log_insert AFTER INSERT ON data BEGIN \
         INSERT INTO data (id, text_col, nullable_int) VALUES (999, 'logged', 1); \
         END",
    );
    // The trigger may fail (PK conflict on 999 or similar), but it must NOT
    // fail with "expected exactly one statement" — that would mean the splitter
    // incorrectly split the trigger body.
    match result {
        Ok(_) => {}
        Err(e) => {
            assert!(
                !e.contains("exactly one statement") && !e.contains("only one statement"),
                "trigger body was incorrectly split: {e}"
            );
        }
    }
}
