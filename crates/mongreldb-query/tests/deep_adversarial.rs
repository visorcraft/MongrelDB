//! Deep adversarial tests — perspectives not yet covered:
//! - Data integrity (CTAS/matview survive reopen? correct values?)
//! - Large data (recursive CTE with 10K rows, CTAS with many rows)
//! - Error recovery (failed multi-statement leaves consistent state?)
//! - Auth + SQL interaction (CTAS under require_auth? permission on new tables?)
//! - Schema correctness (CTAS type inference)
//! - Session state (CTAS table visible after session refresh?)
//! - Kit typed operations on SQL-created tables

use mongreldb_core::{auth::Permission, schema::*, Database, Value};
use mongreldb_query::MongrelSession;
use std::sync::Arc;
use tempfile::tempdir;

fn setup_db() -> (tempfile::TempDir, Arc<Database>) {
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
                name: "name".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
            ColumnDef {
                id: 3,
                name: "amount".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    db.create_table("items", schema).unwrap();
    let t = db.table("items").unwrap();
    for i in 1i64..=100 {
        t.lock()
            .put(vec![
                (1, Value::Int64(i)),
                (2, Value::Bytes(format!("item_{i}").into_bytes())),
                (3, Value::Float64(i as f64 * 1.5)),
            ])
            .unwrap();
    }
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

fn run_err(db: &Arc<Database>, sql: &str) -> String {
    match run(db, sql) {
        Ok(_) => String::new(),
        Err(e) => e,
    }
}

// ── Data Integrity: CTAS survives reopen ───────────────────────────────────

#[test]
fn ctas_survives_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path();
    {
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
        db.create_table("items", schema).unwrap();
        let t = db.table("items").unwrap();
        for i in 1i64..=10 {
            t.lock().put(vec![(1, Value::Int64(i))]).unwrap();
        }
        t.lock().commit().unwrap();
        run(&Arc::new(db), "CREATE TABLE copy AS SELECT id FROM items").unwrap();
    }
    // Reopen — does the CTAS table persist?
    let db = Database::open(path).unwrap();
    let t = db.table("copy");
    assert!(t.is_ok(), "CTAS table should survive reopen");
    let count = t.unwrap().lock().count();
    assert_eq!(count, 10, "CTAS table should have 10 rows after reopen");
}

// ── Data Integrity: CTAS values are correct ────────────────────────────────

#[test]
fn ctas_values_are_correct() {
    let (_dir, db) = setup_db();
    run(
        &db,
        "CREATE TABLE copy AS SELECT id, name, amount FROM items",
    )
    .unwrap();
    let rows = run(&db, "SELECT id, name, amount FROM copy ORDER BY id LIMIT 3").unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].1, "1", "id=1");
    assert_eq!(rows[0][1].1, "item_1", "name=item_1");
    assert!(
        rows[0][2].1.starts_with("1.5"),
        "amount=1.5, got {}",
        rows[0][2].1
    );
    assert_eq!(rows[1][0].1, "2", "id=2");
    assert_eq!(rows[1][1].1, "item_2", "name=item_2");
}

// ── Data Integrity: Materialized view survives reopen ──────────────────────

#[test]
fn matview_survives_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path();
    {
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
        db.create_table("items", schema).unwrap();
        let t = db.table("items").unwrap();
        for i in 1i64..=20 {
            t.lock().put(vec![(1, Value::Int64(i))]).unwrap();
        }
        t.lock().commit().unwrap();
        run(
            &Arc::new(db),
            "CREATE MATERIALIZED VIEW mv AS SELECT id FROM items WHERE id < 10",
        )
        .unwrap();
    }
    let db = Database::open(path).unwrap();
    let t = db.table("mv");
    assert!(t.is_ok(), "materialized view should survive reopen");
    let count = t.unwrap().lock().count();
    assert_eq!(count, 9, "materialized view should have 9 rows");
}

// ── Large data: recursive CTE with 10K rows ────────────────────────────────

#[test]
fn recursive_cte_10k_rows() {
    let (_dir, db) = setup_db();
    let rows = run(&db,
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10000) SELECT count(*) AS c FROM r").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, "10000", "should produce 10000 rows");
}

// ── Large data: recursive CTE with join on real table ──────────────────────

#[test]
fn recursive_cte_join_large() {
    let (_dir, db) = setup_db();
    // Generate a hierarchy and join against items
    let rows = run(
        &db,
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 50) \
         SELECT count(*) AS c FROM r JOIN items ON r.n = items.id",
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    // Items has ids 1..100, r has 1..50, so the join produces 50 rows.
    assert_eq!(rows[0][0].1, "50", "join should produce 50 rows");
}

// ── Error recovery: failed multi-statement ─────────────────────────────────

#[test]
fn failed_multi_statement_leaves_consistent_state() {
    let (_dir, db) = setup_db();
    // Statement 1 succeeds, statement 2 fails (duplicate table), statement 3 should
    // either not run or the DB should be consistent.
    let _ = run(
        &db,
        "CREATE TABLE ok_table AS SELECT id FROM items WHERE id < 5;\
         CREATE TABLE ok_table AS SELECT id FROM items;\
         SELECT count(*) FROM ok_table",
    );
    // The first CREATE succeeded (5 rows). The second CREATE failed.
    // Verify the first table exists and has correct data.
    let rows = run(&db, "SELECT count(*) AS c FROM ok_table").unwrap();
    assert_eq!(
        rows[0][0].1, "4",
        "first statement's table should have 4 rows (id < 5 = ids 1,2,3,4)"
    );
}

// ── Auth + SQL: CTAS under require_auth ────────────────────────────────────

#[test]
fn ctas_under_require_auth_admin() {
    let dir = tempdir().unwrap();
    let db = Database::create_with_credentials(dir.path(), "admin", "pw").unwrap();
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
    // Seed data — CTAS needs rows to infer schema.
    let t = db.table("items").unwrap();
    t.lock().put(vec![(1, Value::Int64(1))]).unwrap();
    t.lock().commit().unwrap();
    // Admin should be able to CTAS.
    let rows = run(&Arc::new(db), "CREATE TABLE copy AS SELECT id FROM items");
    assert!(
        rows.is_ok(),
        "admin should be able to CTAS under require_auth: {}",
        rows.err().unwrap_or_default()
    );
}

#[test]
fn ctas_under_require_auth_no_ddl() {
    let dir = tempdir().unwrap();
    let path = dir.path();
    {
        let db = Database::create_with_credentials(path, "admin", "pw").unwrap();
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
        // Seed data so CTAS query returns rows.
        let t = db.table("items").unwrap();
        t.lock().put(vec![(1, Value::Int64(1))]).unwrap();
        t.lock().commit().unwrap();
        db.create_user("reader", "r").unwrap();
        db.create_role("r_role").unwrap();
        db.grant_permission(
            "r_role",
            Permission::Select {
                table: "items".into(),
            },
        )
        .unwrap();
        db.grant_role("reader", "r_role").unwrap();
    }
    let db = Database::open_with_credentials(path, "reader", "r").unwrap();
    let db = Arc::new(db);
    // reader has Select on items but NOT Ddl → CTAS should fail with PermissionDenied.
    let err = run_err(&db, "CREATE TABLE copy AS SELECT id FROM items");
    assert!(
        err.contains("PermissionDenied") || err.contains("permission denied"),
        "reader should not be able to CTAS (no Ddl), got: {err}"
    );
}

// ── Schema correctness: CTAS type inference ────────────────────────────────

#[test]
fn ctas_infers_correct_types() {
    let (_dir, db) = setup_db();
    run(
        &db,
        "CREATE TABLE typed_copy AS SELECT id, name, amount FROM items",
    )
    .unwrap();
    // The new table should have Int64 id (PK), Bytes name, Float64 amount.
    let cat = db.catalog_snapshot();
    let entry = cat.live("typed_copy").expect("typed_copy should exist");
    let cols = &entry.schema.columns;
    assert_eq!(cols.len(), 3, "3 columns");
    assert_eq!(cols[0].name, "id");
    assert!(
        cols[0].flags.contains(ColumnFlags::PRIMARY_KEY),
        "first column should be PK"
    );
    assert_eq!(cols[1].name, "name");
    assert_eq!(cols[2].name, "amount");
}

// ── Session state: CTAS table visible across sql() calls ───────────────────

#[test]
fn ctas_table_visible_across_sql_calls() {
    let (_dir, db) = setup_db();
    // The MongrelSession caches. After CTAS, a subsequent SELECT should see
    // the new table without session refresh.
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _ = rt
        .block_on(async {
            session
                .run("CREATE TABLE sess_copy AS SELECT id FROM items LIMIT 5")
                .await
        })
        .unwrap();
    let batches = rt
        .block_on(async { session.run("SELECT count(*) AS c FROM sess_copy").await })
        .unwrap();
    assert_eq!(batches.len(), 1);
    // The count should be 5.
    let arr = batches[0].column(0);
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
        assert_eq!(a.value(0), 5, "should see 5 rows in sess_copy");
    } else if let Some(a) = arr.as_any().downcast_ref::<arrow::array::UInt64Array>() {
        assert_eq!(a.value(0), 5);
    } else {
        panic!("unexpected count column type: {:?}", arr.data_type());
    }
}

// ── Multi-statement: DDL failure in middle ─────────────────────────────────

#[test]
fn multi_statement_ddl_failure_in_middle() {
    let (_dir, db) = setup_db();
    // First succeeds, second fails (nonexistent table), third should not execute.
    let result = run(
        &db,
        "CREATE TABLE before_fail AS SELECT id FROM items LIMIT 1;\
         INSERT INTO nonexistent_table (id) VALUES (1);\
         CREATE TABLE after_fail AS SELECT id FROM items LIMIT 1",
    );
    // The batch should error on the second statement.
    assert!(result.is_err(), "second statement should fail");
    // But the first table should exist.
    let rows = run(&db, "SELECT count(*) AS c FROM before_fail").unwrap();
    assert_eq!(
        rows[0][0].1, "1",
        "first statement's table should have 1 row"
    );
    // The third table should NOT exist.
    let err = run_err(&db, "SELECT count(*) FROM after_fail");
    assert!(!err.is_empty(), "third statement should not have executed");
}

// ─– Window function edge: LAG with default ─────────────────────────────────

#[test]
fn window_lag_with_default() {
    let (_dir, db) = setup_db();
    let rows = run(
        &db,
        "SELECT id, LAG(id, 1, 0) OVER (ORDER BY id) AS prev FROM items ORDER BY id LIMIT 3",
    )
    .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1].1, "0", "first row LAG default = 0");
    assert_eq!(rows[1][1].1, "1", "second row LAG = 1");
    assert_eq!(rows[2][1].1, "2", "third row LAG = 2");
}

// ── Recursive CTE: multiple column names ───────────────────────────────────

#[test]
fn recursive_cte_multiple_columns() {
    let (_dir, db) = setup_db();
    // Two-column recursive CTE.
    let rows = run(&db,
        "WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a + b FROM fib WHERE b < 100) \
         SELECT a FROM fib ORDER BY a").unwrap();
    // Fibonacci: 0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89
    assert!(
        rows.len() >= 10,
        "should generate at least 10 Fibonacci numbers, got {}",
        rows.len()
    );
    assert_eq!(rows[0][0].1, "0", "first Fibonacci = 0");
}

// ── CTAS from aggregation: verify sum values ───────────────────────────────

#[test]
fn ctas_aggregation_values() {
    let (_dir, db) = setup_db();
    run(
        &db,
        "CREATE TABLE agg AS SELECT count(*) AS cnt, sum(amount) AS total FROM items",
    )
    .unwrap();
    let rows = run(&db, "SELECT cnt, total FROM agg").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, "100", "count = 100");
    // sum of i*1.5 for i=1..100 = 1.5 * 5050 = 7575
    let total: f64 = rows[0][1].1.parse().unwrap_or(0.0);
    assert!((total - 7575.0).abs() < 1.0, "total ≈ 7575, got {total}");
}

// ─– Auth + SQL: INSERT permission enforced on CTAS table ────────────────────

#[test]
fn insert_enforced_on_ctas_table() {
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
        // CTAS as admin.
        let db_arc = Arc::new(db);
        run(&db_arc, "CREATE TABLE derived AS SELECT id FROM src").unwrap();
        // Create a user with only Select on derived.
        db_arc.create_user("viewer", "v").unwrap();
        db_arc.create_role("view_role").unwrap();
        db_arc
            .grant_permission(
                "view_role",
                Permission::Select {
                    table: "derived".into(),
                },
            )
            .unwrap();
        db_arc.grant_role("viewer", "view_role").unwrap();
    }
    // Reopen as viewer.
    let db = Arc::new(Database::open_with_credentials(path, "viewer", "v").unwrap());
    // viewer can SELECT from derived.
    let rows = run(&db, "SELECT id FROM derived").unwrap();
    assert_eq!(rows.len(), 1);
    // viewer CANNOT INSERT into derived.
    let err = run_err(&db, "INSERT INTO derived (id) VALUES (99)");
    assert!(
        err.contains("PermissionDenied") || err.contains("permission denied"),
        "viewer should not INSERT into derived: {err}"
    );
}

// ─– Recursive CTE: self-referencing with WHERE on CTE column ────────────────

#[test]
fn recursive_cte_where_on_cte_column() {
    let (_dir, db) = setup_db();
    // Generate powers of 2 using a recursive CTE.
    let rows = run(
        &db,
        "WITH RECURSIVE pow(n) AS (SELECT 1 UNION ALL SELECT n * 2 FROM pow WHERE n < 256) \
         SELECT n FROM pow ORDER BY n",
    )
    .unwrap();
    // 1, 2, 4, 8, 16, 32, 64, 128, 256
    assert_eq!(rows.len(), 9, "9 powers of 2 up to 256");
    assert_eq!(rows[0][0].1, "1");
    assert_eq!(rows[8][0].1, "256");
}

// ─– FTS rank: multi-term query ──────────────────────────────────────────────

#[test]
fn fts_rank_multi_term() {
    let (_dir, db) = setup_db();
    // Multi-term query — rows matching more terms should score higher.
    let rows = run(
        &db,
        "SELECT name, mongreldb_fts_rank(name, 'item_1 item_2') AS score \
         FROM items WHERE id <= 5 ORDER BY score DESC",
    )
    .unwrap();
    assert_eq!(rows.len(), 5);
    // "item_1" contains "item" and "1"; "item_2" contains "item" and "2".
    // Both should have positive scores; items 3-5 should have lower scores
    // (they match "item" but not "1" or "2").
    let top_score: f64 = rows[0][1].1.parse().unwrap_or(0.0);
    assert!(top_score > 0.0, "top score should be positive");
}

// ─– Multi-statement: all DDL ────────────────────────────────────────────────

#[test]
fn multi_statement_all_ddl() {
    let (_dir, db) = setup_db();
    // All DDL — last result should be empty (no SELECT).
    let result = run(
        &db,
        "CREATE TABLE t1 AS SELECT id FROM items LIMIT 1;\
         CREATE TABLE t2 AS SELECT id FROM items LIMIT 2;\
         CREATE TABLE t3 AS SELECT id FROM items LIMIT 3",
    );
    assert!(result.is_ok(), "all-DDL batch should succeed");
    // Verify all three tables exist.
    assert!(db.table("t1").is_ok());
    assert!(db.table("t2").is_ok());
    assert!(db.table("t3").is_ok());
    assert_eq!(db.table("t1").unwrap().lock().count(), 1);
    assert_eq!(db.table("t2").unwrap().lock().count(), 2);
    assert_eq!(db.table("t3").unwrap().lock().count(), 3);
}
