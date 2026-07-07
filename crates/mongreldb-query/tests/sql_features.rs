//! Tests for advanced SQL features: recursive CTEs, window functions,
//! CREATE TABLE AS SELECT.

use mongreldb_core::{schema::*, Database, Value};
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
                name: "amount".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
            ColumnDef {
                id: 3,
                name: "category".into(),
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
    db.create_table("orders", schema).unwrap();
    let t = db.table("orders").unwrap();
    for i in 1i64..=5 {
        let cat = if i <= 2 {
            b"food".to_vec()
        } else {
            b"toys".to_vec()
        };
        t.lock()
            .put(vec![
                (1, Value::Int64(i)),
                (2, Value::Float64(i as f64 * 10.0)),
                (3, Value::Bytes(cat)),
            ])
            .unwrap();
    }
    t.lock().commit().unwrap();
    (dir, Arc::new(db))
}

fn run(session: &MongrelSession, sql: &str) -> Result<Vec<Vec<(String, String)>>, String> {
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

fn make_session() -> (tempfile::TempDir, MongrelSession) {
    let (dir, db) = setup_db();
    let session = MongrelSession::open(db).unwrap();
    (dir, session)
}

#[test]
fn test_recursive_cte() {
    let (_dir, session) = make_session();
    let result = run(&session,
        "WITH RECURSIVE counter(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 5) SELECT n FROM counter ORDER BY n");
    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 5, "recursive CTE should produce 5 rows");
            assert_eq!(rows[0][0].1, "1");
            assert_eq!(rows[4][0].1, "5");
        }
        Err(e) => panic!("recursive CTE failed: {e}"),
    }
}

#[test]
fn test_window_function() {
    let (_dir, session) = make_session();
    let result = run(&session,
        "SELECT id, category, ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) AS rn FROM orders ORDER BY id");
    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 5);
            assert_eq!(rows[0][2].1, "1", "first food row → rn=1");
            assert_eq!(rows[2][2].1, "1", "first toys row → rn=1");
        }
        Err(e) => panic!("window function failed: {e}"),
    }
}

#[test]
fn test_window_aggregate() {
    let (_dir, session) = make_session();
    let result = run(
        &session,
        "SELECT id, SUM(amount) OVER (PARTITION BY category) AS cat_total FROM orders ORDER BY id",
    );
    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 5);
            assert!(
                rows[0][1].1.starts_with("30"),
                "food total: {}",
                rows[0][1].1
            );
            assert!(
                rows[2][1].1.starts_with("120"),
                "toys total: {}",
                rows[2][1].1
            );
        }
        Err(e) => panic!("window aggregate failed: {e}"),
    }
}

#[test]
fn test_create_table_as_select() {
    let (_dir, session) = make_session();
    let result = run(
        &session,
        "CREATE TABLE food_orders AS SELECT id, amount FROM orders WHERE category = 'food'",
    );
    match result {
        Ok(rows) => assert_eq!(rows.len(), 0, "CTAS returns empty result set"),
        Err(e) => panic!("CTAS failed: {e}"),
    }
    let rows = run(&session, "SELECT id FROM food_orders ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2, "food_orders should have 2 rows");
    assert_eq!(rows[0][0].1, "1");
    assert_eq!(rows[1][0].1, "2");
}

// ── Multi-statement execute ──────────────────────────────────────────────────

#[test]
fn test_multi_statement_execute() {
    let (_dir, session) = make_session();
    // Multiple statements separated by semicolons — executes all, returns the
    // last statement's result.
    let result = run(
        &session,
        "SELECT 1; SELECT 2; SELECT id FROM orders ORDER BY id LIMIT 1",
    );
    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1, "last statement should return 1 row");
            assert_eq!(rows[0][0].1, "1", "id = 1");
        }
        Err(e) => panic!("multi-statement failed: {e}"),
    }
}

#[test]
fn test_multi_statement_ddl_then_dml() {
    let (_dir, session) = make_session();
    // DDL + DML + SELECT in one batch.
    let result = run(
        &session,
        "CREATE TABLE temp_copy AS SELECT * FROM orders;\
         INSERT INTO temp_copy (id, amount, category) VALUES (99, 999.0, 'new');\
         SELECT count(*) AS cnt FROM temp_copy",
    );
    match result {
        Ok(rows) => {
            // Should return the count: original 5 + 1 new = 6.
            assert_eq!(rows.len(), 1, "count should return 1 row");
        }
        Err(e) => panic!("multi-statement DDL+DML failed: {e}"),
    }
}

// ── Materialized views ───────────────────────────────────────────────────

#[test]
fn test_materialized_view() {
    let (_dir, session) = make_session();
    // CREATE MATERIALIZED VIEW — physically materializes the query as a table.
    let result = run(&session,
        "CREATE MATERIALIZED VIEW food_mview AS SELECT id, amount FROM orders WHERE category = 'food'");
    match result {
        Ok(rows) => assert_eq!(rows.len(), 0, "CREATE MATERIALIZED VIEW returns empty"),
        Err(e) => panic!("CREATE MATERIALIZED VIEW failed: {e}"),
    }
    // The materialized view is a real table — query it directly.
    let rows = run(&session, "SELECT id FROM food_mview ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2, "food_mview should have 2 rows");
    assert_eq!(rows[0][0].1, "1");
    assert_eq!(rows[1][0].1, "2");
}

// ── Full-text search ranking ──────────────────────────────────────────────

#[test]
fn test_fts_rank() {
    let (_dir, session) = make_session();
    // The fts_rank UDF computes a relevance score for a text column.
    // Higher score = more relevant. Used in ORDER BY to rank results.
    let result = run(
        &session,
        "SELECT mongreldb_fts_rank(category, 'food') AS score FROM orders ORDER BY score DESC",
    );
    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 5);
            // "food" rows should score higher than "toys" rows.
            let food_score: f64 = rows[0][0].1.parse().unwrap_or(0.0);
            let toys_score: f64 = rows[4][0].1.parse().unwrap_or(0.0);
            assert!(food_score > 0.0, "food rows should have positive score");
            assert!(
                toys_score == 0.0 || food_score > toys_score,
                "food should rank higher than toys"
            );
        }
        Err(e) => panic!("fts_rank failed: {e}"),
    }
}
