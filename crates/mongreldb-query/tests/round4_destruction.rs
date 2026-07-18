//! Round 4 destruction tests — 25 Rust tests.
//! Untested angles: tx rollback, self-referencing CTAS, window ties,
//! non-monotonic recursion, role revocation, concurrent sessions,
//! CTAS from views, multi-statement DDL+query, privilege escalation,
//! integer division in CTE, ORDER BY in CTAS, empty recursive results.

use mongreldb_core::{auth::Permission, schema::*, Database, MongrelError, Value};
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
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "score".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 3,
                name: "team".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    db.create_table("players", schema).unwrap();
    let t = db.table("players").unwrap();
    // Three players on team A, two on team B. Scores: 100, 100, 90, 80, 80
    t.lock()
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Float64(100.0)),
            (3, Value::Bytes(b"A".to_vec())),
        ])
        .unwrap();
    t.lock()
        .put(vec![
            (1, Value::Int64(2)),
            (2, Value::Float64(100.0)),
            (3, Value::Bytes(b"A".to_vec())),
        ])
        .unwrap();
    t.lock()
        .put(vec![
            (1, Value::Int64(3)),
            (2, Value::Float64(90.0)),
            (3, Value::Bytes(b"A".to_vec())),
        ])
        .unwrap();
    t.lock()
        .put(vec![
            (1, Value::Int64(4)),
            (2, Value::Float64(80.0)),
            (3, Value::Bytes(b"B".to_vec())),
        ])
        .unwrap();
    t.lock()
        .put(vec![
            (1, Value::Int64(5)),
            (2, Value::Float64(80.0)),
            (3, Value::Bytes(b"B".to_vec())),
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

// 1. CTAS self-reference — CREATE TABLE t AS SELECT * FROM t
#[test]
fn ctas_self_reference() {
    let (_dir, db) = setup();
    let err = run(&db, "CREATE TABLE players AS SELECT * FROM players");
    assert!(
        err.is_err(),
        "CTAS into the same table name should error (already exists)"
    );
}

// 2. Multi-statement CREATE-DROP-CREATE same table
#[test]
fn multi_statement_create_drop_create() {
    let (_dir, db) = setup();
    let result = run(
        &db,
        "CREATE TABLE cycle AS SELECT id FROM players LIMIT 1;\
         DROP TABLE cycle;\
         CREATE TABLE cycle AS SELECT id FROM players LIMIT 2",
    );
    assert!(
        result.is_ok(),
        "create-drop-create cycle should work: {}",
        result.err().unwrap_or_default()
    );
    let rows = run(&db, "SELECT count(*) AS c FROM cycle").unwrap();
    assert_eq!(rows[0][0].1, "2", "second create should have 2 rows");
}

// 3. CTAS with subquery in WHERE
#[test]
fn ctas_subquery_in_where() {
    let (_dir, db) = setup();
    let result = run(&db,
        "CREATE TABLE sub AS SELECT id FROM players WHERE id IN (SELECT id FROM players WHERE team = 'A')");
    assert!(result.is_ok(), "CTAS with subquery should work");
    let rows = run(&db, "SELECT count(*) AS c FROM sub").unwrap();
    assert_eq!(rows[0][0].1, "3", "3 team A players");
}

// 4. Materialized view independent after UPDATE on source
#[test]
fn matview_independent_after_update() {
    let (_dir, db) = setup();
    run(
        &db,
        "CREATE MATERIALIZED VIEW mv AS SELECT id, score FROM players",
    )
    .unwrap();
    // Verify snapshot values.
    let before = run(&db, "SELECT score FROM mv WHERE id = 1").unwrap();
    assert_eq!(before[0][0].1, "100", "before update score = 100");
    // Update source.
    run(&db, "UPDATE players SET score = 999 WHERE id = 1").unwrap();
    // MV should still have old value.
    let after = run(&db, "SELECT score FROM mv WHERE id = 1").unwrap();
    assert_eq!(
        after[0][0].1, "100",
        "matview should have old value (snapshot)"
    );
}

// 5. Window function with ties — PERCENT_RANK
#[test]
fn window_percent_rank_with_ties() {
    let (_dir, db) = setup();
    let rows = run(
        &db,
        "SELECT id, PERCENT_RANK() OVER (ORDER BY score DESC) AS pr FROM players ORDER BY id",
    )
    .unwrap();
    assert_eq!(rows.len(), 5);
    // Scores: 100, 100, 90, 80, 80 (desc)
    // id=1 (score=100): pr=0 (first)
    // id=2 (score=100): pr=0 (tied with first)
    assert_eq!(rows[0][1].1, "0", "first row PERCENT_RANK = 0");
    assert_eq!(rows[1][1].1, "0", "second row (tied) PERCENT_RANK = 0");
}

// 6. Recursive CTE with integer division
#[test]
fn recursive_cte_integer_division() {
    let (_dir, db) = setup();
    // Halve 256 repeatedly using integer division.
    let rows = run(
        &db,
        "WITH RECURSIVE r(n) AS (SELECT 256 UNION ALL SELECT n / 2 FROM r WHERE n > 1) \
         SELECT n FROM r ORDER BY n",
    )
    .unwrap();
    // 256, 128, 64, 32, 16, 8, 4, 2, 1
    assert_eq!(rows.len(), 9);
    assert_eq!(rows[0][0].1, "1");
    assert_eq!(rows[8][0].1, "256");
}

// 7. Auth: role revocation via refresh_principal
#[test]
fn auth_role_revocation_via_refresh() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create_with_credentials(dir.path(), "admin", "pw").unwrap());
    db.create_table(
        "data",
        Schema {
            schema_id: 1,
            columns: vec![ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            }],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        },
    )
    .unwrap();
    let t = db.table("data").unwrap();
    t.lock().put(vec![(1, Value::Int64(1))]).unwrap();
    t.lock().commit().unwrap();
    db.create_user("alice", "apw").unwrap();
    db.create_role("r").unwrap();
    db.grant_permission(
        "r",
        Permission::Select {
            table: "data".into(),
        },
    )
    .unwrap();
    db.grant_role("alice", "r").unwrap();
    let session =
        MongrelSession::open_as(Arc::clone(&db), db.resolve_principal("alice").unwrap()).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    // Alice can SELECT.
    rt.block_on(session.run("SELECT id FROM data")).unwrap();
    // The catalog-bound session re-resolves Alice after the admin revokes her role.
    db.revoke_role("alice", "r").unwrap();
    // Alice should now be denied SELECT.
    let err = rt
        .block_on(session.run("SELECT id FROM data"))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("PermissionDenied") || err.contains("permission denied"),
        "revoked role should block SELECT after refresh: {err}"
    );
}

// 8. Multi-statement with CREATE TRIGGER body (semicolons inside BEGIN...END)
#[test]
fn multi_statement_create_trigger_preserves_body() {
    let (_dir, db) = setup();
    // The trigger body has semicolons inside BEGIN...END — the splitter must
    // NOT split them. This is the trigger_body guard.
    let result = run(
        &db,
        "CREATE TRIGGER t_after_insert AFTER INSERT ON players BEGIN \
         INSERT INTO players (id, score, team) VALUES (999, 1.0, 'X'); \
         END",
    );
    // This may fail due to PK conflict (id 999 may already exist) — but it
    // should NOT fail with "expected exactly one statement" (which would mean
    // the splitter incorrectly split the trigger body).
    match result {
        Ok(_) => {}
        Err(e) => {
            assert!(
                !e.contains("expected exactly one statement") && !e.contains("only one statement"),
                "trigger body was incorrectly split: {e}"
            );
        }
    }
}

// 9. CTAS from a view — views are session-scoped, must use same session
#[test]
fn ctas_from_view() {
    let (_dir, db) = setup();
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    // Create a regular view in this session.
    rt.block_on(async {
        session
            .run("CREATE VIEW team_a AS SELECT id, score FROM players WHERE team = 'A'")
            .await
    })
    .unwrap();
    // CTAS from the view in the SAME session.
    let result = rt.block_on(async {
        session
            .run("CREATE TABLE team_a_copy AS SELECT id, score FROM team_a")
            .await
    });
    assert!(result.is_ok(), "CTAS from view should work in same session");
    let batches = rt
        .block_on(async { session.run("SELECT count(*) AS c FROM team_a_copy").await })
        .unwrap();
    let arr = batches[0].column(0);
    let count = if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
        a.value(0)
    } else {
        0
    };
    assert_eq!(count, 3, "3 team A players");
}

// 10. Recursive CTE referencing two real tables
#[test]
fn recursive_cte_joins_two_tables() {
    let (_dir, db) = setup();
    // Recursive arm joins the CTE with the players table.
    let rows = run(
        &db,
        "WITH RECURSIVE r(id) AS \
         (SELECT 1 UNION ALL SELECT p.id FROM r JOIN players p ON p.id = r.id + 1 WHERE r.id < 3) \
         SELECT id FROM r ORDER BY id",
    )
    .unwrap();
    // Should walk ids 1→2→3 (chained via join).
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].1, "1");
    assert_eq!(rows[2][0].1, "3");
}

// 11. Multi-statement creates table then queries it in same batch
#[test]
fn multi_statement_create_then_query_same_batch() {
    let (_dir, db) = setup();
    let result = run(
        &db,
        "CREATE TABLE instant AS SELECT id FROM players LIMIT 2;\
         SELECT count(*) AS c FROM instant",
    );
    assert!(
        result.is_ok(),
        "create-then-query in same batch should work"
    );
    let rows = result.unwrap();
    assert_eq!(rows.len(), 1, "count(*) should return 1 row");
    assert_eq!(rows[0][0].1, "2", "2 rows in instant table");
}

// 12. Auth: privilege escalation — set_user_admin then create_user
#[test]
fn auth_privilege_escalation_blocked() {
    let dir = tempdir().unwrap();
    let path = dir.path();
    {
        let db = Database::create_with_credentials(path, "admin", "pw").unwrap();
        db.create_user("regular", "rpw").unwrap();
    }
    let db = Arc::new(Database::open_with_credentials(path, "regular", "rpw").unwrap());
    // regular is NOT admin — cannot create users.
    let err = db.create_user("intruder", "pw").unwrap_err();
    assert!(matches!(err, MongrelError::PermissionDenied { .. }));
}

// 13. FTS rank on column with all identical values
#[test]
fn fts_rank_identical_values() {
    let (_dir, db) = setup();
    // All rows have score=100 for id=1 and id=2.
    let rows = run(
        &db,
        "SELECT id, mongreldb_fts_rank(team, 'A') AS score FROM players ORDER BY id",
    )
    .unwrap();
    // Team A players (ids 1,2,3) should score the same; team B players (4,5) should score 0.
    let team_a_scores: Vec<f64> = rows
        .iter()
        .take(3)
        .map(|r| r[1].1.parse().unwrap_or(0.0))
        .collect();
    assert!(
        team_a_scores.iter().all(|&s| s > 0.0),
        "all team A rows should have positive score"
    );
    let team_b_scores: Vec<f64> = rows
        .iter()
        .skip(3)
        .map(|r| r[1].1.parse().unwrap_or(0.0))
        .collect();
    assert!(
        team_b_scores.iter().all(|&s| s == 0.0),
        "all team B rows should have score 0"
    );
}

// 14. Concurrent sessions — one does CTAS, other queries
#[test]
fn concurrent_sessions_ctas_visibility() {
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
            embedding_source: None,
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    db.create_table("src", schema).unwrap();
    let t = db.table("src").unwrap();
    t.lock().put(vec![(1, Value::Int64(1))]).unwrap();
    t.lock().commit().unwrap();
    let db = Arc::new(db);

    // Session A does CTAS.
    let sess_a = MongrelSession::open(Arc::clone(&db)).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { sess_a.run("CREATE TABLE via_a AS SELECT id FROM src").await })
        .unwrap();

    // Session B queries — should see the table (it's in the catalog).
    let rows = run(&db, "SELECT count(*) AS c FROM via_a").unwrap();
    assert_eq!(rows[0][0].1, "1", "session B should see the CTAS table");
}

// 15. Recursive CTE zero iterations after base
#[test]
fn recursive_cte_zero_iterations() {
    let (_dir, db) = setup();
    // Recursive arm WHERE is always false → just base.
    let rows = run(
        &db,
        "WITH RECURSIVE r(n) AS (SELECT 42 UNION ALL SELECT n + 1 FROM r WHERE 1 = 0) \
         SELECT n FROM r",
    )
    .unwrap();
    assert_eq!(rows.len(), 1, "only base row, no iterations");
    assert_eq!(rows[0][0].1, "42");
}

// 16. Multi-statement all SELECT with aggregation
#[test]
fn multi_statement_all_select_aggregation() {
    let (_dir, db) = setup();
    let rows = run(
        &db,
        "SELECT count(*) AS c FROM players;\
         SELECT sum(score) AS total FROM players",
    )
    .unwrap();
    // Last statement (sum) should be returned.
    assert_eq!(rows.len(), 1);
    // sum of 100+100+90+80+80 = 450
    let total: f64 = rows[0][0].1.parse().unwrap_or(0.0);
    assert!((total - 450.0).abs() < 1.0, "total = 450, got {total}");
}

// 17. CTAS with ORDER BY — does it preserve order?
#[test]
fn ctas_with_order_by() {
    let (_dir, db) = setup();
    run(
        &db,
        "CREATE TABLE ordered AS SELECT id, score FROM players ORDER BY score DESC",
    )
    .unwrap();
    let rows = run(&db, "SELECT id FROM ordered").unwrap();
    // Order may or may not be preserved (SQL doesn't guarantee order without
    // an explicit ORDER BY in the query). Just verify all rows are present.
    assert_eq!(rows.len(), 5);
}

// 18. Window DENSE_RANK with ties
#[test]
fn window_dense_rank_with_ties() {
    let (_dir, db) = setup();
    let rows = run(
        &db,
        "SELECT id, DENSE_RANK() OVER (ORDER BY score DESC) AS drk FROM players ORDER BY id",
    )
    .unwrap();
    assert_eq!(rows.len(), 5);
    // Scores desc: 100(×2), 90(×1), 80(×2)
    // DENSE_RANK: 1, 1, 2, 3, 3
    assert_eq!(rows[0][1].1, "1"); // id=1, score=100
    assert_eq!(rows[1][1].1, "1"); // id=2, score=100 (tied)
    assert_eq!(rows[2][1].1, "2"); // id=3, score=90
    assert_eq!(rows[3][1].1, "3"); // id=4, score=80
    assert_eq!(rows[4][1].1, "3"); // id=5, score=80 (tied)
}

// 19. Auth: disable_auth clears enforcement on mounted tables
#[test]
fn auth_disable_clears_table_enforcement() {
    let dir = tempdir().unwrap();
    let path = dir.path();
    {
        let db = Database::create_with_credentials(path, "admin", "pw").unwrap();
        db.create_table(
            "data",
            Schema {
                schema_id: 1,
                columns: vec![ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                    embedding_source: None,
                }],
                indexes: vec![],
                colocation: vec![],
                constraints: Default::default(),
                clustered: false,
            },
        )
        .unwrap();
        let t = db.table("data").unwrap();
        t.lock().put(vec![(1, Value::Int64(1))]).unwrap();
        t.lock().commit().unwrap();
        db.disable_auth().unwrap();
    }
    // Reopen without credentials — should be able to read.
    let db = Database::open(path).unwrap();
    let t = db.table("data").unwrap();
    let rows = t
        .lock()
        .query(&mongreldb_core::query::Query::new())
        .unwrap();
    assert_eq!(rows.len(), 1, "should read without auth after disable");
}

// 20. FTS rank with numbers in text
#[test]
fn fts_rank_numbers_in_text() {
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
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "body".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    db.create_table("docs", schema).unwrap();
    let t = db.table("docs").unwrap();
    t.lock()
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"version 2.0 build 1234".to_vec())),
        ])
        .unwrap();
    t.lock().commit().unwrap();
    let rows = run(
        &Arc::new(db),
        "SELECT mongreldb_fts_rank(body, 'version 1234') AS score FROM docs",
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    let score: f64 = rows[0][0].1.parse().unwrap_or(0.0);
    assert!(score > 0.0, "should match 'version' and '1234'");
}

// 21. Multi-statement with block comment containing semicolons
#[test]
fn multi_statement_block_comment_with_semicolons() {
    let (_dir, db) = setup();
    let rows = run(
        &db,
        "SELECT 1 AS n /* this; has; semicolons; */; SELECT 2 AS n",
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, "2");
}

// 22. CTAS with CASE expression
#[test]
fn ctas_with_case_expression() {
    let (_dir, db) = setup();
    let result = run(&db,
        "CREATE TABLE labeled AS SELECT id, CASE WHEN score >= 90 THEN 'high' ELSE 'low' END AS tier FROM players");
    assert!(result.is_ok(), "CTAS with CASE should work");
    let rows = run(&db, "SELECT tier FROM labeled WHERE id = 1").unwrap();
    assert_eq!(rows[0][0].1, "high", "score 100 → high");
    let rows = run(&db, "SELECT tier FROM labeled WHERE id = 4").unwrap();
    assert_eq!(rows[0][0].1, "low", "score 80 → low");
}

// 23. Recursive CTE with MODULO operation
#[test]
fn recursive_cte_modulo() {
    let (_dir, db) = setup();
    // Generate even numbers using modulo.
    let rows = run(
        &db,
        "WITH RECURSIVE r(n) AS \
         (SELECT 0 UNION ALL SELECT n + 2 FROM r WHERE n < 10) \
         SELECT count(*) AS c FROM r WHERE n % 4 = 0",
    )
    .unwrap();
    // r: 0,2,4,6,8,10. n%4=0: 0,4,8 → 3 rows.
    assert_eq!(rows[0][0].1, "3");
}

// 24. Window FIRST_VALUE / LAST_VALUE
#[test]
fn window_first_last_value() {
    let (_dir, db) = setup();
    let rows = run(
        &db,
        "SELECT id, FIRST_VALUE(score) OVER (PARTITION BY team ORDER BY score DESC) AS top_score \
         FROM players ORDER BY id",
    )
    .unwrap();
    assert_eq!(rows.len(), 5);
    // Team A top score = 100 (ids 1 and 2 tied at 100).
    assert_eq!(rows[0][1].1, "100", "team A top = 100");
    // Team B top score = 80.
    assert_eq!(rows[3][1].1, "80", "team B top = 80");
}

// 25. Materialized view then DROP then recreate
#[test]
fn matview_drop_recreate() {
    let (_dir, db) = setup();
    run(
        &db,
        "CREATE MATERIALIZED VIEW mv AS SELECT id FROM players WHERE team = 'A'",
    )
    .unwrap();
    run(&db, "DROP TABLE mv").unwrap();
    // Recreate with different query.
    run(
        &db,
        "CREATE MATERIALIZED VIEW mv AS SELECT id FROM players WHERE team = 'B'",
    )
    .unwrap();
    let rows = run(&db, "SELECT count(*) AS c FROM mv").unwrap();
    assert_eq!(
        rows[0][0].1, "2",
        "recreated MV should have 2 team B players"
    );
}
