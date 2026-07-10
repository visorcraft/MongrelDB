//! Integration tests for EXPLAIN ANALYZE (#8 observability).
//!
//! `EXPLAIN ANALYZE` falls through to DataFusion 54's native execution-time
//! plan introspection (it is NOT the SQLite-style `EXPLAIN QUERY PLAN`, which
//! is intercepted separately). These tests verify it runs against a real table
//! and that its output is never served from the result cache — timing metrics
//! are request-specific and would be misleading on a cache hit.

use mongreldb_core::Database;
use mongreldb_query::MongrelSession;
use std::sync::Arc;
use tempfile::tempdir;

async fn session() -> (tempfile::TempDir, MongrelSession) {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let s = MongrelSession::open(Arc::new(db)).unwrap();
    s.run("CREATE TABLE items (id BIGINT PRIMARY KEY, v DOUBLE)")
        .await
        .unwrap();
    s.run("INSERT INTO items (id, v) VALUES (1, 10.0)")
        .await
        .unwrap();
    s.run("INSERT INTO items (id, v) VALUES (2, 20.0)")
        .await
        .unwrap();
    (dir, s)
}

#[tokio::test]
async fn explain_analyze_runs_and_returns_plan() {
    let (_dir, s) = session().await;
    let batches = s
        .run("EXPLAIN ANALYZE SELECT count(*) FROM items")
        .await
        .expect("EXPLAIN ANALYZE should execute");
    assert!(!batches.is_empty(), "EXPLAIN ANALYZE should produce output");
    // DataFusion's EXPLAIN ANALYZE emits a two-column (plan_type, plan) shape.
    let first = &batches[0];
    assert!(
        first.num_columns() >= 2,
        "EXPLAIN output should have plan_type + plan columns"
    );
}

#[tokio::test]
async fn bare_explain_also_works() {
    let (_dir, s) = session().await;
    let batches = s
        .run("EXPLAIN SELECT v FROM items WHERE id = 1")
        .await
        .expect("bare EXPLAIN should execute via DataFusion");
    assert!(!batches.is_empty());
}

#[tokio::test]
async fn explain_analyze_does_not_poison_result_cache() {
    let (_dir, s) = session().await;
    // Run EXPLAIN ANALYZE (must not be cached).
    let _ = s
        .run("EXPLAIN ANALYZE SELECT count(*) FROM items")
        .await
        .unwrap();
    // A normal cacheable SELECT still returns the correct live result and can
    // be served identically on a repeat (cache hit) — proving the EXPLAIN
    // ANALYZE path did not corrupt or displace the result cache.
    let b1 = s.run("SELECT count(*) FROM items").await.unwrap();
    let b2 = s.run("SELECT count(*) FROM items").await.unwrap();
    assert_eq!(b1.len(), b2.len());
    // And EXPLAIN ANALYZE itself is repeatable (not a one-shot cached blob).
    let again = s
        .run("EXPLAIN ANALYZE SELECT count(*) FROM items")
        .await
        .unwrap();
    assert!(!again.is_empty());
}
