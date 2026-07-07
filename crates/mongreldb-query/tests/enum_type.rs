//! Integration tests for the engine's native ENUM type via SQL.
//! Exercises CREATE TABLE with ENUM, INSERT valid/invalid variants, and round-trip.

use mongreldb_core::Database;
use mongreldb_query::MongrelSession;
use std::sync::Arc;
use tempfile::tempdir;

async fn session() -> (tempfile::TempDir, MongrelSession) {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let session = MongrelSession::open(Arc::new(db)).unwrap();
    (dir, session)
}

#[tokio::test]
async fn enum_create_and_insert_valid() {
    let (_dir, s) = session().await;
    s.run(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, status ENUM('draft','published','archived'))",
    )
    .await
    .unwrap();

    let result = s
        .run("INSERT INTO items (id, status) VALUES (1, 'draft')")
        .await;
    assert!(
        result.is_ok(),
        "valid enum variant should insert: {:?}",
        result
    );
}

#[tokio::test]
async fn enum_insert_invalid_variant_rejected() {
    let (_dir, s) = session().await;
    s.run("CREATE TABLE items (id BIGINT PRIMARY KEY, status ENUM('a','b'))")
        .await
        .unwrap();

    let result = s
        .run("INSERT INTO items (id, status) VALUES (1, 'zzz')")
        .await;
    assert!(result.is_err(), "invalid enum variant should be rejected");
}

#[tokio::test]
async fn enum_select_roundtrip() {
    let (_dir, s) = session().await;
    s.run("CREATE TABLE items (id BIGINT PRIMARY KEY, status ENUM('new','old'))")
        .await
        .unwrap();
    s.run("INSERT INTO items (id, status) VALUES (1, 'new')")
        .await
        .unwrap();
    s.run("INSERT INTO items (id, status) VALUES (2, 'old')")
        .await
        .unwrap();

    let batches = s
        .run("SELECT id, status FROM items ORDER BY id")
        .await
        .unwrap();
    assert!(!batches.is_empty());
}

#[tokio::test]
async fn enum_null_on_nullable_column() {
    let (_dir, s) = session().await;
    // NOT NULL enum column — must provide a value.
    s.run("CREATE TABLE items (id BIGINT PRIMARY KEY, status ENUM('x','y') NOT NULL)")
        .await
        .unwrap();
    let result = s.run("INSERT INTO items (id) VALUES (1)").await;
    assert!(result.is_err(), "NOT NULL enum without value should fail");

    // Now with a nullable enum column.
    s.run("CREATE TABLE items2 (id BIGINT PRIMARY KEY, status ENUM('x','y') NULL)")
        .await
        .unwrap();
    let result = s.run("INSERT INTO items2 (id) VALUES (1)").await;
    assert!(result.is_ok(), "nullable enum without value should succeed");
}
