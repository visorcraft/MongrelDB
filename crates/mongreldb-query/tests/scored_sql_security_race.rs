use mongreldb_core::{Database, Principal};
use mongreldb_query::MongrelSession;
use std::sync::Arc;

fn admin() -> Principal {
    Principal {
        username: "admin".into(),
        is_admin: true,
        roles: Vec::new(),
        permissions: Vec::new(),
    }
}

#[tokio::test]
async fn scored_sql_rechecks_security_version_after_policy_change() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let admin_session = MongrelSession::open_as(Arc::clone(&db), admin()).unwrap();
    for sql in [
        "CREATE TABLE docs (id BIGINT PRIMARY KEY, owner TEXT, sparse TEXT)",
        "CREATE INDEX sparse_idx ON docs USING sparse (sparse)",
        "INSERT INTO docs VALUES (1, 'alice', mongreldb_sparse_vector('[[1,1.0]]'))",
        "CREATE USER alice WITH PASSWORD 'pw'",
        "CREATE ROLE tenant",
        "GRANT SELECT (id, owner, sparse) ON docs TO tenant",
        "GRANT tenant TO alice",
        "ALTER TABLE docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY owner_only ON docs FOR SELECT TO PUBLIC USING (owner = CURRENT_USER)",
    ] {
        admin_session.run(sql).await.unwrap();
    }
    let alice = db.resolve_principal("alice").unwrap();
    let stale = db.authorized_read_snapshot("docs", Some(&alice)).unwrap();
    assert!(!stale.allowed_row_ids.as_ref().unwrap().is_empty());

    admin_session
        .run("DROP POLICY owner_only ON docs")
        .await
        .unwrap();
    admin_session
        .run("CREATE POLICY owner_only ON docs FOR SELECT TO PUBLIC USING (owner = 'bob')")
        .await
        .unwrap();
    assert!(!db.authorized_read_snapshot_valid(&stale));

    let alice_session = MongrelSession::open_as(Arc::clone(&db), alice).unwrap();
    let rows = alice_session
        .run("SELECT * FROM sparse_search_scored('docs','sparse','[[1,1.0]]',2,'id')")
        .await
        .unwrap();
    assert_eq!(rows.iter().map(|batch| batch.num_rows()).sum::<usize>(), 0);
}
