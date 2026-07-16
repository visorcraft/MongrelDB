use arrow::array::{Int64Array, UInt64Array};
use mongreldb_core::{Database, Principal};
use mongreldb_query::MongrelSession;
use std::sync::Arc;

fn admin() -> Principal {
    Principal {
        user_id: 0,
        created_epoch: 0,
        username: "admin".into(),
        is_admin: true,
        roles: Vec::new(),
        permissions: Vec::new(),
    }
}

#[tokio::test]
async fn scored_sql_ranks_only_authorized_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let admin = MongrelSession::open_as(Arc::clone(&db), admin()).unwrap();
    for sql in [
        "CREATE TABLE docs (id BIGINT PRIMARY KEY, owner TEXT, sparse TEXT)",
        "CREATE INDEX sparse_idx ON docs USING sparse (sparse)",
        "INSERT INTO docs VALUES (1, 'alice', mongreldb_sparse_vector('[[1,1.0]]')), (2, 'bob', mongreldb_sparse_vector('[[1,10.0]]'))",
        "CREATE USER alice WITH PASSWORD 'pw'",
        "CREATE ROLE tenant",
        "GRANT SELECT (id, owner, sparse) ON docs TO tenant",
        "GRANT tenant TO alice",
        "ALTER TABLE docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY owner_only ON docs FOR SELECT TO PUBLIC USING (owner = CURRENT_USER)",
    ] {
        admin.run(sql).await.unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    let alice =
        MongrelSession::open_as(Arc::clone(&db), db.resolve_principal("alice").unwrap()).unwrap();
    let rows = alice
        .run("SELECT * FROM sparse_search_scored('docs','sparse','[[1,1.0]]',2,'id')")
        .await
        .unwrap();
    assert_eq!(rows.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
    assert_eq!(
        rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        rows[0]
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        1
    );
    admin
        .run("REVOKE SELECT (sparse) ON docs FROM tenant")
        .await
        .unwrap();
    assert!(alice
        .run("SELECT * FROM sparse_search_scored('docs','sparse','[[1,1.0]]',2,'id')")
        .await
        .is_err());
}
