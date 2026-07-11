use arrow::array::{Int64Array, StringArray};
use futures::StreamExt;
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
async fn sql_rls_column_grants_masks_and_fast_paths() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let admin = MongrelSession::open_as(Arc::clone(&db), admin()).unwrap();
    admin
        .run("CREATE TABLE docs (id BIGINT PRIMARY KEY, owner TEXT, secret TEXT, value BIGINT)")
        .await
        .unwrap();
    admin
        .run(
            "INSERT INTO docs VALUES \
             (1, 'alice', 'alice-secret', 10), \
             (2, 'bob', 'bob-secret', 20)",
        )
        .await
        .unwrap();
    for sql in [
        "CREATE USER alice WITH PASSWORD 'pw'",
        "CREATE USER bob WITH PASSWORD 'pw'",
        "CREATE ROLE tenant",
        "GRANT SELECT (id, owner, secret) ON docs TO tenant",
        "GRANT INSERT (id, owner, secret, value) ON docs TO tenant",
        "GRANT UPDATE (value) ON docs TO tenant",
        "GRANT DELETE ON docs TO tenant",
        "GRANT tenant TO alice",
        "GRANT tenant TO bob",
        "ALTER TABLE docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY owner_only ON docs FOR ALL TO PUBLIC USING (owner = CURRENT_USER) WITH CHECK (owner = CURRENT_USER)",
        "CREATE MASK hide_secret ON docs(secret) USING REDACT '***'",
    ] {
        admin.run(sql).await.unwrap_or_else(|error| panic!("{sql}: {error}"));
    }

    let alice =
        MongrelSession::open_as(Arc::clone(&db), db.resolve_principal("alice").unwrap()).unwrap();
    let rows = alice
        .run("SELECT id, owner, secret FROM docs WHERE id > 0 ORDER BY id")
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
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "***"
    );

    let count = alice.run("SELECT COUNT(*) AS n FROM docs").await.unwrap();
    assert_eq!(
        count[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    let epoch = db.visible_epoch().0;
    let historical = alice
        .run(&format!(
            "SELECT id FROM docs AS OF EPOCH {epoch} WHERE id > 0"
        ))
        .await
        .unwrap();
    assert_eq!(historical[0].num_rows(), 1);

    let mut stream = alice
        .run_stream("SELECT id FROM docs ORDER BY id")
        .await
        .unwrap();
    let streamed = stream.next().await.unwrap().unwrap();
    assert_eq!(streamed.num_rows(), 1);
    assert!(stream.next().await.is_none());

    alice
        .run("INSERT INTO docs VALUES (3, 'alice', 'new', 30)")
        .await
        .unwrap();
    assert!(alice
        .run("INSERT INTO docs VALUES (4, 'bob', 'stolen', 40)")
        .await
        .unwrap_err()
        .to_string()
        .contains("permission denied"));
    assert!(alice
        .run("UPDATE docs SET value = 99 WHERE id = 2")
        .await
        .unwrap_err()
        .to_string()
        .contains("permission denied"));
    assert!(alice
        .run("DELETE FROM docs WHERE id = 2")
        .await
        .unwrap_err()
        .to_string()
        .contains("permission denied"));
    assert!(alice
        .run("SELECT value FROM docs")
        .await
        .unwrap_err()
        .to_string()
        .contains("value"));
    assert!(alice
        .run("ALTER TABLE docs DISABLE ROW LEVEL SECURITY")
        .await
        .unwrap_err()
        .to_string()
        .contains("permission denied"));

    admin
        .run("REVOKE SELECT (secret) ON docs FROM tenant")
        .await
        .unwrap();
    assert!(alice
        .run("SELECT id, owner, secret FROM docs WHERE id > 0 ORDER BY id")
        .await
        .unwrap_err()
        .to_string()
        .contains("permission denied"));
    admin
        .run("GRANT SELECT (value) ON docs TO tenant")
        .await
        .unwrap();
    let values = alice
        .run("SELECT value FROM docs WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(
        values[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        10
    );
    admin
        .run("ALTER TABLE docs ADD COLUMN note BIGINT")
        .await
        .unwrap();
    assert_eq!(db.security_catalog().policies.len(), 1);
    assert_eq!(db.security_catalog().masks.len(), 1);
    assert!(db
        .resolve_principal("alice")
        .unwrap()
        .permissions
        .iter()
        .any(|permission| matches!(
            permission,
            mongreldb_core::Permission::SelectColumns { columns, .. }
                if columns.contains(&"value".to_string())
        )));
    assert!(admin
        .run("ALTER TABLE docs DROP COLUMN owner")
        .await
        .unwrap_err()
        .to_string()
        .contains("row policy depends"));

    let bob =
        MongrelSession::open_as(Arc::clone(&db), db.resolve_principal("bob").unwrap()).unwrap();
    let rows = bob.run("SELECT id FROM docs ORDER BY id").await.unwrap();
    assert_eq!(rows[0].num_rows(), 1);
    assert_eq!(
        rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );

    admin.run("DROP USER alice").await.unwrap();
    assert!(alice
        .run("SELECT id FROM docs")
        .await
        .unwrap_err()
        .to_string()
        .contains("authentication required"));
}
