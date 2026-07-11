use mongreldb_core::Database;
use mongreldb_query::MongrelSession;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn create_table_lowers_column_and_table_checks() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run(
            "CREATE TABLE inventory (\
                id BIGINT PRIMARY KEY, \
                price BIGINT CHECK (price >= 0), \
                quantity BIGINT, \
                label VARCHAR CHECK (label IS NULL OR label <> ''), \
                CONSTRAINT total_limit CHECK (quantity IS NULL OR price * quantity <= 1000)\
            )",
        )
        .await
        .unwrap();

    session
        .run(
            "INSERT INTO inventory (id, price, quantity, label) VALUES \
             (1, 10, 20, 'ok'), (2, 5, NULL, NULL)",
        )
        .await
        .unwrap();

    assert!(session
        .run("INSERT INTO inventory VALUES (3, -1, 1, 'bad')")
        .await
        .is_err());
    assert!(session
        .run("INSERT INTO inventory VALUES (4, 100, 20, 'bad')")
        .await
        .is_err());
    assert!(session
        .run("INSERT INTO inventory VALUES (5, 1, 1, '')")
        .await
        .is_err());

    let schema = db.table("inventory").unwrap().lock().schema().clone();
    assert_eq!(schema.constraints.checks.len(), 3);
    assert!(schema
        .constraints
        .checks
        .iter()
        .any(|constraint| constraint.name == "total_limit"));
}

#[tokio::test]
async fn regex_checks_support_match_modes_and_reject_bad_patterns_at_ddl() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run(
            "CREATE TABLE handles (\
                id BIGINT PRIMARY KEY, \
                name VARCHAR, \
                CHECK (name ~* '^[a-z]+$'), \
                CHECK (name !~ 'blocked')\
            )",
        )
        .await
        .unwrap();
    session
        .run("INSERT INTO handles VALUES (1, 'Alice')")
        .await
        .unwrap();
    assert!(session
        .run("INSERT INTO handles VALUES (2, 'blocked')")
        .await
        .is_err());
    assert!(session
        .run("INSERT INTO handles VALUES (3, 'alice7')")
        .await
        .is_err());

    let error = session
        .run("CREATE TABLE bad_regex (id BIGINT PRIMARY KEY, value VARCHAR CHECK (value ~ '['))")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("invalid regex pattern"));
    assert!(!db.table_names().iter().any(|name| name == "bad_regex"));
}

#[tokio::test]
async fn alter_add_check_validates_rows_and_survives_reopen() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE scores (id BIGINT PRIMARY KEY, score BIGINT)")
        .await
        .unwrap();
    session
        .run("INSERT INTO scores VALUES (1, -1)")
        .await
        .unwrap();
    assert!(session
        .run("ALTER TABLE scores ADD CONSTRAINT nonnegative CHECK (score >= 0)")
        .await
        .is_err());
    assert!(db.table("scores").is_ok());

    session
        .run("DELETE FROM scores WHERE id = 1")
        .await
        .unwrap();
    session
        .run("INSERT INTO scores VALUES (2, 2)")
        .await
        .unwrap();
    session
        .run("ALTER TABLE scores ADD CONSTRAINT nonnegative CHECK (score >= 0)")
        .await
        .unwrap();
    assert!(session
        .run("INSERT INTO scores VALUES (3, -3)")
        .await
        .is_err());

    drop(session);
    drop(db);

    let reopened = Arc::new(Database::open(dir.path()).unwrap());
    let reopened_session = MongrelSession::open(Arc::clone(&reopened)).unwrap();
    assert_eq!(
        reopened
            .table("scores")
            .unwrap()
            .lock()
            .schema()
            .constraints
            .checks
            .len(),
        1
    );
    assert!(reopened_session
        .run("INSERT INTO scores VALUES (4, -4)")
        .await
        .is_err());
}
