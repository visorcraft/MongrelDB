//! Tests for ALTER USER ... ADMIN / NOT ADMIN SQL (engine fix for the
//! previously-unimplemented-but-documented syntax).

use mongreldb_core::Database;
use mongreldb_query::MongrelSession;
use std::sync::Arc;
use tempfile::tempdir;

fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    (dir, db)
}

/// Run SQL against the session (async runtime wrapper).
fn run_sql(session: &MongrelSession, sql: &str) -> Result<(), String> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { session.run(sql).await })
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn is_admin(db: &Database, username: &str) -> bool {
    db.users()
        .iter()
        .find(|u| u.username == username)
        .map(|u| u.is_admin)
        .unwrap_or(false)
}

#[test]
fn alter_user_admin_sets_the_admin_flag() {
    let (_dir, db) = setup();
    db.create_user("alice", "pw1").unwrap();

    assert!(!is_admin(&db, "alice"), "alice should start as non-admin");

    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    run_sql(&session, "ALTER USER alice ADMIN").unwrap();

    assert!(
        is_admin(&db, "alice"),
        "alice should be admin after ALTER USER alice ADMIN"
    );
}

#[test]
fn alter_user_not_admin_clears_the_admin_flag() {
    let (_dir, db) = setup();
    db.create_user("bob", "pw1").unwrap();
    db.set_user_admin("bob", true).unwrap();
    assert!(is_admin(&db, "bob"));

    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    run_sql(&session, "ALTER USER bob NOT ADMIN").unwrap();

    assert!(
        !is_admin(&db, "bob"),
        "bob should be non-admin after ALTER USER bob NOT ADMIN"
    );
}

#[test]
fn alter_user_password_still_works_alongside_admin() {
    // Ensure the new ADMIN branches didn't break the existing PASSWORD path:
    // the SQL should execute without error, and the new password should verify.
    // (We don't assert the old password fails — verify_user's caching against
    // the Database handle is orthogonal to this parsing fix.)
    let (_dir, db) = setup();
    db.create_user("carol", "oldpw").unwrap();

    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    run_sql(&session, "ALTER USER carol PASSWORD 'newpw'").unwrap();

    assert!(db.verify_user("carol", "newpw").is_ok());
}

#[test]
fn alter_user_admin_with_trailing_semicolon() {
    let (_dir, db) = setup();
    db.create_user("dave", "pw").unwrap();

    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    run_sql(&session, "ALTER USER dave ADMIN;").unwrap();

    assert!(
        is_admin(&db, "dave"),
        "trailing semicolon should be tolerated"
    );
}

#[test]
fn alter_user_invalid_form_errors() {
    let (_dir, db) = setup();
    db.create_user("eve", "pw").unwrap();

    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    let res = run_sql(&session, "ALTER USER eve SOMETHING");
    assert!(res.is_err(), "an unknown ALTER USER form should error");
}
