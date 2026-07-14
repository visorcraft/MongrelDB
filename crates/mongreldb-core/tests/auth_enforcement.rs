//! Phase 1 — optional credential enforcement at the storage layer.
//!
//! These tests cover the engine-level matrix from
//! `docs/15-credential-enforcement.md` §4.3: create-with-credentials bootstrap,
//! credentialed open, the DDL/admin/maintenance enforcement points, and
//! fail-closed semantics. Table/Transaction/SQL enforcement lands in Phase 2.

use mongreldb_core::auth::Permission;
use mongreldb_core::{
    query::{AnnRerankRequest, Condition, Query, VectorMetric},
    schema::*,
    ColumnMask, ColumnOperation, Database, MaskStrategy, MongrelError, PolicyCommand,
    ReadAuthorization, RowPolicy, SecurityCatalog, SecurityExpr, Value,
};
use tempfile::tempdir;

fn int_pk_schema() -> Schema {
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
    }
}

#[test]
fn secure_native_wrappers_apply_rls_masks_and_live_revocation() {
    let dir = tempdir().unwrap();
    let path = dir.path();
    let admin = Database::create_with_credentials(path, "admin", "admin-pw").unwrap();
    admin
        .create_table(
            "docs",
            Schema {
                schema_id: 2,
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
                        name: "owner".into(),
                        ty: TypeId::Bytes,
                        flags: ColumnFlags::empty(),
                        default_value: None,
                    },
                    ColumnDef {
                        id: 3,
                        name: "secret".into(),
                        ty: TypeId::Bytes,
                        flags: ColumnFlags::empty(),
                        default_value: None,
                    },
                    ColumnDef {
                        id: 4,
                        name: "embedding".into(),
                        ty: TypeId::Embedding { dim: 2 },
                        flags: ColumnFlags::empty(),
                        default_value: None,
                    },
                ],
                indexes: vec![IndexDef {
                    name: "ann".into(),
                    column_id: 4,
                    kind: IndexKind::Ann,
                    predicate: None,
                    options: Default::default(),
                }],
                ..Schema::default()
            },
        )
        .unwrap();
    let mut transaction = admin.begin();
    transaction
        .put(
            "docs",
            vec![
                (1, Value::Int64(1)),
                (2, Value::Bytes(b"alice".to_vec())),
                (3, Value::Bytes(b"alice-secret".to_vec())),
                (4, Value::Embedding(vec![0.9, -0.1])),
            ],
        )
        .unwrap();
    for id in 3..=10 {
        transaction
            .put(
                "docs",
                vec![
                    (1, Value::Int64(id)),
                    (2, Value::Bytes(b"bob".to_vec())),
                    (3, Value::Bytes(b"hidden".to_vec())),
                    (4, Value::Embedding(vec![-1.0, -1.0])),
                ],
            )
            .unwrap();
    }
    transaction
        .put(
            "docs",
            vec![
                (1, Value::Int64(2)),
                (2, Value::Bytes(b"bob".to_vec())),
                (3, Value::Bytes(b"bob-secret".to_vec())),
                (4, Value::Embedding(vec![1.0, 0.0])),
            ],
        )
        .unwrap();
    transaction.commit().unwrap();
    for user in ["alice", "bob"] {
        admin.create_user(user, &format!("{user}-pw")).unwrap();
    }
    admin.create_role("reader").unwrap();
    admin
        .grant_permission(
            "reader",
            Permission::Select {
                table: "docs".into(),
            },
        )
        .unwrap();
    admin.grant_role("alice", "reader").unwrap();
    admin.grant_role("bob", "reader").unwrap();
    admin
        .set_security_catalog(SecurityCatalog {
            rls_tables: vec!["docs".into()],
            policies: vec![RowPolicy {
                name: "owner_only".into(),
                table: "docs".into(),
                command: PolicyCommand::Select,
                subjects: vec!["public".into()],
                permissive: true,
                using: Some(SecurityExpr::ColumnEqCurrentUser { column: 2 }),
                with_check: None,
            }],
            masks: vec![ColumnMask {
                name: "redact_secret".into(),
                table: "docs".into(),
                column: 3,
                strategy: MaskStrategy::Redact {
                    replacement: "***".into(),
                },
                exempt_subjects: vec!["admin".into()],
            }],
        })
        .unwrap();

    let alice = Database::open_with_credentials(path, "alice", "alice-pw").unwrap();
    let rows = alice
        .query_for_current_principal("docs", &Query::new(), None)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(1)));
    assert_eq!(
        rows[0].columns.get(&3),
        Some(&Value::Bytes(b"***".to_vec()))
    );
    let ann_rows = alice
        .query_for_current_principal(
            "docs",
            &Query::new().and(Condition::Ann {
                column_id: 4,
                query: vec![1.0, 0.0],
                k: 10,
            }),
            None,
        )
        .unwrap();
    assert_eq!(ann_rows.len(), 1);
    assert_eq!(ann_rows[0].columns.get(&1), Some(&Value::Int64(1)));
    let (reranked, trace) = mongreldb_core::trace::QueryTrace::capture(|| {
        alice.ann_rerank_for_current_principal(
            "docs",
            &AnnRerankRequest {
                column_id: 4,
                query: vec![1.0, 0.0],
                candidate_k: 1,
                limit: 1,
                metric: VectorMetric::Cosine,
            },
        )
    });
    let reranked = reranked.unwrap();
    assert_eq!(reranked.len(), 1);
    assert_eq!(reranked[0].row_id, rows[0].row_id);
    assert!(trace.rls_rows_evaluated < 10, "{trace:?}");
    assert_eq!(trace.rls_policy_columns_decoded, trace.rls_rows_evaluated);
    assert_eq!(
        admin
            .query_for_current_principal("docs", &Query::new(), None)
            .unwrap()
            .len(),
        10
    );

    admin.revoke_role("alice", "reader").unwrap();
    assert!(matches!(
        alice.query_for_current_principal("docs", &Query::new(), None),
        Err(MongrelError::PermissionDenied { .. })
    ));
}

#[test]
fn scored_retry_rechecks_live_column_permissions() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("docs", int_pk_schema()).unwrap();
    admin.create_user("alice", "alice-pw").unwrap();
    admin.create_role("reader").unwrap();
    admin
        .grant_permission(
            "reader",
            Permission::SelectColumns {
                table: "docs".into(),
                columns: vec!["id".into()],
            },
        )
        .unwrap();
    admin.grant_role("alice", "reader").unwrap();
    let alice = admin.resolve_principal("alice").unwrap();
    let calls = std::cell::Cell::new(0);
    let result = admin.with_authorized_scored_read_context_at(
        "docs",
        Some(&alice),
        true,
        Some(&ReadAuthorization {
            operation: ColumnOperation::Select,
            columns: vec![1],
        }),
        None,
        None,
        |_, _, _, principal| {
            calls.set(calls.get() + 1);
            assert_eq!(principal.unwrap().username, "alice");
            admin.revoke_role("alice", "reader")?;
            Ok(())
        },
    );
    assert!(matches!(result, Err(MongrelError::PermissionDenied { .. })));
    assert_eq!(calls.get(), 1);
}

#[test]
fn authorized_retries_refresh_grants_and_dropped_users() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("docs", int_pk_schema()).unwrap();
    admin.create_role("reader").unwrap();
    admin
        .grant_permission(
            "reader",
            Permission::SelectColumns {
                table: "docs".into(),
                columns: vec!["id".into()],
            },
        )
        .unwrap();

    admin.create_user("alice", "alice-pw").unwrap();
    admin.grant_role("alice", "reader").unwrap();
    let alice = admin.resolve_principal("alice").unwrap();
    let calls = std::cell::Cell::new(0);
    let value = admin
        .with_authorized_read_context(
            "docs",
            Some(&alice),
            true,
            Some(&ReadAuthorization {
                operation: ColumnOperation::Select,
                columns: vec![1],
            }),
            None,
            None,
            |_, _, _, principal| {
                calls.set(calls.get() + 1);
                if calls.get() == 1 {
                    admin.create_role("new_grant")?;
                    admin.grant_role("alice", "new_grant")?;
                } else {
                    assert!(principal
                        .unwrap()
                        .roles
                        .iter()
                        .any(|role| role == "new_grant"));
                }
                Ok(7)
            },
        )
        .unwrap();
    assert_eq!(value, 7);
    assert_eq!(calls.get(), 2);

    admin.create_user("bob", "bob-pw").unwrap();
    admin.grant_role("bob", "reader").unwrap();
    let bob = admin.resolve_principal("bob").unwrap();
    let calls = std::cell::Cell::new(0);
    let result = admin.with_authorized_scored_read_context_at(
        "docs",
        Some(&bob),
        true,
        Some(&ReadAuthorization {
            operation: ColumnOperation::Select,
            columns: vec![1],
        }),
        None,
        None,
        |_, _, _, _| {
            calls.set(calls.get() + 1);
            admin.drop_user("bob")?;
            Ok(())
        },
    );
    assert!(matches!(result, Err(MongrelError::AuthRequired)));
    assert_eq!(calls.get(), 1);
}

/// A credentialless database has `require_auth = false`, the `require()`
/// helper is a no-op, and every operation works as today.
#[test]
fn credentialless_database_has_no_enforcement() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    assert!(!db.require_auth_enabled());
    assert!(db.principal().is_none());

    // require() is always Ok on a credentialless database.
    db.require(&Permission::Admin).unwrap();
    db.require(&Permission::Ddl).unwrap();
    db.require(&Permission::All).unwrap();

    // Operations work without any principal.
    db.create_table("orders", int_pk_schema()).unwrap();
    db.create_user("alice", "pw").unwrap();
    drop(db);
}

/// `create_with_credentials` produces a database that has `require_auth =
/// true`, one admin user, and a cached admin principal on the handle.
#[test]
fn create_with_credentials_bootstraps_admin() {
    let dir = tempdir().unwrap();
    let db = Database::create_with_credentials(dir.path(), "admin", "s3cret").unwrap();
    assert!(db.require_auth_enabled());
    let principal = db.principal().expect("admin principal cached");
    assert_eq!(principal.username, "admin");
    assert!(principal.is_admin);
    assert_eq!(db.users().len(), 1);
    drop(db);
}

/// Reopening a `require_auth` database without credentials fails with
/// `AuthRequired`. Reopening with the right credentials succeeds.
#[test]
fn require_auth_database_reopen_requires_credentials() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create_with_credentials(dir.path(), "admin", "s3cret").unwrap();
        db.create_table("orders", int_pk_schema()).unwrap();
    }
    // Plain open fails — credentials are required.
    match Database::open(dir.path()) {
        Err(MongrelError::AuthRequired) => {}
        other => panic!("expected AuthRequired, got {other:?}"),
    }
    // Wrong password fails.
    match Database::open_with_credentials(dir.path(), "admin", "wrong") {
        Err(MongrelError::InvalidCredentials { .. }) => {}
        other => panic!("expected InvalidCredentials, got {other:?}"),
    }
    // Right credentials succeed.
    let db = Database::open_with_credentials(dir.path(), "admin", "s3cret").unwrap();
    assert!(db.require_auth_enabled());
    assert_eq!(db.principal().unwrap().username, "admin");
}

/// Using a credentialed constructor on a credentialless database fails with
/// `AuthNotRequired` — callers must pick the matching constructor.
#[test]
fn credentialed_open_on_credentialless_database_is_rejected() {
    let dir = tempdir().unwrap();
    {
        let _db = Database::create(dir.path()).unwrap();
    }
    match Database::open_with_credentials(dir.path(), "x", "y") {
        Err(MongrelError::AuthNotRequired) => {}
        other => panic!("expected AuthNotRequired, got {other:?}"),
    }
}

/// An admin principal bypasses every permission check (the four-way matrix
/// all return Ok).
#[test]
fn admin_bypasses_all_checks() {
    let dir = tempdir().unwrap();
    let db = Database::create_with_credentials(dir.path(), "admin", "s3cret").unwrap();
    db.create_table("orders", int_pk_schema()).unwrap(); // Ddl
    db.create_user("alice", "pw").unwrap(); // Admin
    db.create_role("analyst").unwrap(); // Admin
    db.compact().unwrap(); // Ddl (maintenance)
}

/// A non-admin principal with no permissions is denied DDL and admin
/// operations. Bootstrap an admin, grant a low-privilege user, reopen as
/// that user, and verify the denials.
#[test]
fn non_admin_is_denied_ddl_and_admin() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Bootstrap as admin, then create a low-privilege user.
    {
        let db = Database::create_with_credentials(&path, "admin", "admin-pw").unwrap();
        db.create_table("orders", int_pk_schema()).unwrap();
        db.create_user("alice", "alice-pw").unwrap();
        db.create_role("reader").unwrap();
        db.grant_permission(
            "reader",
            Permission::Select {
                table: "orders".into(),
            },
        )
        .unwrap();
        db.grant_role("alice", "reader").unwrap();
    }

    // Reopen as alice (Select on orders only).
    let db = Database::open_with_credentials(&path, "alice", "alice-pw").unwrap();
    let p = db.principal().unwrap();
    assert_eq!(p.username, "alice");
    assert!(!p.is_admin);

    // DDL denied.
    match db.create_table("more", int_pk_schema()) {
        Err(MongrelError::PermissionDenied { .. }) => {}
        other => panic!("expected PermissionDenied for create_table, got {other:?}"),
    }
    // Admin denied.
    match db.create_user("bob", "pw") {
        Err(MongrelError::PermissionDenied { .. }) => {}
        other => panic!("expected PermissionDenied for create_user, got {other:?}"),
    }
    match db.create_role("writer") {
        Err(MongrelError::PermissionDenied { .. }) => {}
        other => panic!("expected PermissionDenied for create_role, got {other:?}"),
    }
    match db.grant_permission("reader", Permission::All) {
        Err(MongrelError::PermissionDenied { .. }) => {}
        other => panic!("expected PermissionDenied for grant_permission, got {other:?}"),
    }
    // Maintenance (Ddl) denied.
    match db.compact() {
        Err(MongrelError::PermissionDenied { .. }) => {}
        other => panic!("expected PermissionDenied for compact, got {other:?}"),
    }
}

/// A role with `Ddl` permission can create tables but still cannot create
/// users (Admin is a separate permission — `All` does not imply `Admin`).
#[test]
fn ddl_role_can_do_ddl_but_not_admin() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create_with_credentials(&path, "admin", "admin-pw").unwrap();
        db.create_user("dev", "dev-pw").unwrap();
        db.create_role("ddl_role").unwrap();
        db.grant_permission("ddl_role", Permission::Ddl).unwrap();
        db.grant_role("dev", "ddl_role").unwrap();
    }
    let db = Database::open_with_credentials(&path, "dev", "dev-pw").unwrap();

    // DDL allowed.
    db.create_table("schema_migrations", int_pk_schema())
        .unwrap();
    db.compact().unwrap(); // maintenance = Ddl

    // Admin still denied even with Ddl.
    match db.create_user("intruder", "pw") {
        Err(MongrelError::PermissionDenied { .. }) => {}
        other => panic!("expected PermissionDenied, got {other:?}"),
    }
}

/// `Permission::All` grants every table/DDL operation but does NOT grant
/// Admin — only `is_admin = true` does (spec §9 decision 2).
#[test]
fn all_permission_does_not_imply_admin() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create_with_credentials(&path, "admin", "admin-pw").unwrap();
        db.create_table("orders", int_pk_schema()).unwrap();
        db.create_user("power", "power-pw").unwrap();
        db.create_role("super").unwrap();
        db.grant_permission("super", Permission::All).unwrap();
        db.grant_role("power", "super").unwrap();
    }
    let db = Database::open_with_credentials(&path, "power", "power-pw").unwrap();

    // DDL and maintenance allowed via All.
    db.create_table("more", int_pk_schema()).unwrap();
    db.compact().unwrap();

    // Admin denied — All does not satisfy Admin (spec §9 decision 2).
    match db.create_user("intruder", "pw") {
        Err(MongrelError::PermissionDenied { .. }) => {}
        other => panic!("expected PermissionDenied, got {other:?}"),
    }
}

/// `enable_auth` converts a credentialless database to a credentialed one
/// in place: the handle continues to work (cached admin principal), and the
/// next reopen requires credentials.
#[test]
fn enable_auth_converts_existing_database() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Start credentialless, write some data.
    {
        let db = Database::create(&path).unwrap();
        db.create_table("orders", int_pk_schema()).unwrap();
        db.table("orders")
            .unwrap()
            .lock()
            .put(vec![(1, Value::Int64(1))])
            .unwrap();
        db.table("orders").unwrap().lock().commit().unwrap();
        // Convert in place.
        db.enable_auth("admin", "s3cret").unwrap();
        // The same handle is now authenticated as admin.
        assert!(db.require_auth_enabled());
        assert_eq!(db.principal().unwrap().username, "admin");
        // Operations on this handle still work.
        db.create_table("items", int_pk_schema()).unwrap();
    }

    // Reopen without credentials → AuthRequired.
    match Database::open(&path) {
        Err(MongrelError::AuthRequired) => {}
        other => panic!("expected AuthRequired, got {other:?}"),
    }
    // Reopen with credentials works.
    let db = Database::open_with_credentials(&path, "admin", "s3cret").unwrap();
    // The data written before enable_auth is still there — the table is live
    // and the row count reflects the put.
    let count = db.table("orders").unwrap().lock().count();
    assert_eq!(count, 1, "data survived enable_auth conversion");
}

/// `enable_auth` refuses if auth is already enabled (idempotency guard).
#[test]
fn enable_auth_refuses_if_already_enabled() {
    let dir = tempdir().unwrap();
    let db = Database::create_with_credentials(dir.path(), "admin", "s3cret").unwrap();
    let err = db.enable_auth("other", "pw").unwrap_err();
    assert!(
        matches!(err, MongrelError::InvalidArgument(_)),
        "got {err:?}"
    );
}

/// `enable_auth` rejects a duplicate username so the bootstrap doesn't
/// shadow an existing user.
#[test]
fn enable_auth_rejects_duplicate_username() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_user("alice", "first").unwrap();
    let err = db.enable_auth("alice", "second").unwrap_err();
    assert!(
        matches!(err, MongrelError::InvalidArgument(_)),
        "got {err:?}"
    );
}

/// `disable_auth` reverts a credentialed database to credentialless. After
/// disable, plain `open` works without credentials, and existing users/roles
/// are preserved in the catalog.
#[test]
fn disable_auth_reverts_to_credentialless() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Create credentialed + add some data.
    {
        let db = Database::create_with_credentials(&path, "admin", "admin-pw").unwrap();
        db.create_table("orders", int_pk_schema()).unwrap();
        db.create_user("alice", "alice-pw").unwrap();
        db.create_role("analyst").unwrap();
    }

    // Reopen with credentials, then disable.
    {
        let db = Database::open_with_credentials(&path, "admin", "admin-pw").unwrap();
        assert!(db.require_auth_enabled());
        db.disable_auth().unwrap();
        assert!(!db.require_auth_enabled());
    }

    // Plain open now works without credentials.
    let db = Database::open(&path).unwrap();
    assert!(!db.require_auth_enabled());

    // Users/roles preserved (auth data is still in the catalog, just not enforced).
    assert_eq!(db.users().len(), 2); // admin + alice
    assert_eq!(db.roles().len(), 1); // analyst

    // Can re-enable later.
    db.enable_auth("admin2", "new-pw").unwrap();
    assert!(db.require_auth_enabled());
}

/// `disable_auth` refuses if auth is already disabled.
#[test]
fn disable_auth_refuses_if_already_disabled() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let err = db.disable_auth().unwrap_err();
    assert!(
        matches!(err, MongrelError::InvalidArgument(_)),
        "got {err:?}"
    );
}

/// `refresh_principal` picks up a newly granted permission without
/// re-verifying the password (spec §9 decision 3).
#[test]
fn refresh_principal_picks_up_new_grant() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create_with_credentials(&path, "admin", "admin-pw").unwrap();
        db.create_table("orders", int_pk_schema()).unwrap();
        db.create_user("alice", "alice-pw").unwrap();
        // alice has no permissions yet.
    }
    let db = Database::open_with_credentials(&path, "alice", "alice-pw").unwrap();

    // alice cannot do DDL yet.
    match db.create_table("more", int_pk_schema()) {
        Err(MongrelError::PermissionDenied { .. }) => {}
        other => panic!("expected PermissionDenied pre-grant, got {other:?}"),
    }

    // Admin (separate handle) grants alice the Ddl permission.
    let admin_db = Database::open_with_credentials(&path, "admin", "admin-pw").unwrap();
    admin_db.create_role("ddl_role").unwrap();
    admin_db
        .grant_permission("ddl_role", Permission::Ddl)
        .unwrap();
    admin_db.grant_role("alice", "ddl_role").unwrap();
    drop(admin_db);

    // Existing handles refresh before enforcement and pick up the grant.
    db.create_table("now_yes", int_pk_schema()).unwrap();
}

/// Backward compatibility: a catalog serialized without `require_auth`
/// (simulating an old database) deserializes to `require_auth = false`.
#[test]
fn old_catalog_without_require_auth_deserializes_to_false() {
    use mongreldb_core::catalog::Catalog;
    // Hand-build a minimal catalog JSON that omits require_auth entirely
    // (as every pre-0.33 database on disk would).
    let old_json = r#"{
        "db_epoch": 1,
        "next_table_id": 1,
        "open_generation": 1,
        "next_segment_no": 1,
        "tables": []
    }"#;
    let cat: Catalog = serde_json::from_str(old_json).unwrap();
    assert!(!cat.require_auth, "missing field must default to false");
}

/// Encrypted + credentialed databases compose: the passphrase protects the
/// bytes, the credentials protect the operations.
#[cfg(feature = "encryption")]
#[test]
fn encrypted_and_credentialed_compose() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db =
            Database::create_encrypted_with_credentials(&path, "passphrase", "admin", "s3cret")
                .unwrap();
        assert!(db.require_auth_enabled());
        db.create_table("orders", int_pk_schema()).unwrap();
    }

    // Wrong passphrase → can't even decrypt the catalog.
    assert!(Database::open_encrypted(&path, "wrong").is_err());
    // Right passphrase, no credentials → AuthRequired (catalog decrypts, but
    // require_auth is set and there is no principal).
    match Database::open_encrypted(&path, "passphrase") {
        Err(MongrelError::AuthRequired) => {}
        other => panic!("expected AuthRequired, got {other:?}"),
    }
    // Right passphrase + right credentials → works.
    let db =
        Database::open_encrypted_with_credentials(&path, "passphrase", "admin", "s3cret").unwrap();
    assert!(db.require_auth_enabled());
}

// ── Phase 2: Table / Transaction / SQL enforcement ─────────────────────────

/// A user with `Insert` on a table can put rows via Transaction, but cannot
/// delete or query (no `Delete`/`Select` permission).
#[test]
fn transaction_put_enforces_insert_permission() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create_with_credentials(&path, "admin", "admin-pw").unwrap();
        db.create_table("orders", int_pk_schema()).unwrap();
        db.create_user("writer", "writer-pw").unwrap();
        db.create_role("insert_only").unwrap();
        db.grant_permission(
            "insert_only",
            Permission::Insert {
                table: "orders".into(),
            },
        )
        .unwrap();
        db.grant_role("writer", "insert_only").unwrap();
    }
    let db = Database::open_with_credentials(&path, "writer", "writer-pw").unwrap();

    // put via Transaction → Insert allowed.
    let mut txn = db.begin();
    txn.put("orders", vec![(1, Value::Int64(1))]).unwrap();
    txn.commit().unwrap();

    // delete via Transaction → Delete denied.
    let mut txn = db.begin();
    match txn.delete("orders", mongreldb_core::RowId(1)) {
        Err(MongrelError::PermissionDenied { .. }) => {}
        other => panic!("expected PermissionDenied for delete, got {other:?}"),
    }
    txn.rollback();

    // query via Table → Select denied.
    let handle = db.table("orders").unwrap();
    let err = handle
        .lock()
        .query(&mongreldb_core::query::Query::new())
        .unwrap_err();
    assert!(
        matches!(err, MongrelError::PermissionDenied { .. }),
        "got {err:?}"
    );
}

/// A user with only `Select` can query but not put.
#[test]
fn table_query_enforces_select_permission() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create_with_credentials(&path, "admin", "admin-pw").unwrap();
        db.create_table("orders", int_pk_schema()).unwrap();
        // Seed a row as admin.
        let mut txn = db.begin();
        txn.put("orders", vec![(1, Value::Int64(42))]).unwrap();
        txn.commit().unwrap();
        db.create_user("reader", "reader-pw").unwrap();
        db.create_role("read_only").unwrap();
        db.grant_permission(
            "read_only",
            Permission::Select {
                table: "orders".into(),
            },
        )
        .unwrap();
        db.grant_role("reader", "read_only").unwrap();
    }
    let db = Database::open_with_credentials(&path, "reader", "reader-pw").unwrap();

    // query → Select allowed.
    let handle = db.table("orders").unwrap();
    let rows = {
        let mut guard = handle.lock();
        guard.query(&mongreldb_core::query::Query::new()).unwrap()
    };
    assert_eq!(rows.len(), 1);

    // put via Table → Insert denied.
    let err = handle.lock().put(vec![(1, Value::Int64(99))]).unwrap_err();
    assert!(
        matches!(err, MongrelError::PermissionDenied { .. }),
        "got {err:?}"
    );
}

/// Transaction `update_many` requires `Update`; `upsert` with DoUpdate also
/// requires `Update` on the update branch.
#[test]
fn transaction_update_enforces_update_permission() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create_with_credentials(&path, "admin", "admin-pw").unwrap();
        db.create_table("orders", int_pk_schema()).unwrap();
        let mut txn = db.begin();
        txn.put("orders", vec![(1, Value::Int64(1))]).unwrap();
        txn.commit().unwrap();
        db.create_user("writer", "writer-pw").unwrap();
        db.create_role("insert_select").unwrap();
        db.grant_permission(
            "insert_select",
            Permission::Insert {
                table: "orders".into(),
            },
        )
        .unwrap();
        db.grant_permission(
            "insert_select",
            Permission::Select {
                table: "orders".into(),
            },
        )
        .unwrap();
        db.grant_role("writer", "insert_select").unwrap();
    }
    let db = Database::open_with_credentials(&path, "writer", "writer-pw").unwrap();

    // update_many → Update denied (only has Insert + Select).
    let mut txn = db.begin();
    let result = txn.update_many(
        "orders",
        vec![(mongreldb_core::RowId(1), vec![(1, Value::Int64(2))])],
    );
    match result {
        Err(MongrelError::PermissionDenied { .. }) => {}
        other => panic!("expected PermissionDenied for update_many, got {other:?}"),
    }
    txn.rollback();
}

/// Table::put_batch and Table::delete are enforced (direct table access path,
/// not via Transaction).
#[test]
fn direct_table_access_enforces_permissions() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create_with_credentials(&path, "admin", "admin-pw").unwrap();
        db.create_table("orders", int_pk_schema()).unwrap();
        db.create_user("reader", "reader-pw").unwrap();
        db.create_role("read_only").unwrap();
        db.grant_permission(
            "read_only",
            Permission::Select {
                table: "orders".into(),
            },
        )
        .unwrap();
        db.grant_role("reader", "read_only").unwrap();
    }
    let db = Database::open_with_credentials(&path, "reader", "reader-pw").unwrap();

    // Direct put on table → Insert denied.
    let handle = db.table("orders").unwrap();
    let err = handle.lock().put(vec![(1, Value::Int64(1))]).unwrap_err();
    assert!(
        matches!(err, MongrelError::PermissionDenied { .. }),
        "got {err:?}"
    );
    // Direct put_batch → Insert denied.
    let err = handle
        .lock()
        .put_batch(vec![vec![(1, Value::Int64(1))]])
        .unwrap_err();
    assert!(
        matches!(err, MongrelError::PermissionDenied { .. }),
        "got {err:?}"
    );
    // Direct truncate → Delete denied.
    let err = handle.lock().truncate().unwrap_err();
    assert!(
        matches!(err, MongrelError::PermissionDenied { .. }),
        "got {err:?}"
    );
}

/// Admin bypass works at the Table/Transaction layer too (is_admin short-
/// circuits all checks).
#[test]
fn admin_bypasses_table_and_transaction_checks() {
    let dir = tempdir().unwrap();
    let db = Database::create_with_credentials(dir.path(), "admin", "s3cret").unwrap();
    db.create_table("orders", int_pk_schema()).unwrap();

    // Admin can do everything via Transaction.
    let mut txn = db.begin();
    txn.put("orders", vec![(1, Value::Int64(1))]).unwrap();
    txn.delete("orders", mongreldb_core::RowId(1)).unwrap();
    txn.commit().unwrap();

    // Admin can do everything via direct Table access.
    let handle = db.table("orders").unwrap();
    handle.lock().put(vec![(1, Value::Int64(2))]).unwrap();
    handle
        .lock()
        .query(&mongreldb_core::query::Query::new())
        .unwrap();
    handle.lock().truncate().unwrap();
}

/// `Permission::All` satisfies all table-level operations at the Table/
/// Transaction layer (but still not Admin, per spec §9 decision 2).
#[test]
fn all_permission_satisfies_all_table_operations() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create_with_credentials(&path, "admin", "admin-pw").unwrap();
        db.create_table("orders", int_pk_schema()).unwrap();
        db.create_user("power", "power-pw").unwrap();
        db.create_role("super").unwrap();
        db.grant_permission("super", Permission::All).unwrap();
        db.grant_role("power", "super").unwrap();
    }
    let db = Database::open_with_credentials(&path, "power", "power-pw").unwrap();

    // All table-level operations succeed.
    let mut txn = db.begin();
    txn.put("orders", vec![(1, Value::Int64(1))]).unwrap();
    txn.delete("orders", mongreldb_core::RowId(1)).unwrap();
    txn.commit().unwrap();

    let handle = db.table("orders").unwrap();
    handle.lock().put(vec![(1, Value::Int64(2))]).unwrap();
    handle
        .lock()
        .query(&mongreldb_core::query::Query::new())
        .unwrap();

    // But Admin operations are still denied.
    match db.create_user("intruder", "pw") {
        Err(MongrelError::PermissionDenied { .. }) => {}
        other => panic!("expected PermissionDenied for create_user, got {other:?}"),
    }
}
