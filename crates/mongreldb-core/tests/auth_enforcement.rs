//! Phase 1 — optional credential enforcement at the storage layer.
//!
//! These tests cover the engine-level matrix from
//! `docs/15-credential-enforcement.md` §4.3: create-with-credentials bootstrap,
//! credentialed open, the DDL/admin/maintenance enforcement points, and
//! fail-closed semantics. Table/Transaction/SQL enforcement lands in Phase 2.

use mongreldb_core::auth::Permission;
use mongreldb_core::catalog;
use mongreldb_core::{
    query::{AnnRerankRequest, Condition, Query, VectorMetric},
    schema::*,
    ApproxAgg, ColumnMask, ColumnOperation, Database, MaskStrategy, MongrelError, NativeAgg,
    PolicyCommand, ReadAuthorization, RowPolicy, SecurityCatalog, SecurityExpr, StoredTrigger,
    TriggerCell, TriggerDefinition, TriggerEvent, TriggerProgram, TriggerStep, TriggerTarget,
    TriggerTiming, TriggerValue, Value,
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
            embedding_source: None,
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn two_column_schema() -> Schema {
    Schema {
        schema_id: 2,
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
                name: "secret".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        ..Schema::default()
    }
}

fn three_column_schema() -> Schema {
    Schema {
        schema_id: 3,
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
                name: "public_title".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 3,
                name: "private_notes".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        ..Schema::default()
    }
}

fn query_as(
    db: &Database,
    principal: &mongreldb_core::Principal,
    table_name: &str,
    query: &Query,
    projection: Option<&[u16]>,
) -> mongreldb_core::Result<Vec<mongreldb_core::Row>> {
    let condition_columns = mongreldb_core::query::condition_columns(&query.conditions);
    db.with_authorized_read(
        table_name,
        Some(principal),
        true,
        |table, snapshot, allowed, effective_principal| {
            let allowed_columns = db.select_column_ids_for(table_name, effective_principal)?;
            db.require_columns_for(
                table_name,
                ColumnOperation::Select,
                &condition_columns,
                effective_principal,
            )?;
            if let Some(projection) = projection {
                db.require_columns_for(
                    table_name,
                    ColumnOperation::Select,
                    projection,
                    effective_principal,
                )?;
            }
            let mut rows = table.query_at_with_allowed(query, snapshot, allowed)?;
            let projection = projection.map(|columns| {
                columns
                    .iter()
                    .copied()
                    .collect::<std::collections::HashSet<_>>()
            });
            for row in &mut rows {
                row.columns.retain(|column, _| {
                    allowed_columns.contains(column)
                        && projection
                            .as_ref()
                            .is_none_or(|projection| projection.contains(column))
                });
            }
            db.secure_rows_for(table_name, rows, effective_principal)
        },
    )
}

fn ann_rerank_as(
    db: &Database,
    principal: &mongreldb_core::Principal,
    table_name: &str,
    request: &AnnRerankRequest,
) -> mongreldb_core::Result<Vec<mongreldb_core::query::AnnRerankHit>> {
    let authorization = ReadAuthorization {
        operation: ColumnOperation::Select,
        columns: vec![request.column_id],
        permissions: Vec::new(),
    };
    db.with_authorized_scored_read_context(
        table_name,
        Some(principal),
        true,
        Some(&authorization),
        None,
        |table, snapshot, candidate_authorization, effective_principal| {
            db.require_columns_for(
                table_name,
                ColumnOperation::Select,
                &[request.column_id],
                effective_principal,
            )?;
            table.ann_rerank_at_with_candidate_authorization_on_generation(
                request,
                snapshot,
                candidate_authorization,
                None,
            )
        },
    )
}

fn approx_aggregate_as(
    db: &Database,
    principal: &mongreldb_core::Principal,
    table_name: &str,
    conditions: &[Condition],
    column: Option<u16>,
    agg: ApproxAgg,
    z: f64,
) -> mongreldb_core::Result<Option<mongreldb_core::ApproxResult>> {
    let mut columns = mongreldb_core::query::condition_columns(conditions);
    columns.extend(column);
    columns.sort_unstable();
    columns.dedup();
    let authorization = ReadAuthorization {
        operation: ColumnOperation::Select,
        columns,
        permissions: Vec::new(),
    };
    db.with_authorized_scored_read_context(
        table_name,
        Some(principal),
        true,
        Some(&authorization),
        None,
        |table, _, candidate_authorization, _| {
            table.approx_aggregate_with_candidate_authorization(
                conditions,
                column,
                agg,
                z,
                candidate_authorization,
            )
        },
    )
}

fn incremental_aggregate_as(
    db: &Database,
    principal: &mongreldb_core::Principal,
    table_name: &str,
    conditions: &[Condition],
    column: Option<u16>,
    _agg: NativeAgg,
) -> mongreldb_core::Result<()> {
    let mut columns = mongreldb_core::query::condition_columns(conditions);
    columns.extend(column);
    columns.sort_unstable();
    columns.dedup();
    db.with_authorized_read(
        table_name,
        Some(principal),
        true,
        |_, _, _, effective_principal| {
            db.require_columns_for(
                table_name,
                ColumnOperation::Select,
                &columns,
                effective_principal,
            )?;
            if db.security_active_for(table_name) {
                return Err(MongrelError::InvalidArgument(
                    "incremental aggregate is unsupported while RLS or column masks are active"
                        .into(),
                ));
            }
            Ok(())
        },
    )
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
                        embedding_source: None,
                    },
                    ColumnDef {
                        id: 2,
                        name: "owner".into(),
                        ty: TypeId::Bytes,
                        flags: ColumnFlags::empty(),
                        default_value: None,
                        embedding_source: None,
                    },
                    ColumnDef {
                        id: 3,
                        name: "secret".into(),
                        ty: TypeId::Bytes,
                        flags: ColumnFlags::empty(),
                        default_value: None,
                        embedding_source: None,
                    },
                    ColumnDef {
                        id: 4,
                        name: "embedding".into(),
                        ty: TypeId::Embedding { dim: 2 },
                        flags: ColumnFlags::empty(),
                        default_value: None,
                        embedding_source: None,
                    },
                    ColumnDef {
                        id: 5,
                        name: "value".into(),
                        ty: TypeId::Int64,
                        flags: ColumnFlags::empty(),
                        default_value: None,
                        embedding_source: None,
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
                (5, Value::Int64(100)),
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
                    (5, Value::Int64(id)),
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
                (5, Value::Int64(200)),
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
            masks: vec![
                ColumnMask {
                    name: "redact_secret".into(),
                    table: "docs".into(),
                    column: 3,
                    strategy: MaskStrategy::Redact {
                        replacement: "***".into(),
                    },
                    exempt_subjects: vec!["admin".into()],
                },
                ColumnMask {
                    name: "hide_value".into(),
                    table: "docs".into(),
                    column: 5,
                    strategy: MaskStrategy::Null,
                    exempt_subjects: vec!["admin".into()],
                },
            ],
        })
        .unwrap();

    let alice = admin.resolve_principal("alice").unwrap();
    let rows = query_as(&admin, &alice, "docs", &Query::new(), None).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(1)));
    assert_eq!(
        rows[0].columns.get(&3),
        Some(&Value::Bytes(b"***".to_vec()))
    );
    let ann_rows = query_as(
        &admin,
        &alice,
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
        ann_rerank_as(
            &admin,
            &alice,
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
    let approximate =
        approx_aggregate_as(&admin, &alice, "docs", &[], None, ApproxAgg::Count, 1.96)
            .unwrap()
            .unwrap();
    assert_eq!(approximate.point, 1.0);
    assert_eq!(approximate.n_population, 10);
    assert_eq!(approximate.n_sample_live, 10);
    let masked_sum =
        approx_aggregate_as(&admin, &alice, "docs", &[], Some(5), ApproxAgg::Sum, 1.96)
            .unwrap()
            .unwrap();
    assert_eq!(masked_sum.point, 0.0);
    assert_eq!(masked_sum.n_passing, 0);
    assert!(matches!(
        incremental_aggregate_as(&admin, &alice, "docs", &[], None, NativeAgg::Count),
        Err(MongrelError::InvalidArgument(message))
            if message.contains("unsupported while RLS or column masks are active")
    ));
    admin
        .revoke_permission(
            "reader",
            Permission::Select {
                table: "docs".into(),
            },
        )
        .unwrap();
    admin
        .grant_permission(
            "reader",
            Permission::SelectColumns {
                table: "docs".into(),
                columns: vec![
                    "id".into(),
                    "owner".into(),
                    "secret".into(),
                    "embedding".into(),
                ],
            },
        )
        .unwrap();
    assert!(matches!(
        approx_aggregate_as(&admin, &alice, "docs", &[], Some(5), ApproxAgg::Sum, 1.96,),
        Err(MongrelError::PermissionDenied { .. })
    ));
    assert_eq!(
        admin
            .query_for_current_principal("docs", &Query::new(), None)
            .unwrap()
            .len(),
        10
    );

    admin.revoke_role("alice", "reader").unwrap();
    assert!(matches!(
        query_as(&admin, &alice, "docs", &Query::new(), None),
        Err(MongrelError::PermissionDenied { .. })
    ));
    assert!(matches!(
        approx_aggregate_as(&admin, &alice, "docs", &[], None, ApproxAgg::Count, 1.96,),
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
            permissions: Vec::new(),
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
fn scored_retry_rechecks_live_admin_role() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("docs", int_pk_schema()).unwrap();
    admin.create_user("alice", "alice-pw").unwrap();
    admin.create_role("operator").unwrap();
    admin
        .grant_permission(
            "operator",
            Permission::Select {
                table: "docs".into(),
            },
        )
        .unwrap();
    admin
        .grant_permission("operator", Permission::Admin)
        .unwrap();
    admin.grant_role("alice", "operator").unwrap();
    let alice = admin.resolve_principal("alice").unwrap();
    let calls = std::cell::Cell::new(0);
    let result = admin.with_authorized_scored_read_context_at(
        "docs",
        Some(&alice),
        true,
        Some(&ReadAuthorization {
            operation: ColumnOperation::Select,
            columns: vec![1],
            permissions: vec![Permission::Admin],
        }),
        None,
        None,
        |_, _, _, _| {
            calls.set(calls.get() + 1);
            admin.revoke_role("alice", "operator")?;
            Ok(())
        },
    );
    assert!(matches!(result, Err(MongrelError::PermissionDenied { .. })));
    assert_eq!(calls.get(), 1);
}

#[test]
fn scored_retry_rechecks_live_admin_permission() {
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
    admin.set_user_admin("alice", true).unwrap();
    let alice = admin.resolve_principal("alice").unwrap();
    let calls = std::cell::Cell::new(0);
    let result = admin.with_authorized_scored_read_context_at(
        "docs",
        Some(&alice),
        true,
        Some(&ReadAuthorization {
            operation: ColumnOperation::Select,
            columns: vec![1],
            permissions: vec![Permission::Admin],
        }),
        None,
        None,
        |_, _, _, principal| {
            calls.set(calls.get() + 1);
            assert!(principal.unwrap().is_admin);
            admin.set_user_admin("alice", false)?;
            Ok(())
        },
    );
    assert!(matches!(result, Err(MongrelError::PermissionDenied { .. })));
    assert_eq!(calls.get(), 1);
    admin
        .with_authorized_scored_read_context_at(
            "docs",
            Some(&alice),
            true,
            Some(&ReadAuthorization {
                operation: ColumnOperation::Select,
                columns: vec![1],
                permissions: Vec::new(),
            }),
            None,
            None,
            |_, _, _, _| Ok(()),
        )
        .unwrap();
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
                permissions: Vec::new(),
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
            permissions: Vec::new(),
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
    db.with_authorized_scored_read_context_at(
        "orders",
        None,
        false,
        Some(&ReadAuthorization {
            operation: ColumnOperation::Select,
            columns: vec![1],
            permissions: vec![Permission::Admin],
        }),
        None,
        None,
        |_, _, _, _| Ok(()),
    )
    .unwrap();
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

/// Resolving a catalog principal picks up a newly granted permission without
/// opening a second engine for the same database.
#[test]
fn resolved_principal_picks_up_new_grant() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let db = Database::create_with_credentials(&path, "admin", "admin-pw").unwrap();
    db.create_table("orders", int_pk_schema()).unwrap();
    db.create_user("alice", "alice-pw").unwrap();
    let alice = db.resolve_principal("alice").unwrap();

    // alice cannot do DDL yet.
    match db.require_for(Some(&alice), &Permission::Ddl) {
        Err(MongrelError::PermissionDenied { .. }) => {}
        other => panic!("expected PermissionDenied pre-grant, got {other:?}"),
    }

    db.create_role("ddl_role").unwrap();
    db.grant_permission("ddl_role", Permission::Ddl).unwrap();
    db.grant_role("alice", "ddl_role").unwrap();

    let refreshed = db.resolve_principal("alice").unwrap();
    db.require_for(Some(&refreshed), &Permission::Ddl).unwrap();
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

#[test]
fn update_authorization_uses_changed_columns_not_full_post_image() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("docs", three_column_schema()).unwrap();
    admin
        .transaction(|transaction| {
            transaction.put(
                "docs",
                vec![
                    (1, Value::Int64(1)),
                    (2, Value::Bytes(b"old".to_vec())),
                    (3, Value::Bytes(b"restricted".to_vec())),
                ],
            )?;
            Ok(())
        })
        .unwrap();
    admin.create_user("writer", "writer-pw").unwrap();
    admin.create_role("limited_writer").unwrap();
    admin
        .grant_permission(
            "limited_writer",
            Permission::Select {
                table: "docs".into(),
            },
        )
        .unwrap();
    admin
        .grant_permission(
            "limited_writer",
            Permission::Insert {
                table: "docs".into(),
            },
        )
        .unwrap();
    admin
        .grant_permission(
            "limited_writer",
            Permission::UpdateColumns {
                table: "docs".into(),
                columns: vec!["public_title".into()],
            },
        )
        .unwrap();
    admin.grant_role("writer", "limited_writer").unwrap();
    let writer = admin.resolve_principal("writer").unwrap();

    let row_id = admin
        .table("docs")
        .unwrap()
        .lock()
        .visible_rows(admin.snapshot().0)
        .unwrap()[0]
        .row_id;
    let reads_before = admin.security_catalog_disk_read_count();
    let mut transaction = admin.begin_as(Some(writer.clone()));
    transaction
        .update_many(
            "docs",
            vec![(row_id, vec![(2, Value::Bytes(b"changed".to_vec()))])],
        )
        .unwrap();
    transaction.commit().unwrap();
    assert_eq!(admin.security_catalog_disk_read_count(), reads_before);

    let current = admin
        .table("docs")
        .unwrap()
        .lock()
        .visible_rows(admin.snapshot().0)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(current.columns[&2], Value::Bytes(b"changed".to_vec()));
    assert_eq!(current.columns[&3], Value::Bytes(b"restricted".to_vec()));

    let mut denied = admin.begin_as(Some(writer.clone()));
    assert!(matches!(
        denied.update_many(
            "docs",
            vec![(
                current.row_id,
                vec![(3, Value::Bytes(b"forbidden".to_vec()))]
            )]
        ),
        Err(MongrelError::PermissionDenied { .. })
    ));
    denied.rollback();

    let mut transaction = admin.begin_as(Some(writer.clone()));
    transaction
        .upsert(
            "docs",
            vec![
                (1, Value::Int64(1)),
                (2, Value::Bytes(b"ignored".to_vec())),
                (3, Value::Bytes(b"ignored".to_vec())),
            ],
            mongreldb_core::UpsertAction::DoUpdate(vec![(2, Value::Bytes(b"upserted".to_vec()))]),
        )
        .unwrap();
    transaction.commit().unwrap();

    let current = admin
        .table("docs")
        .unwrap()
        .lock()
        .visible_rows(admin.snapshot().0)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let mut revoked = admin.begin_as(Some(writer));
    revoked
        .update_many(
            "docs",
            vec![(current.row_id, vec![(2, Value::Bytes(b"late".to_vec()))])],
        )
        .unwrap();
    admin.revoke_role("writer", "limited_writer").unwrap();
    assert!(matches!(
        revoked.commit(),
        Err(MongrelError::PermissionDenied { .. })
    ));
}

#[test]
fn shared_security_catalog_is_version_gated_and_fails_closed() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("docs", int_pk_schema()).unwrap();
    admin.create_user("writer", "writer-pw").unwrap();
    admin.create_role("writer_role").unwrap();
    admin
        .grant_permission(
            "writer_role",
            Permission::Insert {
                table: "docs".into(),
            },
        )
        .unwrap();
    admin.grant_role("writer", "writer_role").unwrap();
    let writer = admin.resolve_principal("writer").unwrap();

    let before = admin.security_catalog_disk_read_count();
    let mut transaction = admin.begin_as(Some(writer.clone()));
    transaction.put("docs", vec![(1, Value::Int64(1))]).unwrap();
    transaction.commit().unwrap();
    assert_eq!(admin.security_catalog_disk_read_count(), before);

    admin.create_role("unrelated").unwrap();
    let mut transaction = admin.begin_as(Some(writer.clone()));
    transaction.put("docs", vec![(1, Value::Int64(2))]).unwrap();
    transaction.commit().unwrap();
    assert_eq!(admin.security_catalog_disk_read_count(), before);

    let mut transaction = admin.begin_as(Some(writer));
    transaction.put("docs", vec![(1, Value::Int64(3))]).unwrap();
    admin.revoke_role("writer", "writer_role").unwrap();
    assert!(matches!(
        transaction.commit(),
        Err(MongrelError::PermissionDenied { .. })
    ));
    assert_eq!(admin.table("docs").unwrap().lock().count(), 2);
}

#[test]
fn failed_security_catalog_checkpoint_reports_durable_commit_and_recovers() {
    let dir = tempdir().unwrap();
    let db = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    let before = db.catalog_snapshot();
    let catalog = dir.path().join("CATALOG");
    let saved_catalog = dir.path().join("CATALOG.saved");

    std::fs::rename(&catalog, &saved_catalog).unwrap();
    std::fs::create_dir(&catalog).unwrap();
    let result = db.create_role("must_not_publish");
    std::fs::remove_dir(&catalog).unwrap();
    std::fs::rename(&saved_catalog, &catalog).unwrap();

    assert!(matches!(result, Err(MongrelError::DurableCommit { .. })));
    let after = db.catalog_snapshot();
    assert_eq!(after.security_version, before.security_version + 1);
    assert!(after
        .roles
        .iter()
        .any(|role| role.name == "must_not_publish"));

    assert!(db.create_role("after_restore").is_err());
    drop(db);
    let reopened = Database::open_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    assert!(reopened
        .roles()
        .iter()
        .any(|role| role.name == "must_not_publish"));
    assert!(reopened
        .roles()
        .iter()
        .all(|role| role.name != "after_restore"));
}

#[test]
fn controlled_security_catalog_rejection_does_not_publish_mutation() {
    let dir = tempdir().unwrap();
    let db = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    let before = db.catalog_snapshot();
    let mut callbacks = 0;

    let error = db
        .create_role_controlled("must_not_publish", || {
            callbacks += 1;
            Err(MongrelError::Other("cancelled before publish".into()))
        })
        .unwrap_err();
    assert_eq!(callbacks, 1);
    assert!(error.to_string().contains("cancelled before publish"));
    let after = db.catalog_snapshot();
    assert_eq!(after.security_version, before.security_version);
    assert!(after
        .roles
        .iter()
        .all(|role| role.name != "must_not_publish"));

    drop(db);
    let reopened = Database::open_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    assert!(reopened
        .roles()
        .iter()
        .all(|role| role.name != "must_not_publish"));
}

#[test]
fn trigger_added_update_column_is_finally_authorized() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("docs", three_column_schema()).unwrap();
    admin
        .create_trigger(
            StoredTrigger::new(
                "rewrite_private_notes",
                TriggerDefinition {
                    target: TriggerTarget::Table("docs".into()),
                    timing: TriggerTiming::Before,
                    event: TriggerEvent::Update,
                    update_of: Vec::new(),
                    target_columns: Vec::new(),
                    when: None,
                    program: TriggerProgram {
                        steps: vec![TriggerStep::SetNew {
                            cells: vec![TriggerCell {
                                column_id: 3,
                                value: TriggerValue::Literal(Value::Bytes(b"triggered".to_vec())),
                            }],
                        }],
                    },
                },
                0,
            )
            .unwrap(),
        )
        .unwrap();
    admin
        .transaction(|transaction| {
            transaction.put(
                "docs",
                vec![
                    (1, Value::Int64(1)),
                    (2, Value::Bytes(b"old".to_vec())),
                    (3, Value::Bytes(b"private".to_vec())),
                ],
            )?;
            Ok(())
        })
        .unwrap();
    admin.create_user("writer", "writer-pw").unwrap();
    admin.create_role("limited_writer").unwrap();
    admin
        .grant_permission(
            "limited_writer",
            Permission::UpdateColumns {
                table: "docs".into(),
                columns: vec!["public_title".into()],
            },
        )
        .unwrap();
    admin.grant_role("writer", "limited_writer").unwrap();
    let writer = admin.resolve_principal("writer").unwrap();
    let row_id = admin
        .table("docs")
        .unwrap()
        .lock()
        .visible_rows(admin.snapshot().0)
        .unwrap()[0]
        .row_id;
    let mut transaction = admin.begin_as(Some(writer));
    transaction
        .update_many(
            "docs",
            vec![(row_id, vec![(2, Value::Bytes(b"changed".to_vec()))])],
        )
        .unwrap();
    assert!(matches!(
        transaction.commit(),
        Err(MongrelError::PermissionDenied { .. })
    ));
    let row = admin
        .table("docs")
        .unwrap()
        .lock()
        .visible_rows(admin.snapshot().0)
        .unwrap()[0]
        .clone();
    assert_eq!(row.columns[&2], Value::Bytes(b"old".to_vec()));
    assert_eq!(row.columns[&3], Value::Bytes(b"private".to_vec()));
}

#[test]
fn batch_column_permissions_cover_every_row() {
    let dir = tempdir().unwrap();
    let db = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    db.create_table("docs", two_column_schema()).unwrap();
    db.create_user("writer", "writer-pw").unwrap();
    db.create_role("id_writer").unwrap();
    db.grant_permission(
        "id_writer",
        Permission::InsertColumns {
            table: "docs".into(),
            columns: vec!["id".into()],
        },
    )
    .unwrap();
    db.grant_role("writer", "id_writer").unwrap();

    let writer = db.resolve_principal("writer").unwrap();
    let mut txn = db.begin_as(Some(writer));
    let error = txn
        .put_batch(
            "docs",
            vec![
                vec![(1, Value::Int64(1))],
                vec![(1, Value::Int64(2)), (2, Value::Bytes(b"denied".to_vec()))],
            ],
        )
        .unwrap_err();
    assert!(matches!(error, MongrelError::PermissionDenied { .. }));
}

#[test]
fn commit_rechecks_revoked_batch_permission() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("docs", int_pk_schema()).unwrap();
    admin.create_user("writer", "writer-pw").unwrap();
    admin.create_role("writer_role").unwrap();
    admin
        .grant_permission(
            "writer_role",
            Permission::Insert {
                table: "docs".into(),
            },
        )
        .unwrap();
    admin.grant_role("writer", "writer_role").unwrap();
    let writer = admin.resolve_principal("writer").unwrap();

    let mut txn = admin.begin_as(Some(writer.clone()));
    txn.put_batch("docs", vec![vec![(1, Value::Int64(1))]])
        .unwrap();
    admin.revoke_role("writer", "writer_role").unwrap();

    let error = txn.commit().unwrap_err();
    assert!(matches!(error, MongrelError::PermissionDenied { .. }));
}

#[test]
fn commit_fails_auth_required_when_catalog_bound_user_was_dropped() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("docs", int_pk_schema()).unwrap();
    admin.create_user("writer", "writer-pw").unwrap();
    admin.create_role("writer_role").unwrap();
    admin
        .grant_permission(
            "writer_role",
            Permission::Insert {
                table: "docs".into(),
            },
        )
        .unwrap();
    admin.grant_role("writer", "writer_role").unwrap();
    let writer = admin.resolve_principal("writer").unwrap();
    let mut transaction = admin.begin_as(Some(writer.clone()));
    transaction.put("docs", vec![(1, Value::Int64(1))]).unwrap();
    admin.drop_user("writer").unwrap();
    assert!(matches!(
        transaction.commit(),
        Err(MongrelError::AuthRequired)
    ));
    assert_eq!(admin.table("docs").unwrap().lock().count(), 0);
}

#[test]
fn dropped_and_recreated_user_cannot_revive_preexisting_transaction() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("docs", int_pk_schema()).unwrap();
    admin.create_user("writer", "old-pw").unwrap();
    admin.create_role("writer_role").unwrap();
    admin
        .grant_permission(
            "writer_role",
            Permission::Insert {
                table: "docs".into(),
            },
        )
        .unwrap();
    admin.grant_role("writer", "writer_role").unwrap();
    let old = admin.resolve_principal("writer").unwrap();
    let mut transaction = admin.begin_as(Some(old));
    transaction.put("docs", vec![(1, Value::Int64(1))]).unwrap();

    admin.drop_user("writer").unwrap();
    admin.create_user("writer", "new-pw").unwrap();
    admin.grant_role("writer", "writer_role").unwrap();

    assert!(matches!(
        transaction.commit(),
        Err(MongrelError::AuthRequired)
    ));
    assert_eq!(admin.table("docs").unwrap().lock().count(), 0);
}

#[test]
fn dropped_user_cached_principal_cannot_start_writes() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("docs", int_pk_schema()).unwrap();
    admin.create_user("writer", "writer-pw").unwrap();
    admin.create_role("writer_role").unwrap();
    admin
        .grant_permission(
            "writer_role",
            Permission::Insert {
                table: "docs".into(),
            },
        )
        .unwrap();
    admin.grant_role("writer", "writer_role").unwrap();
    let stale = admin.resolve_principal("writer").unwrap();
    admin.drop_user("writer").unwrap();

    let mut transaction = admin.begin_as(Some(stale));
    assert!(matches!(
        transaction.put("docs", vec![(1, Value::Int64(1))]),
        Err(MongrelError::AuthRequired)
    ));
    assert!(matches!(
        transaction.commit(),
        Err(MongrelError::AuthRequired)
    ));
    assert_eq!(admin.table("docs").unwrap().lock().count(), 0);
}

#[test]
fn dropped_user_cached_principal_cannot_read_when_not_prebound() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("docs", int_pk_schema()).unwrap();
    admin.create_user("reader", "reader-pw").unwrap();
    admin.create_role("reader_role").unwrap();
    admin
        .grant_permission(
            "reader_role",
            Permission::Select {
                table: "docs".into(),
            },
        )
        .unwrap();
    admin.grant_role("reader", "reader_role").unwrap();
    let stale = admin.resolve_principal("reader").unwrap();
    admin.drop_user("reader").unwrap();

    let result = admin.with_authorized_read(
        "docs",
        Some(&stale),
        false,
        |_table, _snapshot, _allowed, _principal| Ok(()),
    );
    assert!(matches!(result, Err(MongrelError::AuthRequired)));
}

#[test]
fn dropped_and_recreated_user_cannot_revive_preexisting_read_principal() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("docs", int_pk_schema()).unwrap();
    admin.create_user("reader", "old-pw").unwrap();
    admin.create_role("reader_role").unwrap();
    admin
        .grant_permission(
            "reader_role",
            Permission::Select {
                table: "docs".into(),
            },
        )
        .unwrap();
    admin.grant_role("reader", "reader_role").unwrap();
    let old = admin.resolve_principal("reader").unwrap();
    admin.drop_user("reader").unwrap();
    admin.create_user("reader", "new-pw").unwrap();
    admin.grant_role("reader", "reader_role").unwrap();

    let result = admin.with_authorized_read(
        "docs",
        Some(&old),
        false,
        |_table, _snapshot, _allowed, _principal| Ok(()),
    );
    assert!(matches!(result, Err(MongrelError::AuthRequired)));
    assert!(matches!(
        admin.select_column_ids_for("docs", Some(&old)),
        Err(MongrelError::AuthRequired)
    ));
    assert!(matches!(
        admin.rows_for("docs", Some(&old)),
        Err(MongrelError::AuthRequired)
    ));
    assert!(matches!(
        admin.count_for("docs", Some(&old)),
        Err(MongrelError::AuthRequired)
    ));
    assert!(matches!(
        admin.secure_rows_for("docs", Vec::new(), Some(&old)),
        Err(MongrelError::AuthRequired)
    ));
}

#[test]
fn trigger_ddl_rejects_recreated_request_principal() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("docs", int_pk_schema()).unwrap();
    admin.create_user("designer", "old-pw").unwrap();
    admin.create_role("ddl_role").unwrap();
    admin.grant_permission("ddl_role", Permission::Ddl).unwrap();
    admin.grant_role("designer", "ddl_role").unwrap();
    let old = admin.resolve_principal("designer").unwrap();
    admin.drop_user("designer").unwrap();
    admin.create_user("designer", "new-pw").unwrap();
    admin.grant_role("designer", "ddl_role").unwrap();
    let trigger = StoredTrigger::new(
        "docs_ai",
        TriggerDefinition {
            target: TriggerTarget::Table("docs".into()),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            update_of: Vec::new(),
            target_columns: Vec::new(),
            when: None,
            program: TriggerProgram { steps: Vec::new() },
        },
        0,
    )
    .unwrap();

    assert!(matches!(
        admin.create_trigger_as_controlled(trigger, Some(&old), || Ok(())),
        Err(MongrelError::AuthRequired)
    ));
    assert!(admin.trigger("docs_ai").is_none());
}

#[test]
fn credentialed_open_authenticates_recovered_wal_catalog() {
    let dir = tempdir().unwrap();
    let db = Database::create_with_credentials(dir.path(), "admin", "old-pw").unwrap();
    let stale = db.catalog_snapshot();
    db.alter_user_password("admin", "new-pw").unwrap();
    drop(db);

    catalog::write_atomic(dir.path(), &stale, None).unwrap();
    assert!(matches!(
        Database::open_with_credentials(dir.path(), "admin", "old-pw"),
        Err(MongrelError::InvalidCredentials { .. })
    ));
    Database::open_with_credentials(dir.path(), "admin", "new-pw").unwrap();
}

#[test]
fn commit_rechecks_revoked_delete_batch_permission() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("docs", int_pk_schema()).unwrap();
    admin
        .transaction(|transaction| {
            transaction.put("docs", vec![(1, Value::Int64(1))])?;
            Ok(())
        })
        .unwrap();
    admin.create_user("writer", "writer-pw").unwrap();
    admin.create_role("delete_role").unwrap();
    admin
        .grant_permission(
            "delete_role",
            Permission::Delete {
                table: "docs".into(),
            },
        )
        .unwrap();
    admin.grant_role("writer", "delete_role").unwrap();
    let writer = admin.resolve_principal("writer").unwrap();
    let row_id = admin
        .table("docs")
        .unwrap()
        .lock()
        .visible_rows(admin.snapshot().0)
        .unwrap()[0]
        .row_id;

    let mut transaction = admin.begin_as(Some(writer.clone()));
    transaction.delete_batch("docs", vec![row_id]).unwrap();
    admin.revoke_role("writer", "delete_role").unwrap();

    let error = transaction.commit().unwrap_err();
    assert!(matches!(error, MongrelError::PermissionDenied { .. }));
    assert_eq!(admin.table("docs").unwrap().lock().count(), 1);
}

#[test]
fn revocation_during_commit_preparation_fails_before_publish() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("docs", int_pk_schema()).unwrap();
    admin.create_user("writer", "writer-pw").unwrap();
    admin.create_role("writer_role").unwrap();
    admin
        .grant_permission(
            "writer_role",
            Permission::Insert {
                table: "docs".into(),
            },
        )
        .unwrap();
    admin.grant_role("writer", "writer_role").unwrap();
    let writer = admin.resolve_principal("writer").unwrap();
    admin.set_spill_threshold(1);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let resume = std::sync::Arc::new(std::sync::Barrier::new(2));
    let hook_resume = resume.clone();
    admin.__set_spill_hook(move || {
        ready_tx.send(()).unwrap();
        hook_resume.wait();
    });
    let mut transaction = admin.begin_as(Some(writer.clone()));
    transaction.put("docs", vec![(1, Value::Int64(1))]).unwrap();

    let admin_ref = &admin;
    std::thread::scope(|scope| {
        scope.spawn(move || {
            ready_rx.recv().unwrap();
            admin_ref.revoke_role("writer", "writer_role").unwrap();
            resume.wait();
        });
        let error = transaction.commit().unwrap_err();
        assert!(matches!(error, MongrelError::Conflict(_)));
    });
    assert_eq!(admin.table("docs").unwrap().lock().count(), 0);
}

#[test]
fn stale_credentialless_transaction_observes_auth_enable_before_commit() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("docs", int_pk_schema()).unwrap();
    let mut transaction = db.begin();
    transaction.put("docs", vec![(1, Value::Int64(1))]).unwrap();
    db.enable_auth("admin", "admin-password").unwrap();

    let error = transaction.commit().unwrap_err();
    assert!(matches!(error, MongrelError::AuthRequired));
    assert_eq!(db.table("docs").unwrap().lock().count(), 0);
}

#[test]
fn revocation_waits_for_commit_already_inside_security_gate() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("docs", int_pk_schema()).unwrap();
    admin.create_user("writer", "writer-pw").unwrap();
    admin.create_role("writer_role").unwrap();
    admin
        .grant_permission(
            "writer_role",
            Permission::Insert {
                table: "docs".into(),
            },
        )
        .unwrap();
    admin.grant_role("writer", "writer_role").unwrap();
    let writer = admin.resolve_principal("writer").unwrap();
    admin.set_spill_threshold(1);

    let entered = std::sync::Arc::new(std::sync::Barrier::new(2));
    let attempted = std::sync::Arc::new(std::sync::Barrier::new(2));
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hook_entered = entered.clone();
    let hook_attempted = attempted.clone();
    let hook_done = done.clone();
    admin.__set_security_commit_hook(move || {
        hook_entered.wait();
        hook_attempted.wait();
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(!hook_done.load(std::sync::atomic::Ordering::Acquire));
    });
    let mut transaction = admin.begin_as(Some(writer.clone()));
    transaction.put("docs", vec![(1, Value::Int64(1))]).unwrap();

    let admin_ref = &admin;
    std::thread::scope(|scope| {
        scope.spawn(move || {
            entered.wait();
            attempted.wait();
            admin_ref.revoke_role("writer", "writer_role").unwrap();
            done.store(true, std::sync::atomic::Ordering::Release);
        });
        transaction.commit().unwrap();
    });
    assert_eq!(admin.table("docs").unwrap().lock().count(), 1);
}

#[test]
fn trigger_generated_batch_write_is_finally_authorized() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap();
    admin.create_table("base", int_pk_schema()).unwrap();
    admin.create_table("audit", int_pk_schema()).unwrap();
    admin
        .create_trigger(
            StoredTrigger::new(
                "base_ai",
                TriggerDefinition {
                    target: TriggerTarget::Table("base".into()),
                    timing: TriggerTiming::After,
                    event: TriggerEvent::Insert,
                    update_of: Vec::new(),
                    target_columns: Vec::new(),
                    when: None,
                    program: TriggerProgram {
                        steps: vec![TriggerStep::Insert {
                            table: "audit".into(),
                            cells: vec![TriggerCell {
                                column_id: 1,
                                value: TriggerValue::NewColumn(1),
                            }],
                        }],
                    },
                },
                0,
            )
            .unwrap(),
        )
        .unwrap();
    admin.create_user("writer", "writer-pw").unwrap();
    admin.create_role("base_writer").unwrap();
    admin
        .grant_permission(
            "base_writer",
            Permission::Insert {
                table: "base".into(),
            },
        )
        .unwrap();
    admin.grant_role("writer", "base_writer").unwrap();
    let writer = admin.resolve_principal("writer").unwrap();

    let mut txn = admin.begin_as(Some(writer));
    txn.put("base", vec![(1, Value::Int64(1))]).unwrap();
    let error = txn.commit().unwrap_err();
    assert!(matches!(error, MongrelError::PermissionDenied { .. }));
    assert_eq!(admin.table("base").unwrap().lock().count(), 0);
    assert_eq!(admin.table("audit").unwrap().lock().count(), 0);
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
