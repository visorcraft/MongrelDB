//! Stage 1A (spec §10.1): shared storage cores behind lightweight handles.
//!
//! Covers S1A-001 (one `DatabaseCore`, many `DatabaseHandle`s), S1A-002
//! (process-local `DatabaseManager` registry, exactly-once initialization,
//! last-drop closes), S1A-003 (stable file identity), and S1A-004
//! (lifecycle: drain-on-shutdown, operation guards).

use mongreldb_core::auth::Permission;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{
    Database, DatabaseManager, HandleAccess, HandleIdentity, LifecycleState, MongrelError,
    OpenIdentity, PolicyCommand, RowPolicy, SecretString, SecurityCatalog, SecurityExpr, Value,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn id_schema() -> Schema {
    Schema {
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
            embedding_source: None,
        }],
        ..Schema::default()
    }
}

/// Create a fresh database at a temporary root, then drop the exclusive
/// creator so `open_shared` performs the one shared initialization.
fn fresh_root() -> tempfile::TempDir {
    let dir = tempdir().unwrap();
    drop(Database::create(dir.path()).unwrap());
    dir
}

fn fresh_auth_root() -> tempfile::TempDir {
    let dir = tempdir().unwrap();
    let database =
        Database::create_with_credentials(dir.path(), "admin", "admin-password").unwrap();
    database.create_table("items", id_schema()).unwrap();
    database.create_role("reader").unwrap();
    database
        .grant_permission(
            "reader",
            Permission::Select {
                table: "items".into(),
            },
        )
        .unwrap();
    database.create_role("writer").unwrap();
    database
        .grant_permission(
            "writer",
            Permission::Insert {
                table: "items".into(),
            },
        )
        .unwrap();
    for username in ["alice", "bob"] {
        database.create_user(username, "user-password").unwrap();
        database.grant_role(username, "reader").unwrap();
        database.grant_role(username, "writer").unwrap();
    }
    drop(database);
    dir
}

fn owner_schema() -> Schema {
    Schema {
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
        ],
        ..Schema::default()
    }
}

#[test]
fn open_shared_handles_reference_the_same_core() {
    let dir = fresh_root();
    let alice = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    let worker = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    assert!(
        alice.shares_core_with(&worker),
        "both handles must reference the exact same process-local DatabaseCore"
    );
    assert_eq!(alice.lifecycle_state(), LifecycleState::Open);
}

#[test]
fn writes_through_one_handle_are_visible_to_the_other() {
    let dir = fresh_root();
    let writer = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    let reader = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();

    writer.create_table("items", id_schema()).unwrap();
    writer.put("items", vec![(1, Value::Int64(1))]).unwrap();
    writer.put("items", vec![(1, Value::Int64(2))]).unwrap();
    assert_eq!(reader.count("items").unwrap(), 2);
}

#[test]
fn per_handle_identity_is_retained() {
    let dir = fresh_root();
    let anonymous = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    let def = anonymous
        .register_service_principal(
            "worker-a",
            [7; 16],
            vec![Permission::Select {
                table: "items".into(),
            }],
            "service-secret",
            0,
        )
        .unwrap();
    let service = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::ServiceCredentials {
                token_id: "worker-a".into(),
                secret: SecretString::new("service-secret"),
            },
        )
        .unwrap();
    assert_eq!(anonymous.identity(), &HandleIdentity::Credentialless);
    assert_eq!(
        service.identity(),
        &HandleIdentity::ServicePrincipal {
            token_id: "worker-a".into(),
            principal_id: [7; 16],
            creation_version: def.creation_version,
        }
    );
    assert!(
        anonymous.shares_core_with(&service),
        "distinct identities still share one core"
    );
}

#[test]
fn dropping_one_handle_keeps_storage_open_for_the_rest() {
    let dir = fresh_root();
    let first = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    let second = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    first.create_table("items", id_schema()).unwrap();
    drop(first);

    // Storage is unaffected by the drop: the surviving handle still writes.
    second.put("items", vec![(1, Value::Int64(9))]).unwrap();
    assert_eq!(second.count("items").unwrap(), 1);
    assert_eq!(second.lifecycle_state(), LifecycleState::Open);

    // Last drop closes storage: an exclusive open succeeds afterwards.
    drop(second);
    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.table("items").unwrap().lock().count(), 1);
}

#[test]
fn exclusive_open_and_shared_core_reject_each_other() {
    let dir = fresh_root();

    // Shared core exists → exclusive open is rejected.
    let handle = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    assert!(matches!(
        Database::open(dir.path()),
        Err(MongrelError::DatabaseLocked { .. })
    ));
    drop(handle);

    // Exclusive owner exists → shared attach is rejected.
    let owner = Database::open(dir.path()).unwrap();
    assert!(matches!(
        DatabaseManager::global().open_shared(dir.path(), OpenIdentity::Credentialless),
        Err(MongrelError::DatabaseLocked { .. })
    ));
    drop(owner);

    // Both directions clear once the other closes.
    let handle = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    drop(handle);
    Database::open(dir.path()).unwrap();
}

#[test]
fn concurrent_open_shared_initializes_exactly_once() {
    let dir = fresh_root();
    let root = dir.path().to_path_buf();
    let mut threads = Vec::new();
    for _ in 0..8 {
        let root = root.clone();
        threads.push(std::thread::spawn(move || {
            DatabaseManager::global()
                .open_shared(root, OpenIdentity::Credentialless)
                .unwrap()
        }));
    }
    let handles: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    for handle in &handles[1..] {
        assert!(
            handles[0].shares_core_with(handle),
            "every racer must land on the one initialized core"
        );
    }
    // The shared core works for every racer.
    handles[0].create_table("items", id_schema()).unwrap();
    handles[3].put("items", vec![(1, Value::Int64(4))]).unwrap();
    assert_eq!(handles[7].count("items").unwrap(), 1);
}

#[test]
fn shutdown_drains_operations_and_closes_the_core() {
    let dir = fresh_root();
    let operator = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    let closer = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    operator.create_table("items", id_schema()).unwrap();
    operator.put("items", vec![(1, Value::Int64(1))]).unwrap();

    // An in-flight operation holds the drain open.
    let guard = operator.operation_guard().unwrap();
    let shutdown = std::thread::spawn(move || closer.shutdown(Duration::from_secs(30)));
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !shutdown.is_finished(),
        "shutdown must wait for in-flight operations"
    );
    assert_eq!(operator.lifecycle_state(), LifecycleState::Draining);
    // New operations are rejected while draining.
    assert!(operator.operation_guard().is_err());
    drop(guard);

    shutdown.join().unwrap().unwrap();
    assert_eq!(operator.lifecycle_state(), LifecycleState::Closed);
    assert!(operator.operation_guard().is_err());

    // A fresh attach re-initializes a new core; durable state survived.
    let reopened = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    assert_eq!(reopened.lifecycle_state(), LifecycleState::Open);
    assert_eq!(reopened.count("items").unwrap(), 1);
    assert!(!operator.shares_core_with(&reopened));
}

#[test]
fn exclusive_database_reports_identity_and_open_lifecycle() {
    let dir = tempdir().unwrap();
    let database = Database::create(dir.path()).unwrap();
    assert_eq!(database.identity(), HandleIdentity::Credentialless);
    assert_eq!(database.lifecycle_state(), LifecycleState::Open);
    database.operation_guard().unwrap();
}

#[test]
fn read_only_handle_rejects_writes_and_allows_reads() {
    let dir = fresh_root();
    let writer = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    writer.create_table("items", id_schema()).unwrap();
    writer.put("items", vec![(1, Value::Int64(1))]).unwrap();
    let reader = DatabaseManager::global()
        .open_shared_with_access(
            dir.path(),
            OpenIdentity::Credentialless,
            HandleAccess::read_only(),
        )
        .unwrap();
    assert_eq!(reader.count("items").unwrap(), 1);
    assert!(matches!(
        reader.put("items", vec![(1, Value::Int64(2))]),
        Err(MongrelError::ReadOnlyHandle { .. })
    ));
    let row_id = reader.rows("items").unwrap()[0].row_id;
    assert!(matches!(
        reader.delete("items", row_id),
        Err(MongrelError::ReadOnlyHandle { .. })
    ));
    assert!(matches!(
        reader.create_table("forbidden", id_schema()),
        Err(MongrelError::ReadOnlyHandle { .. })
    ));
    // P1.4: full write surface denied on read-only handles.
    assert!(matches!(
        reader.put_batch("items", vec![vec![(1, Value::Int64(3))]]),
        Err(MongrelError::ReadOnlyHandle { .. })
    ));
    assert!(matches!(
        reader.update("items", row_id, vec![(1, Value::Int64(9))]),
        Err(MongrelError::ReadOnlyHandle { .. })
    ));
    assert!(matches!(
        reader.begin(),
        Err(MongrelError::ReadOnlyHandle { .. })
    ));
}

/// P1.4-X1/X2/X3: full CRUD + batch + explicit transaction on shared handles.
#[test]
fn p14_full_crud_batch_and_authorized_transaction() {
    let dir = fresh_root();
    let handle = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    handle.create_table("items", id_schema()).unwrap();

    // Create
    handle.put("items", vec![(1, Value::Int64(1))]).unwrap();
    let assigned = handle
        .put_batch(
            "items",
            vec![
                vec![(1, Value::Int64(2))],
                vec![(1, Value::Int64(3))],
            ],
        )
        .unwrap();
    assert_eq!(assigned.len(), 2);
    assert_eq!(handle.count("items").unwrap(), 3);

    // Read
    let rows = handle.rows("items").unwrap();
    assert_eq!(rows.len(), 3);
    let target = rows
        .iter()
        .find(|r| r.columns.get(&1) == Some(&Value::Int64(2)))
        .expect("row 2");
    let row_id = target.row_id;

    // Update
    let post = handle
        .update("items", row_id, vec![(1, Value::Int64(20))])
        .unwrap();
    assert_eq!(
        post.columns
            .iter()
            .find(|(id, _)| *id == 1)
            .map(|(_, v)| v),
        Some(&Value::Int64(20))
    );
    assert!(handle
        .rows("items")
        .unwrap()
        .iter()
        .any(|r| r.columns.get(&1) == Some(&Value::Int64(20))));

    // Explicit multi-op transaction (session-like begin+ops).
    {
        let mut tx = handle.begin().unwrap();
        tx.put("items", vec![(1, Value::Int64(4))]).unwrap();
        tx.put_batch("items", vec![vec![(1, Value::Int64(5))]])
            .unwrap();
        let r4 = handle
            .rows("items")
            .unwrap()
            .into_iter()
            .find(|r| r.columns.get(&1) == Some(&Value::Int64(20)))
            .unwrap()
            .row_id;
        // Visible only after commit; update staged against currently visible row.
        tx.update("items", r4, vec![(1, Value::Int64(21))]).unwrap();
        tx.commit().unwrap();
    }
    assert_eq!(handle.count("items").unwrap(), 5);
    assert!(handle
        .rows("items")
        .unwrap()
        .iter()
        .any(|r| r.columns.get(&1) == Some(&Value::Int64(21))));

    // Delete
    let doomed = handle
        .rows("items")
        .unwrap()
        .into_iter()
        .find(|r| r.columns.get(&1) == Some(&Value::Int64(1)))
        .unwrap()
        .row_id;
    handle.delete("items", doomed).unwrap();
    assert_eq!(handle.count("items").unwrap(), 4);
}

/// P1.4-X5: Index DDL via AuthorizedMongrelSession / DatabaseHandle.
#[test]
fn p14_x5_index_ddl_via_authorized_session() {
    let dir = fresh_root();
    let handle = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    handle
        .create_table(
            "docs",
            Schema {
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
                        name: "category".into(),
                        ty: TypeId::Int64,
                        flags: ColumnFlags::empty(),
                        default_value: None,
                        embedding_source: None,
                    },
                ],
                ..Schema::default()
            },
        )
        .unwrap();
    handle
        .put("docs", vec![(1, Value::Int64(1)), (2, Value::Int64(10))])
        .unwrap();
    handle
        .put("docs", vec![(1, Value::Int64(2)), (2, Value::Int64(20))])
        .unwrap();

    let session = handle.session().unwrap();
    let definition = IndexDef {
        name: "idx_category_bm".into(),
        column_id: 2,
        kind: IndexKind::Bitmap,
        predicate: None,
        options: Default::default(),
    };
    let job_id = session.create_index("docs", definition).unwrap();
    assert!(job_id > 0);

    // Drop via the same authorized session surface.
    session.drop_index("docs", "idx_category_bm").unwrap();
    // Second drop fails closed (index gone).
    assert!(session.drop_index("docs", "idx_category_bm").is_err());

    // Read-only handle cannot run index DDL.
    let reader = DatabaseManager::global()
        .open_shared_with_access(
            dir.path(),
            OpenIdentity::Credentialless,
            HandleAccess::read_only(),
        )
        .unwrap();
    let read_session = reader.session().unwrap();
    assert!(matches!(
        read_session.create_index(
            "docs",
            IndexDef {
                name: "idx_ro".into(),
                column_id: 2,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            },
        ),
        Err(MongrelError::ReadOnlyHandle { .. })
    ));
    assert!(matches!(
        read_session.drop_index("docs", "idx_category_bm"),
        Err(MongrelError::ReadOnlyHandle { .. })
    ));
}

/// P1.4-X6: procedure and trigger behavior via AuthorizedMongrelSession.
#[test]
fn p14_x6_procedure_and_trigger_via_authorized_session() {
    use mongreldb_core::procedure::{
        ProcedureBody, ProcedureCallOutput, ProcedureMode, ProcedureStep, ProcedureValue,
        StoredProcedure,
    };
    use mongreldb_core::trigger::{
        StoredTrigger, TriggerDefinition, TriggerEvent, TriggerProgram, TriggerTarget,
        TriggerTiming,
    };

    let dir = fresh_root();
    let handle = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    handle
        .create_table(
            "users",
            Schema {
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
                        name: "status".into(),
                        ty: TypeId::Bytes,
                        flags: ColumnFlags::empty(),
                        default_value: None,
                        embedding_source: None,
                    },
                ],
                ..Schema::default()
            },
        )
        .unwrap();
    handle
        .put(
            "users",
            vec![
                (1, Value::Int64(1)),
                (2, Value::Bytes(b"active".to_vec())),
            ],
        )
        .unwrap();

    let session = handle.session().unwrap();
    let procedure = StoredProcedure::new(
        "read_users",
        ProcedureMode::ReadOnly,
        Vec::new(),
        ProcedureBody {
            steps: vec![ProcedureStep::NativeQuery {
                id: "q".into(),
                table: "users".into(),
                conditions: Vec::new(),
                projection: Some(vec![1, 2]),
                limit: None,
            }],
            return_value: ProcedureValue::StepRows("q".into()),
        },
        0,
    )
    .unwrap();
    session.create_procedure(procedure).unwrap();
    let result = session
        .call_procedure("read_users", std::collections::HashMap::new())
        .unwrap();
    let ProcedureCallOutput::Rows(rows) = result.output else {
        panic!("expected rows from procedure");
    };
    assert_eq!(rows.len(), 1);

    let trigger = StoredTrigger::new(
        "users_ai",
        TriggerDefinition {
            target: TriggerTarget::Table("users".into()),
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
    session.create_trigger(trigger).unwrap();
    session.drop_trigger("users_ai").unwrap();
    session.drop_procedure("read_users").unwrap();

    // Read-only denial on DDL-shaped procedure/trigger mutations.
    let reader = DatabaseManager::global()
        .open_shared_with_access(
            dir.path(),
            OpenIdentity::Credentialless,
            HandleAccess::read_only(),
        )
        .unwrap();
    let read_session = reader.session().unwrap();
    let proc = StoredProcedure::new(
        "ro_denied",
        ProcedureMode::ReadOnly,
        Vec::new(),
        ProcedureBody {
            steps: Vec::new(),
            return_value: ProcedureValue::Literal(Value::Null),
        },
        0,
    )
    .unwrap();
    assert!(matches!(
        read_session.create_procedure(proc),
        Err(MongrelError::ReadOnlyHandle { .. })
    ));
}

/// P1.4-X7: history and aggregate authorization via AuthorizedMongrelSession.
#[test]
fn p14_x7_history_and_aggregate_authorization() {
    use mongreldb_core::Snapshot;

    let dir = fresh_root();
    let handle = DatabaseManager::global()
        .open_shared(dir.path(), OpenIdentity::Credentialless)
        .unwrap();
    handle.create_table("items", id_schema()).unwrap();
    handle.put("items", vec![(1, Value::Int64(1))]).unwrap();
    handle.put("items", vec![(1, Value::Int64(2))]).unwrap();

    let session = handle.session().unwrap();
    // Aggregate count is principal-bound (same surface as count).
    assert_eq!(session.aggregate_count("items").unwrap(), 2);

    // History: unbounded snapshot sees current rows under authorization.
    let history = session
        .rows_at_epoch("items", Snapshot::unbounded())
        .unwrap();
    assert_eq!(history.len(), 2);

    // Read-only session can still read history/aggregates.
    let reader = DatabaseManager::global()
        .open_shared_with_access(
            dir.path(),
            OpenIdentity::Credentialless,
            HandleAccess::read_only(),
        )
        .unwrap();
    let read_session = reader.session().unwrap();
    assert_eq!(read_session.aggregate_count("items").unwrap(), 2);
    assert_eq!(
        read_session
            .rows_at_epoch("items", Snapshot::unbounded())
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn catalog_credentials_attach_and_revocation_is_live() {
    let dir = fresh_auth_root();
    let manager = DatabaseManager::global();
    assert!(matches!(
        manager.open_shared(
            dir.path(),
            OpenIdentity::CatalogCredentials {
                username: "alice".into(),
                password: SecretString::new("wrong"),
            },
        ),
        Err(MongrelError::InvalidCredentials { .. })
    ));
    let admin = manager
        .open_shared(
            dir.path(),
            OpenIdentity::CatalogCredentials {
                username: "admin".into(),
                password: SecretString::new("admin-password"),
            },
        )
        .unwrap();
    let alice = manager
        .open_shared(
            dir.path(),
            OpenIdentity::CatalogCredentials {
                username: "alice".into(),
                password: SecretString::new("user-password"),
            },
        )
        .unwrap();
    let bob = manager
        .open_shared(
            dir.path(),
            OpenIdentity::CatalogCredentials {
                username: "bob".into(),
                password: SecretString::new("user-password"),
            },
        )
        .unwrap();
    alice.put("items", vec![(1, Value::Int64(1))]).unwrap();
    admin.revoke_role("alice", "writer").unwrap();
    assert!(matches!(
        alice.put("items", vec![(1, Value::Int64(2))]),
        Err(MongrelError::PermissionDenied { .. })
    ));

    admin.drop_user("bob").unwrap();
    assert!(bob.count("items").is_err());
    assert_eq!(alice.count("items").unwrap(), 1);
}

#[test]
fn service_principal_scopes_are_enforced() {
    let dir = fresh_auth_root();
    let admin = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::CatalogCredentials {
                username: "admin".into(),
                password: SecretString::new("admin-password"),
            },
        )
        .unwrap();
    admin
        .register_service_principal(
            "worker-b",
            [9; 16],
            vec![
                Permission::Select {
                    table: "items".into(),
                },
                Permission::Insert {
                    table: "items".into(),
                },
            ],
            "service-secret",
            0,
        )
        .unwrap();
    let service = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::ServiceCredentials {
                token_id: "worker-b".into(),
                secret: SecretString::new("service-secret"),
            },
        )
        .unwrap();
    service.put("items", vec![(1, Value::Int64(7))]).unwrap();
    assert_eq!(service.count("items").unwrap(), 1);
    assert!(matches!(
        service.create_table("forbidden", id_schema()),
        Err(MongrelError::PermissionDenied { .. })
    ));
}

// ── P0.1 acceptance tests ────────────────────────────────────────────────

#[test]
fn p01_x1_caller_cannot_supply_admin_via_open_identity() {
    // OpenIdentity::ServiceCredentials carries only token_id + secret —
    // there is no public permission vector (the old ScopedServicePrincipal
    // gap). Compile-time shape is the authority boundary; runtime check:
    // wrong/secret-less attach never yields Admin.
    let dir = fresh_auth_root();
    let admin = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::CatalogCredentials {
                username: "admin".into(),
                password: SecretString::new("admin-password"),
            },
        )
        .unwrap();
    admin
        .register_service_principal(
            "no-admin",
            [1; 16],
            vec![Permission::Select {
                table: "items".into(),
            }],
            "service-secret",
            0,
        )
        .unwrap();
    let service = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::ServiceCredentials {
                token_id: "no-admin".into(),
                secret: SecretString::new("service-secret"),
            },
        )
        .unwrap();
    assert!(matches!(
        service.create_user("eve", "password"),
        Err(MongrelError::PermissionDenied { .. })
    ));
    assert!(matches!(
        service.register_service_principal("x", [2; 16], vec![Permission::Admin], "s", 0),
        Err(MongrelError::PermissionDenied { .. })
    ));
}

#[test]
fn p01_x2_wrong_service_secret_fails() {
    let dir = fresh_auth_root();
    let admin = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::CatalogCredentials {
                username: "admin".into(),
                password: SecretString::new("admin-password"),
            },
        )
        .unwrap();
    admin
        .register_service_principal(
            "worker-secret",
            [2; 16],
            vec![Permission::Select {
                table: "items".into(),
            }],
            "correct-secret",
            0,
        )
        .unwrap();
    assert!(matches!(
        DatabaseManager::global().open_shared(
            dir.path(),
            OpenIdentity::ServiceCredentials {
                token_id: "worker-secret".into(),
                secret: SecretString::new("wrong-secret"),
            },
        ),
        Err(MongrelError::InvalidCredentials { .. })
    ));
}

#[test]
fn p01_x3_expired_service_token_fails() {
    let dir = fresh_auth_root();
    let admin = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::CatalogCredentials {
                username: "admin".into(),
                password: SecretString::new("admin-password"),
            },
        )
        .unwrap();
    admin
        .register_service_principal(
            "expired-token",
            [3; 16],
            vec![Permission::Select {
                table: "items".into(),
            }],
            "service-secret",
            now_unix().saturating_sub(10),
        )
        .unwrap();
    assert!(matches!(
        DatabaseManager::global().open_shared(
            dir.path(),
            OpenIdentity::ServiceCredentials {
                token_id: "expired-token".into(),
                secret: SecretString::new("service-secret"),
            },
        ),
        Err(MongrelError::InvalidCredentials { .. })
    ));
}

#[test]
fn p01_x4_revoked_token_fails_on_existing_handle() {
    let dir = fresh_auth_root();
    let admin = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::CatalogCredentials {
                username: "admin".into(),
                password: SecretString::new("admin-password"),
            },
        )
        .unwrap();
    admin
        .register_service_principal(
            "revocable",
            [4; 16],
            vec![
                Permission::Select {
                    table: "items".into(),
                },
                Permission::Insert {
                    table: "items".into(),
                },
            ],
            "service-secret",
            0,
        )
        .unwrap();
    let service = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::ServiceCredentials {
                token_id: "revocable".into(),
                secret: SecretString::new("service-secret"),
            },
        )
        .unwrap();
    service.put("items", vec![(1, Value::Int64(1))]).unwrap();
    admin.revoke_service_principal("revocable").unwrap();
    assert!(matches!(
        service.put("items", vec![(1, Value::Int64(2))]),
        Err(MongrelError::InvalidCredentials { .. })
    ));
    assert!(matches!(
        service.count("items"),
        Err(MongrelError::InvalidCredentials { .. })
    ));
}

#[test]
fn p01_x5_scope_reduction_takes_effect_without_reopen() {
    let dir = fresh_auth_root();
    let admin = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::CatalogCredentials {
                username: "admin".into(),
                password: SecretString::new("admin-password"),
            },
        )
        .unwrap();
    admin
        .register_service_principal(
            "scoped",
            [5; 16],
            vec![
                Permission::Select {
                    table: "items".into(),
                },
                Permission::Insert {
                    table: "items".into(),
                },
            ],
            "service-secret",
            0,
        )
        .unwrap();
    let service = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::ServiceCredentials {
                token_id: "scoped".into(),
                secret: SecretString::new("service-secret"),
            },
        )
        .unwrap();
    service.put("items", vec![(1, Value::Int64(1))]).unwrap();
    admin
        .set_service_principal_permissions(
            "scoped",
            vec![Permission::Select {
                table: "items".into(),
            }],
        )
        .unwrap();
    assert!(matches!(
        service.put("items", vec![(1, Value::Int64(2))]),
        Err(MongrelError::PermissionDenied { .. })
    ));
    assert_eq!(service.count("items").unwrap(), 1);
}

#[test]
fn p01_x6_recreated_token_does_not_revive_old_handle() {
    let dir = fresh_auth_root();
    let admin = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::CatalogCredentials {
                username: "admin".into(),
                password: SecretString::new("admin-password"),
            },
        )
        .unwrap();
    let first = admin
        .register_service_principal(
            "recreate",
            [6; 16],
            vec![
                Permission::Select {
                    table: "items".into(),
                },
                Permission::Insert {
                    table: "items".into(),
                },
            ],
            "service-secret-v1",
            0,
        )
        .unwrap();
    let old_handle = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::ServiceCredentials {
                token_id: "recreate".into(),
                secret: SecretString::new("service-secret-v1"),
            },
        )
        .unwrap();
    assert_eq!(
        old_handle.identity(),
        &HandleIdentity::ServicePrincipal {
            token_id: "recreate".into(),
            principal_id: [6; 16],
            creation_version: first.creation_version,
        }
    );
    old_handle
        .put("items", vec![(1, Value::Int64(1))])
        .unwrap();
    admin.revoke_service_principal("recreate").unwrap();
    let second = admin
        .register_service_principal(
            "recreate",
            [6; 16],
            vec![
                Permission::Select {
                    table: "items".into(),
                },
                Permission::Insert {
                    table: "items".into(),
                },
            ],
            "service-secret-v2",
            0,
        )
        .unwrap();
    assert_ne!(first.creation_version, second.creation_version);
    // Old handle is pinned to the first creation_version and must not revive.
    assert!(matches!(
        old_handle.put("items", vec![(1, Value::Int64(2))]),
        Err(MongrelError::InvalidCredentials { .. })
    ));
    let new_handle = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::ServiceCredentials {
                token_id: "recreate".into(),
                secret: SecretString::new("service-secret-v2"),
            },
        )
        .unwrap();
    assert_eq!(
        new_handle.identity(),
        &HandleIdentity::ServicePrincipal {
            token_id: "recreate".into(),
            principal_id: [6; 16],
            creation_version: second.creation_version,
        }
    );
    new_handle
        .put("items", vec![(1, Value::Int64(3))])
        .unwrap();
}

#[test]
fn p01_x7_internal_service_capability_is_crate_private() {
    // InternalServiceCapability is `pub(crate)` and is not re-exported from
    // the crate root. Public OpenIdentity has no permission vector. This
    // test documents the boundary; compile-time privacy is the enforcement.
    let dir = fresh_auth_root();
    let admin = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::CatalogCredentials {
                username: "admin".into(),
                password: SecretString::new("admin-password"),
            },
        )
        .unwrap();
    // Public registration is the only way external code assigns scopes.
    let def = admin
        .register_service_principal(
            "public-only",
            [7; 16],
            vec![Permission::Select {
                table: "items".into(),
            }],
            "service-secret",
            0,
        )
        .unwrap();
    assert_eq!(def.token_id, "public-only");
    assert!(!def.permissions.is_empty());
}

#[test]
fn p01_x8_read_only_overrides_authenticated_write_permission() {
    let dir = fresh_auth_root();
    let admin = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::CatalogCredentials {
                username: "admin".into(),
                password: SecretString::new("admin-password"),
            },
        )
        .unwrap();
    admin
        .register_service_principal(
            "writer-ro",
            [8; 16],
            vec![
                Permission::Select {
                    table: "items".into(),
                },
                Permission::Insert {
                    table: "items".into(),
                },
            ],
            "service-secret",
            0,
        )
        .unwrap();
    // Seed a row via a read-write service handle.
    DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::ServiceCredentials {
                token_id: "writer-ro".into(),
                secret: SecretString::new("service-secret"),
            },
        )
        .unwrap()
        .put("items", vec![(1, Value::Int64(1))])
        .unwrap();
    let reader = DatabaseManager::global()
        .open_shared_with_access(
            dir.path(),
            OpenIdentity::ServiceCredentials {
                token_id: "writer-ro".into(),
                secret: SecretString::new("service-secret"),
            },
            HandleAccess::read_only(),
        )
        .unwrap();
    assert_eq!(reader.count("items").unwrap(), 1);
    assert!(matches!(
        reader.put("items", vec![(1, Value::Int64(2))]),
        Err(MongrelError::ReadOnlyHandle { .. })
    ));
}

#[test]
fn catalog_users_get_distinct_rls_results_from_one_core() {
    let dir = tempdir().unwrap();
    let database =
        Database::create_with_credentials(dir.path(), "admin", "admin-password").unwrap();
    database.create_table("docs", owner_schema()).unwrap();
    database
        .transaction(|transaction| {
            transaction.put(
                "docs",
                vec![(1, Value::Int64(1)), (2, Value::Bytes(b"alice".to_vec()))],
            )?;
            transaction.put(
                "docs",
                vec![(1, Value::Int64(2)), (2, Value::Bytes(b"bob".to_vec()))],
            )?;
            Ok(())
        })
        .unwrap();
    database.create_role("reader").unwrap();
    database
        .grant_permission(
            "reader",
            Permission::Select {
                table: "docs".into(),
            },
        )
        .unwrap();
    for username in ["alice", "bob"] {
        database.create_user(username, "user-password").unwrap();
        database.grant_role(username, "reader").unwrap();
    }
    database
        .set_security_catalog(SecurityCatalog {
            rls_tables: vec!["docs".into()],
            policies: vec![RowPolicy {
                name: "owner_only".into(),
                table: "docs".into(),
                command: PolicyCommand::All,
                subjects: vec!["public".into()],
                permissive: true,
                using: Some(SecurityExpr::ColumnEqCurrentUser { column: 2 }),
                with_check: Some(SecurityExpr::ColumnEqCurrentUser { column: 2 }),
            }],
            masks: Vec::new(),
        })
        .unwrap();
    assert_eq!(
        database
            .count_for("docs", database.principal().as_ref())
            .unwrap(),
        2
    );
    assert_eq!(
        database
            .rows_for("docs", database.resolve_principal("alice").as_ref())
            .unwrap()
            .len(),
        1
    );
    drop(database);

    let admin = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::CatalogCredentials {
                username: "admin".into(),
                password: SecretString::new("admin-password"),
            },
        )
        .unwrap();
    let attach = |username: &str| {
        DatabaseManager::global()
            .open_shared(
                dir.path(),
                OpenIdentity::CatalogCredentials {
                    username: username.into(),
                    password: SecretString::new("user-password"),
                },
            )
            .unwrap()
    };
    let alice = attach("alice");
    let bob = attach("bob");
    assert!(alice.shares_core_with(&bob));
    assert_eq!(admin.count("docs").unwrap(), 2);
    assert_eq!(alice.count("docs").unwrap(), 1);
    assert_eq!(bob.count("docs").unwrap(), 1);
    let alice_rows = alice.rows("docs").unwrap();
    let bob_rows = bob.rows("docs").unwrap();
    assert_eq!(alice_rows.len(), 1);
    assert_eq!(bob_rows.len(), 1);
    assert_eq!(alice_rows[0].columns.get(&1), Some(&Value::Int64(1)));
    assert_eq!(bob_rows[0].columns.get(&1), Some(&Value::Int64(2)));
}

#[test]
fn p14_session_returns_authorized_mongrel_session() {
    let dir = fresh_auth_root();
    let admin = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::CatalogCredentials {
                username: "admin".into(),
                password: SecretString::new("admin-password"),
            },
        )
        .unwrap();
    let session = admin.session().unwrap();
    session
        .put("items", vec![(1, Value::Int64(42))])
        .unwrap();
    assert_eq!(session.count("items").unwrap(), 1);
    let mut txn = session.begin().unwrap();
    txn.put("items", vec![(1, Value::Int64(43))]).unwrap();
    txn.commit().unwrap();
    assert_eq!(admin.count("items").unwrap(), 2);
}
