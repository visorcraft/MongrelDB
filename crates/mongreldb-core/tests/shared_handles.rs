//! Stage 1A (spec §10.1): shared storage cores behind lightweight handles.
//!
//! Covers S1A-001 (one `DatabaseCore`, many `DatabaseHandle`s), S1A-002
//! (process-local `DatabaseManager` registry, exactly-once initialization,
//! last-drop closes), S1A-003 (stable file identity), and S1A-004
//! (lifecycle: drain-on-shutdown, operation guards).

use mongreldb_core::auth::Permission;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{
    Database, DatabaseManager, HandleAccess, HandleIdentity, LifecycleState, MongrelError,
    OpenIdentity, PolicyCommand, RowPolicy, SecretString, SecurityCatalog, SecurityExpr, Value,
};
use std::time::Duration;
use tempfile::tempdir;

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
    let service = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::ServicePrincipal {
                principal_id: [7; 16],
            },
        )
        .unwrap();
    assert_eq!(anonymous.identity(), &HandleIdentity::Credentialless);
    assert_eq!(
        service.identity(),
        &HandleIdentity::ServicePrincipal {
            principal_id: [7; 16]
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
    let service = DatabaseManager::global()
        .open_shared(
            dir.path(),
            OpenIdentity::ScopedServicePrincipal {
                principal_id: [9; 16],
                permissions: vec![
                    Permission::Select {
                        table: "items".into(),
                    },
                    Permission::Insert {
                        table: "items".into(),
                    },
                ],
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
