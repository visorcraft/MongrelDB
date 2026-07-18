//! S1F-001 — versioned catalog command state machine (spec §10.6).
//!
//! Covers: deterministic application of every command variant, monotonic
//! version bumps, bounded history, restart reload of version + state through
//! the CATALOG checkpoint, fail-closed decoding (§4.10), command records
//! riding the `DdlOp::CatalogSnapshot` payload unchanged, pre-S1F-001 catalog
//! files opening with defaulted command state, and a differential check that
//! user/role/RLS mutations through commands produce the same catalog content
//! as the legacy `Database` path.

use mongreldb_core::catalog::{self, Catalog, MaterializedViewEntry};
use mongreldb_core::catalog_cmds::{
    decode_command, encode_command, CatalogCommand, CatalogCommandRecord, CatalogDelta,
    JobDefinition, JobKind, JobState, ResourceGroupDef, CATALOG_COMMAND_FORMAT_VERSION,
};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::trigger::{
    StoredTrigger, TriggerDefinition, TriggerEvent, TriggerProgram, TriggerTarget, TriggerTiming,
};
use mongreldb_core::{
    ColumnMask, DdlOp, MaskStrategy, Permission, PolicyCommand, RowPolicy, SecurityExpr,
    StoredProcedure, Value,
};
use mongreldb_core::{Database, MongrelError};
use tempfile::tempdir;

fn two_column_schema() -> Schema {
    Schema {
        schema_id: 0,
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
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn test_trigger(name: &str) -> StoredTrigger {
    StoredTrigger::new(
        name,
        TriggerDefinition {
            target: TriggerTarget::Table("t".into()),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            update_of: Vec::new(),
            target_columns: Vec::new(),
            when: None,
            program: TriggerProgram { steps: Vec::new() },
        },
        0,
    )
    .unwrap()
}

fn test_procedure(name: &str) -> StoredProcedure {
    StoredProcedure::new(
        name,
        mongreldb_core::ProcedureMode::ReadOnly,
        Vec::new(),
        mongreldb_core::procedure::ProcedureBody {
            steps: Vec::new(),
            return_value: mongreldb_core::procedure::ProcedureValue::Literal(Value::Null),
        },
        0,
    )
    .unwrap()
}

fn test_policy() -> RowPolicy {
    RowPolicy {
        name: "p".into(),
        table: "t".into(),
        command: PolicyCommand::Select,
        subjects: Vec::new(),
        permissive: true,
        using: Some(SecurityExpr::ColumnEqCurrentUser { column: 2 }),
        with_check: None,
    }
}

fn test_mask() -> ColumnMask {
    ColumnMask {
        name: "m".into(),
        table: "t".into(),
        column: 2,
        strategy: MaskStrategy::Sha256,
        exempt_subjects: Vec::new(),
    }
}

/// The full command surface, in an order that keeps every precondition valid.
/// Returns the sequence plus the version each command should assign.
fn full_command_sequence() -> Vec<CatalogCommand> {
    vec![
        CatalogCommand::CreateTable {
            name: "t".into(),
            schema: two_column_schema(),
            created_epoch: 1,
        },
        CatalogCommand::CreateTable {
            name: "mv".into(),
            schema: two_column_schema(),
            created_epoch: 1,
        },
        CatalogCommand::AddColumn {
            table: "t".into(),
            column: ColumnDef {
                id: 3,
                name: "extra".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        },
        CatalogCommand::AlterColumn {
            table: "t".into(),
            column: ColumnDef {
                id: 3,
                name: "extra".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
        },
        CatalogCommand::AddIndex {
            table: "t".into(),
            index: IndexDef {
                name: "idx_secret".into(),
                column_id: 2,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            },
        },
        CatalogCommand::CreateTrigger {
            trigger: test_trigger("trg"),
        },
        CatalogCommand::CreateProcedure {
            procedure: test_procedure("proc"),
        },
        CatalogCommand::CreateUser {
            username: "alice".into(),
            password_hash: "hash-a".into(),
            is_admin: false,
            created_epoch: 2,
        },
        CatalogCommand::CreateUser {
            username: "bob".into(),
            password_hash: "hash-b".into(),
            is_admin: false,
            created_epoch: 2,
        },
        CatalogCommand::CreateRole {
            name: "analyst".into(),
            created_epoch: 2,
        },
        CatalogCommand::GrantRole {
            username: "alice".into(),
            role: "analyst".into(),
        },
        CatalogCommand::GrantPermission {
            role: "analyst".into(),
            permission: Permission::Select { table: "t".into() },
        },
        CatalogCommand::EnableRls { table: "t".into() },
        CatalogCommand::SetRowPolicy {
            policy: test_policy(),
        },
        CatalogCommand::SetColumnMask { mask: test_mask() },
        CatalogCommand::CreateMaterializedView {
            definition: MaterializedViewEntry {
                name: "mv".into(),
                query: "SELECT id FROM t".into(),
                last_refresh_epoch: 0,
                incremental: None,
            },
        },
        CatalogCommand::RefreshMaterializedView {
            name: "mv".into(),
            at_epoch: 9,
            checkpoint_event_id: None,
        },
        CatalogCommand::SetResourceGroup {
            group: ResourceGroupDef {
                name: "rg".into(),
                max_concurrency: 4,
                max_queue: 64,
                memory_bytes: 1 << 20,
                temporary_disk_bytes: 1 << 30,
                work_units: 1,
                cpu_weight: 1,
                priority: 100,
                max_result_bytes: 1 << 30,
            },
        },
        CatalogCommand::SubmitJob {
            job: JobDefinition {
                job_id: 1,
                kind: JobKind::IndexBuild,
                state: JobState::Pending,
                target: Some("t".into()),
                created_epoch: 3,
                updated_epoch: 3,
            },
        },
        CatalogCommand::SetJobState {
            job_id: 1,
            state: JobState::Running,
            at_epoch: 4,
        },
        CatalogCommand::SetUserAdmin {
            username: "bob".into(),
            is_admin: true,
        },
        CatalogCommand::AlterUserPassword {
            username: "bob".into(),
            password_hash: "hash-b2".into(),
        },
        CatalogCommand::RenameTable {
            name: "t".into(),
            new_name: "t2".into(),
            at_epoch: 5,
        },
        // After the rename, table-scoped security state and role permissions
        // live on "t2" (the delta mirrors `Database::rename_table`).
        CatalogCommand::DisableRls { table: "t2".into() },
        CatalogCommand::DropRowPolicy {
            table: "t2".into(),
            name: "p".into(),
        },
        CatalogCommand::DropColumnMask {
            table: "t2".into(),
            name: "m".into(),
        },
        CatalogCommand::RemoveIndex {
            table: "t2".into(),
            name: "idx_secret".into(),
        },
        CatalogCommand::DropColumn {
            table: "t2".into(),
            column: "extra".into(),
        },
        CatalogCommand::DropTrigger { name: "trg".into() },
        CatalogCommand::DropProcedure {
            name: "proc".into(),
        },
        CatalogCommand::DropMaterializedView { name: "mv".into() },
        CatalogCommand::RevokePermission {
            role: "analyst".into(),
            permission: Permission::Select { table: "t2".into() },
        },
        CatalogCommand::RevokeRole {
            username: "alice".into(),
            role: "analyst".into(),
        },
        CatalogCommand::DropRole {
            name: "analyst".into(),
        },
        CatalogCommand::DropUser {
            username: "bob".into(),
        },
        CatalogCommand::RemoveResourceGroup { name: "rg".into() },
        CatalogCommand::DropTable {
            name: "t2".into(),
            at_epoch: 8,
        },
        CatalogCommand::DropTable {
            name: "mv".into(),
            at_epoch: 8,
        },
    ]
}

fn apply_sequence(catalog: &mut Catalog, commands: &[CatalogCommand]) {
    for command in commands {
        let record = CatalogCommandRecord::next(catalog, command.clone());
        catalog.apply_command(&record).unwrap();
    }
}

#[test]
fn every_command_applies_and_version_bumps_monotonically() {
    let commands = full_command_sequence();
    let mut catalog = Catalog::empty();
    let mut expected_version = 0_u64;
    for command in &commands {
        let record = CatalogCommandRecord::next(&catalog, command.clone());
        assert_eq!(record.catalog_version, expected_version + 1);
        assert_eq!(record.version, CATALOG_COMMAND_FORMAT_VERSION);
        catalog.apply_command(&record).unwrap();
        expected_version += 1;
        assert_eq!(catalog.catalog_version(), expected_version);
    }
    assert_eq!(catalog.commands_since(0).len(), commands.len());

    // Final state spot checks across the whole surface.
    assert!(catalog.live("t2").is_none());
    assert!(matches!(
        catalog.tables[0].state,
        catalog::TableState::Dropped { at_epoch: 8 }
    ));
    assert!(catalog.triggers.is_empty());
    assert!(catalog.procedures.is_empty());
    assert!(catalog.materialized_views.is_empty());
    assert!(catalog.security.policies.is_empty());
    assert!(catalog.security.masks.is_empty());
    assert!(!catalog.security.rls_enabled("t"));
    assert!(catalog.roles.is_empty());
    assert_eq!(catalog.users.len(), 1);
    assert_eq!(catalog.users[0].username, "alice");
    assert!(catalog.resource_groups.is_empty());
    assert_eq!(catalog.job_definitions.len(), 1);
    assert_eq!(catalog.job_definitions[0].state, JobState::Running);
    // security bumps: enable RLS + policy + mask + disable + drop policy +
    // drop mask + user/role/grant/revoke mutations.
    assert!(catalog.security_version > 0);
}

#[test]
fn application_is_deterministic_across_catalogs() {
    let commands = full_command_sequence();
    let mut first = Catalog::empty();
    let mut second = Catalog::empty();
    apply_sequence(&mut first, &commands);
    apply_sequence(&mut second, &commands);

    // Byte-identical checkpoints prove the resolved state (command history
    // included) is a pure function of the command sequence.
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    catalog::write_atomic(dir_a.path(), &first, None).unwrap();
    catalog::write_atomic(dir_b.path(), &second, None).unwrap();
    let bytes_a = std::fs::read(dir_a.path().join(catalog::CATALOG_FILENAME)).unwrap();
    let bytes_b = std::fs::read(dir_b.path().join(catalog::CATALOG_FILENAME)).unwrap();
    assert_eq!(bytes_a, bytes_b);
}

#[test]
fn restart_reloads_version_and_state_identically() {
    let dir = tempdir().unwrap();
    let commands = vec![
        CatalogCommand::CreateTable {
            name: "t".into(),
            schema: two_column_schema(),
            created_epoch: 1,
        },
        CatalogCommand::CreateUser {
            username: "alice".into(),
            password_hash: "hash-a".into(),
            is_admin: true,
            created_epoch: 2,
        },
        CatalogCommand::CreateRole {
            name: "analyst".into(),
            created_epoch: 2,
        },
        CatalogCommand::GrantRole {
            username: "alice".into(),
            role: "analyst".into(),
        },
        CatalogCommand::EnableRls { table: "t".into() },
        CatalogCommand::SetRowPolicy {
            policy: test_policy(),
        },
        CatalogCommand::SetColumnMask { mask: test_mask() },
    ];
    let mut catalog = Catalog::empty();
    for command in &commands {
        let record = CatalogCommandRecord::next(&catalog, command.clone());
        catalog
            .apply_command_and_checkpoint(dir.path(), None, &record)
            .unwrap();
    }

    // "Restart": reload the checkpoint from disk.
    let reloaded = catalog::read(dir.path(), None).unwrap().unwrap();
    assert_eq!(reloaded.catalog_version(), commands.len() as u64);
    assert_eq!(reloaded.commands_since(0).len(), commands.len());
    assert!(reloaded.live("t").is_some());
    assert_eq!(reloaded.users.len(), 1);
    assert_eq!(reloaded.users[0].roles, vec!["analyst".to_string()]);
    assert_eq!(reloaded.roles.len(), 1);
    assert_eq!(reloaded.security, catalog.security);
    assert_eq!(reloaded.security_version, catalog.security_version);
    // The retained history survives the round trip byte-for-byte.
    for (reloaded_record, record) in reloaded.command_log.iter().zip(catalog.command_log.iter()) {
        assert_eq!(
            encode_command(reloaded_record).unwrap(),
            encode_command(record).unwrap()
        );
    }
}

#[test]
fn unknown_version_decode_fails_closed() {
    let mut catalog = Catalog::empty();
    let record =
        CatalogCommandRecord::next(&catalog, CatalogCommand::EnableRls { table: "t".into() });
    let bytes = encode_command(&record).unwrap();
    let mut json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    json["version"] = serde_json::json!(CATALOG_COMMAND_FORMAT_VERSION + 1);
    let bumped = serde_json::to_vec(&json).unwrap();
    assert!(matches!(
        decode_command(&bumped),
        Err(MongrelError::UnsupportedStorageVersion { .. })
    ));
    // Unknown command variants and unknown record fields fail closed too.
    assert!(decode_command(
        br#"{"version":1,"catalog_version":1,"command":{"DropEverything":{}}}"#
    )
    .is_err());
    assert!(decode_command(
        br#"{"version":1,"catalog_version":1,"command":{"EnableRls":{"table":"t"}},"extra":true}"#
    )
    .is_err());
    // And a record carrying an unsupported version is rejected on apply.
    let bad = CatalogCommandRecord {
        version: CATALOG_COMMAND_FORMAT_VERSION + 1,
        catalog_version: 1,
        command: CatalogCommand::EnableRls { table: "t".into() },
    };
    assert!(matches!(
        catalog.apply_command(&bad),
        Err(MongrelError::UnsupportedStorageVersion { .. })
    ));
    assert_eq!(catalog.catalog_version(), 0);
}

#[test]
fn command_records_ride_the_catalog_snapshot_payload() {
    // The WAL `DdlOp::CatalogSnapshot` payload is `DdlOp::encode_catalog` —
    // unchanged on disk. Command records and the version ride inside it.
    let mut catalog = Catalog::empty();
    apply_sequence(&mut catalog, &full_command_sequence()[..11]);
    let payload = DdlOp::encode_catalog(&catalog).unwrap();
    let recovered = DdlOp::decode_catalog(&payload).unwrap();
    assert_eq!(recovered.catalog_version(), 11);
    assert_eq!(recovered.commands_since(0).len(), 11);
    assert!(recovered.live("t").is_some());
    assert_eq!(recovered.triggers.len(), 1);
    assert_eq!(recovered.procedures.len(), 1);
    assert_eq!(recovered.users.len(), 2);
    assert_eq!(recovered.roles.len(), 1);
}

#[test]
fn legacy_catalog_without_command_fields_opens_with_defaults() {
    // Simulate a pre-S1F-001 CATALOG body: strip every state-machine field
    // from an encoded snapshot. Decode must fill defaults (§4.10 migration),
    // keeping current database files readable unchanged.
    let mut catalog = Catalog::empty();
    apply_sequence(&mut catalog, &full_command_sequence()[..2]);
    let payload = DdlOp::encode_catalog(&catalog).unwrap();
    let mut json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    let body = json["catalog"].as_object_mut().unwrap();
    body.remove("catalog_version");
    body.remove("command_log");
    body.remove("resource_groups");
    body.remove("job_definitions");
    let legacy = serde_json::to_vec(&json).unwrap();
    let opened = DdlOp::decode_catalog(&legacy).unwrap();
    assert_eq!(opened.catalog_version(), 0);
    assert!(opened.commands_since(0).is_empty());
    assert!(opened.resource_groups.is_empty());
    assert!(opened.job_definitions.is_empty());
    assert!(opened.live("t").is_some());
}

#[test]
fn replay_is_idempotent_and_conflicting_replay_fails() {
    let mut catalog = Catalog::empty();
    apply_sequence(&mut catalog, &full_command_sequence()[..4]);
    let recorded = catalog.commands_since(0);
    // Re-applying the recorded tail in order is a sequence of no-ops.
    for record in &recorded {
        let delta = catalog.apply_command(record).unwrap();
        assert!(matches!(delta, CatalogDelta::NoOp));
    }
    assert_eq!(catalog.catalog_version(), 4);
    // A different command claiming version 3 conflicts.
    let conflicting = CatalogCommandRecord {
        version: CATALOG_COMMAND_FORMAT_VERSION,
        catalog_version: 3,
        command: CatalogCommand::DropUser {
            username: "nobody".into(),
        },
    };
    assert!(matches!(
        catalog.apply_command(&conflicting),
        Err(MongrelError::Conflict(_))
    ));
    // A gap (version 6 with 4 applied) conflicts.
    let gap = CatalogCommandRecord {
        version: CATALOG_COMMAND_FORMAT_VERSION,
        catalog_version: 6,
        command: CatalogCommand::DisableRls { table: "t".into() },
    };
    assert!(matches!(
        catalog.apply_command(&gap),
        Err(MongrelError::Conflict(_))
    ));
}

#[test]
fn users_roles_rls_commands_match_legacy_path() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", two_column_schema()).unwrap();

    // Base state shared by both paths: the catalog right after CREATE TABLE,
    // which is itself one recorded command on the routed path (S1F-001).
    let base = db.catalog_snapshot();
    assert_eq!(base.catalog_version(), 1);

    // Legacy path through the Database entry points.
    db.create_user_with_password_hash("alice", "hash-a".into())
        .unwrap();
    db.create_user_with_password_hash("bob", "hash-b".into())
        .unwrap();
    db.create_role("analyst").unwrap();
    db.grant_role("alice", "analyst").unwrap();
    db.grant_permission("analyst", Permission::Select { table: "t".into() })
        .unwrap();
    db.grant_permission(
        "analyst",
        Permission::SelectColumns {
            table: "t".into(),
            columns: vec!["secret".into()],
        },
    )
    .unwrap();
    let mut security = db.security_catalog();
    security.rls_tables.push("t".into());
    security.policies.push(test_policy());
    security.masks.push(test_mask());
    db.set_security_catalog(security).unwrap();

    // Command path from the same base catalog.
    let base_security_version = base.security_version;
    let mut commanded = base;
    let commands = vec![
        CatalogCommand::CreateUser {
            username: "alice".into(),
            password_hash: "hash-a".into(),
            is_admin: false,
            created_epoch: 0,
        },
        CatalogCommand::CreateUser {
            username: "bob".into(),
            password_hash: "hash-b".into(),
            is_admin: false,
            created_epoch: 0,
        },
        CatalogCommand::CreateRole {
            name: "analyst".into(),
            created_epoch: 0,
        },
        CatalogCommand::GrantRole {
            username: "alice".into(),
            role: "analyst".into(),
        },
        CatalogCommand::GrantPermission {
            role: "analyst".into(),
            permission: Permission::Select { table: "t".into() },
        },
        CatalogCommand::GrantPermission {
            role: "analyst".into(),
            permission: Permission::SelectColumns {
                table: "t".into(),
                columns: vec!["secret".into()],
            },
        },
        CatalogCommand::EnableRls { table: "t".into() },
        CatalogCommand::SetRowPolicy {
            policy: test_policy(),
        },
        CatalogCommand::SetColumnMask { mask: test_mask() },
    ];
    apply_sequence(&mut commanded, &commands);

    // Users match on stable identity fields (engine-assigned epochs differ by
    // construction; ids and content match exactly).
    let legacy_users = db.users();
    assert_eq!(legacy_users.len(), commanded.users.len());
    for (legacy, commanded_user) in legacy_users.iter().zip(commanded.users.iter()) {
        assert_eq!(legacy.id, commanded_user.id);
        assert_eq!(legacy.username, commanded_user.username);
        assert_eq!(legacy.password_hash, commanded_user.password_hash);
        assert_eq!(legacy.roles, commanded_user.roles);
        assert_eq!(legacy.is_admin, commanded_user.is_admin);
    }

    // Roles match on name + merged permissions.
    let legacy_roles = db.roles();
    assert_eq!(legacy_roles.len(), commanded.roles.len());
    for (legacy, commanded_role) in legacy_roles.iter().zip(commanded.roles.iter()) {
        assert_eq!(legacy.name, commanded_role.name);
        assert_eq!(legacy.permissions, commanded_role.permissions);
    }

    // RLS/mask content matches the wholesale legacy replacement.
    assert_eq!(db.security_catalog(), commanded.security);

    // security_version counts differ by construction (the legacy path bumps
    // once per API call; the command path bumps once per command) — both
    // advanced past the base, which is the observable contract.
    assert!(db.security_version() > base_security_version);
    assert!(commanded.security_version > base_security_version);
}
