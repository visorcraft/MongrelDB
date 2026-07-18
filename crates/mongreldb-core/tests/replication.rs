use mongreldb_core::procedure::{
    ProcedureBody, ProcedureMode, ProcedureStep, ProcedureValue, StoredProcedure,
};
use mongreldb_core::schema::{AlterColumn, ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{
    write_replica_epoch, Database, ExternalTableDefinition, ExternalTableEntry, ModuleArg,
    ModuleCapabilities, MongrelError, Snapshot, StoredTrigger, TriggerCell, TriggerDefinition,
    TriggerEvent, TriggerProgram, TriggerStep, TriggerTarget, TriggerTiming, TriggerValue, Value,
};
use std::sync::{Arc, Barrier};

fn schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
            embedding_source: None,
        }],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn put(db: &Database, id: i64) {
    db.transaction(|txn| {
        txn.put("items", vec![(1, Value::Int64(id))])?;
        Ok(())
    })
    .unwrap();
}

fn metadata_schema() -> Schema {
    Schema {
        schema_id: 7,
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
                name: "note".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn sample_procedure() -> StoredProcedure {
    StoredProcedure::new(
        "read_renamed",
        ProcedureMode::ReadOnly,
        Vec::new(),
        ProcedureBody {
            steps: vec![ProcedureStep::NativeQuery {
                id: "read".into(),
                table: "renamed".into(),
                conditions: Vec::new(),
                projection: Some(vec![1, 2]),
                limit: Some(10),
            }],
            return_value: ProcedureValue::StepRows("read".into()),
        },
        0,
    )
    .unwrap()
}

fn sample_trigger() -> StoredTrigger {
    StoredTrigger::new(
        "fill_note",
        TriggerDefinition {
            target: TriggerTarget::Table("renamed".into()),
            timing: TriggerTiming::Before,
            event: TriggerEvent::Insert,
            update_of: Vec::new(),
            target_columns: Vec::new(),
            when: None,
            program: TriggerProgram {
                steps: vec![TriggerStep::SetNew {
                    cells: vec![TriggerCell {
                        column_id: 2,
                        value: TriggerValue::Literal(Value::Bytes(b"replicated".to_vec())),
                    }],
                }],
            },
        },
        0,
    )
    .unwrap()
}

fn external_entry(name: &str) -> ExternalTableEntry {
    ExternalTableEntry::new(
        name,
        ExternalTableDefinition {
            module: "series".into(),
            args: vec![ModuleArg::Number("3".into())],
            declared_schema: schema(),
            hidden_columns: Vec::new(),
            options: Default::default(),
            capabilities: ModuleCapabilities {
                read_only: true,
                deterministic: true,
                ..ModuleCapabilities::default()
            },
        },
        0,
    )
    .unwrap()
}

fn external_state(root: &std::path::Path, name: &str) -> Vec<u8> {
    std::fs::read(root.join("_vtab").join(name).join("state.json")).unwrap()
}

#[test]
fn bootstrap_incremental_apply_and_read_only_enforcement() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");
    let leader = Database::create(&leader_path).unwrap();
    leader.create_table("items", schema()).unwrap();
    put(&leader, 1);

    let snapshot = leader.replication_snapshot().unwrap();
    snapshot.install(&follower_path).unwrap();
    assert_eq!(
        mongreldb_core::replica_epoch(&follower_path).unwrap(),
        snapshot.epoch()
    );

    let follower = Database::open(&follower_path).unwrap();
    assert!(follower.is_read_only_replica());
    assert!(matches!(
        follower.create_table("blocked", schema()),
        Err(MongrelError::ReadOnlyReplica)
    ));
    let table = follower.table("items").unwrap();
    assert!(matches!(
        table.lock().put(vec![(1, Value::Int64(9))]),
        Err(MongrelError::ReadOnlyReplica)
    ));
    drop(follower);

    put(&leader, 2);
    leader.create_table("extra", schema()).unwrap();
    leader
        .transaction(|txn| {
            txn.put("extra", vec![(1, Value::Int64(7))])?;
            Ok(())
        })
        .unwrap();
    let batch = leader.replication_batch_since(snapshot.epoch()).unwrap();
    assert!(!batch.requires_snapshot);
    assert!(!batch.records.is_empty());

    let follower = Database::open(&follower_path).unwrap();
    let applied_epoch = follower.append_replication_batch(&batch).unwrap();
    let durable_records = mongreldb_core::SharedWal::replay(&follower_path)
        .unwrap()
        .len();
    assert_eq!(
        follower.append_replication_batch(&batch).unwrap(),
        applied_epoch
    );
    assert_eq!(
        mongreldb_core::SharedWal::replay(&follower_path)
            .unwrap()
            .len(),
        durable_records,
        "retry must not duplicate a durable remote commit"
    );
    drop(follower);

    let follower = Database::open(&follower_path).unwrap();
    assert!(follower.visible_epoch().0 >= applied_epoch);
    let table = follower.table("items").unwrap();
    let rows = table
        .lock()
        .visible_rows(Snapshot::at(follower.visible_epoch()))
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(table.lock().count(), 2);
    assert_eq!(follower.table("extra").unwrap().lock().count(), 1);
    drop(follower);
    write_replica_epoch(&follower_path, applied_epoch).unwrap();
}

#[test]
fn retention_gap_and_spilled_run_require_bootstrap() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    let before = db.visible_epoch().0;
    db.set_spill_threshold(1);
    put(&db, 1);
    assert!(
        db.replication_batch_since(before)
            .unwrap()
            .requires_snapshot
    );

    db.checkpoint().unwrap();
    assert!(db.replication_batch_since(0).unwrap().requires_snapshot);
}

#[test]
fn spilled_snapshot_survives_repeated_replica_open() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");
    let leader = Database::create(&leader_path).unwrap();
    leader.create_table("items", schema()).unwrap();
    put(&leader, 1);
    put(&leader, 2);
    leader.set_spill_threshold(1);
    put(&leader, 3);

    leader
        .replication_snapshot()
        .unwrap()
        .install(&follower_path)
        .unwrap();

    for _ in 0..2 {
        let follower = Database::open(&follower_path).unwrap();
        assert_eq!(follower.table("items").unwrap().lock().count(), 3);
    }
}

#[test]
fn batch_proof_rejects_an_omitted_committed_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("proof-leader");
    let follower_path = dir.path().join("proof-follower");
    let leader = Database::create(&leader_path).unwrap();
    leader.create_table("items", schema()).unwrap();
    let snapshot = leader.replication_snapshot().unwrap();
    snapshot.install(&follower_path).unwrap();
    put(&leader, 1);
    put(&leader, 2);
    put(&leader, 3);

    let batch = leader.replication_batch_since(snapshot.epoch()).unwrap();
    let mut commits = batch
        .records
        .iter()
        .filter_map(|record| match record.op {
            mongreldb_core::wal::Op::TxnCommit { epoch, .. } => Some((epoch, record.txn_id)),
            _ => None,
        })
        .collect::<Vec<_>>();
    commits.sort_unstable();
    let omitted_txn = commits[1].1;
    let mut tampered = batch.clone();
    tampered
        .records
        .retain(|record| record.txn_id != omitted_txn);

    let follower = Database::open(&follower_path).unwrap();
    let error = follower.append_replication_batch(&tampered).unwrap_err();
    assert!(matches!(error, MongrelError::InvalidArgument(_)));
    assert!(error.to_string().contains("commit count mismatch"));
    assert_eq!(
        mongreldb_core::replica_epoch(&follower_path).unwrap(),
        snapshot.epoch()
    );
}

#[test]
fn abandoned_epoch_gap_is_a_complete_incremental_batch() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("gap-leader");
    let follower_path = dir.path().join("gap-follower");
    let leader = Database::create(&leader_path).unwrap();
    leader.create_table("items", schema()).unwrap();
    let snapshot = leader.replication_snapshot().unwrap();
    snapshot.install(&follower_path).unwrap();

    let mut invalid = schema();
    invalid.columns[0].flags = ColumnFlags::empty().with(ColumnFlags::AUTO_INCREMENT);
    assert!(leader.create_table("invalid", invalid).is_err());
    put(&leader, 1);
    let batch = leader.replication_batch_since(snapshot.epoch()).unwrap();
    assert!(batch.current_epoch > snapshot.epoch().saturating_add(1));
    assert!(!batch.requires_snapshot);

    let follower = Database::open(&follower_path).unwrap();
    let applied = follower.append_replication_batch(&batch).unwrap();
    assert_eq!(applied, batch.current_epoch);
}

#[test]
fn auth_enable_applies_to_the_live_follower_handle() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("live-auth-leader");
    let follower_path = dir.path().join("live-auth-follower");
    let leader = Database::create(&leader_path).unwrap();
    leader.create_table("items", schema()).unwrap();
    put(&leader, 1);
    let snapshot = leader.replication_snapshot().unwrap();
    snapshot.install(&follower_path).unwrap();
    leader.enable_auth("admin", "password").unwrap();
    let batch = leader.replication_batch_since(snapshot.epoch()).unwrap();

    let follower = Database::open(&follower_path).unwrap();
    let mounted_before_apply = follower.table("items").unwrap();
    follower.append_replication_batch(&batch).unwrap();
    assert!(follower.require_auth_enabled());
    assert!(matches!(
        mounted_before_apply
            .lock()
            .query(&mongreldb_core::Query::new()),
        Err(MongrelError::AuthRequired)
    ));
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_replica_bootstrap_and_incremental_apply() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("encrypted-leader");
    let follower_path = dir.path().join("encrypted-follower");
    let leader = Database::create_encrypted(&leader_path, "secret").unwrap();
    leader.create_table("items", schema()).unwrap();
    put(&leader, 1);
    let snapshot = leader.replication_snapshot().unwrap();
    snapshot
        .install_validated(&follower_path, |stage| {
            drop(Database::open_encrypted(stage, "secret")?);
            Ok(())
        })
        .unwrap();

    put(&leader, 2);
    let batch = leader.replication_batch_since(snapshot.epoch()).unwrap();
    let follower = Database::open_encrypted(&follower_path, "secret").unwrap();
    let epoch = follower.append_replication_batch(&batch).unwrap();
    drop(follower);
    let follower = Database::open_encrypted(&follower_path, "secret").unwrap();
    assert!(follower.visible_epoch().0 >= epoch);
    assert_eq!(follower.table("items").unwrap().lock().count(), 2);
}

#[test]
fn authenticated_replica_reopens_with_copied_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("auth-leader");
    let follower_path = dir.path().join("auth-follower");
    let leader = Database::create_with_credentials(&leader_path, "admin", "pw").unwrap();
    leader.create_table("items", schema()).unwrap();
    put(&leader, 1);
    leader
        .replication_snapshot()
        .unwrap()
        .install_validated(&follower_path, |stage| {
            drop(Database::open_with_credentials(stage, "admin", "pw")?);
            Ok(())
        })
        .unwrap();

    assert!(matches!(
        Database::open(&follower_path),
        Err(MongrelError::AuthRequired)
    ));
    let follower = Database::open_with_credentials(&follower_path, "admin", "pw").unwrap();
    assert_eq!(follower.table("items").unwrap().lock().count(), 1);
}

#[test]
fn replication_wal_export_rechecks_exact_admin_after_wait() {
    let dir = tempfile::tempdir().unwrap();
    let leader = Arc::new(
        Database::create_with_credentials(dir.path().join("leader"), "admin", "admin-pw").unwrap(),
    );
    leader.create_table("items", schema()).unwrap();
    put(&leader, 1);
    leader.create_user("rescue", "rescue-password").unwrap();
    leader.set_user_admin("rescue", true).unwrap();
    let entered = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    leader.__set_replication_hook({
        let entered = Arc::clone(&entered);
        let resume = Arc::clone(&resume);
        move || {
            entered.wait();
            resume.wait();
        }
    });
    let worker = {
        let leader = Arc::clone(&leader);
        std::thread::spawn(move || leader.replication_batch_since(0))
    };
    entered.wait();
    leader.drop_user("admin").unwrap();
    resume.wait();

    assert!(matches!(
        worker.join().unwrap(),
        Err(MongrelError::AuthRequired)
    ));
}

#[test]
fn incremental_apply_replays_exact_catalog_and_external_generation() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("metadata-leader");
    let follower_path = dir.path().join("metadata-follower");
    let leader = Database::create(&leader_path).unwrap();
    leader.create_table("items", metadata_schema()).unwrap();
    leader.create_table("discarded", schema()).unwrap();
    let snapshot = leader.replication_snapshot().unwrap();
    snapshot.install(&follower_path).unwrap();

    leader.rename_table("items", "renamed").unwrap();
    leader
        .alter_column("renamed", "note", AlterColumn::rename("label"))
        .unwrap();
    leader.drop_table("discarded").unwrap();
    leader.create_procedure(sample_procedure()).unwrap();
    leader.create_trigger(sample_trigger()).unwrap();
    leader.create_external_table(external_entry("ext")).unwrap();
    leader
        .commit_external_table_state("ext", b"old-state")
        .unwrap();
    leader.drop_external_table("ext").unwrap();
    leader.create_external_table(external_entry("ext")).unwrap();
    leader
        .commit_external_table_state("ext", b"new-state")
        .unwrap();
    leader
        .set_sql_pragma_i64_with_epoch("user_version", 42)
        .unwrap();
    leader.create_user("worker", "old-password").unwrap();
    leader.create_role("reader").unwrap();
    leader.grant_role("worker", "reader").unwrap();
    leader
        .grant_permission(
            "reader",
            mongreldb_core::auth::Permission::Select {
                table: "renamed".into(),
            },
        )
        .unwrap();
    leader
        .alter_user_password("worker", "new-password")
        .unwrap();
    leader.enable_auth("admin", "admin-password").unwrap();

    let batch = leader.replication_batch_since(snapshot.epoch()).unwrap();
    assert!(!batch.requires_snapshot);
    let follower = Database::open(&follower_path).unwrap();
    let applied = follower.append_replication_batch(&batch).unwrap();
    assert_eq!(applied, leader.visible_epoch().0);
    drop(follower);

    let follower =
        Database::open_with_credentials(&follower_path, "admin", "admin-password").unwrap();
    assert_eq!(
        serde_json::to_value(follower.catalog_snapshot()).unwrap(),
        serde_json::to_value(leader.catalog_snapshot()).unwrap()
    );
    assert_eq!(external_state(&follower_path, "ext"), b"new-state");
    assert!(follower.table("items").is_err());
    assert!(follower.table("renamed").is_ok());
    assert!(follower.table("discarded").is_err());
    assert!(follower.procedure("read_renamed").is_some());
    assert!(follower.trigger("fill_note").is_some());
    assert_eq!(follower.sql_pragma_i64("user_version").unwrap(), Some(42));
    drop(follower);
    let follower =
        Database::open_with_credentials(&follower_path, "admin", "admin-password").unwrap();
    assert_eq!(
        serde_json::to_value(follower.catalog_snapshot()).unwrap(),
        serde_json::to_value(leader.catalog_snapshot()).unwrap(),
        "catalog recovery must be idempotent across repeated opens"
    );
}

#[test]
fn replica_auth_password_and_disable_transitions_choose_correct_open_mode() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("auth-transition-leader");
    let follower_path = dir.path().join("auth-transition-follower");
    let leader = Database::create_with_credentials(&leader_path, "admin", "first").unwrap();
    leader.create_table("items", schema()).unwrap();
    let snapshot = leader.replication_snapshot().unwrap();
    snapshot
        .install_validated(&follower_path, |stage| {
            drop(Database::open_with_credentials(stage, "admin", "first")?);
            Ok(())
        })
        .unwrap();

    leader.alter_user_password("admin", "second").unwrap();
    let password_batch = leader.replication_batch_since(snapshot.epoch()).unwrap();
    let follower = Database::open_with_credentials(&follower_path, "admin", "first").unwrap();
    let password_epoch = follower.append_replication_batch(&password_batch).unwrap();
    drop(follower);
    assert!(Database::open_with_credentials(&follower_path, "admin", "first").is_err());
    let follower = Database::open_with_credentials(&follower_path, "admin", "second").unwrap();
    assert_eq!(
        mongreldb_core::replica_epoch(&follower_path).unwrap(),
        password_epoch
    );

    leader.disable_auth().unwrap();
    let disable_batch = leader.replication_batch_since(password_epoch).unwrap();
    follower.append_replication_batch(&disable_batch).unwrap();
    drop(follower);
    let follower = Database::open(&follower_path).unwrap();
    assert!(!follower.require_auth_enabled());
    assert_eq!(
        serde_json::to_value(follower.catalog_snapshot()).unwrap(),
        serde_json::to_value(leader.catalog_snapshot()).unwrap()
    );
}

#[test]
fn doctor_drop_replicates_after_bootstrap() {
    let dir = tempfile::tempdir().unwrap();
    let leader_path = dir.path().join("doctor-leader");
    let follower_path = dir.path().join("doctor-follower");
    let leader = Database::create(&leader_path).unwrap();
    leader.create_table("broken", schema()).unwrap();
    leader.create_table("healthy", schema()).unwrap();
    leader
        .table("broken")
        .unwrap()
        .lock()
        .set_mutable_run_spill_bytes(1);
    put_named(&leader, "broken", 1);
    leader.table("broken").unwrap().lock().flush().unwrap();
    let snapshot = leader.replication_snapshot().unwrap();
    snapshot.install(&follower_path).unwrap();

    let table_id = leader.table_id("broken").unwrap();
    let run = std::fs::read_dir(
        leader_path
            .join("tables")
            .join(table_id.to_string())
            .join("_runs"),
    )
    .unwrap()
    .filter_map(|entry| entry.ok())
    .map(|entry| entry.path())
    .find(|path| path.extension().and_then(|value| value.to_str()) == Some("sr"))
    .unwrap();
    std::fs::remove_file(run).unwrap();
    assert!(leader.doctor().unwrap().contains(&table_id));

    let batch = leader.replication_batch_since(snapshot.epoch()).unwrap();
    let follower = Database::open(&follower_path).unwrap();
    follower.append_replication_batch(&batch).unwrap();
    drop(follower);
    let follower = Database::open(&follower_path).unwrap();
    assert!(follower.table("broken").is_err());
    assert!(follower.table("healthy").is_ok());
}

#[test]
fn foreign_snapshot_and_wal_cannot_replace_or_advance_a_replica() {
    let dir = tempfile::tempdir().unwrap();
    let leader_a_path = dir.path().join("leader-a");
    let leader_b_path = dir.path().join("leader-b");
    let follower_path = dir.path().join("follower");

    let leader_a = Database::create(&leader_a_path).unwrap();
    leader_a.create_table("items", schema()).unwrap();
    put(&leader_a, 1);
    let snapshot_a = leader_a.replication_snapshot().unwrap();
    snapshot_a.install(&follower_path).unwrap();
    let follower_epoch = mongreldb_core::replica_epoch(&follower_path).unwrap();

    let leader_b = Database::create(&leader_b_path).unwrap();
    leader_b.create_table("items", schema()).unwrap();
    put(&leader_b, 99);
    let snapshot_b = leader_b.replication_snapshot().unwrap();
    let error = snapshot_b.install(&follower_path).unwrap_err();
    assert!(error.to_string().contains("source does not match"));

    put(&leader_b, 100);
    let batch_b = leader_b
        .replication_batch_since(snapshot_b.epoch())
        .unwrap();
    let follower = Database::open(&follower_path).unwrap();
    let error = follower.append_replication_batch(&batch_b).unwrap_err();
    assert!(error.to_string().contains("source does not match"));
    assert_eq!(
        mongreldb_core::replica_epoch(&follower_path).unwrap(),
        follower_epoch
    );
    assert_eq!(follower.table("items").unwrap().lock().count(), 1);
}

fn put_named(db: &Database, table: &str, id: i64) {
    db.transaction(|txn| {
        txn.put(table, vec![(1, Value::Int64(id))])?;
        Ok(())
    })
    .unwrap();
}
