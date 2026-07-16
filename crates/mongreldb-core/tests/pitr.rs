use mongreldb_core::{
    read_pitr_manifest, restore_pitr, ColumnDef, ColumnFlags, Database, ExternalTableDefinition,
    ExternalTableEntry, ModuleArg, ModuleCapabilities, MongrelError, PitrCredentials, PitrTarget,
    Schema, TypeId, Value,
};
use std::sync::{Arc, Barrier};
use std::time::Duration;
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
        }],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn bytes_schema() -> Schema {
    Schema {
        schema_id: 0,
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
                name: "payload".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn put(db: &Database, id: i64) -> u64 {
    let mut transaction = db.begin();
    transaction
        .put("items", vec![(1, Value::Int64(id))])
        .unwrap();
    transaction.commit().unwrap().0
}

fn put_bytes(db: &Database, id: i64, payload: &[u8]) -> u64 {
    let mut transaction = db.begin();
    transaction
        .put(
            "secrets",
            vec![(1, Value::Int64(id)), (2, Value::Bytes(payload.to_vec()))],
        )
        .unwrap();
    transaction.commit().unwrap().0
}

fn ids(db: &Database) -> Vec<i64> {
    let mut ids = db
        .table("items")
        .unwrap()
        .lock()
        .visible_rows(db.snapshot().0)
        .unwrap()
        .into_iter()
        .filter_map(|row| match row.columns.get(&1) {
            Some(Value::Int64(value)) => Some(*value),
            _ => None,
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
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
fn pitr_restores_epoch_timestamp_and_latest_cutoffs() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let restore_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let db = Database::create(source.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    put(&db, 1);
    let base = db.create_pitr_archive(&archive).unwrap();
    let second_epoch = put(&db, 2);
    let third_epoch = put(&db, 3);
    let archived = db.archive_pitr(&archive).unwrap();
    assert_eq!(archived.through_epoch, third_epoch);
    assert!(archived.records > 0);

    let manifest = read_pitr_manifest(&archive).unwrap();
    assert_eq!(manifest.base_epoch, base.through_epoch);
    let second_timestamp = manifest
        .chunks
        .iter()
        .flat_map(|chunk| &chunk.commits)
        .find(|commit| commit.epoch == second_epoch)
        .unwrap()
        .unix_nanos;

    let at_epoch = restore_parent.path().join("at-epoch");
    assert_eq!(
        restore_pitr(
            &archive,
            &at_epoch,
            PitrTarget::Epoch(second_epoch),
            PitrCredentials::None,
        )
        .unwrap(),
        second_epoch
    );
    assert_eq!(ids(&Database::open(&at_epoch).unwrap()), vec![1, 2]);

    let at_timestamp = restore_parent.path().join("at-time");
    restore_pitr(
        &archive,
        &at_timestamp,
        PitrTarget::TimestampNanos(second_timestamp),
        PitrCredentials::None,
    )
    .unwrap();
    assert_eq!(ids(&Database::open(&at_timestamp).unwrap()), vec![1, 2]);

    let latest = restore_parent.path().join("latest");
    restore_pitr(&archive, &latest, PitrTarget::Latest, PitrCredentials::None).unwrap();
    assert_eq!(ids(&Database::open(&latest).unwrap()), vec![1, 2, 3]);
}

#[test]
fn pitr_epoch_target_maps_abandoned_ticket_to_previous_commit() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let restore_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let destination = restore_parent.path().join("restored");
    let db = Database::create(source.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    put(&db, 1);
    db.create_pitr_archive(&archive).unwrap();
    let prior = put(&db, 2);
    assert!(matches!(
        db.create_role_controlled("cancelled", || Err(MongrelError::Cancelled)),
        Err(MongrelError::Cancelled)
    ));
    let after_gap = put(&db, 3);
    assert!(after_gap > prior + 1);
    db.archive_pitr(&archive).unwrap();

    let effective = restore_pitr(
        &archive,
        &destination,
        PitrTarget::Epoch(after_gap - 1),
        PitrCredentials::None,
    )
    .unwrap();
    assert_eq!(effective, prior);
    assert_eq!(ids(&Database::open(destination).unwrap()), vec![1, 2]);
}

#[test]
fn pitr_ignores_an_abandoned_latest_epoch_ticket() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let db = Database::create(source.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    let base = db.create_pitr_archive(&archive).unwrap();

    let mut invalid = schema();
    invalid.columns[0].ty = TypeId::Bytes;
    invalid.columns[0].flags = invalid.columns[0].flags.with(ColumnFlags::AUTO_INCREMENT);
    assert!(db.create_table("invalid", invalid).is_err());
    assert!(db.visible_epoch().0 > base.through_epoch);

    let report = db.archive_pitr(&archive).unwrap();
    assert_eq!(report.from_epoch, base.through_epoch);
    assert_eq!(report.through_epoch, base.through_epoch);
    assert_eq!(report.records, 0);
}

#[test]
fn pitr_accepts_an_abandoned_epoch_between_commits() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let restore_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let destination = restore_parent.path().join("restored");
    let db = Database::create(source.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    let base = db.create_pitr_archive(&archive).unwrap();

    let mut invalid = schema();
    invalid.columns[0].ty = TypeId::Bytes;
    invalid.columns[0].flags = invalid.columns[0].flags.with(ColumnFlags::AUTO_INCREMENT);
    assert!(db.create_table("invalid", invalid).is_err());
    let committed = put(&db, 1);
    assert!(committed > base.through_epoch.saturating_add(1));

    let report = db.archive_pitr(&archive).unwrap();
    assert_eq!(report.through_epoch, committed);
    restore_pitr(
        &archive,
        &destination,
        PitrTarget::Latest,
        PitrCredentials::None,
    )
    .unwrap();
    assert_eq!(ids(&Database::open(destination).unwrap()), vec![1]);
}

#[test]
fn pitr_base_timestamp_is_captured_at_the_backup_boundary() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let db = Arc::new(Database::create(source.path()).unwrap());
    db.create_table("items", schema()).unwrap();
    put(&db, 1);

    let boundary = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    db.__set_backup_hook({
        let boundary = Arc::clone(&boundary);
        let release = Arc::clone(&release);
        move || {
            boundary.wait();
            release.wait();
        }
    });
    let worker = {
        let db = Arc::clone(&db);
        let archive = archive.clone();
        std::thread::spawn(move || db.create_pitr_archive(archive))
    };
    boundary.wait();
    let committed_epoch = put(&db, 2);
    std::thread::sleep(Duration::from_millis(20));
    release.wait();
    let base = worker.join().unwrap().unwrap();

    let batch = db.replication_batch_since(base.through_epoch).unwrap();
    let txn_id = batch
        .records
        .iter()
        .find_map(|record| match record.op {
            mongreldb_core::wal::Op::TxnCommit { epoch, .. } if epoch == committed_epoch => {
                Some(record.txn_id)
            }
            _ => None,
        })
        .unwrap();
    let commit_time = batch
        .records
        .iter()
        .find_map(|record| match record.op {
            mongreldb_core::wal::Op::CommitTimestamp { unix_nanos } if record.txn_id == txn_id => {
                Some(unix_nanos)
            }
            _ => None,
        })
        .unwrap();
    let manifest = read_pitr_manifest(&archive).unwrap();
    assert_eq!(manifest.base_epoch, base.through_epoch);
    assert!(manifest.base_unix_nanos <= commit_time);
}

#[test]
fn pitr_materializes_spilled_transactions() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let restore_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let destination = restore_parent.path().join("restored");
    let db = Database::create(source.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    put(&db, 1);
    db.create_pitr_archive(&archive).unwrap();
    db.set_spill_threshold(1);
    put(&db, 2);
    db.archive_pitr(&archive).unwrap();

    restore_pitr(
        &archive,
        &destination,
        PitrTarget::Latest,
        PitrCredentials::None,
    )
    .unwrap();
    assert_eq!(ids(&Database::open(destination).unwrap()), vec![1, 2]);
}

#[test]
fn pitr_archive_fails_closed_after_wal_gap() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let db = Database::create(source.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    put(&db, 1);
    db.create_pitr_archive(&archive).unwrap();
    put(&db, 2);
    db.checkpoint().unwrap();
    assert!(db.archive_pitr(&archive).is_err());
}

#[test]
fn spilled_commit_cannot_hide_a_pitr_retention_gap() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let db = Database::create(source.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    put(&db, 1);
    let base = db.create_pitr_archive(&archive).unwrap();

    put(&db, 2);
    db.checkpoint().unwrap();
    db.set_spill_threshold(1);
    put(&db, 3);

    let batch = db.replication_batch_since(base.through_epoch).unwrap();
    assert!(batch.retention_gap);
    assert!(batch.contains_spilled_commits);
    assert!(db.archive_pitr(&archive).is_err());
}

#[test]
fn fixed_manifest_temp_name_does_not_block_pitr_archive() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let db = Database::create(source.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    put(&db, 1);
    db.create_pitr_archive(&archive).unwrap();
    put(&db, 2);

    let manifest_stage = archive.join(".pitr.json.tmp");
    std::fs::create_dir(&manifest_stage).unwrap();
    let report = db.archive_pitr(&archive).unwrap();
    assert!(report.records > 0);
    assert!(manifest_stage.is_dir());
    let manifest = read_pitr_manifest(&archive).unwrap();
    assert_eq!(manifest.chunks.len(), 1);
}

#[cfg(feature = "encryption")]
#[test]
fn fixed_manifest_temp_name_does_not_block_encrypted_pitr_archive() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let db = Database::create_encrypted(source.path(), "secret passphrase").unwrap();
    db.create_table("items", schema()).unwrap();
    db.create_pitr_archive(&archive).unwrap();
    put(&db, 2);

    let manifest_stage = archive.join(".pitr.json.tmp");
    std::fs::create_dir(&manifest_stage).unwrap();
    let report = db.archive_pitr(&archive).unwrap();
    assert!(report.records > 0);
    assert!(manifest_stage.is_dir());
    let manifest = read_pitr_manifest(&archive).unwrap();
    assert_eq!(manifest.chunks.len(), 1);
    assert!(manifest.authentication.is_some());
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_pitr_reuses_base_key_material() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let restore_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let destination = restore_parent.path().join("restored");
    let db = Database::create_encrypted(source.path(), "secret passphrase").unwrap();
    db.create_table("items", schema()).unwrap();
    put(&db, 1);
    db.create_pitr_archive(&archive).unwrap();
    put(&db, 2);
    db.archive_pitr(&archive).unwrap();

    assert!(restore_pitr(
        &archive,
        restore_parent.path().join("wrong"),
        PitrTarget::Latest,
        PitrCredentials::Encryption("wrong"),
    )
    .is_err());
    restore_pitr(
        &archive,
        &destination,
        PitrTarget::Latest,
        PitrCredentials::Encryption("secret passphrase"),
    )
    .unwrap();
    let restored = Database::open_encrypted(destination, "secret passphrase").unwrap();
    assert_eq!(ids(&restored), vec![1, 2]);
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_pitr_chunks_hide_plaintext_and_reject_tampering() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let restore_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let destination = restore_parent.path().join("restored");
    let tampered_destination = restore_parent.path().join("tampered");
    let sentinel = b"MONGRELDB-PITR-SECRET-SENTINEL-7e9dc2b1";
    let db = Database::create_encrypted(source.path(), "secret passphrase").unwrap();
    db.create_table("secrets", bytes_schema()).unwrap();
    db.create_pitr_archive(&archive).unwrap();
    put_bytes(&db, 1, sentinel);
    db.archive_pitr(&archive).unwrap();

    let manifest = read_pitr_manifest(&archive).unwrap();
    assert!(manifest.encrypted);
    assert!(manifest.authentication.is_some());
    let chunk_path = archive.join(&manifest.chunks[0].file);
    let mut chunk_bytes = std::fs::read(&chunk_path).unwrap();
    assert!(!chunk_bytes
        .windows(sentinel.len())
        .any(|window| window == sentinel));

    restore_pitr(
        &archive,
        &destination,
        PitrTarget::Latest,
        PitrCredentials::Encryption("secret passphrase"),
    )
    .unwrap();
    let restored = Database::open_encrypted(&destination, "secret passphrase").unwrap();
    let rows = restored
        .table("secrets")
        .unwrap()
        .lock()
        .visible_rows(restored.snapshot().0)
        .unwrap();
    assert_eq!(
        rows[0].columns.get(&2),
        Some(&Value::Bytes(sentinel.to_vec()))
    );

    let middle = chunk_bytes.len() / 2;
    chunk_bytes[middle] ^= 0x80;
    std::fs::write(&chunk_path, chunk_bytes).unwrap();
    assert!(restore_pitr(
        &archive,
        &tampered_destination,
        PitrTarget::Latest,
        PitrCredentials::Encryption("secret passphrase"),
    )
    .is_err());
    assert!(!tampered_destination.exists());
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_pitr_rejects_valid_looking_manifest_rewrite() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let restore_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let destination = restore_parent.path().join("restored");
    let db = Database::create_encrypted(source.path(), "secret passphrase").unwrap();
    db.create_table("items", schema()).unwrap();
    db.create_pitr_archive(&archive).unwrap();
    put(&db, 1);
    db.archive_pitr(&archive).unwrap();

    let mut manifest = read_pitr_manifest(&archive).unwrap();
    let forged_timestamp = manifest.last_commit_unix_nanos.saturating_add(1);
    manifest.last_commit_unix_nanos = forged_timestamp;
    manifest
        .chunks
        .last_mut()
        .unwrap()
        .commits
        .last_mut()
        .unwrap()
        .unix_nanos = forged_timestamp;
    std::fs::write(
        archive.join("pitr.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();

    assert!(restore_pitr(
        &archive,
        &destination,
        PitrTarget::Latest,
        PitrCredentials::Encryption("secret passphrase"),
    )
    .is_err());
    assert!(!destination.exists());
}

#[test]
fn pitr_rejects_rewritten_base_backup_manifest() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let restore_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let destination = restore_parent.path().join("restored");
    let db = Database::create(source.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    db.create_pitr_archive(&archive).unwrap();
    let backup_manifest = archive.join("base/_meta/backup.json");
    let mut backup: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&backup_manifest).unwrap()).unwrap();
    backup["created_unix_nanos"] = serde_json::json!(backup["created_unix_nanos"]
        .as_u64()
        .unwrap()
        .saturating_add(1));
    std::fs::write(
        &backup_manifest,
        serde_json::to_vec_pretty(&backup).unwrap(),
    )
    .unwrap();

    assert!(restore_pitr(
        &archive,
        &destination,
        PitrTarget::Latest,
        PitrCredentials::None,
    )
    .is_err());
    assert!(!destination.exists());
}

#[test]
fn pitr_rejects_unlisted_base_backup_file() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let restore_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let destination = restore_parent.path().join("restored");
    let db = Database::create(source.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    db.create_pitr_archive(&archive).unwrap();
    std::fs::write(archive.join("base/unlisted"), b"not authenticated").unwrap();

    assert!(restore_pitr(
        &archive,
        &destination,
        PitrTarget::Latest,
        PitrCredentials::None,
    )
    .is_err());
    assert!(!destination.exists());
}

#[test]
fn pitr_rejects_chunk_substitution() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let restore_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let destination = restore_parent.path().join("restored");
    let db = Database::create(source.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    db.create_pitr_archive(&archive).unwrap();
    put(&db, 1);
    db.archive_pitr(&archive).unwrap();
    put(&db, 2);
    db.archive_pitr(&archive).unwrap();

    let manifest = read_pitr_manifest(&archive).unwrap();
    assert_eq!(manifest.chunks.len(), 2);
    let first = std::fs::read(archive.join(&manifest.chunks[0].file)).unwrap();
    std::fs::write(archive.join(&manifest.chunks[1].file), first).unwrap();

    assert!(restore_pitr(
        &archive,
        &destination,
        PitrTarget::Latest,
        PitrCredentials::None,
    )
    .is_err());
    assert!(!destination.exists());
}

#[test]
fn pitr_restores_catalog_cutoffs_and_external_generations() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let restore_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let db = Database::create(source.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    db.create_pitr_archive(&archive).unwrap();

    db.create_external_table(external_entry("ext")).unwrap();
    let old_generation_epoch = db
        .commit_external_table_state("ext", b"old-generation")
        .unwrap()
        .0;
    db.set_sql_pragma_i64_with_epoch("application_id", 9001)
        .unwrap();
    db.drop_external_table("ext").unwrap();
    db.create_external_table(external_entry("ext")).unwrap();
    db.commit_external_table_state("ext", b"new-generation")
        .unwrap();
    db.rename_table("items", "renamed").unwrap();
    db.enable_auth("admin", "admin-password").unwrap();
    db.archive_pitr(&archive).unwrap();

    let bad_credentials_destination = restore_parent.path().join("bad-credentials");
    let error = restore_pitr(
        &archive,
        &bad_credentials_destination,
        PitrTarget::Latest,
        PitrCredentials::User {
            username: "admin",
            password: "wrong",
        },
    )
    .unwrap_err();
    assert!(matches!(
        error,
        MongrelError::InvalidCredentials { username } if username == "admin"
    ));
    assert!(!bad_credentials_destination.exists());

    let verified_destination = restore_parent.path().join("verified-credentials");
    restore_pitr(
        &archive,
        &verified_destination,
        PitrTarget::Latest,
        PitrCredentials::User {
            username: "admin",
            password: "admin-password",
        },
    )
    .unwrap();
    assert!(
        Database::open_with_credentials(&verified_destination, "admin", "admin-password").is_ok()
    );

    let old_destination = restore_parent.path().join("old-generation");
    restore_pitr(
        &archive,
        &old_destination,
        PitrTarget::Epoch(old_generation_epoch),
        PitrCredentials::None,
    )
    .unwrap();
    let old = Database::open(&old_destination).unwrap();
    assert_eq!(external_state(&old_destination, "ext"), b"old-generation");
    assert!(old.table("items").is_ok());
    assert!(old.table("renamed").is_err());
    assert_eq!(old.sql_pragma_i64("application_id").unwrap(), None);

    let latest_destination = restore_parent.path().join("latest-generation");
    restore_pitr(
        &archive,
        &latest_destination,
        PitrTarget::Latest,
        PitrCredentials::None,
    )
    .unwrap();
    let latest =
        Database::open_with_credentials(&latest_destination, "admin", "admin-password").unwrap();
    assert_eq!(
        serde_json::to_value(latest.catalog_snapshot()).unwrap(),
        serde_json::to_value(db.catalog_snapshot()).unwrap()
    );
    assert_eq!(
        external_state(&latest_destination, "ext"),
        b"new-generation"
    );
    assert!(latest.table("items").is_err());
    assert!(latest.table("renamed").is_ok());
    assert_eq!(latest.sql_pragma_i64("application_id").unwrap(), Some(9001));
}

#[test]
fn pitr_materializes_spilled_commit_after_source_table_is_dropped() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let restore_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let db = Database::create(source.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    db.create_pitr_archive(&archive).unwrap();
    db.set_spill_threshold(1);
    let spill_epoch = put(&db, 77);
    db.drop_table("items").unwrap();
    db.gc().unwrap();
    db.archive_pitr(&archive).unwrap();

    let at_spill = restore_parent.path().join("at-spill");
    restore_pitr(
        &archive,
        &at_spill,
        PitrTarget::Epoch(spill_epoch),
        PitrCredentials::None,
    )
    .unwrap();
    assert_eq!(ids(&Database::open(&at_spill).unwrap()), vec![77]);

    let latest = restore_parent.path().join("after-drop");
    restore_pitr(&archive, &latest, PitrTarget::Latest, PitrCredentials::None).unwrap();
    assert!(Database::open(&latest).unwrap().table("items").is_err());
}

#[test]
fn doctor_drop_is_replayed_by_pitr() {
    let source = tempdir().unwrap();
    let archive_parent = tempdir().unwrap();
    let restore_parent = tempdir().unwrap();
    let archive = archive_parent.path().join("archive");
    let destination = restore_parent.path().join("restored");
    let db = Database::create(source.path()).unwrap();
    db.create_table("broken", schema()).unwrap();
    db.table("broken")
        .unwrap()
        .lock()
        .set_mutable_run_spill_bytes(1);
    db.transaction(|transaction| {
        transaction.put("broken", vec![(1, Value::Int64(1))])?;
        Ok(())
    })
    .unwrap();
    db.table("broken").unwrap().lock().flush().unwrap();
    db.create_pitr_archive(&archive).unwrap();

    let table_id = db.table_id("broken").unwrap();
    let run = std::fs::read_dir(
        source
            .path()
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
    assert!(db.doctor().unwrap().contains(&table_id));
    db.archive_pitr(&archive).unwrap();

    restore_pitr(
        &archive,
        &destination,
        PitrTarget::Latest,
        PitrCredentials::None,
    )
    .unwrap();
    assert!(Database::open(&destination)
        .unwrap()
        .table("broken")
        .is_err());
}
