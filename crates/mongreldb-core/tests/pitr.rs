use mongreldb_core::{
    read_pitr_manifest, restore_pitr, ColumnDef, ColumnFlags, Database, PitrCredentials,
    PitrTarget, Schema, TypeId, Value,
};
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

fn put(db: &Database, id: i64) -> u64 {
    let mut transaction = db.begin();
    transaction
        .put("items", vec![(1, Value::Int64(id))])
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
