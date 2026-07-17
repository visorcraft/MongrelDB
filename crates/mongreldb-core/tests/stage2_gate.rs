//! Scratch — empirical probes (will be replaced by the real gate tests).

use mongreldb_core::catalog_cmds::{
    CatalogCommand, CatalogCommandRecord, CATALOG_COMMAND_FORMAT_VERSION,
};
use mongreldb_core::memtable::{Row, Value};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::storage_mode::StorageMode;
use mongreldb_core::wal::{Op, Record};
use mongreldb_core::{Database, Epoch, RowId};
use mongreldb_types::ids::{ClusterId, DatabaseId, NodeId};

fn simple_schema() -> Schema {
    Schema {
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
        }],
        ..Schema::default()
    }
}

fn create_table_record(name: &str, catalog_version: u64) -> CatalogCommandRecord {
    CatalogCommandRecord {
        version: CATALOG_COMMAND_FORMAT_VERSION,
        catalog_version,
        command: CatalogCommand::CreateTable {
            name: name.to_string(),
            schema: simple_schema(),
            created_epoch: 1,
        },
    }
}

fn put_records(txn_id: u64, table_id: u64, epoch: u64, values: &[i64]) -> Vec<Record> {
    let rows: Vec<Row> = values
        .iter()
        .map(|value| {
            Row::new(RowId(*value as u64), Epoch(epoch)).with_column(1, Value::Int64(*value))
        })
        .collect();
    vec![
        Record::new(
            Epoch(0),
            txn_id,
            Op::Put {
                table_id,
                rows: bincode::serialize(&rows).unwrap(),
            },
        ),
        Record::new(Epoch(0), txn_id, Op::CommitTimestamp { unix_nanos: 1_000 }),
        Record::new(
            Epoch(0),
            txn_id,
            Op::TxnCommit {
                epoch,
                added_runs: Vec::new(),
            },
        ),
    ]
}

fn build_replica(root: &std::path::Path, node_seed: u8) -> Database {
    let db = Database::create_cluster_replica(
        root,
        ClusterId::from_bytes([1; 16]),
        NodeId::from_bytes([node_seed; 16]),
        DatabaseId::from_bytes([3; 16]),
    )
    .unwrap();
    db.apply_replicated_catalog_command(&create_table_record("items", 1))
        .unwrap();
    assert!(db.apply_replicated_records(&put_records(1, 0, 2, &[10, 20, 30])).unwrap());
    assert!(db.apply_replicated_records(&put_records(2, 0, 3, &[40])).unwrap());
    db
}

fn dump(root: &std::path::Path) -> Vec<(String, u64, String)> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                let bytes = std::fs::read(&path).unwrap();
                let rel = path.strip_prefix(root).unwrap().to_string_lossy().to_string();
                let hash = format!("{:x}", sha2::Sha256::digest(&bytes));
                out.push((rel, bytes.len() as u64, hash));
            }
        }
    }
    out.sort();
    out
}

use sha2::Digest;

#[test]
fn scratch_probe() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let root_a = dir_a.path().join("db");
    let root_b = dir_b.path().join("db");
    let db_a = build_replica(&root_a, 10);
    let db_b = build_replica(&root_b, 20);

    // Probe 1: hot_backup on a live read-only replica core.
    let parent = tempfile::tempdir().unwrap();
    let dest = parent.path().join("backup");
    let result = db_a.hot_backup(&dest);
    eprintln!("PROBE hot_backup on replica: {:?}", result.as_ref().map(|r| (r.epoch, r.files, r.bytes)));
    if let Err(error) = &result {
        eprintln!("PROBE hot_backup error: {error}");
    }

    // Probe 2: shutdown both, compare file trees.
    let epoch_a = db_a.visible_epoch();
    let epoch_b = db_b.visible_epoch();
    eprintln!("PROBE epochs: {epoch_a:?} vs {epoch_b:?}");
    drop(db_a);
    drop(db_b);
    let files_a = dump(&root_a);
    let files_b = dump(&root_b);
    let names_a: std::collections::BTreeSet<_> = files_a.iter().map(|f| f.0.clone()).collect();
    let names_b: std::collections::BTreeSet<_> = files_b.iter().map(|f| f.0.clone()).collect();
    eprintln!("PROBE only-in-A: {:?}", names_a.difference(&names_b).collect::<Vec<_>>());
    eprintln!("PROBE only-in-B: {:?}", names_b.difference(&names_a).collect::<Vec<_>>());
    for (rel, size, hash) in &files_a {
        let other = files_b.iter().find(|f| &f.0 == rel).unwrap();
        let same = hash == &other.2 && size == &other.1;
        eprintln!("PROBE file {rel} size={size} identical={same}");
    }

    // Probe 3: storage-mode contents
    let mode_a = mongreldb_core::storage_mode::read_at(&root_a).unwrap();
    let mode_b = mongreldb_core::storage_mode::read_at(&root_b).unwrap();
    eprintln!("PROBE mode A: {mode_a:?}");
    eprintln!("PROBE mode B: {mode_b:?}");
    assert_eq!(mode_a, Some(StorageMode::ClusterReplica {
        cluster_id: ClusterId::from_bytes([1; 16]),
        node_id: NodeId::from_bytes([10; 16]),
        database_id: DatabaseId::from_bytes([3; 16]),
    }));
}
