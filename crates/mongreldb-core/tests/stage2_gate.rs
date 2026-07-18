//! Stage 2 gate leftovers (spec section 11, "Stage 2 gate") — integration
//! coverage for the deferred items `docs/20-replicated-ha.md` tracked:
//!
//! 1. **Backup from a follower is valid**
//!    (`backup_from_a_follower_is_valid_and_matches_the_leader`).
//! 2. **AI and SQL results match standalone behavior at the same snapshot**
//!    (`ai_and_sql_results_match_standalone_at_the_same_snapshot`).
//! 3. **Storage-mode open gate** (spec section 5.3) as an integration-level
//!    sanity pass (`storage_mode_open_gate_matrix`).
//!
//! # Surface and scope notes
//!
//! - The consensus engine-group factory
//!   (`mongreldb_consensus::engine_sink::open_engine_group`) is not reachable
//!   from `mongreldb-core` integration tests: the dependency points the other
//!   way (consensus depends on core), and the raft → sink → core binding is
//!   covered by the consensus crate's own `engine_sink` tests. These tests
//!   therefore drive the identical public apply path that
//!   `EngineApplySink::apply` dispatches to —
//!   `Database::apply_replicated_catalog_command` and
//!   `Database::apply_replicated_records` — against `ClusterReplica` cores.
//!   A "leader" and a "follower" replica fed the same committed command
//!   stream are exactly the two applied states a consensus group converges
//!   to (the state machine's applied-sequence equality is chaos-qualified on
//!   the consensus side).
//! - `Database::hot_backup` requires `Permission::Admin`, and every replica
//!   open (cluster runtime or offline validation) is read-only, so the live
//!   call is rejected with `MongrelError::ReadOnlyReplica` — pinned below.
//!   The follower backup is therefore staged **offline** from the quiesced
//!   applied root (the operator/cluster-runtime flow of spec section 5.3:
//!   quiesce the replica, copy its tree, manifest it), then validated with
//!   the real `verify_backup` + `validate_restore` machinery. A live online
//!   hot backup from a replica core is follow-up work for the cluster
//!   runtime wave.
//! - SQL proper lives in `mongreldb-query`, which has no replica wiring yet;
//!   the aggregate comparison exercises the two engine surfaces the SQL
//!   layer uses: `Table::aggregate_native` (the native pushdown it lowers
//!   `COUNT`/`SUM`/`MIN`/`MAX`/`AVG` to, servable where a sorted run exists)
//!   and the scan fallback it takes when the pushdown declines — a replica's
//!   applied state is overlay-only this wave (replicated spilled-run commits
//!   fail closed until Stage 2C spill translation), so on a replica the
//!   pushdown always declines. The gate asserts the standalone native
//!   results equal the replica fallback values at the same snapshot.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

use mongreldb_core::backup::{
    validate_restore, BackupFile, BackupManifest, BACKUP_FORMAT_VERSION, BACKUP_MANIFEST_PATH,
};
use mongreldb_core::catalog_cmds::{
    CatalogCommand, CatalogCommandRecord, CATALOG_COMMAND_FORMAT_VERSION,
};
use mongreldb_core::memtable::{Row, Value};
use mongreldb_core::query::{Condition, Query, Retriever, RetrieverScore, SetMember};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::storage_mode::{self, StorageMode, STORAGE_MODE_FORMAT_VERSION};
use mongreldb_core::wal::{Op, Record};
use mongreldb_core::{
    verify_backup, Database, Epoch, MongrelError, NativeAgg, NativeAggResult, OpenOptions, RowId,
    Snapshot, STORAGE_MODE_FILENAME,
};
use mongreldb_types::ids::{ClusterId, DatabaseId, NodeId};
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Shared fixtures: replicated identity, schemas, and the replicated apply
// stream (the same payloads a consensus leader assigns and every replica's
// apply sink delivers).
// ---------------------------------------------------------------------------

fn cluster_id() -> ClusterId {
    ClusterId::from_bytes([1; 16])
}

fn database_id() -> DatabaseId {
    DatabaseId::from_bytes([3; 16])
}

fn node_id(seed: u8) -> NodeId {
    NodeId::from_bytes([seed; 16])
}

fn cluster_mode(node_seed: u8) -> StorageMode {
    StorageMode::ClusterReplica {
        cluster_id: cluster_id(),
        node_id: node_id(node_seed),
        database_id: database_id(),
    }
}

fn column(id: u16, name: &str, ty: TypeId, primary_key: bool) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        ty,
        flags: if primary_key {
            ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY)
        } else {
            ColumnFlags::empty()
        },
        default_value: None,
        embedding_source: None,
    }
}

/// One-column table for the backup and storage-mode tests.
fn simple_schema() -> Schema {
    Schema {
        columns: vec![column(1, "id", TypeId::Int64, true)],
        ..Schema::default()
    }
}

/// Seven-column table exercising every public AI index kind plus a bitmap and
/// an FM index, with a plain Int64 column for native aggregates.
fn ai_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            column(1, "id", TypeId::Int64, true),
            column(2, "embedding", TypeId::Embedding { dim: 8 }, false),
            column(3, "sparse", TypeId::Bytes, false),
            column(4, "members", TypeId::Bytes, false),
            column(5, "city", TypeId::Bytes, false),
            column(6, "doc", TypeId::Bytes, false),
            column(7, "score", TypeId::Int64, false),
        ],
        indexes: vec![
            IndexDef {
                name: "ann".into(),
                column_id: 2,
                kind: IndexKind::Ann,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "sparse".into(),
                column_id: 3,
                kind: IndexKind::Sparse,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "minhash".into(),
                column_id: 4,
                kind: IndexKind::MinHash,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "city_bm".into(),
                column_id: 5,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "doc_fm".into(),
                column_id: 6,
                kind: IndexKind::FmIndex,
                predicate: None,
                options: Default::default(),
            },
        ],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn create_table_record(
    name: &str,
    schema: &Schema,
    catalog_version: u64,
    created_epoch: u64,
) -> CatalogCommandRecord {
    CatalogCommandRecord {
        version: CATALOG_COMMAND_FORMAT_VERSION,
        catalog_version,
        command: CatalogCommand::CreateTable {
            name: name.to_string(),
            schema: schema.clone(),
            created_epoch,
        },
    }
}

/// One committed transaction as the leader replicates it: put records, the
/// leader-assigned commit timestamp, and the commit marker at the tail.
fn put_records(
    txn_id: u64,
    table_id: u64,
    epoch: u64,
    rows: Vec<(u64, Vec<(u16, Value)>)>,
) -> Vec<Record> {
    let rows: Vec<Row> = rows
        .into_iter()
        .map(|(row_id, columns)| {
            let mut row = Row::new(RowId(row_id), Epoch(epoch));
            for (column_id, value) in columns {
                row.columns.insert(column_id, value);
            }
            row
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

fn visible_ids(db: &Database, table: &str) -> Vec<i64> {
    let handle = db.table(table).unwrap();
    let rows = handle
        .lock()
        .visible_rows(Snapshot::at(Epoch(u64::MAX)))
        .unwrap();
    let mut values: Vec<i64> = rows
        .iter()
        .map(|row| match row.columns.get(&1) {
            Some(Value::Int64(value)) => *value,
            other => panic!("unexpected column: {other:?}"),
        })
        .collect();
    values.sort_unstable();
    values
}

// ---------------------------------------------------------------------------
// (1) Backup from a follower is valid.
// ---------------------------------------------------------------------------

/// Builds a `ClusterReplica` core and applies the committed stream the
/// consensus group delivers: create the table (catalog v1, epoch 1), commit
/// [10, 20, 30] at epoch 2, commit [40] at epoch 3.
fn build_simple_replica(root: &Path, node_seed: u8) -> Database {
    let db =
        Database::create_cluster_replica(root, cluster_id(), node_id(node_seed), database_id())
            .unwrap();
    db.apply_replicated_catalog_command(&create_table_record("items", &simple_schema(), 1, 1))
        .unwrap();
    let first = put_records(
        1,
        0,
        2,
        vec![(1, 10), (2, 20), (3, 30)]
            .into_iter()
            .map(|(row_id, value)| (row_id, vec![(1, Value::Int64(value))]))
            .collect(),
    );
    assert!(db.apply_replicated_records(&first).unwrap());
    let second = put_records(2, 0, 3, vec![(4, vec![(1, Value::Int64(40))])]);
    assert!(db.apply_replicated_records(&second).unwrap());
    db
}

/// The file set the online backup path copies (`backup_path_excluded` in
/// `database.rs`): everything except the lock, Stage 1 replication markers, a
/// previous manifest, and cache/txn/pin scratch.
fn backup_excluded(relative: &Path) -> bool {
    relative == Path::new("_meta/.lock")
        || relative == Path::new("_meta/replica")
        || relative == Path::new("_meta/repl_epoch")
        || relative == Path::new(BACKUP_MANIFEST_PATH)
        || relative.components().any(|component| {
            matches!(
                component,
                Component::Normal(name) if name == "_cache" || name == "_txn" || name == "backup-pins"
            )
        })
}

fn walk_tree(root: &Path, dir: &Path, dirs: &mut Vec<PathBuf>, files: &mut Vec<PathBuf>) {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    entries.sort();
    for path in entries {
        let relative = path.strip_prefix(root).unwrap().to_path_buf();
        if path.is_dir() {
            dirs.push(relative);
            walk_tree(root, &path, dirs, files);
        } else {
            files.push(relative);
        }
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Stages an offline backup of a quiesced database root: copy the
/// `backup_path_excluded`-filtered tree (directories included — an open
/// expects `_runs`/`_rcache` to exist, exactly as the online
/// `copy_backup_boundary` preserves them), then write the checksummed
/// manifest last, mirroring the online `hot_backup` ordering and per-file
/// hashing (`BackupManifest::create_controlled_durable`).
fn stage_offline_backup(
    source: &Path,
    destination: &Path,
    epoch: u64,
    catalog_version: u64,
) -> BackupManifest {
    let mut dirs = Vec::new();
    let mut relatives = Vec::new();
    walk_tree(source, source, &mut dirs, &mut relatives);
    dirs.retain(|relative| !backup_excluded(relative));
    dirs.sort();
    relatives.retain(|relative| !backup_excluded(relative));
    relatives.sort();
    relatives.dedup();

    for dir in &dirs {
        std::fs::create_dir_all(destination.join(dir)).unwrap();
    }
    let mut files = Vec::with_capacity(relatives.len());
    for relative in &relatives {
        let bytes = std::fs::read(source.join(relative)).unwrap();
        let target = destination.join(relative);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, &bytes).unwrap();
        files.push(BackupFile {
            path: relative.clone(),
            bytes: bytes.len() as u64,
            sha256: sha256_hex(&bytes),
        });
    }

    let identity = std::fs::read(source.join("_meta/replication_id")).unwrap();
    let generation = std::fs::read(source.join("_meta/generation")).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let manifest = BackupManifest {
        format_version: BACKUP_FORMAT_VERSION,
        epoch,
        created_unix_nanos: now.as_nanos() as u64,
        database_id: Some(DatabaseId::from_bytes(identity[..16].try_into().unwrap())),
        catalog_version,
        snapshot_unix_micros: now.as_micros() as u64,
        open_generation: u64::from_le_bytes(generation.try_into().unwrap()),
        encryption: None,
        files,
    };
    std::fs::create_dir_all(destination.join("_meta")).unwrap();
    std::fs::write(
        destination.join(BACKUP_MANIFEST_PATH),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    manifest
}

fn manifest_hashes(manifest: &BackupManifest) -> BTreeMap<&Path, &str> {
    manifest
        .files
        .iter()
        .map(|file| (file.path.as_path(), file.sha256.as_str()))
        .collect()
}

#[test]
fn backup_from_a_follower_is_valid_and_matches_the_leader() {
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();
    let leader_root = leader_dir.path().join("db");
    let follower_root = follower_dir.path().join("db");
    let leader = build_simple_replica(&leader_root, 10);
    let follower = build_simple_replica(&follower_root, 20);

    // The live online path is honestly pinned out this wave: every replica
    // open is read-only, and `hot_backup` requires `Permission::Admin`.
    let live_destination = follower_dir.path().join("live-backup");
    assert!(matches!(
        follower.hot_backup(&live_destination),
        Err(MongrelError::ReadOnlyReplica)
    ));
    assert!(!live_destination.exists());

    // Same committed stream ⇒ same applied watermark and catalog version.
    let watermark = follower.visible_epoch();
    assert_eq!(watermark, Epoch(3));
    assert_eq!(leader.visible_epoch(), watermark);
    let catalog_version = follower.catalog_version();
    assert_eq!(leader.catalog_version(), catalog_version);
    drop(leader);
    drop(follower);

    // Stage both backups offline from the quiesced roots (spec section 5.3).
    let parent = tempfile::tempdir().unwrap();
    let follower_backup = parent.path().join("follower-backup");
    let leader_backup = parent.path().join("leader-backup");
    let follower_manifest = stage_offline_backup(
        &follower_root,
        &follower_backup,
        watermark.0,
        catalog_version,
    );
    let leader_manifest =
        stage_offline_backup(&leader_root, &leader_backup, watermark.0, catalog_version);

    // The real validation machinery accepts the follower's backup.
    let verified = verify_backup(&follower_backup).unwrap();
    assert_eq!(verified, follower_manifest);
    assert_eq!(verified.epoch, watermark.0);
    let report = validate_restore(&follower_backup).unwrap();
    assert!(report.manifest_consistent);
    assert!(report.catalog_loaded);
    assert_eq!(report.files_checked, report.files_ok);
    assert!(report.bytes_checked > 0);
    assert!(report.issues.is_empty());
    // And the leader's, staged the same way at the same watermark.
    verify_backup(&leader_backup).unwrap();
    let leader_report = validate_restore(&leader_backup).unwrap();
    assert!(leader_report.manifest_consistent);
    assert!(leader_report.catalog_loaded);

    // Identical manifest file hashes vs the leader's backup at the same
    // applied watermark — except the two node-local markers, which must
    // differ: `_meta/replication_id` is a per-root CSPRNG identity (it also
    // feeds `database_id`), and `_meta/storage-mode` records the owning
    // node id. Everything that carries replicated applied state — catalog
    // checkpoint, WAL head and segments, table manifests, schemas — is
    // byte-identical.
    let follower_hashes = manifest_hashes(&follower_manifest);
    let leader_hashes = manifest_hashes(&leader_manifest);
    let follower_files: std::collections::BTreeSet<_> = follower_hashes.keys().collect();
    assert_eq!(
        follower_files,
        leader_hashes
            .keys()
            .collect::<std::collections::BTreeSet<_>>()
    );
    for (path, follower_hash) in &follower_hashes {
        let follower_hash = *follower_hash;
        let leader_hash = leader_hashes[path];
        if *path == Path::new("_meta/replication_id") || *path == Path::new("_meta/storage-mode") {
            assert_ne!(follower_hash, leader_hash, "node-local marker: {path:?}");
        } else {
            assert_eq!(follower_hash, leader_hash, "replicated state: {path:?}");
        }
    }
    assert_eq!(follower_manifest.epoch, leader_manifest.epoch);
    assert_eq!(
        follower_manifest.catalog_version,
        leader_manifest.catalog_version
    );
    assert_eq!(
        follower_manifest.open_generation,
        leader_manifest.open_generation
    );
    assert_ne!(follower_manifest.database_id, leader_manifest.database_id);

    // The follower's backup is directly openable — under the replica open
    // rules, because the marker travels with the backup: rejected by normal
    // opens, read-only through offline validation with the exact applied
    // rows, and open for the cluster runtime under the follower's identity.
    let error = Database::open(&follower_backup).unwrap_err();
    assert!(
        error.to_string().contains("cluster node runtime"),
        "unexpected error: {error}"
    );
    let reopened = Database::open_with_options(
        &follower_backup,
        OpenOptions::default().with_offline_validation(true),
    )
    .unwrap();
    assert!(reopened.is_read_only_replica());
    assert_eq!(visible_ids(&reopened, "items"), vec![10, 20, 30, 40]);
    assert!(matches!(
        reopened.create_table("nope", simple_schema()),
        Err(MongrelError::ReadOnlyReplica)
    ));
    drop(reopened);
    let runtime = Database::open_cluster_replica(&follower_backup, &cluster_mode(20)).unwrap();
    assert_eq!(visible_ids(&runtime, "items"), vec![10, 20, 30, 40]);
}

// ---------------------------------------------------------------------------
// (2) AI and SQL results match standalone behavior at the same snapshot.
// ---------------------------------------------------------------------------

/// Six rows with deliberately distinct retrieval scores: ANN hamming
/// distances 0..5 against an all-ones query (one more flipped sign per id),
/// sparse dot products 6/5/4 on token 1 for ids 1-3 (disjoint token for
/// 4-6), and MinHash Jaccard 1.0 / ~0.6 / ~0.33 against `{a,b,c,d}`.
fn ai_rows() -> Vec<Vec<(u16, Value)>> {
    let cities = ["alpha", "beta", "alpha", "gamma", "beta", "alpha"];
    let docs = [
        "alpha needle one",
        "bravo",
        "needle charlie",
        "delta",
        "echo needle",
        "foxtrot",
    ];
    let member_sets: [&[&str]; 6] = [
        &["a", "b", "c", "d"],
        &["a", "b", "c", "x"],
        &["a", "b", "x", "y"],
        &["p", "q", "r", "s"],
        &["p", "q", "r", "t"],
        &["p", "q", "u", "v"],
    ];
    (1..=6_i64)
        .map(|id| {
            let index = (id - 1) as usize;
            let mut embedding = vec![1.0_f32; 8];
            for slot in embedding.iter_mut().take(index) {
                *slot = -1.0;
            }
            let sparse: Vec<(u32, f32)> = if id <= 3 {
                vec![(1, (7 - id) as f32)]
            } else {
                vec![(2, 1.0)]
            };
            vec![
                (1, Value::Int64(id)),
                (2, Value::Embedding(embedding)),
                (3, Value::Bytes(bincode::serialize(&sparse).unwrap())),
                (
                    4,
                    Value::Bytes(serde_json::to_vec(member_sets[index]).unwrap()),
                ),
                (5, Value::Bytes(cities[index].as_bytes().to_vec())),
                (6, Value::Bytes(docs[index].as_bytes().to_vec())),
                (7, Value::Int64(id * 10)),
            ]
        })
        .collect()
}

/// Standalone baseline: the same schema and rows through the normal
/// transaction path (two commits, epochs 2 and 3 behind the DDL epoch 1).
/// The mutable-run spill threshold is pinned at 1 byte so the closing flush
/// publishes a real sorted run (the shape the native aggregate fast path
/// serves).
fn seed_standalone(root: &Path) -> Database {
    let db = Database::create(root).unwrap();
    db.create_table("items", ai_schema()).unwrap();
    db.table("items")
        .unwrap()
        .lock()
        .set_mutable_run_spill_bytes(1);
    let rows = ai_rows();
    db.transaction(|txn| {
        for row in &rows[..3] {
            txn.put("items", row.clone())?;
        }
        Ok(())
    })
    .unwrap();
    db.transaction(|txn| {
        for row in &rows[3..] {
            txn.put("items", row.clone())?;
        }
        Ok(())
    })
    .unwrap();
    db
}

/// Replica side: the same schema and rows through the replicated apply path,
/// at the same epochs (created epoch 1, commits 2 and 3).
fn seed_replica(root: &Path, node_seed: u8) -> Database {
    let db =
        Database::create_cluster_replica(root, cluster_id(), node_id(node_seed), database_id())
            .unwrap();
    db.apply_replicated_catalog_command(&create_table_record("items", &ai_schema(), 1, 1))
        .unwrap();
    let rows: Vec<(u64, Vec<(u16, Value)>)> = ai_rows()
        .into_iter()
        .enumerate()
        .map(|(index, row)| (index as u64 + 1, row))
        .collect();
    assert!(db
        .apply_replicated_records(&put_records(1, 0, 2, rows[..3].to_vec()))
        .unwrap());
    assert!(db
        .apply_replicated_records(&put_records(2, 0, 3, rows[3..].to_vec()))
        .unwrap());
    db
}

/// row_id → id-column value at the full-visibility snapshot: comparisons run
/// in id space so they never depend on how each side allocated row ids.
fn rowid_to_id(db: &Database) -> BTreeMap<u64, i64> {
    let handle = db.table("items").unwrap();
    let rows = handle
        .lock()
        .visible_rows(Snapshot::at(Epoch(u64::MAX)))
        .unwrap();
    rows.iter()
        .map(|row| {
            (
                row.row_id.0,
                match row.columns.get(&1) {
                    Some(Value::Int64(id)) => *id,
                    other => panic!("unexpected id column: {other:?}"),
                },
            )
        })
        .collect()
}

/// Full visible row content keyed by id: (commit epoch, columns sorted by
/// column id) — the strongest "same data at the same snapshot" comparison.
fn content_by_id(db: &Database, watermark: Epoch) -> BTreeMap<i64, (u64, Vec<(u16, Value)>)> {
    let handle = db.table("items").unwrap();
    let rows = handle.lock().visible_rows(Snapshot::at(watermark)).unwrap();
    rows.iter()
        .map(|row| {
            let id = match row.columns.get(&1) {
                Some(Value::Int64(id)) => *id,
                other => panic!("unexpected id column: {other:?}"),
            };
            let mut columns: Vec<(u16, Value)> = row
                .columns
                .iter()
                .map(|(column_id, value)| (*column_id, value.clone()))
                .collect();
            columns.sort_by_key(|(column_id, _)| *column_id);
            (id, (row.committed_epoch.0, columns))
        })
        .collect()
}

/// The five SQL aggregates through `Table::aggregate_native` — `None` where
/// the fast path declines (an overlay-only table with no sorted run).
fn native_aggregates(db: &Database, watermark: Epoch) -> Vec<Option<NativeAggResult>> {
    let handle = db.table("items").unwrap();
    let table = handle.lock();
    let snapshot = Snapshot::at(watermark);
    [
        (None, NativeAgg::Count),
        (Some(7), NativeAgg::Sum),
        (Some(7), NativeAgg::Min),
        (Some(7), NativeAgg::Max),
        (Some(7), NativeAgg::Avg),
    ]
    .into_iter()
    .map(|(column, agg)| table.aggregate_native(snapshot, column, &[], agg).unwrap())
    .collect()
}

/// The scan-side aggregate computation over the engine's visible rows — the
/// same row stream the SQL layer's fallback plan scans when
/// `aggregate_native` declines.
fn fallback_aggregates(db: &Database, watermark: Epoch) -> Vec<NativeAggResult> {
    let handle = db.table("items").unwrap();
    let rows = handle.lock().visible_rows(Snapshot::at(watermark)).unwrap();
    let mut count = 0_u64;
    let mut sum = 0_i64;
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for row in &rows {
        count += 1;
        match row.columns.get(&7) {
            Some(Value::Int64(value)) => {
                sum += *value;
                min = min.min(*value);
                max = max.max(*value);
            }
            other => panic!("unexpected score column: {other:?}"),
        }
    }
    vec![
        NativeAggResult::Count(count),
        NativeAggResult::Int(sum),
        NativeAggResult::Int(min),
        NativeAggResult::Int(max),
        NativeAggResult::Float(sum as f64 / count as f64),
    ]
}

fn retrieve_ids(db: &Database, retriever: &Retriever) -> Vec<(i64, RetrieverScore)> {
    let id_of = rowid_to_id(db);
    let handle = db.table("items").unwrap();
    let hits = handle
        .lock()
        .retrieve(retriever)
        .unwrap()
        .into_iter()
        .map(|hit| (id_of[&hit.row_id.0], hit.score))
        .collect();
    hits
}

fn query_ids(db: &Database, condition: Condition) -> Vec<i64> {
    let id_of = rowid_to_id(db);
    let handle = db.table("items").unwrap();
    let mut ids: Vec<i64> = handle
        .lock()
        .query(&Query::new().and(condition))
        .unwrap()
        .into_iter()
        .map(|row| id_of[&row.row_id.0])
        .collect();
    ids.sort_unstable();
    ids
}

#[test]
fn ai_and_sql_results_match_standalone_at_the_same_snapshot() {
    let standalone_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let standalone = seed_standalone(standalone_dir.path());
    let replica = seed_replica(replica_dir.path(), 20);

    // Same snapshot: the committed watermarks agree on both engines.
    let watermark = standalone.visible_epoch();
    assert_eq!(watermark, Epoch(3));
    assert_eq!(replica.visible_epoch(), watermark);
    assert_eq!(replica.catalog_version(), standalone.catalog_version());

    // Same data: every visible row, its commit epoch, and its column values.
    assert_eq!(
        content_by_id(&standalone, watermark),
        content_by_id(&replica, watermark)
    );
    // The engine's live-count surface agrees on both shapes.
    let standalone_count = standalone.table("items").unwrap().lock().count();
    assert_eq!(standalone_count, 6);
    assert_eq!(
        standalone_count,
        replica.table("items").unwrap().lock().count()
    );

    // ANN top-k: identical (id, score) sequences; the crafted distances make
    // the order fully deterministic (0, 1, 2 hamming).
    let ann = Retriever::Ann {
        column_id: 2,
        query: vec![1.0; 8],
        k: 3,
    };
    let standalone_ann = retrieve_ids(&standalone, &ann);
    let expected_ann = vec![
        (1, RetrieverScore::AnnHammingDistance(0)),
        (2, RetrieverScore::AnnHammingDistance(1)),
        (3, RetrieverScore::AnnHammingDistance(2)),
    ];
    assert_eq!(standalone_ann, expected_ann);
    assert_eq!(standalone_ann, retrieve_ids(&replica, &ann));

    // Sparse top-k: dot products 6, 5, 4 on both sides.
    let sparse = Retriever::Sparse {
        column_id: 3,
        query: vec![(1, 1.0)],
        k: 3,
    };
    let standalone_sparse = retrieve_ids(&standalone, &sparse);
    let expected_sparse = vec![
        (1, RetrieverScore::SparseDotProduct(6.0)),
        (2, RetrieverScore::SparseDotProduct(5.0)),
        (3, RetrieverScore::SparseDotProduct(4.0)),
    ];
    assert_eq!(standalone_sparse, expected_sparse);
    assert_eq!(standalone_sparse, retrieve_ids(&replica, &sparse));

    // MinHash top-k: LSH recall is approximate, so the gate asserts exact
    // equality between the engines (the deterministic point), plus the
    // standalone baseline's shape: best match first, every hit one of the
    // three crafted similar sets.
    let minhash = Retriever::MinHash {
        column_id: 4,
        members: ["a", "b", "c", "d"]
            .into_iter()
            .map(|member| SetMember::String(member.into()))
            .collect(),
        k: 3,
    };
    let standalone_minhash = retrieve_ids(&standalone, &minhash);
    assert_eq!(standalone_minhash[0].0, 1);
    assert!(standalone_minhash.len() >= 2);
    assert!(standalone_minhash
        .iter()
        .all(|(id, _)| (1..=3).contains(id)));
    assert_eq!(standalone_minhash, retrieve_ids(&replica, &minhash));

    // Boolean index queries: bitmap equality and FM substring resolve to the
    // same id sets on both engines.
    for (condition, expected) in [
        (
            Condition::BitmapEq {
                column_id: 5,
                value: b"alpha".to_vec(),
            },
            vec![1, 3, 6],
        ),
        (
            Condition::FmContains {
                column_id: 6,
                pattern: b"needle".to_vec(),
            },
            vec![1, 3, 5],
        ),
    ] {
        let standalone_ids = query_ids(&standalone, condition.clone());
        assert_eq!(
            standalone_ids, expected,
            "standalone baseline for {condition:?}"
        );
        assert_eq!(standalone_ids, query_ids(&replica, condition));
    }

    // SQL-surface aggregates. Expected values over score = 10..60.
    let expected_aggregates = vec![
        NativeAggResult::Count(6),
        NativeAggResult::Int(210),
        NativeAggResult::Int(10),
        NativeAggResult::Int(60),
        NativeAggResult::Float(35.0),
    ];
    // Replicas remain read-only (no local flush); applied rows live in the
    // overlay. `aggregate_native` still answers via the visible-row fallback
    // when there is no sorted run — same results as the scan-side plan.
    assert!(matches!(
        replica.table("items").unwrap().lock().flush(),
        Err(MongrelError::ReadOnlyReplica)
    ));
    let expected_native: Vec<Option<NativeAggResult>> =
        expected_aggregates.iter().cloned().map(Some).collect();
    assert_eq!(native_aggregates(&replica, watermark), expected_native);
    // Standalone after flush uses the sorted-run pushdown path.
    standalone.table("items").unwrap().lock().flush().unwrap();
    assert_eq!(native_aggregates(&standalone, watermark), expected_native);
    assert_eq!(
        fallback_aggregates(&replica, watermark),
        expected_aggregates
    );
    assert_eq!(
        fallback_aggregates(&standalone, watermark),
        expected_aggregates
    );
}

// ---------------------------------------------------------------------------
// (3) Storage-mode open gate matrix (spec section 5.3).
// ---------------------------------------------------------------------------

/// Writes the `_meta/storage-mode` frame a server-owned standalone database
/// carries. The frame format (`storage_mode.rs`) is
/// `MAGIC "MMODE001" | sha256(body) | body` with
/// `body = format version u16 LE | mode tag` (1 = ServerOwnedStandalone); the
/// server's own create path writes it through the durable root, and this
/// fixture mirrors that byte-for-byte.
fn write_server_owned_marker(root: &Path) {
    let mut body = STORAGE_MODE_FORMAT_VERSION.to_le_bytes().to_vec();
    body.push(1);
    let hash = Sha256::digest(&body);
    let mut frame = b"MMODE001".to_vec();
    frame.extend_from_slice(&hash);
    frame.extend_from_slice(&body);
    std::fs::write(root.join("_meta").join(STORAGE_MODE_FILENAME), frame).unwrap();
}

#[test]
fn storage_mode_open_gate_matrix() {
    let dir = tempfile::tempdir().unwrap();

    // ClusterReplica: rejected by every normal open, read-only through the
    // offline validator, and open for the cluster runtime under exactly its
    // own identity.
    let replica_root = dir.path().join("replica-db");
    let replica = build_simple_replica(&replica_root, 30);
    drop(replica);

    let error = Database::open(&replica_root).unwrap_err();
    assert!(
        error.to_string().contains("cluster node runtime"),
        "unexpected error: {error}"
    );
    let error = Database::open_with_options(&replica_root, OpenOptions::default()).unwrap_err();
    assert!(
        error.to_string().contains("cluster node runtime"),
        "unexpected error: {error}"
    );
    assert_eq!(
        storage_mode::read_at(&replica_root).unwrap(),
        Some(cluster_mode(30)),
        "rejected opens never disturb the marker"
    );

    let offline = Database::open_with_options(
        &replica_root,
        OpenOptions::default().with_offline_validation(true),
    )
    .unwrap();
    assert!(offline.is_read_only_replica());
    assert_eq!(visible_ids(&offline, "items"), vec![10, 20, 30, 40]);
    assert!(matches!(
        offline.create_table("nope", simple_schema()),
        Err(MongrelError::ReadOnlyReplica)
    ));
    assert_eq!(
        offline.storage_mode().unwrap(),
        Some(cluster_mode(30)),
        "offline validation leaves the marker as found"
    );
    drop(offline);

    let runtime = Database::open_cluster_replica(&replica_root, &cluster_mode(30)).unwrap();
    assert!(runtime.is_read_only_replica());
    assert_eq!(visible_ids(&runtime, "items"), vec![10, 20, 30, 40]);
    drop(runtime);
    // A mismatched identity fails closed.
    let wrong = StorageMode::ClusterReplica {
        cluster_id: cluster_id(),
        node_id: node_id(99),
        database_id: database_id(),
    };
    assert!(Database::open_cluster_replica(&replica_root, &wrong).is_err());

    // ServerOwnedStandalone: functionally standalone — embedded opens succeed
    // and stay writable (the owning server merely holds the lock).
    let server_root = dir.path().join("server-db");
    let server = Database::create(&server_root).unwrap();
    server.create_table("items", simple_schema()).unwrap();
    drop(server);
    write_server_owned_marker(&server_root);
    let server = Database::open(&server_root).unwrap();
    assert_eq!(
        server.storage_mode().unwrap(),
        Some(StorageMode::ServerOwnedStandalone)
    );
    server
        .transaction(|txn| {
            txn.put("items", vec![(1, Value::Int64(7))])?;
            Ok(())
        })
        .unwrap();
    assert_eq!(visible_ids(&server, "items"), vec![7]);
    assert_eq!(
        storage_mode::read_at(&server_root).unwrap(),
        Some(StorageMode::ServerOwnedStandalone)
    );
    drop(server);

    // Legacy databases without a marker open as Standalone, and the marker
    // is backfilled on first open (purely additive).
    let legacy_root = dir.path().join("legacy-db");
    let legacy = Database::create(&legacy_root).unwrap();
    legacy.create_table("items", simple_schema()).unwrap();
    drop(legacy);
    std::fs::remove_file(legacy_root.join("_meta").join(STORAGE_MODE_FILENAME)).unwrap();
    assert_eq!(storage_mode::read_at(&legacy_root).unwrap(), None);
    let legacy = Database::open(&legacy_root).unwrap();
    assert_eq!(
        legacy.storage_mode().unwrap(),
        Some(StorageMode::Standalone)
    );
    legacy
        .transaction(|txn| {
            txn.put("items", vec![(1, Value::Int64(9))])?;
            Ok(())
        })
        .unwrap();
    assert_eq!(visible_ids(&legacy, "items"), vec![9]);
    assert_eq!(
        storage_mode::read_at(&legacy_root).unwrap(),
        Some(StorageMode::Standalone),
        "first open backfills the marker"
    );
}
