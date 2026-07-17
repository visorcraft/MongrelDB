//! Replicated-mode apply payloads and the engine snapshot image (spec
//! sections 4.4, 11.5; Stage 2E).
//!
//! In replicated mode the consensus log is the single commit authority (spec
//! section 4.4, ADR-0002): the leader stages commands, the group commits them,
//! and every replica applies the identical bytes deterministically. This
//! module owns the wire contracts the apply sink decodes:
//!
//! - [`ReplicatedTxnPayload`]: the staged records of one committed
//!   transaction (`command_type == COMMAND_TYPE_TRANSACTION`). The sink
//!   replays them through [`Database::apply_replicated_records`] — the same
//!   logic the WAL recovery path uses.
//! - `Catalog` envelopes carry one [`crate::catalog_cmds::CatalogCommandRecord`]
//!   (`command_type == COMMAND_TYPE_CATALOG_COMMAND`) and route through
//!   [`Database::apply_replicated_catalog_command`].
//! - `Maintenance` and `Noop` commands are documented no-ops for applied
//!   state: maintenance commands drive node-local actions owned by the
//!   cluster runtime (membership, decommission), never engine state.
//!
//! [`EngineSnapshot`] is the sink's snapshot payload (spec section 11.5): the
//! group id, the last included term/index, the catalog checkpoint, the MVCC
//! snapshot epoch, the table/run manifest with the required run/index files
//! and their hashes, and the format versions. Install never writes over live
//! state: the image is staged in a sibling directory, verified (hashes,
//! versions, semantic open), and only then atomically swapped with the live
//! root through the same rename idiom as
//! [`crate::replication::ReplicationSnapshot::install_validated`].

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use mongreldb_log::commit_log::LogPosition;
use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{ClusterId, DatabaseId, NodeId, RaftGroupId};

use crate::storage_mode::StorageMode;
use crate::{MongrelError, Result};

/// [`mongreldb_log::CommandEnvelope::command_type`] for one replicated
/// catalog command (a [`crate::catalog_cmds::CatalogCommandRecord`] payload).
/// `1` is the transaction command (`crate::commit_log::COMMAND_TYPE_TRANSACTION`);
/// discriminants are never reused (spec section 9.3).
pub const COMMAND_TYPE_CATALOG_COMMAND: u32 = 2;

/// [`mongreldb_log::CommandEnvelope::command_type`] reserved for replicated
/// maintenance commands. Maintenance commands are node-runtime directives and
/// documented no-ops for engine applied state this wave.
pub const COMMAND_TYPE_MAINTENANCE: u32 = 3;

// ---------------------------------------------------------------------------
// Replicated transaction payload
// ---------------------------------------------------------------------------

/// The only payload format version this build reads and writes.
pub const REPLICATED_TXN_FORMAT_VERSION: u16 = 1;

/// The staged record sequence of one committed transaction (spec section
/// 4.4): exactly the records the leader's commit sequencer appended for the
/// transaction — data ops, `Op::CommitTimestamp`, and one trailing
/// `Op::TxnCommit` carrying the leader-assigned commit epoch. Replicas apply
/// the identical bytes, so applied state diverges nowhere.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicatedTxnPayload {
    /// Payload format version; must equal [`REPLICATED_TXN_FORMAT_VERSION`].
    pub version: u16,
    /// The transaction's complete record sequence.
    pub records: Vec<crate::wal::Record>,
}

impl ReplicatedTxnPayload {
    /// Wraps `records` at the current format version.
    pub fn new(records: Vec<crate::wal::Record>) -> Self {
        Self {
            version: REPLICATED_TXN_FORMAT_VERSION,
            records,
        }
    }

    /// Serializes deterministically (bincode over the versioned struct).
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    /// Decodes, failing closed on an unknown format version.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let payload: Self = bincode::deserialize(bytes)?;
        if payload.version != REPLICATED_TXN_FORMAT_VERSION {
            return Err(MongrelError::UnsupportedStorageVersion {
                component: "replicated transaction payload",
                found: payload.version,
                supported: REPLICATED_TXN_FORMAT_VERSION,
            });
        }
        Ok(payload)
    }
}

// ---------------------------------------------------------------------------
// Engine snapshot (spec section 11.5)
// ---------------------------------------------------------------------------

/// The only engine-snapshot format version this build reads and writes.
pub const ENGINE_SNAPSHOT_FORMAT_VERSION: u16 = 1;

/// One required file of an [`EngineSnapshot`] (run, index, catalog checkpoint,
/// WAL segment): its database-relative path, content hash, and bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineSnapshotFile {
    /// Database-relative path (validated: plain relative, no `..`).
    pub path: PathBuf,
    /// SHA-256 over `data`.
    pub sha256: [u8; 32],
    /// File content.
    pub data: Vec<u8>,
}

/// One table's entry in the snapshot's table/run manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineSnapshotTable {
    /// Catalog table id.
    pub table_id: u64,
    /// Catalog name.
    pub name: String,
    /// Rows visible at the snapshot's MVCC epoch.
    pub visible_rows: u64,
}

/// The engine apply sink's snapshot payload (spec section 11.5): applied
/// state at one log boundary, plus everything needed to verify and
/// semantically validate it before it replaces a replica's state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineSnapshot {
    /// Payload format version; must equal [`ENGINE_SNAPSHOT_FORMAT_VERSION`].
    pub version: u16,
    /// The consensus group this snapshot belongs to.
    pub group_id: RaftGroupId,
    /// Last log position whose effects the image includes.
    pub last_included: LogPosition,
    /// Commit timestamp recorded at `last_included`, when any command has
    /// been applied.
    pub commit_ts: Option<HlcTimestamp>,
    /// MVCC snapshot epoch (the core's visible watermark at capture).
    pub epoch: u64,
    /// Owning cluster (identity of the replicated database).
    pub cluster_id: ClusterId,
    /// Replicated logical database.
    pub database_id: DatabaseId,
    /// Catalog command version at capture (S1F-001).
    pub catalog_version: u64,
    /// Table/run manifest: every live table and its visible row count.
    pub tables: Vec<EngineSnapshotTable>,
    /// Every required file (catalog checkpoint, manifests, run/index files,
    /// WAL), hashed.
    pub files: Vec<EngineSnapshotFile>,
    /// On-disk WAL format version at capture (verified on install).
    pub wal_format: u16,
    /// Storage-mode marker format version at capture (verified on install).
    pub storage_mode_format: u16,
}

impl EngineSnapshot {
    /// Captures the live core's applied state. The caller (the apply sink)
    /// holds the apply mutex, so no replicated command is applying and the
    /// read-only core is quiescent: the copied files and the recorded
    /// position describe one log boundary.
    pub fn capture(
        db: &crate::Database,
        group_id: RaftGroupId,
        last_included: LogPosition,
        commit_ts: Option<HlcTimestamp>,
    ) -> Result<Self> {
        let mode = db.storage_mode()?.ok_or_else(|| {
            MongrelError::Other("engine snapshot capture before marker write".into())
        })?;
        let (cluster_id, _node_id, database_id) = mode.cluster_identity().ok_or_else(|| {
            MongrelError::InvalidArgument(format!(
                "engine snapshots capture cluster replicas, got mode {mode:?}"
            ))
        })?;
        let epoch = db.visible_epoch();
        let snapshot = crate::epoch::Snapshot::at(epoch);
        let mut tables = Vec::new();
        for name in db.table_names() {
            let table_id = db.table_id(&name)?;
            let handle = db.table(&name)?;
            let visible_rows = handle.lock().visible_rows(snapshot)?.len() as u64;
            tables.push(EngineSnapshotTable {
                table_id,
                name,
                visible_rows,
            });
        }
        tables.sort_by_key(|table| table.table_id);
        let files = crate::replication::capture_files(db.root())?
            .into_iter()
            .map(|file| {
                let sha256: [u8; 32] = Sha256::digest(&file.data).into();
                Ok(EngineSnapshotFile {
                    path: file.path,
                    sha256,
                    data: file.data,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            version: ENGINE_SNAPSHOT_FORMAT_VERSION,
            group_id,
            last_included,
            commit_ts,
            epoch: epoch.0,
            cluster_id,
            database_id,
            catalog_version: db.catalog_version(),
            tables,
            files,
            wal_format: crate::wal::WAL_VERSION,
            storage_mode_format: crate::storage_mode::STORAGE_MODE_FORMAT_VERSION,
        })
    }

    /// Serializes deterministically (bincode over the versioned struct).
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    /// Decodes, failing closed on an unknown format version.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let snapshot: Self = bincode::deserialize(bytes)?;
        if snapshot.version != ENGINE_SNAPSHOT_FORMAT_VERSION {
            return Err(MongrelError::UnsupportedStorageVersion {
                component: "engine snapshot",
                found: snapshot.version,
                supported: ENGINE_SNAPSHOT_FORMAT_VERSION,
            });
        }
        Ok(snapshot)
    }

    /// Spec step "verify hashes and versions": the group and database
    /// identity must match this replica, every file hash must hold, and the
    /// recorded format versions must be readable by this build.
    pub fn validate(
        &self,
        group_id: &RaftGroupId,
        cluster_id: &ClusterId,
        database_id: &DatabaseId,
    ) -> Result<()> {
        if &self.group_id != group_id {
            return Err(MongrelError::InvalidArgument(format!(
                "engine snapshot group {:?} does not match this replica's group {:?}",
                self.group_id, group_id
            )));
        }
        if &self.cluster_id != cluster_id || &self.database_id != database_id {
            return Err(MongrelError::InvalidArgument(
                "engine snapshot database identity does not match this replica".into(),
            ));
        }
        if self.wal_format != crate::wal::WAL_VERSION {
            return Err(MongrelError::UnsupportedStorageVersion {
                component: "wal",
                found: self.wal_format,
                supported: crate::wal::WAL_VERSION,
            });
        }
        if self.storage_mode_format != crate::storage_mode::STORAGE_MODE_FORMAT_VERSION {
            return Err(MongrelError::UnsupportedStorageVersion {
                component: "storage-mode marker",
                found: self.storage_mode_format,
                supported: crate::storage_mode::STORAGE_MODE_FORMAT_VERSION,
            });
        }
        let mut seen = std::collections::HashSet::new();
        for file in &self.files {
            crate::replication::validate_relative_path(&file.path)?;
            if !seen.insert(file.path.clone()) {
                return Err(MongrelError::InvalidArgument(format!(
                    "duplicate engine snapshot path {:?}",
                    file.path
                )));
            }
            let digest: [u8; 32] = Sha256::digest(&file.data).into();
            if digest != file.sha256 {
                return Err(MongrelError::Other(format!(
                    "engine snapshot file {:?} failed its content hash",
                    file.path
                )));
            }
        }
        Ok(())
    }

    /// Spec step "download to staging": write the image into the (fresh,
    /// empty) `staging` directory, fsyncing every file. The storage-mode
    /// marker is rewritten with the LOCAL node identity: the image came from
    /// a peer replica of the same database, and a marker must name its owner.
    pub fn stage_into(&self, staging: &Path, node_id: NodeId) -> Result<()> {
        let marker_relative =
            Path::new(crate::database::META_DIR).join(crate::storage_mode::STORAGE_MODE_FILENAME);
        for file in &self.files {
            crate::replication::validate_relative_path(&file.path)?;
            if file.path == marker_relative {
                // Rewritten below with the local identity.
                continue;
            }
            let path = staging.join(&file.path);
            let parent = path.parent().expect("validated file has parent");
            crate::durable_file::create_directory_all(parent)?;
            let mut output = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)?;
            std::io::Write::write_all(&mut output, &file.data)?;
            output.sync_all()?;
            crate::durable_file::sync_directory(parent)?;
        }
        // Every mounted table requires `_runs` during semantic open (mirrors
        // the replication install path).
        for table in &self.tables {
            crate::durable_file::create_directory_all(
                &staging
                    .join(crate::database::TABLES_DIR)
                    .join(table.table_id.to_string())
                    .join("_runs"),
            )?;
        }
        if !staging.join(crate::catalog::CATALOG_FILENAME).is_file() {
            return Err(MongrelError::InvalidArgument(
                "engine snapshot has no CATALOG".into(),
            ));
        }
        let durable_stage = crate::durable_file::DurableRoot::open(staging)?;
        crate::storage_mode::rewrite(
            &durable_stage,
            &StorageMode::ClusterReplica {
                cluster_id: self.cluster_id,
                node_id,
                database_id: self.database_id,
            },
        )?;
        crate::durable_file::sync_directory(staging)?;
        Ok(())
    }

    /// Spec step "open and semantically validate": open the staged image
    /// through the offline-validation API (read-only, any storage mode) and
    /// confirm every manifest table mounts with the recorded visible row
    /// count at the snapshot epoch.
    pub fn validate_staged(&self, staging: &Path) -> Result<()> {
        let db = crate::Database::open_offline_validation(staging)?;
        let snapshot = crate::epoch::Snapshot::at(crate::epoch::Epoch(self.epoch));
        for table in &self.tables {
            let handle = db.table(&table.name).map_err(|error| {
                MongrelError::Other(format!(
                    "staged engine snapshot is missing table {:?}: {error}",
                    table.name
                ))
            })?;
            let rows = handle.lock().visible_rows(snapshot)?.len() as u64;
            if rows != table.visible_rows {
                return Err(MongrelError::Other(format!(
                    "staged engine snapshot table {:?} has {rows} visible rows at epoch {}, expected {}",
                    table.name, self.epoch, table.visible_rows
                )));
            }
        }
        Ok(())
    }

    /// Spec steps "pause apply → atomically replace → resume → remove old
    /// state". The apply mutex the caller holds is the pause. The staged,
    /// validated image is swapped with the live root through the rename idiom
    /// of [`crate::replication::ReplicationSnapshot::install_validated`] —
    /// never installed over live state: the live core is shut down first
    /// (refusing with [`MongrelError::Conflict`] while other owners hold it,
    /// leaving `live` untouched), the old root is renamed aside and retained
    /// until success, the replica is reopened as the cluster runtime, and the
    /// old tree is removed. On success `live` holds the reopened database;
    /// on a pre-shutdown failure it is unchanged.
    pub fn install(self, live: &mut Option<Arc<crate::Database>>, node_id: NodeId) -> Result<()> {
        let db = live.as_ref().ok_or_else(|| {
            MongrelError::Other("engine snapshot install without a live database".into())
        })?;
        if Arc::strong_count(db) > 1 {
            return Err(MongrelError::Conflict(
                "engine snapshot install refused over live state: database is busy".into(),
            ));
        }
        let destination = db.root().to_path_buf();
        // Refuse to replace anything but this database's own replica.
        match db.storage_mode()? {
            Some(StorageMode::ClusterReplica {
                cluster_id,
                database_id,
                ..
            }) if cluster_id == self.cluster_id && database_id == self.database_id => {}
            other => {
                return Err(MongrelError::InvalidArgument(format!(
                    "refusing to install an engine snapshot over storage mode {other:?}"
                )));
            }
        }
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let name = destination
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| MongrelError::InvalidArgument("invalid database root".into()))?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let stage = parent.join(format!(
            ".{name}.engine-stage-{}-{nonce}",
            std::process::id()
        ));
        let backup = parent.join(format!(".{name}.engine-old-{}-{nonce}", std::process::id()));
        if stage.exists() || backup.exists() {
            return Err(MongrelError::Conflict(
                "engine snapshot staging path already exists".into(),
            ));
        }
        let result = (|| -> Result<()> {
            crate::durable_file::create_directory(&stage)?;
            self.stage_into(&stage, node_id)?;
            self.validate_staged(&stage)?;
            // Pause live state. The busy check ran above, so the shutdown
            // succeeds; no live file is ever mutated in place.
            let owned = live.take().expect("checked above");
            owned.shutdown()?;
            // Atomically swap the staged tree in (never over live state: the
            // old root is renamed aside first and retained until success).
            if let Err(failure) = crate::replication::rename_entry(&destination, &backup) {
                return Err(failure.error.into());
            }
            if let Err(failure) = crate::replication::rename_entry(&stage, &destination) {
                if let Err(rollback) = crate::replication::rename_entry(&backup, &destination) {
                    return Err(crate::replication::uncertain_install_error(
                        self.epoch,
                        &failure.error,
                        &rollback.error,
                    ));
                }
                return Err(failure.error.into());
            }
            crate::replication::remove_directory(&backup)?;
            Ok(())
        })();
        if let Err(error) = result {
            if stage.exists() {
                let _ = crate::replication::remove_directory(&stage);
            }
            return Err(error);
        }
        let expected = StorageMode::ClusterReplica {
            cluster_id: self.cluster_id,
            node_id,
            database_id: self.database_id,
        };
        let reopened = crate::Database::open_cluster_replica(&destination, &expected)?;
        *live = Some(Arc::new(reopened));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txn_payload_round_trip_and_version_gate() {
        let payload = ReplicatedTxnPayload::new(vec![crate::wal::Record::new(
            crate::epoch::Epoch(1),
            7,
            crate::wal::Op::TxnCommit {
                epoch: 1,
                added_runs: Vec::new(),
            },
        )]);
        let bytes = payload.encode().unwrap();
        let decoded = ReplicatedTxnPayload::decode(&bytes).unwrap();
        assert_eq!(decoded.records.len(), 1);

        let mut corrupt = payload.clone();
        corrupt.version = REPLICATED_TXN_FORMAT_VERSION + 1;
        let bytes = bincode::serialize(&corrupt).unwrap();
        assert!(matches!(
            ReplicatedTxnPayload::decode(&bytes),
            Err(MongrelError::UnsupportedStorageVersion { .. })
        ));
    }

    #[test]
    fn validate_rejects_bad_hash_and_foreign_identity() {
        let snapshot = EngineSnapshot {
            version: ENGINE_SNAPSHOT_FORMAT_VERSION,
            group_id: RaftGroupId::from_bytes([1; 16]),
            last_included: LogPosition { term: 1, index: 2 },
            commit_ts: None,
            epoch: 2,
            cluster_id: ClusterId::from_bytes([2; 16]),
            database_id: DatabaseId::from_bytes([3; 16]),
            catalog_version: 0,
            tables: Vec::new(),
            files: vec![EngineSnapshotFile {
                path: PathBuf::from("CATALOG"),
                sha256: [9; 32],
                data: b"catalog".to_vec(),
            }],
            wal_format: crate::wal::WAL_VERSION,
            storage_mode_format: crate::storage_mode::STORAGE_MODE_FORMAT_VERSION,
        };
        let group = RaftGroupId::from_bytes([1; 16]);
        let cluster = ClusterId::from_bytes([2; 16]);
        let database = DatabaseId::from_bytes([3; 16]);
        // Content hash mismatch fails closed.
        assert!(snapshot.validate(&group, &cluster, &database).is_err());
        let wrong_group = RaftGroupId::from_bytes([9; 16]);
        assert!(snapshot
            .validate(&wrong_group, &cluster, &database)
            .is_err());
        let wrong_database = DatabaseId::from_bytes([9; 16]);
        assert!(snapshot
            .validate(&group, &cluster, &wrong_database)
            .is_err());

        let mut good = snapshot.clone();
        good.files[0].sha256 = Sha256::digest(&good.files[0].data).into();
        good.validate(&group, &cluster, &database).unwrap();
    }
}
