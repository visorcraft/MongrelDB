//! Engine apply sink: binds committed [`ReplicatedCommand`]s to a core
//! `DatabaseCore` opened in `ClusterReplica` storage mode (spec sections 4.4,
//! 5.3, 11.5; Stage 2E).
//!
//! # Apply contract
//!
//! - `Transaction` commands carry a [`ReplicatedTxnPayload`]: the complete
//!   record sequence of one committed transaction, replayed through
//!   `Database::apply_replicated_records` — the **same** apply logic the WAL
//!   recovery path uses. Idempotency comes from the state machine's
//!   applied-command tracking (S2B-004), with the core's visible-epoch
//!   watermark as the crash-window backstop.
//! - `Catalog` commands carry one versioned
//!   [`mongreldb_core::catalog_cmds::CatalogCommandRecord`] and route through
//!   `Database::apply_replicated_catalog_command` (the S1F-001 idempotent
//!   command path).
//! - `Maintenance` and `Noop` commands are documented no-ops for applied
//!   engine state: maintenance commands are node-runtime directives
//!   (membership, decommission orchestration), never engine mutations.
//!
//! The sink is deterministic and has no side effects outside the core's own
//! durable state: every decision (ids, epochs, timestamps) was made by the
//! leader and travels inside the command.
//!
//! # Snapshots (spec section 11.5)
//!
//! [`ApplySink::snapshot`] captures an [`EngineSnapshot`] (group id, last
//! included term/index, catalog checkpoint, MVCC snapshot epoch, table/run
//! manifest, required run/index files, file hashes, format versions).
//! [`ApplySink::install`] stages the image in a sibling directory, verifies
//! hashes and versions, opens and semantically validates it, and only then
//! atomically swaps it with the live root — never installing over live
//! state — before the state machine updates its last-applied metadata.
//!
//! # Directory layout (spec sections 11.5, 12.3; tablet-less)
//!
//! ```text
//! node-data/
//!   groups/
//!     <raft-group-id>/
//!       raft/        log segments, vote, state machine checkpoint + snapshots
//!       db/          the applied database root (ClusterReplica marker)
//! ```
//!
//! Stage 3 adds `tablets/`; this wave's single-group replicated database
//! needs only the group directory and its applied database root.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use mongreldb_core::replicated_apply::{
    EngineSnapshot, ReplicatedTxnPayload, COMMAND_TYPE_CATALOG_COMMAND,
};
use mongreldb_core::storage_mode::StorageMode;
use mongreldb_core::MongrelError;
use mongreldb_log::commit_log::LogPosition;
use mongreldb_log::envelope::CommandEnvelope;
use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{ClusterId, DatabaseId, NodeId, RaftGroupId};

use crate::identity::ReplicatedCommand;
use crate::state_machine::{AppliedCommand, ApplySink, MongrelStateMachine, StateMachineError};
use crate::storage::{MongrelLogStore, StorageConfig};

/// Errors produced by the engine sink factory (apply failures reach the state
/// machine as [`StateMachineError`]).
#[derive(Debug, thiserror::Error)]
pub enum EngineSinkError {
    /// Core engine failure.
    #[error(transparent)]
    Engine(#[from] MongrelError),
    /// State machine failure.
    #[error(transparent)]
    StateMachine(#[from] StateMachineError),
    /// Log storage failure.
    #[error(transparent)]
    Store(#[from] crate::storage::StoreError),
}

fn sink_error(error: MongrelError) -> StateMachineError {
    StateMachineError::Sink(error.to_string())
}

/// The [`ApplySink`] binding one raft group's committed commands to the
/// applied database root of a `ClusterReplica` storage core.
pub struct EngineApplySink {
    db: Option<Arc<mongreldb_core::Database>>,
    db_root: PathBuf,
    group_id: RaftGroupId,
    cluster_id: ClusterId,
    node_id: NodeId,
    database_id: DatabaseId,
    /// Last applied position reported to snapshots (mirrors the state
    /// machine's own checkpoint; kept here so the engine image can carry it).
    last_applied: LogPosition,
    last_commit_ts: Option<HlcTimestamp>,
}

impl EngineApplySink {
    /// Opens (creating if needed) the applied database root at `db_root` with
    /// the `ClusterReplica` marker for `(cluster_id, node_id, database_id)`
    /// and builds the sink over it.
    pub fn open(
        db_root: &Path,
        group_id: RaftGroupId,
        cluster_id: ClusterId,
        node_id: NodeId,
        database_id: DatabaseId,
    ) -> Result<Self, EngineSinkError> {
        let expected = StorageMode::ClusterReplica {
            cluster_id,
            node_id,
            database_id,
        };
        let db = if db_root
            .join(mongreldb_core::catalog::CATALOG_FILENAME)
            .is_file()
        {
            mongreldb_core::Database::open_cluster_replica(db_root, &expected)?
        } else {
            mongreldb_core::Database::create_cluster_replica(
                db_root,
                cluster_id,
                node_id,
                database_id,
            )?
        };
        Ok(EngineApplySink {
            db: Some(Arc::new(db)),
            db_root: db_root.to_path_buf(),
            group_id,
            cluster_id,
            node_id,
            database_id,
            last_applied: LogPosition::ZERO,
            last_commit_ts: None,
        })
    }

    /// Seeds the in-memory apply watermark from the state machine's
    /// persisted `AppliedRecord` after open (review finding **m8**).
    ///
    /// Without this, a restart leaves `last_applied = ZERO` until the next
    /// apply, so the next engine snapshot would embed a wrong
    /// `last_included` / `commit_ts` even though the raft SM watermark is
    /// correct. Callers that open the SM after the sink (e.g.
    /// [`open_engine_group`]) must invoke this once with the loaded record.
    pub fn seed_watermark(
        &mut self,
        last_applied: LogPosition,
        last_commit_ts: Option<HlcTimestamp>,
    ) {
        self.last_applied = last_applied;
        self.last_commit_ts = last_commit_ts;
    }

    /// The live database (for read-path inspection; raft-gated writes only).
    pub fn database(&self) -> Option<Arc<mongreldb_core::Database>> {
        self.db.clone()
    }

    /// The last position the sink reported into a snapshot.
    pub fn last_applied(&self) -> LogPosition {
        self.last_applied
    }

    fn cluster_mode(&self) -> StorageMode {
        StorageMode::ClusterReplica {
            cluster_id: self.cluster_id,
            node_id: self.node_id,
            database_id: self.database_id,
        }
    }

    /// Reopens the applied root after a failed install left the sink without
    /// a live database (whatever tree — old or new — occupies the root is a
    /// valid cluster replica of this database).
    fn reopen(&mut self) -> Result<(), StateMachineError> {
        let db =
            mongreldb_core::Database::open_cluster_replica(&self.db_root, &self.cluster_mode())
                .map_err(sink_error)?;
        self.db = Some(Arc::new(db));
        Ok(())
    }
}

impl ApplySink for EngineApplySink {
    fn apply(&mut self, command: &AppliedCommand) -> Result<(), StateMachineError> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| StateMachineError::Sink("engine sink has no open database".into()))?;
        match &command.command {
            ReplicatedCommand::Transaction(transaction) => {
                transaction.envelope.verify().map_err(|error| {
                    StateMachineError::Corrupt(format!("transaction envelope: {error}"))
                })?;
                if transaction.envelope.command_type
                    != mongreldb_core::commit_log::COMMAND_TYPE_TRANSACTION
                {
                    return Err(StateMachineError::Corrupt(format!(
                        "transaction command_type {} is not COMMAND_TYPE_TRANSACTION",
                        transaction.envelope.command_type
                    )));
                }
                let payload = ReplicatedTxnPayload::decode(&transaction.envelope.payload)
                    .map_err(sink_error)?;
                db.apply_replicated_records(&payload.records)
                    .map_err(sink_error)?;
            }
            ReplicatedCommand::Catalog(catalog) => {
                catalog.envelope.verify().map_err(|error| {
                    StateMachineError::Corrupt(format!("catalog envelope: {error}"))
                })?;
                if catalog.envelope.command_type != COMMAND_TYPE_CATALOG_COMMAND {
                    return Err(StateMachineError::Corrupt(format!(
                        "catalog command_type {} is not COMMAND_TYPE_CATALOG_COMMAND",
                        catalog.envelope.command_type
                    )));
                }
                let record =
                    mongreldb_core::catalog_cmds::decode_command(&catalog.envelope.payload)
                        .map_err(sink_error)?;
                db.apply_replicated_catalog_command(&record)
                    .map_err(sink_error)?;
            }
            // Maintenance commands are node-runtime directives; Noop advances
            // the commit index. Neither touches engine applied state.
            ReplicatedCommand::Maintenance(_) | ReplicatedCommand::Noop => {}
        }
        self.last_applied = command.position;
        if let Some(commit_ts) = command.commit_ts() {
            self.last_commit_ts = Some(commit_ts);
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<u8>, StateMachineError> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| StateMachineError::Sink("engine sink has no open database".into()))?;
        let snapshot =
            EngineSnapshot::capture(db, self.group_id, self.last_applied, self.last_commit_ts)
                .map_err(sink_error)?;
        snapshot.encode().map_err(sink_error)
    }

    fn install(&mut self, data: &[u8]) -> Result<(), StateMachineError> {
        let snapshot = EngineSnapshot::decode(data).map_err(sink_error)?;
        snapshot
            .validate(&self.group_id, &self.cluster_id, &self.database_id)
            .map_err(sink_error)?;
        let last_included = snapshot.last_included;
        let commit_ts = snapshot.commit_ts;
        // The slot passes through: on success it holds the reopened database,
        // on a pre-shutdown refusal the live database is untouched.
        match snapshot.install(&mut self.db, self.node_id) {
            Ok(()) => {
                self.last_applied = last_included;
                self.last_commit_ts = commit_ts;
                Ok(())
            }
            Err(error) => {
                // Keep the sink functional if the shutdown did happen (a
                // post-shutdown failure): reopen whatever tree survived at
                // the applied root (the swap is atomic — old or new, never a
                // mix) so later applies and installs can proceed.
                if self.db.is_none() {
                    self.reopen()?;
                }
                Err(sink_error(error))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Factory: single-group replicated database layout (spec 11.5 / 12.3, no tablets)
// ---------------------------------------------------------------------------

/// Static layout + identity of one single-group replicated database member.
#[derive(Debug, Clone)]
pub struct EngineGroupConfig {
    /// The node's local data root (`node-data`).
    pub node_data: PathBuf,
    /// The raft group owning this database's committed log order.
    pub group_id: RaftGroupId,
    /// Owning cluster identity.
    pub cluster_id: ClusterId,
    /// This node's durable id.
    pub node_id: NodeId,
    /// The replicated logical database.
    pub database_id: DatabaseId,
    /// Durable log storage configuration.
    pub storage: StorageConfig,
    /// Bound on the apply idempotency set (S2B-004).
    pub idempotency_retention: usize,
}

impl EngineGroupConfig {
    /// Required identities; storage and retention default to production
    /// values ([`StorageConfig::default`],
    /// [`crate::state_machine::DEFAULT_IDEMPOTENCY_RETENTION`]).
    pub fn new(
        node_data: PathBuf,
        group_id: RaftGroupId,
        cluster_id: ClusterId,
        node_id: NodeId,
        database_id: DatabaseId,
    ) -> Self {
        EngineGroupConfig {
            node_data,
            group_id,
            cluster_id,
            node_id,
            database_id,
            storage: StorageConfig::default(),
            idempotency_retention: crate::state_machine::DEFAULT_IDEMPOTENCY_RETENTION,
        }
    }

    /// `<node-data>/groups/<raft-group-id>` — the group directory handed to
    /// [`MongrelLogStore::open`], [`MongrelStateMachine::open`], and
    /// [`crate::group::GroupConfig::dir`].
    pub fn group_dir(&self) -> PathBuf {
        self.node_data.join("groups").join(self.group_id.to_hex())
    }

    /// `<node-data>/groups/<raft-group-id>/db` — the applied database root.
    pub fn database_root(&self) -> PathBuf {
        self.group_dir().join("db")
    }
}

/// The opened parts of one single-group replicated database member.
pub struct EngineGroupParts {
    /// The checksummed durable raft log store at `<group dir>/raft/`.
    pub log_store: MongrelLogStore,
    /// The apply state machine, with the engine sink installed.
    pub state_machine: MongrelStateMachine,
    /// Shared handle to the engine sink (read-path inspection, tests).
    pub sink: Arc<Mutex<EngineApplySink>>,
}

/// Opens the applied database root and returns the engine sink for
/// [`crate::group::ConsensusGroup::create`] (which opens the log store and
/// state machine itself). Use this when driving a full `ConsensusGroup`;
/// [`open_engine_group`] when wiring the raft storage traits directly.
pub fn open_engine_sink(
    config: &EngineGroupConfig,
) -> Result<Arc<Mutex<EngineApplySink>>, EngineSinkError> {
    let db_root = config.database_root();
    std::fs::create_dir_all(&db_root).map_err(MongrelError::Io)?;
    let sink = EngineApplySink::open(
        &db_root,
        config.group_id,
        config.cluster_id,
        config.node_id,
        config.database_id,
    )?;
    Ok(Arc::new(Mutex::new(sink)))
}

// ---------------------------------------------------------------------------
// Leader-side command construction (spec section 11.3 step 3)
// ---------------------------------------------------------------------------

/// Builds the replicated transaction command envelope for one committed
/// transaction's staged WAL records — the leader side of the apply contract
/// (spec section 11.3 step 3, "leader constructs transaction command";
/// review finding M2). The caller proposes the envelope through its group
/// (which stamps the commit timestamp), e.g.
/// [`crate::group::ConsensusGroup::propose`].
///
/// A commit that spilled staged its rows as logical `Op::SpilledRows`
/// records plus leader-local run links (`added_runs`) in its commit marker
/// (spec section 8.5). Run files exist only on the leader, so the payload
/// proposed to the group must carry the spilled rows as logical row records
/// instead: the records pass through
/// [`mongreldb_core::database::translate_records_for_replication`], which
/// re-tags the spill payloads as ordinary `Op::Put`s and strips the run
/// links, so no `added_runs` ever reaches a replica apply. The standalone
/// commit path is untouched — the leader's own WAL keeps the original
/// sequence byte-identical.
///
/// A sequence that cannot be represented self-contained (malformed, or a
/// linked run whose rows are missing from the logical records) is rejected
/// here, at proposal construction — a quorum-committed entry that no replica
/// can apply would otherwise wedge the group's apply stream.
pub fn build_transaction_envelope(
    command_id: [u8; 16],
    records: &[mongreldb_core::wal::Record],
) -> Result<CommandEnvelope, EngineSinkError> {
    let translated = mongreldb_core::database::translate_records_for_replication(records)?;
    let payload = ReplicatedTxnPayload::new(translated).encode()?;
    Ok(CommandEnvelope::new(
        mongreldb_core::commit_log::COMMAND_TYPE_TRANSACTION,
        command_id,
        payload,
    ))
}

/// Test-support helpers for downstream integration tests
/// (`mongreldb-cluster`'s distributed-transaction engine binding drives
/// two-phase commit across engine-backed tablet groups, and the cluster
/// crate has no `mongreldb-core` edge to build row payloads with). These
/// mirror this module's own fixtures; they are not a public API.
#[doc(hidden)]
pub mod testing {
    use super::*;
    use mongreldb_core::catalog_cmds::{
        CatalogCommand, CatalogCommandRecord, CATALOG_COMMAND_FORMAT_VERSION,
    };
    use mongreldb_core::memtable::{Row, Value};
    use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
    use mongreldb_core::{Epoch, RowId};

    /// The single-column i64 primary-key schema the fixtures use.
    pub fn i64_schema() -> Schema {
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

    /// A catalog command envelope creating `name` with the i64 schema.
    pub fn create_i64_table_envelope(
        seq: u64,
        name: &str,
        catalog_version: u64,
    ) -> CommandEnvelope {
        let record = CatalogCommandRecord {
            version: CATALOG_COMMAND_FORMAT_VERSION,
            catalog_version,
            command: CatalogCommand::CreateTable {
                name: name.to_string(),
                schema: i64_schema(),
                created_epoch: 1,
            },
        };
        let payload = mongreldb_core::catalog_cmds::encode_command(&record).unwrap();
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&seq.to_le_bytes());
        CommandEnvelope::new(COMMAND_TYPE_CATALOG_COMMAND, id, payload)
    }

    /// Encodes staged i64 puts (one row per value, row id = value as u64,
    /// column 1) as one `StagedTxnWrite::Put` payload for a distributed
    /// transaction write intent.
    pub fn staged_put_i64(table_id: u64, values: &[i64]) -> Vec<u8> {
        let rows: Vec<Row> = values
            .iter()
            .map(|value| {
                Row::new(RowId(*value as u64), Epoch(0)).with_column(1, Value::Int64(*value))
            })
            .collect();
        mongreldb_core::database::StagedTxnWrite::Put {
            table_id,
            rows: bincode::serialize(&rows).unwrap(),
        }
        .encode()
        .unwrap()
    }

    /// Encodes a staged i64 delete as one `StagedTxnWrite::Delete` payload.
    pub fn staged_delete_i64(table_id: u64, row_ids: &[u64]) -> Vec<u8> {
        mongreldb_core::database::StagedTxnWrite::Delete {
            table_id,
            row_ids: row_ids.to_vec(),
        }
        .encode()
        .unwrap()
    }

    /// The sorted visible i64 values of `table` (column 1) on `db`.
    pub fn visible_i64s(db: &mongreldb_core::Database, table: &str) -> Vec<i64> {
        let handle = db.table(table).unwrap();
        let rows = handle
            .lock()
            .visible_rows(mongreldb_core::Snapshot::at(Epoch(u64::MAX)))
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
}

/// Constructs the durable storage parts of one single-group replicated
/// database member (spec sections 11.5, 12.3 minus tablets): the
/// [`MongrelLogStore`] and a [`MongrelStateMachine`] whose apply sink drives
/// the applied database root at `<group dir>/db`. Opens — creating with the
/// `ClusterReplica` marker if needed — the database root.
pub fn open_engine_group(config: &EngineGroupConfig) -> Result<EngineGroupParts, EngineSinkError> {
    let sink = open_engine_sink(config)?;
    let group_dir = config.group_dir();
    let log_store = MongrelLogStore::open(&group_dir, config.storage.clone())?;
    let state_machine = MongrelStateMachine::open(
        &group_dir,
        sink.clone() as Arc<Mutex<dyn ApplySink>>,
        config.idempotency_retention,
    )?;
    // Review m8: recover the engine-side watermark from the SM's durable
    // applied record so the next snapshot does not advertise ZERO.
    let record = state_machine
        .applied_record()
        .map_err(|e| MongrelError::Other(e.to_string()))?;
    sink.lock()
        .map_err(|_| MongrelError::Other("engine sink lock poisoned".into()))?
        .seed_watermark(record.position(), record.last_applied_commit_ts);
    Ok(EngineGroupParts {
        log_store,
        state_machine,
        sink,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::group::{ConsensusGroup, GroupConfig};
    use crate::identity::CommandKind;
    use crate::network::InMemoryTransport;
    use mongreldb_core::catalog_cmds::{
        CatalogCommand, CatalogCommandRecord, CATALOG_COMMAND_FORMAT_VERSION,
    };
    use mongreldb_core::memtable::{Row, Value};
    use mongreldb_core::replicated_apply::ReplicatedTxnPayload;
    use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
    use mongreldb_core::wal::{Op, Record};
    use mongreldb_core::{Epoch, RowId};
    use mongreldb_log::commit_log::ExecutionControl;
    use mongreldb_log::envelope::CommandEnvelope;
    use openraft::BasicNode;
    use std::collections::BTreeMap;
    use std::time::Duration;

    const LEADER_TIMEOUT: Duration = Duration::from_secs(10);

    /// Replicas of one database share the cluster/database identity; only
    /// the node identity differs.
    fn ids(node_seed: u8) -> (ClusterId, NodeId, DatabaseId) {
        (
            ClusterId::from_bytes([1; 16]),
            NodeId::from_bytes([node_seed; 16]),
            DatabaseId::from_bytes([3; 16]),
        )
    }

    fn raft_group_id() -> RaftGroupId {
        RaftGroupId::from_bytes([7; 16])
    }

    fn group_config(node: u64, dir: &Path) -> GroupConfig {
        let mut config = GroupConfig::new("engine-test", node, dir.to_path_buf());
        config.heartbeat_interval = Duration::from_millis(50);
        config.election_timeout_min = Duration::from_millis(150);
        config.election_timeout_max = Duration::from_millis(300);
        config.install_snapshot_timeout = Duration::from_millis(1_000);
        config
    }

    fn engine_config(node_data: &Path, node_seed: u8) -> EngineGroupConfig {
        let (cluster_id, node_id, database_id) = ids(node_seed);
        EngineGroupConfig::new(
            node_data.to_path_buf(),
            raft_group_id(),
            cluster_id,
            node_id,
            database_id,
        )
    }

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

    fn envelope(seq: u64, kind_payload: (u32, Vec<u8>)) -> CommandEnvelope {
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&seq.to_le_bytes());
        CommandEnvelope::new(kind_payload.0, id, kind_payload.1)
    }

    fn create_table_envelope(seq: u64, name: &str, catalog_version: u64) -> CommandEnvelope {
        let record = CatalogCommandRecord {
            version: CATALOG_COMMAND_FORMAT_VERSION,
            catalog_version,
            command: CatalogCommand::CreateTable {
                name: name.to_string(),
                schema: simple_schema(),
                created_epoch: 1,
            },
        };
        let payload = mongreldb_core::catalog_cmds::encode_command(&record).unwrap();
        envelope(seq, (COMMAND_TYPE_CATALOG_COMMAND, payload))
    }

    fn drop_table_envelope(seq: u64, name: &str, catalog_version: u64) -> CommandEnvelope {
        let record = CatalogCommandRecord {
            version: CATALOG_COMMAND_FORMAT_VERSION,
            catalog_version,
            command: CatalogCommand::DropTable {
                name: name.to_string(),
                at_epoch: 90,
            },
        };
        let payload = mongreldb_core::catalog_cmds::encode_command(&record).unwrap();
        envelope(seq, (COMMAND_TYPE_CATALOG_COMMAND, payload))
    }

    fn put_envelope(
        seq: u64,
        txn_id: u64,
        table_id: u64,
        epoch: u64,
        values: &[i64],
    ) -> CommandEnvelope {
        let rows: Vec<Row> = values
            .iter()
            .map(|value| {
                Row::new(RowId(*value as u64), Epoch(epoch)).with_column(1, Value::Int64(*value))
            })
            .collect();
        let records = vec![
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
        ];
        let payload = ReplicatedTxnPayload::new(records).encode().unwrap();
        envelope(
            seq,
            (
                mongreldb_core::commit_log::COMMAND_TYPE_TRANSACTION,
                payload,
            ),
        )
    }

    fn visible_ids(db: &mongreldb_core::Database, table: &str) -> Vec<i64> {
        let handle = db.table(table).unwrap();
        let rows = handle
            .lock()
            .visible_rows(mongreldb_core::Snapshot::at(Epoch(u64::MAX)))
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

    fn rows_hash(db: &mongreldb_core::Database, table: &str) -> [u8; 32] {
        let handle = db.table(table).unwrap();
        let rows = handle
            .lock()
            .visible_rows(mongreldb_core::Snapshot::at(Epoch(u64::MAX)))
            .unwrap();
        mongreldb_core::cluster_import::hash_rows_canonical(&rows)
    }

    async fn one_node_group(
        node_data: &Path,
        raft_id: u64,
        node_seed: u8,
    ) -> (
        Arc<ConsensusGroup<InMemoryTransport>>,
        Arc<Mutex<EngineApplySink>>,
        EngineGroupConfig,
        tempfile::TempDir,
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join(node_data);
        let config = engine_config(&data, node_seed);
        let sink = open_engine_sink(&config).unwrap();
        let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink.clone();
        let transport = Arc::new(InMemoryTransport::new());
        let group = ConsensusGroup::create(
            group_config(raft_id, &config.group_dir()),
            transport,
            dyn_sink,
        )
        .await
        .unwrap();
        group
            .bootstrap(BTreeMap::from([(raft_id, BasicNode::new("node-1"))]))
            .await
            .unwrap();
        group.wait_leader(LEADER_TIMEOUT).await.unwrap();
        (Arc::new(group), sink, config, tmp)
    }

    #[tokio::test]
    async fn propose_through_group_applies_to_core() {
        let (group, sink, _config, _tmp) = one_node_group(Path::new("node-a"), 1, 10).await;

        group
            .propose(
                CommandKind::Catalog,
                create_table_envelope(1, "items", 1),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        group
            .propose(
                CommandKind::Transaction,
                put_envelope(2, 1, 0, 2, &[10, 20, 30]),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();

        let db = sink.lock().unwrap().database().unwrap();
        assert_eq!(db.table_names(), vec!["items".to_string()]);
        assert_eq!(visible_ids(&db, "items"), vec![10, 20, 30]);
        assert_eq!(db.visible_epoch(), Epoch(2));
        // User writes stay rejected: only the replicated apply path mutates.
        assert!(matches!(
            db.create_table("nope", simple_schema()),
            Err(MongrelError::ReadOnlyReplica)
        ));
        group.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn catalog_drop_unmounts_table() {
        let (group, sink, _config, _tmp) = one_node_group(Path::new("node-a"), 1, 20).await;
        group
            .propose(
                CommandKind::Catalog,
                create_table_envelope(1, "items", 1),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        group
            .propose(
                CommandKind::Transaction,
                put_envelope(2, 1, 0, 2, &[10]),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        group
            .propose(
                CommandKind::Catalog,
                drop_table_envelope(3, "items", 2),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        let db = sink.lock().unwrap().database().unwrap();
        assert!(db.table("items").is_err());
        assert!(db.table_names().is_empty());
        group.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn restart_replays_idempotently() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("node-a");
        let config = engine_config(&data, 30);
        let (group, sink) = {
            let sink = open_engine_sink(&config).unwrap();
            let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink.clone();
            let transport = Arc::new(InMemoryTransport::new());
            let group =
                ConsensusGroup::create(group_config(1, &config.group_dir()), transport, dyn_sink)
                    .await
                    .unwrap();
            group
                .bootstrap(BTreeMap::from([(1, BasicNode::new("node-1"))]))
                .await
                .unwrap();
            group.wait_leader(LEADER_TIMEOUT).await.unwrap();
            (Arc::new(group), sink)
        };
        group
            .propose(
                CommandKind::Catalog,
                create_table_envelope(1, "items", 1),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        group
            .propose(
                CommandKind::Transaction,
                put_envelope(2, 1, 0, 2, &[10, 20]),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        let applied_before = group.applied_position();
        group.shutdown().await.unwrap();
        // Release every reference into the first open (the group's state
        // machine holds the sink, the sink holds the database) before
        // reopening the same roots.
        drop(group);
        drop(sink);

        // Full restart: reopen the sink (the core recovers applied rows from
        // its local WAL staging) and the group (the state machine checkpoint
        // covers every committed entry; nothing redelivers).
        let sink = open_engine_sink(&config).unwrap();
        {
            let db = sink.lock().unwrap().database().unwrap();
            assert_eq!(visible_ids(&db, "items"), vec![10, 20]);
            // Direct redelivery of an already-applied payload (the crash
            // window) is recognized and skipped by the core watermark.
            let payload =
                ReplicatedTxnPayload::decode(&put_envelope(2, 1, 0, 2, &[10, 20]).payload).unwrap();
            assert!(!db.apply_replicated_records(&payload.records).unwrap());
            assert_eq!(visible_ids(&db, "items"), vec![10, 20]);
        }
        let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink.clone();
        let transport = Arc::new(InMemoryTransport::new());
        let group =
            ConsensusGroup::create(group_config(1, &config.group_dir()), transport, dyn_sink)
                .await
                .unwrap();
        group.wait_leader(LEADER_TIMEOUT).await.unwrap();
        assert_eq!(group.applied_position(), applied_before);
        let db = sink.lock().unwrap().database().unwrap();
        assert_eq!(visible_ids(&db, "items"), vec![10, 20]);

        // New work still applies after the restart.
        group
            .propose(
                CommandKind::Transaction,
                put_envelope(3, 2, 0, 3, &[30]),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        assert_eq!(visible_ids(&db, "items"), vec![10, 20, 30]);
        group.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn snapshot_install_catches_up_fresh_replica() {
        let (group_a, sink_a, _config_a, _tmp_a) = one_node_group(Path::new("node-a"), 1, 40).await;
        group_a
            .propose(
                CommandKind::Catalog,
                create_table_envelope(1, "items", 1),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        group_a
            .propose(
                CommandKind::Transaction,
                put_envelope(2, 1, 0, 2, &[10, 20, 30]),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        group_a
            .propose(
                CommandKind::Transaction,
                put_envelope(3, 2, 0, 3, &[40]),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        let snapshot = group_a.snapshot().await.unwrap();

        // A fresh replica of the same database (different node identity)
        // installs the snapshot and catches up with identical table state.
        let (group_b, sink_b, _config_b, _tmp_b) = one_node_group(Path::new("node-b"), 2, 50).await;
        group_b.install_snapshot(&snapshot).unwrap();
        let db_a = sink_a.lock().unwrap().database().unwrap();
        let db_b = sink_b.lock().unwrap().database().unwrap();
        assert_eq!(visible_ids(&db_b, "items"), vec![10, 20, 30, 40]);
        assert_eq!(rows_hash(&db_a, "items"), rows_hash(&db_b, "items"));
        assert_eq!(db_b.catalog_version(), db_a.catalog_version());

        // The installed replica keeps applying new commands.
        group_a
            .propose(
                CommandKind::Transaction,
                put_envelope(4, 3, 0, 4, &[50]),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        group_a.shutdown().await.unwrap();
        group_b.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn install_over_live_refused_and_staged_atomic() {
        let (group_a, sink_a, _config_a, _tmp_a) = one_node_group(Path::new("node-a"), 1, 60).await;
        group_a
            .propose(
                CommandKind::Catalog,
                create_table_envelope(1, "items", 1),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        group_a
            .propose(
                CommandKind::Transaction,
                put_envelope(2, 1, 0, 2, &[10, 20]),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        let payload = sink_a.lock().unwrap().snapshot().unwrap();

        // Sink B holds a live core; a second owner of that core makes the
        // shutdown (and thus the install) refuse — live state is never
        // mutated in place.
        let tmp_b = tempfile::tempdir().unwrap();
        let config_b = engine_config(&tmp_b.path().join("node-b"), 70);
        let sink_b = open_engine_sink(&config_b).unwrap();
        let live_clone = sink_b.lock().unwrap().database().unwrap();
        let error = sink_b.lock().unwrap().install(&payload).unwrap_err();
        assert!(
            error.to_string().contains("refused"),
            "unexpected error: {error}"
        );
        // B's state is untouched and the sink stayed functional.
        let db_b = sink_b.lock().unwrap().database().unwrap();
        assert!(db_b.table("items").is_err());
        let staging_leftovers = std::fs::read_dir(config_b.group_dir())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .any(|entry| entry.file_name().to_string_lossy().contains("engine-stage"));
        assert!(!staging_leftovers, "staging tree must be cleaned up");
        drop(live_clone);
        drop(db_b);

        // With the extra owner gone the staged, validated install succeeds.
        sink_b.lock().unwrap().install(&payload).unwrap();
        let db_b = sink_b.lock().unwrap().database().unwrap();
        assert_eq!(visible_ids(&db_b, "items"), vec![10, 20]);
        let db_a = sink_a.lock().unwrap().database().unwrap();
        assert_eq!(rows_hash(&db_a, "items"), rows_hash(&db_b, "items"));
        group_a.shutdown().await.unwrap();
    }

    // -- M2: leader-side spill translation -----------------------------------

    /// A real standalone commit that spills, captured from the leader's WAL:
    /// `SpilledRows` records plus an `added_runs` commit marker.
    fn standalone_spilled_commit() -> (tempfile::TempDir, Vec<Record>, u64) {
        let dir = tempfile::tempdir().unwrap();
        let db = mongreldb_core::Database::create(dir.path()).unwrap();
        db.create_table("t", simple_schema()).unwrap();
        db.set_spill_threshold(1);
        let table_id = db.table_id("t").unwrap();
        db.transaction(|txn| {
            for value in 0..40_i64 {
                txn.put("t", vec![(1, Value::Int64(value))])?;
            }
            Ok(())
        })
        .unwrap();
        let wal_records =
            mongreldb_core::wal::SharedWal::replay_with_dek(dir.path(), None).unwrap();
        let txn_id = wal_records
            .iter()
            .find_map(|record| match &record.op {
                Op::TxnCommit { added_runs, .. } if !added_runs.is_empty() => Some(record.txn_id),
                _ => None,
            })
            .expect("a spilled commit is present in the WAL");
        let records: Vec<Record> = wal_records
            .into_iter()
            .filter(|record| record.txn_id == txn_id)
            .collect();
        (dir, records, table_id)
    }

    #[test]
    fn build_transaction_envelope_translates_spills_and_rejects_untranslatable() {
        let (_dir, records, table_id) = standalone_spilled_commit();
        assert!(records
            .iter()
            .any(|record| matches!(record.op, Op::SpilledRows { .. })));

        let envelope = build_transaction_envelope([9u8; 16], &records).unwrap();
        envelope.verify().unwrap();
        let payload = ReplicatedTxnPayload::decode(&envelope.payload).unwrap();
        // No run links and no spill payloads ever reach a replica apply.
        assert!(payload
            .records
            .iter()
            .all(|record| !matches!(record.op, Op::SpilledRows { .. })));
        let Some(Op::TxnCommit { added_runs, .. }) = payload.records.last().map(|r| &r.op) else {
            panic!("a commit sequence ends in TxnCommit");
        };
        assert!(added_runs.is_empty());
        assert!(payload
            .records
            .iter()
            .any(|record| matches!(&record.op, Op::Put { table_id: id, .. } if *id == table_id)));
        // The leader's own record sequence is untouched (standalone behavior
        // stays byte-identical).
        assert!(records
            .iter()
            .any(|record| matches!(record.op, Op::SpilledRows { .. })));
        let Some(Op::TxnCommit { added_runs, .. }) = records.last().map(|r| &r.op) else {
            panic!("a commit sequence ends in TxnCommit");
        };
        assert!(!added_runs.is_empty());

        // A commit whose linked run rows are missing from the logical
        // records is rejected at proposal construction (fail closed).
        let mut broken = put_envelope(1, 1, 0, 2, &[10]);
        let decoded = ReplicatedTxnPayload::decode(&broken.payload).unwrap();
        let mut records = decoded.records;
        let Some(Op::TxnCommit { added_runs, .. }) = records.last_mut().map(|r| &mut r.op) else {
            panic!("a commit sequence ends in TxnCommit");
        };
        added_runs.push(mongreldb_core::wal::AddedRun {
            table_id: 0,
            run_id: 7,
            row_count: 1,
            level: 0,
            min_row_id: 10,
            max_row_id: 10,
            content_hash: [0; 32],
        });
        let error = build_transaction_envelope([1u8; 16], &records).unwrap_err();
        assert!(
            error.to_string().contains("no logical row records"),
            "unexpected error: {error}"
        );
        // A non-spilled commit passes through byte-identical.
        broken = put_envelope(1, 1, 0, 2, &[10]);
        let decoded = ReplicatedTxnPayload::decode(&broken.payload).unwrap();
        let envelope = build_transaction_envelope([1u8; 16], &decoded.records).unwrap();
        let round = ReplicatedTxnPayload::decode(&envelope.payload).unwrap();
        assert_eq!(
            bincode::serialize(&round.records).unwrap(),
            bincode::serialize(&decoded.records).unwrap()
        );
    }

    #[tokio::test]
    async fn spilled_commit_proposal_applies_identical_rows_on_replica() {
        // Two members of one raft group over one shared transport.
        let tmp = tempfile::tempdir().unwrap();
        let transport = Arc::new(InMemoryTransport::new());
        let config_a = engine_config(&tmp.path().join("node-a"), 80);
        let config_b = engine_config(&tmp.path().join("node-b"), 90);
        let sink_a = open_engine_sink(&config_a).unwrap();
        let sink_b = open_engine_sink(&config_b).unwrap();
        let group_a = ConsensusGroup::create(
            group_config(1, &config_a.group_dir()),
            transport.clone(),
            sink_a.clone() as Arc<Mutex<dyn ApplySink>>,
        )
        .await
        .unwrap();
        let group_b = ConsensusGroup::create(
            group_config(2, &config_b.group_dir()),
            transport,
            sink_b.clone() as Arc<Mutex<dyn ApplySink>>,
        )
        .await
        .unwrap();
        group_a
            .bootstrap(BTreeMap::from([
                (1, BasicNode::new("node-1")),
                (2, BasicNode::new("node-2")),
            ]))
            .await
            .unwrap();
        group_a.wait_leader(LEADER_TIMEOUT).await.unwrap();
        group_b.wait_leader(LEADER_TIMEOUT).await.unwrap();

        // The table exists on both replicas through the catalog command.
        let receipt = group_a
            .propose(
                CommandKind::Catalog,
                create_table_envelope(1, "t", 1),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        group_b
            .wait_applied_index(receipt.position.index, LEADER_TIMEOUT)
            .await
            .unwrap();

        // A real spilled commit, translated at proposal construction.
        let (_dir, records, table_id) = standalone_spilled_commit();
        let envelope = build_transaction_envelope([7u8; 16], &records).unwrap();
        let receipt = group_a
            .propose(
                CommandKind::Transaction,
                envelope,
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        group_b
            .wait_applied_index(receipt.position.index, LEADER_TIMEOUT)
            .await
            .unwrap();

        // Both replicas apply the spilled rows identically (count + content).
        let db_a = sink_a.lock().unwrap().database().unwrap();
        let db_b = sink_b.lock().unwrap().database().unwrap();
        let expected: Vec<i64> = (0..40).collect();
        assert_eq!(visible_ids(&db_a, "t"), expected);
        assert_eq!(visible_ids(&db_b, "t"), expected);
        assert_eq!(rows_hash(&db_a, "t"), rows_hash(&db_b, "t"));
        assert_eq!(db_a.table_id("t").unwrap(), table_id);
        group_a.shutdown().await.unwrap();
        group_b.shutdown().await.unwrap();
    }
}
