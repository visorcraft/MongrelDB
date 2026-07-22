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

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use mongreldb_core::catalog_cmds::{CatalogCommand, CatalogCommandRecord};
use mongreldb_core::memtable::{Row, Value};
use mongreldb_core::replicated_apply::{
    EngineSnapshot, ReplicatedTxnPayload, COMMAND_TYPE_CATALOG_COMMAND,
};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::storage_mode::StorageMode;
use mongreldb_core::{Epoch, MongrelError, OwnedSnapshotGuard, RowId, Snapshot};
use mongreldb_log::commit_log::LogPosition;
use mongreldb_log::envelope::CommandEnvelope;
use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{ClusterId, DatabaseId, NodeId, RaftGroupId};
use serde::{Deserialize, Serialize};

use crate::identity::ReplicatedCommand;
use crate::state_machine::{AppliedCommand, ApplySink, MongrelStateMachine, StateMachineError};
use crate::storage::{MongrelLogStore, StorageConfig};

/// Visible typed rows of a bound tablet: `row_id → (column_id → value)`.
///
/// Re-exported as a consensus type so `mongreldb-cluster` can expose the map
/// without taking a direct `mongreldb-core` dependency (AGENTS.md invariant).
pub type TypedTabletRows = BTreeMap<u64, BTreeMap<u16, Value>>;

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

/// Replicated command type for the opaque, partition-keyed tablet row stream.
///
/// The command is applied into a hidden clustered core table. It is not a
/// second storage format: normal core WAL, MVCC, sorted runs, retention, and
/// engine snapshots own the bytes.
///
/// # Legacy / migration gate
///
/// This opaque `partition_key` / `row_image` path remains available while
/// split/merge and ledger migration still depend on it
/// ([`OPAQUE_TABLET_KEYSPACE_LEGACY`]). **New production tablets with a
/// [`TabletTableBinding`] use the typed user-table path**
/// ([`COMMAND_TYPE_TABLET_WRITE`] / [`TabletWriteOperation`]) by default.
pub const COMMAND_TYPE_TABLET_DATA: u32 = 5;
/// Current tablet-data command payload version.
pub const TABLET_DATA_COMMAND_FORMAT_VERSION: u32 = 1;
/// Oldest tablet-data command payload version accepted.
pub const MIN_SUPPORTED_TABLET_DATA_COMMAND_FORMAT_VERSION: u32 = 1;

/// Replicated command type for typed user-table tablet mutations (P0.3).
///
/// Payloads are [`TabletWriteCommandRecord`]: Put/Delete/Truncate of leader-
/// resolved typed cells into a real local core table mirrored from the logical
/// schema via [`TabletTableBinding`]. Query execution projects those typed
/// columns; there is no opaque `row_image` on this path.
pub const COMMAND_TYPE_TABLET_WRITE: u32 = 6;
/// Current typed tablet-write command payload version.
pub const TABLET_WRITE_COMMAND_FORMAT_VERSION: u32 = 1;
/// Oldest typed tablet-write command payload version accepted.
pub const MIN_SUPPORTED_TABLET_WRITE_COMMAND_FORMAT_VERSION: u32 = 1;

/// Production gate for creating the opaque `__mongreldb_tablet_rows` envelope.
///
/// **Production default is `false` (P0.3):** new tablets must use
/// [`EngineApplySink::bind_tablet_user_table`] with a real
/// [`TabletTableBinding`]. [`EngineApplySink::initialize_tablet_keyspace`]
/// refuses to create the opaque keyspace when this is `false`.
///
/// Existing opaque keyspaces (table already present on disk, legacy ledger
/// install migration, or `#[cfg(test)]` explicit helpers) can still be read
/// and mutated via [`COMMAND_TYPE_TABLET_DATA`].
///
/// # Split/merge (P0.3-T6)
///
/// Opaque split/merge catch-up still uses partition-key / `row_image`
/// ([`TabletDataMutation`], pins, epoch deltas) for legacy keyspaces.
///
/// Typed-bound tablets use [`EngineApplySink::export_typed_tablet_snapshot`] /
/// [`EngineApplySink::install_typed_tablet_snapshot`] plus
/// [`EngineApplySink::typed_tablet_deltas_after_epoch`] /
/// [`EngineApplySink::apply_typed_tablet_mutations`]. Those APIs preserve the
/// [`TabletTableBinding`] and typed cells (`row_id` + column map). Opaque-only
/// split APIs refuse typed-bound sinks that have no opaque keyspace; operators
/// must use the typed snapshot path for T6.
pub const OPAQUE_TABLET_KEYSPACE_LEGACY: bool = false;

/// Current on-disk / wire format version for typed tablet split snapshots.
pub const TYPED_TABLET_SNAPSHOT_FORMAT_VERSION: u32 = 1;
/// Oldest typed tablet split snapshot format accepted.
pub const MIN_SUPPORTED_TYPED_TABLET_SNAPSHOT_FORMAT_VERSION: u32 = 1;

const TABLET_KEYSPACE_TABLE: &str = "__mongreldb_tablet_rows";
const TABLET_KEY_COLUMN: u16 = 1;
const TABLET_VALUE_COLUMN: u16 = 2;
const TABLET_SNAPSHOT_PIN_FILE: &str = "_meta/tablet-snapshot-pin.json";
const TABLET_SNAPSHOT_PIN_FORMAT_VERSION: u32 = 1;
const TABLET_LEDGER_MIGRATION_FILE: &str = "_meta/tablet-ledger-migration.json";
const TABLET_LEDGER_MIGRATION_FORMAT_VERSION: u32 = 1;
const LEGACY_TABLET_LEDGER_FORMAT_VERSION: u32 = 1;
const TABLET_BINDING_FILE: &str = "_meta/tablet-table-binding.json";
const TABLET_BINDING_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TabletSnapshotPinRecord {
    format_version: u32,
    timestamp: HlcTimestamp,
    epoch: u64,
    previous_history_epochs: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum TabletLedgerMigrationState {
    Started,
    Complete,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TabletLedgerMigrationRecord {
    format_version: u32,
    state: TabletLedgerMigrationState,
    previous_history_epochs: u64,
}

#[derive(Debug, Deserialize)]
struct LegacyTabletLedgerCheckpoint {
    format_version: u32,
    position: LogPosition,
    rows: BTreeMap<String, Vec<(HlcTimestamp, Vec<u8>)>>,
}

/// One mutation of the engine-backed tablet keyspace.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TabletDataCommand {
    /// Inserts or replaces the value of each encoded partition key.
    Upsert {
        /// Raw encoded key and row-image bytes.
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// Deletes the named encoded partition keys.
    Delete {
        /// Raw encoded keys.
        keys: Vec<Vec<u8>>,
    },
    /// Atomically replaces the complete keyspace.
    Replace {
        /// Complete raw encoded key and row-image set.
        rows: Vec<(Vec<u8>, Vec<u8>)>,
    },
}

/// One final-state mutation returned by split/merge catch-up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TabletDataMutation {
    /// Insert or replace one key.
    Upsert(Vec<u8>, Vec<u8>),
    /// Delete one key.
    Delete(Vec<u8>),
}

/// Versioned payload of one [`COMMAND_TYPE_TABLET_DATA`] envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TabletDataCommandRecord {
    /// Payload format version.
    pub format_version: u32,
    /// Mutation.
    pub command: TabletDataCommand,
}

impl TabletDataCommandRecord {
    /// Wraps `command` at the current format version.
    pub fn new(command: TabletDataCommand) -> Self {
        Self {
            format_version: TABLET_DATA_COMMAND_FORMAT_VERSION,
            command,
        }
    }

    /// Serializes the record.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("tablet data command encoding is total")
    }

    /// Decodes a record, rejecting malformed and unknown versions.
    pub fn decode(payload: &[u8]) -> Result<Self, StateMachineError> {
        let record: Self = serde_json::from_slice(payload)
            .map_err(|error| StateMachineError::Corrupt(format!("tablet data command: {error}")))?;
        if record.format_version < MIN_SUPPORTED_TABLET_DATA_COMMAND_FORMAT_VERSION
            || record.format_version > TABLET_DATA_COMMAND_FORMAT_VERSION
        {
            return Err(StateMachineError::Corrupt(format!(
                "tablet data command format version {} is outside \
                 {MIN_SUPPORTED_TABLET_DATA_COMMAND_FORMAT_VERSION}..=\
                 {TABLET_DATA_COMMAND_FORMAT_VERSION}",
                record.format_version
            )));
        }
        Ok(record)
    }
}

// ---------------------------------------------------------------------------
// P0.3 typed user-table tablet engine
// ---------------------------------------------------------------------------

/// Key-range a tablet covers, encoded as optional bounds.
///
/// `None` means unbounded on that side. Bytes are the partition-key encoding
/// used by the cluster partitioner; the consensus crate does not interpret them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TabletPartitionBounds {
    /// Inclusive low bound, or unbounded.
    pub low: Option<Vec<u8>>,
    /// Exclusive high bound, or unbounded.
    pub high: Option<Vec<u8>>,
}

/// Catalog binding of one tablet replica to a logical user table fragment.
///
/// Stored durably under the applied database root. When present, production
/// writes use [`TabletWriteOperation`] into a real local core table whose
/// schema mirrors the logical table (column ids/types, indexes, constraints).
///
/// `Schema` is not `PartialEq` in core, so equality is not derived here.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TabletTableBinding {
    /// Binding on-disk format version.
    pub format_version: u32,
    /// Cluster tablet identity.
    pub tablet_id: mongreldb_types::ids::TabletId,
    /// Logical table id owned by the meta control plane.
    pub logical_table_id: u64,
    /// Schema version the binding was opened against.
    pub schema_version: u64,
    /// Local core table name for this fragment.
    pub local_table_name: String,
    /// Local catalog `table_id` after bind (what staged writes target).
    /// Zero is a valid first-table id (`Catalog::next_table_id` starts at 0).
    pub local_table_id: u64,
    /// Local schema mirror of the logical table (columns, indexes, constraints).
    pub schema: Schema,
    /// Partition this tablet owns.
    pub partition: TabletPartitionBounds,
    /// Per-index generation counters published with the binding.
    #[serde(default)]
    pub index_generations: BTreeMap<String, u64>,
}

impl TabletTableBinding {
    /// Builds a binding at the current format version. `local_table_id` is
    /// filled by [`EngineApplySink::bind_tablet_user_table`].
    pub fn new(
        tablet_id: mongreldb_types::ids::TabletId,
        logical_table_id: u64,
        schema_version: u64,
        local_table_name: impl Into<String>,
        schema: Schema,
        partition: TabletPartitionBounds,
    ) -> Self {
        Self {
            format_version: TABLET_BINDING_FORMAT_VERSION,
            tablet_id,
            logical_table_id,
            schema_version,
            local_table_name: local_table_name.into(),
            local_table_id: 0,
            schema,
            partition,
            index_generations: BTreeMap::new(),
        }
    }
}

/// One leader-resolved typed mutation of a bound user-table tablet fragment.
///
/// Cells are fully resolved before proposal (row ids, defaults, generated
/// embeddings, sequences): followers apply the values verbatim. There is no
/// opaque `row_image` on this path.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TabletWriteOperation {
    /// Insert or replace one row's typed cells.
    Put {
        /// Local mounted table id (from [`TabletTableBinding::local_table_id`]).
        table_id: u64,
        /// Tablet-scoped row id.
        row_id: u64,
        /// `(column_id, value)` cells in leader-resolved form.
        cells: Vec<(u16, Value)>,
    },
    /// Delete one row by id.
    Delete {
        /// Local mounted table id.
        table_id: u64,
        /// Tablet-scoped row id.
        row_id: u64,
    },
    /// Delete every visible row of the local table.
    Truncate {
        /// Local mounted table id.
        table_id: u64,
    },
}

/// Versioned payload of one [`COMMAND_TYPE_TABLET_WRITE`] envelope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TabletWriteCommandRecord {
    /// Payload format version.
    pub format_version: u32,
    /// Ordered batch of typed operations applied atomically at one commit ts.
    pub operations: Vec<TabletWriteOperation>,
}

impl TabletWriteCommandRecord {
    /// Wraps `operations` at the current format version.
    pub fn new(operations: Vec<TabletWriteOperation>) -> Self {
        Self {
            format_version: TABLET_WRITE_COMMAND_FORMAT_VERSION,
            operations,
        }
    }

    /// Serializes the record (JSON, matching the tablet-data envelope codec).
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("tablet write command encoding is total")
    }

    /// Decodes a record, rejecting malformed and unknown versions.
    pub fn decode(payload: &[u8]) -> Result<Self, StateMachineError> {
        let record: Self = serde_json::from_slice(payload).map_err(|error| {
            StateMachineError::Corrupt(format!("tablet write command: {error}"))
        })?;
        if record.format_version < MIN_SUPPORTED_TABLET_WRITE_COMMAND_FORMAT_VERSION
            || record.format_version > TABLET_WRITE_COMMAND_FORMAT_VERSION
        {
            return Err(StateMachineError::Corrupt(format!(
                "tablet write command format version {} is outside                  {MIN_SUPPORTED_TABLET_WRITE_COMMAND_FORMAT_VERSION}..=                 {TABLET_WRITE_COMMAND_FORMAT_VERSION}",
                record.format_version
            )));
        }
        Ok(record)
    }
}

/// Builds the replicated catalog-kind envelope for one typed tablet write
/// batch (leader side of the P0.3 apply contract).
pub fn build_tablet_write_envelope(
    command_id: [u8; 16],
    operations: Vec<TabletWriteOperation>,
) -> CommandEnvelope {
    CommandEnvelope::new(
        COMMAND_TYPE_TABLET_WRITE,
        command_id,
        TabletWriteCommandRecord::new(operations).encode(),
    )
}

/// One final-state typed mutation returned by typed split/merge catch-up.
///
/// Re-applied via [`EngineApplySink::apply_typed_tablet_mutations`] as
/// [`TabletWriteOperation`]s — never as opaque `row_image` bytes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TypedTabletMutation {
    /// Insert or replace one row's typed cells.
    Put {
        /// Tablet-scoped row id.
        row_id: u64,
        /// `(column_id, value)` cells.
        cells: Vec<(u16, Value)>,
    },
    /// Delete one row by id.
    Delete {
        /// Tablet-scoped row id.
        row_id: u64,
    },
}

/// Portable typed tablet state for split/merge (P0.3-T6).
///
/// Captures the durable [`TabletTableBinding`] plus visible typed cells so a
/// child/replacement tablet can be rebuilt without the opaque
/// `partition_key` / `row_image` envelope.
///
/// Full range-split routing still lives in `mongreldb-cluster`. Operators and
/// split/merge executors use
/// [`EngineApplySink::export_typed_tablet_snapshot`] /
/// [`EngineApplySink::install_typed_tablet_snapshot`] as the engine seam for T6.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TypedTabletSnapshot {
    /// Snapshot envelope format version.
    pub format_version: u32,
    /// Schema binding to recreate on the destination sink.
    pub binding: TabletTableBinding,
    /// Core MVCC epoch the rows were read at (pin epoch when under a pin).
    pub epoch: u64,
    /// Source pin HLC when exported under [`EngineApplySink::pin_tablet_snapshot`].
    pub pin_timestamp: Option<HlcTimestamp>,
    /// Visible typed rows: `row_id → (column_id → value)`.
    pub rows: TypedTabletRows,
    /// Exporter last-applied log position (catch-up watermark hint).
    pub last_applied: LogPosition,
    /// Exporter last commit timestamp, when known.
    pub last_commit_ts: Option<HlcTimestamp>,
}

impl TypedTabletSnapshot {
    /// Serializes the snapshot (JSON, matching other tablet envelopes).
    pub fn encode(&self) -> Result<Vec<u8>, EngineSinkError> {
        serde_json::to_vec(self).map_err(|error| {
            MongrelError::Other(format!("encode typed tablet snapshot: {error}")).into()
        })
    }

    /// Decodes a snapshot, rejecting malformed and unknown versions.
    pub fn decode(bytes: &[u8]) -> Result<Self, EngineSinkError> {
        let snapshot: Self = serde_json::from_slice(bytes).map_err(|error| {
            MongrelError::Other(format!("decode typed tablet snapshot: {error}"))
        })?;
        if snapshot.format_version < MIN_SUPPORTED_TYPED_TABLET_SNAPSHOT_FORMAT_VERSION
            || snapshot.format_version > TYPED_TABLET_SNAPSHOT_FORMAT_VERSION
        {
            return Err(MongrelError::UnsupportedStorageVersion {
                component: "typed tablet snapshot",
                found: snapshot.format_version as u16,
                supported: TYPED_TABLET_SNAPSHOT_FORMAT_VERSION as u16,
            }
            .into());
        }
        validate_binding_record(&snapshot.binding)?;
        Ok(snapshot)
    }
}

/// A pinned core MVCC epoch used by tablet split/merge.
pub struct EngineTabletPin {
    _guard: OwnedSnapshotGuard,
    epoch: u64,
}

impl EngineTabletPin {
    /// Core epoch held by this pin.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
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
    tablet_keyspace: bool,
    tablet_history_before: Option<u64>,
    /// Durable typed user-table binding for this tablet replica (P0.3).
    tablet_binding: Option<TabletTableBinding>,
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
        let tablet_binding = read_tablet_binding(db_root)?;
        if let Some(binding) = tablet_binding.as_ref() {
            validate_binding_record(binding)?;
        }
        let tablet_keyspace = db.catalog_snapshot().live(TABLET_KEYSPACE_TABLE).is_some();
        // Restore split/merge pin retention for opaque *or* typed tablets so a
        // crashed executor resumes at the same retained core epoch.
        let tablet_history_before = match read_tablet_pin(db_root)? {
            Some(record) => {
                if record.format_version != TABLET_SNAPSHOT_PIN_FORMAT_VERSION {
                    return Err(MongrelError::UnsupportedStorageVersion {
                        component: "tablet snapshot pin",
                        found: record.format_version as u16,
                        supported: TABLET_SNAPSHOT_PIN_FORMAT_VERSION as u16,
                    }
                    .into());
                }
                db.set_history_retention_epochs(u64::MAX)?;
                Some(record.previous_history_epochs)
            }
            None => None,
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
            tablet_keyspace,
            tablet_history_before,
            tablet_binding,
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

    /// Bind the applied core to the consensus group's commit-log authority.
    pub fn bind_commit_log(
        &self,
        commit_log: Arc<dyn mongreldb_log::CommitLog>,
    ) -> Result<(), EngineSinkError> {
        self.db
            .as_ref()
            .ok_or_else(|| MongrelError::Other("engine sink has no open database".into()).into())
            .and_then(|db| {
                db.bind_cluster_commit_log(commit_log)
                    .map_err(EngineSinkError::from)
            })
    }

    /// Flushes and releases the live replica core during graceful runtime
    /// shutdown. Detaching the Raft commit log first breaks the ownership
    /// cycle `database -> commit log -> group -> state machine -> sink`.
    pub fn close(&mut self) -> Result<(), EngineSinkError> {
        if let Some(db) = self.db.as_ref() {
            db.detach_cluster_commit_log()?;
            db.close()?;
        }
        self.db.take();
        Ok(())
    }

    /// Releases the live replica core without flushing, matching process-loss
    /// semantics while still breaking in-process ownership cycles used by
    /// deterministic crash tests.
    pub fn crash(&mut self) -> Result<(), EngineSinkError> {
        if let Some(db) = self.db.as_ref() {
            db.detach_cluster_commit_log()?;
        }
        self.db.take();
        Ok(())
    }

    /// Initializes the hidden clustered table used by tablet split/merge.
    ///
    /// This is deterministic replica bootstrap, not user DDL. The table then
    /// follows the ordinary replicated apply, WAL, MVCC, sorted-run, and
    /// engine-snapshot paths.
    ///
    /// **Legacy path.** Gated by [`OPAQUE_TABLET_KEYSPACE_LEGACY`] (production
    /// default `false`). Prefer [`Self::bind_tablet_user_table`] for new
    /// user-table tablets.
    ///
    /// When the gate is off:
    /// - **Existing** opaque `__mongreldb_tablet_rows` tables still open (read /
    ///   migrate / split-merge catch-up on historical data).
    /// - **Creating** a new opaque keyspace fails so production cannot init
    ///   opaque envelopes for new tablets — callers must bind a typed schema.
    pub fn initialize_tablet_keyspace(&mut self) -> Result<(), EngineSinkError> {
        if !OPAQUE_TABLET_KEYSPACE_LEGACY {
            let catalog = self.database_required()?.catalog_snapshot();
            if catalog.live(TABLET_KEYSPACE_TABLE).is_none() {
                return Err(MongrelError::InvalidArgument(
                    "opaque tablet keyspace is disabled; bind a typed TabletTableBinding".into(),
                )
                .into());
            }
            // Existing opaque keyspace: validate and mark ready for legacy ops.
        }
        self.initialize_opaque_tablet_keyspace_unchecked()
    }

    /// Test-only explicit API: creates the opaque `__mongreldb_tablet_rows`
    /// envelope even when [`OPAQUE_TABLET_KEYSPACE_LEGACY`] is `false`.
    ///
    /// Production code must not call this. Used by split/merge and legacy
    /// ledger migration unit tests that still exercise the opaque path.
    #[cfg(test)]
    pub fn initialize_opaque_tablet_keyspace_for_tests(&mut self) -> Result<(), EngineSinkError> {
        self.initialize_opaque_tablet_keyspace_unchecked()
    }

    /// Creates or validates the opaque tablet keyspace table without the
    /// production legacy gate. Used by install-time migration of historical
    /// opaque group snapshots and by the test-only helper.
    fn initialize_opaque_tablet_keyspace_unchecked(&mut self) -> Result<(), EngineSinkError> {
        let db = self.database_required()?.clone();
        let catalog = db.catalog_snapshot();
        if let Some(entry) = catalog.live(TABLET_KEYSPACE_TABLE) {
            validate_tablet_schema(&entry.schema)?;
        } else {
            let record = CatalogCommandRecord::next(
                &catalog,
                CatalogCommand::CreateTable {
                    name: TABLET_KEYSPACE_TABLE.to_owned(),
                    schema: tablet_schema(),
                    created_epoch: db.visible_epoch().0,
                },
            );
            db.apply_replicated_catalog_command(&record)?;
        }
        self.tablet_keyspace = true;
        if let Some(record) = read_tablet_pin(&self.db_root)? {
            if record.format_version != TABLET_SNAPSHOT_PIN_FORMAT_VERSION {
                return Err(MongrelError::UnsupportedStorageVersion {
                    component: "tablet snapshot pin",
                    found: record.format_version as u16,
                    supported: TABLET_SNAPSHOT_PIN_FORMAT_VERSION as u16,
                }
                .into());
            }
            db.set_history_retention_epochs(u64::MAX)?;
            self.tablet_history_before = Some(record.previous_history_epochs);
        }
        Ok(())
    }

    /// Binds this tablet replica to a real user-table schema fragment (P0.3).
    ///
    /// Creates the local core table when missing, validates an existing table's
    /// columns against the binding schema, persists the binding under
    /// `_meta/tablet-table-binding.json`, and returns the resolved binding
    /// (with `local_table_id` filled). Subsequent typed writes apply into that
    /// table — not into opaque `row_image` bytes.
    pub fn bind_tablet_user_table(
        &mut self,
        mut binding: TabletTableBinding,
    ) -> Result<TabletTableBinding, EngineSinkError> {
        validate_binding_record(&binding)?;
        if binding.local_table_name.is_empty()
            || binding.local_table_name == TABLET_KEYSPACE_TABLE
            || binding.local_table_name.starts_with("__mongreldb_")
        {
            return Err(MongrelError::InvalidArgument(format!(
                "tablet user-table name {:?} is reserved or empty",
                binding.local_table_name
            ))
            .into());
        }
        if binding.schema.columns.is_empty() {
            return Err(MongrelError::InvalidArgument(
                "tablet user-table binding schema has no columns".into(),
            )
            .into());
        }
        let db = self.database_required()?.clone();
        let catalog = db.catalog_snapshot();
        if let Some(entry) = catalog.live(&binding.local_table_name) {
            validate_bound_user_schema(&entry.schema, &binding.schema)?;
            binding.local_table_id = entry.table_id;
        } else {
            let mut schema = binding.schema.clone();
            schema.schema_id = binding.schema_version;
            let record = CatalogCommandRecord::next(
                &catalog,
                CatalogCommand::CreateTable {
                    name: binding.local_table_name.clone(),
                    schema,
                    created_epoch: db.visible_epoch().0,
                },
            );
            db.apply_replicated_catalog_command(&record)?;
            let catalog = db.catalog_snapshot();
            let entry = catalog.live(&binding.local_table_name).ok_or_else(|| {
                MongrelError::Other(format!(
                    "tablet user table {:?} missing after create",
                    binding.local_table_name
                ))
            })?;
            binding.local_table_id = entry.table_id;
        }
        write_tablet_binding(&self.db_root, &binding)?;
        self.tablet_binding = Some(binding.clone());
        Ok(binding)
    }

    /// The durable typed user-table binding, when this tablet is bound.
    pub fn tablet_table_binding(&self) -> Option<&TabletTableBinding> {
        self.tablet_binding.as_ref()
    }

    /// Whether this sink is on the typed user-table production path.
    pub fn has_typed_user_table(&self) -> bool {
        self.tablet_binding.is_some()
    }

    /// Visible typed rows of the bound user table, ordered by row id.
    ///
    /// Each entry is `(row_id, cells)` where cells are the full column map —
    /// typed projections, not opaque `row_image` bytes.
    pub fn tablet_typed_rows(&self) -> Result<TypedTabletRows, EngineSinkError> {
        let binding = self.require_tablet_binding()?;
        let db = self.database_required()?;
        // Unbounded HLC pin: product "current rows" after reopen/recovery must
        // see every committed HLC-stamped version (P0.5).
        typed_rows_from_table(db, &binding.local_table_name, Snapshot::unbounded())
    }

    /// Typed rows of the bound user table visible at one pinned core epoch.
    pub fn tablet_typed_rows_at_epoch(
        &self,
        epoch: u64,
    ) -> Result<TypedTabletRows, EngineSinkError> {
        let binding = self.require_tablet_binding()?;
        let db = self.database_required()?;
        typed_rows_from_table(
            db,
            &binding.local_table_name,
            db.snapshot_for_epoch(Epoch(epoch)),
        )
    }

    /// Exports the typed binding + visible rows for split/merge (P0.3-T6).
    ///
    /// Current unbounded view. Prefer
    /// [`Self::export_typed_tablet_snapshot_at_epoch`] under a pin when the
    /// child must match a frozen split_ts.
    pub fn export_typed_tablet_snapshot(&self) -> Result<TypedTabletSnapshot, EngineSinkError> {
        let binding = self.require_tablet_binding()?.clone();
        let db = self.database_required()?;
        let epoch = db.visible_epoch().0;
        let rows = typed_rows_from_table(db, &binding.local_table_name, Snapshot::unbounded())?;
        Ok(TypedTabletSnapshot {
            format_version: TYPED_TABLET_SNAPSHOT_FORMAT_VERSION,
            binding,
            epoch,
            pin_timestamp: None,
            rows,
            last_applied: self.last_applied,
            last_commit_ts: self.last_commit_ts,
        })
    }

    /// Exports the typed binding + rows at a pinned core epoch (split snapshot).
    ///
    /// `pin_timestamp` is recorded for catch-up watermark bookkeeping when the
    /// caller holds an [`EngineTabletPin`] from [`Self::pin_tablet_snapshot`].
    pub fn export_typed_tablet_snapshot_at_epoch(
        &self,
        epoch: u64,
        pin_timestamp: Option<HlcTimestamp>,
    ) -> Result<TypedTabletSnapshot, EngineSinkError> {
        let binding = self.require_tablet_binding()?.clone();
        let db = self.database_required()?;
        let rows = typed_rows_from_table(
            db,
            &binding.local_table_name,
            db.snapshot_for_epoch(Epoch(epoch)),
        )?;
        Ok(TypedTabletSnapshot {
            format_version: TYPED_TABLET_SNAPSHOT_FORMAT_VERSION,
            binding,
            epoch,
            pin_timestamp,
            rows,
            last_applied: self.last_applied,
            last_commit_ts: self.last_commit_ts,
        })
    }

    /// Installs a typed tablet snapshot: binds the user table and replaces
    /// visible rows with the snapshot's typed cells (P0.3-T6).
    ///
    /// Destination `local_table_id` is resolved by bind (may differ from the
    /// source). Rows are applied as typed Put ops after Truncate — never as
    /// opaque `row_image` envelopes.
    ///
    /// Used by split/merge child build and by operators migrating typed tablets.
    pub fn install_typed_tablet_snapshot(
        &mut self,
        snapshot: &TypedTabletSnapshot,
        position: LogPosition,
        commit_ts: HlcTimestamp,
    ) -> Result<(), EngineSinkError> {
        if snapshot.format_version < MIN_SUPPORTED_TYPED_TABLET_SNAPSHOT_FORMAT_VERSION
            || snapshot.format_version > TYPED_TABLET_SNAPSHOT_FORMAT_VERSION
        {
            return Err(MongrelError::UnsupportedStorageVersion {
                component: "typed tablet snapshot",
                found: snapshot.format_version as u16,
                supported: TYPED_TABLET_SNAPSHOT_FORMAT_VERSION as u16,
            }
            .into());
        }
        validate_binding_record(&snapshot.binding)?;
        let installed = self.bind_tablet_user_table(snapshot.binding.clone())?;
        let table_id = installed.local_table_id;
        let mut operations = vec![TabletWriteOperation::Truncate { table_id }];
        for (row_id, cells) in &snapshot.rows {
            operations.push(TabletWriteOperation::Put {
                table_id,
                row_id: *row_id,
                cells: cells
                    .iter()
                    .map(|(id, value)| (*id, value.clone()))
                    .collect(),
            });
        }
        self.apply_tablet_writes(
            TabletWriteCommandRecord::new(operations),
            position,
            commit_ts,
        )?;
        self.last_applied = self.last_applied.max(position);
        if self
            .last_commit_ts
            .map(|existing| existing < commit_ts)
            .unwrap_or(true)
        {
            self.last_commit_ts = Some(commit_ts);
        }
        Ok(())
    }

    /// Final-state typed changes since `epoch` (typed split/merge catch-up).
    ///
    /// Comparing the two pinned MVCC views preserves Puts and Deletes without
    /// a parallel delta log. Refuses sinks without a typed binding.
    pub fn typed_tablet_deltas_after_epoch(
        &self,
        epoch: u64,
    ) -> Result<Vec<TypedTabletMutation>, EngineSinkError> {
        let before = self.tablet_typed_rows_at_epoch(epoch)?;
        let current = self.tablet_typed_rows()?;
        let mut changes = Vec::new();
        for (row_id, cells) in &current {
            if before.get(row_id) != Some(cells) {
                changes.push(TypedTabletMutation::Put {
                    row_id: *row_id,
                    cells: cells
                        .iter()
                        .map(|(id, value)| (*id, value.clone()))
                        .collect(),
                });
            }
        }
        for row_id in before.keys() {
            if !current.contains_key(row_id) {
                changes.push(TypedTabletMutation::Delete { row_id: *row_id });
            }
        }
        Ok(changes)
    }

    /// Applies typed catch-up mutations via the typed write path.
    pub fn apply_typed_tablet_mutations(
        &self,
        mutations: Vec<TypedTabletMutation>,
        position: LogPosition,
        commit_ts: HlcTimestamp,
    ) -> Result<(), EngineSinkError> {
        let binding = self.require_tablet_binding()?;
        let table_id = binding.local_table_id;
        let operations = mutations
            .into_iter()
            .map(|mutation| match mutation {
                TypedTabletMutation::Put { row_id, cells } => TabletWriteOperation::Put {
                    table_id,
                    row_id,
                    cells,
                },
                TypedTabletMutation::Delete { row_id } => {
                    TabletWriteOperation::Delete { table_id, row_id }
                }
            })
            .collect();
        self.apply_tablet_writes(
            TabletWriteCommandRecord::new(operations),
            position,
            commit_ts,
        )
    }

    /// Migrates a v0.60.3 `tablet-ledger.json` checkpoint into the hidden core
    /// table. Replaying a started migration is safe; a completed migration is
    /// never applied again.
    pub fn migrate_legacy_tablet_ledger(
        &mut self,
        bytes: &[u8],
        pin_timestamp: Option<HlcTimestamp>,
    ) -> Result<(), EngineSinkError> {
        self.require_tablet_keyspace()?;
        let legacy: LegacyTabletLedgerCheckpoint =
            serde_json::from_slice(bytes).map_err(|error| {
                MongrelError::Other(format!("decode legacy tablet ledger: {error}"))
            })?;
        if legacy.format_version != LEGACY_TABLET_LEDGER_FORMAT_VERSION {
            return Err(MongrelError::UnsupportedStorageVersion {
                component: "legacy tablet ledger",
                found: legacy.format_version as u16,
                supported: LEGACY_TABLET_LEDGER_FORMAT_VERSION as u16,
            }
            .into());
        }
        let db = self.database_required()?.clone();
        let mut migration = match read_migration_record(&self.db_root)? {
            Some(record) => {
                validate_migration_record(&record)?;
                record
            }
            None => {
                let record = TabletLedgerMigrationRecord {
                    format_version: TABLET_LEDGER_MIGRATION_FORMAT_VERSION,
                    state: TabletLedgerMigrationState::Started,
                    previous_history_epochs: db.history_retention_epochs(),
                };
                write_migration_record(&self.db_root, &record)?;
                record
            }
        };
        if migration.state == TabletLedgerMigrationState::Complete {
            return Ok(());
        }
        db.set_history_retention_epochs(u64::MAX)?;
        self.tablet_history_before = Some(migration.previous_history_epochs);

        let mut versions = BTreeMap::<HlcTimestamp, Vec<(Vec<u8>, Vec<u8>)>>::new();
        let mut expected = BTreeMap::new();
        for (encoded_key, chain) in legacy.rows {
            let key = decode_hex_key(&encoded_key)?;
            if chain.is_empty() || chain.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
                return Err(MongrelError::Other(
                    "legacy tablet ledger contains an empty or unordered version chain".into(),
                )
                .into());
            }
            for (timestamp, value) in &chain {
                versions
                    .entry(*timestamp)
                    .or_default()
                    .push((key.clone(), value.clone()));
            }
            if let Some((_, value)) = chain.last() {
                expected.insert(key, value.clone());
            }
        }
        let count = u64::try_from(versions.len()).unwrap_or(u64::MAX);
        let first_index = legacy
            .position
            .index
            .saturating_sub(count)
            .saturating_add(1)
            .max(1);
        for (offset, (timestamp, entries)) in versions.into_iter().enumerate() {
            self.apply_tablet_data(
                TabletDataCommandRecord::new(TabletDataCommand::Upsert { entries }),
                LogPosition {
                    term: legacy.position.term,
                    index: first_index.saturating_add(offset as u64),
                },
                timestamp,
            )?;
        }
        if self.tablet_rows()? != expected {
            return Err(MongrelError::Other(
                "legacy tablet ledger migration did not reproduce current rows".into(),
            )
            .into());
        }
        self.last_applied = self.last_applied.max(legacy.position);

        if let Some(timestamp) = pin_timestamp {
            let epoch = db
                .epoch_at_or_before_commit_ts(timestamp)
                .unwrap_or_else(|| db.visible_epoch());
            write_tablet_pin(
                &self.db_root,
                &TabletSnapshotPinRecord {
                    format_version: TABLET_SNAPSHOT_PIN_FORMAT_VERSION,
                    timestamp,
                    epoch: epoch.0,
                    previous_history_epochs: migration.previous_history_epochs,
                },
            )?;
        } else {
            db.set_history_retention_epochs(migration.previous_history_epochs)?;
            self.tablet_history_before = None;
        }
        migration.state = TabletLedgerMigrationState::Complete;
        write_migration_record(&self.db_root, &migration)?;
        Ok(())
    }

    /// Removes the crash marker after the caller durably removes the legacy
    /// ledger checkpoint.
    pub fn finish_legacy_tablet_migration(&self) -> Result<(), EngineSinkError> {
        remove_migration_record(&self.db_root)
    }

    /// Pins the engine MVCC snapshot selected by `timestamp`. The durable pin
    /// record lets a crashed split/merge resume at the identical core epoch.
    ///
    /// Works for opaque keyspaces **and** typed user-table bindings. Typed
    /// split/merge (P0.3-T6) pins with this API, then exports via
    /// [`Self::export_typed_tablet_snapshot_at_epoch`].
    pub fn pin_tablet_snapshot(
        &mut self,
        timestamp: HlcTimestamp,
    ) -> Result<EngineTabletPin, EngineSinkError> {
        self.require_tablet_split_support()?;
        let db = self.database_required()?.clone();
        let record = match read_tablet_pin(&self.db_root)? {
            Some(record) => {
                if record.format_version != TABLET_SNAPSHOT_PIN_FORMAT_VERSION {
                    return Err(MongrelError::UnsupportedStorageVersion {
                        component: "tablet snapshot pin",
                        found: record.format_version as u16,
                        supported: TABLET_SNAPSHOT_PIN_FORMAT_VERSION as u16,
                    }
                    .into());
                }
                if record.timestamp != timestamp {
                    return Err(MongrelError::InvalidArgument(format!(
                        "tablet snapshot already pinned at {:?}, requested {:?}",
                        record.timestamp, timestamp
                    ))
                    .into());
                }
                record
            }
            None => {
                let epoch = db
                    .epoch_at_or_before_commit_ts(timestamp)
                    .unwrap_or_else(|| db.visible_epoch());
                if epoch < db.earliest_retained_epoch() {
                    return Err(MongrelError::InvalidArgument(format!(
                        "tablet snapshot epoch {} is no longer retained",
                        epoch.0
                    ))
                    .into());
                }
                let record = TabletSnapshotPinRecord {
                    format_version: TABLET_SNAPSHOT_PIN_FORMAT_VERSION,
                    timestamp,
                    epoch: epoch.0,
                    previous_history_epochs: db.history_retention_epochs(),
                };
                write_tablet_pin(&self.db_root, &record)?;
                record
            }
        };
        db.set_history_retention_epochs(u64::MAX)?;
        self.tablet_history_before = Some(record.previous_history_epochs);
        let (_, guard) = db.snapshot_at_owned(Epoch(record.epoch))?;
        Ok(EngineTabletPin {
            _guard: guard,
            epoch: record.epoch,
        })
    }

    /// Releases the durable split/merge snapshot pin and restores the prior
    /// history-retention setting.
    pub fn release_tablet_snapshot(&mut self) -> Result<(), EngineSinkError> {
        self.require_tablet_split_support()?;
        let db = self.database_required()?.clone();
        let previous = read_tablet_pin(&self.db_root)?
            .map(|record| record.previous_history_epochs)
            .or(self.tablet_history_before)
            .unwrap_or_else(|| db.history_retention_epochs());
        db.set_history_retention_epochs(previous)?;
        remove_tablet_pin(&self.db_root)?;
        self.tablet_history_before = None;
        Ok(())
    }

    /// Tablet rows visible at one pinned core epoch, ordered by encoded key.
    ///
    /// **Opaque path only.** Typed-bound tablets without an opaque keyspace
    /// must use [`Self::tablet_typed_rows_at_epoch`] /
    /// [`Self::export_typed_tablet_snapshot_at_epoch`].
    pub fn tablet_rows_at_epoch(
        &self,
        epoch: u64,
    ) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, EngineSinkError> {
        self.require_opaque_tablet_keyspace_for_split()?;
        let db = self.database_required()?;
        // P0.5: pin HLC for the epoch so HLC-stamped tablet rows remain visible.
        tablet_rows(db, db.snapshot_for_epoch(Epoch(epoch)))
    }

    /// Current tablet rows, ordered by encoded key.
    ///
    /// **Opaque path only.** See [`Self::tablet_typed_rows`] for typed tablets.
    pub fn tablet_rows(&self) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, EngineSinkError> {
        self.require_opaque_tablet_keyspace_for_split()?;
        let db = self.database_required()?;
        tablet_rows(db, Snapshot::unbounded())
    }

    /// Final-state changes since `epoch`. Comparing the two pinned MVCC views
    /// avoids a parallel delta log while preserving deletes.
    ///
    /// **Opaque path only.** Typed-bound tablets without an opaque keyspace
    /// must use [`Self::typed_tablet_deltas_after_epoch`].
    pub fn tablet_deltas_after_epoch(
        &self,
        epoch: u64,
    ) -> Result<Vec<TabletDataMutation>, EngineSinkError> {
        self.require_opaque_tablet_keyspace_for_split()?;
        let before = self.tablet_rows_at_epoch(epoch)?;
        let current = self.tablet_rows()?;
        let mut changes = Vec::new();
        for (key, value) in &current {
            if before.get(key) != Some(value) {
                changes.push(TabletDataMutation::Upsert(key.clone(), value.clone()));
            }
        }
        for key in before.keys() {
            if !current.contains_key(key) {
                changes.push(TabletDataMutation::Delete(key.clone()));
            }
        }
        Ok(changes)
    }

    /// Current logical key/value bytes used by merge size admission.
    pub fn tablet_size_bytes(&self) -> Result<u64, EngineSinkError> {
        Ok(self
            .tablet_rows()?
            .iter()
            .map(|(key, value)| (key.len() + value.len()) as u64)
            .sum())
    }

    /// The last position the sink reported into a snapshot.
    pub fn last_applied(&self) -> LogPosition {
        self.last_applied
    }

    fn database_required(&self) -> Result<&Arc<mongreldb_core::Database>, EngineSinkError> {
        self.db.as_ref().ok_or_else(|| {
            EngineSinkError::Engine(MongrelError::Other(
                "engine sink has no open database".into(),
            ))
        })
    }

    fn require_tablet_keyspace(&self) -> Result<(), EngineSinkError> {
        if self.tablet_keyspace {
            Ok(())
        } else {
            Err(MongrelError::InvalidArgument(
                "engine sink is not initialized as a tablet keyspace".into(),
            )
            .into())
        }
    }

    /// Split/merge pin support: opaque keyspace **or** typed user-table binding.
    fn require_tablet_split_support(&self) -> Result<(), EngineSinkError> {
        if self.tablet_keyspace || self.tablet_binding.is_some() {
            Ok(())
        } else {
            Err(MongrelError::InvalidArgument(
                "engine sink has neither opaque tablet keyspace nor typed user-table binding"
                    .into(),
            )
            .into())
        }
    }

    /// Opaque split/merge row path. Refuses typed-only sinks so callers cannot
    /// silently treat a real user table as `partition_key`/`row_image` bytes.
    fn require_opaque_tablet_keyspace_for_split(&self) -> Result<(), EngineSinkError> {
        if self.tablet_keyspace {
            return Ok(());
        }
        if self.tablet_binding.is_some() {
            return Err(MongrelError::InvalidArgument(
                "typed-bound tablet refuses opaque row_image split/merge; \
                 use export_typed_tablet_snapshot / install_typed_tablet_snapshot"
                    .into(),
            )
            .into());
        }
        Err(MongrelError::InvalidArgument(
            "engine sink is not initialized as a tablet keyspace".into(),
        )
        .into())
    }

    fn apply_tablet_data(
        &self,
        record: TabletDataCommandRecord,
        position: LogPosition,
        commit_ts: HlcTimestamp,
    ) -> Result<(), EngineSinkError> {
        self.require_tablet_keyspace()?;
        let db = self.database_required()?;
        let table_id = db
            .catalog_snapshot()
            .live(TABLET_KEYSPACE_TABLE)
            .ok_or_else(|| MongrelError::NotFound("tablet keyspace table is missing".into()))?
            .table_id;
        let table = db.table(TABLET_KEYSPACE_TABLE)?;
        let mut writes = Vec::new();
        match record.command {
            TabletDataCommand::Upsert { entries } => {
                let rows = prepare_tablet_rows(&table, entries, false)?;
                if !rows.is_empty() {
                    writes.push(mongreldb_core::database::StagedTxnWrite::Put {
                        table_id,
                        rows: bincode::serialize(&rows).map_err(MongrelError::from)?,
                    });
                }
            }
            TabletDataCommand::Delete { keys } => {
                let row_ids = prepare_tablet_deletes(&table, keys)?;
                if !row_ids.is_empty() {
                    writes.push(mongreldb_core::database::StagedTxnWrite::Delete {
                        table_id,
                        row_ids,
                    });
                }
            }
            TabletDataCommand::Replace { rows } => {
                let existing = table
                    .lock()
                    .visible_rows(db.visible_snapshot())?
                    .into_iter()
                    .map(|row| row.row_id.0)
                    .collect::<Vec<_>>();
                if !existing.is_empty() {
                    writes.push(mongreldb_core::database::StagedTxnWrite::Delete {
                        table_id,
                        row_ids: existing,
                    });
                }
                let rows = prepare_tablet_rows(&table, rows, true)?;
                if !rows.is_empty() {
                    writes.push(mongreldb_core::database::StagedTxnWrite::Put {
                        table_id,
                        rows: bincode::serialize(&rows).map_err(MongrelError::from)?,
                    });
                }
            }
        }
        let staged = writes
            .iter()
            .map(mongreldb_core::database::StagedTxnWrite::encode)
            .collect::<Result<Vec<_>, _>>()?;
        db.apply_staged_txn_writes(position.index, &staged, commit_ts)?;
        Ok(())
    }

    fn require_tablet_binding(&self) -> Result<&TabletTableBinding, EngineSinkError> {
        self.tablet_binding.as_ref().ok_or_else(|| {
            MongrelError::InvalidArgument(
                "engine sink has no typed tablet user-table binding".into(),
            )
            .into()
        })
    }

    /// Applies one typed user-table write batch into the bound local table.
    fn apply_tablet_writes(
        &self,
        record: TabletWriteCommandRecord,
        position: LogPosition,
        commit_ts: HlcTimestamp,
    ) -> Result<(), EngineSinkError> {
        let binding = self.require_tablet_binding()?.clone();
        let db = self.database_required()?;
        let catalog = db.catalog_snapshot();
        let entry = catalog.live(&binding.local_table_name).ok_or_else(|| {
            MongrelError::NotFound(format!(
                "bound tablet user table {:?} is missing",
                binding.local_table_name
            ))
        })?;
        if entry.table_id != binding.local_table_id {
            return Err(MongrelError::Other(format!(
                "bound tablet user table {:?} id {} does not match binding local_table_id {}",
                binding.local_table_name, entry.table_id, binding.local_table_id
            ))
            .into());
        }
        let schema = entry.schema.clone();
        drop(catalog);
        let table = db.table(&binding.local_table_name)?;
        let mut puts_by_table: BTreeMap<u64, Vec<Row>> = BTreeMap::new();
        let mut deletes_by_table: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();

        for operation in record.operations {
            match operation {
                TabletWriteOperation::Put {
                    table_id,
                    row_id,
                    cells,
                } => {
                    if table_id != binding.local_table_id {
                        return Err(MongrelError::InvalidArgument(format!(
                            "tablet write Put targets table_id {table_id}, bound local_table_id is {}",
                            binding.local_table_id
                        ))
                        .into());
                    }
                    if row_id == 0 {
                        return Err(MongrelError::InvalidArgument(
                            "tablet write Put rejects reserved row_id 0".into(),
                        )
                        .into());
                    }
                    let row = build_typed_row(&schema, row_id, cells)?;
                    puts_by_table.entry(table_id).or_default().push(row);
                }
                TabletWriteOperation::Delete { table_id, row_id } => {
                    if table_id != binding.local_table_id {
                        return Err(MongrelError::InvalidArgument(format!(
                            "tablet write Delete targets table_id {table_id}, bound local_table_id is {}",
                            binding.local_table_id
                        ))
                        .into());
                    }
                    if row_id == 0 {
                        return Err(MongrelError::InvalidArgument(
                            "tablet write Delete rejects reserved row_id 0".into(),
                        )
                        .into());
                    }
                    deletes_by_table.entry(table_id).or_default().insert(row_id);
                }
                TabletWriteOperation::Truncate { table_id } => {
                    if table_id != binding.local_table_id {
                        return Err(MongrelError::InvalidArgument(format!(
                            "tablet write Truncate targets table_id {table_id}, bound local_table_id is {}",
                            binding.local_table_id
                        ))
                        .into());
                    }
                    let existing = table
                        .lock()
                        .visible_rows(db.visible_snapshot())?
                        .into_iter()
                        .map(|row| row.row_id.0)
                        .collect::<BTreeSet<_>>();
                    deletes_by_table
                        .entry(table_id)
                        .or_default()
                        .extend(existing);
                }
            }
        }

        let mut writes = Vec::new();
        for (table_id, row_ids) in deletes_by_table {
            if !row_ids.is_empty() {
                writes.push(mongreldb_core::database::StagedTxnWrite::Delete {
                    table_id,
                    row_ids: row_ids.into_iter().collect(),
                });
            }
        }
        for (table_id, rows) in puts_by_table {
            if !rows.is_empty() {
                writes.push(mongreldb_core::database::StagedTxnWrite::Put {
                    table_id,
                    rows: bincode::serialize(&rows).map_err(MongrelError::from)?,
                });
            }
        }
        if writes.is_empty() {
            return Ok(());
        }
        let staged = writes
            .iter()
            .map(mongreldb_core::database::StagedTxnWrite::encode)
            .collect::<Result<Vec<_>, _>>()?;
        db.apply_staged_txn_writes(position.index, &staged, commit_ts)?;
        Ok(())
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

    /// Reloads durable tablet metadata (typed binding + opaque keyspace flag)
    /// from the current applied database root.
    fn reload_tablet_metadata(&mut self) -> Result<(), EngineSinkError> {
        let binding = read_tablet_binding(&self.db_root)?;
        if let Some(record) = binding.as_ref() {
            validate_binding_record(record)?;
        }
        self.tablet_binding = binding;
        let db = self.database_required()?;
        self.tablet_keyspace = db.catalog_snapshot().live(TABLET_KEYSPACE_TABLE).is_some();
        Ok(())
    }
}

fn tablet_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef {
                id: TABLET_KEY_COLUMN,
                name: "partition_key".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: TABLET_VALUE_COLUMN,
                name: "row_image".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        clustered: true,
        ..Schema::default()
    }
}

fn validate_tablet_schema(schema: &Schema) -> Result<(), EngineSinkError> {
    let expected = tablet_schema();
    let compatible = schema.clustered
        && schema.columns == expected.columns
        && schema.indexes.is_empty()
        && schema.colocation.is_empty()
        && schema.constraints == expected.constraints;
    if compatible {
        Ok(())
    } else {
        Err(MongrelError::Schema(format!(
            "reserved tablet keyspace table {TABLET_KEYSPACE_TABLE:?} has incompatible schema"
        ))
        .into())
    }
}

fn validate_binding_record(binding: &TabletTableBinding) -> Result<(), EngineSinkError> {
    if binding.format_version != TABLET_BINDING_FORMAT_VERSION {
        return Err(MongrelError::UnsupportedStorageVersion {
            component: "tablet table binding",
            found: binding.format_version as u16,
            supported: TABLET_BINDING_FORMAT_VERSION as u16,
        }
        .into());
    }
    if binding.tablet_id == mongreldb_types::ids::TabletId::ZERO {
        return Err(MongrelError::InvalidArgument(
            "tablet table binding rejects reserved zero tablet id".into(),
        )
        .into());
    }
    if binding.logical_table_id == 0 {
        return Err(MongrelError::InvalidArgument(
            "tablet table binding rejects reserved zero logical_table_id".into(),
        )
        .into());
    }
    Ok(())
}

/// Column-id and type compatibility of a live local table against the binding
/// schema. Index/constraint sets may grow after bind; column identity is fixed.
fn validate_bound_user_schema(live: &Schema, expected: &Schema) -> Result<(), EngineSinkError> {
    if live.columns.len() != expected.columns.len() {
        return Err(MongrelError::Schema(format!(
            "bound tablet user table has {} columns, binding expects {}",
            live.columns.len(),
            expected.columns.len()
        ))
        .into());
    }
    for (live_col, expected_col) in live.columns.iter().zip(expected.columns.iter()) {
        if live_col.id != expected_col.id
            || live_col.name != expected_col.name
            || live_col.ty != expected_col.ty
        {
            return Err(MongrelError::Schema(format!(
                "bound tablet user table column mismatch: live {:?}/{:?}/{:?} vs binding {:?}/{:?}/{:?}",
                live_col.id,
                live_col.name,
                live_col.ty,
                expected_col.id,
                expected_col.name,
                expected_col.ty
            ))
            .into());
        }
    }
    if live.clustered != expected.clustered {
        return Err(MongrelError::Schema(
            "bound tablet user table clustered flag does not match binding".into(),
        )
        .into());
    }
    Ok(())
}

fn build_typed_row(
    schema: &Schema,
    row_id: u64,
    cells: Vec<(u16, Value)>,
) -> Result<Row, EngineSinkError> {
    let known: BTreeMap<u16, &ColumnDef> = schema
        .columns
        .iter()
        .map(|column| (column.id, column))
        .collect();
    let mut row = Row::new(RowId(row_id), Epoch(0));
    let mut seen = BTreeSet::new();
    let mut validated_cells = Vec::with_capacity(cells.len());
    for (column_id, value) in cells {
        if !seen.insert(column_id) {
            return Err(MongrelError::InvalidArgument(format!(
                "tablet write Put has duplicate column_id {column_id}"
            ))
            .into());
        }
        let column = known.get(&column_id).ok_or_else(|| {
            MongrelError::InvalidArgument(format!(
                "tablet write Put references unknown column_id {column_id}"
            ))
        })?;
        // Leader validation: per-cell type / NOT NULL flag checks (P0.3-X7).
        validate_cell_type(column, &value)?;
        validated_cells.push((column_id, value.clone()));
        row = row.with_column(column_id, value);
    }
    // Full schema validation (NOT NULL omission, enum membership, AI reps).
    schema
        .validate_values(&validated_cells)
        .map_err(|error| MongrelError::Schema(format!("tablet write constraint: {error}")))?;
    Ok(row)
}

fn validate_cell_type(column: &ColumnDef, value: &Value) -> Result<(), EngineSinkError> {
    let ok = match (&column.ty, value) {
        (_, Value::Null) => column.flags.contains(ColumnFlags::NULLABLE),
        (TypeId::Bool, Value::Bool(_)) => true,
        (
            TypeId::Int8
            | TypeId::Int16
            | TypeId::Int32
            | TypeId::Int64
            | TypeId::UInt8
            | TypeId::UInt16
            | TypeId::UInt32
            | TypeId::UInt64
            | TypeId::TimestampNanos
            | TypeId::Date32
            | TypeId::Date64
            | TypeId::Time64,
            Value::Int64(_),
        ) => true,
        (TypeId::Float32 | TypeId::Float64, Value::Float64(_)) => true,
        (TypeId::Bytes | TypeId::Enum { .. }, Value::Bytes(_)) => true,
        (TypeId::Json | TypeId::Array { .. }, Value::Json(_) | Value::Bytes(_)) => true,
        (TypeId::Embedding { .. }, Value::Embedding(_) | Value::GeneratedEmbedding(_)) => true,
        (TypeId::Decimal128 { .. }, Value::Decimal(_)) => true,
        (TypeId::Interval, Value::Interval { .. }) => true,
        (TypeId::Uuid, Value::Uuid(_)) => true,
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        Err(MongrelError::Schema(format!(
            "tablet write cell type mismatch for column {} ({:?}): got {value:?}",
            column.name, column.ty
        ))
        .into())
    }
}

fn tablet_binding_path(db_root: &Path) -> PathBuf {
    db_root.join(TABLET_BINDING_FILE)
}

fn read_tablet_binding(db_root: &Path) -> Result<Option<TabletTableBinding>, EngineSinkError> {
    let path = tablet_binding_path(db_root);
    match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            MongrelError::Other(format!(
                "decode tablet table binding {}: {error}",
                path.display()
            ))
            .into()
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(MongrelError::Io(error).into()),
    }
}

fn write_tablet_binding(
    db_root: &Path,
    binding: &TabletTableBinding,
) -> Result<(), EngineSinkError> {
    let bytes = serde_json::to_vec(binding)
        .map_err(|error| MongrelError::Other(format!("encode tablet table binding: {error}")))?;
    write_atomic_file(
        &tablet_binding_path(db_root),
        "tablet-table-binding.json.tmp",
        &bytes,
    )
}

fn prepare_tablet_rows(
    table: &mongreldb_core::database::TableHandle,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    replacing: bool,
) -> Result<Vec<Row>, EngineSinkError> {
    let entries = entries.into_iter().collect::<BTreeMap<_, _>>();
    let current = table.lock();
    let mut row_ids = BTreeMap::<u64, Vec<u8>>::new();
    let mut rows = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        let key_value = Value::Bytes(key.clone());
        let row_id = mongreldb_core::engine::clustered_row_id(&key_value);
        if let Some(other) = row_ids.insert(row_id.0, key.clone()) {
            if other != key {
                return Err(tablet_row_id_collision(&other, &key, row_id));
            }
        }
        if !replacing {
            if let Some(existing) = current.get(row_id, Snapshot::unbounded()) {
                let existing_key = tablet_row_key(&existing)?;
                if existing_key != key {
                    return Err(tablet_row_id_collision(&existing_key, &key, row_id));
                }
            }
        }
        rows.push(
            Row::new(row_id, Epoch(0))
                .with_column(TABLET_KEY_COLUMN, key_value)
                .with_column(TABLET_VALUE_COLUMN, Value::Bytes(value)),
        );
    }
    Ok(rows)
}

fn prepare_tablet_deletes(
    table: &mongreldb_core::database::TableHandle,
    keys: Vec<Vec<u8>>,
) -> Result<Vec<u64>, EngineSinkError> {
    let current = table.lock();
    let mut row_ids = BTreeSet::new();
    for key in keys {
        let row_id = mongreldb_core::engine::clustered_row_id(&Value::Bytes(key.clone()));
        if let Some(existing) = current.get(row_id, Snapshot::unbounded()) {
            let existing_key = tablet_row_key(&existing)?;
            if existing_key != key {
                return Err(tablet_row_id_collision(&existing_key, &key, row_id));
            }
        }
        row_ids.insert(row_id.0);
    }
    Ok(row_ids.into_iter().collect())
}

fn tablet_rows(
    db: &mongreldb_core::Database,
    snapshot: Snapshot,
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, EngineSinkError> {
    let rows = db
        .table(TABLET_KEYSPACE_TABLE)?
        .lock()
        .visible_rows(snapshot)?;
    let mut decoded = BTreeMap::new();
    for row in rows {
        let key = tablet_row_key(&row)?;
        let expected = mongreldb_core::engine::clustered_row_id(&Value::Bytes(key.clone()));
        if expected != row.row_id {
            return Err(MongrelError::Other(format!(
                "corrupt tablet keyspace row {}: primary key hashes to {}",
                row.row_id.0, expected.0
            ))
            .into());
        }
        let value = match row.columns.get(&TABLET_VALUE_COLUMN) {
            Some(Value::Bytes(value)) => value.clone(),
            _ => {
                return Err(MongrelError::Other(format!(
                    "corrupt tablet keyspace row {}: missing byte row image",
                    row.row_id.0
                ))
                .into())
            }
        };
        if decoded.insert(key, value).is_some() {
            return Err(MongrelError::Other(
                "corrupt tablet keyspace: duplicate primary key".into(),
            )
            .into());
        }
    }
    Ok(decoded)
}

fn typed_rows_from_table(
    db: &mongreldb_core::Database,
    table_name: &str,
    snapshot: Snapshot,
) -> Result<TypedTabletRows, EngineSinkError> {
    let rows = db.table(table_name)?.lock().visible_rows(snapshot)?;
    let mut out = BTreeMap::new();
    for row in rows {
        let mut cells = BTreeMap::new();
        for (column_id, value) in row.columns {
            cells.insert(column_id, value);
        }
        out.insert(row.row_id.0, cells);
    }
    Ok(out)
}

fn tablet_row_key(row: &Row) -> Result<Vec<u8>, EngineSinkError> {
    match row.columns.get(&TABLET_KEY_COLUMN) {
        Some(Value::Bytes(key)) => Ok(key.clone()),
        _ => Err(MongrelError::Other(format!(
            "corrupt tablet keyspace row {}: missing byte partition key",
            row.row_id.0
        ))
        .into()),
    }
}

fn tablet_row_id_collision(left: &[u8], right: &[u8], row_id: RowId) -> EngineSinkError {
    MongrelError::InvalidArgument(format!(
        "tablet partition keys {left:?} and {right:?} collide at row id {}",
        row_id.0
    ))
    .into()
}

fn tablet_pin_path(db_root: &Path) -> PathBuf {
    db_root.join(TABLET_SNAPSHOT_PIN_FILE)
}

fn read_tablet_pin(db_root: &Path) -> Result<Option<TabletSnapshotPinRecord>, EngineSinkError> {
    let path = tablet_pin_path(db_root);
    match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            MongrelError::Other(format!(
                "decode tablet snapshot pin {}: {error}",
                path.display()
            ))
            .into()
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(MongrelError::Io(error).into()),
    }
}

fn write_tablet_pin(
    db_root: &Path,
    record: &TabletSnapshotPinRecord,
) -> Result<(), EngineSinkError> {
    let path = tablet_pin_path(db_root);
    let bytes = serde_json::to_vec(record)
        .map_err(|error| MongrelError::Other(format!("encode tablet snapshot pin: {error}")))?;
    write_atomic_file(&path, "tablet-snapshot-pin.json.tmp", &bytes)
}

fn write_atomic_file(
    path: &Path,
    temporary_name: &str,
    bytes: &[u8],
) -> Result<(), EngineSinkError> {
    let parent = path.parent().expect("durable file has parent");
    std::fs::create_dir_all(parent).map_err(MongrelError::Io)?;
    let temporary = parent.join(temporary_name);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(MongrelError::Io)?;
    file.write_all(bytes).map_err(MongrelError::Io)?;
    file.sync_all().map_err(MongrelError::Io)?;
    std::fs::rename(&temporary, path).map_err(MongrelError::Io)?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(MongrelError::Io)?;
    Ok(())
}

fn remove_tablet_pin(db_root: &Path) -> Result<(), EngineSinkError> {
    remove_durable_file(&tablet_pin_path(db_root))
}

fn remove_durable_file(path: &Path) -> Result<(), EngineSinkError> {
    match std::fs::remove_file(path) {
        Ok(()) => {
            let parent = path.parent().expect("tablet pin has parent");
            std::fs::File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(MongrelError::Io)?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(MongrelError::Io(error).into()),
    }
}

fn migration_record_path(db_root: &Path) -> PathBuf {
    db_root.join(TABLET_LEDGER_MIGRATION_FILE)
}

fn read_migration_record(
    db_root: &Path,
) -> Result<Option<TabletLedgerMigrationRecord>, EngineSinkError> {
    let path = migration_record_path(db_root);
    match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            MongrelError::Other(format!(
                "decode tablet ledger migration {}: {error}",
                path.display()
            ))
            .into()
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(MongrelError::Io(error).into()),
    }
}

fn write_migration_record(
    db_root: &Path,
    record: &TabletLedgerMigrationRecord,
) -> Result<(), EngineSinkError> {
    let bytes = serde_json::to_vec(record)
        .map_err(|error| MongrelError::Other(format!("encode tablet ledger migration: {error}")))?;
    write_atomic_file(
        &migration_record_path(db_root),
        "tablet-ledger-migration.json.tmp",
        &bytes,
    )
}

fn remove_migration_record(db_root: &Path) -> Result<(), EngineSinkError> {
    remove_durable_file(&migration_record_path(db_root))
}

fn validate_migration_record(record: &TabletLedgerMigrationRecord) -> Result<(), EngineSinkError> {
    if record.format_version == TABLET_LEDGER_MIGRATION_FORMAT_VERSION {
        Ok(())
    } else {
        Err(MongrelError::UnsupportedStorageVersion {
            component: "tablet ledger migration",
            found: record.format_version as u16,
            supported: TABLET_LEDGER_MIGRATION_FORMAT_VERSION as u16,
        }
        .into())
    }
}

fn decode_hex_key(encoded: &str) -> Result<Vec<u8>, EngineSinkError> {
    if !encoded.len().is_multiple_of(2) {
        return Err(
            MongrelError::Other("legacy tablet ledger contains an odd-length key".into()).into(),
        );
    }
    (0..encoded.len())
        .step_by(2)
        .map(|offset| {
            u8::from_str_radix(&encoded[offset..offset + 2], 16).map_err(|error| {
                EngineSinkError::Engine(MongrelError::Other(format!(
                    "legacy tablet ledger key is not hexadecimal: {error}"
                )))
            })
        })
        .collect()
}

fn decode_legacy_group_snapshot(data: &[u8]) -> Result<(Vec<u8>, Vec<u8>), StateMachineError> {
    if data.len() < 12 {
        return Err(StateMachineError::Corrupt(
            "legacy tablet group snapshot is truncated".into(),
        ));
    }
    let version = u32::from_le_bytes(data[..4].try_into().expect("four bytes"));
    if version != 1 {
        return Err(StateMachineError::Corrupt(format!(
            "legacy tablet group snapshot version {version} is not 1"
        )));
    }
    let engine_len = usize::try_from(u64::from_le_bytes(
        data[4..12].try_into().expect("eight bytes"),
    ))
    .map_err(|_| StateMachineError::Corrupt("legacy engine payload is too large".into()))?;
    let boundary = 12usize
        .checked_add(engine_len)
        .filter(|boundary| *boundary <= data.len())
        .ok_or_else(|| {
            StateMachineError::Corrupt("legacy tablet group snapshot is truncated".into())
        })?;
    Ok((data[12..boundary].to_vec(), data[boundary..].to_vec()))
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
                if catalog.envelope.command_type == COMMAND_TYPE_TABLET_DATA {
                    let record = TabletDataCommandRecord::decode(&catalog.envelope.payload)?;
                    let commit_ts = command
                        .commit_ts()
                        .filter(|ts| *ts != HlcTimestamp::ZERO)
                        .or_else(|| self.database().and_then(|db| db.hlc().now().ok()))
                        .unwrap_or_else(|| HlcTimestamp {
                            physical_micros: command.position.index.saturating_mul(1_000),
                            logical: 0,
                            node_tiebreaker: 0,
                        });
                    self.apply_tablet_data(record, command.position, commit_ts)
                        .map_err(|error| StateMachineError::Sink(error.to_string()))?;
                    self.last_applied = command.position;
                    self.last_commit_ts = Some(commit_ts);
                    return Ok(());
                }
                if catalog.envelope.command_type == COMMAND_TYPE_TABLET_WRITE {
                    let record = TabletWriteCommandRecord::decode(&catalog.envelope.payload)?;
                    let commit_ts = command
                        .commit_ts()
                        .filter(|ts| *ts != HlcTimestamp::ZERO)
                        .or_else(|| self.database().and_then(|db| db.hlc().now().ok()))
                        .unwrap_or_else(|| HlcTimestamp {
                            physical_micros: command.position.index.saturating_mul(1_000),
                            logical: 0,
                            node_tiebreaker: 0,
                        });
                    self.apply_tablet_writes(record, command.position, commit_ts)
                        .map_err(|error| StateMachineError::Sink(error.to_string()))?;
                    self.last_applied = command.position;
                    self.last_commit_ts = Some(commit_ts);
                    return Ok(());
                }
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
        let (snapshot, legacy_ledger) = match EngineSnapshot::decode(data) {
            Ok(snapshot) => (snapshot, None),
            Err(engine_error) => match decode_legacy_group_snapshot(data) {
                Ok((engine, ledger)) => (
                    EngineSnapshot::decode(&engine).map_err(sink_error)?,
                    Some(ledger),
                ),
                Err(_) => return Err(sink_error(engine_error)),
            },
        };
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
                self.reload_tablet_metadata()
                    .map_err(|error| StateMachineError::Sink(error.to_string()))?;
                if let Some(ledger) = legacy_ledger {
                    // Historical opaque group snapshots still install into the
                    // legacy keyspace even when production init is gated off.
                    self.initialize_opaque_tablet_keyspace_unchecked()
                        .map_err(|error| StateMachineError::Sink(error.to_string()))?;
                    self.migrate_legacy_tablet_ledger(&ledger, None)
                        .map_err(|error| StateMachineError::Sink(error.to_string()))?;
                    self.finish_legacy_tablet_migration()
                        .map_err(|error| StateMachineError::Sink(error.to_string()))?;
                }
                Ok(())
            }
            Err(error) => {
                // Keep the sink functional if the shutdown did happen (a
                // post-shutdown failure): reopen whatever tree survived at
                // the applied root (the swap is atomic — old or new, never a
                // mix) so later applies and installs can proceed.
                if self.db.is_none() {
                    self.reopen()?;
                    let _ = self.reload_tablet_metadata();
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
                embedding_source: None,
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
            .visible_rows(mongreldb_core::Snapshot::unbounded())
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
                embedding_source: None,
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
            .visible_rows(mongreldb_core::Snapshot::unbounded())
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
            .visible_rows(mongreldb_core::Snapshot::unbounded())
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

    fn tablet_ts(physical_micros: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros,
            logical: 0,
            node_tiebreaker: 1,
        }
    }

    #[test]
    fn production_init_refuses_opaque_when_legacy_false() {
        const {
            assert!(
                !OPAQUE_TABLET_KEYSPACE_LEGACY,
                "production default must refuse opaque __mongreldb_tablet_rows init"
            );
        }
        let tmp = tempfile::tempdir().unwrap();
        let config = engine_config(tmp.path(), 40);
        let sink = open_engine_sink(&config).unwrap();
        let mut sink = sink.lock().unwrap();
        let err = sink
            .initialize_tablet_keyspace()
            .expect_err("production initialize_tablet_keyspace must fail for new tablets");
        let msg = err.to_string();
        assert!(
            msg.contains("opaque tablet keyspace is disabled"),
            "unexpected error: {msg}"
        );
        assert!(!sink.tablet_keyspace);
        assert!(!sink.has_typed_user_table());
        let catalog = sink.database().unwrap().catalog_snapshot();
        assert!(
            catalog.live(TABLET_KEYSPACE_TABLE).is_none(),
            "production must not create opaque tablet keyspace when legacy is false"
        );

        // Existing opaque keyspaces remain openable for migration/read.
        sink.initialize_opaque_tablet_keyspace_for_tests().unwrap();
        assert!(sink.tablet_keyspace);
        sink.initialize_tablet_keyspace()
            .expect("reopen of existing opaque keyspace must succeed");
        assert!(sink
            .database()
            .unwrap()
            .catalog_snapshot()
            .live(TABLET_KEYSPACE_TABLE)
            .is_some());
    }

    #[test]
    fn tablet_keyspace_uses_core_mvcc_for_snapshots_updates_and_deletes() {
        let tmp = tempfile::tempdir().unwrap();
        let config = engine_config(tmp.path(), 41);
        let sink = open_engine_sink(&config).unwrap();
        let mut sink = sink.lock().unwrap();
        // Explicit test API — production initialize_tablet_keyspace is gated off.
        sink.initialize_opaque_tablet_keyspace_for_tests().unwrap();
        sink.apply_tablet_data(
            TabletDataCommandRecord::new(TabletDataCommand::Upsert {
                entries: vec![
                    (b"a".to_vec(), b"a@1".to_vec()),
                    (b"b".to_vec(), b"b@1".to_vec()),
                ],
            }),
            LogPosition { term: 1, index: 1 },
            tablet_ts(100),
        )
        .unwrap();
        let pin = sink.pin_tablet_snapshot(tablet_ts(100)).unwrap();
        sink.apply_tablet_data(
            TabletDataCommandRecord::new(TabletDataCommand::Upsert {
                entries: vec![(b"a".to_vec(), b"a@2".to_vec())],
            }),
            LogPosition { term: 1, index: 2 },
            tablet_ts(200),
        )
        .unwrap();
        sink.apply_tablet_data(
            TabletDataCommandRecord::new(TabletDataCommand::Delete {
                keys: vec![b"b".to_vec()],
            }),
            LogPosition { term: 1, index: 3 },
            tablet_ts(300),
        )
        .unwrap();

        assert_eq!(
            sink.tablet_rows_at_epoch(pin.epoch()).unwrap(),
            BTreeMap::from([
                (b"a".to_vec(), b"a@1".to_vec()),
                (b"b".to_vec(), b"b@1".to_vec()),
            ])
        );
        assert_eq!(
            sink.tablet_deltas_after_epoch(pin.epoch()).unwrap(),
            vec![
                TabletDataMutation::Upsert(b"a".to_vec(), b"a@2".to_vec()),
                TabletDataMutation::Delete(b"b".to_vec()),
            ]
        );
        drop(pin);
        sink.release_tablet_snapshot().unwrap();
        assert_eq!(
            sink.tablet_rows().unwrap(),
            BTreeMap::from([(b"a".to_vec(), b"a@2".to_vec())])
        );
    }

    #[test]
    fn legacy_tablet_ledger_migrates_once_and_preserves_resume_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let config = engine_config(tmp.path(), 42);
        let sink = open_engine_sink(&config).unwrap();
        let mut sink = sink.lock().unwrap();
        // Legacy migration tests create the opaque keyspace via explicit test API.
        sink.initialize_opaque_tablet_keyspace_for_tests().unwrap();
        let legacy = serde_json::to_vec(&serde_json::json!({
            "format_version": 1,
            "position": {"term": 2, "index": 9},
            "rows": {
                "61": [[tablet_ts(100), [1]], [tablet_ts(200), [2]]],
                "62": [[tablet_ts(100), [3]]]
            }
        }))
        .unwrap();
        sink.migrate_legacy_tablet_ledger(&legacy, Some(tablet_ts(100)))
            .unwrap();
        let pin = sink.pin_tablet_snapshot(tablet_ts(100)).unwrap();
        assert_eq!(
            sink.tablet_rows_at_epoch(pin.epoch()).unwrap(),
            BTreeMap::from([(b"a".to_vec(), vec![1]), (b"b".to_vec(), vec![3]),])
        );
        assert_eq!(
            sink.tablet_rows().unwrap(),
            BTreeMap::from([(b"a".to_vec(), vec![2]), (b"b".to_vec(), vec![3]),])
        );
        drop(pin);
        sink.release_tablet_snapshot().unwrap();
        sink.finish_legacy_tablet_migration().unwrap();
    }

    fn typed_user_schema() -> Schema {
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
                    name: "name".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                    default_value: None,
                    embedding_source: None,
                },
                ColumnDef {
                    id: 3,
                    name: "score".into(),
                    ty: TypeId::Float64,
                    flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                    default_value: None,
                    embedding_source: None,
                },
            ],
            clustered: true,
            ..Schema::default()
        }
    }

    #[test]
    fn typed_tablet_binding_creates_real_user_table_with_typed_columns() {
        let tmp = tempfile::tempdir().unwrap();
        let config = engine_config(tmp.path(), 50);
        let sink = open_engine_sink(&config).unwrap();
        let mut sink = sink.lock().unwrap();

        let binding = sink
            .bind_tablet_user_table(TabletTableBinding::new(
                mongreldb_types::ids::TabletId::from_bytes([9; 16]),
                42,
                1,
                "orders",
                typed_user_schema(),
                TabletPartitionBounds::default(),
            ))
            .unwrap();
        assert!(sink.has_typed_user_table());
        assert_eq!(
            sink.tablet_table_binding().unwrap().local_table_name,
            "orders"
        );

        sink.apply_tablet_writes(
            TabletWriteCommandRecord::new(vec![
                TabletWriteOperation::Put {
                    table_id: binding.local_table_id,
                    row_id: 1,
                    cells: vec![
                        (1, Value::Int64(1)),
                        (2, Value::Bytes(b"alpha".to_vec())),
                        (3, Value::Float64(1.5)),
                    ],
                },
                TabletWriteOperation::Put {
                    table_id: binding.local_table_id,
                    row_id: 2,
                    cells: vec![
                        (1, Value::Int64(2)),
                        (2, Value::Bytes(b"beta".to_vec())),
                        (3, Value::Float64(2.5)),
                    ],
                },
            ]),
            LogPosition { term: 1, index: 1 },
            tablet_ts(100),
        )
        .unwrap();

        let rows = sink.tablet_typed_rows().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[&1].get(&1), Some(&Value::Int64(1)));
        assert_eq!(rows[&1].get(&2), Some(&Value::Bytes(b"alpha".to_vec())));
        assert_eq!(rows[&1].get(&3), Some(&Value::Float64(1.5)));
        assert_eq!(rows[&2].get(&2), Some(&Value::Bytes(b"beta".to_vec())));

        let db = sink.database().unwrap();
        let catalog = db.catalog_snapshot();
        let entry = catalog.live("orders").expect("orders table");
        let names: Vec<_> = entry
            .schema
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["id", "name", "score"]);
        assert!(entry
            .schema
            .columns
            .iter()
            .all(|c| c.name != "row_image" && c.name != "partition_key"));
        assert!(catalog.live(TABLET_KEYSPACE_TABLE).is_none());

        // Production query path for new tablets is typed cells, not row_image.
        let typed = sink.tablet_typed_rows().unwrap();
        for cells in typed.values() {
            assert!(
                cells.values().any(|v| !matches!(v, Value::Bytes(_))),
                "typed path must surface non-opaque cell types for query"
            );
        }
        // Production init remains refused even after a successful typed bind.
        assert!(sink.initialize_tablet_keyspace().is_err());
    }

    #[test]
    fn typed_tablet_write_delete_and_truncate_after_replicated_command() {
        let tmp = tempfile::tempdir().unwrap();
        let config = engine_config(tmp.path(), 51);
        let sink = open_engine_sink(&config).unwrap();
        let mut sink = sink.lock().unwrap();
        let binding = sink
            .bind_tablet_user_table(TabletTableBinding::new(
                mongreldb_types::ids::TabletId::from_bytes([8; 16]),
                7,
                1,
                "items",
                typed_user_schema(),
                TabletPartitionBounds::default(),
            ))
            .unwrap();
        let table_id = binding.local_table_id;

        sink.apply_tablet_writes(
            TabletWriteCommandRecord::new(vec![
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 10,
                    cells: vec![(1, Value::Int64(10)), (2, Value::Bytes(b"keep".to_vec()))],
                },
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 11,
                    cells: vec![(1, Value::Int64(11)), (2, Value::Bytes(b"drop".to_vec()))],
                },
            ]),
            LogPosition { term: 1, index: 1 },
            tablet_ts(10),
        )
        .unwrap();
        sink.apply_tablet_writes(
            TabletWriteCommandRecord::new(vec![TabletWriteOperation::Delete {
                table_id,
                row_id: 11,
            }]),
            LogPosition { term: 1, index: 2 },
            tablet_ts(20),
        )
        .unwrap();
        assert_eq!(
            sink.tablet_typed_rows()
                .unwrap()
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![10]
        );

        sink.apply_tablet_writes(
            TabletWriteCommandRecord::new(vec![TabletWriteOperation::Truncate { table_id }]),
            LogPosition { term: 1, index: 3 },
            tablet_ts(30),
        )
        .unwrap();
        assert!(sink.tablet_typed_rows().unwrap().is_empty());
    }

    #[test]
    fn typed_tablet_binding_survives_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let config = engine_config(tmp.path(), 52);
        {
            let sink = open_engine_sink(&config).unwrap();
            let mut sink = sink.lock().unwrap();
            let binding = sink
                .bind_tablet_user_table(TabletTableBinding::new(
                    mongreldb_types::ids::TabletId::from_bytes([7; 16]),
                    99,
                    3,
                    "users",
                    typed_user_schema(),
                    TabletPartitionBounds {
                        low: Some(b"a".to_vec()),
                        high: Some(b"m".to_vec()),
                    },
                ))
                .unwrap();
            sink.apply_tablet_writes(
                TabletWriteCommandRecord::new(vec![TabletWriteOperation::Put {
                    table_id: binding.local_table_id,
                    row_id: 5,
                    cells: vec![(1, Value::Int64(5)), (2, Value::Bytes(b"persist".to_vec()))],
                }]),
                LogPosition { term: 1, index: 1 },
                tablet_ts(50),
            )
            .unwrap();
        }
        let sink = open_engine_sink(&config).unwrap();
        let sink = sink.lock().unwrap();
        let binding = sink.tablet_table_binding().expect("binding restored");
        assert_eq!(binding.local_table_name, "users");
        assert_eq!(binding.logical_table_id, 99);
        assert_eq!(binding.schema_version, 3);
        assert_eq!(
            sink.tablet_typed_rows().unwrap()[&5].get(&2),
            Some(&Value::Bytes(b"persist".to_vec()))
        );
    }

    #[test]
    fn typed_tablet_write_record_rejects_unknown_format_version() {
        let record = TabletWriteCommandRecord {
            format_version: 99,
            operations: vec![],
        };
        let encoded = serde_json::to_vec(&record).unwrap();
        assert!(TabletWriteCommandRecord::decode(&encoded).is_err());
        assert!(TabletWriteCommandRecord::decode(b"not-json").is_err());
    }

    #[tokio::test]
    async fn typed_tablet_write_applies_through_consensus_group() {
        let (group, sink, _config, _tmp) = one_node_group(Path::new("node-typed"), 1, 53).await;

        let binding = {
            let mut engine = sink.lock().unwrap();
            engine
                .bind_tablet_user_table(TabletTableBinding::new(
                    mongreldb_types::ids::TabletId::from_bytes([6; 16]),
                    11,
                    1,
                    "replicated_orders",
                    typed_user_schema(),
                    TabletPartitionBounds::default(),
                ))
                .unwrap()
        };

        let envelope = build_tablet_write_envelope(
            [3u8; 16],
            vec![TabletWriteOperation::Put {
                table_id: binding.local_table_id,
                row_id: 100,
                cells: vec![
                    (1, Value::Int64(100)),
                    (2, Value::Bytes(b"via-raft".to_vec())),
                    (3, Value::Float64(9.25)),
                ],
            }],
        );
        let receipt = group
            .propose(CommandKind::Catalog, envelope, &ExecutionControl::default())
            .await
            .unwrap();
        group
            .wait_applied_index(receipt.position.index, LEADER_TIMEOUT)
            .await
            .unwrap();

        let rows = sink.lock().unwrap().tablet_typed_rows().unwrap();
        assert_eq!(
            rows[&100].get(&2),
            Some(&Value::Bytes(b"via-raft".to_vec()))
        );
        assert_eq!(rows[&100].get(&3), Some(&Value::Float64(9.25)));

        let db = sink.lock().unwrap().database().unwrap();
        let handle = db.table("replicated_orders").unwrap();
        let visible = handle.lock().visible_rows(Snapshot::unbounded()).unwrap();
        assert_eq!(visible.len(), 1);
        assert!(visible[0].columns.contains_key(&1));
        assert!(visible[0].columns.contains_key(&2));
        assert!(visible[0].columns.contains_key(&3));
        assert!(!visible[0]
            .columns
            .values()
            .all(|v| matches!(v, Value::Bytes(_))));

        group.shutdown().await.unwrap();
    }

    /// P0.3-T6 / X8 / FAC-TE-6: typed split snapshot + catch-up preserves schema
    /// binding and typed cells across a second sink (no opaque row_image).
    /// ID: FAC-TE-6 Split/merge typed data (export/install + catch-up).
    #[test]
    fn typed_tablet_split_snapshot_and_catch_up_preserve_schema_and_cells() {
        let src_tmp = tempfile::tempdir().unwrap();
        let dst_tmp = tempfile::tempdir().unwrap();
        let src_config = engine_config(src_tmp.path(), 60);
        let dst_config = engine_config(dst_tmp.path(), 61);

        let src = open_engine_sink(&src_config).unwrap();
        let mut src = src.lock().unwrap();
        let binding = src
            .bind_tablet_user_table(TabletTableBinding::new(
                mongreldb_types::ids::TabletId::from_bytes([0x60; 16]),
                100,
                2,
                "orders",
                typed_user_schema(),
                TabletPartitionBounds {
                    low: Some(b"a".to_vec()),
                    high: Some(b"m".to_vec()),
                },
            ))
            .unwrap();
        let table_id = binding.local_table_id;

        // Seed rows at split_ts.
        src.apply_tablet_writes(
            TabletWriteCommandRecord::new(vec![
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 1,
                    cells: vec![
                        (1, Value::Int64(1)),
                        (2, Value::Bytes(b"alpha".to_vec())),
                        (3, Value::Float64(1.5)),
                    ],
                },
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 2,
                    cells: vec![
                        (1, Value::Int64(2)),
                        (2, Value::Bytes(b"beta".to_vec())),
                        (3, Value::Float64(2.5)),
                    ],
                },
            ]),
            LogPosition { term: 1, index: 1 },
            tablet_ts(100),
        )
        .unwrap();

        // Opaque split APIs must refuse typed-only tablets.
        let opaque_err = src.tablet_rows().expect_err("opaque path refused");
        assert!(
            opaque_err
                .to_string()
                .contains("typed-bound tablet refuses opaque"),
            "unexpected opaque refusal: {opaque_err}"
        );

        let pin = src.pin_tablet_snapshot(tablet_ts(100)).unwrap();
        let split_epoch = pin.epoch();
        let snapshot = src
            .export_typed_tablet_snapshot_at_epoch(split_epoch, Some(tablet_ts(100)))
            .unwrap();
        assert_eq!(
            snapshot.format_version,
            TYPED_TABLET_SNAPSHOT_FORMAT_VERSION
        );
        assert_eq!(snapshot.binding.logical_table_id, 100);
        assert_eq!(snapshot.binding.schema_version, 2);
        assert_eq!(snapshot.binding.local_table_name, "orders");
        assert_eq!(snapshot.rows.len(), 2);
        assert_eq!(
            snapshot.rows[&1].get(&2),
            Some(&Value::Bytes(b"alpha".to_vec()))
        );
        // Wire encode/decode preserves typed cells (no row_image envelope).
        let encoded = snapshot.encode().unwrap();
        let decoded = TypedTabletSnapshot::decode(&encoded).unwrap();
        assert_eq!(decoded.rows, snapshot.rows);
        assert_eq!(decoded.binding.logical_table_id, 100);

        // Post-pin mutations become typed catch-up deltas.
        src.apply_tablet_writes(
            TabletWriteCommandRecord::new(vec![
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 1,
                    cells: vec![
                        (1, Value::Int64(1)),
                        (2, Value::Bytes(b"alpha-v2".to_vec())),
                        (3, Value::Float64(1.75)),
                    ],
                },
                TabletWriteOperation::Delete {
                    table_id,
                    row_id: 2,
                },
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 3,
                    cells: vec![
                        (1, Value::Int64(3)),
                        (2, Value::Bytes(b"gamma".to_vec())),
                        (3, Value::Float64(3.0)),
                    ],
                },
            ]),
            LogPosition { term: 1, index: 2 },
            tablet_ts(200),
        )
        .unwrap();

        assert_eq!(
            src.tablet_typed_rows_at_epoch(split_epoch).unwrap().len(),
            2
        );
        let deltas = src.typed_tablet_deltas_after_epoch(split_epoch).unwrap();
        assert!(
            deltas
                .iter()
                .any(|m| matches!(m, TypedTabletMutation::Put { row_id: 1, .. })),
            "expected put for row 1: {deltas:?}"
        );
        assert!(
            deltas
                .iter()
                .any(|m| matches!(m, TypedTabletMutation::Delete { row_id: 2 })),
            "expected delete for row 2: {deltas:?}"
        );
        assert!(
            deltas
                .iter()
                .any(|m| matches!(m, TypedTabletMutation::Put { row_id: 3, .. })),
            "expected put for row 3: {deltas:?}"
        );

        // Child sink: install frozen snapshot, then apply catch-up.
        let dst = open_engine_sink(&dst_config).unwrap();
        let mut dst = dst.lock().unwrap();
        dst.install_typed_tablet_snapshot(
            &decoded,
            LogPosition { term: 1, index: 10 },
            tablet_ts(100),
        )
        .unwrap();
        assert!(dst.has_typed_user_table());
        let dst_binding = dst.tablet_table_binding().unwrap().clone();
        assert_eq!(dst_binding.local_table_name, "orders");
        assert_eq!(dst_binding.logical_table_id, 100);
        assert_eq!(dst_binding.schema_version, 2);
        assert_eq!(
            dst_binding.partition,
            TabletPartitionBounds {
                low: Some(b"a".to_vec()),
                high: Some(b"m".to_vec()),
            }
        );
        // Schema columns survive (typed, not partition_key/row_image).
        let names: Vec<_> = dst_binding
            .schema
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["id", "name", "score"]);
        assert_eq!(dst.tablet_typed_rows().unwrap().len(), 2);
        assert_eq!(
            dst.tablet_typed_rows().unwrap()[&1].get(&2),
            Some(&Value::Bytes(b"alpha".to_vec()))
        );

        dst.apply_typed_tablet_mutations(
            deltas,
            LogPosition { term: 1, index: 11 },
            tablet_ts(200),
        )
        .unwrap();
        let final_rows = dst.tablet_typed_rows().unwrap();
        assert_eq!(final_rows.len(), 2);
        assert_eq!(
            final_rows[&1].get(&2),
            Some(&Value::Bytes(b"alpha-v2".to_vec()))
        );
        assert_eq!(final_rows[&1].get(&3), Some(&Value::Float64(1.75)));
        assert!(!final_rows.contains_key(&2));
        assert_eq!(
            final_rows[&3].get(&2),
            Some(&Value::Bytes(b"gamma".to_vec()))
        );
        assert_eq!(final_rows, src.tablet_typed_rows().unwrap());

        // Destination also refuses opaque split path.
        assert!(dst.tablet_deltas_after_epoch(0).is_err());

        drop(pin);
        src.release_tablet_snapshot().unwrap();
    }

    #[test]
    fn typed_tablet_export_install_round_trip_current_view() {
        let src_tmp = tempfile::tempdir().unwrap();
        let dst_tmp = tempfile::tempdir().unwrap();
        let src = open_engine_sink(&engine_config(src_tmp.path(), 62)).unwrap();
        let mut src = src.lock().unwrap();
        let binding = src
            .bind_tablet_user_table(TabletTableBinding::new(
                mongreldb_types::ids::TabletId::from_bytes([0x62; 16]),
                7,
                1,
                "items",
                typed_user_schema(),
                TabletPartitionBounds::default(),
            ))
            .unwrap();
        src.apply_tablet_writes(
            TabletWriteCommandRecord::new(vec![TabletWriteOperation::Put {
                table_id: binding.local_table_id,
                row_id: 42,
                cells: vec![
                    (1, Value::Int64(42)),
                    (2, Value::Bytes(b"keep".to_vec())),
                    (3, Value::Float64(4.2)),
                ],
            }]),
            LogPosition { term: 1, index: 1 },
            tablet_ts(50),
        )
        .unwrap();

        let snapshot = src.export_typed_tablet_snapshot().unwrap();
        let dst = open_engine_sink(&engine_config(dst_tmp.path(), 63)).unwrap();
        let mut dst = dst.lock().unwrap();
        dst.install_typed_tablet_snapshot(
            &snapshot,
            LogPosition { term: 2, index: 1 },
            tablet_ts(50),
        )
        .unwrap();
        assert_eq!(
            dst.tablet_typed_rows().unwrap(),
            src.tablet_typed_rows().unwrap()
        );
        assert_eq!(
            dst.tablet_table_binding().unwrap().local_table_name,
            "items"
        );
    }

    // P0.3-X7: leader validation rejects invalid typed ops (NOT NULL / type).
    #[test]
    fn typed_tablet_constraints_reject_invalid_ops_at_validation() {
        use mongreldb_core::schema::{IndexDef, IndexKind};

        let tmp = tempfile::tempdir().unwrap();
        let config = engine_config(tmp.path(), 70);
        let sink = open_engine_sink(&config).unwrap();
        let mut sink = sink.lock().unwrap();

        // name is NOT NULL (no NULLABLE flag).
        let schema = Schema {
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
                    name: "name".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty(), // NOT NULL
                    default_value: None,
                    embedding_source: None,
                },
            ],
            clustered: true,
            indexes: vec![IndexDef {
                name: "name_bm".into(),
                column_id: 2,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            }],
            ..Schema::default()
        };
        let binding = sink
            .bind_tablet_user_table(TabletTableBinding::new(
                mongreldb_types::ids::TabletId::from_bytes([1; 16]),
                1,
                1,
                "docs",
                schema,
                TabletPartitionBounds::default(),
            ))
            .unwrap();
        let table_id = binding.local_table_id;

        // Type mismatch: Int64 column gets Bytes.
        let err = sink
            .apply_tablet_writes(
                TabletWriteCommandRecord::new(vec![TabletWriteOperation::Put {
                    table_id,
                    row_id: 1,
                    cells: vec![
                        (1, Value::Bytes(b"not-int".to_vec())),
                        (2, Value::Bytes(b"ok".to_vec())),
                    ],
                }]),
                LogPosition { term: 1, index: 1 },
                tablet_ts(1),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("type mismatch")
                || msg.contains("does not match type")
                || msg.contains("constraint"),
            "expected type rejection, got: {msg}"
        );

        // NOT NULL: name is Null.
        let err = sink
            .apply_tablet_writes(
                TabletWriteCommandRecord::new(vec![TabletWriteOperation::Put {
                    table_id,
                    row_id: 2,
                    cells: vec![(1, Value::Int64(2)), (2, Value::Null)],
                }]),
                LogPosition { term: 1, index: 2 },
                tablet_ts(2),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("NULL")
                || msg.contains("nullable")
                || msg.contains("constraint")
                || msg.contains("type mismatch"),
            "expected NOT NULL rejection, got: {msg}"
        );

        // Omitted NOT NULL column.
        let err = sink
            .apply_tablet_writes(
                TabletWriteCommandRecord::new(vec![TabletWriteOperation::Put {
                    table_id,
                    row_id: 3,
                    cells: vec![(1, Value::Int64(3))],
                }]),
                LogPosition { term: 1, index: 3 },
                tablet_ts(3),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("NOT NULL") || msg.contains("omitted") || msg.contains("constraint"),
            "expected omitted NOT NULL rejection, got: {msg}"
        );

        // Valid write still applies.
        sink.apply_tablet_writes(
            TabletWriteCommandRecord::new(vec![TabletWriteOperation::Put {
                table_id,
                row_id: 4,
                cells: vec![(1, Value::Int64(4)), (2, Value::Bytes(b"alpha".to_vec()))],
            }]),
            LogPosition { term: 1, index: 4 },
            tablet_ts(4),
        )
        .unwrap();
        assert_eq!(sink.tablet_typed_rows().unwrap().len(), 1);
    }

    // P0.3-X2: Bitmap index works after replicated typed apply.
    #[test]
    fn typed_tablet_bitmap_index_works_after_replicated_apply() {
        use mongreldb_core::query::{Condition, Query};
        use mongreldb_core::schema::{IndexDef, IndexKind};

        let tmp = tempfile::tempdir().unwrap();
        let config = engine_config(tmp.path(), 71);
        let sink = open_engine_sink(&config).unwrap();
        let mut sink = sink.lock().unwrap();

        let schema = Schema {
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
            clustered: true,
            indexes: vec![IndexDef {
                name: "cat_bm".into(),
                column_id: 2,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            }],
            ..Schema::default()
        };
        let binding = sink
            .bind_tablet_user_table(TabletTableBinding::new(
                mongreldb_types::ids::TabletId::from_bytes([2; 16]),
                2,
                1,
                "catalog",
                schema,
                TabletPartitionBounds::default(),
            ))
            .unwrap();
        let table_id = binding.local_table_id;
        sink.apply_tablet_writes(
            TabletWriteCommandRecord::new(vec![
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 1,
                    cells: vec![(1, Value::Int64(1)), (2, Value::Int64(10))],
                },
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 2,
                    cells: vec![(1, Value::Int64(2)), (2, Value::Int64(20))],
                },
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 3,
                    cells: vec![(1, Value::Int64(3)), (2, Value::Int64(10))],
                },
            ]),
            LogPosition { term: 1, index: 1 },
            tablet_ts(10),
        )
        .unwrap();

        let db = sink.database().unwrap();
        let query = Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: Value::Int64(10).encode_key(),
        });
        let rows = db
            .query_for_current_principal("catalog", &query, Some(&[1, 2]))
            .unwrap();
        let mut ids: Vec<i64> = rows
            .iter()
            .filter_map(|r| match r.columns.get(&1) {
                Some(Value::Int64(v)) => Some(*v),
                _ => None,
            })
            .collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![1, 3],
            "bitmap eq after typed apply must hit index path"
        );
    }

    // ID: P0.3-X3 Dense ANN works after replicated typed apply.
    #[test]
    fn typed_tablet_dense_ann_works_after_replicated_apply() {
        use mongreldb_core::query::{Condition, Query};
        use mongreldb_core::schema::{IndexDef, IndexKind};

        let tmp = tempfile::tempdir().unwrap();
        let config = engine_config(tmp.path(), 73);
        let sink = open_engine_sink(&config).unwrap();
        let mut sink = sink.lock().unwrap();

        let schema = Schema {
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
                    name: "vec".into(),
                    ty: TypeId::Embedding { dim: 4 },
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
            ],
            clustered: true,
            indexes: vec![IndexDef {
                name: "vec_ann".into(),
                column_id: 2,
                kind: IndexKind::Ann,
                predicate: None,
                options: Default::default(),
            }],
            ..Schema::default()
        };
        let binding = sink
            .bind_tablet_user_table(TabletTableBinding::new(
                mongreldb_types::ids::TabletId::from_bytes([3; 16]),
                3,
                1,
                "vectors",
                schema,
                TabletPartitionBounds::default(),
            ))
            .unwrap();
        let table_id = binding.local_table_id;
        sink.apply_tablet_writes(
            TabletWriteCommandRecord::new(vec![
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 1,
                    cells: vec![
                        (1, Value::Int64(1)),
                        (2, Value::Embedding(vec![1.0, 0.0, 0.0, 0.0])),
                    ],
                },
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 2,
                    cells: vec![
                        (1, Value::Int64(2)),
                        (2, Value::Embedding(vec![0.0, 1.0, 0.0, 0.0])),
                    ],
                },
            ]),
            LogPosition { term: 1, index: 1 },
            tablet_ts(11),
        )
        .unwrap();

        let db = sink.database().unwrap();
        let query = Query::new().and(Condition::Ann {
            column_id: 2,
            query: vec![1.0, 0.0, 0.0, 0.0],
            k: 1,
        });
        let rows = db
            .query_for_current_principal("vectors", &query, Some(&[1, 2]))
            .unwrap();
        assert_eq!(rows.len(), 1, "ANN top-1 after typed apply: {rows:?}");
        match rows[0].columns.get(&1) {
            Some(Value::Int64(1)) => {}
            other => panic!("expected nearest row id 1, got {other:?}"),
        }
    }

    // ID: P0.3-X4 Sparse and MinHash work after replicated typed apply.
    #[test]
    fn typed_tablet_sparse_and_minhash_work_after_replicated_apply() {
        use mongreldb_core::query::{Condition, Query};
        use mongreldb_core::schema::{IndexDef, IndexKind};

        let tmp = tempfile::tempdir().unwrap();
        let config = engine_config(tmp.path(), 74);
        let sink = open_engine_sink(&config).unwrap();
        let mut sink = sink.lock().unwrap();

        let schema = Schema {
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
                    name: "sparse".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
                ColumnDef {
                    id: 3,
                    name: "members".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
            ],
            clustered: true,
            indexes: vec![
                IndexDef {
                    name: "sp".into(),
                    column_id: 2,
                    kind: IndexKind::Sparse,
                    predicate: None,
                    options: Default::default(),
                },
                IndexDef {
                    name: "mh".into(),
                    column_id: 3,
                    kind: IndexKind::MinHash,
                    predicate: None,
                    options: Default::default(),
                },
            ],
            ..Schema::default()
        };
        let binding = sink
            .bind_tablet_user_table(TabletTableBinding::new(
                mongreldb_types::ids::TabletId::from_bytes([4; 16]),
                4,
                1,
                "hybrid",
                schema,
                TabletPartitionBounds::default(),
            ))
            .unwrap();
        let table_id = binding.local_table_id;
        let sparse_a = Value::Bytes(bincode::serialize(&vec![(1u32, 2.0f32)]).unwrap());
        let sparse_b = Value::Bytes(bincode::serialize(&vec![(2u32, 1.0f32)]).unwrap());
        let members = |vals: &[&str]| Value::Bytes(serde_json::to_vec(vals).unwrap());
        sink.apply_tablet_writes(
            TabletWriteCommandRecord::new(vec![
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 1,
                    cells: vec![
                        (1, Value::Int64(1)),
                        (2, sparse_a),
                        (3, members(&["a", "b", "c", "d"])),
                    ],
                },
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 2,
                    cells: vec![
                        (1, Value::Int64(2)),
                        (2, sparse_b),
                        (3, members(&["a", "b", "c", "x"])),
                    ],
                },
            ]),
            LogPosition { term: 1, index: 1 },
            tablet_ts(12),
        )
        .unwrap();

        let db = sink.database().unwrap();
        let sparse_q = Query::new().and(Condition::SparseMatch {
            column_id: 2,
            query: vec![(1, 1.0)],
            k: 1,
        });
        let sparse_rows = db
            .query_for_current_principal("hybrid", &sparse_q, Some(&[1, 2, 3]))
            .unwrap();
        assert_eq!(
            sparse_rows.len(),
            1,
            "sparse after typed apply: {sparse_rows:?}"
        );
        assert!(matches!(
            sparse_rows[0].columns.get(&1),
            Some(Value::Int64(1))
        ));

        let minhash_q = Query::new().and(Condition::MinHashSimilar {
            column_id: 3,
            query: ["a", "b", "c", "d"]
                .into_iter()
                .map(mongreldb_core::index::minhash_token_hash)
                .collect(),
            k: 1,
        });
        let minhash_rows = db
            .query_for_current_principal("hybrid", &minhash_q, Some(&[1, 2, 3]))
            .unwrap();
        assert_eq!(
            minhash_rows.len(),
            1,
            "minhash after typed apply: {minhash_rows:?}"
        );
        assert!(matches!(
            minhash_rows[0].columns.get(&1),
            Some(Value::Int64(1))
        ));
    }

    // P0.7-X9: followers apply leader-resolved GeneratedEmbedding without provider.
    #[test]
    fn typed_tablet_generated_embedding_arrives_without_follower_inference() {
        use mongreldb_core::embedding::{
            EmbeddingGenerationStatus, EmbeddingNormalization, EmbeddingProviderRef,
            GeneratedEmbeddingMetadata, GeneratedEmbeddingValue,
        };

        let tmp = tempfile::tempdir().unwrap();
        let config = engine_config(tmp.path(), 72);
        let sink = open_engine_sink(&config).unwrap();
        let mut sink = sink.lock().unwrap();

        let schema = Schema {
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
                    name: "vec".into(),
                    ty: TypeId::Embedding { dim: 2 },
                    flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                    default_value: None,
                    embedding_source: None,
                },
            ],
            clustered: true,
            ..Schema::default()
        };
        let binding = sink
            .bind_tablet_user_table(TabletTableBinding::new(
                mongreldb_types::ids::TabletId::from_bytes([3; 16]),
                3,
                1,
                "emb",
                schema,
                TabletPartitionBounds::default(),
            ))
            .unwrap();
        let identity = EmbeddingProviderRef {
            provider_id: "leader-only".into(),
            provider_version: "1".into(),
            model_id: "fixed".into(),
            model_version: "1".into(),
            model_artifact_sha256: [1; 32],
            tokenizer_sha256: [2; 32],
            preprocessing_sha256: [3; 32],
            dimension: 2,
            normalization: EmbeddingNormalization::None,
        };
        let generated = GeneratedEmbeddingValue {
            vector: vec![0.25, 0.75],
            metadata: GeneratedEmbeddingMetadata {
                provider_id: "leader-only".into(),
                model_id: "fixed".into(),
                model_version: "1".into(),
                preprocessing_version: "1".into(),
                source_fingerprint: [9; 32],
                status: EmbeddingGenerationStatus::Ready,
                last_error_category: None,
                attempt_count: 1,
                semantic_identity: identity,
                provider_registry_generation: 1,
            },
        };
        // ClusterReplica has no provider registry entry for "leader-only"; apply
        // must accept the leader-resolved value verbatim (no follower inference).
        sink.apply_tablet_writes(
            TabletWriteCommandRecord::new(vec![TabletWriteOperation::Put {
                table_id: binding.local_table_id,
                row_id: 1,
                cells: vec![
                    (1, Value::Int64(1)),
                    (2, Value::GeneratedEmbedding(Box::new(generated.clone()))),
                ],
            }]),
            LogPosition { term: 1, index: 1 },
            tablet_ts(1),
        )
        .unwrap();
        let rows = sink.tablet_typed_rows().unwrap();
        match rows.get(&1).and_then(|c| c.get(&2)) {
            Some(Value::GeneratedEmbedding(got)) => {
                assert_eq!(got.vector, generated.vector);
                assert_eq!(got.metadata.status, EmbeddingGenerationStatus::Ready);
            }
            other => panic!("expected GeneratedEmbedding, got {other:?}"),
        }
    }

    /// ID: P0.3-X6 — RLS-hidden row is excluded before local top-k on the typed
    /// tablet AI path (candidate authorization filters contributions so a
    /// nearer hidden neighbor cannot occupy the k=1 slot).
    #[test]
    fn p03_x6_rls_hidden_row_excluded_before_local_topk_on_typed_tablet() {
        use mongreldb_core::auth::Principal;
        use mongreldb_core::query::Retriever;
        use mongreldb_core::schema::{IndexDef, IndexKind};
        use mongreldb_core::security::{
            CandidateAuthorization, PolicyCommand, RowPolicy, SecurityCatalog, SecurityExpr,
        };

        let tmp = tempfile::tempdir().unwrap();
        let sink = open_engine_sink(&engine_config(tmp.path(), 75)).unwrap();
        let mut sink = sink.lock().unwrap();

        let schema = Schema {
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
                ColumnDef {
                    id: 3,
                    name: "vec".into(),
                    ty: TypeId::Embedding { dim: 4 },
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
            ],
            clustered: true,
            indexes: vec![IndexDef {
                name: "vec_ann".into(),
                column_id: 3,
                kind: IndexKind::Ann,
                predicate: None,
                options: Default::default(),
            }],
            ..Schema::default()
        };
        let binding = sink
            .bind_tablet_user_table(TabletTableBinding::new(
                mongreldb_types::ids::TabletId::from_bytes([0x75; 16]),
                75,
                1,
                "docs",
                schema,
                TabletPartitionBounds::default(),
            ))
            .unwrap();
        let table_id = binding.local_table_id;
        // Row 1 (bob): nearest to query [1,0,0,0] but RLS-hidden for alice.
        // Row 2 (alice): farther, but the only visible candidate — must fill k=1.
        sink.apply_tablet_writes(
            TabletWriteCommandRecord::new(vec![
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 1,
                    cells: vec![
                        (1, Value::Int64(1)),
                        (2, Value::Bytes(b"bob".to_vec())),
                        (3, Value::Embedding(vec![1.0, 0.0, 0.0, 0.0])),
                    ],
                },
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 2,
                    cells: vec![
                        (1, Value::Int64(2)),
                        (2, Value::Bytes(b"alice".to_vec())),
                        (3, Value::Embedding(vec![0.6, 0.4, 0.0, 0.0])),
                    ],
                },
            ]),
            LogPosition { term: 1, index: 1 },
            tablet_ts(10),
        )
        .unwrap();

        let security = SecurityCatalog {
            rls_tables: vec!["docs".into()],
            policies: vec![RowPolicy {
                name: "owner_only".into(),
                table: "docs".into(),
                command: PolicyCommand::Select,
                subjects: vec!["public".into()],
                permissive: true,
                using: Some(SecurityExpr::ColumnEqCurrentUser { column: 2 }),
                with_check: None,
            }],
            masks: Vec::new(),
        };
        let alice = Principal {
            user_id: 1,
            created_epoch: 1,
            username: "alice".into(),
            is_admin: false,
            roles: vec!["public".into()],
            permissions: Vec::new(),
        };
        let auth = CandidateAuthorization {
            table: "docs",
            security: &security,
            principal: &alice,
        };

        let db = sink.database().unwrap();
        let handle = db.table("docs").unwrap();
        let table = handle.lock();
        let hits = table
            .retrieve_at_with_candidate_authorization_on_generation(
                &Retriever::Ann {
                    column_id: 3,
                    query: vec![1.0, 0.0, 0.0, 0.0],
                    k: 1,
                },
                table.snapshot(),
                Some(&auth),
                None,
            )
            .unwrap();
        assert_eq!(
            hits.len(),
            1,
            "local top-k must fill with visible rows: {hits:?}"
        );
        assert_eq!(
            hits[0].row_id,
            RowId(2),
            "RLS-hidden nearer neighbor (row 1) must not occupy top-1"
        );
        // Without RLS the nearer bob row would win — sanity that authorization
        // is what excluded it (not index ordering).
        let open = table
            .retrieve_at_with_candidate_authorization_on_generation(
                &Retriever::Ann {
                    column_id: 3,
                    query: vec![1.0, 0.0, 0.0, 0.0],
                    k: 1,
                },
                table.snapshot(),
                None,
                None,
            )
            .unwrap();
        assert_eq!(open[0].row_id, RowId(1), "without RLS nearest is bob");
    }

    /// ID: P0.3-X9 — after typed snapshot install, the child sink answers a
    /// query-like API (`tablet_typed_rows` + native `Query` / bitmap path), not
    /// only opaque keyspace reads.
    #[test]
    fn p03_x9_split_child_answers_typed_rows_and_query_api() {
        use mongreldb_core::query::{Condition, Query};
        use mongreldb_core::schema::{IndexDef, IndexKind};

        let src_tmp = tempfile::tempdir().unwrap();
        let dst_tmp = tempfile::tempdir().unwrap();
        let src = open_engine_sink(&engine_config(src_tmp.path(), 76)).unwrap();
        let mut src = src.lock().unwrap();

        let schema = Schema {
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
            clustered: true,
            indexes: vec![IndexDef {
                name: "cat_bm".into(),
                column_id: 2,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            }],
            ..Schema::default()
        };
        let binding = src
            .bind_tablet_user_table(TabletTableBinding::new(
                mongreldb_types::ids::TabletId::from_bytes([0x76; 16]),
                76,
                1,
                "catalog",
                schema,
                TabletPartitionBounds {
                    low: Some(b"a".to_vec()),
                    high: Some(b"m".to_vec()),
                },
            ))
            .unwrap();
        let table_id = binding.local_table_id;
        src.apply_tablet_writes(
            TabletWriteCommandRecord::new(vec![
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 1,
                    cells: vec![(1, Value::Int64(1)), (2, Value::Int64(10))],
                },
                TabletWriteOperation::Put {
                    table_id,
                    row_id: 2,
                    cells: vec![(1, Value::Int64(2)), (2, Value::Int64(20))],
                },
            ]),
            LogPosition { term: 1, index: 1 },
            tablet_ts(50),
        )
        .unwrap();

        let snapshot = src.export_typed_tablet_snapshot().unwrap();
        let dst = open_engine_sink(&engine_config(dst_tmp.path(), 77)).unwrap();
        let mut dst = dst.lock().unwrap();
        dst.install_typed_tablet_snapshot(
            &snapshot,
            LogPosition { term: 1, index: 10 },
            tablet_ts(50),
        )
        .unwrap();

        // Query-like API #1: typed_rows projection.
        let typed = dst.tablet_typed_rows().unwrap();
        assert_eq!(typed.len(), 2);
        assert_eq!(typed[&1].get(&2), Some(&Value::Int64(10)));
        assert_eq!(typed[&2].get(&2), Some(&Value::Int64(20)));

        // Query-like API #2: native Query / bitmap against the child table.
        let db = dst.database().unwrap();
        let query = Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: Value::Int64(10).encode_key(),
        });
        let rows = db
            .query_for_current_principal("catalog", &query, Some(&[1, 2]))
            .unwrap();
        assert_eq!(rows.len(), 1, "child bitmap query: {rows:?}");
        assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(1)));
    }

    /// ID: P0.3-X10 — merge/split snapshot preserves index definitions and
    /// `index_generations` on the installed child binding.
    #[test]
    fn p03_x10_merge_preserves_index_definitions_in_binding() {
        use mongreldb_core::schema::{IndexDef, IndexKind};
        use std::collections::BTreeMap;

        let src_tmp = tempfile::tempdir().unwrap();
        let dst_tmp = tempfile::tempdir().unwrap();
        let src = open_engine_sink(&engine_config(src_tmp.path(), 78)).unwrap();
        let mut src = src.lock().unwrap();

        let schema = Schema {
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
                    name: "tag".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
            ],
            clustered: true,
            indexes: vec![
                IndexDef {
                    name: "tag_bm".into(),
                    column_id: 2,
                    kind: IndexKind::Bitmap,
                    predicate: None,
                    options: Default::default(),
                },
                IndexDef {
                    name: "id_bm".into(),
                    column_id: 1,
                    kind: IndexKind::Bitmap,
                    predicate: None,
                    options: Default::default(),
                },
            ],
            ..Schema::default()
        };
        let mut binding = TabletTableBinding::new(
            mongreldb_types::ids::TabletId::from_bytes([0x78; 16]),
            78,
            3,
            "items",
            schema,
            TabletPartitionBounds::default(),
        );
        binding.index_generations =
            BTreeMap::from([("tag_bm".to_owned(), 4), ("id_bm".to_owned(), 9)]);
        let binding = src.bind_tablet_user_table(binding).unwrap();
        src.apply_tablet_writes(
            TabletWriteCommandRecord::new(vec![TabletWriteOperation::Put {
                table_id: binding.local_table_id,
                row_id: 1,
                cells: vec![(1, Value::Int64(1)), (2, Value::Bytes(b"x".to_vec()))],
            }]),
            LogPosition { term: 1, index: 1 },
            tablet_ts(1),
        )
        .unwrap();

        let snapshot = src.export_typed_tablet_snapshot().unwrap();
        assert_eq!(snapshot.binding.index_generations.get("tag_bm"), Some(&4));
        assert_eq!(snapshot.binding.index_generations.get("id_bm"), Some(&9));
        let idx_names: Vec<_> = snapshot
            .binding
            .schema
            .indexes
            .iter()
            .map(|i| i.name.as_str())
            .collect();
        assert_eq!(idx_names, vec!["tag_bm", "id_bm"]);

        // Wire round-trip (merge transport) must keep both fields.
        let decoded = TypedTabletSnapshot::decode(&snapshot.encode().unwrap()).unwrap();
        assert_eq!(
            decoded.binding.index_generations,
            snapshot.binding.index_generations
        );
        assert_eq!(decoded.binding.schema.indexes.len(), 2);

        let dst = open_engine_sink(&engine_config(dst_tmp.path(), 79)).unwrap();
        let mut dst = dst.lock().unwrap();
        dst.install_typed_tablet_snapshot(
            &decoded,
            LogPosition { term: 2, index: 1 },
            tablet_ts(1),
        )
        .unwrap();
        let child = dst.tablet_table_binding().unwrap();
        assert_eq!(
            child.index_generations,
            BTreeMap::from([("tag_bm".to_owned(), 4), ("id_bm".to_owned(), 9)])
        );
        let child_idx: Vec<_> = child
            .schema
            .indexes
            .iter()
            .map(|i| (i.name.as_str(), i.kind, i.column_id))
            .collect();
        assert_eq!(
            child_idx,
            vec![
                ("tag_bm", IndexKind::Bitmap, 2),
                ("id_bm", IndexKind::Bitmap, 1),
            ]
        );
        // Child is queryable after install with preserved index defs.
        assert_eq!(dst.tablet_typed_rows().unwrap().len(), 1);
    }
}
