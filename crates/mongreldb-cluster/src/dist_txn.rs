//! Replicated two-phase commit for distributed transactions (spec section
//! 12.8, Stage 3H; ADR-0007).
//!
//! Once a table is partitioned across tablets (ADR-0006), one transaction can
//! write rows owned by different raft groups. This module implements the
//! atomic commit protocol for that case:
//!
//! - **Coordinator record.** Every distributed transaction owns one
//!   [`TxnRecord`], replicated through a dedicated transaction-status raft
//!   group (spec section 12.1 lists transaction-status partitions as
//!   control-plane state; [`crate::meta::TxnStatusPartition`] names the home
//!   group). The record's [`DistributedTxnState`] is the single authoritative
//!   outcome: `Pending` → `Preparing` → `Committed { commit_ts }` or
//!   `Aborted { reason }`, and a terminal state is final.
//! - **Write intents.** Each participant persists the transaction's
//!   [`WriteIntent`]s through its own raft group before answering prepare
//!   (phase 1), so a prepared participant can never lose the evidence of an
//!   in-flight transaction. The prepare answer is a [`PrepareToken`]:
//!   prepare timestamp plus the raft position that proves durability.
//! - **Decision.** When every participant prepared, the coordinator chooses
//!   `commit_ts` strictly greater than every observed participant
//!   prepare/read/write timestamp via [`HlcClock::next_after`] (spec section
//!   8.2) and persists `Committed` through its raft group (phase 2). The
//!   client is answered only after the decision is durable. Participants
//!   resolve intents from the durable decision — driven both by the
//!   coordinator's resolve broadcast and by lazy recovery.
//! - **Recovery.** The coordinator record is raft-replicated, so a
//!   coordinator leadership change does not lose it: the new leader
//!   continues the protocol ([`DistTxnDriver::recover`]). Any node may query
//!   the record ([`TxnStatusGroup::record_consistent`], linearizable or
//!   session-token). Orphaned intents are resolved only through the record:
//!   a terminal decision resolves immediately; a non-terminal transaction is
//!   pushed to `Aborted` only under the documented heartbeat-expiry rule
//!   ([`DistTxnDriver::push_expired`]) — never on suspicion.
//! - **Client outcome.** A commit returns [`TxnOutcome`] (transaction id,
//!   commit timestamp, participant set, durability). Ambiguous transport
//!   failures resolve by re-running with the same transaction id and
//!   idempotency key: every protocol step rides a deterministic command id,
//!   so a retry replays the original raft entries (S2B-004 idempotent apply)
//!   and converges to the original outcome without double-apply.
//!
//! # Single-participant fast path
//!
//! A transaction touching one tablet runs the same durable machinery (begin,
//! intent persistence, decision) but the coordinator skips the
//! `MarkPreparing` progress write: with one participant there is no partial
//! prepare progress a failover could resume from — recovery re-probes the
//! single participant's intent record instead. That saves one raft round
//! trip versus the general path. (Single-*tablet* engine transactions that
//! never leave Stage 1B do not enter this protocol at all; ADR-0007.)
//!
//! # Abort rules (never on suspicion)
//!
//! A third party may force `Aborted` only when the coordinator record shows
//! a non-terminal transaction whose heartbeat `expiry` has passed
//! (spec section 12.8 "push expired pending transaction"). The push is
//! itself a replicated record transition racing the live protocol through
//! the same total order; whichever decision lands first stands. A
//! non-expired transaction is left to its coordinator.
//!
//! # Scope of this wave
//!
//! The engine binding is landed for the participant side: an
//! [`IntentApplySink`] bound to an [`EngineApplySink`] (a
//! [`TabletTxnGroup`]) applies a committed resolution's staged writes into
//! the tablet's engine core through `Database::apply_staged_txn_writes` —
//! the same `apply_replicated_records` path the engine sink uses — stamped
//! at the decision's `commit_ts`. Staged-write payloads are the
//! engine-defined `mongreldb_core::database::StagedTxnWrite` encoding; they
//! are validated at prepare (a malformed payload is refused deterministically,
//! never after the decision commits) and applied at most once (the intent
//! record tracks `applied`; a replayed or redelivered resolve never
//! double-applies). Aborted resolutions drop intents with zero MVCC effect.
//! Resolved intent tombstones are bounded by a replicated sweep
//! ([`IntentCommand::SweepResolved`]) driven with a configurable retention
//! ([`DistTxnDriver::sweep_resolved`]). Prepares fan out concurrently under
//! the caller's deadline; `commit_ts` derives from the greatest prepare
//! timestamp, so the decision is independent of completion order.
//! Coordinator records remain engine-free (they carry no row data).

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use mongreldb_consensus::engine_sink::EngineApplySink;
use mongreldb_consensus::error::ConsensusError;
use mongreldb_consensus::group::{ConsensusGroup, GroupCommitReceipt, GroupConfig};
use mongreldb_consensus::identity::{CommandKind, RaftNodeId, ReplicatedCommand};
use mongreldb_consensus::network::RaftTransport;
use mongreldb_consensus::read::{ReadConsistency, ReadConsistencyError, SessionToken};
use mongreldb_consensus::state_machine::{AppliedCommand, ApplySink, StateMachineError};
use mongreldb_log::commit_log::{DurabilityLevel, ExecutionControl, LogPosition};
use mongreldb_log::envelope::CommandEnvelope;
use mongreldb_types::hlc::{HlcClock, HlcTimestamp};
use mongreldb_types::ids::{RaftGroupId, SchemaVersion, TabletId, TransactionId};

use crate::meta::TxnStatusPartition;
use crate::tablet::TabletDescriptor;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Envelope discriminant for coordinator (transaction-status) records.
/// Discriminants are never reused (spec section 4.10): `1` transaction,
/// `2` catalog, `3` maintenance (engine), `4` meta control-plane; `5` and
/// `6` belong to the distributed transaction protocol.
pub const COMMAND_TYPE_DIST_TXN_COORDINATOR: u32 = 5;
/// Envelope discriminant for participant intent records; see
/// [`COMMAND_TYPE_DIST_TXN_COORDINATOR`].
pub const COMMAND_TYPE_DIST_TXN_INTENT: u32 = 6;

/// Format version of the [`CoordinatorCommandRecord`] / [`IntentCommandRecord`]
/// payloads this build writes.
pub const DIST_TXN_RECORD_FORMAT_VERSION: u32 = 1;
/// Oldest payload format version this build accepts.
pub const MIN_SUPPORTED_DIST_TXN_RECORD_FORMAT_VERSION: u32 = 1;

/// Format version of the sink checkpoints this build writes.
pub const DIST_TXN_CHECKPOINT_FORMAT_VERSION: u32 = 1;
/// Oldest sink checkpoint format version this build accepts.
pub const MIN_SUPPORTED_DIST_TXN_CHECKPOINT_FORMAT_VERSION: u32 = 1;

/// Coordinator sink checkpoint file under `<group dir>/raft/state`.
pub const TXN_STATUS_CHECKPOINT_FILENAME: &str = "dist-txn-status.json";
/// Participant intent sink checkpoint file under `<group dir>/raft/state`.
pub const INTENT_CHECKPOINT_FILENAME: &str = "dist-txn-intents.json";

/// Bound on the rejection journals (mirrors [`crate::meta::META_REJECTION_LIMIT`]).
pub const DIST_TXN_REJECTION_LIMIT: usize = 256;

/// Default heartbeat-expiry window for the pending/push rules.
pub const DEFAULT_PENDING_TIMEOUT: Duration = Duration::from_secs(10);

/// Default retention of resolved intent tombstones before they are swept
/// (spec section 12.8; see [`DistTxnDriver::sweep_resolved`]). The retention
/// must exceed the longest possible prepare→resolve gap: the coordinator
/// record's heartbeat expiry bounds a live transaction's prepare window, so
/// the default dwarfs [`DEFAULT_PENDING_TIMEOUT`]. After a tombstone is
/// swept, a late prepare of the same transaction is no longer refused on
/// sight — the driver API never re-prepares a terminal transaction (a retry
/// short-circuits on the durable coordinator record), so the gap is only
/// reachable by callers driving `prepare_participant` directly.
pub const DEFAULT_RESOLVED_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

/// Default bound on tombstones removed by one sweep command.
pub const DEFAULT_SWEEP_LIMIT: u32 = 1024;

/// Deterministic command-id tags (sha256 domain separation). The command id
/// of every protocol step is a pure function of the transaction id and the
/// step, so a client retry re-proposes the identical id and the state
/// machine's idempotent apply (S2B-004) replays the original outcome.
const TAG_BEGIN: &str = "dist-txn/begin";
const TAG_HEARTBEAT: &str = "dist-txn/heartbeat";
const TAG_MARK: &str = "dist-txn/mark-preparing";
const TAG_COMMIT: &str = "dist-txn/commit";
const TAG_ABORT: &str = "dist-txn/abort";
const TAG_PREPARE: &str = "dist-txn/intents";
const TAG_RESOLVE: &str = "dist-txn/resolve";
const TAG_SWEEP: &str = "dist-txn/sweep-resolved";

/// Derives the deterministic command id of one protocol step.
fn command_id_for(tag: &str, txn_id: &TransactionId, extra: &[u8]) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hasher.update(tag.as_bytes());
    hasher.update(txn_id.as_bytes());
    hasher.update(extra);
    let digest = hasher.finalize();
    digest[..16].try_into().expect("sha256 digest is 32 bytes")
}

/// FNV-1a 64-bit, mirroring the engine's WITHOUT ROWID derivation (the core
/// crate sits above the cluster crate in the dependency graph, so the tiny
/// pure function is mirrored rather than imported).
fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors produced by the distributed transaction protocol.
#[derive(Debug, thiserror::Error)]
pub enum DistTxnError {
    /// Consensus group failure (including the routed
    /// [`ConsensusError::NotLeader`] leader hint, spec section 11.7).
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
    /// Read-barrier failure on a consistent record read.
    #[error(transparent)]
    Read(#[from] ReadConsistencyError),
    /// Encoding a command record failed.
    #[error("distributed transaction command encoding failed: {0}")]
    Encode(String),
    /// The caller's request was malformed for this group or configuration.
    #[error("invalid distributed transaction request: {0}")]
    InvalidRequest(String),
    /// Group I/O failure.
    #[error("distributed transaction group I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// A sink's durable checkpoint failed verification (fails closed, spec
    /// section 4.10).
    #[error("corrupt distributed transaction checkpoint: {0}")]
    CorruptCheckpoint(String),
    /// The transaction aborted; the durable coordinator record carries this
    /// reason. Definitive (never ambiguous).
    #[error("the distributed transaction aborted: {0:?}")]
    Aborted(AbortReason),
    /// A participant refused prepare; the typed reason is also journaled in
    /// the participant's intent state.
    #[error("prepare refused by the participant: {0}")]
    PrepareRejected(PrepareRejectionReason),
    /// The transaction id already exists under a different idempotency key.
    #[error("transaction {0} already exists under a different idempotency key")]
    IdempotencyConflict(TransactionId),
    /// The outcome may already be durable either way (spec section 4.7).
    /// Resolve by re-running commit/recovery with the same transaction id
    /// and idempotency key — never by guessing.
    #[error(
        "transaction {txn_id} outcome is ambiguous ({detail}); \
         re-run with the same transaction id and idempotency key"
    )]
    OutcomeAmbiguous {
        /// The transaction whose outcome is unknown.
        txn_id: TransactionId,
        /// What failed while the outcome was in flight.
        detail: String,
    },
    /// No group member answered as a live leader within the retry budget.
    #[error("no live group leader: {0}")]
    Unavailable(String),
    /// The coordinator's HLC clock could not produce a timestamp.
    #[error("coordinator clock failure: {0}")]
    Clock(String),
}

/// Why a command record payload could not be decoded. Decode failures fail
/// closed (spec section 4.10).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DistTxnDecodeError {
    /// The payload is not a well-formed record.
    #[error("distributed transaction command decode failed: {0}")]
    Malformed(String),
    /// The record's format version is outside the supported range.
    #[error(
        "unsupported distributed transaction record version {found} (supported {min}..={max})"
    )]
    UnsupportedVersion {
        /// Version found in the payload.
        found: u32,
        /// Oldest version this build accepts.
        min: u32,
        /// Newest version this build accepts.
        max: u32,
    },
}

// ---------------------------------------------------------------------------
// Protocol record types (spec section 12.8)
// ---------------------------------------------------------------------------

/// Why a transaction ended without a commit (mirrors the engine's
/// `AbortReason` in `mongreldb-core::txn`, made serde-capable for
/// replication; a heartbeat-expiry push aborts with
/// `Cancelled("heartbeat expired …")`, the documented deadline case).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AbortReason {
    /// Explicit rollback, or the transaction was dropped while still active.
    RolledBack,
    /// Write/write conflict (first-committer-wins) or an SSI serialization
    /// failure detected at commit.
    Conflict(String),
    /// Constraint, authorization, or catalog validation failed before the
    /// commit fence.
    Validation(String),
    /// Cancellation or deadline before the commit fence (including
    /// heartbeat-expiry pushes).
    Cancelled(String),
    /// Any other pre-fence failure.
    Error(String),
}

/// Replicated transaction state (spec section 12.8). A terminal state
/// (`Committed` / `Aborted`) is final: the apply path never rewrites it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistributedTxnState {
    /// Record created; no decision and no recorded prepare progress yet.
    Pending,
    /// At least one participant's prepare is recorded; no decision yet.
    Preparing,
    /// Durable commit decision at `commit_ts` (strictly greater than every
    /// observed participant prepare/read/write timestamp).
    Committed {
        /// The HLC commit timestamp (spec section 8.2).
        commit_ts: HlcTimestamp,
    },
    /// Durable abort decision.
    Aborted {
        /// Why the transaction aborted.
        reason: AbortReason,
    },
}

impl DistributedTxnState {
    /// Whether the state is a final decision.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            DistributedTxnState::Committed { .. } | DistributedTxnState::Aborted { .. }
        )
    }
}

/// One tablet participating in a distributed transaction (carried with its
/// raft group so participants are addressable from the record alone).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnParticipant {
    /// The tablet owning the written keys.
    pub tablet_id: TabletId,
    /// The raft group replicating that tablet.
    pub raft_group_id: RaftGroupId,
}

/// The replicated coordinator record (spec section 12.8). One record per
/// transaction, owned by the coordinator's transaction-status group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnRecord {
    /// The transaction's identity.
    pub txn_id: TransactionId,
    /// Protocol state; terminal states are final.
    pub state: DistributedTxnState,
    /// Every tablet the transaction writes (with its raft group).
    pub participants: Vec<TxnParticipant>,
    /// Recorded per-participant prepare timestamps (populated by
    /// `MarkPreparing` on the general path; intentionally empty on the
    /// single-participant fast path, where recovery re-probes the
    /// participant's intent record).
    pub prepare_ts: BTreeMap<TabletId, HlcTimestamp>,
    /// The raft group coordinating this transaction.
    pub coordinator: RaftGroupId,
    /// When the record was created (coordinator HLC).
    pub created_at: HlcTimestamp,
    /// Last heartbeat (coordinator HLC); `expiry` derives from it.
    pub heartbeat: HlcTimestamp,
    /// Heartbeat-expiry deadline. A non-terminal record with
    /// `now >= expiry` may be pushed to `Aborted` by any node (the
    /// documented timeout rule); before `expiry`, never.
    pub expiry: HlcTimestamp,
    /// Greatest read/write timestamp the transaction observed, as durably
    /// reported by its coordinator driver; `commit_ts` must exceed it.
    pub max_observed: HlcTimestamp,
    /// The client's idempotency key: a retry under the same key replays the
    /// original outcome; the same transaction id under a different key
    /// conflicts.
    pub idempotency_key: [u8; 16],
}

impl TxnRecord {
    /// Whether the heartbeat expiry passed at `now` (the push rule's gate).
    pub fn expired(&self, now: HlcTimestamp) -> bool {
        now >= self.expiry
    }

    /// The participant entry for one tablet, if any.
    pub fn participant(&self, tablet_id: &TabletId) -> Option<&TxnParticipant> {
        self.participants.iter().find(|p| p.tablet_id == *tablet_id)
    }
}

/// A staged write persisted through the participant's raft group (phase 1).
/// Durable before the prepare response is sent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteIntent {
    /// The owning transaction.
    pub txn_id: TransactionId,
    /// The tablet-local row key.
    pub key: Vec<u8>,
    /// The staged value: opaque to this layer, interpreted only by the
    /// engine binding. The contract is the engine-defined
    /// `mongreldb_core::database::StagedTxnWrite` encoding (a staged row put
    /// or delete); an engine-backed participant validates it at prepare and
    /// applies it at a committed resolution.
    pub value_ref: Vec<u8>,
    /// Prepare timestamp stamped by the coordinator driver that proposed
    /// the intent (identical on every replica).
    pub prepare_ts: HlcTimestamp,
}

/// Proof that a participant durably prepared (the prepare answer of
/// phase 1): prepare timestamp plus the raft position of the intent record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepareToken {
    /// The prepared transaction.
    pub txn_id: TransactionId,
    /// The prepared tablet.
    pub tablet_id: TabletId,
    /// The tablet's raft group.
    pub raft_group_id: RaftGroupId,
    /// The participant's prepare timestamp (as durably stored).
    pub prepare_ts: HlcTimestamp,
    /// Raft position of the persisted intent record.
    pub position: LogPosition,
    /// The deterministic command id of the persisted intent record.
    pub command_id: [u8; 16],
}

/// A durable decision delivered to a participant for resolution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxnDecision {
    /// Commit at `commit_ts`: intents become visible versions.
    Committed {
        /// The coordinator-record commit timestamp.
        commit_ts: HlcTimestamp,
    },
    /// Abort: intents are removed.
    Aborted {
        /// The coordinator-record abort reason.
        reason: AbortReason,
    },
}

/// What a committed client receives (spec section 12.8 "Client outcome").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOutcome {
    /// The committed transaction.
    pub txn_id: TransactionId,
    /// The durable commit timestamp.
    pub commit_ts: HlcTimestamp,
    /// Every participant that prepared.
    pub participants: Vec<TxnParticipant>,
    /// The durability the decision achieved (quorum commit + apply).
    pub durability: DurabilityLevel,
}

/// One participant's intent-holding state for one transaction (replicated
/// through the participant's raft group).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParticipantTxn {
    /// The transaction.
    pub txn_id: TransactionId,
    /// The stored prepare timestamp (the prepare answer's authoritative
    /// value; an idempotent prepare replay keeps the original).
    pub prepare_ts: HlcTimestamp,
    /// Command id of the entry that persisted the intents.
    pub command_id: [u8; 16],
    /// The staged write intents (emptied once resolved).
    pub intents: Vec<WriteIntent>,
    /// The applied decision, once resolved. A record carrying a decision is
    /// a tombstone: it bars any later prepare of the same transaction
    /// (resolution always wins a resolve/prepare race).
    pub resolution: Option<TxnDecision>,
    /// Whether the resolution is fully materialized: a committed decision's
    /// staged writes were applied to the engine core (or there was nothing
    /// to apply); an aborted decision's intents were dropped. A replayed or
    /// redelivered resolve never applies twice (idempotency across retries
    /// and restarts). Defaults to `false` when decoding pre-binding
    /// checkpoints so an upgraded sink re-materializes on the next resolve
    /// replay.
    #[serde(default)]
    pub applied: bool,
    /// When the resolution applied (the resolve command's leader-assigned
    /// timestamp), feeding the resolved-tombstone sweep.
    #[serde(default)]
    pub resolved_at: Option<HlcTimestamp>,
}

/// One write made visible by a committed resolution (the resolution
/// contract the engine binding consumes).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedWrite {
    /// The tablet-local row key.
    pub key: Vec<u8>,
    /// The staged value reference from the intent.
    pub value_ref: Vec<u8>,
    /// The visibility timestamp (the transaction's `commit_ts`).
    pub commit_ts: HlcTimestamp,
    /// The transaction that wrote it.
    pub txn_id: TransactionId,
}

// ---------------------------------------------------------------------------
// Coordinator selection (spec section 12.8 "Transaction coordinator")
// ---------------------------------------------------------------------------

/// How the coordinator group is chosen for a transaction (deterministic in
/// both modes; [`CoordinatorSelection::TxnIdDerived`] is the default).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CoordinatorSelection {
    /// The transaction-status shard derived from the transaction id:
    /// `fnv1a(txn_id) % status_partition_count`, mapped onto the
    /// partitions sorted by partition id.
    #[default]
    TxnIdDerived,
    /// The home tablet of the transaction's first write.
    FirstWriteHome,
}

/// The transaction-status partition index of one transaction id:
/// `fnv1a(txn_id) % partition_count` (spec section 12.8).
pub fn txn_status_partition_index(txn_id: &TransactionId, partition_count: u32) -> u32 {
    assert!(partition_count > 0, "partition count must be positive");
    u32::try_from(fnv1a_64(txn_id.as_bytes()) % u64::from(partition_count))
        .expect("remainder below partition count")
}

/// Chooses the coordinator group for one transaction (deterministic; every
/// node computes the same answer for the same transaction without
/// consultation, which is what makes decentralized recovery possible).
pub fn select_coordinator_group(
    selection: &CoordinatorSelection,
    txn_id: &TransactionId,
    first_write_tablet: Option<&TabletDescriptor>,
    partitions: &BTreeMap<u32, TxnStatusPartition>,
) -> Result<RaftGroupId, DistTxnError> {
    match selection {
        CoordinatorSelection::TxnIdDerived => {
            if partitions.is_empty() {
                return Err(DistTxnError::InvalidRequest(
                    "no transaction-status partitions are published in meta state".to_owned(),
                ));
            }
            let index = txn_status_partition_index(
                txn_id,
                u32::try_from(partitions.len()).unwrap_or(u32::MAX),
            );
            partitions
                .values()
                .nth(index as usize)
                .map(|partition| partition.home_raft_group)
                .ok_or_else(|| {
                    DistTxnError::InvalidRequest(format!(
                        "transaction-status partition index {index} is not published"
                    ))
                })
        }
        CoordinatorSelection::FirstWriteHome => first_write_tablet
            .map(|tablet| tablet.raft_group_id)
            .ok_or_else(|| {
                DistTxnError::InvalidRequest(
                    "first-write-home coordinator selection requires the first write's tablet \
                     descriptor"
                        .to_owned(),
                )
            }),
    }
}

// ---------------------------------------------------------------------------
// Replicated commands and their versioned payload records
// ---------------------------------------------------------------------------

/// One transition of the coordinator record, replicated through the
/// transaction-status group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CoordinatorCommand {
    /// Creates the `Pending` record (with expiry and the client's
    /// idempotency key). A replay under the same key is a no-op; the same
    /// transaction id under a different key is journaled as
    /// [`StatusRejectionReason::IdempotencyKeyConflict`].
    Begin {
        /// The record to create (`state` must be `Pending`).
        record: TxnRecord,
    },
    /// Refreshes `heartbeat` and `expiry` of a non-terminal record.
    Heartbeat {
        /// The transaction.
        txn_id: TransactionId,
        /// The new heartbeat timestamp.
        heartbeat: HlcTimestamp,
        /// The new expiry (`heartbeat + pending timeout`).
        expiry: HlcTimestamp,
    },
    /// Records one participant's prepare (general path; skipped on the
    /// single-participant fast path). Moves the record to `Preparing`.
    MarkPreparing {
        /// The transaction.
        txn_id: TransactionId,
        /// The prepared tablet.
        tablet_id: TabletId,
        /// The participant's prepare timestamp.
        prepare_ts: HlcTimestamp,
        /// The driver's greatest observed timestamp so far (folded into
        /// [`TxnRecord::max_observed`]).
        observed: HlcTimestamp,
    },
    /// The durable commit decision (phase 2). `commit_ts` is chosen by the
    /// coordinator strictly greater than every observed timestamp.
    Commit {
        /// The transaction.
        txn_id: TransactionId,
        /// The commit timestamp.
        commit_ts: HlcTimestamp,
    },
    /// The durable abort decision.
    Abort {
        /// The transaction.
        txn_id: TransactionId,
        /// Why the transaction aborted.
        reason: AbortReason,
    },
}

/// One transition of a participant's intent state, replicated through the
/// participant's raft group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntentCommand {
    /// Publishes the tablet's current schema/authorization versions
    /// (prepare validates against them; spec section 12.11's stale-schema
    /// rule surfaces as [`PrepareRejectionReason::StaleSchemaVersion`]).
    SetTabletVersions {
        /// Current schema version.
        schema_version: SchemaVersion,
        /// Current authorization version.
        authz_version: u64,
    },
    /// Persists the transaction's write intents (phase 1). Validates
    /// schema/authorization versions and checks write/write conflicts
    /// against every unresolved intent of other transactions.
    PersistIntents {
        /// The transaction.
        txn_id: TransactionId,
        /// Schema version the transaction planned against.
        expected_schema_version: SchemaVersion,
        /// Authorization version the transaction authenticated against.
        expected_authz_version: u64,
        /// The prepare timestamp (stamped by the proposing driver, applied
        /// identically on every replica).
        prepare_ts: HlcTimestamp,
        /// The write intents.
        intents: Vec<WriteIntent>,
    },
    /// Applies the durable decision: commit makes intents visible at
    /// `commit_ts`; abort removes them. Idempotent; a conflicting decision
    /// for an already-resolved transaction fails closed.
    Resolve {
        /// The transaction.
        txn_id: TransactionId,
        /// The durable decision.
        decision: TxnDecision,
    },
    /// Sweeps resolved intent tombstones (and their `committed_writes`
    /// entries) whose `resolved_at` is older than `older_than`, at most
    /// `limit` per command, keeping the participant state bounded
    /// (spec section 12.8's orphan machinery plus retention). Deterministic:
    /// every replica sweeps the identical prefix in transaction-id order.
    SweepResolved {
        /// Sweep tombstones resolved before this timestamp.
        older_than: HlcTimestamp,
        /// Maximum tombstones to remove.
        limit: u32,
    },
}

/// The versioned envelope payload carrying one [`CoordinatorCommand`]
/// (spec section 4.10). Serialized as JSON into a [`CommandEnvelope`]
/// stamped with [`COMMAND_TYPE_DIST_TXN_COORDINATOR`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinatorCommandRecord {
    /// Format version; see [`DIST_TXN_RECORD_FORMAT_VERSION`].
    pub format_version: u32,
    /// The command.
    pub command: CoordinatorCommand,
}

/// The versioned envelope payload carrying one [`IntentCommand`]; see
/// [`CoordinatorCommandRecord`]. Stamped with [`COMMAND_TYPE_DIST_TXN_INTENT`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntentCommandRecord {
    /// Format version; see [`DIST_TXN_RECORD_FORMAT_VERSION`].
    pub format_version: u32,
    /// The command.
    pub command: IntentCommand,
}

fn encode_record<T: Serialize>(record: &T) -> Result<Vec<u8>, DistTxnError> {
    serde_json::to_vec(record).map_err(|error| DistTxnError::Encode(error.to_string()))
}

fn decode_record<T: for<'de> Deserialize<'de>>(payload: &[u8]) -> Result<T, DistTxnDecodeError> {
    serde_json::from_slice(payload)
        .map_err(|error| DistTxnDecodeError::Malformed(error.to_string()))
}

fn check_record_version(found: u32) -> Result<(), DistTxnDecodeError> {
    if !(MIN_SUPPORTED_DIST_TXN_RECORD_FORMAT_VERSION..=DIST_TXN_RECORD_FORMAT_VERSION)
        .contains(&found)
    {
        return Err(DistTxnDecodeError::UnsupportedVersion {
            found,
            min: MIN_SUPPORTED_DIST_TXN_RECORD_FORMAT_VERSION,
            max: DIST_TXN_RECORD_FORMAT_VERSION,
        });
    }
    Ok(())
}

impl CoordinatorCommandRecord {
    /// Wraps `command` at the current format version.
    pub fn new(command: CoordinatorCommand) -> Self {
        CoordinatorCommandRecord {
            format_version: DIST_TXN_RECORD_FORMAT_VERSION,
            command,
        }
    }

    /// Encodes the record for the envelope payload.
    pub fn encode(&self) -> Result<Vec<u8>, DistTxnError> {
        encode_record(self)
    }

    /// Decodes an envelope payload, failing closed on malformed input and
    /// unsupported versions.
    pub fn decode(payload: &[u8]) -> Result<Self, DistTxnDecodeError> {
        let record: CoordinatorCommandRecord = decode_record(payload)?;
        check_record_version(record.format_version)?;
        Ok(record)
    }
}

impl IntentCommandRecord {
    /// Wraps `command` at the current format version.
    pub fn new(command: IntentCommand) -> Self {
        IntentCommandRecord {
            format_version: DIST_TXN_RECORD_FORMAT_VERSION,
            command,
        }
    }

    /// Encodes the record for the envelope payload.
    pub fn encode(&self) -> Result<Vec<u8>, DistTxnError> {
        encode_record(self)
    }

    /// Decodes an envelope payload, failing closed on malformed input and
    /// unsupported versions.
    pub fn decode(payload: &[u8]) -> Result<Self, DistTxnDecodeError> {
        let record: IntentCommandRecord = decode_record(payload)?;
        check_record_version(record.format_version)?;
        Ok(record)
    }
}

// ---------------------------------------------------------------------------
// Apply rejections (journaled state, never state-machine errors)
// ---------------------------------------------------------------------------

/// Why the coordinator apply path refused a command. Refusals are journaled
/// in [`TxnStatusState::rejections`]; the raft entry commits normally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum StatusRejectionReason {
    /// `Begin` for a transaction id that already exists under a different
    /// idempotency key.
    #[error("transaction already exists under a different idempotency key")]
    IdempotencyKeyConflict {
        /// The key the existing record holds.
        existing: [u8; 16],
    },
    /// A transition named a transaction with no record.
    #[error("unknown transaction {0}")]
    UnknownTxn(TransactionId),
    /// A transition arrived over a terminal record (the existing decision
    /// stands; journaled so the proposer learns it).
    #[error("the transaction already reached a final decision: {existing:?}")]
    DecisionFinal {
        /// The final state already held.
        existing: DistributedTxnState,
    },
    /// `MarkPreparing` named a tablet that is not a participant.
    #[error("tablet {0} is not a participant of the transaction")]
    UnknownParticipant(TabletId),
    /// Commit apply re-validation failed: `commit_ts` is not strictly greater
    /// than every prepare/observed timestamp (review finding **D2**).
    #[error(
        "invalid commit_ts {commit_ts}: must be strictly greater than max_observed {max_observed} \
         and every prepare timestamp"
    )]
    InvalidCommitTs {
        /// The refused commit timestamp.
        commit_ts: HlcTimestamp,
        /// The record's max observed timestamp.
        max_observed: HlcTimestamp,
    },
}

/// One journaled coordinator rejection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusRejection {
    /// Raft position of the refused entry.
    pub position: LogPosition,
    /// The refused command's id.
    pub command_id: Option<[u8; 16]>,
    /// The transaction concerned.
    pub txn_id: TransactionId,
    /// Why it was refused.
    pub reason: StatusRejectionReason,
}

/// Why a participant apply path refused `PersistIntents`. Journaled in
/// [`IntentState::rejections`]; the raft entry commits normally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum PrepareRejectionReason {
    /// Another unresolved transaction already holds an intent on the key
    /// (write/write conflict; first-preparer-wins).
    #[error("key conflict: another transaction holds a write intent on the key")]
    KeyConflict {
        /// The contested key.
        key: Vec<u8>,
        /// The transaction holding the intent.
        holder: TransactionId,
    },
    /// A replay carried a different write set for the same transaction.
    #[error("the transaction already prepared with a different write set")]
    PayloadMismatch,
    /// The transaction is already resolved (a prepare that lost the race
    /// against the decision; the resolution always wins).
    #[error("the transaction is already resolved: {decision:?}")]
    AlreadyResolved {
        /// The resolution already applied.
        decision: TxnDecision,
    },
    /// The transaction's schema version is stale (spec section 12.11's
    /// structured retry error surface).
    #[error("stale schema version {expected:?} (tablet is at {found:?})")]
    StaleSchemaVersion {
        /// The version the transaction planned against.
        expected: SchemaVersion,
        /// The tablet's current version.
        found: SchemaVersion,
    },
    /// The transaction's authorization version is stale.
    #[error("stale authorization version {expected} (tablet is at {found})")]
    StaleAuthzVersion {
        /// The version the transaction authenticated against.
        expected: u64,
        /// The tablet's current version.
        found: u64,
    },
    /// A staged-write payload failed the engine binding's prepare-time
    /// validation (engine-backed participants only): it does not decode as
    /// the engine's staged-write contract, targets an unmounted table, or
    /// carries a row the apply path would reject. Refused at prepare so an
    /// un-appliable payload can never reach a committed resolution.
    #[error("staged write is not appliable: {detail}")]
    MalformedStagedWrite {
        /// The validation failure.
        detail: String,
    },
}

/// One journaled prepare rejection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepareRejection {
    /// Raft position of the refused entry.
    pub position: LogPosition,
    /// The refused command's id.
    pub command_id: Option<[u8; 16]>,
    /// The transaction concerned.
    pub txn_id: TransactionId,
    /// Why it was refused.
    pub reason: PrepareRejectionReason,
}

// ---------------------------------------------------------------------------
// Coordinator (transaction-status) apply sink
// ---------------------------------------------------------------------------

/// The replicated coordinator state: one record per transaction plus the
/// bounded rejection journal.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnStatusState {
    /// Coordinator records by transaction id.
    pub records: BTreeMap<TransactionId, TxnRecord>,
    /// Bounded journal of refused transitions (newest last).
    pub rejections: VecDeque<StatusRejection>,
}

/// The coordinator sink's durable checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TxnStatusCheckpoint {
    format_version: u32,
    position: LogPosition,
    command_id: Option<[u8; 16]>,
    state: TxnStatusState,
}

/// Apply sink of a transaction-status group: applies [`CoordinatorCommand`]s
/// to [`TxnStatusState`], checkpointed under `<group dir>/raft/state` (the
/// state survives process crash; it also travels inside raft snapshots).
///
/// Apply is deterministic and total. Refused commands are journaled in
/// state, never returned as state-machine errors; genuine faults fail
/// closed: an undecodable payload, an envelope that is not a
/// [`COMMAND_TYPE_DIST_TXN_COORDINATOR`] transaction command, a `Begin` with
/// a non-`Pending` record, or a conflicting decision for a resolved
/// transaction.
pub struct TxnStatusApplySink {
    state: TxnStatusState,
    position: LogPosition,
    command_id: Option<[u8; 16]>,
    /// `<group dir>/raft/state`.
    state_dir: std::path::PathBuf,
}

impl TxnStatusApplySink {
    /// Opens (creating if needed) the sink under `group_dir`, loading the
    /// persisted checkpoint when present. A present but undecodable or
    /// unsupported-version checkpoint fails closed (spec section 4.10).
    pub fn open(group_dir: &Path) -> Result<Self, DistTxnError> {
        let state_dir = group_dir.join("raft").join("state");
        std::fs::create_dir_all(&state_dir).map_err(DistTxnError::Io)?;
        let checkpoint_path = state_dir.join(TXN_STATUS_CHECKPOINT_FILENAME);
        let Some(bytes) =
            crate::node::read_meta_file(&checkpoint_path).map_err(|error| match error {
                crate::node::ClusterError::Io(error) => DistTxnError::Io(error),
                other => DistTxnError::CorruptCheckpoint(other.to_string()),
            })?
        else {
            return Ok(TxnStatusApplySink {
                state: TxnStatusState::default(),
                position: LogPosition::ZERO,
                command_id: None,
                state_dir,
            });
        };
        let checkpoint: TxnStatusCheckpoint = serde_json::from_slice(&bytes)
            .map_err(|error| DistTxnError::CorruptCheckpoint(format!("decode: {error}")))?;
        if !(MIN_SUPPORTED_DIST_TXN_CHECKPOINT_FORMAT_VERSION..=DIST_TXN_CHECKPOINT_FORMAT_VERSION)
            .contains(&checkpoint.format_version)
        {
            return Err(DistTxnError::CorruptCheckpoint(format!(
                "unsupported format version {} (supported \
                 {MIN_SUPPORTED_DIST_TXN_CHECKPOINT_FORMAT_VERSION}..=\
                 {DIST_TXN_CHECKPOINT_FORMAT_VERSION})",
                checkpoint.format_version
            )));
        }
        Ok(TxnStatusApplySink {
            state: checkpoint.state,
            position: checkpoint.position,
            command_id: checkpoint.command_id,
            state_dir,
        })
    }

    /// The current replicated state.
    pub fn state(&self) -> &TxnStatusState {
        &self.state
    }

    /// The log position the state reflects (the crash-window replay
    /// watermark).
    pub fn applied_position(&self) -> LogPosition {
        self.position
    }

    /// One coordinator record.
    pub fn record(&self, txn_id: &TransactionId) -> Option<&TxnRecord> {
        self.state.records.get(txn_id)
    }

    fn checkpoint(&self) -> TxnStatusCheckpoint {
        TxnStatusCheckpoint {
            format_version: DIST_TXN_CHECKPOINT_FORMAT_VERSION,
            position: self.position,
            command_id: self.command_id,
            state: self.state.clone(),
        }
    }

    fn persist(&self) -> Result<(), StateMachineError> {
        let bytes = serde_json::to_vec(&self.checkpoint()).map_err(|error| {
            StateMachineError::Sink(format!("txn status checkpoint encode: {error}"))
        })?;
        crate::node::write_meta_atomic(&self.state_dir, TXN_STATUS_CHECKPOINT_FILENAME, &bytes)
            .map_err(|error| {
                StateMachineError::Sink(format!("txn status checkpoint write: {error}"))
            })
    }

    fn journal(
        &mut self,
        command: &AppliedCommand,
        txn_id: TransactionId,
        reason: StatusRejectionReason,
    ) {
        self.state.rejections.push_back(StatusRejection {
            position: command.position,
            command_id: command.command_id(),
            txn_id,
            reason,
        });
        while self.state.rejections.len() > DIST_TXN_REJECTION_LIMIT {
            self.state.rejections.pop_front();
        }
    }

    fn apply_command(
        &mut self,
        command: &AppliedCommand,
        transition: &CoordinatorCommand,
    ) -> Result<(), StateMachineError> {
        match transition {
            CoordinatorCommand::Begin { record } => {
                if record.state != DistributedTxnState::Pending {
                    return Err(StateMachineError::Corrupt(
                        "Begin record must be Pending".to_owned(),
                    ));
                }
                match self.state.records.get(&record.txn_id) {
                    Some(existing) => {
                        if existing.idempotency_key == record.idempotency_key {
                            // Idempotent replay: the original record stands.
                            return Ok(());
                        }
                        let existing_key = existing.idempotency_key;
                        self.journal(
                            command,
                            record.txn_id,
                            StatusRejectionReason::IdempotencyKeyConflict {
                                existing: existing_key,
                            },
                        );
                    }
                    None => {
                        self.state.records.insert(record.txn_id, record.clone());
                    }
                }
                Ok(())
            }
            CoordinatorCommand::Heartbeat {
                txn_id,
                heartbeat,
                expiry,
            } => match self.state.records.get_mut(txn_id) {
                None => {
                    self.journal(command, *txn_id, StatusRejectionReason::UnknownTxn(*txn_id));
                    Ok(())
                }
                Some(record) if record.state.is_terminal() => {
                    let existing = record.state.clone();
                    self.journal(
                        command,
                        *txn_id,
                        StatusRejectionReason::DecisionFinal { existing },
                    );
                    Ok(())
                }
                Some(record) => {
                    if *heartbeat > record.heartbeat {
                        record.heartbeat = *heartbeat;
                        record.expiry = *expiry;
                    }
                    Ok(())
                }
            },
            CoordinatorCommand::MarkPreparing {
                txn_id,
                tablet_id,
                prepare_ts,
                observed,
            } => match self.state.records.get_mut(txn_id) {
                None => {
                    self.journal(command, *txn_id, StatusRejectionReason::UnknownTxn(*txn_id));
                    Ok(())
                }
                Some(record) if record.state.is_terminal() => {
                    let existing = record.state.clone();
                    self.journal(
                        command,
                        *txn_id,
                        StatusRejectionReason::DecisionFinal { existing },
                    );
                    Ok(())
                }
                Some(record) => {
                    if record.participant(tablet_id).is_none() {
                        self.journal(
                            command,
                            *txn_id,
                            StatusRejectionReason::UnknownParticipant(*tablet_id),
                        );
                        return Ok(());
                    }
                    record.prepare_ts.insert(*tablet_id, *prepare_ts);
                    record.max_observed = record.max_observed.max(*observed);
                    record.state = DistributedTxnState::Preparing;
                    Ok(())
                }
            },
            CoordinatorCommand::Commit { txn_id, commit_ts } => {
                // Pre-check without a long-lived mut borrow so journal can run.
                let pre = self.state.records.get(txn_id).map(|record| {
                    (
                        record.state.is_terminal(),
                        record.state.clone(),
                        record.max_observed,
                        record.prepare_ts.values().any(|ts| *commit_ts <= *ts)
                            || *commit_ts <= record.max_observed,
                    )
                });
                match pre {
                    None => {
                        self.journal(command, *txn_id, StatusRejectionReason::UnknownTxn(*txn_id));
                        Ok(())
                    }
                    Some((true, existing, _, _)) => {
                        self.journal(
                            command,
                            *txn_id,
                            StatusRejectionReason::DecisionFinal { existing },
                        );
                        Ok(())
                    }
                    Some((false, _, max_observed, true)) => {
                        // Review D2: re-validate commit_ts at apply.
                        self.journal(
                            command,
                            *txn_id,
                            StatusRejectionReason::InvalidCommitTs {
                                commit_ts: *commit_ts,
                                max_observed,
                            },
                        );
                        Ok(())
                    }
                    Some((false, _, _, false)) => {
                        if let Some(record) = self.state.records.get_mut(txn_id) {
                            record.state = DistributedTxnState::Committed {
                                commit_ts: *commit_ts,
                            };
                        }
                        Ok(())
                    }
                }
            }
            CoordinatorCommand::Abort { txn_id, reason } => {
                match self.state.records.get_mut(txn_id) {
                    None => {
                        self.journal(command, *txn_id, StatusRejectionReason::UnknownTxn(*txn_id));
                        Ok(())
                    }
                    Some(record) if record.state.is_terminal() => {
                        let existing = record.state.clone();
                        self.journal(
                            command,
                            *txn_id,
                            StatusRejectionReason::DecisionFinal { existing },
                        );
                        Ok(())
                    }
                    Some(record) => {
                        record.state = DistributedTxnState::Aborted {
                            reason: reason.clone(),
                        };
                        Ok(())
                    }
                }
            }
        }
    }
}

impl ApplySink for TxnStatusApplySink {
    fn apply(&mut self, command: &AppliedCommand) -> Result<(), StateMachineError> {
        // Crash-window replay: the sink persisted this entry (or a later
        // one) already; skip it so records never double-apply.
        if command.position.index <= self.position.index {
            return Ok(());
        }
        match &command.command {
            ReplicatedCommand::Transaction(transaction) => {
                transaction.envelope.verify().map_err(|error| {
                    StateMachineError::Corrupt(format!("txn status envelope: {error}"))
                })?;
                if transaction.envelope.command_type != COMMAND_TYPE_DIST_TXN_COORDINATOR {
                    return Err(StateMachineError::Corrupt(format!(
                        "txn status command_type {} is not COMMAND_TYPE_DIST_TXN_COORDINATOR",
                        transaction.envelope.command_type
                    )));
                }
                let record = CoordinatorCommandRecord::decode(&transaction.envelope.payload)
                    .map_err(|error| StateMachineError::Corrupt(error.to_string()))?;
                self.apply_command(command, &record.command)?;
            }
            // Maintenance commands are node-runtime directives and Noop
            // advances the commit index; neither touches the records.
            ReplicatedCommand::Maintenance(_) | ReplicatedCommand::Noop => {}
            // A catalog command here is misrouted — fail closed.
            ReplicatedCommand::Catalog(_) => {
                return Err(StateMachineError::Corrupt(
                    "catalog command on a transaction-status group".to_owned(),
                ));
            }
        }
        self.position = command.position;
        if let Some(command_id) = command.command_id() {
            self.command_id = Some(command_id);
        }
        self.persist()
    }

    fn snapshot(&self) -> Result<Vec<u8>, StateMachineError> {
        serde_json::to_vec(&self.checkpoint()).map_err(|error| {
            StateMachineError::Sink(format!("txn status snapshot encode: {error}"))
        })
    }

    fn install(&mut self, data: &[u8]) -> Result<(), StateMachineError> {
        let checkpoint: TxnStatusCheckpoint = serde_json::from_slice(data).map_err(|error| {
            StateMachineError::Corrupt(format!("txn status snapshot decode: {error}"))
        })?;
        if !(MIN_SUPPORTED_DIST_TXN_CHECKPOINT_FORMAT_VERSION..=DIST_TXN_CHECKPOINT_FORMAT_VERSION)
            .contains(&checkpoint.format_version)
        {
            return Err(StateMachineError::Corrupt(format!(
                "unsupported txn status checkpoint format version {} (supported \
                 {MIN_SUPPORTED_DIST_TXN_CHECKPOINT_FORMAT_VERSION}..=\
                 {DIST_TXN_CHECKPOINT_FORMAT_VERSION})",
                checkpoint.format_version
            )));
        }
        self.state = checkpoint.state;
        self.position = checkpoint.position;
        self.command_id = checkpoint.command_id;
        self.persist()
    }
}

impl fmt::Debug for TxnStatusApplySink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxnStatusApplySink")
            .field("records", &self.state.records.len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Participant intent apply sink
// ---------------------------------------------------------------------------

/// The replicated intent state of one participant tablet group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntentState {
    /// The tablet's current schema version (prepare validates against it).
    pub schema_version: SchemaVersion,
    /// The tablet's current authorization version (prepare validates
    /// against it).
    pub authz_version: u64,
    /// Per-transaction intent records (including resolution tombstones).
    pub txns: BTreeMap<TransactionId, ParticipantTxn>,
    /// Writes made visible by committed resolutions, in apply order.
    pub committed_writes: Vec<CommittedWrite>,
    /// Bounded journal of refused prepares (newest last).
    pub rejections: VecDeque<PrepareRejection>,
}

impl Default for IntentState {
    fn default() -> Self {
        IntentState {
            schema_version: SchemaVersion::ZERO,
            authz_version: 0,
            txns: BTreeMap::new(),
            committed_writes: Vec::new(),
            rejections: VecDeque::new(),
        }
    }
}

/// The intent sink's durable checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IntentCheckpoint {
    format_version: u32,
    position: LogPosition,
    command_id: Option<[u8; 16]>,
    state: IntentState,
}

/// The composite snapshot of an engine-backed participant sink: the engine
/// image plus the intent checkpoint. Written and read only by sinks with a
/// bound engine; unbound sinks keep the plain intent-checkpoint shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompositeSnapshot {
    format_version: u32,
    engine: Vec<u8>,
    intent: Vec<u8>,
}

/// Format version of the [`CompositeSnapshot`] envelope.
pub const COMPOSITE_SNAPSHOT_FORMAT_VERSION: u32 = 1;

/// Apply sink of a participant tablet group: applies [`IntentCommand`]s to
/// [`IntentState`], checkpointed under `<group dir>/raft/state`.
///
/// Same apply contract as [`TxnStatusApplySink`]: deterministic and total,
/// refusals journaled, genuine faults (undecodable payload, wrong envelope
/// type, conflicting decision on an already-resolved transaction) fail
/// closed.
///
/// # Engine binding (Stage 3H MVCC)
///
/// With an [`EngineApplySink`] bound ([`IntentApplySink::open_with_engine`])
/// the sink is the apply half of an engine-backed tablet group:
///
/// - `Transaction` envelopes of type `COMMAND_TYPE_DIST_TXN_INTENT` run the
///   intent protocol below; every other command (engine transactions,
///   catalog commands) is forwarded to the engine sink — one raft stream
///   orders both.
/// - `PersistIntents` validates every staged-write payload through the
///   engine ([`mongreldb_core::database::Database::validate_staged_txn_writes`])
///   before persisting: a payload the resolution apply could not apply is
///   refused at prepare with [`PrepareRejectionReason::MalformedStagedWrite`],
///   journaled, never wedging a committed stream later.
/// - `Resolve` with a committed decision applies the staged writes into the
///   tablet core ([`mongreldb_core::database::Database::apply_staged_txn_writes`])
///   at the decision's `commit_ts` **before** the intent state transition is
///   persisted, then marks the record `applied`. A crash between the engine
///   apply and the checkpoint re-delivers the resolve; the re-apply restamps
///   the identical row ids at a fresh synthetic epoch (visible state is
///   unchanged — the same benign redelivery class the engine watermark
///   absorbs for ordinary replicated commits). A replayed resolve under an
///   already-`applied` record never re-applies.
/// - Snapshots compose both halves ([`CompositeSnapshot`]); install restores
///   the engine first, then the intent checkpoint.
pub struct IntentApplySink {
    state: IntentState,
    position: LogPosition,
    command_id: Option<[u8; 16]>,
    /// `<group dir>/raft/state`.
    state_dir: std::path::PathBuf,
    /// The bound engine apply sink (engine-backed tablet groups only).
    engine: Option<Arc<Mutex<EngineApplySink>>>,
}

/// Whether two write sets carry the same keys and value references (the
/// prepare timestamp is stamped per proposal and excluded from the
/// idempotency comparison).
fn same_write_set(a: &[WriteIntent], b: &[WriteIntent]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(left, right)| left.key == right.key && left.value_ref == right.value_ref)
}

impl IntentApplySink {
    /// Opens (creating if needed) the sink under `group_dir`; see
    /// [`TxnStatusApplySink::open`] for the fail-closed contract. The sink
    /// has no engine binding (protocol-only participant).
    pub fn open(group_dir: &Path) -> Result<Self, DistTxnError> {
        Self::open_with_binding(group_dir, None)
    }

    /// Opens the sink with an [`EngineApplySink`] bound: engine transaction
    /// and catalog commands forward to it, prepares validate staged writes
    /// against the core, and committed resolutions apply into it.
    pub fn open_with_engine(
        group_dir: &Path,
        engine: Arc<Mutex<EngineApplySink>>,
    ) -> Result<Self, DistTxnError> {
        Self::open_with_binding(group_dir, Some(engine))
    }

    fn open_with_binding(
        group_dir: &Path,
        engine: Option<Arc<Mutex<EngineApplySink>>>,
    ) -> Result<Self, DistTxnError> {
        let state_dir = group_dir.join("raft").join("state");
        std::fs::create_dir_all(&state_dir).map_err(DistTxnError::Io)?;
        let checkpoint_path = state_dir.join(INTENT_CHECKPOINT_FILENAME);
        let Some(bytes) =
            crate::node::read_meta_file(&checkpoint_path).map_err(|error| match error {
                crate::node::ClusterError::Io(error) => DistTxnError::Io(error),
                other => DistTxnError::CorruptCheckpoint(other.to_string()),
            })?
        else {
            return Ok(IntentApplySink {
                state: IntentState::default(),
                position: LogPosition::ZERO,
                command_id: None,
                state_dir,
                engine,
            });
        };
        let checkpoint: IntentCheckpoint = serde_json::from_slice(&bytes)
            .map_err(|error| DistTxnError::CorruptCheckpoint(format!("decode: {error}")))?;
        if !(MIN_SUPPORTED_DIST_TXN_CHECKPOINT_FORMAT_VERSION..=DIST_TXN_CHECKPOINT_FORMAT_VERSION)
            .contains(&checkpoint.format_version)
        {
            return Err(DistTxnError::CorruptCheckpoint(format!(
                "unsupported format version {} (supported \
                 {MIN_SUPPORTED_DIST_TXN_CHECKPOINT_FORMAT_VERSION}..=\
                 {DIST_TXN_CHECKPOINT_FORMAT_VERSION})",
                checkpoint.format_version
            )));
        }
        Ok(IntentApplySink {
            state: checkpoint.state,
            position: checkpoint.position,
            command_id: checkpoint.command_id,
            state_dir,
            engine,
        })
    }

    /// The current replicated state.
    pub fn state(&self) -> &IntentState {
        &self.state
    }

    /// The log position the state reflects (the crash-window replay
    /// watermark).
    pub fn applied_position(&self) -> LogPosition {
        self.position
    }

    /// One transaction's intent record.
    pub fn txn(&self, txn_id: &TransactionId) -> Option<&ParticipantTxn> {
        self.state.txns.get(txn_id)
    }

    /// Transaction ids holding unresolved intents (the orphan-sweep input).
    pub fn unresolved_txn_ids(&self) -> Vec<TransactionId> {
        self.state
            .txns
            .values()
            .filter(|txn| txn.resolution.is_none())
            .map(|txn| txn.txn_id)
            .collect()
    }

    /// The bound engine sink, when this is an engine-backed tablet sink.
    pub fn engine(&self) -> Option<&Arc<Mutex<EngineApplySink>>> {
        self.engine.as_ref()
    }

    fn checkpoint(&self) -> IntentCheckpoint {
        IntentCheckpoint {
            format_version: DIST_TXN_CHECKPOINT_FORMAT_VERSION,
            position: self.position,
            command_id: self.command_id,
            state: self.state.clone(),
        }
    }

    fn persist(&self) -> Result<(), StateMachineError> {
        let bytes = serde_json::to_vec(&self.checkpoint()).map_err(|error| {
            StateMachineError::Sink(format!("intent checkpoint encode: {error}"))
        })?;
        crate::node::write_meta_atomic(&self.state_dir, INTENT_CHECKPOINT_FILENAME, &bytes)
            .map_err(|error| StateMachineError::Sink(format!("intent checkpoint write: {error}")))
    }

    fn journal(
        &mut self,
        command: &AppliedCommand,
        txn_id: TransactionId,
        reason: PrepareRejectionReason,
    ) {
        self.state.rejections.push_back(PrepareRejection {
            position: command.position,
            command_id: command.command_id(),
            txn_id,
            reason,
        });
        while self.state.rejections.len() > DIST_TXN_REJECTION_LIMIT {
            self.state.rejections.pop_front();
        }
    }

    /// Applies one committed resolution's staged writes into the bound
    /// engine core at the decision's commit timestamp. The synthetic WAL
    /// transaction tag is a pure function of the transaction id, identical
    /// on every replica.
    fn apply_committed_to_engine(
        engine: &Arc<Mutex<EngineApplySink>>,
        txn_id: &TransactionId,
        staged: &[Vec<u8>],
        commit_ts: HlcTimestamp,
    ) -> Result<(), StateMachineError> {
        if staged.is_empty() {
            return Ok(());
        }
        let db = engine
            .lock()
            .map_err(|_| StateMachineError::Sink("engine sink lock poisoned".to_owned()))?
            .database()
            .ok_or_else(|| {
                StateMachineError::Sink("engine sink has no open database".to_owned())
            })?;
        let txn_tag = fnv1a_64(txn_id.as_bytes());
        db.apply_staged_txn_writes(txn_tag, staged, commit_ts)
            .map_err(|error| {
                StateMachineError::Sink(format!("engine apply of committed resolution: {error}"))
            })?;
        Ok(())
    }

    /// The first unresolved intent of another transaction on `key`, if any
    /// (write/write conflict detection: first-preparer-wins).
    fn conflicting_holder(&self, txn_id: &TransactionId, key: &[u8]) -> Option<TransactionId> {
        self.state
            .txns
            .values()
            .find(|txn| {
                txn.txn_id != *txn_id
                    && txn.resolution.is_none()
                    && txn.intents.iter().any(|intent| intent.key == key)
            })
            .map(|txn| txn.txn_id)
    }

    fn apply_persist(
        &mut self,
        command: &AppliedCommand,
        txn_id: TransactionId,
        expected_schema_version: SchemaVersion,
        expected_authz_version: u64,
        prepare_ts: HlcTimestamp,
        intents: &[WriteIntent],
    ) -> Result<(), StateMachineError> {
        // Idempotent replay paths first: an existing record decides.
        if let Some(existing) = self.state.txns.get(&txn_id) {
            if let Some(decision) = &existing.resolution {
                let decision = decision.clone();
                self.journal(
                    command,
                    txn_id,
                    PrepareRejectionReason::AlreadyResolved { decision },
                );
                return Ok(());
            }
            if same_write_set(&existing.intents, intents) {
                // Replay of the original prepare: the stored (original)
                // prepare timestamp stands.
                return Ok(());
            }
            self.journal(command, txn_id, PrepareRejectionReason::PayloadMismatch);
            return Ok(());
        }
        // Prepare step 1: validate schema and authorization versions.
        if expected_schema_version != self.state.schema_version {
            let found = self.state.schema_version;
            self.journal(
                command,
                txn_id,
                PrepareRejectionReason::StaleSchemaVersion {
                    expected: expected_schema_version,
                    found,
                },
            );
            return Ok(());
        }
        if expected_authz_version != self.state.authz_version {
            let found = self.state.authz_version;
            self.journal(
                command,
                txn_id,
                PrepareRejectionReason::StaleAuthzVersion {
                    expected: expected_authz_version,
                    found,
                },
            );
            return Ok(());
        }
        // Prepare step 2: check conflicts/locks (write/write, unresolved
        // intents of other transactions).
        for intent in intents {
            if let Some(holder) = self.conflicting_holder(&txn_id, &intent.key) {
                self.journal(
                    command,
                    txn_id,
                    PrepareRejectionReason::KeyConflict {
                        key: intent.key.clone(),
                        holder,
                    },
                );
                return Ok(());
            }
        }
        // Engine binding: every staged payload must be appliable at
        // resolution. A malformed payload is refused at prepare — a decision
        // committing over it would otherwise meet an un-appliable payload on
        // every replica.
        if let Some(engine) = &self.engine {
            let staged: Vec<Vec<u8>> = intents
                .iter()
                .map(|intent| intent.value_ref.clone())
                .collect();
            let db = engine
                .lock()
                .map_err(|_| StateMachineError::Sink("engine sink lock poisoned".to_owned()))?
                .database()
                .ok_or_else(|| {
                    StateMachineError::Sink("engine sink has no open database".to_owned())
                })?;
            if let Err(error) = db.validate_staged_txn_writes(&staged) {
                self.journal(
                    command,
                    txn_id,
                    PrepareRejectionReason::MalformedStagedWrite {
                        detail: error.to_string(),
                    },
                );
                return Ok(());
            }
        }
        // Prepare step 3: persist the intents (durable before the response).
        self.state.txns.insert(
            txn_id,
            ParticipantTxn {
                txn_id,
                prepare_ts,
                command_id: command.command_id().unwrap_or([0u8; 16]),
                intents: intents.to_vec(),
                resolution: None,
                applied: false,
                resolved_at: None,
            },
        );
        Ok(())
    }

    fn apply_resolve(
        &mut self,
        command: &AppliedCommand,
        txn_id: TransactionId,
        decision: &TxnDecision,
    ) -> Result<(), StateMachineError> {
        let engine = self.engine.clone();
        let resolved_at = command.commit_ts();
        match self.state.txns.get_mut(&txn_id) {
            None => {
                // Resolution of a transaction this participant never
                // prepared: record a tombstone so a late prepare loses the
                // race against the decision (resolution always wins). Nothing
                // was staged here, so there is nothing to apply.
                self.state.txns.insert(
                    txn_id,
                    ParticipantTxn {
                        txn_id,
                        prepare_ts: HlcTimestamp::ZERO,
                        command_id: [0u8; 16],
                        intents: Vec::new(),
                        resolution: Some(decision.clone()),
                        applied: true,
                        resolved_at,
                    },
                );
                Ok(())
            }
            Some(existing) => match (&existing.resolution, decision) {
                (Some(prior), new) if *prior == *new => {
                    // Idempotent replay of the same decision: never re-apply.
                    // The one exception is a record whose resolution was
                    // never materialized (a checkpoint written before the
                    // engine binding landed) — materialize it now from the
                    // recorded committed writes.
                    if !existing.applied {
                        if let (Some(engine), TxnDecision::Committed { commit_ts }) =
                            (&engine, prior)
                        {
                            let staged: Vec<Vec<u8>> = self
                                .state
                                .committed_writes
                                .iter()
                                .filter(|write| write.txn_id == txn_id)
                                .map(|write| write.value_ref.clone())
                                .collect();
                            Self::apply_committed_to_engine(engine, &txn_id, &staged, *commit_ts)?;
                        }
                        if let Some(existing) = self.state.txns.get_mut(&txn_id) {
                            existing.applied = true;
                        }
                    }
                    Ok(())
                }
                (Some(prior), new) => Err(StateMachineError::Corrupt(format!(
                    "conflicting resolve decisions for transaction {txn_id}: \
                     applied {prior:?}, got {new:?}"
                ))),
                (None, TxnDecision::Committed { commit_ts }) => {
                    let commit_ts = *commit_ts;
                    // Engine first, state transition second: the engine apply
                    // is durable before this sink's checkpoint persists
                    // (a crash in between re-delivers and re-applies the
                    // identical rows benignly; never skips them).
                    if let Some(engine) = &engine {
                        let staged: Vec<Vec<u8>> = existing
                            .intents
                            .iter()
                            .map(|intent| intent.value_ref.clone())
                            .collect();
                        Self::apply_committed_to_engine(engine, &txn_id, &staged, commit_ts)?;
                    }
                    let intents = std::mem::take(&mut existing.intents);
                    let writes = intents.into_iter().map(|intent| CommittedWrite {
                        key: intent.key,
                        value_ref: intent.value_ref,
                        commit_ts,
                        txn_id,
                    });
                    existing.resolution = Some(decision.clone());
                    existing.applied = true;
                    existing.resolved_at = resolved_at;
                    self.state.committed_writes.extend(writes);
                    Ok(())
                }
                (None, TxnDecision::Aborted { .. }) => {
                    // Abort: intents are dropped with zero MVCC effect.
                    existing.intents.clear();
                    existing.resolution = Some(decision.clone());
                    existing.applied = true;
                    existing.resolved_at = resolved_at;
                    Ok(())
                }
            },
        }
    }

    /// Sweeps resolved tombstones older than `older_than`, at most `limit`,
    /// in transaction-id order (deterministic on every replica).
    fn apply_sweep(&mut self, older_than: HlcTimestamp, limit: u32) {
        let mut swept = Vec::new();
        for (txn_id, txn) in &self.state.txns {
            if swept.len() >= limit as usize {
                break;
            }
            if txn.resolution.is_some() && txn.resolved_at.is_some_and(|at| at < older_than) {
                swept.push(*txn_id);
            }
        }
        for txn_id in &swept {
            self.state.txns.remove(txn_id);
        }
        self.state
            .committed_writes
            .retain(|write| !swept.contains(&write.txn_id));
    }
}

impl ApplySink for IntentApplySink {
    fn apply(&mut self, command: &AppliedCommand) -> Result<(), StateMachineError> {
        // Crash-window replay guard (see TxnStatusApplySink).
        if command.position.index <= self.position.index {
            return Ok(());
        }
        match &command.command {
            ReplicatedCommand::Transaction(transaction) => {
                transaction.envelope.verify().map_err(|error| {
                    StateMachineError::Corrupt(format!("intent envelope: {error}"))
                })?;
                if transaction.envelope.command_type == COMMAND_TYPE_DIST_TXN_INTENT {
                    let record = IntentCommandRecord::decode(&transaction.envelope.payload)
                        .map_err(|error| StateMachineError::Corrupt(error.to_string()))?;
                    match record.command {
                        IntentCommand::SetTabletVersions {
                            schema_version,
                            authz_version,
                        } => {
                            self.state.schema_version = schema_version;
                            self.state.authz_version = authz_version;
                        }
                        IntentCommand::PersistIntents {
                            txn_id,
                            expected_schema_version,
                            expected_authz_version,
                            prepare_ts,
                            intents,
                        } => {
                            self.apply_persist(
                                command,
                                txn_id,
                                expected_schema_version,
                                expected_authz_version,
                                prepare_ts,
                                &intents,
                            )?;
                        }
                        IntentCommand::Resolve { txn_id, decision } => {
                            self.apply_resolve(command, txn_id, &decision)?;
                        }
                        IntentCommand::SweepResolved { older_than, limit } => {
                            self.apply_sweep(older_than, limit);
                        }
                    }
                } else if let Some(engine) = &self.engine {
                    // One raft stream orders both: engine transaction
                    // commands forward to the bound engine sink (which
                    // re-validates the envelope type itself).
                    engine
                        .lock()
                        .map_err(|_| StateMachineError::Sink("engine sink lock poisoned".into()))?
                        .apply(command)?;
                } else {
                    return Err(StateMachineError::Corrupt(format!(
                        "intent command_type {} is not COMMAND_TYPE_DIST_TXN_INTENT",
                        transaction.envelope.command_type
                    )));
                }
            }
            ReplicatedCommand::Catalog(_) => {
                if let Some(engine) = &self.engine {
                    engine
                        .lock()
                        .map_err(|_| StateMachineError::Sink("engine sink lock poisoned".into()))?
                        .apply(command)?;
                } else {
                    return Err(StateMachineError::Corrupt(
                        "catalog command on a participant intent group".to_owned(),
                    ));
                }
            }
            // Maintenance commands are node-runtime directives and Noop
            // advances the commit index; the engine half tracks them for its
            // snapshot watermark, the intent state ignores them.
            ReplicatedCommand::Maintenance(_) | ReplicatedCommand::Noop => {
                if let Some(engine) = &self.engine {
                    engine
                        .lock()
                        .map_err(|_| StateMachineError::Sink("engine sink lock poisoned".into()))?
                        .apply(command)?;
                }
            }
        }
        self.position = command.position;
        if let Some(command_id) = command.command_id() {
            self.command_id = Some(command_id);
        }
        self.persist()
    }

    fn snapshot(&self) -> Result<Vec<u8>, StateMachineError> {
        let intent = serde_json::to_vec(&self.checkpoint())
            .map_err(|error| StateMachineError::Sink(format!("intent snapshot encode: {error}")))?;
        match &self.engine {
            None => Ok(intent),
            Some(engine) => {
                let engine_image = engine
                    .lock()
                    .map_err(|_| StateMachineError::Sink("engine sink lock poisoned".into()))?
                    .snapshot()?;
                serde_json::to_vec(&CompositeSnapshot {
                    format_version: COMPOSITE_SNAPSHOT_FORMAT_VERSION,
                    engine: engine_image,
                    intent,
                })
                .map_err(|error| {
                    StateMachineError::Sink(format!("composite snapshot encode: {error}"))
                })
            }
        }
    }

    fn install(&mut self, data: &[u8]) -> Result<(), StateMachineError> {
        // An engine-backed sink restores the engine image first (the
        // fallible half: it refuses over live state), then the intent
        // checkpoint. An unbound sink keeps the plain checkpoint shape.
        let intent_bytes;
        if let Some(engine) = &self.engine {
            let composite: CompositeSnapshot = serde_json::from_slice(data).map_err(|error| {
                StateMachineError::Corrupt(format!("composite snapshot decode: {error}"))
            })?;
            if composite.format_version != COMPOSITE_SNAPSHOT_FORMAT_VERSION {
                return Err(StateMachineError::Corrupt(format!(
                    "unsupported composite snapshot format version {} (supported \
                     {COMPOSITE_SNAPSHOT_FORMAT_VERSION})",
                    composite.format_version
                )));
            }
            engine
                .lock()
                .map_err(|_| StateMachineError::Sink("engine sink lock poisoned".into()))?
                .install(&composite.engine)?;
            intent_bytes = composite.intent;
        } else {
            intent_bytes = data.to_vec();
        }
        let checkpoint: IntentCheckpoint =
            serde_json::from_slice(&intent_bytes).map_err(|error| {
                StateMachineError::Corrupt(format!("intent snapshot decode: {error}"))
            })?;
        if !(MIN_SUPPORTED_DIST_TXN_CHECKPOINT_FORMAT_VERSION..=DIST_TXN_CHECKPOINT_FORMAT_VERSION)
            .contains(&checkpoint.format_version)
        {
            return Err(StateMachineError::Corrupt(format!(
                "unsupported intent checkpoint format version {} (supported \
                 {MIN_SUPPORTED_DIST_TXN_CHECKPOINT_FORMAT_VERSION}..=\
                 {DIST_TXN_CHECKPOINT_FORMAT_VERSION})",
                checkpoint.format_version
            )));
        }
        self.state = checkpoint.state;
        self.position = checkpoint.position;
        self.command_id = checkpoint.command_id;
        self.persist()
    }
}

impl fmt::Debug for IntentApplySink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IntentApplySink")
            .field("txns", &self.state.txns.len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Group wrappers
// ---------------------------------------------------------------------------

/// Builds one openraft node value for the membership calls without naming
/// the openraft type (same serde-shape bridge as the meta group; the
/// cluster crate deliberately has no openraft dependency, ADR-0004).
fn basic_node<N>(address: &str) -> Result<N, DistTxnError>
where
    N: for<'de> Deserialize<'de>,
{
    serde_json::from_value(serde_json::json!({ "addr": address })).map_err(|error| {
        DistTxnError::InvalidRequest(format!("member address `{address}`: {error}"))
    })
}

/// Shared member view for the leader-retry loop.
pub trait GroupMember {
    /// This member's raft node id.
    fn node_id(&self) -> RaftNodeId;
    /// The group's current leader as this member sees it, if any.
    fn current_leader(&self) -> Option<RaftNodeId>;
}

/// One member of a transaction-status (coordinator) group: a
/// [`ConsensusGroup`] whose apply sink is a [`TxnStatusApplySink`], plus the
/// propose/read workflow the protocol driver uses. Mirrors
/// [`crate::meta::MetaGroup`].
pub struct TxnStatusGroup<T: RaftTransport> {
    group: ConsensusGroup<T>,
    sink: Arc<Mutex<TxnStatusApplySink>>,
    raft_group_id: RaftGroupId,
}

impl<T: RaftTransport> TxnStatusGroup<T> {
    /// Opens the group's durable state and starts the raft task with a
    /// [`TxnStatusApplySink`] installed. The group's text id is forced to
    /// the raft group id's canonical hex form (session tokens carry it).
    pub async fn create(
        mut group_config: GroupConfig,
        raft_group_id: RaftGroupId,
        transport: Arc<T>,
    ) -> Result<Self, DistTxnError> {
        group_config.cluster_name = raft_group_id.to_hex();
        let sink = Arc::new(Mutex::new(TxnStatusApplySink::open(&group_config.dir)?));
        let group = ConsensusGroup::create(
            group_config,
            transport,
            sink.clone() as Arc<Mutex<dyn ApplySink>>,
        )
        .await?;
        Ok(TxnStatusGroup {
            group,
            sink,
            raft_group_id,
        })
    }

    /// The underlying consensus group (snapshots, membership, barriers,
    /// metrics, shutdown).
    pub fn group(&self) -> &ConsensusGroup<T> {
        &self.group
    }

    /// The group's durable id.
    pub fn raft_group_id(&self) -> RaftGroupId {
        self.raft_group_id
    }

    /// Bootstraps a pristine group with the given `(raft_id, rpc_address)`
    /// voter set (call on one pristine member; check
    /// [`ConsensusGroup::is_initialized`] on reopen).
    pub async fn bootstrap(&self, members: &[(RaftNodeId, String)]) -> Result<(), DistTxnError> {
        let mut map = BTreeMap::new();
        for (raft_id, address) in members {
            map.insert(*raft_id, basic_node(address)?);
        }
        self.group
            .bootstrap(map)
            .await
            .map_err(DistTxnError::Consensus)
    }

    /// Proposes one coordinator command (quorum durability) and waits for
    /// commit + apply. `command_id` is the idempotency token: a retry with
    /// the same id replays the original apply (S2B-004). A refused command
    /// returns its journaled [`StatusRejectionReason`]; the raft entry
    /// itself committed normally.
    pub async fn propose(
        &self,
        command_id: [u8; 16],
        command: CoordinatorCommand,
        control: &ExecutionControl,
    ) -> Result<(GroupCommitReceipt, Option<StatusRejectionReason>), DistTxnError> {
        let payload = CoordinatorCommandRecord::new(command).encode()?;
        let envelope = CommandEnvelope::new(COMMAND_TYPE_DIST_TXN_COORDINATOR, command_id, payload);
        let receipt = self
            .group
            .propose(CommandKind::Transaction, envelope, control)
            .await?;
        // client_write returns after local apply, so the local sink's view
        // already includes this command (or its refusal).
        let rejection = {
            let sink = self.sink.lock().map_err(|_| {
                DistTxnError::InvalidRequest("txn status sink lock poisoned".to_owned())
            })?;
            sink.state()
                .rejections
                .iter()
                .rev()
                .find(|entry| entry.command_id == Some(command_id))
                .map(|entry| entry.reason.clone())
        };
        Ok((receipt, rejection))
    }

    /// One coordinator record at this node's applied watermark.
    pub fn record(&self, txn_id: &TransactionId) -> Option<TxnRecord> {
        self.sink
            .lock()
            .expect("txn status sink lock poisoned")
            .record(txn_id)
            .cloned()
    }

    /// One coordinator record behind a read barrier (spec section 11.4):
    /// [`ReadConsistency::Linearizable`] confirms leadership with a quorum
    /// before reading; [`ReadConsistency::ReadYourWrites`] waits until this
    /// replica applied the session token's write. Any node may query the
    /// coordinator record through this API (spec section 12.8 "Recovery").
    pub async fn record_consistent(
        &self,
        txn_id: &TransactionId,
        consistency: &ReadConsistency,
        control: &ExecutionControl,
    ) -> Result<Option<TxnRecord>, DistTxnError> {
        self.group
            .consistent_read(consistency, control)
            .await
            .map_err(DistTxnError::Read)?;
        Ok(self.record(txn_id))
    }

    /// The full replicated state at this node's applied watermark.
    pub fn state(&self) -> TxnStatusState {
        self.sink
            .lock()
            .expect("txn status sink lock poisoned")
            .state()
            .clone()
    }

    /// A session token proving `receipt`'s write, for later
    /// read-your-writes record queries on any replica.
    pub fn session_token(&self, receipt: &GroupCommitReceipt) -> SessionToken {
        SessionToken {
            group_id: self.group.group_id().to_owned(),
            commit_index: receipt.position.index,
            commit_ts: receipt.commit_ts,
        }
    }

    /// Graceful shutdown of the underlying group.
    pub async fn shutdown(&self) -> Result<(), DistTxnError> {
        self.group.shutdown().await.map_err(DistTxnError::Consensus)
    }

    /// Process-free crash simulation (durability tests): stops the raft
    /// task without the graceful storage close; everything fsynced
    /// survives.
    pub async fn crash(self) {
        self.group.crash().await;
    }
}

impl<T: RaftTransport> GroupMember for TxnStatusGroup<T> {
    fn node_id(&self) -> RaftNodeId {
        self.group.node_id()
    }

    fn current_leader(&self) -> Option<RaftNodeId> {
        self.group.metrics().current_leader
    }
}

/// One member of a participant tablet's intent group: a [`ConsensusGroup`]
/// whose apply sink is an [`IntentApplySink`], plus the prepare/resolve
/// workflow the protocol driver uses.
pub struct IntentGroup<T: RaftTransport> {
    group: ConsensusGroup<T>,
    sink: Arc<Mutex<IntentApplySink>>,
    raft_group_id: RaftGroupId,
}

impl<T: RaftTransport> IntentGroup<T> {
    /// Opens the group's durable state and starts the raft task with an
    /// [`IntentApplySink`] installed; see [`TxnStatusGroup::create`].
    pub async fn create(
        mut group_config: GroupConfig,
        raft_group_id: RaftGroupId,
        transport: Arc<T>,
    ) -> Result<Self, DistTxnError> {
        group_config.cluster_name = raft_group_id.to_hex();
        let sink = Arc::new(Mutex::new(IntentApplySink::open(&group_config.dir)?));
        let group = ConsensusGroup::create(
            group_config,
            transport,
            sink.clone() as Arc<Mutex<dyn ApplySink>>,
        )
        .await?;
        Ok(IntentGroup {
            group,
            sink,
            raft_group_id,
        })
    }

    /// The underlying consensus group.
    pub fn group(&self) -> &ConsensusGroup<T> {
        &self.group
    }

    /// The group's durable id.
    pub fn raft_group_id(&self) -> RaftGroupId {
        self.raft_group_id
    }

    /// Bootstraps a pristine group; see [`TxnStatusGroup::bootstrap`].
    pub async fn bootstrap(&self, members: &[(RaftNodeId, String)]) -> Result<(), DistTxnError> {
        let mut map = BTreeMap::new();
        for (raft_id, address) in members {
            map.insert(*raft_id, basic_node(address)?);
        }
        self.group
            .bootstrap(map)
            .await
            .map_err(DistTxnError::Consensus)
    }

    /// Proposes one intent command (quorum durability) and waits for
    /// commit + apply; see [`TxnStatusGroup::propose`]. A refused prepare
    /// returns its journaled [`PrepareRejectionReason`].
    pub async fn propose(
        &self,
        command_id: [u8; 16],
        command: IntentCommand,
        control: &ExecutionControl,
    ) -> Result<(GroupCommitReceipt, Option<PrepareRejectionReason>), DistTxnError> {
        let payload = IntentCommandRecord::new(command).encode()?;
        let envelope = CommandEnvelope::new(COMMAND_TYPE_DIST_TXN_INTENT, command_id, payload);
        let receipt = self
            .group
            .propose(CommandKind::Transaction, envelope, control)
            .await?;
        let rejection = {
            let sink = self.sink.lock().map_err(|_| {
                DistTxnError::InvalidRequest("intent sink lock poisoned".to_owned())
            })?;
            sink.state()
                .rejections
                .iter()
                .rev()
                .find(|entry| entry.command_id == Some(command_id))
                .map(|entry| entry.reason.clone())
        };
        Ok((receipt, rejection))
    }

    /// One transaction's intent record at this node's applied watermark.
    pub fn txn(&self, txn_id: &TransactionId) -> Option<ParticipantTxn> {
        self.sink
            .lock()
            .expect("intent sink lock poisoned")
            .txn(txn_id)
            .cloned()
    }

    /// Transaction ids holding unresolved intents at this node's applied
    /// watermark (the orphan-sweep input).
    pub fn unresolved_txn_ids(&self) -> Vec<TransactionId> {
        self.sink
            .lock()
            .expect("intent sink lock poisoned")
            .unresolved_txn_ids()
    }

    /// The writes made visible by committed resolutions at this node's
    /// applied watermark.
    pub fn committed_writes(&self) -> Vec<CommittedWrite> {
        self.sink
            .lock()
            .expect("intent sink lock poisoned")
            .state()
            .committed_writes
            .clone()
    }

    /// The full replicated state at this node's applied watermark.
    pub fn state(&self) -> IntentState {
        self.sink
            .lock()
            .expect("intent sink lock poisoned")
            .state()
            .clone()
    }

    /// Graceful shutdown of the underlying group.
    pub async fn shutdown(&self) -> Result<(), DistTxnError> {
        self.group.shutdown().await.map_err(DistTxnError::Consensus)
    }

    /// Process-free crash simulation; see [`TxnStatusGroup::crash`].
    pub async fn crash(self) {
        self.group.crash().await;
    }
}

impl<T: RaftTransport> GroupMember for IntentGroup<T> {
    fn node_id(&self) -> RaftNodeId {
        self.group.node_id()
    }

    fn current_leader(&self) -> Option<RaftNodeId> {
        self.group.metrics().current_leader
    }
}

/// The participant-group surface the protocol driver drives (spec section
/// 12.8): propose intent commands, read intent records at the applied
/// watermark. Implemented by the protocol-only [`IntentGroup`] and by
/// [`TabletTxnGroup`], whose engine binding materializes committed
/// resolutions into the tablet core.
pub trait IntentGroupMember<T: RaftTransport>: GroupMember {
    /// The group's durable id.
    fn raft_group_id(&self) -> RaftGroupId;
    /// The underlying consensus group (applied watermark, metrics,
    /// membership, shutdown).
    fn group(&self) -> &ConsensusGroup<T>;
    /// Proposes one intent command (quorum durability) and waits for
    /// commit + apply; see [`IntentGroup::propose`]. A refused prepare
    /// returns its journaled [`PrepareRejectionReason`].
    fn propose(
        &self,
        command_id: [u8; 16],
        command: IntentCommand,
        control: &ExecutionControl,
    ) -> impl Future<
        Output = Result<(GroupCommitReceipt, Option<PrepareRejectionReason>), DistTxnError>,
    > + Send;
    /// One transaction's intent record at this member's applied watermark.
    fn txn(&self, txn_id: &TransactionId) -> Option<ParticipantTxn>;
    /// Transaction ids holding unresolved intents at this member's applied
    /// watermark.
    fn unresolved_txn_ids(&self) -> Vec<TransactionId>;
    /// The full replicated intent state at this member's applied watermark.
    fn state(&self) -> IntentState;
}

impl<T: RaftTransport> IntentGroupMember<T> for IntentGroup<T> {
    fn raft_group_id(&self) -> RaftGroupId {
        self.raft_group_id
    }

    fn group(&self) -> &ConsensusGroup<T> {
        &self.group
    }

    fn propose(
        &self,
        command_id: [u8; 16],
        command: IntentCommand,
        control: &ExecutionControl,
    ) -> impl Future<
        Output = Result<(GroupCommitReceipt, Option<PrepareRejectionReason>), DistTxnError>,
    > + Send {
        self.propose(command_id, command, control)
    }

    fn txn(&self, txn_id: &TransactionId) -> Option<ParticipantTxn> {
        self.txn(txn_id)
    }

    fn unresolved_txn_ids(&self) -> Vec<TransactionId> {
        self.unresolved_txn_ids()
    }

    fn state(&self) -> IntentState {
        self.state()
    }
}

/// One member of an engine-backed tablet group: a [`ConsensusGroup`] whose
/// apply sink is an [`IntentApplySink`] bound to an [`EngineApplySink`]
/// (Stage 3H MVCC binding). One raft stream orders the two-phase-commit
/// intent protocol and the tablet's engine transaction/catalog commands; a
/// committed resolution applies its staged writes into the tablet core
/// through the same replicated apply path the engine sink uses.
pub struct TabletTxnGroup<T: RaftTransport> {
    group: ConsensusGroup<T>,
    sink: Arc<Mutex<IntentApplySink>>,
    engine: Arc<Mutex<EngineApplySink>>,
    raft_group_id: RaftGroupId,
}

impl<T: RaftTransport> TabletTxnGroup<T> {
    /// Starts the raft task over an [`IntentApplySink`] bound to `engine`
    /// (opened by the caller, e.g. through
    /// [`mongreldb_consensus::engine_sink::open_engine_sink`], so the node
    /// runtime owns the engine's directory layout). The engine must be the
    /// apply sink of THIS group's engine commands: the sink forwards them.
    pub async fn create(
        mut group_config: GroupConfig,
        raft_group_id: RaftGroupId,
        transport: Arc<T>,
        engine: Arc<Mutex<EngineApplySink>>,
    ) -> Result<Self, DistTxnError> {
        group_config.cluster_name = raft_group_id.to_hex();
        let sink = Arc::new(Mutex::new(IntentApplySink::open_with_engine(
            &group_config.dir,
            engine.clone(),
        )?));
        let group = ConsensusGroup::create(
            group_config,
            transport,
            sink.clone() as Arc<Mutex<dyn ApplySink>>,
        )
        .await?;
        Ok(TabletTxnGroup {
            group,
            sink,
            engine,
            raft_group_id,
        })
    }

    /// The underlying consensus group.
    pub fn group(&self) -> &ConsensusGroup<T> {
        &self.group
    }

    /// The group's durable id.
    pub fn raft_group_id(&self) -> RaftGroupId {
        self.raft_group_id
    }

    /// The bound engine sink (read-path inspection, tests).
    pub fn engine(&self) -> &Arc<Mutex<EngineApplySink>> {
        &self.engine
    }

    /// Bootstraps a pristine group; see [`TxnStatusGroup::bootstrap`].
    pub async fn bootstrap(&self, members: &[(RaftNodeId, String)]) -> Result<(), DistTxnError> {
        let mut map = BTreeMap::new();
        for (raft_id, address) in members {
            map.insert(*raft_id, basic_node(address)?);
        }
        self.group
            .bootstrap(map)
            .await
            .map_err(DistTxnError::Consensus)
    }

    /// Proposes one intent command (quorum durability) and waits for
    /// commit + apply; see [`IntentGroup::propose`].
    pub async fn propose(
        &self,
        command_id: [u8; 16],
        command: IntentCommand,
        control: &ExecutionControl,
    ) -> Result<(GroupCommitReceipt, Option<PrepareRejectionReason>), DistTxnError> {
        let payload = IntentCommandRecord::new(command).encode()?;
        let envelope = CommandEnvelope::new(COMMAND_TYPE_DIST_TXN_INTENT, command_id, payload);
        let receipt = self
            .group
            .propose(CommandKind::Transaction, envelope, control)
            .await?;
        let rejection = {
            let sink = self.sink.lock().map_err(|_| {
                DistTxnError::InvalidRequest("intent sink lock poisoned".to_owned())
            })?;
            sink.state()
                .rejections
                .iter()
                .rev()
                .find(|entry| entry.command_id == Some(command_id))
                .map(|entry| entry.reason.clone())
        };
        Ok((receipt, rejection))
    }

    /// One transaction's intent record at this node's applied watermark.
    pub fn txn(&self, txn_id: &TransactionId) -> Option<ParticipantTxn> {
        self.sink
            .lock()
            .expect("intent sink lock poisoned")
            .txn(txn_id)
            .cloned()
    }

    /// Transaction ids holding unresolved intents at this node's applied
    /// watermark (the orphan-sweep input).
    pub fn unresolved_txn_ids(&self) -> Vec<TransactionId> {
        self.sink
            .lock()
            .expect("intent sink lock poisoned")
            .unresolved_txn_ids()
    }

    /// The writes made visible by committed resolutions at this node's
    /// applied watermark.
    pub fn committed_writes(&self) -> Vec<CommittedWrite> {
        self.sink
            .lock()
            .expect("intent sink lock poisoned")
            .state()
            .committed_writes
            .clone()
    }

    /// The full replicated intent state at this node's applied watermark.
    pub fn state(&self) -> IntentState {
        self.sink
            .lock()
            .expect("intent sink lock poisoned")
            .state()
            .clone()
    }

    /// Graceful shutdown of the underlying group.
    pub async fn shutdown(&self) -> Result<(), DistTxnError> {
        self.group.shutdown().await.map_err(DistTxnError::Consensus)
    }

    /// Process-free crash simulation; see [`TxnStatusGroup::crash`].
    pub async fn crash(self) {
        self.group.crash().await;
    }
}

impl<T: RaftTransport> GroupMember for TabletTxnGroup<T> {
    fn node_id(&self) -> RaftNodeId {
        self.group.node_id()
    }

    fn current_leader(&self) -> Option<RaftNodeId> {
        self.group.metrics().current_leader
    }
}

impl<T: RaftTransport> IntentGroupMember<T> for TabletTxnGroup<T> {
    fn raft_group_id(&self) -> RaftGroupId {
        self.raft_group_id
    }

    fn group(&self) -> &ConsensusGroup<T> {
        &self.group
    }

    fn propose(
        &self,
        command_id: [u8; 16],
        command: IntentCommand,
        control: &ExecutionControl,
    ) -> impl Future<
        Output = Result<(GroupCommitReceipt, Option<PrepareRejectionReason>), DistTxnError>,
    > + Send {
        self.propose(command_id, command, control)
    }

    fn txn(&self, txn_id: &TransactionId) -> Option<ParticipantTxn> {
        self.txn(txn_id)
    }

    fn unresolved_txn_ids(&self) -> Vec<TransactionId> {
        self.unresolved_txn_ids()
    }

    fn state(&self) -> IntentState {
        self.state()
    }
}

// ---------------------------------------------------------------------------
// Protocol driver (coordinator logic; spec section 12.8 flows)
// ---------------------------------------------------------------------------

/// Static configuration of one protocol driver instance.
#[derive(Debug, Clone)]
pub struct DistTxnConfig {
    /// Coordinator selection mode (default
    /// [`CoordinatorSelection::TxnIdDerived`]).
    pub selection: CoordinatorSelection,
    /// The published transaction-status partitions (read from meta state;
    /// required by [`CoordinatorSelection::TxnIdDerived`]).
    pub status_partitions: BTreeMap<u32, TxnStatusPartition>,
    /// Heartbeat-expiry window: a non-terminal record whose heartbeat is
    /// older than this may be pushed to `Aborted` by any node.
    pub pending_timeout: Duration,
    /// How long resolved intent tombstones are retained before
    /// [`DistTxnDriver::sweep_resolved`] may remove them
    /// ([`DEFAULT_RESOLVED_RETENTION`]; see its documentation for the
    /// retention-window rule).
    pub resolved_retention: Duration,
    /// Bound on tombstones one sweep command removes.
    pub sweep_limit: u32,
    /// HLC skew bound of the driver's clock.
    pub hlc_max_skew: Duration,
    /// HLC node tiebreaker of the driver's clock.
    pub node_tiebreaker: u32,
    /// Pacing between leader-retry rounds.
    pub propose_retry_interval: Duration,
    /// Bound on leader-retry rounds before giving up (fail-safe when no
    /// deadline is configured).
    pub max_propose_rounds: usize,
}

impl Default for DistTxnConfig {
    fn default() -> Self {
        DistTxnConfig {
            selection: CoordinatorSelection::TxnIdDerived,
            status_partitions: BTreeMap::new(),
            pending_timeout: DEFAULT_PENDING_TIMEOUT,
            resolved_retention: DEFAULT_RESOLVED_RETENTION,
            sweep_limit: DEFAULT_SWEEP_LIMIT,
            hlc_max_skew: Duration::from_millis(500),
            node_tiebreaker: 0,
            propose_retry_interval: Duration::from_millis(50),
            max_propose_rounds: 600,
        }
    }
}

/// One participant's write set in a commit request.
#[derive(Debug, Clone)]
pub struct ParticipantWrites {
    /// The participant (tablet + raft group).
    pub participant: TxnParticipant,
    /// Schema version the transaction planned against (validated at
    /// prepare).
    pub expected_schema_version: SchemaVersion,
    /// Authorization version the transaction authenticated against
    /// (validated at prepare).
    pub expected_authz_version: u64,
    /// The write intents (`prepare_ts` is stamped by the driver at
    /// proposal; any placeholder value is overwritten).
    pub intents: Vec<WriteIntent>,
}

/// A distributed commit request.
#[derive(Debug, Clone)]
pub struct CommitRequest {
    /// The transaction id (minted by the client; never reused).
    pub txn_id: TransactionId,
    /// The client's idempotency key: retries must carry the same key.
    pub idempotency_key: [u8; 16],
    /// Per-participant write sets (each tablet at most once).
    pub writes: Vec<ParticipantWrites>,
    /// Read/write timestamps the transaction observed; `commit_ts` is
    /// chosen strictly greater than all of them (spec section 8.2).
    pub observed: Vec<HlcTimestamp>,
    /// The first write's tablet descriptor (required by
    /// [`CoordinatorSelection::FirstWriteHome`], ignored otherwise).
    pub first_write_tablet: Option<TabletDescriptor>,
}

/// The outcome of [`DistTxnDriver::recover`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryOutcome {
    /// No coordinator record exists for the transaction.
    NotFound,
    /// The transaction is non-terminal and not expired, and at least one
    /// participant's intents are not (yet) visible: left to its
    /// coordinator (recovery never aborts on suspicion).
    InFlight {
        /// How many participants showed no durable intents.
        missing_participants: usize,
    },
    /// The transaction reached (or already held) a durable decision;
    /// resolution was broadcast to every participant.
    Decided(DistributedTxnState),
}

/// The outcome of [`DistTxnDriver::push_expired`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushOutcome {
    /// No coordinator record exists.
    NotFound,
    /// The record is non-terminal but its heartbeat expiry has not passed:
    /// left alone (the push rule's gate).
    NotExpired,
    /// The record was already terminal; resolution was re-broadcast.
    Terminal(DistributedTxnState),
    /// The record was expired and non-terminal: pushed to `Aborted` (or a
    /// racing decision landed first — the final state is returned either
    /// way) and resolution was broadcast.
    Pushed(DistributedTxnState),
}

/// The outcome of resolving one orphan intent set
/// ([`DistTxnDriver::resolve_orphan`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrphanOutcome {
    /// The coordinator record showed a durable commit; the intents were
    /// resolved visible at `commit_ts`.
    ResolvedCommit {
        /// The durable commit timestamp.
        commit_ts: HlcTimestamp,
    },
    /// The coordinator record already showed a durable abort; the intents
    /// were removed.
    ResolvedAbort {
        /// The abort reason.
        reason: AbortReason,
    },
    /// The record was non-terminal but heartbeat-expired: the sweep pushed
    /// it to `Aborted` through the replicated record (the documented
    /// timeout rule), then removed the intents.
    PushedAbort {
        /// The abort reason the push persisted.
        reason: AbortReason,
    },
    /// The transaction is non-terminal and unexpired: left untouched (the
    /// timeout rules are honored, never suspicion).
    InFlight,
    /// No coordinator record exists: left untouched (recovery never
    /// resolves without the record).
    UnknownRecord,
}

/// Tallies of one orphan sweep ([`DistTxnDriver::sweep_orphans`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Intents resolved against a durable commit.
    pub resolved_committed: usize,
    /// Intents resolved against a durable (or pushed) abort.
    pub resolved_aborted: usize,
    /// Expired non-terminal transactions pushed to abort.
    pub pushed: usize,
    /// Live transactions left untouched (timeout honored).
    pub in_flight: usize,
    /// Intent sets with no coordinator record (left untouched).
    pub unknown: usize,
}

/// Tallies of one resolved-tombstone sweep
/// ([`DistTxnDriver::sweep_resolved`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepResolvedReport {
    /// Tombstones removed by this sweep.
    pub swept: usize,
    /// Resolved tombstones older than the retention cutoff that remain
    /// (the sweep is bounded; call again to continue).
    pub remaining: usize,
}

/// Broadcast tally of one decision fan-out.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolveBroadcast {
    /// Groups where the resolve committed during the broadcast.
    pub resolved: Vec<RaftGroupId>,
    /// Groups where the resolve did not land (left to lazy recovery).
    pub deferred: Vec<RaftGroupId>,
}

/// Advances `base` by `by` on the HLC physical axis (expiry arithmetic).
fn advance(base: HlcTimestamp, by: Duration) -> HlcTimestamp {
    HlcTimestamp {
        physical_micros: base
            .physical_micros
            .saturating_add(u64::try_from(by.as_micros()).unwrap_or(u64::MAX)),
        logical: base.logical,
        node_tiebreaker: base.node_tiebreaker,
    }
}

/// Whether a propose/read failure is a leadership transient worth retrying
/// on another member (never an answer to the client by itself).
fn retryable(error: &DistTxnError) -> bool {
    match error {
        DistTxnError::Consensus(
            ConsensusError::NotLeader { .. } | ConsensusError::Closed | ConsensusError::Raft(_),
        )
        | DistTxnError::Read(
            ReadConsistencyError::NotLeader { .. }
            | ReadConsistencyError::LeaderUnknown
            | ReadConsistencyError::Closed,
        ) => true,
        DistTxnError::Consensus(ConsensusError::Cancelled | ConsensusError::DeadlineExceeded) => {
            false
        }
        _ => false,
    }
}

/// Maps a cooperative-control failure to the driver's error surface.
fn control_error(control: &ExecutionControl) -> Option<DistTxnError> {
    control.check().err().map(|error| match error {
        mongreldb_log::commit_log::LogError::Cancelled => {
            DistTxnError::Consensus(ConsensusError::Cancelled)
        }
        mongreldb_log::commit_log::LogError::DeadlineExceeded => {
            DistTxnError::Consensus(ConsensusError::DeadlineExceeded)
        }
        other => DistTxnError::Consensus(ConsensusError::Raft(other.to_string())),
    })
}

/// Runs `call` against the members, leader-claimants first, retrying
/// leadership transients across members until the retry budget or the
/// caller's control fires. Returns the answering member and its answer.
async fn retry_across_members<'m, G, F, Fut, R>(
    members: &'m [G],
    group_label: &str,
    config: &DistTxnConfig,
    control: &ExecutionControl,
    mut call: F,
) -> Result<(&'m G, R), DistTxnError>
where
    G: GroupMember,
    F: FnMut(&'m G) -> Fut,
    Fut: Future<Output = Result<R, DistTxnError>>,
{
    if members.is_empty() {
        return Err(DistTxnError::InvalidRequest(format!(
            "no members supplied for {group_label}"
        )));
    }
    let mut rounds = 0_usize;
    loop {
        let mut ordered: Vec<&G> = members
            .iter()
            .filter(|member| member.current_leader() == Some(member.node_id()))
            .collect();
        ordered.extend(
            members
                .iter()
                .filter(|member| member.current_leader() != Some(member.node_id())),
        );
        let mut last_error: Option<DistTxnError> = None;
        for member in ordered {
            match call(member).await {
                Ok(answer) => return Ok((member, answer)),
                Err(error) if retryable(&error) => last_error = Some(error),
                Err(error) => return Err(error),
            }
        }
        rounds += 1;
        if let Some(error) = control_error(control) {
            return Err(error);
        }
        if rounds >= config.max_propose_rounds {
            return Err(DistTxnError::Unavailable(format!(
                "{group_label}: no live leader after {rounds} rounds ({})",
                last_error
                    .map(|error| error.to_string())
                    .unwrap_or_else(|| "no members answered".to_owned())
            )));
        }
        tokio::time::sleep(config.propose_retry_interval).await;
    }
}

/// The stateless distributed-transaction protocol driver (spec section
/// 12.8). All durable state lives in the groups; the driver composes the
/// flows and owns the coordinator-side HLC clock. Any driver instance can
/// drive or recover any transaction from the replicated records.
pub struct DistTxnDriver {
    config: DistTxnConfig,
    hlc: HlcClock,
}

impl DistTxnDriver {
    /// Creates a driver with its own coordinator-side HLC clock.
    pub fn new(config: DistTxnConfig) -> Self {
        let hlc = HlcClock::new(config.node_tiebreaker, config.hlc_max_skew);
        DistTxnDriver { config, hlc }
    }

    /// The driver's configuration.
    pub fn config(&self) -> &DistTxnConfig {
        &self.config
    }

    /// The driver's current HLC reading.
    pub fn now(&self) -> Result<HlcTimestamp, DistTxnError> {
        self.hlc
            .now()
            .map_err(|error| DistTxnError::Clock(error.to_string()))
    }

    fn expiry_from(&self, base: HlcTimestamp) -> HlcTimestamp {
        advance(base, self.config.pending_timeout)
    }

    /// Validates the commit request and chooses the coordinator group
    /// (deterministic selection, spec section 12.8).
    fn coordinator_for(
        &self,
        request: &CommitRequest,
    ) -> Result<(RaftGroupId, Vec<TxnParticipant>), DistTxnError> {
        if request.writes.is_empty() {
            return Err(DistTxnError::InvalidRequest(
                "a distributed commit needs at least one participant write".to_owned(),
            ));
        }
        let mut participants = Vec::with_capacity(request.writes.len());
        for writes in &request.writes {
            if participants.contains(&writes.participant) {
                return Err(DistTxnError::InvalidRequest(format!(
                    "tablet {} appears twice in the write set",
                    writes.participant.tablet_id
                )));
            }
            participants.push(writes.participant);
        }
        let group = select_coordinator_group(
            &self.config.selection,
            &request.txn_id,
            request.first_write_tablet.as_ref(),
            &self.config.status_partitions,
        )?;
        Ok((group, participants))
    }

    fn validate_status_set<T: RaftTransport>(
        members: &[TxnStatusGroup<T>],
        coordinator: RaftGroupId,
    ) -> Result<(), DistTxnError> {
        if members.is_empty() {
            return Err(DistTxnError::InvalidRequest(
                "no transaction-status members supplied".to_owned(),
            ));
        }
        if members
            .iter()
            .any(|member| member.raft_group_id() != coordinator)
        {
            return Err(DistTxnError::InvalidRequest(format!(
                "the supplied transaction-status members are not the selected coordinator group \
                 {coordinator}"
            )));
        }
        Ok(())
    }

    /// Phase 0: creates the `Pending` coordinator record (with expiry and
    /// the client's idempotency key). Idempotent: a replay under the same
    /// transaction id and key returns the existing record (including a
    /// terminal one, which short-circuits the caller's flow); the same
    /// transaction id under a different key is an
    /// [`DistTxnError::IdempotencyConflict`].
    pub async fn begin<T: RaftTransport>(
        &self,
        status: &[TxnStatusGroup<T>],
        request: &CommitRequest,
        control: &ExecutionControl,
    ) -> Result<TxnRecord, DistTxnError> {
        let (coordinator, participants) = self.coordinator_for(request)?;
        Self::validate_status_set(status, coordinator)?;
        let now = self.now()?;
        let max_observed = request
            .observed
            .iter()
            .copied()
            .max()
            .unwrap_or(HlcTimestamp::ZERO);
        let record = TxnRecord {
            txn_id: request.txn_id,
            state: DistributedTxnState::Pending,
            participants,
            prepare_ts: BTreeMap::new(),
            coordinator,
            created_at: now,
            heartbeat: now,
            expiry: self.expiry_from(now),
            max_observed,
            idempotency_key: request.idempotency_key,
        };
        let command_id = command_id_for(TAG_BEGIN, &request.txn_id, &[]);
        let (member, (_, rejection)) = retry_across_members(
            status,
            "txn-status begin",
            &self.config,
            control,
            |member| {
                let record = record.clone();
                let control = control.clone();
                async move {
                    member
                        .propose(command_id, CoordinatorCommand::Begin { record }, &control)
                        .await
                }
            },
        )
        .await?;
        match rejection {
            Some(StatusRejectionReason::IdempotencyKeyConflict { .. }) => {
                return Err(DistTxnError::IdempotencyConflict(request.txn_id));
            }
            Some(reason) => {
                return Err(DistTxnError::InvalidRequest(format!(
                    "begin refused by the coordinator: {reason}"
                )));
            }
            None => {}
        }
        let applied = member.record(&request.txn_id).ok_or_else(|| {
            DistTxnError::InvalidRequest("begin committed but no record is visible".to_owned())
        })?;
        if applied.idempotency_key != request.idempotency_key {
            return Err(DistTxnError::IdempotencyConflict(request.txn_id));
        }
        Ok(applied)
    }

    /// Refreshes the record's heartbeat and expiry (long transactions renew
    /// their lease against the push rule through this).
    pub async fn heartbeat<T: RaftTransport>(
        &self,
        status: &[TxnStatusGroup<T>],
        coordinator: RaftGroupId,
        txn_id: &TransactionId,
        control: &ExecutionControl,
    ) -> Result<TxnRecord, DistTxnError> {
        Self::validate_status_set(status, coordinator)?;
        let heartbeat = self.now()?;
        let expiry = self.expiry_from(heartbeat);
        let mut extra = Vec::with_capacity(16);
        extra.extend_from_slice(&heartbeat.physical_micros.to_le_bytes());
        extra.extend_from_slice(&heartbeat.logical.to_le_bytes());
        extra.extend_from_slice(&heartbeat.node_tiebreaker.to_le_bytes());
        let command_id = command_id_for(TAG_HEARTBEAT, txn_id, &extra);
        let (member, (_, rejection)) = retry_across_members(
            status,
            "txn-status heartbeat",
            &self.config,
            control,
            |member| {
                let control = control.clone();
                async move {
                    member
                        .propose(
                            command_id,
                            CoordinatorCommand::Heartbeat {
                                txn_id: *txn_id,
                                heartbeat,
                                expiry,
                            },
                            &control,
                        )
                        .await
                }
            },
        )
        .await?;
        match rejection {
            Some(StatusRejectionReason::UnknownTxn(_)) => {
                return Err(DistTxnError::InvalidRequest(format!(
                    "heartbeat for unknown transaction {txn_id}"
                )));
            }
            Some(reason) => {
                return Err(DistTxnError::InvalidRequest(format!(
                    "heartbeat refused by the coordinator: {reason}"
                )));
            }
            None => {}
        }
        member.record(txn_id).ok_or_else(|| {
            DistTxnError::InvalidRequest("heartbeat committed but no record is visible".to_owned())
        })
    }

    /// Phase 1 for one participant: persists the write intents through the
    /// participant's raft group and returns the prepare token (prepare
    /// timestamp + durability proof). A refusal (conflict, stale versions,
    /// lost resolution race) is [`DistTxnError::PrepareRejected`]; an
    /// idempotent replay returns the original stored prepare timestamp.
    pub async fn prepare_participant<T: RaftTransport, G: IntentGroupMember<T>>(
        &self,
        intents: &[G],
        txn_id: &TransactionId,
        writes: &ParticipantWrites,
        control: &ExecutionControl,
    ) -> Result<PrepareToken, DistTxnError> {
        if intents.is_empty()
            || intents
                .iter()
                .any(|member| member.raft_group_id() != writes.participant.raft_group_id)
        {
            return Err(DistTxnError::InvalidRequest(format!(
                "the supplied intent members are not the participant group {}",
                writes.participant.raft_group_id
            )));
        }
        let prepare_ts = self.now()?;
        let mut staged = writes.intents.clone();
        for intent in &mut staged {
            intent.txn_id = *txn_id;
            intent.prepare_ts = prepare_ts;
        }
        let command_id =
            command_id_for(TAG_PREPARE, txn_id, writes.participant.tablet_id.as_bytes());
        let (member, (receipt, rejection)) = retry_across_members(
            intents,
            "participant prepare",
            &self.config,
            control,
            |member| {
                let staged = staged.clone();
                let control = control.clone();
                async move {
                    member
                        .propose(
                            command_id,
                            IntentCommand::PersistIntents {
                                txn_id: *txn_id,
                                expected_schema_version: writes.expected_schema_version,
                                expected_authz_version: writes.expected_authz_version,
                                prepare_ts,
                                intents: staged,
                            },
                            &control,
                        )
                        .await
                }
            },
        )
        .await?;
        if let Some(reason) = rejection {
            return Err(DistTxnError::PrepareRejected(reason));
        }
        // The stored record is authoritative: an idempotent replay keeps
        // the original prepare timestamp.
        let stored = member.txn(txn_id).ok_or_else(|| {
            DistTxnError::InvalidRequest(
                "prepare committed but no intent record is visible".to_owned(),
            )
        })?;
        Ok(PrepareToken {
            txn_id: *txn_id,
            tablet_id: writes.participant.tablet_id,
            raft_group_id: writes.participant.raft_group_id,
            prepare_ts: stored.prepare_ts,
            position: receipt.position,
            command_id,
        })
    }

    /// Records one participant's prepare on the coordinator record
    /// (general path; the single-participant fast path skips it — see the
    /// module docs).
    async fn mark_prepared<T: RaftTransport>(
        &self,
        status: &[TxnStatusGroup<T>],
        txn_id: &TransactionId,
        token: &PrepareToken,
        observed: HlcTimestamp,
        control: &ExecutionControl,
    ) -> Result<(), DistTxnError> {
        let command_id = command_id_for(TAG_MARK, txn_id, token.tablet_id.as_bytes());
        let (_, (_, rejection)) = retry_across_members(
            status,
            "txn-status mark-preparing",
            &self.config,
            control,
            |member| {
                let control = control.clone();
                async move {
                    member
                        .propose(
                            command_id,
                            CoordinatorCommand::MarkPreparing {
                                txn_id: *txn_id,
                                tablet_id: token.tablet_id,
                                prepare_ts: token.prepare_ts,
                                observed,
                            },
                            &control,
                        )
                        .await
                }
            },
        )
        .await?;
        match rejection {
            None => Ok(()),
            Some(StatusRejectionReason::DecisionFinal { existing }) => Err(match existing {
                DistributedTxnState::Aborted { reason } => DistTxnError::Aborted(reason),
                state => DistTxnError::InvalidRequest(format!(
                    "mark-preparing raced a final decision: {state:?}"
                )),
            }),
            Some(reason) => Err(DistTxnError::InvalidRequest(format!(
                "mark-preparing refused by the coordinator: {reason}"
            ))),
        }
    }

    /// Phase 2: chooses `commit_ts` strictly greater than every observed
    /// timestamp (all prepare timestamps and the transaction's observed
    /// read/write timestamps, via [`HlcClock::next_after`]; spec section
    /// 8.2), persists `Committed` through the coordinator's raft group, and
    /// answers only after the decision is durable. The durable record is
    /// authoritative: if a race already decided the transaction, the
    /// recorded decision is returned (commit) or surfaced (abort).
    pub async fn decide_commit<T: RaftTransport, G: IntentGroupMember<T>>(
        &self,
        status: &[TxnStatusGroup<T>],
        participants: &BTreeMap<RaftGroupId, Vec<G>>,
        txn_id: &TransactionId,
        prepared: &[PrepareToken],
        observed: &[HlcTimestamp],
        control: &ExecutionControl,
    ) -> Result<TxnOutcome, DistTxnError> {
        let mut minimum = HlcTimestamp::ZERO;
        for token in prepared {
            minimum = minimum.max(token.prepare_ts);
        }
        for timestamp in observed {
            minimum = minimum.max(*timestamp);
        }
        let commit_ts = self.hlc.next_after(minimum);
        let command_id = command_id_for(TAG_COMMIT, txn_id, &[]);
        retry_across_members(
            status,
            "txn-status commit",
            &self.config,
            control,
            |member| {
                let control = control.clone();
                async move {
                    member
                        .propose(
                            command_id,
                            CoordinatorCommand::Commit {
                                txn_id: *txn_id,
                                commit_ts,
                            },
                            &control,
                        )
                        .await
                }
            },
        )
        .await
        .map_err(|error| DistTxnError::OutcomeAmbiguous {
            txn_id: *txn_id,
            detail: format!("decision proposal failed: {error}"),
        })?;
        self.finalize_decision(status, participants, txn_id, control)
            .await
    }

    /// Reads back the durable record after a decision proposal and
    /// broadcasts it to the participants. The record — never the proposal's
    /// local success — decides the returned outcome, so retries and races
    /// converge to the original durable decision.
    async fn finalize_decision<T: RaftTransport, G: IntentGroupMember<T>>(
        &self,
        status: &[TxnStatusGroup<T>],
        participants: &BTreeMap<RaftGroupId, Vec<G>>,
        txn_id: &TransactionId,
        control: &ExecutionControl,
    ) -> Result<TxnOutcome, DistTxnError> {
        let record = self
            .read_record(status, txn_id, control)
            .await
            .map_err(|error| DistTxnError::OutcomeAmbiguous {
                txn_id: *txn_id,
                detail: format!("decision read-back failed: {error}"),
            })?
            .ok_or_else(|| DistTxnError::OutcomeAmbiguous {
                txn_id: *txn_id,
                detail: "no coordinator record after the decision proposal".to_owned(),
            })?;
        match record.state.clone() {
            DistributedTxnState::Committed { commit_ts } => {
                self.broadcast_resolve(
                    participants,
                    txn_id,
                    &record.participants,
                    TxnDecision::Committed { commit_ts },
                    control,
                )
                .await;
                Ok(TxnOutcome {
                    txn_id: *txn_id,
                    commit_ts,
                    participants: record.participants.clone(),
                    durability: DurabilityLevel::Quorum,
                })
            }
            DistributedTxnState::Aborted { reason } => {
                self.broadcast_resolve(
                    participants,
                    txn_id,
                    &record.participants,
                    TxnDecision::Aborted {
                        reason: reason.clone(),
                    },
                    control,
                )
                .await;
                Err(DistTxnError::Aborted(reason))
            }
            state => Err(DistTxnError::OutcomeAmbiguous {
                txn_id: *txn_id,
                detail: format!("decision is not durable; record state is {state:?}"),
            }),
        }
    }

    /// Persists `Aborted` through the coordinator's raft group (unless a
    /// decision already landed) and broadcasts the final state. Returns the
    /// durable final state; the caller maps `Aborted` to
    /// [`DistTxnError::Aborted`] and a raced `Committed` to the commit
    /// outcome.
    async fn finalize_abort<T: RaftTransport, G: IntentGroupMember<T>>(
        &self,
        status: &[TxnStatusGroup<T>],
        participants: &BTreeMap<RaftGroupId, Vec<G>>,
        txn_id: &TransactionId,
        reason: AbortReason,
        control: &ExecutionControl,
    ) -> Result<DistributedTxnState, DistTxnError> {
        let command_id = command_id_for(TAG_ABORT, txn_id, &[]);
        retry_across_members(
            status,
            "txn-status abort",
            &self.config,
            control,
            |member| {
                let reason = reason.clone();
                let control = control.clone();
                async move {
                    member
                        .propose(
                            command_id,
                            CoordinatorCommand::Abort {
                                txn_id: *txn_id,
                                reason,
                            },
                            &control,
                        )
                        .await
                }
            },
        )
        .await
        .map_err(|error| DistTxnError::OutcomeAmbiguous {
            txn_id: *txn_id,
            detail: format!("abort proposal failed: {error}"),
        })?;
        let record = self
            .read_record(status, txn_id, control)
            .await
            .map_err(|error| DistTxnError::OutcomeAmbiguous {
                txn_id: *txn_id,
                detail: format!("abort read-back failed: {error}"),
            })?
            .ok_or_else(|| DistTxnError::OutcomeAmbiguous {
                txn_id: *txn_id,
                detail: "no coordinator record after the abort proposal".to_owned(),
            })?;
        match record.state.clone() {
            DistributedTxnState::Committed { commit_ts } => {
                self.broadcast_resolve(
                    participants,
                    txn_id,
                    &record.participants,
                    TxnDecision::Committed { commit_ts },
                    control,
                )
                .await;
                Ok(DistributedTxnState::Committed { commit_ts })
            }
            DistributedTxnState::Aborted { reason } => {
                self.broadcast_resolve(
                    participants,
                    txn_id,
                    &record.participants,
                    TxnDecision::Aborted {
                        reason: reason.clone(),
                    },
                    control,
                )
                .await;
                Ok(DistributedTxnState::Aborted { reason })
            }
            state => Err(DistTxnError::OutcomeAmbiguous {
                txn_id: *txn_id,
                detail: format!("abort is not durable; record state is {state:?}"),
            }),
        }
    }

    /// The full commit flow (spec section 12.8): begin, prepare every
    /// participant (recording progress on the general path), decide, answer
    /// after the decision is durable, then broadcast resolution. Any
    /// prepare failure or timeout persists `Aborted` and propagates; any
    /// post-fence failure is [`DistTxnError::OutcomeAmbiguous`] (never a
    /// false abort). Re-running with the same transaction id and
    /// idempotency key converges to the original outcome.
    ///
    /// Phase 1 fans out: every participant's prepare runs concurrently
    /// under the caller's deadline rather than serially. `commit_ts` derives
    /// from the greatest durably observed timestamp (a max over the whole
    /// prepare set), so the decision is identical regardless of completion
    /// order, and the first prepare failure in request order aborts —
    /// deterministically the same outcome as the serial rule.
    pub async fn commit<T: RaftTransport, G: IntentGroupMember<T>>(
        &self,
        status: &[TxnStatusGroup<T>],
        participants: &BTreeMap<RaftGroupId, Vec<G>>,
        request: CommitRequest,
        control: &ExecutionControl,
    ) -> Result<TxnOutcome, DistTxnError> {
        let record = self.begin(status, &request, control).await?;
        match record.state {
            // Retry of an already-decided transaction: the original outcome
            // stands; make sure resolution is (still) broadcast.
            DistributedTxnState::Committed { commit_ts } => {
                self.broadcast_resolve(
                    participants,
                    &request.txn_id,
                    &record.participants,
                    TxnDecision::Committed { commit_ts },
                    control,
                )
                .await;
                return Ok(TxnOutcome {
                    txn_id: request.txn_id,
                    commit_ts,
                    participants: record.participants.clone(),
                    durability: DurabilityLevel::Quorum,
                });
            }
            DistributedTxnState::Aborted { reason } => {
                self.broadcast_resolve(
                    participants,
                    &request.txn_id,
                    &record.participants,
                    TxnDecision::Aborted {
                        reason: reason.clone(),
                    },
                    control,
                )
                .await;
                return Err(DistTxnError::Aborted(reason));
            }
            _ => {}
        }
        // Single-participant fast path: no MarkPreparing progress write
        // (see the module docs).
        let general_path = request.writes.len() > 1;
        let mut prepare_futures = Vec::with_capacity(request.writes.len());
        for writes in &request.writes {
            let intent_members = participants
                .get(&writes.participant.raft_group_id)
                .ok_or_else(|| {
                    DistTxnError::InvalidRequest(format!(
                        "no intent members supplied for participant group {}",
                        writes.participant.raft_group_id
                    ))
                })?;
            prepare_futures.push(self.prepare_participant(
                intent_members,
                &request.txn_id,
                writes,
                control,
            ));
        }
        let results = run_all(prepare_futures).await;
        // The first failure in request order aborts (the serial rule).
        let mut prepared = Vec::with_capacity(results.len());
        for result in results {
            match result {
                Ok(token) => prepared.push(token),
                Err(error) => {
                    let reason = abort_reason_of(&error);
                    return match self
                        .finalize_abort(status, participants, &request.txn_id, reason, control)
                        .await?
                    {
                        DistributedTxnState::Aborted { reason } => {
                            Err(DistTxnError::Aborted(reason))
                        }
                        DistributedTxnState::Committed { commit_ts } => Ok(TxnOutcome {
                            txn_id: request.txn_id,
                            commit_ts,
                            participants: record.participants.clone(),
                            durability: DurabilityLevel::Quorum,
                        }),
                        state => Err(DistTxnError::OutcomeAmbiguous {
                            txn_id: request.txn_id,
                            detail: format!(
                                "abort after a failed prepare is not durable; state is {state:?}"
                            ),
                        }),
                    };
                }
            }
        }
        let mut observed = request.observed.clone();
        if general_path {
            observed.extend(prepared.iter().map(|token| token.prepare_ts));
            let max_observed = observed.iter().copied().max().unwrap_or(HlcTimestamp::ZERO);
            for token in &prepared {
                if let Err(error) = self
                    .mark_prepared(status, &request.txn_id, token, max_observed, control)
                    .await
                {
                    let reason = match error {
                        DistTxnError::Aborted(reason) => reason,
                        other => AbortReason::Error(other.to_string()),
                    };
                    match self
                        .finalize_abort(status, participants, &request.txn_id, reason, control)
                        .await?
                    {
                        DistributedTxnState::Aborted { reason } => {
                            return Err(DistTxnError::Aborted(reason));
                        }
                        DistributedTxnState::Committed { commit_ts } => {
                            return Ok(TxnOutcome {
                                txn_id: request.txn_id,
                                commit_ts,
                                participants: record.participants.clone(),
                                durability: DurabilityLevel::Quorum,
                            });
                        }
                        state => {
                            return Err(DistTxnError::OutcomeAmbiguous {
                                txn_id: request.txn_id,
                                detail: format!(
                                    "abort after a failed mark is not durable; state is {state:?}"
                                ),
                            });
                        }
                    }
                }
            }
        }
        self.decide_commit(
            status,
            participants,
            &request.txn_id,
            &prepared,
            &observed,
            control,
        )
        .await
    }

    /// Client-driven abort: persists `Aborted` through the coordinator's
    /// raft group and removes intents at every participant. A transaction
    /// that already committed cannot abort — the durable commit outcome is
    /// returned instead.
    pub async fn abort<T: RaftTransport, G: IntentGroupMember<T>>(
        &self,
        status: &[TxnStatusGroup<T>],
        participants: &BTreeMap<RaftGroupId, Vec<G>>,
        txn_id: &TransactionId,
        reason: AbortReason,
        control: &ExecutionControl,
    ) -> Result<DistributedTxnState, DistTxnError> {
        self.finalize_abort(status, participants, txn_id, reason, control)
            .await
    }

    /// Broadcasts the durable decision to every participant (best effort:
    /// groups that do not answer are left to lazy recovery — the sweep and
    /// drive-by resolution re-deliver the same deterministic command).
    pub async fn broadcast_resolve<T: RaftTransport, G: IntentGroupMember<T>>(
        &self,
        participants: &BTreeMap<RaftGroupId, Vec<G>>,
        txn_id: &TransactionId,
        txn_participants: &[TxnParticipant],
        decision: TxnDecision,
        control: &ExecutionControl,
    ) -> ResolveBroadcast {
        let mut report = ResolveBroadcast::default();
        for participant in txn_participants {
            let command_id = command_id_for(TAG_RESOLVE, txn_id, participant.tablet_id.as_bytes());
            let Some(members) = participants.get(&participant.raft_group_id) else {
                report.deferred.push(participant.raft_group_id);
                continue;
            };
            let result = retry_across_members(
                members,
                "participant resolve",
                &self.config,
                control,
                |member| {
                    let decision = decision.clone();
                    let control = control.clone();
                    async move {
                        member
                            .propose(
                                command_id,
                                IntentCommand::Resolve {
                                    txn_id: *txn_id,
                                    decision,
                                },
                                &control,
                            )
                            .await
                    }
                },
            )
            .await;
            match result {
                Ok(_) => report.resolved.push(participant.raft_group_id),
                Err(_) => report.deferred.push(participant.raft_group_id),
            }
        }
        report
    }

    /// Reads the coordinator record behind a linearizable barrier, retrying
    /// across members (spec section 12.8: any node may query the record).
    pub async fn read_record<T: RaftTransport>(
        &self,
        status: &[TxnStatusGroup<T>],
        txn_id: &TransactionId,
        control: &ExecutionControl,
    ) -> Result<Option<TxnRecord>, DistTxnError> {
        let (_, record) =
            retry_across_members(status, "txn-status read", &self.config, control, |member| {
                let control = control.clone();
                async move {
                    member
                        .record_consistent(txn_id, &ReadConsistency::Linearizable, &control)
                        .await
                }
            })
            .await?;
        Ok(record)
    }

    /// Probes one participant group for the transaction's intent record
    /// (any member's applied view proves durability — an intent record only
    /// appears after its raft entry committed; absence is treated
    /// conservatively, never as abort evidence).
    fn probe_participant<T: RaftTransport, G: IntentGroupMember<T>>(
        participants: &BTreeMap<RaftGroupId, Vec<G>>,
        participant: &TxnParticipant,
        txn_id: &TransactionId,
    ) -> Option<ParticipantTxn> {
        let members = participants.get(&participant.raft_group_id)?;
        members.iter().find_map(|member| member.txn(txn_id))
    }

    /// Recovery (spec section 12.8): continues the protocol from the
    /// replicated records after a coordinator change. A terminal decision
    /// is (re-)broadcast. A non-terminal transaction with every
    /// participant's intents durable is committed with `commit_ts` strictly
    /// greater than every durably observed timestamp; one with missing
    /// intents is pushed to `Aborted` only when its heartbeat expiry has
    /// passed — otherwise it is left to its (possibly live) coordinator.
    pub async fn recover<T: RaftTransport, G: IntentGroupMember<T>>(
        &self,
        status: &[TxnStatusGroup<T>],
        participants: &BTreeMap<RaftGroupId, Vec<G>>,
        txn_id: &TransactionId,
        now: HlcTimestamp,
        control: &ExecutionControl,
    ) -> Result<RecoveryOutcome, DistTxnError> {
        let Some(record) = self.read_record(status, txn_id, control).await? else {
            return Ok(RecoveryOutcome::NotFound);
        };
        match record.state.clone() {
            DistributedTxnState::Committed { commit_ts } => {
                self.broadcast_resolve(
                    participants,
                    txn_id,
                    &record.participants,
                    TxnDecision::Committed { commit_ts },
                    control,
                )
                .await;
                Ok(RecoveryOutcome::Decided(DistributedTxnState::Committed {
                    commit_ts,
                }))
            }
            DistributedTxnState::Aborted { reason } => {
                self.broadcast_resolve(
                    participants,
                    txn_id,
                    &record.participants,
                    TxnDecision::Aborted {
                        reason: reason.clone(),
                    },
                    control,
                )
                .await;
                Ok(RecoveryOutcome::Decided(DistributedTxnState::Aborted {
                    reason,
                }))
            }
            DistributedTxnState::Pending | DistributedTxnState::Preparing => {
                let mut prepare_ts: Vec<HlcTimestamp> = Vec::new();
                let mut missing = 0_usize;
                for participant in &record.participants {
                    match Self::probe_participant(participants, participant, txn_id) {
                        Some(ParticipantTxn {
                            resolution: None,
                            prepare_ts: ts,
                            ..
                        }) => prepare_ts.push(ts),
                        // A locally resolved participant already follows a
                        // durable decision the linearizable record read
                        // would show; anything else means the intents are
                        // not (provably) durable.
                        _ => missing += 1,
                    }
                }
                if missing == 0 {
                    // Every intent is durable: drive the decision. The
                    // commit timestamp exceeds every durably observed
                    // timestamp (probed prepare timestamps, the recorded
                    // per-participant marks, and the record's max_observed).
                    let mut minimum = record.max_observed;
                    for ts in prepare_ts {
                        minimum = minimum.max(ts);
                    }
                    for ts in record.prepare_ts.values() {
                        minimum = minimum.max(*ts);
                    }
                    let commit_ts = self.hlc.next_after(minimum);
                    let command_id = command_id_for(TAG_COMMIT, txn_id, &[]);
                    retry_across_members(
                        status,
                        "txn-status commit",
                        &self.config,
                        control,
                        |member| {
                            let control = control.clone();
                            async move {
                                member
                                    .propose(
                                        command_id,
                                        CoordinatorCommand::Commit {
                                            txn_id: *txn_id,
                                            commit_ts,
                                        },
                                        &control,
                                    )
                                    .await
                            }
                        },
                    )
                    .await?;
                    let outcome = self
                        .finalize_decision(status, participants, txn_id, control)
                        .await;
                    return match outcome {
                        Ok(outcome) => {
                            Ok(RecoveryOutcome::Decided(DistributedTxnState::Committed {
                                commit_ts: outcome.commit_ts,
                            }))
                        }
                        Err(DistTxnError::Aborted(reason)) => {
                            Ok(RecoveryOutcome::Decided(DistributedTxnState::Aborted {
                                reason,
                            }))
                        }
                        Err(error) => Err(error),
                    };
                }
                if record.expired(now) {
                    // The documented push rule: heartbeat expiry, through
                    // the replicated record (never an in-memory hint).
                    let reason = AbortReason::Cancelled(format!(
                        "transaction heartbeat expired at {:?} (pushed by recovery)",
                        record.expiry
                    ));
                    let state = self
                        .finalize_abort(status, participants, txn_id, reason, control)
                        .await?;
                    return Ok(RecoveryOutcome::Decided(state));
                }
                Ok(RecoveryOutcome::InFlight {
                    missing_participants: missing,
                })
            }
        }
    }

    /// The push half of recovery (spec section 12.8 "push expired pending
    /// transaction"): forces `Aborted` on a heartbeat-expired non-terminal
    /// record — and only there. A terminal record is re-broadcast; an
    /// unexpired one is left alone (never aborted on suspicion).
    pub async fn push_expired<T: RaftTransport, G: IntentGroupMember<T>>(
        &self,
        status: &[TxnStatusGroup<T>],
        participants: &BTreeMap<RaftGroupId, Vec<G>>,
        txn_id: &TransactionId,
        now: HlcTimestamp,
        control: &ExecutionControl,
    ) -> Result<PushOutcome, DistTxnError> {
        let Some(record) = self.read_record(status, txn_id, control).await? else {
            return Ok(PushOutcome::NotFound);
        };
        match record.state.clone() {
            DistributedTxnState::Committed { commit_ts } => {
                self.broadcast_resolve(
                    participants,
                    txn_id,
                    &record.participants,
                    TxnDecision::Committed { commit_ts },
                    control,
                )
                .await;
                Ok(PushOutcome::Terminal(DistributedTxnState::Committed {
                    commit_ts,
                }))
            }
            DistributedTxnState::Aborted { reason } => {
                self.broadcast_resolve(
                    participants,
                    txn_id,
                    &record.participants,
                    TxnDecision::Aborted {
                        reason: reason.clone(),
                    },
                    control,
                )
                .await;
                Ok(PushOutcome::Terminal(DistributedTxnState::Aborted {
                    reason,
                }))
            }
            DistributedTxnState::Pending | DistributedTxnState::Preparing => {
                if !record.expired(now) {
                    return Ok(PushOutcome::NotExpired);
                }
                let reason = AbortReason::Cancelled(format!(
                    "transaction heartbeat expired at {:?} (pushed by a third party)",
                    record.expiry
                ));
                let state = self
                    .finalize_abort(status, participants, txn_id, reason, control)
                    .await?;
                Ok(PushOutcome::Pushed(state))
            }
        }
    }

    /// Lazy recovery of one orphan intent set (spec section 12.8 "orphaned
    /// intents"): checks the coordinator record and acts exactly by the
    /// rules — a durable decision resolves the intents in that direction; a
    /// heartbeat-expired non-terminal record is pushed first, then
    /// resolved; anything else is left untouched.
    pub async fn resolve_orphan<T: RaftTransport, G: IntentGroupMember<T>>(
        &self,
        status: &[TxnStatusGroup<T>],
        intent_members: &[G],
        txn_id: &TransactionId,
        now: HlcTimestamp,
        control: &ExecutionControl,
    ) -> Result<OrphanOutcome, DistTxnError> {
        let Some(record) = self.read_record(status, txn_id, control).await? else {
            return Ok(OrphanOutcome::UnknownRecord);
        };
        let (decision, pushed) = match record.state.clone() {
            DistributedTxnState::Committed { commit_ts } => {
                (TxnDecision::Committed { commit_ts }, false)
            }
            DistributedTxnState::Aborted { reason } => (TxnDecision::Aborted { reason }, false),
            DistributedTxnState::Pending | DistributedTxnState::Preparing => {
                if !record.expired(now) {
                    return Ok(OrphanOutcome::InFlight);
                }
                let reason = AbortReason::Cancelled(format!(
                    "transaction heartbeat expired at {:?} (pushed by orphan recovery)",
                    record.expiry
                ));
                let decision = match self
                    .finalize_abort_for_one(status, intent_members, &record, reason, control)
                    .await?
                {
                    DistributedTxnState::Committed { commit_ts } => {
                        TxnDecision::Committed { commit_ts }
                    }
                    DistributedTxnState::Aborted { reason } => TxnDecision::Aborted { reason },
                    state => {
                        return Err(DistTxnError::OutcomeAmbiguous {
                            txn_id: *txn_id,
                            detail: format!("orphan push is not durable; state is {state:?}"),
                        });
                    }
                };
                (decision, true)
            }
        };
        // The push path already resolved this participant inside
        // finalize_abort_for_one; the terminal path resolves here.
        if !pushed {
            let participant = record.participants.iter().find(|participant| {
                intent_members
                    .first()
                    .is_some_and(|member| member.raft_group_id() == participant.raft_group_id)
            });
            let tablet = participant.map_or(TabletId::ZERO, |participant| participant.tablet_id);
            let command_id = command_id_for(TAG_RESOLVE, txn_id, tablet.as_bytes());
            retry_across_members(
                intent_members,
                "participant resolve",
                &self.config,
                control,
                |member| {
                    let decision = decision.clone();
                    let control = control.clone();
                    async move {
                        member
                            .propose(
                                command_id,
                                IntentCommand::Resolve {
                                    txn_id: *txn_id,
                                    decision,
                                },
                                &control,
                            )
                            .await
                    }
                },
            )
            .await?;
        }
        match decision {
            TxnDecision::Committed { commit_ts } => Ok(OrphanOutcome::ResolvedCommit { commit_ts }),
            TxnDecision::Aborted { reason } if pushed => Ok(OrphanOutcome::PushedAbort { reason }),
            TxnDecision::Aborted { reason } => Ok(OrphanOutcome::ResolvedAbort { reason }),
        }
    }

    /// Abort push targeted at the single intent group that found the
    /// orphan (the resolve broadcast reaches the record's other
    /// participants only when they are supplied; here the local group is
    /// resolved directly and deterministically).
    async fn finalize_abort_for_one<T: RaftTransport, G: IntentGroupMember<T>>(
        &self,
        status: &[TxnStatusGroup<T>],
        intent_members: &[G],
        record: &TxnRecord,
        reason: AbortReason,
        control: &ExecutionControl,
    ) -> Result<DistributedTxnState, DistTxnError> {
        let txn_id = record.txn_id;
        let command_id = command_id_for(TAG_ABORT, &txn_id, &[]);
        retry_across_members(
            status,
            "txn-status abort",
            &self.config,
            control,
            |member| {
                let reason = reason.clone();
                let control = control.clone();
                async move {
                    member
                        .propose(
                            command_id,
                            CoordinatorCommand::Abort { txn_id, reason },
                            &control,
                        )
                        .await
                }
            },
        )
        .await
        .map_err(|error| DistTxnError::OutcomeAmbiguous {
            txn_id,
            detail: format!("orphan push failed: {error}"),
        })?;
        let final_record = self
            .read_record(status, &txn_id, control)
            .await?
            .ok_or_else(|| DistTxnError::OutcomeAmbiguous {
                txn_id,
                detail: "no coordinator record after the orphan push".to_owned(),
            })?;
        let decision = match final_record.state.clone() {
            DistributedTxnState::Committed { commit_ts } => TxnDecision::Committed { commit_ts },
            DistributedTxnState::Aborted { reason } => TxnDecision::Aborted { reason },
            state => {
                return Err(DistTxnError::OutcomeAmbiguous {
                    txn_id,
                    detail: format!("orphan push is not durable; state is {state:?}"),
                });
            }
        };
        let participant = record.participants.iter().find(|participant| {
            intent_members
                .first()
                .is_some_and(|member| member.raft_group_id() == participant.raft_group_id)
        });
        let tablet = participant.map_or(TabletId::ZERO, |participant| participant.tablet_id);
        let resolve_id = command_id_for(TAG_RESOLVE, &txn_id, tablet.as_bytes());
        retry_across_members(
            intent_members,
            "participant resolve",
            &self.config,
            control,
            |member| {
                let decision = decision.clone();
                let control = control.clone();
                async move {
                    member
                        .propose(
                            resolve_id,
                            IntentCommand::Resolve { txn_id, decision },
                            &control,
                        )
                        .await
                }
            },
        )
        .await?;
        Ok(final_record.state.clone())
    }

    /// The orphan sweep (spec section 12.8): scans one participant group's
    /// unresolved intents and resolves each by the orphan rules
    /// ([`DistTxnDriver::resolve_orphan`]). Live, unexpired transactions
    /// are left untouched — the timeout rules are honored, never
    /// suspicion.
    pub async fn sweep_orphans<T: RaftTransport, G: IntentGroupMember<T>>(
        &self,
        status: &[TxnStatusGroup<T>],
        intent_members: &[G],
        now: HlcTimestamp,
        control: &ExecutionControl,
    ) -> Result<SweepReport, DistTxnError> {
        let mut report = SweepReport::default();
        if intent_members.is_empty() {
            return Err(DistTxnError::InvalidRequest(
                "no intent members supplied for the sweep".to_owned(),
            ));
        }
        // The freshest local view enumerates the orphans (advisory only —
        // every action still goes through the replicated records).
        let orphans = intent_members
            .iter()
            .max_by_key(|member| member.group().applied_position().index)
            .map(|member| member.unresolved_txn_ids())
            .unwrap_or_default();
        for txn_id in orphans {
            match self
                .resolve_orphan(status, intent_members, &txn_id, now, control)
                .await?
            {
                OrphanOutcome::ResolvedCommit { .. } => report.resolved_committed += 1,
                OrphanOutcome::ResolvedAbort { .. } => report.resolved_aborted += 1,
                OrphanOutcome::PushedAbort { .. } => {
                    report.pushed += 1;
                    report.resolved_aborted += 1;
                }
                OrphanOutcome::InFlight => report.in_flight += 1,
                OrphanOutcome::UnknownRecord => report.unknown += 1,
            }
        }
        Ok(report)
    }

    /// Sweeps resolved intent tombstones from one participant group
    /// (spec section 12.8): tombstones whose `resolved_at` is older than
    /// `now - resolved_retention` are removed, at most
    /// [`DistTxnConfig::sweep_limit`] per call, keeping the participant
    /// state bounded. The sweep is itself a replicated command, so every
    /// replica removes the identical set deterministically. Unresolved
    /// intents are never touched. Engine state is untouched: sweeping a
    /// tombstone only drops the protocol record (the rows a committed
    /// resolution applied stay applied).
    ///
    /// Retention rule: the retention must exceed the longest possible
    /// prepare→resolve gap (bounded by the heartbeat-expiry push rules).
    /// The driver API never re-prepares a terminal transaction — a commit
    /// retry short-circuits on the durable coordinator record — so a
    /// prepare landing after its tombstone was swept is unreachable through
    /// [`DistTxnDriver::commit`]; only a caller driving
    /// [`DistTxnDriver::prepare_participant`] directly for an aged-out
    /// transaction could stage one, and the orphan machinery would then
    /// resolve it against the durable record.
    pub async fn sweep_resolved<T: RaftTransport, G: IntentGroupMember<T>>(
        &self,
        intent_members: &[G],
        now: HlcTimestamp,
        control: &ExecutionControl,
    ) -> Result<SweepResolvedReport, DistTxnError> {
        if intent_members.is_empty() {
            return Err(DistTxnError::InvalidRequest(
                "no intent members supplied for the resolved sweep".to_owned(),
            ));
        }
        let older_than = HlcTimestamp {
            physical_micros: now.physical_micros.saturating_sub(
                u64::try_from(self.config.resolved_retention.as_micros()).unwrap_or(u64::MAX),
            ),
            logical: 0,
            node_tiebreaker: 0,
        };
        let eligible = |state: &IntentState| {
            state
                .txns
                .values()
                .filter(|txn| {
                    txn.resolution.is_some() && txn.resolved_at.is_some_and(|at| at < older_than)
                })
                .count()
        };
        let before = intent_members
            .iter()
            .map(|member| eligible(&member.state()))
            .max()
            .unwrap_or(0);
        let mut extra = Vec::with_capacity(24);
        extra.extend_from_slice(&older_than.physical_micros.to_le_bytes());
        extra.extend_from_slice(&self.config.sweep_limit.to_le_bytes());
        let command_id = command_id_for(TAG_SWEEP, &TransactionId::ZERO, &extra);
        let (member, _) = retry_across_members(
            intent_members,
            "participant resolved sweep",
            &self.config,
            control,
            |member| {
                let control = control.clone();
                async move {
                    member
                        .propose(
                            command_id,
                            IntentCommand::SweepResolved {
                                older_than,
                                limit: self.config.sweep_limit,
                            },
                            &control,
                        )
                        .await
                }
            },
        )
        .await?;
        let after = eligible(&member.state());
        Ok(SweepResolvedReport {
            swept: before.saturating_sub(after),
            remaining: after,
        })
    }
}

/// Drives every future to completion concurrently on the current task and
/// returns the outputs in input order. Used for the phase-1 prepare fan-out:
/// the caller derives the outcome from the whole result set (greatest
/// prepare timestamp; first failure in request order), never from
/// completion order, so concurrency changes nothing observable.
async fn run_all<F, T>(futures: Vec<F>) -> Vec<T>
where
    F: Future<Output = T>,
{
    let mut pending: Vec<Option<std::pin::Pin<Box<F>>>> = futures
        .into_iter()
        .map(|future| Some(Box::pin(future)))
        .collect();
    let mut outputs: Vec<Option<T>> = std::iter::repeat_with(|| None)
        .take(pending.len())
        .collect();
    let mut remaining = pending.len();
    std::future::poll_fn(|cx| {
        for (index, slot) in pending.iter_mut().enumerate() {
            let Some(future) = slot else { continue };
            match future.as_mut().poll(cx) {
                std::task::Poll::Ready(output) => {
                    *slot = None;
                    outputs[index] = Some(output);
                    remaining -= 1;
                }
                std::task::Poll::Pending => {}
            }
        }
        if remaining == 0 {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    })
    .await;
    outputs
        .into_iter()
        .map(|output| output.expect("every future completed"))
        .collect()
}

/// Maps a prepare-phase failure onto the abort reason persisted with the
/// `Aborted` decision.
fn abort_reason_of(error: &DistTxnError) -> AbortReason {
    match error {
        DistTxnError::PrepareRejected(PrepareRejectionReason::KeyConflict { holder, .. }) => {
            AbortReason::Conflict(format!("write intent conflict with transaction {holder}"))
        }
        DistTxnError::PrepareRejected(reason) => AbortReason::Validation(reason.to_string()),
        DistTxnError::Consensus(ConsensusError::Cancelled) => {
            AbortReason::Cancelled("cancelled during prepare".to_owned())
        }
        DistTxnError::Consensus(ConsensusError::DeadlineExceeded) => {
            AbortReason::Cancelled("deadline exceeded during prepare".to_owned())
        }
        other => AbortReason::Error(other.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::{PartitionBounds, TabletState};
    use mongreldb_consensus::network::InMemoryTransport;
    use mongreldb_types::ids::TableId;
    use std::time::Instant;

    // -- small helpers ------------------------------------------------------

    fn ts(micros: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros: micros,
            logical: 0,
            node_tiebreaker: 0,
        }
    }

    fn gid(byte: u8) -> RaftGroupId {
        RaftGroupId::from_bytes([byte; 16])
    }

    fn tid(byte: u8) -> TabletId {
        TabletId::from_bytes([byte; 16])
    }

    fn xid(byte: u8) -> TransactionId {
        TransactionId::from_bytes([byte; 16])
    }

    fn later(base: HlcTimestamp, delta: Duration) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros: base
                .physical_micros
                .saturating_add(u64::try_from(delta.as_micros()).unwrap_or(u64::MAX)),
            logical: base.logical,
            node_tiebreaker: base.node_tiebreaker,
        }
    }

    fn one_before(ts: HlcTimestamp) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros: ts.physical_micros.saturating_sub(1),
            logical: ts.logical,
            node_tiebreaker: ts.node_tiebreaker,
        }
    }

    fn record(txn_id: TransactionId, participants: Vec<TxnParticipant>) -> TxnRecord {
        TxnRecord {
            txn_id,
            state: DistributedTxnState::Pending,
            participants,
            prepare_ts: BTreeMap::new(),
            coordinator: gid(90),
            created_at: ts(1_000),
            heartbeat: ts(1_000),
            expiry: ts(2_000),
            max_observed: HlcTimestamp::ZERO,
            idempotency_key: [7u8; 16],
        }
    }

    fn participant(group: u8, tablet: u8) -> TxnParticipant {
        TxnParticipant {
            tablet_id: tid(tablet),
            raft_group_id: gid(group),
        }
    }

    fn intent(txn: u8, key: &[u8], prepare_micros: u64) -> WriteIntent {
        WriteIntent {
            txn_id: xid(txn),
            key: key.to_vec(),
            value_ref: format!("value-{}", String::from_utf8_lossy(key)).into_bytes(),
            prepare_ts: ts(prepare_micros),
        }
    }

    fn coordinator_envelope(
        id: [u8; 16],
        command: CoordinatorCommand,
        index_micros: u64,
    ) -> ReplicatedCommand {
        let payload = CoordinatorCommandRecord::new(command).encode().unwrap();
        ReplicatedCommand::new(
            CommandKind::Transaction,
            CommandEnvelope::new(COMMAND_TYPE_DIST_TXN_COORDINATOR, id, payload),
            ts(index_micros),
        )
    }

    fn intent_envelope(
        id: [u8; 16],
        command: IntentCommand,
        index_micros: u64,
    ) -> ReplicatedCommand {
        let payload = IntentCommandRecord::new(command).encode().unwrap();
        ReplicatedCommand::new(
            CommandKind::Transaction,
            CommandEnvelope::new(COMMAND_TYPE_DIST_TXN_INTENT, id, payload),
            ts(index_micros),
        )
    }

    fn applied(index: u64, command: ReplicatedCommand) -> AppliedCommand {
        AppliedCommand {
            position: LogPosition { term: 1, index },
            command,
        }
    }

    // -- coordinator selection + hashing ------------------------------------

    #[test]
    fn fnv1a_is_deterministic_and_matches_the_empty_input_constant() {
        assert_eq!(fnv1a_64(&[]), 0xcbf29ce484222325);
        assert_eq!(fnv1a_64(b"mongrel"), fnv1a_64(b"mongrel"));
        assert_ne!(fnv1a_64(b"mongrel"), fnv1a_64(b"mongrele"));
    }

    #[test]
    fn command_ids_are_deterministic_and_distinct_per_step() {
        let txn = xid(5);
        assert_eq!(
            command_id_for(TAG_BEGIN, &txn, &[]),
            command_id_for(TAG_BEGIN, &txn, &[])
        );
        let ids: Vec<[u8; 16]> = [
            TAG_BEGIN,
            TAG_MARK,
            TAG_COMMIT,
            TAG_ABORT,
            TAG_PREPARE,
            TAG_RESOLVE,
        ]
        .iter()
        .map(|tag| command_id_for(tag, &txn, &[]))
        .collect();
        for (i, left) in ids.iter().enumerate() {
            for right in ids.iter().skip(i + 1) {
                assert_ne!(left, right);
            }
        }
        // The id changes with the transaction id.
        assert_ne!(
            command_id_for(TAG_BEGIN, &xid(6), &[]),
            command_id_for(TAG_BEGIN, &txn, &[])
        );
    }

    #[test]
    fn txn_id_derived_selection_is_deterministic() {
        let mut partitions = BTreeMap::new();
        for index in 0..4_u32 {
            partitions.insert(
                index,
                TxnStatusPartition {
                    partition_id: index,
                    home_raft_group: gid(50 + index as u8),
                },
            );
        }
        let txn = xid(9);
        let expected_index = txn_status_partition_index(&txn, 4) as usize;
        let first =
            select_coordinator_group(&CoordinatorSelection::TxnIdDerived, &txn, None, &partitions)
                .unwrap();
        let second =
            select_coordinator_group(&CoordinatorSelection::TxnIdDerived, &txn, None, &partitions)
                .unwrap();
        assert_eq!(first, second);
        assert_eq!(first, gid(50 + expected_index as u8));
        // The default selection mode is txn-id-derived.
        assert_eq!(
            CoordinatorSelection::default(),
            CoordinatorSelection::TxnIdDerived
        );
        // No partitions published: fail closed.
        assert!(select_coordinator_group(
            &CoordinatorSelection::TxnIdDerived,
            &txn,
            None,
            &BTreeMap::new()
        )
        .is_err());
    }

    #[test]
    fn first_write_home_selection_uses_the_tablets_group() {
        let tablet = TabletDescriptor {
            tablet_id: tid(7),
            table_id: TableId::new(3),
            raft_group_id: gid(42),
            partition: PartitionBounds::unbounded(),
            replicas: Vec::new(),
            leader_hint: None,
            generation: 1,
            state: TabletState::Active,
        };
        let selected = select_coordinator_group(
            &CoordinatorSelection::FirstWriteHome,
            &xid(1),
            Some(&tablet),
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(selected, gid(42));
        // Without the first write's tablet the mode fails closed.
        assert!(select_coordinator_group(
            &CoordinatorSelection::FirstWriteHome,
            &xid(1),
            None,
            &BTreeMap::new()
        )
        .is_err());
    }

    // -- serde + versioned records ------------------------------------------

    #[test]
    fn command_records_round_trip_and_fail_closed_on_bad_versions() {
        let coordinator = CoordinatorCommandRecord::new(CoordinatorCommand::Begin {
            record: record(xid(1), vec![participant(91, 1)]),
        });
        let decoded = CoordinatorCommandRecord::decode(&coordinator.encode().unwrap()).unwrap();
        assert_eq!(decoded, coordinator);
        let intent_record = IntentCommandRecord::new(IntentCommand::PersistIntents {
            txn_id: xid(1),
            expected_schema_version: SchemaVersion::new(4),
            expected_authz_version: 2,
            prepare_ts: ts(9),
            intents: vec![intent(1, b"k", 9)],
        });
        let decoded = IntentCommandRecord::decode(&intent_record.encode().unwrap()).unwrap();
        assert_eq!(decoded, intent_record);

        // Unsupported versions and malformed payloads fail closed.
        let future = CoordinatorCommandRecord {
            format_version: DIST_TXN_RECORD_FORMAT_VERSION + 1,
            command: CoordinatorCommand::Abort {
                txn_id: xid(1),
                reason: AbortReason::RolledBack,
            },
        };
        let error = CoordinatorCommandRecord::decode(&future.encode().unwrap()).unwrap_err();
        assert!(matches!(
            error,
            DistTxnDecodeError::UnsupportedVersion { .. }
        ));
        assert!(matches!(
            CoordinatorCommandRecord::decode(b"not json"),
            Err(DistTxnDecodeError::Malformed(_))
        ));
    }

    #[test]
    fn protocol_types_serde_round_trip() {
        let states = vec![
            DistributedTxnState::Pending,
            DistributedTxnState::Preparing,
            DistributedTxnState::Committed { commit_ts: ts(5) },
            DistributedTxnState::Aborted {
                reason: AbortReason::Conflict("conflict".to_owned()),
            },
        ];
        for state in states {
            let json = serde_json::to_string(&state).unwrap();
            assert_eq!(
                serde_json::from_str::<DistributedTxnState>(&json).unwrap(),
                state
            );
        }
        let mut full = record(xid(2), vec![participant(91, 1), participant(92, 2)]);
        full.prepare_ts.insert(tid(1), ts(11));
        full.prepare_ts.insert(tid(2), ts(12));
        full.state = DistributedTxnState::Preparing;
        full.max_observed = ts(10);
        let json = serde_json::to_string(&full).unwrap();
        assert_eq!(serde_json::from_str::<TxnRecord>(&json).unwrap(), full);
    }

    // -- coordinator sink semantics ------------------------------------------

    #[test]
    fn status_sink_begin_is_idempotent_and_terminal_states_are_final() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sink = TxnStatusApplySink::open(tmp.path()).unwrap();
        let txn = xid(1);
        let begin = CoordinatorCommand::Begin {
            record: record(txn, vec![participant(91, 1)]),
        };
        sink.apply(&applied(
            1,
            coordinator_envelope([1u8; 16], begin.clone(), 100),
        ))
        .unwrap();
        // Replay under the same key: no-op, no journal.
        sink.apply(&applied(2, coordinator_envelope([2u8; 16], begin, 101)))
            .unwrap();
        assert!(sink.state().rejections.is_empty());
        // Same transaction id under a different key: journaled conflict.
        let mut conflicting = record(txn, vec![participant(91, 1)]);
        conflicting.idempotency_key = [9u8; 16];
        sink.apply(&applied(
            3,
            coordinator_envelope(
                [3u8; 16],
                CoordinatorCommand::Begin {
                    record: conflicting,
                },
                102,
            ),
        ))
        .unwrap();
        assert_eq!(sink.state().rejections.len(), 1);
        assert!(matches!(
            sink.state().rejections[0].reason,
            StatusRejectionReason::IdempotencyKeyConflict { existing } if existing == [7u8; 16]
        ));
        // Prepare progress, then commit.
        sink.apply(&applied(
            4,
            coordinator_envelope(
                [4u8; 16],
                CoordinatorCommand::MarkPreparing {
                    txn_id: txn,
                    tablet_id: tid(1),
                    prepare_ts: ts(150),
                    observed: ts(160),
                },
                150,
            ),
        ))
        .unwrap();
        sink.apply(&applied(
            5,
            coordinator_envelope(
                [5u8; 16],
                CoordinatorCommand::Commit {
                    txn_id: txn,
                    commit_ts: ts(200),
                },
                200,
            ),
        ))
        .unwrap();
        let stored = sink.record(&txn).unwrap();
        assert_eq!(
            stored.state,
            DistributedTxnState::Committed { commit_ts: ts(200) }
        );
        assert_eq!(stored.prepare_ts[&tid(1)], ts(150));
        assert_eq!(stored.max_observed, ts(160));
        // A terminal state is final: a late abort is journaled, never applied.
        sink.apply(&applied(
            6,
            coordinator_envelope(
                [6u8; 16],
                CoordinatorCommand::Abort {
                    txn_id: txn,
                    reason: AbortReason::RolledBack,
                },
                300,
            ),
        ))
        .unwrap();
        assert_eq!(
            sink.record(&txn).unwrap().state,
            DistributedTxnState::Committed { commit_ts: ts(200) }
        );
        assert!(matches!(
            sink.state().rejections[1].reason,
            StatusRejectionReason::DecisionFinal { .. }
        ));
        // Transitions for unknown transactions are journaled.
        sink.apply(&applied(
            7,
            coordinator_envelope(
                [7u8; 16],
                CoordinatorCommand::Commit {
                    txn_id: xid(99),
                    commit_ts: ts(400),
                },
                400,
            ),
        ))
        .unwrap();
        assert!(matches!(
            sink.state().rejections[2].reason,
            StatusRejectionReason::UnknownTxn(_)
        ));
        // A Begin with a non-Pending record fails closed.
        let mut bad = record(xid(5), vec![participant(91, 1)]);
        bad.state = DistributedTxnState::Committed { commit_ts: ts(1) };
        assert!(sink
            .apply(&applied(
                8,
                coordinator_envelope([8u8; 16], CoordinatorCommand::Begin { record: bad }, 500,)
            ))
            .is_err());
    }

    #[test]
    fn status_sink_rejects_commit_ts_not_after_max_observed() {
        // Review D2: coordinator apply re-validates commit_ts.
        let tmp = tempfile::tempdir().unwrap();
        let mut sink = TxnStatusApplySink::open(tmp.path()).unwrap();
        let txn = xid(42);
        sink.apply(&applied(
            1,
            coordinator_envelope(
                [1u8; 16],
                CoordinatorCommand::Begin {
                    record: record(txn, vec![participant(91, 1)]),
                },
                100,
            ),
        ))
        .unwrap();
        sink.apply(&applied(
            2,
            coordinator_envelope(
                [2u8; 16],
                CoordinatorCommand::MarkPreparing {
                    txn_id: txn,
                    tablet_id: tid(1),
                    prepare_ts: ts(150),
                    observed: ts(160),
                },
                150,
            ),
        ))
        .unwrap();
        // commit_ts == max_observed (160) must fail; must be strictly greater.
        sink.apply(&applied(
            3,
            coordinator_envelope(
                [3u8; 16],
                CoordinatorCommand::Commit {
                    txn_id: txn,
                    commit_ts: ts(160),
                },
                160,
            ),
        ))
        .unwrap();
        assert!(
            matches!(
                sink.state().rejections.back().map(|r| &r.reason),
                Some(StatusRejectionReason::InvalidCommitTs { .. })
            ),
            "expected InvalidCommitTs, got {:?}",
            sink.state().rejections
        );
        // Transaction remains non-terminal (still Preparing).
        assert!(matches!(
            sink.record(&txn).unwrap().state,
            DistributedTxnState::Preparing
        ));
        // Strictly greater commit_ts succeeds.
        sink.apply(&applied(
            4,
            coordinator_envelope(
                [4u8; 16],
                CoordinatorCommand::Commit {
                    txn_id: txn,
                    commit_ts: ts(161),
                },
                161,
            ),
        ))
        .unwrap();
        assert_eq!(
            sink.record(&txn).unwrap().state,
            DistributedTxnState::Committed { commit_ts: ts(161) }
        );
    }

    #[test]
    fn status_sink_heartbeat_extends_expiry_only_forward() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sink = TxnStatusApplySink::open(tmp.path()).unwrap();
        let txn = xid(3);
        sink.apply(&applied(
            1,
            coordinator_envelope(
                [1u8; 16],
                CoordinatorCommand::Begin {
                    record: record(txn, vec![participant(91, 1)]),
                },
                100,
            ),
        ))
        .unwrap();
        sink.apply(&applied(
            2,
            coordinator_envelope(
                [2u8; 16],
                CoordinatorCommand::Heartbeat {
                    txn_id: txn,
                    heartbeat: ts(1_500),
                    expiry: ts(2_500),
                },
                150,
            ),
        ))
        .unwrap();
        assert_eq!(sink.record(&txn).unwrap().heartbeat, ts(1_500));
        assert_eq!(sink.record(&txn).unwrap().expiry, ts(2_500));
        // A stale heartbeat does not move the record backward.
        sink.apply(&applied(
            3,
            coordinator_envelope(
                [3u8; 16],
                CoordinatorCommand::Heartbeat {
                    txn_id: txn,
                    heartbeat: ts(1_200),
                    expiry: ts(2_200),
                },
                151,
            ),
        ))
        .unwrap();
        assert_eq!(sink.record(&txn).unwrap().heartbeat, ts(1_500));
    }

    #[test]
    fn status_sink_checkpoint_survives_reopen_and_replay_is_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let txn = xid(4);
        {
            let mut sink = TxnStatusApplySink::open(tmp.path()).unwrap();
            sink.apply(&applied(
                1,
                coordinator_envelope(
                    [1u8; 16],
                    CoordinatorCommand::Begin {
                        record: record(txn, vec![participant(91, 1)]),
                    },
                    100,
                ),
            ))
            .unwrap();
        }
        let mut sink = TxnStatusApplySink::open(tmp.path()).unwrap();
        assert!(sink.record(&txn).is_some());
        // Crash-window replay of the persisted position is skipped.
        sink.apply(&applied(
            1,
            coordinator_envelope(
                [9u8; 16],
                CoordinatorCommand::Abort {
                    txn_id: txn,
                    reason: AbortReason::RolledBack,
                },
                101,
            ),
        ))
        .unwrap();
        assert_eq!(
            sink.record(&txn).unwrap().state,
            DistributedTxnState::Pending
        );
        // A misrouted catalog command fails closed.
        let foreign = ReplicatedCommand::new(
            CommandKind::Catalog,
            CommandEnvelope::new(COMMAND_TYPE_DIST_TXN_COORDINATOR, [5u8; 16], vec![1]),
            ts(600),
        );
        assert!(sink.apply(&applied(2, foreign)).is_err());
        // A transaction envelope with a foreign command type fails closed.
        let wrong_type = ReplicatedCommand::new(
            CommandKind::Transaction,
            CommandEnvelope::new(1, [6u8; 16], vec![1]),
            ts(601),
        );
        assert!(sink.apply(&applied(2, wrong_type)).is_err());
    }

    // -- participant intent sink semantics -----------------------------------

    fn persist(
        txn: u8,
        schema: u64,
        authz: u64,
        prepare_micros: u64,
        keys: &[&[u8]],
    ) -> IntentCommand {
        IntentCommand::PersistIntents {
            txn_id: xid(txn),
            expected_schema_version: SchemaVersion::new(schema),
            expected_authz_version: authz,
            prepare_ts: ts(prepare_micros),
            intents: keys
                .iter()
                .map(|key| intent(txn, key, prepare_micros))
                .collect(),
        }
    }

    #[test]
    fn intent_sink_validates_versions_and_conflicts() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sink = IntentApplySink::open(tmp.path()).unwrap();
        sink.apply(&applied(
            1,
            intent_envelope(
                [1u8; 16],
                IntentCommand::SetTabletVersions {
                    schema_version: SchemaVersion::new(7),
                    authz_version: 3,
                },
                100,
            ),
        ))
        .unwrap();
        // Stale schema version: journaled rejection.
        sink.apply(&applied(
            2,
            intent_envelope([2u8; 16], persist(1, 0, 3, 110, &[b"k1"]), 110),
        ))
        .unwrap();
        assert!(matches!(
            sink.state().rejections[0].reason,
            PrepareRejectionReason::StaleSchemaVersion { .. }
        ));
        // Stale authorization version: journaled rejection.
        sink.apply(&applied(
            3,
            intent_envelope([3u8; 16], persist(1, 7, 0, 111, &[b"k1"]), 111),
        ))
        .unwrap();
        assert!(matches!(
            sink.state().rejections[1].reason,
            PrepareRejectionReason::StaleAuthzVersion { .. }
        ));
        // Current versions: persisted.
        sink.apply(&applied(
            4,
            intent_envelope([4u8; 16], persist(1, 7, 3, 112, &[b"k1", b"k2"]), 112),
        ))
        .unwrap();
        assert!(sink.txn(&xid(1)).is_some());
        // A conflicting key from another transaction: journaled.
        sink.apply(&applied(
            5,
            intent_envelope([5u8; 16], persist(2, 7, 3, 113, &[b"k1"]), 113),
        ))
        .unwrap();
        assert!(matches!(
            sink.state().rejections[2].reason,
            PrepareRejectionReason::KeyConflict { holder, .. } if holder == xid(1)
        ));
        assert!(sink.txn(&xid(2)).is_none());
        // A disjoint key from the same other transaction: persisted.
        sink.apply(&applied(
            6,
            intent_envelope([6u8; 16], persist(2, 7, 3, 114, &[b"k9"]), 114),
        ))
        .unwrap();
        assert!(sink.txn(&xid(2)).is_some());
    }

    #[test]
    fn intent_sink_replay_keeps_the_original_prepare_timestamp() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sink = IntentApplySink::open(tmp.path()).unwrap();
        sink.apply(&applied(
            1,
            intent_envelope([1u8; 16], persist(1, 0, 0, 100, &[b"k1"]), 100),
        ))
        .unwrap();
        // Same write set, freshly stamped timestamp (a client retry after an
        // ambiguous failure beyond the raft idempotency window): no-op, the
        // original prepare timestamp stands.
        sink.apply(&applied(
            2,
            intent_envelope([2u8; 16], persist(1, 0, 0, 999, &[b"k1"]), 999),
        ))
        .unwrap();
        assert_eq!(sink.txn(&xid(1)).unwrap().prepare_ts, ts(100));
        assert!(sink.state().rejections.is_empty());
        // A different write set under the same transaction id: journaled.
        sink.apply(&applied(
            3,
            intent_envelope([3u8; 16], persist(1, 0, 0, 1000, &[b"k2"]), 1000),
        ))
        .unwrap();
        assert!(matches!(
            sink.state().rejections[0].reason,
            PrepareRejectionReason::PayloadMismatch
        ));
    }

    #[test]
    fn intent_sink_resolution_commits_visible_or_removes_and_never_conflicts() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sink = IntentApplySink::open(tmp.path()).unwrap();
        sink.apply(&applied(
            1,
            intent_envelope([1u8; 16], persist(1, 0, 0, 100, &[b"k1", b"k2"]), 100),
        ))
        .unwrap();
        // Commit resolution: intents become visible at commit_ts.
        sink.apply(&applied(
            2,
            intent_envelope(
                [2u8; 16],
                IntentCommand::Resolve {
                    txn_id: xid(1),
                    decision: TxnDecision::Committed { commit_ts: ts(200) },
                },
                200,
            ),
        ))
        .unwrap();
        let writes = sink.state().committed_writes.clone();
        assert_eq!(writes.len(), 2);
        assert!(writes.iter().all(|write| write.commit_ts == ts(200)));
        assert!(sink.txn(&xid(1)).unwrap().intents.is_empty());
        // Same decision replayed: no-op, no double visibility.
        sink.apply(&applied(
            3,
            intent_envelope(
                [3u8; 16],
                IntentCommand::Resolve {
                    txn_id: xid(1),
                    decision: TxnDecision::Committed { commit_ts: ts(200) },
                },
                201,
            ),
        ))
        .unwrap();
        assert_eq!(sink.state().committed_writes.len(), 2);
        // A conflicting decision for a resolved transaction fails closed.
        assert!(sink
            .apply(&applied(
                4,
                intent_envelope(
                    [4u8; 16],
                    IntentCommand::Resolve {
                        txn_id: xid(1),
                        decision: TxnDecision::Aborted {
                            reason: AbortReason::RolledBack,
                        },
                    },
                    202,
                ),
            ))
            .is_err());
        // A prepare losing the race against the resolution is refused.
        sink.apply(&applied(
            5,
            intent_envelope([5u8; 16], persist(1, 0, 0, 300, &[b"k3"]), 300),
        ))
        .unwrap();
        assert!(matches!(
            sink.state().rejections[0].reason,
            PrepareRejectionReason::AlreadyResolved { .. }
        ));
        // Abort resolution removes intents; the keys are free afterwards.
        sink.apply(&applied(
            6,
            intent_envelope([6u8; 16], persist(2, 0, 0, 400, &[b"k1"]), 400),
        ))
        .unwrap();
        sink.apply(&applied(
            7,
            intent_envelope(
                [7u8; 16],
                IntentCommand::Resolve {
                    txn_id: xid(2),
                    decision: TxnDecision::Aborted {
                        reason: AbortReason::Conflict("test".to_owned()),
                    },
                },
                401,
            ),
        ))
        .unwrap();
        assert!(sink.txn(&xid(2)).unwrap().intents.is_empty());
        assert_eq!(sink.state().committed_writes.len(), 2);
        sink.apply(&applied(
            8,
            intent_envelope([8u8; 16], persist(3, 0, 0, 500, &[b"k1"]), 500),
        ))
        .unwrap();
        assert!(sink.txn(&xid(3)).unwrap().resolution.is_none());
        // Resolving an unknown transaction tombstones it (late prepares lose).
        sink.apply(&applied(
            9,
            intent_envelope(
                [9u8; 16],
                IntentCommand::Resolve {
                    txn_id: xid(9),
                    decision: TxnDecision::Aborted {
                        reason: AbortReason::Cancelled("expired".to_owned()),
                    },
                },
                600,
            ),
        ))
        .unwrap();
        sink.apply(&applied(
            10,
            intent_envelope([10u8; 16], persist(9, 0, 0, 700, &[b"k7"]), 700),
        ))
        .unwrap();
        assert!(matches!(
            sink.state().rejections[1].reason,
            PrepareRejectionReason::AlreadyResolved { .. }
        ));
        assert!(sink.unresolved_txn_ids().contains(&xid(3)));
        assert!(!sink.unresolved_txn_ids().contains(&xid(2)));
    }

    #[test]
    fn intent_sink_checkpoint_survives_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut sink = IntentApplySink::open(tmp.path()).unwrap();
            sink.apply(&applied(
                1,
                intent_envelope([1u8; 16], persist(1, 0, 0, 100, &[b"k1"]), 100),
            ))
            .unwrap();
        }
        let sink = IntentApplySink::open(tmp.path()).unwrap();
        assert_eq!(sink.txn(&xid(1)).unwrap().prepare_ts, ts(100));
        assert_eq!(sink.applied_position(), LogPosition { term: 1, index: 1 });
    }

    // -- 3-node in-memory cell -----------------------------------------------

    const LEADER_TIMEOUT: Duration = Duration::from_secs(15);
    const STATUS_GROUP: u8 = 90;
    const P1_GROUP: u8 = 91;
    const P2_GROUP: u8 = 92;
    const TEN_MINUTES: Duration = Duration::from_secs(600);

    fn raft_id(group: u8, member: u8) -> RaftNodeId {
        u64::from(group) * 100 + u64::from(member)
    }

    fn fast_group_config(dir: &Path, group: u8, member: u8) -> GroupConfig {
        let mut config =
            GroupConfig::new("dist-txn-test", raft_id(group, member), dir.to_path_buf());
        config.heartbeat_interval = Duration::from_millis(50);
        config.election_timeout_min = Duration::from_millis(150);
        config.election_timeout_max = Duration::from_millis(300);
        config.install_snapshot_timeout = Duration::from_millis(1_000);
        config
    }

    struct Cell {
        tmp: tempfile::TempDir,
        transport: Arc<InMemoryTransport>,
        status: Vec<TxnStatusGroup<InMemoryTransport>>,
        participants: BTreeMap<RaftGroupId, Vec<IntentGroup<InMemoryTransport>>>,
    }

    impl Cell {
        fn p1(&self) -> &[IntentGroup<InMemoryTransport>] {
            &self.participants[&gid(P1_GROUP)]
        }

        fn p2(&self) -> &[IntentGroup<InMemoryTransport>] {
            &self.participants[&gid(P2_GROUP)]
        }

        async fn shutdown(self) {
            for member in &self.status {
                let _ = member.shutdown().await;
            }
            for members in self.participants.values() {
                for member in members {
                    let _ = member.shutdown().await;
                }
            }
        }
    }

    async fn boot_status_member(
        cell_dir: &Path,
        member: u8,
        transport: &Arc<InMemoryTransport>,
    ) -> TxnStatusGroup<InMemoryTransport> {
        TxnStatusGroup::create(
            fast_group_config(
                &cell_dir.join(format!("status-{member}")),
                STATUS_GROUP,
                member,
            ),
            gid(STATUS_GROUP),
            transport.clone(),
        )
        .await
        .unwrap()
    }

    async fn boot_intent_member(
        cell_dir: &Path,
        group: u8,
        member: u8,
        transport: &Arc<InMemoryTransport>,
    ) -> IntentGroup<InMemoryTransport> {
        IntentGroup::create(
            fast_group_config(
                &cell_dir.join(format!("intent-{group}-{member}")),
                group,
                member,
            ),
            gid(group),
            transport.clone(),
        )
        .await
        .unwrap()
    }

    fn member_addresses(group: u8) -> Vec<(RaftNodeId, String)> {
        (1..=3_u8)
            .map(|member| {
                (
                    raft_id(group, member),
                    format!(
                        "127.0.0.1:{}",
                        9_000 + u16::from(group) * 10 + u16::from(member)
                    ),
                )
            })
            .collect()
    }

    async fn wait_leader_among<M: GroupMember>(members: &[&M]) -> RaftNodeId {
        let allowed: std::collections::BTreeSet<RaftNodeId> =
            members.iter().map(|member| member.node_id()).collect();
        let deadline = Instant::now() + LEADER_TIMEOUT;
        loop {
            let mut leaders = std::collections::BTreeSet::new();
            let mut seen = 0_usize;
            for member in members {
                if let Some(leader) = member.current_leader() {
                    leaders.insert(leader);
                    seen += 1;
                }
            }
            if seen == members.len() && leaders.len() == 1 {
                let leader = *leaders.iter().next().expect("one leader");
                if allowed.contains(&leader) {
                    return leader;
                }
            }
            assert!(
                Instant::now() < deadline,
                "no consensus leader (saw {leaders:?})"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn leader_index<M: GroupMember>(members: &[M]) -> usize {
        members
            .iter()
            .position(|member| member.current_leader() == Some(member.node_id()))
            .expect("a settled group has a leader member")
    }

    async fn wait_until<F, Fut>(what: &str, mut predicate: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = Instant::now() + LEADER_TIMEOUT;
        loop {
            if predicate().await {
                return;
            }
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Waits until every replica of every group in the cell reports the same
    /// applied sink state (no new commands land after the drivers return, so
    /// full equality is reachable).
    async fn wait_cell_converged(cell: &Cell) {
        wait_until("status convergence", || async {
            let reference = cell.status[0].state();
            cell.status.iter().all(|member| member.state() == reference)
        })
        .await;
        for group in [P1_GROUP, P2_GROUP] {
            let members = &cell.participants[&gid(group)];
            wait_until("participant convergence", || async {
                let reference = members[0].state();
                members.iter().all(|member| member.state() == reference)
            })
            .await;
        }
    }

    async fn boot_cell() -> Cell {
        let tmp = tempfile::tempdir().unwrap();
        let transport = Arc::new(InMemoryTransport::new());
        let mut status = Vec::new();
        for member in 1..=3_u8 {
            status.push(boot_status_member(tmp.path(), member, &transport).await);
        }
        status[0]
            .bootstrap(&member_addresses(STATUS_GROUP))
            .await
            .unwrap();
        let mut participants: BTreeMap<RaftGroupId, Vec<IntentGroup<InMemoryTransport>>> =
            BTreeMap::new();
        for group in [P1_GROUP, P2_GROUP] {
            let mut members = Vec::new();
            for member in 1..=3_u8 {
                members.push(boot_intent_member(tmp.path(), group, member, &transport).await);
            }
            members[0]
                .bootstrap(&member_addresses(group))
                .await
                .unwrap();
            participants.insert(gid(group), members);
        }
        wait_leader_among(&status.iter().collect::<Vec<_>>()).await;
        for group in [P1_GROUP, P2_GROUP] {
            wait_leader_among(&participants[&gid(group)].iter().collect::<Vec<_>>()).await;
        }
        Cell {
            tmp,
            transport,
            status,
            participants,
        }
    }

    fn driver_config(pending_timeout: Duration) -> DistTxnConfig {
        let mut status_partitions = BTreeMap::new();
        status_partitions.insert(
            0,
            TxnStatusPartition {
                partition_id: 0,
                home_raft_group: gid(STATUS_GROUP),
            },
        );
        DistTxnConfig {
            selection: CoordinatorSelection::TxnIdDerived,
            status_partitions,
            pending_timeout,
            ..DistTxnConfig::default()
        }
    }

    fn participant_writes(group: u8, tablet: u8, keys: &[&[u8]]) -> ParticipantWrites {
        ParticipantWrites {
            participant: participant(group, tablet),
            expected_schema_version: SchemaVersion::ZERO,
            expected_authz_version: 0,
            intents: keys
                .iter()
                .map(|key| WriteIntent {
                    txn_id: TransactionId::ZERO,
                    key: key.to_vec(),
                    value_ref: format!("staged-{}", String::from_utf8_lossy(key)).into_bytes(),
                    prepare_ts: HlcTimestamp::ZERO,
                })
                .collect(),
        }
    }

    fn commit_request(txn: u8, key: u8, writes: Vec<ParticipantWrites>) -> CommitRequest {
        CommitRequest {
            txn_id: xid(txn),
            idempotency_key: [key; 16],
            writes,
            observed: Vec::new(),
            first_write_tablet: None,
        }
    }

    // -- integration: commit flows -------------------------------------------

    #[tokio::test]
    async fn single_participant_commit_fast_path() {
        let cell = boot_cell().await;
        let driver = DistTxnDriver::new(driver_config(TEN_MINUTES));
        let control = ExecutionControl::default();
        let request = commit_request(1, 1, vec![participant_writes(P1_GROUP, 1, &[b"alpha"])]);
        let outcome = driver
            .commit(&cell.status, &cell.participants, request, &control)
            .await
            .unwrap();
        wait_cell_converged(&cell).await;
        assert_eq!(outcome.txn_id, xid(1));
        assert_eq!(outcome.durability, DurabilityLevel::Quorum);
        assert_eq!(outcome.participants, vec![participant(P1_GROUP, 1)]);
        // The record is committed; the fast path recorded no prepare marks.
        let stored = cell.status[0].record(&xid(1)).unwrap();
        assert_eq!(
            stored.state,
            DistributedTxnState::Committed {
                commit_ts: outcome.commit_ts
            }
        );
        assert!(stored.prepare_ts.is_empty());
        // The intents resolved visible at commit_ts, strictly after prepare.
        let participant_txn = cell.p1()[0].txn(&xid(1)).unwrap();
        assert_eq!(
            participant_txn.resolution,
            Some(TxnDecision::Committed {
                commit_ts: outcome.commit_ts
            })
        );
        assert!(participant_txn.prepare_ts > HlcTimestamp::ZERO);
        assert!(outcome.commit_ts > participant_txn.prepare_ts);
        let writes = cell.p1()[0].committed_writes();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].key, b"alpha");
        assert_eq!(writes[0].commit_ts, outcome.commit_ts);
        assert!(cell.p1()[0].unresolved_txn_ids().is_empty());
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn two_participant_commit_is_atomic_and_commit_ts_exceeds_all_prepare_ts() {
        let cell = boot_cell().await;
        let driver = DistTxnDriver::new(driver_config(TEN_MINUTES));
        let control = ExecutionControl::default();
        let mut request = commit_request(
            2,
            2,
            vec![
                participant_writes(P1_GROUP, 1, &[b"a"]),
                participant_writes(P2_GROUP, 2, &[b"b", b"c"]),
            ],
        );
        request.observed = vec![ts(42)];
        let outcome = driver
            .commit(&cell.status, &cell.participants, request, &control)
            .await
            .unwrap();
        wait_cell_converged(&cell).await;
        // General path: both prepares recorded; commit_ts strictly greater
        // than every observed timestamp (prepare marks and client-observed).
        let stored = cell.status[0].record(&xid(2)).unwrap();
        assert_eq!(stored.prepare_ts.len(), 2);
        assert_eq!(
            stored.state,
            DistributedTxnState::Committed {
                commit_ts: outcome.commit_ts
            }
        );
        for prepare_ts in stored.prepare_ts.values() {
            assert!(outcome.commit_ts > *prepare_ts);
        }
        assert!(outcome.commit_ts > ts(42));
        assert!(stored.max_observed >= ts(42));
        // Atomicity: one commit_ts, visible on both participants, no
        // unresolved intents anywhere.
        for (group, keys) in [
            (P1_GROUP, vec![b"a".to_vec()]),
            (P2_GROUP, vec![b"b".to_vec(), b"c".to_vec()]),
        ] {
            let members = &cell.participants[&gid(group)];
            let txn = members[0].txn(&xid(2)).unwrap();
            assert_eq!(
                txn.resolution,
                Some(TxnDecision::Committed {
                    commit_ts: outcome.commit_ts
                })
            );
            assert!(txn.prepare_ts < outcome.commit_ts);
            assert!(members[0].unresolved_txn_ids().is_empty());
            let writes = members[0].committed_writes();
            let visible: Vec<&[u8]> = writes
                .iter()
                .filter(|write| write.txn_id == xid(2))
                .map(|write| write.key.as_slice())
                .collect();
            assert_eq!(visible.len(), keys.len());
            for key in &keys {
                assert!(visible.contains(&key.as_slice()));
            }
        }
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn prepare_conflict_aborts_everywhere_without_intent_leaks() {
        let cell = boot_cell().await;
        let driver = DistTxnDriver::new(driver_config(TEN_MINUTES));
        let control = ExecutionControl::default();
        // Transaction A holds an unresolved intent on the shared key at P1
        // (driven step by step so it stays in flight).
        let request_a = commit_request(10, 10, vec![participant_writes(P1_GROUP, 1, &[b"shared"])]);
        driver
            .begin(&cell.status, &request_a, &control)
            .await
            .unwrap();
        driver
            .prepare_participant(cell.p1(), &request_a.txn_id, &request_a.writes[0], &control)
            .await
            .unwrap();
        // Transaction B prepares P2 first, then conflicts on the shared key
        // at P1: the driver persists Aborted and removes B's intents.
        let request_b = commit_request(
            11,
            11,
            vec![
                participant_writes(P2_GROUP, 2, &[b"b-key"]),
                participant_writes(P1_GROUP, 1, &[b"shared"]),
            ],
        );
        let error = driver
            .commit(&cell.status, &cell.participants, request_b, &control)
            .await
            .unwrap_err();
        wait_cell_converged(&cell).await;
        assert!(matches!(
            error,
            DistTxnError::Aborted(AbortReason::Conflict(_))
        ));
        let record_b = cell.status[0].record(&xid(11)).unwrap();
        assert!(matches!(
            record_b.state,
            DistributedTxnState::Aborted {
                reason: AbortReason::Conflict(_)
            }
        ));
        // No intent leaks: B left nothing unresolved at either participant,
        // and nothing of B is visible.
        assert!(
            cell.p1()[0].txn(&xid(11)).is_none()
                || cell.p1()[0].txn(&xid(11)).unwrap().resolution.is_some()
        );
        assert!(cell.p2()[0].unresolved_txn_ids().is_empty());
        assert!(cell.p2()[0]
            .committed_writes()
            .iter()
            .all(|write| write.txn_id != xid(11)));
        // A is untouched: still unresolved, not visible.
        let txn_a = cell.p1()[0].txn(&xid(10)).unwrap();
        assert!(txn_a.resolution.is_none());
        assert!(cell.p1()[0].committed_writes().is_empty());
        // Cleanup: explicit abort removes A's intents.
        let final_state = driver
            .abort(
                &cell.status,
                &cell.participants,
                &xid(10),
                AbortReason::RolledBack,
                &control,
            )
            .await
            .unwrap();
        assert!(matches!(final_state, DistributedTxnState::Aborted { .. }));
        wait_until("abort resolution", || async {
            cell.p1()[0].unresolved_txn_ids().is_empty()
        })
        .await;
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn coordinator_leader_kill_between_prepare_and_decision_recovers() {
        let mut cell = boot_cell().await;
        let driver = DistTxnDriver::new(driver_config(TEN_MINUTES));
        let control = ExecutionControl::default();
        let request = commit_request(
            20,
            20,
            vec![
                participant_writes(P1_GROUP, 1, &[b"k1"]),
                participant_writes(P2_GROUP, 2, &[b"k2"]),
            ],
        );
        driver
            .begin(&cell.status, &request, &control)
            .await
            .unwrap();
        let token1 = driver
            .prepare_participant(cell.p1(), &request.txn_id, &request.writes[0], &control)
            .await
            .unwrap();
        let token2 = driver
            .prepare_participant(cell.p2(), &request.txn_id, &request.writes[1], &control)
            .await
            .unwrap();
        // Kill the coordinator-group leader between prepare and decision.
        let leader = leader_index(&cell.status);
        let victim = cell.status.remove(leader);
        victim.crash().await;
        let survivor_refs: Vec<&TxnStatusGroup<InMemoryTransport>> = cell.status.iter().collect();
        wait_leader_among(&survivor_refs).await;
        // The new leader continues: recovery drives the decision from the
        // replicated record and the durable intents.
        let recovery_config = DistTxnConfig {
            node_tiebreaker: 99,
            ..driver_config(TEN_MINUTES)
        };
        let recovery = DistTxnDriver::new(recovery_config);
        let now = recovery.now().unwrap();
        let outcome = recovery
            .recover(
                &cell.status,
                &cell.participants,
                &request.txn_id,
                now,
                &control,
            )
            .await
            .unwrap();
        let commit_ts = match outcome {
            RecoveryOutcome::Decided(DistributedTxnState::Committed { commit_ts }) => commit_ts,
            other => panic!("expected a committed recovery, got {other:?}"),
        };
        assert!(commit_ts > token1.prepare_ts);
        assert!(commit_ts > token2.prepare_ts);
        // Resolution is identical on all participants.
        for group in [P1_GROUP, P2_GROUP] {
            let members = &cell.participants[&gid(group)];
            wait_until("participant resolution", || async {
                members[0]
                    .txn(&request.txn_id)
                    .is_some_and(|txn| txn.resolution.is_some())
            })
            .await;
            let txn = members[0].txn(&request.txn_id).unwrap();
            assert_eq!(txn.resolution, Some(TxnDecision::Committed { commit_ts }));
            assert!(members[0]
                .committed_writes()
                .iter()
                .any(|write| write.txn_id == request.txn_id && write.commit_ts == commit_ts));
        }
        // The killed member heals from its durable state and converges on
        // the committed record.
        let healed = boot_status_member(
            cell.tmp.path(),
            u8::try_from(leader + 1).unwrap(),
            &cell.transport,
        )
        .await;
        wait_until("healed status member", || async {
            healed.record(&request.txn_id).is_some_and(|record| {
                record.state == (DistributedTxnState::Committed { commit_ts })
            })
        })
        .await;
        cell.status.push(healed);
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn participant_kill_post_prepare_heals_and_resolves() {
        let mut cell = boot_cell().await;
        let driver = DistTxnDriver::new(driver_config(TEN_MINUTES));
        let control = ExecutionControl::default();
        let request = commit_request(
            21,
            21,
            vec![
                participant_writes(P1_GROUP, 1, &[b"p1-key"]),
                participant_writes(P2_GROUP, 2, &[b"p2-key"]),
            ],
        );
        driver
            .begin(&cell.status, &request, &control)
            .await
            .unwrap();
        let token1 = driver
            .prepare_participant(cell.p1(), &request.txn_id, &request.writes[0], &control)
            .await
            .unwrap();
        let token2 = driver
            .prepare_participant(cell.p2(), &request.txn_id, &request.writes[1], &control)
            .await
            .unwrap();
        // Kill P1's leader after it durably prepared.
        let p1_leader = leader_index(cell.p1());
        let victim = cell
            .participants
            .get_mut(&gid(P1_GROUP))
            .unwrap()
            .remove(p1_leader);
        victim.crash().await;
        let survivor_refs: Vec<&IntentGroup<InMemoryTransport>> = cell.p1().iter().collect();
        wait_leader_among(&survivor_refs).await;
        // The decision lands (the coordinator group is intact) and the
        // resolve broadcast reaches P1 through its survivors.
        let outcome = driver
            .decide_commit(
                &cell.status,
                &cell.participants,
                &request.txn_id,
                &[token1, token2],
                &request.observed,
                &control,
            )
            .await
            .unwrap();
        wait_until("p1 survivor resolution", || async {
            cell.p1().iter().all(|member| {
                member
                    .committed_writes()
                    .iter()
                    .any(|write| write.txn_id == request.txn_id)
            })
        })
        .await;
        // Heal the killed member: it catches up and converges on the
        // resolved state.
        let healed = boot_intent_member(
            cell.tmp.path(),
            P1_GROUP,
            u8::try_from(p1_leader + 1).unwrap(),
            &cell.transport,
        )
        .await;
        let expected = cell.p1()[0].state();
        wait_until("healed p1 member", || async { healed.state() == expected }).await;
        assert!(healed
            .committed_writes()
            .iter()
            .any(|write| write.txn_id == request.txn_id && write.commit_ts == outcome.commit_ts));
        cell.participants
            .get_mut(&gid(P1_GROUP))
            .unwrap()
            .push(healed);
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn expired_pending_is_pushed_to_abort_only_under_the_timeout_rule() {
        let cell = boot_cell().await;
        let driver = DistTxnDriver::new(driver_config(TEN_MINUTES));
        let control = ExecutionControl::default();
        let request = commit_request(30, 30, vec![participant_writes(P1_GROUP, 1, &[b"e-key"])]);
        let begun = driver
            .begin(&cell.status, &request, &control)
            .await
            .unwrap();
        let pusher_config = DistTxnConfig {
            node_tiebreaker: 77,
            ..driver_config(TEN_MINUTES)
        };
        let pusher = DistTxnDriver::new(pusher_config);
        // A heartbeat renews the lease (one clock strictly advances, so the
        // refreshed heartbeat and expiry move forward deterministically).
        let renewed = driver
            .heartbeat(&cell.status, begun.coordinator, &request.txn_id, &control)
            .await
            .unwrap();
        assert!(renewed.heartbeat > begun.heartbeat);
        assert!(renewed.expiry > begun.expiry);
        wait_cell_converged(&cell).await;
        // Before expiry: never pushed on suspicion.
        let outcome = pusher
            .push_expired(
                &cell.status,
                &cell.participants,
                &request.txn_id,
                one_before(renewed.expiry),
                &control,
            )
            .await
            .unwrap();
        assert_eq!(outcome, PushOutcome::NotExpired);
        assert_eq!(
            cell.status[0].record(&request.txn_id).unwrap().state,
            DistributedTxnState::Pending
        );
        // At expiry: a third party pushes the transaction to Aborted through
        // the replicated record.
        let outcome = pusher
            .push_expired(
                &cell.status,
                &cell.participants,
                &request.txn_id,
                renewed.expiry,
                &control,
            )
            .await
            .unwrap();
        match outcome {
            PushOutcome::Pushed(DistributedTxnState::Aborted {
                reason: AbortReason::Cancelled(_),
            }) => {}
            other => panic!("expected a pushed abort, got {other:?}"),
        }
        wait_cell_converged(&cell).await;
        let stored = cell.status[0].record(&request.txn_id).unwrap();
        assert!(matches!(
            stored.state,
            DistributedTxnState::Aborted {
                reason: AbortReason::Cancelled(_)
            }
        ));
        // The abort decision was broadcast: P1 carries the tombstone, no
        // committed writes, nothing unresolved.
        wait_until("abort tombstone", || async {
            cell.p1()[0]
                .txn(&request.txn_id)
                .is_some_and(|txn| txn.resolution.is_some())
        })
        .await;
        assert!(cell.p1()[0].committed_writes().is_empty());
        assert!(cell.p1()[0].unresolved_txn_ids().is_empty());
        // A second push is a terminal no-op.
        let again = pusher
            .push_expired(
                &cell.status,
                &cell.participants,
                &request.txn_id,
                renewed.expiry,
                &control,
            )
            .await
            .unwrap();
        assert!(matches!(again, PushOutcome::Terminal(_)));
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn duplicate_client_retry_yields_one_outcome() {
        let cell = boot_cell().await;
        let driver = DistTxnDriver::new(driver_config(TEN_MINUTES));
        let control = ExecutionControl::default();
        let request = commit_request(
            40,
            40,
            vec![
                participant_writes(P1_GROUP, 1, &[b"r1"]),
                participant_writes(P2_GROUP, 2, &[b"r2"]),
            ],
        );
        let first = driver
            .commit(&cell.status, &cell.participants, request.clone(), &control)
            .await
            .unwrap();
        // The ambiguous-failure retry: same transaction id, same idempotency
        // key, same write set — the original outcome replays.
        let second = driver
            .commit(&cell.status, &cell.participants, request, &control)
            .await
            .unwrap();
        wait_cell_converged(&cell).await;
        assert_eq!(first, second);
        // One record, one intent set per participant, no double visibility.
        assert_eq!(cell.status[0].state().records.len(), 1);
        for group in [P1_GROUP, P2_GROUP] {
            let members = &cell.participants[&gid(group)];
            assert_eq!(members[0].state().txns.len(), 1);
            assert_eq!(
                members[0]
                    .committed_writes()
                    .iter()
                    .filter(|write| write.txn_id == xid(40))
                    .count(),
                1
            );
        }
        // The same transaction id under a different idempotency key
        // conflicts.
        let conflicting = commit_request(40, 41, vec![participant_writes(P1_GROUP, 1, &[b"r1"])]);
        let error = driver
            .commit(&cell.status, &cell.participants, conflicting, &control)
            .await
            .unwrap_err();
        assert!(matches!(error, DistTxnError::IdempotencyConflict(_)));
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn orphan_intent_sweep_honors_the_timeout_rules() {
        let cell = boot_cell().await;
        let control = ExecutionControl::default();
        let live_driver = DistTxnDriver::new(driver_config(TEN_MINUTES));
        let expiring_driver = DistTxnDriver::new(driver_config(Duration::from_millis(1)));
        // C: committed at the coordinator, but the resolve never reached P1
        // (decided with an empty participant map, so the broadcast defers).
        let request_c = commit_request(50, 50, vec![participant_writes(P1_GROUP, 1, &[b"c-key"])]);
        live_driver
            .begin(&cell.status, &request_c, &control)
            .await
            .unwrap();
        let token_c = live_driver
            .prepare_participant(cell.p1(), &request_c.txn_id, &request_c.writes[0], &control)
            .await
            .unwrap();
        let outcome_c = live_driver
            .decide_commit(
                &cell.status,
                &BTreeMap::<RaftGroupId, Vec<IntentGroup<InMemoryTransport>>>::new(),
                &request_c.txn_id,
                &[token_c],
                &[],
                &control,
            )
            .await
            .unwrap();
        // E: begun with a tiny lease, prepared at P1, never decided.
        let request_e = commit_request(51, 51, vec![participant_writes(P1_GROUP, 1, &[b"e-key"])]);
        expiring_driver
            .begin(&cell.status, &request_e, &control)
            .await
            .unwrap();
        expiring_driver
            .prepare_participant(cell.p1(), &request_e.txn_id, &request_e.writes[0], &control)
            .await
            .unwrap();
        // L: live lease, prepared at P1, never decided.
        let request_l = commit_request(52, 52, vec![participant_writes(P1_GROUP, 1, &[b"l-key"])]);
        live_driver
            .begin(&cell.status, &request_l, &control)
            .await
            .unwrap();
        live_driver
            .prepare_participant(cell.p1(), &request_l.txn_id, &request_l.writes[0], &control)
            .await
            .unwrap();
        // X: intents with no coordinator record at all.
        let request_x = commit_request(53, 53, vec![participant_writes(P1_GROUP, 1, &[b"x-key"])]);
        live_driver
            .prepare_participant(cell.p1(), &request_x.txn_id, &request_x.writes[0], &control)
            .await
            .unwrap();
        // Sweep five minutes in: every rule fires once.
        let now = later(live_driver.now().unwrap(), Duration::from_secs(300));
        let report = live_driver
            .sweep_orphans(&cell.status, cell.p1(), now, &control)
            .await
            .unwrap();
        wait_cell_converged(&cell).await;
        assert_eq!(
            report,
            SweepReport {
                resolved_committed: 1,
                resolved_aborted: 1,
                pushed: 1,
                in_flight: 1,
                unknown: 1,
            }
        );
        // C: resolved visible at its durable commit timestamp.
        assert!(cell.p1()[0]
            .committed_writes()
            .iter()
            .any(
                |write| write.txn_id == request_c.txn_id && write.commit_ts == outcome_c.commit_ts
            ));
        // E: pushed to abort, intents removed.
        assert!(matches!(
            cell.status[0].record(&request_e.txn_id).unwrap().state,
            DistributedTxnState::Aborted { .. }
        ));
        let txn_e = cell.p1()[0].txn(&request_e.txn_id).unwrap();
        assert!(txn_e.intents.is_empty());
        assert!(matches!(
            txn_e.resolution,
            Some(TxnDecision::Aborted { .. })
        ));
        // L: untouched (its timeout is honored).
        let txn_l = cell.p1()[0].txn(&request_l.txn_id).unwrap();
        assert!(txn_l.resolution.is_none());
        assert!(!txn_l.intents.is_empty());
        assert_eq!(
            cell.status[0].record(&request_l.txn_id).unwrap().state,
            DistributedTxnState::Pending
        );
        // X: untouched (recovery never resolves without the record).
        let txn_x = cell.p1()[0].txn(&request_x.txn_id).unwrap();
        assert!(txn_x.resolution.is_none());
        assert!(!txn_x.intents.is_empty());
        assert!(cell.status[0].record(&request_x.txn_id).is_none());
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn record_read_api_supports_linearizable_and_session_token_reads() {
        let cell = boot_cell().await;
        let driver = DistTxnDriver::new(driver_config(TEN_MINUTES));
        let control = ExecutionControl::default();
        let request = commit_request(60, 60, vec![participant_writes(P1_GROUP, 1, &[b"s-key"])]);
        driver
            .begin(&cell.status, &request, &control)
            .await
            .unwrap();
        // Linearizable read through the driver (any member answers).
        let record = driver
            .read_record(&cell.status, &request.txn_id, &control)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.txn_id, request.txn_id);
        // Session-token read-your-writes: propose directly, then read the
        // record back from ANOTHER member behind the session token.
        let leader = leader_index(&cell.status);
        let heartbeat = driver.now().unwrap();
        let expiry = later(heartbeat, TEN_MINUTES);
        let (receipt, rejection) = cell.status[leader]
            .propose(
                [0xAB; 16],
                CoordinatorCommand::Heartbeat {
                    txn_id: request.txn_id,
                    heartbeat,
                    expiry,
                },
                &control,
            )
            .await
            .unwrap();
        assert!(rejection.is_none());
        let token = cell.status[leader].session_token(&receipt);
        let follower = (leader + 1) % cell.status.len();
        let seen = cell.status[follower]
            .record_consistent(
                &request.txn_id,
                &ReadConsistency::ReadYourWrites { token },
                &control,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(seen.heartbeat, heartbeat);
        // A linearizable read on a non-leader is never served.
        let result = cell.status[follower]
            .record_consistent(&request.txn_id, &ReadConsistency::Linearizable, &control)
            .await;
        match cell.status[follower].group().metrics().current_leader {
            Some(leader_id) if leader_id == cell.status[follower].group().node_id() => {
                // This member became the leader: the read succeeds.
                assert!(result.is_ok());
            }
            _ => assert!(result.is_err()),
        }
        cell.shutdown().await;
    }

    // -- engine-backed tablet groups (Stage 3H MVCC binding) ------------------

    use mongreldb_consensus::engine_sink::{
        open_engine_sink, testing as engine_testing, EngineGroupConfig,
    };
    use mongreldb_types::ids::{ClusterId, DatabaseId, NodeId};

    const T1_GROUP: u8 = 81;
    const T2_GROUP: u8 = 82;

    struct EngineCell {
        tmp: tempfile::TempDir,
        transport: Arc<InMemoryTransport>,
        status: Vec<TxnStatusGroup<InMemoryTransport>>,
        tablets: BTreeMap<RaftGroupId, Vec<TabletTxnGroup<InMemoryTransport>>>,
    }

    impl EngineCell {
        fn t1(&self) -> &[TabletTxnGroup<InMemoryTransport>] {
            &self.tablets[&gid(T1_GROUP)]
        }

        fn t2(&self) -> &[TabletTxnGroup<InMemoryTransport>] {
            &self.tablets[&gid(T2_GROUP)]
        }

        async fn shutdown(self) {
            for member in &self.status {
                let _ = member.shutdown().await;
            }
            for members in self.tablets.values() {
                for member in members {
                    let _ = member.shutdown().await;
                }
            }
        }
    }

    async fn boot_tablet_member(
        cell_dir: &Path,
        group: u8,
        member: u8,
        transport: &Arc<InMemoryTransport>,
        bootstrap: bool,
    ) -> TabletTxnGroup<InMemoryTransport> {
        let node_data = cell_dir.join(format!("tablet-{group}-{member}"));
        let engine_config = EngineGroupConfig::new(
            node_data,
            gid(group),
            ClusterId::from_bytes([1; 16]),
            NodeId::from_bytes([group.wrapping_add(0x40); 16]),
            DatabaseId::from_bytes([group; 16]),
        );
        let engine = open_engine_sink(&engine_config).unwrap();
        let tablet = TabletTxnGroup::create(
            fast_group_config(&engine_config.group_dir(), group, member),
            gid(group),
            transport.clone(),
            engine,
        )
        .await
        .unwrap();
        if bootstrap {
            tablet
                .bootstrap(&[(
                    raft_id(group, member),
                    format!(
                        "127.0.0.1:{}",
                        7_000 + u16::from(group) * 10 + u16::from(member)
                    ),
                )])
                .await
                .unwrap();
            tablet.group().wait_leader(LEADER_TIMEOUT).await.unwrap();
        }
        tablet
    }

    /// Creates the i64 test table on the tablet through a catalog command
    /// (the same raft stream that later carries the intent protocol).
    async fn create_table(tablet: &TabletTxnGroup<InMemoryTransport>) {
        tablet
            .group()
            .propose(
                CommandKind::Catalog,
                engine_testing::create_i64_table_envelope(1, "t", 1),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        let db = tablet.engine().lock().unwrap().database().unwrap();
        assert_eq!(db.table_id("t").unwrap(), 0);
    }

    fn engine_writes(group: u8, tablet: u8, values: &[i64]) -> ParticipantWrites {
        ParticipantWrites {
            participant: participant(group, tablet),
            expected_schema_version: SchemaVersion::ZERO,
            expected_authz_version: 0,
            intents: values
                .iter()
                .map(|value| WriteIntent {
                    txn_id: TransactionId::ZERO,
                    key: value.to_le_bytes().to_vec(),
                    value_ref: engine_testing::staged_put_i64(0, &[*value]),
                    prepare_ts: HlcTimestamp::ZERO,
                })
                .collect(),
        }
    }

    fn visible(tablet: &TabletTxnGroup<InMemoryTransport>) -> Vec<i64> {
        let db = tablet.engine().lock().unwrap().database().unwrap();
        engine_testing::visible_i64s(&db, "t")
    }

    async fn boot_engine_cell() -> EngineCell {
        let tmp = tempfile::tempdir().unwrap();
        let transport = Arc::new(InMemoryTransport::new());
        let mut status = Vec::new();
        for member in 1..=3_u8 {
            status.push(boot_status_member(tmp.path(), member, &transport).await);
        }
        status[0]
            .bootstrap(&member_addresses(STATUS_GROUP))
            .await
            .unwrap();
        let mut tablets: BTreeMap<RaftGroupId, Vec<TabletTxnGroup<InMemoryTransport>>> =
            BTreeMap::new();
        for group in [T1_GROUP, T2_GROUP] {
            let member = boot_tablet_member(tmp.path(), group, 1, &transport, true).await;
            create_table(&member).await;
            tablets.insert(gid(group), vec![member]);
        }
        wait_leader_among(&status.iter().collect::<Vec<_>>()).await;
        EngineCell {
            tmp,
            transport,
            status,
            tablets,
        }
    }

    #[tokio::test]
    async fn engine_backed_commit_applies_rows_on_both_tablets_after_decision() {
        let cell = boot_engine_cell().await;
        let driver = DistTxnDriver::new(driver_config(TEN_MINUTES));
        let control = ExecutionControl::default();
        let request = commit_request(
            101,
            101,
            vec![
                engine_writes(T1_GROUP, 1, &[10, 20]),
                engine_writes(T2_GROUP, 2, &[30, 40]),
            ],
        );
        driver
            .begin(&cell.status, &request, &control)
            .await
            .unwrap();
        let token1 = driver
            .prepare_participant(cell.t1(), &request.txn_id, &request.writes[0], &control)
            .await
            .unwrap();
        let token2 = driver
            .prepare_participant(cell.t2(), &request.txn_id, &request.writes[1], &control)
            .await
            .unwrap();
        // Prepared but undecided: intents are durable, nothing is visible.
        assert_eq!(visible(&cell.t1()[0]), Vec::<i64>::new());
        assert_eq!(visible(&cell.t2()[0]), Vec::<i64>::new());
        let outcome = driver
            .decide_commit(
                &cell.status,
                &cell.tablets,
                &request.txn_id,
                &[token1, token2],
                &request.observed,
                &control,
            )
            .await
            .unwrap();
        // The decision materializes the staged writes on both tablets.
        assert_eq!(visible(&cell.t1()[0]), vec![10, 20]);
        assert_eq!(visible(&cell.t2()[0]), vec![30, 40]);
        for (group, keys) in [(T1_GROUP, 2_usize), (T2_GROUP, 2)] {
            let member = &cell.tablets[&gid(group)][0];
            let txn = member.txn(&xid(101)).unwrap();
            assert_eq!(
                txn.resolution,
                Some(TxnDecision::Committed {
                    commit_ts: outcome.commit_ts
                })
            );
            assert!(txn.applied);
            assert!(txn.resolved_at.is_some());
            assert!(txn.intents.is_empty());
            assert_eq!(
                member
                    .committed_writes()
                    .iter()
                    .filter(|write| write.txn_id == xid(101))
                    .count(),
                keys
            );
        }
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn engine_backed_abort_leaves_zero_rows() {
        let cell = boot_engine_cell().await;
        let driver = DistTxnDriver::new(driver_config(TEN_MINUTES));
        let control = ExecutionControl::default();
        let request = commit_request(
            102,
            102,
            vec![
                engine_writes(T1_GROUP, 1, &[10, 20]),
                engine_writes(T2_GROUP, 2, &[30, 40]),
            ],
        );
        driver
            .begin(&cell.status, &request, &control)
            .await
            .unwrap();
        driver
            .prepare_participant(cell.t1(), &request.txn_id, &request.writes[0], &control)
            .await
            .unwrap();
        driver
            .prepare_participant(cell.t2(), &request.txn_id, &request.writes[1], &control)
            .await
            .unwrap();
        let state = driver
            .abort(
                &cell.status,
                &cell.tablets,
                &request.txn_id,
                AbortReason::RolledBack,
                &control,
            )
            .await
            .unwrap();
        assert!(matches!(state, DistributedTxnState::Aborted { .. }));
        // Zero MVCC effect on both tablets; intents dropped.
        assert_eq!(visible(&cell.t1()[0]), Vec::<i64>::new());
        assert_eq!(visible(&cell.t2()[0]), Vec::<i64>::new());
        for group in [T1_GROUP, T2_GROUP] {
            let member = &cell.tablets[&gid(group)][0];
            let txn = member.txn(&xid(102)).unwrap();
            assert!(matches!(txn.resolution, Some(TxnDecision::Aborted { .. })));
            assert!(txn.applied);
            assert!(txn.intents.is_empty());
            assert!(member.committed_writes().is_empty());
        }
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn coordinator_kill_recovers_and_applies_identically() {
        let mut cell = boot_engine_cell().await;
        let driver = DistTxnDriver::new(driver_config(TEN_MINUTES));
        let control = ExecutionControl::default();
        let request = commit_request(
            103,
            103,
            vec![
                engine_writes(T1_GROUP, 1, &[10, 20]),
                engine_writes(T2_GROUP, 2, &[30, 40]),
            ],
        );
        driver
            .begin(&cell.status, &request, &control)
            .await
            .unwrap();
        let token1 = driver
            .prepare_participant(cell.t1(), &request.txn_id, &request.writes[0], &control)
            .await
            .unwrap();
        let token2 = driver
            .prepare_participant(cell.t2(), &request.txn_id, &request.writes[1], &control)
            .await
            .unwrap();
        // Kill the coordinator-group leader between prepare and decision.
        let leader = leader_index(&cell.status);
        let victim = cell.status.remove(leader);
        victim.crash().await;
        let survivor_refs: Vec<&TxnStatusGroup<InMemoryTransport>> = cell.status.iter().collect();
        wait_leader_among(&survivor_refs).await;
        let recovery = DistTxnDriver::new(DistTxnConfig {
            node_tiebreaker: 99,
            ..driver_config(TEN_MINUTES)
        });
        let now = recovery.now().unwrap();
        let outcome = recovery
            .recover(&cell.status, &cell.tablets, &request.txn_id, now, &control)
            .await
            .unwrap();
        let commit_ts = match outcome {
            RecoveryOutcome::Decided(DistributedTxnState::Committed { commit_ts }) => commit_ts,
            other => panic!("expected a committed recovery, got {other:?}"),
        };
        assert!(commit_ts > token1.prepare_ts);
        assert!(commit_ts > token2.prepare_ts);
        // Recovery applies identically on both tablets.
        assert_eq!(visible(&cell.t1()[0]), vec![10, 20]);
        assert_eq!(visible(&cell.t2()[0]), vec![30, 40]);
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn duplicate_resolve_never_double_applies() {
        let cell = boot_engine_cell().await;
        let driver = DistTxnDriver::new(driver_config(TEN_MINUTES));
        let control = ExecutionControl::default();
        let request = commit_request(
            104,
            104,
            vec![
                engine_writes(T1_GROUP, 1, &[10, 20]),
                engine_writes(T2_GROUP, 2, &[30, 40]),
            ],
        );
        let first = driver
            .commit(&cell.status, &cell.tablets, request.clone(), &control)
            .await
            .unwrap();
        // Client retry after an ambiguous failure: the same transaction id
        // and idempotency key replays the original outcome and re-broadcasts
        // the resolve.
        let second = driver
            .commit(&cell.status, &cell.tablets, request, &control)
            .await
            .unwrap();
        assert_eq!(first, second);
        // Explicit duplicate resolve broadcast on top: still no re-apply.
        driver
            .broadcast_resolve(
                &cell.tablets,
                &xid(104),
                &second.participants,
                TxnDecision::Committed {
                    commit_ts: second.commit_ts,
                },
                &control,
            )
            .await;
        assert_eq!(visible(&cell.t1()[0]), vec![10, 20]);
        assert_eq!(visible(&cell.t2()[0]), vec![30, 40]);
        for group in [T1_GROUP, T2_GROUP] {
            let member = &cell.tablets[&gid(group)][0];
            assert_eq!(member.state().txns.len(), 1);
            assert_eq!(
                member
                    .committed_writes()
                    .iter()
                    .filter(|write| write.txn_id == xid(104))
                    .count(),
                2
            );
        }
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn tablet_restart_after_resolve_replays_without_double_apply() {
        let mut cell = boot_engine_cell().await;
        let driver = DistTxnDriver::new(driver_config(TEN_MINUTES));
        let control = ExecutionControl::default();
        let request = commit_request(
            105,
            105,
            vec![
                engine_writes(T1_GROUP, 1, &[10, 20]),
                engine_writes(T2_GROUP, 2, &[30, 40]),
            ],
        );
        let outcome = driver
            .commit(&cell.status, &cell.tablets, request, &control)
            .await
            .unwrap();
        assert_eq!(visible(&cell.t1()[0]), vec![10, 20]);
        // Crash T1's only member and reboot it over the same directories:
        // the engine core recovers the applied rows from its WAL, the intent
        // checkpoint recovers the applied resolution.
        let victim = cell.tablets.get_mut(&gid(T1_GROUP)).unwrap().remove(0);
        victim.crash().await;
        let healed = boot_tablet_member(cell.tmp.path(), T1_GROUP, 1, &cell.transport, false).await;
        healed.group().wait_leader(LEADER_TIMEOUT).await.unwrap();
        let txn = healed.txn(&xid(105)).unwrap();
        assert!(txn.applied);
        assert_eq!(
            txn.resolution,
            Some(TxnDecision::Committed {
                commit_ts: outcome.commit_ts
            })
        );
        assert_eq!(visible(&healed), vec![10, 20]);
        // A redriven resolve reaches the healed member and is a no-op for
        // the engine (the record is already applied).
        cell.tablets.get_mut(&gid(T1_GROUP)).unwrap().push(healed);
        driver
            .broadcast_resolve(
                &cell.tablets,
                &xid(105),
                &outcome.participants,
                TxnDecision::Committed {
                    commit_ts: outcome.commit_ts,
                },
                &control,
            )
            .await;
        assert_eq!(visible(&cell.t1()[0]), vec![10, 20]);
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn resolved_sweep_bounds_tombstones_and_keeps_engine_rows() {
        let cell = boot_engine_cell().await;
        let config = DistTxnConfig {
            resolved_retention: Duration::ZERO,
            sweep_limit: 2,
            ..driver_config(TEN_MINUTES)
        };
        let driver = DistTxnDriver::new(config);
        let control = ExecutionControl::default();
        // Three committed single-participant transactions plus one aborted:
        // four tombstones on T1.
        for (index, values) in [[10_i64, 20], [30, 40], [50, 60]].iter().enumerate() {
            let txn = 110 + index as u8;
            let request = commit_request(txn, txn, vec![engine_writes(T1_GROUP, 1, values)]);
            driver
                .commit(&cell.status, &cell.tablets, request, &control)
                .await
                .unwrap();
        }
        let aborted = commit_request(113, 113, vec![engine_writes(T1_GROUP, 1, &[70])]);
        driver
            .begin(&cell.status, &aborted, &control)
            .await
            .unwrap();
        driver
            .prepare_participant(cell.t1(), &aborted.txn_id, &aborted.writes[0], &control)
            .await
            .unwrap();
        driver
            .abort(
                &cell.status,
                &cell.tablets,
                &aborted.txn_id,
                AbortReason::RolledBack,
                &control,
            )
            .await
            .unwrap();
        assert_eq!(cell.t1()[0].state().txns.len(), 4);
        assert_eq!(visible(&cell.t1()[0]), vec![10, 20, 30, 40, 50, 60]);

        // Bounded sweep: at most `sweep_limit` tombstones per command.
        let now = later(driver.now().unwrap(), Duration::from_secs(3_600));
        let report = driver
            .sweep_resolved(cell.t1(), now, &control)
            .await
            .unwrap();
        assert_eq!(
            report,
            SweepResolvedReport {
                swept: 2,
                remaining: 2
            }
        );
        assert_eq!(cell.t1()[0].state().txns.len(), 2);
        // A follow-up sweep carries a newer cutoff (sweeps are
        // time-parameterized housekeeping; an identical command id would be
        // an idempotent replay).
        let later_now = later(now, Duration::from_secs(1));
        let report = driver
            .sweep_resolved(cell.t1(), later_now, &control)
            .await
            .unwrap();
        assert_eq!(
            report,
            SweepResolvedReport {
                swept: 2,
                remaining: 0
            }
        );
        assert!(cell.t1()[0].state().txns.is_empty());
        assert!(cell.t1()[0].committed_writes().is_empty());
        // The sweep only drops protocol records: the applied rows stay.
        assert_eq!(visible(&cell.t1()[0]), vec![10, 20, 30, 40, 50, 60]);
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn parallel_prepare_respects_the_deadline() {
        let tmp = tempfile::tempdir().unwrap();
        let transport = Arc::new(InMemoryTransport::new());
        let mut status = Vec::new();
        for member in 1..=3_u8 {
            status.push(boot_status_member(tmp.path(), member, &transport).await);
        }
        status[0]
            .bootstrap(&member_addresses(STATUS_GROUP))
            .await
            .unwrap();
        // T1: one healthy engine tablet. T2: three members, two of which
        // will be killed so the group has no quorum and every prepare must
        // wait out the deadline.
        let t1 = boot_tablet_member(tmp.path(), T1_GROUP, 1, &transport, true).await;
        create_table(&t1).await;
        let mut t2_members = Vec::new();
        for member in 1..=3_u8 {
            t2_members
                .push(boot_tablet_member(tmp.path(), T2_GROUP, member, &transport, false).await);
        }
        t2_members[0]
            .bootstrap(&member_addresses(T2_GROUP))
            .await
            .unwrap();
        t2_members.pop().unwrap().crash().await;
        t2_members.pop().unwrap().crash().await;
        let mut tablets: BTreeMap<RaftGroupId, Vec<TabletTxnGroup<InMemoryTransport>>> =
            BTreeMap::new();
        tablets.insert(gid(T1_GROUP), vec![t1]);
        tablets.insert(gid(T2_GROUP), t2_members);
        wait_leader_among(&status.iter().collect::<Vec<_>>()).await;

        let driver = DistTxnDriver::new(driver_config(Duration::from_millis(1)));
        let control = ExecutionControl {
            deadline: Some(std::time::Instant::now() + Duration::from_millis(1_000)),
            cancellation: None,
        };
        // The dead participant comes first in request order: serially, T1's
        // prepare would never run; concurrently, it lands while T2's waits
        // out the deadline.
        let request = commit_request(
            120,
            120,
            vec![
                engine_writes(T2_GROUP, 2, &[30]),
                engine_writes(T1_GROUP, 1, &[10, 20]),
            ],
        );
        let error = driver
            .commit(&status, &tablets, request.clone(), &control)
            .await
            .unwrap_err();
        assert!(
            matches!(error, DistTxnError::OutcomeAmbiguous { .. }),
            "expected an ambiguous outcome after the deadline, got {error:?}"
        );
        // T1 prepared (the concurrency proof) but shows zero rows: no
        // decision was durable, so nothing is visible.
        {
            let t1 = &tablets[&gid(T1_GROUP)][0];
            assert_eq!(t1.unresolved_txn_ids(), vec![xid(120)]);
            assert_eq!(visible(t1), Vec::<i64>::new());
        }
        // Recovery with a fresh control and an expired lease resolves the
        // transaction deterministically: pushed to abort, intents dropped,
        // still zero rows. The last T2 member is gone now, so its resolve
        // broadcast defers immediately (empty member list) and T1 resolves.
        let last_t2 = tablets.get_mut(&gid(T2_GROUP)).unwrap().remove(0);
        last_t2.crash().await;
        let pusher = DistTxnDriver::new(driver_config(Duration::from_millis(1)));
        let now = later(pusher.now().unwrap(), Duration::from_secs(3_600));
        let fresh = ExecutionControl::default();
        let outcome = pusher
            .push_expired(&status, &tablets, &request.txn_id, now, &fresh)
            .await
            .unwrap();
        assert!(matches!(outcome, PushOutcome::Pushed(_)));
        let t1 = &tablets[&gid(T1_GROUP)][0];
        wait_until("t1 abort resolution", || async {
            t1.txn(&xid(120))
                .is_some_and(|txn| txn.resolution.is_some())
        })
        .await;
        let txn = t1.txn(&xid(120)).unwrap();
        assert!(matches!(txn.resolution, Some(TxnDecision::Aborted { .. })));
        assert!(txn.applied);
        assert!(txn.intents.is_empty());
        assert_eq!(visible(t1), Vec::<i64>::new());
        for member in &status {
            let _ = member.shutdown().await;
        }
        for members in tablets.values() {
            for member in members {
                let _ = member.shutdown().await;
            }
        }
    }
}
