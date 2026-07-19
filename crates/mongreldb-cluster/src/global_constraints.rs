//! Global constraints: unique indexes, foreign keys, and replicated sequences
//! (spec section 12.9, Stage 3I).
//!
//! Once a table is partitioned across tablets, row-local constraint
//! machinery is no longer enough: a unique key can hash onto a different
//! tablet than a concurrent duplicate, a foreign-key parent can live on
//! another tablet than its children, and an auto-increment counter must
//! never re-issue a value across nodes. This module implements the three
//! global mechanisms on top of the replicated two-phase commit of
//! [`crate::dist_txn`].
//!
//! # Unique constraints
//!
//! **Fast path (documented, engine-local).** When the unique key includes
//! the partition key, every duplicate of a value hashes onto the same
//! tablet as the value itself, so the tablet's own unique machinery
//! decides — no distributed work. [`unique_enforcement_path`] makes the
//! decision from the table's [`Partitioning`]; the engine binding consumes
//! it and never enters the global path for local constraints.
//!
//! **Global path.** A *global unique-index tablet* (a system table mapping
//! `unique-key-hash -> (table, pk)`, spec section 12.9) is updated in the
//! SAME distributed transaction as the row write: one 2PC with a claim
//! intent on the unique-index tablet and the row intent on the data
//! tablet, driven through [`crate::dist_txn::DistTxnDriver`]
//! ([`GlobalConstraintDriver::commit_unique_insert`]). Conflict detection
//! happens *at prepare*, inside the unique-index tablet's apply path:
//! [`UniqueIndexApplySink`] speaks the intent wire protocol of
//! [`crate::dist_txn`] (same envelope type, same commands) but persists
//! committed claims, so a `PersistIntents` that claims an already-claimed
//! key is refused in the raft total order — first-preparer-wins for
//! in-flight claims, claim-table for committed ones. Two concurrent claims
//! of the same key on different nodes therefore commit exactly once; the
//! loser aborts with [`AbortReason::Conflict`] and surfaces
//! [`GlobalConstraintError::UniqueViolation`] (category
//! [`ErrorCategory::TransactionConflict`]).
//!
//! The unique-index tablet participates in the coordinator record like any
//! other participant, so the decision and its recovery ride the standard
//! protocol. Constraint-aware orphan recovery (probing the unique-index
//! group in [`crate::dist_txn::DistTxnDriver::recover`]) is a later-wave
//! binding, exactly like the engine binding of data-tablet intents; the
//! resolve command ids here are derived with the same deterministic scheme
//! as `dist_txn`, so that binding replays idempotently.
//!
//! # Foreign keys
//!
//! **Fast path.** Colocated parent and child (same tablet) check locally —
//! [`fk_enforcement_path`] decides.
//!
//! **Cross-tablet.** The child insert runs one 2PC (spec section 12.9:
//! "distributed transactions and replicated locks/intents") carrying the
//! child row intent plus a *probe intent* on the parent tablet: a lock
//! intent on the parent row's key that interlocks with any concurrent
//! parent delete through the intent layer's first-preparer-wins conflict
//! detection. Parent *existence* is an engine-level read; this module
//! binds it through the [`ParentExistence`] seam (an in-memory oracle in
//! tests, the tablet server in the engine wave). A missing parent fails
//! validation before the commit fence
//! ([`GlobalConstraintError::ForeignKeyViolation`], category
//! [`ErrorCategory::TransactionAborted`]); a parent delete racing the
//! insert loses or wins the key conflict exactly once
//! ([`ErrorCategory::TransactionConflict`]).
//!
//! **Cascades** are bounded by exactly the five bounds of spec section
//! 12.9 — maximum rows, maximum tablets, maximum depth, work budget,
//! deadline ([`CascadeBounds`]). [`CascadeExecutor::plan`] walks the FK
//! graph and enforces every bound BEFORE any write is proposed; a tripped
//! bound fails with [`GlobalConstraintError::CascadeExhausted`] (category
//! [`ErrorCategory::ResourceExhausted`]) and nothing is applied.
//! [`CascadeExecutor::execute`] then applies every level in ONE
//! distributed transaction, so a cascade never partially applies.
//!
//! # Replicated sequences
//!
//! A *sequence tablet* (a dedicated raft group, [`SequenceGroup`]) owns
//! each sequence's high-water mark and grants ranges
//! `[N, N + 999]`-style ([`DEFAULT_SEQUENCE_GRANT_WIDTH`]) through the
//! replicated [`SequenceCommand::Grant`] command; the granted range is
//! recorded as a [`SequenceGrant`]. A node consumes its granted range
//! locally with a monotonic counter ([`SequenceAllocator`]); the grant
//! high-water mark is replicated and checkpointed, so a restart never
//! re-issues a value, and concurrent allocators on different nodes draw
//! disjoint ranges.
//!
//! **Sequences are not gapless.** Values consumed from a grant and then
//! rolled back (transaction abort), or left unused when a node crashes or
//! its allocator is dropped, are never returned to the pool: the
//! high-water mark only moves forward. Rollback therefore does NOT
//! guarantee gapless sequences (spec section 12.9); callers that need
//! gapless numbering must serialize allocation inside the committing
//! transaction instead.
//!
//! # Scope of this wave
//!
//! The cluster crate deliberately has no engine (`mongreldb-core`)
//! dependency, so every row-level check binds through a documented seam:
//! parent existence through [`ParentExistence`], cascade children through
//! [`CascadeGraph`], and claim release on row delete / local unique
//! enforcement through the engine binding of the tablet-server wave. What
//! lands here is the replicated machinery: the unique-index apply sink and
//! group, the constraint commit flows on [`DistTxnDriver`], the bounded
//! cascade executor, and the sequence grant group with its durable
//! high-water mark.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use mongreldb_consensus::error::ConsensusError;
use mongreldb_consensus::group::{ConsensusGroup, GroupCommitReceipt, GroupConfig};
use mongreldb_consensus::identity::{CommandKind, RaftNodeId, ReplicatedCommand};
use mongreldb_consensus::network::RaftTransport;
use mongreldb_consensus::read::ReadConsistencyError;
use mongreldb_consensus::state_machine::{AppliedCommand, ApplySink, StateMachineError};
use mongreldb_log::commit_log::{ExecutionControl, LogPosition};
use mongreldb_log::envelope::CommandEnvelope;
use mongreldb_types::errors::ErrorCategory;
use mongreldb_types::hlc::{HlcClock, HlcTimestamp};
use mongreldb_types::ids::{NodeId, RaftGroupId, SchemaVersion, TableId, TabletId, TransactionId};

use crate::dist_txn::{
    AbortReason, CommitRequest, CommittedWrite, DistTxnConfig, DistTxnDriver, DistTxnError,
    DistributedTxnState, IntentCommand, IntentCommandRecord, IntentGroup, ParticipantTxn,
    ParticipantWrites, PrepareToken, TxnDecision, TxnParticipant, TxnStatusGroup, WriteIntent,
    COMMAND_TYPE_DIST_TXN_INTENT,
};
use crate::tablet::{ColumnId, Partitioning};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Envelope discriminant for sequence-grant commands. Discriminants are
/// never reused (spec section 4.10): `1` transaction, `2` catalog,
/// `3` maintenance (engine), `4` meta control-plane, `5`/`6` distributed
/// transaction protocol; `7` belongs to the replicated sequence allocator.
/// (The unique-index tablet speaks the intent protocol and therefore
/// shares [`COMMAND_TYPE_DIST_TXN_INTENT`].)
pub const COMMAND_TYPE_SEQUENCE: u32 = 7;

/// Format version of the [`SequenceCommandRecord`] payloads this build
/// writes.
pub const SEQUENCE_RECORD_FORMAT_VERSION: u32 = 1;
/// Oldest sequence payload format version this build accepts.
pub const MIN_SUPPORTED_SEQUENCE_RECORD_FORMAT_VERSION: u32 = 1;

/// Format version of the constraint sink checkpoints this build writes.
pub const CONSTRAINT_CHECKPOINT_FORMAT_VERSION: u32 = 1;
/// Oldest constraint sink checkpoint format version this build accepts.
pub const MIN_SUPPORTED_CONSTRAINT_CHECKPOINT_FORMAT_VERSION: u32 = 1;

/// Unique-index sink checkpoint file under `<group dir>/raft/state`.
pub const UNIQUE_INDEX_CHECKPOINT_FILENAME: &str = "unique-index-state.json";
/// Sequence sink checkpoint file under `<group dir>/raft/state`.
pub const SEQUENCE_CHECKPOINT_FILENAME: &str = "sequence-state.json";

/// Bound on the constraint rejection journals (mirrors
/// [`crate::dist_txn::DIST_TXN_REJECTION_LIMIT`]).
pub const CONSTRAINT_REJECTION_LIMIT: usize = 256;

/// Default width of one sequence range grant: the sequence tablet grants
/// `[N, N + 999]` (spec section 12.9).
pub const DEFAULT_SEQUENCE_GRANT_WIDTH: u64 = 1_000;
/// Largest width a single sequence grant may request (fail-closed guard
/// against high-water exhaustion by a malformed or runaway client).
pub const MAX_SEQUENCE_GRANT_WIDTH: u64 = 1_000_000;

/// Deterministic command-id tags (sha256 domain separation). These three
/// steps are proposed by this module into groups whose protocol
/// [`crate::dist_txn`] also drives (or, for recovery, may one day drive):
/// the tag strings and the derivation are identical to `dist_txn`'s, so a
/// re-proposal of the same logical step lands the same command id and the
/// state machine's idempotent apply (S2B-004) replays the original
/// outcome. `command_id_for` itself is private to `dist_txn`; the tiny
/// pure function is mirrored here exactly like `dist_txn` mirrors the
/// engine's FNV-1a.
const TAG_MARK: &str = "dist-txn/mark-preparing";
const TAG_PREPARE: &str = "dist-txn/intents";
const TAG_RESOLVE: &str = "dist-txn/resolve";

/// Derives the deterministic command id of one protocol step (identical
/// derivation to `crate::dist_txn`; see the tag docs above).
fn command_id_for(tag: &str, txn_id: &TransactionId, extra: &[u8]) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hasher.update(tag.as_bytes());
    hasher.update(txn_id.as_bytes());
    hasher.update(extra);
    let digest = hasher.finalize();
    digest[..16].try_into().expect("sha256 digest is 32 bytes")
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors produced by the global-constraint machinery. Each variant maps
/// onto the stable [`ErrorCategory`] taxonomy (spec section 9.7): handle
/// categories, not messages.
#[derive(Debug, thiserror::Error)]
pub enum GlobalConstraintError {
    /// The underlying distributed-transaction protocol failed (including
    /// its definitive aborts and ambiguous outcomes).
    #[error(transparent)]
    Txn(#[from] DistTxnError),
    /// Consensus group failure on a constraint-owned group (unique-index
    /// or sequence).
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
    /// A unique-key claim lost the prepare race or found the key already
    /// claimed: the transaction aborted with [`AbortReason::Conflict`].
    #[error("unique constraint {constraint} on table {table} violated: {detail}")]
    UniqueViolation {
        /// The table holding the constraint.
        table: TableId,
        /// The constraint/index name.
        constraint: String,
        /// What conflicted (committed claim or in-flight claim).
        detail: String,
    },
    /// A unique-index tablet refused the claim prepare for a
    /// protocol-level reason (stale versions, lost resolution race,
    /// malformed claim).
    #[error("unique claim prepare refused: {0}")]
    ClaimPrepareRejected(UniqueRejectionReason),
    /// A foreign-key parent does not exist (validation failure before the
    /// commit fence).
    #[error("foreign key violation on table {child_table}: parent row missing ({detail})")]
    ForeignKeyViolation {
        /// The child table being inserted into.
        child_table: TableId,
        /// Which parent probe failed.
        detail: String,
    },
    /// A cascade exceeded one of its bounds; nothing was applied.
    #[error("cascade bound {bound} exceeded: {detail}")]
    CascadeExhausted {
        /// Which bound tripped.
        bound: CascadeBoundKind,
        /// The measured value versus the bound.
        detail: String,
    },
    /// A sequence's high-water mark cannot advance (the `u64` space is
    /// exhausted) or a grant request was malformed.
    #[error("sequence {sequence} cannot grant a range: {detail}")]
    SequenceExhausted {
        /// The sequence.
        sequence: String,
        /// Why the grant failed.
        detail: String,
    },
    /// A sequence tablet refused the grant for a protocol-level reason.
    #[error("sequence grant refused: {0}")]
    SequenceGrantRejected(SequenceRejectionReason),
    /// The caller's request was malformed for this flow or configuration.
    #[error("invalid global-constraint request: {0}")]
    InvalidRequest(String),
    /// Encoding a command record failed.
    #[error("global-constraint command encoding failed: {0}")]
    Encode(String),
    /// Group I/O failure.
    #[error("global-constraint group I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// A sink's durable checkpoint failed verification (fails closed, spec
    /// section 4.10).
    #[error("corrupt global-constraint checkpoint: {0}")]
    CorruptCheckpoint(String),
    /// No group member answered as a live leader within the retry budget.
    #[error("no live group leader: {0}")]
    Unavailable(String),
    /// Minting a deterministic-or-random command id failed.
    #[error("could not mint a command id: {0}")]
    CommandId(String),
}

impl GlobalConstraintError {
    /// The stable taxonomy category of this error (spec section 9.7).
    pub fn category(&self) -> ErrorCategory {
        match self {
            GlobalConstraintError::UniqueViolation { .. }
            | GlobalConstraintError::ClaimPrepareRejected(
                UniqueRejectionReason::KeyConflict { .. } | UniqueRejectionReason::ClaimHeld { .. },
            )
            | GlobalConstraintError::Txn(DistTxnError::Aborted(AbortReason::Conflict(_)))
            | GlobalConstraintError::Txn(DistTxnError::PrepareRejected(_)) => {
                ErrorCategory::TransactionConflict
            }
            GlobalConstraintError::ClaimPrepareRejected(
                UniqueRejectionReason::StaleSchemaVersion { .. },
            ) => ErrorCategory::SchemaVersionMismatch,
            GlobalConstraintError::ClaimPrepareRejected(
                UniqueRejectionReason::StaleAuthzVersion { .. },
            ) => ErrorCategory::StaleMetadata,
            GlobalConstraintError::ClaimPrepareRejected(
                UniqueRejectionReason::MalformedClaim { .. }
                | UniqueRejectionReason::PayloadMismatch
                | UniqueRejectionReason::AlreadyResolved { .. },
            ) => ErrorCategory::TransactionAborted,
            GlobalConstraintError::Txn(DistTxnError::OutcomeAmbiguous { .. }) => {
                ErrorCategory::CommitOutcomeUnknown
            }
            GlobalConstraintError::ForeignKeyViolation { .. }
            | GlobalConstraintError::Txn(DistTxnError::Aborted(_)) => {
                ErrorCategory::TransactionAborted
            }
            GlobalConstraintError::CascadeExhausted { .. }
            | GlobalConstraintError::SequenceExhausted { .. } => ErrorCategory::ResourceExhausted,
            GlobalConstraintError::Consensus(ConsensusError::NotLeader { .. }) => {
                ErrorCategory::NotLeader
            }
            GlobalConstraintError::Consensus(ConsensusError::Cancelled) => ErrorCategory::Cancelled,
            GlobalConstraintError::Consensus(ConsensusError::DeadlineExceeded) => {
                ErrorCategory::DeadlineExceeded
            }
            GlobalConstraintError::Unavailable(_) => ErrorCategory::QuorumUnavailable,
            GlobalConstraintError::Txn(_)
            | GlobalConstraintError::Consensus(_)
            | GlobalConstraintError::SequenceGrantRejected(_)
            | GlobalConstraintError::InvalidRequest(_)
            | GlobalConstraintError::Encode(_)
            | GlobalConstraintError::Io(_)
            | GlobalConstraintError::CorruptCheckpoint(_)
            | GlobalConstraintError::CommandId(_) => ErrorCategory::ReplicaUnavailable,
        }
    }
}

// ---------------------------------------------------------------------------
// Mirrored driver helpers (dist_txn's are private to it)
// ---------------------------------------------------------------------------

/// Builds one openraft node value for the membership calls without naming
/// the openraft type (same serde-shape bridge as `dist_txn` and the meta
/// group; the cluster crate deliberately has no openraft dependency,
/// ADR-0004).
fn basic_node<N>(address: &str) -> Result<N, GlobalConstraintError>
where
    N: for<'de> Deserialize<'de>,
{
    serde_json::from_value(serde_json::json!({ "addr": address })).map_err(|error| {
        GlobalConstraintError::InvalidRequest(format!("member address `{address}`: {error}"))
    })
}

/// The leader view of one group member for the retry loop (mirrors
/// `dist_txn`'s private `GroupMember`).
trait MemberView {
    fn member_node_id(&self) -> RaftNodeId;
    fn member_leader(&self) -> Option<RaftNodeId>;
}

impl<T: RaftTransport> MemberView for TxnStatusGroup<T> {
    fn member_node_id(&self) -> RaftNodeId {
        self.group().node_id()
    }

    fn member_leader(&self) -> Option<RaftNodeId> {
        self.group().metrics().current_leader
    }
}

/// Whether a propose/read failure is a leadership transient worth retrying
/// on another member (same classification as `dist_txn`'s private helper).
fn retryable(error: &GlobalConstraintError) -> bool {
    matches!(
        error,
        GlobalConstraintError::Txn(
            DistTxnError::Consensus(
                ConsensusError::NotLeader { .. } | ConsensusError::Closed | ConsensusError::Raft(_),
            ) | DistTxnError::Read(
                ReadConsistencyError::NotLeader { .. }
                    | ReadConsistencyError::LeaderUnknown
                    | ReadConsistencyError::Closed,
            ),
        ) | GlobalConstraintError::Consensus(
            ConsensusError::NotLeader { .. } | ConsensusError::Closed | ConsensusError::Raft(_),
        )
    )
}

/// Maps a cooperative-control failure to this module's error surface.
fn control_error(control: &ExecutionControl) -> Option<GlobalConstraintError> {
    control.check().err().map(|error| match error {
        mongreldb_log::commit_log::LogError::Cancelled => {
            GlobalConstraintError::Consensus(ConsensusError::Cancelled)
        }
        mongreldb_log::commit_log::LogError::DeadlineExceeded => {
            GlobalConstraintError::Consensus(ConsensusError::DeadlineExceeded)
        }
        other => GlobalConstraintError::Consensus(ConsensusError::Raft(other.to_string())),
    })
}

/// Runs `call` against the members, leader-claimants first, retrying
/// leadership transients across members until the retry budget or the
/// caller's control fires (mirrors `dist_txn`'s private
/// `retry_across_members`, which is typed on its own private member
/// trait).
async fn retry_across_members<'m, G, F, Fut, R>(
    members: &'m [G],
    group_label: &str,
    config: &DistTxnConfig,
    control: &ExecutionControl,
    mut call: F,
) -> Result<(&'m G, R), GlobalConstraintError>
where
    G: MemberView,
    F: FnMut(&'m G) -> Fut,
    Fut: Future<Output = Result<R, GlobalConstraintError>>,
{
    if members.is_empty() {
        return Err(GlobalConstraintError::InvalidRequest(format!(
            "no members supplied for {group_label}"
        )));
    }
    let mut rounds = 0_usize;
    loop {
        let mut ordered: Vec<&G> = members
            .iter()
            .filter(|member| member.member_leader() == Some(member.member_node_id()))
            .collect();
        ordered.extend(
            members
                .iter()
                .filter(|member| member.member_leader() != Some(member.member_node_id())),
        );
        let mut last_error: Option<GlobalConstraintError> = None;
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
            return Err(GlobalConstraintError::Unavailable(format!(
                "{group_label}: no live leader after {rounds} rounds ({})",
                last_error
                    .map(|error| error.to_string())
                    .unwrap_or_else(|| "no members answered".to_owned())
            )));
        }
        tokio::time::sleep(config.propose_retry_interval).await;
    }
}

/// Maps a protocol failure to the abort reason persisted on the
/// coordinator record (mirrors `dist_txn`'s private `abort_reason_of`).
fn abort_reason_of(error: &GlobalConstraintError) -> AbortReason {
    match error {
        GlobalConstraintError::UniqueViolation { detail, .. } => {
            AbortReason::Conflict(detail.clone())
        }
        GlobalConstraintError::ClaimPrepareRejected(reason) => {
            AbortReason::Conflict(reason.to_string())
        }
        GlobalConstraintError::Txn(DistTxnError::Aborted(reason)) => reason.clone(),
        GlobalConstraintError::Consensus(ConsensusError::Cancelled) => {
            AbortReason::Cancelled("cancelled".to_owned())
        }
        GlobalConstraintError::Consensus(ConsensusError::DeadlineExceeded) => {
            AbortReason::Cancelled("deadline exceeded".to_owned())
        }
        other => AbortReason::Error(other.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Unique constraints: fast-path selection and claim records
// ---------------------------------------------------------------------------

/// How a unique constraint is enforced (spec section 12.9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniqueEnforcementPath {
    /// The unique key includes the partition key: every duplicate hashes
    /// onto the same tablet, so the tablet's own unique machinery decides
    /// (single-tablet check; no distributed work). The engine binding
    /// performs the local check; this layer is never entered.
    Local,
    /// The unique key does not include the partition key: a global
    /// unique-index tablet arbitrates claims inside the row write's
    /// distributed transaction ([`GlobalConstraintDriver::commit_unique_insert`]).
    GlobalIndex,
}

/// Selects the enforcement path for one unique constraint: `Local` exactly
/// when the table's partition-key columns are a subset of the unique key's
/// columns (spec section 12.9 "unique key includes partition key").
pub fn unique_enforcement_path(
    partitioning: &Partitioning,
    unique_columns: &[ColumnId],
) -> UniqueEnforcementPath {
    let partition_columns = partitioning.partition_columns();
    if unique_columns.is_empty()
        || !partition_columns
            .iter()
            .all(|column| unique_columns.contains(column))
    {
        return UniqueEnforcementPath::GlobalIndex;
    }
    UniqueEnforcementPath::Local
}

/// Identity of one global unique constraint's index partition: the
/// constrained table, the constraint's name, and the unique-index tablet
/// (with its raft group) arbitrating claims for this key range. The
/// unique-index system table is partitioned by claim-key hash; routing its
/// descriptors is the meta plane's job and rides this reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniqueIndexRef {
    /// The table holding the constraint.
    pub table: TableId,
    /// The constraint/index name (stable in the catalog).
    pub constraint: String,
    /// The unique-index tablet arbitrating this claim.
    pub tablet: TxnParticipant,
}

/// The value side of a committed claim (spec section 12.9: the global
/// unique index maps `unique-key-hash -> (table, pk)`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniqueClaimValue {
    /// The table holding the constrained row.
    pub table: TableId,
    /// The primary key of the row holding the unique value.
    pub pk: Vec<u8>,
}

impl UniqueClaimValue {
    /// Encodes the claim value for the intent's `value_ref` (JSON, the
    /// crate's human-readable convention).
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("UniqueClaimValue serialization is total")
    }

    /// Decodes a claim value from an intent's `value_ref`.
    pub fn decode(payload: &[u8]) -> Result<Self, GlobalConstraintError> {
        serde_json::from_slice(payload)
            .map_err(|error| GlobalConstraintError::Encode(format!("claim value: {error}")))
    }
}

/// One unique-key claim inside a row write's transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniqueClaim {
    /// The constraint and its arbitrating unique-index tablet.
    pub index: UniqueIndexRef,
    /// The encoded unique-key components (the engine's key encoding; the
    /// claim hashes them, so any encoding the table uses consistently
    /// works).
    pub key_bytes: Vec<u8>,
    /// The primary key of the row being written.
    pub pk: Vec<u8>,
}

impl UniqueClaim {
    /// The deterministic claim key on the unique-index tablet:
    /// `uq/<table>/<constraint>/<sha256(key_bytes) hex>`. The hash makes
    /// the key fixed-width and keeps raw user data out of the index
    /// tablet's key space.
    pub fn intent_key(&self) -> Vec<u8> {
        let digest = Sha256::digest(&self.key_bytes);
        let mut key =
            format!("uq/{}/{}", self.index.table.get(), self.index.constraint).into_bytes();
        key.push(b'/');
        for byte in digest.iter() {
            key.extend_from_slice(format!("{byte:02x}").as_bytes());
        }
        key
    }

    /// The claim's value reference (`(table, pk)`, spec section 12.9).
    pub fn value_ref(&self) -> Vec<u8> {
        UniqueClaimValue {
            table: self.index.table,
            pk: self.pk.clone(),
        }
        .encode()
    }

    /// The claim as a [`ParticipantWrites`] entry for the 2PC (prepare
    /// timestamp and transaction id are stamped by the driver).
    pub fn writes(
        &self,
        expected_schema_version: SchemaVersion,
        expected_authz_version: u64,
    ) -> ParticipantWrites {
        ParticipantWrites {
            participant: self.index.tablet,
            expected_schema_version,
            expected_authz_version,
            intents: vec![WriteIntent {
                txn_id: TransactionId::ZERO,
                key: self.intent_key(),
                value_ref: self.value_ref(),
                prepare_ts: HlcTimestamp::ZERO,
            }],
        }
    }
}

/// A claim made visible by a committed resolution (the unique-index
/// tablet's materialization of spec section 12.9's mapping).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedClaim {
    /// The claim key (see [`UniqueClaim::intent_key`]).
    pub key: Vec<u8>,
    /// The claimed `(table, pk)`.
    pub value: UniqueClaimValue,
    /// The transaction that committed the claim.
    pub txn_id: TransactionId,
    /// The claim's visibility timestamp (the transaction's `commit_ts`).
    pub commit_ts: HlcTimestamp,
}

// ---------------------------------------------------------------------------
// Unique-index apply sink: the intent protocol plus a persistent claim table
// ---------------------------------------------------------------------------

/// Why the unique-index apply path refused `PersistIntents`. Refusals are
/// journaled in [`UniqueIndexState::rejections`]; the raft entry commits
/// normally (deterministic, total apply; same contract as
/// [`crate::dist_txn::IntentApplySink`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum UniqueRejectionReason {
    /// Another unresolved transaction already holds an intent on the claim
    /// key (write/write conflict; first-preparer-wins).
    #[error("claim conflict: another transaction holds an intent on the key")]
    KeyConflict {
        /// The contested claim key.
        key: Vec<u8>,
        /// The transaction holding the intent.
        holder: TransactionId,
    },
    /// The key is already claimed by a committed transaction with a
    /// different `(table, pk)` (conflict detection at prepare, spec
    /// section 12.9).
    #[error("claim conflict: the key is already claimed: {detail}")]
    ClaimHeld {
        /// The contested claim key.
        key: Vec<u8>,
        /// The transaction that committed the claim.
        holder: TransactionId,
        /// Human-readable detail (holder table/pk).
        detail: String,
    },
    /// A claim intent's value is not a decodable [`UniqueClaimValue`].
    #[error("malformed claim value")]
    MalformedClaim {
        /// The claim key whose value failed to decode.
        key: Vec<u8>,
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
    /// The transaction's schema version is stale.
    #[error("stale schema version {expected:?} (unique-index tablet is at {found:?})")]
    StaleSchemaVersion {
        /// The version the transaction planned against.
        expected: SchemaVersion,
        /// The tablet's current version.
        found: SchemaVersion,
    },
    /// The transaction's authorization version is stale.
    #[error("stale authorization version {expected} (unique-index tablet is at {found})")]
    StaleAuthzVersion {
        /// The version the transaction authenticated against.
        expected: u64,
        /// The tablet's current version.
        found: u64,
    },
}

/// One journaled unique-index rejection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniqueRejection {
    /// Raft position of the refused entry.
    pub position: LogPosition,
    /// The refused command's id.
    pub command_id: Option<[u8; 16]>,
    /// The transaction concerned.
    pub txn_id: TransactionId,
    /// Why it was refused.
    pub reason: UniqueRejectionReason,
}

/// The replicated state of one unique-index tablet group: the 2PC intent
/// records plus the persistent claim table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniqueIndexState {
    /// The tablet's current schema version (prepare validates against it).
    pub schema_version: SchemaVersion,
    /// The tablet's current authorization version (prepare validates
    /// against it).
    pub authz_version: u64,
    /// Per-transaction intent records (including resolution tombstones).
    pub txns: BTreeMap<TransactionId, ParticipantTxn>,
    /// The committed claim table: claim key -> `(table, pk)`. Claims are
    /// insert-only at this layer; releasing a claim when its row deletes
    /// binds with the engine wave (see the module docs).
    #[serde(with = "vec_map_serde")]
    pub claims: BTreeMap<Vec<u8>, CommittedClaim>,
    /// Writes made visible by committed resolutions, in apply order (the
    /// resolution contract, mirroring
    /// [`crate::dist_txn::IntentState::committed_writes`]).
    pub committed_writes: Vec<CommittedWrite>,
    /// Bounded journal of refused prepares (newest last).
    pub rejections: VecDeque<UniqueRejection>,
}

/// Serde adapter for byte-keyed maps: JSON object keys must be strings, so
/// these maps persist as a flat array of `(key, value)` entries. BTreeMap
/// order is deterministic, so checkpoints and snapshots encode
/// byte-identically on every replica.
mod vec_map_serde {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<K, V, S>(map: &BTreeMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        K: Serialize + Ord,
        V: Serialize,
        S: Serializer,
    {
        map.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, K, V, D>(deserializer: D) -> Result<BTreeMap<K, V>, D::Error>
    where
        K: Deserialize<'de> + Ord,
        V: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        let entries: Vec<(K, V)> = Vec::deserialize(deserializer)?;
        Ok(entries.into_iter().collect())
    }
}

impl Default for UniqueIndexState {
    fn default() -> Self {
        UniqueIndexState {
            schema_version: SchemaVersion::ZERO,
            authz_version: 0,
            txns: BTreeMap::new(),
            claims: BTreeMap::new(),
            committed_writes: Vec::new(),
            rejections: VecDeque::new(),
        }
    }
}

/// The unique-index sink's durable checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UniqueIndexCheckpoint {
    format_version: u32,
    position: LogPosition,
    command_id: Option<[u8; 16]>,
    state: UniqueIndexState,
}

/// Apply sink of a global unique-index tablet group: applies the intent
/// protocol of [`crate::dist_txn`] (same envelope type and commands) to
/// [`UniqueIndexState`], checkpointed under `<group dir>/raft/state`.
///
/// The sink differs from a plain intent group in exactly one way:
/// `PersistIntents` additionally validates every intent as a unique-key
/// claim against (a) the unresolved intents of other transactions
/// (first-preparer-wins) and (b) the persistent claim table — the
/// committed-claim half of spec section 12.9's "conflict detection at
/// prepare". Both checks run inside the apply path, in the raft total
/// order, so concurrent claims on different nodes serialize and exactly
/// one commits.
///
/// Apply is deterministic and total: refusals are journaled, genuine
/// faults (undecodable payload, wrong envelope type, a conflicting
/// decision on an already-resolved transaction) fail closed.
pub struct UniqueIndexApplySink {
    state: UniqueIndexState,
    position: LogPosition,
    command_id: Option<[u8; 16]>,
    /// `<group dir>/raft/state`.
    state_dir: std::path::PathBuf,
}

/// Whether two write sets carry the same keys and value references (the
/// prepare timestamp is stamped per proposal and excluded from the
/// idempotency comparison; same rule as `dist_txn`).
fn same_write_set(a: &[WriteIntent], b: &[WriteIntent]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(left, right)| left.key == right.key && left.value_ref == right.value_ref)
}

impl UniqueIndexApplySink {
    /// Opens (creating if needed) the sink under `group_dir`, loading the
    /// persisted checkpoint when present. A present but undecodable or
    /// unsupported-version checkpoint fails closed (spec section 4.10).
    pub fn open(group_dir: &Path) -> Result<Self, GlobalConstraintError> {
        let state_dir = group_dir.join("raft").join("state");
        std::fs::create_dir_all(&state_dir).map_err(GlobalConstraintError::Io)?;
        let checkpoint_path = state_dir.join(UNIQUE_INDEX_CHECKPOINT_FILENAME);
        let Some(bytes) =
            crate::node::read_meta_file(&checkpoint_path).map_err(|error| match error {
                crate::node::ClusterError::Io(error) => GlobalConstraintError::Io(error),
                other => GlobalConstraintError::CorruptCheckpoint(other.to_string()),
            })?
        else {
            return Ok(UniqueIndexApplySink {
                state: UniqueIndexState::default(),
                position: LogPosition::ZERO,
                command_id: None,
                state_dir,
            });
        };
        let checkpoint: UniqueIndexCheckpoint =
            serde_json::from_slice(&bytes).map_err(|error| {
                GlobalConstraintError::CorruptCheckpoint(format!("decode: {error}"))
            })?;
        if !(MIN_SUPPORTED_CONSTRAINT_CHECKPOINT_FORMAT_VERSION
            ..=CONSTRAINT_CHECKPOINT_FORMAT_VERSION)
            .contains(&checkpoint.format_version)
        {
            return Err(GlobalConstraintError::CorruptCheckpoint(format!(
                "unsupported format version {} (supported \
                 {MIN_SUPPORTED_CONSTRAINT_CHECKPOINT_FORMAT_VERSION}..=\
                 {CONSTRAINT_CHECKPOINT_FORMAT_VERSION})",
                checkpoint.format_version
            )));
        }
        Ok(UniqueIndexApplySink {
            state: checkpoint.state,
            position: checkpoint.position,
            command_id: checkpoint.command_id,
            state_dir,
        })
    }

    /// The current replicated state.
    pub fn state(&self) -> &UniqueIndexState {
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

    /// The committed claim for one claim key, if any.
    pub fn claim(&self, key: &[u8]) -> Option<&CommittedClaim> {
        self.state.claims.get(key)
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

    fn checkpoint(&self) -> UniqueIndexCheckpoint {
        UniqueIndexCheckpoint {
            format_version: CONSTRAINT_CHECKPOINT_FORMAT_VERSION,
            position: self.position,
            command_id: self.command_id,
            state: self.state.clone(),
        }
    }

    fn persist(&self) -> Result<(), StateMachineError> {
        let bytes = serde_json::to_vec(&self.checkpoint()).map_err(|error| {
            StateMachineError::Sink(format!("unique-index checkpoint encode: {error}"))
        })?;
        crate::node::write_meta_atomic(&self.state_dir, UNIQUE_INDEX_CHECKPOINT_FILENAME, &bytes)
            .map_err(|error| {
                StateMachineError::Sink(format!("unique-index checkpoint write: {error}"))
            })
    }

    fn journal(
        &mut self,
        command: &AppliedCommand,
        txn_id: TransactionId,
        reason: UniqueRejectionReason,
    ) {
        self.state.rejections.push_back(UniqueRejection {
            position: command.position,
            command_id: command.command_id(),
            txn_id,
            reason,
        });
        while self.state.rejections.len() > CONSTRAINT_REJECTION_LIMIT {
            self.state.rejections.pop_front();
        }
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
                    UniqueRejectionReason::AlreadyResolved { decision },
                );
                return Ok(());
            }
            if same_write_set(&existing.intents, intents) {
                // Replay of the original prepare: the stored (original)
                // prepare timestamp stands.
                return Ok(());
            }
            self.journal(command, txn_id, UniqueRejectionReason::PayloadMismatch);
            return Ok(());
        }
        // Prepare step 1: validate schema and authorization versions.
        if expected_schema_version != self.state.schema_version {
            let found = self.state.schema_version;
            self.journal(
                command,
                txn_id,
                UniqueRejectionReason::StaleSchemaVersion {
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
                UniqueRejectionReason::StaleAuthzVersion {
                    expected: expected_authz_version,
                    found,
                },
            );
            return Ok(());
        }
        // Prepare step 2: claim validation in the raft total order. Every
        // intent on this tablet is a unique-key claim.
        let mut staged_claims = Vec::with_capacity(intents.len());
        for intent in intents {
            let Ok(value) = UniqueClaimValue::decode(&intent.value_ref) else {
                self.journal(
                    command,
                    txn_id,
                    UniqueRejectionReason::MalformedClaim {
                        key: intent.key.clone(),
                    },
                );
                return Ok(());
            };
            if let Some(holder) = self.conflicting_holder(&txn_id, &intent.key) {
                self.journal(
                    command,
                    txn_id,
                    UniqueRejectionReason::KeyConflict {
                        key: intent.key.clone(),
                        holder,
                    },
                );
                return Ok(());
            }
            if let Some(claim) = self.state.claims.get(&intent.key) {
                if claim.value != value {
                    let detail = format!(
                        "claimed by transaction {} for table {} pk {:02x?}",
                        claim.txn_id,
                        claim.value.table.get(),
                        claim.value.pk
                    );
                    self.journal(
                        command,
                        txn_id,
                        UniqueRejectionReason::ClaimHeld {
                            key: intent.key.clone(),
                            holder: claim.txn_id,
                            detail,
                        },
                    );
                    return Ok(());
                }
                // Identical claim (same table and pk): the row is being
                // re-written by a new transaction — the claim carries over.
            }
            staged_claims.push((intent.key.clone(), value));
        }
        // Prepare step 3: persist the intents (durable before the
        // response). The claim itself materializes only at resolve-commit.
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
        // Every resolution this sink records is materialized in the same
        // apply (claims inserted / intents dropped), so `applied` is set
        // with the resolution and a replayed resolve never applies twice
        // (the identical-decision arm returns first).
        let resolved_at = command.commit_ts();
        match self.state.txns.get_mut(&txn_id) {
            None => {
                // Resolution of a transaction this participant never
                // prepared: record a tombstone so a late prepare loses the
                // race against the decision (resolution always wins).
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
                (Some(prior), new) if *prior == *new => Ok(()),
                (Some(prior), new) => Err(StateMachineError::Corrupt(format!(
                    "conflicting resolve decisions for transaction {txn_id}: \
                     applied {prior:?}, got {new:?}"
                ))),
                (None, TxnDecision::Committed { commit_ts }) => {
                    let commit_ts = *commit_ts;
                    let intents = std::mem::take(&mut existing.intents);
                    let mut writes = Vec::with_capacity(intents.len());
                    for intent in intents {
                        // The claim materializes: the persistent table is
                        // what later prepares check (prepare step 2).
                        if let Ok(value) = UniqueClaimValue::decode(&intent.value_ref) {
                            self.state.claims.insert(
                                intent.key.clone(),
                                CommittedClaim {
                                    key: intent.key.clone(),
                                    value,
                                    txn_id,
                                    commit_ts,
                                },
                            );
                        }
                        writes.push(CommittedWrite {
                            key: intent.key,
                            value_ref: intent.value_ref,
                            commit_ts,
                            txn_id,
                        });
                    }
                    existing.resolution = Some(decision.clone());
                    existing.applied = true;
                    existing.resolved_at = resolved_at;
                    self.state.committed_writes.extend(writes);
                    Ok(())
                }
                (None, TxnDecision::Aborted { .. }) => {
                    existing.intents.clear();
                    existing.resolution = Some(decision.clone());
                    existing.applied = true;
                    existing.resolved_at = resolved_at;
                    Ok(())
                }
            },
        }
    }

    /// Sweeps resolved intent tombstones (and their `committed_writes`
    /// entries) whose `resolved_at` is older than `older_than`, at most
    /// `limit` per command — the same bounded retention as
    /// [`crate::dist_txn::IntentCommand::SweepResolved`], deterministic in
    /// transaction-id order. The claim table is NEVER swept: claims are
    /// the durable unique index itself, not resolution bookkeeping.
    fn apply_sweep(&mut self, older_than: HlcTimestamp, limit: u32) {
        let swept: Vec<TransactionId> = self
            .state
            .txns
            .iter()
            .filter(|(_, txn)| {
                txn.resolution.is_some() && txn.resolved_at.is_some_and(|at| at < older_than)
            })
            .take(limit as usize)
            .map(|(txn_id, _)| *txn_id)
            .collect();
        for txn_id in &swept {
            self.state.txns.remove(txn_id);
        }
        if !swept.is_empty() {
            self.state
                .committed_writes
                .retain(|write| !swept.contains(&write.txn_id));
        }
    }
}

impl ApplySink for UniqueIndexApplySink {
    fn apply(&mut self, command: &AppliedCommand) -> Result<(), StateMachineError> {
        // Crash-window replay guard (same contract as the dist_txn sinks).
        if command.position.index <= self.position.index {
            return Ok(());
        }
        match &command.command {
            ReplicatedCommand::Transaction(transaction) => {
                transaction.envelope.verify().map_err(|error| {
                    StateMachineError::Corrupt(format!("unique-index envelope: {error}"))
                })?;
                if transaction.envelope.command_type != COMMAND_TYPE_DIST_TXN_INTENT {
                    return Err(StateMachineError::Corrupt(format!(
                        "unique-index command_type {} is not COMMAND_TYPE_DIST_TXN_INTENT",
                        transaction.envelope.command_type
                    )));
                }
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
            }
            ReplicatedCommand::Maintenance(_) | ReplicatedCommand::Noop => {}
            ReplicatedCommand::Catalog(_) => {
                return Err(StateMachineError::Corrupt(
                    "catalog command on a unique-index group".to_owned(),
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
            StateMachineError::Sink(format!("unique-index snapshot encode: {error}"))
        })
    }

    fn install(&mut self, data: &[u8]) -> Result<(), StateMachineError> {
        let checkpoint: UniqueIndexCheckpoint = serde_json::from_slice(data).map_err(|error| {
            StateMachineError::Corrupt(format!("unique-index snapshot decode: {error}"))
        })?;
        if !(MIN_SUPPORTED_CONSTRAINT_CHECKPOINT_FORMAT_VERSION
            ..=CONSTRAINT_CHECKPOINT_FORMAT_VERSION)
            .contains(&checkpoint.format_version)
        {
            return Err(StateMachineError::Corrupt(format!(
                "unsupported unique-index checkpoint format version {} (supported \
                 {MIN_SUPPORTED_CONSTRAINT_CHECKPOINT_FORMAT_VERSION}..=\
                 {CONSTRAINT_CHECKPOINT_FORMAT_VERSION})",
                checkpoint.format_version
            )));
        }
        self.state = checkpoint.state;
        self.position = checkpoint.position;
        self.command_id = checkpoint.command_id;
        self.persist()
    }
}

impl fmt::Debug for UniqueIndexApplySink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UniqueIndexApplySink")
            .field("txns", &self.state.txns.len())
            .field("claims", &self.state.claims.len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Unique-index group wrapper
// ---------------------------------------------------------------------------

/// One member of a global unique-index tablet's raft group: a
/// [`ConsensusGroup`] whose apply sink is a [`UniqueIndexApplySink`], plus
/// the prepare/resolve workflow the constraint driver uses. Mirrors
/// [`crate::dist_txn::IntentGroup`]; the sink is what adds claim
/// validation at prepare.
pub struct UniqueIndexGroup<T: RaftTransport> {
    group: ConsensusGroup<T>,
    sink: Arc<Mutex<UniqueIndexApplySink>>,
    raft_group_id: RaftGroupId,
}

impl<T: RaftTransport> UniqueIndexGroup<T> {
    /// Opens the group's durable state and starts the raft task with a
    /// [`UniqueIndexApplySink`] installed; see
    /// [`crate::dist_txn::IntentGroup::create`].
    pub async fn create(
        mut group_config: GroupConfig,
        raft_group_id: RaftGroupId,
        transport: Arc<T>,
    ) -> Result<Self, GlobalConstraintError> {
        group_config.cluster_name = raft_group_id.to_hex();
        let sink = Arc::new(Mutex::new(UniqueIndexApplySink::open(&group_config.dir)?));
        let group = ConsensusGroup::create(
            group_config,
            transport,
            sink.clone() as Arc<Mutex<dyn ApplySink>>,
        )
        .await?;
        Ok(UniqueIndexGroup {
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
    /// voter set (call on one pristine member).
    pub async fn bootstrap(
        &self,
        members: &[(RaftNodeId, String)],
    ) -> Result<(), GlobalConstraintError> {
        let mut map = BTreeMap::new();
        for (raft_id, address) in members {
            map.insert(*raft_id, basic_node(address)?);
        }
        self.group
            .bootstrap(map)
            .await
            .map_err(GlobalConstraintError::Consensus)
    }

    /// Proposes one intent command (quorum durability) and waits for
    /// commit + apply; see [`crate::dist_txn::IntentGroup::propose`]. A
    /// refused prepare returns its journaled [`UniqueRejectionReason`].
    pub async fn propose(
        &self,
        command_id: [u8; 16],
        command: IntentCommand,
        control: &ExecutionControl,
    ) -> Result<(GroupCommitReceipt, Option<UniqueRejectionReason>), GlobalConstraintError> {
        let payload = IntentCommandRecord::new(command).encode()?;
        let envelope = CommandEnvelope::new(COMMAND_TYPE_DIST_TXN_INTENT, command_id, payload);
        let receipt = self
            .group
            .propose(CommandKind::Transaction, envelope, control)
            .await?;
        // client_write returns after local apply, so the local sink's view
        // already includes this command (or its refusal).
        let rejection = {
            let sink = self.sink.lock().map_err(|_| {
                GlobalConstraintError::InvalidRequest("unique-index sink lock poisoned".to_owned())
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
            .expect("unique-index sink lock poisoned")
            .txn(txn_id)
            .cloned()
    }

    /// The committed claim for one claim key at this node's applied
    /// watermark.
    pub fn claim(&self, key: &[u8]) -> Option<CommittedClaim> {
        self.sink
            .lock()
            .expect("unique-index sink lock poisoned")
            .claim(key)
            .cloned()
    }

    /// Transaction ids holding unresolved intents at this node's applied
    /// watermark (the orphan-sweep input).
    pub fn unresolved_txn_ids(&self) -> Vec<TransactionId> {
        self.sink
            .lock()
            .expect("unique-index sink lock poisoned")
            .unresolved_txn_ids()
    }

    /// The full replicated state at this node's applied watermark.
    pub fn state(&self) -> UniqueIndexState {
        self.sink
            .lock()
            .expect("unique-index sink lock poisoned")
            .state()
            .clone()
    }

    /// Graceful shutdown of the underlying group.
    pub async fn shutdown(&self) -> Result<(), GlobalConstraintError> {
        self.group
            .shutdown()
            .await
            .map_err(GlobalConstraintError::Consensus)
    }

    /// Process-free crash simulation (durability tests): stops the raft
    /// task without the graceful storage close; everything fsynced
    /// survives.
    pub async fn crash(self) {
        self.group.crash().await;
    }
}

impl<T: RaftTransport> MemberView for UniqueIndexGroup<T> {
    fn member_node_id(&self) -> RaftNodeId {
        self.group.node_id()
    }

    fn member_leader(&self) -> Option<RaftNodeId> {
        self.group.metrics().current_leader
    }
}

// ---------------------------------------------------------------------------
// Constraint driver: unique insert and FK-checked child insert flows
// ---------------------------------------------------------------------------

/// A distributed unique-constrained insert request (claim + row in one
/// 2PC).
#[derive(Debug, Clone)]
pub struct UniqueInsert {
    /// The transaction id (minted by the client; never reused).
    pub txn_id: TransactionId,
    /// The client's idempotency key: retries must carry the same key.
    pub idempotency_key: [u8; 16],
    /// The row write on the data tablet (prepare timestamp and transaction
    /// id are stamped by the driver).
    pub row: ParticipantWrites,
    /// The unique-key claim on the unique-index tablet.
    pub claim: UniqueClaim,
    /// Schema version the claim prepare validates against.
    pub expected_claim_schema_version: SchemaVersion,
    /// Authorization version the claim prepare validates against.
    pub expected_claim_authz_version: u64,
    /// Read/write timestamps the transaction observed; `commit_ts` is
    /// chosen strictly greater than all of them (spec section 8.2).
    pub observed: Vec<HlcTimestamp>,
}

/// A distributed foreign-key-checked child insert request (child row +
/// parent probe in one 2PC).
#[derive(Debug, Clone)]
pub struct ChildInsert {
    /// The transaction id (minted by the client; never reused).
    pub txn_id: TransactionId,
    /// The client's idempotency key: retries must carry the same key.
    pub idempotency_key: [u8; 16],
    /// The child row write on the child tablet.
    pub child: ParticipantWrites,
    /// The parent-existence probe on the parent tablet.
    pub probe: FkProbe,
    /// Schema version the parent probe validates against.
    pub parent_expected_schema_version: SchemaVersion,
    /// Authorization version the parent probe validates against.
    pub parent_expected_authz_version: u64,
    /// Read/write timestamps the transaction observed; `commit_ts` is
    /// chosen strictly greater than all of them (spec section 8.2).
    pub observed: Vec<HlcTimestamp>,
}

/// The global-constraint commit driver (spec section 12.9). Stateless:
/// all durable state lives in the groups; the driver composes the
/// distributed-transaction protocol of [`crate::dist_txn`] with the
/// constraint participants. One driver instance can drive any constraint
/// flow from the replicated records.
pub struct GlobalConstraintDriver {
    txn: DistTxnDriver,
}

/// The groups and identity of one unique-insert flow, bundled so the
/// failure paths stay readable.
struct UniqueFlow<'a, T: RaftTransport> {
    /// The transaction-status members coordinating the transaction.
    status: &'a [TxnStatusGroup<T>],
    /// Data-tablet intent members by raft group.
    data_participants: &'a BTreeMap<RaftGroupId, Vec<IntentGroup<T>>>,
    /// The unique-index members.
    unique_members: &'a [UniqueIndexGroup<T>],
    /// The unique-index tablet the claim rides on.
    claim_participant: TxnParticipant,
}

impl GlobalConstraintDriver {
    /// Creates a driver wrapping a [`DistTxnDriver`] on the same
    /// configuration (coordinator selection, partitions, timeouts).
    pub fn new(config: DistTxnConfig) -> Self {
        GlobalConstraintDriver {
            txn: DistTxnDriver::new(config),
        }
    }

    /// The underlying distributed-transaction driver (begin, prepare,
    /// decide, abort, recovery — all reused unchanged).
    pub fn txn_driver(&self) -> &DistTxnDriver {
        &self.txn
    }

    fn config(&self) -> &DistTxnConfig {
        self.txn.config()
    }

    /// Phase 1 for the unique-index tablet: persists the claim intent
    /// through the unique-index group's raft log. Conflict detection
    /// happens here, inside the apply path: an existing committed claim or
    /// a concurrent in-flight claim refuses the prepare and surfaces as
    /// [`GlobalConstraintError::ClaimPrepareRejected`] carrying
    /// [`UniqueRejectionReason::ClaimHeld`] /
    /// [`UniqueRejectionReason::KeyConflict`]. Public so barrier-coordinated
    /// interleavings (and future engine bindings) can drive the steps
    /// directly; an idempotent replay returns the original stored prepare
    /// timestamp.
    pub async fn prepare_claim<T: RaftTransport>(
        &self,
        members: &[UniqueIndexGroup<T>],
        txn_id: &TransactionId,
        writes: &ParticipantWrites,
        control: &ExecutionControl,
    ) -> Result<PrepareToken, GlobalConstraintError> {
        if members.is_empty()
            || members
                .iter()
                .any(|member| member.raft_group_id() != writes.participant.raft_group_id)
        {
            return Err(GlobalConstraintError::InvalidRequest(format!(
                "the supplied unique-index members are not the participant group {}",
                writes.participant.raft_group_id
            )));
        }
        let prepare_ts = self.txn.now().map_err(GlobalConstraintError::Txn)?;
        let mut staged = writes.intents.clone();
        for intent in &mut staged {
            intent.txn_id = *txn_id;
            intent.prepare_ts = prepare_ts;
        }
        let command_id =
            command_id_for(TAG_PREPARE, txn_id, writes.participant.tablet_id.as_bytes());
        let (member, (receipt, rejection)) = retry_across_members(
            members,
            "unique claim prepare",
            self.config(),
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
            return Err(GlobalConstraintError::ClaimPrepareRejected(reason));
        }
        // The stored record is authoritative: an idempotent replay keeps
        // the original prepare timestamp.
        let stored = member.txn(txn_id).ok_or_else(|| {
            GlobalConstraintError::InvalidRequest(
                "claim prepare committed but no intent record is visible".to_owned(),
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
    /// (general path; mirrors `DistTxnDriver`'s private mark step so the
    /// unique-index participant's progress is on the record too).
    async fn mark_prepared<T: RaftTransport>(
        &self,
        status: &[TxnStatusGroup<T>],
        txn_id: &TransactionId,
        token: &PrepareToken,
        observed: HlcTimestamp,
        control: &ExecutionControl,
    ) -> Result<(), GlobalConstraintError> {
        let command_id = command_id_for(TAG_MARK, txn_id, token.tablet_id.as_bytes());
        let (_, (_, rejection)) = retry_across_members(
            status,
            "txn-status mark-preparing",
            self.config(),
            control,
            |member| {
                let control = control.clone();
                async move {
                    member
                        .propose(
                            command_id,
                            crate::dist_txn::CoordinatorCommand::MarkPreparing {
                                txn_id: *txn_id,
                                tablet_id: token.tablet_id,
                                prepare_ts: token.prepare_ts,
                                observed,
                            },
                            &control,
                        )
                        .await
                        .map_err(GlobalConstraintError::Txn)
                }
            },
        )
        .await?;
        match rejection {
            None => Ok(()),
            Some(crate::dist_txn::StatusRejectionReason::DecisionFinal { existing }) => {
                Err(match existing {
                    DistributedTxnState::Aborted { reason } => {
                        GlobalConstraintError::Txn(DistTxnError::Aborted(reason))
                    }
                    state => GlobalConstraintError::InvalidRequest(format!(
                        "mark-preparing raced a final decision: {state:?}"
                    )),
                })
            }
            Some(reason) => Err(GlobalConstraintError::InvalidRequest(format!(
                "mark-preparing refused by the coordinator: {reason}"
            ))),
        }
    }

    /// Broadcasts the durable decision to the unique-index tablet (best
    /// effort: a group that does not answer is left to lazy recovery,
    /// which re-delivers the same deterministic command id — identical to
    /// the data participants' resolve semantics).
    async fn resolve_unique<T: RaftTransport>(
        &self,
        members: &[UniqueIndexGroup<T>],
        txn_id: &TransactionId,
        tablet: &TxnParticipant,
        decision: TxnDecision,
        control: &ExecutionControl,
    ) {
        let command_id = command_id_for(TAG_RESOLVE, txn_id, tablet.tablet_id.as_bytes());
        let _ = retry_across_members(
            members,
            "unique-index resolve",
            self.config(),
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
    }

    /// Aborts the transaction and resolves every participant (data tablets
    /// through the standard driver, the unique-index tablet here), then
    /// returns `error` — unless the record shows a durable commit won the
    /// race, in which case the commit outcome is returned instead (the
    /// durable record, never the local failure, decides).
    async fn abort_with<T: RaftTransport>(
        &self,
        flow: &UniqueFlow<'_, T>,
        txn_id: &TransactionId,
        participants: &[TxnParticipant],
        reason: AbortReason,
        error: GlobalConstraintError,
        control: &ExecutionControl,
    ) -> Result<crate::dist_txn::TxnOutcome, GlobalConstraintError> {
        let state = self
            .txn
            .abort(flow.status, flow.data_participants, txn_id, reason, control)
            .await?;
        match state {
            DistributedTxnState::Committed { commit_ts } => {
                self.resolve_unique(
                    flow.unique_members,
                    txn_id,
                    &flow.claim_participant,
                    TxnDecision::Committed { commit_ts },
                    control,
                )
                .await;
                Ok(crate::dist_txn::TxnOutcome {
                    txn_id: *txn_id,
                    commit_ts,
                    participants: participants.to_vec(),
                    durability: mongreldb_log::commit_log::DurabilityLevel::Quorum,
                })
            }
            DistributedTxnState::Aborted { reason } => {
                self.resolve_unique(
                    flow.unique_members,
                    txn_id,
                    &flow.claim_participant,
                    TxnDecision::Aborted { reason },
                    control,
                )
                .await;
                Err(error)
            }
            state => Err(GlobalConstraintError::Txn(DistTxnError::OutcomeAmbiguous {
                txn_id: *txn_id,
                detail: format!("abort is not durable; record state is {state:?}"),
            })),
        }
    }

    /// The global unique-insert flow (spec section 12.9): one distributed
    /// transaction carrying the row intent on the data tablet and the
    /// unique-key claim intent on the unique-index tablet. Begin, prepare
    /// both participants (the claim prepare is where uniqueness is
    /// decided), record prepare progress, decide, answer after the
    /// decision is durable, then broadcast resolution to both sides. Any
    /// prepare failure persists `Aborted` and surfaces as
    /// [`GlobalConstraintError::UniqueViolation`] (claim conflict) or the
    /// protocol error; any post-fence failure is
    /// [`DistTxnError::OutcomeAmbiguous`] (never a false abort). Re-running
    /// with the same transaction id and idempotency key converges to the
    /// original outcome.
    pub async fn commit_unique_insert<T: RaftTransport>(
        &self,
        status: &[TxnStatusGroup<T>],
        data_participants: &BTreeMap<RaftGroupId, Vec<IntentGroup<T>>>,
        unique_members: &[UniqueIndexGroup<T>],
        request: UniqueInsert,
        control: &ExecutionControl,
    ) -> Result<crate::dist_txn::TxnOutcome, GlobalConstraintError> {
        if request.row.participant == request.claim.index.tablet {
            return Err(GlobalConstraintError::InvalidRequest(
                "the claim and the row name the same tablet: a constraint whose unique key \
                 includes the partition key takes the local fast path, not the global index"
                    .to_owned(),
            ));
        }
        let claim_writes = request.claim.writes(
            request.expected_claim_schema_version,
            request.expected_claim_authz_version,
        );
        let commit_request = CommitRequest {
            txn_id: request.txn_id,
            idempotency_key: request.idempotency_key,
            writes: vec![request.row.clone(), claim_writes.clone()],
            observed: request.observed.clone(),
            first_write_tablet: None,
        };
        let record = self.txn.begin(status, &commit_request, control).await?;
        match record.state {
            // Retry of an already-decided transaction: the original outcome
            // stands; make sure resolution is (still) broadcast to both
            // sides.
            DistributedTxnState::Committed { commit_ts } => {
                self.txn
                    .broadcast_resolve(
                        data_participants,
                        &request.txn_id,
                        &record.participants,
                        TxnDecision::Committed { commit_ts },
                        control,
                    )
                    .await;
                self.resolve_unique(
                    unique_members,
                    &request.txn_id,
                    &request.claim.index.tablet,
                    TxnDecision::Committed { commit_ts },
                    control,
                )
                .await;
                return Ok(crate::dist_txn::TxnOutcome {
                    txn_id: request.txn_id,
                    commit_ts,
                    participants: record.participants.clone(),
                    durability: mongreldb_log::commit_log::DurabilityLevel::Quorum,
                });
            }
            DistributedTxnState::Aborted { reason } => {
                self.txn
                    .broadcast_resolve(
                        data_participants,
                        &request.txn_id,
                        &record.participants,
                        TxnDecision::Aborted {
                            reason: reason.clone(),
                        },
                        control,
                    )
                    .await;
                self.resolve_unique(
                    unique_members,
                    &request.txn_id,
                    &request.claim.index.tablet,
                    TxnDecision::Aborted {
                        reason: reason.clone(),
                    },
                    control,
                )
                .await;
                return Err(GlobalConstraintError::Txn(DistTxnError::Aborted(reason)));
            }
            _ => {}
        }
        let flow = UniqueFlow {
            status,
            data_participants,
            unique_members,
            claim_participant: request.claim.index.tablet,
        };
        let mut prepared = Vec::with_capacity(2);
        let mut observed = request.observed.clone();
        // Phase 1a: the data tablet's row intents (standard intent group).
        let data_members = data_participants
            .get(&request.row.participant.raft_group_id)
            .ok_or_else(|| {
                GlobalConstraintError::InvalidRequest(format!(
                    "no intent members supplied for participant group {}",
                    request.row.participant.raft_group_id
                ))
            })?;
        let data_token = match self
            .txn
            .prepare_participant(data_members, &request.txn_id, &request.row, control)
            .await
        {
            Ok(token) => token,
            Err(error) => {
                let error = GlobalConstraintError::Txn(error);
                let reason = abort_reason_of(&error);
                return self
                    .abort_with(
                        &flow,
                        &request.txn_id,
                        &record.participants,
                        reason,
                        error,
                        control,
                    )
                    .await;
            }
        };
        observed.push(data_token.prepare_ts);
        prepared.push(data_token);
        // Phase 1b: the unique-key claim (conflict detection at prepare).
        let claim_token = match self
            .prepare_claim(unique_members, &request.txn_id, &claim_writes, control)
            .await
        {
            Ok(token) => token,
            Err(error) => {
                let reason = abort_reason_of(&error);
                let error = match error {
                    GlobalConstraintError::ClaimPrepareRejected(
                        reason @ (UniqueRejectionReason::KeyConflict { .. }
                        | UniqueRejectionReason::ClaimHeld { .. }),
                    ) => GlobalConstraintError::UniqueViolation {
                        table: request.claim.index.table,
                        constraint: request.claim.index.constraint.clone(),
                        detail: reason.to_string(),
                    },
                    other => other,
                };
                return self
                    .abort_with(
                        &flow,
                        &request.txn_id,
                        &record.participants,
                        reason,
                        error,
                        control,
                    )
                    .await;
            }
        };
        observed.push(claim_token.prepare_ts);
        prepared.push(claim_token);
        // Record prepare progress for both participants (general path).
        for token in &prepared {
            let max_observed = observed.iter().copied().max().unwrap_or(HlcTimestamp::ZERO);
            if let Err(error) = self
                .mark_prepared(status, &request.txn_id, token, max_observed, control)
                .await
            {
                let reason = abort_reason_of(&error);
                return self
                    .abort_with(
                        &flow,
                        &request.txn_id,
                        &record.participants,
                        reason,
                        error,
                        control,
                    )
                    .await;
            }
        }
        // Phase 2: the driver's durable decision (commit_ts strictly above
        // every observed timestamp), then resolve both sides.
        let outcome = self
            .txn
            .decide_commit(
                status,
                data_participants,
                &request.txn_id,
                &prepared,
                &observed,
                control,
            )
            .await;
        match outcome {
            Ok(outcome) => {
                self.resolve_unique(
                    unique_members,
                    &request.txn_id,
                    &flow.claim_participant,
                    TxnDecision::Committed {
                        commit_ts: outcome.commit_ts,
                    },
                    control,
                )
                .await;
                Ok(outcome)
            }
            Err(error) => {
                if let DistTxnError::Aborted(reason) = &error {
                    self.resolve_unique(
                        unique_members,
                        &request.txn_id,
                        &flow.claim_participant,
                        TxnDecision::Aborted {
                            reason: reason.clone(),
                        },
                        control,
                    )
                    .await;
                }
                Err(GlobalConstraintError::Txn(error))
            }
        }
    }

    /// The cross-tablet foreign-key flow (spec section 12.9): the child
    /// row write and the parent-existence probe ride ONE distributed
    /// transaction (distributed transactions + replicated intents). The
    /// parent existence read binds through the [`ParentExistence`] seam
    /// and is checked before the commit fence; a concurrent parent delete
    /// conflicts with the probe intent at prepare through the standard
    /// first-preparer-wins machinery, so insert-versus-delete serializes
    /// and exactly one side commits.
    pub async fn commit_child_insert<T: RaftTransport, O: ParentExistence>(
        &self,
        status: &[TxnStatusGroup<T>],
        participants: &BTreeMap<RaftGroupId, Vec<IntentGroup<T>>>,
        oracle: &O,
        request: ChildInsert,
        control: &ExecutionControl,
    ) -> Result<crate::dist_txn::TxnOutcome, GlobalConstraintError> {
        if request.child.participant == request.probe.parent {
            return Err(GlobalConstraintError::InvalidRequest(
                "the child and the parent name the same tablet: a colocated foreign key \
                 takes the local fast path, not the distributed path"
                    .to_owned(),
            ));
        }
        if !oracle.parent_exists(&request.probe) {
            return Err(GlobalConstraintError::ForeignKeyViolation {
                child_table: request.probe.child_table,
                detail: format!(
                    "no row {} on parent table {} tablet {}",
                    hex(&request.probe.parent_key),
                    request.probe.parent_table.get(),
                    request.probe.parent.tablet_id,
                ),
            });
        }
        let commit_request = CommitRequest {
            txn_id: request.txn_id,
            idempotency_key: request.idempotency_key,
            writes: vec![
                request.child,
                request.probe.writes(
                    request.parent_expected_schema_version,
                    request.parent_expected_authz_version,
                ),
            ],
            observed: request.observed,
            first_write_tablet: None,
        };
        Ok(self
            .txn
            .commit(status, participants, commit_request, control)
            .await?)
    }
}

/// Lowercase hex rendering for diagnostics (avoids a formatting dependency
/// for byte keys).
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

// ---------------------------------------------------------------------------
// Foreign keys: fast-path selection and the parent probe
// ---------------------------------------------------------------------------

/// How a foreign key is enforced (spec section 12.9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FkEnforcementPath {
    /// Parent and child are colocated on one tablet: the tablet's local
    /// check decides (no distributed work). The engine binding performs
    /// the local check; this layer is never entered.
    Local,
    /// Parent and child live on different tablets: the child insert runs
    /// one distributed transaction with a probe intent on the parent
    /// tablet ([`GlobalConstraintDriver::commit_child_insert`]).
    Distributed,
}

/// Selects the enforcement path for one foreign key: `Local` exactly when
/// the parent and child rows are colocated on the same tablet (spec
/// section 12.9 "colocated parent and child").
pub fn fk_enforcement_path(child_tablet: &TabletId, parent_tablet: &TabletId) -> FkEnforcementPath {
    if child_tablet == parent_tablet {
        FkEnforcementPath::Local
    } else {
        FkEnforcementPath::Distributed
    }
}

/// The value reference of a probe intent: a lock marker (never a row
/// payload), naming the referencing child for diagnostics. The engine
/// binding materializes it as a no-op lock write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FkProbeValue {
    /// Discriminant: this value reference is a foreign-key probe, not a
    /// row payload.
    pub foreign_key_probe: bool,
    /// The referencing child table.
    pub child_table: TableId,
    /// The referencing child row's key.
    pub child_key: Vec<u8>,
}

/// A parent-existence probe: the replicated lock/intent half of a
/// cross-tablet foreign key (spec section 12.9). The probe intent rides
/// the child insert's transaction on the parent row's own key, so a
/// concurrent parent delete — which writes an intent on the same key —
/// conflicts with it at prepare (first-preparer-wins) and exactly one
/// transaction commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FkProbe {
    /// The parent table.
    pub parent_table: TableId,
    /// The parent tablet (with its raft group).
    pub parent: TxnParticipant,
    /// The parent row's tablet-local key (the intent key: the probe locks
    /// the parent row itself).
    pub parent_key: Vec<u8>,
    /// The child table inserting the referencing row.
    pub child_table: TableId,
    /// The child row's key (diagnostics in the probe marker).
    pub child_key: Vec<u8>,
}

impl FkProbe {
    /// The probe as a [`ParticipantWrites`] entry for the 2PC (prepare
    /// timestamp and transaction id are stamped by the driver).
    pub fn writes(
        &self,
        expected_schema_version: SchemaVersion,
        expected_authz_version: u64,
    ) -> ParticipantWrites {
        let value = FkProbeValue {
            foreign_key_probe: true,
            child_table: self.child_table,
            child_key: self.child_key.clone(),
        };
        ParticipantWrites {
            participant: self.parent,
            expected_schema_version,
            expected_authz_version,
            intents: vec![WriteIntent {
                txn_id: TransactionId::ZERO,
                key: self.parent_key.clone(),
                value_ref: serde_json::to_vec(&value).expect("FkProbeValue serialization is total"),
                prepare_ts: HlcTimestamp::ZERO,
            }],
        }
    }
}

/// The parent-existence seam (the engine-bound half of a cross-tablet
/// foreign key): whether the probed parent row currently exists. In
/// production this is the tablet server's row read at the transaction's
/// snapshot; in tests it is an in-memory oracle. The check runs before the
/// commit fence; the probe intent closes the race against concurrent
/// parent deletes between the check and the commit.
pub trait ParentExistence {
    /// Whether the probed parent row exists.
    fn parent_exists(&self, probe: &FkProbe) -> bool;
}

// ---------------------------------------------------------------------------
// Cascades: bounded graph walks applied in one distributed transaction
// ---------------------------------------------------------------------------

/// Which cascade bound tripped (spec section 12.9 lists exactly these
/// five).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum CascadeBoundKind {
    /// Total affected rows across every level.
    #[error("maximum rows")]
    Rows,
    /// Distinct tablets the cascade touches.
    #[error("maximum tablets")]
    Tablets,
    /// Foreign-key graph depth (the root is depth 0).
    #[error("maximum depth")]
    Depth,
    /// Abstract work budget (one unit per graph probe and per row seen).
    #[error("work budget")]
    Work,
    /// Wall-clock deadline measured from the walk's start.
    #[error("deadline")]
    Deadline,
}

/// The five cascade bounds of spec section 12.9. Every bound is enforced
/// during planning, BEFORE any write is proposed: a tripped bound fails
/// the whole cascade with
/// [`GlobalConstraintError::CascadeExhausted`] and nothing is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CascadeBounds {
    /// Maximum total affected rows (root included) across all levels.
    pub max_rows: usize,
    /// Maximum distinct tablets the cascade may touch.
    pub max_tablets: usize,
    /// Maximum foreign-key depth below the root (a row deeper than this
    /// trips the bound).
    pub max_depth: usize,
    /// Work budget: one unit per graph probe plus one per row seen.
    pub work_budget: u64,
    /// Wall-clock budget measured from the walk's start
    /// ([`CascadeExecutor::plan`] checks it before every level expansion;
    /// [`CascadeExecutor::execute`] re-checks it at the commit fence).
    pub deadline: Duration,
}

impl Default for CascadeBounds {
    fn default() -> Self {
        CascadeBounds {
            max_rows: 10_000,
            max_tablets: 64,
            max_depth: 16,
            work_budget: 100_000,
            deadline: Duration::from_secs(30),
        }
    }
}

/// One row reference inside a cascade walk (the row to delete).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CascadeRowRef {
    /// The row's table.
    pub table: TableId,
    /// The tablet owning the row (with its raft group).
    pub participant: TxnParticipant,
    /// The row's tablet-local key.
    pub key: Vec<u8>,
}

/// One depth level of a planned cascade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CascadeLevel {
    /// The depth below the root (the root is depth 0).
    pub depth: usize,
    /// The rows deleted at this depth.
    pub rows: Vec<CascadeRowRef>,
}

/// The result of a bounded cascade walk: every level the transaction will
/// apply, plus the measured consumption the bounds were checked against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CascadePlan {
    /// The row the cascade started from (deleted at depth 0).
    pub root: CascadeRowRef,
    /// One entry per depth, in apply order (depth 0 first).
    pub levels: Vec<CascadeLevel>,
    /// Total affected rows (root included).
    pub total_rows: usize,
    /// Distinct tablets touched.
    pub tablet_count: usize,
    /// Work units consumed by the walk.
    pub work_units: u64,
}

/// The value reference of a cascade delete intent: names the root
/// transaction's target so the engine binding can materialize the delete
/// (and distinguish it from a user write).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CascadeDeleteValue {
    /// Discriminant: this value reference is a cascade delete, not a user
    /// row payload.
    pub cascade_delete: bool,
    /// The deleted row's table.
    pub table: TableId,
}

impl CascadePlan {
    /// The plan as per-participant write sets for one distributed
    /// transaction: every level's deletes, grouped by tablet (each tablet
    /// appears at most once, as the protocol requires).
    pub fn participant_writes(
        &self,
        expected_schema_version: SchemaVersion,
        expected_authz_version: u64,
    ) -> Vec<ParticipantWrites> {
        let mut by_tablet: BTreeMap<TabletId, (TxnParticipant, Vec<WriteIntent>)> = BTreeMap::new();
        for level in &self.levels {
            for row in &level.rows {
                let value = CascadeDeleteValue {
                    cascade_delete: true,
                    table: row.table,
                };
                by_tablet
                    .entry(row.participant.tablet_id)
                    .or_insert_with(|| (row.participant, Vec::new()))
                    .1
                    .push(WriteIntent {
                        txn_id: TransactionId::ZERO,
                        key: row.key.clone(),
                        value_ref: serde_json::to_vec(&value)
                            .expect("CascadeDeleteValue serialization is total"),
                        prepare_ts: HlcTimestamp::ZERO,
                    });
            }
        }
        by_tablet
            .into_values()
            .map(|(participant, intents)| ParticipantWrites {
                participant,
                expected_schema_version,
                expected_authz_version,
                intents,
            })
            .collect()
    }
}

/// The cascade-graph seam (the engine-bound half of a cascade): the direct
/// children referencing one parent row. In production this is the tablet
/// server's foreign-key index probe at the transaction's snapshot; in
/// tests it is an in-memory graph. Implementations must return each child
/// at most once and must not mutate the graph.
pub trait CascadeGraph {
    /// The rows directly referencing `parent` (one level of the walk).
    fn children_of(
        &mut self,
        parent: &CascadeRowRef,
    ) -> Result<Vec<CascadeRowRef>, GlobalConstraintError>;
}

/// A cascade execution request: the transaction identity plus the walk
/// root and the versions every participant validates at prepare.
#[derive(Debug, Clone)]
pub struct CascadeRequest {
    /// The transaction id (minted by the client; never reused).
    pub txn_id: TransactionId,
    /// The client's idempotency key: retries must carry the same key.
    pub idempotency_key: [u8; 16],
    /// The row the cascade starts from (deleted at depth 0).
    pub root: CascadeRowRef,
    /// Schema version every participant validates at prepare.
    pub expected_schema_version: SchemaVersion,
    /// Authorization version every participant validates at prepare.
    pub expected_authz_version: u64,
    /// When the walk started (the deadline bound measures from here).
    pub started: Instant,
}

/// The bounded cascade executor (spec section 12.9): walks the
/// foreign-key graph under the five [`CascadeBounds`], then applies every
/// level in ONE distributed transaction — a cascade never partially
/// applies. Bound enforcement happens entirely in
/// [`CascadeExecutor::plan`], before any write is proposed.
pub struct CascadeExecutor<'g, G: CascadeGraph> {
    graph: &'g mut G,
    bounds: CascadeBounds,
}

impl<'g, G: CascadeGraph> CascadeExecutor<'g, G> {
    /// Creates an executor over `graph` under `bounds`.
    pub fn new(graph: &'g mut G, bounds: CascadeBounds) -> Self {
        CascadeExecutor { graph, bounds }
    }

    fn exhausted(&self, bound: CascadeBoundKind, detail: String) -> GlobalConstraintError {
        GlobalConstraintError::CascadeExhausted { bound, detail }
    }

    /// Walks the graph breadth-first from `root`, enforcing every bound,
    /// and returns the plan. Cycle-safe: a row already visited (by table
    /// and key) is not re-walked, so a cyclic foreign-key graph terminates
    /// — and the depth bound caps pathological shapes regardless. No write
    /// is proposed here; a tripped bound leaves the cluster untouched.
    pub fn plan(
        &mut self,
        root: &CascadeRowRef,
        started: Instant,
    ) -> Result<CascadePlan, GlobalConstraintError> {
        let mut visited: BTreeSet<(TableId, Vec<u8>)> = BTreeSet::new();
        let mut tablets: BTreeSet<TabletId> = BTreeSet::new();
        let mut levels: Vec<CascadeLevel> = vec![CascadeLevel {
            depth: 0,
            rows: vec![root.clone()],
        }];
        visited.insert((root.table, root.key.clone()));
        tablets.insert(root.participant.tablet_id);
        let mut total_rows = 1_usize;
        if total_rows > self.bounds.max_rows {
            return Err(self.exhausted(
                CascadeBoundKind::Rows,
                format!("row count exceeds the bound of {}", self.bounds.max_rows),
            ));
        }
        if tablets.len() > self.bounds.max_tablets {
            return Err(self.exhausted(
                CascadeBoundKind::Tablets,
                format!(
                    "tablet count exceeds the bound of {}",
                    self.bounds.max_tablets
                ),
            ));
        }
        let mut work = 0_u64;
        let mut frontier = vec![root.clone()];
        let mut depth = 1_usize;
        loop {
            if started.elapsed() >= self.bounds.deadline {
                return Err(self.exhausted(
                    CascadeBoundKind::Deadline,
                    format!(
                        "deadline of {:?} elapsed before depth {depth} was planned",
                        self.bounds.deadline
                    ),
                ));
            }
            let mut next: Vec<CascadeRowRef> = Vec::new();
            let mut level_rows: Vec<CascadeRowRef> = Vec::new();
            for parent in &frontier {
                work = work.saturating_add(1);
                if work > self.bounds.work_budget {
                    return Err(self.exhausted(
                        CascadeBoundKind::Work,
                        format!("work budget of {} units exceeded", self.bounds.work_budget),
                    ));
                }
                for child in self.graph.children_of(parent)? {
                    work = work.saturating_add(1);
                    if work > self.bounds.work_budget {
                        return Err(self.exhausted(
                            CascadeBoundKind::Work,
                            format!("work budget of {} units exceeded", self.bounds.work_budget),
                        ));
                    }
                    if !visited.insert((child.table, child.key.clone())) {
                        // Already walked (a cycle or a diamond): counted
                        // once, deleted once.
                        continue;
                    }
                    total_rows += 1;
                    if total_rows > self.bounds.max_rows {
                        return Err(self.exhausted(
                            CascadeBoundKind::Rows,
                            format!("row count exceeds the bound of {}", self.bounds.max_rows),
                        ));
                    }
                    tablets.insert(child.participant.tablet_id);
                    if tablets.len() > self.bounds.max_tablets {
                        return Err(self.exhausted(
                            CascadeBoundKind::Tablets,
                            format!(
                                "tablet count exceeds the bound of {}",
                                self.bounds.max_tablets
                            ),
                        ));
                    }
                    next.push(child.clone());
                    level_rows.push(child);
                }
            }
            if level_rows.is_empty() {
                break;
            }
            if depth > self.bounds.max_depth {
                return Err(self.exhausted(
                    CascadeBoundKind::Depth,
                    format!(
                        "rows at depth {depth} exceed the bound of {}",
                        self.bounds.max_depth
                    ),
                ));
            }
            levels.push(CascadeLevel {
                depth,
                rows: level_rows,
            });
            frontier = next;
            depth += 1;
        }
        Ok(CascadePlan {
            root: root.clone(),
            levels,
            total_rows,
            tablet_count: tablets.len(),
            work_units: work,
        })
    }

    /// Plans the cascade and applies every level in ONE distributed
    /// transaction (the atomicity half of "never partially applies"; the
    /// bound half is [`CascadeExecutor::plan`]). The deadline is
    /// re-checked at the commit fence; after the fence the two-phase
    /// protocol decides the whole plan atomically.
    pub async fn execute<T: RaftTransport>(
        &mut self,
        driver: &DistTxnDriver,
        status: &[TxnStatusGroup<T>],
        participants: &BTreeMap<RaftGroupId, Vec<IntentGroup<T>>>,
        request: &CascadeRequest,
        control: &ExecutionControl,
    ) -> Result<crate::dist_txn::TxnOutcome, GlobalConstraintError> {
        let plan = self.plan(&request.root, request.started)?;
        if request.started.elapsed() >= self.bounds.deadline {
            return Err(self.exhausted(
                CascadeBoundKind::Deadline,
                format!(
                    "deadline of {:?} elapsed at the commit fence",
                    self.bounds.deadline
                ),
            ));
        }
        let commit = CommitRequest {
            txn_id: request.txn_id,
            idempotency_key: request.idempotency_key,
            writes: plan.participant_writes(
                request.expected_schema_version,
                request.expected_authz_version,
            ),
            observed: Vec::new(),
            first_write_tablet: None,
        };
        Ok(driver.commit(status, participants, commit, control).await?)
    }
}

// ---------------------------------------------------------------------------
// Replicated sequences: range grants with a replicated high-water mark
// ---------------------------------------------------------------------------

/// One granted value range (spec section 12.9: "sequence tablet grants
/// `[N, N+999]`"). The record is replicated through the sequence tablet's
/// raft group; the per-sequence high-water mark it advances survives
/// restart, so no value is ever re-issued.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceGrant {
    /// The sequence the range belongs to.
    pub sequence: String,
    /// First value of the range (inclusive).
    pub first: u64,
    /// Last value of the range (inclusive); `last - first + 1` values.
    pub last: u64,
    /// The node the range was granted to.
    pub holder: NodeId,
    /// Grant timestamp (stamped by the proposer, applied identically on
    /// every replica).
    pub granted_at: HlcTimestamp,
    /// The deterministic-or-random command id of the grant (the proposer's
    /// read-back key and the idempotent-apply token).
    pub command_id: [u8; 16],
}

/// One transition of a sequence tablet, replicated through its raft group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SequenceCommand {
    /// Grants the next `width` values of `sequence` to `holder`. The apply
    /// path computes the range from the replicated high-water mark, so the
    /// command is deterministic on every replica. A retry with the same
    /// command id replays the original grant (S2B-004 idempotent apply).
    Grant {
        /// The sequence to advance.
        sequence: String,
        /// Requested range width (`1..=MAX_SEQUENCE_GRANT_WIDTH`;
        /// [`DEFAULT_SEQUENCE_GRANT_WIDTH`] is the spec's `[N, N+999]`).
        width: u64,
        /// The node the range is granted to.
        holder: NodeId,
        /// Grant timestamp (stamped by the proposer).
        granted_at: HlcTimestamp,
    },
}

/// The versioned envelope payload carrying one [`SequenceCommand`] (spec
/// section 4.10). Serialized as JSON into a [`CommandEnvelope`] stamped
/// with [`COMMAND_TYPE_SEQUENCE`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceCommandRecord {
    /// Format version; see [`SEQUENCE_RECORD_FORMAT_VERSION`].
    pub format_version: u32,
    /// The command.
    pub command: SequenceCommand,
}

impl SequenceCommandRecord {
    /// Wraps `command` at the current format version.
    pub fn new(command: SequenceCommand) -> Self {
        SequenceCommandRecord {
            format_version: SEQUENCE_RECORD_FORMAT_VERSION,
            command,
        }
    }

    /// Encodes the record for the envelope payload.
    pub fn encode(&self) -> Result<Vec<u8>, GlobalConstraintError> {
        serde_json::to_vec(self).map_err(|error| GlobalConstraintError::Encode(error.to_string()))
    }

    /// Decodes an envelope payload, failing closed on malformed input and
    /// unsupported versions.
    pub fn decode(payload: &[u8]) -> Result<Self, GlobalConstraintError> {
        let record: SequenceCommandRecord = serde_json::from_slice(payload)
            .map_err(|error| GlobalConstraintError::Encode(format!("decode: {error}")))?;
        if !(MIN_SUPPORTED_SEQUENCE_RECORD_FORMAT_VERSION..=SEQUENCE_RECORD_FORMAT_VERSION)
            .contains(&record.format_version)
        {
            return Err(GlobalConstraintError::Encode(format!(
                "unsupported sequence record version {} (supported \
                 {MIN_SUPPORTED_SEQUENCE_RECORD_FORMAT_VERSION}..=\
                 {SEQUENCE_RECORD_FORMAT_VERSION})",
                record.format_version
            )));
        }
        Ok(record)
    }
}

/// Why the sequence apply path refused a command. Refusals are journaled
/// in [`SequenceAllocatorState::rejections`]; the raft entry commits
/// normally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum SequenceRejectionReason {
    /// The requested width is outside `1..=MAX_SEQUENCE_GRANT_WIDTH`.
    #[error("invalid grant width {width} (allowed 1..={MAX_SEQUENCE_GRANT_WIDTH})")]
    InvalidWidth {
        /// The requested width.
        width: u64,
    },
    /// The high-water mark cannot advance by the requested width (the
    /// `u64` value space is exhausted).
    #[error("sequence {sequence} is exhausted")]
    Exhausted {
        /// The exhausted sequence.
        sequence: String,
    },
}

/// One journaled sequence rejection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceRejection {
    /// Raft position of the refused entry.
    pub position: LogPosition,
    /// The refused command's id.
    pub command_id: Option<[u8; 16]>,
    /// Why it was refused.
    pub reason: SequenceRejectionReason,
}

/// The replicated per-sequence state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceHighWater {
    /// The last value ever granted (the high-water mark; starts at 0, so
    /// the first grant is `[1, width]`). Only ever advances — this is the
    /// "no value re-issued" invariant, and it survives restart through the
    /// checkpoint.
    pub high_water: u64,
    /// The most recent grant, for diagnostics.
    pub last_grant: Option<SequenceGrant>,
}

/// The replicated state of one sequence tablet group.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceAllocatorState {
    /// High-water marks by sequence name.
    pub sequences: BTreeMap<String, SequenceHighWater>,
    /// Granted ranges by command id (the proposer's read-back view).
    #[serde(with = "vec_map_serde")]
    pub grants: BTreeMap<[u8; 16], SequenceGrant>,
    /// Bounded journal of refused grants (newest last).
    pub rejections: VecDeque<SequenceRejection>,
}

/// The sequence sink's durable checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SequenceCheckpoint {
    format_version: u32,
    position: LogPosition,
    command_id: Option<[u8; 16]>,
    state: SequenceAllocatorState,
}

/// Apply sink of a sequence tablet group: applies [`SequenceCommand`]s to
/// [`SequenceAllocatorState`], checkpointed under `<group
/// dir>/raft/state`. Same apply contract as the other replicated sinks:
/// deterministic and total, refusals journaled, genuine faults (an
/// undecodable payload or a wrong envelope type) fail closed.
pub struct SequenceApplySink {
    state: SequenceAllocatorState,
    position: LogPosition,
    command_id: Option<[u8; 16]>,
    /// `<group dir>/raft/state`.
    state_dir: std::path::PathBuf,
}

impl SequenceApplySink {
    /// Opens (creating if needed) the sink under `group_dir`, loading the
    /// persisted checkpoint when present. A present but undecodable or
    /// unsupported-version checkpoint fails closed (spec section 4.10).
    pub fn open(group_dir: &Path) -> Result<Self, GlobalConstraintError> {
        let state_dir = group_dir.join("raft").join("state");
        std::fs::create_dir_all(&state_dir).map_err(GlobalConstraintError::Io)?;
        let checkpoint_path = state_dir.join(SEQUENCE_CHECKPOINT_FILENAME);
        let Some(bytes) =
            crate::node::read_meta_file(&checkpoint_path).map_err(|error| match error {
                crate::node::ClusterError::Io(error) => GlobalConstraintError::Io(error),
                other => GlobalConstraintError::CorruptCheckpoint(other.to_string()),
            })?
        else {
            return Ok(SequenceApplySink {
                state: SequenceAllocatorState::default(),
                position: LogPosition::ZERO,
                command_id: None,
                state_dir,
            });
        };
        let checkpoint: SequenceCheckpoint = serde_json::from_slice(&bytes).map_err(|error| {
            GlobalConstraintError::CorruptCheckpoint(format!("decode: {error}"))
        })?;
        if !(MIN_SUPPORTED_CONSTRAINT_CHECKPOINT_FORMAT_VERSION
            ..=CONSTRAINT_CHECKPOINT_FORMAT_VERSION)
            .contains(&checkpoint.format_version)
        {
            return Err(GlobalConstraintError::CorruptCheckpoint(format!(
                "unsupported format version {} (supported \
                 {MIN_SUPPORTED_CONSTRAINT_CHECKPOINT_FORMAT_VERSION}..=\
                 {CONSTRAINT_CHECKPOINT_FORMAT_VERSION})",
                checkpoint.format_version
            )));
        }
        Ok(SequenceApplySink {
            state: checkpoint.state,
            position: checkpoint.position,
            command_id: checkpoint.command_id,
            state_dir,
        })
    }

    /// The current replicated state.
    pub fn state(&self) -> &SequenceAllocatorState {
        &self.state
    }

    /// The log position the state reflects (the crash-window replay
    /// watermark).
    pub fn applied_position(&self) -> LogPosition {
        self.position
    }

    fn checkpoint(&self) -> SequenceCheckpoint {
        SequenceCheckpoint {
            format_version: CONSTRAINT_CHECKPOINT_FORMAT_VERSION,
            position: self.position,
            command_id: self.command_id,
            state: self.state.clone(),
        }
    }

    fn persist(&self) -> Result<(), StateMachineError> {
        let bytes = serde_json::to_vec(&self.checkpoint()).map_err(|error| {
            StateMachineError::Sink(format!("sequence checkpoint encode: {error}"))
        })?;
        crate::node::write_meta_atomic(&self.state_dir, SEQUENCE_CHECKPOINT_FILENAME, &bytes)
            .map_err(|error| StateMachineError::Sink(format!("sequence checkpoint write: {error}")))
    }

    fn journal(&mut self, command: &AppliedCommand, reason: SequenceRejectionReason) {
        self.state.rejections.push_back(SequenceRejection {
            position: command.position,
            command_id: command.command_id(),
            reason,
        });
        while self.state.rejections.len() > CONSTRAINT_REJECTION_LIMIT {
            self.state.rejections.pop_front();
        }
    }

    fn apply_grant(
        &mut self,
        command: &AppliedCommand,
        sequence: &str,
        width: u64,
        holder: NodeId,
        granted_at: HlcTimestamp,
    ) -> Result<(), StateMachineError> {
        if width == 0 || width > MAX_SEQUENCE_GRANT_WIDTH {
            self.journal(command, SequenceRejectionReason::InvalidWidth { width });
            return Ok(());
        }
        let command_id = command.command_id().unwrap_or([0u8; 16]);
        let high_water = self
            .state
            .sequences
            .get(sequence)
            .map_or(0, |entry| entry.high_water);
        let (Some(first), Some(last)) = (high_water.checked_add(1), high_water.checked_add(width))
        else {
            self.journal(
                command,
                SequenceRejectionReason::Exhausted {
                    sequence: sequence.to_owned(),
                },
            );
            return Ok(());
        };
        let grant = SequenceGrant {
            sequence: sequence.to_owned(),
            first,
            last,
            holder,
            granted_at,
            command_id,
        };
        self.state.sequences.insert(
            sequence.to_owned(),
            SequenceHighWater {
                high_water: last,
                last_grant: Some(grant.clone()),
            },
        );
        self.state.grants.insert(command_id, grant);
        Ok(())
    }
}

impl ApplySink for SequenceApplySink {
    fn apply(&mut self, command: &AppliedCommand) -> Result<(), StateMachineError> {
        // Crash-window replay guard (same contract as the dist_txn sinks).
        if command.position.index <= self.position.index {
            return Ok(());
        }
        match &command.command {
            ReplicatedCommand::Transaction(transaction) => {
                transaction.envelope.verify().map_err(|error| {
                    StateMachineError::Corrupt(format!("sequence envelope: {error}"))
                })?;
                if transaction.envelope.command_type != COMMAND_TYPE_SEQUENCE {
                    return Err(StateMachineError::Corrupt(format!(
                        "sequence command_type {} is not COMMAND_TYPE_SEQUENCE",
                        transaction.envelope.command_type
                    )));
                }
                let record = SequenceCommandRecord::decode(&transaction.envelope.payload)
                    .map_err(|error| StateMachineError::Corrupt(error.to_string()))?;
                match record.command {
                    SequenceCommand::Grant {
                        sequence,
                        width,
                        holder,
                        granted_at,
                    } => {
                        self.apply_grant(command, &sequence, width, holder, granted_at)?;
                    }
                }
            }
            ReplicatedCommand::Maintenance(_) | ReplicatedCommand::Noop => {}
            ReplicatedCommand::Catalog(_) => {
                return Err(StateMachineError::Corrupt(
                    "catalog command on a sequence group".to_owned(),
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
        serde_json::to_vec(&self.checkpoint())
            .map_err(|error| StateMachineError::Sink(format!("sequence snapshot encode: {error}")))
    }

    fn install(&mut self, data: &[u8]) -> Result<(), StateMachineError> {
        let checkpoint: SequenceCheckpoint = serde_json::from_slice(data).map_err(|error| {
            StateMachineError::Corrupt(format!("sequence snapshot decode: {error}"))
        })?;
        if !(MIN_SUPPORTED_CONSTRAINT_CHECKPOINT_FORMAT_VERSION
            ..=CONSTRAINT_CHECKPOINT_FORMAT_VERSION)
            .contains(&checkpoint.format_version)
        {
            return Err(StateMachineError::Corrupt(format!(
                "unsupported sequence checkpoint format version {} (supported \
                 {MIN_SUPPORTED_CONSTRAINT_CHECKPOINT_FORMAT_VERSION}..=\
                 {CONSTRAINT_CHECKPOINT_FORMAT_VERSION})",
                checkpoint.format_version
            )));
        }
        self.state = checkpoint.state;
        self.position = checkpoint.position;
        self.command_id = checkpoint.command_id;
        self.persist()
    }
}

impl fmt::Debug for SequenceApplySink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SequenceApplySink")
            .field("sequences", &self.state.sequences.len())
            .finish()
    }
}

/// One member of a sequence tablet's raft group: a [`ConsensusGroup`]
/// whose apply sink is a [`SequenceApplySink`], plus the grant workflow
/// the allocator uses. Mirrors the other group wrappers.
pub struct SequenceGroup<T: RaftTransport> {
    group: ConsensusGroup<T>,
    sink: Arc<Mutex<SequenceApplySink>>,
    raft_group_id: RaftGroupId,
}

impl<T: RaftTransport> SequenceGroup<T> {
    /// Opens the group's durable state and starts the raft task with a
    /// [`SequenceApplySink`] installed; see
    /// [`crate::dist_txn::TxnStatusGroup::create`].
    pub async fn create(
        mut group_config: GroupConfig,
        raft_group_id: RaftGroupId,
        transport: Arc<T>,
    ) -> Result<Self, GlobalConstraintError> {
        group_config.cluster_name = raft_group_id.to_hex();
        let sink = Arc::new(Mutex::new(SequenceApplySink::open(&group_config.dir)?));
        let group = ConsensusGroup::create(
            group_config,
            transport,
            sink.clone() as Arc<Mutex<dyn ApplySink>>,
        )
        .await?;
        Ok(SequenceGroup {
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
    /// voter set (call on one pristine member).
    pub async fn bootstrap(
        &self,
        members: &[(RaftNodeId, String)],
    ) -> Result<(), GlobalConstraintError> {
        let mut map = BTreeMap::new();
        for (raft_id, address) in members {
            map.insert(*raft_id, basic_node(address)?);
        }
        self.group
            .bootstrap(map)
            .await
            .map_err(GlobalConstraintError::Consensus)
    }

    /// Proposes one sequence command (quorum durability) and waits for
    /// commit + apply; see [`crate::dist_txn::TxnStatusGroup::propose`]. A
    /// refused grant returns its journaled [`SequenceRejectionReason`].
    pub async fn propose(
        &self,
        command_id: [u8; 16],
        command: SequenceCommand,
        control: &ExecutionControl,
    ) -> Result<(GroupCommitReceipt, Option<SequenceRejectionReason>), GlobalConstraintError> {
        let payload = SequenceCommandRecord::new(command).encode()?;
        let envelope = CommandEnvelope::new(COMMAND_TYPE_SEQUENCE, command_id, payload);
        let receipt = self
            .group
            .propose(CommandKind::Transaction, envelope, control)
            .await?;
        let rejection = {
            let sink = self.sink.lock().map_err(|_| {
                GlobalConstraintError::InvalidRequest("sequence sink lock poisoned".to_owned())
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

    /// One grant record at this node's applied watermark (the proposer's
    /// read-back).
    pub fn grant(&self, command_id: &[u8; 16]) -> Option<SequenceGrant> {
        self.sink
            .lock()
            .expect("sequence sink lock poisoned")
            .state()
            .grants
            .get(command_id)
            .cloned()
    }

    /// The high-water mark of one sequence at this node's applied
    /// watermark (`None` if the sequence was never granted).
    pub fn high_water(&self, sequence: &str) -> Option<u64> {
        self.sink
            .lock()
            .expect("sequence sink lock poisoned")
            .state()
            .sequences
            .get(sequence)
            .map(|entry| entry.high_water)
    }

    /// The full replicated state at this node's applied watermark.
    pub fn state(&self) -> SequenceAllocatorState {
        self.sink
            .lock()
            .expect("sequence sink lock poisoned")
            .state()
            .clone()
    }

    /// Graceful shutdown of the underlying group.
    pub async fn shutdown(&self) -> Result<(), GlobalConstraintError> {
        self.group
            .shutdown()
            .await
            .map_err(GlobalConstraintError::Consensus)
    }

    /// Process-free crash simulation (durability tests); see
    /// [`crate::dist_txn::TxnStatusGroup::crash`].
    pub async fn crash(self) {
        self.group.crash().await;
    }
}

impl<T: RaftTransport> MemberView for SequenceGroup<T> {
    fn member_node_id(&self) -> RaftNodeId {
        self.group.node_id()
    }

    fn member_leader(&self) -> Option<RaftNodeId> {
        self.group.metrics().current_leader
    }
}

/// The client-side range consumer (spec section 12.9: "tablet/node
/// consumes locally"). An allocator owns at most one granted range at a
/// time and hands out its values with a strictly monotonic local counter;
/// when the range runs out it draws the next range from the sequence
/// tablet. The allocator's own counter is volatile: a crash or drop loses
/// the unconsumed tail of its range — those values gap, and the replicated
/// high-water mark guarantees they are never re-issued. **Rollback does
/// not guarantee gapless sequences** (spec section 12.9): values handed
/// out inside a transaction that later aborts are not returned.
#[derive(Debug, Clone)]
pub struct SequenceAllocator {
    /// The sequence this allocator draws from.
    sequence: String,
    /// The node this allocator runs on (recorded as the grant holder).
    holder: NodeId,
    /// Requested grant width.
    width: u64,
    /// The next value to hand out (valid only while `has_grant`).
    next_value: u64,
    /// The granted range's last value (inclusive).
    grant_last: u64,
    /// Whether a range is currently held.
    has_grant: bool,
}

impl SequenceAllocator {
    /// Creates an allocator drawing [`DEFAULT_SEQUENCE_GRANT_WIDTH`]-wide
    /// ranges (`[N, N+999]`, spec section 12.9).
    pub fn new(sequence: impl Into<String>, holder: NodeId) -> Self {
        Self::with_width(sequence, holder, DEFAULT_SEQUENCE_GRANT_WIDTH)
    }

    /// Creates an allocator drawing `width`-wide ranges.
    pub fn with_width(sequence: impl Into<String>, holder: NodeId, width: u64) -> Self {
        SequenceAllocator {
            sequence: sequence.into(),
            holder,
            width,
            next_value: 0,
            grant_last: 0,
            has_grant: false,
        }
    }

    /// The sequence this allocator draws from.
    pub fn sequence(&self) -> &str {
        &self.sequence
    }

    /// The next sequence value, drawing a fresh replicated range grant
    /// when the held range is exhausted. Strictly monotonic within this
    /// allocator; disjoint from every other node's allocator because the
    /// high-water mark is replicated. Concurrent `next` calls on the same
    /// allocator are serialized by the `&mut self` receiver.
    pub async fn next<T: RaftTransport>(
        &mut self,
        members: &[SequenceGroup<T>],
        clock: &HlcClock,
        control: &ExecutionControl,
    ) -> Result<u64, GlobalConstraintError> {
        if !self.has_grant || self.next_value > self.grant_last {
            self.refill(members, clock, control).await?;
        }
        let value = self.next_value;
        self.next_value = self.next_value.saturating_add(1);
        Ok(value)
    }

    /// The granted range this allocator is consuming, if any
    /// (`(next_value, grant_last)`; diagnostics).
    pub fn held_range(&self) -> Option<(u64, u64)> {
        self.has_grant.then_some((self.next_value, self.grant_last))
    }

    async fn refill<T: RaftTransport>(
        &mut self,
        members: &[SequenceGroup<T>],
        clock: &HlcClock,
        control: &ExecutionControl,
    ) -> Result<(), GlobalConstraintError> {
        if members.is_empty() {
            return Err(GlobalConstraintError::InvalidRequest(
                "no sequence members supplied".to_owned(),
            ));
        }
        if members
            .iter()
            .any(|member| member.raft_group_id() != members[0].raft_group_id())
        {
            return Err(GlobalConstraintError::InvalidRequest(
                "the supplied sequence members are not one raft group".to_owned(),
            ));
        }
        let granted_at = clock.now().map_err(|error| {
            GlobalConstraintError::InvalidRequest(format!("allocator clock: {error}"))
        })?;
        // One random command id per logical refill: a retry of the same
        // refill re-proposes the same id and the idempotent apply replays
        // the original grant (no double allocation). A brand-new refill
        // draws a fresh range even if an earlier ambiguous attempt
        // actually landed — the spec's documented gap semantics.
        let command_id = crate::meta::new_command_id()
            .map_err(|error| GlobalConstraintError::CommandId(error.to_string()))?;
        let (member, (_, rejection)) = retry_across_members(
            members,
            "sequence grant",
            &DistTxnConfig::default(),
            control,
            |member| {
                let control = control.clone();
                let sequence = self.sequence.clone();
                let width = self.width;
                let holder = self.holder;
                async move {
                    member
                        .propose(
                            command_id,
                            SequenceCommand::Grant {
                                sequence,
                                width,
                                holder,
                                granted_at,
                            },
                            &control,
                        )
                        .await
                }
            },
        )
        .await?;
        if let Some(reason) = rejection {
            return Err(match reason {
                SequenceRejectionReason::Exhausted { sequence } => {
                    GlobalConstraintError::SequenceExhausted {
                        sequence,
                        detail: "the value space is exhausted".to_owned(),
                    }
                }
                other => GlobalConstraintError::SequenceGrantRejected(other),
            });
        }
        let grant = member.grant(&command_id).ok_or_else(|| {
            GlobalConstraintError::InvalidRequest(
                "sequence grant committed but no grant record is visible".to_owned(),
            )
        })?;
        self.next_value = grant.first;
        self.grant_last = grant.last;
        self.has_grant = true;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dist_txn::{CoordinatorSelection, IntentState};
    use crate::meta::TxnStatusPartition;
    use crate::tablet::{Partitioning, TablePartitioningRecord};
    use mongreldb_consensus::network::InMemoryTransport;
    use std::time::Instant;
    use tokio::sync::Barrier;

    // -- small helpers -------------------------------------------------------

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

    fn nid(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    fn participant(group: u8, tablet: u8) -> TxnParticipant {
        TxnParticipant {
            tablet_id: tid(tablet),
            raft_group_id: gid(group),
        }
    }

    fn table(id: u64) -> TableId {
        TableId::new(id)
    }

    fn claim(group: u8, tablet: u8, constraint: &str, key: &[u8], pk: &[u8]) -> UniqueClaim {
        UniqueClaim {
            index: UniqueIndexRef {
                table: table(7),
                constraint: constraint.to_owned(),
                tablet: participant(group, tablet),
            },
            key_bytes: key.to_vec(),
            pk: pk.to_vec(),
        }
    }

    fn row_writes(group: u8, tablet: u8, keys: &[&[u8]]) -> ParticipantWrites {
        ParticipantWrites {
            participant: participant(group, tablet),
            expected_schema_version: SchemaVersion::ZERO,
            expected_authz_version: 0,
            intents: keys
                .iter()
                .map(|key| WriteIntent {
                    txn_id: TransactionId::ZERO,
                    key: key.to_vec(),
                    value_ref: format!("row-{}", String::from_utf8_lossy(key)).into_bytes(),
                    prepare_ts: HlcTimestamp::ZERO,
                })
                .collect(),
        }
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

    fn sequence_envelope(
        id: [u8; 16],
        command: SequenceCommand,
        index_micros: u64,
    ) -> ReplicatedCommand {
        let payload = SequenceCommandRecord::new(command).encode().unwrap();
        ReplicatedCommand::new(
            CommandKind::Transaction,
            CommandEnvelope::new(COMMAND_TYPE_SEQUENCE, id, payload),
            ts(index_micros),
        )
    }

    fn applied(index: u64, command: ReplicatedCommand) -> AppliedCommand {
        AppliedCommand {
            position: LogPosition { term: 1, index },
            command,
        }
    }

    fn persist(
        txn: u8,
        prepare_micros: u64,
        claims: &[(&[u8], &[u8])], // (claim key, pk)
    ) -> IntentCommand {
        IntentCommand::PersistIntents {
            txn_id: xid(txn),
            expected_schema_version: SchemaVersion::ZERO,
            expected_authz_version: 0,
            prepare_ts: ts(prepare_micros),
            intents: claims
                .iter()
                .map(|(key, pk)| WriteIntent {
                    txn_id: xid(txn),
                    key: key.to_vec(),
                    value_ref: UniqueClaimValue {
                        table: table(7),
                        pk: pk.to_vec(),
                    }
                    .encode(),
                    prepare_ts: ts(prepare_micros),
                })
                .collect(),
        }
    }

    fn grant(sequence: &str, width: u64, holder: u8) -> SequenceCommand {
        SequenceCommand::Grant {
            sequence: sequence.to_owned(),
            width,
            holder: nid(holder),
            granted_at: ts(100),
        }
    }

    // -- pure selection + records --------------------------------------------

    #[test]
    fn unique_path_is_local_exactly_when_partition_key_is_inside_unique_key() {
        let record = TablePartitioningRecord::automatic_default(
            table(3),
            vec![ColumnId::new(1), ColumnId::new(2)],
            64,
        );
        let partitioning = record.partitioning;
        // Unique key covers both partition columns: local fast path.
        assert_eq!(
            unique_enforcement_path(
                &partitioning,
                &[ColumnId::new(1), ColumnId::new(2), ColumnId::new(3)]
            ),
            UniqueEnforcementPath::Local
        );
        // Unique key covers exactly the partition columns: local.
        assert_eq!(
            unique_enforcement_path(&partitioning, &[ColumnId::new(1), ColumnId::new(2)]),
            UniqueEnforcementPath::Local
        );
        // A partition column outside the unique key: global index.
        assert_eq!(
            unique_enforcement_path(&partitioning, &[ColumnId::new(1)]),
            UniqueEnforcementPath::GlobalIndex
        );
        // Degenerate: no unique columns at all is not the fast path.
        assert_eq!(
            unique_enforcement_path(&partitioning, &[]),
            UniqueEnforcementPath::GlobalIndex
        );
        // Tenant partitioning: the tenant column is the partition key.
        let tenant = Partitioning::Tenant {
            tenant_column: ColumnId::new(9),
            buckets_per_tenant: 16,
        };
        assert_eq!(
            unique_enforcement_path(&tenant, &[ColumnId::new(9), ColumnId::new(5)]),
            UniqueEnforcementPath::Local
        );
        assert_eq!(
            unique_enforcement_path(&tenant, &[ColumnId::new(5)]),
            UniqueEnforcementPath::GlobalIndex
        );
    }

    #[test]
    fn fk_path_is_local_exactly_when_colocated() {
        assert_eq!(
            fk_enforcement_path(&tid(1), &tid(1)),
            FkEnforcementPath::Local
        );
        assert_eq!(
            fk_enforcement_path(&tid(1), &tid(2)),
            FkEnforcementPath::Distributed
        );
    }

    #[test]
    fn claim_keys_are_deterministic_and_scoped() {
        let first = claim(81, 1, "uq_email", b"alice@example.com", b"pk-1");
        assert_eq!(first.intent_key(), first.intent_key());
        // The key carries table and constraint namespacing.
        let other_constraint = claim(81, 1, "uq_name", b"alice@example.com", b"pk-1");
        assert_ne!(first.intent_key(), other_constraint.intent_key());
        let other_key = claim(81, 1, "uq_email", b"bob@example.com", b"pk-1");
        assert_ne!(first.intent_key(), other_key.intent_key());
        // The pk is not part of the key (claims of one value collide
        // regardless of which row claims it).
        let other_pk = claim(81, 1, "uq_email", b"alice@example.com", b"pk-2");
        assert_eq!(first.intent_key(), other_pk.intent_key());
        // The value carries (table, pk) per spec section 12.9.
        let value = UniqueClaimValue::decode(&first.value_ref()).unwrap();
        assert_eq!(value.table, table(7));
        assert_eq!(value.pk, b"pk-1");
    }

    #[test]
    fn error_categories_map_onto_the_stable_taxonomy() {
        let conflict = GlobalConstraintError::UniqueViolation {
            table: table(7),
            constraint: "uq_email".to_owned(),
            detail: "claimed".to_owned(),
        };
        assert_eq!(conflict.category(), ErrorCategory::TransactionConflict);
        let claim_conflict =
            GlobalConstraintError::ClaimPrepareRejected(UniqueRejectionReason::KeyConflict {
                key: b"k".to_vec(),
                holder: xid(1),
            });
        assert_eq!(
            claim_conflict.category(),
            ErrorCategory::TransactionConflict
        );
        let fk = GlobalConstraintError::ForeignKeyViolation {
            child_table: table(8),
            detail: "missing".to_owned(),
        };
        assert_eq!(fk.category(), ErrorCategory::TransactionAborted);
        let cascade = GlobalConstraintError::CascadeExhausted {
            bound: CascadeBoundKind::Rows,
            detail: "too many".to_owned(),
        };
        assert_eq!(cascade.category(), ErrorCategory::ResourceExhausted);
        let sequence = GlobalConstraintError::SequenceExhausted {
            sequence: "s".to_owned(),
            detail: "exhausted".to_owned(),
        };
        assert_eq!(sequence.category(), ErrorCategory::ResourceExhausted);
        let aborted = GlobalConstraintError::Txn(DistTxnError::Aborted(AbortReason::Conflict(
            "write/write".to_owned(),
        )));
        assert_eq!(aborted.category(), ErrorCategory::TransactionConflict);
    }

    // -- unique-index sink semantics ------------------------------------------

    #[test]
    fn unique_sink_claims_materialize_at_commit_and_block_conflicting_claims() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sink = UniqueIndexApplySink::open(tmp.path()).unwrap();
        // Prepare and commit a claim.
        sink.apply(&applied(
            1,
            intent_envelope([1u8; 16], persist(1, 100, &[(b"uq/k", b"pk-1")]), 100),
        ))
        .unwrap();
        // A different transaction's claim of the same key while the first
        // is unresolved: first-preparer-wins.
        sink.apply(&applied(
            2,
            intent_envelope([2u8; 16], persist(2, 110, &[(b"uq/k", b"pk-2")]), 110),
        ))
        .unwrap();
        assert_eq!(sink.state().rejections.len(), 1);
        assert!(matches!(
            sink.state().rejections[0].reason,
            UniqueRejectionReason::KeyConflict { ref key, holder }
                if *key == b"uq/k" && holder == xid(1)
        ));
        // Commit the first claim: it materializes.
        sink.apply(&applied(
            3,
            intent_envelope(
                [3u8; 16],
                IntentCommand::Resolve {
                    txn_id: xid(1),
                    decision: TxnDecision::Committed { commit_ts: ts(200) },
                },
                200,
            ),
        ))
        .unwrap();
        let committed = sink.claim(b"uq/k").unwrap();
        assert_eq!(committed.value.pk, b"pk-1");
        assert_eq!(committed.txn_id, xid(1));
        assert_eq!(committed.commit_ts, ts(200));
        // A fresh claim of the committed key with a different pk: ClaimHeld.
        sink.apply(&applied(
            4,
            intent_envelope([4u8; 16], persist(3, 210, &[(b"uq/k", b"pk-2")]), 210),
        ))
        .unwrap();
        assert_eq!(sink.state().rejections.len(), 2);
        assert!(matches!(
            sink.state().rejections[1].reason,
            UniqueRejectionReason::ClaimHeld { ref key, holder, .. }
                if *key == b"uq/k" && holder == xid(1)
        ));
        // The same (table, pk) re-claimed by a new transaction is allowed
        // (the row is being re-written; the claim carries over).
        sink.apply(&applied(
            5,
            intent_envelope([5u8; 16], persist(4, 220, &[(b"uq/k", b"pk-1")]), 220),
        ))
        .unwrap();
        assert_eq!(sink.state().rejections.len(), 2);
        assert!(sink.txn(&xid(4)).is_some());
        // Claims and intents survive a checkpoint reopen.
        let position = sink.applied_position();
        drop(sink);
        let sink = UniqueIndexApplySink::open(tmp.path()).unwrap();
        assert_eq!(sink.applied_position(), position);
        assert_eq!(sink.claim(b"uq/k").unwrap().value.pk, b"pk-1");
        assert!(sink.txn(&xid(4)).is_some());
        assert_eq!(sink.unresolved_txn_ids(), vec![xid(4)]);
    }

    #[test]
    fn unique_sink_rejects_malformed_claims_and_replays_idempotently() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sink = UniqueIndexApplySink::open(tmp.path()).unwrap();
        // A malformed claim value is journaled, never applied.
        let malformed = IntentCommand::PersistIntents {
            txn_id: xid(1),
            expected_schema_version: SchemaVersion::ZERO,
            expected_authz_version: 0,
            prepare_ts: ts(100),
            intents: vec![WriteIntent {
                txn_id: xid(1),
                key: b"uq/bad".to_vec(),
                value_ref: b"not json".to_vec(),
                prepare_ts: ts(100),
            }],
        };
        sink.apply(&applied(1, intent_envelope([1u8; 16], malformed, 100)))
            .unwrap();
        assert!(matches!(
            sink.state().rejections[0].reason,
            UniqueRejectionReason::MalformedClaim { ref key } if *key == b"uq/bad"
        ));
        assert!(sink.txn(&xid(1)).is_none());
        // Prepare, then replay the identical prepare: no journal, original
        // prepare timestamp stands.
        let prepare = persist(2, 150, &[(b"uq/k", b"pk-1")]);
        sink.apply(&applied(
            2,
            intent_envelope([2u8; 16], prepare.clone(), 150),
        ))
        .unwrap();
        sink.apply(&applied(3, intent_envelope([3u8; 16], prepare, 160)))
            .unwrap();
        assert_eq!(sink.state().rejections.len(), 1);
        assert_eq!(sink.txn(&xid(2)).unwrap().prepare_ts, ts(150));
        // A replay with a different write set is a payload mismatch.
        sink.apply(&applied(
            4,
            intent_envelope([4u8; 16], persist(2, 170, &[(b"uq/other", b"pk-1")]), 170),
        ))
        .unwrap();
        assert!(matches!(
            sink.state().rejections[1].reason,
            UniqueRejectionReason::PayloadMismatch
        ));
        // Resolve-abort: no claim materializes; a late prepare loses to the
        // tombstone.
        sink.apply(&applied(
            5,
            intent_envelope(
                [5u8; 16],
                IntentCommand::Resolve {
                    txn_id: xid(2),
                    decision: TxnDecision::Aborted {
                        reason: AbortReason::RolledBack,
                    },
                },
                200,
            ),
        ))
        .unwrap();
        assert!(sink.claim(b"uq/k").is_none());
        sink.apply(&applied(
            6,
            intent_envelope([6u8; 16], persist(2, 210, &[(b"uq/k", b"pk-1")]), 210),
        ))
        .unwrap();
        assert!(matches!(
            sink.state().rejections[2].reason,
            UniqueRejectionReason::AlreadyResolved { .. }
        ));
    }

    // -- sequence sink semantics -----------------------------------------------

    #[test]
    fn sequence_sink_allocates_monotonic_ranges_and_survives_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sink = SequenceApplySink::open(tmp.path()).unwrap();
        sink.apply(&applied(
            1,
            sequence_envelope([1u8; 16], grant("order-ids", 1_000, 1), 100),
        ))
        .unwrap();
        sink.apply(&applied(
            2,
            sequence_envelope([2u8; 16], grant("order-ids", 1_000, 2), 110),
        ))
        .unwrap();
        let state = sink.state();
        let first = &state.grants[&[1u8; 16]];
        assert_eq!((first.first, first.last), (1, 1_000));
        assert_eq!(first.holder, nid(1));
        let second = &state.grants[&[2u8; 16]];
        assert_eq!((second.first, second.last), (1_001, 2_000));
        assert_eq!(
            state.sequences["order-ids"].high_water, 2_000,
            "the high-water mark only moves forward"
        );
        // Independent sequences have independent watermarks.
        sink.apply(&applied(
            3,
            sequence_envelope([3u8; 16], grant("other", 10, 1), 120),
        ))
        .unwrap();
        assert_eq!(sink.state().sequences["other"].high_water, 10);
        // Invalid widths are journaled, never applied.
        sink.apply(&applied(
            4,
            sequence_envelope([4u8; 16], grant("order-ids", 0, 1), 130),
        ))
        .unwrap();
        assert!(matches!(
            sink.state().rejections[0].reason,
            SequenceRejectionReason::InvalidWidth { width: 0 }
        ));
        assert_eq!(sink.state().sequences["order-ids"].high_water, 2_000);
        // The high-water mark survives a checkpoint reopen: nothing is
        // re-issued after a restart.
        drop(sink);
        let mut sink = SequenceApplySink::open(tmp.path()).unwrap();
        assert_eq!(sink.state().sequences["order-ids"].high_water, 2_000);
        sink.apply(&applied(
            5,
            sequence_envelope([5u8; 16], grant("order-ids", 1_000, 3), 140),
        ))
        .unwrap();
        let third = &sink.state().grants[&[5u8; 16]];
        assert_eq!((third.first, third.last), (2_001, 3_000));
    }

    #[test]
    fn sequence_records_fail_closed_on_bad_versions() {
        let record = SequenceCommandRecord::new(grant("s", 5, 1));
        let decoded = SequenceCommandRecord::decode(&record.encode().unwrap()).unwrap();
        assert_eq!(decoded, record);
        let future = SequenceCommandRecord {
            format_version: SEQUENCE_RECORD_FORMAT_VERSION + 1,
            command: grant("s", 5, 1),
        };
        assert!(SequenceCommandRecord::decode(&future.encode().unwrap()).is_err());
        assert!(SequenceCommandRecord::decode(b"not json").is_err());
    }

    // -- cascade planning ------------------------------------------------------

    fn row(table_id: u64, group: u8, tablet: u8, key: &[u8]) -> CascadeRowRef {
        CascadeRowRef {
            table: table(table_id),
            participant: participant(group, tablet),
            key: key.to_vec(),
        }
    }

    struct MapGraph {
        edges: BTreeMap<(TableId, Vec<u8>), Vec<CascadeRowRef>>,
        probes: u64,
    }

    impl MapGraph {
        fn link(&mut self, parent: &CascadeRowRef, children: Vec<CascadeRowRef>) {
            self.edges
                .insert((parent.table, parent.key.clone()), children);
        }
    }

    impl CascadeGraph for MapGraph {
        fn children_of(
            &mut self,
            parent: &CascadeRowRef,
        ) -> Result<Vec<CascadeRowRef>, GlobalConstraintError> {
            self.probes += 1;
            Ok(self
                .edges
                .get(&(parent.table, parent.key.clone()))
                .cloned()
                .unwrap_or_default())
        }
    }

    fn graph() -> MapGraph {
        // parent p -> children c1, c2; c1 -> grandchild g1 (diamond: g1 is
        // also a child of c2); g1 -> great-grandchild gg.
        let p = row(1, 91, 1, b"p");
        let c1 = row(2, 91, 1, b"c1");
        let c2 = row(2, 92, 2, b"c2");
        let g1 = row(3, 92, 2, b"g1");
        let gg = row(4, 91, 1, b"gg");
        let mut graph = MapGraph {
            edges: BTreeMap::new(),
            probes: 0,
        };
        graph.link(&p, vec![c1.clone(), c2.clone()]);
        graph.link(&c1, vec![g1.clone()]);
        graph.link(&c2, vec![g1.clone()]);
        graph.link(&g1, vec![gg.clone()]);
        graph
    }

    fn bounds() -> CascadeBounds {
        CascadeBounds {
            max_rows: 100,
            max_tablets: 8,
            max_depth: 8,
            work_budget: 1_000,
            deadline: Duration::from_secs(60),
        }
    }

    #[test]
    fn cascade_plans_every_level_once_and_groups_writes_by_tablet() {
        let mut graph = graph();
        let root = row(1, 91, 1, b"p");
        let plan = CascadeExecutor::new(&mut graph, bounds())
            .plan(&root, Instant::now())
            .unwrap();
        assert_eq!(plan.total_rows, 5);
        assert_eq!(plan.tablet_count, 2);
        assert_eq!(plan.levels.len(), 4);
        assert_eq!(plan.levels[0].rows, vec![root.clone()]);
        assert_eq!(plan.levels[1].rows.len(), 2);
        // The diamond row g1 is walked once (visited set), at depth 2.
        assert_eq!(plan.levels[2].rows, vec![row(3, 92, 2, b"g1")]);
        assert_eq!(plan.levels[3].rows, vec![row(4, 91, 1, b"gg")]);
        // Work: one unit per probe and per returned row.
        assert_eq!(plan.work_units, graph.probes + 5);
        // Writes group by tablet: two participants, each with its deletes.
        let writes = plan.participant_writes(SchemaVersion::ZERO, 0);
        assert_eq!(writes.len(), 2);
        let total_intents: usize = writes.iter().map(|write| write.intents.len()).sum();
        assert_eq!(total_intents, 5);
        for write in &writes {
            assert!(write.participant.tablet_id == tid(1) || write.participant.tablet_id == tid(2));
            for intent in &write.intents {
                let value: CascadeDeleteValue = serde_json::from_slice(&intent.value_ref).unwrap();
                assert!(value.cascade_delete);
            }
        }
    }

    #[test]
    fn cascade_cycles_terminate_and_delete_each_row_once() {
        let a = row(1, 91, 1, b"a");
        let b = row(2, 91, 1, b"b");
        let mut graph = MapGraph {
            edges: BTreeMap::new(),
            probes: 0,
        };
        graph.link(&a, vec![b.clone()]);
        graph.link(&b, vec![a.clone()]);
        let plan = CascadeExecutor::new(&mut graph, bounds())
            .plan(&a, Instant::now())
            .unwrap();
        assert_eq!(plan.total_rows, 2);
        assert_eq!(plan.levels.len(), 2);
    }

    #[test]
    fn cascade_bounds_each_trip_with_resource_exhausted() {
        let root = row(1, 91, 1, b"p");
        let cases: Vec<(CascadeBoundKind, CascadeBounds)> = vec![
            (
                CascadeBoundKind::Rows,
                CascadeBounds {
                    max_rows: 3, // 5 rows total
                    ..bounds()
                },
            ),
            (
                CascadeBoundKind::Tablets,
                CascadeBounds {
                    max_tablets: 1, // the graph spans tablets 1 and 2
                    ..bounds()
                },
            ),
            (
                CascadeBoundKind::Depth,
                CascadeBounds {
                    max_depth: 1, // the graph reaches depth 3
                    ..bounds()
                },
            ),
            (
                CascadeBoundKind::Work,
                CascadeBounds {
                    work_budget: 2, // probes alone exceed this
                    ..bounds()
                },
            ),
            (
                CascadeBoundKind::Deadline,
                CascadeBounds {
                    deadline: Duration::ZERO, // the first check trips
                    ..bounds()
                },
            ),
        ];
        for (kind, bound) in cases {
            let mut graph = graph();
            let error = CascadeExecutor::new(&mut graph, bound)
                .plan(&root, Instant::now())
                .unwrap_err();
            match &error {
                GlobalConstraintError::CascadeExhausted { bound: tripped, .. } => {
                    assert_eq!(*tripped, kind, "expected the {kind} bound to trip");
                }
                other => panic!("expected CascadeExhausted, got {other:?}"),
            }
            assert_eq!(error.category(), ErrorCategory::ResourceExhausted);
        }
        // A generous plan stays under every bound.
        let mut graph = graph();
        assert!(CascadeExecutor::new(&mut graph, bounds())
            .plan(&root, Instant::now())
            .is_ok());
    }

    // -- 3-node in-memory cells -------------------------------------------------

    // Windows CI runs many three-node cells concurrently and can starve Raft
    // timers during debug builds. Keep the wait bounded but allow slow hosts.
    const LEADER_TIMEOUT: Duration = Duration::from_secs(60);
    const STATUS_GROUP: u8 = 90;
    const DATA_GROUP: u8 = 91;
    const PARENT_GROUP: u8 = 92;
    const UNIQUE_GROUP: u8 = 93;
    const SEQUENCE_GROUP: u8 = 94;
    const TEN_MINUTES: Duration = Duration::from_secs(600);

    fn raft_id(group: u8, member: u8) -> RaftNodeId {
        u64::from(group) * 100 + u64::from(member)
    }

    fn fast_group_config(dir: &Path, group: u8, member: u8) -> GroupConfig {
        let mut config = GroupConfig::new(
            "constraints-test",
            raft_id(group, member),
            dir.to_path_buf(),
        );
        config.heartbeat_interval = Duration::from_millis(50);
        config.election_timeout_min = Duration::from_millis(150);
        config.election_timeout_max = Duration::from_millis(300);
        config.install_snapshot_timeout = Duration::from_millis(1_000);
        config
    }

    fn member_addresses(group: u8) -> Vec<(RaftNodeId, String)> {
        (1..=3_u8)
            .map(|member| {
                (
                    raft_id(group, member),
                    format!(
                        "127.0.0.1:{}",
                        9_500 + u16::from(group) * 10 + u16::from(member)
                    ),
                )
            })
            .collect()
    }

    async fn wait_leader<T: RaftTransport>(members: &[&ConsensusGroup<T>]) -> RaftNodeId {
        let allowed: BTreeSet<RaftNodeId> = members.iter().map(|member| member.node_id()).collect();
        let deadline = Instant::now() + LEADER_TIMEOUT;
        loop {
            let mut leaders = BTreeSet::new();
            let mut seen = 0_usize;
            for member in members {
                if let Some(leader) = member.metrics().current_leader {
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

    /// One cell with every constraint participant: a transaction-status
    /// group, two data-tablet intent groups, and a unique-index group.
    /// `tmp` and `transport` are never read after boot: they are held for
    /// their lifetimes (the tempdir anchor and the transport registry).
    #[allow(dead_code)]
    struct Cell {
        tmp: tempfile::TempDir,
        transport: Arc<InMemoryTransport>,
        status: Vec<TxnStatusGroup<InMemoryTransport>>,
        participants: BTreeMap<RaftGroupId, Vec<IntentGroup<InMemoryTransport>>>,
        unique: Vec<UniqueIndexGroup<InMemoryTransport>>,
    }

    impl Cell {
        fn participants(&self) -> &BTreeMap<RaftGroupId, Vec<IntentGroup<InMemoryTransport>>> {
            &self.participants
        }

        fn data(&self, group: u8) -> &[IntentGroup<InMemoryTransport>] {
            &self.participants[&gid(group)]
        }

        async fn data_states(&self, group: u8) -> Vec<IntentState> {
            let members = &self.participants[&gid(group)];
            wait_until("data convergence", || async {
                let reference = members[0].state();
                members.iter().all(|member| member.state() == reference)
            })
            .await;
            members.iter().map(|member| member.state()).collect()
        }

        async fn unique_state(&self) -> UniqueIndexState {
            wait_until("unique convergence", || async {
                let reference = self.unique[0].state();
                self.unique.iter().all(|member| member.state() == reference)
            })
            .await;
            self.unique[0].state()
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
            for member in &self.unique {
                let _ = member.shutdown().await;
            }
        }
    }

    async fn boot_cell() -> Cell {
        let tmp = tempfile::tempdir().unwrap();
        let transport = Arc::new(InMemoryTransport::new());
        let mut status = Vec::new();
        for member in 1..=3_u8 {
            status.push(
                TxnStatusGroup::create(
                    fast_group_config(
                        &tmp.path().join(format!("status-{member}")),
                        STATUS_GROUP,
                        member,
                    ),
                    gid(STATUS_GROUP),
                    transport.clone(),
                )
                .await
                .unwrap(),
            );
        }
        status[0]
            .bootstrap(&member_addresses(STATUS_GROUP))
            .await
            .unwrap();
        let mut participants: BTreeMap<RaftGroupId, Vec<IntentGroup<InMemoryTransport>>> =
            BTreeMap::new();
        for group in [DATA_GROUP, PARENT_GROUP] {
            let mut members = Vec::new();
            for member in 1..=3_u8 {
                members.push(
                    IntentGroup::create(
                        fast_group_config(
                            &tmp.path().join(format!("data-{group}-{member}")),
                            group,
                            member,
                        ),
                        gid(group),
                        transport.clone(),
                    )
                    .await
                    .unwrap(),
                );
            }
            members[0]
                .bootstrap(&member_addresses(group))
                .await
                .unwrap();
            participants.insert(gid(group), members);
        }
        let mut unique = Vec::new();
        for member in 1..=3_u8 {
            unique.push(
                UniqueIndexGroup::create(
                    fast_group_config(
                        &tmp.path().join(format!("unique-{member}")),
                        UNIQUE_GROUP,
                        member,
                    ),
                    gid(UNIQUE_GROUP),
                    transport.clone(),
                )
                .await
                .unwrap(),
            );
        }
        unique[0]
            .bootstrap(&member_addresses(UNIQUE_GROUP))
            .await
            .unwrap();
        wait_leader(
            &status
                .iter()
                .map(|member| member.group())
                .collect::<Vec<_>>(),
        )
        .await;
        for group in [DATA_GROUP, PARENT_GROUP] {
            wait_leader(
                &participants[&gid(group)]
                    .iter()
                    .map(|member| member.group())
                    .collect::<Vec<_>>(),
            )
            .await;
        }
        wait_leader(
            &unique
                .iter()
                .map(|member| member.group())
                .collect::<Vec<_>>(),
        )
        .await;
        Cell {
            tmp,
            transport,
            status,
            participants,
            unique,
        }
    }

    fn driver_config() -> DistTxnConfig {
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
            pending_timeout: TEN_MINUTES,
            ..DistTxnConfig::default()
        }
    }

    fn unique_insert(txn: u8, row_key: &[u8], claim: UniqueClaim) -> UniqueInsert {
        UniqueInsert {
            txn_id: xid(txn),
            idempotency_key: [txn; 16],
            row: row_writes(DATA_GROUP, 1, &[row_key]),
            claim,
            expected_claim_schema_version: SchemaVersion::ZERO,
            expected_claim_authz_version: 0,
            observed: Vec::new(),
        }
    }

    // -- unique constraint integration -----------------------------------------

    #[tokio::test]
    async fn unique_insert_commits_claim_and_row_atomically() {
        let cell = boot_cell().await;
        let driver = GlobalConstraintDriver::new(driver_config());
        let control = ExecutionControl::default();
        let request = unique_insert(
            1,
            b"row-1",
            claim(UNIQUE_GROUP, 3, "uq_email", b"alice@example.com", b"pk-1"),
        );
        let claim_key = request.claim.intent_key();
        let outcome = driver
            .commit_unique_insert(
                &cell.status,
                cell.participants(),
                &cell.unique,
                request,
                &control,
            )
            .await
            .unwrap();
        assert_eq!(outcome.txn_id, xid(1));
        // The coordinator record names BOTH participants.
        assert_eq!(outcome.participants.len(), 2);
        let stored = cell.status[0].record(&xid(1)).unwrap();
        assert_eq!(
            stored.state,
            DistributedTxnState::Committed {
                commit_ts: outcome.commit_ts
            }
        );
        assert_eq!(stored.prepare_ts.len(), 2);
        // The data tablet resolved the row visible at commit_ts.
        let data = cell.data_states(DATA_GROUP).await;
        let writes = &data[0].committed_writes;
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].key, b"row-1");
        assert_eq!(writes[0].commit_ts, outcome.commit_ts);
        assert!(data[0].txns[&xid(1)].resolution.is_some());
        // The unique-index tablet materialized the claim.
        let unique = cell.unique_state().await;
        let committed = unique.claims.get(&claim_key).unwrap();
        assert_eq!(committed.value.pk, b"pk-1");
        assert_eq!(committed.value.table, table(7));
        assert_eq!(committed.commit_ts, outcome.commit_ts);
        assert_eq!(
            unique.txns[&xid(1)].resolution,
            Some(TxnDecision::Committed {
                commit_ts: outcome.commit_ts
            })
        );
        assert!(unique.rejections.is_empty());
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn duplicate_unique_insert_retry_yields_one_outcome() {
        let cell = boot_cell().await;
        let driver = GlobalConstraintDriver::new(driver_config());
        let control = ExecutionControl::default();
        let make = || {
            unique_insert(
                2,
                b"row-2",
                claim(UNIQUE_GROUP, 3, "uq_email", b"bob@example.com", b"pk-2"),
            )
        };
        let first = driver
            .commit_unique_insert(
                &cell.status,
                cell.participants(),
                &cell.unique,
                make(),
                &control,
            )
            .await
            .unwrap();
        // A client retry with the same transaction id and idempotency key
        // replays the original outcome (no second claim, no second row).
        let second = driver
            .commit_unique_insert(
                &cell.status,
                cell.participants(),
                &cell.unique,
                make(),
                &control,
            )
            .await
            .unwrap();
        assert_eq!(first.commit_ts, second.commit_ts);
        let data = cell.data_states(DATA_GROUP).await;
        assert_eq!(data[0].committed_writes.len(), 1);
        let unique = cell.unique_state().await;
        assert_eq!(unique.claims.len(), 1);
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn committed_claim_blocks_a_later_conflicting_claim() {
        let cell = boot_cell().await;
        let driver = GlobalConstraintDriver::new(driver_config());
        let control = ExecutionControl::default();
        driver
            .commit_unique_insert(
                &cell.status,
                cell.participants(),
                &cell.unique,
                unique_insert(
                    3,
                    b"row-3",
                    claim(UNIQUE_GROUP, 3, "uq_email", b"carol@example.com", b"pk-3"),
                ),
                &control,
            )
            .await
            .unwrap();
        // A later transaction claiming the same value for a different row
        // aborts with TransactionConflict; nothing of it is applied.
        let error = driver
            .commit_unique_insert(
                &cell.status,
                cell.participants(),
                &cell.unique,
                unique_insert(
                    4,
                    b"row-4",
                    claim(UNIQUE_GROUP, 3, "uq_email", b"carol@example.com", b"pk-4"),
                ),
                &control,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, GlobalConstraintError::UniqueViolation { .. }),
            "expected UniqueViolation, got {error:?}"
        );
        assert_eq!(error.category(), ErrorCategory::TransactionConflict);
        let data = cell.data_states(DATA_GROUP).await;
        assert_eq!(data[0].committed_writes.len(), 1);
        let unique = cell.unique_state().await;
        assert_eq!(unique.claims.len(), 1);
        // The aborted transaction resolved everywhere (no intent leaks).
        assert!(unique.txns[&xid(4)].resolution.is_some());
        assert!(matches!(
            data[0].txns[&xid(4)].resolution,
            Some(TxnDecision::Aborted {
                reason: AbortReason::Conflict(_)
            })
        ));
        assert!(cell.data(DATA_GROUP)[0].unresolved_txn_ids().is_empty());
        cell.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_unique_claims_exactly_one_commits() {
        let cell = boot_cell().await;
        let driver = GlobalConstraintDriver::new(driver_config());
        let control = ExecutionControl::default();
        // Two transactions (as if on two nodes) claim the same unique value
        // for different rows. Begin both, prepare both data tablets, then
        // race the two claim prepares behind a barrier: the raft total
        // order serializes them and first-preparer-wins.
        let claim_a = claim(UNIQUE_GROUP, 3, "uq_email", b"race@example.com", b"pk-a");
        let claim_b = claim(UNIQUE_GROUP, 3, "uq_email", b"race@example.com", b"pk-b");
        let claim_key = claim_a.intent_key();
        let request_for = |txn: u8, row_key: &[u8], claim: &UniqueClaim| CommitRequest {
            txn_id: xid(txn),
            idempotency_key: [txn; 16],
            writes: vec![
                row_writes(DATA_GROUP, 1, &[row_key]),
                claim.writes(SchemaVersion::ZERO, 0),
            ],
            observed: Vec::new(),
            first_write_tablet: None,
        };
        let participants = cell.participants();
        for txn in [10_u8, 11] {
            let claim = if txn == 10 { &claim_a } else { &claim_b };
            driver
                .txn_driver()
                .begin(
                    &cell.status,
                    &request_for(txn, format!("row-{txn}").as_bytes(), claim),
                    &control,
                )
                .await
                .unwrap();
            driver
                .txn_driver()
                .prepare_participant(
                    cell.data(DATA_GROUP),
                    &xid(txn),
                    &row_writes(DATA_GROUP, 1, &[format!("row-{txn}").as_bytes()]),
                    &control,
                )
                .await
                .unwrap();
        }
        // Race the claim prepares behind a barrier: the raft total order
        // serializes them and first-preparer-wins.
        let barrier = Barrier::new(2);
        let claim_writes_a = claim_a.writes(SchemaVersion::ZERO, 0);
        let claim_writes_b = claim_b.writes(SchemaVersion::ZERO, 0);
        let (race_a, race_b) = tokio::join!(
            async {
                barrier.wait().await;
                driver
                    .prepare_claim(&cell.unique, &xid(10), &claim_writes_a, &control)
                    .await
            },
            async {
                barrier.wait().await;
                driver
                    .prepare_claim(&cell.unique, &xid(11), &claim_writes_b, &control)
                    .await
            },
        );
        let mut prepared_tokens = Vec::new();
        let mut losers = Vec::new();
        for race in [race_a, race_b] {
            match race {
                Ok(token) => prepared_tokens.push(token),
                Err(error) => losers.push(error),
            }
        }
        assert_eq!(prepared_tokens.len(), 1, "exactly one claim may prepare");
        assert_eq!(losers.len(), 1);
        let loser_error = &losers[0];
        assert!(
            matches!(
                loser_error,
                GlobalConstraintError::ClaimPrepareRejected(
                    UniqueRejectionReason::KeyConflict { .. }
                )
            ),
            "the loser must lose the prepare race, got {loser_error:?}"
        );
        assert_eq!(loser_error.category(), ErrorCategory::TransactionConflict);
        let winner_txn = prepared_tokens[0].txn_id;
        let loser_txn = if winner_txn == xid(10) {
            xid(11)
        } else {
            xid(10)
        };
        // The winner commits (data + claim resolved); the loser aborts and
        // resolves everywhere.
        let data_token = cell.data(DATA_GROUP)[0].txn(&winner_txn).unwrap();
        let outcome = driver
            .txn_driver()
            .decide_commit(
                &cell.status,
                participants,
                &winner_txn,
                &[
                    PrepareToken {
                        txn_id: winner_txn,
                        tablet_id: tid(1),
                        raft_group_id: gid(DATA_GROUP),
                        prepare_ts: data_token.prepare_ts,
                        position: LogPosition { term: 1, index: 1 },
                        command_id: [0u8; 16],
                    },
                    prepared_tokens[0].clone(),
                ],
                &[],
                &control,
            )
            .await
            .unwrap();
        driver
            .resolve_unique(
                &cell.unique,
                &winner_txn,
                &participant(UNIQUE_GROUP, 3),
                TxnDecision::Committed {
                    commit_ts: outcome.commit_ts,
                },
                &control,
            )
            .await;
        driver
            .txn_driver()
            .abort(
                &cell.status,
                participants,
                &loser_txn,
                AbortReason::Conflict("lost the claim race".to_owned()),
                &control,
            )
            .await
            .unwrap();
        driver
            .resolve_unique(
                &cell.unique,
                &loser_txn,
                &participant(UNIQUE_GROUP, 3),
                TxnDecision::Aborted {
                    reason: AbortReason::Conflict("lost the claim race".to_owned()),
                },
                &control,
            )
            .await;
        // Final state: exactly one committed claim, naming the winner's pk;
        // no unresolved intents anywhere.
        let unique = cell.unique_state().await;
        let committed = unique.claims.get(&claim_key).unwrap();
        let winner_pk = if winner_txn == xid(10) {
            b"pk-a"
        } else {
            b"pk-b"
        };
        assert_eq!(committed.value.pk, winner_pk);
        assert_eq!(committed.txn_id, winner_txn);
        assert!(unique.txns[&loser_txn].resolution.is_some());
        assert!(unique.rejections.iter().any(|rejection| matches!(
            rejection.reason,
            UniqueRejectionReason::KeyConflict { .. }
        )));
        let data = cell.data_states(DATA_GROUP).await;
        assert!(data[0].txns[&loser_txn]
            .resolution
            .as_ref()
            .is_some_and(|decision| matches!(decision, TxnDecision::Aborted { .. })));
        assert_eq!(data[0].committed_writes.len(), 1);
        cell.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_unique_inserts_at_the_flow_level_commit_exactly_once() {
        let cell = boot_cell().await;
        let participants = cell.participants();
        let control = ExecutionControl::default();
        // Two full flows race end to end (no step coordination): whichever
        // way the interleaving falls — first-preparer-wins or committed
        // claim — exactly one transaction commits.
        let start = Barrier::new(2);
        let driver_a = GlobalConstraintDriver::new(DistTxnConfig {
            node_tiebreaker: 20,
            ..driver_config()
        });
        let driver_b = GlobalConstraintDriver::new(DistTxnConfig {
            node_tiebreaker: 21,
            ..driver_config()
        });
        let request_for = |txn: u8| {
            unique_insert(
                txn,
                format!("row-{txn}").as_bytes(),
                claim(
                    UNIQUE_GROUP,
                    3,
                    "uq_email",
                    b"flow-race@example.com",
                    format!("pk-{txn}").as_bytes(),
                ),
            )
        };
        let (result_a, result_b) = tokio::join!(
            async {
                start.wait().await;
                driver_a
                    .commit_unique_insert(
                        &cell.status,
                        participants,
                        &cell.unique,
                        request_for(20),
                        &control,
                    )
                    .await
            },
            async {
                start.wait().await;
                driver_b
                    .commit_unique_insert(
                        &cell.status,
                        participants,
                        &cell.unique,
                        request_for(21),
                        &control,
                    )
                    .await
            },
        );
        let mut committed = 0_usize;
        let mut conflicted = 0_usize;
        for result in [result_a, result_b] {
            match result {
                Ok(_outcome) => committed += 1,
                Err(error) => {
                    assert_eq!(
                        error.category(),
                        ErrorCategory::TransactionConflict,
                        "the loser must surface a conflict, got {error:?}"
                    );
                    conflicted += 1;
                }
            }
        }
        assert_eq!((committed, conflicted), (1, 1));
        let unique = cell.unique_state().await;
        assert_eq!(unique.claims.len(), 1);
        let data = cell.data_states(DATA_GROUP).await;
        assert_eq!(data[0].committed_writes.len(), 1);
        cell.shutdown().await;
    }

    // -- foreign key integration ------------------------------------------------

    struct MapOracle {
        present: BTreeSet<(TableId, Vec<u8>)>,
    }

    impl ParentExistence for MapOracle {
        fn parent_exists(&self, probe: &FkProbe) -> bool {
            self.present
                .contains(&(probe.parent_table, probe.parent_key.clone()))
        }
    }

    fn probe(parent_key: &[u8], child_key: &[u8]) -> FkProbe {
        FkProbe {
            parent_table: table(1),
            parent: participant(PARENT_GROUP, 2),
            parent_key: parent_key.to_vec(),
            child_table: table(2),
            child_key: child_key.to_vec(),
        }
    }

    fn child_insert(txn: u8, child_key: &[u8], probe: FkProbe) -> ChildInsert {
        ChildInsert {
            txn_id: xid(txn),
            idempotency_key: [txn; 16],
            child: row_writes(DATA_GROUP, 1, &[child_key]),
            probe,
            parent_expected_schema_version: SchemaVersion::ZERO,
            parent_expected_authz_version: 0,
            observed: Vec::new(),
        }
    }

    #[tokio::test]
    async fn child_insert_with_live_parent_commits_probe_and_row() {
        let cell = boot_cell().await;
        let driver = GlobalConstraintDriver::new(driver_config());
        let control = ExecutionControl::default();
        let oracle = MapOracle {
            present: BTreeSet::from([(table(1), b"parent-1".to_vec())]),
        };
        let outcome = driver
            .commit_child_insert(
                &cell.status,
                cell.participants(),
                &oracle,
                child_insert(30, b"child-1", probe(b"parent-1", b"child-1")),
                &control,
            )
            .await
            .unwrap();
        assert_eq!(outcome.participants.len(), 2);
        // The child row committed on the data tablet.
        let data = cell.data_states(DATA_GROUP).await;
        assert_eq!(data[0].committed_writes.len(), 1);
        assert_eq!(data[0].committed_writes[0].key, b"child-1");
        // The probe committed on the parent tablet as a lock marker.
        let parent = cell.data_states(PARENT_GROUP).await;
        assert_eq!(parent[0].committed_writes.len(), 1);
        assert_eq!(parent[0].committed_writes[0].key, b"parent-1");
        let marker: FkProbeValue =
            serde_json::from_slice(&parent[0].committed_writes[0].value_ref).unwrap();
        assert!(marker.foreign_key_probe);
        assert_eq!(marker.child_table, table(2));
        assert_eq!(marker.child_key, b"child-1");
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn child_insert_with_missing_parent_fails_before_any_write() {
        let cell = boot_cell().await;
        let driver = GlobalConstraintDriver::new(driver_config());
        let control = ExecutionControl::default();
        let oracle = MapOracle {
            present: BTreeSet::new(),
        };
        let error = driver
            .commit_child_insert(
                &cell.status,
                cell.participants(),
                &oracle,
                child_insert(31, b"child-2", probe(b"parent-missing", b"child-2")),
                &control,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, GlobalConstraintError::ForeignKeyViolation { .. }),
            "expected ForeignKeyViolation, got {error:?}"
        );
        assert_eq!(error.category(), ErrorCategory::TransactionAborted);
        // Validation failed before the commit fence: nothing was proposed.
        assert!(cell.data(DATA_GROUP)[0].txn(&xid(31)).is_none());
        assert!(cell.data(PARENT_GROUP)[0].txn(&xid(31)).is_none());
        assert!(cell.status[0].record(&xid(31)).is_none());
        cell.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_child_insert_vs_parent_delete_exactly_one_commits() {
        let cell = boot_cell().await;
        let driver = GlobalConstraintDriver::new(driver_config());
        let control = ExecutionControl::default();
        let participants = cell.participants();
        let oracle = MapOracle {
            present: BTreeSet::from([(table(1), b"parent-9".to_vec())]),
        };
        // Transaction 40 deletes the parent row (single-participant fast
        // path); transaction 41 inserts a child with a probe intent on the
        // same parent row key. Both begin, the insert prepares its child
        // row, then the two parent-tablet prepares race behind a barrier:
        // first-preparer-wins, exactly one transaction commits.
        let delete_request = CommitRequest {
            txn_id: xid(40),
            idempotency_key: [40; 16],
            writes: vec![row_writes(PARENT_GROUP, 2, &[b"parent-9"])],
            observed: Vec::new(),
            first_write_tablet: None,
        };
        driver
            .txn_driver()
            .begin(&cell.status, &delete_request, &control)
            .await
            .unwrap();
        let insert = child_insert(41, b"child-9", probe(b"parent-9", b"child-9"));
        let insert_request = CommitRequest {
            txn_id: insert.txn_id,
            idempotency_key: insert.idempotency_key,
            writes: vec![
                insert.child.clone(),
                insert.probe.writes(SchemaVersion::ZERO, 0),
            ],
            observed: Vec::new(),
            first_write_tablet: None,
        };
        driver
            .txn_driver()
            .begin(&cell.status, &insert_request, &control)
            .await
            .unwrap();
        // The insert's oracle check and child-row prepare succeed.
        assert!(oracle.parent_exists(&insert.probe));
        driver
            .txn_driver()
            .prepare_participant(cell.data(DATA_GROUP), &xid(41), &insert.child, &control)
            .await
            .unwrap();
        // Race: the delete's parent-row intent versus the insert's probe
        // intent on the same key.
        let barrier = Barrier::new(2);
        let probe_writes = insert.probe.writes(SchemaVersion::ZERO, 0);
        let (delete_prepare, probe_prepare) = tokio::join!(
            async {
                barrier.wait().await;
                driver
                    .txn_driver()
                    .prepare_participant(
                        cell.data(PARENT_GROUP),
                        &xid(40),
                        &delete_request.writes[0],
                        &control,
                    )
                    .await
            },
            async {
                barrier.wait().await;
                driver
                    .txn_driver()
                    .prepare_participant(cell.data(PARENT_GROUP), &xid(41), &probe_writes, &control)
                    .await
            },
        );
        let (winner, loser) = match (delete_prepare, probe_prepare) {
            (Ok(delete), Err(probe_error)) => {
                assert!(matches!(
                    probe_error,
                    DistTxnError::PrepareRejected(
                        crate::dist_txn::PrepareRejectionReason::KeyConflict { .. }
                    )
                ));
                ((40, delete), 41)
            }
            (Err(delete_error), Ok(probe_token)) => {
                assert!(matches!(
                    delete_error,
                    DistTxnError::PrepareRejected(
                        crate::dist_txn::PrepareRejectionReason::KeyConflict { .. }
                    )
                ));
                ((41, probe_token), 40)
            }
            (delete, probe) => panic!(
                "exactly one parent-tablet prepare may win: delete={delete:?} probe={probe:?}"
            ),
        };
        // The winner decides; the loser aborts.
        let winner_txn = xid(winner.0);
        let loser_txn = xid(loser);
        driver
            .txn_driver()
            .decide_commit(
                &cell.status,
                participants,
                &winner_txn,
                &[winner.1],
                &[],
                &control,
            )
            .await
            .unwrap();
        driver
            .txn_driver()
            .abort(
                &cell.status,
                participants,
                &loser_txn,
                AbortReason::Conflict("lost the parent-row race".to_owned()),
                &control,
            )
            .await
            .unwrap();
        // Final state: exactly one committed write on the parent tablet
        // (either the delete or the probe), and the loser is aborted
        // everywhere.
        let parent = cell.data_states(PARENT_GROUP).await;
        assert_eq!(parent[0].committed_writes.len(), 1);
        assert_eq!(
            parent[0].committed_writes[0].txn_id, winner_txn,
            "exactly one transaction may win the parent row"
        );
        assert!(matches!(
            parent[0].txns[&loser_txn].resolution,
            Some(TxnDecision::Aborted { .. })
        ));
        let data = cell.data_states(DATA_GROUP).await;
        if winner_txn == xid(41) {
            assert_eq!(data[0].committed_writes.len(), 1);
        } else {
            assert!(matches!(
                data[0].txns[&loser_txn].resolution,
                Some(TxnDecision::Aborted { .. })
            ));
        }
        cell.shutdown().await;
    }

    // -- cascade execution -------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cascade_applies_every_level_in_one_transaction() {
        let cell = boot_cell().await;
        let driver = GlobalConstraintDriver::new(driver_config());
        let control = ExecutionControl::default();
        // parent p (data tablet) -> children c1 (data) and c2 (parent
        // tablet); c1 -> g1 (parent tablet). One transaction, two
        // participant tablets, four deletes.
        let p = row(1, DATA_GROUP, 1, b"p");
        let c1 = row(2, DATA_GROUP, 1, b"c1");
        let c2 = row(2, PARENT_GROUP, 2, b"c2");
        let g1 = row(3, PARENT_GROUP, 2, b"g1");
        let mut graph = MapGraph {
            edges: BTreeMap::new(),
            probes: 0,
        };
        graph.link(&p, vec![c1.clone(), c2.clone()]);
        graph.link(&c1, vec![g1.clone()]);
        let outcome = CascadeExecutor::new(&mut graph, bounds())
            .execute(
                driver.txn_driver(),
                &cell.status,
                cell.participants(),
                &CascadeRequest {
                    txn_id: xid(50),
                    idempotency_key: [50; 16],
                    root: p.clone(),
                    expected_schema_version: SchemaVersion::ZERO,
                    expected_authz_version: 0,
                    started: Instant::now(),
                },
                &control,
            )
            .await
            .unwrap();
        assert_eq!(outcome.participants.len(), 2);
        // Every level landed atomically at one commit timestamp.
        let data = cell.data_states(DATA_GROUP).await;
        let parent = cell.data_states(PARENT_GROUP).await;
        let data_keys: Vec<&[u8]> = data[0]
            .committed_writes
            .iter()
            .map(|write| write.key.as_slice())
            .collect();
        assert_eq!(data_keys, [b"p".as_slice(), b"c1".as_slice()]);
        let parent_keys: Vec<&[u8]> = parent[0]
            .committed_writes
            .iter()
            .map(|write| write.key.as_slice())
            .collect();
        assert_eq!(parent_keys, [b"c2".as_slice(), b"g1".as_slice()]);
        for write in data[0]
            .committed_writes
            .iter()
            .chain(parent[0].committed_writes.iter())
        {
            assert_eq!(write.commit_ts, outcome.commit_ts);
            assert_eq!(write.txn_id, xid(50));
            let value: CascadeDeleteValue = serde_json::from_slice(&write.value_ref).unwrap();
            assert!(value.cascade_delete);
        }
        assert!(cell.data(DATA_GROUP)[0].unresolved_txn_ids().is_empty());
        assert!(cell.data(PARENT_GROUP)[0].unresolved_txn_ids().is_empty());
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn cascade_bound_trip_applies_nothing() {
        let cell = boot_cell().await;
        let driver = GlobalConstraintDriver::new(driver_config());
        let control = ExecutionControl::default();
        let p = row(1, DATA_GROUP, 1, b"p");
        let mut graph = MapGraph {
            edges: BTreeMap::new(),
            probes: 0,
        };
        graph.link(&p, vec![row(2, PARENT_GROUP, 2, b"c2")]);
        // max_tablets = 1 trips (the cascade spans two tablets).
        let error = CascadeExecutor::new(
            &mut graph,
            CascadeBounds {
                max_tablets: 1,
                ..bounds()
            },
        )
        .execute(
            driver.txn_driver(),
            &cell.status,
            cell.participants(),
            &CascadeRequest {
                txn_id: xid(51),
                idempotency_key: [51; 16],
                root: p.clone(),
                expected_schema_version: SchemaVersion::ZERO,
                expected_authz_version: 0,
                started: Instant::now(),
            },
            &control,
        )
        .await
        .unwrap_err();
        assert_eq!(error.category(), ErrorCategory::ResourceExhausted);
        assert!(matches!(
            error,
            GlobalConstraintError::CascadeExhausted {
                bound: CascadeBoundKind::Tablets,
                ..
            }
        ));
        // The bound tripped before the commit fence: no transaction exists
        // anywhere.
        assert!(cell.status[0].record(&xid(51)).is_none());
        assert!(cell.data(DATA_GROUP)[0].txn(&xid(51)).is_none());
        assert!(cell.data(PARENT_GROUP)[0].txn(&xid(51)).is_none());
        assert!(cell.data(DATA_GROUP)[0].committed_writes().is_empty());
        cell.shutdown().await;
    }

    // -- sequence integration ------------------------------------------------------

    struct SequenceCell {
        tmp: tempfile::TempDir,
        transport: Arc<InMemoryTransport>,
        members: Vec<SequenceGroup<InMemoryTransport>>,
    }

    impl SequenceCell {
        async fn shutdown(self) {
            for member in &self.members {
                let _ = member.shutdown().await;
            }
        }
    }

    async fn boot_sequence_member(
        dir: &Path,
        member: u8,
        transport: &Arc<InMemoryTransport>,
    ) -> SequenceGroup<InMemoryTransport> {
        SequenceGroup::create(
            fast_group_config(dir, SEQUENCE_GROUP, member),
            gid(SEQUENCE_GROUP),
            transport.clone(),
        )
        .await
        .unwrap()
    }

    async fn boot_sequence_cell() -> SequenceCell {
        let tmp = tempfile::tempdir().unwrap();
        let transport = Arc::new(InMemoryTransport::new());
        let mut members = Vec::new();
        for member in 1..=3_u8 {
            members.push(
                boot_sequence_member(
                    &tmp.path().join(format!("sequence-{member}")),
                    member,
                    &transport,
                )
                .await,
            );
        }
        members[0]
            .bootstrap(&member_addresses(SEQUENCE_GROUP))
            .await
            .unwrap();
        wait_leader(
            &members
                .iter()
                .map(|member| member.group())
                .collect::<Vec<_>>(),
        )
        .await;
        SequenceCell {
            tmp,
            transport,
            members,
        }
    }

    fn clock(tiebreaker: u32) -> HlcClock {
        HlcClock::new(tiebreaker, Duration::from_millis(500))
    }

    async fn draw_n(
        allocator: &mut SequenceAllocator,
        members: &[SequenceGroup<InMemoryTransport>],
        clock: &HlcClock,
        start: &Barrier,
        control: &ExecutionControl,
        count: usize,
    ) -> Vec<u64> {
        start.wait().await;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(allocator.next(members, clock, control).await.unwrap());
        }
        values
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_allocators_on_three_nodes_never_collide() {
        let cell = boot_sequence_cell().await;
        let control = ExecutionControl::default();
        let members = &cell.members;
        // Three allocators (three nodes) draw from the same sequence
        // concurrently: disjoint ranges, no shared value.
        let start = Barrier::new(3);
        let mut alloc_a = SequenceAllocator::new("order-ids", nid(1));
        let mut alloc_b = SequenceAllocator::new("order-ids", nid(2));
        let mut alloc_c = SequenceAllocator::new("order-ids", nid(3));
        let (clock_a, clock_b, clock_c) = (clock(1), clock(2), clock(3));
        let (values_a, values_b, values_c) = tokio::join!(
            draw_n(&mut alloc_a, members, &clock_a, &start, &control, 20),
            draw_n(&mut alloc_b, members, &clock_b, &start, &control, 20),
            draw_n(&mut alloc_c, members, &clock_c, &start, &control, 20),
        );
        let mut all: Vec<u64> = Vec::new();
        all.extend(&values_a);
        all.extend(&values_b);
        all.extend(&values_c);
        assert_eq!(all.len(), 60);
        let unique: BTreeSet<u64> = all.iter().copied().collect();
        assert_eq!(
            unique.len(),
            60,
            "concurrent allocators must never share a value"
        );
        // Each allocator's values are strictly increasing (monotonic local
        // counters over disjoint ranges).
        for values in [&values_a, &values_b, &values_c] {
            assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
        }
        // The replicated high-water mark covers everything handed out.
        wait_until("sequence convergence", || async {
            cell.members.iter().all(|member| {
                member.high_water("order-ids") == cell.members[0].high_water("order-ids")
            })
        })
        .await;
        let high_water = cell.members[0].high_water("order-ids").unwrap();
        assert!(high_water >= *all.iter().max().unwrap());
        assert_eq!(high_water, 3_000, "three default-width grants landed");
        cell.shutdown().await;
    }

    #[tokio::test]
    async fn sequence_high_water_survives_a_full_group_restart() {
        let cell = boot_sequence_cell().await;
        let control = ExecutionControl::default();
        let mut allocator = SequenceAllocator::with_width("invoice-ids", nid(1), 100);
        let first = allocator
            .next(&cell.members, &clock(1), &control)
            .await
            .unwrap();
        assert_eq!(first, 1);
        for expected in 2..=5 {
            assert_eq!(
                allocator
                    .next(&cell.members, &clock(1), &control)
                    .await
                    .unwrap(),
                expected
            );
        }
        assert_eq!(
            cell.members[0].high_water("invoice-ids"),
            Some(100),
            "one grant covers the draws"
        );
        // Crash every member (no graceful close) and reopen from disk.
        for member in cell.members {
            member.crash().await;
        }
        let transport = cell.transport.clone();
        let mut members = Vec::new();
        for member in 1..=3_u8 {
            members.push(
                boot_sequence_member(
                    &cell.tmp.path().join(format!("sequence-{member}")),
                    member,
                    &transport,
                )
                .await,
            );
        }
        wait_leader(
            &members
                .iter()
                .map(|member| member.group())
                .collect::<Vec<_>>(),
        )
        .await;
        // A fresh allocator (the old one's counter is gone with the
        // "process") continues ABOVE the replicated high-water mark: the
        // unused tail of the old grant (6..=100) gaps — documented
        // semantics — and no value is re-issued.
        let mut restarted = SequenceAllocator::with_width("invoice-ids", nid(2), 100);
        let next = restarted.next(&members, &clock(2), &control).await.unwrap();
        assert_eq!(
            next, 101,
            "the grant high-water mark is replicated: nothing is re-issued"
        );
        for member in &members {
            let _ = member.shutdown().await;
        }
    }

    #[tokio::test]
    async fn allocator_refills_monotonically_within_one_range_and_across_ranges() {
        let cell = boot_sequence_cell().await;
        let control = ExecutionControl::default();
        let mut allocator = SequenceAllocator::with_width("small", nid(1), 3);
        let clock = clock(1);
        let mut values = Vec::new();
        for _ in 0..7 {
            values.push(
                allocator
                    .next(&cell.members, &clock, &control)
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7]);
        // Three refills of width 3 (1..=3, 4..=6, 7..=9): the third draw
        // into the last range leaves headroom.
        assert_eq!(cell.members[0].high_water("small"), Some(9));
        assert_eq!(allocator.held_range(), Some((8, 9)));
        cell.shutdown().await;
    }
}
