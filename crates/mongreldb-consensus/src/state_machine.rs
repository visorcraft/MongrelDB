//! Deterministic, idempotent apply state machine (spec section 11.2, S2B-004).
//!
//! The state machine persists, under `<group dir>/raft/`:
//!
//! ```text
//! raft/state/applied   last applied term/index, last applied command ID,
//!                      last applied commit timestamp, and the bounded
//!                      idempotency set (checkpointed every apply batch)
//! raft/membership      last applied membership (S2B-002)
//! raft/snapshot/       snapshot data frames + CURRENT metadata
//! ```
//!
//! # Idempotent apply (S2B-004)
//!
//! Every command carries a leader-assigned command ID. Applying an entry
//! whose ID equals the last applied command ID or is present in the bounded
//! recent-ID set is recognized as a replay and skipped without re-dispatching
//! to the [`ApplySink`]; the response is marked `duplicate`. The set is
//! checkpointed with the applied record (so client retries stay idempotent
//! across restarts) and travels inside snapshots (so it survives snapshot
//! install). Applied positions at or below the persisted watermark are
//! skipped outright.
//!
//! Dispatch order within a batch is sink-first, checkpoint-second: a crash in
//! that window can replay an entry the sink already saw. The engine's durable
//! idempotency ledger (`TXN_IDEMPOTENCY`, ADR-0003) is the backstop for that
//! window; the adapter set covers client-retry duplicates.
//!
//! # HLC observation (spec section 8.2)
//!
//! A received timestamp advances the local clock: when the state machine is
//! opened with a [`CommitTsObserver`] ([`MongrelStateMachine::open_with_clock`],
//! the [`crate::group::ConsensusGroup`] path), every applied command's
//! leader-assigned commit timestamp — and the checkpoint timestamp of every
//! installed snapshot — is handed to it so the group's HLC clock never
//! trails the commit timestamps that flowed past it, and a new leader cannot
//! stamp below an already-committed value after a failover. Observation is
//! best-effort and never blocks or fails apply: the replicated apply stream
//! keeps its order, and timestamp *allocation* remains the fail-closed path
//! (spec section 8.2).
//!
//! # Fault hooks (FND-006)
//!
//! `raft.sm.apply.before` / `raft.sm.apply.after`,
//! `raft.snapshot.install.before` / `raft.snapshot.install.after`.

use std::collections::{HashSet, VecDeque};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{ErrorSubject, ErrorVerb, Snapshot, StorageIOError};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::identity::{
    log_position_of, MongrelRaftConfig, RaftLogEntry, RaftLogId, RaftNodeId, RaftSnapshotMeta,
    RaftStorageError, RaftStoredMembership, ReplicatedCommand,
};
use crate::storage::{
    decode_versioned, encode_versioned, read_frame_file, write_frame_file, StoreError,
};
use mongreldb_log::commit_log::LogPosition;
use mongreldb_log::envelope::CommandEnvelope;
use mongreldb_types::hlc::HlcTimestamp;

/// Sink receiving every applied or snapshot-installed commit timestamp so
/// the group's HLC clock can advance past it (spec section 8.2). The
/// implementation must never fail, panic, or block: observation is
/// best-effort so the apply stream keeps the replicated order (a rejected
/// observation must still leave the clock's skew high-water intact so later
/// timestamp *allocation* fails closed).
pub type CommitTsObserver = Arc<dyn Fn(HlcTimestamp) + Send + Sync>;

const MEMBERSHIP_MAGIC: &[u8; 8] = b"MRFT-MB1";
const APPLIED_MAGIC: &[u8; 8] = b"MRFT-AP1";
const SNAP_CURRENT_MAGIC: &[u8; 8] = b"MRFT-SC1";
const SNAP_MAGIC: &[u8; 8] = b"MRFT-SN1";

/// Default bound on the idempotency set (recently applied command IDs).
pub const DEFAULT_IDEMPOTENCY_RETENTION: usize = 4096;

/// Errors produced by the apply state machine.
#[derive(Debug, thiserror::Error)]
pub enum StateMachineError {
    /// Underlying storage failure.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The [`ApplySink`] rejected or failed a dispatch.
    #[error("apply sink failure: {0}")]
    Sink(String),
    /// Snapshot or checkpoint bytes failed validation (fails closed).
    #[error("corrupt state machine data: {0}")]
    Corrupt(String),
}

fn raft_sm_error(
    subject: ErrorSubject<RaftNodeId>,
    verb: ErrorVerb,
    err: StateMachineError,
) -> RaftStorageError {
    RaftStorageError::IO {
        source: StorageIOError::new(subject, verb, openraft::AnyError::new(&err)),
    }
}

// ---------------------------------------------------------------------------
// ApplySink
// ---------------------------------------------------------------------------

/// One command dispatched to applied state, in log order.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AppliedCommand {
    /// Position of the command's entry in the raft log.
    pub position: LogPosition,
    /// The replicated command (envelope + leader-assigned commit timestamp).
    pub command: ReplicatedCommand,
}

impl AppliedCommand {
    /// The carried command envelope.
    pub fn envelope(&self) -> Option<&CommandEnvelope> {
        self.command.envelope()
    }

    /// The leader-assigned commit timestamp.
    pub fn commit_ts(&self) -> Option<HlcTimestamp> {
        self.command.commit_ts()
    }

    /// The leader-assigned command ID (idempotent-apply identifier).
    pub fn command_id(&self) -> Option<[u8; 16]> {
        self.command.command_id()
    }
}

/// Destination of applied commands (the engine binding lands in the
/// integration wave; tests use [`InMemoryApplySink`]).
///
/// Implementations must be deterministic: every replica applies the same
/// commands in the same order.
pub trait ApplySink: Send + 'static {
    /// Dispatches one committed command. Never called twice for the same
    /// command ID unless the command crossed the crash window documented in
    /// the module docs (the engine ledger deduplicates that case).
    fn apply(&mut self, command: &AppliedCommand) -> Result<(), StateMachineError>;

    /// Serializes the sink's applied state for inclusion in a snapshot.
    fn snapshot(&self) -> Result<Vec<u8>, StateMachineError>;

    /// Replaces the sink's state with snapshot payload produced by
    /// [`ApplySink::snapshot`].
    fn install(&mut self, data: &[u8]) -> Result<(), StateMachineError>;
}

/// In-memory [`ApplySink`] for tests: records every dispatched command and
/// round-trips them through snapshots.
#[derive(Clone, Default)]
pub struct InMemoryApplySink {
    state: Arc<Mutex<Vec<AppliedCommand>>>,
}

impl InMemoryApplySink {
    /// Creates an empty sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// Every command dispatched so far, in apply order.
    pub fn applied(&self) -> Vec<AppliedCommand> {
        self.state.lock().expect("sink lock poisoned").clone()
    }

    /// Number of dispatched commands.
    pub fn len(&self) -> usize {
        self.state.lock().expect("sink lock poisoned").len()
    }

    /// Whether no command was dispatched yet.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl std::fmt::Debug for InMemoryApplySink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryApplySink")
            .field("applied", &self.len())
            .finish()
    }
}

impl ApplySink for InMemoryApplySink {
    fn apply(&mut self, command: &AppliedCommand) -> Result<(), StateMachineError> {
        self.state
            .lock()
            .map_err(|_| StateMachineError::Sink("sink lock poisoned".to_owned()))?
            .push(command.clone());
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<u8>, StateMachineError> {
        let applied = self
            .state
            .lock()
            .map_err(|_| StateMachineError::Sink("sink lock poisoned".to_owned()))?;
        bincode::serialize(&*applied).map_err(|e| StateMachineError::Sink(e.to_string()))
    }

    fn install(&mut self, data: &[u8]) -> Result<(), StateMachineError> {
        let applied: Vec<AppliedCommand> =
            bincode::deserialize(data).map_err(|e| StateMachineError::Corrupt(e.to_string()))?;
        *self
            .state
            .lock()
            .map_err(|_| StateMachineError::Sink("sink lock poisoned".to_owned()))? = applied;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Applied record (S2B-004)
// ---------------------------------------------------------------------------

/// The apply checkpoint persisted to `raft/state/applied`.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct AppliedRecord {
    /// Last applied log id (term/index).
    pub last_applied: Option<RaftLogId>,
    /// Command ID of the last applied command.
    pub last_applied_command_id: Option<[u8; 16]>,
    /// Commit timestamp of the last applied command.
    pub last_applied_commit_ts: Option<HlcTimestamp>,
    /// Bounded set of recently applied command IDs (oldest first).
    pub recent_command_ids: VecDeque<[u8; 16]>,
}

impl AppliedRecord {
    /// The applied position as a commit-log [`LogPosition`].
    pub fn position(&self) -> LogPosition {
        self.last_applied
            .as_ref()
            .map_or(LogPosition::ZERO, log_position_of)
    }
}

/// Snapshot payload: apply checkpoint + membership + sink state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SmSnapshot {
    record: AppliedRecord,
    membership: RaftStoredMembership,
    sink_payload: Vec<u8>,
}

// ---------------------------------------------------------------------------
// SharedStateMachine
// ---------------------------------------------------------------------------

struct SmInner {
    record: AppliedRecord,
    recent_set: HashSet<[u8; 16]>,
    membership: RaftStoredMembership,
}

/// Cloneable access to the apply state, used by the group and `RaftCommitLog`
/// (same-crate only).
#[derive(Clone)]
pub(crate) struct SharedStateMachine {
    inner: Arc<Mutex<SmInner>>,
    /// `<group dir>/raft`.
    raft_dir: PathBuf,
    sink: Arc<Mutex<dyn ApplySink>>,
    retention: usize,
    /// Receives every applied/installed commit timestamp (spec section 8.2);
    /// `None` when opened without one.
    commit_ts_observer: Option<CommitTsObserver>,
}

impl SharedStateMachine {
    fn lock(&self) -> Result<MutexGuard<'_, SmInner>, StateMachineError> {
        self.inner
            .lock()
            .map_err(|_| StateMachineError::Corrupt("state machine lock poisoned".to_owned()))
    }

    fn sink_lock(&self) -> Result<MutexGuard<'_, dyn ApplySink>, StateMachineError> {
        self.sink
            .lock()
            .map_err(|_| StateMachineError::Sink("sink lock poisoned".to_owned()))
    }

    fn snapshot_dir(&self) -> PathBuf {
        self.raft_dir.join("snapshot")
    }
    /// The current apply checkpoint.
    pub(crate) fn applied_record(&self) -> Result<AppliedRecord, StateMachineError> {
        Ok(self.lock()?.record.clone())
    }

    /// The applied position (`LogPosition::ZERO` before any apply).
    pub(crate) fn applied_position(&self) -> LogPosition {
        self.lock()
            .map_or(LogPosition::ZERO, |inner| inner.record.position())
    }

    /// Hands a replicated commit timestamp to the group's observer so the
    /// group clock can advance past it (spec section 8.2: a received
    /// timestamp advances the local clock). Best-effort: the observer never
    /// fails the apply path.
    fn observe_commit_ts(&self, commit_ts: HlcTimestamp) {
        if let Some(observer) = &self.commit_ts_observer {
            observer(commit_ts);
        }
    }

    fn persist_record(&self, record: &AppliedRecord) -> Result<(), StateMachineError> {
        let body = encode_versioned(record)?;
        write_frame_file(
            &self.raft_dir.join("state"),
            "applied",
            APPLIED_MAGIC,
            &body,
        )?;
        Ok(())
    }

    fn persist_membership(
        &self,
        membership: &RaftStoredMembership,
    ) -> Result<(), StateMachineError> {
        let body = encode_versioned(membership)?;
        write_frame_file(&self.raft_dir, "membership", MEMBERSHIP_MAGIC, &body)?;
        Ok(())
    }

    fn record_command_id(&self, inner: &mut SmInner, id: [u8; 16]) {
        inner.record.last_applied_command_id = Some(id);
        if inner.recent_set.insert(id) {
            inner.record.recent_command_ids.push_back(id);
            while inner.record.recent_command_ids.len() > self.retention {
                if let Some(evicted) = inner.record.recent_command_ids.pop_front() {
                    inner.recent_set.remove(&evicted);
                }
            }
        }
    }

    /// Builds a snapshot of the current applied state, persists it under
    /// `raft/snapshot/`, and returns its metadata and framed bytes.
    pub(crate) fn build_snapshot_now(
        &self,
    ) -> Result<(RaftSnapshotMeta, Vec<u8>), StateMachineError> {
        let (record, membership, sink_payload) = {
            let inner = self.lock()?;
            let payload = self.sink_lock()?.snapshot()?;
            (inner.record.clone(), inner.membership.clone(), payload)
        };
        let snapshot = SmSnapshot {
            record: record.clone(),
            membership: membership.clone(),
            sink_payload,
        };
        let body = encode_versioned(&snapshot)?;
        let snapshot_id = snapshot_id_of(&record, &body);
        // Stage the snapshot data file, then publish CURRENT atomically.
        write_frame_file(
            &self.snapshot_dir(),
            &format!("snap-{snapshot_id}.snap"),
            SNAP_MAGIC,
            &body,
        )?;
        let meta = RaftSnapshotMeta {
            last_log_id: record.last_applied,
            last_membership: membership,
            snapshot_id: snapshot_id.clone(),
        };
        let meta_body = encode_versioned(&meta)?;
        write_frame_file(
            &self.snapshot_dir(),
            "CURRENT",
            SNAP_CURRENT_MAGIC,
            &meta_body,
        )?;
        self.remove_stale_snapshots(&snapshot_id)?;
        Ok((meta, framed_bytes(SNAP_MAGIC, &body)))
    }

    /// Reads the current snapshot metadata and framed bytes, if any.
    pub(crate) fn current_snapshot(
        &self,
    ) -> Result<Option<(RaftSnapshotMeta, Vec<u8>)>, StateMachineError> {
        let current_path = self.snapshot_dir().join("CURRENT");
        let Some(meta_body) = read_frame_file(&current_path, SNAP_CURRENT_MAGIC)? else {
            return Ok(None);
        };
        let meta: RaftSnapshotMeta = decode_versioned(&current_path, &meta_body)?;
        let snap_path = self
            .snapshot_dir()
            .join(format!("snap-{}.snap", meta.snapshot_id));
        let framed = std::fs::read(&snap_path).map_err(StoreError::io(&snap_path, "read"))?;
        // Verify the data frame before handing it out (fail closed).
        parse_framed(SNAP_MAGIC, &framed).map_err(|e| match e {
            StoreError::Corrupt { reason, .. } => StateMachineError::Corrupt(reason),
            other => StateMachineError::Store(other),
        })?;
        Ok(Some((meta, framed)))
    }

    /// Installs a snapshot produced by [`SharedStateMachine::build_snapshot_now`]
    /// locally (the `CommitLog::install_snapshot` path). The snapshot id is
    /// derived from the framed content, so identical snapshots install under
    /// identical names on every node.
    pub(crate) fn install_local_snapshot(
        &self,
        framed: &[u8],
    ) -> Result<AppliedRecord, StateMachineError> {
        let body = parse_framed(SNAP_MAGIC, framed).map_err(|e| match e {
            StoreError::Corrupt { reason, .. } => StateMachineError::Corrupt(reason),
            other => StateMachineError::Store(other),
        })?;
        let snapshot: SmSnapshot = decode_versioned(Path::new("<snapshot>"), &body)?;
        let snapshot_id = snapshot_id_of(&snapshot.record, &body);
        self.install_framed_snapshot(&snapshot_id, framed)
    }

    /// Installs framed snapshot bytes (staging → verify → replace → update
    /// last-applied → remove old state; spec section 11.5). Returns the
    /// restored apply checkpoint.
    pub(crate) fn install_framed_snapshot(
        &self,
        snapshot_id: &str,
        framed: &[u8],
    ) -> Result<AppliedRecord, StateMachineError> {
        let body = parse_framed(SNAP_MAGIC, framed).map_err(|e| match e {
            StoreError::Corrupt { reason, .. } => StateMachineError::Corrupt(reason),
            other => StateMachineError::Store(other),
        })?;
        let snapshot: SmSnapshot = decode_versioned(Path::new("<snapshot>"), &body)?;
        mongreldb_fault::inject("raft.snapshot.install.before")
            .map_err(|f| StateMachineError::Sink(f.to_string()))?;

        // Stage the new snapshot file before touching live state.
        write_frame_file(
            &self.snapshot_dir(),
            &format!("snap-{snapshot_id}.snap"),
            SNAP_MAGIC,
            &body,
        )?;
        // Replace applied state: sink first, then membership + checkpoint,
        // each through the atomic frame path.
        self.sink_lock()?.install(&snapshot.sink_payload)?;
        self.persist_membership(&snapshot.membership)?;
        self.persist_record(&snapshot.record)?;
        {
            let mut inner = self.lock()?;
            inner.recent_set = snapshot.record.recent_command_ids.iter().copied().collect();
            inner.record = snapshot.record.clone();
            inner.membership = snapshot.membership.clone();
        }
        // The restored checkpoint timestamp is a received timestamp too:
        // advance the group clock past it (spec section 8.2).
        if let Some(commit_ts) = snapshot.record.last_applied_commit_ts {
            self.observe_commit_ts(commit_ts);
        }
        let meta = RaftSnapshotMeta {
            last_log_id: snapshot.record.last_applied,
            last_membership: snapshot.membership,
            snapshot_id: snapshot_id.to_owned(),
        };
        let meta_body = encode_versioned(&meta)?;
        write_frame_file(
            &self.snapshot_dir(),
            "CURRENT",
            SNAP_CURRENT_MAGIC,
            &meta_body,
        )?;
        self.remove_stale_snapshots(snapshot_id)?;
        mongreldb_fault::inject("raft.snapshot.install.after")
            .map_err(|f| StateMachineError::Sink(f.to_string()))?;
        Ok(snapshot.record)
    }

    fn remove_stale_snapshots(&self, keep_id: &str) -> Result<(), StateMachineError> {
        let keep = format!("snap-{keep_id}.snap");
        for entry in std::fs::read_dir(self.snapshot_dir())
            .map_err(StoreError::io(&self.snapshot_dir(), "read dir"))?
        {
            let entry = entry.map_err(StoreError::io(&self.snapshot_dir(), "read dir"))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("snap-") && name.ends_with(".snap") && name != keep {
                std::fs::remove_file(entry.path())
                    .map_err(StoreError::io(&entry.path(), "delete"))?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Framing helpers for byte buffers (file framing lives in storage.rs).
// ---------------------------------------------------------------------------

/// Deterministic snapshot id from the checkpoint position and content hash,
/// so identical snapshots install under identical names everywhere.
fn snapshot_id_of(record: &AppliedRecord, body: &[u8]) -> String {
    let hash = Sha256::digest(body);
    let (term, index) = record
        .last_applied
        .as_ref()
        .map_or((0, 0), |log_id| (log_id.leader_id.term, log_id.index));
    format!(
        "{term}-{index}-{}",
        hash.iter()
            .take(4)
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    )
}

/// Composes `MAGIC | sha256(body) | body` (same layout as frame files).
fn framed_bytes(magic: &[u8; 8], body: &[u8]) -> Vec<u8> {
    let hash = Sha256::digest(body);
    let mut out = Vec::with_capacity(8 + 32 + body.len());
    out.extend_from_slice(magic);
    out.extend_from_slice(&hash);
    out.extend_from_slice(body);
    out
}

/// Verifies and unwraps bytes produced by [`framed_bytes`].
fn parse_framed(magic: &[u8; 8], bytes: &[u8]) -> Result<Vec<u8>, StoreError> {
    let path = Path::new("<snapshot>");
    if bytes.len() < 8 + 32 || &bytes[..8] != magic {
        return Err(StoreError::corrupt(path, "bad magic or truncated header"));
    }
    let (tag, body) = bytes[8..].split_at(32);
    let calc = Sha256::digest(body);
    if tag != calc.as_slice() {
        return Err(StoreError::corrupt(path, "checksum mismatch"));
    }
    Ok(body.to_vec())
}

// ---------------------------------------------------------------------------
// MongrelStateMachine (openraft RaftStateMachine)
// ---------------------------------------------------------------------------

/// The openraft-facing state machine: deterministic, idempotent apply with a
/// persisted checkpoint and snapshot support (S2B-004).
pub struct MongrelStateMachine {
    shared: SharedStateMachine,
}

impl MongrelStateMachine {
    /// Opens (creating if needed) the state machine under `<group_dir>/raft/`.
    pub fn open(
        group_dir: &Path,
        sink: Arc<Mutex<dyn ApplySink>>,
        idempotency_retention: usize,
    ) -> Result<Self, StateMachineError> {
        Self::open_with_clock(group_dir, sink, idempotency_retention, None)
    }

    /// Opens the state machine with a [`CommitTsObserver`] that every
    /// applied or snapshot-installed commit timestamp is handed to (spec
    /// section 8.2; [`ConsensusGroup`](crate::group::ConsensusGroup) passes
    /// an observer advancing its stamping clock here).
    pub fn open_with_clock(
        group_dir: &Path,
        sink: Arc<Mutex<dyn ApplySink>>,
        idempotency_retention: usize,
        commit_ts_observer: Option<CommitTsObserver>,
    ) -> Result<Self, StateMachineError> {
        let raft_dir = group_dir.join("raft");
        let state_dir = raft_dir.join("state");
        let snapshot_dir = raft_dir.join("snapshot");
        std::fs::create_dir_all(&state_dir).map_err(StoreError::io(&state_dir, "create dirs"))?;
        std::fs::create_dir_all(&snapshot_dir)
            .map_err(StoreError::io(&snapshot_dir, "create dirs"))?;

        let record: AppliedRecord =
            match read_frame_file(&state_dir.join("applied"), APPLIED_MAGIC)? {
                None => AppliedRecord::default(),
                Some(body) => decode_versioned(&state_dir.join("applied"), &body)?,
            };
        let membership: RaftStoredMembership =
            match read_frame_file(&raft_dir.join("membership"), MEMBERSHIP_MAGIC)? {
                None => RaftStoredMembership::default(),
                Some(body) => decode_versioned(&raft_dir.join("membership"), &body)?,
            };
        let recent_set: HashSet<[u8; 16]> = record.recent_command_ids.iter().copied().collect();
        Ok(MongrelStateMachine {
            shared: SharedStateMachine {
                inner: Arc::new(Mutex::new(SmInner {
                    record,
                    recent_set,
                    membership,
                })),
                raft_dir,
                sink,
                retention: idempotency_retention,
                commit_ts_observer,
            },
        })
    }

    /// Shared access for the group and `RaftCommitLog`.
    pub(crate) fn shared(&self) -> SharedStateMachine {
        self.shared.clone()
    }

    fn apply_one(
        &self,
        entry: &RaftLogEntry,
    ) -> Result<crate::identity::ApplyResponse, StateMachineError> {
        let position = log_position_of(&entry.log_id);
        let mut response = crate::identity::ApplyResponse {
            position,
            command_id: None,
            commit_ts: None,
            duplicate: false,
        };
        let mut inner = self.shared.lock()?;

        // Skip entries at or below the persisted watermark entirely.
        if let Some(last) = &inner.record.last_applied {
            if entry.log_id.index <= last.index {
                response.duplicate = true;
                return Ok(response);
            }
        }

        match &entry.payload {
            openraft::EntryPayload::Blank => {}
            openraft::EntryPayload::Membership(membership) => {
                inner.membership =
                    RaftStoredMembership::new(Some(entry.log_id), membership.clone());
                self.shared.persist_membership(&inner.membership)?;
            }
            openraft::EntryPayload::Normal(command) => {
                response.command_id = command.command_id();
                response.commit_ts = command.commit_ts();
                let duplicate = command.command_id().is_some_and(|id| {
                    inner.record.last_applied_command_id == Some(id)
                        || inner.recent_set.contains(&id)
                });
                if duplicate {
                    response.duplicate = true;
                } else {
                    let applied = AppliedCommand {
                        position,
                        command: command.clone(),
                    };
                    self.shared.sink_lock()?.apply(&applied)?;
                    if let Some(id) = command.command_id() {
                        self.shared.record_command_id(&mut inner, id);
                    }
                    if let Some(ts) = command.commit_ts() {
                        inner.record.last_applied_commit_ts = Some(ts);
                        // A received timestamp advances the local clock
                        // (spec section 8.2): the group clock learns every
                        // commit timestamp it applies, so a future leader on
                        // this node never stamps below them.
                        self.shared.observe_commit_ts(ts);
                    }
                }
            }
        }
        inner.record.last_applied = Some(entry.log_id);
        Ok(response)
    }
}

impl RaftStateMachine<MongrelRaftConfig> for MongrelStateMachine {
    type SnapshotBuilder = MongrelSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<RaftLogId>, RaftStoredMembership), RaftStorageError> {
        let inner = self
            .shared
            .lock()
            .map_err(|e| raft_sm_error(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
        Ok((inner.record.last_applied, inner.membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<crate::identity::ApplyResponse>, RaftStorageError>
    where
        I: IntoIterator<Item = RaftLogEntry> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<RaftLogEntry> = entries.into_iter().collect();
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        mongreldb_fault::inject("raft.sm.apply.before")
            .map_err(|f| StateMachineError::Sink(f.to_string()))
            .map_err(|e| raft_sm_error(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        let mut responses = Vec::with_capacity(entries.len());
        for entry in &entries {
            responses.push(
                self.apply_one(entry)
                    .map_err(|e| raft_sm_error(ErrorSubject::StateMachine, ErrorVerb::Write, e))?,
            );
        }
        // Checkpoint the applied record once per batch, fsynced.
        let record = self
            .shared
            .applied_record()
            .map_err(|e| raft_sm_error(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
        self.shared
            .persist_record(&record)
            .map_err(|e| raft_sm_error(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        mongreldb_fault::inject("raft.sm.apply.after")
            .map_err(|f| StateMachineError::Sink(f.to_string()))
            .map_err(|e| raft_sm_error(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        MongrelSnapshotBuilder {
            shared: self.shared.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, RaftStorageError> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &RaftSnapshotMeta,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), RaftStorageError> {
        // The received cursor is positioned at the end after the chunk
        // writes; rewind before reading.
        let mut bytes = snapshot;
        bytes.seek(std::io::SeekFrom::Start(0)).await.map_err(|e| {
            raft_sm_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Seek,
                StateMachineError::Corrupt(format!("seeking snapshot: {e}")),
            )
        })?;
        let mut framed = Vec::new();
        bytes.read_to_end(&mut framed).await.map_err(|e| {
            raft_sm_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                StateMachineError::Corrupt(format!("reading snapshot: {e}")),
            )
        })?;
        let record = self
            .shared
            .install_framed_snapshot(&meta.snapshot_id, &framed)
            .map_err(|e| raft_sm_error(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        // The installed checkpoint must agree with the metadata openraft
        // committed to (fail closed on divergence).
        if record.last_applied != meta.last_log_id {
            return Err(raft_sm_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
                StateMachineError::Corrupt(format!(
                    "snapshot metadata last_log_id {:?} != checkpoint {:?}",
                    meta.last_log_id, record.last_applied
                )),
            ));
        }
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<MongrelRaftConfig>>, RaftStorageError> {
        let Some((meta, framed)) = self
            .shared
            .current_snapshot()
            .map_err(|e| raft_sm_error(ErrorSubject::StateMachine, ErrorVerb::Read, e))?
        else {
            return Ok(None);
        };
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(framed)),
        }))
    }
}

/// Snapshot builder handed to openraft; captures shared handles and
/// serializes a consistent checkpoint at build time.
pub struct MongrelSnapshotBuilder {
    shared: SharedStateMachine,
}

impl RaftSnapshotBuilder<MongrelRaftConfig> for MongrelSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<MongrelRaftConfig>, RaftStorageError> {
        let (meta, framed) = self
            .shared
            .build_snapshot_now()
            .map_err(|e| raft_sm_error(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(framed)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::CommandKind;
    use mongreldb_types::hlc::HlcClock;

    /// An observer advancing `clock` with every handed-out timestamp (the
    /// production observer in `group.rs` additionally filters stale stamps).
    fn observing(clock: &Arc<HlcClock>) -> CommitTsObserver {
        let clock = clock.clone();
        Arc::new(move |commit_ts| {
            let _ = clock.observe(commit_ts);
        })
    }

    fn command(index: u64, id: u8) -> RaftLogEntry {
        let envelope = CommandEnvelope::new(1, [id; 16], vec![id; 4]);
        RaftLogEntry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: openraft::EntryPayload::Normal(ReplicatedCommand::new(
                CommandKind::Transaction,
                envelope,
                HlcTimestamp {
                    physical_micros: 1_000 + index,
                    logical: 0,
                    node_tiebreaker: 0,
                },
            )),
        }
    }

    fn machine(dir: &Path, sink: Arc<Mutex<dyn ApplySink>>) -> MongrelStateMachine {
        MongrelStateMachine::open(dir, sink, DEFAULT_IDEMPOTENCY_RETENTION).unwrap()
    }

    #[tokio::test]
    async fn apply_records_checkpoint_and_skips_duplicates() {
        let tmp = tempfile::tempdir().unwrap();
        let test_sink = Arc::new(Mutex::new(InMemoryApplySink::new()));
        let sink: Arc<Mutex<dyn ApplySink>> = test_sink.clone();
        let mut sm = machine(tmp.path(), sink);

        let responses = sm.apply(vec![command(1, 7), command(2, 8)]).await.unwrap();
        assert_eq!(responses.len(), 2);
        assert!(!responses[0].duplicate);
        assert_eq!(test_sink.lock().unwrap().len(), 2);

        // Client retry of command id 8 under a new log index: skipped.
        let retry = command(3, 8);
        let responses = sm.apply(vec![retry]).await.unwrap();
        assert!(responses[0].duplicate);
        assert_eq!(test_sink.lock().unwrap().len(), 2);

        // Checkpoint reflects the last applied entry.
        let record = sm.shared.applied_record().unwrap();
        assert_eq!(record.last_applied.as_ref().map(|l| l.index), Some(3));
        assert_eq!(record.last_applied_command_id, Some([8u8; 16]));
        assert_eq!(
            record.last_applied_commit_ts,
            Some(HlcTimestamp {
                physical_micros: 1_002,
                logical: 0,
                node_tiebreaker: 0,
            })
        );
    }

    #[tokio::test]
    async fn idempotency_survives_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let test_sink = Arc::new(Mutex::new(InMemoryApplySink::new()));
        {
            let mut sm = machine(tmp.path(), test_sink.clone());
            sm.apply(vec![command(1, 42)]).await.unwrap();
        }
        // Reopen: replaying the same command id is skipped even though the
        // in-memory set was lost.
        let mut sm = machine(tmp.path(), test_sink.clone());
        let responses = sm.apply(vec![command(2, 42)]).await.unwrap();
        assert!(responses[0].duplicate);
        assert_eq!(test_sink.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn watermark_skips_replayed_positions() {
        let tmp = tempfile::tempdir().unwrap();
        let test_sink = Arc::new(Mutex::new(InMemoryApplySink::new()));
        let mut sm = machine(tmp.path(), test_sink.clone());
        sm.apply(vec![command(5, 1)]).await.unwrap();
        // openraft re-delivers index 5 (e.g. after a crash before the
        // checkpoint fsync of a later batch): skipped by the watermark even
        // with a fresh command id.
        let responses = sm.apply(vec![command(5, 99)]).await.unwrap();
        assert!(responses[0].duplicate);
        assert_eq!(test_sink.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn snapshot_round_trip_into_fresh_machine() {
        let tmp_a = tempfile::tempdir().unwrap();
        let tmp_b = tempfile::tempdir().unwrap();
        let sink_a = Arc::new(Mutex::new(InMemoryApplySink::new()));
        let sink_b = Arc::new(Mutex::new(InMemoryApplySink::new()));
        let mut sm_a = machine(tmp_a.path(), sink_a.clone());
        sm_a.apply(vec![command(1, 1), command(2, 2)])
            .await
            .unwrap();
        let (meta, framed) = sm_a.shared.build_snapshot_now().unwrap();

        let mut sm_b = machine(tmp_b.path(), sink_b.clone());
        sm_b.install_snapshot(&meta, Box::new(Cursor::new(framed)))
            .await
            .unwrap();
        assert_eq!(
            sm_b.shared.applied_position(),
            LogPosition { term: 1, index: 2 }
        );
        assert_eq!(
            sink_a.lock().unwrap().applied(),
            sink_b.lock().unwrap().applied()
        );

        // Idempotency traveled with the snapshot: re-applying command 2 is a
        // duplicate on the fresh machine too.
        let responses = sm_b.apply(vec![command(3, 2)]).await.unwrap();
        assert!(responses[0].duplicate);
        assert_eq!(sink_b.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn corrupted_snapshot_fails_closed() {
        let tmp = tempfile::tempdir().unwrap();
        let test_sink = Arc::new(Mutex::new(InMemoryApplySink::new()));
        let mut sm = machine(tmp.path(), test_sink);
        let (meta, mut framed) = sm.shared.build_snapshot_now().unwrap();
        let last = framed.len() - 1;
        framed[last] ^= 0x01;
        assert!(sm
            .install_snapshot(&meta, Box::new(Cursor::new(framed)))
            .await
            .is_err());
    }

    fn wall_plus(offset: std::time::Duration) -> u64 {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros()
                + offset.as_micros(),
        )
        .unwrap()
    }

    fn command_at(index: u64, id: u8, commit_ts: HlcTimestamp) -> RaftLogEntry {
        let envelope = CommandEnvelope::new(1, [id; 16], vec![id; 4]);
        RaftLogEntry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: openraft::EntryPayload::Normal(ReplicatedCommand::new(
                CommandKind::Transaction,
                envelope,
                commit_ts,
            )),
        }
    }

    #[tokio::test]
    async fn applied_commit_ts_advances_the_group_clock() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = Arc::new(Mutex::new(InMemoryApplySink::new()));
        let clock = Arc::new(HlcClock::new(1, std::time::Duration::from_secs(7_200)));
        let mut sm = MongrelStateMachine::open_with_clock(
            tmp.path(),
            sink,
            DEFAULT_IDEMPOTENCY_RETENTION,
            Some(observing(&clock)),
        )
        .unwrap();

        // A commit timestamp an hour ahead of the wall clock: observing it
        // advances the group clock past it (spec section 8.2).
        let ahead = HlcTimestamp {
            physical_micros: wall_plus(std::time::Duration::from_secs(3_600)),
            logical: 0,
            node_tiebreaker: 9,
        };
        sm.apply(vec![command_at(1, 7, ahead)]).await.unwrap();
        let stamped = clock.now().unwrap();
        assert!(
            stamped > ahead,
            "the group clock must pass the applied commit timestamp: {stamped:?} <= {ahead:?}"
        );
    }

    #[tokio::test]
    async fn skewed_observation_never_wedges_apply() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = Arc::new(Mutex::new(InMemoryApplySink::new()));
        // A tight skew bound: observing a far-ahead commit timestamp is a
        // skew event, which must not fail the apply stream...
        let clock = Arc::new(HlcClock::new(1, std::time::Duration::from_millis(500)));
        let mut sm = MongrelStateMachine::open_with_clock(
            tmp.path(),
            sink.clone(),
            DEFAULT_IDEMPOTENCY_RETENTION,
            Some(observing(&clock)),
        )
        .unwrap();
        let far_ahead = HlcTimestamp {
            physical_micros: wall_plus(std::time::Duration::from_secs(3_600)),
            logical: 0,
            node_tiebreaker: 9,
        };
        sm.apply(vec![command_at(1, 7, far_ahead)]).await.unwrap();
        assert_eq!(sink.lock().unwrap().len(), 1);
        // ...but allocation through that clock now fails closed (spec
        // section 8.2: excessive skew rejects timestamp allocation).
        assert!(clock.now().is_err());
    }

    #[tokio::test]
    async fn installed_snapshot_commit_ts_advances_the_group_clock() {
        let tmp_a = tempfile::tempdir().unwrap();
        let tmp_b = tempfile::tempdir().unwrap();
        let sink_a = Arc::new(Mutex::new(InMemoryApplySink::new()));
        let sink_b = Arc::new(Mutex::new(InMemoryApplySink::new()));
        let clock_b = Arc::new(HlcClock::new(2, std::time::Duration::from_secs(7_200)));

        let ahead = HlcTimestamp {
            physical_micros: wall_plus(std::time::Duration::from_secs(3_600)),
            logical: 3,
            node_tiebreaker: 1,
        };
        let mut sm_a = machine(tmp_a.path(), sink_a);
        sm_a.apply(vec![command_at(1, 1, ahead)]).await.unwrap();
        let (meta, framed) = sm_a.shared.build_snapshot_now().unwrap();

        let mut sm_b = MongrelStateMachine::open_with_clock(
            tmp_b.path(),
            sink_b,
            DEFAULT_IDEMPOTENCY_RETENTION,
            Some(observing(&clock_b)),
        )
        .unwrap();
        sm_b.install_snapshot(&meta, Box::new(Cursor::new(framed)))
            .await
            .unwrap();
        let stamped = clock_b.now().unwrap();
        assert!(
            stamped > ahead,
            "snapshot install must advance the group clock past the \
             checkpoint timestamp: {stamped:?} <= {ahead:?}"
        );
    }
}
