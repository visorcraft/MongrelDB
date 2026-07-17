//! Consensus group lifecycle (spec section 6.5, 11.2; S2B-001).
//!
//! [`ConsensusGroup`] wraps one `openraft::Raft` node: one group per
//! replicated tablet, one committed log order, at most one effective leader
//! per term (spec section 4.2). It owns proposals, the linearizable read
//! barrier, snapshots, membership changes (joint consensus via openraft's
//! `change_membership`), best-effort leadership transfer, and observability
//! metrics (spec section 14.4).
//!
//! # Leadership transfer
//!
//! openraft 0.9 has no dedicated transfer-leadership RPC, so transfer is
//! orchestrated: the transport asks the target to start an election
//! (`Raft::trigger().elect()`), the target campaigns with a higher term, and
//! the old leader steps down when it sees the new term. This is best-effort —
//! it completes when the target is a healthy voter — and is used only for
//! planned failover and rolling upgrade (ADR-0010), never required for
//! correctness.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::error::{CheckIsLeaderError, ClientWriteError, Fatal, RaftError};
use openraft::{BasicNode, ChangeMembers, SnapshotPolicy};
use tokio::sync::watch;

use crate::error::ConsensusError;
use crate::identity::{
    log_position_of, ApplyResponse, CommandKind, MongrelRaft, MongrelRaftConfig, RaftNodeId,
    ReplicatedCommand,
};
use crate::network::{RaftTransport, TransportNetworkFactory};
use crate::state_machine::{ApplySink, MongrelStateMachine, SharedStateMachine};
use crate::storage::{MongrelLogStore, SharedLog, StorageConfig};
use mongreldb_log::commit_log::{ExecutionControl, LogPosition};
use mongreldb_log::envelope::CommandEnvelope;
use mongreldb_types::hlc::{HlcClock, HlcTimestamp};

pub use openraft::ServerState as RaftServerState;

/// Static configuration of one consensus group member.
#[derive(Debug, Clone)]
pub struct GroupConfig {
    /// Cluster name carried by openraft metrics/logging (the raft group id
    /// text form for MongrelDB groups).
    pub cluster_name: String,
    /// This node's projected raft id (see [`crate::identity::raft_node_id`]).
    pub node_id: RaftNodeId,
    /// Per-node local group directory (spec section 4.3: separate local
    /// directories per node).
    pub dir: PathBuf,
    /// Leader heartbeat interval.
    pub heartbeat_interval: Duration,
    /// Minimum election timeout (must exceed the heartbeat interval).
    pub election_timeout_min: Duration,
    /// Maximum election timeout.
    pub election_timeout_max: Duration,
    /// Timeout for one snapshot-send/install round.
    pub install_snapshot_timeout: Duration,
    /// Trigger a snapshot after this many logs since the last one.
    pub snapshot_policy_logs: u64,
    /// How many in-snapshot logs to keep before purging.
    pub max_in_snapshot_log_to_keep: u64,
    /// Follower lag (in logs) at which replication switches to snapshots.
    pub replication_lag_threshold: u64,
    /// Durable storage configuration (fsync policy, segment rolling).
    pub storage: StorageConfig,
    /// Bound on the apply idempotency set (S2B-004).
    pub idempotency_retention: usize,
    /// Maximum tolerated HLC clock skew for commit-timestamp stamping.
    pub hlc_max_skew: Duration,
}

impl GroupConfig {
    /// Production-shaped defaults for `node_id` at `dir`.
    pub fn new(cluster_name: impl Into<String>, node_id: RaftNodeId, dir: PathBuf) -> Self {
        GroupConfig {
            cluster_name: cluster_name.into(),
            node_id,
            dir,
            heartbeat_interval: Duration::from_millis(200),
            election_timeout_min: Duration::from_millis(600),
            election_timeout_max: Duration::from_millis(1_200),
            install_snapshot_timeout: Duration::from_millis(4_000),
            snapshot_policy_logs: 1_000,
            max_in_snapshot_log_to_keep: 500,
            replication_lag_threshold: 1_000,
            storage: StorageConfig::default(),
            idempotency_retention: crate::state_machine::DEFAULT_IDEMPOTENCY_RETENTION,
            hlc_max_skew: Duration::from_millis(500),
        }
    }

    fn openraft_config(&self) -> Result<Arc<openraft::Config>, ConsensusError> {
        let config = openraft::Config {
            cluster_name: self.cluster_name.clone(),
            heartbeat_interval: millis(&self.heartbeat_interval),
            election_timeout_min: millis(&self.election_timeout_min),
            election_timeout_max: millis(&self.election_timeout_max),
            install_snapshot_timeout: millis(&self.install_snapshot_timeout),
            snapshot_policy: SnapshotPolicy::LogsSinceLast(self.snapshot_policy_logs),
            max_in_snapshot_log_to_keep: self.max_in_snapshot_log_to_keep,
            replication_lag_threshold: self.replication_lag_threshold,
            ..Default::default()
        };
        config
            .validate()
            .map(Arc::new)
            .map_err(|e| ConsensusError::InvalidRequest(format!("invalid raft config: {e}")))
    }
}

fn millis(duration: &Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Proof that a command was committed by the group (term/index from the raft
/// log, commit timestamp and command id assigned by the leader per S2B-003).
#[derive(Debug, Clone)]
pub struct GroupCommitReceipt {
    /// Committed log position.
    pub position: LogPosition,
    /// Leader-assigned commit timestamp.
    pub commit_ts: HlcTimestamp,
    /// Leader-assigned command id (the envelope's id).
    pub command_id: Option<[u8; 16]>,
    /// The apply response (including the idempotent-replay flag).
    pub response: ApplyResponse,
}

/// A point-in-time image of applied state at a log boundary (mirrors
/// `mongreldb_log::commit_log::LogSnapshot`).
#[derive(Debug, Clone)]
pub struct ConsensusSnapshot {
    /// Last log position included in the snapshot.
    pub position: LogPosition,
    /// Commit timestamp recorded at `position`.
    pub commit_ts: HlcTimestamp,
    /// Framed (checksummed) snapshot payload.
    pub data: Vec<u8>,
}

/// Observability snapshot of the group (spec section 14.4).
#[derive(Debug, Clone)]
pub struct GroupMetrics {
    /// This node's raft id.
    pub node_id: RaftNodeId,
    /// Raft server state (learner/follower/candidate/leader/shutdown).
    pub state: RaftServerState,
    /// Current raft term.
    pub current_term: u64,
    /// Current leader as seen by this node.
    pub current_leader: Option<RaftNodeId>,
    /// Highest log index appended locally.
    pub last_log_index: Option<u64>,
    /// Highest position applied to the state machine.
    pub last_applied: Option<LogPosition>,
    /// Position of the last log included in the current snapshot.
    pub snapshot: Option<LogPosition>,
    /// Last purged log position.
    pub purged: Option<LogPosition>,
    /// Leader only: milliseconds since a quorum last acknowledged.
    pub millis_since_quorum_ack: Option<u64>,
}

/// One member of a replicated consensus group.
pub struct ConsensusGroup<T: RaftTransport> {
    config: GroupConfig,
    raft: MongrelRaft,
    transport: Arc<T>,
    shared_log: SharedLog,
    sm: SharedStateMachine,
    hlc: HlcClock,
    closed: AtomicBool,
}

impl<T: RaftTransport> ConsensusGroup<T> {
    /// Opens the group's durable state and starts the raft task. The node is
    /// passive until [`ConsensusGroup::bootstrap`] (first deployment) or until
    /// it is added as a learner by an existing leader.
    pub async fn create(
        config: GroupConfig,
        transport: Arc<T>,
        sink: Arc<std::sync::Mutex<dyn ApplySink>>,
    ) -> Result<Self, ConsensusError> {
        let log_store = MongrelLogStore::open(&config.dir, config.storage.clone())?;
        let shared_log = log_store.shared_log();
        let state_machine =
            MongrelStateMachine::open(&config.dir, sink, config.idempotency_retention)?;
        let sm = state_machine.shared();
        let raft_config = config.openraft_config()?;
        let factory = TransportNetworkFactory::new(transport.clone(), config.node_id);
        let raft = MongrelRaft::new(config.node_id, raft_config, factory, log_store, state_machine)
            .await
            .map_err(|e| ConsensusError::Raft(e.to_string()))?;
        transport.attach(config.node_id, raft.clone());
        Ok(ConsensusGroup {
            hlc: HlcClock::new(config.node_id as u32, config.hlc_max_skew),
            config,
            raft,
            transport,
            shared_log,
            sm,
            closed: AtomicBool::new(false),
        })
    }

    /// Bootstraps a pristine group with the given voter set (spec section
    /// 2.4). Safe to call on every pristine member with the same map; check
    /// [`ConsensusGroup::is_initialized`] before calling on a reopened node.
    pub async fn bootstrap(
        &self,
        members: BTreeMap<RaftNodeId, BasicNode>,
    ) -> Result<(), ConsensusError> {
        self.raft
            .initialize(members)
            .await
            .map_err(|e| ConsensusError::Raft(e.to_string()))
    }

    /// Whether this node already holds an initialized membership.
    pub async fn is_initialized(&self) -> Result<bool, ConsensusError> {
        self.raft
            .is_initialized()
            .await
            .map_err(|e| ConsensusError::Raft(e.to_string()))
    }

    /// This node's raft id.
    pub fn node_id(&self) -> RaftNodeId {
        self.config.node_id
    }

    /// Waits until this node sees any leader and returns its id (event-driven
    /// wait on raft metrics).
    pub async fn wait_leader(&self, timeout: Duration) -> Result<RaftNodeId, ConsensusError> {
        self.ensure_open()?;
        let metrics = self
            .raft
            .wait(Some(timeout))
            .metrics(
                |m| m.current_leader.is_some(),
                "wait for a known leader",
            )
            .await
            .map_err(|_| ConsensusError::DeadlineExceeded)?;
        Ok(metrics.current_leader.expect("predicate checked"))
    }

    /// Waits until this node's applied index reaches `index` (event-driven).
    pub async fn wait_applied_index(
        &self,
        index: u64,
        timeout: Duration,
    ) -> Result<(), ConsensusError> {
        self.ensure_open()?;
        self.raft
            .wait(Some(timeout))
            .applied_index_at_least(Some(index), "wait applied index")
            .await
            .map_err(|_| ConsensusError::DeadlineExceeded)?;
        Ok(())
    }

    fn ensure_open(&self) -> Result<(), ConsensusError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ConsensusError::Closed);
        }
        Ok(())
    }

    fn check_control(&self, control: &ExecutionControl) -> Result<(), ConsensusError> {
        control
            .check()
            .map_err(|e| match e {
                mongreldb_log::commit_log::LogError::Cancelled => ConsensusError::Cancelled,
                mongreldb_log::commit_log::LogError::DeadlineExceeded => {
                    ConsensusError::DeadlineExceeded
                }
                other => ConsensusError::Raft(other.to_string()),
            })
    }

    /// Proposes one command and waits until it is committed and applied
    /// (quorum durability; spec section 11.3). The leader stamps the commit
    /// timestamp from the group HLC clock before replication so every replica
    /// applies the identical value.
    pub async fn propose(
        &self,
        kind: CommandKind,
        envelope: CommandEnvelope,
        control: &ExecutionControl,
    ) -> Result<GroupCommitReceipt, ConsensusError> {
        self.ensure_open()?;
        self.check_control(control)?;
        envelope.verify()?;
        let commit_ts = self
            .hlc
            .now()
            .map_err(|e| ConsensusError::Clock(e.to_string()))?;
        let command_id = envelope.command_id;
        let command = ReplicatedCommand::new(kind, envelope, commit_ts);
        eprintln!("[dbg] propose: stamped, calling client_write");

        let write = self.raft.client_write(command);
        let response = match remaining(control) {
            Some(deadline) => tokio::time::timeout(deadline, write)
                .await
                .map_err(|_| ConsensusError::DeadlineExceeded)?,
            None => write.await,
        }
        .map_err(map_client_write_error)?;
        eprintln!("[dbg] propose: client_write returned");

        Ok(GroupCommitReceipt {
            position: log_position_of(&response.log_id),
            commit_ts,
            command_id: Some(command_id),
            response: response.data,
        })
    }

    /// Proposes a no-op barrier (advances the commit index; used to confirm
    /// leadership in a fresh term).
    pub async fn propose_noop(
        &self,
        control: &ExecutionControl,
    ) -> Result<GroupCommitReceipt, ConsensusError> {
        self.ensure_open()?;
        self.check_control(control)?;
        let write = self.raft.client_write(ReplicatedCommand::Noop);
        let response = match remaining(control) {
            Some(deadline) => tokio::time::timeout(deadline, write)
                .await
                .map_err(|_| ConsensusError::DeadlineExceeded)?,
            None => write.await,
        }
        .map_err(map_client_write_error)?;
        Ok(GroupCommitReceipt {
            position: log_position_of(&response.log_id),
            commit_ts: HlcTimestamp::ZERO,
            command_id: None,
            response: response.data,
        })
    }

    /// Linearizable read barrier (spec section 11.4): confirms leadership
    /// with a quorum and waits until the state machine has applied the
    /// leader's read position. Reads at or below the returned position are
    /// linearizable. Never served by an unconfirmed leader.
    pub async fn read_index(&self, control: &ExecutionControl) -> Result<LogPosition, ConsensusError> {
        self.ensure_open()?;
        self.check_control(control)?;
        let (read_log_id, applied) = self
            .raft
            .get_read_log_id()
            .await
            .map_err(map_check_is_leader_error)?;
        let Some(read_log_id) = read_log_id else {
            return Ok(LogPosition::ZERO);
        };
        let applied_index = applied.as_ref().map_or(0, |log_id| log_id.index);
        if read_log_id.index > applied_index {
            let wait = self.raft.wait(remaining(control));
            wait.applied_index_at_least(Some(read_log_id.index), "linearizable read barrier")
                .await
                .map_err(|_| ConsensusError::DeadlineExceeded)?;
        }
        Ok(log_position_of(&read_log_id))
    }

    /// Committed entries strictly after `after`, in log order, bounded by the
    /// applied watermark (applied is always `<=` committed, so this never
    /// exposes an uncommitted entry). Command-less entries (blank,
    /// membership) are skipped.
    pub fn read_committed(
        &self,
        after: LogPosition,
        limit: usize,
    ) -> Result<Vec<mongreldb_log::commit_log::CommittedEntry>, ConsensusError> {
        let record = self.sm.applied_record()?;
        let bound = record.last_applied.as_ref().map_or(0, |log_id| log_id.index);
        if bound <= after.index {
            return Ok(Vec::new());
        }
        let entries = self.shared_log.read_entries(after.index + 1, bound + 1, limit)?;
        let mut committed = Vec::new();
        for entry in entries {
            if let openraft::EntryPayload::Normal(command) = entry.payload {
                committed.push(mongreldb_log::commit_log::CommittedEntry {
                    position: log_position_of(&entry.log_id),
                    commit_ts: command.commit_ts().unwrap_or(HlcTimestamp::ZERO),
                    envelope: command.envelope().expect("normal payload").clone(),
                });
            }
        }
        Ok(committed)
    }

    /// The highest position the local state machine has applied.
    pub fn applied_position(&self) -> LogPosition {
        self.sm.applied_position()
    }

    /// Triggers a raft snapshot and waits until it covers the current applied
    /// position, then returns its framed bytes. Snapshot building is driven
    /// by the group's snapshot policy; this forces one (log compaction and
    /// follower catch-up, spec section 11.5).
    pub async fn snapshot(&self) -> Result<ConsensusSnapshot, ConsensusError> {
        self.ensure_open()?;
        let applied = self.sm.applied_record()?;
        let Some(last_applied) = applied.last_applied.clone() else {
            // Nothing applied: build directly (raft has nothing to compact).
            let (meta, framed) = self.sm.build_snapshot_now()?;
            return Ok(ConsensusSnapshot {
                position: meta
                    .last_log_id
                    .as_ref()
                    .map_or(LogPosition::ZERO, log_position_of),
                commit_ts: HlcTimestamp::ZERO,
                data: framed,
            });
        };
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| ConsensusError::Raft(e.to_string()))?;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some((meta, framed)) = self.sm.current_snapshot()? {
                let covers = meta
                    .last_log_id
                    .as_ref()
                    .is_some_and(|log_id| log_id.index >= last_applied.index);
                if covers {
                    let record = self.sm.applied_record()?;
                    return Ok(ConsensusSnapshot {
                        position: meta.last_log_id.as_ref().map_or(LogPosition::ZERO, log_position_of),
                        commit_ts: record.last_applied_commit_ts.unwrap_or(HlcTimestamp::ZERO),
                        data: framed,
                    });
                }
            }
            if Instant::now() >= deadline {
                return Err(ConsensusError::DeadlineExceeded);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Replaces local applied state with a snapshot produced by
    /// [`ConsensusGroup::snapshot`] (never installed over live state without
    /// staging; spec section 11.5). The snapshot's restored position must
    /// equal `snapshot.position`.
    pub fn install_snapshot(&self, snapshot: &ConsensusSnapshot) -> Result<(), ConsensusError> {
        self.ensure_open()?;
        let record = self.sm.install_local_snapshot(&snapshot.data)?;
        if record.position() != snapshot.position {
            return Err(ConsensusError::InvalidRequest(format!(
                "snapshot position {:?} does not match its checkpoint {:?}",
                snapshot.position,
                record.position()
            )));
        }
        Ok(())
    }

    /// Adds a node as a learner and blocks until it is line-rate (spec
    /// section 11.6 step "add learner, catch up").
    pub async fn add_learner(
        &self,
        node_id: RaftNodeId,
        node: BasicNode,
    ) -> Result<(), ConsensusError> {
        self.ensure_open()?;
        self.raft
            .add_learner(node_id, node, true)
            .await
            .map_err(map_client_write_error)?;
        Ok(())
    }

    /// Promotes a learner to voter through joint consensus (openraft's
    /// two-step `change_membership`).
    pub async fn promote(&self, node_id: RaftNodeId) -> Result<(), ConsensusError> {
        self.ensure_open()?;
        let mut voters = BTreeSet::new();
        voters.insert(node_id);
        self.raft
            .change_membership(ChangeMembers::AddVoterIds(voters), true)
            .await
            .map_err(map_client_write_error)?;
        Ok(())
    }

    /// Removes a voter (or learner) through joint consensus; the node is not
    /// retained as a learner. Transfer leadership first when removing the
    /// leader (spec section 11.6).
    pub async fn remove(&self, node_id: RaftNodeId) -> Result<(), ConsensusError> {
        self.ensure_open()?;
        let mut voters = BTreeSet::new();
        voters.insert(node_id);
        self.raft
            .change_membership(ChangeMembers::RemoveVoters(voters), false)
            .await
            .map_err(map_client_write_error)?;
        Ok(())
    }

    /// Current membership as seen by this node: `(voters, learners)`.
    pub fn members(&self) -> (Vec<RaftNodeId>, Vec<RaftNodeId>) {
        let metrics_rx = self.raft.metrics();
        let metrics = metrics_rx.borrow();
        let membership = metrics.membership_config.membership();
        (
            membership.voter_ids().collect(),
            membership.learner_ids().collect(),
        )
    }

    /// Best-effort leadership transfer to `target` (see module docs).
    pub async fn transfer_leader(
        &self,
        target: RaftNodeId,
        timeout: Duration,
    ) -> Result<(), ConsensusError> {
        self.ensure_open()?;
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.current_leader != Some(self.config.node_id) {
            return Err(ConsensusError::NotLeader {
                leader: metrics.current_leader,
            });
        }
        if target == self.config.node_id {
            return Ok(());
        }
        let (voters, _) = self.members();
        if !voters.contains(&target) {
            return Err(ConsensusError::InvalidRequest(format!(
                "transfer target {target} is not a voter"
            )));
        }
        self.transport.trigger_election(target).await?;
        self.raft
            .wait(Some(timeout))
            .current_leader(target, "leadership transfer")
            .await
            .map_err(|_| ConsensusError::DeadlineExceeded)?;
        Ok(())
    }

    /// Observability metrics (spec section 14.4).
    pub fn metrics(&self) -> GroupMetrics {
        let metrics = self.raft.metrics().borrow().clone();
        GroupMetrics {
            node_id: self.config.node_id,
            state: metrics.state,
            current_term: metrics.current_term,
            current_leader: metrics.current_leader,
            last_log_index: metrics.last_log_index,
            last_applied: metrics.last_applied.as_ref().map(log_position_of),
            snapshot: metrics.snapshot.as_ref().map(log_position_of),
            purged: metrics.purged.as_ref().map(log_position_of),
            millis_since_quorum_ack: metrics.millis_since_quorum_ack,
        }
    }

    /// A watch receiver of openraft's raw metrics (event-driven waits in
    /// tests and the server wave).
    pub fn raw_metrics(&self) -> watch::Receiver<openraft::RaftMetrics<RaftNodeId, BasicNode>> {
        self.raft.metrics()
    }

    /// Graceful shutdown: fsyncs and stops storage, then stops the raft task.
    pub async fn shutdown(&self) -> Result<(), ConsensusError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.transport.detach(self.config.node_id);
        self.shared_log.close();
        self.raft
            .shutdown()
            .await
            .map_err(|e| ConsensusError::Raft(e.to_string()))
    }

    /// Process-free crash simulation for durability tests: detaches and stops
    /// the raft task **without** the graceful storage close. Everything
    /// fsynced survives (the crash-durability contract); anything merely
    /// written may not.
    pub async fn crash(self) {
        self.transport.detach(self.config.node_id);
        let _ = self.raft.shutdown().await;
    }
}

fn remaining(control: &ExecutionControl) -> Option<Duration> {
    control
        .deadline
        .map(|deadline| deadline.saturating_duration_since(Instant::now()))
}

fn map_client_write_error(
    err: RaftError<RaftNodeId, ClientWriteError<RaftNodeId, BasicNode>>,
) -> ConsensusError {
    match err {
        RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
            ConsensusError::NotLeader {
                leader: forward.leader_id,
            }
        }
        RaftError::APIError(ClientWriteError::ChangeMembershipError(e)) => {
            ConsensusError::InvalidRequest(e.to_string())
        }
        RaftError::Fatal(Fatal::Stopped) => ConsensusError::Closed,
        other => ConsensusError::Raft(other.to_string()),
    }
}

fn map_check_is_leader_error(
    err: RaftError<RaftNodeId, CheckIsLeaderError<RaftNodeId, BasicNode>>,
) -> ConsensusError {
    match err {
        RaftError::APIError(CheckIsLeaderError::ForwardToLeader(forward)) => {
            ConsensusError::NotLeader {
                leader: forward.leader_id,
            }
        }
        RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_)) => {
            ConsensusError::NotLeader { leader: None }
        }
        RaftError::Fatal(Fatal::Stopped) => ConsensusError::Closed,
        other => ConsensusError::Raft(other.to_string()),
    }
}

/// Marker trait alias so rustdoc links resolve (`Raft<MongrelRaftConfig>`).
#[allow(dead_code)]
type _GroupRaft = openraft::Raft<MongrelRaftConfig>;
