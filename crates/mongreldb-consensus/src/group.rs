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
//! orchestrated: the old leader suppresses its own candidacy
//! (`Raft::runtime_config().elect(false)`, restored afterwards), the transport asks the
//! target to start an election (`Raft::trigger().elect()`), and the target
//! wins the next election round once the old leader's lease lapses. This is
//! best-effort — it completes when the target is a healthy voter — and is
//! used only for planned failover and rolling upgrade (ADR-0010), never
//! required for correctness.

//! # Commit-timestamp monotonicity (spec section 8.2)
//!
//! The leader stamps every proposal's `commit_ts` from the group HLC clock
//! as `next_after(max(local now, commit floor))`. The floor is the persisted
//! last-applied commit timestamp (surviving restarts) raised by any command
//! timestamp in the unapplied local log tail (covering entries a previous
//! leader committed that this node applies *before* its own proposals), and
//! the state machine observes every applied or snapshot-installed commit
//! timestamp into the same clock. Commit timestamps therefore never regress
//! in log order across leader failover, clock steps backward, or restarts,
//! and timestamp-based read barriers (`ReadConsistency::Snapshot`) stay
//! satisfiable on the new leader.

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
    log_position_of, ApplyResponse, CommandKind, MongrelRaft, RaftNodeId, ReplicatedCommand,
};
use crate::network::{RaftTransport, TransportNetworkFactory};
use crate::read::{ReadConsistency, ReadConsistencyError, ReadWatermark};
use crate::state_machine::{ApplySink, CommitTsObserver, MongrelStateMachine, SharedStateMachine};
use crate::storage::{LeaderDiskDrop, MongrelLogStore, SharedLog, StorageConfig};
use mongreldb_log::commit_log::{ExecutionControl, LogPosition};
use mongreldb_log::envelope::CommandEnvelope;
use mongreldb_types::hlc::{HlcClock, HlcTimestamp, WallClockSource};

pub use openraft::ServerState as RaftServerState;

/// Wall clock in microseconds since the Unix epoch (the group HLC clock's
/// default time source; mirrors `HlcClock::new`'s).
fn system_time_micros() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros())
            .unwrap_or(0),
    )
    .unwrap_or(u64::MAX)
}

/// Static configuration of one consensus group member.
#[derive(Clone)]
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
    /// Wall-clock source for the group HLC clock; `None` reads the system
    /// clock. Tests inject controlled time to exercise clock regressions.
    pub hlc_time_source: Option<WallClockSource>,
}

impl std::fmt::Debug for GroupConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupConfig")
            .field("cluster_name", &self.cluster_name)
            .field("node_id", &self.node_id)
            .field("dir", &self.dir)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("election_timeout_min", &self.election_timeout_min)
            .field("election_timeout_max", &self.election_timeout_max)
            .field("install_snapshot_timeout", &self.install_snapshot_timeout)
            .field("snapshot_policy_logs", &self.snapshot_policy_logs)
            .field(
                "max_in_snapshot_log_to_keep",
                &self.max_in_snapshot_log_to_keep,
            )
            .field("replication_lag_threshold", &self.replication_lag_threshold)
            .field("storage", &self.storage)
            .field("idempotency_retention", &self.idempotency_retention)
            .field("hlc_max_skew", &self.hlc_max_skew)
            .field(
                "hlc_time_source",
                &self.hlc_time_source.as_ref().map(|_| "<injected>"),
            )
            .finish()
    }
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
            hlc_time_source: None,
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

/// Outcome of a leader-local-durable propose (spec section 11.3, S2C).
///
/// # Honesty contract
///
/// The receipt is issued strictly after the segment fsync covering the
/// entry completed, so a crash before that fsync never acknowledges. The
/// entry is **not** declared committed: visibility still gates on quorum
/// commit + apply (spec section 4.4), and a LeaderDisk-acknowledged entry
/// can be truncated on leader loss (RPO > 0 — the documented
/// [`DurabilityLevel::LeaderDisk`](mongreldb_log::commit_log::DurabilityLevel)
/// trade-off). `quorum_committed` distinguishes the race where quorum
/// commit landed before the local fsync signal; the receipt then carries
/// quorum strength.
#[derive(Debug, Clone)]
pub struct LeaderDiskReceipt {
    /// Log position the leader assigned the entry.
    pub position: LogPosition,
    /// Leader-assigned commit timestamp (stamped before replication).
    pub commit_ts: HlcTimestamp,
    /// The command's id (the envelope's id).
    pub command_id: [u8; 16],
    /// `true` when quorum commit + apply landed before the local fsync
    /// signal; the receipt then carries quorum durability.
    pub quorum_committed: bool,
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
    /// The group commit-timestamp clock; shared with the state machine so
    /// every applied commit timestamp advances it (spec section 8.2).
    hlc: Arc<HlcClock>,
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
        let wall: WallClockSource = match &config.hlc_time_source {
            Some(source) => source.clone(),
            None => Arc::new(system_time_micros),
        };
        let hlc = Arc::new(HlcClock::with_time_source(
            config.node_id as u32,
            config.hlc_max_skew,
            wall.clone(),
        ));
        // The apply path advances the group clock past every applied commit
        // timestamp (spec section 8.2; review M1). Only timestamps ahead of
        // the local wall are observed: a stale replicated entry reflects
        // replication latency, not clock skew, and any future stamp exceeds
        // it already — feeding it to `observe` would only poison the skew
        // high-water with ordinary apply lag (the skew guard stays reserved
        // for genuinely future-dated stamps). A rejected observation still
        // folds into the clock's high-water skew, keeping allocation
        // fail-closed; the apply stream itself never fails on observation.
        let commit_ts_observer: CommitTsObserver = {
            let hlc = hlc.clone();
            Arc::new(move |commit_ts: HlcTimestamp| {
                if commit_ts.physical_micros > wall() {
                    let _ = hlc.observe(commit_ts);
                }
            })
        };
        let log_store = MongrelLogStore::open(&config.dir, config.storage.clone())?;
        let shared_log = log_store.shared_log();
        let state_machine = MongrelStateMachine::open_with_clock(
            &config.dir,
            sink,
            config.idempotency_retention,
            Some(commit_ts_observer),
        )?;
        let sm = state_machine.shared();
        let raft_config = config.openraft_config()?;
        let factory = TransportNetworkFactory::new(transport.clone(), config.node_id);
        let raft = MongrelRaft::new(
            config.node_id,
            raft_config,
            factory,
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| ConsensusError::Raft(e.to_string()))?;
        transport.attach(config.node_id, raft.clone());
        Ok(ConsensusGroup {
            hlc,
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

    /// The group's text identifier (session tokens carry it; spec section
    /// 11.4).
    pub fn group_id(&self) -> &str {
        &self.config.cluster_name
    }

    /// Waits until this node sees any leader and returns its id (event-driven
    /// wait on raft metrics).
    pub async fn wait_leader(&self, timeout: Duration) -> Result<RaftNodeId, ConsensusError> {
        self.ensure_open()?;
        let metrics = self
            .raft
            .wait(Some(timeout))
            .metrics(|m| m.current_leader.is_some(), "wait for a known leader")
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
        control.check().map_err(|e| match e {
            mongreldb_log::commit_log::LogError::Cancelled => ConsensusError::Cancelled,
            mongreldb_log::commit_log::LogError::DeadlineExceeded => {
                ConsensusError::DeadlineExceeded
            }
            other => ConsensusError::Raft(other.to_string()),
        })
    }

    /// Stamps the commit timestamp for a new proposal (spec section 8.2):
    /// `next_after(max(local now, commit floor))`, so the leader never stamps
    /// at or below a commit timestamp it already knows — across failovers,
    /// backward clock steps, and restarts. The skew guard (`HlcClock::now`)
    /// still gates allocation; the floor only ever raises the result.
    fn stamp_commit_ts(&self) -> Result<HlcTimestamp, ConsensusError> {
        let floor = self.commit_ts_floor()?;
        let now = self
            .hlc
            .now()
            .map_err(|e| ConsensusError::Clock(e.to_string()))?;
        Ok(match floor {
            Some(floor) if floor >= now => self.hlc.next_after(floor),
            _ => now,
        })
    }

    /// The highest commit timestamp a new stamp must exceed: the persisted
    /// last-applied commit timestamp (the durable floor, surviving restarts),
    /// raised by any command timestamp in the local log above the applied
    /// index. The tail covers entries a previous leader replicated (and
    /// possibly quorum-committed) that this node applies *before* its own
    /// proposals: without it, a freshly elected leader whose apply lags
    /// could stamp below such an entry and regress the applied watermark in
    /// log order (review M1). Keeping every stamp above the whole local log
    /// is also the inductive step that keeps log-order commit timestamps
    /// monotonic cluster-wide.
    fn commit_ts_floor(&self) -> Result<Option<HlcTimestamp>, ConsensusError> {
        let record = self.sm.applied_record()?;
        let mut floor = record.last_applied_commit_ts;
        let applied_index = record
            .last_applied
            .as_ref()
            .map_or(0, |log_id| log_id.index);
        let last_log_index = self.raft.metrics().borrow().last_log_index.unwrap_or(0);
        if last_log_index > applied_index {
            let span = usize::try_from(last_log_index - applied_index).unwrap_or(usize::MAX);
            let tail = self
                .shared_log
                .read_entries(applied_index + 1, last_log_index + 1, span)?;
            for entry in &tail {
                if let openraft::EntryPayload::Normal(command) = &entry.payload {
                    if let Some(commit_ts) = command.commit_ts() {
                        floor = Some(floor.map_or(commit_ts, |known| known.max(commit_ts)));
                    }
                }
            }
        }
        Ok(floor)
    }

    /// Proposes one command and waits until it is committed and applied
    /// (quorum durability; spec section 11.3). The leader stamps the commit
    /// timestamp from the group HLC clock before replication so every replica
    /// applies the identical value; the stamp is never below the commit
    /// floor (see [`Self::commit_ts_floor`]), keeping commit timestamps
    /// monotonic across leader failover (spec section 8.2).
    pub async fn propose(
        &self,
        kind: CommandKind,
        envelope: CommandEnvelope,
        control: &ExecutionControl,
    ) -> Result<GroupCommitReceipt, ConsensusError> {
        self.ensure_open()?;
        self.check_control(control)?;
        envelope.verify()?;
        let commit_ts = self.stamp_commit_ts()?;
        let command_id = envelope.command_id;
        let command = ReplicatedCommand::new(kind, envelope, commit_ts);

        let write = self.raft.client_write(command);
        let response = match remaining(control) {
            Some(deadline) => tokio::time::timeout(deadline, write)
                .await
                .map_err(|_| ConsensusError::DeadlineExceeded)?,
            None => write.await,
        }
        .map_err(map_client_write_error)?;

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

    /// Leader-local-durable propose (spec section 11.3, S2C): registers an
    /// fsync completion waiter keyed by the command id, submits the command
    /// fire-and-forget, and races the post-fsync signal against quorum
    /// commit/apply and the caller's [`ExecutionControl`].
    ///
    /// The returned receipt is **not** a commit declaration — see
    /// [`LeaderDiskReceipt`] for the honesty contract. Leadership is
    /// enforced by the raft write path itself: a non-leader's fire-and-
    /// forget write is answered with [`ConsensusError::NotLeader`] by the
    /// raft core, and waiters are keyed only to commands proposed in the
    /// current term (conflicting old-term entries are truncated, failing
    /// their waiters with [`LeaderDiskDrop::Truncated`]).
    pub async fn propose_leader_durable(
        &self,
        kind: CommandKind,
        envelope: CommandEnvelope,
        control: &ExecutionControl,
    ) -> Result<LeaderDiskReceipt, ConsensusError> {
        self.ensure_open()?;
        self.check_control(control)?;
        // Review N10: refuse non-leaders before registering a LeaderDisk
        // waiter. A follower that accepted the same command id via
        // replication could otherwise surface a LeaderDisk receipt whose
        // durability is honest (local fsync) but not "leader-local".
        {
            let metrics = self.raft.metrics().borrow().clone();
            if metrics.current_leader != Some(self.config.node_id) {
                return Err(ConsensusError::NotLeader {
                    leader: metrics.current_leader,
                });
            }
        }
        envelope.verify()?;
        let commit_ts = self.stamp_commit_ts()?;
        let command_id = envelope.command_id;
        let command = ReplicatedCommand::new(kind, envelope, commit_ts);

        // Register before proposing so the append cannot bind ahead of the
        // registration; the signal itself fires strictly post-fsync.
        let mut fsync_rx = self.shared_log.register_leader_disk_waiter(command_id)?;
        let mut ff_rx = match self.raft.client_write_ff(command).await {
            Ok(receiver) => receiver,
            Err(fatal) => {
                self.shared_log.deregister_leader_disk_waiter(command_id);
                return Err(map_fatal(fatal));
            }
        };

        tokio::select! {
            biased;
            // Quorum commit + apply landing first upgrades the receipt to
            // quorum strength (never weaker than asked). Dropping the fsync
            // receiver leaves the (unbound-or-bound) waiter to be reaped.
            ff = &mut ff_rx => {
                self.shared_log.deregister_leader_disk_waiter(command_id);
                match ff {
                    Ok(Ok(response)) => Ok(LeaderDiskReceipt {
                        position: log_position_of(&response.log_id),
                        commit_ts,
                        command_id,
                        quorum_committed: true,
                    }),
                    Ok(Err(write_error)) => Err(map_client_write_api_error(write_error)),
                    // The raft core stopped without answering.
                    Err(_) => Err(ConsensusError::Closed),
                }
            }
            // The entry is durable on the leader's disk. The fire-and-forget
            // receiver is dropped: the entry still commits (or is truncated
            // on leader loss) normally — only the waiting ends here.
            signal = &mut fsync_rx => {
                match signal {
                    Ok(Ok(log_id)) => Ok(LeaderDiskReceipt {
                        position: log_position_of(&log_id),
                        commit_ts,
                        command_id,
                        quorum_committed: false,
                    }),
                    Ok(Err(LeaderDiskDrop::Truncated)) => Err(ConsensusError::NotLeader {
                        leader: self.raft.metrics().borrow().current_leader,
                    }),
                    Ok(Err(LeaderDiskDrop::Closed)) => Err(ConsensusError::Closed),
                    Err(_) => Err(ConsensusError::Raft(
                        "LeaderDisk waiter dropped before its fsync outcome".to_owned(),
                    )),
                }
            }
            fired = wait_control(control) => {
                self.shared_log.deregister_leader_disk_waiter(command_id);
                Err(fired)
            }
        }
    }

    /// Linearizable read barrier (spec section 11.4): confirms leadership
    /// with a quorum and waits until the state machine has applied the
    /// leader's read position. Reads at or below the returned position are
    /// linearizable. Never served by an unconfirmed leader.
    pub async fn read_index(
        &self,
        control: &ExecutionControl,
    ) -> Result<LogPosition, ConsensusError> {
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

    /// The current applied watermark (position + last applied commit
    /// timestamp) for read-barrier answers.
    fn read_watermark(&self) -> Result<ReadWatermark, ReadConsistencyError> {
        let record = self
            .sm
            .applied_record()
            .map_err(|e| ReadConsistencyError::Internal(e.to_string()))?;
        Ok(ReadWatermark {
            position: record.position(),
            commit_ts: record.last_applied_commit_ts,
        })
    }

    /// Waits until the local state machine applied at least `target.index`
    /// (read-your-writes primitive, spec section 11.4), then returns the
    /// applied watermark. Committed log prefixes are identical across
    /// replicas, so the index alone identifies the position; `target.term`
    /// is not consulted.
    pub async fn wait_applied(
        &self,
        target: LogPosition,
        control: &ExecutionControl,
    ) -> Result<ReadWatermark, ReadConsistencyError> {
        self.ensure_open().map_err(ReadConsistencyError::from)?;
        self.check_control(control)
            .map_err(ReadConsistencyError::from)?;
        self.raft
            .wait(remaining(control))
            .applied_index_at_least(Some(target.index), "read barrier wait applied")
            .await
            .map_err(|_| ReadConsistencyError::DeadlineExceeded)?;
        self.read_watermark()
    }

    /// Read barrier for one [`ReadConsistency`] level (spec section 11.4,
    /// Stage 2D). On success the caller may serve the read at or below the
    /// returned applied watermark.
    pub async fn consistent_read(
        &self,
        consistency: &ReadConsistency,
        control: &ExecutionControl,
    ) -> Result<ReadWatermark, ReadConsistencyError> {
        self.ensure_open().map_err(ReadConsistencyError::from)?;
        self.check_control(control)
            .map_err(ReadConsistencyError::from)?;
        match consistency {
            // read_index confirms leadership with a quorum and waits for the
            // local apply of the read position; an unconfirmed leader never
            // serves it (its ForwardToLeader / quorum-lack errors surface as
            // NotLeader / LeaderUnknown).
            ReadConsistency::Linearizable => {
                let _confirmed = self
                    .read_index(control)
                    .await
                    .map_err(ReadConsistencyError::from)?;
                self.read_watermark()
            }
            ReadConsistency::ReadYourWrites { token } => {
                if token.group_id != self.config.cluster_name {
                    return Err(ReadConsistencyError::InvalidSessionToken(format!(
                        "token group `{}` does not match this group `{}`",
                        token.group_id, self.config.cluster_name
                    )));
                }
                self.wait_applied(
                    LogPosition {
                        term: 0,
                        index: token.commit_index,
                    },
                    control,
                )
                .await
            }
            // Serve at the requested timestamp once the applied watermark
            // covers it. The watermark is monotonic in apply order (leaders
            // stamp above the commit floor and the state machine observes
            // every applied commit timestamp into the group clock — review
            // M1), so a satisfiable barrier stays satisfiable across leader
            // failover. The wait is caller-bounded: `control` firing maps to
            // `ReadConsistencyError::DeadlineExceeded`/`Cancelled`, which is
            // also the answer for a timestamp no committed entry will ever
            // cover (e.g. one from a truncated LeaderDisk write) — callers
            // must always set a deadline.
            ReadConsistency::Snapshot { timestamp } => loop {
                let watermark = self.read_watermark()?;
                if watermark
                    .commit_ts
                    .is_some_and(|applied| applied >= *timestamp)
                {
                    return Ok(watermark);
                }
                self.check_control(control)
                    .map_err(ReadConsistencyError::from)?;
                tokio::time::sleep(BARRIER_POLL_INTERVAL).await;
            },
            // Serve only if the replica is fresh (review m9; see
            // `read::evaluate_bounded_staleness` for the exact rule and the
            // best-effort caveat). The clock is only consulted when the
            // replica is behind, so skew fail-closed cannot refuse a
            // caught-up read.
            ReadConsistency::BoundedStaleness { max_lag_ms } => {
                let watermark = self.read_watermark()?;
                let last_known_index = self.raft.metrics().borrow().last_log_index;
                crate::read::evaluate_bounded_staleness(
                    &watermark,
                    last_known_index,
                    || {
                        self.hlc
                            .now()
                            .map_err(|e| ReadConsistencyError::Clock(e.to_string()))
                    },
                    *max_lag_ms,
                )?;
                Ok(watermark)
            }
            ReadConsistency::Eventual => self.read_watermark(),
        }
    }

    /// Committed entries strictly after `after`, in log order, bounded by the
    /// applied watermark (applied is always `<=` committed, so this never
    /// exposes an uncommitted entry). Command-less entries (blank,
    /// membership) are skipped.
    ///
    /// Review **N5**: if `after` falls at or behind the snapshot purge point
    /// the gap is reported as [`ConsensusError::LogPurgedGap`] instead of a
    /// silent hole-skip.
    pub fn read_committed(
        &self,
        after: LogPosition,
        limit: usize,
    ) -> Result<Vec<mongreldb_log::commit_log::CommittedEntry>, ConsensusError> {
        if let Some(purged) = self.shared_log.last_purged_log_id()? {
            if after.index > 0 && after.index <= purged.index {
                return Err(ConsensusError::LogPurgedGap {
                    after_index: after.index,
                    purged_index: purged.index,
                });
            }
        }
        let record = self.sm.applied_record()?;
        let bound = record
            .last_applied
            .as_ref()
            .map_or(0, |log_id| log_id.index);
        if bound <= after.index {
            return Ok(Vec::new());
        }
        let entries = self
            .shared_log
            .read_entries(after.index + 1, bound + 1, limit)?;
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
    ///
    /// Uses a 10 s default wait (historical). Prefer
    /// [`Self::snapshot_with_timeout`] (or pass the caller's
    /// [`ExecutionControl`] deadline via that helper) when the caller has a
    /// tighter budget — review **N4**.
    pub async fn snapshot(&self) -> Result<ConsensusSnapshot, ConsensusError> {
        self.snapshot_with_timeout(Duration::from_secs(10)).await
    }

    /// Like [`Self::snapshot`], but the wait for the snapshot to cover the
    /// applied watermark is bounded by `timeout` (review **N4**).
    pub async fn snapshot_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<ConsensusSnapshot, ConsensusError> {
        self.ensure_open()?;
        let applied = self.sm.applied_record()?;
        let Some(last_applied) = applied.last_applied else {
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
        let deadline = Instant::now() + timeout;
        loop {
            if let Some((meta, framed)) = self.sm.current_snapshot()? {
                let covers = meta
                    .last_log_id
                    .as_ref()
                    .is_some_and(|log_id| log_id.index >= last_applied.index);
                if covers {
                    let record = self.sm.applied_record()?;
                    return Ok(ConsensusSnapshot {
                        position: meta
                            .last_log_id
                            .as_ref()
                            .map_or(LogPosition::ZERO, log_position_of),
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

    /// Whether the group's committed membership is a joint (in-transition)
    /// config.
    pub fn is_joint_membership(&self) -> bool {
        let metrics_rx = self.raft.metrics();
        let metrics = metrics_rx.borrow();
        metrics
            .membership_config
            .membership()
            .get_joint_config()
            .len()
            > 1
    }

    /// Waits until a committed membership exists and is uniform (no in-flight
    /// membership change). Membership changes are serialized — openraft
    /// rejects a second one while another is in flight, including the initial
    /// bootstrap change before it commits — so a caller racing an in-flight
    /// change must wait it out rather than retry immediately.
    pub async fn wait_uniform_membership(&self, timeout: Duration) -> Result<(), ConsensusError> {
        self.ensure_open()?;
        let deadline = Instant::now() + timeout;
        loop {
            {
                let metrics_rx = self.raft.metrics();
                let metrics = metrics_rx.borrow();
                let settled = metrics.membership_config.log_id().is_some()
                    && metrics
                        .membership_config
                        .membership()
                        .get_joint_config()
                        .len()
                        == 1;
                if settled {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                return Err(ConsensusError::DeadlineExceeded);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
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
        // Keep this node from campaigning during the handoff so the target
        // does not race the old leader through repeated elections (openraft's
        // leader lease makes the first campaign rounds fail by design).
        // Always restored before returning.
        self.raft.runtime_config().elect(false);
        let result = async {
            self.transport.trigger_election(target).await?;
            self.raft
                .wait(Some(timeout))
                .current_leader(target, "leadership transfer")
                .await
                .map_err(|_| ConsensusError::DeadlineExceeded)?;
            Ok(())
        }
        .await;
        self.raft.runtime_config().elect(true);
        result
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

/// Poll interval for cooperative cancellation/deadline checks and the
/// snapshot-read apply wait (ExecutionControl has no async notification).
const BARRIER_POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Resolves when `control` fires (cancellation or deadline); pends forever
/// when neither is configured. Used to bound the LeaderDisk fsync race.
async fn wait_control(control: &ExecutionControl) -> ConsensusError {
    loop {
        if let Err(err) = control.check() {
            return match err {
                mongreldb_log::commit_log::LogError::Cancelled => ConsensusError::Cancelled,
                mongreldb_log::commit_log::LogError::DeadlineExceeded => {
                    ConsensusError::DeadlineExceeded
                }
                other => ConsensusError::Raft(other.to_string()),
            };
        }
        tokio::time::sleep(BARRIER_POLL_INTERVAL).await;
    }
}

/// Maps a fatal raft-core failure (returned by `client_write_ff`).
fn map_fatal(err: Fatal<RaftNodeId>) -> ConsensusError {
    match err {
        Fatal::Stopped => ConsensusError::Closed,
        other => ConsensusError::Raft(other.to_string()),
    }
}

/// Maps the raft core's client-write answer (the `ForwardToLeader` reply a
/// non-leader sends through the responder, spec section 11.7).
fn map_client_write_api_error(err: ClientWriteError<RaftNodeId, BasicNode>) -> ConsensusError {
    match err {
        ClientWriteError::ForwardToLeader(forward) => ConsensusError::NotLeader {
            leader: forward.leader_id,
        },
        ClientWriteError::ChangeMembershipError(e) => match e {
            openraft::error::ChangeMembershipError::InProgress(_) => {
                ConsensusError::MembershipInProgress
            }
            other => ConsensusError::InvalidRequest(other.to_string()),
        },
    }
}

fn map_client_write_error(
    err: RaftError<RaftNodeId, ClientWriteError<RaftNodeId, BasicNode>>,
) -> ConsensusError {
    match err {
        RaftError::APIError(api) => map_client_write_api_error(api),
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
