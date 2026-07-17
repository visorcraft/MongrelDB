//! Multi-node consensus group scenarios over the in-memory transport
//! (spec section 11.2, S2B-001 through S2B-004): election, replication,
//! failover, partition safety (no split-brain), snapshot catch-up, membership
//! changes, leadership transfer, idempotent apply across restart, and
//! crash durability.
//!
//! All scenarios run in a fixed order with fixed link policies — no
//! randomized scheduling — so there are no seeds to persist.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mongreldb_consensus::error::ConsensusError;
use mongreldb_consensus::group::{ConsensusGroup, GroupConfig};
use mongreldb_consensus::identity::CommandKind;
use mongreldb_consensus::network::InMemoryTransport;
use mongreldb_consensus::raft_log::RaftCommitLog;
use mongreldb_consensus::state_machine::{ApplySink, InMemoryApplySink};
use mongreldb_log::commit_log::{CommitLog, ExecutionControl, LogPosition};
use mongreldb_log::envelope::CommandEnvelope;
use openraft::BasicNode;
use tempfile::TempDir;

const FAST: Duration = Duration::from_millis(5);
const LEADER_TIMEOUT: Duration = Duration::from_secs(10);

fn envelope(seq: u64) -> CommandEnvelope {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&seq.to_le_bytes());
    CommandEnvelope::new(1, id, format!("cmd-{seq}").into_bytes())
}

fn group_config(node: u64, dir: &std::path::Path, cluster: &str) -> GroupConfig {
    let mut config = GroupConfig::new(cluster, node, dir.to_path_buf());
    config.heartbeat_interval = Duration::from_millis(50);
    config.election_timeout_min = Duration::from_millis(150);
    config.election_timeout_max = Duration::from_millis(300);
    config.install_snapshot_timeout = Duration::from_millis(1_000);
    config
}

struct TestCluster {
    tmp: TempDir,
    transport: Arc<InMemoryTransport>,
    groups: BTreeMap<u64, Arc<ConsensusGroup<InMemoryTransport>>>,
    sinks: BTreeMap<u64, Arc<Mutex<InMemoryApplySink>>>,
}

impl TestCluster {
    fn new() -> Self {
        TestCluster {
            tmp: tempfile::tempdir().unwrap(),
            transport: Arc::new(InMemoryTransport::new()),
            groups: BTreeMap::new(),
            sinks: BTreeMap::new(),
        }
    }

    async fn start_node(&mut self, id: u64) {
        let dir = self.tmp.path().join(format!("node-{id}"));
        let sink = Arc::new(Mutex::new(InMemoryApplySink::new()));
        let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink.clone();
        let group = ConsensusGroup::create(
            group_config(id, &dir, "test-cluster"),
            self.transport.clone(),
            dyn_sink,
        )
        .await
        .unwrap();
        self.groups.insert(id, Arc::new(group));
        self.sinks.insert(id, sink);
    }

    async fn bootstrapped(ids: &[u64]) -> Self {
        let mut cluster = TestCluster::new();
        for &id in ids {
            cluster.start_node(id).await;
        }
        let members: BTreeMap<u64, BasicNode> = ids
            .iter()
            .map(|&id| (id, BasicNode::new(format!("node-{id}"))))
            .collect();
        // openraft: initializing every pristine node with the same membership
        // is safe.
        for &id in ids {
            cluster.groups[&id].bootstrap(members.clone()).await.unwrap();
        }
        cluster
    }

    fn group(&self, id: u64) -> Arc<ConsensusGroup<InMemoryTransport>> {
        self.groups[&id].clone()
    }

    fn sink(&self, id: u64) -> Arc<Mutex<InMemoryApplySink>> {
        self.sinks[&id].clone()
    }

    /// Waits until every node agrees on one leader; returns its id.
    async fn wait_consensus_leader(&self, among: &[u64]) -> u64 {
        let deadline = Instant::now() + LEADER_TIMEOUT;
        loop {
            let mut leaders = BTreeMap::new();
            for &id in among {
                if let Some(group) = self.groups.get(&id) {
                    if let Some(leader) = group.metrics().current_leader {
                        leaders.insert(id, leader);
                    }
                }
            }
            if leaders.len() == among.len() && leaders.values().all(|l| *l == *leaders.values().next().unwrap()) {
                return *leaders.values().next().unwrap();
            }
            assert!(Instant::now() < deadline, "no consensus leader among {among:?} (saw {leaders:?})");
            tokio::time::sleep(FAST).await;
        }
    }

    async fn wait_applied(&self, id: u64, index: u64) {
        self.group(id)
            .wait_applied_index(index, LEADER_TIMEOUT)
            .await
            .unwrap();
    }

    fn applied_envelopes(&self, id: u64) -> Vec<CommandEnvelope> {
        self.sink(id)
            .lock()
            .unwrap()
            .applied()
            .iter()
            .map(|applied| applied.envelope().expect("command").clone())
            .collect()
    }

    async fn shutdown(self) {
        for group in self.groups.values() {
            let _ = group.shutdown().await;
        }
    }
}

fn deadline_control(ms: u64) -> ExecutionControl {
    ExecutionControl {
        deadline: Some(Instant::now() + Duration::from_millis(ms)),
        cancellation: None,
    }
}

// ---------------------------------------------------------------------------
// Single node
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_propose_read_snapshot_install_round_trip() {
    let mut cluster = TestCluster::new();
    cluster.start_node(1).await;
    cluster
        .group(1)
        .bootstrap(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    let group = cluster.group(1);
    assert_eq!(group.wait_leader(LEADER_TIMEOUT).await.unwrap(), 1);

    let mut first_index = None;
    let mut last_position = LogPosition::ZERO;
    for seq in 1..=3u64 {
        let receipt = group
            .propose(CommandKind::Transaction, envelope(seq), &ExecutionControl::default())
            .await
            .unwrap();
        assert!(receipt.position.term >= 1);
        match first_index {
            None => first_index = Some(receipt.position.index),
            Some(first) => assert_eq!(receipt.position.index, first + seq - 1),
        }
        assert!(!receipt.response.duplicate);
        assert_eq!(receipt.command_id, Some(envelope(seq).command_id));
        last_position = receipt.position;
    }
    assert_eq!(group.applied_position(), last_position);

    let committed = group.read_committed(LogPosition::ZERO, 10).unwrap();
    assert_eq!(committed.len(), 3);
    assert_eq!(committed[0].envelope, envelope(1));
    assert!(committed
        .iter()
        .all(|entry| entry.commit_ts > mongreldb_types::hlc::HlcTimestamp::ZERO));

    // Snapshot, then install it into a fresh node's state (CommitLog-level
    // install; the raft-driven install path is Stage 2E).
    let snapshot = group.snapshot().await.unwrap();
    assert_eq!(snapshot.position, last_position);

    let mut other = TestCluster::new();
    other.start_node(9).await;
    let fresh = other.group(9);
    fresh.install_snapshot(&snapshot).unwrap();
    assert_eq!(fresh.applied_position(), snapshot.position);
    assert_eq!(cluster.applied_envelopes(1), other.applied_envelopes(9));

    cluster.shutdown().await;
    other.shutdown().await;
}

// ---------------------------------------------------------------------------
// Three-node cluster
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_node_election_and_replication() {
    let cluster = TestCluster::bootstrapped(&[1, 2, 3]).await;
    let leader = cluster.wait_consensus_leader(&[1, 2, 3]).await;

    let metrics = [1, 2, 3].map(|id| cluster.group(id).metrics());
    assert_eq!(
        metrics.iter().filter(|m| matches!(m.state, mongreldb_consensus::group::RaftServerState::Leader)).count(),
        1,
        "exactly one effective leader per term (spec 4.2)"
    );
    assert!(metrics.iter().all(|m| m.current_term == metrics[0].current_term));

    for seq in 1..=5u64 {
        cluster
            .group(leader)
            .propose(CommandKind::Transaction, envelope(seq), &ExecutionControl::default())
            .await
            .unwrap();
    }
    for &id in &[1, 2, 3] {
        let last = cluster.group(leader).metrics().last_log_index.unwrap();
        cluster.wait_applied(id, last).await;
        assert_eq!(cluster.applied_envelopes(id).len(), 5);
    }
    // All replicas applied the identical commands with identical timestamps.
    for &id in &[2, 3] {
        assert_eq!(cluster.applied_envelopes(1), cluster.applied_envelopes(id));
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn leader_kill_triggers_new_election() {
    let cluster = TestCluster::bootstrapped(&[1, 2, 3]).await;
    let leader = cluster.wait_consensus_leader(&[1, 2, 3]).await;
    cluster
        .group(leader)
        .propose(CommandKind::Transaction, envelope(1), &ExecutionControl::default())
        .await
        .unwrap();

    // Kill the leader (graceful stop + detach).
    cluster.group(leader).shutdown().await.unwrap();

    let survivors: Vec<u64> = [1, 2, 3].into_iter().filter(|id| *id != leader).collect();
    let new_leader = cluster.wait_consensus_leader(&survivors).await;
    assert_ne!(new_leader, leader);

    cluster
        .group(new_leader)
        .propose(CommandKind::Transaction, envelope(2), &ExecutionControl::default())
        .await
        .unwrap();
    let last = cluster.group(new_leader).metrics().last_log_index.unwrap();
    for &id in &survivors {
        cluster.wait_applied(id, last).await;
        assert_eq!(cluster.applied_envelopes(id).len(), 2);
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn minority_partition_cannot_commit_no_split_brain() {
    let cluster = TestCluster::bootstrapped(&[1, 2, 3]).await;
    let leader = cluster.wait_consensus_leader(&[1, 2, 3]).await;
    let others: Vec<u64> = [1, 2, 3].into_iter().filter(|id| *id != leader).collect();

    cluster
        .group(leader)
        .propose(CommandKind::Transaction, envelope(1), &ExecutionControl::default())
        .await
        .unwrap();

    // Isolate the leader: a partition may produce candidates, but only a
    // quorum-authorized leader may commit (spec 4.2).
    cluster.transport.partition(&[leader], &others);

    // The partitioned leader's proposal never commits.
    let partitioned = envelope(100);
    let result = cluster
        .group(leader)
        .propose(CommandKind::Transaction, partitioned.clone(), &deadline_control(1_000))
        .await;
    assert!(result.is_err(), "minority leader must not commit: {result:?}");

    // The majority elects a new leader and keeps committing.
    let new_leader = cluster.wait_consensus_leader(&others).await;
    assert_ne!(new_leader, leader);
    cluster
        .group(new_leader)
        .propose(CommandKind::Transaction, envelope(2), &ExecutionControl::default())
        .await
        .unwrap();

    // No replica ever applied the partitioned command.
    for &id in &others {
        let last = cluster.group(new_leader).metrics().last_log_index.unwrap();
        cluster.wait_applied(id, last).await;
        assert!(!cluster.applied_envelopes(id).contains(&partitioned));
    }

    // Heal: the old leader catches up and drops its uncommitted entry.
    cluster.transport.heal();
    let last = cluster.group(new_leader).metrics().last_log_index.unwrap();
    cluster.wait_applied(leader, last).await;
    let healed = cluster.applied_envelopes(leader);
    assert!(!healed.contains(&partitioned));
    assert_eq!(healed, cluster.applied_envelopes(new_leader));
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn lagging_follower_catches_up_via_snapshot() {
    let mut cluster = TestCluster::new();
    // Aggressive snapshot policy so the lagging follower must be brought up
    // to date by a snapshot, not by log replication (log truncation).
    for id in [1, 2, 3] {
        let dir = cluster.tmp.path().join(format!("node-{id}"));
        let sink = Arc::new(Mutex::new(InMemoryApplySink::new()));
        let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink.clone();
        let mut config = group_config(id, &dir, "test-cluster");
        config.snapshot_policy_logs = 5;
        config.max_in_snapshot_log_to_keep = 2;
        config.replication_lag_threshold = 8;
        let group = ConsensusGroup::create(config, cluster.transport.clone(), dyn_sink)
            .await
            .unwrap();
        cluster.groups.insert(id, Arc::new(group));
        cluster.sinks.insert(id, sink);
    }
    let members: BTreeMap<u64, BasicNode> = [1, 2, 3]
        .iter()
        .map(|&id| (id, BasicNode::new(format!("node-{id}"))))
        .collect();
    for group in cluster.groups.values() {
        group.bootstrap(members.clone()).await.unwrap();
    }
    let leader = cluster.wait_consensus_leader(&[1, 2, 3]).await;
    let follower = [1, 2, 3].into_iter().find(|id| *id != leader).unwrap();
    let other = [1, 2, 3]
        .into_iter()
        .find(|id| *id != leader && *id != follower)
        .unwrap();

    // Partition the follower, then push enough entries to snapshot and purge
    // past its match index.
    cluster.transport.partition(&[follower], &[leader, other]);
    for seq in 1..=12u64 {
        cluster
            .group(leader)
            .propose(CommandKind::Transaction, envelope(seq), &ExecutionControl::default())
            .await
            .unwrap();
    }

    // Wait until the leader's snapshot covers most of the log.
    let deadline = Instant::now() + LEADER_TIMEOUT;
    loop {
        let snapshot = cluster.group(leader).metrics().snapshot;
        if snapshot.is_some_and(|pos| pos.index >= 10) {
            break;
        }
        assert!(Instant::now() < deadline, "leader never snapshotted: {snapshot:?}");
        tokio::time::sleep(FAST).await;
    }

    // Heal: the follower must catch up through snapshot install.
    cluster.transport.heal();
    let last = cluster.group(leader).metrics().last_log_index.unwrap();
    cluster.wait_applied(follower, last).await;
    assert_eq!(cluster.applied_envelopes(follower), cluster.applied_envelopes(leader));

    // The follower installed a snapshot (its snapshot position advanced).
    let follower_snapshot = cluster.group(follower).metrics().snapshot;
    assert!(
        follower_snapshot.is_some_and(|pos| pos.index >= 10),
        "expected snapshot install on the lagging follower: {follower_snapshot:?}"
    );
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// Membership and leadership
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn membership_add_learner_promote_remove() {
    let mut cluster = TestCluster::bootstrapped(&[1, 2, 3]).await;
    let leader = cluster.wait_consensus_leader(&[1, 2, 3]).await;
    cluster
        .group(leader)
        .propose(CommandKind::Transaction, envelope(1), &ExecutionControl::default())
        .await
        .unwrap();

    // Node 4 joins as a learner and catches up.
    cluster.start_node(4).await;
    cluster
        .group(leader)
        .add_learner(4, BasicNode::new("node-4"))
        .await
        .unwrap();
    let last = cluster.group(leader).metrics().last_log_index.unwrap();
    cluster.wait_applied(4, last).await;
    assert_eq!(cluster.applied_envelopes(4), vec![envelope(1)]);

    // Promote through joint consensus: 4 becomes a voter.
    cluster.group(leader).promote(4).await.unwrap();
    let deadline = Instant::now() + LEADER_TIMEOUT;
    loop {
        let (voters, _) = cluster.group(leader).members();
        if voters.len() == 4 && voters.contains(&4) {
            break;
        }
        assert!(Instant::now() < deadline, "promotion not visible: {voters:?}");
        tokio::time::sleep(FAST).await;
    }

    // Remove node 2 through joint consensus.
    cluster.group(leader).remove(2).await.unwrap();
    let deadline = Instant::now() + LEADER_TIMEOUT;
    loop {
        let (voters, _) = cluster.group(leader).members();
        if voters.len() == 3 && !voters.contains(&2) {
            break;
        }
        assert!(Instant::now() < deadline, "removal not visible: {voters:?}");
        tokio::time::sleep(FAST).await;
    }

    // The reconfigured cluster still commits.
    cluster
        .group(leader)
        .propose(CommandKind::Transaction, envelope(2), &ExecutionControl::default())
        .await
        .unwrap();
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn transfer_leader_to_voter() {
    let cluster = TestCluster::bootstrapped(&[1, 2, 3]).await;
    let leader = cluster.wait_consensus_leader(&[1, 2, 3]).await;
    let target = [1, 2, 3].into_iter().find(|id| *id != leader).unwrap();

    cluster
        .group(leader)
        .transfer_leader(target, LEADER_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(cluster.wait_consensus_leader(&[1, 2, 3]).await, target);

    cluster
        .group(target)
        .propose(CommandKind::Transaction, envelope(1), &ExecutionControl::default())
        .await
        .unwrap();

    // Transferring from a non-leader is rejected with the leader hint.
    let err = cluster
        .group(leader)
        .transfer_leader(target, Duration::from_millis(500))
        .await
        .unwrap_err();
    match err {
        ConsensusError::NotLeader { leader: Some(hint) } => assert_eq!(hint, target),
        other => panic!("expected NotLeader with the target hint, got {other:?}"),
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn read_index_linearizable_barrier() {
    let cluster = TestCluster::bootstrapped(&[1, 2, 3]).await;
    let leader = cluster.wait_consensus_leader(&[1, 2, 3]).await;
    for seq in 1..=3u64 {
        cluster
            .group(leader)
            .propose(CommandKind::Transaction, envelope(seq), &ExecutionControl::default())
            .await
            .unwrap();
    }

    // A confirmed leader serves a barrier at or above the applied position.
    let position = cluster
        .group(leader)
        .read_index(&ExecutionControl::default())
        .await
        .unwrap();
    assert!(position.index >= 3);

    // An isolated ex-leader may not serve linearizable reads (spec 11.4).
    let others: Vec<u64> = [1, 2, 3].into_iter().filter(|id| *id != leader).collect();
    cluster.transport.partition(&[leader], &others);
    let result = cluster
        .group(leader)
        .read_index(&deadline_control(1_500))
        .await;
    assert!(
        matches!(result, Err(ConsensusError::NotLeader { .. } | ConsensusError::DeadlineExceeded)),
        "unconfirmed leader must not serve a read barrier: {result:?}"
    );
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// Idempotency and durability across restart
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idempotent_apply_across_client_retry_and_restart() {
    let mut cluster = TestCluster::new();
    cluster.start_node(1).await;
    cluster
        .group(1)
        .bootstrap(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    let group = cluster.group(1);
    group.wait_leader(LEADER_TIMEOUT).await.unwrap();

    let command = envelope(42);
    let first = group
        .propose(CommandKind::Transaction, command.clone(), &ExecutionControl::default())
        .await
        .unwrap();
    assert!(!first.response.duplicate);

    // Client retry of the same command id: commits (a second log order entry)
    // but is applied once (S2B-004).
    let retry = group
        .propose(CommandKind::Transaction, command.clone(), &ExecutionControl::default())
        .await
        .unwrap();
    assert!(retry.response.duplicate);
    assert_eq!(retry.position.index, first.position.index + 1);
    assert_eq!(cluster.sink(1).lock().unwrap().len(), 1);

    // Process-free crash: stop without graceful storage close; everything
    // fsynced (including the idempotency checkpoint) survives.
    let sink = cluster.sink(1);
    drop(cluster.groups.remove(&1));
    let transport = cluster.transport.clone();
    let dir = cluster.tmp.path().join("node-1");
    eprintln!("[t] into_inner");
    let crashed = Arc::into_inner(group).expect("sole group owner");
    crashed.crash().await;
    eprintln!("[t] crashed, reopening");

    let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink.clone();
    let reopened = ConsensusGroup::create(group_config(1, &dir, "test-cluster"), transport, dyn_sink)
        .await
        .unwrap();
    reopened.wait_leader(LEADER_TIMEOUT).await.unwrap();
    assert_eq!(reopened.applied_position(), retry.position);

    // The retried command is still idempotent after the restart.
    let after = reopened
        .propose(CommandKind::Transaction, command, &ExecutionControl::default())
        .await
        .unwrap();
    assert!(after.response.duplicate);
    assert_eq!(sink.lock().unwrap().len(), 1);
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_durable_entries_survive_restart() {
    let mut cluster = TestCluster::new();
    eprintln!("[t] start_node");
    cluster.start_node(1).await;
    eprintln!("[t] bootstrap");
    cluster
        .group(1)
        .bootstrap(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    let group = cluster.group(1);
    group.wait_leader(LEADER_TIMEOUT).await.unwrap();
    for seq in 1..=5u64 {
        group
            .propose(CommandKind::Transaction, envelope(seq), &ExecutionControl::default())
            .await
            .unwrap();
    }
    let term_before = group.metrics().current_term;
    let last_before = group.applied_position();
    eprintln!("[t] proposed 5, crashing");

    // Crash (no graceful close), then reopen from the same directory.
    let transport = cluster.transport.clone();
    let dir = cluster.tmp.path().join("node-1");
    let sink = cluster.sink(1);
    drop(cluster.groups.remove(&1));
    eprintln!("[t] into_inner");
    let crashed = Arc::into_inner(group).expect("sole group owner");
    crashed.crash().await;
    eprintln!("[t] crashed, reopening");

    let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink.clone();
    let reopened = ConsensusGroup::create(group_config(1, &dir, "test-cluster"), transport, dyn_sink)
        .await
        .unwrap();
    // Hard state survived: the node comes back initialized in a term no
    // lower than before the crash.
    assert!(reopened.is_initialized().await.unwrap());
    assert!(reopened.metrics().current_term >= term_before);
    assert_eq!(reopened.applied_position(), last_before);
    let committed = reopened.read_committed(LogPosition::ZERO, 10).unwrap();
    assert_eq!(committed.len(), 5);
    assert_eq!(committed[4].envelope, envelope(5));

    eprintln!("[t] reopened");
    // The log continues without index reuse.
    reopened.wait_leader(LEADER_TIMEOUT).await.unwrap();
    eprintln!("[t] leader after reopen");
    let receipt = reopened
        .propose(CommandKind::Transaction, envelope(6), &ExecutionControl::default())
        .await
        .unwrap();
    assert_eq!(receipt.position.index, last_before.index + 1);
    reopened.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// RaftCommitLog (the CommitLog trait surface)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_commit_log_round_trip() {
    let mut cluster = TestCluster::new();
    cluster.start_node(1).await;
    cluster
        .group(1)
        .bootstrap(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    let group = cluster.group(1);
    group.wait_leader(LEADER_TIMEOUT).await.unwrap();
    let log = RaftCommitLog::new(group.clone());
    log::sanity(&log);

    let receipt = log.propose(envelope(1), &ExecutionControl::default()).unwrap();
    let first = receipt.log_position;
    assert!(first.term >= 1);
    assert_eq!(receipt.durability, mongreldb_log::commit_log::DurabilityLevel::Quorum);
    assert_eq!(
        receipt.transaction_id,
        mongreldb_types::ids::TransactionId::from_bytes(envelope(1).command_id)
    );
    assert!(receipt.commit_ts > mongreldb_types::hlc::HlcTimestamp::ZERO);

    let second = log.propose(envelope(2), &ExecutionControl::default()).unwrap();
    assert_eq!(second.log_position.index, first.index + 1);
    assert_eq!(log.applied_position(), second.log_position);
    let committed = log.read_committed(LogPosition::ZERO, 10).unwrap();
    assert_eq!(committed.len(), 2);
    assert_eq!(committed[1].envelope, envelope(2));
    let page = log.read_committed(first, 10).unwrap();
    assert_eq!(page.len(), 1);

    // Snapshot through the CommitLog surface and install into a fresh log.
    let snapshot = log.create_snapshot().unwrap();
    assert_eq!(snapshot.position, second.log_position);

    let mut other = TestCluster::new();
    other.start_node(9).await;
    let other_group = other.group(9);
    let other_log = RaftCommitLog::new(other_group.clone());
    other_log.install_snapshot(snapshot).unwrap();
    assert_eq!(other_log.applied_position(), second.log_position);
    assert_eq!(cluster.applied_envelopes(1), other.applied_envelopes(9));

    log.shutdown().await.unwrap();
    other_log.shutdown().await.unwrap();
}

mod log {
    use mongreldb_consensus::network::InMemoryTransport;
    use mongreldb_consensus::raft_log::RaftCommitLog;
    use mongreldb_log::commit_log::CommitLog;

    /// Static check that RaftCommitLog implements CommitLog.
    pub fn sanity(log: &RaftCommitLog<InMemoryTransport>) {
        fn assert_commit_log<T: CommitLog>(_: &T) {}
        assert_commit_log(log);
    }
}
