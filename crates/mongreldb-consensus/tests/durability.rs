//! Stage 2C write-protocol scenarios (spec section 11.3): LeaderDisk
//! durability receipts, their honesty rules, and routed `NotLeader` errors.
//!
//! Fault-hook scenarios share the process-global `mongreldb_fault` registry,
//! so they run sequentially inside ONE test function of this binary (same
//! discipline as `fault_hooks.rs`); hook-free scenarios get their own
//! functions.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mongreldb_consensus::group::{ConsensusGroup, GroupConfig};
use mongreldb_consensus::network::InMemoryTransport;
use mongreldb_consensus::raft_log::RaftCommitLog;
use mongreldb_consensus::state_machine::{ApplySink, InMemoryApplySink};
use mongreldb_fault::{Action, BarrierAction, ScopedGuard};
use mongreldb_log::commit_log::{
    CommitLog, DurabilityLevel, ExecutionControl, LogError, LogPosition,
};
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
    config
}

/// Slow elections for the partition scenario: a partitioned leader must not
/// be replaced (and the partition must not trigger follower elections)
/// inside the test's millisecond window.
fn slow_election_config(node: u64, dir: &std::path::Path, cluster: &str) -> GroupConfig {
    let mut config = group_config(node, dir, cluster);
    config.election_timeout_min = Duration::from_millis(1_500);
    config.election_timeout_max = Duration::from_millis(3_000);
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

    async fn start_node_with(&mut self, id: u64, config: GroupConfig) {
        let sink = Arc::new(Mutex::new(InMemoryApplySink::new()));
        let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink.clone();
        let group = ConsensusGroup::create(config, self.transport.clone(), dyn_sink)
            .await
            .unwrap();
        self.groups.insert(id, Arc::new(group));
        self.sinks.insert(id, sink);
    }

    async fn start_node(&mut self, id: u64) {
        let dir = self.tmp.path().join(format!("node-{id}"));
        self.start_node_with(id, group_config(id, &dir, "test-cluster"))
            .await;
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
        cluster.groups[&ids[0]].bootstrap(members).await.unwrap();
        cluster
    }

    fn group(&self, id: u64) -> Arc<ConsensusGroup<InMemoryTransport>> {
        self.groups[&id].clone()
    }

    fn sink(&self, id: u64) -> Arc<Mutex<InMemoryApplySink>> {
        self.sinks[&id].clone()
    }

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
            if leaders.len() == among.len()
                && leaders
                    .values()
                    .all(|l| *l == *leaders.values().next().unwrap())
            {
                return *leaders.values().next().unwrap();
            }
            assert!(
                Instant::now() < deadline,
                "no consensus leader among {among:?} (saw {leaders:?})"
            );
            tokio::time::sleep(FAST).await;
        }
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
// LeaderDisk durability (spec 11.3)
// ---------------------------------------------------------------------------

/// A LeaderDisk receipt is issued from the leader's local fsync alone:
/// with every follower partitioned away the entry is durable on the leader
/// but NOT committed when the receipt is delivered; after the partition
/// heals the same entry commits normally (the fire-and-forget write keeps
/// running in the background).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn leader_disk_receipt_precedes_commit_then_commits_after_heal() {
    let mut cluster = TestCluster::new();
    for id in [1, 2, 3] {
        let dir = cluster.tmp.path().join(format!("node-{id}"));
        cluster
            .start_node_with(id, slow_election_config(id, &dir, "test-cluster"))
            .await;
    }
    let members: BTreeMap<u64, BasicNode> = [1, 2, 3]
        .iter()
        .map(|&id| (id, BasicNode::new(format!("node-{id}"))))
        .collect();
    cluster.group(1).bootstrap(members).await.unwrap();
    let leader = cluster.wait_consensus_leader(&[1, 2, 3]).await;
    let others: Vec<u64> = [1, 2, 3].into_iter().filter(|id| *id != leader).collect();
    let log = RaftCommitLog::with_durability(cluster.group(leader), DurabilityLevel::LeaderDisk);

    // Isolate the leader: the entry can be fsynced locally but can never
    // reach a quorum.
    cluster.transport.partition(&[leader], &others);

    let receipt = log
        .propose(envelope(1), &deadline_control(5_000))
        .expect("LeaderDisk receipt must come from the local fsync alone");
    assert_eq!(receipt.durability, DurabilityLevel::LeaderDisk);
    assert!(receipt.log_position.term >= 1);
    assert!(receipt.commit_ts > mongreldb_types::hlc::HlcTimestamp::ZERO);

    // Honesty: the receipt is NOT a commit declaration (spec 4.4) — the
    // entry is neither applied nor readable as committed at this point.
    let applied = cluster.group(leader).applied_position();
    assert!(
        applied.index < receipt.log_position.index,
        "entry must be uncommitted at the LeaderDisk receipt: applied {applied:?}, receipt {:?}",
        receipt.log_position
    );
    assert!(log
        .read_committed(LogPosition::ZERO, 10)
        .unwrap()
        .iter()
        .all(|entry| entry.position != receipt.log_position));

    // Heal: the background fire-and-forget write commits normally.
    cluster.transport.heal();
    cluster
        .group(leader)
        .wait_applied_index(receipt.log_position.index, LEADER_TIMEOUT)
        .await
        .unwrap();
    let committed = log.read_committed(LogPosition::ZERO, 10).unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].envelope, envelope(1));
    assert_eq!(committed[0].position, receipt.log_position);
    for &id in &others {
        cluster
            .group(id)
            .wait_applied_index(receipt.log_position.index, LEADER_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(cluster.sink(id).lock().unwrap().len(), 1);
    }
    cluster.shutdown().await;
}

/// The Deferred fsync policy releases the LeaderDisk signal from the
/// background flusher, after the batch fsync — the waiter rides the same
/// durability boundary as the openraft flush callbacks (S2C).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_disk_receipt_under_deferred_fsync_policy() {
    let mut cluster = TestCluster::new();
    let dir = cluster.tmp.path().join("node-1");
    let mut config = group_config(1, &dir, "test-cluster");
    config.storage.fsync_policy = mongreldb_consensus::storage::FsyncPolicy::Deferred {
        interval: Duration::from_millis(10),
    };
    cluster.start_node_with(1, config).await;
    cluster
        .group(1)
        .bootstrap(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    let group = cluster.group(1);
    group.wait_leader(LEADER_TIMEOUT).await.unwrap();
    let log = RaftCommitLog::with_durability(group.clone(), DurabilityLevel::LeaderDisk);

    // Without the flusher-side signal this propose could only time out.
    let receipt = log
        .propose(envelope(1), &deadline_control(5_000))
        .expect("the Deferred flusher must release the LeaderDisk signal");
    // On a single node the quorum commit can race the flusher; either way
    // the receipt exists only because a post-fsync boundary released it.
    assert!(matches!(
        receipt.durability,
        DurabilityLevel::LeaderDisk | DurabilityLevel::Quorum
    ));
    // The entry still commits normally.
    cluster
        .group(1)
        .wait_applied_index(receipt.log_position.index, LEADER_TIMEOUT)
        .await
        .unwrap();
    cluster.shutdown().await;
}

/// `NotLeader` is routed (S2C): the error carries the current leader hint
/// for the gateway's retry (spec section 11.7), for both durability levels.
/// Its category semantics (`ErrorCategory::NotLeader`) are documented on
/// the variant; the `LogError` → `ErrorCategory` bridge lands with the
/// Stage 2G gateway wave (no bridge exists today).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn not_leader_propose_carries_leader_hint() {
    let cluster = TestCluster::bootstrapped(&[1, 2, 3]).await;
    let leader = cluster.wait_consensus_leader(&[1, 2, 3]).await;
    let follower = [1, 2, 3].into_iter().find(|id| *id != leader).unwrap();

    for durability in [DurabilityLevel::Quorum, DurabilityLevel::LeaderDisk] {
        let log = RaftCommitLog::with_durability(cluster.group(follower), durability);
        let err = log
            .propose(envelope(1), &deadline_control(5_000))
            .expect_err("a follower must never acknowledge a write");
        match err {
            LogError::NotLeader { leader_hint } => {
                assert_eq!(
                    leader_hint.as_deref(),
                    Some(leader.to_string().as_str()),
                    "the hint routes the retry to the current leader ({durability:?})"
                );
            }
            other => panic!("expected routed NotLeader ({durability:?}), got {other:?}"),
        }
    }
    cluster.shutdown().await;
}

/// Barrier-synchronized proof that the LeaderDisk fsync signal never
/// precedes the fsync, plus the injected-failure honesty rule: a failed
/// fsync never produces a receipt. One test function because the fault
/// registry is process-global.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fsync_hooks_gate_leader_disk_receipts() {
    // (a) The signal follows the fsync: when the receipt is delivered, the
    // `raft.log.fsync.after` boundary has already been crossed.
    {
        let mut cluster = TestCluster::new();
        cluster.start_node(1).await;
        cluster
            .group(1)
            .bootstrap(BTreeMap::from([(1, BasicNode::new("node-1"))]))
            .await
            .unwrap();
        let group = cluster.group(1);
        group.wait_leader(LEADER_TIMEOUT).await.unwrap();
        // Exercise the append path once so the leadership blank entry is
        // durable before the barrier is armed.
        let warmup = RaftCommitLog::new(group.clone());
        warmup
            .propose(envelope(0), &ExecutionControl::default())
            .unwrap();

        let _guard = ScopedGuard::new(
            "raft.log.fsync.after",
            Action::Barrier(BarrierAction::new("leader-disk-fsync")),
        );
        let log = RaftCommitLog::with_durability(group.clone(), DurabilityLevel::LeaderDisk);
        let propose =
            std::thread::spawn(move || log.propose(envelope(1), &deadline_control(5_000)));
        // Blocks until the fsync boundary was crossed; only then may the
        // receipt exist.
        tokio::task::block_in_place(|| {
            mongreldb_fault::wait_barrier("leader-disk-fsync", 1, LEADER_TIMEOUT).unwrap();
        });
        let receipt = propose.join().unwrap().unwrap();
        // The barrier already arrived when the receipt was delivered:
        // arrival is recorded inside the hook, before the signal is sent.
        mongreldb_fault::wait_barrier("leader-disk-fsync", 1, Duration::ZERO).unwrap();
        assert!(matches!(
            receipt.durability,
            DurabilityLevel::LeaderDisk | DurabilityLevel::Quorum
        ));
        assert!(mongreldb_fault::hits("raft.log.fsync.after") >= 1);
        cluster.shutdown().await;
    }

    // (b) An injected fsync failure means no receipt, ever: the signal is
    // strictly post-fsync, so the propose can only fail.
    {
        let mut cluster = TestCluster::new();
        cluster.start_node(2).await;
        cluster
            .group(2)
            .bootstrap(BTreeMap::from([(2, BasicNode::new("node-2"))]))
            .await
            .unwrap();
        let group = cluster.group(2);
        group.wait_leader(LEADER_TIMEOUT).await.unwrap();
        let warmup = RaftCommitLog::new(group.clone());
        warmup
            .propose(envelope(0), &ExecutionControl::default())
            .unwrap();

        let _guard = ScopedGuard::limited("raft.log.fsync.before", Action::Fail, 1);
        let log = RaftCommitLog::with_durability(group.clone(), DurabilityLevel::LeaderDisk);
        let err = log
            .propose(envelope(1), &deadline_control(2_000))
            .expect_err("a failed fsync must never produce a LeaderDisk receipt");
        assert!(
            matches!(
                err,
                LogError::Closed | LogError::DeadlineExceeded | LogError::Internal(_)
            ),
            "expected the propose to fail without a receipt, got {err:?}"
        );
        assert!(mongreldb_fault::hits("raft.log.fsync.before") >= 1);
        // openraft treats storage errors as fatal; shutdown is best-effort.
        let _ = group.shutdown().await;
    }
}
