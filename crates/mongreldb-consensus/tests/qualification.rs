//! Stage 2 gate qualification (spec section 11 gate, 11.6 failover target):
//! measured failover timing over repeated leader kills, quorum durability
//! across one-node loss, snapshot catch-up after log truncation, and the
//! read-consistency surface (spec 11.4): the linearizable barrier
//! (`read_index` / `ReadConsistency::Linearizable`) and read-your-writes both
//! in its receipt-position primitive form (wait for a replica's applied
//! watermark to reach the receipt's log position) and through the landed
//! `SessionToken`-carried `ReadConsistency::ReadYourWrites` barrier.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mongreldb_consensus::error::ConsensusError;
use mongreldb_consensus::group::{ConsensusGroup, GroupConfig};
use mongreldb_consensus::identity::CommandKind;
use mongreldb_consensus::network::InMemoryTransport;
use mongreldb_consensus::raft_log::RaftCommitLog;
use mongreldb_consensus::read::ReadConsistency;
use mongreldb_consensus::state_machine::{ApplySink, InMemoryApplySink};
use mongreldb_log::commit_log::{CommitLog, DurabilityLevel, ExecutionControl, LogPosition};
use mongreldb_log::envelope::CommandEnvelope;
use openraft::BasicNode;
use tempfile::TempDir;

const CLUSTER: &str = "qualification";
const POLL: Duration = Duration::from_millis(5);
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(20);

fn envelope(seq: u64) -> CommandEnvelope {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&seq.to_le_bytes());
    CommandEnvelope::new(1, id, format!("cmd-{seq}").into_bytes())
}

fn deadline_control(ms: u64) -> ExecutionControl {
    ExecutionControl {
        deadline: Some(Instant::now() + Duration::from_millis(ms)),
        cancellation: None,
    }
}

fn group_config(node: u64, dir: &std::path::Path) -> GroupConfig {
    let mut config = GroupConfig::new(CLUSTER, node, dir.to_path_buf());
    config.heartbeat_interval = Duration::from_millis(50);
    config.election_timeout_min = Duration::from_millis(150);
    config.election_timeout_max = Duration::from_millis(300);
    config.install_snapshot_timeout = Duration::from_millis(1_000);
    config
}

struct QualCluster {
    tmp: TempDir,
    transport: Arc<InMemoryTransport>,
    groups: BTreeMap<u64, Arc<ConsensusGroup<InMemoryTransport>>>,
    sinks: BTreeMap<u64, Arc<Mutex<InMemoryApplySink>>>,
}

impl QualCluster {
    fn new() -> Self {
        QualCluster {
            tmp: tempfile::tempdir().unwrap(),
            transport: Arc::new(InMemoryTransport::new()),
            groups: BTreeMap::new(),
            sinks: BTreeMap::new(),
        }
    }

    async fn bootstrapped(ids: &[u64]) -> Self {
        let mut cluster = QualCluster::new();
        for &id in ids {
            cluster.start_node(id).await;
        }
        Self::bootstrap_first(&cluster, ids).await;
        cluster
    }

    /// Bootstraps the first node only; the rest adopt membership through
    /// replication (see tests/cluster.rs for the rationale).
    async fn bootstrap_first(cluster: &QualCluster, ids: &[u64]) {
        let members: BTreeMap<u64, BasicNode> = ids
            .iter()
            .map(|&id| (id, BasicNode::new(format!("node-{id}"))))
            .collect();
        cluster.groups[&ids[0]].bootstrap(members).await.unwrap();
    }

    async fn start_node(&mut self, id: u64) {
        self.start_node_with(id, |_| {}).await;
    }

    /// Starts (or restarts) `id` from its durable directory, keeping its
    /// applied sink so applied state survives a simulated process crash.
    async fn start_node_with(&mut self, id: u64, adjust: impl Fn(&mut GroupConfig)) {
        let dir = self.tmp.path().join(format!("node-{id}"));
        let mut config = group_config(id, &dir);
        adjust(&mut config);
        let sink = self
            .sinks
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(InMemoryApplySink::new())))
            .clone();
        let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink;
        let group = ConsensusGroup::create(config, self.transport.clone(), dyn_sink)
            .await
            .unwrap();
        self.groups.insert(id, Arc::new(group));
    }

    /// Process-free crash (no graceful storage close); restart with
    /// [`QualCluster::start_node`].
    async fn crash_node(&mut self, id: u64) {
        let group = self.groups.remove(&id).expect("crashed node exists");
        let crashed = Arc::into_inner(group).expect("no clones between steps");
        crashed.crash().await;
    }

    fn group(&self, id: u64) -> Arc<ConsensusGroup<InMemoryTransport>> {
        self.groups[&id].clone()
    }

    fn applied_envelopes(&self, id: u64) -> Vec<CommandEnvelope> {
        self.sinks[&id]
            .lock()
            .unwrap()
            .applied()
            .iter()
            .map(|applied| applied.envelope().expect("command").clone())
            .collect()
    }

    /// Waits until every node in `among` agrees on one leader; returns its id.
    async fn wait_consensus_leader(&self, among: &[u64]) -> u64 {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
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
                    .all(|leader| *leader == *leaders.values().next().unwrap())
            {
                return *leaders.values().next().unwrap();
            }
            assert!(
                Instant::now() < deadline,
                "no consensus leader among {among:?} (saw {leaders:?})"
            );
            tokio::time::sleep(POLL).await;
        }
    }

    async fn shutdown(self) {
        for group in self.groups.values() {
            let _ = group.shutdown().await;
        }
    }
}

// ---------------------------------------------------------------------------
// Failover timing (spec 11.6: leader failure detected and a new leader
// available within 10 seconds p95 on a one-AZ network)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failover_detection_and_availability_meets_p95_target() {
    const KILLS: usize = 20;
    let mut cluster = QualCluster::bootstrapped(&[1, 2, 3]).await;
    let mut samples = Vec::with_capacity(KILLS);

    for kill in 0..KILLS {
        let leader = cluster.wait_consensus_leader(&[1, 2, 3]).await;
        let survivors: Vec<u64> = [1, 2, 3].into_iter().filter(|id| *id != leader).collect();

        let start = Instant::now();
        cluster.crash_node(leader).await;

        // Detection: the survivors agree on a *new* leader (their metrics
        // still report the dead leader until the election timeout fires).
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        let new_leader = loop {
            let mut votes = Vec::new();
            for &id in &survivors {
                if let Some(current) = cluster.group(id).metrics().current_leader {
                    votes.push(current);
                }
            }
            if votes.len() == survivors.len()
                && votes.iter().all(|vote| *vote == votes[0])
                && votes[0] != leader
            {
                break votes[0];
            }
            assert!(
                Instant::now() < deadline,
                "kill {kill}: no new leader elected: {votes:?}"
            );
            tokio::time::sleep(POLL).await;
        };

        // Availability: the group commits a client write again.
        cluster
            .group(new_leader)
            .propose(
                CommandKind::Transaction,
                envelope(kill as u64 + 1),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        samples.push(start.elapsed());

        // Restore the third voter for the next kill.
        cluster.start_node(leader).await;
        let last = cluster.group(new_leader).metrics().last_log_index.unwrap();
        cluster
            .group(leader)
            .wait_applied_index(last, CONVERGE_TIMEOUT)
            .await
            .unwrap();
    }

    samples.sort();
    let p50 = samples[(50 * KILLS).div_ceil(100) - 1];
    let p95 = samples[(95 * KILLS).div_ceil(100) - 1];
    let millis: Vec<u128> = samples.iter().map(|d| d.as_millis()).collect();
    eprintln!("failover samples over {KILLS} leader kills (ms): {millis:?}");
    eprintln!("failover p50 {p50:?}, p95 {p95:?} (spec 11.6 target: p95 < 10 s)");
    // The test timing config (50 ms heartbeats, 150-300 ms election timeouts)
    // lands failovers well under a second; the assertion is the spec 11.6
    // regression tripwire, not a performance claim.
    assert!(
        p95 < Duration::from_secs(10),
        "failover p95 {p95:?} breached the spec 11.6 target"
    );
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// Quorum durability (spec 11 gate: quorum writes survive one-node loss)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quorum_acknowledged_writes_survive_one_node_loss() {
    let mut cluster = QualCluster::bootstrapped(&[1, 2, 3]).await;
    let leader = cluster.wait_consensus_leader(&[1, 2, 3]).await;
    let victim = [1, 2, 3].into_iter().find(|id| *id != leader).unwrap();

    // One follower is lost; the remaining two voters still hold a quorum.
    cluster.crash_node(victim).await;

    let log = RaftCommitLog::new(cluster.group(leader));
    for seq in 1..=8u64 {
        let receipt = log
            .propose(envelope(seq), &ExecutionControl::default())
            .unwrap();
        assert_eq!(
            receipt.durability,
            DurabilityLevel::Quorum,
            "replicated writes acknowledge at quorum durability (spec 11.3)"
        );
    }
    let last = cluster.group(leader).metrics().last_log_index.unwrap();

    // The lost node returns and catches up: no acknowledged write is missing.
    cluster.start_node(victim).await;
    cluster
        .group(victim)
        .wait_applied_index(last, CONVERGE_TIMEOUT)
        .await
        .unwrap();
    let committed: Vec<CommandEnvelope> = cluster
        .group(victim)
        .read_committed(LogPosition::ZERO, 100)
        .unwrap()
        .into_iter()
        .map(|entry| entry.envelope)
        .collect();
    for seq in 1..=8u64 {
        assert!(
            committed.contains(&envelope(seq)),
            "acknowledged write {seq} lost on the restarted node"
        );
    }
    assert_eq!(
        cluster.applied_envelopes(victim),
        cluster.applied_envelopes(leader)
    );
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// Snapshot catch-up (spec 11 gate: works after log truncation)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn snapshot_catch_up_after_log_truncation() {
    let mut cluster = QualCluster::new();
    // Aggressive snapshot policy so the lagging follower can only be brought
    // up to date by a snapshot, not by log replication.
    for id in [1, 2, 3] {
        cluster
            .start_node_with(id, |config| {
                config.snapshot_policy_logs = 5;
                config.max_in_snapshot_log_to_keep = 2;
                config.replication_lag_threshold = 8;
            })
            .await;
    }
    QualCluster::bootstrap_first(&cluster, &[1, 2, 3]).await;
    let leader = cluster.wait_consensus_leader(&[1, 2, 3]).await;
    let follower = [1, 2, 3].into_iter().find(|id| *id != leader).unwrap();
    let third = [1, 2, 3]
        .into_iter()
        .find(|id| *id != leader && *id != follower)
        .unwrap();

    // Lag the follower behind a partition, then push enough entries for the
    // leader to snapshot and purge past the follower's match index.
    cluster.transport.partition(&[follower], &[leader, third]);
    for seq in 1..=12u64 {
        cluster
            .group(leader)
            .propose(
                CommandKind::Transaction,
                envelope(seq),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
    }
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let metrics = cluster.group(leader).metrics();
        if metrics.snapshot.is_some_and(|pos| pos.index >= 5)
            && metrics.purged.is_some_and(|pos| pos.index >= 3)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "leader never snapshotted/purged: {metrics:?}"
        );
        tokio::time::sleep(POLL).await;
    }

    // Heal: log replication alone cannot close the gap (the prefix is
    // purged), so the follower must install a snapshot.
    cluster.transport.heal();
    let last = cluster.group(leader).metrics().last_log_index.unwrap();
    cluster
        .group(follower)
        .wait_applied_index(last, CONVERGE_TIMEOUT)
        .await
        .unwrap();

    // The follower installed a snapshot past the truncated prefix...
    let follower_snapshot = cluster.group(follower).metrics().snapshot;
    assert!(
        follower_snapshot.is_some_and(|pos| pos.index >= 5),
        "expected snapshot install on the lagging follower: {follower_snapshot:?}"
    );

    // ...and no committed entry is missing: applied sequences agree exactly.
    assert_eq!(
        cluster.applied_envelopes(follower),
        cluster.applied_envelopes(leader)
    );
    for seq in 1..=12u64 {
        assert!(
            cluster.applied_envelopes(follower).contains(&envelope(seq)),
            "committed write {seq} missing after snapshot catch-up"
        );
    }
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// Read consistency (spec 11.4, landed surface)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linearizable_read_index_smoke() {
    let cluster = QualCluster::bootstrapped(&[1, 2, 3]).await;
    let leader = cluster.wait_consensus_leader(&[1, 2, 3]).await;
    let mut last = LogPosition::ZERO;
    for seq in 1..=3u64 {
        last = cluster
            .group(leader)
            .propose(
                CommandKind::Transaction,
                envelope(seq),
                &ExecutionControl::default(),
            )
            .await
            .unwrap()
            .position;
    }

    // A confirmed leader's read barrier covers every acknowledged write.
    let barrier = cluster
        .group(leader)
        .read_index(&ExecutionControl::default())
        .await
        .unwrap();
    assert!(
        (barrier.term, barrier.index) >= (last.term, last.index),
        "read barrier {barrier:?} behind acknowledged receipt {last:?}"
    );

    // The same barrier through the ReadConsistency surface (Stage 2D).
    let watermark = cluster
        .group(leader)
        .consistent_read(&ReadConsistency::Linearizable, &deadline_control(10_000))
        .await
        .unwrap();
    assert!(
        (watermark.position.term, watermark.position.index) >= (last.term, last.index),
        "linearizable watermark {watermark:?} behind acknowledged receipt {last:?}"
    );

    // Followers never serve the linearizable barrier; the error carries the
    // leader hint for routing (spec 11.7).
    let follower = [1, 2, 3].into_iter().find(|id| *id != leader).unwrap();
    match cluster
        .group(follower)
        .read_index(&ExecutionControl::default())
        .await
    {
        Err(ConsensusError::NotLeader { leader: hint }) => {
            assert_eq!(hint, Some(leader), "stale leader hint");
        }
        other => panic!("follower must not serve a linearizable barrier: {other:?}"),
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_your_writes_via_applied_watermark() {
    // Spec 11.4 read-your-writes in both landed forms: the receipt's log
    // position is the session position a replica waits for (the primitive),
    // and the SessionToken built from the receipt drives
    // ReadConsistency::ReadYourWrites on any replica (the surface).
    let cluster = QualCluster::bootstrapped(&[1, 2, 3]).await;
    let leader = cluster.wait_consensus_leader(&[1, 2, 3]).await;
    let log = RaftCommitLog::new(cluster.group(leader));
    let receipt = log
        .propose(envelope(7), &ExecutionControl::default())
        .unwrap();
    let token = log.session_token(&receipt);
    assert_eq!(token.commit_index, receipt.log_position.index);

    for id in [1, 2, 3].into_iter().filter(|id| *id != leader) {
        // Primitive form: the replica's applied watermark reaches the
        // receipt's log position.
        cluster
            .group(id)
            .wait_applied_index(receipt.log_position.index, CONVERGE_TIMEOUT)
            .await
            .unwrap();
        // Token-carried barrier: authorizes serving the read at or below the
        // returned applied watermark.
        let watermark = cluster
            .group(id)
            .consistent_read(
                &ReadConsistency::ReadYourWrites {
                    token: token.clone(),
                },
                &deadline_control(10_000),
            )
            .await
            .unwrap();
        assert!(
            watermark.position.index >= receipt.log_position.index,
            "read-your-writes watermark {watermark:?} behind the receipt {receipt:?}"
        );
        let committed = cluster
            .group(id)
            .read_committed(LogPosition::ZERO, 100)
            .unwrap();
        assert!(
            committed.iter().any(
                |entry| entry.position == receipt.log_position && entry.envelope == envelope(7)
            ),
            "replica {id} cannot read its write at {receipt:?}"
        );
    }
    cluster.shutdown().await;
}
