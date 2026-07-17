//! Stage 2 gate chaos qualification (spec section 11 gate): a
//! randomized-but-seeded scenario matrix over three-node in-memory clusters.
//!
//! Rounds of leader crash, follower crash, minority partition, majority
//! partition, heal, membership add/promote/remove, and leadership transfer run
//! against a sequential client loop proposing monotonically numbered,
//! idempotent commands. The scenario order is a pure function of
//! `DEFAULT_SEED` (set the `MONGREL_CHAOS_SEED` environment variable to
//! reproduce a different run); there is no wall-clock randomness, and the seed
//! is printed at the start of the run and inside every failure message.
//!
//! After every round — on the healed, converged cluster — the gate invariants
//! are asserted:
//!
//! - at most one effective leader per term (metrics observations accumulated
//!   over the whole run);
//! - no committed entry ever lost or reordered on any node (`read_committed`
//!   streams agree exactly, first occurrences are monotone, and every
//!   acknowledged receipt is present at its recorded position);
//! - no split-brain commit: proposals attempted on the quorum-less side of a
//!   partition are rejected and never appear in any committed stream
//!   post-heal;
//! - state-machine applied sequences are identical on all live nodes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mongreldb_consensus::error::ConsensusError;
use mongreldb_consensus::group::{ConsensusGroup, GroupConfig, RaftServerState};
use mongreldb_consensus::identity::CommandKind;
use mongreldb_consensus::network::InMemoryTransport;
use mongreldb_consensus::state_machine::{AppliedCommand, ApplySink, InMemoryApplySink};
use mongreldb_log::commit_log::{ExecutionControl, LogPosition};
use mongreldb_log::envelope::CommandEnvelope;
use openraft::BasicNode;
use tempfile::TempDir;

const CLUSTER: &str = "chaos-gate";
/// Default scenario seed; deterministic across runs. Override with the
/// `MONGREL_CHAOS_SEED` environment variable to explore other orders.
const DEFAULT_SEED: u64 = 0x5EED_0002;
const ROUNDS: usize = 21;
const FAULT_STEPS: usize = 3;
const PROGRESS_STEPS: usize = 3;
const STEP_DEADLINE_MS: u64 = 300;
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(5);
/// Sequence base for taint proposals (kept disjoint from client sequences).
const TAINT_BASE: u64 = 1 << 40;
const READ_LIMIT: usize = 100_000;

// ---------------------------------------------------------------------------
// Deterministic scenario generation (no wall-clock randomness)
// ---------------------------------------------------------------------------

/// SplitMix64: a small deterministic PRNG so the matrix needs no external
/// crates and no clock input.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// One chaos round's fault action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Scenario {
    LeaderCrash,
    FollowerCrash,
    MinorityPartition,
    MajorityPartition,
    Heal,
    MembershipChurn,
    TransferLeadership,
}

const SCENARIO_KINDS: [Scenario; 7] = [
    Scenario::LeaderCrash,
    Scenario::FollowerCrash,
    Scenario::MinorityPartition,
    Scenario::MajorityPartition,
    Scenario::Heal,
    Scenario::MembershipChurn,
    Scenario::TransferLeadership,
];

/// The deterministic scenario order for `seed`: shuffled blocks of the full
/// kind set (Fisher-Yates driven by the seed), so every block of seven rounds
/// covers every scenario kind exactly once for any seed.
fn scenario_sequence(seed: u64, rounds: usize) -> Vec<Scenario> {
    let mut rng = SplitMix64(seed);
    let mut scenarios = Vec::with_capacity(rounds);
    while scenarios.len() < rounds {
        let mut block = SCENARIO_KINDS;
        for i in (1..block.len()).rev() {
            let j = rng.below(i as u64 + 1) as usize;
            block.swap(i, j);
        }
        scenarios.extend_from_slice(&block);
    }
    scenarios.truncate(rounds);
    scenarios
}

/// Per-round PRNG for deterministic target selection within a round.
fn round_rng(seed: u64, round: usize) -> SplitMix64 {
    SplitMix64(seed ^ (round as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

// ---------------------------------------------------------------------------
// Harness (mirrors tests/cluster.rs idioms)
// ---------------------------------------------------------------------------

fn group_config(node: u64, dir: &std::path::Path) -> GroupConfig {
    let mut config = GroupConfig::new(CLUSTER, node, dir.to_path_buf());
    config.heartbeat_interval = Duration::from_millis(50);
    config.election_timeout_min = Duration::from_millis(150);
    config.election_timeout_max = Duration::from_millis(300);
    config.install_snapshot_timeout = Duration::from_millis(1_000);
    config
}

fn envelope(seq: u64) -> CommandEnvelope {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&seq.to_le_bytes());
    CommandEnvelope::new(1, id, format!("cmd-{seq}").into_bytes())
}

/// A proposal that must never commit: attempted only on quorum-less nodes.
fn taint_envelope(n: u64) -> CommandEnvelope {
    let seq = TAINT_BASE + n;
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&seq.to_le_bytes());
    CommandEnvelope::new(1, id, format!("taint-{n}").into_bytes())
}

/// The client/taint sequence carried by an envelope.
fn seq_of(envelope: &CommandEnvelope) -> u64 {
    u64::from_le_bytes(envelope.command_id[..8].try_into().expect("16-byte id"))
}

fn deadline_control(ms: u64) -> ExecutionControl {
    ExecutionControl {
        deadline: Some(Instant::now() + Duration::from_millis(ms)),
        cancellation: None,
    }
}

struct ChaosCluster {
    tmp: TempDir,
    transport: Arc<InMemoryTransport>,
    groups: BTreeMap<u64, Arc<ConsensusGroup<InMemoryTransport>>>,
    sinks: BTreeMap<u64, Arc<Mutex<InMemoryApplySink>>>,
    /// Voters expected to replicate (updated by membership actions).
    members: BTreeSet<u64>,
    /// Members currently crashed (restarted in the recovery phase).
    down: BTreeSet<u64>,
    /// The client loop's current leader hint.
    leader_hint: Option<u64>,
}

impl ChaosCluster {
    async fn bootstrapped(ids: &[u64]) -> Self {
        let mut cluster = ChaosCluster {
            tmp: tempfile::tempdir().unwrap(),
            transport: Arc::new(InMemoryTransport::new()),
            groups: BTreeMap::new(),
            sinks: BTreeMap::new(),
            members: ids.iter().copied().collect(),
            down: BTreeSet::new(),
            leader_hint: None,
        };
        for &id in ids {
            cluster.start_node(id).await;
        }
        let members: BTreeMap<u64, BasicNode> = ids
            .iter()
            .map(|&id| (id, BasicNode::new(format!("node-{id}"))))
            .collect();
        // Bootstrap the first node only; the rest adopt membership through
        // replication (see tests/cluster.rs for the rationale).
        cluster.groups[&ids[0]].bootstrap(members).await.unwrap();
        cluster
    }

    /// Starts (or restarts) `id` from its durable directory, keeping its
    /// applied sink so applied state survives a simulated process crash.
    async fn start_node(&mut self, id: u64) {
        let dir = self.tmp.path().join(format!("node-{id}"));
        let sink = self
            .sinks
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(InMemoryApplySink::new())))
            .clone();
        let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink;
        let group =
            ConsensusGroup::create(group_config(id, &dir), self.transport.clone(), dyn_sink)
                .await
                .unwrap();
        self.groups.insert(id, Arc::new(group));
        self.down.remove(&id);
    }

    /// Process-free crash (no graceful storage close); the node stays down
    /// until the recovery phase restarts it with [`ChaosCluster::start_node`].
    async fn crash_node(&mut self, id: u64) {
        if self.leader_hint == Some(id) {
            self.leader_hint = None;
        }
        let group = self.groups.remove(&id).expect("crashed node exists");
        let crashed = Arc::into_inner(group).expect("client clones are dropped between steps");
        crashed.crash().await;
        self.down.insert(id);
    }

    /// Members currently expected to make progress (up and not removed).
    fn live_members(&self) -> Vec<u64> {
        self.members.difference(&self.down).copied().collect()
    }

    fn group(&self, id: u64) -> Arc<ConsensusGroup<InMemoryTransport>> {
        self.groups[&id].clone()
    }

    fn applied(&self, id: u64) -> Vec<AppliedCommand> {
        self.sinks[&id].lock().unwrap().applied()
    }

    /// The full committed command stream (position + envelope) in log order.
    fn committed(&self, id: u64) -> Vec<(LogPosition, CommandEnvelope)> {
        self.group(id)
            .read_committed(LogPosition::ZERO, READ_LIMIT)
            .unwrap()
            .into_iter()
            .map(|entry| (entry.position, entry.envelope))
            .collect()
    }

    /// Waits until every node in `among` agrees on one leader; returns its id.
    async fn wait_consensus_leader(&self, among: &[u64], seed: u64) -> u64 {
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
                "no consensus leader among {among:?} (saw {leaders:?}, chaos seed {seed})"
            );
            tokio::time::sleep(POLL).await;
        }
    }

    /// Waits until every live member has applied the leader's last log index.
    async fn wait_all_applied(&self, leader: u64, seed: u64) {
        let target = self
            .group(leader)
            .metrics()
            .last_log_index
            .expect("leader has a log");
        for id in self.live_members() {
            self.group(id)
                .wait_applied_index(target, CONVERGE_TIMEOUT)
                .await
                .unwrap_or_else(|err| {
                    panic!("node {id} never applied index {target}: {err} (chaos seed {seed})")
                });
        }
    }

    /// Waits until `observer` sees exactly `expected` as the voter set.
    async fn wait_voters(&self, observer: u64, expected: &BTreeSet<u64>, seed: u64) {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            let (voters, _) = self.group(observer).members();
            if voters.iter().copied().collect::<BTreeSet<_>>() == *expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "membership {voters:?} never converged to {expected:?} (chaos seed {seed})"
            );
            tokio::time::sleep(POLL).await;
        }
    }

    /// Records every observed (term, leader-state) pair; a term with two
    /// different effective leaders is a split-brain violation (spec 4.2).
    fn observe_term_leaders(&self, term_leaders: &mut BTreeMap<u64, u64>, seed: u64) {
        for id in self.live_members() {
            let metrics = self.group(id).metrics();
            if matches!(metrics.state, RaftServerState::Leader) {
                match term_leaders.get(&metrics.current_term) {
                    Some(&other) if other != id => panic!(
                        "split leadership: term {} led by {other} and {id} (chaos seed {seed})",
                        metrics.current_term
                    ),
                    Some(_) => {}
                    None => {
                        term_leaders.insert(metrics.current_term, id);
                    }
                }
            }
        }
    }

    /// One propose attempt for the client's pending command against the live
    /// members (leader hint first). With `expect_no_progress`, any
    /// acknowledgment is a commit without quorum — a split-brain failure.
    async fn client_step(&mut self, client: &mut Client, expect_no_progress: bool, seed: u64) {
        let pending = client
            .pending
            .get_or_insert_with(|| envelope(client.next_seq))
            .clone();
        let mut targets = self.live_members();
        if let Some(hint) = self.leader_hint {
            targets.retain(|&target| target != hint);
            targets.insert(0, hint);
        }
        for target in targets {
            let result = self
                .group(target)
                .propose(
                    CommandKind::Transaction,
                    pending.clone(),
                    &deadline_control(STEP_DEADLINE_MS),
                )
                .await;
            match result {
                Ok(receipt) => {
                    assert!(
                        !expect_no_progress,
                        "commit without a quorum on node {target}: {receipt:?} (chaos seed {seed})"
                    );
                    assert!(
                        receipt.position.index > client.last_receipt_index,
                        "acknowledged writes reordered: receipt {receipt:?} after index {} \
                         (chaos seed {seed})",
                        client.last_receipt_index
                    );
                    client.last_receipt_index = receipt.position.index;
                    client
                        .acknowledged
                        .insert(client.next_seq, receipt.position);
                    client.next_seq += 1;
                    client.pending = None;
                    self.leader_hint = Some(target);
                    return;
                }
                Err(ConsensusError::NotLeader { leader }) => {
                    self.leader_hint = leader
                        .filter(|hint| self.members.contains(hint) && !self.down.contains(hint));
                    // Try the hinted/next target within this step.
                }
                Err(_) => {
                    // Ambiguous (deadline, transport, shutdown): the entry may
                    // still commit; the identical envelope is retried later
                    // and idempotent apply keeps it single (S2B-004).
                    return;
                }
            }
        }
    }

    /// Attempts a proposal on a quorum-less node. It must be rejected, and
    /// the envelope must never appear in any committed stream post-heal.
    async fn taint_step(&mut self, client: &mut Client, node: u64, seed: u64) {
        let tainted = taint_envelope(client.taint_seq);
        client.taint_seq += 1;
        let result = self
            .group(node)
            .propose(
                CommandKind::Transaction,
                tainted.clone(),
                &deadline_control(STEP_DEADLINE_MS),
            )
            .await;
        assert!(
            result.is_err(),
            "split-brain commit on quorum-less node {node}: {result:?} (chaos seed {seed})"
        );
        client.tainted.push(tainted);
    }

    /// Transfers leadership to `target` with retries: transfer is best-effort
    /// orchestration (see the group module docs), so a timed-out attempt is
    /// retried against the current consensus leader. It must still complete —
    /// the spec section 11 gate requires transfer to preserve availability.
    async fn transfer_with_retry(&mut self, target: u64, seed: u64, round: usize) {
        for attempt in 1..=3u32 {
            let live = self.live_members();
            let leader = self.wait_consensus_leader(&live, seed).await;
            if leader == target {
                self.leader_hint = Some(target);
                return;
            }
            match self
                .group(leader)
                .transfer_leader(target, CONVERGE_TIMEOUT)
                .await
            {
                Ok(()) => {
                    let live = self.live_members();
                    assert_eq!(
                        self.wait_consensus_leader(&live, seed).await,
                        target,
                        "transfer target did not take over (chaos seed {seed}, round {round})"
                    );
                    self.leader_hint = Some(target);
                    return;
                }
                // Best-effort orchestration can lose the election race or see
                // leadership move mid-handoff; retry against the new leader.
                Err(ConsensusError::DeadlineExceeded | ConsensusError::NotLeader { .. }) => {
                    eprintln!(
                        "transfer attempt {attempt} to {target} did not complete; retrying \
                         (chaos seed {seed}, round {round})"
                    );
                }
                Err(err) => panic!(
                    "leadership transfer to {target} failed: {err} (chaos seed {seed}, round {round})"
                ),
            }
        }
        panic!(
            "leadership transfer to {target} did not complete after 3 attempts \
             (chaos seed {seed}, round {round})"
        );
    }

    async fn shutdown(self) {
        for group in self.groups.values() {
            let _ = group.shutdown().await;
        }
    }
}

/// Sequential client proposing monotonically numbered commands. Ambiguous
/// failures are retried with the identical envelope (idempotent apply,
/// S2B-004), so an acknowledged sequence number is never renumbered.
struct Client {
    next_seq: u64,
    pending: Option<CommandEnvelope>,
    /// Acknowledged sequence -> committed receipt position.
    acknowledged: BTreeMap<u64, LogPosition>,
    last_receipt_index: u64,
    /// Quorum-less-side proposals that were rejected and must stay uncommitted.
    tainted: Vec<CommandEnvelope>,
    taint_seq: u64,
}

impl Client {
    fn new() -> Self {
        Client {
            next_seq: 1,
            pending: None,
            acknowledged: BTreeMap::new(),
            last_receipt_index: 0,
            tainted: Vec::new(),
            taint_seq: 0,
        }
    }
}

/// The gate invariants, asserted after every round on the healed, converged
/// cluster (spec section 11 gate).
fn check_invariants(cluster: &ChaosCluster, client: &Client, round: usize, seed: u64) {
    let live = cluster.live_members();
    let reference = cluster.committed(live[0]);

    // No committed entry lost or reordered: first occurrences of client
    // sequences are monotone (the client is strictly sequential, so the log
    // order of first occurrences must be the numbering order).
    let mut first_occurrence: Vec<u64> = Vec::new();
    let mut seen = BTreeSet::new();
    for (_, entry) in &reference {
        let seq = seq_of(entry);
        assert!(
            seq < TAINT_BASE,
            "minority-side proposal committed: {entry:?} (chaos seed {seed}, round {round})"
        );
        if seen.insert(seq) {
            first_occurrence.push(seq);
        }
    }
    assert!(
        first_occurrence.windows(2).all(|pair| pair[0] < pair[1]),
        "committed order not monotone: {first_occurrence:?} (chaos seed {seed}, round {round})"
    );

    // Every acknowledged write is present at its recorded receipt position.
    for (&seq, &position) in &client.acknowledged {
        match reference.iter().find(|(pos, _)| *pos == position) {
            Some((_, entry)) if seq_of(entry) == seq => {}
            other => panic!(
                "acknowledged write {seq} at {position:?} lost or displaced: {other:?} \
                 (chaos seed {seed}, round {round})"
            ),
        }
    }

    // No split-brain commit: tainted proposals never appear post-heal.
    for tainted in &client.tainted {
        assert!(
            !reference.iter().any(|(_, entry)| entry == tainted),
            "quorum-less proposal appeared post-heal: {tainted:?} (chaos seed {seed}, round {round})"
        );
    }

    // The committed streams agree exactly on every live node.
    for &id in &live[1..] {
        assert_eq!(
            cluster.committed(id),
            reference,
            "committed stream diverged on node {id} (chaos seed {seed}, round {round})"
        );
    }

    // State-machine applied sequences are identical on all live nodes.
    let applied = cluster.applied(live[0]);
    for &id in &live[1..] {
        assert_eq!(
            cluster.applied(id),
            applied,
            "applied sequence diverged on node {id} (chaos seed {seed}, round {round})"
        );
    }

    // Converged: exactly one effective leader, agreed on by every live node.
    let metrics: Vec<(u64, _)> = live
        .iter()
        .map(|&id| (id, cluster.group(id).metrics()))
        .collect();
    let leaders: Vec<u64> = metrics
        .iter()
        .filter(|(_, m)| matches!(m.state, RaftServerState::Leader))
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(
        leaders.len(),
        1,
        "expected exactly one effective leader among {live:?}: {leaders:?} \
         (chaos seed {seed}, round {round})"
    );
    assert!(
        metrics
            .iter()
            .all(|(_, m)| m.current_leader == Some(leaders[0])),
        "leader belief diverged among {live:?} (chaos seed {seed}, round {round})"
    );
}

// ---------------------------------------------------------------------------
// The matrix
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn stage2_gate_chaos_matrix() {
    let seed = std::env::var("MONGREL_CHAOS_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_SEED);
    eprintln!(
        "stage 2 gate chaos matrix seed: {seed} (set MONGREL_CHAOS_SEED={seed} to reproduce)"
    );
    let scenarios = scenario_sequence(seed, ROUNDS);
    eprintln!("scenario order: {scenarios:?}");

    let mut cluster = ChaosCluster::bootstrapped(&[1, 2, 3]).await;
    let mut client = Client::new();
    let mut term_leaders: BTreeMap<u64, u64> = BTreeMap::new();

    // Baseline convergence before the first round.
    let leader = cluster
        .wait_consensus_leader(&cluster.live_members(), seed)
        .await;
    cluster.leader_hint = Some(leader);

    for (round, scenario) in scenarios.iter().copied().enumerate() {
        // Fault action.
        match scenario {
            Scenario::LeaderCrash => {
                let live = cluster.live_members();
                let leader = cluster.wait_consensus_leader(&live, seed).await;
                cluster.crash_node(leader).await;
            }
            Scenario::FollowerCrash => {
                let live = cluster.live_members();
                let leader = cluster.wait_consensus_leader(&live, seed).await;
                let followers: Vec<u64> = live.into_iter().filter(|&id| id != leader).collect();
                let victim =
                    followers[round_rng(seed, round).below(followers.len() as u64) as usize];
                cluster.crash_node(victim).await;
            }
            Scenario::MinorityPartition => {
                let live = cluster.live_members();
                let leader = cluster.wait_consensus_leader(&live, seed).await;
                let mut rng = round_rng(seed, round);
                // Half the time isolate the leader, half a follower.
                let minority = if rng.below(2) == 0 {
                    leader
                } else {
                    let others: Vec<u64> =
                        live.iter().copied().filter(|&id| id != leader).collect();
                    others[rng.below(others.len() as u64) as usize]
                };
                let majority: Vec<u64> = live.into_iter().filter(|&id| id != minority).collect();
                cluster.transport.partition(&[minority], &majority);

                // The quorum-less side must reject proposals, and those
                // envelopes must never appear post-heal.
                cluster.taint_step(&mut client, minority, seed).await;
            }
            Scenario::MajorityPartition => {
                // Isolate every live voter from every other: no side holds a
                // quorum anywhere.
                let live = cluster.live_members();
                for (i, &a) in live.iter().enumerate() {
                    for &b in &live[i + 1..] {
                        cluster.transport.partition(&[a], &[b]);
                    }
                }
                for node in cluster.live_members() {
                    cluster.taint_step(&mut client, node, seed).await;
                }
            }
            Scenario::Heal => {}
            Scenario::MembershipChurn => {
                let live = cluster.live_members();
                let leader = cluster.wait_consensus_leader(&live, seed).await;
                if !cluster.members.contains(&4) && !cluster.groups.contains_key(&4) {
                    // First churn: add node 4 as a learner, catch it up,
                    // promote it to voter through joint consensus.
                    cluster.start_node(4).await;
                    cluster
                        .group(leader)
                        .add_learner(4, BasicNode::new("node-4"))
                        .await
                        .unwrap();
                    let last = cluster.group(leader).metrics().last_log_index.unwrap();
                    cluster
                        .group(4)
                        .wait_applied_index(last, CONVERGE_TIMEOUT)
                        .await
                        .unwrap();
                    cluster.group(leader).promote(4).await.unwrap();
                    cluster.members.insert(4);
                    let expected = cluster.members.clone();
                    cluster.wait_voters(leader, &expected, seed).await;
                } else if cluster.members.len() > 3 {
                    // Later churns: remove the lowest-id non-leader voter
                    // through joint consensus.
                    let victim = cluster
                        .members
                        .iter()
                        .copied()
                        .filter(|&id| id != leader)
                        .min()
                        .expect("a non-leader voter exists");
                    cluster.group(leader).remove(victim).await.unwrap();
                    cluster.members.remove(&victim);
                    let expected = cluster.members.clone();
                    cluster.wait_voters(leader, &expected, seed).await;
                }
            }
            Scenario::TransferLeadership => {
                let live = cluster.live_members();
                let leader = cluster.wait_consensus_leader(&live, seed).await;
                let target = cluster
                    .members
                    .iter()
                    .copied()
                    .filter(|&id| id != leader && !cluster.down.contains(&id))
                    .min();
                if let Some(target) = target {
                    cluster.transfer_with_retry(target, seed, round).await;
                }
            }
        }

        // Client steps during the fault window; under a majority partition no
        // progress whatsoever is legal.
        for _ in 0..FAULT_STEPS {
            let no_quorum_anywhere = matches!(scenario, Scenario::MajorityPartition);
            cluster
                .client_step(&mut client, no_quorum_anywhere, seed)
                .await;
            cluster.observe_term_leaders(&mut term_leaders, seed);
        }

        // Recovery: heal every link and restart crashed members.
        cluster.transport.heal();
        let down: Vec<u64> = cluster.down.iter().copied().collect();
        for id in down {
            cluster.start_node(id).await;
        }
        let live = cluster.live_members();
        let leader = cluster.wait_consensus_leader(&live, seed).await;
        cluster.leader_hint = Some(leader);

        // Progress steps prove availability after the fault.
        for _ in 0..PROGRESS_STEPS {
            cluster.client_step(&mut client, false, seed).await;
            cluster.observe_term_leaders(&mut term_leaders, seed);
        }

        // Full catch-up, then the gate invariants.
        cluster.wait_all_applied(leader, seed).await;
        check_invariants(&cluster, &client, round, seed);
        cluster.observe_term_leaders(&mut term_leaders, seed);
    }

    let live = cluster.live_members();
    let committed = cluster.committed(live[0]);
    eprintln!(
        "chaos matrix complete: {ROUNDS} rounds, {} acknowledged writes, {} committed entries, \
         {} quorum-less proposals rejected (seed {seed})",
        client.acknowledged.len(),
        committed.len(),
        client.tainted.len()
    );
    assert!(
        client.acknowledged.len() >= ROUNDS,
        "client made too little progress: {} acknowledged writes in {ROUNDS} rounds \
         (chaos seed {seed})",
        client.acknowledged.len()
    );
    cluster.shutdown().await;
}

#[test]
fn scenario_sequence_is_deterministic() {
    let first = scenario_sequence(DEFAULT_SEED, ROUNDS);
    assert_eq!(first, scenario_sequence(DEFAULT_SEED, ROUNDS));
    assert_ne!(first, scenario_sequence(DEFAULT_SEED + 1, ROUNDS));
    // Every block of seven rounds covers the full matrix for any seed.
    for block in first.chunks(SCENARIO_KINDS.len()) {
        let kinds: BTreeSet<_> = block.iter().collect();
        assert_eq!(
            kinds.len(),
            SCENARIO_KINDS.len(),
            "block misses kinds: {block:?}"
        );
    }
}
