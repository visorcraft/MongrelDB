//! Commit-timestamp monotonicity across leader failover, restart, and idle
//! periods (spec section 8.2; review findings M1 and m9).
//!
//! A new leader must never stamp a `commit_ts` at or below an
//! already-committed value, even when its wall clock trails the previous
//! leader's: the state machine observes every applied commit timestamp into
//! the shared group HLC clock, and the leader stamps
//! `next_after(max(local now, commit floor))` where the floor is the
//! persisted apply checkpoint plus the unapplied local log tail. The tests
//! below inject regressed wall clocks so the pre-fix behavior would produce
//! visibly smaller timestamps.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mongreldb_consensus::group::{ConsensusGroup, GroupConfig};
use mongreldb_consensus::identity::CommandKind;
use mongreldb_consensus::network::InMemoryTransport;
use mongreldb_consensus::read::{ReadConsistency, ReadConsistencyError};
use mongreldb_consensus::state_machine::{ApplySink, InMemoryApplySink};
use mongreldb_log::commit_log::ExecutionControl;
use mongreldb_log::envelope::CommandEnvelope;
use mongreldb_types::hlc::{HlcTimestamp, WallClockSource};
use openraft::BasicNode;
use tempfile::TempDir;

const FAST: Duration = Duration::from_millis(5);
const LEADER_TIMEOUT: Duration = Duration::from_secs(10);
/// How far behind the system clock the injected regressed sources sit.
const CLOCK_REGRESSION: Duration = Duration::from_secs(3_600);
/// Skew bound wide enough to tolerate the injected regression (the fail-
/// closed skew path is `HlcClock`'s own tested behavior, not under test here).
const SKEW_BOUND: Duration = Duration::from_secs(7_200);

fn envelope(seq: u64) -> CommandEnvelope {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&seq.to_le_bytes());
    CommandEnvelope::new(1, id, format!("cmd-{seq}").into_bytes())
}

fn system_micros() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_micros(),
    )
    .unwrap_or(u64::MAX)
}

/// A wall-clock source lagging the system clock by a fixed offset: the
/// pre-fix regression scenario (a new leader whose physical time trails the
/// previous leader's stamps).
fn lagging_clock(lag: Duration) -> WallClockSource {
    let lag_micros = u64::try_from(lag.as_micros()).unwrap_or(u64::MAX);
    Arc::new(move || system_micros().saturating_sub(lag_micros))
}

fn group_config(
    node: u64,
    dir: &std::path::Path,
    cluster: &str,
    time_source: Option<WallClockSource>,
) -> GroupConfig {
    let mut config = GroupConfig::new(cluster, node, dir.to_path_buf());
    config.heartbeat_interval = Duration::from_millis(50);
    config.election_timeout_min = Duration::from_millis(150);
    config.election_timeout_max = Duration::from_millis(300);
    config.install_snapshot_timeout = Duration::from_millis(1_000);
    config.hlc_max_skew = SKEW_BOUND;
    config.hlc_time_source = time_source;
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

    async fn start_node(&mut self, id: u64, time_source: Option<WallClockSource>) {
        let dir = self.tmp.path().join(format!("node-{id}"));
        let sink = Arc::new(Mutex::new(InMemoryApplySink::new()));
        let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink.clone();
        let group = ConsensusGroup::create(
            group_config(id, &dir, "test-cluster", time_source),
            self.transport.clone(),
            dyn_sink,
        )
        .await
        .unwrap();
        self.groups.insert(id, Arc::new(group));
        self.sinks.insert(id, sink);
    }

    async fn bootstrapped(ids: &[u64], lagging: &[u64]) -> Self {
        let mut cluster = TestCluster::new();
        for &id in ids {
            let time_source = lagging
                .contains(&id)
                .then(|| lagging_clock(CLOCK_REGRESSION));
            cluster.start_node(id, time_source).await;
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

    /// Waits until every listed node agrees on one leader; returns its id.
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

    /// Waits until the listed nodes agree on a leader other than `not`.
    async fn wait_new_leader(&self, among: &[u64], not: u64) -> u64 {
        let deadline = Instant::now() + LEADER_TIMEOUT;
        loop {
            let leader = self.wait_consensus_leader(among).await;
            if leader != not {
                return leader;
            }
            assert!(
                Instant::now() < deadline,
                "no new leader among {among:?} (still {not})"
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

/// Review M1: after a failover, the new leader's commit timestamps must
/// exceed the previous leader's last committed timestamp even though the new
/// leader's wall clock trails by an hour — and a Snapshot read at the old
/// committed timestamp completes on the new leader instead of hanging on a
/// regressed watermark.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn commit_ts_stays_monotonic_across_leader_failover() {
    // Node 1 reads the system clock; nodes 2 and 3 lag it by an hour, so a
    // pre-fix leader on 2/3 would stamp visibly *below* node 1's stamps.
    let cluster = TestCluster::bootstrapped(&[1, 2, 3], &[2, 3]).await;
    let elected = cluster.wait_consensus_leader(&[1, 2, 3]).await;
    if elected != 1 {
        cluster
            .group(elected)
            .transfer_leader(1, LEADER_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(cluster.wait_consensus_leader(&[1, 2, 3]).await, 1);
    }

    let mut last_ts = HlcTimestamp::ZERO;
    let mut last_index = 0;
    for seq in 1..=3u64 {
        let receipt = cluster
            .group(1)
            .propose(
                CommandKind::Transaction,
                envelope(seq),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        assert!(
            receipt.commit_ts > last_ts,
            "same-leader stamps must be strictly monotonic"
        );
        last_ts = receipt.commit_ts;
        last_index = receipt.position.index;
    }

    // Kill the leader; the lagging-clock survivors elect a new one.
    cluster.group(1).shutdown().await.unwrap();
    let survivors = [2, 3];
    let new_leader = cluster.wait_new_leader(&survivors, 1).await;

    // The new leader applied the old leader's committed prefix, and a
    // Snapshot read at the old committed timestamp completes (no hang on a
    // regressed watermark) both before and after its first own commit.
    for &id in &survivors {
        cluster
            .group(id)
            .wait_applied_index(last_index, LEADER_TIMEOUT)
            .await
            .unwrap();
        let watermark = cluster
            .group(id)
            .consistent_read(
                &ReadConsistency::Snapshot { timestamp: last_ts },
                &deadline_control(10_000),
            )
            .await
            .unwrap();
        assert!(
            watermark
                .commit_ts
                .is_some_and(|applied| applied >= last_ts),
            "applied watermark {:?} regressed below {last_ts:?}",
            watermark.commit_ts
        );
    }

    let receipt = cluster
        .group(new_leader)
        .propose(
            CommandKind::Transaction,
            envelope(4),
            &deadline_control(10_000),
        )
        .await
        .unwrap();
    assert!(
        receipt.commit_ts > last_ts,
        "new leader commit_ts {:?} must exceed the previous leader's last \
         committed {last_ts:?} despite the regressed wall clock",
        receipt.commit_ts
    );

    // The barrier stays satisfiable once the new commit lands.
    cluster
        .group(new_leader)
        .wait_applied_index(receipt.position.index, LEADER_TIMEOUT)
        .await
        .unwrap();
    let watermark = cluster
        .group(new_leader)
        .consistent_read(
            &ReadConsistency::Snapshot { timestamp: last_ts },
            &deadline_control(10_000),
        )
        .await
        .unwrap();
    assert!(watermark
        .commit_ts
        .is_some_and(|applied| applied >= last_ts));

    cluster.shutdown().await;
}

/// Review M1, restart leg: the commit floor is the persisted apply
/// checkpoint, so a node restarted with a fresh HLC clock and a wall clock
/// an hour behind still stamps above everything it committed before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_ts_floor_survives_restart() {
    let mut cluster = TestCluster::new();
    cluster.start_node(1, None).await;
    cluster
        .group(1)
        .bootstrap(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    cluster.group(1).wait_leader(LEADER_TIMEOUT).await.unwrap();

    let mut last_ts = HlcTimestamp::ZERO;
    for seq in 1..=2u64 {
        last_ts = cluster
            .group(1)
            .propose(
                CommandKind::Transaction,
                envelope(seq),
                &ExecutionControl::default(),
            )
            .await
            .unwrap()
            .commit_ts;
    }

    // Process-free crash; everything fsynced (including the applied-record
    // checkpoint carrying the commit floor) survives.
    let group = cluster.group(1);
    let transport = cluster.transport.clone();
    let dir = cluster.tmp.path().join("node-1");
    drop(cluster.groups.remove(&1));
    let crashed = Arc::into_inner(group).expect("sole group owner");
    crashed.crash().await;

    // Reopen on the same directory with a fresh clock whose wall time lags
    // the pre-crash stamps by an hour.
    let sink = Arc::new(Mutex::new(InMemoryApplySink::new()));
    let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink;
    let reopened = ConsensusGroup::create(
        group_config(
            1,
            &dir,
            "test-cluster",
            Some(lagging_clock(CLOCK_REGRESSION)),
        ),
        transport,
        dyn_sink,
    )
    .await
    .unwrap();
    reopened.wait_leader(LEADER_TIMEOUT).await.unwrap();

    let receipt = reopened
        .propose(
            CommandKind::Transaction,
            envelope(3),
            &deadline_control(10_000),
        )
        .await
        .unwrap();
    assert!(
        receipt.commit_ts > last_ts,
        "restarted node's commit_ts {:?} must exceed the pre-crash \
         {last_ts:?} (persisted floor) despite the regressed wall clock",
        receipt.commit_ts
    );
    reopened.shutdown().await.unwrap();
}

/// The Snapshot barrier's timeout mapping: a timestamp no committed entry
/// will ever cover waits until the caller's deadline and surfaces
/// `ReadConsistencyError::DeadlineExceeded` (never an unbounded hang when a
/// deadline is set).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_read_at_uncovered_ts_maps_to_deadline() {
    let cluster = TestCluster::bootstrapped(&[1], &[]).await;
    cluster.group(1).wait_leader(LEADER_TIMEOUT).await.unwrap();
    cluster
        .group(1)
        .propose(
            CommandKind::Transaction,
            envelope(1),
            &ExecutionControl::default(),
        )
        .await
        .unwrap();

    let unreachable = HlcTimestamp {
        physical_micros: u64::MAX / 2,
        logical: 0,
        node_tiebreaker: 0,
    };
    let result = cluster
        .group(1)
        .consistent_read(
            &ReadConsistency::Snapshot {
                timestamp: unreachable,
            },
            &deadline_control(300),
        )
        .await;
    assert!(
        matches!(result, Err(ReadConsistencyError::DeadlineExceeded)),
        "uncoverable snapshot read must map to DeadlineExceeded, got {result:?}"
    );
    cluster.shutdown().await;
}

/// Review m9: bounded-staleness reads measure *known missing data*, not
/// time since the last write. An idle but caught-up cluster is fresh, and
/// a cluster with no writes at all is fresh too.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn bounded_staleness_idle_cluster_is_fresh() {
    let cluster = TestCluster::bootstrapped(&[1, 2, 3], &[]).await;
    let leader = cluster.wait_consensus_leader(&[1, 2, 3]).await;
    let receipt = cluster
        .group(leader)
        .propose(
            CommandKind::Transaction,
            envelope(1),
            &ExecutionControl::default(),
        )
        .await
        .unwrap();
    for &id in &[1, 2, 3] {
        cluster
            .group(id)
            .wait_applied_index(receipt.position.index, LEADER_TIMEOUT)
            .await
            .unwrap();
    }

    // Let the cluster go idle far beyond the staleness bound below.
    tokio::time::sleep(Duration::from_millis(250)).await;

    for &id in &[1, 2, 3] {
        let watermark = cluster
            .group(id)
            .consistent_read(
                &ReadConsistency::BoundedStaleness { max_lag_ms: 1 },
                &deadline_control(5_000),
            )
            .await
            .unwrap_or_else(|e| panic!("caught-up replica {id} must be fresh: {e}"));
        assert_eq!(watermark.position.index, receipt.position.index);
    }
    cluster.shutdown().await;

    // No writes at all: nothing is missing, so the read is fresh. (A fresh
    // leader appends and applies a blank entry first; wait for that.)
    let quiet = TestCluster::bootstrapped(&[1], &[]).await;
    quiet.group(1).wait_leader(LEADER_TIMEOUT).await.unwrap();
    let last = quiet.group(1).metrics().last_log_index.unwrap_or(0);
    quiet
        .group(1)
        .wait_applied_index(last, LEADER_TIMEOUT)
        .await
        .unwrap();
    let watermark = quiet
        .group(1)
        .consistent_read(
            &ReadConsistency::BoundedStaleness { max_lag_ms: 1 },
            &deadline_control(5_000),
        )
        .await
        .expect("a write-less cluster is fresh");
    assert_eq!(watermark.commit_ts, None);
    quiet.shutdown().await;
}
