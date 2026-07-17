//! Fault-injection coverage (FND-006) for the consensus adapter.
//!
//! The `mongreldb_fault` registry is process-global, so these tests live in
//! their own integration-test binary: no other test process can arm or clear
//! hooks underneath them. Both scenarios run in one test function so they
//! cannot interleave within this binary either.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mongreldb_consensus::group::{ConsensusGroup, GroupConfig};
use mongreldb_consensus::identity::CommandKind;
use mongreldb_consensus::network::InMemoryTransport;
use mongreldb_consensus::state_machine::{ApplySink, InMemoryApplySink};
use mongreldb_fault::{Action, ScopedGuard};
use mongreldb_log::commit_log::ExecutionControl;
use mongreldb_log::envelope::CommandEnvelope;
use openraft::BasicNode;

fn group_config(node: u64, dir: &std::path::Path) -> GroupConfig {
    let mut config = GroupConfig::new("fault-hooks", node, dir.to_path_buf());
    config.heartbeat_interval = Duration::from_millis(50);
    config.election_timeout_min = Duration::from_millis(150);
    config.election_timeout_max = Duration::from_millis(300);
    config
}

fn envelope(seq: u64) -> CommandEnvelope {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&seq.to_le_bytes());
    CommandEnvelope::new(1, id, format!("cmd-{seq}").into_bytes())
}

async fn start(
    tmp: &tempfile::TempDir,
    transport: Arc<InMemoryTransport>,
    id: u64,
) -> (
    Arc<ConsensusGroup<InMemoryTransport>>,
    Arc<Mutex<InMemoryApplySink>>,
) {
    let sink = Arc::new(Mutex::new(InMemoryApplySink::new()));
    let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink.clone();
    let group = ConsensusGroup::create(
        group_config(id, &tmp.path().join(format!("node-{id}"))),
        transport,
        dyn_sink,
    )
    .await
    .unwrap();
    (Arc::new(group), sink)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn injected_faults_surface_and_recover() {
    let tmp = tempfile::tempdir().unwrap();
    let transport = Arc::new(InMemoryTransport::new());

    // (a) A storage fault at the append boundary fails the proposal. openraft
    // treats storage errors as fatal to the raft core, so this node is done;
    // the point is that the injected failure surfaces, is counted, and the
    // group shuts down cleanly instead of corrupting state.
    let (group, _sink) = start(&tmp, transport.clone(), 1).await;
    group
        .bootstrap(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    group.wait_leader(Duration::from_secs(10)).await.unwrap();
    // Exercise the append path once so the leader's blank entry is durable
    // before the hook is armed (the firing below is then deterministically
    // the proposal's own append).
    group
        .propose(CommandKind::Transaction, envelope(0), &ExecutionControl::default())
        .await
        .unwrap();
    {
        let _guard = ScopedGuard::limited("raft.log.append.before", Action::Fail, 1);
        let result = group
            .propose(CommandKind::Transaction, envelope(1), &ExecutionControl::default())
            .await;
        assert!(result.is_err(), "injected storage fault must surface");
        assert!(mongreldb_fault::hits("raft.log.append.before") >= 1);
    }
    group.shutdown().await.unwrap();

    // (b) A network fault on the vote path is retryable: with a bounded
    // firing budget the election fails twice, then succeeds once the hook
    // passes through.
    let (n1, _s1) = start(&tmp, transport.clone(), 2).await;
    let (n2, _s2) = start(&tmp, transport.clone(), 3).await;
    let members = BTreeMap::from([
        (2, BasicNode::new("node-2")),
        (3, BasicNode::new("node-3")),
    ]);
    {
        let _guard = ScopedGuard::limited("raft.net.vote.before", Action::Fail, 2);
        n1.bootstrap(members.clone()).await.unwrap();
        n2.bootstrap(members).await.unwrap();
        let leader = n1.wait_leader(Duration::from_secs(10)).await.unwrap();
        assert!(leader == 2 || leader == 3);
        // Two firings consumed the budget; later evaluations pass through and
        // still count as hits, so assert the floor only.
        assert!(mongreldb_fault::hits("raft.net.vote.before") >= 2);
        n1.propose(CommandKind::Transaction, envelope(7), &ExecutionControl::default())
            .await
            .unwrap();
    }
    n1.shutdown().await.unwrap();
    n2.shutdown().await.unwrap();
}
