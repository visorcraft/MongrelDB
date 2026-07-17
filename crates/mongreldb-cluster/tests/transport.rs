//! Cluster transport integration tests (spec section 6.7, Stage 2C
//! transport; mTLS direction of spec section 14.3).
//!
//! Loopback coverage: plaintext two- and three-node raft clusters electing,
//! replicating, and surviving the crash/restart of one node; an mTLS
//! three-node cluster over the checked-in `tests/fixtures` certificates;
//! mTLS rejection cases (missing client certificate, wrong CA, unadmitted
//! and mismatched node identities); fail-closed framing (unknown message
//! type) and bounded-connection enforcement on the listener.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mongreldb_cluster::network::{
    node_cert_name, PeerEndpoint, TcpTransport, TransportConfig, TransportError, TransportSecurity,
    TransportServer, TlsConfig, RAFT_MSG_ERROR,
};
use mongreldb_consensus::group::{ConsensusGroup, GroupConfig};
use mongreldb_consensus::identity::{raft_node_id, CommandKind, RaftNodeId};
use mongreldb_consensus::state_machine::{ApplySink, InMemoryApplySink};
use mongreldb_log::commit_log::ExecutionControl;
use mongreldb_log::envelope::CommandEnvelope;
use mongreldb_protocol::envelope::{ProtocolEnvelope, CHECKSUM_LEN, HEADER_LEN};
use mongreldb_types::ids::NodeId;
use openraft::BasicNode;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const LEADER_TIMEOUT: Duration = Duration::from_secs(15);
const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

fn fixture(name: &str) -> String {
    std::fs::read_to_string(format!("{FIXTURES}/{name}")).expect("fixture is checked in")
}

/// The fixture identities: `NodeId` = one repeated byte (see
/// `tests/fixtures/README.md`).
fn fixture_node_id(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 16])
}

fn node1() -> NodeId {
    fixture_node_id(1)
}

fn node2() -> NodeId {
    fixture_node_id(2)
}

fn node3() -> NodeId {
    fixture_node_id(3)
}

fn envelope(seq: u64) -> CommandEnvelope {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&seq.to_le_bytes());
    CommandEnvelope::new(1, id, format!("cmd-{seq}").into_bytes())
}

fn test_transport_config() -> TransportConfig {
    TransportConfig {
        connect_timeout: Duration::from_millis(500),
        rpc_timeout: Duration::from_secs(2),
        snapshot_timeout: Duration::from_secs(5),
        connect_attempts: 5,
        reconnect_backoff: Duration::from_millis(10),
        max_frame_bytes: 16 * 1024 * 1024,
        max_connections: 64,
        handshake_timeout: Duration::from_secs(2),
        shutdown_grace: Duration::from_secs(2),
    }
}

fn group_config(raft_id: RaftNodeId, dir: &std::path::Path) -> GroupConfig {
    let mut config = GroupConfig::new("transport-test", raft_id, dir.to_path_buf());
    config.heartbeat_interval = Duration::from_millis(50);
    config.election_timeout_min = Duration::from_millis(150);
    config.election_timeout_max = Duration::from_millis(300);
    config.install_snapshot_timeout = Duration::from_millis(2_000);
    config
}

/// Loads the fixture TLS material for one node, admitting the given peers.
fn node_tls(cert_prefix: &str, allowed: &[NodeId]) -> TlsConfig {
    TlsConfig::from_pems(
        &fixture("ca.crt.pem"),
        &fixture(&format!("{cert_prefix}.crt.pem")),
        &fixture(&format!("{cert_prefix}.key.pem")),
        allowed,
    )
    .expect("fixture TLS config loads")
}

struct Node {
    transport: Arc<TcpTransport>,
    server: TransportServer,
    sink: Arc<Mutex<InMemoryApplySink>>,
    group: Option<Arc<ConsensusGroup<TcpTransport>>>,
}

struct Cluster {
    tmp: TempDir,
    mtls: bool,
    nodes: BTreeMap<RaftNodeId, Node>,
    /// Raft id -> durable identity (used for mTLS peer binding).
    identities: BTreeMap<RaftNodeId, NodeId>,
}

impl Cluster {
    fn new(mtls: bool) -> Self {
        Cluster {
            tmp: tempfile::tempdir().unwrap(),
            mtls,
            nodes: BTreeMap::new(),
            identities: BTreeMap::new(),
        }
    }

    async fn start_node(&mut self, node_id: NodeId) {
        let raft_id = raft_node_id(&node_id);
        let security = if self.mtls {
            TransportSecurity::Mtls(node_tls(
                &format!("node{}", node_id.as_bytes()[0]),
                &[node1(), node2(), node3()],
            ))
        } else {
            TransportSecurity::PlaintextForTesting
        };
        let transport = Arc::new(TcpTransport::new(test_transport_config(), security.clone()));
        let server = TransportServer::bind(
            "127.0.0.1:0",
            security,
            transport.registry(),
            test_transport_config(),
        )
        .await
        .unwrap();
        let sink = Arc::new(Mutex::new(InMemoryApplySink::new()));
        let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink.clone();
        let dir = self.tmp.path().join(format!("node-{raft_id}"));
        let group = ConsensusGroup::create(
            group_config(raft_id, &dir),
            transport.clone(),
            dyn_sink,
        )
        .await
        .unwrap();
        self.identities.insert(raft_id, node_id);
        self.nodes.insert(
            raft_id,
            Node {
                transport,
                server,
                sink,
                group: Some(Arc::new(group)),
            },
        );
    }

    /// Every transport learns every other node's endpoint from the membership
    /// directory (here: the test harness).
    fn wire_peers(&self) {
        for (raft_id, node) in &self.nodes {
            for (other_id, other) in &self.nodes {
                if raft_id == other_id {
                    continue;
                }
                let address = other.server.local_addr().to_string();
                let endpoint = if self.mtls {
                    PeerEndpoint::mtls(address, self.identities[other_id])
                } else {
                    PeerEndpoint::plaintext(address)
                };
                node.transport.upsert_peer(*other_id, endpoint);
            }
        }
    }

    async fn bootstrap(&self) {
        let members: BTreeMap<RaftNodeId, BasicNode> = self
            .nodes
            .iter()
            .map(|(raft_id, node)| (*raft_id, BasicNode::new(node.server.local_addr().to_string())))
            .collect();
        let first = self.nodes.keys().next().unwrap();
        self.group(first).bootstrap(members).await.unwrap();
    }

    fn group(&self, raft_id: &RaftNodeId) -> Arc<ConsensusGroup<TcpTransport>> {
        self.nodes[raft_id].group.as_ref().expect("node is up").clone()
    }

    /// The current leader as seen from `from`, waiting through elections.
    async fn leader(&self, from: &RaftNodeId) -> RaftNodeId {
        self.group(from).wait_leader(LEADER_TIMEOUT).await.unwrap()
    }

    async fn propose(&self, seq: u64) -> u64 {
        let any = self.nodes.keys().next().unwrap();
        let leader = self.leader(any).await;
        let receipt = self
            .group(&leader)
            .propose(
                CommandKind::Transaction,
                envelope(seq),
                &ExecutionControl::default(),
            )
            .await
            .unwrap();
        receipt.position.index
    }

    async fn wait_applied(&self, raft_id: &RaftNodeId, index: u64) {
        self.group(raft_id)
            .wait_applied_index(index, LEADER_TIMEOUT)
            .await
            .unwrap();
    }

    async fn wait_all_applied(&self, index: u64) {
        for raft_id in self.nodes.keys() {
            if self.nodes[raft_id].group.is_some() {
                self.wait_applied(raft_id, index).await;
            }
        }
    }

    /// Committed command streams (from the log, not the apply sink, so they
    /// survive restarts) must be identical on every caught-up node.
    fn assert_committed_streams_equal(&self) {
        let mut streams = BTreeMap::new();
        for (raft_id, node) in &self.nodes {
            let envelopes: Vec<CommandEnvelope> = node
                .group
                .as_ref()
                .expect("node is up")
                .read_committed(mongreldb_log::commit_log::LogPosition::ZERO, 1_000)
                .unwrap()
                .into_iter()
                .map(|entry| entry.envelope)
                .collect();
            streams.insert(*raft_id, envelopes);
        }
        let mut iter = streams.values();
        let first = iter.next().unwrap();
        for (raft_id, other) in streams.iter().skip(1) {
            assert_eq!(
                first,
                other,
                "committed streams diverged at node {raft_id}"
            );
        }
    }

    async fn shutdown(self) {
        for node in self.nodes.into_values() {
            if let Some(group) = node.group {
                let _ = group.shutdown().await;
            }
            node.server.shutdown().await;
        }
    }
}

// ---------------------------------------------------------------------------
// Plaintext loopback clusters (TransportSecurity::PlaintextForTesting)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plaintext_two_node_cluster_elects_replicates_and_transfers() {
    let mut cluster = Cluster::new(false);
    cluster.start_node(node1()).await;
    cluster.start_node(node2()).await;
    cluster.wire_peers();
    cluster.bootstrap().await;

    let one = raft_node_id(&node1());
    let two = raft_node_id(&node2());
    let first_leader = cluster.leader(&one).await;

    let mut last_index = 0;
    for seq in 1..=3 {
        last_index = cluster.propose(seq).await;
    }
    cluster.wait_all_applied(last_index).await;

    // Best-effort leadership transfer drives the election-trigger RPC over
    // the TCP transport.
    let other = if first_leader == one { two } else { one };
    cluster
        .group(&first_leader)
        .transfer_leader(other, LEADER_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(cluster.leader(&one).await, other);

    last_index = cluster.propose(4).await;
    cluster.wait_all_applied(last_index).await;
    cluster.assert_committed_streams_equal();
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plaintext_three_node_cluster_survives_one_restart() {
    let mut cluster = Cluster::new(false);
    cluster.start_node(node1()).await;
    cluster.start_node(node2()).await;
    cluster.start_node(node3()).await;
    cluster.wire_peers();
    cluster.bootstrap().await;

    let one = raft_node_id(&node1());
    let three = raft_node_id(&node3());

    let mut last_index = 0;
    for seq in 1..=3 {
        last_index = cluster.propose(seq).await;
    }
    cluster.wait_all_applied(last_index).await;

    // Crash node three: the raft task stops without the graceful storage
    // close; its server keeps running with an empty dispatch table.
    let crashed = cluster.nodes.get_mut(&three).unwrap().group.take().unwrap();
    Arc::try_unwrap(crashed)
        .expect("the crashed group has no other owners")
        .crash()
        .await;

    // The remaining quorum keeps electing and committing.
    for seq in 4..=5 {
        last_index = cluster.propose(seq).await;
    }
    cluster.wait_applied(&one, last_index).await;

    // Restart node three on the same directory; it rejoins and catches up.
    let sink = Arc::new(Mutex::new(InMemoryApplySink::new()));
    let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink.clone();
    let dir = cluster.tmp.path().join(format!("node-{three}"));
    let transport = cluster.nodes[&three].transport.clone();
    let restarted = ConsensusGroup::create(group_config(three, &dir), transport, dyn_sink)
        .await
        .unwrap();
    cluster.nodes.get_mut(&three).unwrap().sink = sink;
    cluster.nodes.get_mut(&three).unwrap().group = Some(Arc::new(restarted));

    cluster.wait_applied(&three, last_index).await;
    cluster.assert_committed_streams_equal();
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// mTLS loopback cluster (TransportSecurity::Mtls over the fixture material)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mtls_three_node_cluster_elects_and_replicates() {
    let mut cluster = Cluster::new(true);
    cluster.start_node(node1()).await;
    cluster.start_node(node2()).await;
    cluster.start_node(node3()).await;
    cluster.wire_peers();
    cluster.bootstrap().await;

    let one = raft_node_id(&node1());
    let mut last_index = 0;
    for seq in 1..=3 {
        last_index = cluster.propose(seq).await;
    }
    cluster.wait_all_applied(last_index).await;
    assert_ne!(cluster.leader(&one).await, 0);
    cluster.assert_committed_streams_equal();
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// mTLS rejection cases
// ---------------------------------------------------------------------------

/// A plaintext-free control: a fully authenticated client reaches dispatch
/// (the empty registry answers an error frame), proving the connection and
/// both identity checks succeeded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_authenticated_client_reaches_dispatch() {
    let server_security = TransportSecurity::Mtls(node_tls("node1", &[node2()]));
    let server = TransportServer::bind(
        "127.0.0.1:0",
        server_security,
        mongreldb_cluster::network::TransportRegistry::new(),
        test_transport_config(),
    )
    .await
    .unwrap();

    let client = TcpTransport::new(
        test_transport_config(),
        TransportSecurity::Mtls(node_tls("node2", &[node1()])),
    );
    let target = raft_node_id(&node1());
    client.upsert_peer(target, PeerEndpoint::mtls(server.local_addr().to_string(), node1()));
    let error = client.trigger_election(target).await.unwrap_err();
    assert!(
        matches!(
            error,
            mongreldb_consensus::network::TransportError::Fault(ref message)
                if message.contains("not attached")
        ),
        "an authenticated client must reach dispatch, got: {error}"
    );
    server.shutdown().await;
}

/// A TLS client presenting no certificate is rejected by the server's
/// mandatory client-certificate verification.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_rejects_client_without_certificate() {
    let server_security = TransportSecurity::Mtls(node_tls("node1", &[node2()]));
    let server = TransportServer::bind(
        "127.0.0.1:0",
        server_security,
        mongreldb_cluster::network::TransportRegistry::new(),
        test_transport_config(),
    )
    .await
    .unwrap();

    // A raw client without client authentication (the transport always
    // presents a certificate, so the negative case drives rustls directly).
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut io::BufReader::new(fixture("ca.crt.pem").as_bytes())) {
        roots.add(cert.unwrap()).unwrap();
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let server_name =
        rustls::pki_types::ServerName::try_from(node_cert_name(&node1())).unwrap();
    let tcp = TcpStream::connect(server.local_addr()).await.unwrap();
    let handshake = tokio::time::timeout(
        Duration::from_secs(2),
        connector.connect(server_name, tcp),
    )
    .await;
    match handshake {
        // TLS 1.3 lets the client finish its flight before the server's
        // certificate_required alert arrives; the connection must still be
        // dead at first use.
        Ok(Ok(mut stream)) => {
            let mut buf = [0u8; 16];
            let read = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await;
            match read {
                Ok(Ok(0)) | Ok(Err(_)) | Err(_) => {}
                Ok(Ok(n)) => panic!("an unauthenticated client read {n} bytes from the server"),
            }
        }
        Ok(Err(_)) | Err(_) => {}
    }
    server.shutdown().await;
}

/// A certificate chaining to a foreign CA fails the server's client-chain
/// verification even though its identity has the right form.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_rejects_certificate_from_wrong_ca() {
    let server_security = TransportSecurity::Mtls(node_tls("node1", &[fixture_node_id(9)]));
    let server = TransportServer::bind(
        "127.0.0.1:0",
        server_security,
        mongreldb_cluster::network::TransportRegistry::new(),
        test_transport_config(),
    )
    .await
    .unwrap();

    // The rogue material is valid X.509 signed by rogue-ca; the client trusts
    // the real cluster CA so the failure is the server rejecting the client.
    let rogue_tls = TlsConfig::from_pems(
        &fixture("ca.crt.pem"),
        &fixture("rogue-node.crt.pem"),
        &fixture("rogue-node.key.pem"),
        &[node1()],
    )
    .unwrap();
    let client = TcpTransport::new(test_transport_config(), TransportSecurity::Mtls(rogue_tls));
    let target = raft_node_id(&node1());
    client.upsert_peer(target, PeerEndpoint::mtls(server.local_addr().to_string(), node1()));
    let error = client.trigger_election(target).await.unwrap_err();
    assert!(
        matches!(
            error,
            mongreldb_consensus::network::TransportError::Fault(ref message)
                if !message.contains("not attached")
        ),
        "a wrong-CA client must never reach dispatch, got: {error}"
    );
    server.shutdown().await;
}

/// A certificate chaining to the cluster CA but naming a node outside the
/// admitted set fails the server's identity binding check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_rejects_unadmitted_node_identity() {
    // The server admits only node two; node one's certificate is valid but
    // not admitted.
    let server_security = TransportSecurity::Mtls(node_tls("node3", &[node2()]));
    let server = TransportServer::bind(
        "127.0.0.1:0",
        server_security,
        mongreldb_cluster::network::TransportRegistry::new(),
        test_transport_config(),
    )
    .await
    .unwrap();

    let client = TcpTransport::new(
        test_transport_config(),
        TransportSecurity::Mtls(node_tls("node1", &[node3()])),
    );
    let target = raft_node_id(&node3());
    client.upsert_peer(target, PeerEndpoint::mtls(server.local_addr().to_string(), node3()));
    let error = client.trigger_election(target).await.unwrap_err();
    assert!(
        matches!(
            error,
            mongreldb_consensus::network::TransportError::Fault(ref message)
                if !message.contains("not attached")
        ),
        "an unadmitted node must never reach dispatch, got: {error}"
    );
    server.shutdown().await;
}

/// The client fails closed when the connected server's certificate names a
/// different node than the peer directory expects.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_client_rejects_mismatched_server_identity() {
    let server_security = TransportSecurity::Mtls(node_tls("node1", &[node2()]));
    let server = TransportServer::bind(
        "127.0.0.1:0",
        server_security,
        mongreldb_cluster::network::TransportRegistry::new(),
        test_transport_config(),
    )
    .await
    .unwrap();

    let client = TcpTransport::new(
        test_transport_config(),
        TransportSecurity::Mtls(node_tls("node2", &[node1(), node3()])),
    );
    // The directory claims this address is node three; it is node one.
    let target = raft_node_id(&node3());
    client.upsert_peer(target, PeerEndpoint::mtls(server.local_addr().to_string(), node3()));
    let error = client.trigger_election(target).await.unwrap_err();
    assert!(
        matches!(
            error,
            mongreldb_consensus::network::TransportError::Fault(ref message)
                if !message.contains("not attached")
        ),
        "a mismatched server identity must fail closed, got: {error}"
    );
    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Fail-closed framing and listener bounds (plaintext servers)
// ---------------------------------------------------------------------------

/// Reads one raw envelope frame; `Ok(None)` on a clean EOF.
async fn read_raw_frame(stream: &mut TcpStream) -> io::Result<Option<ProtocolEnvelope>> {
    let mut header = [0u8; HEADER_LEN];
    let first = stream.read(&mut header[..1]).await?;
    if first == 0 {
        return Ok(None);
    }
    stream.read_exact(&mut header[1..]).await?;
    let payload_len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
    let mut frame = Vec::with_capacity(HEADER_LEN + payload_len + CHECKSUM_LEN);
    frame.extend_from_slice(&header);
    frame.resize(HEADER_LEN + payload_len + CHECKSUM_LEN, 0);
    stream.read_exact(&mut frame[HEADER_LEN..]).await?;
    Ok(Some(ProtocolEnvelope::decode(&frame).expect("well-formed frame")))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_message_type_fails_closed() {
    let server = TransportServer::bind(
        "127.0.0.1:0",
        TransportSecurity::PlaintextForTesting,
        mongreldb_cluster::network::TransportRegistry::new(),
        test_transport_config(),
    )
    .await
    .unwrap();

    let mut stream = TcpStream::connect(server.local_addr()).await.unwrap();
    let frame = ProtocolEnvelope::new(999, b"bogus".to_vec());
    stream.write_all(&frame.encode()).await.unwrap();
    let response = read_raw_frame(&mut stream)
        .await
        .unwrap()
        .expect("the server answers with an error frame");
    assert_eq!(response.message_type, RAFT_MSG_ERROR);
    let message: String = serde_json::from_slice(&response.payload).unwrap();
    assert!(message.contains("unknown raft message type"), "{message}");
    // The connection is closed after the single response.
    let mut buf = [0u8; 8];
    let read = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await;
    match read {
        Ok(Ok(0)) | Ok(Err(_)) => {}
        other => panic!("expected the server to close after its answer, got {other:?}"),
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_connections_are_enforced() {
    let mut config = test_transport_config();
    config.max_connections = 2;
    // Hold idle connections long enough to observe the third being refused.
    config.snapshot_timeout = Duration::from_secs(30);
    let server = TransportServer::bind(
        "127.0.0.1:0",
        TransportSecurity::PlaintextForTesting,
        mongreldb_cluster::network::TransportRegistry::new(),
        config,
    )
    .await
    .unwrap();

    let mut first = TcpStream::connect(server.local_addr()).await.unwrap();
    let mut second = TcpStream::connect(server.local_addr()).await.unwrap();
    // The third connection exceeds the bound and is closed immediately.
    let mut third = TcpStream::connect(server.local_addr()).await.unwrap();
    let mut buf = [0u8; 8];
    let third_read = tokio::time::timeout(Duration::from_secs(2), third.read(&mut buf)).await;
    match third_read {
        Ok(Ok(0)) | Ok(Err(_)) => {}
        other => panic!("the over-bound connection must be closed at once, got {other:?}"),
    }
    // The two admitted connections are held (no EOF within the window).
    for stream in [&mut first, &mut second] {
        let read = tokio::time::timeout(Duration::from_millis(200), stream.read(&mut buf)).await;
        assert!(read.is_err(), "an admitted connection must stay open");
    }
    drop(first);
    drop(second);
    server.shutdown().await;
}

/// The consensus `TransportError` surface for an RPC against a server with
/// no attached group.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unattached_node_reports_remote_error() {
    let server = TransportServer::bind(
        "127.0.0.1:0",
        TransportSecurity::PlaintextForTesting,
        mongreldb_cluster::network::TransportRegistry::new(),
        test_transport_config(),
    )
    .await
    .unwrap();
    let client = TcpTransport::new(
        test_transport_config(),
        TransportSecurity::PlaintextForTesting,
    );
    client.upsert_peer(7, PeerEndpoint::plaintext(server.local_addr().to_string()));
    let error = client.trigger_election(7).await.unwrap_err();
    assert!(
        matches!(
            error,
            mongreldb_consensus::network::TransportError::Fault(ref message)
                if message.contains("not attached")
        ),
        "expected the remote not-attached error, got: {error}"
    );
    // No route at all is reported locally without any connection attempt.
    let unrouted = client.trigger_election(99).await.unwrap_err();
    assert!(
        matches!(
            unrouted,
            mongreldb_consensus::network::TransportError::NoRoute(99)
        ),
        "expected NoRoute, got: {unrouted}"
    );
    server.shutdown().await;
}
