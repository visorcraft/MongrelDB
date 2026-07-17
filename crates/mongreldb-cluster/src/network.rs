//! Cluster transport: TCP/TLS implementation of the consensus `RaftTransport`
//! trait (spec section 6.7, Stage 2C transport; ADR-0005; the mTLS direction
//! of spec section 14.3).
//!
//! # Wire format
//!
//! Every raft RPC travels as one [`ProtocolEnvelope`] frame per direction on
//! a dedicated connection: the client connects, writes the request frame,
//! reads exactly one response frame, and closes. The frame carries the
//! versioned, checksummed envelope header (protocol version, message type,
//! payload length, payload CRC-32) of `mongreldb-protocol`; unknown protocol
//! versions, oversized payloads, truncated frames, and checksum mismatches
//! fail closed (spec section 4.10). Request payloads are the serde encoding
//! (JSON today) of the openraft RPC type wrapped with the target raft id;
//! response payloads are the serde encoding of the openraft response type, or
//! [`RAFT_MSG_ERROR`] with an explanatory message when dispatch failed.
//!
//! One connection per RPC keeps the in-flight bound trivial (one request per
//! connection) and makes EOF/reconnect handling total: a failed connection is
//! never reused. Connection pooling and a compact binary payload codec are
//! deliberate follow-ups; correctness and bounds come first.
//!
//! # Transport security
//!
//! Production deployments must use [`TransportSecurity::Mtls`]: TLS 1.3 only,
//! both peers authenticate with certificates chaining to the cluster CA
//! (TrustConfig material), and node certificates bind node identities. The
//! binding scheme: a node certificate carries its durable 128-bit [`NodeId`]
//! as the subject CN and as a SAN dNSName of the form
//! `node-<lowercase hex of the 16-byte NodeId>.mongreldb.cluster`
//! ([`node_cert_name`]). The client authenticates the server by passing that
//! name as the TLS server name, so rustls/webpki verifies the certificate
//! covers the expected identity. The server cannot rely on a server-name
//! check for client certificates, so after the handshake it extracts the
//! client certificate's CN and SAN dNSNames (a minimal DER walk, no extra
//! dependencies) and requires one of them to name an admitted node
//! ([`TlsConfig::allows_identity`], sourced from
//! [`crate::bootstrap::TrustConfig::allowed_node_ids`]).
//!
//! [`TransportSecurity::PlaintextForTesting`] exists for loopback tests only.
//!
//! # Failure semantics
//!
//! Connect failures surface to openraft as `Unreachable`, timeouts as
//! `Timeout`, and every other transport failure as `NetworkError`; nothing
//! panics, and openraft drives retries and failover on top. The transport
//! itself retries only connection establishment with bounded exponential
//! backoff, so a rebooting peer rejoins without every RPC failing instantly.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mongreldb_consensus::identity::{MongrelRaft, MongrelRaftConfig, RaftNodeId};
use mongreldb_consensus::network::{AppendRpcError, RaftTransport, SnapshotRpcError, VoteRpcError};
use mongreldb_protocol::envelope::{
    EnvelopeError, ProtocolEnvelope, CHECKSUM_LEN, HEADER_LEN, MAX_MESSAGE_PAYLOAD_BYTES,
};
use mongreldb_types::ids::NodeId;
use openraft::error::{NetworkError, RPCError, Timeout, Unreachable};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::RPCTypes;
use rustls::pki_types::{CertificateDer, ServerName};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

/// Response frame reporting a server-side dispatch failure; the payload is a
/// JSON string with the reason. Distinct from every RPC response type so a
/// client never mistakes an error for a raft answer.
pub const RAFT_MSG_ERROR: u32 = 0;
/// AppendEntries request frame.
pub const RAFT_MSG_APPEND_ENTRIES_REQUEST: u32 = 1;
/// AppendEntries response frame.
pub const RAFT_MSG_APPEND_ENTRIES_RESPONSE: u32 = 2;
/// RequestVote request frame.
pub const RAFT_MSG_VOTE_REQUEST: u32 = 3;
/// RequestVote response frame.
pub const RAFT_MSG_VOTE_RESPONSE: u32 = 4;
/// InstallSnapshot chunk request frame.
pub const RAFT_MSG_INSTALL_SNAPSHOT_REQUEST: u32 = 5;
/// InstallSnapshot chunk response frame.
pub const RAFT_MSG_INSTALL_SNAPSHOT_RESPONSE: u32 = 6;
/// Election-trigger request frame (best-effort leadership transfer).
pub const RAFT_MSG_TRIGGER_ELECTION_REQUEST: u32 = 7;
/// Election-trigger response frame.
pub const RAFT_MSG_TRIGGER_ELECTION_RESPONSE: u32 = 8;

/// DNS domain node certificate names live under; see [`node_cert_name`].
pub const NODE_CERT_DOMAIN: &str = "mongreldb.cluster";

/// The one error type of the cluster transport.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The target node is not in the peer directory.
    #[error("no route to node {0}")]
    NoRoute(RaftNodeId),
    /// Socket or stream I/O failed (connect, read, write, peer EOF).
    #[error("transport I/O error: {0}")]
    Io(#[from] io::Error),
    /// An RPC round trip, connect, or handshake exceeded its bound.
    #[error("operation timed out after {0:?}")]
    Timeout(Duration),
    /// TLS configuration or handshake failed.
    #[error("TLS error: {0}")]
    Tls(String),
    /// The frame failed envelope verification (bad version, checksum,
    /// truncation, trailing bytes).
    #[error("protocol envelope error: {0}")]
    Envelope(#[from] EnvelopeError),
    /// The frame's declared payload exceeds the configured bound; rejected
    /// before any payload bytes are read or allocated.
    #[error("frame payload of {actual} bytes exceeds the {limit} byte bound")]
    FrameTooLarge {
        /// Declared payload length.
        actual: usize,
        /// Configured bound.
        limit: usize,
    },
    /// The peer's certificate does not authenticate it as an admitted node.
    #[error("peer authentication failed: {0}")]
    PeerAuthentication(String),
    /// A payload failed serde encoding/decoding, or the response frame type
    /// did not match the request.
    #[error("protocol violation: {0}")]
    ProtocolViolation(String),
    /// The peer answered with an error frame (no attached group, raft core
    /// failure, unknown message type).
    #[error("remote node error: {0}")]
    Remote(String),
}

/// Static configuration of a [`TcpTransport`] / [`TransportServer`].
#[derive(Clone, Debug)]
pub struct TransportConfig {
    /// Bound on establishing one TCP connection.
    pub connect_timeout: Duration,
    /// Bound on one AppendEntries/Vote/election-trigger round trip and on
    /// writing a response.
    pub rpc_timeout: Duration,
    /// Bound on one InstallSnapshot round trip; also the server's bound on
    /// reading a request frame (snapshot chunks can be large).
    pub snapshot_timeout: Duration,
    /// Connection-establishment attempts per RPC before surfacing
    /// `Unreachable` (openraft retries above this).
    pub connect_attempts: usize,
    /// Base of the exponential reconnect backoff between attempts.
    pub reconnect_backoff: Duration,
    /// Largest accepted frame payload; hard-capped at
    /// [`MAX_MESSAGE_PAYLOAD_BYTES`]. Snapshot chunks larger than this fail
    /// the RPC, so the bound must cover the configured snapshot chunking.
    pub max_frame_bytes: usize,
    /// Largest number of concurrently held inbound connections; excess
    /// connections are closed immediately (bounded, fail closed).
    pub max_connections: usize,
    /// Bound on the TLS handshake, both directions.
    pub handshake_timeout: Duration,
    /// Grace period for in-flight connections during server shutdown before
    /// they are aborted.
    pub shutdown_grace: Duration,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_millis(2_000),
            rpc_timeout: Duration::from_millis(1_000),
            snapshot_timeout: Duration::from_millis(10_000),
            connect_attempts: 3,
            reconnect_backoff: Duration::from_millis(25),
            max_frame_bytes: 16 * 1024 * 1024,
            max_connections: 256,
            handshake_timeout: Duration::from_millis(3_000),
            shutdown_grace: Duration::from_millis(5_000),
        }
    }
}

/// How one peer is reached, and — under mTLS — who it must prove to be.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerEndpoint {
    /// `host:port` address of the peer's [`TransportServer`].
    pub address: String,
    /// The durable node identity the peer's certificate must bind (mTLS).
    /// Required when the transport runs [`TransportSecurity::Mtls`]; the
    /// connection fails closed without it. Ignored under plaintext.
    pub tls_node_id: Option<NodeId>,
}

impl PeerEndpoint {
    /// A plaintext-testing endpoint (no identity binding).
    pub fn plaintext(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            tls_node_id: None,
        }
    }

    /// An mTLS endpoint bound to `node_id`'s certificate identity.
    pub fn mtls(address: impl Into<String>, node_id: NodeId) -> Self {
        Self {
            address: address.into(),
            tls_node_id: Some(node_id),
        }
    }
}

/// The dispatch table shared by a [`TcpTransport`] and its
/// [`TransportServer`]: which local raft nodes inbound RPCs route to.
/// `RaftTransport::attach`/`detach` registrations land here, so the server's
/// view of attached groups follows the trait lifecycle exactly.
#[derive(Clone, Default)]
pub struct TransportRegistry {
    nodes: Arc<Mutex<HashMap<RaftNodeId, MongrelRaft>>>,
}

impl TransportRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a running raft node (called by `RaftTransport::attach`).
    pub fn attach(&self, node_id: RaftNodeId, raft: MongrelRaft) {
        self.lock().insert(node_id, raft);
    }

    /// Deregisters a raft node (called by `RaftTransport::detach`).
    pub fn detach(&self, node_id: RaftNodeId) {
        self.lock().remove(&node_id);
    }

    /// The raft handle registered for `node_id`, if any.
    pub fn get(&self, node_id: RaftNodeId) -> Option<MongrelRaft> {
        self.lock().get(&node_id).cloned()
    }

    /// Number of attached nodes.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether no nodes are attached.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<RaftNodeId, MongrelRaft>> {
        self.nodes.lock().expect("transport registry lock poisoned")
    }
}

impl fmt::Debug for TransportRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransportRegistry")
            .field("attached", &self.len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Transport security: mTLS configuration and the node-identity binding
// ---------------------------------------------------------------------------

/// The canonical certificate name binding a durable [`NodeId`]: both the
/// subject CN and a SAN dNSName of a node certificate carry this value (see
/// `tests/fixtures/README.md` for the generation commands).
pub fn node_cert_name(node_id: &NodeId) -> String {
    let mut hex = String::with_capacity(32);
    for byte in node_id.as_bytes() {
        hex.push(char::from_digit(u32::from(byte >> 4), 16).expect("hex digit"));
        hex.push(char::from_digit(u32::from(byte & 0x0f), 16).expect("hex digit"));
    }
    format!("node-{hex}.{NODE_CERT_DOMAIN}")
}

/// Parsed mTLS material: TLS 1.3-only rustls configurations (ring provider)
/// plus the admitted peer identities.
///
/// Built from PEM material — normally the node's persisted
/// [`crate::bootstrap::TrustConfig`] via [`TlsConfig::from_trust`]. Loading is
/// fail-closed: unparsable or empty certificate/key material and an empty
/// admitted-identity set are errors, never silently degraded.
#[derive(Clone)]
pub struct TlsConfig {
    client_config: Arc<rustls::ClientConfig>,
    server_config: Arc<rustls::ServerConfig>,
    allowed_identities: Arc<BTreeSet<String>>,
}

impl TlsConfig {
    /// Loads mTLS material from PEM strings: the cluster CA chain, this
    /// node's certificate and private key, and the durable identities of the
    /// nodes admitted to the cluster.
    pub fn from_pems(
        ca_cert_pem: &str,
        node_cert_pem: &str,
        node_key_pem: &str,
        allowed_node_ids: &[NodeId],
    ) -> Result<Self, TransportError> {
        let ca_certs = parse_pem_certificates("ca_cert_pem", ca_cert_pem)?;
        let mut roots = rustls::RootCertStore::empty();
        for cert in ca_certs {
            roots.add(cert).map_err(|e| {
                TransportError::Tls(format!("cluster CA certificate rejected: {e}"))
            })?;
        }
        let roots = Arc::new(roots);
        let node_certs = parse_pem_certificates("node_cert_pem", node_cert_pem)?;
        let key = rustls_pemfile::private_key(&mut io::BufReader::new(node_key_pem.as_bytes()))
            .map_err(|e| TransportError::Tls(format!("node_key_pem unreadable: {e}")))?
            .ok_or(TransportError::Tls(
                "node_key_pem contains no private key".to_owned(),
            ))?;
        if allowed_node_ids.is_empty() {
            return Err(TransportError::Tls(
                "allowed_node_ids is empty; an empty list admits no peers".to_owned(),
            ));
        }
        let allowed_identities = Arc::new(
            allowed_node_ids
                .iter()
                .map(node_cert_name)
                .collect::<BTreeSet<_>>(),
        );

        // The explicit provider keeps behavior deterministic regardless of
        // feature unification elsewhere in the build; TLS 1.3 only.
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let client_verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            roots.clone(),
            provider.clone(),
        )
        .build()
        .map_err(|e| TransportError::Tls(format!("client-certificate verifier: {e}")))?;
        let server_config = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| TransportError::Tls(format!("TLS 1.3 server configuration: {e}")))?
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(node_certs.clone(), key.clone_key())
            .map_err(|e| TransportError::Tls(format!("node certificate/key rejected: {e}")))?;
        let client_config = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| TransportError::Tls(format!("TLS 1.3 client configuration: {e}")))?
            .with_root_certificates(roots)
            .with_client_auth_cert(node_certs, key)
            .map_err(|e| TransportError::Tls(format!("node certificate/key rejected: {e}")))?;
        Ok(Self {
            client_config: Arc::new(client_config),
            server_config: Arc::new(server_config),
            allowed_identities,
        })
    }

    /// Loads mTLS material from the node's persisted trust configuration.
    pub fn from_trust(trust: &crate::bootstrap::TrustConfig) -> Result<Self, TransportError> {
        Self::from_pems(
            &trust.ca_cert_pem,
            &trust.node_cert_pem,
            &trust.node_key_pem,
            &trust.allowed_node_ids,
        )
    }

    /// The TLS 1.3 client configuration (presents the node certificate).
    pub fn client_config(&self) -> Arc<rustls::ClientConfig> {
        self.client_config.clone()
    }

    /// The TLS 1.3 server configuration (requires client certificates).
    pub fn server_config(&self) -> Arc<rustls::ServerConfig> {
        self.server_config.clone()
    }

    /// Whether a peer certificate identity (CN or SAN dNSName of the
    /// [`node_cert_name`] form) is admitted to the cluster.
    pub fn allows_identity(&self, identity: &str) -> bool {
        self.allowed_identities.contains(identity)
    }
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConfig")
            .field("allowed_identities", &self.allowed_identities)
            .finish_non_exhaustive()
    }
}

/// Parses all PEM-armored certificates; fails closed on empty or unparsable
/// material.
fn parse_pem_certificates(
    field: &'static str,
    pem: &str,
) -> Result<Vec<CertificateDer<'static>>, TransportError> {
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut io::BufReader::new(pem.as_bytes()))
            .collect::<Result<_, _>>()
            .map_err(|e| TransportError::Tls(format!("{field} unreadable: {e}")))?;
    if certs.is_empty() {
        return Err(TransportError::Tls(format!(
            "{field} contains no certificates"
        )));
    }
    Ok(certs)
}

/// How the transport authenticates its peers.
///
/// Production deployments must use [`TransportSecurity::Mtls`]: spec section
/// 14.3 requires mutual authentication on cluster internal RPC, with node
/// certificates binding node IDs.
#[derive(Clone, Debug)]
pub enum TransportSecurity {
    /// TLS 1.3 mutual authentication with node-identity-bound certificates
    /// (the production mode).
    Mtls(TlsConfig),
    /// Plaintext TCP without any authentication or confidentiality.
    ///
    /// **Testing only.** Production deployments must run
    /// [`TransportSecurity::Mtls`]: an unauthenticated raft port lets any
    /// process that can reach it inject votes, append entries, and snapshots
    /// into the consensus group.
    PlaintextForTesting,
}

/// The certificate name a peer certificate must carry for `node_id`.
fn peer_server_name(peer: &PeerEndpoint) -> Result<ServerName<'static>, TransportError> {
    let Some(node_id) = peer.tls_node_id else {
        return Err(TransportError::PeerAuthentication(format!(
            "mTLS endpoint {} has no node identity to authenticate against",
            peer.address
        )));
    };
    let name = node_cert_name(&node_id);
    ServerName::try_from(name.clone())
        .map_err(|e| TransportError::Tls(format!("invalid peer server name {name}: {e}")))
}

/// Authenticates a connected peer's certificate chain against the admitted
/// node identities (server-side check; the chain itself was already verified
/// against the cluster CA by the handshake).
fn verify_peer_identity(
    peer_certificates: Option<&[CertificateDer<'_>]>,
    tls: &TlsConfig,
) -> Result<(), TransportError> {
    let Some(certificates) = peer_certificates else {
        return Err(TransportError::PeerAuthentication(
            "peer presented no certificate".to_owned(),
        ));
    };
    let Some(leaf) = certificates.first() else {
        return Err(TransportError::PeerAuthentication(
            "peer presented an empty certificate chain".to_owned(),
        ));
    };
    let identities = der::certificate_identities(leaf.as_ref()).map_err(|e| {
        TransportError::PeerAuthentication(format!("peer certificate identity unreadable: {e}"))
    })?;
    if identities
        .iter()
        .any(|identity| tls.allows_identity(identity))
    {
        Ok(())
    } else {
        Err(TransportError::PeerAuthentication(format!(
            "peer certificate identities {identities:?} are not admitted to this cluster"
        )))
    }
}

/// Minimal DER walker extracting node identities (subject CN and dNSName
/// SANs) from an X.509 certificate.
///
/// This is deliberately not a general ASN.1 parser: it accepts only the
/// narrow DER shape of a certificate — enough to find the subject and the
/// subjectAltName extension — and fails closed on anything unexpected. The
/// crate's dependency set is frozen, so no X.509 parsing crate is available;
/// the walk is verified against the `tests/fixtures` certificates.
mod der {
    /// One parsed TLV element.
    struct Tlv<'a> {
        tag: u8,
        content: &'a [u8],
        /// Total encoded length (header + content).
        encoded_len: usize,
    }

    const TAG_SEQUENCE: u8 = 0x30;
    const TAG_SET: u8 = 0x31;
    const TAG_OCTET_STRING: u8 = 0x04;
    const TAG_OID: u8 = 0x06;
    const TAG_UTF8_STRING: u8 = 0x0c;
    const TAG_PRINTABLE_STRING: u8 = 0x13;
    const TAG_IA5_STRING: u8 = 0x16;
    /// `extensions [3] EXPLICIT` in TBSCertificate.
    const TAG_EXTENSIONS: u8 = 0xa3;
    /// GeneralName `dNSName [2] IA5String` (context, primitive).
    const TAG_SAN_DNS_NAME: u8 = 0x82;

    /// OID 2.5.4.3 (id-at-commonName), DER content bytes.
    const OID_COMMON_NAME: &[u8] = &[0x55, 0x04, 0x03];
    /// OID 2.5.29.17 (id-ce-subjectAltName), DER content bytes.
    const OID_SUBJECT_ALT_NAME: &[u8] = &[0x55, 0x1d, 0x11];

    /// Reads one DER TLV from the front of `buf`.
    fn read_tlv(buf: &[u8]) -> Result<Tlv<'_>, String> {
        if buf.len() < 2 {
            return Err("truncated TLV header".to_owned());
        }
        let tag = buf[0];
        if tag & 0x1f == 0x1f {
            return Err("multi-byte tags are outside the certificate subset".to_owned());
        }
        let first_len = buf[1];
        let (content_len, header_len) = if first_len & 0x80 == 0 {
            (usize::from(first_len), 2)
        } else {
            let len_bytes = usize::from(first_len & 0x7f);
            // 0 would be indefinite length, which DER forbids; certificate
            // lengths never need more than 4 bytes.
            if len_bytes == 0 || len_bytes > 4 {
                return Err("invalid DER length".to_owned());
            }
            if buf.len() < 2 + len_bytes {
                return Err("truncated DER length".to_owned());
            }
            let mut len = 0usize;
            for &byte in &buf[2..2 + len_bytes] {
                len = len
                    .checked_mul(256)
                    .and_then(|len| len.checked_add(usize::from(byte)))
                    .ok_or_else(|| "DER length overflow".to_owned())?;
            }
            (len, 2 + len_bytes)
        };
        if buf.len() < header_len + content_len {
            return Err("truncated TLV content".to_owned());
        }
        Ok(Tlv {
            tag,
            content: &buf[header_len..header_len + content_len],
            encoded_len: header_len + content_len,
        })
    }

    fn expect_directory_string(tlv: &Tlv<'_>) -> Result<String, String> {
        match tlv.tag {
            TAG_UTF8_STRING | TAG_PRINTABLE_STRING | TAG_IA5_STRING => {
                std::str::from_utf8(tlv.content)
                    .map(str::to_owned)
                    .map_err(|_| "non-UTF8 directory string".to_owned())
            }
            other => Err(format!("unexpected string tag 0x{other:02x}")),
        }
    }

    /// Extracts the subject CN values and every dNSName SAN from a DER
    /// certificate. Malformed input fails closed with an error.
    pub fn certificate_identities(cert_der: &[u8]) -> Result<Vec<String>, String> {
        let certificate = read_tlv(cert_der)?;
        if certificate.tag != TAG_SEQUENCE || certificate.encoded_len != cert_der.len() {
            return Err("certificate is not exactly one SEQUENCE".to_owned());
        }
        let tbs = read_tlv(certificate.content)?;
        if tbs.tag != TAG_SEQUENCE {
            return Err("tbsCertificate is not a SEQUENCE".to_owned());
        }
        let mut identities = Vec::new();
        // TBSCertificate children in order: version [0]?, serialNumber,
        // signature SEQ, issuer Name SEQ, validity SEQ, subject Name SEQ,
        // spki SEQ, ..., extensions [3]?. Only SEQUENCEs count toward the
        // subject position, so the subject is always the fourth.
        let mut rest = tbs.content;
        let mut sequences_seen = 0u32;
        while !rest.is_empty() {
            let tlv = read_tlv(rest)?;
            rest = &rest[tlv.encoded_len..];
            match tlv.tag {
                TAG_SEQUENCE => {
                    sequences_seen += 1;
                    if sequences_seen == 4 {
                        identities.extend(subject_common_names(tlv.content)?);
                    }
                }
                TAG_EXTENSIONS => {
                    identities.extend(extension_san_dns_names(tlv.content)?);
                }
                _ => {}
            }
        }
        Ok(identities)
    }

    /// Collects every CN value of an RDNSequence.
    fn subject_common_names(rdn_sequence: &[u8]) -> Result<Vec<String>, String> {
        let mut names = Vec::new();
        let mut rest = rdn_sequence;
        while !rest.is_empty() {
            let rdn = read_tlv(rest)?;
            rest = &rest[rdn.encoded_len..];
            if rdn.tag != TAG_SET {
                return Err("RDN is not a SET".to_owned());
            }
            let mut rdn_rest = rdn.content;
            while !rdn_rest.is_empty() {
                let attribute = read_tlv(rdn_rest)?;
                rdn_rest = &rdn_rest[attribute.encoded_len..];
                if attribute.tag != TAG_SEQUENCE {
                    return Err("AttributeTypeAndValue is not a SEQUENCE".to_owned());
                }
                let oid = read_tlv(attribute.content)?;
                if oid.tag != TAG_OID {
                    return Err("attribute type is not an OID".to_owned());
                }
                if oid.content == OID_COMMON_NAME {
                    let value = read_tlv(&attribute.content[oid.encoded_len..])?;
                    names.push(expect_directory_string(&value)?);
                }
            }
        }
        Ok(names)
    }

    /// Collects every dNSName of the subjectAltName extension inside an
    /// `extensions [3] EXPLICIT` element.
    fn extension_san_dns_names(explicit_content: &[u8]) -> Result<Vec<String>, String> {
        let extensions = read_tlv(explicit_content)?;
        if extensions.tag != TAG_SEQUENCE || extensions.encoded_len != explicit_content.len() {
            return Err("malformed extensions".to_owned());
        }
        let mut names = Vec::new();
        let mut rest = extensions.content;
        while !rest.is_empty() {
            let extension = read_tlv(rest)?;
            rest = &rest[extension.encoded_len..];
            if extension.tag != TAG_SEQUENCE {
                return Err("extension is not a SEQUENCE".to_owned());
            }
            let oid = read_tlv(extension.content)?;
            if oid.tag != TAG_OID {
                return Err("extension id is not an OID".to_owned());
            }
            if oid.content != OID_SUBJECT_ALT_NAME {
                continue;
            }
            // extnValue is the last child; an optional BOOLEAN `critical`
            // may precede it.
            let mut ext_rest = &extension.content[oid.encoded_len..];
            let mut san_bytes = None;
            while !ext_rest.is_empty() {
                let child = read_tlv(ext_rest)?;
                ext_rest = &ext_rest[child.encoded_len..];
                if child.tag == TAG_OCTET_STRING {
                    san_bytes = Some(child.content);
                }
            }
            let Some(san_bytes) = san_bytes else {
                return Err("subjectAltName extension has no extnValue".to_owned());
            };
            let general_names = read_tlv(san_bytes)?;
            if general_names.tag != TAG_SEQUENCE || general_names.encoded_len != san_bytes.len() {
                return Err("malformed GeneralNames".to_owned());
            }
            let mut names_rest = general_names.content;
            while !names_rest.is_empty() {
                let name = read_tlv(names_rest)?;
                names_rest = &names_rest[name.encoded_len..];
                if name.tag == TAG_SAN_DNS_NAME {
                    names.push(
                        std::str::from_utf8(name.content)
                            .map(str::to_owned)
                            .map_err(|_| "non-UTF8 dNSName".to_owned())?,
                    );
                }
            }
        }
        Ok(names)
    }
}

// ---------------------------------------------------------------------------
// Frame codec
// ---------------------------------------------------------------------------

/// Wire payload of every request frame: the target raft id (so a server
/// hosting several groups can route, and a single-group server fails closed
/// on a mismatched target) plus the serde-encoded openraft RPC.
#[derive(Debug, Serialize, Deserialize)]
struct RpcPayload<T> {
    target: RaftNodeId,
    rpc: T,
}

/// Writes one envelope frame, bounded by `timeout`.
async fn write_frame<S: AsyncWrite + Unpin>(
    stream: &mut S,
    message_type: u32,
    payload: Vec<u8>,
    timeout: Duration,
) -> Result<(), TransportError> {
    match tokio::time::timeout(timeout, write_frame_inner(stream, message_type, payload)).await {
        Ok(result) => result,
        Err(_) => Err(TransportError::Timeout(timeout)),
    }
}

async fn write_frame_inner<S: AsyncWrite + Unpin>(
    stream: &mut S,
    message_type: u32,
    payload: Vec<u8>,
) -> Result<(), TransportError> {
    if payload.len() > MAX_MESSAGE_PAYLOAD_BYTES {
        return Err(TransportError::FrameTooLarge {
            actual: payload.len(),
            limit: MAX_MESSAGE_PAYLOAD_BYTES,
        });
    }
    let envelope = ProtocolEnvelope::new(message_type, payload);
    stream.write_all(&envelope.encode()).await?;
    stream.flush().await?;
    Ok(())
}

/// Reads one envelope frame. `Ok(None)` is a clean EOF at a frame boundary;
/// a partial frame is an error. The declared payload length is checked
/// against `max_frame_bytes` (hard-capped at [`MAX_MESSAGE_PAYLOAD_BYTES`])
/// before any payload bytes are read or allocated.
async fn read_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    max_frame_bytes: usize,
    timeout: Duration,
) -> Result<Option<ProtocolEnvelope>, TransportError> {
    match tokio::time::timeout(timeout, read_frame_inner(stream, max_frame_bytes)).await {
        Ok(result) => result,
        Err(_) => Err(TransportError::Timeout(timeout)),
    }
}

async fn read_frame_inner<S: AsyncRead + Unpin>(
    stream: &mut S,
    max_frame_bytes: usize,
) -> Result<Option<ProtocolEnvelope>, TransportError> {
    let mut header = [0u8; HEADER_LEN];
    // A peer between RPCs closes cleanly; a peer mid-frame does not.
    let first = stream.read(&mut header[..1]).await?;
    if first == 0 {
        return Ok(None);
    }
    stream.read_exact(&mut header[1..]).await?;
    let payload_len = u32::from_le_bytes(header[8..12].try_into().expect("header slice")) as usize;
    let bound = max_frame_bytes.min(MAX_MESSAGE_PAYLOAD_BYTES);
    if payload_len > bound {
        return Err(TransportError::FrameTooLarge {
            actual: payload_len,
            limit: bound,
        });
    }
    let mut frame = Vec::with_capacity(HEADER_LEN + payload_len + CHECKSUM_LEN);
    frame.extend_from_slice(&header);
    frame.resize(HEADER_LEN + payload_len + CHECKSUM_LEN, 0);
    stream.read_exact(&mut frame[HEADER_LEN..]).await?;
    Ok(Some(ProtocolEnvelope::decode(&frame)?))
}

// ---------------------------------------------------------------------------
// TcpTransport (client side + registry owner)
// ---------------------------------------------------------------------------

/// TCP/TLS [`RaftTransport`]: the client side of the cluster transport plus
/// the owner of the [`TransportRegistry`] a [`TransportServer`] dispatches
/// against.
///
/// Peer addresses are resolved through the transport's own peer directory
/// ([`TcpTransport::upsert_peer`]), fed by the cluster membership directory;
/// openraft's `BasicNode` addresses are not consulted. One RPC runs per
/// connection; see the module documentation for the rationale and bounds.
pub struct TcpTransport {
    config: TransportConfig,
    security: TransportSecurity,
    peers: Arc<Mutex<HashMap<RaftNodeId, PeerEndpoint>>>,
    registry: TransportRegistry,
}

impl TcpTransport {
    /// A transport with `security` and an empty peer directory.
    pub fn new(config: TransportConfig, security: TransportSecurity) -> Self {
        Self {
            config,
            security,
            peers: Arc::new(Mutex::new(HashMap::new())),
            registry: TransportRegistry::new(),
        }
    }

    /// The registry a [`TransportServer`] binds to dispatch inbound RPCs to
    /// the groups attached to this transport.
    pub fn registry(&self) -> TransportRegistry {
        self.registry.clone()
    }

    /// The transport bounds (shared with the server by convention).
    pub fn config(&self) -> &TransportConfig {
        &self.config
    }

    /// Registers or replaces a peer endpoint (membership directory update).
    pub fn upsert_peer(&self, node_id: RaftNodeId, endpoint: PeerEndpoint) {
        self.peers
            .lock()
            .expect("peer directory lock poisoned")
            .insert(node_id, endpoint);
    }

    /// Removes a peer; later RPCs to it fail with
    /// [`TransportError::NoRoute`].
    pub fn remove_peer(&self, node_id: RaftNodeId) -> Option<PeerEndpoint> {
        self.peers
            .lock()
            .expect("peer directory lock poisoned")
            .remove(&node_id)
    }

    /// The endpoint registered for `node_id`, if any.
    pub fn peer(&self, node_id: RaftNodeId) -> Option<PeerEndpoint> {
        self.peers
            .lock()
            .expect("peer directory lock poisoned")
            .get(&node_id)
            .cloned()
    }

    /// One RPC round trip: connect (with bounded reconnect backoff), write
    /// the request frame, read the single response frame.
    async fn round_trip(
        &self,
        target: RaftNodeId,
        request_type: u32,
        expected_response_type: u32,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<Vec<u8>, TransportError> {
        let peer = self.peer(target).ok_or(TransportError::NoRoute(target))?;
        let attempts = self.config.connect_attempts.max(1);
        let mut attempt = 0usize;
        let mut stream = loop {
            match self.connect_once(&peer).await {
                Ok(stream) => break stream,
                Err(error) => {
                    attempt += 1;
                    // Only socket-level connect failures are retryable; TLS
                    // and configuration failures fail fast.
                    if attempt >= attempts || !matches!(error, TransportError::Io(_)) {
                        return Err(error);
                    }
                    let shift = u32::try_from(attempt - 1).unwrap_or(u32::MAX).min(10);
                    tokio::time::sleep(self.config.reconnect_backoff * (1u32 << shift)).await;
                }
            }
        };
        match &mut stream {
            ClientStream::Plain(stream) => {
                self.round_trip_on(
                    stream,
                    request_type,
                    expected_response_type,
                    payload,
                    timeout,
                )
                .await
            }
            ClientStream::Tls(stream) => {
                self.round_trip_on(
                    stream,
                    request_type,
                    expected_response_type,
                    payload,
                    timeout,
                )
                .await
            }
        }
    }

    async fn round_trip_on<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        request_type: u32,
        expected_response_type: u32,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<Vec<u8>, TransportError> {
        write_frame(stream, request_type, payload, timeout).await?;
        let Some(frame) = read_frame(stream, self.config.max_frame_bytes, timeout).await? else {
            return Err(TransportError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "peer closed the connection without answering",
            )));
        };
        if frame.message_type == RAFT_MSG_ERROR {
            let message = serde_json::from_slice(&frame.payload)
                .unwrap_or_else(|_| "remote dispatch failure".to_owned());
            return Err(TransportError::Remote(message));
        }
        if frame.message_type != expected_response_type {
            return Err(TransportError::ProtocolViolation(format!(
                "expected response type {expected_response_type}, got {}",
                frame.message_type
            )));
        }
        Ok(frame.payload)
    }

    /// Establishes one connection: TCP, then the TLS 1.3 handshake and — for
    /// mTLS — server-identity verification against the peer directory entry.
    async fn connect_once(&self, peer: &PeerEndpoint) -> Result<ClientStream, TransportError> {
        let tcp = match tokio::time::timeout(
            self.config.connect_timeout,
            TcpStream::connect(&peer.address),
        )
        .await
        {
            Ok(Ok(tcp)) => tcp,
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => return Err(TransportError::Timeout(self.config.connect_timeout)),
        };
        let _ = tcp.set_nodelay(true);
        match &self.security {
            TransportSecurity::PlaintextForTesting => Ok(ClientStream::Plain(tcp)),
            TransportSecurity::Mtls(tls) => {
                let server_name = peer_server_name(peer)?;
                let connector = tokio_rustls::TlsConnector::from(tls.client_config());
                match tokio::time::timeout(
                    self.config.handshake_timeout,
                    connector.connect(server_name, tcp),
                )
                .await
                {
                    Ok(Ok(stream)) => Ok(ClientStream::Tls(Box::new(stream))),
                    Ok(Err(error)) => Err(TransportError::Tls(format!(
                        "TLS handshake with {} failed: {error}",
                        peer.address
                    ))),
                    Err(_) => Err(TransportError::Timeout(self.config.handshake_timeout)),
                }
            }
        }
    }

    fn encode_payload<T: Serialize>(
        target: RaftNodeId,
        rpc: &T,
    ) -> Result<Vec<u8>, TransportError> {
        serde_json::to_vec(&RpcPayload { target, rpc })
            .map_err(|e| TransportError::ProtocolViolation(format!("unencodable request: {e}")))
    }

    fn append_error(from: RaftNodeId, target: RaftNodeId, error: TransportError) -> AppendRpcError {
        match error {
            TransportError::Timeout(timeout) => RPCError::Timeout(Timeout {
                action: RPCTypes::AppendEntries,
                id: from,
                target,
                timeout,
            }),
            error @ (TransportError::NoRoute(_)
            | TransportError::Io(_)
            | TransportError::Tls(_)) => RPCError::Unreachable(Unreachable::new(&error)),
            error => RPCError::Network(NetworkError::new(&error)),
        }
    }

    fn vote_error(from: RaftNodeId, target: RaftNodeId, error: TransportError) -> VoteRpcError {
        match error {
            TransportError::Timeout(timeout) => RPCError::Timeout(Timeout {
                action: RPCTypes::Vote,
                id: from,
                target,
                timeout,
            }),
            error @ (TransportError::NoRoute(_)
            | TransportError::Io(_)
            | TransportError::Tls(_)) => RPCError::Unreachable(Unreachable::new(&error)),
            error => RPCError::Network(NetworkError::new(&error)),
        }
    }

    fn snapshot_error(
        from: RaftNodeId,
        target: RaftNodeId,
        error: TransportError,
    ) -> SnapshotRpcError {
        match error {
            TransportError::Timeout(timeout) => RPCError::Timeout(Timeout {
                action: RPCTypes::InstallSnapshot,
                id: from,
                target,
                timeout,
            }),
            error @ (TransportError::NoRoute(_)
            | TransportError::Io(_)
            | TransportError::Tls(_)) => RPCError::Unreachable(Unreachable::new(&error)),
            error => RPCError::Network(NetworkError::new(&error)),
        }
    }
}

/// One established client connection. The TLS variant is boxed: a rustls
/// connection state dwarfs the plain variant.
enum ClientStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl RaftTransport for TcpTransport {
    async fn append_entries(
        &self,
        from: RaftNodeId,
        target: RaftNodeId,
        rpc: AppendEntriesRequest<MongrelRaftConfig>,
    ) -> Result<AppendEntriesResponse<RaftNodeId>, AppendRpcError> {
        let result = async {
            let payload = Self::encode_payload(target, &rpc)?;
            self.round_trip(
                target,
                RAFT_MSG_APPEND_ENTRIES_REQUEST,
                RAFT_MSG_APPEND_ENTRIES_RESPONSE,
                payload,
                self.config.rpc_timeout,
            )
            .await
        }
        .await;
        match result {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                RPCError::Network(NetworkError::new(&TransportError::ProtocolViolation(
                    format!("undecodable AppendEntries response: {e}"),
                )))
            }),
            Err(error) => Err(Self::append_error(from, target, error)),
        }
    }

    async fn vote(
        &self,
        from: RaftNodeId,
        target: RaftNodeId,
        rpc: VoteRequest<RaftNodeId>,
    ) -> Result<VoteResponse<RaftNodeId>, VoteRpcError> {
        let result = async {
            let payload = Self::encode_payload(target, &rpc)?;
            self.round_trip(
                target,
                RAFT_MSG_VOTE_REQUEST,
                RAFT_MSG_VOTE_RESPONSE,
                payload,
                self.config.rpc_timeout,
            )
            .await
        }
        .await;
        match result {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                RPCError::Network(NetworkError::new(&TransportError::ProtocolViolation(
                    format!("undecodable Vote response: {e}"),
                )))
            }),
            Err(error) => Err(Self::vote_error(from, target, error)),
        }
    }

    async fn install_snapshot(
        &self,
        from: RaftNodeId,
        target: RaftNodeId,
        rpc: InstallSnapshotRequest<MongrelRaftConfig>,
    ) -> Result<InstallSnapshotResponse<RaftNodeId>, SnapshotRpcError> {
        let result = async {
            let payload = Self::encode_payload(target, &rpc)?;
            self.round_trip(
                target,
                RAFT_MSG_INSTALL_SNAPSHOT_REQUEST,
                RAFT_MSG_INSTALL_SNAPSHOT_RESPONSE,
                payload,
                self.config.snapshot_timeout,
            )
            .await
        }
        .await;
        match result {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                RPCError::Network(NetworkError::new(&TransportError::ProtocolViolation(
                    format!("undecodable InstallSnapshot response: {e}"),
                )))
            }),
            Err(error) => Err(Self::snapshot_error(from, target, error)),
        }
    }

    fn attach(&self, node_id: RaftNodeId, raft: MongrelRaft) {
        self.registry.attach(node_id, raft);
    }

    fn detach(&self, node_id: RaftNodeId) {
        self.registry.detach(node_id);
    }

    async fn trigger_election(
        &self,
        target: RaftNodeId,
    ) -> Result<(), mongreldb_consensus::network::TransportError> {
        let payload = Self::encode_payload(target, &()).map_err(|error| {
            mongreldb_consensus::network::TransportError::Fault(error.to_string())
        })?;
        self.round_trip(
            target,
            RAFT_MSG_TRIGGER_ELECTION_REQUEST,
            RAFT_MSG_TRIGGER_ELECTION_RESPONSE,
            payload,
            self.config.rpc_timeout,
        )
        .await
        .map(|_| ())
        .map_err(|error| match error {
            TransportError::NoRoute(node) => {
                mongreldb_consensus::network::TransportError::NoRoute(node)
            }
            // The consensus error taxonomy has no transport-failure
            // variant; `Fault` carries the typed message.
            other => mongreldb_consensus::network::TransportError::Fault(other.to_string()),
        })
    }
}

impl fmt::Debug for TcpTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpTransport")
            .field("config", &self.config)
            .field("security", &self.security)
            .field("peers", &self.peers.lock().map(|peers| peers.len()))
            .field("registry", &self.registry)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// TransportServer (listener + dispatcher)
// ---------------------------------------------------------------------------

/// The inbound side of the cluster transport: accepts connections on a tokio
/// task, runs the TLS handshake and peer-identity check under mTLS, reads the
/// single request frame, dispatches it to the attached raft node named by its
/// target id, and writes one response frame.
///
/// Bounds: at most [`TransportConfig::max_connections`] connections are held
/// concurrently (excess connections are closed immediately, failing closed),
/// one in-flight frame per connection, every read/write/handshake bounded by
/// its configured timeout, and frame payloads bounded by
/// [`TransportConfig::max_frame_bytes`]. [`TransportServer::shutdown`] stops
/// accepting, lets in-flight connections finish within
/// [`TransportConfig::shutdown_grace`], and aborts the rest.
pub struct TransportServer {
    local_addr: SocketAddr,
    shutdown: Option<watch::Sender<bool>>,
    task: Option<JoinHandle<()>>,
}

impl TransportServer {
    /// Binds the listener and starts the accept task. `registry` should be
    /// the local [`TcpTransport`]'s registry so attached groups are served as
    /// they attach and stop being served as they detach.
    pub async fn bind(
        address: &str,
        security: TransportSecurity,
        registry: TransportRegistry,
        config: TransportConfig,
    ) -> Result<Self, TransportError> {
        let listener = TcpListener::bind(address).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(Self::accept_loop(
            listener,
            security,
            registry,
            config,
            shutdown_rx,
        ));
        Ok(Self {
            local_addr,
            shutdown: Some(shutdown_tx),
            task: Some(task),
        })
    }

    /// The bound address (meaningful when binding port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Graceful shutdown: stop accepting, drain in-flight connections within
    /// the configured grace period, abort stragglers, and join the task.
    pub async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(true);
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }

    async fn accept_loop(
        listener: TcpListener,
        security: TransportSecurity,
        registry: TransportRegistry,
        config: TransportConfig,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let permits = Arc::new(Semaphore::new(config.max_connections));
        let mut connections: JoinSet<()> = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            match permits.clone().try_acquire_owned() {
                                Ok(permit) => {
                                    connections.spawn(Self::serve_connection(
                                        stream,
                                        security.clone(),
                                        registry.clone(),
                                        config.clone(),
                                        permit,
                                    ));
                                }
                                // Bounded, fail closed: the peer sees an
                                // immediate close and retries per openraft's
                                // policy.
                                Err(_) => drop(stream),
                            }
                        }
                        Err(_) => {
                            // Transient accept failure; avoid a hot loop.
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    }
                }
            }
        }
        let drain = async { while connections.join_next().await.is_some() {} };
        if tokio::time::timeout(config.shutdown_grace, drain)
            .await
            .is_err()
        {
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        }
    }

    async fn serve_connection(
        stream: TcpStream,
        security: TransportSecurity,
        registry: TransportRegistry,
        config: TransportConfig,
        _permit: OwnedSemaphorePermit,
    ) {
        let _ = stream.set_nodelay(true);
        match security {
            TransportSecurity::PlaintextForTesting => {
                Self::serve_stream(stream, &registry, &config).await;
            }
            TransportSecurity::Mtls(tls) => {
                let acceptor = tokio_rustls::TlsAcceptor::from(tls.server_config());
                let handshake =
                    tokio::time::timeout(config.handshake_timeout, acceptor.accept(stream)).await;
                let Ok(Ok(mut tls_stream)) = handshake else {
                    return;
                };
                let authenticated = {
                    let (_, connection) = tls_stream.get_ref();
                    verify_peer_identity(connection.peer_certificates(), &tls)
                };
                if authenticated.is_err() {
                    // Fail closed without a frame: an unauthenticated peer
                    // gets nothing parseable.
                    return;
                }
                Self::serve_stream(&mut tls_stream, &registry, &config).await;
            }
        }
    }

    /// Reads the single request frame, dispatches it, writes the single
    /// response frame. Any read failure closes the connection silently — an
    /// invalid frame is never answered with something that could be mistaken
    /// for a raft response (fail closed, spec section 4.10).
    async fn serve_stream<S: AsyncRead + AsyncWrite + Unpin>(
        mut stream: S,
        registry: &TransportRegistry,
        config: &TransportConfig,
    ) {
        // Snapshot chunks set the read bound; the type is unknown until the
        // frame is parsed.
        let frame =
            match read_frame(&mut stream, config.max_frame_bytes, config.snapshot_timeout).await {
                Ok(Some(frame)) => frame,
                Ok(None) | Err(_) => return,
            };
        let (response_type, response_payload) =
            Self::dispatch(registry, frame.message_type, &frame.payload).await;
        let _ = write_frame(
            &mut stream,
            response_type,
            response_payload,
            config.rpc_timeout,
        )
        .await;
    }

    /// Routes one request frame to the attached raft node it targets. Every
    /// failure path produces a [`RAFT_MSG_ERROR`] frame; unknown message
    /// types are rejected the same way (fail closed).
    async fn dispatch(
        registry: &TransportRegistry,
        message_type: u32,
        payload: &[u8],
    ) -> (u32, Vec<u8>) {
        match message_type {
            RAFT_MSG_APPEND_ENTRIES_REQUEST => {
                Self::dispatch_rpc(
                    registry,
                    RAFT_MSG_APPEND_ENTRIES_RESPONSE,
                    payload,
                    |raft, rpc| async move { raft.append_entries(rpc).await },
                )
                .await
            }
            RAFT_MSG_VOTE_REQUEST => {
                Self::dispatch_rpc(
                    registry,
                    RAFT_MSG_VOTE_RESPONSE,
                    payload,
                    |raft, rpc| async move { raft.vote(rpc).await },
                )
                .await
            }
            RAFT_MSG_INSTALL_SNAPSHOT_REQUEST => {
                Self::dispatch_rpc(
                    registry,
                    RAFT_MSG_INSTALL_SNAPSHOT_RESPONSE,
                    payload,
                    |raft, rpc| async move { raft.install_snapshot(rpc).await },
                )
                .await
            }
            RAFT_MSG_TRIGGER_ELECTION_REQUEST => {
                let request = serde_json::from_slice::<RpcPayload<()>>(payload);
                match request {
                    Err(error) => {
                        Self::error_frame(format!("undecodable request payload: {error}"))
                    }
                    Ok(request) => match registry.get(request.target) {
                        None => Self::error_frame(format!(
                            "node {} is not attached to this transport",
                            request.target
                        )),
                        Some(raft) => match raft.trigger().elect().await {
                            Ok(()) => (RAFT_MSG_TRIGGER_ELECTION_RESPONSE, Vec::new()),
                            Err(error) => Self::error_frame(format!("raft core error: {error}")),
                        },
                    },
                }
            }
            unknown => Self::error_frame(format!("unknown raft message type {unknown}")),
        }
    }

    /// Shared shape of the three raft RPC dispatch paths: decode the targeted
    /// payload, resolve the attached group, invoke the RPC, encode the
    /// response. Raft-core failures are reported as error frames; openraft
    /// leadership/term answers always travel inside the response payload
    /// itself, so they are never confused with transport failures.
    async fn dispatch_rpc<Req, Resp, E, F, Fut>(
        registry: &TransportRegistry,
        response_type: u32,
        payload: &[u8],
        call: F,
    ) -> (u32, Vec<u8>)
    where
        Req: for<'de> Deserialize<'de>,
        Resp: Serialize,
        E: fmt::Display,
        F: FnOnce(MongrelRaft, Req) -> Fut,
        Fut: std::future::Future<Output = Result<Resp, openraft::error::RaftError<RaftNodeId, E>>>,
    {
        let request = serde_json::from_slice::<RpcPayload<Req>>(payload);
        match request {
            Err(error) => Self::error_frame(format!("undecodable request payload: {error}")),
            Ok(request) => match registry.get(request.target) {
                None => Self::error_frame(format!(
                    "node {} is not attached to this transport",
                    request.target
                )),
                Some(raft) => match call(raft, request.rpc).await {
                    Ok(response) => match serde_json::to_vec(&response) {
                        Ok(bytes) => (response_type, bytes),
                        Err(error) => Self::error_frame(format!("unencodable response: {error}")),
                    },
                    Err(error) => Self::error_frame(format!("raft core error: {error}")),
                },
            },
        }
    }

    fn error_frame(message: String) -> (u32, Vec<u8>) {
        (
            RAFT_MSG_ERROR,
            serde_json::to_vec(&message).expect("string serialization is total"),
        )
    }
}

impl Drop for TransportServer {
    fn drop(&mut self) {
        // Dropping without `shutdown()`: stop accepting immediately.
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl fmt::Debug for TransportServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransportServer")
            .field("local_addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!("{FIXTURES}/{name}")).expect("fixture is checked in")
    }

    fn fixture_bytes(name: &str) -> Vec<u8> {
        std::fs::read(format!("{FIXTURES}/{name}")).expect("fixture is checked in")
    }

    fn node_id(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    fn node1_tls(allowed: &[NodeId]) -> TlsConfig {
        TlsConfig::from_pems(
            &fixture("ca.crt.pem"),
            &fixture("node1.crt.pem"),
            &fixture("node1.key.pem"),
            allowed,
        )
        .expect("fixture TLS config loads")
    }

    // -- node certificate naming -------------------------------------------

    #[test]
    fn node_cert_name_matches_the_fixture_scheme() {
        assert_eq!(
            node_cert_name(&node_id(1)),
            "node-01010101010101010101010101010101.mongreldb.cluster"
        );
        assert_eq!(
            node_cert_name(&node_id(0xab)),
            "node-abababababababababababababababab.mongreldb.cluster"
        );
    }

    // -- TlsConfig loading ---------------------------------------------------

    #[test]
    fn tls_config_loads_fixture_material() {
        let allowed = vec![node_id(1), node_id(2)];
        let tls = node1_tls(&allowed);
        assert!(tls.allows_identity(&node_cert_name(&node_id(1))));
        assert!(tls.allows_identity(&node_cert_name(&node_id(2))));
        assert!(!tls.allows_identity(&node_cert_name(&node_id(3))));
    }

    #[test]
    fn tls_config_rejects_unparsable_material() {
        let allowed = vec![node_id(1)];
        for (ca, cert, key) in [
            (
                "not pem",
                fixture("node1.crt.pem").as_str(),
                fixture("node1.key.pem").as_str(),
            ),
            (
                fixture("ca.crt.pem").as_str(),
                "not pem",
                fixture("node1.key.pem").as_str(),
            ),
            (
                fixture("ca.crt.pem").as_str(),
                fixture("node1.crt.pem").as_str(),
                "not pem",
            ),
        ] {
            assert!(
                TlsConfig::from_pems(ca, cert, key, &allowed).is_err(),
                "garbage material must fail closed"
            );
        }
        // An empty admission list admits no peers; fail closed.
        assert!(TlsConfig::from_pems(
            &fixture("ca.crt.pem"),
            &fixture("node1.crt.pem"),
            &fixture("node1.key.pem"),
            &[],
        )
        .is_err());
    }

    #[test]
    fn tls_config_loads_from_trust_config() {
        let trust = crate::bootstrap::TrustConfig::from_pems(
            fixture("ca.crt.pem"),
            fixture("node1.crt.pem"),
            fixture("node1.key.pem"),
            vec![node_id(1)],
        )
        .expect("trust config validates");
        let tls = TlsConfig::from_trust(&trust).expect("TLS config from trust");
        assert!(tls.allows_identity(&node_cert_name(&node_id(1))));
    }

    // -- DER identity extraction ---------------------------------------------

    fn pem_cert_der(name: &str) -> Vec<u8> {
        let pem = fixture_bytes(name);
        let mut reader = io::BufReader::new(pem.as_slice());
        let certificates: Vec<_> = rustls_pemfile::certs(&mut reader)
            .collect::<Result<_, _>>()
            .expect("parsable certificate");
        certificates
            .into_iter()
            .next()
            .expect("one certificate")
            .to_vec()
    }

    #[test]
    fn der_extracts_node_identity_from_cn_and_san() {
        let identities = der::certificate_identities(&pem_cert_der("node1.crt.pem")).unwrap();
        assert!(
            identities.contains(&node_cert_name(&node_id(1))),
            "expected the node identity in {identities:?}"
        );
        // CN and SAN agree; both are extracted.
        assert_eq!(identities.len(), 2, "{identities:?}");
    }

    #[test]
    fn der_extracts_ca_cn_without_san() {
        let identities = der::certificate_identities(&pem_cert_der("ca.crt.pem")).unwrap();
        assert_eq!(identities, vec!["mongreldb-test-ca".to_owned()]);
    }

    #[test]
    fn der_fails_closed_on_malformed_input() {
        assert!(der::certificate_identities(&[]).is_err());
        assert!(der::certificate_identities(b"junk").is_err());
        let mut truncated = pem_cert_der("node1.crt.pem");
        truncated.truncate(truncated.len() / 2);
        assert!(der::certificate_identities(&truncated).is_err());
        let mut garbage = pem_cert_der("node1.crt.pem");
        for byte in &mut garbage[10..20] {
            *byte ^= 0xff;
        }
        assert!(der::certificate_identities(&garbage).is_err());
    }

    // -- frame codec -----------------------------------------------------------

    const TEST_MAX_FRAME: usize = 1024;

    async fn write_and_read(
        bytes: &[u8],
        max_frame: usize,
    ) -> Result<Option<ProtocolEnvelope>, TransportError> {
        let (mut writer, mut reader) = tokio::io::duplex(bytes.len() + 64);
        writer.write_all(bytes).await.unwrap();
        drop(writer);
        read_frame(&mut reader, max_frame, Duration::from_secs(5)).await
    }

    #[tokio::test]
    async fn frame_round_trip() {
        let (mut writer, mut reader) = tokio::io::duplex(4096);
        write_frame(
            &mut writer,
            RAFT_MSG_VOTE_REQUEST,
            b"hello".to_vec(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let frame = read_frame(&mut reader, TEST_MAX_FRAME, Duration::from_secs(5))
            .await
            .unwrap()
            .expect("a frame");
        assert_eq!(frame.message_type, RAFT_MSG_VOTE_REQUEST);
        assert_eq!(frame.payload, b"hello");
        // A clean close at a frame boundary is Ok(None), not an error.
        drop(writer);
        assert_eq!(
            read_frame(&mut reader, TEST_MAX_FRAME, Duration::from_secs(5))
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn unknown_protocol_version_fails_closed() {
        let mut envelope = ProtocolEnvelope::new(1, vec![1, 2, 3]);
        envelope.protocol_version = 99;
        envelope.payload_crc32 =
            ProtocolEnvelope::checksum(envelope.protocol_version, 1, &envelope.payload);
        let error = write_and_read(&envelope.encode(), TEST_MAX_FRAME)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                TransportError::Envelope(EnvelopeError::UnsupportedVersion { found: 99, .. })
            ),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn oversize_length_prefix_fails_before_reading_the_payload() {
        // Only the header ever arrives; a decoder that trusted the length
        // would block or allocate, so FrameTooLarge proves the early check.
        let mut header = Vec::new();
        header.extend_from_slice(&1u32.to_le_bytes());
        header.extend_from_slice(&1u32.to_le_bytes());
        header.extend_from_slice(&(TEST_MAX_FRAME as u32 + 1).to_le_bytes());
        let error = write_and_read(&header, TEST_MAX_FRAME).await.unwrap_err();
        assert!(
            matches!(
                error,
                TransportError::FrameTooLarge {
                    actual,
                    limit
                } if actual == TEST_MAX_FRAME + 1 && limit == TEST_MAX_FRAME
            ),
            "unexpected error: {error}"
        );
        // A length beyond the protocol maximum fails just as early even when
        // the configured bound is raised.
        let mut header = Vec::new();
        header.extend_from_slice(&1u32.to_le_bytes());
        header.extend_from_slice(&1u32.to_le_bytes());
        header.extend_from_slice(&(MAX_MESSAGE_PAYLOAD_BYTES as u32 + 1).to_le_bytes());
        let error = write_and_read(&header, usize::MAX).await.unwrap_err();
        assert!(
            matches!(error, TransportError::FrameTooLarge { .. }),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn truncated_frame_fails_closed() {
        let envelope = ProtocolEnvelope::new(3, vec![9u8; 64]);
        let bytes = envelope.encode();
        // EOF mid-payload (writer dropped above) is an I/O error, not a frame.
        let error = write_and_read(&bytes[..bytes.len() - 5], TEST_MAX_FRAME)
            .await
            .unwrap_err();
        assert!(
            matches!(error, TransportError::Io(ref e) if e.kind() == io::ErrorKind::UnexpectedEof),
            "unexpected error: {error}"
        );
        // EOF mid-header is truncated too.
        let error = write_and_read(&bytes[..4], TEST_MAX_FRAME)
            .await
            .unwrap_err();
        assert!(
            matches!(error, TransportError::Io(ref e) if e.kind() == io::ErrorKind::UnexpectedEof),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn checksum_mismatch_fails_closed() {
        let envelope = ProtocolEnvelope::new(2, vec![7u8; 32]);
        let mut bytes = envelope.encode();
        bytes[HEADER_LEN] ^= 0x01;
        let error = write_and_read(&bytes, TEST_MAX_FRAME).await.unwrap_err();
        assert!(
            matches!(
                error,
                TransportError::Envelope(EnvelopeError::ChecksumMismatch)
            ),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn write_rejects_oversize_payload() {
        let (mut writer, _reader) = tokio::io::duplex(64);
        let error = write_frame(
            &mut writer,
            1,
            vec![0u8; MAX_MESSAGE_PAYLOAD_BYTES + 1],
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, TransportError::FrameTooLarge { .. }));
    }

    // -- registry --------------------------------------------------------------

    #[test]
    fn registry_attach_detach_lifecycle() {
        let registry = TransportRegistry::new();
        assert!(registry.is_empty());
        assert!(registry.get(42).is_none());
        registry.detach(42); // detach of an unknown node is a no-op
        assert!(registry.is_empty());
    }

    // -- peer directory ---------------------------------------------------------

    #[test]
    fn peer_directory_crud() {
        let transport = TcpTransport::new(
            TransportConfig::default(),
            TransportSecurity::PlaintextForTesting,
        );
        assert_eq!(transport.peer(1), None);
        transport.upsert_peer(1, PeerEndpoint::plaintext("127.0.0.1:9001"));
        assert_eq!(
            transport.peer(1),
            Some(PeerEndpoint::plaintext("127.0.0.1:9001"))
        );
        transport.upsert_peer(1, PeerEndpoint::plaintext("127.0.0.1:9002"));
        assert_eq!(
            transport.peer(1).map(|peer| peer.address),
            Some("127.0.0.1:9002".to_owned())
        );
        assert!(transport.remove_peer(1).is_some());
        assert_eq!(transport.peer(1), None);
    }

    #[test]
    fn mtls_endpoint_requires_an_identity() {
        let transport = TcpTransport::new(
            TransportConfig::default(),
            TransportSecurity::Mtls(node1_tls(&[node_id(1), node_id(2)])),
        );
        // An mTLS peer entry without a node identity fails closed before any
        // handshake is attempted.
        let error = super::peer_server_name(&PeerEndpoint::plaintext("127.0.0.1:1")).unwrap_err();
        assert!(
            matches!(error, TransportError::PeerAuthentication(_)),
            "unexpected error: {error}"
        );
        assert!(super::peer_server_name(&PeerEndpoint::mtls("127.0.0.1:1", node_id(2))).is_ok());
        drop(transport);
    }
}
