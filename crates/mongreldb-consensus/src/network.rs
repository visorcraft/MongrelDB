//! Consensus network adapter: openraft RPCs over a pluggable transport
//! (spec section 11.2; ADR-0004 wraps simulation/fault injection around the
//! adapter's network traits, never around openraft internals).
//!
//! [`RaftTransport`] carries the three raft RPCs plus two group-management
//! hooks (peer registration and an election trigger used for best-effort
//! leadership transfer). The real RPC transport lands in a later Stage 2
//! wave; this module ships [`InMemoryTransport`], which delivers RPCs between
//! groups in one process and provides per-link drop/delay policies for
//! partition testing.
//!
//! # Fault hooks (FND-006)
//!
//! Every send passes `raft.net.append_entries.before/after`,
//! `raft.net.vote.before/after`, and
//! `raft.net.install_snapshot.before/after` through `mongreldb_fault`, so
//! tests can fail or observe RPC boundaries globally in addition to the
//! per-link policies.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;

use crate::identity::{MongrelRaft, MongrelRaftConfig, RaftNodeId};

/// RPC error types (openraft-native, so remote raft errors keep their type).
pub type AppendRpcError = RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>;
/// Vote RPC error type.
pub type VoteRpcError = RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>;
/// Install-snapshot RPC error type.
pub type SnapshotRpcError =
    RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, InstallSnapshotError>>;

/// Transport-level failures (management operations and link policies; RPC
/// delivery itself reports openraft-native [`RPCError`]s).
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The target node is not registered with the transport.
    #[error("no route to node {0}")]
    NoRoute(RaftNodeId),
    /// The link policy drops traffic between the two nodes.
    #[error("link {from} -> {to} is down")]
    LinkDown {
        /// Sender.
        from: RaftNodeId,
        /// Receiver.
        to: RaftNodeId,
    },
    /// An injected fault fired at a network hook.
    #[error("injected network fault: {0}")]
    Fault(String),
    /// The transport does not implement this operation.
    #[error("unsupported operation: {0}")]
    Unsupported(&'static str),
}

/// Carries raft RPCs between group members.
///
/// Implementations must be cheap to clone behind an `Arc`; the group holds
/// one and hands clones to openraft's network factory.
pub trait RaftTransport: Send + Sync + 'static {
    /// Delivers an AppendEntries RPC from `from` to `target`.
    fn append_entries(
        &self,
        from: RaftNodeId,
        target: RaftNodeId,
        rpc: AppendEntriesRequest<MongrelRaftConfig>,
    ) -> impl Future<Output = Result<AppendEntriesResponse<RaftNodeId>, AppendRpcError>> + Send;

    /// Delivers a RequestVote RPC from `from` to `target`.
    fn vote(
        &self,
        from: RaftNodeId,
        target: RaftNodeId,
        rpc: VoteRequest<RaftNodeId>,
    ) -> impl Future<Output = Result<VoteResponse<RaftNodeId>, VoteRpcError>> + Send;

    /// Delivers an InstallSnapshot chunk RPC from `from` to `target`.
    fn install_snapshot(
        &self,
        from: RaftNodeId,
        target: RaftNodeId,
        rpc: InstallSnapshotRequest<MongrelRaftConfig>,
    ) -> impl Future<Output = Result<InstallSnapshotResponse<RaftNodeId>, SnapshotRpcError>> + Send;

    /// Registers a running raft node (in-process transports route to it;
    /// RPC transports resolve peers by membership address and ignore this).
    fn attach(&self, _node_id: RaftNodeId, _raft: MongrelRaft) {}

    /// Deregisters a raft node (group shutdown).
    fn detach(&self, _node_id: RaftNodeId) {}

    /// Asks `target` to start an election now; used for best-effort
    /// leadership transfer. The default reports [`TransportError::Unsupported`].
    fn trigger_election(
        &self,
        _target: RaftNodeId,
    ) -> impl Future<Output = Result<(), TransportError>> + Send {
        async { Err(TransportError::Unsupported("election trigger")) }
    }
}

// ---------------------------------------------------------------------------
// TransportNetworkFactory / TransportNetwork (openraft-facing)
// ---------------------------------------------------------------------------

/// openraft network factory bound to one local node and one transport.
pub struct TransportNetworkFactory<T: RaftTransport> {
    transport: Arc<T>,
    local: RaftNodeId,
}

impl<T: RaftTransport> TransportNetworkFactory<T> {
    /// Creates a factory for `local` over `transport`.
    pub fn new(transport: Arc<T>, local: RaftNodeId) -> Self {
        Self { transport, local }
    }
}

impl<T: RaftTransport> RaftNetworkFactory<MongrelRaftConfig> for TransportNetworkFactory<T> {
    type Network = TransportNetwork<T>;

    async fn new_client(&mut self, target: RaftNodeId, _node: &BasicNode) -> Self::Network {
        TransportNetwork {
            transport: self.transport.clone(),
            from: self.local,
            target,
        }
    }
}

/// openraft network instance carrying RPCs from one node to one target.
pub struct TransportNetwork<T: RaftTransport> {
    transport: Arc<T>,
    from: RaftNodeId,
    target: RaftNodeId,
}

impl<T: RaftTransport> RaftNetwork<MongrelRaftConfig> for TransportNetwork<T> {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MongrelRaftConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<AppendEntriesResponse<RaftNodeId>, AppendRpcError> {
        self.transport
            .append_entries(self.from, self.target, rpc)
            .await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<MongrelRaftConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<InstallSnapshotResponse<RaftNodeId>, SnapshotRpcError> {
        self.transport
            .install_snapshot(self.from, self.target, rpc)
            .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<RaftNodeId>,
        _option: openraft::network::RPCOption,
    ) -> Result<VoteResponse<RaftNodeId>, VoteRpcError> {
        self.transport.vote(self.from, self.target, rpc).await
    }
}

// ---------------------------------------------------------------------------
// InMemoryTransport
// ---------------------------------------------------------------------------

/// Per-link delivery policy for [`InMemoryTransport`].
#[derive(Debug, Clone, Default)]
pub struct LinkPolicy {
    /// Delay every RPC on this link (delivery still occurs).
    pub delay: Duration,
    /// Drop every RPC on this link (the sender sees "unreachable").
    pub drop: bool,
}

#[derive(Default)]
struct InMemoryState {
    nodes: HashMap<RaftNodeId, MongrelRaft>,
    /// (from, to) -> policy; links without an entry deliver immediately.
    links: HashMap<(RaftNodeId, RaftNodeId), LinkPolicy>,
}

/// In-process transport for tests and deterministic simulation: RPCs are
/// delivered directly to the target's `Raft` handle. Per-link drop/delay
/// policies model partitions and slow links without sleeps in test code.
#[derive(Clone, Default)]
pub struct InMemoryTransport {
    state: Arc<Mutex<InMemoryState>>,
}

impl InMemoryTransport {
    /// Creates an empty transport.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, InMemoryState> {
        self.state.lock().expect("transport lock poisoned")
    }

    fn policy(&self, from: RaftNodeId, to: RaftNodeId) -> LinkPolicy {
        self.lock()
            .links
            .get(&(from, to))
            .cloned()
            .unwrap_or_default()
    }

    /// Sets the policy for one directed link.
    pub fn set_link(&self, from: RaftNodeId, to: RaftNodeId, policy: LinkPolicy) {
        self.lock().links.insert((from, to), policy);
    }

    /// Clears every link policy (heals all partitions and delays).
    pub fn heal(&self) {
        self.lock().links.clear();
    }

    /// Drops all traffic between the two node sets, both directions.
    pub fn partition(&self, side_a: &[RaftNodeId], side_b: &[RaftNodeId]) {
        let down = LinkPolicy {
            delay: Duration::ZERO,
            drop: true,
        };
        for &a in side_a {
            for &b in side_b {
                self.set_link(a, b, down.clone());
                self.set_link(b, a, down.clone());
            }
        }
    }

    fn down_error(&self, from: RaftNodeId, to: RaftNodeId) -> Unreachable {
        Unreachable::new(&TransportError::LinkDown { from, to })
    }

    fn no_route_error(&self, to: RaftNodeId) -> Unreachable {
        Unreachable::new(&TransportError::NoRoute(to))
    }

    fn lookup(&self, target: RaftNodeId) -> Option<MongrelRaft> {
        self.lock().nodes.get(&target).cloned()
    }
}

impl RaftTransport for InMemoryTransport {
    async fn append_entries(
        &self,
        from: RaftNodeId,
        target: RaftNodeId,
        rpc: AppendEntriesRequest<MongrelRaftConfig>,
    ) -> Result<AppendEntriesResponse<RaftNodeId>, AppendRpcError> {
        if let Err(fault) = mongreldb_fault::inject("raft.net.append_entries.before") {
            return Err(RPCError::Network(NetworkError::new(&fault)));
        }
        let policy = self.policy(from, target);
        if policy.drop {
            return Err(RPCError::Unreachable(self.down_error(from, target)));
        }
        if policy.delay > Duration::ZERO {
            tokio::time::sleep(policy.delay).await;
        }
        let Some(raft) = self.lookup(target) else {
            return Err(RPCError::Unreachable(self.no_route_error(target)));
        };
        let result = raft
            .append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(target, e)));
        if let Err(fault) = mongreldb_fault::inject("raft.net.append_entries.after") {
            return Err(RPCError::Network(NetworkError::new(&fault)));
        }
        result
    }

    async fn vote(
        &self,
        from: RaftNodeId,
        target: RaftNodeId,
        rpc: VoteRequest<RaftNodeId>,
    ) -> Result<VoteResponse<RaftNodeId>, VoteRpcError> {
        if let Err(fault) = mongreldb_fault::inject("raft.net.vote.before") {
            return Err(RPCError::Network(NetworkError::new(&fault)));
        }
        let policy = self.policy(from, target);
        if policy.drop {
            return Err(RPCError::Unreachable(self.down_error(from, target)));
        }
        if policy.delay > Duration::ZERO {
            tokio::time::sleep(policy.delay).await;
        }
        let Some(raft) = self.lookup(target) else {
            return Err(RPCError::Unreachable(self.no_route_error(target)));
        };
        let result = raft
            .vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(target, e)));
        if let Err(fault) = mongreldb_fault::inject("raft.net.vote.after") {
            return Err(RPCError::Network(NetworkError::new(&fault)));
        }
        result
    }

    async fn install_snapshot(
        &self,
        from: RaftNodeId,
        target: RaftNodeId,
        rpc: InstallSnapshotRequest<MongrelRaftConfig>,
    ) -> Result<InstallSnapshotResponse<RaftNodeId>, SnapshotRpcError> {
        if let Err(fault) = mongreldb_fault::inject("raft.net.install_snapshot.before") {
            return Err(RPCError::Network(NetworkError::new(&fault)));
        }
        let policy = self.policy(from, target);
        if policy.drop {
            return Err(RPCError::Unreachable(self.down_error(from, target)));
        }
        if policy.delay > Duration::ZERO {
            tokio::time::sleep(policy.delay).await;
        }
        let Some(raft) = self.lookup(target) else {
            return Err(RPCError::Unreachable(self.no_route_error(target)));
        };
        let result = raft
            .install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(target, e)));
        if let Err(fault) = mongreldb_fault::inject("raft.net.install_snapshot.after") {
            return Err(RPCError::Network(NetworkError::new(&fault)));
        }
        result
    }

    fn attach(&self, node_id: RaftNodeId, raft: MongrelRaft) {
        self.lock().nodes.insert(node_id, raft);
    }

    fn detach(&self, node_id: RaftNodeId) {
        self.lock().nodes.remove(&node_id);
    }

    async fn trigger_election(&self, target: RaftNodeId) -> Result<(), TransportError> {
        let Some(raft) = self.lookup(target) else {
            return Err(TransportError::NoRoute(target));
        };
        raft.trigger()
            .elect()
            .await
            .map_err(|e| TransportError::Fault(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_policies_are_per_directed_link() {
        let transport = InMemoryTransport::new();
        assert!(!transport.policy(1, 2).drop);
        transport.set_link(
            1,
            2,
            LinkPolicy {
                delay: Duration::from_millis(5),
                drop: true,
            },
        );
        assert!(transport.policy(1, 2).drop);
        assert_eq!(transport.policy(1, 2).delay, Duration::from_millis(5));
        // The reverse direction is unaffected.
        assert!(!transport.policy(2, 1).drop);
        transport.heal();
        assert!(!transport.policy(1, 2).drop);
    }

    #[test]
    fn partition_drops_both_directions() {
        let transport = InMemoryTransport::new();
        transport.partition(&[1], &[2, 3]);
        assert!(transport.policy(1, 2).drop);
        assert!(transport.policy(2, 1).drop);
        assert!(transport.policy(1, 3).drop);
        assert!(transport.policy(3, 1).drop);
        assert!(!transport.policy(2, 3).drop);
        transport.heal();
        assert!(!transport.policy(1, 2).drop);
    }
}
