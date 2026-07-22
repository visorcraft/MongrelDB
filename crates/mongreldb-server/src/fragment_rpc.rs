//! Distributed fragment binding over the authenticated cluster transport.

use std::sync::Arc;

use mongreldb_cluster::network::{InternalRpcFuture, InternalRpcHandler};
use mongreldb_cluster::runtime::NodeInternalRpcClient;
use mongreldb_core::auth::Principal;
use mongreldb_core::query::{AiExecutionContext, RetrieverScore};
use mongreldb_core::Database;
use mongreldb_query::ai_retrieval::{
    AiRetrievalError, AiRpcClient, AiTabletExecutor, RemoteAiEndpoint, RemoteAiTransport,
    REMOTE_AI_SERVICE_ID,
};
use mongreldb_query::distributed::{
    CoreFragmentTableSource, DistributedError, DistributedResult, FragmentAuthorizationResolver,
    FragmentDatabaseProvider, FragmentExecutor, FragmentRpcClient, FragmentTransport,
    InMemoryFragmentExecutor, RemoteFragmentEndpoint, RemoteFragmentTransport,
    REMOTE_FRAGMENT_SERVICE_ID,
};
use mongreldb_types::ids::{NodeId, TabletId};

use crate::cluster_runtime::{ClusterRuntimeError, ClusterRuntimeHandle};

/// Resolves the local engine owner for a hosted tablet.
pub trait TabletDatabaseProvider: Send + Sync {
    /// Returns the engine handle only when this node currently hosts
    /// `tablet_id`.
    fn database(&self, tablet_id: TabletId) -> Option<Arc<Database>>;
}

/// Validates a server-issued AI authorization envelope.
pub trait AiAuthorizationResolver: Send + Sync {
    /// Resolves the exact catalog principal bound to the request. Returning
    /// `None` is valid only for a credentialless database.
    fn resolve(
        &self,
        database: &Database,
        context: &[u8],
    ) -> Result<Option<Principal>, AiRetrievalError>;
}

/// Real core-backed tablet AI executor.
///
/// It resolves the hosted tablet engine, validates the forwarded identity,
/// and calls the core's explicitly-principaled scored read. RLS, grants,
/// masks, work limits, cancellation, and exact rerank therefore execute at
/// the storage boundary.
pub struct DatabaseAiTabletExecutor {
    databases: Arc<dyn TabletDatabaseProvider>,
    authorization: Arc<dyn AiAuthorizationResolver>,
}

impl DatabaseAiTabletExecutor {
    /// Creates an engine-backed worker.
    pub fn new(
        databases: Arc<dyn TabletDatabaseProvider>,
        authorization: Arc<dyn AiAuthorizationResolver>,
    ) -> Self {
        Self {
            databases,
            authorization,
        }
    }
}

#[async_trait::async_trait]
impl AiTabletExecutor for DatabaseAiTabletExecutor {
    async fn retrieve(
        &self,
        request: &mongreldb_query::AiTabletQuery,
        control: mongreldb_core::ExecutionControl,
    ) -> Result<Vec<mongreldb_query::AiTabletHit>, AiRetrievalError> {
        let database = self.databases.database(request.tablet_id).ok_or_else(|| {
            AiRetrievalError::Transport(format!(
                "this node does not host tablet {}",
                request.tablet_id
            ))
        })?;
        let principal = self
            .authorization
            .resolve(&database, &request.authorization_context)?;
        let request = request.clone();
        tokio::task::spawn_blocking(move || {
            let context = AiExecutionContext::with_control(
                control,
                request.budget.max_local_candidates,
                request.budget.candidate_ceiling,
            );
            let weight_by_name: std::collections::HashMap<String, f64> = request
                .search
                .retrievers
                .iter()
                .map(|named| (named.name.clone(), named.weight))
                .collect();
            database
                .search_for_principal_with_context(
                    &request.table,
                    &request.search,
                    principal.as_ref(),
                    Some(&context),
                )
                .map_err(|error| AiRetrievalError::Transport(error.to_string()))?
                .into_iter()
                .map(|hit| {
                    let local_rank = u32::try_from(hit.final_rank).map_err(|_| {
                        AiRetrievalError::Protocol(format!(
                            "local rank {} exceeds the wire range",
                            hit.final_rank
                        ))
                    })?;
                    // Emit per-retriever contributions so the coordinator can
                    // run global hybrid fusion (P0.8) instead of single-rank RRF.
                    let contributions = hit
                        .components
                        .iter()
                        .map(|component| {
                            let weight = weight_by_name
                                .get(component.retriever_name.as_ref())
                                .copied()
                                .unwrap_or(1.0);
                            let component_rank =
                                u32::try_from(component.rank).unwrap_or(u32::MAX).max(1);
                            mongreldb_query::LocalRetrieverContribution {
                                tablet_id: request.tablet_id,
                                row_id: hit.row_id,
                                retriever_id: component.retriever_name.to_string(),
                                retriever_kind: None,
                                local_rank: component_rank,
                                raw_score: retriever_score_higher_better(component.raw_score),
                                upper_bound_after: None,
                                rls_visible: true,
                                weight,
                            }
                        })
                        .collect();
                    Ok(mongreldb_query::AiTabletHit {
                        candidate: mongreldb_query::LocalCandidate {
                            tablet_id: request.tablet_id,
                            row_id: hit.row_id,
                            score: hit.final_score,
                            local_rank,
                            rls_visible: true,
                        },
                        cells: hit.cells,
                        exact_rerank_score: hit.exact_rerank_score,
                        consistency: None,
                        contributions,
                        metadata: mongreldb_query::TabletAiResponseMetadata::default(),
                    })
                })
                .collect()
        })
        .await
        .map_err(|error| AiRetrievalError::Transport(format!("AI worker task failed: {error}")))?
    }
}

/// Convert core retriever scores to higher-is-better raw scores for hybrid merge.
fn retriever_score_higher_better(score: RetrieverScore) -> f64 {
    match score {
        // ANN distances are lower-is-better; invert so MaxScore fusion works.
        RetrieverScore::AnnHammingDistance(distance) => -(f64::from(distance)),
        RetrieverScore::AnnCosineDistance(distance) => -f64::from(distance),
        RetrieverScore::SparseDotProduct(score) => score,
        RetrieverScore::MinHashEstimatedJaccard(jaccard) => f64::from(jaccard),
    }
}

struct ClusterFragmentHandler {
    endpoint: Arc<RemoteFragmentEndpoint>,
}

struct ClusterAiHandler {
    endpoint: Arc<RemoteAiEndpoint>,
}

impl InternalRpcHandler for ClusterAiHandler {
    fn handle<'a>(&'a self, body: &'a [u8]) -> InternalRpcFuture<'a> {
        Box::pin(async move {
            self.endpoint
                .handle(body)
                .await
                .map_err(|error| error.to_string())
        })
    }
}

impl InternalRpcHandler for ClusterFragmentHandler {
    fn handle<'a>(&'a self, body: &'a [u8]) -> InternalRpcFuture<'a> {
        Box::pin(async move {
            self.endpoint
                .handle(body)
                .await
                .map_err(|error| error.to_string())
        })
    }
}

/// Installs a fragment worker on a live cluster node.
///
/// The returned endpoint exposes active-cursor counts for operations and
/// tests. Calls arrive only after cluster mTLS and admitted-node validation.
pub async fn install_fragment_worker(
    runtime: &ClusterRuntimeHandle,
    executor: Arc<dyn FragmentExecutor>,
) -> Result<Arc<RemoteFragmentEndpoint>, ClusterRuntimeError> {
    let endpoint = Arc::new(RemoteFragmentEndpoint::new(executor));
    runtime
        .attach_internal_rpc_handler(
            REMOTE_FRAGMENT_SERVICE_ID,
            Arc::new(ClusterFragmentHandler {
                endpoint: Arc::clone(&endpoint),
            }),
        )
        .await?;
    Ok(endpoint)
}

/// Installs the shipped core-MVCC fragment worker.
pub async fn install_database_fragment_worker(
    runtime: &ClusterRuntimeHandle,
    databases: Arc<dyn FragmentDatabaseProvider>,
    authorization: Arc<dyn FragmentAuthorizationResolver>,
) -> Result<Arc<RemoteFragmentEndpoint>, ClusterRuntimeError> {
    let source = Arc::new(CoreFragmentTableSource::new(databases, authorization));
    install_fragment_worker(
        runtime,
        Arc::new(InMemoryFragmentExecutor::from_source(source)),
    )
    .await
}

/// Installs a distributed-AI worker on the same authenticated cluster
/// transport under its own stable service id.
pub async fn install_ai_worker(
    runtime: &ClusterRuntimeHandle,
    executor: Arc<dyn AiTabletExecutor>,
) -> Result<Arc<RemoteAiEndpoint>, ClusterRuntimeError> {
    let endpoint = Arc::new(RemoteAiEndpoint::new(executor));
    runtime
        .attach_internal_rpc_handler(
            REMOTE_AI_SERVICE_ID,
            Arc::new(ClusterAiHandler {
                endpoint: Arc::clone(&endpoint),
            }),
        )
        .await?;
    Ok(endpoint)
}

/// Runtime-backed tablet database lookup for production fragment/AI workers.
///
/// Resolves applied `ClusterReplica` cores only for tablets this node hosts;
/// never opens a peer standalone user database (P0.2 dual-root refusal).
struct RuntimeTabletDatabases {
    runtime: ClusterRuntimeHandle,
}

impl FragmentDatabaseProvider for RuntimeTabletDatabases {
    fn database(&self, tablet: TabletId) -> Option<Arc<Database>> {
        self.runtime.tablet_database_try(tablet)
    }
}

impl TabletDatabaseProvider for RuntimeTabletDatabases {
    fn database(&self, tablet_id: TabletId) -> Option<Arc<Database>> {
        self.runtime.tablet_database_try(tablet_id)
    }
}

/// Authorization envelope for tablet workers: empty context is credentialless;
/// non-empty contexts must match a catalog principal username (UTF-8).
struct RuntimeAuthorizationResolver;

impl FragmentAuthorizationResolver for RuntimeAuthorizationResolver {
    fn resolve(&self, database: &Database, context: &[u8]) -> DistributedResult<Option<Principal>> {
        resolve_worker_principal(database, context).map_err(DistributedError::RemoteProtocol)
    }
}

impl AiAuthorizationResolver for RuntimeAuthorizationResolver {
    fn resolve(
        &self,
        database: &Database,
        context: &[u8],
    ) -> Result<Option<Principal>, AiRetrievalError> {
        resolve_worker_principal(database, context).map_err(AiRetrievalError::Transport)
    }
}

fn resolve_worker_principal(
    database: &Database,
    context: &[u8],
) -> Result<Option<Principal>, String> {
    if context.is_empty() {
        if database.require_auth_enabled() {
            return Err("authorization context required for credentialed tablet core".into());
        }
        return Ok(None);
    }
    let username = std::str::from_utf8(context)
        .map_err(|_| "authorization context is not valid UTF-8".to_owned())?;
    database
        .resolve_principal(username)
        .map(Some)
        .ok_or_else(|| format!("authorization context principal `{username}` is unknown"))
}

/// Production cluster start: install fragment + AI workers on the authenticated
/// internal RPC transport (P0.2 residual #9 / audit §1 item 9).
///
/// Workers resolve hosted tablet engines through [`ClusterRuntimeHandle`];
/// they never open a standalone user-database root alongside the node runtime.
pub async fn install_production_cluster_workers(
    runtime: &ClusterRuntimeHandle,
) -> Result<(Arc<RemoteFragmentEndpoint>, Arc<RemoteAiEndpoint>), ClusterRuntimeError> {
    let databases = Arc::new(RuntimeTabletDatabases {
        runtime: runtime.clone(),
    });
    let authorization = Arc::new(RuntimeAuthorizationResolver);
    let fragment = install_database_fragment_worker(
        runtime,
        Arc::clone(&databases) as Arc<dyn FragmentDatabaseProvider>,
        Arc::clone(&authorization) as Arc<dyn FragmentAuthorizationResolver>,
    )
    .await?;
    let ai = install_ai_worker(
        runtime,
        Arc::new(DatabaseAiTabletExecutor::new(
            databases as Arc<dyn TabletDatabaseProvider>,
            authorization as Arc<dyn AiAuthorizationResolver>,
        )),
    )
    .await?;
    Ok((fragment, ai))
}

/// Builds a fail-closed fragment transport from an already bound gateway
/// plan. Every tablet must have a preferred or fallback replica endpoint.
pub async fn fragment_transport_for_bound_plan(
    runtime: &ClusterRuntimeHandle,
    plan: &mongreldb_cluster::gateway::BoundPlan,
) -> Result<Arc<dyn FragmentTransport>, ClusterRuntimeError> {
    let rpc = runtime.internal_rpc_client().await?;
    let mut routes = std::collections::BTreeMap::<TabletId, NodeId>::new();
    for fragment in &plan.fragments {
        for target in &fragment.targets {
            let endpoint = target
                .preferred_endpoint
                .as_ref()
                .or_else(|| target.endpoints.first())
                .ok_or_else(|| {
                    ClusterRuntimeError::Config(format!(
                        "tablet {} has no replica endpoint",
                        target.tablet_id
                    ))
                })?;
            if let Some(prior) = routes.insert(target.tablet_id, endpoint.node_id) {
                if prior != endpoint.node_id {
                    return Err(ClusterRuntimeError::Config(format!(
                        "tablet {} is bound to conflicting nodes {prior} and {}",
                        target.tablet_id, endpoint.node_id
                    )));
                }
            }
        }
    }
    let mut transport = RemoteFragmentTransport::routed();
    for (tablet, node) in routes {
        transport = transport.with_client(
            tablet,
            Arc::new(ClusterFragmentClient::new(rpc.clone(), node)),
        );
    }
    Ok(Arc::new(transport))
}

/// Builds a fail-closed distributed-AI transport from tablet-to-node routes.
pub async fn ai_transport_for_routes(
    runtime: &ClusterRuntimeHandle,
    routes: impl IntoIterator<Item = (TabletId, NodeId)>,
) -> Result<RemoteAiTransport, ClusterRuntimeError> {
    let rpc = runtime.internal_rpc_client().await?;
    let mut transport = RemoteAiTransport::routed();
    for (tablet, node) in routes {
        transport =
            transport.with_client(tablet, Arc::new(ClusterAiClient::new(rpc.clone(), node)));
    }
    Ok(transport)
}

/// Query-side carrier for one target cluster node.
pub struct ClusterFragmentClient {
    rpc: NodeInternalRpcClient,
    target: NodeId,
}

impl ClusterFragmentClient {
    /// Creates a carrier from a cloneable node-internal client.
    pub fn new(rpc: NodeInternalRpcClient, target: NodeId) -> Self {
        Self { rpc, target }
    }

    /// Creates a carrier from a live server runtime.
    pub async fn from_runtime(
        runtime: &ClusterRuntimeHandle,
        target: NodeId,
    ) -> Result<Self, ClusterRuntimeError> {
        Ok(Self::new(runtime.internal_rpc_client().await?, target))
    }
}

#[async_trait::async_trait]
impl FragmentRpcClient for ClusterFragmentClient {
    async fn call(&self, request: Vec<u8>) -> DistributedResult<Vec<u8>> {
        self.rpc
            .call(self.target, REMOTE_FRAGMENT_SERVICE_ID, request)
            .await
            .map_err(|error| DistributedError::RemoteTransport(error.to_string()))
    }
}

/// Distributed-AI carrier for one target node.
pub struct ClusterAiClient {
    rpc: NodeInternalRpcClient,
    target: NodeId,
}

impl ClusterAiClient {
    /// Creates a carrier from a cloneable node-internal client.
    pub fn new(rpc: NodeInternalRpcClient, target: NodeId) -> Self {
        Self { rpc, target }
    }

    /// Creates a carrier from a live server runtime.
    pub async fn from_runtime(
        runtime: &ClusterRuntimeHandle,
        target: NodeId,
    ) -> Result<Self, ClusterRuntimeError> {
        Ok(Self::new(runtime.internal_rpc_client().await?, target))
    }
}

#[async_trait::async_trait]
impl AiRpcClient for ClusterAiClient {
    async fn call(&self, request: Vec<u8>) -> Result<Vec<u8>, AiRetrievalError> {
        self.rpc
            .call(self.target, REMOTE_AI_SERVICE_ID, request)
            .await
            .map_err(|error| AiRetrievalError::Transport(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use mongreldb_cluster::bootstrap::{cluster_init, InitRequest, TrustConfig};
    use mongreldb_cluster::gateway::{BoundFragment, BoundPlan, BoundTabletTarget};
    use mongreldb_cluster::node::{Locality, NodeCapacity, NodeIdentity};
    use mongreldb_cluster::routing::Endpoint;
    use mongreldb_core::query::{Fusion, SearchRequest};
    use mongreldb_core::{ExecutionControl, RowId, Value};
    use mongreldb_query::ai_retrieval::{
        AiFanoutRequest, AiTabletHit, AiTabletQuery, AiWorkBudget, FusionMethod, LocalCandidate,
    };
    use mongreldb_query::distributed::{
        Coordinator, DistributedPlan, ExchangeDescriptor, ExchangeKind, FragmentAssignment,
        FragmentOperator, InMemoryFragmentExecutor, InMemoryTableStore, PlanFragment,
    };
    use mongreldb_query::SqlQueryRegistry;
    use mongreldb_types::ids::{MetadataVersion, QueryId, RaftGroupId, TabletId};
    use tempfile::tempdir;

    const CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nY2E=\n-----END CERTIFICATE-----\n";
    const CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nbm9kZQ==\n-----END CERTIFICATE-----\n";
    const KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nc2VjcmV0\n-----END PRIVATE KEY-----\n";

    fn bootstrap(data: &Path) -> NodeIdentity {
        let mut counter = 0u64;
        let mut csprng = |buffer: &mut [u8]| {
            for chunk in buffer.chunks_mut(8) {
                counter += 1;
                let bytes = counter.to_le_bytes();
                chunk.copy_from_slice(&bytes[..chunk.len()]);
            }
            Ok(())
        };
        let identity = NodeIdentity::load_or_create(data, &mut csprng).unwrap();
        cluster_init(
            data,
            &InitRequest {
                rpc_address: "127.0.0.1:0".to_owned(),
                locality: Locality::default(),
                capacity: NodeCapacity::default(),
                trust: TrustConfig::from_pems(
                    CA_PEM.to_owned(),
                    CERT_PEM.to_owned(),
                    KEY_PEM.to_owned(),
                    vec![identity.node_id],
                )
                .unwrap(),
            },
            &mut csprng,
        )
        .unwrap()
        .identity
    }

    fn batch(values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(values)) as ArrayRef],
        )
        .unwrap()
    }

    struct TestAiExecutor;

    #[async_trait::async_trait]
    impl AiTabletExecutor for TestAiExecutor {
        async fn retrieve(
            &self,
            request: &AiTabletQuery,
            control: ExecutionControl,
        ) -> Result<Vec<AiTabletHit>, AiRetrievalError> {
            control
                .checkpoint()
                .map_err(|_| AiRetrievalError::Cancelled(control.reason()))?;
            if request.authorization_context != b"signed-user" {
                return Err(AiRetrievalError::Transport(
                    "authorization context rejected".to_owned(),
                ));
            }
            Ok(vec![AiTabletHit {
                candidate: LocalCandidate {
                    tablet_id: request.tablet_id,
                    row_id: RowId(44),
                    score: 0.75,
                    local_rank: 1,
                    rls_visible: true,
                },
                cells: vec![(1, Value::Int64(44))],
                exact_rerank_score: Some(0.95),
                consistency: None,
                contributions: Vec::new(),
                metadata: Default::default(),
            }])
        }
    }

    #[tokio::test]
    async fn gateway_fragment_crosses_real_cluster_tcp_and_arrow_ipc() {
        let data = tempdir().unwrap();
        let identity = bootstrap(data.path());
        let listen = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().to_string()
        };
        let runtime = ClusterRuntimeHandle::start(crate::cluster_runtime::ClusterRuntimeOptions {
            node_data: data.path().to_path_buf(),
            rpc_listen: listen.clone(),
            plaintext_test: true,
            fast_timing: true,
        })
        .await
        .unwrap();

        let tablet = TabletId::from_bytes([7; 16]);
        let store = Arc::new(InMemoryTableStore::new());
        store.insert("events", tablet, batch(vec![11, 22, 33]));
        let executor: Arc<dyn FragmentExecutor> = Arc::new(InMemoryFragmentExecutor::new(store));
        let endpoint = install_fragment_worker(&runtime, executor).await.unwrap();
        let transport = fragment_transport_for_bound_plan(
            &runtime,
            &BoundPlan {
                query_id: QueryId::new_random(),
                metadata_version: MetadataVersion::new(1),
                fragments: vec![BoundFragment {
                    fragment_id: 0,
                    targets: vec![BoundTabletTarget {
                        tablet_id: tablet,
                        raft_group_id: RaftGroupId::from_bytes([8; 16]),
                        generation: 1,
                        preferred_endpoint: Some(Endpoint {
                            node_id: identity.node_id,
                            address: listen,
                        }),
                        endpoints: Vec::new(),
                    }],
                }],
            },
        )
        .await
        .unwrap();
        let coordinator = Coordinator::new(transport, Arc::new(SqlQueryRegistry::default()));
        let plan = DistributedPlan {
            query_id: QueryId::new_random(),
            metadata_version: MetadataVersion::new(1),
            fragments: vec![
                PlanFragment {
                    fragment_id: 0,
                    assignment: FragmentAssignment::Tablet(tablet),
                    operators: vec![
                        FragmentOperator::TabletScan {
                            table: "events".to_owned(),
                            predicate: None,
                            projection: vec![],
                        },
                        FragmentOperator::RemoteExchangeSink { exchange: 0 },
                    ],
                    estimated_rows: 3,
                    estimated_bytes: 24,
                    max_spill_bytes: 0,
                },
                PlanFragment {
                    fragment_id: 1,
                    assignment: FragmentAssignment::Coordinator,
                    operators: vec![
                        FragmentOperator::RemoteExchangeSource { exchange: 0 },
                        FragmentOperator::DistributedLimit { limit: 10 },
                    ],
                    estimated_rows: 3,
                    estimated_bytes: 24,
                    max_spill_bytes: 0,
                },
            ],
            exchanges: vec![ExchangeDescriptor {
                exchange_id: 0,
                producer: 0,
                consumer: 1,
                kind: ExchangeKind::Merge,
                schema_fingerprint: 0,
            }],
        };

        let output = coordinator.execute(&plan).await.unwrap();
        let values = output
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![11, 22, 33]);
        assert_eq!(endpoint.active_executions(), 0);

        let ai_endpoint = install_ai_worker(&runtime, Arc::new(TestAiExecutor))
            .await
            .unwrap();
        let ai = ai_transport_for_routes(&runtime, [(tablet, identity.node_id)])
            .await
            .unwrap();
        let ai_result = ai
            .retrieve(AiFanoutRequest {
                query_id: QueryId::new_random(),
                tablets: &[tablet],
                table: "events",
                search: &SearchRequest {
                    must: Vec::new(),
                    retrievers: Vec::new(),
                    fusion: Fusion::ReciprocalRank { constant: 60 },
                    rerank: None,
                    limit: 1,
                    projection: Some(vec![1]),
                },
                authorization_context: b"signed-user",
                fusion: FusionMethod::default(),
                overfetch_factor: 2.0,
                budget: &AiWorkBudget {
                    candidate_ceiling: 1,
                    max_local_candidates: 2,
                    ..AiWorkBudget::default()
                },
                control: &ExecutionControl::new(None),
            })
            .await
            .unwrap();
        assert_eq!(ai_result.hits[0].exact_rerank_score, Some(0.95));
        assert_eq!(ai_result.hits[0].cells, vec![(1, Value::Int64(44))]);
        assert_eq!(ai_endpoint.active_executions(), 0);

        runtime.shutdown().await.unwrap();
    }
}
