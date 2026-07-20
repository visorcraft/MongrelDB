//! Distributed AI retrieval (spec section 13.4, Stage 4D).
//!
//! Per-tablet retrievers apply RLS **before** local top-k, return bounded
//! candidates with raw scores, and the coordinator merges with deterministic
//! RRF (or configured fusion). Tie-break: final score desc, tablet id asc,
//! RowId asc. Adaptive ANN candidate counts with global work budget,
//! candidate ceiling, memory, and deadline. [`RemoteAiTransport`] fans typed
//! searches over authenticated node-internal RPC routes; workers validate the
//! forwarded authorization envelope and apply RLS before local top-k.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use bincode::Options;
use futures::stream::{FuturesUnordered, StreamExt};
use mongreldb_core::query::{Rerank, Retriever, SearchRequest};
use mongreldb_core::{CancellationReason, ExecutionControl, RowId, Value};
use mongreldb_types::ids::{QueryId, TabletId};
use serde::{Deserialize, Serialize};

/// One candidate from a tablet-local retriever (post-RLS).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalCandidate {
    /// Source tablet.
    pub tablet_id: TabletId,
    /// Row identity.
    pub row_id: RowId,
    /// Raw local score (higher is better).
    pub score: f64,
    /// Local rank (1-based) after RLS filtering.
    pub local_rank: u32,
    /// Whether this row was visible after RLS (must be true when emitted).
    pub rls_visible: bool,
}

/// Fusion method for coordinator merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FusionMethod {
    /// Reciprocal rank fusion with constant `k` (default 60).
    Rrf {
        /// RRF constant k.
        k: u32,
    },
    /// Max of raw scores (deterministic with tie-breaks).
    MaxScore,
}

impl Default for FusionMethod {
    fn default() -> Self {
        Self::Rrf { k: 60 }
    }
}

/// Global work budget for a distributed AI request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiWorkBudget {
    /// Maximum candidates retained globally after merge.
    pub candidate_ceiling: usize,
    /// Maximum total local candidates fetched across tablets.
    pub max_local_candidates: usize,
    /// Memory reservation bytes (advisory for the governor).
    pub memory_bytes: u64,
    /// Wall-clock deadline remaining in milliseconds.
    pub deadline_ms: u64,
}

impl Default for AiWorkBudget {
    fn default() -> Self {
        Self {
            candidate_ceiling: 100,
            max_local_candidates: 1_000,
            memory_bytes: 64 * 1024 * 1024,
            deadline_ms: 5_000,
        }
    }
}

/// Adaptive per-tablet candidate count.
///
/// `ceil(global_k * overfetch_factor / active_tablet_count)`.
pub fn adaptive_local_k(global_k: usize, overfetch_factor: f64, active_tablets: usize) -> usize {
    let tablets = active_tablets.max(1) as f64;
    let raw = (global_k as f64) * overfetch_factor / tablets;
    raw.ceil().max(1.0) as usize
}

/// Errors of distributed AI retrieval.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AiRetrievalError {
    /// A tablet returned an RLS-hidden row as a candidate (hygiene violation).
    #[error("RLS hygiene violation: tablet {tablet_id} emitted hidden row {row_id}")]
    RlsHygiene {
        /// Offending tablet.
        tablet_id: TabletId,
        /// Hidden row.
        row_id: u64,
    },
    /// Work budget exceeded.
    #[error("AI work budget exceeded: {0}")]
    BudgetExceeded(String),
    /// The request was cancelled locally or remotely.
    #[error("distributed AI request cancelled: {0:?}")]
    Cancelled(CancellationReason),
    /// Authenticated node-to-node transport failed.
    #[error("distributed AI transport failed: {0}")]
    Transport(String),
    /// A peer violated the versioned distributed-AI protocol.
    #[error("distributed AI protocol error: {0}")]
    Protocol(String),
}

/// One merged result row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MergedCandidate {
    /// Tablet of origin (lowest tablet id if fused across; for RRF we keep
    /// the tablet of the best local rank contribution for tie-break).
    pub tablet_id: TabletId,
    /// Row id.
    pub row_id: RowId,
    /// Fused final score.
    pub final_score: f64,
    /// Best raw local score observed.
    pub raw_score: f64,
}

#[derive(Debug, Clone)]
struct HeapKey {
    final_score: f64,
    tablet_id: TabletId,
    row_id: RowId,
}

impl PartialEq for HeapKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for HeapKey {}
impl PartialOrd for HeapKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // Max-heap: higher score first; then lower tablet id; then lower row id.
        match self
            .final_score
            .partial_cmp(&other.final_score)
            .unwrap_or(Ordering::Equal)
        {
            Ordering::Equal => other
                .tablet_id
                .cmp(&self.tablet_id)
                .then_with(|| other.row_id.cmp(&self.row_id)),
            ord => ord,
        }
    }
}

/// Deterministic coordinator merge (spec §13.4).
///
/// RLS hygiene: any candidate with `rls_visible == false` is a hard error
/// (hidden rows must never influence ranking).
pub fn merge_candidates(
    locals: &[LocalCandidate],
    method: FusionMethod,
    budget: &AiWorkBudget,
) -> Result<Vec<MergedCandidate>, AiRetrievalError> {
    if locals.len() > budget.max_local_candidates {
        return Err(AiRetrievalError::BudgetExceeded(format!(
            "{} local candidates > max {}",
            locals.len(),
            budget.max_local_candidates
        )));
    }
    for c in locals {
        if !c.rls_visible {
            return Err(AiRetrievalError::RlsHygiene {
                tablet_id: c.tablet_id,
                row_id: c.row_id.0,
            });
        }
    }

    // Group by (tablet, row) — a row should appear once per tablet.
    let mut by_key: HashMap<(TabletId, RowId), LocalCandidate> = HashMap::new();
    for c in locals {
        by_key
            .entry((c.tablet_id, c.row_id))
            .and_modify(|existing| {
                if c.score > existing.score {
                    *existing = c.clone();
                }
            })
            .or_insert_with(|| c.clone());
    }

    let fused: Vec<MergedCandidate> = match method {
        FusionMethod::Rrf { k } => {
            // RRF score = sum 1/(k + rank) across appearances (here one per tablet).
            by_key
                .into_values()
                .map(|c| {
                    let rrf = 1.0 / (f64::from(k) + f64::from(c.local_rank));
                    MergedCandidate {
                        tablet_id: c.tablet_id,
                        row_id: c.row_id,
                        final_score: rrf,
                        raw_score: c.score,
                    }
                })
                .collect()
        }
        FusionMethod::MaxScore => by_key
            .into_values()
            .map(|c| MergedCandidate {
                tablet_id: c.tablet_id,
                row_id: c.row_id,
                final_score: c.score,
                raw_score: c.score,
            })
            .collect(),
    };

    // Sort with deterministic tie-breaks, take ceiling.
    let mut heap: BinaryHeap<HeapKey> = BinaryHeap::new();
    let mut map: HashMap<(TabletId, RowId), MergedCandidate> = HashMap::new();
    for m in fused {
        let key = (m.tablet_id, m.row_id);
        heap.push(HeapKey {
            final_score: m.final_score,
            tablet_id: m.tablet_id,
            row_id: m.row_id,
        });
        map.insert(key, m);
    }

    let mut out = Vec::with_capacity(budget.candidate_ceiling.min(map.len()));
    while out.len() < budget.candidate_ceiling {
        let Some(hk) = heap.pop() else {
            break;
        };
        if let Some(m) = map.remove(&(hk.tablet_id, hk.row_id)) {
            out.push(m);
        }
    }
    Ok(out)
}

/// Audit metadata returned with AI/analytics results (spec §13.6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiConsistencyAudit {
    /// Requested read timestamp (string form of HLC).
    pub read_ts: String,
    /// Replica applied timestamp.
    pub replica_applied_ts: String,
    /// Measured staleness in microseconds (0 if caught up).
    pub staleness_micros: u64,
    /// Index applied_through timestamp.
    pub index_applied_ts: String,
    /// Model / preprocessing version.
    pub model_version: Option<String>,
    /// Preprocessing version.
    pub preprocessing_version: Option<String>,
}

// ---------------------------------------------------------------------------
// Authenticated remote tablet retrieval
// ---------------------------------------------------------------------------

/// Stable service discriminator inside the cluster internal-RPC multiplexer.
pub const REMOTE_AI_SERVICE_ID: u32 = 2;
/// Current private distributed-AI wire generation.
pub const REMOTE_AI_PROTOCOL_VERSION: u16 = 1;
/// Default maximum request or response body.
pub const DEFAULT_REMOTE_AI_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
/// Default maximum concurrent tablet retrievals held by one worker.
pub const DEFAULT_REMOTE_AI_EXECUTIONS: usize = 256;
/// Maximum forwarded authorization-context bytes.
pub const MAX_AI_AUTHORIZATION_CONTEXT_BYTES: usize = 64 * 1024;

/// One typed tablet-local search request.
///
/// `authorization_context` is an opaque, bounded server-issued identity
/// envelope. The worker executor must validate it and apply column grants,
/// RLS, and masks before returning candidates. The transport never treats
/// node mTLS alone as user authorization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiTabletQuery {
    /// Global query identity.
    pub query_id: QueryId,
    /// Tablet whose replica must execute this request.
    pub tablet_id: TabletId,
    /// Table name inside that tablet.
    pub table: String,
    /// Core hybrid-search request, bounded to tablet-local `k` by the
    /// coordinator.
    pub search: SearchRequest,
    /// Server-issued authorization envelope.
    pub authorization_context: Vec<u8>,
    /// Remaining deadline propagated to the worker.
    pub deadline_ms: Option<u64>,
    /// Global work budget.
    pub budget: AiWorkBudget,
}

/// One tablet result after authorization and optional exact rerank.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiTabletHit {
    /// Ranked candidate used by the deterministic global merge.
    pub candidate: LocalCandidate,
    /// Projected, already-masked cells.
    pub cells: Vec<(u16, Value)>,
    /// Exact-vector rerank score when requested.
    pub exact_rerank_score: Option<f32>,
    /// Per-replica consistency evidence.
    pub consistency: Option<AiConsistencyAudit>,
}

/// Worker implementation for tablet-local AI search.
#[async_trait::async_trait]
pub trait AiTabletExecutor: Send + Sync {
    /// Executes one request. Implementations must validate
    /// `authorization_context` and enforce RLS before local top-k.
    async fn retrieve(
        &self,
        request: &AiTabletQuery,
        control: ExecutionControl,
    ) -> Result<Vec<AiTabletHit>, AiRetrievalError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteAiEnvelope {
    version: u16,
    request: RemoteAiRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RemoteAiRequest {
    Retrieve(Box<AiTabletQuery>),
    Cancel {
        query_id: QueryId,
        tablet_id: TabletId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteAiResponseEnvelope {
    version: u16,
    response: RemoteAiResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RemoteAiResponse {
    Hits(Vec<AiTabletHit>),
    Cancelled,
    Error(String),
}

type AiExecutionKey = (QueryId, TabletId);

/// Worker-side endpoint for the distributed-AI protocol.
pub struct RemoteAiEndpoint {
    executor: Arc<dyn AiTabletExecutor>,
    controls: parking_lot::Mutex<HashMap<AiExecutionKey, ExecutionControl>>,
    max_executions: usize,
    max_message_bytes: usize,
}

impl RemoteAiEndpoint {
    /// Creates a bounded endpoint.
    pub fn new(executor: Arc<dyn AiTabletExecutor>) -> Self {
        Self::with_limits(
            executor,
            DEFAULT_REMOTE_AI_EXECUTIONS,
            DEFAULT_REMOTE_AI_MESSAGE_BYTES,
        )
    }

    /// Creates an endpoint with explicit concurrency and message bounds.
    pub fn with_limits(
        executor: Arc<dyn AiTabletExecutor>,
        max_executions: usize,
        max_message_bytes: usize,
    ) -> Self {
        Self {
            executor,
            controls: parking_lot::Mutex::new(HashMap::new()),
            max_executions: max_executions.max(1),
            max_message_bytes: max_message_bytes.max(1),
        }
    }

    /// Number of running tablet requests.
    pub fn active_executions(&self) -> usize {
        self.controls.lock().len()
    }

    /// Handles one authenticated internal RPC body.
    pub async fn handle(&self, bytes: &[u8]) -> Result<Vec<u8>, AiRetrievalError> {
        let envelope: RemoteAiEnvelope = decode_ai_wire(bytes, self.max_message_bytes)?;
        if envelope.version != REMOTE_AI_PROTOCOL_VERSION {
            return Err(AiRetrievalError::Protocol(format!(
                "unsupported distributed-AI protocol version {}; supported version is {}",
                envelope.version, REMOTE_AI_PROTOCOL_VERSION
            )));
        }
        let response = match self.handle_request(envelope.request).await {
            Ok(response) => response,
            Err(error) => RemoteAiResponse::Error(error.to_string()),
        };
        encode_ai_wire(
            &RemoteAiResponseEnvelope {
                version: REMOTE_AI_PROTOCOL_VERSION,
                response,
            },
            self.max_message_bytes,
        )
    }

    async fn handle_request(
        &self,
        request: RemoteAiRequest,
    ) -> Result<RemoteAiResponse, AiRetrievalError> {
        match request {
            RemoteAiRequest::Retrieve(request) => {
                if request.authorization_context.len() > MAX_AI_AUTHORIZATION_CONTEXT_BYTES {
                    return Err(AiRetrievalError::Protocol(format!(
                        "authorization context is {} bytes; limit is {}",
                        request.authorization_context.len(),
                        MAX_AI_AUTHORIZATION_CONTEXT_BYTES
                    )));
                }
                let key = (request.query_id, request.tablet_id);
                let control = request.deadline_ms.map_or_else(
                    || ExecutionControl::new(None),
                    |milliseconds| {
                        ExecutionControl::with_timeout(Duration::from_millis(milliseconds))
                    },
                );
                {
                    let mut controls = self.controls.lock();
                    if controls.contains_key(&key) {
                        return Err(AiRetrievalError::Protocol(format!(
                            "query {} is already retrieving tablet {}",
                            request.query_id, request.tablet_id
                        )));
                    }
                    if controls.len() >= self.max_executions {
                        return Err(AiRetrievalError::BudgetExceeded(format!(
                            "worker holds {} AI requests; limit is {}",
                            controls.len(),
                            self.max_executions
                        )));
                    }
                    controls.insert(key, control.clone());
                }
                let result = self.executor.retrieve(&request, control.clone()).await;
                let was_active = self.controls.lock().remove(&key).is_some();
                if !was_active || control.checkpoint().is_err() {
                    return Err(AiRetrievalError::Cancelled(control.reason()));
                }
                let hits = result?;
                for hit in &hits {
                    if hit.candidate.tablet_id != request.tablet_id {
                        return Err(AiRetrievalError::Protocol(format!(
                            "tablet {} returned candidate labeled {}",
                            request.tablet_id, hit.candidate.tablet_id
                        )));
                    }
                    if !hit.candidate.rls_visible {
                        return Err(AiRetrievalError::RlsHygiene {
                            tablet_id: hit.candidate.tablet_id,
                            row_id: hit.candidate.row_id.0,
                        });
                    }
                }
                Ok(RemoteAiResponse::Hits(hits))
            }
            RemoteAiRequest::Cancel {
                query_id,
                tablet_id,
            } => {
                if let Some(control) = self.controls.lock().get(&(query_id, tablet_id)).cloned() {
                    control.cancel(CancellationReason::ClientRequest);
                }
                Ok(RemoteAiResponse::Cancelled)
            }
        }
    }
}

/// One authenticated request/response carrier for distributed AI.
#[async_trait::async_trait]
pub trait AiRpcClient: Send + Sync {
    /// Performs one bounded internal RPC.
    async fn call(&self, request: Vec<u8>) -> Result<Vec<u8>, AiRetrievalError>;
}

/// In-process carrier for endpoint tests.
pub struct LoopbackAiRpcClient {
    endpoint: Arc<RemoteAiEndpoint>,
}

impl LoopbackAiRpcClient {
    /// Wraps one worker endpoint.
    pub fn new(endpoint: Arc<RemoteAiEndpoint>) -> Self {
        Self { endpoint }
    }
}

#[async_trait::async_trait]
impl AiRpcClient for LoopbackAiRpcClient {
    async fn call(&self, request: Vec<u8>) -> Result<Vec<u8>, AiRetrievalError> {
        self.endpoint.handle(&request).await
    }
}

/// Fully merged result plus authorized tablet payloads for the winners.
#[derive(Debug, Clone)]
pub struct DistributedAiResult {
    /// Deterministic global winners.
    pub candidates: Vec<MergedCandidate>,
    /// Winner payloads in the same order as `candidates`.
    pub hits: Vec<AiTabletHit>,
}

/// Borrowed inputs for one coordinator-side distributed-AI fan-out.
pub struct AiFanoutRequest<'a> {
    /// Stable query identity propagated to every tablet.
    pub query_id: QueryId,
    /// Tablets participating in the fan-out.
    pub tablets: &'a [TabletId],
    /// Authoritative table name.
    pub table: &'a str,
    /// Scored-search request.
    pub search: &'a SearchRequest,
    /// Server-issued authorization envelope.
    pub authorization_context: &'a [u8],
    /// Deterministic global fusion policy.
    pub fusion: FusionMethod,
    /// Tablet-local candidate overfetch multiplier.
    pub overfetch_factor: f64,
    /// Global deadline, work, candidate, and memory limits.
    pub budget: &'a AiWorkBudget,
    /// Parent cancellation and deadline control.
    pub control: &'a ExecutionControl,
}

/// Coordinator-side distributed-AI fan-out.
pub struct RemoteAiTransport {
    default_client: Option<Arc<dyn AiRpcClient>>,
    clients: parking_lot::RwLock<HashMap<TabletId, Arc<dyn AiRpcClient>>>,
    max_message_bytes: usize,
}

impl RemoteAiTransport {
    /// Creates a transport whose tablets use `default_client`.
    pub fn new(default_client: Arc<dyn AiRpcClient>) -> Self {
        Self {
            default_client: Some(default_client),
            clients: parking_lot::RwLock::new(HashMap::new()),
            max_message_bytes: DEFAULT_REMOTE_AI_MESSAGE_BYTES,
        }
    }

    /// Creates a fail-closed transport with no fallback tablet route.
    pub fn routed() -> Self {
        Self {
            default_client: None,
            clients: parking_lot::RwLock::new(HashMap::new()),
            max_message_bytes: DEFAULT_REMOTE_AI_MESSAGE_BYTES,
        }
    }

    /// Routes one tablet to a specific peer.
    pub fn with_client(self, tablet: TabletId, client: Arc<dyn AiRpcClient>) -> Self {
        self.clients.write().insert(tablet, client);
        self
    }

    fn client_for(&self, tablet: TabletId) -> Result<Arc<dyn AiRpcClient>, AiRetrievalError> {
        self.clients
            .read()
            .get(&tablet)
            .cloned()
            .or_else(|| self.default_client.as_ref().map(Arc::clone))
            .ok_or_else(|| {
                AiRetrievalError::Transport(format!(
                    "no authenticated AI route for tablet {tablet}"
                ))
            })
    }

    /// Fans out a typed search, enforces the global budget, and merges the
    /// authorized tablet-local top-k results.
    pub async fn retrieve(
        &self,
        request: AiFanoutRequest<'_>,
    ) -> Result<DistributedAiResult, AiRetrievalError> {
        let AiFanoutRequest {
            query_id,
            tablets,
            table,
            search,
            authorization_context,
            fusion,
            overfetch_factor,
            budget,
            control,
        } = request;
        if tablets.is_empty() {
            return Ok(DistributedAiResult {
                candidates: Vec::new(),
                hits: Vec::new(),
            });
        }
        if authorization_context.len() > MAX_AI_AUTHORIZATION_CONTEXT_BYTES {
            return Err(AiRetrievalError::Protocol(format!(
                "authorization context is {} bytes; limit is {}",
                authorization_context.len(),
                MAX_AI_AUTHORIZATION_CONTEXT_BYTES
            )));
        }
        if !overfetch_factor.is_finite() || overfetch_factor <= 0.0 {
            return Err(AiRetrievalError::Protocol(
                "overfetch factor must be finite and positive".to_owned(),
            ));
        }
        let request_control = control.child_with_timeout(Duration::from_millis(budget.deadline_ms));
        request_control
            .checkpoint()
            .map_err(|_| AiRetrievalError::Cancelled(request_control.reason()))?;
        let local_k = adaptive_local_k(budget.candidate_ceiling, overfetch_factor, tablets.len());
        let total_requested = local_k
            .checked_mul(tablets.len())
            .ok_or_else(|| AiRetrievalError::BudgetExceeded("candidate count overflow".into()))?;
        if total_requested > budget.max_local_candidates {
            return Err(AiRetrievalError::BudgetExceeded(format!(
                "{total_requested} requested local candidates > max {}",
                budget.max_local_candidates
            )));
        }
        let mut local_search = search.clone();
        local_search.limit = local_k;
        for named in &mut local_search.retrievers {
            match &mut named.retriever {
                Retriever::Ann { k, .. }
                | Retriever::Sparse { k, .. }
                | Retriever::MinHash { k, .. } => *k = local_k,
            }
        }
        if let Some(Rerank::ExactVector {
            candidate_limit, ..
        }) = &mut local_search.rerank
        {
            *candidate_limit = (*candidate_limit).max(local_k);
        }

        let mut calls = FuturesUnordered::new();
        for &tablet_id in tablets {
            let client = self.client_for(tablet_id)?;
            let request = AiTabletQuery {
                query_id,
                tablet_id,
                table: table.to_owned(),
                search: local_search.clone(),
                authorization_context: authorization_context.to_vec(),
                deadline_ms: request_control
                    .remaining_duration()
                    .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64),
                budget: budget.clone(),
            };
            let max_message_bytes = self.max_message_bytes;
            calls.push(async move {
                let response = ai_remote_call(
                    &client,
                    RemoteAiRequest::Retrieve(Box::new(request)),
                    max_message_bytes,
                )
                .await;
                (tablet_id, client, response)
            });
        }
        let mut hits = Vec::new();
        let mut retained_bytes = 0u64;
        while !calls.is_empty() {
            tokio::select! {
                _ = request_control.cancelled() => {
                    for &tablet_id in tablets {
                        let client = self.client_for(tablet_id)?;
                        let max_message_bytes = self.max_message_bytes;
                        tokio::spawn(async move {
                            let _ = ai_remote_call(
                                &client,
                                RemoteAiRequest::Cancel { query_id, tablet_id },
                                max_message_bytes,
                            ).await;
                        });
                    }
                    return Err(AiRetrievalError::Cancelled(request_control.reason()));
                }
                result = calls.next() => {
                    let Some((tablet_id, _client, response)) = result else {
                        break;
                    };
                    let response = match response {
                        Ok(response) => response,
                        Err(error) => {
                            self.cancel_tablets(query_id, tablets);
                            return Err(error);
                        }
                    };
                    match response {
                        RemoteAiResponse::Hits(mut tablet_hits) => {
                            if tablet_hits.len() > local_k {
                                self.cancel_tablets(query_id, tablets);
                                return Err(AiRetrievalError::Protocol(format!(
                                    "tablet {tablet_id} returned {} candidates; local limit is {local_k}",
                                    tablet_hits.len()
                                )));
                            }
                            let bytes = ai_wire_options()
                                .serialized_size(&tablet_hits)
                                .map_err(|error| AiRetrievalError::Protocol(error.to_string()))?;
                            retained_bytes = retained_bytes.saturating_add(bytes);
                            if retained_bytes > budget.memory_bytes {
                                self.cancel_tablets(query_id, tablets);
                                return Err(AiRetrievalError::BudgetExceeded(format!(
                                    "{retained_bytes} retained result bytes > memory budget {}",
                                    budget.memory_bytes
                                )));
                            }
                            hits.append(&mut tablet_hits);
                        }
                        other => {
                            self.cancel_tablets(query_id, tablets);
                            return Err(AiRetrievalError::Protocol(format!(
                                "expected AI Hits response, got {other:?}"
                            )));
                        }
                    }
                }
            }
        }
        if hits.len() > budget.max_local_candidates {
            return Err(AiRetrievalError::BudgetExceeded(format!(
                "{} returned local candidates > max {}",
                hits.len(),
                budget.max_local_candidates
            )));
        }
        let locals = hits
            .iter()
            .map(|hit| hit.candidate.clone())
            .collect::<Vec<_>>();
        let candidates = merge_candidates(&locals, fusion, budget)?;
        let by_key = hits
            .into_iter()
            .map(|hit| ((hit.candidate.tablet_id, hit.candidate.row_id), hit))
            .collect::<HashMap<_, _>>();
        let winner_hits = candidates
            .iter()
            .map(|candidate| {
                by_key
                    .get(&(candidate.tablet_id, candidate.row_id))
                    .cloned()
                    .ok_or_else(|| {
                        AiRetrievalError::Protocol(format!(
                            "winner {} from tablet {} has no payload",
                            candidate.row_id, candidate.tablet_id
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(DistributedAiResult {
            candidates,
            hits: winner_hits,
        })
    }

    fn cancel_tablets(&self, query_id: QueryId, tablets: &[TabletId]) {
        for &tablet_id in tablets {
            let Ok(client) = self.client_for(tablet_id) else {
                continue;
            };
            let max_message_bytes = self.max_message_bytes;
            tokio::spawn(async move {
                let _ = ai_remote_call(
                    &client,
                    RemoteAiRequest::Cancel {
                        query_id,
                        tablet_id,
                    },
                    max_message_bytes,
                )
                .await;
            });
        }
    }
}

async fn ai_remote_call(
    client: &Arc<dyn AiRpcClient>,
    request: RemoteAiRequest,
    max_message_bytes: usize,
) -> Result<RemoteAiResponse, AiRetrievalError> {
    let bytes = encode_ai_wire(
        &RemoteAiEnvelope {
            version: REMOTE_AI_PROTOCOL_VERSION,
            request,
        },
        max_message_bytes,
    )?;
    let bytes = client.call(bytes).await?;
    let response: RemoteAiResponseEnvelope = decode_ai_wire(&bytes, max_message_bytes)?;
    if response.version != REMOTE_AI_PROTOCOL_VERSION {
        return Err(AiRetrievalError::Protocol(format!(
            "peer answered with distributed-AI protocol version {}; supported version is {}",
            response.version, REMOTE_AI_PROTOCOL_VERSION
        )));
    }
    match response.response {
        RemoteAiResponse::Error(message) => Err(AiRetrievalError::Transport(message)),
        response => Ok(response),
    }
}

fn ai_wire_options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
}

fn encode_ai_wire<T: Serialize>(
    value: &T,
    max_message_bytes: usize,
) -> Result<Vec<u8>, AiRetrievalError> {
    let bytes = ai_wire_options()
        .serialize(value)
        .map_err(|error| AiRetrievalError::Protocol(error.to_string()))?;
    if bytes.len() > max_message_bytes {
        return Err(AiRetrievalError::Protocol(format!(
            "encoded distributed-AI message is {} bytes; limit is {max_message_bytes}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

fn decode_ai_wire<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    max_message_bytes: usize,
) -> Result<T, AiRetrievalError> {
    if bytes.len() > max_message_bytes {
        return Err(AiRetrievalError::Protocol(format!(
            "distributed-AI message is {} bytes; limit is {max_message_bytes}",
            bytes.len()
        )));
    }
    ai_wire_options()
        .with_limit(max_message_bytes as u64)
        .deserialize(bytes)
        .map_err(|error| AiRetrievalError::Protocol(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongreldb_core::query::Fusion;

    fn tid(n: u8) -> TabletId {
        TabletId::from_bytes({
            let mut b = [0u8; 16];
            b[15] = n;
            b
        })
    }

    fn cand(tablet: u8, row: u64, score: f64, rank: u32) -> LocalCandidate {
        LocalCandidate {
            tablet_id: tid(tablet),
            row_id: RowId(row),
            score,
            local_rank: rank,
            rls_visible: true,
        }
    }

    #[test]
    fn merge_is_deterministic() {
        let locals = vec![
            cand(2, 10, 0.9, 1),
            cand(1, 20, 0.8, 1),
            cand(2, 30, 0.7, 2),
            cand(1, 10, 0.95, 2),
        ];
        let budget = AiWorkBudget {
            candidate_ceiling: 10,
            ..AiWorkBudget::default()
        };
        let a = merge_candidates(&locals, FusionMethod::Rrf { k: 60 }, &budget).unwrap();
        let b = merge_candidates(&locals, FusionMethod::Rrf { k: 60 }, &budget).unwrap();
        assert_eq!(a, b);
        // Score desc: rank-1 entries outrank rank-2 for same k.
        assert!(a[0].final_score >= a[1].final_score);
    }

    #[test]
    fn rls_hidden_row_fails_closed() {
        let mut c = cand(1, 1, 1.0, 1);
        c.rls_visible = false;
        let err =
            merge_candidates(&[c], FusionMethod::default(), &AiWorkBudget::default()).unwrap_err();
        assert!(matches!(err, AiRetrievalError::RlsHygiene { .. }));
    }

    #[test]
    fn adaptive_local_k_scales() {
        assert_eq!(adaptive_local_k(10, 2.0, 5), 4); // ceil(4.0)
        assert_eq!(adaptive_local_k(10, 2.0, 1), 20);
        assert_eq!(adaptive_local_k(10, 2.0, 0), 20); // treat 0 tablets as 1
    }

    #[test]
    fn tie_break_tablet_then_row() {
        // Equal RRF ranks → lower tablet id first among equal scores.
        let locals = vec![cand(2, 5, 1.0, 1), cand(1, 9, 1.0, 1)];
        let out = merge_candidates(
            &locals,
            FusionMethod::Rrf { k: 60 },
            &AiWorkBudget {
                candidate_ceiling: 2,
                ..AiWorkBudget::default()
            },
        )
        .unwrap();
        assert_eq!(out.len(), 2);
        // Same rank → same RRF score; tablet 1 before tablet 2.
        assert_eq!(out[0].tablet_id, tid(1));
        assert_eq!(out[1].tablet_id, tid(2));
    }

    struct StaticExecutor;

    #[async_trait::async_trait]
    impl AiTabletExecutor for StaticExecutor {
        async fn retrieve(
            &self,
            request: &AiTabletQuery,
            control: ExecutionControl,
        ) -> Result<Vec<AiTabletHit>, AiRetrievalError> {
            control
                .checkpoint()
                .map_err(|_| AiRetrievalError::Cancelled(control.reason()))?;
            assert_eq!(request.authorization_context, b"signed-user");
            Ok(vec![AiTabletHit {
                candidate: LocalCandidate {
                    tablet_id: request.tablet_id,
                    row_id: RowId(u64::from(request.tablet_id.as_bytes()[15])),
                    score: 0.9,
                    local_rank: 1,
                    rls_visible: true,
                },
                cells: vec![(1, Value::Int64(7))],
                exact_rerank_score: Some(0.99),
                consistency: None,
            }])
        }
    }

    #[tokio::test]
    async fn remote_ai_fanout_merges_and_preserves_exact_rerank_payload() {
        let endpoint = Arc::new(RemoteAiEndpoint::new(Arc::new(StaticExecutor)));
        let client: Arc<dyn AiRpcClient> =
            Arc::new(LoopbackAiRpcClient::new(Arc::clone(&endpoint)));
        let transport = RemoteAiTransport::new(client);
        let budget = AiWorkBudget {
            candidate_ceiling: 2,
            max_local_candidates: 8,
            ..AiWorkBudget::default()
        };
        let search = SearchRequest {
            must: Vec::new(),
            retrievers: Vec::new(),
            fusion: Fusion::ReciprocalRank { constant: 60 },
            rerank: None,
            limit: 2,
            projection: Some(vec![1]),
        };
        let result = transport
            .retrieve(AiFanoutRequest {
                query_id: QueryId::new_random(),
                tablets: &[tid(2), tid(1)],
                table: "items",
                search: &search,
                authorization_context: b"signed-user",
                fusion: FusionMethod::default(),
                overfetch_factor: 2.0,
                budget: &budget,
                control: &ExecutionControl::new(None),
            })
            .await
            .unwrap();
        assert_eq!(result.candidates.len(), 2);
        assert_eq!(result.candidates[0].tablet_id, tid(1));
        assert_eq!(result.hits[0].exact_rerank_score, Some(0.99));
        assert_eq!(result.hits[0].cells, vec![(1, Value::Int64(7))]);
        assert_eq!(endpoint.active_executions(), 0);
    }

    #[tokio::test]
    async fn remote_ai_enforces_deadline_and_retained_memory_budget() {
        let endpoint = Arc::new(RemoteAiEndpoint::new(Arc::new(StaticExecutor)));
        let client: Arc<dyn AiRpcClient> =
            Arc::new(LoopbackAiRpcClient::new(Arc::clone(&endpoint)));
        let transport = RemoteAiTransport::new(client);
        let search = SearchRequest {
            must: Vec::new(),
            retrievers: Vec::new(),
            fusion: Fusion::ReciprocalRank { constant: 60 },
            rerank: None,
            limit: 1,
            projection: Some(vec![1]),
        };
        let deadline = transport
            .retrieve(AiFanoutRequest {
                query_id: QueryId::new_random(),
                tablets: &[tid(1)],
                table: "items",
                search: &search,
                authorization_context: b"signed-user",
                fusion: FusionMethod::default(),
                overfetch_factor: 1.0,
                budget: &AiWorkBudget {
                    deadline_ms: 0,
                    ..AiWorkBudget::default()
                },
                control: &ExecutionControl::new(None),
            })
            .await
            .unwrap_err();
        assert_eq!(
            deadline,
            AiRetrievalError::Cancelled(CancellationReason::Deadline)
        );

        let memory = transport
            .retrieve(AiFanoutRequest {
                query_id: QueryId::new_random(),
                tablets: &[tid(1)],
                table: "items",
                search: &search,
                authorization_context: b"signed-user",
                fusion: FusionMethod::default(),
                overfetch_factor: 1.0,
                budget: &AiWorkBudget {
                    memory_bytes: 1,
                    ..AiWorkBudget::default()
                },
                control: &ExecutionControl::new(None),
            })
            .await
            .unwrap_err();
        assert!(matches!(memory, AiRetrievalError::BudgetExceeded(_)));
        assert_eq!(endpoint.active_executions(), 0);
    }
}
